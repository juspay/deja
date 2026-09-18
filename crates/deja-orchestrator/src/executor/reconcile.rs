//! Restart-durable reconciler for k8s replay runs (#34 V3/V7).
//!
//! `api::runs::spawn_k8s_run` launches each run as a Job and watches it to a
//! terminal verdict on a per-launch background thread. That watcher is NOT
//! restart-durable: if the orchestrator process restarts, every in-flight
//! watcher is gone and its run hangs in a non-terminal state forever — nothing
//! is left to notice the Job finished (or was never created).
//!
//! This module is the safety net. On an interval it re-derives, from the
//! ground truth (the store's non-terminal runs + the live Jobs), what each run
//! should be, and settles it:
//!   * Job reached a terminal verdict  → report it (completed / failed).
//!   * No Job at all, past a grace period (orphaned — the Job was never created
//!     or has been deleted) → fail the run with a clear reason rather than let
//!     it hang.
//!   * Job still running, or a young orphan still inside the grace period (a
//!     launch may be in flight) → wait, do nothing this pass.
//!
//! It also settles what a Job cannot report, from the pod: a sidecar that
//! restarted mid-run (the replay continued against a cold process), and a pod
//! that cannot get past its init containers (the runner never starts, so the
//! thing that would normally report a broken candidate never runs).
//!
//! Every settle goes through [`deja_store::Store::update_run_state`], which is
//! terminal-guarded (V4: `WHERE state NOT IN ('completed','failed')`), so a
//! report that races the run's own push-back is a harmless zero-row no-op. That
//! is what makes the reconciler idempotent and re-runnable.
//!
//! The pure decision — [`reconcile_decisions`] and [`run_jobs_from_items`] —
//! is separated from the live loop (the store I/O, the kube LIST, the sleep) so
//! the classification is unit-tested with no cluster and no clock.
//!
//! GENERIC: the reconciler deals only in run ids, the launcher's run-id label,
//! and Job verdicts. It has no knowledge of any particular candidate.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::config::K8sExecutorConfig;
use super::k8s::{job_terminal_verdict, InClusterConfig, KubeApi, KubeTransport, UreqTransport};
use super::launch::RUN_ID_LABEL;
use deja_store::Store;

/// How often the reconciler runs a pass (override: `DEJA_RECONCILE_INTERVAL_SECS`).
const DEFAULT_INTERVAL_SECS: u64 = 30;

/// How long an init container must stay blocked before its run is failed
/// (override: `DEJA_INIT_BLOCKED_GRACE_SECS`). A pull can fail transiently and
/// recover; two passes of the same blocked reason is not a blip. Short, because
/// the alternative is a pod holding a node until `activeDeadlineSeconds`.
const DEFAULT_INIT_BLOCKED_GRACE_SECS: u64 = 120;

/// How long a non-terminal run with NO Job is tolerated before it is failed as
/// orphaned (override: `DEJA_RECONCILE_ORPHAN_GRACE_SECS`). Generous enough to
/// cover a launch that is still in flight — the launcher creates the Job just
/// after the run row — and for a freshly-created Job to become visible in the
/// LIST, so a healthy run is never failed out from under itself.
const DEFAULT_ORPHAN_GRACE_SECS: u64 = 300;

/// A non-terminal run as the store sees it (one input to the pure decision).
#[derive(Debug, Clone)]
pub struct ReconcileRun {
    pub run_id: String,
    /// The run's current store state (for log context; all are non-terminal).
    pub state: String,
    /// How long the run has existed (drives the orphan grace period).
    pub age: Duration,
}

/// A Job the launcher created, keyed by the run id it carries in its
/// [`RUN_ID_LABEL`] label, with its terminal verdict (`None` = still running).
#[derive(Debug, Clone)]
pub struct RunJob {
    pub job_name: String,
    pub run_id: String,
    pub verdict: Option<bool>,
    /// `spec.suspend` — the Job exists but runs nothing. This is what a queued
    /// run looks like: the scheduler creates the Job suspended and resumes it
    /// when the pool has room.
    pub suspended: bool,
}

/// What the reconciler decides to do about one run. Every variant carries a
/// human `reason` for fail-loud logging.
#[derive(Debug, Clone)]
pub enum ReconcileAction {
    /// The run's Job reached a terminal verdict — settle the run to match.
    /// `ok = true` → completed, `ok = false` → failed.
    Report {
        run_id: String,
        ok: bool,
        reason: String,
    },
    /// The run has no Job and has outlived the grace period — fail it.
    OrphanFail { run_id: String, reason: String },
    /// Leave the run alone this pass (running Job, or young orphan).
    Wait { run_id: String, reason: String },
}

/// A pod backing a run, reduced to the two things its Job never reports.
///
/// Both became reachable when the candidate became a native sidecar. A Job's
/// conditions describe its APP containers; a sidecar that crashes and comes back,
/// or an init container that cannot start at all, leaves the Job saying nothing
/// while the run is already lost.
#[derive(Debug, Clone)]
pub struct RunPod {
    pub run_id: String,
    pub pod_name: String,
    /// Containers that have restarted.
    pub restarts: Vec<Restarted>,
    /// An init container that is not going to start on its own.
    pub blocked_init: Option<BlockedInit>,
}

/// A container that has restarted, and how the previous life ended.
///
/// `restartPolicy: Always` restarts a sidecar whatever the exit code, so a clean
/// exit and an OOM kill both land here — and they send a reader to opposite
/// places. Carrying the last termination is the difference between "raise the
/// memory limit" and "the router decided to shut down".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restarted {
    pub container: String,
    pub count: u64,
    /// `lastState.terminated.reason` (e.g. `OOMKilled`, `Error`, `Completed`).
    pub last_reason: Option<String>,
    /// `lastState.terminated.exitCode`.
    pub last_exit_code: Option<i64>,
}

/// An init container stuck in a waiting state it will not leave by itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockedInit {
    pub container: String,
    pub reason: String,
    pub message: String,
}

/// What a pod says about a run that its Job does not.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PodTrouble {
    /// A container restarted mid-run.
    Restarted(Restarted),
    /// The pod cannot get past its init containers.
    BlockedInit(BlockedInit),
}

/// Waiting reasons that mean "this container is not going to start on its own".
/// `ContainerCreating` and `PodInitializing` are deliberately absent: those are
/// a pod doing its job, and failing on them would fail every healthy run.
const BLOCKED_REASONS: [&str; 5] = [
    "ImagePullBackOff",
    "ErrImagePull",
    "InvalidImageName",
    "CreateContainerConfigError",
    "CrashLoopBackOff",
];

/// Map raw Pod `.items` (from [`KubeApi::list_pods`]) to what each says about
/// its run. Pods without the run-id label are skipped (not ours). Pure — tested
/// against hand-built Pod JSON.
pub fn run_pods_from_items(items: &[Value], label_key: &str) -> Vec<RunPod> {
    items
        .iter()
        .filter_map(|pod| {
            let run_id = pod
                .pointer("/metadata/labels")
                .and_then(|labels| labels.get(label_key))
                .and_then(Value::as_str)?
                .to_owned();
            let statuses = |path: &str| -> Vec<Value> {
                pod.pointer(path)
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default()
            };
            let inits = statuses("/status/initContainerStatuses");
            let apps = statuses("/status/containerStatuses");
            let name_of = |c: &Value| {
                c.get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("<unnamed>")
                    .to_owned()
            };
            let restarts = inits
                .iter()
                .chain(apps.iter())
                .filter_map(|c| {
                    let count = c.get("restartCount").and_then(Value::as_u64).unwrap_or(0);
                    (count > 0).then(|| Restarted {
                        container: name_of(c),
                        count,
                        last_reason: c
                            .pointer("/lastState/terminated/reason")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                        last_exit_code: c
                            .pointer("/lastState/terminated/exitCode")
                            .and_then(Value::as_i64),
                    })
                })
                .collect();
            let blocked_init = inits.iter().find_map(|c| {
                let reason = c
                    .pointer("/state/waiting/reason")
                    .and_then(Value::as_str)?
                    .to_owned();
                BLOCKED_REASONS
                    .contains(&reason.as_str())
                    .then(|| BlockedInit {
                        container: name_of(c),
                        reason,
                        message: c
                            .pointer("/state/waiting/message")
                            .and_then(Value::as_str)
                            .unwrap_or("")
                            .to_owned(),
                    })
            });
            Some(RunPod {
                run_id,
                pod_name: pod
                    .pointer("/metadata/name")
                    .and_then(Value::as_str)
                    .unwrap_or("<unnamed>")
                    .to_owned(),
                restarts,
                blocked_init,
            })
        })
        .collect()
}

/// The pure pod decision: what, if anything, is wrong with this pod that its
/// Job will never say. A restart outranks a blocked init — a container that has
/// restarted has already run, so the run is already invalid whatever the pod
/// does next.
pub fn pod_trouble(pod: &RunPod) -> Option<PodTrouble> {
    if let Some(restarted) = pod.restarts.first() {
        return Some(PodTrouble::Restarted(restarted.clone()));
    }
    pod.blocked_init.clone().map(PodTrouble::BlockedInit)
}

/// The failure a [`PodTrouble`] settles the run with. Written here, once, so
/// the run's `failure_reason` explains itself without a reader having to go and
/// find the pod — which by then is usually gone.
pub fn pod_trouble_reason(pod_name: &str, trouble: &PodTrouble) -> String {
    match trouble {
        PodTrouble::Restarted(r) => format!(
            "pod {pod_name}: container '{}' restarted {} time(s) during the run{}. A restarted \
             container comes back COLD — its in-process state is empty, and a restarted store \
             sidecar loses what the runner seeded into it — so the rest of the replay would \
             diverge at boundaries the candidate never touched. Failing the run rather than \
             scoring those divergences",
            r.container,
            r.count,
            match (&r.last_reason, r.last_exit_code) {
                (Some(reason), Some(code)) => format!(" (last exit: {reason}, code {code})"),
                (Some(reason), None) => format!(" (last exit: {reason})"),
                (None, Some(code)) => format!(" (last exit code {code})"),
                (None, None) => String::new(),
            }
        ),
        PodTrouble::BlockedInit(b) => format!(
            "pod {pod_name}: init container '{}' cannot start ({}{}). The runner does not start \
             until every init container has, so nothing would report this run until its \
             activeDeadlineSeconds expires",
            b.container,
            b.reason,
            if b.message.is_empty() {
                String::new()
            } else {
                format!(": {}", b.message)
            }
        ),
    }
}

/// Map raw Job `.items` (from [`KubeApi::list_jobs`]) to the run each backs. A
/// launcher Job carries the run id in its [`RUN_ID_LABEL`] label; its verdict
/// comes from the shared [`job_terminal_verdict`]. Jobs without the label are
/// skipped (not ours). Pure — tested against hand-built Job JSON.
pub fn run_jobs_from_items(items: &[Value], label_key: &str) -> Vec<RunJob> {
    items
        .iter()
        .filter_map(|job| {
            let run_id = job
                .pointer("/metadata/labels")
                .and_then(|labels| labels.get(label_key))
                .and_then(Value::as_str)?
                .to_owned();
            let job_name = job
                .pointer("/metadata/name")
                .and_then(Value::as_str)
                .unwrap_or("<unnamed>")
                .to_owned();
            Some(RunJob {
                job_name,
                run_id,
                verdict: job_terminal_verdict(job),
                suspended: job
                    .pointer("/spec/suspend")
                    .and_then(Value::as_bool)
                    .unwrap_or(false),
            })
        })
        .collect()
}

/// The pure reconcile decision: given the non-terminal runs and the live Jobs,
/// classify each run into exactly one [`ReconcileAction`]. No I/O, no clock —
/// ages are supplied on `runs`, the grace threshold is a parameter — so the
/// classification is fully unit-testable.
///
/// A queued run needs no exception here: the scheduler creates its Job up
/// front, SUSPENDED, so "no Job" still means what it always meant. Only the
/// wording of the wait changes — a suspended Job is waiting for capacity, and
/// saying "still running" about one would be false.
pub fn reconcile_decisions(
    runs: &[ReconcileRun],
    jobs: &[RunJob],
    grace: Duration,
) -> Vec<ReconcileAction> {
    let by_run: HashMap<&str, &RunJob> = jobs.iter().map(|j| (j.run_id.as_str(), j)).collect();

    runs.iter()
        .map(|run| match by_run.get(run.run_id.as_str()) {
            Some(job) => match job.verdict {
                Some(true) => ReconcileAction::Report {
                    run_id: run.run_id.clone(),
                    ok: true,
                    reason: format!("Job {} reached terminal verdict: complete", job.job_name),
                },
                Some(false) => ReconcileAction::Report {
                    run_id: run.run_id.clone(),
                    ok: false,
                    reason: format!(
                        "Job {} reached terminal verdict: failed (see runner logs / pod events)",
                        job.job_name
                    ),
                },
                None if job.suspended => ReconcileAction::Wait {
                    run_id: run.run_id.clone(),
                    reason: format!("Job {} suspended — queued for capacity", job.job_name),
                },
                None => ReconcileAction::Wait {
                    run_id: run.run_id.clone(),
                    reason: format!("Job {} still running", job.job_name),
                },
            },
            None if run.age >= grace => ReconcileAction::OrphanFail {
                run_id: run.run_id.clone(),
                reason: format!(
                    "no Job found for run (state '{}') after grace period \
                     (age {}s >= grace {}s) — the Job was never created or has been \
                     deleted; failing rather than letting the run hang",
                    run.state,
                    run.age.as_secs(),
                    grace.as_secs()
                ),
            },
            None => ReconcileAction::Wait {
                run_id: run.run_id.clone(),
                reason: format!(
                    "no Job yet but within grace (age {}s < grace {}s) — launch may be in flight",
                    run.age.as_secs(),
                    grace.as_secs()
                ),
            },
        })
        .collect()
}

/// Spawn the reconcile loop as a background tokio task. Builds the in-cluster
/// kube client up front; if that fails it logs and returns without spawning
/// (the reconciler is a safety net — its absence must not take the API down,
/// and the per-run launch path builds its own client anyway).
///
/// V7 (singleton): for a SINGLE orchestrator replica this is exactly-once per
/// pass and safe. With multiple replicas, each would reconcile independently
/// and double-report — harmless because every settle is idempotent through the
/// store's terminal guard (V4), just redundant work. True single-writer
/// election (a k8s `Lease` / leader election) is out of scope here; add it if
/// the orchestrator is ever scaled past one replica.
pub fn spawn(store: Arc<Store>, incluster: InClusterConfig, cfg: K8sExecutorConfig) {
    let transport = match UreqTransport::new(&incluster) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "deja-orchestrator: k8s reconciler NOT started — cannot build kube client: {e}"
            );
            return;
        }
    };
    let api = Arc::new(KubeApi::new(transport));
    let interval = Duration::from_secs(env_secs(
        "DEJA_RECONCILE_INTERVAL_SECS",
        DEFAULT_INTERVAL_SECS,
    ));
    let grace = Duration::from_secs(env_secs(
        "DEJA_RECONCILE_ORPHAN_GRACE_SECS",
        DEFAULT_ORPHAN_GRACE_SECS,
    ));
    let init_grace = Duration::from_secs(env_secs(
        "DEJA_INIT_BLOCKED_GRACE_SECS",
        DEFAULT_INIT_BLOCKED_GRACE_SECS,
    ));
    eprintln!(
        "deja-orchestrator: k8s reconciler started (jobs ns {}, every {}s, orphan grace {}s)",
        cfg.jobs_namespace,
        interval.as_secs(),
        grace.as_secs()
    );
    tokio::spawn(run_loop(store, api, cfg, interval, grace, init_grace));
}

/// The live loop: one pass, then sleep, forever. The blocking kube LIST is the
/// only untested part; the classification it feeds is [`reconcile_decisions`].
async fn run_loop<T>(
    store: Arc<Store>,
    api: Arc<KubeApi<T>>,
    cfg: K8sExecutorConfig,
    interval: Duration,
    grace: Duration,
    init_grace: Duration,
) where
    T: KubeTransport + Send + Sync + 'static,
{
    // When each still-blocked run was FIRST seen blocked. Held across passes so
    // a transient pull failure is not a verdict; lost on restart, which costs
    // one more grace period and never a wrong failure.
    let mut blocked_since: HashMap<String, Instant> = HashMap::new();
    loop {
        reconcile_pass(&store, &api, &cfg, grace, init_grace, &mut blocked_since).await;
        tokio::time::sleep(interval).await;
    }
}

/// One reconcile pass. Fail-loud: reports what it did (or why it could not) on
/// every pass. Any store/kube failure aborts THIS pass only — the next tick
/// retries.
async fn reconcile_pass<T>(
    store: &Store,
    api: &Arc<KubeApi<T>>,
    cfg: &K8sExecutorConfig,
    grace: Duration,
    init_grace: Duration,
    blocked_since: &mut HashMap<String, Instant>,
) where
    T: KubeTransport + Send + Sync + 'static,
{
    let active = match store.list_active_runs().await {
        Ok(a) => a,
        Err(e) => {
            eprintln!("reconcile: list_active_runs failed: {e} — skipping this pass");
            return;
        }
    };
    if active.is_empty() {
        return; // nothing outstanding — stay quiet
    }

    // The kube client is blocking (ureq); keep it off the async runtime worker.
    let api_for_list = api.clone();
    let ns = cfg.jobs_namespace.clone();
    let items = match tokio::task::spawn_blocking(move || api_for_list.list_jobs(&ns, RUN_ID_LABEL))
        .await
    {
        Ok(Ok(items)) => items,
        Ok(Err(e)) => {
            eprintln!("reconcile: list_jobs failed: {e} — skipping this pass");
            return;
        }
        Err(e) => {
            eprintln!("reconcile: list_jobs task join failed: {e} — skipping this pass");
            return;
        }
    };

    let jobs = run_jobs_from_items(&items, RUN_ID_LABEL);
    let runs: Vec<ReconcileRun> = active
        .iter()
        .map(|r| ReconcileRun {
            run_id: r.run_id.clone(),
            state: r.state.clone(),
            age: Duration::from_secs_f64(r.age_secs.max(0.0)),
        })
        .collect();

    let actions = reconcile_decisions(&runs, &jobs, grace);
    let (mut reported, mut orphaned, mut waiting) = (0u32, 0u32, 0u32);
    for action in &actions {
        match action {
            ReconcileAction::Report { run_id, ok, reason } => {
                reported += 1;
                settle(store, run_id, *ok, reason).await;
            }
            ReconcileAction::OrphanFail { run_id, reason } => {
                orphaned += 1;
                settle(store, run_id, false, reason).await;
            }
            ReconcileAction::Wait { .. } => waiting += 1,
        }
    }
    eprintln!(
        "reconcile: pass over {} non-terminal run(s) vs {} Job(s): reported {reported}, \
         orphaned {orphaned}, waiting {waiting}",
        runs.len(),
        jobs.len()
    );

    // Second half: what the PODS say that their Jobs do not. A sidecar that
    // restarted, or an init container that cannot start, leaves the Job
    // reporting nothing at all — the first because the pod is still Running, the
    // second because the app containers never begin. Both end with a node held
    // until activeDeadlineSeconds, and the second one silently: the runner is
    // the thing that reports a candidate it cannot reach, and it never starts.
    let still_live: Vec<&str> = actions
        .iter()
        .filter_map(|a| match a {
            ReconcileAction::Wait { run_id, .. } => Some(run_id.as_str()),
            _ => None,
        })
        .collect();
    if still_live.is_empty() {
        blocked_since.clear();
        return;
    }
    let api_for_pods = api.clone();
    let ns = cfg.jobs_namespace.clone();
    let pod_items = match tokio::task::spawn_blocking(move || {
        api_for_pods.list_pods(&ns, RUN_ID_LABEL)
    })
    .await
    {
        Ok(Ok(items)) => items,
        Ok(Err(e)) => {
            eprintln!("reconcile: list_pods failed: {e} — skipping the pod checks this pass");
            return;
        }
        Err(e) => {
            eprintln!("reconcile: list_pods task join failed: {e} — skipping the pod checks");
            return;
        }
    };
    for pod in run_pods_from_items(&pod_items, RUN_ID_LABEL) {
        if !still_live.contains(&pod.run_id.as_str()) {
            continue;
        }
        let Some(trouble) = pod_trouble(&pod) else {
            // Healthy: whatever it was blocked on earlier, it is not blocked now.
            blocked_since.remove(&pod.run_id);
            continue;
        };
        if matches!(trouble, PodTrouble::BlockedInit(_)) {
            let first_seen = blocked_since
                .entry(pod.run_id.clone())
                .or_insert_with(Instant::now);
            if first_seen.elapsed() < init_grace {
                continue; // a pull can fail once and recover; wait one more pass
            }
        }
        let reason = pod_trouble_reason(&pod.pod_name, &trouble);
        eprintln!("reconcile: {}: {reason}", pod.run_id);
        settle(store, &pod.run_id, false, &reason).await;
        blocked_since.remove(&pod.run_id);
        // Reclaim the node. The run is settled either way, but a Job left behind
        // goes on holding a pod that cannot finish — which is the whole failure
        // being reported here.
        let api_for_kill = api.clone();
        let (ns, run_id) = (cfg.jobs_namespace.clone(), pod.run_id.clone());
        if let Ok(Err(e)) = tokio::task::spawn_blocking(move || {
            super::launch::kill_run(&api_for_kill, &ns, &run_id)
        })
        .await
        {
            eprintln!("reconcile: {}: could not delete its Job: {e}", pod.run_id);
        }
    }
}

/// Settle one run through the terminal-guarded store update (idempotent). A
/// failed verdict records the reason; a completed one carries none — matching
/// `spawn_k8s_run`'s own finish semantics.
async fn settle(store: &Store, run_id: &str, ok: bool, reason: &str) {
    let state = if ok { "completed" } else { "failed" };
    let failure = if ok {
        None
    } else {
        Some(serde_json::json!({ "message": reason }))
    };
    match store
        .update_run_state(run_id, state, failure.as_ref())
        .await
    {
        Ok(()) => eprintln!("reconcile: settled {run_id} -> {state} ({reason})"),
        Err(e) => eprintln!("reconcile: settle {run_id} -> {state} failed: {e}"),
    }
}

/// Read a u64 seconds knob from the environment, falling back to `default` when
/// unset or unparseable.
pub(super) fn env_secs(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn run(run_id: &str, age: Duration) -> ReconcileRun {
        ReconcileRun {
            run_id: run_id.to_owned(),
            state: "running".to_owned(),
            age,
        }
    }

    fn job(run_id: &str, verdict: Option<bool>) -> RunJob {
        RunJob {
            job_name: format!("deja-replay-{run_id}"),
            run_id: run_id.to_owned(),
            verdict,
            suspended: false,
        }
    }

    /// A queued run's Job: created, suspended, running nothing.
    fn suspended_job(run_id: &str) -> RunJob {
        RunJob {
            suspended: true,
            ..job(run_id, None)
        }
    }

    const GRACE: Duration = Duration::from_secs(300);

    #[test]
    fn completed_job_reports_ok() {
        let actions = reconcile_decisions(
            &[run("run-1", Duration::ZERO)],
            &[job("run-1", Some(true))],
            GRACE,
        );
        match &actions[0] {
            ReconcileAction::Report { run_id, ok, .. } => {
                assert_eq!(run_id, "run-1");
                assert!(*ok);
            }
            other => panic!("expected Report ok, got {other:?}"),
        }
    }

    #[test]
    fn failed_job_reports_not_ok() {
        let actions = reconcile_decisions(
            &[run("run-2", Duration::ZERO)],
            &[job("run-2", Some(false))],
            GRACE,
        );
        match &actions[0] {
            ReconcileAction::Report { run_id, ok, .. } => {
                assert_eq!(run_id, "run-2");
                assert!(!*ok);
            }
            other => panic!("expected Report not-ok, got {other:?}"),
        }
    }

    #[test]
    fn running_job_waits() {
        let actions = reconcile_decisions(
            &[run("run-3", Duration::ZERO)],
            &[job("run-3", None)],
            GRACE,
        );
        assert!(
            matches!(&actions[0], ReconcileAction::Wait { run_id, .. } if run_id == "run-3"),
            "a still-running Job means wait, not settle: {:?}",
            actions[0]
        );
    }

    #[test]
    fn orphan_past_grace_fails() {
        // No Job for the run, and it is older than the grace period.
        let actions = reconcile_decisions(&[run("run-4", Duration::from_secs(600))], &[], GRACE);
        match &actions[0] {
            ReconcileAction::OrphanFail { run_id, reason } => {
                assert_eq!(run_id, "run-4");
                assert!(reason.contains("no Job"));
            }
            other => panic!("expected OrphanFail, got {other:?}"),
        }
    }

    #[test]
    fn young_orphan_waits_within_grace() {
        // No Job yet, but the run is younger than the grace period — the launch
        // may still be in flight, so we must NOT fail it out from under itself.
        let actions = reconcile_decisions(&[run("run-5", Duration::from_secs(5))], &[], GRACE);
        assert!(
            matches!(&actions[0], ReconcileAction::Wait { run_id, .. } if run_id == "run-5"),
            "a young orphan waits, it is not failed: {:?}",
            actions[0]
        );
    }

    #[test]
    fn unrelated_jobs_do_not_settle_a_run() {
        // A Job exists but for a DIFFERENT run — the run under review is a young
        // orphan and must wait, not adopt someone else's verdict.
        let actions = reconcile_decisions(
            &[run("run-6", Duration::from_secs(1))],
            &[job("run-other", Some(true))],
            GRACE,
        );
        assert!(matches!(&actions[0], ReconcileAction::Wait { .. }));
    }

    /// A queued run has a Job — suspended — so it takes the Job arm, and the
    /// wait must not call it "still running". A suspended Job is running
    /// nothing; reading that line during an incident would send someone looking
    /// for a pod that does not exist.
    #[test]
    fn a_suspended_job_waits_as_queued_rather_than_as_running() {
        let actions = reconcile_decisions(
            &[run("run-q", Duration::from_secs(600))],
            &[suspended_job("run-q")],
            GRACE,
        );
        match &actions[0] {
            ReconcileAction::Wait { run_id, reason } => {
                assert_eq!(run_id, "run-q");
                assert!(
                    reason.contains("suspended") && reason.contains("queued for capacity"),
                    "the wait must say the Job is queued, not running: {reason}"
                );
            }
            other => panic!("expected Wait, got {other:?}"),
        }
    }

    /// And the orphan rule is untouched by the queue existing: a run with no Job
    /// past the grace is still failed, whether or not a scheduler is running.
    /// This is what creating the Job up front buys — no exception to carry.
    #[test]
    fn a_run_with_no_job_is_still_an_orphan_with_a_scheduler_running() {
        let actions = reconcile_decisions(&[run("run-r", Duration::from_secs(600))], &[], GRACE);
        assert!(
            matches!(&actions[0], ReconcileAction::OrphanFail { reason, .. } if reason.contains("no Job")),
            "{:?}",
            actions[0]
        );
    }

    /// `spec.suspend` is how a queued Job is told apart from a running one, so
    /// it has to survive the parse — a Job read as un-suspended would be counted
    /// as holding a slot it does not hold.
    /// Finding A, made a test: a sidecar that crashes is restarted in place, so
    /// the pod stays Running and the Job stays happy while the replay continues
    /// against a router with empty in-process caches. Nothing else in the system
    /// notices — which is how a candidate crash would surface as cache-shaped
    /// divergences at a boundary the candidate never touched.
    #[test]
    fn a_restarted_container_is_trouble_the_job_never_reports() {
        let items = vec![json!({
            "metadata": { "name": "deja-replay-r-abc", "labels": { "deja.run-id": "r" } },
            "status": {
                "phase": "Running",
                "initContainerStatuses": [
                    { "name": "postgres", "restartCount": 0 },
                    { "name": "candidate", "restartCount": 2 }
                ],
                "containerStatuses": [{ "name": "runner", "restartCount": 0 }]
            }
        })];
        let pods = run_pods_from_items(&items, "deja.run-id");
        assert_eq!(pods[0].run_id, "r");
        let trouble = pod_trouble(&pods[0]).expect("a restarted container is trouble");
        let PodTrouble::Restarted(r) = &trouble else {
            panic!("expected Restarted: {trouble:?}");
        };
        assert_eq!((r.container.as_str(), r.count), ("candidate", 2));
        let reason = pod_trouble_reason(&pods[0].pod_name, &trouble);
        assert!(
            reason.contains("deja-replay-r-abc") && reason.to_lowercase().contains("cold"),
            "the failure must name the pod and explain why a restart invalidates the \
             replay: {reason}"
        );
    }

    /// Finding B: an init container that cannot start means the RUNNER never
    /// starts — and the runner is what reports an unreachable candidate. So the
    /// run would sit until activeDeadlineSeconds holding its node, reported by
    /// nothing. This is the state that has to be recognised from the pod.
    #[test]
    fn an_init_container_that_cannot_start_is_recognised() {
        let items = vec![json!({
            "metadata": { "name": "deja-replay-b-xyz", "labels": { "deja.run-id": "b" } },
            "status": {
                "phase": "Pending",
                "initContainerStatuses": [
                    { "name": "postgres", "restartCount": 0, "state": { "running": {} } },
                    { "name": "candidate", "restartCount": 0, "state": { "waiting": {
                        "reason": "ImagePullBackOff",
                        "message": "Back-off pulling image \"candidate-image-patched-per-run\""
                    } } }
                ]
            }
        })];
        let pods = run_pods_from_items(&items, "deja.run-id");
        let Some(PodTrouble::BlockedInit(blocked)) = pod_trouble(&pods[0]) else {
            panic!("expected BlockedInit: {:?}", pod_trouble(&pods[0]));
        };
        assert_eq!(blocked.container, "candidate");
        assert_eq!(blocked.reason, "ImagePullBackOff");
        assert!(blocked.message.contains("Back-off pulling"));
    }

    /// The other half of that rule, and the one that would hurt if it were
    /// wrong: a pod doing normal work is NOT trouble. `PodInitializing` and
    /// `ContainerCreating` are what every healthy run looks like for its first
    /// seconds, and failing on them would fail every run.
    #[test]
    fn a_pod_merely_starting_up_is_not_trouble() {
        let items = vec![json!({
            "metadata": { "name": "deja-replay-h", "labels": { "deja.run-id": "h" } },
            "status": {
                "phase": "Pending",
                "initContainerStatuses": [
                    { "name": "migrations", "restartCount": 0, "state": { "waiting": {
                        "reason": "ContainerCreating" } } }
                ],
                "containerStatuses": [
                    { "name": "runner", "restartCount": 0, "state": { "waiting": {
                        "reason": "PodInitializing" } } }
                ]
            }
        })];
        let pods = run_pods_from_items(&items, "deja.run-id");
        assert_eq!(pod_trouble(&pods[0]), None, "{:?}", pods[0]);
    }

    /// `restartPolicy: Always` restarts a sidecar whatever the exit code, so the
    /// two ways a candidate can end its life — killed, or exiting cleanly — both
    /// arrive here. They point a reader at opposite things (a memory limit
    /// versus a router that decided to shut down), so the failure has to say
    /// which one happened, not just that a restart occurred.
    #[test]
    fn a_restart_says_how_the_previous_life_ended() {
        let pod_json = |last: serde_json::Value| {
            json!({
                "metadata": { "name": "deja-replay-k", "labels": { "deja.run-id": "k" } },
                "status": { "initContainerStatuses": [
                    { "name": "candidate", "restartCount": 1, "lastState": last }
                ] }
            })
        };
        let reason_for = |last: serde_json::Value| {
            let pods = run_pods_from_items(&[pod_json(last)], "deja.run-id");
            pod_trouble_reason(&pods[0].pod_name, &pod_trouble(&pods[0]).expect("trouble"))
        };

        let killed =
            reason_for(json!({ "terminated": { "reason": "OOMKilled", "exitCode": 137 } }));
        assert!(
            killed.contains("OOMKilled") && killed.contains("137"),
            "a killed container must say so: {killed}"
        );

        let clean = reason_for(json!({ "terminated": { "reason": "Completed", "exitCode": 0 } }));
        assert!(
            clean.contains("Completed") && clean.contains("code 0"),
            "a clean exit is still a restart, and must not read like a crash: {clean}"
        );

        // A pod whose previous state kubelet has not filled in is still a
        // restart — the run is just as invalid, so it must not be skipped.
        let unknown = reason_for(json!({}));
        assert!(
            unknown.contains("restarted 1 time(s)") && !unknown.contains("last exit"),
            "an unknown previous life must still report the restart: {unknown}"
        );
    }

    /// A restart outranks a blocked init: a container that has restarted has
    /// already run, so the replay is invalid whatever the pod does next, and
    /// reporting the block instead would send the reader to the wrong cause.
    #[test]
    fn a_restart_outranks_a_blocked_init() {
        let pod = RunPod {
            run_id: "x".into(),
            pod_name: "deja-replay-x".into(),
            restarts: vec![Restarted {
                container: "candidate".into(),
                count: 1,
                last_reason: None,
                last_exit_code: None,
            }],
            blocked_init: Some(BlockedInit {
                container: "candidate".into(),
                reason: "CrashLoopBackOff".into(),
                message: String::new(),
            }),
        };
        assert!(matches!(pod_trouble(&pod), Some(PodTrouble::Restarted(_))));
    }

    /// Pods that are not ours carry no run-id label and must be ignored — the
    /// LIST is namespace-wide.
    #[test]
    fn pods_without_a_run_id_label_are_not_ours() {
        let items = vec![json!({
            "metadata": { "name": "some-other-pod", "labels": { "app": "unrelated" } },
            "status": { "phase": "Running" }
        })];
        assert!(run_pods_from_items(&items, "deja.run-id").is_empty());
    }

    #[test]
    fn run_jobs_from_items_reads_suspension() {
        let items = vec![
            json!({
                "metadata": { "name": "deja-replay-q", "labels": { "deja.run-id": "q" } },
                "spec": { "suspend": true }
            }),
            json!({
                "metadata": { "name": "deja-replay-r", "labels": { "deja.run-id": "r" } },
                "spec": {}
            }),
        ];
        let jobs = run_jobs_from_items(&items, "deja.run-id");
        assert_eq!(jobs[0].run_id, "q");
        assert!(
            jobs[0].suspended,
            "spec.suspend true must read as suspended"
        );
        assert!(
            !jobs[1].suspended,
            "an absent spec.suspend is a running Job, not a queued one"
        );
    }

    #[test]
    fn run_jobs_from_items_extracts_run_id_and_verdict() {
        let items = vec![
            json!({
                "metadata": { "name": "deja-replay-run-1", "labels": { "deja.run-id": "run-1" } },
                "status": { "conditions": [{ "type": "Complete", "status": "True" }] }
            }),
            json!({
                "metadata": { "name": "deja-replay-run-2", "labels": { "deja.run-id": "run-2" } },
                "status": { "active": 1 }
            }),
        ];
        let jobs = run_jobs_from_items(&items, RUN_ID_LABEL);
        assert_eq!(jobs.len(), 2);
        assert_eq!(jobs[0].run_id, "run-1");
        assert_eq!(jobs[0].job_name, "deja-replay-run-1");
        assert_eq!(jobs[0].verdict, Some(true));
        assert_eq!(jobs[1].run_id, "run-2");
        assert_eq!(jobs[1].verdict, None);
    }

    #[test]
    fn run_jobs_from_items_skips_jobs_without_the_label() {
        // A Job carrying no run-id label is not one of ours — skip it.
        let items = vec![json!({
            "metadata": { "name": "some-other-job", "labels": { "app": "unrelated" } },
            "status": { "succeeded": 1 }
        })];
        let jobs = run_jobs_from_items(&items, RUN_ID_LABEL);
        assert!(jobs.is_empty());
    }
}
