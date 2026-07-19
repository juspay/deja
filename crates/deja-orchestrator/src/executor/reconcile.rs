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
use std::time::Duration;

use serde_json::Value;

use super::config::K8sExecutorConfig;
use super::k8s::{
    job_terminal_verdict, InClusterConfig, KubeApi, KubeTransport, UreqTransport,
};
use super::launch::RUN_ID_LABEL;
use deja_store::Store;

/// How often the reconciler runs a pass (override: `DEJA_RECONCILE_INTERVAL_SECS`).
const DEFAULT_INTERVAL_SECS: u64 = 30;

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
            })
        })
        .collect()
}

/// The pure reconcile decision: given the non-terminal runs and the live Jobs,
/// classify each run into exactly one [`ReconcileAction`]. No I/O, no clock —
/// ages are supplied on `runs`, the grace threshold is a parameter — so the
/// classification is fully unit-testable.
pub fn reconcile_decisions(
    runs: &[ReconcileRun],
    jobs: &[RunJob],
    grace: Duration,
) -> Vec<ReconcileAction> {
    let by_run: HashMap<&str, &RunJob> =
        jobs.iter().map(|j| (j.run_id.as_str(), j)).collect();

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
    eprintln!(
        "deja-orchestrator: k8s reconciler started (jobs ns {}, every {}s, orphan grace {}s)",
        cfg.jobs_namespace,
        interval.as_secs(),
        grace.as_secs()
    );
    tokio::spawn(run_loop(store, api, cfg.jobs_namespace, interval, grace));
}

/// The live loop: one pass, then sleep, forever. The blocking kube LIST is the
/// only untested part; the classification it feeds is [`reconcile_decisions`].
async fn run_loop<T>(
    store: Arc<Store>,
    api: Arc<KubeApi<T>>,
    jobs_namespace: String,
    interval: Duration,
    grace: Duration,
) where
    T: KubeTransport + Send + Sync + 'static,
{
    loop {
        reconcile_pass(&store, &api, &jobs_namespace, grace).await;
        tokio::time::sleep(interval).await;
    }
}

/// One reconcile pass. Fail-loud: reports what it did (or why it could not) on
/// every pass. Any store/kube failure aborts THIS pass only — the next tick
/// retries.
async fn reconcile_pass<T>(
    store: &Store,
    api: &Arc<KubeApi<T>>,
    jobs_namespace: &str,
    grace: Duration,
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
    let ns = jobs_namespace.to_owned();
    let items = match tokio::task::spawn_blocking(move || {
        api_for_list.list_jobs(&ns, RUN_ID_LABEL)
    })
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
    match store.update_run_state(run_id, state, failure.as_ref()).await {
        Ok(()) => eprintln!("reconcile: settled {run_id} -> {state} ({reason})"),
        Err(e) => eprintln!("reconcile: settle {run_id} -> {state} failed: {e}"),
    }
}

/// Read a u64 seconds knob from the environment, falling back to `default` when
/// unset or unparseable.
fn env_secs(key: &str, default: u64) -> u64 {
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
        }
    }

    const GRACE: Duration = Duration::from_secs(300);

    #[test]
    fn completed_job_reports_ok() {
        let actions =
            reconcile_decisions(&[run("run-1", Duration::ZERO)], &[job("run-1", Some(true))], GRACE);
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
        let actions =
            reconcile_decisions(&[run("run-3", Duration::ZERO)], &[job("run-3", None)], GRACE);
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
