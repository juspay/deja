//! The run scheduler — decides WHEN a replay run starts, so a burst of
//! requests does not become a burst of running pods.
//!
//! `POST /runs` used to create a Job that started immediately, so "how many
//! replays run at once" was never a decision this process made: it was whatever
//! the cluster would take. What the cluster will take is small and hard. The
//! replay Jobs carry a REQUIRED pod anti-affinity (one replay pod per node) and
//! the pool has a CPU limit, so the ceiling is a handful of concurrent runs.
//!
//! The queue is kubernetes' own, not one kept beside it. Every run gets its Job
//! at request time, created SUSPENDED: it holds the whole run — image, env,
//! tape, bundle — starts no pod, and is visible to anyone with `kubectl get
//! jobs`. This loop resumes them, oldest first, while the number of running
//! Jobs is below the configured capacity.
//!
//! **Why suspended rather than not-yet-created.** `activeDeadlineSeconds` is
//! measured from the Job's start time, and the API reference is explicit that
//! suspending resets it: *"If a Job is suspended (at creation or through an
//! update), this timer will effectively be stopped and reset when the Job is
//! resumed again."* A queue of un-created Jobs would need its own durable
//! record, its own rebuild-after-restart path, and an exception in the
//! reconciler for runs that have no Job yet. A queue of suspended Jobs needs
//! none of those: the Job IS the record, it outlives this process, and "no Job"
//! goes on meaning what it always meant.
//!
//! It is also the on-ramp to a real queueing controller — Kueue admits work by
//! resuming suspended Jobs, so adopting it later replaces this loop rather than
//! the launch path.
//!
//! GENERIC: like the reconciler, this deals only in run ids, the launcher's
//! run-id label, and Job state. It knows nothing about any candidate.

use std::sync::Arc;
use std::time::Duration;

use super::config::K8sExecutorConfig;
use super::k8s::{InClusterConfig, KubeApi, KubeTransport, UreqTransport};
use super::launch::{resume_job, RUN_ID_LABEL};
use super::reconcile::{env_secs, run_jobs_from_items, ReconcileRun, RunJob};
use crate::lifecycle::StoreCtx;
use deja_store::Store;

/// How often the loop runs a pass (override: `DEJA_SCHEDULER_INTERVAL_SECS`).
///
/// Shorter than the reconciler's: this one decides how quickly a freed slot is
/// filled, and a replay's own work is measured in seconds.
const DEFAULT_INTERVAL_SECS: u64 = 10;

/// How long a run may stay queued before it is failed (override:
/// `DEJA_QUEUE_MAX_WAIT_SECS`). Matches the Job's own `activeDeadlineSeconds`
/// ceiling: a run that has waited as long as a run is allowed to TAKE has
/// waited long enough. Without this a pool that never frees a slot — every node
/// held by something that will not finish — would queue silently forever.
const DEFAULT_MAX_WAIT_SECS: u64 = 7200;

/// How many runs may be running at once — `DEJA_MAX_CONCURRENT_RUNS`.
///
/// Absent or `0` means NO scheduling, which is what this process did before:
/// `POST /runs` creates a Job that starts immediately. That is the default on
/// purpose — a queue that appears because a binary was upgraded would change
/// the meaning of every deployment that never asked for one. The environment
/// that has a ceiling declares it.
pub fn capacity_from_env() -> usize {
    std::env::var("DEJA_MAX_CONCURRENT_RUNS")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

/// What the scheduler decides about one queued run. Every variant carries a
/// human `reason`: a run that is waiting must be able to say what it waits for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchedulerAction {
    /// Resume the run's suspended Job — start it now.
    Start {
        run_id: String,
        job_name: String,
        reason: String,
    },
    /// Leave it queued this pass.
    Wait { run_id: String, reason: String },
    /// It has waited past the deadline; fail it and delete its Job.
    Expire {
        run_id: String,
        job_name: String,
        reason: String,
    },
}

impl SchedulerAction {
    fn reason(&self) -> &str {
        match self {
            SchedulerAction::Start { reason, .. }
            | SchedulerAction::Wait { reason, .. }
            | SchedulerAction::Expire { reason, .. } => reason,
        }
    }
}

/// The pure scheduling decision: given the non-terminal runs (oldest first, as
/// the store returns them), their Jobs, the capacity and the queue deadline,
/// say what to do with each QUEUED run. No I/O, no clock — ages are supplied,
/// the deadline is a parameter — so the policy is unit-tested with no cluster.
///
/// A slot is held by a Job that is running: not suspended, and not yet at a
/// verdict. That is deliberately a statement about Jobs rather than runs — the
/// Job is what holds the node, and it keeps holding it while its pod lingers
/// after the runner exits. Counting runs would admit into capacity that is not
/// free.
pub fn schedule(
    runs: &[ReconcileRun],
    jobs: &[RunJob],
    capacity: usize,
    max_wait: Duration,
) -> Vec<SchedulerAction> {
    let running = jobs
        .iter()
        .filter(|j| j.verdict.is_none() && !j.suspended)
        .count();
    let mut slots = capacity.saturating_sub(running);

    runs.iter()
        .filter_map(|run| {
            let job = jobs
                .iter()
                .find(|j| j.run_id == run.run_id && j.suspended && j.verdict.is_none())?;
            Some((run, job))
        })
        .enumerate()
        .map(|(position, (run, job))| {
            if run.age >= max_wait {
                SchedulerAction::Expire {
                    run_id: run.run_id.clone(),
                    job_name: job.job_name.clone(),
                    reason: format!(
                        "queued {}s without a free slot (deadline {}s, {running} of {capacity} \
                         running) — failing rather than letting the run wait forever",
                        run.age.as_secs(),
                        max_wait.as_secs()
                    ),
                }
            } else if slots > 0 {
                slots -= 1;
                SchedulerAction::Start {
                    run_id: run.run_id.clone(),
                    job_name: job.job_name.clone(),
                    reason: format!(
                        "starting after {}s queued ({running} of {capacity} running)",
                        run.age.as_secs()
                    ),
                }
            } else {
                SchedulerAction::Wait {
                    run_id: run.run_id.clone(),
                    reason: format!(
                        "queued {}s at position {} ({running} of {capacity} running)",
                        run.age.as_secs(),
                        position + 1
                    ),
                }
            }
        })
        .collect()
}

/// Spawn the scheduler loop. Does nothing when `capacity == 0` (no scheduling —
/// Jobs are created running), and says so, because a scheduler that is silently
/// absent is indistinguishable from one that is stuck.
pub fn spawn(
    store: Arc<Store>,
    incluster: InClusterConfig,
    cfg: K8sExecutorConfig,
    capacity: usize,
) {
    if capacity == 0 {
        eprintln!(
            "deja-orchestrator: run scheduler disabled (DEJA_MAX_CONCURRENT_RUNS unset or 0) \
             — Jobs start on create"
        );
        return;
    }
    let transport = match UreqTransport::new(&incluster) {
        Ok(t) => t,
        Err(e) => {
            eprintln!(
                "deja-orchestrator: run scheduler NOT started — cannot build kube client: {e}. \
                 Queued runs will stay suspended until the queue deadline expires them"
            );
            return;
        }
    };
    let api = Arc::new(KubeApi::new(transport));
    let interval = Duration::from_secs(env_secs(
        "DEJA_SCHEDULER_INTERVAL_SECS",
        DEFAULT_INTERVAL_SECS,
    ));
    let max_wait = Duration::from_secs(env_secs("DEJA_QUEUE_MAX_WAIT_SECS", DEFAULT_MAX_WAIT_SECS));
    eprintln!(
        "deja-orchestrator: run scheduler started (capacity {capacity} concurrent run(s), \
         jobs ns {}, every {}s, queue deadline {}s)",
        cfg.jobs_namespace,
        interval.as_secs(),
        max_wait.as_secs()
    );
    tokio::spawn(run_loop(
        store, api, incluster, cfg, capacity, max_wait, interval,
    ));
}

#[allow(clippy::too_many_arguments)]
async fn run_loop<T>(
    store: Arc<Store>,
    api: Arc<KubeApi<T>>,
    incluster: InClusterConfig,
    cfg: K8sExecutorConfig,
    capacity: usize,
    max_wait: Duration,
    interval: Duration,
) where
    T: KubeTransport + Send + Sync + 'static,
{
    loop {
        scheduler_pass(&store, &api, &incluster, &cfg, capacity, max_wait).await;
        tokio::time::sleep(interval).await;
    }
}

/// One scheduling pass. Any store/kube failure aborts THIS pass only — the next
/// tick retries, and the queue is re-derived then, so nothing is lost by
/// skipping one.
async fn scheduler_pass<T>(
    store: &Arc<Store>,
    api: &Arc<KubeApi<T>>,
    incluster: &InClusterConfig,
    cfg: &K8sExecutorConfig,
    capacity: usize,
    max_wait: Duration,
) where
    T: KubeTransport + Send + Sync + 'static,
{
    let active = match store.list_active_runs().await {
        Ok(a) => a,
        Err(e) => {
            eprintln!("scheduler: list_active_runs failed: {e} — skipping this pass");
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
            eprintln!("scheduler: list_jobs failed: {e} — skipping this pass");
            return;
        }
        Err(e) => {
            eprintln!("scheduler: list_jobs task join failed: {e} — skipping this pass");
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

    let actions = schedule(&runs, &jobs, capacity, max_wait);
    if actions.is_empty() {
        return; // no queued runs — every live Job is already running
    }
    let (mut started, mut waiting, mut expired) = (0u32, 0u32, 0u32);
    for action in &actions {
        match action {
            SchedulerAction::Start {
                run_id,
                job_name,
                reason,
            } => {
                started += 1;
                start_run(api, incluster, cfg, run_id, job_name, reason, store).await;
            }
            SchedulerAction::Wait { .. } => waiting += 1,
            SchedulerAction::Expire {
                run_id,
                job_name,
                reason,
            } => {
                expired += 1;
                expire_run(api, cfg, store, run_id, job_name, reason).await;
            }
        }
    }
    eprintln!(
        "scheduler: pass over {} queued run(s): started {started}, waiting {waiting}, \
         expired {expired} (capacity {capacity}); {}",
        actions.len(),
        actions
            .iter()
            .map(SchedulerAction::reason)
            .collect::<Vec<_>>()
            .join("; ")
    );
}

/// Resume one Job and take up watching it.
///
/// The resume is the whole start: the Job already holds everything the run
/// needs. The watcher that follows is the same infra safety net a direct launch
/// has — it reports an image-pull failure or an OOM that the runner itself
/// never gets to report.
async fn start_run<T>(
    api: &Arc<KubeApi<T>>,
    incluster: &InClusterConfig,
    cfg: &K8sExecutorConfig,
    run_id: &str,
    job_name: &str,
    reason: &str,
    store: &Arc<Store>,
) where
    T: KubeTransport + Send + Sync + 'static,
{
    let api_for_resume = api.clone();
    let (ns, job) = (cfg.jobs_namespace.clone(), job_name.to_owned());
    let resumed = tokio::task::spawn_blocking(move || resume_job(&api_for_resume, &ns, &job)).await;
    match resumed {
        Ok(Ok(())) => {
            // The run's own log, written through the ASYNC store api — never
            // through `StoreCtx`, whose emit blocks on the runtime handle and
            // therefore PANICS when called from inside the runtime. A panic
            // here takes the whole scheduler task with it and every queued run
            // stays suspended forever. Not hypothetical: measured on sbx, on
            // the first resume that succeeded.
            log_line(store, run_id, reason).await;
            // Built only to hand to the watcher, which uses it from a plain
            // thread, where blocking is what it is designed for.
            let ctx = StoreCtx::new(
                run_id,
                Some((tokio::runtime::Handle::current(), store.clone())),
            );
            crate::api::runs::watch_k8s_run(run_id, job_name, ctx, incluster.clone(), cfg.clone());
        }
        // A resume that fails is NOT terminal for the run: the Job is still
        // there, still suspended, and the next pass tries again. Only the queue
        // deadline ends it, which is the one bound that must not be bypassed by
        // a transient apiserver error. Measured on sbx: fifteen runs sat through
        // ~90s of 403s and then started themselves the moment the permission
        // landed, having burned no deadline and held no node.
        Ok(Err(e)) => eprintln!(
            "scheduler: {run_id}: resume {job_name} failed: {e} — will retry{}",
            missing_patch_hint(&e)
        ),
        Err(e) => eprintln!("scheduler: {run_id}: resume task join failed: {e} — will retry"),
    }
}

/// Name the permission when a resume is refused.
///
/// Creating a Job suspended needs `create`; RESUMING it needs `patch`, which
/// nothing in this orchestrator required before the scheduler existed — so an
/// environment upgraded to a scheduling build has a Role that lets it queue
/// work it can never start. That reads as a bare 403 per run per pass, which
/// says nothing about the verb that is missing. It cost a diagnosis on sbx
/// before this line existed; say it in the log instead.
fn missing_patch_hint(err: &super::launch::ExecutorError) -> &'static str {
    match err {
        super::launch::ExecutorError::Kube(super::k8s::KubeError::Api { status: 403, .. }) => {
            " — the orchestrator's Role is missing `patch` on batch/jobs, which is what resumes \
             a suspended Job; every queued run will stay queued until it is granted"
        }
        _ => "",
    }
}

/// Fail a run that waited past the deadline, and delete the Job holding its
/// place. Deleting is what keeps an expired run from being resumed later by a
/// pass that no longer remembers why it was queued.
async fn expire_run<T>(
    api: &Arc<KubeApi<T>>,
    cfg: &K8sExecutorConfig,
    store: &Arc<Store>,
    run_id: &str,
    job_name: &str,
    reason: &str,
) where
    T: KubeTransport + Send + Sync + 'static,
{
    let api_for_delete = api.clone();
    let (ns, job) = (cfg.jobs_namespace.clone(), job_name.to_owned());
    let deleted = tokio::task::spawn_blocking(move || api_for_delete.delete_job(&ns, &job)).await;
    if let Ok(Err(e)) = deleted {
        eprintln!("scheduler: {run_id}: delete expired job {job_name}: {e}");
    }
    log_line(store, run_id, reason).await;
    // Same rule: settle through the async store, never a blocking StoreCtx.
    // `update_run_state` is terminal-guarded, so this is a no-op if the run
    // somehow reported for itself first.
    let failure = serde_json::json!({ "message": reason });
    match store
        .update_run_state(run_id, "failed", Some(&failure))
        .await
    {
        Ok(()) => eprintln!("scheduler: settled {run_id} -> failed ({reason})"),
        Err(e) => eprintln!("scheduler: settle {run_id} -> failed: {e}"),
    }
}

/// Append one line to a run's log from async code.
///
/// Best-effort and never fatal: a scheduler that cannot write a log line must
/// still start the run. Sequence 0 because this is one line per run, written
/// before the runner has written any of its own.
async fn log_line(store: &Arc<Store>, run_id: &str, line: &str) {
    if let Err(e) = store.append_log(run_id, "scheduler", 0, line).await {
        eprintln!("scheduler: {run_id}: log write failed: {e}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAPACITY: usize = 2;
    const MAX_WAIT: Duration = Duration::from_secs(7200);

    fn run(run_id: &str, age_secs: u64) -> ReconcileRun {
        ReconcileRun {
            run_id: run_id.to_owned(),
            state: "pending".to_owned(),
            age: Duration::from_secs(age_secs),
        }
    }

    fn job(run_id: &str, suspended: bool, verdict: Option<bool>) -> RunJob {
        RunJob {
            job_name: format!("deja-replay-{run_id}"),
            run_id: run_id.to_owned(),
            verdict,
            suspended,
        }
    }

    fn started(actions: &[SchedulerAction]) -> Vec<&str> {
        actions
            .iter()
            .filter_map(|a| match a {
                SchedulerAction::Start { run_id, .. } => Some(run_id.as_str()),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_free_pool_starts_every_queued_run() {
        let runs = [run("a", 5), run("b", 3)];
        let jobs = [job("a", true, None), job("b", true, None)];
        assert_eq!(
            started(&schedule(&runs, &jobs, CAPACITY, MAX_WAIT)),
            ["a", "b"]
        );
    }

    /// The point of the whole module: past capacity, a run waits as a SUSPENDED
    /// Job — no pod, and no deadline running against it.
    #[test]
    fn a_full_pool_starts_nothing() {
        let runs = [run("c", 60)];
        let jobs = [
            job("a", false, None),
            job("b", false, None),
            job("c", true, None),
        ];
        let actions = schedule(&runs, &jobs, CAPACITY, MAX_WAIT);
        assert!(started(&actions).is_empty(), "{actions:?}");
        let SchedulerAction::Wait { reason, .. } = &actions[0] else {
            panic!("expected a wait: {actions:?}");
        };
        assert!(
            reason.contains("position 1") && reason.contains("2 of 2"),
            "a queued run must say what it waits for and where it stands: {reason}"
        );
    }

    /// Oldest first, in the order the store returns (ORDER BY created_at). A
    /// freed slot belongs to the run that has waited longest.
    #[test]
    fn scheduling_is_oldest_first() {
        let runs = [run("oldest", 900), run("middle", 60), run("newest", 1)];
        let jobs = [
            job("oldest", true, None),
            job("middle", true, None),
            job("newest", true, None),
        ];
        assert_eq!(started(&schedule(&runs, &jobs, 1, MAX_WAIT)), ["oldest"]);
    }

    /// A slot is held for the Job's whole life, including the stretch after the
    /// runner exits while a lingering container keeps the node. A run counted
    /// instead of its Job would free the slot at the verdict and start another
    /// into capacity that is still held.
    #[test]
    fn a_running_job_without_a_verdict_holds_its_slot() {
        let runs = [run("queued", 10)];
        let jobs = [job("running", false, None), job("queued", true, None)];
        assert!(started(&schedule(&runs, &jobs, 1, MAX_WAIT)).is_empty());
    }

    #[test]
    fn a_finished_job_frees_its_slot() {
        let runs = [run("queued", 10)];
        let jobs = [
            job("done", false, Some(true)),
            job("failed", false, Some(false)),
            job("queued", true, None),
        ];
        assert_eq!(started(&schedule(&runs, &jobs, 1, MAX_WAIT)), ["queued"]);
    }

    /// The loop keeps no memory between passes: it sees its own past starts
    /// only as Jobs that are no longer suspended. Without that, every pass would
    /// start the same run again — the failure a stateless scheduler has to be
    /// immune to by construction.
    #[test]
    fn a_resumed_job_is_never_started_again() {
        let runs = [run("a", 30)];
        let actions = schedule(&runs, &[job("a", false, None)], 8, MAX_WAIT);
        assert!(actions.is_empty(), "not even a queue entry: {actions:?}");
    }

    /// A run with no Job at all belongs to the reconciler (its launch may be in
    /// flight, or its Job was deleted), never to the scheduler: there is nothing
    /// to resume, and inventing one would drive the same recording twice.
    #[test]
    fn a_run_with_no_job_is_not_a_queue_entry() {
        assert!(schedule(&[run("gone", 400)], &[], 8, MAX_WAIT).is_empty());
    }

    /// The bound. A pool where no slot ever frees — every node held by something
    /// that will not finish — must not queue silently forever.
    #[test]
    fn a_run_queued_past_the_deadline_expires() {
        let runs = [run("old", 7201)];
        let jobs = [
            job("a", false, None),
            job("b", false, None),
            job("old", true, None),
        ];
        let actions = schedule(&runs, &jobs, CAPACITY, MAX_WAIT);
        let SchedulerAction::Expire {
            job_name, reason, ..
        } = &actions[0]
        else {
            panic!("expected Expire: {actions:?}");
        };
        assert_eq!(job_name, "deja-replay-old");
        assert!(
            reason.contains("queued 7201s") && reason.contains("deadline 7200s"),
            "the failure must name the wait: {reason}"
        );
    }

    /// Expiry outranks a free slot: a run that has already waited past the
    /// deadline is failed rather than started, because starting it would give it
    /// a full activeDeadlineSeconds on top of the wait.
    #[test]
    fn an_expired_run_is_not_started_even_when_a_slot_is_free() {
        let runs = [run("old", 7201)];
        let actions = schedule(&runs, &[job("old", true, None)], 8, MAX_WAIT);
        assert!(
            matches!(&actions[0], SchedulerAction::Expire { .. }),
            "{actions:?}"
        );
    }

    /// The 403 that cost a diagnosis on sbx: the orchestrator could create a
    /// suspended Job and could not resume it, because `patch` on batch/jobs is a
    /// verb nothing needed before the scheduler existed. A bare "403 Forbidden"
    /// per run per pass does not say which verb is missing, so the log says it.
    #[test]
    fn a_refused_resume_names_the_permission_it_needs() {
        let forbidden =
            super::super::launch::ExecutorError::Kube(super::super::k8s::KubeError::Api {
                status: 403,
                reason: "Forbidden".into(),
            });
        let hint = missing_patch_hint(&forbidden);
        assert!(
            hint.contains("patch") && hint.contains("batch/jobs"),
            "a refused resume must name the verb and the resource: {hint}"
        );

        // Anything else is not a permission problem and must not be reported as
        // one — a transport blip that claimed the Role was wrong would send the
        // reader to edit RBAC that is already correct.
        for other in [
            super::super::launch::ExecutorError::Kube(super::super::k8s::KubeError::Api {
                status: 500,
                reason: "server error".into(),
            }),
            super::super::launch::ExecutorError::Kube(super::super::k8s::KubeError::Transport(
                "connection reset".into(),
            )),
        ] {
            assert_eq!(missing_patch_hint(&other), "", "{other:?}");
        }
    }

    /// Capacity 0 is how scheduling is turned off, so it must start nothing even
    /// if the loop is somehow running. (`spawn` refuses to start it, which is
    /// the real guard; this keeps the two from disagreeing.)
    #[test]
    fn capacity_zero_starts_nothing() {
        let runs = [run("a", 5)];
        assert!(started(&schedule(&runs, &[job("a", true, None)], 0, MAX_WAIT)).is_empty());
    }
}
