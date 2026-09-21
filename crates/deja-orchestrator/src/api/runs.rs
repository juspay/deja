//! Run lifecycle endpoints — create, fetch status.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::executor::{
    collect_pod_diagnostics, launch, launch_spec_for_run, watch_to_terminal, InClusterConfig,
    K8sExecutorConfig, KubeApi, LaunchSpec, UreqTransport,
};
use crate::lifecycle::StoreCtx;
use crate::{
    new_id, read_json, replay_run_id, run_id_stamp, write_json, CandidateSpec, HarnessRoot, Run,
    RunMode, RunSpec, RunStatus,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRunResponse {
    pub run_id: String,
    pub status: RunStatus,
}

/// The id a run is addressed by, composed from what identifies it.
///
/// A replay names what it drove: environment, candidate ref, recording, and
/// when. So the id can be searched — every run of a candidate, or of a
/// recording, is a substring away — and a link says what it points at without
/// being opened.
///
/// The candidate ref is taken AS DECLARED rather than resolved. The id is
/// minted here, at request time, before the executor has resolved a tag to a
/// registry path or a digest; and resolution reads process env, so the same
/// request would mint different ids in two deployments. The resolved image is
/// recorded on the run when the executor learns it, which is where a tag that
/// moved becomes visible.
///
/// A record run has no recording to name yet — it is producing one — so it
/// keeps the plain time-based id.
fn run_id_for(spec: &RunSpec) -> String {
    if spec.mode != RunMode::Replay {
        return new_id("run");
    }
    let candidate = match &spec.candidate_spec {
        CandidateSpec::PrebuiltImage { image } => image
            .rsplit('/')
            .next()
            .and_then(|name| name.rsplit(':').next())
            .unwrap_or(image)
            .to_owned(),
        CandidateSpec::RepoSha { sha, .. } => sha.clone(),
        CandidateSpec::RepoBranch { branch, .. } => branch.clone(),
        CandidateSpec::RepoPr { pr, .. } => format!("pr{pr}"),
        CandidateSpec::LocalPath { .. } => "local".to_owned(),
    };
    // What this run drives, whichever way it was named. A GROUP is a name, not
    // an absence: it says "every recording this revision wrote that day", and
    // the worker resolves it to members at pull time exactly as an `s3_source`
    // prefix resolves to a session.
    //
    // Reading `recording_id` alone made every group replay address itself
    // `…-unresolved-…`, because a group arrives in `recording_group` and leaves
    // the other field empty. That is the same shape as three other checks that
    // asked about `recording_id` specifically after a second way to name a
    // recording existed — the run's submit gate, the pull dispatch, and here.
    //
    // `unresolved` stays for the case it was written for: an `s3_source` with
    // no session filter, where the recording genuinely is not known until the
    // worker scans the prefix.
    let recording = spec
        .recording_group
        .as_deref()
        .or(spec.recording_id.as_deref())
        .unwrap_or("unresolved");
    replay_run_id(
        &std::env::var("DEJA_ENV").unwrap_or_else(|_| "dev".to_owned()),
        &candidate,
        recording,
        &run_id_stamp(),
    )
}

/// Build and persist a Pending run record (no worker yet). The caller is
/// responsible for inserting the store row (if a store is connected) BEFORE
/// spawning the worker — stage rows reference the run row by foreign key.
pub fn persist_new(root: &HarnessRoot, spec: RunSpec) -> std::io::Result<Run> {
    let run_id = run_id_for(&spec);
    let run = Run {
        run_id: run_id.clone(),
        spec,
        status: RunStatus::Pending,
        recording_id: None,
        candidate_image: None,
        failure_reason: None,
        stage: Some("queued".to_owned()),
        step: 0,
        steps_total: 0,
        stage_updated_ms: crate::now_ms(),
    };
    write_json(&root.run_path(&run_id), &run)?;
    Ok(run)
}

/// Spawn the lifecycle worker for an already-persisted run.
///
/// The worker drives the run asynchronously (compose up → record/replay →
/// score → tear down) on a background thread, persisting progress to the
/// file store and (via `ctx`) the Postgres store.
pub fn spawn_worker(root: &HarnessRoot, run_id: &str, ctx: StoreCtx) {
    let root_path = root.root.clone();
    let worker_run_id = run_id.to_owned();
    std::thread::spawn(move || match HarnessRoot::new(&root_path) {
        Ok(root) => crate::lifecycle::drive(&root, &worker_run_id, &ctx),
        Err(e) => eprintln!(
            "lifecycle: cannot open HarnessRoot {}: {e}",
            root_path.display()
        ),
    });
}

/// Fill a `DEJA_CANDIDATE_TARBALL_URL` template: `{sha}` always, `{repo}` from
/// the run's `candidate_repo` (a per-run parameter) or the `DEJA_CANDIDATE_REPO`
/// default. Returns None (logged) if the template still needs a repo that none
/// was supplied for — better to fall back to a local checkout than to fetch a
/// malformed URL.
pub(crate) fn resolve_tarball_url(
    template: &str,
    run_repo: Option<&str>,
    sha: &str,
) -> Option<String> {
    let url = template.replace("{sha}", sha);
    if !url.contains("{repo}") {
        return Some(url);
    }
    // A blank per-run repo (an empty form field) is treated as "not provided" so
    // it falls through to the DEJA_CANDIDATE_REPO default — never a `//tar.gz`.
    let repo = run_repo
        .map(str::trim)
        .filter(|r| !r.is_empty())
        .map(str::to_owned)
        .or_else(|| std::env::var("DEJA_CANDIDATE_REPO").ok())
        .map(|r| r.trim().to_owned())
        .filter(|r| !r.is_empty());
    match repo {
        Some(r) => Some(url.replace("{repo}", &r)),
        None => {
            eprintln!(
                "codebundle: tarball template needs {{repo}} but no candidate_repo / \
                 DEJA_CANDIDATE_REPO for {sha}"
            );
            None
        }
    }
}

/// Resolve the candidate's migration bundle: stage the tar to S3 (so the Job's
/// initContainer can pull the candidate's own migrations — Option B) and return
/// its manifest, the P1 gate's *expected* set. Everything is a function of the
/// candidate ref; nothing is guessed, and every branch is logged. Two producers,
/// tried in order:
///   1. `DEJA_CANDIDATE_TARBALL_URL` — a codeload-style `…/tar.gz/{sha}` template
///      ({sha} substituted). The orchestrator fetches the ref's repo tarball,
///      keeps migrations/, and stages the bundle: migrations = f(repo_url, sha),
///      no local checkout, no CI dependency. This is the in-cluster primary; the
///      sealed replay pod never makes this call.
///   2. `DEJA_CANDIDATE_REPO_DIR` — a local candidate git checkout (compose/dev).
///
/// Neither configured / both fail → None (P1 record-only).
fn resolve_expected_schema(run: &Run) -> Option<(crate::SchemaFingerprint, String)> {
    // Publishing a CodeBundle — the candidate repo's migrations/ (the P1 schema
    // gate) plus its config seed — is a CAPABILITY, declared as
    // `DEJA_<SYSTEM>_HAS_CODE_BUNDLE`. It was spelled `system != the default
    // system`, which is true of every system today and says the wrong thing:
    // it makes "is this the original integration" decide "does this system ship
    // migrations the harness can gate on", exactly as `manages_stores` once
    // did. Resolving a bundle for a system that has none would fetch the
    // DEFAULT repo's migrations for a foreign image sha, and arm the patch to
    // inject DEJA_CODE_BUNDLE_URI into a `migrations` initContainer that
    // system's job template deliberately does not have, failing the launch on
    // ContainerNotFound. The default when undeclared is the previous behaviour
    // exactly, so this is behaviour preserving and declarable from now on.
    let system = run.spec.system();
    if !crate::system::system_config(system).has_code_bundle {
        eprintln!("codebundle: system '{system}' has no CodeBundle contract; skipping (P1 not applicable)");
        return None;
    }
    let (_, sha) = crate::executor::resolve_candidate_image(&run.spec.candidate_spec).ok()?;
    let s3 = crate::s3::S3Config::from_env();
    let uri = crate::codebundle::bundle_s3_uri(&s3, &sha);
    let nonempty = |s: String| (!s.trim().is_empty()).then_some(s);

    // 1) git-host tarball producer (primary). The repo is a per-run parameter
    // (candidate images can be built from any repo/fork); DEJA_CANDIDATE_REPO is
    // only the orchestrator default.
    if let Some(url) = std::env::var("DEJA_CANDIDATE_TARBALL_URL")
        .ok()
        .and_then(nonempty)
        .and_then(|tmpl| resolve_tarball_url(&tmpl, run.spec.candidate_repo.as_deref(), &sha))
    {
        match crate::codebundle::ensure_bundle_staged_from_url(&s3, &url, &sha) {
            Ok((fp, source)) => {
                // `source` is the whole point of this line: "staged" used to be
                // printed whether the object was fetched or found and returned
                // untouched, which reads as work that did not happen and sends
                // the next reader looking in the wrong place.
                eprintln!(
                    "codebundle: candidate {sha} from {url} at {uri}; {}; expects {} \
                     migrations (P1 armed)",
                    source.render(),
                    fp.count()
                );
                return Some((fp, uri));
            }
            // Fall through to the local checkout (if any) on a transient fetch
            // or a bad ref — logged, never silently ignored.
            Err(e) => eprintln!("codebundle: tarball producer failed for {sha} ({e})"),
        }
    }

    // 2) local candidate checkout (fallback).
    if let Some(repo) = std::env::var("DEJA_CANDIDATE_REPO_DIR")
        .ok()
        .and_then(nonempty)
    {
        let repo = std::path::Path::new(&repo);
        match crate::codebundle::ensure_bundle_staged(&s3, repo, &sha) {
            Ok((fp, source)) => {
                eprintln!(
                    "codebundle: candidate {sha} from local checkout at {uri}; {}; expects \
                     {} migrations (P1 armed)",
                    source.render(),
                    fp.count()
                );
                return Some((fp, uri));
            }
            // S3 write may be denied/misconfigured; still arm P1 from the
            // manifest. The initContainer then fails loudly on the missing
            // object — the right place for that, not a silent wrong-schema seed.
            Err(stage_err) => match crate::codebundle::manifest_from_repo(repo, &sha) {
                Ok(fp) => {
                    eprintln!(
                        "codebundle: {sha} manifest ok ({} migrations, P1 armed) but bundle NOT \
                         staged: {stage_err}",
                        fp.count()
                    );
                    return Some((fp, uri));
                }
                Err(e) => eprintln!("codebundle: local manifest failed for {sha} ({e})"),
            },
        }
    }

    eprintln!("codebundle: no producer resolved a bundle for {sha}; P1 record-only");
    None
}

/// Launch a run as a k8s Job and watch it to a terminal state.
///
/// The in-Job runner reports its own stages + Finish through push-back (the
/// `/events` ingest), so this thread does NOT drive the run — it is the infra
/// safety net: an image-pull failure, OOM, or a pod that never runs the runner
/// would otherwise leave the run hanging forever. When the Job reaches a
/// terminal state this reports it via `ctx`; the terminal-guard (V4) makes that
/// a no-op when the runner already reported its own verdict.
pub fn spawn_k8s_run(
    root: &HarnessRoot,
    run: Run,
    ctx: StoreCtx,
    incluster: InClusterConfig,
    cfg: K8sExecutorConfig,
) {
    spawn_k8s_job(root, run, ctx, incluster, cfg, false);
}

/// Create a run's Job SUSPENDED and stop there — the scheduler starts it.
///
/// The Job is built in full at request time (candidate image resolved, bundle
/// staged, env patched) so that a queued run is a real, inspectable object
/// rather than an intention: `kubectl get jobs` shows the queue, and a bad
/// candidate ref fails now instead of after the wait. Nothing watches it yet,
/// because there is nothing to watch until it is resumed.
pub fn spawn_k8s_run_queued(
    root: &HarnessRoot,
    run: Run,
    ctx: StoreCtx,
    incluster: InClusterConfig,
    cfg: K8sExecutorConfig,
) {
    spawn_k8s_job(root, run, ctx, incluster, cfg, true);
}

/// Watch an already-running Job to its terminal state. Used by the scheduler
/// once it resumes a suspended Job, so a scheduled run gets the same infra
/// safety net a direct launch has.
pub fn watch_k8s_run(
    run_id: &str,
    job_name: &str,
    ctx: StoreCtx,
    incluster: InClusterConfig,
    cfg: K8sExecutorConfig,
) {
    let (run_id, job_name) = (run_id.to_owned(), job_name.to_owned());
    std::thread::spawn(move || {
        let api = match UreqTransport::new(&incluster) {
            Ok(t) => KubeApi::new(t),
            Err(e) => return ctx.finish(false, Some(&format!("k8s client: {e}"))),
        };
        watch_job_to_finish(&api, &cfg, &run_id, &job_name, &ctx);
    });
}

fn spawn_k8s_job(
    root: &HarnessRoot,
    run: Run,
    ctx: StoreCtx,
    incluster: InClusterConfig,
    cfg: K8sExecutorConfig,
    suspend: bool,
) {
    let root_path = root.root.clone();
    std::thread::spawn(move || {
        // Fail the run cleanly if the control plane's own state root is unusable.
        // Nothing is derived FROM it here: a run's artifacts live on the pod's
        // shared mount, and the executor derives their paths there, so the
        // candidate is never handed a path from this filesystem.
        if let Err(e) = HarnessRoot::new(&root_path) {
            return ctx.finish(false, Some(&format!("state root: {e}")));
        }
        //
        // A1/P1: resolve the candidate's migration bundle from its ref — the
        // expected set (runner refuses on drift) AND the S3 URI the migrations
        // initContainer pulls the candidate's own migrations from. Absent →
        // record-only. Always logged.
        let bundle = resolve_expected_schema(&run);
        let (expected, uri) = match &bundle {
            Some((fp, u)) => (Some(fp), Some(u.as_str())),
            None => (None, None),
        };
        let spec = match launch_spec_for_run(&run, &cfg, expected, uri) {
            // Suspended: the Job exists and holds the run, but starts no pod
            // until the scheduler resumes it. `activeDeadlineSeconds` does not
            // run while it waits.
            Ok(s) => LaunchSpec { suspend, ..s },
            Err(e) => {
                ctx.log("launch", &format!("build launch spec failed: {e}"));
                return ctx.finish(false, Some(&format!("build launch spec: {e}")));
            }
        };
        let api = match UreqTransport::new(&incluster) {
            Ok(t) => KubeApi::new(t),
            Err(e) => return ctx.finish(false, Some(&format!("k8s client: {e}"))),
        };
        let name = match launch(&api, &spec) {
            Ok(n) => {
                ctx.log(
                    "launch",
                    &format!(
                        "created {}Job {n} in namespace {}",
                        if suspend { "suspended " } else { "" },
                        cfg.jobs_namespace
                    ),
                );
                n
            }
            Err(e) => return ctx.finish(false, Some(&format!("launch job: {e}"))),
        };
        if suspend {
            // Queued. The scheduler resumes it and takes up the watch; watching
            // a suspended Job here would hold a thread for the whole wait and
            // report "no terminal state" for a run that has not started.
            return;
        }
        watch_job_to_finish(&api, &cfg, &run.run_id, &name, &ctx);
    });
}

/// Poll a Job to its terminal state and report it.
///
/// The in-Job runner reports its own stages + Finish through push-back (the
/// `/events` ingest), so this does NOT drive the run — it is the infra safety
/// net: an image-pull failure, OOM, or a pod that never runs the runner would
/// otherwise leave the run hanging forever. The terminal-guard (V4) makes the
/// report a no-op when the runner already reported its own verdict.
fn watch_job_to_finish<T: crate::executor::KubeTransport>(
    api: &KubeApi<T>,
    cfg: &K8sExecutorConfig,
    run_id: &str,
    name: &str,
    ctx: &StoreCtx,
) {
    // Poll to a terminal Job state. The Job's own activeDeadlineSeconds is
    // the authoritative timeout and the watch reads it off the Job, so the
    // two cannot drift; this hour is only the ceiling for a template that
    // declares no deadline at all.
    match watch_to_terminal(
        api,
        &cfg.jobs_namespace,
        name,
        Duration::from_secs(5),
        Duration::from_secs(60 * 60),
        std::thread::sleep,
    ) {
        Ok(Some(true)) => ctx.finish(true, None),
        Ok(Some(false)) => {
            let cause = capture_diagnostics(api, cfg, run_id, ctx);
            ctx.finish(false, Some(&failure_line("job failed", cause.as_deref())))
        }
        Ok(None) => {
            let cause = capture_diagnostics(api, cfg, run_id, ctx);
            ctx.finish(
                false,
                Some(&failure_line(
                    "job did not reach a terminal state within the watch deadline",
                    cause.as_deref(),
                )),
            )
        }
        Err(e) => ctx.finish(false, Some(&format!("watch job: {e}"))),
    }
}

/// Pull the pod's per-container state and output into the run's log, so a failure
/// can be read from the run itself. Runs after the failure is known and before it
/// is recorded, so the diagnostics are already there when someone opens the run.
fn capture_diagnostics<T: crate::executor::KubeTransport>(
    api: &KubeApi<T>,
    cfg: &K8sExecutorConfig,
    run_id: &str,
    ctx: &StoreCtx,
) -> Option<String> {
    let mut cause = None;
    for (label, body) in
        collect_pod_diagnostics(api, &cfg.jobs_namespace, run_id, cfg.diagnostics_tail_lines)
    {
        // The container-state lines already say WHY — `terminated(OOMKilled,
        // exit 137)` and the like. They were written to the log and nowhere
        // else, so `failure_reason` said "job failed (see pod diagnostics)" and
        // an operator had to go read the log to learn the run was OOMKilled.
        // Lift the first terminal cause into the failure itself.
        if cause.is_none() && !label.ends_with(" log") {
            if let Some(reason) = terminal_cause(&label, &body) {
                cause = Some(reason);
            }
        }
        ctx.log("diagnostics", &format!("{label}: {body}"));
    }
    cause
}

/// A container-state line that names a FAILURE, reduced to `container: state`.
///
/// `describe_container_state` renders every container, healthy ones included, so
/// this picks out the ones that actually explain a failed run: a non-zero exit,
/// or a wait reason like `CrashLoopBackOff`/`ImagePullBackOff`. A clean
/// `terminated(Completed, exit 0)` is not a cause and must not be reported as
/// one — the runner exits 0 on a healthy run while its sidecars keep running.
fn terminal_cause(label: &str, body: &str) -> Option<String> {
    let interesting = body.contains("OOMKilled")
        || body.contains("Error")
        || body.contains("BackOff")
        || body.contains("Evicted")
        || (body.contains("exit ") && !body.contains("exit 0"));
    interesting.then(|| {
        let container = label.rsplit('/').next().unwrap_or(label);
        format!("{container}: {}", body.trim())
    })
}

/// Prefix the generic outcome with the cause when one was found.
fn failure_line(outcome: &str, cause: Option<&str>) -> String {
    match cause {
        Some(cause) => format!("{outcome} — {cause}"),
        None => format!("{outcome} (see pod diagnostics in the run log)"),
    }
}

/// Serialized run mode (the store's `mode` column).
pub fn mode_str(mode: RunMode) -> &'static str {
    match mode {
        RunMode::Record => "record",
        RunMode::Replay => "replay",
    }
}

/// `GET /runs/{id}` — fetch persisted run record.
pub fn get(root: &HarnessRoot, run_id: &str) -> std::io::Result<Run> {
    read_json::<Run>(&root.run_path(run_id))
}

#[cfg(test)]
mod tests {
    use super::{failure_line, resolve_tarball_url, run_id_for, terminal_cause};
    use crate::{CandidateSpec, RunMode, RunSpec};

    /// A replay spec naming nothing in particular, so each test states only the
    /// field it is about.
    fn replay_spec() -> RunSpec {
        RunSpec {
            mode: RunMode::Replay,
            candidate_spec: CandidateSpec::PrebuiltImage {
                image: "repo/router:abc123".to_owned(),
            },
            system_under_test: None,
            candidate_repo: None,
            recording_id: None,
            recording_group: None,
            correlation_filter: None,
            workload: serde_json::Value::Null,
            scored_span_namespaces: Vec::new(),
            s3_source: None,
        }
    }

    /// A run driving a DEPLOYMENT DAY addresses itself by that day.
    ///
    /// The id read `…-unresolved-…` for every group replay, because it asked
    /// `recording_id` and a group arrives in `recording_group`. "Unresolved" is
    /// a real answer for an `s3_source` whose session is not known until the
    /// worker scans the prefix; it is the wrong word for a run that named
    /// precisely what it wanted.
    #[test]
    fn a_group_replay_is_addressed_by_its_group_not_as_unresolved() {
        let mut spec = replay_spec();
        spec.recording_group = Some("f42feeb-0916".to_owned());
        let id = run_id_for(&spec);
        assert!(
            !id.contains("unresolved"),
            "a named group is not an unresolved recording: {id}"
        );
        assert!(
            id.contains("0916"),
            "the day the run drives must be in its address: {id}"
        );
    }

    /// Naming a single recording is unchanged.
    #[test]
    fn a_recording_replay_is_still_addressed_by_its_recording() {
        let mut spec = replay_spec();
        spec.recording_id = Some("rec-f42feeb-09161131-k8".to_owned());
        let id = run_id_for(&spec);
        assert!(!id.contains("unresolved"), "{id}");
        assert!(id.contains("09161131"), "{id}");
    }

    /// The address agrees with the RESOLVER about which name wins.
    ///
    /// `stage_resolve_recording` takes `recording_group.or(recording_id)`, so
    /// the id must prefer the group too. A spec naming both is refused at
    /// stage 1 — that refusal is the point, a caller that set both has not
    /// decided — but the id is minted before the refusal, and an address
    /// naming the field the resolver would NOT have used would describe a run
    /// that never existed.
    #[test]
    fn the_address_prefers_the_same_name_the_resolver_does() {
        let mut spec = replay_spec();
        spec.recording_group = Some("f42feeb-0916".to_owned());
        spec.recording_id = Some("rec-f42feeb-09161131-k8".to_owned());
        let id = run_id_for(&spec);
        assert!(
            id.contains("0916") && !id.contains("09161131"),
            "the group wins, as it does in stage_resolve_recording: {id}"
        );
    }

    /// And a run that genuinely does not know yet still says so. This is the
    /// case the word was written for and the one it should keep.
    #[test]
    fn a_run_with_nothing_named_is_still_unresolved() {
        let id = run_id_for(&replay_spec());
        assert!(
            id.contains("unresolved"),
            "an s3_source run resolves its session in the worker: {id}"
        );
    }

    /// The case this exists for: a run whose runner was OOMKilled reported only
    /// "job failed (see pod diagnostics in the run log)", so an operator had to
    /// open the log to learn the cause. It cost a 26-minute run being diagnosed
    /// five different wrong ways before anyone read the container state.
    #[test]
    fn an_oomkilled_container_becomes_the_failure_reason() {
        let cause = terminal_cause(
            "pod/deja-replay-abc container/runner",
            "terminated(OOMKilled, exit 137), ready=false, restarts=0",
        );
        let cause = cause.expect("an OOMKill is a cause");
        assert!(
            cause.starts_with("runner:"),
            "must name the container: {cause}"
        );
        assert!(cause.contains("OOMKilled"), "must name the reason: {cause}");
        assert!(
            failure_line("job failed", Some(&cause)).contains("OOMKilled"),
            "the reason must reach the run's failure line"
        );
    }

    /// A HEALTHY container must never be reported as the cause.
    ///
    /// Without this the first line rendered wins, and on these Jobs that is
    /// routinely `migrations: terminated(Completed, exit 0)` — which would
    /// replace a generic-but-honest message with a confident wrong one. That is
    /// a worse failure than the one being fixed.
    #[test]
    fn a_clean_exit_is_not_mistaken_for_a_cause() {
        assert!(
            terminal_cause(
                "pod/deja-replay-abc init/migrations",
                "terminated(Completed, exit 0), ready=true, restarts=0",
            )
            .is_none(),
            "exit 0 is not a failure"
        );
        assert!(
            terminal_cause(
                "pod/deja-replay-abc container/postgres",
                "running, ready=true, restarts=0",
            )
            .is_none(),
            "a running container is not a failure"
        );
    }

    /// With no identifiable cause the old message is kept rather than inventing one.
    #[test]
    fn no_cause_falls_back_to_the_generic_line() {
        let line = failure_line("job failed", None);
        assert!(line.contains("see pod diagnostics"), "{line}");
    }

    #[test]
    fn a_crashloop_or_image_pull_failure_also_counts() {
        for body in [
            "waiting(CrashLoopBackOff), ready=false, restarts=5",
            "waiting(ImagePullBackOff), ready=false, restarts=0",
        ] {
            assert!(
                terminal_cause("pod/x container/candidate", body).is_some(),
                "must be treated as a cause: {body}"
            );
        }
    }

    #[test]
    fn tarball_url_substitutes_sha_only_when_no_repo_hole() {
        // No {repo} in the template → env/run repo irrelevant, just fill {sha}.
        let url = resolve_tarball_url(
            "https://codeload.github.com/juspay/hyperswitch/tar.gz/{sha}",
            None,
            "ff191d7f79",
        )
        .expect("resolves");
        assert_eq!(
            url,
            "https://codeload.github.com/juspay/hyperswitch/tar.gz/ff191d7f79"
        );
    }

    #[test]
    fn tarball_url_fills_repo_from_the_run_parameter() {
        // A per-run repo overrides everything (and short-circuits before env).
        let url = resolve_tarball_url(
            "https://codeload.github.com/{repo}/tar.gz/{sha}",
            Some("acme/hyperswitch-fork"),
            "abc123",
        )
        .expect("resolves");
        assert_eq!(
            url,
            "https://codeload.github.com/acme/hyperswitch-fork/tar.gz/abc123"
        );
    }

    #[test]
    fn tarball_url_blank_run_repo_is_ignored() {
        // A blank per-run repo must not produce `//tar.gz`; with no env default
        // set in the test process it yields None (fall back to a local checkout).
        let out = resolve_tarball_url(
            "https://codeload.github.com/{repo}/tar.gz/{sha}",
            Some("   "),
            "abc123",
        );
        // Either None (no DEJA_CANDIDATE_REPO in the env) or, if the env happens
        // to set one, a URL with no `{repo}` hole left — never a blank segment.
        if let Some(url) = out {
            assert!(!url.contains("{repo}"));
            assert!(!url.contains("//tar.gz"));
        }
    }
}
