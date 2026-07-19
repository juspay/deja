//! Run lifecycle endpoints — create, fetch status.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::executor::{
    launch, launch_spec_for_run, watch_to_terminal, InClusterConfig, K8sExecutorConfig, KubeApi,
    UreqTransport,
};
use crate::lifecycle::StoreCtx;
use crate::{new_id, read_json, write_json, HarnessRoot, Run, RunMode, RunSpec, RunStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRunResponse {
    pub run_id: String,
    pub status: RunStatus,
}

/// Build and persist a Pending run record (no worker yet). The caller is
/// responsible for inserting the store row (if a store is connected) BEFORE
/// spawning the worker — stage rows reference the run row by foreign key.
pub fn persist_new(root: &HarnessRoot, spec: RunSpec) -> std::io::Result<Run> {
    let run_id = new_id("run");
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
fn resolve_tarball_url(template: &str, run_repo: Option<&str>, sha: &str) -> Option<String> {
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
            Ok(fp) => {
                eprintln!(
                    "codebundle: candidate {sha} staged from {url} at {uri}; expects {} \
                     migrations (P1 armed)",
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
    if let Some(repo) = std::env::var("DEJA_CANDIDATE_REPO_DIR").ok().and_then(nonempty) {
        let repo = std::path::Path::new(&repo);
        match crate::codebundle::ensure_bundle_staged(&s3, repo, &sha) {
            Ok(fp) => {
                eprintln!(
                    "codebundle: candidate {sha} staged from local checkout at {uri}; expects \
                     {} migrations (P1 armed)",
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
    let root_path = root.root.clone();
    std::thread::spawn(move || {
        let root = match HarnessRoot::new(&root_path) {
            Ok(r) => r,
            Err(e) => return ctx.finish(false, Some(&format!("state root: {e}"))),
        };
        let contract = root.replay_contract(&run.run_id);
        // A1/P1: resolve the candidate's migration bundle from its ref — the
        // expected set (runner refuses on drift) AND the S3 URI the migrations
        // initContainer pulls the candidate's own migrations from. Absent →
        // record-only. Always logged.
        let bundle = resolve_expected_schema(&run);
        let (expected, uri) = match &bundle {
            Some((fp, u)) => (Some(fp), Some(u.as_str())),
            None => (None, None),
        };
        let spec = match launch_spec_for_run(&run, &contract, &cfg, expected, uri) {
            Ok(s) => s,
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
                    &format!("created Job {n} in namespace {}", cfg.jobs_namespace),
                );
                n
            }
            Err(e) => return ctx.finish(false, Some(&format!("launch job: {e}"))),
        };
        // Poll to a terminal Job state. A 1h ceiling bounds a stuck Job; the
        // Job's own activeDeadlineSeconds (template) is the authoritative timeout.
        match watch_to_terminal(
            &api,
            &cfg.jobs_namespace,
            &name,
            Duration::from_secs(5),
            Duration::from_secs(60 * 60),
            std::thread::sleep,
        ) {
            Ok(Some(true)) => ctx.finish(true, None),
            Ok(Some(false)) => {
                ctx.finish(false, Some("job failed (see runner logs / pod events)"))
            }
            Ok(None) => ctx.finish(
                false,
                Some("job did not reach a terminal state within the watch deadline"),
            ),
            Err(e) => ctx.finish(false, Some(&format!("watch job: {e}"))),
        }
    });
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
    use super::resolve_tarball_url;

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
