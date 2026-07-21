//! Turning one run into a Job: fetch the env profile's Job template from its
//! ConfigMap, apply the per-run typed patch, and create it — then watch it to a
//! terminal verdict. The template shape (SA, sidecars, volumes, the candidate
//! boot guard, resource limits) is the env profile's; this module only overlays
//! the per-run fields and drives the lifecycle.

use std::time::{Duration, Instant};

use serde_json::Value;

use super::config::{resolve_candidate_image, K8sExecutorConfig};
use super::env::runner_env;
use super::k8s::{job_terminal_verdict, KubeApi, KubeError, KubeTransport};
use super::patch::{apply_job_patch, EnvUpsert, JobPatch};
use crate::{ReplayContract, Run, SchemaFingerprint};

/// The label the launcher stamps on every replay Job (and its pod template),
/// carrying the run id that Job backs. One source of truth for both sides: the
/// launcher SETS it here, and the reconciler SELECTS Jobs by it and reads the
/// run id back from its value — so the two can never drift apart.
pub const RUN_ID_LABEL: &str = "deja.run-id";

/// Everything the executor needs to launch a run as a Job. The env computation
/// (runner env, the candidate's own env binding) is the caller's — built from
/// the `ReplayContract`, the candidate ref, and the env profile — so this stays
/// candidate-agnostic. The template coordinates say WHERE the Job template lives.
pub struct LaunchSpec {
    pub run_id: String,
    /// Namespace the Job is created in (the data-plane namespace, e.g. replay-sbx).
    pub jobs_namespace: String,
    /// Namespace + ConfigMap + data key holding the Job template JSON.
    pub template_namespace: String,
    pub template_configmap: String,
    pub template_key: String,
    /// Template container names to target.
    pub candidate_container: String,
    /// The resolved candidate image (sha_C-tagged).
    pub candidate_image: String,
    /// Env upserts for every container (runner env + the candidate binding). Each
    /// already carries its target container name.
    pub env: Vec<EnvUpsert>,
    /// Labels merged onto the Job + pod template (selectors, run correlation).
    pub labels: Vec<(String, String)>,
}

#[derive(Debug)]
pub enum ExecutorError {
    Kube(KubeError),
    /// The ConfigMap exists but does not hold a usable Job template at the key.
    Template(String),
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorError::Kube(e) => write!(f, "{e}"),
            ExecutorError::Template(m) => write!(f, "job template: {m}"),
        }
    }
}

impl std::error::Error for ExecutorError {}

impl From<KubeError> for ExecutorError {
    fn from(e: KubeError) -> Self {
        ExecutorError::Kube(e)
    }
}

/// A k8s object name must be DNS-1123: lowercase alphanumeric and '-', <=63
/// chars, start/end alphanumeric. Run ids are already close; normalize defensively
/// so a stray character never makes the apiserver reject the Job.
pub fn job_name_for(run_id: &str) -> String {
    let mut s: String = format!("deja-replay-{run_id}")
        .chars()
        .map(|c| {
            let c = c.to_ascii_lowercase();
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    s.truncate(63);
    // Must end alphanumeric.
    while s.ends_with('-') {
        s.pop();
    }
    s
}

/// Build a [`LaunchSpec`] from a run + its harness contract + the k8s config.
/// Composes the runner's per-run env and the candidate's bound env (both derived
/// from the contract + resolved sha — no constants), and resolves the candidate
/// image. `expected_schema` is the candidate's migration set once resolved
/// (Option B: the staged CodeBundle); `None` runs the P1 gate in record-only
/// mode until that lands.
pub fn launch_spec_for_run(
    run: &Run,
    contract: &ReplayContract,
    cfg: &K8sExecutorConfig,
    expected_schema: Option<&SchemaFingerprint>,
    code_bundle_uri: Option<&str>,
) -> Result<LaunchSpec, ExecutorError> {
    let (candidate_image, code_sha) = resolve_candidate_image(&run.spec.candidate_spec)?;
    let run_spec_json = serde_json::to_string(&run.spec)
        .map_err(|e| ExecutorError::Template(format!("serialize run spec: {e}")))?;

    let mut env = runner_env(
        &cfg.runner_container,
        &run.run_id,
        &run_spec_json,
        expected_schema,
    );
    env.extend(cfg.candidate_binding.env_for(contract, &code_sha));
    // The migrations initContainer pulls the candidate's bundle from this URI
    // (Option B). Only injected when the bundle resolved — else the template's
    // placeholder stays and the init no-ops / fails loudly on a bad URI.
    if let Some(uri) = code_bundle_uri {
        env.push(EnvUpsert::new(
            &cfg.migrations_init_container,
            &cfg.code_bundle_uri_env,
            uri,
        ));
    }

    Ok(LaunchSpec {
        run_id: run.run_id.clone(),
        jobs_namespace: cfg.jobs_namespace.clone(),
        template_namespace: cfg.template_namespace.clone(),
        template_configmap: cfg.template_configmap.clone(),
        template_key: cfg.template_key.clone(),
        candidate_container: cfg.candidate_binding.container.clone(),
        candidate_image,
        env,
        labels: vec![(RUN_ID_LABEL.to_owned(), run.run_id.clone())],
    })
}

/// Fetch the template ConfigMap, extract the Job JSON at the configured key, and
/// apply the per-run patch. Pure w.r.t. the transport — tested against a fake.
pub fn build_job<T: KubeTransport>(
    api: &KubeApi<T>,
    spec: &LaunchSpec,
) -> Result<Value, ExecutorError> {
    let cm = api.get_configmap(&spec.template_namespace, &spec.template_configmap)?;
    let raw = cm
        .pointer(&format!("/data/{}", spec.template_key))
        .and_then(Value::as_str)
        .ok_or_else(|| {
            ExecutorError::Template(format!(
                "ConfigMap {}/{} has no string data key '{}'",
                spec.template_namespace, spec.template_configmap, spec.template_key
            ))
        })?;
    let template: Value = serde_json::from_str(raw).map_err(|e| {
        ExecutorError::Template(format!("data['{}'] is not valid JSON: {e}", spec.template_key))
    })?;

    let patch = JobPatch {
        job_name: job_name_for(&spec.run_id),
        labels: spec.labels.clone(),
        images: vec![(spec.candidate_container.clone(), spec.candidate_image.clone())],
        env: spec.env.clone(),
    };
    apply_job_patch(&template, &patch).map_err(|e| ExecutorError::Template(e.to_string()))
}

/// Build + create the Job. A 409 (the Job already exists) is treated as SUCCESS
/// — an idempotent relaunch must not create a second Job or error (V6). Returns
/// the Job's name so the caller can watch it.
pub fn launch<T: KubeTransport>(
    api: &KubeApi<T>,
    spec: &LaunchSpec,
) -> Result<String, ExecutorError> {
    let job = build_job(api, spec)?;
    let name = job_name_for(&spec.run_id);
    match api.create_job(&spec.jobs_namespace, &job) {
        Ok(_) => Ok(name),
        // Idempotent: the Job already exists (a retried launch) — adopt it.
        Err(KubeError::AlreadyExists { .. }) => Ok(name),
        Err(e) => Err(ExecutorError::Kube(e)),
    }
}

/// Poll a Job to its terminal verdict. `Some(true)` complete, `Some(false)`
/// failed, `None` if the deadline passed while still running. Tolerates a
/// transient NotFound (the Job may not be visible the instant after create).
/// The sleep makes this the live half; the interpretation it delegates to
/// `job_terminal_verdict` is unit-tested separately.
pub fn watch_to_terminal<T: KubeTransport>(
    api: &KubeApi<T>,
    namespace: &str,
    name: &str,
    poll: Duration,
    deadline: Duration,
    mut sleep: impl FnMut(Duration),
) -> Result<Option<bool>, ExecutorError> {
    let start = Instant::now();
    loop {
        match api.get_job(namespace, name) {
            Ok(job) => {
                if let Some(verdict) = job_terminal_verdict(&job) {
                    return Ok(Some(verdict));
                }
            }
            // Right after create the Job may not be readable yet; keep polling.
            Err(KubeError::Api { status: 404, .. }) => {}
            Err(e) => return Err(ExecutorError::Kube(e)),
        }
        if start.elapsed() >= deadline {
            return Ok(None);
        }
        sleep(poll);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::k8s::{KubeRequest, KubeResponse, KubeTransport};
    use serde_json::json;
    use std::cell::RefCell;

    struct FakeTransport {
        responses: RefCell<Vec<KubeResponse>>,
        seen: RefCell<Vec<(String, String, Option<Value>)>>,
    }
    impl FakeTransport {
        fn new(responses: Vec<KubeResponse>) -> Self {
            Self {
                responses: RefCell::new(responses),
                seen: RefCell::new(Vec::new()),
            }
        }
    }
    impl KubeTransport for FakeTransport {
        fn send(&self, req: &KubeRequest) -> Result<KubeResponse, KubeError> {
            self.seen.borrow_mut().push((
                req.method.to_owned(),
                req.path.clone(),
                req.body.clone(),
            ));
            Ok(self.responses.borrow_mut().remove(0))
        }
    }
    fn resp(status: u16, body: Value) -> KubeResponse {
        KubeResponse { status, body }
    }

    // A template ConfigMap whose data['job.json'] is a two-container Job.
    fn template_cm() -> Value {
        let job = json!({
            "apiVersion": "batch/v1", "kind": "Job",
            "metadata": { "labels": { "app": "replay" } },
            "spec": { "template": { "metadata": {}, "spec": { "containers": [
                { "name": "runner", "image": "orchestrator:tmpl", "env": [] },
                { "name": "candidate", "image": "candidate:tmpl" }
            ] } } }
        });
        json!({ "data": { "job.json": job.to_string() } })
    }

    fn spec() -> LaunchSpec {
        LaunchSpec {
            run_id: "run-9f".into(),
            jobs_namespace: "replay-sbx".into(),
            template_namespace: "replay-env".into(),
            template_configmap: "job-template".into(),
            template_key: "job.json".into(),
            candidate_container: "candidate".into(),
            candidate_image: "hyperswitch:sha_c".into(),
            env: vec![
                EnvUpsert::new("runner", "DEJA_RUN_ID", "run-9f"),
                EnvUpsert::new("candidate", "ROUTER__DEJA__MODE", "replay"),
            ],
            labels: vec![("deja.run-id".into(), "run-9f".into())],
        }
    }

    #[test]
    fn build_job_fetches_template_and_applies_patch() {
        let api = KubeApi::new(FakeTransport::new(vec![resp(200, template_cm())]));
        let job = build_job(&api, &spec()).expect("built");

        assert_eq!(job["metadata"]["name"], json!("deja-replay-run-9f"));
        assert_eq!(job["metadata"]["labels"]["deja.run-id"], json!("run-9f"));
        let containers = job["spec"]["template"]["spec"]["containers"]
            .as_array()
            .expect("containers");
        assert_eq!(containers[1]["image"], json!("hyperswitch:sha_c"));
        assert_eq!(containers[0]["env"][0]["name"], json!("DEJA_RUN_ID"));
        assert_eq!(containers[1]["env"][0]["name"], json!("ROUTER__DEJA__MODE"));
    }

    #[test]
    fn build_job_missing_key_is_a_template_error() {
        let cm = json!({ "data": { "other.json": "{}" } });
        let api = KubeApi::new(FakeTransport::new(vec![resp(200, cm)]));
        match build_job(&api, &spec()) {
            Err(ExecutorError::Template(m)) => assert!(m.contains("job.json")),
            other => panic!("expected Template error, got {other:?}"),
        }
    }

    #[test]
    fn build_job_unparseable_template_is_a_template_error() {
        let cm = json!({ "data": { "job.json": "not json {" } });
        let api = KubeApi::new(FakeTransport::new(vec![resp(200, cm)]));
        assert!(matches!(
            build_job(&api, &spec()),
            Err(ExecutorError::Template(_))
        ));
    }

    #[test]
    fn launch_create_conflict_is_idempotent_success() {
        // GET template (200), then POST job → 409 AlreadyExists.
        let api = KubeApi::new(FakeTransport::new(vec![
            resp(200, template_cm()),
            resp(409, json!({"reason": "AlreadyExists"})),
        ]));
        let name = launch(&api, &spec()).expect("idempotent launch");
        assert_eq!(name, "deja-replay-run-9f");
    }

    #[test]
    fn watch_returns_verdict_when_job_completes() {
        // First poll: running; second poll: complete.
        let api = KubeApi::new(FakeTransport::new(vec![
            resp(200, json!({"status": {"active": 1}})),
            resp(200, json!({"status": {"conditions": [{"type": "Complete", "status": "True"}]}})),
        ]));
        let mut slept = 0;
        let v = watch_to_terminal(
            &api,
            "replay-sbx",
            "deja-replay-run-9f",
            Duration::from_millis(1),
            Duration::from_secs(60),
            |_| slept += 1,
        )
        .expect("watch");
        assert_eq!(v, Some(true));
        assert_eq!(slept, 1, "slept once between the two polls");
    }

    #[test]
    fn watch_tolerates_initial_not_found() {
        let api = KubeApi::new(FakeTransport::new(vec![
            resp(404, json!({"reason": "NotFound"})),
            resp(200, json!({"status": {"failed": 1}})),
        ]));
        let v = watch_to_terminal(
            &api,
            "replay-sbx",
            "j",
            Duration::from_millis(1),
            Duration::from_secs(60),
            |_| {},
        )
        .expect("watch");
        assert_eq!(v, Some(false));
    }

    #[test]
    fn watch_gives_up_at_deadline_still_running() {
        let api = KubeApi::new(FakeTransport::new(vec![resp(
            200,
            json!({"status": {"active": 1}}),
        )]));
        // Zero deadline: after the first (still-running) read, elapsed>=deadline.
        let v = watch_to_terminal(
            &api,
            "replay-sbx",
            "j",
            Duration::from_millis(1),
            Duration::from_secs(0),
            |_| {},
        )
        .expect("watch");
        assert_eq!(v, None);
    }

    #[test]
    fn job_name_is_dns_1123_safe() {
        assert_eq!(job_name_for("run_9F.x"), "deja-replay-run-9f-x");
        let long = job_name_for(&"a".repeat(200));
        assert!(long.len() <= 63);
        assert!(!long.ends_with('-'));
    }
}
