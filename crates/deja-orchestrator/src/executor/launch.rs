//! Turning one run into a Job: fetch the env profile's Job template from its
//! ConfigMap, apply the per-run typed patch, and create it — then watch it to a
//! terminal verdict. The template shape (SA, sidecars, volumes, the candidate
//! boot guard, resource limits) is the env profile's; this module only overlays
//! the per-run fields and drives the lifecycle.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::config::{resolve_candidate_image, K8sExecutorConfig};
use super::env::runner_env;
use super::k8s::{job_terminal_verdict, KubeApi, KubeError, KubeTransport};
use super::patch::{
    apply_job_patch, referenced_secret_names, ConfigEnvCopy, EnvUpsert, JobPatch, OwnerRef,
};
use crate::{HarnessRoot, Run, SchemaFingerprint};

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
    /// Where to copy the L1 config ENV from: the recorded system's rendered
    /// workload. Read at Job build (it needs a cluster read). `None` disables
    /// copying.
    pub config_source: Option<ConfigSourceRef>,
}

/// A resolved pointer to the rendered workload whose container env is copied.
#[derive(Debug, Clone)]
pub struct ConfigSourceRef {
    /// Deployment name.
    pub deployment: String,
    /// Container within it whose `env` + `envFrom` are copied.
    pub container: String,
}

#[derive(Debug)]
pub enum ExecutorError {
    Kube(KubeError),
    /// The ConfigMap exists but does not hold a usable Job template at the key.
    Template(String),
    /// The copied config env references secrets that do not exist in the Jobs
    /// namespace. Refusing here states the environment gap by name, instead of
    /// letting the pod fail opaquely at container-create time.
    MissingSecrets {
        namespace: String,
        names: Vec<String>,
    },
}

impl std::fmt::Display for ExecutorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExecutorError::Kube(e) => write!(f, "{e}"),
            ExecutorError::Template(m) => write!(f, "job template: {m}"),
            ExecutorError::MissingSecrets { namespace, names } => write!(
                f,
                "the recorded config env references secret(s) [{}] which do not exist in \
                 namespace '{namespace}' — provision them there (same names, same keys) \
                 before replaying this recording",
                names.join(", ")
            ),
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

/// Build a [`LaunchSpec`] from a run + the k8s config.
///
/// Composes the runner's per-run env and the candidate's bound env, and resolves
/// the candidate image. The artifact contract is derived HERE, at the Job's state
/// mount, rather than taken from the caller: the control plane's own contract
/// names paths on a filesystem the pod does not have, and passing one in is how
/// the candidate ends up pointed at a sink it cannot open.
///
/// `expected_schema` is the candidate's migration set once resolved (Option B:
/// the staged CodeBundle); `None` runs the P1 gate in record-only mode.
pub fn launch_spec_for_run(
    run: &Run,
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
    // Point the candidate at the JOB's copy of the run artifacts, not the control
    // plane's. `contract` is derived from the orchestrator's own state root, which
    // exists on a different filesystem than the pod: handing those paths to the
    // candidate gives it a lookup table it cannot read and a sink it cannot write.
    // Re-derive at the Job's shared mount through the SAME function the runner
    // calls, so the two processes cannot drift apart on a path convention.
    let job_contract = HarnessRoot {
        root: PathBuf::from(&cfg.job_state_dir),
    }
    .replay_contract(&run.run_id);
    // The binding and config source are per-run: the run's `system_under_test`
    // selects which env-var names carry the replay contract into the candidate.
    let candidate_binding = cfg.candidate_binding_for(run.spec.system());
    env.extend(candidate_binding.env_for(&job_contract, &code_sha));
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

    // One fixed render supplies the config env for every run. An empty name in
    // the profile switches the copy off — the Job then boots with whatever env
    // its template carries.
    let run_config_source = cfg.config_source_for(run.spec.system());
    let config_source = (!run_config_source.deployment.is_empty()).then(|| ConfigSourceRef {
        deployment: run_config_source.deployment.clone(),
        container: run_config_source.container.clone(),
    });

    Ok(LaunchSpec {
        run_id: run.run_id.clone(),
        jobs_namespace: cfg.jobs_namespace.clone(),
        template_namespace: cfg.template_namespace.clone(),
        template_configmap: cfg.template_configmap.clone(),
        // Per-system: a prism run resolves `job.prism.json`; the default system
        // keeps `job.json`. See `template_key_for` for why there is no fallback.
        template_key: cfg.template_key_for(run.spec.system()),
        candidate_container: candidate_binding.container.clone(),
        candidate_image,
        env,
        labels: vec![(RUN_ID_LABEL.to_owned(), run.run_id.clone())],
        config_source,
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
        ExecutorError::Template(format!(
            "data['{}'] is not valid JSON: {e}",
            spec.template_key
        ))
    })?;

    // Owner = the template ConfigMap itself: same-namespace (required — k8s
    // forbids cross-namespace ownerRefs), chart-managed (ArgoCD tracks it, so the
    // Job renders as its child in the resource tree) and already fetched here.
    // Omitted when the Job lands in a different namespace than the template (falls
    // back to an un-owned Job rather than a create k8s would reject) or when the
    // fetched ConfigMap carries no uid (e.g. a fake in tests).
    let owner = if spec.template_namespace == spec.jobs_namespace {
        cm.pointer("/metadata/uid")
            .and_then(Value::as_str)
            .map(|uid| OwnerRef {
                api_version: "v1".to_owned(),
                kind: "ConfigMap".to_owned(),
                name: spec.template_configmap.clone(),
                uid: uid.to_owned(),
            })
    } else {
        None
    };

    // L1 config env: copy the recorded system's own generated container env.
    let config_env = match &spec.config_source {
        Some(src) => Some(copy_config_env(api, spec, src)?),
        None => None,
    };

    let patch = JobPatch {
        job_name: job_name_for(&spec.run_id),
        labels: spec.labels.clone(),
        images: vec![(
            spec.candidate_container.clone(),
            spec.candidate_image.clone(),
        )],
        env: spec.env.clone(),
        owner,
        config_env,
    };
    apply_job_patch(&template, &patch).map_err(|e| ExecutorError::Template(e.to_string()))
}

/// Copy the recorded system's generated container env, and refuse if anything it
/// references is absent.
///
/// The env is read from a rendered workload the recorded system's OWN chart
/// produced (artifact-only, zero replicas) — so every value, and every
/// `secretKeyRef` name/key indirection, is exactly what that system's config
/// declares. Nothing is enumerated or rewritten here: a config key
/// or secret added to that system tomorrow flows through untouched, and no secret
/// VALUE is ever read by the orchestrator (only names, to check existence).
fn copy_config_env<T: KubeTransport>(
    api: &KubeApi<T>,
    spec: &LaunchSpec,
    src: &ConfigSourceRef,
) -> Result<ConfigEnvCopy, ExecutorError> {
    let dep = api.get_deployment(&spec.jobs_namespace, &src.deployment)?;
    let container = dep
        .pointer("/spec/template/spec/containers")
        .and_then(Value::as_array)
        .and_then(|cs| {
            cs.iter()
                .find(|c| c.get("name").and_then(Value::as_str) == Some(src.container.as_str()))
        })
        .ok_or_else(|| {
            ExecutorError::Template(format!(
                "config source deployment '{}' has no container '{}'",
                src.deployment, src.container
            ))
        })?;

    let list = |key: &str| {
        container
            .get(key)
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default()
    };
    let env = list("env");
    let env_from = list("envFrom");
    if env.is_empty() && env_from.is_empty() {
        return Err(ExecutorError::Template(format!(
            "config source '{}'/'{}' carries no env — refusing to boot the candidate \
             with no recorded config",
            src.deployment, src.container
        )));
    }

    // Pre-launch check: every secret the copied env points at must exist here, or
    // the pod would fail opaquely at container-create. Names come from the data.
    let missing: Vec<String> = referenced_secret_names(&env)
        .into_iter()
        .filter(|name| api.get_secret(&spec.jobs_namespace, name).is_err())
        .collect();
    if !missing.is_empty() {
        return Err(ExecutorError::MissingSecrets {
            namespace: spec.jobs_namespace.clone(),
            names: missing,
        });
    }

    Ok(ConfigEnvCopy {
        container: spec.candidate_container.clone(),
        env,
        env_from,
    })
}

/// What killing a run actually removed, so the caller can report it rather than
/// claim success blindly.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct KillReport {
    /// The Job, if it was still there. Absent = already gone, which is success.
    pub job_deleted: Option<String>,
    /// Pods deleted directly — normally none, since deleting the Job takes them.
    pub pods_deleted: Vec<String>,
    /// Non-fatal problems. A kill reports what it could not remove instead of
    /// failing outright, so one stuck pod never blocks reclaiming the rest.
    pub problems: Vec<String>,
}

/// Tear a run's Job down and make sure its pods go with it.
///
/// Deleting the Job cascades in the background, which is usually enough. But a
/// replay pod is expensive — a candidate, a runner and two stores — and it can
/// outlive its Job (a stuck terminating container, or a Job already reaped while
/// its pod lingers). A failed run holding that for hours is why this sweeps the
/// pods afterwards rather than trusting the cascade.
///
/// Idempotent: killing an already-dead run reports nothing removed and succeeds,
/// so a retry is always safe.
pub fn kill_run<T: KubeTransport>(
    api: &KubeApi<T>,
    namespace: &str,
    run_id: &str,
) -> Result<KillReport, ExecutorError> {
    let mut report = KillReport::default();
    let job = job_name_for(run_id);

    match api.delete_job(namespace, &job) {
        Ok(_) => report.job_deleted = Some(job.clone()),
        // Already gone is the desired end state, not a failure.
        Err(KubeError::Api { status: 404, .. }) => {}
        Err(e) => report.problems.push(format!("delete job {job}: {e}")),
    }

    // Sweep whatever the cascade has not taken. Selecting by the run-id label
    // catches a pod whose Job was already reaped.
    match api.list_pods(namespace, &format!("{RUN_ID_LABEL}={run_id}")) {
        Ok(pods) => {
            for pod in pods {
                let Some(name) = pod.pointer("/metadata/name").and_then(Value::as_str) else {
                    continue;
                };
                // A pod already terminating is on its way out; deleting again
                // would be noise, not progress.
                if pod.pointer("/metadata/deletionTimestamp").is_some() {
                    continue;
                }
                match api.delete_pod(namespace, name) {
                    Ok(_) => report.pods_deleted.push(name.to_owned()),
                    Err(KubeError::Api { status: 404, .. }) => {}
                    Err(e) => report.problems.push(format!("delete pod {name}: {e}")),
                }
            }
        }
        Err(e) => report.problems.push(format!("list pods: {e}")),
    }
    Ok(report)
}

/// Everything a dead run can still tell us about its pod: per-container state,
/// and what each container printed.
///
/// The runner can only observe that the candidate never became healthy — the
/// reason is in the candidate's own output, in a pod that is deleted soon after.
/// Collected on failure so the account survives the pod, and reported through the
/// run's own log rather than requiring cluster access to read.
///
/// Best-effort by construction: every step degrades to a note. A run has already
/// failed by the time this is called, and a diagnostics error must never replace
/// the real failure with one about diagnostics.
pub fn collect_pod_diagnostics<T: KubeTransport>(
    api: &KubeApi<T>,
    namespace: &str,
    run_id: &str,
    tail_lines: u32,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let pods = match api.list_pods(namespace, &format!("{RUN_ID_LABEL}={run_id}")) {
        Ok(p) if p.is_empty() => {
            out.push((
                "pod".into(),
                format!("no pod carries {RUN_ID_LABEL}={run_id} — already garbage-collected?"),
            ));
            return out;
        }
        Ok(p) => p,
        Err(e) => {
            out.push(("pod".into(), format!("listing pods failed: {e}")));
            return out;
        }
    };

    for pod in &pods {
        let name = pod
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>")
            .to_owned();
        let phase = pod
            .pointer("/status/phase")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        out.push((format!("pod/{name}"), format!("phase={phase}")));

        // Container states first: they say WHICH container failed and how, which
        // is what makes the logs below worth reading.
        for (kind, path) in [
            ("init", "/status/initContainerStatuses"),
            ("container", "/status/containerStatuses"),
        ] {
            for cs in pod
                .pointer(path)
                .and_then(Value::as_array)
                .unwrap_or(&vec![])
            {
                let cname = cs.get("name").and_then(Value::as_str).unwrap_or("?");
                out.push((
                    format!("pod/{name} {kind}/{cname}"),
                    describe_container_state(cs),
                ));
            }
        }

        // Then each container's own output.
        for (kind, path) in [
            ("init", "/status/initContainerStatuses"),
            ("container", "/status/containerStatuses"),
        ] {
            for cs in pod
                .pointer(path)
                .and_then(Value::as_array)
                .unwrap_or(&vec![])
            {
                let Some(cname) = cs.get("name").and_then(Value::as_str) else {
                    continue;
                };
                let body = match api.pod_logs(namespace, &name, cname, tail_lines) {
                    Ok(l) if l.trim().is_empty() => "<no output>".to_owned(),
                    Ok(l) => l,
                    Err(e) => format!("<log unavailable: {e}>"),
                };
                out.push((format!("pod/{name} {kind}/{cname} log"), body));
            }
        }
    }
    out
}

/// One line describing a container's state: running, waiting with a reason, or
/// terminated with its exit code — the shape `kubectl describe` shows, reduced to
/// what identifies a failure.
fn describe_container_state(status: &Value) -> String {
    let ready = status
        .get("ready")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let restarts = status
        .get("restartCount")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let state = status.get("state").and_then(Value::as_object);
    let detail = match state.and_then(|s| s.keys().next().cloned()) {
        Some(k) => {
            let inner = status.pointer(&format!("/state/{k}"));
            let field = |f: &str| {
                inner
                    .and_then(|v| v.get(f))
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned()
            };
            let code = inner
                .and_then(|v| v.get("exitCode"))
                .and_then(Value::as_i64);
            let mut d = k.clone();
            let reason = field("reason");
            if !reason.is_empty() {
                d.push_str(&format!(" reason={reason}"));
            }
            if let Some(c) = code {
                d.push_str(&format!(" exitCode={c}"));
            }
            let msg = field("message");
            if !msg.is_empty() {
                d.push_str(&format!(" message={msg}"));
            }
            d
        }
        None => "<no state>".to_owned(),
    };
    format!("{detail} ready={ready} restarts={restarts}")
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
            config_source: None,
        }
    }

    /// A rendered workload of the recorded system: its own chart's generated
    /// container env, including a `secretKeyRef` under that system's own names.
    fn source_deployment() -> Value {
        json!({ "spec": { "template": { "spec": { "containers": [
            { "name": "some-system-server", "env": [
                { "name": "RUN_ENV", "value": "sandbox" },
                { "name": "SOME__DB__HOST", "value": "sbx-pg.internal" },
                { "name": "SOME__SECRET_FIELD", "valueFrom": { "secretKeyRef": {
                    "name": "some-system-secrets", "key": "SOME__SECRET_FIELD_V2" } } }
              ],
              "envFrom": [ { "configMapRef": { "name": "recorded-configs" } } ] }
        ] } } } })
    }

    /// The candidate's artifact paths must name the JOB's shared mount, never the
    /// control plane's state root — those are different filesystems, and a path
    /// from the wrong one is something the candidate cannot open. It failed
    /// exactly that way once the candidate first booted: `failed to open replay
    /// observed sink '/var/lib/deja/state/observed/<run>.jsonl': Permission denied`.
    #[test]
    fn candidate_artifact_paths_are_on_the_job_mount() {
        let mut cfg = K8sExecutorConfig::from_env();
        cfg.job_state_dir = "/workspace/state".into();
        let run = Run {
            run_id: "run-42".into(),
            spec: crate::RunSpec {
                scored_span_namespaces: Vec::new(),
                mode: crate::RunMode::Replay,
                system_under_test: None,
                candidate_spec: crate::CandidateSpec::PrebuiltImage {
                    image: "img:abc123".into(),
                },
                candidate_repo: None,
                recording_id: None,
                s3_source: None,
                correlation_filter: None,
                workload: serde_json::Value::Null,
            },
            status: crate::RunStatus::Pending,
            recording_id: None,
            candidate_image: None,
            failure_reason: None,
            stage: None,
            step: 0,
            steps_total: 0,
            stage_updated_ms: 0,
        };
        let spec = launch_spec_for_run(&run, &cfg, None, None).expect("spec builds");
        let candidate: Vec<_> = spec
            .env
            .iter()
            .filter(|e| e.container == cfg.candidate_binding.container)
            .collect();
        let val = |name: &str| {
            candidate
                .iter()
                .find(|e| e.name == name)
                .unwrap_or_else(|| panic!("{name} bound"))
                .value
                .clone()
        };
        assert_eq!(
            val(&cfg.candidate_binding.source_env),
            "/workspace/state/lookup-tables/run-42.jsonl"
        );
        assert_eq!(
            val(&cfg.candidate_binding.observed_env),
            "/workspace/state/observed/run-42.jsonl"
        );
    }

    /// A prism run must fetch ITS template key and bind the CS__DEJA__* names —
    /// the first prism launch in sandbox booted the router template (postgres,
    /// redis, router-config) and died with "DEJA_RUN_ID unset". The template
    /// key and the binding both follow the run's system, in the same spec.
    #[test]
    fn a_prism_run_resolves_its_own_template_key_and_binding() {
        let cfg = K8sExecutorConfig::from_env();
        let run = Run {
            run_id: "run-43".into(),
            spec: crate::RunSpec {
                scored_span_namespaces: Vec::new(),
                mode: crate::RunMode::Replay,
                system_under_test: Some("prism".into()),
                candidate_spec: crate::CandidateSpec::PrebuiltImage {
                    image: "img:abc123".into(),
                },
                candidate_repo: None,
                recording_id: None,
                s3_source: None,
                correlation_filter: None,
                workload: serde_json::Value::Null,
            },
            status: crate::RunStatus::Pending,
            recording_id: None,
            candidate_image: None,
            failure_reason: None,
            stage: None,
            step: 0,
            steps_total: 0,
            stage_updated_ms: 0,
        };
        let spec = launch_spec_for_run(&run, &cfg, None, None).expect("spec builds");
        assert_eq!(spec.template_key, "job.prism.json");
        assert!(
            spec.env
                .iter()
                .any(|e| e.name == "CS__DEJA__RUN_ID" && e.value == "run-43"),
            "the prism binding names carry the replay contract"
        );
        // And a default-system run keeps the base key untouched.
        let mut base = run;
        base.spec.system_under_test = None;
        let spec = launch_spec_for_run(&base, &cfg, None, None).expect("spec builds");
        assert_eq!(spec.template_key, cfg.template_key);
    }

    /// Killing must take the pod with the Job, and sweep one the cascade missed —
    /// a replay pod holds a candidate, a runner and two stores, so one left behind
    /// is what keeps a failed run occupying a node for hours.
    #[test]
    fn kill_deletes_the_job_and_sweeps_remaining_pods() {
        let api = KubeApi::new(FakeTransport::new(vec![
            resp(200, json!({ "kind": "Status", "status": "Success" })), // delete job
            resp(
                200,
                json!({ "items": [
                    { "metadata": { "name": "pod-still-here" } },
                    // already terminating: leave it alone, deleting again is noise
                    { "metadata": { "name": "pod-going", "deletionTimestamp": "2026-08-03T10:00:00Z" } }
                ] }),
            ),
            resp(200, json!({ "kind": "Status", "status": "Success" })), // delete pod
        ]));
        let r = kill_run(&api, "replay-sbx", "run-7").expect("kill");
        assert_eq!(r.job_deleted.as_deref(), Some("deja-replay-run-7"));
        assert_eq!(r.pods_deleted, vec!["pod-still-here".to_owned()]);
        assert!(r.problems.is_empty(), "{:?}", r.problems);
    }

    /// Killing an already-dead run is success, not an error — otherwise the
    /// button is unsafe to press twice, which is exactly when it gets pressed.
    #[test]
    fn kill_is_idempotent_when_everything_is_already_gone() {
        let api = KubeApi::new(FakeTransport::new(vec![
            resp(404, json!({ "kind": "Status", "reason": "NotFound" })),
            resp(200, json!({ "items": [] })),
        ]));
        let r = kill_run(&api, "replay-sbx", "run-8").expect("kill");
        assert_eq!(r, KillReport::default());
    }

    /// A dead run must still be able to say WHY. Reproduces the shape that cost a
    /// day: the runner reports only "candidate never became healthy", while the
    /// candidate's own last words — a config error at boot — are in its container
    /// log, in a pod that is about to be deleted.
    #[test]
    fn diagnostics_report_container_state_and_output() {
        let pod = json!({ "items": [ { "metadata": { "name": "deja-replay-run-7-abc" },
            "status": {
                "phase": "Failed",
                "initContainerStatuses": [
                    { "name": "migrations", "ready": true, "restartCount": 0,
                      "state": { "terminated": { "reason": "Completed", "exitCode": 0 } } }
                ],
                "containerStatuses": [
                    { "name": "candidate", "ready": false, "restartCount": 0,
                      "state": { "terminated": { "reason": "Error", "exitCode": 1 } } }
                ]
            } } ] });
        let api = KubeApi::new(FakeTransport::new(vec![
            resp(200, pod),
            // one log fetch per container, in the order the collector walks them
            resp(200, json!("staging CodeBundle …")),
            resp(
                200,
                json!("Error: failed to open replay observed sink: Permission denied"),
            ),
        ]));
        let out = collect_pod_diagnostics(&api, "replay-sbx", "run-7", 200);
        let joined = out
            .iter()
            .map(|(k, v)| format!("{k}: {v}"))
            .collect::<Vec<_>>()
            .join("\n");

        // which container failed, and how
        assert!(joined.contains("phase=Failed"), "{joined}");
        assert!(
            joined.contains("container/candidate") && joined.contains("exitCode=1"),
            "{joined}"
        );
        // and what it actually said
        assert!(joined.contains("Permission denied"), "{joined}");
        // the healthy init container is reported too, so a clean step is visibly clean
        assert!(joined.contains("init/migrations") && joined.contains("exitCode=0"));
    }

    /// Diagnostics run only after a failure, so they must never introduce one:
    /// a vanished pod is reported as a note, not an error.
    #[test]
    fn diagnostics_degrade_when_the_pod_is_gone() {
        let api = KubeApi::new(FakeTransport::new(vec![resp(200, json!({ "items": [] }))]));
        let out = collect_pod_diagnostics(&api, "replay-sbx", "run-9", 200);
        assert_eq!(out.len(), 1);
        assert!(out[0].1.contains("garbage-collected"), "{:?}", out[0]);
    }

    fn spec_with_source() -> LaunchSpec {
        let mut s = spec();
        s.config_source = Some(ConfigSourceRef {
            deployment: "replay-sbx-some-system-server".into(),
            container: "some-system-server".into(),
        });
        s
    }

    #[test]
    fn build_job_copies_recorded_env_and_checks_its_secrets() {
        // template CM, then the source Deployment, then the secret existence probe.
        let api = KubeApi::new(FakeTransport::new(vec![
            resp(200, template_cm()),
            resp(200, source_deployment()),
            resp(
                200,
                json!({ "metadata": { "name": "some-system-secrets" } }),
            ),
        ]));
        let job = build_job(&api, &spec_with_source()).expect("built");

        let cand = job
            .pointer("/spec/template/spec/containers/1")
            .expect("candidate container");
        let env = cand["env"].as_array().expect("candidate env");
        // recorded plain value copied
        assert!(env
            .iter()
            .any(|e| e["name"] == json!("RUN_ENV") && e["value"] == json!("sandbox")));
        // secretKeyRef indirection copied verbatim — this codebase never names it
        let sref = env
            .iter()
            .find(|e| e["name"] == json!("SOME__SECRET_FIELD"))
            .expect("copied secret ref");
        assert_eq!(
            sref["valueFrom"]["secretKeyRef"]["name"],
            json!("some-system-secrets")
        );
        assert_eq!(
            sref["valueFrom"]["secretKeyRef"]["key"],
            json!("SOME__SECRET_FIELD_V2")
        );
        // the per-run replay override is still applied on top
        assert!(env
            .iter()
            .any(|e| e["name"] == json!("ROUTER__DEJA__MODE") && e["value"] == json!("replay")));
        // envFrom came from the recorded render too
        assert_eq!(
            cand["envFrom"][0]["configMapRef"]["name"],
            json!("recorded-configs")
        );
    }

    #[test]
    fn build_job_refuses_when_a_referenced_secret_is_absent() {
        let api = KubeApi::new(FakeTransport::new(vec![
            resp(200, template_cm()),
            resp(200, source_deployment()),
            resp(404, json!({ "message": "not found" })),
        ]));
        let err = build_job(&api, &spec_with_source()).expect_err("missing secret refuses");
        match err {
            ExecutorError::MissingSecrets { namespace, names } => {
                assert_eq!(namespace, "replay-sbx");
                assert_eq!(names, vec!["some-system-secrets"]);
            }
            other => panic!("expected MissingSecrets, got {other:?}"),
        }
    }

    #[test]
    fn build_job_refuses_when_the_source_container_is_absent() {
        let api = KubeApi::new(FakeTransport::new(vec![
            resp(200, template_cm()),
            resp(
                200,
                json!({ "spec": { "template": { "spec": { "containers": [
                { "name": "a-different-container", "env": [] }
            ] } } } }),
            ),
        ]));
        let err = build_job(&api, &spec_with_source()).expect_err("wrong container refuses");
        assert!(matches!(err, ExecutorError::Template(_)));
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

    // template_cm() plus the ConfigMap's OWN metadata.uid, so the owner (the
    // ConfigMap itself) is derivable in build_job.
    fn template_cm_with_uid(uid: &str) -> Value {
        json!({
            "metadata": { "uid": uid, "name": "job-template" },
            "data": template_cm()["data"].clone()
        })
    }

    #[test]
    fn build_job_stamps_configmap_owner_when_same_namespace() {
        let mut s = spec();
        s.template_namespace = "replay-sbx".into(); // == jobs_namespace
        let api = KubeApi::new(FakeTransport::new(vec![resp(
            200,
            template_cm_with_uid("cm-uid-1"),
        )]));
        let job = build_job(&api, &s).expect("built");
        let owners = job["metadata"]["ownerReferences"]
            .as_array()
            .expect("ownerReferences present");
        assert_eq!(owners.len(), 1);
        assert_eq!(owners[0]["kind"], json!("ConfigMap"));
        assert_eq!(owners[0]["name"], json!("job-template"));
        assert_eq!(owners[0]["uid"], json!("cm-uid-1"));
        assert_eq!(owners[0]["controller"], json!(false));
        assert_eq!(owners[0]["blockOwnerDeletion"], json!(false));
    }

    #[test]
    fn build_job_omits_owner_across_namespaces() {
        // default spec(): template_namespace (replay-env) != jobs_namespace
        // (replay-sbx) — a cross-namespace ownerRef is forbidden, so it is omitted
        // even though the ConfigMap has a uid.
        let api = KubeApi::new(FakeTransport::new(vec![resp(
            200,
            template_cm_with_uid("cm-uid-1"),
        )]));
        let job = build_job(&api, &spec()).expect("built");
        assert!(
            job["metadata"].get("ownerReferences").is_none(),
            "cross-namespace owner must be omitted (k8s forbids it)"
        );
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
            resp(
                200,
                json!({"status": {"conditions": [{"type": "Complete", "status": "True"}]}}),
            ),
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
