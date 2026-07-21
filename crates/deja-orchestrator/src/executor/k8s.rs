//! In-cluster k8s API transport for the executor — raw REST against the
//! apiserver. Deliberately not a `kube`/`k8s-openapi` dependency: the executor
//! touches exactly four verbs (get a ConfigMap, create/get/delete a Job), so a
//! thin typed client over `ureq` is smaller and has no derive/codegen weight.
//!
//! Two seams keep the untestable part small:
//!   * [`KubeTransport`] is the HTTP boundary. The real [`UreqTransport`] does
//!     TLS against the apiserver (trusting the pod's mounted cluster CA) and
//!     re-reads the SA token on every call (projected tokens rotate). It needs a
//!     live cluster, so it is not unit-tested.
//!   * [`KubeApi`] is the verb layer — URL construction and response
//!     interpretation — and is generic over the transport, so it is fully
//!     unit-tested against a fake.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;

/// Standard in-pod mount for the service account.
const SA_ROOT: &str = "/var/run/secrets/kubernetes.io/serviceaccount";

#[derive(Debug)]
pub enum KubeError {
    /// Missing/invalid in-cluster config (env or mounted SA files).
    Config(String),
    /// Transport failure (DNS/TLS/connection/timeout).
    Transport(String),
    /// The apiserver returned a non-2xx status. `reason` is the parsed
    /// `.reason`/`.message` from the Status body when present.
    Api {
        status: u16,
        reason: String,
    },
    /// A `create` collided with an existing object (HTTP 409). Called out
    /// separately because for an idempotent launch it is SUCCESS, not failure
    /// (the Job already exists — do not launch a second one). (V6)
    AlreadyExists {
        name: String,
    },
}

impl std::fmt::Display for KubeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KubeError::Config(m) => write!(f, "in-cluster config: {m}"),
            KubeError::Transport(m) => write!(f, "k8s transport: {m}"),
            KubeError::Api { status, reason } => {
                write!(f, "k8s api error {status}: {reason}")
            }
            KubeError::AlreadyExists { name } => write!(f, "k8s object already exists: {name}"),
        }
    }
}

impl std::error::Error for KubeError {}

/// One apiserver request, transport-agnostic. `path` is apiserver-absolute
/// (`/apis/batch/v1/namespaces/…`); the transport prepends the API base.
pub struct KubeRequest {
    pub method: &'static str,
    pub path: String,
    pub body: Option<Value>,
}

/// The parsed apiserver response.
pub struct KubeResponse {
    pub status: u16,
    pub body: Value,
}

/// The HTTP boundary. One method, so a fake is trivial.
pub trait KubeTransport {
    fn send(&self, req: &KubeRequest) -> Result<KubeResponse, KubeError>;
}

/// In-cluster access derived from the injected `KUBERNETES_SERVICE_*` env and
/// the pod's mounted service account. The token is NOT captured here — it is
/// re-read from disk per request by the transport, since projected tokens
/// rotate under the pod.
#[derive(Clone)]
pub struct InClusterConfig {
    /// `https://host:port`.
    pub api_base: String,
    /// Cluster CA PEM (immutable for the pod's life).
    pub ca_pem: Vec<u8>,
    /// SA token file; re-read per request.
    pub token_path: PathBuf,
    /// The pod's own namespace (the control plane's namespace).
    pub namespace: String,
}

impl InClusterConfig {
    /// Build from the standard in-pod environment. Errors name exactly which
    /// piece is missing — the usual cause is running outside a cluster.
    pub fn from_env() -> Result<Self, KubeError> {
        Self::from_env_at(SA_ROOT)
    }

    /// `from_env` with an overridable SA mount root (for tests).
    pub fn from_env_at(sa_root: impl AsRef<Path>) -> Result<Self, KubeError> {
        let host = std::env::var("KUBERNETES_SERVICE_HOST")
            .map_err(|_| KubeError::Config("KUBERNETES_SERVICE_HOST unset".into()))?;
        let port = std::env::var("KUBERNETES_SERVICE_PORT")
            .map_err(|_| KubeError::Config("KUBERNETES_SERVICE_PORT unset".into()))?;
        // An IPv6 literal host must be bracketed in a URL authority.
        let host_auth = if host.contains(':') && !host.starts_with('[') {
            format!("[{host}]")
        } else {
            host
        };
        let sa = sa_root.as_ref();
        let ca_pem = std::fs::read(sa.join("ca.crt"))
            .map_err(|e| KubeError::Config(format!("read {}: {e}", sa.join("ca.crt").display())))?;
        let namespace = std::fs::read_to_string(sa.join("namespace"))
            .map_err(|e| {
                KubeError::Config(format!("read {}: {e}", sa.join("namespace").display()))
            })?
            .trim()
            .to_owned();
        Ok(Self {
            api_base: format!("https://{host_auth}:{port}"),
            ca_pem,
            token_path: sa.join("token"),
            namespace,
        })
    }
}

// ---------------------------------------------------------------------------
// Verb layer — generic over the transport, fully testable.
// ---------------------------------------------------------------------------

pub struct KubeApi<T: KubeTransport> {
    transport: T,
}

impl<T: KubeTransport> KubeApi<T> {
    pub fn new(transport: T) -> Self {
        Self { transport }
    }

    /// GET a ConfigMap (the Job template lives in one).
    pub fn get_configmap(&self, ns: &str, name: &str) -> Result<Value, KubeError> {
        let req = KubeRequest {
            method: "GET",
            path: format!("/api/v1/namespaces/{ns}/configmaps/{name}"),
            body: None,
        };
        self.expect_2xx(self.transport.send(&req)?, name)
    }

    /// POST a Job. A 409 is surfaced as [`KubeError::AlreadyExists`] so an
    /// idempotent launch can treat it as success rather than an error (V6).
    pub fn create_job(&self, ns: &str, job: &Value) -> Result<Value, KubeError> {
        let name = job
            .pointer("/metadata/name")
            .and_then(Value::as_str)
            .unwrap_or("<unnamed>")
            .to_owned();
        let req = KubeRequest {
            method: "POST",
            path: format!("/apis/batch/v1/namespaces/{ns}/jobs"),
            body: Some(job.clone()),
        };
        let resp = self.transport.send(&req)?;
        if resp.status == 409 {
            return Err(KubeError::AlreadyExists { name });
        }
        self.expect_2xx(resp, &name)
    }

    /// GET a Job (for status/watch polling).
    pub fn get_job(&self, ns: &str, name: &str) -> Result<Value, KubeError> {
        let req = KubeRequest {
            method: "GET",
            path: format!("/apis/batch/v1/namespaces/{ns}/jobs/{name}"),
            body: None,
        };
        self.expect_2xx(self.transport.send(&req)?, name)
    }

    /// LIST the Jobs in a namespace matching a label selector, returning the
    /// `.items` array (empty when the list is empty or absent). The reconciler
    /// uses this once per pass to find every Job the launcher created (selector
    /// = the launcher's run-id label), instead of a per-run GET.
    ///
    /// `label_selector` is placed verbatim in the `labelSelector` query param.
    /// Callers pass an already-URL-safe selector — the launcher's label keys and
    /// values are DNS-1123 (alphanumeric plus `-_.`), so no escaping is needed
    /// (matching the raw query style of `delete_job`).
    pub fn list_jobs(&self, ns: &str, label_selector: &str) -> Result<Vec<Value>, KubeError> {
        let req = KubeRequest {
            method: "GET",
            path: format!("/apis/batch/v1/namespaces/{ns}/jobs?labelSelector={label_selector}"),
            body: None,
        };
        let body = self.expect_2xx(self.transport.send(&req)?, "jobs")?;
        Ok(body
            .get("items")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default())
    }

    /// DELETE a Job. `propagationPolicy=Background` so the pods go too.
    pub fn delete_job(&self, ns: &str, name: &str) -> Result<Value, KubeError> {
        let req = KubeRequest {
            method: "DELETE",
            path: format!(
                "/apis/batch/v1/namespaces/{ns}/jobs/{name}?propagationPolicy=Background"
            ),
            body: None,
        };
        self.expect_2xx(self.transport.send(&req)?, name)
    }

    fn expect_2xx(&self, resp: KubeResponse, name: &str) -> Result<Value, KubeError> {
        if (200..300).contains(&resp.status) {
            return Ok(resp.body);
        }
        // A k8s error body is a Status object: prefer .reason, fall back to
        // .message, then the raw body — always name the object.
        let reason = resp
            .body
            .get("reason")
            .and_then(Value::as_str)
            .or_else(|| resp.body.get("message").and_then(Value::as_str))
            .map(str::to_owned)
            .unwrap_or_else(|| resp.body.to_string());
        Err(KubeError::Api {
            status: resp.status,
            reason: format!("{reason} (object {name})"),
        })
    }
}

/// Interpret a Job's `.status` into a terminal verdict, if any. Returns
/// `Some(true)` on complete, `Some(false)` on failed, `None` while still
/// running. Reads the standard `conditions[].type in {Complete, Failed}` with
/// `status == "True"`, falling back to the `succeeded`/`failed` counts.
pub fn job_terminal_verdict(job: &Value) -> Option<bool> {
    let status = job.get("status")?;
    if let Some(conds) = status.get("conditions").and_then(Value::as_array) {
        for c in conds {
            let ctype = c.get("type").and_then(Value::as_str);
            let ctrue = c.get("status").and_then(Value::as_str) == Some("True");
            match (ctype, ctrue) {
                (Some("Complete"), true) => return Some(true),
                (Some("Failed"), true) => return Some(false),
                _ => {}
            }
        }
    }
    if status.get("succeeded").and_then(Value::as_i64).unwrap_or(0) > 0 {
        return Some(true);
    }
    if status.get("failed").and_then(Value::as_i64).unwrap_or(0) > 0 {
        return Some(false);
    }
    None
}

// ---------------------------------------------------------------------------
// Real transport — ureq + rustls, trusting the mounted cluster CA.
// ---------------------------------------------------------------------------

/// The live apiserver transport. Trusts the cluster CA and re-reads the SA
/// token per request. Not unit-tested (needs a cluster); the verb layer above
/// carries the tested logic.
pub struct UreqTransport {
    agent: ureq::Agent,
    api_base: String,
    token_path: PathBuf,
}

impl UreqTransport {
    pub fn new(cfg: &InClusterConfig) -> Result<Self, KubeError> {
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut cfg.ca_pem.as_slice()) {
            let cert =
                cert.map_err(|e| KubeError::Config(format!("parse cluster CA PEM: {e}")))?;
            roots
                .add(cert)
                .map_err(|e| KubeError::Config(format!("add cluster CA to trust store: {e}")))?;
        }
        if roots.is_empty() {
            return Err(KubeError::Config(
                "cluster CA bundle contained no certificates".into(),
            ));
        }
        let tls = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let agent = ureq::AgentBuilder::new().tls_config(Arc::new(tls)).build();
        Ok(Self {
            agent,
            api_base: cfg.api_base.clone(),
            token_path: cfg.token_path.clone(),
        })
    }

    fn bearer(&self) -> Result<String, KubeError> {
        std::fs::read_to_string(&self.token_path)
            .map(|t| t.trim().to_owned())
            .map_err(|e| KubeError::Config(format!("read SA token {}: {e}", self.token_path.display())))
    }
}

impl KubeTransport for UreqTransport {
    fn send(&self, req: &KubeRequest) -> Result<KubeResponse, KubeError> {
        let url = format!("{}{}", self.api_base, req.path);
        let token = self.bearer()?;
        let r = self
            .agent
            .request(req.method, &url)
            .set("Authorization", &format!("Bearer {token}"))
            .set("Accept", "application/json");
        let resp = match &req.body {
            Some(b) => r.send_json(b),
            None => r.call(),
        };
        // ureq treats non-2xx as Err(Status); we want the body either way so the
        // verb layer can parse the Status object.
        match resp {
            Ok(ok) => read_response(ok.status(), ok),
            Err(ureq::Error::Status(code, resp)) => read_response(code, resp),
            Err(ureq::Error::Transport(t)) => Err(KubeError::Transport(t.to_string())),
        }
    }
}

fn read_response(status: u16, resp: ureq::Response) -> Result<KubeResponse, KubeError> {
    let body = resp
        .into_json::<Value>()
        .unwrap_or(Value::Null); // a 204/empty body is fine; verb layer keys off status
    Ok(KubeResponse { status, body })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::cell::RefCell;

    /// A scripted transport: returns queued responses and records requests.
    struct FakeTransport {
        responses: RefCell<Vec<KubeResponse>>,
        seen: RefCell<Vec<(String, String)>>, // (method, path)
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
            self.seen
                .borrow_mut()
                .push((req.method.to_owned(), req.path.clone()));
            Ok(self.responses.borrow_mut().remove(0))
        }
    }

    fn resp(status: u16, body: Value) -> KubeResponse {
        KubeResponse { status, body }
    }

    #[test]
    fn create_job_paths_and_returns_body_on_201() {
        let fake = FakeTransport::new(vec![resp(201, json!({"metadata": {"name": "j1"}}))]);
        let api = KubeApi::new(fake);
        let job = json!({ "metadata": { "name": "j1" }, "kind": "Job" });
        let out = api.create_job("replay-sbx", &job).expect("created");
        assert_eq!(out["metadata"]["name"], json!("j1"));
        let seen = api.transport.seen.borrow();
        assert_eq!(seen[0].0, "POST");
        assert_eq!(seen[0].1, "/apis/batch/v1/namespaces/replay-sbx/jobs");
    }

    #[test]
    fn create_job_409_is_already_exists_not_error() {
        let fake = FakeTransport::new(vec![resp(
            409,
            json!({"reason": "AlreadyExists", "message": "jobs \"j1\" already exists"}),
        )]);
        let api = KubeApi::new(fake);
        let job = json!({ "metadata": { "name": "j1" } });
        match api.create_job("replay-sbx", &job) {
            Err(KubeError::AlreadyExists { name }) => assert_eq!(name, "j1"),
            other => panic!("expected AlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn non_2xx_surfaces_status_reason() {
        let fake = FakeTransport::new(vec![resp(
            403,
            json!({"reason": "Forbidden", "message": "no perms"}),
        )]);
        let api = KubeApi::new(fake);
        match api.get_configmap("replay-sbx", "job-template") {
            Err(KubeError::Api { status, reason }) => {
                assert_eq!(status, 403);
                assert!(reason.contains("Forbidden"));
                assert!(reason.contains("job-template"));
            }
            other => panic!("expected Api error, got {other:?}"),
        }
    }

    #[test]
    fn get_configmap_uses_core_api_path() {
        let fake = FakeTransport::new(vec![resp(200, json!({"data": {}}))]);
        let api = KubeApi::new(fake);
        api.get_configmap("replay-env", "job-template")
            .expect("configmap fetch");
        assert_eq!(
            api.transport.seen.borrow()[0].1,
            "/api/v1/namespaces/replay-env/configmaps/job-template"
        );
    }

    #[test]
    fn delete_job_sets_background_propagation() {
        let fake = FakeTransport::new(vec![resp(200, json!({"status": "Success"}))]);
        let api = KubeApi::new(fake);
        api.delete_job("replay-sbx", "j1").expect("delete job");
        assert!(api.transport.seen.borrow()[0]
            .1
            .ends_with("/jobs/j1?propagationPolicy=Background"));
    }

    #[test]
    fn list_jobs_builds_label_selector_path_and_returns_items() {
        let body = json!({
            "kind": "JobList",
            "items": [
                {
                    "metadata": { "name": "deja-replay-run-1", "labels": { "deja.run-id": "run-1" } },
                    "status": { "conditions": [{ "type": "Complete", "status": "True" }] }
                },
                {
                    "metadata": { "name": "deja-replay-run-2", "labels": { "deja.run-id": "run-2" } },
                    "status": { "active": 1 }
                }
            ]
        });
        let api = KubeApi::new(FakeTransport::new(vec![resp(200, body)]));
        let items = api.list_jobs("replay-sbx", "deja.run-id").expect("list jobs");
        assert_eq!(items.len(), 2);
        let seen = api.transport.seen.borrow();
        assert_eq!(seen[0].0, "GET");
        assert_eq!(
            seen[0].1,
            "/apis/batch/v1/namespaces/replay-sbx/jobs?labelSelector=deja.run-id"
        );
    }

    #[test]
    fn list_jobs_missing_items_is_empty_not_error() {
        // A JobList with no `.items` (nothing matched) yields an empty Vec.
        let api = KubeApi::new(FakeTransport::new(vec![resp(200, json!({"kind": "JobList"}))]));
        let items = api.list_jobs("replay-sbx", "deja.run-id").expect("list jobs");
        assert!(items.is_empty());
    }

    #[test]
    fn verdict_reads_conditions_then_counts() {
        let complete =
            json!({"status": {"conditions": [{"type": "Complete", "status": "True"}]}});
        let failed = json!({"status": {"conditions": [{"type": "Failed", "status": "True"}]}});
        let running = json!({"status": {"active": 1}});
        let by_count = json!({"status": {"succeeded": 1}});
        assert_eq!(job_terminal_verdict(&complete), Some(true));
        assert_eq!(job_terminal_verdict(&failed), Some(false));
        assert_eq!(job_terminal_verdict(&running), None);
        assert_eq!(job_terminal_verdict(&by_count), Some(true));
    }

    #[test]
    fn from_env_builds_base_and_reads_sa_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(dir.path().join("ca.crt"), b"-----BEGIN CERTIFICATE-----\n")
            .expect("write ca");
        std::fs::write(dir.path().join("namespace"), "replay-orchestrator-sandbox\n")
            .expect("write namespace");
        std::fs::write(dir.path().join("token"), "tok").expect("write token");
        // Guard the shared process env with a mutex-free approach: set/read/clear.
        std::env::set_var("KUBERNETES_SERVICE_HOST", "10.0.0.1");
        std::env::set_var("KUBERNETES_SERVICE_PORT", "443");
        let cfg = InClusterConfig::from_env_at(dir.path()).expect("config");
        assert_eq!(cfg.api_base, "https://10.0.0.1:443");
        assert_eq!(cfg.namespace, "replay-orchestrator-sandbox");
        assert!(cfg.token_path.ends_with("token"));
        std::env::remove_var("KUBERNETES_SERVICE_HOST");
        std::env::remove_var("KUBERNETES_SERVICE_PORT");
    }
}
