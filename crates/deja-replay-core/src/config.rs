//! TOML-first configuration shared by the dashboard and the replay agent.
//!
//! The `[s3]` table is ONE struct used by both sides, so bucket/prefix/creds
//! cannot drift. Env vars override individual keys (double underscore as the
//! table separator, e.g. `DEJA_S3__ACCESS_KEY`) so secrets can stay out of
//! files.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

fn default_region() -> String {
    "us-east-1".to_owned()
}

/// The `[s3]` table — identical shape in `dashboard.toml` and `agent.toml`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct S3Settings {
    #[serde(default = "default_region")]
    pub region: String,
    pub access_key: String,
    pub secret_key: String,
    /// AWS STS session token for temporary credentials.
    #[serde(default)]
    pub session_token: String,
    pub bucket: String,
    /// Key prefix for ALL objects; empty = today's bucket-root layout.
    #[serde(default)]
    pub prefix: String,
    /// Custom endpoint (MinIO/localstack). `None` = real AWS S3.
    #[serde(default)]
    pub endpoint: Option<String>,
}

impl S3Settings {
    /// Bridge to the compactor's store builder. `endpoint: None` maps to the
    /// empty string, which `S3Config::build` treats as real AWS (Task 2).
    pub fn to_compactor(&self) -> deja_compactor::S3Config {
        deja_compactor::S3Config {
            endpoint: self.endpoint.clone().unwrap_or_default(),
            bucket: self.bucket.clone(),
            access_key: self.access_key.clone(),
            secret_key: self.secret_key.clone(),
            session_token: self.session_token.clone(),
            region: self.region.clone(),
            allow_http: self
                .endpoint
                .as_deref()
                .is_some_and(|endpoint| endpoint.starts_with("http://")),
        }
    }

    /// Apply `DEJA_S3__*` overrides. `get` abstracts the process env so tests
    /// stay hermetic (no env mutation in parallel test runs).
    pub fn apply_overrides(&mut self, get: impl Fn(&str) -> Option<String>) {
        if let Some(v) = get("DEJA_S3__REGION") {
            self.region = v;
        }
        if let Some(v) = get("DEJA_S3__ACCESS_KEY") {
            self.access_key = v;
        }
        if let Some(v) = get("DEJA_S3__SECRET_KEY") {
            self.secret_key = v;
        }
        if let Some(v) = get("DEJA_S3__SESSION_TOKEN") {
            self.session_token = v;
        }
        if let Some(v) = get("DEJA_S3__BUCKET") {
            self.bucket = v;
        }
        if let Some(v) = get("DEJA_S3__PREFIX") {
            self.prefix = v;
        }
        if let Some(v) = get("DEJA_S3__ENDPOINT") {
            self.endpoint = Some(v);
        }
    }
}

// -- agent -------------------------------------------------------------------

/// `agent.toml` — the replay agent's whole configuration (spec §7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentConfig {
    pub run: RunSection,
    pub s3: S3Settings,
    pub router: RouterSection,
    pub stores: StoresSection,
    pub callback: CallbackSection,
    #[serde(default)]
    pub limits: LimitsSection,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunSection {
    pub run_id: String,
    pub recording_id: String,
    #[serde(default)]
    pub recording_uri: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouterSection {
    pub base_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoresSection {
    pub redis_url: String,
    pub pg_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CallbackSection {
    pub url: String,
    pub token: String,
}

fn default_health_deadline() -> u64 {
    300
}

fn default_request_timeout() -> u64 {
    30
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LimitsSection {
    #[serde(default = "default_health_deadline")]
    pub health_deadline_secs: u64,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,
}

impl Default for LimitsSection {
    fn default() -> Self {
        Self {
            health_deadline_secs: default_health_deadline(),
            request_timeout_secs: default_request_timeout(),
        }
    }
}

// -- dashboard ---------------------------------------------------------------

/// `dashboard.toml` — the always-on orchestrator's configuration (spec §7).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DashboardConfig {
    #[serde(default)]
    pub api: ApiSection,
    pub database: DatabaseSection,
    pub s3: S3Settings,
    /// Present means the Helm-sandbox replay driver is enabled.
    #[serde(default)]
    pub sandbox: Option<SandboxSection>,
}

fn default_bind() -> String {
    "0.0.0.0:8070".to_owned()
}

fn default_state_dir() -> String {
    "./harness-state".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiSection {
    #[serde(default = "default_bind")]
    pub bind: String,
    #[serde(default = "default_state_dir")]
    pub state_dir: String,
}

impl Default for ApiSection {
    fn default() -> Self {
        Self {
            bind: default_bind(),
            state_dir: default_state_dir(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DatabaseSection {
    pub url: String,
}

fn default_namespace_prefix() -> String {
    "deja-run-".to_owned()
}

fn default_run_deadline() -> u64 {
    1800
}

fn default_watchdog_interval() -> u64 {
    30
}

fn default_sandbox_chart() -> String {
    "/charts/replay-sandbox".to_owned()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxSection {
    #[serde(default = "default_sandbox_chart")]
    pub chart: String,
    #[serde(default = "default_namespace_prefix")]
    pub namespace_prefix: String,
    #[serde(default = "default_run_deadline")]
    pub run_deadline_secs: u64,
    #[serde(default = "default_watchdog_interval")]
    pub watchdog_interval_secs: u64,
    /// Host address pods can reach the dashboard on for verdict callbacks.
    pub callback_base_url: String,
}

// -- loading -----------------------------------------------------------------

#[derive(Debug)]
pub enum ConfigError {
    Io {
        path: PathBuf,
        source: std::io::Error,
    },
    Parse {
        path: PathBuf,
        message: String,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => write!(f, "config {}: {source}", path.display()),
            Self::Parse { path, message } => write!(f, "config {}: {message}", path.display()),
        }
    }
}

impl std::error::Error for ConfigError {}

fn load_toml<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, ConfigError> {
    let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Io {
        path: path.to_owned(),
        source,
    })?;
    toml::from_str(&text).map_err(|e| ConfigError::Parse {
        path: path.to_owned(),
        message: e.to_string(),
    })
}

/// Load `agent.toml`, then apply `DEJA_S3__*` process-env overrides.
pub fn load_agent_config(path: &Path) -> Result<AgentConfig, ConfigError> {
    let mut cfg: AgentConfig = load_toml(path)?;
    cfg.s3.apply_overrides(|key| std::env::var(key).ok());
    Ok(cfg)
}

/// Load `dashboard.toml`, then apply `DEJA_S3__*` process-env overrides.
pub fn load_dashboard_config(path: &Path) -> Result<DashboardConfig, ConfigError> {
    let mut cfg: DashboardConfig = load_toml(path)?;
    cfg.s3.apply_overrides(|key| std::env::var(key).ok());
    Ok(cfg)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;

    #[test]
    fn s3_settings_parse_with_defaults() {
        let s: S3Settings = toml::from_str(
            r#"
            access_key = "ak"
            secret_key = "sk"
            bucket = "deja-recordings"
            "#,
        )
        .unwrap();
        assert_eq!(s.region, "us-east-1");
        assert_eq!(s.prefix, "");
        assert_eq!(s.endpoint, None);
        assert_eq!(s.session_token, "");
        assert_eq!(s.bucket, "deja-recordings");
    }

    #[test]
    fn s3_settings_to_compactor_maps_endpoint_none_to_empty() {
        let s = S3Settings {
            region: "eu-west-1".into(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            session_token: "tok".into(),
            bucket: "b".into(),
            prefix: "deja/v1".into(),
            endpoint: None,
        };
        let c = s.to_compactor();
        assert_eq!(c.endpoint, "");
        assert_eq!(c.region, "eu-west-1");
        assert_eq!(c.bucket, "b");
        assert_eq!(c.session_token, "tok");
    }

    #[test]
    fn env_overrides_win_over_file_values() {
        let mut s = S3Settings {
            region: "us-east-1".into(),
            access_key: "file-ak".into(),
            secret_key: "file-sk".into(),
            session_token: "file-token".into(),
            bucket: "file-bucket".into(),
            prefix: "".into(),
            endpoint: None,
        };
        s.apply_overrides(|key| match key {
            "DEJA_S3__ACCESS_KEY" => Some("env-ak".into()),
            "DEJA_S3__SESSION_TOKEN" => Some("env-token".into()),
            "DEJA_S3__ENDPOINT" => Some("http://minio:9000".into()),
            "DEJA_S3__PREFIX" => Some("deja/v1".into()),
            _ => None,
        });
        assert_eq!(s.access_key, "env-ak");
        assert_eq!(s.secret_key, "file-sk"); // untouched
        assert_eq!(s.session_token, "env-token");
        assert_eq!(s.endpoint.as_deref(), Some("http://minio:9000"));
        assert_eq!(s.prefix, "deja/v1");
    }

    const AGENT_TOML: &str = r#"
        [run]
        run_id = "run-1"
        recording_id = "rec-1"

        [s3]
        access_key = "ak"
        secret_key = "sk"
        bucket = "deja-recordings"

        [router]
        base_url = "http://router:8080"

        [stores]
        redis_url = "redis://redis:6379"
        pg_url = "postgres://hs:hs@postgres:5432/hyperswitch"

        [callback]
        url = "http://host.k3d.internal:8070/api/v1/runs/run-1/verdict"
        token = "t0k3n"
    "#;

    #[test]
    fn agent_config_parses_with_limit_defaults() {
        let cfg: AgentConfig = toml::from_str(AGENT_TOML).unwrap();
        assert_eq!(cfg.run.run_id, "run-1");
        assert_eq!(cfg.limits.health_deadline_secs, 300);
        assert_eq!(cfg.limits.request_timeout_secs, 30);
        assert_eq!(cfg.stores.redis_url, "redis://redis:6379");
        assert!(cfg.run.recording_uri.is_none());
    }

    #[test]
    fn agent_config_accepts_direct_recording_uri() {
        let cfg: AgentConfig = toml::from_str(
            r#"
            [run]
            run_id = "run-1"
            recording_id = "rec-1"
            recording_uri = "s3://hyperswitch-art/2026/07/09/file.log.gz"

            [s3]
            access_key = "ak"
            secret_key = "sk"
            bucket = "deja-recordings"

            [router]
            base_url = "http://router:8080"

            [stores]
            redis_url = "redis://redis:6379"
            pg_url = "postgres://hs:hs@postgres:5432/hyperswitch"

            [callback]
            url = "http://host.k3d.internal:8070/api/v1/runs/run-1/verdict"
            token = "t0k3n"
            "#,
        )
        .unwrap();
        assert_eq!(
            cfg.run.recording_uri.as_deref(),
            Some("s3://hyperswitch-art/2026/07/09/file.log.gz")
        );
    }

    #[test]
    fn dashboard_config_parses_with_api_defaults_and_optional_sandbox() {
        let cfg: DashboardConfig = toml::from_str(
            r#"
            [database]
            url = "postgres://deja:deja@pg:5432/deja"

            [s3]
            access_key = "ak"
            secret_key = "sk"
            bucket = "deja-recordings"
            "#,
        )
        .unwrap();
        assert_eq!(cfg.api.bind, "0.0.0.0:8070");
        assert_eq!(cfg.api.state_dir, "./harness-state");
        assert!(cfg.sandbox.is_none());

        let with_sandbox: DashboardConfig = toml::from_str(
            r#"
            [database]
            url = "postgres://deja:deja@pg:5432/deja"

            [s3]
            access_key = "ak"
            secret_key = "sk"
            bucket = "deja-recordings"

            [sandbox]
            callback_base_url = "http://host.k3d.internal:8070"
            "#,
        )
        .unwrap();
        let sandbox = with_sandbox.sandbox.unwrap();
        assert_eq!(sandbox.chart, "/charts/replay-sandbox");
        assert_eq!(sandbox.namespace_prefix, "deja-run-");
        assert_eq!(sandbox.run_deadline_secs, 1800);
        assert_eq!(sandbox.watchdog_interval_secs, 30);
    }

    #[test]
    fn missing_required_key_is_a_named_parse_error() {
        let err = toml::from_str::<AgentConfig>("[run]\nrun_id = \"r\"\n").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("recording_id") || msg.contains("missing field"),
            "error must name what is missing: {msg}"
        );
    }

    #[test]
    fn load_agent_config_reads_file_and_reports_io_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("agent.toml");
        std::fs::write(&path, AGENT_TOML).unwrap();
        let cfg = load_agent_config(&path).unwrap();
        assert_eq!(cfg.run.recording_id, "rec-1");

        let missing = load_agent_config(&dir.path().join("nope.toml"));
        let msg = missing.unwrap_err().to_string();
        assert!(
            msg.contains("nope.toml"),
            "io error must name the path: {msg}"
        );
    }
}
