# deja-replay-core Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create the `deja-replay-core` library crate — shared TOML configuration, the single S3 key-layout module, and relocated ingest/lookup-render logic — that the sandbox replay agent (plan 3) and the dashboard (plan 4) will both link.

**Architecture:** New workspace library crate `crates/deja-replay-core`. It owns: (1) `layout` — every S3 key template in the platform; (2) `config` — `S3Settings` + `AgentConfig` + `DashboardConfig`, TOML-loaded with `DEJA_S3__*` env overrides; (3) `ingest` — recording pull from S3 (moved from `deja-orchestrator::s3`); (4) `lookup` — the lookup-table renderer (moved from `deja-orchestrator::lookup`) plus a new per-correlation filter. `deja-orchestrator` keeps compiling via re-export shims. `deja-compactor::S3Config` is extended with a `region` field and endpoint-less (real AWS) support.

**Tech Stack:** Rust 2021 (workspace pins rustc 1.85), serde, toml 0.8, existing workspace crates (`deja`, `deja-compactor`).

**Spec:** `docs/superpowers/specs/2026-07-08-sandbox-replay-design.md` (§3.1, §7; S3 layout paths from §3.1).

## Global Constraints

- Workspace lints are strict: `unsafe_code = "forbid"`, clippy `unwrap_used`, `todo`, `dbg_macro` all **deny**. No `.unwrap()` outside test modules; test modules start with `#[allow(clippy::unwrap_used)] // tests panic on failure by design` (existing repo idiom).
- Rust toolchain is pinned to 1.85 (`rust-toolchain.toml`); do not add dependencies requiring newer rustc. The ONLY new third-party dependency permitted by this plan is `toml = "0.8"`.
- All work on branch `feat/sandbox-replay-core`, created from current `main` HEAD. Keep staging explicit: always `git add <explicit paths>` — never `git add -A`, `-u`, or `.`.
- S3 key layout (spec §3.1): recordings `{prefix}/sessions/v1/{recording_id}/…`, run artifacts `{prefix}/runs/{run_id}/{name}`, candidate builds `{prefix}/builds/{build_ref}/router`. Empty prefix ⇒ keys identical to today's (backward compatible with existing MinIO data).
- Env override names use double underscore as table separator: `DEJA_S3__REGION`, `DEJA_S3__ACCESS_KEY`, `DEJA_S3__SECRET_KEY`, `DEJA_S3__BUCKET`, `DEJA_S3__PREFIX`, `DEJA_S3__ENDPOINT` (spec §7).
- Config defaults (spec §7): s3.region `"us-east-1"`, s3.prefix `""`; agent limits `health_deadline_secs = 300`, `request_timeout_secs = 30`; dashboard api.bind `"0.0.0.0:8070"`, sandbox namespace_prefix `"deja-run-"`, run_deadline_secs `1800`, watchdog_interval_secs `30`.
- Final gate: `just verify` (fmt-check + clippy `-D warnings` + workspace tests) must pass.
- Commit messages end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`

---

### Task 1: Crate scaffold + S3 layout module

**Files:**
- Create: `crates/deja-replay-core/Cargo.toml`
- Create: `crates/deja-replay-core/src/lib.rs`
- Create: `crates/deja-replay-core/src/layout.rs`
- Modify: `Cargo.toml` (workspace members)

**Interfaces:**
- Produces: `deja_replay_core::layout::{recording_session_root, run_artifact, candidate_build}` — all `fn(prefix: &str, …) -> String`. Later plans (agent, dashboard) build every S3 key through these.

- [ ] **Step 1: Create branch**

```bash
cd /Users/nishanth.challa/Desktop/deja
git checkout -b feat/sandbox-replay-core
```

- [ ] **Step 2: Scaffold the crate**

Create `crates/deja-replay-core/Cargo.toml`:

```toml
[package]
name = "deja-replay-core"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true
authors.workspace = true
description = "Shared foundation for the Deja sandbox replay platform: TOML configuration, S3 key layout, recording ingest, lookup rendering and per-correlation filtering."

[dependencies]
deja = { path = "../deja", default-features = false }
deja-compactor = { path = "../deja-compactor" }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"

[dev-dependencies]
tempfile = "3"

[lints]
workspace = true
```

Create `crates/deja-replay-core/src/lib.rs`:

```rust
//! Shared foundation for the Deja sandbox replay platform.
//!
//! Linked by BOTH the dashboard (`deja-orchestrator`) and the in-sandbox
//! replay agent, so S3 key layout and configuration shapes cannot drift
//! between the two sides.

pub mod layout;

pub use deja_compactor::S3Config;
```

Modify the workspace `Cargo.toml` members list — insert alphabetically:

```toml
    "crates/deja-orchestrator",
    "crates/deja-replay-core",
    "crates/deja-store",
```

- [ ] **Step 3: Write the failing layout tests**

Create `crates/deja-replay-core/src/layout.rs`:

```rust
//! Every S3 key template in the replay platform lives here — nowhere else.
//!
//! All functions take the configured key `prefix` explicitly; an empty
//! prefix yields exactly today's bucket-root layout, so existing recordings
//! remain addressable.

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;

    #[test]
    fn empty_prefix_matches_legacy_layout() {
        assert_eq!(
            recording_session_root("", "rec-1"),
            "sessions/v1/rec-1"
        );
        assert_eq!(run_artifact("", "run-1", "scorecard.json"), "runs/run-1/scorecard.json");
        assert_eq!(candidate_build("", "abc123"), "builds/abc123/router");
    }

    #[test]
    fn prefix_is_joined_and_slash_trimmed() {
        assert_eq!(
            recording_session_root("/deja/v1/", "rec-1"),
            "deja/v1/sessions/v1/rec-1"
        );
        assert_eq!(
            run_artifact("deja/v1", "run-1", "agent.log"),
            "deja/v1/runs/run-1/agent.log"
        );
        assert_eq!(
            candidate_build("deja/v1", "sha-9f"),
            "deja/v1/builds/sha-9f/router"
        );
    }
}
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `cargo test -p deja-replay-core`
Expected: COMPILE ERROR — `recording_session_root` (etc.) not found.

- [ ] **Step 5: Implement the layout functions**

Prepend to `crates/deja-replay-core/src/layout.rs` (above the test module):

```rust
fn join(prefix: &str, rest: &str) -> String {
    let p = prefix.trim_matches('/');
    if p.is_empty() {
        rest.to_owned()
    } else {
        format!("{p}/{rest}")
    }
}

/// Root of a sealed recording session: `{prefix}/sessions/v1/{recording_id}`.
/// Delegates the un-prefixed tail to `deja_compactor::layout` so the two
/// crates cannot disagree about the session shape.
pub fn recording_session_root(prefix: &str, recording_id: &str) -> String {
    join(prefix, &deja_compactor::layout::session_root(recording_id))
}

/// A run artifact object: `{prefix}/runs/{run_id}/{name}`.
pub fn run_artifact(prefix: &str, run_id: &str, name: &str) -> String {
    join(prefix, &format!("runs/{run_id}/{name}"))
}

/// A published candidate router binary: `{prefix}/builds/{build_ref}/router`.
pub fn candidate_build(prefix: &str, build_ref: &str) -> String {
    join(prefix, &format!("builds/{build_ref}/router"))
}
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `cargo test -p deja-replay-core`
Expected: `test result: ok. 2 passed`

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock crates/deja-replay-core
git commit -m "feat(replay-core): scaffold crate with S3 key layout module

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: `region` + endpoint-less support on `deja_compactor::S3Config`

**Files:**
- Modify: `crates/deja-compactor/src/lib.rs:44-75` (struct, `from_env`, `build`)

**Interfaces:**
- Produces: `S3Config { endpoint, bucket, access_key, secret_key, region }` — `region: String` is the new field. `endpoint == ""` now means "real AWS: no custom endpoint, no allow_http". Task 3's `S3Settings::to_compactor` relies on both.

- [ ] **Step 1: Write the failing test**

Append to `crates/deja-compactor/src/lib.rs` (inside the existing test module if one exists at file end; otherwise create one):

```rust
#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod s3_config_tests {
    use super::*;

    #[test]
    fn from_env_defaults_region() {
        // DEJA_S3_REGION is never set by other tests; default must apply.
        let cfg = S3Config::from_env();
        assert_eq!(cfg.region, "us-east-1");
    }

    #[test]
    fn endpointless_config_builds_store() {
        let cfg = S3Config {
            endpoint: String::new(),
            bucket: "b".into(),
            access_key: "a".into(),
            secret_key: "s".into(),
            region: "eu-west-1".into(),
        };
        assert!(cfg.build().is_ok(), "empty endpoint must mean real AWS, not an error");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p deja-compactor s3_config_tests`
Expected: COMPILE ERROR — no field `region` on `S3Config`.

- [ ] **Step 3: Implement**

In `crates/deja-compactor/src/lib.rs`, change the struct (currently lines 44-49):

```rust
pub struct S3Config {
    pub endpoint: String,
    pub bucket: String,
    pub access_key: String,
    pub secret_key: String,
    /// AWS region; ignored by MinIO but required by real S3.
    pub region: String,
}
```

In `from_env` add the region line alongside the others:

```rust
            region: env("DEJA_S3_REGION", "us-east-1"),
```

In `build()`, replace the fixed builder chain head (currently
`AmazonS3Builder::new().with_endpoint(...)...with_region("us-east-1").with_allow_http(true)`) with a conditional endpoint:

```rust
        let mut builder = AmazonS3Builder::new()
            .with_bucket_name(&self.bucket)
            .with_access_key_id(&self.access_key)
            .with_secret_access_key(&self.secret_key)
            .with_region(&self.region);
        // Empty endpoint = real AWS S3 (default resolver, TLS only). A set
        // endpoint = MinIO/localstack, which needs plain http allowed.
        if !self.endpoint.is_empty() {
            builder = builder.with_endpoint(&self.endpoint).with_allow_http(true);
        }
        let store = builder
```

Keep the remainder of the existing chain (the `.build()` call and error
mapping) exactly as it is today.

Then fix every struct-literal construction of `S3Config` elsewhere:

Run: `grep -rn "S3Config {" crates/ --include="*.rs"`
For each hit that is a literal construction (not the definition), add
`region: "us-east-1".into(),`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p deja-compactor && cargo build --workspace`
Expected: compactor tests PASS (including the two new ones); workspace builds clean.

- [ ] **Step 5: Commit**

```bash
git add crates/deja-compactor/src/lib.rs
git commit -m "feat(compactor): add region to S3Config; empty endpoint means real AWS

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: `S3Settings` — the shared `[s3]` TOML table

**Files:**
- Create: `crates/deja-replay-core/src/config.rs`
- Modify: `crates/deja-replay-core/src/lib.rs` (add `pub mod config;`)

**Interfaces:**
- Consumes: `deja_compactor::S3Config` with `region` (Task 2).
- Produces: `config::S3Settings { region, access_key, secret_key, bucket, prefix, endpoint: Option<String> }` with `fn to_compactor(&self) -> deja_compactor::S3Config` and `fn apply_overrides(&mut self, get: impl Fn(&str) -> Option<String>)`. Tasks 4+ and later plans embed this struct as their `[s3]` section.

- [ ] **Step 1: Write the failing tests**

Create `crates/deja-replay-core/src/config.rs`:

```rust
//! TOML-first configuration shared by the dashboard and the replay agent.
//!
//! The `[s3]` table is ONE struct used by both sides, so bucket/prefix/creds
//! cannot drift. Env vars override individual keys (double underscore as the
//! table separator, e.g. `DEJA_S3__ACCESS_KEY`) so secrets can stay out of
//! files.

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
        assert_eq!(s.bucket, "deja-recordings");
    }

    #[test]
    fn s3_settings_to_compactor_maps_endpoint_none_to_empty() {
        let s = S3Settings {
            region: "eu-west-1".into(),
            access_key: "ak".into(),
            secret_key: "sk".into(),
            bucket: "b".into(),
            prefix: "deja/v1".into(),
            endpoint: None,
        };
        let c = s.to_compactor();
        assert_eq!(c.endpoint, "");
        assert_eq!(c.region, "eu-west-1");
        assert_eq!(c.bucket, "b");
    }

    #[test]
    fn env_overrides_win_over_file_values() {
        let mut s = S3Settings {
            region: "us-east-1".into(),
            access_key: "file-ak".into(),
            secret_key: "file-sk".into(),
            bucket: "file-bucket".into(),
            prefix: "".into(),
            endpoint: None,
        };
        s.apply_overrides(|key| match key {
            "DEJA_S3__ACCESS_KEY" => Some("env-ak".into()),
            "DEJA_S3__ENDPOINT" => Some("http://minio:9000".into()),
            "DEJA_S3__PREFIX" => Some("deja/v1".into()),
            _ => None,
        });
        assert_eq!(s.access_key, "env-ak");
        assert_eq!(s.secret_key, "file-sk"); // untouched
        assert_eq!(s.endpoint.as_deref(), Some("http://minio:9000"));
        assert_eq!(s.prefix, "deja/v1");
    }
}
```

Add to `crates/deja-replay-core/src/lib.rs`:

```rust
pub mod config;
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p deja-replay-core config`
Expected: COMPILE ERROR — `S3Settings` not found.

- [ ] **Step 3: Implement `S3Settings`**

Prepend to `crates/deja-replay-core/src/config.rs` (above the test module):

```rust
use serde::{Deserialize, Serialize};

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
            region: self.region.clone(),
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p deja-replay-core config`
Expected: `3 passed`

- [ ] **Step 5: Commit**

```bash
git add crates/deja-replay-core/src/config.rs crates/deja-replay-core/src/lib.rs
git commit -m "feat(replay-core): shared S3Settings TOML table with env overrides

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: `AgentConfig` + `DashboardConfig` + file loaders

**Files:**
- Modify: `crates/deja-replay-core/src/config.rs`

**Interfaces:**
- Consumes: `S3Settings` (Task 3).
- Produces (used by plans 3 and 4):
  - `config::AgentConfig { run: RunSection, s3: S3Settings, router: RouterSection, stores: StoresSection, callback: CallbackSection, limits: LimitsSection }`
  - `config::DashboardConfig { api: ApiSection, database: DatabaseSection, s3: S3Settings, sandbox: Option<SandboxSection> }`
  - `config::load_agent_config(path: &Path) -> Result<AgentConfig, ConfigError>` and `config::load_dashboard_config(path: &Path) -> Result<DashboardConfig, ConfigError>` — both apply `DEJA_S3__*` process-env overrides after parsing.
  - `config::ConfigError` (Io | Parse), `Display` names the offending path and, for parse errors, the missing/invalid key.

- [ ] **Step 1: Write the failing tests**

Append inside the test module of `crates/deja-replay-core/src/config.rs`:

```rust
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
        lookup_admin = "http://router:8080/deja/lookup"

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
            chart = "/charts/replay-sandbox"
            callback_base_url = "http://host.k3d.internal:8070"
            "#,
        )
        .unwrap();
        let sandbox = with_sandbox.sandbox.unwrap();
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
        assert!(msg.contains("nope.toml"), "io error must name the path: {msg}");
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p deja-replay-core config`
Expected: COMPILE ERROR — `AgentConfig` not found.

- [ ] **Step 3: Implement the config structs and loaders**

Add to `crates/deja-replay-core/src/config.rs` (after `S3Settings`):

```rust
use std::path::{Path, PathBuf};

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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouterSection {
    pub base_url: String,
    pub lookup_admin: String,
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
    /// Present ⇒ the helm-sandbox replay driver is enabled.
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxSection {
    pub chart: String,
    #[serde(default = "default_namespace_prefix")]
    pub namespace_prefix: String,
    #[serde(default = "default_run_deadline")]
    pub run_deadline_secs: u64,
    #[serde(default = "default_watchdog_interval")]
    pub watchdog_interval_secs: u64,
    /// Host address PODS can reach the dashboard on (verdict callbacks).
    pub callback_base_url: String,
}

// -- loading -----------------------------------------------------------------

#[derive(Debug)]
pub enum ConfigError {
    Io { path: PathBuf, source: std::io::Error },
    Parse { path: PathBuf, message: String },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, source } => {
                write!(f, "config {}: {source}", path.display())
            }
            Self::Parse { path, message } => {
                write!(f, "config {}: {message}", path.display())
            }
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p deja-replay-core config`
Expected: `7 passed` (3 from Task 3 + 4 new)

- [ ] **Step 5: Commit**

```bash
git add crates/deja-replay-core/src/config.rs
git commit -m "feat(replay-core): AgentConfig and DashboardConfig TOML loaders

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: Relocate ingest + lookup renderer into `deja-replay-core`

**Files:**
- Create: `crates/deja-replay-core/src/ingest.rs` (contents of `crates/deja-orchestrator/src/s3/mod.rs`)
- Create: `crates/deja-replay-core/src/lookup.rs` (contents of `crates/deja-orchestrator/src/lookup/mod.rs`)
- Modify: `crates/deja-replay-core/src/lib.rs` (add modules)
- Modify: `crates/deja-orchestrator/src/s3/mod.rs` → re-export shim
- Modify: `crates/deja-orchestrator/src/lookup/mod.rs` → re-export shim
- Modify: `crates/deja-orchestrator/Cargo.toml` (add `deja-replay-core` dependency)

**Interfaces:**
- Consumes: nothing new (moved code already depends only on `deja`, `deja-compactor`, `serde_json`).
- Produces (agent plan consumes these directly from `deja_replay_core`):
  - `ingest::{IngestReport, count_session_objects(cfg, recording_id) -> Result<usize, String>, pull_recording(cfg: &S3Config, recording_id: &str, dest: &Path) -> Result<(IngestReport, deja_compactor::SessionManifest), String>}`
  - `lookup::render_lookup_table(recording_path: &Path, recording_id: &str, policy_version: u32) -> io::Result<deja::LookupTable>`
- Orchestrator's existing `crate::s3::…` / `crate::lookup::…` call sites keep compiling unchanged via the shims.

- [ ] **Step 1: Move the two files verbatim**

Plain copy (`git mv` is not used — the destination is a different crate and
the source files become shims in place):

```bash
cp crates/deja-orchestrator/src/s3/mod.rs crates/deja-replay-core/src/ingest.rs
cp crates/deja-orchestrator/src/lookup/mod.rs crates/deja-replay-core/src/lookup.rs
```

In `crates/deja-replay-core/src/ingest.rs`: the file already uses only
`deja_compactor` + `serde` + `serde_json` paths — no edits needed beyond the
module doc (keep it).

In `crates/deja-replay-core/src/lookup.rs`: imports are `use deja::{…}` —
also no path edits needed.

Add to `crates/deja-replay-core/src/lib.rs`:

```rust
pub mod ingest;
pub mod lookup;
```

- [ ] **Step 2: Replace the orchestrator modules with shims**

Replace the ENTIRE contents of `crates/deja-orchestrator/src/s3/mod.rs` with:

```rust
//! Recording ingest — relocated to `deja-replay-core` so the in-sandbox
//! replay agent shares the exact same S3 pull path. This module is a
//! re-export shim keeping existing `crate::s3::…` call sites stable.

pub use deja_replay_core::ingest::{count_session_objects, pull_recording, IngestReport};
pub use deja_replay_core::S3Config;
```

Replace the ENTIRE contents of `crates/deja-orchestrator/src/lookup/mod.rs` with:

```rust
//! Lookup-table renderer — relocated to `deja-replay-core` (shared with the
//! replay agent). Re-export shim for existing `crate::lookup::…` call sites.

pub use deja_replay_core::lookup::render_lookup_table;
```

Add to `crates/deja-orchestrator/Cargo.toml` `[dependencies]` (next to the
other path deps):

```toml
deja-replay-core = { path = "../deja-replay-core" }
```

- [ ] **Step 3: Run the workspace tests**

Run: `cargo test --workspace`
Expected: PASS. The moved test modules (2 ingest tests, the renderer tests)
now run inside `deja-replay-core`; orchestrator totals drop accordingly, no
failures. If the renderer tests fail to compile in their new home, the fix is
a missing dev-dependency — `tempfile = "3"` is already declared (Task 1); do
NOT add anything else.

- [ ] **Step 4: Commit**

```bash
git add crates/deja-replay-core/src/ingest.rs crates/deja-replay-core/src/lookup.rs \
        crates/deja-replay-core/src/lib.rs \
        crates/deja-orchestrator/src/s3/mod.rs crates/deja-orchestrator/src/lookup/mod.rs \
        crates/deja-orchestrator/Cargo.toml Cargo.lock
git commit -m "refactor: relocate recording ingest + lookup renderer to deja-replay-core

Orchestrator keeps its call sites via re-export shims; the sandbox replay
agent will link the same code, so ingest and rendering cannot drift.

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Per-correlation lookup filter

**Files:**
- Modify: `crates/deja-replay-core/src/lookup.rs`

**Interfaces:**
- Consumes: `deja::{LookupTable, LookupEntry, LookupKey}` (facade re-exports; `LookupKey.correlation_id: Option<String>`).
- Produces: `lookup::table_for_correlation(table: &LookupTable, correlation: Option<&str>) -> LookupTable` — the agent (plan 3) calls this once per request to build the table it pushes to the router IMC; `None` selects the ambient (null-correlation) entries.

- [ ] **Step 1: Write the failing test**

Append inside the existing test module of `crates/deja-replay-core/src/lookup.rs`:

```rust
    fn entry_for(corr: Option<&str>, seq: u64) -> deja::LookupEntry {
        deja::LookupEntry {
            key: deja::LookupKey {
                correlation_id: corr.map(str::to_owned),
                bucket_id: None,
                fork_seq: 0,
                address: deja::Address::Sequence {
                    boundary: "redis".into(),
                    method: "get".into(),
                    request_sequence: seq,
                },
                args_hash: 0,
                occurrence: 0,
            },
            result: serde_json::json!("v"),
            source_event_global_sequence: seq,
        }
    }

    #[test]
    fn table_for_correlation_partitions_by_correlation_id() {
        let table = deja::LookupTable {
            recording_id: "rec-1".into(),
            policy_version: 1,
            entries: vec![
                entry_for(Some("c-1"), 0),
                entry_for(Some("c-2"), 1),
                entry_for(None, 2),
                entry_for(Some("c-1"), 3),
            ],
        };

        let c1 = table_for_correlation(&table, Some("c-1"));
        assert_eq!(c1.recording_id, "rec-1");
        assert_eq!(c1.policy_version, 1);
        assert_eq!(c1.entries.len(), 2);
        assert!(c1
            .entries
            .iter()
            .all(|e| e.key.correlation_id.as_deref() == Some("c-1")));

        let ambient = table_for_correlation(&table, None);
        assert_eq!(ambient.entries.len(), 1);
        assert_eq!(ambient.entries[0].key.correlation_id, None);

        let absent = table_for_correlation(&table, Some("c-404"));
        assert!(absent.entries.is_empty());
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p deja-replay-core table_for_correlation`
Expected: COMPILE ERROR — `table_for_correlation` not found.

- [ ] **Step 3: Implement**

Add to `crates/deja-replay-core/src/lookup.rs` after `render_lookup_table`:

```rust
/// Extract ONE correlation's slice of a rendered table — the per-request
/// table the replay agent pushes to the router IMC before driving that
/// request (spec §4). `None` selects the ambient (null-correlation) entries,
/// which are pushed once at agent start and never cleared.
pub fn table_for_correlation(table: &LookupTable, correlation: Option<&str>) -> LookupTable {
    LookupTable {
        recording_id: table.recording_id.clone(),
        policy_version: table.policy_version,
        entries: table
            .entries
            .iter()
            .filter(|entry| entry.key.correlation_id.as_deref() == correlation)
            .cloned()
            .collect(),
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p deja-replay-core`
Expected: all crate tests PASS (layout 2, config 7, renderer suite, filter 1).

- [ ] **Step 5: Full verification gate**

Run: `just verify`
Expected: fmt-check clean, clippy clean (`-D warnings`), workspace tests all PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/deja-replay-core/src/lookup.rs
git commit -m "feat(replay-core): per-correlation lookup table filter

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

## Plan Self-Review (completed)

- **Spec coverage (this plan's slice of §3.1 + §7):** layout templates ✔ (Task 1), region/access/secret/bucket/prefix maintained centrally ✔ (Tasks 2-3), TOML files with env overrides + startup errors naming missing keys ✔ (Task 4), ingest + renderer relocation with orchestrator re-exports ✔ (Task 5), per-correlation filter ✔ (Task 6). Seed-planning relocation (spec §3.1 mentions it) is deferred to plan 3 where its docker-exec I/O is rewritten against direct clients — relocating it now would move code plan 3 immediately rewrites.
- **Placeholder scan:** none — every step carries code or an exact command.
- **Type consistency:** `S3Settings.to_compactor` matches Task 2's five-field `S3Config`; `table_for_correlation` uses `LookupKey.correlation_id: Option<String>` as verified in `replay.rs:1069`; loader names match between Interfaces blocks and code.
