//! Replay-harness orchestrator library.
//!
//! Types and store layer shared between the HTTP handlers (in `main.rs`)
//! and the future fill-in modules (lookup-table renderer, divergence
//! detector, candidate resolvers). Kept dependency-light for now —
//! filesystem-JSON metadata, no SQLite yet, no async runtime.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub mod api;
pub mod divergence;
pub mod lifecycle;
pub mod lookup;
pub mod s3;
pub mod store;

/// Specification of a candidate Hyperswitch identity. All five resolution
/// modes promised in the plan; only `LocalPath` has a real backing impl in
/// the first cut (task #7 lands the rest).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CandidateSpec {
    LocalPath { binary_or_source: PathBuf },
    PrebuiltImage { image: String },
    S3Build { build_ref: String },
    RepoSha { repo: String, sha: String },
    RepoBranch { repo: String, branch: String },
    RepoTag { repo: String, tag: String },
    RepoPr { repo: String, pr: u32 },
}

/// Source ref used to run Hyperswitch schema migrations for a replay sandbox.
/// This is separate from the router image identity because hosted replays may
/// use an already-built image while still needing migrations from the branch,
/// commit, or tag that produced it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MigrationSource {
    Branch { branch: String },
    Sha { sha: String },
    Tag { tag: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateImage {
    pub docker_image: String,
    pub source_ref: String,
}

/// Runtime dependency image tags supplied by the dashboard per replay run.
/// Values are Docker tags, not full image references: e.g. `17-alpine`,
/// `7.2-alpine`, `0.112.0`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct RuntimeVersions {
    #[serde(default)]
    pub postgres: Option<String>,
    #[serde(default)]
    pub redis: Option<String>,
    #[serde(default)]
    pub superposition: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Record,
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Resolving,
    Building,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSpec {
    pub mode: RunMode,
    pub candidate_spec: CandidateSpec,
    /// Optional explicit source for replay database migrations. When omitted
    /// for repo candidates, the sandbox driver uses the same branch/SHA/tag as
    /// the candidate; prebuilt image sandbox runs must provide this explicitly.
    #[serde(default)]
    pub migration_source: Option<MigrationSource>,
    /// For mode=replay: which recording to drive.
    pub recording_id: Option<String>,
    /// Optional direct S3 source for the recording, e.g.
    /// `s3://bucket/prefix-or-object.log.gz`. When present, sandbox agents
    /// fetch and merge the matching objects into the canonical events file
    /// before replaying.
    #[serde(default)]
    pub recording_uri: Option<String>,
    /// Optional dependency image tags chosen by the dashboard for this run.
    #[serde(default)]
    pub runtime_versions: RuntimeVersions,
    /// For mode=record: workload arguments (kept opaque for now).
    #[serde(default)]
    pub workload: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub run_id: String,
    pub spec: RunSpec,
    pub status: RunStatus,
    pub recording_id: Option<String>,
    pub candidate_image: Option<CandidateImage>,
    pub failure_reason: Option<String>,
    /// Human-facing progress (separate from the coarse `status`): the current
    /// sub-step label, its 1-based index, and the total for this run's mode, so
    /// a client can render `[step/total] stage`. `stage_updated_ms` is the wall
    /// clock when the stage last changed — a climbing "time in stage" with a
    /// static step is how you tell "slow" from "stuck".
    #[serde(default)]
    pub stage: Option<String>,
    #[serde(default)]
    pub step: u32,
    #[serde(default)]
    pub steps_total: u32,
    #[serde(default)]
    pub stage_updated_ms: u64,
}

/// Milliseconds since the UNIX epoch (best-effort; 0 on clock error).
pub fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Minimal "give me a unique id" helper. Time-based for now; SQLite/UUID
/// can swap in later.
pub fn new_id(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos:x}")
}

/// On-disk root for harness state. Defaults to `./harness-state` relative
/// to the working directory. Layout:
///   {root}/runs/{run_id}.json
///   {root}/recordings/{recording_id}/events.jsonl
///   {root}/lookup-tables/{run_id}.jsonl
///   {root}/observed/{run_id}.jsonl
///   {root}/http-diffs/{run_id}.jsonl
pub struct HarnessRoot {
    pub root: PathBuf,
}

impl HarnessRoot {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        for sub in [
            "runs",
            "recordings",
            "lookup-tables",
            "observed",
            "http-diffs",
        ] {
            fs::create_dir_all(root.join(sub))?;
        }
        Ok(Self { root })
    }

    pub fn run_path(&self, run_id: &str) -> PathBuf {
        self.root.join("runs").join(format!("{run_id}.json"))
    }
    pub fn recording_events_path(&self, recording_id: &str) -> PathBuf {
        self.root
            .join("recordings")
            .join(recording_id)
            .join("events.jsonl")
    }
    pub fn lookup_table_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("lookup-tables")
            .join(format!("{run_id}.jsonl"))
    }
    pub fn observed_path(&self, run_id: &str) -> PathBuf {
        self.root.join("observed").join(format!("{run_id}.jsonl"))
    }
    pub fn http_diff_path(&self, run_id: &str) -> PathBuf {
        self.root.join("http-diffs").join(format!("{run_id}.jsonl"))
    }
    /// Per-run docker build context for `local_binary` candidates.
    pub fn candidate_stage_dir(&self, run_id: &str) -> PathBuf {
        self.root.join("candidates").join(run_id)
    }
    pub fn scorecard_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("runs")
            .join(format!("{run_id}.scorecard.json"))
    }
    /// Per-call divergence ledger sidecar (one CallRecord per line).
    pub fn call_ledger_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("runs")
            .join(format!("{run_id}.call-ledger.jsonl"))
    }
    /// Seed/readback certificate sidecar written before the replay kernel runs.
    pub fn seed_certificate_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("runs")
            .join(format!("{run_id}.seed-certificate.json"))
    }
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    fs::write(path, bytes)
}

pub fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<T> {
    let bytes = fs::read(path)?;
    serde_json::from_slice::<T>(&bytes).map_err(io::Error::other)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;

    #[test]
    fn candidate_spec_accepts_s3_build_ref() {
        let spec: CandidateSpec = serde_json::from_value(serde_json::json!({
            "kind": "s3_build",
            "build_ref": "sha-abc123"
        }))
        .unwrap();
        assert!(matches!(spec, CandidateSpec::S3Build { build_ref } if build_ref == "sha-abc123"));
    }

    #[test]
    fn run_spec_accepts_explicit_migration_source() {
        let spec: RunSpec = serde_json::from_value(serde_json::json!({
            "mode": "replay",
            "candidate_spec": {
                "kind": "prebuilt_image",
                "image": "registry/router:feature-pay-fix"
            },
            "migration_source": {
                "kind": "branch",
                "branch": "feature/pay-fix"
            },
            "recording_uri": "s3://hyperswitch-art/2026/07/09/",
            "runtime_versions": {
                "postgres": "17-alpine",
                "redis": "7.2-alpine",
                "superposition": "0.112.0"
            },
            "recording_id": "rec-1"
        }))
        .unwrap();
        assert!(matches!(
            spec.migration_source,
            Some(MigrationSource::Branch { branch }) if branch == "feature/pay-fix"
        ));
        assert_eq!(
            spec.recording_uri.as_deref(),
            Some("s3://hyperswitch-art/2026/07/09/")
        );
        assert_eq!(
            spec.runtime_versions,
            RuntimeVersions {
                postgres: Some("17-alpine".into()),
                redis: Some("7.2-alpine".into()),
                superposition: Some("0.112.0".into()),
            }
        );
    }
}
