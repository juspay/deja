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
    RepoSha { repo: String, sha: String },
    RepoBranch { repo: String, branch: String },
    RepoPr { repo: String, pr: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateImage {
    pub docker_image: String,
    pub source_ref: String,
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
    /// For mode=replay: which recording to drive.
    pub recording_id: Option<String>,
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
    /// Record-side execution graph dir (bind-mounted into the record router as
    /// `DEJA_GRAPH_DIR`); the layer writes `execution-graph.jsonl` inside it.
    pub fn graph_record_dir(&self, recording_id: &str) -> PathBuf {
        self.root.join("graph").join(recording_id)
    }
    /// Replay-side execution graph dir for one run.
    pub fn graph_replay_dir(&self, run_id: &str) -> PathBuf {
        self.root.join("graph-replay").join(run_id)
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
