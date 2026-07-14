//! Replay-harness orchestrator library.
//!
//! Types and store layer shared between the HTTP handlers (in `main.rs`)
//! and the future fill-in modules (lookup-table renderer, divergence
//! detector, candidate resolvers). Kept dependency-light for now —
//! filesystem-JSON metadata, no SQLite yet, no async runtime.

use std::fs;
use std::io::{self, BufRead, Write};
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
///   {root}/recordings/{recording_id}/graph.jsonl
///   {root}/lookup-tables/{run_id}.jsonl
///   {root}/observed/{run_id}.jsonl
///   {root}/http-diffs/{run_id}.jsonl
///   {root}/runs/{run_id}.graph-replay.jsonl
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
    pub fn recording_graph_path(&self, recording_id: &str) -> PathBuf {
        self.root
            .join("recordings")
            .join(recording_id)
            .join("graph.jsonl")
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
    /// Replay-side execution graph extracted from the observed stream.
    pub fn replay_graph_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("runs")
            .join(format!("{run_id}.graph-replay.jsonl"))
    }
    /// Seed/readback certificate sidecar written before the replay kernel runs.
    pub fn seed_certificate_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("runs")
            .join(format!("{run_id}.seed-certificate.json"))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecordingArtifactSplit {
    pub boundary_events: usize,
    pub graph_nodes: usize,
}

/// Split a recording tape into replay-ready boundary events plus a graph
/// sidecar. S3 recordings can contain millions of execution-graph nodes next
/// to the much smaller boundary stream; keeping `events.jsonl` boundary-only
/// makes lookup rendering scale with replayable work while preserving the graph
/// for the dashboard.
pub fn split_recording_artifacts(
    events_path: &Path,
    graph_path: &Path,
) -> io::Result<RecordingArtifactSplit> {
    let file = match fs::File::open(events_path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            remove_file_if_exists(graph_path)?;
            return Ok(RecordingArtifactSplit::default());
        }
        Err(e) => return Err(e),
    };

    if let Some(parent) = events_path.parent() {
        fs::create_dir_all(parent)?;
    }
    if let Some(parent) = graph_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let events_tmp = events_path.with_extension("jsonl.events.tmp");
    let graph_tmp = graph_path.with_extension("jsonl.tmp");
    let mut events_out = fs::File::create(&events_tmp)?;
    let mut graph_out = fs::File::create(&graph_tmp)?;
    let mut split = RecordingArtifactSplit::default();

    for line in io::BufReader::new(file).lines() {
        let line = line?;
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        if has_record_kind(trimmed, "boundary_event") {
            events_out.write_all(trimmed.as_bytes())?;
            events_out.write_all(b"\n")?;
            split.boundary_events += 1;
            continue;
        }
        if has_record_kind(trimmed, "graph_node") {
            graph_out.write_all(trimmed.as_bytes())?;
            graph_out.write_all(b"\n")?;
            split.graph_nodes += 1;
            continue;
        }

        match serde_json::from_str::<deja::DejaRecord>(trimmed) {
            Ok(deja::DejaRecord::BoundaryEvent(_)) => {
                events_out.write_all(trimmed.as_bytes())?;
                events_out.write_all(b"\n")?;
                split.boundary_events += 1;
            }
            Ok(deja::DejaRecord::GraphNode(_)) => {
                graph_out.write_all(trimmed.as_bytes())?;
                graph_out.write_all(b"\n")?;
                split.graph_nodes += 1;
            }
            Ok(deja::DejaRecord::Observed(_)) => {}
            Err(_) => {
                if let Ok(event) = serde_json::from_str::<deja::BoundaryEvent>(trimmed) {
                    serde_json::to_writer(
                        &mut events_out,
                        &deja::DejaRecord::BoundaryEvent(Box::new(event)),
                    )
                    .map_err(io::Error::other)?;
                    events_out.write_all(b"\n")?;
                    split.boundary_events += 1;
                } else if serde_json::from_str::<deja_core::ExecutionGraphNode>(trimmed).is_ok() {
                    graph_out.write_all(trimmed.as_bytes())?;
                    graph_out.write_all(b"\n")?;
                    split.graph_nodes += 1;
                }
            }
        }
    }

    events_out.flush()?;
    graph_out.flush()?;
    drop(events_out);
    drop(graph_out);

    fs::rename(events_tmp, events_path)?;
    if split.graph_nodes > 0 {
        fs::rename(graph_tmp, graph_path)?;
    } else {
        remove_file_if_exists(&graph_tmp)?;
    }

    Ok(split)
}

fn has_record_kind(line: &str, kind: &str) -> bool {
    let Some(pos) = line.find("\"record_kind\"") else {
        return false;
    };
    let after_key = &line[pos + "\"record_kind\"".len()..];
    let after_colon = after_key.trim_start().strip_prefix(':');
    let Some(after_colon) = after_colon else {
        return false;
    };
    let after_colon = after_colon.trim_start();
    let expected = kind.as_bytes();
    let bytes = after_colon.as_bytes();
    bytes.first() == Some(&b'"')
        && bytes.get(1..1 + expected.len()) == Some(expected)
        && bytes.get(1 + expected.len()) == Some(&b'"')
}

/// Extract execution-graph nodes from a mixed DejaRecord JSONL stream into a
/// plain `ExecutionGraphNode` JSONL artifact.
///
/// Recording streams carry `BoundaryEvent` + `GraphNode`; replay observed
/// streams carry `ObservedCall` + `GraphNode`. Materializing the graph sidecar
/// makes trace traversal downloadable without changing the canonical streams.
pub fn materialize_graph_artifact(source: &Path, dest: &Path) -> io::Result<usize> {
    let file = match fs::File::open(source) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            remove_file_if_exists(dest)?;
            return Ok(0);
        }
        Err(e) => return Err(e),
    };

    let tmp = dest.with_extension("jsonl.tmp");
    if let Some(parent) = tmp.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut out = fs::File::create(&tmp)?;
    let mut count = 0usize;
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let node = match serde_json::from_str::<deja::DejaRecord>(&line) {
            Ok(deja::DejaRecord::GraphNode(node)) => Some(*node),
            Ok(deja::DejaRecord::BoundaryEvent(_) | deja::DejaRecord::Observed(_)) => None,
            Err(_) => serde_json::from_str::<deja_core::ExecutionGraphNode>(&line).ok(),
        };
        if let Some(node) = node {
            serde_json::to_writer(&mut out, &node).map_err(io::Error::other)?;
            out.write_all(b"\n")?;
            count += 1;
        }
    }
    out.flush()?;
    drop(out);

    if count == 0 {
        remove_file_if_exists(dest)?;
        remove_file_if_exists(&tmp)?;
        return Ok(0);
    }
    fs::rename(tmp, dest)?;
    Ok(count)
}

fn remove_file_if_exists(path: &Path) -> io::Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
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

    #[test]
    fn materialize_graph_artifact_extracts_graph_nodes_from_mixed_stream() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("events.jsonl");
        let dest = dir.path().join("graph.jsonl");
        let node = deja_core::ExecutionGraphNode {
            node_id: 7,
            global_sequence: 3,
            parent_id: Some(1),
            causal_parent_ids: vec![],
            sequence: 2,
            recording_run_id: Some("rec-1".to_owned()),
            span_name: "request".to_owned(),
            target: "router".to_owned(),
            level: "INFO".to_owned(),
            fields: [(
                "request_id".to_owned(),
                serde_json::Value::String("corr-1".to_owned()),
            )]
            .into_iter()
            .collect(),
            started_ns: 10,
            closed_ns: Some(20),
            extras: serde_json::Map::new(),
        };
        let graph_record = deja::DejaRecord::GraphNode(Box::new(node.clone()));
        std::fs::write(
            &source,
            format!(
                "{}\n{}\n",
                serde_json::json!({
                    "record_kind": "boundary_event",
                    "global_sequence": 1,
                    "request_sequence": 0,
                    "timestamp_ns": 1,
                    "boundary": "redis",
                    "trait_name": "RedisStore",
                    "method_name": "get",
                    "call_file": "x.rs",
                    "call_line": 1,
                    "call_column": 1,
                    "request": [],
                    "args": [],
                    "result": null,
                    "response": null,
                    "is_error": false,
                    "duration_us": 0,
                    "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
                    "provenance": "recorded",
                    "recon": "lossless",
                    "replay_strategy": "substitute"
                }),
                serde_json::to_string(&graph_record).unwrap()
            ),
        )
        .unwrap();

        assert_eq!(materialize_graph_artifact(&source, &dest).unwrap(), 1);
        let lines = std::fs::read_to_string(dest).unwrap();
        let parsed: deja_core::ExecutionGraphNode = serde_json::from_str(lines.trim()).unwrap();
        assert_eq!(parsed, node);
    }

    #[test]
    fn split_recording_artifacts_keeps_events_boundary_only() {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("events.jsonl");
        let graph = dir.path().join("graph.jsonl");
        let node = deja_core::ExecutionGraphNode {
            node_id: 9,
            global_sequence: 2,
            parent_id: None,
            causal_parent_ids: vec![],
            sequence: 1,
            recording_run_id: Some("rec-1".to_owned()),
            span_name: "request".to_owned(),
            target: "router".to_owned(),
            level: "INFO".to_owned(),
            fields: Default::default(),
            started_ns: 10,
            closed_ns: None,
            extras: serde_json::Map::new(),
        };
        let boundary = serde_json::json!({
            "record_kind": "boundary_event",
            "global_sequence": 1,
            "request_sequence": 0,
            "timestamp_ns": 1,
            "boundary": "redis",
            "trait_name": "RedisStore",
            "method_name": "get",
            "call_file": "x.rs",
            "call_line": 1,
            "call_column": 1,
            "request": [],
            "args": [],
            "result": null,
            "response": null,
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute"
        });
        std::fs::write(
            &source,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&deja::DejaRecord::GraphNode(Box::new(node.clone())))
                    .unwrap(),
                boundary
            ),
        )
        .unwrap();

        let split = split_recording_artifacts(&source, &graph).unwrap();
        assert_eq!(
            split,
            RecordingArtifactSplit {
                boundary_events: 1,
                graph_nodes: 1,
            }
        );
        let events = std::fs::read_to_string(&source).unwrap();
        assert_eq!(events.lines().count(), 1);
        assert!(events.contains("\"record_kind\":\"boundary_event\""));
        assert!(!events.contains("\"record_kind\":\"graph_node\""));
        let graph_lines = std::fs::read_to_string(&graph).unwrap();
        assert_eq!(graph_lines.lines().count(), 1);
        assert!(matches!(
            serde_json::from_str::<deja::DejaRecord>(graph_lines.trim()).unwrap(),
            deja::DejaRecord::GraphNode(parsed) if *parsed == node
        ));
    }
}
