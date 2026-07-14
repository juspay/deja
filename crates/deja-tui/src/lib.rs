use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use deja_core::ExecutionGraphNode;
use deja_runtime::{BoundaryEvent, DejaRecord};
use serde::Deserialize;
use serde_json::Value;

pub const SEMANTIC_FILE_NAME: &str = "semantic-events.jsonl";
pub const OBSERVED_DIR_NAME: &str = "observed";
pub const RUNS_DIR_NAME: &str = "runs";
pub const HTTP_DIFFS_DIR_NAME: &str = "http-diffs";

#[derive(Debug, Clone)]
pub struct ArtifactPaths {
    pub root: PathBuf,
    /// The recording tape — a tagged one-stream JSONL where boundary events
    /// and execution-graph nodes interleave (`record_kind` routes each line).
    pub semantic: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct JsonlStats {
    pub path: PathBuf,
    pub lines: usize,
    pub skipped: usize,
}

#[derive(Debug, Clone)]
pub struct LoadedArtifacts {
    pub paths: ArtifactPaths,
    pub semantic_events: Vec<BoundaryEvent>,
    pub graph_records: Vec<ExecutionGraphNode>,
    pub semantic_stats: Option<JsonlStats>,
    pub graph_stats: Option<JsonlStats>,
    pub replay: Option<ReplayArtifacts>,
}

/// One executed boundary call observed during a replay run
/// (`<root>/observed/<run>.jsonl`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ObservedCall {
    #[serde(default)]
    pub boundary: String,
    #[serde(default)]
    pub method_name: String,
    #[serde(default)]
    pub trait_name: String,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub resolved: bool,
    #[serde(default)]
    pub resolved_rank: Option<u32>,
    #[serde(default)]
    pub source_event_global_sequence: Option<u64>,
    #[serde(default)]
    pub args: Value,
    #[serde(default)]
    pub real_impl_will_fail: bool,
    #[serde(default)]
    pub synthesized: bool,
}

/// Pass/fail verdict for a replay run (`<root>/runs/<run>.scorecard.json`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Verdict {
    #[serde(default)]
    pub pass: bool,
    #[serde(default)]
    pub inconclusive: bool,
    #[serde(default)]
    pub reason: String,
}

/// Aggregate divergence counters from a replay scorecard.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ScoreSummary {
    #[serde(default)]
    pub matched_correlations: u64,
    #[serde(default)]
    pub total_correlations: u64,
    #[serde(default)]
    pub http_status_mismatches: u64,
    #[serde(default)]
    pub http_body_mismatches: u64,
    #[serde(default)]
    pub side_effect_divergences: u64,
    #[serde(default)]
    pub omitted_calls: u64,
    #[serde(default)]
    pub novel_calls: u64,
    #[serde(default)]
    pub matched_side_effect_calls: u64,
    #[serde(default)]
    pub inconclusive_races: u64,
    #[serde(default)]
    pub environmental_misses: u64,
    #[serde(default)]
    pub resolved_by_rank: BTreeMap<String, u64>,
    #[serde(default)]
    pub uncorrelated_events_seen: u64,
    #[serde(default)]
    pub uncorrelated_events_tolerated: bool,
}

/// Per-boundary substitution stats from a replay scorecard.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BoundaryStat {
    #[serde(default)]
    pub matched: u64,
    #[serde(default)]
    pub diverged: u64,
    #[serde(default)]
    pub tier: String,
    #[serde(default)]
    pub kinds: BTreeMap<String, u64>,
    #[serde(default)]
    pub resolved_by_rank: BTreeMap<String, u64>,
    #[serde(default)]
    pub note: Option<String>,
}

/// Per-request (correlation) outcome from a replay scorecard.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct CorrelationOutcome {
    #[serde(default)]
    pub correlation_id: String,
    #[serde(default)]
    pub http_status_match: bool,
    #[serde(default)]
    pub http_body_match: bool,
    #[serde(default)]
    pub side_effect_divergences: u64,
    #[serde(default)]
    pub passed: bool,
}

/// A replay run scorecard (`<root>/runs/<run>.scorecard.json`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Scorecard {
    #[serde(default)]
    pub run_id: String,
    #[serde(default)]
    pub recording_id: Option<String>,
    #[serde(default)]
    pub verdict: Verdict,
    #[serde(default)]
    pub summary: ScoreSummary,
    #[serde(default)]
    pub per_boundary: BTreeMap<String, BoundaryStat>,
    #[serde(default)]
    pub per_correlation: Vec<CorrelationOutcome>,
    #[serde(default)]
    pub warnings: Vec<String>,
}

/// One `json_path` divergence between the recorded and replayed response body.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct BodyDiffEntry {
    #[serde(default)]
    pub json_path: String,
    #[serde(default)]
    pub baseline: Value,
    #[serde(default)]
    pub candidate: Value,
}

/// Recorded-vs-replayed HTTP comparison for one driven request
/// (`<root>/http-diffs/<run>.jsonl`).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct HttpDiff {
    #[serde(default)]
    pub correlation_id: String,
    #[serde(default)]
    pub request_sequence: u64,
    #[serde(default)]
    pub request_path: String,
    #[serde(default)]
    pub status_baseline: u16,
    #[serde(default)]
    pub status_candidate: u16,
    #[serde(default)]
    pub status_match: bool,
    #[serde(default)]
    pub body_diff: Vec<BodyDiffEntry>,
}

/// Replay-side artifacts joined to a recording: the executed boundary calls and
/// (optionally) the scorecard verdict/summary.
#[derive(Debug, Clone, Default)]
pub struct ReplayArtifacts {
    pub observed: Vec<ObservedCall>,
    pub scorecard: Option<Scorecard>,
    pub http_diffs: Vec<HttpDiff>,
    pub observed_path: Option<PathBuf>,
    pub scorecard_path: Option<PathBuf>,
    pub http_diffs_path: Option<PathBuf>,
}

/// How a recorded `BoundaryEvent` was treated during replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Substitution {
    /// The recorded result was substituted from the lookup table during replay.
    Substituted { rank: Option<u32> },
    /// The recorded event was not replayed (omitted, or executed live).
    NotReplayed,
}

#[derive(Debug, Clone)]
pub struct Summary {
    pub boundary_counts: Vec<(String, usize)>,
    pub top_operations: Vec<(String, usize)>,
    pub span_counts: Vec<(String, usize)>,
    pub request_counts: Vec<(String, usize, usize)>,
    pub semantic_errors: usize,
    pub graph_errors: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestKey {
    pub id: String,
    pub semantic_count: usize,
    pub graph_count: usize,
}

pub fn discover_artifacts(input: impl AsRef<Path>) -> ArtifactPaths {
    let input = input.as_ref();

    if input.is_file() {
        let file_name = input.file_name().and_then(|name| name.to_str());
        let parent = input.parent().unwrap_or_else(|| Path::new("."));
        let root = artifact_root_for(parent);
        return ArtifactPaths {
            semantic: (file_name == Some(SEMANTIC_FILE_NAME))
                .then(|| input.to_path_buf())
                .or_else(|| find_semantic_artifact(&root)),
            root,
        };
    }

    let input_root = input.to_path_buf();
    let artifact_root = artifact_root_for(input);

    ArtifactPaths {
        semantic: find_semantic_artifact(&input_root)
            .or_else(|| find_semantic_artifact(&artifact_root)),
        root: artifact_root,
    }
}

fn artifact_root_for(path: &Path) -> PathBuf {
    match path.file_name().and_then(|name| name.to_str()) {
        Some("semantic" | "recording") => path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| path.to_path_buf()),
        _ => path.to_path_buf(),
    }
}

fn find_semantic_artifact(root: &Path) -> Option<PathBuf> {
    existing_file(&root.join(SEMANTIC_FILE_NAME))
        .or_else(|| existing_file(&root.join("semantic").join(SEMANTIC_FILE_NAME)))
        .or_else(|| existing_file(&root.join("recording").join(SEMANTIC_FILE_NAME)))
        // Orchestrator layout: the S3-pulled session at
        // recordings/{id}/events.jsonl (newest recording wins).
        .or_else(|| newest_pulled_recording(root))
}

/// Newest `recordings/{id}/events.jsonl` under a HarnessRoot — the canonical
/// slot the orchestrator materializes pulled sessions into.
fn newest_pulled_recording(root: &Path) -> Option<PathBuf> {
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in std::fs::read_dir(root.join("recordings")).ok()?.flatten() {
        let candidate = entry.path().join("events.jsonl");
        let Ok(meta) = std::fs::metadata(&candidate) else {
            continue;
        };
        let modified = meta.modified().ok()?;
        if best.as_ref().is_none_or(|(t, _)| modified > *t) {
            best = Some((modified, candidate));
        }
    }
    best.map(|(_, path)| path)
}

/// When `base` itself holds no artifacts but contains run directories that do
/// (e.g. `demo/harness-state/<epoch>/`), return the newest such run dir.
pub fn discover_latest_run(base: impl AsRef<Path>) -> Option<PathBuf> {
    let base = base.as_ref();
    if discover_artifacts(base).semantic.is_some() {
        return None;
    }
    let mut best: Option<(std::time::SystemTime, PathBuf)> = None;
    for entry in std::fs::read_dir(base).ok()?.flatten() {
        let path = entry.path();
        if !path.is_dir() {
            continue;
        }
        if discover_artifacts(&path).semantic.is_none() {
            continue;
        }
        let modified = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .unwrap_or(std::time::UNIX_EPOCH);
        if best
            .as_ref()
            .map(|(time, _)| modified > *time)
            .unwrap_or(true)
        {
            best = Some((modified, path));
        }
    }
    best.map(|(_, path)| path)
}

pub fn load_artifacts(input: impl AsRef<Path>) -> Result<LoadedArtifacts> {
    let input = input.as_ref();
    let input = discover_latest_run(input).unwrap_or_else(|| input.to_path_buf());
    let paths = discover_artifacts(input);
    let (semantic_events, graph_records, semantic_stats, graph_stats) = match &paths.semantic {
        Some(path) => {
            let loaded = read_tape(path)
                .with_context(|| format!("reading recording tape from {}", path.display()))?;
            (
                loaded.events,
                loaded.graph_records,
                Some(loaded.semantic_stats),
                Some(loaded.graph_stats),
            )
        }
        None => (Vec::new(), Vec::new(), None, None),
    };

    let replay = load_replay(&paths.root);

    Ok(LoadedArtifacts {
        paths,
        semantic_events,
        graph_records,
        semantic_stats,
        graph_stats,
        replay,
    })
}

/// Discover and load the newest replay artifacts under `root`: the latest
/// `observed/*.jsonl` and the latest `runs/*.scorecard.json`. Returns `None`
/// when neither file exists.
pub fn load_replay(root: &Path) -> Option<ReplayArtifacts> {
    let root = artifact_root_for(root);
    let observed_path = find_observed_artifact(&root);
    let scorecard_path = find_scorecard_artifact(&root);
    let http_diffs_path = find_http_diffs_artifact(&root);

    if observed_path.is_none() && scorecard_path.is_none() && http_diffs_path.is_none() {
        return None;
    }

    let observed = observed_path
        .as_deref()
        .map(|path| read_jsonl(path, parse_observed_call).map(|loaded| loaded.records))
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_default();

    let scorecard = scorecard_path
        .as_deref()
        .and_then(|path| std::fs::read_to_string(path).ok())
        .and_then(|raw| serde_json::from_str::<Scorecard>(&raw).ok());

    let http_diffs = http_diffs_path
        .as_deref()
        .map(|path| read_jsonl(path, parse_http_diff).map(|loaded| loaded.records))
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_default();

    Some(ReplayArtifacts {
        observed,
        scorecard,
        http_diffs,
        observed_path,
        scorecard_path,
        http_diffs_path,
    })
}

fn find_observed_artifact(root: &Path) -> Option<PathBuf> {
    newest_matching(&root.join(OBSERVED_DIR_NAME), |name| {
        name.ends_with(".jsonl")
    })
}

fn find_scorecard_artifact(root: &Path) -> Option<PathBuf> {
    newest_matching(&root.join(RUNS_DIR_NAME), |name| {
        name.ends_with(".scorecard.json")
    })
}

fn find_http_diffs_artifact(root: &Path) -> Option<PathBuf> {
    newest_matching(&root.join(HTTP_DIFFS_DIR_NAME), |name| {
        name.ends_with(".jsonl")
    })
}

/// Return the most recently modified file in `dir` whose file name matches
/// `predicate`. Falls back to lexicographic ordering when mtimes are unavailable.
fn newest_matching(dir: &Path, predicate: impl Fn(&str) -> bool) -> Option<PathBuf> {
    let mut best: Option<(std::time::SystemTime, String, PathBuf)> = None;
    for entry in std::fs::read_dir(dir).ok()?.flatten() {
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = match path.file_name().and_then(|name| name.to_str()) {
            Some(name) if predicate(name) => name.to_owned(),
            _ => continue,
        };
        let modified = entry
            .metadata()
            .and_then(|meta| meta.modified())
            .unwrap_or(std::time::UNIX_EPOCH);
        let better = match &best {
            None => true,
            Some((best_time, best_name, _)) => (modified, &name) > (*best_time, best_name),
        };
        if better {
            best = Some((modified, name, path));
        }
    }
    best.map(|(_, _, path)| path)
}

fn parse_observed_call(line: &str) -> Option<ObservedCall> {
    serde_json::from_str(line).ok()
}

fn parse_http_diff(line: &str) -> Option<HttpDiff> {
    serde_json::from_str(line).ok()
}

/// Observed boundary calls that did NOT resolve against the recording —
/// replay executed something the recording never saw ("novel" calls).
pub fn novel_observed_calls(replay: &ReplayArtifacts) -> Vec<&ObservedCall> {
    replay
        .observed
        .iter()
        .filter(|call| !call.resolved)
        .collect()
}

/// The scorecard's `resolved_by_rank` histogram as sorted `(rank, count)`
/// pairs, e.g. `[("rank_2", 195), ("rank_4", 1)]`.
pub fn rank_histogram(scorecard: &Scorecard) -> Vec<(String, u64)> {
    scorecard
        .summary
        .resolved_by_rank
        .iter()
        .map(|(rank, count)| (rank.clone(), *count))
        .collect()
}

/// Everything the TUI knows about one driven request, joined across the
/// recording (`http_incoming` event), the scorecard (`per_correlation`) and
/// the kernel's HTTP comparison (`http-diffs`).
#[derive(Debug, Clone, Default)]
pub struct RequestOutcome {
    pub correlation_id: String,
    pub method: String,
    pub path: String,
    pub event_count: usize,
    pub outcome: Option<CorrelationOutcome>,
    pub http_diff: Option<HttpDiff>,
}

/// Join recorded requests to their replay outcomes, in recorded order.
/// Requests missing from the scorecard/diffs (e.g. health probes) still
/// appear, with `outcome`/`http_diff` empty.
pub fn request_outcomes(artifacts: &LoadedArtifacts) -> Vec<RequestOutcome> {
    let replay = artifacts.replay.as_ref();
    let outcome_by_corr: HashMap<&str, &CorrelationOutcome> = replay
        .and_then(|replay| replay.scorecard.as_ref())
        .map(|card| {
            card.per_correlation
                .iter()
                .map(|outcome| (outcome.correlation_id.as_str(), outcome))
                .collect()
        })
        .unwrap_or_default();
    let diff_by_corr: HashMap<&str, &HttpDiff> = replay
        .map(|replay| {
            replay
                .http_diffs
                .iter()
                .map(|diff| (diff.correlation_id.as_str(), diff))
                .collect()
        })
        .unwrap_or_default();

    let mut event_counts: HashMap<&str, usize> = HashMap::new();
    for event in &artifacts.semantic_events {
        if let Some(id) = event.correlation_id.as_deref() {
            *event_counts.entry(id).or_default() += 1;
        }
    }

    // Method/path live on the correlation's http_incoming event — which is
    // emitted at request COMPLETION, so it is usually the correlation's LAST
    // event, not its first.
    let mut ingress: HashMap<&str, (&str, &str)> = HashMap::new();
    for event in &artifacts.semantic_events {
        if event.boundary != "http_incoming" {
            continue;
        }
        let Some(id) = event.correlation_id.as_deref() else {
            continue;
        };
        let method = event.request.get("method").and_then(Value::as_str);
        let path = event.request.get("path").and_then(Value::as_str);
        if let (Some(method), Some(path)) = (method, path) {
            ingress.entry(id).or_insert((method, path));
        }
    }

    let mut seen = std::collections::HashSet::new();
    let mut outcomes = Vec::new();
    for event in &artifacts.semantic_events {
        let Some(id) = event.correlation_id.as_deref() else {
            continue;
        };
        if !seen.insert(id.to_owned()) {
            continue;
        }
        let (method, path) = ingress
            .get(id)
            .map(|(method, path)| ((*method).to_owned(), (*path).to_owned()))
            .unwrap_or_else(|| {
                // No ingress event — fall back to the kernel's record of what
                // it drove.
                let path = diff_by_corr
                    .get(id)
                    .map(|diff| diff.request_path.clone())
                    .unwrap_or_default();
                (String::new(), path)
            });
        outcomes.push(RequestOutcome {
            correlation_id: id.to_owned(),
            method,
            path,
            event_count: event_counts.get(id).copied().unwrap_or(0),
            outcome: outcome_by_corr.get(id).map(|outcome| (*outcome).clone()),
            http_diff: diff_by_corr.get(id).map(|diff| (*diff).clone()),
        });
    }
    outcomes
}

/// Join recorded events against the replay's observed calls: every recorded
/// event whose `global_sequence` was resolved (substituted) from the lookup
/// table is [`Substitution::Substituted`]; all others are
/// [`Substitution::NotReplayed`].
pub fn substitution_status(
    events: &[BoundaryEvent],
    replay: &ReplayArtifacts,
) -> HashMap<u64, Substitution> {
    let mut resolved_rank: HashMap<u64, Option<u32>> = HashMap::new();
    for call in &replay.observed {
        if !call.resolved {
            continue;
        }
        if let Some(seq) = call.source_event_global_sequence {
            // Keep the first (lowest-rank) substitution we see for a sequence.
            resolved_rank.entry(seq).or_insert(call.resolved_rank);
        }
    }

    events
        .iter()
        .map(|event| {
            let status = match resolved_rank.get(&event.global_sequence) {
                Some(rank) => Substitution::Substituted { rank: *rank },
                None => Substitution::NotReplayed,
            };
            (event.global_sequence, status)
        })
        .collect()
}

/// Per-boundary `(substituted, total)` substitution counts. Prefers the
/// scorecard's `per_boundary.matched`/`diverged` when present, otherwise
/// derives counts from the recorded events joined to the observed calls.
pub fn boundary_substitution_counts(
    events: &[BoundaryEvent],
    replay: &ReplayArtifacts,
) -> Vec<(String, usize, usize)> {
    if let Some(scorecard) = &replay.scorecard {
        if !scorecard.per_boundary.is_empty() {
            let mut counts = scorecard
                .per_boundary
                .iter()
                .map(|(boundary, stat)| {
                    let matched = stat.matched as usize;
                    let total = (stat.matched + stat.diverged) as usize;
                    (boundary.clone(), matched, total)
                })
                .collect::<Vec<_>>();
            counts.sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0)));
            return counts;
        }
    }

    let status = substitution_status(events, replay);
    let mut by_boundary: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    for event in events {
        let entry = by_boundary.entry(event.boundary.clone()).or_default();
        entry.1 += 1;
        if matches!(
            status.get(&event.global_sequence),
            Some(Substitution::Substituted { .. })
        ) {
            entry.0 += 1;
        }
    }
    let mut counts = by_boundary
        .into_iter()
        .map(|(boundary, (substituted, total))| (boundary, substituted, total))
        .collect::<Vec<_>>();
    counts.sort_by(|left, right| right.2.cmp(&left.2).then_with(|| left.0.cmp(&right.0)));
    counts
}

pub fn summarize(artifacts: &LoadedArtifacts) -> Summary {
    let mut boundary_counts = BTreeMap::new();
    let mut operation_counts = BTreeMap::new();
    let mut span_counts = BTreeMap::new();
    let mut requests: BTreeMap<String, (usize, usize)> = BTreeMap::new();
    let mut semantic_errors = 0;
    let mut graph_errors = 0;

    for event in &artifacts.semantic_events {
        *boundary_counts.entry(event.boundary.clone()).or_insert(0) += 1;
        let operation = format!(
            "{} {}::{}",
            event.boundary, event.trait_name, event.method_name
        );
        *operation_counts.entry(operation).or_insert(0) += 1;
        if let Some(id) = &event.correlation_id {
            requests.entry(id.clone()).or_default().0 += 1;
        }
        if event.is_error {
            semantic_errors += 1;
        }
    }

    let graph_counts = graph_request_counts(&artifacts.graph_records);
    for node in &artifacts.graph_records {
        *span_counts.entry(node.span_name.clone()).or_insert(0) += 1;
        if graph_record_has_error(node) {
            graph_errors += 1;
        }
    }
    for (request_id, graph_count) in graph_counts {
        requests.entry(request_id).or_default().1 = graph_count;
    }

    Summary {
        boundary_counts: sorted_counts(boundary_counts),
        top_operations: sorted_counts(operation_counts),
        span_counts: sorted_counts(span_counts),
        request_counts: requests
            .into_iter()
            .map(|(id, (semantic_count, graph_count))| (id, semantic_count, graph_count))
            .collect(),
        semantic_errors,
        graph_errors,
    }
}

pub fn request_keys(artifacts: &LoadedArtifacts) -> Vec<RequestKey> {
    summarize(artifacts)
        .request_counts
        .into_iter()
        .map(|(id, semantic_count, graph_count)| RequestKey {
            id,
            semantic_count,
            graph_count,
        })
        .collect()
}

pub fn semantic_event_text(event: &BoundaryEvent) -> String {
    format!(
        "{} {} {} {} {} {} {} {} {}",
        event.global_sequence,
        event.correlation_id.as_deref().unwrap_or(""),
        event.boundary,
        event.trait_name,
        event.method_name,
        event.call_file,
        event
            .graph_node_id
            .map(|id| id.to_string())
            .unwrap_or_default(),
        event.args,
        event.result
    )
}

pub fn graph_record_text(record: &ExecutionGraphNode) -> String {
    format!(
        "{} {} {} {} {} {}",
        record.sequence,
        record.span_name,
        record.target,
        record.level,
        record.node_id,
        Value::Object(
            record
                .fields
                .clone()
                .into_iter()
                .collect::<serde_json::Map<String, Value>>()
        )
    )
}

pub fn semantic_event_request_id(event: &BoundaryEvent) -> Option<&str> {
    event.correlation_id.as_deref()
}

pub fn graph_request_id(record: &ExecutionGraphNode) -> Option<&str> {
    record
        .request_id()
        .or_else(|| record.fields.get("correlation_id").and_then(Value::as_str))
}

pub fn graph_records_for_request<'a>(
    records: &'a [ExecutionGraphNode],
    request_id: &str,
) -> Vec<&'a ExecutionGraphNode> {
    let mut children: HashMap<Option<u64>, Vec<&ExecutionGraphNode>> = HashMap::new();
    for record in records {
        children.entry(record.parent_id).or_default().push(record);
    }

    let mut selected = Vec::new();
    let mut visited = BTreeSet::new();
    for record in records
        .iter()
        .filter(|record| graph_request_id(record) == Some(request_id))
    {
        collect_graph_subtree(record, &children, request_id, &mut visited, &mut selected);
    }
    selected.sort_by_key(|record| record.sequence);
    selected
}

pub fn graph_request_counts(records: &[ExecutionGraphNode]) -> BTreeMap<String, usize> {
    let request_ids = records
        .iter()
        .filter_map(graph_request_id)
        .map(str::to_owned)
        .collect::<BTreeSet<_>>();

    request_ids
        .into_iter()
        .map(|request_id| {
            let count = graph_records_for_request(records, &request_id).len();
            (request_id, count)
        })
        .collect()
}

pub fn graph_record_has_error(record: &ExecutionGraphNode) -> bool {
    if record.level.eq_ignore_ascii_case("error") {
        return true;
    }

    record.fields.iter().any(|(key, value)| {
        key.to_ascii_lowercase().contains("error")
            || value
                .as_str()
                .map(|value| value.to_ascii_lowercase().contains("error"))
                .unwrap_or(false)
    })
}

fn collect_graph_subtree<'a>(
    record: &'a ExecutionGraphNode,
    children: &HashMap<Option<u64>, Vec<&'a ExecutionGraphNode>>,
    request_id: &str,
    visited: &mut BTreeSet<u64>,
    selected: &mut Vec<&'a ExecutionGraphNode>,
) {
    if !visited.insert(record.node_id) {
        return;
    }
    selected.push(record);

    if let Some(child_records) = children.get(&Some(record.node_id)) {
        for child in child_records {
            if graph_request_id(child).is_none() || graph_request_id(child) == Some(request_id) {
                collect_graph_subtree(child, children, request_id, visited, selected);
            }
        }
    }
}

pub fn unique_boundaries(events: &[BoundaryEvent]) -> Vec<String> {
    events
        .iter()
        .map(|event| event.boundary.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

struct TapeLoad {
    events: Vec<BoundaryEvent>,
    graph_records: Vec<ExecutionGraphNode>,
    semantic_stats: JsonlStats,
    graph_stats: JsonlStats,
}

fn read_tape(path: &Path) -> Result<TapeLoad> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    let mut graph_records = Vec::new();
    let mut lines = 0;
    let mut skipped = 0;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        lines += 1;

        match serde_json::from_str::<DejaRecord>(&line) {
            Ok(DejaRecord::BoundaryEvent(event)) => events.push(*event),
            Ok(DejaRecord::GraphNode(node)) => graph_records.push(node),
            Ok(DejaRecord::Observed(_)) | Err(_) => {
                if let Some(event) = parse_semantic_event(&line) {
                    events.push(event);
                } else if let Some(node) = parse_graph_record(&line) {
                    graph_records.push(node);
                } else {
                    skipped += 1;
                }
            }
        }
    }

    let stats = JsonlStats {
        path: path.to_path_buf(),
        lines,
        skipped,
    };

    Ok(TapeLoad {
        events,
        graph_records,
        semantic_stats: stats.clone(),
        graph_stats: stats,
    })
}

struct JsonlLoad<T> {
    records: Vec<T>,
}

fn existing_file(path: &Path) -> Option<PathBuf> {
    path.is_file().then(|| path.to_path_buf())
}

fn read_jsonl<T>(path: &Path, parse: impl Fn(&str) -> Option<T>) -> Result<JsonlLoad<T>> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        if let Some(record) = parse(&line) {
            records.push(record);
        }
    }

    Ok(JsonlLoad { records })
}

fn parse_semantic_event(line: &str) -> Option<BoundaryEvent> {
    let value = serde_json::from_str::<Value>(line).ok()?;
    serde_json::from_value::<DejaRecord>(value.clone())
        .ok()
        .and_then(|record| match record {
            DejaRecord::BoundaryEvent(event) => Some(*event),
            _ => None,
        })
        .or_else(|| serde_json::from_value::<BoundaryEvent>(value).ok())
}

fn parse_graph_record(line: &str) -> Option<ExecutionGraphNode> {
    let value = serde_json::from_str::<Value>(line).ok()?;
    serde_json::from_value::<DejaRecord>(value.clone())
        .ok()
        .and_then(|record| match record {
            DejaRecord::GraphNode(node) => Some(node),
            _ => None,
        })
        .or_else(|| serde_json::from_value::<ExecutionGraphNode>(value.clone()).ok())
        .or_else(|| {
            value
                .get("node")
                .cloned()
                .and_then(|node| serde_json::from_value::<ExecutionGraphNode>(node).ok())
        })
}

fn sorted_counts(counts: BTreeMap<String, usize>) -> Vec<(String, usize)> {
    let mut counts = counts.into_iter().collect::<Vec<_>>();
    counts.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    counts
}

// ---------------------------------------------------------------------------
// Record↔replay split diff (the Divergences tab)
//
// Turns a request's recorded side-effect timeline + the candidate's observed
// calls + the HTTP diff into an ordered list of aligned `DiffRow`s, git-diff
// style: Matched (context), Omitted (recorded, never replayed), Novel (replayed,
// never recorded), and Changed (an omitted+novel of the same logical call fused,
// e.g. the same db INSERT with one column value changed).
// ---------------------------------------------------------------------------

/// A real side-effect seam (db/redis/http/grpc...) — excludes the request
/// boundary and the deterministic/tolerated seams (time/id/crypto) so the diff
/// shows the calls that actually matter.
pub fn is_side_effect_boundary(boundary: &str) -> bool {
    !matches!(
        boundary,
        "http_incoming" | "time" | "id" | "id_generation" | "uuid" | "rng" | "crypto" | "function"
    )
}

/// One field-level difference between two JSON values (also the shape of an
/// HTTP `body_diff` entry).
#[derive(Debug, Clone)]
pub struct FieldDiff {
    pub json_path: String,
    pub baseline: Value, // recorded side; `Null` = key absent on baseline (added)
    pub candidate: Value, // replayed side; `Null` = key absent on candidate (removed)
}

/// What a single logical row of the split diff represents.
#[derive(Debug, Clone)]
pub enum DiffKind {
    Matched,
    Omitted,
    Novel,
    Changed { field_diffs: Vec<FieldDiff> },
    HttpStatus { baseline: u16, candidate: u16 },
    HttpBody { field_diffs: Vec<FieldDiff> },
}

/// One side (recorded or candidate) of a diff row, pre-cloned so rendering is
/// borrow-free.
#[derive(Debug, Clone)]
pub struct SideCell {
    pub args: Value,
    pub result: Option<Value>,
}

/// One logical row of the record↔replay split diff.
#[derive(Debug, Clone)]
pub struct DiffRow {
    pub kind: DiffKind,
    pub label: String,
    pub left: Option<SideCell>,  // recorded side (None => right-only row)
    pub right: Option<SideCell>, // candidate side (None => left-only row)
    pub gseq: Option<u64>,
}

impl DiffRow {
    /// True for any row that represents an actual divergence (not Matched).
    pub fn is_divergence(&self) -> bool {
        !matches!(self.kind, DiffKind::Matched)
    }
}

/// The per-boundary discriminator that distinguishes one logical call from
/// another of the same boundary+method: db→table, redis→key, http→url/path.
/// Returns `(display label, pairing key)`.
fn logical_key(boundary: &str, method: &str, args: &Value) -> (String, String) {
    let disc = match boundary {
        "db" => args.get("table").and_then(Value::as_str).unwrap_or(""),
        "redis" => args.get("key").and_then(Value::as_str).unwrap_or(""),
        "http_outgoing" | "http_client" | "grpc" => args
            .get("url")
            .or_else(|| args.get("path"))
            .and_then(Value::as_str)
            .unwrap_or(""),
        _ => "",
    };
    let label = if disc.is_empty() {
        format!("{boundary}::{method}")
    } else {
        format!("{boundary}::{method}  {disc}")
    };
    // `\u{1}` is a separator that can't appear in the components.
    let key = format!("{boundary}\u{1}{method}\u{1}{disc}");
    (label, key)
}

/// Recursive structural diff of two JSON values. Equal subtrees emit nothing;
/// a key present on only one side becomes a `Null` on the absent side. Reused
/// for both argument diffs and HTTP body diffs.
pub fn json_field_diff(baseline: &Value, candidate: &Value) -> Vec<FieldDiff> {
    let mut out = Vec::new();
    diff_walk("", baseline, candidate, &mut out);
    out
}

fn diff_walk(path: &str, b: &Value, c: &Value, out: &mut Vec<FieldDiff>) {
    if b == c {
        return;
    }
    match (b, c) {
        (Value::Object(bo), Value::Object(co)) => {
            let mut keys: Vec<&String> = bo.keys().collect();
            for k in co.keys() {
                if !bo.contains_key(k) {
                    keys.push(k);
                }
            }
            for k in keys {
                let child = if path.is_empty() {
                    k.clone()
                } else {
                    format!("{path}.{k}")
                };
                diff_walk(
                    &child,
                    bo.get(k).unwrap_or(&Value::Null),
                    co.get(k).unwrap_or(&Value::Null),
                    out,
                );
            }
        }
        (Value::Array(ba), Value::Array(ca)) => {
            for i in 0..ba.len().max(ca.len()) {
                let child = format!("{path}[{i}]");
                diff_walk(
                    &child,
                    ba.get(i).unwrap_or(&Value::Null),
                    ca.get(i).unwrap_or(&Value::Null),
                    out,
                );
            }
        }
        _ => out.push(FieldDiff {
            json_path: path.to_string(),
            baseline: b.clone(),
            candidate: c.clone(),
        }),
    }
}

/// Build the ordered, aligned record↔replay diff rows for one correlation.
///
/// Emits, in recorded `global_sequence` order, exactly one row per recorded
/// side-effect (Matched / Changed / Omitted), then any leftover Novel candidate
/// calls, then the HTTP status/body rows (when divergent).
pub fn build_diff_rows(
    events: &[BoundaryEvent],
    replay: Option<&ReplayArtifacts>,
    http: Option<&HttpDiff>,
    corr: &str,
) -> Vec<DiffRow> {
    let rec: Vec<&BoundaryEvent> = events
        .iter()
        .filter(|e| e.correlation_id.as_deref() == Some(corr))
        .filter(|e| is_side_effect_boundary(&e.boundary))
        .collect();
    let obs: &[ObservedCall] = replay.map(|r| r.observed.as_slice()).unwrap_or(&[]);

    // Recorded events the candidate actually replayed (Matched), by sequence.
    let consumed: HashSet<u64> = obs
        .iter()
        .filter(|c| c.resolved)
        .filter_map(|c| c.source_event_global_sequence)
        .collect();

    // Novel = observed calls for THIS correlation, side-effect, unresolved.
    // (Novels can carry a different correlation_id — filter strictly.)
    let novel: Vec<&ObservedCall> = obs
        .iter()
        .filter(|c| c.correlation_id.as_deref() == Some(corr))
        .filter(|c| !c.resolved)
        .filter(|c| is_side_effect_boundary(&c.boundary))
        .collect();

    // Pairing pass: fuse an omitted recorded event with a novel candidate call
    // sharing a logical key (FIFO, in recorded order) into a Changed row.
    let mut novel_by_key: BTreeMap<String, VecDeque<usize>> = BTreeMap::new();
    for (ni, c) in novel.iter().enumerate() {
        let (_, key) = logical_key(&c.boundary, &c.method_name, &c.args);
        novel_by_key.entry(key).or_default().push_back(ni);
    }
    let mut paired_novel: HashSet<usize> = HashSet::new();
    // gseq -> (paired novel index, field diffs)
    let mut changed_for_gseq: HashMap<u64, (usize, Vec<FieldDiff>)> = HashMap::new();
    for e in rec
        .iter()
        .filter(|e| !consumed.contains(&e.global_sequence))
    {
        let (_, key) = logical_key(&e.boundary, &e.method_name, &e.args);
        if let Some(ni) = novel_by_key.get_mut(&key).and_then(|q| q.pop_front()) {
            let fds = json_field_diff(&e.args, &novel[ni].args);
            changed_for_gseq.insert(e.global_sequence, (ni, fds));
            paired_novel.insert(ni);
        }
    }

    // Emit one row per recorded side-effect, in recorded order.
    let mut rows = Vec::new();
    for e in &rec {
        let (label, _) = logical_key(&e.boundary, &e.method_name, &e.args);
        let left = Some(SideCell {
            args: e.args.clone(),
            result: Some(e.result.clone()),
        });
        if consumed.contains(&e.global_sequence) {
            rows.push(DiffRow {
                kind: DiffKind::Matched,
                label,
                left: left.clone(),
                // Right echoes the substituted (recorded) value the candidate received.
                right: Some(SideCell {
                    args: e.args.clone(),
                    result: None,
                }),
                gseq: Some(e.global_sequence),
            });
        } else if let Some((ni, fds)) = changed_for_gseq.remove(&e.global_sequence) {
            rows.push(DiffRow {
                kind: DiffKind::Changed { field_diffs: fds },
                label,
                left,
                right: Some(SideCell {
                    args: novel[ni].args.clone(),
                    result: None,
                }),
                gseq: Some(e.global_sequence),
            });
        } else {
            rows.push(DiffRow {
                kind: DiffKind::Omitted,
                label,
                left,
                right: None,
                gseq: Some(e.global_sequence),
            });
        }
    }

    // Leftover novel calls (never paired) → pure Novel rows.
    for (ni, c) in novel.iter().enumerate() {
        if !paired_novel.contains(&ni) {
            let (label, _) = logical_key(&c.boundary, &c.method_name, &c.args);
            rows.push(DiffRow {
                kind: DiffKind::Novel,
                label,
                left: None,
                right: Some(SideCell {
                    args: c.args.clone(),
                    result: None,
                }),
                gseq: None,
            });
        }
    }

    // HTTP rows last — the externally-observable result.
    if let Some(d) = http {
        if !d.status_match {
            rows.push(DiffRow {
                kind: DiffKind::HttpStatus {
                    baseline: d.status_baseline,
                    candidate: d.status_candidate,
                },
                label: format!("HTTP status {} → {}", d.status_baseline, d.status_candidate),
                left: None,
                right: None,
                gseq: None,
            });
        }
        if !d.body_diff.is_empty() {
            let fds = d
                .body_diff
                .iter()
                .map(|b| FieldDiff {
                    json_path: b.json_path.clone(),
                    baseline: b.baseline.clone(),
                    candidate: b.candidate.clone(),
                })
                .collect();
            rows.push(DiffRow {
                kind: DiffKind::HttpBody { field_diffs: fds },
                label: format!("HTTP body ({} fields)", d.body_diff.len()),
                left: None,
                right: None,
                gseq: None,
            });
        }
    }

    rows
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;

    fn ev(
        gseq: u64,
        corr: &str,
        boundary: &str,
        method: &str,
        args: Value,
        result: Value,
    ) -> BoundaryEvent {
        serde_json::from_value(serde_json::json!({
            "global_sequence": gseq, "request_sequence": gseq, "correlation_id": corr,
            "timestamp_ns": 0, "boundary": boundary, "trait_name": "T", "method_name": method,
            "call_file": "x.rs", "call_line": 1, "call_column": 1,
            "request": {}, "args": args, "response": {}, "result": result,
            "is_error": false, "duration_us": 0,
            "event_schema_version": 7, "provenance": "recorded",
            "recon": "lossless", "replay_strategy": "substitute"
        }))
        .unwrap()
    }
    fn obs(
        corr: &str,
        boundary: &str,
        method: &str,
        resolved: bool,
        src: Option<u64>,
        args: Value,
    ) -> ObservedCall {
        serde_json::from_value(serde_json::json!({
            "boundary": boundary, "method_name": method, "trait_name": "T",
            "correlation_id": corr, "resolved": resolved, "resolved_rank": null,
            "source_event_global_sequence": src, "args": args,
            "real_impl_will_fail": false, "synthesized": false
        }))
        .unwrap()
    }

    #[test]
    fn build_diff_rows_pairs_changed_keeps_omitted_and_appends_http() {
        use serde_json::json;
        let corr = "c1";
        // recording: a matched find, a payment_attempt insert (will become Changed),
        // and a redis set (pure Omitted).
        let events = vec![
            ev(
                0,
                corr,
                "db",
                "find",
                json!({"table":"merchant","sql":"S"}),
                json!({"Ok":1}),
            ),
            ev(
                1,
                corr,
                "db",
                "generic_insert",
                json!({"table":"payment_attempt","inputs":{"values":{"updated_by":""}}}),
                json!({"Ok":1}),
            ),
            ev(
                2,
                corr,
                "redis",
                "set_key",
                json!({"key":"k1","command":"SET"}),
                json!({"ok":true}),
            ),
        ];
        // observed: one resolved (matches event 0), one novel insert with the changed
        // value (pairs with event 1), and a novel carrying a DIFFERENT corr (ignored).
        let observed = vec![
            obs(
                corr,
                "db",
                "find",
                true,
                Some(0),
                json!({"table":"merchant","sql":"S"}),
            ),
            obs(
                corr,
                "db",
                "generic_insert",
                false,
                None,
                json!({"table":"payment_attempt","inputs":{"values":{"updated_by":"v2-candidate"}}}),
            ),
            obs(
                "other",
                "db",
                "generic_insert",
                false,
                None,
                json!({"table":"payment_attempt"}),
            ),
        ];
        let replay = ReplayArtifacts {
            observed,
            scorecard: None,
            http_diffs: Vec::new(),
            observed_path: None,
            scorecard_path: None,
            http_diffs_path: None,
        };
        let http: HttpDiff = serde_json::from_value(json!({
            "correlation_id": corr, "request_path": "/payments", "request_sequence": 0,
            "status_baseline": 200, "status_candidate": 400, "status_match": false,
            "body_diff": [{"json_path":"$.status","baseline":"ok","candidate":null}]
        }))
        .unwrap();

        let rows = build_diff_rows(&events, Some(&replay), Some(&http), corr);
        let kinds: Vec<&str> = rows
            .iter()
            .map(|r| match r.kind {
                DiffKind::Matched => "matched",
                DiffKind::Omitted => "omitted",
                DiffKind::Novel => "novel",
                DiffKind::Changed { .. } => "changed",
                DiffKind::HttpStatus { .. } => "status",
                DiffKind::HttpBody { .. } => "body",
            })
            .collect();
        // recorded order: matched, changed (insert), omitted (redis), then http rows.
        // The "other"-corr novel must NOT appear.
        assert_eq!(
            kinds,
            vec!["matched", "changed", "omitted", "status", "body"]
        );

        let changed = rows
            .iter()
            .find(|r| matches!(r.kind, DiffKind::Changed { .. }))
            .unwrap();
        assert!(changed.label.contains("payment_attempt"));
        assert!(
            changed.right.is_some(),
            "changed row carries the candidate side"
        );
        if let DiffKind::Changed { field_diffs } = &changed.kind {
            assert!(
                field_diffs
                    .iter()
                    .any(|f| f.json_path.contains("updated_by")),
                "the field diff pinpoints updated_by: {field_diffs:?}"
            );
        }
    }

    #[test]
    fn json_field_diff_finds_only_the_changed_leaf() {
        use serde_json::json;
        let a = json!({"a":1,"b":{"x":"keep","y":""}});
        let b = json!({"a":1,"b":{"x":"keep","y":"changed"}});
        let diffs = json_field_diff(&a, &b);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].json_path, "b.y");
        assert_eq!(diffs[0].baseline, json!(""));
        assert_eq!(diffs[0].candidate, json!("changed"));
    }
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn loads_nested_artifacts_and_counts_skipped_lines() {
        let dir = temp_artifact_dir("nested");
        let semantic_dir = dir.join("semantic");
        fs::create_dir_all(&semantic_dir).expect("semantic dir");

        // One tagged DejaRecord stream: a boundary event, two graph nodes, and
        // one unparseable line (counted as skipped). Graph nodes ride the tape
        // — there is no separate graph file.
        fs::write(
            semantic_dir.join(SEMANTIC_FILE_NAME),
            concat!(
                r#"{"record_kind":"boundary_event","global_sequence":0,"request_sequence":0,"correlation_id":"req-1","timestamp_ns":1,"boundary":"storage","trait_name":"Store","method_name":"find","call_file":"store.rs","call_line":7,"call_column":1,"request":{},"args":{},"response":{},"result":{},"is_error":false,"duration_us":10,"event_schema_version":7,"provenance":"recorded","recon":"lossless","replay_strategy":"substitute"}"#,
                "\n",
                r#"{"record_kind":"graph_node","node_id":1,"sequence":1,"span_name":"root","target":"router","level":"INFO","fields":{"request_id":"req-1"},"started_ns":1,"closed_ns":2}"#,
                "\n",
                r#"{"record_kind":"graph_node","node_id":2,"parent_id":1,"sequence":2,"span_name":"child","target":"router","level":"INFO","fields":{},"started_ns":2,"closed_ns":3}"#,
                "\n",
                "not json\n"
            ),
        )
        .expect("semantic file");

        let loaded = load_artifacts(&dir).expect("load artifacts");
        assert_eq!(loaded.semantic_events.len(), 1);
        assert_eq!(loaded.graph_records.len(), 2);
        assert_eq!(loaded.semantic_stats.as_ref().expect("stats").skipped, 1);

        let summary = summarize(&loaded);
        assert_eq!(summary.boundary_counts, vec![("storage".to_owned(), 1)]);
        assert_eq!(summary.request_counts, vec![("req-1".to_owned(), 1, 2)]);

        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn loads_replay_artifacts_and_joins_substitution() {
        let dir = temp_artifact_dir("replay");
        let recording_dir = dir.join("recording");
        let observed_dir = dir.join(OBSERVED_DIR_NAME);
        let runs_dir = dir.join(RUNS_DIR_NAME);
        fs::create_dir_all(&recording_dir).expect("recording dir");
        fs::create_dir_all(&observed_dir).expect("observed dir");
        fs::create_dir_all(&runs_dir).expect("runs dir");

        // Two recorded events: seq 0 (storage) gets substituted, seq 1 (redis) does not.
        fs::write(
            recording_dir.join(SEMANTIC_FILE_NAME),
            concat!(
                r#"{"global_sequence":0,"request_sequence":0,"correlation_id":"req-1","timestamp_ns":1,"boundary":"storage","trait_name":"Store","method_name":"find","call_file":"store.rs","call_line":7,"call_column":1,"request":{},"args":{},"response":{},"result":{},"is_error":false,"duration_us":10,"event_schema_version":7,"provenance":"recorded","recon":"lossless","replay_strategy":"substitute"}"#,
                "\n",
                r#"{"global_sequence":1,"request_sequence":1,"correlation_id":"req-1","timestamp_ns":2,"boundary":"redis","trait_name":"Cache","method_name":"get","call_file":"cache.rs","call_line":3,"call_column":1,"request":{},"args":{},"response":{},"result":{},"is_error":false,"duration_us":5,"event_schema_version":7,"provenance":"recorded","recon":"lossless","replay_strategy":"substitute"}"#,
                "\n"
            ),
        )
        .expect("semantic file");

        // Observed: one resolved call against seq 0, one unresolved novel call.
        fs::write(
            observed_dir.join("run-aaaa.jsonl"),
            concat!(
                r#"{"boundary":"storage","trait_name":"Store","method_name":"find","correlation_id":"req-1","resolved":true,"resolved_rank":4,"source_event_global_sequence":0,"args":{},"real_impl_will_fail":false,"synthesized":false}"#,
                "\n",
                r#"{"boundary":"redis","trait_name":"Cache","method_name":"get","correlation_id":"req-1","resolved":false,"resolved_rank":null,"source_event_global_sequence":null,"args":{},"real_impl_will_fail":true,"synthesized":false}"#,
                "\n"
            ),
        )
        .expect("observed file");

        fs::write(
            runs_dir.join("run-aaaa.scorecard.json"),
            r#"{"verdict":{"pass":false,"inconclusive":false,"reason":"1 omitted side-effect call(s)"},
                "summary":{"matched_correlations":1,"total_correlations":1,"http_status_mismatches":0,"http_body_mismatches":0,"side_effect_divergences":1,"omitted_calls":1,"novel_calls":0,"matched_side_effect_calls":1,"resolved_by_rank":{"rank_4":1}},
                "per_boundary":{"storage":{"matched":1,"diverged":0,"kinds":{},"tier":"stateful"},"redis":{"matched":0,"diverged":1,"kinds":{"OmittedCall":1},"tier":"stateful"}}}"#,
        )
        .expect("scorecard file");

        let loaded = load_artifacts(&dir).expect("load artifacts");
        let replay = loaded.replay.as_ref().expect("replay artifacts");
        assert_eq!(replay.observed.len(), 2);
        let scorecard = replay.scorecard.as_ref().expect("scorecard");
        assert!(!scorecard.verdict.pass);
        assert_eq!(scorecard.summary.matched_correlations, 1);
        assert_eq!(scorecard.summary.side_effect_divergences, 1);

        let status = substitution_status(&loaded.semantic_events, replay);
        assert_eq!(
            status.get(&0),
            Some(&Substitution::Substituted { rank: Some(4) })
        );
        assert_eq!(status.get(&1), Some(&Substitution::NotReplayed));

        // Scorecard is present, so per-boundary counts come from it.
        let counts = boundary_substitution_counts(&loaded.semantic_events, replay);
        assert!(counts.contains(&("storage".to_owned(), 1, 1)));
        assert!(counts.contains(&("redis".to_owned(), 0, 1)));

        fs::remove_dir_all(dir).expect("cleanup");
    }

    #[test]
    fn load_replay_returns_none_without_artifacts() {
        let dir = temp_artifact_dir("replay-empty");
        fs::create_dir_all(&dir).expect("dir");
        assert!(load_replay(&dir).is_none());
        fs::remove_dir_all(dir).expect("cleanup");
    }

    fn temp_artifact_dir(label: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!(
            "deja-tui-test-{label}-{}-{nanos}",
            std::process::id()
        ))
    }
}
