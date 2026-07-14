//! In-sandbox replay agent.
//!
//! The agent owns one sandbox run in two phases sharing one state dir:
//! `prepare` (init container) pulls the recording and renders the WHOLE
//! lookup table into the shared state dir; `drive` (main container) replays
//! the recorded HTTP requests against a router that loads that table itself.
//! Before each request, `drive` materializes that correlation's Redis/DB
//! preconditions, then sends the request and lets the router write observed
//! calls into the shared state dir for scoring/upload.

mod seeding;

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use deja_kernel::{
    compare_response, group_by_correlation, is_health_request_path, reconstruct_driver_request,
    BoundaryEvent, DriverRequest,
};
use deja_orchestrator::{CandidateSpec, HarnessRoot, Run, RunMode, RunSpec, RunStatus};
use deja_replay_core::config::{load_agent_config, AgentConfig};
use deja_replay_core::{ingest, layout, lookup};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentError {
    message: String,
}

impl AgentError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for AgentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for AgentError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AgentRunOptions {
    pub upload_artifacts: bool,
    pub post_verdict: bool,
    /// Report progress to the dashboard's stage callback (best-effort; a
    /// failed post never fails the run). State is always logged to stderr.
    pub post_stages: bool,
}

impl AgentRunOptions {
    pub fn local_only() -> Self {
        Self {
            upload_artifacts: false,
            post_verdict: false,
            post_stages: false,
        }
    }
}

impl Default for AgentRunOptions {
    fn default() -> Self {
        Self {
            upload_artifacts: true,
            post_verdict: true,
            post_stages: true,
        }
    }
}

/// The agent's stage callback URL, derived from the configured verdict URL
/// (`.../runs/{id}/verdict` → `.../runs/{id}/stage`).
fn stage_url_from_verdict_url(url: &str) -> Option<String> {
    url.strip_suffix("/verdict")
        .map(|base| format!("{base}/stage"))
}

/// Reports the agent's current state: always to stderr (visible in the pod
/// logs), and best-effort to the dashboard when `post` is set.
struct StateReporter<'a> {
    cfg: &'a AgentConfig,
    post: bool,
}

const AGENT_STEPS_TOTAL: u32 = 8;

impl StateReporter<'_> {
    fn report(&self, step: u32, stage: &str, detail: &str) {
        eprintln!("deja-replay-agent: [{step}/{AGENT_STEPS_TOTAL}] {stage} — {detail}");
        if !self.post {
            return;
        }
        if let Err(e) = self.post_stage(step, stage, detail) {
            eprintln!("deja-replay-agent: stage callback failed (continuing): {e}");
        }
    }

    fn post_stage(&self, step: u32, stage: &str, detail: &str) -> Result<(), AgentError> {
        let Some(url) = stage_url_from_verdict_url(&self.cfg.callback.url) else {
            return Ok(()); // callback URL is not the standard shape; skip
        };
        let body = serde_json::to_vec(&serde_json::json!({
            "stage": stage,
            "step": step,
            "steps_total": AGENT_STEPS_TOTAL,
            "detail": detail,
        }))
        .map_err(|e| AgentError::new(e.to_string()))?;
        let endpoint = HttpEndpoint::parse(&url)?;
        let headers = vec![
            (
                "Authorization".to_owned(),
                format!("Bearer {}", self.cfg.callback.token),
            ),
            ("X-Deja-Actor".to_owned(), "agent:replay-sandbox".to_owned()),
            ("Content-Type".to_owned(), "application/json".to_owned()),
        ];
        let response = send_http(
            "PATCH",
            &endpoint.host,
            endpoint.port,
            &endpoint.path,
            &headers,
            Some(&body),
            Duration::from_secs(10),
        )?;
        if (200..300).contains(&response.status) {
            Ok(())
        } else {
            Err(AgentError::new(format!(
                "stage callback status {}",
                response.status
            )))
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSummary {
    pub run_id: String,
    pub recording_id: String,
    pub correlations: usize,
    pub driven: usize,
    pub skipped: usize,
    pub observed: usize,
    pub verdict: String,
    pub verdict_reason: String,
    pub artifacts: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateResponse {
    pub status: u16,
    pub body: serde_json::Value,
}

pub trait SandboxClient {
    fn wait_healthy(&mut self, deadline: Duration) -> Result<(), AgentError>;
    fn drive(
        &mut self,
        request: &DriverRequest,
        timeout: Duration,
    ) -> Result<CandidateResponse, AgentError>;
}

/// Shared state-dir resolution: DEJA_AGENT_STATE_DIR, or a per-run tmp dir.
fn agent_state_dir(cfg: &AgentConfig) -> PathBuf {
    std::env::var("DEJA_AGENT_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from("/tmp")
                .join("deja-replay-agent")
                .join(&cfg.run.run_id)
        })
}

/// Init-container entrypoint: pull the recording and render the WHOLE lookup
/// table into the shared state dir, then reset the artifact files the router
/// and the drive phase will append to. Never contacts the router or mutates
/// request-derived stores.
pub fn prepare_from_config_path(path: &Path) -> Result<(), AgentError> {
    let cfg = load_agent_config(path).map_err(|e| AgentError::new(e.to_string()))?;
    let root = HarnessRoot::new(agent_state_dir(&cfg))
        .map_err(|e| AgentError::new(format!("root: {e}")))?;
    pull_recording(&cfg, &root)?;
    prepare_loaded(&cfg, &root)
}

fn pull_recording(cfg: &AgentConfig, root: &HarnessRoot) -> Result<(), AgentError> {
    let reporter = StateReporter { cfg, post: true };
    reporter.report(
        1,
        "pulling recording",
        &cfg.run.recording_uri.as_ref().map_or_else(
            || {
                format!(
                    "recording {} from s3://{}/{}",
                    cfg.run.recording_id, cfg.s3.bucket, cfg.s3.prefix
                )
            },
            |uri| format!("recording {} from {uri}", cfg.run.recording_id),
        ),
    );
    let events_path = root.recording_events_path(&cfg.run.recording_id);
    let pulled = ingest::pull_recording_source(
        &cfg.s3.to_compactor(),
        &cfg.run.recording_id,
        cfg.run.recording_uri.as_deref(),
        &events_path,
    )
    .map_err(|e| AgentError::new(format!("ingest: {e}")))?;
    reporter.report(1, "recording pulled", &ingest_summary(&pulled.report));
    Ok(())
}

fn ingest_summary(report: &ingest::IngestReport) -> String {
    let source_kind = if report.sealed {
        "sealed session"
    } else {
        "direct prefix"
    };
    let mut detail = format!(
        "{source_kind} {}: {} object(s), {} line(s), {} duplicate(s) dropped, {} event(s), {} correlation(s)",
        report.prefix,
        report.landing_objects,
        report.lines_in,
        report.duplicates_dropped,
        report.events_out,
        report.correlations,
    );

    if !report.downloaded_objects.is_empty() {
        detail.push_str("; objects: ");
        detail.push_str(&summarize_objects(&report.downloaded_objects, 8));
    }
    if !report.breakdown.record_kinds.is_empty() {
        detail.push_str("; record kinds: ");
        detail.push_str(&summarize_counts(&report.breakdown.record_kinds, 8));
    }
    if !report.breakdown.boundaries.is_empty() {
        detail.push_str("; boundaries: ");
        detail.push_str(&summarize_counts(&report.breakdown.boundaries, 12));
    }

    detail
}

fn summarize_counts(counts: &BTreeMap<String, usize>, limit: usize) -> String {
    let shown = counts
        .iter()
        .take(limit)
        .map(|(name, count)| format!("{name}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    let remaining = counts.len().saturating_sub(limit);
    if remaining == 0 {
        shown
    } else {
        format!("{shown}, ... +{remaining} more")
    }
}

fn summarize_objects(objects: &[String], limit: usize) -> String {
    let shown = objects
        .iter()
        .take(limit)
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join(", ");
    let remaining = objects.len().saturating_sub(limit);
    if remaining == 0 {
        shown
    } else {
        format!("{shown}, ... +{remaining} more")
    }
}

/// Render + write the lookup table from an already-pulled events file, and
/// reset the observed / http-diff artifacts (before the router boots, so a
/// restarting router never appends to a stale file).
fn prepare_loaded(cfg: &AgentConfig, root: &HarnessRoot) -> Result<(), AgentError> {
    let reporter = StateReporter { cfg, post: true };
    let events_path = root.recording_events_path(&cfg.run.recording_id);
    let split = deja_orchestrator::split_recording_artifacts(
        &events_path,
        &root.recording_graph_path(&cfg.run.recording_id),
    )
    .map_err(|e| AgentError::new(format!("split recording artifacts: {e}")))?;
    if split.graph_nodes > 0 {
        eprintln!(
            "deja-replay-agent: split recording stream into {} boundary event(s) and {} graph node(s)",
            split.boundary_events, split.graph_nodes
        );
    }
    let events = load_events(&events_path)?;
    let health_correlations = health_correlation_ids(&events);
    if !health_correlations.is_empty() {
        eprintln!(
            "deja-replay-agent: excluding {} /health correlation(s) from lookup table",
            health_correlations.len()
        );
    }
    reporter.report(
        2,
        "rendering lookup table",
        &format!("{} recorded events", events.len()),
    );
    let table = lookup::render_lookup_table_excluding_correlations(
        &events_path,
        &cfg.run.recording_id,
        1,
        &health_correlations,
    )
    .map_err(|e| AgentError::new(format!("lookup render: {e}")))?;
    if table.entries.is_empty() {
        return Err(AgentError::new("rendered lookup table is empty"));
    }
    write_json_file(&root.lookup_table_path(&cfg.run.run_id), &table)?;
    reset_file(&root.observed_path(&cfg.run.run_id))?;
    reset_file(&root.http_diff_path(&cfg.run.run_id))?;
    remove_file_if_exists(&root.replay_graph_path(&cfg.run.run_id))?;
    Ok(())
}

/// Main-container entrypoint: drive an already-prepared run.
pub fn drive_from_config_path(path: &Path) -> Result<AgentSummary, AgentError> {
    let cfg = load_agent_config(path).map_err(|e| AgentError::new(e.to_string()))?;
    let state_dir = agent_state_dir(&cfg);
    let mut client = HttpSandboxClient::from_config(&cfg)?;
    let root = HarnessRoot::new(&state_dir).map_err(|e| AgentError::new(format!("root: {e}")))?;
    run_loaded_recording_with_root(&cfg, &root, &mut client, AgentRunOptions::default())
}

/// Legacy single-process entrypoint: prepare then drive.
pub fn run_from_config_path(path: &Path) -> Result<AgentSummary, AgentError> {
    prepare_from_config_path(path)?;
    drive_from_config_path(path)
}

pub fn run_with_client<C: SandboxClient>(
    cfg: &AgentConfig,
    root_path: &Path,
    client: &mut C,
    options: AgentRunOptions,
) -> Result<AgentSummary, AgentError> {
    let root = HarnessRoot::new(root_path).map_err(|e| AgentError::new(format!("root: {e}")))?;
    pull_recording(cfg, &root)?;
    prepare_loaded(cfg, &root)?;
    run_loaded_recording_with_root(cfg, &root, client, options)
}

pub fn run_loaded_recording_with_client<C: SandboxClient>(
    cfg: &AgentConfig,
    root_path: &Path,
    client: &mut C,
) -> Result<AgentSummary, AgentError> {
    let root = HarnessRoot::new(root_path).map_err(|e| AgentError::new(format!("root: {e}")))?;
    run_loaded_recording_with_root(cfg, &root, client, AgentRunOptions::local_only())
}

pub fn run_loaded_recording_with_options<C: SandboxClient>(
    cfg: &AgentConfig,
    root_path: &Path,
    client: &mut C,
    options: AgentRunOptions,
) -> Result<AgentSummary, AgentError> {
    let root = HarnessRoot::new(root_path).map_err(|e| AgentError::new(format!("root: {e}")))?;
    run_loaded_recording_with_root(cfg, &root, client, options)
}

fn run_loaded_recording_with_root<C: SandboxClient>(
    cfg: &AgentConfig,
    root: &HarnessRoot,
    client: &mut C,
    options: AgentRunOptions,
) -> Result<AgentSummary, AgentError> {
    let reporter = StateReporter {
        cfg,
        post: options.post_stages,
    };
    write_run_metadata(cfg, root)?;
    let events_path = root.recording_events_path(&cfg.run.recording_id);
    let events = load_events(&events_path)?;
    let health_correlations = health_correlation_ids(&events);
    let events = exclude_correlations(events, &health_correlations);

    reporter.report(
        3,
        "waiting for router health",
        &format!("deadline {}s", cfg.limits.health_deadline_secs),
    );
    client.wait_healthy(Duration::from_secs(cfg.limits.health_deadline_secs))?;

    let mut seed_materializer = seeding::SeedMaterializer::new(cfg);
    write_json_file(
        &root.seed_certificate_path(&cfg.run.run_id),
        seed_materializer.certificate(),
    )?;

    let (by_correlation, _) = group_by_correlation(events);
    let mut ordered: Vec<(&String, &Vec<BoundaryEvent>)> = by_correlation.iter().collect();
    ordered.sort_by_key(|(_, events)| {
        events
            .iter()
            .map(|event| event.global_sequence)
            .min()
            .unwrap_or(u64::MAX)
    });

    let mut skipped = 0usize;
    let mut health_filtered = health_correlations.len();
    let mut replay_cases = Vec::new();
    for (correlation_id, events) in ordered {
        match reconstruct_driver_request(events) {
            Some(driver) if is_health_request_path(&driver.path) => {
                health_filtered += 1;
            }
            Some(driver) => replay_cases.push((correlation_id, events, driver)),
            None => skipped += 1,
        }
    }
    if health_filtered > 0 {
        eprintln!(
            "deja-replay-agent: removed {health_filtered} /health correlation(s) from replay set"
        );
    }

    let mut driven = 0usize;
    let timeout = Duration::from_secs(cfg.limits.request_timeout_secs);
    let total_correlations = replay_cases.len();
    for (index, (correlation_id, events, mut driver)) in replay_cases.into_iter().enumerate() {
        // stderr every request; dashboard on milestones so a long recording
        // does not flood the stage log.
        let position = index + 1;
        let detail = format!("correlation {position}/{total_correlations} ({correlation_id})");

        eprintln!("deja-replay-agent: [4/{AGENT_STEPS_TOTAL}] seeding stores — {detail}");
        if reporter.post && (position == 1 || position == total_correlations || position % 10 == 0)
        {
            if let Err(e) = reporter.post_stage(4, "seeding stores", &detail) {
                eprintln!("deja-replay-agent: stage callback failed (continuing): {e}");
            }
        }
        seed_materializer.materialize_correlation(cfg, correlation_id, events);
        write_json_file(
            &root.seed_certificate_path(&cfg.run.run_id),
            seed_materializer.certificate(),
        )?;

        prepare_driver_request(&mut driver, correlation_id);
        eprintln!("deja-replay-agent: [5/{AGENT_STEPS_TOTAL}] driving requests — {detail}");
        if reporter.post && (position == 1 || position == total_correlations || position % 10 == 0)
        {
            if let Err(e) = reporter.post_stage(5, "driving requests", &detail) {
                eprintln!("deja-replay-agent: stage callback failed (continuing): {e}");
            }
        }
        drive_and_collect(client, root, &cfg.run.run_id, &driver, timeout)?;
        driven += 1;
    }

    // The router owns the observed file now (its OBSERVED_SINK); count what
    // it wrote for the summary/stage detail.
    let observed_total = fs::read_to_string(root.observed_path(&cfg.run.run_id))
        .map(|text| text.lines().filter(|l| !l.trim().is_empty()).count())
        .unwrap_or(0);

    // Driven requests with ZERO observed calls is a rig failure (observed
    // sink path mismatch, replay hook not installed), not a candidate
    // signal: scoring it turns every lookup entry into a blocking
    // OmittedCall — a false full-red verdict. Refuse instead of posting one.
    if options.post_verdict && driven > 0 && observed_total == 0 {
        return Err(AgentError::new(
            "router wrote zero observed calls for driven requests; check \
             ROUTER__DEJA__REPLAY__OBSERVED_SINK matches the agent state dir \
             and that the router booted with the replay hook installed",
        ));
    }

    reporter.report(
        6,
        "scoring",
        &format!("{driven} driven, {skipped} skipped, {observed_total} observed calls"),
    );
    let scorecard = deja_orchestrator::divergence::detect_and_score(root, &cfg.run.run_id)
        .map_err(|e| AgentError::new(format!("score: {e}")))?;
    let replay_graph_count = deja_orchestrator::materialize_graph_artifact(
        &root.observed_path(&cfg.run.run_id),
        &root.replay_graph_path(&cfg.run.run_id),
    )
    .map_err(|e| AgentError::new(format!("materialize replay graph: {e}")))?;
    if replay_graph_count > 0 {
        eprintln!("deja-replay-agent: materialized {replay_graph_count} replay graph node(s)");
    }
    let verdict = if scorecard.verdict.inconclusive {
        "inconclusive"
    } else if scorecard.verdict.pass {
        "pass"
    } else {
        "fail"
    }
    .to_owned();

    let artifacts = if options.upload_artifacts {
        reporter.report(7, "uploading artifacts", &format!("verdict {verdict}"));
        upload_artifacts(cfg, root)?
    } else {
        BTreeMap::new()
    };

    let summary = AgentSummary {
        run_id: cfg.run.run_id.clone(),
        recording_id: cfg.run.recording_id.clone(),
        correlations: total_correlations,
        driven,
        skipped,
        observed: observed_total,
        verdict,
        verdict_reason: scorecard.verdict.reason.clone(),
        artifacts,
    };

    if options.post_verdict {
        reporter.report(
            8,
            "posting verdict",
            &format!(
                "{} ({} artifacts uploaded)",
                summary.verdict,
                summary.artifacts.len()
            ),
        );
        post_verdict(cfg, &summary, &scorecard)?;
    }
    eprintln!(
        "deja-replay-agent: done — verdict {} ({}/{} correlations driven)",
        summary.verdict, summary.driven, summary.correlations
    );

    Ok(summary)
}

fn drive_and_collect<C: SandboxClient>(
    client: &mut C,
    root: &HarnessRoot,
    run_id: &str,
    driver: &DriverRequest,
    timeout: Duration,
) -> Result<(), AgentError> {
    log_http_incoming_request(driver);
    let response = client.drive(driver, timeout)?;
    log_http_candidate_response(driver, &response);
    let diff = compare_response(driver, response.status, &response.body, &[]);
    append_jsonl(&root.http_diff_path(run_id), &diff)
}

fn prepare_driver_request(driver: &mut DriverRequest, correlation_id: &str) {
    driver
        .headers
        .retain(|(name, _)| !name.eq_ignore_ascii_case("x-request-id"));
    driver
        .headers
        .push(("x-request-id".to_owned(), correlation_id.to_owned()));
}

fn log_http_incoming_request(driver: &DriverRequest) {
    if !debug_log_enabled("DEJA_AGENT_LOG_HTTP_TRAFFIC", true) {
        return;
    }
    let headers = serde_json::to_string(&redacted_headers(&driver.headers))
        .unwrap_or_else(|_| "[]".to_owned());
    let body = driver
        .body
        .as_deref()
        .map(format_body_for_log)
        .unwrap_or_else(|| "<empty>".to_owned());
    eprintln!(
        "deja-replay-agent: http_incoming request correlation={} sequence={} method={} path={} headers={} body={}",
        driver.correlation_id,
        driver.request_sequence,
        driver.method,
        driver_path_with_query(driver),
        headers,
        body,
    );
}

fn log_http_candidate_response(driver: &DriverRequest, response: &CandidateResponse) {
    if !debug_log_enabled("DEJA_AGENT_LOG_HTTP_TRAFFIC", true) {
        return;
    }
    let body = truncate_for_log(&response.body.to_string());
    eprintln!(
        "deja-replay-agent: http_incoming response correlation={} sequence={} status={} body={}",
        driver.correlation_id, driver.request_sequence, response.status, body
    );
}

fn driver_path_with_query(driver: &DriverRequest) -> String {
    match &driver.query {
        Some(query) if !query.is_empty() => format!("{}?{query}", driver.path),
        _ => driver.path.clone(),
    }
}

fn redacted_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    headers
        .iter()
        .map(|(name, value)| {
            if is_sensitive_header(name) {
                (name.clone(), "<redacted>".to_owned())
            } else {
                (name.clone(), truncate_for_log(value))
            }
        })
        .collect()
}

fn is_sensitive_header(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "authorization"
        || name == "proxy-authorization"
        || name == "cookie"
        || name == "set-cookie"
        || name.contains("api-key")
        || name.contains("token")
        || name.contains("secret")
        || name.contains("signature")
}

fn format_body_for_log(body: &[u8]) -> String {
    if body.is_empty() {
        return "<empty>".to_owned();
    }
    truncate_for_log(&String::from_utf8_lossy(body))
}

fn truncate_for_log(value: &str) -> String {
    let limit = std::env::var("DEJA_AGENT_LOG_BODY_LIMIT_BYTES")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(8192);
    if value.len() <= limit {
        return value.to_owned();
    }
    let end = value
        .char_indices()
        .map(|(index, _)| index)
        .take_while(|index| *index <= limit)
        .last()
        .unwrap_or(0);
    format!(
        "{}...<truncated {} bytes>",
        &value[..end],
        value.len().saturating_sub(end)
    )
}

fn debug_log_enabled(name: &str, default: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            !matches!(value.as_str(), "0" | "false" | "off" | "no")
        })
        .unwrap_or(default)
}

fn write_run_metadata(cfg: &AgentConfig, root: &HarnessRoot) -> Result<(), AgentError> {
    let run = Run {
        run_id: cfg.run.run_id.clone(),
        spec: RunSpec {
            mode: RunMode::Replay,
            candidate_spec: CandidateSpec::PrebuiltImage {
                image: "sandbox-router".to_owned(),
            },
            migration_source: None,
            recording_id: Some(cfg.run.recording_id.clone()),
            recording_uri: cfg.run.recording_uri.clone(),
            runtime_versions: Default::default(),
            workload: serde_json::Value::Null,
        },
        status: RunStatus::Running,
        recording_id: Some(cfg.run.recording_id.clone()),
        candidate_image: None,
        failure_reason: None,
        stage: Some("agent running".to_owned()),
        step: 0,
        steps_total: 0,
        stage_updated_ms: deja_orchestrator::now_ms(),
    };
    write_json_file(&root.run_path(&cfg.run.run_id), &run)
}

fn load_events(path: &Path) -> Result<Vec<BoundaryEvent>, AgentError> {
    let file = fs::File::open(path)
        .map_err(|e| AgentError::new(format!("open {}: {e}", path.display())))?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(|e| AgentError::new(format!("read {}: {e}", path.display())))?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<deja::DejaRecord>(&line) {
            Ok(deja::DejaRecord::BoundaryEvent(event)) => events.push(*event),
            Ok(deja::DejaRecord::GraphNode(_) | deja::DejaRecord::Observed(_)) => continue,
            Err(record_err) => {
                // Older local fixtures used raw BoundaryEvent JSONL before
                // the canonical tape became a mixed DejaRecord stream.
                let event = serde_json::from_str::<BoundaryEvent>(&line).map_err(|event_err| {
                    AgentError::new(format!(
                        "parse {}: {event_err}; as DejaRecord: {record_err}",
                        path.display()
                    ))
                })?;
                events.push(event);
            }
        }
    }
    Ok(events)
}

fn health_correlation_ids(events: &[BoundaryEvent]) -> HashSet<String> {
    events
        .iter()
        .filter(|event| {
            event.boundary == "http_incoming"
                && event
                    .request
                    .get("path")
                    .and_then(|path| path.as_str())
                    .is_some_and(is_health_request_path)
        })
        .filter_map(|event| event.correlation_id.clone())
        .collect()
}

fn exclude_correlations(
    events: Vec<BoundaryEvent>,
    excluded_correlations: &HashSet<String>,
) -> Vec<BoundaryEvent> {
    if excluded_correlations.is_empty() {
        return events;
    }
    events
        .into_iter()
        .filter(|event| {
            !event
                .correlation_id
                .as_ref()
                .is_some_and(|correlation_id| excluded_correlations.contains(correlation_id))
        })
        .collect()
}

fn reset_file(path: &Path) -> Result<(), AgentError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| AgentError::new(format!("mkdir {}: {e}", parent.display())))?;
    }
    fs::File::create(path)
        .map_err(|e| AgentError::new(format!("create {}: {e}", path.display())))?;
    Ok(())
}

fn remove_file_if_exists(path: &Path) -> Result<(), AgentError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(AgentError::new(format!("remove {}: {e}", path.display()))),
    }
}

fn write_json_file<T: Serialize>(path: &Path, value: &T) -> Result<(), AgentError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| AgentError::new(format!("mkdir {}: {e}", parent.display())))?;
    }
    let bytes = serde_json::to_vec_pretty(value).map_err(|e| AgentError::new(e.to_string()))?;
    fs::write(path, bytes).map_err(|e| AgentError::new(format!("write {}: {e}", path.display())))
}

fn append_jsonl<T: Serialize>(path: &Path, value: &T) -> Result<(), AgentError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .map_err(|e| AgentError::new(format!("mkdir {}: {e}", parent.display())))?;
    }
    let mut file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| AgentError::new(format!("open {}: {e}", path.display())))?;
    let line = serde_json::to_vec(value).map_err(|e| AgentError::new(e.to_string()))?;
    file.write_all(&line)
        .and_then(|_| file.write_all(b"\n"))
        .map_err(|e| AgentError::new(format!("write {}: {e}", path.display())))
}

fn upload_artifacts(
    cfg: &AgentConfig,
    root: &HarnessRoot,
) -> Result<BTreeMap<String, String>, AgentError> {
    let s3 = cfg.s3.to_compactor();
    let run_id = &cfg.run.run_id;
    let artifacts = [
        (
            "events.jsonl",
            root.recording_events_path(&cfg.run.recording_id),
        ),
        (
            "graph.jsonl",
            root.recording_graph_path(&cfg.run.recording_id),
        ),
        ("lookup-table.json", root.lookup_table_path(run_id)),
        ("observed.jsonl", root.observed_path(run_id)),
        ("graph-replay.jsonl", root.replay_graph_path(run_id)),
        ("http-diffs.jsonl", root.http_diff_path(run_id)),
        ("scorecard.json", root.scorecard_path(run_id)),
        ("call-ledger.jsonl", root.call_ledger_path(run_id)),
        ("seed-certificate.json", root.seed_certificate_path(run_id)),
    ];

    let mut uploaded = BTreeMap::new();
    for (name, path) in artifacts {
        if !path.exists() {
            continue;
        }
        let bytes = fs::read(&path)
            .map_err(|e| AgentError::new(format!("read {}: {e}", path.display())))?;
        let key = layout::run_artifact(&cfg.s3.prefix, run_id, name);
        deja_compactor::put_object(&s3, &key, bytes)
            .map_err(|e| AgentError::new(format!("upload {name}: {e}")))?;
        uploaded.insert(name.to_owned(), format!("s3://{}/{}", cfg.s3.bucket, key));
    }
    Ok(uploaded)
}

#[derive(Debug, Serialize)]
struct VerdictPayload<'a, S: Serialize> {
    run_id: &'a str,
    verdict: &'a str,
    reason: &'a str,
    artifacts: &'a BTreeMap<String, String>,
    scorecard: &'a S,
}

fn post_verdict<S: Serialize>(
    cfg: &AgentConfig,
    summary: &AgentSummary,
    scorecard: &S,
) -> Result<(), AgentError> {
    let payload = VerdictPayload {
        run_id: &summary.run_id,
        verdict: &summary.verdict,
        reason: &summary.verdict_reason,
        artifacts: &summary.artifacts,
        scorecard,
    };
    let body = serde_json::to_vec(&payload).map_err(|e| AgentError::new(e.to_string()))?;
    let endpoint = HttpEndpoint::parse(&cfg.callback.url)?;
    let headers = vec![
        (
            "Authorization".to_owned(),
            format!("Bearer {}", cfg.callback.token),
        ),
        // The dashboard's mutation auth requires an actor identity in
        // addition to the service token.
        ("X-Deja-Actor".to_owned(), "agent:replay-sandbox".to_owned()),
        ("Content-Type".to_owned(), "application/json".to_owned()),
    ];
    let response = send_http(
        "POST",
        &endpoint.host,
        endpoint.port,
        &endpoint.path,
        &headers,
        Some(&body),
        Duration::from_secs(30),
    )?;
    if (200..300).contains(&response.status) {
        Ok(())
    } else {
        Err(AgentError::new(format!(
            "verdict callback status {}",
            response.status
        )))
    }
}

pub struct HttpSandboxClient {
    router: HttpEndpoint,
}

impl HttpSandboxClient {
    pub fn from_config(cfg: &AgentConfig) -> Result<Self, AgentError> {
        let router = HttpEndpoint::parse(&cfg.router.base_url)?;
        Ok(Self { router })
    }
}

impl SandboxClient for HttpSandboxClient {
    fn wait_healthy(&mut self, deadline: Duration) -> Result<(), AgentError> {
        let start = Instant::now();
        let path = format!("{}/health", self.router.path.trim_end_matches('/'));
        loop {
            let response = send_http(
                "GET",
                &self.router.host,
                self.router.port,
                &path,
                &[],
                None,
                Duration::from_secs(5),
            );
            if matches!(
                response,
                Ok(HttpResponse {
                    status: 200..=299,
                    ..
                })
            ) {
                return Ok(());
            }
            if start.elapsed() >= deadline {
                return Err(AgentError::new("router health deadline exceeded"));
            }
            std::thread::sleep(Duration::from_secs(1));
        }
    }

    fn drive(
        &mut self,
        request: &DriverRequest,
        timeout: Duration,
    ) -> Result<CandidateResponse, AgentError> {
        let mut path = format!("{}{}", self.router.path.trim_end_matches('/'), request.path);
        if let Some(query) = &request.query {
            path.push('?');
            path.push_str(query);
        }
        let response = match send_http(
            &request.method,
            &self.router.host,
            self.router.port,
            &path,
            &request.headers,
            request.body.as_deref(),
            timeout,
        ) {
            Ok(response) => response,
            Err(error) => {
                let message = error.to_string();
                eprintln!(
                    "deja-replay-agent: transport error correlation={} method={} path={}: {}",
                    request.correlation_id, request.method, path, message
                );
                return Ok(candidate_transport_error_response(message));
            }
        };
        let body = parse_body_json(&response.body);
        Ok(CandidateResponse {
            status: response.status,
            body,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpEndpoint {
    host: String,
    port: u16,
    path: String,
}

impl HttpEndpoint {
    fn parse(url: &str) -> Result<Self, AgentError> {
        let rest = url
            .strip_prefix("http://")
            .ok_or_else(|| AgentError::new(format!("only http:// URLs are supported: {url}")))?;
        let (authority, raw_path) = rest.split_once('/').unwrap_or((rest, ""));
        if authority.is_empty() {
            return Err(AgentError::new(format!("missing host in URL: {url}")));
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => {
                let parsed = port
                    .parse::<u16>()
                    .map_err(|e| AgentError::new(format!("invalid URL port {port}: {e}")))?;
                (host.to_owned(), parsed)
            }
            None => (authority.to_owned(), 80),
        };
        let path = if raw_path.is_empty() {
            "/".to_owned()
        } else {
            format!("/{}", raw_path.trim_end_matches('/'))
        };
        Ok(Self { host, port, path })
    }
}

fn candidate_transport_error_response(message: impl Into<String>) -> CandidateResponse {
    CandidateResponse {
        status: 599,
        body: serde_json::json!({
            "error": "transport_error",
            "message": message.into(),
        }),
    }
}

#[derive(Debug)]
struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

fn send_http(
    method: &str,
    host: &str,
    port: u16,
    path: &str,
    headers: &[(String, String)],
    body: Option<&[u8]>,
    timeout: Duration,
) -> Result<HttpResponse, AgentError> {
    let addr = format!("{host}:{port}");
    let mut stream =
        TcpStream::connect(&addr).map_err(|e| AgentError::new(format!("connect {addr}: {e}")))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| AgentError::new(format!("set read timeout: {e}")))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| AgentError::new(format!("set write timeout: {e}")))?;

    let mut head = format!("{method} {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n");
    let mut have_content_length = false;
    for (name, value) in headers {
        if name.eq_ignore_ascii_case("host") || name.eq_ignore_ascii_case("connection") {
            continue;
        }
        if name.eq_ignore_ascii_case("content-length") {
            have_content_length = true;
        }
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    if let Some(body) = body {
        if !have_content_length {
            head.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
    }
    head.push_str("\r\n");

    stream
        .write_all(head.as_bytes())
        .map_err(|e| AgentError::new(format!("write request head: {e}")))?;
    if let Some(body) = body {
        stream
            .write_all(body)
            .map_err(|e| AgentError::new(format!("write request body: {e}")))?;
    }

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|e| AgentError::new(format!("read response: {e}")))?;
    parse_http_response(&response)
}

fn parse_http_response(buf: &[u8]) -> Result<HttpResponse, AgentError> {
    let separator = b"\r\n\r\n";
    let header_end = buf
        .windows(separator.len())
        .position(|w| w == separator)
        .ok_or_else(|| AgentError::new("no header/body separator"))?;
    let header_block = &buf[..header_end];
    let body = buf[header_end + separator.len()..].to_vec();
    let header_text = std::str::from_utf8(header_block)
        .map_err(|e| AgentError::new(format!("header utf8: {e}")))?;
    let first_line = header_text
        .lines()
        .next()
        .ok_or_else(|| AgentError::new("empty header block"))?;
    let mut parts = first_line.splitn(3, ' ');
    let _version = parts.next();
    let status_str = parts
        .next()
        .ok_or_else(|| AgentError::new("missing HTTP status"))?;
    let status = status_str
        .parse::<u16>()
        .map_err(|e| AgentError::new(format!("HTTP status parse: {e}")))?;
    Ok(HttpResponse { status, body })
}

fn parse_body_json(body: &[u8]) -> serde_json::Value {
    if body.is_empty() {
        return serde_json::Value::Null;
    }
    match std::str::from_utf8(body) {
        Ok(text) => serde_json::from_str(text)
            .unwrap_or_else(|_| serde_json::Value::String(text.to_owned())),
        Err(_) => serde_json::json!({ "raw_bytes": body }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;

    #[test]
    fn stage_url_derives_from_the_verdict_url() {
        assert_eq!(
            stage_url_from_verdict_url("http://dash:8070/api/v1/runs/run-1/verdict").as_deref(),
            Some("http://dash:8070/api/v1/runs/run-1/stage")
        );
        assert_eq!(stage_url_from_verdict_url("http://dash:8070/custom"), None);
    }

    #[test]
    fn ingest_summary_previews_downloaded_objects() {
        let report = ingest::IngestReport {
            prefix: "s3://bucket/prefix".to_owned(),
            landing_objects: 3,
            lines_in: 30,
            duplicates_dropped: 2,
            lines_dropped: 0,
            events_out: 28,
            correlations: 4,
            sealed: false,
            breakdown: ingest::IngestBreakdown {
                record_kinds: BTreeMap::from([
                    ("boundary_event".to_owned(), 28),
                    ("graph_node".to_owned(), 2),
                ]),
                boundaries: BTreeMap::from([
                    ("db".to_owned(), 10),
                    ("http_incoming".to_owned(), 18),
                ]),
            },
            downloaded_objects: vec![
                "prefix/a.log.gz".to_owned(),
                "prefix/b.log.gz".to_owned(),
                "prefix/c.log.gz".to_owned(),
            ],
        };

        let summary = ingest_summary(&report);

        assert!(summary.contains("direct prefix s3://bucket/prefix"));
        assert!(summary.contains("3 object(s), 30 line(s), 2 duplicate(s) dropped"));
        assert!(summary.contains("objects: prefix/a.log.gz, prefix/b.log.gz, prefix/c.log.gz"));
        assert!(summary.contains("record kinds: boundary_event=28, graph_node=2"));
        assert!(summary.contains("boundaries: db=10, http_incoming=18"));
    }

    #[test]
    fn http_log_helpers_redact_sensitive_headers_and_truncate_utf8_safely() {
        let headers = redacted_headers(&[
            ("authorization".to_owned(), "Bearer secret".to_owned()),
            ("x-api-key".to_owned(), "key-secret".to_owned()),
            ("content-type".to_owned(), "application/json".to_owned()),
        ]);

        assert_eq!(headers[0].1, "<redacted>");
        assert_eq!(headers[1].1, "<redacted>");
        assert_eq!(headers[2].1, "application/json");

        std::env::set_var("DEJA_AGENT_LOG_BODY_LIMIT_BYTES", "5");
        let truncated = truncate_for_log("hello🙂world");
        std::env::remove_var("DEJA_AGENT_LOG_BODY_LIMIT_BYTES");
        assert!(truncated.starts_with("hello"));
        assert!(truncated.contains("<truncated "));
    }

    fn cfg() -> AgentConfig {
        toml::from_str(
            r#"
            [run]
            run_id = "run-1"
            recording_id = "rec-1"

            [s3]
            access_key = "ak"
            secret_key = "sk"
            bucket = "bucket"

            [router]
            base_url = "http://router:8080"

            [stores]
            redis_url = "redis://redis:6379"
            pg_url = "postgres://pg/deja"

            [callback]
            url = "http://dashboard:8070/api/v1/runs/run-1/verdict"
            token = "token"
            "#,
        )
        .unwrap()
    }

    fn event(correlation_id: Option<&str>, boundary: &str, seq: u64) -> BoundaryEvent {
        BoundaryEvent {
            global_sequence: seq,
            request_sequence: seq,
            correlation_id: correlation_id.map(str::to_owned),
            extras: serde_json::Map::new(),
            timestamp_ns: 0,
            recording_run_id: Some("rec-1".to_owned()),
            graph_node_id: None,
            tracing_span_id: None,
            task_id: Some("root".to_owned()),
            parent_task_id: None,
            task_bucket: Some("root".to_owned()),
            bucket_id: Some("root".to_owned()),
            fork_seq: Some(0),
            boundary: boundary.to_owned(),
            trait_name: "T".to_owned(),
            method_name: "m".to_owned(),
            call_file: "test.rs".to_owned(),
            call_line: 1,
            call_column: 1,
            receiver: None,
            request: if boundary == "http_incoming" {
                serde_json::json!({
                    "method": "POST",
                    "path": format!("/{}", correlation_id.unwrap_or("ambient")),
                    "headers": { "content-type": ["application/json"] },
                    "request_body": { "json": { "amount": 100 } }
                })
            } else {
                serde_json::Value::Null
            },
            args: serde_json::json!({ "seq": seq }),
            response: if boundary == "http_incoming" {
                serde_json::json!({
                    "status": 200,
                    "response_body": { "json": { "ok": true } }
                })
            } else {
                serde_json::Value::Null
            },
            result: serde_json::json!({ "Ok": seq }),
            is_error: false,
            duration_us: 0,
            event_schema_version: deja::CURRENT_EVENT_SCHEMA_VERSION,
            callsite_identity: None,
            provenance: deja::Provenance::default(),
            fidelity: deja::Fidelity::default(),
            result_image: None,
            pre_image: None,
            read_set: Vec::new(),
            write_set: Vec::new(),
            value_digest: None,
            entropy_source: None,
            replay_strategy: deja::ReplayStrategy::default(),
            kind: None,
            declaration: None,
            raw_draw: None,
            end_timestamp_ns: None,
        }
    }

    fn graph_node_record() -> String {
        serde_json::json!({
            "record_kind": "graph_node",
            "node_id": 1,
            "global_sequence": 0,
            "parent_id": null,
            "causal_parent_ids": [],
            "sequence": 0,
            "recording_run_id": "rec-1",
            "span_name": "root",
            "target": "router",
            "level": "INFO",
            "fields": {},
            "started_ns": 0,
            "closed_ns": null
        })
        .to_string()
    }

    struct FakeClient {
        driven: Vec<String>,
    }

    impl FakeClient {
        fn new() -> Self {
            Self { driven: Vec::new() }
        }
    }

    impl SandboxClient for FakeClient {
        fn wait_healthy(&mut self, _deadline: Duration) -> Result<(), AgentError> {
            Ok(())
        }

        fn drive(
            &mut self,
            request: &DriverRequest,
            _timeout: Duration,
        ) -> Result<CandidateResponse, AgentError> {
            self.driven.push(request.correlation_id.clone());
            assert!(request
                .headers
                .iter()
                .any(|(name, value)| name == "x-request-id" && value == &request.correlation_id));
            Ok(CandidateResponse {
                status: request.baseline_response.status,
                body: request
                    .baseline_response
                    .body_json
                    .clone()
                    .unwrap_or(serde_json::Value::Null),
            })
        }
    }

    #[test]
    fn prepare_renders_the_whole_table_and_drive_replays_requests() {
        let cfg = cfg();
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let events_path = root.recording_events_path(&cfg.run.recording_id);
        if let Some(parent) = events_path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        let events = vec![
            event(None, "redis", 0),
            event(Some("c-1"), "http_incoming", 1),
            event(Some("c-1"), "redis", 2),
            event(Some("c-2"), "http_incoming", 3),
        ];
        let mut health = event(Some("health-corr"), "http_incoming", 4);
        health.request["path"] = serde_json::json!("/health");
        let health_redis = event(Some("health-corr"), "redis", 5);
        let mut file = fs::File::create(&events_path).unwrap();
        writeln!(file, "{}", graph_node_record()).unwrap();
        for event in events {
            let record = deja::DejaRecord::BoundaryEvent(Box::new(event));
            writeln!(file, "{}", serde_json::to_string(&record).unwrap()).unwrap();
        }
        let record = deja::DejaRecord::BoundaryEvent(Box::new(health));
        writeln!(file, "{}", serde_json::to_string(&record).unwrap()).unwrap();
        let record = deja::DejaRecord::BoundaryEvent(Box::new(health_redis));
        writeln!(file, "{}", serde_json::to_string(&record).unwrap()).unwrap();

        // prepare: renders the FULL table once and resets artifact files
        prepare_loaded(&cfg, &root).unwrap();
        assert!(root.lookup_table_path(&cfg.run.run_id).exists());
        assert!(root.recording_graph_path(&cfg.run.recording_id).exists());
        let prepared_events = fs::read_to_string(&events_path).unwrap();
        assert!(!prepared_events.contains("\"record_kind\":\"graph_node\""));
        let lookup_table: deja::LookupTable =
            serde_json::from_slice(&fs::read(root.lookup_table_path(&cfg.run.run_id)).unwrap())
                .unwrap();
        assert!(lookup_table
            .entries
            .iter()
            .all(|entry| entry.key.correlation_id.as_deref() != Some("health-corr")));
        assert!(!root.seed_certificate_path(&cfg.run.run_id).exists());
        assert_eq!(
            fs::read_to_string(root.observed_path(&cfg.run.run_id)).unwrap(),
            ""
        );

        // drive: no lookup traffic, just requests
        let mut client = FakeClient::new();
        let summary = run_loaded_recording_with_client(&cfg, dir.path(), &mut client).unwrap();
        assert_eq!(summary.correlations, 2);
        assert_eq!(summary.driven, 2);
        assert_eq!(summary.skipped, 0);
        assert_eq!(client.driven, vec!["c-1", "c-2"]);
        assert!(root.seed_certificate_path(&cfg.run.run_id).exists());
        assert!(root.scorecard_path(&cfg.run.run_id).exists());
    }

    #[test]
    fn zero_observed_calls_with_driven_requests_refuses_a_verdict() {
        let cfg = cfg();
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let events_path = root.recording_events_path(&cfg.run.recording_id);
        fs::create_dir_all(events_path.parent().unwrap()).unwrap();
        let events = vec![
            event(Some("c-1"), "http_incoming", 1),
            event(Some("c-1"), "redis", 2),
        ];
        let mut file = fs::File::create(&events_path).unwrap();
        for event in events {
            let record = deja::DejaRecord::BoundaryEvent(Box::new(event));
            writeln!(file, "{}", serde_json::to_string(&record).unwrap()).unwrap();
        }
        prepare_loaded(&cfg, &root).unwrap();

        // post_verdict on (a real sandbox run) but the router never wrote a
        // single observed call: the agent must refuse, not post a full-red.
        let mut client = FakeClient::new();
        let options = AgentRunOptions {
            upload_artifacts: false,
            post_verdict: true,
            post_stages: false,
        };
        let err =
            run_loaded_recording_with_options(&cfg, dir.path(), &mut client, options).unwrap_err();
        assert!(err.to_string().contains("observed"), "got: {err}");
    }

    #[test]
    fn prepare_fails_on_an_empty_lookup_table() {
        let cfg = cfg();
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let events_path = root.recording_events_path(&cfg.run.recording_id);
        fs::create_dir_all(events_path.parent().unwrap()).unwrap();
        fs::write(&events_path, "").unwrap();
        let err = prepare_loaded(&cfg, &root).unwrap_err();
        assert!(err.to_string().contains("empty"), "got: {err}");
    }

    #[test]
    fn endpoint_parser_extracts_host_port_and_path() {
        let endpoint = HttpEndpoint::parse("http://router:8080/api/base").unwrap();
        assert_eq!(endpoint.host, "router");
        assert_eq!(endpoint.port, 8080);
        assert_eq!(endpoint.path, "/api/base");
    }

    #[test]
    fn parse_http_response_extracts_status_and_body() {
        let raw = b"HTTP/1.1 202 Accepted\r\nContent-Length: 2\r\n\r\n{}";
        let response = parse_http_response(raw).unwrap();
        assert_eq!(response.status, 202);
        assert_eq!(response.body, b"{}");
    }

    #[test]
    fn candidate_transport_error_response_uses_599() {
        let response = candidate_transport_error_response("no header/body separator");
        assert_eq!(response.status, 599);
        assert_eq!(response.body["error"], "transport_error");
        assert!(response.body["message"]
            .as_str()
            .unwrap()
            .contains("no header/body separator"));
    }
}
