//! Replay-harness kernel library.
//!
//! Pure-logic surface for the workload player: load a recording of
//! `BoundaryEvent`s, group by `correlation_id`, reconstruct each
//! correlation's first `http_incoming` event into a drivable HTTP request,
//! and compare the candidate's response against the baseline recorded
//! response. The orchestration shell in `main.rs` wires this to a
//! `reqwest::blocking::Client` and an HTTP diff sink.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub use deja::BoundaryEvent;

/// Reconstructed driver-side HTTP request, derived from a recorded
/// `http_incoming` `BoundaryEvent`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DriverRequest {
    pub correlation_id: String,
    pub request_sequence: u64,
    pub method: String,
    pub path: String,
    pub query: Option<String>,
    /// Header tuples as recorded. The kernel will set the `Host` header to
    /// the target candidate at drive time rather than reusing whatever the
    /// recorder saw.
    pub headers: Vec<(String, String)>,
    pub body: Option<Vec<u8>>,
    pub baseline_response: BaselineResponse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BaselineResponse {
    pub status: u16,
    pub body_json: Option<serde_json::Value>,
    pub body_text: Option<String>,
}

/// Whether an incoming HTTP path is the router health endpoint, which is
/// harness noise rather than replay workload.
pub fn is_health_request_path(path: &str) -> bool {
    let path = path.split_once('?').map_or(path, |(path, _)| path);
    path.trim_end_matches('/') == "/health"
}

/// Per-request comparison output, posted to the orchestrator's HTTP diff sink.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpDiff {
    pub correlation_id: String,
    pub request_sequence: u64,
    pub request_path: String,
    pub status_baseline: u16,
    pub status_candidate: u16,
    pub status_match: bool,
    pub body_diff: Vec<JsonFieldDiff>,
    /// Full recorded + replayed response bodies, so the dashboard can render a
    /// real side-by-side before/after with unchanged context (not just the
    /// changed leaves in `body_diff`). `#[serde(default)]` so pre-change diffs
    /// still parse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline_body: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_body: Option<serde_json::Value>,
}

/// A single mismatched JSON path between baseline and candidate bodies.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonFieldDiff {
    pub json_path: String,
    pub baseline: serde_json::Value,
    pub candidate: serde_json::Value,
}

/// Group events by `correlation_id`. Events with `correlation_id = None`
/// are not driveable; they appear in the background-task stream and are
/// returned separately so the caller can either skip them or surface them
/// in the run scorecard.
pub fn group_by_correlation(
    events: Vec<BoundaryEvent>,
) -> (BTreeMap<String, Vec<BoundaryEvent>>, Vec<BoundaryEvent>) {
    let mut by_corr: BTreeMap<String, Vec<BoundaryEvent>> = BTreeMap::new();
    let mut uncorrelated = Vec::new();
    for ev in events {
        match ev.correlation_id.clone() {
            Some(cid) => by_corr.entry(cid).or_default().push(ev),
            None => uncorrelated.push(ev),
        }
    }
    // Sort each correlation by request_sequence so the driver replays in
    // recorded order.
    for events in by_corr.values_mut() {
        events.sort_by_key(|e| e.request_sequence);
    }
    (by_corr, uncorrelated)
}

/// Extract the FIRST `boundary == "http_incoming"` event from a correlation
/// group and reconstruct a driveable request. Returns None when the
/// correlation has no incoming-HTTP event (background-only correlation).
pub fn reconstruct_driver_request(events: &[BoundaryEvent]) -> Option<DriverRequest> {
    let event = events.iter().find(|e| e.boundary == "http_incoming")?;
    let req = &event.request;
    let method = req.get("method")?.as_str()?.to_string();
    let path = req.get("path")?.as_str()?.to_string();
    let query = req
        .get("query")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
        .filter(|s| !s.is_empty());

    let headers = extract_headers(req.get("headers"));

    // The recorder stores the request body under `request_body` (deja's
    // IncomingHttpRecord), not `body`. Accept both for back-compat with fixtures.
    let body = extract_body_bytes(req.get("request_body").or_else(|| req.get("body")));

    let baseline_response = baseline_from_event(event);

    Some(DriverRequest {
        correlation_id: event.correlation_id.clone()?,
        request_sequence: event.request_sequence,
        method,
        path,
        query,
        headers,
        body,
        baseline_response,
    })
}

fn baseline_from_event(event: &BoundaryEvent) -> BaselineResponse {
    let resp = &event.response;
    let status = resp
        .get("status")
        .and_then(|v| v.as_u64())
        .map(|n| n as u16)
        .unwrap_or(0);
    // The recorder stores the response body under `response_body`, not `body`.
    let body = resp.get("response_body").or_else(|| resp.get("body"));
    let body_json = body.and_then(|b| b.get("json")).cloned();
    let body_text = body
        .and_then(|b| b.get("text"))
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    BaselineResponse {
        status,
        body_json,
        body_text,
    }
}

/// Extract request headers as flat (name, value) pairs. The recorder emits a
/// multimap object `{ "accept": ["*/*"], "host": ["h"] }` (deja::http::headers);
/// older fixtures use an array `[{"key":..,"value":..}]`. Accept both.
fn extract_headers(value: Option<&serde_json::Value>) -> Vec<(String, String)> {
    let value = match value {
        Some(v) => v,
        None => return Vec::new(),
    };
    // Recorder shape: object name -> [values] (or a bare string value).
    if let Some(obj) = value.as_object() {
        let mut out = Vec::new();
        for (name, v) in obj {
            match v {
                serde_json::Value::Array(vals) => {
                    for vv in vals {
                        if let Some(s) = vv.as_str() {
                            out.push((name.clone(), s.to_owned()));
                        }
                    }
                }
                serde_json::Value::String(s) => out.push((name.clone(), s.clone())),
                _ => {}
            }
        }
        return out;
    }
    // Legacy/fixture shape: [{"key":..,"value":..}, ...].
    if let Some(arr) = value.as_array() {
        return arr
            .iter()
            .filter_map(|h| {
                let k = h.get("key")?.as_str()?.to_string();
                let v = h.get("value")?.as_str()?.to_string();
                Some((k, v))
            })
            .collect();
    }
    Vec::new()
}

fn extract_body_bytes(body: Option<&serde_json::Value>) -> Option<Vec<u8>> {
    let body = body?;
    // Prefer raw_bytes (exact wire bytes), fall back to text, then to a
    // re-serialized json field.
    if let Some(arr) = body.get("raw_bytes").and_then(|v| v.as_array()) {
        let bytes: Vec<u8> = arr
            .iter()
            .filter_map(|v| v.as_u64().map(|n| n as u8))
            .collect();
        if !bytes.is_empty() {
            return Some(bytes);
        }
    }
    if let Some(text) = body.get("text").and_then(|v| v.as_str()) {
        if !text.is_empty() {
            return Some(text.as_bytes().to_vec());
        }
    }
    if let Some(json) = body.get("json") {
        if !json.is_null() {
            return serde_json::to_vec(json).ok();
        }
    }
    None
}

/// Compute a path-level diff between two JSON values. `path` is the JSONPath
/// prefix the caller is recursing under (starts as `"$"`). `allowlist`
/// suppresses divergences at any JSONPath in the set (e.g. `$.payment_id`
/// for fields the candidate computes itself).
pub fn diff_json(
    baseline: &serde_json::Value,
    candidate: &serde_json::Value,
    path: &str,
    allowlist: &[&str],
) -> Vec<JsonFieldDiff> {
    if allowlist.contains(&path) {
        return Vec::new();
    }
    if baseline == candidate {
        return Vec::new();
    }
    match (baseline, candidate) {
        (serde_json::Value::Object(b), serde_json::Value::Object(c)) => {
            let mut diffs = Vec::new();
            let mut keys: Vec<&String> = b.keys().chain(c.keys()).collect();
            keys.sort();
            keys.dedup();
            for k in keys {
                let next_path = format!("{path}.{k}");
                let b_val = b.get(k).unwrap_or(&serde_json::Value::Null);
                let c_val = c.get(k).unwrap_or(&serde_json::Value::Null);
                diffs.extend(diff_json(b_val, c_val, &next_path, allowlist));
            }
            diffs
        }
        (serde_json::Value::Array(b), serde_json::Value::Array(c)) => {
            let mut diffs = Vec::new();
            let len = b.len().max(c.len());
            for i in 0..len {
                let next_path = format!("{path}[{i}]");
                let b_val = b.get(i).unwrap_or(&serde_json::Value::Null);
                let c_val = c.get(i).unwrap_or(&serde_json::Value::Null);
                diffs.extend(diff_json(b_val, c_val, &next_path, allowlist));
            }
            diffs
        }
        (b, c) => vec![JsonFieldDiff {
            json_path: path.to_owned(),
            baseline: b.clone(),
            candidate: c.clone(),
        }],
    }
}

/// Build an `HttpDiff` from baseline + candidate, applying the allowlist.
pub fn compare_response(
    driver: &DriverRequest,
    candidate_status: u16,
    candidate_body: &serde_json::Value,
    allowlist: &[&str],
) -> HttpDiff {
    let baseline_json = driver
        .baseline_response
        .body_json
        .clone()
        .unwrap_or(serde_json::Value::Null);
    let body_diff = diff_json(&baseline_json, candidate_body, "$", allowlist);
    HttpDiff {
        correlation_id: driver.correlation_id.clone(),
        request_sequence: driver.request_sequence,
        request_path: driver.path.clone(),
        status_baseline: driver.baseline_response.status,
        status_candidate: candidate_status,
        status_match: driver.baseline_response.status == candidate_status,
        body_diff,
        baseline_body: driver.baseline_response.body_json.clone(),
        candidate_body: Some(candidate_body.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_event(req: serde_json::Value, resp: serde_json::Value, seq: u64) -> BoundaryEvent {
        BoundaryEvent {
            extras: serde_json::Map::new(),
            global_sequence: seq,
            request_sequence: seq,
            correlation_id: Some("c-1".to_owned()),
            timestamp_ns: 0,
            recording_run_id: Some("run".to_owned()),
            graph_node_id: None,
            tracing_span_id: None,
            task_id: Some("root".to_owned()),
            parent_task_id: None,
            task_bucket: Some("c-1".to_owned()),
            bucket_id: Some("c-1".to_owned()),
            fork_seq: Some(0),
            boundary: "http_incoming".to_owned(),
            trait_name: "RequestIdMiddleware".to_owned(),
            method_name: "call".to_owned(),
            call_file: "x".to_owned(),
            call_line: 0,
            call_column: 0,
            receiver: None,
            request: req,
            args: serde_json::Value::Null,
            response: resp,
            result: serde_json::Value::Null,
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

    #[test]
    fn reconstruct_extracts_method_path_headers_body_status() {
        let req = serde_json::json!({
            "method": "POST",
            "path": "/payments",
            "query": "expand=true",
            "headers": [
                { "key": "content-type", "value": "application/json" },
                { "key": "api-key", "value": "secret" }
            ],
            "body": { "text": "{\"amount\":100}" }
        });
        let resp = serde_json::json!({
            "status": 200,
            "body": { "json": { "id": "pay_1", "status": "succeeded" } }
        });
        let event = json_event(req, resp, 0);
        let drv = reconstruct_driver_request(&[event]).expect("reconstruct");
        assert_eq!(drv.method, "POST");
        assert_eq!(drv.path, "/payments");
        assert_eq!(drv.query.as_deref(), Some("expand=true"));
        assert_eq!(drv.headers.len(), 2);
        assert_eq!(drv.headers[0].0, "content-type");
        assert_eq!(drv.body.as_deref(), Some(b"{\"amount\":100}".as_slice()));
        assert_eq!(drv.baseline_response.status, 200);
        assert_eq!(
            drv.baseline_response.body_json,
            Some(serde_json::json!({ "id": "pay_1", "status": "succeeded" }))
        );
    }

    #[test]
    fn health_request_path_matches_only_health_endpoint() {
        assert!(is_health_request_path("/health"));
        assert!(is_health_request_path("/health/"));
        assert!(is_health_request_path("/health?probe=1"));
        assert!(!is_health_request_path("/healthz"));
        assert!(!is_health_request_path("/payments/health"));
        assert!(!is_health_request_path("/"));
    }

    #[test]
    fn reconstruct_handles_real_recorder_shape() {
        // The shape deja ACTUALLY produces (verified against a real recording):
        // headers as a name->[values] object, request body under `request_body`,
        // response body under `response_body` (no top-level `body` key).
        let req = serde_json::json!({
            "method": "POST",
            "path": "/payments",
            "headers": { "content-type": ["application/json"], "api-key": ["secret"] },
            "request_body": { "text": "{\"amount\":100}" }
        });
        let resp = serde_json::json!({
            "status": 200,
            "response_body": { "json": { "id": "pay_1", "status": "succeeded" } }
        });
        let event = json_event(req, resp, 0);
        let drv = reconstruct_driver_request(&[event]).expect("reconstruct");
        assert_eq!(drv.method, "POST");
        assert_eq!(drv.body.as_deref(), Some(b"{\"amount\":100}".as_slice()));
        assert_eq!(drv.headers.len(), 2, "name->[values] object headers parsed");
        assert!(drv
            .headers
            .iter()
            .any(|(k, v)| k == "content-type" && v == "application/json"));
        assert_eq!(drv.baseline_response.status, 200);
        assert_eq!(
            drv.baseline_response.body_json,
            Some(serde_json::json!({ "id": "pay_1", "status": "succeeded" })),
            "response_body.json read as baseline"
        );
    }

    #[test]
    fn group_by_correlation_separates_correlated_and_uncorrelated() {
        let mut a = json_event(serde_json::Value::Null, serde_json::Value::Null, 0);
        a.correlation_id = Some("a".into());
        let mut b = json_event(serde_json::Value::Null, serde_json::Value::Null, 1);
        b.correlation_id = Some("b".into());
        let mut c = json_event(serde_json::Value::Null, serde_json::Value::Null, 2);
        c.correlation_id = None;
        let (by_corr, uncorr) = group_by_correlation(vec![a, b, c]);
        assert_eq!(by_corr.len(), 2);
        assert_eq!(uncorr.len(), 1);
    }

    #[test]
    fn json_diff_finds_field_mismatch_at_nested_path() {
        let baseline = serde_json::json!({
            "id": "pay_1",
            "amount": 100,
            "customer": { "email": "a@b.c" }
        });
        let candidate = serde_json::json!({
            "id": "pay_1",
            "amount": 200,
            "customer": { "email": "a@b.c" }
        });
        let diffs = diff_json(&baseline, &candidate, "$", &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].json_path, "$.amount");
        assert_eq!(diffs[0].baseline, serde_json::json!(100));
        assert_eq!(diffs[0].candidate, serde_json::json!(200));
    }

    #[test]
    fn json_diff_respects_allowlist() {
        let baseline = serde_json::json!({ "id": "pay_X", "amount": 100 });
        let candidate = serde_json::json!({ "id": "pay_Y", "amount": 100 });
        // Without allowlist: 1 diff at $.id.
        assert_eq!(diff_json(&baseline, &candidate, "$", &[]).len(), 1);
        // With $.id allowlisted: 0 diffs.
        assert_eq!(diff_json(&baseline, &candidate, "$", &["$.id"]).len(), 0);
    }

    #[test]
    fn json_diff_handles_array_length_mismatch() {
        let baseline = serde_json::json!([1, 2, 3]);
        let candidate = serde_json::json!([1, 2, 3, 4]);
        let diffs = diff_json(&baseline, &candidate, "$", &[]);
        assert_eq!(diffs.len(), 1);
        assert_eq!(diffs[0].json_path, "$[3]");
        assert_eq!(diffs[0].baseline, serde_json::Value::Null);
        assert_eq!(diffs[0].candidate, serde_json::json!(4));
    }
}
