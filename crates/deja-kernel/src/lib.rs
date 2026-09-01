//! Replay-harness kernel library.
//!
//! Pure-logic surface for the workload player: load a recording of
//! `BoundaryEvent`s, group by `correlation_id`, reconstruct each
//! correlation's ingress event into a drivable request, and compare the
//! candidate's response against the baseline recorded response. The
//! orchestration shell in `main.rs` dispatches per correlation on the
//! recorded ingress shape: HTTP ingress (`method`/`path`) drives over the
//! hand-rolled HTTP/1.1 client, gRPC ingress (`rpc`) over the [`grpc`]
//! module's HTTP/2 client — one kernel binary, one `KERNEL_*` contract.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

pub mod grpc;

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
    /// The recorded response body as BYTES, recovered from whichever
    /// representation the tape carries. Bytes, not a pre-picked `json`/`text`
    /// pair: the recorder writes both halves of every body (`text` always,
    /// `json` only when the bytes happen to parse), and holding both here is
    /// what let the comparison read one field while the candidate side read
    /// the other. There is one representation now, and `project_body` is the
    /// only way it becomes a comparable value.
    ///
    /// `None` means the tape carried no body object at all for this response.
    /// `Some(empty)` means the recorder captured zero bytes.
    pub body: Option<Vec<u8>>,
}

impl BaselineResponse {
    /// The recorded body exactly as the comparison sees it, or `None` when the
    /// tape carried no body at all. Same function as the candidate side.
    pub fn projected_body(&self) -> Option<serde_json::Value> {
        self.body.as_deref().map(project_body)
    }
}

/// The ONE projection from captured response bytes to the value a diff is
/// taken over. Both halves call it — the baseline half on the bytes the
/// recorder wrote, the candidate half on the bytes it read off the wire — so
/// "same bytes" implies "same value" by construction rather than by two
/// hand-matched implementations.
///
/// Bytes that parse as JSON become that JSON (including the literal `null`
/// body, which becomes `Value::Null` on BOTH sides and so still compares
/// equal). Bytes that do not parse — an HTML redirect form, a plain-text
/// error, an empty body — become the string of those bytes. Non-UTF-8 bytes
/// go through `from_utf8_lossy`, which is lossy but symmetric: identical
/// bytes still project identically.
pub fn project_body(bytes: &[u8]) -> serde_json::Value {
    let text = String::from_utf8_lossy(bytes).into_owned();
    serde_json::from_str(&text).unwrap_or(serde_json::Value::String(text))
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
    /// Why there is no candidate response, when there is none. A transport
    /// failure used to be encoded as `status_candidate: 0` with the error
    /// buried in the body — and the field diff only walks RECORDED fields, so
    /// the one string that says WHY (connection refused vs read timeout) never
    /// surfaced anywhere. Every k8s run to date failed exactly this way and
    /// nothing said so. `Some` here means "the candidate never answered";
    /// field diffs on such a row describe the absence, not behaviour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport_error: Option<String>,
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

/// Extract the FIRST ingress event (self-described `role: "ingress"`, legacy
/// `http_incoming` name) from a correlation group and reconstruct a driveable
/// HTTP request. Returns None when the correlation has no ingress event
/// (background-only correlation) — or when its ingress is not HTTP-shaped
/// (no `method`/`path`; a gRPC ingress), which the caller hands to
/// [`grpc::reconstruct_grpc_request`] instead.
pub fn reconstruct_driver_request(events: &[BoundaryEvent]) -> Option<DriverRequest> {
    let event = events.iter().find(|e| e.is_ingress())?;
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
    let body = resp
        .get("response_body")
        .or_else(|| resp.get("body"))
        .and_then(captured_body_bytes);
    BaselineResponse { status, body }
}

/// Recover the recorded body BYTES from a capture object, in descending order
/// of fidelity: the exact wire bytes, then the UTF-8 text, then a
/// re-serialization of the parsed JSON (old fixtures carry only `json`).
///
/// `Some(empty)` for `captured: false`. The recorder cannot tell an empty body
/// from a stream it never finished draining — its own reason string says
/// "empty body or stream incomplete" — so this projects the common case (a
/// genuinely bodyless response) and ACCEPTS that a truncated capture will
/// compare equal to an empty candidate response instead of raising a body
/// divergence. The alternative, treating it as `null`, made every bodyless
/// response diff against the candidate's empty body forever.
///
/// `None` only when the tape says nothing about a body at all.
pub(crate) fn captured_body_bytes(body: &serde_json::Value) -> Option<Vec<u8>> {
    if let Some(arr) = body.get("raw_bytes").and_then(|v| v.as_array()) {
        return Some(
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as u8))
                .collect(),
        );
    }
    if let Some(text) = body.get("text").and_then(|v| v.as_str()) {
        return Some(text.as_bytes().to_vec());
    }
    // A `json` of literal `null` is the recorder saying "these bytes did not
    // parse", not "the body was null" — the body that WAS null still carries
    // `text: "null"` and is caught above.
    if let Some(json) = body.get("json").filter(|j| !j.is_null()) {
        return serde_json::to_vec(json).ok();
    }
    if body.get("captured").and_then(|v| v.as_bool()) == Some(false) {
        return Some(Vec::new());
    }
    None
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

/// Request-side sibling of `captured_body_bytes`, deliberately NOT the same
/// function: here `None` and "zero bytes" mean the same thing — no body is
/// written and no `Content-Length` is sent — whereas on the response side the
/// difference between "no body recorded" and "an empty body was recorded" is
/// exactly what a diff has to report.
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
        // EMBEDDED JSON IS COMPARED AS JSON, NOT AS BYTES. A string field
        // that carries a serialized document (prism's rawConnectorRequest
        // echo, hyperswitch metadata blobs) is byte-compared by default — and
        // a map stringified by two different PROCESSES serializes its keys in
        // two different orders (HashMap iteration is per-process random), so
        // identical behavior diffs on every response. Key order inside a JSON
        // object is serialization noise, not behavior; the comparator's job
        // is behavioral equivalence, so both sides are parsed and diffed
        // structurally — which also locates a REAL inner difference at its
        // own path instead of reporting one opaque blob.
        //
        // Only when BOTH sides parse to an object or array: scalar strings
        // keep byte semantics ("1.0" vs "1.00" stays a diff — relaxing number
        // formatting was never asked for), and a non-JSON string on either
        // side falls through to the leaf diff unchanged. The TAPE is never
        // touched: this normalizes judgment, not evidence.
        (serde_json::Value::String(b), serde_json::Value::String(c)) => {
            if let (Ok(bv), Ok(cv)) = (
                serde_json::from_str::<serde_json::Value>(b),
                serde_json::from_str::<serde_json::Value>(c),
            ) {
                let structural = |v: &serde_json::Value| v.is_object() || v.is_array();
                if structural(&bv) && structural(&cv) {
                    return diff_json(&bv, &cv, path, allowlist);
                }
            }
            // Same rule, second encoding: a form-urlencoded body is a map
            // serialized into a string, and the serializing map's iteration
            // order is per-process random — prism's Stripe bodies reorder
            // `metadata[...]` pairs on every process. DISTINCT keys compare
            // order-insensitively; repeated same-named keys keep their
            // relative order (that order is array semantics, not noise).
            if let (Some(bp), Some(cp)) = (parse_form_pairs(b), parse_form_pairs(c)) {
                return diff_form_pairs(&bp, &cp, path, allowlist);
            }
            vec![JsonFieldDiff {
                json_path: path.to_owned(),
                baseline: baseline.clone(),
                candidate: candidate.clone(),
            }]
        }
        (b, c) => vec![JsonFieldDiff {
            json_path: path.to_owned(),
            baseline: b.clone(),
            candidate: c.clone(),
        }],
    }
}

/// Read a string as form-urlencoded pairs, STRICTLY: at least two
/// `&`-separated segments (a single pair cannot be reordered, so bytes
/// suffice there), every segment non-empty and carrying `key=`, keys
/// non-empty. Anything else — prose, html, base64 — is not form data and
/// keeps byte semantics. Values stay percent-ENCODED: both sides were
/// produced by the same encoder, so encoded equality is value equality and
/// decoding could only blur that.
fn parse_form_pairs(raw: &str) -> Option<Vec<(&str, &str)>> {
    let segments: Vec<&str> = raw.split('&').collect();
    if segments.len() < 2 {
        return None;
    }
    segments
        .iter()
        .map(|segment| match segment.split_once('=') {
            Some((k, v)) if !k.is_empty() => Some((k, v)),
            _ => None,
        })
        .collect()
}

/// Compare two pair lists: per key, the ordered list of its values must be
/// equal (repeated-key order is meaningful; distinct-key order is not). A
/// differing key reports at `{path}.{key}` — the key names the field, which
/// beats one opaque body diff.
fn diff_form_pairs(
    baseline: &[(&str, &str)],
    candidate: &[(&str, &str)],
    path: &str,
    allowlist: &[&str],
) -> Vec<JsonFieldDiff> {
    let group = |pairs: &[(&str, &str)]| {
        let mut by_key: std::collections::BTreeMap<String, Vec<String>> = Default::default();
        for (k, v) in pairs {
            by_key
                .entry((*k).to_owned())
                .or_default()
                .push((*v).to_owned());
        }
        by_key
    };
    let b = group(baseline);
    let c = group(candidate);
    let mut keys: Vec<&String> = b.keys().chain(c.keys()).collect();
    keys.sort();
    keys.dedup();
    let mut diffs = Vec::new();
    for k in keys {
        let key_path = format!("{path}.{k}");
        if allowlist.contains(&key_path.as_str()) {
            continue;
        }
        let bv = b.get(k);
        let cv = c.get(k);
        if bv == cv {
            continue;
        }
        let to_json = |v: Option<&Vec<String>>| match v {
            None => serde_json::Value::Null,
            Some(vals) if vals.len() == 1 => serde_json::Value::String(vals[0].clone()),
            Some(vals) => serde_json::json!(vals),
        };
        diffs.push(JsonFieldDiff {
            json_path: key_path,
            baseline: to_json(bv),
            candidate: to_json(cv),
        });
    }
    diffs
}

/// Build an `HttpDiff` from baseline + candidate, applying the allowlist.
pub fn compare_response(
    driver: &DriverRequest,
    candidate_status: u16,
    candidate_body: &serde_json::Value,
    allowlist: &[&str],
) -> HttpDiff {
    // `baseline_body` IS the value that was diffed — not a second, separately
    // derived view of the same bytes. That equality is the invariant this
    // function exists to hold.
    let baseline_body = driver.baseline_response.projected_body();
    let baseline_json = baseline_body.clone().unwrap_or(serde_json::Value::Null);
    let body_diff = diff_json(&baseline_json, candidate_body, "$", allowlist);
    HttpDiff {
        correlation_id: driver.correlation_id.clone(),
        request_sequence: driver.request_sequence,
        request_path: driver.path.clone(),
        status_baseline: driver.baseline_response.status,
        status_candidate: candidate_status,
        status_match: driver.baseline_response.status == candidate_status,
        body_diff,
        baseline_body,
        candidate_body: Some(candidate_body.clone()),
        transport_error: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_event(req: serde_json::Value, resp: serde_json::Value, seq: u64) -> BoundaryEvent {
        BoundaryEvent {
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
            role: None,
            declaration: None,
            raw_draw: None,
            end_timestamp_ns: None,
        }
    }

    #[test]
    fn reconstruct_finds_ingress_by_role_not_only_by_legacy_name() {
        // A role-described HTTP ingress under a NEW boundary name reconstructs;
        // the legacy-name path is covered by every other test in this module.
        let req = serde_json::json!({ "method": "GET", "path": "/x" });
        let mut event = json_event(req, serde_json::json!({"status": 200}), 0);
        event.boundary = "axum_incoming".to_owned();
        assert!(
            reconstruct_driver_request(std::slice::from_ref(&event)).is_none(),
            "an unknown boundary without a role is NOT ingress"
        );
        event.role = Some("ingress".to_owned());
        let drv = reconstruct_driver_request(&[event]).expect("role marks ingress");
        assert_eq!(drv.path, "/x");
    }

    #[test]
    fn grpc_shaped_ingress_yields_none_from_the_http_reconstruct() {
        // A gRPC ingress (rpc, no method/path) is ingress but not HTTP-drivable;
        // the drive loop hands it to grpc::reconstruct_grpc_request instead.
        let req = serde_json::json!({ "rpc": "/types.PaymentService/Authorize" });
        let mut event = json_event(req, serde_json::json!({}), 0);
        event.boundary = "grpc_incoming".to_owned();
        event.role = Some("ingress".to_owned());
        assert!(reconstruct_driver_request(&[event]).is_none());
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
            drv.baseline_response.projected_body(),
            Some(serde_json::json!({ "id": "pay_1", "status": "succeeded" }))
        );
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
            drv.baseline_response.projected_body(),
            Some(serde_json::json!({ "id": "pay_1", "status": "succeeded" })),
            "response_body read as baseline"
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

    /// Byte-for-byte what `deja-runtime`'s `inject_body_json` writes for a
    /// non-empty body: BOTH representations, with `json` null whenever the
    /// bytes do not parse. The tests below drive the real recorder shape so a
    /// change to that shape breaks them here rather than in a run.
    fn captured_body(bytes: &[u8]) -> serde_json::Value {
        let text = std::str::from_utf8(bytes).ok().map(str::to_string);
        let parsed = text
            .as_deref()
            .and_then(|t| serde_json::from_str::<serde_json::Value>(t).ok());
        serde_json::json!({
            "captured": true,
            "bytes_len": bytes.len(),
            "utf8": text.is_some(),
            "text": text,
            "json": parsed,
            "raw_bytes": bytes,
        })
    }

    /// What the recorder writes when it captured no bytes.
    fn captured_nothing() -> serde_json::Value {
        serde_json::json!({
            "captured": false,
            "reason": "empty body or stream incomplete",
        })
    }

    /// Drive one recorded body against one candidate body, both as raw bytes,
    /// through the exact path a real run takes.
    fn compare_bodies(recorded: serde_json::Value, candidate: &[u8]) -> HttpDiff {
        let req = serde_json::json!({ "method": "GET", "path": "/payments/redirect/x" });
        let resp = serde_json::json!({ "status": 200, "response_body": recorded });
        let drv = reconstruct_driver_request(&[json_event(req, resp, 0)]).expect("reconstruct");
        compare_response(&drv, 200, &project_body(candidate), &[])
    }

    const HTML: &[u8] =
        b"<!DOCTYPE html><html><body><form method=\"post\" action=\"https://psp/pay\"></form></body></html>";

    /// The defect: a string field carrying a serialized document is
    /// byte-compared, and a map stringified by two different PROCESSES
    /// serializes its keys in two different orders (HashMap iteration is
    /// per-process random) — prism's rawConnectorRequest echo diffed on every
    /// replayed response over pure key order. Key order inside a JSON object
    /// is serialization noise, not behavior: embedded JSON is compared AS
    /// JSON. The tape is untouched — this normalizes judgment, not evidence.
    #[test]
    fn embedded_json_strings_compare_structurally_not_bytewise() {
        let recorded = serde_json::json!({
            "rawConnectorRequest": {"value":
                "{\"url\":\"https://api.stripe.com/v1/payment_intents\",\"headers\":{\"stripe-version\":\"2022-11-15\",\"via\":\"HyperSwitch\"}}"}
        });
        let replayed = serde_json::json!({
            "rawConnectorRequest": {"value":
                "{\"headers\":{\"via\":\"HyperSwitch\",\"stripe-version\":\"2022-11-15\"},\"url\":\"https://api.stripe.com/v1/payment_intents\"}"}
        });
        let diff = diff_json(&recorded, &replayed, "$", &[]);
        assert!(diff.is_empty(), "key order is not behavior: {diff:?}");
    }

    /// …and the other half: a REAL difference inside the embedded document
    /// still fails — now located at its own inner path instead of one opaque
    /// blob diff, which is strictly better reporting.
    #[test]
    fn a_real_difference_inside_an_embedded_json_string_still_diverges() {
        let recorded =
            serde_json::json!({"echo": "{\"headers\":{\"via\":\"HyperSwitch\"},\"amount\":6540}"});
        let replayed =
            serde_json::json!({"echo": "{\"amount\":9999,\"headers\":{\"via\":\"HyperSwitch\"}}"});
        let diff = diff_json(&recorded, &replayed, "$", &[]);
        assert_eq!(diff.len(), 1, "{diff:?}");
        assert_eq!(diff[0].json_path, "$.echo.amount");
        assert_eq!(diff[0].baseline, serde_json::json!(6540));
        assert_eq!(diff[0].candidate, serde_json::json!(9999));
    }

    /// Scalar strings keep BYTE semantics: "1.0" and "1.00" parse to equal
    /// numbers, but relaxing number formatting was never asked for — only
    /// object/array documents get the structural comparison. And a parent-path
    /// allowlist entry still suppresses everything beneath, embedded or not.
    #[test]
    fn scalar_strings_stay_byte_exact_and_the_allowlist_still_covers_embedded_docs() {
        let b = serde_json::json!({"v": "1.0"});
        let c = serde_json::json!({"v": "1.00"});
        let diff = diff_json(&b, &c, "$", &[]);
        assert_eq!(
            diff.len(),
            1,
            "scalar formatting must stay a diff: {diff:?}"
        );
        assert_eq!(diff[0].json_path, "$.v");

        let b = serde_json::json!({"echo": "{\"a\":1}"});
        let c = serde_json::json!({"echo": "{\"a\":2}"});
        assert!(diff_json(&b, &c, "$", &["$.echo"]).is_empty());
    }

    /// Same rule, second encoding: prism serializes the Stripe form body's
    /// `metadata[...]` map in per-process-random order — identical pairs,
    /// different byte order, on every replayed response. Distinct-key order
    /// is serialization noise; the pairs are the behavior.
    #[test]
    fn form_encoded_strings_with_reordered_distinct_keys_are_equal() {
        let b = serde_json::json!({"body":
            "amount=6540&currency=USD&metadata%5Blogin_date%5D=2019-09-10T10%3A11%3A12Z&metadata%5Budf1%5D=value1"});
        let c = serde_json::json!({"body":
            "amount=6540&metadata%5Budf1%5D=value1&metadata%5Blogin_date%5D=2019-09-10T10%3A11%3A12Z&currency=USD"});
        let diff = diff_json(&b, &c, "$", &[]);
        assert!(
            diff.is_empty(),
            "distinct-key order is not behavior: {diff:?}"
        );
    }

    /// …but repeated SAME-named keys are ordered data (array semantics), and
    /// a real value change reports at its own key, not as one opaque body.
    #[test]
    fn form_encoded_repeats_keep_order_and_value_changes_name_their_key() {
        let b = serde_json::json!({"body": "items=a&items=b"});
        let c = serde_json::json!({"body": "items=b&items=a"});
        let diff = diff_json(&b, &c, "$", &[]);
        assert_eq!(
            diff.len(),
            1,
            "repeated-key reorder must stay a diff: {diff:?}"
        );
        assert_eq!(diff[0].json_path, "$.body.items");

        let b = serde_json::json!({"body": "amount=6540&currency=USD"});
        let c = serde_json::json!({"body": "currency=USD&amount=9999"});
        let diff = diff_json(&b, &c, "$", &[]);
        assert_eq!(diff.len(), 1, "{diff:?}");
        assert_eq!(diff[0].json_path, "$.body.amount");
        assert_eq!(diff[0].baseline, serde_json::json!("6540"));
        assert_eq!(diff[0].candidate, serde_json::json!("9999"));
    }

    /// The qualifier is STRICT: prose, a lone pair, or an `=`-less segment
    /// is not form data and keeps byte semantics — over-matching here would
    /// quietly relax comparisons that were never map-shaped.
    #[test]
    fn non_form_strings_keep_byte_semantics() {
        for (b, c) in [
            ("a=1", "a=2"),                         // single pair: bytes suffice
            ("one&two=2", "two=2&one"),             // '='-less segment
            ("hello world & more", "more & hello"), // prose
        ] {
            let bj = serde_json::json!({ "v": b });
            let cj = serde_json::json!({ "v": c });
            let diff = diff_json(&bj, &cj, "$", &[]);
            assert_eq!(diff.len(), 1, "{b:?} vs {c:?} must byte-diff: {diff:?}");
            assert_eq!(diff[0].json_path, "$.v");
        }
    }

    /// The defect: `text/html` never parses as JSON, so the recorder's `json`
    /// field is null while `text` holds the document. Reading only `json` on
    /// the baseline side while the candidate side kept non-JSON as a string
    /// reported one whole-body divergence at `$` for EVERY html reply — bodies
    /// that were identical included. Identical bytes must diff nowhere.
    #[test]
    fn identical_html_bodies_are_not_a_divergence() {
        let diff = compare_bodies(captured_body(HTML), HTML);
        assert!(
            diff.body_diff.is_empty(),
            "identical html reported as divergence: {:?}",
            diff.body_diff
        );
        assert_eq!(
            diff.baseline_body,
            Some(serde_json::Value::String(
                String::from_utf8_lossy(HTML).into_owned()
            )),
            "baseline_body must carry the document, not null"
        );
        assert_eq!(diff.baseline_body, diff.candidate_body);
    }

    /// …and the other half of the property: suppressing the false positive
    /// must not suppress the true one. An html body that really changed still
    /// diffs, with both sides visible.
    #[test]
    fn differing_html_bodies_still_diverge() {
        let candidate = b"<!DOCTYPE html><html><body><form method=\"post\" action=\"https://other/pay\"></form></body></html>";
        let diff = compare_bodies(captured_body(HTML), candidate);
        assert_eq!(diff.body_diff.len(), 1, "{:?}", diff.body_diff);
        assert_eq!(diff.body_diff[0].json_path, "$");
        assert!(
            diff.body_diff[0].baseline.is_string(),
            "baseline side must be the recorded document, not null: {:?}",
            diff.body_diff[0].baseline
        );
        assert_ne!(diff.body_diff[0].baseline, diff.body_diff[0].candidate);
    }

    /// The ambiguity `json` alone cannot resolve: a body whose bytes ARE the
    /// JSON literal `null` records `json: null` too — indistinguishable from
    /// "did not parse" on that field. Projecting from bytes settles it: both
    /// sides parse `null` to `Value::Null` and agree.
    #[test]
    fn literal_null_body_stays_null_on_both_sides() {
        let diff = compare_bodies(captured_body(b"null"), b"null");
        assert!(diff.body_diff.is_empty(), "{:?}", diff.body_diff);
        assert_eq!(diff.baseline_body, Some(serde_json::Value::Null));
    }

    /// A bodyless response is the same false positive in miniature: the
    /// candidate's zero bytes project to `""`, so a baseline projected as
    /// `null` diffed against every 204 forever.
    #[test]
    fn empty_body_matches_an_empty_candidate() {
        let diff = compare_bodies(captured_nothing(), b"");
        assert!(diff.body_diff.is_empty(), "{:?}", diff.body_diff);
        assert_eq!(diff.baseline_body, Some(serde_json::json!("")));
    }

    /// Non-UTF-8 bytes have no `text` at all — only `raw_bytes`. The
    /// projection is lossy there, but symmetrically lossy: equal bytes still
    /// compare equal, and unequal bytes still diverge.
    #[test]
    fn non_utf8_body_projects_from_raw_bytes() {
        let raw: &[u8] = &[0xff, 0xfe, 0x00, 0x42];
        let recorded = captured_body(raw);
        assert!(
            recorded["text"].is_null() && recorded["json"].is_null(),
            "fixture must reproduce a capture with neither text nor json"
        );
        let same = compare_bodies(recorded.clone(), raw);
        assert!(same.body_diff.is_empty(), "{:?}", same.body_diff);
        let other = compare_bodies(recorded, &[0xff, 0xfe, 0x00, 0x43]);
        assert_eq!(other.body_diff.len(), 1, "{:?}", other.body_diff);
    }

    /// A JSON reply must survive the change untouched: it projects to the
    /// parsed object, and a real field change is still reported at its own
    /// path rather than as one opaque whole-body diff.
    #[test]
    fn json_bodies_still_diff_field_by_field() {
        let diff = compare_bodies(
            captured_body(br#"{"id":"pay_1","amount":100}"#),
            br#"{"id":"pay_1","amount":200}"#,
        );
        assert_eq!(diff.body_diff.len(), 1, "{:?}", diff.body_diff);
        assert_eq!(diff.body_diff[0].json_path, "$.amount");
    }

    /// A response the tape says nothing about stays `null`, and stays absent
    /// from the record: "not recorded" is not "recorded empty".
    #[test]
    fn absent_body_reports_no_baseline_body() {
        let req = serde_json::json!({ "method": "GET", "path": "/x" });
        let resp = serde_json::json!({ "status": 200 });
        let drv = reconstruct_driver_request(&[json_event(req, resp, 0)]).expect("reconstruct");
        assert_eq!(drv.baseline_response.body, None);
        let diff = compare_response(&drv, 200, &serde_json::json!({"a": 1}), &[]);
        assert_eq!(diff.baseline_body, None);
        assert_eq!(diff.body_diff.len(), 1);
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
