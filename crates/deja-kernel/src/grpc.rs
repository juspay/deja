//! The gRPC drive path: reconstruct a recorded gRPC ingress event
//! (`boundary: "grpc_incoming"`, `role: "ingress"`) into a drivable request,
//! re-drive it over HTTP/2, and report the same [`HttpDiff`] row shape the
//! HTTP driver writes — so the scorer, dashboard, and diff sink stay
//! system-agnostic.
//!
//! Wire-format facts this module is built on (the recorder is a tower layer
//! sitting at the http level of a tonic server, e.g. hyperswitch-prism):
//!   - `request.rpc` is the full path (`/types.PaymentService/Authorize`),
//!     `request.metadata` the sorted `[name, value]` pairs, and
//!     `request.request` either `{raw_b64}` — the request body EXACTLY as read
//!     off the transport, i.e. length-prefixed gRPC frames — or `{decoded}`,
//!     proto3-JSON of the request message, which needs descriptors
//!     (`KERNEL_DESCRIPTOR_SET`) to re-encode.
//!   - the response finalizer tees the response body frames into
//!     `response.response_body` (the same capture object the HTTP recorder
//!     writes), so the baseline bytes are the recorded framed response; a
//!     status-aware recorder also stamps `response.grpc_status` (the outcome
//!     baseline — `is_error` is the fallback).
//!   - grpc-status arrives in trailers, or in the response HEADERS for a
//!     trailers-only response.
//!
//! Status semantics mirror the HTTP driver's symmetric outcome contract:
//! "the rpc completed" is 200 on both sides (recorded `is_error: false`,
//! candidate grpc-status 0), "it failed" is 500. Body comparison is
//! field-level proto3-JSON when a descriptor set is available, byte-exact
//! otherwise — a byte mismatch without descriptors is reported as one opaque
//! root diff, never silently dropped.

use std::time::Duration;

use base64::Engine as _;

use crate::{captured_body_bytes, diff_json, BoundaryEvent, HttpDiff, JsonFieldDiff};

/// Reconstructed driver-side gRPC request, derived from a recorded ingress
/// event whose request carries an `rpc` (not `method`/`path`).
#[derive(Debug, Clone)]
pub struct GrpcDriverRequest {
    pub correlation_id: String,
    pub request_sequence: u64,
    /// Full rpc path, `/package.Service/Method`.
    pub rpc: String,
    /// Recorded metadata as flat (name, value) pairs; pseudo/framing headers
    /// are filtered at send time, mirroring the HTTP driver's KERNEL_OWNED set.
    pub metadata: Vec<(String, String)>,
    /// The framed request body bytes to send (length-prefixed gRPC frames).
    pub framed_request: Vec<u8>,
    /// Whether the recorded rpc failed (`is_error` on the ingress event).
    pub baseline_is_error: bool,
    /// The recorded framed response bytes, when the tape captured them.
    pub baseline_frames: Option<Vec<u8>>,
}

/// What the candidate answered: the grpc-status (trailers, or headers on a
/// trailers-only response) and the collected framed response body.
#[derive(Debug, Clone)]
pub struct GrpcOutcome {
    pub grpc_status: u64,
    pub framed_body: Vec<u8>,
}

/// Extract the first gRPC-shaped ingress event from a correlation group and
/// reconstruct a driveable request. `None` when the correlation has no gRPC
/// ingress, or when the request was recorded decoded-only and no descriptor
/// pool is available to re-encode it (the caller reports that skip).
pub fn reconstruct_grpc_request(
    events: &[BoundaryEvent],
    pool: Option<&prost_reflect::DescriptorPool>,
) -> Option<GrpcDriverRequest> {
    let event = events
        .iter()
        .find(|e| e.is_ingress() && e.request.get("rpc").is_some())?;
    let req = &event.request;
    let rpc = req.get("rpc")?.as_str()?.to_string();
    let metadata = metadata_pairs(req.get("metadata"));

    let message = req.get("request")?;
    let framed_request = if let Some(raw) = message.get("raw_b64").and_then(|v| v.as_str()) {
        // The recorder captured the transport bytes verbatim — already framed.
        base64::engine::general_purpose::STANDARD.decode(raw).ok()?
    } else {
        // Decoded-only recording: re-encode the proto3-JSON message through the
        // descriptor pool and frame it ourselves.
        let decoded = message.get("decoded")?;
        let input = method_descriptor(pool?, &rpc)?.input();
        let encoded = encode_proto3_json(&input, decoded)?;
        frame(&encoded)
    };

    let baseline_frames = event
        .response
        .get("response_body")
        .or_else(|| event.response.get("body"))
        .and_then(captured_body_bytes);

    // Outcome baseline, most-specific first: a recorded `grpc_status` on the
    // response (what a status-aware recorder writes — non-zero = the rpc
    // failed), else the event's `is_error`. A recorder that writes neither for
    // a failed rpc records a false 200 baseline; that is a recorder gap the
    // diff will then surface as a status mismatch, not something to paper over.
    let baseline_is_error = match event.response.get("grpc_status").and_then(|v| v.as_u64()) {
        Some(status) => status != 0,
        None => event.is_error,
    };

    Some(GrpcDriverRequest {
        correlation_id: event.correlation_id.clone()?,
        request_sequence: event.request_sequence,
        rpc,
        metadata,
        framed_request,
        baseline_is_error,
        baseline_frames,
    })
}

/// Whether a correlation group's ingress is gRPC-shaped at all — used by the
/// drive loop to tell "no ingress" apart from "gRPC ingress that could not be
/// reconstructed" (undecodable without descriptors) in its skip accounting.
pub fn has_grpc_ingress(events: &[BoundaryEvent]) -> bool {
    events
        .iter()
        .any(|e| e.is_ingress() && e.request.get("rpc").is_some())
}

/// Drive one reconstructed gRPC request against the candidate. Blocking: each
/// call runs its own current-thread tokio runtime, so the kernel's existing
/// thread-pool concurrency model is untouched.
pub fn drive_grpc(
    host: &str,
    port: u16,
    driver: &GrpcDriverRequest,
) -> Result<GrpcOutcome, String> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("tokio runtime: {e}"))?;
    runtime.block_on(async {
        tokio::time::timeout(
            Duration::from_secs(30),
            drive_grpc_async(host, port, driver),
        )
        .await
        .map_err(|_| "grpc drive timed out after 30s".to_string())?
    })
}

async fn drive_grpc_async(
    host: &str,
    port: u16,
    driver: &GrpcDriverRequest,
) -> Result<GrpcOutcome, String> {
    let tcp = tokio::net::TcpStream::connect((host, port))
        .await
        .map_err(|e| format!("connect {host}:{port}: {e}"))?;
    let (client, connection) = h2::client::handshake(tcp)
        .await
        .map_err(|e| format!("h2 handshake: {e}"))?;
    tokio::spawn(async move {
        // The connection task owns the socket; an error here surfaces to the
        // request future as a stream error, which we already report.
        let _ = connection.await;
    });
    let mut client = client.ready().await.map_err(|e| format!("h2 ready: {e}"))?;

    let mut builder = http::Request::builder()
        .method("POST")
        .uri(format!("http://{host}:{port}{}", driver.rpc))
        .header("content-type", "application/grpc")
        .header("te", "trailers")
        .header("x-request-id", driver.correlation_id.as_str());
    for (name, value) in &driver.metadata {
        if kernel_owned_metadata(name) {
            continue;
        }
        builder = builder.header(name.as_str(), value.as_str());
    }
    let request = builder
        .body(())
        .map_err(|e| format!("build request: {e}"))?;

    let (response, mut send_stream) = client
        .send_request(request, false)
        .map_err(|e| format!("send_request: {e}"))?;
    send_stream
        .send_data(bytes::Bytes::from(driver.framed_request.clone()), true)
        .map_err(|e| format!("send_data: {e}"))?;

    let response = response.await.map_err(|e| format!("response: {e}"))?;
    // Trailers-only responses (immediate errors) carry grpc-status in HEADERS.
    let mut grpc_status = header_grpc_status(response.headers());
    let mut body = response.into_body();
    let mut collected = Vec::new();
    while let Some(chunk) = body.data().await {
        let chunk = chunk.map_err(|e| format!("read body: {e}"))?;
        let _ = body.flow_control().release_capacity(chunk.len());
        collected.extend_from_slice(&chunk);
    }
    if let Some(trailers) = body
        .trailers()
        .await
        .map_err(|e| format!("read trailers: {e}"))?
    {
        if let Some(status) = header_grpc_status(&trailers) {
            grpc_status = Some(status);
        }
    }
    Ok(GrpcOutcome {
        // No grpc-status anywhere is a protocol violation; report it as
        // UNKNOWN (2) rather than inventing success.
        grpc_status: grpc_status.unwrap_or(2),
        framed_body: collected,
    })
}

/// Compare the candidate's answer (or transport failure) against the recorded
/// baseline and shape the result as the one [`HttpDiff`] row the sink and
/// scorer already understand.
pub fn compare_grpc_response(
    driver: &GrpcDriverRequest,
    outcome: &Result<GrpcOutcome, String>,
    pool: Option<&prost_reflect::DescriptorPool>,
    allowlist: &[&str],
) -> HttpDiff {
    let status_baseline: u16 = if driver.baseline_is_error { 500 } else { 200 };
    let outcome = match outcome {
        Err(transport) => {
            return HttpDiff {
                correlation_id: driver.correlation_id.clone(),
                request_sequence: driver.request_sequence,
                request_path: driver.rpc.clone(),
                status_baseline,
                status_candidate: 0,
                status_match: false,
                body_diff: Vec::new(),
                baseline_body: None,
                candidate_body: None,
                transport_error: Some(transport.clone()),
            }
        }
        Ok(outcome) => outcome,
    };
    let status_candidate: u16 = if outcome.grpc_status == 0 { 200 } else { 500 };

    let output = pool.and_then(|pool| method_descriptor(pool, &driver.rpc).map(|m| m.output()));
    let baseline_body = match (&driver.baseline_frames, &output) {
        (Some(frames), Some(output)) => decode_framed_proto3_json(output, frames),
        _ => None,
    };
    let candidate_body = output
        .as_ref()
        .and_then(|output| decode_framed_proto3_json(output, &outcome.framed_body));

    let body_diff = match (&baseline_body, &candidate_body) {
        (Some(baseline), Some(candidate)) => diff_json(baseline, candidate, "$", allowlist),
        // No descriptors (or an undecodable side): byte-exact gate. Equal bytes
        // (or no recorded baseline to compare) pass; a mismatch is one opaque
        // root diff — visible, never silently absorbed.
        _ => match &driver.baseline_frames {
            Some(frames) if *frames != outcome.framed_body => vec![JsonFieldDiff {
                json_path: "$".to_owned(),
                baseline: serde_json::json!({
                    "raw_b64": base64::engine::general_purpose::STANDARD.encode(frames),
                }),
                candidate: serde_json::json!({
                    "raw_b64":
                        base64::engine::general_purpose::STANDARD.encode(&outcome.framed_body),
                }),
            }],
            _ => Vec::new(),
        },
    };

    HttpDiff {
        correlation_id: driver.correlation_id.clone(),
        request_sequence: driver.request_sequence,
        request_path: driver.rpc.clone(),
        status_baseline,
        status_candidate,
        status_match: status_baseline == status_candidate,
        body_diff,
        baseline_body,
        candidate_body,
        transport_error: None,
    }
}

/// Load the optional descriptor pool named by `KERNEL_DESCRIPTOR_SET` — a
/// compiled `FileDescriptorSet` (e.g. the server's own build-time
/// `FILE_DESCRIPTOR_SET` dumped to a file). Absent: gRPC drives still run on
/// raw recorded bytes; response comparison degrades to byte-exact.
pub fn descriptor_pool_from_env() -> Option<prost_reflect::DescriptorPool> {
    let path = std::env::var("KERNEL_DESCRIPTOR_SET").ok()?;
    match std::fs::read(&path)
        .map_err(|e| e.to_string())
        .and_then(|bytes| {
            prost_reflect::DescriptorPool::decode(bytes.as_slice()).map_err(|e| e.to_string())
        }) {
        Ok(pool) => {
            eprintln!("deja-kernel: descriptor set loaded from {path}");
            Some(pool)
        }
        Err(err) => {
            eprintln!("deja-kernel: KERNEL_DESCRIPTOR_SET={path} unusable ({err}) — gRPC body comparison degrades to byte-exact");
            None
        }
    }
}

/// `/package.Service/Method` → the pool's method descriptor.
fn method_descriptor(
    pool: &prost_reflect::DescriptorPool,
    rpc: &str,
) -> Option<prost_reflect::MethodDescriptor> {
    let (service, method) = rpc.trim_start_matches('/').split_once('/')?;
    pool.services()
        .find(|s| s.full_name() == service)?
        .methods()
        .find(|m| m.name() == method)
}

/// Encode a proto3-JSON message value through its descriptor.
fn encode_proto3_json(
    message: &prost_reflect::MessageDescriptor,
    json: &serde_json::Value,
) -> Option<Vec<u8>> {
    use prost::Message as _;
    let dynamic = prost_reflect::DynamicMessage::deserialize(message.clone(), json).ok()?;
    Some(dynamic.encode_to_vec())
}

/// Decode the FIRST gRPC frame of `frames` to proto3-JSON. Unary rpcs carry
/// exactly one message; a malformed or empty stream decodes to `None` and the
/// caller falls back to the byte-exact gate.
fn decode_framed_proto3_json(
    message: &prost_reflect::MessageDescriptor,
    frames: &[u8],
) -> Option<serde_json::Value> {
    let payload = unframe_first(frames)?;
    let dynamic = prost_reflect::DynamicMessage::decode(message.clone(), payload).ok()?;
    serde_json::to_value(&dynamic).ok()
}

/// Split off the first length-prefixed gRPC frame's payload. Tolerates a bare
/// unframed message (no 5-byte prefix) for fixtures.
fn unframe_first(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() >= 5 && bytes[0] == 0 {
        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
        return bytes.get(5..5 + len);
    }
    if bytes.is_empty() {
        return None;
    }
    Some(bytes)
}

/// Wrap an encoded message in the length-prefixed gRPC frame.
fn frame(message: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(message.len() + 5);
    framed.push(0);
    framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
    framed.extend_from_slice(message);
    framed
}

/// `grpc-status` out of a header/trailer map.
fn header_grpc_status(headers: &http::HeaderMap) -> Option<u64> {
    headers
        .get("grpc-status")?
        .to_str()
        .ok()?
        .parse::<u64>()
        .ok()
}

/// Recorded metadata the kernel owns on the wire and must not replay verbatim:
/// pseudo-headers, connection/framing headers, the content type it sets
/// itself, a recorded deadline that has long expired, and the correlation
/// header it re-stamps. The gRPC twin of `main.rs`'s KERNEL_OWNED list.
fn kernel_owned_metadata(name: &str) -> bool {
    name.starts_with(':')
        || matches!(
            name,
            "host"
                | "te"
                | "content-type"
                | "content-length"
                | "connection"
                | "keep-alive"
                | "transfer-encoding"
                | "upgrade"
                | "grpc-timeout"
                | "x-request-id"
        )
}

/// The recorded metadata shape: sorted `[[name, value], ...]` pairs (the
/// recorder sorts because header-map iteration order is unstable).
fn metadata_pairs(value: Option<&serde_json::Value>) -> Vec<(String, String)> {
    value
        .and_then(|v| v.as_array())
        .map(|pairs| {
            pairs
                .iter()
                .filter_map(|pair| {
                    let name = pair.get(0)?.as_str()?.to_owned();
                    let value = pair.get(1)?.as_str()?.to_owned();
                    Some((name, value))
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn grpc_ingress_event(raw_b64: Option<&str>) -> BoundaryEvent {
        let request = match raw_b64 {
            Some(raw) => serde_json::json!({ "raw_b64": raw, "undecoded": true }),
            None => serde_json::json!({ "decoded": {"amount": 1} }),
        };
        let req = serde_json::json!({
            "rpc": "/types.PaymentService/Authorize",
            "authority": "localhost:8000",
            "metadata": [["content-type", "application/grpc"],
                         ["x-connector", "stripe"],
                         ["x-request-id", "corr-1"]],
            "request": request,
        });
        let event: BoundaryEvent = serde_json::from_value(serde_json::json!({
            "global_sequence": 0,
            "request_sequence": 0,
            "correlation_id": "corr-1",
            "timestamp_ns": 1,
            "boundary": "grpc_incoming",
            "role": "ingress",
            "trait_name": "GrpcServer",
            "method_name": "call",
            "call_file": "layer.rs",
            "call_line": 1,
            "call_column": 1,
            "request": req,
            "args": req,
            "response": {"response_body": {"captured": true, "raw_bytes": [0,0,0,0,0]}},
            "result": {},
            "is_error": false,
            "duration_us": 10,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute",
        }))
        .expect("fixture event parses");
        event
    }

    #[test]
    fn grpc_ingress_reconstructs_from_raw_bytes_without_descriptors() {
        // b64 of a framed 3-byte message: 00 00000003 010203
        let framed = [0u8, 0, 0, 0, 3, 1, 2, 3];
        let raw = base64::engine::general_purpose::STANDARD.encode(framed);
        let events = vec![grpc_ingress_event(Some(&raw))];
        let driver = reconstruct_grpc_request(&events, None).expect("reconstructs");
        assert_eq!(driver.rpc, "/types.PaymentService/Authorize");
        assert_eq!(driver.framed_request, framed);
        assert_eq!(driver.correlation_id, "corr-1");
        assert!(!driver.baseline_is_error);
        assert_eq!(driver.baseline_frames, Some(vec![0, 0, 0, 0, 0]));
        // The recorded metadata rides along; the kernel-owned filter is
        // applied at send time, not here.
        assert!(driver
            .metadata
            .iter()
            .any(|(name, value)| name == "x-connector" && value == "stripe"));
    }

    #[test]
    fn decoded_only_grpc_ingress_needs_descriptors() {
        let events = vec![grpc_ingress_event(None)];
        assert!(has_grpc_ingress(&events));
        // No pool → cannot re-encode → None (the drive loop reports the skip).
        assert!(reconstruct_grpc_request(&events, None).is_none());
    }

    #[test]
    fn http_ingress_is_not_grpc_shaped() {
        let events: Vec<BoundaryEvent> = Vec::new();
        assert!(!has_grpc_ingress(&events));
    }

    #[test]
    fn byte_exact_gate_reports_opaque_root_diff_without_descriptors() {
        let framed = [0u8, 0, 0, 0, 1, 7];
        let raw = base64::engine::general_purpose::STANDARD.encode(framed);
        let events = vec![grpc_ingress_event(Some(&raw))];
        let driver = reconstruct_grpc_request(&events, None).expect("reconstructs");
        // Candidate answered OK but with different bytes than the baseline.
        let outcome = Ok(GrpcOutcome {
            grpc_status: 0,
            framed_body: vec![9, 9],
        });
        let diff = compare_grpc_response(&driver, &outcome, None, &[]);
        assert_eq!(diff.status_baseline, 200);
        assert_eq!(diff.status_candidate, 200);
        assert!(diff.status_match);
        assert_eq!(diff.body_diff.len(), 1);
        assert_eq!(diff.body_diff[0].json_path, "$");
        // Matching bytes produce a clean row.
        let outcome = Ok(GrpcOutcome {
            grpc_status: 0,
            framed_body: vec![0, 0, 0, 0, 0],
        });
        let diff = compare_grpc_response(&driver, &outcome, None, &[]);
        assert!(diff.body_diff.is_empty());
    }

    #[test]
    fn recorded_grpc_status_outranks_is_error_as_the_outcome_baseline() {
        let framed = [0u8, 0, 0, 0, 1, 7];
        let raw = base64::engine::general_purpose::STANDARD.encode(framed);
        let mut event = grpc_ingress_event(Some(&raw));
        // The recorder saw the rpc FAIL (grpc-status 13) even though the
        // ingress layer's own is_error flag stayed false.
        event.response["grpc_status"] = serde_json::json!(13);
        let driver =
            reconstruct_grpc_request(std::slice::from_ref(&event), None).expect("reconstructs");
        assert!(driver.baseline_is_error, "grpc_status 13 = failed baseline");
        // And a failing candidate then MATCHES that baseline (500 vs 500).
        let outcome = Ok(GrpcOutcome {
            grpc_status: 13,
            framed_body: Vec::new(),
        });
        let diff = compare_grpc_response(&driver, &outcome, None, &[]);
        assert_eq!((diff.status_baseline, diff.status_candidate), (500, 500));
        assert!(diff.status_match);
    }

    #[test]
    fn transport_failure_is_reported_not_dropped() {
        let framed = [0u8, 0, 0, 0, 1, 7];
        let raw = base64::engine::general_purpose::STANDARD.encode(framed);
        let events = vec![grpc_ingress_event(Some(&raw))];
        let driver = reconstruct_grpc_request(&events, None).expect("reconstructs");
        let outcome = Err("connect refused".to_owned());
        let diff = compare_grpc_response(&driver, &outcome, None, &[]);
        assert_eq!(diff.status_candidate, 0);
        assert!(!diff.status_match);
        assert_eq!(diff.transport_error.as_deref(), Some("connect refused"));
    }

    #[test]
    fn kernel_owned_metadata_filters_framing_but_keeps_business_headers() {
        assert!(kernel_owned_metadata(":authority"));
        assert!(kernel_owned_metadata("content-type"));
        assert!(kernel_owned_metadata("grpc-timeout"));
        assert!(kernel_owned_metadata("x-request-id"));
        assert!(!kernel_owned_metadata("x-connector"));
        assert!(!kernel_owned_metadata("x-merchant-id"));
        assert!(!kernel_owned_metadata("grpc-encoding"));
    }
}
