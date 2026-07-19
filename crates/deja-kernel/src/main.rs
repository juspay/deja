//! Replay-harness kernel — the workload player.
//!
//! Boot env:
//!   KERNEL_RECORDING_PATH=/artifacts/run.jsonl   (local file)
//!   KERNEL_TARGET_HOST=candidate                 (defaults to "candidate")
//!   KERNEL_TARGET_PORT=8080                      (defaults to 8080)
//!   KERNEL_HTTP_DIFF_SINK=/tmp/http-diffs.jsonl  (local file)
//!   KERNEL_BODY_ALLOWLIST=$.payment_id,$.created (comma-sep JSONPaths; default
//!                                                 empty = byte-exact gate)

use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use deja_kernel::{
    compare_response, group_by_correlation, reconstruct_driver_request, BoundaryEvent,
    DriverRequest, HttpDiff,
};

fn main() -> ExitCode {
    if let Err(err) = run() {
        eprintln!("deja-kernel: {err}");
        return ExitCode::FAILURE;
    }
    ExitCode::SUCCESS
}

fn run() -> Result<(), String> {
    let recording_path = std::env::var("KERNEL_RECORDING_PATH")
        .map_err(|_| "KERNEL_RECORDING_PATH unset".to_string())?;
    let target_host =
        std::env::var("KERNEL_TARGET_HOST").unwrap_or_else(|_| "candidate".to_string());
    let target_port: u16 = std::env::var("KERNEL_TARGET_PORT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(8080);
    let diff_sink_path = std::env::var("KERNEL_HTTP_DIFF_SINK")
        .map_err(|_| "KERNEL_HTTP_DIFF_SINK unset".to_string())?;

    let events = load_recording(&PathBuf::from(&recording_path))
        .map_err(|e| format!("load recording: {e}"))?;
    eprintln!(
        "deja-kernel: loaded {} events from {recording_path}",
        events.len()
    );

    let (by_corr, uncorrelated) = group_by_correlation(events);
    eprintln!(
        "deja-kernel: {} correlations, {} uncorrelated background events",
        by_corr.len(),
        uncorrelated.len()
    );

    let mut sink_file = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&diff_sink_path)
        .map_err(|e| format!("open diff sink: {e}"))?;

    // Byte-exact gate: an empty allowlist means every response field must
    // match. During Phase D bring-up, KERNEL_BODY_ALLOWLIST inventories the
    // non-deterministic fields (server-generated ids, timestamps) so the run
    // can pass while those generators are migrated onto deja boundaries.
    let allowlist_owned: Vec<String> = std::env::var("KERNEL_BODY_ALLOWLIST")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let allowlist: Vec<&str> = allowlist_owned.iter().map(String::as_str).collect();
    if allowlist.is_empty() {
        eprintln!("deja-kernel: body allowlist empty (byte-exact mode)");
    } else {
        eprintln!(
            "deja-kernel: body allowlist ({}): {}",
            allowlist.len(),
            allowlist.join(", ")
        );
    }

    // Drive correlations in RECORD ORDER (earliest global_sequence first), not
    // BTreeMap/UUID order. Side-effect calls carry correlation_id=null (no
    // correlation middleware in the router yet), so they all share one global
    // occurrence/sequence bucket in the lookup table; replaying requests out of
    // record order would misalign that numbering and resolve to wrong values.
    let mut ordered: Vec<(&String, &Vec<BoundaryEvent>)> = by_corr.iter().collect();
    ordered.sort_by_key(|(_, events)| {
        events
            .iter()
            .map(|e| e.global_sequence)
            .min()
            .unwrap_or(u64::MAX)
    });

    // Optional test-case subset (KERNEL_CORRELATION_FILTER, comma-separated
    // correlation ids): each request is an independent test case, so driving a
    // subset is sound — scoring scopes to the same filter. Logged loudly: a
    // silently narrowed drive-list would read as full coverage.
    if let Some(filter) = parse_correlation_filter(std::env::var("KERNEL_CORRELATION_FILTER").ok())
    {
        let before = ordered.len();
        ordered.retain(|(cid, _)| filter.contains(*cid));
        eprintln!(
            "deja-kernel: correlation filter ({} id(s)): driving {} of {before} correlations ({} filtered out)",
            filter.len(),
            ordered.len(),
            before - ordered.len(),
        );
        for want in &filter {
            if !by_corr.contains_key(want) {
                eprintln!("deja-kernel: WARNING filter correlation {want} is not in the recording");
            }
        }
    }

    // Concurrency knob (default 1 = today's serial drive). >1 drives that many
    // correlations at once, so the replay router's per-correlation isolation
    // (schema-per-correlation `search_path` routing, redis `{corr}:` namespace,
    // correlation propagation) is actually exercised under contention on the
    // shared bb8 pool — a single-correlation-at-a-time drive can't surface a
    // cross-correlation bleed. Each request already carries its own
    // `x-request-id`, so the correlations stay independent test cases; only the
    // uncorrelated (null-correlation) background events share a bucket and may
    // interleave, which is itself honest signal about concurrency safety.
    let concurrency = std::env::var("KERNEL_DRIVE_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n >= 1)
        .unwrap_or(1);

    use std::sync::atomic::{AtomicUsize, Ordering};
    let driven = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let sink = std::sync::Mutex::new(&mut sink_file);
    let write_err: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

    // Drive ONE correlation: reconstruct its request, stamp the recorded
    // correlation as `x-request-id` (the replay router runs IdReuse::UseIncoming,
    // so its time/id/db replay lookups key off the SAME correlation that was
    // recorded), hit the candidate, and record the diff. Shared verbatim by the
    // serial and concurrent paths; sink writes serialize under a mutex.
    let drive_one = |cid: &String, events: &Vec<BoundaryEvent>| {
        match reconstruct_driver_request(events) {
            // Skip liveness probes — harness noise, not workload.
            Some(driver) if driver.path == "/health" => {
                skipped.fetch_add(1, Ordering::Relaxed);
            }
            Some(mut driver) => {
                driver
                    .headers
                    .retain(|(k, _)| !k.eq_ignore_ascii_case("x-request-id"));
                driver
                    .headers
                    .push(("x-request-id".to_string(), cid.clone()));
                let diff = drive(&target_host, target_port, &driver, &allowlist);
                {
                    let mut file = sink.lock().unwrap_or_else(|p| p.into_inner());
                    if let Err(e) = write_diff(&mut *file, &diff) {
                        let mut slot = write_err.lock().unwrap_or_else(|p| p.into_inner());
                        if slot.is_none() {
                            *slot = Some(format!("write diff: {e}"));
                        }
                    }
                }
                driven.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "deja-kernel: drove {cid} → {}{} (status {} vs {}, body diffs {})",
                    driver.method,
                    driver.path,
                    diff.status_candidate,
                    diff.status_baseline,
                    diff.body_diff.len(),
                );
            }
            None => {
                skipped.fetch_add(1, Ordering::Relaxed);
            }
        }
    };

    if concurrency <= 1 {
        for &(cid, events) in &ordered {
            drive_one(cid, events);
        }
    } else {
        eprintln!(
            "deja-kernel: CONCURRENT DRIVE — {} correlations across {} threads (KERNEL_DRIVE_CONCURRENCY={concurrency})",
            ordered.len(),
            concurrency,
        );
        let cursor = AtomicUsize::new(0);
        let cursor_ref = &cursor;
        let ordered_ref = &ordered;
        let drive_one_ref = &drive_one;
        std::thread::scope(|scope| {
            for _ in 0..concurrency {
                scope.spawn(move || loop {
                    let i = cursor_ref.fetch_add(1, Ordering::Relaxed);
                    if i >= ordered_ref.len() {
                        break;
                    }
                    let (cid, events) = ordered_ref[i];
                    drive_one_ref(cid, events);
                });
            }
        });
    }

    if let Some(e) = write_err.lock().unwrap_or_else(|p| p.into_inner()).take() {
        return Err(e);
    }
    let driven = driven.load(Ordering::Relaxed);
    let skipped = skipped.load(Ordering::Relaxed);
    eprintln!("deja-kernel: complete (driven {driven}, skipped {skipped})");
    Ok(())
}

fn load_recording(path: &PathBuf) -> std::io::Result<Vec<BoundaryEvent>> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut events = Vec::new();
    let mut graph_nodes = 0usize;
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        // Tagged one-stream tape: `record_kind` routes each line. Boundary
        // events deserialize with the tag beside their flat fields (serde
        // ignores the extra key); graph nodes ride the same stream but are
        // not driveable, so they skip silently.
        let value = match serde_json::from_str::<serde_json::Value>(&line) {
            Ok(value) => value,
            Err(err) => {
                eprintln!("deja-kernel: skipping unparseable line: {err}");
                continue;
            }
        };
        match value.get("record_kind").and_then(|k| k.as_str()) {
            Some("boundary_event") => match serde_json::from_value::<BoundaryEvent>(value) {
                Ok(ev) => events.push(ev),
                Err(err) => {
                    eprintln!("deja-kernel: skipping unparseable boundary_event: {err}");
                }
            },
            Some("graph_node") => graph_nodes += 1,
            other => {
                eprintln!("deja-kernel: skipping line with record_kind {other:?}");
            }
        }
    }
    if graph_nodes > 0 {
        eprintln!("deja-kernel: skipped {graph_nodes} graph_node record(s) riding the tape");
    }
    Ok(events)
}

fn drive(
    target_host: &str,
    target_port: u16,
    driver: &DriverRequest,
    allowlist: &[&str],
) -> HttpDiff {
    match drive_inner(target_host, target_port, driver) {
        Ok((status, body)) => {
            let body_text = String::from_utf8_lossy(&body).into_owned();
            let body_json: serde_json::Value =
                serde_json::from_str(&body_text).unwrap_or(serde_json::Value::String(body_text));
            compare_response(driver, status, &body_json, allowlist)
        }
        Err(err) => {
            let body = serde_json::json!({ "error": err });
            compare_response(driver, 0, &body, allowlist)
        }
    }
}

/// Minimal HTTP/1.1 client over `TcpStream`. The kernel only talks plain
/// HTTP to a known target, so we avoid pulling reqwest/url/idna into the
/// dependency graph (icu 2.2 requires rustc 1.86; this workspace pins 1.85).
fn drive_inner(host: &str, port: u16, driver: &DriverRequest) -> Result<(u16, Vec<u8>), String> {
    let addr = format!("{host}:{port}");
    let mut stream = TcpStream::connect(&addr).map_err(|e| format!("connect {addr}: {e}"))?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| format!("set_read_timeout: {e}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .map_err(|e| format!("set_write_timeout: {e}"))?;

    let mut request_line = format!("{} {}", driver.method, driver.path);
    if let Some(q) = &driver.query {
        request_line.push('?');
        request_line.push_str(q);
    }
    request_line.push_str(" HTTP/1.1\r\n");

    let mut head = request_line;
    head.push_str(&format!("Host: {host}\r\n"));
    head.push_str("Connection: close\r\n");
    let mut have_content_length = false;
    for (k, v) in &driver.headers {
        if k.eq_ignore_ascii_case("host") || k.eq_ignore_ascii_case("connection") {
            continue;
        }
        if k.eq_ignore_ascii_case("content-length") {
            have_content_length = true;
        }
        head.push_str(&format!("{k}: {v}\r\n"));
    }
    if let Some(body) = &driver.body {
        if !have_content_length {
            head.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
    }
    head.push_str("\r\n");

    stream
        .write_all(head.as_bytes())
        .map_err(|e| format!("write head: {e}"))?;
    if let Some(body) = &driver.body {
        stream
            .write_all(body)
            .map_err(|e| format!("write body: {e}"))?;
    }

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|e| format!("read response: {e}"))?;

    parse_http_response(&response)
}

/// Parse a minimal HTTP/1.1 response. Returns (status_code, body_bytes).
/// Does NOT handle chunked transfer encoding — Hyperswitch responses are
/// typically content-length-delimited; if chunked support becomes
/// necessary, this is the place to add it.
fn parse_http_response(buf: &[u8]) -> Result<(u16, Vec<u8>), String> {
    // Find end of headers.
    let separator = b"\r\n\r\n";
    let header_end = buf
        .windows(separator.len())
        .position(|w| w == separator)
        .ok_or_else(|| "no header/body separator".to_string())?;
    let header_block = &buf[..header_end];
    let body = &buf[header_end + separator.len()..];

    let header_text = std::str::from_utf8(header_block).map_err(|e| format!("header utf8: {e}"))?;
    let first_line = header_text
        .lines()
        .next()
        .ok_or_else(|| "empty header block".to_string())?;
    // "HTTP/1.1 200 OK"
    let mut parts = first_line.splitn(3, ' ');
    parts.next(); // version
    let status_str = parts.next().ok_or_else(|| "no status code".to_string())?;
    let status: u16 = status_str
        .parse()
        .map_err(|e| format!("status parse: {e}"))?;

    Ok((status, body.to_vec()))
}

fn write_diff(file: &mut fs::File, diff: &HttpDiff) -> std::io::Result<()> {
    let line = serde_json::to_string(diff)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    file.write_all(line.as_bytes())?;
    file.write_all(b"\n")?;
    Ok(())
}

/// Parse the comma-separated correlation filter; `None` when unset/blank so an
/// empty env var never becomes "drive nothing".
fn parse_correlation_filter(raw: Option<String>) -> Option<std::collections::BTreeSet<String>> {
    raw.map(|raw| {
        raw.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<std::collections::BTreeSet<_>>()
    })
    .filter(|set| !set.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn correlation_filter_blank_means_no_filter() {
        assert_eq!(parse_correlation_filter(None), None);
        assert_eq!(parse_correlation_filter(Some("".into())), None);
        assert_eq!(parse_correlation_filter(Some(" , ,".into())), None);
        let set = parse_correlation_filter(Some("c-2, c-1 ,c-2".into())).expect("set");
        assert_eq!(set.into_iter().collect::<Vec<_>>(), ["c-1", "c-2"]);
    }

    #[test]
    fn parse_http_response_extracts_status_and_body() {
        let raw = b"HTTP/1.1 201 Created\r\nContent-Type: application/json\r\n\r\n{\"id\":\"x\"}";
        let (status, body) = parse_http_response(raw).expect("parse");
        assert_eq!(status, 201);
        assert_eq!(body, b"{\"id\":\"x\"}");
    }

    #[test]
    fn parse_http_response_handles_empty_body() {
        let raw = b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n";
        let (status, body) = parse_http_response(raw).expect("parse");
        assert_eq!(status, 204);
        assert!(body.is_empty());
    }
}
