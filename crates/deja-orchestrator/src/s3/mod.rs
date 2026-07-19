//! Recording ingest: sealed sessions out of S3 (Phase 2.1 + 2.3).
//!
//! The durable form of a recording is the compacted session
//! (`sessions/v1/{id}/` — data parts + correlations index + manifest seal,
//! see `deja-compactor`). Pulling a recording means:
//!
//! 1. read the manifest; if the session is unsealed, compact it first
//!    (the record lifecycle's quiesce wait has already settled the landing)
//! 2. stream the data parts (full envelope lines, already deduped + sorted)
//! 3. unwrap envelopes — raw event bytes preserved via `RawValue`, no
//!    reserialization — and re-verify dedup/order by
//!    `(recording_run_id, global_sequence)` while materializing the
//!    canonical `events.jsonl` the kernel + renderer read
//!
//! (`KeyStamper` occurrences are correlation/address/args-scoped, so
//! dedup+sort cannot perturb lookup stamping.)

use std::io::Write;
use std::path::Path;

pub use deja_compactor::S3Config;

/// What `pull_recording` reports back (persisted next to the events file,
/// registered as a run artifact, folded into the catalog row).
#[derive(Debug, Clone, serde::Serialize)]
pub struct IngestReport {
    pub prefix: String,
    pub landing_objects: usize,
    pub lines_in: usize,
    pub duplicates_dropped: usize,
    pub events_out: usize,
    pub correlations: usize,
    pub sealed: bool,
}

/// Minimal probe of an event for identity (dedup/sort key) — everything else
/// stays raw.
#[derive(serde::Deserialize)]
struct EventProbe {
    #[serde(default)]
    recording_run_id: Option<String>,
    #[serde(default)]
    global_sequence: u64,
}

/// Envelope shape (v2): the payload is kept as raw bytes.
#[derive(serde::Deserialize)]
struct EnvelopeProbe<'a> {
    #[serde(default)]
    artifact_type: Option<String>,
    #[serde(borrow)]
    event: Option<&'a serde_json::value::RawValue>,
}

/// Light probe for session grouping during an arbitrary-prefix scan — only
/// the envelope's capture identity, everything else untouched.
#[derive(serde::Deserialize)]
struct SessionProbe {
    #[serde(default)]
    capture: Option<CaptureProbe>,
}

#[derive(serde::Deserialize)]
struct CaptureProbe {
    #[serde(default)]
    session_id: Option<String>,
}

/// Per-event correlation probe (for the ingest report's correlation count —
/// the session layout gets this from the manifest; a raw prefix has none).
#[derive(serde::Deserialize)]
struct CorrelationProbe {
    #[serde(default)]
    correlation_id: Option<String>,
}

/// Count landing objects for a recording (the "did Vector land anything yet /
/// has the flush settled" poll the lifecycle runs before compacting).
pub fn count_session_objects(cfg: &S3Config, recording_id: &str) -> Result<usize, String> {
    deja_compactor::count_landing_objects(cfg, recording_id)
}

/// Pull a session recording into `dest` (the canonical
/// `{root}/recordings/{id}/events.jsonl` slot), compacting first if the
/// session isn't sealed yet. Returns the ingest report plus the manifest.
pub fn pull_recording(
    cfg: &S3Config,
    recording_id: &str,
    dest: &Path,
) -> Result<(IngestReport, deja_compactor::SessionManifest), String> {
    let manifest = match deja_compactor::read_manifest(cfg, recording_id)? {
        Some(m) => m,
        None => deja_compactor::compact_session(cfg, recording_id)?,
    };
    let lines = deja_compactor::read_session_lines(cfg, &manifest)?;
    let chunk = lines.join("\n").into_bytes();
    let (events, lines_in, duplicates) = collate(&[chunk]);

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(dest).map_err(|e| format!("create {}: {e}", dest.display()))?,
    );
    for (_, _, line) in &events {
        out.write_all(line.as_bytes())
            .and_then(|_| out.write_all(b"\n"))
            .map_err(|e| format!("write {}: {e}", dest.display()))?;
    }
    out.flush().map_err(|e| format!("flush: {e}"))?;

    let report = IngestReport {
        prefix: deja_compactor::layout::session_root(recording_id),
        landing_objects: manifest.counts.landing_objects,
        lines_in,
        duplicates_dropped: manifest.counts.duplicates_dropped + duplicates,
        events_out: events.len(),
        correlations: manifest.counts.correlations,
        sealed: true,
    };
    Ok((report, manifest))
}

/// Sessions discovered in a prefix scan: `(session_id, envelope line count)`,
/// most lines first.
pub type SessionsSeen = Vec<(String, usize)>;

/// Pull a recording out of an ARBITRARY S3 prefix in the DEPLOYED aggregator
/// layout — date-partitioned objects (e.g. `%Y/%m/%d/…log.gz`, gzip NDJSON)
/// whose lines are full `deja.artifact_record/v2` envelopes with sessions
/// INTERLEAVED (the aggregator pipe has no transforms, so envelope content is
/// identical to the session layout; only key scheme + compression differ).
///
/// The recording is identified by envelope CONTENT (`capture.session_id`),
/// not key layout: scan every object under `prefix`, group lines by session,
/// then materialize the chosen session through the same collate (unwrap,
/// dedup, sort) as the session-layout path.
///
/// `session`: `Some(id)` filters to that session; `None` auto-resolves when
/// the scan finds exactly ONE session and errors with the discovered list
/// otherwise. `dest_for` maps the RESOLVED session id to the events.jsonl
/// destination (the id isn't known until the scan when auto-resolving).
/// Returns the report, the resolved session id, and everything the scan saw
/// (surfaced for a re-submit with an explicit session).
pub fn pull_recording_from_prefix(
    cfg: &S3Config,
    prefix: &str,
    session: Option<&str>,
    dest_for: impl Fn(&str) -> std::path::PathBuf,
) -> Result<(IngestReport, String, SessionsSeen), String> {
    let prefix = prefix.trim_matches('/');
    let keys = deja_compactor::list_objects(cfg, prefix)?;
    if keys.is_empty() {
        return Err(format!(
            "no objects under s3://{}/{prefix} — check the path (and that the recording window landed)",
            cfg.bucket
        ));
    }

    let mut by_session: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    let mut junk_lines = 0usize;
    for key in &keys {
        let data = deja_compactor::get_object_decoded(cfg, key)?;
        for line in data.split(|&b| b == b'\n') {
            if line.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }
            let line_str = String::from_utf8_lossy(line).into_owned();
            let sid = serde_json::from_str::<SessionProbe>(&line_str)
                .ok()
                .and_then(|p| p.capture)
                .and_then(|c| c.session_id);
            match sid {
                Some(sid) => by_session.entry(sid).or_default().push(line_str),
                None => junk_lines += 1,
            }
        }
    }
    if junk_lines > 0 {
        eprintln!("ingest: {junk_lines} line(s) without a capture.session_id skipped");
    }

    let mut seen: SessionsSeen = by_session
        .iter()
        .map(|(sid, lines)| (sid.clone(), lines.len()))
        .collect();
    seen.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

    let resolved = match session {
        Some(want) => {
            if !by_session.contains_key(want) {
                return Err(format!(
                    "session '{want}' not found under s3://{}/{prefix}; sessions seen: {}",
                    cfg.bucket,
                    describe_sessions(&seen)
                ));
            }
            want.to_owned()
        }
        None => match seen.len() {
            1 => seen[0].0.clone(),
            0 => {
                return Err(format!(
                    "objects under s3://{}/{prefix} contained no envelope lines",
                    cfg.bucket
                ))
            }
            _ => {
                return Err(format!(
                    "multiple sessions under s3://{}/{prefix} — pick one as the recording id: {}",
                    cfg.bucket,
                    describe_sessions(&seen)
                ))
            }
        },
    };

    let lines = by_session.remove(&resolved).unwrap_or_default();
    let chunk = lines.join("\n").into_bytes();
    let (events, lines_in, duplicates) = collate(&[chunk]);

    let dest = dest_for(&resolved);
    let dest = dest.as_path();
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(dest).map_err(|e| format!("create {}: {e}", dest.display()))?,
    );
    let mut correlations = std::collections::HashSet::new();
    for (_, _, line) in &events {
        if let Ok(probe) = serde_json::from_str::<CorrelationProbe>(line) {
            if let Some(corr) = probe.correlation_id {
                correlations.insert(corr);
            }
        }
        out.write_all(line.as_bytes())
            .and_then(|_| out.write_all(b"\n"))
            .map_err(|e| format!("write {}: {e}", dest.display()))?;
    }
    out.flush().map_err(|e| format!("flush: {e}"))?;

    let report = IngestReport {
        prefix: format!("s3://{}/{prefix}", cfg.bucket),
        landing_objects: keys.len(),
        lines_in,
        duplicates_dropped: duplicates,
        events_out: events.len(),
        correlations: correlations.len(),
        // A raw prefix has no manifest seal; completeness is whatever the
        // aggregator had flushed when we scanned.
        sealed: false,
    };
    Ok((report, resolved, seen))
}

fn describe_sessions(seen: &SessionsSeen) -> String {
    seen.iter()
        .map(|(sid, n)| format!("{sid} ({n} lines)"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Inject the internally-tagged `record_kind` as the first field of a raw JSON
/// event object, so the unwrapped line deserializes as a `DejaRecord`. Preserves
/// the original payload bytes verbatim (no reparse of the event body).
fn stamp_record_kind(event_json: &str, record_kind: &str) -> String {
    match event_json.trim_start().strip_prefix('{') {
        // Empty object `{}` — no trailing comma.
        Some(rest) if rest.trim_start().starts_with('}') => {
            format!("{{\"record_kind\":\"{record_kind}\"{rest}")
        }
        Some(rest) => format!("{{\"record_kind\":\"{record_kind}\",{rest}"),
        // Not a JSON object; leave as-is (it will fail the EventProbe parse and drop).
        None => event_json.to_owned(),
    }
}

/// Unwrap envelopes (raw event bytes preserved), probe the dedup/sort key,
/// drop duplicates and sink markers, sort canonically. Returns the sorted
/// `(recording_run_id, global_sequence, raw_event_json)` triples plus
/// `(lines_in, duplicates_dropped)`.
#[allow(clippy::type_complexity)]
fn collate(raw_chunks: &[Vec<u8>]) -> (Vec<(Option<String>, u64, String)>, usize, usize) {
    let mut seen = std::collections::HashSet::new();
    let mut events: Vec<(Option<String>, u64, String)> = Vec::new();
    let mut lines_in = 0usize;
    let mut duplicates = 0usize;
    for chunk in raw_chunks {
        for line in chunk.split(|&b| b == b'\n') {
            if line.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }
            lines_in += 1;
            let line_str = String::from_utf8_lossy(line);
            // Landing lines are envelopes; the payload's raw bytes are kept.
            let event_raw: String = match serde_json::from_str::<EnvelopeProbe>(&line_str) {
                Ok(EnvelopeProbe {
                    artifact_type,
                    event: Some(event),
                }) => {
                    // The canonical events.jsonl is a `DejaRecord` stream, internally
                    // tagged by `record_kind`. The wire envelope's `artifact_type` is
                    // the record kind, but the sink omits the tag from the raw event
                    // payload — stamp the matching one as we unwrap so the renderer
                    // and kernel can deserialize the line as a `DejaRecord`.
                    let record_kind = match artifact_type.as_deref() {
                        Some("deja_sink_marker") => continue, // loss-accounting, not events
                        Some("deja_graph_node") => "graph_node",
                        _ => "boundary_event", // deja_artifact_record (+ unset legacy)
                    };
                    stamp_record_kind(event.get(), record_kind)
                }
                _ => {
                    eprintln!("ingest: dropping non-envelope line");
                    continue;
                }
            };
            let probe: EventProbe = match serde_json::from_str(&event_raw) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("ingest: dropping unparseable line ({e})");
                    continue;
                }
            };
            if !seen.insert((probe.recording_run_id.clone(), probe.global_sequence)) {
                duplicates += 1;
                continue;
            }
            events.push((probe.recording_run_id, probe.global_sequence, event_raw));
        }
    }
    events.sort_by(|a, b| (&a.0, a.1).cmp(&(&b.0, b.1)));
    (events, lines_in, duplicates)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(rid: &str, gseq: u64, payload_extra: &str) -> String {
        format!(
            r#"{{"schema_version":2,"artifact_type":"deja_artifact_record","instance_id":"router-h-1","event":{{"recording_run_id":"{rid}","global_sequence":{gseq}{payload_extra}}}}}"#
        )
    }

    #[test]
    fn collate_unwraps_dedups_and_sorts() {
        // Two objects, out-of-order gseq, one duplicate across objects, one
        // sink marker, one junk line.
        let obj1 = format!(
            "{}\n{}\n{{\"artifact_type\":\"deja_sink_marker\",\"event\":{{\"kind\":\"checkpoint\"}}}}\n",
            envelope("r1", 3, r#","k":"c""#),
            envelope("r1", 1, r#","k":"a""#),
        );
        let obj2 = format!(
            "{}\n{}\nnot-json\n",
            envelope("r1", 1, r#","k":"a""#), // duplicate of obj1's gseq 1
            envelope("r1", 2, r#","k":"b""#),
        );
        let (events, lines_in, dupes) = collate(&[obj1.into_bytes(), obj2.into_bytes()]);
        assert_eq!(lines_in, 6);
        assert_eq!(dupes, 1);
        let gseqs: Vec<u64> = events.iter().map(|(_, g, _)| *g).collect();
        assert_eq!(gseqs, vec![1, 2, 3]);
        // Raw event bytes preserved verbatim (no key reordering).
        assert!(events[0].2.contains(r#""global_sequence":1,"k":"a""#));
    }

    #[test]
    fn collate_keeps_distinct_runs_apart() {
        let chunks =
            vec![format!("{}\n{}\n", envelope("r2", 1, ""), envelope("r1", 1, "")).into_bytes()];
        let (events, _, dupes) = collate(&chunks);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0.as_deref(), Some("r1")); // sorted by (rid, gseq)
    }
}
