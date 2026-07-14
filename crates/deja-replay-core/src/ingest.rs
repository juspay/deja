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

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
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
    #[serde(default)]
    pub breakdown: IngestBreakdown,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub downloaded_objects: Vec<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct IngestBreakdown {
    pub record_kinds: BTreeMap<String, usize>,
    pub boundaries: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct PulledRecording {
    pub report: IngestReport,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub manifest: Option<deja_compactor::SessionManifest>,
}

/// Minimal probe of an event for identity (dedup/sort key) — everything else
/// stays raw.
#[derive(serde::Deserialize)]
struct EventProbe {
    #[serde(default)]
    recording_run_id: Option<String>,
    #[serde(default, deserialize_with = "de_u64_lenient")]
    global_sequence: u64,
    #[serde(default)]
    correlation_id: Option<String>,
}

/// Envelope shape (v2): the payload is kept as raw bytes.
#[derive(serde::Deserialize)]
struct EnvelopeProbe<'a> {
    #[serde(default)]
    artifact_type: Option<String>,
    #[serde(borrow)]
    event: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow)]
    node: Option<&'a serde_json::value::RawValue>,
}

/// Marker/checkpoint lines are transport bookkeeping, not replay material.
#[derive(serde::Deserialize)]
struct LineKindProbe {
    #[serde(default)]
    artifact_type: Option<String>,
    #[serde(default)]
    record_kind: Option<String>,
    #[serde(default)]
    marker_kind: Option<String>,
}

#[derive(serde::Deserialize)]
struct RecordKindProbe {
    #[serde(default)]
    record_kind: Option<String>,
}

#[derive(serde::Deserialize)]
struct EventBreakdownProbe {
    #[serde(default)]
    record_kind: Option<String>,
    #[serde(default)]
    boundary: Option<String>,
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

    write_events(dest, &events)?;

    let report = IngestReport {
        prefix: deja_compactor::layout::session_root(recording_id),
        landing_objects: manifest.counts.landing_objects,
        lines_in,
        duplicates_dropped: manifest.counts.duplicates_dropped + duplicates,
        events_out: events.len(),
        correlations: manifest.counts.correlations,
        sealed: true,
        breakdown: ingest_breakdown(&events),
        downloaded_objects: manifest
            .data_parts
            .iter()
            .map(|part| part.key.clone())
            .collect(),
    };
    Ok((report, manifest))
}

/// Pull either the compacted session layout (`recording_source_uri == None`) or
/// a direct S3 object/prefix source (`s3://bucket/key-or-prefix`). Direct S3
/// sources are expanded recursively in lexical order, gzip-decoded when needed,
/// flattened when a part is a JSON array, merged, deduped, sorted, and
/// materialized into the same canonical `events.jsonl`.
pub fn pull_recording_source(
    cfg: &S3Config,
    recording_id: &str,
    recording_source_uri: Option<&str>,
    dest: &Path,
) -> Result<PulledRecording, String> {
    let Some(source) = recording_source_uri.filter(|source| !source.trim().is_empty()) else {
        let (report, manifest) = pull_recording(cfg, recording_id, dest)?;
        return Ok(PulledRecording {
            report,
            manifest: Some(manifest),
        });
    };
    if source.trim_start().starts_with("s3://") {
        return pull_direct_s3_recording(cfg, source, dest).map(|report| PulledRecording {
            report,
            manifest: None,
        });
    }
    Err(format!(
        "unsupported recording source {source:?}; expected s3://bucket/key-or-prefix"
    ))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3Uri {
    bucket: String,
    key: String,
}

fn parse_s3_uri(uri: &str) -> Result<S3Uri, String> {
    let without_scheme = uri
        .trim()
        .strip_prefix("s3://")
        .ok_or_else(|| format!("recording source is not an s3 URI: {uri}"))?;
    let (bucket, key) = without_scheme
        .split_once('/')
        .ok_or_else(|| format!("s3 URI must include a bucket and key/prefix: {uri}"))?;
    if bucket.trim().is_empty() || key.trim().is_empty() {
        return Err(format!(
            "s3 URI must include a bucket and key/prefix: {uri}"
        ));
    }
    Ok(S3Uri {
        bucket: bucket.to_owned(),
        key: key.trim_start_matches('/').to_owned(),
    })
}

fn is_supported_recording_key(key: &str) -> bool {
    matches!(
        key,
        k if k.ends_with(".log")
            || k.ends_with(".log.gz")
            || k.ends_with(".jsonl")
            || k.ends_with(".jsonl.gz")
            || k.ends_with(".ndjson")
            || k.ends_with(".ndjson.gz")
            || k.ends_with(".zst")
    )
}

fn decode_recording_object(key: &str, bytes: Vec<u8>) -> Result<Vec<u8>, String> {
    if key.ends_with(".gz") {
        let mut decoder = flate2::read::GzDecoder::new(std::io::Cursor::new(bytes));
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .map_err(|e| format!("gzip {key}: {e}"))?;
        Ok(decoded)
    } else {
        Ok(bytes)
    }
}

fn records_from_chunk(chunk: &[u8]) -> Vec<String> {
    let text = String::from_utf8_lossy(chunk);
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return Vec::new();
    }

    if trimmed.starts_with('[') {
        match serde_json::from_str::<Vec<&serde_json::value::RawValue>>(trimmed) {
            Ok(records) => {
                return records
                    .into_iter()
                    .map(|record| record.get().to_owned())
                    .collect()
            }
            Err(e) => {
                eprintln!("ingest: JSON array parse failed ({e}); falling back to line mode");
            }
        }
    }

    text.split('\n')
        .filter(|line| !line.bytes().all(|b| b.is_ascii_whitespace()))
        .map(str::to_owned)
        .collect()
}

fn is_sink_marker_line(line: &str) -> bool {
    let Ok(probe) = serde_json::from_str::<LineKindProbe>(line) else {
        return false;
    };

    probe
        .artifact_type
        .as_deref()
        .is_some_and(is_sink_marker_kind)
        || probe
            .record_kind
            .as_deref()
            .is_some_and(is_sink_marker_kind)
        || probe.marker_kind.is_some()
}

fn normalized_kind(kind: &str) -> String {
    kind.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(char::to_lowercase)
        .collect()
}

fn is_sink_marker_kind(kind: &str) -> bool {
    matches!(
        normalized_kind(kind).as_str(),
        "dejasinkmarker" | "sinkmarker"
    )
}

fn default_record_kind_for_artifact(artifact_type: Option<&str>) -> Option<&'static str> {
    let Some(artifact_type) = artifact_type else {
        return Some("boundary_event");
    };
    match normalized_kind(artifact_type).as_str() {
        "dejasinkmarker" | "sinkmarker" => None,
        "dejagraph" | "dejagraphnode" | "graph" | "graphnode" => Some("graph_node"),
        "dejarecord" | "dejaartifactrecord" | "artifactrecord" | "record" => Some("boundary_event"),
        _ => Some("boundary_event"),
    }
}

fn has_record_kind(event_json: &str) -> bool {
    if !event_json.contains("\"record_kind\"") {
        return false;
    }
    serde_json::from_str::<RecordKindProbe>(event_json)
        .ok()
        .and_then(|probe| probe.record_kind)
        .is_some()
}

fn ensure_record_kind(event_json: &str, default_record_kind: &str) -> String {
    if has_record_kind(event_json) {
        event_json.to_owned()
    } else {
        stamp_record_kind(event_json, default_record_kind)
    }
}

fn de_u64_lenient<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;

    match Option::<serde_json::Value>::deserialize(deserializer)? {
        None | Some(serde_json::Value::Null) => Ok(0),
        Some(serde_json::Value::Number(n)) => n
            .as_u64()
            .ok_or_else(|| serde::de::Error::custom(format!("expected u64, got {n}"))),
        Some(serde_json::Value::String(s)) => s.parse::<u64>().map_err(serde::de::Error::custom),
        Some(other) => Err(serde::de::Error::custom(format!(
            "expected u64 number or string, got {other}"
        ))),
    }
}

fn coerce_u64_string(value: &mut serde_json::Value) {
    let serde_json::Value::String(raw) = value else {
        return;
    };
    let Ok(parsed) = raw.parse::<u64>() else {
        return;
    };
    *value = serde_json::Value::Number(serde_json::Number::from(parsed));
}

fn coerce_u64_field(object: &mut serde_json::Map<String, serde_json::Value>, field: &str) {
    if let Some(value) = object.get_mut(field) {
        coerce_u64_string(value);
    }
}

fn coerce_u64_array_field(object: &mut serde_json::Map<String, serde_json::Value>, field: &str) {
    let Some(serde_json::Value::Array(values)) = object.get_mut(field) else {
        return;
    };
    for value in values {
        coerce_u64_string(value);
    }
}

/// Vector/string-preserving JSON pipelines stringify large unsigned integers
/// (notably values above i64::MAX). Canonical replay JSONL keeps those Deja
/// metadata fields typed as numbers so the agent and lookup renderer can parse
/// the tape normally. Payload fields (`request`, `response`, `args`, `result`)
/// are intentionally not inspected.
fn missing_or_null(object: &serde_json::Map<String, serde_json::Value>, field: &str) -> bool {
    object
        .get(field)
        .is_none_or(|value| matches!(value, serde_json::Value::Null))
}

fn normalize_event_numbers(event_json: &str, fallback_sequence: u64) -> String {
    let Ok(mut value) = serde_json::from_str::<serde_json::Value>(event_json) else {
        return event_json.to_owned();
    };
    let Some(object) = value.as_object_mut() else {
        return event_json.to_owned();
    };

    for field in [
        "global_sequence",
        "request_sequence",
        "timestamp_ns",
        "graph_node_id",
        "tracing_span_id",
        "fork_seq",
        "call_line",
        "call_column",
        "duration_us",
        "event_schema_version",
        "value_digest",
        "end_timestamp_ns",
        "source_event_global_sequence",
        "node_id",
        "parent_id",
        "sequence",
        "started_ns",
        "closed_ns",
        "policy_version",
    ] {
        coerce_u64_field(object, field);
    }
    coerce_u64_array_field(object, "causal_parent_ids");

    if matches!(
        object
            .get("record_kind")
            .and_then(serde_json::Value::as_str),
        Some("boundary_event")
    ) {
        if missing_or_null(object, "global_sequence") {
            object.insert(
                "global_sequence".to_owned(),
                serde_json::Value::Number(serde_json::Number::from(fallback_sequence)),
            );
        }
        let sequence = object
            .get("global_sequence")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(fallback_sequence);
        if missing_or_null(object, "request_sequence") {
            object.insert(
                "request_sequence".to_owned(),
                serde_json::Value::Number(serde_json::Number::from(sequence)),
            );
        }
    }

    if let Some(serde_json::Value::Object(identity)) = object.get_mut("callsite_identity") {
        for field in ["version", "occurrence", "syntax_hash"] {
            coerce_u64_field(identity, field);
        }
    }

    serde_json::to_string(&value).unwrap_or_else(|_| event_json.to_owned())
}

fn pull_direct_s3_recording(
    cfg: &S3Config,
    uri: &str,
    dest: &Path,
) -> Result<IngestReport, String> {
    let source = parse_s3_uri(uri)?;
    let mut source_cfg = cfg.clone();
    source_cfg.bucket = source.bucket.clone();
    let listed = deja_compactor::list_objects(&source_cfg, &source.key)?;
    let keys: Vec<String> = listed
        .into_iter()
        .filter(|key| is_supported_recording_key(key))
        .collect();
    if keys.is_empty() {
        return Err(format!(
            "no supported recording objects found under s3://{}/{}",
            source.bucket, source.key
        ));
    }

    let mut chunks = Vec::with_capacity(keys.len());
    for key in &keys {
        let bytes = deja_compactor::get_object(&source_cfg, key)?;
        chunks.push(decode_recording_object(key, bytes)?);
    }
    let (events, lines_in, duplicates) = collate(&chunks);
    write_events(dest, &events)?;

    Ok(IngestReport {
        prefix: format!("s3://{}/{}", source.bucket, source.key),
        landing_objects: keys.len(),
        lines_in,
        duplicates_dropped: duplicates,
        events_out: events.len(),
        correlations: correlation_count(&events),
        sealed: false,
        breakdown: ingest_breakdown(&events),
        downloaded_objects: keys,
    })
}

fn write_events(dest: &Path, events: &[(Option<String>, u64, String)]) -> Result<(), String> {
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(dest).map_err(|e| format!("create {}: {e}", dest.display()))?,
    );
    for (_, _, line) in events {
        out.write_all(line.as_bytes())
            .and_then(|_| out.write_all(b"\n"))
            .map_err(|e| format!("write {}: {e}", dest.display()))?;
    }
    out.flush().map_err(|e| format!("flush: {e}"))
}

fn correlation_count(events: &[(Option<String>, u64, String)]) -> usize {
    events
        .iter()
        .filter(|(_, _, line)| {
            serde_json::from_str::<RecordKindProbe>(line)
                .ok()
                .and_then(|probe| probe.record_kind)
                .is_none_or(|kind| kind == "boundary_event")
        })
        .filter_map(|(_, _, line)| serde_json::from_str::<EventProbe>(line).ok())
        .filter_map(|probe| probe.correlation_id)
        .collect::<BTreeSet<_>>()
        .len()
}

fn ingest_breakdown(events: &[(Option<String>, u64, String)]) -> IngestBreakdown {
    let mut breakdown = IngestBreakdown::default();
    for (_, _, line) in events {
        let Ok(probe) = serde_json::from_str::<EventBreakdownProbe>(line) else {
            continue;
        };
        let kind = probe
            .record_kind
            .unwrap_or_else(|| "boundary_event".to_owned());
        *breakdown.record_kinds.entry(kind.clone()).or_insert(0) += 1;
        if kind == "boundary_event" {
            let boundary = probe.boundary.unwrap_or_else(|| "<missing>".to_owned());
            *breakdown.boundaries.entry(boundary).or_insert(0) += 1;
        }
    }
    breakdown
}

/// Inject the internally-tagged `record_kind` as the first field of a raw JSON
/// event object, so the unwrapped line deserializes as a `DejaRecord`. Preserves
/// the original payload bytes verbatim (no reparse of the event body).
fn stamp_record_kind(event_json: &str, record_kind: &str) -> String {
    match event_json.trim_start().strip_prefix('{') {
        Some(rest) if rest.trim_start().starts_with('}') => {
            format!("{{\"record_kind\":\"{record_kind}\"{rest}")
        }
        Some(rest) => format!("{{\"record_kind\":\"{record_kind}\",{rest}"),
        None => event_json.to_owned(),
    }
}

/// Unwrap envelopes (raw event bytes preserved), probe the sort key, drop
/// exact normalized duplicates and sink markers, then sort canonically. The
/// event stream can contain graph and boundary records whose sequence spaces
/// are not globally unique in older tapes, so dedupe must not collapse records
/// solely by `(recording_run_id, global_sequence)`.
#[allow(clippy::type_complexity)]
fn collate(raw_chunks: &[Vec<u8>]) -> (Vec<(Option<String>, u64, String)>, usize, usize) {
    let mut seen = std::collections::HashSet::new();
    let mut events: Vec<(Option<String>, u64, String)> = Vec::new();
    let mut lines_in = 0usize;
    let mut duplicates = 0usize;
    for chunk in raw_chunks {
        for line_str in records_from_chunk(chunk) {
            lines_in += 1;
            if is_sink_marker_line(&line_str) {
                continue;
            }
            // Landing lines are envelopes; the payload's raw bytes are kept.
            let event_raw: String = match serde_json::from_str::<EnvelopeProbe>(&line_str) {
                Ok(EnvelopeProbe {
                    artifact_type,
                    event: Some(event),
                    ..
                }) => {
                    // The canonical events.jsonl is a `DejaRecord` stream,
                    // internally tagged by `record_kind`. The wire envelope's
                    // `artifact_type` is the record kind, but the sink omits
                    // the tag from the raw event payload — stamp the matching
                    // one as we unwrap so the renderer and kernel can
                    // deserialize the line as a `DejaRecord`.
                    let Some(record_kind) =
                        default_record_kind_for_artifact(artifact_type.as_deref())
                    else {
                        continue;
                    };
                    ensure_record_kind(event.get(), record_kind)
                }
                Ok(EnvelopeProbe {
                    artifact_type,
                    node: Some(node),
                    ..
                }) => {
                    let Some(record_kind) =
                        default_record_kind_for_artifact(artifact_type.as_deref())
                    else {
                        continue;
                    };
                    ensure_record_kind(node.get(), record_kind)
                }
                Ok(EnvelopeProbe {
                    artifact_type: Some(_),
                    event: None,
                    node: None,
                }) => continue,
                _ => match serde_json::from_str::<EventProbe>(&line_str) {
                    Ok(_) if line_str.contains("\"record_kind\"") => line_str,
                    Ok(_) => stamp_record_kind(&line_str, "boundary_event"),
                    Err(_) => {
                        eprintln!("ingest: dropping non-envelope line");
                        continue;
                    }
                },
            };
            let fallback_sequence = events.len() as u64 + 1;
            let event_raw = normalize_event_numbers(&event_raw, fallback_sequence);
            let probe: EventProbe = match serde_json::from_str(&event_raw) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("ingest: dropping unparseable line ({e})");
                    continue;
                }
            };
            if !seen.insert(event_raw.clone()) {
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

    #[test]
    fn parse_s3_uri_splits_bucket_and_key() {
        assert_eq!(
            parse_s3_uri("s3://hyperswitch-art/2026/07/09/file.log.gz").unwrap(),
            S3Uri {
                bucket: "hyperswitch-art".into(),
                key: "2026/07/09/file.log.gz".into(),
            }
        );
        assert!(parse_s3_uri("s3://hyperswitch-art").is_err());
    }

    #[test]
    fn collate_accepts_raw_event_lines() {
        let raw = br#"{"recording_run_id":"r1","global_sequence":7,"correlation_id":"c1"}"#;
        let (events, lines_in, dupes) = collate(&[raw.to_vec()]);
        assert_eq!(lines_in, 1);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 1);
        let value: serde_json::Value = serde_json::from_str(&events[0].2).unwrap();
        assert_eq!(value["record_kind"], "boundary_event");
        assert_eq!(correlation_count(&events), 1);
    }

    #[test]
    fn collate_skips_raw_sink_marker_lines() {
        let marker = br#"{"artifact_type":"deja_sink_marker","record_kind":"sink_marker","recording_run_id":"r1","global_sequence":1,"marker_kind":"flush","records_written":10,"records_dropped":0}"#;
        let raw = br#"{"recording_run_id":"r1","global_sequence":2,"correlation_id":"c1"}"#;
        let chunk = [marker.as_slice(), b"\n", raw.as_slice()].concat();

        let (events, lines_in, dupes) = collate(&[chunk]);

        assert_eq!(lines_in, 2);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1, 2);
        assert!(events[0].2.contains(r#""record_kind":"boundary_event""#));
    }

    #[test]
    fn collate_accepts_json_array_chunks() {
        let array = format!(
            "[\n{},\n{}\n]",
            envelope("r1", 2, r#","k":"b""#),
            envelope("r1", 1, r#","k":"a""#),
        );

        let (events, lines_in, dupes) = collate(&[array.into_bytes()]);

        assert_eq!(lines_in, 2);
        assert_eq!(dupes, 0);
        let gseqs: Vec<u64> = events.iter().map(|(_, g, _)| *g).collect();
        assert_eq!(gseqs, vec![1, 2]);
        assert!(events[0].2.contains(r#""global_sequence":1"#));
    }

    #[test]
    fn collate_normalizes_stringified_boundary_metadata_numbers() {
        let event = serde_json::json!({
            "recording_run_id": "r1",
            "global_sequence": "1",
            "request_sequence": "0",
            "correlation_id": "c1",
            "timestamp_ns": "1783029410812345678",
            "tracing_span_id": "9223372586610589699",
            "callsite_identity": {
                "version": "1",
                "source": "SyntacticHash",
                "id": null,
                "scope": null,
                "occurrence": "0",
                "caller_function": null,
                "lexical_path": null,
                "syntax_hash": "9223372586610589699"
            },
            "boundary": "db",
            "trait_name": "T",
            "method_name": "m",
            "call_file": "lib.rs",
            "call_line": "1",
            "call_column": "1",
            "request": {},
            "args": {},
            "response": {"ok": true},
            "result": {"ok": true},
            "is_error": false,
            "duration_us": "5",
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION.to_string(),
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute"
        });
        let envelope = serde_json::json!({
            "schema_version": 2,
            "artifact_type": "deja_artifact_record",
            "instance_id": "router-h-1",
            "event": event
        })
        .to_string();

        let (events, lines_in, dupes) = collate(&[envelope.into_bytes()]);

        assert_eq!(lines_in, 1);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 1);
        let record: deja::DejaRecord = serde_json::from_str(&events[0].2).unwrap();
        match record {
            deja::DejaRecord::BoundaryEvent(event) => {
                assert_eq!(event.global_sequence, 1);
                assert_eq!(event.timestamp_ns, 1_783_029_410_812_345_678);
                assert_eq!(event.tracing_span_id, Some(9_223_372_586_610_589_699));
                assert_eq!(
                    event
                        .callsite_identity
                        .and_then(|identity| identity.syntax_hash),
                    Some(9_223_372_586_610_589_699)
                );
            }
            other => panic!("expected boundary event, got {other:?}"),
        }
    }

    #[test]
    fn collate_routes_deja_record_and_deja_graph_artifacts_by_kind() {
        let boundary = serde_json::json!({
            "recording_run_id": "r1",
            "global_sequence": "2",
            "request_sequence": "1",
            "correlation_id": "c1",
            "timestamp_ns": "1783029410812345678",
            "boundary": "db",
            "trait_name": "T",
            "method_name": "m",
            "call_file": "lib.rs",
            "call_line": "1",
            "call_column": "1",
            "request": {},
            "args": {},
            "response": {"ok": true},
            "result": {"ok": true},
            "is_error": false,
            "duration_us": "5",
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION.to_string(),
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute"
        });
        let graph = serde_json::json!({
            "node_id": "7",
            "global_sequence": "1",
            "parent_id": null,
            "causal_parent_ids": [],
            "sequence": "0",
            "recording_run_id": "r1",
            "span_name": "request",
            "target": "router",
            "level": "INFO",
            "fields": {},
            "started_ns": "1783029410812345678",
            "closed_ns": null
        });
        let chunk = format!(
            "{}\n{}",
            serde_json::json!({
                "schema_version": 2,
                "artifact_type": "DejaRecord",
                "instance_id": "router-h-1",
                "event": boundary
            }),
            serde_json::json!({
                "schema_version": 2,
                "artifact_type": "DejaGraph",
                "instance_id": "router-h-1",
                "event": graph
            })
        );

        let (events, lines_in, dupes) = collate(&[chunk.into_bytes()]);

        assert_eq!(lines_in, 2);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 2);
        let records = events
            .iter()
            .map(|(_, _, line)| serde_json::from_str::<deja::DejaRecord>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(records[0], deja::DejaRecord::GraphNode(_)));
        assert!(matches!(records[1], deja::DejaRecord::BoundaryEvent(_)));
    }

    #[test]
    fn collate_keeps_distinct_kinds_with_same_global_sequence() {
        let boundary = serde_json::json!({
            "recording_run_id": "r1",
            "global_sequence": "1",
            "request_sequence": "1",
            "correlation_id": "c1",
            "timestamp_ns": "1783029410812345678",
            "boundary": "db",
            "trait_name": "T",
            "method_name": "m",
            "call_file": "lib.rs",
            "call_line": "1",
            "call_column": "1",
            "request": {},
            "args": {},
            "response": {"ok": true},
            "result": {"ok": true},
            "is_error": false,
            "duration_us": "5",
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION.to_string(),
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute"
        });
        let graph = serde_json::json!({
            "node_id": "7",
            "global_sequence": "1",
            "parent_id": null,
            "causal_parent_ids": [],
            "sequence": "0",
            "recording_run_id": "r1",
            "correlation_id": "graph-correlation",
            "span_name": "request",
            "target": "router",
            "level": "INFO",
            "fields": {},
            "started_ns": "1783029410812345678",
            "closed_ns": null
        });
        let chunk = format!(
            "{}\n{}",
            serde_json::json!({
                "schema_version": 2,
                "artifact_type": "deja_artifact_record",
                "instance_id": "router-h-1",
                "event": boundary
            }),
            serde_json::json!({
                "schema_version": 2,
                "artifact_type": "deja_graph_node",
                "instance_id": "router-h-1",
                "node": graph
            })
        );

        let (events, lines_in, dupes) = collate(&[chunk.into_bytes()]);

        assert_eq!(lines_in, 2);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 2);
        assert_eq!(correlation_count(&events), 1);
        let records = events
            .iter()
            .map(|(_, _, line)| serde_json::from_str::<deja::DejaRecord>(line).unwrap())
            .collect::<Vec<_>>();
        assert!(matches!(records[0], deja::DejaRecord::BoundaryEvent(_)));
        assert!(matches!(records[1], deja::DejaRecord::GraphNode(_)));
    }

    #[test]
    fn collate_unwraps_graph_node_payload_even_when_wrapper_record_kind_is_wrong() {
        let graph = serde_json::json!({
            "node_id": "7",
            "global_sequence": "1",
            "parent_id": null,
            "causal_parent_ids": [],
            "sequence": "0",
            "recording_run_id": "r1",
            "span_name": "request",
            "target": "router",
            "level": "INFO",
            "fields": {},
            "started_ns": "1783029410812345678",
            "closed_ns": null
        });
        let envelope = serde_json::json!({
            "schema_version": 2,
            "artifact_type": "deja_graph_node",
            "record_kind": "boundary_event",
            "instance_id": "router-h-1",
            "node": graph
        })
        .to_string();

        let (events, lines_in, dupes) = collate(&[envelope.into_bytes()]);

        assert_eq!(lines_in, 1);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 1);
        let record: deja::DejaRecord = serde_json::from_str(&events[0].2).unwrap();
        match record {
            deja::DejaRecord::GraphNode(node) => {
                assert_eq!(node.node_id, 7);
                assert_eq!(node.global_sequence, 1);
            }
            other => panic!("expected graph node, got {other:?}"),
        }
    }

    #[test]
    fn collate_backfills_missing_boundary_global_sequence() {
        let event = serde_json::json!({
            "recording_run_id": "r1",
            "correlation_id": "c1",
            "timestamp_ns": 1_783_029_410_812_345_678_u64,
            "boundary": "db",
            "trait_name": "T",
            "method_name": "m",
            "call_file": "lib.rs",
            "call_line": 1,
            "call_column": 1,
            "request": {},
            "args": {},
            "response": {"ok": true},
            "result": {"ok": true},
            "is_error": false,
            "duration_us": 5,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute"
        });
        let envelope = serde_json::json!({
            "schema_version": 2,
            "artifact_type": "deja_artifact_record",
            "instance_id": "router-h-1",
            "event": event
        })
        .to_string();

        let (events, lines_in, dupes) = collate(&[envelope.into_bytes()]);

        assert_eq!(lines_in, 1);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 1);
        let record: deja::DejaRecord = serde_json::from_str(&events[0].2).unwrap();
        match record {
            deja::DejaRecord::BoundaryEvent(event) => {
                assert_eq!(event.global_sequence, 1);
                assert_eq!(event.request_sequence, 1);
            }
            other => panic!("expected boundary event, got {other:?}"),
        }
    }

    #[test]
    fn collate_normalizes_stringified_graph_metadata_numbers() {
        let event = serde_json::json!({
            "node_id": "7",
            "global_sequence": "2",
            "parent_id": "1",
            "causal_parent_ids": ["1", "6"],
            "sequence": "3",
            "recording_run_id": "r1",
            "span_name": "request",
            "target": "router",
            "level": "INFO",
            "fields": {},
            "started_ns": "1783029410812345678",
            "closed_ns": "1783029410812345999"
        });
        let envelope = serde_json::json!({
            "schema_version": 2,
            "artifact_type": "deja_graph_node",
            "instance_id": "router-h-1",
            "event": event
        })
        .to_string();

        let (events, _, _) = collate(&[envelope.into_bytes()]);

        assert_eq!(events.len(), 1);
        let record: deja::DejaRecord = serde_json::from_str(&events[0].2).unwrap();
        match record {
            deja::DejaRecord::GraphNode(node) => {
                assert_eq!(node.node_id, 7);
                assert_eq!(node.global_sequence, 2);
                assert_eq!(node.parent_id, Some(1));
                assert_eq!(node.causal_parent_ids, vec![1, 6]);
                assert_eq!(node.started_ns, 1_783_029_410_812_345_678);
                assert_eq!(node.closed_ns, Some(1_783_029_410_812_345_999));
            }
            other => panic!("expected graph node, got {other:?}"),
        }
    }

    #[test]
    fn gzip_recording_object_decodes() {
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(b"hello\n").unwrap();
        let bytes = gz.finish().unwrap();
        assert_eq!(
            decode_recording_object("part.log.gz", bytes).unwrap(),
            b"hello\n"
        );
    }

    #[test]
    fn gzip_json_array_recording_decodes_and_collates() {
        let array = format!("[{},{}]", envelope("r1", 1, ""), envelope("r1", 2, ""));
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(array.as_bytes()).unwrap();
        let decoded = decode_recording_object("part.log.gz", gz.finish().unwrap()).unwrap();

        let (events, lines_in, dupes) = collate(&[decoded]);

        assert_eq!(lines_in, 2);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 2);
    }
}
