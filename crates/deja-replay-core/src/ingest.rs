//! Recording ingest: sealed sessions out of S3 (Phase 2.1 + 2.3).
//!
//! The durable form of a recording is the compacted session
//! (`sessions/v1/{id}/` — data parts + correlations index + manifest seal,
//! see `deja-compactor`). Pulling a recording means:
//!
//! 1. read the manifest; if the session is unsealed, compact it first
//!    (the record lifecycle's quiesce wait has already settled the landing)
//! 2. stream the data parts (full envelope lines, already deduped + sorted)
//! 3. unwrap envelopes into typed [`deja::DejaRecord`]s (unknown payload
//!    fields ride the extras maps) — re-verify dedup/order by
//!    `(recording_run_id, global_sequence)` while materializing the
//!    canonical `events.jsonl` the kernel + renderer read
//!
//! (`KeyStamper` occurrences are correlation/address/args-scoped, so
//! dedup+sort cannot perturb lookup stamping.)

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::path::Path;

pub use deja_compactor::S3Config;

/// Typed ingest failure. `S3` wraps the compactor's string errors at that
/// crate boundary; everything downstream is structured.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("{0}")]
    S3(String),
    #[error("{0}")]
    Decode(String),
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    #[error("unsupported recording source {0:?}; expected s3://bucket/key-or-prefix")]
    UnsupportedSource(String),
    #[error("recording {recording_id} produced no valid events ({lines_in} line(s) in, {lines_dropped} dropped)")]
    NoEvents {
        recording_id: String,
        lines_in: usize,
        lines_dropped: usize,
    },
}

/// What `pull_recording` reports back (persisted next to the events file,
/// registered as a run artifact, folded into the catalog row).
#[derive(Debug, Clone, serde::Serialize)]
pub struct IngestReport {
    pub prefix: String,
    pub landing_objects: usize,
    pub lines_in: usize,
    pub duplicates_dropped: usize,
    /// Lines that failed to parse or carried an unknown artifact type. Sink
    /// markers and blank lines are transport bookkeeping, not drops.
    #[serde(default)]
    pub lines_dropped: usize,
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

/// Landing-line envelope, typed. Unknown envelope metadata (`instance_id`,
/// `capture`, `code`, envelope-level `schema_version` — v1 and v2 both occur)
/// is intentionally ignored; only the payload reaches `events.jsonl`.
#[derive(serde::Deserialize)]
struct LandingEnvelope<'a> {
    #[serde(default)]
    artifact_type: Option<ArtifactType>,
    #[serde(borrow, default)]
    event: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow, default)]
    node: Option<&'a serde_json::value::RawValue>,
    /// Top-level `record_kind` on raw (non-enveloped) marker lines.
    #[serde(default)]
    record_kind: Option<String>,
    /// Presence alone marks a sink marker line.
    #[serde(default)]
    marker_kind: Option<serde_json::Value>,
}

impl LandingEnvelope<'_> {
    fn is_raw_line(&self) -> bool {
        self.artifact_type.is_none() && self.event.is_none() && self.node.is_none()
    }

    fn is_sink_marker(&self) -> bool {
        self.marker_kind.is_some()
            || self.artifact_type == Some(ArtifactType::SinkMarker)
            || self
                .record_kind
                .as_deref()
                .is_some_and(|kind| ArtifactType::from_wire(kind) == ArtifactType::SinkMarker)
    }
}

/// Artifact routing kind. Spelling-tolerant: `DejaGraph`, `deja_graph_node`,
/// and `GRAPH-NODE` all normalize to the same kind. Unrecognized types become
/// `Unknown` so the envelope still parses and the line is dropped WITH
/// accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactType {
    BoundaryEvent,
    GraphNode,
    SinkMarker,
    Unknown,
}

impl ArtifactType {
    fn from_wire(kind: &str) -> Self {
        let normalized: String = kind
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .flat_map(char::to_lowercase)
            .collect();
        match normalized.as_str() {
            "dejasinkmarker" | "sinkmarker" => Self::SinkMarker,
            "dejagraph" | "dejagraphnode" | "graph" | "graphnode" => Self::GraphNode,
            "dejarecord" | "dejaartifactrecord" | "artifactrecord" | "record" => {
                Self::BoundaryEvent
            }
            _ => Self::Unknown,
        }
    }
}

impl<'de> serde::Deserialize<'de> for ArtifactType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Self::from_wire(&String::deserialize(d)?))
    }
}

/// Typed routing probe: does the payload carry the `record_kind` tag? When it
/// does, `DejaRecord`'s internal tag wins over the envelope's artifact type.
#[derive(serde::Deserialize)]
struct PayloadTag {
    #[serde(default)]
    record_kind: Option<String>,
}

/// One collated output line: the typed record plus its canonical serialized
/// form (serialized exactly once; dedup compares the canonical form because
/// graph and boundary records share the gseq counter space).
pub struct CollatedRecord {
    pub record: deja::DejaRecord,
    pub json: String,
}

impl CollatedRecord {
    fn new(record: deja::DejaRecord) -> Result<Self, serde_json::Error> {
        let json = serde_json::to_string(&record)?;
        Ok(Self { record, json })
    }

    pub fn run_id(&self) -> Option<&str> {
        match &self.record {
            deja::DejaRecord::BoundaryEvent(event) => event.recording_run_id.as_deref(),
            deja::DejaRecord::GraphNode(node) => node.recording_run_id.as_deref(),
            deja::DejaRecord::Observed(_) => None,
        }
    }

    pub fn global_sequence(&self) -> u64 {
        self.record.global_sequence()
    }
}

mod legacy {
    //! The one quarantined `serde_json::Value` shim: old direct-S3 tapes can
    //! omit `global_sequence`/`request_sequence` entirely. Both are required
    //! fields and `0` is a legitimate value (real tapes carry
    //! `request_sequence: 0`), so "missing" cannot be expressed by a serde
    //! default. Attempted only after a typed boundary parse fails.

    pub(super) fn rescue_missing_sequences(payload: &str, fallback: u64) -> Option<String> {
        let mut value: serde_json::Value = serde_json::from_str(payload).ok()?;
        let object = value.as_object_mut()?;
        let missing = |object: &serde_json::Map<String, serde_json::Value>, field: &str| {
            object.get(field).is_none_or(serde_json::Value::is_null)
        };
        if missing(object, "global_sequence") {
            object.insert("global_sequence".to_owned(), fallback.into());
        }
        let sequence = match object.get("global_sequence") {
            Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(fallback),
            Some(serde_json::Value::String(s)) => s.parse().unwrap_or(fallback),
            _ => fallback,
        };
        if missing(object, "request_sequence") {
            object.insert("request_sequence".to_owned(), sequence.into());
        }
        serde_json::to_string(&value).ok()
    }
}

/// Count landing objects for a recording (the "did Vector land anything yet /
/// has the flush settled" poll the lifecycle runs before compacting).
pub fn count_session_objects(cfg: &S3Config, recording_id: &str) -> Result<usize, IngestError> {
    deja_compactor::count_landing_objects(cfg, recording_id).map_err(IngestError::S3)
}

/// Pull a session recording into `dest` (the canonical
/// `{root}/recordings/{id}/events.jsonl` slot), compacting first if the
/// session isn't sealed yet. Returns the ingest report plus the manifest.
pub fn pull_recording(
    cfg: &S3Config,
    recording_id: &str,
    dest: &Path,
) -> Result<(IngestReport, deja_compactor::SessionManifest), IngestError> {
    let manifest = match deja_compactor::read_manifest(cfg, recording_id).map_err(IngestError::S3)?
    {
        Some(m) => m,
        None => deja_compactor::compact_session(cfg, recording_id).map_err(IngestError::S3)?,
    };
    let lines = deja_compactor::read_session_lines(cfg, &manifest).map_err(IngestError::S3)?;
    let chunk = lines.join("\n").into_bytes();
    let (events, lines_in, duplicates, dropped) = collate(&[chunk]);
    if events.is_empty() {
        return Err(IngestError::NoEvents {
            recording_id: recording_id.to_owned(),
            lines_in,
            lines_dropped: dropped,
        });
    }

    write_events(dest, &events)?;

    let report = IngestReport {
        prefix: deja_compactor::layout::session_root(recording_id),
        landing_objects: manifest.counts.landing_objects,
        lines_in,
        duplicates_dropped: manifest.counts.duplicates_dropped + duplicates,
        lines_dropped: dropped,
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
) -> Result<PulledRecording, IngestError> {
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
    Err(IngestError::UnsupportedSource(source.to_owned()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct S3Uri {
    bucket: String,
    key: String,
}

fn parse_s3_uri(uri: &str) -> Result<S3Uri, IngestError> {
    let without_scheme = uri
        .trim()
        .strip_prefix("s3://")
        .ok_or_else(|| IngestError::Decode(format!("recording source is not an s3 URI: {uri}")))?;
    let (bucket, key) = without_scheme.split_once('/').ok_or_else(|| {
        IngestError::Decode(format!("s3 URI must include a bucket and key/prefix: {uri}"))
    })?;
    if bucket.trim().is_empty() || key.trim().is_empty() {
        return Err(IngestError::Decode(format!(
            "s3 URI must include a bucket and key/prefix: {uri}"
        )));
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

fn decode_recording_object(key: &str, bytes: Vec<u8>) -> Result<Vec<u8>, IngestError> {
    if key.ends_with(".gz") {
        let mut decoder = flate2::read::GzDecoder::new(std::io::Cursor::new(bytes));
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .map_err(|e| IngestError::Decode(format!("gzip {key}: {e}")))?;
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

/// Parse one payload into a typed record. When the payload carries the
/// `record_kind` tag, `DejaRecord`'s internal tag wins over the envelope's
/// artifact type; otherwise the artifact type picks the route. Boundary
/// payloads that fail to parse get one legacy rescue for missing sequence
/// fields before being dropped.
fn parse_payload(
    payload: &str,
    artifact_type: ArtifactType,
    fallback_sequence: u64,
) -> Option<deja::DejaRecord> {
    let tagged = serde_json::from_str::<PayloadTag>(payload)
        .ok()
        .and_then(|tag| tag.record_kind)
        .is_some();
    if tagged {
        if let Ok(record) = serde_json::from_str::<deja::DejaRecord>(payload) {
            return Some(record);
        }
        // A tagged boundary payload may still be missing its sequences.
        let rescued = legacy::rescue_missing_sequences(payload, fallback_sequence)?;
        return serde_json::from_str::<deja::DejaRecord>(&rescued).ok();
    }
    match artifact_type {
        ArtifactType::BoundaryEvent => {
            if let Ok(event) = serde_json::from_str::<deja::BoundaryEvent>(payload) {
                return Some(deja::DejaRecord::BoundaryEvent(Box::new(event)));
            }
            let rescued = legacy::rescue_missing_sequences(payload, fallback_sequence)?;
            serde_json::from_str::<deja::BoundaryEvent>(&rescued)
                .ok()
                .map(|event| deja::DejaRecord::BoundaryEvent(Box::new(event)))
        }
        ArtifactType::GraphNode => serde_json::from_str::<deja::ExecutionGraphNode>(payload)
            .ok()
            .map(deja::DejaRecord::GraphNode),
        ArtifactType::SinkMarker | ArtifactType::Unknown => None,
    }
}

fn pull_direct_s3_recording(
    cfg: &S3Config,
    uri: &str,
    dest: &Path,
) -> Result<IngestReport, IngestError> {
    let source = parse_s3_uri(uri)?;
    let mut source_cfg = cfg.clone();
    source_cfg.bucket = source.bucket.clone();
    let listed = deja_compactor::list_objects(&source_cfg, &source.key).map_err(IngestError::S3)?;
    let keys: Vec<String> = listed
        .into_iter()
        .filter(|key| is_supported_recording_key(key))
        .collect();
    if keys.is_empty() {
        return Err(IngestError::S3(format!(
            "no supported recording objects found under s3://{}/{}",
            source.bucket, source.key
        )));
    }

    let mut chunks = Vec::with_capacity(keys.len());
    for key in &keys {
        let bytes = deja_compactor::get_object(&source_cfg, key).map_err(IngestError::S3)?;
        chunks.push(decode_recording_object(key, bytes)?);
    }
    let (events, lines_in, duplicates, dropped) = collate(&chunks);
    if events.is_empty() {
        return Err(IngestError::NoEvents {
            recording_id: format!("s3://{}/{}", source.bucket, source.key),
            lines_in,
            lines_dropped: dropped,
        });
    }
    write_events(dest, &events)?;

    Ok(IngestReport {
        prefix: format!("s3://{}/{}", source.bucket, source.key),
        landing_objects: keys.len(),
        lines_in,
        duplicates_dropped: duplicates,
        lines_dropped: dropped,
        events_out: events.len(),
        correlations: correlation_count(&events),
        sealed: false,
        breakdown: ingest_breakdown(&events),
        downloaded_objects: keys,
    })
}

fn write_events(dest: &Path, events: &[CollatedRecord]) -> Result<(), IngestError> {
    let io_err = |context: String| move |source: std::io::Error| IngestError::Io { context, source };
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .map_err(io_err(format!("mkdir {}", parent.display())))?;
    }
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(dest).map_err(io_err(format!("create {}", dest.display())))?,
    );
    for event in events {
        out.write_all(event.json.as_bytes())
            .and_then(|_| out.write_all(b"\n"))
            .map_err(io_err(format!("write {}", dest.display())))?;
    }
    out.flush().map_err(io_err("flush".to_owned()))
}

fn correlation_count(events: &[CollatedRecord]) -> usize {
    events
        .iter()
        .filter_map(|event| match &event.record {
            deja::DejaRecord::BoundaryEvent(inner) => inner.correlation_id.as_deref(),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .len()
}

fn ingest_breakdown(events: &[CollatedRecord]) -> IngestBreakdown {
    let mut breakdown = IngestBreakdown::default();
    for event in events {
        let kind = match &event.record {
            deja::DejaRecord::BoundaryEvent(_) => "boundary_event",
            deja::DejaRecord::GraphNode(_) => "graph_node",
            deja::DejaRecord::Observed(_) => "observed",
        };
        *breakdown.record_kinds.entry(kind.to_owned()).or_insert(0) += 1;
        if let deja::DejaRecord::BoundaryEvent(inner) = &event.record {
            *breakdown
                .boundaries
                .entry(inner.boundary.clone())
                .or_insert(0) += 1;
        }
    }
    breakdown
}
/// the original payload bytes verbatim (no reparse of the event body).
#[allow(clippy::type_complexity)]
/// Unwrap envelopes into typed records, drop sink markers and unparseable
/// lines (counting the drops), dedup on the canonical serialized form, then
/// sort by `(recording_run_id, global_sequence)`. Graph and boundary records
/// share the gseq counter space in older tapes, so dedup must never collapse
/// records on the sequence key alone.
fn collate(raw_chunks: &[Vec<u8>]) -> (Vec<CollatedRecord>, usize, usize, usize) {
    let mut seen = std::collections::HashSet::new();
    let mut events: Vec<CollatedRecord> = Vec::new();
    let mut lines_in = 0usize;
    let mut duplicates = 0usize;
    let mut dropped = 0usize;
    for chunk in raw_chunks {
        for line_str in records_from_chunk(chunk) {
            lines_in += 1;
            let Ok(envelope) = serde_json::from_str::<LandingEnvelope>(&line_str) else {
                eprintln!("ingest: dropping non-JSON line");
                dropped += 1;
                continue;
            };
            if envelope.is_sink_marker() {
                continue;
            }
            let fallback_sequence = events.len() as u64 + 1;
            let record = if envelope.is_raw_line() {
                // Raw (non-enveloped) event line: route by its own tag, or
                // default to the boundary route like the old pipeline.
                parse_payload(&line_str, ArtifactType::BoundaryEvent, fallback_sequence)
            } else {
                let artifact_type = envelope.artifact_type.unwrap_or(ArtifactType::BoundaryEvent);
                if artifact_type == ArtifactType::Unknown {
                    eprintln!("ingest: dropping envelope with unknown artifact_type");
                    dropped += 1;
                    continue;
                }
                let payload = match artifact_type {
                    ArtifactType::BoundaryEvent => envelope.event.or(envelope.node),
                    ArtifactType::GraphNode => envelope.node.or(envelope.event),
                    ArtifactType::SinkMarker | ArtifactType::Unknown => None,
                };
                payload.and_then(|payload| {
                    parse_payload(payload.get(), artifact_type, fallback_sequence)
                })
            };
            let Some(record) = record else {
                eprintln!("ingest: dropping unparseable line");
                dropped += 1;
                continue;
            };
            let Ok(collated) = CollatedRecord::new(record) else {
                eprintln!("ingest: dropping unserializable record");
                dropped += 1;
                continue;
            };
            if !seen.insert(collated.json.clone()) {
                duplicates += 1;
                continue;
            }
            events.push(collated);
        }
    }
    events.sort_by(|a, b| {
        (a.run_id().map(str::to_owned), a.global_sequence())
            .cmp(&(b.run_id().map(str::to_owned), b.global_sequence()))
    });
    (events, lines_in, duplicates, dropped)
}
#[cfg(test)]
mod tests {
    use super::*;

    /// Full valid boundary payload: the typed pipeline rejects skeleton events
    /// (that rejection is itself under test in
    /// `junk_object_lines_are_dropped_not_minted_as_events`).
    fn boundary_payload(rid: &str, gseq: u64, payload_extra: &str) -> String {
        format!(
            concat!(
                r#"{{"recording_run_id":"{rid}","global_sequence":{gseq},"#,
                r#""request_sequence":{gseq},"correlation_id":"c1","timestamp_ns":1,"#,
                r#""boundary":"db","trait_name":"T","method_name":"m","#,
                r#""call_file":"f","call_line":1,"call_column":1,"#,
                r#""request":{{}},"args":{{}},"response":{{}},"result":{{}},"#,
                r#""is_error":false,"duration_us":1,"event_schema_version":{schema},"#,
                r#""provenance":"recorded","recon":"lossless","#,
                r#""replay_strategy":"substitute"{payload_extra}}}"#
            ),
            rid = rid,
            gseq = gseq,
            schema = deja::CURRENT_EVENT_SCHEMA_VERSION,
            payload_extra = payload_extra,
        )
    }

    fn envelope(rid: &str, gseq: u64, payload_extra: &str) -> String {
        format!(
            r#"{{"schema_version":2,"artifact_type":"deja_artifact_record","instance_id":"router-h-1","event":{}}}"#,
            boundary_payload(rid, gseq, payload_extra)
        )
    }

    #[test]
    fn artifact_type_spelling_variants_all_route() {
        for spelling in ["deja_graph_node", "DejaGraph", "GRAPH-NODE", "graphnode"] {
            let envelope = serde_json::json!({
                "artifact_type": spelling,
                "node": {"node_id": 1, "sequence": 0, "span_name": "s",
                          "target": "t", "level": "INFO", "started_ns": 5}
            })
            .to_string();
            let (events, _, _, dropped) = collate(&[envelope.into_bytes()]);
            assert_eq!(events.len(), 1, "spelling {spelling:?} must route");
            assert_eq!(dropped, 0);
            assert!(matches!(events[0].record, deja::DejaRecord::GraphNode(_)));
        }
    }

    #[test]
    fn unknown_payload_fields_survive_ingest() {
        let envelope = serde_json::json!({
            "artifact_type": "deja_artifact_record",
            "event": {
                "global_sequence": 1, "request_sequence": 1, "correlation_id": "c1",
                "timestamp_ns": 1, "boundary": "db", "trait_name": "T",
                "method_name": "m", "call_file": "f", "call_line": 1, "call_column": 1,
                "request": {}, "args": {}, "response": {}, "result": {},
                "is_error": false, "duration_us": 1,
                "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
                "provenance": "recorded", "recon": "lossless",
                "replay_strategy": "substitute",
                "brand_new_field": "must-survive"
            }
        })
        .to_string();
        let (events, _, _, dropped) = collate(&[envelope.into_bytes()]);
        assert_eq!(dropped, 0);
        let v: serde_json::Value = serde_json::from_str(&events[0].json).unwrap();
        assert_eq!(v["brand_new_field"], "must-survive");
    }

    #[test]
    fn rescue_applies_only_to_missing_sequences_not_present_zero() {
        // request_sequence present as 0 must stay 0 (real tapes carry it).
        let with_zero = serde_json::json!({
            "artifact_type": "deja_artifact_record",
            "event": {
                "global_sequence": 979, "request_sequence": 0, "timestamp_ns": 1,
                "boundary": "http_incoming", "trait_name": "T", "method_name": "m",
                "call_file": "f", "call_line": 1, "call_column": 1,
                "request": {}, "args": {}, "response": {}, "result": {},
                "is_error": false, "duration_us": 1,
                "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
                "provenance": "recorded", "recon": "lossless",
                "replay_strategy": "substitute"
            }
        })
        .to_string();
        let (events, _, _, _) = collate(&[with_zero.into_bytes()]);
        match &events[0].record {
            deja::DejaRecord::BoundaryEvent(e) => {
                assert_eq!(e.global_sequence, 979);
                assert_eq!(e.request_sequence, 0);
            }
            other => panic!("expected boundary event, got {other:?}"),
        }
    }

    #[test]
    fn junk_object_lines_are_dropped_not_minted_as_events() {
        // Previously any JSON object was stamped boundary_event; typed parse
        // rejects it and counts the drop.
        let raw = br#"{"foo": 1}"#.to_vec();
        let (events, lines_in, _, dropped) = collate(&[raw]);
        assert!(events.is_empty());
        assert_eq!(lines_in, 1);
        assert_eq!(dropped, 1);
    }

    #[test]
    fn collate_counts_dropped_lines() {
        let junk = b"not-json\n{\"artifact_type\":\"unexpected_record_type\",\"event\":{\"global_sequence\":1}}\n".to_vec();
        let (events, lines_in, _dupes, dropped) = collate(&[junk]);
        assert!(events.is_empty());
        assert_eq!(lines_in, 2);
        assert_eq!(dropped, 2);
    }

    #[test]
    fn no_events_error_reports_counts() {
        let err = IngestError::NoEvents {
            recording_id: "rec-1".into(),
            lines_in: 5,
            lines_dropped: 5,
        };
        assert_eq!(
            err.to_string(),
            "recording rec-1 produced no valid events (5 line(s) in, 5 dropped)"
        );
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
        let (events, lines_in, dupes, _dropped) = collate(&[obj1.into_bytes(), obj2.into_bytes()]);
        assert_eq!(lines_in, 6);
        assert_eq!(dupes, 1);
        let gseqs: Vec<u64> = events.iter().map(CollatedRecord::global_sequence).collect();
        assert_eq!(gseqs, vec![1, 2, 3]);
        // Unknown payload fields survive the typed round-trip.
        let value: serde_json::Value = serde_json::from_str(&events[0].json).unwrap();
        assert_eq!(value["global_sequence"], 1);
        assert_eq!(value["k"], "a");
    }

    #[test]
    fn collate_keeps_distinct_runs_apart() {
        let chunks =
            vec![format!("{}\n{}\n", envelope("r2", 1, ""), envelope("r1", 1, "")).into_bytes()];
        let (events, _, dupes, _dropped) = collate(&chunks);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].run_id(), Some("r1")); // sorted by (rid, gseq)
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
        let raw = boundary_payload("r1", 7, "").into_bytes();
        let (events, lines_in, dupes, _dropped) = collate(&[raw]);
        assert_eq!(lines_in, 1);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 1);
        let value: serde_json::Value = serde_json::from_str(&events[0].json).unwrap();
        assert_eq!(value["record_kind"], "boundary_event");
        assert_eq!(correlation_count(&events), 1);
    }

    #[test]
    fn collate_skips_raw_sink_marker_lines() {
        let marker = br#"{"artifact_type":"deja_sink_marker","record_kind":"sink_marker","recording_run_id":"r1","global_sequence":1,"marker_kind":"flush","records_written":10,"records_dropped":0}"#;
        let raw = boundary_payload("r1", 2, "").into_bytes();
        let chunk = [marker.as_slice(), b"\n", raw.as_slice()].concat();

        let (events, lines_in, dupes, _dropped) = collate(&[chunk]);

        assert_eq!(lines_in, 2);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].global_sequence(), 2);
        assert!(events[0].json.contains(r#""record_kind":"boundary_event""#));
    }

    #[test]
    fn collate_accepts_json_array_chunks() {
        let array = format!(
            "[\n{},\n{}\n]",
            envelope("r1", 2, r#","k":"b""#),
            envelope("r1", 1, r#","k":"a""#),
        );

        let (events, lines_in, dupes, _dropped) = collate(&[array.into_bytes()]);

        assert_eq!(lines_in, 2);
        assert_eq!(dupes, 0);
        let gseqs: Vec<u64> = events.iter().map(CollatedRecord::global_sequence).collect();
        assert_eq!(gseqs, vec![1, 2]);
        assert!(events[0].json.contains(r#""global_sequence":1"#));
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

        let (events, lines_in, dupes, _dropped) = collate(&[envelope.into_bytes()]);

        assert_eq!(lines_in, 1);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 1);
        let record: deja::DejaRecord = serde_json::from_str(&events[0].json).unwrap();
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

        let (events, lines_in, dupes, _dropped) = collate(&[chunk.into_bytes()]);

        assert_eq!(lines_in, 2);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 2);
        let records = events
            .iter()
            .map(|event| serde_json::from_str::<deja::DejaRecord>(&event.json).unwrap())
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

        let (events, lines_in, dupes, _dropped) = collate(&[chunk.into_bytes()]);

        assert_eq!(lines_in, 2);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 2);
        assert_eq!(correlation_count(&events), 1);
        let records = events
            .iter()
            .map(|event| serde_json::from_str::<deja::DejaRecord>(&event.json).unwrap())
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

        let (events, lines_in, dupes, _dropped) = collate(&[envelope.into_bytes()]);

        assert_eq!(lines_in, 1);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 1);
        let record: deja::DejaRecord = serde_json::from_str(&events[0].json).unwrap();
        match record {
            deja::DejaRecord::GraphNode(node) => {
                assert_eq!(node.node_id, 7);
                assert_eq!(node.global_sequence, 1);
            }
            other => panic!("expected graph node, got {other:?}"),
        }
    }

    #[test]
    fn collate_drops_unknown_envelope_artifact_type() {
        let event = serde_json::json!({
            "recording_run_id": "r1",
            "global_sequence": 1,
            "correlation_id": "c1"
        });
        let envelope = serde_json::json!({
            "schema_version": 2,
            "artifact_type": "unexpected_record_type",
            "instance_id": "router-h-1",
            "event": event
        })
        .to_string();

        let (events, lines_in, dupes, _dropped) = collate(&[envelope.into_bytes()]);

        assert_eq!(lines_in, 1);
        assert_eq!(dupes, 0);
        assert!(events.is_empty());
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

        let (events, lines_in, dupes, _dropped) = collate(&[envelope.into_bytes()]);

        assert_eq!(lines_in, 1);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 1);
        let record: deja::DejaRecord = serde_json::from_str(&events[0].json).unwrap();
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

        let (events, _, _, _) = collate(&[envelope.into_bytes()]);

        assert_eq!(events.len(), 1);
        let record: deja::DejaRecord = serde_json::from_str(&events[0].json).unwrap();
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

        let (events, lines_in, dupes, _dropped) = collate(&[decoded]);

        assert_eq!(lines_in, 2);
        assert_eq!(dupes, 0);
        assert_eq!(events.len(), 2);
    }
}
