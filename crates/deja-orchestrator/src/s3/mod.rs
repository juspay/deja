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
                    if artifact_type.as_deref() == Some("deja_sink_marker") {
                        continue; // loss-accounting records, not events
                    }
                    event.get().to_owned()
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
