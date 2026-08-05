//! Recording ingest: sealed sessions out of S3 (Phase 2.1 + 2.3).
//!
//! The durable form of a recording is the compacted session
//! (`sessions/v1/{id}/` — data parts + correlations index + manifest seal,
//! see `deja-compactor`). Both ingest paths here end in that form: the one that
//! reads a sealed session takes it as given, and the one that scans a raw
//! landing prefix writes it, so a recording is collated once rather than once
//! per run. Pulling a recording means:
//!
//! 1. read the manifest; if the session is unsealed, compact it first
//!    (the record lifecycle's quiesce wait has already settled the landing)
//! 2. stream the data parts (full envelope lines, already deduped + sorted)
//! 3. unwrap envelopes — raw event bytes preserved via `RawValue`, no
//!    reserialization — and re-verify dedup/order by
//!    `(recording_run_id, record_kind, global_sequence)` while materializing
//!    the canonical `events.jsonl` the kernel + renderer read. The kind
//!    belongs in the key: each record kind is numbered in a sequence space of
//!    its own, so a sequence identifies an event only within its kind.
//!
//! (`KeyStamper` occurrences are correlation/address/args-scoped, so
//! dedup+sort cannot perturb lookup stamping.)

use std::io::Write;
use std::path::Path;

pub use deja_compactor::S3Config;
/// How many correlations a sealed recording has, and the per-correlation rows
/// behind that count, WITHOUT pulling the tape — the seal writes the index, so
/// answering costs one manifest GET (plus one sidecar GET for the rows).
pub use deja_compactor::{correlation_count, read_correlation_index, CorrelationSummary};

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
    /// Every other way a line can leave the ingest, counted.
    ///
    /// These exist because a line used to be able to disappear without
    /// appearing anywhere: `lines_in` and `events_out` differed by 97,309 on a
    /// real recording and nothing said where those lines went. A count that
    /// does not balance is the only signal that the ingest is discarding a
    /// record shape it does not understand, so all of them are reported and
    /// [`IngestReport::balances`] asserts they add up.
    pub markers_dropped: usize,
    pub non_envelope_dropped: usize,
    pub unparseable_dropped: usize,
}

impl IngestReport {
    /// Whether every line read is accounted for by exactly one outcome. A
    /// false here means the ingest grew a silent exit.
    pub fn balances(&self) -> bool {
        self.events_out
            + self.duplicates_dropped
            + self.markers_dropped
            + self.non_envelope_dropped
            + self.unparseable_dropped
            == self.lines_in
    }

    /// One line naming where the input went — logged on every ingest, so a
    /// loss is visible in the run's own record rather than inferred later.
    pub fn accounting(&self) -> String {
        format!(
            "ingest: {} line(s) -> {} event(s); dropped {} duplicate(s), {} marker(s), \
             {} non-envelope, {} unparseable{}",
            self.lines_in,
            self.events_out,
            self.duplicates_dropped,
            self.markers_dropped,
            self.non_envelope_dropped,
            self.unparseable_dropped,
            if self.balances() {
                String::new()
            } else {
                format!(
                    " — UNACCOUNTED: {} line(s) left the ingest without being counted",
                    self.lines_in as i64
                        - (self.events_out
                            + self.duplicates_dropped
                            + self.markers_dropped
                            + self.non_envelope_dropped
                            + self.unparseable_dropped) as i64
                )
            }
        )
    }
}

/// Why a line did not become an event. Returned alongside the events so the
/// caller can report every outcome rather than only the successful one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DropCounts {
    pub duplicates: usize,
    pub markers: usize,
    pub non_envelope: usize,
    pub unparseable: usize,
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

/// Envelope shape: the payload is kept as raw bytes.
///
/// The payload's KEY depends on what the envelope carries, and the producer
/// chose those names deliberately: a boundary event is nested under `event`,
/// while an execution-graph node is nested under `node` because it carries its
/// own `recording_run_id` and `global_sequence` that flattening would collide
/// with the envelope's. A probe that knows only `event` therefore reads every
/// graph envelope as having no payload at all.
#[derive(serde::Deserialize)]
struct EnvelopeProbe<'a> {
    #[serde(default)]
    artifact_type: Option<String>,
    #[serde(borrow)]
    event: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow)]
    node: Option<&'a serde_json::value::RawValue>,
}

/// The wire `artifact_type` values, and the `DejaRecord` tag each becomes.
///
/// A marker is loss accounting rather than an event and is counted, not kept.
/// An unset type is the legacy boundary-event envelope.
const ARTIFACT_TYPE_MARKER: &str = "deja_sink_marker";
const ARTIFACT_TYPE_GRAPH_NODE: &str = "deja_graph_node";

impl<'a> EnvelopeProbe<'a> {
    /// The record kind this envelope declares, and the raw payload under
    /// whichever key that kind uses. `None` when the envelope declares a kind
    /// whose payload is absent — a malformed line, not a shape we don't know.
    fn payload(&self) -> Option<(&'static str, &'a serde_json::value::RawValue)> {
        match self.artifact_type.as_deref() {
            Some(ARTIFACT_TYPE_GRAPH_NODE) => self.node.map(|n| ("graph_node", n)),
            // `deja_artifact_record`, and an unset type for legacy envelopes.
            _ => self.event.map(|e| ("boundary_event", e)),
        }
    }

    fn is_marker(&self) -> bool {
        self.artifact_type.as_deref() == Some(ARTIFACT_TYPE_MARKER)
    }
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
    let (events, lines_in, drops) = collate(&[chunk]);

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(dest).map_err(|e| format!("create {}: {e}", dest.display()))?,
    );
    for (_, _, _, line) in &events {
        out.write_all(line.as_bytes())
            .and_then(|_| out.write_all(b"\n"))
            .map_err(|e| format!("write {}: {e}", dest.display()))?;
    }
    out.flush().map_err(|e| format!("flush: {e}"))?;

    let report = IngestReport {
        prefix: deja_compactor::layout::session_root(recording_id),
        landing_objects: manifest.counts.landing_objects,
        lines_in,
        // The manifest's own duplicates were dropped when the session was
        // sealed, so they are outside this pass's line accounting and are
        // reported separately from what collate saw.
        duplicates_dropped: manifest.counts.duplicates_dropped + drops.duplicates,
        events_out: events.len(),
        correlations: manifest.counts.correlations,
        sealed: true,
        markers_dropped: drops.markers,
        non_envelope_dropped: drops.non_envelope,
        unparseable_dropped: drops.unparseable,
    };
    eprintln!("{}", report.accounting());
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
/// The resolved session is SEALED on the way out (`sessions/v1/{id}/`), so this
/// scan happens once per recording rather than once per run — see the sealing
/// block below.
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
    // Objects per session, not just lines: the scanned prefix can hold several
    // sessions (a recording spanning midnight is addressed from the shared
    // parent), so the object count that belongs to the recording is the number
    // of objects that carried a line of it, never the size of the scan.
    let mut objects_by_session: std::collections::BTreeMap<String, usize> = Default::default();
    let mut junk_lines = 0usize;
    for key in &keys {
        let data = deja_compactor::get_object_decoded(cfg, key)?;
        let mut sessions_here: std::collections::BTreeSet<String> = Default::default();
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
                Some(sid) => {
                    sessions_here.insert(sid.clone());
                    by_session.entry(sid).or_default().push(line_str);
                }
                None => junk_lines += 1,
            }
        }
        for sid in sessions_here {
            *objects_by_session.entry(sid).or_default() += 1;
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
    let (events, lines_in, drops) = collate(&[chunk]);

    let dest = dest_for(&resolved);
    let dest = dest.as_path();
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(dest).map_err(|e| format!("create {}: {e}", dest.display()))?,
    );
    let mut correlations = std::collections::HashSet::new();
    for (_, _, _, line) in &events {
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

    // Promote what we just read into the durable form.
    //
    // A raw prefix is re-listed, re-fetched and re-collated on EVERY run — 8 to
    // 17 seconds each time, forever, for a recording that cannot change. The
    // seal is written from the same lines that produced the events file above,
    // so the manifest fast path the next run takes reads exactly what this run
    // read. Only the resolved session is sealed: the others under this prefix
    // were grouped, not ingested, and sealing a session from a scan that was
    // never meant to cover it would put a completeness claim on partial data.
    //
    // A failed seal is not a failed ingest. The events file is written and the
    // landing is untouched, so the next run falls back to this same rescan —
    // slow, but correct. It is reported rather than swallowed, because "still
    // unsealed after an ingest" is the difference between a slow pull and a
    // pull that will be slow forever.
    let session_objects = objects_by_session
        .get(&resolved)
        .copied()
        .unwrap_or_default();
    let sealed = match deja_compactor::seal_session(cfg, &resolved, &lines, session_objects) {
        Ok(manifest) => {
            eprintln!(
                "ingest: sealed {resolved} at s3://{}/{} — {} data part(s), {} correlation(s)",
                cfg.bucket,
                deja_compactor::layout::session_root(&resolved),
                manifest.data_parts.len(),
                manifest.counts.correlations,
            );
            true
        }
        Err(e) => {
            eprintln!(
                "ingest: sealing {resolved} failed ({e}) — this run is unaffected, but the next \
                 pull will rescan s3://{}/{prefix} again",
                cfg.bucket
            );
            false
        }
    };

    let report = IngestReport {
        prefix: format!("s3://{}/{prefix}", cfg.bucket),
        landing_objects: session_objects,
        lines_in,
        duplicates_dropped: drops.duplicates,
        events_out: events.len(),
        correlations: correlations.len(),
        sealed,
        markers_dropped: drops.markers,
        non_envelope_dropped: drops.non_envelope,
        unparseable_dropped: drops.unparseable,
    };
    eprintln!("{}", report.accounting());
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

/// Unwrap envelopes (raw payload bytes preserved), probe the dedup/sort key,
/// drop duplicates and sink markers, sort canonically. Returns the sorted
/// `(recording_run_id, record_kind, global_sequence, raw_event_json)` tuples,
/// the line count, and every reason a line did not become an event.
///
/// The kind is dispatched from `artifact_type` BEFORE the payload is looked
/// for, because which key holds the payload depends on the kind — see
/// [`EnvelopeProbe`]. Requiring `event` first made every graph envelope look
/// like a line that was not an envelope at all, so the graph arm below could
/// never be reached and an entire record type was discarded on the way in.
///
/// The kind is also part of the dedup key, because `global_sequence` is only
/// unique WITHIN a kind: graph nodes are numbered in a sequence space of their
/// own so that boundary-event numbering is identical whether or not graph
/// capture is on, and replay's lookup addressing mirrors that numbering. Both
/// spaces start at zero in one recording, so a key without the kind makes graph
/// node N collide with boundary event N.
#[allow(clippy::type_complexity)]
fn collate(
    raw_chunks: &[Vec<u8>],
) -> (
    Vec<(Option<String>, &'static str, u64, String)>,
    usize,
    DropCounts,
) {
    let mut seen = std::collections::HashSet::new();
    let mut events: Vec<(Option<String>, &'static str, u64, String)> = Vec::new();
    let mut lines_in = 0usize;
    let mut drops = DropCounts::default();
    for chunk in raw_chunks {
        for line in chunk.split(|&b| b == b'\n') {
            if line.iter().all(|b| b.is_ascii_whitespace()) {
                continue;
            }
            lines_in += 1;
            let line_str = String::from_utf8_lossy(line);
            // Landing lines are envelopes; the payload's raw bytes are kept.
            let (record_kind, event_raw): (&'static str, String) =
                match serde_json::from_str::<EnvelopeProbe>(&line_str) {
                    // Loss accounting, not events — counted so the totals balance.
                    Ok(probe) if probe.is_marker() => {
                        drops.markers += 1;
                        continue;
                    }
                    // The canonical events.jsonl is a `DejaRecord` stream, internally
                    // tagged by `record_kind`. The wire envelope's `artifact_type` is
                    // the record kind, but the sink omits the tag from the raw payload
                    // — stamp the matching one as we unwrap so the renderer and kernel
                    // can deserialize the line as a `DejaRecord`.
                    Ok(probe) => match probe.payload() {
                        Some((kind, payload)) => (kind, stamp_record_kind(payload.get(), kind)),
                        None => {
                            drops.non_envelope += 1;
                            continue;
                        }
                    },
                    Err(_) => {
                        drops.non_envelope += 1;
                        continue;
                    }
                };
            let probe: EventProbe = match serde_json::from_str(&event_raw) {
                Ok(p) => p,
                Err(_) => {
                    drops.unparseable += 1;
                    continue;
                }
            };
            if !seen.insert((
                probe.recording_run_id.clone(),
                record_kind,
                probe.global_sequence,
            )) {
                drops.duplicates += 1;
                continue;
            }
            events.push((
                probe.recording_run_id,
                record_kind,
                probe.global_sequence,
                event_raw,
            ));
        }
    }
    // Sequence first so a kind's own order is preserved and boundary-event
    // ordering is unchanged; kind only breaks the tie between the two spaces.
    events.sort_by(|a, b| (&a.0, a.2, a.1).cmp(&(&b.0, b.2, b.1)));
    (events, lines_in, drops)
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
        let (events, lines_in, drops) = collate(&[obj1.into_bytes(), obj2.into_bytes()]);
        assert_eq!(lines_in, 6);
        assert_eq!(drops.duplicates, 1);
        assert_eq!(drops.markers, 1);
        assert_eq!(drops.non_envelope, 1); // the `not-json` line
        let gseqs: Vec<u64> = events.iter().map(|(_, _, g, _)| *g).collect();
        assert_eq!(gseqs, vec![1, 2, 3]);
        // Raw event bytes preserved verbatim (no key reordering).
        assert!(events[0].3.contains(r#""global_sequence":1,"k":"a""#));
    }

    /// A graph-node envelope EXACTLY as the recorded system emits it.
    ///
    /// The payload is under `node`, not `event`, and the schema version is 1 —
    /// both copied from the producer (`GraphEnvelope` in the router's deja
    /// record sink), not invented here. An earlier version of this helper used
    /// `event`, which is why a test suite could pass while every graph node in
    /// production was discarded: the fixture described a wire shape nothing
    /// ever wrote.
    fn graph_envelope(rid: &str, gseq: u64) -> String {
        format!(
            r#"{{"schema_version":1,"artifact_type":"deja_graph_node","instance_id":"router-h-1","recording_run_id":"{rid}","capture":{{"mode":"session","session_id":"{rid}"}},"node":{{"recording_run_id":"{rid}","global_sequence":{gseq},"node_id":{gseq},"span_name":"payments_create"}}}}"#
        )
    }

    #[test]
    fn a_graph_envelope_is_kept_even_though_its_payload_is_not_under_event() {
        // The defect this pins: the probe looked only for `event`, so a graph
        // envelope — whose payload the producer nests under `node`, precisely
        // because the node carries its own recording_run_id and global_sequence
        // that flattening would collide with — parsed as an envelope with no
        // payload and was discarded as "not an envelope" before its
        // artifact_type was ever consulted.
        let chunk = format!("{}\n", graph_envelope("r1", 5)).into_bytes();
        let (events, lines_in, drops) = collate(&[chunk]);
        assert_eq!(lines_in, 1);
        assert_eq!(
            drops.non_envelope, 0,
            "a graph envelope is an envelope; it must not be dropped as junk"
        );
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1, "graph_node");
        assert_eq!(events[0].2, 5);
        assert!(events[0].3.contains(r#""record_kind":"graph_node""#));
        // The node's own payload survives verbatim, not the envelope's copy.
        assert!(events[0].3.contains(r#""span_name":"payments_create""#));
    }

    #[test]
    fn every_line_read_is_accounted_for() {
        // The balance property. A line may become an event or be dropped for a
        // named reason, and nothing else. This is the check that would have
        // surfaced 97,309 vanishing lines on a real recording instead of
        // leaving them to be inferred from a graph that never rendered.
        let chunk = format!(
            "{}\n{}\n{}\n{}\n{}\nnot-json-at-all\n{}\n",
            envelope("r1", 1, ""),
            envelope("r1", 1, ""), // duplicate
            graph_envelope("r1", 1),
            graph_envelope("r1", 1), // duplicate
            r#"{"artifact_type":"deja_sink_marker","recording_run_id":"r1"}"#,
            r#"{"artifact_type":"deja_artifact_record","instance_id":"h"}"#, // no payload
        )
        .into_bytes();
        let (events, lines_in, drops) = collate(&[chunk]);
        assert_eq!(lines_in, 7);
        assert_eq!(events.len(), 2);
        assert_eq!(drops.duplicates, 2);
        assert_eq!(drops.markers, 1);
        assert_eq!(drops.non_envelope, 2); // the junk line and the payload-less envelope

        let report = IngestReport {
            prefix: "s3://b/p".into(),
            landing_objects: 1,
            lines_in,
            duplicates_dropped: drops.duplicates,
            events_out: events.len(),
            correlations: 0,
            sealed: false,
            markers_dropped: drops.markers,
            non_envelope_dropped: drops.non_envelope,
            unparseable_dropped: drops.unparseable,
        };
        assert!(report.balances(), "{}", report.accounting());
        assert!(!report.accounting().contains("UNACCOUNTED"));
    }

    #[test]
    fn an_unbalanced_report_says_so() {
        // The assertion has to be able to fail, or it is decoration.
        let report = IngestReport {
            prefix: "s3://b/p".into(),
            landing_objects: 1,
            lines_in: 139_916,
            duplicates_dropped: 0,
            events_out: 42_607,
            correlations: 3,
            sealed: false,
            markers_dropped: 0,
            non_envelope_dropped: 0,
            unparseable_dropped: 0,
        };
        assert!(!report.balances());
        assert!(report.accounting().contains("UNACCOUNTED: 97309"));
    }

    #[test]
    fn a_graph_node_does_not_displace_the_boundary_event_of_the_same_sequence() {
        // Graph nodes are numbered in a sequence space of their own so that
        // boundary-event numbering is identical whether or not graph capture is
        // on. Both spaces therefore start at zero in one recording, and a key
        // that is only `(recording, sequence)` treats node N and event N as the
        // same record — silently discarding one of them. With boundary events
        // covering the whole range that drops the entire graph, which is what
        // left the record side of the execution graph empty.
        let chunk = format!(
            "{}\n{}\n{}\n{}\n",
            envelope("r1", 0, r#","k":"a""#),
            envelope("r1", 1, r#","k":"b""#),
            graph_envelope("r1", 0),
            graph_envelope("r1", 1),
        )
        .into_bytes();
        let (events, _, drops) = collate(&[chunk]);
        assert_eq!(
            drops.duplicates, 0,
            "no record may be dropped as a duplicate here"
        );
        assert_eq!(events.len(), 4);

        let kinds: Vec<&str> = events.iter().map(|(_, kind, _, _)| *kind).collect();
        assert_eq!(kinds.iter().filter(|k| **k == "graph_node").count(), 2);
        assert_eq!(kinds.iter().filter(|k| **k == "boundary_event").count(), 2);

        // Each kind keeps its own order, and the stamped tag makes the line
        // deserializable as the right `DejaRecord` variant.
        let graph: Vec<&String> = events
            .iter()
            .filter(|(_, kind, _, _)| *kind == "graph_node")
            .map(|(_, _, _, line)| line)
            .collect();
        assert!(graph
            .iter()
            .all(|l| l.contains(r#""record_kind":"graph_node""#)));
    }

    #[test]
    fn a_true_duplicate_within_one_kind_is_still_dropped() {
        let chunk = format!(
            "{}\n{}\n{}\n{}\n",
            envelope("r1", 7, ""),
            envelope("r1", 7, ""),
            graph_envelope("r1", 7),
            graph_envelope("r1", 7),
        )
        .into_bytes();
        let (events, _, drops) = collate(&[chunk]);
        assert_eq!(drops.duplicates, 2);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn collate_keeps_distinct_runs_apart() {
        let chunks =
            vec![format!("{}\n{}\n", envelope("r2", 1, ""), envelope("r1", 1, "")).into_bytes()];
        let (events, _, drops) = collate(&chunks);
        assert_eq!(drops.duplicates, 0);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0.as_deref(), Some("r1")); // sorted by (rid, gseq, kind)
    }
}
