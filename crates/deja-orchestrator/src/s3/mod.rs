//! Recording ingest: sealed sessions out of S3 (Phase 2.1 + 2.3).
//!
//! The durable form of a recording is the compacted session
//! (`sessions/v1/{id}/` — data parts + correlations index + manifest seal,
//! see `deja-compactor`). Both ingest paths here READ; neither writes. A replay
//! is judged against a recording, so it holds `s3:PutObject` on `replay-runs/*`
//! and nothing else — it cannot alter the tape store, by policy and on purpose.
//! Producing the durable form is a compaction job's work, not a replay's.
//!
//! Pulling a recording means:
//!
//! 1. read the manifest; when the session is not sealed yet, fall back to
//!    scanning its landing prefix — slower every run, and correct every run,
//!    until a compaction job promotes it
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
    ///
    /// `markers_dropped` is the count of marker lines, which is the term the
    /// balance needs — a marker is not a `DejaRecord` and never joins the event
    /// stream. It is no longer the whole story: what those lines SAID is read
    /// into [`IngestReport::delivery`] rather than thrown away.
    pub markers_dropped: usize,
    pub non_envelope_dropped: usize,
    pub unparseable_dropped: usize,
    /// Whether the recording had stopped growing when this pull read it.
    ///
    /// A replay is scored against the tape it was handed. Reading a recording
    /// that is still landing scores a PREFIX of it, and the verdict looks
    /// exactly like a complete one — run
    /// `rp-sbx-1868bb92e9-1868bb9-08120623-rk-0812064511654` scored 9/44 on a
    /// recording that later reported 87 correlations. This is the fact that
    /// says which happened; acting on it is the lifecycle's job.
    pub completeness: Completeness,
    /// What the sink's own markers, plus the event stream's per-correlation
    /// ordering, establish about delivery.
    pub delivery: DeliveryCertificate,
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

// -- delivery accounting -----------------------------------------------------

/// Whether more of this recording could still arrive.
///
/// `sealed` alone cannot answer this. A replay holds `s3:PutObject` on
/// `replay-runs/*` and nothing else, so it may not seal what it reads: every
/// prefix-scan pull reports `sealed: false`, including pulls of recordings that
/// finished hours ago. Gating on it would refuse the normal path.
///
/// The recorder answers it instead. The writer's `eof` marker is emitted on
/// shutdown after a real producer drain — "everything before this landed" — so
/// a recording that has stopped growing has one per producer instance, and a
/// recording still being written has none. The record lifecycle stops the
/// router before it waits for the landing precisely so the `eof` is in the
/// prefix before anything reads it.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum Completeness {
    /// A compaction job promoted this into a sealed session. The seal is
    /// terminal, and it is taken on trust here for the same reason the manifest
    /// is: the compactor is the component allowed to make it.
    Sealed,
    /// Not sealed, but every producer instance that spoke also said goodbye.
    EofObserved { instances: usize },
    /// Nothing here proves the recording stopped growing. Scoring a replay
    /// against it scores a prefix.
    StillLanding { reason: String },
}

impl Completeness {
    /// Whether the recording can be shown to have stopped growing.
    pub fn is_complete(&self) -> bool {
        !matches!(self, Completeness::StillLanding { .. })
    }

    /// Why not, when not.
    pub fn incomplete_reason(&self) -> Option<&str> {
        match self {
            Completeness::StillLanding { reason } => Some(reason),
            _ => None,
        }
    }

    /// One line naming the state and how it was established.
    pub fn describe(&self) -> String {
        match self {
            Completeness::Sealed => "sealed session — the recording is final".to_owned(),
            Completeness::EofObserved { instances } => format!(
                "complete — {instances} producer instance(s) emitted their `eof` marker, so \
                 nothing more is coming"
            ),
            Completeness::StillLanding { reason } => format!("INCOMPLETE — {reason}"),
        }
    }
}

/// What delivery can be established for this recording.
///
/// Two independent sources, and it matters which says what:
///
/// * The EVENT STREAM proves per-correlation continuity. `request_sequence` is
///   a dense per-correlation counter allocated in `next_request_sequence`, and
///   a sampled-out or sink-down boundary no-ops BEFORE any sequence is
///   allocated (`capture_verdict`), so within a correlation the recorder emits
///   0, 1, 2, … with no legitimate holes. A hole is loss.
/// * The MARKERS prove nothing per-correlation — they carry no `correlation_id`
///   and their sequence space is `global_sequence`, not `request_sequence`. What
///   they carry is the producer's own claim: which `global_sequence` ranges it
///   knows it dropped, how many records it wrote, and whether it finished.
///
/// So the certificate asserts continuity from the events and uses the markers
/// to say whether a break is loss the producer ADMITS (a `dropped` marker
/// covering it) or loss nobody admits — which is worse, because it means the
/// records left the writer and went missing in transport.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct DeliveryCertificate {
    /// Marker lines read (equal to `IngestReport::markers_dropped`).
    pub markers_seen: usize,
    /// Marker lines whose body could not be read. Counted rather than ignored:
    /// an unreadable marker is a marker whose claim is lost.
    pub markers_unreadable: usize,
    pub checkpoints: usize,
    /// One row per producer instance that emitted a marker.
    pub instances: Vec<InstanceDelivery>,
    /// Correlations whose `request_sequence` run was checked.
    pub correlations_checked: usize,
    /// Correlations whose run had a hole, and which sequence numbers are gone.
    pub gaps: Vec<DeliveryGap>,
}

impl DeliveryCertificate {
    /// Whether every checked correlation's `request_sequence` run is contiguous
    /// from zero — the property the certificate exists to assert.
    pub fn continuous(&self) -> bool {
        self.gaps.is_empty()
    }

    /// One line naming what the markers and the event stream established.
    pub fn describe(&self) -> String {
        let admitted: usize = self
            .gaps
            .iter()
            .filter(|g| g.explained_by_sink_drop)
            .count();
        let unexplained = self.gaps.len() - admitted;
        format!(
            "delivery: {} marker(s) consumed ({} checkpoint(s), {} instance(s), {} unreadable); \
             {} correlation(s) checked for request_sequence continuity, {} gap(s) ({} admitted by \
             a `dropped` marker, {} unexplained)",
            self.markers_seen,
            self.checkpoints,
            self.instances.len(),
            self.markers_unreadable,
            self.correlations_checked,
            self.gaps.len(),
            admitted,
            unexplained,
        )
    }
}

/// What one producer instance's markers claim.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize)]
pub struct InstanceDelivery {
    pub instance_id: String,
    /// The writer emitted its shutdown marker: everything before it landed.
    pub eof: bool,
    /// Highest `global_sequence` the writer claims to have written.
    pub last_seq: Option<u64>,
    pub records_written: Option<u64>,
    pub records_dropped: Option<u64>,
    /// Inclusive `global_sequence` ranges the writer KNOWS it lost — fail-open
    /// enqueue drops and batches lost to a transient sink failure.
    pub dropped_ranges: Vec<[u64; 2]>,
}

/// A correlation whose `request_sequence` run is not contiguous: the recorder
/// allocated these numbers and their events never reached the tape.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct DeliveryGap {
    pub correlation_id: String,
    /// The `request_sequence` values that never arrived.
    pub missing: Vec<u64>,
    /// The `global_sequence` span of the events that DID arrive for this
    /// correlation — the window a `dropped` marker has to overlap to explain
    /// the hole.
    pub gseq_span: [u64; 2],
    /// A `dropped` marker overlaps that window, so the producer itself
    /// accounted for this loss. False means nothing did: the records were
    /// written and then lost downstream.
    pub explained_by_sink_drop: bool,
}

/// The markers of one pull, folded per producer instance.
#[derive(Debug, Clone, Default)]
struct MarkerLedger {
    seen: usize,
    unreadable: usize,
    checkpoints: usize,
    by_instance: std::collections::BTreeMap<String, InstanceDelivery>,
}

impl MarkerLedger {
    fn record(&mut self, line: &str) {
        self.seen += 1;
        let Ok(probe) = serde_json::from_str::<MarkerProbe>(line) else {
            self.unreadable += 1;
            return;
        };
        let Some(body) = probe.marker else {
            // A `deja_sink_marker` envelope with no `marker` body claims
            // nothing. Counting it as read would inflate the audit trail.
            self.unreadable += 1;
            return;
        };
        let instance_id = probe.instance_id.unwrap_or_else(|| "unknown".to_owned());
        let entry = self
            .by_instance
            .entry(instance_id.clone())
            .or_insert_with(|| InstanceDelivery {
                instance_id,
                ..Default::default()
            });
        match body.kind.as_str() {
            "checkpoint" => {
                self.checkpoints += 1;
                fold_totals(entry, &body);
            }
            "eof" => {
                entry.eof = true;
                fold_totals(entry, &body);
            }
            "dropped" => entry.dropped_ranges.extend(body.ranges.iter().copied()),
            // A kind this build does not know is still a marker line, and
            // pretending to have read it would be the defect being fixed.
            _ => self.unreadable += 1,
        }
    }

    /// What the markers say about the recording having stopped growing.
    fn completeness(&self) -> Completeness {
        if self.by_instance.is_empty() {
            return Completeness::StillLanding {
                reason: format!(
                    "no readable sink marker under this prefix ({} marker line(s), {} unreadable) \
                     — the recorder's `eof` stamp is what says a recording stopped growing, and \
                     none arrived",
                    self.seen, self.unreadable
                ),
            };
        }
        let silent: Vec<&str> = self
            .by_instance
            .values()
            .filter(|i| !i.eof)
            .map(|i| i.instance_id.as_str())
            .collect();
        if silent.is_empty() {
            return Completeness::EofObserved {
                instances: self.by_instance.len(),
            };
        }
        Completeness::StillLanding {
            reason: format!(
                "instance(s) {} emitted checkpoints but no `eof` — that writer has not shut down \
                 and drained, so this prefix is a snapshot of a recording still being written",
                silent.join(", ")
            ),
        }
    }

    fn into_instances(self) -> Vec<InstanceDelivery> {
        self.by_instance.into_values().collect()
    }
}

/// Monotonic counters: the highest value seen is the latest, whatever order the
/// markers arrived in after a dedup+sort.
fn fold_totals(entry: &mut InstanceDelivery, body: &MarkerBody) {
    entry.last_seq = entry.last_seq.max(body.last_seq);
    entry.records_written = entry.records_written.max(body.records_written);
    entry.records_dropped = entry.records_dropped.max(body.records_dropped);
}

/// Build the certificate: continuity from the events, cause from the markers.
fn certify(
    per_correlation: std::collections::BTreeMap<String, Vec<(u64, u64)>>,
    markers: MarkerLedger,
) -> DeliveryCertificate {
    let dropped_ranges: Vec<[u64; 2]> = markers
        .by_instance
        .values()
        .flat_map(|i| i.dropped_ranges.iter().copied())
        .collect();

    let correlations_checked = per_correlation.len();
    let mut gaps = Vec::new();
    for (correlation_id, mut seen) in per_correlation {
        seen.sort_unstable();
        let highest = seen.last().map(|(rseq, _)| *rseq).unwrap_or_default();
        let present: std::collections::BTreeSet<u64> = seen.iter().map(|(rseq, _)| *rseq).collect();
        // The recorder starts every correlation at 0 and increments by one, so
        // the run it emitted is exactly `0..=highest`. Anything in that range
        // that did not arrive was allocated and then lost.
        let missing: Vec<u64> = (0..=highest).filter(|r| !present.contains(r)).collect();
        if missing.is_empty() {
            continue;
        }
        let gseq_lo = seen.iter().map(|(_, g)| *g).min().unwrap_or_default();
        let gseq_hi = seen.iter().map(|(_, g)| *g).max().unwrap_or_default();
        let explained_by_sink_drop = dropped_ranges
            .iter()
            .any(|[from, to]| *from <= gseq_hi && *to >= gseq_lo);
        gaps.push(DeliveryGap {
            correlation_id,
            missing,
            gseq_span: [gseq_lo, gseq_hi],
            explained_by_sink_drop,
        });
    }

    DeliveryCertificate {
        markers_seen: markers.seen,
        markers_unreadable: markers.unreadable,
        checkpoints: markers.checkpoints,
        correlations_checked,
        gaps,
        instances: markers.into_instances(),
    }
}

/// Minimal probe of an event for identity (dedup/sort key) plus the two fields
/// the delivery certificate is computed from — everything else stays raw.
#[derive(serde::Deserialize)]
struct EventProbe {
    #[serde(default)]
    recording_run_id: Option<String>,
    #[serde(default)]
    global_sequence: u64,
    /// Absent on graph nodes and on ambient (uncorrelated) traffic. Both are
    /// excluded from continuity: a graph node is numbered in another space
    /// entirely, and an uncorrelated event's `request_sequence` comes from a
    /// process-wide counter rather than a per-correlation one, so folding
    /// either in would invent gaps.
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    request_sequence: u64,
}

/// A sink marker, as the record sink puts it on the wire: the payload the
/// writer built is flattened under `marker` alongside its `kind`.
///
/// Note where the body is. It is not under `event`, and it is not at the top
/// level — the same producer/consumer split that made every graph envelope
/// (payload under `node`) read as "not an envelope at all". These shapes are
/// copied from the sink's `MarkerEnvelope`, not invented here.
#[derive(serde::Deserialize)]
struct MarkerProbe {
    #[serde(default)]
    instance_id: Option<String>,
    #[serde(default)]
    marker: Option<MarkerBody>,
}

#[derive(serde::Deserialize)]
struct MarkerBody {
    #[serde(default)]
    kind: String,
    /// `checkpoint` / `eof`: the writer's own progress claim.
    #[serde(default)]
    last_seq: Option<u64>,
    #[serde(default)]
    records_written: Option<u64>,
    #[serde(default)]
    records_dropped: Option<u64>,
    /// `dropped`: inclusive `global_sequence` ranges the writer lost.
    #[serde(default)]
    ranges: Vec<[u64; 2]>,
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
    let collated = collate(&[chunk]);

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(dest).map_err(|e| format!("create {}: {e}", dest.display()))?,
    );
    for (_, _, _, line) in &collated.events {
        out.write_all(line.as_bytes())
            .and_then(|_| out.write_all(b"\n"))
            .map_err(|e| format!("write {}: {e}", dest.display()))?;
    }
    out.flush().map_err(|e| format!("flush: {e}"))?;

    let report = IngestReport {
        prefix: deja_compactor::layout::session_root(recording_id),
        landing_objects: manifest.counts.landing_objects,
        lines_in: collated.lines_in,
        // The manifest's own duplicates were dropped when the session was
        // sealed, so they are outside this pass's line accounting and are
        // reported separately from what collate saw.
        duplicates_dropped: manifest.counts.duplicates_dropped + collated.drops.duplicates,
        events_out: collated.events.len(),
        correlations: manifest.counts.correlations,
        sealed: true,
        markers_dropped: collated.drops.markers,
        non_envelope_dropped: collated.drops.non_envelope,
        unparseable_dropped: collated.drops.unparseable,
        // The seal is terminal, so this recording has stopped growing whatever
        // the markers say. They will say nothing: the compactor skips marker
        // lines when it builds a session, so a sealed session carries no audit
        // trail and the certificate below rests on the event stream alone.
        completeness: Completeness::Sealed,
        delivery: certify(collated.per_correlation, collated.markers),
    };
    eprintln!("{}", report.accounting());
    eprintln!("{}", report.delivery.describe());
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
    let collated = collate(&[chunk]);

    let dest = dest_for(&resolved);
    let dest = dest.as_path();
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    let mut out = std::io::BufWriter::new(
        std::fs::File::create(dest).map_err(|e| format!("create {}: {e}", dest.display()))?,
    );
    for (_, _, _, line) in &collated.events {
        out.write_all(line.as_bytes())
            .and_then(|_| out.write_all(b"\n"))
            .map_err(|e| format!("write {}: {e}", dest.display()))?;
    }
    out.flush().map_err(|e| format!("flush: {e}"))?;

    // A replay READS the tape store and never writes it.
    //
    // Promoting this landing prefix into a sealed session used to happen right
    // here, from these same lines. It is gone, and not for tidiness: the replay
    // Job's role is deliberately least-privilege — `s3:PutObject` on
    // `replay-runs/*` and nothing else, because a replay must not be able to
    // alter the recordings it is judged against. A seal writes `sessions/v1/*`,
    // so every attempt was denied, and the denial did not fail fast — the
    // object store retried with backoff, several multipart pieces at a time,
    // turning a read into an hour-long hang that the watch deadline eventually
    // killed. One recording spent 3,589s here and never reached stage 2.
    //
    // Sealing belongs to a job that is ALLOWED to write tapes, run on its own
    // schedule against recordings that are no longer being written to. Until
    // that job seals a recording, this rescan is what a replay uses: slower,
    // but correct, and it cannot hang on a permission it was never given.
    let session_objects = objects_by_session
        .get(&resolved)
        .copied()
        .unwrap_or_default();

    // The markers answer "has this recording stopped growing" BEFORE they are
    // folded into the certificate, because that question decides whether the
    // run may report a verdict at all.
    let completeness = collated.markers.completeness();
    let report = IngestReport {
        prefix: format!("s3://{}/{prefix}", cfg.bucket),
        landing_objects: session_objects,
        lines_in: collated.lines_in,
        duplicates_dropped: collated.drops.duplicates,
        events_out: collated.events.len(),
        correlations: collated.per_correlation.len(),
        // This pull read a landing prefix. It is sealed only if a compaction job
        // has already promoted it, which a replay neither does nor may do — so
        // this is NOT the completeness signal; `completeness` is.
        sealed: false,
        markers_dropped: collated.drops.markers,
        non_envelope_dropped: collated.drops.non_envelope,
        unparseable_dropped: collated.drops.unparseable,
        completeness,
        delivery: certify(collated.per_correlation, collated.markers),
    };
    eprintln!("{}", report.accounting());
    eprintln!("{}", report.delivery.describe());
    eprintln!("ingest: {}", report.completeness.describe());
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
fn collate(raw_chunks: &[Vec<u8>]) -> Collated {
    let mut seen = std::collections::HashSet::new();
    let mut events: Vec<(Option<String>, &'static str, u64, String)> = Vec::new();
    let mut per_correlation: std::collections::BTreeMap<String, Vec<(u64, u64)>> =
        Default::default();
    let mut markers = MarkerLedger::default();
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
                    // Loss accounting, not events. Still counted, because the
                    // line balance needs the term — but READ, because what a
                    // marker says is the recording's own audit trail and the
                    // only producer-side account of what it lost.
                    Ok(probe) if probe.is_marker() => {
                        drops.markers += 1;
                        markers.record(&line_str);
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
            // AFTER the dedup check, so a correlation's run is the events that
            // actually reached the tape rather than the lines that mentioned
            // them. Boundary events only — see `EventProbe::correlation_id`.
            if record_kind == "boundary_event" {
                if let Some(correlation_id) = &probe.correlation_id {
                    per_correlation
                        .entry(correlation_id.clone())
                        .or_default()
                        .push((probe.request_sequence, probe.global_sequence));
                }
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
    Collated {
        events,
        per_correlation,
        lines_in,
        drops,
        markers,
    }
}

/// Everything ONE pass over the landing lines produces.
///
/// One pass, not two: the correlation index and the marker ledger are computed
/// beside the event stream from the same lines, because a second pass over the
/// same input is exactly the producer/consumer split that keeps failing here.
struct Collated {
    events: Vec<(Option<String>, &'static str, u64, String)>,
    /// `correlation_id -> [(request_sequence, global_sequence)]` for the
    /// boundary events that survived dedup.
    per_correlation: std::collections::BTreeMap<String, Vec<(u64, u64)>>,
    lines_in: usize,
    drops: DropCounts,
    markers: MarkerLedger,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn envelope(rid: &str, gseq: u64, payload_extra: &str) -> String {
        format!(
            r#"{{"schema_version":2,"artifact_type":"deja_artifact_record","instance_id":"router-h-1","event":{{"recording_run_id":"{rid}","global_sequence":{gseq}{payload_extra}}}}}"#
        )
    }

    /// A boundary event carrying the two fields continuity is computed from.
    fn correlated(instance: &str, gseq: u64, correlation: &str, rseq: u64) -> String {
        format!(
            r#"{{"schema_version":2,"artifact_type":"deja_artifact_record","instance_id":"{instance}","event":{{"recording_run_id":"r1","global_sequence":{gseq},"correlation_id":"{correlation}","request_sequence":{rseq}}}}}"#
        )
    }

    /// A sink marker EXACTLY as the record sink emits it.
    ///
    /// The body is under `marker` — `{kind, ...flattened payload}` — copied
    /// from the producer's `MarkerEnvelope`, not invented here. This repo has
    /// already paid once for a fixture that described a wire shape nothing
    /// wrote (the graph envelope below), so the marker fixtures are the
    /// producer's shape or they are worthless.
    fn marker_envelope(instance: &str, body: &str) -> String {
        format!(
            r#"{{"schema_version":2,"artifact_type":"deja_sink_marker","instance_id":"{instance}","recording_run_id":"r1","capture":{{"mode":"session","session_id":"r1"}},"marker":{body}}}"#
        )
    }

    fn checkpoint(instance: &str, last_seq: u64) -> String {
        marker_envelope(
            instance,
            &format!(
                r#"{{"kind":"checkpoint","last_seq":{last_seq},"records_written":{last_seq},"records_dropped":0}}"#
            ),
        )
    }

    fn eof(instance: &str, last_seq: u64) -> String {
        marker_envelope(
            instance,
            &format!(
                r#"{{"kind":"eof","last_seq":{last_seq},"records_written":{last_seq},"records_dropped":0}}"#
            ),
        )
    }

    fn dropped(instance: &str, from: u64, to: u64) -> String {
        marker_envelope(
            instance,
            &format!(r#"{{"kind":"dropped","ranges":[[{from},{to}]]}}"#),
        )
    }

    #[test]
    fn collate_unwraps_dedups_and_sorts() {
        // Two objects, out-of-order gseq, one duplicate across objects, one
        // sink marker, one junk line.
        let obj1 = format!(
            "{}\n{}\n{}\n",
            envelope("r1", 3, r#","k":"c""#),
            envelope("r1", 1, r#","k":"a""#),
            checkpoint("router-h-1", 3),
        );
        let obj2 = format!(
            "{}\n{}\nnot-json\n",
            envelope("r1", 1, r#","k":"a""#), // duplicate of obj1's gseq 1
            envelope("r1", 2, r#","k":"b""#),
        );
        let Collated {
            events,
            lines_in,
            drops,
            ..
        } = collate(&[obj1.into_bytes(), obj2.into_bytes()]);
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
        let Collated {
            events,
            lines_in,
            drops,
            ..
        } = collate(&[chunk]);
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
            eof("router-h-1", 1),
            r#"{"artifact_type":"deja_artifact_record","instance_id":"h"}"#, // no payload
        )
        .into_bytes();
        let Collated {
            events,
            lines_in,
            drops,
            ..
        } = collate(&[chunk]);
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
            completeness: Completeness::Sealed,
            delivery: DeliveryCertificate::default(),
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
            completeness: Completeness::Sealed,
            delivery: DeliveryCertificate::default(),
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
        let Collated { events, drops, .. } = collate(&[chunk]);
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
        let Collated { events, drops, .. } = collate(&[chunk]);
        assert_eq!(drops.duplicates, 2);
        assert_eq!(events.len(), 2);
    }

    // -- defect 2: markers are consumed, not discarded ----------------------

    #[test]
    fn a_marker_is_read_rather_than_only_counted() {
        // The defect: ingest counted 83 marker lines on a real recording and
        // threw every one away, so nothing could establish that a correlation's
        // events arrived intact. The count still exists — the line balance
        // needs it — but the CLAIM each marker carries now lands somewhere.
        let chunk = format!(
            "{}\n{}\n{}\n{}\n",
            correlated("router-h-1", 0, "c-1", 0),
            checkpoint("router-h-1", 0),
            dropped("router-h-1", 40, 41),
            eof("router-h-1", 7),
        )
        .into_bytes();
        let collated = collate(&[chunk]);
        assert_eq!(collated.drops.markers, 3, "still counted for the balance");

        let cert = certify(collated.per_correlation, collated.markers);
        assert_eq!(cert.markers_seen, 3);
        assert_eq!(cert.markers_unreadable, 0, "the producer's shape parses");
        assert_eq!(cert.checkpoints, 1);
        assert_eq!(cert.instances.len(), 1);

        let instance = &cert.instances[0];
        assert_eq!(instance.instance_id, "router-h-1");
        assert!(instance.eof, "the eof marker was read, not discarded");
        // Monotonic counters fold to their highest value, so the eof's totals
        // win over the earlier checkpoint's whatever order they arrived in.
        assert_eq!(instance.last_seq, Some(7));
        assert_eq!(instance.records_written, Some(7));
        assert_eq!(instance.dropped_ranges, vec![[40, 41]]);
    }

    #[test]
    fn a_marker_whose_body_cannot_be_read_is_named_not_assumed_fine() {
        // A marker line whose claim is unreadable is a LOST claim. Counting it
        // as read would be the same silent-drop shape one layer up.
        let chunk = format!(
            "{}\n{}\n{}\n",
            r#"{"artifact_type":"deja_sink_marker","instance_id":"h","recording_run_id":"r1"}"#,
            marker_envelope("h", r#"{"kind":"a_kind_this_build_never_heard_of"}"#),
            eof("h", 1),
        )
        .into_bytes();
        let collated = collate(&[chunk]);
        let cert = certify(collated.per_correlation, collated.markers);
        assert_eq!(cert.markers_seen, 3);
        assert_eq!(
            cert.markers_unreadable, 2,
            "a body-less marker and an unknown kind each lose a claim"
        );
    }

    #[test]
    fn a_correlation_missing_a_request_sequence_is_a_named_gap() {
        // THE continuity property. `request_sequence` is a dense per-correlation
        // counter (`next_request_sequence`), and a sampled-out or sink-down
        // boundary no-ops BEFORE any sequence is allocated (`capture_verdict`),
        // so 0,1,3 means the event numbered 2 was allocated and then lost.
        let chunk = format!(
            "{}\n{}\n{}\n",
            correlated("router-h-1", 10, "c-1", 0),
            correlated("router-h-1", 11, "c-1", 1),
            correlated("router-h-1", 13, "c-1", 3),
        )
        .into_bytes();
        let collated = collate(&[chunk]);
        let cert = certify(collated.per_correlation, collated.markers);

        assert_eq!(cert.correlations_checked, 1);
        assert!(!cert.continuous(), "a hole must not pass as intact");
        assert_eq!(cert.gaps.len(), 1);
        assert_eq!(cert.gaps[0].correlation_id, "c-1");
        assert_eq!(cert.gaps[0].missing, vec![2]);
        assert_eq!(cert.gaps[0].gseq_span, [10, 13]);
        assert!(
            !cert.gaps[0].explained_by_sink_drop,
            "no `dropped` marker covers it — the producer does not admit this loss"
        );
        assert!(cert.describe().contains("1 unexplained"));
    }

    #[test]
    fn a_gap_the_writer_admits_is_told_apart_from_one_nobody_admits() {
        // The markers cannot prove continuity — they carry no correlation_id
        // and their sequence space is global_sequence. What they CAN do is say
        // whether the producer accounted for the loss. A hole covered by a
        // `dropped` marker is loss the writer admits (fail-open backpressure);
        // a hole nobody admits means the records left the writer and vanished
        // in transport, which is the worse of the two.
        let chunk = format!(
            "{}\n{}\n{}\n{}\n{}\n",
            correlated("router-h-1", 10, "c-admitted", 0),
            correlated("router-h-1", 12, "c-admitted", 2),
            correlated("router-h-1", 30, "c-silent", 0),
            correlated("router-h-1", 32, "c-silent", 2),
            dropped("router-h-1", 11, 11),
        )
        .into_bytes();
        let collated = collate(&[chunk]);
        let cert = certify(collated.per_correlation, collated.markers);

        assert_eq!(cert.gaps.len(), 2);
        let admitted = cert
            .gaps
            .iter()
            .find(|g| g.correlation_id == "c-admitted")
            .expect("admitted gap");
        assert_eq!(admitted.missing, vec![1]);
        assert!(
            admitted.explained_by_sink_drop,
            "gseq 11 sits inside the correlation's span and the writer ledgered it"
        );

        let silent = cert
            .gaps
            .iter()
            .find(|g| g.correlation_id == "c-silent")
            .expect("silent gap");
        assert!(
            !silent.explained_by_sink_drop,
            "the dropped range is nowhere near this correlation's span"
        );
        assert!(cert.describe().contains("1 admitted"));
    }

    #[test]
    fn ambient_and_graph_records_do_not_invent_gaps() {
        // Uncorrelated traffic numbers its `request_sequence` from a
        // process-wide counter, and a graph node has neither field. Folding
        // either into the continuity check would manufacture holes in a
        // recording that lost nothing.
        let chunk = format!(
            "{}\n{}\n{}\n{}\n",
            correlated("router-h-1", 0, "c-1", 0),
            correlated("router-h-1", 1, "c-1", 1),
            envelope("r1", 2, r#","request_sequence":97"#), // ambient: no correlation
            graph_envelope("r1", 0),
        )
        .into_bytes();
        let collated = collate(&[chunk]);
        let cert = certify(collated.per_correlation, collated.markers);
        assert_eq!(cert.correlations_checked, 1, "only the real correlation");
        assert!(cert.continuous(), "{:?}", cert.gaps);
    }

    // -- defect 1: an unsealed recording cannot pass as complete -------------

    #[test]
    fn no_eof_marker_means_the_recording_may_still_be_landing() {
        // The observed failure: a replay read a prefix of a tape still being
        // written and reported 9/44 as though it were the whole recording. The
        // writer's `eof` is what says a recording stopped growing; checkpoints
        // alone say the opposite — it was still flushing when this was read.
        let chunk = format!(
            "{}\n{}\n",
            correlated("router-h-1", 0, "c-1", 0),
            checkpoint("router-h-1", 0),
        )
        .into_bytes();
        let collated = collate(&[chunk]);
        let completeness = collated.markers.completeness();

        assert!(
            !completeness.is_complete(),
            "a checkpoint is not a goodbye: {completeness:?}"
        );
        let reason = completeness.incomplete_reason().expect("a named reason");
        assert!(reason.contains("router-h-1"), "names the silent instance");
        assert!(reason.contains("eof"));
        assert!(completeness.describe().contains("INCOMPLETE"));
    }

    #[test]
    fn an_eof_from_every_instance_means_nothing_more_is_coming() {
        let chunk = format!(
            "{}\n{}\n{}\n{}\n",
            correlated("router-h-1", 0, "c-1", 0),
            correlated("router-h-2", 1, "c-2", 0),
            eof("router-h-1", 0),
            eof("router-h-2", 1),
        )
        .into_bytes();
        let collated = collate(&[chunk]);
        let completeness = collated.markers.completeness();
        assert_eq!(completeness, Completeness::EofObserved { instances: 2 });
        assert!(completeness.is_complete());
    }

    #[test]
    fn one_instance_still_writing_holds_the_whole_recording_incomplete() {
        // A multi-producer recording is complete only when EVERY writer has
        // said goodbye. One that hasn't is still appending to this session.
        let chunk = format!(
            "{}\n{}\n",
            eof("router-h-1", 4),
            checkpoint("router-h-2", 9),
        )
        .into_bytes();
        let collated = collate(&[chunk]);
        let completeness = collated.markers.completeness();
        assert!(!completeness.is_complete());
        let reason = completeness.incomplete_reason().expect("a named reason");
        assert!(reason.contains("router-h-2"));
        assert!(
            !reason.contains("router-h-1"),
            "the instance that DID finish must not be blamed: {reason}"
        );
    }

    #[test]
    fn a_prefix_with_no_markers_at_all_cannot_claim_completeness() {
        // Absence of the audit trail is not evidence the recording finished.
        // This is the case the real 9/44 run was in if its markers never
        // reached the scanned prefix.
        let chunk = format!("{}\n", correlated("router-h-1", 0, "c-1", 0)).into_bytes();
        let collated = collate(&[chunk]);
        let completeness = collated.markers.completeness();
        assert!(!completeness.is_complete());
        assert!(completeness
            .incomplete_reason()
            .expect("a named reason")
            .contains("no readable sink marker"));
    }

    #[test]
    fn collate_keeps_distinct_runs_apart() {
        let chunks =
            vec![format!("{}\n{}\n", envelope("r2", 1, ""), envelope("r1", 1, "")).into_bytes()];
        let Collated { events, drops, .. } = collate(&chunks);
        assert_eq!(drops.duplicates, 0);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].0.as_deref(), Some("r1")); // sorted by (rid, gseq, kind)
    }
}
