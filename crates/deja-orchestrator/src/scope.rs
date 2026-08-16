//! The run's correlation scope, and the ONLY door onto a recording tape.
//!
//! A replay run drives a subset of a recorded session — 3 correlations out of
//! 42,310 is a normal shape. Before this module that subset reached the various
//! stages by four unrelated mechanisms: an explicit `Option<&[String]>`
//! parameter, a post-load `retain`, an env var across a process boundary
//! (`KERNEL_CORRELATION_FILTER`), and transitive accident. `HarnessRoot` also
//! exposed `recording_events_path`, so ANY new function could take a `&Path`
//! and skip all four. Three already had: `write_record_graph_nodes` published
//! all 86,204 graph nodes (span structure and field values for every request in
//! a production session) through an unauthenticated `GET /graph`;
//! `render_lookup_table` built the 5.95 MB substitution table off the whole
//! session; `build_ledger` reloaded the tape unscoped while the scorecard read
//! the scoped events.
//!
//! Threading a newtype through today's call sites would not have stopped the
//! next function from simply not taking it — which is exactly how
//! `write_record_graph_nodes` was written. So the raw-path seam is gone
//! instead: `recording_events_path` is private to this file, a reader gets a
//! [`ScopedRecording`] that yields DATA and has no `.path()` accessor, and the
//! three remaining legitimate uses of the path go through [`TapeSlot`]'s typed
//! doors. `tests/scope_invariant.rs` fails the build if the name reappears
//! anywhere else.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::{self, BufRead};
use std::path::PathBuf;

use deja_core::ExecutionGraphNode;
use deja_forest::{root_for, RootResolution};

use crate::{HarnessRoot, Run, RunSpec};

/// The most correlations one replay run drives.
///
/// Seeding is LINEAR in correlations — 362.5s for 455 (~0.8 s/correlation) and
/// 3.8s for 3 (1.27 s/correlation) — so 100 costs roughly 80 seconds of seeding,
/// which is a run someone will wait for. (The earlier belief that seeding grew
/// superlinearly came from mislabelling that 455-correlation run as 42; it does
/// not, and the ceiling is about total run time, not about a cliff.) An
/// unfiltered run of a real session is 455 correlations, took 439.8s and died in
/// the scorer, which is why "no choice" resolves to a bounded default rather
/// than to everything.
pub const MAX_CORRELATIONS_PER_RUN: usize = 100;

/// Refuse an explicit filter larger than [`MAX_CORRELATIONS_PER_RUN`].
///
/// A caller that names 400 correlations is asking for something this harness
/// will not do, and the wrong answer is to drive 100 of them: the run would
/// score as though it had driven all 400, and the 300 it never touched would
/// read as clean. So an oversized filter is a refusal, never a truncation. The
/// cap applies to the NORMALIZED set — blank and duplicate ids are not names of
/// test cases and must not count against a caller.
pub fn check_requested_correlations(requested: Option<&[String]>) -> Result<(), String> {
    if let Some(ids) = RunScope::from_filter(requested).ids() {
        if ids.len() > MAX_CORRELATIONS_PER_RUN {
            return Err(format!(
                "correlation_filter names {} correlations; one run drives at most {}. \
                 Split this into {} runs — driving a subset under one run id would score \
                 the correlations it skipped as though they had passed.",
                ids.len(),
                MAX_CORRELATIONS_PER_RUN,
                ids.len().div_ceil(MAX_CORRELATIONS_PER_RUN),
            ));
        }
    }
    Ok(())
}

/// The concrete correlations a run will drive.
///
/// An absent or blank filter is not "drive everything" for a replay run: it is
/// "the caller did not choose", and the answer is the first
/// [`MAX_CORRELATIONS_PER_RUN`] in TAPE ORDER — the earliest requests in the
/// recording. That is deterministic and explainable ("the first 100 requests of
/// the session"), and it is a real default rather than a refusal.
///
/// The result is always the explicit list, never an empty filter plus a rule
/// applied somewhere downstream: the run's own record has to show which 100 ran.
/// A recording with fewer correlations than the cap runs all of them — the cap
/// is a ceiling, not a target.
///
/// A default keeps tape order, so the persisted list reads as what it is (the
/// session's first hundred requests, in order); an explicit filter comes back
/// normalized and sorted. Neither ordering reaches behaviour — [`RunScope`]
/// takes the list as a set — so the difference is only in how the run's record
/// reads.
pub fn resolve_run_correlations(
    requested: Option<&[String]>,
    tape_order: &[String],
) -> Result<Vec<String>, String> {
    check_requested_correlations(requested)?;
    match RunScope::from_filter(requested).ids() {
        Some(ids) => Ok(ids.iter().cloned().collect()),
        None => Ok(tape_order
            .iter()
            .take(MAX_CORRELATIONS_PER_RUN)
            .cloned()
            .collect()),
    }
}

/// Which recorded test cases a run covers.
///
/// `EntireSession` is not "no filter yet" — it is a real answer, and the one
/// record mode always gives (a recording has nothing to filter against) and the
/// local demo relies on. Enumerating 170,568 correlations as a value to avoid
/// an enum variant would be absurd; the containment guarantee comes from there
/// being no way to open a tape without saying which of the two you mean.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Correlations {
    Only(BTreeSet<String>),
    EntireSession,
}

/// The scope of one run. ONE field: everything else a caller might want to hang
/// here (a fingerprint, an ingress-path exclusion list) is derivable or belongs
/// to whoever needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunScope {
    pub correlations: Correlations,
}

impl RunScope {
    /// The only run-derived constructor. Blank/whitespace ids are dropped and an
    /// empty result degrades to `EntireSession`, matching what every hand-rolled
    /// site did with `filter(|f| !f.is_empty())` — a typo'd empty filter has
    /// never meant "drive nothing".
    pub fn of(run: &Run) -> Self {
        Self::of_spec(&run.spec)
    }

    pub fn of_spec(spec: &RunSpec) -> Self {
        Self::from_filter(spec.correlation_filter.as_deref())
    }

    pub fn from_filter(filter: Option<&[String]>) -> Self {
        let ids: BTreeSet<String> = filter
            .unwrap_or(&[])
            .iter()
            .map(|s| s.trim().to_owned())
            .filter(|s| !s.is_empty())
            .collect();
        if ids.is_empty() {
            Self::entire_session()
        } else {
            Self {
                correlations: Correlations::Only(ids),
            }
        }
    }

    pub fn entire_session() -> Self {
        Self {
            correlations: Correlations::EntireSession,
        }
    }

    pub fn is_entire_session(&self) -> bool {
        matches!(self.correlations, Correlations::EntireSession)
    }

    /// The pinned ids, or `None` for `EntireSession`.
    pub fn ids(&self) -> Option<&BTreeSet<String>> {
        match &self.correlations {
            Correlations::Only(ids) => Some(ids),
            Correlations::EntireSession => None,
        }
    }

    /// Is this record in scope? An UNCORRELATED record (`None`) always is:
    /// ambient/background traffic is shared across cases and is tolerated by
    /// the scorer rather than attributed to any one of them. Dropping it would
    /// turn shared setup into a per-case omission.
    pub fn contains(&self, correlation_id: Option<&str>) -> bool {
        match (&self.correlations, correlation_id) {
            (Correlations::EntireSession, _) => true,
            (Correlations::Only(_), None) => true,
            (Correlations::Only(ids), Some(id)) => ids.contains(id),
        }
    }
}

/// The four counters worth having: what came off the tape, what survived the
/// scope, how many graph anchors the in-scope events named, and how many
/// distinct graph roots those anchors resolved to. Everything else a caller
/// wanted was derivable from these.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Census {
    /// Boundary events on the tape, before scoping.
    pub events_in: u64,
    /// Boundary events in scope.
    pub events_kept: u64,
    /// Distinct `graph_node_id`s named by in-scope boundary events.
    pub anchors: u64,
    /// Distinct graph roots those anchors resolve to.
    pub roots_reached: u64,
}

/// One line of the tape as a reader sees it. Malformed lines are DATA, not a
/// silent drop: `render_lookup_table` fails closed on them because a dropped
/// event is a hole in replay coverage that would otherwise pass as a clean run.
#[derive(Debug)]
pub enum TapeItem {
    /// An in-scope boundary event.
    Event(Box<deja::BoundaryEvent>),
    /// A line that did not parse as a `DejaRecord`.
    Malformed {
        line_no: usize,
        error: String,
        excerpt: String,
    },
}

/// The only way to READ a recording. Yields data, never a path — a caller handed
/// a path does what today's callers did.
#[derive(Debug)]
pub struct ScopedRecording {
    recording_id: String,
    /// PRIVATE, and there is deliberately no accessor for it.
    path: PathBuf,
    scope: RunScope,
}

impl ScopedRecording {
    /// Open the recording's tape under `scope`. Fails if the tape is not
    /// materialized — a reader that silently sees zero events is how an entire
    /// missing record side looked like an ordinary run. Callers that tolerate a
    /// mid-flight run check [`TapeSlot::is_materialized`] first, or match on
    /// `io::ErrorKind::NotFound`.
    pub fn open(root: &HarnessRoot, recording_id: &str, scope: RunScope) -> io::Result<Self> {
        let path = recording_events_path(root, recording_id);
        if !path.exists() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "recording {recording_id} not materialized at {}",
                    path.display()
                ),
            ));
        }
        Ok(Self {
            recording_id: recording_id.to_owned(),
            path,
            scope,
        })
    }

    pub fn recording_id(&self) -> &str {
        &self.recording_id
    }

    pub fn scope(&self) -> &RunScope {
        &self.scope
    }

    /// STREAM the in-scope boundary events. Never a `Vec`: `EntireSession` on a
    /// live recording is 171,234 events from a 361 MB tape, and materializing
    /// that for the life of a run would be a regression dressed as a fix.
    /// Graph nodes and replay observations are skipped (they are not boundary
    /// events); unparseable lines surface as [`TapeItem::Malformed`].
    pub fn events(&self) -> io::Result<impl Iterator<Item = TapeItem> + '_> {
        let reader = io::BufReader::new(std::fs::File::open(&self.path)?);
        Ok(reader
            .lines()
            .map_while(Result::ok)
            .enumerate()
            .filter_map(move |(i, line)| {
                if line.trim().is_empty() {
                    return None;
                }
                match serde_json::from_str::<deja::DejaRecord>(&line) {
                    Ok(deja::DejaRecord::BoundaryEvent(event)) => self
                        .scope
                        .contains(event.correlation_id.as_deref())
                        .then_some(TapeItem::Event(event)),
                    Ok(deja::DejaRecord::GraphNode(_) | deja::DejaRecord::Observed(_)) => None,
                    Err(e) => Some(TapeItem::Malformed {
                        line_no: i + 1,
                        error: e.to_string(),
                        excerpt: line.chars().take(200).collect(),
                    }),
                }
            }))
    }

    /// The in-scope slice of the record-side execution graph.
    ///
    /// TWO passes, never a buffer of raw lines per node — buffering is the whole
    /// 28 MB in RAM. Pass 1 reads `node_id -> parent_id` only (u64 -> u64, 1.4 MB
    /// for 86,204 nodes) and collects ANCHORS: the `graph_node_id`s named by
    /// in-scope boundary events. Those resolve up the parent chain to their
    /// roots; every node under a reached root is kept (that is the request's own
    /// span tree). Pass 2 re-reads and emits the kept nodes.
    ///
    /// Hard-errors rather than returning a plausible-looking empty graph — see
    /// [`Self::resolve`].
    pub fn graph_nodes(&self) -> io::Result<Vec<ExecutionGraphNode>> {
        let index = self.index()?;
        let resolved = self.resolve(&index)?;
        let mut nodes = Vec::new();
        self.for_each_line(|_, line| {
            if let Ok(deja::DejaRecord::GraphNode(node)) =
                serde_json::from_str::<deja::DejaRecord>(line)
            {
                if resolved.keep.contains(&node.node_id) {
                    nodes.push(*node);
                }
            }
        })?;
        Ok(nodes)
    }

    /// In / kept / anchors / roots for this tape under this scope. Reports; does
    /// NOT enforce — the two hard errors live in [`Self::graph_nodes`], so a
    /// census can describe a broken tape instead of refusing to.
    pub fn census(&self) -> io::Result<Census> {
        let index = self.index()?;
        let (anchors, roots_reached) = match self.resolve(&index) {
            Ok(r) => (r.anchors, r.roots_reached),
            Err(_) => (index.anchor_count(), 0),
        };
        Ok(Census {
            events_in: index.events_in,
            events_kept: index.events_kept,
            anchors,
            roots_reached,
        })
    }

    // -- internals ----------------------------------------------------------

    fn for_each_line(&self, mut f: impl FnMut(usize, &str)) -> io::Result<()> {
        let reader = io::BufReader::new(std::fs::File::open(&self.path)?);
        for (i, line) in reader.lines().map_while(Result::ok).enumerate() {
            if line.trim().is_empty() {
                continue;
            }
            f(i + 1, &line);
        }
        Ok(())
    }

    /// Pass 1: the parent map and the anchor set, and nothing else. Both
    /// projections deserialize a handful of fields, not the whole record, so a
    /// 361 MB tape costs one sequential read and ~1.4 MB of map.
    fn index(&self) -> io::Result<TapeIndex> {
        let mut index = TapeIndex::default();
        self.for_each_line(|_, line| {
            // Cheap discriminator first: full `DejaRecord` parse on 171,234
            // lines to read two fields is the cost this pass exists to avoid.
            match serde_json::from_str::<RecordHead>(line) {
                Ok(RecordHead::GraphNode(node)) => {
                    index.parent_of.insert(node.node_id, node.parent_id);
                    // A node that states its own correlation anchors that case
                    // directly, with no join. Older tapes state none, so this
                    // adds anchors and never removes any: a case is reachable if
                    // EITHER its events name a node or a node names the case.
                    if let Some(correlation) = node.correlation_id {
                        if self.scope.contains(Some(correlation.as_str())) {
                            index.nodes_stating_correlation += 1;
                            index
                                .anchors_by_correlation
                                .entry(correlation)
                                .or_default()
                                .insert(node.node_id);
                        }
                    }
                }
                Ok(RecordHead::BoundaryEvent(event)) => {
                    index.events_in += 1;
                    if !self.scope.contains(event.correlation_id.as_deref()) {
                        return;
                    }
                    index.events_kept += 1;
                    match event.correlation_id {
                        // A CASE. It gets a bucket whether or not it anchors,
                        // so a case that reaches no graph root can be named.
                        Some(correlation) => {
                            if event.boundary != "http_incoming" {
                                index
                                    .instrumented_work_by_correlation
                                    .insert(correlation.clone());
                            }
                            let bucket =
                                index.anchors_by_correlation.entry(correlation).or_default();
                            bucket.extend(event.graph_node_id);
                        }
                        // AMBIENT. Uncorrelated background traffic is not a test
                        // case: it is shared, it is tolerated by the scorer, and
                        // it routinely carries no graph node at all. Requiring it
                        // to reach a root would refuse every real tape.
                        None => index.ambient_anchors.extend(event.graph_node_id),
                    }
                }
                Ok(RecordHead::Other) | Err(_) => {}
            }
        })?;
        Ok(index)
    }

    /// Anchors -> roots -> kept subtrees, with the two refusals.
    ///
    /// `graph_node_id` is `Option<u64>`: on a tape recorded before the graph
    /// carried it, EVERY anchor is absent, the closure keeps zero nodes, and the
    /// record graph silently goes 86,204 -> 0 — strictly worse than publishing
    /// the whole session, because it reads as "this run had no cascade". So a
    /// scoped read with no anchors, or an in-scope correlation whose events
    /// reach no graph root, is a hard error naming the tape.
    fn resolve(&self, index: &TapeIndex) -> io::Result<Resolved> {
        let mut root_of: HashMap<u64, RootResolution> = HashMap::new();

        // `EntireSession` keeps the whole graph WITHOUT consulting anchors at
        // all: record mode has no filter, and "no filter = everything" is what
        // the local demo drives. Anchor-based narrowing is a property of a
        // pinned subset, never a precondition for reading a tape.
        if self.scope.is_entire_session() {
            let mut roots: BTreeSet<u64> = BTreeSet::new();
            for node_id in index.parent_of.keys() {
                if let RootResolution::Root(root) | RootResolution::Cycle { root, .. } =
                    root_for(*node_id, &index.parent_of, &mut root_of)
                {
                    roots.insert(root);
                }
            }
            return Ok(Resolved {
                keep: index.parent_of.keys().copied().collect(),
                anchors: index.anchor_count(),
                roots_reached: roots.len() as u64,
            });
        }

        let mut anchors: BTreeSet<u64> = index.ambient_anchors.clone();
        for ids in index.anchors_by_correlation.values() {
            anchors.extend(ids.iter().copied());
        }

        if anchors.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recording {}: {} in-scope event(s) name NO execution-graph node — \
                     `graph_node_id` is absent on this tape, so a scoped record graph would \
                     be empty rather than small. Refusing to publish an empty graph as if the \
                     run had no cascade.",
                    self.recording_id, index.events_kept,
                ),
            ));
        }

        // Root of each node, memoized. A node whose parent is absent from the
        // payload is PROMOTED to a root: the replay graph carries 175 such
        // dangling references and treating them as unreachable would drop real
        // spans.
        let mut roots: BTreeSet<u64> = BTreeSet::new();
        let mut unreachable: Vec<String> = Vec::new();

        for anchor in &index.ambient_anchors {
            if let RootResolution::Root(root) | RootResolution::Cycle { root, .. } =
                root_for(*anchor, &index.parent_of, &mut root_of)
            {
                roots.insert(root);
            }
        }
        // Two distinguishable causes, kept apart. "Names no node" is a CAPTURE
        // gap — nothing on the tape ties the case to a span. "Names a node the
        // tape does not carry" is a DELIVERY gap — the tie exists and the node
        // is missing. They are fixed in different places, so one blanket message
        // sends the reader looking in the wrong half of the system.
        let mut names_nothing: Vec<String> = Vec::new();
        for (correlation, ids) in &index.anchors_by_correlation {
            let mut reached = false;
            for anchor in ids {
                if let RootResolution::Root(root) | RootResolution::Cycle { root, .. } =
                    root_for(*anchor, &index.parent_of, &mut root_of)
                {
                    roots.insert(root);
                    reached = true;
                }
            }
            if !reached {
                if ids.is_empty() {
                    // A case whose ONLY event is the http_incoming request
                    // performed no instrumented work — a validation
                    // rejection. It has no span tree to be missing; "names
                    // no node" is the correct state, not a capture gap.
                    if index
                        .instrumented_work_by_correlation
                        .contains(correlation.as_str())
                    {
                        names_nothing.push(correlation.clone());
                    }
                } else {
                    unreachable.push(correlation.clone());
                }
            }
        }

        if !names_nothing.is_empty() || !unreachable.is_empty() {
            let cases = index.anchors_by_correlation.len();
            let stated = if index.nodes_stating_correlation > 0 {
                format!(
                    "{} in-scope node(s) state their own correlation, so a case named here is \
                     one no node claims",
                    index.nodes_stating_correlation,
                )
            } else {
                "no graph node on this tape states its own correlation, so a case can only be \
                 tied to the graph through an event naming a node — a tape recorded before \
                 nodes carried a correlation cannot do better"
                    .to_owned()
            };
            let mut detail = String::new();
            if !names_nothing.is_empty() {
                detail.push_str(&format!(
                    " {} of {cases} case(s) name NO execution-graph node at all ({}) — a \
                     capture gap: nothing on the tape ties them to a span.",
                    names_nothing.len(),
                    names_nothing.join(", "),
                ));
            }
            if !unreachable.is_empty() {
                detail.push_str(&format!(
                    " {} of {cases} case(s) name node(s) the tape does not carry ({}) — a \
                     delivery gap: the tie exists but the nodes are missing.",
                    unreachable.len(),
                    unreachable.join(", "),
                ));
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recording {}:{detail} Their spans would be missing from the record graph \
                     with no sign that anything was dropped. Context: {stated}.",
                    self.recording_id,
                ),
            ));
        }

        // Keep every node under a reached root — the request's own span tree.
        let mut keep: HashSet<u64> = HashSet::new();
        for node_id in index.parent_of.keys() {
            if let RootResolution::Root(root) | RootResolution::Cycle { root, .. } =
                root_for(*node_id, &index.parent_of, &mut root_of)
            {
                if roots.contains(&root) {
                    keep.insert(*node_id);
                }
            }
        }

        Ok(Resolved {
            keep,
            anchors: anchors.len() as u64,
            roots_reached: roots.len() as u64,
        })
    }
}

#[derive(Default)]
struct TapeIndex {
    /// EVERY graph node on the tape: `node_id -> parent_id`. Nothing else.
    parent_of: HashMap<u64, Option<u64>>,
    /// In-scope anchors per CASE, grouped so a correlation that reaches no root
    /// can be NAMED in the refusal. A case with events but no anchors gets an
    /// empty entry, which is exactly the condition that must be refused.
    anchors_by_correlation: BTreeMap<String, BTreeSet<u64>>,
    /// Cases with at least one event BESIDES the `http_incoming` request
    /// marker. A request rejected before any instrumented work (a 400 at
    /// validation) records only its request/response — its absence from the
    /// execution graph is CORRECT, not a capture gap, and flagging it taught
    /// two clean 400→400 correlations to read as graph-capture failures on
    /// every run report.
    instrumented_work_by_correlation: BTreeSet<String>,
    /// Anchors from uncorrelated (ambient) events. They contribute roots — the
    /// background traffic's spans belong to the run too — but they are NOT a
    /// case, so they are exempt from the reach-a-root requirement.
    ambient_anchors: BTreeSet<u64>,
    events_in: u64,
    events_kept: u64,
    /// How many in-scope graph nodes stated their own correlation. Zero means
    /// the tape predates the field, which is what separates "this recording
    /// cannot say" from "this recording says these spans belong nowhere".
    nodes_stating_correlation: u64,
}

impl TapeIndex {
    fn anchor_count(&self) -> u64 {
        self.anchors_by_correlation
            .values()
            .flatten()
            .chain(self.ambient_anchors.iter())
            .collect::<BTreeSet<_>>()
            .len() as u64
    }
}

struct Resolved {
    keep: HashSet<u64>,
    anchors: u64,
    roots_reached: u64,
}

/// Minimal projection of a tape line: the tag plus the two or three fields
/// pass 1 needs. Deserializing the full `DejaRecord` to read `parent_id` would
/// make the index pass as expensive as the emit pass.
#[derive(serde::Deserialize)]
#[serde(tag = "record_kind", rename_all = "snake_case")]
enum RecordHead {
    GraphNode(GraphNodeHead),
    BoundaryEvent(BoundaryEventHead),
    #[serde(other)]
    Other,
}

#[derive(serde::Deserialize)]
struct GraphNodeHead {
    node_id: u64,
    #[serde(default)]
    parent_id: Option<u64>,
    /// The case this span belongs to, when the tape states it. Absent on tapes
    /// recorded before graph nodes carried a correlation, which is why it can
    /// only ever ADD anchors — absence means "not stated", not "uncorrelated".
    #[serde(default)]
    correlation_id: Option<String>,
}

#[derive(serde::Deserialize)]
struct BoundaryEventHead {
    #[serde(default)]
    correlation_id: Option<String>,
    /// Which boundary produced the event. `http_incoming` is the request
    /// marker itself — a case whose ONLY event is that marker performed no
    /// instrumented work and legitimately has no graph node. Defaulted so a
    /// pre-field tape parses (and then every event counts as work, the old
    /// behavior).
    #[serde(default)]
    boundary: String,
    /// `Option<u64>`, and the Kafka -> Vector -> S3 pipeline stringifies u64s
    /// above `i64::MAX`, so this must accept both forms exactly like the full
    /// `BoundaryEvent` does — otherwise pass 1 silently loses anchors that
    /// pass 2 would have kept.
    #[serde(default, deserialize_with = "de_opt_u64")]
    graph_node_id: Option<u64>,
}

fn de_opt_u64<'de, D>(de: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize as _;
    match Option::<serde_json::Value>::deserialize(de)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => Ok(n.as_u64()),
        Some(serde_json::Value::String(s)) => Ok(s.parse::<u64>().ok()),
        Some(_) => Ok(None),
    }
}

/// The three legitimate uses of the tape's PATH, each with its own door, so
/// that "I have a path" is never a general capability.
pub struct TapeSlot;

impl TapeSlot {
    /// INGEST: where a pull writes the session's events. The only door that
    /// hands out a writable path.
    pub fn for_write(root: &HarnessRoot, recording_id: &str) -> PathBuf {
        recording_events_path(root, recording_id)
    }

    /// Has the tape been pulled onto this node yet?
    pub fn is_materialized(root: &HarnessRoot, recording_id: &str) -> bool {
        recording_events_path(root, recording_id).exists()
    }

    /// The ingest report written BESIDE the tape when it was pulled.
    ///
    /// It rides here rather than in memory because a run that reuses an
    /// already-materialized tape never re-ingests it, and the question the
    /// report answers — did this recording stop growing before it was read —
    /// is a property of the TAPE, not of the run that happened to pull it. A
    /// door of its own, for the same reason the tape has one: the alternative
    /// is every caller rebuilding `for_write(..).with_file_name(..)`, which is
    /// how the raw path escaped last time.
    pub fn ingest_report_path(root: &HarnessRoot, recording_id: &str) -> PathBuf {
        recording_events_path(root, recording_id).with_file_name("ingest-report.json")
    }

    /// CHOOSING a scope: the recording's correlations in tape order — each one
    /// at its first appearance, earliest first.
    ///
    /// This is the one read that cannot be scoped, because its whole job is to
    /// decide what the scope will be. It stays behind a typed door for the same
    /// reason the others do, and it yields ids only: no event, no payload, no
    /// path. One sequential pass with the same cheap projection the index pass
    /// uses — a full `DejaRecord` parse per line to read one field is the cost
    /// this shape exists to avoid.
    ///
    /// The sealed session's correlation index (`deja-compactor`) is ordered the
    /// same way, so the list a caller browses and the list a run defaults to
    /// agree. The two are computed over the same records in the same
    /// sequence-major order; they can only diverge for a session written by more
    /// than one producer, which no capture mode emits today.
    pub fn correlations_in_tape_order(
        root: &HarnessRoot,
        recording_id: &str,
    ) -> io::Result<Vec<String>> {
        let path = recording_events_path(root, recording_id);
        let reader = io::BufReader::new(std::fs::File::open(&path)?);
        let mut seen: HashSet<String> = HashSet::new();
        let mut order: Vec<String> = Vec::new();
        for line in reader.lines().map_while(Result::ok) {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(RecordHead::BoundaryEvent(event)) = serde_json::from_str::<RecordHead>(&line)
            {
                // An uncorrelated record is ambient traffic shared across cases,
                // not a case anyone can choose to drive.
                if let Some(correlation) = event.correlation_id {
                    if seen.insert(correlation.clone()) {
                        order.push(correlation);
                    }
                }
            }
        }
        Ok(order)
    }

    /// SUBPROCESS: the replay kernel reads the tape in another process, so the
    /// scope has to cross as an env var. This door emits `KERNEL_RECORDING_PATH`
    /// and `KERNEL_CORRELATION_FILTER` TOGETHER — there is no way to set the
    /// path without also setting the filter, which is how the kernel used to end
    /// up pointed at a whole session by a caller that forgot the second `env()`
    /// call. `EntireSession` emits no filter var, which is what the kernel reads
    /// as "drive everything".
    pub fn subprocess(
        root: &HarnessRoot,
        recording_id: &str,
        scope: &RunScope,
    ) -> Vec<(String, String)> {
        let mut env = vec![(
            "KERNEL_RECORDING_PATH".to_owned(),
            recording_events_path(root, recording_id)
                .display()
                .to_string(),
        )];
        if let Some(ids) = scope.ids() {
            env.push((
                "KERNEL_CORRELATION_FILTER".to_owned(),
                ids.iter().cloned().collect::<Vec<_>>().join(","),
            ));
        }
        env
    }
}

/// THE tape location. Private to this module on purpose: this function having
/// been a `pub fn` on `HarnessRoot` is the entire root cause — 18 call sites,
/// four different scoping mechanisms between them, three that had none.
fn recording_events_path(root: &HarnessRoot, recording_id: &str) -> PathBuf {
    root.root
        .join("recordings")
        .join(recording_id)
        .join("events.jsonl")
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn scope_of(ids: &[&str]) -> RunScope {
        RunScope::from_filter(Some(
            &ids.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>(),
        ))
    }

    fn tape(root: &HarnessRoot, recording_id: &str, lines: &[serde_json::Value]) {
        let path = TapeSlot::for_write(root, recording_id);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{line}").unwrap();
        }
    }

    fn node(id: u64, parent: Option<u64>, name: &str) -> serde_json::Value {
        serde_json::json!({
            "record_kind": "graph_node",
            "node_id": id,
            "global_sequence": id,
            "parent_id": parent,
            "sequence": id,
            "span_name": name,
            "target": "router",
            "level": "INFO",
            "fields": {},
            "started_ns": 1,
        })
    }

    fn event(seq: u64, correlation: &str, graph_node_id: Option<u64>) -> serde_json::Value {
        serde_json::json!({
            "record_kind": "boundary_event",
            "global_sequence": seq,
            "request_sequence": 0,
            "correlation_id": correlation,
            "timestamp_ns": 0,
            "recording_run_id": "r",
            "boundary": "db",
            "trait_name": "T",
            "method_name": "m",
            "call_file": "x.rs",
            "call_line": 1,
            "call_column": 0,
            "request": null,
            "args": {},
            "response": null,
            "result": "v",
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute",
            "graph_node_id": graph_node_id,
        })
    }

    #[test]
    fn entire_session_keeps_everything_and_pins_nothing() {
        let scope = RunScope::entire_session();
        assert!(scope.is_entire_session());
        assert!(scope.contains(Some("anything")));
        assert!(scope.contains(None));
        assert_eq!(scope.ids(), None);
        // A blank / whitespace-only filter is not "drive nothing".
        assert!(RunScope::from_filter(None).is_entire_session());
        assert!(RunScope::from_filter(Some(&[])).is_entire_session());
        assert!(RunScope::from_filter(Some(&[" ".to_owned()])).is_entire_session());
    }

    #[test]
    fn only_keeps_the_pinned_ids_and_all_ambient_records() {
        let scope = scope_of(&["c-1", " c-2 "]);
        assert!(scope.contains(Some("c-1")));
        assert!(scope.contains(Some("c-2")), "ids are trimmed");
        assert!(!scope.contains(Some("c-3")));
        assert!(
            scope.contains(None),
            "uncorrelated background records are shared across cases, never dropped"
        );
    }

    #[test]
    fn events_stream_only_the_in_scope_boundary_events() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        tape(
            &root,
            "rec",
            &[
                node(1, None, "ROOT_SPAN"),
                event(1, "c-1", Some(1)),
                event(2, "c-2", Some(1)),
                serde_json::json!({"record_kind": "boundary_event", "global_sequence": "nope"}),
            ],
        );
        let rec = ScopedRecording::open(&root, "rec", scope_of(&["c-1"])).unwrap();
        let items: Vec<TapeItem> = rec.events().unwrap().collect();
        let kept: Vec<&str> = items
            .iter()
            .filter_map(|i| match i {
                TapeItem::Event(e) => e.correlation_id.as_deref(),
                TapeItem::Malformed { .. } => None,
            })
            .collect();
        assert_eq!(kept, ["c-1"], "out-of-scope events never reach the caller");
        assert_eq!(
            items
                .iter()
                .filter(|i| matches!(i, TapeItem::Malformed { .. }))
                .count(),
            1,
            "a dropped event is a coverage hole and must be visible, not swallowed"
        );
    }

    #[test]
    fn graph_nodes_keeps_the_in_scope_request_trees_only() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        tape(
            &root,
            "rec",
            &[
                // request A
                node(1, None, "ROOT_SPAN"),
                node(2, Some(1), "handler"),
                node(3, Some(2), "db"),
                // request B — a whole other production request
                node(10, None, "ROOT_SPAN"),
                node(11, Some(10), "handler"),
                event(1, "c-1", Some(3)),
                event(2, "c-2", Some(11)),
            ],
        );
        let rec = ScopedRecording::open(&root, "rec", scope_of(&["c-1"])).unwrap();
        let mut ids: Vec<u64> = rec
            .graph_nodes()
            .unwrap()
            .iter()
            .map(|n| n.node_id)
            .collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            [1, 2, 3],
            "the anchored request's whole span tree is kept, and only it"
        );

        let census = rec.census().unwrap();
        assert_eq!(census.events_in, 2);
        assert_eq!(census.events_kept, 1);
        assert_eq!(census.anchors, 1);
        assert_eq!(census.roots_reached, 1);

        // EntireSession still means everything — record mode has no filter.
        let all = ScopedRecording::open(&root, "rec", RunScope::entire_session()).unwrap();
        assert_eq!(all.graph_nodes().unwrap().len(), 5);
    }

    #[test]
    fn a_node_whose_parent_is_absent_is_promoted_to_a_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        tape(
            &root,
            "rec",
            &[
                // 7's parent (99) is not in the payload — the replay graph has
                // 175 of these. Dropping them would lose real spans.
                node(7, Some(99), "orphan-top"),
                node(8, Some(7), "child"),
                event(1, "c-1", Some(8)),
            ],
        );
        let rec = ScopedRecording::open(&root, "rec", scope_of(&["c-1"])).unwrap();
        let mut ids: Vec<u64> = rec
            .graph_nodes()
            .unwrap()
            .iter()
            .map(|n| n.node_id)
            .collect();
        ids.sort_unstable();
        assert_eq!(ids, [7, 8]);
    }

    #[test]
    fn a_tape_with_no_graph_anchors_is_refused_not_silently_emptied() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        // An older tape: graph nodes exist, but no event carries graph_node_id.
        tape(
            &root,
            "rec",
            &[
                node(1, None, "ROOT_SPAN"),
                node(2, Some(1), "handler"),
                event(1, "c-1", None),
            ],
        );
        let rec = ScopedRecording::open(&root, "rec", scope_of(&["c-1"])).unwrap();
        let err = rec.graph_nodes().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("graph_node_id"),
            "the refusal names the cause: {err}"
        );
        // EntireSession has no anchors by design and must NOT be refused.
        let all = ScopedRecording::open(&root, "rec", RunScope::entire_session()).unwrap();
        assert_eq!(all.graph_nodes().unwrap().len(), 2);
    }

    #[test]
    fn an_in_scope_correlation_that_reaches_no_root_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        tape(
            &root,
            "rec",
            &[
                node(1, None, "ROOT_SPAN"),
                node(2, Some(1), "handler"),
                event(1, "c-1", Some(2)),
                // c-2's anchor names a node that is not in the graph at all.
                event(2, "c-2", Some(4242)),
            ],
        );
        let rec = ScopedRecording::open(&root, "rec", scope_of(&["c-1", "c-2"])).unwrap();
        let err = rec.graph_nodes().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("c-2"),
            "the refusal names the correlation that would vanish: {err}"
        );
    }

    fn http_incoming_event(seq: u64, correlation: &str) -> serde_json::Value {
        let mut ev = event(seq, correlation, None);
        ev["boundary"] = serde_json::json!("http_incoming");
        ev
    }

    /// A case whose ONLY event is its `http_incoming` request marker — a 400
    /// rejected at validation — performed no instrumented work: no graph node
    /// is its CORRECT state, not a capture gap. Every run report was carrying
    /// a "record graph unavailable" warning for two clean 400→400 cases. A
    /// case with real boundary work that names nothing is still refused.
    #[test]
    fn a_request_only_case_with_no_graph_node_is_not_a_capture_gap() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        tape(
            &root,
            "rec",
            &[
                node(1, None, "ROOT_SPAN"),
                node(2, Some(1), "handler"),
                event(1, "c-ok", Some(2)),
                // Validation rejection: request/response recorded, no
                // instrumented work, no graph node — correct, not a gap.
                http_incoming_event(2, "c-400"),
            ],
        );
        let rec = ScopedRecording::open(&root, "rec", scope_of(&["c-ok", "c-400"])).unwrap();
        let nodes = rec
            .graph_nodes()
            .expect("a request-only case must not refuse the record graph");
        assert_eq!(nodes.len(), 2, "the anchored case's tree is kept");

        // Control: real boundary work that names no node is still a capture
        // gap and still refuses, naming the case.
        tape(
            &root,
            "rec2",
            &[
                node(1, None, "ROOT_SPAN"),
                event(1, "c-ok", Some(1)),
                event(2, "c-gap", None),
            ],
        );
        let rec2 = ScopedRecording::open(&root, "rec2", scope_of(&["c-ok", "c-gap"])).unwrap();
        let err = rec2.graph_nodes().unwrap_err();
        assert!(
            err.to_string().contains("c-gap"),
            "instrumented work with no anchor is still refused: {err}"
        );
    }

    #[test]
    fn pass_one_accepts_the_stringified_u64_anchors_the_pipeline_emits() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let big: u64 = 18_000_000_000_000_000_000;
        let mut ev = event(1, "c-1", None);
        ev["graph_node_id"] = serde_json::json!(big.to_string());
        tape(&root, "rec", &[node(big, None, "ROOT_SPAN"), ev]);
        let rec = ScopedRecording::open(&root, "rec", scope_of(&["c-1"])).unwrap();
        assert_eq!(
            rec.graph_nodes().unwrap().len(),
            1,
            "a stringified u64 anchor must resolve, or pass 1 loses nodes pass 2 would keep"
        );
    }

    #[test]
    fn subprocess_emits_the_path_and_the_filter_together() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let env = TapeSlot::subprocess(&root, "rec", &scope_of(&["c-2", "c-1"]));
        let keys: Vec<&str> = env.iter().map(|(k, _)| k.as_str()).collect();
        assert_eq!(keys, ["KERNEL_RECORDING_PATH", "KERNEL_CORRELATION_FILTER"]);
        assert_eq!(env[1].1, "c-1,c-2", "ids are sorted + deduped");

        let full = TapeSlot::subprocess(&root, "rec", &RunScope::entire_session());
        assert_eq!(
            full.len(),
            1,
            "EntireSession sets no filter var — the kernel reads its absence as drive-everything"
        );
    }

    #[test]
    fn opening_an_unmaterialized_recording_fails_loudly() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        assert!(!TapeSlot::is_materialized(&root, "rec"));
        let err = ScopedRecording::open(&root, "rec", RunScope::entire_session()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    // -- how many correlations a run drives -----------------------------------

    fn ids(n: usize) -> Vec<String> {
        (0..n).map(|i| format!("c-{i:04}")).collect()
    }

    #[test]
    fn no_choice_drives_the_first_hundred_in_tape_order() {
        // The recording's order, not the ids' order: these ids sort backwards
        // against the traffic, so a resolution that sorted would return the
        // LAST hundred requests of the session and call them the first.
        let tape: Vec<String> = (0..455).map(|i| format!("c-{:04}", 455 - i)).collect();

        let resolved = resolve_run_correlations(None, &tape).unwrap();
        assert_eq!(resolved.len(), MAX_CORRELATIONS_PER_RUN);
        let expected: Vec<String> = tape[..MAX_CORRELATIONS_PER_RUN].to_vec();
        assert_eq!(
            resolved, expected,
            "the first 100 the recording saw, in the order it saw them, whatever \
             their ids sort like"
        );

        // A blank filter is not a choice either — the dashboard sends one when
        // nothing is selected.
        assert_eq!(
            resolve_run_correlations(Some(&[]), &tape).unwrap(),
            expected
        );
        assert_eq!(
            resolve_run_correlations(Some(&[" ".to_owned()]), &tape).unwrap(),
            expected
        );
    }

    #[test]
    fn a_recording_smaller_than_the_cap_runs_all_of_it() {
        // The cap is a ceiling, not a target.
        let tape = ids(40);
        let resolved = resolve_run_correlations(None, &tape).unwrap();
        assert_eq!(resolved.len(), 40);
        assert_eq!(resolved, tape);
    }

    #[test]
    fn an_explicit_filter_over_the_cap_is_refused_not_trimmed() {
        // Silently driving 100 of 400 would score the 300 that never ran as
        // though they had passed — a run that looks clean because it did less.
        let asked = ids(101);
        let err = resolve_run_correlations(Some(&asked), &ids(500)).unwrap_err();
        assert!(err.contains("101"), "{err}");
        assert!(err.contains("100"), "{err}");
        assert!(
            check_requested_correlations(Some(&asked)).is_err(),
            "the same refusal is available before a run is created"
        );

        // Exactly at the cap is allowed, and is returned whole.
        let at_cap = ids(MAX_CORRELATIONS_PER_RUN);
        assert_eq!(
            resolve_run_correlations(Some(&at_cap), &[]).unwrap().len(),
            MAX_CORRELATIONS_PER_RUN
        );
    }

    #[test]
    fn blanks_and_duplicates_do_not_count_against_a_callers_cap() {
        // The cap is about test cases, and neither a blank nor a repeat is one.
        let mut asked = ids(MAX_CORRELATIONS_PER_RUN);
        asked.push(asked[0].clone());
        asked.push("   ".to_owned());
        assert!(check_requested_correlations(Some(&asked)).is_ok());
        assert_eq!(
            resolve_run_correlations(Some(&asked), &[]).unwrap().len(),
            MAX_CORRELATIONS_PER_RUN
        );
    }

    #[test]
    fn an_explicit_filter_never_takes_the_default() {
        // The default applies to an ABSENT filter only. A caller that named
        // three correlations gets three, not the first hundred.
        let resolved =
            resolve_run_correlations(Some(&["c-2".to_owned(), "c-1".to_owned()]), &ids(400))
                .unwrap();
        assert_eq!(resolved, vec!["c-1".to_owned(), "c-2".to_owned()]);
    }

    #[test]
    fn the_tape_order_reader_yields_first_appearance_and_ids_only() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        tape(
            &root,
            "rec",
            &[
                node(1, None, "ROOT_SPAN"),
                event(1, "c-late", Some(1)),
                event(2, "c-early", Some(1)),
                // c-late again: it is not a new correlation, and must not move.
                event(3, "c-late", Some(1)),
                event(4, "c-mid", Some(1)),
            ],
        );
        assert_eq!(
            TapeSlot::correlations_in_tape_order(&root, "rec").unwrap(),
            vec![
                "c-late".to_owned(),
                "c-early".to_owned(),
                "c-mid".to_owned()
            ],
        );
    }

    #[test]
    fn ambient_traffic_is_not_offered_as_a_test_case() {
        // Uncorrelated records are shared background traffic. They ride along
        // with every case and there is nothing to drive, so they must not
        // occupy one of a run's hundred slots.
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let mut ambient = event(1, "ignored", None);
        ambient["correlation_id"] = serde_json::Value::Null;
        tape(&root, "rec", &[ambient, event(2, "c-1", None)]);
        assert_eq!(
            TapeSlot::correlations_in_tape_order(&root, "rec").unwrap(),
            vec!["c-1".to_owned()]
        );
    }
}
