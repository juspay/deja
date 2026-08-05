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

use crate::{HarnessRoot, Run, RunSpec};

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
                    nodes.push(node);
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
        let mut root_of: HashMap<u64, u64> = HashMap::new();

        // `EntireSession` keeps the whole graph WITHOUT consulting anchors at
        // all: record mode has no filter, and "no filter = everything" is what
        // the local demo drives. Anchor-based narrowing is a property of a
        // pinned subset, never a precondition for reading a tape.
        if self.scope.is_entire_session() {
            let mut roots: BTreeSet<u64> = BTreeSet::new();
            for node_id in index.parent_of.keys() {
                if let Some(root) = root_for(*node_id, &index.parent_of, &mut root_of) {
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
            if let Some(root) = root_for(*anchor, &index.parent_of, &mut root_of) {
                roots.insert(root);
            }
        }
        for (correlation, ids) in &index.anchors_by_correlation {
            let mut reached = false;
            for anchor in ids {
                if let Some(root) = root_for(*anchor, &index.parent_of, &mut root_of) {
                    roots.insert(root);
                    reached = true;
                }
            }
            if !reached {
                unreachable.push(correlation.clone());
            }
        }

        if !unreachable.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "recording {}: {} of {} in-scope correlation(s) reach NO execution-graph \
                     root ({}). Their spans would be missing from the record graph with no \
                     sign that anything was dropped.",
                    self.recording_id,
                    unreachable.len(),
                    index.anchors_by_correlation.len(),
                    unreachable.join(", "),
                ),
            ));
        }

        // Keep every node under a reached root — the request's own span tree.
        let mut keep: HashSet<u64> = HashSet::new();
        for node_id in index.parent_of.keys() {
            if let Some(root) = root_for(*node_id, &index.parent_of, &mut root_of) {
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
    /// Anchors from uncorrelated (ambient) events. They contribute roots — the
    /// background traffic's spans belong to the run too — but they are NOT a
    /// case, so they are exempt from the reach-a-root requirement.
    ambient_anchors: BTreeSet<u64>,
    events_in: u64,
    events_kept: u64,
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

/// Walk to the root, memoizing every node on the way. Cycle-guarded: a tape is
/// untrusted input and a parent cycle would otherwise hang the extractor.
fn root_for(
    node: u64,
    parent_of: &HashMap<u64, Option<u64>>,
    memo: &mut HashMap<u64, u64>,
) -> Option<u64> {
    if let Some(root) = memo.get(&node) {
        return Some(*root);
    }
    let mut chain: Vec<u64> = Vec::new();
    let mut seen: HashSet<u64> = HashSet::new();
    let mut current = node;
    let root = loop {
        if let Some(root) = memo.get(&current) {
            break *root;
        }
        if !seen.insert(current) {
            // Cycle: treat the entry point as its own root rather than looping.
            break node;
        }
        chain.push(current);
        match parent_of.get(&current) {
            // A parent the payload does not carry: promote to root.
            Some(Some(parent)) if !parent_of.contains_key(parent) => break current,
            Some(Some(parent)) => current = *parent,
            Some(None) => break current,
            // The anchor names a node absent from the graph entirely.
            None => return None,
        }
    };
    for n in chain {
        memo.insert(n, root);
    }
    Some(root)
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
}

#[derive(serde::Deserialize)]
struct BoundaryEventHead {
    #[serde(default)]
    correlation_id: Option<String>,
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
}
