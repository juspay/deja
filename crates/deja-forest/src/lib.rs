//! Activation forests, and the alignment of two of them.
//!
//! A replay produces two execution graphs — the record graph on the tape and
//! the replay graph in the observed stream — and the verdict has historically
//! used neither, classifying over flat call streams whose every identity
//! (`span_path` strings, FIFO occurrence) is a lossy projection of the trees it
//! declined to look at. This crate is the shared home for both halves of the
//! remedy: turning `(nodes, events)` into forests, and aligning two forests.
//!
//! # Why one crate rather than two implementations
//!
//! The compactor will eventually materialise forests into per-correlation
//! storage, and the orchestrator builds them in process from the tape it
//! already pulls. Those are two ACCESS PATHS to one artifact, and the moment
//! they become two implementations they must agree forever — the
//! producer/consumer split this codebase's rules exist to kill. [`build`] is
//! the single producer. Storage, when it lands, is a cache in front of it.
//!
//! # The object
//!
//! An **activation forest** is acyclic by construction. Nodes are activations,
//! not code sites: the graph layer mints a fresh `node_id` per span instance,
//! so recursion unrolls into a chain rather than closing a cycle. Each node
//! carries exactly one structural edge, `parent_id`, pointing at the span it
//! was created under, and a parent necessarily exists before its child — so
//! creation order is a topological order.
//!
//! It is a FOREST rather than a tree because a node whose parent the tape does
//! not carry is promoted to a root. That is not corruption: a scoped read
//! legitimately holds a subtree whose parent lies outside the scope.
//!
//! `causal_parent_ids` (follows-from edges, for race attribution) make the
//! whole structure a DAG. **Alignment walks `parent_id` only.** Causal edges
//! ride along as annotations for the ported race-evidence rules and are never
//! alignment structure, because occurrence-scoped-to-aligned-parent is only
//! well-defined on a tree.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    hash::{DefaultHasher, Hash, Hasher},
};

pub use deja_core::ExecutionGraphNode;

// ---------------------------------------------------------------------------
// Forest construction — B1/B2
// ---------------------------------------------------------------------------
/// The outcome of resolving an execution-graph node to its structural root.
///
/// Cycles still resolve to the node from which the walk started, preserving
/// the scope extractor's historical behavior. `break_at` identifies the
/// repeated node whose parent edge must be cut when constructing a forest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootResolution {
    Root(u64),
    Cycle { root: u64, break_at: u64 },
    Missing,
}

/// Walk a graph's structural parents to a root, memoizing the traversed path.
///
/// A missing initial node is [`RootResolution::Missing`]. If a carried node
/// names a parent absent from the payload, that node is promoted to a root.
/// Cycles are guarded so malformed graph input cannot make callers hang.
pub fn root_for(
    node: u64,
    parent_of: &HashMap<u64, Option<u64>>,
    memo: &mut HashMap<u64, RootResolution>,
) -> RootResolution {
    if let Some(resolution) = memo.get(&node) {
        return *resolution;
    }

    let mut chain: Vec<u64> = Vec::new();
    let mut seen: HashSet<u64> = HashSet::new();
    let mut current = node;
    let resolution = loop {
        if let Some(resolution) = memo.get(&current) {
            break *resolution;
        }
        if !seen.insert(current) {
            break RootResolution::Cycle {
                root: node,
                break_at: current,
            };
        }
        chain.push(current);
        match parent_of.get(&current) {
            Some(Some(parent)) if !parent_of.contains_key(parent) => {
                break RootResolution::Root(current);
            }
            Some(Some(parent)) => current = *parent,
            Some(None) => break RootResolution::Root(current),
            None => return RootResolution::Missing,
        }
    };

    for traversed in chain {
        memo.insert(traversed, resolution);
    }
    resolution
}

/// One activation, with its events attached and its subtree summarised.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ForestNode {
    pub node_id: u64,
    /// `None` for a root — either a genuine root, or a node PROMOTED to one
    /// because the payload does not carry its parent. [`ForestNode::promoted`]
    /// tells those apart; they are different facts and a reader that conflates
    /// them cannot tell a scoped read from a delivery gap.
    pub parent_id: Option<u64>,
    pub promoted: bool,
    pub span_name: String,
    /// Children in creation order. Alignment consumes this order only WITHIN a
    /// same-named group; across names, sibling sets compare as multisets.
    pub children: Vec<u64>,
    /// Global sequences of the boundary events attached to this node, in tape
    /// order. Attachment is by `graph_node_id` alone — see [`build`].
    pub events: Vec<u64>,
    /// Boundary events in this node's whole subtree, including its own. The
    /// skeleton prune reads this: a subtree with a zero rollup cannot affect
    /// any comparison and is filler.
    pub subtree_events: u64,
}

/// Every activation of one correlation, plus what could not be attached.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct ActivationForest {
    /// `None` is the ambient forest: spans belonging to no request (startup,
    /// background work). Ambient is always in scope and never a test case.
    pub correlation_id: Option<String>,
    pub nodes: BTreeMap<u64, ForestNode>,
    /// Roots in creation order. Aligned as the sibling set of a virtual top
    /// node, by the same (span_name, occurrence) rule as any other siblings.
    pub roots: Vec<u64>,
}

/// Events held out of the forest, each under the cause that put it there.
///
/// The causes are kept apart because they are fixed in different halves of the
/// system, and because two of them are faults while the third is not. An event
/// with no `graph_node_id` is a CAPTURE gap — nothing on the tape ties the call
/// to a span; an event naming an absent node is a DELIVERY gap — the tie exists
/// and the node went missing; an event with no possible counterpart is neither
/// — it is healthy, its tie exists, and it is withheld on purpose. One blanket
/// message sends the reader looking in the wrong place.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct Annex {
    /// Global sequences of events carrying no `graph_node_id`.
    pub names_no_node: Vec<u64>,
    /// Global sequences of events naming a node absent from the payload.
    pub names_absent_node: Vec<u64>,
    /// Global sequences of events the other side cannot produce a counterpart
    /// for however faithfully it behaves, because the harness drives that
    /// boundary rather than the code under test.
    ///
    /// **This is not a fault, and nothing here needs fixing.** Attaching such an
    /// event would make one side's subtree event-bearing while the other's stays
    /// empty, and that asymmetry is structural — no candidate can close it. The
    /// tie between this event and its span exists; it is withheld so the two
    /// sides stay comparable. See [`EventRef::counterpart_possible`].
    pub no_counterpart_by_construction: Vec<u64>,
}

impl Annex {
    /// Every annexed sequence, whatever its cause.
    ///
    /// THE list of buckets, and the only one. [`ForestSet::balance`] sums and
    /// de-duplicates over this, [`build`] cross-checks its sequence set over it,
    /// and the orchestrator's `flat_record_events` / `flat_replay_events` are
    /// built from it — before this existed those carried four separate
    /// hand-written chains, so a new bucket had to be remembered in four places
    /// and the balance passed while `build` still called the event dropped.
    ///
    /// Two consequences a reader adding a bucket needs, because they pull in
    /// opposite directions. Omitting it here fails `build` and `balance` at once,
    /// loudly — that is intended. But the flat-tier sets have no such assertion
    /// behind them, and an event annexed without reaching them is scored by no
    /// tier at all, which the accounting cannot see: it balances, because being
    /// annexed is what balancing asks of it.
    ///
    /// So adding a bucket here also enrols its events in **flat scoring**, and
    /// that is the deliberate default: an annexed event has no node, so the graph
    /// tier has nothing to say about it, and being scored by the wrong tier is a
    /// visible finding while being scored by none is a silent drop. A bucket that
    /// genuinely must not be scored at all needs to say so at the two flat-tier
    /// call sites, and to explain itself there.
    pub fn annexed_sequences(&self) -> impl Iterator<Item = u64> + '_ {
        self.names_no_node
            .iter()
            .chain(&self.names_absent_node)
            .chain(&self.no_counterpart_by_construction)
            .copied()
    }
}

/// The whole of one side: per-correlation forests, the ambient forest, and the
/// annex. Construction ASSERTS that this balances (see [`ForestSet::balance`]).
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct ForestSet {
    pub by_correlation: BTreeMap<String, ActivationForest>,
    pub ambient: ActivationForest,
    pub annex: Annex,
    /// Correlations whose structure could not be trusted — a cycle survived the
    /// guard — mapped to the reason. These demote to the flat tier rather than
    /// being scored, because `PrunedSubtree`/`NovelSubtree` are only
    /// well-defined over acyclic structure.
    pub unusable: BTreeMap<String, String>,
}

/// Why construction refused. Imbalance is a refusal, not a warning: a forest
/// that has silently dropped an event is exactly the artifact this design
/// exists to stop trusting.
#[derive(Debug, Clone, PartialEq)]
pub enum BuildError {
    /// Events in, events accounted for, and the two disagree.
    Imbalanced {
        events_in: u64,
        attached: u64,
        annexed: u64,
    },
    /// A node landed in no forest, or in more than one.
    NodePartition { node_id: u64, forests: usize },
}

impl ForestSet {
    /// Every event is attached to exactly one node or named in the annex, and
    /// every node lives in exactly one forest. Asserted by [`build`]; exposed
    /// so a consumer that receives a `ForestSet` from elsewhere (a future
    /// storage tier) can re-check rather than trust.
    pub fn balance(&self, events_in: u64) -> Result<(), BuildError> {
        let mut placements: HashMap<u64, usize> = HashMap::new();
        let mut accounted_sequences: HashSet<u64> = HashSet::new();
        let mut sequences_are_unique = true;
        let mut attached = 0_u64;

        for forest in std::iter::once(&self.ambient).chain(self.by_correlation.values()) {
            for (node_id, node) in &forest.nodes {
                if *node_id != node.node_id {
                    return Err(BuildError::NodePartition {
                        node_id: node.node_id,
                        forests: 0,
                    });
                }
                *placements.entry(*node_id).or_insert(0) += 1;
                attached += node.events.len() as u64;
                for sequence in &node.events {
                    sequences_are_unique &= accounted_sequences.insert(*sequence);
                }
            }
        }

        if let Some((node_id, forests)) = placements.into_iter().find(|(_, count)| *count != 1) {
            return Err(BuildError::NodePartition { node_id, forests });
        }

        let annexed = self.annex.annexed_sequences().count() as u64;
        for sequence in self.annex.annexed_sequences() {
            sequences_are_unique &= accounted_sequences.insert(sequence);
        }

        if attached + annexed != events_in
            || accounted_sequences.len() as u64 != events_in
            || !sequences_are_unique
        {
            return Err(BuildError::Imbalanced {
                events_in,
                attached,
                annexed,
            });
        }

        Ok(())
    }
}

/// THE producer. `(nodes, events) -> forests`, pure, total, and the only place
/// a forest is ever constructed.
///
/// # The join contract
///
/// Attachment keys on `graph_node_id` **alone**. `recording_run_id` is present
/// on record-side nodes and (after the pin bump) on replay-side ones, but it is
/// a LOUD ASSERTION, never a join column: joining on it would annex every event
/// fleet-wide during the window where one side stamps it and the other does
/// not. A mismatch is reported, not silently dropped.
///
/// # Correlation partitioning
///
/// A node states its own `correlation_id`. `None` means ambient — genuinely
/// belonging to no request — EXCEPT on a tape recorded before nodes carried the
/// field, where it means "not stated". Callers that must tell those apart pass
/// the distinction in; this function treats `None` as ambient and reports the
/// count so a caller can refuse.
pub fn build(nodes: &[ExecutionGraphNode], events: &[EventRef]) -> Result<ForestSet, BuildError> {
    let mut result = ForestSet::default();
    let mut node_partition: HashMap<u64, Option<String>> = HashMap::new();
    let mut creation_sequence: HashMap<u64, u64> = HashMap::new();

    for node in nodes {
        if node_partition
            .insert(node.node_id, node.correlation_id.clone())
            .is_some()
        {
            return Err(BuildError::NodePartition {
                node_id: node.node_id,
                forests: 2,
            });
        }
        creation_sequence.insert(node.node_id, node.sequence);

        let forest = match &node.correlation_id {
            Some(correlation_id) => result
                .by_correlation
                .entry(correlation_id.clone())
                .or_insert_with(|| ActivationForest {
                    correlation_id: Some(correlation_id.clone()),
                    ..ActivationForest::default()
                }),
            None => &mut result.ambient,
        };
        if forest
            .nodes
            .insert(
                node.node_id,
                ForestNode {
                    node_id: node.node_id,
                    parent_id: node.parent_id,
                    promoted: false,
                    span_name: node.span_name.clone(),
                    children: Vec::new(),
                    events: Vec::new(),
                    subtree_events: 0,
                },
            )
            .is_some()
        {
            return Err(BuildError::NodePartition {
                node_id: node.node_id,
                forests: 2,
            });
        }
    }

    let resolve_structure = |forest: &mut ActivationForest| -> Result<Vec<(u64, u64)>, BuildError> {
        let parent_of: HashMap<u64, Option<u64>> = forest
            .nodes
            .iter()
            .map(|(node_id, node)| (*node_id, node.parent_id))
            .collect();
        let mut ordered_nodes: Vec<u64> = forest.nodes.keys().copied().collect();
        ordered_nodes.sort_by_key(|node_id| {
            (
                creation_sequence.get(node_id).copied().unwrap_or_default(),
                *node_id,
            )
        });

        let mut memo: HashMap<u64, RootResolution> = HashMap::new();
        let mut cuts: HashSet<u64> = HashSet::new();
        let mut cycles = std::collections::BTreeSet::new();
        for node_id in &ordered_nodes {
            match root_for(*node_id, &parent_of, &mut memo) {
                RootResolution::Root(_) => {}
                RootResolution::Cycle { root, break_at } => {
                    cuts.insert(break_at);
                    cycles.insert((root, break_at));
                }
                RootResolution::Missing => {
                    return Err(BuildError::NodePartition {
                        node_id: *node_id,
                        forests: 0,
                    });
                }
            }
        }

        for node_id in &ordered_nodes {
            let original_parent = parent_of[node_id];
            let (parent_id, promoted) = if cuts.contains(node_id) {
                (None, false)
            } else {
                match original_parent {
                    Some(parent_id) if forest.nodes.contains_key(&parent_id) => {
                        (Some(parent_id), false)
                    }
                    Some(_) => (None, true),
                    None => (None, false),
                }
            };
            let node = forest
                .nodes
                .get_mut(node_id)
                .expect("ordered node came from this forest");
            node.parent_id = parent_id;
            node.promoted = promoted;
            node.children.clear();
        }

        let edges: Vec<(u64, u64)> = ordered_nodes
            .iter()
            .filter_map(|node_id| {
                forest.nodes[node_id]
                    .parent_id
                    .map(|parent_id| (parent_id, *node_id))
            })
            .collect();
        for (parent_id, child_id) in edges {
            forest
                .nodes
                .get_mut(&parent_id)
                .expect("retained parent is in this forest")
                .children
                .push(child_id);
        }

        forest.roots = ordered_nodes
            .iter()
            .copied()
            .filter(|node_id| forest.nodes[node_id].parent_id.is_none())
            .collect();
        forest.roots.sort_by_key(|node_id| {
            (
                creation_sequence.get(node_id).copied().unwrap_or_default(),
                *node_id,
            )
        });
        for node in forest.nodes.values_mut() {
            node.children.sort_by_key(|node_id| {
                (
                    creation_sequence.get(node_id).copied().unwrap_or_default(),
                    *node_id,
                )
            });
        }

        Ok(cycles.into_iter().collect())
    };

    let ambient_cycles = resolve_structure(&mut result.ambient)?;
    let mut named_cycles = Vec::new();
    for (correlation_id, forest) in &mut result.by_correlation {
        for (root, break_at) in resolve_structure(forest)? {
            named_cycles.push((correlation_id.clone(), root, break_at));
        }
    }
    for (correlation_id, root, break_at) in named_cycles {
        let reason =
            format!("parent cycle resolved at root {root}; cut parent edge at node {break_at}");
        result
            .unusable
            .entry(correlation_id)
            .and_modify(|existing| {
                existing.push_str("; ");
                existing.push_str(&reason);
            })
            .or_insert(reason);
    }
    // `unusable` is keyed by a concrete correlation, so ambient cycles can
    // only be made structurally safe here; there is no key with which to name
    // them as unusable.
    let _ = ambient_cycles;

    for event in events {
        // Checked before `graph_node_id`, and deliberately so: such an event
        // usually HAS a node, and routing it by that node would attach it. The
        // question "can the other side ever match this?" outranks "where did it
        // happen?", because a node the other side cannot reach is not a
        // comparison, it is a guaranteed divergence.
        if !event.counterpart_possible {
            result
                .annex
                .no_counterpart_by_construction
                .push(event.global_sequence);
            continue;
        }
        match event.graph_node_id {
            None => result.annex.names_no_node.push(event.global_sequence),
            Some(node_id) => match node_partition.get(&node_id) {
                None => result.annex.names_absent_node.push(event.global_sequence),
                Some(Some(correlation_id)) => result
                    .by_correlation
                    .get_mut(correlation_id)
                    .expect("node partition created its named forest")
                    .nodes
                    .get_mut(&node_id)
                    .expect("node partition names a node in its forest")
                    .events
                    .push(event.global_sequence),
                Some(None) => result
                    .ambient
                    .nodes
                    .get_mut(&node_id)
                    .expect("node partition names an ambient node")
                    .events
                    .push(event.global_sequence),
            },
        }
    }

    result.annex.names_no_node.sort_unstable();
    result.annex.names_absent_node.sort_unstable();
    result.annex.no_counterpart_by_construction.sort_unstable();
    for forest in std::iter::once(&mut result.ambient).chain(result.by_correlation.values_mut()) {
        for node in forest.nodes.values_mut() {
            node.events.sort_unstable();
            node.subtree_events = node.events.len() as u64;
        }

        let mut remaining_children: HashMap<u64, usize> = forest
            .nodes
            .iter()
            .map(|(node_id, node)| (*node_id, node.children.len()))
            .collect();
        let mut leaves: std::collections::VecDeque<u64> = remaining_children
            .iter()
            .filter_map(|(node_id, remaining)| (*remaining == 0).then_some(*node_id))
            .collect();
        while let Some(node_id) = leaves.pop_front() {
            let node = &forest.nodes[&node_id];
            let parent_id = node.parent_id;
            let subtree_events = node.subtree_events;
            if let Some(parent_id) = parent_id {
                forest
                    .nodes
                    .get_mut(&parent_id)
                    .expect("retained parent is in this forest")
                    .subtree_events += subtree_events;
                let remaining = remaining_children
                    .get_mut(&parent_id)
                    .expect("retained parent has a child count");
                *remaining -= 1;
                if *remaining == 0 {
                    leaves.push_back(parent_id);
                }
            }
        }
    }

    result.balance(events.len() as u64)?;

    let mut expected_sequences: Vec<u64> =
        events.iter().map(|event| event.global_sequence).collect();
    expected_sequences.sort_unstable();
    let mut accounted_sequences: Vec<u64> = result
        .by_correlation
        .values()
        .chain(std::iter::once(&result.ambient))
        .flat_map(|forest| forest.nodes.values())
        .flat_map(|node| node.events.iter().copied())
        .chain(result.annex.annexed_sequences())
        .collect();
    accounted_sequences.sort_unstable();
    if accounted_sequences != expected_sequences {
        let attached = result
            .by_correlation
            .values()
            .chain(std::iter::once(&result.ambient))
            .flat_map(|forest| forest.nodes.values())
            .map(|node| node.events.len() as u64)
            .sum();
        let annexed = result.annex.annexed_sequences().count() as u64;
        return Err(BuildError::Imbalanced {
            events_in: events.len() as u64,
            attached,
            annexed,
        });
    }

    Ok(result)
}

/// The minimum an event contributes to forest construction. Deliberately not
/// `deja::BoundaryEvent`: the forest crate must not depend on the boundary
/// payload types, and construction needs three fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EventRef {
    pub global_sequence: u64,
    pub graph_node_id: Option<u64>,
    /// Stated by the event, used only to cross-check the node's own answer.
    /// A disagreement is reported, never used to re-home the event.
    pub correlation_id_present: bool,
    /// Whether the other side is capable of producing a counterpart for this
    /// event at all.
    ///
    /// `false` for a boundary the harness drives rather than the code under
    /// test: the candidate can behave perfectly and still never emit it, so
    /// comparing its presence measures the harness, not the candidate. Such an
    /// event is annexed to [`Annex::no_counterpart_by_construction`] rather than
    /// attached to its node, which keeps it from making one side's subtree
    /// event-bearing while the other's is empty.
    ///
    /// The caller decides this, because it is a property of the boundary and
    /// this crate deliberately knows nothing about boundaries. `true` is the
    /// answer for every ordinary event, including entropy seams — a substituted
    /// seam still emits an observation on replay, hit or miss.
    pub counterpart_possible: bool,
}

// ---------------------------------------------------------------------------
// Alignment — D1
// ---------------------------------------------------------------------------

/// How one correlation was scored, declared per correlation and never inferred.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ScoringMode {
    /// Both forests existed and aligned.
    Graph,
    /// Scored by the flat span-scoped scorer, for a stated reason. The flat
    /// tier is the declared fallback, not throwaway work.
    Flat { reason: FlatReason },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FlatReason {
    /// One side carries no forest for this correlation (old tape, capture gap).
    MissingForest,
    /// A cycle survived the guard; structure cannot be trusted.
    CycleDetected,
    /// One side has an ingress root and the other does not. Defensive: the
    /// record-only `deja::http_incoming` ingress span asymmetry is a live
    /// producer bug, so this tier must be correct BEFORE that fix reaches the
    /// candidate image, not after.
    IngressRootAsymmetry,
}

/// What alignment concluded about one node pair, or one unaligned node.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NodeOutcome {
    Matched,
    /// Values differ. `origin` iff no aligned ancestor also diverged — under
    /// alignment this is a structural fact, not the heuristic split between two
    /// pairing arms that it is in the flat tier.
    ValueDiverged {
        origin: bool,
    },
    /// The serving ladder bound a recorded event to this node that alignment
    /// did NOT pair here. Counted and listed with both bindings, never scored
    /// as a value divergence — scoring a node with a value the server bound
    /// crosswise manufactures exactly the plausible-and-wrong finding this
    /// design exists to kill.
    IdentitySkew {
        aligned_event: Option<u64>,
        served_event: Option<u64>,
    },
    /// A record subtree with no replay counterpart. ONE finding covering every
    /// call beneath it.
    PrunedSubtree {
        events_below: u64,
    },
    /// A replay subtree with no record counterpart. ONE finding covering every
    /// call beneath it.
    NovelSubtree {
        events_below: u64,
    },
}

/// One aligned or unaligned node, with the outcome it carries.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct AlignedNode {
    pub record_node: Option<u64>,
    pub replay_node: Option<u64>,
    pub span_name: String,
    pub outcome: NodeOutcome,
    /// True when this node sat in a same-named sibling group with more than one
    /// member, so the k-th↔k-th pairing reused each side's local creation order.
    /// The report must present these as lower-confidence alignments, named as
    /// such — a bare span node carries no task lineage to discriminate with.
    pub low_confidence: bool,
}

/// The result of aligning one correlation's two forests.
#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct Alignment {
    pub nodes: Vec<AlignedNode>,
    /// A pruned and a novel subtree under the same aligned parent with matching
    /// shape (same child-name multiset, same boundary-event count). Flagged as
    /// a suspected rename rather than reported as two independent findings —
    /// the record graph is the baseline's code and the replay graph is the
    /// candidate's, so a renamed ancestor makes its whole subtree unalignable
    /// and reads exactly like a real wall. Detected, not solved.
    pub suspected_renames: Vec<(u64, u64)>,
    /// Events that scored through the flat tier because their attachment was
    /// ambiguous. Counted and named per correlation — never a silent guess
    /// among the very occurrence-siblings alignment exists to disambiguate.
    pub flat_tier_events: Vec<u64>,
}

impl Alignment {
    /// `|record nodes| = aligned + pruned` and `|replay nodes| = aligned +
    /// novel`, and every boundary event maps to exactly one node outcome.
    /// Asserted, not logged — the same standard as the flat scorer's backstop.
    pub fn balance(&self, record_nodes: u64, replay_nodes: u64) -> Result<(), String> {
        use std::collections::HashSet;

        let mut seen_record = HashSet::new();
        let mut seen_replay = HashSet::new();
        let mut record_collapsed = false;
        let mut replay_collapsed = false;

        for (index, node) in self.nodes.iter().enumerate() {
            let expected_sides = match &node.outcome {
                NodeOutcome::Matched
                | NodeOutcome::ValueDiverged { .. }
                | NodeOutcome::IdentitySkew { .. } => (true, true),
                NodeOutcome::PrunedSubtree { events_below } => {
                    if *events_below == 0 {
                        return Err(format!(
                            "alignment row {index} collapses a pruned subtree with no events"
                        ));
                    }
                    record_collapsed = true;
                    (true, false)
                }
                NodeOutcome::NovelSubtree { events_below } => {
                    if *events_below == 0 {
                        return Err(format!(
                            "alignment row {index} collapses a novel subtree with no events"
                        ));
                    }
                    replay_collapsed = true;
                    (false, true)
                }
            };

            if node.record_node.is_some() != expected_sides.0
                || node.replay_node.is_some() != expected_sides.1
            {
                return Err(format!(
                    "alignment row {index} has node sides inconsistent with its outcome"
                ));
            }

            if let Some(node_id) = node.record_node {
                if !seen_record.insert(node_id) {
                    return Err(format!(
                        "record node {node_id} is owned by more than one alignment row"
                    ));
                }
            }
            if let Some(node_id) = node.replay_node {
                if !seen_replay.insert(node_id) {
                    return Err(format!(
                        "replay node {node_id} is owned by more than one alignment row"
                    ));
                }
            }
        }

        check_represented_node_count(
            "record",
            seen_record.len() as u64,
            record_nodes,
            record_collapsed,
        )?;
        check_represented_node_count(
            "replay",
            seen_replay.len() as u64,
            replay_nodes,
            replay_collapsed,
        )?;
        Ok(())
    }
}

fn check_represented_node_count(
    side: &str,
    represented: u64,
    provided: u64,
    contains_collapsed_subtree: bool,
) -> Result<(), String> {
    let valid = if contains_collapsed_subtree {
        represented <= provided
    } else {
        represented == provided
    };
    if valid {
        Ok(())
    } else if contains_collapsed_subtree {
        Err(format!(
            "{side} alignment represents {represented} subtree roots, exceeding {provided} nodes"
        ))
    } else {
        Err(format!(
            "{side} alignment represents {represented} nodes, expected {provided}"
        ))
    }
}

/// Align two forests of the SAME correlation.
///
/// A single top-down pass, O(nodes), no search:
///
/// ```text
/// align(record_node, replay_node) when
///   their parents are aligned (roots align per correlation),
///   span_name matches,
///   and they are the k-th same-named child of their parents respectively
/// ```
///
/// Three load-bearing properties:
///
/// 1. **Order-strict along ancestry.** A call under `get_trackers >
///    find_business_profile` is not the same call under `server_wrap >
///    release_lock`, whatever its method name.
/// 2. **Order-free among siblings.** The recording ran concurrent traffic and
///    replay may serialise it differently; sibling sets compare as multisets
///    grouped by name. Wall-clock interleaving carries no meaning.
/// 3. **Occurrence scoped to the parent — a declared trade.** Within one
///    same-named group the k-th↔k-th pairing reuses local creation order, the
///    very temporal signal property 2 discards across names. Optimal
///    within-group matching is deliberately not attempted; that omission is
///    what makes the O(nodes) claim true. Such nodes are marked
///    [`AlignedNode::low_confidence`].
///
/// # Skeleton pruning happens FIRST
///
/// Each side independently drops every node whose `subtree_events` is zero.
/// This is not an optimisation. The replay graph carries thousands of
/// transport activations (`poll_ready`, `FramedRead::poll_next`, …) whose COUNT
/// is load-dependent nondeterminism: two runs of identical behaviour never poll
/// the same number of times, so under naive alignment every count mismatch
/// becomes a fabricated finding — thousands per run, drowning the real ones.
/// The rule is principled rather than a name list: a subtree with no boundary
/// events cannot affect any comparison. A span carrying events on ONE side only
/// still participates — its absence IS the finding.
pub fn align(record: &ActivationForest, replay: &ActivationForest) -> Alignment {
    if has_event_bearing_cycle(record) || has_event_bearing_cycle(replay) {
        let mut flat_tier_events = boundary_events(record);
        flat_tier_events.extend(boundary_events(replay));
        return Alignment {
            flat_tier_events,
            ..Alignment::default()
        };
    }

    let record_excluded = HashSet::new();
    let replay_excluded = HashSet::new();
    let mut alignment = Alignment::default();
    let record_roots = event_bearing(&record.roots, record, &record_excluded);
    let replay_roots = event_bearing(&replay.roots, replay, &replay_excluded);
    let mut seen_pairs = HashSet::new();
    align_siblings(
        &record_roots,
        &replay_roots,
        record,
        replay,
        &record_excluded,
        &replay_excluded,
        &mut seen_pairs,
        &mut alignment,
    );
    alignment
}

fn event_bearing(ids: &[u64], forest: &ActivationForest, excluded: &HashSet<u64>) -> Vec<u64> {
    ids.iter()
        .copied()
        .filter(|id| {
            !excluded.contains(id)
                && forest
                    .nodes
                    .get(id)
                    .is_some_and(|node| node.subtree_events != 0)
        })
        .collect()
}

fn boundary_events(forest: &ActivationForest) -> Vec<u64> {
    forest
        .nodes
        .values()
        .flat_map(|node| node.events.iter().copied())
        .collect()
}

fn has_event_bearing_cycle(forest: &ActivationForest) -> bool {
    let event_nodes: HashSet<_> = forest
        .nodes
        .iter()
        .filter_map(|(&id, node)| (node.subtree_events != 0).then_some(id))
        .collect();

    let mut child_edges = HashMap::<u64, Vec<u64>>::new();
    let mut child_indegrees: HashMap<_, _> = event_nodes
        .iter()
        .copied()
        .map(|id| (id, 0_usize))
        .collect();
    for (&id, node) in &forest.nodes {
        if !event_nodes.contains(&id) {
            continue;
        }
        for &child in &node.children {
            if event_nodes.contains(&child) {
                child_edges.entry(id).or_default().push(child);
                *child_indegrees
                    .get_mut(&child)
                    .expect("event-bearing child has an indegree") += 1;
            }
        }
    }
    if directed_graph_has_cycle(child_indegrees, &child_edges) {
        return true;
    }

    let mut parent_edges = HashMap::<u64, Vec<u64>>::new();
    let mut parent_indegrees: HashMap<_, _> = event_nodes
        .iter()
        .copied()
        .map(|id| (id, 0_usize))
        .collect();
    for (&id, node) in &forest.nodes {
        let Some(parent) = node.parent_id.filter(|parent| event_nodes.contains(parent)) else {
            continue;
        };
        parent_edges.entry(parent).or_default().push(id);
        *parent_indegrees
            .get_mut(&id)
            .expect("event-bearing node has an indegree") += 1;
    }
    directed_graph_has_cycle(parent_indegrees, &parent_edges)
}

fn directed_graph_has_cycle(
    mut indegrees: HashMap<u64, usize>,
    edges: &HashMap<u64, Vec<u64>>,
) -> bool {
    let mut ready: Vec<u64> = indegrees
        .iter()
        .filter_map(|(&id, &indegree)| (indegree == 0).then_some(id))
        .collect();
    let mut visited = 0;
    while let Some(id) = ready.pop() {
        visited += 1;
        if let Some(children) = edges.get(&id) {
            for child in children {
                let indegree = indegrees
                    .get_mut(child)
                    .expect("event-bearing child has an indegree");
                *indegree -= 1;
                if *indegree == 0 {
                    ready.push(*child);
                }
            }
        }
    }
    visited != indegrees.len()
}

struct SiblingGroups {
    order: Vec<String>,
    members: HashMap<String, Vec<u64>>,
}

fn sibling_groups(ids: &[u64], forest: &ActivationForest) -> SiblingGroups {
    let mut order = Vec::new();
    let mut members: HashMap<String, Vec<u64>> = HashMap::new();
    for &id in ids {
        let Some(node) = forest.nodes.get(&id) else {
            continue;
        };
        if !members.contains_key(&node.span_name) {
            order.push(node.span_name.clone());
        }
        members.entry(node.span_name.clone()).or_default().push(id);
    }
    SiblingGroups { order, members }
}

#[derive(PartialEq, Eq)]
struct RenameShape {
    subtree_events: u64,
    child_names: HashMap<String, usize>,
}

impl Hash for RenameShape {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.subtree_events.hash(state);
        self.child_names.len().hash(state);
        let mut sum = 0_u64;
        let mut xor = 0_u64;
        for (name, count) in &self.child_names {
            let mut item = DefaultHasher::new();
            name.hash(&mut item);
            count.hash(&mut item);
            let hash = item.finish();
            sum = sum.wrapping_add(hash);
            xor ^= hash.rotate_left((hash & 63) as u32);
        }
        sum.hash(state);
        xor.hash(state);
    }
}

fn rename_shape(id: u64, forest: &ActivationForest, excluded: &HashSet<u64>) -> RenameShape {
    let node = &forest.nodes[&id];
    let mut child_names = HashMap::new();
    for child in event_bearing(&node.children, forest, excluded) {
        let name = forest.nodes[&child].span_name.clone();
        *child_names.entry(name).or_default() += 1;
    }
    RenameShape {
        subtree_events: node.subtree_events,
        child_names,
    }
}

#[allow(clippy::too_many_arguments)]
fn align_siblings(
    record_ids: &[u64],
    replay_ids: &[u64],
    record: &ActivationForest,
    replay: &ActivationForest,
    record_excluded: &HashSet<u64>,
    replay_excluded: &HashSet<u64>,
    seen_pairs: &mut HashSet<(u64, u64)>,
    alignment: &mut Alignment,
) {
    let record_groups = sibling_groups(record_ids, record);
    let replay_groups = sibling_groups(replay_ids, replay);
    let mut group_order = record_groups.order.clone();
    for name in &replay_groups.order {
        if !record_groups.members.contains_key(name) {
            group_order.push(name.clone());
        }
    }

    let mut unmatched_record = Vec::new();
    let mut unmatched_replay = Vec::new();
    for name in group_order {
        let record_group = record_groups
            .members
            .get(&name)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let replay_group = replay_groups
            .members
            .get(&name)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let paired = record_group.len().min(replay_group.len());
        let low_confidence = record_group.len() > 1 || replay_group.len() > 1;

        for index in 0..paired {
            align_pair(
                record_group[index],
                replay_group[index],
                low_confidence,
                record,
                replay,
                record_excluded,
                replay_excluded,
                seen_pairs,
                alignment,
            );
        }
        unmatched_record.extend(
            record_group[paired..]
                .iter()
                .copied()
                .map(|id| (id, low_confidence)),
        );
        unmatched_replay.extend(
            replay_group[paired..]
                .iter()
                .copied()
                .map(|id| (id, low_confidence)),
        );
    }

    let mut replay_by_shape: HashMap<RenameShape, Vec<usize>> = HashMap::new();
    for (index, &(id, _)) in unmatched_replay.iter().enumerate() {
        replay_by_shape
            .entry(rename_shape(id, replay, replay_excluded))
            .or_default()
            .push(index);
    }
    let mut replay_renamed = vec![false; unmatched_replay.len()];
    for &(record_id, _) in &unmatched_record {
        let shape = rename_shape(record_id, record, record_excluded);
        let Some(candidates) = replay_by_shape.get_mut(&shape) else {
            continue;
        };
        while let Some(replay_index) = candidates.pop() {
            if !replay_renamed[replay_index] {
                replay_renamed[replay_index] = true;
                alignment
                    .suspected_renames
                    .push((record_id, unmatched_replay[replay_index].0));
                break;
            }
        }
    }

    for &(id, low_confidence) in &unmatched_record {
        let node = &record.nodes[&id];
        alignment.nodes.push(AlignedNode {
            record_node: Some(id),
            replay_node: None,
            span_name: node.span_name.clone(),
            outcome: NodeOutcome::PrunedSubtree {
                events_below: node.subtree_events,
            },
            low_confidence,
        });
    }
    for &(id, low_confidence) in &unmatched_replay {
        let node = &replay.nodes[&id];
        alignment.nodes.push(AlignedNode {
            record_node: None,
            replay_node: Some(id),
            span_name: node.span_name.clone(),
            outcome: NodeOutcome::NovelSubtree {
                events_below: node.subtree_events,
            },
            low_confidence,
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn align_pair(
    record_id: u64,
    replay_id: u64,
    low_confidence: bool,
    record: &ActivationForest,
    replay: &ActivationForest,
    record_excluded: &HashSet<u64>,
    replay_excluded: &HashSet<u64>,
    seen_pairs: &mut HashSet<(u64, u64)>,
    alignment: &mut Alignment,
) {
    if !seen_pairs.insert((record_id, replay_id)) {
        alignment
            .flat_tier_events
            .extend(record.nodes[&record_id].events.iter().copied());
        alignment
            .flat_tier_events
            .extend(replay.nodes[&replay_id].events.iter().copied());
        return;
    }

    let record_node = &record.nodes[&record_id];
    let replay_node = &replay.nodes[&replay_id];
    alignment.nodes.push(AlignedNode {
        record_node: Some(record_id),
        replay_node: Some(replay_id),
        span_name: record_node.span_name.clone(),
        outcome: NodeOutcome::Matched,
        low_confidence,
    });

    let record_children = event_bearing(&record_node.children, record, record_excluded);
    let replay_children = event_bearing(&replay_node.children, replay, replay_excluded);
    align_siblings(
        &record_children,
        &replay_children,
        record,
        replay,
        record_excluded,
        replay_excluded,
        seen_pairs,
        alignment,
    );
}

#[cfg(test)]
mod structural_alignment_tests {
    use super::*;

    fn node(
        node_id: u64,
        parent_id: Option<u64>,
        span_name: &str,
        children: &[u64],
        events: &[u64],
        subtree_events: u64,
    ) -> ForestNode {
        ForestNode {
            node_id,
            parent_id,
            promoted: false,
            span_name: span_name.to_owned(),
            children: children.to_vec(),
            events: events.to_vec(),
            subtree_events,
        }
    }

    fn forest(roots: &[u64], nodes: Vec<ForestNode>) -> ActivationForest {
        ActivationForest {
            correlation_id: Some("literal".to_owned()),
            nodes: nodes.into_iter().map(|node| (node.node_id, node)).collect(),
            roots: roots.to_vec(),
        }
    }

    #[test]
    fn wall_collapses_each_unmatched_subtree_to_its_root() {
        let record = forest(
            &[1],
            vec![
                node(1, None, "request", &[2], &[100], 3),
                node(2, Some(1), "record_wall", &[3], &[101], 2),
                node(3, Some(2), "inside", &[], &[102], 1),
            ],
        );
        let replay = forest(
            &[11],
            vec![
                node(11, None, "request", &[12], &[200], 4),
                node(12, Some(11), "replay_wall", &[13], &[201], 3),
                node(13, Some(12), "different_inside", &[], &[202, 203], 2),
            ],
        );

        let alignment = align(&record, &replay);

        assert_eq!(alignment.nodes.len(), 3);
        assert!(matches!(alignment.nodes[0].outcome, NodeOutcome::Matched));
        assert!(matches!(
            alignment.nodes[1].outcome,
            NodeOutcome::PrunedSubtree { events_below: 2 }
        ));
        assert!(matches!(
            alignment.nodes[2].outcome,
            NodeOutcome::NovelSubtree { events_below: 3 }
        ));
        assert!(alignment.suspected_renames.is_empty());
    }

    #[test]
    fn differently_named_siblings_align_regardless_of_order() {
        let record = forest(
            &[1],
            vec![
                node(1, None, "root", &[2, 3], &[], 2),
                node(2, Some(1), "alpha", &[], &[10], 1),
                node(3, Some(1), "beta", &[], &[11], 1),
            ],
        );
        let replay = forest(
            &[11],
            vec![
                node(11, None, "root", &[13, 12], &[], 2),
                node(12, Some(11), "alpha", &[], &[20], 1),
                node(13, Some(11), "beta", &[], &[21], 1),
            ],
        );

        let alignment = align(&record, &replay);
        let pairs: Vec<_> = alignment
            .nodes
            .iter()
            .map(|node| (node.record_node, node.replay_node))
            .collect();

        assert_eq!(
            pairs,
            vec![
                (Some(1), Some(11)),
                (Some(2), Some(12)),
                (Some(3), Some(13))
            ]
        );
        assert!(alignment
            .nodes
            .iter()
            .all(|node| matches!(node.outcome, NodeOutcome::Matched)));
    }

    #[test]
    fn empty_forests_produce_no_alignment_evidence() {
        let alignment = align(&ActivationForest::default(), &ActivationForest::default());

        assert!(alignment.nodes.is_empty());
        assert!(alignment.suspected_renames.is_empty());
        assert!(alignment.flat_tier_events.is_empty());
    }

    #[test]
    fn suspected_rename_is_sideband_over_the_two_accounting_rows() {
        let record = forest(
            &[1],
            vec![
                node(1, None, "root", &[2], &[], 2),
                node(2, Some(1), "old_name", &[3], &[10], 2),
                node(3, Some(2), "leaf", &[], &[11], 1),
            ],
        );
        let replay = forest(
            &[11],
            vec![
                node(11, None, "root", &[12], &[], 2),
                node(12, Some(11), "new_name", &[13], &[20], 2),
                node(13, Some(12), "leaf", &[], &[21], 1),
            ],
        );

        let alignment = align(&record, &replay);

        assert_eq!(alignment.suspected_renames, vec![(2, 12)]);
        assert_eq!(alignment.nodes.len(), 3);
        assert!(matches!(
            alignment.nodes[1].outcome,
            NodeOutcome::PrunedSubtree { events_below: 2 }
        ));
        assert!(matches!(
            alignment.nodes[2].outcome,
            NodeOutcome::NovelSubtree { events_below: 2 }
        ));
    }

    #[test]
    fn skeleton_pruning_is_independent_on_each_side() {
        let record = forest(
            &[1, 2],
            vec![
                node(1, None, "record_filler", &[], &[], 0),
                node(2, None, "work", &[], &[10], 1),
            ],
        );
        let replay = forest(
            &[11, 12],
            vec![
                node(11, None, "work", &[], &[20], 1),
                node(12, None, "replay_filler", &[], &[], 0),
            ],
        );

        let alignment = align(&record, &replay);

        assert_eq!(alignment.nodes.len(), 1);
        assert_eq!(alignment.nodes[0].record_node, Some(2));
        assert_eq!(alignment.nodes[0].replay_node, Some(11));
        assert!(matches!(alignment.nodes[0].outcome, NodeOutcome::Matched));
    }

    #[test]
    fn repeated_names_pair_by_occurrence_and_mark_every_group_row() {
        let record = forest(
            &[1, 2, 3],
            vec![
                node(1, None, "same", &[], &[10], 1),
                node(2, None, "same", &[], &[11], 1),
                node(3, None, "same", &[], &[12], 1),
            ],
        );
        let replay = forest(
            &[11, 12],
            vec![
                node(11, None, "same", &[], &[20], 1),
                node(12, None, "same", &[], &[21], 1),
            ],
        );

        let alignment = align(&record, &replay);
        let pairs: Vec<_> = alignment
            .nodes
            .iter()
            .map(|node| (node.record_node, node.replay_node))
            .collect();

        assert_eq!(
            pairs,
            vec![(Some(1), Some(11)), (Some(2), Some(12)), (Some(3), None)]
        );
        assert!(alignment.nodes.iter().all(|node| node.low_confidence));
    }

    #[test]
    fn cycle_demotes_the_entire_correlation_instead_of_recursing() {
        let record = forest(
            &[1],
            vec![
                node(1, None, "cycle_a", &[2], &[10], 2),
                node(2, Some(1), "cycle_b", &[1], &[11], 1),
            ],
        );
        let replay = forest(
            &[20],
            vec![node(20, None, "otherwise_alignable", &[], &[20], 1)],
        );

        let alignment = align(&record, &replay);

        assert!(alignment.nodes.is_empty());
        assert_eq!(alignment.flat_tier_events, vec![10, 11, 20]);
    }

    #[test]
    fn parent_cycle_is_detected_even_without_a_reachable_root() {
        let record = forest(
            &[],
            vec![
                node(1, Some(2), "cycle_a", &[], &[10], 2),
                node(2, Some(1), "cycle_b", &[], &[11], 1),
            ],
        );

        let alignment = align(&record, &ActivationForest::default());

        assert!(alignment.nodes.is_empty());
        assert_eq!(alignment.flat_tier_events, vec![10, 11]);
    }
}

/// Reconcile alignment against what the serving ladder actually did.
///
/// Replay SERVES through the lookup ladder (`LookupKey::occurrence`, FIFO per
/// (correlation, bucket, address, args)) while the aligner SCORES by
/// k-th-child-of-aligned-parent. Two different occurrence schemes, and nothing
/// forces them to agree: on a loop of same-named calls the ladder may bind
/// recorded result *n* to the node alignment calls *k*.
///
/// The reconciliation is evidence, not assumption. Every observed call records
/// which recorded event the ladder served (`source_event_global_sequence`).
/// Where the served event is attached to the node alignment paired, the two
/// identities agree and the comparison is anchored. Where they disagree, the
/// node becomes [`NodeOutcome::IdentitySkew`] — reported, not scored.
///
/// `served` maps replay node IDs to the global sequence of the recorded event
/// served to that node.
pub fn reconcile_serving(
    alignment: &mut Alignment,
    served: &BTreeMap<u64, u64>,
    record: &ActivationForest,
) {
    for node in &mut alignment.nodes {
        let (Some(record_node), Some(replay_node)) = (node.record_node, node.replay_node) else {
            continue;
        };
        let Some(&served_event) = served.get(&replay_node) else {
            continue;
        };
        let aligned_events = record
            .nodes
            .get(&record_node)
            .map(|node| node.events.as_slice())
            .unwrap_or_default();

        if !aligned_events.contains(&served_event) {
            let aligned_event = match aligned_events {
                [event] => Some(*event),
                _ => None,
            };
            node.outcome = NodeOutcome::IdentitySkew {
                aligned_event,
                served_event: Some(served_event),
            };
        }
    }
}

#[cfg(test)]
mod serving_balance_tests {
    use super::*;

    fn forest(nodes: Vec<ForestNode>, roots: Vec<u64>) -> ActivationForest {
        ActivationForest {
            correlation_id: Some("correlation".to_owned()),
            nodes: nodes.into_iter().map(|node| (node.node_id, node)).collect(),
            roots,
        }
    }

    fn node(
        node_id: u64,
        parent_id: Option<u64>,
        span_name: &str,
        children: Vec<u64>,
        events: Vec<u64>,
        subtree_events: u64,
    ) -> ForestNode {
        ForestNode {
            node_id,
            parent_id,
            promoted: false,
            span_name: span_name.to_owned(),
            children,
            events,
            subtree_events,
        }
    }

    fn aligned(
        record_node: Option<u64>,
        replay_node: Option<u64>,
        outcome: NodeOutcome,
    ) -> AlignedNode {
        AlignedNode {
            record_node,
            replay_node,
            span_name: "call".to_owned(),
            outcome,
            low_confidence: false,
        }
    }

    #[test]
    fn reconcile_serving_marks_crosswise_same_name_bindings_as_identity_skew() {
        let record = forest(
            vec![
                node(1, None, "root", vec![2, 3], vec![], 2),
                node(2, Some(1), "same_statement", vec![], vec![101], 1),
                node(3, Some(1), "same_statement", vec![], vec![202], 1),
            ],
            vec![1],
        );
        let replay = forest(
            vec![
                node(10, None, "root", vec![11, 12], vec![], 2),
                node(11, Some(10), "same_statement", vec![], vec![301], 1),
                node(12, Some(10), "same_statement", vec![], vec![302], 1),
            ],
            vec![10],
        );
        let mut alignment = align(&record, &replay);
        let served = BTreeMap::from([(11, 202), (12, 101)]);

        reconcile_serving(&mut alignment, &served, &record);

        let first = alignment
            .nodes
            .iter()
            .find(|node| node.replay_node == Some(11))
            .expect("first repeated replay node must be aligned");
        assert_eq!(first.record_node, Some(2));
        assert!(first.low_confidence);
        assert_eq!(
            first.outcome,
            NodeOutcome::IdentitySkew {
                aligned_event: Some(101),
                served_event: Some(202),
            }
        );
        let second = alignment
            .nodes
            .iter()
            .find(|node| node.replay_node == Some(12))
            .expect("second repeated replay node must be aligned");
        assert_eq!(second.record_node, Some(3));
        assert!(second.low_confidence);
        assert_eq!(
            second.outcome,
            NodeOutcome::IdentitySkew {
                aligned_event: Some(202),
                served_event: Some(101),
            }
        );
    }

    #[test]
    fn reconcile_serving_overrides_value_divergence_but_leaves_unmatched_rows() {
        let record = forest(vec![node(1, None, "call", vec![], vec![101], 1)], vec![1]);
        let mut alignment = Alignment {
            nodes: vec![
                aligned(
                    Some(1),
                    Some(10),
                    NodeOutcome::ValueDiverged { origin: true },
                ),
                aligned(
                    Some(2),
                    None,
                    NodeOutcome::PrunedSubtree { events_below: 3 },
                ),
                aligned(
                    None,
                    Some(12),
                    NodeOutcome::NovelSubtree { events_below: 2 },
                ),
            ],
            ..Alignment::default()
        };

        reconcile_serving(
            &mut alignment,
            &BTreeMap::from([(10, 202), (12, 303)]),
            &record,
        );

        assert_eq!(
            alignment.nodes[0].outcome,
            NodeOutcome::IdentitySkew {
                aligned_event: Some(101),
                served_event: Some(202),
            }
        );
        assert_eq!(
            alignment.nodes[1].outcome,
            NodeOutcome::PrunedSubtree { events_below: 3 }
        );
        assert_eq!(
            alignment.nodes[2].outcome,
            NodeOutcome::NovelSubtree { events_below: 2 }
        );
    }

    #[test]
    fn reconcile_serving_accepts_attached_events_and_never_guesses_an_ambiguous_binding() {
        let record = forest(
            vec![node(1, None, "call", vec![], vec![101, 102], 2)],
            vec![1],
        );
        let mut alignment = Alignment {
            nodes: vec![aligned(Some(1), Some(10), NodeOutcome::Matched)],
            ..Alignment::default()
        };

        reconcile_serving(&mut alignment, &BTreeMap::from([(10, 102)]), &record);
        assert_eq!(alignment.nodes[0].outcome, NodeOutcome::Matched);

        reconcile_serving(&mut alignment, &BTreeMap::from([(10, 999)]), &record);
        assert_eq!(
            alignment.nodes[0].outcome,
            NodeOutcome::IdentitySkew {
                aligned_event: None,
                served_event: Some(999),
            }
        );
    }

    #[test]
    fn balance_accepts_exact_rows_and_collapsed_subtree_lower_bounds() {
        let exact = Alignment {
            nodes: vec![
                aligned(Some(1), Some(10), NodeOutcome::Matched),
                aligned(
                    Some(2),
                    Some(11),
                    NodeOutcome::IdentitySkew {
                        aligned_event: Some(101),
                        served_event: Some(202),
                    },
                ),
            ],
            ..Alignment::default()
        };
        assert_eq!(exact.balance(2, 2), Ok(()));

        let collapsed = Alignment {
            nodes: vec![
                aligned(Some(1), Some(10), NodeOutcome::Matched),
                aligned(
                    Some(2),
                    None,
                    NodeOutcome::PrunedSubtree { events_below: 4 },
                ),
                aligned(
                    None,
                    Some(11),
                    NodeOutcome::NovelSubtree { events_below: 3 },
                ),
            ],
            ..Alignment::default()
        };
        assert_eq!(collapsed.balance(8, 7), Ok(()));
    }

    #[test]
    fn balance_rejects_outcome_side_mismatch() {
        let alignment = Alignment {
            nodes: vec![aligned(Some(1), None, NodeOutcome::Matched)],
            ..Alignment::default()
        };
        assert!(alignment.balance(1, 0).unwrap_err().contains("row 0"));
    }

    #[test]
    fn balance_rejects_duplicate_node_ownership_on_either_side() {
        let duplicate_record = Alignment {
            nodes: vec![
                aligned(Some(1), Some(10), NodeOutcome::Matched),
                aligned(Some(1), Some(11), NodeOutcome::Matched),
            ],
            ..Alignment::default()
        };
        assert!(duplicate_record
            .balance(2, 2)
            .unwrap_err()
            .contains("record node 1"));

        let duplicate_replay = Alignment {
            nodes: vec![
                aligned(Some(1), Some(10), NodeOutcome::Matched),
                aligned(Some(2), Some(10), NodeOutcome::Matched),
            ],
            ..Alignment::default()
        };
        assert!(duplicate_replay
            .balance(2, 2)
            .unwrap_err()
            .contains("replay node 10"));
    }

    #[test]
    fn balance_rejects_exact_count_mismatches_and_impossible_collapsed_bounds() {
        let exact = Alignment {
            nodes: vec![aligned(Some(1), Some(10), NodeOutcome::Matched)],
            ..Alignment::default()
        };
        assert!(exact.balance(2, 1).unwrap_err().contains("record"));
        assert!(exact.balance(1, 2).unwrap_err().contains("replay"));

        let collapsed = Alignment {
            nodes: vec![aligned(
                Some(1),
                None,
                NodeOutcome::PrunedSubtree { events_below: 2 },
            )],
            ..Alignment::default()
        };
        assert!(collapsed.balance(0, 0).unwrap_err().contains("exceeding"));

        let empty_collapsed = Alignment {
            nodes: vec![aligned(
                Some(1),
                None,
                NodeOutcome::PrunedSubtree { events_below: 0 },
            )],
            ..Alignment::default()
        };
        assert!(empty_collapsed
            .balance(1, 0)
            .unwrap_err()
            .contains("no events"));
    }
}
