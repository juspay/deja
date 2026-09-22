//! Is a differing integer leaf a stopwatch reading of a span in its own run?
//!
//! A vendor that measures its own work — `Instant::now()` then `elapsed()` — and
//! puts the reading in an outgoing request body makes that body differ on every
//! replay, because a substituted call returns in microseconds and a live one did
//! not. The reading is not behaviour; it is the clock, which deja already treats
//! as entropy everywhere it can intercept. It cannot intercept this one: the
//! measurement is `std`, taken inside the vendor, and by the time deja sees the
//! value it is an ordinary integer in a body.
//!
//! So it is DERIVED rather than declared. deja records an execution graph on
//! both sides, and each node carries when it opened and closed. If a recorded
//! leaf equals the floored duration of a span in the recorded correlation, and
//! the span at that same path in the replayed correlation floors to the observed
//! leaf, the leaf is that span's elapsed time and the difference is the clock.
//!
//! No configuration, no schema, no vendor change, and it works on tapes already
//! sealed — which the alternative (a deployment declaring the path non-identity)
//! does not, and which a vendor-side seam on the elapsed reading would only fix
//! for new builds.
//!
//! **The recorded side carries the evidence.** The rule keys on the RECORDED
//! value identifying a span, and asks only that the replay side be CONSISTENT
//! with it. A rule that fired on an observed-side match alone would launder every
//! small number into an absorption.
//!
//! **ANCESTRY is what makes the identification sound — not magnitude.** The
//! intuition that a large distinctive value picks out one span is wrong, and
//! measurement says so: collisions cluster at the TOP of a correlation's range,
//! not the bottom, because a dominating child hands every ancestor its own wall
//! time. Across 111,926 spans in 8 graphs, a duration shared by several spans is
//! a single nesting chain 91.2% of the time at >=1000 ms but only 14.6% of the
//! time at 0 ms. So duplicates at the top are one measurement seen at several
//! depths (harmless), and duplicates at the bottom are unrelated spans
//! coinciding (genuine ambiguity). No threshold separates those, because it is
//! not a magnitude question. Ancestry separates them exactly.
//!
//! The motivating case is itself a three-way collision — the reading matched
//! `send_request` (4004.263 ms), its parent (4004.404 ms) and its grandparent
//! (4004.921 ms). One call measured at three depths, admitted by ancestry and
//! named at the deepest. A uniqueness rule without ancestry would have refused
//! the very case this exists for.
//!
//! **What the measurement did not cover.** The distributions above were taken
//! over each node's OWN duration, `floor((closed_ns - started_ns) / 1e6)`, which
//! is exactly what this module compares — so they transfer. They are the RECORDED
//! side only; replay-side multiplicity was not measurable on the same footing, so
//! the replay check is deliberately the weak half (consistency, not
//! identification) and nothing here rests on it.
//!
//! **The case this rule cannot detect.** In a correlation where exactly ONE span
//! sits above the floor, uniqueness is satisfied trivially rather than earned:
//! there is no rival span for ancestry to refuse, so a body integer that happens
//! to coincide with that span's duration is absorbed on no real evidence. The
//! floor cannot help, since the coincidence is above it by construction. "Unique"
//! reads as strong evidence and in a one-span correlation it is none.
//!
//! **Known limits, stated rather than solved.** A vendor that ACCUMULATES the
//! reading across retries (`val + elapsed`) produces a leaf that is the sum of
//! several spans and matches none of them: it keeps blocking, correctly, because
//! this rule cannot prove what it is. Units other than milliseconds are not
//! attempted. Both are cases where blocking is the honest answer.

use std::collections::BTreeMap;

use deja_core::ExecutionGraphNode;

/// Below this many milliseconds a duration carries no information, so a match on
/// one is not evidence of anything.
///
/// This is NOT the rule's discriminator — ancestry is (see the module docs). The
/// floor exists only because ancestry can be satisfied by coincidence at the
/// dense end: a parent and child that both took 0 ms are a chain, and that chain
/// means nothing. So the floor's job is narrow — exclude the band where the
/// clock has not resolved anything yet.
///
/// MEASURED, and that is what picks the value: 78% of spans are exactly 0 ms and
/// 96.8% are <= 10 ms, stable across both measured groups. Above 10 ms a duration
/// has begun to say something; below it, it has not.
///
/// A LARGER floor was tried and is wrong. 50 ms was exceeded by the
/// highest-unrelated-collision ceiling in 106 of 145 correlations, sits below
/// that ceiling's median of 69 ms, and is not stable between groups (p25 66 ms
/// against 14 ms). Raising the floor also makes the top-end collisions WORSE,
/// since those are the ones that grow with duration. A bigger number here buys
/// nothing and misdescribes what the floor is for.
pub(crate) const MIN_EVIDENTIAL_MS: u64 = 10;

/// Why a leaf was or was not explained.
///
/// `NoReplayEvidence` exists because the alternative is a SILENT DEAD FEATURE.
/// The replay side is found by filtering replay graph nodes to this correlation,
/// and if those nodes carry no correlation at all the filter matches nothing, the
/// rule absorbs nothing, and it looks exactly like "this leaf was not a stopwatch
/// reading". Every unit test still passes, because fixtures build their nodes
/// directly rather than loading an artifact, so nothing in the suite can see it.
///
/// So an inability to judge is reported as itself rather than folded into a
/// refusal: the recorded side identified a span, and the replay side supplied
/// nothing to check it against. A caller that sees this on real data is looking
/// at a broken join, not at a leaf that failed the test.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ElapsedVerdict {
    /// A stopwatch reading of a span, identified on both sides.
    Explained(ElapsedMatch),
    /// The recorded side identified a span; the replay side has no spans in this
    /// correlation at all, so consistency could not be tested.
    NoReplayEvidence { span_path: String },
    /// Not a stopwatch reading.
    Refused,
}

/// What a leaf turned out to be, so an absorption can say so instead of going
/// quiet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ElapsedMatch {
    /// Root-to-span names, the same place in both trees.
    pub(crate) span_path: String,
    pub(crate) recorded_ms: u64,
    pub(crate) observed_ms: u64,
}

/// Floored milliseconds between a span opening and closing. `None` when it never
/// closed — an unclosed span has no duration, and guessing one would invent the
/// evidence this rule rests on.
fn duration_ms(node: &ExecutionGraphNode) -> Option<u64> {
    let closed = node.closed_ns?;
    Some(closed.checked_sub(node.started_ns)? / 1_000_000)
}

/// Root-to-node span names, `>`-joined. "The same span" has to mean the same
/// place in the tree: a name repeated at two depths is two different spans, and
/// pairing them by name alone would compare a wrapper against what it wraps.
fn span_path(node: &ExecutionGraphNode, by_id: &BTreeMap<u64, &ExecutionGraphNode>) -> String {
    let mut names = vec![node.span_name.as_str()];
    let mut parent = node.parent_id;
    // Bounded by the number of nodes: a cycle would otherwise hang the scorer,
    // and a graph is not required to be acyclic by anything this reads.
    let mut seen = 0usize;
    while let Some(id) = parent {
        if seen > by_id.len() {
            break;
        }
        let Some(ancestor) = by_id.get(&id) else {
            break;
        };
        names.push(ancestor.span_name.as_str());
        parent = ancestor.parent_id;
        seen += 1;
    }
    names.reverse();
    names.join(">")
}

fn in_correlation<'a>(
    graph: &'a [ExecutionGraphNode],
    correlation: &str,
) -> Vec<&'a ExecutionGraphNode> {
    graph
        .iter()
        .filter(|node| node.correlation_id.as_deref() == Some(correlation))
        .collect()
}

/// Do these nodes form ONE ancestry chain — a call and the wrappers around it?
///
/// Several spans legitimately read the same duration when one encloses another
/// and almost nothing happens between them. That is one call measured at several
/// depths, not an ambiguous match, so "unique" means unique up to ancestry.
/// Unrelated spans that happen to share a duration are ambiguous and refused.
fn one_ancestry_chain(
    nodes: &[&ExecutionGraphNode],
    by_id: &BTreeMap<u64, &ExecutionGraphNode>,
) -> bool {
    let ids: std::collections::BTreeSet<u64> = nodes.iter().map(|node| node.node_id).collect();
    // Every node but one must have an ancestor inside the set: walking up from
    // each, exactly one reaches the top without meeting another member.
    let mut rootless = 0usize;
    for node in nodes {
        let mut parent = node.parent_id;
        let mut seen = 0usize;
        let mut found = false;
        while let Some(id) = parent {
            if seen > by_id.len() {
                break;
            }
            if ids.contains(&id) {
                found = true;
                break;
            }
            let Some(ancestor) = by_id.get(&id) else {
                break;
            };
            parent = ancestor.parent_id;
            seen += 1;
        }
        if !found {
            rootless += 1;
        }
    }
    rootless == 1
}

/// `Some` when the two leaves are one span's elapsed time, read on each side of
/// the run.
pub(crate) fn elapsed_derived(
    recorded: u64,
    observed: u64,
    correlation: &str,
    record_graph: &[ExecutionGraphNode],
    replay_graph: &[ExecutionGraphNode],
) -> ElapsedVerdict {
    if recorded < MIN_EVIDENTIAL_MS {
        return ElapsedVerdict::Refused;
    }
    let recorded_nodes = in_correlation(record_graph, correlation);
    let recorded_by_id: BTreeMap<u64, &ExecutionGraphNode> = recorded_nodes
        .iter()
        .map(|node| (node.node_id, *node))
        .collect();

    let matches: Vec<&ExecutionGraphNode> = recorded_nodes
        .iter()
        .filter(|node| duration_ms(node) == Some(recorded))
        .copied()
        .collect();
    if matches.is_empty() {
        return ElapsedVerdict::Refused;
    }
    if matches.len() > 1 && !one_ancestry_chain(&matches, &recorded_by_id) {
        return ElapsedVerdict::Refused;
    }
    // The DEEPEST match names the call itself rather than a wrapper around it,
    // so the path is the most specific one the evidence supports.
    let Some(deepest) = matches
        .iter()
        .max_by_key(|node| span_path(node, &recorded_by_id).matches('>').count())
    else {
        return ElapsedVerdict::Refused;
    };
    let path = span_path(deepest, &recorded_by_id);

    let replay_nodes = in_correlation(replay_graph, correlation);
    let replay_by_id: BTreeMap<u64, &ExecutionGraphNode> = replay_nodes
        .iter()
        .map(|node| (node.node_id, *node))
        .collect();
    // CONSISTENCY, not proof: the recorded side already identified the span. The
    // replay side only has to agree that the span at that path read what the
    // candidate sent.
    // NOTHING to check against is not the same as DISAGREEING. Say which.
    if replay_nodes.is_empty() {
        return ElapsedVerdict::NoReplayEvidence { span_path: path };
    }
    let consistent = replay_nodes
        .iter()
        .filter(|node| span_path(node, &replay_by_id) == path)
        .any(|node| duration_ms(node) == Some(observed));
    if consistent {
        ElapsedVerdict::Explained(ElapsedMatch {
            span_path: path,
            recorded_ms: recorded,
            observed_ms: observed,
        })
    } else {
        ElapsedVerdict::Refused
    }
}

/// A captured body is judged by its CONTENT.
///
/// `bytes_len`, `raw_bytes` and `text` are renderings of `json`: change one leaf
/// inside the document and all three move with it. Comparing them would re-block
/// precisely what the content rule absorbed, so the envelope is projected to the
/// document it carries before anything is compared. This is the principle
/// `hash_request_body` already applies when addressing a call, applied when
/// judging one.
///
/// This is the SAME BUG AT TWO LAYERS, and worth naming as such because there
/// will be a third. The pairing shape carried the body's encoded size, so a call
/// failed to find its own twin; judgment compared the body's renderings, so a
/// pair that did find its twin blocked on the encoding of a difference already
/// explained. Both are a rendering of content being mistaken for the content.
/// Wherever a captured body is read, read what it carries.
///
/// An envelope whose capture FAILED (`captured: false`, or no `json`) is left
/// alone: there is no content to judge by, and substituting one would invent it.
fn project_content(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let captured = map
                .get("captured")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false);
            if captured {
                if let Some(json) = map.get("json").filter(|json| !json.is_null()) {
                    return project_content(json);
                }
            }
            serde_json::Value::Object(
                map.iter()
                    .map(|(key, child)| (key.clone(), project_content(child)))
                    .collect(),
            )
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(project_content).collect())
        }
        other => other.clone(),
    }
}

/// Every leaf that differs between two documents, by path.
///
/// A STRUCTURAL difference — a key on one side only, arrays of different length,
/// a type change — is reported as a difference at its own path with no leaf pair
/// to explain it, which is what makes it unexplainable and therefore blocking.
fn differing_leaves(
    recorded: &serde_json::Value,
    observed: &serde_json::Value,
    path: &str,
    out: &mut Vec<(String, serde_json::Value, serde_json::Value)>,
) {
    if recorded == observed {
        return;
    }
    match (recorded, observed) {
        (serde_json::Value::Object(r), serde_json::Value::Object(o)) => {
            let mut keys: Vec<&String> = r.keys().chain(o.keys()).collect();
            keys.sort();
            keys.dedup();
            for key in keys {
                let next = format!("{path}.{key}");
                match (r.get(key), o.get(key)) {
                    (Some(rv), Some(ov)) => differing_leaves(rv, ov, &next, out),
                    // Present on one side only: structural, and nothing about a
                    // clock explains a field appearing or vanishing.
                    (rv, ov) => out.push((
                        next,
                        rv.cloned().unwrap_or(serde_json::Value::Null),
                        ov.cloned().unwrap_or(serde_json::Value::Null),
                    )),
                }
            }
        }
        (serde_json::Value::Array(r), serde_json::Value::Array(o)) if r.len() == o.len() => {
            for (index, (rv, ov)) in r.iter().zip(o).enumerate() {
                differing_leaves(rv, ov, &format!("{path}[{index}]"), out);
            }
        }
        (r, o) => out.push((path.to_owned(), r.clone(), o.clone())),
    }
}

/// The pair-level answer, carrying the same inability-to-judge distinction.
///
/// **A REQUIREMENT ON WHOEVER WIRES THIS.** The three variants must stay three at
/// the reporting boundary. If `NoReplayEvidence` collapses into "not absorbed"
/// alongside `Refused`, an operator sees a blocking divergence and cannot tell
/// "the rule judged this and refused it" from "the rule could not check it at
/// all" — which is precisely the distinction this type exists to carry, thrown
/// away one layer up. The second case means a join went empty on real data and is
/// a defect in the scorer; the first is the rule working. They must not read the
/// same.
///
/// So the warning this feeds has to name which silence applied, the same way the
/// absorption names the span it matched. A variant that is load-bearing in the
/// type and mutation-killed in the suite is still invisible to a person if
/// nothing surfaces it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PairElapsed {
    Explained(Vec<ElapsedMatch>),
    NoReplayEvidence { span_path: String },
    Refused,
}

/// `Explained` when the pair differs ONLY in leaves that are stopwatch readings.
///
/// One genuinely changed leaf beside a timing one still blocks: the timing leaf
/// is explained, the other is not, and a pair is absorbed only when every
/// difference in it has an explanation. The matches are returned so the caller
/// can NAME what it forgave — an absorption that cannot say which span it
/// matched is the silent kind this rule must not be.
pub(crate) fn pair_differs_only_by_elapsed(
    recorded: &serde_json::Value,
    observed: &serde_json::Value,
    correlation: &str,
    record_graph: &[ExecutionGraphNode],
    replay_graph: &[ExecutionGraphNode],
) -> PairElapsed {
    let recorded = project_content(recorded);
    let observed = project_content(observed);
    let mut leaves = Vec::new();
    differing_leaves(&recorded, &observed, "$", &mut leaves);
    if leaves.is_empty() {
        return PairElapsed::Refused;
    }
    let mut matched = Vec::with_capacity(leaves.len());
    for (_, recorded_leaf, observed_leaf) in &leaves {
        let (Some(recorded_ms), Some(observed_ms)) =
            (recorded_leaf.as_u64(), observed_leaf.as_u64())
        else {
            return PairElapsed::Refused;
        };
        match elapsed_derived(
            recorded_ms,
            observed_ms,
            correlation,
            record_graph,
            replay_graph,
        ) {
            ElapsedVerdict::Explained(found) => matched.push(found),
            // Propagated, not swallowed: a pair that could not be judged must not
            // read as a pair that was judged and refused.
            ElapsedVerdict::NoReplayEvidence { span_path } => {
                return PairElapsed::NoReplayEvidence { span_path }
            }
            ElapsedVerdict::Refused => return PairElapsed::Refused,
        }
    }
    PairElapsed::Explained(matched)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// Span names here are deliberately generic. deja names no vendor's spans,
    /// and a fixture that did would pin this rule to one service's shape.
    fn node(
        node_id: u64,
        parent_id: Option<u64>,
        span_name: &str,
        correlation: &str,
        started_ns: u64,
        closed_ns: Option<u64>,
    ) -> ExecutionGraphNode {
        ExecutionGraphNode {
            node_id,
            global_sequence: node_id,
            parent_id,
            causal_parent_ids: Vec::new(),
            sequence: node_id,
            correlation_id: Some(correlation.to_owned()),
            recording_run_id: None,
            span_name: span_name.to_owned(),
            target: "t".to_owned(),
            level: "INFO".to_owned(),
            fields: Default::default(),
            started_ns,
            closed_ns,
        }
    }

    const MS: u64 = 1_000_000;
    const C: &str = "corr-1";

    /// The shape this rule was built from: a live call took seconds, the
    /// substituted one returned instantly, and the vendor had written its own
    /// stopwatch reading into the request body.
    #[test]
    fn a_reading_of_one_span_is_absorbed_and_names_the_span() {
        let record = vec![
            node(1, None, "root", C, 0, Some(5000 * MS)),
            node(2, Some(1), "work", C, 0, Some(4004 * MS + 260_000)),
        ];
        // Replay: the same span, served from tape, took under a millisecond.
        let replay = vec![
            node(1, None, "root", C, 0, Some(9 * MS)),
            node(2, Some(1), "work", C, 0, Some(240_000)),
        ];
        let ElapsedVerdict::Explained(found) = elapsed_derived(4004, 0, C, &record, &replay) else {
            panic!("expected an explained reading");
        };
        assert_eq!(found.span_path, "root>work");
        assert_eq!((found.recorded_ms, found.observed_ms), (4004, 0));
    }

    /// The floor, from below: a recorded value under it is not evidence even
    /// when a span matches it exactly.
    #[test]
    fn a_value_below_the_floor_is_not_evidence() {
        let ms = MIN_EVIDENTIAL_MS - 1;
        let record = vec![node(1, None, "work", C, 0, Some(ms * MS))];
        let replay = vec![node(1, None, "work", C, 0, Some(0))];
        assert_eq!(
            elapsed_derived(ms, 0, C, &record, &replay),
            ElapsedVerdict::Refused
        );
    }

    /// The floor, from above: the first value it admits behaves like any other.
    #[test]
    fn a_value_at_the_floor_is_evidence() {
        let ms = MIN_EVIDENTIAL_MS;
        let record = vec![node(1, None, "work", C, 0, Some(ms * MS))];
        let replay = vec![node(1, None, "work", C, 0, Some(0))];
        assert!(matches!(
            elapsed_derived(ms, 0, C, &record, &replay),
            ElapsedVerdict::Explained(_)
        ));
    }

    /// SIBLINGS reading the same duration are the near-miss case, and the one
    /// the ancestry rule has to get right: two spans under one parent are not a
    /// call and its wrapper, so the recorded side genuinely does not say which
    /// span the leaf came from. Ancestry is what separates "one call measured at
    /// several depths" from "two candidates and no way to choose".
    #[test]
    fn matching_siblings_are_ambiguous_and_refused() {
        let record = vec![
            node(1, None, "root", C, 0, Some(9000 * MS)),
            node(2, Some(1), "work", C, 0, Some(890 * MS)),
            node(3, Some(1), "other", C, 2000 * MS, Some(2890 * MS)),
        ];
        // BOTH candidates exist on the replay side and both read 0, so without
        // the ambiguity refusal this would find a consistent match and absorb.
        // Without them the test would pass for the wrong reason — failing on
        // replay consistency rather than on ambiguity.
        let replay = vec![
            node(1, None, "root", C, 0, Some(9 * MS)),
            node(2, Some(1), "work", C, 0, Some(0)),
            node(3, Some(1), "other", C, 0, Some(0)),
        ];
        assert_eq!(
            elapsed_derived(890, 0, C, &record, &replay),
            ElapsedVerdict::Refused
        );
    }

    /// The measured motivating case, which is a THREE-way collision: the reading
    /// matched the call (4004.263 ms), its parent (4004.404 ms) and its
    /// grandparent (4004.921 ms), because a dominating child hands every ancestor
    /// its own wall time. That is one call measured at three depths, not three
    /// candidates — so ancestry admits it and the deepest is named.
    ///
    /// A uniqueness rule WITHOUT ancestry would refuse exactly the case this
    /// module exists for, which is why ancestry is the discriminator and the
    /// floor is not.
    #[test]
    fn one_call_measured_at_three_depths_stays_unique_and_names_the_deepest() {
        let record = vec![
            node(1, None, "root", C, 0, Some(9000 * MS)),
            node(2, Some(1), "grandparent", C, 0, Some(4004 * MS + 921_000)),
            node(3, Some(2), "parent", C, 0, Some(4004 * MS + 404_000)),
            node(4, Some(3), "call", C, 0, Some(4004 * MS + 263_000)),
        ];
        let replay = vec![
            node(1, None, "root", C, 0, Some(9 * MS)),
            node(2, Some(1), "grandparent", C, 0, Some(921_000)),
            node(3, Some(2), "parent", C, 0, Some(404_000)),
            node(4, Some(3), "call", C, 0, Some(263_000)),
        ];
        let ElapsedVerdict::Explained(found) = elapsed_derived(4004, 0, C, &record, &replay) else {
            panic!("expected an explained reading");
        };
        assert_eq!(found.span_path, "root>grandparent>parent>call");
    }

    /// The replay side has to AGREE. A span that read something else on replay
    /// means the leaf is not tracking it, whatever the recorded side showed.
    #[test]
    fn a_replay_side_that_disagrees_refuses_the_match() {
        let record = vec![node(1, None, "work", C, 0, Some(4004 * MS))];
        let replay = vec![node(1, None, "work", C, 0, Some(77 * MS))];
        assert_eq!(
            elapsed_derived(4004, 0, C, &record, &replay),
            ElapsedVerdict::Refused
        );
    }

    /// The recorded side carries the evidence. A leaf whose recorded value
    /// matches no span is not laundered into an absorption because its observed
    /// value happens to match a near-zero one.
    #[test]
    fn an_observed_side_match_alone_proves_nothing() {
        let record = vec![node(1, None, "work", C, 0, Some(1234 * MS))];
        let replay = vec![node(1, None, "work", C, 0, Some(240_000))];
        // 4004 is not any recorded span's duration; 0 is the replay span's.
        assert_eq!(
            elapsed_derived(4004, 0, C, &record, &replay),
            ElapsedVerdict::Refused
        );
    }

    /// A span that never closed has no duration, and inventing one would invent
    /// the evidence.
    #[test]
    fn an_unclosed_span_supplies_no_duration() {
        let record = vec![node(1, None, "work", C, 0, None)];
        let replay = vec![node(1, None, "work", C, 0, Some(0))];
        assert_eq!(
            elapsed_derived(4004, 0, C, &record, &replay),
            ElapsedVerdict::Refused
        );
    }

    /// A replay side that supplies NOTHING is not a replay side that disagrees.
    ///
    /// This is the dead-join case: replay graph nodes exist but carry a different
    /// correlation (or none), so the filter matches nothing. Folded into a
    /// refusal it would be invisible — the rule would absorb nothing, report
    /// nothing, and every other fixture here would still pass, because they build
    /// nodes directly instead of loading an artifact. A green suite over a dead
    /// feature is the failure this variant exists to make impossible.
    #[test]
    fn a_replay_side_with_no_spans_here_says_so_rather_than_refusing() {
        let record = vec![node(1, None, "work", C, 0, Some(4004 * MS))];
        // Present, plausible, and not this correlation.
        let replay = vec![node(1, None, "work", "another-corr", 0, Some(240_000))];
        assert_eq!(
            elapsed_derived(4004, 0, C, &record, &replay),
            ElapsedVerdict::NoReplayEvidence {
                span_path: "work".to_owned()
            }
        );
    }

    /// And the pair level propagates it rather than collapsing it, so a caller
    /// judging a whole request still learns the join was empty.
    #[test]
    fn the_pair_level_propagates_an_empty_replay_side() {
        let record = vec![node(1, None, "work", C, 0, Some(4004 * MS))];
        let replay = vec![node(1, None, "work", "another-corr", 0, Some(240_000))];
        let recorded = serde_json::json!({"a": 4004});
        let observed = serde_json::json!({"a": 0});
        assert_eq!(
            pair_differs_only_by_elapsed(&recorded, &observed, C, &record, &replay),
            PairElapsed::NoReplayEvidence {
                span_path: "work".to_owned()
            }
        );
    }

    /// deja's byte-capture envelope, built from its content so the renderings
    /// are genuinely derived — the point of the fixture is that they DIFFER.
    fn envelope(content: &serde_json::Value) -> serde_json::Value {
        let text = serde_json::to_string(content).unwrap();
        serde_json::json!({
            "captured": true,
            "bytes_len": text.len(),
            "utf8": true,
            "text": text,
            "json": content,
            "raw_bytes": text.as_bytes().to_vec(),
        })
    }

    fn two_span_run() -> (Vec<ExecutionGraphNode>, Vec<ExecutionGraphNode>) {
        let record = vec![
            node(1, None, "root", C, 0, Some(9000 * MS)),
            node(2, Some(1), "outer", C, 0, Some(4004 * MS + 260_000)),
            node(3, Some(1), "work", C, 5000 * MS, Some(5890 * MS + 610_000)),
        ];
        let replay = vec![
            node(1, None, "root", C, 0, Some(9 * MS)),
            node(2, Some(1), "outer", C, 0, Some(240_000)),
            node(3, Some(1), "work", C, 2 * MS, Some(2 * MS + 770_000)),
        ];
        (record, replay)
    }

    /// The measured shape, end to end. Two readings inside a captured body, and
    /// the body's OWN renderings differ as a consequence — `bytes_len`, `text`
    /// and `raw_bytes` all move when a digit does. If the renderings were
    /// compared this would block; judging the content is what lets the two
    /// explained leaves be the whole of the difference.
    #[test]
    fn a_captured_body_differing_only_in_readings_is_explained() {
        let (record, replay) = two_span_run();
        let recorded = envelope(&serde_json::json!({"stage": "s", "a": 4004, "b": 890}));
        let observed = envelope(&serde_json::json!({"stage": "s", "a": 0, "b": 0}));
        assert_ne!(
            recorded["bytes_len"], observed["bytes_len"],
            "the fixture is worthless unless the renderings really differ"
        );
        let PairElapsed::Explained(found) =
            pair_differs_only_by_elapsed(&recorded, &observed, C, &record, &replay)
        else {
            panic!("expected an explained pair");
        };
        assert_eq!(found.len(), 2);
        let mut paths: Vec<&str> = found.iter().map(|m| m.span_path.as_str()).collect();
        paths.sort_unstable();
        assert_eq!(paths, ["root>outer", "root>work"]);
    }

    /// One genuinely changed leaf beside an explained one still blocks. A pair
    /// is absorbed only when EVERY difference in it has an explanation; forgiving
    /// the pair because part of it is explained is how a real change rides out on
    /// a timing leaf.
    #[test]
    fn one_real_change_beside_a_reading_still_blocks() {
        let (record, replay) = two_span_run();
        let recorded = envelope(&serde_json::json!({"amount": 100, "a": 4004}));
        let observed = envelope(&serde_json::json!({"amount": 250, "a": 0}));
        assert_eq!(
            pair_differs_only_by_elapsed(&recorded, &observed, C, &record, &replay),
            PairElapsed::Refused
        );
    }

    /// A field present on one side only is structural: no clock explains a value
    /// appearing or vanishing, so there is no leaf pair to test and it blocks.
    #[test]
    fn a_field_on_one_side_only_blocks() {
        let (record, replay) = two_span_run();
        let recorded = envelope(&serde_json::json!({"a": 4004}));
        let observed = envelope(&serde_json::json!({"a": 0, "extra": true}));
        assert_eq!(
            pair_differs_only_by_elapsed(&recorded, &observed, C, &record, &replay),
            PairElapsed::Refused
        );
    }

    /// A leaf that is not an integer cannot be a millisecond reading.
    #[test]
    fn a_non_integer_leaf_blocks() {
        let (record, replay) = two_span_run();
        let recorded = envelope(&serde_json::json!({"ref": "abc"}));
        let observed = envelope(&serde_json::json!({"ref": "xyz"}));
        assert_eq!(
            pair_differs_only_by_elapsed(&recorded, &observed, C, &record, &replay),
            PairElapsed::Refused
        );
    }

    /// Another correlation's spans are not evidence for this one.
    #[test]
    fn a_match_in_a_different_correlation_does_not_count() {
        let record = vec![node(1, None, "work", "other-corr", 0, Some(4004 * MS))];
        let replay = vec![node(1, None, "work", "other-corr", 0, Some(0))];
        assert_eq!(
            elapsed_derived(4004, 0, C, &record, &replay),
            ElapsedVerdict::Refused
        );
    }
}
