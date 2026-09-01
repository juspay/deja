//! THE SPAN-SHAPE CHECK: replay must re-execute the instrumentation the tape
//! declared, not just the calls it recorded.
//!
//! The graph tier (deja-forest) deliberately prunes every span subtree that
//! carries no boundary event, because replay-side graphs drown in load-dependent
//! transport spans (`poll`, `send_data`, …) whose count is nondeterminism, not
//! behavior. That prune has a blind spot: a span a candidate placed ON PURPOSE
//! (`#[instrument]` on a pipeline chokepoint) is invisible to scoring unless a
//! side effect happens to fire beneath it — a candidate that silently stops
//! executing `connector::request_body` still passes.
//!
//! This module closes the blind spot without touching the forest's invariants.
//! A run may declare `scored_span_namespaces` (span-name prefixes, e.g.
//! `["ucs::", "connector::"]`). Spans under those prefixes are the candidate's
//! instrumentation CONTRACT: each side's graph is projected down to just those
//! spans — every other node contracts away, so transport noise cannot exist in
//! the projection by construction — and the two projections must agree:
//!
//!   - a recorded span with no replay partner is `Missing` (blocking),
//!   - a replayed span with no recorded partner is `Novel` (blocking: the tape
//!     is the contract in both directions — an old tape recorded before the
//!     candidate was instrumented FAILS against an instrumented candidate, by
//!     design; re-record rather than special-case),
//!   - partners whose captured field values differ (`connector`, `flow`, …)
//!     are `FieldDiverged` (blocking),
//!   - otherwise `Matched`.
//!
//! Pairing mirrors the forest's sibling rule: group by the span's namespace
//! PATH (the `>`-joined chain of namespaced ancestors — contraction, again, so
//! a refactor that inserts a plain helper span between two scored spans does
//! not move them), order each group by creation `sequence`, pair k-th with k-th.
//!
//! Declaring no namespaces disables all of this and the scorecard stays
//! byte-identical — systems that never opted in (hyperswitch) cannot regress.

use std::collections::BTreeMap;

use deja_core::ExecutionGraphNode;
use serde::{Deserialize, Serialize};

/// Field keys excluded from the value comparison: the correlation id rides on
/// every instrumented root span and is equal by construction (the kernel drives
/// the recorded correlation), and the capture-panic marker describes the
/// recorder, not the candidate.
const VOLATILE_FIELD_KEYS: &[&str] = &["request_id", "deja.field_capture_panicked"];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SpanShapeStatus {
    Matched,
    Missing,
    Novel,
    FieldDiverged,
}

/// One field key whose recorded and replayed values disagree. `None` on a side
/// means the key was absent there.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldDiff {
    pub key: String,
    pub recorded: Option<serde_json::Value>,
    pub replayed: Option<serde_json::Value>,
}

/// One scored span occurrence, addressed by its namespace path. `k` is the
/// occurrence index within the path group (k-th recorded pairs with k-th
/// replayed), so two same-named siblings stay distinguishable in a report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpanShapeOutcome {
    pub path: String,
    pub span_name: String,
    pub k: usize,
    pub status: SpanShapeStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub record_node_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replay_node_id: Option<u64>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub field_diffs: Vec<FieldDiff>,
}

/// The per-correlation section serialized onto the scorecard. Counts are
/// projections of `outcomes` — kept because a reader deciding pass/fail should
/// not have to re-derive them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationSpanShape {
    pub matched: u64,
    pub missing: u64,
    pub novel: u64,
    pub field_diverged: u64,
    pub outcomes: Vec<SpanShapeOutcome>,
}

impl CorrelationSpanShape {
    pub fn clean(&self) -> bool {
        self.missing == 0 && self.novel == 0 && self.field_diverged == 0
    }
}

fn in_namespace(span_name: &str, namespaces: &[String]) -> bool {
    namespaces
        .iter()
        .any(|ns| span_name.starts_with(ns.as_str()))
}

/// A side's projection: namespace path -> occurrences in creation order.
///
/// The path walk uses the FULL node map (all of the correlation's nodes, scored
/// or not) so a scored span anchored under plain spans still resolves its
/// scored ancestors; only namespaced names appear in the path itself.
fn project<'a>(
    nodes: &[&'a ExecutionGraphNode],
    namespaces: &[String],
) -> BTreeMap<String, Vec<&'a ExecutionGraphNode>> {
    let by_id: BTreeMap<u64, &ExecutionGraphNode> = nodes.iter().map(|n| (n.node_id, *n)).collect();
    let mut groups: BTreeMap<String, Vec<&ExecutionGraphNode>> = BTreeMap::new();
    for node in nodes {
        if !in_namespace(&node.span_name, namespaces) {
            continue;
        }
        let mut chain = vec![node.span_name.clone()];
        let mut cursor = node.parent_id;
        // Cycle guard: a hop count bounded by the node population. The graph
        // layer cannot emit a parent cycle, but this projection must not hang
        // on a hand-built or corrupted artifact either.
        let mut hops = 0usize;
        while let Some(parent_id) = cursor {
            if hops > by_id.len() {
                break;
            }
            hops += 1;
            match by_id.get(&parent_id) {
                Some(parent) => {
                    if in_namespace(&parent.span_name, namespaces) {
                        chain.push(parent.span_name.clone());
                    }
                    cursor = parent.parent_id;
                }
                None => break,
            }
        }
        chain.reverse();
        groups.entry(chain.join(">")).or_default().push(node);
    }
    for occurrences in groups.values_mut() {
        occurrences.sort_by_key(|n| (n.sequence, n.node_id));
    }
    groups
}

fn field_diffs(record: &ExecutionGraphNode, replay: &ExecutionGraphNode) -> Vec<FieldDiff> {
    let mut keys: Vec<&String> = record.fields.keys().chain(replay.fields.keys()).collect();
    keys.sort();
    keys.dedup();
    keys.into_iter()
        .filter(|k| !VOLATILE_FIELD_KEYS.contains(&k.as_str()))
        .filter(|k| record.fields.get(*k) != replay.fields.get(*k))
        .map(|k| FieldDiff {
            key: k.clone(),
            recorded: record.fields.get(k).cloned(),
            replayed: replay.fields.get(k).cloned(),
        })
        .collect()
}

/// Compare one correlation's scored-span projections. `None` when neither side
/// carries a namespaced span — the section then stays off the scorecard, which
/// is what keeps un-instrumented correlations (and whole systems) byte-stable.
pub fn compare(
    record_nodes: &[&ExecutionGraphNode],
    replay_nodes: &[&ExecutionGraphNode],
    namespaces: &[String],
) -> Option<CorrelationSpanShape> {
    if namespaces.is_empty() {
        return None;
    }
    let record = project(record_nodes, namespaces);
    let replay = project(replay_nodes, namespaces);
    if record.is_empty() && replay.is_empty() {
        return None;
    }

    let mut outcomes = Vec::new();
    let mut paths: Vec<&String> = record.keys().chain(replay.keys()).collect();
    paths.sort();
    paths.dedup();
    for path in paths {
        let empty = Vec::new();
        let rec = record.get(path).unwrap_or(&empty);
        let rep = replay.get(path).unwrap_or(&empty);
        let span_name = rec
            .first()
            .or_else(|| rep.first())
            .map(|n| n.span_name.clone())
            .unwrap_or_default();
        for k in 0..rec.len().max(rep.len()) {
            let outcome = match (rec.get(k), rep.get(k)) {
                (Some(r), Some(p)) => {
                    let diffs = field_diffs(r, p);
                    SpanShapeOutcome {
                        path: path.clone(),
                        span_name: span_name.clone(),
                        k,
                        status: if diffs.is_empty() {
                            SpanShapeStatus::Matched
                        } else {
                            SpanShapeStatus::FieldDiverged
                        },
                        record_node_id: Some(r.node_id),
                        replay_node_id: Some(p.node_id),
                        field_diffs: diffs,
                    }
                }
                (Some(r), None) => SpanShapeOutcome {
                    path: path.clone(),
                    span_name: span_name.clone(),
                    k,
                    status: SpanShapeStatus::Missing,
                    record_node_id: Some(r.node_id),
                    replay_node_id: None,
                    field_diffs: Vec::new(),
                },
                (None, Some(p)) => SpanShapeOutcome {
                    path: path.clone(),
                    span_name: span_name.clone(),
                    k,
                    status: SpanShapeStatus::Novel,
                    record_node_id: None,
                    replay_node_id: Some(p.node_id),
                    field_diffs: Vec::new(),
                },
                (None, None) => unreachable!("k bounded by max of the two group lengths"),
            };
            outcomes.push(outcome);
        }
    }

    let count = |s: SpanShapeStatus| outcomes.iter().filter(|o| o.status == s).count() as u64;
    Some(CorrelationSpanShape {
        matched: count(SpanShapeStatus::Matched),
        missing: count(SpanShapeStatus::Missing),
        novel: count(SpanShapeStatus::Novel),
        field_diverged: count(SpanShapeStatus::FieldDiverged),
        outcomes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns() -> Vec<String> {
        vec!["ucs::".to_owned(), "connector::".to_owned()]
    }

    fn node(
        node_id: u64,
        parent_id: Option<u64>,
        span_name: &str,
        fields: &[(&str, &str)],
    ) -> ExecutionGraphNode {
        ExecutionGraphNode {
            node_id,
            global_sequence: node_id,
            parent_id,
            causal_parent_ids: Vec::new(),
            sequence: node_id,
            correlation_id: Some("corr-1".to_owned()),
            recording_run_id: None,
            span_name: span_name.to_owned(),
            target: "test".to_owned(),
            level: "INFO".to_owned(),
            fields: fields
                .iter()
                .map(|(k, v)| ((*k).to_owned(), serde_json::Value::String((*v).to_owned())))
                .collect(),
            started_ns: node_id,
            closed_ns: None,
        }
    }

    /// The lattice prism actually records: scored spans anchored under plain
    /// spans (`payment_authorize`, `execute_connector_processing_step`). The
    /// path must contract the plain spans away, so a helper span inserted
    /// between two scored spans in a refactor does not move the contract.
    fn instrumented_side() -> Vec<ExecutionGraphNode> {
        vec![
            node(0, None, "deja::grpc_incoming", &[]),
            node(1, Some(0), "payment_authorize", &[]),
            node(2, Some(1), "ucs::metadata_extract", &[]),
            node(
                3,
                Some(1),
                "ucs::flow_orchestration",
                &[("connector", "stripe"), ("flow", "Authorize")],
            ),
            node(4, Some(3), "execute_connector_processing_step", &[]),
            node(5, Some(4), "ucs::build_request", &[]),
            node(
                6,
                Some(5),
                "connector::request_body",
                &[("connector", "Stripe"), ("flow", "Authorize")],
            ),
        ]
    }

    fn refs(nodes: &[ExecutionGraphNode]) -> Vec<&ExecutionGraphNode> {
        nodes.iter().collect()
    }

    #[test]
    fn identical_projections_match_and_paths_contract_plain_ancestors() {
        let rec = instrumented_side();
        let rep = instrumented_side();
        let shape = compare(&refs(&rec), &refs(&rep), &ns()).expect("scored spans present");
        assert!(shape.clean(), "identical sides must be clean: {shape:?}");
        assert_eq!(shape.matched, 4);
        let paths: Vec<&str> = shape.outcomes.iter().map(|o| o.path.as_str()).collect();
        assert!(
            paths.contains(&"ucs::flow_orchestration>ucs::build_request>connector::request_body"),
            "plain spans (payment_authorize, execute_connector_processing_step) must not \
             appear in the namespace path: {paths:?}"
        );
    }

    #[test]
    fn a_recorded_span_the_replay_never_executed_is_missing() {
        let rec = instrumented_side();
        let mut rep = instrumented_side();
        rep.retain(|n| n.span_name != "connector::request_body");
        let shape = compare(&refs(&rec), &refs(&rep), &ns()).expect("scored spans present");
        assert_eq!(shape.missing, 1);
        let miss = shape
            .outcomes
            .iter()
            .find(|o| o.status == SpanShapeStatus::Missing)
            .expect("missing outcome");
        assert_eq!(miss.span_name, "connector::request_body");
        assert!(miss.replay_node_id.is_none());
    }

    #[test]
    fn a_replayed_span_the_tape_never_recorded_is_novel() {
        // The old-tape case: a recording made before the candidate was
        // instrumented has no scored spans at all. The contract reads in both
        // directions, so this FAILS (novel) rather than silently passing —
        // re-record the tape with the instrumented build.
        let rec = vec![
            node(0, None, "deja::grpc_incoming", &[]),
            node(1, Some(0), "payment_authorize", &[]),
        ];
        let rep = instrumented_side();
        let shape = compare(&refs(&rec), &refs(&rep), &ns()).expect("replay side has scored spans");
        assert_eq!(shape.novel, 4);
        assert_eq!(shape.matched, 0);
        assert!(!shape.clean());
    }

    #[test]
    fn a_changed_chokepoint_field_value_is_field_diverged_with_the_key_named() {
        let rec = instrumented_side();
        let mut rep = instrumented_side();
        for n in &mut rep {
            if n.span_name == "ucs::flow_orchestration" {
                n.fields.insert(
                    "connector".to_owned(),
                    serde_json::Value::String("adyen".to_owned()),
                );
            }
        }
        let shape = compare(&refs(&rec), &refs(&rep), &ns()).expect("scored spans present");
        assert_eq!(shape.field_diverged, 1);
        let div = shape
            .outcomes
            .iter()
            .find(|o| o.status == SpanShapeStatus::FieldDiverged)
            .expect("diverged outcome");
        assert_eq!(div.field_diffs.len(), 1);
        assert_eq!(div.field_diffs[0].key, "connector");
        assert_eq!(
            div.field_diffs[0].recorded,
            Some(serde_json::Value::String("stripe".to_owned()))
        );
        assert_eq!(
            div.field_diffs[0].replayed,
            Some(serde_json::Value::String("adyen".to_owned()))
        );
    }

    #[test]
    fn volatile_field_keys_do_not_diverge() {
        let rec = instrumented_side();
        let mut rep = instrumented_side();
        for n in &mut rep {
            n.fields.insert(
                "request_id".to_owned(),
                serde_json::Value::String("different-every-run".to_owned()),
            );
        }
        let shape = compare(&refs(&rec), &refs(&rep), &ns()).expect("scored spans present");
        assert!(
            shape.clean(),
            "request_id must not count as a field divergence"
        );
    }

    #[test]
    fn same_named_siblings_pair_kth_with_kth_by_creation_order() {
        // Two retry attempts under the same parent: first with first, second
        // with second — the forest's sibling rule, applied to the projection.
        let side = |second_connector: &str| {
            vec![
                node(0, None, "ucs::flow_orchestration", &[]),
                node(1, Some(0), "connector::request_body", &[("attempt", "a")]),
                node(
                    2,
                    Some(0),
                    "connector::request_body",
                    &[("attempt", second_connector)],
                ),
            ]
        };
        let rec = side("b");
        let rep = side("CHANGED");
        let shape = compare(&refs(&rec), &refs(&rep), &ns()).expect("scored spans present");
        assert_eq!(shape.matched, 2, "first occurrences and the parent pair up");
        assert_eq!(
            shape.field_diverged, 1,
            "only the SECOND occurrence diverges"
        );
        let div = shape
            .outcomes
            .iter()
            .find(|o| o.status == SpanShapeStatus::FieldDiverged)
            .expect("diverged outcome");
        assert_eq!(div.k, 1);
    }

    #[test]
    fn no_scored_spans_on_either_side_yields_no_section() {
        let rec = vec![node(0, None, "payment_authorize", &[])];
        let rep = vec![node(0, None, "payment_authorize", &[])];
        assert!(compare(&refs(&rec), &refs(&rep), &ns()).is_none());
        assert!(compare(&refs(&rec), &refs(&rep), &[]).is_none());
    }
}
