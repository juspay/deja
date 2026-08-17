use std::collections::BTreeMap;

use deja_forest::{
    build, ActivationForest, Annex, BuildError, EventRef, ExecutionGraphNode, ForestSet,
};

fn node(
    node_id: u64,
    sequence: u64,
    parent_id: Option<u64>,
    correlation_id: Option<&str>,
    span_name: &str,
) -> ExecutionGraphNode {
    ExecutionGraphNode {
        node_id,
        global_sequence: sequence,
        parent_id,
        causal_parent_ids: Vec::new(),
        sequence,
        correlation_id: correlation_id.map(str::to_owned),
        recording_run_id: None,
        span_name: span_name.to_owned(),
        target: "test".to_owned(),
        level: "INFO".to_owned(),
        fields: BTreeMap::new(),
        started_ns: sequence,
        closed_ns: None,
    }
}

fn event(global_sequence: u64, graph_node_id: Option<u64>) -> EventRef {
    EventRef {
        global_sequence,
        graph_node_id,
        correlation_id_present: graph_node_id.is_some(),
        counterpart_possible: true,
    }
}

/// An event whose boundary the harness drives, so the other side can never
/// produce a counterpart. It names a node — that is the point of the case.
fn event_without_counterpart(global_sequence: u64, graph_node_id: Option<u64>) -> EventRef {
    EventRef {
        counterpart_possible: false,
        ..event(global_sequence, graph_node_id)
    }
}

#[test]
fn straight_parent_chain_builds_one_forest_with_one_root() {
    let nodes = vec![
        node(30, 3, Some(20), Some("request-a"), "leaf"),
        node(10, 1, None, Some("request-a"), "root"),
        node(20, 2, Some(10), Some("request-a"), "middle"),
    ];
    let events = vec![
        event(300, Some(30)),
        event(100, Some(10)),
        event(200, Some(20)),
    ];

    let built = build(&nodes, &events).expect("a valid chain should build");

    assert_eq!(
        built.by_correlation.keys().collect::<Vec<_>>(),
        vec!["request-a"]
    );
    assert!(built.ambient.nodes.is_empty());
    assert_eq!(built.annex, Annex::default());
    assert!(built.unusable.is_empty());

    let forest = &built.by_correlation["request-a"];
    assert_eq!(forest.roots, vec![10]);
    assert_eq!(
        forest.nodes.keys().copied().collect::<Vec<_>>(),
        vec![10, 20, 30]
    );
    assert_eq!(forest.nodes[&10].parent_id, None);
    assert!(!forest.nodes[&10].promoted);
    assert_eq!(forest.nodes[&10].children, vec![20]);
    assert_eq!(forest.nodes[&20].children, vec![30]);
    assert_eq!(forest.nodes[&30].children, Vec::<u64>::new());
    assert_eq!(forest.nodes[&10].events, vec![100]);
    assert_eq!(forest.nodes[&20].events, vec![200]);
    assert_eq!(forest.nodes[&30].events, vec![300]);
}

#[test]
fn node_with_absent_parent_is_promoted_to_root() {
    let nodes = vec![node(7, 10, Some(999), Some("request-a"), "scoped")];

    let built = build(&nodes, &[event(11, Some(7))]).expect("a scoped subtree should build");
    let forest = &built.by_correlation["request-a"];

    assert_eq!(forest.roots, vec![7]);
    assert_eq!(forest.nodes[&7].parent_id, None);
    assert!(forest.nodes[&7].promoted);
    assert_eq!(forest.nodes[&7].events, vec![11]);
}

#[test]
fn parent_cycle_terminates_and_marks_correlation_unusable() {
    let nodes = vec![
        node(1, 1, Some(2), Some("cyclic"), "first"),
        node(2, 2, Some(1), Some("cyclic"), "second"),
    ];

    let built = build(&nodes, &[event(10, Some(1)), event(20, Some(2))])
        .expect("cycle handling must remain total");
    let forest = &built.by_correlation["cyclic"];

    assert!(built.unusable.contains_key("cyclic"));
    assert_eq!(forest.roots, vec![1]);
    assert_eq!(forest.nodes[&1].parent_id, None);
    assert_eq!(forest.nodes[&1].children, vec![2]);
    assert_eq!(forest.nodes[&2].parent_id, Some(1));
    assert_eq!(forest.nodes[&2].children, Vec::<u64>::new());
    assert_eq!(forest.nodes[&1].events, vec![10]);
    assert_eq!(forest.nodes[&2].events, vec![20]);
}

#[test]
fn no_node_and_absent_node_events_are_split_between_annex_vectors() {
    let nodes = vec![node(1, 1, None, Some("request-a"), "root")];
    let events = vec![
        event(40, None),
        event(30, Some(404)),
        event(10, None),
        event(20, Some(405)),
        event(50, Some(1)),
    ];

    let built = build(&nodes, &events).expect("annexed events are accounted events");

    assert_eq!(built.annex.names_no_node, vec![10, 40]);
    assert_eq!(built.annex.names_absent_node, vec![20, 30]);
    assert_eq!(built.by_correlation["request-a"].nodes[&1].events, vec![50]);
}

#[test]
fn branching_subtree_rollups_include_descendants_when_own_events_are_empty() {
    let nodes = vec![
        node(1, 1, None, Some("request-a"), "root"),
        node(2, 2, Some(1), Some("request-a"), "left"),
        node(3, 3, Some(1), Some("request-a"), "right"),
        node(4, 4, Some(3), Some("request-a"), "right-leaf"),
    ];
    let events = vec![
        event(200, Some(2)),
        event(400, Some(4)),
        event(401, Some(4)),
    ];

    let built = build(&nodes, &events).expect("a branching tree should build");
    let forest = &built.by_correlation["request-a"];

    assert_eq!(forest.nodes[&1].children, vec![2, 3]);
    assert!(forest.nodes[&1].events.is_empty());
    assert!(forest.nodes[&3].events.is_empty());
    assert_eq!(forest.nodes[&1].subtree_events, 3);
    assert_eq!(forest.nodes[&2].subtree_events, 1);
    assert_eq!(forest.nodes[&3].subtree_events, 2);
    assert_eq!(forest.nodes[&4].subtree_events, 2);
}

#[test]
fn balance_rejects_an_input_event_that_is_neither_attached_nor_annexed() {
    let malformed = ForestSet {
        by_correlation: BTreeMap::new(),
        ambient: ActivationForest::default(),
        annex: Annex::default(),
        unusable: BTreeMap::new(),
    };

    assert_eq!(
        malformed.balance(1),
        Err(BuildError::Imbalanced {
            events_in: 1,
            attached: 0,
            annexed: 0,
        })
    );
}

#[test]
fn ambient_nodes_form_a_separate_forest_from_named_correlation() {
    let nodes = vec![
        node(1, 1, None, None, "ambient-root"),
        node(2, 2, Some(1), Some("request-a"), "request-child"),
        node(3, 3, Some(1), None, "ambient-child"),
    ];
    let events = vec![event(10, Some(1)), event(20, Some(2)), event(30, Some(3))];

    let built = build(&nodes, &events).expect("ambient and named forests should coexist");
    let named = &built.by_correlation["request-a"];

    assert_eq!(
        built.by_correlation.keys().collect::<Vec<_>>(),
        vec!["request-a"]
    );
    assert_eq!(built.ambient.correlation_id, None);
    assert_eq!(built.ambient.roots, vec![1]);
    assert_eq!(
        built.ambient.nodes.keys().copied().collect::<Vec<_>>(),
        vec![1, 3]
    );
    assert_eq!(built.ambient.nodes[&1].children, vec![3]);
    assert_eq!(built.ambient.nodes[&1].events, vec![10]);
    assert_eq!(built.ambient.nodes[&3].events, vec![30]);

    assert_eq!(named.correlation_id.as_deref(), Some("request-a"));
    assert_eq!(named.roots, vec![2]);
    assert_eq!(named.nodes.keys().copied().collect::<Vec<_>>(), vec![2]);
    assert_eq!(named.nodes[&2].parent_id, None);
    assert!(named.nodes[&2].promoted);
    assert_eq!(named.nodes[&2].events, vec![20]);
}

// ---------------------------------------------------------------------------
// Events with no possible counterpart on the other side
// ---------------------------------------------------------------------------

/// The ingress shape, both sides, as a recording actually carries it: a
/// `deja::http_incoming` root beside the request tree, holding one event on the
/// record side that replay structurally cannot have.
fn ingress_shaped_nodes() -> Vec<ExecutionGraphNode> {
    vec![
        node(1, 1, None, Some("request-a"), "deja::http_incoming"),
        node(2, 2, None, Some("request-a"), "HTTP request"),
        node(3, 3, Some(2), Some("request-a"), "payments_operation_core"),
    ]
}

#[test]
fn an_event_with_no_possible_counterpart_is_annexed_under_its_own_cause_not_attached() {
    let events = vec![event_without_counterpart(100, Some(1)), event(200, Some(3))];

    let built = build(&ingress_shaped_nodes(), &events).expect("forest builds");
    let forest = &built.by_correlation["request-a"];

    assert_eq!(
        built.annex.no_counterpart_by_construction,
        vec![100],
        "the ingress event belongs in its own bucket"
    );
    assert!(
        built.annex.names_no_node.is_empty(),
        "it named a node, so calling it a capture gap would send a reader \
         hunting for an instrumentation bug that does not exist"
    );
    assert!(built.annex.names_absent_node.is_empty());

    assert!(
        forest.nodes[&1].events.is_empty(),
        "annexed means not attached"
    );
    assert_eq!(
        forest.nodes[&1].subtree_events, 0,
        "the ingress root must roll up to zero, or it stays event-bearing on \
         the record side alone and the tier demotes on the asymmetry"
    );
    assert_eq!(forest.nodes[&2].subtree_events, 1);
}

#[test]
fn balance_counts_the_no_counterpart_bucket_and_rejects_a_sequence_duplicated_into_it() {
    let events = vec![event_without_counterpart(100, Some(1)), event(200, Some(3))];
    let built = build(&ingress_shaped_nodes(), &events).expect("forest builds");

    // The bucket is counted, so the set balances rather than reading as a drop.
    assert_eq!(built.balance(2), Ok(()));

    // And it is in the uniqueness chain. A sequence that is both attached and
    // annexed must fail, or a double-counted event balances by arithmetic while
    // being scored twice.
    let mut duplicated = built.clone();
    duplicated.annex.no_counterpart_by_construction.push(200);
    assert_eq!(
        duplicated.balance(3),
        Err(BuildError::Imbalanced {
            events_in: 3,
            attached: 1,
            annexed: 2,
        }),
        "a sequence in two places at once must be refused, not silently tolerated"
    );
}

#[test]
fn a_zero_event_ingress_root_is_absent_from_the_alignment_on_both_sides() {
    // Record carries the ingress event; replay cannot. This is exactly the
    // asymmetry that demoted all 78 correlations on run
    // rp-sbx-1ed5b454b7-1ed5b45-08171332-8j-0817194832193.
    let record = build(
        &ingress_shaped_nodes(),
        &[event_without_counterpart(100, Some(1)), event(200, Some(3))],
    )
    .expect("record forest builds");
    let replay =
        build(&ingress_shaped_nodes(), &[event(200, Some(3))]).expect("replay forest builds");

    let record_forest = &record.by_correlation["request-a"];
    let replay_forest = &replay.by_correlation["request-a"];

    // Both ingress roots roll up to zero, so neither survives skeleton pruning.
    assert_eq!(record_forest.nodes[&1].subtree_events, 0);
    assert_eq!(replay_forest.nodes[&1].subtree_events, 0);

    let alignment = deja_forest::align(record_forest, replay_forest);

    assert!(
        !alignment
            .nodes
            .iter()
            .any(|row| row.span_name == "deja::http_incoming"),
        "a zero-event ingress root must not reach the alignment at all — not as \
         a match, and above all not as a PrunedSubtree, which is the finding \
         that relaxing the gate alone would have produced: {:#?}",
        alignment.nodes
    );
    assert!(
        alignment
            .nodes
            .iter()
            .any(|row| row.span_name == "HTTP request"
                && matches!(row.outcome, deja_forest::NodeOutcome::Matched)),
        "the request tree still pairs 1:1"
    );
}
