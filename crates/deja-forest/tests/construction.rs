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
