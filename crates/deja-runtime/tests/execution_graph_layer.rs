use std::sync::{Arc, Mutex};

use deja_core::ExecutionGraphNode;
use deja_runtime::{
    read_events, read_execution_graph_records, EventBuilder, ExecutionGraphLayer, GraphNodeSink,
};
use tracing::{span, Level, Subscriber};
use tracing_subscriber::prelude::*;

/// Deterministic in-memory sink: layer behavior is asserted without any
/// async writer or file round-trip in the loop.
#[derive(Default)]
struct CollectingSink {
    nodes: Mutex<Vec<ExecutionGraphNode>>,
}

impl CollectingSink {
    fn drain(&self) -> Vec<ExecutionGraphNode> {
        self.nodes
            .lock()
            .map(|mut buf| std::mem::take(&mut *buf))
            .unwrap_or_default()
    }
}

impl GraphNodeSink for CollectingSink {
    fn graph_node(&self, node: ExecutionGraphNode) {
        if let Ok(mut buf) = self.nodes.lock() {
            buf.push(node);
        }
    }
}

fn subscriber(sink: Arc<CollectingSink>) -> impl Subscriber + Send + Sync {
    tracing_subscriber::registry().with(ExecutionGraphLayer::new(sink))
}

fn collect_graph<T>(f: impl FnOnce() -> T) -> Vec<ExecutionGraphNode> {
    let sink = Arc::new(CollectingSink::default());
    tracing::subscriber::with_default(subscriber(Arc::clone(&sink)), f);
    sink.drain()
}

#[test]
fn records_span_creation_fields() {
    let nodes = collect_graph(|| {
        let span = span!(
            Level::INFO,
            "payment.request",
            request_id = "req_123",
            payment_id = "pay_123",
            attempt = 2_u64,
            cached = false
        );
        drop(span);
    });
    assert_eq!(nodes.len(), 1);

    let node = &nodes[0];
    assert_eq!(node.sequence, 0);
    assert_eq!(node.span_name, "payment.request");
    assert_eq!(node.level, "INFO");
    assert_eq!(node.fields["request_id"], "req_123");
    assert_eq!(node.fields["payment_id"], "pay_123");
    assert_eq!(node.fields["attempt"], 2);
    assert_eq!(node.fields["cached"], false);
    // Stream identity belongs to the sink, not the layer.
    assert_eq!(node.global_sequence, 0);
    assert_eq!(node.recording_run_id, None);
}

#[test]
fn merges_field_updates_from_span_record() {
    let nodes = collect_graph(|| {
        let span = span!(
            Level::INFO,
            "field.update",
            request_id = tracing::field::Empty,
            status = "started",
            http.status_code = tracing::field::Empty
        );
        span.record("request_id", "req_updated");
        span.record("status", "finished");
        span.record("http.status_code", 200_u64);
        drop(span);
    });

    let fields = &nodes[0].fields;
    assert_eq!(fields["request_id"], "req_updated");
    assert_eq!(fields["status"], "finished");
    assert_eq!(fields["http.status_code"], 200);
}

#[test]
fn records_parent_child_relationship() {
    let nodes = collect_graph(|| {
        let parent = span!(Level::INFO, "parent");
        let _guard = parent.enter();
        let child = span!(Level::DEBUG, "child");
        drop(child);
        drop(_guard);
        drop(parent);
    });
    assert_eq!(nodes.len(), 2);

    let child = nodes
        .iter()
        .find(|node| node.span_name == "child")
        .expect("child");
    let parent = nodes
        .iter()
        .find(|node| node.span_name == "parent")
        .expect("parent");

    assert_eq!(child.parent_id, Some(parent.node_id));
    assert_eq!(parent.parent_id, None);
}

#[test]
fn records_causal_parent_relationship() {
    let nodes = collect_graph(|| {
        let cause = span!(Level::INFO, "cause");
        let effect = span!(Level::INFO, "effect");
        effect.follows_from(&cause);
        drop(effect);
        drop(cause);
    });

    let cause = nodes
        .iter()
        .find(|node| node.span_name == "cause")
        .expect("cause");
    let effect = nodes
        .iter()
        .find(|node| node.span_name == "effect")
        .expect("effect");

    assert_eq!(effect.causal_parent_ids, vec![cause.node_id]);
}

#[test]
fn records_closed_timestamp_after_start() {
    let nodes = collect_graph(|| {
        let span = span!(Level::WARN, "closed");
        drop(span);
    });

    let node = &nodes[0];
    let closed_ns = node.closed_ns.expect("closed timestamp");
    assert!(closed_ns >= node.started_ns);
}

/// Full tape integration: graph nodes ride the SAME semantic-events stream as
/// boundary events, stamped with the hook's run id and a graph-space sequence,
/// and the boundary event joins onto the node id.
#[test]
fn graph_nodes_ride_the_recording_tape() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = Arc::new(deja_runtime::RecordingHook::new(dir.path()).expect("recording hook"));
    let layer_sink: Arc<dyn GraphNodeSink> = Arc::clone(&hook) as _;
    let subscriber = tracing_subscriber::registry().with(ExecutionGraphLayer::new(layer_sink));
    tracing::subscriber::with_default(subscriber, || {
        let span = span!(Level::INFO, "semantic.parent", request_id = "req_join");
        let _guard = span.enter();
        let event = EventBuilder::start(
            hook.as_ref(),
            "db",
            "PaymentIntentInterface",
            "insert_payment_intent",
            std::panic::Location::caller(),
            serde_json::json!({"payment_id": "pay_join"}),
        );
        event.finish(hook.as_ref(), serde_json::json!({"ok": true}), false);
        drop(_guard);
        drop(span);
    });
    hook.flush().expect("flush tape");

    let nodes = read_execution_graph_records(dir.path()).expect("read graph nodes");
    let events = read_events(dir.path()).expect("read semantic events");
    assert_eq!(nodes.len(), 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].graph_node_id, Some(nodes[0].node_id));
    assert!(events[0].tracing_span_id.is_some());
    assert_eq!(
        nodes[0].recording_run_id.as_deref(),
        Some(hook.recording_run_id())
    );
    // Graph nodes use their own sequence space; boundary numbering is
    // graph-invariant.
    assert_eq!(nodes[0].global_sequence, 0);
    assert_eq!(events[0].global_sequence, 0);
}
