use deja_core::{ExecutionGraphRecord, EXECUTION_GRAPH_FILE_NAME};
use deja_runtime::{read_events, read_execution_graph_records, EventBuilder, ExecutionGraphLayer};
use tracing::{span, Level, Subscriber};
use tracing_subscriber::prelude::*;

fn subscriber(dir: &std::path::Path) -> impl Subscriber + Send + Sync {
    tracing_subscriber::registry().with(ExecutionGraphLayer::new(dir).expect("graph layer"))
}

fn with_graph_subscriber<T>(dir: &std::path::Path, f: impl FnOnce() -> T) -> T {
    let subscriber = subscriber(dir);
    tracing::subscriber::with_default(subscriber, f)
}

#[test]
fn records_span_creation_fields_and_jsonl_readback() {
    let dir = tempfile::tempdir().expect("tempdir");
    with_graph_subscriber(dir.path(), || {
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

    let records = read_execution_graph_records(dir.path()).expect("read graph");
    assert_eq!(records.len(), 1);

    let node = &records[0].node;
    assert_eq!(node.sequence, 0);
    assert_eq!(node.span_name, "payment.request");
    assert_eq!(node.level, "INFO");
    assert_eq!(node.fields["request_id"], "req_123");
    assert_eq!(node.fields["payment_id"], "pay_123");
    assert_eq!(node.fields["attempt"], 2);
    assert_eq!(node.fields["cached"], false);

    let graph_path = dir.path().join(EXECUTION_GRAPH_FILE_NAME);
    let line = std::fs::read_to_string(graph_path).expect("jsonl");
    let parsed: ExecutionGraphRecord = serde_json::from_str(line.trim()).expect("record");
    assert_eq!(parsed.node.node_id, node.node_id);
}

#[test]
fn merges_field_updates_from_span_record() {
    let dir = tempfile::tempdir().expect("tempdir");
    with_graph_subscriber(dir.path(), || {
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

    let records = read_execution_graph_records(dir.path()).expect("read graph");
    let fields = &records[0].node.fields;
    assert_eq!(fields["request_id"], "req_updated");
    assert_eq!(fields["status"], "finished");
    assert_eq!(fields["http.status_code"], 200);
}

#[test]
fn records_parent_child_relationship() {
    let dir = tempfile::tempdir().expect("tempdir");
    with_graph_subscriber(dir.path(), || {
        let parent = span!(Level::INFO, "parent");
        let _guard = parent.enter();
        let child = span!(Level::DEBUG, "child");
        drop(child);
        drop(_guard);
        drop(parent);
    });

    let records = read_execution_graph_records(dir.path()).expect("read graph");
    assert_eq!(records.len(), 2);

    let child = records
        .iter()
        .find(|record| record.node.span_name == "child")
        .expect("child");
    let parent = records
        .iter()
        .find(|record| record.node.span_name == "parent")
        .expect("parent");

    assert_eq!(child.node.parent_id, Some(parent.node.node_id));
    assert_eq!(parent.node.parent_id, None);
}

#[test]
fn records_causal_parent_relationship() {
    let dir = tempfile::tempdir().expect("tempdir");
    with_graph_subscriber(dir.path(), || {
        let cause = span!(Level::INFO, "cause");
        let effect = span!(Level::INFO, "effect");
        effect.follows_from(&cause);
        drop(effect);
        drop(cause);
    });

    let records = read_execution_graph_records(dir.path()).expect("read graph");
    let cause = records
        .iter()
        .find(|record| record.node.span_name == "cause")
        .expect("cause");
    let effect = records
        .iter()
        .find(|record| record.node.span_name == "effect")
        .expect("effect");

    assert_eq!(effect.node.causal_parent_ids, vec![cause.node.node_id]);
}

#[test]
fn records_closed_timestamp_after_start() {
    let dir = tempfile::tempdir().expect("tempdir");
    with_graph_subscriber(dir.path(), || {
        let span = span!(Level::WARN, "closed");
        drop(span);
    });

    let records = read_execution_graph_records(dir.path()).expect("read graph");
    let node = &records[0].node;
    let closed_ns = node.closed_ns.expect("closed timestamp");
    assert!(closed_ns >= node.started_ns);
}

#[test]
fn semantic_event_records_active_graph_node_id() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = deja_runtime::RecordingHook::new(dir.path()).expect("recording hook");
    with_graph_subscriber(dir.path(), || {
        let span = span!(Level::INFO, "semantic.parent", request_id = "req_join");
        let _guard = span.enter();
        let event = EventBuilder::start(
            &hook,
            "db",
            "PaymentIntentInterface",
            "insert_payment_intent",
            std::panic::Location::caller(),
            serde_json::json!({"payment_id": "pay_join"}),
        );
        event.finish(&hook, serde_json::json!({"ok": true}), false);
        drop(_guard);
        drop(span);
    });

    let graph_records = read_execution_graph_records(dir.path()).expect("read graph");
    hook.flush().expect("flush semantic events");
    let events = read_events(dir.path()).expect("read semantic events");
    assert_eq!(graph_records.len(), 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].graph_node_id, Some(graph_records[0].node.node_id));
    assert!(events[0].tracing_span_id.is_some());
}
