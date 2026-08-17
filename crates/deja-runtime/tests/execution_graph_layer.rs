use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use deja_core::ExecutionGraphNode;
use deja_runtime::{
    read_events, read_execution_graph_records, EventBuilder, ExecutionGraphLayer, GraphNodeSink,
};
use tracing::{span, Level, Subscriber};
use tracing_subscriber::prelude::*;

/// The graph layer gates every method on `observation_is_active()`, which reads
/// the process runtime hook and this correlation's recording decision, so a bare
/// layer install observes nothing. These helpers establish the context a running
/// process has: a process Record hook (installed once, since the hook is a process
/// `OnceLock`) plus a per-test correlation carrying a Record or Skip decision.
fn install_process_record_hook() {
    static HOOK_DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
    HOOK_DIR.get_or_init(|| {
        let dir = tempfile::tempdir().expect("hook tempdir");
        let hook = deja_runtime::RecordingHook::new(dir.path()).expect("recording hook");
        deja_runtime::set_global_runtime_hook(Some(deja_runtime::RuntimeHook::Recording(
            Arc::new(hook),
        )))
        .expect("install the process Record hook before any observation call");
        dir
    });
}

static NEXT_CORRELATION: AtomicU64 = AtomicU64::new(0);

/// Run `f` under a fresh correlation carrying `decision`, after ensuring the
/// process Record hook is installed. The decision lives on a thread-local
/// snapshot (no global registry write), so parallel tests never collide. With
/// `Record`, `observation_is_active()` is true and the layer observes; with
/// `Skip` it is false and the layer is inert.
fn with_decision<T>(decision: bool, f: impl FnOnce() -> T) -> T {
    install_process_record_hook();
    let correlation_id = format!(
        "graph-layer-test-{}",
        NEXT_CORRELATION.fetch_add(1, Ordering::Relaxed)
    );
    let snapshot =
        deja_context::ContextSnapshot::new(correlation_id).with_recording_decision(decision);
    let _guard = deja_context::enter(snapshot);
    f()
}

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
    with_decision(true, || {
        tracing::subscriber::with_default(subscriber(Arc::clone(&sink)), f);
    });
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
    // Record decision in scope so the gated layer observes; the layer still emits
    // to `hook` (its sink), independent of which process hook `with_decision`
    // installs.
    with_decision(true, || {
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

/// A sampled-out (Skip) request allocates no graph nodes: `on_new_span`
/// early-returns on `!observation_is_active()` before any node id, `graph_node_map`
/// lock, or span extension, so the sink stays empty even though spans open and
/// close.
#[test]
fn skip_decision_records_no_graph_nodes() {
    let sink = Arc::new(CollectingSink::default());
    with_decision(false, || {
        tracing::subscriber::with_default(subscriber(Arc::clone(&sink)), || {
            let parent = span!(Level::INFO, "payment.request", request_id = "req_skip");
            let _guard = parent.enter();
            let child = span!(Level::DEBUG, "child");
            drop(child);
            drop(_guard);
            drop(parent);
        });
    });
    assert!(
        sink.drain().is_empty(),
        "a Skip request must produce zero graph nodes (layer inert)"
    );
}

// ---------------------------------------------------------------------------
// A node states its own correlation.
//
// These assert WHERE the correlation comes from, which is the whole point: two
// nearer-looking sources are wrong in ways that pass a shallower test. Reading
// `DejaCorrelationLayer`'s span extension yields `None` (that layer runs after
// this one), and reading the ambient `deja_context` correlation attributes a
// request's ROOT span to whatever ran before it, because the ambient scope is
// engaged on enter and not on new-span. `with_decision` sets an ambient
// correlation that is deliberately NOT the one these spans name, so a node that
// picked up the ambient value instead of the span field fails loudly here.
// ---------------------------------------------------------------------------

fn node_named<'a>(nodes: &'a [ExecutionGraphNode], name: &str) -> &'a ExecutionGraphNode {
    nodes
        .iter()
        .find(|n| n.span_name == name)
        .unwrap_or_else(|| panic!("no node named {name} in {nodes:#?}"))
}

#[test]
fn a_span_carrying_a_request_id_states_that_correlation() {
    let nodes = collect_graph(|| {
        let span = span!(Level::INFO, "ingress", request_id = "corr-a");
        drop(span);
    });
    assert_eq!(
        node_named(&nodes, "ingress").correlation_id.as_deref(),
        Some("corr-a"),
        "the root span establishes the correlation and must state it — it is the \
         node a scoped graph needs most, and the one an ambient read gets wrong",
    );
}

#[test]
fn a_child_inherits_the_correlation_it_never_names() {
    let nodes = collect_graph(|| {
        let root = span!(Level::INFO, "ingress", request_id = "corr-b");
        let _root = root.enter();
        let child = span!(Level::INFO, "payments_core");
        let _child = child.enter();
        let leaf = span!(Level::INFO, "update_trackers");
        drop(leaf);
    });
    for name in ["payments_core", "update_trackers"] {
        assert_eq!(
            node_named(&nodes, name).correlation_id.as_deref(),
            Some("corr-b"),
            "{name} runs under the request and must say so",
        );
    }
}

#[test]
fn a_span_outside_any_request_states_no_correlation() {
    let nodes = collect_graph(|| {
        let span = span!(Level::INFO, "scheduler_tick");
        drop(span);
    });
    assert_eq!(
        node_named(&nodes, "scheduler_tick").correlation_id,
        None,
        "ambient work belongs to no case; None is the honest answer, and a reader \
         must not confuse it with a tape that predates the field",
    );
}

#[test]
fn a_spans_own_request_id_wins_over_the_one_it_would_inherit() {
    let nodes = collect_graph(|| {
        let outer = span!(Level::INFO, "outer", request_id = "corr-outer");
        let _outer = outer.enter();
        let inner = span!(Level::INFO, "inner", request_id = "corr-inner");
        drop(inner);
    });
    assert_eq!(
        node_named(&nodes, "inner").correlation_id.as_deref(),
        Some("corr-inner"),
    );
    assert_eq!(
        node_named(&nodes, "outer").correlation_id.as_deref(),
        Some("corr-outer"),
    );
}

#[test]
fn a_later_request_does_not_inherit_the_earlier_one() {
    let nodes = collect_graph(|| {
        {
            let first = span!(Level::INFO, "first", request_id = "corr-1");
            let _first = first.enter();
        }
        let second = span!(Level::INFO, "second", request_id = "corr-2");
        drop(second);
    });
    assert_eq!(
        node_named(&nodes, "second").correlation_id.as_deref(),
        Some("corr-2"),
        "a closed request must not bleed into the next one",
    );
}

// ---------------------------------------------------------------------------
// The real ingress order.
//
// Every test above establishes a correlation-carrying context BEFORE creating a
// span, so the thread already has an ambient correlation when `on_new_span`
// runs. A router does the opposite, and the difference is not cosmetic:
//
//   1. the sampler decides and `set_recording_decision` writes the
//      correlation-keyed REGISTRY — no correlation is live on the thread
//   2. the ingress root span is CREATED (`on_new_span` fires here)
//   3. the future is polled and the span is ENTERED — only now does
//      `DejaCorrelationLayer` make the correlation ambient
//
// So at step 2 the ambient gate answers `false` on every recorded request. A
// layer that consults only the ambient gate allocates no state for the root,
// its children find no parent state, and the record side loses its forest root
// while the replay side keeps one (`observation_active` is unconditionally true
// for `RuntimeMode::Replay`). Two forests differing at the root cannot align.
//
// These drive the registry, not a thread-local snapshot, because that is what
// the ingress writes and it is the only store that can answer for a request the
// thread is not yet serving.
// ---------------------------------------------------------------------------

/// Collect nodes for `f` with no ambient correlation, under both layer
/// registration orders, asserting the two agree.
///
/// The order matters to the mechanism: `.with(a).with(b)` runs `a`'s
/// `on_new_span` first (`Layered` delegates inner-first), and the host installs
/// `.with(deja_layer()).with(deja_correlation_layer())`, so the graph layer
/// runs BEFORE the correlation layer and cannot read its span extension. That
/// is why the fix reads the registry rather than that extension — and why this
/// helper pins the property in both directions, so a host that reorders its
/// `.with` calls cannot silently change what gets recorded.
fn collect_graph_at_ingress_order<T>(f: impl Fn() -> T) -> Vec<ExecutionGraphNode> {
    install_process_record_hook();
    assert_eq!(
        deja_context::current_correlation_id(),
        None,
        "these tests must run with no ambient correlation — that is the condition \
         under test",
    );

    let graph_first = Arc::new(CollectingSink::default());
    tracing::subscriber::with_default(
        tracing_subscriber::registry()
            .with(ExecutionGraphLayer::new(
                Arc::clone(&graph_first) as Arc<dyn GraphNodeSink>
            ))
            .with(deja_runtime::DejaCorrelationLayer::new()),
        || {
            f();
        },
    );

    let correlation_first = Arc::new(CollectingSink::default());
    tracing::subscriber::with_default(
        tracing_subscriber::registry()
            .with(deja_runtime::DejaCorrelationLayer::new())
            .with(ExecutionGraphLayer::new(
                Arc::clone(&correlation_first) as Arc<dyn GraphNodeSink>
            )),
        || {
            f();
        },
    );

    let a = graph_first.drain();
    let b = correlation_first.drain();
    let shape = |nodes: &[ExecutionGraphNode]| -> Vec<(String, Option<String>)> {
        nodes
            .iter()
            .map(|n| (n.span_name.clone(), n.correlation_id.clone()))
            .collect()
    };
    assert_eq!(
        shape(&a),
        shape(&b),
        "graph allocation must not depend on which layer was registered first; \
         `Layered` runs the inner layer's `on_new_span` first, so a rule that \
         reads the other layer's span extension would pass one order and fail \
         the other",
    );
    a
}

/// The ingress root span, created before the correlation it carries is engaged.
fn drive_one_ingress_request(correlation_id: &str) {
    let root = span!(Level::INFO, "deja::http_incoming", request_id = %correlation_id);
    let _entered = root.enter();
    // `TracingLogger`'s root span sits inside the deja span and names the same
    // correlation; application spans below inherit it.
    let actix_root = span!(Level::INFO, "HTTP request", request_id = %correlation_id);
    let _actix = actix_root.enter();
    let work = span!(Level::INFO, "payments.core");
    drop(work);
}

#[test]
fn ingress_root_span_gets_a_node_though_its_correlation_is_not_yet_ambient() {
    let correlation_id = "ingress-root-allocates";
    deja_context::set_recording_decision(correlation_id, true);
    let nodes = collect_graph_at_ingress_order(|| drive_one_ingress_request(correlation_id));
    deja_context::clear_recording_decision(correlation_id);

    let root = node_named(&nodes, "deja::http_incoming");
    assert_eq!(
        root.correlation_id.as_deref(),
        Some(correlation_id),
        "the ingress root establishes the correlation and must state it",
    );
    assert_eq!(
        root.parent_id, None,
        "the ingress root is the forest root — nothing is above it",
    );

    // The point of allocating the root is that everything below can reach it.
    // Without it these two hang off nothing and the record-side forest has a
    // different root than the replay-side one built from the same spans.
    let http = node_named(&nodes, "HTTP request");
    assert_eq!(
        http.parent_id,
        Some(root.node_id),
        "the next span in must hang off the ingress root, not off nothing",
    );
    assert_eq!(
        node_named(&nodes, "payments.core").parent_id,
        Some(http.node_id),
        "application spans keep their place in the tree",
    );
}

#[test]
fn a_span_naming_a_sampled_out_correlation_allocates_nothing() {
    let correlation_id = "ingress-root-sampled-out";
    deja_context::set_recording_decision(correlation_id, false);
    let nodes = collect_graph_at_ingress_order(|| drive_one_ingress_request(correlation_id));
    deja_context::clear_recording_decision(correlation_id);

    assert!(
        nodes.is_empty(),
        "an explicit Skip must stay as inert as before — answering for a named \
         correlation changes WHICH decision is consulted, never whether one is \
         required: {nodes:#?}",
    );
}

#[test]
fn a_span_naming_an_undecided_correlation_allocates_nothing() {
    // No `set_recording_decision` at all: recording is opt-in, so a span that
    // names a correlation nobody decided on records nothing. This is the guard
    // against the named path becoming a way to capture what `capture_verdict`
    // refuses — note the polarity differs from `DejaCorrelationLayer`, which
    // engages a correlation unless an explicit Skip says otherwise because
    // engaging one records nothing.
    let nodes = collect_graph_at_ingress_order(|| drive_one_ingress_request("ingress-undecided"));
    assert!(
        nodes.is_empty(),
        "an undecided correlation must not capture — recording is opt-in: {nodes:#?}",
    );
}
