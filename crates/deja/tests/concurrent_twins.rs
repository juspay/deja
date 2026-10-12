//! Two concurrent branches that run the same instrumented fn and make the same
//! boundary call with the same args must each be served their OWN recorded
//! value on replay.
//!
//! This is the shape of payment create building its billing and shipping
//! addresses under `try_join!`: both branches open a `create_address` span and
//! call `generate_id` with identical args, so the span path and args are shared
//! and only the order the calls arrive in tells the twins apart. That order is a
//! scheduler race. Here the recording has billing call first and the candidate
//! has shipping call first; a branch served the other branch's id is the bug.
//!
//! The table and the candidate's resolution are rebuilt with the same public
//! primitives the renderer and the replay hook use, as `v2_regression` does.
//!
//! Own test binary: `set_global_runtime_hook` is a one-shot `OnceLock`.

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use deja::{canonical_args_hash, loci_for, BoundaryEvent, KeyStamper, LookupKey};
use tracing::Instrument;
use tracing_subscriber::prelude::*;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

/// Same args on every call, a fresh value from each: the value says which call
/// it came from.
#[deja::boundary(
    boundary = "id",
    component = "tests::concurrent_twins",
    operation = "generate_id",
    args = serde_json::json!({ "prefix": prefix, "length": 20 }),
    result = (serde_json::json!({ "output": __deja_result }), false),
)]
fn generate_id(prefix: &str) -> String {
    format!("{prefix}_{}", NEXT_ID.fetch_add(1, Ordering::SeqCst))
}

/// Pending once, waking itself, so the other branch gets polled in between.
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// One span name for both branches; `#[instrument]` on an async fn creates the
/// span at first poll, as hyperswitch's address builders do.
#[tracing::instrument(skip_all)]
async fn create_address(label: &'static str, yields: usize) -> (&'static str, String) {
    for _ in 0..yields {
        YieldOnce(false).await;
    }
    (label, generate_id("add"))
}

/// Polls both branches in declaration order on every pass until both are done,
/// as `join!`/`try_join!` do.
async fn join2<A: Future, B: Future>(a: A, b: B) -> (A::Output, B::Output) {
    let mut a = std::pin::pin!(a);
    let mut b = std::pin::pin!(b);
    let mut out_a = None;
    let mut out_b = None;
    std::future::poll_fn(move |cx| {
        if out_a.is_none() {
            if let Poll::Ready(v) = a.as_mut().poll(cx) {
                out_a = Some(v);
            }
        }
        if out_b.is_none() {
            if let Poll::Ready(v) = b.as_mut().poll(cx) {
                out_b = Some(v);
            }
        }
        match (out_a.is_some(), out_b.is_some()) {
            (true, true) => Poll::Ready((
                out_a.take().expect("checked"),
                out_b.take().expect("checked"),
            )),
            _ => Poll::Pending,
        }
    })
    .await
}

fn block_on<F: Future>(fut: F) -> F::Output {
    let mut cx = Context::from_waker(std::task::Waker::noop());
    let mut fut = std::pin::pin!(fut);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        std::hint::spin_loop();
    }
}

/// One request: a root span carrying the correlation, `get_trackers` under it,
/// and the two address branches joined inside that. Returns what each branch's
/// `generate_id` call produced.
fn run_request(
    correlation: &'static str,
    billing_yields: usize,
    shipping_yields: usize,
) -> HashMap<&'static str, String> {
    deja_context::set_recording_decision(correlation, true);
    let root = tracing::info_span!("deja::http_incoming", request_id = correlation);
    let (billing, shipping) = block_on(
        async {
            join2(
                create_address("billing", billing_yields),
                create_address("shipping", shipping_yields),
            )
            .instrument(tracing::info_span!("get_trackers"))
            .await
        }
        .instrument(root),
    );
    HashMap::from([billing, shipping])
}

fn by_corr<'a>(events: &'a [BoundaryEvent], corr: &str) -> Vec<&'a BoundaryEvent> {
    let mut stream: Vec<&BoundaryEvent> = events
        .iter()
        .filter(|event| event.correlation_id.as_deref() == Some(corr))
        .collect();
    stream.sort_by_key(|event| event.global_sequence);
    stream
}

/// The keys one call registers or looks up at, strongest rank first. The
/// stamper is shared across a stream, so occurrence follows arrival order.
fn keys_for(stamper: &mut KeyStamper, event: &BoundaryEvent, corr: &str) -> Vec<LookupKey> {
    let location = Some((event.call_file.as_str(), event.call_line, event.call_column));
    let addresses = loci_for(event.callsite_identity.as_ref(), location);
    stamper.stamp(
        Some(corr),
        event.bucket_id.as_deref(),
        event.fork_seq.unwrap_or(0),
        deja::CallIdentity {
            boundary: "test",
            component: "tests",
            operation: "op",
        },
        &addresses,
        canonical_args_hash(&event.args),
    )
}

fn build_table(events: &[&BoundaryEvent], corr: &str) -> HashMap<LookupKey, serde_json::Value> {
    let mut stamper = KeyStamper::new();
    let mut table = HashMap::new();
    for event in events {
        for key in keys_for(&mut stamper, event, corr) {
            table.insert(key, event.result.to_value());
        }
    }
    table
}

fn resolve_stream(
    events: &[&BoundaryEvent],
    corr: &str,
    table: &HashMap<LookupKey, serde_json::Value>,
) -> Vec<Option<(u8, &'static str, serde_json::Value)>> {
    let mut stamper = KeyStamper::new();
    events
        .iter()
        .map(|event| {
            keys_for(&mut stamper, event, corr).iter().find_map(|key| {
                table
                    .get(key)
                    .map(|result| (key.locus.rank(), key.locus.kind(), result.clone()))
            })
        })
        .collect()
}

fn output(value: &serde_json::Value) -> &str {
    value
        .get("output")
        .and_then(serde_json::Value::as_str)
        .expect("generate_id records its id under `output`")
}

#[test]
fn concurrent_twins_are_each_served_their_own_recorded_value() {
    let subscriber = tracing_subscriber::registry().with(deja::DejaCorrelationLayer::new());
    let _subscriber = tracing::subscriber::set_default(subscriber);
    let artifacts = tempfile::tempdir().expect("tempdir");
    deja_runtime::set_global_runtime_hook(Some(deja_runtime::RuntimeHook::Recording(
        std::sync::Arc::new(
            deja_runtime::RecordingHook::new(artifacts.path()).expect("recording hook"),
        ),
    )))
    .expect("install recording hook");

    // Recording: billing calls first. Candidate: shipping calls first.
    let recorded = run_request("c-rec", 0, 3);
    let candidate = run_request("c-cand", 3, 0);

    deja_runtime::flush_global_hook().expect("flush events");
    let events = deja_runtime::read_events(artifacts.path()).expect("read events");
    let rec = by_corr(&events, "c-rec");
    let cand = by_corr(&events, "c-cand");
    assert_eq!(rec.len(), 2, "one recorded event per branch");
    assert_eq!(cand.len(), 2, "one candidate event per branch");

    // The fixture only reproduces the race if the arrival order really flipped
    // while the two calls stayed indistinguishable by span path and args.
    let branch_of = |stream: &[&BoundaryEvent], values: &HashMap<&'static str, String>| {
        stream
            .iter()
            .map(|event| {
                let id = output(&event.result.to_value()).to_owned();
                *values
                    .iter()
                    .find(|(_, v)| **v == id)
                    .map(|(label, _)| label)
                    .expect("every event is one branch's call")
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(branch_of(&rec, &recorded), ["billing", "shipping"]);
    assert_eq!(branch_of(&cand, &candidate), ["shipping", "billing"]);
    for stream in [&rec, &cand] {
        let path = |e: &BoundaryEvent| {
            e.callsite_identity
                .as_ref()
                .and_then(|id| id.span_path.clone())
        };
        assert_eq!(
            path(stream[0]).as_deref(),
            Some("deja::http_incoming>get_trackers>create_address"),
        );
        assert_eq!(
            path(stream[0]),
            path(stream[1]),
            "the twins share a span path"
        );
        assert_eq!(stream[0].args, stream[1].args, "the twins share args");
    }

    // Both sides use one correlation id, as a replay of a baseline does.
    let table = build_table(&rec, "req");
    let resolved = resolve_stream(&cand, "req", &table);
    for (event, hit) in cand.iter().zip(&resolved) {
        let served_to = branch_of(&[event], &candidate)[0];
        let (rank, kind, value) = hit.as_ref().unwrap_or_else(|| {
            panic!("the candidate {served_to} call resolved at no rank");
        });
        let served = output(value);
        let owner = recorded
            .iter()
            .find(|(_, v)| v.as_str() == served)
            .map(|(label, _)| *label)
            .expect("served value is one of the recorded ids");
        assert_eq!(
            owner, served_to,
            "the candidate {served_to} call was served the recorded {owner} id ({served}); \
             recorded billing={}, shipping={}",
            recorded["billing"], recorded["shipping"],
        );
        assert_eq!(
            (*rank, *kind),
            (2, "span_instance"),
            "the candidate {served_to} call resolved by {kind}, not by its span instance"
        );
    }

    // A tape that predates the instance field keeps arrival-order addressing:
    // the twins may swap, as they always could, but no recorded row is served
    // to both. An instance key rendered for it would be numbered by arrival,
    // and a twin falling through to the plain path could take its sibling's row.
    let legacy: Vec<BoundaryEvent> = rec
        .iter()
        .map(|event| {
            let mut event = (*event).clone();
            if let Some(id) = event.callsite_identity.as_mut() {
                id.span_instance = None;
            }
            event
        })
        .collect();
    let legacy: Vec<&BoundaryEvent> = legacy.iter().collect();
    let table = build_table(&legacy, "req");
    let resolved = resolve_stream(&cand, "req", &table);
    let served: Vec<(&str, &str)> = resolved
        .iter()
        .map(|hit| {
            let (_, kind, value) = hit.as_ref().expect("a legacy twin still resolves");
            (*kind, output(value))
        })
        .collect();
    assert_ne!(
        served[0].1, served[1].1,
        "one recorded row served to both twins: {served:?}"
    );
    assert!(
        served.iter().all(|(kind, _)| *kind == "span_path"),
        "a legacy tape resolves by span path only: {served:?}"
    );
}
