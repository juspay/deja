#![allow(clippy::unwrap_used)] // tests panic on failure by design

//! Property 5: a correlation draws its hash keys EXACTLY ONCE, and the tape
//! carries that one event with the real key pair.
//!
//! This is the assertion that guards the memo. If it breaks, N collections
//! produce N draws, the correlation's maps stop agreeing on an iteration order,
//! and the divergence this seam exists to close comes back — silently, because
//! every individual request still succeeds. So the count is ASSERTED here rather
//! than left to be noticed in a tape someone happens to read.
//!
//! Own test binary: `set_global_runtime_hook` is a one-shot `OnceLock`, so a
//! process gets one hook and this one needs a recorder.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use deja_runtime::{
    read_events, DejaCorrelationLayer, DejaRandomState, RecordingHook, RuntimeHook,
};
use tracing_subscriber::prelude::*;

const CORRELATION: &str = "req-hash-seed-one-event";

/// Run `f` inside a recording request: the ingress span carrying the
/// correlation, under the correlation layer, with the sampler's decision for it
/// registered BEFORE the span is created — the layer resolves the decision once,
/// at span creation, and carries it for the span's lifetime.
///
/// A real span, not a bare `deja_context::enter`: the correlation's hash-key cell
/// lives on the span, and a correlation with no span is deliberately not seeded.
fn in_a_recording_request<T>(f: impl FnOnce() -> T) -> T {
    deja_context::set_recording_decision(CORRELATION, deja_context::RecordDecision::Record);
    let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
    tracing::subscriber::with_default(subscriber, || {
        let request = tracing::info_span!("deja::http_incoming", request_id = %CORRELATION);
        let _entered = request.enter();
        f()
    })
}

/// The process's one recording hook and the directory it writes to. Two tests
/// share a binary here, and `set_global_runtime_hook` is a one-shot.
fn recording_hook() -> &'static (tempfile::TempDir, Arc<RecordingHook>) {
    static HOOK: std::sync::OnceLock<(tempfile::TempDir, Arc<RecordingHook>)> =
        std::sync::OnceLock::new();
    HOOK.get_or_init(|| {
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = Arc::new(RecordingHook::new(dir.path()).expect("recording hook"));
        deja_runtime::set_global_runtime_hook(Some(RuntimeHook::Recording(Arc::clone(&hook))))
            .expect("install recording hook");
        (dir, hook)
    })
}

#[test]
fn a_correlation_records_exactly_one_hash_key_event() {
    let (dir, hook) = recording_hook();

    let recorded_keys = in_a_recording_request(|| {
        // Several collections, of both kinds, built at different moments — the
        // shape a real request has. All of them must share ONE draw.
        let mut first: HashMap<u32, u32, DejaRandomState> = HashMap::default();
        first.insert(1, 1);
        let mut second: HashMap<String, u32, DejaRandomState> = HashMap::default();
        second.insert("k".to_owned(), 2);
        let third: HashSet<u32, DejaRandomState> = HashSet::default();
        let fourth: HashMap<u32, u32, DejaRandomState> =
            HashMap::with_capacity_and_hasher(16, DejaRandomState::default());
        // FromIterator funnels through `S::default()` too.
        let fifth: HashMap<u32, u32, DejaRandomState> = (0..4).map(|i| (i, i)).collect();

        assert_eq!(third.len(), 0);
        assert_eq!(fourth.len(), 0);
        assert_eq!(fifth.len(), 4);

        // Whatever the seam drew, every collection above is using it.
        DejaRandomState::default()
    });

    hook.flush().expect("flush the recorder");

    let events = read_events(dir.path()).expect("read the tape");
    let seed_events: Vec<_> = events
        .iter()
        .filter(|event| {
            event.boundary == "hash_seed" && event.correlation_id.as_deref() == Some(CORRELATION)
        })
        .collect();

    assert_eq!(
        seed_events.len(),
        1,
        "five collections in one correlation must produce ONE hash-key event, not \
         one per collection — more than one means the memo is not holding and the \
         collections no longer share an iteration order. Got: {seed_events:#?}"
    );

    let event = seed_events[0];
    assert_eq!(
        event.correlation_id.as_deref(),
        Some(CORRELATION),
        "the draw must be attributed to the correlation that made it, or replay \
         cannot find it"
    );
    assert!(
        !event.is_error,
        "a hash-key draw cannot fail; an error image here means the seam recorded \
         something it should not have"
    );

    // The event is addressed by the request span's path like every other
    // boundary in the request: `draw_and_record` stamps `current_span_path()`.
    assert_eq!(
        event
            .callsite_identity
            .as_ref()
            .and_then(|identity| identity.span_path.as_deref()),
        Some("deja::http_incoming"),
        "the hash-key event must carry the request span's path, or it sits \
         outside the span-path address the rest of the request is keyed by"
    );

    // The tape must hold the REAL pair. A masked or absent image would replay as
    // an unreconstructable hit, which fail-stops every request on the tape.
    let k0 = event.result.get("k0").and_then(serde_json::Value::as_u64);
    let k1 = event.result.get("k1").and_then(serde_json::Value::as_u64);
    let DejaRandomState::Seeded(in_process) = recorded_keys else {
        panic!("inside a correlation the seeded arm must be taken");
    };
    assert_eq!(
        (k0, k1),
        (Some(in_process.k0), Some(in_process.k1)),
        "the recorded image must carry the exposed key pair the process actually \
         used — masking it here would put `***` on the tape and substitute nothing \
         on replay"
    );
}

/// The other half of "one regime, sampled or not": a request the ingress
/// sampled OUT is seeded — the unit test asserts that — and leaves NOTHING on
/// the tape, no `hash_seed` event and no event of any kind under its
/// correlation. The seeded arm is asserted first so the absence below cannot
/// pass because the request was simply never seeded.
#[test]
fn a_sampled_out_request_is_seeded_and_records_nothing() {
    const SAMPLED_OUT: &str = "req-hash-seed-sampled-out";
    let (dir, hook) = recording_hook();

    deja_context::set_recording_decision(SAMPLED_OUT, deja_context::RecordDecision::Skip);
    let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
    let state = tracing::subscriber::with_default(subscriber, || {
        let request = tracing::info_span!("deja::http_incoming", request_id = %SAMPLED_OUT);
        let _entered = request.enter();
        let mut map: HashMap<u32, u32, DejaRandomState> = HashMap::default();
        map.insert(1, 1);
        DejaRandomState::default()
    });
    deja_context::clear_recording_decision(SAMPLED_OUT);

    assert!(
        matches!(state, DejaRandomState::Seeded(_)),
        "precondition: a sampled-out request must take the seeded arm, or the \
         absence asserted below is vacuous"
    );

    hook.flush().expect("flush the recorder");
    let events = read_events(dir.path()).expect("read the tape");
    let under_it: Vec<_> = events
        .iter()
        .filter(|event| event.correlation_id.as_deref() == Some(SAMPLED_OUT))
        .collect();
    assert!(
        under_it.is_empty(),
        "a sampled-out request must leave nothing on the tape — its draw is a \
         bare draw, not a boundary crossing. Got: {under_it:#?}"
    );
}
