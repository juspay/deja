//! Inactive-hook fast-path proof for the `#[deja::boundary]` family.
//!
//! This test deliberately NEVER sets `DEJA_MODE` / `DEJA_ARTIFACT_DIR`, so the
//! process-global recording / runtime hooks both resolve to `None` and Déjà is
//! inactive. Under the single `dispatch` seam the inactive path must run the real
//! block and return WITHOUT evaluating the `args` thunk — the zero-overhead
//! recording-disabled behavior the design preserves (recording-capture-decoupled
//! §3, §6 Step 4).
//!
//! Kept in its own test binary because the global hook is a process-wide
//! `OnceLock`: a sibling test that activates recording would initialize that
//! lock and make "inactive" unobservable in the same binary.

#![allow(unused_braces)]

use std::sync::atomic::{AtomicUsize, Ordering};

// Counts how many times the macro evaluated the `args` expression. When Déjà is
// inactive, the `dispatch` seam must NOT evaluate the args thunk, so this stays
// at zero even after the boundary runs.
static ARGS_EVALUATED: AtomicUsize = AtomicUsize::new(0);

fn args_probe(value: u64) -> serde_json::Value {
    ARGS_EVALUATED.fetch_add(1, Ordering::SeqCst);
    serde_json::json!({ "input": value })
}

#[deja::boundary(
    boundary = "inactive_probe",
    component = "BoundaryInactiveTest",
    operation = "sync_probe",
    args = args_probe(value),
    result = { (serde_json::json!({ "output": *__deja_result }), false) },
)]
fn sync_probe(value: u64) -> u64 {
    value + 1
}

#[test]
fn inactive_boundary_runs_block_without_serializing_args() {
    // No DEJA_* env set: Déjà is inactive for this whole binary.
    assert_eq!(
        sync_probe(41),
        42,
        "the real block runs and returns normally"
    );
    assert_eq!(
        ARGS_EVALUATED.load(Ordering::SeqCst),
        0,
        "inactive boundary must NOT evaluate the args thunk (zero-overhead path)"
    );
}

static OWNED_CAPTURES: AtomicUsize = AtomicUsize::new(0);

/// Readable only by consuming it.
struct Body(String);

struct BodyCodec;

impl deja::codec::OwnedReplayCodec for BodyCodec {
    type Value = Body;
    type Read = String;

    async fn read(value: Body) -> (Body, Result<String, String>) {
        OWNED_CAPTURES.fetch_add(1, Ordering::SeqCst);
        let text = value.0.clone();
        (value, Ok(text))
    }

    fn record(text: String, _: &Body) -> (serde_json::Value, bool) {
        (serde_json::json!({ "text": text }), false)
    }

    fn reconstruct(recorded: serde_json::Value) -> Option<Body> {
        Some(Body(recorded.get("text")?.as_str()?.to_owned()))
    }
}

#[deja::boundary(
    boundary = "inactive_probe",
    component = "BoundaryInactiveTest",
    operation = "owned_probe",
    owned_codec = BodyCodec,
    args = args_probe(value),
)]
async fn owned_probe(value: u64) -> Body {
    Body(format!("body {value}"))
}

#[test]
fn inactive_owned_codec_never_consumes_the_value() {
    use std::future::Future as _;
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let mut fut = std::pin::pin!(owned_probe(7));
    let std::task::Poll::Ready(body) = fut.as_mut().poll(&mut cx) else {
        panic!("the probe never awaits");
    };
    assert_eq!(body.0, "body 7");
    assert_eq!(
        OWNED_CAPTURES.load(Ordering::SeqCst),
        0,
        "an inactive boundary must not consume the value"
    );
    assert_eq!(
        ARGS_EVALUATED.load(Ordering::SeqCst),
        0,
        "an inactive owned boundary does no prep at all"
    );
}

/// Behind the macro's gate, the seam itself also leaves an unrecorded value
/// alone.
#[test]
fn the_owned_seam_does_not_capture_a_call_it_does_not_record() {
    use std::future::Future as _;
    let identity = deja::__private::CallsiteIdentity {
        version: 1,
        source: deja::__private::CallsiteSource::SyntacticHash,
        id: None,
        scope: None,
        occurrence: 0,
        caller_function: None,
        lexical_path: None,
        syntax_hash: None,
        span_path: None,
    };
    let obs = deja::__private::CrossingObservation::with_correlation(
        deja::__private::BoundarySpec::new("inactive_probe", "BoundaryInactiveTest", "seam"),
        identity,
        std::panic::Location::caller(),
        None,
    );
    let seam = deja::__private::owned_dispatch_async(
        obs,
        || serde_json::Value::Null,
        || async { Body("seam".to_owned()) },
        |_: deja::__private::ReconstructInput<'_>| deja::__private::Reconstructed::<Body>::NoValue,
        |value: Body| <BodyCodec as deja::codec::OwnedReplayCodec>::read(value),
        <BodyCodec as deja::codec::OwnedReplayCodec>::record,
        deja::__private::RoundTrip::<fn(&Body, &Body) -> deja::__private::Comparison>::RecordOnly,
        None::<fn(&Body) -> bool>,
    );
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let mut seam = std::pin::pin!(seam);
    let std::task::Poll::Ready(body) = seam.as_mut().poll(&mut cx) else {
        panic!("the seam never awaits here");
    };
    assert_eq!(body.0, "seam");
    assert_eq!(OWNED_CAPTURES.load(Ordering::SeqCst), 0);
}
