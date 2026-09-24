//! The recorder rebuilds every value it records and says whether it came back.
//!
//! Each site below is recorded once through `#[deja::boundary]`, and the test
//! reads the verdict the recorder stamped on the event (`recon` on the wire). A
//! site whose codec does not round-trip also panics: tests run as debug builds,
//! and there a broken codec must fail the build rather than a replay weeks
//! later.
//!
//! Own test binary: `set_global_runtime_hook` is a one-shot `OnceLock`.

use deja::codec::ReplayCodec;
use deja::Fidelity;

/// Records only the even part of a number, so an odd one comes back different.
struct HalvingCodec;

impl ReplayCodec for HalvingCodec {
    type Value = u64;
    fn capture(value: &u64) -> (serde_json::Value, bool) {
        (serde_json::json!(value / 2), false)
    }
    fn reconstruct(recorded: serde_json::Value) -> Option<u64> {
        recorded.as_u64().map(|half| half * 2)
    }
}

/// Captures a shape its own reconstruct cannot read.
struct UnreadableCodec;

impl ReplayCodec for UnreadableCodec {
    type Value = u64;
    fn capture(value: &u64) -> (serde_json::Value, bool) {
        (serde_json::json!({ "n": value }), false)
    }
    fn reconstruct(recorded: serde_json::Value) -> Option<u64> {
        recorded.as_u64()
    }
}

/// A codec whose reconstruct panics on what it captured.
struct PanickingCodec;

impl ReplayCodec for PanickingCodec {
    type Value = u64;
    fn capture(value: &u64) -> (serde_json::Value, bool) {
        (serde_json::json!(value), false)
    }
    fn reconstruct(_: serde_json::Value) -> Option<u64> {
        panic!("reconstruct cannot read its own capture")
    }
}

/// A type with nothing to compare by, and a codec that counts its rebuilds.
struct Opaque(u64);
static OPAQUE_REBUILDS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
struct CountingCodec;

impl ReplayCodec for CountingCodec {
    type Value = Opaque;
    fn capture(value: &Opaque) -> (serde_json::Value, bool) {
        (serde_json::json!(value.0), false)
    }
    fn reconstruct(recorded: serde_json::Value) -> Option<Opaque> {
        OPAQUE_REBUILDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        recorded.as_u64().map(Opaque)
    }
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "panicking", codec = PanickingCodec)]
fn panicking(n: u64) -> u64 {
    n
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "uncomparable", codec = CountingCodec)]
fn uncomparable(n: u64) -> Opaque {
    Opaque(n)
}

/// An error type with nothing to compare by, and a codec that rebuilds every
/// `Ok` it captured as an `Err`.
#[derive(Debug)]
struct NoCompare;
struct ErringCodec;

impl ReplayCodec for ErringCodec {
    type Value = Result<u64, NoCompare>;
    fn capture(value: &Self::Value) -> (serde_json::Value, bool) {
        match value {
            Ok(n) => (serde_json::json!(n), false),
            Err(_) => (serde_json::Value::Null, true),
        }
    }
    fn reconstruct(_: serde_json::Value) -> Option<Self::Value> {
        Some(Err(NoCompare))
    }
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "erring", codec = ErringCodec)]
fn erring(n: u64) -> Result<u64, NoCompare> {
    Ok(n)
}

/// A set behind a wrapper that serialises through its own `Serialize`, as a
/// collections facade does: the check cannot see it is a set.
#[derive(serde::Deserialize)]
struct Delegating(std::collections::HashSet<u32>);

impl serde::Serialize for Delegating {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter())
    }
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "delegating", codec = SerdeCodec)]
fn delegating() -> Delegating {
    Delegating((0..64).collect())
}

/// A capture larger than the check's limit, from a codec that counts rebuilds.
static LARGE_REBUILDS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
struct LargeCodec;

impl ReplayCodec for LargeCodec {
    type Value = String;
    fn capture(value: &String) -> (serde_json::Value, bool) {
        (serde_json::json!(value), false)
    }
    fn reconstruct(recorded: serde_json::Value) -> Option<String> {
        LARGE_REBUILDS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        recorded.as_str().map(str::to_owned)
    }
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "large", codec = LargeCodec)]
fn large(len: usize) -> String {
    "x".repeat(len)
}

/// A sequence behind a wrapper that serialises through its own `Serialize`,
/// rebuilt reversed: a difference in nothing but order, certain every run.
struct Reordered(Vec<u32>);

impl serde::Serialize for Reordered {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter())
    }
}

struct ReversingCodec;

impl ReplayCodec for ReversingCodec {
    type Value = Reordered;
    fn capture(value: &Reordered) -> (serde_json::Value, bool) {
        (serde_json::json!(value.0), false)
    }
    fn reconstruct(recorded: serde_json::Value) -> Option<Reordered> {
        let mut items: Vec<u32> = serde_json::from_value(recorded).ok()?;
        items.reverse();
        Some(Reordered(items))
    }
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "reordered", codec = ReversingCodec)]
fn reordered() -> Reordered {
    Reordered(vec![1, 2, 3])
}

/// The async record path, with a codec that loses information.
#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "halved_async", codec = HalvingCodec)]
async fn halved_async(n: u64) -> u64 {
    n
}

/// A single-poll executor: the record path never yields.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn raw() -> RawWaker {
        fn clone(_: *const ()) -> RawWaker {
            raw()
        }
        fn nop(_: *const ()) {}
        RawWaker::new(std::ptr::null(), &RawWakerVTable::new(clone, nop, nop, nop))
    }
    // SAFETY: every vtable function is a no-op on a null pointer.
    let waker = unsafe { Waker::from_raw(raw()) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
    }
}

/// A float whose JSON text does not read back as the same bits.
#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "float", codec = SerdeCodec)]
fn float(x: f64) -> f64 {
    x
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "cached", codec = SerdeCodec)]
fn cached(key: &str) -> Option<Option<String>> {
    (key == "known-absent").then_some(None)
}

/// Generic, as a cache read is: the only comparison `T` offers is `Serialize`.
#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "generic", codec = SerdeCodec)]
fn generic<T>(value: T) -> Option<T>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    Some(value)
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "halved", codec = HalvingCodec)]
fn halved(n: u64) -> u64 {
    n
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "unreadable", codec = UnreadableCodec)]
fn unreadable(n: u64) -> u64 {
    n
}

/// An error type with neither `PartialEq` nor `Serialize`, as an
/// `error_stack::Report` is: the site is compared by its `Ok` arm.
#[derive(Debug)]
struct StoreError;

#[deja::boundary(boundary = "db", component = "RoundTrip", operation = "fallible", codec = ResultOkCodec)]
fn fallible(key: &str) -> Result<Option<Option<String>>, StoreError> {
    Ok((key == "known-absent").then_some(None))
}

/// An `Err` under the Ok-only codec is recorded as a sentinel that never
/// rebuilds, by design; the check covers the value arm only.
#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "failing", codec = ResultOkCodec)]
fn failing(n: u64) -> Result<u64, String> {
    Err(format!("no {n}"))
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "record_only")]
fn record_only(n: u64) -> u64 {
    n
}

#[deja::boundary(boundary = "imc", component = "RoundTrip", operation = "executed", replay = Execute, codec = SerdeCodec)]
fn executed(n: u64) -> u64 {
    n
}

fn fidelity_of(events: &[deja::BoundaryEvent], operation: &str) -> Vec<Fidelity> {
    events
        .iter()
        .filter(|event| event.method_name == operation)
        .map(|event| event.fidelity)
        .collect()
}

#[test]
fn every_recorded_value_is_rebuilt_and_compared() {
    let artifacts = tempfile::tempdir().expect("tempdir");
    deja_runtime::set_global_runtime_hook(Some(deja_runtime::RuntimeHook::Recording(
        std::sync::Arc::new(
            deja_runtime::RecordingHook::new(artifacts.path()).expect("recording hook"),
        ),
    )))
    .expect("install recording hook");
    let recording = deja::test_support::recording_correlation("req-round-trip");

    assert_eq!(cached("known-absent"), Some(None));
    assert_eq!(cached("never-seen"), None);
    assert_eq!(generic(None::<String>), Some(None));
    assert_eq!(halved(4), 4);
    assert_eq!(record_only(1), 1);
    assert!(failing(1).is_err());
    assert!(matches!(fallible("known-absent"), Ok(Some(None))));
    assert_eq!(executed(1), 1);

    assert_eq!(uncomparable(7).0, 7);
    assert_eq!(float(0.5), 0.5);
    assert_eq!(delegating().0.len(), 64);
    assert_eq!(reordered().0, [1, 2, 3]);
    assert_eq!(large(8).len(), 8);
    // A capture of 70 KiB: past the 64 KiB limit, fixed rather than derived
    // from it, so a limit that stopped applying would show here.
    let past_limit = 70 * 1024;
    assert!(deja::__private::MAX_CHECKED_BYTES < past_limit);
    assert_eq!(large(past_limit).len(), past_limit);
    let async_lossy = std::panic::catch_unwind(|| block_on(halved_async(3)));
    let ok_as_err = std::panic::catch_unwind(|| erring(3));
    let lossy = std::panic::catch_unwind(|| halved(3));
    let reconstruct_panicked = std::panic::catch_unwind(|| panicking(3));
    let text_lossy = std::panic::catch_unwind(|| float(1.071_566_039_146_582_6e-75));
    let opaque = std::panic::catch_unwind(|| unreadable(3));

    drop(recording);
    deja::flush_global_runtime_hook().ok();
    let events = deja_runtime::read_events(artifacts.path()).expect("events");

    assert_eq!(
        fidelity_of(&events, "cached"),
        [Fidelity::Lossless, Fidelity::Lossless],
        "a cache hit holding None and a miss both come back as themselves"
    );
    assert_eq!(
        fidelity_of(&events, "generic"),
        [Fidelity::Lossless],
        "a generic site is compared by its serde image"
    );
    assert_eq!(
        fidelity_of(&events, "halved"),
        [Fidelity::Lossless, Fidelity::Lossy],
        "an even number survives halving, an odd one does not"
    );
    assert!(lossy.is_err(), "a lossy codec fails a debug build");
    assert_eq!(fidelity_of(&events, "unreadable"), [Fidelity::Opaque]);
    assert!(
        opaque.is_err(),
        "a codec that cannot read its own capture fails a debug build"
    );
    assert_eq!(
        fidelity_of(&events, "panicking"),
        [Fidelity::Opaque],
        "a reconstruct that panics still leaves its event, and reads as not rebuilding"
    );
    assert!(reconstruct_panicked.is_err(), "and fails a debug build");
    assert_eq!(fidelity_of(&events, "uncomparable"), [Fidelity::Unverified]);
    assert_eq!(
        OPAQUE_REBUILDS.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "a type with nothing to compare by is never rebuilt"
    );
    assert_eq!(
        fidelity_of(&events, "float"),
        [Fidelity::Lossless, Fidelity::Lossy],
        "the rebuild reads the capture back from its text, as replay does"
    );
    assert!(
        text_lossy.is_err(),
        "and a value the text changes fails a debug build"
    );
    assert_eq!(
        fidelity_of(&events, "reordered"),
        [Fidelity::Unverified],
        "a difference in nothing but order is not lossy, and not a pass either"
    );
    assert_eq!(
        fidelity_of(&events, "large"),
        [Fidelity::Lossless, Fidelity::Unverified],
        "a capture past the limit is not checked"
    );
    assert_eq!(
        LARGE_REBUILDS.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "and is never rebuilt; only the small one was"
    );
    assert_eq!(fidelity_of(&events, "halved_async"), [Fidelity::Lossy]);
    assert!(
        async_lossy.is_err(),
        "the async record path fails a debug build too"
    );
    assert_eq!(
        fidelity_of(&events, "erring"),
        [Fidelity::Lossy],
        "an Ok rebuilt as an Err is lossy, whatever the error type offers"
    );
    assert!(ok_as_err.is_err(), "and fails a debug build");
    assert_ne!(
        fidelity_of(&events, "delegating"),
        [Fidelity::Lossy],
        "a set the check cannot see does not fail a debug build over its order"
    );
    assert_eq!(fidelity_of(&events, "delegating").len(), 1);
    assert_eq!(
        fidelity_of(&events, "fallible"),
        [Fidelity::Lossless],
        "a fallible site whose error compares by nothing is checked on its Ok arm"
    );
    assert_eq!(
        fidelity_of(&events, "failing"),
        [Fidelity::Unverified],
        "an error arm is not rebuilt, so it is neither checked nor a failure"
    );
    assert_eq!(
        fidelity_of(&events, "record_only"),
        [Fidelity::Opaque],
        "a site with no codec is recorded as not rebuildable, and that is not a failure"
    );
    assert_eq!(
        fidelity_of(&events, "executed"),
        [Fidelity::Unverified],
        "an Execute site is re-run on replay, so its capture is never rebuilt"
    );
}
