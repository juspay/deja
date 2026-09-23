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
