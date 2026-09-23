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

    let lossy = std::panic::catch_unwind(|| halved(3));
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
