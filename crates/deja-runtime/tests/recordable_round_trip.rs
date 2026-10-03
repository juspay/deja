#![allow(clippy::unwrap_used)] // tests panic on failure by design

//! The `#[recordable]` delegate path measures each recorded value's round trip
//! and stamps it on the event, as the boundary macro does. A trait boundary is
//! how a service most often reaches deja, so the verdict has to be proven here
//! and not only through `#[deja::boundary]`.

use std::sync::Arc;

use deja_runtime::{read_events, Fidelity, RecordingHook};

/// Records half of itself and rebuilds double: an even value survives, an odd
/// one does not.
#[derive(Debug, PartialEq)]
pub struct Halved(u64);

impl serde::Serialize for Halved {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0 / 2)
    }
}

impl<'de> serde::Deserialize<'de> for Halved {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        <u64 as serde::Deserialize>::deserialize(deserializer).map(|half| Self(half * 2))
    }
}

#[deja_derive::recordable]
pub trait HalvingStore {
    fn halve(&self, n: u64) -> Halved;
}

struct RealStore;

impl HalvingStore for RealStore {
    fn halve(&self, n: u64) -> Halved {
        Halved(n)
    }
}

struct DejaStore {
    inner: Box<dyn HalvingStore + Send + Sync>,
    hook: Arc<dyn deja_runtime::DejaHook>,
}

delegate_halving_store_with_replay!(DejaStore, inner, hook, "imc");

#[test]
fn a_recordable_boundary_stamps_whether_its_value_came_back() {
    let _recording = deja_context::enter(
        deja_context::ContextSnapshot::new("req-recordable-round-trip")
            .with_recording_decision(true),
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = Arc::new(RecordingHook::new(dir.path()).expect("hook"));
    let store = DejaStore {
        inner: Box::new(RealStore),
        hook: hook.clone(),
    };

    assert_eq!(store.halve(4), Halved(4));
    let lossy = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| store.halve(3)));
    drop(store);
    drop(hook);

    let fidelity: Vec<Fidelity> = read_events(dir.path())
        .expect("read")
        .iter()
        .map(|event| event.fidelity)
        .collect();
    assert_eq!(
        fidelity,
        [Fidelity::Lossless, Fidelity::Lossy],
        "an even value survives halving, an odd one does not"
    );
    assert!(lossy.is_err(), "and the lossy one fails a debug build");
}
