#![allow(clippy::unwrap_used)] // tests panic on failure by design

//! Property 6: a hash-key MISS on replay fail-stops. Deliberately, and this test
//! is the lock that keeps someone from "fixing" it into a graceful fallback.
//!
//! The fail-stop default is wrong for a lot of boundaries — absorbing a miss on
//! a cache read lets the rest of a correlation be scored instead of dying at the
//! first novel call. The hash keys are the case that goes the other way. Absorb
//! this miss and a fresh pair is drawn, every collection in the correlation
//! iterates on keys the recording never held, and every downstream body diff
//! reads as a genuine divergence rather than as the artifact it is. A fail-stop's
//! unwind is at least recognisable as an unwind; that noise is not.
//!
//! `ReplayStrategy::Execute` is not the remedy here either, despite what the
//! generic miss message suggests: executing re-runs the draw, which is exactly
//! the nondeterminism the seam exists to remove.
//!
//! Own test binary: `set_global_runtime_hook` is one-shot, and this one needs a
//! replay hook whose table is empty so every lookup misses.

use std::collections::HashMap;

use deja_runtime::DejaRandomState;

#[test]
fn a_hash_key_miss_fail_stops_instead_of_drawing_a_fresh_pair() {
    // An EMPTY lookup table: every lookup misses.
    let table = deja_runtime::replay::LookupTable {
        recording_id: "hash-seed-miss-test".to_string(),
        policy_version: 1,
        entries: vec![],
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lookup.json");
    std::fs::write(&path, serde_json::to_vec(&table).expect("serialize")).expect("write table");

    let hook = deja_runtime::replay::LookupTableHook::from_source(
        deja_runtime::replay::LocalFileLookupSource::new(path),
        deja_runtime::replay::InMemoryObservedSink::new(),
    )
    .expect("hook");
    deja_runtime::set_global_runtime_hook(Some(deja_runtime::RuntimeHook::LookupReplay(hook)))
        .expect("install runtime hook");
    drop(dir);

    let _guard = deja_context::enter(deja_context::ContextSnapshot::new("req-hash-seed-miss"));

    // The deliberate fail-stop would otherwise print as if the test failed.
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(|| {
        let mut map: HashMap<u32, u32, DejaRandomState> = HashMap::default();
        map.insert(1, 1);
        map
    });
    std::panic::set_hook(previous);

    let payload = outcome.expect_err(
        "a hash-key miss must fail-stop. Returning a freshly drawn pair would let \
         the request continue on keys the recording never held, and every map \
         iteration downstream would diverge as if the candidate had changed",
    );
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or_default();

    assert!(
        message.contains(deja_runtime::FAIL_STOP_SENTINEL),
        "the stop must carry the sentinel, or the request guard re-raises it as an \
         ordinary bug panic instead of scoring it as a replay stop; got: {message}"
    );
    assert!(
        message.contains("hash_seed"),
        "the stop must name the boundary that missed, or an operator reading a pod \
         log cannot tell which seam halted the request; got: {message}"
    );
}
