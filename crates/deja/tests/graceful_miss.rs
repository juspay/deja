//! Graceful-miss counterpart to `fail_stop.rs`.
//!
//! A `Substitute` boundary built with `dispatch_async_or_miss` returns the
//! caller's `on_miss` value on a replay MISS instead of fail-stopping. This is
//! the Superposition read boundary's contract: in replay there is NO
//! Superposition service, so a config read that was never recorded (a novel
//! read) must degrade to a recoverable `Err` and let the caller fall back to
//! its default — it must NOT panic, and it must NOT run the real boundary.
//!
//! The boundary here is hand-built through the same `deja::__private` seam the
//! real `external_services::superposition::deja_boundary::read` uses (rank-2
//! span-path identity, `Substitute` strategy, `ExternalCall`), so a regression
//! that broke graceful degradation would be caught here.
//!
//! Own test binary: `set_global_runtime_hook` is a one-shot `OnceLock`, so only
//! one install per process.
#![allow(unused_braces)]

use serde_json::json;

const BOUNDARY: &str = "imc";
const COMPONENT: &str = "GracefulMissTest";
const MISS_SENTINEL: &str = "graceful-miss: no recorded value, caller falls back";
const REAL_BODY: u64 = 0xBAD_u64;

/// Mirrors the vendor Superposition `read` boundary: engage only when a deja
/// hook is live, build a rank-2 identity, dispatch under `Substitute` with a
/// recoverable `on_miss`.
async fn read_config(operation: &'static str, key: &str) -> Result<u64, String> {
    let caller = std::panic::Location::caller();

    // Passthrough when deja is inactive — no observation, no allocation.
    if !deja::__private::observation_is_active() {
        return Ok(REAL_BODY);
    }

    let correlation = deja::current_correlation_id();
    let scope = format!("graceful::{operation}");
    let identity = deja::__private::CallsiteIdentity {
        version: 1,
        source: deja::__private::CallsiteSource::SyntacticHash,
        id: None,
        scope: Some(scope.clone()),
        occurrence: deja::__private::next_boundary_occurrence(
            correlation.as_deref(),
            deja::__private::CallsiteSource::SyntacticHash,
            Some(&scope),
        ),
        caller_function: Some(operation.to_string()),
        lexical_path: Some(scope.clone()),
        syntax_hash: Some(deja::__private::stable_callsite_hash(&scope)),
        span_path: deja::__private::current_span_path(),
    };

    let semantics = deja::__private::BoundarySemantics {
        replay_strategy: deja::ReplayStrategy::Substitute,
        kind: Some(BOUNDARY.to_string()),
        declaration: Some(
            deja::BoundaryDeclaration::default().operation(deja::OperationKind::ExternalCall),
        ),
    };
    let spec =
        deja::__private::BoundarySpec::with_semantics(BOUNDARY, COMPONENT, operation, semantics);
    let observation = deja::__private::CrossingObservation::with_correlation(
        spec,
        identity,
        caller,
        correlation,
    );

    let args = json!({ "key": key });
    deja::__private::dispatch_async_or_miss(
        observation,
        move || args,
        // The "real" run: under a replay MISS this must NOT execute.
        || async { Ok::<u64, String>(REAL_BODY) },
        // reconstruct: rebuild the result from a recorded value (HIT path only).
        |v: serde_json::Value| match v.get("Ok").and_then(serde_json::Value::as_u64) {
            Some(n) => deja::__private::Reconstructed::Value(Ok(n)),
            None => deja::__private::Reconstructed::Failed,
        },
        // extract: (Value, is_error) image of a live result (record/execute path).
        |r: &Result<u64, String>| match r {
            Ok(n) => (json!({ "Ok": n }), false),
            Err(e) => (json!({ "Err": e }), true),
        },
        // on_miss: the graceful degrade — a recoverable Err, NOT a panic.
        || Err(MISS_SENTINEL.to_string()),
    )
    .await
}

/// Minimal std-only executor: the replay-miss dispatch does only synchronous
/// work (arg image, in-memory lookup, `on_miss`) and never yields, so a single
/// poll drives it to completion. Avoids pulling a tokio dev-dependency (and the
/// shared `Cargo.lock` churn) into the `deja` facade crate just for this test.
fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
    fn raw() -> RawWaker {
        fn clone(_: *const ()) -> RawWaker {
            raw()
        }
        fn nop(_: *const ()) {}
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, nop, nop, nop),
        )
    }
    // SAFETY: the vtable's fns are all no-ops on a null data pointer.
    let waker = unsafe { Waker::from_raw(raw()) };
    let mut cx = Context::from_waker(&waker);
    let mut fut = std::pin::pin!(fut);
    loop {
        if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        std::hint::spin_loop();
    }
}

#[test]
fn substitute_miss_returns_on_miss_value_in_replay() {
    // Empty lookup table → every lookup misses.
    let table = deja::LookupTable {
        recording_id: "graceful-miss-test".to_string(),
        policy_version: 1,
        entries: vec![],
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lookup.json");
    std::fs::write(&path, serde_json::to_vec(&table).expect("serialize")).expect("write table");

    let hook = deja::LookupTableHook::from_source(
        deja::LocalFileLookupSource::new(path),
        deja::InMemoryObservedSink::new(),
    )
    .expect("hook");
    // Install as a REPLAY hook (`is_replay() == true`).
    deja::set_global_runtime_hook(Some(deja::RuntimeHook::LookupReplay(hook)))
        .expect("install runtime hook");

    // A novel (unrecorded) config read in replay must degrade gracefully.
    let result = block_on(read_config("get_flag", "novel_key"));

    assert_eq!(
        result,
        Err(MISS_SENTINEL.to_string()),
        "a Substitute MISS built with dispatch_async_or_miss must return the on_miss \
         value (a recoverable Err) — NOT fail-stop, NOT serve a stale value"
    );
    assert_ne!(
        result,
        Ok(REAL_BODY),
        "the real boundary body must NOT run on a replay miss"
    );
}
