//! A declined miss is answered from the tape's own shape.
//!
//! A `Substitute` boundary whose reconstruct closure answers a MISS with
//! `Reconstructed::Synthesized` returns that value instead of fail-stopping,
//! and the observation records that it did. This is
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
const COMPONENT: &str = "BorrowFallbackTest";
const RECORDED_ID: &str = "pay_recorded";
const REAL_BODY: &str = "the-real-boundary-ran";

/// Mirrors the vendor Superposition `read` boundary: engage only when a deja
/// hook is live, build a rank-2 identity, dispatch under `Substitute` with a
/// recoverable `on_miss`.
async fn read_config(operation: &'static str, key: &str) -> Result<String, String> {
    let caller = std::panic::Location::caller();

    // Passthrough when deja is inactive — no observation, no allocation.
    if !deja::__private::observation_is_active() {
        return Ok(REAL_BODY.to_string());
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
    let observation =
        deja::__private::CrossingObservation::with_correlation(spec, identity, caller, correlation);

    let args = json!({ "key": key });
    deja::__private::dispatch_async(
        observation,
        move || args,
        // The "real" run: under a replay MISS this must NOT execute.
        || async { Ok::<String, String>(REAL_BODY.to_string()) },
        // ONE closure answers both halves of the lookup. The absorbing
        // behaviour is no longer declared alongside it — the miss arm RETURNS
        // `Synthesized`, so the observation records what the site actually did
        // rather than what the seam was told to expect. A hand-built seam can
        // no longer degrade gracefully while being scored as though the miss
        // had killed the request, because there is no second place to say so.
        |input| match input {
            deja::__private::ReconstructInput::Hit(v) => {
                match v
                    .get("Ok")
                    .and_then(|ok| ok.get("payment_id"))
                    .and_then(serde_json::Value::as_str)
                {
                    Some(id) => deja::__private::Reconstructed::Value(Ok(id.to_string())),
                    None => deja::__private::Reconstructed::Failed(String::from(
                        "test codec: recorded payload carried no payment_id",
                    )),
                }
            }
            // THE SITE DECLINES. Before the seam fallback this reached
            // `fail_stop_substitute_miss` and killed the request; the site is
            // unchanged and says exactly what it always said.
            deja::__private::ReconstructInput::Miss(_) => deja::__private::Reconstructed::NoValue,
        },
        // extract: (Value, is_error) image of a live result (record/execute path).
        |r: &Result<String, String>| match r {
            Ok(n) => (json!({ "Ok": n }), false),
            Err(e) => (json!({ "Err": e }), true),
        },
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
        RawWaker::new(std::ptr::null(), &RawWakerVTable::new(clone, nop, nop, nop))
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
fn a_declined_miss_is_answered_from_the_tape_rather_than_stopping() {
    // OPT IN. The fallback defaults to STOPPING and must keep doing so until an
    // absorbed miss costs something on the scorecard — today it is subtracted
    // from the blocking-reason count and is not a term in a correlation's
    // `passed`, so flipping the default would turn a loud blocking failure into
    // a silent non-blocking one at scale. This test exercises the mechanism; it
    // is not evidence that the mechanism should be on.
    unsafe { std::env::set_var("DEJA_MISS_FALLBACK", "answer") };

    // The tape covers this SITE but not this CALL: one recorded entry at
    // (imc, BorrowFallbackTest, read), and the call below asks with different
    // args, so the lookup misses and the site declines.
    let table = deja::LookupTable {
        recording_id: "borrow-fallback-test".to_string(),
        policy_version: deja::POLICY_VERSION,
        entries: vec![deja::LookupEntry {
            key: deja::LookupKey {
                correlation_id: None,
                bucket_id: None,
                fork_seq: 0,
                boundary: BOUNDARY.to_string(),
                component: COMPONENT.to_string(),
                operation: "read".to_string(),
                locus: deja::Locus::DeclaredSite("recorded-site".to_string()),
                args_hash: 1,
                occurrence: 0,
            },
            result: json!({ "Ok": { "payment_id": RECORDED_ID } }),
            source_event_global_sequence: 1,
        }],
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lookup.json");
    std::fs::write(&path, serde_json::to_vec(&table).expect("serialize")).expect("write table");

    let hook = deja::LookupTableHook::from_source(
        deja::LocalFileLookupSource::new(path),
        deja::InMemoryObservedSink::new(),
    )
    .expect("hook");
    deja::set_global_runtime_hook(Some(deja::RuntimeHook::LookupReplay(hook)))
        .expect("install runtime hook");

    let answered = block_on(read_config("read", "a_key_the_recording_never_had"))
        .expect("a declined miss must be ANSWERED, not fail-stopped");

    // It came from the tape's shape...
    assert!(
        !answered.is_empty(),
        "the borrowed payload rebuilt into the site's own type"
    );
    // ...but NOT verbatim. A recorded id handed back unchanged would flow into
    // a downstream call's args, that lookup would HIT, and the run would report
    // matches against a trace it never earned.
    assert_ne!(
        answered, RECORDED_ID,
        "a borrowed identity must be moved into the synthesized subspace, never \
         returned as recorded"
    );
    assert!(
        answered.starts_with(deja::synth::SYNTH_PREFIX),
        "the fabricated id must be MARKED so a diff shows it was not recorded: {answered}"
    );
    // And the real boundary still never ran.
    assert_ne!(
        answered, REAL_BODY,
        "the live boundary must not run on a replay miss"
    );
}
