//! Request-boundary containment of a replay fail-stop — the scope half of the
//! partial-function model.
//!
//! `fail_stop.rs` proves the STOP: a `Substitute` miss panics instead of serving
//! a stale value or running the real boundary. That is deliberate and unchanged.
//! This file proves the stop's SCOPE: wrapped in `deja::catch_fail_stop` /
//! `deja::catch_fail_stop_async`, the panic does not unwind PAST the guard, so a
//! host that owns a request boundary keeps its worker and can answer with a 5xx
//! instead of closing the connection with zero bytes (the transport fault that
//! made 8 of 73 correlations, and 178 of 182 omitted calls, unattributable on run
//! `rp-sbx-bb148328f7-…-0812141147885`).
//!
//! Three properties, in one test because they share a process-global panic hook
//! and a one-shot runtime hook (`set_global_runtime_hook` is a `OnceLock`), which
//! parallel `#[test]` functions would race on:
//!
//! 1. a sync `Substitute` miss is contained, and the real boundary never runs;
//! 2. an async `Substitute` miss is contained, and the real boundary never runs;
//! 3. a panic that is NOT a deja fail-stop is re-raised, not swallowed — a
//!    candidate bug must never be laundered into a replay verdict.
#![allow(unused_braces)]

use std::sync::atomic::{AtomicBool, Ordering};

use serde_json::json;

/// Set from inside a real boundary body. The fail-stop must leave it false: a
/// contained stop that still ran the boundary would have done the production I/O
/// the model exists to prevent.
static SYNC_BODY_RAN: AtomicBool = AtomicBool::new(false);
static ASYNC_BODY_RAN: AtomicBool = AtomicBool::new(false);

#[deja::boundary(
    boundary = "redis",
    component = "FailStopGuardTest",
    operation = "guarded_probe_get",
    replay = Substitute,
    codec = SerdeCodec,
    correlation = None::<String>,
    args = json!({ "key": key }),
)]
fn guarded_probe_get(key: &str) -> u64 {
    let _ = key;
    SYNC_BODY_RAN.store(true, Ordering::SeqCst);
    0xBAD_u64
}

#[deja::boundary(
    boundary = "http_outgoing",
    component = "FailStopGuardTest",
    operation = "guarded_probe_post",
    replay = Substitute,
    codec = SerdeCodec,
    correlation = None::<String>,
    args = json!({ "key": key }),
)]
async fn guarded_probe_post(key: &str) -> u64 {
    let _ = key;
    ASYNC_BODY_RAN.store(true, Ordering::SeqCst);
    0xBAD_u64
}

/// Minimal std-only executor. The replay fail-stop does only synchronous work
/// (arg image, in-memory lookup) before it panics, so a single poll drives the
/// guarded future to completion — no tokio dev-dependency, and no churn in the
/// shared lockfile. Mirrors `graceful_miss.rs`.
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
fn fail_stop_is_contained_at_the_guard_and_never_runs_the_boundary() {
    // Empty lookup table → every lookup misses.
    let table = deja::LookupTable {
        recording_id: "fail-stop-guard-test".to_string(),
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
    // Install as a REPLAY hook (`replay_is_active() == true`), which is the only
    // mode in which the guard engages at all.
    deja::set_global_runtime_hook(Some(deja::RuntimeHook::LookupReplay(hook)))
        .expect("install runtime hook");

    // The fail-stop's panic backtrace is expected; silence the default hook for
    // the whole test (parallel tests would race on this global, hence one test).
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));

    // 1 — sync. `reached_after` proves the guard, not the runtime, is the
    // frontier: the statement after the boundary call inside the guarded closure
    // must NOT run, while control DOES return to the caller of the guard.
    let mut reached_after_sync = false;
    let sync_result = deja::catch_fail_stop(|| {
        let value = guarded_probe_get("any-key");
        reached_after_sync = true;
        value
    });

    // 2 — async, the shape the HTTP ingress actually needs.
    let async_result = block_on(deja::catch_fail_stop_async(guarded_probe_post("any-key")));

    // 3 — a panic that is not a fail-stop must pass straight through the guard.
    let foreign = std::panic::catch_unwind(|| {
        deja::catch_fail_stop(|| -> u64 { panic!("candidate bug, not a deja fail-stop") })
    });

    std::panic::set_hook(previous_hook);

    let sync_stop = sync_result.expect_err(
        "a Substitute MISS under `catch_fail_stop` must be CONTAINED (returned as \
         Err) — not unwind past the guard, and not serve a value",
    );
    assert!(
        sync_stop.message().starts_with(deja::FAIL_STOP_SENTINEL),
        "a contained fail-stop must carry the sentinel the guard classifies on; got: {:?}",
        sync_stop.message()
    );
    assert!(
        sync_stop.message().contains("guarded_probe_get"),
        "a contained fail-stop must still name the boundary that stopped; got: {:?}",
        sync_stop.message()
    );
    assert!(
        !SYNC_BODY_RAN.load(Ordering::SeqCst),
        "containing the fail-stop must NOT run the real boundary — that would do \
         the production I/O the partial-function model exists to prevent"
    );
    assert!(
        !reached_after_sync,
        "the fail-stop must still discard the request's downstream subtree: the \
         statement after the missed boundary must not execute"
    );

    let async_stop = async_result.expect_err(
        "a Substitute MISS under `catch_fail_stop_async` must be CONTAINED — a \
         panic escapes a future through `poll`, so the guard must wrap the poll",
    );
    assert!(
        async_stop.message().contains("guarded_probe_post"),
        "a contained async fail-stop must name the boundary that stopped; got: {:?}",
        async_stop.message()
    );
    assert!(
        !ASYNC_BODY_RAN.load(Ordering::SeqCst),
        "containing an async fail-stop must NOT run the real boundary"
    );

    let payload = foreign.expect_err(
        "the guard must re-raise a panic that is NOT a deja fail-stop — swallowing \
         it would launder a candidate bug into a replay verdict",
    );
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic payload>");
    assert!(
        message.contains("candidate bug"),
        "a re-raised panic must reach the caller with its payload intact; got: {message:?}"
    );
}
