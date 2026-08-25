//! `#[deja::boundary(on_miss = ...)]` — the declared graceful Substitute-miss.
//!
//! `graceful_miss.rs` proves the RUNTIME seam (`dispatch_async_or_miss`) returns
//! the caller's value on a miss. This proves the MACRO reaches it: a boundary
//! that declares `on_miss` returns that value, a boundary that does not still
//! fail-stops, and the declared expression can name the `deja::SubstituteMiss`
//! marker so a degraded continuation stays attributable.
//!
//! Why the default is wrong for the boundaries that get one: a candidate under
//! review adds boundary calls the recording never made — that is what a change
//! IS — and unwinding the request at the first one answers nothing, so the whole
//! correlation scores as a 500 and censors every other signal in the run. The
//! miss is still emitted as a blocking divergence before `on_miss` runs; only
//! the continuation changes.
//!
//! Own test binary: `set_global_runtime_hook` is a one-shot `OnceLock`, so only
//! one install per process.
#![allow(unused_braces)]

use std::sync::atomic::{AtomicUsize, Ordering};

/// Counts real-body executions. A replay miss must NEVER run the body — neither
/// the graceful arm nor the fail-stop arm.
static BODY_RUNS: AtomicUsize = AtomicUsize::new(0);

const REAL_BODY: u64 = 0xBAD_u64;

/// A host error type that can represent a miss. Deja never names it; the
/// declaration site does, which is the whole inversion — deja cannot construct
/// an `Err` for an `E` it does not know, but the boundary's author can.
#[derive(Debug, PartialEq, Eq)]
enum HostError {
    NotRecorded(String),
}

impl From<deja::SubstituteMiss> for HostError {
    fn from(miss: deja::SubstituteMiss) -> Self {
        Self::NotRecorded(format!("{}::{}", miss.component, miss.method))
    }
}

/// Declared graceful miss. `None` is the honest miss value for a cache read: it
/// means "not in cache", which is TRUE on replay, and it asserts nothing the
/// recording did not show. (Contrast a fabricated egress response, which claims
/// a third party answered when none did.)
#[deja::boundary(
    boundary = "imc",
    component = "tests::macro_on_miss",
    operation = "graceful_get",
    replay = Substitute,
    codec = SerdeCodec,
    on_miss = None,
)]
async fn graceful_get(key: &str) -> Option<u64> {
    let _ = key;
    BODY_RUNS.fetch_add(1, Ordering::SeqCst);
    Some(REAL_BODY)
}

/// Declared graceful miss that NAMES the marker, so the host's error carries
/// which boundary had no recorded answer instead of an ad-hoc string.
#[deja::boundary(
    boundary = "imc",
    component = "tests::macro_on_miss",
    operation = "attributed_get",
    replay = Substitute,
    // Ok-only codec: the host error need not be serde, which is exactly the
    // reason deja cannot construct one itself.
    codec = ResultOkCodec,
    on_miss = Err(__deja_miss.into()),
)]
async fn attributed_get(key: &str) -> Result<u64, HostError> {
    let _ = key;
    BODY_RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(REAL_BODY)
}

/// NO `on_miss` — today's default must be untouched. Egress depends on it.
#[deja::boundary(
    boundary = "imc",
    component = "tests::macro_on_miss",
    operation = "strict_get",
    replay = Substitute,
    codec = SerdeCodec,
)]
async fn strict_get(key: &str) -> Option<u64> {
    let _ = key;
    BODY_RUNS.fetch_add(1, Ordering::SeqCst);
    Some(REAL_BODY)
}

/// Minimal std-only executor: the replay-miss dispatch does only synchronous
/// work (arg image, in-memory lookup, `on_miss`) and never yields, so a single
/// poll drives it to completion. Avoids a tokio dev-dependency (and the shared
/// lockfile churn) in the `deja` facade crate just for this test.
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

/// Install an EMPTY lookup table as the replay hook — every lookup misses.
/// `set_global_runtime_hook` is one-shot, so this runs once per binary and the
/// tests below share it.
fn install_replay_hook() {
    static ONCE: std::sync::Once = std::sync::Once::new();
    ONCE.call_once(|| {
        let table = deja::LookupTable {
            recording_id: "macro-on-miss-test".to_string(),
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
        deja::set_global_runtime_hook(Some(deja::RuntimeHook::LookupReplay(hook)))
            .expect("install runtime hook");
        // The table was read at install; the tempdir may go now.
        drop(dir);
    });
}

#[test]
fn declared_on_miss_returns_its_value_instead_of_fail_stopping() {
    install_replay_hook();
    let before = BODY_RUNS.load(Ordering::SeqCst);

    let value = block_on(graceful_get("novel-key"));

    assert_eq!(
        value, None,
        "a Substitute MISS on a boundary declaring `on_miss` must return the declared \
         value — not panic, and not serve a value the recording never held"
    );
    assert_eq!(
        BODY_RUNS.load(Ordering::SeqCst),
        before,
        "the real boundary body must NOT run on a replay miss"
    );
}

#[test]
fn declared_on_miss_can_name_the_substitute_miss_marker() {
    install_replay_hook();

    let value = block_on(attributed_get("novel-key"));

    assert_eq!(
        value,
        Err(HostError::NotRecorded(
            "tests::macro_on_miss::attributed_get".to_string()
        )),
        "the `on_miss` expression must see `__deja_miss` carrying the boundary, \
         component and method that missed, so the host error is attributable"
    );
}

#[test]
fn a_boundary_without_on_miss_still_fail_stops() {
    install_replay_hook();
    let before = BODY_RUNS.load(Ordering::SeqCst);

    // The default panic hook would print this deliberate fail-stop as if it were
    // a test failure; silence it for the duration of the call only.
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        block_on(strict_get("novel-key"))
    }));
    std::panic::set_hook(previous);

    let payload = outcome.expect_err(
        "a Substitute MISS on a boundary that declares NO `on_miss` must still \
         fail-stop — egress correctness depends on that default",
    );
    let message = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .unwrap_or_default();
    assert!(
        message.contains(deja::FAIL_STOP_SENTINEL),
        "the fail-stop must carry the sentinel so the request guard can classify \
         it; got: {message}"
    );
    assert_eq!(
        BODY_RUNS.load(Ordering::SeqCst),
        before,
        "the real boundary body must NOT run on a replay miss"
    );
}
