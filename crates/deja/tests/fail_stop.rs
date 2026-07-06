//! Partial-function model — the headline behavior: a `Substitute` boundary that
//! MISSES the recording during replay FAIL-STOPS (panics) instead of serving a
//! stale value or running the real boundary.
//!
//! An EMPTY lookup table guarantees a miss for any call, so this is decisive
//! without constructing matching keys. The hook is installed as a `LookupReplay`
//! runtime hook so `replay_is_active()` is true and the dispatch seam takes the
//! fail-stop arm. (Own test binary: `set_global_runtime_hook` is a one-shot
//! `OnceLock`, so only one install per process.)
#![allow(unused_braces)]

use serde_json::json;

#[deja::boundary(
    boundary = "redis",
    component = "FailStopTest",
    operation = "probe_get",
    // DEFAULT under the partial-function model; declared redundantly.
    replay = Substitute,
    codec = SerdeCodec,
    correlation = None::<String>,
    args = json!({ "key": key }),
)]
fn probe_get(key: &str) -> u64 {
    // The "real" boundary body. Under a Substitute MISS in replay it must NOT run
    // — the seam fail-stops first. If this ever returns, the test's `catch_unwind`
    // sees `Ok` and fails (proving the fall-through-to-run regression).
    let _ = key;
    0xBAD_u64
}

#[test]
fn substitute_miss_fail_stops_in_replay() {
    // Empty lookup table → every lookup misses.
    let table = deja::LookupTable {
        recording_id: "fail-stop-test".to_string(),
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

    // The fail-stop's panic backtrace is expected; silence the default hook.
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = std::panic::catch_unwind(|| probe_get("any-key"));
    std::panic::set_hook(prev);

    let payload = result.expect_err(
        "a Substitute MISS in replay must FAIL-STOP (panic) — NOT serve a stale value \
         and NOT run the real boundary",
    );
    let msg = payload
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| payload.downcast_ref::<&str>().copied())
        .unwrap_or("<non-string panic payload>");
    assert!(
        msg.contains("deja replay fail-stop") && msg.contains("probe_get"),
        "fail-stop panic must identify the boundary; got: {msg:?}"
    );
}
