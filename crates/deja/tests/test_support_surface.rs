//! `deja::test_support::recording_correlation` must be sufficient on its own for
//! a host to record a boundary from a test.
//!
//! The gap this closes was found from the outside: a hyperswitch test called
//! `set_recording_decision` and recorded nothing, because a decision registered
//! for a correlation is not the same as that correlation being CURRENT. The
//! facade re-exported the setter but not the guard, so the host had to add
//! `deja-context` as a second dependency to write one test.
//!
//! Inside this crate `deja_context` is reachable anyway, so its absence here is
//! discipline rather than proof; what the test does establish is that the
//! surface is SUFFICIENT — one call, and the guard never has to be named. The
//! host-side proof is the router test dropping its `deja-context` dev-dependency
//! once the pin moves.
//!
//! Own test binary: `set_global_runtime_hook` is a one-shot `OnceLock`.

use serde_json::json;

#[deja::boundary(
    boundary = "unit",
    component = "TestSupportSurface",
    operation = "probe",
    replay = Substitute,
    codec = SerdeCodec,
    correlation = None::<String>,
    args = json!({ "n": n }),
)]
fn probe(n: u64) -> u64 {
    n + 1
}

#[test]
fn recording_correlation_is_enough_to_record_a_boundary() {
    let artifacts = tempfile::tempdir().expect("tempdir");
    deja_runtime::set_global_runtime_hook(Some(deja_runtime::RuntimeHook::Recording(
        std::sync::Arc::new(
            deja_runtime::RecordingHook::new(artifacts.path()).expect("recording hook"),
        ),
    )))
    .expect("install recording hook");

    // The whole surface under test: one call, one bound guard, no other import.
    let recording = deja::test_support::recording_correlation("req-test-support-surface");
    assert_eq!(probe(41), 42);
    drop(recording);

    deja::flush_global_runtime_hook().ok();
    let events = deja_runtime::read_events(artifacts.path()).expect("events");
    let probes: Vec<_> = events.iter().filter(|e| e.method_name == "probe").collect();

    assert_eq!(
        probes.len(),
        1,
        "entering via recording_correlation must record the boundary; \
         recorded methods were {:?}",
        events.iter().map(|e| &e.method_name).collect::<Vec<_>>()
    );
    assert_eq!(
        probes[0].correlation_id.as_deref(),
        Some("req-test-support-surface"),
        "the recorded event must carry the correlation the guard entered"
    );
}
