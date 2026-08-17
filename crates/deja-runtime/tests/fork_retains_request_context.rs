//! A detached fork must still belong to the request that spawned it — including
//! after that request has returned and its registry entry has been cleared.
//!
//! This is the shape that made a whole class of real work invisible. A host
//! spawns fire-and-forget work from inside a request (registering a key with the
//! decision service, saving a payment method); the response returns; ingress
//! drops `RecordingDecisionGuard`, which clears the correlation-keyed decision;
//! and only then does the child run. With the decision resolved by lookup, the
//! child finds nothing, `capture_verdict` answers `SkipNoDecision`, and the work
//! is absent from the tape — while a replayed candidate still performs it and
//! has it written off as environmental.
//!
//! `spawn_fork` captures the context at spawn, while the request is still live,
//! and re-enters it per poll. These tests pin both directions: a `Record`
//! request's tail still captures, and a `Skip` request's tail still does not.

use std::sync::{Arc, Mutex, OnceLock};

use deja_runtime::DejaCorrelationLayer;
use tracing_subscriber::prelude::*;

fn install_process_record_hook() {
    static HOOK_DIR: OnceLock<tempfile::TempDir> = OnceLock::new();
    HOOK_DIR.get_or_init(|| {
        let dir = tempfile::tempdir().expect("hook tempdir");
        let hook = deja_runtime::RecordingHook::new(dir.path()).expect("recording hook");
        deja_runtime::set_global_runtime_hook(Some(deja_runtime::RuntimeHook::Recording(
            Arc::new(hook),
        )))
        .expect("install the process Record hook");
        dir
    });
}

/// What the detached tail could see about the request it came from.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct TailSaw {
    correlation_id: Option<String>,
    captures: bool,
}

/// Drive one request that forks a tail, then tear the request down BEFORE the
/// tail is allowed to run — the ordering that makes this a defect rather than a
/// theory.
///
/// A current-thread runtime is deliberate: every task is polled on this thread,
/// so the thread-local test subscriber applies to the tail as well. `yield_now`
/// is what lets teardown win the race, without a sleep.
fn run_request_forking_a_tail(correlation_id: &str, decision: bool) -> TailSaw {
    install_process_record_hook();
    let saw = Arc::new(Mutex::new(TailSaw::default()));

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");

    let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
    tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
            // Ingress: the sampler's decision goes into the correlation-keyed
            // registry before the request's span exists.
            deja_context::set_recording_decision(correlation_id, decision);

            {
                let request =
                    tracing::info_span!("deja::http_incoming", request_id = %correlation_id);
                let _entered = request.enter();

                let saw = Arc::clone(&saw);
                deja_runtime::spawn_fork(async move {
                    // Runs after teardown — see the yields below.
                    let hook = deja_runtime::installed_runtime_hook().expect("hook installed");
                    *saw.lock().expect("saw lock") = TailSaw {
                        correlation_id: deja_context::current_correlation_id(),
                        captures: hook.capture_verdict().should_capture(),
                    };
                });
            }

            // Request teardown: `RecordingDecisionGuard` drops and the registry
            // entry goes away. The tail has not been polled yet.
            deja_context::clear_recording_decision(correlation_id);
            assert_eq!(
                deja_context::recording_decision(correlation_id),
                None,
                "the registry entry must be gone before the tail runs — that is \
                 the condition under test",
            );

            // Now let the tail run.
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        });
    });

    let saw = saw.lock().expect("saw lock").clone();
    saw
}

#[test]
fn a_detached_tail_still_captures_after_its_request_has_torn_down() {
    let saw = run_request_forking_a_tail("fork-retains-record", true);
    assert_eq!(
        saw,
        TailSaw {
            correlation_id: Some("fork-retains-record".to_owned()),
            captures: true,
        },
        "a fire-and-forget tail of a sampled-IN request must record; resolving \
         the decision by lookup instead of carrying it is what silently dropped \
         every such call",
    );
}

#[test]
fn a_detached_tail_of_a_sampled_out_request_still_does_not_capture() {
    let saw = run_request_forking_a_tail("fork-retains-skip", false);
    assert_eq!(
        saw,
        TailSaw {
            correlation_id: Some("fork-retains-skip".to_owned()),
            captures: false,
        },
        "carrying the decision must carry an explicit Skip just as faithfully as \
         a Record — this is the guard against the fix becoming a way to record \
         what the sampler excluded",
    );
}

#[test]
fn a_tail_forked_outside_any_request_captures_nothing() {
    install_process_record_hook();
    let saw = Arc::new(Mutex::new(TailSaw::default()));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("current-thread runtime");

    let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
    tracing::subscriber::with_default(subscriber, || {
        runtime.block_on(async {
            let saw = Arc::clone(&saw);
            deja_runtime::spawn_fork(async move {
                let hook = deja_runtime::installed_runtime_hook().expect("hook installed");
                *saw.lock().expect("saw lock") = TailSaw {
                    correlation_id: deja_context::current_correlation_id(),
                    captures: hook.capture_verdict().should_capture(),
                };
            });
            tokio::task::yield_now().await;
            tokio::task::yield_now().await;
        });
    });

    assert_eq!(
        *saw.lock().expect("saw lock"),
        TailSaw {
            correlation_id: None,
            captures: false,
        },
        "capturing the spawn-time context must not invent one — a fork with no \
         request behind it stays uncorrelated and records nothing",
    );
}
