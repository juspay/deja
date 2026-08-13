//! The fail-stop guard is INERT outside replay.
//!
//! Its own test binary because the property is the absence of a runtime hook, and
//! `set_global_runtime_hook` is a one-shot `OnceLock` per process — a test that
//! installs one cannot coexist with this.
//!
//! Why this is worth a test: `catch_fail_stop` is meant to wrap the host's whole
//! request. If it installed a `catch_unwind` unconditionally, every recording and
//! every deja-off production request would run under different panic semantics
//! than it does today — a cost paid by the 100 % of traffic that can never
//! fail-stop, since a fail-stop is replay-only. So outside replay the guard must
//! be a pure passthrough: the body runs, its value is returned, and a panic is
//! NOT caught.
#![allow(unused_braces)]

use std::sync::atomic::{AtomicBool, Ordering};

static BODY_RAN: AtomicBool = AtomicBool::new(false);

#[test]
fn guard_is_a_pure_passthrough_when_replay_is_not_active() {
    assert!(
        !deja::replay_is_active(),
        "no runtime hook is installed in this binary, so replay must be inactive"
    );

    // The body runs and its value is returned, untouched.
    let value = deja::catch_fail_stop(|| {
        BODY_RAN.store(true, Ordering::SeqCst);
        7u64
    });
    assert_eq!(
        value,
        Ok(7),
        "outside replay the guard must return the body's value unchanged"
    );
    assert!(
        BODY_RAN.load(Ordering::SeqCst),
        "outside replay the guard must run the body"
    );

    // A panic is NOT contained: production panic behaviour is byte-identical to
    // an unguarded call. Note the payload here even carries the fail-stop
    // sentinel — outside replay the guard does not so much as look at it.
    let previous_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let escaped = std::panic::catch_unwind(|| {
        deja::catch_fail_stop(|| -> u64 {
            panic!("{} synthetic, outside replay", deja::FAIL_STOP_SENTINEL)
        })
    });
    std::panic::set_hook(previous_hook);
    assert!(
        escaped.is_err(),
        "outside replay the guard must NOT catch a panic — record-mode and \
         deja-off builds keep their exact panic semantics"
    );
}
