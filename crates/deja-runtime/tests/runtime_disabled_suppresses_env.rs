//! A typed `Disabled` runtime hook keeps recording OFF.
//!
//! Kept in its own integration-test binary because `set_global_runtime_hook`,
//! `global_hook_from_env`, and `observation_is_active` all read a process-wide
//! `OnceLock` that is installed exactly once at boot.

#[test]
fn typed_disabled_hook_exposes_no_recorder_and_keeps_observation_inactive() {
    deja_runtime::set_global_runtime_hook(Some(deja_runtime::RuntimeHook::Disabled(
        deja_runtime::DisabledHook,
    )))
    .expect("install disabled runtime hook");

    assert!(
        deja_runtime::global_hook_from_env().is_none(),
        "a typed Disabled runtime hook exposes no recording hook"
    );
    assert!(
        !deja_runtime::observation_is_active(),
        "a typed Disabled runtime hook keeps capture inactive"
    );
}
