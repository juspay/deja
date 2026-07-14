//! Typed runtime-hook install must be authoritative over legacy env recording.
//!
//! Kept in its own integration-test binary because `set_global_runtime_hook`,
//! `global_hook_from_env`, and the env-derived hooks are process-wide `OnceLock`s.

#[test]
fn typed_disabled_hook_suppresses_legacy_env_recording_fallback() {
    let artifacts = tempfile::tempdir().expect("tempdir");
    std::env::set_var("DEJA_MODE", "record");
    std::env::set_var("DEJA_ARTIFACT_DIR", artifacts.path());

    deja_runtime::set_global_runtime_hook(Some(deja_runtime::RuntimeHook::Disabled(
        deja_runtime::DisabledHook,
    )))
    .expect("install disabled runtime hook before env fallback is resolved");

    assert!(
        deja_runtime::global_hook_from_env().is_none(),
        "an explicit typed Disabled runtime hook must suppress the legacy DEJA_MODE=record standalone recorder"
    );
    assert!(
        !deja_runtime::capture_is_active(),
        "legacy recording env must not make capture active after a typed Disabled runtime hook is installed"
    );
}
