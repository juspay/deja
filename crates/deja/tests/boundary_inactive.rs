//! Inactive-hook fast-path proof for the `#[deja::boundary]` family.
//!
//! This test deliberately NEVER sets `DEJA_MODE` / `DEJA_ARTIFACT_DIR`, so the
//! process-global recording / runtime hooks both resolve to `None` and Déjà is
//! inactive. Under the single `dispatch` seam the inactive path must run the real
//! block and return WITHOUT evaluating the `args` thunk — the zero-overhead
//! recording-disabled behavior the design preserves (recording-capture-decoupled
//! §3, §6 Step 4).
//!
//! Kept in its own test binary because the global hook is a process-wide
//! `OnceLock`: a sibling test that activates recording would initialize that
//! lock and make "inactive" unobservable in the same binary.

#![allow(unused_braces)]

use std::sync::atomic::{AtomicUsize, Ordering};

// Counts how many times the macro evaluated the `args` expression. When Déjà is
// inactive, the `dispatch` seam must NOT evaluate the args thunk, so this stays
// at zero even after the boundary runs.
static ARGS_EVALUATED: AtomicUsize = AtomicUsize::new(0);

fn args_probe(value: u64) -> serde_json::Value {
    ARGS_EVALUATED.fetch_add(1, Ordering::SeqCst);
    serde_json::json!({ "input": value })
}

#[deja::boundary(
    boundary = "inactive_probe",
    component = "BoundaryInactiveTest",
    operation = "sync_probe",
    args = args_probe(value),
    result = { (serde_json::json!({ "output": *__deja_result }), false) },
)]
fn sync_probe(value: u64) -> u64 {
    value + 1
}

#[test]
fn inactive_boundary_runs_block_without_serializing_args() {
    // No DEJA_* env set: Déjà is inactive for this whole binary.
    assert_eq!(
        sync_probe(41),
        42,
        "the real block runs and returns normally"
    );
    assert_eq!(
        ARGS_EVALUATED.load(Ordering::SeqCst),
        0,
        "inactive boundary must NOT evaluate the args thunk (zero-overhead path)"
    );
}
