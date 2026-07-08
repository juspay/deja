//! Run lifecycle endpoints — create, fetch status.

use serde::{Deserialize, Serialize};

use crate::lifecycle::StoreCtx;
use crate::{new_id, read_json, write_json, HarnessRoot, Run, RunMode, RunSpec, RunStatus};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateRunResponse {
    pub run_id: String,
    pub status: RunStatus,
}

/// Build and persist a Pending run record (no worker yet). The caller is
/// responsible for inserting the store row (if a store is connected) BEFORE
/// spawning the worker — stage rows reference the run row by foreign key.
pub fn persist_new(root: &HarnessRoot, spec: RunSpec) -> std::io::Result<Run> {
    let run_id = new_id("run");
    let run = Run {
        run_id: run_id.clone(),
        spec,
        status: RunStatus::Pending,
        recording_id: None,
        candidate_image: None,
        failure_reason: None,
        stage: Some("queued".to_owned()),
        step: 0,
        steps_total: 0,
        stage_updated_ms: crate::now_ms(),
    };
    write_json(&root.run_path(&run_id), &run)?;
    Ok(run)
}

/// Spawn the lifecycle worker for an already-persisted run.
///
/// The worker drives the run asynchronously (compose up → record/replay →
/// score → tear down) on a background thread, persisting progress to the
/// file store and (via `ctx`) the Postgres store.
pub fn spawn_worker(root: &HarnessRoot, run_id: &str, ctx: StoreCtx) {
    let root_path = root.root.clone();
    let worker_run_id = run_id.to_owned();
    std::thread::spawn(move || match HarnessRoot::new(&root_path) {
        Ok(root) => crate::lifecycle::drive(&root, &worker_run_id, &ctx),
        Err(e) => eprintln!(
            "lifecycle: cannot open HarnessRoot {}: {e}",
            root_path.display()
        ),
    });
}

/// Serialized run mode (the store's `mode` column).
pub fn mode_str(mode: RunMode) -> &'static str {
    match mode {
        RunMode::Record => "record",
        RunMode::Replay => "replay",
    }
}

/// `GET /runs/{id}` — fetch persisted run record.
pub fn get(root: &HarnessRoot, run_id: &str) -> std::io::Result<Run> {
    read_json::<Run>(&root.run_path(run_id))
}
