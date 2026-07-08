//! Sync→async bridge for the lifecycle worker's Postgres writes.
//!
//! The worker runs on a plain thread (it blocks on docker/compose for
//! minutes); the store is async (sqlx). `StoreCtx` carries a tokio runtime
//! `Handle` captured in the async run-creation handler, and every write is
//! best-effort: persistence of dashboard state must never fail a run that the
//! file-backed flow would have completed.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use deja_store::Store;

#[derive(Clone)]
pub struct StoreCtx {
    inner: Option<Inner>,
    run_id: String,
}

#[derive(Clone)]
struct Inner {
    handle: tokio::runtime::Handle,
    store: Arc<Store>,
    log_seq: Arc<AtomicI64>,
}

impl StoreCtx {
    pub fn new(run_id: &str, store: Option<(tokio::runtime::Handle, Arc<Store>)>) -> Self {
        Self {
            inner: store.map(|(handle, store)| Inner {
                handle,
                store,
                log_seq: Arc::new(AtomicI64::new(0)),
            }),
            run_id: run_id.to_owned(),
        }
    }

    pub fn disabled(run_id: &str) -> Self {
        Self::new(run_id, None)
    }

    fn exec<F, Fut>(&self, f: F)
    where
        F: FnOnce(Arc<Store>, String) -> Fut,
        Fut: std::future::Future<Output = Result<(), deja_store::StoreError>>,
    {
        if let Some(inner) = &self.inner {
            let fut = f(inner.store.clone(), self.run_id.clone());
            if let Err(e) = inner.handle.block_on(fut) {
                eprintln!("lifecycle[store]: write failed for {}: {e}", self.run_id);
            }
        }
    }

    /// Append a worker log line (also echoed to stderr by the caller).
    pub fn log(&self, stage: &str, line: &str) {
        let Some(inner) = &self.inner else { return };
        let seq = inner.log_seq.fetch_add(1, Ordering::Relaxed);
        let stage = stage.to_owned();
        let line = line.to_owned();
        self.exec(move |store, run_id| async move {
            store.append_log(&run_id, &stage, seq, &line).await
        });
    }

    /// Record a stage transition (closes the previous running stage as ok).
    pub fn stage(&self, stage: &str, step: u32, total: u32) {
        let stage = stage.to_owned();
        self.exec(move |store, run_id| async move {
            store
                .stage_transition(&run_id, &stage, Some(step as i32), Some(total as i32), "ok")
                .await
        });
    }

    /// Terminal state: close the running stage and update the run row.
    pub fn finish(&self, ok: bool, failure: Option<&str>) {
        let stage_status = if ok { "ok" } else { "failed" };
        let state = if ok { "completed" } else { "failed" };
        let failure_json = failure.map(|f| serde_json::json!({ "message": f }));
        let stage_status = stage_status.to_owned();
        let state = state.to_owned();
        self.exec(move |store, run_id| async move {
            store.stage_finish(&run_id, &stage_status).await?;
            store
                .update_run_state(&run_id, &state, failure_json.as_ref())
                .await
        });
    }

    pub fn run_state(&self, state: &str) {
        let state = state.to_owned();
        self.exec(move |store, run_id| async move {
            store.update_run_state(&run_id, &state, None).await
        });
    }

    pub fn run_recording(&self, recording_id: &str) {
        let recording_id = recording_id.to_owned();
        self.exec(move |store, run_id| async move {
            store.set_run_recording(&run_id, &recording_id).await
        });
    }

    pub fn candidate_sha(&self, sha256: &str) {
        let sha256 = sha256.to_owned();
        self.exec(move |store, run_id| async move {
            store.set_run_candidate_sha(&run_id, &sha256).await
        });
    }

    pub fn result(&self, verdict: Option<&str>, scorecard: Option<&serde_json::Value>) {
        let verdict = verdict.map(str::to_owned);
        let scorecard = scorecard.cloned();
        self.exec(move |store, run_id| async move {
            store
                .set_run_result(&run_id, verdict.as_deref(), scorecard.as_ref())
                .await
        });
    }

    /// Upsert the recording catalog row (machine actor: the lifecycle is the
    /// only writer now that the legacy register endpoint is gone). The
    /// manifest is the compactor's session seal — coverage badges read it.
    pub fn recording(
        &self,
        recording_id: &str,
        source_path: Option<&str>,
        event_count: Option<i64>,
        correlation_count: Option<i64>,
        bytes: Option<i64>,
        manifest: Option<&serde_json::Value>,
    ) {
        let recording_id = recording_id.to_owned();
        let source_path = source_path.map(str::to_owned);
        let manifest = manifest.cloned();
        self.exec(move |store, _run_id| async move {
            store
                .upsert_recording(
                    &recording_id,
                    source_path.as_deref(),
                    event_count,
                    correlation_count,
                    bytes,
                    manifest.as_ref(),
                    "system:lifecycle",
                )
                .await
        });
    }

    /// Register an artifact for this run (and optionally a recording).
    pub fn artifact(&self, recording_id: Option<&str>, kind: &str, path: &std::path::Path) {
        let meta = std::fs::metadata(path).ok();
        let Some(meta) = meta else {
            return; // artifact absent — nothing to register
        };
        let bytes = meta.len() as i64;
        let kind = kind.to_owned();
        let uri = path.display().to_string();
        let recording_id = recording_id.map(str::to_owned);
        self.exec(move |store, run_id| async move {
            store
                .register_artifact(
                    Some(&run_id),
                    recording_id.as_deref(),
                    &kind,
                    &uri,
                    Some(bytes),
                    None,
                )
                .await
        });
    }
}
