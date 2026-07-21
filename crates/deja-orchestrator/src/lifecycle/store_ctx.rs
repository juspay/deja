//! Run-progress reporting seam (S2 of the k8s-executor design,
//! docs/design/replay-orchestrator-k8s-executor.md).
//!
//! `StoreCtx` is the narrow surface the lifecycle worker reports through. Two
//! transports behind identical method signatures, so the ~40 lifecycle call
//! sites are transport-blind:
//!
//! - **Pg** (in-process worker): a sync→async bridge onto the Postgres store.
//!   The worker runs on a plain thread (it blocks on docker/compose for
//!   minutes); the store is async (sqlx), so writes hop through a captured
//!   tokio `Handle`.
//! - **Http** (out-of-process runner, the k8s Job): each report becomes a
//!   [`RunEvent`] POSTed to the orchestrator's ingest endpoint
//!   (`POST /api/v1/runs/{id}/events`, service-token auth). The orchestrator
//!   applies it through [`apply_run_event`] — the SAME store mapping the Pg
//!   variant uses — so dashboards/SSE behave identically for both executors.
//!
//! Every write is best-effort: persistence of dashboard state must never fail
//! a run that the file-backed flow would have completed.

use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use deja_store::Store;

/// One push-back report from a lifecycle runner. Mirrors the [`StoreCtx`]
/// method surface 1:1; the wire shape for the ingest endpoint. Tagged by
/// `event` (not `kind` — that collides with `Artifact::kind`).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum RunEvent {
    /// Append a worker log line. `seq` is assigned by the SENDER so ordering
    /// survives the network (the orchestrator must not re-number).
    Log { stage: String, seq: i64, line: String },
    /// Stage transition (closes the previous running stage as ok).
    Stage { stage: String, step: u32, total: u32 },
    /// Coarse run state ("resolving" | "building" | "running" | …).
    State { state: String },
    /// Terminal state: close the running stage and settle the run row.
    Finish { ok: bool, failure: Option<String> },
    /// The recording this run drives (resolved by the runner for S3-source runs).
    Recording { recording_id: String },
    /// Candidate binary/image identity.
    CandidateSha { sha256: String },
    /// Verdict + scorecard.
    Result {
        verdict: Option<String>,
        scorecard: Option<serde_json::Value>,
    },
    /// Recording catalog upsert (machine actor).
    CatalogUpsert {
        recording_id: String,
        source_path: Option<String>,
        event_count: Option<i64>,
        correlation_count: Option<i64>,
        bytes: Option<i64>,
        manifest: Option<serde_json::Value>,
    },
    /// Register an artifact by URI. In-pod runners register pod-local paths
    /// until the S3-upload step lands (#21); the orchestrator streams local
    /// paths today and s3:// URIs later.
    Artifact {
        recording_id: Option<String>,
        kind: String,
        uri: String,
        bytes: Option<i64>,
    },
}

/// Apply one [`RunEvent`] to the Postgres store — the single mapping both the
/// Pg transport and the HTTP ingest handler go through.
pub async fn apply_run_event(
    store: &Store,
    run_id: &str,
    ev: &RunEvent,
) -> Result<(), deja_store::StoreError> {
    match ev {
        RunEvent::Log { stage, seq, line } => store.append_log(run_id, stage, *seq, line).await,
        RunEvent::Stage { stage, step, total } => {
            store
                .stage_transition(run_id, stage, Some(*step as i32), Some(*total as i32), "ok")
                .await
        }
        RunEvent::State { state } => store.update_run_state(run_id, state, None).await,
        RunEvent::Finish { ok, failure } => {
            let stage_status = if *ok { "ok" } else { "failed" };
            let state = if *ok { "completed" } else { "failed" };
            let failure_json = failure
                .as_ref()
                .map(|f| serde_json::json!({ "message": f }));
            store.stage_finish(run_id, stage_status).await?;
            store
                .update_run_state(run_id, state, failure_json.as_ref())
                .await
        }
        RunEvent::Recording { recording_id } => {
            store.set_run_recording(run_id, recording_id).await
        }
        RunEvent::CandidateSha { sha256 } => store.set_run_candidate_sha(run_id, sha256).await,
        RunEvent::Result { verdict, scorecard } => {
            store
                .set_run_result(run_id, verdict.as_deref(), scorecard.as_ref())
                .await
        }
        RunEvent::CatalogUpsert {
            recording_id,
            source_path,
            event_count,
            correlation_count,
            bytes,
            manifest,
        } => {
            store
                .upsert_recording(
                    recording_id,
                    source_path.as_deref(),
                    *event_count,
                    *correlation_count,
                    *bytes,
                    manifest.as_ref(),
                    "system:lifecycle",
                )
                .await
        }
        RunEvent::Artifact {
            recording_id,
            kind,
            uri,
            bytes,
        } => {
            store
                .register_artifact(
                    Some(run_id),
                    recording_id.as_deref(),
                    kind,
                    uri,
                    *bytes,
                    None,
                )
                .await
        }
    }
}

#[derive(Clone)]
pub struct StoreCtx {
    inner: Option<Inner>,
    run_id: String,
}

#[derive(Clone)]
enum Inner {
    Pg {
        handle: tokio::runtime::Handle,
        store: Arc<Store>,
        log_seq: Arc<AtomicI64>,
    },
    Http {
        agent: ureq::Agent,
        /// Orchestrator base URL (no trailing slash), e.g. `http://orchestrator:8080`.
        base: String,
        /// Bearer token when the orchestrator has DEJA_API_SERVICE_TOKEN set.
        token: Option<String>,
        /// X-Deja-Actor value (always required by the mutation boundary).
        actor: String,
        log_seq: Arc<AtomicI64>,
    },
}

impl StoreCtx {
    pub fn new(run_id: &str, store: Option<(tokio::runtime::Handle, Arc<Store>)>) -> Self {
        Self {
            inner: store.map(|(handle, store)| Inner::Pg {
                handle,
                store,
                log_seq: Arc::new(AtomicI64::new(0)),
            }),
            run_id: run_id.to_owned(),
        }
    }

    /// Out-of-process transport: report through the orchestrator's ingest
    /// endpoint (the in-Job runner's path).
    pub fn http(run_id: &str, base: &str, token: Option<&str>, actor: &str) -> Self {
        Self {
            inner: Some(Inner::Http {
                agent: ureq::AgentBuilder::new()
                    .timeout(std::time::Duration::from_secs(10))
                    .build(),
                base: base.trim_end_matches('/').to_owned(),
                token: token.map(str::to_owned),
                actor: actor.to_owned(),
                log_seq: Arc::new(AtomicI64::new(0)),
            }),
            run_id: run_id.to_owned(),
        }
    }

    pub fn disabled(run_id: &str) -> Self {
        Self::new(run_id, None)
    }

    /// Ship one event through whichever transport is configured. Best-effort:
    /// failures log to stderr and never propagate.
    fn emit(&self, ev: RunEvent) {
        match &self.inner {
            None => {}
            Some(Inner::Pg { handle, store, .. }) => {
                let fut = apply_run_event(store, &self.run_id, &ev);
                if let Err(e) = handle.block_on(fut) {
                    eprintln!("lifecycle[store]: write failed for {}: {e}", self.run_id);
                }
            }
            Some(Inner::Http {
                agent,
                base,
                token,
                actor,
                ..
            }) => {
                let url = format!("{base}/api/v1/runs/{}/events", self.run_id);
                // V2: push-back is the run's ONLY progress/verdict channel out of
                // the pod — a dropped Finish or State loses the run's terminal
                // status. Retry TRANSIENT failures (connection drops, 5xx) with
                // backoff; the ingest is idempotent (terminal-guarded), so a
                // duplicated delivery is a no-op. A 4xx is a permanent reject
                // (bad request / auth / unknown run) — never retried.
                const ATTEMPTS: u32 = 3;
                for attempt in 1..=ATTEMPTS {
                    let mut req = agent.post(&url).set("x-deja-actor", actor);
                    if let Some(token) = token {
                        req = req.set("authorization", &format!("Bearer {token}"));
                    }
                    match req.send_json(&ev) {
                        Ok(_) => break,
                        Err(ureq::Error::Status(code, _)) if code < 500 => {
                            eprintln!(
                                "lifecycle[push-back]: {url} rejected {code} (permanent, not retrying)"
                            );
                            break;
                        }
                        Err(e) if attempt < ATTEMPTS => {
                            let backoff =
                                std::time::Duration::from_millis(150 * u64::from(attempt));
                            eprintln!(
                                "lifecycle[push-back]: {url} attempt {attempt}/{ATTEMPTS} \
                                 failed: {e}; retrying in {backoff:?}"
                            );
                            std::thread::sleep(backoff);
                        }
                        Err(e) => {
                            eprintln!(
                                "lifecycle[push-back]: {url} failed after {ATTEMPTS} attempts: {e}"
                            );
                        }
                    }
                }
            }
        }
    }

    fn next_log_seq(&self) -> i64 {
        match &self.inner {
            Some(Inner::Pg { log_seq, .. }) | Some(Inner::Http { log_seq, .. }) => {
                log_seq.fetch_add(1, Ordering::Relaxed)
            }
            None => 0,
        }
    }

    /// Append a worker log line (also echoed to stderr by the caller).
    pub fn log(&self, stage: &str, line: &str) {
        if self.inner.is_none() {
            return;
        }
        let seq = self.next_log_seq();
        self.emit(RunEvent::Log {
            stage: stage.to_owned(),
            seq,
            line: line.to_owned(),
        });
    }

    /// Record a stage transition (closes the previous running stage as ok).
    pub fn stage(&self, stage: &str, step: u32, total: u32) {
        self.emit(RunEvent::Stage {
            stage: stage.to_owned(),
            step,
            total,
        });
    }

    /// Terminal state: close the running stage and update the run row.
    pub fn finish(&self, ok: bool, failure: Option<&str>) {
        self.emit(RunEvent::Finish {
            ok,
            failure: failure.map(str::to_owned),
        });
    }

    pub fn run_state(&self, state: &str) {
        self.emit(RunEvent::State {
            state: state.to_owned(),
        });
    }

    pub fn run_recording(&self, recording_id: &str) {
        self.emit(RunEvent::Recording {
            recording_id: recording_id.to_owned(),
        });
    }

    pub fn candidate_sha(&self, sha256: &str) {
        self.emit(RunEvent::CandidateSha {
            sha256: sha256.to_owned(),
        });
    }

    pub fn result(&self, verdict: Option<&str>, scorecard: Option<&serde_json::Value>) {
        self.emit(RunEvent::Result {
            verdict: verdict.map(str::to_owned),
            scorecard: scorecard.cloned(),
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
        self.emit(RunEvent::CatalogUpsert {
            recording_id: recording_id.to_owned(),
            source_path: source_path.map(str::to_owned),
            event_count,
            correlation_count,
            bytes,
            manifest: manifest.cloned(),
        });
    }

    /// Register an artifact for this run (and optionally a recording).
    pub fn artifact(&self, recording_id: Option<&str>, kind: &str, path: &std::path::Path) {
        let meta = std::fs::metadata(path).ok();
        let Some(meta) = meta else {
            return; // artifact absent — nothing to register
        };
        self.emit(RunEvent::Artifact {
            recording_id: recording_id.map(str::to_owned),
            kind: kind.to_owned(),
            uri: path.display().to_string(),
            bytes: Some(meta.len() as i64),
        });
    }

    /// Register an artifact by an ALREADY-RESOLVED uri — e.g. the `s3://` object
    /// an in-pod [`ArtifactSink`](super::ArtifactSink) uploaded. Unlike
    /// [`Self::artifact`], it does not stat a local file (there may be none on
    /// this host); the caller supplies the byte count.
    pub fn artifact_uri(
        &self,
        recording_id: Option<&str>,
        kind: &str,
        uri: &str,
        bytes: Option<i64>,
    ) {
        self.emit(RunEvent::Artifact {
            recording_id: recording_id.map(str::to_owned),
            kind: kind.to_owned(),
            uri: uri.to_owned(),
            bytes,
        });
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;

    #[test]
    fn run_event_wire_shape_is_tagged_snake_case() {
        let ev = RunEvent::Stage {
            stage: "seeding".to_owned(),
            step: 5,
            total: 6,
        };
        let json = serde_json::to_value(&ev).unwrap();
        assert_eq!(
            json,
            serde_json::json!({"event": "stage", "stage": "seeding", "step": 5, "total": 6})
        );
        let back: RunEvent = serde_json::from_value(json).unwrap();
        assert!(matches!(back, RunEvent::Stage { step: 5, total: 6, .. }));
    }

    #[test]
    fn log_seq_is_sender_assigned_and_monotonic() {
        // Http variant against a port nobody listens on: emits fail (logged,
        // best-effort) but the sender-side sequence must still advance.
        let ctx = StoreCtx::http("run-x", "http://127.0.0.1:1", None, "system:test");
        assert_eq!(ctx.next_log_seq(), 0);
        assert_eq!(ctx.next_log_seq(), 1);
        assert_eq!(ctx.next_log_seq(), 2);
    }
}
