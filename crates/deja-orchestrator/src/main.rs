//! Replay-harness API service entry.
//!
//! axum server hosting one API surface (`/api/v1`) plus the embedded
//! dashboard SPA. Nothing lives outside `/api/v1`, so the SPA owns the whole
//! page URL space via the fallback — no content negotiation anywhere.
//!
//!   GET  /api/v1/healthz                  → liveness
//!   GET  /api/v1/recordings               → recordings catalog
//!   POST /api/v1/runs                     → create a run (spawns the worker)
//!   GET  /api/v1/runs                     → run list
//!   POST /api/v1/runs/{id}/events         → push-back ingest (out-of-process runner)
//!   GET  /api/v1/runs/{id}                → store row + live worker snapshot
//!   GET  /api/v1/runs/{id}/stages         → stage history
//!   GET  /api/v1/runs/{id}/logs           → persisted worker logs
//!   GET  /api/v1/runs/{id}/artifacts      → registered artifacts
//!   GET  /api/v1/runs/{id}/scorecard      → divergence scorecard
//!   GET  /api/v1/runs/{id}/stream         → SSE run progress
//!   GET  /api/v1/artifacts/{id}/raw       → stream an artifact file
//!   GET  /api/v1/audit                    → append-only audit log
//!
//! The lifecycle worker (compose up → record/replay → score → tear down) is
//! spawned per run by the create handler; this binary hosts the API and
//! persists/serves run state.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::{
    extract::{Extension, Path, State},
    http::{header, HeaderMap, Request, StatusCode},
    middleware::{self, Next},
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    routing::{get, post},
    Router,
};
use deja_orchestrator::executor::{ExecutorKind, InClusterConfig, K8sExecutorConfig};
use deja_orchestrator::{api::runs, divergence, HarnessRoot, Run, RunStatus};
use deja_store::Store;
use sha2::{Digest, Sha256};

/// The built dashboard SPA (web/dist), embedded at compile time so the
/// orchestrator stays a single deployable binary. `npm run build` in web/
/// refreshes it; the dist is committed so cargo builds never need node.
#[derive(rust_embed::RustEmbed)]
#[folder = "../../web/dist"]
struct WebAssets;

#[derive(Clone)]
struct AppState {
    root: Arc<HarnessRoot>,
    store: Option<Arc<Store>>,
    mutation_auth: MutationAuth,
    executor: Arc<ExecutorSelection>,
}

/// Which executor drives runs, resolved ONCE at startup. K8s carries the
/// in-cluster access + the Job/template coordinates (all from env). Arc-wrapped
/// in `AppState` so per-request clones don't copy the CA bundle; the K8s payload
/// is boxed so the enum stays small.
enum ExecutorSelection {
    Compose,
    K8s(Box<K8sExecutor>),
}

struct K8sExecutor {
    incluster: InClusterConfig,
    cfg: K8sExecutorConfig,
}

impl ExecutorSelection {
    /// Resolve from `DEJA_EXECUTOR`. For k8s, the in-cluster config + Job
    /// coordinates are read now and any failure is fatal — better to refuse to
    /// start than to silently fall back to the compose executor in a cluster.
    fn from_env() -> Result<Self, String> {
        match ExecutorKind::from_env().map_err(|e| e.to_string())? {
            ExecutorKind::Compose => Ok(ExecutorSelection::Compose),
            ExecutorKind::K8s => {
                let incluster = InClusterConfig::from_env().map_err(|e| e.to_string())?;
                let cfg = K8sExecutorConfig::from_env();
                Ok(ExecutorSelection::K8s(Box::new(K8sExecutor {
                    incluster,
                    cfg,
                })))
            }
        }
    }
}

#[derive(Clone)]
struct MutationAuth {
    service_token: Option<Arc<str>>,
}

impl MutationAuth {
    fn from_env() -> Self {
        let service_token = std::env::var("DEJA_API_SERVICE_TOKEN")
            .ok()
            .map(|token| token.trim().to_owned())
            .filter(|token| !token.is_empty())
            .map(Arc::<str>::from);
        Self { service_token }
    }
}

#[derive(Clone, Debug)]
struct AuthenticatedActor(String);

#[tokio::main]
async fn main() {
    // rustls 0.23 refuses to auto-select a CryptoProvider when both aws-lc-rs
    // and ring are in the dependency tree (they are, transitively). The k8s
    // executor's apiserver client (UreqTransport) builds a rustls ClientConfig,
    // which panics without a process-level provider — install one explicitly.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let bind_addr = std::env::var("HARNESS_BIND").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let root_dir =
        std::env::var("HARNESS_STATE_DIR").unwrap_or_else(|_| "./harness-state".to_string());
    let root = match HarnessRoot::new(&root_dir) {
        Ok(r) => Arc::new(r),
        Err(err) => {
            eprintln!("deja-orchestrator: HARNESS_STATE_DIR setup failed: {err}");
            std::process::exit(1);
        }
    };
    // Optional Postgres store: dashboard state, stage history, audit. Runs
    // still execute without it (file-backed worker state); store-backed
    // surfaces return 503 until it is up (demo/lib.sh boots the orchestrator
    // pg).
    let db_url =
        std::env::var("DEJA_DB_URL").unwrap_or_else(|_| deja_store::DEFAULT_DB_URL.to_string());
    let store = match Store::connect(&db_url).await {
        Ok(s) => {
            eprintln!("deja-orchestrator: store connected + migrated ({db_url})");
            Some(Arc::new(s))
        }
        Err(err) => {
            eprintln!(
                "deja-orchestrator: store unavailable ({db_url}): {err} — running file-only; \
                 start it with: docker compose -p deja-orchestrator -f demo/docker-compose.orchestrator.yml up -d"
            );
            None
        }
    };
    let executor = match ExecutorSelection::from_env() {
        Ok(e) => {
            match &e {
                ExecutorSelection::Compose => eprintln!("deja-orchestrator: executor = compose"),
                ExecutorSelection::K8s(k) => eprintln!(
                    "deja-orchestrator: executor = k8s (jobs ns {}, template {}/{})",
                    k.cfg.jobs_namespace, k.cfg.template_namespace, k.cfg.template_configmap
                ),
            }
            Arc::new(e)
        }
        Err(err) => {
            eprintln!("deja-orchestrator: executor config failed: {err}");
            std::process::exit(1);
        }
    };
    let state = AppState {
        root: root.clone(),
        store,
        mutation_auth: MutationAuth::from_env(),
        executor,
    };

    // Restart-durable reconciler (#34 V3/V7). The per-launch watcher in
    // `spawn_k8s_run` is lost if this process restarts, leaving its run hung in
    // a non-terminal state. When the executor is k8s, run a background loop that
    // re-derives each non-terminal run's verdict from its Job and settles it
    // (idempotent via the store's terminal guard). It needs the store as the run
    // registry — without one there is nothing to reconcile, so log and skip.
    if let ExecutorSelection::K8s(k) = &*state.executor {
        match &state.store {
            Some(store) => deja_orchestrator::executor::reconcile::spawn(
                store.clone(),
                k.incluster.clone(),
                k.cfg.clone(),
            ),
            None => eprintln!(
                "deja-orchestrator: k8s reconciler disabled — no store (the reconciler needs \
                 the run registry to know which runs to settle)"
            ),
        }
    }

    let app = app_router(state);

    let listener = match tokio::net::TcpListener::bind(&bind_addr).await {
        Ok(l) => l,
        Err(err) => {
            eprintln!("deja-orchestrator: bind {bind_addr} failed: {err}");
            std::process::exit(1);
        }
    };
    eprintln!(
        "deja-orchestrator: listening on http://{bind_addr} (state: {})",
        root.root.display()
    );

    if let Err(err) = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
    {
        eprintln!("deja-orchestrator: server error: {err}");
        std::process::exit(1);
    }
}

fn app_router(state: AppState) -> Router {
    let create_run = post(v1_create_run).route_layer(middleware::from_fn_with_state(
        state.mutation_auth.clone(),
        require_mutation_auth,
    ));
    // Push-back ingest: an out-of-process lifecycle runner (the k8s Job)
    // reports RunEvents here; same mutation boundary as run creation.
    let ingest_run_event = post(v1_ingest_run_event).route_layer(middleware::from_fn_with_state(
        state.mutation_auth.clone(),
        require_mutation_auth,
    ));

    let api_v1 = Router::new()
        .route("/healthz", get(healthz))
        .route("/recordings", get(v1_list_recordings))
        .route("/runs", create_run.get(v1_list_runs))
        .route("/runs/{run_id}/events", ingest_run_event)
        .route("/runs/{run_id}", get(v1_get_run))
        .route("/runs/{run_id}/stages", get(v1_run_stages))
        .route("/runs/{run_id}/logs", get(v1_run_logs))
        .route("/runs/{run_id}/artifacts", get(v1_run_artifacts))
        .route("/runs/{run_id}/scorecard", get(v1_scorecard))
        .route("/runs/{run_id}/calls", get(v1_calls))
        .route("/runs/{run_id}/http-diffs", get(v1_http_diffs))
        .route("/runs/{run_id}/graph", get(v1_graph))
        .route("/runs/{run_id}/stream", get(run_stream))
        .route("/artifacts/{id}/raw", get(v1_artifact_raw))
        .route("/audit", get(v1_audit));

    Router::new()
        .nest("/api/v1", api_v1)
        // SPA: real assets by path; any other GET falls back to index.html
        // (client-side routing). The API is entirely under /api/v1, so the
        // page URL space (/runs/..., /recordings, ...) is the SPA's alone.
        .fallback(get(spa_fallback))
        .with_state(state)
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    // k8s terminates pods with SIGTERM, not SIGINT — awaiting only ctrl_c means
    // graceful shutdown never fires in-cluster (the pod is SIGKILLed at the end
    // of its grace period instead, cutting any in-flight push-back ingest). Wait
    // on both. (V5)
    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            // If the handler can't be installed, never resolve this arm so
            // ctrl_c still governs shutdown rather than shutting down at once.
            Err(e) => {
                eprintln!("deja-orchestrator: cannot install SIGTERM handler: {e}");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
    eprintln!("deja-orchestrator: shutting down");
}

// ---------------------------------------------------------------------------
// /api/v1 handlers
// ---------------------------------------------------------------------------

async fn healthz() -> Response {
    json_ok(serde_json::json!({ "status": "ok" }))
}

/// Shorthand: the Postgres store, or a 503 telling the operator how to start it.
#[allow(clippy::result_large_err)] // the Err is an axum Response; cold path
fn require_store(st: &AppState) -> Result<Arc<Store>, Response> {
    st.store.clone().ok_or_else(|| {
        error_resp(
            503,
            "store unavailable — start it: docker compose -p deja-orchestrator -f demo/docker-compose.orchestrator.yml up -d",
        )
    })
}

async fn require_mutation_auth(
    State(auth): State<MutationAuth>,
    mut req: Request<axum::body::Body>,
    next: Next,
) -> Response {
    let Some(actor) = actor_from_headers(req.headers()) else {
        return error_resp(401, "X-Deja-Actor header required for mutating requests");
    };

    if let Some(expected) = auth.service_token.as_deref() {
        let Some(supplied) = bearer_token(req.headers()) else {
            return error_resp(401, "Authorization: Bearer token required");
        };
        if !service_token_matches(expected, supplied) {
            return error_resp(401, "invalid bearer token");
        }
    }

    req.extensions_mut().insert(AuthenticatedActor(actor));
    next.run(req).await
}

fn actor_from_headers(headers: &HeaderMap) -> Option<String> {
    headers
        .get("x-deja-actor")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|actor| !actor.is_empty())
        .map(str::to_owned)
}

fn bearer_token(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|token| !token.is_empty())
}

fn service_token_matches(expected: &str, supplied: &str) -> bool {
    let expected_digest = Sha256::digest(expected.as_bytes());
    let supplied_digest = Sha256::digest(supplied.as_bytes());
    expected_digest
        .iter()
        .zip(supplied_digest.iter())
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right))
        == 0
}

/// `POST /api/v1/runs` — create a run and spawn its lifecycle worker.
///
/// Mutating requests reach this handler only after `require_mutation_auth`
/// resolved an `AuthenticatedActor`: local/dev supplies `X-Deja-Actor`, and
/// hosted sandboxes additionally set `DEJA_API_SERVICE_TOKEN` so the middleware
/// requires a matching bearer token before audit/store mutation.
async fn v1_create_run(
    State(st): State<AppState>,
    Extension(actor): Extension<AuthenticatedActor>,
    body: axum::body::Bytes,
) -> Response {
    let actor = actor.0;
    let raw: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return error_resp(400, &format!("parse RunSpec: {e}")),
    };
    let expectation = raw
        .get("expectation")
        .and_then(|v| v.as_str())
        .map(str::to_owned);
    let spec: deja_orchestrator::RunSpec = match serde_json::from_value(raw) {
        Ok(s) => s,
        Err(e) => return error_resp(400, &format!("parse RunSpec: {e}")),
    };
    let run = match runs::persist_new(&st.root, spec) {
        Ok(run) => run,
        Err(e) => return error_resp(500, &format!("create run: {e}")),
    };
    // Store row + audit BEFORE the worker spawns (stage rows FK the run row).
    let ctx = if let Some(store) = &st.store {
        let candidate = serde_json::to_value(&run.spec.candidate_spec).unwrap_or_default();
        let params = serde_json::json!({
            "workload": run.spec.workload,
        });
        if let Err(e) = store
            .insert_run(
                &run.run_id,
                runs::mode_str(run.spec.mode),
                run.spec.recording_id.as_deref(),
                &candidate,
                &params,
                expectation.as_deref(),
                &actor,
            )
            .await
        {
            eprintln!("deja-orchestrator: store insert_run failed: {e}");
        }
        let _ = store
            .audit(
                &actor,
                "run.create",
                "run",
                &run.run_id,
                &serde_json::json!({ "spec": run.spec, "expectation": expectation }),
            )
            .await;
        deja_orchestrator::lifecycle::StoreCtx::new(
            &run.run_id,
            Some((tokio::runtime::Handle::current(), store.clone())),
        )
    } else {
        deja_orchestrator::lifecycle::StoreCtx::disabled(&run.run_id)
    };
    match &*st.executor {
        ExecutorSelection::Compose => runs::spawn_worker(&st.root, &run.run_id, ctx),
        ExecutorSelection::K8s(k) => {
            runs::spawn_k8s_run(&st.root, run.clone(), ctx, k.incluster.clone(), k.cfg.clone())
        }
    }
    json_ok(
        serde_json::to_value(&runs::CreateRunResponse {
            run_id: run.run_id,
            status: run.status,
        })
        .unwrap_or_default(),
    )
}

/// `POST /api/v1/runs/{run_id}/events` — push-back ingest for an
/// out-of-process lifecycle runner (the k8s Job). The event is mirrored into
/// the file-backed run record (so `GET /runs/{id}` and the SSE stream see it
/// even store-less) and applied to the Postgres store through the SAME
/// mapping the in-process worker uses. Store failures are best-effort (logged,
/// 202 regardless) — matching the in-process transport's semantics.
async fn v1_ingest_run_event(
    State(st): State<AppState>,
    Path(run_id): Path<String>,
    body: axum::body::Bytes,
) -> Response {
    use deja_orchestrator::lifecycle::store_ctx::{apply_run_event, RunEvent};

    let ev: RunEvent = match serde_json::from_slice(&body) {
        Ok(ev) => ev,
        Err(e) => return error_resp(400, &format!("parse RunEvent: {e}")),
    };

    // File-side mirror: the run record must exist (the orchestrator created it
    // before launching the Job) — an unknown id is a 404, not an upsert.
    let run_path = st.root.run_path(&run_id);
    let mut run: Run = match deja_orchestrator::read_json(&run_path) {
        Ok(run) => run,
        Err(_) => return error_resp(404, &format!("unknown run {run_id}")),
    };

    // V4 terminal-guard: push-back is at-least-once and may reorder. A terminal
    // status (Completed/Failed) is the settled verdict — the FIRST one wins. A
    // stale `state`/`stage` delivered afterwards must not resurrect the run, and
    // a second, conflicting `finish` must not flip the verdict. Drop such events
    // before they touch either the file mirror or the store (accepted, ignored).
    if matches!(run.status, RunStatus::Completed | RunStatus::Failed) {
        match &ev {
            RunEvent::Stage { .. } | RunEvent::State { .. } => {
                eprintln!(
                    "deja-orchestrator: dropping post-terminal progress event for {run_id} \
                     (settled {:?})",
                    run.status
                );
                return StatusCode::ACCEPTED.into_response();
            }
            RunEvent::Finish { ok, .. } => {
                let incoming = if *ok {
                    RunStatus::Completed
                } else {
                    RunStatus::Failed
                };
                if incoming != run.status {
                    eprintln!(
                        "deja-orchestrator: conflicting finish for {run_id}: keeping settled \
                         {:?}, ignoring {incoming:?}",
                        run.status
                    );
                }
                return StatusCode::ACCEPTED.into_response();
            }
            // Recording/Log/Result/Artifact after terminal are harmless — a
            // trailing artifact or log line still belongs to this run.
            _ => {}
        }
    }

    let file_side_changed = match &ev {
        RunEvent::Stage { stage, step, total } => {
            run.stage = Some(stage.clone());
            run.step = *step;
            run.steps_total = *total;
            run.stage_updated_ms = deja_orchestrator::now_ms();
            true
        }
        RunEvent::State { state } => {
            match serde_json::from_value::<RunStatus>(serde_json::json!(state)) {
                Ok(status) => {
                    run.status = status;
                    true
                }
                Err(_) => return error_resp(400, &format!("unknown run state '{state}'")),
            }
        }
        RunEvent::Finish { ok, failure } => {
            run.status = if *ok {
                RunStatus::Completed
            } else {
                RunStatus::Failed
            };
            run.failure_reason = failure.clone();
            run.stage_updated_ms = deja_orchestrator::now_ms();
            true
        }
        RunEvent::Recording { recording_id } => {
            run.recording_id = Some(recording_id.clone());
            true
        }
        // Log/CandidateSha/Result/CatalogUpsert/Artifact live in the store only.
        _ => false,
    };
    if file_side_changed {
        if let Err(e) = deja_orchestrator::write_json(&run_path, &run) {
            return error_resp(500, &format!("persist run: {e}"));
        }
    }

    if let Some(store) = &st.store {
        if let Err(e) = apply_run_event(store, &run_id, &ev).await {
            eprintln!("deja-orchestrator: run-event store write failed for {run_id}: {e}");
        }
    }
    StatusCode::ACCEPTED.into_response()
}

/// `GET /api/v1/recordings` — the recordings catalog (Postgres-backed).
async fn v1_list_recordings(State(st): State<AppState>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_recordings(200).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("list recordings: {e}")),
    }
}

/// `GET /api/v1/runs` — run list (Postgres-backed; newest first).
async fn v1_list_runs(State(st): State<AppState>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_runs(200).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("list runs: {e}")),
    }
}

/// The worker's live file-store snapshot as the `live` sub-object.
fn live_json(live: &Run) -> serde_json::Value {
    serde_json::json!({
        "status": live.status,
        "stage": live.stage,
        "step": live.step,
        "steps_total": live.steps_total,
        "stage_updated_ms": live.stage_updated_ms,
        "failure_reason": live.failure_reason,
        "candidate_image": live.candidate_image,
    })
}

/// `GET /api/v1/runs/{id}` — store row + live file-store snapshot merged: the
/// row carries dashboard fields (verdict, expectation, candidate sha, actor),
/// the snapshot carries the worker's live stage/step (file store is the
/// worker's source of truth mid-run). Degrades to the snapshot alone when the
/// store is down, so script polling works file-only too.
async fn v1_get_run(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let row = match &st.store {
        Some(store) => match store.get_run(&id).await {
            Ok(row) => row,
            Err(e) => return error_resp(500, &format!("get run: {e}")),
        },
        None => None,
    };
    let live = runs::get(&st.root, &id).ok();
    let mut body = match (row, &live) {
        (Some(row), _) => serde_json::to_value(&row).unwrap_or_default(),
        (None, Some(live)) => serde_json::json!({
            "run_id": live.run_id,
            "state": format!("{:?}", live.status).to_lowercase(),
            "recording_id": live.recording_id,
        }),
        (None, None) => return error_resp(404, "run not found"),
    };
    if let Some(live) = &live {
        body["live"] = live_json(live);
    }
    json_ok(body)
}

/// The orchestrator-local path a hydrated artifact of `kind` belongs at (the
/// path the detail endpoints read). None for kinds not served from a file.
fn local_path_for_artifact_kind(
    root: &HarnessRoot,
    run_id: &str,
    kind: &str,
) -> Option<std::path::PathBuf> {
    Some(match kind {
        "observed" => root.observed_path(run_id),
        "http_diffs" => root.http_diff_path(run_id),
        "lookup_table" => root.lookup_table_path(run_id),
        "scorecard" => root.scorecard_path(run_id),
        "call_ledger" => root.call_ledger_path(run_id),
        "record_graph" => root.record_graph_path(run_id),
        _ => return None,
    })
}

/// Pull a run's `s3://` artifacts down to the local paths the detail endpoints
/// read (idempotent — a path already present is left alone). k8s runs publish
/// artifacts to S3 (the pod is ephemeral); this makes them readable on the
/// orchestrator. Best-effort: a missing/failed artifact just leaves that view
/// empty, never errors the request. No-op for compose runs — their artifacts are
/// already local and their URIs are filesystem paths, not `s3://`.
async fn hydrate_run_artifacts(st: &AppState, run_id: &str) {
    let Some(store) = st.store.clone() else {
        return;
    };
    let Ok(arts) = store.list_artifacts(run_id).await else {
        return;
    };
    let root = st.root.clone();
    let run_id = run_id.to_owned();
    // object_store's sync API blocks on its own runtime — run it off the async
    // worker so we never nest block_on inside tokio.
    let _ = tokio::task::spawn_blocking(move || {
        for art in arts {
            let Some(local) = local_path_for_artifact_kind(&root, &run_id, &art.kind) else {
                continue;
            };
            if local.exists() {
                continue; // cached from an earlier view
            }
            let Ok((bucket, key)) = deja_orchestrator::codebundle::parse_s3_uri(&art.uri) else {
                continue; // not an s3:// uri (compose local path) — nothing to pull
            };
            let mut cfg = deja_orchestrator::s3::S3Config::from_env();
            cfg.bucket = bucket;
            match deja_compactor::get_object_decoded(&cfg, &key) {
                Ok(bytes) => {
                    if let Some(parent) = local.parent() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                    if let Err(e) = std::fs::write(&local, bytes) {
                        eprintln!("hydrate: write {}: {e}", local.display());
                    }
                }
                Err(e) => eprintln!("hydrate: {} <- {}: {e}", local.display(), art.uri),
            }
        }
    })
    .await;
}

/// `GET /api/v1/runs/{id}/scorecard` — serve the divergence scorecard. Prefers
/// the runner's PRECOMPUTED scorecard (a k8s recompute would need the recording,
/// which isn't on the orchestrator); falls back to recomputing for compose.
async fn v1_scorecard(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    hydrate_run_artifacts(&st, &id).await;
    if let Ok(content) = std::fs::read_to_string(st.root.scorecard_path(&id)) {
        if let Ok(card) = serde_json::from_str::<serde_json::Value>(&content) {
            return json_ok(card);
        }
    }
    match divergence::scorecard(&st.root, &id) {
        Ok(card) => json_ok(serde_json::to_value(&card).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("scorecard: {e}")),
    }
}

/// `GET /api/v1/runs/{id}/calls` — the per-call divergence ledger (recorded vs
/// observed, classified + located) that backs the interactive diff view. Prefers
/// the runner's PRECOMPUTED ledger (a recompute needs the recording, absent on
/// the orchestrator for k8s runs); falls back to recomputing for compose.
async fn v1_calls(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    hydrate_run_artifacts(&st, &id).await;
    if let Ok(content) = std::fs::read_to_string(st.root.call_ledger_path(&id)) {
        let rows: Vec<serde_json::Value> = content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        if !rows.is_empty() {
            return json_ok(serde_json::Value::Array(rows));
        }
    }
    match divergence::call_ledger(&st.root, &id) {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("call ledger: {e}")),
    }
}

/// `GET /api/v1/runs/{id}/http-diffs` — the kernel's per-request HTTP diffs
/// (status + field-level body diff), parsed from the run's http-diff stream.
async fn v1_http_diffs(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    hydrate_run_artifacts(&st, &id).await;
    let rows: Vec<serde_json::Value> = std::fs::read_to_string(st.root.http_diff_path(&id))
        .map(|c| {
            c.lines()
                .filter(|l| !l.trim().is_empty())
                .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
                .collect()
        })
        .unwrap_or_default();
    json_ok(serde_json::Value::Array(rows))
}

/// `GET /api/v1/runs/{id}/graph` — the record-side and replay-side execution
/// graphs (raw nodes) for the cascade/tree view. The UI builds the tree from
/// node_id/parent_id and hangs boundary events off nodes via graph_node_id
/// (recorded events + the call ledger's observed side). Graph nodes ride the
/// shared `DejaRecord` stream: record-side in the recording tape, replay-side
/// in the run's observed stream.
async fn v1_graph(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    // k8s: the replay-side observed stream AND the record-side graph nodes both
    // ride S3 artifacts — hydrate pulls them to their local paths. The record
    // side comes from the `record_graph` artifact (span STRUCTURE only, extracted
    // in-pod by the runner) so the sensitive recording tape never reaches the
    // orchestrator; compose runs also produce it, but fall back to the local
    // recording tape for legacy runs that predate the artifact.
    hydrate_run_artifacts(&st, &id).await;
    let read_nodes = |path: std::path::PathBuf| -> Vec<serde_json::Value> {
        let Ok(file) = std::fs::File::open(&path) else {
            return Vec::new();
        };
        std::io::BufRead::lines(std::io::BufReader::new(file))
            .map_while(Result::ok)
            .filter(|line| !line.trim().is_empty())
            .filter_map(
                |line| match serde_json::from_str::<deja::DejaRecord>(&line) {
                    Ok(deja::DejaRecord::GraphNode(node)) => serde_json::to_value(node).ok(),
                    Ok(deja::DejaRecord::BoundaryEvent(_) | deja::DejaRecord::Observed(_)) => None,
                    Err(_) => None,
                },
            )
            .collect()
    };
    // Prefer the `record_graph` artifact (present for k8s post-hydrate and for
    // compose runs); fall back to the local recording tape (older runs / compose
    // before this artifact existed). recording_id comes from the run record.
    let mut record = read_nodes(st.root.record_graph_path(&id));
    if record.is_empty() {
        if let Some(rec) = runs::get(&st.root, &id)
            .ok()
            .and_then(|r| r.recording_id.or(r.spec.recording_id))
        {
            record = read_nodes(st.root.recording_events_path(&rec));
        }
    }
    let replay = read_nodes(st.root.observed_path(&id));
    json_ok(serde_json::json!({ "record": record, "replay": replay }))
}

/// `GET /api/v1/runs/{id}/stages` — append-only stage history.
async fn v1_run_stages(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_stages(&id).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("list stages: {e}")),
    }
}

#[derive(serde::Deserialize)]
struct LogsQuery {
    stage: Option<String>,
    #[serde(default)]
    after_seq: i64,
}

/// `GET /api/v1/runs/{id}/logs?stage=&after_seq=` — persisted worker logs.
async fn v1_run_logs(
    State(st): State<AppState>,
    Path(id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<LogsQuery>,
) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_logs(&id, q.stage.as_deref(), q.after_seq).await {
        Ok(rows) => {
            let body: Vec<serde_json::Value> = rows
                .into_iter()
                .map(|(stage, seq, lines)| {
                    serde_json::json!({ "stage": stage, "seq": seq, "lines": lines })
                })
                .collect();
            json_ok(serde_json::Value::Array(body))
        }
        Err(e) => error_resp(500, &format!("list logs: {e}")),
    }
}

/// `GET /api/v1/runs/{id}/artifacts` — registered artifacts for a run.
async fn v1_run_artifacts(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_artifacts(&id).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("list artifacts: {e}")),
    }
}

/// `GET /api/v1/artifacts/{id}/raw` — stream a registered artifact file.
/// HTML renders inline (the embedded visualization); JSONL downloads as ndjson.
async fn v1_artifact_raw(State(st): State<AppState>, Path(id): Path<i64>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    let art = match store.get_artifact(id).await {
        Ok(Some(a)) => a,
        Ok(None) => return error_resp(404, "artifact not found"),
        Err(e) => return error_resp(500, &format!("get artifact: {e}")),
    };
    let content_type = if art.kind == "visualization_html" {
        "text/html; charset=utf-8"
    } else if art.uri.ends_with(".json") {
        "application/json"
    } else {
        "application/x-ndjson"
    };
    // s3:// artifact (k8s run) → fetch from S3; else a local path (compose run).
    let bytes = if let Ok((bucket, key)) = deja_orchestrator::codebundle::parse_s3_uri(&art.uri) {
        let fetch = tokio::task::spawn_blocking(move || {
            let mut cfg = deja_orchestrator::s3::S3Config::from_env();
            cfg.bucket = bucket;
            deja_compactor::get_object_decoded(&cfg, &key)
        })
        .await;
        match fetch {
            Ok(Ok(b)) => b,
            Ok(Err(e)) => return error_resp(502, &format!("artifact fetch from s3: {e}")),
            Err(e) => return error_resp(500, &format!("artifact fetch task: {e}")),
        }
    } else {
        match std::fs::read(&art.uri) {
            Ok(b) => b,
            Err(e) => return error_resp(404, &format!("artifact file unreadable: {e}")),
        }
    };
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, content_type)],
        bytes,
    )
        .into_response()
}

/// `GET /api/v1/audit` — the append-only audit log (newest first).
async fn v1_audit(State(st): State<AppState>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.audit_list(500).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("audit list: {e}")),
    }
}

/// `GET /api/v1/runs/{id}/stream` — SSE run progress.
///
/// Emits a `run` event with the full run snapshot whenever it changes, then a
/// terminal `done` event once the run reaches a terminal status. Implemented
/// as a store poll (500ms) so it is backend-agnostic: identical behavior over
/// the file store today and the Postgres store later (which can tighten it to
/// LISTEN/NOTIFY wake-ups without changing the wire contract).
async fn run_stream(
    State(st): State<AppState>,
    Path(run_id): Path<String>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let stream = async_stream::stream! {
        let mut last: Option<String> = None;
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            let run: Run = match runs::get(&st.root, &run_id) {
                Ok(r) => r,
                Err(_) => {
                    yield Ok(Event::default().event("error").data(
                        serde_json::json!({ "error": "run not found" }).to_string(),
                    ));
                    break;
                }
            };
            let snapshot = serde_json::to_string(&run).unwrap_or_default();
            if last.as_deref() != Some(snapshot.as_str()) {
                last = Some(snapshot.clone());
                yield Ok(Event::default().event("run").data(snapshot));
            }
            if matches!(run.status, RunStatus::Completed | RunStatus::Failed) {
                yield Ok(Event::default().event("done").data(
                    serde_json::json!({ "status": run.status }).to_string(),
                ));
                break;
            }
        }
    };
    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ka"),
    )
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn json_ok(value: serde_json::Value) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_vec(&value).unwrap_or_default(),
    )
        .into_response()
}

fn error_resp(status: u16, msg: &str) -> Response {
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::json!({ "error": msg }).to_string(),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Embedded dashboard
// ---------------------------------------------------------------------------

async fn spa_fallback(uri: axum::http::Uri) -> Response {
    let path = uri.path().trim_start_matches('/');
    let (name, asset) = match WebAssets::get(path) {
        Some(a) if !path.is_empty() => (path, a),
        _ => match WebAssets::get("index.html") {
            Some(a) => ("index.html", a),
            None => return error_resp(404, "dashboard not built"),
        },
    };
    let mime = mime_guess::from_path(name).first_or_octet_stream();
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, mime.as_ref().to_owned())],
        asset.data.into_owned(),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use axum::body::Body;
    use axum::http::{Method, Request, StatusCode};
    use tower::ServiceExt;

    async fn ok(Extension(actor): Extension<AuthenticatedActor>) -> String {
        actor.0
    }

    async fn read_ok() -> &'static str {
        "read-ok"
    }

    fn protected_router(auth: MutationAuth) -> Router {
        let create_run =
            post(ok).route_layer(middleware::from_fn_with_state(auth, require_mutation_auth));
        Router::new().route("/runs", create_run.get(read_ok))
    }

    async fn request_status(
        auth: MutationAuth,
        method: Method,
        token: Option<&str>,
        actor: Option<&str>,
    ) -> StatusCode {
        let mut builder = Request::builder().method(method).uri("/runs");
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        if let Some(actor) = actor {
            builder = builder.header("X-Deja-Actor", actor);
        }
        protected_router(auth)
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    #[tokio::test]
    async fn auth_boundary_allows_dev_mutation_with_actor_when_no_token_configured() {
        let status = request_status(
            MutationAuth {
                service_token: None,
            },
            Method::POST,
            None,
            Some("local-dev"),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    #[tokio::test]
    async fn auth_boundary_denies_anonymous_mutation_even_without_token() {
        let status = request_status(
            MutationAuth {
                service_token: None,
            },
            Method::POST,
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn auth_boundary_requires_configured_service_token_for_mutation() {
        let auth = MutationAuth {
            service_token: Some(Arc::<str>::from("sandbox-secret")),
        };
        let missing = request_status(auth.clone(), Method::POST, None, Some("hosted-user")).await;
        let wrong = request_status(
            auth.clone(),
            Method::POST,
            Some("wrong"),
            Some("hosted-user"),
        )
        .await;
        let allowed = request_status(
            auth,
            Method::POST,
            Some("sandbox-secret"),
            Some("hosted-user"),
        )
        .await;

        assert_eq!(missing, StatusCode::UNAUTHORIZED);
        assert_eq!(wrong, StatusCode::UNAUTHORIZED);
        assert_eq!(allowed, StatusCode::OK);
    }

    #[tokio::test]
    async fn auth_boundary_does_not_gate_read_only_routes() {
        let status = request_status(
            MutationAuth {
                service_token: Some(Arc::<str>::from("sandbox-secret")),
            },
            Method::GET,
            None,
            None,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
    }

    fn test_state(dir: &std::path::Path) -> AppState {
        AppState {
            root: Arc::new(HarnessRoot::new(dir).unwrap()),
            store: None,
            mutation_auth: MutationAuth {
                service_token: None,
            },
            executor: Arc::new(ExecutorSelection::Compose),
        }
    }

    fn pending_run(run_id: &str) -> Run {
        Run {
            run_id: run_id.to_owned(),
            spec: deja_orchestrator::RunSpec {
                mode: deja_orchestrator::RunMode::Replay,
                candidate_spec: deja_orchestrator::CandidateSpec::PrebuiltImage {
                    image: "deja-demo".to_owned(),
                },
                candidate_repo: None,
                recording_id: Some("rec-1".to_owned()),
                s3_source: None,
                correlation_filter: None,
                workload: serde_json::Value::Null,
            },
            status: RunStatus::Pending,
            recording_id: None,
            candidate_image: None,
            failure_reason: None,
            stage: None,
            step: 0,
            steps_total: 0,
            stage_updated_ms: 0,
        }
    }

    async fn post_event(
        state: AppState,
        run_id: &str,
        body: serde_json::Value,
    ) -> StatusCode {
        let req = Request::builder()
            .method(Method::POST)
            .uri(format!("/api/v1/runs/{run_id}/events"))
            .header("X-Deja-Actor", "system:test-runner")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        app_router(state).oneshot(req).await.unwrap().status()
    }

    #[tokio::test]
    async fn ingest_mirrors_stage_and_finish_into_the_run_record() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let run = pending_run("run-ev");
        deja_orchestrator::write_json(&state.root.run_path("run-ev"), &run).unwrap();

        let status = post_event(
            state.clone(),
            "run-ev",
            serde_json::json!({"event": "stage", "stage": "seeding", "step": 5, "total": 6}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let run: Run = deja_orchestrator::read_json(&state.root.run_path("run-ev")).unwrap();
        assert_eq!(run.stage.as_deref(), Some("seeding"));
        assert_eq!((run.step, run.steps_total), (5, 6));
        assert!(run.stage_updated_ms > 0);

        let status = post_event(
            state.clone(),
            "run-ev",
            serde_json::json!({"event": "finish", "ok": false, "failure": "kernel failed"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
        let run: Run = deja_orchestrator::read_json(&state.root.run_path("run-ev")).unwrap();
        assert!(matches!(run.status, RunStatus::Failed));
        assert_eq!(run.failure_reason.as_deref(), Some("kernel failed"));
    }

    // V4: at-least-once push-back can reorder. Once a run is terminal, a stale
    // `state=running`, a late `stage`, and a conflicting `finish` must all be
    // accepted-but-ignored — the first terminal verdict is final.
    #[tokio::test]
    async fn ingest_terminal_guard_ignores_post_finish_events() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        deja_orchestrator::write_json(&state.root.run_path("run-t"), &pending_run("run-t")).unwrap();

        // Settle the run as Failed.
        let status = post_event(
            state.clone(),
            "run-t",
            serde_json::json!({"event": "finish", "ok": false, "failure": "kernel failed"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // A stale "still running" delivered after the finish — accepted, ignored.
        let status = post_event(
            state.clone(),
            "run-t",
            serde_json::json!({"event": "state", "state": "running"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // A late progress stage — accepted, ignored.
        let status = post_event(
            state.clone(),
            "run-t",
            serde_json::json!({"event": "stage", "stage": "seeding", "step": 4, "total": 6}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        // A conflicting finish (ok=true) — must NOT flip the settled Failed.
        let status = post_event(
            state.clone(),
            "run-t",
            serde_json::json!({"event": "finish", "ok": true}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);

        let run: Run = deja_orchestrator::read_json(&state.root.run_path("run-t")).unwrap();
        assert!(matches!(run.status, RunStatus::Failed), "terminal verdict is final");
        assert_eq!(run.failure_reason.as_deref(), Some("kernel failed"));
        // The dropped stage never touched progress.
        assert_ne!(run.stage.as_deref(), Some("seeding"));
    }

    #[tokio::test]
    async fn ingest_rejects_unknown_run_and_unknown_state() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        let status = post_event(
            state.clone(),
            "run-missing",
            serde_json::json!({"event": "state", "state": "running"}),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "unknown run must 404, not upsert");

        deja_orchestrator::write_json(&state.root.run_path("run-ev2"), &pending_run("run-ev2"))
            .unwrap();
        let status = post_event(
            state.clone(),
            "run-ev2",
            serde_json::json!({"event": "state", "state": "sideways"}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let status = post_event(
            state,
            "run-ev2",
            serde_json::json!({"event": "state", "state": "running"}),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED);
    }
}
