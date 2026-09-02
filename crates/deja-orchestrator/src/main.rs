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
//!   POST /api/v1/runs/{id}/kill           → stop a run, delete its Job + pods
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
    // Human create: audited via X-Deja-Actor, no service token (this endpoint is
    // internal-only, so reachability is the access boundary). The service secret
    // stays scoped to inter-service callbacks below.
    let create_run = post(v1_create_run).route_layer(middleware::from_fn(require_human_auth));
    // Killing a run is a human action like creating one: same auth, and audited.
    let kill_run_route = post(v1_kill_run).route_layer(middleware::from_fn(require_human_auth));
    // Push-back ingest: an out-of-process lifecycle runner (the k8s Job) reports
    // RunEvents here and authenticates with the service token (require_service_auth).
    let ingest_run_event = post(v1_ingest_run_event).route_layer(middleware::from_fn_with_state(
        state.mutation_auth.clone(),
        require_service_auth,
    ));

    let api_v1 = Router::new()
        .route("/healthz", get(healthz))
        .route("/systems", get(v1_systems))
        .route("/recordings", get(v1_list_recordings))
        .route("/recordings/available", get(v1_available_recordings))
        .route(
            "/recordings/{id}/correlations",
            get(v1_recording_correlations),
        )
        .route("/runs", create_run.get(v1_list_runs))
        .route("/runs/{run_id}/events", ingest_run_event)
        .route("/runs/{run_id}/kill", kill_run_route)
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

/// Human-facing mutations (`POST /runs`): identify the caller via `X-Deja-Actor`
/// for the audit trail, but do NOT require the service token. The orchestrator is
/// internal-only, so network reachability is the access boundary; keeping the
/// service token off this path means operators never handle it (it stays scoped
/// to inter-service callbacks — see `require_service_auth`). Stronger human authn
/// (SSO at the ingress) is a deliberate follow-up, not this layer's job.
async fn require_human_auth(mut req: Request<axum::body::Body>, next: Next) -> Response {
    let Some(actor) = actor_from_headers(req.headers()) else {
        return error_resp(401, "X-Deja-Actor header required for mutating requests");
    };
    req.extensions_mut().insert(AuthenticatedActor(actor));
    next.run(req).await
}

/// Inter-service callbacks (`POST /runs/{id}/events`): the out-of-process runner
/// authenticates with the shared `DEJA_API_SERVICE_TOKEN` (plus `X-Deja-Actor`
/// for audit). The token is provisioned to services only; human clients never
/// need it. When no token is configured (local/dev) the actor alone suffices.
async fn require_service_auth(
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
/// Requests reach this handler only after `require_human_auth` resolved an
/// `AuthenticatedActor` from `X-Deja-Actor` (the audit identity). This is a
/// human-facing endpoint, so it does NOT require the service token — that token
/// is scoped to inter-service callbacks (`require_service_auth`) so operators
/// never handle it. The endpoint is internal-only; SSO in front is a follow-up.
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
    // Refuse an oversized filter HERE, where the caller is still listening. The
    // lifecycle refuses it too — that is the gate no caller can go around — but
    // a request that will never be honoured should fail as a 400 now rather than
    // as a failed run several minutes later.
    if let Err(e) =
        deja_orchestrator::scope::check_requested_correlations(spec.correlation_filter.as_deref())
    {
        return error_resp(400, &e);
    }
    let run = match runs::persist_new(&st.root, spec) {
        Ok(run) => run,
        Err(e) => return error_resp(500, &format!("create run: {e}")),
    };
    // Store row + audit BEFORE the worker spawns (stage rows FK the run row).
    let ctx = if let Some(store) = &st.store {
        let candidate = serde_json::to_value(&run.spec.candidate_spec).unwrap_or_default();
        // The whole request, defaults already applied — the run row is the only
        // durable record of what this run was asked to do, and a report cannot
        // name a scope, a recording or a candidate it was never told.
        let params =
            deja_orchestrator::RunParams::resolved(&run.spec, expectation.as_deref()).to_json();
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
        ExecutorSelection::K8s(k) => runs::spawn_k8s_run(
            &st.root,
            run.clone(),
            ctx,
            k.incluster.clone(),
            k.cfg.clone(),
        ),
    }
    json_ok(
        serde_json::to_value(&runs::CreateRunResponse {
            run_id: run.run_id,
            status: run.status,
        })
        .unwrap_or_default(),
    )
}

/// `POST /api/v1/runs/{run_id}/kill` — stop a run and reclaim its pod.
///
/// A replay pod holds a candidate, a runner and two stores, and it is kept after
/// the Job finishes so its logs can be read — so a run left running, or one whose
/// Job outlives a failure, sits on that for hours. This deletes the Job and
/// sweeps any pod the cascade misses, then records the run as failed.
///
/// Idempotent: killing an already-dead run reports nothing removed and still
/// succeeds, so it is always safe to press again.
async fn v1_kill_run(
    State(st): State<AppState>,
    Extension(actor): Extension<AuthenticatedActor>,
    Path(run_id): Path<String>,
) -> Response {
    let ExecutorSelection::K8s(k) = &*st.executor else {
        return error_resp(400, "kill is only supported for the k8s executor");
    };
    let incluster = k.incluster.clone();
    let namespace = k.cfg.jobs_namespace.clone();
    let store = st.store.clone();
    let handle = tokio::runtime::Handle::current();
    let (id, who) = (run_id.clone(), actor.0.clone());

    // Off the async runtime: the apiserver client is blocking, and settling the
    // run goes through the same StoreCtx the worker thread uses, which drives its
    // async writes with `Handle::block_on` — that panics if called on a runtime
    // thread, taking the connection down with it.
    let report = match tokio::task::spawn_blocking(move || {
        let transport = deja_orchestrator::executor::UreqTransport::new(&incluster)
            .map_err(|e| format!("k8s client: {e}"))?;
        let api = deja_orchestrator::executor::KubeApi::new(transport);
        let report = deja_orchestrator::executor::kill_run(&api, &namespace, &id)
            .map_err(|e| format!("kill run: {e}"))?;
        // Settle the run so it stops showing as in-flight. The store's terminal
        // guard makes this a no-op if the runner already reported a verdict.
        let ctx = match &store {
            Some(s) => deja_orchestrator::lifecycle::StoreCtx::new(
                &id,
                Some((handle, std::sync::Arc::clone(s))),
            ),
            None => deja_orchestrator::lifecycle::StoreCtx::disabled(&id),
        };
        ctx.finish(false, Some(&format!("killed by {who}")));
        Ok::<_, String>(report)
    })
    .await
    {
        Ok(Ok(r)) => r,
        Ok(Err(e)) => return error_resp(500, &e),
        Err(e) => return error_resp(500, &format!("kill task: {e}")),
    };

    if let Some(store) = &st.store {
        let _ = store
            .audit(
                &actor.0,
                "run.kill",
                "run",
                &run_id,
                &serde_json::json!({ "job_deleted": report.job_deleted,
                                     "pods_deleted": report.pods_deleted,
                                     "problems": report.problems }),
            )
            .await;
    }

    json_ok(serde_json::json!({
        "run_id": run_id,
        "job_deleted": report.job_deleted,
        "pods_deleted": report.pods_deleted,
        "problems": report.problems,
    }))
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

/// Which bucket and key root a listing request scans, for an optional
/// `?system=`.
///
/// Absent, or naming the DEFAULT system, scans the deployment's own bucket —
/// naming the default has to mean what omitting it means. It did not: a
/// `?system=hyperswitch` was refused with a 400 while no parameter at all
/// returned 200 over the same bucket, so the default system was reachable only
/// by staying silent about it. That made a caller declaring the system it
/// replays impossible for the one system every existing caller uses, which is
/// backwards. The exemption here is the same one `candidate_binding_for` and
/// `config_source_for` already make.
///
/// Any OTHER system must have its bucket configured, and is refused by name if
/// it does not. The variable is the whitelist: scanning the default bucket and
/// labelling the rows with another system's name would be a wrong answer
/// wearing a confident label, which is worse than a refusal that says what to
/// set. Its root override is optional and only consulted once its bucket
/// resolves.
fn scan_scope(system: Option<&str>) -> Result<(String, String), String> {
    // Naming nothing means the default system, and the default system resolves
    // through the same registry as every other — declared, not special.
    // Delegates so that this endpoint, the correlation endpoint and the replay
    // pull path cannot disagree about where a recording is.
    deja_orchestrator::system::recording_scope(
        system.unwrap_or_else(|| deja_orchestrator::default_system()),
    )
}

/// `GET /api/v1/recordings/available` — what is in the bucket, newest first.
///
/// The catalog above lists recordings that have been PULLED, which is a
/// property of what has been replayed rather than of what exists; a recording
/// made an hour ago does not appear there until something drives it. This
/// lists the landing area itself, so choosing a recording is choosing from
/// what was recorded.
///
/// Nothing here takes a path. Where recordings land, and how the keys are
/// partitioned, belong to the deployment (`DEJA_S3_BUCKET`,
/// `DEJA_RECORDING_ROOT`) — a caller names a recording and the orchestrator
/// resolves the rest.
///
/// `?limit=` and `?offset=` page the result; `pulled` says whether the catalog
/// already has it, so a picker can show what is ready versus what will be
/// fetched on first use.
/// The systems this deployment can replay, with the configuration each
/// resolves to.
///
/// A discovery endpoint exists so that no CLIENT has to know the set. The
/// dashboard used to carry it three times over — a two-option `<select>`, a
/// TypeScript union that could not express a third system, and prism's own span
/// namespaces written into a React component — so adding a system meant editing
/// the browser as well as the orchestrator, in a language where the compiler
/// enforced the omission. Everything below is data the deployment already
/// stated; this only puts it where a caller can read it.
///
/// `configured` is the honest field: a system in this list may still be missing
/// what it needs to run. Naming it here and saying it is unconfigured is a
/// better answer than omitting it, which is indistinguishable from a system
/// nobody has heard of.
async fn v1_systems() -> Response {
    let systems: Vec<serde_json::Value> = deja_orchestrator::system::registry()
        .into_iter()
        .map(|s| {
            // Unusable if its declaration did not parse, whatever else resolved.
            let configured = s.error.is_none() && (s.is_default || s.s3_bucket.is_some());
            serde_json::json!({
                "name": s.name,
                "is_default": s.is_default,
                "configured": configured,
                "s3_bucket": s.s3_bucket,
                "recording_root": s.recording_root,
                "manages_stores": s.manages_stores,
                "manages_stores_declared": s.manages_stores_declared,
                "has_code_bundle": s.has_code_bundle,
                "job_template_key": s.job_template_key,
                "candidate_image_repo": s.candidate_image_repo,
                "instance_pattern": s.instance_pattern,
                "scored_span_namespaces": s.scored_span_namespaces,
                // Reported so a deployment can see the canon the scorer will
                // apply, rather than inferring it from a verdict that stopped
                // blocking.
                "reply_canons": s.reply_canons,
                // The five variable names a candidate reads, as derived from
                // the declared prefix (or overridden per slot). Exposed so the
                // derivation is observable on a deployment, not only asserted
                // in a test: "prism reads CS__DEJA__RUN_ID" is a fact worth
                // reading off the running orchestrator.
                "candidate_env": s.candidate_env,
                "candidate_config_files": s.candidate_config_files,
                "code_bundle_uri_env": s.code_bundle_uri_env,
                // Declarations this deployment made that are not being used.
                // Empty is the normal answer; a non-empty one is a
                // configuration mistake that would otherwise be invisible,
                // because every one of them degrades to a working default.
                "warnings": s.warnings,
                // Present only when the deployment stated something the
                // orchestrator could not honour. Such a system must not be run.
                "error": s.error,
            })
        })
        .collect();
    json_ok(serde_json::json!({ "systems": systems }))
}

async fn v1_available_recordings(
    State(st): State<AppState>,
    axum::extract::Query(q): axum::extract::Query<AvailableQuery>,
) -> Response {
    let mut cfg = deja_orchestrator::s3::S3Config::from_env();
    let system_scope = q.system.as_deref().filter(|s| !s.trim().is_empty());
    let root = match scan_scope(system_scope) {
        Ok((bucket, root)) => {
            cfg.bucket = bucket;
            root
        }
        Err(message) => return error_resp(400, &message),
    };
    let scan_bucket = cfg.bucket.clone();
    let found = match tokio::task::spawn_blocking(move || {
        deja_compactor::list_landed_recordings(&cfg, &root)
    })
    .await
    {
        Ok(Ok(found)) => found,
        Ok(Err(e)) => return error_resp(502, &format!("list recordings in bucket: {e}")),
        Err(e) => return error_resp(500, &format!("list recordings in bucket: {e}")),
    };

    // Which of them the catalog already holds. A failure to read the catalog
    // must not hide the bucket's contents, so it degrades to "unknown" rather
    // than failing the request.
    let pulled: std::collections::HashSet<String> = match require_store(&st) {
        Ok(store) => store
            .list_recordings(500)
            .await
            .map(|rows| rows.iter().map(|r| r.recording_id.clone()).collect())
            .unwrap_or_default(),
        Err(_) => Default::default(),
    };

    let total = found.len();
    let offset = q.offset.unwrap_or(0);
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let page_rows: Vec<deja_compactor::LandedRecording> =
        found.into_iter().skip(offset).take(limit).collect();

    // What each recording HOLDS, for the rows actually being returned.
    //
    // Until now this endpoint could offer only `objects` — a count of S3 objects
    // — and every caller choosing a recording had to guess from it whether the
    // tape was worth replaying. It is a bad proxy in both directions: the
    // recording that broke five PR replays was picked because it was in the
    // pulled catalog, and a two-object tape in this very bucket holds twelve
    // correlations. `correlations` is the number the choice actually wants — how
    // many recorded test cases are in there — and the seal already knows it, so
    // reporting it costs one small GET per row and never touches a data part.
    //
    // How many of them are DRIVABLE is a further question, and deliberately not
    // answered here: it depends on the recorded system's ingress convention,
    // which this endpoint does not know. The correlation index carries the
    // per-correlation boundaries a caller needs to decide it (see
    // `CorrelationSummary::boundaries`); a count on this row would have to pick a
    // convention and would be wrong for every system that does not share it.
    //
    // Enrichment, not a precondition: a recording that is not sealed keeps every
    // field it had, and reports its seal facts as null rather than as zero. Zero
    // correlations and "not counted yet" are different answers, and a picker that
    // rendered the second as the first would hide good recordings as empty ones.
    let ids: Vec<String> = page_rows.iter().map(|r| r.session_id.clone()).collect();
    // The SCANNED bucket, not the deployment's default. The rows above came from
    // whichever bucket `scan_scope` resolved for the named system, so reading
    // their manifests from `from_env()` would look for a prism recording's seal
    // in hyperswitch-art — finding nothing, and reporting every prism row as
    // unsealed with null counts. That failure is silent: "not sealed" is a valid
    // answer, so nothing downstream could tell it from the truth.
    let mut cfg_for_manifests = deja_orchestrator::s3::S3Config::from_env();
    cfg_for_manifests.bucket = scan_bucket.clone();
    let manifests = tokio::task::spawn_blocking(move || {
        deja_compactor::read_manifests(&cfg_for_manifests, &ids)
    })
    .await
    .ok()
    .and_then(Result::ok)
    .unwrap_or_default();

    let page: Vec<serde_json::Value> = page_rows
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            let manifest = manifests.get(i).and_then(Option::as_ref);
            // The id's provenance is parsed HERE, not by the client: a
            // recording made before ids carried a revision reports none, and
            // that difference should be one field rather than every reader
            // reimplementing the same two shapes.
            let identity = deja_orchestrator::parse_recording_id(&r.session_id);
            // Which system minted the session. The `inst=` pod names are the
            // authoritative signal when the scan captured any (the UCS pods
            // carry the pattern below; router pods do not). The id SHAPE alone
            // decides only the unambiguous case: rec-<sha>-<time>-<inst> is
            // the hyperswitch recorder's. run-<nanos> is NOT prism-specific —
            // the router recorder minted the same shape before ids carried a
            // revision, and a router tape wearing it was once badged "prism"
            // and replayed against a prism candidate: every request's
            // connection reset. Ambiguous stays null; a wrong label is worse
            // than none.
            let system: Option<String> = if let Some(system) = system_scope {
                // Scoped scan: the SOURCE BUCKET names the system — that is
                // the whole point of keeping the buckets separate.
                Some(system.to_owned())
            } else if !r.instances.is_empty() {
                // Match every registered system's declared pod-name pattern. No
                // match is UNKNOWN, not the default system: this arm used to
                // return hyperswitch for any pod name it did not recognise,
                // which made a third system silently mislabelled rather than
                // merely unconfigured, against what the comment above says.
                deja_orchestrator::system::system_from_instances(&r.instances)
            } else if matches!(
                identity,
                deja_orchestrator::RecordingIdentity::Described { .. }
            ) {
                // The id SHAPE is positive evidence for exactly one system: the
                // `rec-<revision>-<time>-<instance>` form is the default
                // recorder's. No other shape identifies its minter.
                Some(deja_orchestrator::default_system().to_owned())
            } else {
                None
            };
            let described = match &identity {
                deja_orchestrator::RecordingIdentity::Described {
                    revision,
                    recorded_at,
                    instance,
                } => serde_json::json!({
                    "revision": revision,
                    "recorded_at": recorded_at,
                    "instance": instance,
                }),
                deja_orchestrator::RecordingIdentity::BootDerived { booted_at_nanos } => {
                    serde_json::json!({ "booted_at_nanos": booted_at_nanos })
                }
                deja_orchestrator::RecordingIdentity::Opaque => serde_json::Value::Null,
            };
            serde_json::json!({
                "recording_id": r.session_id,
                "dates": r.dates,
                "latest_date": r.latest_date(),
                "objects": r.objects,
                "pulled": pulled.contains(&r.session_id),
                // Null for a recording whose id names no revision; its envelopes
                // still carry `code.sha` and `instance_id`.
                "identity": described,
                // Which recorded system minted the session, from the id shape;
                // null when the shape names neither.
                "system": system,
                // The bucket the session was FOUND in — with the scoped scan
                // this differs from the default, and a replay of the session
                // needs it to build `s3_source` (`s3://{bucket}/{prefix}`).
                "bucket": scan_bucket,
                // The `inst=` discriminators under the session — for a UCS
                // session this is the recorder's pod name, the only identity
                // its id does not carry.
                "instances": r.instances,
                // The prefix the orchestrator would ingest from. Reported so a
                // run can be reproduced by hand, not so a caller has to supply it.
                "prefix": r.prefix,
                // Seal facts. Null, never zero, when the recording is unsealed.
                "sealed": manifest.is_some(),
                "correlations": manifest.map(|m| m.counts.correlations),
                "events": manifest.map(|m| m.counts.events),
                // `sealed_instances`, not `instances`, and the prefix is load
                // bearing: the LISTING also reports instances — the `inst=` pod
                // names it can read straight off the keys — and that is a
                // different fact from this one, which is how many producers the
                // SEAL recorded per-instance coverage for. Both are worth having
                // and they can disagree (a pod that wrote objects the seal has
                // not covered yet). Sharing the key would not fail: `json!` keeps
                // the last of two identical keys, so one of the two facts would
                // vanish silently and readers would get a list or a number
                // depending on which line came last.
                "sealed_instances": manifest.map(|m| m.instances.len()),
                // Capture gaps: `global_sequence` ranges the recorder allocated
                // whose events never reached the tape. Already computed at seal
                // time and, until now, surfaced nowhere — it is the evidence the
                // tail-truncation work was reconstructing from ledger sequence
                // numbers by hand.
                "gaps": manifest.map(|m| m.instances.iter().map(|i| i.gaps.len()).sum::<usize>()),
            })
        })
        .collect();

    json_ok(serde_json::json!({
        "recordings": page,
        "total": total,
        "offset": offset,
        "limit": limit,
    }))
}

#[derive(serde::Deserialize)]
struct AvailableQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    /// Which system's recordings to list. Absent = the default bucket
    /// (`DEJA_S3_BUCKET`). A named system scans ITS bucket
    /// (`DEJA_<SYSTEM>_S3_BUCKET`, root `DEJA_<SYSTEM>_RECORDING_ROOT`
    /// default `landing/v1`) — the separate-buckets posture: prism tapes
    /// carry payment payloads on a tighter retention and never mix into the
    /// default bucket, so the LISTING goes to them instead.
    system: Option<String>,
}

/// What the store can say about one recording's correlations.
///
/// Three answers, kept apart on purpose. "Sealed, and here they are" and "it is
/// there but nothing has ingested it yet, so the list is not knowable cheaply"
/// and "no such recording" are three different facts, and a caller that flattens
/// them tells a user something false — most damagingly by rendering the middle
/// one as a recording with zero correlations, i.e. as nothing worth running.
enum RecordingCorrelations {
    Sealed(Vec<deja_orchestrator::s3::CorrelationSummary>),
    /// Sealed, but the index sidecar is absent — a seal written before it
    /// existed. The manifest still knows how many correlations it covered, so
    /// the count is answerable even though the rows are not.
    SealedWithoutIndex {
        correlations: usize,
    },
    /// In the landing area, not yet compacted into a sealed session.
    Landing {
        prefix: String,
    },
    Unknown,
}

#[derive(serde::Deserialize)]
struct CorrelationsQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    /// Case-insensitive substring match on the correlation id.
    q: Option<String>,
    /// Which system's recordings to look in. Absent means the default system,
    /// exactly as it does on `/recordings/available` — the two endpoints answer
    /// questions about the same recording and must resolve it the same way.
    system: Option<String>,
}

/// `GET /api/v1/recordings/{id}/correlations` — the recorded test cases in a
/// recording, in the order they happened.
///
/// Cheap by construction: this reads the sealed session's manifest and its
/// correlations index, and never a data part. Learning that a recording holds
/// 455 correlations costs two small GETs instead of the 119 MB the tape itself
/// is — which is the point of sealing the index next to the data. `q` filters
/// the rows already in hand, so searching costs nothing beyond that.
///
/// Rows are in TAPE ORDER — each correlation's first appearance — and paging
/// never reorders them. That matters beyond presentation: a run that names no
/// correlations drives the first [`deja_orchestrator::scope::MAX_CORRELATIONS_PER_RUN`]
/// in this same order, so the head of page zero IS what such a run will drive.
/// Any other default ordering here would show one set and run another.
async fn v1_recording_correlations(
    State(st): State<AppState>,
    Path(id): Path<String>,
    axum::extract::Query(q): axum::extract::Query<CorrelationsQuery>,
) -> Response {
    if id.trim().is_empty() {
        return error_resp(400, "recording id is required");
    }
    // Resolved through the same `scan_scope` as the listing, so this endpoint
    // and the one that offered the recording agree on where it is — and scoped
    // to the system the caller named, so a recording the listing reported in
    // another system's bucket is readable here rather than answering "is not in
    // s3://<default>/landing/v1" about a recording that exists.
    let mut cfg = deja_orchestrator::s3::S3Config::from_env();
    let root = match scan_scope(q.system.as_deref().filter(|s| !s.trim().is_empty())) {
        Ok((bucket, root)) => {
            cfg.bucket = bucket;
            root
        }
        Err(message) => return error_resp(400, &format!("{message} (reading correlations)")),
    };
    let bucket = cfg.bucket.clone();
    let scanned = root.clone();
    let wanted = id.clone();
    let found = match tokio::task::spawn_blocking(move || -> Result<_, String> {
        use deja_compactor::CorrelationIndex;
        let landing = |cfg: &_| -> Result<RecordingCorrelations, String> {
            Ok(
                match deja_compactor::locate_landing_prefix(cfg, &wanted, &scanned)? {
                    Some(prefix) => RecordingCorrelations::Landing { prefix },
                    None => RecordingCorrelations::Unknown,
                },
            )
        };
        match deja_orchestrator::s3::read_correlation_index(&cfg, &wanted)? {
            CorrelationIndex::Rows(rows) => Ok(RecordingCorrelations::Sealed(rows)),
            // Sealed before the index sidecar existed. The recording is real and
            // the landing area can still say what is in it, so this reads it
            // from there rather than failing — a missing index is a fact about
            // the seal, not about the recording.
            // NOT the landing fallback: we can prove this recording was sealed,
            // so answering "unknown" when its landing objects have since been
            // cleaned up would deny a recording we hold the manifest for. The
            // count is what the manifest knows; the rows are what it lost.
            CorrelationIndex::SealedWithoutIndex { correlations } => {
                Ok(RecordingCorrelations::SealedWithoutIndex { correlations })
            }
            // Not sealed. Whether that means "not ingested yet" or "no such
            // recording" is a question only the landing area can answer, and
            // they must not come back as the same thing.
            CorrelationIndex::NotSealed => landing(&cfg),
        }
    })
    .await
    {
        Ok(Ok(found)) => found,
        Ok(Err(e)) => return error_resp(502, &format!("read correlations: {e}")),
        Err(e) => return error_resp(500, &format!("read correlations: {e}")),
    };

    let offset = q.offset.unwrap_or(0);
    let limit = q.limit.unwrap_or(1000).clamp(1, 5000);
    let needle = q.q.as_deref().map(str::to_lowercase);

    match found {
        RecordingCorrelations::Sealed(rows) => {
            // The index also carries a row for UNCORRELATED events — ambient
            // background traffic, shared across cases. It is accounted for
            // there because its events are real, but it is not a test case and
            // nothing can drive it, so it is not offered as one.
            let cases: Vec<&deja_orchestrator::s3::CorrelationSummary> = rows
                .iter()
                .filter(|row| row.correlation_id.is_some())
                .collect();
            let matched: Vec<&&deja_orchestrator::s3::CorrelationSummary> = cases
                .iter()
                .filter(|row| match (&needle, row.correlation_id.as_deref()) {
                    (Some(needle), Some(id)) => id.to_lowercase().contains(needle),
                    (Some(_), None) => false,
                    (None, _) => true,
                })
                .collect();
            let page: Vec<&deja_orchestrator::s3::CorrelationSummary> = matched
                .iter()
                .skip(offset)
                .take(limit)
                .map(|row| **row)
                .collect();
            json_ok(serde_json::json!({
                "recording_id": id,
                "status": "sealed",
                // Everything the recording holds, independent of q/limit/offset:
                // "showing 100 of 455" needs the 455 to stay the recording's.
                "total": cases.len(),
                "matched": matched.len(),
                "max_per_run": deja_orchestrator::scope::MAX_CORRELATIONS_PER_RUN,
                "offset": offset,
                "limit": limit,
                "correlations": page,
            }))
        }
        RecordingCorrelations::Landing { prefix } => {
            // The list needs the sealed index, but the COUNT may already be
            // known: a recording ingested before it could be sealed leaves its
            // correlation count in the catalog. Report it when it is there —
            // "cannot be listed yet" is a much weaker thing to tell someone
            // without the number that says the recording is worth waiting for.
            let total = match require_store(&st) {
                Ok(store) => store
                    .recording_correlation_count(&id)
                    .await
                    .unwrap_or(None)
                    .map(serde_json::Value::from)
                    .unwrap_or(serde_json::Value::Null),
                // No catalog configured is not an answer about the recording.
                Err(_) => serde_json::Value::Null,
            };
            json_ok(serde_json::json!({
                "recording_id": id,
                "status": "landing",
                // Null, never zero: nothing has read this recording yet, so how
                // many correlations it holds is unknown rather than none.
                "total": total,
                "matched": serde_json::Value::Null,
                "max_per_run": deja_orchestrator::scope::MAX_CORRELATIONS_PER_RUN,
                "offset": offset,
                "limit": limit,
                "correlations": serde_json::Value::Null,
                "prefix": prefix,
                "detail": "recording has landed but is not sealed yet — its correlations are not \
                           knowable without ingesting it, which the first replay run of it does",
            }))
        }
        // 200, not an error: the recording exists and its size is known. A
        // caller gets the count it would have summed from the rows, and an
        // explicit note that the rows themselves are not available — rather
        // than a 502 about a healthy recording.
        RecordingCorrelations::SealedWithoutIndex { correlations } => json_ok(serde_json::json!({
            "recording_id": id,
            "status": "sealed_without_index",
            // The manifest's own count. Answerable even though the rows are
            // not — and NOT zero, which would report a recording we can prove
            // was sealed as one holding nothing.
            "total": correlations,
            "matched": serde_json::Value::Null,
            "max_per_run": deja_orchestrator::scope::MAX_CORRELATIONS_PER_RUN,
            "cases": Vec::<serde_json::Value>::new(),
            "note": "sealed before the correlation index existed: the manifest knows how many \
                     correlations the seal covered but not which",
        })),
        RecordingCorrelations::Unknown => error_resp(
            404,
            &format!("recording {id} is not in s3://{bucket}/{root}"),
        ),
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
    //
    // The fallback reads the tape THROUGH the run's scope. It used to take the
    // raw path and return every node in the session, so a run driving three
    // correlations answered this unauthenticated endpoint with the span
    // structure and field values of all 42,310. A scoping refusal is returned as
    // a 500, not swallowed into an empty record side: an empty graph reads as
    // "this run had no cascade", which is a false finding rather than a missing
    // one.
    let mut record = read_nodes(st.root.record_graph_path(&id));
    // The run may have completed WITHOUT a record graph, with the reason left
    // in a note. That is an answer, not an error: return the empty record side
    // with the reason stated, so the view can say "unavailable because …"
    // instead of the caller receiving a 500 from a successful run.
    let mut record_note = std::fs::read_to_string(st.root.record_graph_note_path(&id))
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty());
    if record.is_empty() && record_note.is_none() {
        if let Ok(run) = runs::get(&st.root, &id) {
            let scope = deja_orchestrator::scope::RunScope::of(&run);
            if let Some(rec) = run.recording_id.clone().or(run.spec.recording_id.clone()) {
                match deja_orchestrator::scope::ScopedRecording::open(&st.root, &rec, scope) {
                    Ok(recording) => match recording.graph_nodes() {
                        Ok(nodes) => {
                            record = nodes
                                .into_iter()
                                .filter_map(|n| serde_json::to_value(n).ok())
                                .collect();
                        }
                        // The tape's scoping refusal: same contract as the
                        // extract — an empty record side with its reason
                        // stated, never an empty side pretending to be a
                        // cascade-free run, and never a 500.
                        Err(e) if e.kind() == std::io::ErrorKind::InvalidData => {
                            record_note = Some(format!("record graph unavailable: {e}"));
                        }
                        Err(e) => {
                            return error_resp(500, &format!("scoped record graph: {e}"));
                        }
                    },
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                    Err(e) => return error_resp(500, &format!("open recording {rec}: {e}")),
                }
            }
        }
    }
    let replay = read_nodes(st.root.observed_path(&id));
    json_ok(serde_json::json!({
        "record": record,
        "replay": replay,
        "record_note": record_note,
    }))
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

    /// The bin's tests share one process environment too. See the lib's
    /// `test_env` for why readers hold this as well as writers.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Naming the default system means what omitting it means, and both mean
    /// the bucket the default system DECLARED. There is no fallback to the
    /// orchestrator's own bucket any more: a system that has not declared where
    /// its recordings are cannot be scanned for them, the default included.
    /// The WIRING, not just the seam: proof the handler consults the system
    /// resolver at all. An undeclared system can only produce a 400 through the
    /// new resolution — before it, `?system=` was ignored entirely and the
    /// request went on to S3 under the deployment's own bucket. Needs no S3,
    /// because the refusal happens before any store is built.
    #[test]
    fn the_correlations_endpoint_refuses_an_undeclared_system() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("runtime");
        std::env::set_var(
            "DEJA_CONFIG_TOML",
            "default_system = \"hyperswitch\"\n[systems.hyperswitch]\ns3_bucket = \"hyperswitch-art\"\n",
        );
        let dir = tempfile::tempdir().unwrap();
        let response = rt.block_on(v1_recording_correlations(
            axum::extract::State(test_state(dir.path())),
            axum::extract::Path("rec-whatever".to_owned()),
            axum::extract::Query(CorrelationsQuery {
                limit: None,
                offset: None,
                q: None,
                system: Some("zzz".to_owned()),
            }),
        ));
        std::env::remove_var("DEJA_CONFIG_TOML");

        assert_eq!(
            response.status(),
            axum::http::StatusCode::BAD_REQUEST,
            "an undeclared system is refused, not scanned for in the default bucket"
        );
        let body = rt
            .block_on(axum::body::to_bytes(response.into_body(), 64 * 1024))
            .expect("body");
        let body = String::from_utf8_lossy(&body);
        assert!(
            body.contains("systems.zzz") && body.contains("correlations"),
            "the refusal names what to declare AND which endpoint refused: {body}"
        );
    }

    /// Both recording endpoints answer questions about the same recording, so
    /// they must resolve it the same way. `/recordings/available?system=prism`
    /// reported a recording in `ucs-deja` while
    /// `/recordings/{id}/correlations?system=prism` looked in the default
    /// bucket and answered "is not in s3://hyperswitch-art/landing/v1" — a
    /// recording that existed to one endpoint and not to its sibling.
    ///
    /// Values are the deployment's own, so this fails if the document changes
    /// shape rather than passing against a plausible invention.
    #[test]
    fn both_recording_endpoints_resolve_a_system_the_same_way() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        std::env::set_var(
            "DEJA_CONFIG_TOML",
            "default_system = \"hyperswitch\"\n\
             [systems.hyperswitch]\ns3_bucket = \"hyperswitch-art\"\n\
             [systems.prism]\ns3_bucket = \"ucs-deja\"\n",
        );
        let prism = scan_scope(Some("prism"));
        let hyperswitch = scan_scope(Some("hyperswitch"));
        let omitted = scan_scope(None);
        let undeclared = scan_scope(Some("zzz"));
        std::env::remove_var("DEJA_CONFIG_TOML");

        assert_eq!(
            prism.as_ref().map(|(b, _)| b.as_str()),
            Ok("ucs-deja"),
            "a prism recording is in prism's bucket, whichever endpoint asks"
        );
        assert_eq!(
            hyperswitch.as_ref().map(|(b, _)| b.as_str()),
            Ok("hyperswitch-art")
        );
        assert_eq!(hyperswitch, omitted, "naming the default is omitting it");
        let refusal = undeclared.expect_err("an undeclared system is refused");
        assert!(
            refusal.contains("declared") && refusal.contains("systems.zzz"),
            "and the refusal names what to declare: {refusal}"
        );
    }

    #[test]
    fn naming_the_default_system_means_what_omitting_it_means() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let default = deja_orchestrator::default_system();
        std::env::set_var(
            "DEJA_CONFIG_TOML",
            format!(
                "[systems.{default}]\ns3_bucket = \"declared-art\"\nrecording_root = \"landing/v7\"\n[systems.other]\ns3_bucket = \"other-art\"\n"
            ),
        );
        let named = scan_scope(Some(default));
        let omitted = scan_scope(None);
        let other = scan_scope(Some("other"));
        let unknown = scan_scope(Some("zzz"));
        std::env::remove_var("DEJA_CONFIG_TOML");

        assert_eq!(
            named, omitted,
            "the same scope, whichever way it is asked for"
        );
        assert_eq!(
            named,
            Ok(("declared-art".to_owned(), "landing/v7".to_owned())),
            "the DECLARED bucket, not the orchestrator's own"
        );
        assert_eq!(other, Ok(("other-art".to_owned(), "landing/v1".to_owned())));
        let err = unknown.expect_err("an undeclared system is refused by name");
        assert!(
            err.contains("zzz") && err.contains("systems.zzz.s3_bucket"),
            "{err}"
        );

        // Undeclared, the default is refused too — asking the orchestrator's
        // own bucket for recordings would be a wrong answer wearing a
        // confident label.
        let bare = scan_scope(None);
        assert!(bare.is_err(), "no declaration, no scan: {bare:?}");
    }

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

    // Human mutation boundary (POST /runs): X-Deja-Actor only, no service token.
    fn human_router() -> Router {
        let create_run = post(ok).route_layer(middleware::from_fn(require_human_auth));
        Router::new().route("/runs", create_run.get(read_ok))
    }

    // Service callback boundary (POST /runs/{id}/events): X-Deja-Actor plus the
    // bearer token when DEJA_API_SERVICE_TOKEN is configured.
    fn service_router(auth: MutationAuth) -> Router {
        let ingest =
            post(ok).route_layer(middleware::from_fn_with_state(auth, require_service_auth));
        Router::new().route("/events", ingest)
    }

    async fn oneshot_status(
        router: Router,
        uri: &str,
        method: Method,
        token: Option<&str>,
        actor: Option<&str>,
    ) -> StatusCode {
        let mut builder = Request::builder().method(method).uri(uri);
        if let Some(token) = token {
            builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
        }
        if let Some(actor) = actor {
            builder = builder.header("X-Deja-Actor", actor);
        }
        router
            .oneshot(builder.body(Body::empty()).unwrap())
            .await
            .unwrap()
            .status()
    }

    async fn human_status(method: Method, token: Option<&str>, actor: Option<&str>) -> StatusCode {
        oneshot_status(human_router(), "/runs", method, token, actor).await
    }

    async fn service_status(
        auth: MutationAuth,
        token: Option<&str>,
        actor: Option<&str>,
    ) -> StatusCode {
        oneshot_status(service_router(auth), "/events", Method::POST, token, actor).await
    }

    #[tokio::test]
    async fn human_create_allows_actor_only() {
        assert_eq!(
            human_status(Method::POST, None, Some("local-dev")).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn human_create_does_not_require_service_token() {
        // The point of the split: a human scheduling a run never presents the
        // service token, even where one is configured (it's a service secret).
        assert_eq!(
            human_status(Method::POST, None, Some("hosted-user")).await,
            StatusCode::OK
        );
    }

    #[tokio::test]
    async fn human_create_denies_anonymous() {
        assert_eq!(
            human_status(Method::POST, None, None).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn human_read_routes_are_open() {
        assert_eq!(human_status(Method::GET, None, None).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn service_callback_requires_configured_token() {
        let auth = MutationAuth {
            service_token: Some(Arc::<str>::from("sandbox-secret")),
        };
        let missing = service_status(auth.clone(), None, Some("runner")).await;
        let wrong = service_status(auth.clone(), Some("wrong"), Some("runner")).await;
        let allowed = service_status(auth, Some("sandbox-secret"), Some("runner")).await;

        assert_eq!(missing, StatusCode::UNAUTHORIZED);
        assert_eq!(wrong, StatusCode::UNAUTHORIZED);
        assert_eq!(allowed, StatusCode::OK);
    }

    #[tokio::test]
    async fn service_callback_denies_anonymous() {
        let auth = MutationAuth {
            service_token: Some(Arc::<str>::from("sandbox-secret")),
        };
        assert_eq!(
            service_status(auth, Some("sandbox-secret"), None).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn service_callback_allows_actor_when_no_token_configured() {
        assert_eq!(
            service_status(
                MutationAuth {
                    service_token: None,
                },
                None,
                Some("runner"),
            )
            .await,
            StatusCode::OK
        );
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
                scored_span_namespaces: Vec::new(),
                mode: deja_orchestrator::RunMode::Replay,
                system_under_test: None,
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

    /// An unrouted `/api/v1/...` path does NOT 404: `spa_fallback` claims every
    /// URL the API router does not and answers `index.html` with 200 OK and
    /// `text/html`. So a route that was never registered fails as a confusing
    /// success, and no request against it can tell you it is missing. This
    /// asserts registration by the one thing that differs — who answered.
    #[tokio::test]
    async fn the_correlations_route_is_claimed_by_the_api_not_the_spa_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let answer = |uri: String| {
            let state = test_state(dir.path());
            async move {
                let req = Request::builder()
                    .method(Method::GET)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap();
                let resp = app_router(state).oneshot(req).await.unwrap();
                let content_type = resp
                    .headers()
                    .get(header::CONTENT_TYPE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_owned();
                (resp.status(), content_type)
            }
        };

        // A blank id is refused before any bucket work, so this reaches the
        // handler without needing a store to talk to.
        let (status, content_type) = answer("/api/v1/recordings/%20/correlations".to_owned()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(
            content_type.starts_with("application/json"),
            "the API must answer this path, not the SPA: {content_type}"
        );

        // The control: this is what an UNREGISTERED api path does, and why the
        // assertion above is about content type rather than status.
        let (status, content_type) = answer("/api/v1/recordings/x/not-a-route".to_owned()).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            content_type.starts_with("text/html"),
            "expected the SPA fallback to claim an unrouted api path: {content_type}"
        );
    }

    async fn post_event(state: AppState, run_id: &str, body: serde_json::Value) -> StatusCode {
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
        deja_orchestrator::write_json(&state.root.run_path("run-t"), &pending_run("run-t"))
            .unwrap();

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
        assert!(
            matches!(run.status, RunStatus::Failed),
            "terminal verdict is final"
        );
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
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "unknown run must 404, not upsert"
        );

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
