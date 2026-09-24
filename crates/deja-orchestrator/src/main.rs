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
use deja_orchestrator::{api::runs, divergence, HarnessRoot, Run, RunId, RunStatus};
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
    /// How many runs may RUN at once; `0` = no scheduling, which is the create
    /// endpoint launching Jobs that start immediately, as it always did.
    /// Resolved once at startup so the queue the endpoint creates into and the
    /// queue the scheduler drains are the same one.
    scheduler_capacity: usize,
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
                    scheduler_capacity: deja_orchestrator::executor::scheduler_capacity_from_env(),
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
    // Sweep the hydrated-artifact cache at boot, not only on the next view.
    // The cache is the reason this volume fills, so a deployment that fixes it
    // should reclaim on restart rather than waiting for someone to open a run —
    // which on a full volume is the one thing nobody can do.
    sweep_artifact_cache(&root, "");

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
            Some(store) => {
                deja_orchestrator::executor::reconcile::spawn(
                    store.clone(),
                    k.incluster.clone(),
                    k.cfg.clone(),
                );
                // The run scheduler. Off unless the environment declares a
                // capacity, in which case `v1_create_run` creates each Job
                // SUSPENDED and this loop resumes it when the pool has room.
                deja_orchestrator::executor::scheduler::spawn(
                    store.clone(),
                    k.incluster.clone(),
                    k.cfg.clone(),
                    k.scheduler_capacity,
                );
            }
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
        .route("/runs/{run_id}/change-coverage", get(v1_change_coverage))
        .route("/runs/{run_id}/tree", get(v1_tree))
        .route("/runs/{run_id}/delta", get(v1_delta))
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
    // A baseline is named by run id; a value that is not one would only be
    // discovered when the report asks for the delta.
    if let Some(against) = spec.delta_against.as_deref() {
        if let Err(e) = against.parse::<RunId>() {
            return error_resp(400, &format!("delta_against: {e}"));
        }
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
        // Scheduling on: the run is ACCEPTED and its Job created SUSPENDED —
        // the scheduler resumes it when the pool has room. Starting it here
        // would be the burst the queue exists to prevent, and a running Job
        // spends its `activeDeadlineSeconds` waiting for a node.
        ExecutorSelection::K8s(k) if k.scheduler_capacity > 0 => runs::spawn_k8s_run_queued(
            &st.root,
            run.clone(),
            ctx,
            k.incluster.clone(),
            k.cfg.clone(),
        ),
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
    run_id: RunId,
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
        "run_id": run_id.as_str(),
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
    run_id: RunId,
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
        // This run's own delta, and those measured against it. Off the
        // ingest path.
        if settles_deltas(&ev) {
            tokio::spawn(settle_deltas_for(st.clone(), run_id.to_string()));
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
                "main_instance_prefix": s.main_instance_prefix,
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
    let mut found = match tokio::task::spawn_blocking(move || {
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

    // Selection filters. Both default OFF, so every existing caller sees exactly
    // what it saw before. A caller that wants a recording it can actually drive
    // asks for it, and is refused BY NAME when the deployment has not declared
    // enough to answer — never served the unfiltered list as if the question had
    // been honoured.
    if q.require_revision.unwrap_or(false) {
        found.retain(|r| {
            matches!(
                deja_orchestrator::parse_recording_id(&r.session_id),
                deja_orchestrator::RecordingIdentity::Described { .. }
            )
        });
    }
    if let Some(wanted) = q.group.as_deref().map(str::trim).filter(|g| !g.is_empty()) {
        found.retain(|r| {
            group_of(&deja_orchestrator::parse_recording_id(&r.session_id)).as_deref()
                == Some(wanted)
        });
    }
    if q.main_instances.unwrap_or(false) {
        // Matched rather than `unwrap_or_else`: that unifies this borrow of
        // `q.system` with the `&'static str` the default returns, which asks the
        // query string to live forever. A match lets the static coerce down
        // instead.
        let scoped: &str = match system_scope {
            Some(s) => s,
            None => deja_orchestrator::default_system(),
        };
        let Some(prefix) = deja_orchestrator::system::system_config(scoped).main_instance_prefix
        else {
            return error_resp(
                400,
                &format!(
                    "system `{scoped}` declares no `main_instance_prefix`, so which pods are its \
                     primary deployment is not knowable here: declare \
                     `systems.{scoped}.main_instance_prefix` or drop `main_instances`"
                ),
            );
        };
        // Non-emptiness is asserted separately and first. `all` over an empty
        // list is TRUE, so a recording whose `inst=` partitions the scan could
        // not read would otherwise pass a test it was never measured against —
        // and pass it precisely when we know least about it.
        found.retain(|r| from_main_deployment(r, &prefix));
    }

    // Re-order now that ids can be parsed here. This must happen BEFORE the page
    // is cut: a page taken from the wrong order does not merely mis-sort, it can
    // drop the newest recording off the end of the page entirely, which is what
    // it did in sandbox.
    found.sort_by_cached_key(selection_order_key);
    found.reverse();

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
    // This listing does not distinguish "not sealed" from "could not tell" —
    // read_manifests preserves that per-recording, a replay's membership check
    // needs it, this enrichment does not.
    let manifests: Vec<Option<deja_compactor::SessionManifest>> =
        tokio::task::spawn_blocking(move || {
            deja_compactor::read_manifests(&cfg_for_manifests, &ids)
        })
        .await
        .ok()
        .and_then(Result::ok)
        .unwrap_or_default()
        .into_iter()
        .map(|r| r.ok().flatten())
        .collect();

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
            // An id that names no revision is not the same as a recording whose
            // revision is unknown. The envelopes carry `code.sha`, the seal
            // already collected it, and this endpoint already holds the
            // manifest for the row — so report it rather than making every
            // reader open the tape to find out. Purely additive: it fills a
            // field that was null, and never overrides what an id did say,
            // because the two spell a sha at different lengths and reconciling
            // them is a separate question from filling a gap.
            let described = match (described, manifest.and_then(manifest_revision)) {
                (serde_json::Value::Null, Some(revision)) => serde_json::json!({
                    "revision": revision,
                    "revision_source": "manifest",
                }),
                (serde_json::Value::Object(mut o), Some(revision))
                    if !o.contains_key("revision") =>
                {
                    o.insert("revision".into(), revision.into());
                    o.insert("revision_source".into(), "manifest".into());
                    serde_json::Value::Object(o)
                }
                (other, _) => other,
            };
            serde_json::json!({
                "recording_id": r.session_id,
                // The deployment-and-day this belongs to, from the ID. Null
                // when the id does not carry both a revision and a start date —
                // which is NOT the same condition as `identity.revision` being
                // null, because that field can be answered from the manifest
                // and the manifest does not supply a day. See `group_of`.
                "group": group_of(&identity),
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

/// The order a caller means by "newest first".
///
/// The compactor's listing sorts by write DATE and then by session id, and it
/// cannot do better: it carries no deja dependency, so it cannot parse an id.
/// That tiebreak sorts `rec-<revision>-<time>-<instance>` on the REVISION hex,
/// so the moment two revisions are live the order stops tracking time — sandbox
/// had `rec-72b65cb-…0800` sorted above `rec-4157177-…1400`, six hours newer,
/// and the pipeline replayed the older tape without anything reporting a fault.
///
/// A total key rather than a hand-written comparator. The shapes here
/// (`Described`, `BootDerived`, `Opaque`) make a pairwise rule easy to write
/// non-transitively, and an inconsistent comparator does not fail loudly: it
/// yields a wrong order at some list lengths and not others.
///
/// `None` sorts below `Some`, so within one date a recording that names its
/// time comes before one that does not. For a system whose ids are ALL
/// boot-derived — prism mints `run-<nanos>` for every recording — every middle
/// element is `None` and the session id decides, and a fixed-width nanosecond
/// epoch sorts lexically exactly as it sorts numerically. That case was already
/// correct and stays correct.
/// Whether every instance that wrote this recording belongs to the system's
/// primary deployment.
///
/// Non-emptiness is asserted separately and FIRST, because `all` over an empty
/// list is true: a recording whose `inst=` partitions the scan could not read
/// would otherwise pass a test it was never measured against, and pass it
/// exactly when we know least about it.
///
/// The match is anchored with `starts_with` rather than `contains` for a reason
/// that is not stylistic: the custom deployments are named
/// `sbx-custom-cug-hyperswitch-server-…`, which CONTAINS the main deployment's
/// name `sbx-hyperswitch-server`. A substring test — which is what the
/// neighbouring `instance_pattern` does, for a different question — admits
/// precisely the pods this one exists to exclude.
/// The revision a sealed recording's ENVELOPES claim, when they claim exactly
/// one.
///
/// The manifest's `code` is the distinct code identities seen across the
/// session's envelopes, which is where the revision authoritatively lives —
/// `parse_recording_id`'s own documentation says so: "An id is a convenience,
/// and the recording's envelopes carry the same facts authoritatively."
///
/// Zero or several read as UNKNOWN rather than as a pick. A recording whose
/// envelopes disagree about which code produced them has no single revision,
/// and naming one of them would be a confident lie in exactly the case where a
/// caller most needs to know it cannot compare a candidate to this tape.
fn manifest_revision(manifest: &deja_compactor::SessionManifest) -> Option<String> {
    let distinct: std::collections::BTreeSet<&str> = manifest
        .code
        .iter()
        .filter_map(|c| c.sha.as_deref())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    match distinct.len() {
        1 => distinct.into_iter().next().map(str::to_owned),
        _ => None,
    }
}

/// The deployment-and-day a recording belongs to, or `None` when its id does not
/// name BOTH a revision and a start date.
///
/// Both, and the distinction is live rather than theoretical. A row's
/// `identity.revision` can come from the MANIFEST when the id does not carry
/// one (`revision_source: "manifest"`), so a boot-derived recording can report
/// a revision and still have no group — `run-1789076520165195354` does exactly
/// that today, with revision `28d8299` and 59 correlations. The manifest
/// supplies the revision; nothing supplies the day, because the id has no start
/// date and the recording's own `latest_date` is the day it last WROTE, which
/// for a session straddling midnight is not the day it belongs to.
///
/// Grouping it by the wrong day would put a recording in a selection whose
/// scope nobody named, which is worse than leaving it ungroupable: the pipeline
/// already excludes these on the main-deployment test, so nothing is lost by
/// declining to guess.
///
/// `<revision>-<MMDD>`, derived rather than stored. The id ALREADY carries the
/// minute a recording started, so the day is a prefix of something every
/// recording has had all along — no new id shape, nothing to mint, and every
/// recording ever sealed is groupable the moment this ships.
///
/// This is the unit a replay actually wants. A pod's recording is an arbitrary
/// slice: pods are replaced every thirty minutes, so "the traffic this
/// deployment served that day" is spread across dozens of them — 82 on the day
/// this was measured — and picking one is picking a fraction for no reason a
/// caller could state.
fn group_of(identity: &deja_orchestrator::RecordingIdentity) -> Option<String> {
    match identity {
        deja_orchestrator::RecordingIdentity::Described {
            revision,
            recorded_at,
            ..
        } => Some(format!(
            "{revision}-{}",
            &recorded_at[..4.min(recorded_at.len())]
        )),
        _ => None,
    }
}

fn from_main_deployment(r: &deja_compactor::LandedRecording, prefix: &str) -> bool {
    !r.instances.is_empty() && r.instances.iter().all(|i| i.starts_with(prefix))
}

fn selection_order_key(
    r: &deja_compactor::LandedRecording,
) -> (Option<String>, Option<String>, String) {
    let recorded_at = match deja_orchestrator::parse_recording_id(&r.session_id) {
        deja_orchestrator::RecordingIdentity::Described { recorded_at, .. } => Some(recorded_at),
        _ => None,
    };
    (
        r.latest_date().map(str::to_owned),
        recorded_at,
        r.session_id.clone(),
    )
}

#[derive(serde::Deserialize)]
struct AvailableQuery {
    limit: Option<usize>,
    offset: Option<usize>,
    /// Keep only recordings whose id names the revision that produced them.
    ///
    /// `run-<nanos>` is NOT merely a legacy spelling — it is what the prism
    /// recorder mints for every recording it makes — so this is a per-caller
    /// question rather than something the endpoint may decide. A router replay
    /// needs the revision (it is what makes a candidate comparable and what the
    /// candidate-migrations fetch resolves against) and asks for it; a prism
    /// caller must not, or it would filter away everything prism records.
    require_revision: Option<bool>,
    /// Keep only recordings written entirely by the system's primary
    /// deployment, per its declared `main_instance_prefix`.
    main_instances: Option<bool>,
    /// Keep only the members of one deployment-and-day, `<revision>-<MMDD>`.
    ///
    /// The members ARE the recording: replaying a deployment's day means
    /// driving all of them as one run rather than picking one pod's slice. So
    /// this is how a caller turns a group it has chosen into the list it needs,
    /// having chosen it from the `group` field on the rows.
    group: Option<String>,
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

/// How many runs `GET /api/v1/runs` returns.
///
/// Unchanged at 200. The 2026-09-17 outage was NOT this number — it was the
/// bytes PER row: 200 rows weighed 46.5 MB, of which 173,866 bytes were the
/// rows and the other 97.5% was `scorecard`. Without that column the same 200
/// rows are ~174 KB, so cutting the count would have bought 10x against the
/// 267x that dropping the column buys, and would have cost something real —
/// `RunsPage` derives attempt ordinals ("attempt 3 of 4") over the WHOLE list,
/// and a page-sized list makes that number silently wrong.
///
/// Paginating is still worth doing. It is worth doing with the ordinals moved
/// server-side first, which is a separate change and not one to make while the
/// pod is in CrashLoopBackOff.
const RUN_LIST_LIMIT: i64 = 200;

/// `GET /api/v1/runs` — run list (Postgres-backed; newest first).
///
/// Rows are [`RunSummaryRow`], which carries no `scorecard`. A caller that
/// needs one asks for a single run.
async fn v1_list_runs(State(st): State<AppState>) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_run_summaries(RUN_LIST_LIMIT).await {
        Ok(rows) => json_ok_ser(&rows),
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
async fn v1_get_run(State(st): State<AppState>, id: RunId) -> Response {
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

/// Default ceiling on the hydrated-artifact cache. `DEJA_ARTIFACT_CACHE_MAX_BYTES`
/// overrides it; `0` disables the sweep entirely.
///
/// Sized well under the state volume so tapes under `recordings/`, run records
/// and the seed work still have room: the cache is the part that grows without
/// anyone deciding to grow it.
const DEFAULT_ARTIFACT_CACHE_MAX_BYTES: u64 = 8 * 1024 * 1024 * 1024;

fn artifact_cache_max_bytes() -> u64 {
    std::env::var("DEJA_ARTIFACT_CACHE_MAX_BYTES")
        .ok()
        .and_then(|raw| raw.trim().parse::<u64>().ok())
        .unwrap_or(DEFAULT_ARTIFACT_CACHE_MAX_BYTES)
}

/// The kinds `hydrate_run_artifacts` copies down from the store.
const HYDRATED_KINDS: [&str; 6] = [
    "observed",
    "http_diffs",
    "lookup_table",
    "scorecard",
    "call_ledger",
    "record_graph",
];

/// Caches the orchestrator derives from hydrated files and rebuilds on a miss.
const DERIVED_CACHES: [&str; 3] = ["behaviour_tree", "delta", "change_coverage"];

/// Where the cache sweep may find a file of `kind` for `run_id`: a hydrated
/// copy, or a cache derived from one. Nothing else is cache.
fn cache_path_for_kind(root: &HarnessRoot, run_id: &str, kind: &str) -> Option<std::path::PathBuf> {
    match kind {
        "behaviour_tree" => Some(root.behaviour_tree_path(run_id)),
        "delta" => Some(root.delta_cache_path(run_id)),
        "change_coverage" => Some(root.change_coverage_path(run_id)),
        _ => local_path_for_artifact_kind(root, run_id, kind),
    }
}

fn cache_kinds() -> impl Iterator<Item = &'static str> {
    HYDRATED_KINDS.into_iter().chain(DERIVED_CACHES)
}

/// The directories the cache sweep looks in, asked of `cache_path_for_kind`
/// rather than named here.
fn artifact_cache_dirs(root: &HarnessRoot) -> Vec<std::path::PathBuf> {
    let mut dirs: Vec<std::path::PathBuf> = cache_kinds()
        .filter_map(|kind| cache_path_for_kind(root, "_probe", kind))
        .filter_map(|path| path.parent().map(std::path::Path::to_path_buf))
        .collect();
    dirs.sort();
    dirs.dedup();
    dirs
}

/// The run whose cache file `path` is, or `None` when it is not one.
///
/// A path is cache only if `cache_path_for_kind` would produce exactly it for
/// some run and kind. Those directories also hold run records, seed
/// certificates, notes and manifests, which are not copies of anything; a
/// directory is not a category.
fn cached_file_run(root: &HarnessRoot, path: &std::path::Path) -> Option<String> {
    const MARK: &str = "\u{1}";
    let name = path.file_name()?.to_str()?;
    cache_kinds().find_map(|kind| {
        let template = cache_path_for_kind(root, MARK, kind)?;
        let (prefix, suffix) = template.file_name()?.to_str()?.split_once(MARK)?;
        let run_id = name.strip_prefix(prefix)?.strip_suffix(suffix)?;
        (cache_path_for_kind(root, run_id, kind).as_deref() == Some(path))
            .then(|| run_id.to_owned())
    })
}

/// Delete least-recently-modified hydrated artifacts until the cache is under
/// budget.
///
/// Only files `cached_file_run` recognises are counted or deleted. Each is a
/// copy of an `s3://` object that the run's artifact row still points at, or a
/// cache derived from one, so eviction costs a re-download or a rebuild on the
/// next view and loses nothing. Without it the cache only ever grows: `hydrate_run_artifacts` skips
/// a path that already exists and has no counterpart that removes one, so the
/// volume fills in proportion to runs LOOKED AT rather than runs executed.
///
/// `keep` is the run being served right now — evicting its files between the
/// write and the read would turn a view into an empty one.
///
/// Modification time is the ordering key, not access time: `relatime` makes
/// atime unreliable and a hydrated file is written once and then only read.
/// So this is least-recently-HYDRATED, which for a write-once cache is the same
/// order.
fn sweep_artifact_cache(root: &HarnessRoot, keep: &str) {
    let budget = artifact_cache_max_bytes();
    if budget == 0 {
        return;
    }
    let mut entries: Vec<(std::time::SystemTime, u64, std::path::PathBuf)> = Vec::new();
    let mut total: u64 = 0;
    for dir in artifact_cache_dirs(root) {
        let Ok(listing) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in listing.flatten() {
            let path = entry.path();
            let Some(run_id) = cached_file_run(root, &path) else {
                continue;
            };
            // `keep` empty means keep nothing: the boot sweep protects no run.
            if !keep.is_empty() && run_id == keep {
                continue;
            }
            let Ok(meta) = entry.metadata() else { continue };
            if !meta.is_file() {
                continue;
            }
            let Ok(modified) = meta.modified() else {
                continue;
            };
            total = total.saturating_add(meta.len());
            entries.push((modified, meta.len(), path));
        }
    }
    if total <= budget {
        return;
    }
    entries.sort_by_key(|(modified, _, _)| *modified);
    let mut freed: u64 = 0;
    let mut removed = 0_usize;
    for (_, size, path) in entries {
        if total.saturating_sub(freed) <= budget {
            break;
        }
        if std::fs::remove_file(&path).is_ok() {
            freed = freed.saturating_add(size);
            removed += 1;
        }
    }
    if removed > 0 {
        eprintln!(
            "artifact cache: evicted {removed} file(s), {freed} byte(s); \
             {} of {budget} byte(s) remain",
            total.saturating_sub(freed)
        );
    }
}

/// Pull a run's `s3://` artifacts down to the local paths the detail endpoints
/// read (idempotent — a path already present is left alone). k8s runs publish
/// artifacts to S3 (the pod is ephemeral); this makes them readable on the
/// orchestrator. A failed pull leaves the path absent; the scorecard, calls and
/// http-diffs endpoints then answer through `absent_artifact`, which says the
/// artifact is registered but is not on this host, while `/graph` still reads
/// an absent side as empty. No-op for compose runs — their artifacts are already
/// local and their URIs are filesystem paths, not `s3://`.
///
/// Returns what the run registered, so a reader can tell an artifact that was
/// never published from one that has not arrived, and which of those can be
/// pulled again; `None` without a store.
async fn hydrate_run_artifacts(st: &AppState, run_id: &str) -> Option<Hydrated> {
    let store = st.store.clone()?;
    let arts = store.list_artifacts(run_id).await.ok()?;
    let registered: std::collections::BTreeSet<String> =
        arts.iter().map(|art| art.kind.clone()).collect();
    // Only an artifact that lives in S3 can be fetched a second time. A
    // compose run's artifacts are registered under their local paths: the
    // file on disk IS the artifact, and removing it would remove the only copy.
    let pullable: std::collections::BTreeSet<String> = arts
        .iter()
        .filter(|art| deja_orchestrator::codebundle::parse_s3_uri(&art.uri).is_ok())
        .map(|art| art.kind.clone())
        .collect();
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
                    // renamed into place: a concurrent view that finds the
                    // path takes it as whole, so it must never see a prefix
                    if let Err(e) = divergence::behaviour_tree::write_atomic(&local, &bytes) {
                        eprintln!("hydrate: write {}: {e}", local.display());
                    }
                }
                Err(e) => eprintln!("hydrate: {} <- {}: {e}", local.display(), art.uri),
            }
        }
        // Sweep AFTER writing, and never the run just written: a view that
        // hydrated its own files and then evicted them would render empty.
        sweep_artifact_cache(&root, &run_id);
    })
    .await;
    Some(Hydrated {
        registered,
        pullable,
    })
}

/// What a run's artifact registration says: every kind it published, and the
/// subset held in S3 that the orchestrator can pull again.
struct Hydrated {
    registered: std::collections::BTreeSet<String>,
    pullable: std::collections::BTreeSet<String>,
}

/// `GET /api/v1/runs/{id}/scorecard` — serve the scorecard the run published.
///
/// The API does not score. A run whose scorecard is not here gets a refusal
/// naming why; a card built on this host from whatever inputs happened to be
/// lying around would look like a judgement the run never made.
async fn v1_scorecard(State(st): State<AppState>, id: RunId) -> Response {
    hydrate_run_artifacts(&st, &id).await;
    let content = match std::fs::read_to_string(st.root.scorecard_path(&id)) {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return absent_artifact(&st, &id, "scorecard").await;
        }
        Err(e) => return error_resp(500, &format!("scorecard: {e}")),
    };
    let mut card = match serde_json::from_str::<serde_json::Value>(&content) {
        Ok(card) => card,
        Err(e) => {
            return error_resp(
                500,
                &format!("scorecard: the published artifact does not parse: {e}"),
            )
        }
    };
    // A scorer that ingested nothing can only say so; which of the run's
    // dispositions explains it is known here and not there.
    if let Some(reason) = card.pointer_mut("/verdict/reason") {
        if reason.as_str() == Some(divergence::NO_ARTIFACTS_REASON) {
            let (state, failure) = run_disposition(&st, &id).await;
            *reason = serde_json::Value::String(empty_scorecard_reason(
                state.as_deref(),
                failure.as_deref(),
            ));
        }
    }
    json_ok(card)
}

/// The run's state and failure message, preferring the STORE row.
///
/// Order matters and is not arbitrary. The file store is the worker's LIVE
/// snapshot, and on the k8s path it is not where a terminal state lands:
/// `StoreCtx` reports through Postgres or the ingest endpoint and never writes
/// back to `run.json`. The run this was found on still said `resolving` on disk
/// an hour after the row said `failed`, so reading the file first would state
/// the opposite of the truth with full confidence. The file remains the
/// fallback because a compose deployment has no store, and there it is the only
/// record there is.
async fn run_disposition(st: &AppState, id: &str) -> (Option<String>, Option<String>) {
    if let Some(store) = &st.store {
        if let Ok(Some(row)) = store.get_run(id).await {
            let failure = row
                .failure
                .as_ref()
                .and_then(|f| f.get("message"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned);
            return (Some(row.state), failure);
        }
    }
    // The live record, read through the same containment check as every
    // other file the delta handlers open.
    let live: Option<Run> = confined(st.root.run_path(id), &st.root.root.join("runs"))
        .and_then(|path| deja_orchestrator::read_json::<Run>(&path).ok());
    match live {
        Some(run) => (
            serde_json::to_value(run.status)
                .ok()
                .and_then(|v| v.as_str().map(str::to_owned)),
            run.failure_reason,
        ),
        None => (None, None),
    }
}

/// Where a run stands, as the one thing both "empty" and "absent" answers name.
enum Disposition<'a> {
    Failed(Option<&'a str>),
    Completed,
    InProgress(&'a str),
    Unknown,
}

impl<'a> Disposition<'a> {
    fn of(state: Option<&'a str>, failure: Option<&'a str>) -> Self {
        match state {
            Some("failed") => Self::Failed(failure),
            Some("completed") => Self::Completed,
            Some(other) => Self::InProgress(other),
            None => Self::Unknown,
        }
    }

    fn named(&self) -> String {
        match self {
            Self::Failed(failure) => format!(
                "the run FAILED — {}",
                failure.unwrap_or("no failure message was recorded against the run")
            ),
            Self::Completed => "the run reports COMPLETED".to_owned(),
            Self::InProgress(state) => format!("the run is still {state}"),
            Self::Unknown => "the run itself could not be read".to_owned(),
        }
    }
}

/// Why a scorecard that judged nothing is empty, said in the run's own terms.
///
/// The scorer is handed the artifacts and nothing else, so the most it can say
/// is that none arrived. Why none arrived is a property of the run: still
/// going, so they may appear; FAILED, so they never will; or COMPLETED having
/// ingested nothing, the loudest, because a run that succeeded without
/// comparing anything is a hole in the pipeline rather than a result.
fn empty_scorecard_reason(state: Option<&str>, failure: Option<&str>) -> String {
    let base = divergence::NO_ARTIFACTS_REASON;
    let disposition = Disposition::of(state, failure);
    let consequence = match disposition {
        Disposition::Failed(_) => {
            "Nothing was compared, so nothing here is evidence about the candidate"
        }
        Disposition::Completed => {
            "A run that finished without ingesting anything has not scored the candidate, \
             and this scorecard must not be read as though it had"
        }
        Disposition::InProgress(_) => {
            "This is a snapshot of a run in progress rather than a verdict on it"
        }
        Disposition::Unknown => "Whether more are coming is therefore unknown, not \"not yet\"",
    };
    format!("{base}: {}. {consequence}", disposition.named())
}

/// How the artifact index accounts for a `kind` this host does not have.
async fn artifact_registration(st: &AppState, id: &str, kind: &str) -> String {
    let listing = match &st.store {
        None => None,
        Some(store) => Some(
            store
                .list_artifacts(id)
                .await
                .map(|arts| arts.into_iter().map(|a| (a.kind, a.uri)).collect())
                .map_err(|e| e.to_string()),
        ),
    };
    describe_registration(listing, kind)
}

/// The index's account of `kind`, from its listing of the run as
/// `(kind, uri)` rows; `None` when the deployment has no store.
fn describe_registration(
    listing: Option<Result<Vec<(String, String)>, String>>,
    kind: &str,
) -> String {
    match listing {
        None => "this deployment has no artifact store, so the run's own files are the only \
                 copy and this one was never written"
            .to_owned(),
        Some(Ok(rows)) => match rows.into_iter().find(|(k, _)| k == kind) {
            Some((_, uri)) => format!(
                "it is registered at {uri} but is not on this host — the pull failed or the \
                 cached copy was evicted; a fault in serving it, not a fact about the run"
            ),
            // The index records uploads, not attempts: a run that never produced
            // this artifact and one whose upload failed look the same from here.
            None => "the run never registered one — whether it was not produced or its \
                     upload failed is not recorded here"
                .to_owned(),
        },
        Some(Err(e)) => format!("the artifact index could not be read ({e})"),
    }
}

/// The one answer every detail endpoint gives for an artifact that is not
/// here: which artifact, what the index says about it, and where the run
/// stands. 404 because the thing asked for does not exist on this host; the
/// body says whether it ever will.
async fn absent_artifact(st: &AppState, id: &str, kind: &str) -> Response {
    let registration = artifact_registration(st, id, kind).await;
    let (state, failure) = run_disposition(st, id).await;
    let disposition = Disposition::of(state.as_deref(), failure.as_deref());
    let consequence = match disposition {
        Disposition::Failed(_) => "It will not arrive; the failure is the answer",
        Disposition::Completed => {
            "A completed run is expected to have published it, so this is a hole in the \
             pipeline, not an empty result"
        }
        Disposition::InProgress(_) => "It may still arrive",
        Disposition::Unknown => "Whether it will arrive is unknown",
    };
    error_resp(
        404,
        &format!(
            "no {kind} artifact for {id}: {registration}; {}. {consequence}",
            disposition.named()
        ),
    )
}

/// A JSON-lines artifact, read so every non-blank line becomes a row or a
/// counted drop. `Ok(None)` is absence; a line that will not parse refuses the
/// whole artifact, because a truncated stream served as a whole one is
/// indistinguishable from a complete answer.
fn read_jsonl_artifact(
    path: &std::path::Path,
    kind: &str,
) -> Result<Option<Vec<serde_json::Value>>, String> {
    use std::io::BufRead as _;
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(format!("{kind}: {e}")),
    };
    let mut rows = Vec::new();
    let mut unparseable = 0usize;
    for line in std::io::BufReader::new(file).lines() {
        let line = line.map_err(|e| format!("{kind}: {e}"))?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str(&line) {
            Ok(row) => rows.push(row),
            Err(_) => unparseable += 1,
        }
    }
    if unparseable > 0 {
        return Err(format!(
            "{kind}: {unparseable} of {} lines could not be parsed; refusing to serve a \
             partial artifact as a whole one",
            rows.len() + unparseable
        ));
    }
    Ok(Some(rows))
}

/// Serve a JSON-lines artifact the run published, or name why it is not here.
async fn serve_jsonl_artifact(
    st: &AppState,
    id: &str,
    kind: &str,
    path: std::path::PathBuf,
) -> Response {
    hydrate_run_artifacts(st, id).await;
    match read_jsonl_artifact(&path, kind) {
        Ok(Some(rows)) => json_ok(serde_json::Value::Array(rows)),
        Ok(None) => absent_artifact(st, id, kind).await,
        Err(e) => error_resp(500, &e),
    }
}

/// `GET /api/v1/runs/{id}/calls` — the per-call divergence ledger (recorded vs
/// observed, classified + located) that backs the interactive diff view, as the
/// run published it. A published empty ledger is a run that made no calls.
async fn v1_calls(State(st): State<AppState>, id: RunId) -> Response {
    let path = st.root.call_ledger_path(&id);
    serve_jsonl_artifact(&st, &id, "call_ledger", path).await
}

/// `GET /api/v1/runs/{id}/http-diffs` — the kernel's per-request HTTP diffs
/// (status + field-level body diff), from the run's published http-diff stream.
async fn v1_http_diffs(State(st): State<AppState>, id: RunId) -> Response {
    let path = st.root.http_diff_path(&id);
    serve_jsonl_artifact(&st, &id, "http_diffs", path).await
}

/// `GET /api/v1/runs/{id}/graph` — the record-side and replay-side execution
/// graphs (raw nodes) for the cascade/tree view. The UI builds the tree from
/// node_id/parent_id and hangs boundary events off nodes via graph_node_id
/// (recorded events + the call ledger's observed side). Graph nodes ride the
/// shared `DejaRecord` stream: record-side in the recording tape, replay-side
/// in the run's observed stream.
async fn v1_graph(State(st): State<AppState>, id: RunId) -> Response {
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

/// `GET /api/v1/runs/{id}/change-coverage` — did the replay reach what the
/// candidate changed? Computed on first request from the git host's compare of
/// the candidate against its base branch and the run's own replay graph and
/// call ledger, then cached beside the run. Never an error for a run that
/// cannot be assessed: the body says why, so the report can say "not assessed"
/// instead of a reader receiving a 500 from a successful run.
async fn v1_change_coverage(State(st): State<AppState>, id: RunId) -> Response {
    use deja_orchestrator::change_coverage::{self, Assessment};

    let unavailable = |why: String| json_ok_ser(&Assessment::Unavailable { unavailable: why });

    // The run's own parameters — the live record on compose, the stored row's
    // params on k8s — name the system and the candidate. The live record is
    // read through the same containment check as every other file this
    // handler opens: resolved, and confirmed to lie under the runs directory.
    let live: Option<Run> = confined(st.root.run_path(&id), &st.root.root.join("runs"))
        .and_then(|path| deja_orchestrator::read_json::<Run>(&path).ok());
    let params: Option<deja_orchestrator::RunParams> = match live {
        Some(run) => Some(deja_orchestrator::RunParams::resolved(&run.spec, None)),
        None => match &st.store {
            Some(store) => match store.get_run(&id).await {
                Ok(Some(row)) => serde_json::from_value(row.params).ok(),
                _ => None,
            },
            None => None,
        },
    };
    let Some(params) = params else {
        return error_resp(404, "run not found");
    };

    // The evidence files, and the cache beside them. Each path is resolved and
    // then checked to lie under its own directory before it is opened — the
    // same containment check whatever the id looked like — and the cache is
    // named off the resolved replay-graph path rather than off the id, so no
    // file is ever written to a path the id alone chose.
    hydrate_run_artifacts(&st, &id).await;
    let observed_dir = st.root.root.join("observed");
    let runs_dir = st.root.root.join("runs");
    let Some(observed_path) = confined(st.root.observed_path(&id), &observed_dir) else {
        return unavailable(
            "the run published no replay execution graph, so there is no evidence of what ran"
                .to_owned(),
        );
    };
    let ledger_path = confined(st.root.call_ledger_path(&id), &runs_dir);
    let cache = deja_orchestrator::change_coverage_cache_of(&observed_path);
    if let Ok(cached) = std::fs::read_to_string(&cache) {
        if let Ok(value) = serde_json::from_str::<serde_json::Value>(&cached) {
            return json_ok(value);
        }
    }

    let system = params
        .system_under_test
        .clone()
        .unwrap_or_else(|| deja_orchestrator::default_system().to_owned());
    let config = deja_orchestrator::system::system_config(&system);
    let nonempty = |s: String| (!s.trim().is_empty()).then(|| s.trim().to_owned());
    let deployment_default = std::env::var("DEJA_CANDIDATE_REPO").ok();
    let Some(repo) = change_coverage::source_repo_for(
        params.candidate_repo.as_deref(),
        config.source_repo.as_deref(),
        config.is_default,
        deployment_default.as_deref(),
    ) else {
        return unavailable(format!(
            "no source repository is declared for system '{system}': set systems.{system}.source_repo (owner/name) in the deja configuration, or send candidate_repo on the run"
        ));
    };
    let Some(template) = std::env::var("DEJA_CANDIDATE_TARBALL_URL")
        .ok()
        .and_then(nonempty)
    else {
        return unavailable(
            "DEJA_CANDIDATE_TARBALL_URL is not set, so the candidate's source cannot be fetched"
                .to_owned(),
        );
    };
    let sha = match deja_orchestrator::executor::resolve_candidate_image_for(
        &params.candidate_spec,
        &system,
    ) {
        Ok((_, sha)) => sha,
        Err(e) => return unavailable(format!("the candidate does not name a build sha: {e}")),
    };
    let base_ref = config.change_base_ref.clone();

    let computed = tokio::task::spawn_blocking(
        move || -> Result<change_coverage::ChangeCoverage, String> {
            let replay: Vec<deja_core::ExecutionGraphNode> = std::fs::File::open(&observed_path)
                .map(|file| {
                    std::io::BufRead::lines(std::io::BufReader::new(file))
                        .map_while(Result::ok)
                        .filter_map(
                            |line| match serde_json::from_str::<deja::DejaRecord>(&line) {
                                Ok(deja::DejaRecord::GraphNode(node)) => Some(*node),
                                _ => None,
                            },
                        )
                        .collect()
                })
                .map_err(|e| format!("the run's replay graph could not be read: {e}"))?;
            if replay.is_empty() {
                return Err("the run published no replay execution graph, so there is no evidence of what ran".to_owned());
            }
            let calls: Vec<serde_json::Value> = ledger_path
                .and_then(|path| std::fs::read_to_string(path).ok())
                .map(|content| {
                    content
                        .lines()
                        .filter(|l| !l.trim().is_empty())
                        .filter_map(|l| serde_json::from_str(l).ok())
                        .collect()
                })
                .unwrap_or_default();
            let evidence = change_coverage::Evidence::from_graph_and_calls(&replay, &calls);
            let change = change_coverage::fetch_change_set(&repo, &base_ref, &sha, &template)?;
            Ok(change_coverage::assess(&system, &repo, &base_ref, change, &evidence))
        },
    )
    .await;
    let assessment = match computed {
        Ok(Ok(coverage)) => Assessment::Assessed(coverage),
        Ok(Err(why)) => Assessment::Unavailable { unavailable: why },
        Err(e) => Assessment::Unavailable {
            unavailable: format!("assessment task failed: {e}"),
        },
    };
    // Cache only an assessment: a transient failure must not be remembered as
    // the answer.
    if let Assessment::Assessed(_) = &assessment {
        if let Ok(text) = serde_json::to_string(&assessment) {
            let _ = std::fs::write(&cache, text);
        }
    }
    json_ok_ser(&assessment)
}

/// Why a run has no behaviour tree, or a pair of runs no delta. The two kinds
/// are different news: a pending answer clears on its own, so a reader may ask
/// again; a refusal never will, and asking again only hides it.
#[derive(Debug)]
enum Unavailable {
    Pending(String),
    Refused(String),
}

impl Unavailable {
    fn kind(&self) -> &'static str {
        match self {
            Unavailable::Pending(_) => "pending",
            Unavailable::Refused(_) => "refused",
        }
    }

    fn to_json(&self) -> serde_json::Value {
        let (Unavailable::Pending(why) | Unavailable::Refused(why)) = self;
        serde_json::json!({ "unavailable": why, "unavailable_kind": self.kind() })
    }
}

/// A file a tree is built from that could not be read whole.
enum ReadFailure {
    Missing(std::path::PathBuf),
    Unparsable {
        path: std::path::PathBuf,
        line: usize,
    },
}

/// Every line of `path` as a `T`, or the first line that is not one. A
/// dropped line would build a short tree that reads as a whole one.
fn read_jsonl_strict<T: serde::de::DeserializeOwned>(
    path: &std::path::Path,
) -> Result<Vec<T>, ReadFailure> {
    let text =
        std::fs::read_to_string(path).map_err(|_| ReadFailure::Missing(path.to_path_buf()))?;
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(i, line)| {
            serde_json::from_str(line).map_err(|_| ReadFailure::Unparsable {
                path: path.to_path_buf(),
                line: i + 1,
            })
        })
        .collect()
}

/// The files a run's tree is read from, each resolved under its own directory.
struct TreeSources {
    ledger: std::path::PathBuf,
    diffs: Option<std::path::PathBuf>,
    observed: Option<std::path::PathBuf>,
    cache: std::path::PathBuf,
}

/// Resolve a run's tree sources, or say why they are not all there. A source
/// the run registered but that is not on disk has not arrived yet; one it
/// never registered after it finished will never exist.
fn tree_sources(
    st: &AppState,
    id: &str,
    registered: Option<&std::collections::BTreeSet<String>>,
    state: Option<&str>,
) -> Result<TreeSources, Unavailable> {
    let absent = |kind: &str, what: &str| {
        match state {
        _ if registered.is_some_and(|r| r.contains(kind)) => Unavailable::Pending(format!(
            "run {id}'s {what} is published but has not reached the orchestrator yet"
        )),
        Some("completed" | "failed") => Unavailable::Refused(format!(
            "run {id} finished without publishing its {what}, so it was never scored and has no behaviour to compare"
        )),
        Some(state) => Unavailable::Pending(format!(
            "run {id} is still {state}; its {what} is published when it finishes"
        )),
        None => Unavailable::Refused(format!("run {id} could not be read")),
    }
    };
    let root = &st.root;
    let ledger = confined(root.call_ledger_path(id), &root.root.join("runs"))
        .ok_or_else(|| absent("call_ledger", "call ledger"))?;
    let diffs = confined(root.http_diff_path(id), &root.root.join("http-diffs"));
    if diffs.is_none() && registered.is_some_and(|r| r.contains("http_diffs")) {
        return Err(absent("http_diffs", "http diffs"));
    }
    let observed = confined(root.observed_path(id), &root.root.join("observed"));
    if observed.is_none() && registered.is_some_and(|r| r.contains("observed")) {
        return Err(absent("observed", "observed stream"));
    }
    // beside the ledger it is built from, named off the confined ledger path:
    // `<run>.call-ledger.jsonl` → `<run>.call-ledger.behaviour-tree.jsonl`
    let cache = deja_orchestrator::behaviour_tree_cache_of(&ledger);
    Ok(TreeSources {
        ledger,
        diffs,
        observed,
        cache,
    })
}

/// The cached tree when it is current, else one built strictly from its
/// sources and cached. Nothing short is ever built, so nothing short is
/// ever cached.
fn read_or_build_tree(
    id: &str,
    sources: &TreeSources,
) -> Result<divergence::behaviour_tree::BehaviourTree, ReadFailure> {
    use divergence::behaviour_tree::{self, BehaviourTree};

    if let Some(tree) = std::fs::read_to_string(&sources.cache)
        .ok()
        .and_then(|t| BehaviourTree::from_jsonl(&t))
        .filter(|t| t.canon_version == behaviour_tree::CANON_VERSION)
    {
        return Ok(tree);
    }
    let rows: Vec<divergence::ledger::CallRecord> = read_jsonl_strict(&sources.ledger)?;
    let diffs: Vec<deja_kernel::HttpDiff> = match &sources.diffs {
        Some(path) => read_jsonl_strict(path)?,
        None => Vec::new(),
    };
    let event_schema_versions = match &sources.observed {
        Some(path) => behaviour_tree::event_schema_versions(
            &std::fs::read_to_string(path).map_err(|_| ReadFailure::Missing(path.clone()))?,
        ),
        None => Default::default(),
    };
    let mut tree = behaviour_tree::build(id, &rows, &diffs);
    tree.event_schema_versions = event_schema_versions;
    // renamed into place, so a concurrent reader never sees a prefix
    let _ = tree.write_atomic(&sources.cache);
    Ok(tree)
}

/// A run's behaviour tree: read from the cache beside its ledger when one is
/// there, else built from the ledger, the http diffs and the observed stream,
/// and cached. `Err` says whether one can still appear.
///
/// Every file is opened through a path that was resolved and confirmed to lie
/// under its own directory, and the cache is named off the resolved ledger
/// path rather than off the id, so no file is read or written at a path the
/// id alone chose.
///
/// A hydrated file that cannot be read whole (evicted by the cache sweep
/// between hydration and read, or left torn by an older writer) is removed
/// and fetched once more before the answer is given — only when the run
/// holds that artifact in S3. A file that is the artifact's only copy is
/// never removed.
async fn behaviour_tree_for(
    st: &AppState,
    id: &str,
) -> Result<divergence::behaviour_tree::BehaviourTree, Unavailable> {
    let (state, _) = run_disposition(st, id).await;
    let mut refetched = false;
    loop {
        let hydrated = hydrate_run_artifacts(st, id).await;
        let sources = tree_sources(
            st,
            id,
            hydrated.as_ref().map(|h| &h.registered),
            state.as_deref(),
        )?;
        let kind_of = {
            let (ledger, diffs, observed) = (
                sources.ledger.clone(),
                sources.diffs.clone(),
                sources.observed.clone(),
            );
            move |path: &std::path::Path| -> &'static str {
                if path == ledger {
                    "call_ledger"
                } else if diffs.as_deref() == Some(path) {
                    "http_diffs"
                } else if observed.as_deref() == Some(path) {
                    "observed"
                } else {
                    ""
                }
            }
        };
        let run = id.to_owned();
        let read = tokio::task::spawn_blocking(move || read_or_build_tree(&run, &sources))
            .await
            .map_err(|e| Unavailable::Pending(format!("build behaviour tree of run {id}: {e}")))?;
        let failure = match read {
            Ok(tree) => return Ok(tree),
            Err(failure) => failure,
        };
        let path = match &failure {
            ReadFailure::Missing(path) | ReadFailure::Unparsable { path, .. } => path.clone(),
        };
        let pullable = hydrated
            .as_ref()
            .is_some_and(|h| h.pullable.contains(kind_of(&path)));
        if pullable && !refetched {
            let _ = std::fs::remove_file(&path);
            refetched = true;
            continue;
        }
        return Err(match failure {
            ReadFailure::Missing(path) if pullable => Unavailable::Pending(format!(
                "{} went missing while run {id}'s tree was read; it is fetched again on the next request",
                path.display()
            )),
            ReadFailure::Missing(path) => Unavailable::Refused(format!(
                "{} is not on this orchestrator and the run holds no copy to fetch, so run {id} has no behaviour to compare",
                path.display()
            )),
            ReadFailure::Unparsable { path, line } => Unavailable::Refused(format!(
                "line {line} of {} does not parse, so run {id}'s published artifact is not whole",
                path.display()
            )),
        });
    }
}

/// `GET /api/v1/runs/{id}/tree` — the run as a behaviour tree: every address
/// the tape holds, with whether the run reproduced it and, when not, a hash of
/// what it produced. `{unavailable, unavailable_kind}` when there is none.
async fn v1_tree(State(st): State<AppState>, id: RunId) -> Response {
    match behaviour_tree_for(&st, &id).await {
        Ok(tree) => json_ok_ser(&tree),
        Err(why) => json_ok(why.to_json()),
    }
}

#[derive(serde::Deserialize)]
struct DeltaQuery {
    /// The run to measure against: the baseline `M`. The path's run is `Y`.
    against: Option<String>,
}

/// `GET /api/v1/runs/{id}/delta?against={run}` — what this run changed
/// relative to another run of the same tape, three-way against the tape.
/// Both runs' tape-relative verdicts ride along so a reader sees the two
/// verdicts side by side. `{unavailable, unavailable_kind}` names why no
/// delta can be computed, and whether one still can be: `pending` while a
/// side is still being scored, `refused` for a pairing that never will.
async fn v1_delta(
    State(st): State<AppState>,
    id: RunId,
    axum::extract::Query(q): axum::extract::Query<DeltaQuery>,
) -> Response {
    let refused = |why: String| json_ok(Unavailable::Refused(why).to_json());
    let y_params = run_params_for(&st, &id).await;
    let declared = y_params.as_ref().and_then(|p| p.delta_against.clone());
    // The query names the baseline; without one, the run's own record does,
    // when the pipeline that created it said what to measure it against.
    let named = q
        .against
        .as_deref()
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .map(str::to_owned)
        .or_else(|| declared.clone());
    let against = match named {
        None => {
            return refused(
                "no baseline run named: pass ?against=<run id> of a run on the same tape, or create the run with delta_against"
                    .to_owned(),
            )
        }
        Some(raw) => match raw.parse::<RunId>() {
            Ok(id) => id,
            Err(e) => return refused(format!("against is not a run id: {e}")),
        },
    };
    if *against == *id {
        return refused("a run measured against itself has no delta".to_owned());
    }
    // The run's OWN delta — against the baseline it was created with — is
    // cached beside its ledger and its verdict is written to the run row.
    // Any other pairing is computed on the spot and kept nowhere.
    let result = if declared.as_deref() == Some(&*against) {
        delta_for_run(&st, &id, &against).await
    } else {
        delta_between(&st, &id, &against).await
    };
    match result {
        Ok(body) => json_ok(body),
        Err(why) => json_ok(why.to_json()),
    }
}

/// The delta of `y` against `m`: both trees, the three-way, and both sides'
/// tape verdicts. `Err` says why there is none, and whether there can be.
async fn delta_between(
    st: &AppState,
    y_id: &str,
    m_id: &str,
) -> Result<serde_json::Value, Unavailable> {
    let y_params = run_params_for(st, y_id).await;
    let m_params = run_params_for(st, m_id).await;
    let tape = |p: &Option<deja_orchestrator::RunParams>| {
        p.as_ref()
            .and_then(|p| p.recording_group.clone().or_else(|| p.recording_id.clone()))
    };
    if let (Some(y_tape), Some(m_tape)) = (tape(&y_params), tape(&m_params)) {
        if y_tape != m_tape {
            return Err(Unavailable::Refused(format!(
                "the runs drove different tapes ({y_tape} and {m_tape}); a delta only holds between runs of one tape"
            )));
        }
    }
    // One name is not one tape: a recording re-seals as it grows, so two runs
    // of one group can have read different content. What each run actually
    // read decides, and a run whose tape cannot be read refuses rather than
    // passing unchecked.
    if st.store.is_none() {
        return Err(Unavailable::Refused(
            "a delta needs the run store to read what each run's ingest report says it scored"
                .to_owned(),
        ));
    }
    let y_report = tape_report(st, y_id).await?;
    let m_report = tape_report(st, m_id).await?;
    divergence::tape::same_tape(y_id, Some(&y_report), m_id, Some(&m_report)).map_err(|why| {
        Unavailable::Refused(format!(
            "{why}; a delta only holds between runs of one tape"
        ))
    })?;
    let y = behaviour_tree_for(st, y_id).await?;
    let m = behaviour_tree_for(st, m_id).await?;
    let delta = divergence::delta::three_way(&m, &y).map_err(Unavailable::Refused)?;
    let verdict_of = |run: &str| -> serde_json::Value {
        confined(st.root.scorecard_path(run), &st.root.root.join("runs"))
            .and_then(|p| std::fs::read_to_string(p).ok())
            .and_then(|c| serde_json::from_str::<serde_json::Value>(&c).ok())
            .and_then(|v| v.get("verdict").cloned())
            .unwrap_or(serde_json::Value::Null)
    };
    let candidate_of = |p: &Option<deja_orchestrator::RunParams>| {
        p.as_ref()
            .map(|p| serde_json::to_value(&p.candidate_spec).unwrap_or_default())
    };
    let mut body = serde_json::to_value(&delta).unwrap_or_default();
    body["tape"] = serde_json::json!(tape(&y_params).or_else(|| tape(&m_params)));
    body["sides"] = serde_json::json!({
        "m": { "run": m_id, "tape_verdict": verdict_of(m_id), "candidate": candidate_of(&m_params) },
        "y": { "run": y_id, "tape_verdict": verdict_of(y_id), "candidate": candidate_of(&y_params) },
    });
    Ok(body)
}

/// The run row's word for a delta result: `pass` or `fail` once computed,
/// else the kind of unavailability, so a reader of the row can tell a delta
/// still coming from one that never will.
fn delta_verdict_word(result: &Result<serde_json::Value, Unavailable>) -> &'static str {
    match result {
        Ok(body) => match body.pointer("/verdict/pass").and_then(|v| v.as_bool()) {
            Some(true) => "pass",
            Some(false) => "fail",
            None => "refused",
        },
        Err(why) => why.kind(),
    }
}

/// The run's own delta, against the baseline named in its params: served
/// from the cache beside its ledger when that holds the current answer,
/// else computed, cached, and its verdict written to the run row. The cache
/// is named off the confined ledger path, and read and written off the async
/// worker, as the tree cache is. Only a computed delta is cached.
async fn delta_for_run(
    st: &AppState,
    y_id: &str,
    m_id: &str,
) -> Result<serde_json::Value, Unavailable> {
    use divergence::behaviour_tree::CANON_VERSION;

    let runs_dir = st.root.root.join("runs");
    let cache = confined(st.root.call_ledger_path(y_id), &runs_dir)
        .map(|ledger| deja_orchestrator::delta_cache_of(&ledger));
    if let Some(cache) = cache.clone() {
        let (y, m) = (y_id.to_owned(), m_id.to_owned());
        let cached = tokio::task::spawn_blocking(move || -> Option<serde_json::Value> {
            let doc: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&cache).ok()?).ok()?;
            divergence::delta::cached_is_current(&doc, &y, &m, CANON_VERSION).then_some(doc)
        })
        .await
        .ok()
        .flatten();
        if let Some(doc) = cached {
            // The row is settled from the cache too: a column that was reset,
            // or never written because the store was away, catches up on the
            // next view rather than waiting for a recomputation.
            record_delta_verdict(st, y_id, &Ok(doc.clone())).await;
            return Ok(doc);
        }
    }
    let computed = delta_between(st, y_id, m_id).await;
    record_delta_verdict(st, y_id, &computed).await;
    if let (Ok(body), Some(cache)) = (&computed, cache) {
        let text = body.to_string();
        let _ = tokio::task::spawn_blocking(move || {
            divergence::behaviour_tree::write_atomic(&cache, text.as_bytes())
        })
        .await;
    }
    computed
}

/// The run row's delta verdict, from a computed delta or the reason there is
/// none. Idempotent; the newest answer wins.
async fn record_delta_verdict(
    st: &AppState,
    y_id: &str,
    computed: &Result<serde_json::Value, Unavailable>,
) {
    if let Some(store) = &st.store {
        if let Err(e) = store
            .set_delta_verdict(y_id, delta_verdict_word(computed))
            .await
        {
            eprintln!("deja-orchestrator: delta verdict store write failed for {y_id}: {e}");
        }
    }
}

/// Whether ingesting `ev` settles deltas. A run's finish, not its result: the
/// runner reports the result BEFORE it publishes the ledger and diffs a tree
/// is read from, and finishes after.
fn settles_deltas(ev: &deja_orchestrator::lifecycle::store_ctx::RunEvent) -> bool {
    use deja_orchestrator::lifecycle::store_ctx::RunEvent;
    matches!(ev, RunEvent::Finish { .. })
}

/// Settle deltas when a run finishes, which is after it has published
/// everything a tree is read from: the run's own delta, if it names a
/// baseline, and the delta of every run that names THIS run as its
/// baseline. Best-effort and off the ingest path; a delta whose other side
/// is still running is left pending and settled when that side finishes.
async fn settle_deltas_for(st: AppState, run_id: String) {
    if let Some(against) = run_params_for(&st, &run_id)
        .await
        .and_then(|p| p.delta_against)
    {
        let _ = delta_for_run(&st, &run_id, &against).await;
    }
    let Some(store) = st.store.clone() else {
        return;
    };
    let Ok(dependents) = store.runs_measured_against(&run_id).await else {
        return;
    };
    for dependent in dependents {
        let _ = delta_for_run(&st, &dependent, &run_id).await;
    }
}

/// The run's parameters: the live record on compose, the stored row on k8s.
async fn run_params_for(st: &AppState, id: &str) -> Option<deja_orchestrator::RunParams> {
    let live: Option<Run> = confined(st.root.run_path(id), &st.root.root.join("runs"))
        .and_then(|path| deja_orchestrator::read_json::<Run>(&path).ok());
    match live {
        Some(run) => Some(deja_orchestrator::RunParams::resolved(&run.spec, None)),
        None => match &st.store {
            Some(store) => match store.get_run(id).await {
                Ok(Some(row)) => serde_json::from_value(row.params).ok(),
                _ => None,
            },
            None => None,
        },
    }
}

/// `candidate` resolved, if it exists and lies under `base`; `None` otherwise.
/// The resolution follows symlinks and folds `..`, so what is checked is the
/// file that would actually be opened.
fn confined(candidate: std::path::PathBuf, base: &std::path::Path) -> Option<std::path::PathBuf> {
    let base = base.canonicalize().ok()?;
    let candidate = candidate.canonicalize().ok()?;
    if !candidate.starts_with(&base) {
        return None;
    }
    Some(candidate)
}

/// `GET /api/v1/runs/{id}/stages` — append-only stage history.
async fn v1_run_stages(State(st): State<AppState>, id: RunId) -> Response {
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
    id: RunId,
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
async fn v1_run_artifacts(State(st): State<AppState>, id: RunId) -> Response {
    let store = match require_store(&st) {
        Ok(s) => s,
        Err(resp) => return resp,
    };
    match store.list_artifacts(&id).await {
        Ok(rows) => json_ok(serde_json::to_value(&rows).unwrap_or_default()),
        Err(e) => error_resp(500, &format!("list artifacts: {e}")),
    }
}

/// A registered artifact's bytes: an `s3://` uri (k8s run) is fetched from S3,
/// anything else (compose run) is read as a local path. The error carries the
/// HTTP status the raw endpoint answers with.
async fn artifact_bytes(uri: &str) -> Result<Vec<u8>, (u16, String)> {
    if let Ok((bucket, key)) = deja_orchestrator::codebundle::parse_s3_uri(uri) {
        let fetch = tokio::task::spawn_blocking(move || {
            let mut cfg = deja_orchestrator::s3::S3Config::from_env();
            cfg.bucket = bucket;
            deja_compactor::get_object_decoded(&cfg, &key)
        })
        .await;
        match fetch {
            Ok(Ok(b)) => Ok(b),
            Ok(Err(e)) => Err((502, format!("artifact fetch from s3: {e}"))),
            Err(e) => Err((500, format!("artifact fetch task: {e}"))),
        }
    } else {
        std::fs::read(uri).map_err(|e| (404, format!("artifact file unreadable: {e}")))
    }
}

/// A run's ingest report, or what its absence means for a delta.
async fn tape_report(st: &AppState, run_id: &str) -> Result<serde_json::Value, Unavailable> {
    match ingest_report_for(st, run_id).await {
        Ok(report) => Ok(report),
        Err(gap) => {
            let (state, _) = run_disposition(st, run_id).await;
            Err(report_gap(run_id, state.as_deref(), gap))
        }
    }
}

/// Why a run's ingest report could not be read.
#[derive(Debug)]
enum ReportGap {
    /// The run registered no report.
    NotRegistered,
    /// A report was registered but its object is no longer there: the
    /// artifact bucket expires objects, and a compose run's file can be removed.
    Gone,
    /// The object is there but is not a report.
    Corrupt(String),
    /// It could not be read this time, which may pass.
    Unreadable(String),
}

/// What a missing report means for a delta: refused when it cannot appear,
/// pending when it still can. A run that has not ingested yet has no report,
/// which is a wait, not a mismatch.
fn report_gap(run_id: &str, state: Option<&str>, gap: ReportGap) -> Unavailable {
    match gap {
        ReportGap::NotRegistered => match state {
            Some("completed" | "failed") | None => Unavailable::Refused(format!(
                "run {run_id} has no ingest report, so the tape it scored is unknown"
            )),
            Some(state) => Unavailable::Pending(format!(
                "run {run_id} is still {state}; its ingest report is published when it ingests"
            )),
        },
        ReportGap::Gone => Unavailable::Refused(format!(
            "run {run_id}'s ingest report is registered but no longer stored \
             (the artifact bucket expires objects), so the tape it scored is unknown"
        )),
        ReportGap::Corrupt(e) => {
            Unavailable::Refused(format!("run {run_id}'s ingest report is not JSON: {e}"))
        }
        ReportGap::Unreadable(e) => Unavailable::Pending(format!(
            "run {run_id}'s ingest report could not be read: {e}"
        )),
    }
}

/// The `ingest_report` a run published, parsed.
async fn ingest_report_for(st: &AppState, run_id: &str) -> Result<serde_json::Value, ReportGap> {
    let Some(store) = &st.store else {
        return Err(ReportGap::NotRegistered);
    };
    let artifacts = store
        .list_artifacts(run_id)
        .await
        .map_err(|e| ReportGap::Unreadable(format!("list artifacts: {e}")))?;
    // `list_artifacts` also matches on recording id, so the run is checked here.
    let Some(report) = artifacts
        .into_iter()
        .filter(|a| a.kind == "ingest_report" && a.run_id.as_deref() == Some(run_id))
        .max_by_key(|a| a.id)
    else {
        return Err(ReportGap::NotRegistered);
    };
    let bytes = match artifact_bytes(&report.uri).await {
        Ok(bytes) => bytes,
        Err(_) if artifact_is_gone(&report.uri).await => return Err(ReportGap::Gone),
        Err((_, e)) => return Err(ReportGap::Unreadable(e)),
    };
    serde_json::from_slice(&bytes).map_err(|e| ReportGap::Corrupt(e.to_string()))
}

/// Whether a registered artifact's object is definitely absent — a not-found
/// from the store, or a missing local file — as opposed to unreachable.
async fn artifact_is_gone(uri: &str) -> bool {
    match deja_orchestrator::codebundle::parse_s3_uri(uri) {
        Ok((bucket, key)) => tokio::task::spawn_blocking(move || {
            let mut cfg = deja_orchestrator::s3::S3Config::from_env();
            cfg.bucket = bucket;
            deja_compactor::object_exists(&cfg, &key)
        })
        .await
        .is_ok_and(|exists| matches!(exists, Ok(false))),
        Err(_) => !std::path::Path::new(uri).exists(),
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
    let bytes = match artifact_bytes(&art.uri).await {
        Ok(b) => b,
        Err((status, msg)) => return error_resp(status, &msg),
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
        Ok(rows) => json_ok_ser(&rows),
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
    run_id: RunId,
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

/// 200 with a JSON body serialised STRAIGHT from the value, with no
/// `serde_json::Value` in between.
///
/// [`json_ok`] takes an already-built `Value`, so a caller holding typed rows
/// has to clone them into a second tree first. For small bodies that is
/// invisible; for a list it is a full deep copy of everything being sent, and
/// on 2026-09-17 that copy was one of the three materialisations that OOMKilled
/// the orchestrator ten times. Handlers that hold typed rows should use this
/// one; handlers that genuinely assemble a `Value` keep [`json_ok`].
fn json_ok_ser<T: serde::Serialize>(value: &T) -> Response {
    match serde_json::to_vec(value) {
        Ok(bytes) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            bytes,
        )
            .into_response(),
        // Serialising our own row types cannot fail on shape; this is here so
        // the failure is NAMED rather than served as an empty body that reads
        // to a client as "no runs".
        Err(e) => error_resp(500, &format!("serialize response: {e}")),
    }
}

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

    /// The guard the type exists to enforce, on the endpoints that had skipped it.
    ///
    /// `RunId` refuses anything carrying a path separator or a parent reference,
    /// and its own doc says the check happens during extraction so "there is
    /// nothing for a new handler to remember to call and nothing for an existing
    /// one to have skipped". Five handlers had skipped it, by taking
    /// `Path<String>` instead — which is how a check documented as unskippable
    /// gets skipped: the seam is opt-in by TYPE, and the comment asserts the
    /// property rather than enforcing it.
    ///
    /// `run_stream` is the one that mattered. It carries no auth layer and its id
    /// reached `runs::get`, which resolves to a filesystem path.
    ///
    /// The body is asserted, not just the status, so a refusal is attributable
    /// to the run-id check rather than to a later failure that happens to share
    /// a status. On these four the statuses differ anyway when the guard is
    /// removed — the stream yields 200 with an SSE error event, the store-backed
    /// three yield the store's own refusal — but `v1_kill_run` answers a wrong
    /// executor with the same 400 this asserts, so status alone is not a safe
    /// thing to rely on for the family.
    #[tokio::test]
    async fn a_run_id_carrying_a_traversal_is_refused_before_the_handler() {
        let dir = tempfile::tempdir().unwrap();
        let app = Router::new()
            .route("/runs/{run_id}/stages", get(v1_run_stages))
            .route("/runs/{run_id}/logs", get(v1_run_logs))
            .route("/runs/{run_id}/artifacts", get(v1_run_artifacts))
            .route("/runs/{run_id}/stream", get(run_stream))
            .with_state(test_state(dir.path()));

        for suffix in ["stages", "logs", "artifacts", "stream"] {
            let uri = format!("/runs/..%2F..%2Fetc%2Fpasswd/{suffix}");
            let response = app
                .clone()
                .oneshot(Request::builder().uri(&uri).body(Body::empty()).unwrap())
                .await
                .unwrap();
            let status = response.status();
            let body = axum::body::to_bytes(response.into_body(), 64 * 1024)
                .await
                .unwrap();
            let text = String::from_utf8_lossy(&body);
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "/{suffix} must refuse a traversal id: {text}"
            );
            assert!(
                text.contains("run id must be plain"),
                "/{suffix} must refuse it AS a malformed run id, not as some \
                 later failure that happens to share a status: {text}"
            );
        }
    }

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

    const LEDGER_ROW: &str = r#"{"correlation_id":"c1","boundary":"redis","trait_name":"Cache","method_name":"get","kind":"matched","blocking":false}"#;

    /// A compose run in `status`, with `ledger` as its call ledger if given.
    fn run_with_ledger(
        dir: &std::path::Path,
        id: &str,
        status: RunStatus,
        ledger: Option<&str>,
    ) -> AppState {
        let st = test_state(dir);
        let mut run = pending_run(id);
        run.status = status;
        deja_orchestrator::write_json(&st.root.run_path(id), &run).unwrap();
        if let Some(ledger) = ledger {
            std::fs::write(st.root.call_ledger_path(id), ledger).unwrap();
        }
        st
    }

    fn rt() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// A missing ledger is pending while the run can still publish one, and
    /// refused once it has finished without one: only the first is worth
    /// asking again about.
    #[test]
    fn a_missing_ledger_is_pending_until_the_run_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let running = run_with_ledger(dir.path(), "run-a", RunStatus::Running, None);
        let finished = run_with_ledger(dir.path(), "run-b", RunStatus::Completed, None);
        let rt = rt();
        match rt.block_on(behaviour_tree_for(&running, "run-a")) {
            Err(why @ Unavailable::Pending(_)) => assert_eq!(why.kind(), "pending"),
            other => panic!("expected pending, got {other:?}"),
        }
        match rt.block_on(behaviour_tree_for(&finished, "run-b")) {
            Err(why @ Unavailable::Refused(_)) => assert_eq!(why.kind(), "refused"),
            other => panic!("expected refused, got {other:?}"),
        }
    }

    /// A published artifact that has not been hydrated yet will arrive, so it
    /// is pending even for a finished run.
    #[test]
    fn a_registered_but_absent_artifact_is_pending() {
        let dir = tempfile::tempdir().unwrap();
        let st = run_with_ledger(dir.path(), "run-c", RunStatus::Completed, Some(LEDGER_ROW));
        let registered: std::collections::BTreeSet<String> = ["call_ledger", "http_diffs"]
            .into_iter()
            .map(str::to_owned)
            .collect();
        match tree_sources(&st, "run-c", Some(&registered), Some("completed")) {
            Err(Unavailable::Pending(why)) => assert!(why.contains("http diffs"), "{why}"),
            Err(other) => panic!("expected pending, got {other:?}"),
            Ok(_) => panic!("expected pending, got sources"),
        }
        assert!(
            tree_sources(&st, "run-c", None, Some("completed")).is_ok(),
            "unregistered diffs are simply none"
        );
    }

    /// A ledger that does not parse whole builds nothing and caches nothing: a
    /// short tree would read the addresses past the cut as reproduced.
    #[test]
    fn a_torn_ledger_builds_no_tree_and_caches_none() {
        let dir = tempfile::tempdir().unwrap();
        let torn = format!("{LEDGER_ROW}\n{{\"correlation_id\":\"c2\",\"bound");
        let st = run_with_ledger(dir.path(), "run-d", RunStatus::Completed, Some(&torn));
        let cache = st
            .root
            .call_ledger_path("run-d")
            .with_extension("behaviour-tree.jsonl");
        match rt().block_on(behaviour_tree_for(&st, "run-d")) {
            Err(Unavailable::Refused(why)) => assert!(why.contains("line 2"), "{why}"),
            other => panic!("expected refused, got {other:?}"),
        }
        assert!(!cache.exists(), "nothing short is cached");

        let whole = run_with_ledger(dir.path(), "run-e", RunStatus::Completed, Some(LEDGER_ROW));
        let tree = rt().block_on(behaviour_tree_for(&whole, "run-e")).unwrap();
        assert_eq!(tree.correlations.len(), 1);
        assert!(whole
            .root
            .call_ledger_path("run-e")
            .with_extension("behaviour-tree.jsonl")
            .exists());
    }

    /// The tree records the event schema its candidate captured under, read
    /// off the observed stream.
    #[test]
    fn the_tree_carries_the_candidates_event_schema() {
        let dir = tempfile::tempdir().unwrap();
        let st = run_with_ledger(dir.path(), "run-f", RunStatus::Completed, Some(LEDGER_ROW));
        std::fs::write(
            st.root.observed_path("run-f"),
            r#"{"record_kind":"boundary_event","event_schema_version":10}"#,
        )
        .unwrap();
        let tree = rt().block_on(behaviour_tree_for(&st, "run-f")).unwrap();
        assert_eq!(tree.event_schema_versions, [10].into_iter().collect());
    }

    #[test]
    fn the_row_says_which_kind_of_unavailable() {
        let computed = |pass| Ok(serde_json::json!({ "verdict": { "pass": pass } }));
        assert_eq!(delta_verdict_word(&computed(true)), "pass");
        assert_eq!(delta_verdict_word(&computed(false)), "fail");
        assert_eq!(
            delta_verdict_word(&Err(Unavailable::Pending(String::new()))),
            "pending"
        );
        assert_eq!(
            delta_verdict_word(&Err(Unavailable::Refused(String::new()))),
            "refused"
        );
    }

    /// The runner reports its result before it publishes the ledger and diffs,
    /// so settling on the result would always find nothing to compare.
    #[test]
    fn deltas_settle_on_finish_not_on_result() {
        use deja_orchestrator::lifecycle::store_ctx::RunEvent;
        assert!(settles_deltas(&RunEvent::Finish {
            ok: true,
            failure: None
        }));
        assert!(!settles_deltas(&RunEvent::Result {
            verdict: Some("pass".to_owned()),
            scorecard: None
        }));
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
                delta_against: None,
                purpose: None,
                scored_span_namespaces: Vec::new(),
                mode: deja_orchestrator::RunMode::Replay,
                system_under_test: None,
                candidate_spec: deja_orchestrator::CandidateSpec::PrebuiltImage {
                    image: "deja-demo".to_owned(),
                },
                candidate_repo: None,
                recording_id: Some("rec-1".to_owned()),
                recording_group: None,
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

    // -- an empty scorecard names WHICH of its causes applies -----------------
    //
    // The rule these serve is the repo's standing one: an empty result names
    // which of its possible causes applies. A run that failed at step 1 of 6
    // used to answer "no artifacts ingested for this run yet" — true about the
    // artifacts, silent about the run, and the "yet" told the reader more were
    // coming when the run had been dead for an hour.

    /// THE SEAM. The lifecycle's scorer writes this reason into the card it
    /// publishes, and the scorecard endpoint matches on it to decide whether to
    /// name the run's disposition. If the scorer's wording drifts, the endpoint
    /// stops matching and the naming silently stops happening — the exact
    /// producer/consumer split that keeps costing this repo.
    #[test]
    fn the_scorer_emits_exactly_the_reason_the_endpoint_matches_on() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        // No artifacts of any kind: the `nothing` arm of `detect`.
        let card =
            deja_orchestrator::divergence::detect_and_score(&root, "run-with-nothing").unwrap();
        assert!(
            card.verdict.inconclusive,
            "an artifact-less run is not judgeable"
        );
        assert!(!card.verdict.pass, "and it certainly does not pass");
        assert_eq!(
            card.verdict.reason,
            deja_orchestrator::divergence::NO_ARTIFACTS_REASON,
            "the endpoint keys off this exact string; if the scorer's wording moved, \
             `v1_scorecard` has silently stopped naming why runs are empty"
        );
    }

    /// A run that FAILED says so, and carries the failure that explains it.
    #[test]
    fn a_failed_run_says_the_run_failed_and_why() {
        let reason = empty_scorecard_reason(
            Some("failed"),
            Some("job did not reach a terminal state within the watch deadline"),
        );
        assert!(reason.contains("FAILED"), "{reason}");
        assert!(
            reason.contains("job did not reach a terminal state within the watch deadline"),
            "the run's own failure is the answer to \"why is this empty\": {reason}"
        );
        // The precise regression: no claim that anything is still on its way.
        assert!(
            !reason.contains(" yet"),
            "a finished run's scorecard must not imply artifacts are still coming: {reason}"
        );
    }

    /// A failed run with no recorded message still says the run failed. The
    /// missing message is named as missing rather than papered over with the
    /// in-progress wording, which would be the wrong answer entirely.
    #[test]
    fn a_failed_run_without_a_message_still_says_it_failed() {
        let reason = empty_scorecard_reason(Some("failed"), None);
        assert!(reason.contains("FAILED"), "{reason}");
        assert!(reason.contains("no failure message"), "{reason}");
        assert!(!reason.contains(" yet"), "{reason}");
    }

    /// A run still in flight keeps the honest in-progress reading — this is the
    /// one case where "more may arrive" is true, and it must not be lost to the
    /// fix for the case where it is false.
    #[test]
    fn a_running_run_is_not_reported_as_finished() {
        let reason = empty_scorecard_reason(Some("resolving"), None);
        assert!(reason.contains("still resolving"), "{reason}");
        assert!(!reason.contains("FAILED"), "{reason}");
    }

    /// COMPLETED with nothing ingested is the loudest of the three: the run did
    /// not fail, so nothing else will flag it, and a reader skimming for red has
    /// no other cue that the pipeline dropped the whole comparison.
    #[test]
    fn a_completed_run_that_ingested_nothing_is_called_out() {
        let reason = empty_scorecard_reason(Some("completed"), None);
        assert!(reason.contains("COMPLETED"), "{reason}");
        assert!(reason.contains("has not scored the candidate"), "{reason}");
    }

    /// Every arm names a cause. A bare restatement of the base reason would be
    /// the old behaviour wearing the new code's clothes, and this is what would
    /// catch a future arm added without one.
    #[test]
    fn no_arm_leaves_the_cause_unnamed() {
        let base = deja_orchestrator::divergence::NO_ARTIFACTS_REASON;
        for state in [Some("failed"), Some("completed"), Some("running"), None] {
            let reason = empty_scorecard_reason(state, None);
            assert!(
                reason.len() > base.len(),
                "state {state:?} added nothing to the bare reason: {reason}"
            );
            assert!(reason.starts_with(base), "state {state:?}: {reason}");
        }
    }

    // -- the API serves what the run published, and names what it did not ----
    //
    // Each detail endpoint has exactly three answers: the artifact parses and is
    // served; it is absent and the response says why; it is present and will not
    // parse, and the response refuses with a count. No endpoint computes an
    // artifact the run did not publish.

    fn run_in(state: &AppState, run_id: &str, status: RunStatus, failure: Option<&str>) {
        let mut run = pending_run(run_id);
        run.status = status;
        run.failure_reason = failure.map(str::to_owned);
        deja_orchestrator::write_json(&state.root.run_path(run_id), &run).unwrap();
    }

    async fn get_json(state: AppState, uri: String) -> (StatusCode, serde_json::Value) {
        let req = Request::builder()
            .method(Method::GET)
            .uri(uri)
            .body(Body::empty())
            .unwrap();
        let resp = app_router(state).oneshot(req).await.unwrap();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    fn error_of(body: &serde_json::Value) -> &str {
        body.get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or_else(|| panic!("expected an error body, got {body}"))
    }

    /// A failed run with no scorecard gets a refusal carrying the failure, not a
    /// card synthesised to look like a judgement.
    #[tokio::test]
    async fn an_absent_scorecard_is_refused_with_the_runs_failure() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        run_in(
            &state,
            "run-a",
            RunStatus::Failed,
            Some("session not found"),
        );

        let (status, body) = get_json(state, "/api/v1/runs/run-a/scorecard".into()).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        let err = error_of(&body);
        assert!(err.contains("scorecard"), "names the artifact: {err}");
        assert!(err.contains("FAILED"), "names the run's state: {err}");
        assert!(
            err.contains("session not found"),
            "carries the failure: {err}"
        );
    }

    /// Inputs on disk but no published ledger: the ledger is absent, not
    /// rebuilt from those inputs inside the API process.
    #[tokio::test]
    async fn an_absent_ledger_is_not_rebuilt_from_inputs_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        run_in(&state, "run-b", RunStatus::Completed, None);
        for path in [
            state.root.lookup_table_path("run-b"),
            state.root.observed_path("run-b"),
        ] {
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(&path, "").unwrap();
        }

        let (status, body) = get_json(state, "/api/v1/runs/run-b/calls".into()).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        let err = error_of(&body);
        assert!(err.contains("call_ledger"), "names the artifact: {err}");
        assert!(err.contains("COMPLETED"), "names the run's state: {err}");
    }

    /// A ledger the run published empty is a run that made no calls — a fact
    /// about the run, served as one.
    #[tokio::test]
    async fn a_published_empty_ledger_is_a_run_that_made_no_calls() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        run_in(&state, "run-c", RunStatus::Completed, None);
        let path = state.root.call_ledger_path("run-c");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "").unwrap();

        let (status, body) = get_json(state, "/api/v1/runs/run-c/calls".into()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body, serde_json::json!([]));
    }

    /// A missing http-diff stream used to answer `[]`, which reads as "this run
    /// had no HTTP diffs".
    #[tokio::test]
    async fn an_absent_http_diff_stream_is_not_an_empty_one() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        run_in(&state, "run-d", RunStatus::Running, None);

        let (status, body) = get_json(state, "/api/v1/runs/run-d/http-diffs".into()).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{body}");
        let err = error_of(&body);
        assert!(err.contains("http_diffs"), "names the artifact: {err}");
        assert!(
            err.contains("still running"),
            "names the run's state: {err}"
        );
    }

    /// An unparseable line is counted and refused, not dropped from a stream
    /// that is then served as whole.
    #[tokio::test]
    async fn an_unparseable_http_diff_line_is_refused_not_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        run_in(&state, "run-e", RunStatus::Completed, None);
        let path = state.root.http_diff_path("run-e");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "{\"correlation_id\":\"c1\"}\n{truncated\n").unwrap();

        let (status, body) = get_json(state, "/api/v1/runs/run-e/http-diffs".into()).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR, "{body}");
        let err = error_of(&body);
        assert!(err.contains("1 of 2"), "counts the drop: {err}");
    }

    /// The other polarity: a stream the run published empty is served empty.
    #[tokio::test]
    async fn a_published_empty_http_diff_stream_is_served_empty() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        run_in(&state, "run-f", RunStatus::Completed, None);
        let path = state.root.http_diff_path("run-f");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "").unwrap();

        let (status, body) = get_json(state, "/api/v1/runs/run-f/http-diffs".into()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body, serde_json::json!([]));
    }

    /// A card the scorer PUBLISHED with nothing ingested still names the run's
    /// disposition. Produced by `detect_and_score`, the lifecycle's own writer,
    /// so this is the producer's wording meeting the endpoint's match.
    #[tokio::test]
    async fn a_published_empty_scorecard_names_the_runs_disposition() {
        let dir = tempfile::tempdir().unwrap();
        let state = test_state(dir.path());
        run_in(&state, "run-g", RunStatus::Completed, None);
        deja_orchestrator::divergence::detect_and_score(&state.root, "run-g").unwrap();
        assert!(
            state.root.scorecard_path("run-g").exists(),
            "precondition: the producer wrote the path the endpoint reads"
        );

        let (status, body) = get_json(state, "/api/v1/runs/run-g/scorecard".into()).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let reason = body["verdict"]["reason"].as_str().unwrap_or_default();
        assert!(reason.contains("COMPLETED"), "{reason}");
    }

    /// Registered-but-absent is a serving fault and says where the object is;
    /// a DIFFERENT kind registered for the run is not this one.
    #[test]
    fn a_registered_artifact_that_is_absent_is_a_serving_fault() {
        let rows = vec![
            (
                "scorecard".to_owned(),
                "s3://b/runs/r/scorecard.json".to_owned(),
            ),
            (
                "call_ledger".to_owned(),
                "s3://b/runs/r/call_ledger.jsonl".to_owned(),
            ),
        ];
        let said = describe_registration(Some(Ok(rows.clone())), "call_ledger");
        assert!(said.contains("s3://b/runs/r/call_ledger.jsonl"), "{said}");
        assert!(said.contains("not on this host"), "{said}");

        let said = describe_registration(Some(Ok(rows)), "http_diffs");
        assert!(said.contains("never registered"), "{said}");
        assert!(
            said.contains("upload failed"),
            "names the case it cannot rule out: {said}"
        );
    }

    /// No store and an unreadable index are each their own answer.
    #[test]
    fn no_store_and_an_unreadable_index_are_named_apart() {
        let said = describe_registration(None, "http_diffs");
        assert!(said.contains("no artifact store"), "{said}");
        let said = describe_registration(Some(Err("pool timed out".to_owned())), "http_diffs");
        assert!(
            said.contains("could not be read (pool timed out)"),
            "{said}"
        );
    }

    // ---- grouping: a deployment and a day ----

    /// The group is derived from an id that already exists. Every recording ever
    /// sealed is groupable the moment this ships — nothing to mint, no new id
    /// shape, no migration.
    #[test]
    fn a_group_is_the_revision_and_the_day_of_an_existing_id() {
        let id = deja_orchestrator::parse_recording_id("rec-4157177-09101430-xc");
        assert_eq!(super::group_of(&id).as_deref(), Some("4157177-0910"));
    }

    /// Two pods, two half-hour windows, ONE group. This is the whole point: the
    /// recordings of a deployment's day are spread across dozens of pods because
    /// pods are replaced every thirty minutes, and picking one is picking a
    /// fraction for no reason a caller could state.
    #[test]
    fn every_pod_of_a_deployments_day_lands_in_one_group() {
        let a = deja_orchestrator::parse_recording_id("rec-4157177-09100030-aa");
        let b = deja_orchestrator::parse_recording_id("rec-4157177-09102330-zz");
        assert_eq!(super::group_of(&a), super::group_of(&b));

        // The precondition that makes this a test of grouping rather than of
        // two identical inputs: they really are different recordings.
        assert_ne!(
            "rec-4157177-09100030-aa", "rec-4157177-09102330-zz",
            "precondition: distinct recordings"
        );
    }

    /// A different day and a different revision are both different groups. A
    /// grouping that collapsed either would replay one deployment's traffic
    /// against another's candidate, or mix two days into a run whose scope
    /// nobody named.
    #[test]
    fn the_day_and_the_revision_both_separate_groups() {
        let base = deja_orchestrator::parse_recording_id("rec-4157177-09101430-xc");
        let other_day = deja_orchestrator::parse_recording_id("rec-4157177-09111430-xc");
        let other_rev = deja_orchestrator::parse_recording_id("rec-72b65cb-09101430-xc");
        assert_ne!(super::group_of(&base), super::group_of(&other_day));
        assert_ne!(super::group_of(&base), super::group_of(&other_rev));
    }

    /// A recording whose id names no revision has no group. Null rather than a
    /// bucket for the unidentifiable, which would be a group a replay could
    /// select and then have no candidate to compare against.
    #[test]
    fn a_recording_without_a_revision_has_no_group() {
        for id in ["run-1788907613122648573", "rec-nonsense", "whatever"] {
            assert_eq!(
                super::group_of(&deja_orchestrator::parse_recording_id(id)),
                None,
                "{id} must not be grouped"
            );
        }
    }

    /// A REVISION IS NOT ENOUGH — the day has to come from the id too.
    ///
    /// This is the case live data produced rather than one I imagined:
    /// `run-1789076520165195354` reports revision `28d8299` on its row, because
    /// the manifest answered when the id could not, and it holds 59
    /// correlations. The manifest supplies no DAY, so grouping it would mean
    /// guessing one — and a recording placed in the wrong day is a member of a
    /// selection whose scope nobody named.
    ///
    /// So a row can have `identity.revision` set and `group` null, and that is
    /// the intended answer rather than an oversight.
    #[test]
    fn a_manifest_supplied_revision_does_not_make_a_group() {
        let boot = deja_orchestrator::parse_recording_id("run-1789076520165195354");
        // The precondition: this really is the boot-derived shape, so the test
        // is about a revision arriving from elsewhere and not about a bad id.
        assert!(
            matches!(
                boot,
                deja_orchestrator::RecordingIdentity::BootDerived { .. }
            ),
            "precondition: boot-derived"
        );
        assert_eq!(super::group_of(&boot), None);
    }

    // ---- identity: the revision the envelopes claim ----

    fn refused(u: &super::Unavailable) -> Option<&str> {
        match u {
            super::Unavailable::Refused(why) => Some(why),
            super::Unavailable::Pending(_) => None,
        }
    }

    /// A baseline that has not ingested yet has no report: that is a wait,
    /// not a mismatch, and must not read as "never".
    #[test]
    fn a_missing_report_on_an_unfinished_run_is_pending() {
        for state in ["queued", "seeding", "running", "resolving"] {
            let gap = super::report_gap("run-M", Some(state), super::ReportGap::NotRegistered);
            assert!(
                refused(&gap).is_none(),
                "{state} must be pending, got {gap:?}"
            );
        }
    }

    #[test]
    fn a_missing_report_on_a_finished_or_unknown_run_is_refused() {
        for state in [Some("completed"), Some("failed"), None] {
            let gap = super::report_gap("run-M", state, super::ReportGap::NotRegistered);
            let why = refused(&gap).unwrap_or_default();
            assert!(
                why.contains("run run-M has no ingest report"),
                "{state:?}: {gap:?}"
            );
        }
    }

    /// An expired object never comes back, so it refuses whatever the run's
    /// state; a transient read failure does not.
    #[test]
    fn a_gone_report_refuses_and_an_unreadable_one_waits() {
        let gone = super::report_gap("run-M", Some("completed"), super::ReportGap::Gone);
        assert!(
            refused(&gone)
                .unwrap_or_default()
                .contains("no longer stored"),
            "{gone:?}"
        );
        let corrupt = super::report_gap(
            "run-M",
            Some("completed"),
            super::ReportGap::Corrupt("eof".into()),
        );
        assert!(
            refused(&corrupt).unwrap_or_default().contains("not JSON"),
            "{corrupt:?}"
        );
        let flaky = super::report_gap(
            "run-M",
            Some("completed"),
            super::ReportGap::Unreadable("503".into()),
        );
        assert!(refused(&flaky).is_none(), "{flaky:?}");
    }

    fn manifest_with_codes(shas: &[Option<&str>]) -> deja_compactor::SessionManifest {
        let code: Vec<serde_json::Value> = shas
            .iter()
            .map(|s| serde_json::json!({ "sha": s, "deja_version": null }))
            .collect();
        serde_json::from_value(serde_json::json!({
            "manifest_version": 1,
            "session_id": "s",
            "status": "sealed",
            "capture_mode": "session",
            "envelope_schema_versions": [1],
            "event_schema_versions": [1],
            "code": code,
            "instances": [],
            "counts": {
                "landing_objects": 1, "lines_in": 1, "events": 1,
                "duplicates_dropped": 0, "correlations": 1
            },
            "data_parts": [],
            "created_unix_ms": 0
        }))
        .unwrap()
    }

    /// The gap this closes: a recording whose ID names no revision still has one
    /// in its envelopes, and the seal already collected it.
    #[test]
    fn a_single_envelope_sha_is_the_recordings_revision() {
        let m = manifest_with_codes(&[Some("4157177")]);
        assert_eq!(super::manifest_revision(&m).as_deref(), Some("4157177"));
    }

    /// Several entries naming the SAME sha is one revision, not an ambiguity —
    /// the manifest holds one entry per distinct code identity, but nothing
    /// stops a repeat, and collapsing to a set is what makes that harmless.
    #[test]
    fn repeated_entries_naming_one_sha_are_not_ambiguous() {
        let m = manifest_with_codes(&[Some("4157177"), Some("4157177")]);
        assert_eq!(super::manifest_revision(&m).as_deref(), Some("4157177"));
    }

    /// Two different shas means the recording spans revisions, so it has no
    /// single one. Picking either would be a confident lie in exactly the case
    /// where a caller most needs to know it cannot compare a candidate to this
    /// tape.
    #[test]
    fn two_envelope_shas_read_as_unknown_not_as_a_pick() {
        let m = manifest_with_codes(&[Some("4157177"), Some("72b65cb")]);

        // The precondition: both really are present, so this is testing the
        // ambiguity rule and not an empty collection.
        assert_eq!(m.code.len(), 2, "precondition: two code identities");

        assert_eq!(super::manifest_revision(&m), None);
    }

    /// Absent and blank both mean "not stated". A blank would otherwise become
    /// a revision that renders as an empty string and compares equal to
    /// nothing, which is worse than reporting none.
    #[test]
    fn absent_or_blank_shas_are_not_a_revision() {
        assert_eq!(super::manifest_revision(&manifest_with_codes(&[])), None);
        assert_eq!(
            super::manifest_revision(&manifest_with_codes(&[None])),
            None
        );
        assert_eq!(
            super::manifest_revision(&manifest_with_codes(&[Some("   ")])),
            None
        );
        // ...and a blank alongside a real one does not make the real one
        // ambiguous.
        assert_eq!(
            super::manifest_revision(&manifest_with_codes(&[Some("  "), Some("4157177")]))
                .as_deref(),
            Some("4157177")
        );
    }

    // ---- recording selection: order, and which pods count ----

    fn landed(session_id: &str, date: &str, instances: &[&str]) -> deja_compactor::LandedRecording {
        deja_compactor::LandedRecording {
            session_id: session_id.to_owned(),
            dates: vec![date.to_owned()],
            prefix: format!("landing/v1/dt={date}/session={session_id}"),
            objects: 1,
            instances: instances.iter().map(|s| (*s).to_owned()).collect(),
        }
    }

    fn newest_first(mut rows: Vec<deja_compactor::LandedRecording>) -> Vec<String> {
        rows.sort_by_cached_key(super::selection_order_key);
        rows.reverse();
        rows.into_iter().map(|r| r.session_id).collect()
    }

    /// The sandbox failure written as a test. Two revisions recorded on the same
    /// day, and the raw session-id tiebreak preferred the OLDER tape because it
    /// compares the revision hex before the timestamp.
    #[test]
    fn ordering_tracks_time_not_the_revision_hex() {
        let older = landed("rec-72b65cb-09070800-y1", "2026-09-07", &["pod"]);
        let newer = landed("rec-4157177-09071400-xc", "2026-09-07", &["pod"]);

        // The precondition that makes this test about the fix rather than about
        // nothing: the order being replaced really does prefer the older tape.
        assert!(
            older.session_id > newer.session_id,
            "precondition: raw id order puts the 08:00 tape above the 14:00 one"
        );

        assert_eq!(
            newest_first(vec![older, newer])[0],
            "rec-4157177-09071400-xc",
            "the 14:00 recording is newer than the 08:00 one whatever its revision"
        );
    }

    /// A session that straddles midnight is ordered by the date it last wrote
    /// into, so yesterday's 23:50 tape does not outrank this morning's.
    #[test]
    fn a_later_write_date_outranks_an_earlier_one() {
        let yesterday = landed("rec-4157177-09062350-aa", "2026-09-06", &["pod"]);
        let today = landed("rec-4157177-09070100-bb", "2026-09-07", &["pod"]);
        assert_eq!(
            newest_first(vec![yesterday, today])[0],
            "rec-4157177-09070100-bb"
        );
    }

    /// prism mints `run-<nanos>` for every recording it makes, so its ids carry
    /// no parsed time and fall through to the session id — where a fixed-width
    /// nanosecond epoch sorts lexically exactly as it sorts numerically. That
    /// order was already correct; this change must leave it alone.
    #[test]
    fn boot_derived_ids_keep_their_nanosecond_order() {
        let older = landed("run-1788539093442862902", "2026-09-07", &["pod"]);
        let newer = landed("run-1788680145151199733", "2026-09-07", &["pod"]);
        assert_eq!(
            newest_first(vec![older, newer])[0],
            "run-1788680145151199733"
        );
    }

    /// Seed one hydrated artifact with a chosen size and modification time.
    fn hydrated(root: &HarnessRoot, run_id: &str, kind: &str, bytes: usize, age_secs: u64) {
        let path = super::local_path_for_artifact_kind(root, run_id, kind).expect("known kind");
        std::fs::create_dir_all(path.parent().expect("kind dir")).expect("mkdir");
        std::fs::write(&path, vec![b'x'; bytes]).expect("write");
        let when = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(age_secs);
        let handle = std::fs::File::options()
            .write(true)
            .open(&path)
            .expect("open");
        handle
            .set_times(std::fs::FileTimes::new().set_modified(when))
            .expect("set mtime");
    }

    fn present(root: &HarnessRoot, run_id: &str, kind: &str) -> bool {
        super::local_path_for_artifact_kind(root, run_id, kind).is_some_and(|path| path.exists())
    }

    /// Seed a file that is NOT a hydrated artifact, with a chosen age.
    fn resident(path: &std::path::Path, bytes: usize, age_secs: u64) {
        std::fs::create_dir_all(path.parent().expect("dir")).expect("mkdir");
        std::fs::write(path, vec![b'x'; bytes]).expect("write");
        let when = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(age_secs);
        std::fs::File::options()
            .write(true)
            .open(path)
            .expect("open")
            .set_times(std::fs::FileTimes::new().set_modified(when))
            .expect("set mtime");
    }

    /// The files that share a directory with hydrated artifacts and are not
    /// copies of anything: the run record is the only copy of a run's progress,
    /// and the ingest endpoint refuses every event for a run without one.
    fn residents(root: &HarnessRoot, run_id: &str) -> Vec<std::path::PathBuf> {
        vec![
            root.run_path(run_id),
            root.seed_certificate_path(run_id),
            root.record_graph_note_path(run_id),
            root.root
                .join("runs")
                .join(format!("{run_id}.manifest.json")),
        ]
    }

    /// THE bug: at boot nothing is protected, the budget forces eviction, and
    /// the oldest files in `runs/` are run records. Only the hydrated copy may go.
    #[test]
    fn the_boot_sweep_deletes_only_hydrated_copies() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        for path in residents(&root, "run-old") {
            resident(&path, 1_000, 10);
        }
        hydrated(&root, "run-cached", "scorecard", 1_000, 100);
        std::env::set_var("DEJA_ARTIFACT_CACHE_MAX_BYTES", "1");
        super::sweep_artifact_cache(&root, "");
        // Vacuity guard: the sweep really did evict.
        assert!(
            !present(&root, "run-cached", "scorecard"),
            "precondition: the budget forced eviction"
        );
        for path in residents(&root, "run-old") {
            assert!(path.exists(), "the sweep deleted {}", path.display());
        }
    }

    /// A name is not enough: `runs/<id>.jsonl` has the file name an observed
    /// stream would have, in a directory observed streams never live in.
    #[test]
    fn a_hydrated_name_in_the_wrong_directory_is_not_cache() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let stray = root.root.join("runs").join("run-x.jsonl");
        resident(&stray, 1_000, 10);
        hydrated(&root, "run-cached", "scorecard", 1_000, 100);
        std::env::set_var("DEJA_ARTIFACT_CACHE_MAX_BYTES", "1");
        super::sweep_artifact_cache(&root, "");
        assert!(!present(&root, "run-cached", "scorecard"), "precondition");
        assert!(stray.exists(), "only the seam's exact path is cache");
    }

    /// Files that are not cache do not count toward the cache's budget. The
    /// run record is the NEWEST file here, so a sweep that counted it would
    /// evict both cached files to make room for it, and delete nothing it
    /// should not; only the count tells the two apart.
    #[test]
    fn only_cache_counts_toward_the_budget() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        hydrated(&root, "run-a", "call_ledger", 1_000, 50);
        hydrated(&root, "run-b", "call_ledger", 1_000, 100);
        resident(&root.run_path("run-big"), 10_000, 200);
        std::env::set_var("DEJA_ARTIFACT_CACHE_MAX_BYTES", "1500");
        super::sweep_artifact_cache(&root, "");
        assert!(
            !present(&root, "run-a", "call_ledger"),
            "precondition: 2,000 cached bytes over a 1,500 budget evicts the oldest"
        );
        assert!(
            present(&root, "run-b", "call_ledger"),
            "1,000 cached bytes remain, under budget: the record's bytes are not cache"
        );
        assert!(
            root.run_path("run-big").exists(),
            "a run record is not cache"
        );
    }

    /// The caches the orchestrator derives beside hydrated files are cache too:
    /// the behaviour tree is written when a run finishes, so leaving it out
    /// would grow the volume with every run executed.
    #[test]
    fn derived_caches_are_swept_and_the_record_beside_them_is_not() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let derived = [
            root.behaviour_tree_path("run-old"),
            root.delta_cache_path("run-old"),
            root.change_coverage_path("run-old"),
        ];
        for path in &derived {
            resident(path, 1_000, 10);
        }
        resident(&root.run_path("run-old"), 1_000, 5);
        std::env::set_var("DEJA_ARTIFACT_CACHE_MAX_BYTES", "1");
        super::sweep_artifact_cache(&root, "");
        for path in &derived {
            assert!(
                !path.exists(),
                "a derived cache was left: {}",
                path.display()
            );
        }
        assert!(root.run_path("run-old").exists(), "the run record survives");
    }

    /// `keep` protects one run, not every run whose id it prefixes.
    #[test]
    fn keep_protects_exactly_the_run_it_names() {
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        hydrated(&root, "run-a", "scorecard", 1_000, 10);
        hydrated(&root, "run-ab", "scorecard", 1_000, 20);
        std::env::set_var("DEJA_ARTIFACT_CACHE_MAX_BYTES", "1");
        super::sweep_artifact_cache(&root, "run-a");
        assert!(
            present(&root, "run-a", "scorecard"),
            "the served run is kept"
        );
        assert!(
            !present(&root, "run-ab", "scorecard"),
            "a run whose id merely starts with it is not"
        );
    }

    /// A cache under budget is left entirely alone — the sweep is a ceiling, not
    /// a scheduled deletion.
    #[test]
    fn a_cache_under_budget_loses_nothing() {
        // The budget is read from the process environment, which every test in
        // this binary shares — without the lock these race each other.
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        hydrated(&root, "run-a", "observed", 1_000, 100);
        hydrated(&root, "run-b", "observed", 1_000, 200);
        std::env::set_var("DEJA_ARTIFACT_CACHE_MAX_BYTES", "1000000");
        super::sweep_artifact_cache(&root, "");
        assert!(
            present(&root, "run-a", "observed"),
            "under budget, nothing goes"
        );
        assert!(present(&root, "run-b", "observed"));
    }

    /// Over budget, the OLDEST goes first and the sweep stops as soon as it is
    /// under — not "delete everything old", which would throw away a cache that
    /// is merely full.
    #[test]
    fn eviction_takes_the_oldest_first_and_stops_at_the_budget() {
        // The budget is read from the process environment, which every test in
        // this binary shares — without the lock these race each other.
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        hydrated(&root, "oldest", "observed", 1_000, 100);
        hydrated(&root, "middle", "observed", 1_000, 200);
        hydrated(&root, "newest", "observed", 1_000, 300);
        // 3000 bytes present, budget 2500: exactly one file must go.
        std::env::set_var("DEJA_ARTIFACT_CACHE_MAX_BYTES", "2500");
        super::sweep_artifact_cache(&root, "");
        assert!(
            !present(&root, "oldest", "observed"),
            "the oldest is evicted"
        );
        assert!(
            present(&root, "middle", "observed"),
            "and the sweep then stops"
        );
        assert!(present(&root, "newest", "observed"));
    }

    /// The run being served survives even when it is the oldest thing there.
    ///
    /// `hydrate_run_artifacts` writes then sweeps, so without this the very
    /// files a view just downloaded could be deleted before it reads them, and
    /// the view would render empty on a cache that was merely full.
    #[test]
    fn the_run_being_served_is_never_evicted() {
        // The budget is read from the process environment, which every test in
        // this binary shares — without the lock these race each other.
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        hydrated(&root, "serving", "observed", 4_000, 1);
        hydrated(&root, "other", "observed", 1_000, 999);
        std::env::set_var("DEJA_ARTIFACT_CACHE_MAX_BYTES", "500");
        super::sweep_artifact_cache(&root, "serving");
        assert!(
            present(&root, "serving", "observed"),
            "the served run is kept"
        );
        assert!(
            !present(&root, "other", "observed"),
            "others go to make room"
        );
    }

    /// Every hydrated kind is swept, not only the big one. A kind added to
    /// `local_path_for_artifact_kind` joins the sweep by existing.
    #[test]
    fn every_hydrated_kind_is_swept() {
        // The budget is read from the process environment, which every test in
        // this binary shares — without the lock these race each other.
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        for kind in [
            "observed",
            "http_diffs",
            "lookup_table",
            "scorecard",
            "call_ledger",
            "record_graph",
        ] {
            hydrated(&root, "run-x", kind, 1_000, 100);
        }
        std::env::set_var("DEJA_ARTIFACT_CACHE_MAX_BYTES", "1");
        super::sweep_artifact_cache(&root, "");
        for kind in [
            "observed",
            "http_diffs",
            "lookup_table",
            "scorecard",
            "call_ledger",
            "record_graph",
        ] {
            assert!(!present(&root, "run-x", kind), "{kind} was not swept");
        }
    }

    /// Zero disables the sweep, so a deployment can turn it off without editing
    /// the image.
    #[test]
    fn a_zero_budget_disables_the_sweep() {
        // The budget is read from the process environment, which every test in
        // this binary shares — without the lock these race each other.
        let _lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        hydrated(&root, "run-a", "observed", 10_000, 100);
        std::env::set_var("DEJA_ARTIFACT_CACHE_MAX_BYTES", "0");
        super::sweep_artifact_cache(&root, "");
        assert!(present(&root, "run-a", "observed"), "zero means no ceiling");
    }

    /// The substring trap, and why the predicate anchors. The custom
    /// deployment's pod name CONTAINS the main deployment's name, so the
    /// `contains` rule its neighbour `instance_pattern` uses would admit exactly
    /// what this filter exists to exclude.
    #[test]
    fn main_deployment_match_is_anchored_not_a_substring() {
        const MAIN: &str = "sbx-hyperswitch-server-";
        let custom = landed(
            "run-1788680145151199733",
            "2026-09-07",
            &["sbx-custom-cug-hyperswitch-server-54d4746479-d2qr7"],
        );
        let main = landed(
            "rec-4157177-09071400-xc",
            "2026-09-07",
            &["sbx-hyperswitch-server-66d5b699fc-xx5tg"],
        );

        // The trap, stated exactly. A custom pod is NOT named
        // `sbx-hyperswitch-server-…` — so the anchored prefix rejects it on the
        // `sbx-` boundary. What it does contain is the bare service name
        // `hyperswitch-server`, and a bare service name is precisely the shape
        // `instance_pattern` takes elsewhere in this document (prism declares
        // `"ucs"`). So the substring rule that answers "which system minted
        // this" would answer "yes, main" here, and the anchor is what keeps the
        // two questions apart.
        assert!(
            custom.instances[0].contains("hyperswitch-server"),
            "precondition: a bare service-name pattern really does match a custom pod"
        );
        assert!(
            !custom.instances[0].starts_with(MAIN),
            "precondition: and the anchored prefix really does not"
        );

        assert!(super::from_main_deployment(&main, MAIN));
        assert!(
            !super::from_main_deployment(&custom, MAIN),
            "a custom deployment is not the main one however its name reads"
        );
    }

    /// `all` over an empty list is TRUE, so emptiness has to be its own
    /// assertion and has to come first. Without it, a recording whose `inst=`
    /// partitions the scan could not read passes the main-deployment filter —
    /// admitted precisely because nothing is known about it.
    #[test]
    fn a_recording_with_no_instances_is_not_from_the_main_deployment() {
        const MAIN: &str = "sbx-hyperswitch-server-";
        let unknown = landed("rec-4157177-09071400-xc", "2026-09-07", &[]);

        assert!(
            unknown.instances.iter().all(|i| i.starts_with(MAIN)),
            "precondition: the vacuous `all` really does pass on an empty list"
        );

        assert!(!super::from_main_deployment(&unknown, MAIN));
    }
}
