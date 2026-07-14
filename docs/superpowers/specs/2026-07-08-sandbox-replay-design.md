# Sandbox Replay Platform — Design

Date: 2026-07-08
Status: approved-in-discussion, pending spec review

## 1. Overview

Move Déjà's per-run replay execution out of the orchestrator host process and
into per-run Kubernetes sandboxes deployed by Helm. The dashboard
(deja-orchestrator) becomes a thin, always-on control plane: it creates a
sandbox per run, watches it, and tears it down. Everything else — pulling the
recording from S3, constructing lookup tables, driving the router, scoring
divergence, publishing results — happens inside the sandbox, performed by a new
**replay agent**.

Goals:

- **Run independence.** Each run owns a focused stack (router, redis,
  postgres, superposition) in its own namespace. N runs execute
  concurrently with zero shared mutable state except S3.
- **Per-request lookup delivery.** The lookup table is built per request
  (correlation) and pushed into the router's in-memory cache (IMC) immediately
  before that request is driven, then cleared immediately after — replacing
  today's whole-recording file loaded once at router boot.
- **Self-contained sandboxes.** The dashboard's involvement per run is
  install → watch → uninstall. If the dashboard restarts mid-run, sandboxes
  keep running and results are recoverable from S3.
- **Wall-clock reduction** through sandbox-level parallelism and S3-delivered
  candidate builds (no per-run image builds).

Non-goals (this iteration):

- Parallel workers *within* a sandbox. Requests replay serially, in record
  order. Concurrency is achieved across sandboxes.
- Record-mode runs in sandboxes. Recording keeps its current pipeline
  (HS → Kafka → Vector → S3). This design covers replay runs.
- Retiring the docker-compose demo path. It remains the local-dev loop;
  file-based lookup (`DEJA_LOOKUP_TABLE`) stays the default outside sandboxes.

## 2. Architecture & Topology

```
┌─ dashboard (always-on, docker compose) ─────────────────────────┐
│  deja-orchestrator container:                                   │
│   · axum API + embedded SPA (:8070)                             │
│   · helm-sandbox replay driver (helm + kubectl CLIs in image,   │
│     kubeconfig mounted read-only)                               │
│  postgres:17 container (run state, stages, logs, audit)         │
└──────────────┬──────────────────────────────────────────────────┘
               │ per run:  helm install deja-run-<id> -n deja-run-<id>
               ▼
┌─ k8s namespace deja-run-<id> (one per run, N in parallel) ──────┐
│  chart services: router (DEJA_MODE=replay, DEJA_LOOKUP_MODE=imc)│
│    redis · postgres · superposition                             │
│  replay-agent (k8s Job):                                        │
│    S3 pull → per-request loop → score → S3 upload → verdict POST│
└──────────────┬──────────────────────────────────────────────────┘
               │ verdict callback (bearer token)
               ▼
     dashboard closes run → helm uninstall → namespace deleted
```

Topology decisions:

1. **Dashboard is a two-container docker compose stack** (`dashboard` + `pg`),
   `restart: always`. New `Dockerfile.orchestrator`: multi-stage cargo build →
   `debian:trixie-slim` with `helm` and `kubectl` installed. Kubeconfig mounted
   read-only. Web UI stays embedded via rust-embed (single deployable image).
2. **Sandbox = one Helm release in its own namespace**, both named
   `deja-run-<runid>`. Namespace deletion is the cleanup backstop independent
   of Helm state.
3. **Replay agent is a k8s Job** (`backoffLimit: 0`), not a service. Job
   status is the coarse fallback signal when the verdict callback is missing.
4. **S3 is the only shared data plane.** Recordings are read from it;
   artifacts, scorecards, and candidate builds live in it.
5. **One dashboard↔agent API contract:** `PATCH /api/v1/runs/{id}/stage`
   (progress) and `POST /api/v1/runs/{id}/verdict` (result), authenticated via
   the existing `DEJA_API_SERVICE_TOKEN` bearer + `X-Deja-Actor` middleware.

## 3. Components

### 3.1 `deja-replay-core` (new library crate)

Shared foundation linked by both the dashboard and the agent:

- **Config module.** TOML-first configuration (section 7). The `[s3]` table
  deserializes into one `S3Config` struct used by both sides.
- **S3 layout module.** Every S3 key template in the codebase lives here and
  nowhere else:
  - `recording_session(recording_id)` → `{prefix}/sessions/v1/{recording_id}/…`
  - `run_artifact(run_id, name)` → `{prefix}/runs/{run_id}/{name}`
  - `candidate_build(build_ref)` → `{prefix}/builds/{build_ref}/router`
- **Relocated logic** (moved from `deja-orchestrator`, re-exported to keep it
  building): recording ingest (`s3::pull_recording` + collate), lookup
  rendering (`render_lookup_table`) plus a new per-correlation filter, seed
  planning, divergence scoring.

### 3.2 `deja-replay-agent` (new binary crate + container image)

The in-sandbox brain. Configured by `agent.toml`. Phases:

1. **Ingest:** pull the sealed session from S3 (compact first if unsealed),
   collate to `events.jsonl`, group by correlation, order by
   `global_sequence`.
2. **Ambient setup:** push null-correlation lookup entries to the router IMC's
   ambient partition (never cleared); seed ambient store keys once.
3. **Per-request loop** (serial, record order) — see section 4.
4. **Score & publish:** `detect_and_score` over lookup ∪ observed ∪ http
   diffs → scorecard + call ledger; upload artifacts to
   `{prefix}/runs/{run_id}/`; POST verdict to the dashboard.

Store access is via direct clients — `fred` (redis) and `tokio-postgres`
(pg) — replacing the orchestrator's `docker compose exec redis-cli/psql`
shell-outs. Request driving reuses `deja-kernel`'s
reconstruct/drive/compare as a library.

### 3.3 `deja-runtime`: IMC lookup store + admin surface

- `ImcLookupStore`: `RwLock<HashMap<CorrelationId, CorrelationPartition>>`.
  A partition holds that correlation's `LookupEntry` set **and** its
  occurrence counters, so clearing a correlation atomically drops both. One
  reserved ambient partition holds null-correlation entries.
- Selected by `DEJA_LOOKUP_MODE=imc`; the existing file mode
  (`DEJA_LOOKUP_TABLE`) remains the default. `runtime_hook_from_env` builds
  the IMC-backed `LookupTableSource` variant in imc mode.
- Router admin surface (handlers provided by the deja facade crate; the
  Hyperswitch deja branch mounts them **only when `DEJA_MODE=replay`**):
  - `POST /deja/lookup/{correlation}` — install entries for a correlation
  - `DELETE /deja/lookup/{correlation}` — drop entries + counters
  - `GET /deja/observed/{correlation}` — drain that correlation's
    `ObservedCall` buffer (replaces the file sink for sandbox runs)
  - `GET /deja/lookup/health` — replay-readiness probe

### 3.4 Orchestrator: `helm-sandbox` replay driver

A third lifecycle arm next to compose record/replay, selected by
`[sandbox]` config presence (or `DEJA_SANDBOX=helm`). Per replay run:

1. Render `values-run.yaml` (run_id, recording_id, candidate spec, S3 secret
   values, callback URL + token).
2. `helm install deja-run-<id> <chart> -n deja-run-<id> --create-namespace
   -f values-run.yaml` via the existing `run_streamed` (stage logs land in
   the dashboard as with compose runs).
3. Wait for the verdict callback; watchdog polls the agent Job every
   `watchdog_interval_secs` with an overall `run_deadline_secs`.
4. On terminal state: `helm uninstall` + `kubectl delete namespace`.

The `DEJA_REPLAY_*` hook system (prepare/collect/cleanup commands) remains as
an escape hatch for bespoke environments; the helm driver is the first-class
path.

### 3.5 Helm chart

Extends the hyperswitch chart work: a `dejaReplay` values block (runId,
recordingId, s3 secret ref, callback), a `replay-agent` Job template, router
env (`DEJA_MODE=replay`, `DEJA_LOOKUP_MODE=imc`). The ConfigMap-mounted
whole-recording lookup table is **removed** — IMC push replaces it. Consumer,
producer, and drainer stay disabled for replay.

### 3.6 Candidate builds

`CandidateSpec` gains a variant:

```
LocalPath     { binary_or_source }   // local dev (existing)
PrebuiltImage { image }              // existing
S3Build       { build_ref }          // NEW: router binary at builds/<ref>/router
```

For `S3Build`, the router pod runs an initContainer that downloads
`{prefix}/builds/{build_ref}/router` into a shared `emptyDir`, marks it
executable, and the main container (generic slim base image) executes it. No
per-run image builds or registry pushes; publishing a candidate is an S3
upload.

### 3.7 Dashboard image + API additions

- `Dockerfile.orchestrator` and `demo/docker-compose.dashboard.yml`
  (dashboard + pg, `restart: always`, kubeconfig + `dashboard.toml` mounts).
- New endpoints: `POST /api/v1/runs/{id}/verdict`,
  `PATCH /api/v1/runs/{id}/stage` — both idempotent (run_id-keyed upserts),
  both behind the existing mutation-auth middleware.
- Artifact reads fall back to S3 (via the layout module) when the local file
  is absent, so run pages work for sandbox runs.

## 4. The Per-Request Cycle

For each correlation, in record order, strictly serial:

```
render        build this correlation's LookupEntry set from events.jsonl
PUSH IMC      POST /deja/lookup/{corr}  { entries: [...] }
seed          redis keys under "{corr}:" prefix (fred)
              pg rows into schema "<corr>" (tokio-postgres; schema created
              per correlation, router routes via search_path)
drive         reconstructed request, x-request-id = corr (router adopts it
              via IdReuse::UseIncoming); diff response vs recorded baseline
collect       GET /deja/observed/{corr} → append to observed.jsonl
CLEAR IMC     DELETE /deja/lookup/{corr}   (entries + occurrence counters)
CLEAR stores  redis: SCAN "{corr}:*" → UNLINK
              pg:    DROP SCHEMA "<corr>" CASCADE
```

Notes:

- **Store cleanup removes both seeds and writes** the request made while being
  driven — nothing carries into the next request. Namespacing already prevents
  cross-request collisions; the wipe adds bounded resource growth and turns
  any namespacing bug into a loud missing-seed error instead of silent
  contamination.
- **Ambient state** (null-correlation lookup entries; un-namespaced config
  keys such as `settlement_rate_default`) is installed once at agent start
  and never cleared.
- Serial driving preserves record order, which keeps null-correlation
  occurrence sequencing valid (the same invariant today's kernel maintains by
  sorting correlations by `global_sequence`).
- Liveness probes recorded as `/health` correlations are skipped, as in the
  current kernel.

## 5. Data Flow (one run, end to end)

1. **Create:** UI/API POST → run row (`Pending`) + audit → helm driver spawns.
2. **Install:** values rendered; `helm install`; status `Building`.
3. **Boot:** chart brings up pg (+migrations), redis, superposition, router.
   Agent Job init-waits on router `/health` +
   `/deja/lookup/health` (deadline: `health_deadline_secs`).
4. **Replay:** agent runs sections 3.2/4. Progress PATCHes stream to the
   existing stage-history table (live progress bar unchanged). Status
   `Running` from the first progress PATCH.
5. **Publish:** artifacts to `{prefix}/runs/{run_id}/`: `scorecard.json`,
   `call-ledger.json`, `observed.jsonl`, `http-diffs.jsonl`,
   `seed-certificate.json`, `agent.log`. Then verdict POST.
6. **Close:** dashboard records verdict + S3 artifact URIs, marks run
   terminal, `helm uninstall`, namespace delete.

## 6. Error Handling

Principle: the dashboard always converges to a terminal state; the namespace
always dies.

| Failure | Behavior |
|---|---|
| `helm install` fails | run `Failed` with streamed stderr; best-effort namespace delete |
| Sandbox never healthy | agent init-wait deadline → agent exits non-zero → Job `Failed` → watchdog closes run |
| Agent errors mid-run (S3, seed, drive, IMC) | upload partial artifacts + `agent-error.json`; POST `inconclusive` verdict with reason; exit non-zero. A scored divergence is a normal `fail` verdict, not an error |
| Callback unreachable | agent retries 5× with backoff; artifacts already in S3; watchdog recovers |
| No callback by deadline | watchdog reads `runs/<id>/scorecard.json` or `agent-error.json` from S3 and closes the run; if neither exists, run `Failed` (reason: no result) |
| Dashboard restart mid-run | run state in pg; on boot, re-adopt non-terminal runs and resume watching their Jobs |
| Teardown leaks | janitor on dashboard boot deletes any `deja-run-*` namespace whose run is terminal or unknown |
| Per-request cleanup fails | log + continue (namespacing still isolates); recorded in the seed certificate |

Idempotency: verdict POST and stage PATCH are run_id-keyed upserts; duplicate
delivery after watchdog recovery is a no-op. Agent Job `backoffLimit: 0` — a
failed run is failed; rerunning is a user action.

## 7. Configuration (TOML-first)

Both binaries take `--config <path>` (or `DEJA_CONFIG`). Env vars override
individual keys (`DEJA_S3__ACCESS_KEY=…`) for secrets and local tweaks.
Missing required keys produce a startup error listing exactly what is absent.

**`config/dashboard.toml`:**

```toml
[api]
bind = "0.0.0.0:8070"
state_dir = "/harness-state"

[database]
url = "postgres://deja:deja@pg:5432/deja"

[s3]
region = "us-east-1"
access_key = "…"
secret_key = "…"
bucket = "deja-recordings"
prefix = "deja/v1"
endpoint = "http://minio:9000"   # omit for AWS

[sandbox]
chart = "/charts/replay-sandbox"
namespace_prefix = "deja-run-"
run_deadline_secs = 1800
watchdog_interval_secs = 30
# host address pods can reach the dashboard on (see note under agent.toml)
callback_base_url = "http://host.k3d.internal:8070"
```

**`agent.toml`** (rendered by the chart from values; secrets from a k8s
Secret):

```toml
[run]
run_id = "…"
recording_id = "…"

[s3]        # identical shape/struct to the dashboard's [s3]

[router]
base_url = "http://router:8080"
lookup_admin = "http://router:8080/deja/lookup"

[stores]
redis_url = "redis://redis:6379"
pg_url = "postgres://…@postgres:5432/hyperswitch"

[callback]
url = "http://dashboard:8070/api/v1/runs/run-…/verdict"
token = "…"

[limits]
health_deadline_secs = 300
request_timeout_secs = 30
```

Note on `callback.url`: the agent runs inside the cluster while the dashboard
runs in docker compose on the host, so the URL must be a host address
reachable from pods (e.g. `host.k3d.internal` / `host.minikube.internal` for
local clusters, a routable address in shared clusters). The dashboard driver
fills this value into the rendered chart values from `[sandbox]` config
(`callback_base_url`), so it is set in exactly one place.

## 8. Testing

- **`deja-replay-core` unit tests:** TOML parsing + env-override precedence,
  S3 layout templates, per-correlation table filtering, seed/teardown command
  rendering. Pure, no I/O.
- **`deja-runtime` IMC tests:** push/lookup/clear cycle, counter reset on
  clear, ambient partition survives clears, concurrent-correlation isolation
  (same style as existing `LookupTableHook` tests).
- **Agent integration test:** full loop against a stub router (tiny axum
  server implementing the `/deja/*` admin surface and echoing driven
  requests) + MinIO + redis + pg via testcontainers. Asserts zero residue
  after each request (schema dropped, keys gone, IMC empty).
- **Dashboard contract tests:** verdict/stage endpoints (auth, idempotency)
  and the watchdog S3 read-through recovery path.
- **E2E (`demo/run-deja-sandbox.sh`):** one real sandbox run on a local
  cluster (k3d/minikube) with a small recording; CI-gated.

## 9. Build Order

1. `deja-replay-core`: config + S3 layout + relocations (everything else
   depends on it).
2. `deja-runtime` IMC store + admin handler surface (+ facade exports).
3. `deja-replay-agent` binary + image (stub-router integration tests).
4. Helm chart changes (agent Job, router env, init-container candidate
   fetch).
5. Orchestrator helm-sandbox driver + verdict/stage endpoints + S3 artifact
   read-through.
6. Dashboard Dockerfile + compose + `demo/run-deja-sandbox.sh` E2E.

Hyperswitch-side work (mounting the admin routes on the deja branch) happens
in the vendored tree alongside step 2's facade exports.

## 10. Resolved Decisions (log)

- Per-request IMC push/clear, not whole-table-at-boot (user decision).
- Serial replay within a sandbox; concurrency across sandboxes (user
  decision).
- Scoring in-sandbox; artifacts + scorecard to S3; verdict via callback
  (user decision, option A).
- Store rows/keys cleared between requests, ambient state resident (user
  decision).
- S3 config centralized (region/access key/secret/bucket/prefix) with a
  single layout module; candidate builds downloadable from S3 for
  non-local-dev (user decision).
- All service configuration in TOML files with env overrides (user decision).
- Observed calls returned via HTTP drain endpoint, not shared volume
  (agent/router may be on different nodes; keeps the chart storage-free).
