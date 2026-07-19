# Replay orchestrator: k8s Job executor (compose kept for local dev)

**Status:** design, ratified direction; implementation-shape leans flagged below.
**Date:** 2026-07-10.
**Constraint (hard):** the vendor is FROZEN — the candidate router image is the Jenkins build of
`juspay/hyperswitch:deja-pr` @ `ff191d7f79`. No vendor code change, no deja-lib repin, no Jenkins
rebuild. Everything here is outer-repo work + runtime config.

## User-ratified decisions (2026-07-10)
1. **Direct k8s API** from the orchestrator (kube client creating Jobs) — no runner-claim
   protocol yet. The Job document stays protocol-shaped JSON so the pull-based runner design
   (`replay-pipeline.md` §3) can absorb it later without rework.
2. **Progress push-back**: the in-Job runner POSTs stage/log/artifact/completion to the
   orchestrator API, authenticated with the existing service token. The k8s watch is only a
   liveness backstop (Job failed/deadline → mark run failed if no push-back said so).
3. **Compose stays** as the local-dev executor behind the same seam (explicit one-time waiver of
   the no-legacy-compat policy: the compose demo is still the validation rig).

## What exists (verified in code, 2026-07-10)
- `deja-orchestrator` is already the control plane: axum `/api/v1` (create/list runs, stages,
  logs, artifacts, scorecard, SSE stream, audit), embedded dashboard SPA, `DEJA_API_SERVICE_TOKEN`
  mutation auth, Postgres run store, S3 ingest pod-ready (AWS credential chain — `29fab85a`).
- The lifecycle worker is the non-cluster part: in-process thread (`api/runs.rs:40`) driving
  `lifecycle/mod.rs` — compose shell-outs + host `deja-kernel` binary.
- Replay stages (`drive_replay`, lifecycle/mod.rs:566): (1) S3 pull → (2) render lookup →
  (3) compose_up router → (4) wait_health → (5) flush+seed stores, run kernel → (6) score.
  Stages 1, 2, 6 are pure outer logic — reusable in-pod unchanged.

## Seam analysis — the coupling is transport, not logic
Three seams, smallest-change-first:

### S1. Store transport (`StoreExec`)
Every store interaction is `docker compose exec -T <service> <cli> …` where the *logic* depends
only on the CLI protocol: `redis-cli SET/EXISTS/--raw GET` (seed_redis_image :911,
readback_redis :957, redis_cli_output :1022, flush_redis :834) and `psql` (seed_db :1425,
create_db_schema :1962, load_db_catalog :1851, readback :1552). Seam = a small enum that yields a
prepared `Command`:

```rust
enum StoreExec {
    Compose { /* demo compose args + env */ },          // docker compose … exec -T redis-standalone redis-cli …
    Direct  { redis_host: String, database_url: String } // redis-cli -h …  /  psql <url>
}
impl StoreExec { fn redis_cli(&self, args) -> Command; fn psql(&self, args) -> Command; }
```

**Lean (a):** keep the CLI binaries (`redis-cli`, `psql` in the runner image) rather than adding
Rust redis/pg client deps — output parsing (`strip_redis_cli_terminator`, pg bool parsing) and the
demo-validated seeding semantics carry over byte-for-byte.

### S2. Progress reporting (`StoreCtx`)
`StoreCtx` (lifecycle/store_ctx.rs) is already the narrow reporting surface (9 methods: log,
stage, finish, run_state, run_recording, candidate_sha, result, recording, artifact), best-effort,
Option-gated. Seam = make `Inner` a two-variant enum:

```rust
enum Inner {
    Pg   { handle, store, log_seq },                  // today, unchanged
    Http { base_url, token, client, log_seq },        // in-Job runner → POST /api/v1/…
}
```
Public method signatures identical ⇒ zero churn at the ~40 lifecycle call sites, and the dashboard
SSE stream works identically for compose and k8s runs.

**Lean (b):** in-Job artifacts (scorecard, seed certificate, http diff, kernel logs) upload to S3
(the Job already has IRSA S3 access) and register by S3 URI; the orchestrator's
`GET /artifacts/{id}/raw` learns to stream `s3://` URIs alongside local paths. Pod-local files die
with the pod — S3 is the only durable home.

### S3. Run executor (orchestrator side)
Where `api/runs.rs::spawn_worker` spawns the thread today:

```rust
trait RunExecutor { fn launch(&self, root, run_id, ctx); }
ComposeExecutor  // today's thread + lifecycle::drive  (local dev)
K8sJobExecutor   // render Job manifest from run spec + boot contract → create via k8s API
                 // → watch Job status as liveness backstop (progress comes via push-back)
```
**Lean (c):** selection via `DEJA_EXECUTOR=compose|k8s` env on the orchestrator deployment.
**Lean (d):** kube-rs for the client; manifest rendered from a template embedded in the
orchestrator, parameterized by the boot contract (below).

### Kernel: no seam needed
`run_kernel` (:2431) is already a plain `Command::new(kernel_bin)` + env with
`KERNEL_TARGET_HOST=127.0.0.1` — in-pod the containers share the network namespace, so it runs
unchanged from the runner container (kernel binary ships in the runner image).

## Job pod shape
| container | image | role |
|---|---|---|
| router (candidate) | frozen Jenkins image | replay target; lookup dir via shared emptyDir |
| pg sidecar | stock postgres | migrated by runner; schema-per-corr by seeder |
| redis sidecar | stock redis | seeded under `{corr}:` namespaces |
| runner | outer-repo image (the only build) | S3 pull → compact → render lookup → migrate → seed → kernel → score → push-back |

Per-Job sidecar stores are FORCED by the vendor freeze: sharing cluster stores would need a
run-id in the replay namespace, which lives in the deja lib the vendor pins by rev
(`replay_key_namespace()`) — a repin = Jenkins rebuild. Sidecars need zero code change anywhere.

## Router container boot contract
Owned by the OMP pane → `docs/design/replay-pod-boot-contract.md` (in flight). Verified so far:
- `Settings::validate()` validates superposition config UNCONDITIONALLY
  (vendor settings.rs:1490 → endpoint non-empty valid URL, token, org_id non-empty).
- App-state build calls `get_superposition_client().await.expect(…)` — boot PANICS on client-init
  failure (vendor routes/app.rs:534).
- Escape hatch in the frozen code: `superposition.backup_file_path`
  (env_specific.toml:515, default `./config/superposition_seed.toml`) — file fallback when the
  HTTP service is unavailable. OMP is verifying it covers init-time unreachability; if yes, the
  pod needs shape-valid superposition config + a bundled seed file, no live service.

## Work plan (tasks #18–#21)
1. **#18 seam extraction**: introduce `StoreExec` + `StoreCtx::Inner` enum + `RunExecutor` trait;
   compose paths keep byte-identical behavior (fast gate: existing demo run green).
2. **#19 push-back endpoints**: POST stage/log/artifact/result/complete under service-token auth,
   feeding the same `Store` writes `StoreCtx::Pg` does today.
3. **#20 RunSpec extension**: S3 source override `{bucket, prefix, region?}`, `correlation_filter`
   (flows into lookup render + kernel drive list), implement `CandidateSpec::PrebuiltImage`.
4. **#21 in-pod runner**: `deja-runner` binary = `drive_replay` minus stage 3 (router is a pod
   container; wait_health against localhost) with `StoreExec::Direct` + `StoreCtx::Http`;
   runner image = runner + kernel + redis-cli + psql + migrations for the pinned sha
   (migration mechanism per OMP's contract doc).
5. k8s manifests + IRSA (`ops/` per packaging.md) once the boot contract lands.

## Open questions (non-blocking, surfaced for the user)
- Leans (a)–(d) above — objections welcome, otherwise proceeding as stated.
- Correlation filter semantics: filter at lookup-render (smaller lookup table, replay sees only
  chosen cases) vs at kernel drive-list (full table, drive subset). Lean: kernel drive-list —
  keeps the lookup table identical to the recording and the filter purely a driver concern.
- Job parallelism: N Jobs = N independent runs already works (per-Job stores). Parallelism WITHIN
  a run stays bounded by the serial kernel (replay-harness-kernel/src/main.rs:103) — separate,
  pre-existing track.
