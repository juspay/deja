# Replay-Orchestrator Delivery Plan — deployable, scalable, usable

**Status:** the single ratified execution plan. Supersedes and absorbs the five area drafts
(`incluster-deployment-plan.md` Phases 1–3, `replay-runtime-resolution.md`,
`replay-pod-boot-contract.md`, `replay-env-contract.md`,
`replay-orchestrator-k8s-executor.md`). Where those disagree, **this document is
authoritative** — the reconciliations are listed in §2.
**Date:** 2026-07-11. **Monday deliverable:** the `hyperswitch-infra` PR + the named infra-team ask list (§9, §10).

Legend: **[DECIDED]** ratified direction · **[BUILT]** exists and works today · **[GAP]** must be written this cycle · **[TRAP]** exists and will mislead you.

---

## 1. What we are building, and the two-input promise

A user triggers a replay from the dashboard with exactly **two strings**:

1. a **recording** — an S3 path (`s3://hyperswitch-art/2026/07/11`) or a catalog id, and
2. a **candidate** — a branch / PR# / short-sha / tag / full image ref.

From that pair the system derives everything else. A replay run is
`derive(R, C, env_profile)`: `R` is the recording's own code ref (in the tape), `C` is the
candidate image, `env_profile` is the replay-env package. The orchestrator resolves the two
refs, runs a **fail-closed preflight**, and — in cluster mode — enqueues a durable row that a
dispatcher turns into **one Kubernetes Job per run** (per-Job pg + redis + frozen-router native
sidecars + a runner container), talking to the k8s API by **raw REST over ureq/rustls with the
pod ServiceAccount token** (no kube-rs, no kubectl, no DinD). Progress returns by push-back; a
single stateless reconcile loop is the liveness backstop and orphan reaper. Results live in
Postgres so nothing evaporates on a pod restart.

Three planes (Amendment 1, **[DECIDED]**):

- **Control plane** — the orchestrator Deployment. Accepts the two strings, resolves refs,
  preflights, enqueues, dispatches, tracks, renders reports. Knows nothing about *how* the env
  is built.
- **Environment plane** — a `replay-env` helm package per source env (`sbx-mirror` today). The
  Job manifest lives in a ConfigMap; the orchestrator applies a **typed patch** (image, named
  env, labels) and POSTs. An env change is an infra PR, never an image rebuild.
- **Data plane** — the Job pod.

**North-star invariant (local == cluster):** local and cluster execute the *byte-identical*
Rust path — `deja-runner` → `drive_replay_in_pod` → `StoreExec::Direct` — fed the identical
typed env. The only thing allowed to differ is the *provisioning* primitive (compose/host vs
k8s Job). This disqualifies the pure-compose demo (`drive_replay`/`StoreExec::Compose`) as a
parity vehicle; it is retained only as a fixture **producer**.

**The single riskiest unknown** is whether the sandbox EKS control plane is **≥ 1.29**: the
whole Job shape relies on native sidecars (initContainers with `restartPolicy: Always`) so the
never-exiting pg/redis/router auto-terminate when the runner exits. On an older cluster the pod
never Completes and the manifest shape must be redesigned. **Confirm O1 before hardening any Job
shape.**

---

## 2. Cross-area reconciliations this plan locks [DECIDED]

The five drafts overlap; a senior engineer must not build the same seam twice or pick the wrong
name. These are the binding decisions.

**R1 — `DEJA_EXECUTOR` is a four-value knob**, resolving the three drafts that each defined it
differently:

| value | create-time behaviour | who drives the run | store | use |
|---|---|---|---|---|
| `compose` *(default)* | `spawn_worker` inline (`ComposeExecutor`, current body verbatim) | in-process compose worker (`drive_replay`, `StoreExec::Compose`) | optional | demo / local dev; **only Record-capable path** |
| `external` | persist run row + audit, **suppress** the worker | an external process (`run-deja-local-pod.sh` driving `deja-runner`) | optional | Tier-1 local pipeline (host-native runner, in-pod path) |
| `local` | **enqueue** durable `queued` row, return 202 | the scheduler dispatcher → `LocalProcessBackend` (spawns `deja-runner` as a child) | **mandatory** | scheduler/scale testing without k8s |
| `k8s` | **enqueue** durable `queued` row, return 202 | the scheduler dispatcher → `K8sJobBackend` (typed patch → POST) | **mandatory** | production cluster |

`compose` stays the default so **every intermediate commit leaves the orchestrator deployable**.

**R2 — Two seams at two levels, not one.** The draft "`RunExecutor` trait at `spawn_worker`"
(SPINE-2/C2) and the scheduler's "`JobBackend` trait" (scale S4) are different levels and both
exist:
- *Create-time seam* (`v1_create_run` → `spawn_worker`): routes by `DEJA_EXECUTOR`. `compose`
  → `ComposeExecutor` (verbatim). `external` → persist-only. `local`/`k8s` → `enqueue`.
- *Dispatch-time seam* (`JobBackend { launch, list, delete }`): the dispatcher claims a queued
  row and calls `launch`. `K8sJobBackend::launch` **is** the #27 spine (resolve → preflight →
  GET ConfigMap → typed patch → POST). `LocalProcessBackend`/`NullBackend` for local/tests.
- The K8s **launch** must `std::thread::spawn` FIRST (no blocking ureq on the async worker) and
  do all network I/O inside that thread.

**R3 — ONE stateless reconcile loop.** The scheduler's `reconcile_loop`
(one `backend.list()` × one `store.list_scheduled()` → pure `reconcile_decisions`) **subsumes**
the draft's per-Job poll threads *and* the separate startup reconciler (SPINE-8, base-plan C5).
There is exactly one loop; restart-survival is by construction because its entire input is
durable. Do not also build per-launch poll threads.

**R4 — ONE `resolve` module.** Merge the two `resolve.rs` proposals into a single
`crates/deja-orchestrator/src/resolve.rs`:
- candidate-string shape → `CandidateSpec` → image (config-surface CS1/CS5),
- recording sha from the `SessionManifest`/envelope probe (CS3 / SPINE-4),
- CodeBundle `fingerprints.json` fetch (SPINE-4).
Preflight and the executor both consume it. (Not `lifecycle/resolve.rs`; keep it crate-level so
the HTTP handler and the local runner CLI share it.)

**R5 — Canonical identity table [DECIDED]** (AREA-5 supersedes base-plan C5 *same-namespace* and
the earlier `replay-orchestrator-job-role` name). An IRSA trust subject that does not byte-match
the running pod's `ns:sa` is denied and S3/SecretsManager fail closed, so this table is load-bearing:

| plane | namespace | ServiceAccount | IRSA role | trust subject | policy |
|---|---|---|---|---|---|
| control (orchestrator Deployment) | `replay-orchestrator-sandbox` *(keep existing Argo destination)* | `replay-orchestrator-role` | `sandbox-hyperswitch-replay-orchestrator-role` | `system:serviceaccount:replay-orchestrator-sandbox:replay-orchestrator-role` | S3 **RW** on `hyperswitch-art` |
| env (Jobs) | `replay-sbx` *(dedicated, Amendment 1)* | `replay-job-role` | `sandbox-hyperswitch-replay-job-role` | `system:serviceaccount:replay-sbx:replay-job-role` | S3 **read** on `hyperswitch-art` + SecretsManager read on `sandbox/hyperswitch*` (+ `kms:Decrypt` if CMK) |

Orchestrator→Jobs RBAC is a **cross-namespace** Kubernetes grant: Role `replay-job-manager` in
`replay-sbx` + a RoleBinding in `replay-sbx` whose subject is the orchestrator SA in
`replay-orchestrator-sandbox`. Therefore `DEJA_JOB_NAMESPACE=replay-sbx` and
`DEJA_JOB_SERVICE_ACCOUNT=replay-job-role`. The old chart `role.yaml`/`rolebinding.yaml` bind in
`.Release.Namespace` and satisfy neither model — they move into the replay-env package.

**R6 — Preflight P0..P9 and the seeding parity gates are the same assertions from two entry
points.** Orchestrator-side (before POST, refusal is free): P0 P1 P3 P4 P5 P7 P8 P9. In-pod
(needs the live DB, after migrate/before seed): P2 P6. The seeding area's `ReplayParity`
(crypto epoch P3, redis prefix P5, schema head P2, NOT-NULL skew P6) is the in-pod **carrier**;
`preflight.rs` computes fingerprints and injects `DEJA_PARITY_*` into the Job. `preflight.rs`
(orchestrator) and `parity.rs` (runner) are complementary, both reading CodeBundle
`fingerprints.json`.

**R7 — Push-back URL is chart-rendered, never compiled in.** `DEJA_ORCHESTRATOR_CALLBACK_URL =
http://sandbox-replay-orchestrator.replay-orchestrator-sandbox.svc.cluster.local:80` (fullname
helper, **port 80** = the Service port; targetPort 8080 is not answerable on the ClusterIP).
The executor copies it verbatim into the Job's `DEJA_ORCHESTRATOR_URL`; the runner appends
`/api/v1/runs/{id}/events`. This fixes the verified 3-axis push-back bug (name, namespace, port).

**R8 — One shared-mount constant.** `SHARED_MOUNT = /deja/work` is the single source of truth
for A3: the runner's `HARNESS_STATE_DIR`, the router's `REPLAY__SOURCE`
(`/deja/work/lookup-tables/${RUN_ID}.jsonl`) and `OBSERVED_SINK`
(`/deja/work/observed/${RUN_ID}.jsonl`) all derive from it. The runner default moves from
`/workspace/state` to `/deja/work`. The runner asserts at boot that `lookup_table_path(run_id)`
is under `HARNESS_STATE_DIR` and fails loud otherwise (P9 in-pod).

---

## 3. State of the world (what is BUILT vs GAP)

**[BUILT]** — the in-pod replay driver `drive_replay_in_pod` + `deja-runner` bin; `StoreExec`
transport seam (Compose|Direct); push-back (`RunEvent` ingest + `StoreCtx::Http`); S3 pull
(`stage_resolve_recording`, transport-agnostic, offline short-circuit); Pg run store
(`Store::connect` auto-migrates); `PrebuiltImage` candidate; correlation filter; the recording
envelope already carries `code.sha` + `code.deja_version` (`deja_record_sink.rs:211`); seeding
core `materialize_seed_plan(&StoreExec, …)` is already transport-agnostic and called identically
from compose and in-pod drivers.

**[GAP]** — the K8s executor (typed patch + REST client); the scheduler (queue/ceiling/reconcile/
shard/fan-in); the two-string config surface (`resolve.rs`, `expand_create_body`, form collapse);
the local pipeline (`run-deja-local-pod.sh`, pod-in-a-box); the whole replay-env infra package +
two IRSA units; and the six replay-correctness blockers below.

**Six verified replay-correctness blockers (these invalidate the *verdict*, not just the
deploy).** Until these land, no real run's result is trustworthy:

- **A1 [BLOCKER, #28]** runner image ships `hyperswitch-deja-clean` = **461** migrations;
  candidate `ff191d7f79` = **496** (strict superset, 35 missing). Wrong-schema migrate → router
  500 → false body divergence. Fix: CodeBundle carries migrations **by sha**.
- **A2 [BLOCKER, #29]** the `router-start` sentinel the boot contract mandates is written
  **nowhere**; `HarnessRoot::new` creates no `ready` dir. A sentinel-gated router hangs forever;
  the built model boots the router *before* migrate+seed. Fix: write the sentinel after seed.
- **A3 [#30]** runner `HARNESS_STATE_DIR=/workspace/state` vs contract router
  `REPLAY__SOURCE=/deja/work/...` → every boundary a novel **live** call; replay silently becomes
  live traffic. Fix: R8 shared-mount + fail-loud assertion.
- **A4 [#31, VENDOR]** redis `add_prefix` → `replay_key_namespace()` → `current_correlation_id()`
  reads the ambient thread-local the pg path was rewritten to avoid → seed miss / unfenced RMW
  contamination. Fix needs a **vendor change** + rebuild of all 7 images — out of scope for
  Monday, named as a dependency.
- **A5 [P3]** no crypto-epoch parity check; mismatched `master_enc_key` → garbage decrypt →
  false connector divergence. Fix: fingerprint both keys (keyed digest, never the key), assert.
- **A6 [MINOR, latent]** seeder links deja by path, router pins a rev; byte-identical today, but
  a future skew silently 100%-seed-misses. Fix: P4 join-compat assertion.

---

## 4. Blockers to kick off NOW (user + infra-team; lead time)

These gate the cluster milestones and cannot be self-served past `terragrunt apply`
(Atlantis owns apply; ArgoCD only syncs manifests). Start these first.

- [ ] **O1 — EKS ≥ 1.29** (native sidecars GA). `kubectl get --raw /version | jq '.major,.minor'`.
  **Structural gate.** If older, the Job never Completes and the shape is invalid.
- [ ] **O2 — PSA enforce level** on the Argo-created `replay-sbx` + `replay-orchestrator-sandbox`.
  If `restricted`, root-running pg/redis/orchestrator pods are rejected; fix in chart
  `securityContext` or `managedNamespaceMetadata`.
- [ ] **O3 — VPC-CNI NetworkPolicy controller enabled?** On EKS a NetworkPolicy is **silently
  ignored** otherwise. Highest-consequence item — a payment tape with one un-hooked boundary is
  a live charge. P8 refuses every launch until a probe proves egress is denied.
- [ ] **O4 — `terragrunt apply` the two IRSA units + the ECR edit** (the load-bearing ordering
  gate). Merge the terragrunt PR → infra applies → `role_arn` lands in each tfstate → ArgoCD can
  resolve `$<replay_orchestrator.role_arn>$` / `$<replay_job.role_arn>$`.
- [ ] **O5 — Persistent Postgres** endpoint for `DEJA_DB_URL` (RDS or in-cluster PG). Mandatory
  in k8s/local mode: the queue, ceiling, reconcile, fan-in, and scorecard-serving all live there.
- [ ] **O6 — KMS CMK ARN** for `sandbox/hyperswitch` (if not the default `aws/secretsmanager`
  key) so a `kms:Decrypt` statement can be added to `replay-job-role`. Path is known; only the
  key is unknown.
- [ ] **O7 — Images pullable in ECR / `public.ecr.aws`, digest-pinned:** the orchestrator/runner
  image (new ECR repo `hyperswitch-replay-orchestrator`), the candidate `deja-pr @ ff191d7f79`
  and the six `deja-pr-patch-*` shas (Jenkins → `hyperswitch-router:<sha>`, tag == git sha), and
  pg/redis (pg from `public.ecr.aws/docker/library/postgres`). Node role needs `ecr:GetAuthorizationToken`
  + `ecr:BatchGetImage` (IRSA does not grant pulls); else attach `imagePullSecrets`.
- [ ] **O8 — Recording env stamps its ref** so P0 can assert: one infra line on the recording
  pod, `ROUTER__DEJA__IDENTITY__CODE_SHA = <imageTag>`. Without it a tape's sha is `"unknown"`
  and P0 correctly refuses — but no real tape can run.
- [ ] **O9 — Crypto-epoch/redis-prefix emission from the record side** (P3/P5). The recording env
  must stamp a keyed digest of `master_enc_key`/`hash_key` and its `redis_key_prefix` into the
  manifest/envelope. Until then P3/P5 are inert (both-None → pass). Decision: is inert-pass
  acceptable for Monday non-demo tapes, or `DEJA_PARITY_REQUIRE=true` hard-fail? (§10)
- [ ] **O10 — CodeBundle producer** (CI stage vs `deja-bundle` subcommand): who builds
  `code/<sha>/{bundle.tar.zst,fingerprints.json}` per sha, and the branch→sha `refs/<ref>` index.
  Until it exists, A1 falls back to the image-bundled 496 set + a static `DEJA_EXPECTED_SCHEMA_HEAD`
  and P1 degrades to a presence check.
- [ ] **O11 — Infra PR approval** on `hyperswitch-infra` branch `deja-custom-pod-deployment`. No
  push without explicit USER approval.
- [ ] **O12 — Orchestrator→git egress?** for `git ls-remote` (branch/PR → sha). If unreachable,
  branch/PR candidates refuse (named reason) and only tag/sha/full-ref tiers work for Monday.

---

## 5. Workstream A — Correctness spine (all local; `compose` stays default)

Every step here is a safe refactor while `DEJA_EXECUTOR=compose`. Order: A1→A2 unblock the
sentinel/state-dir (also the local pipeline's verification target), then the k8s client, resolve,
preflight, patch, migrations, and the seeding parity gates.

**A-SENTINEL — A2 sentinel + A3 state-dir unification** *(was SPINE-1 / L1 / C1 / #29 / #30)*.
Add `"ready"` to the subdir list in `HarnessRoot::new` (`lib.rs:184-190`); add
`ready_dir()`/`ready_sentinel_path()` → `{root}/ready/router-start`. In `drive_replay_in_pod`
(`mod.rs`), **after** `materialize_seed_plan` (stage 4, ~928) and **before** `wait_health` (~940),
`create_dir_all` + touch the sentinel. Introduce `SHARED_MOUNT="/deja/work"` (R8). In
`deja-runner.rs`, reconcile the default `HARNESS_STATE_DIR` from `/workspace/state` → `/deja/work`,
and assert `root.lookup_table_path(run_id).starts_with(HARNESS_STATE_DIR)` (fail loud = P9 in-pod).
*Verify:* unit test asserts `ready` dir + sentinel path; grep confirms touch precedes `wait_health`;
runner unit test errors when the two roots diverge.

**A-SEAM — create-time executor routing** *(was SPINE-2 / C2 / L2)*. In `v1_create_run` /
`spawn_worker` (`api/runs.rs:40`, single caller `main.rs:314`, no change needed), route by
`DEJA_EXECUTOR` (R1): `compose` → `ComposeExecutor` (current body verbatim, Record-capable);
`external` → persist row + audit, **skip** the worker; `local`/`k8s` → `enqueue` (Workstream B).
*Verify:* compose demo unchanged with the var unset; `external` create leaves no compose stack up
and the row awaits push-back.

**A-K8S — raw-REST k8s client** *(was SPINE-3 / C3)*. New `lifecycle/k8s.rs`, **no Cargo.toml
change** (ureq 2.12.1 + rustls 0.23 + ring already present; `grep -c 'name = "aws-lc-rs"'
Cargo.lock` must stay 0). `ClientConfig` via
`builder_with_provider(ring::default_provider().into()).with_protocol_versions(&[&TLS12,&TLS13])`;
roots from `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt`, **assert `added >= 1`**,
fail-fast + surface to `/healthz`. `timeout_connect(10s).timeout_read(15s)`. **Re-read the SA
token fresh on every request** (projected tokens rotate ~1h, runs outlive them; never cache in
the struct). Bracket IPv6 `KUBERNETES_SERVICE_HOST`. Surface: `create_job` (POST batch/v1, 409 =
idempotent-ok), `job_status` (terminal only from `.status.conditions[]` type∈{Complete,Failed}
status=="True", **not** the counters), `delete_job`, `get_configmap`, `pod_logs`. **Error
taxonomy:** never derive Job failure from transport/401/5xx — retry with capped backoff bounded
by `activeDeadlineSeconds`; settle terminal failure only from a real `Failed` condition. *Verify:*
`git diff Cargo.toml` empty; aws-lc-rs count 0; token re-read test; fake-apiserver integration.

**A-RESOLVE — two-ref + candidate-string resolution** *(merged SPINE-4 + CS1/CS3/CS5, R4)*. One
`src/resolve.rs`:
- `CandidateSpec::from_candidate_str(s, &CandidateDefaults)` by shape: full registry ref
  (host-with-dot + `:tag`|`@sha256:`) → `PrebuiltImage`; `^[0-9a-f]{7,40}$` → `RepoSha`
  (`{registry}:{sha}`, `sha_C=sha`, no git); bare non-hex tag → `PrebuiltImage {registry}:{tag}`;
  `^#?\d+$`/`pr[-/]\d+` → `RepoPr`; leading `/`|`./`/existing path → `LocalPath`; else `RepoBranch`.
- `resolve_recording_sha(&S3Config, prefix)` → reads `SessionManifest.code[].sha` (compactor
  `lib.rs:120-132`; **no dependency on the ingest-envelope fix** — the manifest already carries
  `code[]`); refuse if >1 distinct sha. For the aggregator layout, extend the envelope probe with
  `CodeProbe{sha,deja_version}` (`s3/mod.rs:47-67` today discards `code`).
- `resolve_candidate_image(&CandidateSpec, &CandidateDefaults)` → image + `sha_C`; `RepoBranch`/
  `RepoPr` shell `git ls-remote $DEJA_CANDIDATE_GIT_REMOTE` (control plane only, O12) → short-sha
  → `{registry}:{sha}`, else a **named refusal** (teaches, never a silent wrong image).
- `fetch_bundle_fingerprints(&S3Config, sha)` → GET `s3://<DEJA_CODE_BUNDLE_PREFIX>/code/<sha>/
  fingerprints.json` = `{schema_head, deja_rev, redis_key_prefix, config_sha256, crypto_epoch}`;
  missing → the P1 error.
- Populate `ResolvedRefs { sha_r, sha_c, candidate_image, candidate_digest?, bundle_uri_r,
  bundle_uri_c, env_profile }` and attach it to `RunSpec` as `#[serde(default)] resolved:
  Option<ResolvedRefs>` so it serializes into `DEJA_RUN_SPEC` and reaches the in-Job preflight.
*Verify:* table test of the five candidate shapes; manifest fixture → `sha_R`; two-sha fixture →
refusal; fingerprints-absent → P1 error; `RunSpec` serde round-trip with/without `resolved`.

**A-PREFLIGHT — fail-closed gate P0..P9** *(was SPINE-5, R6)*. New `lifecycle/preflight.rs`,
`preflight(rref, cref, fp_r, fp_c, env, patch_paths) -> Result<(), Refusal{gate,reason}>`.
Orchestrator-side: **P0** `sha_R != unknown`; **P1** fingerprints present for both shas; **P3**
`fp_r.crypto_epoch == env.crypto_epoch`; **P4** `fp_r.deja_version` join-compat with `fp_c.deja_rev`;
**P5** `fp_r.redis_key_prefix == fp_c.redis_key_prefix`; **P7** every ConfigMap env key classified
keep/replace/forbid (unclassified → refuse); **P8** read the cached egress-enforcement probe
(A-EGRESS below), refuse if not proven; **P9** the `HARNESS_STATE_DIR` the patch sets is the
prefix of the router `REPLAY__SOURCE`/`OBSERVED_SINK` it sets (static, both sides owned here). Any
`Err(Refusal)` → `ctx.finish(false, "PREFLIGHT {gate}: {reason}")` and **no POST**. In-pod **P2**
(`__diesel_schema_migrations` head == `C.schema_head`) and **P6** (schema skew has no unfilled
NOT NULL additions) live in the runner after migrate, before seed. *Verify:* per-gate table test
asserts the exact `Refusal{gate}`; executor-level test: a P0 refusal yields exactly one
`Finish{ok:false}` and zero `create_job` calls.

**A-MIGRATIONS — A1 durable fix + near-term unblock** *(was SPINE-6 / O8 / #28)*. Durable: in
`deja-runner.rs` read `DEJA_CODE_BUNDLE_URI` + `DEJA_EXPECTED_SCHEMA_HEAD`; before stage 3 fetch
`bundle.tar.zst` (reuse compactor `get_object_decoded`), extract `migrations/` + `diesel.toml`,
set `RUNNER_MIGRATE_CMD=diesel migration run --migration-dir <extracted>/migrations --config-file
<extracted>/diesel.toml` so stage 3 runs the **candidate's** migrations; assert P2 after migrate.
Near-term (single frozen candidate, until O10): swap `ops/orchestrator/Dockerfile:50` from
`vendor/hyperswitch-deja-clean` (461) to `vendor/hyperswitch @ ff191d7f79` (496) + a static
`DEJA_EXPECTED_SCHEMA_HEAD`. *Verify:* `comm -3` candidate vs bundled migration names is empty
(35-gap closed); local runner migrates to the expected head, P2 passes; a wrong head exits
non-zero naming P2.

**A-PATCH — typed patch over the ConfigMap Job template + golden test** *(was SPINE-7 / C4,
Amendment 1)*. New `lifecycle/patch.rs`, `patch_job(template:Value, run, cref, k8s, shape) ->
Value`. **Never string-template — parse to `Value`, patch paths:** find the router container by
name, set `.image = cref.image_ref`; find the runner container, set `.image = k8s.runner_image`;
upsert env-by-key into runner (`DEJA_RUN_ID`, `DEJA_RUN_SPEC`, `DEJA_ORCHESTRATOR_URL`=callback,
`HARNESS_STATE_DIR`=`SHARED_MOUNT`, `DEJA_CODE_BUNDLE_URI`, `DEJA_EXPECTED_SCHEMA_HEAD`, the
`DEJA_PARITY_*` set, `DEJA_S3_ACCESS_KEY:""`, `DEJA_S3_SECRET_KEY:""`, `DEJA_RUNNER_ACTOR`) and
into router (`ROUTER__DEJA__RUN_ID`, `ROUTER__DEJA__REPLAY__SOURCE`,
`ROUTER__DEJA__REPLAY__OBSERVED_SINK` from `SHARED_MOUNT`); set `.metadata.name` deterministically
from run_id (409 idempotent); stamp labels `app=deja-replay`, `deja-run-id=<run_id>`.
`DEJA_JOB_SHAPE` selects `data["job-replay.json"]` vs `data["job-echo.json"]`. **Golden test**
(`ops/orchestrator/replay-job.golden.json`) with explicit assertions: sentinel-gate command,
blank `DEJA_S3_*`, `sidecar.istio.io/inject:"false"` on pod-template labels+annotations,
`backoffLimit:0`, `ttlSecondsAfterFinished`/`activeDeadlineSeconds` present, and
`REPLAY__SOURCE`/`OBSERVED_SINK` share the `SHARED_MOUNT` prefix used for `HARNESS_STATE_DIR`
(P9 static). `DEJA_ORCHESTRATOR_URL` equals the configured callback (port :80); executor
construction fails loud if `DEJA_ORCHESTRATOR_CALLBACK_URL` is unset.

**A-EGRESS — P8 egress-enforcement probe** *(was SPINE-13 / #32)*. A one-shot startup probe (Job
or cached result) attempts a denied egress from a `replay-sbx` pod and confirms it is blocked;
store the boolean P8 reads. Until it returns `enforced=true`, P8 refuses every launch
(fail-closed). Pairs with the default-deny NetworkPolicy in the replay-env package.

**Seeding purification + parity gates** *(AREA-2; the seeding core is already transport-agnostic —
this is not a rewrite)*. Two residual demo leaks and three unguarded parity requirements:

- **A-SEED-1..3 (purify).** Thread `SeedInputs { ambient:&AmbientTemplate, seed_db_enabled:bool,
  parity:&ReplayParity }` into `materialize_seed_plan` (5th arg); delete the two internal env
  reads (`DEJA_SEED_DB` at `mod.rs:1433`, `load_ambient_template()` at `mod.rs:1454`) — move both
  to the caller. `load_ambient_template()` default becomes `AmbientTemplate::new()` (**EMPTY**),
  never `demo_defaults()` (`settlement_rate_premium 0.20`); ship the demo's ambient as a TSV via
  `DEJA_AMBIENT_TEMPLATE`. Delete `drive_record`'s hardcoded `seed_redis(…,
  "settlement_rate_default","0.10")` (`mod.rs:538`) → move to `demo/workload.sh`. Preserve the
  non-issue: the redis seed key `{corr}:{entry.key}` is already prefix-correct (the recorded key
  is physical) — do **not** re-key it.
- **A-SEED-4..7 (parity, fail-closed).** New `lifecycle/parity.rs`: `Parity<T>{record,env}` with
  `check` semantics (both-None → pass+warn; equal → pass; unequal → fail; asymmetric+require →
  fail). `ReplayParity { crypto_epoch, redis_key_prefix, schema_head_expected, require }` +
  `from_env()` (reads `DEJA_PARITY_*` injected by the patch) + `trivially_equal()` (compose
  self-fixture). **Static gates** P3+P5 asserted **before** materialization in both drivers.
  **Live-DB gates** P2 (schema head via `store.psql`) after migrate/before
  `load_db_catalog`/`create_db_schema`; P6 classifies a would-be INSERT failure as NOT-NULL-skew
  using the already-loaded `DbCatalog` nullability. **Fail-loud aggregate:** after
  `materialize_seed_plan` returns, if `certificate.summary.failed > 0`, the caller returns Err
  (run FAILED, precondition) **before scoring** — so an A1 mismatch or a P6 skew is a loud
  precondition failure, never a false CAUGHT. Per-INSERT stays best-effort so the certificate
  captures the full failure set. Wire `deja-runner` to build `ReplayParity::from_env()` and the
  compose worker to `trivially_equal()`.
  *Note:* self-produced demo fixtures pass all three by construction (same image → equal
  fingerprints). P2/P6 become meaningful once A-MIGRATIONS lands; P3/P5 once O9 lands — but the
  gate code and fail-closed wiring land **now** so the seam is self-protecting.

---

## 6. Workstream B — Scale + durability (the run row IS the queue)

Build three durable-state mechanisms on the existing Postgres store (`crates/deja-store`) plus one
backend seam. **No broker, no in-memory scheduler.** Fixes emptyDir orphaning and scorecard
evaporation by construction.

**B-0006 — scheduling columns** *(scale S1)*. New `0006_run_scheduling.sql`
(`Store::connect` auto-runs it). `ALTER TABLE replay_runs ADD` `sched_state text NOT NULL DEFAULT
'done' CHECK (…'queued','dispatching','active','done','parent')`, `job_name`, `attempt int NOT
NULL DEFAULT 0`, `dispatched_at`, `deadline_at`, `parent_run_id … REFERENCES replay_runs ON
DELETE CASCADE`, `shard_index`, `shard_total`; indexes `(sched_state, created_at)` and
`(parent_run_id)`. DEFAULT `'done'` keeps the dispatcher off legacy/compose rows.
`sched_state` is **orthogonal** to the runner's free-text `state` so push-back `'building'` never
clobbers `'active'`.

**B-STORE — scheduling API** *(scale S2)*. `enqueue_run(… config_snapshot = full RunSpec json …)`
(`sched_state='queued'`, `deadline_at=now()+…`, `ON CONFLICT DO NOTHING`); `insert_parent_run`
(`sched_state='parent'`); **`claim_and_dispatch(ceiling, prefix, deadline)`** as the single
atomic `UPDATE … SET sched_state='dispatching', attempt=attempt+1, … WHERE run_id=(SELECT … WHERE
sched_state='queued' AND (SELECT count(*) … IN ('dispatching','active')) < ceiling ORDER BY
created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING`; `mark_active`, `requeue` (job_name=NULL),
`mark_sched_done`, `list_scheduled`, `list_children`, `count_active`. Reuse
`update_run_state`/`set_run_result` to settle. Atomic claim = no double-launch; SKIP LOCKED stays
correct if replicas ever exceed 1.

**B-SPEC — shard fields + immutable snapshot** *(scale S3)*. `RunSpec` gains
`#[serde(default)] shard_total: Option<u32>` + `shard_index: Option<u32>` (both absent = single
run). The dispatcher renders `DEJA_RUN_SPEC` from `config_snapshot`, **not** from the file store —
this is what makes re-dispatch survive an orchestrator restart with no local disk.

**B-BACKEND — `JobBackend` seam + LocalProcess/Null** *(scale S4, R2)*. `trait JobBackend {
launch(&LaunchRequest)->Result; list()->Vec<JobObservation>; delete(job_name)->Result; }`.
`launch` treats 409/AlreadyExists as success (idempotent; reconciler may relaunch).
`LocalProcessBackend` spawns `deja-runner` as a child (tracks PID → phase); `NullBackend` for
tests. **The `K8sJobBackend` is the Workstream-A spine** (resolve → preflight → GET cm → patch →
POST) implementing this same trait — confirm it does so exactly, so dispatcher/reconciler stay
backend-agnostic.

**B-DISPATCH — dispatcher loop** *(scale S5)*. `Scheduler{store,backend,cfg}`; on a
`DEJA_DISPATCH_INTERVAL_SECS` tick, loop `claim_and_dispatch` until None; build `LaunchRequest`
from `config_snapshot`; `backend.launch`; Ok → `mark_active`, Err → `requeue` (attempt < max) or
settle failed. Deterministic `job_name = <prefix>-<tail>-a<attempt>` + `backoffLimit:0` make a
re-dispatch a **new attempt**, never a silent k8s retry.

**B-RECONCILE — one pure decision fn + orphan reap + re-dispatch** *(scale S6, R3)*. Pure
`reconcile_decisions(tracked, observed, now) -> Vec<Decision>` (`SettleFailed|SettleCompleted|
Reap|Requeue`): terminal run + Job present → Reap; Job Failed + not terminal → SettleFailed; Job
Complete + not terminal (lost Finish) → SettleCompleted; Job Missing + active + not terminal →
SettleFailed('orphaned'); past deadline → SettleFailed('deadline')+Reap; dispatching + Job Missing
past grace → Requeue. `reconcile_loop` does ONE `backend.list()` + ONE `store.list_scheduled()`
per `DEJA_RECONCILE_INTERVAL_SECS` tick and holds **no in-memory state** → restart-transparent.
Error taxonomy: never settle FAILED from a transport error, only from a real `Failed`/true absence.

**B-SHARD — fan-out at create + deterministic in-runner partition** *(scale S7)*. If
`shard_total>1`: insert a `parent` row + N `queued` child rows (`<parent>-s<i>`, `parent_run_id`,
`shard_index`, spec clone). In-runner (after `stage_resolve_recording`): `effective_filter =
base` (or full correlation set) filtered by **FNV-1a(corr) % shard_total == shard_index** (a
FIXED-seed hash — `std::DefaultHasher` is per-process randomized and would give inconsistent
partitions); thread the same allowlist into `materialize_seed_plan` (a shard only seeds its
cases) and the kernel. Reuses the proven per-correlation redis `{corr}:` + pg schema-per-correlation
isolation. **The serial kernel is escaped by process fan-out, not kernel concurrency.**

**B-FANIN — merge child scorecards + AND verdict** *(scale S8)*. `divergence::merge_scorecards`
sums Summary counters + per_boundary, concatenates per_correlation, unions correlation_scope,
recomputes verdict. In `reconcile_loop`, a `parent` whose children are all terminal → merge →
verdict = AND (fail > inconclusive > pass) → `set_run_result` + `update_run_state` +
`mark_sched_done`. A null child scorecard (crashed shard) forces inconclusive/fail.

**B-WIRE — main.rs wiring; store mandatory; create→enqueue; readiness** *(scale S9)*.
`SchedulerConfig::from_env` (`DEJA_MAX_CONCURRENT_JOBS`=4, `DEJA_DISPATCH_INTERVAL_SECS`=2,
`DEJA_RECONCILE_INTERVAL_SECS`=10, `DEJA_JOB_ACTIVE_DEADLINE_SECONDS`=7200,
`DEJA_JOB_MAX_ATTEMPTS`=3, `DEJA_REPLAY_JOB_LABEL`=`app=deja-replay`,
`DEJA_JOB_NAME_PREFIX`=`deja`). When `DEJA_EXECUTOR=k8s|local`, a failed `Store::connect` is
**FATAL**; construct backend + Scheduler, `tokio::spawn` dispatch + reconcile. `v1_create_run`
enqueues (202) in scheduler modes; compose unchanged. Add `GET /api/v1/readyz` returning 503 when
a scheduler mode is active but the store is absent.

**B-READ — scorecard durability** *(scale S10)*. `v1_scorecard` falls back to the stored
`replay_runs.scorecard` JSON when `divergence::scorecard(&st.root,&id)` finds nothing (the normal
case for a k8s Job whose artifacts are pod-local). Surface the new sched columns on `list_runs`.
Heavy artifact drill-down over `s3://` stays #21.

*Verify (B):* ceiling test (3 enqueued, ceiling=1/2 → exact concurrency); reconcile table drives
each rule with zero DB/k8s; shard partition is pairwise-disjoint + union = full + byte-stable;
merge → AND verdict; restart re-attach integration (`--ignored`, needs demo pg).

---

## 7. Workstream C — Minimal config surface + dashboard

Three-layer config: **Layer A** per-run (the two strings + optional `correlations`/`expectation`/
`env`/`shard_total`); **Layer B** service defaults (orchestrator Deployment env, set once);
**Layer C** profile-owned (the replay-env ConfigMap). The orchestrator's per-run vocabulary
shrinks to: resolve two refs, populate `ResolvedRefs`, hand to the executor.

**C-BODY — lenient `CreateRunBody` + `expand_create_body`** *(CS2)*. Accept EITHER the minimal
form (`recording`, `candidate`, `mode`? default replay, `correlations`?, `expectation`?, `env`?,
`shard_total`?) OR the legacy full `RunSpec` (`candidate_spec` present) so old curl + the runner's
own `DEJA_RUN_SPEC` round-trip keep working. `pub fn expand_create_body(body, &Defaults) ->
Result<(RunSpec, Option<expectation>), String>`: `recording` starting `s3://`/containing `/` →
`S3Source` (bucket defaults to `DEJA_S3_BUCKET` when bare) else `recording_id`; `candidate` →
`CandidateSpec::from_candidate_str` (A-RESOLVE); `correlations` → `correlation_filter`. Keep
`expectation` as a first-class slot outside `RunSpec` (audit-only). **Called by both** the HTTP
handler and the local `deja-runner` CLI so local expands identically to cluster.

**C-SHA — populate `sha_R` at create** *(CS3)*. Call `resolve_recording_sha` for the S3 path;
store it in `ResolvedRefs.sha_r`. `env_profile = body.env.unwrap_or(DEJA_REPLAY_ENV)`.

**C-CLI — shared two-input local runner entry** *(CS7)*. `deja-runner`: when `DEJA_RUN_SPEC` is
absent but `--recording`/`--candidate` (or `DEJA_RECORDING`/`DEJA_CANDIDATE`) are present, call
the **same** `expand_create_body` to mint the RunSpec locally. This is the hook
`run-deja-local-pod.sh` consumes.

**C-FORM — collapse the dashboard New Run form to two fields + Advanced** *(CS6)*. Two visible
controls: `recording` (S3 path or catalog dropdown) and `candidate` (one text box, placeholder
`branch / PR# / short-sha / tag / full image ref`), plus a collapsed `<details>` Advanced
(correlations, expectation, env). Drop the mode toggle (cluster is replay-only), region/endpoint
fields, and the binaryPath field (move the build-a-binary snippet to a help tab). Live curl
preview renders the two-field body.

**C-DEFAULTS — cluster Layer-B env (chart)** *(CS8, infra)*. `DEJA_S3_BUCKET=hyperswitch-art`,
`DEJA_S3_REGION=ap-south-1`, `DEJA_S3_ALLOW_HTTP=false`, `DEJA_S3_ACCESS_KEY=""` +
`DEJA_S3_SECRET_KEY=""` (explicit empty so IRSA engages — `S3Config` defaults to `minioadmin` and
`has_static_credentials()` masks IRSA → 403), `DEJA_REPLAY_ENV=sbx-mirror`,
`DEJA_CANDIDATE_REGISTRY_DEFAULT=223655089699.dkr.ecr.ap-south-1.amazonaws.com/hyperswitch-router`,
`DEJA_CANDIDATE_REPO_DEFAULT=juspay/hyperswitch`, `DEJA_CANDIDATE_GIT_REMOTE` only if O12
confirmed. **Remove** the static-key `replay-orchestrator-aws` secretRefs so they cannot mask
IRSA. The demo-MinIO S3 defaults in code (`compactor lib.rs:57-64`) stay as-is and are overridden
purely by chart env — no code default flip.

*Verify (C):* candidate-shape table test; minimal + legacy bodies both POST 200; `sha_R` from a
manifest fixture; `web` build compiles with exactly two visible inputs; local `deja-runner
--recording --candidate` mints the same RunSpec JSON as the API.

---

## 8. Workstream D — Local testing pipeline (shares `drive_replay_in_pod`)

**Tier 1 (pod-in-a-box, minutes):** pg + redis + the candidate router as containers sharing one
`/deja/work` state dir, with the real `deja-runner` run **host-native** (host has psql/redis-cli/
diesel). Covers replay correctness and reproduces A1/A2/A3 locally before they can reach the
cluster. **Tier 1b:** the runner *image* sharing the router netns (after C8 image tightening).
**Tier 2:** kind/k3d (k8s ≥ 1.29) applying the **real** replay-env ConfigMap Job template + the
orchestrator's typed patch — the k8s-only surface (typed patch, native-sidecar auto-termination,
SA-token REST client, default-deny NetworkPolicy). Depends on Workstream A landing.

**D-COMPOSE — local-pod stack** *(L3)*. New `demo/overlays/local-pod/docker-compose.pod.yml`:
pg + redis (host-published ports) + a `router` service `image: ${CANDIDATE_IMAGE}` with the
boot-contract command `sh -ec 'until [ -f /deja/work/ready/router-start ]; do sleep 1; done; exec
router -f /local/config/docker_compose.toml'` (keep the image's own entrypoint per boot-contract
GAP), health disabled; router env per the contract (`ROUTER__DEJA__MODE=replay`, RUN_ID,
`REPLAY__SOURCE`/`OBSERVED_SINK` under `/deja/work`, `MASTER_DATABASE__HOST=pg`, `REDIS__HOST=redis`,
Superposition file-fallback, `RUST_MIN_STACK`, optional crypto passthrough); bind-mount the host
`HarnessRoot` at `/deja/work` (identical absolute path) and config via **per-file subPath** so the
frozen image's baked `/local/config` is not shadowed.

**D-DRIVER — `run-deja-local-pod.sh`** *(L4)*. Two inputs: `--candidate <image-ref>` and
`--recording s3://…` XOR `--fixture <id>` (offline), plus `--migrations`, `--router-port`,
`--runner-mode host|image`, `--keep`, `--expect-divergence|--expect-pass`. Flow: build binaries →
init the ONE shared state dir → start a local orchestrator (`DEJA_EXECUTOR=external`) → mint
run_id + RunSpec → POST `/runs` to create the row → `compose up` the pod stack with
`CANDIDATE_IMAGE` → run `deja-runner` host-native with the typed env (`RUNNER_DATABASE_URL`,
`RUNNER_REDIS_*`, `RUNNER_ROUTER_PORT`, `RUNNER_MIGRATE_CMD`, `DEJA_RUN_ID`/`DEJA_RUN_SPEC`/
`DEJA_ORCHESTRATOR_URL`, `DEJA_S3_*`) → **assert `REPLAY__SOURCE == root.lookup_table_path(run_id)`**
(A3 guard) → print scorecard, exit code from verdict vs expectation. This is the verification
vehicle for #29/#30: a faithfully sentinel-gated router forces the A2 fix; a divergent state root
forces A3.

**D-PRODUCER — the compose demo as fixture producer** *(L5)*. `run-deja-demo.sh --keep` records a
tape into local MinIO with the demo image whose baked crypto keys match the candidate (zero-config
crypto/Superposition parity). `run-deja-local-pod.sh` consumes it via `--recording s3://…` (MinIO)
or `--fixture <id>` (offline short-circuit). Keeps the disqualified compose replay path strictly
as the tape source, never the parity vehicle.

**D-IMAGE — tighten the runner image (Tier 1b/C8)** *(L6)*. Rewrite `ops/orchestrator/Dockerfile`:
drop DinD (docker-ce-cli, compose plugin, `DOCKER_HOST`); add `postgresql-client` + `redis-tools`
+ a pinned `diesel` (with libpq) + the candidate-sha migrations. Enables `docker run
--network=container:<router>` (shared netns) executing `StoreExec::Direct` against 127.0.0.1
sidecars exactly as the pod does.

*Verify (D):* `--candidate deja-demo --fixture <self-produced-id>` runs `drive_replay_in_pod`
end-to-end, `verdict.pass=true`, exit 0, same run at the local dashboard; a real-change candidate
with `--expect-divergence` detects and exits 0; `command -v psql redis-cli diesel` resolves in the
tightened image.

---

## 9. Workstream E — the Monday `hyperswitch-infra` PR

Two coordinated repo pieces + a named ask list. **The repo self-serves everything up to the
`terragrunt apply` boundary** (Atlantis owns apply; ArgoCD only syncs manifests). Load-bearing
ordering: merge the terragrunt PR → infra applies → `role_arn` in tfstate → ArgoCD resolves the
`$<…>$` tokens. First, drop the stray Vector sink diff:
`git checkout -- infra-configurations/vector/sandbox-hyperswitch-art-s3.yaml`.

**E-ORCH — transform the replay-orchestrator chart** off the superseded DinD model into the thin
control plane *(infra S1/S2/S3/S4/S12, incluster I1/I2/I5/I6)*:
- Strip DinD: delete the `dind:` block (values), the `dind` sidecar + `DOCKER_HOST` + three
  emptyDirs (deployment), the `replay-orchestrator.dindImage` helper; rewrite `Chart.yaml:3`.
- IRSA + keys: `serviceAccount.annotations['eks.amazonaws.com/role-arn'] =
  $<replay_orchestrator.role_arn>$` (SA `replay-orchestrator-role`); **blank both S3 keys to `""`**
  and **remove** the `replay-orchestrator-aws` secretRefs (deletion alone leaves the `minioadmin`
  default).
- Callback + executor knobs: `DEJA_ORCHESTRATOR_CALLBACK_URL =
  http://sandbox-replay-orchestrator.replay-orchestrator-sandbox.svc.cluster.local:80` (R7);
  `DEJA_EXECUTOR=k8s`, `DEJA_REPLAY_ENV=sbx-mirror`, `DEJA_JOB_NAMESPACE=replay-sbx`,
  `DEJA_JOB_SHAPE` (echo first), `DEJA_RUNNER_IMAGE`, `DEJA_JOB_SERVICE_ACCOUNT=replay-job-role`,
  `DEJA_CANDIDATE_IMAGE_DEFAULT`, `DEJA_PG_IMAGE`, `DEJA_REDIS_IMAGE`, `DEJA_JOB_TTL_SECONDS=86400`,
  `DEJA_JOB_ACTIVE_DEADLINE_SECONDS=7200`, `DEJA_CODE_BUNDLE_PREFIX`, the `DEJA_CANDIDATE_*`
  defaults (C-DEFAULTS).
- Durable state: mandatory `DEJA_DB_URL` (O5); PVC-backed `HARNESS_STATE_DIR` (scratch only, e.g.
  `/var/lib/deja/state`); `strategy: Recreate`; readiness probe `/api/v1/readyz`.
- Move the cross-namespace RBAC (Role+RoleBinding in `replay-sbx`) **into the replay-env package**
  (R5); neutralize the chart's `.Release.Namespace`-bound role/rolebinding.
- Add `$tfstate.replay_orchestrator` drySource param to
  `argo-sandbox/apps/sandbox/replay-orchestrator.yaml`. Rewrite `DEPLOY-NOTES.md` off DinD.

**E-ENV — the new `replay-env` helm package** (does not exist today) *(infra S7/S8/S9,
Amendment 1)*: `helm-charts/charts/replay-env/` with:
- `job-configmap.yaml` — the batch/v1 Job **manifest as ConfigMap data** (typed-patch target,
  no Rust renderer): 3 native sidecars pg/redis/router (`initContainers restartPolicy: Always`) +
  runner; one shared `/deja/work` emptyDir in runner AND router; router config via per-file
  subPath mounts; router command = the sentinel-gate loop then `exec router -f
  /local/config/docker_compose.toml`, no probes; `ROUTER__DEJA__MODE=replay` + RUN_ID +
  `REPLAY__SOURCE=/deja/work/lookup-tables/${RUN_ID}.jsonl`; `DEJA_S3_*=""`;
  `sidecar.istio.io/inject:"false"` on labels+annotations; `backoffLimit:0`; `restartPolicy:Never`;
  `activeDeadlineSeconds:7200`; `ttlSecondsAfterFinished:86400`; `serviceAccountName:replay-job-role`.
  The keep/replace/forbid env taxonomy (P7) lives here.
- `router-config-configmap.yaml` — `docker_compose.toml` + `superposition_seed.toml` +
  `payment_required_fields_v2.toml` (~313 KB, < 1 MiB).
- `external-secret.yaml` + `secret-store.yaml` — mirror the recon ESO shape; SecretStore
  provider.aws SecretsManager region ap-south-1, `auth.jwt.serviceAccountRef.name: replay-job-role`;
  ExternalSecret `dataFrom.extract.key: "sandbox/hyperswitch"` → `replay-crypto`. **Same path,
  never a copy** (a copy drifts on rotation = false connector divergence = A5).
- `job-serviceaccount.yaml` — SA `replay-job-role`, only the IRSA annotation
  `$<replay_job.role_arn>$`, `automountServiceAccountToken:false` (the runner never calls the k8s
  API; only ESO assumes the role).
- `networkpolicy.yaml` — default-deny egress (authored from scratch; zero NetworkPolicy exists in
  the repo). Allow DNS, the orchestrator Service, S3/ECR VPC endpoints; deny all else.
- `orchestrator-rbac.yaml` — the cross-namespace Role + RoleBinding (R5).
- ArgoCD app+project (destination `replay-sbx`, `CreateNamespace=true`, `$tfstate.replay_job`
  param) + `argoapps-values.yaml` registration (2 lines; the replay-orchestrator 2 lines already
  staged).

**E-IRSA — two terragrunt units** reusing the generic `application-resources/revenue-recovery`
module (@ `tf/app/revenue-recovery-v0.1.0` — generic IRSA+S3, role name
`${environment}-${project_name}-${app_name}-role`, OIDC synthesized from the existing cluster
provider, **no separate OIDC ask**) *(infra S5/S6)*:
- `apps/replay-orchestrator/terragrunt.hcl` — `app_name=replay-orchestrator`,
  `cluster_service_accounts {cluster => [{namespace=replay-orchestrator-sandbox,
  name=replay-orchestrator-role}]}`, `s3={create=false, bucket_arn=arn:aws:s3:::hyperswitch-art}`
  (RW, never re-creates the bucket).
- `apps/replay-job/terragrunt.hcl` — `app_name=replay-job`, `{namespace=replay-sbx,
  name=replay-job-role}`, **no** s3 block (module policy is hardcoded RW); grant least-privilege
  via `inline_policies`: S3 read-only on `hyperswitch-art`, SecretsManager read on
  `sandbox/hyperswitch*`, optional `kms:Decrypt` on the CMK once O6 confirms.

**E-ECR** *(infra S10)* — add `hyperswitch-replay-orchestrator` to the repositories map in
`terraform/aws/live/sandbox/ap-south-1/ecr/terragrunt.hcl` (mirror the `hyperswitch-router` block,
IMMUTABLE). The candidate router already has a home (`hyperswitch-router` exists). `postgres`
pulls from `public.ecr.aws` (whitelisted; no docker.io).

*Verify (E):* `helm template … | grep -c -E 'privileged|docker.sock|27-dind'` = 0; the rendered
orchestrator Deployment has one container + PVC + `strategy:Recreate` + `/api/v1/readyz`; S3 keys
render as `""`; callback shows `:80` and `sandbox-replay-orchestrator`; `helm lint replay-env`
passes and renders a parseable batch/v1 Job, an SA with only the role-arn annotation +
`automount:false`, an ExternalSecret with key `sandbox/hyperswitch`, and a default-deny egress
NetworkPolicy; `terragrunt hclfmt --terragrunt-check` passes on both units; `kubectl auth can-i
create jobs --as=system:serviceaccount:replay-orchestrator-sandbox:replay-orchestrator-role -n
replay-sbx` = yes (post-deploy).

---

## 10. The infra-team ask list (the only things the repo cannot self-execute)

Named explicitly, in Monday-deliverable form. Everything else in Workstream E is a self-served PR.

1. **Run `terragrunt apply`** on `apps/replay-orchestrator`, `apps/replay-job`, and the ecr unit
   via Atlantis (the load-bearing ordering gate). Merge the terragrunt PR **before** ArgoCD syncs
   the charts. *(O4)*
2. **Confirm the KMS CMK ARN** (if any) for `sandbox/hyperswitch`, else confirm the default
   `aws/secretsmanager` key (no grant needed). *(O6)*
3. **Build + push** the orchestrator/runner image to the new ECR repo; confirm the candidate/patch
   router shas (`ff191d7f79` + six `deja-pr-patch-*`) and pg/redis images are pullable. *(O7)*
4. **Confirm EKS ≥ 1.29** (native sidecars), the PSA enforce level on `replay-sbx`, and whether
   the **VPC-CNI NetworkPolicy controller is enabled** (else the egress seal is silently ignored —
   highest-consequence). *(O1/O2/O3)*
5. **Provide a persistent Postgres endpoint** for `DEJA_DB_URL`. *(O5)*
6. **Set `ROUTER__DEJA__IDENTITY__CODE_SHA = <imageTag>`** on the recording env (so P0 can assert
   a non-anonymous tape). *(O8)*

Not asks: OIDC trust (synthesized by the module), the SecretsManager entry (already exists) and
its path (known from the repo).

---

## 11. Adversarial findings and how the plan answers them

**Verdict-invalidating (correctness, not deploy).** A1 461-vs-496 → A-MIGRATIONS (CodeBundle-by-sha
+ P2). A2 missing sentinel → A-SENTINEL. A3 state-root drift → R8 + fail-loud + D-DRIVER guard.
A5 crypto epoch → P3 + `ReplayParity` static gate (fail-closed precondition, never a scored
divergence). Silent zero-observed → false full-red (env-contract §4): `OBSERVED_SINK` derived
from the shared mount (P9) + `load_table`/`load_jsonl` should warn on `NotFound` **[GAP, folded]**.
Seed failures best-effort/silent → A-SEED-7 fail-loud aggregate. A4 (vendor) → named dependency,
out of Monday scope.

**Deploy blockers.** Push-back URL wrong on 3 axes → R7 chart-rendered callback (port 80). Restart
orphans in-flight runs (emptyDir + store=None) → O5 PG + B-WIRE mandatory store + `strategy:Recreate`
+ B-RECONCILE re-attach. Scorecard evaporation → B-READ serve from stored JSON + `ttlSecondsAfterFinished`
>> poll. Blocking ureq on a tokio worker → K8s launch `std::thread::spawn` first (regression
assertion).

**Majors.** PSA rejects pods → O2 + chart securityContext/managedNamespaceMetadata. Missing/mismatched
Job SA → R5 byte-match `replay-job-role` + B-RECONCILE "0 pods + FailedCreate" = launch failure.
istio sidecar never exits → `inject:"false"` on labels+annotations. IRSA minioadmin 403 → blank
`DEJA_S3_*` to `""` (not omit) both sides. Double-execute → `backoffLimit:0` + deterministic
`job_name` per attempt. Orphaned Jobs after restart → B-RECONCILE (one stateless loop). ttl races
poll → ttl 86400 >> poll 10s; previously-active-now-404 = failure. SA token expiry 401 → re-read
per request. Transport-vs-failure conflation → settle only from a real `Failed` condition; retry
transport/401/5xx. Empty root-cert store → assert `added >= 1`, fail-fast. NetworkPolicy silently
ignored on VPC-CNI → P8 probe proves enforcement (O3). FNV vs `DefaultHasher` → fixed-seed FNV-1a
so shard partitions are stable across processes.

**Minors (folded).** Cross-namespace 403 → R5 cross-namespace RoleBinding (supersedes base-plan C5
same-ns pin). IPv6 `KUBERNETES_SERVICE_HOST` → bracket. Regional STS → rely on VPC endpoints, not
the squid allowlist; verify `sts.ap-south-1`. DockerHub rate-limit → mirror pg/redis/candidate,
digest-pinned. `lookup_dir` untested branch (env-contract §3) → use absolute `SOURCE`, no
`lookup_dir` (the only shape any rig exercises); fix `deja-settings.md:552-553`. `DEJA_GRAPH_DIR`
dead in the pinned rev (env-contract §6) → set `ROUTER__DEJA__RECORDING__GRAPH=enabled`, delete the
stale env; sandbox already correct — out of the deploy path, flagged.

---

## 12. Sequencing / critical path / proof gates

**Kick off day 0 (parallel, lead time):** O1–O12 to the user/infra team (§4, §10). O1 gates the
Job shape; O4 gates all cluster milestones; O5 gates scheduler modes.

**Days 1–3 — Workstream A + B foundations (all local, `compose` default):**
- A-SENTINEL, A-SEAM, A-SEED-1..3 (independent, land first) → also make D-DRIVER passable.
- A-K8S, A-RESOLVE, A-PREFLIGHT, A-PATCH, A-EGRESS (the K8s spine as a `JobBackend`).
- A-MIGRATIONS (near-term image swap now; CodeBundle path gated on O10).
- B-0006, B-STORE, B-SPEC, B-BACKEND, B-DISPATCH, B-RECONCILE, B-SHARD, B-FANIN, B-WIRE, B-READ.
- A-SEED-4..7 parity gates (code lands now; P2/P6 meaningful after A-MIGRATIONS, P3/P5 after O9).

**Days 2–3 — Workstream C + D (share the code path):**
- C-BODY, C-SHA, C-CLI, C-FORM, C-DEFAULTS.
- D-COMPOSE, D-DRIVER, D-PRODUCER (Tier 1 proves A1/A2/A3 off-cluster **before** cluster sync),
  D-IMAGE (Tier 1b).

**Days 3–4 — Workstream E (the Monday PR, needs O11 approval to push):** E-ORCH, E-ENV, E-IRSA,
E-ECR. Golden test round-trips the rendered ConfigMap through `patch_job`.

**Cluster rollout (converges the gates; each is a keystone proof):**
- **G0 — native-sidecar + PSA + inject:false probe** (as soon as cluster access; gated on O1): a
  throwaway Job mirroring the shape reaches Complete; PSA admits it.
- **G1 — echo-shape smoke** (`DEJA_JOB_SHAPE=echo`): the orchestrator creates a single-container
  Job that curls one `Finish{ok:true}` to the callback and exits 0; the poll observes Complete.
  Proves — with clean attribution, before any sidecar/IRSA-S3 is on the line — orchestrator-SA
  RBAC, apiserver IP-SAN TLS with the fresh token, ECR pull, in-cluster Service DNS + correct
  callback URL/port, push-back token auth, terminal-condition polling. A Record-mode create fails
  fast with zero Jobs.
- **G2 — first real replay** (flip `DEJA_JOB_SHAPE=replay` after A-MIGRATIONS + crypto parity +
  the ConfigMap sync): full pod boots, runner migrates+seeds+writes the sentinel, router boots
  past it, kernel drives, scores, push-back settles; native sidecars auto-terminate → Job
  Complete; scorecard served from the durable store. **Survives a deliberate orchestrator restart
  mid-run** (durable-state check).
- **G3 — candidate matrix + scale:** the six `deja-pr-patch-*` branches each produce their
  expected verdict; two concurrent same-tape Jobs confirm per-Job isolation; ceiling holds
  (`DEJA_MAX_CONCURRENT_JOBS`); a `shard_total>1` run fans out and fans in with an AND verdict.

---

## 13. Open decisions for you (aggregated; recommendations)

**Structural / correctness**
1. **EKS < 1.29 fallback** — verify O1 first; if older, a runner that explicitly signals
   pg/redis/router shutdown reshapes the Job. *Do not build the ConfigMap shape until O1 returns ≥ 29.*
2. **P3/P5 for non-demo tapes before O9** — inert-pass (both-None) or `DEJA_PARITY_REQUIRE=true`
   hard-fail an unstamped real tape? *Rec: require=true in-cluster once O8/O9 land; inert-pass only
   for self-fixtures.*
3. **CodeBundle producer** (O10) — CI stage vs `deja-bundle` subcommand; nail the
   `fingerprints.json` schema. *Rec: `deja-bundle` subcommand (self-contained, testable), promoted
   to a CI stage later.*
4. **Branch/PR → sha source** (O12) — orchestrator `git ls-remote` egress vs a CI-published S3
   `refs/<ref>` index. *Rec: S3 refs index (keeps both orchestrator and egress-sealed pod off git);
   for Monday, tag/sha/full-ref tiers cover the six branches.*
5. **Candidate digest-pinning** — needs `ecr:BatchGetImage` on the orchestrator role. *Rec: skip
   for v1 (kubelet pulls; `sha_C` from the tag == git sha); add later for immutability.*

**Scale / tracking**
6. **Ceiling default** — `DEJA_MAX_CONCURRENT_JOBS=4` is a placeholder pending node/ResourceQuota
   sizing (each Job ~2–4 GiB / 1–2 vCPU). *Infra ask.*
7. **Shard fan-out** — auto-derive `shard_total` from the manifest correlation count vs explicit
   param. *Rec: explicit for v1 (preserves the two-string UX); auto-fan-out is a later ergonomics
   step.*
8. **Complete-without-Finish** — settle `completed` with no verdict, or `inconclusive` to force an
   auto-rerun? *Rec: `completed` (do not fabricate a verdict); revisit if it recurs.*
9. **Reap timing vs artifact durability** — reconciler reaps as soon as terminal, relying on #21
   uploading artifacts before Finish. *Rec: keep verdict/scorecard durable now; gate heavy-artifact
   reaping on #21.*

**Deploy / infra**
10. **Durable state** — PG (`DEJA_DB_URL`) vs RWO PVC alone. *Rec: PG — the only path that serves
    `/scorecard` for k8s runs from pushed-back Results.*
11. **Orchestrator istio injection** — *Rec: `inject:"false"` on the orchestrator pod too so its
    blocking apiserver calls bypass Envoy.*
12. **ESO principal** — reuse `replay-job-role` (2 roles) vs a dedicated `replay-eso` role (3,
    stricter). *Rec: reuse (the router token is not automounted; only ESO assumes it).*
13. **`replay-job-role` S3 read vs read-write** — #21 artifact upload needs write (a second apply).
    *Rec: grant read now; extend to write when #21 lands, per least-privilege.*
14. **Superposition sufficiency** — file-fallback cannot represent flags absent from the seed file.
    *Rec: proceed with file-fallback; stand up a seeded Superposition sidecar only if a target tape
    depends on an absent flag (unknowable without the tape).*
15. **`DEJA_API_SERVICE_TOKEN` provenance** — hand-created Secret (v1) vs ESO. *Rec: hand-created
    for v1.*

---

## 14. Appendix — consolidated config surface

**New orchestrator env (Layer B, chart).** `DEJA_EXECUTOR` (compose|external|local|k8s, default
compose) · `DEJA_REPLAY_ENV` · `DEJA_JOB_NAMESPACE=replay-sbx` · `DEJA_JOB_SHAPE` (echo|replay) ·
`DEJA_RUNNER_IMAGE` · `DEJA_JOB_SERVICE_ACCOUNT=replay-job-role` · `DEJA_JOB_TTL_SECONDS=86400` ·
`DEJA_JOB_ACTIVE_DEADLINE_SECONDS=7200` · `DEJA_JOB_POLL/RECONCILE/DISPATCH_INTERVAL_SECS` ·
`DEJA_MAX_CONCURRENT_JOBS=4` · `DEJA_JOB_MAX_ATTEMPTS=3` · `DEJA_REPLAY_JOB_LABEL=app=deja-replay` ·
`DEJA_JOB_NAME_PREFIX=deja` · `DEJA_ORCHESTRATOR_CALLBACK_URL` (fullname, port 80) ·
`DEJA_CANDIDATE_REGISTRY_DEFAULT` · `DEJA_CANDIDATE_REPO_DEFAULT` · `DEJA_CANDIDATE_IMAGE_DEFAULT` ·
`DEJA_CANDIDATE_GIT_REMOTE` (optional) · `DEJA_CODE_BUNDLE_PREFIX` · `DEJA_DB_URL` (mandatory in
k8s/local) · `DEJA_PG_IMAGE` · `DEJA_REDIS_IMAGE`. **Set-for-cluster (existing keys):**
`DEJA_S3_BUCKET=hyperswitch-art`, `DEJA_S3_REGION=ap-south-1`, `DEJA_S3_ALLOW_HTTP=false`,
`DEJA_S3_ACCESS_KEY=""`, `DEJA_S3_SECRET_KEY=""`.

**Job-injected by the typed patch.** `DEJA_RUN_ID`, `DEJA_RUN_SPEC`, `DEJA_ORCHESTRATOR_URL`,
`HARNESS_STATE_DIR=/deja/work`, `DEJA_CODE_BUNDLE_URI`, `DEJA_EXPECTED_SCHEMA_HEAD`,
`DEJA_PARITY_{CRYPTO_EPOCH_RECORD,CRYPTO_EPOCH_ENV,REDIS_PREFIX_RECORD,REDIS_PREFIX_ENV,SCHEMA_HEAD,
REQUIRE}`, `DEJA_S3_ACCESS_KEY=""`, `DEJA_S3_SECRET_KEY=""`, `DEJA_RUNNER_ACTOR`; and on the router
`ROUTER__DEJA__{MODE=replay,RUN_ID,REPLAY__SOURCE,REPLAY__OBSERVED_SINK}`.

**API body (Layer A).** ADD `recording`, `candidate`, `env`?, `correlations`?; KEEP `expectation`
(audit-only), `mode` (default replay), `shard_total`?; legacy `candidate_spec` object still
accepted. `RunSpec` ADD `resolved: Option<ResolvedRefs>`, `shard_total`/`shard_index`.

**Behaviour changes.** `load_ambient_template()` default now EMPTY (not `demo_defaults()`);
`drive_record` no longer seeds `settlement_rate_default` (→ `demo/workload.sh`); runner default
`HARNESS_STATE_DIR` `/workspace/state` → `/deja/work`; `DEJA_DB_URL` mandatory in k8s/local;
`v1_create_run` enqueues (not `spawn_worker`) in scheduler modes; Dockerfile migration set swaps to
`vendor/hyperswitch @ ff191d7f79` (496).

**New Rust modules.** `lifecycle/{k8s,preflight,patch,parity}.rs`, `resolve.rs`, `scheduler/{mod,
backend}.rs`; `deja-store/migrations/0006_run_scheduling.sql`. **New infra:**
`helm-charts/charts/replay-env/*`, `apps/{replay-orchestrator,replay-job}/terragrunt.hcl`,
`argo-sandbox/{apps/sandbox,projects}/replay-env.yaml`. **New ops:**
`demo/overlays/local-pod/docker-compose.pod.yml`, `demo/run-deja-local-pod.sh`,
`ops/orchestrator/replay-job.golden.json`.

---

# Amendment A — recovered adversarial findings (§11 was written without them)

The planning workflow's adversarial phase ran, but the four reviewers returned
prose instead of the structured shape the postprocessing expected, so their
findings were silently dropped and §11 above was synthesized from the pre-seeded
A1–A6 list only. The real findings are below, recovered from the run journal and
**each re-verified against the cited files this session**. Severity is the
reviewer's; the checkmark is my independent confirmation.

## NEW BLOCKER — not on the A1–A6 list

### V1 [BLOCKER, verified ✓] redis seed materializer corrupts every recording-derived non-null redis value
`render_redis_seed_value` (`lifecycle/mod.rs:2342`) matches only
`serde_json::Value::String`; everything else falls to `other.to_string()`. The
redis seed branch (`mod.rs:1476`) calls it on `entry.value`, which for a recorded
redis GET **hit** is the externally-tagged `DejaRedisValue`, i.e.
`{"BulkString":[..bytes..]}`. So the wrapper *text* is `redis-cli SET` into the
store, not the raw bytes.

Verified against a real tape (`demo/harness-state/1783513055/.../events.jsonl`): a
GET hit records `result: {"BulkString":[48,49,57,...]}` — those bytes are ASCII
`019f41b1-220e-7282-…`, a UUID — and the event carries **no decoded `image`
field**, so the wrapper is the only source and it reaches the renderer verbatim.
On replay the router (EXECUTE strategy) reads back the literal `{"BulkString":…}`
text and branches on garbage → its scored output diverges. A pure harness
artifact — a **false divergence** — untouched by any parity gate (P2/P3/P5/P6 all
concern schema/crypto/prefix, never value encoding).

Why the demo never caught it: every redis seed in the demo tapes is
`origin:"ambient"` (a bare string from the ambient template, which renders
correctly); there is not one `origin:"recording"` redis seed. A real sandbox tape
reads pre-existing merchant configs / tokens / routing caches out of redis before
writing — every one will be corrupted.

*Fix:* reconstruct the raw redis bytes from `DejaRedisValue` (invert
`into_supported_redis_value`: `BulkString→bytes`, `Int→ascii`, …) — the same
decode path replay already uses — instead of `to_string()`-ing the wrapper.

## Scale + lifecycle (attacker 1) — the tracking/reconciler design cannot hold its 3 headline properties

- **V2 [HIGH, verified ✓] no durable, at-least-once, idempotent runner→store channel.** Push-back is fire-and-forget (`store_ctx.rs:226`, `if let Err(e) = req.send_json(&ev)`), the scorecard rides its own `Result` POST before `Finish`, and recovery recomputes from the orchestrator's local fs which is empty for a Job. A 90-minute run that *catches a real regression*, then loses the `Result` POST to a transient blip, settles `completed` with `verdict=NULL` — a silent false-negative, unrecoverable once the pod emptyDir is gone.
- **V3 [HIGH] requeue mints a new `job_name`, defeating 409 idempotency → two live Jobs for one `run_id`.** The DB claim and the k8s create-POST are not one transaction; a transport error after the apiserver persisted the Job, or a `mark_active` failure, drives a differently-named twin. Both twins share `REPLAY__SOURCE`/`OBSERVED_SINK` keyed on `RUN_ID` → interleaved seeding + two conflicting `Result` events. (SPINE-8 "retry same-name" and Scheduler-S5 "requeue → new name" **contradict** on this exact failure.)
- **V4 [HIGH, verified ✓] reconciler settle is a blind UPDATE with no terminal guard.** `update_run_state` (`lib.rs:243`) has no `WHERE state NOT IN ('completed','failed')`. A reconcile tick that read the Job as momentarily Missing can overwrite a just-arrived `completed` (real pass) with `failed`. TOCTOU, reachable every tick.
- **V5 [MED-HIGH, verified ✓] SIGTERM is never caught.** `shutdown_signal` (`main.rs:175`) awaits only `ctrl_c()` (SIGINT); k8s sends SIGTERM on every rolling update / termination, so graceful drain is dead code in-cluster and all in-flight push-backs in the grace window are lost (feeds V2). The infra area mandates `strategy: Recreate`, guaranteeing a down-window on every deploy.
- **V6 [MED] the concurrency ceiling is not actually enforced** across claimers (SKIP LOCKED dedupes rows, not the aggregate count gate under Read-Committed); nothing elects a singleton, so a stray scale/HPA/rolling-slip silently over-admits Jobs.
- **V7 [MED] correlation sharding pays N× full-tape pull + N× full migration** to parallelize a serial kernel: `stage_resolve_recording` pulls the whole tape before the filter applies (`mod.rs:717` vs filter at `:948`), and one big fan-out head-of-line-blocks the global FIFO.

## Monday infra-PR (attacker 2) — the branch in the tree is the SUPERSEDED DinD v1

- **V8 [BLOCKER, verified ✓] none of the design is in the working tree.** `dind.enabled: true`, `privileged: true` (`templates/deployment.yaml:93`); no `helm-charts/charts/replay-env/`; no `terraform/.../apps/replay-*` unit; zero `replay-sbx`/`replay-job-role`/`replay-env` strings. Handed over as-is Monday it delivers the *opposite* of every locked decision, and carries a stray breaking Vector-sink diff. **The PR must be built before it can be handed over — it does not exist yet.**
- **V9 [BLOCKER] Job ServiceAccount name is contradictory across areas** — `replay-job-role` (infra area) vs `replay-orchestrator-job-role` (spine area). IRSA is a byte-match of `system:serviceaccount:<ns>:<sa>`; disagreement fails every Job's S3 pull + crypto ESO closed. Reconcile to ONE name.
- **V10 [MAJOR] the orchestrator's own `replay-orchestrator-api` token secret is an unlisted dependency** — nothing produces it, it's not in the ask-list; a missing `secretKeyRef` target is `CreateContainerConfigError`, the orchestrator never boots.
- Verified **SOUND** (do not chase): the push-back URL `sandbox-replay-orchestrator.replay-orchestrator-sandbox.svc:80` is correct on all three axes; the tfstate underscore-key/hyphen-dir split is real; the terraform module supports `create=false`+existing-bucket and exports `role_arn`; the crypto `sandbox/hyperswitch` path is real.

## Config + local parity (attacker 3)

- **V11 [HIGH, verified ✓] sha_R cannot be resolved for the aggregator (date-partitioned) layout — the headline input.** `read_manifest` (`compactor/lib.rs:319`) GETs only the session-layout `manifest.json`; the aggregator prefix has none (`pull_recording_from_prefix` returns `sealed:false`, no manifest). So the ideal body `recording: "s3://hyperswitch-art/2026/07/11"` → manifest NotFound → P0 refuses **every** run. CS3 (manifest always present) contradicts SPINE-4 (envelope CodeProbe). And the envelope route only works if the record env set `identity.code_sha` — which sandbox does not. The two-param promise leaks a record-time dependency.
- **V12 [HIGH, verified ✓] the local pipeline's A3 guard cannot fire in its fast tier.** Tier-1a runs a host-native runner + a container router mounting the same dir at different absolute paths, so `root.lookup_table_path` (host) can never string-equal `REPLAY__SOURCE` (container `/state/…`). The guard that is supposed to make "local success predict cluster success" for the live-traffic-leak bug is always-false in the tier billed as the inner loop.
- **V13 [MED-HIGH] `DEJA_EXECUTOR` is defined four incompatible ways** across the four areas (`compose|k8s` / `compose|local|k8s` / `compose|external` / `k8s`); the spine's default-to-compose fallthrough re-introduces the double-drive bug the local pipeline exists to prevent. Must be ONE enum with a "not-compose ⇒ never `spawn_worker`" rule.
- **V14 [MED] the headline candidate `deja-pr-patch-real-change` resolves to the git-egress tier** that can't run Monday, and the branch-vs-tag shape heuristic can't disambiguate it from a bare tag; a wrong guess is an ImagePullBackOff caught only at pod-pull, no preflight. **V15 [LOW] a bare recording prefix mis-parses the bucket** (`to_config` treats segment 0 as the bucket).

## Verdict-correctness (attacker 4), beyond V1
- **V16 [MED] P5 (redis-key-prefix parity) is inert:** the record side never stamps `redis_key_prefix` (the manifest has no such field), so P5's both-None → pass, while a tape recorded under a non-empty tenant prefix replayed against the `""` router misses every seeded redis read → false divergence, ungated.
- **V17 [LOW-MED] P2 "schema head" is diesel `MAX(version)`, not set-equality:** a bundle missing interior migrations but carrying the top one passes P2. Use a hash of the sorted migration filenames, not just the head.
