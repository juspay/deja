# In-Cluster Replay-Orchestrator — Ratified Execution Plan

## Executive summary
We are deploying the `replay-orchestrator` service into the sandbox EKS cluster under ArgoCD GitOps, so a user triggers a replay from its dashboard and the orchestrator spawns **one Kubernetes Job per run** (per-Job pg + redis + frozen-router native-sidecars + a runner container), talking to the k8s API by **raw REST over ureq/rustls with the pod ServiceAccount token** — no kube-rs, no kubectl, no DinD. The single riskiest unknown is **whether the sandbox EKS control plane is ≥ 1.29**: the entire Job shape relies on native sidecars (initContainers with `restartPolicy: Always`) so the never-exiting pg/redis/router are auto-terminated when the runner exits; on an older cluster the pod never Completes and the shape must be redesigned. Second-order risks are all durability/plumbing: the orchestrator currently runs with an ephemeral emptyDir and `store=None`, so run state and pushed-back results are lost on any pod restart or rolling update. State of the world: the **in-pod replay driver, RunSpec, S3 pull, push-back protocol, and Pg store are BUILT but the Pg store is disabled and the K8sJobExecutor, the router-start sentinel, durable state, and the dashboard-prefix fix are GAPs**. We de-risk by proving every code path locally, then validating the cluster spine with a one-container echo Job before the full sidecar stack is ever on the line.

Legend: **[DECIDED]** = ratified direction · **[BUILT]** = exists and works today · **[GAP]** = must be written this cycle.

---

## Blockers requiring the USER (kick off first — these have lead time)

- [ ] **O1 — EKS server version ≥ 1.29.** Native sidecars are GA at 1.29 (beta-on 1.28). If older, the Job never Completes and the manifest shape is invalid. `kubectl get --raw /version | jq '.major,.minor'`. **Structural gate — do this before anything hardens on the sidecar shape.**
- [ ] **O2 — Namespace Pod Security Admission enforced level.** ArgoCD creates `replay-orchestrator-sandbox` bare (`CreateNamespace=true`, no PSA labels) so it inherits the cluster default. If default enforce is `restricted`, admission rejects both the root-running orchestrator pod and every root-running pg/redis Job pod. Determine the enforced level (see I3 for the in-repo fix).
- [ ] **O3 — Node instance role ECR pull.** IRSA does **not** grant image pulls; the kubelet uses the node role. Confirm the sandbox node role has `ecr:GetAuthorizationToken` + `ecr:BatchGetImage` for `223655089699.dkr.ecr.ap-south-1.amazonaws.com`, else we attach `imagePullSecrets` to the Job SA.
- [ ] **O4 — ECR repo** for the orchestrator/runner image at `223655089699.dkr.ecr.ap-south-1.amazonaws.com/<repo>`, with push creds.
- [ ] **O5 — Image mirrors, digest-pinned.** pg, redis, and the candidate `juspay/hyperswitch:deja-pr@ff191d7f79` must be ECR- or `public.ecr.aws`-resident. Kubelet pulls reach DockerHub via NAT (proven: hyperswitch-app runs docker.io bitnami here), so docker.io *works* but shares one anonymous rate-limit budget across nodes — mirror to avoid 429/ImagePullBackOff bursts. Use the exact tags hyperswitch-app runs (`bitnamilegacy/postgresql:16.1.0-*`, `redis:7.2.3-*`) or `public.ecr.aws/docker/library/{postgres:16,redis:7}`, pinned by digest.
- [ ] **O6 — Two IRSA terraform roles + tfstate export.** `replay_orchestrator` (orchestrator SA → S3 rw on hyperswitch-art) and a separate least-privilege role for the Job SA (S3 read for the recording pull only — do NOT reuse the orchestrator SA, which will hold jobs:create/delete). Each must export `role_arn` at `s3://<bucket>/sandbox/ap-south-1/application-stack/apps/replay-orchestrator/terraform.tfstate` so the vals plugin resolves `$<replay_orchestrator.role_arn>$`. Token key spelling (underscore) must equal the `$tfstate` param key.
- [ ] **O7 — Secrets + crypto-parity decision.** Create `replay-orchestrator-api/service_token` (= the orchestrator's `DEJA_API_SERVICE_TOKEN`; a mismatch makes every push-back 401 and runs look stuck). **Correctness precondition, not an open item:** determine whether the target tape was recorded with the demo keys already baked into `docker_compose.toml` (then the mounted toml suffices) or with different keys (then create `replay-crypto` with `ROUTER__SECRETS__MASTER_ENC_KEY` + `ROUTER__API_KEYS__HASH_KEY`). A mismatch diverges auth/decrypt *before* the replayed flow and silently invalidates the verdict.
- [ ] **O8 — Migration/schema parity gate (correctness).** The runner bundles `vendor/hyperswitch-deja-clean/migrations` (461 entries); this is **not proven identical** to the frozen candidate `ff191d7f79` schema. Extract the candidate image's migrations (or `diesel migration run` on a scratch pg then `diesel print-schema`) and `diff` clean. **Blocks trust in the first real replay verdict, independent of whether the Job boots.**
- [ ] **O9 — Persistent Postgres for orchestrator run-state.** Required to fix the run-orphaning blockers (see C6/I5). Provision an RDS instance or a persistent in-cluster PG and hand the URL to the chart as `DEJA_DB_URL`. Alternatively (weaker) a RWO PVC for `HARNESS_STATE_DIR` — but the store is needed to serve k8s-run scorecards, so PG is the recommendation.
- [ ] **O10 — Confirm S3/STS/ECR reach via VPC endpoints, not squid.** Squid is an *explicit* forward proxy; being on its dstdomain allowlist does nothing for a client that sets no `HTTP(S)_PROXY` (the orchestrator/runner don't). AWS paths work only because gateway (S3) + interface (STS, ECR) VPC endpoints exist. Verify object_store's web-identity uses the **regional** `sts.ap-south-1.amazonaws.com` (which has an interface endpoint) with a real IRSA pull; global `sts.amazonaws.com` has no path out.
- [ ] **O11 — Infra PR approval** on `hyperswitch-infra` branch `deja-custom-pod-deployment`. No push without explicit USER approval.

---

## Phase 1 — Outer repo code (all local; ComposeExecutor stays the default so every intermediate commit leaves the orchestrator deployable) **[CODE]**

Invariant **[DECIDED]**: `DEJA_EXECUTOR` defaults to `compose`. Until we flip it to `k8s` in-cluster, every merge is a safe refactor.

### C1 — Runner writes the router-start sentinel **[CODE]** (GAP; #1 boot blocker)
`drive_replay_in_pod` (`crates/deja-orchestrator/src/lifecycle/mod.rs:879-952`) goes render→migrate→seed→wait_health with **no sentinel write anywhere**, and `HarnessRoot::new` (`crates/deja-orchestrator/src/lib.rs:182-194`) creates only `[runs,recordings,lookup-tables,observed,http-diffs]`. The router's boot gate `until [ -f /workspace/state/ready/router-start ]` would hang forever.
- Add `"ready"` to the subdir list in `HarnessRoot::new`; add a `ready_sentinel_path()` helper.
- In `drive_replay_in_pod`, `mkdir -p $HARNESS_STATE_DIR/ready && touch .../router-start` **immediately before** `wait_health(opts.router_port)` (~mod.rs:940), i.e. after migrate+seed so the router boots against a seeded store.
- **Verify:** `cargo test -p deja-orchestrator` — new unit test asserts `HarnessRoot::new` creates `{root}/ready` and the sentinel path resolves; grep confirms the touch precedes `wait_health`.

### C2 — RunExecutor seam + ComposeExecutor + `executor_from_env` **[CODE]** (GAP)
Cut the seam at `crates/deja-orchestrator/src/api/runs.rs:40` `spawn_worker` (single caller `main.rs:314`, needs zero change).
- `pub trait RunExecutor: Send + Sync { fn launch(&self, root:&HarnessRoot, run_id:&str, ctx:StoreCtx); }` — byte-identical to `spawn_worker`'s params. Three types cross: `&HarnessRoot`, `&str`, `StoreCtx` by value (Clone+Send, already moved into a thread today).
- `ComposeExecutor` = unit struct; `launch` is the current `spawn_worker` body verbatim (clone `root.root`, `std::thread::spawn`, re-open `HarnessRoot::new`, `lifecycle::drive`). It stays the **only** Record-capable path.
- `executor_from_env() -> Box<dyn RunExecutor>` keyed on `DEJA_EXECUTOR=compose|k8s` (default `compose`). Replace `spawn_worker`'s body with `executor_from_env().launch(root, run_id, ctx)`.
- **Verify:** `cargo test` green; unit test asserts selection; an existing compose demo run still completes unchanged with `DEJA_EXECUTOR` unset.

### C3 — Dependency-free k8s REST client **[CODE]** (GAP)
New module `crates/deja-orchestrator/src/lifecycle/k8s.rs`. **No Cargo.toml change** — use `ureq`'s re-exports (`ureq::rustls`, `ureq::rustls::pki_types::pem::PemObject`); rustls-pemfile/direct-rustls are NOT needed and a direct `rustls` dep would drag aws-lc-rs → provider-ambiguity panic.
- Build `ClientConfig` exactly like ureq's own default: `builder_with_provider(ring::default_provider().into()).with_protocol_versions(&[&TLS12,&TLS13]).unwrap().with_root_certificates(roots).with_no_client_auth()`; `AgentBuilder::new().timeout_connect(10s).timeout_read(15s).tls_config(Arc::new(cfg)).build()`. **[fixes: no-read-timeout wedge]**
- Root store: parse `ca.crt` via `CertificateDer::pem_slice_iter`; **assert `added >= 1`** and fail-fast at construction with an explicit "cluster CA produced 0 certificates" error; surface CA-load failure to `/healthz`. **[fixes: silent empty-cert store]**
- **Read the SA token file fresh on every request**; rebuild the `Authorization: Bearer` header per call. Never store the token in the Agent/executor struct (the codebase's `StoreCtx` token-caching shape at `store_ctx.rs:162` is the anti-pattern to avoid — projected token rotates ~1h and runs outlast it). CA + namespace read once. **[fixes: SA-token expiry 401]**
- **Bracket IPv6 hosts:** if `KUBERNETES_SERVICE_HOST` parses as an IPv6 literal, compose `https://[{host}]:{port}`. **[fixes: IPv6 URL]**
- API surface: `create_job` (POST `/apis/batch/v1/namespaces/{ns}/jobs`; treat 409 as idempotent-ok), `job_status` (GET `.../jobs/{name}`; terminal = `.status.conditions[]` `type∈{Complete,Failed}, status=="True"`, **not** the succeeded/failed counters), `delete_job`, and `pod_logs` (failure backstop only).
- **Error taxonomy [DECIDED]:** distinguish `Error::Status` (HTTP) from `Error::Transport` (reset/timeout/DNS/TLS). Never derive Job failure from a transport error, a 401, or a 5xx — retry those with capped backoff bounded by `activeDeadlineSeconds`. Only settle terminal failure from an actual `Failed` condition. **[fixes: poll conflates transport/auth with Job failure]**
- **Verify:** `git diff Cargo.toml` empty; `grep -c aws-lc-rs Cargo.lock == 0`; unit test builds the Agent against a generated ca.crt (the proven `scratchpad/ureqprobe` pattern) and asserts two time-separated calls each re-read the token path.

### C4 — batch/v1 Job renderer, golden-tested **[CODE]** (GAP)
Pure fn `(RunSpec, K8sConfig, shape: Echo|Replay) -> serde_json Job`. Reference shape: `ops/orchestrator/replay-job.yaml`.
- **Replay shape:** 3 native sidecars (pg, redis, router as initContainers `restartPolicy: Always`) + runner container; one shared `/workspace/state` emptyDir in runner AND router; router config via **per-file subPath** ConfigMap mounts (`docker_compose.toml`/`superposition_seed.toml`/`payment_required_fields_v2.toml`) so the frozen image's baked `/local/config` is not shadowed; router command = sentinel-gate `sh` loop + `exec router -f docker_compose.toml`, **no probes**; `ROUTER__DEJA__REPLAY__SOURCE=/workspace/state/lookup-tables/{run_id}.jsonl` + `OBSERVED_SINK=/workspace/state/observed/{run_id}.jsonl` (must equal `HarnessRoot::lookup_table_path`/`observed_path`).
- **`DEJA_S3_ACCESS_KEY: ""` and `DEJA_S3_SECRET_KEY: ""` (empty strings, not omitted)** — `S3Config::from_env` defaults to `minioadmin` and `has_static_credentials()` is true for any non-empty value, masking IRSA → 403. **[fixes: IRSA minioadmin 403]**
- **`sidecar.istio.io/inject: "false"` on the Job POD TEMPLATE `metadata.labels`** (and as an annotation, defensively) so completion is independent of whether the namespace is ever mesh-labeled. **[fixes: istio sidecar never exits]**
- `serviceAccountName` = the Job SA name (see below); `DEJA_RUNNER_ACTOR` stamped explicitly (doc/code default drift); deterministic Job name from `run_id` (409 idempotent).
- `backoffLimit: 0` **[DECIDED — fixes: silent double-execute]**; `restartPolicy: Never`; `activeDeadlineSeconds: 7200` (default; must exceed max serial-kernel wall-clock); `ttlSecondsAfterFinished: 86400` (>> poll interval and >> orchestrator-restart gap) **[fixes: ttl races poll]**.
- **`DEJA_ORCHESTRATOR_URL`** injected from a chart-rendered config value (see I2), NOT hardcoded. Runner appends `/api/v1/runs/{id}/events`.
- Emit `imagePullSecrets` into the pod spec **if** O3 shows the node role lacks ECR pull.
- **Echo shape:** single container (runner ECR image), command `sh -ec` that curls one `Finish{ok:true}` RunEvent to `$DEJA_ORCHESTRATOR_URL/api/v1/runs/$DEJA_RUN_ID/events` with the Bearer token, then exits 0. Selected by env `DEJA_JOB_SHAPE=echo|replay` (default `replay`).
- **Verify:** golden test — render for a fixed RunSpec, diff committed `ops/orchestrator/replay-job.golden.json`; explicit assertions on the sentinel-gate command string, blank `DEJA_S3_*`, `inject:"false"` on pod template, ECR image refs, `backoffLimit:0`, and SOURCE/OBSERVED path equality with `HarnessRoot`.

### C5 — K8sJobExecutor: launch + poll backstop + startup reconciler **[CODE]** (GAP)
- **`launch` spawns a `std::thread` FIRST**, exactly like ComposeExecutor, and does read_json + render + create-POST + poll **entirely inside that thread** so `launch` returns immediately. `v1_create_run` is `#[tokio::main]` async and calls `spawn_worker` inline; a blocking ureq POST on the async worker would park runtime threads and flap `/healthz`. **[fixes: blocking ureq on tokio worker]**
- Fail-fast if `run.spec.mode == Record` via `ctx.finish(false, ...)` — no in-pod record driver exists.
- Read the Run off disk (`read_json::<Run>(root.run_path(run_id))`) to recover `RunSpec`; re-serialize into `DEJA_RUN_SPEC`.
- **Pin the Job namespace to the orchestrator's own namespace** (read `/var/run/secrets/.../namespace`); reject a differing `DEJA_JOB_NAMESPACE` — a namespaced Role cannot authorize cross-namespace create (403). **[fixes: cross-namespace 403]**
- Poll (10s interval) as **liveness backstop only** — normal progress arrives via the in-pod runner's push-back into the same store, so the executor must not re-report. Backstop settles `ctx.finish(false, ...)` **only** on a `Failed` condition, guarded against a run already settled by push-back (idempotent finish). Treat **"a Job previously observed active, now 404 with no recorded terminal"** as failure (GC'd-before-observed or SA-rejected). Treat **"0 active pods + `FailedCreate` events"** as launch failure (GET events, `ctx.finish(false)`). On genuine give-up, `delete_job` (RBAC grants it) before finishing so nothing orphans. **[fixes: ttl-races-poll, missing-SA hang, transport-vs-failure]**
- **Startup reconciler:** on boot, list Jobs by label `app=deja-replay`, re-attach a poll thread to each still-active Job, and settle any durable non-terminal run whose Job is terminal-or-gone. Depends on durable state (C6). **[fixes: orphaned Jobs after orchestrator restart]**
- `K8sConfig::from_env()` per-launch (fresh token per run) **[DECIDED]**.
- **Verify:** integration test against a fake apiserver (tiny axum, canned 201 then Complete/Failed): a replay run yields exactly one POST and settles; a record run yields `ctx.finish(false)` and zero POSTs; a push-back-settled run is not re-finished; a regression assertion that `launch` returns without network I/O on the caller's thread.

### C6 — Durable run-state + result persistence **[CODE]** (GAP; answers the run-orphaning blockers)
The store (`StoreCtx::Pg`, `apply_run_event`, `store/migrations`) is **BUILT but disabled** — `DATABASE_URL` is omitted for v1 so `store=None`, and `HARNESS_STATE_DIR` is an emptyDir. That combination means an orchestrator restart or rolling update loses the file record; push-back then 404s (`main.rs:345-348`) and terminal Finish/Result are dropped; ttl GC destroys the only copy of the scorecard.
- Enable the store: consume `DEJA_DB_URL` (O9). Confirm `Store::connect` succeeds → `store=Some`.
- **Persist `RunEvent::Result` and `RunEvent::Artifact`** — today they fall into the `_ => false` arm at `main.rs:380-382` and never touch the store. Route them through `apply_run_event` so verdict/scorecard survive pod-local emptyDir loss.
- **`v1_ingest_run_event`: reconstruct/upsert a missing file record from the store** instead of returning 404, so late events after an orchestrator restart are not lost.
- **`v1_scorecard`: for k8s runs, serve from the stored Result JSON**, not by recomputing `divergence::scorecard(&st.root, &id)` from the orchestrator's local fs (which holds none of the Job-pod artifacts). (Landing the #21 S3 artifact upload is the alternative durable sink; DB-served scorecard is the smaller change and the recommendation.)
- **Verify:** integration test — ingest a Result event, restart-simulate (drop the file record), re-ingest a follow-up event, assert the run is reconstructed and `GET /scorecard` returns the stored verdict.

### C7 — Dashboard behind the `/replay-orchestrator/` gateway prefix **[CODE]** (GAP)
D3 (vite base) is necessary but **not sufficient** — three more root-absolute breakages exist that `base` cannot touch.
- `web/vite.config.ts`: `base: '/replay-orchestrator/'` (fixes emitted asset URLs).
- `web/src/lib/api.ts`: `const API_BASE = import.meta.env.BASE_URL`; build every request as `${API_BASE}api/v1/...` (BASE_URL keeps its trailing slash — no leading slash on the suffix). All data calls are hardcoded `/api/v1/...` literals today.
- `web/src/main.tsx:71`: pass `basename: import.meta.env.BASE_URL` to `createBrowserRouter` (else routes never match behind the prefix and the shell renders empty).
- `web/src/pages/RunDetailPage.tsx`: BASE_URL-prefix the artifact `<a href>` and viz `<iframe src>`.
- `web/src/pages/ScorecardPage.tsx`: replace `window.location.href` hard-nav with router `navigate()`.
- `cd web && npm run build`, commit the regenerated `web/dist`, then recompile the orchestrator (rust-embed bakes dist at compile time, `main.rs:47-49`). The VirtualService `rewrite uri: /` and axum route table stay unchanged.
- **Verify:** `web/dist/index.html` emits `src="/replay-orchestrator/assets/..."`; the built bundle contains `/replay-orchestrator/api/v1`; a local prefix-rewriting proxy loads the runs list (data calls 200) and deep links render.

### C8 — Runner image: add store clients, drop DinD, slim vendor **[CODE]** (GAP; D1)
`ops/orchestrator/Dockerfile`. The in-pod `StoreExec::Direct` path shells `psql`/`redis-cli`/`diesel` **inside** the runner (compose ran them inside store containers) and the current image lacks them.
- **ADD:** `postgresql-client` (brings `psql` + `libpq5`, also satisfies the diesel binary's `libpq.so.5`), `redis-tools` (`redis-cli`), `install -m0755 demo/.diesel-cli/bin/diesel /usr/local/bin/diesel`.
- **REMOVE (D1):** the docker apt-repo + `docker-ce-cli` + `docker-compose-plugin` block and `DOCKER_HOST` env (dead in the Job path).
- Keep `deja-orchestrator`, `deja-runner`, `./target/release/deja-kernel` under unchanged `WORKDIR /workspace/repo` (kernel default path is relative) **or** set `RUNNER_KERNEL_BIN=/usr/local/bin/deja-kernel`.
- Slim the vendor COPY to `vendor/hyperswitch-deja-clean/{migrations,diesel.toml}` only — **must not drift the migration set** (gated by O8).
- **Verify:** `docker build` succeeds; `docker run --rm IMG sh -c 'command -v psql redis-cli diesel deja-runner deja-kernel'` resolves all five; `command -v docker` fails.

### C9 — Local end-to-end proof of the in-pod driver **[CODE]** (validates C1 + the migrate/seed/health/score chain off-cluster)
Before any image ships, run `cargo run --bin deja-runner` against **local** pg + redis + the candidate router on localhost, with a real RunSpec and a pulled recording, env per the built contract (`RUNNER_DATABASE_URL`, `RUNNER_MIGRATE_CMD=diesel migration run --migration-dir .../migrations --config-file .../diesel.toml`, `HARNESS_STATE_DIR=/workspace/state`, `DEJA_ORCHESTRATOR_URL` → a locally-run orchestrator, `DEJA_S3_*`).
- **Verify:** runner exits 0; `$HARNESS_STATE_DIR/ready/router-start` exists before wait_health; the local orchestrator received RunEvents and the run reaches terminal Scored reading a non-empty observed file. This is the last time the full in-pod path runs before the approval-gated cluster sync.

---

## Phase 2 — Infra PR (`hyperswitch-infra`, branch `deja-custom-pod-deployment`, needs O11 approval) **[INFRA]**

### I1 — Strip DinD (D1)
- `helm-charts/charts/replay-orchestrator/values.yaml`: delete the `dind:` block (79-110) and the DinD `podLabels` rationale.
- `templates/deployment.yaml`: remove `DOCKER_HOST` env (61-64), the privileged `dind` sidecar (85-114), the app-container dind volumeMounts (75-80), and the `docker-run`/`workspace`/`dind-storage` emptyDirs (117-124). Collapse the `if or .Values.dind.enabled ...` guards to the plain `volumeMounts`/`volumes` conditions.
- `templates/_helpers.tpl`: delete the unused `replay-orchestrator.dindImage` define (74-79).
- `infra-configurations/replay-orchestrator/sandbox-values.yaml`: delete the `dind:` block (52-60).
- **Verify:** `helm template ... -f infra-configurations/replay-orchestrator/sandbox-values.yaml` shows no `dind` container, no `DOCKER_HOST`, no `privileged: true`.

### I2 — RBAC on + IRSA + Job SA + executor knobs + push-back URL + blank S3 keys + Vector revert + docs
- **RBAC (D2):** `rbac.create: true` in both values files; keep `serviceAccount.automount: true`. Pre-staged rules (`batch/jobs: create,get,list,watch,delete` + `pods,pods/log: get,list,watch`) match D2; `role.yaml`/`rolebinding.yaml` are values-gated (no template edit).
- **Orchestrator IRSA (D4):** `serviceAccount.annotations.eks.amazonaws.com/role-arn: $<replay_orchestrator.role_arn>$` (name `replay-orchestrator-role`).
- **Job SA (NEW template)** `templates/job-serviceaccount.yaml`: a second SA **`replay-orchestrator-job-role`** carrying only the IRSA annotation, **no Role/RoleBinding** (the runner never calls the k8s API), `automountServiceAccountToken: false`. Gate on a `jobServiceAccount:{create,name,annotations}` block. **The executor's `DEJA_JOB_SERVICE_ACCOUNT` default MUST be byte-identical to this name** — reconcile away the scratchpad's `replay-runner` vs chart's proposed name; **`replay-orchestrator-job-role` is DECIDED.** Ship AND enable it before the first run, or the ServiceAccount admission controller rejects every Job pod (Job created, 0 pods, no Failed condition, poll hangs). **[fixes: missing/mismatched Job SA]**
- **Push-back URL (blocker):** add a config key the orchestrator injects into the Job, rendered from the real Service — `DEJA_ORCHESTRATOR_CALLBACK_URL: http://{{ include "replay-orchestrator.fullname" . }}.{{ .Release.Namespace }}.svc.cluster.local:80` → `http://sandbox-replay-orchestrator.replay-orchestrator-sandbox.svc.cluster.local:80`. Use **port 80** (the Service port; targetPort 8080 is not answerable on the ClusterIP). The executor copies this into the Job's `DEJA_ORCHESTRATOR_URL`. **[fixes: NXDOMAIN + wrong port push-back]**
- **Blank S3 keys, don't just delete secretRefs (D4):** in `deployment-configs/replay-orchestrator/sandbox-values-dep.yaml` remove the `DEJA_S3_ACCESS_KEY`/`SECRET_KEY` `_secretRef`s to `replay-orchestrator-aws` **and set both to `""` explicitly** in the orchestrator config, so `has_static_credentials()` is false and the web-identity chain is used. Deletion alone leaves the `minioadmin` default in force. Keep `DEJA_S3_ALLOW_HTTP:false`, `HARNESS_BIND`, `HARNESS_STATE_DIR`. **[fixes: IRSA minioadmin 403 at orchestrator]**
- **Executor knobs (flat `configs:` keys, prefix-less):** `DEJA_EXECUTOR=k8s`, `DEJA_JOB_SHAPE=echo` (flipped to `replay` at R5), `DEJA_JOB_NAMESPACE` (default `.Release.Namespace`), `DEJA_RUNNER_IMAGE`, `DEJA_JOB_SERVICE_ACCOUNT=replay-orchestrator-job-role`, `DEJA_JOB_TTL_SECONDS=86400`, `DEJA_JOB_ACTIVE_DEADLINE_SECONDS=7200`, `DEJA_RUNNER_ACTOR`.
- **D5 revert:** `git checkout -- infra-configurations/vector/sandbox-hyperswitch-art-s3.yaml` (drop the uncommitted sink rewrite; replay ingests the deployed layout).
- **Docs:** rewrite `Chart.yaml:3` description and `DEPLOY-NOTES.md` off the DinD model.
- **Verify:** `helm template` renders Role+RoleBinding with the batch/pods verbs, orchestrator SA with role-arn, a separate RBAC-less job SA with `automount:false`; `git diff` shows the Vector file reverted and S3 keys blanked (not omitted).

### I3 — Namespace Pod Security Admission label (conditional on O2)
The namespace is Argo-created bare; there is no `namespace.yaml` in the chart and no `managedNamespaceMetadata` in the repo. If O2 shows enforce > `baseline`, set `spec.syncPolicy.managedNamespaceMetadata.labels['pod-security.kubernetes.io/enforce']` in `argo-sandbox/apps/sandbox/replay-orchestrator.yaml` to `privileged` (pragmatic; pg/redis run as root) or `baseline`. This is the only in-repo mechanism since the namespace is Argo-created. Do **not** rely on "DinD is gone so it passes" — `restricted` still needs `runAsNonRoot`/`seccompProfile`/`allowPrivilegeEscalation:false`/`drop:[ALL]`, none set, and stock pg/redis run as root. **[fixes: PSA rejects orchestrator + Job pods]**
- **Verify:** after first create, `kubectl get ns replay-orchestrator-sandbox -o jsonpath='{.metadata.labels}'` shows the intended enforce label; the O2 probe Job (native sidecar + inject:false + emptyDir) reaches Complete.

### I4 — Router config ConfigMap + real-Job image knobs
- Ship `replay-router-config` ConfigMap: `docker_compose.toml` + `superposition_seed.toml` + `payment_required_fields_v2.toml` (~313 KB, under 1 MiB), mounted per-file `subPath` (dir-mount would shadow the frozen image's `/local/config`).
- Real-Job flat `configs:` keys: `DEJA_CANDIDATE_IMAGE_DEFAULT=<ECR ref for deja-pr@ff191d7f79>` (per-run image already flows via `RunSpec.candidate_spec`), `DEJA_PG_IMAGE` + `DEJA_REDIS_IMAGE` (ECR/`public.ecr.aws`, digest-pinned, `imagePullPolicy: IfNotPresent`), per-container resources, Job node-affinity/tolerations mirroring `global.affinity` generic-compute.
- **Verify:** `helm template` renders the ConfigMap under 1 MiB and all image keys resolving to ECR/`public.ecr.aws` refs (no docker.io defaults).

### I5 — Durable orchestrator state + safe rollout
- Set `DEJA_DB_URL` to the persistent PG (O9) so run rows/results survive pod loss.
- Mount `HARNESS_STATE_DIR` on a RWO PVC (safe at `replicaCount: 1`) for lookup-tables/artifacts continuity.
- Set `strategy: Recreate` (or `maxSurge: 0`) so old and new orchestrator pods never coexist while a Job pushes back. **[fixes: push-back lost on rolling update]**
- Add a real `readinessProbe` httpGet `/api/v1/healthz` (currently `{}`) so a not-yet-ready pod isn't Service-eligible.
- **Verify:** `helm template` shows the Deployment with `DEJA_DB_URL`, the PVC + mount, `strategy: Recreate`, and a `/api/v1/healthz` readiness probe.

### I6 — ArgoCD `$tfstate.replay_orchestrator` param (for IRSA resolution)
Add a `$tfstate.replay_orchestrator` drySource param (`value: s3://{{s3Bucket}}/sandbox/{{region}}/application-stack/apps/replay-orchestrator/terraform.tfstate`) to `argo-sandbox/apps/sandbox/replay-orchestrator.yaml` (currently passes only `$tfstate.istio`). Underscore key must match the `$<replay_orchestrator.role_arn>$` token. `argoapps-values.yaml` is already correctly patched (+2 lines). Hard prerequisite: O6 tfstate must exist or the sync fails on an unresolved token.
- **Verify:** the ApplicationSet lists `$tfstate.replay_orchestrator`; after O6, rendered manifests carry a concrete role-arn (no residual `$<...>$`).

---

## Phase 3 — Rollout (converges all gates) **[OPS] / [INFRA]**

### R0 — Native-sidecar + PSA + inject:false probe **[OPS]** (can run as soon as cluster access; gated on O1)
Apply a throwaway Job mirroring the manifest shape (initContainers `restartPolicy: Always` pg/redis stand-ins + emptyDir + `sidecar.istio.io/inject:"false"` on the pod template). Confirm the namespace PSA **admits** it and it reaches **Complete** (native sidecars auto-terminate when the main container exits). This validates the shared BIGGEST RISK cheaply — the echo skeleton (single container) cannot exercise native-sidecar admission or auto-termination.
- **Verify:** probe Job `.status` = Complete; no PSA rejection; no lingering pod.

### R1 — Build + push the single image to ECR **[OPS/CODE]**
One image serves the Deployment (orchestrator) and the Job runner (command override). Built at the Phase-1 head (C1–C8, dist re-embedded). **[BLOCKED_ON: O4]**
- **Verify:** `aws ecr describe-images` shows the digest; `docker run <tag> deja-orchestrator --help` works.

### R2 — First ArgoCD sync (`DEJA_JOB_SHAPE=echo`) **[INFRA/OPS]**
Merge the I1–I6 PR (O11 approval); Argo syncs. Confirm the orchestrator pod is Ready, the dashboard loads through the gateway with assets **and** data calls 200 under `/replay-orchestrator/`, the orchestrator SA can create jobs, and the IRSA S3 chain resolves. **[BLOCKED_ON: R1, O2/I3, O6, O7]**
- **Verify:** pod Ready on `/api/v1/healthz`; `curl https://host/replay-orchestrator/` returns the app (assets 200, runs list populates); `kubectl auth can-i create jobs --as system:serviceaccount:replay-orchestrator-sandbox:sandbox-replay-orchestrator` = yes; orchestrator log shows S3 reachable via web-identity (regional STS).

### R3 — Echo skeleton smoke **[OPS]** (keystone spine proof)
Trigger a minimal replay run (dashboard or `curl POST /api/v1/runs` with the service token). The orchestrator POSTs the single-container echo Job → the Job pulls the runner image from ECR, curls one `Finish` RunEvent back through in-cluster DNS with the service token, exits 0 → the poll thread sees the `Complete` condition and the ingested Finish settles the run. This proves, together and with clean attribution: **orchestrator-SA RBAC to create a Job, apiserver IP-SAN TLS handshake with the SA token, ECR pull, in-cluster Service DNS + correct callback URL/port, push-back token auth, and terminal-condition polling** — before any sidecar/ConfigMap/IRSA-S3 is in play.
- **Verify:** `kubectl get jobs` shows the run's Job Completed; `GET /api/v1/runs/{id}` shows a terminal status set by the pushed Finish; orchestrator logs show the poll observing `Complete`; a Record-mode create fails fast with no Job.

### R4 — Flip `DEJA_JOB_SHAPE=replay` (config-only sync) **[INFRA]**
Same image; flip the config after O8 (migration parity) and O7 (crypto parity) are closed and I4 (ConfigMap) is synced.
- **Verify:** Argo Synced/Healthy; orchestrator env shows `DEJA_JOB_SHAPE=replay`.

### R5 — First real replay against a known tape **[OPS]**
Trigger a replay (RunSpec with `s3_source`, `correlation_filter`, `candidate_spec=PrebuiltImage`). The full pod comes up: pg/redis ready, runner migrates+seeds+writes the sentinel, router boots past the sentinel and passes the runner's shallow `/health`, kernel drives the recorded requests, run scores and registers artifacts via push-back (backstop does not double-write); native sidecars auto-terminate on runner exit so the Job Completes and results are served from the durable store.
- **Verify:** Job `.status` = Complete; `GET /api/v1/runs/{id}/scorecard` returns the expected divergence class (from stored Result, not local fs); artifacts present; orchestrator logs show no 401/403/backstop double-write; the run survives a deliberate orchestrator pod restart mid-run (durable-state check).

### R6 — Candidate matrix **[OPS]**
Drive the six pushed branches (`deja-pr-patch-{real-change,earlier-fork,dropped-write,response-only,extra-call,transitive-chain}`); confirm each produces its expected verdict. Run two concurrently on one tape to confirm per-Job pg/redis isolation holds and the serial kernel is the only throughput bound.
- **Verify:** six runs each Complete with a scorecard matching that branch's expected divergence; concurrent same-tape Jobs do not collide.

---

## Adversarial findings and how the plan answers them

**Blockers**
- **Push-back URL wrong (name+namespace+port):** hardcoded `replay-orchestrator.replay.svc:8080` is wrong on all three axes (fullname is `sandbox-replay-orchestrator`, ns is `replay-orchestrator-sandbox`, Service port is 80). → **I2** renders `DEJA_ORCHESTRATOR_CALLBACK_URL` from the fullname helper on port 80; **C4** injects it into the Job (never hardcoded).
- **Orchestrator restart orphans in-flight runs (emptyDir + store=None):** → **O9** persistent PG; **I5** `DEJA_DB_URL` + PVC + `strategy: Recreate` + readiness probe; **C6** upsert-on-missing ingest so late events reconstruct the run.
- **Empty scorecard; Job GC destroys output:** Result/Artifact hit the `_ => false` arm and scorecard recomputes from the orchestrator's empty fs. → **C6** persists Result/Artifact to the store and serves `/scorecard` from stored JSON; **C4** sets `ttlSecondsAfterFinished` large so GC never races capture.
- **Blocking ureq POST inline on a tokio worker:** → **C5** `std::thread::spawn` first; all k8s I/O off the async handler thread; regression assertion.

**Majors**
- **PSA rejects orchestrator + Job pods on the bare namespace:** → **O2** read the enforced level; **I3** set `pod-security.kubernetes.io/enforce` via `managedNamespaceMetadata`; **R0** probe-Job admission check.
- **Job created but pods never scheduled (missing/mismatched Job SA):** → **I2** ship+enable `replay-orchestrator-job-role` and make `DEJA_JOB_SERVICE_ACCOUNT` byte-match; **C4** always sets `activeDeadlineSeconds`; **C5** treats "0 active pods + FailedCreate" as launch failure.
- **istio sidecar on the Job pod never exits:** → **C4** stamps `sidecar.istio.io/inject:"false"` on the pod-template labels+annotations; **R0** confirms termination.
- **IRSA minioadmin 403:** → **C4** blank `DEJA_S3_ACCESS_KEY`/`SECRET_KEY` on the Job; **I2** blank them (not omit) on the orchestrator.
- **Single run executes twice (`backoffLimit:1` retry):** → **C4** `backoffLimit: 0` (DECIDED); failure requires an explicit user re-trigger (fresh run_id).
- **Orphaned Jobs, no reaper after orchestrator restart:** → **C5** startup reconciler lists Jobs by `app=deja-replay`, re-attaches polls, settles terminal-or-gone runs (depends on **C6** durable state).
- **`ttlSecondsAfterFinished` races the poll backstop:** → **C4** ttl (86400s) >> poll interval (10s) and >> restart gap; **C5** treats previously-active-now-404 as failure.
- **Push-back lost on rolling updates:** → **I5** `strategy: Recreate` + readiness probe; **C6** durable store so either pod serves push-back.
- **k8s Agent no read timeout → wedge:** → **C3** explicit `timeout_read(15s)` + `timeout_connect(10s)`; timeouts are retryable, not Job failure.
- **SA token expiry → 401 mid-run:** → **C3** re-read the token file per request; token never cached in a struct; test asserts per-call read.
- **Poll conflates transport/auth errors with Job failure:** → **C3/C5** settle failure only from a real `Failed` condition; retry transport/401/5xx with capped backoff; `delete_job` on give-up.
- **Silent empty root-cert store:** → **C3** assert `added >= 1`, fail-fast at construction, surface to `/healthz`.
- **ImagePullBackOff if node role lacks ECR pull (imagePullSecrets not propagated to Job pods):** → **O3** verify node role; **C4** emits `imagePullSecrets` into the Job pod spec if the node role is insufficient (IRSA does not grant pulls).

**Minors (folded, not dropped)**
- **Cross-namespace 403:** **C5** pins the Job namespace to the orchestrator's own; rejects a differing `DEJA_JOB_NAMESPACE`.
- **Squid explicit-proxy misreasoning / regional STS:** **O10** relies on S3/STS/ECR VPC endpoints (not the allowlist) and verifies regional `sts.ap-south-1` via a real IRSA pull; the "allowlist covers S3" reasoning is removed.
- **DockerHub rate-limit bursts:** **O5** mirror pg/redis/candidate to ECR/`public.ecr.aws`, digest-pinned, `IfNotPresent`.
- **istio intercepts the orchestrator's own apiserver calls:** **Open decision D-below** — recommend `inject:"false"` on the orchestrator pod too (or `holdApplicationUntilProxyStarts` + exclude the apiserver ClusterIP).
- **IPv6/dual-stack `KUBERNETES_SERVICE_HOST`:** **C3** brackets IPv6 literals.

---

## Open decisions for the user (with recommendations)

1. **EKS version fallback (O1).** If the cluster is < 1.29, native sidecars are unavailable. *Recommendation:* verify first; if older, fall back to a runner that explicitly signals pg/redis/router shutdown (or a supervisor wrapper) — but this reshapes C4/C5, so do not build the renderer until O1 returns ≥ 29.
2. **Durable state mechanism (O9/C6/I5).** Persistent PG (`DEJA_DB_URL`) vs. RWO PVC alone. *Recommendation:* **PG** — it's the only path that lets `/scorecard` be served for k8s runs from pushed-back Results; PVC alone does not survive the emptyDir/`store=None` scorecard gap.
3. **Orchestrator istio injection.** *Recommendation:* set `sidecar.istio.io/inject:"false"` on the orchestrator pod so its blocking apiserver calls bypass Envoy (avoids first-request-after-deploy connection-refused). The Job pod is already forced off-mesh.
4. **Router config delivery (I4).** Chart-shipped ConfigMap with per-file subPath mounts (recommended) vs. runner copies its bundled files onto the shared emptyDir (needs a small runner addition). *Recommendation:* **ConfigMap subPath** — no extra runner code; 313 KB is well under 1 MiB.
5. **Superposition sufficiency.** File-fallback (`superposition_seed.toml`, `overrides=[]`) cannot represent flags absent from that file. *Recommendation:* proceed with file-fallback; if the target recording depends on an absent flag, stand up a live seeded Superposition sidecar. Unknowable without inspecting the specific tape.
6. **Job timeouts.** Defaults chosen: `activeDeadlineSeconds=7200`, `ttlSecondsAfterFinished=86400`, poll 10s. *Recommendation:* keep unless a known tape's serial-kernel replay approaches 2h, then raise `activeDeadlineSeconds`.
7. **`DEJA_API_SERVICE_TOKEN` provenance.** Hand-created k8s Secret (v1) vs. ESO-from-SecretsManager. *Recommendation:* **hand-created** for v1; ESO is a later hardening.
8. **Record mode in-cluster.** K8sJobExecutor fails-fast on `mode==Record` (no in-pod record driver). *Recommendation:* keep record **compose-only** and document that the in-cluster gateway path is replay-only.
---

# Amendment 1 — thin orchestrator + replay-env package **[DECIDED]**

Supersedes step `C4` (Rust Job renderer). The Job manifest is no longer code.

**Three planes.**

- **Control plane** — the orchestrator. Accepts params (candidate image, S3 source,
  correlation filter), triggers the run, streams lifecycle hooks, renders reports.
  It knows nothing about how the environment is built.
- **Environment plane** — a `replay-env` package per source environment
  (`sbx-mirror` today, `prod-mirror` later): Job template, ConfigMaps,
  ExternalSecrets, ServiceAccount, NetworkPolicy, sidecar image pins. Owned by
  ArgoCD. An env change is an infra PR, never an image rebuild.
- **Data plane** — the Job pod.

**Instantiation [DECIDED]:** the env package ships the Job template in a ConfigMap.
The orchestrator GETs it, applies a *typed patch* — set `image`, set named env vars,
stamp labels — and POSTs the Job. Typed patch, never string templating: parse to
JSON, patch specific paths. `DEJA_REPLAY_ENV` selects the profile. The golden test
from `C4` survives, retargeted at the patcher.

**Namespace [DECIDED]:** a dedicated namespace per env profile (`replay-sbx`), so
NetworkPolicy, PSA labels, RBAC and crypto secrets scope to the replay blast radius.
The orchestrator holds cross-namespace `jobs:create/get/delete`.

**Crypto [DECIDED]:** ExternalSecret pointing at the *same* SecretsManager path the
recording env uses. Not a copy — a copy drifts silently on rotation, and a drifted
key yields a false divergence that reads as a real regression.

## A mirror is a derivation, not a copy

Copying sandbox wholesale imports the proxy, the Kafka recording sink, the live
Superposition endpoint and real connector credentials. Stripping everything breaks
the verdict, because `MASTER_ENC_KEY` and `API_KEYS__HASH_KEY` *must* match the
recording environment. The boot contract's taxonomy is the mapping function:
every key is classified `keep` / `replace` / `forbid`, the classification lives in
the env package, and a lint fails the build when an *unclassified* key appears — so
the next sandbox config knob refuses to be silently inherited.

## "squid not set" is the opposite of safe **[GAP]**

Verified: sandbox's router runs `proxy.enabled: true` with a MITM CA
(`deployment-configs/hyperswitch-app/sandbox-values-dep.yaml:468`), and the custom
recording pod inherits it. Squid is an **opt-in forward proxy** — only clients that
set the proxy env traverse it (`hypersense`, `hs-demo` set `https_proxy_url`).
Dropping the proxy therefore removes the router's only egress chokepoint and leaves
the pod free to reach the internet through NAT.

Replay substitutes only *instrumented* boundaries. An un-instrumented outbound call
executes for real. An egress-open replay pod running a payment tape is one missing
`#[deja::boundary]` away from a live connector charge.

Safety must come from a **default-deny egress NetworkPolicy** (allow DNS, the
orchestrator Service, S3/ECR VPC endpoints; deny all else). Two caveats:
there is no NetworkPolicy anywhere in `hyperswitch-infra` to copy, and on EKS with
the AWS VPC CNI a NetworkPolicy is **silently ignored** unless the network-policy
controller is enabled. An unenforced policy is indistinguishable from a working one.
**Verify enforcement before treating it as a control.**

---

# Amendment 2 — verified replay-correctness blockers

Each independently confirmed by reading the cited files. These invalidate the
*verdict*, not merely the deployment.

### A1 [BLOCKER] The runner image ships a schema 35 migrations behind the candidate
`vendor/hyperswitch @ ff191d7f79` has **496** migrations; `vendor/hyperswitch-deja-clean
@ deja-lean` has **461**, a strict subset (`comm`: 35 only-in-candidate, 0 only-in-clean).
`ops/orchestrator/Dockerfile:50` copies **deja-clean**. The frozen router's diesel models
select columns and match enum variants those 35 migrations add, so queries error, the
replayed request 500s where the recording was 2xx, and the kernel scores a body/status
divergence. **The candidate looks regressed when the harness is at fault.** No schema
fingerprint exists anywhere.
*Fix:* bundle migrations from `vendor/hyperswitch @ ff191d7f79`; assert at boot that the
migrated `__diesel_schema_migrations` head matches the candidate's expected head; fail loud.

### A2 [BLOCKER] The `router-start` sentinel is mandated but written nowhere
Boot contract Decision 5 has the router block on `/deja/work/ready/router-start`.
`grep -rn 'router-start|ready/' crates/` returns nothing, and `HarnessRoot::new`
(`lib.rs:182`) creates no `ready` dir. If the Job implements the contract literally the
router blocks forever. Under the *built* model the router may instead boot as soon as the
lookup file appears (stage 2, `mod.rs:891`) — **before** migrate (stage 3) and **before**
seed (stage 4) — warming in-process caches from an unmigrated, unseeded store.
*Fix:* write the sentinel after stage 4 and have the router wait on it.

### A3 [MAJOR] Runner state root ≠ contract router source root → replay silently becomes live traffic
The runner defaults `HARNESS_STATE_DIR=/workspace/state` (`deja-runner.rs:69`); the contract
points the router at `/deja/work/lookup-tables/${RUN_ID}.jsonl`. If the two disagree, the
router finds no source and every boundary becomes a novel call **executed live** — the
"replay" is live traffic and the verdict is meaningless. Nothing enforces agreement.
*Fix:* derive the router's `REPLAY__SOURCE` from the same `HARNESS_STATE_DIR`; the runner
fails loudly if the rendered lookup path is not under the shared-mount root.

### A4 [MAJOR] Redis isolation reads the ambient thread-local the pg path was fixed to avoid
`connection.rs:27-30` routes pg by `store.get_request_id()` and documents that the ambient
"is bled at checkout when connection acquisition resumes off the request's correlation span."
But `add_prefix` → `deja::replay_key_namespace()` → `current_correlation_id()`
(`fred/commands.rs:120`, `redis_rs/commands.rs:209`) still reads that ambient. Redis ops
running off-span namespace under the wrong correlation or none, while the seeder wrote
`{recorded_corr}:{key}` (`mod.rs:1478`) → seed miss, or an unfenced RMW under the bare key
contaminating across cases.
**Collides with the vendor freeze:** `redis_interface` is vendor code; fixing it means a
vendor change and rebuilding all seven images (`deja-pr` + six patch branches).
*Fix:* thread a request-scoped correlation into the redis seam, mirroring `get_request_id()`.

### A5 [MAJOR] No crypto-epoch parity check
The seeder emits encrypted columns verbatim as bytea hex, decryptable only with the
*recording's* `master_enc_key`. Nothing asserts the recording's key epoch matches the pod's.
A `hash_key` mismatch fails early (auth lookup miss); a `master_enc_key` mismatch decrypts
`merchant_key_store.key` to garbage, so the connector request body differs and we get an
HTTP-boundary divergence that reads as a candidate regression.
*Fix:* fingerprint both keys (keyed digest, never the key) at record time; assert at seed time.

### A6 [MINOR, latent] Unpinned deja coupling between seeder and frozen router
The seeder links deja by path; the frozen router pins git rev `075a614…`. Currently
byte-identical (verified), so not a live bug. If `crates/deja` ever changes `db_schema_for`
or the namespace shape while the router stays frozen, **every** seed lands in a namespace the
router never reads — 100% seed miss with a green-looking pipeline.
*Fix:* build the runner's seeder from the rev the frozen router pins, or self-test at boot.

### Cleared (checked, not a problem)
- Seeder and frozen router agree byte-for-byte on `db_schema_for` / `replay_key_namespace`.
- Frozen `redis_key_prefix = ""`, so the seeder's `{corr}:{key}` matches `add_prefix`.
- Correlation reproduction is sound at this pin: the kernel injects
  `x-request-id = correlation_id` and the router forces `IdReuse::UseIncoming` in replay.
