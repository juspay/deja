# Replay Pod Boot Contract

**Status:** design contract for the k8s replay Job template.  
**Scope:** frozen candidate router image (`juspay/hyperswitch:deja-pr` at `ff191d7f79`) plus runtime config, mounted files, sidecars, and the outer-repo runner.  
**Non-goal:** vendor code changes, Deja library repin, Jenkins rebuild, settings overlay implementation, seed-backed Superposition implementation.

## DECISIONS FOR YOU

1. **Use typed Deja settings, not legacy `DEJA_*`, for the frozen router.**
   - **DECIDED:** The replay Job template must set `ROUTER__DEJA__MODE=replay`, `ROUTER__DEJA__RUN_ID`, `ROUTER__DEJA__REPLAY__SOURCE`, and `ROUTER__DEJA__REPLAY__OBSERVED_SINK`.
   - **Reason:** frozen router boot calls `deja_boot::install(&conf.deja)` after `Settings::with_config_path` + `validate`; it does not use a runtime-only `DEJA_MODE/DEJA_LOOKUP_TABLE` contract for primary installation.
   - **Evidence:** `vendor/hyperswitch/crates/router/src/bin/router.rs:15-30`; `vendor/hyperswitch/crates/router/src/deja_boot.rs:225-281`; compose replay env at `demo/overlays/hyperswitch/docker-compose.deja.yml:128-136`.

2. **The router pod must mount and explicitly use a frozen-sha config file.**
   - **DECIDED:** The router container command must be equivalent to:
     ```text
     /local/bin/router -f /local/config/docker_compose.toml
     ```
     and `/local/config/docker_compose.toml` must come from a ConfigMap built from the frozen sha's config plus runtime overrides.
   - **Reason:** `Settings::with_config_path` loads a config file, then overlays `ROUTER__...` env. The Job template author must not rely on unknown image-baked config.
   - **Evidence:** config-file CLI at `settings.rs:69-75`; config load/env overlay at `settings.rs:1323-1370`; compose uses `command: ["-f", "/local/config/docker_compose.toml"]` at `docker-compose.deja.yml:125-127`.
   - **GAP:** Confirm the Jenkins image's binary path. The local demo image uses `/local/bin/router`; if the frozen image uses a different entrypoint, keep the image's binary path and still pass `-f /local/config/docker_compose.toml`.

3. **Do not omit Superposition for the frozen image. Use file fallback or a live sidecar.**
   - **DECIDED:** Preferred runtime-only contract: provide syntactically valid Superposition settings and mount `/local/config/superposition_seed.toml` as `superposition.backup_file_path`; use a fast-failing endpoint (`http://127.0.0.1:1`) or a real Superposition sidecar.
   - **Reason:** `Settings::validate()` validates Superposition unconditionally, and `AppState` unconditionally calls `get_superposition_client(...).await.expect(...)`. Frozen `SuperpositionClient::new` builds HTTP primary plus optional file fallback, then calls `provider.init(...)`. Provider source confirms boot-time fallback: `LocalResolutionProvider::init` calls `primary.fetch_config(None)` and on any `Err` uses `fallback.fetch_config(None)`; it errors only when both primary and fallback fail.
   - **Evidence:** `settings.rs:1490-1493`; `routes/app.rs:530-536`; `external_services/src/superposition.rs:475-530`; `superposition_provider-0.114.0/src/local_provider.rs:64-94`.
   - **Runtime workaround:** mount the fallback file and set absolute `ROUTER__SUPERPOSITION__BACKUP_FILE_PATH=/local/config/superposition_seed.toml`. This overrides the TOML's relative `./config/superposition_seed.toml`, so fallback resolution no longer depends on `working_dir`. Do not describe this as Superposition absence; it is Superposition init via file fallback.
   - **GAP requiring vendor change if not using fallback:** true seed-backed Superposition replay and typed config seed substitution are not in the frozen image.

4. **Migrations belong to the runner image, not the frozen router image.**
   - **DECIDED:** The outer-repo runner image bundles `migrations/`, `diesel.toml`, and a known diesel CLI for `ff191d7f79`; runner runs migrations against the pg sidecar before router start is released.
   - **Reason:** compose relies on a migration runner service with source mounted at `/app` and a helper script that runs `just migrate` or `diesel migration run --migration-dir /app/migrations --config-file /app/diesel.toml`. The frozen Jenkins router image should not be assumed to carry source migrations or diesel CLI.
   - **Evidence:** stock compose migration runner uses `/app`, `DATABASE_URL`, and `just migrate` (`vendor/hyperswitch/docker-compose.yml:59-78`); routers depend on migration completion (`vendor/hyperswitch/docker-compose.yml:133-141`); frozen Dockerfile copies the router binary and config file but no `migrations/` (`vendor/hyperswitch/Dockerfile:43-84`); overlay migration runner command/mounts at `docker-compose.deja.yml:34-48`; helper script behavior at `demo/migration-runner.sh:61-69`.

5. **Router startup must be gated by the runner.**
   - **DECIDED:** Use a shared `emptyDir` sentinel. Router command waits for `/deja/work/ready/router-start` before execing the router; runner writes the sentinel after pg/redis readiness, migrations, lookup render, and seed materialization.
   - **Reason:** containers in one pod start concurrently. Kubernetes `depends_on` does not exist for ordinary containers; compose's `depends_on` sequencing must be translated explicitly.
   - **Evidence:** compose replay waits on pg, redis, migration_runner, and superposition-init before router (`docker-compose.deja.yml:143-151`); lifecycle waits for replay router before kernel (`lifecycle/mod.rs:643-683`).

---

## Taxonomy

| Class | Meaning | Pod value rule |
|---|---|---|
| `local-infra` | plumbing for the replay pod sidecars/files | point to local pod paths/localhost sidecars |
| `crypto/auth` | secrets/key material whose derived values affect auth, decrypt, encrypted DB values, or connector auth | must equal recording env or recover the same key material |
| `logic-config` | config that changes branches, validation, routing, side-effect count, or side-effect args | must equal recording env or be represented by an equivalent mounted seed/config file |
| `diagnostic` | logging/identity/telemetry only | local value is safe unless used in lookup identity |
| `unsupported-gap` | desirable contract not present in frozen image | document workaround or block |

---

## Router container command and volumes

### Command

**DECIDED:** use a runtime command wrapper so the router does not boot before the runner prepares the pod state:

```sh
/bin/sh -ec '
  until [ -f /deja/work/ready/router-start ]; do sleep 1; done
  exec /local/bin/router -f /local/config/docker_compose.toml
'
```

If the Jenkins image does not have `/local/bin/router`, replace only the binary path with the image's real router entrypoint. Keep `-f /local/config/docker_compose.toml`.

### Volumes

| Volume | Type | Mounted in | Path | Contents |
|---|---|---|---|---|
| `deja-work` | `emptyDir` | runner, router | `/deja/work` | lookup table, observed sink, http diffs, scorecard/call ledger, seed cert, readiness sentinels |
| `router-config` | ConfigMap | router | `/local/config` | frozen `docker_compose.toml`, `superposition_seed.toml`, optional required-fields config |
| `pg-data` | `emptyDir` | pg | postgres data dir | disposable replay DB |
| `redis-data` | `emptyDir` or memory | redis | redis data dir | disposable replay Redis |
| `runner-cache` | `emptyDir` | runner | `/deja/cache` | S3 pull/compact intermediate files |

`DEJA_GRAPH_DIR` is **not** a required volume contract for this frozen tree. The frozen router installs graph capture through typed Deja settings/stream behavior; the old compose `DEJA_GRAPH_DIR` entry is treated as stale unless a code reader proves live usage.

---

## Router replay env contract

The router starts from `/local/config/docker_compose.toml`; env vars below are deltas or values that must be supplied explicitly for the pod.

### Required Deja replay env

| Env | Class | Required value | Evidence/status |
|---|---|---|---|
| `ROUTER__DEJA__MODE` | local-infra | `replay` | **BUILT.** Typed Deja install dispatches on `settings.mode` (`deja_boot.rs:276-281`). |
| `ROUTER__DEJA__RUN_ID` | diagnostic/local-infra | orchestrator run id | **BUILT.** compose sets it at `docker-compose.deja.yml:135`; install report carries effective run id (`deja_boot.rs:257-263`). |
| `ROUTER__DEJA__REPLAY__SOURCE` | local-infra | `/deja/work/lookup-tables/${RUN_ID}.jsonl` | **BUILT.** replay requires source or lookup dir (`deja_boot.rs:198-226`); compose source at `docker-compose.deja.yml:132`. |
| `ROUTER__DEJA__REPLAY__OBSERVED_SINK` | local-infra | `/deja/work/observed/${RUN_ID}.jsonl` | **BUILT.** file observed sink created when set (`deja_boot.rs:228-244`); compose sink at `docker-compose.deja.yml:133`. |
| `DEJA_MODE`, `DEJA_LOOKUP_TABLE`, `DEJA_OBSERVED_SINK`, `DEJA_RUN_ID` | unsupported-gap | do not rely on these | **DECIDED.** Legacy runtime-only names are not the primary frozen router install path. Set typed `ROUTER__...` names. |

### Required local sidecar env/config

| Setting/env | Class | Pod value rule | Notes |
|---|---|---|---|
| `master_database.host` / `ROUTER__MASTER_DATABASE__HOST` | local-infra | `127.0.0.1` or pg service hostname in same pod | baseline config has host `pg` (`docker_compose.toml:71-78`); pod should use localhost sidecar. |
| `master_database.port` / `ROUTER__MASTER_DATABASE__PORT` | local-infra | `5432` | pg sidecar. |
| `master_database.username/password/dbname` | local-infra + crypto wrapper caveat | local replay DB credentials | DB auth is local. Do not confuse DB password with `secrets.master_enc_key`. |
| `replica_database.*` | local-infra | same as master DB unless OLAP disabled by build/config | baseline includes replica DB (`docker_compose.toml:86-92`); validation is cfg-gated by `olap`. |
| `redis.host` / `ROUTER__REDIS__HOST` | local-infra | `127.0.0.1` or redis pod DNS | baseline uses `redis-standalone` (`docker_compose.toml:130-145`). |
| `redis.port` / `ROUTER__REDIS__PORT` | local-infra | `6379` | Redis sidecar. |
| `redis.cluster_enabled` / `ROUTER__REDIS__CLUSTER_ENABLED` | local-infra | `false` | One Redis sidecar. |
| `events.source` / `ROUTER__EVENTS__SOURCE` | local-infra | `logs` for replay | Replay does not need Kafka event sink; baseline is logs (`docker_compose.toml:1257-1259`). |
| `RUST_MIN_STACK` | local-infra | `16777216` | compose sets this for router (`docker-compose.deja.yml:137`). |

### Crypto/auth parity env/config

These values must equal the recording environment **or** the pod must be seeded so the same derived key material is recovered.

| Setting/env | Class | Pod value rule | Failure if wrong |
|---|---|---|---|
| `api_keys.hash_key` / `ROUTER__API_KEYS__HASH_KEY` | crypto/auth | equal recording env | Ingress auth computes `PlaintextApiKey::keyed_hash(hash_key)` then DB lookup by hash; wrong key causes auth lookup miss before replayed flow. Baseline key at `docker_compose.toml:165-166`. |
| `secrets.master_enc_key` / `ROUTER__SECRETS__MASTER_ENC_KEY` | crypto/auth | equal recording env unless merchant key rows are rewrapped for a local key | Used as wrapper/master encryption material; wrong value can prevent recovering `MerchantKeyStore.key` and can break AES decrypt/encrypt parity. Baseline key at `docker_compose.toml:94-98`. |
| `key_manager.url` / `ROUTER__KEY_MANAGER__URL` | crypto/auth/local-infra hybrid | point to a KeyManager that can return/transfer the same merchant keys, or configure legacy/local path consistently | baseline has URL and `use_legacy_key_store_decryption=false` (`docker_compose.toml:151-153`). If the replay path calls KeyManager, it must be reachable and seeded. |
| `STRIPE_API_KEY` and connector auth secrets | crypto/auth | equal recording env for any connector path that constructs auth headers/body before a captured boundary | The compose record path forwards `STRIPE_API_KEY` (`docker-compose.deja.yml:103-104`). Replay should supply the same if connector request construction is exercised. |
| Apple/Google/Paze decrypt keys and webhook/HMAC secrets | crypto/auth | equal recording env if the replayed route decrypts/verifies/signs with them | Settings validates some decrypt-key configs (`settings.rs:1456-1466`). Different keys can fail deterministic crypto before side effects. |

### Logic-config parity env/config

These values must equal the recording environment or be represented by an equivalent mounted config/seed file. Frozen image does not support settings overlays; parity is supplied by ConfigMap/env.

| Setting/env | Class | Pod value rule | Failure if wrong |
|---|---|---|---|
| `lock_settings.redis_lock_expiry_seconds` | logic-config | equal recording env | Changes Redis lock TTL args; baseline 180 at `docker_compose.toml:1195-1197`. |
| `lock_settings.delay_between_retries_in_milliseconds` | logic-config | equal recording env | Changes retry timing/count behavior. |
| `webhooks.redis_lock_expiry_seconds` and `webhooks.outgoing_enabled` | logic-config | equal recording env | Changes webhook locking/emission branches (`docker_compose.toml:1199-1201`). |
| `connector_request_reference_id_config.*` | logic-config | equal recording env | Changes connector request/reference IDs; env parser has list key support at `settings.rs:1351-1359`. |
| `bank_config`, `required_fields`, connector filter/routing configs | logic-config | equal recording env | Can change validation, routing, available payment methods, and side-effect shape. `required_fields` is derived from `bank_config` after parse (`settings.rs:1373-1376`). |
| Superposition resolved flags | logic-config | mount fallback seed equivalent to recording env | Frozen image reads Superposition through provider cache/file fallback; no tape substitution. |
| `locker.mock_locker`, `locker.locker_enabled`, `locker.host` | logic-config/local-infra | match recording mode; if real locker is used, provide reachable equivalent | baseline mock locker true with empty host (`docker_compose.toml:120-124`). |
| connector base URLs | logic-config/local-infra | equal recording env unless connector HTTP boundary identity is proven independent | Different URLs can change request shape or external call identity. |

### Superposition env/config

**BUILT:** Frozen code supports a file fallback configured by `backup_file_path`.

Required pod contract for no-live-service mode:

| Setting/env | Class | Pod value |
|---|---|---|
| `ROUTER__SUPERPOSITION__ENDPOINT` | local-infra shape | valid URL; recommended `http://127.0.0.1:1` for fast failure if no sidecar |
| `ROUTER__SUPERPOSITION__TOKEN` | local-infra shape | non-empty placeholder or recording-compatible token if using live sidecar |
| `ROUTER__SUPERPOSITION__ORG_ID` | local-infra shape | non-empty placeholder or real org if sidecar |
| `ROUTER__SUPERPOSITION__WORKSPACE_ID` | local-infra shape | non-empty placeholder or real workspace if sidecar |
| `ROUTER__SUPERPOSITION__REQUEST_TIMEOUT` | local-infra | small value, e.g. `1`, to bound HTTP-primary fallback delay |
| `ROUTER__SUPERPOSITION__BACKUP_FILE_PATH` | logic-config | `/local/config/superposition_seed.toml`; absolute path intentionally overrides the TOML's relative `./config/superposition_seed.toml` |

**Evidence:** validation requires endpoint/token/org/workspace (`external_services/src/superposition/types.rs:148-180`, from prior verified read); app state panics on init failure (`routes/app.rs:530-536`); constructor builds HTTP primary and optional file fallback and calls provider init (`external_services/src/superposition.rs:475-530`); provider implementation performs boot-time fallback on primary error (`superposition_provider-0.114.0/src/local_provider.rs:64-94`).

**GAP:** If the fallback file cannot represent all flags needed by the recording, a live Superposition sidecar seeded to the recording's config is required. Seed-backed Superposition replay would require vendor code changes and is outside this frozen-image contract.

---

## Runner container contract

The runner is the only image built by the outer repo. It owns orchestration inside the pod.

### Inputs

| Env | Meaning |
|---|---|
| `RUN_ID` | orchestrator replay run id |
| `RECORDING_ID` | recording id |
| `S3_RECORDING_URI` or equivalent RunSpec source | recording source to pull |
| `CORRELATION_FILTER` | optional subset filter |
| `ORCHESTRATOR_CALLBACK_URL` | progress API endpoint |
| `ORCHESTRATOR_SERVICE_TOKEN` | bearer token for progress callback |
| `ROUTER_URL` | `http://127.0.0.1:8080` inside pod |
| `KERNEL_RECORDING_PATH` | path after S3 pull/compact, e.g. `/deja/work/recording/events.jsonl` |
| `KERNEL_TARGET_HOST` | `127.0.0.1` |
| `KERNEL_TARGET_PORT` | `8080` |
| `KERNEL_HTTP_DIFF_SINK` | `/deja/work/http-diffs/${RUN_ID}.jsonl` |

Kernel env evidence in local lifecycle: `run_kernel` sets `KERNEL_RECORDING_PATH`, `KERNEL_TARGET_HOST=127.0.0.1`, `KERNEL_TARGET_PORT`, and `KERNEL_HTTP_DIFF_SINK` (`lifecycle/mod.rs:2440-2444`, from repo anchor).

### Sequence

1. Pull recording from S3 into `/deja/work/recording/events.jsonl`.
2. Apply correlation filter if requested.
3. Compact/render lookup table to `/deja/work/lookup-tables/${RUN_ID}.jsonl`.
4. Wait for pg and redis sidecars.
5. Run migrations against pg.
6. Seed pg/redis preconditions required by the selected correlations.
7. Write `/deja/work/ready/router-start`.
8. Wait for router health at `http://127.0.0.1:8080/health`.
9. Run kernel against router.
10. Score divergence and write artifacts under `/deja/work`.
11. POST progress/results to orchestrator API.

---

## Migration contract

### BUILT in local compose

- Stock compose defines `migration_runner` as `debian:trixie-slim`, sets `DATABASE_URL=postgresql://db_user:db_pass@pg:5432/hyperswitch_db`, works in `/app`, bind-mounts the source tree, installs diesel CLI, and runs `just migrate` (`vendor/hyperswitch/docker-compose.yml:59-78`).
- Router services depend on pg/redis health and `migration_runner` completion before boot (`vendor/hyperswitch/docker-compose.yml:133-141`).
- Overlay replaces migration runner command with `bash /migration-runner.sh` and mounts the helper plus host-built diesel CLI (`docker-compose.deja.yml:44-48`).
- Helper prefers mounted diesel, falls back to GitHub release install, then runs `just migrate` or direct `diesel migration run --database-url "$DATABASE_URL" --migration-dir /app/migrations --config-file /app/diesel.toml` (`demo/migration-runner.sh:61-69`).

### DECIDED for k8s Job pod

The runner image must bundle:

```text
migrations/                  # from vendor/hyperswitch at ff191d7f79
diesel.toml                  # same sha
/usr/local/bin/diesel         # diesel_cli with postgres support
optional just                 # only if using just migrate
```

Runner executes direct diesel migration, not the router image:

```sh
diesel migration run \
  --database-url "postgres://db_user:db_pass@127.0.0.1:5432/hyperswitch_db" \
  --migration-dir /runner/vendor/hyperswitch/migrations \
  --config-file /runner/vendor/hyperswitch/diesel.toml
```

### GAP / risk

- Do not assume the frozen Jenkins router image contains source migrations.
- Do not download diesel from GitHub at pod runtime; local helper exists because that path is flaky (`demo/migration-runner.sh:4-18`).
- If the candidate image schema differs from bundled migrations, replay is invalid. Since vendor is frozen at `ff191d7f79`, runner migration bundle must be from exactly that sha.

### Runner image contents

**DECIDED:** bundle the v1 migration path for the frozen `deja-pr`/v1 image:

```text
vendor/hyperswitch/migrations/
vendor/hyperswitch/diesel.toml
diesel_cli 2.3.5 with postgres feature
```

**GAP:** upstream `vendor/hyperswitch/docker/migration-runner.Dockerfile` is not sufficient as-is for this contract: it copies only `migrations/` + `diesel.toml`, installs diesel from GitHub `latest`, and does not model the local `just migrate`/v2-compatible migration paths. Use a runner image built by the outer repo for the pinned sha.

---

## Readiness and probes

### Sidecars

| Container | Readiness |
|---|---|
| pg | `pg_isready` on localhost:5432 |
| redis | `redis-cli ping` on localhost:6379 |
| router | `/health` after runner writes start sentinel and router starts |
| runner | no service readiness; Job progress posted to orchestrator |

### Router boot ordering

Compose uses `depends_on` for pg, redis, migration_runner, and superposition-init before replay router (`docker-compose.deja.yml:143-151`). The k8s equivalent is the sentinel gate, because ordinary pod containers do not provide compose-style ordering.

### Health endpoint

Router v1 mounts shallow `/health`, which returns `200` with `\"health is good\"` (`routes/health.rs:16-21`), and deep `/health/ready`, which runs through `api::server_wrap` and checks DB/Redis/Locker plus feature-gated external checks such as Analytics, gRPC, Decision Engine, Opensearch, and unified connector service (`routes/health.rs:23-185`). Local lifecycle waits for router health before kernel (`lifecycle/mod.rs:643-646`). The compose overlay disables container healthcheck for replay to avoid health probes creating Deja noise (`docker-compose.deja.yml:152-162`).

**DECIDED:** runner should poll shallow `/health` for startup/liveness. Avoid `/health/ready` as a repeating kube readiness probe: it executes DB/Redis/Locker plus feature-gated external checks, can fail in feature builds whose optional services are intentionally absent, and can contaminate replay `observed` calls. Use `/health/ready` only as a one-shot diagnostic after migrations/seed/router startup if a deep dependency check is wanted.

---

## Frozen-image gaps and runtime workarounds

| Would require vendor change | Runtime workaround in this contract |
|---|---|
| Settings logic overlay replay | Supply equal logic config via ConfigMap/env. |
| Seed-backed Superposition replay | Use Superposition file fallback or a seeded live Superposition sidecar. |
| Legacy `DEJA_*` replay install | Use typed `ROUTER__DEJA__...` settings. |
| Graph directory env (`DEJA_GRAPH_DIR`) as required output | Do not require it; use observed stream artifacts. |
| Router waits for migrations internally | Wrap container command with sentinel wait. |
| Router image contains migrations/diesel | Bundle migrations/diesel in runner image. |

---

## Minimal pod file layout

```text
/deja/work/
  recording/events.jsonl
  lookup-tables/<run_id>.jsonl
  observed/<run_id>.jsonl
  http-diffs/<run_id>.jsonl
  scorecards/<run_id>.json
  call-ledgers/<run_id>.jsonl
  seed-certificates/<run_id>.json
  ready/router-start

/local/config/
  docker_compose.toml
  superposition_seed.toml
  payment_required_fields_v2.toml  # exact filename used by frozen Settings defaults/path logic
```

---

## Final contract summary

**BUILT in frozen router:** typed `ROUTER__DEJA__...` replay install, file lookup source, file observed sink, Settings env overlay, Superposition HTTP-primary/file-fallback initialization.

**DECIDED for k8s:** runner prepares files/migrations/seeds, router waits for sentinel, router uses mounted frozen config with env deltas, Superposition uses mounted fallback file or live seeded sidecar, pg/redis are local sidecars.

**GAP:** no vendor-supported config seed overlay, no seed-backed Superposition, no reliance on legacy `DEJA_*`, and no guarantee the frozen router image carries migrations.
