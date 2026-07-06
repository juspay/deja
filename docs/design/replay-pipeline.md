> Full design produced by the planning workflow (2026-06-12). The adversarial review
> appended at the bottom contains corrections that SUPERSEDE the body where they
> conflict; the reconciled master plan is ../REPLAY_PLATFORM_DESIGN.md.

# Deja Replay Pipeline — Orchestrator, Runner VMs, Build-from-Source

All file references: worktree root `WT = {WT}`, vendor fork `HS = WT/vendor/hyperswitch-deja-clean`.

## 0. Architecture overview

```
                   PRODUCTION / STAGING                          CONTROL PLANE                       RUNNER VM(s)
┌─────────────────────────────────────────────┐   ┌──────────────────────────────────┐   ┌─────────────────────────────────┐
│ Hyperswitch (post-PR, in-tree deja crates)  │   │ deja-orchestrator (orchestrator)│   │ deja-runner agent (new binary)  │
│  ingress: Superposition bool → sample?      │   │  axum + sqlx → POSTGRES          │   │  pull protocol (outbound HTTPS) │
│  macro instr → RecordingHook                │   │  REST /api/v1 + SPA dashboard    │◄──┤  claim → build → replay → score │
│  → AsyncRecordWriter (bounded, backpress.)  │   │  recording catalog (windows)     │   │  Executor: ComposeExecutor      │
│  → HARDENED KafkaRecordSink (SOLE sink)     │   │  job queue (SKIP LOCKED)         │   │  (K8sExecutor later)            │
│         │                                   │   │  audit log (append-only)         │   │  caches: git mirror, cargo reg, │
│         ▼                                   │   │  window materializer             │   │   sccache, target vols, images  │
│  Kafka topic (N partitions, key=corr_id)    │   └───────────┬──────────────────────┘   └───────────┬─────────────────────┘
│         │                                   │               │                                      │
│         ▼                                   │               ▼                                      ▼
│  Vector → S3 raw/ (zstd ndjson, hr-keyed)   │      S3: windows/{id}/ (materialized,        S3: replay-artifacts/runs/{id}/
│         masking hook → S3 masked/           │          manifest = loss detection)              (scorecard, diffs, logs)
└─────────────────────────────────────────────┘
```

Three deployables, one repo, no fork:
- **orchestrator** — `crates/deja-orchestrator` evolved: axum (replacing single-threaded tiny_http, which cannot serve SSE log tails + runner long-polls + the SPA), sqlx/Postgres store filling the intentionally-empty `WT/crates/deja-orchestrator/src/store/mod.rs` slot.
- **runner agent** — `crates/replay-harness-runner` (new), extracted from `lifecycle/mod.rs`. Embedded in-process in local mode (mode A), standalone agent on VMs (mode B). Internally an `Executor` trait; v1 ships `ComposeExecutor` only.
- **shared crates** — kernel, lookup renderer, divergence scorer, deja-record types — already shared; runner and orchestrator both link them.

---

## 1. Postgres schema (DDL)

Replaces filesystem JSON (`{root}/runs/*.json` etc.). Text + CHECK rather than native enums (cheaper migrations). IDs: `run_id` UUIDv7 (time-sortable, replaces `{prefix}-{nanos:x}` from `lib.rs:109-116`); recordings keep human-readable text ids (`win-…`, `rec-…`).

```sql
-- migrations/0001_init.sql

CREATE TABLE runners (
  runner_id         text PRIMARY KEY,                 -- 'rnr-<uuid>' minted at registration
  name              text NOT NULL,
  token_hash        text NOT NULL,                    -- sha256 of bearer token (auth-light, audit-ready)
  labels            jsonb NOT NULL DEFAULT '{}',      -- {"env":"staging","arch":"x86_64"} — claim matching
  capabilities      jsonb NOT NULL DEFAULT '{}',      -- {"max_concurrent_runs":1,"executors":["compose"],
                                                      --  "disk_gb":200,"cpus":16,"toolchains":["1.85.1"]}
  agent_version     text,
  status            text NOT NULL DEFAULT 'offline'
                    CHECK (status IN ('idle','busy','draining','offline')),
  last_heartbeat_at timestamptz,
  registered_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE recordings (
  recording_id     text PRIMARY KEY,                  -- 'win-stg-20260612T10-1h-3f9c' | 'rec-baseline'
  kind             text NOT NULL CHECK (kind IN ('window','session')),
  env              text NOT NULL CHECK (env IN ('local','staging','production')),
  window_start     timestamptz,                       -- kind=window
  window_end       timestamptz,
  recording_run_id text,                              -- kind=session (today's DEJA_RECORDING_RUN_ID)
  s3_bucket        text NOT NULL,
  s3_prefix        text NOT NULL,                     -- materialized events (raw)
  masking_state    text NOT NULL DEFAULT 'not_required'
                   CHECK (masking_state IN ('not_required','pending','masked','failed')),
  masked_s3_prefix text,                              -- the masking hook point (decision 3)
  manifest         jsonb NOT NULL DEFAULT '{}',       -- see §2.4: objects, seq coverage, gaps
  event_count      bigint,
  byte_size        bigint,
  correlation_count integer,
  schema_versions  int[]  NOT NULL DEFAULT '{1}',     -- event_schema_version values observed
  deja_versions    text[] NOT NULL DEFAULT '{}',      -- producer deja crate versions observed
  complete         boolean,                           -- manifest seq-coverage verdict (loss detection of record)
  status           text NOT NULL DEFAULT 'materializing'
                   CHECK (status IN ('materializing','sealed','failed','deleted')),
  created_by       text NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  sealed_at        timestamptz,
  CHECK (kind <> 'window' OR (window_start IS NOT NULL AND window_end IS NOT NULL))
);
CREATE INDEX recordings_browse ON recordings (env, kind, window_start DESC NULLS LAST);

CREATE TABLE replay_runs (
  run_id           uuid PRIMARY KEY,
  mode             text NOT NULL DEFAULT 'replay' CHECK (mode IN ('replay','record')),
  recording_id     text REFERENCES recordings(recording_id),   -- NULL for mode=record until produced
  code_ref         jsonb NOT NULL,        -- {"repo":"https://github.com/juspay/hyperswitch","kind":"pr","value":"1234"}
  resolved_sha     text,                  -- pinned by orchestrator at enqueue (audit)
  params           jsonb NOT NULL DEFAULT '{}',   -- user-supplied (§6)
  config_snapshot  jsonb NOT NULL,        -- frozen effective config (§6) — what 'full audit' replays
  state            text NOT NULL DEFAULT 'queued'
                   CHECK (state IN ('queued','claimed','preparing','building','provisioning',
                                    'rendering_lookup','replaying','scoring',
                                    'completed','failed','canceled','lost')),
  priority         int  NOT NULL DEFAULT 0,
  attempt          int  NOT NULL DEFAULT 1,
  max_attempts     int  NOT NULL DEFAULT 2,
  runner_id        text REFERENCES runners(runner_id),
  lease_expires_at timestamptz,
  candidate_image  text,                  -- 'deja-candidate:abc123def456-fast'
  candidate_image_digest text,
  candidate_deja_version text,            -- from checkout Cargo.lock; drives compat check
  verdict          text CHECK (verdict IN ('pass','fail','inconclusive')),
  scorecard        jsonb,                 -- replay-scorecard/v1 inline (small); raw streams in artifacts
  failure_class    text CHECK (failure_class IN ('infra','build','incompatible','recording',
                                                 'timeout','kernel','lost','canceled')),
  failure_stage    text,
  failure_message  text,
  created_by       text NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  claimed_at       timestamptz,
  started_at       timestamptz,
  finished_at      timestamptz
);
CREATE INDEX replay_runs_queue ON replay_runs (priority DESC, created_at) WHERE state = 'queued';
CREATE INDEX replay_runs_lease ON replay_runs (lease_expires_at)
  WHERE state NOT IN ('queued','completed','failed','canceled');
CREATE INDEX replay_runs_recording ON replay_runs (recording_id, created_at DESC);

CREATE TABLE run_stages (                  -- append-only stage history (fixes "no per-stage timing" gap)
  id            bigserial PRIMARY KEY,
  run_id        uuid NOT NULL REFERENCES replay_runs(run_id),
  attempt       int  NOT NULL,
  stage         text NOT NULL,             -- §4 state names
  status        text NOT NULL CHECK (status IN ('running','ok','failed','skipped')),
  started_at    timestamptz NOT NULL,
  finished_at   timestamptz,
  detail        jsonb NOT NULL DEFAULT '{}',  -- {"cache":"warm","sccache_hit_pct":91,"image_digest":"sha256:…"}
  log_artifact_id bigint                   -- FK to artifacts, set when stage log is sealed to S3
);
CREATE INDEX run_stages_run ON run_stages (run_id, attempt, id);

CREATE TABLE artifacts (
  id          bigserial PRIMARY KEY,
  run_id      uuid REFERENCES replay_runs(run_id),
  recording_id text REFERENCES recordings(recording_id),
  kind        text NOT NULL CHECK (kind IN ('events','manifest','lookup_table','observed',
                                            'http_diffs','divergences','scorecard','graph','log')),
  storage     text NOT NULL CHECK (storage IN ('s3','fs')),   -- fs = local mode
  uri         text NOT NULL,               -- s3://… or absolute path
  bytes       bigint,
  sha256      text,
  created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX artifacts_run ON artifacts (run_id, kind);

CREATE TABLE run_log_chunks (              -- live-tail buffer; sealed logs move to S3, chunks reaped
  run_id  uuid   NOT NULL,
  stage   text   NOT NULL,
  seq     bigint NOT NULL,
  lines   text   NOT NULL,                 -- batched NDJSON-ish raw lines
  ts      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, stage, seq)
);

CREATE TABLE audit_events (                -- append-only; decision 8
  id          bigserial PRIMARY KEY,
  ts          timestamptz NOT NULL DEFAULT now(),
  actor       text NOT NULL,               -- 'user:<email>' (v1: X-Deja-Actor header) | 'runner:<id>' | 'system:sweeper'
  action      text NOT NULL,               -- 'run.create','run.cancel','run.state_change','recording.import',
                                           -- 'runner.register','schedule.create',…
  object_type text NOT NULL,
  object_id   text NOT NULL,
  params      jsonb NOT NULL,              -- FULL request body / transition detail (decision 8)
  request_id  text,
  source_ip   inet
);
CREATE INDEX audit_object ON audit_events (object_type, object_id, ts);
CREATE INDEX audit_actor  ON audit_events (actor, ts);
-- append-only enforcement: app role gets INSERT+SELECT only
REVOKE UPDATE, DELETE, TRUNCATE ON audit_events FROM deja_app;
CREATE RULE audit_no_update AS ON UPDATE TO audit_events DO INSTEAD NOTHING;
CREATE RULE audit_no_delete AS ON DELETE TO audit_events DO INSTEAD NOTHING;

CREATE TABLE schedules (                   -- designed now, executed in v2 (decision 7: no webhooks/schedules in v1)
  schedule_id    uuid PRIMARY KEY,
  name           text NOT NULL UNIQUE,
  enabled        boolean NOT NULL DEFAULT false,
  cron           text NOT NULL,
  window_selector jsonb NOT NULL,          -- {"kind":"latest_sealed","duration":"1h","env":"staging","require_complete":true}
  code_ref       jsonb NOT NULL,           -- {"repo":…,"kind":"branch","value":"main"} resolved at fire time
  params         jsonb NOT NULL DEFAULT '{}',
  created_by     text NOT NULL,
  created_at     timestamptz NOT NULL DEFAULT now(),
  last_run_id    uuid,
  next_fire_at   timestamptz
);
```

Queue mechanics: claim = `UPDATE replay_runs SET state='claimed', runner_id=$1, claimed_at=now(), lease_expires_at=now()+interval '5 minutes' WHERE run_id = (SELECT run_id FROM replay_runs WHERE state='queued' AND <label match> ORDER BY priority DESC, created_at FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING *` — no extra queue infra.

---

## 2. Record sink hardening + S3 window layout (decision 10 obligations)

### 2.1 Kafka becomes the sole sink — hardened semantics
Today `HyperswitchKafkaRecordSink` is fire-and-forget (`deja_record_sink.rs:104-113`: enqueue only, `flush()` = `Ok(())`, delivery reports never awaited, 5s flush only on producer Drop), and `CompositeSink` swallows its errors (`writer.rs:170-179`). As sole sink it is rebuilt:

- **Producer config**: `acks=all`, `enable.idempotence=true` (gives per-partition exactly-once *produce*; downstream is still at-least-once), bounded `queue.buffering.max.messages`/`max.kbytes`, `message.timeout.ms=30000`.
- **Delivery accounting**: a `ProducerContext` delivery callback counts `acked` / `delivery_failed`; the sink tracks `enqueued`. New env `DEJA_SINK_POLICY = fail_open | block`:
  - `fail_open` (production default): on local-queue-full, block up to `DEJA_SINK_BLOCK_MS` (default 2000, propagated as backpressure through `AsyncRecordWriter`'s existing blocking `sync_channel` send), then drop and increment `events_dropped`. Delivery failures increment `events_lost`. **Instrumentation never takes down payments.**
  - `block` (local demo / CI): block indefinitely — the demo wants zero loss and the writer's no-drop backpressure already exists (`writer.rs:306-348`).
- **Real flush**: `flush()` calls `producer.flush(deadline)` and errors if in-flight > 0 — wired into `AsyncRecordWriter`'s existing Flush round-trip and Drop/shutdown path (replacing the Drop-only 5s flush at `kafka.rs:715-725`).
- **Writer semantics change**: today a primary failure permanently disables the writer (`writer.rs:374-377`). With Kafka-as-primary under `fail_open`, transient produce errors must NOT disable — they count as loss; only unrecoverable setup errors (producer creation) disable. `deja_boot.rs` drops the `CompositeSink(JsonlSink)+secondary` composition for a single hardened sink; the `DEJA_SINK=both|kafka` gate collapses to `kafka` (JSONL path deleted). Boot failure policy: if `events.source != kafka` or producer creation fails, recording is OFF (logged loudly) — never aborts router boot, same as today (`deja_boot.rs:61-81`).
- **Envelope (additive, stays schema_version 1)**: add `producer_instance_id` (one per process incarnation, `pi-{host}-{pid}-{boot_ns}`) and a `deja_version` header. In continuous mode `recording_run_id` is set to the producer_instance_id (it must be non-null — `skip_serializing_if` None would break Vector's `key_prefix` template); discrete sessions keep their explicit run id. `global_sequence` is already per-process monotonic-from-0 — that is exactly what makes coverage checkable.

### 2.2 Loss detection of record = window manifests (decision 10b)
Completeness is computed from the data, not trusted from the producer: for each `producer_instance_id` in a window, the materializer records `min_seq`, `max_seq`, `count`, and explicit `gaps[]` (missing global_sequence ranges inside [min,max]). `complete = no gaps across all instances` (edge truncation is handled by correlation-complete materialization, §2.3). Producer-side counters (`events_dropped`, `events_lost`) are surfaced as metrics for alerting, but the manifest is authoritative.

### 2.3 S3 layout: continuous windows + masking hook
```
s3://deja-recordings/
  raw/env={staging|production|local}/dt=YYYY-MM-DD/hour=HH/{producer_instance_id}/{ts}-{uuid}.ndjson.zst
       # Vector lands here: zstd ON (replaces compression:none — 42–420 GB/day raw at 10^7–10^8 ev/day
       # demands it), key templated on event fields env+timestamp+producer_instance_id
  windows/{window_id}/manifest.json
  windows/{window_id}/events.ndjson.zst          # materialized, deduped, replay-ready
  masked/windows/{window_id}/...                 # MASKING HOOK: masker reads windows/, writes masked/
  sessions/{recording_run_id}/...                # local-dev discrete sessions (today's layout, kept)
s3://deja-replay-artifacts/
  runs/{run_id}/{lookup-table.json,observed.jsonl,http-diffs.jsonl,divergences.jsonl,scorecard.json,
                 logs/{stage}.log,graph.jsonl?}
```
**Window materializer** (orchestrator background job; runnable on a runner later): given `[t0,t1)`, reads `raw/` objects covering `[t0, t1+grace)` (grace default 120s — `LazyEventFinalizer` emits `http_incoming` only at response-stream completion, so ingress events for requests near the window edge land late), selects **whole correlations** whose `http_incoming.timestamp_ns ∈ [t0,t1)` plus uncorrelated events in-range, **dedups by (producer_instance_id, global_sequence)** (idempotent producer + at-least-once consumption + Vector retries can duplicate — today nothing dedups, ground truth §7), sorts within each correlation by `(producer_instance_id, global_sequence)` and across correlations by ingress time, writes `events.ndjson.zst` + `manifest.json`, then INSERTs/seals the `recordings` row. This removes both single-partition ordering dependence (production topic gets N partitions keyed by `correlation_id`, preserving the only ordering replay needs — per-correlation; ground truth §8.5) and the no-dedup fragility.

Manifest shape:
```json
{ "manifest_version": 1, "window_id": "win-stg-20260612T10-1h-3f9c",
  "env": "staging", "window": {"start": "...", "end": "...", "grace_s": 120},
  "topic": "hyperswitch-deja-recording-events", "source_objects": [{"key":"raw/…","etag":"…","lines":12345}],
  "coverage": [{"producer_instance_id":"pi-…","min_seq":1042,"max_seq":99871,"count":98830,"gaps":[]}],
  "complete": true, "event_count": 412009, "correlation_count": 18234,
  "schema_versions": [1], "deja_versions": ["0.1.0"], "duplicates_dropped": 17,
  "masking": {"state": "not_required"} }
```

### 2.4 Masking + env policy (decision 3)
`env=staging` windows: `masking_state='not_required'`, replayable from `windows/`. `env=production` windows: created with `masking_state='pending'`; the orchestrator **refuses to enqueue a replay** against them until a masker (separate workstream) writes `masked/windows/{id}/` and flips state to `masked`; replays then read `masked_s3_prefix`. The hook is purely the layout + state machine — masker internals out of scope. (Risk: masking that alters `args` changes `args_hash` lookup keys — flagged in Risks.)

### 2.5 Sampling at ingress (decision 2)
The Superposition bool slots into `RequestIdMiddleware::call` (HS `router_env/src/request_id.rs:839-898`). Since `router_env` cannot depend on `external_services`, the decision is an **injected async sampler** constructed at `router/src/lib.rs:429` where `SuperpositionClient` is in scope (`Arc<dyn Fn(&ServiceRequest) -> BoxFuture<bool>>`); the call site is already inside `Box::pin(async move …)`. Key e.g. `deja_record_request`, `targeting_key = request_id` (deterministic percentage bucketing for free), context = path/method only (pre-auth). Evaluation is in-memory CAC eval (µs), freshness ≤ 15s poll. Error ⇒ false (don't record). `scope_correlation` gains a `sampled` flag; the RecordingHook records only sampled correlations. Uncorrelated/background events: recorded unconditionally in v1 (low volume per ground truth: 0 in the demo workload's correlated flow), revisit if noisy — open question.

### 2.6 Every former-JSONL consumer reads the S3-pulled copy (decision 10c)
Verified consumer inventory: the **lookup renderer** and **kernel** already read `{root}/recordings/{id}/events.jsonl` — the S3-pulled copy (`lifecycle/mod.rs:256-263, 423`); **divergence scoring** reads only derived streams (lookup/observed/http-diffs), never the primary; **deja-tui** and the **visualizer** are the two consumers that today can be pointed at `DEJA_ARTIFACT_DIR/semantic-events.jsonl` — they are repointed at `{root}/recordings/{id}/events.jsonl` (no code change to parsing; same SemanticEvent NDJSON, and `de_u64_opt_lenient` already handles Vector's u64-stringification). `DEJA_ARTIFACT_DIR`'s role shrinks to "scratch dir" (graph output) — the JSONL primary file ceases to exist; demo compose sets `DEJA_SINK=kafka`.

---

## 3. Runner protocol (decision 4)

### 3.1 Pull, not push — justification
Runner agents poll/claim from the orchestrator; the orchestrator never connects to runners.
- **Network/auth fit**: runner VMs need zero inbound ports — only outbound HTTPS to the orchestrator with a bearer token. In an auth-light v1 (decision 8) the smallest attack surface wins; SSO later only touches the orchestrator.
- **Backpressure & scheduling**: runners claim only when they have capacity — load-aware by construction; the orchestrator needs no placement logic or runner address book.
- **Crash semantics unify**: one mechanism (lease expiry) covers runner death, network partition, and agent bugs. Push needs delivery retries + runner-side dedup + health probing.
- **Precedent**: GitHub Actions / GitLab / Buildkite agents are all pull; the k8s executor later is just a controller that claims jobs and creates k8s Jobs — the protocol doesn't change (it is job-document + callbacks over HTTP; nothing in it mentions docker).

### 3.2 Runner-facing API (`/api/runner/v1`, bearer token per runner)
| Endpoint | Purpose |
|---|---|
| `POST /runners/register` | `{name, labels, capabilities, agent_version}` → `{runner_id}` (token pre-provisioned by admin; hash stored) |
| `POST /runners/{id}/heartbeat` | every 15s; body `{active_runs:[{run_id,stage}], disk_free_gb, load}`; **response carries control**: `{cancel:["run_id"], drain:bool, lease_extended_to}` — heartbeat renews all leases held by this runner |
| `POST /jobs/claim` | `{runner_id, capacity, labels}`; long-poll up to 30s; SKIP LOCKED claim; returns the **job document** (below) or 204 |
| `POST /runs/{run_id}/stages` | append stage transitions: `{attempt, stage, status, started_at, finished_at?, detail}` → run_stages + replay_runs.state |
| `POST /runs/{run_id}/logs` | batched: `{stage, seq, lines}` → run_log_chunks (live tail) |
| `POST /runs/{run_id}/artifacts` | register after S3 PUT via presigned URL: `{kind, uri, bytes, sha256}`; small artifacts (scorecard) may be inlined |
| `POST /runs/{run_id}/complete` | `{status: completed|failed, verdict?, scorecard?, failure:{class,stage,message}?}` |

**Job document** (returned by claim):
```json
{ "run_id": "0190a1b2-…", "attempt": 1, "lease_seconds": 300,
  "recording": { "recording_id": "win-stg-…",
    "events_uri": "s3://deja-recordings/windows/win-…/events.ndjson.zst",
    "manifest_uri": "s3://…/manifest.json", "sha256": "…", "schema_versions": [1] },
  "candidate": { "repo": "https://github.com/juspay/hyperswitch", "resolved_sha": "abc123…",
    "features": ["deja","v1"], "build_profile": "fast" },
  "params": { "body_allowlist": [], "correlation_filter": null, "env_overrides": {}, "kernel_timeout_s": 30 },
  "compat": { "supported_event_schema": [1], "supported_lookup_policy": [1] },
  "artifact_upload": { "mode": "presigned",
    "puts": { "scorecard": "https://…", "divergences": "https://…", "observed": "https://…",
              "http_diffs": "https://…", "lookup_table": "https://…", "logs_prefix": "https://…" } } }
```

### 3.3 Lease / heartbeat / requeue
- Lease 5 min, renewed by every heartbeat (15s cadence). Runner marked `offline` after 60s of silence.
- Orchestrator **sweeper** (background task): any non-terminal run with `lease_expires_at < now()` → state `lost`, audit event `run.lost`; if `attempt < max_attempts` and failure class is retryable, re-INSERT semantics: same run_id, `attempt+1`, state `queued` (stage history is per-attempt, preserved).
- Cancellation: `POST /api/v1/runs/{id}/cancel` flips intent; delivered in the next heartbeat response; runner kills the stage subprocess, runs cleanup, posts `complete{status:failed, failure.class:canceled}`.

### 3.4 Artifact destination: S3, registered with the orchestrator
Large artifacts (observed/http-diffs/lookup/logs/events) go **direct to S3 via presigned PUTs** from the job document — the orchestrator never proxies bulk bytes. The **scorecard is dual-homed**: uploaded to S3 *and* inlined in `complete` → stored in `replay_runs.scorecard` jsonb so list/dashboard queries need no S3 round-trip. **Scoring runs on the runner** (it links the same `divergence` crate) — it has the streams locally and this keeps the orchestrator thin; it additionally emits the new `divergences.jsonl` (§5, per-divergence rows with event refs, closing the "scorecard is counters-only" gap). Local mode: `storage='fs'`, URIs are HarnessRoot paths — same `artifacts` rows, no S3.

### 3.5 Concurrency per runner
v1: `max_concurrent_runs = 1` (honest about today's compose flow). But the design removes the singletons so it's a config bump later: compose project name becomes `deja-run-{run_id8}` (today one shared `DEMO_PROJECT` — `lifecycle/mod.rs:65-75`), all containers labeled `io.deja.run_id={run_id}`, and the fixed `REPLAY_HOST_PORT=8090` is replaced by a runner-allocated ephemeral port passed to the kernel (`KERNEL_TARGET_PORT`). Build stages can overlap replays only when cache volumes are per-concurrency-slot — later scope.

### 3.6 Cleanup guarantees + crash recovery
1. **Per-run supervisor**: the agent runs each job in a child process; a parent-side `finally` always executes `docker compose -p deja-run-{id8} down -v --remove-orphans` + per-run workspace removal (checkout dir, rendered compose file), even if the stage runner panics.
2. **Startup orphan sweep**: on agent boot, list compose projects / containers labeled `io.deja.run_id`, reconcile against the local **run journal** (`/var/lib/deja-runner/runs/{run_id}/state.json`, written at claim + each stage); tear down anything not active; report journal-known-but-unfinished runs to the orchestrator as `failed{class:lost, message:"runner restarted"}` (no resume in v1 — requeue handles it).
3. **Disk GC**: persistent caches (git mirror, cargo registry, sccache, target volumes, candidate images) are LRU-pruned against a high-watermark (e.g. keep < 80% disk); disk_free reported in heartbeats; orchestrator stops handing jobs to runners below a floor.
4. **Orchestrator-side guarantee**: lease expiry means no run record ever hangs in a live state if the VM evaporates — the docker-level cleanup then happens at next agent boot (sweep #2) or VM re-image.

---

## 4. Build-from-source stage (decision 5)

Pre-req (part of the deja PR, not the runner): the **vendor `../../../../crates/deja` path-dep layout is local-dev only**. Production candidates build from the real Hyperswitch repo where the PR has landed the integration with **in-tree crates** — copy the 5 runtime crates (`deja`, `deja-context`, `deja-core`, `deja-derive`, `deja-record`) into `hyperswitch/crates/`, switch the 7 declarations to `path = "../deja"`. Rationale: matches hyperswitch's no-git-deps convention, makes `git clone && cargo build --features deja,v1` work verbatim at any ref, and crates.io publication is currently blocked anyway (version-less path deps, empty `repository` metadata, no `[features]` on the facade). crates.io publication is the long-term form (later scope); the byte-identical-instrumentation discipline moves to a CI check that diffs `hyperswitch/crates/deja*` against the deja repo at a pinned version.

**Resolution (orchestrator, at enqueue)**: `code_ref {repo, kind: tag|sha|branch|pr, value}` → immutable sha via `git ls-remote` (PR → `refs/pull/{n}/head`); `resolved_sha` stored on the run and in the audit event. The runner builds exactly that sha — re-running a run is reproducible by construction.

**Runner build pipeline** (stage `building`):
1. **Checkout**: persistent bare mirror per repo (`/var/lib/deja-runner/git/{sha256(repo)}.git`, `git fetch` then `git worktree add` per run). Seconds, not a fresh clone.
2. **Ref sanity**: assert `crates/router/Cargo.toml` declares feature `deja` and `crates/deja/` exists → else fail `incompatible` ("ref predates deja integration"), no retry.
3. **Isolated build**: `docker run` a pinned builder image (`deja-builder:rust-1.85.1` = rust toolchain + apt build deps; honors the ref's `rust-toolchain.toml` via rustup inside) with mounts:
   - checkout (rw), named volume `cargo-registry` (shared, ~2.5G),
   - named volume `sccache` + `RUSTC_WRAPPER=sccache` (local disk backend v1; shared S3 backend later),
   - named volume `target-{repo}-{profile}-{features_hash}` (per repo+profile+feature-set incremental target dir, LRU-capped ~40G).
   Command: `cargo build --release -p router --features deja,v1 --bin router` with profile override per `build_profile` param:
   - `production`: shipped profile (fat LTO, codegen-units=1, strip — the observed ~35min cold shape),
   - `fast` (**default for the PR gate**): `lto="thin", codegen-units=16` via `--config` overrides — semantically identical, link tail minutes not tens of minutes. The scorecard records which profile ran.
   Cache math vs the observed ~35min cold build: warm registry + warm sccache means recompiles are mostly cache hits across refs (the 38-crate workspace barely changes between PRs); the **fat-LTO link tail is the one uncacheable cost**, which is exactly why `fast` is the gate default. Estimates (unverified, flagged in Risks): warm `fast` ≈ 5–10 min, warm `production` ≈ 12–20 min, cold ≈ 35–45 min.
4. **Bake image**: runner-bundled thin Dockerfile (generalizing `WT/demo/Dockerfile.hyperswitch-semantic`): `debian:trixie-slim` + router binary + `workload.sh` + the **checkout's** superposition seed. Labels: `io.deja.sha`, `io.deja.deja_version` (from the checkout's `Cargo.lock`), `io.deja.event_schema_versions`, `io.deja.lookup_policy_versions`, `io.deja.build_profile`. Tag `deja-candidate:{sha12}-{profile}`; digest recorded. **No registry in v1**: build and replay are one job on one runner, the image stays local. Registry push is the k8s-era addition.
5. **Provisioning detail**: the replay stack (pg + migrations + redis + superposition + candidate) comes from a **runner-bundled compose template** rendered per run — *not* the checkout's docker-compose (that file's shape isn't a contract we control). The template references the checkout for the bits that must match the candidate: **migrations** (the candidate sha's `migrations/` via `migration_runner`) and config. This is what makes a v2 candidate with schema changes provision correctly.

**Version-compat check — when must a replay be refused?** Evaluated twice: at enqueue (orchestrator, against recording metadata + a versioned compat matrix in config) and at claim-time on the runner (against the actually-built image labels). Refuse (`failure_class=incompatible`, never retried) when:
1. recording `schema_versions` ⊄ candidate's `supported_event_schema` (candidate can't parse the events);
2. lookup `policy_version` ∉ candidate's `supported_lookup_policy` (keys wouldn't match — `addresses_for`/`KeyStamper` contract);
3. recording's manifest flags producer `deja_versions` *newer* than the candidate's deja with a known-breaking marker in the matrix.
Candidate-newer-than-recording ⇒ allowed with a scorecard warning (backward compat is the deja crates' tested claim). v1 is trivially all-v1/all-v1, but the check is data-driven so the first v2 bump refuses correctly instead of producing garbage divergences.

---

## 5. Run lifecycle, insights, audit

### 5.1 State machine (replay mode)
```
queued → claimed → preparing → building → provisioning → rendering_lookup → replaying → scoring → completed
            │           │          │            │               │               │           │
            └───────────┴──────────┴────────────┴───────────────┴───────────────┴───────────┴─→ failed / canceled
   (lease expiry from any live state) ─→ lost ─→ queued (attempt+1, if retryable & attempts remain)
```
Stage → today's mapping: `preparing` = checkout+compat (new), `building` = source build + image bake (new; today `--build` of the COPY Dockerfile), `provisioning` = compose up pg/redis/superposition/candidate + health (today stages 3–4), `rendering_lookup` = pull window events + `render_lookup_table` (today stages 1–2), `replaying` = redis FLUSHALL + kernel (stage 5), `scoring` = `detect_and_score` + divergences.jsonl + uploads (stage 6). Record-mode (local) runs keep the existing 6 record stages under the same machine. **Verdict ≠ run status**: a replay that completes with `verdict=fail` is `state=completed` — divergence is the product, not an error.

### 5.2 Insights captured (closing the ground-truth gaps)
- `run_stages` rows give per-stage start/end/duration history per attempt (today: only a live label + `stage_updated_ms`, overwritten).
- `replay_runs.created_at/claimed_at/started_at/finished_at` (today: timestamp only encoded in the id).
- Stage `detail` jsonb carries build telemetry: cache warm/cold, sccache hit %, image digest, checkout duration, kernel request count.
- Logs: runner streams per-stage chunks (live tail via `run_log_chunks`); at stage end the runner uploads the sealed log to S3 and registers the artifact; chunks reaped after 14 days; S3 logs under a 90-day lifecycle policy; runs/scorecards/audit retained indefinitely.

### 5.3 Failure taxonomy + retries
`infra` (docker/compose/network/disk — retryable), `timeout` (stage budget exceeded — retryable once), `lost` (lease expiry — retryable), `build` (compile error — NOT retryable, it's the PR's fault and the gate's signal), `incompatible` (version refusal — not retryable), `recording` (window missing/`complete=false` and `params.require_complete` — not retryable), `kernel` (transport errors driving requests — retryable once), `canceled`. Stage budgets inherit today's empirically-derived timeouts (kafka 150s, health 240s, …) plus new ones: checkout 5min, build 60min hard cap, kernel total = `30s × correlations + 10min`.

### 5.4 What "full audit" means end-to-end
Every mutating REST call inserts an `audit_events` row *in the same transaction* as its effect: actor (v1: `X-Deja-Actor` header, honor-system on the internal network — the column and middleware are SSO-shaped so the bolt-on only changes how actor is derived), action, object, **full params jsonb**, ts, request_id, source_ip. Every state transition (runner- or sweeper-driven) is also an audit row with `actor='runner:rnr-…'`/`'system:sweeper'`. The run's `config_snapshot` (§6) freezes everything else. Net: for any scorecard you can answer *who asked for it, with exactly what parameters, against exactly which sha and which window, what every stage did and when, and what the runner uploaded* — without trusting any mutable row, since audit_events is INSERT-only at the DB-grant level.

**Sandbox auth boundary (v1).** Keep the first hosted sandbox behind the
orchestrator, not in the runner: every mutating `/api/v1/*` request passes a
single auth middleware that resolves an actor and rejects anonymous mutations.
Local/dev remains ergonomic by accepting explicit `X-Deja-Actor` without a token
when `DEJA_API_SERVICE_TOKEN` is unset. Hosted sandbox mode sets
`DEJA_API_SERVICE_TOKEN`; then mutating replay API calls must carry both
`Authorization: Bearer <token>` and `X-Deja-Actor` (from SSO or a signed
internal proxy header) before the handler can create runs or write audit rows.
Runners never receive browser credentials; runner protocol endpoints use scoped
`runner:{id}` bearer tokens, can claim/heartbeat/complete only their leased run,
and cannot create new runs. Read-only scorecard/artifact GETs may stay
network-restricted before SSO, but they must still log access once exposed
outside the team network.

---

## 6. Config/params surface (snapshotted for audit)

`POST /runs` params (all optional, defaults applied server-side):
```json
{ "build_profile": "fast | production",                  // default fast
  "body_allowlist": ["$.payment.created_at"],            // → KERNEL_BODY_ALLOWLIST; [] = byte-exact
  "correlation_filter": { "ids": ["…"], "path_prefix": "/payments" },  // replay a subset
  "env_overrides": { "ROUTER__SERVER__REQUEST_BODY_LIMIT": "65536" },  // ALLOWLISTED prefixes only
                                                          // (ROUTER__*, DEJA_QUEUE_* …) — no secret injection
  "deja": { "graph_enabled": false, "lookup_policy_version": 1 },
  "kernel": { "request_timeout_s": 30 },
  "require_complete_recording": true,                     // refuse windows with manifest gaps
  "priority": 0, "max_attempts": 2,
  "workload": { "iterations": 1 } }                       // record mode (local) only
```
At enqueue the orchestrator computes `config_snapshot` = params ⊕ defaults ⊕ `resolved_sha` ⊕ recording manifest sha256 ⊕ compat-matrix version ⊕ orchestrator version, stored immutably on the run and echoed in the `run.create` audit row. The job document the runner receives is derived solely from the snapshot — re-enqueueing the snapshot reproduces the run.

**Hosted orchestrator config (v1 sandbox).** Required deployment config is
explicit and snapshotted where it affects replay behavior:
- `DATABASE_URL` for the orchestrator Postgres store; migrations are owned by
  `deja-orchestrator`.
- `ARTIFACT_STORE=fs|s3`; S3 mode requires bucket/prefix/KMS key/retention and
  presigned PUT/GET TTLs. The runner gets presigned artifact URLs, not broad S3
  credentials.
- `RUNNER_SHARED_SECRET` or equivalent service-token issuer; `PUBLIC_BASE_URL`
  for dashboard links; `ALLOWED_CODE_REPOS` and `ALLOWED_REF_KINDS` for the
  candidate resolver; `COMPAT_MATRIX_PATH` for schema/version refusal rules.
- `ENV_OVERRIDE_ALLOWLIST` and `SECRET_ENV_DENYLIST`; run params may only set
  allowlisted non-secret router/deja knobs, never credentials.
- Runner pool config: heartbeat interval, lease duration, max concurrent runs,
  disk watermarks, cache roots, and compose template version. These values are
  copied into each run's `config_snapshot` when they can change replay output.

**Hosted sandbox deployment seams and ownership.** First hosted sandbox is a
single orchestrator plus one or more runner VMs; it proves the control-plane
interfaces only and deliberately does not introduce production IaC here.
- **Orchestrator runtime env**: set `DATABASE_URL`, `ARTIFACT_STORE=s3`,
  recording/artifact bucket+prefix config, presign TTLs, `PUBLIC_BASE_URL`,
  repo/ref allowlists, compat-matrix path, env-override allow/deny lists, and
  runner lease/heartbeat/cache-watermark defaults before boot; persist the
  effective values that affect replay output in `config_snapshot`.
- **Postgres requirement**: provision a sandbox Postgres reachable only by the
  orchestrator, run the embedded `deja-orchestrator` migrations at deploy time,
  and grant the app role normal table access except append-only audit tables.
- **Artifact storage requirement**: provision sandbox S3-compatible storage with
  separate recording-window and replay-artifact prefixes, encryption, lifecycle
  retention, and least-privilege access. Runners upload artifacts through
  orchestrator-issued presigned URLs; they do not receive broad write access.
- **Runner service token**: an operator pre-provisions a runner identity/token
  and stores only its hash in Postgres. The runner starts with
  `ORCHESTRATOR_URL`, runner identity/labels/capabilities, and the token; the
  token is accepted only on `/api/runner/v1/*` and authorizes claim/heartbeat/
  stage/log/artifact/complete calls for that runner's leased runs.
- **Out-of-tree production ownership**: upstream/vendor PRs carry router code,
  reference config, and local-dev reproductions only. Production Vector
  aggregators, Kafka/topic provisioning, buckets/IAM/KMS/lifecycle, runner VM
  images/provisioning, SSO/proxy wiring, observability, and any Terraform/Helm
  live in the owning production ops workstream/repo outside this vendor tree;
  this document defines the required seams rather than shipping those resources.

---

## 7. REST API v1 (grown on deja-orchestrator) + dashboard data

Versioned `/api/v1`; today's routes kept as aliases for the demo scripts during migration. Server: axum + sqlx; SPA (TypeScript, decision 6) served at `/` from embedded static assets.

**Recording catalog**
- `GET /api/v1/recordings?env=&kind=&from=&to=&status=&complete=&page=&per_page=` → rows + paging.
- `GET /api/v1/recordings/{id}` → full row incl. manifest, coverage, masking_state.
- `POST /api/v1/recordings/windows` `{env, start, end, grace_s?}` → kicks the materializer, returns `{recording_id, status:"materializing"}` (the UI's window picker).
- `POST /api/v1/recordings/import` `{source_path}` → local-mode session registration (today's `POST /recordings`).
- `GET /api/v1/recordings/{id}/events?download=true` → presigned S3 GET (or file stream in local mode).

**Runs (v1 milestone: the manual PR gate)**
- `POST /api/v1/runs` `{recording_id, code_ref:{repo,kind,value}, params}` → resolves sha, compat pre-check, snapshots config, enqueues → `{run_id, resolved_sha, state:"queued"}`; 409 + reason on compat/masking refusal.
- `GET /api/v1/runs?state=&recording_id=&sha=&created_by=&page=` ; `GET /api/v1/runs/{id}` (run + latest-attempt stage summary + verdict + candidate image/digest).
- `GET /api/v1/runs/{id}/stages` (full per-attempt history); `GET /api/v1/runs/{id}/logs?stage=&follow=true` (SSE tail from run_log_chunks, sealed-log redirect after).
- `POST /api/v1/runs/{id}/cancel` ; `POST /api/v1/runs/{id}/retry` (clone snapshot → new run, audit-linked).
- `GET /api/v1/runs/{id}/scorecard` (jsonb; identical shape to replay-scorecard/v1).
- `GET /api/v1/runs/{id}/divergences?kind=&boundary=&correlation_id=&blocking=` → **new persisted rows** from divergences.jsonl: `{kind, blocking, boundary, correlation_id, source_event_global_sequence?, resolved_rank?, json_path?, baseline?, candidate?}` — the web explorer's clickable rows (ports the TUI's `build_diff_rows` join server-side instead of re-deriving in the browser).
- `GET /api/v1/runs/{id}/artifacts` → kinds + presigned GETs; raw `observed` / `http-diffs` / `lookup-table` stay readable for parity with today.
- `GET /api/v1/recordings/{id}/graph?correlation_id=` → graph subtree for the explorer's trace-upward affordance, **always scoped by `recording_run_id`/producer instance** (node_id collides across runs — proven on hs41-latest); 404-with-reason when the recording has no graph artifact (graph capture stays optional/local in v1).

**Replay trigger conveniences (sandbox, not policy).**
- Dashboard "Replay this recording" pre-fills `POST /api/v1/runs` from a selected
  recording/window and a PR URL/branch/SHA, shows the resolved SHA before enqueue,
  and renders a copyable run URL immediately.
- A tiny CLI/curl wrapper is only a client of the same API: `deja run --recording
  <id> --repo <url> --pr <n> --profile fast --wait` should print the run URL and
  exit non-zero only when the completed scorecard verdict fails or the run errors.
- PR-review convenience is link-based in v1: no webhook auto-gates yet. The UI can
  generate a Markdown snippet containing recording id, candidate ref, resolved
  sha, verdict, scorecard URL, and artifact URL; posting it to GitHub is a manual
  or later bot step.
- Retry and "rerun with same snapshot" always clone the immutable
  `config_snapshot`; "edit and rerun" creates a new run with a new audit event.

Current repository implementation (local/manual v1, before M3 build-from-ref):
1. Create or pick a sealed recording from the dashboard recording catalog
   (`/recordings` → `replay →`) or `GET /api/v1/recordings`.
2. Apply the PR/branch diff to a local Hyperswitch checkout and build/copy an
   amd64 router binary, e.g. `/tmp/router-candidate`. The dashboard's New run
   page shows the exact local build command for the checked-in cross-version
   patches.
3. Trigger the orchestrator with the implemented `RunSpec` shape (not the future
   `code_ref` shape):
   ```sh
   curl -fsS -H 'content-type: application/json' \
     -H "X-Deja-Actor: user:<name>" \
     -d '{"mode":"replay","candidate_spec":{"kind":"local_path","binary_or_source":"/tmp/router-candidate"},"recording_id":"<recording_id>","expectation":"pass"}' \
     http://127.0.0.1:8070/api/v1/runs
   ```
   `demo/lib.sh::post_run` is the same curl wrapper used by
   `demo/run-deja-demo.sh`, `demo/run-deja-matrix.sh`, and the parallel drivers.
4. Open `http://127.0.0.1:8070/runs/<run_id>` (or the helper
   `demo/lib.sh::run_url`) and manually paste the run/scorecard/artifact links
   into the PR review.

Follow-up boundary: `CandidateSpec::{RepoPr,RepoBranch,RepoSha}` and the
dashboard PR/branch/SHA form are wire/design seams only until M3 adds the
candidate resolver, source checkout, build-from-ref, resolved-sha snapshot, and
compat pre-check. v1 must not pretend those refs are runnable, and there are no
webhook auto-gates in this PR.

**Runners / audit / schedules**
- `GET /api/v1/runners` (status, heartbeat age, active runs, capabilities); `POST /api/v1/runners/{id}/drain`.
- `GET /api/v1/audit?actor=&action=&object_type=&object_id=&from=&to=&page=`.
- `GET|POST /api/v1/schedules` — CRUD only in v1 (rows exist, executor disabled; decision 7).

Dashboard pages map 1:1 onto the TUI blueprint: Runs list → run detail (stages timeline + live logs) → scorecard (verdict, boundary bars, rank histogram) → divergence explorer (split diff from `/divergences`) → HTTP diff → recording catalog (window picker + completeness badge) → audit.

---

## 8. Migration path: one codebase, two modes (decision pt. 7)

**Mode A (local)** = today's demo UX on the new internals; **Mode B** = orchestrator + runner VMs. Same crates throughout — the fork is avoided by extracting, not rewriting:

- **M0 — store swap**: implement `PgStore` in `store/mod.rs` (sqlx, migrations embedded); orchestrator always uses Postgres — local mode auto-starts/uses the pg container the demo compose stack already runs (no second persistence path to maintain). One-shot importer backfills existing `runs/*.json` / scorecards into Postgres for continuity. Artifact access behind an `ArtifactStore` trait: `FsArtifacts` (HarnessRoot, local) / `S3Artifacts`.
- **M1 — runner extraction**: move `lifecycle/mod.rs` into `crates/replay-harness-runner` with the `Executor` trait (ComposeExecutor = today's code, generalized: per-run project names, rendered compose template, port allocation). Local mode embeds the runner in-process (orchestrator claims its own jobs through the same queue — the protocol is exercised even locally); `deja-runner` binary speaks the pull protocol for mode B. Demo scripts keep working against alias routes.
- **M2 — sink change**: hardened Kafka-only sink in deja-record + deja_boot (JsonlSink removed from the boot composition), demo overlay flips `DEJA_SINK=kafka`, materializer-with-dedup replaces the raw `mc find|sort|cat` concat, deja-tui/visualizer repointed at the pulled copy. The demo now proves the production path exactly (it already proves Kafka→Vector→S3 end-to-end).
- **M3 — build-from-source**: CandidateResolver for `repo_sha|repo_branch|repo_pr` + builder pipeline + compat check; `local_path`/`prebuilt_image` remain the local-dev arms (vendor layout, decision 5). Requires the upstream PR with in-tree deja crates to exist for real-repo refs; until then mode B exercises the flow against the published fork branch.
- **M4 — windows + catalog**: continuous-capture S3 layout (zstd, hour-keyed), window materializer, recordings catalog UI; sessions remain `kind=session` for local dev.
- **M5 — SPA**: dashboard + divergence explorer on the v1 API. **v1 milestone shipped = M0–M5 on staging traffic with one runner VM.**
---

# Adversarial review

BLOCKING:
- SAMPLING vs GAP-BASED COMPLETENESS CONTRADICTION (decision 2 vs decision 10b): global_sequence is assigned at EventBuilder::start, BEFORE any record/skip filtering (crates/deja-record/src/lib.rs:904 'let global_sequence = hook.next_global_sequence()'). The design says 'the RecordingHook records only sampled correlations' (a record-time filter) while simultaneously making 'no gaps in per-producer global_sequence' the loss-detection mechanism of record (manifest complete = no gaps). As written, every unsampled request consumes sequence numbers that never reach the sink, so every sampled production window manifests complete=false by construction, the loss signal is destroyed, and require_complete_recording (default true) refuses all windows. Fix is in-frame but must be specified: the sampling gate must sit BEFORE sequence assignment (skip EventBuilder entirely for unsampled correlations), i.e. sampled events + unconditional uncorrelated events must share a contiguous counter.
- MULTI-PRODUCER WINDOWS BREAK EVERY BARE-global_sequence CONSUMER: per-process counters start at 0 (deja-record/src/lib.rs:451,497; tests at 1659-1661 assert 0,1,2), so any window spanning >1 router pod — or even one pod restart, since each incarnation is a new producer_instance_id — contains colliding global_sequence values. The design fixes this only for dedup (compound key) and the graph endpoint (scoped by producer instance), but: (a) the kernel drives correlations in 'record order' = min(global_sequence) across correlations (deja-kernel/src/main.rs:87-100), which the code comments mark as load-bearing for the shared uncorrelated occurrence bucket and which also matters for live-Redis cross-correlation state — meaningless across instances whose counters all start near 0; (b) LookupEntry.source_event_global_sequence (deja-orchestrator/src/lookup/mod.rs:92), ObservedCall.source_event_global_sequence, the proposed divergences.jsonl rows ('source_event_global_sequence?'), the TUI build_diff_rows join, and the visualizer's obs_by_src join (demo/visualize-replay.py:78-81) all key on the bare u64 and become ambiguous. As designed, windowed replays silently drive in wrong order and produce wrong/ambiguous joins. Needs ingress-time drive ordering plus producer-scoped (or materializer-renumbered) sequences threaded through lookup/observed/divergence artifacts.
- fail_open DOES NOT BOUND PRODUCER IMPACT under sustained broker outage, contradicting its own 'instrumentation never takes down payments' requirement (decision 10a). The block-up-to-DEJA_SINK_BLOCK_MS-then-drop lives in the sink's write_batch, so while rdkafka's local queue stays full the writer thread drains at ~batch_size (256) per 2s ≈ 128 events/s. The upstream AsyncRecordWriter bounded channel (default 8192) then fills, and record() falls back to the UNTIMED blocking send (crates/deja-record/src/writer.rs:320-335) — request threads stall at the writer's degraded drain rate. At the stated production rates (avg ~1.2k events/s, peaks far higher at 10^8/day) this is an effective payment outage within seconds. The design even cites this blocking send as the backpressure path ('propagated as backpressure through AsyncRecordWriter's existing blocking sync_channel send'). The drop decision must also exist non-blockingly at the writer-enqueue layer under fail_open (try_send → count drop), or the sink must enter immediate-drop mode after the first timeout.

CORRECTIONS:
- Sampler injection point is wrong: router/src/lib.rs:429 is inside get_application_builder(request_body_limit, cors, trace_header) (lib.rs:390-403) — no AppState and no SuperpositionClient in scope there. The client lives on AppState (vendor .../router/src/routes/app.rs:145 superposition_service: Arc<SuperpositionClient>); construct the sampler in mk_app (lib.rs:116-130, get_application_builder's only caller, lib.rs:128) and thread it through get_application_builder's signature into RequestIdentifier. Mechanical, but the stated claim 'where SuperpositionClient is in scope' is false.
- deja-tui repointing is NOT zero-code: discovery is filename-bound to 'semantic-events.jsonl' (deja-tui/src/lib.rs:12 SEMANTIC_FILE_NAME; discover_artifacts lib.rs:228-255 and find_semantic_artifact 267-271 only match that exact name, even for explicit file paths). Reading {root}/recordings/{id}/events.jsonl needs a small discovery change. Conversely the visualizer needs NO change — demo/visualize-replay.py:72 already globs harness-state/recordings/*/events.jsonl (the S3-pulled copy), so the design's consumer inventory is off in both directions.
- Missed JSONL consumer: deja-record/src/bin/deja-semantic-metrics.rs reads semantic-events.jsonl (the bench/metrics path the deja_boot comment also cites). Decide its fate when the JSONL sink is deleted.
- Build-time anchor conflicts with in-repo ground truth: docs/DEJA_RECORDING_ARCHITECTURE.md §8.3 records the full 'cargo build -p router --features deja,v1 --release' at ≈11m22s (cargo check 2m19s-2m48s), not 'observed ~35min cold'. The 35min figure may be the loaded demo machine; re-anchor estimates per runner-VM hardware before committing PR-gate latency targets.
- The Hyperswitch repo has no rust-toolchain.toml (vendor/hyperswitch-deja-clean root lacks one; only the deja worktree has it). 'Honors the ref's rust-toolchain.toml via rustup' needs a pinned-default fallback in the builder.
- Local-mode Postgres bootstrap: the overlay deliberately unpublishes pg's host ports (docker-compose.deja.yml 'pg: ports: !override []' to avoid host collisions) and pg comes up per-run via the lifecycle, but the orchestrator needs its store at boot, before any run exists. Local mode must publish pg on a non-default port (re-opening the collision concern the overlay solved) or run/start a dedicated pg at orchestrator startup.
- run_log_chunks PK (run_id, stage, seq) omits attempt — a retried run re-running the same stage collides. Add attempt to the key (run_stages already carries it).
- Runner-side compat check 'at claim-time ... against the actually-built image labels' is self-contradictory: labels exist only after the build stage. It is a post-build/pre-replay check; only the orchestrator's metadata check can run at enqueue/claim.
- Manifest coverage computation is ambiguous between §2.2 and §2.3: gaps must be computed over the RAW scanned event set [t0, t1+grace), not the correlation-filtered materialized output — otherwise sequences belonging to interleaved out-of-window correlations register as false gaps on every window. Also note LazyEventFinalizer assigns sequence+timestamp at start but WRITES at response-body completion (vendor .../router_env/src/request_id.rs:299-305), so a still-open long stream at materialization time yields a false complete=false manifest that the default require_complete gate then refuses — stronger than the stated 'shrinks the driveable set' risk.
- Vector keying detail: SemanticEvent has no env field and timestamp_ns is a plain u64 (≈1.8e18, fits VRL i64 so it stays numeric), so the hour-keyed raw/ layout needs a remap-derived real timestamp plus env enrichment before key_prefix templating — 'key templated on event fields env+timestamp+producer_instance_id' is not directly possible on existing fields. Pin the Vector image (currently timberio/vector:latest-debian) when adopting zstd; zstd on aws_s3 is supported only on recent Vector.
- S3 layout: 'sessions/{recording_run_id}/... (today's layout, kept)' is a rename — today's actual prefix is deja-recordings/recordings/{id}/ (config/vector.deja.yaml key_prefix; pull_recording hardcodes 'local/deja-recordings/recordings/{id}/' at lifecycle/mod.rs:617,646). Keep 'recordings/' or migrate the puller and Vector config together.
- Hardened producer config (acks=all, idempotence, bounded buffering) must be derived for the deja-created producer instance only — deja_boot.rs:71 creates its own KafkaProducer from HS's shared KafkaSettings, so naive settings changes would alter HS analytics-producer semantics; needs deja-specific config derivation (unstated in the design).

NOTES:
Verified true (load-bearing claims): store/mod.rs is the intentionally-empty slot; main.rs is a single-threaded tiny_http loop (cannot serve SSE+long-poll concurrently); HyperswitchKafkaRecordSink is enqueue-only with no-op flush (deja_record_sink.rs:104-113) and only the rdkafka Drop flush at kafka.rs:~715-725; CompositeSink swallows secondary errors (writer.rs:170-179); record() has untimed blocking backpressure (writer.rs:306-348) and disable-on-primary-failure (374-377); deja_boot fail-soft matches deja_boot.rs:60-121; DEJA_SINK both|kafka gate verified; exactly 7 path-dep declarations of deja across HS crates and 5 runtime crates; publication genuinely blocked (version-less path deps in crates/deja/Cargo.toml, repository="" in workspace Cargo.toml, no [features] on the facade); router feature 'deja' exists and demo builds --features deja,v1; release profile is fat-LTO/CGU=1/strip; lifecycle has exactly 6 record + 6 replay stages with kafka 150s/health 240s timeouts, shared DEMO_PROJECT and REPLAY_HOST_PORT=8090 singletons, mc-based no-dedup concat pull; kernel env vars (KERNEL_TARGET_PORT, KERNEL_BODY_ALLOWLIST, etc.) all exist; lookup renderer/kernel already read the S3-pulled events.jsonl; divergence scoring reads only derived streams; scorecards persist at runs/{id}.scorecard.json; SemanticEvent.recording_run_id has skip_serializing_if (None would break Vector's {{ .recording_run_id }} key_prefix); de_u64_opt_lenient exists (syntax_hash only — sufficient, since timestamp_ns fits i64); global_sequence is per-process monotonic-from-0; SuperpositionClient uses LocalResolutionProvider with default 15s polling; RequestIdMiddleware::call is at request_id.rs:839-898 inside Box::pin(async move); superposition_seed.toml exists in the HS fork's config/; HS workspace is 38 crates; scale math checks out against live data (879,553 B / 207 events = 4.25 KB/event → 42-420 GB/day at 10^7-10^8); live recording has 0 uncorrelated events (the architecture doc's older audit figures of ~605+4369 are stale — current data supports the design's claim, though production volume remains the open question the design flags); vendor branch is deja-lean. Decision compliance: all 10 fixed decisions are respected (sessions kept as kind=session, Kafka-only sink with both demo and prod, masking hook as layout+state machine, pull-runner protocol with compose executor, in-tree-crates build-from-source with vendor as local-only, SPA on deja-orchestrator, manual gate only with schedules executor disabled, X-Deja-Actor + INSERT-only audit, Postgres store, JSONL consumers inventoried). The three blocking issues are spec-level defects with in-frame fixes (sampling gate placement, producer-scoped sequence semantics + kernel ordering for windowed recordings, fail_open drop placement) — none invalidates the architecture, but the design must be amended before implementation, as each would otherwise fail silently in exactly the production scenarios v1 targets (sampled staging windows, multi-pod/restart windows, broker outages). Ground-truth section refs in the design (§7, §8.5) do not match docs/DEJA_RECORDING_ARCHITECTURE.md numbering; the substance they cite (no dedup today, per-correlation partition ordering) is verified. The 'node_id collides across runs / hs41-latest' claim is structurally true (per-process u64 counters) though the hs41 evidence lives only in archived preload-era docs.