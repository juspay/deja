> Full design produced by the planning workflow (2026-06-12). The adversarial review
> appended at the bottom contains corrections that SUPERSEDE the body where they
> conflict; the reconciled master plan is ../REPLAY_PLATFORM_DESIGN.md.

# Deja Packaging + Integration Design

How deja ships so production candidates build from the real Hyperswitch repo, and what the upstream PR contains for production recording.

All paths: `WT` = the deja worktree root, `HS` = the Hyperswitch fork (today nested at `WT/vendor/hyperswitch-deja-clean`, branch `deja-integration` = the 2-commit publishable patch on tag `2026.04.21.0`; `deja-lean` = `deja-integration` + local-only material, never published).

---

## 1. Dependency form for the deja crates in the real Hyperswitch repo

### Evaluation

The runtime closure HS needs is exactly 5 crates (verified): `deja` (facade, 745 LoC), `deja-context` (247), `deja-core` (2,216), `deja-derive` (proc-macro, 1,381), `deja-record` (6,041) — **~10.6k lines total**. The integration patch today is 48 files, +2,745/−220.

| Form | Reproducible candidate builds | PR reviewability | Source-of-truth | HS policy fit | Friction now |
|---|---|---|---|---|---|
| (a) In-tree copy (`hyperswitch/crates/deja*`) | Perfect — deja pinned by the HS commit itself | Bad: PR balloons from +2.7k to ~+13k lines; maintainers must review/own 10.6k lines of runtime they didn't write | Two copies; the demo's "instrumentation byte-identical" guard becomes a cross-repo sync discipline | Matches monorepo convention | Low |
| (b) Git dep, rev/tag-pinned, on the public deja repo | Exact: rev pin in Cargo.toml + HS commits Cargo.lock (verified: the integration diff already touches `Cargo.lock`) | PR stays 48 files / +2.7k | Single source (deja repo) | **Verified precedent**: upstream HS already ships rev-pinned git deps — `unified-connector-service-client` (juspay/connector-service @ rev 71fcc81f), `rust-i18n` fork @ rev, `rusty-money` @ tag, in `crates/{router,external_services,hyperswitch_interfaces,currency_conversion}/Cargo.toml` | Requires the public deja repo to exist (export script ready) |
| (c) crates.io | Exact via version pin | Same small PR | Single source | Cleanest for a long-lived upstream merge | Highest: publish 5 crates in dep order; **currently blocked** — version-less path deps (`WT/crates/deja/Cargo.toml:11-14`), `repository = ""` (`WT/Cargo.toml:20`); plus a publish round-trip per iteration during active development |

### Decision: phased — **(b) git dep pinned to a deja release tag for the PR and v1 runner; (c) crates.io as the explicit upstream-merge endgame**, with the manifest blockers fixed now so the switch is mechanical.

Concretely, the 7 HS crate declarations change from the local-dev escape hatch

```toml
# today (only valid in the nested vendor layout)
deja = { path = "../../../../crates/deja", optional = true, default-features = false }
```

to

```toml
deja = { git = "https://github.com/<org>/deja", tag = "v0.2.0", optional = true, default-features = false }
```

in: `common_utils`, `hyperswitch_domain_models`, `router_env`, `diesel_models`, `redis_interface`, `external_services`, `router`. The feature fan-out (`router/Cargo.toml:40` → 6 sub-crate `deja` features) is already repo-relative and carries over unchanged. The `deja` feature stays **out of `default`/`common_default`** (verified) — zero cost, zero new deps for anyone not opting in; this is the key upstream-acceptability property.

**MSRV**: deja workspace `rust-version = 1.85` == HS `rust-version = 1.85.0`. Contract: deja never raises MSRV above HS's; CI enforces (§5).

**Manifest fixes landing in the deja repo now (prereq for both b and c):**
1. Every inter-crate dep gains a version: `deja-core = { path = "../deja-core", version = "0.2.0" }` (path+version is the standard publishable form; path wins locally, version is what consumers resolve).
2. `WT/Cargo.toml` `[workspace.package] repository` filled with the public URL (already item 3 of `export-public.sh`'s post-export checklist).
3. The `deja` facade gains an explicit (initially empty-default) `[features]` section so consumers' `default-features = false` is meaningful rather than a no-op latch.
4. Version bumped to 0.2.0 and a `vX.Y.Z` tag discipline: every HS-pinned ref is a tag, never a branch.

**Local dev after the switch (replaces the nested-vendor path dep):** a side-by-side checkout (`~/src/deja`, `~/src/hyperswitch`) plus a **non-committed** `[patch]` in the HS checkout's `.cargo/config.toml` (stable since Cargo 1.56):

```toml
[patch."https://github.com/<org>/deja"]
deja         = { path = "../deja/crates/deja" }
deja-context = { path = "../deja/crates/deja-context" }
deja-core    = { path = "../deja/crates/deja-core" }
deja-derive  = { path = "../deja/crates/deja-derive" }
deja-record  = { path = "../deja/crates/deja-record" }
```

A helper `deja dev-link` script in the deja repo writes this file. The nested `vendor/` layout may persist as a personal convenience but stops being load-bearing: nothing in the published HS manifests references `../../../../` anymore, so `git clone hyperswitch && git checkout <any sha> && cargo build --features deja,v1` works verbatim on a clean machine — exactly what the runner needs (decision 5).

**crates.io endgame trigger:** when upstream maintainers signal merge intent. Publish order: deja-context → deja-core → deja-derive → deja-record → deja; flip the 7 git deps to `version = "x.y"`; nothing else changes (the lockfile pin discipline is identical).

---

## 2. The production recording PR contents

Recommended structure: one PR, three reviewable commits (extending the existing two-commit shape of `deja-integration`):

- **Commit 1 — deps, features, boot**: the 7 Cargo.toml git-dep declarations, feature fan-out, `deja_boot.rs`, `services/kafka/deja_record_sink.rs`. No behavior change unless `--features deja`.
- **Commit 2 — boundary instrumentation**: the existing macro instrumentation across db/redis/crypto/id/time seams (the bulk of the 48-file diff), all `#[cfg(feature = "deja")]`-gated.
- **Commit 3 — production recording**: sampling hook, hardened Kafka-only sink, envelope v2 with code-ref stamping, reference configs (below).

### 2.1 Superposition-driven per-request sampling at ingress

**Where:** `HS/crates/router_env/src/request_id.rs`, inside `RequestIdMiddleware::call` (verified: the deja branch is the `if semantic_boundary::is_active()` test at request_id.rs:872, already inside `Box::pin(async move { ... })`, so an async lookup fits with no signature change). The decision must run **before** `capture_incoming_request` so unsampled requests skip body buffering entirely.

**Dependency direction** (router_env cannot import `external_services::SuperpositionClient` — verified inversion): define the trait in router_env, implement it in router, inject at construction.

```rust
// router_env (new, feature = "deja")
pub trait RecordSampler: Send + Sync {
    /// Decide whether this request should be recorded. Must never error
    /// outward; implementations map all failures to `false`.
    fn should_record<'a>(&'a self, meta: &'a RecordSampleMeta<'a>)
        -> BoxFuture<'a, bool>;
}
pub struct RecordSampleMeta<'a> {
    pub method: &'a str,
    pub path: &'a str,
    pub request_id: &'a str,   // targeting key
}
```

`RequestIdentifier::new(...)` gains `.with_record_sampler(Arc<dyn RecordSampler>)`. The router wires it at the existing middleware construction site (`HS/crates/router/src/lib.rs:429-445`, where conf/state are in scope, and which already hosts the `DEJA_MODE=replay → IdReuse::UseIncoming` branch).

**The lookup** (router-side impl): `SuperpositionClient::get_config_value::<bool>(DEJA_RECORD_REQUEST_KEY, context, targeting_key)` — the same in-memory CAC eval every other key uses (RwLock cache + 15s background poll; microseconds, no network on the hot path; verified in `superposition_provider-0.102.0/src/local_provider.rs`).

- **Config key:** `deja_record_request` (bool, **default `false`** in the seed/default-configs), added to `consts.rs` alongside `should_call_gsm` etc. and to `config/superposition_seed.toml` defaults.
- **Context dimensions:** ingress is pre-auth, so merchant/profile dimensions are unavailable (verified). v1 context = `{api_path_group, http_method, deployment_env}`. The sampling *strategy* (percentages, path allowlists, ramps) lives entirely in Superposition and evolves there — deja only reads the bool (user decision 2).
- **Targeting key:** the request_id — Superposition's variant bucketing gives deterministic percentage sampling for free.
- **Failure semantics:** any error (empty cache at boot, eval error, missing key) → `false` = don't record; the request is **never** blocked or failed by sampling. Recording is fail-silent w.r.t. traffic. A counter (`deja.sampler_errors`) tracks lookup failures.
- **Break-glass env override:** `DEJA_SAMPLING = superposition | all | off` (default `superposition` when a sampler is injected, `all` when none — which is what the local demo uses, preserving today's behavior with zero Superposition dependency in local dev).
- **Freshness:** bounded by the 15s poll — acceptable; kill switch = flip the key to false, fully off within one poll interval.

**Plumbing the decision to every downstream boundary:** extend `deja-context::ContextSnapshot` from `{correlation_id}` to `{correlation_id, recording: bool}` and add `scope_correlation_with(id, recording, fut)` (exported via `deja::__private`, next to the existing `scope_correlation` re-export). The middleware:

- mode=record, sampled → today's path: `capture_incoming_request` + `scope_correlation_with(request_id, true, …)`.
- mode=record, NOT sampled → skip capture entirely; `scope_correlation_with(request_id, false, …)` so downstream macro hooks short-circuit on one thread-local read.
- mode=replay → unchanged.

`record_boundary_*` in deja-record early-returns when a correlation scope exists with `recording=false`, **before** `EventBuilder::start` — i.e. before a `global_sequence` is allocated. This is load-bearing: unsampled requests leave **no sequence gaps**, so per-producer sequence contiguity remains a pure loss signal for the manifests (§2.3). Uncorrelated/background events (no correlation scope) keep recording — they're low-volume and already `uncorrelated_events_tolerated` at scoring.

### 2.2 Hardened Kafka sink (sole sink — JSONL retired)

Per decision 10, `CompositeSink` + `JsonlSink` leave the record path. `deja_boot.rs` composes `RecordingHook::with_sink(Box::new(HyperswitchKafkaRecordSink))` directly; `DEJA_SINK` collapses to `kafka` (values `jsonl`/`both` removed); `DEJA_ARTIFACT_DIR` is no longer consulted in record mode. The `AsyncRecordWriter` (bounded queue, dedicated thread) is retained as the app-side buffer.

The sink itself is rebuilt from fire-and-forget to tracked delivery:

1. **Delivery tracking:** replace the bare `BaseRecord` enqueue on HS's shared `ThreadedProducer<DefaultProducerContext>` with a **dedicated rdkafka producer owned by the sink**, using a counting `ProducerContext` whose delivery callback decrements an in-flight gauge and bumps `delivered` / `delivery_failed` counters. Producer config: `acks=all`, `enable.idempotence=true` (broker-side dedup of producer retries — removes the main at-least-once duplication source), `message.timeout.ms=60000`, bounded `queue.buffering.max.messages`/`max.kbytes`.
2. **Backpressure + explicit policy:** new env `DEJA_RECORD_DELIVERY = fail_open | block` (production default `fail_open`; the local demo sets `block` to preserve no-drop semantics).
   - `fail_open`: when the producer queue is full or the broker is down, drop the event, increment `dropped_queue_full`, throttled warn. Application latency is never coupled to Kafka health.
   - `block`: the writer thread blocks on enqueue (the existing `sync_channel` backpressure then propagates to request threads via `record()`'s blocking fallback).
3. **Real flush:** `RecordSink::flush()` becomes `producer.flush(timeout)` + wait for in-flight==0 (replacing today's no-op at deja_record_sink.rs:111-113). Writer shutdown and `Drop` honor `DEJA_SHUTDOWN_FLUSH_MS` (default 10_000), replacing the silent 5s `Drop` flush.
4. **No permanent self-disable on transient errors:** today any primary sink error sets `active=false` forever (writer.rs:374-377) — correct for a broken file, wrong for a flapping broker. The sink classifies errors: retryable (queue full, transport) → policy applies, keep going; fatal (auth, unknown topic, config) → disable + `deja.sink_fatal` metric + loud log.
5. **Loss accounting:** counters exported through HS's metrics infra (`deja.events_enqueued`, `delivered`, `dropped_queue_full`, `delivery_failed`, `inactive_records`, `backpressure_blocks`, `sampler_errors`). The **system of record for loss is sequence coverage in the window manifests** (§2.3): a gap in a producer's `global_sequence` range ⇒ events were created but never landed. Metrics are the alarm; manifests are the audit.

**JSONL-consumer migration** (decision 10c — verified consumer list): all four downstream consumers — the lookup renderer (`lookup/mod.rs:24-116`), the kernel (`KERNEL_RECORDING_PATH`), divergence scoring, and deja-tui — already read `{root}/recordings/{id}/events.jsonl`, which is the **S3-pulled copy** in the harness flow. The only true primary-JSONL consumers are demo-script/doc references to `DEJA_ARTIFACT_DIR/semantic-events.jsonl`; those references are deleted and deja-tui's local docs point at the pulled file. Consequence: the Vector u64-stringification shape becomes the canonical on-disk shape everywhere; the lenient deserializer (lib.rs:186-216) already handles it, and the golden tests in §5 pin that shape.

### 2.3 Envelope v2: code-version tagging + the production S3 layout

**Envelope** (`deja_record_sink.rs`), `schema_version: 2`, additive fields:

```json
{
  "schema_version": 2,
  "artifact_type": "deja_artifact_record",
  "recording_run_id": "router-{hostname}-{boot_ns}",
  "correlation_id": "…|null",
  "code_ref":     "<git sha of the router build>",
  "deja_version": "0.2.0",
  "service": "router",
  "env": "staging",
  "event": { …full SemanticEvent… }
}
```

- `code_ref` sourced from router_env's existing vergen build metadata (verified: `router_env/build.rs` runs vergen with `git`/`cargo`/`rustc` features), with `DEJA_CODE_REF` env override for images built outside a git checkout.
- `deja_version` = `env!("CARGO_PKG_VERSION")` of the linked deja crate.
- `recording_run_id` in production = **per-process instance id** (`{service}-{pod|hostname}-{boot_ns}`, env-overridable as today). This keeps `global_sequence` monotonic and gapless per producer — the manifest coverage unit. Partition key and headers unchanged from v1.

**Production Vector lands the FULL envelope** (a deliberate change from the demo's unwrap remap): the per-line `code_ref`/`deja_version`/`env` is exactly what the manifest builder needs, at ~120 B/event (~3% of the 4.2 KB average). The orchestrator's ingest step unwraps. The demo's `vector.deja.yaml` is updated to the same no-unwrap shape so there is **one** pipeline contract (the demo continues to prove the full path, decision 10).

**S3 layout (bucket `deja-recordings`), with the masking hook point:**

```
raw/env={env}/service={service}/dt=YYYY-MM-DD/hh=HH/run={recording_run_id}/{ts}-{uuid}.ndjson
curated/…same key shape…            ← what replay reads; staging: identity copy
manifests/env={env}/service={service}/dt=YYYY-MM-DD/hh=HH/manifest.json
```

- **Windows** are hour-aligned `[start,end)` prefixes; a replayable unit = a selected set of windows (+ overlap margin, below) per decision 1. Vector sink: `key_prefix` templated on `{{ env }}/{{ service }}` + strftime date/hour + `{{ recording_run_id }}`; `filename_append_uuid: true`; `compression: none` in v1 (zstd is later-scope; the manifest carries a `compression` field so the switch is non-breaking); batch sizing per environment.
- **Masking hook:** replay **only ever reads `curated/`**. A masking job (per-window, manifest-driven) transforms `raw/` → `curated/`; until the masking workstream lands it is an identity copy in staging, and **production recording stays off** (decision 3). The manifest records `masking: {status: none|pending|masked, policy_version}`; the orchestrator refuses to schedule replays from windows whose masking status doesn't satisfy the environment's policy.

**Window manifests = the loss-detection mechanism of record** (decision 10b). Built by the orchestrator's window-indexer after an hour closes (listing objects under the prefix, streaming envelope lines):

```json
{
  "manifest_version": 1,
  "window": {"start": "...Z", "end": "...Z"}, "env": "staging", "service": "router",
  "producers": [{
      "recording_run_id": "...", "min_seq": 0, "max_seq": 18231,
      "event_count": 18230, "gaps": [[412,415]],
      "code_refs": ["<sha>"], "deja_versions": ["0.2.0"],
      "event_schema_versions": [1], "callsite_identity_versions": [1]
  }],
  "objects": [{"key": "...", "events": 2000, "bytes": 8388608}],
  "completeness": {"complete": false, "gap_total": 4, "cross_window_continuity": "ok"},
  "masking": {"status": "none", "policy_version": null},
  "compression": "none"
}
```

Because sampling never allocates sequences (§2.1), gaps are unambiguously loss. The indexer also checks **cross-window continuity** per producer (this window's `min_seq` vs the previous window's `max_seq`+1) so inter-window loss is caught, and writes a recording-catalog row to Postgres (decision 9).

**Ingest normalization (orchestrator-side, replaces `mc find | sort | mc cat`):** pulling a window set for replay does: fetch objects → unwrap envelopes → **dedup by `(recording_run_id, global_sequence)`** → **sort by `(recording_run_id, global_sequence)`** → write `events.jsonl` + an ingest report (dupes dropped, gaps, line counts). This converts three emergent properties of the demo into engineered guarantees: ordering no longer depends on the implicit single-partition topic + single Vector consumer; at-least-once redelivery duplicates are removed; and LazyEventFinalizer's late `http_incoming` lines are normalized into sequence order. Within a correlation, `global_sequence` order = call-start order, which is exactly what the renderer's `KeyStamper` occurrence stamping requires; the kernel's `request_sequence` sort is unaffected. **Window-edge correlations:** the puller fetches the selected windows plus a configurable margin (default ±1 window) and filters to correlations whose `http_incoming` timestamp falls inside the selection, so a request whose tail events landed in the next hour's objects replays completely.

**Topic + IAM provisioning** (IaC in the deja repo's `ops/`, §4):
- Topic `deja.recording.{env}` — explicitly provisioned (no auto-create): **12 partitions** at staging scale, key = `correlation_id` as today (all events of one request → one partition, in produce order, which ingest-sort then makes irrelevant anyway); retention 24–48h (Vector drains continuously); `min.insync.replicas=2` to back `acks=all`.
- IAM/ACLs: router → produce-only on the topic; Vector → consume topic + `s3:PutObject` on `raw/*` only; masking job → read `raw/*`, write `curated/*`; orchestrator/runners → read `curated/*` + `manifests/*`, write `manifests/*`; nobody else writes `raw/`. Lifecycle: `raw/` expires 30d, `curated/` 90d (staging defaults).
- The HS PR itself carries only what must live in-repo: config stanzas (`events.kafka.deja_recording_topic` already exists; new `deja.sampling` knobs in `config/deployments/env_specific.toml`), the `superposition_seed.toml` default for `deja_record_request=false`, and the updated local-dev `docker-compose.deja.yml` + `config/vector.deja.yaml`. Production Vector aggregator config and Terraform/Helm stay out of the HS PR (§4 ownership).

---

## 3. Version compatibility contract

Four versioned surfaces, and where each is stamped:

| Surface | Owner | Current | Stamped in |
|---|---|---|---|
| `event_schema_version` (SemanticEvent, u16) | deja-record | 1 | every event (already); aggregated per producer in manifests |
| Envelope `schema_version` (u32) | HS sink (deja_record_sink.rs) | 1 → **2** | every envelope; manifests; consumed by ingest only |
| `lookup_policy_version` (LookupTable) | deja-record (`addresses_for`/`KeyStamper`) | 1 | LookupTable header; run row |
| `callsite_identity.version` (u16) | deja-derive/deja-record (rank-3 syntax-hash algorithm etc.) | 1 | every event's callsite_identity; manifests |

Plus build identities: `deja_version` + `code_ref` in every envelope; candidate images carry OCI labels `org.deja.version`, `org.deja.code_ref`, `org.deja.event_schemas`, `org.deja.lookup_policies` (stamped by the runner at assembly).

**The compat table** is data, not code: `compat.toml` in the deja repo, shipped with the orchestrator, mapping each deja release to its supported sets:

```toml
[deja."0.2"]
event_schema   = [1]
lookup_policy  = [1]
callsite_identity = [1]
envelope       = [1, 2]
```

**Orchestrator gate, enforced before scheduling any replay (and recorded in the run row + audit log):**

1. Resolve the candidate ref → read its `Cargo.lock` → extract the pinned deja version `V_cand` (this is why the rev/tag pin in §1 matters: the lockfile makes `V_cand` knowable *before* any 35-minute build starts).
2. Read the selected windows' manifests → `S_rec` = union of event_schema versions, callsite-identity versions, recorder deja versions; masking status.
3. Checks, all against `compat.toml`:
   - the orchestrator's own deja-record (renderer + kernel are built from it) supports every version in `S_rec` — else **refuse** ("upgrade orchestrator");
   - `lookup_policy`: pick `P` = max policy supported by BOTH the orchestrator and `V_cand`; none in common → **refuse**;
   - `callsite_identity`: the recording's identity version must be in `V_cand`'s supported set, else rank-3 (syntax-hash) matching silently degrades — refuse, or schedule with a recorded "rank-3 unavailable" warning if the operator overrides (audited);
   - masking status satisfies the environment policy (§2.3).
4. The renderer writes `policy_version = P` into the LookupTable. **Defense in depth on the candidate:** `LocalFileLookupSource`/`LookupTableHook` gains a boot-time assertion that `table.policy_version` is in its supported set — a mismatched candidate fails the run loudly at boot instead of producing a wall of NovelCall/Omitted noise.
5. Everything resolved (versions, code_refs, manifest ids, verdict) is persisted on the run row — this is also the audit-readiness story for the replay gate (decision 8).

Rule-of-thumb encoded in the table: additive event fields don't bump `event_schema_version` (serde ignores unknowns; the lenient syntax_hash deserializer stays); semantic changes to existing fields do. Envelope versions only ever concern ingest. Lookup-policy and callsite-identity versions bump whenever key derivation or hashing changes, because they couple the **orchestrator's renderer binary** to the **candidate's in-image deja** (verified coupling: `deja-orchestrator` and the candidate share `addresses_for`/`KeyStamper` from deja-record by construction).

---

## 4. Repo topology end-state

**Two repos. The orchestrator and dashboard live in the deja repo** — they share deja-record types (SemanticEvent, LookupTable, the compat table) whose skew across repos would be constant friction, and the compat table must update atomically with schema changes.

```
github.com/<org>/deja            (public; created by scripts/export-public.sh — fresh single-commit history)
├── crates/
│   ├── deja, deja-context, deja-core, deja-derive, deja-record   # runtime closure (what HS pins)
│   ├── deja-tui                                                  # local explorer (stays, decision 6)
│   ├── deja-orchestrator      # orchestrator: REST + Postgres + lifecycle + dashboard serving
│   ├── deja-kernel
│   └── deja-runner             # NEW: runner agent (claim/build/replay protocol client)
├── web/                        # TypeScript SPA (served by deja-orchestrator)
├── compat.toml                 # the version compat table (§3)
├── ops/                        # production deployment, owned here NOT in the HS PR:
│   ├── vector/                 #   aggregator config (envelope-landing pipeline, §2.3)
│   ├── terraform/              #   topic provisioning, bucket+lifecycle, IAM
│   ├── runner/                 #   runner VM image build (Packer/Dockerfile) + provisioning
│   └── k8s/                    #   later-scope executor manifests
├── demo/                       # local dev drivers (compose flow), unchanged role
├── ci/hs-ref.lock              # pinned HS ref for the integration CI job (§5)
└── scripts/export-public.sh    # already exists; remains the publish path for the dev clone

github.com/<org>/hyperswitch     (fork of juspay/hyperswitch)
└── branch deja-integration → the upstream PR (3 commits, §2)
    owns: in-router code, feature wiring, deja_boot, sink, sampling hook,
          reference config stanzas, superposition seed entries,
          LOCAL-DEV docker-compose.deja.yml + config/vector.deja.yaml
```

**Ownership rule:** anything that must compile into or boot with the router, plus local-dev reproductions, lives in the HS tree (and goes upstream). Anything operating *around* the router — production Vector aggregator, topic/bucket/IAM IaC, runner image, k8s manifests, the orchestrator/dashboard — lives in `deja/ops/` and never burdens the upstream PR. The export script's existing guidance holds: publish `deja-integration` only, never `deja-lean`; rotate the historical Stripe test key before the first export.

**Runner VM image contents** (generalizing the docker-compose flow, decision 4):
- rustup with 1.85.1 preinstalled; honors a ref's own `rust-toolchain.toml` if present (HS has none today; `rust-version = 1.85.0` governs).
- git + a maintained **bare mirror of hyperswitch.git** (clones in seconds instead of minutes); `gh` for PR-ref resolution.
- docker + compose (replay stack execution) — image *assembly* stays the seconds-long thin pattern (debian-trixie-slim + COPY of the cargo-built binary + workload assets), generalizing `demo/Dockerfile.hyperswitch-semantic`; no in-Dockerfile compile in v1.
- `aws`/`mc` CLI for S3 pulls; the `deja-runner` agent binary.
- Persistent volumes: shared cargo registry (~2.5 GB), per-(HS-major × deja-version) target-dir caches (5–8 GB each — verified 4.6 GB mid-build; cross-ref sharing within a cache key is safe, across deja versions is not because the feature graph changes), git mirror.
- Build procedure per `repo_sha`: mirror-fetch → worktree checkout at sha → (compat pre-check already passed, §3) → `cargo build --release -p router --features deja,v1 --bin router` (the ref's own profile: fat LTO + CGU=1 ⇒ budget ~35 min cold) → assemble + label image → push to the internal registry → report image digest + timings to the orchestrator. `CandidateSpec` finally gets its resolver: `prebuilt_image` pass-through; `repo_sha` direct; `repo_branch`/`repo_pr` resolved to a sha first (recorded in the run row — branches are never the build input).
- **Protocol shape** (detail belongs to the orchestrator session; the constraint here): pull-based — runners register, long-poll/claim jobs (`build_candidate`, `replay_window`) over the orchestrator REST, heartbeat, post stage events, reference artifacts by S3 key + registry digest. Pull-based claiming is what makes a k8s Job executor a drop-in later: same agent, same protocol, pod-shaped.

---

## 5. CI for the deja repo (post-vendor-era integration proof)

`WT/.github/workflows/ci.yml` today: fmt + clippy + workspace tests only. It grows to:

1. **`workspace`** (every push/PR — existing job unchanged): fmt, clippy `-D warnings`, tests on the pinned 1.85.1 toolchain.
2. **`msrv`** (PR): `cargo check --workspace` on exactly `1.85.0` — deja must never outrun HS's `rust-version` (§1 contract).
3. **`schema-guard`** (PR): golden-file tests pinning the contracts other systems depend on: envelope v2 serialization (including the Vector-stringified-u64 variant as a golden input — the canonical on-disk shape per §2.2), SemanticEvent round-trip per supported `event_schema_version`, manifest JSON schema, and a consistency test that every version referenced in `compat.toml` exists in code (and vice versa: bumping `lookup_policy`/`callsite_identity` constants without a compat.toml entry fails CI).
4. **`integration-check`** (PR — the headline job): proves *this deja commit compiles inside the real Hyperswitch tree* without any nested vendor dir:
   - `ci/hs-ref.lock` pins `{repo_url, ref_sha}` of the HS fork's `deja-integration` branch (post-upstream-merge: an upstream sha).
   - Steps: checkout deja → shallow-clone HS at the pinned sha → write `.cargo/config.toml` `[patch."https://github.com/<org>/deja"]` redirecting all 5 crates to the CI checkout (the exact mechanism local dev uses, §1 — CI thereby also proves the dev-link flow) → `cargo check -p router --features deja,v1` (+ `cargo test -p router --features deja,v1 --lib` for the deja-gated unit tests if time allows).
   - `cargo check` skips LTO/codegen, so this is the HS workspace front-end only: ~15–20 min cold, ~3–5 min warm with `Swatinem/rust-cache` keyed on the HS `Cargo.lock` hash (check-only target dirs fit GH's cache budget; if eviction bites, this job moves to a self-hosted runner with a persistent volume — same machine profile as the runner VM).
   - Bumping `ci/hs-ref.lock` is an ordinary reviewed PR; CI on that PR proves the new HS ref against current deja.
5. **`candidate-build` (nightly, scheduled)**: exercises the *actual runner path* end-to-end so the 35-minute reality stays honest: run the `deja-runner` build procedure against the pinned ref (full `--release` build, thin-image assembly, OCI labels), then boot-smoke the image with `DEJA_MODE=record` + `DEJA_RECORD_DELIVERY=fail_open` and **no broker** — asserting the §2.2 fail-open contract: router serves traffic, loss counters tick, process never blocks. Runs on a self-hosted/large runner.
6. **`export-check`** (PR): the export script's secret-grep preflight (`sk_(test|live)_…`/`whsec_…` over tracked files) runs as a standalone gate so a leaked token fails CI long before anyone runs `export-public.sh`.

The HS fork branch additionally carries a minimal workflow (fork-only, dropped from the upstream PR): `cargo check -p router --features deja,v1 --locked` — proving the *committed* git-pin resolves clean with no patch, i.e. exactly what a runner does.
---

# Adversarial review

BLOCKING:
- Sampling-flag propagation breaks for spawned-task events. The design plumbs the per-request decision ONLY via scope_correlation_with on the middleware future, and claims downstream hooks short-circuit on a thread-local read. But the HS patch's own correlation propagation for tokio::spawn'ed work is the tracing layer (deja-record/src/correlation_layer.rs, installed at vendor/hyperswitch-deja-clean/crates/router_env/src/logger/setup.rs deja_correlation_layer), which re-enters context via enter_correlation_id -> ContextSnapshot::new(correlation_id) — it cannot carry the new recording:bool. As designed, spawned-task boundary events of a request fall back to a flag-less scope: default=record means unsampled requests still record every spawned-task event (defeating the sampling cost reduction at 10^7-10^8/day and filling windows with partial correlations), default=skip means SAMPLED requests lose their spawned-task events (incomplete recordings -> replay divergence noise). Fix is clear but must be designed: a correlation_id -> decision registry written at ingress and consulted by the layer (deja-context already has a string-keyed TASK_CONTEXTS map to model this on), or extend SpanCorrelation to carry the flag resolved from the root span.
- The 'manifests are the loss-detection mechanism of record' claim (decision 10b) has two structural blind spots the design does not close in v1: (a) TAIL loss — events after a producer's last delivered sequence (crash, failed shutdown flush, fail_open drops at end of life) create no gap; and because recording_run_id is per-boot ({service}-{host}-{boot_ns}), that producer never reappears, so the design's cross-window continuity check can never close the stream — max_seq is simply 'the highest that happened to arrive'; (b) TOTAL silence — an instance whose sole sink never delivered anything (boot-time fatal sink error, full-window broker outage under fail_open) leaves no manifest row at all, so the window can look 'complete' while an entire producer is missing. The design explicitly defers loss_marker control records to later-scope with 'sequence gaps suffice for v1' — they do not suffice for exactly the loss modes a sole-sink fail_open design most needs to surface. v1 needs at minimum a periodic/final sequence-checkpoint record or a metrics-vs-manifest reconciliation step before a window may be marked complete.
- The decision-10c consumer migration is wrong about deja-tui and fails as designed. deja-tui does NOT read {root}/recordings/{id}/events.jsonl: its discovery is hardwired to the filename semantic-events.jsonl (crates/deja-tui/src/lib.rs:12, find_semantic_artifact at :267-271, and the file-argument path at :236-238 rejects any other filename), and in today's demo it reads the PRIMARY JSONL the router writes via DEJA_SINK=both + DEJA_ARTIFACT_DIR=/harness-state/recording (docker-compose.deja.yml:81-82 -> {state}/recording/semantic-events.jsonl). Remove the JSONL sink and the demo's final TUI stage (run-deja-demo.sh:166-168 passes $STATE_DIR) finds no semantic artifact; 'deja-tui's local docs point at the public demo/harness-state example' is documentation drift, not proof of the new S3-pulled layout. This is a migration dependency, not a blocker to Kafka/S3 itself.

CORRECTIONS:
- code_ref via vergen is feature-gated OFF in the proposed candidate build. router_env/build.rs emits git metadata only with the optional `vergen` feature (the cfg(not(feature)) variant of generate_cargo_instructions is a no-op); router only enables it via its `vergen` feature, which is in `release` but NOT in default/common_default. The runner's documented command `cargo build --release -p router --features deja,v1 --bin router` (also what demo/lib.sh:62 uses) therefore produces a binary with NO VERGEN_GIT_SHA. This is unconditional, not the 'bare-mirror worktree spike' the open question suggests: either add `vergen` to the candidate feature set or make DEJA_CODE_REF injection by the runner the primary mechanism (the runner already resolves the sha).
- Sampler injection site: get_application_builder (vendor/.../crates/router/src/lib.rs:390) has only request_body_limit/cors/trace_header in scope — NOT 'conf/state'. AppState (with superposition_service: Arc<SuperpositionClient>, routes/app.rs:145) is in scope in the caller mk_app (lib.rs:116). Wiring .with_record_sampler(...) requires threading the sampler through get_application_builder's signature from mk_app — small plumbing change, but the design's stated injection premise is wrong as written.
- Kafka-only boot policy is unspecified: today every deja_boot::install failure path deliberately degrades to 'JSONL only' / 'recording disabled' with a warning (events.source != kafka at deja_boot.rs:65, producer-create failure at :75). With JSONL gone these become 'silently record nothing', which compounds the manifest total-silence blind spot. Define an explicit fail-loud (or at least alarmed) boot policy for DEJA_MODE=record when the sole sink cannot be constructed.
- Drop-flush detail: the current silent Drop flush window is 30s (FLUSH_TIMEOUT, crates/deja-record/src/writer.rs:21), not '5s' as stated in 2.2 item 3.
- Consumer-list precision: divergence scoring does not read events.jsonl — divergence/mod.rs:461-464 loads lookup-table/observed/http-diffs only; the recording reaches it via render_lookup_table. The actual direct events.jsonl consumers are the lookup renderer (lookup/mod.rs:30), the kernel (KERNEL_RECORDING_PATH wired to recording_events_path at lifecycle/mod.rs:417-420), and the visualizer (demo/visualize-replay.py:72). Plus the deja-semantic-metrics bin (crates/deja-record/src/bin/deja-semantic-metrics.rs:33) reads semantic-events.jsonl from the artifact dir and dies with the JSONL sink.
- Event-size figure: prior local demo recordings averaged ~2.9-3.0 KB/event, not 4.2 KB; the envelope-overhead conclusion (~3-4%) is unaffected.
- external_services' deja declaration today lacks default-features=false (Cargo.toml:51), unlike the other six; normalize it during the git-dep swap so all 7 are uniform.
- The 5-entry [patch] block is mostly redundant: only `deja` is a direct dependency of HS manifests; the other four resolve via in-repo path deps of the facade (and follow the patched local checkout automatically). Cargo will warn 'unused patch' for the extra four entries — either patch only `deja` or have the dev-link script expect the warnings.
- API nit: RecordingHook::with_sink takes (sink, recording_run_id) (see deja_boot.rs:135-138), not the single-argument form shown in 2.2; trivial.
- Adding an empty [features] section to the facade changes nothing semantically — `default-features = false` against a crate with no features is already a harmless no-op; keep the fix but drop the claim that it makes the flag 'meaningful'. The real publishability blockers (version-less path deps at crates/deja/Cargo.toml:11-14, repository = "" at Cargo.toml:20, workspace version 0.1.0 -> 0.2.0) are correctly identified.
- Production must leave the graph layer disabled: the HS patch also installs ExecutionGraphLayer when DEJA_GRAPH_DIR is set (setup.rs deja_layer), writing an unbounded local JSONL outside the Kafka/manifest story. The demo compose doesn't set it, but the production recording config should explicitly forbid it (or it becomes an unaccounted local artifact).

NOTES:
Adversarial verification performed read-only against both repos. The design's load-bearing 'verified' claims overwhelmingly check out: integration patch is exactly 48 files +2,745/-220 on 2026.04.21.0 (git diff confirmed); runtime closure is exactly 5 crates totaling 10,630 LoC; the 7 HS crates carry ../../../../crates/deja path deps; rev/tag-pinned git-dep precedent confirmed (unified-connector-service-client@71fcc81f in external_services/router/hyperswitch_interfaces, rust-i18n fork @rev, rusty-money @tag in currency_conversion); feature fan-out at router/Cargo.toml:40 with deja absent from default/common_default; Cargo.lock diff +62; MSRV 1.85/1.85.0 with toolchain pin 1.85.1; HS has no rust-toolchain.toml; profile.release lto=true+CGU=1; request_id.rs deja branch at ~line 872 inside Box::pin; router_env<-external_services dependency direction confirmed inverted; SuperpositionClient::get_config_value over LocalResolutionProvider (superposition_provider 0.102.0, RwLock caches + polling, polling_interval=15 in development.toml, seed file exists); Kafka sink is fire-and-forget on the shared ThreadedProducer<DefaultProducerContext> with no-op flush (~deja_record_sink.rs:110-114) and envelope v1 shape as described; AsyncRecordWriter sync_channel + blocking fallback + permanent disable_after_error (writer.rs:374-377); CompositeSink swallows secondary failures; global_sequence allocated at EventBuilder::start (lib.rs:904) so the no-gap sampling invariant is sound; KeyStamper occurrences are (correlation,address,args_hash)-scoped so ingest sort by (recording_run_id,global_sequence) is sufficient; lenient u64 deserializer covers syntax_hash only, which is the only field that can exceed i64::MAX (timestamp_ns ~1.7e18 fits), so the golden-shape claim holds; mc find|sort|mc cat pull, lifecycle 6+6 stages, KERNEL_RECORDING_PATH, uncorrelated_events_tolerated, CandidateSpec variants (plus LocalPath, which the design omits), export-public.sh checklist/secret-grep, and the fmt/clippy/test-only CI baseline all confirmed. The three blocking findings are all repairable without changing the architecture: (1) the sampling flag must propagate through the DejaCorrelationLayer spawned-task path, (2) manifest sequence-coverage cannot see tail loss or whole-producer silence and needs a v1 checkpoint/reconciliation mechanism (not later-scope), (3) the deja-tui migration claim is factually wrong and the demo TUI stage breaks without a rename/copy or discovery change. Verdict: feasible with the listed fixes; no contradiction found with the ten fixed user decisions.