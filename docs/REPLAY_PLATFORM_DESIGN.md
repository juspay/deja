# Deja Replay Platform — Master Design

> Synthesized 2026-06-12 from four full designs ([docs/design/](design/)) that were each
> adversarially reviewed against the repos, plus a cross-design completeness critique.
> Where the full designs disagree, **this document is the reconciliation** and wins.
> Repo references: `WT` = the deja repo root, `HS` = the Hyperswitch tree (today the
> nested `vendor/hyperswitch-deja-clean`, which is **local-dev only**).

## 0. What we are building

Two products on one foundation:

1. **The recording store** — production/staging routers record sampled traffic through
   Kafka into S3 as time-windowed, manifested, replayable recordings.
2. **The replay platform** — a dashboard + pipeline that takes a recording selection +
   a code ref (tag/commit/PR/branch) + params, builds the candidate from source on a
   runner VM, replays, and serves the verdict: scorecard, structured-log diffs with
   field-level semantic diffs, and trace-upward execution-graph navigation.

### Fixed decisions (user-set; design within these)

| # | Decision |
|---|---|
| 1 | Continuous **time-windowed** capture in production; discrete named **sessions** stay for local dev |
| 2 | Per-request record/skip bool comes from **Superposition**; the strategy evolves there |
| 3 | Design for production-sampling scale (10⁷–10⁸ ev/day) but **staging-only data until masking lands**; layout carries the masking hook |
| 4 | Replays run on **dedicated runner VMs** (compose flow generalized); protocol must admit k8s executors later |
| 5 | Candidates **build from source** from the **real Hyperswitch repo** refs; the nested vendor layout is local-dev only |
| 6 | Dashboard = web UI grown on **deja-orchestrator** (TS SPA); web is the primary explorer, deja-tui stays |
| 7 | v1 milestone = the **manual PR replay gate** (no webhooks/schedules) |
| 8 | **Auth-light but audit-ready**: every mutation records actor + full params, append-only |
| 9 | Orchestrator state moves to **Postgres** |
| 10 | **Kafka is the sole record sink** (JSONL deleted), in prod *and* the demo; manifests become the loss-detection mechanism of record |

### System overview

```
            Superposition (record? bool per request)
                  │
 hyperswitch ─────┤ ingress sampling gate (before any event/sequence exists)
   router         ▼
   (post-PR)  macro instrumentation → RecordingHook → AsyncRecordWriter
                  │                       (bounded queue; fail_open drop point lives HERE)
                  ▼
              HardenedKafkaSink (SOLE sink: envelope v2, idempotent producer,
                  │              awaited delivery, sink markers, real flush)
                  ▼
              Kafka deja.recordings.<env>  (12 partitions, key = correlation_id)
                  │
              Vector (acks on sink) ──► s3://deja-recordings-<env>/landing/…   (7d TTL)
                                              │
                                        deja-compactor  (window mode prod / session mode dev)
                                          dedup → sort → [credential redaction + MASKING HOOK]
                                              │
                                        s3://…/windows/… | sessions/…  (manifest = seal + loss account)
                                              │
              Postgres catalog ◄──────────────┘
                  │
   deja-orchestrator (axum + sqlx + SPA)  ◄── browser: catalog → schedule → runs → scorecard → explorer → trace-up
                  │  pull protocol (outbound HTTPS only)
                  ▼
   runner VM: claim → checkout ref → cargo build (cached) → bake image →
              provision stack (egress-blocked) → render lookup shards → kernel → score →
              upload artifacts + divergence rows → complete
```

---

## 1. Shared contracts (reconciled — every component implements against these)

### 1.1 Event identity & ordering

This spec resolves the four interacting sequence problems the reviews found
(cancellation gaps, sampling gaps, spawned-task leaks, multi-producer collisions).

**Identities.** Three distinct fields, all stamped in the envelope:

| Field | Meaning | Set from |
|---|---|---|
| `instance_id` | one per process incarnation: `{service}-{host|pod}-{boot_ms}` | computed at boot |
| `capture.mode` + `capture.session_id` | `window` (continuous) or `session` (discrete, local dev — carries today's `DEJA_RECORDING_RUN_ID`) | env |
| `SemanticEvent.recording_run_id` | **the graph-join scope**: = `session_id` in session mode, = `instance_id` in window mode | resolver below |

One resolver, used by **both** the RecordingHook and the execution-graph layer:
`DEJA_RECORDING_RUN_ID → DEJA_RUN_ID → instance_id`. (Today graph nodes read only
`DEJA_RUN_ID` while events read both — the demo overlay sets only the former's sibling,
so graph joins silently null out. Unify in `deja-record`; the index builder also
cross-checks `event.recording_run_id == node.recording_run_id` and surfaces mismatch
as a capability warning, never as empty traces.)

**Sequencing rules:**

1. `global_sequence` is allocated **at emit time** (writer enqueue), not at call start.
   Today's start-time allocation + finish-time emission means every cancelled in-flight
   future (client disconnect, `select!`/timeout, shutdown abort — patterns Hyperswitch
   demonstrably has) permanently burns a sequence number and would make virtually every
   production window read "sealed-with-gaps". Emit-time allocation makes per-instance
   gseq gap-free *by construction*. Within-correlation replay order never depended on
   it (`request_sequence` is the canonical order; gseq is the tiebreak).
2. The **sampling gate runs before any event exists** — an unsampled request skips
   `EventBuilder` entirely, so sampling never creates gaps. Sampled events and
   unconditional uncorrelated/background events share one contiguous counter.
3. The sampling decision **propagates to spawned tasks**: deja-context gains a
   correlation→decision registry (modeled on its existing string-keyed task-context
   map) consulted by `DejaCorrelationLayer` when it re-enters a correlation on a
   spawned task. Without this, unsampled requests' spawned-task events still record
   (defeating sampling) or sampled requests lose their spawned-task tails (fake
   divergences). This is part of the HS PR.
4. **The compound key `(instance_id, global_sequence)` is the event's identity**
   everywhere downstream: compactor dedup, `LookupEntry.source_event`,
   `ObservedCall.source_event`, divergence rows, and UI deep-link anchors
   (`?row=i:pod-7f9c…:123`). Bare gseq is ambiguous the moment a window spans two
   pods or one restart — and staging is multi-pod, so this is v1-critical, not a
   scale deferral.
5. **Kernel drive order** for windowed recordings = ingress event-time
   (`http_incoming.timestamp_ns`), not min-gseq (meaningless across instances).
   Session-mode keeps today's order.
6. Canonical on-disk order inside a window part: `(correlation_id, request_sequence,
   (instance_id, gseq))` — correlation-clustered, replay-ready.

### 1.2 The recording store

(Authoritative spec: [design/s3-store.md](design/s3-store.md), as corrected by its
review; the "raw/curated hour-window" and "orchestrator materializer" variants from
the other designs are **superseded** by this one.)

- **Buckets**: `deja-recordings-<env>`; local MinIO keeps `deja-recordings`.
- **Prefixes**: `landing/v1/…` (Vector raw output, 7-day TTL, never read by replay),
  `windows/v1/…` (compacted canonical windows), `sessions/v1/…` (local-dev discrete
  sessions, same canonical shape), `manifests/` live inside each window dir; data
  objects are **tagged at PUT** so S3 lifecycle rules can expire data while keeping
  manifests (lifecycle filters cannot match suffixes — tags, not name patterns).
- **Windows**: 5-minute UTC-aligned base windows on **event time**; a replayable unit
  is any contiguous range of sealed windows. Vector computes the window key in a VRL
  remap (with a `win=malformed` fallback) and lands the **full envelope** (no unwrap;
  the remap deletes Vector's own injected source fields).
- **Vector hardening**: `acknowledgements.enabled: true` **on the aws_s3 sink** (offset
  commits then defer to S3 PUT success), zstd compression, pinned image version.
- **`deja-compactor`** (new crate, runs on runner VMs / CronJob later): claim →
  seal-check (grace 10 min + consumer-lag check, hard cap with `sealed-with-gaps`) →
  stream/dedup/sort (external merge for bursts) → **credential redaction + masking
  hook** (§5.1) → correlation-clustered data parts + per-correlation index →
  `manifest.json` PUT **last** (its existence = the seal) → catalog upsert. Late
  events: sweeper compacts into `late/` parts and bumps `manifest_revision`
  (consumed-object keys are persisted so the sweeper can tell new from processed).
- **Manifest** (full schema in the s3-store doc): per-instance gseq coverage ranges,
  gaps with `explained_by` (sink markers), duplicates dropped, cross-window edge
  continuity, event/envelope schema versions, code shas, masking state, data-file
  checksums, correlation index (`ingress` summary per correlation makes the request
  browser and replay filters index-only).
- **Tail-loss closure** (review blocker): manifests alone cannot see tail loss (a
  crashed producer's last events) or whole-producer silence (sink never delivered).
  v1 adds: (a) a **periodic sequence-checkpoint marker** (`deja_sink_marker{kind:
  "checkpoint", last_gseq}` every N seconds/events) so the compactor can bound tail
  loss; (b) the `eof` marker on graceful shutdown (the session lifecycle gains an
  explicit router stop/flush step so it actually fires); (c) a **metrics-vs-manifest
  reconciliation** check before a window may be marked `complete`.
- **Catalog** (Postgres): `recording_windows` + `recording_window_producers` rows
  upserted by the compactor (idempotent on `(window_id, manifest_revision)`,
  audit-logged with a service-principal actor). `window_id` is flat/URL-safe
  (no `/` — it is both a PK and a path segment).
- **`manifest_revision` is threaded through**: catalog rows, the run's
  `config_snapshot`, the scorecard, and an "amended since scoring" UI badge.
- **Local parity**: the demo records in session mode through the *same* envelope →
  Vector → landing → compactor → manifest path, so every demo run regression-tests
  the production pipeline (the 207-event fixture asserts `gseq 0–206 complete`).

### 1.3 Sink policy & delivery semantics (decision 10a)

One env var, one spec, used by the HS PR, the demo overlay, and the runbooks:

```
DEJA_SINK_POLICY = block | fail_open      # default: block (demo/staging), fail_open (prod)
```

- The sink is rebuilt around a **deja-owned producer** (NOT the shared
  `KafkaProducer::create` — hardening that constructor would silently change
  Hyperswitch's analytics producer semantics): `acks=all`,
  `enable.idempotence=true`, bounded buffering, zstd, real
  `flush(timeout)` (today's flush is a no-op and the only flush is a 30s Drop-time
  one), delivery-report accounting per gseq, and error classification — transient
  errors never permanently disable the writer (today any primary error does);
  fatal errors (auth, unknown topic) disable loudly.
- **`fail_open`'s drop decision executes at the writer-enqueue layer**
  (`try_send` → count + remember gseq range), not only inside the sink. The review
  proved the sink-level-only version still stalls request threads through the
  writer's untimed blocking send within seconds of a sustained broker outage —
  exactly the "instrumentation never takes down payments" failure it claims to
  prevent. When the broker recovers the sink emits
  `deja_sink_marker{kind:"dropped", gseq_from, gseq_to}` so the loss is *accounted*.
- **Boot policy**: with the sole sink unconstructable in record mode, recording is
  off — but loudly: a `deja.sink_fatal` metric + error log + (window mode) the
  missing-producer manifest reconciliation alarm. Never aborts router boot.
- Loss accounting: metrics are the alarm (`deja.events_enqueued/delivered/
  dropped_queue_full/delivery_failed/sampler_errors`), **manifests are the audit**.

### 1.4 Version compatibility

Stamped surfaces: `event_schema_version` (per event), envelope `schema_version`,
`lookup_policy_version` (LookupTable), `callsite_identity.version`, plus
`deja_version` + `code.sha` per envelope and as OCI labels on candidate images.
The compat table is **data** (`compat.toml` in the deja repo, shipped with the
orchestrator). The orchestrator gate runs at enqueue (recording manifests vs the
candidate ref's `Cargo.lock`-pinned deja version — knowable *before* any build because
of the git-dep pin) and again post-build against image labels; the candidate's
`LookupTableHook` asserts `policy_version` at boot as defense in depth. Candidate
code sha comes from **runner injection** (`DEJA_CODE_REF`) as the primary mechanism —
the `vergen` git metadata is feature-gated off in the standard build invocation,
so trusting it silently yields builds with no sha.

### 1.5 Graph contract & transport posture

**The join contract** (verified mechanics): `SemanticEvent.graph_node_id ==
graph_node.node_id` is the single durable foreign key — stamped at call time from
the layer's span→node map, which is deleted on span close precisely so recycled
tracing span ids can never mis-attribute later events. `tracing_span_id` is
diagnostic only (arena-recycled). Request-level stitching is the soft join: root
span `fields.request_id == correlation_id`. The two sequence spaces are unrelated
(`node.sequence` = span-creation order, `event.global_sequence` = call order) —
**join by id, never by sequence**. Node ids are allocation-ordered per process, so
record-tree ids and replay-tree ids can never be joined directly: cross-run tree
alignment is structural (span-name path + parent-scoped position — the same
occurrence-class discipline as the lookup ranks), which is the foundation for the
record-vs-replay tree diff (later scope).

**ObservedCall gains `graph_node_id` (+ the §1.1 scope id) in M2** — a one-field
additive change mirroring the event-side stamping at lookup time. Without it,
matched replay calls can be placed on the *record* tree (via `source_event`), but
**novel calls — the ones you most want to localize — have no tree coordinates.**
With it, novel calls land on the *replay* tree exactly where the tree diff needs
them. Orphan events (`graph_node_id: null`, span-less background work) render in
an explicit "unattached" bucket, not silently dropped.

- **v1: local/demo only.** One overlay change *plus the resolver unification of
  §1.1* (without it the trace join is silently empty): set the graph dir on both
  record and replay services; the lifecycle copies `graph/execution-graph.jsonl`
  into the run's recording dir; the explorer reads it (`{"node": …}` wrapper rows).
  Production configs explicitly do **not** set `DEJA_GRAPH_DIR` (an unbounded local
  file outside the manifest story).
- **v2: production transport** as a designed workstream, not a hand-wave: envelope
  gains `artifact_type: "deja_graph_node"` (the current envelope hardcodes one type
  and types its payload as `SemanticEvent` — this is a schema addition),
  `ExecutionGraphLayer` gains a `with_sink` constructor, Vector routes on
  artifact_type to a `graph/` prefix, manifests account for graph objects, masking
  covers span fields (they are Debug-captured and unmasked, same as events).

---

## 2. The Hyperswitch PR (record side)

One PR, three reviewable commits (deps/boot · instrumentation · production
recording), on top of the existing 48-file integration patch.

### 2.1 Packaging: rev/tag-pinned git dependency (in-tree REJECTED)

The 7 HS crate declarations switch from the local-only `path = "../../../../crates/deja"`
to `git = "https://github.com/<org>/deja", tag = "vX.Y.Z"`. Upstream precedent
verified (HS already ships rev-pinned git deps: connector-service, rust-i18n,
rusty-money). The PR stays +2.7k lines instead of +13k; deja remains single-source;
the compat gate can read the pinned version from any ref's `Cargo.lock` without
building. crates.io is the explicit endgame once maintainers signal merge intent
(publish order: context → core → derive → record → facade). Prereqs landing in the
deja repo now: `version` on every inter-crate path dep, `repository` filled,
version bump + tag discipline. Local dev replaces the nested vendor with a
side-by-side checkout + an uncommitted `[patch]` block (a `deja dev-link` script
writes it; patch only the `deja` facade — the other four follow its path deps).
`external_services`' declaration gains the `default-features = false` the other six
already have. MSRV contract: deja never exceeds HS's `rust-version` (CI-enforced).

### 2.2 Superposition sampling at ingress

- Trait inversion (`router_env` cannot see `external_services`): `RecordSampler`
  trait in router_env; impl in router over `SuperpositionClient::get_config_value::
  <bool>("deja_record_request", ctx, targeting_key=request_id)` — in-memory CAC eval
  (µs), 15s freshness, deterministic percentage bucketing via the targeting key.
- **Wiring**: the sampler is constructed in `mk_app` (where `AppState` and the
  superposition service actually live — *not* `get_application_builder`, which has
  neither) and threaded through `get_application_builder`'s signature into
  `RequestIdentifier::with_record_sampler(...)`.
- The decision runs **before** request-body capture; unsampled requests skip capture
  and event creation entirely (§1.1 rule 2) and the flag rides the correlation
  registry to spawned tasks (rule 3).
- Failure semantics: any error → `false` (don't record), counted; never blocks
  traffic. Break-glass: `DEJA_SAMPLING = superposition | all | off` (the demo runs
  `all`, preserving today's behavior with zero Superposition coupling).
- Config: `deja_record_request` default **false** in the seed/defaults; context
  dimensions v1 = `{api_path_group, http_method, deployment_env}` (ingress is
  pre-auth; no merchant dimensions available).
- Superposition is **not on the v1 critical path** — staging can record at 100%
  while the gate ships; the hook lands with the PR regardless.

### 2.3 Envelope v2 + hardened sink + reference config

Envelope v2 (full schema in the store doc): identities per §1.1, `code.sha`/
`code.version`/`deja_version`, `event_time_ns` at top level (also set as the Kafka
message timestamp), `capture.mode`, `masking` stamp, `deja_sink_marker` artifact
type for loss accounting. Kafka headers gain `dedup_key`. The sink and policy per
§1.3. The PR carries only in-repo config: the topic stanza, superposition seed
defaults, and the local-dev overlay (`docker-compose.deja.yml`,
`config/vector.deja.yaml` updated to the no-unwrap envelope shape). Production
Vector aggregator config, topic/bucket/IAM Terraform, and runner provisioning live
in the deja repo's `ops/` and never burden the upstream PR. Topic
`deja.recordings.<env>`: explicitly provisioned, 12 partitions, retention 24–48h,
`min.insync.replicas=2`.

---

## 3. The orchestrator (deja-orchestrator grown up)

(Authoritative: [design/replay-pipeline.md](design/replay-pipeline.md) as corrected;
packaging form per §2.1 supersedes its in-tree assumption.)

- **Server rework is a hard prerequisite**: today's server is single-threaded
  `tiny_http` — it cannot serve an SPA + SSE + runner long-polls. Move to
  axum + tokio + sqlx/Postgres. **Toolchain note**: the workspace pins 1.85.1 and
  the kernel hand-rolled HTTP/1.1 precisely because the icu/url chain needs 1.86;
  pick the Postgres/web dependency set against the pin or bump the toolchain first
  (decide at M0 — bumping also unblocks `diesel_cli` and the url ecosystem).
- **Postgres schema** (full DDL in the pipeline doc, amended): `runners`,
  `recordings` (+ catalog tables §1.2), `replay_runs` (code_ref + `resolved_sha`
  pinned at enqueue, immutable `config_snapshot`, verdict ≠ state), `run_stages`
  (append-only per-attempt stage history with timings + detail jsonb),
  `artifacts`, `run_log_chunks` (**PK includes `attempt`**), `audit_events`
  (INSERT-only at the grant level; machine actors use `runner:<id>` /
  `system:compactor` conventions), `schedules` (rows now, executor v2).
  **One migration crate owns the whole schema** — orchestrator, catalog, divergence
  index, audit (review item B8).
- **Queue**: `FOR UPDATE SKIP LOCKED` claims; 5-min leases renewed by 15s heartbeats;
  sweeper marks expired leases `lost` and requeues retryable failure classes.
- **Runner protocol** (pull; outbound HTTPS + bearer token only): register /
  heartbeat (response carries cancel/drain control) / claim (long-poll, job document
  with presigned artifact PUTs) / stage transitions / log chunks / artifacts /
  **`POST /runs/{id}/divergences`** (idempotent, size-bounded — the review found
  runner-side scoring had no path into the Postgres divergence index) / complete
  (state rule: `completed` requires scorecard + divergence ingestion).
- **REST API v1** (`/api/v1`): recordings catalog (list/detail/requests),
  `POST /runs` (resolves sha, compat gate, masking gate, config snapshot, audit),
  runs list/detail/stages/logs (SSE via LISTEN/NOTIFY as wake-up only — resume
  reads the tables, NOTIFY payloads can't carry log lines), scorecard, divergences,
  events, graph trace/subtree (always identity-scoped), artifacts, audit, runners.
  Legacy demo routes stay during migration **but are local-mode-only or get
  synthetic actors** — unaudited mutating routes would violate decision 8.
  `repo_tag` resolves to a sha at submit (no new CandidateSpec variant needed).
- **Local mode**: same binary, embedded runner, `FsArtifacts`, and the orchestrator
  boots its own Postgres (the overlay deliberately unpublishes pg's ports; local
  mode runs a dedicated pg container/port at startup — decided detail of M0).

---

## 4. The runner (build-from-source + replay execution)

- **New crate `deja-runner`**, extracted from `lifecycle/mod.rs` behind an
  `Executor` trait (`ComposeExecutor` v1; k8s Job executor later — the protocol never
  mentions docker). Embedded in-process for local mode so the demo exercises the
  same claim/stage/complete protocol.
- **Build stage**: bare git mirror per repo → worktree at the resolved sha → ref
  sanity check (deja feature + pinned deja version exist → else `incompatible`) →
  isolated builder container (rustup honoring `rust-toolchain.toml` **with a pinned
  default fallback — HS has none**) with persistent cargo-registry, sccache, and
  per-(repo × profile × feature-set) target volumes → thin image bake (generalizing
  `demo/Dockerfile.hyperswitch-semantic`) with version labels; no registry in v1
  (build + replay are one job on one runner).
- **Build profiles**: `production` (the ref's own fat-LTO profile) and `fast`
  (**gate default**) — lto off/thin + high codegen-units + opt-level 2 + incremental
  + mold, exactly the override set the demo's `lib.sh` now applies
  (`DEMO_CARGO_PROFILE`). The scorecard records which profile ran. Honest anchors:
  full release build ≈ 11–35 min depending on hardware/load (both numbers observed);
  warm-cache `fast` rebuilds of single-file candidate patches were **minutes** in the
  baseline matrix runs.
- **Provisioning**: runner-rendered compose template (per-run project name
  `deja-run-{id8}`, labeled containers, allocated ports — no singletons), migrations
  from the **candidate** ref, fresh pg per run.
- **Replay prep**: manifest fetch (refuse `sealing-delayed` / unmasked-prod; warn
  amended/gaps into the run) → correlation index filter/sample (`random n=2000`
  default for the gate) → batch materialization (≤50k events/batch keeps the lookup
  shard ~1 GB worst-case) → per-batch lookup render (streaming NDJSON) → kernel
  drive (ingress-time order, §1.1) → aggregate scorecard.
- **Cleanup guarantees**: per-run supervisor `finally` (compose down -v + workspace
  removal), startup orphan sweep against a local run journal, LRU cache GC against
  a disk high-watermark, lease expiry as the orchestrator-side backstop.
- **Security (workstream, v1 minimums)**: allowlisted repos/orgs for code refs;
  build-phase egress restricted to git/crates.io; replay-phase egress
  **deny-by-default** (§5.2); runner credentials scoped (S3 read on recordings,
  presigned-only writes); per-run workspace isolation; build = arbitrary code
  execution and is treated as such.

---

## 5. Safety workstreams (cross-cutting; v1-gating where marked)

### 5.1 Sensitive data in recordings — **v1-gating**

The recording flow can capture credential-like fields and card-like payloads, and
every downstream surface (Kafka, S3, lookup tables, observed files, Postgres
divergence rows, the auth-light browser) inherits that sensitivity. "Staging-only
until masking" does NOT cover this. Ahead of the full masking workstream, v1 ships:

1. a **credential-field redaction denylist** applied in the compactor's masking seam
   (`Authorization`, `api_key`, `card_number`, `card_cvc`, JWT-shaped strings…) —
   replay-compatible because redaction is applied before lookup rendering ever sees
   the events (recorded and replayed views redact identically);
2. SSE-KMS on the buckets + IAM separation (producer write-only to landing;
   compactor read-landing/write-windows; runners read-windows);
3. UI-side redaction rules as defense in depth;
4. retention policies covering the *derived* secret-bearing artifacts (lookup
   tables, observed, divergence rows), not just recordings.

### 5.2 Replay egress control — **v1-gating, verified risk**

Outbound HTTP substitutes on lookup hit but **executes live on miss** — a candidate
whose change alters outbound args would hammer real connectors from runner VMs with
recorded credentials. v1: deny-by-default egress on the replay network (no external
route in the compose template; explicit proxy allowlist if ever needed) + a distinct
observed outcome `novel_outbound_blocked` so blocked calls render as such instead of
as status-0 noise.

### 5.3 Replay DB-state provisioning — **v1 contract documentation**

DB boundaries fall through to live pg on miss/unknown-Err; the demo only works
because replay shares the record-phase database. Runner replays use fresh pg +
candidate-ref migrations; the replay contract documents the fall-through semantics,
the scorecard classifies fall-through-induced divergences distinctly, and schema
drift between the recording's ref and the candidate's migrations is surfaced as a
run warning. (State snapshotting/seeding = later scope.)

### 5.4 Observability — named metrics, SLOs

Under `fail_open` + manifests-as-loss-detection, alerting is data integrity.
v1 metric set: producer (`deja.events_enqueued/delivered/dropped_queue_full/
delivery_failed/sink_fatal/sampler_errors`), Vector consumer-group lag (SLO: lag
< window grace), compactor seal latency + stuck-window alert, runner fleet/lease
health, S3 PUT errors, recorded-vs-landed reconciliation. Export through the
existing HS metrics infra on the producer side; orchestrator/compactor export
their own.

### 5.5 Cost & capacity

~42–420 GB/day raw at target scale (zstd ≈ ÷6); landing+windows ≈ 2× until TTL;
**Kafka retention (24–48h) is the loss floor for Vector outages** — derive it from
the worst outage you intend to survive; partition count caps Vector parallelism;
S3 request costs scale with windows × instances key cardinality. The cost telemetry
feeds the Superposition sampling policy — that's the designed cost lever.

### 5.6 Gate signal quality

The demo's verdict still needs a human-set expectation flag today. v1 policy:
an optional **merge-base control run** (self-replay of the recording against the
recording's own sha) subtracts environmental noise from candidate-caused divergence;
documented verdict semantics (what "safe to merge" means); per-divergence triage
states deferred but the scorecard distinguishes blocking/environmental/fall-through
classes from day one.

---

## 6. The dashboard (v1 surface)

(Authoritative: [design/dashboard-ux.md](design/dashboard-ux.md) as corrected.)

React 18 + Vite + TS SPA, embedded in the orchestrator binary (rust-embed; one
deployable, no CORS). TanStack Query/Virtual; SSE with poll fallback; deep links
everywhere (`?row=i:<instance>:<gseq>` anchors); `X-Deja-Actor` header required on
mutations.

Pages: **Recordings catalog** (windows + sessions, completeness/gap badges, masking
state, boundary sparkline; "Schedule replay" CTA, disabled-with-reason on policy) →
**Schedule replay** (recording + code ref + params; expected-duration from build
telemetry) → **Runs** (list; detail = stage timeline from `run_stages`, build panel,
live logs, failure tail) → **Scorecard** (verdict banner, per-boundary bars,
rank-resolution histogram with rank-6 amber "positional" fragility signal) →
**Divergence explorer** (the centerpiece):

- Request rail (ingress × scorecard × http-diffs join), diverged-first filtering.
- **Split diff** per correlation: server-derived `DiffRow`s (the TUI's
  `build_diff_rows` moves server-side into a shared module — TUI and web render
  identical rows); field-level JSON diffs (`json_path` / baseline / candidate),
  omitted/novel/changed fusion with honest counters, rank badge per row, matched
  rows collapsed. Honesty notes built into the UI: matched rows' right side is the
  substituted recorded value; true candidate output exists only at HTTP layer
  (the "is_error flip" surface does not exist — ObservedCall carries no result).
- Timeline tab (all events, substitution status), Events tab + raw-event drawer
  (with derived lookup addresses per rank), HTTP tab (status-0 rendered as
  "transport error"), run-level divergence triage list (Postgres-backed).
- **Trace-up** (`/runs/:id/explorer/trace/:event`): breadcrumb + collapsible tree
  over the execution graph, identity-scoped joins mandatory, lazy subtree loading,
  explicit degraded states (no graph captured / null node id / chain truncated /
  novel calls trace into the *replay* tree once M2's `ObservedCall.graph_node_id` lands; an "unattached" bucket holds span-less events). Root verification badge
  (`fields.request_id == correlation_id` — 4,949/4,954 held on real data).

deja-tui stays for local use, **with its discovery fixed**: it (and the
`deja-semantic-metrics` bin) are hardwired to the JSONL-sink filename; the
session lifecycle materializes the pulled copy at the legacy path
(`recording/semantic-events.jsonl`) *and* the TUI learns the
`recordings/{id}/events.jsonl` layout — both, so neither old data nor the new flow
breaks. The visualizer, renderer, kernel, and scorer already read the pulled copy
(verified).

---

## 7. v1 milestone plan (dependency-ordered; "demo stays green" is an invariant at every step)

The demo (`run-deja-demo.sh` / `run-deja-matrix.sh`) is the project's only
end-to-end regression fixture — every milestone keeps it passing.

| M | Deliverable | Key contents | Unblocks |
|---|---|---|---|
| **M0** | Server + store foundation | axum/tokio rework (toolchain decision), sqlx + the **single migration crate** (all schemas §3), local-pg bootstrap, legacy-route compat + audit policy | everything |
| **M1** | Runner extraction | `deja-runner` + Executor trait + pull protocol (incl. divergence ingestion), embedded-local mode, per-run project names/ports, cleanup guarantees | M3, M6 |
| **M2** | Event identity + hardened sink | §1.1 spec in deja-record (emit-time gseq, gate-before-allocation, registry, compound keys through lookup/observed/divergence), `ObservedCall.graph_node_id` (§1.5), §1.3 sink + `DEJA_SINK_POLICY`, envelope v2, **JSONL sink deletion** with the TUI/metrics-bin migration, demo overlay flips to Kafka-only | M4, the PR |
| **M3** | Build-from-source | git-dep packaging prereqs in the deja repo (§2.1), candidate resolver + builder + caches + profiles, compat gate, ref sanity | the gate |
| **M4** | Compactor + catalog | `deja-compactor` (session mode first — the demo proves it; then window mode), manifests + checkpoint markers, catalog tables/API, S3 lifecycle + tags, **credential redaction denylist** (§5.1) | M5, M6 |
| **M5** | Recording store ops | Vector v2 config (envelope landing, acks, zstd, pinned image), topic/bucket/IAM IaC in `ops/`, observability metric set (§5.4) | staging capture |
| **M6** | Dashboard | SPA + API v1 + explorer + graph trace-up (with the §1.1 resolver fix + overlay graph capture), egress-blocked replay template (§5.2), DB-provisioning contract (§5.3), gate noise policy (§5.6) | **the v1 gate** |
| — | **v1 shipped** | staging records continuously (sampled or 100%) through Kafka→Vector→S3→compactor; a human picks a window + a PR ref in the UI; a runner builds, replays egress-blocked, scores; the verdict + explorer + trace-up are shareable URLs; every action audited | |

The Hyperswitch PR (sampling hook + hardened sink + envelope v2 + git-dep switch)
can land any time after M2/M3 define its contracts; the upstream review clock runs
in parallel with M4–M6.

### Later (explicitly deferred)

Webhook/PR-check automation and schedules executor · masking workstream (beyond the
credential denylist) and prod recording · production graph transport (§1.5 v2) ·
record-vs-replay tree diff (structural alignment) · cross-run comparison + trends ·
k8s executor + image registry · parquet analytics copies · state snapshotting ·
SSO · per-recording SQLite indexes for giant windows.

## 8. Consolidated open questions

1. Vector `aws_s3` zstd + sink-acknowledgement behavior on the pinned version —
   validate in the demo before relying on it (cheap experiment).
2. Uncorrelated/background event volume in real staging traffic (demo shows 0;
   archived audits suggested it can be large) — decides whether they need their own
   sampling knob.
3. Toolchain: stay on 1.85 (constrains sqlx/url ecosystem) vs bump (diverges from
   HS's MSRV while the kernel's hand-rolled HTTP exists for exactly this reason).
4. Runner hardware sizing → real build-time anchors for the gate's UX promise
   (the 11min vs 35min spread is hardware/load, and the `fast` profile changes it
   again).
5. Replay of mid-stream windows: how much divergence noise do warmed caches cause
   in practice on staging traffic (drives whether state snapshotting moves up).
6. PCI posture review of the staging pipeline with the §5.1 minimums — does
   card-bearing staging traffic still put the platform in formal PCI scope?
