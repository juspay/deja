> Full design produced by the planning workflow (2026-06-12). The adversarial review
> appended at the bottom contains corrections that SUPERSEDE the body where they
> conflict; the reconciled master plan is ../REPLAY_PLATFORM_DESIGN.md.

# Deja Production S3 Recording Store — Design

All repo references: worktree root `WT = {WT}`, vendor fork `HS = WT/vendor/hyperswitch-deja-clean`.

---

## 0. Architecture overview

```
                    Superposition (record? bool, per request, in-memory eval)
                          │
 hyperswitch router ──────┤
   macro instrumentation → RecordingHook → AsyncRecordWriter → HardenedKafkaSink (SOLE sink)
                          │                                        │ envelope v2, idempotent producer,
                          │                                        │ awaited delivery, loss markers
                          ▼                                        ▼
                                            Kafka topic deja.recordings.<env>  (N partitions, key=correlation_id)
                                                       │
                                            Vector (kafka source, e2e acks) ── aws_s3 sink
                                                       │
                                            s3://deja-recordings-<env>/landing/...   (raw envelope parts, 7d TTL)
                                                       │
                                            COMPACTOR (new Rust binary; window mode in prod, session mode in dev)
                                              dedup → sort → [MASKING HOOK] → cluster by correlation
                                                       │
                                            s3://deja-recordings-<env>/windows/...   (canonical data + index + MANIFEST)
                                                       │                    │
                                          Postgres recording catalog ◄──────┘ (PUT /catalog/windows, audit-logged)
                                                       │
                                            deja-orchestrator + dashboard: pick window(s) + code ref
                                                       │
                                            runner VM: build candidate → batch correlations → render lookup shards
                                                       → kernel drives batches → scorecard + divergence explorer
```

The JSONL sink is deleted. Kafka is the only sink; the S3 `windows/` (or `sessions/`) copy produced by the compactor is the **only durable recording**, and the window manifest is the **loss-detection mechanism of record**.

---

## 1. Bucket / key layout

### 1.1 Buckets

One bucket per environment: `deja-recordings-staging`, `deja-recordings-prod` (local dev/MinIO keeps the existing name `deja-recordings`). Per-env buckets give clean IAM separation, independent lifecycle policies, and a hard masking posture: the prod bucket's policy denies all writes to `landing/` and `windows/` until the masking workstream lands (see §5) — first deployments physically cannot record prod traffic.

### 1.2 Prefix namespaces (one bucket, three classes)

```
deja-recordings-<env>/
  landing/v1/...      # Vector's raw output. Envelope lines. Short TTL. Never read by replay.
  windows/v1/...      # Compacted canonical windows. Manifest + data + index. THE recording.
  sessions/v1/...     # Local-dev discrete sessions (same canonical shape, keyed by session id).
```

### 1.3 Landing keys (what Vector writes)

```
landing/v1/win={YYYY-MM-DD}T{HH}-{MM}/svc={service}/inst={instance_id}/part-{%s}-{uuid}.ndjson.zst
e.g.
landing/v1/win=2026-06-12T14-35/svc=hyperswitch-router/inst=pod-7f9c4-1718203200/part-1718203512-3f9a…e1.ndjson.zst
```

- `win=` is the 5-minute base window floored from **event time** (`envelope.event_time_ns`), computed in a VRL remap — not strftime-on-event-timestamp, because the Kafka source's decoded event after unwrapping has no Vector-semantic timestamp; computing the window string explicitly in remap is deterministic and template-safe. The remap also sets a fallback `win=malformed` for any event missing `event_time_ns` so template rendering can never drop events.
- `inst=` comes from `envelope.instance_id` (a template on an event field, which Vector's `key_prefix` supports).
- Landing objects contain **envelope v2 lines** (Vector no longer unwraps `.event`) — the compactor needs the envelope metadata; unwrapping moves into the compactor.

### 1.4 What Vector's aws_s3 sink CAN do (and where it stops)

CAN (all used here):
- **Per-key batching**: batches accumulate per rendered `key_prefix` (so per window×instance), flushed on `batch.max_events` / `max_bytes` / `timeout_secs`. Production settings: `max_events: 20000`, `max_bytes: 64MB`, `timeout_secs: 30`.
- **`key_prefix` templating on event fields** (`{{ producer.instance_id }}` style) — already exercised today with `{{ .recording_run_id }}`.
- **Framing/codec**: `newline_delimited` + `json` (ndjson), `compression: zstd` (supported in the deployed `latest-debian` image; today's config uses `none`).
- **Unique object names**: `filename_time_format` + `filename_append_uuid` — no overwrite collisions.
- **End-to-end acknowledgements**: `acknowledgements.enabled: true` on the kafka source — consumer offsets commit only after S3 PUT success. This is the no-loss guarantee for the Vector leg and MUST be enabled (it is not today).

CANNOT (the compactor exists for these):
- No grouping of one correlation_id across batches/partitions; no sorting within a batch (arrival order only).
- No durable dedup (the `dedupe` transform is an in-memory LRU; restarts forget it). At-least-once means duplicate **events** after a Vector crash-before-commit, and the source's redelivery can replay whole batches into new objects.
- No manifests, no counts, no sequence accounting, no atomic "window is done" signal.
- No multi-partition order merging (irrelevant: per-correlation order is preserved by Kafka keying; cross-correlation order is explicitly not needed — ground truth §8.5).
- No Parquet in the OSS sink encodings; ndjson+zstd is the canonical format (a columnar analytics copy is Later-scope).

### 1.5 Canonical window keys (what the compactor writes)

```
windows/v1/svc={service}/dt={YYYY-MM-DD}/win={HH-MM}/
  manifest.json                      # written LAST; its existence = window sealed (S3 PUT is atomic per key)
  data/part-00000.ndjson.zst         # raw SemanticEvent lines (envelope stripped), correlation-clustered,
  data/part-00001.ndjson.zst         #   sorted (correlation_id, request_sequence, global_sequence); ≤250k events/part
  index/correlations.ndjson.zst      # one line per correlation_id → part + line range + ingress summary
  late/part-r2-00000.ndjson.zst      # late-arrival deltas, added by manifest revision 2+
e.g.
windows/v1/svc=hyperswitch-router/dt=2026-06-12/win=14-35/manifest.json
```

Partition dimensions, in order: service, date, window. **Code version is metadata, not a path dimension** — one window can span a deploy (two instance sets with different shas); the manifest's `producers[]` records each instance's `code_sha`, and the catalog indexes it. Env is the bucket. Date/window in the key gives cheap lifecycle rules and human-navigable listings.

---

## 2. Window manifest + compaction

### 2.1 Windowing model

- Base window: **5 minutes, UTC-aligned**, on event time (`event_time_ns` = `SemanticEvent.timestamp_ns`, captured at call start). A *replayable unit* = any contiguous range of sealed base windows; the dashboard composes ranges, the store never needs variable-size windows.
- Size math at target scale (measured 4,242 B/event): 10⁷ ev/day → ~35k events ≈ 147 MB raw ≈ ~25 MB zstd per window; 10⁸ ev/day → ~347k events ≈ 1.47 GB raw ≈ ~250 MB zstd per window. Both compact comfortably in RAM on a runner-class VM (external merge sort is still specified for burst safety).

### 2.2 Kafka partition ordering (today: emergent; production: engineered)

- Topic `deja.recordings.<env>`, **12 partitions** (10⁸/day ≈ 1.2k msg/s avg, ×10 peak ≈ 12k msg/s ≈ 50 MB/s), `retention.ms` = 24–48h (Kafka is a buffer, not a store).
- Partition key stays `correlation_id` (else `{instance_id}:{global_sequence}` for uncorrelated events). All events of one correlation therefore land on one partition **in produce order**, which is AsyncRecordWriter single-thread order — exactly the within-correlation order replay needs. Cross-correlation interleaving across partitions is irrelevant.
- Producer hardening makes this real (today it's emergent on a 1-partition auto-created topic): `enable.idempotence=true` (preserves per-partition order under retries, dedups broker-side resends), `acks=all`. See §8.
- Kafka **message timestamp set to event time** (`BaseRecord.timestamp(event_time_ms)`) so broker-level retention and consumer-lag-by-time work on event time.

### 2.3 Duplicates and late events

Three duplicate sources survive into landing: app-level producer resend after delivery-timeout ambiguity, Vector crash-before-offset-commit, Vector S3 retry edge cases. **Dedup key = `(instance_id, global_sequence)`** — globally unique per event because `global_sequence` is a per-process AtomicU64 and `instance_id` is unique per process incarnation. The compactor is the single, durable dedup point (counted in the manifest as `duplicates_dropped`).

Late events (events whose `win` floor maps to an already-sealed window — possible from producer buffering during a broker outage, or a slow Vector flush): a sweeper pass detects landing parts for sealed windows, compacts them into `late/part-rN-*.ndjson.zst`, and bumps the manifest to revision N with `status: "amended"`. Replay prep merges `data/` + `late/` (both correlation-clustered; k-way merge by the same sort key). The catalog notifies subscribed runs that their window was amended.

### 2.4 Window-close semantics

A window W seals when **both**:
1. `now ≥ W.end + grace` (grace default **10 min**: covers Vector's 30s batch timeout, S3 retries, producer buffering during transient broker pauses), and
2. the Vector consumer group's committed offsets, translated to timestamps via the admin API, are past `W.end + grace` on every partition (or lag == 0). If the lag check can't be satisfied, the window enters `sealing-delayed` (visible in the catalog) instead of sealing with false gaps; a hard cap (60 min) forces a seal with `status: "sealed-with-gaps"` plus an alert.

Sealing is atomic: data and index parts are written first, `manifest.json` is PUT last; manifest existence is the seal. The compactor is idempotent — deterministic output keys + a revision check make re-runs safe.

### 2.5 Sequence coverage = the loss-detection mechanism of record (decision 10b)

Per `(instance_id, window)` the manifest records observed `global_sequence` ranges (run-length encoded), duplicates dropped, and gaps. Because the Superposition sampling decision is taken at ingress (unsampled requests emit **no** events), `global_sequence` over recorded events remains gap-free at the source — every gap downstream is loss or a window-boundary straddle. Disambiguation:
- **Window straddles**: `timestamp_ns` and `global_sequence` are both assigned at call start, so per-instance gseq is monotone in event time (± tiny inversions). The manifest stores each window's per-instance `min/max` gseq; the catalog checks **cross-window edge continuity** (`max(W) + 1 == min(W+1)` within a small tolerance set) and only flags discontinuities not explained by an adjacent window as loss.
- **Explained gaps**: when the hardened sink drops under the fail-open policy (§8), it emits a `deja_sink_marker` envelope carrying the dropped gseq range and count; the compactor matches markers to gaps → `"explained_by": "sink_marker"`. Unexplained gaps ⇒ `status: "sealed-with-gaps"` and a catalog-level alert.
- **Instance shutdown**: the hook's shutdown path emits a final `deja_sink_marker{kind:"eof", last_gseq}` after a real producer flush, so "instance ended" is distinguishable from "tail lost".

### 2.6 Window manifest schema (concrete)

```json
{
  "schema_version": 1,
  "type": "deja-window-manifest",
  "manifest_revision": 1,
  "window_id": "staging/hyperswitch-router/2026-06-12T14:35Z/300",
  "kind": "window",                       // "window" | "session"
  "env": "staging",
  "service": "hyperswitch-router",
  "window": { "start": "2026-06-12T14:35:00Z", "end": "2026-06-12T14:40:00Z", "duration_s": 300 },
  "sealed_at": "2026-06-12T14:51:12Z",
  "grace_s": 600,
  "status": "sealed",                     // sealed | sealed-with-gaps | amended | sealing-delayed
  "masking": { "policy": "none", "policy_version": 0, "applied_at": null },
  "producers": [
    {
      "instance_id": "pod-7f9c4-1718203200",
      "code_sha": "9355950b99",
      "service_version": "2026.04.21.0",
      "deja_version": "0.1.0",
      "gseq_ranges": [[120433, 128327]],
      "events_observed": 7895,
      "duplicates_dropped": 3,
      "gaps": [ { "from": 125001, "to": 125006, "explained_by": "sink_marker", "dropped_count": 6 } ],
      "edge_continuity": { "prev_window_max_gseq": 120432, "continuous_with_prev": true },
      "eof_marker_seen": false
    }
  ],
  "events": {
    "count": 347012,
    "bytes_uncompressed": 1471800000,
    "by_boundary": { "db": 113904, "redis": 80410, "time": 73752, "id": 58659, "http_incoming": 16382, "http_outgoing": 3905 },
    "event_schema_versions": { "1": 347012 },
    "envelope_schema_versions": { "2": 347012 },
    "late_events": 0,
    "uncorrelated": 84
  },
  "correlations": { "count": 16382, "driveable": 16380, "index": "index/correlations.ndjson.zst" },
  "data_files": [
    { "key": "data/part-00000.ndjson.zst", "events": 250000, "bytes": 211384921,
      "sha256": "ab12…", "first_correlation": "0197…", "last_correlation": "0197…" },
    { "key": "data/part-00001.ndjson.zst", "events": 97012, "bytes": 81733202, "sha256": "cd34…",
      "first_correlation": "0197…", "last_correlation": "0197…" }
  ],
  "late_files": [],
  "compactor": { "version": "0.1.0", "input_objects": 412, "input_events": 347027, "duplicates_dropped": 15 },
  "source": { "topic": "deja.recordings.staging", "partitions": 12 }
}
```

### 2.7 Correlation index (one ndjson line per correlation)

```json
{
  "correlation_id": "0197a3b2-…",
  "instance_id": "pod-7f9c4-1718203200",
  "part": "data/part-00000.ndjson.zst",
  "line_start": 104233,
  "line_count": 21,
  "event_count": 21,
  "gseq_min": 120519, "gseq_max": 120539,
  "event_time_ns": 1749738912345678901,
  "boundaries": { "db": 7, "redis": 5, "time": 4, "id": 3, "http_incoming": 1, "http_outgoing": 1 },
  "ingress": { "method": "POST", "path": "/payments", "status": 200 },
  "driveable": true                      // has exactly one usable http_incoming with method/path/baseline response
}
```

`ingress` makes the dashboard's request browser and the replay request-selection filters work off the index alone, without touching data parts. Per-request bundles are **not** materialized as separate objects (16k objects per window ×288 windows/day is an S3 small-object anti-pattern); correlation-clustered parts + line-range index gives the same access pattern with range reads.

### 2.8 Compactor algorithm (sketch)

New Rust binary `deja-compactor` (lives in the deja workspace next to deja-orchestrator; runs on the runner VM under a scheduler in v1, k8s CronJob later).

```
compact(window W):
  1. CLAIM: catalog row upsert (window_id, status='compacting', compactor_id) — Postgres advisory
     lock prevents concurrent compaction of the same window.
  2. SEAL CHECK: now ≥ W.end + grace AND vector consumer-group offsets past W.end + grace
     (else mark 'sealing-delayed' and exit).
  3. LIST: s3 list landing/v1/win=W/** → input objects.
  4. PASS 1 — stream, dedup, sort runs:
       for each object: stream-decode zstd, parse envelope lines
         - reject non-envelope/unparseable lines into quarantine/ (counted in manifest warnings)
         - dedup_key = (instance_id, global_sequence); skip if seen (roaring/hash set; ~350k keys, trivial)
         - collect sink_marker envelopes separately (loss accounting)
         - accumulate per-instance gseq bitmap, boundary counters, schema-version histogram
         - append (sort_key, raw_event_json) to in-memory buffer;
           sort_key = (correlation_id ?? "￿"+instance+gseq, request_sequence, global_sequence)
         - when buffer > 1 GiB: sort, spill as run-file (local disk), continue   # burst safety
  5. PASS 2 — k-way merge of runs:
       merged stream is correlation-clustered in canonical replay order
       → [MASKING HOOK: MaskingEngine::apply(&mut event) — identity in v1, see §5]
       → strip envelope, write SemanticEvent lines to data/part-NNNNN.ndjson.zst (rotate at 250k events)
       → emit one correlations.ndjson line per correlation flush (part, line_start, line_count,
         ingress extracted from the http_incoming event, driveable flag)
  6. COVERAGE: per instance: RLE the gseq bitmap → ranges; diff against [min..max] → gaps;
     match gaps to sink_markers → explained/unexplained; fetch prev window's per-instance max
     from catalog → edge_continuity.
  7. PUBLISH: PUT data parts + index, then PUT manifest.json (revision 1) LAST.
  8. CATALOG: PUT /catalog/windows/{window_id} to the orchestrator (manifest summary + producer rows);
     status sealed | sealed-with-gaps.
  9. (separate sweep) LATE: landing parts for sealed windows → late/part-rN-*, manifest revision N,
     status 'amended', catalog update.
```

Sort-key note: canonical within-correlation order is **`request_sequence` ascending** (call-start order — what the kernel already sorts by, kernel `lib.rs:64-81`) with `global_sequence` tiebreak. This is stronger than today's "file order = completion order" and makes renderer file-order deterministic for `KeyStamper` occurrence stamping.

---

## 3. Envelope evolution: `deja.artifact_record/v2`

Produced by the hardened sink (replacing `HS/crates/router/src/services/kafka/deja_record_sink.rs:36-43`):

```json
{
  "schema_version": 2,
  "artifact_type": "deja_artifact_record",          // or "deja_sink_marker"
  "env": "staging",
  "service": "hyperswitch-router",
  "code": { "sha": "9355950b99", "version": "2026.04.21.0", "deja_version": "0.1.0" },
  "instance_id": "pod-7f9c4-1718203200123",
  "capture": { "mode": "window", "session_id": null },   // dev: { "mode": "session", "session_id": "rec-demo" }
  "correlation_id": "0197a3b2-…",
  "event_time_ns": 1749738912345678901,
  "produce_time_ns": 1749738912349001000,
  "masking": { "policy": "none", "policy_version": 0 },
  "event": { /* full SemanticEvent, unchanged schema v1 */ }
}
```

What's new and why:
- **`code.sha` / `code.version`** — stamped at record time from `router_env::commit!()` / `version!()` (verified present, vergen-backed: `HS/crates/router_env/src/env.rs:93-145`), env-overridable via `DEJA_CODE_SHA`. This is what lets the dashboard answer "what code produced this window" and lets the catalog index windows by deployed sha without guessing from timestamps.
- **`event_time_ns` duplicated at envelope top level** — Vector's remap computes the window key from it without parsing into `.event`; also set as the **Kafka message timestamp** (event-time, not ingest-time). `produce_time_ns` is ingest-time; the delta is the producer-buffering latency signal.
- **`instance_id`** replaces `recording_run_id` as the routing identity in window mode (format: `{hostname|pod}-{boot_unix_ms}`, unique per process incarnation). In session mode (`capture.mode: "session"`), `session_id` carries today's `DEJA_RECORDING_RUN_ID` and drives the `sessions/` keying. `SemanticEvent.recording_run_id` keeps being set to `instance_id` (window) or `session_id` (session) for the graph-join contract (`(recording_run_id, graph_node_id)` scoping).
- **`capture.mode`** — the single switch Vector's remap routes on: `window` → `landing/v1/win=…`, `session` → `landing/v1/session={session_id}/…`.
- **`masking`** — declared at record time as `none/0`; rewritten by the compactor when a masking policy is applied (§5), so a single field answers "is this object safe".
- **New artifact_type `deja_sink_marker`** — loss accounting (`{kind: "dropped", gseq_from, gseq_to, count}` and `{kind: "eof", last_gseq}`), consumed by the compactor, never written to data parts.

Kafka headers v2 (all strings, for header-only tooling): `schema_version`, `dedup_key` (`{instance_id}:{global_sequence}`), `env`, `code_sha`, `boundary`, `method_name`, `event_time_ms`. (`request_sequence`/`recording_run_id` headers retired; they're in the payload.)

Compatibility: the compactor accepts v1 envelopes (maps `recording_run_id`→`instance_id`+`session_id`, `event.timestamp_ns`→`event_time_ns`, code unknown) so the local demo migrates without a flag day.

---

## 4. Replayability contract

### 4.1 What "replay window W" consumes

A replay run's recording input is a **selection**:

```json
{
  "windows": ["staging/hyperswitch-router/2026-06-12T14:35Z/300", "…14:40Z/300"],
  "filter": { "path_prefix": "/payments", "methods": ["POST"], "status_in": [200, 400] },
  "sample": { "kind": "all" } | { "kind": "first_n", "n": 2000 } | { "kind": "random", "n": 2000, "seed": 42 }
}
```

Runner prepare phase (replaces `pull_recording`'s `mc find | sort | mc cat`):
1. GET each manifest; refuse if `status == sealing-delayed`; warn into the run record if `sealed-with-gaps` or `amended`; refuse prod-env windows whose `masking.policy == "none"` (policy gate, §5).
2. Stream `index/correlations.ndjson.zst` per window; apply filter + sample over `driveable` rows → selected correlation set.
3. Partition the selection into **batches** by event volume: target ≤ 50k events ≈ ~210 MB raw per batch (≈ 2,400 requests at the measured ~21 events/request). Batch assignment preserves index order, so each batch's correlations are contiguous in the data parts.
4. Per batch: download the covering `data/part-*.ndjson.zst` (+ merged `late/` parts for amended windows), extract the selected line ranges → `{run}/batches/{k}/events.jsonl` in canonical order.

### 4.2 Per-batch replay loop (generalizes today's single-recording flow)

```
for batch k:
  render_lookup_table(events.jsonl) → lookup-tables/{run}-{k}.ndjson    # true NDJSON now
  point replay router at it (restart or DEJA_LOOKUP_TABLE swap; v1 = router restart per batch)
  flush redis; kernel drives batch k → http-diffs appended (+batch tag)
divergence scoring aggregates all batches → ONE scorecard (schema unchanged; adds summary.batches,
  summary.windows[], and per-window coverage warnings carried from manifests)
```

Required (small) consumer changes, per decision 10(c):
- **Lookup renderer** (`WT/crates/deja-orchestrator/src/lookup/mod.rs`): stream events, emit **NDJSON** one `LookupEntry` per line (today: whole-doc pretty JSON built in memory). `LocalFileLookupSource` already accepts NDJSON (`WT/crates/deja-record/src/replay.rs:953-976`) — no candidate change.
- **Kernel**: already groups by correlation and sorts by `request_sequence`; only gains a `--batch` tag on HttpDiff lines.
- **deja-tui / visualizer / divergence scoring**: already read the harness-root copies (`recordings/{id}/events.jsonl`, lookup/observed/http-diffs) — with the JSONL primary gone they keep working unchanged because the harness root is populated from S3; only `DEJA_ARTIFACT_DIR/semantic-events.jsonl` consumers die, and the lifecycle never read that file directly.

### 4.3 Size math (measured 4,242 B/event, ~21 events/request, ~5 lookup entries/event)

| scale | events/day | raw/day | zstd/day (~6×) | per 5-min window | requests/day |
|---|---|---|---|---|---|
| low | 10⁷ | 42 GB | ~7 GB | 35k ev / 147 MB raw / ~25 MB zstd | ~0.5 M |
| high | 10⁸ | 424 GB | ~71 GB | 347k ev / 1.47 GB raw / ~250 MB zstd | ~4.8 M |

A one-hour replay at 10⁸-scale = 12 windows ≈ 4.2 M events ≈ 17.6 GB raw → ~84 batches; whole-window replay is feasible but slow (kernel is serial), which is why `sample` is first-class: the v1 PR gate's default is `random n=2000` requests per selection, ≈ 1 batch, minutes not hours. Lookup-shard memory in the replay router: 50k events × ~5 entries × ~4 KB ≈ ~1 GB worst-case — the batch cap is chosen to keep this bounded; results are usually far smaller than the 4 KB event envelope.

### 4.4 Fidelity caveat (recorded, not solved, in v1)

Today's replay assumes pre-record empty Redis (`flush_redis`) because the demo records from cold boot. A mid-stream production window violates that for any live-served state. Side-effect calls are substituted via lookup (demo: all 197 at rank 2), so the main risk is router-internal caches warmed differently. v1 posture: scorecard already classifies these as divergences; the dashboard surfaces a "mid-stream window" notice. State snapshotting is Later.

---

## 5. Masking hook placement

**The seam is the compactor, step 5 (PASS 2), between dedup/sort and data-file write** — the one place every event flows through exactly once, after which objects are immutable.

```rust
trait MaskingEngine {
    fn policy(&self) -> MaskingPolicyRef;                  // {policy: "none", version: 0} in v1
    fn apply(&self, event: &mut SemanticEvent) -> Result<MaskOutcome, MaskError>;
}
```

- v1 ships `IdentityMasking` (`none/0`); the manifest and every envelope record the applied policy, so "is this window masked, by what" is a single field check — no layout change when real masking lands (field-level redaction over `request/args/response/result/receiver` JSON values).
- **Staging-only posture enforcement**, layered: (a) the Superposition record flag defaults false in prod and the rollout runbook keeps it so; (b) the prod bucket policy denies `landing/*` and `windows/*` writes until masking ships; (c) the replay runner refuses any window with `masking.policy == "none"` from a prod-env bucket (defense in depth — staging windows replay fine unmasked).
- When masking lands: compactor runs policy vN at compaction; **re-masking** of already-sealed windows (policy upgrade) = recompact landing if still in TTL, else a `windows/` → `windows/` rewrite job bumping `manifest_revision` with `masking.policy_version: N`. Landing's short TTL (§6) bounds how long raw data exists even in staging.

---

## 6. Retention / lifecycle + the Postgres recording catalog

### 6.1 S3 lifecycle rules (per prefix class, per env bucket)

| prefix | rule |
|---|---|
| `landing/v1/` | expire **7 days**; abort incomplete multipart 1 day. Exists only for compaction + late sweep + re-masking headroom. |
| `windows/v1/*/data/`, `index/`, `late/` | transition to IA at 14 days; expire **30 days** staging / **90 days** prod (config per env). 10⁸-scale: ~71 GB/day zstd → ≈ 2.1 TB steady-state at 30 days. |
| `windows/v1/**/manifest.json` | keep **365 days** (tiny; the catalog's history and loss-accounting record outlives the data). Manifests are excluded from the data expiry rule by suffix filter — or equivalently the catalog row is the survivor; keep both. |
| `sessions/v1/` (dev/MinIO) | expire 14 days (MinIO ILM), plus the harness's own cleanup. |

Expired-data windows stay listed in the catalog as `expired` (replayable=false) so scorecard history keeps resolving.

### 6.2 Recording catalog (Postgres — the orchestrator schema slice this design owns)

```sql
CREATE TABLE recording_windows (
  window_id        text PRIMARY KEY,            -- "staging/hyperswitch-router/2026-06-12T14:35Z/300"
  kind             text NOT NULL,               -- 'window' | 'session'
  env              text NOT NULL,
  service          text NOT NULL,
  starts_at        timestamptz NOT NULL,
  ends_at          timestamptz NOT NULL,
  status           text NOT NULL,               -- compacting|sealing-delayed|sealed|sealed-with-gaps|amended|expired
  manifest_key     text NOT NULL,
  manifest_revision int NOT NULL DEFAULT 1,
  masking_policy   text NOT NULL DEFAULT 'none',
  masking_version  int  NOT NULL DEFAULT 0,
  event_count      bigint NOT NULL,
  correlation_count bigint NOT NULL,
  driveable_count  bigint NOT NULL,
  bytes_compressed bigint NOT NULL,
  code_shas        text[] NOT NULL,             -- distinct producer shas (deploy-spanning windows have 2+)
  sealed_at        timestamptz,
  created_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON recording_windows (env, service, starts_at DESC);
CREATE INDEX ON recording_windows USING gin (code_shas);

CREATE TABLE recording_window_producers (
  window_id        text REFERENCES recording_windows ON DELETE CASCADE,
  instance_id      text NOT NULL,
  code_sha         text NOT NULL,
  service_version  text,
  gseq_min         bigint, gseq_max bigint,
  events_observed  bigint NOT NULL,
  duplicates_dropped bigint NOT NULL DEFAULT 0,
  gaps             jsonb NOT NULL DEFAULT '[]', -- [{from,to,explained_by,dropped_count}]
  continuous_with_prev boolean,
  eof_marker_seen  boolean NOT NULL DEFAULT false,
  PRIMARY KEY (window_id, instance_id)
);
```

Population: compactor → `PUT /catalog/windows/{window_id}` on deja-orchestrator (idempotent upsert keyed on `(window_id, manifest_revision)`, **audit-logged** like every mutation per decision 8 — actor = compactor service identity). A reconciliation job (`GET landing+windows listings` vs catalog) heals drift; **S3 manifests remain the source of truth**, Postgres is the queryable cache the dashboard lists/filters (by time range, code sha, status, gap-freeness, masking) — exactly the "recording catalog" of decision 9.

---

## 7. Local-dev parity

Same bucket name (`deja-recordings` on MinIO), same machinery, different namespace:

- Dev envelopes carry `capture: {mode: "session", session_id: $DEJA_RECORDING_RUN_ID}`; the same Vector config routes them to `landing/v1/session={session_id}/inst=…/part-*.ndjson.zst` (one extra remap branch on `capture.mode`).
- The harness record lifecycle replaces stages 5–6 (`wait_minio_objects` + `pull_recording`, `WT/crates/deja-orchestrator/src/lifecycle/mod.rs:580-667`) with: **invoke `deja-compactor --mode session --session {id}`** (sealed by the `eof` sink marker emitted on router shutdown/flush, or by quiesce timeout) → output `sessions/v1/{session_id}/{manifest.json,data/,index/}` — the identical canonical shape with `kind: "session"` → pull = GET manifest, GET data parts, concatenate (already canonical order) into `{root}/recordings/{id}/events.jsonl`.
- What this buys: the demo exercises the **production** path end-to-end on every run — envelope v2, hardened sink, landing layout, compactor dedup/sort/coverage, manifest sealing — instead of the bespoke `mc find | sort | mc cat` concatenation. The completeness machinery gets continuous local validation (the demo's 207-event ground truth becomes a compactor regression fixture: expect `gseq 0–206 complete, duplicates_dropped: 0`).
- Discrete sessions and windows coexist by prefix (`sessions/` vs `windows/`); a dev MinIO never has `windows/`, production never has `sessions/`. The catalog's `kind` column keeps one listing API for both. Migration shim: `pull_recording`'s old path stays behind a flag for one release to read pre-manifest artifacts.

---

## 8. Sink hardening (decision 10a — the contract this store depends on)

The store's loss accounting only works if the producer side keeps its promises. Changes to `HyperswitchKafkaRecordSink` + composition (`HS/crates/router/src/deja_boot.rs:123-135`):

- **Sole primary**: `CompositeSink(JsonlSink) + secondary(Kafka)` → `HardenedKafkaSink` as the only sink. The "primary failure permanently disables the writer" rule (`WT/crates/deja-record/src/writer.rs:374-377`) is replaced: transient produce errors never disable; only fatal producer errors (auth, unknown topic) do, loudly.
- **Producer config** (today literally only `bootstrap.servers`, `kafka.rs:323-324`): `enable.idempotence=true`, `acks=all`, `message.timeout.ms=120000`, bounded `queue.buffering.max.messages` / `.kbytes`, `compression.type=zstd`.
- **Awaited delivery**: custom `ProducerContext::delivery()` callback tallies acks/failures per gseq (ThreadedProducer keeps the no-tokio writer thread model); `flush()` becomes a real `producer.flush(timeout)` (today a no-op, `deja_record_sink.rs:111-113`); graceful shutdown flushes then emits the `eof` marker.
- **Explicit policy** `DEJA_SINK_POLICY = block | drop` when the rdkafka queue is full after the writer's bounded blocking-send backpressure: `block` (recording fidelity over latency — acceptable in staging), `drop` (fail-open: count, remember the gseq range, emit `deja_sink_marker{kind:"dropped"}` when the broker recovers). Default: staging `block`, prod `drop`. Either way losses are **accounted**, and §2.5 turns the account into manifest truth.

---

## 9. v1 component/work breakdown

1. **Envelope v2 + hardened sink** (HS `deja_record_sink.rs`, `deja_boot.rs`, `kafka.rs` config; WT `writer.rs` disable-semantics change; sink markers).
2. **Vector config v2**: keep envelope, remap computes `win`/routes on `capture.mode`, zstd, e2e acknowledgements, landing key template.
3. **`deja-compactor`** (new WT crate): window + session modes, dedup, external sort, masking hook (identity), manifests, index, late sweep, catalog PUT.
4. **Catalog tables + `PUT /catalog/windows` + `GET /catalog/windows`** in deja-orchestrator (audit-logged).
5. **Replay prep rewrite**: selection → batches → events.jsonl materialization; renderer → streaming NDJSON shards; per-batch loop + aggregated scorecard.
6. **Demo lifecycle swap** to session-mode compactor; JSONL sink deletion.
7. **S3 lifecycle policies** (per env) + MinIO ILM for dev.
---

# Adversarial review

BLOCKING:
- §2.5's foundational claim — 'global_sequence over recorded events remains gap-free at the source; every gap downstream is loss or a window-boundary straddle' — is false under future cancellation. Verified in code: gseq is allocated at call START (EventBuilder::start_with_receiver_and_correlation_id, {WT}/crates/deja-record/src/lib.rs:904) but the event is only emitted at call FINISH (record_boundary_async, lib.rs:1206-1209: start → future.await → finish). Any dropped in-flight boundary future (actix client disconnect, tokio::time::timeout/select! cancellation, task abort at shutdown — patterns Hyperswitch demonstrably uses, see docs/hyperswitch-cascade/07_FIRE_AND_FORGET_ASYNC_PATTERNS.md) permanently consumes a gseq with no event and no deja_sink_marker. At 10^7-10^8 events/day even a tiny cancellation rate yields thousands of unexplained gaps per day, so essentially every window seals 'sealed-with-gaps' with alerts — the loss-detection mechanism of record (fixed decision 10b) is chronically false-positive as designed and therefore untrustworthy. Fix within the architecture: assign gseq at emit time (in RecordingHook::record()/writer enqueue — single point, still monotone per instance, gap-free by construction; within-correlation replay order already comes from request_sequence, which tolerates gaps), or add a Drop guard emitting cancellation markers; either way §2.5's 'assigned at call start' edge-continuity reasoning and the kernel-sort tiebreak note must be reworked to match.

CORRECTIONS:
- Decision 10(c) verification miss — deja-tui does NOT 'already read the harness-root copies'. Verified: deja-tui discovers semantic-events.jsonl at {state}/, {state}/semantic/, or {state}/recording/ only (crates/deja-tui/src/lib.rs:12, 267-271), and real run dirs show it reads recording/semantic-events.jsonl — the JSONL sink's bind-mounted DEJA_ARTIFACT_DIR — not recordings/{id}/events.jsonl. Deleting the JSONL sink breaks deja-tui as designed. Trivial fix: session-mode lifecycle materializes the S3-pulled copy at recording/semantic-events.jsonl, or tui discovery learns the recordings/{id}/events.jsonl path. (Divergence scoring, the visualizer, and the lookup renderer were verified correct: divergence/mod.rs:462-463 reads lookup-table/observed/http-diffs only, and the recording reaches it through render_lookup_table.)
- S3 lifecycle rules cannot filter by suffix (only prefix, object tags, object size), and prefix filters take no wildcards — so 'manifests excluded from the data expiry rule by suffix filter' and rules targeting 'windows/v1/*/data/' are not implementable as written. Fix: tag data/index/late objects at PUT and use tag-based lifecycle filters, or move manifests to their own literal prefix (e.g. manifests/v1/...). The design's hedge (catalog row as survivor) stands but the stated mechanism is wrong.
- Vector end-to-end acknowledgements are configured on SINKS (or globally) — source-level acknowledgements are deprecated. The no-loss guarantee needs acknowledgements.enabled: true on the aws_s3 sink (the kafka source then defers offset commits automatically), not 'on the kafka source' as written.
- Producer-hardening location has blast radius: deja_boot creates its own KafkaProducer instance (deja_boot.rs:71) but via the SHARED KafkaProducer::create (kafka.rs:320-346) that also builds Hyperswitch's analytics producer. Applying enable.idempotence/acks=all/bounded-queue/zstd inside that shared constructor changes analytics delivery semantics too; and the proposed custom ProducerContext::delivery callback requires a dedicated constructor anyway. The hardened sink needs its own deja-specific producer construction path.
- Late-event sweep bookkeeping is underspecified: the manifest records only compactor.input_objects as a COUNT, so the sweeper cannot distinguish new landing parts for a sealed window from already-compacted ones. Persist the consumed-object key list (manifest, sidecar, or catalog table) or move processed landing objects to a processed/ prefix.
- Record-mode correlation_id risk is overstated: IdReuse::UseIncoming is forced only when DEJA_MODE=replay (HS router/src/lib.rs:435-443); outside replay the configured id_reuse_strategy applies and its default is IgnoreIncoming (settings.rs:945-952), i.e. production recording generates fresh request ids unless a deployment opts into UseIncoming. The 'flag correlations with multiple http_incoming as non-driveable' mitigation is still worth keeping as config-dependent defense.
- §2.2 is internally inconsistent with §2.8: Kafka produce order is AsyncRecordWriter order = call-COMPLETION order, while §2.8 correctly defines canonical replay order as request_sequence (call-start). 'Exactly the within-correlation order replay needs' is wrong; per-correlation Kafka ordering is not load-bearing at all since the compactor re-sorts — the rationale should be downgraded to 'keeps one correlation on one partition for locality'.
- With the unwrap remap removed, Vector's kafka source (legacy log namespace) injects its own top-level fields (timestamp, offset, partition, topic, headers, message_key) into every landing line alongside the envelope — today the `. = .event` remap discards them. The compactor's envelope parser must ignore unknown top-level fields (serde default behavior — fine) and/or the remap should delete them to avoid bloating landing objects.
- Session-mode sealing via the eof sink marker will rarely fire in the demo: the record lifecycle never stops the recording router before pulling (stage 5 waits for MinIO object-count stability while the router stays up, lifecycle/mod.rs:207-222), so at compaction time no shutdown/eof has occurred and every session seal falls to the quiesce timeout. Either the session lifecycle gains an explicit router stop/flush step before invoking the compactor, or quiesce-timeout should be documented as the primary session seal.
- The cited 'ground truth §8.5' for cross-correlation order does not resolve: docs/DEJA_RECORDING_ARCHITECTURE.md §8 is 'Configuration & deployment' (8.1-8.3, no 8.5). The underlying claim is nonetheless supported by code (kernel groups by correlation into a BTreeMap and sorts only within correlation), so only the citation needs fixing.
- window_id values embed '/' ('staging/hyperswitch-router/2026-06-12T14:35Z/300') and are used both as Postgres PK and as a path segment in PUT /catalog/windows/{window_id} — needs URL encoding or a flat id format.

NOTES:
Adversarial review performed read-only against WT={WT} and HS=WT/vendor/hyperswitch-deja-clean (branch deja-lean confirmed; the manifest example's code_sha 9355950b99 is a real commit on it). The design's repo citations are unusually accurate — every file:line spot-check matched: envelope v1 + best-effort semantics + no-op flush (HS deja_record_sink.rs:36-43/104-113), composite boot (deja_boot.rs:123-135), bare ClientConfig (kafka.rs:324), writer permanent-disable (writer.rs:374-377; note queue-Full also disables at :341-346, which the proposed 'transient errors never disable' change must cover too), NDJSON-tolerant LocalFileLookupSource (replay.rs:953-976, recording_id unvalidated so empty is safe), kernel request_sequence sort (deja-kernel lib.rs:64-81), lifecycle stages 5-6 (lifecycle/mod.rs:219-222 calling :580-667), vergen commit!/version! (env.rs:93-145), de_u64_opt_lenient (deja-record lib.rs:181-201). Vector claims about TODAY's config all verified (config/vector.deja.yaml: unwrap remap, event-field key_prefix templating, compression none, batch 2000/5s, no acks; image timberio/vector:latest-debian in docker-compose.yml:504). Scale-math inputs reproduce from a representative demo run: 207 events, ~4.23 KB/event, 10 requests = 20.7 ev/req, 985 lookup entries = 4.76/event in a whole-doc pretty-JSON table, and 'all 197 side-effects at rank 2' confirmed in multiple clean scorecards (divergent runs show 184). Derived numbers (35k/347k events per 5-min window, 147MB/1.47GB raw, 12-partition throughput, 50k-event batches ≈ 2,400 requests, ~1GB lookup-shard bound, 2.1TB steady state) are arithmetically consistent. The sampling gate CAN be made gap-free as claimed since hook gating already precedes gseq allocation (start_boundary_event_lazy lib.rs:1353-1356) — but the one BLOCKING issue stands: gseq-at-call-start + emit-at-finish means cancelled futures silently consume sequence numbers, so the §2.5 loss-detection mechanism produces chronic false unexplained gaps at production rates; it needs emit-time gseq assignment or cancellation accounting. Everything else found is adjustment-level (see corrections), the largest being the falsified 10(c) claim about deja-tui and the unimplementable suffix-based S3 lifecycle split. Not verifiable in this session (no docker, demo validation running): aws_s3 zstd + e2e-ack behavior on a pinned Vector version — the design already carries this as an open question and the risk list correctly anticipates the >2^53 u64 mangling precedent. No contradictions with the ten fixed user decisions were found; decision 5's deja-crate packaging is absent but appears intentionally out of this design's (s3-store) scope.