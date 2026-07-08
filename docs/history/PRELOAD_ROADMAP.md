> **Archived.** This document records the syscall-preload track roadmap. It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Déjà — Consolidated Roadmap

Everything that needs to be built, in dependency order, with concrete deliverables.

---

## Layer 0: Transport Fidelity — Prove the bytes are correct

*If the bytes are wrong, nothing else matters.*

### 0.1 Stable logical `connection_id` ✅

**Problem:** Events are tied to fd numbers, which are recycled. No way to say "this is connection #3 to Redis" across the whole artifact.

**Deliverable:**
- `connection_id: u64` assigned monotonically per `connect()` / `accept()` call
- Stored in `FdTracker` and propagated to every `SocketBoundaryEvent`
- Survives fd reuse (close + reopen gets a new connection_id)

**Files:** `fd_tracker.rs`, `agent.rs`, `deja-core/src/lib.rs`

### 0.2 Per-connection stream offsets ✅ + stream hashes

**Problem:** No way to prove bytes are complete, contiguous, and uncorrupted within a connection.

**Deliverable:**
- Each `SocketBoundaryEvent` gets `stream_offset: u64` (cumulative byte count per connection+direction)
- On connection close (or artifact flush), compute `stream_sha256` for each connection+direction
- Store in a new `ConnectionSummary` section in the artifact
- Validation command: `deja verify --artifact <PATH>` that re-computes and checks hashes

**Files:** `agent.rs`, `deja-core/src/lib.rs`, new `deja-cli` verify subcommand

### 0.3 Outbound replay byte-compare ✅

**Problem:** During replay, `before_send()` just advances a cursor. If the app sends different bytes than what was recorded, we don't notice.

**Deliverable:**
- `before_send()` in replay mode does `recorded_bytes == app_bytes` check
- Mismatch → emit a `ReplayDivergence` event + eprintln warning
- On close, report: "3/47 sends diverged from recording"

**Files:** `agent.rs`

### 0.4 Explicit supported ✅/unsupported surface contract

**Problem:** Users don't know what we capture. Silent blind spots.

**Deliverable:**
- Document in `ARCHITECTURE.md`:
  - **Supported v1:** `AF_INET/AF_INET6`, `SOCK_STREAM`, plaintext only
  - **Not supported:** TLS, UDP, Unix domain, `sendfile`/`splice`, `sendmmsg`/`recvmmsg`, ancillary data
- Unsupported syscall paths in hooks emit a one-time warning to stderr
- `deja inspect` reports coverage warnings

**Files:** `hooks.rs`, `ARCHITECTURE.md`, `deja-cli/src/main.rs`

### 0.5 Artifact integrity / corruption detection ✅

**Problem:** Append-only JSONL can be truncated (crash, disk full). No way to detect.

**Deliverable:**
- Finalize artifact with a `manifest.json` containing:
  - `total_events`, `total_bytes`, `connection_summaries[]` with hashes
  - `manifest_sha256` over the whole manifest
- `deja verify` checks manifest against actual file
- If manifest is missing (crash during finalize), mark artifact as `incomplete` but still readable

**Files:** `deja-core/src/lib.rs`, `deja-cli/src/main.rs`

### 0.6 Ground-truth validation tests

**Problem:** No independent proof that captured bytes match reality.

**Deliverable:**
- `deja verify --pcap` — Phase 1: byte count comparison via `tshark -z conv,tcp`
- `deja verify --pcap` — Phase 2: stream hash comparison via `tshark -z follow,tcp,raw` + SHA-256
- A9 Ground-Truth Fidelity metric in HS-41 scorecard
- Integration test: echo server + echo client, compare captured bytes against what the client sent
- Fragmentation test: force partial reads/writes, verify reassembly
- Concurrency test: N connections to same host:port, verify no cross-stream mixing
- Negative test: inject missing/extra/corrupted chunks, verify detection

**Status:** ✅ `deja verify --pcap` implemented (Phase 1 + Phase 2 + A9 scorecard)

**Files:** `crates/deja-cli/src/main.rs` (verify_pcap), `demo/pipeline.sh` (Phase 6)

---

## Layer 1: Protocol Parsers — Replace hand-rolled with tested crates

*Raw bytes are the truth. Parsers are a lossy projection for human consumption.*

### 1.1 Replace ✅ `decode_redis()` with `redis-protocol` crate

**Problem:** Hand-rolled RESP parsing breaks on inline commands, nested arrays, RESP3, pushed replies, HGETALL maps.

**Deliverable:**
- Add `redis-protocol = "6"` to `deja-cli/Cargo.toml`
- `decode_redis(data: &[u8]) → Vec<RedisFrame>` using `redis_protocol::resp2::decode::decode`
- Graceful fallback: if decode fails, show raw hex preview (current behavior)
- Same for `deja-compare` — use structured frames for semantic diffing

**Files:** `deja-cli/Cargo.toml`, `deja-cli/src/main.rs`, `deja-compare/src/lib.rs`

### 1.2 Replace ✅ `decode_http()` with `httparse` crate

**Problem:** Current parser only shows the first line. No headers, no body separation.

**Deliverable:**
- Add `httparse = "1"` to `deja-cli/Cargo.toml`
- Parse full request/response: method, path, headers, body
- Display: `POST /user/signin [3 headers, 48 bytes body]`
- Semantic diff: compare status code, headers, body separately

**Files:** `deja-cli/Cargo.toml`, `deja-cli/src/main.rs`, `deja-compare/src/lib.rs`

### 1.3 Replace `decode_pg()` with `pgwire` or keep improved manual

**Problem:** Current PG parser handles ~15 message types. Real PG has ~40+. Multi-message buffers not handled.

**Deliverable:**
- Evaluate `pgwire` crate for offline parsing (may need codec, not just `decode(&[u8])`)
- If `pgwire` requires async/streaming, write a focused `pg_wire_parser` module (~300 lines) that handles all frontend+backend message types from `&[u8]`
- Must handle multi-message buffers (current parser assumes one message per chunk)

**Files:** `deja-cli/Cargo.toml`, `deja-cli/src/main.rs`

### 1.4 HTTP ✅/2 frame-level decoder

**Problem:** No gRPC/HTTP/2 awareness at all.

**Deliverable:**
- Parse the 9-byte HTTP/2 frame header from raw bytes
- Extract: frame type (DATA/HEADERS/SETTINGS/...), stream ID, flags, payload length
- Detect HTTP/2 connection preface (`PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n`)
- Protocol hint upgrades from "HTTP" → "HTTP2" / "GRPC" when preface is detected
- Display: `HEADERS stream=5 len=87` / `DATA stream=5 len=234`

**Files:** `deja-cli/src/main.rs` (new `decode_http2` function), `protocol_detect.rs`

### 1.5 HPACK ✅ stateful decoder

**Problem:** Can't see gRPC method names without decoding HTTP/2 headers.

**Deliverable:**
- Use `hpack` or `httlib-hpack` crate for HPACK decoding
- Must process frames in connection-order to maintain dynamic table state
- Extract `:method`, `:path`, `:status`, `content-type` headers
- Display: `HEADERS stream=5 → POST /payment.Service/Confirm`
- Feed into protocol hint: if `content-type: application/grpc` → label as GRPC

**Files:** `deja-cli/Cargo.toml`, new `deja-cli/src/hpack_decoder.rs`

### 1.6 Schemaless ✅ Protobuf wire parser

**Problem:** gRPC payloads are Protobuf. Without parsing, regression diffs are opaque byte offsets.

**Deliverable:**
- Parse Protobuf wire format from `&[u8]` without `.proto` schema:
  - varint fields → field_number + value
  - length-delimited fields → field_number + sub-message or string or bytes
  - fixed32/fixed64 fields
- Recurse into nested sub-messages (up to depth N)
- Display: `field 2 (varint): 42 → 43` / `field 3.1 (string): "USD" → "EUR"`
- This is what `protoc --decode_raw` does — ~200 lines of Rust

**Files:** new `deja-cli/src/protobuf_raw.rs`

### 1.7 Schema-based ✅ Protobuf parser (with `.proto` files)

**Problem:** Schemaless parser shows field numbers, not names. "field 2 changed" vs "status changed".

**Deliverable:**
- If user provides `--proto-path <DIR>` or `--proto-descriptor <FILE>`:
  - Use `prost` + `prost-types` to compile `.proto` at artifact-load time
  - Decode gRPC payloads with full field names
  - Display: `status: CONFIRMED → DECLINED` / `currency: "USD" → "EUR"`
- If no schema provided → fall back to schemaless parser (1.6)
- Support gRPC server reflection as a future option (query the server for its proto)

**Files:** new `deja-cli/src/proto_schema.rs`, `deja-cli/Cargo.toml` (add `prost`)

---

## Layer 2: Replay Correctness — Close the loop

*Recording is done. Replay works at a basic level. Now make it correct.*

### 2.1 Tighten vectored-I/O replay fidelity ✅

**Problem:** `recvmsg` replay writes into only the first iovec. Multi-buffer receives can be truncated/corrupted.

**Deliverable:**
- Scatter recorded bytes across the iovec array to match the original layout
- If original iovec layout is unknown (we don't record it), fill sequentially

**Files:** `agent.rs`

### 2.2 Stronger ✅ connection identity under concurrency

**Problem:** Replay queues matched by peer address. Multiple concurrent connections to same host:port can be misbound.

**Deliverable:**
- Use `connection_id` (from 0.1) as the primary replay match key
- Fallback: peer_address + temporal ordering
- Warn on ambiguous matches

**Files:** `agent.rs`

### 2.3 End-of-run ✅ replay validation

**Problem:** No check for leftover recorded bytes or unexpected app bytes after replay completes.

**Deliverable:**
- On process exit / SIGTERM: report per-connection stats
  - `connection #5 (Redis 192.168.1.3:6379): 12/12 events consumed, 0 leftover`
  - `connection #7 (PG 192.168.1.2:5432): 8/10 events consumed, 2 leftover WARNING`
- Emit a `ReplaySummary` event to the artifact

**Files:** `agent.rs`, `deja-core/src/lib.rs`

### 2.4 Dependency operation identity for mutable reads

**Problem:** Signature-only replay is unsafe for mutable Redis/DB reads. In Hyperswitch, the same `HGET key field` or SQL `SELECT ... WHERE id = $1` can return different values before and after a write. Collision-free signatures do not solve temporal/state ambiguity.

**Evidence:** `docs/HYPERSWITCH_REPLAY_AMBIGUITY_REPORT.md` found at least 56 concrete Redis read sites that can become replay-ambiguous, plus conservative SQL counts of 193 read/query sites and 67 write sites in storage/router DB code.

**Deliverable:**
- Extend dependency events with replay identity fields:
  - `global_index`
  - `connection_id`
  - `protocol`
  - `operation_signature`
  - `resource_key` when derivable
  - `causal_scope_id` when available
  - `local_sequence_in_scope`
  - `resource_version_or_cursor` when derivable
- During record, assign monotonically increasing operation ordinals:
  - global ordinal
  - per-connection ordinal
  - per-causal-scope ordinal
  - per-signature ordinal
- During replay, use the strongest available match:
  1. stateful emulator result
  2. `causal_scope_id + local_sequence_in_scope + operation_signature`
  3. `resource_key + resource_version_or_cursor`
  4. connection/global sequence
  5. per-signature FIFO queue
  6. signature-only fallback with warning

**Files:** `deja-core/src/lib.rs`, `agent.rs`, `deja-cli/src/main.rs`, protocol parser modules

### 2.5 Ambiguity detector and replay safety warnings

**Problem:** Users need to know when a recording cannot be safely replayed by signature alone. Otherwise replay can silently return the wrong historical response for repeated mutable reads.

**Deliverable:**
- Add `deja inspect --ambiguity <artifact>` or integrate into `deja verify`.
- Detect repeated dependency signatures with divergent responses:
  - same Redis `GET`/`HGET`/`HGETALL`/`SCAN` signature with multiple response payloads
  - same SQL query signature + bind values with multiple row/result payloads
- Classify signatures:
  - `unique_signature_safe`
  - `repeated_identical_response_safe`
  - `ambiguous_mutable_read`
  - `requires_causal_scope`
  - `requires_state_emulation`
- Emit actionable diagnostics:
  - signature
  - response variants
  - event indexes
  - request/correlation IDs if available
  - recommended disambiguator

**Files:** `deja-cli/src/main.rs`, `deja-core/src/lib.rs`, `deja-compare/src/lib.rs`

### 2.6 Per-request state fixture inference for owned dependencies

**Problem:** Causal lookup and signature queues still replay DB/Redis as mocked response streams. For isolated request replay, a stronger model is to reconstruct the request's pre-state, seed isolated Redis/Postgres, and let candidate code hit the real dependency. This removes DB/Redis read-response lookup ambiguity for owned mutable state.

**Reference:** `docs/STATE_SEEDED_REPLAY.md`

**Deliverable:**
- Add `deja inspect --state-fixture <artifact>`.
- For each inbound request/correlation scope, emit:
  - dependency operations sorted by `local_sequence_in_scope`
  - pre-write read facts
  - post-write observations
  - ordered write/delete log
  - negative facts such as Redis key absence
  - fixture confidence score
  - unsupported/underdetermined operations
- Derive initial-state facts only from reads that occur before the first in-request write to the same resource.
- Explicitly avoid seeding reads produced by in-request writes.
- Output both human-readable and JSON fixture plans.

**Files:** `deja-cli/src/main.rs`, `deja-core/src/lib.rs`, protocol parser modules, new `deja-fixture` crate if this grows large.

### 2.7 Redis state-seeded replay mode

**Problem:** Redis GET/HGET/HGETALL/MGET reads are common, mutable, and ambiguous under response lookup. They are also tractable to seed into a real isolated Redis instance.

**Deliverable:**
- Support fixture inference and seeding for core Redis state:
  - strings: `GET`, `MGET`, `SET`, `DEL`, `EXISTS`
  - hashes: `HGET`, `HMGET`, `HGETALL`, `HSET`, `HDEL`
  - negative facts: missing key / missing hash field
  - optional TTL/expiry facts when enough information exists
- Add replay orchestration:
  - create or connect to isolated Redis
  - flush/prefix namespace safely
  - apply fixture facts
  - run request replay
  - capture candidate Redis writes
  - compare ordered write log and final touched-state slice
- Strictness modes:
  - `minimal`: seed only observed facts
  - `strict`: enforce exact key/hash contents when a complete read (`HGETALL`, `SCAN`, etc.) proves completeness

**Files:** new `deja-fixture/src/redis.rs`, `deja-cli/src/main.rs`, `deja-core/src/lib.rs`

### 2.8 SQL state-seeded replay mode, confidence-tiered

**Problem:** SQL read results often underdetermine the database state that produced them. Déjà should seed high-confidence row facts first and classify the rest instead of pretending all SQL can be inverted perfectly.

**Deliverable:**
- Capture or import schema metadata for Postgres tables touched by the recording.
- Infer fixtures by confidence tier:
  - `exact_row_by_primary_key`: full-row `SELECT * FROM table WHERE id = $1`
  - `exact_returned_rows`: row-returning filters with enough columns to insert
  - `partial_projection`: insufficient columns; requires schema/app hints
  - `join_underdetermined`: join result cannot reconstruct all base rows safely
  - `aggregate_underdetermined`: `COUNT`, `SUM`, `EXISTS`, `LIMIT`, etc.
  - `unsupported_side_effecting_query`: stored procedures, locks, complex transactions
- Seed exact rows into an isolated Postgres database/schema.
- Validate candidate writes and final touched-state slices.
- Produce actionable warnings rather than false confidence for underdetermined queries.

**Files:** new `deja-fixture/src/postgres.rs`, `deja-cli/src/main.rs`, `deja-core/src/lib.rs`, PG parser modules

### 2.9 Hybrid replay orchestrator: mock externals, seed owned state

**Problem:** The correct replay strategy differs by dependency type. Redis/Postgres owned state should be seeded and executed for real; external APIs should usually remain mocked; time/random/env should remain deterministic hooks.

**Deliverable:**
- Add a hybrid mode to `deja regress-live`:

```bash
deja regress-live \
  --recording ./artifact \
  --target http://localhost:8080 \
  --owned-dep redis=redis://127.0.0.1:6379 \
  --owned-dep postgres=postgres://127.0.0.1/deja_replay \
  --mock-external http,grpc
```

- Execution plan:
  1. derive per-request fixtures
  2. seed isolated DB/Redis
  3. replay inbound request
  4. mock external APIs from recording
  5. capture candidate response and dependency side effects
  6. compare HTTP response, external calls, ordered write log, and final state
- The report must clearly distinguish:
  - response regressions
  - external call regressions
  - DB/Redis write-order regressions
  - DB/Redis final-state regressions
  - fixture confidence warnings

**Files:** `deja-cli/src/main.rs`, `deja-compare/src/lib.rs`, `deja-core/src/lib.rs`, new `deja-fixture` crate if needed

---

## Layer 3: Regression Report — The developer experience

*What you see when something breaks.*

### 3.1 Unified ✅ regression report format

**Problem:** `deja regress` and `deja replay-traffic` have separate output formats. No unified view.

**Deliverable:**
- Single report format that shows per-protocol, per-connection, per-request diffs:

```
REGRESSION REPORT
═════════════════

  Redis  192.168.107.3:6379
  ────────────────────────────────
  ✓ HGET hyperswitch:configs:merchant_abc   (identical)
  ✓ SET hyperswitch:locks:payment_pm_123    (identical)

  PostgreSQL  192.168.107.2:5432
  ────────────────────────────────
  ✓ Query: SELECT "users".* FROM "users" WHERE email = $1   (identical)
  ✖ Query: INSERT INTO "payments" ...                        CHANGED
    Row 3, col "status": "confirmed" → "declined"

  gRPC  192.168.1.5:443
  ────────────────────────────────
  ✓ /payment.FraudService/Check                    (identical)
  ✖ /payment.PaymentService/Confirm                 CHANGED
    field 2 (varint):   42 → 43
    field 3.1 (string): "USD" → "EUR"
    field 6 (varint):   ABSENT → 1
  ─── or with .proto schema: ───
    status:    CONFIRMED → DECLINED
    currency:  "USD" → "EUR"
    retry_count: ABSENT → 1

  HTTP/1.1  127.0.0.1:8080
  ────────────────────────────────
  ✓ GET /health                     200 → 200
  ✖ POST /user/signin               500 → 401
    body: {"error":"internal"} → {"error":"unauthorized"}

  ────────────────────────────────
  5 unchanged  │  3 regressed  │  0 new
```

**Files:** `deja-compare/src/lib.rs`, `deja-cli/src/main.rs`

### 3.2 `deja regress-live` — single-command record+replay+compare

**Problem:** Currently 3 separate commands. Developer should run one thing.

**Deliverable:**
```
deja regress-live \
  --recording ./recording \
  --target http://localhost:8080 \
  [--proto-path ./protos]
```
- Starts server in replay mode
- Sends recorded inbound requests
- Captures responses
- Compares and outputs the unified report
- Exit code: 0 = clean, 5 = regressions found

**Files:** `deja-cli/src/main.rs`

### 3.3 Machine-readable output (JSON/SARIF) ✅

**Problem:** Only human-readable terminal output. CI pipelines need structured data.

**Deliverable:**
- `--format json` flag on regress/regress-live
- Output structured regression report as JSON
- Optional: SARIF format for GitHub Security tab integration

**Files:** `deja-cli/src/main.rs`, `deja-compare/src/lib.rs`

---

## Layer 5: Production Readiness — Hyperswitch Integration Metrics

*Source: [HS-41] Define success metrics for Déjà testing against juspay/hyperswitch*

*These are the metrics the board needs before Déjà can be deployed alongside Hyperswitch in any real environment.*

### 5.1 Latency impact — recording overhead must be bounded

**Problem:** Every intercepted syscall adds overhead. The board needs to know Hyperswitch API latency doesn't degrade meaningfully under recording.

**Deliverable:**
- Benchmark harness: run Hyperswitch with and without `LD_PRELOAD` recording
- Measure P50 and P99 latency for key API endpoints (health, signin, payments list, refund create)
- Tool: `wrk` or `hey` for load generation, consistent RPS across runs
- Report: delta-P50, delta-P99, and latency distribution histograms
- Target: < 5% P50 increase, < 10% P99 increase under recording

**Measurement methodology:**
```
# Baseline (no LD_PRELOAD)
wrk -t4 -c100 -d30s http://localhost:8080/health

# Recording (with LD_PRELOAD)
LD_PRELOAD=libdeja_preload.so DEJA_PRELOAD_MODE=record ...
wrk -t4 -c100 -d30s http://localhost:8080/health

# Compare
ΔP50 = (recording_P50 - baseline_P50) / baseline_P50
ΔP99 = (recording_P99 - baseline_P99) / baseline_P99
```

**Files:** `demo/benchmark-latency.sh`, results in `demo/benchmark-results/`

### 5.2 Fault isolation — recording failures must never kill the app ✅

**Problem:** If Déjà's recording layer crashes, hangs, or runs out of memory, Hyperswitch must continue serving traffic. The recording layer is NOT in the critical path.

**Deliverable:**
- All hook code paths wrapped in `catch_unwind` / error suppression
- `panic` in any Déjà code → log warning, fall through to real syscall, app continues
- Memory allocation failures in recording → graceful degradation (stop recording, keep serving)
- Mutex poison → bypass recording, call real function
- Test: inject panic into recording path, verify app request succeeds
- Test: `ulimit -v` memory limit, verify app serves but recording degrades

**Invariant:** For every syscall `S`, if Déjà code fails, the real `S` still executes and the app gets a valid result.

**Files:** `hooks.rs` (wrap every hook in safety net), `agent.rs`

### 5.3 Data completeness — no silent drops ✅

**Problem:** Are we capturing ALL relevant side-effect data? Or are there drops? The board needs quantifiable completeness.

**Deliverable:**
- Per-recording report:
  - `events_captured: N`
  - `events_dropped: M` (if ring buffer overflow or allocation failure)
  - `completeness_pct: (N / (N+M)) * 100`
  - `connections_opened: K`, `connections_fully_captured: K'`
  - `stream_hash_mismatches: 0`
- Drop counter: atomic counter incremented when an event can't be persisted
- `deja verify --artifact <PATH>` checks:
  - stream hashes match (from 0.2)
  - every opened connection has a close event
  - no gaps in stream offsets
- Target: 100% completeness for supported surfaces under normal load

**Files:** `agent.rs`, `deja-core/src/lib.rs`, `deja-cli/src/main.rs`

### 5.4 Memory overhead — bounded and measurable

**Problem:** LD_PRELOAD runs in the same address space as Hyperswitch (already ~239MB). Recording buffers must not cause OOM or excessive RSS growth.

**Deliverable:**
- Measure RSS with and without recording: `ps -o rss`
- Cap recording buffer size (ring buffer or bounded Vec with backpressure)
- Report: `memory_overhead_mb = recording_RSS - baseline_RSS`
- Target: < 50MB overhead under typical load
- If buffer fills: oldest events dropped, drop counter incremented (see 5.3)

**Files:** `agent.rs`, `lib.rs`

### 5.5 Disk I/O impact — recording must not starve the app

**Problem:** Writing events to disk on every syscall can cause I/O contention, especially under high throughput.

**Deliverable:**
- Buffer events in memory, batch-flush periodically (e.g. every 100ms or 1000 events)
- Measure disk write throughput during recording
- Compare Hyperswitch throughput (requests/sec) with and without recording
- Target: < 5% throughput decrease

**Files:** `agent.rs`, `lib.rs`

### 5.6 Replay determinism accuracy

**Problem:** When we replay recorded dependencies, does the app produce the same output? What's the match rate?

**Deliverable:**
- Run full record → replay → compare pipeline
- Metric: `replay_match_rate = matching_responses / total_responses`
- Track per-endpoint match rate
- Track specific mismatch categories:
  - timestamp differences (expected, noise-rule filtered)
  - UUID differences (expected, noise-rule filtered)
  - actual logic differences (real regressions)
- Target: 100% match after noise rules applied for deterministic endpoints

**Files:** `deja-compare/src/lib.rs`, `deja-cli/src/main.rs`

### 5.7 Startup time impact

**Problem:** LD_PRELOAD constructor runs before `main()`. If it's slow, service startup is delayed.

**Deliverable:**
- Measure time-to-health: seconds from process start to first successful `/health` response
- Compare with and without LD_PRELOAD
- Target: < 500ms additional startup time

**Files:** `demo/benchmark-startup.sh`

### 5.8 Hyperswitch-specific integration test matrix

**Problem:** Need repeatable benchmarking against the actual target service, not just unit tests.

**Deliverable:**
- Docker Compose setup (already exists: `demo/docker-compose.hyperswitch.yml`)
- Automated benchmark script that runs all 5.1-5.7 metrics
- Results stored as JSON for CI trend tracking
- Matrix of test scenarios:
  - Light load: 10 RPS, 1 min
  - Medium load: 100 RPS, 5 min
  - Burst: 1000 RPS, 30s
  - Soak: 50 RPS, 30 min (memory leak detection)

**Files:** `demo/benchmark-all.sh`, `demo/benchmark-results/`

---

## Layer 3.5: Correlation Architecture — The right way to propagate context

*The fundamental problem: `tokio::spawn` and `tokio::task::spawn_blocking` break naïve task-local propagation. Every Rust observability tool (OTel, tracing, us) hits this wall. Déjà should use upstream Tokio task hooks where available, not a patched Tokio dependency.*

### 3.5.1 Open design decision: How to wrap spawn boundaries without touching application code

**Current state:** We proved correlation works end-to-end by replacing `tokio::spawn` / `tokio::task::spawn_blocking` with `deja_tokio::spawn` / `deja_tokio::spawn_blocking` across Hyperswitch (80 call sites via sed). That proved the mechanism but is not maintainable — every upstream update re-breaks it.

**The approaches, ranked by maintainability:**

| Approach | How it works | Covers deps? | App code changes? | Maintainability |
|----------|-------------|-------------|-------------------|----------------|
| **A. Upstream Tokio task hooks** | Install `on_task_spawn` / poll hooks on the runtime Builder; no fork | ✅ Ordinary task spawns in same runtime | ⚠️ Runtime builder setup | High — no patched dependency |
| **B. `deja-tokio` explicit wrappers** | Use `deja_tokio::spawn` / `spawn_blocking` | ❌ Misses deps | ⚠️ Per-call-site changes | Medium — useful fallback |
| **C. Proc macro `#[deja::main]`** | Builds runtime with hooks and calls `deja::init()` | ✅ Ordinary task spawns | ✅ One annotation | Medium — future ergonomics layer |
| **D. sed / find-replace** | Direct text substitution across codebase | ❌ Fragile | ⚠️ 80+ call sites | Low — breaks on every update |

**Approach A is the right answer now.** The implementation should use Tokio's upstream task-hook API, with `deja-context` as the runtime-independent context store. In Tokio 1.48 those hooks require `RUSTFLAGS="--cfg tokio_unstable"`, but they do not require a vendored Tokio tree or `[patch.crates-io]` override.

**Important boundary:** task hooks solve spawned async task inheritance. They do not solve command-channel ownership inside fred/redis-rs/Kafka driver tasks. Those need command envelopes, wrappers, or upstream metadata extension points.

**Decision before production:** standardize on hook-installed runtimes and explicit command-boundary adapters. Do not make a patched Tokio/fred dependency part of the mainline architecture.

### 3.5.2 Correlation metric: measure what matters

**Current state:** The `deja-cli correlate` command uses `(tagged_events / total_events)` — a blanket metric that penalizes correctly-untagged background events (clock_gettime, pool maintenance). This gave 23.9% when actual I/O correlation was 50.6%.

**The right metric:** `tagged_outbound_socket_events / total_outbound_socket_events`. This directly asks: *of the I/O operations that should belong to a request, how many carry its correlation ID?* Background events (time, random) and pool management (connect, close) are excluded from the denominator — they correctly have no correlation ID.

**Action:** Fix `deja-cli correlate` to use this metric. Remove the duplicated bash metric from `pipeline.sh` — `deja-cli correlate` should be the single source of truth.

---

## Layer 4: Missing Surfaces — Extend coverage

*Not v1 blockers, but tracked so we don't forget.*

### 4.1 TLS interception strategy

**Problem:** Raw socket bytes are ciphertext. Can't parse or meaningfully replay.

**Options:**
- A. Hook OpenSSL/BoringSSL `SSL_read`/`SSL_write` — intercepts plaintext above TLS
- B. MITM proxy — terminate TLS, re-encrypt — complex but language-agnostic
- C. `SSLKEYLOGFILE` + pcap — offline decryption, replay-only, no live intercept
- D. Accept opacity — mark TLS connections as opaque, only hash-compare

**Recommendation:** Start with D (mark opaque + hash compare). Add A (OpenSSL hooks) as the real solution.

### 4.2 UDP support

**Problem:** DNS, some databases, QUIC. Current hooks only track SOCK_STREAM.

**Deliverable:** Hook `sendto`/`recvfrom`/`sendmsg`/`recvmsg` for `SOCK_DGRAM`. Record (addr, payload) pairs.

**Update (2026-04-19):** `sendto()`/`recvfrom()` are now hooked for TCP sockets. UDP recording still requires SOCK_DGRAM tracking (not yet implemented).

**Update (2026-04-19):** `getrandom()` (43 calls in demo) and `clock_gettime()` (13,037 calls) are now hooked for deterministic replay. Record random bytes and timestamps; replay returns recorded values.

### 4.3 `sendfile` / `splice` / `sendmmsg` / `recvmmsg`

**Problem:** Zero-copy and batched I/O syscalls bypass our `send`/`recv` hooks.

**Deliverable:** Hook each individually. `sendfile` is most impactful (static file serving).

### 4.4 Unix domain sockets

**Problem:** Sidecar communication, Docker sockets, Postgres local connections.

**Deliverable:** Extend `FdTracker` to handle `AF_UNIX`. Same byte-stream recording.

---

## Dependency Graph

```
Layer 0 (Transport Fidelity)
  0.1 connection_id ──────────────────────┐
  0.2 stream offsets + hashes ── depends on 0.1
  0.3 outbound replay byte-compare        │
  0.4 surface contract (independent)      │
  0.5 artifact integrity (independent)    │
  0.6 ground-truth tests (depends on 0.2) │
                                          │
Layer 1 (Parsers)                         │
  1.1 redis-protocol (independent)        │
  1.2 httparse (independent)              │
  1.3 PG parser (independent)             │
  1.4 HTTP/2 frame decode (independent)   │
  1.5 HPACK decoder ─── depends on 1.4    │
  1.6 Schemaless protobuf (independent)   │
  1.7 Schema protobuf ── depends on 1.6   │
                                          │
Layer 2 (Replay Correctness)              │
  2.1 vectored I/O (independent)          │
  2.2 connection identity ─ depends on 0.1┘
  2.3 end-of-run validation (independent)

Layer 3 (Regression Report)
  3.1 unified format ──── depends on 1.1-1.7, 0.2
  3.2 regress-live ────── depends on 3.1, 2.2, 2.3
  3.3 JSON/SARIF output ─ depends on 3.1

Layer 5 (Production Readiness Metrics)
  5.1 latency impact ─── depends on working recording
  5.2 fault isolation ─── depends on 0.4 (surface contract)
  5.3 data completeness ─ depends on 0.2 (stream hashes), 0.3 (byte-compare)
  5.4 memory overhead ─── independent
  5.5 disk I/O impact ─── independent
  5.6 replay determinism ─ depends on 2.2, 2.3, 3.1
  5.7 startup impact ──── independent
  5.8 HS integration test matrix ─ depends on 5.1-5.7

Layer 4 (Missing Surfaces) — independent, parallelizable
  4.1 TLS
  4.2 UDP
  4.3 sendfile/splice
  4.4 Unix domain
```

## Already Done (on `feat/replay-pipeline` branch)

- [x] Hook `writev` / `sendmsg` / `recvmsg`
- [x] Per-fd replay queues
- [x] Socketpair-based replay for epoll/tokio
- [x] `deja replay-traffic` command
- [x] Pipeline demo script with structured logs
- [x] 107 events captured from live Hyperswitch
- [x] `accept` / `accept4` hooks (on `main`)
- [x] `EventDirection::Inbound` tracking (on `main`)
- [x] `request_id` from shared memory (on `main`)
- [x] Thread-local REQUEST_SCOPE in hooks (fixes racy shared memory under tokio)
- [x] `deja-tokio` crate: `RequestScope` future wrapper bridges task-local to thread-local
- [x] `deja-actix` crate: `DejaScope` actix-web middleware for Hyperswitch
- [x] `deja correlate` CLI command: validate request_id grouping
- [x] Hyperswitch patched with `deja-actix` middleware + `deja::init()`
- [x] Demo pipeline Phase 5: concurrent traffic + correlation validation

## Priority Order for Next Sprint

| # | Item | Impact | Effort | HS ref |
|---|------|--------|--------|--------|
| 0.1 | Stable `connection_id` | Unblocks 0.2, 2.2 | Small | |
| 0.3 | Outbound replay byte-compare | Catches silent divergence | Small | |
| 5.2 | Fault isolation (recording never kills app) | Board requirement, HS-41 | Small | HS-41 |
| 1.1 | Replace Redis parser with `redis-protocol` | Correctness of most common dependency | Small | |
| 1.2 | Replace HTTP parser with `httparse` | Full header/body visibility | Small | |
| 1.4 | HTTP/2 frame decoder | Foundation for gRPC | Small | |
| 0.4 | Surface contract | Prevents false confidence | Small | |
| 1.6 | Schemaless Protobuf parser | gRPC diff visibility | Medium | |
| 0.2 | Stream offsets + hashes | Proves transport fidelity | Medium | HS-79 |
| 5.3 | Data completeness metrics | Board requirement, HS-41 | Medium | HS-41 |
| 5.1 | Latency impact benchmark | Board requirement, HS-41 | Medium | HS-41 |
| 1.5 | HPACK decoder | gRPC method names | Medium | |
| 1.7 | Schema-based Protobuf | Named field diffs | Medium | |
| 3.1 | Unified regression report | The whole point | Medium | HS-78 |
| 2.2 | Stronger connection identity | Concurrency safety | Small | |
| 2.3 | End-of-run replay validation | Completeness guarantee | Small | |
| 5.4 | Memory overhead measurement | Board requirement, HS-41 | Small | HS-41 |
| 5.5 | Disk I/O impact measurement | Production safety | Small | HS-41 |
| 5.7 | Startup time impact | Production safety | Small | HS-41 |
| 3.2 | `deja regress-live` | One-command UX | Medium | |
| 0.5 | Artifact integrity | Crash safety | Small | |
| 0.6 | Ground-truth tests | ✅ `deja verify --pcap` | Medium | HS-79 |
| 1.3 | PG parser upgrade | Better PG visibility | Medium | |
| 2.1 | Vectored I/O fidelity | Edge case correctness | Small | |
| 3.3 | JSON/SARIF output | CI integration | Small | |
| 5.6 | Replay determinism accuracy | End-to-end proof | Medium | HS-41 |
| 5.8 | HS integration test matrix | Repeatable benchmarks | Medium | HS-41 |

## HS-41 Metric Targets (Board-Visible)

| Metric | Target | Measurement | Phase gate |
|--------|--------|-------------|------------|
| P50 latency increase | < 5% | wrk baseline vs recording | Phase 2 exit |
| P99 latency increase | < 10% | wrk baseline vs recording | Phase 2 exit |
| Recording fault isolation | 0 app failures caused | Inject panic, verify app serves | Phase 2 exit |
| Data completeness | 100% captured, 0 dropped | Event counter + stream hashes | Phase 2 exit |
| Memory overhead | < 50 MB | RSS delta | Phase 3 exit |
| Throughput decrease | < 5% | RPS baseline vs recording | Phase 2 exit |
| Replay match rate | 100% (after noise rules) | regress-live comparison | Phase 3 exit |
| Startup time increase | < 500 ms | Time-to-health delta | Phase 2 exit |

---

## Appendix: Competitive Analysis — AREX and Tri-System Comparison

### Background
AREX (`github.com/arextest`) is an open-source Java record/replay platform that uses ByteBuddy-based Java agent instrumentation. Studying AREX provides validation for Déjà's technical choices and highlights differentiation opportunities.

**Reference document:** `docs/AREX_ANALYSIS.md` (deep-dive)

### AREX Key Characteristics

| Aspect | AREX Approach | Déjà Equivalent |
|--------|--------------|-----------------|
| **Capture** | ByteBuddy Java agent instrumentation | `LD_PRELOAD` syscall interception |
| **Languages** | Java only | Universal (libc-linked) |
| **Context** | `ThreadLocal` + wrapping inheritance | `tokio::task_local!` structured scopes |
| **Replay matching** | Signature → Fuzzy (sequential) | Planned: state-seeded owned deps + causal fallback |
| **Storage** | MongoDB + Redis cache | Streaming JSONL files |
| **Comparison** | Post-replay JSON diff | Real-time structural diff |

### Critical Finding: AREX Uses Sequential Not Causal Matching

AREX's match strategy hierarchy confirms our hypothesis about industry practice:

```
┌────────────────────────────────────────────────────────┐
│  1. ACCURATE: operationName + requestBody hash         │
│  2. FUZZY: Sequential consumption by creationTime     │
│  3. EIGEN: Feature-based (unimplemented)              │
└────────────────────────────────────────────────────────┘
```

**Source:** `arex-instrumentation-api/src/main/java/io/arex/inst/runtime/match/`

The fuzzy strategy's "first unmatched" selection policy handles repeated calls (e.g., `INCR` twice) but relies on **temporal ordering**, not causal ordering. This means:

- ✅ AREX handles `INCR` called twice within one request correctly (sequence matters)
- ⚠️ AREX cannot distinguish reads before/after writes that share the same signature
- ⚠️ Under replay timing variations, temporal order may diverge from causal order

### Déjà's Differentiation: State-Seeded Replay + Causal Correlation

Déjà's planned `Correlation 2.0` (see `CORRELATION_ARCHITECTURE.md`) and state-seeded replay model (see `docs/STATE_SEEDED_REPLAY.md`) address this gap:

```rust
struct DependencyOperationIdentity {
    global_index: u64,              // Monotonic capture order
    causal_scope_id: CorrelationId, // Request/scoped context
    local_sequence_in_scope: u32,   // Ordinal within scope
    resource_version_or_cursor: Option<u64>, // State generation
    // ...
}
```

This enables correct isolated replay of patterns like:
```rust
let v1 = redis.get("payment:intent:123").await?;  // Read before
redis.hset("payment:intent:123", "status", "processing").await?;
let v2 = redis.get("payment:intent:123").await?;  // Read after
// State-seeded mode: seed pre-request state, let real Redis produce v1/v2.
// Response-replay fallback: same signature, different causal ordinals.
```

### Tri-System Summary: Déjà vs Speedscale vs AREX

| Dimension | Déjà | Speedscale | AREX |
|-----------|------|------------|------|
| **Capture** | `LD_PRELOAD` (syscall) | Sidecar proxy / eBPF | ByteBuddy agent |
| **Scope** | Universal | Universal | Java only |
| **Context Model** | Task-local (structured) | None / vUser (replay only) | Thread-local (inheritance) |
| **Matching** | State-seeded owned deps + causal fallback (planned) | Signature + vUser sequence | Signature + sequential |
| **Stateful Reads** | Seed isolated DB/Redis; validate writes/final state | Not addressed | Not addressed |
| **Pool Mediation** | Driver integration needed | Proxy handles naturally | Driver integration needed |
| **Deployment** | Env var | K8s sidecar | JVM `-javaagent` |
| **Operational Cost** | File-based (low) | SaaS/managed | MongoDB + Redis |

### Strategic Implications

1. **Causal correlation and state-seeded isolated replay are rare**: Neither Speedscale nor AREX implement true capture-time causal tracking or per-request owned-state reconstruction. Déjà's investment here is a genuine differentiator.

2. **Sequential != Causal**: AREX's temporal ordering works for simple cases but fails when:
   - Multiple requests interleave operations on shared keys
   - Replay timing differs from recording timing
   - Pool-mediated I/O executes commands out of submission order

3. **Language universality**: AREX's ByteBuddy is powerful but Java-locked. Déjà's `LD_PRELOAD` can capture Go, Rust, Python, Node.js, and compiled binaries without per-language agents.

4. **Operational simplicity**: AREX requires MongoDB + Redis infrastructure. Déjà's file-based artifacts enable local development and CI/CD without external dependencies.

### Action Items Derived from AREX Study

- [ ] **2.4 Dependency operation identity** — Implement causal scope + ordinal tracking (validated as differentiated capability)
- [ ] **2.5 Ambiguity detector** — Identify when signature-only replay would fail (validated by AREX's approach being insufficient for mutable reads)
- [ ] **2.6 Per-request state fixture inference** — Use correlation/order to derive pre-request DB/Redis state slices
- [ ] **2.7 Redis state-seeded replay** — Seed isolated Redis and validate ordered writes/final state
- [ ] **2.8 SQL confidence-tiered fixture replay** — Seed exact row reads; warn on aggregates/joins/partial projections
- [ ] **2.9 Hybrid replay orchestrator** — Mock external APIs, seed owned state, deterministic time/random/env
- [ ] **Fred Redis integration** — Pool-mediated I/O requires command metadata propagation (same problem AREX faces with Lettuce/Jedis, but solved via wrapping)
- [ ] **Benchmark against AREX patterns** — Ensure Déjà handles `GET→SET→GET` sequences correctly where AREX's sequential matching may fail

### References

| Document | Contents |
|----------|----------|
| `docs/AREX_ANALYSIS.md` | Comprehensive AREX architecture analysis |
| `docs/SPEEDSCALE_COMPARISON.md` | Speedscale architecture comparison |
| `docs/SPEEDSCALE_INSIGHTS.md` | Strategic synthesis focusing on causality |
| `docs/STATE_SEEDED_REPLAY.md` | First-principles owned DB/Redis state-seeded replay design |
| `CORRELATION_ARCHITECTURE.md` | Déjà's causal correlation design |
| `HYPERSWITCH_REPLAY_AMBIGUITY_REPORT.md` | Real-world evidence requiring causal tracking |
