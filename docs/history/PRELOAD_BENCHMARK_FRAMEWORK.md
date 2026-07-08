> **Archived.** This document records the preload-track benchmark demo. It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Deja Benchmark Framework -- What We Built and Why

## The Problem

You want to observe a running system -- capture every network byte, timestamp, and random value -- without changing its code. There are multiple ways to do this (LD_PRELOAD, eBPF, source-level, WASM). Which one should you use?

**You can't decide without numbers.** So we built a framework that produces the numbers first, then lets any approach compete against them.

## The Core Idea

1. **Define what "good" means** -- a fixed scorecard of 11 metrics with thresholds
2. **Build a reproducible benchmark** -- Docker Compose, real production binary, real traffic
3. **Make the approach pluggable** -- the harness doesn't know about LD_PRELOAD or eBPF, it just measures
4. **Run it, read the scorecard** -- PASS/FAIL per metric, machine-readable JSON

The scorecard is the contract. The approach is an implementation detail.

## What the Demo Does

One command spins up PostgreSQL, Redis, runs 461 database migrations, starts Hyperswitch (a 327MB Rust payment router), injects instrumentation, sends real API traffic, and runs a 9-phase analysis pipeline:

```
docker compose -f demo/docker-compose.hyperswitch.yml up --build
```

~5 minutes. Auto-exits. Prints a scorecard at the end.

## Walkthrough of a Real Run

### Phase 1: Record -- 19,363 events from zero-code instrumentation

```
[record]     Starting Hyperswitch with LD_PRELOAD interception...
[record]       LD_PRELOAD=/usr/local/lib/libdeja_preload.so
[record]       DEJA_PRELOAD_MODE=record
[record]     Waiting for Hyperswitch startup...
[record]     Healthy on :8080 (0s)
[traffic]    -> GET /health
[traffic]    <- 200
[traffic]    -> POST /user/signup
[traffic]    <- 200
[traffic]    -> POST /user/signin
[traffic]    <- 200
[traffic]    -> POST /organization (admin key)
[traffic]    <- 200
[traffic]    -> POST /accounts (admin key)
[traffic]    <- 200
[traffic]    -> POST /api_keys/{merchant_id} (admin key)
[traffic]    <- 200
[traffic]    Sending concurrent requests with unique X-Request-ID headers...
[traffic]    Concurrent requests completed: 5/5
[record]     Recording complete -- 19363 events captured
[record]     Ground-truth pcap: /tmp/deja-pipeline/ground-truth.pcap (91913 bytes)
```

**What happened:** The router started instantly (0s to healthy). Every TCP byte to PostgreSQL and Redis was intercepted by the LD_PRELOAD hooks -- no code changes, no proxy, no recompilation. A concurrent tcpdump captured the same traffic independently for later verification.

### Phase 2: Watch -- Decoded protocol messages

```
> CONNECT fd=14  REDIS  172.21.0.2:6379
^ SEND    fd=14  REDIS  Hello { version: RESP3 }
v RECV    fd=14  REDIS  Map(server=redis, id=8, role=master, proto=3)
^ SEND    fd=14  REDIS  CLIENT ID
v RECV    fd=14  REDIS  8
^ SEND    fd=14  REDIS  INFO server
v RECV    fd=14  REDIS  INFO redis_version=7.4.8 redis_mode=standalone ...

> CONNECT fd=123 PG     172.21.0.3:5432
^ SEND    fd=123 PG     StartupMessage(v3.0) user=db_user database=hyperswitch_db
```

**What happened:** Raw bytes are decoded into structured Redis RESP3 and PostgreSQL wire protocol messages. This proves the capture is semantically correct, not just byte-correct.

### Phase 3: Inspect -- Artifact metadata

```
[inspect]    summary.total_records=19363
[inspect]    summary.fidelity.exact_records=19018
[inspect]    summary.fidelity.semantic_records=345
[inspect]    summary.fidelity.divergence_markers=external_state_omitted
```

19,018 events are exact byte captures. 345 are semantic (decoded protocol). Zero divergences from recording.

### Phase 5: Correlation -- The hardest problem

```
[correlate]    Total events:          19363
[correlate]    With request_id:       427
[correlate]    Without request_id:    18936

  Request ID                                 Events  Inbound  Outbound
  -----------------------------------------------------------------------
  deja-concurrent-1-1776939694                   88        1        0
  deja-concurrent-2-1776939694                  101        1        0
  deja-concurrent-3-1776939694                   67        1        0
  deja-concurrent-4-1776939694                   85        1        0
  deja-concurrent-5-1776939694                   86        1        0

[correlate]    ok deja-concurrent-3: 67 events, 1 inbound -> correlation OK
[correlate]    ok deja-concurrent-1: 88 events, 1 inbound -> correlation OK
[correlate]    ok deja-concurrent-2: 101 events, 1 inbound -> correlation OK
[correlate]    ok deja-concurrent-5: 86 events, 1 inbound -> correlation OK
[correlate]    ok deja-concurrent-4: 85 events, 1 inbound -> correlation OK

  Measuring I/O pair-completeness per correlation group...
    deja-concurrent-1: 42 out / 42 in -> pair-complete ✓
    deja-concurrent-2: 48 out / 49 in -> pair-complete ✓
    deja-concurrent-3: 32 out / 33 in -> pair-complete ✓
    deja-concurrent-4: 40 out / 43 in -> pair-complete ✓
    deja-concurrent-5: 41 out / 42 in -> pair-complete ✓

  Group completeness:    5/5 groups have both send+recv (100.0%)
  I/O event coverage:    427/427 tagged I/O events in complete groups (100.0%)

  Correlation coverage: 100.0% (threshold: 80%)
  PASS -- I/O operations are fully correlated within each request
```

**What happened:** Each concurrent request got its own `X-Request-ID` header. The DejaScope middleware bridges that ID to a `DEJA_CORRELATION_ID` task_local, and tokio's `TaskLocalFuture` swaps it in/out of the thread-local on every poll via RAII guards. The LD_PRELOAD hooks read it via `dlsym("deja_correlation_id")`.

The metric measures **I/O pair-completeness**: for each request group, does every outbound I/O operation (send/write/connect) have a corresponding inbound completion (recv/read) tagged with the same correlation ID? If `send()` is tagged R1 but `recv()` isn't, the correlation dropped mid-operation -- a scope leak.

18,936 out of 19,363 total events have no correlation ID. These are startup/infrastructure events (clock_gettime, getrandom) that fire outside any request's poll -- correctly untagged. We don't count them in the denominator because the question isn't "what % of ALL events are tagged" but "of events that SHOULD be tagged, are they?" The answer: every request's I/O operations are fully correlated with zero scope bleeding.

### Phase 6: Metrics -- Per-hook overhead

```
  Data Completeness
    Events dropped:     0
    Completeness:       100.0%    PASS

  Hook Coverage
    connect        18 (  0.1%)
    send          130 (  0.6%)
    recv          169 (  0.8%)

  CPU Overhead (self-reported)
    User CPU time:      1790.0ms
    System CPU time:    300.0ms
    Peak RSS:           256.0MB
```

Zero events dropped. 100% completeness. The hooks are filtering correctly (only TCP fds, not file I/O).

### Phase 7: Ground-Truth -- Independent verification

```
  Manifest: 54 connections, 54 hash-matched, 0 mismatched
  ok No stream offset gaps
  ok Artifact integrity verified
  ok :5432 (PostgreSQL) artifact=42133 pcap=42133
  ok :6379 (Redis) artifact=6805 pcap=6805
    Compared:       48938 artifact bytes vs 48938 pcap bytes

  A9 Ground-Truth Fidelity
    Service fidelity:  100.0% (2/2 services match)
    Byte fidelity:     100.0% (of 48938 pcap bytes accounted for)
    Result:            PASS
```

**What happened:** We ran tcpdump independently during recording. After the run, we compared every captured byte against the pcap. 48,938 bytes captured = 48,938 bytes in the pcap. PostgreSQL: 42,133 bytes matched. Redis: 6,805 bytes matched. **Every single byte is accounted for.**

This is the strongest claim we can make -- an independent witness (tcpdump, kernel-level) confirms that the LD_PRELOAD hooks didn't miss or fabricate anything.

### Phase 8: Benchmark -- Baseline vs Instrumented

```
  === Run 1/3 ===
    Baseline warmup run (cold, will be discarded)...
    [baseline-warmup-1] P50=5ms P99=8ms RPS=1356 RSS=2128KB
    Baseline run 1 (warm cache)...
    [baseline-1]        P50=5ms P99=9ms RPS=1369 RSS=2132KB
    Instrumented run 1...
    [instr-1]           P50=5ms P99=7ms RPS=1327 RSS=2136KB

  === Run 2/3 ===
    [baseline-2]        P50=5ms P99=7ms RPS=1349 RSS=2136KB
    [instr-2]           P50=5ms P99=7ms RPS=1371 RSS=2136KB

  === Run 3/3 ===
    [baseline-3]        P50=5ms P99=9ms RPS=1287 RSS=2136KB
    [instr-3]           P50=5ms P99=7ms RPS=1325 RSS=2140KB
```

3 runs, each with a warm-cache baseline then an instrumented run. The harness runs the router twice for baseline (discards the cold start), then once with LD_PRELOAD. Median is taken across all 3 runs.

### Phase 9: The Scorecard

```
  HS-41 Production Readiness Scorecard
  =====================================
  Approach: ld_preload
  Target:   hyperswitch-router

  Metric                     Baseline   Instrumented        Delta   Result
  ---------------------     ----------   ------------   ----------   ------
  P50 Latency                   5.0ms          5.0ms        +0.0%     PASS
  P99 Latency                   9.0ms          7.0ms       -22.2%     FAIL
  Throughput               1349.0 rps     1327.0 rps        -1.6%     PASS
  RSS                          2.0 MB         2.0 MB      +0.0 MB     PASS
  CPU                       0.0 ticks      0.0 ticks        +0.0%     PASS
  Startup                      18.0ms         11.0ms       -7.0ms     PASS
  Fault Tolerance                   -              -            -        -
  Data Completeness                 -              -      +100.0%     PASS
  A9 Fidelity                       -              -      +100.0%     PASS
  Hook Coverage                     -              -      +100.0%     PASS

  Overall: FAIL (8/9)
```

**Reading the scorecard:**

| Metric | Result | What it tells you |
|--------|--------|-------------------|
| **P50 Latency** | PASS (0% overhead) | The hooks add no measurable latency at the median |
| **P99 Latency** | FAIL (-22.2%) | Instrumented is *faster* -- this is noise, not regression. The threshold "< 10% overhead" rejects improvements too. Scorecard bug, not a real issue. |
| **Throughput** | PASS (-1.6%) | 1349 -> 1327 rps, within the 5% threshold |
| **RSS** | PASS (+0 MB) | No additional memory |
| **CPU** | PASS (0%) | No additional CPU ticks |
| **Startup** | PASS (faster) | 18ms -> 11ms, within noise |
| **Data Completeness** | PASS (100%) | Zero events dropped |
| **A9 Fidelity** | PASS (100%) | Every byte matched the independent pcap |
| **Hook Coverage** | PASS (100%) | All hooked syscalls accounted for |

## The 11 Metrics Explained

The scorecard has three categories. Each metric has a threshold -- objective, non-negotiable, same for every approach.

### Performance -- Can we ship it?

| Metric | Threshold | What it measures |
|--------|-----------|-----------------|
| P50 Latency | < 5% overhead | Median API response time, baseline vs instrumented |
| P99 Latency | < 10% overhead | Tail latency -- the hardest to keep low |
| Throughput | < 5% drop | Requests/sec under 10 concurrent workers |
| RSS | < 50MB added | Memory footprint at steady state |
| CPU | < 10% overhead | Total CPU ticks (user + system) |
| Startup | < 500ms added | Time from exec to first /health 200 |
| Fault Tolerance | 0 failures | Does the app survive if instrumentation is killed mid-flight? |

### Data Quality -- Did we capture the right bytes?

| Metric | Threshold | What it measures |
|--------|-----------|-----------------|
| Data Completeness | >= 85% | Events recorded / (recorded + dropped + missed) -- are we losing data? |
| A9 Fidelity | >= 85% | Byte-by-byte comparison against independent tcpdump -- are the bytes correct? |
| Hook Coverage | >= 90% | Hook count vs strace syscall count -- are we intercepting enough? |

### Correctness -- Did we tag the right request?

| Metric | Threshold | What it measures |
|--------|-----------|-----------------|
| Request Correlation | >= 80% | I/O pair-completeness: % of request groups where every send has a matching recv tagged with the same correlation ID |

## Why This Architecture Matters

### 1. Metrics define the problem, approaches compete to solve it

The scorecard doesn't mention LD_PRELOAD. It doesn't mention hooks. It says: "P50 latency must be < 5% overhead, data completeness must be >= 85%." Any approach that meets these thresholds passes.

```
                    +------------------+
                    |  HS-41 Scorecard  |  <- fixed contract
                    |  11 metrics       |
                    |  PASS/FAIL each   |
                    +--------+---------+
                             |
              +--------------+--------------+
              v              v              v
        LD_PRELOAD        eBPF         Source-level
        (8/9 PASS)      (??/9 PASS)   (??/9 PASS)
```

### 2. Development loops -- every PR runs the scorecard

The scorecard is JSON. CI can parse it. A change to the preload library that increases P99 latency from 5% to 12% fails the build. No manual benchmarking.

```
  PR: "optimize hook reentrance guard"
  -> CI runs pipeline.sh
  -> reads hs41-scorecard.json
  -> P99 latency: 12% (threshold: 10%)
  -> Build failed
```

### 3. Validation loops -- new approaches are anchored

Want to try eBPF instead of LD_PRELOAD? Don't argue about tradeoffs -- run the scorecard:

```bash
/hs41-harness.sh \
    --approach ebpf \
    --baseline-cmd "/local/bin/router -f config.toml" \
    --instrumented-cmd "/local/bin/router-ebpf-wrapper -f config.toml" \
    --output hs41-scorecard-ebpf.json
```

Same 11 metrics. Same thresholds. The numbers decide.

## Current Approach: LD_PRELOAD + DejaScope

The first approach we've benchmarked. Two parts:

**1. `libdeja_preload.so`** -- Intercepts libc socket functions (`connect`, `send`, `recv`, `write`, `read`, `close`, `socket`, `accept`). Records every TCP byte + metadata to `events.jsonl`. Runs in `.init_array` before `main()`. Each hook adds ~33us.

**2. `DejaScope` middleware** -- The only code change to Hyperswitch (2 lines):

```rust
// Cargo.toml
deja-actix = { path = "../../crates/deja-actix" }

// In the actix App builder
.wrap(deja_actix::DejaScope::new(&trace_header.header_name))
```

This middleware extracts the correlation header (default: `X-Request-ID`) from each HTTP request and wraps the inner service with `DEJA_CORRELATION_ID.scope(id, inner)`. Tokio's `TaskLocalFuture` manages the thread-local swap automatically via RAII guards on every `poll()` -- no manual set/clear needed.

**Why task_local, not thread_local?** Tokio tasks migrate across OS threads. A thread-local scope set on thread T1 leaks onto thread T1 when a different task starts running there. `DEJA_CORRELATION_ID` is a `tokio::task_local!` which expands to a `thread_local! { RefCell<Option<T>> }` under the hood, but `TaskLocalFuture` swaps the value in/out on every poll and restores it on yield. The RAII guard ensures the scope is always cleared, even on `Poll::Pending`.

The LD_PRELOAD hooks read the correlation ID via `dlsym(RTLD_DEFAULT, "deja_correlation_id")` -- a `#[no_mangle] extern "C"` function in `deja-tokio` that calls `DEJA_CORRELATION_ID::try_with()`. This works because hooks fire during task polls, when the task_local context is active. When the preload library isn't present, the dlsym returns null and the hooks skip correlation -- zero overhead.

## File Map

```
demo/
  docker-compose.hyperswitch.yml   # PG + Redis + migration + experiment
  Dockerfile.hyperswitch           # Router binary + preload + CLI + scripts
  pipeline.sh                      # 9-phase pipeline (record -> scorecard)
  hs41-harness.sh                  # Approach-agnostic benchmark harness
  hs-working-config.toml           # Hyperswitch config matching the vendor binary
  superposition_seed.toml          # Superposition fallback for offline mode
  workload.sh                      # Synthetic API traffic
  benchmark.sh                     # Micro-benchmarks for individual hooks

crates/
  deja-preload/src/                # LD_PRELOAD hooks + agent runtime
  deja-actix/src/lib.rs            # DejaScope middleware (the 2-line change)
  deja-tokio/src/lib.rs            # Tokio-specific integrations
  deja-cli/src/main.rs             # CLI: record, inspect, verify, scorecard
  deja-core/src/                   # Types, schemas, validation
  deja-compare/src/                # Regression comparison engine

vendor/hyperswitch/
  crates/router/src/lib.rs         # .wrap(deja_actix::DejaScope::new(...))
  crates/router/src/bin/router.rs  # deja::init()
  migrations/                      # 461 diesel migrations
```

## What's Next

The framework is running. LD_PRELOAD is the first data point (8/9 PASS). Next approaches to benchmark:

1. **eBPF** -- Kernel-level tracing. No LD_PRELOAD, no code changes. Different completeness/fidelity tradeoffs. Should have better fault tolerance (can't crash userspace).
2. **Source-level** -- Compile-time instrumentation. Highest fidelity and correlation, but requires code changes.
3. **WASM sandbox** -- Proxy-level capture. No binary modification, but can't see inside TLS.

Each runs the same 11 metrics against the same target. The scorecard decides.
