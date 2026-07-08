> **Archived.** This document records the tokio-patch/deja-actix integration model, which was not adopted (the shipped integration uses cfg-gated attribute macros). It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Déjà Integration Roadmap — Scalability for Rust Services

How much work is it to add Déjà to a new Rust service? What must change, and what should be automatic?

Today the answer is "it depends on your I/O drivers." This roadmap defines the phases to make it "add three lines and it works" for the common case.

---

## The core insight

Déjà's LD_PRELOAD hooks see every syscall. The FFI bridge can read the current task-local on every poll. **The only thing that breaks is when the request task yields before the syscall happens, and a different task (one that never had the correlation ID) performs the I/O.**

So integration difficulty is proportional to: **how many execution-ownership boundaries does your service have between receiving a request and performing I/O?**

---

## Boundary classification (recap)

| Boundary | Example | Hook sees ID? | Why |
|---|---|---|---|
| Same-task async | request → driver.write() | ✅ Yes | Task-local is active during poll |
| Spawned child | tokio::spawn(child) | ❌ No | Child has no task-local |
| Blocking pool | spawn_blocking / rayon | ❌ No | Different thread, no scope |
| Command-channel worker | fred routing task, Kafka producer | ❌ No | Worker was spawned at startup |
| Batched/multiplexed transport | Redis pipelining, HTTP/2 | ⚠️ Partial | One syscall can mix multiple request IDs |
| Background/system I/O | pool healthcheck, reconnect | N/A | No request to attribute |

Integration overhead scales with how many of these boundaries a service uses, and whether Déjà already has an adapter for each.

---

## Phase 0: What we have now (Hyperswitch demo)

```
Integration steps:
  1. cargo add deja-tokio deja-actix
  2. Add DejaScope middleware to actix app
  3. Replace tokio::spawn with deja_tokio::spawn (manual, ~80 call sites)
  4. Replace spawn_blocking with deja_tokio::spawn_blocking (manual)
  5. Patch async-bb8-diesel to use deja spawn_blocking (manual)
  6. Redis/fred correlation: not solved (documented limitation)
```

**Effort:** ~2 days for a service the size of Hyperswitch, assuming you already understand the codebase.

**Problems:**
- Manual spawn replacement is fragile (upstream updates break it)
- Each driver with a command-channel worker needs bespoke patching
- No guidance for "does this driver need an adapter?"
- The answer to "how do I add Déjà?" is a conversation, not a doc page

---

## Phase 1: Hook-only spawn propagation

**Goal:** Remove the need to manually replace ordinary `tokio::spawn` call sites without introducing a patched Tokio dependency.

**Mechanism:** build the service runtime with upstream Tokio task hooks via `deja_tokio::RuntimeBuilderExt`.

```rust
use deja_tokio::RuntimeBuilderExt;

let mut builder = tokio::runtime::Builder::new_multi_thread();
builder.enable_all().enable_deja_context_hooks();
let runtime = builder.build()?;
```

The hook driver is:

```text
on_task_spawn(task_id): capture current deja-context snapshot
on_before_task_poll(task_id): enter the task's snapshot
on_after_task_poll(task_id): restore previous thread context
on_task_terminate(task_id): delete task state
```

**Why this is the right approach now:**
- No `[patch.crates-io]` override.
- No vendored Tokio source in the active tree.
- Raw `tokio::spawn` / `Handle::spawn` can inherit context when the runtime has hooks installed.
- The remaining driver-worker problem is addressed explicitly with command envelopes/wrappers, not hidden inside a Tokio mpsc fork.

**Integration after Phase 1:**

```
Steps:
  1. cargo add deja-tokio deja-actix
  2. Add DejaScope middleware
  3. Replace #[tokio::main] with explicit runtime Builder construction
  4. Call enable_deja_context_hooks() on the Builder
  5. Build with RUSTFLAGS="--cfg tokio_unstable" for Tokio 1.48 hook APIs
  6. Call deja::init() in main
```

**Effort:** ~1 hour for services that own runtime construction; more if `#[tokio::main]` must be unwound.

**Limitation still remaining:** Command-channel workers (fred, Kafka) are not fixed by task hooks alone. The routing task inherits no request context because it was spawned at pool startup, not during a request. Use command-boundary context envelopes/wrappers for those drivers.

---

## Phase 2: Command-boundary adapter library

**Goal:** Generic adapter for I/O drivers that use command-channel workers.

**The pattern (abstracted):**

```rust
/// A work item that carries correlation context across an execution boundary.
pub struct Correlated<T> {
    pub inner: T,
    pub correlation_id: Option<String>,
}

impl<T> Correlated<T> {
    /// Capture current correlation ID and wrap the work item.
    pub fn capture(inner: T) -> Self {
        Self {
            inner,
            correlation_id: deja_tokio::current_correlation_id(),
        }
    }

    /// Execute a closure under the captured correlation scope.
    pub async fn scoped<F, R>(&self, f: F) -> R
    where
        F: Future<Output = R>,
    {
        match &self.correlation_id {
            Some(id) => DEJA_CORRELATION_ID.scope(id.clone(), f).await,
            None => f.await,
        }
    }

    /// Execute a blocking closure under the captured correlation scope.
    pub fn scoped_blocking<F, R>(&self, f: F) -> R {
        match &self.correlation_id {
            Some(id) => DEJA_CORRELATION_ID.sync_scope(id.clone(), f),
            None => f(),
        }
    }
}
```

**Driver adapter strategy:**

For each driver, we need one integration point: **where commands enter the channel**.

### 2a. fred (Redis — current Hyperswitch driver)

Do **not** keep a fred fork as the mainline integration.

Preferred options, in order:

1. Wrap the application's Redis abstraction so context is captured at API/enqueue time.
2. Add or request an upstream fred extension point for command metadata.
3. Emit in-band validation markers to empirically detect ownership drift.
4. Use a local fork only as a temporary spike if no extension point exists.

The required semantic shape is still:

```text
RedisCommandEnvelope {
  command,
  context: deja_context::capture_current(),
}
```

The driver must restore that context only while processing the specific command/frame. fred's cluster routing, backpressure, MOVED/ASK retry logic, and batching make this non-trivial, which is exactly why a long-lived fork should not be the product architecture.

### 2a-alt. redis-rs (Redis — possible Hyperswitch migration target)

There is active discussion about replacing fred with redis-rs. This changes the adapter details but **does not change the boundary classification or the fundamental problem**.

redis-rs has two connection types:

| Type | I/O model | Correlation works? |
|---|---|---|
| `Connection` (deprecated) | Request task writes socket directly | ✅ Same-task — works automatically |
| `MultiplexedConnection` | `mpsc::channel` → background `PipelineSink` → socket | ❌ CommandBoundary — same gap as fred |
| `ConnectionManager` | Wraps `MultiplexedConnection` + reconnect | ❌ Same gap |

**`MultiplexedConnection` is the exact same architectural pattern as fred** — a `PipelineMessage` is enqueued into an `mpsc::channel`, and a spawned `forward(PipelineSink)` future drives the actual socket reads/writes.

```
request task:
  connection.send_packed_command(cmd)
    → PipelineMessage { input, output } into mpsc::channel
    → await oneshot response
    → yield Pending

PipelineSink (background):
  receiver.forward(PipelineSink)
    → start_send(input)  // actual socket write
    → poll_read()        // actual socket read
```

The correlation ID is gone by the time `start_send` writes to the socket.

**Why redis-rs might be easier to integrate than fred:**

1. Simpler internal architecture — no cluster routing, no backpressure `Written` enum, no MOVED/ASK retry
2. One clear envelope type: `PipelineMessage<SinkItem>` where upstream metadata support could live
3. One clear I/O site: `PipelineSink::start_send` + `poll_flush` where scoped context could be restored
4. Smaller upstream proposal surface than fred
5. No pipelining-within-a-single-call complexity (pipelines are explicit `Pipeline` objects, not auto-batched)

**Proposed redis-rs wrapper/upstream metadata shape:**

```rust
// Conceptual envelope near PipelineMessage
struct ContextualPipelineMessage<S> {
    input: S,
    output: PipelineOutput,
    pipeline_response_count: Option<usize>,
    context: deja_context::ContextSnapshot,
}

// At API/enqueue boundary
pub async fn send_packed_command_with_context(&mut self, cmd: &Cmd) -> RedisResult<Value> {
    let context = deja_context::capture_current();
    self.pipeline
        .send_single_with_context(cmd.get_packed_command(), context, self.response_timeout)
        .await
        // ...
}

// In PipelineSink::start_send
fn start_send(mut self: Pin<&mut Self>, msg: PipelineMessage<SinkItem>) -> Result<(), Self::Error> {
    if let Some(id) = msg.deja_correlation_id {
        DEJA_CORRELATION_ID.scope(id, self.as_mut().project().sink_stream.start_send(msg.input));
    } else {
        self.as_mut().project().sink_stream.start_send(msg.input);
    }
    // ...
}
```

**However:** `PipelineSink::start_send` doesn't return a future — it's a synchronous `Sink::start_send` call. So `DEJA_CORRELATION_ID.scope()` can wrap the entire `start_send` call synchronously (it's just a thread-local set + restore), no async scoping needed. This is actually simpler than fred.

**Batching in redis-rs:** redis-rs pipelines are explicit — the caller packs multiple commands into a `Pipeline` object, which is sent as a single `PipelineMessage` with `pipeline_response_count`. So one `PipelineMessage` = one logical operation (single command or explicit pipeline). There's no fred-style auto-pipelining where different requests' commands get batched into one flush. **This means the batching-boundary problem is largely absent in redis-rs.** One `PipelineMessage` naturally has one correlation ID.

**Key takeaway:** Switching from fred to redis-rs does not eliminate the correlation gap, but it makes the adapter simpler:

| Aspect | fred | redis-rs |
|---|---|---|
| Boundary type | CommandBoundary | CommandBoundary (same) |
| Envelope type | `RedisCommand` | `PipelineMessage` |
| I/O site | `write_with_backpressure` + router | `PipelineSink::start_send` |
| Auto-pipelining | Yes — needs flush-before-switch | No — explicit pipelines only |
| Cluster routing | Complex (MOVED/ASK/backpressure) | Not in redis-rs core |
| Patch complexity | ~50 lines + many construction sites | ~30 lines, simpler flow |
| Batching risk | Mixed-request frames in one syscall | Single-request per PipelineMessage |
| Scope at I/O time | Async scope needed (write_frame is async) | Sync scope sufficient (start_send is sync) |

### 2b. r2d2 / bb8 (generic pool adapters)

These don't need driver patches. They hand out connections that the request task uses directly. Same-task I/O works already.

The exception is when a pool uses a background reaper/healthcheck task — those are Phase 3 (system I/O labeling).

### 2c. kafka / rskafka

Similar to fred: producer has a background sender task. Needs command-envelope propagation.

### 2d. tonic / gRPC (HTTP/2 multiplexed)

HTTP/2 is a Phase 2.5 problem (batched/multiplexed transport). The tonic client multiplexes streams over one TCP connection. Same syscall can carry frames for different requests.

Needs either:
- per-stream correlation with flush-before-switch (like fred)
- or logical protocol events instead of raw socket attribution

### 2e. sqlx (async Postgres)

sqlx uses a connection-per-task model under the hood. When a request task acquires a connection, it owns it. **Same-task I/O — should work without an adapter.**

If sqlx uses a background statement-prepare task, that would need Phase 3 treatment.

**Integration after Phase 2:**

```
Steps:
  1. cargo add deja-tokio deja-actix
  2. Add DejaScope middleware
  3. Install upstream Tokio task hooks on the runtime Builder
  4. Call deja::init() in main
  5. For each new driver: check adapter table
```

**Effort:** ~2 hours for any service that owns runtime construction. Redis adds a command-boundary wrapper/upstream-extension decision. Other drivers may need no changes.

---

## Phase 3: Driver adapter table + auto-detection

**Goal:** Make "does this driver need an adapter?" a lookup, not an investigation.

### 3a. Adapter classification table

For every common Rust I/O driver, classify its boundary type:

| Crate | I/O model | Boundary type | Adapter needed? |
|---|---|---|---|
| diesel + bb8 | sync, blocking pool | BlockingBoundary | ✅ Phase 1 (spawn propagation) |
| sqlx | async, connection-per-task | SameTask | ❌ Works automatically |
| fred | command-channel worker | CommandBoundary | ✅ Wrapper/upstream metadata hook |
| redis-rs (`MultiplexedConnection`) | mpsc channel + PipelineSink | CommandBoundary | ✅ Wrapper/upstream metadata hook |
| redis-rs (`Connection`, deprecated) | request-task direct I/O | SameTask | ❌ Works automatically |
| deadpool-redis | wraps fred or redis-rs | CommandBoundary | ✅ Inherited from underlying wrapper/hook |
| rskafka | producer sender task | CommandBoundary | ✅ Wrapper/upstream metadata hook |
| tonic client | HTTP/2 multiplexed | BatchBoundary | ✅ Phase 2d |
| hyper client | HTTP/1.1 per-connection | SameTask | ❌ Works automatically |
| reqwest | HTTP/1.1 per-connection | SameTask | ❌ Works automatically |
| lapin (AMQP) | channel-based | CommandBoundary | ✅ Wrapper/upstream metadata hook |
| deadpool-redis | wraps fred | CommandBoundary | ✅ Inherited from fred wrapper/hook |
| deadpool-postgres | wraps tokio-postgres | SameTask | ❌ Works automatically |
| aws-sdk-rust | HTTP/2 via hyper | BatchBoundary | ⚠️ Needs investigation |
| sentry-sdk | background sender | CommandBoundary | ✅ Phase 2 |

### 3b. Auto-detection at init time

`deja::init()` can scan `Cargo.toml` dependencies and warn:

```text
[deja] Detected fred 8.0 — Redis correlation crosses a command-channel worker.
       Use a Redis wrapper/upstream metadata hook or enable in-band validation markers.

[deja] Detected sqlx 0.7 — async driver, no adapter needed.

[deja] Detected rskafka 0.5 — Kafka correlation adapter not yet available.
       Redis-style correlation for Kafka is a known limitation.
```

This turns "read the docs and guess" into "Déjà tells you what to do."

### 3c. Adapter crate pattern

Each adapter becomes a small, focused crate:

```
crates/
  deja-redis/       # wrapper/upstream metadata integration for Redis clients
  deja-kafka/       # wrapper/upstream metadata integration for Kafka producers
  deja-amqp/        # wrapper/upstream metadata integration for AMQP clients
```

Each follows the same pattern:
1. Wrap or attach metadata to the command/work item
2. Capture context at enqueue
3. Re-scope at I/O execution point
4. Handle batching boundaries

---

## Phase 4: Proc-macro assisted integration

**Goal:** Reduce manual steps to annotation-level.

```rust
#[deja::main]
async fn main() {
    // automatically calls deja::init()
    // automatically registers spawn propagation
}
```

```rust
#[deja::service]
async fn main() {
    // builds the Tokio runtime with upstream Déjà task hooks
}
```

This is lower priority than the driver adapters because Phase 1 hook installation already solves ordinary spawn propagation when the service owns runtime construction. A proc-macro attribute on `main` could reduce boilerplate by building the runtime with the necessary hooks.

---

## Phase 5: Non-Rust runtimes

**Goal:** Make Déjà work for Go, Python, Java services.

This requires a fundamentally different correlation mechanism because:
- No `task_local!` equivalent in most runtimes
- LD_PRELOAD still intercepts syscalls
- But thread-local may be the only option, and it's wrong for goroutines/green threads

Possible approaches:
- Go: `runtime.LockOSThread()` + context propagation via `context.Context`
- Python: `threading.local()` + `contextvars`
- Java: ThreadLocal + OpenTelemetry context propagation APIs

This is a separate product decision, not a technical extension of the Rust work.

---

## Integration effort by phase

| Phase | New service effort | What's manual | What's automatic |
|---|---|---|---|
| Phase 0 (now) | 2 days | spawn replacement, driver patches | LD_PRELOAD hooks, FFI bridge, actix middleware |
| Phase 1 | 1 hour | Add Cargo.toml patches | Spawn propagation, blocking pool propagation |
| Phase 2 | 2 hours | Add driver-specific patches | fred/kafka correlation, batch boundaries |
| Phase 3 | 30 minutes | Read adapter table warnings | Auto-detection, adapter guidance |
| Phase 4 | 5 minutes | Add `#[deja::main]` | Everything else |
| Phase 5 | TBD | Runtime-specific adapters | LD_PRELOAD hooks |

---

## What to build first

### Immediate (next sprint)

1. **Phase 1: upstream Tokio hook propagation**
   - Eliminates the biggest manual integration cost without a Tokio fork
   - Requires explicit runtime Builder setup and `--cfg tokio_unstable` on Tokio 1.48
   - Unblocks services that only use same-task + spawned async I/O
   - Keep `deja_tokio::spawn_blocking` for blocking closures

2. **Redis driver adapter (Phase 2a or 2a-alt)**
   - The one driver every Hyperswitch-like service needs
   - Proves the contextual command-envelope pattern works end-to-end
   - **If Hyperswitch stays on fred:** prefer wrapper/upstream command metadata over a fork
   - **If Hyperswitch migrates to redis-rs:** prefer a redis-rs wrapper/upstream metadata hook — simpler, no auto-pipelining, sync scope at I/O time
   - Also solves the batching boundary for Redis (more easily with redis-rs)

### Next

3. **Adapter classification table (Phase 3a)**
   - Documentation, not code
   - Lets users self-serve instead of asking us

4. **Auto-detection warnings (Phase 3b)**
   - Small `deja::init()` enhancement

5. **rskafka-deja or lapin-deja (Phase 2c/2e)**
   - Depends on which drivers our target services actually use

### Later

6. **Proc-macro integration (Phase 4)**
   - Polish, not necessity

7. **Non-Rust runtimes (Phase 5)**
   - Separate product decision

---

## Upstream aspiration

The Rust async ecosystem has no standard mechanism for context propagation across spawn boundaries. Every observability tool (OTel, tracing, us) reinvents wrappers.

Our hook-only runtime integration is a concrete data point for what first-class Tokio context propagation could look like. Once we've proven it in production, we should:

1. Write an RFC for tokio
2. Get OTel Rust SDK team involved (they need the same thing)
3. Propose a `SpawnContext` trait as a standard interface

If Tokio natively supports stable context propagation, Phase 1 becomes "enable stable hooks/context propagation" instead of "compile with tokio_unstable hooks." That's the endgame for spawn boundaries.

For command-boundary drivers, the endgame is different: fred (and similar drivers) should support user-defined command metadata natively. That's a feature request to each driver, not a tokio change.

---

## The scalability thesis

| Claim | Evidence |
|---|---|
| Same-task I/O needs zero integration | Postgres with sqlx, HTTP with hyper — works today |
| Spawn boundaries are solvable once | tokio patch covers all `spawn`/`spawn_blocking` |
| Command boundaries need per-driver adapters | But the pattern is the same: `Correlated<T>` |
| Batch boundaries add flush-before-switch | Same pattern, just more careful about write batching |
| Most Rust web services use 2-3 I/O drivers | So we need 2-3 driver adapters, not 50 |

The integration surface is bounded. It's not "every crate needs a patch." It's "every I/O-driver-with-a-background-worker-task needs one well-understood adapter, and here is the catalog."
