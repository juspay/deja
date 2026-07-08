# Speedscale Research: Strategic Insights for Déjà

**Date:** 2026-05-08  
**Purpose:** Consolidated analysis from competitive research and architectural discussions

---

## 1. The Core Problem Déjà Solves

**Boundary-First Semantic Replay:** Capture all external I/O (network, time, randomness, environment) at the syscall boundary, then replay it deterministically to validate behavioral consistency.

**The Hard Part:** When an async application handles multiple concurrent requests, each request triggers I/O operations. We need to know **which request caused which I/O** to:
- Validate that replay exercises the same dependency paths
- Detect when code changes alter I/O ordering
- Debug what a specific request touched

**Example:** If Request A does `SET x` then `SET y`, but a code change reverses the order, we must detect this. Signature-based matching (Speedscale) cannot.

---

## 2. Why We Studied Speedscale

Speedscale is the closest comparable production-grade traffic replay system. Understanding their approach helps us:
- Validate our differentiation (where we're better)
- Identify gaps to address (where they're ahead)
- Borrow proven patterns (deployment mechanisms, UX)

**Key Finding:** Speedscale and Déjà solve different problems with different trade-offs. Neither is strictly superior—they optimize for different constraints.

---

## 3. eBPF vs LD_PRELOAD: Mechanism Comparison

| Dimension | eBPF (Speedscale Primary) | LD_PRELOAD (Déjà) |
|-----------|---------------------------|-------------------|
| **Execution** | Kernel space | User space (same process) |
| **Permissions** | `CAP_BPF` or root | None required |
| **Static Binaries** | ✅ Works | ❌ LD_PRELOAD ignored |
| **Go/Rust Support** | ✅ Universal | ⚠️ Needs dynamic linking |
| **Can Modify Syscalls?** | Observe only (limited override) | ✅ Full interception |
| **Deterministic Boundaries** | ❌ Cannot intercept getrandom/clock | ✅ Yes (time, randomness, env) |
| **Deployment Complexity** | High (kernel headers, BTF, CO-RE) | Low (shared library) |
| **Production Suitability** | Requires privilege escalation | Zero-config, unprivileged |
| **Performance** | Very low overhead (JIT) | Low (function call) |

**Critical Difference for Replay:**
- eBPF observes what happened (cannot change it)
- LD_PRELOAD can substitute return values (deterministic replay)

---

## 4. Correlation: The Decisive Differentiator

### 4.1 What Speedscale Actually Does

**Claim:** "Full correlation via snapshot grouping"  
**Reality:** No causal tracking at all

Speedscale uses **signature-based matching:**
```
Recording: [SET x → OK] [SET y → OK]
Replay:    App sends SET y → Matches [SET y] → Returns OK
           App sends SET x → Matches [SET x] → Returns OK
Result:    ✓ PASS (but order was reversed!)
```

**Cannot detect:** Ordering changes, state-dependent operations, race conditions.

### 4.2 What Déjà Does

**Causal tracking via spawn-time context propagation:**
```
Request A scope("req-a"):
  └── spawn task (inherits "req-a")
      └── SET x (tagged "req-a")
      └── SET y (tagged "req-a")

Recording: [req-a: SET x] [req-a: SET y]
Replay:    [req-a: SET y] [req-a: SET x]
Result:    ✗ ORDER DIVERGENCE DETECTED
```

**Can detect:** Ordering violations, state changes, side-effect sequences.

### 4.3 Why This Matters

| Scenario | Speedscale | Déjà |
|----------|------------|------|
| Independent reads | ✓ Works | ✓ Works |
| Idempotent writes | ✓ Works | ✓ Works |
| Transaction ordering | ✗ Misses | ✓ Catches |
| Counter increments | ✗ Misses | ✓ Catches |
| Race conditions | ✗ Misses | ✓ Catches |
| State machine validation | ✗ Misses | ✓ Catches |

---

## 5. Generic Correlation Solutions

The multiplexing problem (fred Redis, connection pools) breaks naive task-local inheritance. Here are the abstraction layers, from low-level to high-level:

### 5.1 Low-Level Fundamental: Spawn-Time Context Capture

**Core Insight:** Causality is determined at **SUBMISSION TIME**, not execution time.

```rust
/// The primitive: capture origin when work is submitted
pub struct Annotated<W> {
    work: W,
    origin: ContextSnapshot,  // Captured at spawn/enqueue
}

/// Submit work from request scope → origin captured
fn submit_to_pool<P, W>(pool: &P, work: W) -> Result<()>
where P: WorkQueue<W> {
    let annotated = Annotated {
        work,
        origin: capture_current_context(),  // ← HERE
    };
    pool.enqueue(annotated)
}

/// Pool executor restores context before executing
impl<W> WorkQueue<W> for ThreadPool {
    fn dequeue(&self) -> Annotated<W> {
        let annotated = self.rx.recv();
        restore_context(annotated.origin);  // ← RESTORE HERE
        annotated.work.execute()
    }
}
```

**Applies to:** tokio::spawn, spawn_blocking, mpsc channels, connection pools, worker queues.

### 5.2 Mid-Level: Language-Specific Integration

For Rust/Tokio specifically:

```rust
// Task-local that survives work-stealing
tokio::task_local! { static CORRELATION_ID: String; }

// Scoped execution
CORRELATION_ID.scope(id, async { /* I/O here is tagged */ }).await;

// Spawn wrappers that propagate
pub fn spawn<F>(f: F) -> JoinHandle<F::Output> {
    let parent_id = CORRELATION_ID.try_with(|id| id.clone()).ok();
    tokio::spawn(async move {
        match parent_id {
            Some(id) => CORRELATION_ID.scope(id, f).await,
            None => f.await,
        }
    })
}
```

**Requirements:** Library cooperation or wrapper insertion at spawn points.

### 5.3 High-Level: Framework Integration

**Actix-web middleware pattern (implemented):**
```rust
impl<S, B> Service<ServiceRequest> for DejaScopeMiddleware<S> {
    fn call(&self, req: ServiceRequest) -> Self::Future {
        let correlation_id = extract_from_request(&req);
        let inner = self.service.call(req);
        
        Box::pin(async move {
            DEJA_CORRELATION_ID
                .scope(correlation_id, inner)  // ← Scope entire handler
                .await
        })
    }
}
```

**Requirements:** Framework hooks at request entry/exit.

### 5.4 Universal Fallback: Retroactive Reconstruction

When perfect correlation is impossible (third-party libraries, external services):

```rust
/// Reconstruct causality from event stream
pub struct CausalityEngine {
    events: Vec<RawEvent>,  // From eBPF or hooks
}

impl CausalityEngine {
    pub fn reconstruct(&self) -> Vec<RequestTree> {
        // Signals:
        // 1. Temporal proximity (what happened before what)
        // 2. Connection ownership (which fd belongs to which request)
        // 3. Data flow (value from response appears in subsequent request)
        // 4. Protocol markers (X-Request-ID, SQL comments, etc.)
        
        // Output: Best-effort attribution with confidence scores
    }
}
```

**Use case:** Post-processing artifacts where real-time tagging failed.

---

## 6. Async-Specific Challenges & Solutions

### 6.1 The Two Categories of Async I/O

| Category | Pattern | Correlation Strategy | Examples |
|----------|---------|---------------------|----------|
| **Request-Owned** | Task spawned during request, lives with request | Task-local inheritance | Handler futures, per-request DB queries |
| **Pool-Mediated** | Work submitted to pre-existing pool | Annotated work submission | fred Redis, deadpool, bb8, connection pools |

### 6.2 Request-Owned: Solved

Already working:
- `tokio::spawn` → `deja_tokio::spawn` (wrapper)
- `spawn_blocking` → `deja_tokio::spawn_blocking` (wrapper)
- Actix middleware → `DEJA_CORRELATION_ID.scope()`

### 6.3 Pool-Mediated: The Hard Problem

**Fred Redis example:**
```rust
// Problem: fred's routing task spawned at pool creation
Pool::new() → spawns RouterTask { loop { rx.recv(); socket.write(cmd); } }

// Request uses pool:
Request A → pool.set("x", 1) → channel.send(cmd) → RouterTask writes

// RouterTask has NO request context (spawned before any request)
```

**Solution: Annotated Commands**
```rust
// Wrap fred (or patch fred)
impl DejaRedisPool {
    async fn set(&self, key: &str, value: &str) {
        let cmd = RedisCommand {
            cmd: Set(key, value),
            correlation_id: current_correlation_id(),  // ← CAPTURE
        };
        self.inner.send(cmd).await
    }
}

// RouterTask restores context:
loop {
    let cmd = rx.recv();
    if let Some(id) = cmd.correlation_id {
        DEJA_CORRELATION_ID.scope(id, socket.write(cmd)).await;
    }
}
```

**Generic abstraction:**
```rust
pub trait CausalWorkQueue<W> {
    fn submit(&self, work: W) -> impl Future<Output = Result<()>>;
}

pub trait CausalExecutor {
    fn execute_with_origin<W, R>(
        &self, 
        annotated: Annotated<W>
    ) -> impl Future<Output = R>;
}
```

---

## 7. Strategic Implications

### 7.1 Déjà's Differentiation (Maintain)

| Differentiator | Why It Matters | Competitive Moat |
|---------------|----------------|------------------|
| **Causal tracking** | Catches ordering/state bugs signature matching misses | High engineering investment to replicate |
| **Deterministic boundaries** | Time, randomness, env control for perfect replay | Requires LD_PRELOAD (not eBPF) |
| **Zero permissions** | Runs in any environment without privilege escalation | eBPF competitors require CAP_BPF |
| **Ground-truth verification** | pcap-verified 100% byte fidelity | Establishes trust in capture quality |

### 7.2 Speedscale Patterns to Adopt

| Pattern | Déjà Equivalent | Priority |
|---------|-----------------|----------|
| **Markdown RRPair format** | `--format markdown` in `deja inspect` | Medium — improves human/LLM readability |
| **Transform pipeline** | `deja-transform` crate | High — enables practical replay (JWT rotation, timestamp shifts) |
| **Generator/Responder split** | `deja mock-server` command | Medium — dependency mocking without full replay |
| **Variable cache** | Transform-scoped storage | Medium — share data between requests |
| **Kubernetes operator** | Admission webhook + LD_PRELOAD injection | Low — enterprise deployment option |

### 7.3 What We Won't Do

| Approach | Why Not | Alternative |
|----------|---------|-------------|
| Switch to eBPF | Loses deterministic replay capability | Hybrid: eBPF for capture where available, LD_PRELOAD for replay |
| Signature-only matching | Loses ordering validation | Keep causal tracking as primary mechanism |
| Mock owned DB/Redis reads forever | Reintroduces repeated-read ambiguity for isolated replay | Seed isolated owned dependency state, then validate writes/final state |
| Production K8s operator | Increases deployment complexity | Stay CLI-first, optional operator later |

---

## 8. Recommended Implementation Path

### Phase 1: Consolidate Causal Tracking (Current)

1. **Document the abstraction** — `Annotated<W>`, `CausalWorkQueue`, `CausalExecutor`
2. **Tier 1 drivers** — `deja-tokio-postgres`, `deja-tokio-redis` (full causality)
3. **Tier 2 drivers** — Wrapper libraries for popular crates (best-effort)
4. **Honest signaling** — Mark unattributed I/O with confidence scores

### Phase 2: State-Seeded Owned Dependency Replay (Next)

For isolated request replay, prefer real seeded Redis/Postgres over mocked DB/Redis read responses:
- infer per-request pre-state facts from pre-write reads
- seed isolated Redis/Postgres instances or namespaces
- run candidate against real dependencies
- validate ordered write logs and final touched-state slices
- keep response mocking for external APIs

See `docs/STATE_SEEDED_REPLAY.md`.

### Phase 3: Transform System

Critical for practical replay:
- JWT rotation (expired tokens break replay)
- Timestamp shifting (time-sensitive assertions)
- Dynamic data replacement (session IDs, CSRF tokens)

### Phase 4: Hybrid Capture (Future)

Optional eBPF component for environments where LD_PRELOAD is problematic:
- eBPF captures raw bytes (universal coverage)
- LD_PRELOAD provides correlation + deterministic replay
- Combine in post-processing

---

## 9. Key Terminology Alignment

| Concept | Déjà | Speedscale | Notes |
|---------|------|------------|-------|
| **Capture unit** | `BoundaryEvent` | `RRPair` | Déjà separates boundaries (time/random) from I/O |
| **Collection** | `Artifact` | `Snapshot` | |
| **Correlation** | Task-local + FFI | None | **Critical differentiator** |
| **Replay driver** | In-process substitution | Generator + Responder | Speedscale separates; Déjà embeds |
| **Matching** | State-seeded owned deps + causal fallback | Signature (content similarity) | **Fundamentally different** |
| **Ordering detection** | Yes | No | Déjà catches swaps; Speedscale misses |

---

## 10. Summary

**The Core Insight:** Causal tracking is hard but necessary for rigorous replay. Speedscale bypasses this complexity by not attempting it. Déjà embraces the complexity because ordering and state validation are critical for correctness.

**Additional Insight:** For owned DB/Redis, the strongest isolated replay model is not endless response mocking. It is per-request state seeding: reconstruct the initial state slice, run candidate code against real dependencies, then validate response, ordered writes, and final state.

**The Trade-off:** Speedscale is easier to deploy broadly (eBPF) but misses important bug classes. Déjà is harder to make universal (requires library integration and fixture synthesis) but catches real problems.

**The Path Forward:** 
1. Double down on causal tracking as the differentiating feature
2. Use correlation to build per-request Redis/Postgres fixtures
3. Make propagation generic via `Annotated<W>` abstractions
4. Add transforms for practical replay viability
5. Consider eBPF only as capture augmentation, never replacement

---

*Research basis:* Speedscale docs (github.com/speedscale/docs), BCC/libbpf eBPF references, Déjà architecture docs (ARCHITECTURE.md, CORRELATION_ARCHITECTURE.md, ROADMAP.md).
