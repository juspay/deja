# Tri-System Synthesis: Déjà, Speedscale, AREX

**Comparative Analysis of Record/Replay Architectures**

**Date:** 2026-05-08  
**Status:** Final synthesis for product positioning and technical roadmap

---

## Executive Summary

Three distinct approaches to record/replay testing have been analyzed:

| System | Core Philosophy | Best For | Key Limitation |
|--------|-----------------|----------|----------------|
| **Déjà** | Causal correctness + state-seeded owned dependencies | Isolated replay of stateful systems | Fixture synthesis complexity |
| **Speedscale** | Traffic replay, operational simplicity | API contract testing, chaos engineering | No causal ordering guarantees |
| **AREX** | Java-native, bytecode instrumentation | Java enterprise regression testing | Java-locked, sequential replay |

**Déjà's opportunity**: Both Speedscale and AREX use signature-based or sequential matching—not causal correlation and not per-request state-seeded replay. This leaves a gap for Déjà to own the "correct isolated replay of stateful mutable systems" niche.

---

## 1. Problem Statement: Why Causality Matters

### 1.1 The Repeated Read Ambiguity

Consider a minimal case from Hyperswitch payment processing:

```rust
// Within a single request handler
let token_data = redis.get(&format!("pm_token_{}", pm_id)).await?;     // Read v1
process_payment(&token_data).await?;
redis.del(&format!("pm_token_{}", pm_id)).await?;                      // Delete
let verify_deleted = redis.get(&format!("pm_token_{}", pm_id)).await?; // Read v2 (nil)
```

Both reads have identical signatures:
- Same Redis key: `pm_token_12345`
- Same command: `GET`
- Different results due to causal ordering

### 1.2 How Each System Handles This

| System | Matching Strategy | Expected Behavior | Risk |
|--------|-------------------|-------------------|------|
| **Déjà** (planned) | Prefer state-seeded Redis/DB; fallback to causal scope + ordinal | Real Redis/DB returns v1/v2 from seeded pre-state and candidate writes | Fixture synthesis complexity |
| **Speedscale** | Signature + vUser sequence | May return arbitrary match; ordering by time | Silent incorrect replay |
| **AREX** | Signature + sequential | Returns first-unmatched; ok if temporal==causal | Fails if replay timing differs |

### 1.3 Evidence from Hyperswitch

Static analysis found **56 Redis read sites** and **193 SQL query sites** that can exhibit this pattern:

```
File: storage_impl/src/redis/kv_store.rs:77
Pattern: get_or_populate_redis() → read-miss-populate-read cycle
Risk: Two reads of same key with intervening write

File: router/src/core/payment_methods/vault.rs:45  
Pattern: retrieve_and_delete_cvc_from_payment_token()
Risk: Read then delete then verification read

File: storage_impl/src/payments/payment_intent.rs:228
Pattern: populate_and_get_intent_with_id()
Risk: Status transition reads at multiple lifecycle points
```

Full analysis: `docs/HYPERSWITCH_REPLAY_AMBIGUITY_REPORT.md`

---

## 2. Architectural Comparison

### 2.1 Capture Mechanisms

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          CAPTURE ARCHITECTURES                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  DÉJÀ                              SPEEDSCALE                     AREX       │
│  ═════                             ══════════                     ═════      │
│                                                                              │
│  ┌─────────────┐                   ┌─────────────┐               ┌─────────┐ │
│  │ Application │                   │ Application │               │   JVM   │ │
│  │  (any lang) │                   │  (any lang) │               │  (Java) │ │
│  └──────┬──────┘                   └──────┬──────┘               └────┬────┘ │
│         │                                  │                          │      │
│  ┌──────▼──────┐                   ┌──────▼──────┐               ┌────▼────┐ │
│  │   libc      │                   │   Network   │               │Bytecode │ │
│  │  syscalls   │◄────intercept────►│   Layer     │               │Advice   │ │
│  └──────┬──────┘                   │  (envoy/ebpf)│              └────┬────┘ │
│         │                          └──────┬──────┘                   │      │
│  ┌──────▼──────┐                          │                     ┌────▼────┐ │
│  │ LD_PRELOAD  │                   ┌──────▼──────┐               │ ByteBuddy│ │
│  │   Agent     │                   │  Sidecar    │               │ Agent    │ │
│  └─────────────┘                   └─────────────┘               └─────────┘ │
│                                                                              │
│  Language: Universal              Language: Universal           Lang: Java   │
│  Granularity: Syscall             Granularity: Packet         Grain: Method │
│  Deploy: Env var                  Deploy: K8s sidecar          Dep: JVM opt │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Context Propagation

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CONTEXT PROPAGATION MODELS                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  DÉJÀ                              SPEEDSCALE                     AREX       │
│  ═════                             ══════════                     ═════      │
│                                                                              │
│  tokio::task_local!                NONE (proxy-level)           ThreadLocal │
│         │                                                        + Wrappers  │
│         ▼                                                                   │
│  ┌─────────────┐                                             ┌─────────────┐ │
│  │  scope()    │                                             │RunnableWrap │ │
│  │   await     │                                             │Constructor  │ │
│  └──────┬──────┘                                             └──────┬──────┘ │
│         │                                                          │        │
│         ▼                                                          ▼        │
│  Request task owns              No request context              Captures ctx│
│  all work within                at capture time               at wrap time  │
│  the scope                                                                  │
│                                                                              │
│  Challenge: Pool-mediated I/O    Challenge: Inter-request     Challenge:    │
│  (fred Redis, DB pools)          correlation impossible       ForkJoinPool │
│  Solution: Command metadata      Solution: None (by design)   Solution:     │
│  tagging with correlation ID                                    Explicit wrap│
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.3 Replay Matching Strategies

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         REPLAY MATCHING HIERARCHY                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  DÉJÀ (planned)                                                            │
│  ═══════════════                                                           │
│  1. State-seeded real DB/Redis for owned dependencies                      │
│  2. Stateful emulator where real dependency is not practical               │
│  3. Causal scope + local_sequence + signature                              │
│  4. Resource key + version/cursor                                          │
│  5. Global/connection sequence (fallback)                                  │
│  6. Per-signature FIFO                                                     │
│  7. Signature-only + warning                                               │
│                                                                              │
│  SPEEDSCALE                                                                │
│  ══════════                                                                │
│  1. Request signature (host/method/path/body/query/hash)                   │
│  2. vUser sequential assignment (replay-time only)                         │
│                                                                              │
│  AREX                                                                      │
│  ═════                                                                     │
│  1. ACCURATE: operationName + requestBody hash                             │
│  2. FUZZY: First-unmatched in temporal order                               │
│  3. EIGEN: Feature-based (unimplemented)                                   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Detailed Capability Matrix

### 3.1 Core Capabilities

| Capability | Déjà | Speedscale | AREX |
|------------|------|------------|------|
| **Zero code change** | ✅ `LD_PRELOAD` | ✅ Sidecar injection | ✅ `-javaagent` |
| **Language agnostic** | ✅ Any libc binary | ✅ Any TCP traffic | ❌ Java only |
| **TLS capture** | ⚠️ Planned | ✅ Termination | ⚠️ JVM SSL hooks |
| **gRPC/HTTP2** | 🔄 In progress | ✅ Supported | ✅ Supported |
| **Async/await native** | ✅ Tokio integration | N/A (packet) | ⚠️ Thread-wrap |

### 3.2 Causality and Ordering

| Aspect | Déjà | Speedscale | AREX |
|--------|------|------------|------|
| **Capture-time causality** | ✅ `DEJA_CORRELATION_ID` | ❌ None | ⚠️ Thread-local only |
| **Cross-request correlation** | ✅ Scope nesting | ❌ No | ❌ Per-thread only |
| **Pool-mediated I/O tracking** | 🔄 Planned (driver) | ✅ Proxy sees all | ⚠️ Wrapper-dependent |
| **Request→dependency mapping** | ✅ Direct attribution | ❌ Signature only | ⚠️ Implicit via thread |
| **Mutable read disambiguation** | 🔄 State-seeded owned deps + ordinal fallback planned | ❌ Not supported | ⚠️ Sequential only |

### 3.3 Operational Characteristics

| Aspect | Déjà | Speedscale | AREX |
|--------|------|------------|------|
| **Storage backend** | Local files (JSONL) | SaaS / Object storage | MongoDB + Redis |
| **Deployment complexity** | Low (env var) | Medium (K8s) | Medium (services) |
| **CI/CD integration** | Native CLI | API-based | API-based |
| **On-premise capable** | ✅ Always | ⚠️ Enterprise | ✅ Self-hosted |
| **Operational cost** | File storage | Per-capture pricing | DB + cache infra |

---

## 4. Trade-off Analysis

### 4.1 Déjà's Strengths and Weaknesses

**Strengths:**
1. **Universal capture** — Works for Go, Rust, Python, Node.js, C++, etc.
2. **Causal correctness potential** — Only system designed for capture-time request attribution
3. **File-based simplicity** — No external dependencies for local development
4. **Structured concurrency alignment** — Matches modern async Rust patterns

**Weaknesses:**
1. **Implementation complexity** — Correlation propagation requires driver integrations
2. **Protocol detection** — Must parse bytes; no rich method context like ByteBuddy
3. **TLS blindness** — Cannot see encrypted traffic without MITM
4. **Early stage** — Less mature than Speedscale/AREX in production

### 4.2 Speedscale's Strengths and Weaknesses

**Strengths:**
1. **Operational simplicity** — Sidecar pattern is well-understood in K8s
2. **Traffic shaping** — Built-in transforms, delays, chaos engineering
3. **SaaS convenience** — Managed infrastructure, no DB maintenance
4. **Broad protocol support** — HTTP, gRPC, DB protocols via proxy

**Weaknesses:**
1. **No causal ordering** — Cannot distinguish same-signature operations in causal order
2. **Cloud dependency** — Full features require SaaS connectivity
3. **Sidecar overhead** — Additional hop for all traffic
4. **Coarse granularity** — Packet-level misses application context

### 4.3 AREX's Strengths and Weaknesses

**Strengths:**
1. **Rich Java context** — ByteBuddy captures generics, parameter names, annotations
2. **Established ecosystem** — 40+ instrumentation modules
3. **Explicit sequence handling** — Recognizes repeated-call problem
4. **Standalone mode** — All-in-one JAR for local testing

**Weaknesses:**
1. **Java lock-in** — Cannot capture other languages
2. **Sequential != causal** — Temporal ordering fails when timing varies
3. **Thread-local fragility** — Async/await requires careful wrapping
4. **Infrastructure burden** — Requires MongoDB + Redis deployment

---

## 5. Positioning and Strategy

### 5.1 Market Segmentation

```
                    High precision required
                           ▲
                           │
                           │     ┌─────────┐
                           │     │  DÉJÀ   │ ← Stateful systems,
                           │     │ ★       │   correctness-critical
                           │     └─────────┘
                           │
    ┌─────────────┐        │        ┌─────────────┐
    │  AREX       │        │        │  Speedscale │
    │  Java       │◄───────┴───────►│  General    │
    │  enterprise │   Flexible      │  purpose    │
    └─────────────┘   deployment    └─────────────┘
                           
         Java only ◄───────┬───────► Universal
                           │
                    Language scope
```

### 5.2 Déjà's Recommended Positioning

**Primary:** "Deterministic replay for stateful distributed systems"

**Differentiators to emphasize:**
1. **Causal correctness** — Only system that tracks which request caused which dependency call
2. **Universal capture** — One solution for polyglot microservices (Rust, Go, Python, Node.js)
3. **Correctness verification** — Replay detects not just changes, but ordering violations

**Target use cases:**
- Payment systems (Hyperswitch-class correctness requirements)
- Distributed transaction processing
- State machine replication testing
- Multi-service regression suites

**Avoid competing on:**
- Operational simplicity (Speedscale wins)
- Java ecosystem richness (AREX wins)
- Chaos engineering features (Speedscale wins)

### 5.3 Technical Roadmap Implications

Based on this analysis, Déjà should prioritize:

| Priority | Feature | Justification |
|----------|---------|---------------|
| **P0** | Per-request state fixture inference | Enables isolated replay without DB/Redis response lookup ambiguity |
| **P0** | Correlation 2.0 (causal scope + ordinal) | Required to build fixtures and validate side effects; neither competitor has this |
| **P0** | Redis state-seeded replay | Highest-confidence owned dependency fixture target |
| **P0** | Fred Redis driver integration | Required for causal correctness with pool-mediated I/O |
| **P1** | SQL confidence-tiered fixture synthesis | Handles exact row reads first, warns on underdetermined queries |
| **P1** | Ambiguity detector | Educate users when signature-only replay would fail |
| **P1** | Hybrid replay orchestrator | Mock externals, seed owned state, deterministic time/random/env |
| **P2** | gRPC/HTTP2 parsers | Table stakes for modern services |
| **P2** | TLS interception | Required for production HTTPS capture |

---

## 6. Validation: Test Cases That Expose Differences

### 6.1 Test: Out-of-Order Mutable Reads

```rust
// Scenario: Two concurrent requests modifying shared state

// Request A
spawn(async {
    let v1 = redis.get("balance:user_123").await?;  // 100
    redis.set("balance:user_123", v1 + 50).await?;  // 150
    let v2 = redis.get("balance:user_123").await?;  // 150
});

// Request B (interleaved)
spawn(async {
    let w1 = redis.get("balance:user_123").await?;  // 100 or 150?
    // ... operation ...
});
```

**Expected outcomes:**
- **Déjà**: Correctly replays based on causal order, not temporal interleaving
- **Speedscale**: May return wrong balance depending on vUser assignment
- **AREX**: Depends on thread scheduling; may diverge under replay

### 6.2 Test: Pool-Mediated I/O Attribution

```rust
// Scenario: Shared Redis connection pool

let pool = RedisPool::new();

spawn(async {
    let conn = pool.get().await;
    conn.send(Command::Get("key")).await;  // Submitted first
    // ... other work ...
    conn.recv_response().await;            // Received second
});
```

**Expected outcomes:**
- **Déjà**: With driver integration, correlates response to correct request
- **Speedscale**: Sees packet flow, no attribution challenge
- **AREX**: Depends on Lettuce/Jedis wrapper correctness

### 6.3 Test: Cross-Session State

```rust
// Scenario: Payment method ID persists across API calls

// Session 1: Create payment method → returns pm_id_123
// Session 2: Use pm_id_123 for charge
// Session 3: Delete pm_id_123
// Session 4: Verify pm_id_123 returns 404
```

**Expected outcomes:**
- **Déjà**: Tracks resource lifecycle across sessions
- **Speedscale**: Treats each session independently; replay may fail
- **AREX**: Per-test-case replay; cross-session state not modeled

---

## 7. References

### Documents

| File | Description |
|------|-------------|
| `docs/AREX_ANALYSIS.md` | Deep-dive into AREX architecture |
| `docs/SPEEDSCALE_COMPARISON.md` | Detailed Speedscale comparison |
| `docs/SPEEDSCALE_INSIGHTS.md` | Strategic synthesis on causality |
| `CORRELATION_ARCHITECTURE.md` | Déjà's correlation design |
| `HYPERSWITCH_REPLAY_AMBIGUITY_REPORT.md` | Real-world evidence |
| `ROADMAP.md` | Implementation priorities |

### External Sources

| System | Repository | Commit Analyzed |
|--------|------------|-----------------|
| AREX Agent | `github.com/arextest/arex-agent-java` | `fe4e31407c4b85e610b95cca3613279c8d3bb18b` |
| AREX Storage | `github.com/arextest/arex-storage` | `b2384254662917fc57ab82098f7f0017e628c47e` |
| AREX Schedule | `github.com/arextest/arex-replay-schedule` | `99d9f822d374218b978cbbec5072196e2be099cb` |
| Speedscale | `github.com/speedscale` | Public docs + repos |

---

## 8. Conclusion

The record/replay market has two established players with significant gaps:

1. **Speedscale** prioritizes operational simplicity over correctness — no causal tracking
2. **AREX** provides Java-native instrumentation but uses sequential not causal matching

**Déjà's opportunity** is to own the "correct replay of stateful systems" segment by:

1. Implementing true capture-time causal correlation
2. Maintaining universal language support via `LD_PRELOAD`
3. Providing correctness verification beyond mere diffing

The Hyperswitch analysis proves this is a real problem (56+ Redis read sites, 193+ SQL query sites with ambiguity potential). Neither Speedscale nor AREX can correctly replay all these scenarios.

Déjà's correlation architecture, combined with driver-level integration for pool-mediated I/O, represents a genuine technical differentiation in a crowded market.

---

**End of Document**
