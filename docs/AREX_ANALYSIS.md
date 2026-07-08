# AREX Architecture Deep-Dive and Comparative Analysis

**Date:** 2026-05-08  
**Commit references:**
- `arex-agent-java`: `fe4e31407c4b85e610b95cca3613279c8d3bb18b`
- `arex-storage`: `b2384254662917fc57ab82098f7f0017e628c47e`
- `arex-replay-schedule`: `99d9f822d374218b978cbbec5072196e2be099cb`
- `arex-compare-sdk`: `8616d9f18a9f8ea251a014214528439587667b44`

---

## Executive Summary

AREX is an open-source Java record/replay testing platform that uses **ByteBuddy-based Java agent instrumentation** to intercept application behavior at the bytecode level. While AREX shares Déjà's goal of zero-code-change testing, it differs significantly in:

1. **Capture mechanism**: ByteBuddy instrumentation vs Déjà's `LD_PRELOAD`
2. **Context propagation**: Thread-local inheritance with wrapping vs Déjà's Tokio task-locals
3. **Replay matching**: Sequential consumption with signature fallback vs Déjà's planned causal correlation
4. **Storage architecture**: MongoDB + Redis cache vs Déjà's streaming file-based capture

Critically, AREX uses **signature-based matching with sequential fallback**, not true causal correlation—similar to Speedscale but with explicit sequence-awareness.

---

## 1. Capture Mechanism: ByteBuddy Instrumentation

### 1.1 Instrumentation Targets

AREX instruments via Java agent ByteBuddy advices at these key points:

| Component | Entry Point | Class Path |
|-----------|-------------|------------|
| HTTP Entry | Servlet V3 Filter/Service | `arex-httpservlet/ServletInstrumentationV3.java` |
| HTTP Entry | Netty Provider | `arex-netty-v4/ChannelPipelineInstrumentation.java` |
| Database | MyBatis Executor | `arex-database-mybatis3/ExecutorInstrumentation.java` |
| Database | Hibernate | `arex-database-hibernate/AbstractEntityPersisterInstrumentation.java` |
| Redis | Jedis Wrapper | `arex-jedis-v4/JedisWrapper.java` |
| Redis | Lettuce Wrapper | `arex-lettuce-v6/RedisCommandWrapper.java` |
| RPC | Dubbo Consumer | `arex-dubbo/dubbo-*-instrumentation` |
| Async | ThreadPool | `arex-executors/ThreadPoolInstrumentation.java` |
| Dynamic | Custom Classes | Configured via `dynamic.packages` |

### 1.2 ByteBuddy Pattern Example: Servlet Entry

```java
// From: arex-httpservlet/ServletInstrumentationV3.java
@Override
public ElementMatcher<TypeDescription> typeMatcher() {
    return safeHasSuperType(named("javax.servlet.http.HttpServlet"));
}

@Override
public void transform(TypeTransformer transformer) {
    transformer.applyAdviceToMethod(
        named("service")
            .and(takesArgument(0, named("javax.servlet.http.HttpServletRequest")))
            .and(takesArgument(1, named("javax.servlet.http.HttpServletResponse"))),
        this.getClass().getName() + "$ServiceAdvice"
    );
}

@Advice.OnMethodEnter(suppress = Throwable.class)
public static void onEnter(
    @Advice.Argument(0) HttpServletRequest request,
    @Advice.Local("extractor") ServletExtractor extractor
) {
    // Trace context initialization happens here
    extractor = ServletAdviceHelper.onServiceEnter(request);
}
```

### 1.3 Comparison: ByteBuddy vs LD_PRELOAD

| Aspect | AREX (ByteBuddy) | Déjà (LD_PRELOAD) |
|--------|------------------|-------------------|
| **Language Support** | Java only | Any language using libc |
| **Granularity** | Method-level via bytecode | System call level |
| **Deployment** | JVM `-javaagent` flag | `LD_PRELOAD` env var |
| **Container Ready** | Requires JVM config | Works with any container |
| **Static Binaries** | Cannot instrument | Can intercept via syscalls |
| **Performance** | Negligible overhead | ~5-10% syscall intercept |
| **Security Context** | Runs in-process | Separate agent process |
| **Build Requirement** | None (runtime weaving) | None (preload hook) |

**Key insight**: AREX's ByteBuddy approach provides richer method-level context (parameter names, generics) but ties it permanently to the JVM ecosystem. Déjà's `LD_PRELOAD` is language-agnostic but loses language-specific semantics at the syscall boundary.

---

## 2. Context Propagation: Thread-Local Inheritance

### 2.1 Core Context Model

AREX uses a three-layer context hierarchy:

```
┌─────────────────────────────────────────────┐
│  TraceContextManager (static singleton)     │
│  ├─ ThreadLocal<ArexContext> contextHolder │
│  └─ ThreadLocal<Integer> sequenceRecorder   │
├─────────────────────────────────────────────┤
│  ArexContext (per-request/execution)        │
│  ├─ String caseId                           │
│  ├─ String replayId                         │
│  ├─ String recordId                         │
│  ├─ int sequence                            │
│  └─ Map<String, Object> attachments         │
├─────────────────────────────────────────────┤
│  ContextManager (runtime API)               │
│  ├─ currentContext()                        │
│  ├─ setContext(ArexContext)                 │
│  └─ clear()                                 │
└─────────────────────────────────────────────┘
```

**Source files:**
- `arex-agent-bootstrap/TraceContextManager.java`
- `arex-instrumentation-api/ArexContext.java`
- `arex-instrumentation-api/ContextManager.java`

### 2.2 Thread-Local Inheritance via Wrappers

To propagate context across thread boundaries, AREX wraps Runnables and Callables:

```java
// From: arex-agent-bootstrap/RunnableWrapper.java
public class RunnableWrapper implements Runnable {
    private final Runnable delegate;
    private final ArexContext context;

    public RunnableWrapper(Runnable delegate) {
        this.delegate = delegate;
        // Capture context at construction time
        this.context = ContextManager.currentContext();
    }

    @Override
    public void run() {
        try {
            // Restore context in new thread
            ContextManager.setContext(context);
            delegate.run();
        } finally {
            ContextManager.clear();
        }
    }
}

// Similar pattern in CallableWrapper.java
```

This is conceptually identical to Déjà's `scoped_spawn`, but implemented via constructor-time capture rather than task-local inheritance.

### 2.3 ForkJoinPool Instrumentation

AREX explicitly handles Java's `ForkJoinPool` (used by parallel streams and CompletableFuture):

```java
// From: arex-executors/ForkJoinTaskInstrumentation.java
@Advice.OnMethodEnter
public static void onExecute(@Advice.This ForkJoinTask<?> task) {
    if (ContextManager.needRecordOrReplay() && !(task instanceof RunnableWrapper)) {
        // Wrap the task to carry context
        ContextManager.wrap(task);
    }
}
```

### 2.4 Comparison: Thread-Local vs Task-Local

| Aspect | AREX (ThreadLocal) | Déjà (Tokio Task-Local) |
|--------|--------------------|--------------------------|
| **Propagation Model** | Inheritance via wrapping | Structured concurrency scopes |
| **Thread Boundaries** | Manual wrap/unwrap | Automatic via `scope()` |
| **Async/Await** | Thread-hop challenge | Native support |
| **Pool Mediation** | Same problem as Déjà | Same problem as AREX |
| **Context Lifetime** | Thread-bound | Task-bound |

**Critical similarity**: Both AREX and Déjà face the identical fundamental challenge with **pool-mediated I/O**. When work is submitted to a connection pool, thread-local/task-local context does not automatically travel with the command.

AREX's Redis/Lettuce wrapper must explicitly capture context:

```java
// From: arex-redis-common/lettuce/RedisCommandWrapper.java
public void record(Object response, String methodName) {
    // Context captured here at call site, not at pool execution
    RedisExtractor extractor = new RedisExtractor(...);
    extractor.record(response);
}
```

This is analogous to Déjà's need for fred Redis integration to attach correlation IDs to commands.

---

## 3. Replay Matching: Sequential with Signature Fallback

### 3.1 Match Strategy Hierarchy

AREX implements a three-tier matching strategy:

```
┌──────────────────────────────────────────────────────────────┐
│  1. ACCURATE MATCH                                           │
│     ├─ operationName + requestBody hash                      │
│     ├─ Used for: Most deterministic lookups                  │
│     └─ Fallback: Continue to Fuzzy if multiple matches       │
├──────────────────────────────────────────────────────────────┤
│  2. FUZZY MATCH                                              │
│     ├─ Same method signature only                            │
│     ├─ Sequential consumption (creationTime order)           │
│     ├─ "First unmatched" selection policy                    │
│     └─ Fallback: FIND_LAST mode or unmatched                 │
├──────────────────────────────────────────────────────────────┤
│  3. EIGEN MATCH                                              │
│     ├─ Feature-based similarity (planned)                    │
│     └─ Currently unimplemented                               │
└──────────────────────────────────────────────────────────────┘
```

**Source files:**
- `arex-instrumentation-api/match/AccurateMatchStrategy.java`
- `arex-instrumentation-api/match/FuzzyMatchStrategy.java`
- `arex-instrumentation-api/match/MatchStrategyRegister.java`

### 3.2 Accurate Match Implementation

```java
// From: arex-instrumentation-api/match/AccurateMatchStrategy.java
void process(MatchStrategyContext context) {
    context.setMatchStrategy(MatchStrategyEnum.ACCURATE);
    Mocker requestMocker = context.getRequestMocker();
    List<Mocker> replayList = context.getReplayList();

    // Hash of operationName + requestBody
    int methodSignatureHash = MockUtils.methodSignatureHash(requestMocker);

    List<Mocker> matchedList = new ArrayList<>();
    for (Mocker mocker : replayList) {
        if (methodSignatureHash == mocker.getAccurateMatchKey()) {
            matchedList.add(mocker);
        }
    }

    int matchedCount = matchedList.size();

    if (matchedCount == 1) {
        // Perfect match - mark used and return
        Mocker matchMocker = matchedList.get(0);
        if (!matchMocker.isMatched() || MockStrategyEnum.FIND_LAST == context.getMockStrategy()) {
            matchMocker.setMatched(true);
            context.setMatchMocker(matchMocker);
            context.setInterrupt(true); // Stop searching
        }
    } else if (matchedCount > 1) {
        // Multiple matches (e.g., redis: incr, decr)
        // Narrow to candidates for fuzzy match
        context.setReplayList(matchedList);
        // Continue to fuzzy strategy
    }
}
```

### 3.3 Fuzzy Match Implementation (Sequential Consumption)

```java
// From: arex-instrumentation-api/match/FuzzyMatchStrategy.java
void process(MatchStrategyContext context) {
    context.setMatchStrategy(MatchStrategyEnum.FUZZY);
    List<Mocker> replayList = context.getReplayList();

    // replayList is sorted by creationTime ascending
    Mocker mocker = null;
    for (int i = 0; i < replayList.size(); i++) {
        Mocker mockerDTO = replayList.get(i);
        if (!mockerDTO.isMatched()) {
            mocker = mockerDTO;
            break; // Take first unmatched
        }
    }

    // FIND_LAST mode: take the last one if all matched
    if (mocker == null && MockStrategyEnum.FIND_LAST == context.getMockStrategy()) {
        mocker = replayList.get(replayList.size() - 1);
    }

    if (mocker != null) {
        mocker.setMatched(true);
    }
    context.setMatchMocker(mocker);
}
```

**Key observation**: AREX's fuzzy match consumes recordings sequentially. This handles repeated calls (e.g., `INCR` twice) but relies on **temporal ordering**, not causal ordering.

### 3.4 Redis-Specific Matching

AREX configures distinct strategies per category:

```java
// From: arex-instrumentation-api/match/MatchStrategyRegister.java
strategyMap.put(MockCategoryType.REDIS.getName(), 
    CollectionUtil.newArrayList(ACCURATE_STRATEGY, FUZZY_STRATEGY));

strategyMap.put(MockCategoryType.DATABASE.getName(),
    CollectionUtil.newArrayList(ACCURATE_STRATEGY, FUZZY_STRATEGY));
```

Redis match key builder includes cluster name, key, and field:

```java
// From: arex-storage/mock/internal/matchkey/impl/RedisMatchKeyBuilderImpl.java
public List<byte[]> build(Mocker mocker) {
    Target target = mocker.getTargetRequest();
    String clusterName = target.attributeAsString(MockAttributeNames.CLUSTER_NAME);
    RedisMultiKey redisMultiKey = serializer.deserialize(target.getBody(), RedisMultiKey.class);
    // Build keys from: clusterName + key + field
}
```

### 3.5 Storage-Level Match Strategies

At the storage service, AREX supports additional match policies for edge cases:

```java
// From: arex-storage/mock/MockResultMatchStrategy.java
public enum MockResultMatchStrategy {
    TRY_FIND_LAST_VALUE,    // Return last match when exceeded
    BREAK_RECORDED_COUNT,   // Cycle through recorded values
    STRICT_MATCH            // No match = failure
}
```

---

## 4. Comparison: AREX vs Déjà vs Speedscale

### 4.1 Capture Mechanism

| System | Approach | Language Scope | Deploy Complexity |
|--------|----------|----------------|-------------------|
| **Déjà** | `LD_PRELOAD` syscall interception | Universal (libc) | Minimal (env var) |
| **AREX** | ByteBuddy Java agent | Java only | JVM `-javaagent` config |
| **Speedscale** | Sidecar proxy (Envoy/eBPF) | Universal | Kubernetes sidecar |

### 4.2 Context Propagation

| System | Mechanism | Async Support | Pool Mediation |
|--------|-----------|---------------|----------------|
| **Déjà** | `tokio::task_local!` | Native | Requires driver integration |
| **AREX** | `ThreadLocal` + wrapping | Via wrappers | Requires driver integration |
| **Speedscale** | None (no causal context) | N/A | Proxy handles sequencing |

### 4.3 Replay Matching

| System | Primary | Secondary | Repeated Read Handling |
|--------|---------|-----------|----------------------|
| **Déjà (planned)** | State-seeded real DB/Redis for owned dependencies | Causal scope + ordinal fallback | Real dependency state avoids read-response lookup ambiguity |
| **AREX** | Signature (op+body) | Sequential consumption | Temporal order fallback |
| **Speedscale** | Signature only | vUser sequencing | No inherent handling |

### 4.4 Critical Differentiator: Repeated Read Ambiguity

Consider this pattern (common in Hyperswitch):

```rust
// Recording
let v1 = redis.get("payment:intent:123").await?;  // status="requires_confirmation"
redis.hset("payment:intent:123", "status", "processing").await?;
let v2 = redis.get("payment:intent:123").await?;  // status="processing"
```

| System | Behavior |
|--------|----------|
| **Déjà (state-seeded owned replay)** | Seeds the pre-request state, lets real Redis produce `v1`, candidate `HSET`, then real Redis produces `v2`; causal ordinal remains fallback/validation metadata |
| **AREX** | Accurate match finds both; fuzzy match returns first-unmatched `v1` for both reads unless request bodies differ |
| **Speedscale** | Both reads match same signature; returns arbitrary match unless sequential consumption configured |

**AREX comment in codebase** (from `AccurateMatchStrategy.java`):
```java
// matched multiple result(like as redis: incr、decr) only retain matched item for next fuzzy match
```

This confirms AREX recognizes the repeated-call problem but solves it via **signature narrowing + temporal consumption**, not causal attribution.

---

## 5. Storage and Comparison Architecture

### 5.1 Storage Layout

```
┌─────────────────────────────────────────────────────────────┐
│  AREX Storage Service (Spring Boot + MongoDB + Redis)      │
├─────────────────────────────────────────────────────────────┤
│  MongoDB Collections                                        │
│  ├─arex_recordings ──► AREXMocker documents                 │
│  │                      ├─ _id, appId, recordId             │
│  │                      ├─ operationName                    │
│  │                      ├─ categoryType (REDIS, DATABASE)   │
│  │                      ├─ targetRequest                    │
│  │                      ├─ targetResponse                   │
│  │                      └─ creationTime                     │
│  ├─ arex_replays ────► Replay result documents              │
│  └─ arex_cases ──────► Test case definitions                │
├─────────────────────────────────────────────────────────────┤
│  Redis Cache                                                │
│  └─ Replay mock pre-load cache (TTL-based)                  │
├─────────────────────────────────────────────────────────────┤
│  API Endpoints                                              │
│  ├─ POST /query ──────► Query mocks by operation           │
│  ├─ POST /save ───────► Record new mock                    │
│  └─ GET /replayResult ─► Get comparison results            │
└─────────────────────────────────────────────────────────────┘
```

**Source files:**
- `arex-storage/web/controller/AgentRecordingController.java`
- `arex-storage/mock/impl/DefaultMockResultProviderImpl.java`
- `arex-storage/service/AgentWorkingService.java`

### 5.2 Comparison Flow

AREX separates **mock lookup** from **result comparison**:

1. **During replay**: Agent calls storage `/query` to fetch mocks (using match strategies)
2. **After replay**: Schedule service calls `/replayResult` to fetch actual outcomes
3. **Comparison**: `DefaultReplayResultComparer` aligns recorded vs replayed by `compareKey`

```java
// From: arex-replay-schedule/comparer/impl/DefaultReplayResultComparer.java
private List<ReplayCompareResult> matchCompareReplayResults(...) {
    // Group record results by compareKey (mocker ID)
    Map<String, List<CompareItem>> recordMap = ...

    // For each replay result, find matching record by key
    for (CompareItem resultCompareItem : replayResults) {
        String compareKey = resultCompareItem.getCompareKey();
        if (recordMap.containsKey(compareKey)) {
            // Match found - compare bodies
            compareRecordAndResult(...);
        }
    }
}
```

### 5.3 Comparison: AREX vs Déjà Comparison

| Aspect | AREX | Déjà (planned) |
|--------|------|----------------|
| **Trigger** | Scheduled post-replay | During replay (real-time) |
| **Alignment** | compareKey (mocker ID) | Causal scope + ordinal |
| **Diff Engine** | arex-compare-sdk (JSON tree diff) | Déjà's structured diff |
| **Out-of-Order Arrays** | Supported | Planned |
| **Exclusions** | JSON path patterns | Field-level masking |

---

## 6. Deployment Architecture

### 6.1 AREX Components

```
┌─────────────────────────────────────────────────────────────────┐
│                        AREX Platform                            │
├─────────────────────────────────────────────────────────────────┤
│  Agent (per JVM)              │  Storage Service               │
│  ├─ Bootstrap classloader     │  ├─ MongoDB persistence        │
│  ├─ Instrumentation plugins   │  ├─ Redis mock cache           │
│  ├─ Record/replay logic       │  └─ Query/save endpoints       │
│  └─ Config: arex-agent.jar    │                                │
├─────────────────────────────────────────────────────────────────┤
│  Schedule Service             │  Web/API Service               │
│  ├─ Replay case generation    │  ├─ Case management            │
│  ├─ Sender dispatch           │  ├─ Plan orchestration         │
│  └─ Result comparison         │  └─ Diff visualization         │
├─────────────────────────────────────────────────────────────────┤
│  Standalone Mode                                              │
│  └─ All-in-one JAR for local development                       │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 Comparison: Deployment Models

| System | Agent Location | Control Plane | Storage |
|--------|---------------|---------------|---------|
| **Déjà** | External process (LD_PRELOAD) | CLI / embedded | Local files |
| **AREX** | In-JVM (javaagent) | Separate services | MongoDB + Redis |
| **Speedscale** | Sidecar container | SaaS / self-hosted | Cloud/object storage |

---

## 7. Key Insights for Déjà

### 7.1 What AREX Does Well

1. **Rich method context**: ByteBuddy captures parameter names, generics, and type information that Déjà loses at the syscall boundary.

2. **Explicit sequence handling**: AREX acknowledges the repeated-call problem and provides `MockStrategyEnum` configurations (`FIND_LAST`, `STRICT_MATCH`) for different scenarios.

3. **Plugin ecosystem**: Modular instrumentation architecture (40+ modules) allows incremental support for frameworks.

4. **Standalone mode**: Quick local testing via all-in-one JAR reduces friction.

### 7.2 What AREX Struggles With

1. **Thread-bound context**: Java's `ThreadLocal` doesn't naturally handle async/await patterns—AREX requires wrapping, which can miss edge cases.

2. **Same-signature ambiguity**: Without causal tracking, repeated calls to the same Redis key/SQL query rely on temporal ordering, which may not match causal ordering under replay.

3. **Java lock-in**: ByteBuddy instrumentation is fundamentally JVM-specific.

### 7.3 Déjà's Differentiation Opportunities

1. **True causal correlation**: Déjà's `Annotated<W>` and `CausalWorkQueue` can track work across pool boundaries in ways thread-local cannot.

2. **Universal language support**: `LD_PRELOAD` works for Go, Rust, Python, Node.js without per-language agents.

3. **Streaming architecture**: File-based capture avoids MongoDB/Redis operational complexity.

4. **State machine replay**: By tracking resource versions (e.g., Redis key generations), Déjà can detect when SET x→SET y ordering matters.

---

## 8. Recommendations

### 8.1 For Déjà Implementation

1. **Implement per-request state fixture inference**: For owned Redis/Postgres, derive pre-request state from pre-write reads, seed isolated dependencies, and run candidate code against real DB/Redis.

2. **Implement correlation ordinal**: Add `local_sequence_in_scope` to operation identity (already planned in `CORRELATION_ARCHITECTURE.md`). This remains required for fixture construction and side-effect validation.

3. **Resource versioning**: Track Redis key generations or SQL row versions for stateful replay and final-state comparison.

4. **Hybrid replay strategy**: Use state-seeded owned dependencies first; use causal lookup and signature/sequence fallbacks only where real-state replay is unavailable.

### 8.2 For Testing Against AREX

When evaluating Déjà's replay correctness, test these scenarios that expose AREX's limitations:

```rust
// Test: Out-of-order repeated reads
spawn(async {
    let a = redis.get("key").await?;  // Read 1
    let b = compute_something();
    redis.set("other", b).await?;      // Unrelated write
    let c = redis.get("key").await?;  // Read 2 - same signature, different result
});
```

With state-seeded replay, Déjà can avoid mocking these Redis reads entirely: seed the pre-request value, let the candidate execute its own writes, and then validate both the ordered write log and the final touched state. With causal correlation, Déjà can still distinguish Read 1 from Read 2 when it must fall back to response replay.

---

## 9. References

### AREX Source Locations

| Component | File Path (relative to clone root) |
|-----------|-----------------------------------|
| Context Management | `arex-agent-java/arex-instrumentation-api/src/main/java/io/arex/inst/runtime/context/` |
| Match Strategies | `arex-agent-java/arex-instrumentation-api/src/main/java/io/arex/inst/runtime/match/` |
| Redis Instrumentation | `arex-agent-java/arex-instrumentation/redis/arex-redis-common/src/main/java/io/arex/inst/redis/common/` |
| Database Instrumentation | `arex-agent-java/arex-instrumentation/database/arex-database-common/src/main/java/io/arex/inst/database/common/` |
| Storage Mock Provider | `arex-storage/arex-storage-web-api/src/main/java/com/arextest/storage/mock/impl/DefaultMockResultProviderImpl.java` |
| Replay Comparison | `arex-replay-schedule/arex-schedule-web-api/src/main/java/com/arextest/schedule/comparer/impl/DefaultReplayResultComparer.java` |
| Compare SDK | `arex-compare-sdk/arex-compare-sdk/src/main/java/com/arextest/diff/` |

### Déjà Related Documents

- `CORRELATION_ARCHITECTURE.md` - Déjà's causal correlation design
- `REPLAY_PIPELINE.md` - Replay identity and lookup hierarchy
- `SPEEDSCALE_COMPARISON.md` - Comparison with Speedscale
- `HYPERSWITCH_REPLAY_AMBIGUITY_REPORT.md` - Real-world ambiguity evidence

---

## Appendix: Match Key Examples

### AREX Redis Match Key

```java
// Effective key components:
operationName = "GET"
clusterName = "Cluster1"  // Extracted from connection URL
key = "payment:intent:123"
field = null

accurateMatchKey = hash(operationName + serialized(key + field))
```

### Déjà Planned Operation Identity

```rust
// Proposed from CORRELATION_ARCHITECTURE.md:
struct DependencyOperationIdentity {
    global_index: u64,              // Monotonic capture order
    connection_id: ConnectionId,    // Physical connection
    protocol: Protocol,
    operation_signature: Signature, // Request body hash
    resource_key: ResourceKey,      // Redis key / SQL normalized
    causal_scope_id: CorrelationId, // Request/scope
    local_sequence_in_scope: u32,   // Ordinal within scope
    resource_version_or_cursor: Option<u64>, // State generation
}
```

The key difference: AREX's match key identifies **what** was called; Déjà's identity tracks **when in causal order** it was called.
