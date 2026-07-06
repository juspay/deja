> **Archived.** This document records a lookup-ambiguity taxonomy that predates the implemented 6-rank address ladder. It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Taxonomy of Replay Lookup Ambiguity

## Executive Summary

This document provides a comprehensive taxonomy of strategies for resolving **lookup ambiguity** in replay systems. Lookup ambiguity occurs when multiple identical dependency requests must return different responses. We analyze seven fundamental approaches ranging from simple signature-based lookup to sophisticated causal correlation and state emulation.

---

## 1. The Core Problem: Hidden State Ambiguity

### 1.1 Defining the Problem

Consider a classic Redis sequence:

```
T1: SET key v1      -- write state
T2: GET key         -- returns "v1"
T3: SET key v2      -- write state
T4: GET key         -- returns "v2"
```

Both GET requests are **byte-identical** on the wire:
```
*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
```

Yet they must return **different responses**. No amount of signature refinement (hashing, canonicalization) can distinguish them because the distinguishing information is not in the request—it lives in external state and temporal position.

### 1.2 Information-Theoretic Limit

This is not a technical limitation—it is **information-theoretic**:

```
Given:    Request bytes R
Goal:     Determine correct response from set {v1, v2}
Problem:  Information(IdealResponse | R) = 0
Solution: Additional context C must satisfy H(IdealResponse | R, C) > H(IdealResponse | R)
```

The taxonomy below enumerates strategies for providing context C.

---

## 2. The Seven Strategies

### Strategy 1: Pure Signature Lookup

**Model:** `response = table[signature(request)]`

**Description:** Each unique request signature maps to a single response. Repeated identical requests return the same cached response.

**Implementation:**
```rust
struct SignatureTable {
    entries: HashMap<Signature, Response>,
}

impl SignatureTable {
    fn lookup(&self, request: &[u8]) -> Option<Response> {
        let sig = Signature::from_bytes(request);
        self.entries.get(&sig).cloned()
    }
}
```

**When It Works:**
- Read-heavy workloads with immutable data
- Idempotent operations (cache reads, config lookups)
- No state mutations between identical reads

**When It Fails:**
- Stateful sequences (SET→GET→SET→GET)
- Lock acquisition patterns
- Counter increments
- Any read-after-write within same test

**Example Failure:**
```
Recording:  SET balance 100 → OK
            GET balance   → "100"
            SET balance 200 → OK
            GET balance   → "200"

Replay:     GET balance   → table["GET balance"] returns ???
            
Problem:    Which response? Both map to same key.
Solution:   ❌ No solution within this strategy
```

**Confidence Level:** Low for stateful systems. Only viable for pure read replicas or immutable infrastructure.

---

### Strategy 2: Per-Signature Response Queue (Occurrence Cursor)

**Model:** `response = table[signature(request)][cursor[sig]++]`

**Description:** Each signature maintains a FIFO queue of responses. Sequential accesses cycle through recorded responses in order.

**Implementation:**
```rust
struct QueuedSignatureTable {
    queues: HashMap<Signature, Vec<Response>>,
    cursors: HashMap<Signature, usize>,
}

impl QueuedSignatureTable {
    fn lookup(&mut self, request: &[u8]) -> Option<Response> {
        let sig = Signature::from_bytes(request);
        let queue = self.queues.get(&sig)?;
        let cursor = self.cursors.entry(sig.clone()).or_insert(0);
        let response = queue.get(*cursor)?.clone();
        *cursor += 1;
        Some(response)
    }
    
    fn reset(&mut self) {
        self.cursors.clear();  // Reset for deterministic replay
    }
}
```

**When It Works:**
- Single-threaded sequential execution
- Deterministic request ordering
- No concurrent access to same signature

**When It Fails:**
- Concurrent requests interleave ambiguous reads
- Order changes due to timing/scheduling
- Code changes alter dependency call sequence

**Example Success (Sequential):**
```
Recording:  Req A: GET lock → "owner-A"
            Req B: GET lock → "owner-B"

Replay:     Req A: GET lock → cursor[GET lock]=0 → "owner-A" ✓
            Req B: GET lock → cursor[GET lock]=1 → "owner-B" ✓
```

**Example Failure (Concurrent):**
```
Recording:  Thread A: GET lock → "owner-A"  (time=10ms)
            Thread B: GET lock → "owner-B"  (time=12ms)

Replay:     Thread B: GET lock → cursor[GET lock]=0 → "owner-A" ✗ WRONG!
            Thread A: GET lock → cursor[GET lock]=1 → "owner-B" ✗ WRONG!

Concurrency reversed request order → incorrect ownership assignment
```

**Real-World Implementation:** Speedscale uses this approach with an `instance` field in their signature format.

**Confidence Level:** Medium. Works for controlled environments, fails under production concurrency patterns.

---

### Strategy 3: Global Ordering (Sequence-Based)

**Model:** `response = recording[global_position++]`

**Description:** The entire execution produces a linear sequence of dependency events. Replay consumes events sequentially regardless of signature.

**Implementation:**
```rust
struct GlobalSequenceReplay {
    events: Vec<Event>,
    position: usize,
}

impl GlobalSequenceReplay {
    fn next_event(&mut self) -> Option<Event> {
        let event = self.events.get(self.position)?;
        self.position += 1;
        Some(event.clone())
    }
    
    fn expect(&mut self, signature: Signature) -> Result<Response, Divergence> {
        let event = self.next_event()
            .ok_or(Divergence::UnexpectedEnd)?;
        
        if event.signature != signature {
            return Err(Divergence::SignatureMismatch {
                expected: event.signature,
                actual: signature,
            });
        }
        
        Ok(event.response)
    }
}
```

**When It Works:**
- Strictly deterministic execution
- No concurrent dependency access
- Replay reproduces exact global event order

**When It Fails:**
- Multi-threaded request handling
- Background workers interleave events
- Async/await with non-deterministic polling order
- Any production-grade service with parallelism

**Example Failure:**
```
Recording:  [Req A: GET user] [Background: GET config] [Req B: GET user]

Replay:     [Req A: GET user] [Req B: GET user] [Background: GET config]
                      ↑
            Position mismatch: expected GET config, saw GET user
            
Result:     Divergence error or incorrect response
```

**Confidence Level:** Low for production services. Only suitable for single-threaded integration tests.

---

### Strategy 4: Per-Session Ordering

**Model:** `response = session_table[session_id][session_cursor++]`

**Description:** Events are partitioned by session/connection. Each session maintains its own independent sequence cursor.

**Implementation:**
```rust
struct SessionBasedReplay {
    sessions: HashMap<SessionId, Vec<Event>>,
    cursors: HashMap<SessionId, usize>,
}

impl SessionBasedReplay {
    fn lookup(&mut self, session_id: SessionId, signature: Signature) 
        -> Result<Response, Divergence> 
    {
        let cursor = self.cursors.entry(session_id.clone()).or_insert(0);
        let events = self.sessions.get(&session_id)
            .ok_or(Divergence::UnknownSession)?;
        
        let event = events.get(*cursor)
            .ok_or(Divergence::SessionExhausted)?;
        
        if event.signature != signature {
            return Err(Divergence::SignatureMismatch {
                expected: event.signature,
                actual: signature,
            });
        }
        
        *cursor += 1;
        Ok(event.response.clone())
    }
}
```

**When It Works:**
- Connection-oriented protocols (PostgreSQL, traditional HTTP/1.1)
- Clear session boundaries
- No session sharing or pooling

**When It Fails:**
- Connection pooling multiplexes multiple sessions
- Pipelined protocols (Redis, HTTP/2)
- Shared connection caches
- Request routing changes

**Déjà Context:** This corresponds to per-`connection_id` replay. The artifact already tracks `connection_id` for socket events.

**Confidence Level:** Medium-High for connection-oriented backends. Challenging for modern async connection pooling.

---

### Strategy 5: Per-Request Causal Correlation

**Model:** `response = correlation_table[request_id][logical_index]`

**Description:** Each inbound request (the "cause") is tagged with a unique ID. All dependency calls made while processing that request are tagged with the same ID. Replay uses `(request_id, sequence_index)` as the lookup key.

**Implementation:**
```rust
// Task-local correlation storage
tokio::task_local! {
    pub static REQUEST_CONTEXT: RequestContext;
}

struct RequestContext {
    request_id: String,
    sequence_counter: Cell<u64>,
}

impl RequestContext {
    fn next_dependency_index(&self) -> u64 {
        let idx = self.sequence_counter.get();
        self.sequence_counter.set(idx + 1);
        idx
    }
}

struct CorrelatedReplay {
    events: HashMap<(RequestId, DependencyIndex), Event>,
}

impl CorrelatedReplay {
    fn lookup(&self, request_id: &str, signature: Signature) 
        -> Result<Response, Divergence> 
    {
        let ctx = REQUEST_CONTEXT.try_with(|ctx| ctx.clone())
            .map_err(|_| Divergence::NoCorrelationContext)?;
        
        let index = ctx.next_dependency_index();
        let key = (request_id.to_string(), index);
        
        let event = self.events.get(&key)
            .ok_or(Divergence::MissingCorrelatedEvent)?;
        
        if event.signature != signature {
            return Err(Divergence::SignatureMismatch {
                expected: event.signature,
                actual: signature,
            });
        }
        
        Ok(event.response.clone())
    }
}
```

**When It Works:**
- Request-scoped dependency calls
- Clear parent-child relationship
- Tagged request context propagates through spawn boundaries

**When It Fails:**
- Fire-and-forget background work
- Shared long-lived I/O driver tasks (Redis fred routing task)
- Callback-based async without context propagation

**Déjà Context:** This is the current architecture documented in `CORRELATION_ARCHITECTURE.md`. Uses `DEJA_CORRELATION_ID` task-local with FFI bridge for LD_PRELOAD hooks.

**Complex Case Within Same Request:**
```
Request req-123:
  1. GET balance    → "100"
  2. SET balance 200 → OK
  3. GET balance    → "200"
  
Lookup keys:
  ("req-123", 0) → GET balance → "100"
  ("req-123", 1) → SET balance 200 → OK
  ("req-123", 2) → GET balance → "200"
```

Even with correlation, repeated identical calls within the same request need sequence indexing.

**Confidence Level:** High. The strongest general-purpose solution. Requires proper context propagation through all async boundaries.

---

### Strategy 6: Resource-State Emulation

**Model:** Maintain a mock state machine; `response = f(current_state, request)`

**Description:** Instead of recording responses, record state mutations. Rebuild state during replay and compute responses dynamically.

**Implementation:**
```rust
struct RedisStateEmulator {
    state: HashMap<String, String>,
    expirations: BTreeMap<Instant, Vec<String>>,
}

impl RedisStateEmulator {
    fn execute(&mut self, command: RedisCommand) -> RedisResponse {
        self.process_expirations();
        
        match command {
            RedisCommand::Set { key, value, nx, ex } => {
                if nx && self.state.contains_key(&key) {
                    return RedisResponse::Nil;
                }
                self.state.insert(key.clone(), value.clone());
                if let Some(ttl) = ex {
                    self.expirations.entry(now() + ttl).or_default().push(key);
                }
                RedisResponse::Ok
            }
            
            RedisCommand::Get { key } => {
                match self.state.get(&key) {
                    Some(value) => RedisResponse::BulkString(value.clone()),
                    None => RedisResponse::Nil,
                }
            }
            
            RedisCommand::Del { keys } => {
                let count = keys.iter()
                    .filter(|k| self.state.remove(*k).is_some())
                    .count();
                RedisResponse::Integer(count as i64)
            }
            
            // ... other commands
        }
    }
}
```

**When It Works:**
- Simple key-value operations (Redis GET/SET/DEL)
- Well-understood state semantics
- Deterministic operations (no server-side Lua scripts, no WATCH/MULTI)

**When It Fails:**
- Complex SQL queries with joins, aggregations
- Server-side stored procedures
- Conditional operations (WATCH/MULTI/EXEC)
- Pub/sub, streams, complex data structures
- Time-based operations (TTL precision)

**Complete State Problem:**
To accurately emulate, you need initial state at recording start:
```
Recording:  GET counter → "42"
            INCR counter → "43"
            GET counter → "43"

Emulation:  Initial state[ counter ] = ???
            Without knowing "41" existed, can't replay correctly
```

**Confidence Level:** Low-Medium. Powerful for Redis basics, prohibitively complex for general SQL databases.

---

### Strategy 6b: Per-Request State-Seeded Real Dependency Replay

**Model:** Reconstruct the request's initial DB/Redis state; run candidate against real isolated dependencies.

**Description:** Instead of mocking each DB/Redis read response, derive a minimal pre-request state fixture from recorded reads, seed Redis/Postgres, and allow the candidate version to execute real dependency operations. This removes response lookup ambiguity for owned mutable dependencies.

**Implementation sketch:**
```rust
struct RequestFixture {
    request_id: String,
    initial_facts: Vec<StateFact>,
    negative_facts: Vec<AbsenceFact>,
    recorded_write_log: Vec<WriteOperation>,
    post_state_expectations: Vec<StateFact>,
    confidence: FixtureConfidence,
}

fn replay_request_with_fixture(fixture: RequestFixture) -> ReplayResult {
    let sandbox = create_isolated_dependencies();
    sandbox.apply(fixture.initial_facts, fixture.negative_facts);

    let candidate = run_inbound_request_against_sandbox(fixture.request_id, sandbox);

    compare_response(candidate.response);
    compare_ordered_write_log(candidate.writes, fixture.recorded_write_log);
    compare_final_state(candidate.final_state, fixture.post_state_expectations);
}
```

**Key rule:** Seed **pre-request state**, not all read results. Reads after an in-request write are validation observations, not initial fixture facts.

Example:
```text
GET k -> nil       // seed: ensure k absent
SET k -> "abc"     // mark k written
GET k -> "abc"     // do not seed; replay should produce this
```

**When It Works:**
- Isolated request replay is required.
- Owned Redis/Postgres can be safely sandboxed.
- Read results can be inverted into state facts.
- Final state and write ordering matter.

**When It Fails or Needs Warnings:**
- SQL aggregates (`COUNT`, `SUM`, `EXISTS`) underdetermine concrete rows.
- Joins/projected queries omit columns needed to seed base tables.
- Stored procedures, locks, and triggers perform hidden side effects.
- Redis `SCAN`/pattern reads require namespace completeness, not just listed keys.
- TTL/time-sensitive state needs absolute or replay-relative expiry handling.

**Relationship to Correlation:** Correlation is still required, but its purpose shifts from runtime response lookup to fixture construction and side-effect validation.

**Confidence Level:** High for Redis strings/hashes and primary-key SQL row reads; medium/low for complex SQL unless schema/application hints are available.

---

### Strategy 7: Hybrid Approaches

**Model:** Combine multiple strategies with fallback hierarchy

**Description:** Use the strongest applicable strategy for each situation, falling back to weaker strategies when stronger ones are unavailable.

**Implementation:**
```rust
enum LookupStrategy {
    CorrelatedSequence,     // (request_id, index)
    SessionSequence,        // (session_id, index)
    SignatureQueue,         // (signature, occurrence)
    StateEmulated,          // f(state, request)
}

struct HybridReplay {
    correlated: CorrelatedReplay,
    session_based: SessionBasedReplay,
    queued: QueuedSignatureTable,
    emulator: Option<RedisStateEmulator>,
}

impl HybridReplay {
    fn lookup(&mut self, ctx: LookupContext) -> Result<Response, ReplayError> {
        // Try strongest first
        if let Some(req_id) = &ctx.request_id {
            if let Ok(resp) = self.correlated.lookup(req_id, ctx.index, &ctx.signature) {
                return Ok(resp.with_strategy(LookupStrategy::CorrelatedSequence));
            }
        }
        
        // Fall back to session
        if let Some(session_id) = &ctx.session_id {
            if let Ok(resp) = self.session_based.lookup(session_id, &ctx.signature) {
                return Ok(resp.with_strategy(LookupStrategy::SessionSequence));
            }
        }
        
        // Try state emulation for simple commands
        if let Some(emulator) = &mut self.emulator {
            if let Some(cmd) = parse_redis_command(&ctx.raw_request) {
                if is_emulatable(&cmd) {
                    return Ok(emulator.execute(cmd)
                        .with_strategy(LookupStrategy::StateEmulated));
                }
            }
        }
        
        // Last resort: signature queue
        self.queued.lookup(&ctx.raw_request)
            .map(|r| r.with_strategy(LookupStrategy::SignatureQueue))
            .ok_or(ReplayError::ExhaustedAllStrategies)
    }
}
```

**Fallback Hierarchy:**
```
1. Causal Correlation    ← Most precise, requires context
2. Session Isolation     ← Good for connection-oriented
3. State Emulation       ← Good for simple KV operations
4. Signature Queue       ← Universal but fragile
5. Pure Signature        ← Last resort, often wrong
```

**Déjà Recommendation (from SIGNATURE_VS_CORRELATION_ANALYSIS.md):**
```
Strict replay:     scope + sequence + signature validation
Mock fallback:     signature + occurrence cursor
Protocol state:    Redis/DB-specific state model where feasible
Unscoped fallback: explicit low-confidence attribution
```

**Confidence Level:** High. Adapts to available information. Provides graceful degradation.

---

## 3. Decision Matrix: When Is Correlation Necessary?

### 3.1 Correlation Is UNNECESSARY When:

| Scenario | Example | Suitable Strategy |
|----------|---------|-------------------|
| Pure reads of immutable data | Configuration lookups, reference data | Pure Signature |
| Idempotent operations | Cache warming, prefetching | Pure Signature |
| Unique request patterns | UUID-based keys, timestamps in requests | Pure Signature |
| Write-only telemetry | StatsD metrics, logging | Signature Queue (best-effort) |
| Background health checks | Periodic DB pings | Signature Queue |

### 3.2 Correlation Is NECESSARY When:

| Scenario | Example | Required Strategy |
|----------|---------|-------------------|
| Read-after-write in same flow | Payment state, lock status | Correlation + Index |
| Concurrent ambiguous access | Multiple requests reading same lock key | Correlation + Index |
| Order-sensitive sequences | Distributed transactions, saga pattern | Correlation + Strict Sequence |
| Per-request debugging/reporting | "What did request #123 touch?" | Correlation |
| Regression detection | Detecting added/removed/skipped calls | Correlation + Sequence Validation |

### 3.3 The Critical Threshold

The tipping point is **ambiguous signature count**:

```
ambiguous_signature_rate = (#signatures with >1 distinct response) / (total unique signatures)

if ambiguous_signature_rate < 0.01:
    Correlation optional
elif ambiguous_signature_rate < 0.10:
    Correlation recommended
else:
    Correlation required for reliable replay
```

Measured from Hyperswitch artifact:
```
Redis GET API_LOCK_* appearing twice with different responses
→ ambiguous_signature_rate ~ 5% for lock operations
→ Correlation strongly recommended
```

---

## 4. Detailed Example: SET/GET Sequence

### 4.1 Recording

```
[Request: POST /payment]
  ├── Socket 5: CONNECT redis:6379
  ├── Socket 5: SEND "SET lock:pay-123 owner-abc EX 60 NX"
  ├── Socket 5: RECV ":1"
  ├── Socket 5: SEND "GET lock:pay-123"
  ├── Socket 5: RECV "$9\r\nowner-abc"
  ├── Socket 5: SEND "SET status:pay-123 processing"
  ├── Socket 5: RECV ":1"
  ├── Socket 5: SEND "GET lock:pay-123"
  ├── Socket 5: RECV "$9\r\nowner-abc"
  └── Socket 5: SEND "DEL lock:pay-123"
```

Notice: Two identical `GET lock:pay-123` commands returning the same value here, but consider a retry scenario where the lock owner changes.

### 4.2 Strategy Comparison

| Strategy | Lookup Key for First GET | Lookup Key for Second GET | Works? |
|----------|-------------------------|---------------------------|--------|
| Pure Signature | `hash("GET lock:pay-123")` | Same key | ❌ Collides |
| Signature Queue | `(sig, 0)` | `(sig, 1)` | ✅ If deterministic order |
| Global Order | `(global_pos=4)` | `(global_pos=8)` | ⚠️ Fragile under concurrency |
| Session Order | `(sock=5, pos=1)` | `(sock=5, pos=3)` | ✅ If socket isolated |
| Correlation | `(req="abc", idx=1)` | `(req="abc", idx=3)` | ✅ Robust |
| State Emulation | `state["lock:pay-123"]` | `state["lock:pay-123"]` | ✅ If state seeded correctly |

### 4.3 Retry Scenario (Lock Transfer)

```
First attempt:
  SET lock:pay-123 owner-attempt-1 EX 60 NX → 1
  GET lock:pay-123 → "owner-attempt-1"
  [timeout, expires]
  
Retry:
  SET lock:pay-123 owner-attempt-2 EX 60 NX → 1
  GET lock:pay-123 → "owner-attempt-2"
```

Same request ID, different lock owners. Now even correlation must include sequence index:

```
Attempt 1: (req="pay-123", attempt=1, idx=0) → GET → "owner-attempt-1"
Attempt 2: (req="pay-123", attempt=2, idx=0) → GET → "owner-attempt-2"
```

**Lesson:** Full causality requires `(request_id, logical_sequence_index)`. Request ID alone is insufficient for intra-request ordering.

---

## 5. Low-Level Generic Solutions

### 5.1 Protocol-Level Approach

**Applicable to:** Any TCP-based protocol with request-response pairs.

**Mechanism:**
```
1. Intercept connect() → track connection_id
2. Intercept send() → assign sequence number, store request
3. Intercept recv() → pair with pending request by sequence
4. Record: (connection_id, request_seq, request_bytes, response_bytes)
5. Replay: Match by (connection_id, request_seq, request_bytes)
```

**Advantages:**
- Protocol-agnostic at transport layer
- No application changes
- Works with binary protocols

**Limitations:**
- Cannot distinguish pipelined requests without parsing
- Connection pooling breaks sequence assumptions
- Multiplexed protocols (HTTP/2, QUIC) need stream ID tracking

### 5.2 Application-Level Context Propagation

**Applicable to:** Any application where source code can be modified or instrumented.

**Mechanism:**
```
1. Assign request_id at entry point (HTTP handler, message consumer)
2. Store in thread-local or task-local storage
3. Library instrumentation reads context, attaches to dependency calls
4. LD_PRELOAD hooks read context via FFI or environment
5. Record: (request_id, sequence_idx, dependency_event)
```

**Déjà Implementation:**
```rust
// Middleware sets context
tokio::task_local! {
    pub static DEJA_CORRELATION_ID: String;
}

// Hook reads via FFI
#[no_mangle]
pub unsafe extern "C" fn deja_correlation_id(buf: *mut u8, len: usize) -> usize {
    DEJA_CORRELATION_ID.try_with(|id| {
        let bytes = id.as_bytes();
        let copy_len = bytes.len().min(len);
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, copy_len);
        copy_len
    }).unwrap_or(0)
}
```

**Advantages:**
- Precise causal tracking
- Works with connection pooling
- Language/framework agnostic with proper FFI

**Limitations:**
- Requires context propagation through spawn/spawn_blocking
- Breaks with fire-and-forget tasks
- Third-party library integration needed

---

## 6. High-Level Generic Solutions

### 6.1 Request-Case Model

**Déjà Implementation:** `RequestCase` in `deja-core`

```rust
struct RequestCase {
    case_id: String,              // The correlating request ID
    inbound: RequestCaseInbound,   // What triggered this case
    recorded_inputs: Vec<Event>,   // Time, Random, Environment
    recorded_outputs: Vec<Event>,  // Socket, DNS calls
    response: RequestCaseOutbound, // What was returned
}
```

**Replay Logic:**
```
For each incoming request:
  1. Find matching RequestCase by case_id or similarity
  2. Expect recorded_outputs in sequence
  3. Return recorded response
  4. Report any divergence
```

### 6.2 Oracle Validation Pattern

**In-Band Verification:**
```sql
/* deja_expected_request_id=<id>;deja_backend=postgres */ SELECT 1
```

The marker is embedded in the actual protocol payload. During replay:
```
1. Parse marker from request payload
2. Compare to hook-captured request_id
3. Mismatch indicates correlation leak/contamination
```

**Advantages:**
- Validates correlation from inside the I/O stream
- Detects framework bugs in context propagation
- Self-testing architecture

---

## 7. Implementation Recommendations

### 7.1 Minimal Viable Product

Start with **Strategy 7 (Hybrid)** with minimal hierarchy:
```
1. If correlation available: (request_id, index) → response
2. Else: (signature, occurrence) → response (with warning)
```

### 7.2 Production System

Full **Strategy 7** hierarchy:
```
1. (request_id, index) → response + signature validation
2. (session_id, index) → response + signature validation
3. State emulation for simple KV operations
4. (signature, occurrence) → response (low confidence)
5. Record-only mode for unidentified events
```

### 7.3 Confidence Scoring

Every replayed response should carry confidence:
```rust
enum ReplayConfidence {
    Certain,      // Correlated + signature match
    Probable,     // Session-based + signature match
    Estimated,    // Signature queue, no collisions
    Speculative,  // Signature queue, known collisions
    Unattributed, // No match, passed through to real backend
}
```

---

## 8. Summary Table

| Strategy | Complexity | Concurrency Safety | State Handling | Best For |
|----------|-----------|-------------------|----------------|----------|
| Pure Signature | ⭐ Low | ❌ None | ❌ None | Immutable reads |
| Signature Queue | ⭐ Low | ⚠️ Ordering-dependent | ⚠️ Cursor-based | Sequential tests |
| Global Order | ⭐⭐ Medium | ❌ None | ✅ Implicit | Single-threaded |
| Session Order | ⭐⭐ Medium | ⚠️ Session-limited | ✅ Per-session | Connection-oriented |
| Correlation | ⭐⭐⭐ High | ✅ Yes | ✅ Indexed | Production services |
| State Emulation | ⭐⭐⭐⭐ Very High | ✅ Yes | ✅ Full model | Simple KV stores |
| Hybrid | ⭐⭐⭐⭐ Very High | ✅ Adaptive | ✅ Layered | General purpose |

---

## 9. References

- `CORRELATION_ARCHITECTURE.md` — Déjà's correlation implementation
- `SIGNATURE_VS_CORRELATION_ANALYSIS.md` — Empirical analysis of collision types
- `REPLAY_PIPELINE.md` — Socketpair-based replay architecture
- Speedscale documentation — Industry occurrence-cursor approach
- AREX documentation — Alternative correlation models

---

*Last Updated: 2026-05-08*
*Applies to: Déjà v1+ replay engine design*
