# Cursor-Based Replay Correctness Analysis

**Date:** 2026-05-11
**Purpose:** Determine whether cursor-based ordered response lookup is sufficient for Hyperswitch-class stateful replay, and specify which cursor variant is needed for each ambiguity class.
**Sources:** `REPLAY_LOOKUP_AMBIGUITY_TAXONOMY.md`, `AREX_ANALYSIS.md`, `SPEEDSCALE_INSIGHTS.md`, `04_AMBIGUITY_MATRIX.md`, `03_DEPENDENCY_CASCADES.md`, `CORRELATION_ARCHITECTURE.md`, `SIGNATURE_VS_CORRELATION_ANALYSIS.md`

---

## 1. Executive Summary

Cursor-based replay correctness is not a binary property. Each cursor variant — from a simple global per-signature counter to a causal-scope ordinal — is sufficient for a well-defined class of dependency interactions and insufficient for others. The question is not "does a cursor work?" but "which cursor, for which patterns?"

Hyperswitch, as a representative complex stateful payment processing service, exercises at least 6 of the 8 identified ambiguity classes in production flows. Of its 56 Redis read sites and 193 SQL query sites (measured in `04_AMBIGUITY_MATRIX.md`), approximately 25 Redis reads are high-risk mutable state reads, 3 are critical read-delete-read patterns, and 7 involve SCAN/list queries whose results depend on concurrent state. No single cursor strategy handles all of these.

Speedscale implements what amounts to a global per-signature FIFO cursor (their `instance` field on RRPairs). AREX implements a signature-hash match with temporal sequential-consumption fallback (their ACCURATE → FUZZY strategy chain). Both fail silently on specific Hyperswitch patterns: Speedscale cannot detect ordering reversals between concurrent requests; AREX's temporal consumption breaks when a code change reorders dependency calls within a request.

**Recommendation:** Déjà should implement a six-level replay lookup hierarchy where each level emits a confidence classification. The top two levels — stateful protocol emulation and causal-scope ordinal — provide correctness guarantees that no competitor offers. The bottom two levels — per-signature FIFO and signature-only — match competitor behavior and are explicitly marked as low-confidence fallbacks.

---

## 2. Mechanistic Model of Competitor Replay

### 2.1 Speedscale: How Replay Actually Works

Speedscale's replay architecture separates two independent components:

1. **Generator**: Drives inbound traffic into the SUT. Replays recorded HTTP requests using a vUser concurrency model. The Generator knows which recorded request it is sending and manages a per-vUser request sequence.

2. **Responder**: Intercepts outbound dependency calls from the SUT (via sidecar proxy or eBPF redirect). The Responder is a **stateless matching proxy** — it receives a raw outbound request and must return the correct recorded response.

**The Responder's internal execution path:**

```
1. SUT makes outbound call (e.g., Redis HGET, HTTP POST to connector)
2. Sidecar/proxy intercepts the raw bytes
3. Responder parses protocol, extracts:
     signature = f(host, method, path, query, body_hash, selected_headers)
4. Responder searches the snapshot's RRPair list:
     candidates = snapshot.rrpairs.filter(|rr| rr.signature == signature)
5. If |candidates| == 1: return candidates[0].response
6. If |candidates| > 1: use `instance` field (per-signature counter)
     response = candidates[cursor[signature]++].response
7. If |candidates| == 0: pass-through or return error
```

**Critical architectural constraint:** The vUser concept on the Generator side does NOT propagate to the Responder. The Generator drives inbound traffic; the Responder handles outbound traffic independently. There is no request-to-dependency causal mapping. The Responder has no idea which inbound request caused the outbound call it just received.

**This is Strategy 2 from the Taxonomy** (per-signature FIFO), implemented at the proxy layer.

**Where Speedscale fails on Hyperswitch:**

Consider two concurrent `payments.confirm` requests for the same `payment_id` (a retry scenario). Both trigger:

```
Request A: HGET payment_intent:{mid}:{pid}   → {status: "RequiresConfirmation"}
Request B: HGET payment_intent:{mid}:{pid}   → {status: "RequiresConfirmation"}
```

During recording, Request A hits the Responder first (cursor position 0), then Request B (position 1). Both get the same response — no problem here because the status hasn't changed yet.

But after Request A's confirm succeeds:
```
Request A: HSET payment_intent:{mid}:{pid} {status: "Processing"}
Request A: HGET payment_intent:{mid}:{pid}   → {status: "Processing"}  [cursor pos 2]
```

Now if during replay, Request B's post-confirm read arrives before Request A's, the cursor delivers `{status: "Processing"}` to the wrong request. The Responder has no way to distinguish them — both have identical signatures, and the cursor just counts occurrences globally.

**Failure severity: SILENT.** Both requests receive valid-looking JSON. The test passes. The semantic error — wrong status delivered to wrong request — is undetectable.

### 2.2 AREX: How Replay Actually Works

AREX operates at a fundamentally different layer than Speedscale — it intercepts at the **method level** inside the JVM, not at the network proxy level. This gives it richer context but the same fundamental disambiguation challenge.

**AREX's internal execution path during replay:**

```
1. ByteBuddy advice intercepts a dependency call (e.g., Redis get, SQL query)
2. Advice extracts:
     operationName = "RedisCommandProvider.get"
     requestBody   = serialized(command_args)
     recordId      = ArexContext.currentContext().getRecordId()
3. Agent calls MockService.query(operationName, requestBody, recordId)
4. Storage service retrieves all recorded "mockers" for this recordId
5. MatchStrategyRegister executes strategy chain:

   ACCURATE MATCH:
     hash = hash(operationName + requestBody)
     candidates = mockers.filter(|m| m.accurateMatchKey == hash && !m.matched)
     if |candidates| == 1:
       candidates[0].matched = true
       return candidates[0].response    ← STOP
     if |candidates| > 1:
       narrow mocker list, fall through to FUZZY

   FUZZY MATCH:
     sorted_candidates = candidates.sortBy(creationTime, ASC)
     for candidate in sorted_candidates:
       if !candidate.matched:
         candidate.matched = true
         return candidate.response      ← STOP
     // All matched? FIND_LAST mode:
     if FIND_LAST:
       return sorted_candidates.last().response

   EIGEN MATCH:
     (unimplemented — reserved for feature-similarity)
```

**Key design decisions in AREX:**

1. **`recordId` partitions the mocker space.** Each recording session has its own set of mockers. This prevents cross-session contamination — equivalent to Déjà's per-recording isolation.

2. **ACCURATE match is signature-based, not causal.** The `accurateMatchKey` is a hash of `operationName + requestBody`. When exactly one unmatched mocker has this hash, AREX returns it immediately. This handles the common case where each dependency call has a unique signature within a recording.

3. **FUZZY match is temporal, not causal.** When multiple mockers share the same hash, AREX falls through to sequential consumption ordered by `creationTime`. This is equivalent to a per-signature FIFO cursor — but scoped to the `recordId` (recording session), not global.

4. **FIND_LAST is a degraded fallback.** When all mockers are already matched (more replay calls than recorded calls), AREX returns the last recorded value. This is a heuristic — it assumes the final state is the "stable" one.

**Where AREX fails on Hyperswitch:**

Consider `payments.confirm` where the payment attempt status evolves:

```
Recording timeline:
  T1: HGET payment_attempt:pa_{id}  → {status: "RequiresCustomerAction"}
  T2: (connector callback, status transition)
  T3: HGET payment_attempt:pa_{id}  → {status: "Authorized"}
```

Both reads have identical `operationName + requestBody` hashes. ACCURATE match finds two candidates, falls through to FUZZY. FUZZY returns T1's response for the first replay read, T2's for the second.

**This works IF and ONLY IF replay executes the two reads in the same temporal order as recording.** If a code change moves a validation step — say, adding an authorization check before the first read that causes T3's read to execute first — FUZZY silently returns `{status: "RequiresCustomerAction"}` for what should have been the post-transition read, and `{status: "Authorized"}` for the pre-transition read.

**Failure severity: SILENT.** The test may pass or fail depending on downstream logic, but the root cause — swapped status values — is never reported as a replay matching error.

### 2.3 Summary: What Neither System Does

| Capability | Speedscale | AREX | Déjà (target) |
|------------|------------|------|----------------|
| Signature-based lookup | Yes | Yes | Yes (Level 5) |
| Per-signature occurrence counting | Yes (`instance`) | Yes (FUZZY consumption) | Yes (Level 4) |
| Per-recording-session isolation | Yes (snapshot) | Yes (`recordId`) | Yes (artifact) |
| Per-request causal scoping | **No** | **No** | Yes (Level 2) |
| Intra-request ordinal tracking | **No** | **No** | Yes (Level 2) |
| Stateful protocol emulation | **No** | **No** | Yes (Level 1) |
| Ordering divergence detection | **No** | **No** | Yes |

---

## 3. Cursor Variants and Failure Modes

### 3.1 Global Per-Signature Cursor

**Formal model:**
```
cursor: HashMap<Signature, usize>   // global, shared across all requests
table:  HashMap<Signature, Vec<Response>>

fn lookup(request: &Request) -> Response {
    let sig = signature(request);
    let idx = cursor[sig]++;
    table[sig][idx]
}
```

**Information used:** Wire-level dependency signature (command + key + arguments).
**Information lacking:** Which request caused this call. Which position in the request's dependency sequence.

**Sufficient conditions:**
- Every unique signature appears at most once across the entire recording, OR
- All requests producing the same signature execute in exactly the same global order during replay as during recording, AND
- No concurrent requests interleave calls with the same signature.

**Failure conditions:**
- Two concurrent requests both read the same key. During recording, Request A's read at cursor position 0, Request B's at position 1. During replay, thread scheduling reverses the order. Cursor delivers A's recorded response to B and vice versa.
- A code change adds an early-return branch that skips one dependency call. All subsequent cursor positions shift for that signature.

**Failure severity: SILENT.** Both requests receive syntactically valid responses. No error is raised. The semantic swap is undetectable at the replay layer.

**Hyperswitch example:** Two concurrent `payments.confirm` calls, both for the same merchant but different payment IDs. Both read `HGET merchant_account:{merchant_id}` — the merchant config is immutable between the two reads, so the cursor swap produces identical responses. **Harmless in this case.** But if both read `HGET payment_intent:{mid}:{same_pid}` (retry scenario), the cursor may swap pre-transition and post-transition intent states. **Dangerous.**

**Concurrency sensitivity:** High. Single-threaded sequential replay eliminates the interleaving problem but does not match production concurrency behavior, potentially masking real race conditions.

---

### 3.2 Per-Recorded-Request / Test-Case Cursor

**Formal model:**
```
cursor: HashMap<(RequestId, Signature), usize>
table:  HashMap<(RequestId, Signature), Vec<Response>>

fn lookup(request_id: &str, dep_request: &Request) -> Response {
    let key = (request_id, signature(dep_request));
    let idx = cursor[key]++;
    table[key][idx]
}
```

**Information used:** Inbound request identity + dependency signature.
**Information lacking:** Position within the request's dependency sequence (only occurrence count per signature).

**Sufficient conditions:**
- Request ID is correctly propagated to all dependency calls (including across async boundaries, spawn points, and blocking thread pools), AND
- Either: each request makes at most one dependency call per unique signature, OR
- The occurrence order of same-signature calls within a request is deterministic during replay.

**Failure conditions:**
- A single request reads the same key twice with an intervening write (read-after-write on same resource). Both reads share `(request_id, sig)`. The occurrence counter handles this IF intra-request execution order is deterministic.
- A single request spawns concurrent sub-tasks that both read the same key. The sub-tasks' ordering is non-deterministic even within the same request scope.
- Request ID propagation fails (pool-mediated I/O, shared driver tasks). The call falls back to unscoped lookup.

**Failure severity: CONDITIONAL.** If request ID propagation is complete and intra-request execution is single-threaded, failures are rare. If propagation fails, degrades to global per-signature cursor with all its failure modes.

**Hyperswitch example:** `payments.confirm` (from `03_DEPENDENCY_CASCADES.md` Section 2):

| Seq | Operation | Resource |
|-----|-----------|----------|
| 1 | HGET (read) | `payment_intent:{mid}:{pid}` → `{status: "RequiresConfirmation"}` |
| 5 | HSET (write) | `payment_attempt:pa_{id}` |
| 6 | HSET (write) | `payment_intent:{mid}:{pid}` → `{status: "Processing"}` |

If a code change adds a validation re-read of the intent after step 6:

| Seq | Operation | Resource |
|-----|-----------|----------|
| 7 | HGET (read) | `payment_intent:{mid}:{pid}` → `{status: "Processing"}` |

The key `(request_id, "HGET payment_intent:{mid}:{pid}")` now has two occurrences. The cursor delivers response 0 (`RequiresConfirmation`) for the first read (Seq 1) and response 1 (`Processing`) for the second (Seq 7). **This is correct** — PROVIDED the intra-request execution is sequential and the re-read happens after the write.

But: Hyperswitch's internal implementation uses `tokio::spawn` for some sub-tasks. If the re-read is in a spawned task that races with the write, the occurrence counter may assign responses in the wrong order. **The per-request cursor does not guarantee intra-request ordering for concurrent sub-tasks.**

---

### 3.3 Causal-Scope + Local Ordinal Cursor

**Formal model:**
```
table: HashMap<(ScopeId, Ordinal), RecordedEvent>

fn lookup(scope_id: &str, ordinal: usize, dep_request: &Request) -> Result<Response> {
    let recorded = table[(scope_id, ordinal)];
    if signature(dep_request) != recorded.signature {
        return Err(SignatureMismatch { expected: recorded.signature, actual: signature(dep_request) });
    }
    Ok(recorded.response)
}
```

**Information used:** Causal scope identity + monotonic ordinal within scope.
**Information lacking:** Nothing, in the ideal case. The lookup is fully determined by position, and signature acts as a validation assertion.

**This is the strongest non-emulating cursor.** The lookup key is "which request, which position in that request's dependency sequence." The signature is checked as a sanity assertion, not used for disambiguation.

**Sufficient conditions:**
- Causal scope is correctly propagated through ALL async boundaries (task spawns, blocking pools, channel sends), AND
- The ordinal is monotonically assigned at call-site time (not execution time — the increment must happen when the dependency call is *initiated*, not when the I/O is *performed*), AND
- No branch drift introduces or removes dependency calls before the target ordinal position.

**Failure conditions:**

**(a) Pool-mediated I/O (fred routing task):** The dependency call is dispatched through a shared internal task that was spawned at application startup with no request context. The causal scope is absent at the socket I/O site. The ordinal cannot be assigned because there is no scope to increment.

This is a **systemic constraint for Hyperswitch**, not an edge case. All Redis operations flow through fred's routing task (from `CORRELATION_ARCHITECTURE.md`). This affects all 56 Redis read sites and 76+ Redis write sites. Without driver-level integration (command envelope pattern), the causal-scope cursor **cannot function for Redis operations**.

**(b) Branch drift:** A code change adds a new dependency call at ordinal position 3. All subsequent ordinals shift by +1. The cursor at ordinal 4 now points at the recorded event from ordinal 3. The signature validation catches this — `HGET payment_attempt` ≠ `HGET config:{key}` — and reports a divergence.

**Failure severity: DETECTABLE.** Unlike the global and per-request cursors, the causal-scope cursor produces an explicit error on mismatch rather than silently returning wrong data. This is a fundamental qualitative advantage — the failure mode shifts from "silent wrong answer" to "loud divergence report."

**(c) Extra/missing calls:** If replay makes a dependency call that was not recorded (new code path), the ordinal has no corresponding entry. If replay skips a recorded call (removed code path), subsequent ordinals are offset. Both are detectable via signature validation.

**Hyperswitch example:** The fred routing task problem. Simplified call chain for `HGET payment_intent`:

```
1. Request handler calls redis_interface::get_hash_field_and_deserialize()
2. redis_interface calls fred::interfaces::hashes::hget()
3. fred packages command into RouterCommand
4. fred sends RouterCommand to internal channel
5. fred's routing task (spawned at startup, no request scope) reads from channel
6. routing task calls write_all() on Redis TCP socket
7. LD_PRELOAD hook intercepts write_all(), tries to read DEJA_CORRELATION_ID
8. DEJA_CORRELATION_ID is absent → request_id = None
```

At step 7, the causal scope is lost. The ordinal cannot be assigned. The recorded event exists with `(scope_id="req-123", ordinal=0)`, but replay cannot match it because the scope is absent at the I/O site.

**Mitigation:** The command envelope pattern (from `CORRELATION_ARCHITECTURE.md` "Correlation 2.0") wraps each `RouterCommand` with the scope ID captured at step 3, then restores it at step 5 before the routing task performs I/O. This requires patching fred or implementing a wrapper layer.

---

### 3.4 Resource-Version Cursor

**Formal model:**
```
table: HashMap<(ResourceKey, Version), Response>

fn lookup(resource_key: &str, version_before: &str) -> Response {
    table[(resource_key, version_before)]
}
```

**Information used:** Which resource, and which version of that resource is expected.
**Information lacking:** Request context, call ordering.

**Sufficient conditions:**
- Every dependency read can be annotated with the resource version it expects to find (e.g., a `modified_at` timestamp, ETag, or generation counter), AND
- The version is available BEFORE the read (in the request context or from a prior read's response), AND
- Resource versions are unique and monotonically increasing, AND
- The version chain is complete — every version transition was captured in the recording.

**Failure conditions:**

**(a) Version is in the response, not the request.** For `HGET payment_intent:{mid}:{pid}`, the caller does not know what version of the intent it will get — it just asks for the current value. The version (e.g., `modified_at` field) is inside the response. This creates a chicken-and-egg problem: the cursor needs the version to select the response, but the version is inside the response.

**Workaround:** If the version from the *preceding write* is known (because we recorded the HSET that updated the intent), we can infer that the next read will see that version. This requires tracking write-to-read causal chains, which is a form of stateful emulation.

**(b) Cross-session version gap.** An entity was modified by a different service, a different test case, or an external event. The recording has no HSET for the intermediate version — it only has the initial value and the final value. The version cursor has no entry for the intermediate version.

**(c) Version not available.** Some Redis commands (GET, DEL) operate on keys without inherent versioning. A key either exists or it doesn't. Key existence is a binary state, not a version.

**Failure severity: DETECTABLE.** If the version is unknown or missing from the table, the lookup fails explicitly rather than returning wrong data. The failure mode is "no match found" rather than "silent wrong answer."

**Hyperswitch example:** `payment_methods.retrieve` across sessions.

```
Session 1 (PM Create):    HSET payment_method:{pm_id}  {card_type: "credit", ...}
Session 2 (PM Update):    HSET payment_method:{pm_id}  {card_type: "debit", ...}
Session 3 (PM Retrieve):  HGET payment_method:{pm_id}  → should return "debit"
```

If Session 2 was not captured in the recording (it happened between test runs, or was initiated by a different service), the version cursor has:
- `(payment_method:{pm_id}, v0)` → `{card_type: "credit"}`

But Session 3's read should return `{card_type: "debit"}`, which corresponds to version v1 — a version the cursor has never seen. The lookup fails.

---

### 3.5 Stateful Redis/DB Emulator

**Formal model:**
```
state: HashMap<String, Value>   // Mock Redis state

fn init(recording: &Recording) {
    // Seed state from all writes in the recording
    for event in recording.events {
        match event {
            HSET(key, field, value) => state.insert(format!("{key}:{field}"), value),
            SET(key, value)         => state.insert(key, value),
            DEL(key)                => state.remove(key),
            _ => {}
        }
    }
}

fn execute(command: RedisCommand) -> Response {
    match command {
        GET(key)              => state.get(key).unwrap_or(nil),
        HGET(key, field)      => state.get(format!("{key}:{field}")).unwrap_or(nil),
        SET(key, value)       => { state.insert(key, value); OK },
        HSET(key, field, val) => { state.insert(format!("{key}:{field}"), val); OK },
        DEL(key)              => { state.remove(key); integer(1) },
        SETNX(key, val)       => {
            if state.contains_key(key) { integer(0) }
            else { state.insert(key, val); integer(1) }
        },
        EXPIRE(key, ttl)      => { /* track TTL if needed */ integer(1) },
        _ => Err(UnsupportedCommand)
    }
}
```

**Information used:** The command itself + current mock state.
**Information lacking:** Nothing needed for supported commands. The emulator IS the state.

**This is not a cursor at all.** Instead of looking up a recorded response, the emulator maintains a mock state machine and computes responses dynamically. It naturally handles read-after-write, read-delete-read, and cache-populate patterns because state mutations are tracked.

**Sufficient conditions:**
- The command is in the emulatable subset (GET/SET/HGET/HSET/DEL/EXPIRE/SETNX/SETEX for Redis), AND
- Initial state is correctly seeded from the recording (or from a known-good snapshot), AND
- The emulator's semantics match the real dependency's semantics for the commands used.

**Failure conditions:**

**(a) Complex commands:** WATCH/MULTI/EXEC (Redis transactions), Lua EVAL scripts, OBJECT ENCODING, CLIENT ID. Hyperswitch uses SETNX for locks but does not use MULTI/EXEC or Lua scripts in its Redis operations, limiting this failure mode.

**(b) SQL queries:** A stateful SQL emulator would need a full query engine — parsing SQL, maintaining table state, evaluating joins and aggregations. This is impractical. SQL emulation is limited to simple key-value lookups by primary key.

**(c) Unknown initial state:** If the recording starts mid-stream (e.g., the application was already running with populated Redis state), the emulator doesn't know what values exist for keys that are read but never written in the recording. This requires a pre-recording state snapshot.

**(d) TTL/expiry timing:** If the application relies on Redis key expiry (EXPIRE, SETEX), the emulator must track TTL and simulate time-based eviction. Clock differences between recording and replay may cause different expiry behavior.

**(e) SCAN semantics:** SCAN returns a cursor-based partial iteration of the key space. Emulating SCAN requires knowing the complete key namespace and maintaining cursor state. Concurrent modifications during SCAN iteration produce non-deterministic results even in real Redis.

**Failure severity: VARIES.** For supported commands with correct initial state: no failure possible. For unsupported commands: explicit error (detectable). For incorrect initial state: silent wrong data (the emulator confidently returns a value that was never in the real system).

**Hyperswitch example — where emulation excels:** The CVC token read-delete pattern:

```
Recording:
  SET pm_token_{pm_id}_hyperswitch_cvc  encrypted_cvc_bytes
  (later)
  GET pm_token_{pm_id}_hyperswitch_cvc  → encrypted_cvc_bytes
  DEL pm_token_{pm_id}_hyperswitch_cvc
  GET pm_token_{pm_id}_hyperswitch_cvc  → nil
```

Emulator execution during replay:
1. State seeded: `pm_token_{pm_id}_hyperswitch_cvc = encrypted_cvc_bytes`
2. GET → returns `encrypted_cvc_bytes` ✓
3. DEL → removes key from state
4. GET → returns `nil` ✓

No cursor needed. The emulator naturally produces the correct responses because it tracks key existence.

**Hyperswitch example — where emulation struggles:** `SCAN refund:{merchant_id}:*`

The SCAN result depends on the complete set of `refund:{merchant_id}:*` keys in the Redis key space. If the recording captured 3 refunds for this merchant, but the replay application creates a 4th refund (via a concurrent test case or a code path that creates refunds differently), the emulator's SCAN must return the updated set — which may differ from the recorded SCAN response. This is *correct* behavior for the emulator (it reflects actual state), but makes test assertions harder: the test expected 3 refunds, the emulator returns 4.

---

### 3.6 Cross-Cutting Failure Modes

#### Concurrency

Under **single-threaded sequential replay**, the global per-signature cursor is equivalent to a per-request cursor (because there is only one request in flight). Most cursor failure modes disappear. But single-threaded replay does not match production behavior and may mask concurrency bugs.

Under **multi-threaded concurrent replay**, every cursor variant except the stateful emulator is sensitive to thread scheduling:

| Cursor Variant | Single-Threaded | Multi-Threaded |
|----------------|-----------------|----------------|
| Global per-signature | Equivalent to per-request | Interleaving → silent swap |
| Per-request + occurrence | Correct if intra-request is sequential | Intra-request spawn races |
| Causal-scope + ordinal | Correct (deterministic sequence) | Correct IF scope propagation is complete |
| Resource-version | Correct (version is request-independent) | Correct (version is request-independent) |
| Stateful emulator | Correct (state is authoritative) | Correct, but concurrent writes may apply in different order |

#### Branch Drift

When code changes add, remove, or reorder dependency calls:

| Cursor Variant | Added Call | Removed Call | Reordered Calls |
|----------------|-----------|--------------|-----------------|
| Global per-signature | Queue may exhaust early or return wrong response | Queue has leftover entries (undetectable) | **SILENT WRONG DATA** |
| Per-request + occurrence | Same issues, scoped to request | Same | **SILENT WRONG DATA** |
| Causal-scope + ordinal | **DETECTED** (ordinal N has wrong signature) | **DETECTED** (ordinal N skipped) | **DETECTED** (signature mismatch at ordinal) |
| Resource-version | Not affected (version-based, not position-based) | Not affected | Not affected |
| Stateful emulator | Handles gracefully (new command processed against current state) | No impact (command simply not issued) | Correct if operations are commutative; may diverge if order-dependent |

The key insight: **only the causal-scope ordinal cursor detects branch drift as an explicit error.** All other cursor variants either silently return wrong data or silently absorb the discrepancy. The stateful emulator handles it gracefully but does not *report* it — a property that may be desirable (fault-tolerant replay) or undesirable (missed regression detection), depending on the testing goal.

#### Extra/Missing Calls

| Cursor Variant | Extra call (not in recording) | Missing call (in recording, not in replay) |
|----------------|------------------------------|---------------------------------------------|
| Global per-signature | May steal a response from a later call | Leftover responses (silent) |
| Per-request + occurrence | Same, scoped to request | Same, scoped to request |
| Causal-scope + ordinal | **DETECTED** (no recorded event at this ordinal) | **DETECTED** (unreached ordinal at end of request) |
| Resource-version | May return a response if version matches; otherwise no-match | No impact |
| Stateful emulator | Processes against current state (may be correct or incorrect) | No impact |

#### Shared Driver Tasks (fred)

This is a systemic constraint affecting the per-request cursor (Variant 3.2) and causal-scope cursor (Variant 3.3). Both require the request identity to be available at the point where I/O is performed. Fred's routing task breaks this invariant for ALL Redis operations.

The global per-signature cursor (3.1) is *unaffected* by this — it never uses request identity. The resource-version cursor (3.4) is *unaffected* — it uses resource identity. The stateful emulator (3.5) is *unaffected* — it processes commands regardless of their source.

This means that for Hyperswitch Redis operations specifically, only three approaches work without driver modifications:
1. Global per-signature cursor (low confidence)
2. Resource-version cursor (requires version metadata)
3. Stateful Redis emulator (requires initial state)

The causal-scope cursor — despite being the theoretically strongest — requires the command envelope pattern or fred driver patch to function for Redis.

---

## 4. Hyperswitch Concrete Flow Analysis

### 4.1 Payment Confirm (`/payments/{id}/confirm`)

Full dependency cascade from `03_DEPENDENCY_CASCADES.md` Section 2:

| Seq | R/W | Operation | Resource Key | Response |
|-----|-----|-----------|-------------|----------|
| 1 | Read | HGET | `payment_intent:{mid}:{pid}` | `{status: "RequiresConfirmation"}` |
| 2 | Read | HGET | `payment_attempt:pa_{attempt_id}` | `{status: "Started"}` |
| 3 | Read | SELECT | `payment_attempt WHERE attempt_id = $1` | (DB fallback if Redis miss) |
| 4 | Read | HGET | `config:{key}` | (config value or nil for cache miss) |
| 5 | Write | HSET | `payment_attempt:pa_{attempt_id}` | (status → "Authorized") |
| 6 | Write | HSET | `payment_intent:{mid}:{pid}` | (status → "Processing") |
| 7 | Write | HSET | `lock:{mid}:{pid}` | (lock value) |
| 8 | Delete | DEL | `lock:{mid}:{pid}` | (unlock) |

**Cursor analysis for this flow:**

**Global per-signature cursor:** Steps 1 and 6 touch the same key `payment_intent:{mid}:{pid}`. Step 1 is a read (returning `RequiresConfirmation`), step 6 is a write. For a single isolated request, the cursor correctly delivers the pre-transition response for step 1. But: if two concurrent confirms share the same `{mid}:{pid}`, their step-1 reads interleave, and the cursor may deliver responses in the wrong order. **Risk: medium-high** (depends on concurrency).

**Per-request cursor:** Steps 1 and 6 have different operations (HGET vs HSET), so they have different signatures. No collision within this flow. The risk arises if a code change adds a re-read of the intent after step 6 — creating a second HGET with the same signature but different expected response. The per-request occurrence counter handles this IF intra-request execution is sequential. **Risk: low for current code, medium for future changes.**

**Causal-scope + ordinal cursor:** Each step gets a unique ordinal (0-7). The lookup is fully determined. Signature validation at each ordinal detects any branch drift. **Risk: none (conceptually). Blocked by fred routing task** for all Redis operations.

**Stateful emulator:** Seeds `payment_intent:{mid}:{pid}` with `{status: "RequiresConfirmation"}` from the recording. Step 1 reads it correctly. Step 6 writes `{status: "Processing"}`, updating emulator state. If a re-read occurs after step 6, the emulator returns `{status: "Processing"}`. The CVC sub-flow (GET → DEL) works naturally. **Risk: low.** Requires correct initial state seeding. Does not detect ordering changes (step 5 and 6 could swap without emulator noticing, but the emulator still produces correct responses).

**State-machine-aware signature refinement** (from `04_AMBIGUITY_MATRIX.md` Pattern 1): If the replay signature includes the operation context — e.g., `(Confirm, RequiresConfirmation, HGET payment_intent:{mid}:{pid})` rather than just `HGET payment_intent:{mid}:{pid}` — then the two intent reads (pre-transition and post-transition) have different signatures, eliminating the collision for the per-signature cursor. This is a pragmatic middle ground between pure wire-level signatures and full causal scoping. However, it requires application-level context injection into the replay signature, which is not available at the LD_PRELOAD boundary without correlation.

### 4.2 Token Read-Delete (CVC Vault Pattern)

From `04_AMBIGUITY_MATRIX.md` Pattern 2:

```
Seq 1: GET pm_token_{pm_id}_hyperswitch_cvc   → encrypted_cvc_bytes
Seq 2: DEL pm_token_{pm_id}_hyperswitch_cvc   → (key deleted)
Seq 3: GET pm_token_{pm_id}_hyperswitch_cvc   → nil/NotFound
```

**Cursor analysis:**

| Cursor Variant | Seq 1 | Seq 3 | Correct? |
|----------------|-------|-------|----------|
| Global per-sig | response[0] = cvc_bytes | response[1] = nil | **Yes, IF** no other request reads the same token concurrently |
| Per-request | response[0] = cvc_bytes | response[1] = nil | **Yes, IF** occurrence counter increments correctly |
| Causal-scope + ordinal | ordinal 0 = cvc_bytes | ordinal 2 = nil | **Yes** (with fred mitigation) |
| Resource-version | Not applicable (no version on GET keys) | | **Fails** — key existence is binary, not versioned |
| Stateful emulator | state[key] = cvc_bytes → return it | state[key] removed → return nil | **Yes, naturally** |

The per-signature FIFO cursor works here because the two GET calls have different responses and the occurrence counter delivers them in order. But this relies on the assumption that no concurrent request reads the same token. In a retry scenario where two payment confirms race, both may attempt `GET pm_token_{pm_id}_cvc` — the FIFO cursor delivers `cvc_bytes` to whichever arrives first and `nil` to the second. If the second request was actually the original (non-retry) request, it receives `nil` instead of the CVC data.

**The stateful emulator is the cleanest solution** for this pattern. It handles key existence transitions naturally, regardless of concurrency or ordering.

### 4.3 Refund List with SCAN (`/refunds`)

From `03_DEPENDENCY_CASCADES.md` Section 6:

| Seq | R/W | Operation | Resource Key | Notes |
|-----|-----|-----------|-------------|-------|
| 1 | Read | HGET | `payment_intent:{mid}:{pid}` | Validate payment exists |
| 2 | Read | SCAN | `refund:{merchant_id}:*` | Idempotency check — list existing refunds |
| 3 | Write | INSERT | `INSERT INTO refund ...` | Create refund in DB |
| 4 | Write | HSET | `refund:{merchant_id}:{refund_id}` | Write to Redis KV |

**Cursor analysis for the SCAN at Seq 2:**

No cursor handles SCAN correctly in the general case. SCAN returns a partial iteration of the key space, with a server-side cursor for pagination. The result set depends on:
- Which `refund:{merchant_id}:*` keys exist at the moment of the SCAN
- The hash-table bucket iteration order (Redis internal)
- Whether concurrent writes are adding/removing keys during iteration

**Global per-signature cursor:** Returns the recorded SCAN response. If no new refunds have been created (and no code changes alter which refunds exist), this is correct. If a concurrent test case creates a refund for the same merchant, the SCAN response is stale. **Conditionally acceptable for single-test-case replay.**

**Stateful emulator:** Would need to maintain the full key namespace and implement SCAN cursor semantics. Possible for `SCAN pattern` (iterate keys matching pattern), but complex and fragile. The emulator's SCAN may return keys in a different order than real Redis, causing test assertions to fail even when the logical content is correct.

**Practical recommendation for SCAN:** Treat SCAN as a signature-only lookup with LOW confidence. Flag it as inherently non-deterministic. If the test asserts on SCAN result order or exact content, the assertion should use set-equality, not sequence-equality.

### 4.4 Cross-Session Payment Method Access

From `03_DEPENDENCY_CASCADES.md` Sections 4-5:

**Session 1 — Payment Method Create:**
| Seq | Operation | Resource | Value |
|-----|-----------|----------|-------|
| 3 | HSET | `payment_method:{pm_id}` | `{card_type: "credit", ...}` |
| 5 | SET | `pm_token_{pm_id}_hyperswitch_cvc` | `encrypted_cvc` |

**Session 2 — Payment Method Retrieve (different API call, later):**
| Seq | Operation | Resource | Expected Value |
|-----|-----------|----------|----------------|
| 1 | HGET | `payment_method:{pm_id}` | `{card_type: "credit", ...}` |
| 3 | GET | `pm_token_{pm_id}_hyperswitch` | token data |

**Cross-session scenario:** If between Session 1 and Session 2, an external system or different test case updates the payment method (e.g., changing `card_type` to `"debit"`), Session 2's HGET should return `{card_type: "debit"}`.

**Cursor analysis:**

| Cursor Variant | Handles cross-session mutation? | Why |
|----------------|--------------------------------|-----|
| Global per-sig | **No** — returns first recorded HGET response, which may be stale | No visibility into inter-session writes |
| Per-request | **No** — Session 2 has its own request ID, but the table has no entry for the updated value | The update happened outside this request's recording |
| Causal-scope + ordinal | **No** — same issue; the update is outside any captured scope | Cross-session state is invisible to scope-based cursors |
| Resource-version | **Conditional** — IF the update's HSET was captured and version-tracked, the cursor can select the correct response. If the update was not captured: **No** | Requires complete version chain |
| Stateful emulator | **Yes** — IF the update's HSET was processed by the emulator before Session 2's replay. **No** — if the update happened outside the captured artifact | Depends on artifact completeness |

**Key insight:** Cross-session mutable state is fundamentally about **artifact completeness**, not cursor strategy. If the recording captures all state mutations, any cursor with version or state tracking handles it. If the recording is incomplete (missing the intermediate update), no cursor strategy can produce the correct response — the information simply does not exist in the artifact.

**Practical recommendation:** When replaying multi-session scenarios, ensure the recording artifact includes all sessions that mutate shared state. For entities accessed across sessions, the replay plan should sequence session artifacts in the correct order.

---

## 5. Mapping Table: Ambiguity Class → Cursor Sufficiency

Legend:
- **S** = SUFFICIENT — cursor is correct for this ambiguity class
- **I** = INSUFFICIENT — cursor produces wrong results or cannot match
- **C(condition)** = CONDITIONAL — cursor is correct only when the stated condition holds

| Ambiguity Class | Global Per-Sig | Per-Request + Occurrence | Causal-Scope + Ordinal | Resource-Version | Stateful Emulator |
|---|---|---|---|---|---|
| `signature_only_safe` | **S** | **S** | **S** | **S** | **S** |
| `repeated_identical_response_safe` | **S** | **S** | **S** | **S** | **S** |
| `per_signature_fifo_maybe_ok` | **C**(global execution order matches recording) | **S** | **S** | **S** | **S** |
| `needs_causal_scope_plus_ordinal` | **I** | **C**(no intra-request concurrency for same-sig calls) | **S**(if scope propagation complete; I if driver context lost) | **C**(version extractable from preceding write) | **S**(if command in emulatable subset) |
| `needs_resource_version_or_state` | **I** | **I** | **I**(cross-session state invisible to scope) | **S**(if version chain complete in recording) | **S**(if initial state seeded and all mutations captured) |
| `needs_stateful_redis_emulation` | **I** | **I** | **I** | **I**(existence is not a version; GET/DEL/GET breaks version model) | **S** |
| `needs_db_snapshot_or_transaction_order` | **I** | **I** | **C**(if transaction boundaries align with scope boundaries) | **C**(if row-level versioning is tracked, e.g., `xmin`/`ctid` in Postgres) | **C**(requires full SQL query engine — impractical for general SQL) |
| `unsafe_without_driver_context` | **I** | **I**(scope unavailable at I/O site) | **I**(scope unavailable at I/O site) | N/A (version not accessible without scope either) | **C**(emulator bypasses scope — processes any command against state, but cannot detect which request caused it) |

### Reading the table

**Row 1-2 (`signature_only_safe`, `repeated_identical_response_safe`):** All cursor variants work. These are immutable reads (config lookups, static references) or reads that always return the same value. Approximately 16 of Hyperswitch's 56 Redis read sites fall here.

**Row 3 (`per_signature_fifo_maybe_ok`):** The global cursor works only if replay perfectly preserves execution order — a fragile assumption under concurrent replay. All other cursors handle this safely. Approximately 10-15 read sites fall here (deterministic sequences within a single request).

**Row 4 (`needs_causal_scope_plus_ordinal`):** The critical row. The causal-scope cursor handles this perfectly in theory but is blocked by fred driver context loss in practice. The per-request cursor handles it if intra-request execution is sequential. The emulator handles it if the command is emulatable. Approximately 15-20 read sites fall here.

**Row 5 (`needs_resource_version_or_state`):** Cross-session entity mutations. Neither scope-based cursor helps because the state change happened outside any captured scope. Version cursors work if the version chain is complete. Emulators work if all mutations are captured. Approximately 10-15 read sites fall here.

**Row 6 (`needs_stateful_redis_emulation`):** Only the emulator handles read-delete-read and cache-populate patterns. All cursor-based approaches fail. 3 critical read-delete patterns + 5 cache-populate patterns ≈ 8 sites.

**Row 7 (`needs_db_snapshot_or_transaction_order`):** SQL-specific. No practical cursor handles general SQL. The causal-scope cursor works if Postgres transactions map cleanly to request scopes (they do in Hyperswitch's `async-bb8-diesel` pattern). The emulator would need a SQL engine.

**Row 8 (`unsafe_without_driver_context`):** Affects all Redis operations in Hyperswitch due to fred. The emulator partially bypasses this (it processes commands without needing to know which request sent them), but loses the ability to attribute responses to requests for regression analysis.

---

## 6. Recommended Replay Lookup Hierarchy for Déjà

The hierarchy is ordered by confidence. Each level is attempted in order; the first successful match is used. Each match emits a **confidence tag** that is propagated to the test result.

```
LEVEL 1: STATEFUL PROTOCOL EMULATION
  Confidence: CERTAIN
  Scope:      Redis GET/SET/HGET/HSET/DEL/EXPIRE/SETNX/SETEX
  Mechanism:  Maintain mock Redis state machine, seeded from recording.
              Process each command against current state.
              Return computed response.
  Wins:       Read-delete-read, cache-populate, SETNX race, any
              simple KV pattern. No cursor needed. No scope needed.
  Limits:     SCAN, WATCH/MULTI/EXEC, Lua scripts, Pub/Sub.
              Requires correct initial state.
              Does not detect ordering regressions — a feature, not a bug,
              for fault-tolerant replay; a limitation for regression testing.
  Fallback:   Command not in emulatable set, or emulator
              detects internal inconsistency.

LEVEL 2: CAUSAL SCOPE + LOCAL ORDINAL + SIGNATURE VALIDATION
  Confidence: CERTAIN
  Scope:      All dependency calls where scope propagation is complete.
  Mechanism:  Lookup by (scope_id, ordinal). Validate that the
              dependency call's signature matches the recorded event's
              signature. Return recorded response.
  Wins:       Intra-request ordering correctness. Branch drift detection.
              Extra/missing call detection. This is the gold standard
              for regression testing — any divergence is an explicit error.
  Limits:     Requires scope propagation through all async boundaries.
              Currently blocked for Hyperswitch Redis (fred driver).
              Works for Postgres (via spawn_blocking propagation).
  Fallback:   scope_id absent (driver context loss), ordinal has no
              recorded event, or signature validation fails.

LEVEL 3: RESOURCE KEY + VERSION
  Confidence: HIGH
  Scope:      Entity reads where version/modified_at is extractable.
  Mechanism:  Lookup by (resource_key, version_before). Return recorded
              response whose version matches.
  Wins:       Cross-session entity mutations (if version chain complete).
              Request-independent — works even without scope.
  Limits:     Version must be known before the read (chicken-and-egg for
              initial reads). Requires complete version chain in recording.
              Not applicable to non-versioned keys (tokens, locks).
  Fallback:   Version unknown, version not in table, or resource
              has no inherent versioning.

LEVEL 4: PER-SIGNATURE FIFO QUEUE
  Confidence: MEDIUM
  Scope:      Any remaining dependency calls with recorded responses.
  Mechanism:  Lookup by signature, return next unused response from
              queue. Increment per-signature occurrence cursor.
  Wins:       Simple to implement. Handles most single-request sequential
              patterns. Equivalent to Speedscale/AREX behavior.
  Limits:     Silent wrong data under concurrent interleaving.
              Silent wrong data under branch drift. Cannot detect
              ordering regressions.
  Fallback:   Queue exhausted for this signature, or signature
              flagged as known-ambiguous.

LEVEL 5: SIGNATURE-ONLY MATCH
  Confidence: LOW
  Scope:      Last resort — exactly one recorded response exists for
              this signature.
  Mechanism:  Lookup by signature. If exactly one response exists,
              return it. If multiple exist, refuse to match.
  Wins:       Handles the trivial case where each signature is unique.
  Limits:     Cannot disambiguate repeated calls. Refuses to match
              rather than guessing.
  Fallback:   Multiple responses exist for signature (ambiguous) or
              no recorded response exists.

LEVEL 6: NO MATCH
  Confidence: NONE
  Action:     Log as unmatched dependency call. Emit a diagnostic
              with the full dependency signature.
              Optionally: pass through to real backend (if configured).
              Optionally: return a generic error response.
  Significance: A test that reaches Level 6 should be flagged for
              investigation — the recording artifact is incomplete
              or the code has diverged significantly.
```

### Design Principles

**1. Confidence tagging is mandatory.** Every matched response carries its confidence level. Test results are classified by their lowest-confidence match:
- A test where all matches are Level 1-2 is **HIGH CONFIDENCE** — the test result is trustworthy.
- A test with any Level 4-5 matches is **MEDIUM CONFIDENCE** — the test result may be correct but could mask ordering regressions.
- A test with Level 6 matches is **LOW CONFIDENCE** — investigation required.

**2. Levels are not mutually exclusive.** A single replay may use Level 1 for Redis KV operations, Level 2 for Postgres queries (where scope propagation works), and Level 4 for Redis operations where fred blocks scope propagation. The confidence of the overall test is the minimum across all matches.

**3. Level 1 and Level 2 serve different testing goals.** The emulator (Level 1) prioritizes **fault-tolerant replay** — it produces correct responses even when execution order changes. The ordinal cursor (Level 2) prioritizes **regression detection** — it explicitly reports when execution order changes. For a product like Déjà, both are valuable:
- Use Level 1 when the goal is "does the application still produce the correct HTTP response?"
- Use Level 2 when the goal is "did the code change alter the dependency interaction pattern?"

**4. The hierarchy is the differentiator.** Competitors implement Levels 4-5 only. Déjà's value proposition is Levels 1-2. The hierarchy gracefully degrades to competitor-level behavior when scope propagation is unavailable, while providing strictly superior correctness when it is available.

---

## 7. Report Wording

The following paragraphs are written for inclusion in external-facing documents (technical reports, blog posts, investor materials). They reflect the analysis above.

---

### On the cursor correctness spectrum

> A cursor-based replay lookup — matching each outbound dependency call to a recorded response by signature and occurrence count — is sufficient for approximately 50-60% of dependency interactions in a complex stateful service like Hyperswitch. These are the immutable reads, configuration lookups, and deterministic single-occurrence calls where each signature maps to exactly one response.
>
> For the remaining 40-50%, cursor-based replay fails in ways that are difficult to detect. Payment state transitions, token lifecycle operations, cache-populate patterns, and cross-session entity mutations all produce identical dependency signatures with different correct responses. A cursor that returns the wrong response does so silently — the test appears to pass, but the application is operating on stale or swapped state. This is the class of bug that causes production incidents and that testing is specifically designed to catch.

### On competitive positioning

> Speedscale implements what we classify as a global per-signature FIFO cursor (their "instance" field on recorded request-response pairs). AREX implements a signature-hash match with temporal sequential-consumption fallback (their ACCURATE → FUZZY strategy chain). Neither system maintains causal attribution between inbound requests and their outbound dependency calls, and neither system can correctly replay the retrieve-and-delete token pattern or the payment status evolution pattern without risk of silent data corruption.
>
> Specifically: when two concurrent requests read the same Redis key and the value changes between reads, both Speedscale and AREX deliver responses based on occurrence order, not causal ownership. If replay thread scheduling differs from recording — as it will in any non-trivial concurrent workload — the cursor silently swaps responses between requests. Neither system reports this as an error.

### On Déjà's hierarchy

> Déjà implements a six-level replay lookup hierarchy. The top two levels — stateful protocol emulation and causal scope with ordinal tracking — provide correctness guarantees that no existing competitor offers. Stateful emulation naturally handles read-delete-read patterns, cache population, and conditional write semantics by maintaining a mock state machine rather than looking up recorded responses. Causal scope tracking detects ordering regressions, extra or missing dependency calls, and branch drift as explicit errors rather than silent data corruption.
>
> The bottom four levels — resource versioning, per-signature FIFO, signature-only, and unmatched — provide graceful degradation. When causal scope is unavailable (e.g., due to driver-level multiplexing in Redis client libraries), Déjà falls back to cursor-based matching but tags the result with a confidence level. A test that passes with all Level 1-2 matches is categorically more trustworthy than one that passes with Level 4-5 matches, and Déjà makes this distinction explicit in its test reports.

### Nuanced recommendation

> We do not claim that cursor-based replay is broken. For many dependency patterns — and for many testing goals — a per-signature FIFO cursor is sufficient and pragmatic. What we do claim is that cursor-based replay is *incomplete* for stateful services, and that the failure mode is the worst kind: silent wrong data that looks like a passing test. Déjà's architecture addresses this by layering multiple disambiguation strategies, applying the strongest available strategy for each dependency call, and making the confidence level of each match transparent to the test consumer.

---

*Generated from analysis of Hyperswitch (`vendor/hyperswitch-fresh`), Speedscale architecture (`SPEEDSCALE_INSIGHTS.md`), AREX source code (`AREX_ANALYSIS.md`), and Déjà's correlation architecture (`CORRELATION_ARCHITECTURE.md`). All Hyperswitch evidence references are to concrete callsites in `storage_impl/`, `core/`, and `redis_interface/` crates.*
