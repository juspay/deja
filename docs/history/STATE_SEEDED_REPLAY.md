> **Archived.** This document records a state-seeded replay design that was not adopted (V1 shipped full-mock lookup-table replay). It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# State-Seeded Isolated Replay

**Purpose:** Define a first-principles replay model for stateful owned dependencies such as Redis and Postgres, where Déjà reconstructs the request's initial dependency state instead of mocking every DB/Redis read response.

---

## 1. Core Thesis

For an isolated replay of one inbound request, the candidate program must run against a deterministic world.

For a request `R`, the behavior is approximately:

```text
(response, final_state, side_effects) =
  run(candidate_code, inbound_request, initial_state, time, randomness, external_services)
```

A replay system therefore needs one of three things:

1. **Full historical replay** — replay every previous request in the original order until request `R` is reached.
2. **Recorded response oracle** — intercept DB/Redis reads and return the exact recorded responses.
3. **Initial state oracle** — reconstruct the DB/Redis state that existed before request `R`, then let the candidate code hit real DB/Redis.

If the goal is **isolated request replay**, option 1 is not acceptable. If repeated read signatures are ambiguous, option 2 is fragile. That makes option 3 — state-seeded replay — the principled model for owned mutable dependencies.

---

## 2. Why Signature Queues Fail for Isolated Replay

Signature-based systems commonly record dependency request/response pairs:

```text
signature(GET k1) -> [v1, v2, v3]
```

Replay consumes the queue:

```text
first GET k1  -> v1
second GET k1 -> v2
third GET k1  -> v3
```

This only works when replay preserves the same call order as recording. It fails for isolated replay or concurrency changes:

```text
Recording:
  Req A: GET k1 -> owner-A
  Req B: GET k1 -> owner-B

Replay, different scheduling:
  Req B: GET k1 -> consumes owner-A  // wrong
  Req A: GET k1 -> consumes owner-B  // wrong
```

Cycling, FIFO queues, and vUser sequencing are all variants of order-dependent response lookup. They are not sufficient when requests should be replayed independently.

---

## 3. State-Seeded Replay Model

Instead of asking:

```text
Which recorded response should this read return?
```

State-seeded replay asks:

```text
What initial DB/Redis state would cause the real dependency to return the recorded values?
```

Then replay becomes:

```text
record phase:
  capture inbound request
  capture dependency reads/writes grouped by request
  infer the pre-request dependency-state slice

replay phase:
  create isolated Redis/Postgres namespace
  seed the inferred pre-request state
  run candidate against real Redis/Postgres
  compare HTTP response, external calls, DB/Redis writes, and final state
```

For owned dependencies, this removes the runtime read-response lookup ambiguity because DB/Redis reads are no longer mocked.

---

## 4. Correlation Is Still Required

State seeding reduces the need for correlation in **read response lookup**, but it does not remove correlation.

Déjà still needs request-level attribution to know:

1. Which DB/Redis reads belong to the request.
2. Which reads happened before the first write to the same resource.
3. Which writes were produced by the request.
4. Which final-state changes should be validated.
5. Which external API calls still need response mocking.

So correlation shifts roles:

```text
old role:
  choose the correct recorded response during replay

new role:
  construct the per-request fixture and validate side effects
```

This is a better use of correlation because it avoids returning synthetic DB/Redis responses while still preserving causality and ordering evidence.

---

## 5. The Crucial Distinction: Pre-State, Not All Reads

A naive implementation might seed every read result:

```text
GET k -> nil
SET k -> "abc"
GET k -> "abc"
```

Naive seed set:

```text
SET k "abc"
```

This is wrong. The first recorded read observed absence. The correct pre-request state is:

```text
k is absent
```

The second read is a consequence of the in-request write and should not be seeded.

Therefore the seed generator must derive **pre-request state**, not a bag of observed values.

Rule:

```text
For a resource X:
  reads before the first in-request write to X contribute to the initial fixture
  reads after an in-request write to X are validation observations, not seed facts
```

This requires operation ordering and resource identity.

---

## 6. Minimum Data Model

Each parsed dependency operation should carry:

```rust
struct DependencyOperation {
    request_id: Option<String>,
    global_index: u64,
    local_sequence_in_request: u32,
    protocol: Protocol,
    operation_kind: OperationKind, // read, write, delete, transaction, scan, etc.
    signature: OperationSignature,
    resource_keys: Vec<ResourceKey>,
    request_payload: Bytes,
    response_payload: Bytes,
    parsed_request: Option<ParsedOperation>,
    parsed_response: Option<ParsedResponse>,
}
```

For state-seeded replay, the most important fields are:

```text
request_id
local_sequence_in_request
operation_kind
resource_keys
parsed_request
parsed_response
```

Without request attribution and sequence, Déjà cannot tell which observations are pre-state versus request-produced state.

---

## 7. Fixture Synthesis Algorithm

For each inbound request `R`:

```text
1. Collect all dependency operations attributed to R.
2. Sort by local_sequence_in_request.
3. Maintain a set of resources written by R.
4. For each operation:
   a. If it is a read and none of its resources have been written yet:
        add read-derived facts to fixture.
   b. If it is a write/delete:
        mark affected resources as written.
   c. If it is a read after a write:
        add it to validation observations, not initial fixture.
5. Materialize fixture into an isolated Redis/DB namespace.
6. Run candidate request.
7. Validate response, writes, final state, and replay observations.
```

Pseudocode:

```rust
fn build_fixture(ops: &[DependencyOperation]) -> Fixture {
    let mut fixture = Fixture::default();
    let mut written = ResourceSet::new();
    let mut post_write_observations = Vec::new();

    for op in ops.sorted_by_key(|op| op.local_sequence_in_request) {
        match op.operation_kind {
            OperationKind::Read => {
                if op.resource_keys.iter().any(|k| written.contains(k)) {
                    post_write_observations.push(op.clone());
                } else {
                    fixture.add_facts(infer_facts_from_read(op));
                }
            }
            OperationKind::Write | OperationKind::Delete => {
                for key in &op.resource_keys {
                    written.insert(key.clone());
                }
            }
            OperationKind::Unknown => {
                fixture.add_ambiguity(op, "unknown operation kind");
            }
        }
    }

    fixture
}
```

---

## 8. Redis Fixture Synthesis

Redis is comparatively tractable because many operations map directly to state facts.

| Recorded Read | Fixture Fact |
|---|---|
| `GET k -> v` | `SET k v` |
| `GET k -> nil` | ensure `k` absent |
| `HGET h f -> v` | `HSET h f v` |
| `HGET h f -> nil` | ensure hash field `f` absent, and maybe hash exists/absent depending on other facts |
| `HGETALL h -> {f1:v1,f2:v2}` | `HSET h f1 v1 f2 v2`; optionally ensure no extra fields if strict |
| `MGET k1 k2 -> [v1,nil]` | `SET k1 v1`; ensure `k2` absent |
| `EXISTS k -> 1` | need value/type fact from another read, or mark incomplete |
| `TTL k -> 30` | need value + expiry fact; `TTL` alone is incomplete |
| `SCAN pattern -> keys` | ensure listed keys exist; strict mode also needs absence/namespace constraints |

Important Redis fact types:

```text
PresentString(key, value)
PresentHashField(hash, field, value)
AbsentKey(key)
AbsentHashField(hash, field)
KeyType(key, type)
Expiry(key, ttl_or_abs_deadline)
SetMembers(key, members)
ListItems(key, items)
SortedSetMembers(key, members_with_scores)
NamespaceCompleteness(prefix_or_pattern)
```

### Redis Negative Facts

Absence matters as much as presence:

```text
GET lock:pay_123 -> nil
SET lock:pay_123 owner NX -> OK
```

If replay starts with the lock present, candidate behavior changes. The fixture must explicitly ensure absence for `nil` reads and failed existence checks.

### Redis Strictness Modes

Some reads only partially constrain state:

```text
HGET h f -> v
```

This proves `h[f] = v`, but does not prove the hash has no other fields. Strictness should be configurable:

```text
minimal mode:
  seed only fields observed

strict mode:
  if HGETALL was observed, enforce exact hash contents
  if SCAN was observed, enforce namespace completeness for that pattern
```

---

## 9. SQL Fixture Synthesis

SQL is harder because query results often underdetermine database state.

Easy case:

```sql
SELECT * FROM payment_intent WHERE id = $1
```

If the response includes full row data, Déjà can seed:

```sql
INSERT INTO payment_intent (...) VALUES (...)
```

Hard cases:

```sql
SELECT COUNT(*) FROM payment_attempt WHERE merchant_id = $1;
SELECT EXISTS (...);
SELECT p.id, a.status FROM payment_intent p JOIN payment_attempt a ON ...;
SELECT * FROM payment_attempt WHERE status = 'processing' ORDER BY created_at LIMIT 1;
```

These results do not uniquely define the rows that must exist.

SQL fixture synthesis needs a tiered model:

### Tier 1: Row-Returning Primary-Key Reads

```text
SELECT * FROM table WHERE id = ?
```

Seed direct rows. Highest confidence.

### Tier 2: Row-Returning Filter Reads

```text
SELECT * FROM table WHERE merchant_id = ? AND status = ?
```

Seed returned rows. Optionally add strict absence constraints for exact result matching.

### Tier 3: Joins

Seed projected rows only if the result contains enough columns to reconstruct each table. Otherwise mark as partial and require schema/application hints.

### Tier 4: Aggregates and Existence Checks

`COUNT`, `SUM`, `EXISTS`, `LIMIT`, and `ORDER BY` often require synthetic rows or negative constraints. These should be classified as ambiguous unless supplemented by other row-returning reads.

### Tier 5: Transactions, Locks, Stored Procedures

These require real DB semantics and may not be invertible from captured result rows. Prefer full request fixture with schema-aware capture or mark as unsupported.

---

## 10. Validation Model

State seeding is not enough. Replay must validate what the candidate did.

Validation should include four layers:

### 10.1 Inbound Response Diff

Compare recorded HTTP/gRPC response with candidate response after noise filtering.

### 10.2 External API Call Diff

For non-owned dependencies, continue to mock and compare:

```text
Stripe/Fraud/Email/S3/etc.
```

### 10.3 Ordered DB/Redis Write Log Diff

Final state can hide ordering bugs.

Example:

```text
Recording:
  SET x 1
  SET y 2

Replay:
  SET y 2
  SET x 1

Final state:
  x=1, y=2  // same
```

Final state passes, but ordered write log catches the behavioral change.

The write log should compare:

```text
operation kind
resource key
essential payload fields
causal request id
relative order within request
transaction boundaries where available
```

### 10.4 Final State Diff

Compare selected DB/Redis resources after replay:

```text
recorded post-state slice vs candidate post-state slice
```

This catches cases where the write log looks similar but state differs due to DB triggers, Redis expiries, default values, or changed serialization.

---

## 11. Relationship to Mocking

State-seeded replay should not replace all mocking.

Recommended split:

| Dependency Type | Replay Strategy |
|---|---|
| Owned Redis/Postgres | seed isolated state; run real dependency |
| External HTTP APIs | mock recorded responses; compare calls |
| Payment processors | mock by default; optionally sandbox integration |
| Time/random/env | deterministic hook responses |
| Message queues | hybrid: seed queue/topic state or mock delivery, depending on use case |
| Filesystem/object storage | seed isolated bucket/path state where feasible |

The high-level rule:

```text
Owned mutable state: seed it.
External services: mock them.
Nondeterministic primitives: intercept them.
```

---

## 12. Isolated Namespace Design

To safely replay individual requests, each replay run needs isolated dependency state.

Redis options:

```text
1. Dedicated Redis instance per replay worker
2. Logical key prefix per replay: deja:{run_id}:<original-key>
3. Redis database index per replay, if application supports selecting DB
```

Postgres options:

```text
1. Dedicated database per replay worker
2. Schema per replay: deja_run_<id>
3. Transaction rollback after each request
4. Testcontainers/containerized Postgres per worker
```

For zero-code replay, dedicated instances or transparent connection-string rewriting are safer than requiring application-level key prefix changes.

---

## 13. Record-Time Requirements

To support state-seeded replay, recording should capture:

```text
1. Inbound request and recorded response.
2. Dependency operation stream with request attribution.
3. Parsed Redis commands/responses where possible.
4. Parsed SQL statements, bind values, returned rows where possible.
5. DB schema metadata for SQL fixture generation.
6. Ordered write/delete log.
7. Optional post-request state snapshot for touched resources.
```

The optional post-request snapshot is valuable because it gives final-state validation without requiring perfect semantic inference from write commands.

---

## 14. Why This Still Benefits from LD_PRELOAD

State-seeded replay might sound like it requires application instrumentation, but Déjà's boundary capture still helps:

1. It observes real Redis/Postgres traffic without app code changes.
2. It captures time/random/env deterministically.
3. It records external API calls at the same boundary.
4. It can validate write ordering at the socket/protocol level.
5. It works across languages and frameworks.

The main added requirement is protocol-aware extraction of state facts from captured bytes.

---

## 15. Implementation Phases

### Phase A: Artifact Analysis Only

Add `deja inspect --state-fixture` that outputs inferred facts without replaying.

Deliverables:

```text
per-request read set
pre-write facts
post-write observations
write log
ambiguity report
fixture confidence score
```

### Phase B: Redis Fixture Replay

Support simple Redis strings/hashes:

```text
GET, MGET, SET, DEL, EXISTS, HGET, HSET, HGETALL, HMGET
```

Run candidate against isolated Redis and validate write log + final state.

### Phase C: SQL Row Fixture Replay

Support primary-key and full-row SELECTs:

```text
SELECT * FROM table WHERE id = $1
```

Seed rows into isolated Postgres using captured schema metadata.

### Phase D: SQL Partial/Join/Aggregate Classification

Do not try to solve all SQL immediately. Classify confidence:

```text
exact_fixture
partial_fixture
aggregate_underdetermined
join_requires_schema_hints
unsupported_side_effecting_query
```

### Phase E: Hybrid Replay Orchestration

One command:

```bash
deja regress-live \
  --recording ./artifact \
  --target http://localhost:8080 \
  --owned-dep redis=redis://127.0.0.1:6379 \
  --owned-dep postgres=postgres://127.0.0.1/deja_replay
```

Pipeline:

```text
build fixture -> seed DB/Redis -> replay request -> capture outputs -> validate
```

---

## 16. Decision: Is This Necessary?

### Not necessary when:

```text
- replaying the whole workload in the exact original order is acceptable
- dependencies are immutable/read-only
- response mocking is sufficient and unambiguous
- only HTTP response diff is needed
```

### Necessary or strongly preferred when:

```text
- replaying one request in isolation
- DB/Redis state mutates between identical reads
- concurrency can reorder dependency calls
- final state matters
- side-effect ordering matters
- replay should catch state machine bugs, not merely return recorded responses
```

For Hyperswitch-class systems, isolated state-seeded replay is strongly preferred because payments, attempts, reverse lookups, volatile token flows, caches, and locks all depend on mutable DB/Redis state.

---

## 17. Final Architecture Recommendation

Déjà should adopt a hybrid model:

```text
Inbound HTTP/gRPC:
  replay recorded request

Owned Redis/Postgres:
  seed per-request isolated state
  run candidate against real dependency
  validate ordered writes and final state

External APIs:
  mock recorded responses
  validate outbound calls

Time/random/env:
  deterministic LD_PRELOAD hooks

Comparison:
  response diff + external call diff + write-log diff + final-state diff
```

This reframes Déjà's differentiator:

```text
Speedscale/AREX ask:
  Which recorded response should this dependency request receive?

Déjà should ask:
  What world state did this inbound request require, and did the candidate transform that world correctly?
```

That is the more principled model for isolated replay of stateful systems.
