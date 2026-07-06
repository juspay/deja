> **Archived.** This document records a state-seeded replay comparison that was not adopted. It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Why Owned-State Seeded Replay Beats Response Cycling

**Purpose:** Explain, with concrete examples, why per-request owned-state seeded replay is fundamentally stronger than cycling through recorded DB/Redis responses by signature.

---

## 1. The Core Problem

A replay system needs deterministic answers for impure operations:

```text
Redis GET
Postgres SELECT
external HTTP call
time
randomness
```

For DB/Redis, a common mock design records request/response pairs:

```text
GET k -> v1
GET k -> v2
GET k -> v3
```

Then replay cycles through them:

```text
first GET k  -> v1
second GET k -> v2
third GET k  -> v3
```

This is response cycling.

It can work only if replay executes the same operations in the same order as recording. That assumption breaks for isolated request replay, concurrency, code changes, and stateful read-after-write flows.

---

## 2. Definitions

### Response Cycling

```text
signature(operation) -> queue(recorded_responses)
```

Example:

```text
signature(GET payment:pi_123) -> [requires_confirmation, processing, charged]
```

Replay consumes the queue as requests arrive.

### Owned-State Seeded Replay

```text
1. Infer the pre-request Redis/Postgres state needed by one inbound request.
2. Seed that state into isolated Redis/Postgres.
3. Run candidate code against real Redis/Postgres.
4. Validate response, ordered writes, and final state.
```

This does not mock owned DB/Redis reads. It lets the real dependency answer them from seeded state and candidate writes.

---

## 3. First-Principles View

For one request, behavior is roughly:

```text
(response, final_state, side_effects) =
  run(code, inbound_request, initial_state, time, randomness, external_services)
```

Therefore isolated replay needs one of:

```text
A. replay all previous requests to recreate state
B. mock every DB/Redis read response correctly
C. reconstruct the initial state slice for this request
```

If we want isolated replay, A is not acceptable. If repeated signatures are ambiguous, B is fragile. C is owned-state seeded replay.

---

## 4. Example 1: Same Read Before and After Write

Recording:

```text
GET payment:pi_123.status -> "requires_confirmation"
HSET payment:pi_123 status "processing" -> OK
GET payment:pi_123.status -> "processing"
```

Both reads have the same logical signature:

```text
HGET payment:pi_123 status
```

### Cycling Behavior

```text
signature(HGET payment:pi_123 status) -> [requires_confirmation, processing]
```

This works only if replay calls the two reads in the same order.

If candidate code changes ordering, adds a read, removes a read, or interleaves another request, the queue can return the wrong value.

### Owned-State Seeded Behavior

Seed only the pre-request state:

```text
payment:pi_123.status = "requires_confirmation"
```

Replay:

```text
GET payment:pi_123.status -> real Redis returns "requires_confirmation"
HSET payment:pi_123 status "processing" -> candidate mutates real Redis
GET payment:pi_123.status -> real Redis returns "processing"
```

The second read is not mocked. It is produced by the candidate's own write.

This is fundamentally better because the replay checks whether the candidate actually performed the state transition.

---

## 5. Example 2: Cycling Can Hide a Missing Write

Recording:

```text
GET balance:user_123 -> 100
SET balance:user_123 150 -> OK
GET balance:user_123 -> 150
```

Candidate bug:

```text
GET balance:user_123 -> 100
// BUG: forgot SET balance:user_123 150
GET balance:user_123 -> ?
```

### Cycling Failure

A response-cycling mock may still return:

```text
second GET balance:user_123 -> 150
```

The test may pass even though candidate code never wrote `150`.

This is a dangerous false pass.

### Owned-State Seeded Result

Seed:

```text
balance:user_123 = 100
```

Candidate replay:

```text
GET balance:user_123 -> real Redis returns 100
// missing write
GET balance:user_123 -> real Redis still returns 100
```

Now the regression is visible:

```text
recorded response/state expected 150
candidate produced 100
```

Owned-state replay catches the missing mutation because reads are coupled to actual candidate writes.

---

## 6. Example 3: Isolated Request Replay Breaks Global Queues

Recording order:

```text
Req A: GET lock:pay_1 -> "owner-A"
Req B: GET lock:pay_1 -> "owner-B"
```

Cycling table:

```text
GET lock:pay_1 -> [owner-A, owner-B]
```

Replay order changes:

```text
Req B replays first: GET lock:pay_1 -> owner-A  // wrong
Req A replays second: GET lock:pay_1 -> owner-B // wrong
```

The queue depends on global order. Isolated replay intentionally does not preserve global order.

### Owned-State Seeded Result

For Req A:

```text
seed lock:pay_1 = owner-A
replay Req A alone
```

For Req B:

```text
seed lock:pay_1 = owner-B
replay Req B alone
```

Each request gets its own initial world. There is no shared global response cursor to corrupt.

---

## 7. Example 4: Naively Seeding All Reads Is Also Wrong

State seeding must seed **pre-request state**, not every value read during the request.

Recording:

```text
GET k -> nil
SET k "abc" -> OK
GET k -> "abc"
```

Wrong fixture:

```text
k = "abc"
```

That would make the first replay read return `"abc"`, but recording observed `nil`.

Correct fixture:

```text
k is absent
```

Then replay should produce:

```text
GET k -> nil
SET k "abc" -> OK
GET k -> "abc"
```

This is why fixture generation needs operation order:

```text
reads before first write to resource -> initial fixture facts
reads after in-request write         -> validation observations
```

---

## 8. Example 5: Final State Alone Can Miss Ordering Bugs

Recording:

```text
SET x 1
SET y 2
```

Candidate:

```text
SET y 2
SET x 1
```

Final state:

```text
x = 1
y = 2
```

Final state is identical, but operation order changed.

Owned-state replay should therefore validate both:

```text
1. final state
2. ordered write log
```

Response cycling does not naturally validate either. A strict mock can validate operation order, but then it needs causal operation identity and sequence tracking anyway.

---

## 9. Why Cycling Is Fundamentally Weaker

| Dimension | Response Cycling | Owned-State Seeded Replay |
|---|---|---|
| Isolated request replay | Fragile; depends on global cursor/order | Natural; each request gets its own state slice |
| Read-after-write | Can return recorded post-write read even if write did not happen | Real dependency only returns post-write value if candidate writes it |
| Missing writes | Can be hidden by recorded read responses | Exposed by real state and final-state diff |
| Extra reads | Can shift response cursor and poison later reads | Real dependency answers consistently from state |
| Concurrency | Cursor order can diverge from causal order | Per-request sandbox avoids shared cursor corruption |
| Equivalent query changes | Often no exact recorded response | Real DB/Redis may answer from seeded state |
| Final state validation | Not inherent | Built in as a core validation layer |
| Write ordering validation | Requires extra strict tape logic | Explicit ordered write-log comparison |

---

## 10. Where Cycling Still Makes Sense

Cycling or full response mocking is still useful for dependencies whose internal state is not owned by the application:

```text
external HTTP APIs
payment processors
fraud APIs
email/SMS providers
S3-like APIs
third-party gRPC services
```

For these, replay should usually return recorded responses and validate request shape.

The rule is:

```text
Owned mutable state: seed it.
External service: mock it.
Nondeterministic primitive: intercept or wrap it.
```

---

## 11. What Owned-State Seeded Replay Requires

Owned-state replay is stronger, but not free. It requires:

```text
request correlation
local operation sequence
operation kind: read/write/delete
resource key extraction
pre-write vs post-write classification
negative facts for missing keys/rows
isolated Redis/Postgres namespace
ordered write-log capture
final touched-state snapshot or query
```

So correlation is still required. Its role changes:

```text
not only: choose the right recorded response
but: build the fixture and validate side effects
```

---

## 12. Practical Architecture

The recommended replay strategy is hybrid:

```text
Inbound HTTP/gRPC:
  replay recorded request

Redis/Postgres owned state:
  seed isolated pre-request state
  let candidate hit real dependency
  validate ordered writes and final state

External HTTP/gRPC:
  mock recorded responses
  validate request shape and order

Time/random/env:
  deterministic hooks or wrappers
```

This gives the best of both worlds:

```text
mocks for external systems
real state for owned mutable systems
```

---

## 13. Why This Is the Right Default for Déjà

Cycling asks:

```text
Which recorded response should I return for this operation signature?
```

Owned-state seeded replay asks:

```text
What world state did this request require, and did the candidate transform that world correctly?
```

The second question is closer to what regression testing actually needs.

For payment/state-machine systems, correctness is not just whether reads received familiar responses. Correctness is whether the code performed the right state transitions, in the right order, from the right initial state.

That is why owned-state seeded replay is fundamentally better than simply cycling through recorded responses.
