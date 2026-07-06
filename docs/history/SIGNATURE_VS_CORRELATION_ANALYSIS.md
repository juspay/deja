> **Archived.** This document records signature-vs-correlation analysis from before the 6-rank address ladder. It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Signature Lookup vs Causal Correlation

**Purpose:** Decide whether Déjà needs request correlation, or whether a sufficiently strong signature/key can make replay a pure lookup-table problem.

---

## 1. The Question

A dependency replay engine often looks like a lookup table:

```text
recording: signature(request) -> recorded_response
replay:    signature(candidate_request) -> response
```

If every signature is unique and stable, maybe request correlation is unnecessary.

The real question is:

> Can the signature be made unique enough for all relevant dependency interactions without knowing which inbound request or business flow caused the dependency event?

Answer:

> For some traffic, yes. In general, no. There is a fundamental ambiguity when identical dependency requests have different correct responses because of hidden state.

---

## 2. Two Different Collision Types

### 2.1 Accidental signature collision

Two different wire requests map to the same signature because the signature is too weak.

Example:

```text
GET /users/123?expand=a
GET /users/123?expand=b
```

Bad signature:

```text
method + path = GET /users/123
```

Fix:

```text
method + path + query + selected headers + body hash
```

This class of collision is solvable with better canonicalization.

### 2.2 Semantic collision / hidden-state ambiguity

Two dependency requests are byte-identical, but the correct response differs because external state changed.

Example:

```text
SET key1 v1
GET key1  -> v1
SET key1 v2
GET key1  -> v2
```

Both reads have the same request bytes:

```text
GET key1
GET key1
```

No hash of the request bytes can distinguish them. The missing discriminator is not in the request. It is in one of:

- sequence position
- connection state
- session/request scope
- modeled external state
- explicit protocol/application metadata

This is an information-theoretic problem, not a hashing problem.

---

## 3. Concrete Hyperswitch Evidence

Saved artifact inspected:

```text
/tmp/deja-savepoints/fixed-pipeline-success-20260506-193225/deja-pipeline/recording/events.jsonl
```

Redis command summary from that artifact:

```text
parsed Redis commands: 414
GET commands:           19
HGET commands:           2
SET commands:           30
EXPIRE commands:        28
XADD commands:          35
ECHO markers:          187
```

Duplicate full Redis read signatures found:

```text
GET merchant_key_store_merchant_1778075931                 count=2, same response
GET deja_merch_1778075931                                  count=3, same response
GET routing_default_pro_XPxhF9tDurGWEvEEvEna               count=2, same response
GET API_LOCK_deja_merch_1778075931_payments_pay_n6CE...    count=2, different responses
```

The important one:

```text
GET API_LOCK_deja_merch_1778075931_payments_pay_n6CE1O3e8gCIKIdnnrrZ
```

appeared twice with different recorded responses:

```text
response #1: 019dfd95-97e1-7822-b4f9-0b39adf14abb
response #2: 019dfd95-99d4-78b1-a00f-fe2a71c166fe
```

Nearby sequence shows the pattern:

```text
SET API_LOCK_... 019dfd95-97e1-... EX 180 NX
GET API_LOCK_... -> 019dfd95-97e1-...
DEL API_LOCK_...

SET API_LOCK_... 019dfd95-99d4-... EX 180 NX
GET API_LOCK_... -> 019dfd95-99d4-...
DEL API_LOCK_...
```

This is exactly the semantic-collision pattern: same read signature, different correct responses.

---

## 4. How Speedscale Handles This

Based on public docs, Speedscale uses signatures plus occurrence sequencing.

Their service mocking docs say:

```text
If multiple requests have the same signature but different responses,
Speedscale will cycle through the responses in order.
```

Their markdown RRPair format also includes an `instance` field in the signature:

```text
http:host is api.example.com
http:method is POST
http:url is /v1/users
instance is 0
```

So Speedscale's answer is approximately:

```text
signature + nth occurrence -> response
```

For the lock example:

```text
GET API_LOCK..., instance=0 -> first lock owner
GET API_LOCK..., instance=1 -> second lock owner
```

This is not causal correlation. It is a stateful cursor per signature.

### Where this works

It works if replay produces the same global order of ambiguous requests:

```text
recording: GET key -> A, GET key -> B
replay:    GET key -> A, GET key -> B
```

### Where this fails

It fails if concurrency or code changes alter the order:

```text
recording: Request A does GET key -> A
           Request B does GET key -> B

replay:    Request B reaches GET first -> receives A incorrectly
           Request A reaches GET second -> receives B incorrectly
```

The lookup table is internally consistent, but the ownership is wrong.

---

## 5. Do We Need Correlation?

It depends on which guarantee Déjà wants.

### 5.1 If the goal is dependency mocking only

Then correlation is not strictly required.

A replay engine can use:

```text
signature + occurrence cursor
```

This is enough for many demos and many local tests, especially when:

- traffic is single-threaded
- ambiguous reads are rare
- responses are stable
- ordering is deterministic
- operations are idempotent

This is the Speedscale-style model.

### 5.2 If the goal is robust regression detection

Correlation is needed, but correlation alone is not enough.

Without correlation, the replay engine cannot reliably answer:

```text
Which request/business flow owned this ambiguous GET?
Did this request execute the same dependency sequence as before?
Did the candidate version reverse, skip, or move a stateful operation?
```

However, adding only `request_id` to the signature is also insufficient:

```text
(scope_id, GET key1) -> ?
```

If the same scope does:

```text
GET key1 -> v1
SET key1 v2
GET key1 -> v2
```

then both reads still collide inside the same request/business scope.

For robust replay, the lookup key becomes:

```text
(scope_id, logical_sequence_index, dependency_signature) -> response
```

not just:

```text
dependency_signature -> response
```

and not merely:

```text
(scope_id, dependency_signature) -> response
```

### 5.3 If the goal is dependency-level diffing

Correlation is strongly needed.

A useful report should say:

```text
Request req-123:
  expected: SET lock A -> GET lock A -> DEL lock
  actual:   GET lock ? -> SET lock A -> DEL lock
```

A signature-only engine can only say:

```text
Some GET lock happened.
Some SET lock happened.
```

---

## 6. The Fundamental Replay Strategies

### Strategy A: Stateless signature lookup

```text
key = signature(request)
response = table[key]
```

Pros:
- simple
- no correlation required
- works for unique requests

Cons:
- fails for repeated identical reads with different responses

Verdict:
- insufficient for Hyperswitch-class stateful systems

### Strategy B: Signature + occurrence cursor

```text
key = signature(request)
response = table[key][cursor[key]++]
```

Pros:
- solves repeated identical signatures in deterministic order
- matches Speedscale's documented behavior

Cons:
- global cursor can misassign responses under concurrency
- cannot detect ownership/order regressions

Verdict:
- good fallback, not robust enough as the primary correctness model

### Strategy C: Scoped signature + occurrence cursor

```text
key = (scope_id, signature(request))
response = table[key][cursor[key]++]
```

Pros:
- separates ambiguous reads across concurrent requests
- much more stable under interleaving

Cons:
- still fails for repeated identical reads within the same scope if order changes

Verdict:
- strong practical baseline

### Strategy D: Scoped logical sequence

```text
key = (scope_id, logical_event_index)
expected_signature = recorded[index].signature
response = recorded[index].response

if candidate_signature != expected_signature:
    emit divergence
```

Pros:
- catches ordering changes
- catches skipped/extra dependency events
- strongest regression signal

Cons:
- less tolerant of harmless reorderings
- needs correlation/scope attribution

Verdict:
- best fit for Déjà's robust regression goal

### Strategy E: Protocol state model

For Redis:

```text
record SET key v1 -> update mock state[key] = v1
record GET key    -> return state[key]
```

Pros:
- solves hidden-state reads naturally
- may not need recorded response for simple Redis operations

Cons:
- hard for full Redis semantics
- much harder for SQL databases
- requires initial state seeding
- misses server-specific behavior unless modeled

Verdict:
- useful as an optimization for simple Redis commands, not a universal solution

---

## 7. Recommended Déjà Model

Déjà should not choose between signature and correlation. It should use a hierarchy.

### 7.1 Primary correctness key

```text
(scope_id, logical_sequence_index) -> recorded_event
```

This is the regression model.

During replay:

```text
actual_event.signature must equal recorded_event.signature
actual_response comes from recorded_event.response
```

If the candidate sends the wrong event at that point, report divergence.

### 7.2 Fallback mock key

For less strict dependency mocking:

```text
(signature, occurrence_cursor) -> response
```

This is useful when:

- no scope is available
- running in Speedscale-style mock mode
- background/system I/O is intentionally unscoped

### 7.3 Optional protocol-state fast path

For Redis commands with simple state semantics:

```text
SET/HSET/DEL/EXPIRE mutate mock state
GET/HGET read mock state
```

This can reduce dependence on occurrence cursors, but should be marked as protocol-specific and incomplete.

---

## 8. Hyperswitch Risk Assessment

The saved artifact already shows one risky pattern:

```text
GET API_LOCK_<merchant>_<payment> -> different lock owner values across occurrences
```

This means Hyperswitch is not purely safe under stateless signature lookup.

However, risk is workload-dependent.

### Lower-risk patterns

```text
GET merchant config -> same response across recording
GET routing config  -> same response across recording
```

These can work with plain signatures.

### Higher-risk patterns

```text
GET lock key after SET/DEL cycles
GET payment/session/tracker objects after updates
HGET payment intent / attempt fields after state transitions
```

These require at least occurrence cursors, and preferably scope + sequence.

### What to measure next

For any artifact, compute:

```text
ambiguous_signature_count = number of signatures with >1 distinct response
ambiguous_signature_rate  = ambiguous_signature_count / total_unique_signatures
```

Then classify ambiguous signatures by protocol and operation:

```text
Redis GET/HGET/EXISTS/TTL
Postgres SELECT
HTTP GET/POST to dependency
```

This tells us whether correlation is a must-have for a specific workload.

---

## 9. Final Position

Correlation is not needed for every event.

Correlation is needed when:

1. identical dependency requests can return different responses
2. concurrent requests can interleave those ambiguous requests
3. ordering/regression detection matters
4. per-request dependency reports matter

Hyperswitch already demonstrates at least one ambiguous Redis read pattern, so a pure stateless signature lookup is not sufficient.

The right design is layered:

```text
Strict replay:     scope + sequence + signature validation
Mock fallback:     signature + occurrence cursor
Protocol state:    Redis/DB-specific state model where feasible
Unscoped fallback: explicit low-confidence attribution
```

This lets Déjà support practical replay without giving up its stronger correctness guarantees.
