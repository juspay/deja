# Hyperswitch Replay Signature Ambiguity Report

**Date:** 2026-05-08  
**Repo analyzed for snippets:** `<repo-root>/vendor/hyperswitch-fresh`  
**Fresh checkout commit:** `bc39324410031bec3e8c3d0ba924d81841c0c341`  
**rust-brain source:** restored Hyperswitch snapshot with 219,675 indexed items; snapshot commit metadata was `unknown`.

> Note: `<repo-root>/vendor/hyperswitch` was not used for this report after the correction. A separate fresh checkout was created at `vendor/hyperswitch-fresh`.

---

## 1. Question

Can we avoid request correlation if every recorded dependency event is just a lookup table entry?

In other words, if replay is:

```text
signature(request) -> recorded_response
```

then maybe correlation is unnecessary if the signature/key is collision-free.

The problematic pattern is:

```text
SET key1 v1
GET key1  -> should return v1
SET key1 v2
GET key1  -> should return v2
```

Both `GET key1` requests have the same lookup signature if the signature is only command + key. The correct response depends on hidden mutable state or ordering.

---

## 2. Short Answer

For Hyperswitch, **signature-only replay is not sufficient** for Redis/DB-backed flows.

I found at least:

| Category | Count | Meaning |
|---|---:|---|
| Direct production Redis read primitive call sites | 31 | Calls like `get_and_deserialize_key`, `get_hash_fields`, `get_hash_field_and_deserialize`, `hscan_and_deserialize` |
| Application-level direct Redis read call sites excluding generic wrappers | 27 | Same as above, excluding `kv_wrapper` and `get_or_populate_redis` internals |
| KV-store Redis read call sites through `KvOperation` | 29 | `HGet`, `Get`, `Scan` through Hyperswitch's Redis KV abstraction |
| Total concrete Redis read sites that can create replay lookup ambiguity | **56** | 27 direct app reads + 29 KV-store reads |
| KV-store Redis write call sites | 21 | `Hset`, `HSetNx`, `SetNx` through KV abstraction |
| Direct production Redis write/delete primitive call sites, excluding pub/sub | 76 | `serialize_and_set_*`, `set_hash_fields`, `delete_key`, etc. |
| Production functions containing both Redis read and Redis write/delete | 4 real functions | Strong direct read/write ambiguity candidates |
| Conservative SQL read/query call sites in storage/router DB code | 193 | Static count of `find_by_*`, `find_optional_by_*`, `list_*`, `filter_by_*`, and direct `find(...)` model calls |
| Conservative SQL write call sites in storage/router DB code | 67 | Static count of `insert*`, `update*`, and `delete*` model calls |
| Conservative production functions containing both SQL read and SQL write | 4 | Strong direct same-function SQL state ambiguity candidates |

**Interpretation:** Hyperswitch has enough mutable Redis/DB access that a pure request-signature lookup table will eventually become ambiguous unless replay has one of:

1. stateful dependency emulation,
2. exact global ordering,
3. per-resource/per-signature response cursors,
4. causal request/session correlation + sequence index,
5. or a hybrid of the above.

Correlation is not the only solution, but **some disambiguator beyond request signature is required**.

---

## 3. Why Signature Alone Fails

A Redis `GET`, `HGET`, `HGETALL`, or SQL `SELECT` can be textually identical while returning different values at different times.

Example:

```text
recording:
  HSET payment_attempt pa_1 { status: Pending }
  HGET payment_attempt pa_1 -> { status: Pending }
  HSET payment_attempt pa_1 { status: Charged }
  HGET payment_attempt pa_1 -> { status: Charged }

signature(HGET payment_attempt pa_1) is identical for both reads.
```

If replay stores:

```text
HGET payment_attempt pa_1 -> response
```

then there is no information in the key to decide which response is correct.

Even adding a request id is not fully sufficient:

```text
Request A:
  GET key1 -> v1
  SET key1 -> v2
  GET key1 -> v2
```

Both reads are in the same request scope. The lookup key `(request_id, GET key1)` still collides. You also need an **occurrence index / sequence number** or a **stateful model**.

---

## 4. Concrete Redis Counts

### 4.1 Direct Redis primitive reads

Production Redis read primitive counts:

| Method | Count |
|---|---:|
| `get_and_deserialize_key` | 22 |
| `get_hash_field_and_deserialize` | 4 |
| `get_hash_fields` | 4 |
| `hscan_and_deserialize` | 1 |
| **Total** | **31** |

Representative locations:

| File | Line | Operation |
|---|---:|---|
| `crates/router/src/core/payment_methods.rs` | 5208 | volatile payment method read by `pm_id` |
| `crates/router/src/db/payment_method_session.rs` | 98 | payment method session read |
| `crates/router/src/core/payments/client_session.rs` | 144 | client session read |
| `crates/router/src/core/payment_methods/vault.rs` | 2249 | CVC token read |
| `crates/router/src/core/payment_methods/vault.rs` | 2392 | vault payload read |
| `crates/router/src/core/payment_method_balance.rs` | 389 | payment method balance hash read |
| `crates/router/src/core/payment_method_balance.rs` | 446 | fallible payment method balance hash read |
| `crates/router/src/core/payments/types.rs` | 351 | hash field read |
| `crates/router/src/db/ephemeral_key.rs` | 150 | ephemeral key hash field read |

### 4.2 KV-store Redis reads

Hyperswitch also has a generic Redis KV layer. These are not always direct Redis calls in business code, but they still become Redis commands at runtime.

KV read call sites excluding the wrapper implementation itself:

| KV operation | Count |
|---|---:|
| `KvOperation::<T>::HGet(...)` | 20 |
| `KvOperation::<T>::Get` | 2 |
| `KvOperation::<T>::Scan(...)` | 7 |
| **Total** | **29** |

Representative locations:

| File | Line | Operation |
|---|---:|---|
| `crates/storage_impl/src/kv_router_store.rs` | 265 | generic `find_resource_by_id` via `HGet` |
| `crates/storage_impl/src/kv_router_store.rs` | 334 | generic optional find via `HGet` |
| `crates/storage_impl/src/payments/payment_intent.rs` | 448, 510, 694 | payment intent `HGet` |
| `crates/storage_impl/src/payments/payment_attempt.rs` | 1262, 1583, 1655, 1741, 1867 | payment attempt `HGet` |
| `crates/storage_impl/src/payments/payment_attempt.rs` | 1319, 1392, 1464, 1936 | payment attempt `Scan` |
| `crates/router/src/db/refund.rs` | 581, 1001, 1083 | refund `HGet` |
| `crates/router/src/db/refund.rs` | 810, 1131 | refund `Scan` |
| `crates/router/src/db/mandate.rs` | 119, 177 | mandate `HGet` |
| `crates/router/src/db/address.rs` | 386 | address `HGet` |

### 4.3 KV-store Redis writes

KV write call sites excluding wrapper internals:

| KV operation | Count |
|---|---:|
| `Hset` | 9 |
| `HSetNx` | 10 |
| `SetNx` | 2 |
| **Total** | **21** |

These writes update exactly the same entity families later read through `HGet`, `Get`, and `Scan`: payment intent, payment attempt, refund, address, mandate, payout, reverse lookup.

---

## 5. Highest-Risk Hyperswitch Patterns

### 5.1 Payment Attempt lifecycle: same Redis hash field, changing value

Path:

`crates/storage_impl/src/payments/payment_attempt.rs`

`update_payment_attempt_with_attempt_id` updates a Redis hash field:

```rust
let key = PartitionKey::MerchantIdPaymentId {
    merchant_id: &this.processor_merchant_id,
    payment_id: &this.payment_id,
};
let field = format!("pa_{}", this.attempt_id);
...
KvOperation::Hset::<DieselPaymentAttempt>((&field, redis_value), redis_entry)
```

Reads of the same logical object occur through `HGet` and `Scan` in the same file:

```rust
KvOperation::<DieselPaymentAttempt>::HGet(&field)
KvOperation::<DieselPaymentAttempt>::HGet(&lookup.sk_id)
KvOperation::<DieselPaymentAttempt>::Scan("pa_*")
```

**Ambiguity:**

```text
HGET mid_{merchant}_pid_{payment} pa_{attempt}
```

can return different attempt statuses as the payment moves through `Pending`, `Authorized`, `Charged`, `Failure`, etc. A signature that only includes hash key + field cannot pick the right recorded response.

### 5.2 Payment Intent lifecycle: same intent field, changing value

Path:

`crates/storage_impl/src/payments/payment_intent.rs`

Representative read:

```rust
KvOperation::<DieselPaymentIntent>::HGet(&field)
```

Payment confirm/update flows update the same payment intent object. The same `payment_id` is session-scoped for a payment lifecycle, but the intent value changes during that lifecycle.

**Ambiguity:** same `HGET` signature can validly produce old or new intent state.

### 5.3 Reverse lookup indirection: `reverse_lookup_{id}` points to mutable entity

Paths:

- `crates/storage_impl/src/lookup.rs`
- `crates/router/src/db/reverse_lookup.rs`
- `crates/storage_impl/src/payments/payment_attempt.rs`

Reverse lookup reads:

```rust
KvOperation::<DieselReverseLookup>::Get
KvOperation::<ReverseLookup>::Get
```

Payment attempt insertion/update creates reverse lookup entries before or alongside main entity updates.

**Ambiguity:** reverse lookup may be stable, but it maps to a target object whose value changes. The ambiguity moves from `GET reverse_lookup_*` to the subsequent `HGET pk/sk`.

### 5.4 Volatile payment method record

Path:

`crates/router/src/core/payment_methods.rs:5195`

```rust
let payment_method = redis_conn
    .get_and_deserialize_key::<diesel_models::PaymentMethod>(&pm_id.into(), "PaymentMethod")
    .await
```

Payment method ids can live across multiple calls/sessions. If the same `pm_id` is fetched in multiple recordings but the stored value changes, signature-only replay cannot distinguish versions.

### 5.5 Retrieve-and-delete token pattern

Path:

`crates/router/src/core/payment_methods/vault.rs:2235`

```rust
let resp: Encryption = redis_conn
    .get_and_deserialize_key::<Encryption>(&key.clone().into(), "Vec<u8>")
    .await?;
...
redis_conn.delete_key(&key.into()).await
```

This is directly stateful:

```text
GET token_key -> value
DEL token_key
GET token_key -> NotFound
```

A signature-only table cannot know whether a later identical `GET token_key` should return the original value or NotFound unless replay tracks order/state.

### 5.6 Cache population pattern

Path:

`crates/storage_impl/src/redis/cache.rs:306`

```rust
let redis_val = redis
    .get_and_deserialize_key::<T>(&key.into(), type_name)
    .await;
...
redis.serialize_and_set_key(&key.into(), &data).await?;
```

This pattern is benign in normal operation, but it is replay-ambiguous:

```text
GET config_key -> NotFound
SET config_key computed_value
GET config_key -> computed_value
```

The two `GET config_key` operations have the same signature but different expected responses.

---

## 6. How Many Ambiguous Redis Query Instances?

There are two useful answers:

### Conservative answer: 4 directly proven read/write functions

Production functions containing both Redis read and Redis write/delete:

| Function | File | Risk |
|---|---|---|
| `retrieve_and_delete_cvc_from_payment_token` | `crates/router/src/core/payment_methods/vault.rs` | Read then delete same token key |
| `retrieve_payment_token_data` | `crates/router/src/core/payments/helpers.rs` | Large token retrieval flow with Redis reads and writes/deletes |
| `get_or_populate_redis` | `crates/storage_impl/src/redis/cache.rs` | Read-miss-populate cache pattern |
| `kv_wrapper` | `crates/storage_impl/src/redis/kv_store.rs` | Generic Redis KV read/write gateway |

This is the strictest count: functions where static scanning sees Redis read and Redis mutation inside the same function.

### Practical answer: at least 56 Redis read sites can become ambiguous

If replay signatures are command + key/field only, then every Redis read site is potentially ambiguous when the same key is read at different resource versions.

Count:

```text
27 direct application-level Redis primitive reads
+29 KV-store Redis reads
=56 concrete Redis read sites
```

This is the more useful number for replay design: these are the places where the responder may receive an identical read signature but need to return a different value depending on state/order.

### High-risk subset: payment/intent/attempt/refund/mandate/address KV reads

Within the 29 KV reads, the highest risk subset is entity lifecycle state:

| Entity family | Read pattern | Why risky |
|---|---|---|
| PaymentIntent | `HGET payment_id field` | status/active attempt changes during payment lifecycle |
| PaymentAttempt | `HGET` / `SCAN pa_*` | status, connector txn id, error fields, amount fields mutate repeatedly |
| Refund | `HGET` / `SCAN` | refund status changes after gateway sync/webhook |
| Mandate | `HGET` | connector mandate id/status can be updated |
| Address | `HGET` | less frequent, but mutable record |
| ReverseLookup | `GET reverse_lookup_*` | stable indirection, but target object mutable |

---

## 7. SQL / Postgres Query Ambiguity

Redis is the clearest place to explain mutable-read ambiguity because keys and hash fields are explicit. SQL has the same problem in a more complex form.

A static scan of `crates/storage_impl/src` and `crates/router/src/db` in the fresh checkout found conservative SQL call-site counts:

| SQL pattern | Count |
|---|---:|
| `find_by_*` model calls | 139 |
| `find_optional_by_*` model calls | 13 |
| `list_*` model calls | 29 |
| `filter_by_*` / constraint filter calls | 11 |
| direct model `find(...)` calls | 1 |
| **Total conservative SQL read/query sites** | **193** |
| `insert*` calls | 4 |
| `update*` calls | 35 |
| `delete*` calls | 28 |
| **Total conservative SQL write sites** | **67** |

These counts intentionally exclude many ambiguous cases rather than over-counting them; they are a lower-bound style scan of model-level query calls in storage/router DB code.

The conservative same-function mixed SQL read+write candidates are:

| Function | File | Pattern |
|---|---|---|
| `delete_merchant_connector_account_by_merchant_id_merchant_connector_id` | `crates/storage_impl/src/merchant_connector_account.rs` | read before delete |
| `delete_merchant_connector_account_by_id` | `crates/storage_impl/src/merchant_connector_account.rs` | read before delete |
| `delete_merchant_account_by_merchant_id` | `crates/storage_impl/src/merchant_account.rs` | read before delete |
| `update_api_key` | `crates/router/src/db/api_keys.rs` | read before update |

This does not mean only four SQL flows are replay-ambiguous. It means only four functions were directly identified by a strict same-function read+write scan. Across request lifecycles, the 193 read sites and 67 write sites can still interleave through higher-level payment, refund, merchant, customer, and API-key flows.

For SQL replay, pure SQL-text signatures have the same weakness as Redis signatures:

```text
SELECT * FROM payment_attempt WHERE payment_id = $1 AND attempt_id = $2
```

can return different rows before and after an update using the same bind values. A SQL replay engine therefore needs one of:

- transaction/session ordering,
- causal scope + local query ordinal,
- table/row version tracking,
- or a stateful database emulator/snapshot.

## 8. Do We Need Correlation?

### Not always.

Correlation is not fundamentally required if replay is **stateful**.

For Redis, a stateful replay engine could apply writes to a mock Redis state:

```text
record: HSET key field v1
replay: update mock_state[key][field] = v1

record: HGET key field
replay: return mock_state[key][field]

record: HSET key field v2
replay: update mock_state[key][field] = v2

record: HGET key field
replay: return mock_state[key][field]
```

In that model, `HGET key field` does not need request correlation because state determines the answer.

### But for lookup-table replay, yes.

If replay remains a lookup table:

```text
signature -> recorded response
```

then signature alone is not enough for mutable reads. You need at least one disambiguator:

| Strategy | Works? | Failure mode |
|---|---|---|
| Pure signature | No | Same read signature can have multiple valid responses |
| Per-signature FIFO queue | Sometimes | Breaks under concurrency/interleaving |
| Global event index | Sometimes | Requires exact deterministic global ordering |
| Per-session sequence | Better | Fails with shared pools/background tasks |
| Request correlation | Better | Still needs per-request occurrence index for repeated reads |
| Stateful Redis/DB emulation | Best for Redis; hard for SQL | Requires implementing enough dependency semantics |
| Hybrid | Recommended | Use state where feasible, correlation/sequence for the rest |

### Important correction

A request id in the signature is not sufficient by itself.

You need something like:

```text
(causal_scope_id, dependency_signature, occurrence_index)
```

or:

```text
(resource_key, version_index)
```

or a stateful replay engine.

---

## 9. Recommendation for Déjà

For Hyperswitch, do not rely on pure signature lookup.

Recommended design:

1. **Classify dependencies by statefulness**
   - Redis simple KV/hash: can be state-emulated.
   - SQL: harder; use query signature + sequence/correlation.
   - External HTTP connectors: often signature + transforms may be enough, but still use occurrence index.

2. **For every recorded dependency event store:**

```text
global_index
connection_id / fd
operation_signature
resource_key
response
causal_scope_id (if available)
local_sequence_in_scope
state_version_before/after (if derivable)
```

3. **Replay lookup hierarchy:**

```text
1. Exact causal_scope_id + local_sequence + signature
2. Resource key + version/cursor
3. Per-signature FIFO cursor
4. Fuzzy/signature-only fallback with warning
```

4. **Emit ambiguity diagnostics during recording:**

```text
signature = HGET mid_m1_pid_p1 pa_a1
responses_seen = 4 distinct values
classification = ambiguous_mutable_read
recommended_disambiguator = state_version or causal_scope + ordinal
```

This answers the central question: **we do not necessarily need correlation for every dependency call, but we do need a disambiguation mechanism.** In Hyperswitch, mutable Redis/DB patterns are common enough that signature-only replay will be unsafe.

---

## 10. Repro Commands Used

All scans were run against:

```bash
<repo-root>/vendor/hyperswitch-fresh
```

Representative commands:

```bash
rg -n "get_and_deserialize_key|get_hash_field_and_deserialize|get_hash_fields|hscan_and_deserialize" crates --glob '*.rs'
rg -n "KvOperation::<.*(HGet|Scan|Get)|KvOperation::(HGet|Scan|Get)" crates/storage_impl/src crates/router/src/db --glob '*.rs'
rg -n "serialize_and_set_key|set_hash_fields|delete_key|KvOperation::Hset|KvOperation::HSetNx|KvOperation::SetNx" crates --glob '*.rs'
```

Conservative SQL call-site scan used these source roots:

```bash
crates/storage_impl/src
crates/router/src/db
```

and counted model-level read patterns:

```text
::find_by_*, ::find_optional_by_*, ::list_*, ::filter_by_*, ::find(...)
```

plus write patterns:

```text
::insert*, ::update*, ::delete*, diesel::insert_into(...), diesel::update(...), diesel::delete(...)
```
