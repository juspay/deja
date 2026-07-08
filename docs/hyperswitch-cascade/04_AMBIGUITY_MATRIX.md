# Hyperswitch Replay Ambiguity Matrix

**Date:** 2026-05-08  
**Source:** `<repo-root>/vendor/hyperswitch-fresh`

---

## Executive Summary

| Category | Count | Ambiguity Level |
|----------|-------|-----------------|
| Total Redis read sites | 56 | - |
| High-risk mutable reads | ~25 | CRITICAL |
| Medium-risk cross-session reads | ~15 | HIGH |
| Low-risk immutable reads | ~16 | LOW |
| Read-delete-read patterns | 3 | CRITICAL |
| Cache populate patterns | 5 | HIGH |
| SCAN/list queries | 7 | HIGH |

---

## Ambiguity Classification Definitions

| Classification | Meaning | Required Disambiguator |
|----------------|---------|----------------------|
| `signature_only_safe` | Response never changes; pure lookup | None |
| `repeated_identical_response_safe` | Same response even if called multiple times | None |
| `per_signature_fifo_maybe_ok` | FIFO queue per signature acceptable | Per-signature queue cursor |
| `needs_causal_scope_plus_ordinal` | Request ID alone insufficient; need occurrence index | (causal_scope_id, signature, ordinal) |
| `needs_resource_version_or_state` | Resource mutates; need version or state model | (resource_key, version) or stateful replay |
| `needs_stateful_redis_emulation` | Complex state mutations; full Redis emulator | Stateful Redis mock |
| `needs_db_snapshot_or_transaction_order` | SQL writes affect reads; need transaction ordering | DB snapshot or transaction log |
| `unsafe_without_driver_context` | Multiplexed drivers lose causal context | Driver-level command metadata |

---

## High-Risk Ambiguity Patterns

### Pattern 1: Same-Key Read-After-Write (CRITICAL, but state-machine-signature sensitive)

**Description:** Same Redis/DB resource read multiple times with intervening writes can be ambiguous **only if the replay signature is the wire-level dependency signature** — for example Redis command + key/field or SQL text + bind values. If the replay signature also includes the Hyperswitch state-machine discriminator, operation, or expected status transition, then some apparent collisions are separated by that richer signature.

**Important correction for Payment Confirm:** `payments.confirm.v1` should not be treated as automatically ambiguous merely because a payment status changes. In a state-machine-aware model, `Confirm` from `RequiresPaymentMethod` / `RequiresConfirmation` / similar states is a different logical transition, so the higher-level query/replay signature can be different. The ambiguity only applies when replay is keyed by dependency I/O alone, e.g. `HGET payment_intent:{merchant_id}:{payment_id}`, where the returned status is payload/state and not part of the Redis command signature.

**Wire-level ambiguous shape:**
```
HGET payment_intent:{mid}:{pid}           → {status: "RequiresPaymentMethod"}
HSET payment_intent:{mid}:{pid} {...}     → transition via state machine
HGET payment_intent:{mid}:{pid}           → {status: "Processing"}
```

**State-machine-aware shape:**
```
(confirm, RequiresPaymentMethod, HGET payment_intent:{mid}:{pid})
(confirm, Processing, HGET payment_intent:{mid}:{pid})
```

Those two signatures are different if the state-machine context is included, so signature-only ambiguity is reduced.

| route_id | dependency read | read signature | response can vary? | why | missing context | required disambiguator | evidence |
|----------|-----------------|----------------|-------------------|-----|-----------------|------------------------|----------|
| payments.confirm.v1 | Redis HGET | `HGET payment_intent:{merchant_id}:{payment_id}` | CONDITIONAL | Ambiguous for wire-level dependency replay; less ambiguous if signature includes payment operation + state-machine status/transition such as `Confirm` + `RequiresPaymentMethod` | resource version or state-machine transition context, not just request id | `needs_resource_version_or_state`; `needs_causal_scope_plus_ordinal` only if same wire signature repeats within one scope | storage_impl/src/payments/payment_intent.rs:510, storage_impl/src/payments/payment_intent.rs:694 |
| payments.capture.v1 | Redis HGET | `HGET payment_attempt:pa_{attempt_id}` | CONDITIONAL | Attempt status evolves; collision depends on whether operation/status transition is included in replay signature | resource version or transition context | `needs_resource_version_or_state` or `needs_causal_scope_plus_ordinal` for repeated same-signature reads | storage_impl/src/payments/payment_attempt.rs:1583, storage_impl/src/payments/payment_attempt.rs:1741 |
| refunds.create.v1 | Redis HGET | `HGET payment_intent:{mid}:{pid}` | CONDITIONAL | Original payment state gates refund creation; if the dependency signature excludes payment status/version, the same key can represent different lifecycle versions | resource version/status context | `needs_resource_version_or_state` | core/refunds.rs, storage_impl/src/db/refund.rs:498 |

---

### Pattern 2: CVC/Token Read-Delete (CRITICAL)

**Description:** Retrieve-and-delete pattern for sensitive tokens; second identical read returns NotFound.

**Example Flow:**
```
GET pm_token_{pm_id}_hyperswitch_cvc    → encrypted_cvc_bytes
DEL pm_token_{pm_id}_hyperswitch_cvc    → delete for security
GET pm_token_{pm_id}_hyperswitch_cvc    → nil/NotFound
```

| route_id | dependency read | read signature | response can vary? | why | missing context | required disambiguator | evidence |
|----------|-----------------|----------------|-------------------|-----|-----------------|------------------------|----------|
| payments.confirm.v1 | Redis GET | `GET pm_token_{pm_id}_hyperswitch_cvc` | YES | First read returns CVC, second returns NotFound after DEL | key existence state | `needs_stateful_redis_emulation` | core/payment_methods/vault.rs:2235-2320 |
| payments.create.v1 | Redis GET | `GET pm_token_{pm_id}_hyperswitch` | YES | Payment token retrieved then deleted | key existence state | `needs_stateful_redis_emulation` | core/payment_methods/vault.rs:2196-2249 |
| payment_methods.retrieve.v1 | Redis GET | `GET pm_token_{pm_id}_hyperswitch_cvc` | YES | Same read-delete pattern | key existence state | `needs_stateful_redis_emulation` | core/payment_methods/vault.rs:2235-2320 |

**Affected Files:**
- `crates/router/src/core/payment_methods/vault.rs:2196-2320` - Token storage/retrieval
- `crates/router/src/core/payment_methods.rs:5208` - Payment method reads

---

### Pattern 3: Reverse Lookup Indirection (HIGH)

**Description:** Stable lookup key maps to mutable target entity.

**Example Flow:**
```
GET reverse_lookup:{connector_txn_id}     → {merchant_id, payment_id}
HGET payment_intent:{mid}:{pid}           → {status: current_status}
```

| route_id | dependency read | read signature | response can vary? | why | missing context | required disambiguator | evidence |
|----------|-----------------|----------------|-------------------|-----|-----------------|------------------------|----------|
| webhooks.receive.v1 | Redis GET | `GET reverse_lookup:{connector_txn_id}` | NO (stable) | Lookup itself stable, but target mutable | N/A for lookup; target needs version | `needs_resource_version_or_state` for target read | storage_impl/src/lookup.rs:104, storage_impl/src/db/reverse_lookup.rs:128 |
| payments.retrieve_by_reference.v2 | Redis GET | `GET reverse_lookup:{merchant_reference_id}` | NO (stable) | Same pattern | N/A for lookup | `needs_resource_version_or_state` for target | storage_impl/src/lookup.rs:147 |

---

### Pattern 4: Cache Populate Pattern (HIGH)

**Description:** Cache miss triggers DB read, then cache populate; subsequent read hits cache.

**Example Flow:**
```
GET config:{key}                          → nil (cache miss)
SELECT * FROM configs WHERE key = $1      → config_value
SET config:{key} config_value             → populate cache
GET config:{key}                          → config_value (hit)
```

| route_id | dependency read | read signature | response can vary? | why | missing context | required disambiguator | evidence |
|----------|-----------------|----------------|-------------------|-----|-----------------|------------------------|----------|
| any.config.read | Redis GET | `GET config:{key}` | YES | First returns nil, subsequent returns value | cache state | `needs_stateful_redis_emulation` or skip cache in replay | storage_impl/src/redis/cache.rs:306-340 |
| payments.confirm.v1 | Redis HGET | `HGET merchant_account:{merchant_id}` | YES | Account cached on first read | cache state | `needs_stateful_redis_emulation` | storage_impl/src/merchant_account.rs |
| api_keys.list.v1 | Redis HGET | `HGET merchant_api_keys:{merchant_id}` | YES | List cached on first access | cache state | `needs_stateful_redis_emulation` | storage_impl/src/db/api_keys.rs |

---

### Pattern 5: SCAN/List Queries (HIGH)

**Description:** SCAN or list queries return varying result sets based on concurrent modifications.

| route_id | dependency read | read signature | response can vary? | why | missing context | required disambiguator | evidence |
|----------|-----------------|----------------|-------------------|-----|-----------------|------------------------|----------|
| refunds.create.v1 | Redis SCAN | `SCAN refund:{merchant_id}:*` | YES | Result varies based on concurrent refund creations | result set content + timestamp | `needs_resource_version_or_state` or result hash | storage_impl/src/db/refund.rs:810 |
| disputes.list.v1 | Redis SCAN | `SCAN dispute:{merchant_id}:*` | YES | Concurrent disputes change list | result set content + timestamp | `needs_resource_version_or_state` | storage_impl/src/db/dispute.rs |
| payment_attempts.list.v2 | Redis SCAN | `SCAN pa_{merchant_id}_*` | YES | Concurrent payments change list | result set content + timestamp | `needs_resource_version_or_state` | storage_impl/src/payments/payment_attempt.rs:1319 |
| mandates.list.v1 | Redis SCAN | `SCAN mandate:{merchant_id}:*` | YES | Concurrent mandates change list | result set content + timestamp | `needs_resource_version_or_state` | storage_impl/src/db/mandate.rs |

---

### Pattern 6: Cross-Session Mutable Entity Reads (HIGH)

**Description:** Entities accessed across multiple independent sessions with different states.

| route_id | dependency read | read signature | response can vary? | why | missing context | required disambiguator | evidence |
|----------|-----------------|----------------|-------------------|-----|-----------------|------------------------|----------|
| payment_methods.retrieve.v1 | Redis HGET | `HGET payment_method:{pm_id}` | YES | PM updated by different sessions | entity version or modified_at | `needs_resource_version_or_state` | core/payment_methods.rs:5208 |
| customers.retrieve.v1 | Redis HGET | `HGET customer:{merchant_id}:{customer_id}` | YES | Customer updated elsewhere | entity version | `needs_resource_version_or_state` | storage_impl/src/customer.rs |
| mandates.retrieve.v1 | Redis HGET | `HGET mandate:{merchant_id}:{mandate_id}` | YES | Mandate status changes | entity version | `needs_resource_version_or_state` | storage_impl/src/db/mandate.rs:119 |

**Cross-Session Identifiers at Risk:**
- `payment_method_id` / `pmt_id`
- `customer_id`
- `mandate_id`
- `merchant_id` (configuration changes)
- `connector_transaction_id` (external reference)

---

### Pattern 7: Shared Driver Task Context Loss (MEDIUM)

**Description:** fred Redis client's routing task loses per-request correlation.

| route_id | dependency | issue | reason | required disambiguator |
|----------|------------|-------|--------|------------------------|
| ALL.redis.operations | Any Redis command | Cannot attribute socket write to originating request | fred routes through single task spawned at startup | `unsafe_without_driver_context` for exact attribution; markers in payload for validation |

**Mitigation:** Redis validation markers (`ECHO deja_expected_request_id=...`) provide in-band validation even if hook correlation fails.

---

## Complete Ambiguity Matrix by Route

### Payment Routes

| route_id | Redis Reads | DB Reads | Redis Writes | Highest Risk Pattern | Disambiguator Needed |
|----------|-------------|----------|--------------|---------------------|---------------------|
| payments.create.v1 | 2-3 | 0-1 | 3-5 | Token read-delete | `needs_stateful_redis_emulation` |
| payments.create_intent.v2 | 1-2 | 0-1 | 2-3 | Same-key read-after-write | `needs_causal_scope_plus_ordinal` |
| payments.confirm.v1 | 3-5 | 0-2 | 4-6 | CVC read-delete + intent status changes | `needs_stateful_redis_emulation` |
| payments.confirm_intent.v2 | 2-4 | 0-2 | 3-5 | Same-key read-after-write | `needs_causal_scope_plus_ordinal` |
| payments.retrieve.v1 | 1-3 | 0-2 | 0 | Cache populate | `needs_resource_version_or_state` |
| payments.capture.v1 | 2-4 | 0-2 | 2-4 | Attempt status evolution | `needs_causal_scope_plus_ordinal` |
| payments.cancel.v1 | 2-4 | 0-2 | 2-4 | Attempt status evolution | `needs_causal_scope_plus_ordinal` |
| payments.list.v1 | 0-1 SCAN | 1 SELECT | 0 | List pagination | `needs_resource_version_or_state` |

### Payment Method Routes

| route_id | Redis Reads | DB Reads | Redis Writes | Highest Risk Pattern | Disambiguator Needed |
|----------|-------------|----------|--------------|---------------------|---------------------|
| payment_methods.create.v1 | 1-2 | 0-1 | 2-4 | Token storage (retrieve-delete risk) | `needs_stateful_redis_emulation` |
| payment_methods.retrieve.v1 | 1-2 | 0-1 | 0 | Cross-session PM mutation | `needs_resource_version_or_state` |
| payment_methods.delete.v1 | 1 | 0-1 | 1 DEL | Delete operation | `needs_stateful_redis_emulation` |
| payment_methods.list.v1 | 1 SCAN | 1 SELECT | 0 | List result variability | `needs_resource_version_or_state` |

### Refund Routes

| route_id | Redis Reads | DB Reads | Redis Writes | Highest Risk Pattern | Disambiguator Needed |
|----------|-------------|----------|--------------|---------------------|---------------------|
| refunds.create.v1 | 2-3 | 0-1 | 2-3 | Idempotency SCAN + intent read | `needs_resource_version_or_state` |
| refunds.retrieve.v1 | 1 | 0-1 | 0-1 | Status sync update | `needs_causal_scope_plus_ordinal` |
| refunds.list.v1 | 1 SCAN | 1 SELECT | 0 | List variability | `needs_resource_version_or_state` |

### Customer Routes

| route_id | Redis Reads | DB Reads | Redis Writes | Highest Risk Pattern | Disambiguator Needed |
|----------|-------------|----------|--------------|---------------------|---------------------|
| customers.create.v1 | 1 | 0 | 1 | Simple write | `repeated_identical_response_safe` |
| customers.retrieve.v1 | 1 | 0-1 | 0 | Cross-session mutation | `needs_resource_version_or_state` |
| customers.update.v1 | 1 | 0-1 | 1 | Same-key read-write | `needs_causal_scope_plus_ordinal` |
| customers.delete.v1 | 1 | 0-1 | 1 DEL | Delete | `needs_stateful_redis_emulation` |

### Mandate Routes

| route_id | Redis Reads | DB Reads | Redis Writes | Highest Risk Pattern | Disambiguator Needed |
|----------|-------------|----------|--------------|---------------------|---------------------|
| mandates.retrieve.v1 | 1 | 0-1 | 0 | Cross-session status change | `needs_resource_version_or_state` |
| mandates.list.v1 | 1 SCAN | 1 SELECT | 0 | List variability | `needs_resource_version_or_state` |
| mandates.revoke.v1 | 1 | 0-1 | 1 | Status update | `needs_causal_scope_plus_ordinal` |

### Webhook Routes

| route_id | Redis Reads | DB Reads | Redis Writes | Highest Risk Pattern | Disambiguator Needed |
|----------|-------------|----------|--------------|---------------------|---------------------|
| webhooks.receive.v1 | 3-5 | 1-2 | 2-4 | Reverse lookup + status sync | `needs_stateful_redis_emulation` |

### User/Auth Routes

| route_id | Redis Reads | DB Reads | Redis Writes | Highest Risk Pattern | Disambiguator Needed |
|----------|-------------|----------|--------------|---------------------|---------------------|
| user.signin.v1 | 0-1 | 1 | 1 SETEX | Session token | `per_signature_fifo_maybe_ok` |
| user.signout.v1 | 0 | 0 | 1 DEL | Session deletion | `needs_stateful_redis_emulation` |

---

## Recommended Disambiguator Strategy

### Tier 1: Stateful Redis Emulation (Critical)
**For:** Read-delete-read patterns, cache populate
- Implement minimal Redis state machine for replay
- Track key existence, HSET/HGET state per key

### Tier 2: Causal Scope + Ordinal (High Priority)
**For:** Same-key read-after-write within request
- Tag each dependency call with (request_id, sequence_number)
- Lookup uses composite key

### Tier 3: Resource Version (Medium Priority)
**For:** Cross-session entity reads
- Add `modified_at` or `version` to resource lookup
- Include in signature or as separate lookup dimension

### Tier 4: Result Set Hashing (Lower Priority)
**For:** SCAN/list queries
- Hash result set contents
- Match by content similarity, not just query signature

---

*See `raw/agent-ambiguity-classification.md` for additional pattern details and `CORRELATION_ARCHITECTURE.md` for correlation design rationale.*
