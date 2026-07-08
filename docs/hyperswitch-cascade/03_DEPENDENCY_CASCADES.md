# Hyperswitch Redis and DB Dependency Cascades

**Date:** 2026-05-08  
**Source:** `<repo-root>/vendor/hyperswitch-fresh`

---

## Overview

This document provides ordered sequences of Redis and database operations triggered by key Hyperswitch API endpoints. Each cascade shows the execution order with file:row:col references.

---

## 1. Payment Create Intent (v2) — `/v2/payments/create-intent`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `payment_intent:{merchant_id}:{payment_id}` | merchant_id, payment_id | storage_impl/src/payments/payment_intent.rs:510 | KV enabled | Needs ordinal if reused |
| 2 | DB | Write | INSERT | `INSERT INTO payment_intent ...` | payment_id, merchant_id | storage_impl/src/payments/payment_intent.rs:448 | Postgres fallback | Transaction-safe |
| 3 | Redis | Write | HSET | `payment_intent:{merchant_id}:{payment_id}` | full intent JSON | storage_impl/src/payments/payment_intent.rs:448 | KV enabled | State-dependent |
| 4 | Redis | Write | SET | `reverse_lookup:{reference_id}` | ref→pk mapping | storage_impl/src/lookup.rs:104 | Reference provided | Indirection risk |
| 5 | Redis | Read | HGET | `payment_method:{pm_id}` | payment_method_id | storage_impl/src/payment_method.rs:200 | PM provided | Version ambiguity |

---

## 2. Payment Confirm (v1) — `/payments/{id}/confirm`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `payment_intent:{merchant_id}:{payment_id}` | merchant_id, payment_id | storage_impl/src/payments/payment_intent.rs:510 | KV enabled | Mutable state |
| 2 | Redis | Read | HGET | `payment_attempt:pa_{attempt_id}` | attempt_id | storage_impl/src/payments/payment_attempt.rs:1583 | KV enabled | Mutable state |
| 3 | DB | Read | SELECT | `SELECT * FROM payment_attempt WHERE attempt_id = $1` | attempt_id | storage_impl/src/payments/payment_attempt.rs:1583 | Redis miss | Read-after-write risk |
| 4 | Redis | Read | HGET | `config:{key}` | config_key | storage_impl/src/redis/cache.rs:306 | Cache lookup | Populate pattern |
| 5 | Redis | Write | HSET | `payment_attempt:pa_{attempt_id}` | updated status | storage_impl/src/payments/payment_attempt.rs:1741 | KV enabled | Same-key write |
| 6 | Redis | Write | HSET | `payment_intent:{merchant_id}:{payment_id}` | updated intent | storage_impl/src/payments/payment_intent.rs:694 | KV enabled | Same-key write |
| 7 | Redis | Write | HSET | `lock:{merchant_id}:{payment_id}` | lock value | storage_impl/src/locks.rs | Lock required | Ephemeral key |
| 8 | Redis | Delete | DEL | `lock:{merchant_id}:{payment_id}` | - | storage_impl/src/locks.rs | After unlock | Read-delete-read |

---

## 3. Payment Retrieve (v1) — `/payments/{id}`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `payment_intent:{merchant_id}:{payment_id}` | merchant_id, payment_id | storage_impl/src/payments/payment_intent.rs:510 | KV enabled | Mutable read |
| 2 | DB | Read | SELECT | `SELECT * FROM payment_intent WHERE payment_id = $1` | payment_id | storage_impl/src/payments/payment_intent.rs:510 | Redis miss | Fallback read |
| 3 | Redis | Read | HGET | `payment_attempt:pa_{attempt_id}` | attempt_id | storage_impl/src/payments/payment_attempt.rs:1655 | KV enabled | Optional fetch |
| 4 | Redis | Read | HGET | `payment_method:{pm_id}` | payment_method_id | storage_impl/src/payment_method.rs:200 | PM attached | Cross-session ID |

---

## 4. Payment Method Create — `/payment_methods`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `payment_method:{pm_id}` | payment_method_id | storage_impl/src/payment_method.rs:200 | Duplicate check | Exists check |
| 2 | DB | Write | INSERT | `INSERT INTO payment_method ...` | pm_id, customer_id | storage_impl/src/payment_method.rs:140 | Postgres | Transaction |
| 3 | Redis | Write | HSET | `payment_method:{pm_id}` | full PM JSON | storage_impl/src/payment_method.rs:140 | KV enabled | State write |
| 4 | Redis | Write | SET | `pm_token_{pm_id}_hyperswitch` | token value | core/payment_methods/vault.rs:2196 | Tokenizable | Cross-session key |
| 5 | Redis | Write | SET | `pm_token_{pm_id}_hyperswitch_cvc` | encrypted CVC | core/payment_methods/vault.rs:2320 | CVC provided | Sensitive token |
| 6 | Redis | Write | HSET | `reverse_lookup:{connector_pm_id}` | ref→pm_id | storage_impl/src/lookup.rs:147 | Connector ID | Indirection |

---

## 5. Payment Method Retrieve — `/payment_methods/{id}`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `payment_method:{pm_id}` | payment_method_id | storage_impl/src/payment_method.rs:200 | KV enabled | Mutable read |
| 2 | DB | Read | SELECT | `SELECT * FROM payment_method WHERE payment_method_id = $1` | pm_id | storage_impl/src/payment_method.rs:200 | Redis miss | Fallback |
| 3 | Redis | Read | GET | `pm_token_{pm_id}_hyperswitch` | token data | core/payment_methods/vault.rs:2249 | Vault fetch | Token lifecycle |

---

## 6. Refund Create — `/refunds`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `payment_intent:{merchant_id}:{payment_id}` | merchant_id, payment_id | storage_impl/src/payments/payment_intent.rs:510 | Validate payment | Mutable read |
| 2 | Redis | Read | SCAN | `refund:{merchant_id}:*` | merchant_id pattern | storage_impl/src/db/refund.rs:810 | Idempotency check | SCAN result varies |
| 3 | DB | Write | INSERT | `INSERT INTO refund ...` | refund_id, payment_id | storage_impl/src/db/refund.rs:498 | Postgres | Transaction |
| 4 | Redis | Write | HSET | `refund:{merchant_id}:{refund_id}` | full refund JSON | storage_impl/src/db/refund.rs:498 | KV enabled | State write |
| 5 | Redis | Write | SET | `reverse_lookup:{connector_refund_id}` | ref→refund_id | storage_impl/src/lookup.rs:147 | Connector ID | Indirection |

---

## 7. Refund Sync — `/refunds/{id}`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `refund:{merchant_id}:{refund_id}` | refund_id | storage_impl/src/db/refund.rs:581 | KV enabled | Mutable read |
| 2 | DB | Read | SELECT | `SELECT * FROM refund WHERE refund_id = $1` | refund_id | storage_impl/src/db/refund.rs:581 | Redis miss | Fallback |
| 3 | Redis | Write | HSET | `refund:{merchant_id}:{refund_id}` | updated status | storage_impl/src/db/refund.rs:1001 | Post-sync | Same-key write |

---

## 8. Customer Create — `/customers`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `customer:{merchant_id}:{customer_id}` | merchant_id, customer_id | storage_impl/src/customer.rs | Duplicate check | Exists check |
| 2 | DB | Write | INSERT | `INSERT INTO customers ...` | customer_id, merchant_id | storage_impl/src/customer.rs | Postgres | Transaction |
| 3 | Redis | Write | HSET | `customer:{merchant_id}:{customer_id}` | encrypted JSON | storage_impl/src/customer.rs | KV enabled | PII encryption |
| 4 | Redis | Write | SET | `reverse_lookup:{customer_reference}` | ref→customer_id | storage_impl/src/lookup.rs:147 | Reference provided | Indirection |

---

## 9. Customer Retrieve — `/customers/{id}`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `customer:{merchant_id}:{customer_id}` | merchant_id, customer_id | storage_impl/src/customer.rs | KV enabled | Mutable read |
| 2 | DB | Read | SELECT | `SELECT * FROM customers WHERE customer_id = $1` | customer_id | storage_impl/src/customer.rs | Redis miss | Fallback |
| 3 | KeyManager | Decrypt | Decrypt | customer PII fields | - | domain_models/src/customer.rs | Encrypted fields | External service |

---

## 10. Mandate Retrieve — `/mandates/{id}`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `mandate:{merchant_id}:{mandate_id}` | merchant_id, mandate_id | storage_impl/src/db/mandate.rs:119 | KV enabled | Mutable read |
| 2 | DB | Read | SELECT | `SELECT * FROM mandate WHERE mandate_id = $1` | mandate_id | storage_impl/src/db/mandate.rs:119 | Redis miss | Fallback |
| 3 | Redis | Read | HGET | `payment_method:{pm_id}` | linked PM | core/mandates.rs | Mandate active | Cross-reference |

---

## 11. User Signin — `/user/signin`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | DB | Read | SELECT | `SELECT * FROM users WHERE email = $1` | email | storage_impl/src/user.rs | - | Credential check |
| 2 | - | Verify | bcrypt | password hash verification | - | core/user.rs | Password auth | Crypto operation |
| 3 | Redis | Write | SETEX | `session:{token}` | user_id, expiry | core/user.rs | Success | Session token |
| 4 | DB | Write | UPDATE | `UPDATE users SET last_login = NOW() ...` | user_id | storage_impl/src/user.rs | Success | Audit trail |

---

## 12. Webhook Receive — `/webhooks/{merchant_id}/{connector}`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `merchant_account:{merchant_id}` | merchant_id | core/webhooks.rs | Initial fetch | Mutable config |
| 2 | Redis | Read | HGET | `connector_account:{merchant_id}:{connector}` | connector config | core/webhooks.rs | Connector lookup | Config version |
| 3 | Redis | Read | HGET | `payment_intent:{merchant_id}:{payment_id}` | from webhook | core/webhooks.rs | Payment reference | Mutable state |
| 4 | DB | Write | UPDATE | `UPDATE payment_attempt SET status = ...` | attempt_id | core/webhooks.rs | Status change | Same-record update |
| 5 | Redis | Write | HSET | `payment_attempt:pa_{attempt_id}` | updated status | core/webhooks.rs | KV enabled | Same-key write |
| 6 | Redis | Write | PUBLISH | `webhook:processed:{merchant_id}` | event data | core/webhooks.rs | Async notify | Pub/sub (record only) |

---

## 13. API Key Create — `/api_keys/{merchant_id}`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | DB | Write | INSERT | `INSERT INTO api_keys ...` | key_id, merchant_id | storage_impl/src/db/api_keys.rs | - | Transaction |
| 2 | Redis | Write | SETEX | `api_key:{hashed_key}` | key metadata | storage_impl/src/db/api_keys.rs | Caching enabled | Time-based |
| 3 | Redis | Write | HSET | `merchant_api_keys:{merchant_id}` | key list | storage_impl/src/db/api_keys.rs | List cache | Set membership |

---

## 14. API Key Retrieve — `/api_keys/{merchant_id}/list`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | HGET | `merchant_api_keys:{merchant_id}` | merchant_id | storage_impl/src/db/api_keys.rs | Cache enabled | Cache populate |
| 2 | DB | Read | SELECT | `SELECT * FROM api_keys WHERE merchant_id = $1` | merchant_id | storage_impl/src/db/api_keys.rs | Cache miss | List query |
| 3 | Redis | Write | HSET | `merchant_api_keys:{merchant_id}` | serialized list | storage_impl/src/db/api_keys.rs | Cache update | Populate |

---

## 15. Dispute List — `/disputes/list`

| Seq | Dependency | R/W | Operation | Resource Key/Query | Key Components | Callsite | Branch Condition | Replay Risk |
|-----|------------|-----|-----------|-------------------|----------------|----------|------------------|-------------|
| 1 | Redis | Read | SCAN | `dispute:{merchant_id}:*` | merchant_id pattern | storage_impl/src/db/dispute.rs | Paginated | Result set varies |
| 2 | DB | Read | SELECT | `SELECT * FROM dispute WHERE merchant_id = $1 LIMIT ...` | merchant_id | storage_impl/src/db/dispute.rs | Filter/paginate | Time-dependent |
| 3 | Redis | Read (mget) | HGET | Multiple `dispute:{id}` keys | dispute_ids | storage_impl/src/db/dispute.rs | Batch fetch | Bulk read |

---

## Key Observations

### High Replay Risk Patterns

| Pattern | Example | Risk Level | Mitigation |
|---------|---------|------------|------------|
| Same-key read-after-write | Payment confirm: HGET→HSET→HGET same intent | HIGH | State versioning or causal scope+ordinal |
| Read-delete-read | Token retrieval then delete | HIGH | Stateful replay with key existence |
| Cache populate | Miss→DB→SET→next read hits | MEDIUM | Causal scope or ignore cache in replay |
| Cross-session key access | payment_method_id lookup | HIGH | Entity version or scope isolation |
| SCAN queries | List operations | HIGH | Result set hash comparison |
| Reverse lookup indirection | connector_ref→entity lookup | MEDIUM | Combined key or target versioning |
| Ephemeral locks | Lock→Unlock DEL | LOW | Skip or state-track locks |

### Storage Scheme Decision Points

```rust
// From storage_impl/src/redis/kv_store.rs:200
decide_storage_scheme(&self, op: &StorageOp) -> StorageScheme {
    match op {
        StorageOp::PaymentIntent => StorageScheme::RedisKv,
        StorageOp::PaymentAttempt => StorageScheme::RedisKv,
        StorageOp::Refund => StorageScheme::RedisKv,
        StorageOp::Customer => StorageScheme::RedisKv,
        StorageOp::Mandate => StorageScheme::RedisKv,
        StorageOp::ApiKey => StorageScheme::PostgresOnly, // Or conditional cache
        _ => StorageScheme::PostgresOnly,
    }
}
```

---

*See `raw/agent-dependency-index.md` for complete enumerated callsites and `raw/agent-ambiguity-classification.md` for replay risk classifications.*
