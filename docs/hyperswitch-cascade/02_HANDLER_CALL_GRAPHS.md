# Hyperswitch Handler to Core Call Graphs

**Date:** 2026-05-08  
**Source:** `<repo-root>/vendor/hyperswitch-fresh`

---

## Summary

This document traces the call chains from inbound HTTP routes through core handlers to storage/Redis boundaries for the most critical Hyperswitch flows.

---

## 1. Payment Flow Call Graphs

### 1.1 Payment Create (v1)

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | payments_create | HTTP handler | routes/payments.rs:30 | Entry point |
| 2 | payments_create | authorize_verify_select | auth wrapper | routes/payments.rs:89 | Determines flow |
| 3 | authorize_verify_select | payments_operation_core | core orchestrator | core/payments.rs:244 | Main payment logic |
| 4 | payments_operation_core | get_trackers | tracker fetch | core/payments/operations.rs | Retrieves payment trackers |
| 5 | payments_operation_core | insert_payment_intent | storage insert | storage_impl/src/payments/payment_intent.rs:448 | Redis+DB insert |
| 6 | payments_operation_core | insert_payment_attempt | storage insert | storage_impl/src/payments/payment_attempt.rs:1262 | Redis+DB insert |
| 7 | payments_operation_core | call_connector_service | connector dispatch | core/payments.rs:380 | External connector call |

### 1.2 Payment Confirm (v1)

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | payments_confirm | HTTP handler | routes/payments.rs:592 | Entry point |
| 2 | payments_confirm | authorize_verify_select | auth wrapper | routes/payments.rs:650 | Route to core |
| 3 | authorize_verify_select | payments_operation_core | core orchestrator | core/payments.rs:244 | Main payment logic |
| 4 | payments_operation_core | find_payment_intent | storage read | storage_impl/src/payments/payment_intent.rs:510 | Redis HGet fallback |
| 5 | payments_operation_core | find_payment_attempt | storage read | storage_impl/src/payments/payment_attempt.rs:1583 | Redis HGet |
| 6 | payments_operation_core | update_payment_attempt | storage write | storage_impl/src/payments/payment_attempt.rs:1741 | Redis HSet |
| 7 | payments_operation_core | call_connector_service | connector dispatch | core/payments.rs:380 | External connector call |

### 1.3 Payment Retrieve (v1)

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | payments_retrieve | HTTP handler | routes/payments.rs:303 | Entry point |
| 2 | payments_retrieve | payments_core | core orchestrator | core/payments.rs:480 | Simpler retrieve flow |
| 3 | payments_core | find_payment_intent | storage read | storage_impl/src/payments/payment_intent.rs:510 | Redis HGet |
| 4 | payments_core | find_optional_payment_attempt | storage read | storage_impl/src/payments/payment_attempt.rs:1655 | Redis optional HGet |

### 1.4 Payment Create Intent (v2)

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | payments_create_intent | HTTP handler | routes/payments.rs:130 | v2 entry point |
| 2 | payments_create_intent | payments_intent_core | core orchestrator | core/payments.rs:550 | v2 intent flow |
| 3 | payments_intent_core | insert_payment_intent | storage insert | storage_impl/src/payments/payment_intent.rs:448 | Redis+DB insert |
| 4 | payments_intent_core | get_payment_method | PM fetch | core/payments.rs:316 | Payment method lookup |

### 1.5 Payment Confirm Intent (v2)

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | payment_confirm_intent | HTTP handler | routes/payments.rs:819 | v2 confirm |
| 2 | payment_confirm_intent | payments_intent_core | core orchestrator | core/payments.rs:550 | v2 flow |
| 3 | payments_intent_core | find_payment_intent | storage read | storage_impl/src/payments/payment_intent.rs:510 | Redis HGet |
| 4 | payments_intent_core | insert_payment_attempt | storage insert | storage_impl/src/payments/payment_attempt.rs:1262 | Redis+DB |
| 5 | payments_intent_core | update_payment_intent | storage update | storage_impl/src/payments/payment_intent.rs:694 | Redis HSet |

---

## 2. Payment Method Call Graphs

### 2.1 Create Payment Method (v1)

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | create_payment_method | HTTP handler | routes/payment_methods.rs:51 | Entry point |
| 2 | create_payment_method | create_payment_method_core | core handler | core/payment_methods.rs:6737 | Core logic |
| 3 | create_payment_method_core | decide_storage_scheme | storage decision | storage_impl/src/redis/kv_store.rs:200 | Redis vs DB |
| 4 | create_payment_method_core | insert_payment_method | storage insert | storage_impl/src/payment_method.rs:140 | KV store |
| 5 | insert_payment_method | kv_wrapper | KV wrapper | storage_impl/src/redis/kv_store.rs:250 | KvOperation::Hset |

### 2.2 Retrieve Payment Method

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | retrieve_payment_method | HTTP handler | routes/payment_methods.rs:158 | Entry point |
| 2 | retrieve_payment_method | retrieve_payment_method_core | core handler | core/payment_methods.rs:6800 | Core logic |
| 3 | retrieve_payment_method_core | find_payment_method | storage read | storage_impl/src/payment_method.rs:200 | KV lookup |
| 4 | find_payment_method | kv_wrapper | KV wrapper | storage_impl/src/redis/kv_store.rs:250 | KvOperation::HGet |
| 5 | kv_wrapper | get_and_deserialize_key | Redis primitive | redis_interface/src/commands.rs:328 | Direct Redis read |

---

## 3. Refund Call Graphs

### 3.1 Create Refund (v1)

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | refunds_create | HTTP handler | routes/refunds.rs | Entry point |
| 2 | refunds_create | refunds_create_core | core handler | core/refunds.rs | Main logic |
| 3 | refunds_create_core | validate_and_find_payment | validation | core/refunds.rs | Find original payment |
| 4 | validate_and_find_payment | find_payment_intent | storage read | storage_impl/src/payments/payment_intent.rs:510 | Redis lookup |
| 5 | refunds_create_core | insert_refund | storage insert | storage_impl/src/db/refund.rs:498 | KV insert |
| 6 | insert_refund | kv_wrapper | KV wrapper | storage_impl/src/redis/kv_store.rs:250 | KvOperation::Hset |
| 7 | refunds_create_core | trigger_refund_to_gateway | connector call | core/refunds.rs | External gateway |

### 3.2 Sync Refund

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | refund_retrieve | HTTP handler | routes/refunds.rs | Entry point |
| 2 | refund_retrieve | refund_retrieve_core | core handler | core/refunds.rs | Sync logic |
| 3 | refund_retrieve_core | find_refund | storage read | storage_impl/src/db/refund.rs:581 | Redis HGet |
| 4 | refund_retrieve_core | get_refund_status_from_gateway | connector call | core/refunds.rs | Poll gateway |
| 5 | refund_retrieve_core | update_refund | storage update | storage_impl/src/db/refund.rs:1001 | Status update |

---

## 4. Customer Call Graphs

### 4.1 Create Customer (v1)

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | customers_create | HTTP handler | routes/customers.rs | Entry point |
| 2 | customers_create | customers_create_core | core handler | core/customers.rs | Core logic |
| 3 | customers_create_core | insert_customer | storage insert | storage_impl/src/customer.rs | KV insert |
| 4 | insert_customer | kv_wrapper | KV wrapper | storage_impl/src/redis/kv_store.rs:250 | KvOperation::Hset |

### 4.2 Retrieve Customer

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | customers_retrieve | HTTP handler | routes/customers.rs | Entry point |
| 2 | customers_retrieve | customers_retrieve_core | core handler | core/customers.rs | Core logic |
| 3 | customers_retrieve_core | find_customer | storage read | storage_impl/src/customer.rs | KV lookup |
| 4 | find_customer | kv_wrapper | KV wrapper | storage_impl/src/redis/kv_store.rs:250 | KvOperation::HGet |

---

## 5. Mandate Call Graphs

### 5.1 Retrieve Mandate

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | retrieve_mandate | HTTP handler | routes/mandates.rs | Entry point |
| 2 | retrieve_mandate | retrieve_mandate_core | core handler | core/mandates.rs | Core logic |
| 3 | retrieve_mandate_core | find_mandate_by_id | storage read | storage_impl/src/db/mandate.rs:119 | Redis HGet |
| 4 | find_mandate_by_id | kv_wrapper | KV wrapper | storage_impl/src/redis/kv_store.rs:250 | KvOperation::HGet |

---

## 6. User/Auth Call Graphs

### 6.1 User Signin

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | user_signin | HTTP handler | routes/app.rs:2783 | Entry point |
| 2 | user_signin | user_signin_core | core handler | core/user.rs | Auth logic |
| 3 | user_signin_core | find_user_by_email | storage read | storage_impl/src/user.rs | DB query |
| 4 | user_signin_core | verify_password | crypto | external | bcrypt verify |
| 5 | user_signin_core | generate_auth_token | token gen | core/user.rs | JWT creation |

### 6.2 SSO Sign (OIDC)

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | sso_sign | HTTP handler | routes/app.rs:2786 | OIDC entry |
| 2 | sso_sign | sso_sign_core | core handler | core/user.rs | SSO logic |
| 3 | sso_sign_core | fetch_oidc_config | config read | storage/cache | Discovery doc |
| 4 | sso_sign_core | exchange_code_for_token | token exchange | HTTP client | OIDC provider |
| 5 | sso_sign_core | find_or_create_user | upsert | storage_impl | User lookup/create |

---

## 7. Dispute Call Graphs

### 7.1 Accept Dispute

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | accept_dispute | HTTP handler | routes/app.rs:2305 | Entry point |
| 2 | accept_dispute | accept_dispute_core | core handler | core/disputes.rs | Core logic |
| 3 | accept_dispute_core | find_dispute | storage read | storage_impl/src/db/dispute.rs | DB lookup |
| 4 | accept_dispute_core | accept_dispute_with_connector | connector call | core/disputes.rs | Gateway accept |
| 5 | accept_dispute_core | update_dispute | storage update | storage_impl/src/db/dispute.rs | Status update |

---

## 8. Webhook Call Graphs

### 8.1 Incoming Webhook

| Seq | Caller | Callee | Kind | Location | Notes |
|-----|--------|--------|------|----------|-------|
| 1 | route | receive_incoming_webhook | HTTP handler | routes/app.rs:2129 | Entry point |
| 2 | receive_incoming_webhook | webhook_handler | core handler | core/webhooks.rs | Dispatch logic |
| 3 | webhook_handler | verify_webhook_signature | verification | core/webhooks.rs | HMAC verify |
| 4 | webhook_handler | parse_webhook_body | parsing | core/webhooks.rs | Body decode |
| 5 | webhook_handler | update_payment_from_webhook | state update | core/webhooks.rs | DB update |
| 6 | update_payment_from_webhook | find_payment_intent | storage read | storage_impl | Lookup |
| 7 | update_payment_from_webhook | update_payment_attempt | storage write | storage_impl | Status sync |

---

## 9. Core Storage Abstractions

### 9.1 KV Store Wrapper Pattern

```
route handler
    ↓
core handler (business logic)
    ↓
storage_impl function (decide_storage_scheme)
    ├─→ KV path: kv_wrapper() → KvOperation::{HGet,Hset,Scan}
    └─→ DB path: Diesel query
```

**Key Locations:**
- `storage_impl/src/redis/kv_store.rs:200-350` - kv_wrapper, PartitionKey, decide_storage_scheme
- `storage_impl/src/kv_router_store.rs:265-350` - insert_resource, update_resource, find_resource_by_id
- `storage_impl/src/redis/cache.rs:306-340` - get_or_populate_redis (cache pattern)

### 9.2 Redis Primitive Interface

**Direct Redis calls (not through KV wrapper):**
- `redis_interface/src/commands.rs:328` - get_and_deserialize_key
- `redis_interface/src/commands.rs:724` - get_hash_fields
- `redis_interface/src/commands.rs:753` - get_hash_field_and_deserialize

---

## 10. Summary Statistics

| Flow Type | Average Depth | Storage Calls | Redis Operations |
|-----------|--------------|---------------|------------------|
| Payment Create | 5-7 | 2-4 inserts | HSet x2-4 |
| Payment Confirm | 6-8 | 2-4 read+update | HGet, HSet x2-4 |
| Payment Retrieve | 4-5 | 1-2 reads | HGet x1-2 |
| PM Create | 5-6 | 1-2 inserts | HSet x1-2 |
| PM Retrieve | 4-5 | 1 read | HGet x1 |
| Refund Create | 6-8 | 2-3 operations | HGet, HSet |
| Customer Create | 5-6 | 1 insert | HSet x1 |
| Mandate Retrieve | 4-5 | 1 read | HGet x1 |
| User Signin | 4-5 | 1 read + crypto | DB query |
| Webhook Receive | 6-8 | 2-3 operations | HGet, HSet |

---

*See `raw/agent-payments-cascades.md` and `raw/agent-admin-cascades.md` for complete detailed call chains.*
