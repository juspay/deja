# DB/Redis Replay Ambiguity Patterns — Same-Request vs Cross-Session

**Generated:** 2026-05-14  
**Source:** `vendor/hyperswitch-fresh`  
**Inputs used:** existing full route matrix in `03_DEPENDENCY_CASCADES_FULL_RUST_BRAIN.md`, saved rust-brain artifacts under `docs/hyperswitch-cascade/raw/rust-brain-*.jsonl`, and local source spot checks.

---

## 0. What this artifact answers

There are two different ambiguity problems that should not be collapsed:

1. **Request-local temporal ambiguity**: within one route invocation / one request / one causal scope, a route writes Redis or DB state and later reads state whose result depends on that earlier write. If the same normalized read signature can happen before and after the write, or multiple times after different writes, a recorder cannot replay by `(operation, key/query)` alone. It needs a **local ordinal** or a **stateful Redis/DB model**.

2. **Cross-session lifetime ambiguity**: a route reads a long-lived resource by a stable Redis key or SQL predicate. The same key/query can be read in different requests/sessions and legitimately return different values because the resource lives across sessions and can be updated, deleted, redacted, expire, or have new collection members.

Important correction from validating `POST /v2/payments`: the current happy path is **write intent -> read same intent -> write/update intent/attempt**, not clearly **multiple reads after a write**. It is still request-local read-after-write, but the broader multiple-read-after-write problem must be confirmed route-by-route with trace ordinals.

---

## 1. Vocabulary for replay keys

| Term | Meaning | Example |
|---|---|---|
| `dependency_signature` | Normalized operation shape, often too coarse by itself. | `HGET payment_intent`, `SELECT payment_intent WHERE id = ?` |
| `resource_key` | Concrete Redis key/hash field or SQL where-parameter tuple. | `PartitionKey::GlobalPaymentId(id=pay_x) + field pi_pay_x`; SQL `payment_intent.id = pay_x` |
| `causal_scope_id` | One route invocation / request / session scope. | API request id, route span id, or synthetic replay scope id |
| `local_ordinal` | Sequence number of dependency calls inside one causal scope. | 1st `HGET pi_x`, 2nd `HGET pi_x` |
| `resource_version` | Version of long-lived state. Native `modified_at` where available, otherwise synthetic counter/content hash. | `payment_attempt.modified_at`, response hash |
| `collection_version` | Version/hash of a list/scan result. | count + sorted member IDs + content hash |

---

## 2. Route-class coverage map

This groups all 509 route rows from `03_DEPENDENCY_CASCADES_FULL_RUST_BRAIN.md` by the two ambiguity patterns.

| Cascade class | Route rows | Pattern A: request-local temporal? | Pattern B: cross-session lifetime? | Notes |
|---|---:|---|---|---|
| `ADMIN_CONFIG_MUTATE` | 83 | Medium | **Yes** | Merchant/profile/connector/routing config updates; cache redaction and routing/CGraph versions matter. |
| `ACCOUNTS_MUTATE` | 51 | Medium/High | **Yes** | User/role/org/theme mutations often read/update/list in one request. |
| `ADMIN_CONFIG_READ` | 45 | Low | **Yes** | Config/account/profile reads are long-lived and cacheable. |
| `ACCOUNTS_READ` | 38 | Low | **Yes** | Users/roles/themes/orgs persist across sessions. |
| `PAYMENTS_MUTATING_LIFECYCLE` | 28 | **Yes, high** | **Yes** | Payment intent/attempt status is read, inserted, updated; token/CVC state can be destructive. |
| `ACCOUNTS_AUTH_SESSION` | 27 | Medium | **Yes** | Sessions, blacklist/cache keys, 2FA tokens, auth method updates. |
| `PM_MUTATE_TOKEN_VAULT` | 20 | **Yes, high** | **Yes** | Payment method/session/token routes create/update/delete Redis and DB state. |
| `PAYMENTS_LIST_ANALYTICS` | 18 | Low | **Yes, collection** | DB/OLAP list/filter/aggregate result sets change over time. |
| `WEBHOOK_STATE_SYNC` | 18 | Medium | **Yes** | Event/process states, locks, and poll keys are mutable across deliveries. |
| `GENERIC_DB_OR_CONFIG` | 16 | Medium | **Yes** | Subscriptions, forex, Apple Pay migration, poll/SDK/config routes need concrete resource versions. |
| `PM_READ_CROSS_SESSION` | 14 | Low | **Yes, high** | Stable `payment_method_id` / token lookup returns different versions over time. |
| `PAYMENTS_CREATE_OR_TOKEN` | 12 | **Medium** | **Yes** | Creates generated resources and optional token/PM reads; created IDs become cross-session resources. |
| `PAYOUT_MUTATE` | 11 | Medium | **Yes** | Payout/payout-attempt mutation mirrors payment-attempt patterns. |
| `AUTHENTICATION_STATEFUL` | 10 | **Yes, high** | **Yes** | Authentication rows, eligibility Redis handoffs, and tokenized auth values evolve through 3DS lifecycle. |
| `DISPUTE_READ_LIST` | 9 | Low | **Yes, collection** | Dispute list/filter/aggregate result sets change over time. |
| `PAYMENTS_READ` | 9 | Low | **Yes, high** | Reads long-lived payment intent/attempt/payment-method state. |
| `PAYOUT_LIST` | 9 | Low | **Yes, collection** | Payout list/filter result sets change with payout lifecycle. |
| `REFUND_LIST_SCAN` | 8 | Low | **Yes, collection** | Refund scan/list responses change after create/update/sync. |
| `PAYOUT_READ` | 7 | Low | **Yes** | Long-lived payout rows and alternate connector lookups. |
| `RECOVERY_PROCESS_TRACKER` | 7 | Medium | **Yes** | Process-tracker/recovery Redis state persists across workflow steps. |
| `APIKEY_MUTATE_CACHE` | 6 | Medium | **Yes** | API key DB rows plus hash-key cache invalidation. |
| `CUSTOMER_MUTATE` | 6 | Medium | **Yes** | Customer update/delete/redaction changes future reads/lists. |
| `CUSTOMER_READ` | 5 | Low | **Yes** | Customer records are long-lived and lookup by alternate ids. |
| `PM_LIST_SCAN` | 5 | Medium | **Yes, collection** | Customer/session PM lists and scans are collection-version sensitive. |
| `REFUND_MUTATE` | 5 | Medium | **Yes** | Refund create/update reads payment/refund state, inserts/updates refunds. |
| `APIKEY_READ_CACHE` | 4 | Low | **Yes** | API key auth/read paths depend on cached hashed key and revoked/expired DB state. |
| `CUSTOMER_LIST` | 4 | Low | **Yes, collection** | Customer list/count result sets change over time. |
| `DISPUTE_MUTATE` | 4 | Medium | **Yes** | Dispute accept/evidence updates mutate long-lived dispute/evidence state. |
| `EPHEMERAL_KEY_REDIS_HASH` | 4 | **Yes, destructive/TTL** | **Yes, TTL** | Client-secret/ephemeral-key hash reads and deletes are stateful. |
| `NO_DEP_HEALTH` | 4 | Low | Low | Health checks are mostly safe except intentionally written test Redis keys. |
| `PM_DELETE_TOKEN_STATE` | 4 | **Yes, high** | **Yes** | Delete routes cause explicit token/resource state transitions. |
| `REFUND_READ_SYNC` | 4 | Low/Medium | **Yes** | Refund status changes after sync/update. |
| `CARD_MUTATE` | 3 | Low/Medium | **Yes** | Card metadata/BIN data has long-lived DB behavior. |
| `FILE_DB_STORAGE` | 3 | Medium | **Yes** | DB metadata + object storage side effects. |
| `CHAT_OLAP` | 2 | Medium | **Yes, collection** | AI chat inserts and conversation lists depend on external workflow plus async DB insert. |
| `CACHE_INVALIDATE` | 1 | **Yes, destructive** | **Yes** | Explicit Redis/cache redaction by user-supplied key. |
| `CARD_BIN_READ` | 1 | Low | **Yes** | BIN/card reference data can change after mutation/migration routes. |
| `MANDATE_LIST` | 1 | Low | **Yes, collection** | Mandate list result sets change after setup/revoke/payment. |
| `MANDATE_MUTATE` | 1 | Medium | **Yes** | Mandate status/reference changes after revoke/update. |
| `MANDATE_READ` | 1 | Low | **Yes** | Mandates are long-lived; connector mandate lookup adds indirection. |
| `OLAP_ANALYTICS_READ` | 1 | Low | Config snapshot | `/feature_matrix` is static/config in checked source, not DB/Redis-backed. |

---

## 3. Pattern A — request-local temporal ambiguity

### A.1 `POST /v2/payments`: write intent, then read same intent in one request

**Route:** `POST /v2/payments`  
**Handler:** `router::routes::payments::payments_create_and_confirm_intent`  
**Classification:** `PAYMENTS_MUTATING_LIFECYCLE`

#### Source path

| Step | Operation | Source |
|---:|---|---|
| 1 | Route registration maps `/v2/payments` to handler. | `vendor/hyperswitch-fresh/crates/router/src/routes/app.rs:777-790` |
| 2 | Route handler delegates to core create+confirm. | `vendor/hyperswitch-fresh/crates/router/src/routes/payments.rs:431-483` |
| 3 | Core calls `payments_intent_core` with `PaymentIntentCreate`. | `vendor/hyperswitch-fresh/crates/router/src/core/payments.rs:3379-3427` |
| 4 | `PaymentIntentCreate::get_trackers` builds and inserts a payment intent. | `vendor/hyperswitch-fresh/crates/router/src/core/payments/operations/payment_create_intent.rs:90-191` |
| 5 | Core immediately calls `decide_authorize_or_setup_intent_flow`, then `payments_core` with `PaymentIntentConfirm`. | `vendor/hyperswitch-fresh/crates/router/src/core/payments.rs:3429-3481` |
| 6 | `PaymentIntentConfirm::get_trackers` reads that same payment intent by global id. | `vendor/hyperswitch-fresh/crates/router/src/core/payments/operations/payment_confirm_intent.rs:152-181` |

#### Redis/DB keys and queries

| Backend | Write | Later read | Source |
|---|---|---|---|
| Redis-KV | `HSetNx` field `pi_{global_payment_id}` under `PartitionKey::GlobalPaymentId { id }` | `HGet` field `pi_{global_payment_id}` under same partition | `storage_impl/src/payments/payment_intent.rs:142-215`, `473-533` |
| Postgres | `INSERT payment_intent` | `SELECT payment_intent WHERE id = $global_payment_id` | `storage_impl/src/payments/payment_intent.rs:724-755`, `858-890` |

#### Ambiguity

This is a confirmed same-request **write -> read** on the same logical payment intent. It is not currently a confirmed `write -> read -> read` sequence on the happy path. However, replay cannot key the read only as `find_payment_intent_by_id` or `HGET pi_*`; it must know this read occurs **after** the create write in the same causal scope.

**Required recording fields:**

```yaml
scope_id: request/span id for POST /v2/payments
ordinal: dependency sequence inside request
backend: redis_kv | postgres
operation: HSETNX | INSERT | HGET | SELECT
resource_key:
  payment_intent_id: <global_payment_id>
  redis_partition: GlobalPaymentId(<global_payment_id>)
  redis_field: pi_<global_payment_id>
result_version: inserted | processing | post_connector_final | content_hash
```

---

### A.2 `DELETE /user/user/delete`: delete role(s), then list roles in same request

**Route:** `DELETE /user/user/delete` as listed in `raw/agent-routes.md` and the full route matrix.  
**Handler:** `router::routes::user_role::delete_user_role`  
**Classification in matrix:** `ACCOUNTS_MUTATE`

#### Source path

| Step | Operation | Source |
|---:|---|---|
| 1 | Route registration for `/delete`. | `vendor/hyperswitch-fresh/crates/router/src/routes/app.rs:2988-3007` |
| 2 | Route handler invokes `user_role_core::delete_user_role`. | `vendor/hyperswitch-fresh/crates/router/src/routes/user_role.rs:299-318` |
| 3 | Core reads V2 user role by lineage. | `vendor/hyperswitch-fresh/crates/router/src/core/user_role.rs:564-588` |
| 4 | Core deletes V2 user role. | `vendor/hyperswitch-fresh/crates/router/src/core/user_role.rs:620-637` |
| 5 | Core reads V1 user role by same user/org/merchant/profile lineage, different version. | `vendor/hyperswitch-fresh/crates/router/src/core/user_role.rs:639-663` |
| 6 | Core deletes V1 user role. | `vendor/hyperswitch-fresh/crates/router/src/core/user_role.rs:697-714` |
| 7 | Core then lists all remaining roles for the user. | `vendor/hyperswitch-fresh/crates/router/src/core/user_role.rs:746-764` |
| 8 | If list is empty, core deletes metadata and deactivates user, then writes user blacklist key. | `vendor/hyperswitch-fresh/crates/router/src/core/user_role.rs:768-782` |

#### SQL queries

| Query/write | Source |
|---|---|
| `find_user_role_by_user_id_and_lineage(...)` -> SQL `find_by_user_id_tenant_id_org_id_merchant_id_profile_id(...)` | `vendor/hyperswitch-fresh/crates/router/src/db/user_role.rs:122-146` |
| `delete_user_role_by_user_id_and_lineage(...)` -> SQL `delete_by_user_id_tenant_id_org_id_merchant_id_profile_id(...)` | `vendor/hyperswitch-fresh/crates/router/src/db/user_role.rs:171-194` |
| `list_user_roles_by_user_id(...)` -> SQL `generic_user_roles_list_for_user(...)` | `vendor/hyperswitch-fresh/crates/router/src/db/user_role.rs:197-220` |

#### Ambiguity

The final list query is a collection read whose result depends on deletes earlier in the same request.

```text
R: find role V2(user_id, tenant, org, merchant, profile)
W: delete role V2(user_id, tenant, org, merchant, profile)
R: find role V1(user_id, tenant, org, merchant, profile)
W: delete role V1(user_id, tenant, org, merchant, profile)
R: list roles by user_id   <-- must observe post-delete collection state
```

If a recorder keys list results only by `list_user_roles_by_user_id(user_id)`, replay can confuse pre-delete and post-delete collection versions.

**Required disambiguator:** `(causal_scope_id, local_ordinal)` plus `collection_version` for the returned set.

---

### A.3 Dynamic routing enable: config write followed by read in same request

**Route family:** dynamic routing update/enable routes, including route handlers in `router/src/routes/routing.rs`.  
**Classification:** `ADMIN_CONFIG_MUTATE`

#### Source path

| Step | Operation | Source |
|---:|---|---|
| 1 | Route handlers for routing update/config operations. | `vendor/hyperswitch-fresh/crates/router/src/routes/routing.rs:1273-1437` |
| 2 | Helper reads existing routing algorithm if already enabled. | `vendor/hyperswitch-fresh/crates/router/src/core/routing/helpers.rs:2068-2084` |
| 3 | Helper updates business profile active dynamic algorithm ref. | `vendor/hyperswitch-fresh/crates/router/src/core/routing/helpers.rs:2088-2095` |
| 4 | Helper reads routing algorithm again to generate response. | `vendor/hyperswitch-fresh/crates/router/src/core/routing/helpers.rs:2097-2102` |

#### Ambiguity

This is not same-key Redis ambiguity; it is a cross-resource causal dependency:

```text
W: update business_profile.dynamic_routing_algorithm
R: read routing_algorithm(profile_id, algorithm_id) for response
```

The read result may be stable, but its validity in the route depends on the prior business-profile update. Record/replay should preserve the route-local ordinal and not reorder the config write after the response read.

---

### A.4 Client secret / ephemeral-key delete: read then destructive deletes

**Routes:** `/ephemeral_keys`, `/v2/client-secret` and delete helpers.  
**Classification:** `EPHEMERAL_KEY_REDIS_HASH`

#### Source path

| Operation | Source |
|---|---|
| v1 create stores both `epkey_{secret}` and `epkey_{id}` as Redis hash field `ephkey`, then sets expiry. | `vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:77-131` |
| v1 get reads `get_hash_field_and_deserialize(key, "ephkey")`. | `vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:134-146` |
| v1 delete reads ephemeral key, then deletes both id and secret keys. | `vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:148-170` |
| v2 client-secret create stores both `cs_{secret}` and `cs_{id}` as Redis hash field `csh`, then sets expiry. | `vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:181-237` |
| v2 get reads `get_hash_field_and_deserialize(key, "csh")`. | `vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:240-250` |
| v2 delete reads client secret, then deletes both `cs_{id}` and `cs_{secret}` keys. | `vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:253-281` |
| `ClientSecretId::generate_redis_key` builds `cs_{id}`. | `vendor/hyperswitch-fresh/crates/common_utils/src/id_type/client_secret.rs:29-32` |
| `ClientSecretType::generate_secret_key` builds `cs_{secret}`. | `vendor/hyperswitch-fresh/crates/diesel_models/src/ephemeral_key.rs:21-25` |

#### Ambiguity

This is destructive Redis state. A second read of the same key after delete must return `NotFound`. Even if the current route does not explicitly read twice, replay should model key existence.

**Required disambiguator:** stateful Redis key-existence machine plus TTL.

---

### A.5 Payment CVC token: read then delete inside confirm-like payment paths

**Route family:** payment confirm / payment method token flows.  
**Classification:** `PM_MUTATE_TOKEN_VAULT` and `PAYMENTS_MUTATING_LIFECYCLE`

#### Source path

| Operation | Source |
|---|---|
| CVC write with TTL: `pm_token_{payment_method_id}_hyperswitch_cvc`. | `vendor/hyperswitch-fresh/crates/router/src/core/payment_methods/vault.rs:2190-2228` |
| Confirm path may call `retrieve_and_delete_cvc_from_payment_token`. | `vendor/hyperswitch-fresh/crates/router/src/core/payments/operations/payment_confirm_intent.rs:551-636` |
| CVC read uses `get_and_deserialize_key`, then deletes same key. | `vendor/hyperswitch-fresh/crates/router/src/core/payment_methods/vault.rs:2235-2268` |
| General PM token key builder: `pm_token_{parent_pm_token}_hyperswitch`. | `vendor/hyperswitch-fresh/crates/router/src/routes/payment_methods.rs:1373-1390` |
| General token delete helper deletes `pm_token_*_hyperswitch`. | `vendor/hyperswitch-fresh/crates/router/src/core/payment_methods/utils.rs:925-947` |

#### Ambiguity

This is not a same-route write->multiple-read in the payment confirm route. The write often occurred in an earlier route/session. But confirm performs a destructive read/delete, so two reads with the same key across nearby scopes can have different valid outcomes:

```text
Session A: SET pm_token_pm_123_hyperswitch_cvc = encrypted_cvc EX ttl
Session B/payment-confirm: GET pm_token_pm_123_hyperswitch_cvc -> encrypted_cvc
Session B/payment-confirm: DEL pm_token_pm_123_hyperswitch_cvc
Session C or later retry: GET pm_token_pm_123_hyperswitch_cvc -> NotFound
```

**Required disambiguator:** stateful Redis model with `Exists -> Deleted/Expired`, plus operation ordinal.

---

### A.6 Generic cache-aside: read miss, write, later read hit

**Source:** `vendor/hyperswitch-fresh/crates/storage_impl/src/redis/cache.rs:306-340`

`get_or_populate_redis` first reads Redis; on `NotFound` it computes from DB and writes the cache key. If any route calls this helper twice for the same key in one causal scope, the same `GET key` signature can produce a miss before the write and a hit after the write.

```text
R1: GET cache_key -> NotFound
W1: SET cache_key computed_value
R2: GET cache_key -> computed_value
```

**Static note:** the helper is generic; the exact per-route `R1/W1/R2` multiplicity should be confirmed with dependency tracing. The replay engine should still support this state machine.

---

## 4. Pattern B — cross-session lifetime ambiguity

These are resources whose keys or SQL predicates are stable and meaningful across requests. The same read signature can return different valid values over time.

### B.1 Payment intent by id

| Field | Details |
|---|---|
| Route examples | `GET /v2/payments/{payment_id}`, `POST /v2/payments/{payment_id}/confirm-intent`, `POST /v2/payments/{payment_id}/capture`, `POST /v2/payments/{payment_id}/cancel`, redirection finish/status routes. |
| Redis key | `PartitionKey::GlobalPaymentId { id }`, field `pi_{global_payment_id}`. |
| DB query | `DieselPaymentIntent::find_by_global_id(&conn, id)`. |
| Read source | `vendor/hyperswitch-fresh/crates/storage_impl/src/payments/payment_intent.rs:473-533` |
| Write/update source | `vendor/hyperswitch-fresh/crates/storage_impl/src/payments/payment_intent.rs:330-401` |
| Why ambiguous | Payment intent status/active attempt/metadata changes across confirm, capture, cancel, refund, webhook, sync, redirect completion. |
| Replay key | `(resource_key = payment_intent.id, version_index or modified_at/content_hash, scope_id + ordinal fallback)` |

### B.2 Payment attempt by id/reverse lookup/scan

| Field | Details |
|---|---|
| Route examples | Payment confirm/capture/cancel/sync, refunds, webhooks, list attempts. |
| Redis keys | `PartitionKey::GlobalPaymentId { id: payment_id }`, field `{cluster_label}_{attempt_id}` in v2; v1 uses merchant/payment partition and `pa_*` fields. Reverse lookups point to `pk_id/sk_id`. |
| DB queries | `find_by_id`, `find_by_payment_id`, `find_by_processor_merchant_id_attempt_id`, connector transaction lookups. |
| Read/scan source | `vendor/hyperswitch-fresh/crates/storage_impl/src/payments/payment_attempt.rs:1207-1939` |
| Write/update source | `vendor/hyperswitch-fresh/crates/storage_impl/src/payments/payment_attempt.rs:912-1204` |
| Why ambiguous | Same attempt id can move from `Pending` to authorized/captured/voided/failed. Scans like `pa_*` are collection-version sensitive. |
| Replay key | `(attempt_id or payment_id + field_pattern, result version/collection hash, local ordinal)` |

### B.3 Payment method by id, locker id, customer list

| Field | Details |
|---|---|
| Route examples | `GET /payment_methods/{payment_method_id}`, `DELETE /payment_methods/{payment_method_id}`, saved PM list routes, payment confirm tokenized PM branches. |
| Redis/DB indirection | `find_resource_by_id(... FindResourceBy::LookupId("payment_method_{id}"))`; locker lookup `payment_method_locker_{locker_id}`. |
| Source | `vendor/hyperswitch-fresh/crates/storage_impl/src/payment_method.rs:50-95` |
| Why ambiguous | PM status, metadata, locker id, tokenization/network token data, last-used timestamp, deletion state can change across sessions. |
| Replay key | `(payment_method_id or locker_id, version/modified_at/content_hash)`; for lists use collection hash. |

### B.4 Customer by global/customer id and customer lists

| Field | Details |
|---|---|
| Route examples | `GET /customers/{id}`, `POST /customers/{id}`, `DELETE /customers/{id}`, customer PM list routes. |
| Redis key | v2 customer uses `PartitionKey::GlobalId { id }`, field `cust_{global_customer_id}`. |
| Source | `vendor/hyperswitch-fresh/crates/storage_impl/src/customers.rs:400-496` |
| Why ambiguous | Customer can be updated/redacted/deleted; PM lists under the customer change over time. |
| Replay key | `(customer_id, version/status/content_hash)`, and list collection hash for customer lists. |

### B.5 Refund by id/connector transaction and refund scans

| Field | Details |
|---|---|
| Route examples | `POST /refunds`, `GET /refunds/{id}`, `POST /refunds/{id}`, refund list/filter/aggregate routes. |
| Redis keys | v1 refunds use payment partition and fields like `pa_{attempt_id}_ref_{refund_id}`; reverse lookup for connector refund ids. |
| Read/scan source | `vendor/hyperswitch-fresh/crates/router/src/db/refund.rs:773-840`, `948-1088`, `1106-1158` |
| Insert/update source | `vendor/hyperswitch-fresh/crates/router/src/db/refund.rs:700-740`, `875-929` |
| Why ambiguous | Refund status changes from `Pending` to success/failure; list/scan result sets change when new refunds are inserted. |
| Replay key | `(refund_id or connector_refund_id or scan pattern, version/collection hash, ordinal)` |

### B.6 Payout and payout attempt

| Field | Details |
|---|---|
| Route examples | `/payouts/create`, `/payouts/{payout_id}/confirm`, payout retrieve/list routes. |
| Redis/DB source | `vendor/hyperswitch-fresh/crates/storage_impl/src/payouts/payouts.rs:57-400`, `vendor/hyperswitch-fresh/crates/storage_impl/src/payouts/payout_attempt.rs:37-403` |
| Why ambiguous | Payout and payout-attempt status progresses across create/confirm/fulfillment/sync. |
| Replay key | `(payout_id, payout_attempt_id, connector_payout_id, version)` |

### B.7 Mandate reads by merchant mandate id / connector mandate id

| Field | Details |
|---|---|
| Route examples | Mandate retrieve/list/revoke routes. |
| Redis key | `PartitionKey::MerchantIdMandateId`, field `mandate_{mandate_id}`; connector mandate uses reverse lookup to target `pk_id/sk_id`. |
| Source | `vendor/hyperswitch-fresh/crates/router/src/db/mandate.rs:88-185` |
| Why ambiguous | Mandate status/reference changes after payment setup/revoke. Connector mandate id reverse lookup adds indirection. |
| Replay key | `(merchant_id, mandate_id or connector_mandate_id, target version)` |

### B.8 Admin/account/profile/connector/routing config

| Field | Details |
|---|---|
| Route examples | Merchant account/profile/connector/routing create/update/delete/read routes. |
| DB examples | Merchant connector account `find_by_*` functions in `vendor/hyperswitch-fresh/crates/storage_impl/src/merchant_connector_account.rs:232-582`; business profile reads in `vendor/hyperswitch-fresh/crates/storage_impl/src/business_profile.rs:145-232`; routing algorithm reads in `vendor/hyperswitch-fresh/crates/router/src/db/routing_algorithm.rs:83-165`. |
| Why ambiguous | These are long-lived configuration resources. A read by `merchant_id`, `profile_id`, `connector_name`, or `algorithm_id` can return different config across sessions. |
| Replay key | concrete config id + version/modified_at/content hash; cache invalidation ordinal where caches are used. |

### B.9 API key by hash/key id and accounts cache

| Field | Details |
|---|---|
| Route examples | API key create/list/retrieve/update/delete routes. |
| DB read source | `vendor/hyperswitch-fresh/crates/router/src/db/api_keys.rs:159-208` |
| Cache behavior | `find_api_key_by_hash_optional` may use accounts cache (`cache::get_or_populate_in_memory`) when feature enabled. |
| Why ambiguous | API keys can be revoked/expired/updated; same hash or key id can resolve to different validity state across sessions. |
| Replay key | `(merchant_id, key_id or hashed_api_key, version/status, cache_generation)` |

### B.10 Payment method session Redis key

| Field | Details |
|---|---|
| Route examples | `POST /{prefix}/payment-method-sessions`, `GET/PUT /{prefix}/payment-method-sessions/{payment_method_session_id}`, confirm/list/update/delete saved PM under session. |
| Redis key | `payment_method_session:{global_payment_method_session_id}`. |
| Key builder | `vendor/hyperswitch-fresh/crates/common_utils/src/id_type/global_id/payment_methods.rs:63-66` |
| Insert/read/update source | `vendor/hyperswitch-fresh/crates/router/src/db/payment_method_session.rs:58-151` |
| Route/core source | `vendor/hyperswitch-fresh/crates/router/src/routes/payment_methods.rs:1613-1939`; `vendor/hyperswitch-fresh/crates/router/src/core/payment_methods.rs:5800-6389` |
| Why ambiguous | Session is TTL-bound and updated with associated PMs; same session key can return initial, updated, confirmed, expired/not-found states across sessions. |
| Replay key | `(payment_method_session_id, key_exists, ttl_bucket, version/content_hash)` |

### B.11 Client secret / ephemeral key Redis hashes

| Field | Details |
|---|---|
| Route examples | `/ephemeral_keys`, `/v2/client-secret`, payment/client auth flows. |
| Redis keys | v1 `epkey_{id}` and `epkey_{secret}` with hash field `ephkey`; v2 `cs_{id}` and `cs_{secret}` with hash field `csh`. |
| Source | `vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:77-281` |
| Why ambiguous | TTL + explicit delete + same secret/id can be read in later auth flows. |
| Replay key | `(redis_key, hash_field, key_exists, ttl, ordinal)` |

### B.12 Redis reverse lookup indirection

| Field | Details |
|---|---|
| Route examples | Payment attempt by connector transaction, refund by connector refund id, mandate by connector mandate id, payment intent by merchant reference. |
| Source | `vendor/hyperswitch-fresh/crates/storage_impl/src/lookup.rs:22-65`; references in payment/refund/mandate storage files. |
| Why ambiguous | Reverse lookup key may be stable, but its target entity mutates. A replay lookup must resolve both lookup value and target resource version. |
| Replay key | `(lookup_id, pk_id, sk_id, target_resource_version)` |

---

## 5. Examples where the key/query is ambiguous if under-specified

| Under-specified read signature | Concrete key/query that must be captured | Ambiguity if omitted |
|---|---|---|
| `HGET payment_intent` | `PartitionKey::GlobalPaymentId(id) + field pi_{id}` | Different payments and versions collapse. |
| `SELECT payment_intent WHERE id=?` | exact `global_payment_id` + returned row version/hash | Same payment across create/confirm/capture/cancel sessions can differ. |
| `HGET payment_attempt` | payment partition + attempt field or reverse lookup `pk_id/sk_id` | Attempt status changes; reverse lookup target changes independently. |
| `SCAN pa_*` | partition key + field pattern + sorted returned member IDs/hash | Added attempts/refunds change result set. |
| `find_payment_method` | PM id or locker id + lookup id + row version | PM may be updated/deleted/tokenized. |
| `get payment_method_session` | `payment_method_session:{id}` + TTL/existence/version | Initial, updated, confirmed, expired states share the same key. |
| `get client secret` | `cs_{id}` or `cs_{secret}` + hash field `csh` + TTL/existence | Explicit deletes and TTL expiry alter result. |
| `find_user_role_by_lineage` | `(user_id, tenant_id, org_id, merchant_id, profile_id, version)` | V1/V2 and pre/post delete/update states collapse. |
| `list_user_roles_by_user_id` | full filter tuple + result-set hash | Result changes after role deletes in same request. |
| `find_api_key_by_hash` | hashed key + merchant/key id + status/version/cache generation | Revoked/expired/updated API keys collapse. |

---

## 6. Recommended record/replay handling

### For Pattern A: request-local temporal ambiguity

Record every dependency event with:

```yaml
scope_id: <route invocation id>
route: <method + normalized path>
ordinal: <monotonic per-scope dependency index>
backend: redis | postgres | cache | object_store
operation: HGET | HSET | HSETNX | GET | SET | DEL | SELECT | INSERT | UPDATE | DELETE | SCAN
resource_key: <concrete redis key/hash field or sql table+where params>
read_or_write_set:
  reads: [...]
  writes: [...]
result:
  success_or_error: ...
  content_hash: ...
  row_modified_at: ...
```

Replay rule:

1. Apply writes in ordinal order to an in-memory state model when backend semantics matter.
2. Serve later reads from state if possible.
3. If using recorded responses directly, match by `(scope_id, ordinal, resource_key, operation)`, not just `(operation, resource_key)`.

### For Pattern B: cross-session lifetime ambiguity

Record:

```yaml
resource_key: <stable id/key/query tuple>
resource_version:
  native_modified_at: <if available>
  synthetic_version_index: <monotonic per resource in trace>
  content_hash: <fallback>
scope_id: <request/session>
ordinal: <fallback>
collection_hash: <for lists/scans>
ttl_or_existence: <for Redis TTL/destructive keys>
```

Replay rule:

- For long-lived single rows: match by `(resource_key, resource_version)` or synthesize versions from observed writes.
- For list/scan queries: match by `(query_tuple, collection_hash)` or replay from stateful collection membership.
- For TTL/destructive Redis keys: maintain key state (`Exists`, `Deleted`, `Expired`) and return `NotFound` when state says absent.

---

## 7. What still needs dynamic confirmation

Static source and rust-brain evidence identify route families and concrete keys/queries. To prove exact **multiple reads after a write** occurrences, instrument dependency traces with:

```text
(scope_id, route, ordinal, backend, operation, concrete_key_or_query, result_hash)
```

Then query for:

```sql
-- conceptual
same scope_id
same concrete_key_or_query
exists write at ordinal W
exists read at ordinal R1 > W
exists read at ordinal R2 > R1
```

This will separate:

- true `W -> R -> R` same-key cases,
- `R -> W -> R` same-key cases,
- `W -> R` only cases like current `POST /v2/payments`, and
- cross-resource causal cases like routing config updates.

---

## 8. Expanded concrete examples catalog from continuation pass

This section is the fuller static catalog requested after the first artifact. It combines:

- the 509-row route matrix in `03_DEPENDENCY_CASCADES_FULL_RUST_BRAIN.md`,
- saved rust-brain operation candidates in `raw/rust-brain-*.jsonl`, and
- direct source checks in `vendor/hyperswitch-fresh`.

**Scope note:** “all examples” below means all concrete ambiguity examples found by this static pass. Exact same-key `W -> R -> R` proof still requires the ordinal trace query in section 7. Table A lists directly visible route-local/stateful sequences. Table B lists concrete mutable key/query signatures used by route families and must be versioned/ordered when replaying route traces.

### 8.1 Confirmed request-local or stateful temporal examples

| # | Route / route family | Concrete sequence | Key/query | Source evidence | Replay ambiguity |
|---:|---|---|---|---|---|
| A1 | `POST /v2/payments` | `HSETNX/INSERT payment_intent -> HGET/SELECT same payment_intent -> HSETNX payment_attempt -> HSET updates` | Redis `PartitionKey::GlobalPaymentId(id)` + `pi_{id}`; SQL `payment_intent.id = id` | route/core `routes/payments.rs:431-483`, `core/payments.rs:3379-3481`; storage `payment_intent.rs:142-199`, `473-510`; `payment_attempt.rs:912-992` | Confirmed route-local `W -> R`. Needs scope ordinal and resource version. |
| A2 | `DELETE /user/user/delete` | `find role V2 -> delete role V2 -> find role V1 -> delete role V1 -> list remaining roles` | SQL lineage tuple `(user_id, tenant_id, org_id, merchant_id, profile_id)` plus list by `user_id` | route matrix row 447; core `core/user_role.rs:564-782`; DB `db/user_role.rs:122-220` | Confirmed collection read after deletes. Needs collection hash/ordinal. |
| A3 | `POST /mandates/revoke/{id}` | `HGET mandate -> connector revoke -> HSET mandate(status=Revoked)` | Redis `PartitionKey::MerchantIdMandateId` + `mandate_{mandate_id}`; SQL mandate row | route matrix row 283; core `core/mandate.rs:69-134`; DB `db/mandate.rs:84-119`, `215-288` | Same route observes pre-update mandate then writes new version. Replay must preserve pre/post version. |
| A4 | `POST /refunds` / `POST /v2/refunds` | `read payment_intent -> scan/read payment_attempt -> insert refund -> connector response -> update refund` | PI `pi_*`, PA `pa_*`, refund `pa_{attempt_id}_ref_{refund_id}` | route rows 103,108; core `core/refunds.rs:69-125`, `475-592`, `1426-1456`; DB `db/refund.rs:596-727`, `843-906` | Route-local cascade across payment and refund rows; refund resource has multiple versions. |
| A5 | `POST /refunds/{id}` / manual update / sync | `find refund -> update refund`; sync may also read PI/attempt and update refund | Refund reverse lookups `ref_ref_id_*`, `ref_connector_*`, `ref_inter_ref_*`; field `pa_*_ref_*` | route rows 102,104,106,110,111; core `core/refunds.rs:1240-1267`, `1626-1691`, `2102-2138`; DB `db/refund.rs:938-1083` | Same refund key can represent pre-sync, syncing, succeeded/failed. |
| A6 | Payment confirm/tokenized PM branch | `GET CVC token -> DEL CVC token` | `pm_token_{payment_method_id}_hyperswitch_cvc` | `core/payment_methods/vault.rs:2182-2267`; confirm branch `core/payments/operations/payment_confirm_intent.rs:551-636` | Confirmed destructive read/delete. Needs key-existence state. |
| A7 | Payment parent token routes | `SETEX parent token -> GET token data -> DEL token` | `pm_token_{parent_pm_token}_hyperswitch`; sometimes `pm_token_{parent_pm_token}_{payment_method}_hyperswitch` | `routes/payment_methods.rs:1377-1463`; delete helper `core/payment_methods/utils.rs:865-934` | Token exists, then not-found after delete/TTL. |
| A8 | Tokenized vault temporary locker | `SETNX+TTL locker payload -> GET locker payload -> DEL locker payload` | `{LOCKER_REDIS_PREFIX}_{lookup_key}` via `get_redis_locker_key` | `core/payment_methods/vault.rs:1614-1794` | Same key returns payload, then invalid/expired. |
| A9 | Ephemeral key v1 delete | `HGET ephkey -> DEL epkey_{id} -> DEL epkey_{secret}` | `epkey_{id}` and `epkey_{secret}`, hash field `ephkey` | `db/ephemeral_key.rs:84-170` | Dual-key destructive Redis state; replay must remember both keys. |
| A10 | Client secret v2 delete | `HGET csh -> DEL cs_{id} -> DEL cs_{secret}` | `cs_{id}` and `cs_{secret}`, hash field `csh` | `db/ephemeral_key.rs:181-274`; key builder `common_utils/src/id_type/client_secret.rs:29-32` | TTL + explicit delete means identical read key can return value or not-found. |
| A11 | Payment method session update | `SETEX create -> GET retrieve -> SET without modifying TTL update` | `payment_method_session:{global_payment_method_session_id}` | routes `routes/payment_methods.rs:1613-1938`; DB `db/payment_method_session.rs:58-145`; key builder `common_utils/src/id_type/global_id/payment_methods.rs:63-66` | Same session key has initial, updated, confirmed/listed, expired states. |
| A12 | Single-use payment-method token | `SETEX single-use token -> GET same token` | `SingleUseTokenKey::get_store_key(key)` | `core/payment_methods.rs:6688-6739` | TTL-bound single-use cache; exact key needs version/existence. |
| A13 | API key update/revoke | `read API key by key_id to obtain hashed key -> update/revoke DB row under cache redaction by hash` | SQL `(merchant_id,key_id)`; cache key is `hashed_api_key`, not `key_id` | route rows 301-302,306-307; `db/api_keys.rs:67-151`, `171-192` | Cache invalidation key differs from route key; recorder must capture both. |
| A14 | Merchant account update/delete | `update/delete merchant row -> redact account cache keys` | `CacheKind::Accounts(merchant_id)` and optional `CacheKind::Accounts(publishable_key)` | route class `ADMIN_CONFIG_MUTATE`; `storage_impl/src/merchant_account.rs:225-273`, `379-407`, `808-858` | Multi-key cache invalidation; stale read possible by alternate key. |
| A15 | Merchant connector account update/delete | `update/delete MCA -> redact multiple account/CGraph/PM-filter cache keys` | `{merchant_id}_{connector_label}`, `{profile_id}_{connector_name}`, CGraph keys, PM filter keys | `storage_impl/src/merchant_connector_account.rs:224-456`, `655-775`, `834-993` | Same MCA can be read via several cache keys; partial redaction causes split-brain replay. |
| A16 | Business profile / routing config activation | `update profile active algorithm refs -> redact routing/CGraph caches -> sometimes read algorithm for response` | `routing_config_{merchant_id}_{profile_id}`, `routing_config_po_{merchant_id}_{profile_id}`, CGraph keys | `core/routing/helpers.rs:235-285`, `294-307`, `1936-1951`, `2739-2787`; `core/admin.rs:4413-4458` | Cross-resource route-local dependency; response and future routing depend on update order. |
| A17 | Dynamic routing config caches | `read/populate dynamic config -> update algorithm/profile -> redact type-specific cache` | `SuccessBasedDynamicRoutingCache`, `EliminationBasedDynamicRoutingCache`, `ContractBasedDynamicRoutingCache` | `core/routing/helpers.rs:602-717`, `1816-1936`; `core/routing.rs:1899-2006`, `2333-2336` | Polymorphic cache keys need concrete type and profile key. |
| A18 | Theme update | `DB update theme/config -> DEL Redis theme version` | `{theme_id}_version` | route class `ACCOUNTS_MUTATE`; `utils/user/theme.rs:15-61`; update path references in `core/user/theme.rs` via theme utilities | Same theme version key can be stale unless delete is replayed. |
| A19 | Role cache / role blacklist | `role/user auth mutation -> SET blacklist or DEL role-info cache -> later authorization GET` | `ubl_{user_id}`, `rbl_{role_id}`, `etbl_{token}`, role-info cache prefix | `services/authentication/blacklist.rs:26-121`; `services/authorization.rs:60-115` | Redis-only TTL auth state; replay needs existence/TTL, not just DB user row. |
| A20 | 2FA / recovery-code attempts | `SETEX attempt/secret marker -> EXISTS/GET -> DEL marker` | TOTP/recovery prefixes from `consts::user::*` + `user_id` | `utils/user/two_factor_auth.rs:35-224` | Ephemeral same-key auth state changes within and across login sessions. |
| A21 | OIDC auth code | `SETEX auth code -> GET auth code -> DEL auth code` | OIDC auth-code prefix + code | `utils/oidc.rs:16-98` | Authorization code is single-use/TTL-bound; replay must model delete/expiry. |
| A22 | Outgoing webhook idempotency | `SETNX webhook lock -> DB find event by idempotency id -> DB insert event -> GET/DEL lock` | Redis `WEBHOOK_LOCK_{merchant_id}_{idempotent_event_id}`; SQL `events.idempotent_event_id` | `core/webhooks/outgoing.rs:138-187`; lock helper `core/webhooks/utils.rs:227-303`; DB `db/events.rs:175-185` | Redis lock and DB event dedupe are one causal unit. |
| A23 | Incoming webhook disabled-event config | `GET Redis config -> DB config fallback` | `whconf_disabled_events_{merchant_id}_{connector_id}` | `common_utils/src/id_type/merchant.rs:175-177`; `core/webhooks/utils.rs:35-70` | Cache-aside config read; stale Redis and DB fallback differ. |
| A24 | Revenue recovery connector-customer lock | `SETNX+TTL customer lock -> DB/payment mutations -> DEL/unlock` | `customer:{connector_customer_id}:status` | `types/storage/revenue_recovery_redis_operation.rs:135-243` | TTL lock can expire before route/workflow completes; replay needs lock state/time. |
| A25 | Revenue recovery processor tokens | `GET token hash -> SET/update token hash` | `customer:{connector_customer_id}:tokens` | `types/storage/revenue_recovery_redis_operation.rs:139`, `286-395`, and call sites `core/webhooks/recovery_incoming.rs:943-979` | Redis token hash evolves across recovery workflows and backfill routes. |
| A26 | File upload | `INSERT file_metadata -> object-store upload -> UPDATE file_metadata` | SQL `file_metadata.file_id`; object-store object key | routes rows 325-327; `core/files.rs:14-85` | DB row has at least created vs available/uploaded versions; object-store side effect must be ordered. |
| A27 | Dispute accept/evidence | `find dispute -> read payment/attempt -> connector call -> update dispute/evidence` | SQL `dispute_id`; payment attempt/intent reads | route rows 312,315-317; `core/disputes.rs:294-392`, `416-560`, `572-636` | DB-only but still versioned route-local mutation. |
| A28 | Cards info mutate/migrate | `read optional BIN/card info -> insert/update card_info` | SQL `cards_info.card_iin` | route rows 321-323; `core/cards_info.rs:56-75`, `184-206`, `312+` | DB reference data changes future card/BIN reads. |
| A29 | Redis cache-aside helper | `GET cache -> on miss DB/read callback -> SET cache`; later reads are hits | Any `CacheKind::*` key passed through helper | `storage_impl/src/redis/cache.rs:306-369` | Generic `miss -> populate -> hit` pattern; must record scope/key generation. |
| A30 | Redis health check | `SETEX test_key -> GET test_key -> DEL test_key` | `test_key` | `core/health_check.rs:57-85` | Low business risk but strict local `W -> R -> DEL` sanity example. |

### 8.2 Cross-session lifetime examples by concrete storage signature

| # | Resource / route family | Stable key or query | Source evidence | Why it is ambiguous across sessions |
|---:|---|---|---|---|
| B1 | Payment intent v1 by payment id | `PartitionKey::MerchantIdPaymentId { merchant_id, payment_id }` + field `pi_{payment_id}`; SQL `find_by_payment_id_processor_merchant_id` | `payment_intent.rs:409-448`; insert/update `68-118`, `243-305` | Same payment is created, confirmed, captured, cancelled, synced. |
| B2 | Payment intent v2 by global id | `PartitionKey::GlobalPaymentId(id)` + `pi_{id}`; SQL `find_by_global_id` | `payment_intent.rs:142-199`, `330-384`, `473-510` | Same global payment id progresses through lifecycle routes. |
| B3 | Payment intent by merchant reference id | reverse lookup `pi_merchant_reference_{profile_id}_{merchant_reference_id}` -> target `pi_{id}` | `payment_intent.rs:182-193`, `634-694` | Lookup result may be stable while target PI version changes. |
| B4 | Payment attempt v1 direct field | payment partition + `pa_{attempt_id}` | `payment_attempt.rs:735-883`, `1013-1143`, `1616-1655` | Attempt status and connector ids change across confirm/capture/sync/webhook. |
| B5 | Payment attempt v2 direct field | global payment partition + `{cluster_label}_{attempt_global_id}` | `payment_attempt.rs:912-992`, `1158-1173`, `1774-1805` | Multiple attempts can live under one payment; active/successful attempt changes. |
| B6 | Payment attempt by connector transaction | reverse lookups `pa_conn_trans_{merchant}_{txn}` or profile connector label -> `pa_*` | `payment_attempt.rs:1207-1262`, `1505-1583`, helpers `2112-2160` | Connector transaction id maps to an attempt whose status changes. |
| B7 | Payment attempt by preprocessing id | reverse lookup `pa_preprocessing_{merchant}_{preprocessing_id}` -> `pa_*` | `payment_attempt.rs:1815-1867`, helper `2139-2160` | Preprocessing lookup remains but attempt data mutates. |
| B8 | Payment attempt successful-attempt scans | Redis `SCAN pa_*` under payment partition | `payment_attempt.rs:1282-1464`, `1900-1936` | Result set and selected latest successful attempt change with new attempts/updates. |
| B9 | Refund v1 by refund id | reverse lookup `ref_ref_id_{merchant}_{refund_id}` -> `pa_{attempt_id}_ref_{refund_id}` | `db/refund.rs:596-727`, `938-1001` | Refund status changes after gateway sync/webhook/manual update. |
| B10 | Refund by connector refund id | reverse lookup `ref_connector_{merchant}_{connector_refund_id}_{connector}` -> refund field | `db/refund.rs:706-727`, `1016-1083` | Connector refund id maps to mutable refund row. |
| B11 | Refund by internal reference id | reverse lookup `ref_inter_ref_{merchant}_{internal_reference_id}` -> refund field | `db/refund.rs:517-581`, `678-704` | Internal references survive across retries/status updates. |
| B12 | Refund by connector transaction / payment | scan pattern generated from attempt field or `pa_*_ref_*` | `db/refund.rs:765-810`, `1098-1131` | New refunds under a payment/attempt change scan result. |
| B13 | Payout | `PartitionKey::MerchantIdPayoutId` + field `po_{payout_id}` | `payouts.rs:60-122`, `146-196`, `210-294` | Payout status moves through create/confirm/cancel/fulfill/sync. |
| B14 | Payout attempt | `PartitionKey::MerchantIdPayoutAttemptId` + `poa_{payout_attempt_id}` plus reverse lookup by connector payout id | `payout_attempt.rs:40-129`, `153-241`, `255-359`, helper `779-800` | Attempt state and connector payout id mappings change over workflow. |
| B15 | Payment method by id | lookup `payment_method_{payment_method_id}` -> field `payment_method_id_{id}` | `payment_method.rs:47-76`, `164-195`, `206-224` | PM status, locker metadata, tokenization fields and deletion state change. |
| B16 | Payment method by locker id | lookup `payment_method_locker_{locker_id}` -> PM field | `payment_method.rs:85-98`, `181-185` | Locker id is alternate long-lived handle to mutable PM. |
| B17 | Payment method lists/counts | partition `MerchantIdCustomerId` and SQL filters by customer/global_customer/status/type | `payment_method.rs:275-415`, `536-754` | Saved PM set changes as PMs are created/deleted/defaulted. |
| B18 | Customer v1 by merchant/customer id | partition `MerchantIdCustomerId` + `cust_{customer_id}` | `customers.rs:57-111`, `156-175`, `234-253` | Customer profile can be updated/redacted/deleted. |
| B19 | Customer by merchant reference | partition `MerchantIdMerchantReferenceId` + `cust_{merchant_reference_id}` | `customers.rs:121-140`, `199-218`, insert reverse lookup `318-333` | Merchant reference is a stable alternate key to a mutable customer. |
| B20 | Customer v2 global id | partition `GlobalId(id)` + `cust_{global_customer_id}` | `customers.rs:293-333`, `398-480` | Global customer record changes across customer routes. |
| B21 | Customer lists | SQL list by merchant id / constraints and count | `customers.rs:268-280`, `664-685`, `858-890` | Insert/update/delete changes list and count result sets. |
| B22 | Mandate by mandate id | `PartitionKey::MerchantIdMandateId` + `mandate_{mandate_id}` | `db/mandate.rs:84-119`, `215-288`, `317-376` | Mandate status/reference changes after setup/revoke/payment. |
| B23 | Mandate by connector mandate id | reverse lookup `mid_{merchant}_conn_mandate_{connector_mandate_id}` -> mandate field | `db/mandate.rs:133-177`, insert/update lookup `356-370` | Connector id is stable but target mandate changes. |
| B24 | Mandate/customer lists | SQL `find_mandate_by_merchant_id_customer_id`, `find_mandates_by_merchant_id` | `db/mandate.rs:191-204`, `305-317`, `core/mandate.rs:223-406` | Mandate memberships/statuses change over time. |
| B25 | API key by hash | cache key `hashed_api_key`; SQL `find_optional_by_hashed_api_key` | `db/api_keys.rs:171-192`; mutation redaction `91-151` | Auth reads by hash can see active/revoked/expired versions. |
| B26 | API key by merchant/key id/list | SQL `(merchant_id,key_id)` and list by merchant | `db/api_keys.rs:159-201`, route rows 299-307 | Admin views by key id/list differ after update/revoke. |
| B27 | Merchant account by merchant id | accounts cache key `merchant_id`; SQL merchant account row | `merchant_account.rs:177-207`, invalidation `225-273`, `808-837` | Merchant config and auth settings mutate across admin routes. |
| B28 | Merchant account by publishable key | accounts cache key `publishable_key`; SQL `find_by_publishable_key` | `merchant_account.rs:287-309`, invalidation `812-858` | Alternate lookup can return stale/new merchant account version. |
| B29 | Merchant connector account by label/name/id | accounts cache `{merchant_id}_{connector_label}`, `{profile_id}_{connector_name}`, id | `merchant_connector_account.rs:224-456`, redaction `655-775` | Same connector account has multiple lookup paths and versions. |
| B30 | Business profile | SQL `business_profile.profile_id` / `(merchant_id,profile_id)` / profile name | `business_profile.rs:137-188`, `241-333`, admin calls `core/admin.rs:3937`, `4378-4389` | Profile routing/config/session fields are updated by admin/routing routes. |
| B31 | Routing/CGraph caches | `routing_config_{merchant}_{profile}`, `routing_config_po_{merchant}_{profile}`, CGraph keys | `core/routing/helpers.rs:276-285`, `2739-2787`; `core/admin.rs:4413-4458` | Routing decisions can change with same cache key after config activation/update. |
| B32 | Dynamic routing typed caches | success/elimination/contract dynamic routing cache kinds | `core/routing/helpers.rs:602-717`, `1816-1936` | Same profile/key can map to different dynamic routing algorithm data. |
| B33 | Config cache and generic cache-aside | `CacheKind::Config`, `Accounts`, `Routing`, `CGraph`, etc. | `storage_impl/src/redis/cache.rs:121-148`, `306-457` | Same cache key may be miss, stale hit, fresh hit, or redacted. |
| B34 | Payment method session | Redis key `payment_method_session:{id}` | `db/payment_method_session.rs:58-145`; key builder `global_id/payment_methods.rs:63-66` | TTL-bound session is updated/read by multiple session routes. |
| B35 | Ephemeral key | `epkey_{id}` / `epkey_{secret}` hash field `ephkey` | `db/ephemeral_key.rs:84-170` | Key can exist, expire, or be explicitly deleted. |
| B36 | Client secret | `cs_{id}` / `cs_{secret}` hash field `csh` | `db/ephemeral_key.rs:181-274`; `client_secret.rs:29-32` | Client-secret auth state is TTL/destructive. |
| B37 | Parent PM token | `pm_token_{parent_pm_token}_hyperswitch` and typed variant with payment method | `routes/payment_methods.rs:1377-1463`; `core/payment_methods/utils.rs:865-934` | Payment/tokenization flows create, consume, and delete these keys. |
| B38 | CVC token | `pm_token_{payment_method_id}_hyperswitch_cvc` | `core/payment_methods/vault.rs:2182-2293` | TTL and delete change read result for same key. |
| B39 | Temporary locker tokenized data | `{LOCKER_REDIS_PREFIX}_{lookup_key}` | `core/payment_methods/vault.rs:1614-1794` | Tokenized payload can be present, expired, or deleted. |
| B40 | Single-use PM token | `SingleUseTokenKey::get_store_key(key)` | `core/payment_methods.rs:6688-6739` | Same key can be present before TTL and absent after use/expiry. |
| B41 | Role info cache | role-info cache key from `get_cache_key_from_role_id(role_id)` | `services/authorization.rs:60-115`; invalidation `services/authentication/blacklist.rs:63-66` | Permission reads depend on cached role version and TTL. |
| B42 | User/role/email blacklist | `ubl_{user_id}`, `rbl_{role_id}`, `etbl_{token}` | `services/authentication/blacklist.rs:26-121` | Redis-only security state expires and can be missing. |
| B43 | 2FA/OIDC transient auth | TOTP/recovery attempt keys and OIDC auth code key | `utils/user/two_factor_auth.rs:35-224`; `utils/oidc.rs:16-98` | Same session token/code is one-time or TTL-bound. |
| B44 | Webhook events | SQL `events.event_id`, `events.idempotent_event_id`, initial event list predicates | `db/events.rs:130-185`, `200-447`; outgoing flow `core/webhooks/outgoing.rs:138-187` | Event rows move through initial, retry, delivered/failed states. |
| B45 | Webhook lock | `WEBHOOK_LOCK_{merchant_id}_{idempotent_event_id}` | `core/webhooks/utils.rs:227-303` | Lock key exists/does not exist by TTL and owner value. |
| B46 | Webhook disabled-event config | `whconf_disabled_events_{merchant_id}_{connector_id}` and DB config fallback | `common_utils/src/id_type/merchant.rs:175-177`; `core/webhooks/utils.rs:35-70` | Same config key can be stale in Redis or newer in DB. |
| B47 | Revenue recovery connector-customer status | `customer:{connector_customer_id}:status` | `revenue_recovery_redis_operation.rs:135-243` | Redis lock/status state affects concurrent recovery workflows. |
| B48 | Revenue recovery processor tokens | `customer:{connector_customer_id}:tokens` hash | `revenue_recovery_redis_operation.rs:139`, `286-395`, higher-level flows `582-887` | Token set changes after webhook/backfill/process-tracker routes. |
| B49 | Process tracker rows | SQL `process_tracker.id`, `schedule_time/status` query windows | refund scheduler calls `core/refunds.rs:1885`, `2234-2275`; route rows 491-508 | Task state and query windows change as retries are scheduled/processed. |
| B50 | Disputes | SQL `dispute_id`, lists/filters/aggregates | `core/disputes.rs:52-222`, `294-392`, `416-636`; route rows 308-320 | Dispute status/evidence and list results change across sessions. |
| B51 | File metadata | SQL `file_metadata.file_id`; object-store object side effect | `core/files.rs:14-134`; route rows 325-327 | Metadata can be created, updated after upload, deleted, or retrieved. |
| B52 | Cards/BIN info | SQL `cards_info.card_iin` | `core/cards_info.rs:32-75`, `184-206`, route rows 321-324 | Reference data changes after card-info update/migration. |
| B53 | Blocklist | SQL `blocklist_lookup(merchant_id,fingerprint)` and blocklist entries/list | `core/blocklist.rs:13-62`; query index noted in `raw/agent-dependency-index.md` | Guard decisions and list results change after add/remove/toggle. |
| B54 | Analytics/list/aggregate routes | ClickHouse/OpenSearch/DB aggregate/list queries by time/profile/status | route classes `PAYMENTS_LIST_ANALYTICS`, `REFUND_LIST_SCAN`, `PAYOUT_LIST`, `DISPUTE_READ_LIST` | Same query tuple returns different sets/counts as source tables change. `OLAP_ANALYTICS_READ` is handled separately in C12 because `/feature_matrix` is config/static in checked source. |

### 8.3 Route-class to example-family mapping

Use this as the “which route rows are covered” index; the full 509-row list remains in `03_DEPENDENCY_CASCADES_FULL_RUST_BRAIN.md`.

| Route class | Covered by examples |
|---|---|
| `PAYMENTS_MUTATING_LIFECYCLE` | A1, A6; B1-B8, B37-B40 |
| `PAYMENTS_CREATE_OR_TOKEN` | A1, A7, A8, A12; B1-B8, B37-B40 |
| `PAYMENTS_READ` | B1-B8, B15-B17, B37-B40 |
| `PM_MUTATE_TOKEN_VAULT` | A6-A8, A11-A12; B15-B17, B34, B37-B40 |
| `PM_READ_CROSS_SESSION` | B15-B17, B34, B37-B40 |
| `PM_DELETE_TOKEN_STATE` | A7-A8; B15-B17, B34, B37-B40 |
| `PM_LIST_SCAN` | B17, B21, B34 |
| `REFUND_MUTATE` | A4-A5; B9-B12, B49 |
| `REFUND_READ_SYNC` | A5; B9-B12 |
| `REFUND_LIST_SCAN` | B12, B54 |
| `CUSTOMER_MUTATE` | B18-B21 plus payment-method list effects B17 |
| `CUSTOMER_READ` / `CUSTOMER_LIST` | B18-B21, B23-B24, B54 |
| `MANDATE_READ` / `MANDATE_MUTATE` / `MANDATE_LIST` | A3; B22-B24 |
| `PAYOUT_MUTATE` / `PAYOUT_READ` / `PAYOUT_LIST` | B13-B14, B31, B54 |
| `ADMIN_CONFIG_MUTATE` / `ADMIN_CONFIG_READ` | A14-A17; B27-B33 |
| `APIKEY_MUTATE_CACHE` / `APIKEY_READ_CACHE` | A13; B25-B26 |
| `ACCOUNTS_MUTATE` / `ACCOUNTS_READ` | A2, A18-A21; B41-B43 |
| `ACCOUNTS_AUTH_SESSION` / `AUTHENTICATION_STATEFUL` | A19-A21; B41-B43; C7-C8 |
| `EPHEMERAL_KEY_REDIS_HASH` | A9-A10; B35-B36 |
| `WEBHOOK_STATE_SYNC` | A22-A23, C6; B44-B46 |
| `RECOVERY_PROCESS_TRACKER` | A24-A25; B47-B49 |
| `DISPUTE_MUTATE` / `DISPUTE_READ_LIST` | A27; B50, B54 |
| `FILE_DB_STORAGE` | A26; B51 |
| `CARD_MUTATE` / `CARD_BIN_READ` | A28; B52 |
| `CACHE_INVALIDATE` / `GENERIC_DB_OR_CONFIG` | A29; B33; C3-C5, C10-C11, C13 |
| `CHAT_OLAP` | C1-C2 |
| `OLAP_ANALYTICS_READ` | C12 caveat |
| `NO_DEP_HEALTH` | A30 only; intentionally low-risk test key. |

### 8.4 What this continuation added beyond sections 3-4

Newly cataloged examples not fully enumerated earlier:

- payment attempt reverse lookups and scans (`pa_conn_trans_*`, preprocessing, `pa_*`),
- refund reverse lookups and scan patterns (`ref_ref_id_*`, `ref_connector_*`, `pa_*_ref_*`),
- payout/payout-attempt Redis KV signatures,
- merchant account / connector account multi-key account cache invalidation,
- dynamic routing typed cache families,
- role info cache and user/role/email blacklists,
- OIDC and 2FA transient auth keys,
- webhook locks/events/config cache,
- revenue recovery connector-customer lock/token hashes,
- file metadata, dispute, cards/BIN, blocklist, process-tracker, and analytics DB-only lifetime examples.

---

## 9. Second-pass gap closure for broad or missing classes

The first expanded catalog covered every major route class but left some broad matrix classes without enough concrete route-level detail. This second pass closes those gaps and also marks one over-broad matrix classification.

| # | Route / family | Concrete sequence or stable signature | Source evidence | Replay ambiguity / conclusion |
|---:|---|---|---|---|
| C1 | `POST /chat/ai/data` | `read role-info/cache -> external AI workflow -> async INSERT hyperswitch_ai_interaction` | route `routes/chat.rs:18-49`; core `core/chat.rs:25-116`; builder `utils/chat.rs:16-61`; DB `db/hyperswitch_ai_interaction.rs:30-40`; query `query/hyperswitch_ai_interaction.rs:10-13` | Response is external-service driven; later list visibility depends on async DB insert completion. Need driver context plus DB collection version. |
| C2 | `GET /chat/ai/list` | `SELECT hyperswitch_ai_interaction WHERE merchant_id = ? ORDER BY created_at DESC LIMIT/OFFSET`; decrypt each row | route `routes/chat.rs:52-73`; core `core/chat.rs:118-213`; DB `db/hyperswitch_ai_interaction.rs:42-58`; query `query/hyperswitch_ai_interaction.rs:16-29`; model `hyperswitch_ai_interaction.rs:18-49` | Chat list is a mutable OLAP-style collection keyed by optional `merchant_id`, `limit`, and `offset`; inserts from C1 change page boundaries. |
| C3 | Subscriptions create / create-and-confirm | `find business_profile -> find customer -> INSERT subscription -> connector estimate/create -> create payment -> INSERT invoice -> UPDATE subscription` | `subscriptions/src/core.rs:31-121`, `175-308`; handler `core/subscription_handler.rs:38-78`, `85-116`; invoice `core/invoice_handler.rs:39-73`; storage `storage_impl/src/subscription.rs:21-75`; query `query/subscription.rs:12-66` | Route-local cross-resource cascade. Needs ordering across subscription row, invoice row, payment intent/attempt, customer connector id, and connector response. |
| C4 | Subscription confirm/update/pause/resume/cancel | `find subscription -> get latest invoice/list by subscription -> connector/payment call -> UPDATE invoice and/or UPDATE subscription -> optional GET subscription` | `subscriptions/src/core.rs:308-447`, `490-718`; handler `core/subscription_handler.rs:174-285`, `343-366`; invoice `core/invoice_handler.rs:77-256`; invoice storage `storage_impl/src/invoice.rs:49-89`; query `query/invoice.rs:17-67` | Same `(merchant_id, subscription_id)` and latest-invoice-by-`subscription_id` predicates return different versions as lifecycle actions progress. |
| C5 | Forex rates / convert | local cache miss -> `GET {forex_cache}_data` -> if stale/miss `SETNX {forex_cache}_lock` -> external fetch -> `SETEX {forex_cache}_data` -> `DEL {forex_cache}_lock` | routes `routes/currency.rs:11-72`; core `core/currency.rs:15-47`; helpers `utils/currency.rs:20-21`, `151-289`, `438-503`, `510-535` | Redis + in-memory cache + external API. Same forex key can be miss, stale hit, fresh hit, or locked; replay needs TTL/time and lock state. |
| C6 | Poll status for external authentication | payment/auth flow writes `poll_{merchant_id}_{external_authentication_*}` as `Pending` with TTL; webhook updates same key to `Completed`; `GET /poll/status/{poll_id}` reads it | reader `core/poll.rs:12-50`; key builders `core/utils.rs:2237-2250`, `common_utils/src/id_type/merchant.rs:153-156`, `payment.rs:53-56`, `authentication.rs:22-25`; pending write `core/payments.rs:4369-4388`; completed write `core/webhooks/incoming.rs:2585-2599` | Same Redis key changes pending/completed/expired across sessions; exact poll key and TTL must be recorded. |
| C7 | Authentication lifecycle routes | `INSERT authentication` on create; later eligibility/authenticate/sync/session routes `SELECT authentication WHERE merchant_id AND authentication_id` and update the same row; connector/webhook lookup by connector auth id also exists | routes `routes/authentication.rs:20-321`; create `core/unified_authentication_service.rs:579-704`, `709-848`; reads/updates `1051-1425`, `1591-1684`, `1977-2248`, `2288-2488`; DB `db/authentication.rs:50-153`; query `query/authentication.rs:12-86` | Stable auth row progresses Started/Pending/Success/Failure and stores connector ids/auth data. Needs row version plus causal ordering with payment-attempt `authentication_id` and tokenized CAVV data. |
| C8 | Authentication eligibility-check Redis handoff | `POST /authentication/{id}/eligibility-check` conditionally `SETEX authentication_eligibility_check_data_{merchant_id}_{authentication_id}`; `GET /authentication/{id}/eligibility-check` reads/deserializes same key | set path `core/unified_authentication_service.rs:1475-1531`; get path `core/unified_authentication_service.rs:1648-1684`; route `routes/authentication.rs:133-206` | Confirmed cross-request `SETEX -> GET` TTL handoff; same key can be present, expired, or overwritten by a later eligibility check. |
| C9 | `POST /three_ds_decision/execute` and payment 3DS decision use | `SELECT routing_algorithm WHERE algorithm_id = routing_id AND merchant_id = ?` -> parse Euclid program -> execute decision | route `routes/three_ds_decision_rule.rs:12-34`; core `core/three_ds_decision_rule.rs:24-67`; DB `db/routing_algorithm.rs:93-105`; query `query/routing_algorithm.rs:15-28`; payment call site `core/payments/operations/payment_confirm.rs:1551-1680` | DB-only read, but the same routing algorithm id can be updated/replaced by routing admin routes; replay needs routing algorithm version/content hash. |
| C10 | `POST /cache/invalidate/{key}` | user-supplied cache key -> `redact_from_redis_and_publish(CacheKind::All(key))` | route `routes/cache.rs:11-29`; core `core/cache.rs:8-25`; generic cache helper `storage_impl/src/redis/cache.rs:121-148`, `306-457` | Destructive arbitrary cache invalidation. Replay must model DEL/publish effect, not just route response. |
| C11 | `POST /apple_pay_certificates_migration` | per merchant: read merchant key store -> list MCA including disabled -> parse Apple Pay metadata -> transactional update many MCAs -> redact MCA/account/CGraph caches | route `routes/apple_pay_certificates_migration.rs:10-30`; core `core/apple_pay_certificates_migration.rs:17-115`; MCA storage `storage_impl/src/merchant_connector_account.rs:537-568`, `605-709` | Batch route mutates multiple connector-account rows/cache keys; list and update set depend on merchant input and current MCA collection. |
| C12 | `GET /feature_matrix` | no DB/Redis in checked source; builds response from connector implementations and `state.conf.pm_filters` | route/core `routes/feature_matrix.rs:20-163` | Matrix classification `OLAP_ANALYTICS_READ` is over-broad for this handler. Replay needs config snapshot only, unless connector metadata loading is instrumented elsewhere. |
| C13 | `GET /v1/sdk/configs/{profile_id}/{platform}/{sdk_config}.json` | publishable-key auth -> Superposition cached-config lookup with dimension filter `{profile_id, merchant_id, organization_id}` | route `routes/superposition_sdk_config.rs:11-36`; core `core/superposition_sdk_config.rs:12-56` | Not DB/Redis in router source, but external cached config is a long-lived mutable dependency; replay needs dimension tuple and returned config hash. |

### 9.1 Updated coverage conclusion

After this pass, the only full-matrix class that had no concrete section-8 mapping was `CHAT_OLAP`; it is now covered by C1-C2. The `OLAP_ANALYTICS_READ` route `/feature_matrix` is specifically checked and appears config/static rather than DB/Redis-backed in router source. The broad `GENERIC_DB_OR_CONFIG` class now has concrete subscription, forex, cache-invalidation, Apple Pay migration, and SDK-config examples.

