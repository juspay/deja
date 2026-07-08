# Hyperswitch Inbound HTTP Route Catalog

**Date:** 2026-05-08  
**Source:** `<repo-root>/vendor/hyperswitch-fresh` (commit `bc39324410031bec3e8c3d0ba924d81841c0c341`)  
**Scope:** Complete catalog of inbound Actix-web HTTP routes

---

## Executive Summary

**2026-05-15 MCP/local rerun:** the complete route catalog has been normalized into `raw/route-catalog-normalized.mcp-rerun.tsv` with 509 inbound route rows, exact local `route_registration` links, and exact local `handler_location` links.

| Statistic | Count |
|-----------|------:|
| Total inbound route rows | 509 |
| Distinct domain labels | 41 |
| Route rows with `v1` in feature flags | 360 |
| Route rows with `v2` in feature flags | 139 |
| Route rows with `olap` in feature flags | 364 |
| Route rows with `oltp` in feature flags | 149 |

The tables below are a human-oriented overview. Use `raw/route-catalog-normalized.mcp-rerun.tsv` as the queryable source for exact route IDs and file:line:column evidence.

---

## Route Categories

### 1. Health & System Routes
| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| GET | /health | v1 | health | routes/app.rs:702 | System |
| GET | /health/ready | v1 | deep_health_check | routes/app.rs:703 | System |
| GET | /v2/health | v2 | health | routes/app.rs:712 | System |
| GET | /v2/health/ready | v2 | deep_health_check | routes/app.rs:713 | System |
| POST | /cache/invalidate/{key} | - | invalidate | routes/app.rs:2370 | System |

### 2. Payment Routes (payments.rs)

#### V1 Payment Operations
| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /payments | oltp+v1 | payments::payments_create | routes/app.rs:957 | Payments |
| GET | /payments/{payment_id} | oltp+v1 | payments::payments_retrieve | routes/app.rs:968 | Payments |
| POST | /payments/{payment_id} | oltp+v1 | payments::payments_update | routes/app.rs:969 | Payments |
| POST | /payments/{payment_id}/confirm | oltp+v1 | payments::payments_confirm | routes/app.rs:975 | Payments |
| POST | /payments/{payment_id}/capture | oltp+v1 | payments::payments_capture | routes/app.rs:984 | Payments |
| POST | /payments/{payment_id}/cancel | oltp+v1 | payments::payments_cancel | routes/app.rs:978 | Payments |
| GET | /payments/list | olap+v1 | payments::payments_list | routes/app.rs:922 | Payments |
| POST | /payments/list | olap+v1 | payments::payments_list_by_filter | routes/app.rs:923 | Payments |
| GET | /payments/aggregate | olap+v1 | payments::get_payments_aggregates | routes/app.rs:939 | Payments |

#### V2 Payment Operations
| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /v2/payments/create-intent | olap+oltp+v2 | payments::payments_create_intent | routes/app.rs:781 | Payments |
| POST | /v2/payments | olap+oltp+v2 | payments::payments_create_and_confirm_intent | routes/app.rs:790 | Payments |
| POST | /v2/payments/{payment_id}/confirm-intent | olap+oltp+v2 | payments::payment_confirm_intent | routes/app.rs:819 | Payments |
| GET | /v2/payments/{payment_id}/get-intent | olap+oltp+v2 | payments::payments_get_intent | routes/app.rs:840 | Payments |
| PUT | /v2/payments/{payment_id}/update-intent | olap+oltp+v2 | payments::payments_update_intent | routes/app.rs:848 | Payments |
| GET | /v2/payments/list | olap+oltp+v2 | payments::payments_list | routes/app.rs:792 | Payments |

### 3. Payment Method Routes (payment_methods.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /payment_methods | oltp+v1 | payment_methods::create_payment_method | routes/app.rs:1095 | PaymentMethods |
| GET | /payment_methods/{payment_method_id} | oltp+v1 | payment_methods::retrieve_payment_method | routes/app.rs:1105 | PaymentMethods |
| DELETE | /payment_methods/{payment_method_id} | oltp+v1 | payment_methods::delete_payment_method | routes/app.rs:1108 | PaymentMethods |
| GET | /customers/{customer_id}/payment_methods | oltp+v1 | payment_methods::list_customer_payment_methods | routes/app.rs:1116 | PaymentMethods |
| POST | /payment_method_intents | oltp+v1 | payment_methods::payment_method_intent_create | routes/app.rs:1098 | PaymentMethods |

### 4. Refund Routes (refunds.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /refunds | oltp+v1 | refunds::refunds_create | routes/app.rs:1155 | Refunds |
| GET | /refunds/{refund_id} | oltp+v1 | refunds::refunds_retrieve | routes/app.rs:1165 | Refunds |
| POST | /refunds/{refund_id} | oltp+v1 | refunds::refunds_update | routes/app.rs:1168 | Refunds |
| GET | /refunds/list | olap+v1 | refunds::refunds_list | routes/app.rs:1175 | Refunds |

### 5. Customer Routes (customers.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /customers | oltp+v1 | customers::customers_create | routes/app.rs:1060 | Customers |
| GET | /customers/{customer_id} | oltp+v1 | customers::customers_retrieve | routes/app.rs:1068 | Customers |
| POST | /customers/{customer_id} | oltp+v1 | customers::customers_update | routes/app.rs:1070 | Customers |
| DELETE | /customers/{customer_id} | oltp+v1 | customers::customers_delete | routes/app.rs:1073 | Customers |
| GET | /customers/list | olap+v1 | customers::customers_list | routes/app.rs:1080 | Customers |

### 6. Mandate Routes (mandates.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| GET | /mandates/{mandate_id} | oltp+v1 | mandates::retrieve_mandate | routes/app.rs:1130 | Mandates |
| GET | /mandates/list | oltp+v1 | mandates::list_mandates | routes/app.rs:1135 | Mandates |
| REVOKE | /mandates/{mandate_id}/revoke | oltp+v1 | mandates::revoke_mandate | routes/app.rs:1140 | Mandates |

### 7. Merchant Account Routes (merchant_account.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /accounts | olap+v1 | merchant_account::create_merchant_account | routes/app.rs:1250 | Merchant |
| GET | /accounts/{merchant_id} | olap+v1 | merchant_account::retrieve_merchant_account | routes/app.rs:1260 | Merchant |
| POST | /accounts/{merchant_id} | olap+v1 | merchant_account::update_merchant_account | routes/app.rs:1265 | Merchant |

### 8. Profile Routes (profiles.rs)

#### V2 Profiles
| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /v2/profiles | olap+v2 | profiles::profile_create | routes/app.rs:2420 | Profile |
| GET | /v2/profiles/{profile_id} | olap+v2 | profiles::profile_retrieve | routes/app.rs:2425 | Profile |
| PUT | /v2/profiles/{profile_id} | olap+v2 | profiles::profile_update | routes/app.rs:2426 | Profile |

#### V1 Profiles
| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /account/{account_id}/business_profile | olap+v1 | profiles::profile_create | routes/app.rs:2488 | Profile |
| GET | /account/{account_id}/business_profile | olap+v1 | profiles::profiles_list | routes/app.rs:2489 | Profile |
| GET | /account/{account_id}/business_profile/{profile_id} | olap+v1 | profiles::profile_retrieve | routes/app.rs:2552 | Profile |

### 9. Connector Account Routes (admin.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /connector_accounts | olap+v1 | admin::create_connector_account | routes/app.rs:1185 | Connectors |
| GET | /connector_accounts/{connector_id} | olap+v1 | admin::retrieve_connector_account | routes/app.rs:1195 | Connectors |
| POST | /connector_accounts/{connector_id} | olap+v1 | admin::update_connector_account | routes/app.rs:1200 | Connectors |
| DELETE | /connector_accounts/{connector_id} | olap+v1 | admin::delete_connector_account | routes/app.rs:1205 | Connectors |

### 10. API Key Routes (api_keys.rs)

#### V2 API Keys
| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /v2/api-keys | olap+v2 | api_keys::api_key_create | routes/app.rs:2259 | APIKeys |
| GET | /v2/api-keys/list | olap+v2 | api_keys::api_key_list | routes/app.rs:2260 | APIKeys |
| GET | /v2/api-keys/{key_id} | olap+v2 | api_keys::api_key_retrieve | routes/app.rs:2263 | APIKeys |
| PUT | /v2/api-keys/{key_id} | olap+v2 | api_keys::api_key_update | routes/app.rs:2264 | APIKeys |
| DELETE | /v2/api-keys/{key_id} | olap+v2 | api_keys::api_key_revoke | routes/app.rs:2265 | APIKeys |

#### V1 API Keys
| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /api_keys/{merchant_id} | olap+v1 | api_keys::api_key_create | routes/app.rs:2275 | APIKeys |
| GET | /api_keys/{merchant_id}/list | olap+v1 | api_keys::api_key_list | routes/app.rs:2276 | APIKeys |
| GET | /api_keys/{merchant_id}/{key_id} | olap+v1 | api_keys::api_key_retrieve | routes/app.rs:2279 | APIKeys |
| DELETE | /api_keys/{merchant_id}/{key_id} | olap+v1 | api_keys::api_key_revoke | routes/app.rs:2281 | APIKeys |

### 11. User & Auth Routes (user.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /user/signin | olap+v1 | user::user_signin | routes/app.rs:2783 | Auth |
| POST | /user/signout | olap+v1 | user::signout | routes/app.rs:2787 | Auth |
| POST | /user/rotate_password | olap+v1 | user::rotate_password | routes/app.rs:2788 | Auth |
| POST | /user/change_password | olap+v1 | user::change_password | routes/app.rs:2789 | Auth |
| POST | /user/oidc | olap+v1 | user::sso_sign | routes/app.rs:2786 | Auth |
| GET | /user/2fa/totp/begin | olap+v1 | user::totp_begin | routes/app.rs:2875 | Auth |
| POST | /user/2fa/totp/verify | olap+v1 | user::totp_verify | routes/app.rs:2879 | Auth |

### 12. Webhook Routes (webhooks.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /webhooks/{merchant_id}/{connector_id_or_name} | oltp+v1 | receive_incoming_webhook | routes/app.rs:2129 | Webhooks |
| GET | /webhooks/{merchant_id}/{connector_id_or_name} | oltp+v1 | receive_incoming_webhook | routes/app.rs:2131 | Webhooks |
| PUT | /webhooks/{merchant_id}/{connector_id_or_name} | oltp+v1 | receive_incoming_webhook | routes/app.rs:2131 | Webhooks |
| POST | /webhooks/frm_fulfillment | oltp+v1+frm | frm_routes::frm_fulfillment | routes/app.rs:2139 | Webhooks |

### 13. Analytics Routes (analytics.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| GET | /analytics/v1/metrics | olap | analytics::get_metrics | routes/analytics.rs | Analytics |
| POST | /analytics/v1/metrics | olap | analytics::get_metrics | routes/analytics.rs | Analytics |
| GET | /analytics/v1/connector_event_logs | olap | connector_events::get_event_logs | routes/analytics.rs | Analytics |

### 14. Dispute Routes (disputes.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| GET | /disputes/list | olap+v1 | disputes::retrieve_disputes_list | routes/app.rs:2293 | Disputes |
| GET | /disputes/profile/list | olap+v1 | disputes::retrieve_disputes_list_profile | routes/app.rs:2296 | Disputes |
| POST | /disputes/accept/{dispute_id} | olap+v1 | disputes::accept_dispute | routes/app.rs:2305 | Disputes |
| GET | /disputes/aggregate | olap+v1 | disputes::get_disputes_aggregate | routes/app.rs:2308 | Disputes |
| POST | /disputes/evidence | olap+v1 | disputes::submit_dispute_evidence | routes/app.rs:2316 | Disputes |
| GET | /disputes/{dispute_id} | olap+v1 | disputes::retrieve_dispute | routes/app.rs:2325 | Disputes |

### 15. GSM Routes (gsm.rs)

| Method | Full Path | Feature Flags | Handler | Location | Domain |
|--------|-----------|---------------|---------|----------|--------|
| POST | /gsm | v1+olap | gsm::create_gsm_rule | routes/app.rs:2598 | GSM |
| POST | /gsm/get | v1+olap | gsm::get_gsm_rule | routes/app.rs:2599 | GSM |
| POST | /gsm/update | v1+olap | gsm::update_gsm_rule | routes/app.rs:2600 | GSM |
| POST | /gsm/delete | v1+olap | gsm::delete_gsm_rule | routes/app.rs:2601 | GSM |

### 16. Additional Routes

Complete route listings for the following domains are available in the raw analysis:
- **Cards:** BIN info, card create/update
- **Files:** Upload, download, delete
- **Payment Links:** Create, retrieve, initiate
- **Authentication:** 3DS eligibility, authenticate
- **Process Tracking:** Revenue recovery workflows
- **Recovery Data Backfill:** v2 recovery operations
- **SDK Configs:** Superposition SDK configuration

---

## Route Distribution by Domain

| Domain | Estimated Route Count |
|--------|----------------------|
| Payments | ~50 |
| PaymentMethods | ~15 |
| Refunds | ~10 |
| Customers | ~10 |
| Mandates | ~5 |
| Merchant/Account | ~15 |
| Profile | ~25 |
| Connectors | ~15 |
| API Keys | ~10 |
| Users/Auth | ~60 |
| Roles | ~15 |
| Webhooks | ~10 |
| Analytics | ~20 |
| Disputes | ~15 |
| GSM | ~8 |
| Other | ~30 |

---

## Confidence Levels

- **high:** Clear handler function, exact line numbers verified
- **medium:** Handler in closure or conditional compilation block
- **low:** Complex nested scope, handler inference required

---

*See `raw/agent-routes.md` for complete detailed route listings with full line:column references.*
