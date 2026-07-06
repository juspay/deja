# Fire-and-Forget Async Patterns Outside API Locking

**Generated:** 2026-05-15  
**Source:** `vendor/hyperswitch-fresh`  
**Inputs used:** saved rust-brain route matrix/artifacts under `docs/hyperswitch-cascade/raw/`, `03_DEPENDENCY_CASCADES_FULL_RUST_BRAIN.md`, `06_DB_REDIS_AMBIGUITY_PATTERNS.md`, and local source scans for detached execution primitives.

---

## 0. Executive answer

Static pass result: **83 / 509 route rows have a reachable fire-and-forget path** where route-facing code can spawn work and continue without awaiting that work before returning the API response.

This is a **possible/reachable route-row count**, not a guarantee that every request to those routes always spawns. Many paths are gated by config, feature flags, status transitions, connector response type, outgoing-webhook status mapping, `force_sync`, or dynamic-routing settings.

| Bucket | Route rows | Why it matters for replay |
|---|---:|---|
| Payments connector-response family | 26 | Outgoing webhooks, async save-to-locker, dynamic-routing score updates, process-tracker insertion, and UCS shadow comparison can continue after response/lock release. |
| Refund connector / force-sync family | 6 | UCS shadow calls, refund outgoing webhooks, and payment-intent state-metadata updates can race later reads. |
| Payout connector-response family | 5 | Outgoing payout webhook delivery is detached. UCS shadow payout calls can also detach at gateway layer. |
| Incoming webhook family | 8 | Incoming webhook handling updates state under its own lock, frees it, then can spawn outgoing webhook delivery and state-metadata updates. |
| API-key decision-service sync | 6 | DB mutation returns while decision-service add/revoke job continues. |
| Merchant/publishable-key decision-service sync | 8 | Merchant/account creation or deletion returns while publishable-key decision-service add/revoke continues. |
| User lineage-context updates | 18 | JWT/switch responses can return before `active_user.lineage_context` update finishes. |
| Forex cache refresh | 2 | API can return stale/missing result while background refresh updates Redis/in-memory cache later. |
| Chat AI write-behind | 1 | Chat response returns before `hyperswitch_ai_interaction` insert finishes. |
| Connector-create dispute-sync scheduling | 2 | Connector create can return before dispute-list process-tracker scheduling finishes. |
| Dispute accept outgoing webhook | 1 | Dispute accept awaits event creation but not actual outgoing webhook delivery. |

Distinct route-row IDs from the 509-row matrix:

```text
20,22,24,26,27,28,29,30,31,37,38,39,40,41,42,43,44,45,53,57,60,63,64,70,75,76,
103,104,105,108,109,110,
112,125,126,127,128,
168,169,
221,229,230,237,245,
287,288,289,291,292,293,294,295,
298,301,302,303,306,307,
312,364,
371,374,375,378,381,382,386,387,388,394,395,402,408,409,410,419,424,425,430,431,434,435,436
```

### Count caveats

- **Count basis:** route rows from `03_DEPENDENCY_CASCADES_FULL_RUST_BRAIN.md`, not unique Actix handler functions.
- **Conservative exclusions:** infra/startup spawns, scheduler/drainer workers, tests, analytics `JoinSet` tasks that are drained with `join_next`, and `tokio::spawn` handles that are joined through `flatten_join_error` were excluded.
- **Conditional inclusions:** connector families are counted when the route can reach the connector-response path that contains detached webhook/shadow/side-effect tasks.

---

## 1. What API locking is

Hyperswitch API locking is a Redis-backed per-resource request serialization layer used by selected routes, especially payment/payout routes.

### 1.1 Implementation shape

Source anchors:

- `vendor/hyperswitch-fresh/crates/router/src/core/api_locking.rs:9-35` defines `API_LOCK_PREFIX`, `LockStatus`, `LockAction`, and `LockingInput`.
- `vendor/hyperswitch-fresh/crates/router/src/core/api_locking.rs:37-45` builds Redis lock keys as:

```text
API_LOCK_{merchant_id}_{api_identifier}_{unique_locking_key}
```

- `vendor/hyperswitch-fresh/crates/router/src/core/api_locking.rs:47-145` implements acquisition:
  - `Hold`: Redis `SET NX EX` one lock key, retrying with configured delay.
  - `HoldMultiple`: multi-key set-if-not-exists with one request id, retrying until all keys are owned by this request.
  - `QueueWithOk`, `Drop`, `NotApplicable`: currently no-op in `perform_locking_action`.
- `vendor/hyperswitch-fresh/crates/router/src/core/api_locking.rs:147-274` releases locks by verifying the stored request id, then deleting the Redis key(s).
- `vendor/hyperswitch-fresh/crates/router/src/services/api.rs:265-280` wraps route execution:

```rust
lock_action.perform_locking_action(...).await?;
let res = func(...).await;
lock_action.free_lock_action(...).await?;
```

### 1.2 What it protects

It protects the **main future returned by the HTTP handler**. For example, payment request types implement `GetLockingInput` in `routes/payments.rs:2872-3239`, often locking on the payment id and sometimes also customer id.

This serializes overlapping requests that use the same API lock key while the route future is still running.

### 1.3 What it does not protect

It does **not** wait for tasks spawned inside the handler/core path if their `JoinHandle` is discarded. Once `func(...).await` resolves, `server_wrap_util` frees the API lock and response handling continues. Any detached task that still mutates DB/Redis/cache/external services can race with:

1. the next API request for the same resource,
2. replay verification reads,
3. another detached task from the same or another request,
4. background workers/process-tracker retries.

For replay, API locking is therefore not a complete causal boundary. The recorder needs child-task lineage and completion/side-effect events.

---

## 2. Detached spawn sites found

The scan looked for `tokio::spawn`, `tokio::task::spawn`, `actix_web::rt::spawn`, `std::thread::spawn`, `thread::spawn`, and `.spawn(...)`.

### 2.1 Route-facing fire-and-forget sites

| # | Source | Detached work | Route blast radius |
|---:|---|---|---|
| F1 | `router/src/core/chat.rs:89-114` | Insert `hyperswitch_ai_interaction` after AI response is cloned for return. | 1 route: `POST /chat/ai/data` (`#364`). |
| F2 | `router/src/utils/currency.rs:204-227` | Refresh forex data under Redis lock in background. | 2 routes: `/forex/rates`, `/forex/convert_from_minor` (`#168-169`). |
| F3 | `router/src/services/authentication/decision.rs:194-212` | Generic `spawn_tracked_job` for auth/decision-service mutations. | API-key and merchant/publishable-key mutations (`#221,#229,#230,#298,#301,#302,#303,#306,#307,#371,#378,#394,#395,#402`). |
| F4 | `router/src/utils/user.rs:362-389` | Update `active_user.lineage_context` in DB after token/switch flow. | 18 user auth/switch rows listed in section 3.6. |
| F5 | `router/src/utils.rs:1218-1250` | Payment outgoing webhook event+delivery task. | Payment connector-response family. |
| F6 | `router/src/utils.rs:1271-1323` | Refund outgoing webhook event+delivery task. | Refund create/sync family. |
| F7 | `router/src/utils.rs:1345-1398` | Payout outgoing webhook event+delivery task. | Payout create/update/confirm/cancel/fulfill family. |
| F8 | `router/src/utils.rs:1412-1458` | Subscription outgoing webhook event+delivery task. | Mostly invoice-sync workflow; no direct subscription API row counted in the 83. |
| F9 | `router/src/core/webhooks/outgoing.rs:50-221` | Create/store event, add process-tracker retry task, then spawn actual webhook delivery. | All v1 callers of `create_event_and_trigger_outgoing_webhook`. |
| F10 | `router/src/core/webhooks/outgoing_v2.rs:37-153` | v2 equivalent: create/store event, then spawn actual webhook delivery. | v2 payment/incoming-webhook callers. |
| F11 | `router/src/core/payments/operations/payment_response.rs:658-718` | Save payment method in locker and update payment attempt with PM id after response path continues. | Payment connector-response family when save-PM conditions match. |
| F12 | `router/src/core/payments/operations/payment_response.rs:2845-2866` | Update OpenRouter/gateway score after terminal payment. | Payment connector-response family when dynamic routing is enabled and terminal status reached. |
| F13 | `router/src/core/payments/operations/payment_confirm.rs:1357-1386` | Insert/update payment process-tracker task asynchronously. | Payment confirm-style v1 flows when `should_add_task_to_process_tracker` is true. |
| F14 | `hyperswitch_interfaces/src/api/gateway.rs:413-452` | Payments UCS shadow execution + comparison send. | Payment connector family when `DirectAndShadow` execution path is used. |
| F15 | `hyperswitch_interfaces/src/api/gateway.rs:547-586` | Payout UCS shadow execution + comparison send. | Payout connector family when `DirectAndShadow` execution path is used. |
| F16 | `router/src/core/refunds.rs:648-676`, `1205-1233` | Refund UCS shadow execute/sync + comparison send. | Refund create/sync family when shadow UCS path is used. |
| F17 | `router/src/core/refunds.rs:1087-1108`, `1448-1469` | Payment-intent state metadata update after refund sync/create. | Refund create/sync family on successful refund transitions. |
| F18 | `router/src/core/webhooks/incoming.rs:2199-2211`, `2905-2934` | Payment-intent state metadata update after refund/dispute incoming webhook. | Incoming webhook family. |
| F19 | `router/src/core/disputes.rs:1040-1124` | Schedule dispute-list process-tracker task after connector account creation. | Connector-create routes `#237,#245`. |
| F20 | `router/src/workflows/dispute_list.rs:91-113` | Schedule next dispute-list task in workflow without awaiting. | Background workflow, not counted as an HTTP route row. |

### 2.2 Explicitly excluded spawn patterns

| Excluded pattern | Why excluded |
|---|---|
| `router/src/core/payments/operations/payment_confirm.rs:194-324`, `534-637`, `2637-2832` | Join handles are awaited through `utils::flatten_join_error` / `try_join!`. |
| `router/src/core/payments/operations/payment_recurrence.rs:163-247`, `595-642` | Join handles are awaited through `flatten_join_error`. |
| `router/src/core/payments/operations/payment_response.rs:2710-2822` | Join handles for payment intent/attempt/mandate updates are awaited. |
| `analytics/src/**/core.rs` `JoinSet::spawn` | Join sets are drained with `join_next().await`. |
| `redis_interface/src/commands.rs` `spawn_blocking` | Spawn-blocking handles are awaited. |
| `router/src/lib.rs`, `router/src/bin/scheduler.rs`, `storage_impl/src/redis.rs`, `storage_impl/src/redis/pub_sub.rs`, `drainer`, `scheduler` | Long-lived infra/background workers, not request-return races. |
| tests under `router/tests`, `test_utils`, and `db/events.rs` test concurrency | Not production route handlers. |

---

## 3. Route blast-radius by family

### 3.1 Payments connector-response family — 26 route rows

Counted route IDs:

```text
20,22,24,26,27,28,29,30,31,37,38,39,40,41,42,43,44,45,53,57,60,63,64,70,75,76
```

Representative paths:

- `POST /payments`
- `POST /payments/sync`
- `POST /payments/{payment_id}`
- `POST /payments/{payment_id}/confirm`
- `POST /payments/{payment_id}/cancel`
- `POST /payments/{payment_id}/capture`
- redirect/complete-authorize routes
- `POST /v2/payments`
- `POST /v2/payments/{payment_id}/confirm-intent`
- `POST /v2/payments/{payment_id}/capture`
- `POST /v2/payments/{payment_id}/cancel`

Source anchors:

- `router/src/core/payments.rs:1461-1471`, `1702-1711` call `utils::trigger_payments_webhook(...)` after connector processing.
- `router/src/utils.rs:1218-1250` spawns the outgoing webhook task.
- `router/src/core/payments/operations/payment_response.rs:658-718` spawns async save-payment-method side effects.
- `router/src/core/payments/operations/payment_response.rs:2845-2866` spawns dynamic-routing score updates.
- `router/src/core/payments/operations/payment_confirm.rs:1357-1386` spawns payment process-tracker insertion.
- `hyperswitch_interfaces/src/api/gateway.rs:413-452` spawns payment UCS shadow execution when configured.

Replay risk:

- Payment response can be returned while webhook event delivery, save-to-locker, process-tracker insertion, dynamic-routing score update, or UCS shadow comparison is still running.
- API locking on `payment_id` only covers the awaited main route future. The detached children can mutate payment attempt, payment method, process tracker, routing state, events, or external comparison sinks after the lock is released.

### 3.2 Refund connector / force-sync family — 6 route rows

Counted route IDs:

```text
103,104,105,108,109,110
```

Representative paths:

- `POST /refunds`
- `POST /refunds/sync`
- `GET /refunds/{id}` with `force_sync=true`
- `POST /v2/refunds`
- `GET /v2/refunds/{id}` with `force_sync=true`
- `POST /v2/refunds/{id}` with gateway creds / force-sync payload

Source anchors:

- `router/src/core/refunds.rs:648-676` and `1205-1233` spawn UCS shadow refund execute/sync.
- `router/src/core/refunds.rs:1087-1108` and `1448-1469` spawn payment-intent state-metadata update after refund sync/create.
- `router/src/core/refunds.rs:488-496`, `1121-1129` call `trigger_refund_outgoing_webhook`.
- `router/src/utils.rs:1271-1323` spawns refund outgoing webhook event+delivery.

### 3.3 Payout connector-response family — 5 route rows

Counted route IDs:

```text
112,125,126,127,128
```

Representative paths:

- `POST /payouts/create`
- `PUT /payouts/{payout_id}`
- `POST /payouts/{payout_id}/confirm`
- `POST /payouts/{payout_id}/cancel`
- `POST /payouts/{payout_id}/fulfill`

Source anchors:

- `router/src/core/payouts.rs:412`, `481`, `559`, `715`, `804` call `trigger_webhook_and_handle_response`.
- `router/src/core/payouts.rs:2860-2867` calls `utils::trigger_payouts_webhook`.
- `router/src/utils.rs:1345-1398` spawns payout outgoing webhook event+delivery.
- `hyperswitch_interfaces/src/api/gateway.rs:547-586` spawns payout UCS shadow execution when configured.

### 3.4 Incoming webhook family — 8 route rows

Counted route IDs:

```text
287,288,289,291,292,293,294,295
```

Representative paths:

- `POST|GET|PUT /webhooks/{merchant_id}/{connector_id_or_name}`
- `POST /webhooks/relay/{merchant_id}/{connector_id}`
- `POST|GET|PUT /v2/webhooks/{merchant_id}/{profile_id}/{connector_id}`
- `POST /v2/webhooks/recovery/{merchant_id}/{profile_id}/{connector_id}`

Source anchors:

- `router/src/core/webhooks/incoming.rs:1489-1684` performs a payment API lock internally for incoming payment webhooks.
- `router/src/core/webhooks/incoming.rs:1619-1632`, `1922-1937`, `1992-2006`, `2219-2230`, `2605-2615`, `2703-2715`, `2812-2822`, `2943-2954`, `3030-3040` call outgoing webhook creation.
- `router/src/core/webhooks/incoming.rs:2199-2211`, `2905-2934` spawn payment-intent metadata updates.
- `router/src/core/webhooks/outgoing.rs:213-228` spawns actual v1 outgoing delivery.
- `router/src/core/webhooks/outgoing_v2.rs:145-157` spawns actual v2 outgoing delivery.

Important API-locking note: incoming payment webhook flow explicitly locks the payment while calling `payments_core`, but it frees that lock before returning to later outgoing-webhook code. The outgoing delivery is still detached.

### 3.5 Decision-service async sync — 14 route rows

API-key rows:

```text
298,301,302,303,306,307
```

Merchant/publishable-key rows:

```text
221,229,230,371,378,394,395,402
```

Representative paths:

- `POST /v2/api-keys`
- `PUT /v2/api-keys/{key_id}`
- `DELETE /v2/api-keys/{key_id}`
- legacy `/api_keys/{merchant_id}` create/update/delete
- `POST /accounts`
- `POST /v2/merchant-accounts`
- `DELETE /accounts/{id}`
- user-driven merchant/org/platform creation routes

Source anchors:

- `router/src/core/api_keys.rs:148-159`, `300-311`, `457-464` call `spawn_tracked_job` for add/revoke API key.
- `router/src/core/admin.rs:93-111` defines detached publishable-key add.
- `router/src/core/admin.rs:403` calls it on merchant account create.
- `router/src/core/admin.rs:1399-1407` spawns publishable-key revoke on merchant account delete.
- `router/src/services/authentication/decision.rs:194-212` implements `spawn_tracked_job`.
- `router/src/types/domain/user.rs:521-595` user merchant creation delegates to admin merchant creation.

Replay risk:

The API response can report DB success while the decision/auth service still lacks the new key or still accepts a revoked key until the detached job completes.

### 3.6 User lineage-context updates — 18 route rows

Counted route IDs:

```text
374,375,381,382,386,387,388,408,409,410,419,424,425,430,431,434,435,436
```

Representative paths:

- user switch routes: `/v2/user/switch/merchant`, `/v2/user/switch/profile`, `/user/switch/org`, `/user/switch/merchant`, `/user/switch/profile`
- final token routes: `/user/signin`, `/user/v2/signin`, `/user/oidc`, `/user/2fa/terminate`, `/user/auth/select`, `/user/from_email`, `/user/verify_email`, `/user/terminate_accept_invite`, `/user/accept_invite_from_email`, `/user/signup`

Source anchors:

- `router/src/utils/user.rs:362-389` spawns `update_active_user_by_user_id(... LineageContextUpdate ...)`.
- direct switch callers: `router/src/core/user.rs:3497-3501`, `3692-3696`, `3824-3828`.
- token decision manager caller: `router/src/types/domain/user/decision_manager.rs:125-211`.
- token routes call `NextFlow::get_token`: `router/src/core/user.rs:248-251`, `284-286`, `1477-1483`, `1583-1589`, `2160-2163`, `2278-2280`, `2643-2645`, `3088-3094`, `3131-3140`.

Replay risk:

JWT/cookie issuance can be replayed before the `active_user.lineage_context` DB write exists, so later user-context reads can differ depending on whether the detached write has completed.

### 3.7 Forex cache refresh — 2 route rows

Counted route IDs:

```text
168,169
```

Source anchors:

- `router/src/routes/currency.rs:11-72` routes `/forex/rates` and `/forex/convert_from_minor`.
- `router/src/utils/currency.rs:204-227` spawns `acquire_redis_lock_and_call_forex_api`.

Replay risk:

A request can return stale data or `ForexDataUnavailable` while the spawned refresh later writes `{forex_cache}_data` and deletes `{forex_cache}_lock`.

### 3.8 Chat AI write-behind — 1 route row

Counted route ID:

```text
364
```

Source anchors:

- `router/src/routes/chat.rs:18-49` route `POST /chat/ai/data`.
- `router/src/core/chat.rs:89-114` spawns construction/insertion of `hyperswitch_ai_interaction`.

Replay risk:

The chat response and later `GET /chat/ai/list` can race with the asynchronous insert.

Colored call graph: `docs/hyperswitch-cascade/graphs/chat-ai-fire-and-forget.mmd`.

### 3.9 Connector-create dispute sync scheduling — 2 route rows

Counted route IDs:

```text
237,245
```

Source anchors:

- `router/src/core/admin.rs:2805` calls `disputes::schedule_dispute_sync_task` after connector creation.
- `router/src/core/disputes.rs:1040-1124` spawns `add_dispute_list_task_to_pt`.

Replay risk:

Connector create can return before the dispute-list process-tracker task exists.

### 3.10 Dispute accept outgoing webhook — 1 route row

Counted route ID:

```text
312
```

Source anchors:

- `router/src/core/disputes.rs:120-154` accepts dispute and calls `update_dispute_data`.
- `router/src/core/disputes.rs:903-939` awaits event creation, but `create_event_and_trigger_outgoing_webhook` spawns actual delivery.

---

## 4. Replay implications

Detached tasks require a wider causal model than DB/Redis key ordinals inside a single HTTP future.

Minimum event model for replay:

```yaml
scope_id: request/span id for the parent HTTP route
child_task_id: monotonic id allocated at spawn site
parent_scope_id: original request/span id
spawn_ordinal: sequence number inside parent scope
spawn_site: file:line + logical label
api_lock_keys_held_at_spawn: [optional API_LOCK_* keys]
api_lock_released_at: timestamp/ordinal
child_started_at: timestamp/ordinal
child_completed_at: timestamp/ordinal
child_side_effects:
  - backend: postgres|redis|http|process_tracker|event_log|decision_service
    operation: insert|update|delete|publish|http_call|cache_write
    resource_key: concrete id/key/query tuple
    result_hash: stable response/content hash
```

For deterministic replay, either:

1. **join/replay child tasks before parent completion**, if production semantics can be changed, or
2. **record detached child tasks as independent causal scopes** linked to the parent, and replay their side effects with recorded timing/order constraints.

API locking should be recorded as context, but it is not sufficient: detached children can outlive the lock.

---

## 5. Live rust-brain revalidation

**Validated:** 2026-05-15 after reconnecting the `rust-brain` MCP server.

Why MCP had appeared unavailable: the `rustbrain-mcp` SSE server was running on port `3001`, but Pi's MCP gateway sent `capabilities.sampling = {}` in `initialize`. The MCP server expected `sampling: Option<()>` and logged `JSON error: invalid type: map, expected unit`, so the Pi gateway timed out during connect. The local rust-brain MCP source was patched to deserialize `sampling` as `Option<serde_json::Value>`, then `mcp-sse` was rebuilt/restarted. Pi then listed 16 rust-brain tools, including `rust_brain_pg_query`.

Live `rust_brain_pg_query` results:

| Claim group | Live rust-brain result | Local verification result |
|---|---|---|
| F1-F4, F6-F13, F16-F19 | Confirmed: rust-brain found the expected functions and `tokio::spawn` in indexed bodies. | Confirmed against `vendor/hyperswitch-fresh`. |
| F5 payment outgoing webhook helper | rust-brain indexed a feature-gated `trigger_payments_webhook` body without the spawn (`todo!()` in the indexed snapshot). | Local v1 source confirms the spawned webhook task at `router/src/utils.rs:1227-1230`. |
| F14-F15 gateway UCS shadow execution | rust-brain indexed `gateway.rs` as a source file but did not extract function bodies for this file. | Local source confirms `tokio::spawn` at `hyperswitch_interfaces/src/api/gateway.rs:413` and `:547`. |
| F20 dispute-list workflow reschedule | rust-brain indexed `workflows/dispute_list.rs` as a source file with zero extracted function bodies. | Local source confirms `tokio::spawn` at `router/src/workflows/dispute_list.rs:98`; this remains excluded from the 83 HTTP route-row count. |
| API locking boundary | rust-brain confirms `api_locking::{perform_locking_action, free_lock_action}` and `services::api::server_wrap_util` are indexed. | Local source confirms `server_wrap_util` acquires the lock, awaits `func(...)`, then frees the lock at `services/api.rs:269-279`. |
| Route blast-radius count | Derived from the 509-row local route matrix, not from rust-brain route IDs. | Revalidated: 83 distinct listed route IDs, all present in `03_DEPENDENCY_CASCADES_FULL_RUST_BRAIN.md`; bucket sum is 83. |

Validation conclusion: the artifact's **83 / 509 reachable route-row** claim still holds. Live rust-brain confirms most indexed spawn sites directly; the remaining exceptions are explained by feature-gated or incompletely extracted rust-brain bodies and are locally source-confirmed.
