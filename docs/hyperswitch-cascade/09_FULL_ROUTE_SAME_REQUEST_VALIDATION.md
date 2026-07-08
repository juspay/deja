# Full Route-Level Same-Request Ambiguity Validation

**Date**: 2026-05-15  
**Source commit**: `bc39324410031bec3e8c3d0ba924d81841c0c341`  
**Method**: Static analysis of all 376 unique handler functions across 509 routes, cross-validated with rust-brain MCP

## Executive Summary

**Confirmed same-request same-signature duplicate-read scenarios: 0**

After exhaustively analyzing all 509 routes and 376 unique handler functions — including transitive call chains up to depth 4 — no route handler was found where the **same concrete read signature** (same method, same key/query) is invoked twice within a single request with a write between them that could change the response.

Every candidate turned out to be one of:
1. **Different keys** (V2 vs V1, old_id vs new_id)
2. **Mutually exclusive branches** (early-return vs normal path)
3. **Different entity instances** (same method but different primary key arguments)
4. **Iterator operations** (`filter_map` is a Rust iterator combinator, not a DB read)

## Methodology

### Phase 1: Handler-Level Duplicate Token Scan
Scanned all 376 handler function bodies for repeated `.find_*`, `.list_*`, `.filter_*`, `get_and_deserialize_key`, `HGet`, etc. tokens where the same token appears ≥2 times.

**Result**: 12 entries across 5 unique handlers.

### Phase 2: Write-Between Filter
For each duplicate, checked if any write operation (`.update_*`, `.delete_*`, `.insert_*`, `serialize_and_set_key`, `Hset`, etc.) falls between adjacent repeated reads.

**Result**: 3 handlers had write between duplicate reads.

### Phase 3: Concrete Key Verification
Manually verified each candidate's actual call arguments at source level.

### Phase 4: Cross-Function Call Chain Analysis
For all 376 handlers, flattened the call chain (up to depth 4) and collected all reads/writes from callee functions. Checked for same-entity-type read→write→read patterns.

### Phase 5: Cache-Aside Pattern Check
Checked `get_or_populate_redis` and `get_or_populate_in_memory` for same-key cache-aside miss→set→hit patterns. No handler invokes cache-aside twice on the same key within one request.

### Phase 6: MCP Cross-Validation
Used `rust_brain_pg_query` to verify function locations and read counts for all candidates. MCP results confirmed local findings (with expected stale-path offsets).

## Candidate Analysis

### Candidate 1: `user_role::update_user_role`
- **Route**: `user.post.user.user.update_role`
- **File**: `crates/router/src/core/user_role.rs:155-383`
- **Pattern**: `find_user_role(V2)` → `update_user_role(V2)` → `find_user_role(V1)`
- **Verdict**: ❌ NOT same-key — V2 and V1 are different `version` column values in the WHERE clause
- **Evidence**: `crates/diesel_models/src/query/user_role.rs:96` — `.and(dsl::version.eq(version))`

### Candidate 2: `user_role::delete_user_role`
- **Route**: `user.delete.user.user.delete`
- **File**: `crates/router/src/core/user_role.rs:522-786`
- **Pattern**: `find_user_role(V2)` → `delete_user_role(V2)` → `find_user_role(V1)`
- **Verdict**: ❌ NOT same-key — same as above, V2 ≠ V1 in the database query

### Candidate 3: `webhook_events::retry_delivery_attempt`
- **Route**: `webhooks.post.events.by_merchant_id.by_event_id.retry`
- **File**: `crates/router/src/core/webhooks/webhook_events.rs:265-364`
- **Pattern**: `find_event(merchant_id, event_id)` → `insert_event(new_event)` → `find_event(merchant_id, new_event_id)`
- **Verdict**: ❌ NOT same-key — `event_id ≠ new_event_id` (a new UUID is generated for the retry)

### Candidate 4: `routing::enable_specific_routing_algorithm`
- **Routes**: `routing.post.account.*.dynamic_routing.elimination.create`, `routing.post.account.*.dynamic_routing.success_based.create`
- **File**: `crates/router/src/core/routing/helpers.rs:2019-2107`
- **Pattern**: `find_routing_algorithm(profile_id, algo_id)` at line 2080 → `update_enabled_features` → `find_routing_algorithm(profile_id, algo_id)` at line 2098
- **Verdict**: ❌ Mutually exclusive branches — first read is inside `if features == required { return }` (line 2075-2084), second read is in the else path (line 2098). Only one executes per request.

### Candidates 5-6: `user::resend_invite`, `user::switch_merchant_for_user_in_org`
- **Pattern**: Same method called with V2 then V1 (or in different match branches)
- **Verdict**: ❌ NOT same-key / different branches

### Non-Candidate: `payments::get_payment_filters`
- **Pattern**: `filter_map` appears 3 times
- **Verdict**: ❌ `filter_map` is a Rust `Iterator::filter_map` call, not a database read

### Non-Candidate: `user_role::list_roles_with_info`
- **Pattern**: `filter_map` appears 3 times
- **Verdict**: ❌ Same — Rust iterator method, not a DB read

## Cross-Function Analysis Summary

The transitive call-chain analysis was extremely noisy because:
1. `authenticate_and_fetch` is a ~5000-line dispatch function with many match arms. Every call to it generates dozens of "potential" reads from branches that won't execute.
2. Generic infrastructure like `kv_wrapper` and `get_or_populate_redis` appears in almost every call chain.

After filtering for same-entity read→write→read patterns, only the V2→write(V2)→V1 user_role cases survived — and these are confirmed different-key.

## Cache-Aside Deep Dive

The `get_or_populate_in_memory` helper at `crates/storage_impl/src/redis/cache.rs:343` implements:

```
1. Check in-memory cache → hit? return
2. GET from Redis → hit? populate in-memory, return
3. Call fallback (DB query) → SET in Redis → populate in-memory → return
```

No handler invokes this on the **same key** twice in one request. The pattern that would cause ambiguity:

```
GET key → miss → DB read → SET key    (first call)
... (something invalidates the cache) ...
GET key → hit (returns stale)          (second call)
```

...does not occur within any single request. Cache invalidation (`publish_and_redact`) happens on update/delete paths, but no handler reads the same entity twice with an invalidating write in between.

## Quantitative Summary

| Metric | Count |
|--------|-------|
| Total routes | 509 |
| Unique handler functions | 376 |
| Handlers with repeated read-method token | 5 |
| Handlers with write between repeated reads | 3 |
| Handlers with same **concrete key** repeated read + write between | **0** |
| Routes with same-request same-signature ambiguity | **0** |

## MCP Query Log

| Query | Rows | Purpose |
|-------|------|---------|
| Functions with ≥2 `.find_`/`.list_`/`.filter_` in `core/` | 136 | Identify high-read-count core functions |
| Functions with ≥2 Redis reads (`get_and_deserialize_key`, `HGet`) | 10 | Identify Redis-level duplicates |
| Handler function locations (all 376) | Cross-referenced | Verify MCP has all handlers indexed |
| `update_user_role`, `delete_user_role` locations | 2 | Confirm MCP line offsets |
| `resend_invite`, `switch_merchant_for_user_in_org` locations | 2 | Confirm MCP line offsets |
| `get_payment_filters` location | 1 | Confirm MCP line offsets |
| `list_roles_with_info` location | 2 | Found in user_role.rs and routes/user_role.rs |

## Implications for Replay Design

If the replay identity is defined as:

```
(request_id, full_concrete_read_signature)
```

where `full_concrete_read_signature` includes the method name AND all key arguments (e.g., `find_user_role(user_id, tenant_id, org_id, merchant_id, profile_id, version=V2)`), then:

**All 509 routes are free of same-request same-signature ambiguity at the read level.**

The remaining replay ambiguity concerns are:
1. **Cross-session resource lifetime** — the same key can return different values across different requests
2. **Write ordering** — multiple writes within a request need deterministic ordering
3. **Side-effect non-idempotency** — connector calls, email sends, etc.

For these, the replay identity needs:
```
(causal_scope_id, dependency_signature, local_ordinal_within_scope)
```
or equivalently:
```
(resource_key, resource_version_or_state_cursor)
```

But the specific concern of "same request, same read signature, different response due to in-request write" does **not** occur in Hyperswitch's current codebase.

## Files Modified

- This document: `docs/hyperswitch-cascade/09_FULL_ROUTE_SAME_REQUEST_VALIDATION.md`
