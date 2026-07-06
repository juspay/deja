# Hyperswitch Cascade MCP Rerun — Evidence and Query Guide

**Generated:** 2026-05-15  
**Local source of truth:** `<repo-root>/vendor/hyperswitch-fresh`  
**Verified commit:** `bc39324410031bec3e8c3d0ba924d81841c0c341`  
**Forbidden clone not used:** `<repo-root>/vendor/hyperswitch`

## 1. What changed in this rerun

This rerun used the Pi MCP gateway for `rust-brain` instead of assuming MCP was unavailable. `rust_brain_pg_query` is available and useful as a code-intelligence index. The robust workflow is:

1. use MCP Postgres queries to locate route functions, handler candidates, Redis/KV/SQL candidate functions;
2. distrust rust-brain snapshot line/path metadata because `source_files.git_hash` is `NULL` and at least one path is stale;
3. verify every important claim with local `rg --line-number --column` in `vendor/hyperswitch-fresh`.

Important MCP health result: Postgres and Qdrant have `219675` indexed items; Neo4j has `218834`, so this rerun does not rely on Neo4j graph completeness.

## 2. Queryable artifacts from this rerun

| Artifact | Purpose |
|---|---|
| `raw/mcp-rerun-queries.md` | Exact MCP calls used and observed MCP limitations. |
| `raw/mcp-rerun-counts.json` | Machine-readable route/domain/class/ambiguity counts. |
| `raw/route-catalog-normalized.mcp-rerun.tsv` | 509 route rows with stable `route_id`, local `route_registration`, and local `handler_location`. |
| `raw/dependency-callsite-counts.mcp-rerun.tsv` | Local-verified Redis/KV/SQL operation counts. |
| `raw/route-rg.mcp-rerun.txt` | Local Actix route registration grep with line/column. |
| `raw/redis-rg.mcp-rerun.txt` | Local direct Redis primitive grep with line/column. |
| `raw/kv-rg.mcp-rerun.txt` | Local `KvOperation` grep with line/column. |
| `raw/sql-rg.mcp-rerun.txt` | Local SQL/model call grep with line/column. |

This rerun was also stored in the rust-brain MCP artifact store as `hyperswitch-cascade-mcp-rerun-2026-05-15` with type `hyperswitch_cascade_rerun_manifest`.

Example queries:

```bash
cd <repo-root>
jq '.route_rows, .routes_requiring_disambiguation, .ambiguity_classes' \
  docs/hyperswitch-cascade/raw/mcp-rerun-counts.json

python3 - <<'PY'
import csv
p='docs/hyperswitch-cascade/raw/route-catalog-normalized.mcp-rerun.tsv'
for r in csv.DictReader(open(p), delimiter='\t'):
    if '/v2/payments' in r['full_path_template']:
        print(r['method'], r['full_path_template'], r['route_registration'], r['handler_location'])
PY

rg -n "pm_token_.*hyperswitch_cvc|payment_method_session|reverse_lookup|SCAN|HGet|HSetNx" \
  docs/hyperswitch-cascade docs/hyperswitch-cascade/raw
```

## 3. Counts that supersede older estimates

### Route and ambiguity coverage

| Metric | Count | Notes |
|---|---:|---|
| Inbound route rows cataloged | 509 | From `raw/agent-routes.md`, normalized in `route-catalog-normalized.mcp-rerun.tsv`. |
| Full per-route matrix rows | 509 | From `03_DEPENDENCY_CASCADES_FULL_RUST_BRAIN.md`. |
| Routes requiring a disambiguator beyond pure signature | 505 | All except 4 health routes marked `signature_only_safe`. |
| Routes that are not even FIFO-maybe-safe | 504 | Excludes 4 health routes and 1 `CARD_BIN_READ` route. |
| Routes with Redis/cache possible branch in summary | 503 | Conservative static scan of matrix dependency summaries. |
| Routes with DB/SQL/OLAP possible branch in summary | 502 | Conservative static scan of matrix dependency summaries. |

### Ambiguity class distribution

| Class | Routes |
|---|---:|
| `needs_db_snapshot_or_transaction_order` | 204 |
| `needs_resource_version_or_state` | 134 |
| `needs_causal_scope_plus_ordinal` | 79 |
| `needs_stateful_redis_emulation` | 57 |
| `needs_causal_scope_plus_ordinal + needs_stateful_redis_emulation` | 28 |
| `signature_only_safe` | 4 |
| `unsafe_without_driver_context` | 2 |
| `per_signature_fifo_maybe_ok` | 1 |

### Local-verified dependency operation counts

| Category | Count | Breakdown |
|---|---:|---|
| Direct production Redis read primitive call sites | 31 | `get_and_deserialize_key=22`, `get_hash_field_and_deserialize=4`, `get_hash_fields=4`, `hscan_and_deserialize=1`; excludes `redis_interface` definitions. |
| Direct app-level Redis read sites excluding generic wrappers | 27 | Excludes `storage_impl/src/redis/cache.rs` and `storage_impl/src/redis/kv_store.rs`. |
| KV-store Redis read call sites | 29 | `HGet=20`, `Get=2`, `Scan=7`; excludes `redis/kv_store` wrapper implementation. |
| KV-store Redis write call sites | 21 | `Hset=9`, `HSetNx=10`, `SetNx=2`. |
| Conservative SQL read/query call sites | 193 | `find_by=139`, `find_optional_by=13`, `list=29`, `filter_by=11`, direct `find(...)=1`. |
| Conservative SQL write call sites | 67 | `insert=4`, `update=35`, `delete=28`. |

## 4. Robustness notes about rust-brain MCP

MCP is now available and was used, but it should be treated as an index rather than an authority for source links:

- `rust_brain_pg_query` works and is the reliable MCP surface for this environment.
- `rust_brain_search_code` and `rust_brain_get_function` returned `workspace_id must not be empty`, so semantic/function-detail MCP endpoints were not used for final evidence.
- `source_files.git_hash` is `NULL`.
- A concrete stale-path example: MCP locates `retrieve_and_delete_cvc_from_payment_token` under `crates/router/src/core/payment_methods.rs:1892`; the local fresh checkout has the real function at `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/core/payment_methods/vault.rs:2235:1`.

Therefore, when querying this exercise later: use MCP to discover candidates, but cite the local-verified files in this document and the refreshed raw artifacts.

## 5. Representative high-risk flows with local source links

### A. `POST /v2/payments` create + confirm lifecycle

| Step | Evidence |
|---|---|
| Actix route maps `POST /v2/payments` to the handler | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/routes/app.rs:790:53` |
| Handler entry | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/routes/payments.rs:431:1` |
| Core create+confirm orchestration | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/core/payments.rs:3379:21` |
| Create-intent tracker insertion | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/core/payments/operations/payment_create_intent.rs:93:5` |
| Confirm-intent tracker read/insert path | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/core/payments/operations/payment_confirm_intent.rs:156:5` |
| Payment intent Redis `HSetNx` | `<repo-root>/vendor/hyperswitch-fresh/crates/storage_impl/src/payments/payment_intent.rs:199:57` |
| Payment intent Redis `HGet` | `<repo-root>/vendor/hyperswitch-fresh/crates/storage_impl/src/payments/payment_intent.rs:510:65` |
| Payment intent Redis `Hset` update | `<repo-root>/vendor/hyperswitch-fresh/crates/storage_impl/src/payments/payment_intent.rs:384:57` |
| Payment attempt Redis `HSetNx` | `<repo-root>/vendor/hyperswitch-fresh/crates/storage_impl/src/payments/payment_attempt.rs:992:34` |
| Payment attempt Redis `HGet` | `<repo-root>/vendor/hyperswitch-fresh/crates/storage_impl/src/payments/payment_attempt.rs:1655:66` |

Replay conclusion: this route is not safely replayable by `HGET/HSET signature -> response`. It needs either stateful Redis/DB replay or `(causal_scope_id, local_dependency_ordinal)` plus resource versions.

### B. Payment CVC token retrieve-and-delete

| Step | Evidence |
|---|---|
| Confirm branch may call CVC retrieval | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/core/payments/operations/payment_confirm_intent.rs:584:57` |
| CVC retrieve/delete function | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/core/payment_methods/vault.rs:2235:1` |
| Redis `GET` for encrypted CVC | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/core/payment_methods/vault.rs:2249:10` |
| Redis `DEL` for same key | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/core/payment_methods/vault.rs:2267:16` |

Replay conclusion: same key can validly return value, then `NotFound` after delete/expiry. This requires Redis key-existence/TTL state or a causal ordinal.

### C. User role delete then list remaining roles

| Step | Evidence |
|---|---|
| Core delete user role entry | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/core/user_role.rs:522:1` |
| Post-delete role list read | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/core/user_role.rs:746:10` |

Replay conclusion: the list result is a collection read after deletes in the same request. It needs a collection version/hash and local ordinal.

### D. Ephemeral/client-secret Redis hashes

| Step | Evidence |
|---|---|
| Ephemeral key hash read | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:150:18` |
| Ephemeral key delete | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:164:18` |
| Client secret hash read/delete family | `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:247:18` and `<repo-root>/vendor/hyperswitch-fresh/crates/router/src/db/ephemeral_key.rs:263:18` |

Replay conclusion: TTL + explicit delete means identical `HGET` signatures can map to value or missing state.

### E. Generic cache-aside miss/populate/hit

| Step | Evidence |
|---|---|
| Cache-aside helper | `<repo-root>/vendor/hyperswitch-fresh/crates/storage_impl/src/redis/cache.rs:306:1` |
| Redis read | `<repo-root>/vendor/hyperswitch-fresh/crates/storage_impl/src/redis/cache.rs:319:10` |
| Redis write after miss | `<repo-root>/vendor/hyperswitch-fresh/crates/storage_impl/src/redis/cache.rs:324:14` |

Replay conclusion: `GET key -> miss`, `SET key`, `GET key -> hit` has the same read signature but different correct responses.

## 6. What to query next

- Use `03_DEPENDENCY_CASCADES_FULL_RUST_BRAIN.md` for the 509-row route-to-class matrix.
- Use `06_DB_REDIS_AMBIGUITY_PATTERNS.md` for the most complete pattern taxonomy and source examples.
- Use this rerun's `route-catalog-normalized.mcp-rerun.tsv` when you need route IDs and exact local `file:line:column` links.
- Use `raw/mcp-rerun-queries.md` to repeat the MCP discovery.
- Through MCP, query `rust_brain_context_store` with `op=list_by_type`, `type=hyperswitch_cascade_rerun_manifest` to discover the stored rerun manifest.

Recommended replay identity from this rerun remains:

```text
(causal_scope_id, dependency_signature, local_sequence_in_scope)
```

or, when replay can emulate state:

```text
(resource_key, resource_version_or_state_cursor)
```

Pure signature-only lookup is safe only for the four health routes in this matrix; it is not a robust Hyperswitch-wide replay strategy.
