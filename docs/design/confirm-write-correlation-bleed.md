# confirm 404 — pinned mechanism: writes bleed the PREVIOUS request's correlation

**For OMP.** You were right to reject the "in-memory logic" call — it's a read-vs-write
store inconsistency, and here is the exact mechanism, definitively pinned.

## Symptom
`/payments/confirm` → 404 `HE_02 PaymentNotFound`. Scorecard 7/9 FAIL. confirm replays
its first connector round then returns early without the 2nd Stripe
`/v1/payment_intents` charge. Root: its DB `UPDATE payment_intent` (recorded seq 196)
returns `Ok([])` (0 rows) though the recording returned the row.

## What was ruled out (with evidence)
- **Not in-memory logic.** The UPDATE genuinely matches 0 rows.
- **Not a WHERE mismatch.** UPDATE 196 WHERE = `payment_id AND processor_merchant_id`
  (pure key). Read 158's row has that EXACT key; the read found it.
- **Not correlation-None.** Instrumented the pg-connection lease
  (`router/src/connection.rs::deja_route_replay_schema`): `DEJA_SP_NONE = 0` — every
  replay checkout had a correlation.
- **Not SET-fail / not-stuck.** `setfail = 0`, `stale = 0` — the `SET search_path`
  succeeded and `SHOW search_path` confirmed it contained the schema we set.

## The mechanism (per-schema `payment_intent` counts + drive order)
Drive order = workload order: `/connectors`(bb07) → `/payments`(bb23) → `confirm`(bb4f).

| request | own schema count | reality |
|---|---|---|
| `/connectors` (bb07) | **1** | received **/payments'** INSERT (bled) |
| `/payments` (bb23) | **0** | its own INSERT landed in bb07, not here |
| `confirm` (bb4f) | **1** | only the SEEDED row; confirm's UPDATE hit bb23 (empty) |
| `public` | 0 | — |

**Each request's WRITES use the IMMEDIATELY-PREVIOUS request's correlation.**
`/payments`' INSERT → `/connectors`' schema (bb07). `confirm`'s UPDATE → `/payments`'
schema (bb23, empty) → `[]`, leaving confirm's own seeded row (bb4f) untouched. **Reads**
use the correct (current) correlation — they run inside the request's active span, so
they find the seeded rows. `DEJA_SP_NONE=0`/`stale=0` were true-but-misleading: the
correlation wasn't missing, it was the *wrong* (previous) one, and the checks validated
against that wrong value.

## Root cause
`deja_context::current_correlation_id()` is a **thread-local** set by
`DejaCorrelationLayer` on span-enter. The write path runs in a context (a spawned task /
post-`.await` off the request span) where that thread-local still holds the PRIOR
request's id (kernel is serial → always the immediately-previous request). So the
write's `SET search_path` targets the previous correlation's schema.

## Fix direction (shared with parallel replay, #37)
The correlation must be correct in the write execution context:
1. Clear the thread-local on span-exit (stale → None) AND propagate the id into the
   write context.
2. Make the correlation a **tokio task-local** (survives `.await`; carried on spawn).
3. Instrument hyperswitch's write-path spawns with the request span.

The same propagation gap would make parallel per-correlation replay (#37) bleed
correlations across concurrent requests — so fixing it unblocks both.

## Fix attempts + finding (for OMP)

**Attempt: capture-once query-path route (OMP's requested approach).** Implemented
faithfully: capture the correlation ONCE at DB-boundary build via a new unconditional
`deja::current_correlation_id()`, stamp `QuerySpec.correlation_id` (event resolves
`explicit.or_else(ambient)` at deja-record:1272, so event+occurrence+schema share ONE
value), route `search_path` at query-time from the captured id on the same conn, fail
loud (panic) on SET error / None-corr. Grounded first by a read+adversarial-review
workflow.

**Proof result:** routing WORKS — `DEJA_ROUTE` logs show every write routes to its OWN
correlation's schema (no more previous-request bleed), no panics. **BUT the scorecard
REGRESSED 7/9 → 4/9**, reproducibly (same 4/9 with the earlier no-stamp variant). The
query-time SET perturbs **live reads** that were already correct at checkout: `/api_keys`
seq 84 (`merchant_key_store` find, `execute_shadow` live read) returns a DIFFERENT
merchant encryption key on replay (`inner:[123…]` vs recorded `inner:[180…]`) → auth
fails → 401 cascade.

**Two structural problems with DB-layer `search_path` routing:**
1. **Reads perturbed.** The query-time SET re-routes live reads that the checkout route
   already routed correctly; with both routes present they can disagree.
2. **28 non-macro DB sites** (`sample_data`, `refund`, `user_role`, `health_check`, …)
   never go through the macro, so the checkout route **cannot** be removed → two routes
   must coexist. OMP's "route once, remove checkout" is impossible without instrumenting
   all 28.

**Reverted to the 7/9 baseline** (checkout-route only). Conclusion (matches OMP): DB-layer
schema routing is the wrong layer. The root cause is **correlation propagation** — writes
run off the request's correlation span, so the ambient is stale at their checkout.

**Proposed fix:** make the write path carry the request's correlation (instrument the
span-losing write spawn, or a robust task-local), so the SINGLE existing checkout route
routes ALL DB traffic — macro AND the 28 non-macro sites — correctly. No per-query DB
changes, no double-route, no 28-site gap. (Investigation in progress: locating where
confirm's payment_intent UPDATE loses the span.)

## Note
Temporary instrumentation (`DEJA_SP_*` markers in `connection.rs`, `PICNT`/log-dump in
`lifecycle/mod.rs`) is in the tree to verify the fix; revert once green. The macro's
`$conn` param is now unused (kept as harmless plumbing pending the propagation fix).

## Post-9/9 sweep (OMP audit — do BEFORE PR review, AFTER the correctness gate)

End state = a **reviewable vendor PR**: the vendor branch is a THIN, boring Hyperswitch
integration surface only (boundary instrumentation/wrappers, event-creation handoff,
schema/version fields, buffering/push wiring to Kafka/object store, Superposition switch,
and the minimal replay DB routing hook). Library crates own all generic logic (event
model/codec, buffering, replay matching, seed planning/materialization, reports, storage
backends). No broad payment-core edits, no query-time routing, no temporary probes as
final code.

## HANDOFF — current state (NOT final; correctness gate still open at 8/9)

Per OMP: stopped here rather than auto-starting the replay-seam investigation. No
query-time routing, no broad payment-core edits. Temporary probes remain in-tree (must be
removed before any final PR — see sweep list below) but the state is NOT relabeled final.

**Implemented + VALIDATED (minimal central store/request-id routing):**
- `deja`: `current_correlation_id()`, `replay_search_path_sql_for(&str)` (library owns SQL).
- `storage_impl`: `DatabaseStore::get_request_id()` (default `None`) + `RouterStore` /
  `KVRouterStore` overrides.
- `router/db.rs`: KV **inner** `router_store` request_id propagation (`add_request_id`,
  `#[cfg(feature="kv_store")]`).
- `router/app.rs`: stamp `accounts_store` + `global_store` (+ `RequestIdStore` supertrait on
  `GlobalStorageInterface`/`AccountsStorageInterface`). **This fixed org/accounts** — stamped
  request count went 4 → 6; those requests route to their own schemas.
- `router/connection.rs`: `deja_route_replay_schema` reads `store.get_request_id()`
  (reliable, request-scoped) instead of the bled ambient `current_correlation_id()`.

Result: 8/9, confirm-only, **no regression**; all changes compile under `deja,v1`.

**Residual (handed back — beyond the checkout hook): the durable CONTRADICTION.**
For confirm's `payment_intent` update during replay (replay-gated probes):
- `pi_update` (storage method, payment_intent.rs:243): scheme=**PostgresOnly**, store
  correctly stamped — `outer_req == inner_req == <confirm corr>` (e.g. `a233`).
- Checkout probe: **0** routed checkouts for confirm's corr (`a233`) or /payments (`a212`);
  only the other 6 requests check out.
- Query-time probe: confirm's update **resolves to `public`**; its read resolves to a
  **reused** `deja_<other-corr>` connection.
- => confirm's payment_intent query executes on a connection the checkout hook never routed
  — the **deja replay-seam execute-shadow and/or async connection reuse** runs the query on
  a connection other than the store method's `pg_connection_write` one. NOT a store-stamping
  gap.

**Ruled out durably:** async drainer (it's PostgresOnly, not RedisKv), `None`/unstamped
store (store is `a233`), accounts/global stamping gap (fixed).

**Next options (for OMP; none is minimal-central-routing):**
1. Investigate `deja::db::record_query_async` execute-shadow connection handling — does the
   shadow/replay query run on the store method's routed `pg_connection_write` conn or a
   re-acquired/reused one? (Where the contradiction lives.)
2. Ensure the replay/shadow query executes on the routed connection.
3. A different per-correlation isolation mechanism for the replay-execute path.

## Post-9/9 sweep (OMP audit — do BEFORE PR review, AFTER the correctness gate)

Sweep list (remove/finalize once confirm is 9/9 with durable evidence):
1. `connection.rs` — remove the durable schema-probe writer (checkout `SHOW search_path` +
   `/harness-state/deja-schema-probe.jsonl` append). Keep only the minimal, permanent
   replay DB routing hook (the correctness fix), boring + self-explanatory.
2. `diesel_models/query/generics.rs` — remove the per-op schema-probe SQL (`to_regclass`
   query) + file writer; remove the now-unused `$conn` plumbing if the final fix doesn't
   need it.
3. `deja-orchestrator/lifecycle/mod.rs` — remove the PIN/PROBE block (docker-logs dump,
   `DEJA_SP*`/`DEJA_ROUTE`/`DEJA_CHK` counters, `PICNT` psql query).
4. `deja-record` `current_span_correlation` probe — remove if unused by the final fix.
5. Exclude demo-only **EU settlement** code from the vendor PR unless explicitly required.
6. Recheck `DEJA_RECORDING_ARCHITECTURE.md` transport section against the actual
   Kafka/Vector/S3 code.
7. Library cleanup: eliminate the `method_name.contains("insert")` seed heuristic
   (`deja-record/src/replay.rs` create-mask) — make it explicit event metadata instead.
8. Confirm the vendor↔library split: vendor thin, library owns generic replay/seed/report.
