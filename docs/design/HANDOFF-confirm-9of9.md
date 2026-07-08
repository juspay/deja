# HANDOFF — confirm 404 → 9/9 (correctness gate)

Compact handoff (context at 100%). Full narrative: `confirm-write-correlation-bleed.md`.

## ✅ GREEN — CYCLE 27: `pass: true`, matched_correlations 9/9

Self-check VERDICT: **self PASSES**. Summary: `value_divergences:0`, `side_effect_divergences:0`,
`order_nondeterminism_warnings:0` (div 2 non-deterministic — absent this run), `idempotent_delete_warnings:1`
(div 1 redis delete_key demoted by Rule B). All 9 HTTP requests 200 MATCH.

Path from 4/9 → GREEN (all library/harness fixes + the storage_impl seam patch; strict, tested):
1. storage_impl seam routing patch (real `pg_connection_write`).
2. bytea seed fix (`lifecycle/mod.rs`: Encryption `{inner:[u8]}` → `'\x'::bytea`) → auth + confirm.
3. `build_seed_plan` UPDATE-preimage reorder (`deja-record/replay.rs`) → /connectors 500.
4. Rule A (order-nondeterminism) + Rule B (idempotent redis delete) report/classifier demotions
   (`divergence/mod.rs` + `ledger.rs`) — strict evidence guards; **40 lib tests pass**.

**#40 PROBE SWEEP DONE + VERIFIED** (cycle 28 still pass=true 9/9): removed router/connection.rs +
storage_impl/utils.rs checkout probes (kept the SET routing), generics.rs per-op to_regclass probe +
`$conn` macro param + 10 call-site args, payment_intent.rs pi_update diagnostic, lifecycle PIN/PROBE
block, deja-record `current_span_correlation` dead helper. KEPT all permanent fixes + Rule A/B. Library
warning-free, vendor router `deja,v1` clean, 108 unit tests pass, `deja-schema-probe.jsonl` no longer
written. **Still NOT PR-ready** — #41 (vendor-thin split + audit items) remains.

---

## LATEST — RULE A (order-nondeterminism classifier) → 8/9; confirm div 2 DEMOTED

Implemented OMP's report/classifier policy (NO runtime serialization): a concurrent same-row
UPDATE-RETURNING interleaving is demoted from a blocking value divergence to a non-blocking
`order_nondeterminism_warning`. STRICT guards (all required): run-level HTTP 9/9; `db` UPDATE with a
RETURNING row; same correlation+table+primary-key+final-row (identical recorded result row);
overlapping wall-clock windows; and the FINAL/last write (max global_sequence) MATCHED (reproduces
the recorded final state). If the final write diverges (lost update), NOTHING is demoted.

Files (library only): `crates/deja-orchestrator/src/divergence/mod.rs`
(`order_nondeterministic_demotions` helper; `RunArtifacts.events`; `Summary.order_nondeterminism_warnings`;
demotion in `detect()`; non-blocking verdict line; `load_artifacts` loads events) +
`divergence/ledger.rs` (mirrors: demoted origin row → `blocking:false`, kind `order_nondeterministic`).
Tests: 4 focused Rule A tests (positive + lost-update/non-overlap/HTTP-dirty guards); **all 34
deja-orchestrator lib tests pass** (no regression).

**Cycle 26 (end-to-end):** matched_correlations **8/9**; summary `value_divergences:1`,
`order_nondeterminism_warnings:1`, `side_effect_divergences:1`. confirm's `payment_attempt` div 2 →
`OrderNondeterministicWarning` (demoted, confirm PASSES). Verdict `pass:false` reason
"1 value divergence(s); 1 order-nondeterminism warning(s) (non-blocking)".

**Remaining (gate RED): div 1 — redis `delete_key` on `/connectors` (`KeyDeleted`→`KeyNotDeleted`).**
PARKED per OMP (benign idempotent-delete); Rule B (redis) NOT implemented — awaiting OMP's policy
decision. This is the sole blocking divergence.

---

## CYCLE 25: seed UPDATE-preimage fix → **all 9 HTTP 200 MATCH** (connectors fixed)

`/connectors` 500 was a **seed/materialization UPDATE-preimage gap** (NOT routing, NOT the same
class as the auth bytea gap). `/connectors` does `UPDATE merchant_account SET modified_at=… WHERE
merchant_id=…` with **no prior SELECT**; that event's `read_set == write_set` (same `{table}:{sql}`).
`build_seed_plan` marked the write **before** seeding the read, so the self-referential pre-image was
masked as "already written" → `merchant_account` never materialized into `deja_<connectors>` → the
replayed UPDATE hit an empty table → `Ok([])` (0 rows, `err:null`) → handler 500 before the MCA INSERT
(replay stopped at 3 db ops vs 14 recorded).

**Fix (library only):** `crates/deja-record/src/replay.rs::build_seed_plan` — seed each event's reads
**before** marking its writes, so an UPDATE/DELETE's self-referential pre-image seeds; `created_tables`
still masks reads of a table the correlation INSERTed (INSERT-then-UPDATE unaffected). + 2 unit tests
(`seed_plan_seeds_self_referential_update_preimage`, `seed_plan_skips_update_of_own_inserted_table`);
all 8 `seed_plan` tests pass.

**Cycle 25 scorecard:** all 9 requests **✓ 200 MATCH** (signup, signin, organization, accounts,
api_keys, accounts/{id}, **connectors 31/31**, payments 29/29, confirm 52/52). Summary:
`total_correlations:9, matched_correlations:7, http_status_mismatches:0, http_body_mismatches:0,
side_effect_divergences:2, value_divergences:2`. `verdict.pass:false`, reason "2 value divergence(s)".

**Remaining: 2 side-effect VALUE divergences (gate RED). Both classified (no masking applied):**

DIV 2 — `db generic_update_with_results` payment_attempt, corr confirm, seq 202 → **ASYNC ORDERING
(concurrent same-row write race), correctness-neutral.** Evidence: seq 202 (payload net_amount only,
`status:None`) and seq 204 (payload `status:Some(Charged)`, `connector_transaction_id:Some(pi_3Tof…)`)
are BOTH UPDATE…RETURNING to the SAME row (`attempt_id pay_…_1`) at the SAME call site
(`generics.rs:344`), and their wall-clock windows OVERLAP (202 `449602064→452725230`, 204
`449669903→451986130`). In record, seq 204 commits first → seq 202's RETURNING sees `charged`; in
replay, seq 202 commits first → sees `pending`. seq 204 is `matched` (the charge IS applied); the
final payment_attempt row is `charged` in BOTH. Only seq 202's intermediate RETURNING diverges by
interleaving. NOT substitution (both http_outgoing `matched` rank_2; recorded charge present), NOT
codec loss (seq 204 carries the charge, matched), NOT seed/update gap (row exists + reaches charged).
→ Fix options (OMP's call): deterministic serialization of concurrent same-row writes on replay;
or event-identity/classifier that treats an order-dependent concurrent-write RETURNING as non-blocking;
or accept as concurrency nondeterminism. Relates to #37 (parallel replay) / event-identity.

DIV 1 — `redis delete_key` (cache invalidation), corr /connectors → benign idempotent-delete /
cache-seed-gap (`KeyDeleted`→`KeyNotDeleted`; end-state identical). Deferred per OMP until DIV 2
resolved.

---

## CYCLE 24: bytea seed fix → 7/9, **confirm PASSES** (original blocker fixed)

Two fixes together took it 4/9 → **7/9**:
1. **storage_impl seam patch** (routing at the real seam — preserved; see below).
2. **bytea seed fix** (this cycle): `seed_db`'s `sql_literal` rendered encrypted `bytea` columns
   (the `Encryption` serde shape `{"inner":[<u8>…]}`, e.g. `merchant_key_store.key`) as JSON text,
   which psql can't cast to `bytea` → the INSERT failed and (under `ON_ERROR_STOP=0`) the row was
   silently skipped → auth reads missed → 401 cascade. Fix: render that shape as `'\x<hex>'::bytea`.

**Scorecard (cycle 24):** signup ✓ signin ✓ organization ✓ accounts ✓ **api_keys ✓** accounts/{id} ✓
`/connectors` ✗ **500 vs 200** **payments ✓** **confirm ✓**. → **7/9**, confirm (the original 404
blocker) now MATCHES.

**Materialization proof (durable):** /api_keys (`d336`) — `merchant_key_store` AND `merchant_account`
reads resolve to `deja_…d336…` (its own schema); run logged `seed_db merchant_key_store (1 row(s))`,
`merchant_account (1 row(s))`, `merchant_connector_account (1 row(s))`; /api_keys = 200 MATCH ⇒ the
bytea-fixed rows materialized in the corr schema.

**Files changed (this cycle, library/harness only):**
- `crates/deja-orchestrator/src/lifecycle/mod.rs`: `sql_literal` bytea branch + `bytea_from_encryption`
  helper; 2 unit tests (`seed_db_renders_encrypted_bytea_key_as_hex_literal`,
  `sql_literal_bytea_only_for_inner_byte_array`) — both PASS.

**Remaining: `/connectors` 500** — NOT confirmed same seed class (0 psql/seed_db errors; reads route
to `deja_d352`; http-diff body not captured). Likely MCA-write divergence or replay-matching. Held per
OMP (separate scoped investigation; not cleanup/payment-core/query-time).

**Next action:** scoped `/connectors` 500 diagnosis — capture the replay response body/server log for
`d352`, check whether an MCA create/read (e.g. `connector_account_details` jsonb/bytea) fails or a
replay-match diverges; if it IS an encrypted/bytea seed column of a different shape, extend the seed
literal policy; else treat as a distinct replay-matching/app-divergence bug.

---

## The seam fix (the real root cause)

There are **THREE** `pg_connection_write` functions in the vendor:
- `router/src/connection.rs` — HAS the deja routing hook (what I fixed first — WRONG copy for this path).
- **`storage_impl/src/utils.rs`** — plain `pool.get()`, NO hook. **This is the copy the payment
  store methods use** (`payment_intent.rs:58` imports `pg_connection_write` from `crate::utils`).
- `storage_impl/src/connection.rs` — olap copy, also no hook.

So confirm's `payment_intent` UPDATE (KVRouterStore::update_payment_intent → PostgresOnly →
RouterStore::update_payment_intent @ payment_intent.rs:774 → `pg_connection_write` = `crate::utils`)
acquired a connection **with no SET search_path and no checkout log** — hence the durable
contradiction: storage method truthfully logs PostgresOnly + correctly-stamped store, yet 0 routed
checkouts and the query resolves to `public`/a reused schema → `[]` → 404.

**Fix (bounded central, connection-acquisition routing — NOT query-time macro routing, NO
payment-core edits):** add the deja routing hook to the REAL seam, keyed off `store.get_request_id()`:
- `storage_impl/src/utils.rs` — added `deja_route_replay_schema<T: DatabaseStore>(conn, label, store)`
  (cfg `deja`): if replay active + `store.get_request_id()` is Some, run
  `deja::replay_search_path_sql_for(corr)` on the leased conn; called from all 4 `pg_connection_*`.
- `storage_impl/src/connection.rs` (olap copy) — calls `crate::utils::deja_route_replay_schema`.

## Changed files (correctness work)

Library:
- `crates/deja/src/lib.rs`: `current_correlation_id()`, `replay_search_path_sql_for(&str)`.

Vendor — permanent:
- `storage_impl/src/utils.rs` + `connection.rs`: **the seam fix** (routing hook).
- `storage_impl/src/database/store.rs`: `DatabaseStore::get_request_id()` (default None).
- `storage_impl/src/lib.rs` (RouterStore) + `kv_router_store.rs` (KVRouterStore): `get_request_id` overrides.
- `storage_impl/Cargo.toml`: optional `deja` dep + `deja` feature. `router/Cargo.toml`: `deja` feature adds `storage_impl/deja`.
- `router/src/db.rs`: KV inner-store request_id propagation + `RequestIdStore` supertrait on
  Global/AccountsStorageInterface. `router/src/routes/app.rs`: stamp `accounts_store` + `global_store`.
- `router/src/connection.rs`: the router-copy hook (covers router-direct DB calls; keep or consolidate at cleanup).

Vendor — TEMPORARY probes (task #40 sweep BEFORE PR; do not ship):
- `diesel_models/src/query/generics.rs`: per-op resolved-schema probe (`to_regclass`) + `$conn` plumbing.
- `storage_impl/src/payments/payment_intent.rs`: `pi_update` diagnostic (DEJA_MODE=replay-gated).
- `storage_impl/src/utils.rs` + `router/src/connection.rs`: the `checkout*`/`store_corr` probe appends.
- `deja-orchestrator/src/lifecycle/mod.rs`: PIN block (DEJA_SP*/DEJA_ROUTE/DEJA_CHK/PICNT).

## Durable probe artifact

`demo/harness-state/<latest-tag>/deja-schema-probe.jsonl` — per-op JSON lines:
- generics.rs: `{op,table,corr,resolved_schema,search_path}` (which schema each op resolves to).
- utils/connection: `{phase:"checkout_si"|"checkout",label,store_corr,...}`.
- payment_intent: `{diag:"pi_update",payment_id,scheme,outer_req,inner_req}`.
Verify confirm by: confirm's `payment_intent` ops `resolved_schema` == `deja_<confirm-corr>` (not public/other).

## Cycle 23 (verification of the seam fix)

- Command: `set -a; source demo/.env; set +a; bash demo/run-self-check.sh` (needs STRIPE_API_KEY).
- Compile: clean (`router --features deja,v1`).
- Result: **IN PROGRESS** (build + record + replay running at start of this handoff).

## Cycle 23 RESULT (CRITICAL — read this)

**4/9 — REGRESSION from 8/9.** The seam fix WORKS (routing is now correct), but it **exposes
an incomplete-per-correlation-seeding problem**:
- `/api_keys`, `/accounts/{id}`, `/connectors`, `/payments`, `/confirm` all now **401** (auth).
- Mechanism: with the storage seam now routing EVERY read to `deja_<corr>, public`, auth reads
  (e.g. `merchant_key_store`) resolve to the correlation's **cloned-but-empty** table (the clone
  creates ALL public tables empty; Postgres does NOT fall through to `public` for data once the
  table exists in the first schema) → read misses → 401 → cascade. confirm 401s at AUTH, before
  reaching payment_intent (so no payment_intent probe lines this run).
- This is the SAME failure as the earlier query-time-SET attempt (cycles 11b/12) → confirms it's
  **seeding completeness**, not the routing mechanism. The 8/9 state hid it because, with NO
  routing, all reads hit `public`, which holds the record phase's data.

**So the routing fix is architecturally correct but must be paired with complete per-correlation
seeding of shared/auth reference tables** (`merchant_key_store`, `merchant_account`,
`business_profile`, `configs`, `merchant_connector_account`, …). This is a HARNESS/LIBRARY change
(`deja-orchestrator` materialize / `deja::build_seed_plan` / the ambient template), NOT vendor.

### CLASSIFICATION v2 — PRODUCER→READER per instance (OMP refinement; supersedes the write-count table below)

Whole-recording write counts are NOT enough: a table written during setup is still *reference* for
later corrs. Classified each 401 read by (reader corr/path, table, producer corr, does-reader-mutate):

| read instance | reader | producer corr | reader mutates? | bucket |
|---|---|---|---|---|
| `merchant_key_store` (merch_753c) | `4d2c` /api_keys | `4d0b` /accounts (earlier SETUP) | no | **reference** |
| `merchant_account` | `4d2c` /api_keys, `4d72` /payments | `4d0b` /accounts, `4d59` /connectors (SETUP) | no | **reference** |
| `payment_intent` | `4da0` confirm | `4d72` /payments (different corr) | **yes (2 updates)** | **per-corr seeded state** |

- **Reference** reads (auth): produced by an EARLIER setup corr, reader does not mutate. `merchant_account`
  is UPDATED by /connectors (`4d59`) mid-sequence, so a naive "read raw `public`" is unsafe for a
  reader that saw the pre-update value → the reference read needs the **reader's recorded value**
  materialized into its schema (ambient-copy), not raw public.
- **Per-corr state**: cross-corr producer but the reader mutates it → must be isolated + seeded with
  the reader's recorded read (already the read-set seed's job).

### ROOT CAUSE of the 4/9 regression (evidence: `lifecycle/mod.rs` seed logic + durable probe)

DB seeding is gated by `DEJA_SEED_DB` and WAS ON in cycle 23 (per-corr schemas created + routed;
`create_db_schema` @ mod.rs:1187 clones ALL public tables EMPTY via `LIKE public`; `seed_db` @ :1101
seeds read-set rows by-PK-from-result; `build_seed_plan(events, corr).with_ambient(&ambient)` @ :1045).
The regression is a **seed-completeness gap**: the routing now correctly sends every read to
`deja_<corr>`, but the **reference rows produced by setup corrs are not materialized into later
corrs' schemas** (`merchant_key_store` absent from `deja_<api_keys>`), so those reads hit the empty
clone → 401. The 8/9 state hid this: no routing → reads fell through to `public`, which holds the
setup rows.

### MINIMAL FIX (harness/library, bounded — NOT seed-all, NOT vendor/query-time/payment-core)

Materialize each corr's **read-set reference rows** into its schema (ambient-copy of setup-produced
rows the corr actually reads). This is bounded to the corr's actual read-set, not all tables.
- **First diagnose (exact next step):** (a) inspect what `ambient` (mod.rs ~:1040) currently
  contains and why `merchant_key_store`/`merchant_account` aren't in it; (b) check whether
  `build_seed_plan` emits a `db` seed entry for /api_keys' `merchant_key_store` read — if present but
  `seed_db` failed (row reconstruction of the encrypted `key` column?), it's a `seed_db` bug; if
  absent, it's a `build_seed_plan`/ambient gap.
- **Fix location:** `deja::build_seed_plan` / the `with_ambient` construction + `seed_db` in
  `deja-orchestrator/src/lifecycle/mod.rs`. Ensure setup-produced reference rows a corr reads land
  in that corr's schema (ambient), and per-corr state (payment_intent) stays the read-set seed.
- **Preserve** the `storage_impl` seam patch (routing is correct and necessary).

### CLASSIFICATION v1 (whole-recording write counts — kept for context; superseded by v2 above)

Counted read (find/count/filter) vs write (insert/update/delete) ops per table across the WHOLE
recording:
- `merchant_key_store` r=3 w=2 · `merchant_account` r=3 w=3 · `business_profile` r=4 w=3 ·
  `configs` r=10 w=8 · `merchant_connector_account` r=4 w=1 — **all WRITTEN, by the SETUP requests**
  (/organization, /accounts, /api_keys, /connectors), then read-only by later requests.
- `payment_intent` r=1 w=4 · `payment_attempt` r=1 w=5 — **written by the PAYMENT requests**
  (/payments, /confirm).

So there is no global "reference vs state" split — the right axis is the **per-correlation
write-set**: a table is written by one request and only read by others. The 401s happen because the
current clone makes EVERY table exist (empty) in every corr's schema, so a corr's read of a table it
did NOT write resolves to its empty clone instead of the shared `public` value.

### MINIMAL FIX (write-set isolation — bounded, harness/library only, no vendor/query-time/payment-core)

Per correlation, **clone + seed ONLY the tables that correlation WRITES** (has recorded
insert/update/delete ops for). Tables it only READS are **not cloned** → their reads fall through to
`public`, which holds the record-phase shared data.
- Setup requests isolate their writes (`merchant_key_store` → `deja_<accounts>`), and later requests
  read `merchant_key_store` from `public` (record value) → **auth works**.
- Payment requests isolate `payment_intent`/`payment_attempt` per test case → confirm's write no
  longer leaks; its read is seeded in its own schema.

**Why correct (independent-test-case model, [[project_correlation_test_case_isolation]]):** each
request is seeded from its OWN recording; reads of written tables get seeded values in the isolated
schema; reads of non-written tables get the shared record value from `public`. Record writes only
"leak" through `public` for read-only tables (never mutated per-case → no stale leak). Payment
tables ARE written per-case → cloned → never read from `public`.

**Exact files:**
- `deja-orchestrator/src/lifecycle/mod.rs`: `create_db_schema` (clone only the write-set tables,
  not all of `public`), `seed_db` (seed only write-set tables), the `materialize_*` caller (derive
  the per-corr write-set and pass it in).
- Write-set derivation: recorded events with `boundary=="db"` and
  `method_name` matching `insert|update|delete`, grouped by `args.table` per `correlation_id`
  (in `materialize`, or in `deja::build_seed_plan`).

**Preserve:** the `storage_impl` seam patch — routing is correct and necessary; do not revert.

**Alternative (heavier, OMP cautioned against):** complete read-set seeding — seed every read,
including setup tables, into each corr's clone. Correct but duplicates shared data N× and is the
"seed everything" path.

### Bounded experiment to validate the classification BEFORE the principled change
Temp denylist in `create_db_schema`: skip cloning the 5 setup tables above → they read `public`.
If the scorecard improves (auth passes, confirm reaches payment_intent) and the durable probe shows
setup reads resolving to `public` while `payment_intent` stays `deja_<corr>`, the write-set policy
is validated → then implement the principled per-corr write-set version.

## Current status + next action

- Pre-cycle-23: **8/9** (confirm-only, no regression). Cycle 23 tests the seam fix.
- NEXT: read cycle 23 scorecard + the durable probe. If confirm's `payment_intent` ops now resolve to
  `deja_<confirm>` and scorecard = **9/9** → correctness gate PASSED → then (and only then) sweep
  temporary probes (#40) + vendor↔library split (#41). If still failing → inspect the durable probe
  (does confirm now checkout at the storage seam? resolved schema?), stay on minimal central routing,
  no query-time routing / no payment-core / no probe cleanup before green.
