# Genericity + real seeding — locked plan

Status: **active** (2026-06-30). Two parallel tracks. Grounded in a 5-way code audit;
every claim below is tagged **DECIDED** (agreed direction), **BUILT** (in code, file:line),
or **GAP** (designed / gated-off / not implemented). Do not blur these.

## Goal

1. **Small, generic vendor footprint.** The library provides the primitives
   (Execute/Substitute routing + result codecs). A vendor boundary declares only
   what is genuinely site-specific. All the boilerplate is a library default.
2. **Complete, self-describing recording.** The tape carries everything a
   downstream consumer needs so the ETL/aggregation pipeline can evolve separately
   — no silent loss, stable identity/ordering, honest metadata.
3. **Real, isolated, observable seeding.** On replay we *rebuild the store* and
   execute stateful reads against it — not answer from a lookup table. Each test
   case (correlation) is isolated so parallel cases can't collide and RMW can't
   double-apply. The replay surfaces what was seeded and what ran.

## The two keystones

- **Genericity** collapses onto a `ReplayCodec` selected per return type → folds the
  DB seam into the generic `dispatch`, retires the `replay_ok`/`replay_with` flags,
  and lets the `id`/`time`/`http`/`db`/`redis` presets become one boundary macro =
  `replay_strategy` knob + codec.
- **Correctness** collapses onto **per-correlation store isolation** → real seeding,
  safe redis-Execute (no RMW double-apply), and observable seeding all fall out of it.

## Locked decisions (2026-06-30)

| Decision | Choice |
|---|---|
| Track sequencing | **Both tracks in parallel** |
| Real-seed scope | Stateful stores (**redis + db**) seed-and-**Execute**; **egress/entropy/clock stay Substitute** (can't re-run) |
| DB isolation | **Schema-per-correlation + per-checkout `SET search_path`** (revised 2026-06-30 after the recon below; the original "template-DB-clone" is infeasible — see finding). Localized router edit at the connection-lease points; preserves parallelism. |
| Redis isolation | **Per-correlation key-prefix namespace** (`{corr}:…`); logical-DB caps at 16, instance-per-case too heavy |
| Codec selection | **Explicit `replay_codec = Codec` marker + kit-level codec defaults** (stable toolchain, no autoref magic). `result_codec = Codec` is an accepted alias; legacy `recon = Codec` remains accepted. Blanket `SerdeCodec` is the no-annotation default. |

## Grounded current state (the honest baseline)

**Genericity**
- BUILT: 38 redis sites are raw `#[deja::boundary(...)]`; ~4 of ~7 attributes per site
  are pure boilerplate (`boundary`, `component`=`module_path!()` default,
  `correlation=None` default, `replay_strategy=Substitute` default). `redis_interface/src/commands.rs`.
- BUILT: `#[deja::redis]` exists but is a near no-op (`Preset::None`) — only sets
  `boundary="redis"`. `deja-derive/src/lib.rs:90-95`, `instrument.rs:31-45`.
- GAP: no `Preset::Redis`/`Preset::Db`; no codec abstraction (3 ad-hoc flags
  `replay`/`replay_ok`/`replay_with`, `instrument.rs:240-345`).
- BUILT/GAP: the `dispatch` seam (`deja-record/src/lib.rs:2270-2470`) is already
  general enough to host the DB codec via its `extract`/`reconstruct` closures, but
  the DB path is a **separate hand-written seam** `record_query_async`
  (`deja/src/lib.rs:480-642`) because of its `recover_err` NotFound flow.

**Seeding / isolation** (see `project_seeding_isolation_state` memory)
- BUILT: isolation is **per-RUN only** — `isolated_for_replay`
  (`deja-orchestrator/src/lifecycle/mod.rs:137`) → own compose project
  `deja-run-<short>` + freshly migrated pg + empty redis. All correlations in a run
  **share one redis + one pg**, raw keys.
- BUILT: DB seeding = `INSERT … ON CONFLICT DO UPDATE` (`lifecycle:1011,1069`),
  **GATED OFF** behind `DEJA_SEED_DB` (`:931`). Redis = `FLUSHALL` once/run
  (`:821`) + upsert (`:850`).
- GAP: **template-DB-clone is LOCKED but UNBUILT** — no `CREATE DATABASE … TEMPLATE`
  anywhere. The only "template" in code is `AmbientTemplate` (seed-value TSV, `:1171`).
- GAP: default redis ops are Substitute → reads answered from the **in-memory lookup
  table** (`replay.rs:1296-1302`, `lookup/mod.rs:91`), seeded redis untouched.
- GAP: observability = one `inconclusive_seed_gaps` counter + stderr
  (`divergence/mod.rs:141`); no seed manifest, no store-vs-tape, no query log.
- HONEST NOTE: the **eu-overcharge demo catch fires via the redis `eu_settlement_read`
  Substitute → fail-stop, NOT via DB/template-clone seeding**. We built the seeding
  *plumbing*; we have not validated real per-correlation seeding end to end.

**Recording**
- BUILT: `SemanticEvent` v2 (`deja-record/src/lib.rs:75-200`) → `RecordingHook` →
  `AsyncRecordWriter` → `HyperswitchKafkaRecordSink` (envelope `deja_artifact_record`
  v2, partition key `correlation_id`) → Vector → MinIO/S3.
- GAP (P0): default `FailOpen` buffer **silently drops event payloads** on
  backpressure (`writer.rs:485-495`); writer permanently dies after 8 consecutive
  sink errors (`:26,570-579`). `global_sequence` is **per-process not per-session** →
  `(session_id, global_sequence)` collides across instances.
- GAP (P2): `recon` field **always lies `Lossless`** (`:138`); `raw_draw` inert;
  `request`/`response` duplicate `args`/`result`; three uncoordinated `version`
  integers; `value_digest` is non-crypto FNV; Kafka leg uncompressed; secrets
  cleartext (masking descoped — add a *seam* only).

## Plan

### Track G — genericity

- **G1 (#30) Preset kits.** Add `Preset::Redis`/`Preset::Db` wired like `Time`/`Id`
  (default boundary + kind + `replay_strategy` + codec). A redis GET site:
  `#[deja::boundary(replay_strategy=Substitute, boundary="redis", component=…, operation="get_key", replay_ok, correlation=None, state_read=…, args=…)]`
  → `#[deja::redis(state_read = key.tenant_aware_key(self))]` (kit supplies the rest;
  `operation` kept only where it pins the identity hash). Verify identity hashes unchanged.
- **G2 (#31) `ReplayCodec`.** Trait `{ capture(&V)->(Value,bool); reconstruct(Value)->Option<V> }`
  (maps onto dispatch's `extract`/`reconstruct`). Ship `SerdeCodec<R>` blanket
  (default), `ResultOkCodec`, `HttpResponseCodec`, `DbResultCodec`. Macro selects via
  `replay_codec = Codec` (or `result_codec = Codec`; legacy `recon = Codec`) or kit default. Retire `replay`/`replay_ok`/`replay_with`.
- **G3 (#32, after G2) DB seam fold.** Expose the DB codec; rebuild
  `record_deja_db_query!` to build a `CrossingObservation` and call `dispatch_async`
  with `DbResultCodec` + `extract_kind`/`recover_err`. Add the one real seam change:
  a **fall-through policy** (dispatch re-records on `reconstruct=None`; the DB helper
  runs silently). Delete `record_query_async` and friends.

### Track R — correctness

- **R1 (#33, KEYSTONE) real isolated seeding.** Per-correlation isolation:
  - **Redis** = per-correlation key-prefix at the `add_prefix` chokepoint
    (`redis_interface/commands.rs:106`), mirrored in `seed_redis`
    (`lifecycle/mod.rs:853`). Feasible, localized.
  - **DB** = **schema-per-correlation + per-checkout `SET search_path`**. The
    harness creates one pg schema per correlation and seeds it; the vendored router
    sets `search_path` to `current_correlation_id()`'s schema on each connection
    lease (`connection.rs:25/77`). **Subtlety:** `search_path` must be the corr
    schema ALONE (not `corr, public`) — a `corr, public` fallback would route
    writes to `public` for any table missing in the corr schema, breaking write
    isolation. So each corr schema needs a FULL structural clone of the migrated
    tables (dump DDL once → apply per schema). Connection hygiene: SET on EVERY
    checkout (bb8 reuses connections); a checkout with no active correlation
    defaults to `public`.
  - Seed each correlation from its own `build_seed_plan` into its schema/prefix;
    **DB seeding ON**. Route redis+db reads to **Execute against the seeded isolated
    store**. Add a **seed manifest + query log** to the scorecard (what seeded /
    store-vs-tape / queries run). RMW safe via isolation + record-order.

  **Architecture finding (recon a0eb852d):** the original DB *template-clone*
  assumed the router could connect per-correlation, but it's pinned to ONE bb8 pool
  / ONE `database_url` for the whole process (`connection.rs:8`;
  `storage_impl/.../store.rs:146-168`). The kernel also drives correlations
  **serially** today (`deja-kernel/src/main.rs:103-139`); intra-run
  per-correlation parallelism is net-new. The chosen schema + `SET search_path`
  approach is the parallelism-preserving option with the smallest router footprint.

  **Status (2026-07-01):** REDIS isolation landed + compile-verified across lib
  (`deja::replay_key_namespace`), vendor (`commands.rs` `add_prefix`), and harness
  (`materialize_seed_plan` per-correlation). DB foundation landed + compile-verified
  (`deja::db_schema_for`; schema-aware `seed_db`/`build_upsert_sql`), gated behind
  `DEJA_SEED_DB`. REMAINING (needs a running pg, develop via the demo): the
  schema-clone SQL (gotchas: ON CONFLICT needs cloned PK/unique index; FK on a
  partial seed → `LIKE` no-FK or disable triggers; IDENTITY cols → `OVERRIDING
  SYSTEM VALUE` or clone EXCLUDING IDENTITY; sequence defaults reference public);
  the router per-checkout `SET search_path` (gate behind `DEJA_DB_SCHEMA_ISOLATION`
  so the current demo is untouched, public-fallback when absent); the behavioral
  flips (redis reads→Execute, DB seeding ON, keep `eu_settlement_read`=Substitute);
  the seed manifest + query log; and demo validation.
- **R2 (#34) recording completeness.** Surface buffer drops as a hard signal;
  unique per-session sequencing across instances; kill the lying/dead fields; one
  envelope schema id + registry note; add a masking seam.

## Sequencing

```
G1 kit (strategy/kind defaults now; codec defaults after G2)
G2 ReplayCodec  →  G3 DB seam fold  →  deepen G1 (drop replay_ok)
── parallel ──
R1 isolation → real seed → Execute → seed manifest      (keystone)
R2 recording hardening
```
