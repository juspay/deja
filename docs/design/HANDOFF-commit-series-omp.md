# Commit-series handoff → OMP (concurrent DSL/metadata editing)

Local-only commit prep, 2026-07-02. No pushes. OMP holds: `crates/deja-record/src/lib.rs`,
`crates/deja/src/lib.rs`, `crates/deja-derive/*`, `crates/deja/tests/boundary_*.rs`, vendor
`diesel_models/query/generics.rs`, vendor `services/kafka/deja_record_sink.rs`. Nothing below
touches those; my hunks inside them are handed off here.

## Committed (vendor `deja-lean`)
- `0706104691` fix(deja): replay DB schema routing at the storage connection seam
  (storage_impl utils/connection/store/kv_router_store/lib + Cargo, router connection/db/app + Cargo, Cargo.lock).
  NOTE: depends on two library fns that live UNCOMMITTED in `crates/deja/src/lib.rs` (path-dep, so
  builds fine) — see hunks below.
- `a868860ceb` chore(deja): drop demo-only EU settlement (payment_create.rs → base; redis lib.rs
  KeysInterface re-export removed — also DROP it from deja-pr-next's delta 7c1372fb74 at regeneration).
- `5e7ba64c13` docs(deja): DEJA_ARCHITECTURE.md moved out of vendor (now `docs/DEJA_ARCHITECTURE.md`).

## Hunks of MINE inside OMP-held files (fold into your commits, or hand back)
1. `crates/deja/src/lib.rs` — three changes already in the working tree:
   - `pub fn current_correlation_id() -> Option<String>` (unconditional deja-context re-export).
   - `pub fn replay_search_path_sql_for(correlation: &str) -> String` (library owns the SET SQL;
     vendor commit `0706104691` calls both).
   - In the `__private` re-export list: `current_span_correlation` REMOVED (probe fn deleted).
2. `crates/deja-record/src/lib.rs` — re-export shrunk to
   `pub use correlation_layer::{current_logical_span_path, DejaCorrelationLayer};`
   (pairs with the probe-fn deletion in `correlation_layer.rs`).

## Outer-repo commits HELD until you unlock (mine, green-verified as a whole tree, cycle 30 9/9)
- `crates/deja-record/src/correlation_layer.rs` — temporary `current_span_correlation` probe fn
  removed (+ unused `Registry` import). Pairs with hunk 2 above.
- `crates/deja-record/src/replay.rs` — `build_seed_plan` seeds reads BEFORE marking writes
  (self-referential UPDATE pre-image fix, /connectors 500) + 2 regression tests.
- `crates/deja-orchestrator/src/divergence/{mod,ledger}.rs` — Rule A (order-nondeterminism) +
  Rule B (idempotent redis delete) strict-guarded demotions + `RunArtifacts.events` + 10 tests.
- `crates/deja-orchestrator/src/lifecycle/mod.rs` — bytea seed literals
  (`Encryption {inner:[u8]} → '\x…'::bytea`) + 2 tests; PIN/PROBE block removed.
- `crates/deja-kernel/src/lib.rs` — earlier-arc working state (not this session's).
Held because snapshot-buildability against your in-flight `deja-record/src/lib.rs` /
`deja/src/lib.rs` can't be verified without touching them.

## Flags
- Vendor `redis_interface/commands.rs` — working diff −191/+57 (add_prefix replay-namespace + more)
  NOT authored this arc; left uncommitted for your attribution.
- Vendor `diesel_models/src/schema.rs` — my restore-to-upstream-formatting commit no-op'd: the
  working tree reverted to the one-line collapsed form (looks like `diesel print-schema`
  regeneration in your flow). Either restore upstream formatting when you commit generics.rs, or
  exclude schema.rs at PR curation (deja-pr's copy is already clean per the PR topology notes).
- Final PR gate after your files land: full self-check must stay pass=true 9/9 (baseline: cycle 30).
