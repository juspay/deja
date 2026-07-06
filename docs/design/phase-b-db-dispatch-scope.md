# Phase B scope — fold `record_query_async` into generic dispatch (+ identity risks)

**Requested by OMP** (clean-dsl-closeout, Phase B scoping only — no edits made). Read-only analysis
against the current uncommitted tree. Facts cited from `crates/deja/src/lib.rs` (db module),
`crates/deja-record/src/lib.rs` (dispatch), `crates/deja-derive/src/instrument.rs`,
`crates/deja-record/src/replay.rs`, vendor `diesel_models/query/generics.rs`.

## 1. What is ALREADY generic (no work needed)

`dispatch_async` (deja-record lib.rs:2540) duplicates record_query_async's structure 1:1 for:
mode branching (Record/NoOp/Replay via `record_only_path`), the Execute-shadow arm
(peek → run → extract → observe, same fail-stops), the Substitute-miss fail-stop, state keys
(`CrossingObservation.state_capture` has the same `state_read_to/write_to/touch_to/with_*`
builders as QuerySpec), correlation threading, and declaration/OperationKind attachment
(`BoundarySemantics.declaration`). Confirmed: **declaration metadata does NOT participate in any
lookup address or args_hash** (Address/LookupKey/canonical_args_hash contain no declaration
fields) — stamping richer metadata is identity-safe.

## 2. The two irreducible deltas — and both fit dispatch's existing closure params

**(a) `recover_err` (non-serde error reconstruction).** dispatch has no separate error hook, but
its `reconstruct: FnOnce(Value) -> Option<T>` with `T = Result<R, E>` subsumes it: a composed
closure can decode the `DejaDatabaseResult` envelope and map
`Ok{value} → Some(Ok(R))`, `Err{kind,msg} → recover_err(kind,msg).map(Err)`,
`recover_err→None | FallThrough | undecodable → None`. No new macro knob is required for Phase B
because the DB seam is invoked via the vendor `record_deja_db_query!` macro → a plain fn call —
NOT via the attribute macro. (An attribute-macro `recover =` knob is a LATER, optional step.)

**(b) DB result envelope.** dispatch's `extract: Fn(&T) -> (Value, bool)` can simply be
`|out| result_serialize_db(out, &extract_kind)` — the same envelope on both the record path and
the Execute-shadow observe path (record_query_async already uses it for both).

## 3. ONE behavioral delta requiring an explicit decision (do not paper over)

Lookup-arm fall-through: `record_query_async` runs live WITHOUT re-recording when
`recover_err→None` / `FallThrough` (deja lib.rs:811-820); dispatch's Lookup fall-through
(deja-record lib.rs:2453-2458) runs live AND records the result. Migrating as-is changes replay
accounting (extra observed rows where today there are none) — the divergence classifier and
scorecard counts may shift on tapes that exercise legacy/undecodable hits. Options:
  (i) add a fall-through-record toggle to dispatch (smallest, preserves today's semantics), or
  (ii) accept the re-record accounting and verify the classifier tolerates it.
Recommend (i) for Phase B; revisit under Phase E proof gates.

## 4. Identity risks — the preservation contract (this is the whole ballgame)

The DB seam hand-builds its `CallsiteIdentity`; the safe migration REUSES that construction
verbatim rather than adopting macro-style identity. Every one of these must stay byte-identical or
old tapes stop resolving (today's runs resolve ~everything at rank_2):

| Identity input | Today's value (must preserve exactly) |
|---|---|
| syntax_hash (rank 3) | `stable_callsite_hash("{boundary}::{component}::{operation}")` |
| scope / lexical_path (rank 4) | `"{component}::{operation}"` — ALSO keys the occurrence counter |
| occurrence | `next_boundary_occurrence(corr, SyntacticHash, "{component}::{operation}")` |
| logical_context (rank 2 — the workhorse) | `current_logical_span_path()` captured at the same setup point (before the await) |
| source | `SyntacticHash` (NO rank-1 Explicit) |
| rank-6 Sequence | `{boundary: "db", method: operation, request_sequence}` — operation stays `method_name` |
| rank-5 SourceLocation | vendor macro call site via `#[track_caller]` — the adapter must forward the captured `Location`, not take its own |
| args_hash (every rank) | `db::args(operation, &table, sql, inputs)` envelope `{operation, table, sql, inputs}` — the SAME JSON must feed lookup AND the recorded event |
| semantics | `db_semantics(declaration)`: strategy Execute, kind "db" (policy still maps Execute→Lookup under AllLookup via `replay_strategy_to_execute_mode` — unchanged) |

Declaration/OperationKind/ReturnSemantics/CodecRef: confirmed metadata-only → safe to enrich.

## 5. Smallest safe step sequence

- **B1 (core, library-only, vendor untouched):** keep `record_query_async`'s public signature;
  reimplement its BODY as: build (BoundarySpec, CallsiteIdentity, CrossingObservation) using the
  EXISTING construction code (moved, not rewritten), then call `dispatch_async` with
  args-thunk = the same `db::args` envelope, extract = `result_serialize_db ∘ extract_kind`,
  reconstruct = envelope-decoder folding `recover_err` (§2). Deletes the duplicated arms
  (~150 lines) without changing any caller or any tape byte.
- **B2:** resolve the fall-through delta (§3) — recommend the toggle.
- **B3 (later/optional, NOT Phase B):** vendor macro expands directly to `dispatch_async`
  (kills the public helper; vendor change → PR scope), and/or attribute-macro knobs
  (`recover =`, a `DbEnvelope` recon) if the DSL should express db seams first-class.

## 6. Proof gates for B1/B2

1. Existing unit tests unchanged-green: `result_serialize_db`/`decode_recorded_db_result`
   (deja lib.rs tests) + `boundary_macro.rs` db-helper test.
2. **Old-tape resolution test (the identity gate):** replay an existing recording through the
   migrated binary; require identical `resolved_by_rank` (rank_2 dominance) and 0 new misses.
3. Full self-check pass=true 9/9 (baseline: cycle 30).
4. Grep gate: no remaining direct `replay_boundary`/`execute_shadow_*` callers in deja::db
   (the `#[allow(deprecated)]` on record_query_async should become deletable).

## Out of scope for Phase B (explicitly)

Vendor edits of any kind; attribute-macro knob additions; per-op declared replay_strategy for db;
ingress DSL (see docs/design/ingress-declarative-extraction-map.md); OperationKind consumption in
seed/divergence (Phase A, OMP-owned).
