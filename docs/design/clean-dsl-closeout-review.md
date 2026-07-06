# Clean-DSL closeout — Claude review (coordination-request items)

Responding to `clean-dsl-closeout-tasks.md` §"Claude coordination request". Read-only; no edits,
nothing staged. Items 1–2 (DB helper removal path, identity risks) are delivered in
`docs/design/phase-b-db-dispatch-scope.md` — it maps 1:1 onto plan items 5–7 (B1=design/preserve,
B2=the one behavioral delta the plan doesn't list: dispatch's Lookup fall-through RE-RECORDS where
record_query_async runs live silently — needs a toggle or an accounting decision; B3=callsite
removal + quarantine). Remaining items:

## Is Phase A safe to land before DB helper removal? — YES, with two cautions

Phase A changes CONSUMERS (seed planner, classifier); Phase B changes the PRODUCER plumbing. They
are independent because (a) declarations are already stamped through QuerySpec →
`BoundarySemantics.declaration` on today's record_query_async path, so Phase A consumes data that
exists regardless of which dispatch path produced it; (b) lookup identity is declaration-free
(verified — Address/LookupKey/canonical_args_hash carry no declaration fields), so consumer changes
cannot shift matching; (c) Phase A's legacy fallbacks keep undeclared/old tapes working under
either producer.

Cautions:
1. Phase A tests should construct events both synthetically AND via the real
   `record_query_async` path, but must not assert on producer-path internals (e.g. the exact
   `DejaDatabaseResult` envelope shape) — those are exactly what Phase B refactors. Assert on
   declaration fields + classification outcomes only.
2. Rule A: keep `returns_row()` on the RECORDED result as an EVIDENCE check even when
   `returns == UpdateReturning` is declared. Declaration states intent; the recorded row is
   evidence. Same for Rule B: the declared `IdempotentDelete` replaces the method-name
   identification, not the `KeyDeleted→KeyNotDeleted` reply-pair check or the HTTP-clean gate.

## eu-overcharge status (plan item 17) — answered definitively

The vendor injection is GONE: `payment_create.rs` was reverted to base in vendor commit
`a868860ceb` (2026-07-02, vendor-thin CP1) — `eu_settlement_read/write` + the `get_trackers` call
no longer exist in the tree, and the `KeysInterface` re-export went with them. The 9/9 self-check
never depended on it (verified: cycle 29 ran 9/9 immediately after removal). The cross-version
matrix candidate (`eu-overcharge.patch`, outer repo demo) still exists as an artifact but patches
code that no longer contains its context — treat it as INVALID until reworked as an outer-repo
patch against base. So yes: the transitive-dependency test (item 16) must replace it, and until
then no claim should reference eu-overcharge proof.

## Transitive-dependency test shape (item 16) — recommended design

Ground it in the machinery that already models cascades (ValueDiverged origin→consequence):
- Chain: boundary A writes state (declared `state_write`, Execute) → B reads it (declared
  `state_read`, **Execute**) and writes B′ derived from the read → C reads B′.
- Candidate mutation: change what A writes.
- Expected: B's read = `ValueDivergedOrigin` (args-aligned execute divergence — the CAUSE); B′/C
  writes pair args-free as CONSEQUENCE rows; ledger shows origin→consequence; verdict fails.
- Two assertions the plan's acceptance should add:
  1. The divergence must remain BLOCKING — explicitly assert Rule A/B do NOT demote it (guards
     against over-demotion regressions; it is real state drift, not order nondeterminism).
  2. Run it under Execute for the reads. Under Substitute the recorded value is served at B and
     the cascade is invisible by design (partial-vs-total-derivative). That mode-sensitivity IS
     the point of the test — consider asserting the Substitute run stays quiet as the negative
     control.

## Typed store images (Phase C) — shape review

Direction is right (`DbRowImage` with column name/type-OID/nullability permanently kills the
bytea/JSON-rendering class — the exact bug fixed by hand in `sql_literal`). Risks to design in:
1. `StateKey::DbRow{table, pk}` needs PK knowledge — RETURNING rows carry it, plain INSERTs may
   not; the query-fingerprint fallback (planned) is required, keep it.
2. State keys feed read_set/write_set → seed planning + created_tables masking. Changing key
   SHAPE breaks old-tape seed planning unless legacy `"table:sql"` strings remain accepted as
   opaque keys alongside typed ones (same dual-path posture as the declaration fallbacks).
3. Keep state keys OUT of lookup identity (they are today; preserve under Phase C).
4. Redis TTL capture is approximate at write time — record it as advisory, don't hard-verify
   equality on readback (Phase D item 13), use a tolerance or presence check.

## Isolated parallel replay (Phase E items 14–15) — proof requirements review

- Run-level isolation largely exists (per-run compose project + port, M3). Add to acceptance:
  distinct pg databases per run (schema-per-correlation isolates WITHIN a run only) and zero
  cross-run artifacts in `/harness-state`.
- In-run parallel correlations: the known bleed class is ambient-context propagation
  (thread-local correlation vs spawned tasks) — serial replay masked it; concurrency stresses it.
  The store-request-id routing (storage seam) is already correlation-correct; redis namespacing
  keys off `replay_key_namespace()` (ambient) — verify it under concurrency.
- Occurrence counters are correlation-scoped (`next_boundary_occurrence` keyed on correlation) —
  safe. The fragile input is rank-6 `request_sequence` under concurrent arrival: acceptance
  should assert **no growth in positional (rank-6/recovered) resolutions** vs the serial run —
  "no rank recovery caused by parallel scheduling noise" in the plan is exactly right; make it a
  hard count comparison, not an eyeball.

## Sequencing opinion

Phase A now (safe, above) → Phase B (scoped, small) → Phase C/D (typed images + certificates;
biggest surface, do after B so images are designed against ONE dispatch path, not two) →
Phase E proofs. Item 16 (transitive test) can land any time after Phase A — it depends only on
existing Execute machinery and would restore the "catch" proof lost with eu-overcharge sooner
rather than later.
