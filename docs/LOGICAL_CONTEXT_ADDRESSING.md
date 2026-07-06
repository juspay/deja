# Logical-context (span-path) addressing — design rationale

> **Status: implemented.** All phases of this plan landed: `Address::LogicalContext`
> is rank 2 of the 6-rank ladder (`deja-record/src/replay.rs`), the syntax hash is
> de-signatured, db keys are normalized, and `POSITIONAL_FALLBACK_RANK = 6`
> (`deja-orchestrator/src/divergence/mod.rs`). Kept as the canonical rationale for
> WHY the ladder is shaped this way.
>
> "The occurrence-swap bug" below refers to the motivating failure: two concurrent
> updates (`payment_attempt` / `payment_intent`) calling `now()` from the same
> callsite swapped each other's recorded values under async interleaving, because
> a positional `occurrence` was the only disambiguator.

---

## What addresses what (content vs span-path are complementary layers)

The `LookupKey` has four dimensions: `correlation_id` (request) · `address` · **`args_hash` (content)** · `occurrence`.
Which dimension disambiguates depends on whether the boundary is arg-rich or arg-poor — they are **layers in one
key**, not alternatives:

| Boundary | What addresses it | Span-path needed? | Substitutes |
|---|---|---|---|
| **redis** | **content** — clean `args_hash` (`{key, command, …}`) | no | recorded reply (`DejaRedisValue` raw reply) |
| **db** | **content** — `args_hash` of `{operation, table, sql, inputs}`, **after** excluding version-volatile binds (modified_at, type_name churn) | no | recorded result row (`DejaDatabaseResult`) |
| **arg-poor seams** (`now()`/`id()`/`rng`) | span-path (`LogicalContext`) → occurrence | yes | recorded entropy value |

Content-addressing (`canonical_args_hash`, replay.rs:800-834) is UNCHANGED and is the PRIMARY matcher for redis/db:
distinct args → distinct `args_hash` bucket → occurrence 0 by content; the span-path/occurrence machinery never
engages for them. The span-path redesign is purely additive and only serves the entropy seams where `args_hash` is
constant. (DB is the one boundary whose content is version-volatile → needs the db-key normalization to stay stable;
redis content is already clean.)

## FINAL CONCRETE DESIGN (span-scoped hierarchy — locked 2026-06)

**Hierarchy:** `correlation_id (request)` → `LogicalContext(span-path)` → `occurrence (span-scoped tiebreak)`.
Substitution of `now()/id/rng` STAYS (controlled env). The whole redesign lives in the `Address` dimension —
`LookupKey` shape, `KeyStamper` bucket formula, and the strongest-first probe loop are all **UNCHANGED**.

**Address enum (replay.rs:701-731) — add one variant + renumber:**
```
Explicit(String)                  -> rank 1
LogicalContext { path: String }   -> rank 2   (NEW; root->leaf join of static #[instrument] span NAMES, joined by '>')
SyntacticHash(u64)                -> rank 3   (was 2; de-signatured = boundary::operation only)
LexicalPath { .. }                -> rank 4   (was 3)
SourceLocation { .. }             -> rank 5   (was 4; diagnostic-only)
Sequence { .. }                   -> rank 6   (was 5; positional last resort)
```
Probe loop unchanged (replay.rs:1166-1179) → natural order becomes Explicit>LogicalContext>SyntacticHash>...>Sequence
= the hierarchy, with **zero loop change**.

**Occurrence auto-scopes for free:** KeyStamper buckets on `(correlation_id, Address, args_hash)` (replay.rs:907).
Span-path is INSIDE the Address → bucket already contains it → occurrence becomes scoped to
`(correlation, span-path, args)` with **zero KeyStamper change**. Two distinct logical callers of one arg-poor
seam → two distinct buckets → each occurrence 0, regardless of async interleave (the occurrence-swap fix). Reorder
perturbs only the winning bucket's pool — blast radius = the resolving span, **never wider than correlation**.

**Span-path capture (symmetric record+replay):** a second thread-local LIFO of span NAMES in the already-installed
`DejaCorrelationLayer` (mirror `GUARD_STACK` on_enter push name / on_exit pop, correlation_layer.rs:48-129).
`current_logical_span_path() -> Option<String>` joins root→leaf with `'>'`. Installed whenever
`DEJA_MODE=record|replay` → symmetric. NO graph sidecar / `DEJA_GRAPH_DIR` needed.

**Stamp:** in the macro's single `identity_build` block (instrument.rs:90-111) add
`logical_context: current_logical_span_path()` (runtime call like `next_boundary_occurrence`). Built ONCE, reused
for both `replay_boundary` and `start_boundary_event` → no drift. Mirror in recordable.rs + the hand-written DB
path (deja/src/lib.rs:454-463). Rides ON the event (`CallsiteIdentity` is a serde field of `SemanticEvent`,
lib.rs:129-130) — no sidecar; serde `default` lenient like `syntax_hash`.

**Preserve green (by construction):** purely additive. `canonical_args_hash` unchanged → arg-rich redis/db calls
self-address by content at occurrence 0 in distinct args_hash buckets. If they carry no `logical_context`, they
resolve at the EXACT rank as today (label auto-follows `format!("rank_{rank}")`, mod.rs:85-87); if they do, they
resolve at rank-2 with the SAME args_hash + SAME occurrence 0 + SAME recorded result. Identical outcome.

**Phases (gated):**
- **P0** — de-signature rank-2: drop `sig_string`, `syntax_hash_input = boundary::operation` (instrument.rs:77-79 +
  recordable.rs). *Gate:* same boundary::operation hashes equal across a benign signature edit.
- **P1** — span-path capture: thread-local name-stack in `DejaCorrelationLayer` + `current_logical_span_path()`.
  *Gate:* `info_span!(a)>info_span!(b)` → `Some("a>b")`; empty → None; spawn-propagation smoke test.
- **P2** — `CallsiteIdentity.logical_context: Option<String>` (serde default) + stamp at `identity_build`.
  *Gate:* old recordings still deserialize; events now carry the path under an instrumented fn.
- **P3** — `Address::LogicalContext` variant + rank renumber + emission in `addresses_for`. *Gate:* renderer/hook
  byte-identical keys; the two iteration-order-independence tests still green at occurrence 0.
- **P4** — detector: replace literal `rank == 5` (divergence/mod.rs:280) with named `POSITIONAL_FALLBACK_RANK`
  (= Sequence's rank, guarded by a unit test); ENFORCE `policy_version`/`event_schema_version` (today write-only)
  so a v2-key candidate vs v1-table → **inconclusive**, not mass false OmittedCalls.

**Honest caveats:**
- **Spawn propagation is NOT universal** — span (and thus span-path) reaches a spawned task only if the spawn used
  `.in_current_span()`/`.instrument()`. Bare spawns degrade to occurrence (bounded) — same graceful-degradation as
  the correlation layer; not a correctness break.
- **Version gate is currently write-only** — P4 must add real enforcement, else a new-key candidate silently
  mismatches an old table.
- **Span-name collisions** — two distinct ops sharing an identical root→leaf name chain collide on path → occurrence
  within that bucket (accepted bounded hit).
- **Keep `Summary.recovered_rank5_calls` field name** (fix only its doc) to avoid breaking external dashboards.

---


## The reframe

Deja is for **cross-version regression**: record on baseline V1, replay candidate V2,
did V2 diverge? Therefore every component of the match key must be **version-independent**
— invariant under edits, line-shifts, and reordering (incl. async interleaving) that don't
change observable behavior. This disqualifies anything physical/positional from being the
*disambiguator*. Version-stable signals are only: boundary/operation **names**, args
**content** (canonical_args_hash), and **logical call context**.

Arg-**rich** boundaries (redis/db) self-address by content → already correct, must be preserved.
The unsolved class is arg-**poor shared seams** (`date_time::now`, `generate_*_id`, uuid, ring
nonce/key): no content, and the macro `syntax_hash`/`lexical_path` are the *seam's* identity
(identical for all callers) → they collapse to `occurrence`, which is positional → the occurrence-swap
`modified_at` swap.

## Decision re-evaluation (verdicts)

| Decision | Verdict | Why |
|---|---|---|
| Crypto boundary deletion (keep only nonce seam) | **holds** | AES-GCM is pure; carries no key. A real V2 cipher/KDF change surfaces at the consuming boundary. Stronger under cross-version. |
| Request-id Option B (`current_correlation_id().is_some()`) | **holds** | Behavioral discriminator, not file:line/hash. Document the middleware-ordering assumption. |
| DejaCorrelationLayer (request_id span → context) | **holds** | Most robust: correlation value is *harness-anchored* (same external id on V1 record & V2 replay). |
| args_hash for redis (arg-rich) | **holds** | Clean JSON, structural FNV-1a, distinct bucket @ occurrence 0. The green path — preserve. |
| args_hash for **db** | **needs-revisit** | db args embed `type_name::<P>()`, `format!("{values:?}")`, and `debug_query` sql — version-volatile. A benign V2 struct reorder / type rename / diesel bump → FALSE OmittedCall. Normalize the db key. |
| db DejaDatabaseResult + recover_err (kind-based) | **holds** | Recovers on stable `kind`, not message text. `type_name` is result *metadata*, recorded-and-ignored on decode. |
| **rank-2 `syntax_hash` = FNV-1a(boundary::operation::SIGNATURE)** | **BROKEN** | Folds the whole signature → a benign V2 signature edit changes the hash → strongest address misses → silent demotion. The hand-written db path already omits the signature (correct). Fix: hash only `boundary::operation` (or `::component::`). |
| rank-3 LexicalPath (`module_path!()` of definition) | **needs-revisit** | Seam's module, identical for all callers (no disambiguation); also shifts on V2 module move. Weak fallback only. |
| **`occurrence` as the per-rank disambiguator** | **BROKEN** | Positional → swaps under reorder/async (the occurrence-swap bug). Legitimate ONLY as last-resort tiebreak for genuine repeats of the SAME logical call. |

## Lookup-key redesign

Keep `LookupKey { correlation_id, address, args_hash, occurrence }` shape. Add a strong
Address variant and renumber:

```
Address::Explicit(tag)        = rank 1
Address::LogicalContext{path} = rank 2   ← NEW (version-stable logical caller context)
Address::SyntacticHash(u64)   = rank 3   (de-signatured — boundary::operation only)
Address::LexicalPath{..}      = rank 4   (weak fallback)
Address::SourceLocation{..}   = rank 5   (DIAGNOSTIC-only)
Address::Sequence{..}         = rank 6   (terminal fallback only)
```

- New field `CallsiteIdentity.logical_context: Option<String>` (serde `default`, lenient
  round-trip like `syntax_hash`).
- `addresses_for` emits `LogicalContext{path}` after Explicit / before SyntacticHash when present.
- **KeyStamper bucket formula `(correlation_id, address, args_hash)` is UNCHANGED** — adding the
  path to the Address automatically partitions the space, so two distinct logical callers of the
  same arg-poor seam land in different buckets, each at occurrence 0. Occurrence reverts to
  tiebreaking only genuine same-call repeats.
- **Preservation of the green arg-rich path**: change is purely additive; arg-rich calls resolve
  at LogicalContext (if present) OR fall through to SyntacticHash, both at occurrence 0, identical
  result. Existing iteration-order-independence tests keep passing (their args differ).
- **Bump `event_schema_version` + `policy_version`** so a new-key candidate is never silently
  matched against an old table.

## logical_context source — span-path only (explicit tags AVOIDED)

User directive: avoid explicit callsite tags as much as possible. This is achievable because the
occurrence swap only happens across **concurrent (async-interleaved) tasks** — never within
sequential code. Span-path handles the concurrent case; ordinary occurrence (now scoped *inside* a
span-path bucket) handles the sequential case. Three cases:

- **A — different concurrent operations** (the motivating case: payment_attempt vs payment_intent updates in
  separate spawned tasks): run under DISTINCT spans → distinct `logical_context` → different
  `(corr, LogicalContext(path), args_hash)` buckets → each at occurrence 0. Interleaving irrelevant.
  **No tag.** Confirmed viable for the motivating case (`update_payment_attempt_with_attempt_id` vs
  `update_payment_intent`, verified by events.jsonl span-id grouping; graph.rs:116 already captures
  `span_name`).
- **B — two arg-poor calls in one synchronous fn** (`modified_at = now(); created_at = now();`):
  same span/bucket → occurrence 0,1, but the fn always calls them in the SAME order → stable across
  runs. **No tag.** (The workflow's "two calls in one fn → need a tag" was overstated: B is
  sequential, so occurrence is stable.)
- **C — two *concurrent* tasks sharing the *same* span doing arg-identical seam calls**: same bucket,
  interleaving swaps. The only case span-path + occurrence can't separate — and it's rare. **Preferred
  fix: add a finer `#[instrument]` span** (function-level annotation, fits the seam/span philosophy),
  turning C into A. NOT a callsite tag.

**Plumbing gap to close**: today only the opaque numeric `tracing_span_id` is on the event
(per-process, NOT cross-version comparable) and the motivating recording persisted no graph sidecar — so the logical
span NAME is currently unrecoverable at replay. Must stamp the span *name/path* onto the event +
persist it.

**Explicit tags (`Address::Explicit`)**: DROPPED from the core plan. Kept only as a documented
escape hatch for a residual case C that even finer spans can't resolve (not expected). Not built now.
Note for the future: the consume side exists, but there is NO producer (both derive macros hardcode
`SyntacticHash, id:None`), so a tag would require new producer plumbing.

**Cross-version drift policy**: a `logical_context` MISMATCH is a divergence SIGNAL, never a silent
fallthrough to weaker ranks (which would risk a false match).

## Change plan (ordered, gated)

- **Phase 0 — de-signature rank-2.** `instrument.rs:77-79`: hash `boundary::operation`
  (or `::component::operation`), not the signature. *Gate:* determinism test (same op, different
  signatures → same hash) + re-record + still green. Confirm `boundary::operation` global uniqueness.
- **Phase 1 — normalize the db key.** `deja/src/lib.rs` args(): a normalization pass before
  `canonical_args_hash` that strips version-volatile material (type_name/Debug/sql churn) and
  volatile entropy binds (modified_at/created_at/session_expiry/…). **Do NOT touch the result
  envelope.** *Gate:* db replay tests + the occurrence-swap no longer reproduces at the db layer + Ok-value drift still
  surfaces.
- **Phase 2 — LogicalContext Address + CallsiteIdentity field + rank renumber (source empty).**
  `replay.rs` (Address variant + rank()), `lib.rs` (field, serde default), `divergence/mod.rs`
  (rank_label + replace hardcoded `rank==5` with a named weakest-rank constant). *Gate:* serde
  round-trip + full deja-record suite + arg-rich order-independence tests still @ occurrence 0 +
  schema/policy bumped.
- **Phase 3 — wire span-path source (THE mechanism).** `graph.rs`: cheap current-logical-span-path
  accessor (walk parent_id, join `span_name`); stamp `logical_context` onto the event where
  `current_execution_graph_context()` is called; replay side reads the same ambient path into
  `ReplayLookup`. **Persist the span path** (or co-locate execution-graph.jsonl); confirm graph layer
  registered in deja_boot + `DEJA_GRAPH_DIR` set; verify spawn span propagation. *Gate:* re-recorded
  the motivating recording shows distinct logical_context on the two now() events, resolves WITHOUT occurrence — and
  NO tags.
- **Phase 4 — seam survey + finer-span coverage + reclassify positional ranks as diagnostic.** Audit
  the ~8 arg-poor seams; quantify span-path coverage (cases A/B). Where a genuine case C is found
  (concurrent tasks sharing one span), add a finer `#[instrument]` span — NOT a tag. Make `divergence`
  reject positional-only passes for arg-poor seams. *Gate:* scorecard shows each arg-poor seam resolves
  at LogicalContext (cases A/B) with occurrence stable; any case C resolved by a finer span.

## Residual (the now()-swap) — closed two ways, no tags

1. **Immediate (Phase 1):** exclude volatile timestamp binds from the db match key → the two UPDATEs
   stop disambiguating on a volatile value; the recorded result returns regardless. A real V2
   timestamp-logic regression still surfaces in the response/byte-compare.
2. **Seam-level (Phases 2-3):** the two now() calls carry distinct `logical_context` from their
   DISTINCT leaf spans (`update_payment_attempt_with_attempt_id` vs `update_payment_intent`) →
   different `(corr, LogicalContext(path), args_hash)` buckets, each occurrence 0 → can't receive each
   other's value regardless of interleaving. **Span-path alone resolves the swap — no tag needed.**

## Top risks / open questions

- **Same-binary blind spot:** *every* green run so far is record+replay on the IDENTICAL build, where
  the rank-2-signature and db-type_name/sql fragilities are invisible. We need a real V2 candidate to
  actually exercise the broken verdicts. (Is there one yet?)
- **Span granularity survey:** does each hot seam (nanoid/uuid/ring DEK·nonce/jwt) sit under a
  *distinct* instrumented span? Two arg-poor calls in one fn require a tag.
- **Execution-graph persistence:** must persist the per-event span *path*; the numeric id is useless
  cross-version. Confirm graph layer in deja_boot + `DEJA_GRAPH_DIR`.
- **leaf-only vs full root→leaf path:** leaf cheaper but may collide via shared helper; full more
  discriminating but refactor-fragile.
- **rank-2 uniqueness after de-signature:** is `boundary::operation` globally unique or must
  `component` be included?
- **db volatile-bind exclusion:** value/key denylist may miss an unlisted volatile column — pair with
  a test.
- **Case C residual (tags avoided):** the only thing span-path + occurrence can't separate is two
  *concurrent* tasks sharing one span doing arg-identical seam calls. Preferred fix is a finer
  `#[instrument]` span (turns C into A), NOT a callsite tag. The Phase-4 survey must quantify whether
  any real case C exists in the workload; if none, tags are never needed.
- **Document the HOLDS assumptions** (bootstrap-before-scope; span propagation on spawned tasks) so a
  V2 that breaks them reads as a real change, not a tooling bug.
