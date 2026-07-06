# Detached boundaries + replay join discipline (read-race fix, user-ratified direction)

**Status: IMPLEMENTED in the current worktree; three clean isolated-parallel
active-default replay proofs landed under `1783099999`, `1783111111`, and
`1783133333`, fixing the confirm intra-correlation race without changing the
design keystone:**
**cross-correlation replay stays any-order + parallel.** Explicitly rejected: static
request/correlation ordering, broad source-sequence gates, classifier demotion of
control-flow-changing reads.

**2026-07-06 target-model supersession:** `effect-algebra.md` ratifies this
join discipline as a **transitional certified-R6 implementation only**. The
target pre-sandbox model has no joining at record or replay: spawn wrappers
stamp lineage/bucket metadata, replay executes freely, and tolerance moves to
the scorer algebra. Do not remove the current drain before the step-2
pre-sandbox gate and recertification described there.

## The race, precisely

Hyperswitch's payment path `tokio::spawn`s a fire-and-forget update (payment_intent/attempt
post-response bookkeeping) that runs concurrently with the SAME request's main path. Three
observed faces on one row: (1) earlier UPDATE..RETURNING sees post-charge state; (2) the
recording captures the opposite interleaving; (3) the main path's READ races the background
writer and changes downstream control flow (5 omitted calls). HTTP converged in ALL runs — the
response never depends on the racy write. The race is pure post-response state + tape noise.

## Design: `detached` as a first-class scheduling role

### 1. Declaration (vendor, thin)
A vendor-local `router_env::task` wrapper replaces bare request-path `tokio::spawn`
calls:
```rust
task::spawn_detached(async move { ... }) // declared fire-and-forget work
task::spawn(async move { ... })          // structural work whose JoinHandle is awaited
```
Semantics: "this task is fire-and-forget BY CONTRACT — no ordering guarantee vs the main path."
The declaration is at the SPAWN (task) level, not per-boundary: every boundary event inside the
task inherits `detached=true` (carried via task-local, stamped on events — schema-additive).

### 2. Scheduling discipline — SYMMETRIC record + replay (the key decision)
`spawn_detached` defers the task to a **deterministic join point: after the request's response
completes** (the ingress middleware's response-body finalization already knows this moment), in
**BOTH record and replay modes**. Rationale:
- Deferral only-on-replay pins replay to one interleaving while recordings stay racy → tapes that
  captured the other interleaving would diverge DETERMINISTICALLY (worse than the flake).
- Fire-and-forget means the code already promises nothing about when the task runs. Running it
  post-response is WITHIN that contract (arguably its intent), so record-side deferral is a
  legitimate, tiny semantic pin — not a behavior change to anything the caller can observe.
- With both sides deferred: main path NEVER races detached writes; reads are deterministic; the
  tape and replay agree by construction. The race class is dead, not tolerated.
- Outside deja modes (`DEJA_MODE` unset / prod without recording):
  `task::spawn_detached` degrades to plain `tokio::spawn` — zero production
  impact. (Decision point: could also defer in prod for consistency; default NO
  to keep the vendor surface inert.)

### 3. Replay execution of detached tasks
Deferred tasks still run REAL code under Execute + shadow-observation (nothing is no-op'd);
their boundary lookups resolve normally (identity is order-independent: correlation-scoped
occurrence counters + rank-2 logical paths don't depend on wall-clock interleaving). They run
sequentially at the join point, in spawn order — deterministic within the correlation, invisible
across correlations.

### 4. Fallback variant (if join-point plumbing stalls)
No-op detached WRITES on replay + verify final row state via the Phase D seed-readback
certificate (the tape's post-state is the oracle). Weaker: loses the shadow observation of the
write itself; keep only as fallback.

### 5. Classifier interaction
With symmetric deferral, Rule A's order-swap arm and the read-race class should stop firing for
detached-tagged writers on NEW tapes. Keep Rule A (covers non-detached concurrent writes, e.g.
genuinely parallel handlers). The `detached` event flag also gives the ledger a precise label:
divergences involving a detached-task boundary can be attributed as such.
Replay scoring also has a non-blocking `undeclared_concurrency` warning for any
correlated, non-detached observed boundary call that starts after the replayed
`http_incoming` finalizer. The finalizer sentinel is consumed only for timing and
is skipped by normal observed-call classification; detached work is ignored by
this warning.

### 6. What this does NOT do
- No cross-correlation ordering of any kind; parallel replay (#37 / Phase E) unaffected — in fact
  strengthened, since intra-request determinism removes the biggest flake source for the
  parallel-proof acceptance ("no rank recovery from scheduling noise").
- No event-schema break: `detached` and finalizer timing metadata are additive
  fields on the current v5 event/observed-call wire shape.

## Vendor touchpoints (thin, enumerable)
- `router_env::task` exports `spawn` and `spawn_detached`; the wrapper is the
  only blessed raw Tokio spawn point in request-path code.
- Payment-response fire-and-forget bookkeeping uses `task::spawn_detached`; joined
  structural work uses `task::spawn` and still awaits its `JoinHandle`.
- Workspace Clippy denies raw `tokio::spawn`/`tokio::task::spawn`; infra/test
  harness sites outside the request path keep raw Tokio only behind local
  `#[allow(clippy::disallowed_methods, reason = "...")]`.
Library owns: the detached queue, task-local flag, event stamping, finalizer
sentinel, and replay scorer warning.

## Acceptance
1. Unit: detached task defers past main-path completion in record + replay modes; degrades to
   plain spawn outside deja modes. **Covered locally by `deja-record` detached/finalizer tests
   plus `router_env::task` tests with and without `--features deja`.**
2. Stability: 3 consecutive isolated-parallel full-pipeline cycles green for the
   active-default matrix (pass=true 9/9 on `self`/`benign`, all active divergence
   candidates caught). **Covered by `1783099999`, `1783111111`, and `1783133333`.**
3. The confirm read (cycles 38/39's diverger) resolves matched in all three runs.
   **Covered by the three active-default matrix proofs above.**
4. No new omitted/novel calls from the deferral (detached events still recorded,
   still replayed). **Covered for `self`/`benign`; divergence candidates retain
   intentional omitted/novel/value/body signals by patch design.**
5. Raw request-path `tokio::spawn` does not creep back in. **Covered by vendor
   `.clippy.toml` `disallowed-methods` plus targeted `cargo clippy ... -D
   clippy::disallowed_methods` runs over `router_env` and the touched router/infra
   crates.**
6. Undeclared post-finalization correlated work is visible but non-blocking.
   **Covered by deja-orchestrator `undeclared_concurrency` scorer tests.**

### Current proof addendum — `1783099999`, `1783111111`, `1783133333`

Run tags `1783099999`, `1783111111`, and `1783133333` are three clean
isolated-parallel active-default proofs after this fix. In each matrix,
`self` and `benign` passed `matched=9/9` with zero summary
`side_effect_divergences`, `value_divergences`, `http_body_mismatches`,
`omitted_calls`, or `novel_calls`; the five active divergence candidates
(`real`, `earlier-fork`, `dropped-write`, `response-only`, `extra-call`) were
caught as expected. Failed attempt `1783122222` stopped during the baseline
record workload before replay or scorecard generation and is excluded from this
proof set.
