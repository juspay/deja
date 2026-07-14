# Tracing as the single substrate — findings and verdict (W4)

**Status: FINDINGS + RATIFIED DIRECTION (user, 2026-07-07). Implementation is W4 scope.**
Question under investigation: can `tracing` layers — rather than deja-specific
task-locals and spawn wrappers — solve (a) call-site identity, (b) correlation
across task-local *and* thread-local propagation, and (c) the causal call graph,
comprehensively enough to power reports?

## Current state (what exists today)

- `DejaCorrelationLayer` (deja-runtime/src/correlation_layer.rs) **is already a
  tracing `Layer`**: `on_new_span` resolves a span's correlation once — from its
  own `request_id` field or by inheriting its parent's — and `on_enter` mirrors
  it into deja-context for the current thread. Hyperswitch's root span
  (`CustomRootSpanBuilder`) already records `request_id` as a span field.
- On top of that, deja adds machinery the layer does NOT yet subsume:
  `scope_correlation` future-wrappers at the middleware seam, `deja::spawn_detached`
  (a stamp-only wrapper minting `TaskLineage{bucket_id, fork_seq}` task-locals),
  a macro-built `CallsiteIdentity` (syntax hash + span path + occurrence), and a
  separate execution-graph layer (`graph.rs`).

## Findings per problem

### (a) Call-site identity — SOLVED by static span/event metadata

Every tracing callsite carries **static `Metadata`** (name, target, module path,
file, line) behind a stable `callsite::Identifier`. Keying identity on
`(metadata name+target, span path, per-correlation occurrence)` reproduces the
current rank ladder almost 1:1: explicit span names = rank 1, span path = rank 2,
name+target = the syntax-hash role (rank 3/4), file/line demoted to the weak
positional ranks exactly as today.

Two invariants the design must keep:
1. **Never key lookup identity on runtime span IDs** — they are
   subscriber-assigned and unstable across runs. Static metadata + path +
   occurrence only. (The current design already obeys this; the constraint just
   transfers.)
2. **Never key strong ranks on file/line** — they churn across candidate
   versions; the current model deliberately excludes them from strong ranks and
   that must survive the substrate swap.

### (b) Correlation (task-local AND thread-local) — SOLVED, with one lint-enforced idiom

tracing's subscriber tracks the current span **per thread**, and `Instrumented`
futures re-enter their span on every poll — so span context natively crosses
`.await` points (the task-local role) and is visible to synchronous code on the
current thread (the thread-local role). Since correlation is derivable from span
ancestry (already implemented in the layer), the deja-context propagation
plumbing becomes redundant:

- The middleware `scope_correlation` wrapper → replaced by the root span's
  `request_id` field, which upstream already sets.
- `deja::spawn_detached`'s context capture → replaced by spawning with an
  instrumented span (see (c)).

**Residual**: bare `std::thread::spawn` / `spawn_blocking` closures that never
enter a span lose context — same class of gap as un-wrapped spawns today. Fix is
a tracing idiom (`span.in_scope(...)` / `.instrument(...)`) enforced by the same
lint slot that currently denies raw `tokio::spawn` — standard vocabulary instead
of deja vocabulary. Uninstrumented stragglers surface as uncorrelated events and
the undeclared-concurrency detector, so the failure mode is visible, not silent.

### (c) Call graph + causal relationships — SOLVED, and richer than today

- **Synchronous call tree**: span parent edges, already dense upstream
  (`#[instrument]` coverage) — free.
- **Fork edges**: a span *created before* `tokio::spawn` carries its parent edge
  from creation context, so `tokio::spawn(fut.instrument(child_span))` records
  the fork with zero deja vocabulary; `follows_from` can be added for explicit
  causal semantics. (tokio's automatic task spans need `tokio_unstable`, which
  upstream does not enable — so instrumented spawn is the mechanism.)
- **Lineage**: bucket = the spawned task's root-span subtree; `fork_seq` = a
  per-parent counter maintained by the layer in `on_new_span`. This replaces
  `TaskLineage` task-locals outright.
- **Unification**: the same edges power the execution graph — `graph.rs` merges
  into the one layer, giving reports/HTML a single comprehensive causal DAG
  (sync tree + forks + boundary events) instead of two half-graphs.
- **Ordering contract**: all spawned tasks are treated as unordered regions
  (companion hypothesis) — the detached-vs-joined bit disappears; joined tasks
  complete before their join point in both record and replay, so declared-canon
  comparisons converge regardless. Verified by soak during implementation.

## Graph-as-events (ratified addendum, 2026-07-07)

The execution graph becomes **tape-carried**: the same tracing layer emits two
record kinds through the one Kafka sink — `BoundaryEvent` (judgment stream) and
graph records (span open/close with parent/`follows_from` edges — the
enrichment stream powering fork trees, span timelines, and record-vs-replay
causal diff). Precedent: `deja_sink_marker` records already share the topic.
The `JsonlSink`/`DEJA_GRAPH_DIR` file side-channel is deleted outright (one
transport, not two), which also drops the graph-dir row from the W3 config
migration. Volume dials: graph records gate on the same correlation sampler,
behind a typed `deja.recording.graph = disabled | enabled` setting (disabled
default preserves today's production posture), with optional pruning to spans
on boundary-bearing paths.

## What this deletes from the vendor PR surface

`deja::spawn_detached` + cfg-paired spawn sites (→ plain
`tokio::spawn(fut.instrument(span))`), the `scope_correlation` middleware
wrapping, and the lineage task-local plumbing. The deja surface in upstream code
reduces to: boundary attributes, the layer installation, and the sink — all
standard-shaped.

## Honest limits (what is NOT magically solved)

1. **~5 spawn sites still change** — from `.in_current_span()` (which actively
   erases the fork: same span, no edge) to `.instrument(child_span)`. Pure
   tracing idiom, but call-site touches nonetheless.
2. **Same-callsite concurrency inside one span** remains the identity-collision
   residual (as today); needs a child span at such sites. Verified rare — the
   parallel confirm updates hit distinct callsites.
3. **Idiom coverage is lint-enforced, not automatic** — same enforcement cost as
   the current wrapper lint, different (standard) vocabulary.
4. **Overhead**: the layer already runs; marginal cost is the occurrence/lineage
   bookkeeping it absorbs from task-locals. Measure in record mode during
   implementation; no expected regression.

## Verdict

(a) solved cleanly; (b) solved for async + sync + cross-thread, with one
lint-enforced idiom for uninstrumented spawn paths; (c) solved and strictly
better than today (one causal DAG, comprehensive reports). Wire shape changes
freely as part of implementation (nothing is live; re-gate with the standard
battery: workspace + vendor + matrix + soak). Implementation lands in W4 after
the W2/W3 folds.
