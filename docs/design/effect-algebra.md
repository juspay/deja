# Effect canonicalization + buckets: order-tolerant judgment (ratified target design)

**Status: RATIFIED direction (user, 2026-07-06). Supersedes the scheduling half of
`detached-boundaries-replay-join.md`.** The v1 detached-deferral that ships in the current
certified tree is TRANSITIONAL and is removed when this lands (pre-sandbox gate).

## The ruling that shapes everything

1. **Recording is observationally neutral.** The recorder never reshapes production behavior —
   no deferral, no joining, no fences, no scheduling of any kind. Stamps and metadata only.
2. **No joining in the target model at all** — not at record, not at replay. Replay executes
   freely; determinism of *judgment* replaces determinism of *execution*.
3. **All intelligence lives at scoring/diff time**, evidence-gated, on the model below.

## The model: two axes

Within one correlation, execution is a **partial order** (fork/join task tree), not a sequence.
The tape stores one accidental linearization; replay produces another. Divergence must be judged
against the partial order plus declared canonicalization — never against the accidental order.

**Axis 1 — structure (who is unordered):** every event carries task **lineage** (bucket id +
fork-point seq; root bucket = main path). `deja::spawn`/`spawn_detached` become pure stamps
(a task-local lineage push) — production-inert, no behavior change ever. The lint plan
(blessed wrapper + clippy deny on raw `tokio::spawn`) remains: it guarantees stamping coverage,
not scheduling.

**Axis 2 — canonicalization (what tolerates reordering) — GENERIC, no fixed law enum:** every
tolerated-reordering semantic is a declared FOLD that reduces a group of observations to a
canonical form before comparison:

```
equivalent(tape_group, replay_group)  ⟺  canon(tape_group) == canon(replay_group)
```

Declared per boundary, additive on `BoundaryDeclaration`, as a TRAIT with shipped presets —
exactly the `recon =` codec pattern, so new semantics are a new impl, never an enum variant or a
scorer change:

```
state_canon = Sequence (default) | Bag | FinalState | AbsentAfter | Project(fields…) | <custom Canon impl>
reply_canon = Sequence (default) | …same options…
```

TWO canons because every boundary has two faces (the INCR trap): INCR's state converges under any
order (`state_canon = Sum`-like fold) but its returned counts are order-carrying
(`reply_canon = Sequence`). Existing hand-rolled cases map 1:1: Rule A = `state_canon=FinalState`;
Rule B = `state_canon=AbsentAfter`; volatile columns = `Project(-modified_at,-last_synced)`;
kind-only error scoring = `reply_canon=Project(kind)`. Peer scope for a group = the typed StateKey
cell (Phase C): events group iff same cell within an unordered region.

## Scoring rules (the whole mechanism)

ONE rule instead of a case table: group conflicting events by (StateKey cell × unordered
region) → apply the declared `state_canon`/`reply_canon` to the tape group and the replay group →
compare canonical forms. Mismatch = blocking divergence. There is no trust step: canonical
equality IS the evidence, so a wrong declaration cannot hide anything (its canon still has to
reproduce the tape's canon — e.g. a lost update fails `FinalState`, an unexpected deletion fails
`AbsentAfter`, exactly the guards Rule A/B enforce today). Default canons are `Sequence`/`Sequence`
(strictest); freedom is earned per declaration.

Rule A + order-swap arm + Rule B + the volatile-column allowlist + kind-only error scoring are
the proven hand-rolled canonicalizers; they become preset `Canon` impls (legacy name-based guards
remain as fallback for undeclared tapes, per the established pattern).

## The residual: control-flow read-races → INCONCLUSIVE-RERUN, never red, never silent

When a read races an unordered writer and the branch taken differs from the tape, replay may need
events the tape never captured — no judge can reconstruct an unrecorded branch. Policy:

- The scorer RECOGNIZES the race signature (diverged read in an unordered conflict whose observed
  row equals a recorded same-row write's pre/post image; HTTP clean; downstream delta confined to
  that branch) and classifies the run **inconclusive_race → auto-rerun**, with the evidence named
  in the report. A flake becomes a labeled retry, not a phantom bug and not a hidden demotion.
- Corpus effect: across many production recordings both interleavings appear naturally; races
  that flake against one tape match another.
- NO deterministic-join escape hatch in the model. (If a debugging session ever wants one, it is
  a throwaway local tool, not a shipped mode.)

## What changes vs today's tree

| piece | today (certified R6) | target |
|---|---|---|
| spawn_detached | defers task to response-end drain (record+replay) | stamp-only (lineage + detached flag) |
| ingress drain | joins detached work at body EOF | REMOVED |
| undeclared-concurrency detector | post-finalization correlated events warn | KEPT (works better with lineage) |
| Rule A / order-swap / Rule B | name+shape guards | preset canonicalizers (declared-first, legacy fallback) |
| occurrence counters | per (correlation, callsite) | per (correlation, bucket, callsite) |
| race flake | killed by deferral | inconclusive_race → rerun |

## Sequencing (user-ratified)

1. **Now:** PR ships with the certified tree as-is (v1 deferral is deja-mode-gated; plain prod
   untouched; certified evidence stays valid).
2. **Pre-sandbox gate (before recording real traffic):** lineage stamping + canonicalization
   fields + `Canon` trait/presets (Rule A/B port as `FinalState`/`AbsentAfter`, volatile and
   kind-only projection as `Project`) + inconclusive_race/rerun policy + REMOVE the drain;
   recertify (self-check + matrix + a forced-race soak).
3. **With Phase C/D:** cell-scoped canonicalization via typed StateKeys; net-state via readback
   certificates; volatile columns become per-column `Project` declarations.

## Acceptance for step 2
- Neutrality proof: recorded tape of a racy workload shows natural interleavings (no drain events)
  and byte-identical scheduling vs non-deja run (timing distributions comparable).
- Determinism-of-judgment proof: N repeated replays of the same racy tape → identical verdicts
  (pass or inconclusive_race), zero flaky reds across the soak.
- Matrix stays 7/7; guards proof: seeded lost-update and unexpected-deletion still BLOCK.
