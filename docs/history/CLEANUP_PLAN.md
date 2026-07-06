# Cleanup & publish-readiness record (2026-06)

> **Archived summary.** Before publication this repository went through a full
> audited cleanup, executed against a 200+-finding review and validated
> behaviorally: the demo self-replay and the cross-version matrix reproduced
> their pre-cleanup baselines exactly (self/benign PASS 9/9; the planted real
> change reproduced its exact divergence signature).

What was done, in brief:

- **Repo hygiene**: ~27.8k accidentally tracked files removed from the index
  (build artifacts, run recordings, vendored snapshots); recordings carry
  unmasked credentials by design and are never tracked.
- **History**: the published repository starts from a fresh export
  (`scripts/export-public.sh`) — the development clone's history is not
  publishable.
- **Era removal**: the set-aside syscall-preload and tokio-patch tracks were
  deleted (crates, fixtures, demo cluster); their design docs are preserved in
  this directory.
- **Vendor integration branch**: rebuilt on the pristine upstream tag so the
  diff is pure, feature-gated instrumentation; un-gated default-build behavior
  changes were eliminated (config overlays, gated serde derives).
- **Quality gates**: fmt + clippy `-D warnings` + tests made green and enforced
  in CI; MIT license and crate metadata added; docs triaged with the current
  architecture reference at `docs/DEJA_RECORDING_ARCHITECTURE.md`.

The full operational plan (internal paths, branch topology, secret-handling
steps) is deliberately not published.
