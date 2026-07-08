# Upstream PR finalization — current gate checklist

Status as of 2026-07-03: **do not push the existing PR branch as-is**. The
current pinned root/vendor trees have three clean isolated-parallel active-default
matrix proofs (`run-deja-parallel.sh`): `1783099999`, `1783111111`, and
`1783133333`. Together they cover the detached-task/read-race,
artifact-kind-coverage, Kafka marker-attribution, Redis attribution,
request-id Superposition sampling, and vendor G2/typed-image changes. The
Hyperswitch PR branch still needs a fresh public Deja rev, dependency repin,
final vendor gates after the repin, and approval for the public push/update.

## Current proof artifacts

- Self replay proof:
  - recording `rec-1783042990`
  - replay run `run-18bea48955d9d848`
  - scorecard: `pass=true`, `matched=9/9`, `side_effect_divergences=0`,
    `value_divergences=0`, `http_body_mismatches=0`
  - non-blocking warning: `1 idempotent-delete warning`
- Reduced true-parallel proof:
  - recording `rec-1783044235`
  - `self` → `run-18bea65f1f2c683a`, PASS, `matched=9/9`, `sidefx=0`
  - `benign` → `run-18bea65f20558f3a`, PASS, `matched=9/9`, `sidefx=0`
  - `real` → `run-18bea65f21978ba8`, DIVERGE caught, `matched=8/9`,
    `sidefx=1`
  - each replay used its own sandbox (`deja-run-*` compose project, pg, redis,
    router image, and host replay port) against the same recording.
- Focused local proof after the detached EOF-drain/read-race change:
  - `cargo test -p deja-record --lib detached` passed (`5 passed`).
  - `cargo test -p deja-context` passed (`4 passed`).
  - `cargo test -p router_env --features deja --lib` passed (`4 passed`), including
    the `drain_only_body_waits_for_detached_work_at_eof` regression test.
  - `cargo check -p router --features deja,v1` passed after the request-id body
    wrapper change.
- Focused local proof after the artifact/Kafka/Redis/seed-certificate changes:
  - `cargo test -p deja-orchestrator artifact_kind_constraints_cover_lifecycle_registrations`
    passed (`1 passed`).
  - `cargo test -p deja-orchestrator seed_certificate --lib` passed (`4 passed`).
  - `cargo test -p deja --test boundary_macro boundary_macro_records_sync_function`
    passed (`1 passed`).
  - `cargo test -p deja-orchestrator seed_certificate_db_readback_sql_separates_full_row_and_key_match_predicates --lib`
    passed (`1 passed`).
  - `cargo test -p deja-orchestrator seed_db_renders_encrypted_bytea_key_as_hex_literal_from_metadata --lib`
    passed (`1 passed`).
  - `cargo check -p redis_interface --features deja` passed with only the existing
    `num-bigint-dig` future-incompat warning.
  - `cargo test -p router --lib --features deja,v1 marker_envelope_serializes_sink_marker_shape`
    passed (`1 passed`).
  - `cargo check -p router --features deja,v1` passed with only existing
    third-party future-incompat warnings.
- Clean active-default matrix proof after archiving stale `eu-overcharge`:
  - run tag `1783099999`, recording `rec-1783099999`
  - matrix summary reached with every active default candidate marked `OK`:
    - `self` → `run-18bebdb086be38e1`, PASS, `matched=9/9`,
      `side_effect_divergences=0`, `value_divergences=0`, `http_body_mismatches=0`
    - `benign` → `run-18bebe8d72bbbc13`, PASS, `matched=9/9`,
      `side_effect_divergences=0`, `value_divergences=0`, `http_body_mismatches=0`
    - `real` → `run-18bebf6460f4174e`, DIVERGE caught, `matched=8/9`,
      `side_effect_divergences=1`, `value_divergences=1`
    - `earlier-fork` → `run-18bec0447f851dfe`, DIVERGE caught, `matched=8/9`,
      `side_effect_divergences=1`, `value_divergences=1`
    - `dropped-write` → `run-18bec1440eb34487`, DIVERGE caught, `matched=4/9`,
      `side_effect_divergences=10`, `omitted_calls=10`
    - `response-only` → `run-18bec247a94e29ea`, DIVERGE caught, `matched=6/9`,
      `http_body_mismatches=2`, `side_effect_divergences=1`, `value_divergences=1`
    - `extra-call` → `run-18bec3170b3d47fc`, DIVERGE caught, `matched=8/9`,
      `side_effect_divergences=1`, `novel_calls=1`
  - all seven scorecards include `1 idempotent-delete warning`; the scorecard
    verdict treats this as non-blocking and it is not counted in summary
    `side_effect_divergences` or pass/fail.
  - summary evidence: `/tmp/deja-matrix-1783099999.log:1804-1818`.
  - scorecards are under `demo/harness-state/1783099999/runs/*.scorecard.json`.
- Isolated-parallel active-default repeat proof #2:
  - run tag `1783111111`, recording `rec-1783111111`
  - summary evidence: `/tmp/deja-matrix-1783111111.log:1805-1844`
  - scorecards are under `demo/harness-state/1783111111/runs/*.scorecard.json`
  - outcome matched the `1783099999` shape: `self`/`benign` passed `9/9`, and
    `real`, `earlier-fork`, `dropped-write`, `response-only`, and `extra-call`
    were all caught as expected.
- Isolated-parallel active-default repeat proof #3:
  - run tag `1783133333`, recording `rec-1783133333`
  - summary evidence: `/tmp/deja-matrix-1783133333.log:1801-1839`
  - scorecards are under `demo/harness-state/1783133333/runs/*.scorecard.json`
  - outcome matched the same active-default shape: `self`/`benign` passed `9/9`,
    and all five active divergence candidates were caught as expected.
- Excluded failed attempt:
  - run tag `1783122222` failed during baseline record workload step 4/6 before
    any replay matrix or scorecards existed, so it is not counted toward or
    against the replay proof.
- `eu-overcharge` remains archived/stale, not part of the active default matrix,
  until a new current-code transitive read→write candidate is rebased.

## Current vendor branch evidence

- Vendor branch: `deja-lean`.
- Recent Deja-specific vendor commits present:
  - `0115927d49 feat(deja): migrate boundary codecs and redis isolation`
  - `4919f2da58 feat(deja): record typed db row images`
  - `7083fe3b61 feat(deja): drain detached payment tasks`
- Current local vendor curation commit `46926e570f` is intentionally scoped to PR
  curation:
  - `crates/router/src/services/kafka/deja_record_sink.rs` uses
    `deja::CURRENT_EVENT_SCHEMA_VERSION` in the envelope fixture and includes
    `code` provenance on sink-marker envelopes, matching event envelopes.
  - `crates/redis_interface/src/module/fred/commands.rs` and
    `crates/redis_interface/src/module/redis_rs/commands.rs` add explicit MGET
    read-set/key attribution and SADD write attribution. MGET result capture
    remains ok/error-only rather than a fully lossless `ReplayCodec` capture.
  - `crates/router_env/src/request_id.rs`, `crates/router/src/lib.rs`, and
    `crates/router/src/consts.rs` wire request-id Superposition sampling through
    Deja's per-correlation recording gate.
  - `crates/router_env/src/request_id.rs` also wraps replay/sampled-out response
    bodies so queued detached work is drained at stream EOF instead of racing the
    next main-path read.
  - `docker-compose.deja.yml` removes stale `DEJA_POLICY` /
    `DEJA_EXECUTE_OPS` replay-policy env and remains an intentional opt-in
    local-dev record-transport overlay.
  - The generated `crates/diesel_models/src/schema.rs` collapsed-format diff is
    still present in the merge-base PR diff. Attempting to replace it with
    current `origin/main`'s schema made `diesel_models` fail to compile because
    this branch's model structs still expect the generated schema shape carried
    by the branch. Compile correctness wins here; remove this noise only via a
    proper branch refresh/rebase plus schema regeneration, not by hand-editing the
    generated file.

## Isolated rebase preflight

- Non-mutating rebase worktree created at
  `vendor/hyperswitch-deja-rebased-9d7bf4a1e4`.
- Branch/ref for certification: `deja-lean-rebased-9d7bf4a1e4` at
  `cca5c3c35b`, rebased onto `origin/main` (`9d7bf4a1e4`).
- Rebase completed after porting Deja Redis attribution/isolation through the
  current Redis backend split (`crates/redis_interface/src/module/fred/commands.rs`)
  and preserving the DB request-id routing hook.
- Diff against current `origin/main`: `59 files changed, 4159 insertions(+),
  330 deletions(-)`.
- The generated `crates/diesel_models/src/schema.rs` diff is gone in the rebased
  tree.
- Focused compile check passed explicitly against the rebased-tree manifest:
  `cargo check --manifest-path vendor/hyperswitch-deja-rebased-9d7bf4a1e4/Cargo.toml -p redis_interface --no-default-features --features deja,fred`
  (warnings only: unused qualifications/dead-code/future-incompat).
- Claude was handed the compileable worktree/ref for certification; the prior
  `1783099999` matrix certified the old base, so the rebased tree still needs at
  least self-check and preferably the full active-default matrix before becoming
  the PR candidate.

## Local cleanups already applied

- Removed stale global replay-policy env from
  `vendor/hyperswitch-deja-clean/docker-compose.deja.yml`; replay routing is now
  documented there as per-boundary event metadata, not `DEJA_POLICY` /
  `DEJA_EXECUTE_OPS`.
- Updated the vendor Kafka record-sink test to use
  `deja::CURRENT_EVENT_SCHEMA_VERSION` instead of a stale hard-coded schema `1`.
- Re-exported `CURRENT_EVENT_SCHEMA_VERSION` from the `deja` facade so downstream
  test fixtures can track the public event schema.
- Fixed the replay visualizer so:
  - scorecard verdicts are authoritative when present;
  - skipped/not-driven requests are not treated as divergence cards;
  - concurrent replay runs render run-scoped HTML
    (`replay-visualization-<run_id>.html`) instead of racing on one shared file.
- Added real Superposition-backed Deja ingress sampling:
  `RequestIdentifier` now resolves `deja_record_enabled` in record mode, targets
  OpenFeature rollout by request/correlation id, sends only method/path context,
  defaults quietly to record when Superposition is disabled, and pushes the
  resolved boolean into Deja's per-correlation gate before request handling.
  The Deja context now propagates sampled-out decisions across task snapshots so
  spawned work cannot start recording after ingress teardown clears the registry.
- Added artifact-kind migration coverage:
  `artifact_kind_constraints_cover_lifecycle_registrations` statically checks
  every lifecycle `ctx.artifact(...)` kind against the migration constraint set
  and asserts artifact-kind migrations are monotonic.
- Added replay seed/readback certificates:
  `seed_certificate` artifacts record every planned seed precondition, whether it
  materialized, and DB/Redis readback status; migration `0005` admits the new
  artifact kind.
- Added Kafka sink-marker code provenance so loss-accounting marker envelopes
  carry the same `code.sha` / `code.deja_version` attribution as event envelopes.
  The broad G2 `ReplayCodec` plan is still only partially complete; HTTP egress
  has a custom codec, Redis/id/time/crypto use `replay_codec = ...`, and the DB
  `DbResultCodec`/dispatch fold remains a follow-up rather than part of this
  marker fix.
- Resolved the transport-overlay scope decision: keep `docker-compose.deja.yml`
  and `config/vector.deja.yaml` in the Hyperswitch PR as an opt-in local-dev
  Kafka → Vector → object-store proof path. Production Vector/IaC/runner
  deployment remains out-of-tree.
- Hardened `scripts/export-public.sh` so it refuses dirty or untracked root
  state before creating the single-commit public export; this prevents publishing
  a HEAD snapshot that silently omits freshly validated local changes.
- Closed the detached-task DSL gap: vendor request-path spawns now route through
  `router_env::task::{spawn, spawn_detached}`, raw Tokio spawn is denied by
  workspace Clippy with documented infra/test exceptions, and payment-response
  post-response bookkeeping uses the blessed detached wrapper.
- Added replay-side finalizer observability for undeclared-concurrency scoring:
  `ObservedCall` now carries replay `timestamp_ns`, `end_timestamp_ns`, and
  `detached`; `LookupTableHook::record` emits only `http_incoming` finalizer
  sentinels into the observed stream; the scorer consumes those sentinels only as
  timing markers and keeps the warning non-blocking.
- Focused closeout verification passed after these changes:
  `cargo test -p deja-record --lib`, `cargo test -p deja-orchestrator --lib`,
  `cargo test -p router_env task::tests`,
  `cargo test -p router_env --features deja task::tests`,
  `cargo clippy -p router_env --all-targets -- -D clippy::disallowed_methods`,
  `cargo clippy -p router_env --features deja --all-targets -- -A warnings -D clippy::disallowed_methods`,
  a touched-crate raw-spawn denial sweep over router/hyperswitch_interfaces/scheduler/drainer/storage_impl/redis_interface/test_utils,
  `cargo check -p router`, and `cargo check -p router --features deja,v1`.
- Planned the first hosted-sandbox seams in `docs/design/replay-pipeline.md`:
  explicit hosted orchestrator config/env, a single API auth middleware boundary
  with runner service-token scoping, and manual replay trigger conveniences
  (dashboard prefill, CLI/curl wrapper, copyable PR-review snippet) without adding
  webhook auto-gates to v1.

## Remaining blockers before a credible PR refresh

1. **Repeat active-default matrix proof is complete for the isolated-parallel path.**
   - Evidence: `1783099999`, `1783111111`, and `1783133333` each completed
     `run-deja-parallel.sh --iterations 1` with `self` and `benign` passing 9/9
     and all five active regression candidates caught.
   - This is not a sequential `run-deja-matrix.sh` proof; keep labels precise if
     upstream asks for the sequential script specifically.

2. **Fresh public Deja rev is published.**
   - Exported from the current root after the `replay_codec`/typed-row-image
     cleanup, not from the old `maverox/deja-lib` snapshot.
   - Published to the canonical public repo `https://github.com/juspay/deja`.
   - Pushed commit SHA for Hyperswitch git-dep repin:
     `5b2e21d9e0cc78cf225a75dd09528745163cdb80`.

3. **Repin every vendor `deja` path dependency to that public git rev.**
   The current local vendor tree is intentionally path-based for development.
   Before PR refresh, update all affected manifests:
   - `crates/common_utils/Cargo.toml`
   - `crates/diesel_models/Cargo.toml`
   - `crates/external_services/Cargo.toml`
   - `crates/hyperswitch_domain_models/Cargo.toml`
   - `crates/redis_interface/Cargo.toml`
   - `crates/router/Cargo.toml`
   - `crates/router_env/Cargo.toml`
   - `crates/storage_impl/Cargo.toml`

   Target shape:

   ```toml
   deja = { git = "https://github.com/juspay/deja", rev = "<pushed-sha>", optional = true, default-features = false }
   ```

   Preserve any crate-specific feature/default-feature settings while changing
   only the source from `path` to `git` + `rev`.

4. **PR body draft/live description has been refreshed.**
   - `docs/PR_BODY.md` now describes the `juspay/deja` dependency direction,
     request-id Superposition sampling in this PR, DB typed row images, Redis
     attribution, and the local-dev Kafka/Vector/S3 overlay scope.
   - GitHub PR #12754's body was updated from this draft on 2026-07-04. Re-audit
     the dependency/codegen impact after the git-dep repin before making any
     stronger default-build claim or marking the draft ready for review.
   - Keep the current scope statement: request-id Superposition sampling is in
     this PR; merchant-scoped sampling remains a follow-up unless upstream
     requires it. Keep the current transport statement: `docker-compose.deja.yml`
     and `config/vector.deja.yaml` are included as local-dev proof/supporting
     transport, not production deployment ownership.

5. **Run the final vendor gates after the repin.**
   - `cargo check -p router`
   - `cargo check -p router --features deja,v1`
   - targeted Kafka sink tests:
     `cargo test -p router --features deja,v1 envelope_serializes_artifact_record_v2_shape`
     and
     `cargo test -p router --features deja,v1 marker_envelope_serializes_sink_marker_shape`
   - one Kafka → Vector → object-store recording smoke for the included
     transport overlay.

6. **Rotate external demo credentials before any public push.**
   Local demos used gitignored `.env` / `harness-state` data, but rotate anyway
   before publishing or sharing artifacts.

## Push sequence once the blockers are cleared

1. Export/publish the current Deja root to the agreed public repo.
2. Capture the public commit SHA.
3. Repin the eight vendor manifests from `path` to the public `git` + `rev`.
4. Rebuild/check the vendor branch with and without `--features deja,v1`, plus
   the Kafka → Vector → object-store smoke for the included overlay.
5. Push the refreshed Hyperswitch branch to the fork.
6. Update PR #12754 or open a replacement draft with the refreshed body and proof
   artifacts above.
