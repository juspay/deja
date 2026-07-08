> **Archived.** This document records the full-mock replay PRD as executed; some locked decisions were later superseded (e.g. structured db error replay shipped in V1). It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# PRD — Full-Mock Replay Substitution (deja V1 completion)

## Problem

The deja replay harness records a real Hyperswitch workload through the
production pipeline (HS → Kafka → Vector → MinIO) and renders a lookup table, but
**replay never substitutes recorded values**. Every instrumented boundary wrapper
(`record_query_async`, `record_boundary_*`, `record_id_generation`) is
record-only — it always executes the real call and merely *observes* via the
hook. `try_replay` / `try_replay_with_context` have **zero production callers**
(only the hook's own delegation and `#[cfg(test)]` tests).

Consequence: in `DEJA_MODE=replay` the candidate re-runs live — fresh uuids,
fresh timestamps, real pg/redis, real Stripe — so byte-exact self-replay diverges
across the board (`resolved_by_rank: {}`, 282 omitted side-effects, signup 400 on
an already-mutated DB).

Recording itself is now correct and complete (hook unification landed: all 327
events, one sequence counter, full boundary coverage).

## Goal

Wire the existing substitution machinery into the boundaries so that, in replay
mode, each boundary returns its recorded value instead of executing — making
byte-exact self-replay achievable.

## Mechanism (per boundary)

The hook side already works: `LookupTableHook::try_replay_with_context(query)`
computes the rank-aware key, emits the `ObservedCall` for scoring, and returns
`Some(entry.result)` on a hit / `None` on a miss. What each wrapper must add:

- **Replay branch:** resolve the active hook; call `try_replay_with_context`. On
  `Some(result_json)` → `serde_json::from_value::<T>(result_json)` and return it,
  skipping the real call. On `None` → execute real (the miss is already scored).
- **Lossless record:** store `result = serde_json::to_value(&output)` (not the
  current coarse `Debug` string), so replay can reconstruct `T`.

This requires `T: Serialize + DeserializeOwned` at the boundary.

## Hard constraint / risk

- **`masking::Secret<T>` is lossy by design** — it serializes as `"*** … ***"`.
  Domain/db models carrying secrets cannot round-trip; their substituted value
  loses the secret. (The recording is already lossy here.) Byte-exact for
  secret-bearing values may be unreachable without unmasking at the deja
  serialization boundary — to be assessed in Phase 3, not assumed.
- **Not all HS return types impl `Serialize + DeserializeOwned`** (error types,
  wrappers). Where they don't, that boundary can't be typed-substituted as-is.
- **Error results:** reconstructing a recorded `Err(E)` into the concrete error
  type is generally infeasible; V1 substitutes `Ok` values and treats recorded
  errors as misses (scored), unless a boundary's error type round-trips.

## Phases (each validated by re-running the demo and watching the scorecard)

- **Phase 0 — shared substitution helper.** Add `replay_or_execute` /
  `replay_or_execute_async` in deja-record: resolve hook → `try_replay_with_context`
  → deserialize-or-execute. Switch result recording to lossless
  (`to_value(&output)`) behind the same helper. No behavior change yet for
  unwired boundaries.
- **Phase 1 — generators (uuid + time).** Wire substitution in
  `router_env::request_id` (uuid) and the time boundary (`date_time::now`).
  Simple types, no secrets. Expected: timestamp/uuid body-diffs vanish;
  `resolved_by_rank` starts populating.
- **Phase 2 — http_outgoing (Stripe).** Record/replay the HTTP response
  (status + headers + body bytes) verbatim; mock the external call. Removes the
  largest source of non-determinism (real Stripe). Expected: connector-dependent
  status mismatches resolve.
- **Phase 3 — db (diesel_models `record_query_async`).** Generic `T`
  substitution. Assess serde + masking feasibility per return type; substitute
  where round-trippable, document where not. Reset of mutated state may be needed
  alongside.
- **Phase 4 — redis, crypto, remaining boundaries.** Same pattern.

## Validation

After each phase, re-run `demo/run-deja-demo.sh --iterations 1 --keep` and record:
`resolved_by_rank` distribution, `side_effect_divergences`, `http_*_mismatches`,
`matched_correlations`. Success = monotonic decrease in divergence; goal = a
genuine byte-exact PASS on self-replay (empty allowlist), or a documented,
understood residual (e.g., masking) with the allowlist narrowed to exactly that.

## Out of scope (V2)

Seeded/proxy replay, tiered-miss synthesis (`synthesized` / `real_impl_will_fail`
remain `false`), cross-candidate replay.

## Locked decisions (user)

- **Correlation anchor:** replay router runs `IdReuse::UseIncoming`; the kernel
  injects `x-request-id = recorded correlation_id`. The recorded request_id is
  the deterministic seed of the controlled environment. id_generation replay is
  a backstop if the header path ever fails.
- **Tokio correlation bridge (background `tokio::spawn` tasks): DEFERRED to V2.**
  UseIncoming handles request-scoped correlation; the ~35 background events stay
  uncorrelated for now (use `deja::spawn` wrappers later).
- **Error arms: skip in V1** — substitute only `Ok` results; a recorded `Err`
  (or a non-`DeserializeOwned` error type) falls through to live execution.
- **Masking: record UNMASKED for fidelity** — peek/expose `Secret<T>` and record
  decrypted/ciphertext for `Encryption` so the lookup table round-trips
  byte-exact. ⚠️ Recordings will then contain plaintext secrets; sensitivity
  (encryption-at-rest / redaction policy of artifacts) is deferred, NOT solved.
- **http in & out:** record AND replay BOTH request and response.

## Progress

- ✅ Anchor — router `UseIncoming` (lib.rs) + kernel `x-request-id` injection.
  Adversarially verified sound. Compiles.
- ✅ Infra — `deja_record::replay_boundary`, `deja::value::result_serialize`
  (lossless), re-exported in `__private`. Compiles.
- ✅ Macro `replay` flag — `deja-derive` `instrument.rs`: opt-in replay branch +
  lossless recording; non-replay path unchanged. Compiles.
- ✅ Boundaries wired (non-Result, this slice): `date_time::now`
  (`PrimitiveDateTime`), `now_unix_timestamp` (`i64`), 5× `generate_id*`
  (`String`) — all serde round-trip proven by the compiler. id_generation/uuid
  (manual) done earlier.
- ✅ VALIDATED (run rec-1780950973): substitution fires — `resolved_by_rank.rank_4=25`
  (was `{}`), time/id resolving (observed: time 17, id 7, id_generation 10),
  divergences 282→258. The signup 500 ("Organization id already exists") *proves*
  ids match byte-for-byte (collide with the record run's row in the shared pg).
  `matched_correlations` still 0 — gated on db/redis substitution (next).
- ✅ Shared **3-mode macro**: `replay` (direct), `replay_ok` (Result Ok-only —
  extract Ok type via `first_generic_arg`, never touch the non-serde error),
  `replay_with = expr` (custom reconstruction). + `value::result_serialize_ok`.
- ✅ **http_outgoing** wired: `replay_with = replay_response(&__deja_recorded).map(Ok)`
  rebuilds `reqwest::Response` (status+headers+body) from the recording — Stripe
  served from the lookup table, no network. Records headers too now.
- ✅ **db substitution**: `record_query_async` now generic over `(R, E)`, Ok-only —
  on replay serves the recorded row(s) from the lookup table and SKIPS the query
  (so replay never touches pg → the id-collision 500s should vanish). Lossless Ok
  recording via `result_serialize_ok`. Added `R: Serialize + DeserializeOwned` to
  the 12 `diesel_models` generic helpers + `#[derive(Serialize, Deserialize)]` to
  14 models (User, Profile, Organization, Role, UserRole, Theme, …). ZERO field
  cascade. Full router compiles. Masking: Secret fields reconstruct as `"***"` —
  fine for byte-exact responses (which mask them anyway).
- ⏭ NEXT: **validate** (fresh record→replay; expect `matched_correlations` to
  climb, `side_effect_divergences` to fall sharply, collision 500s gone) → then
  **redis** (`replay_ok` on 20 callsites + reply-enum derives; generic
  `get_key<V>` needs `V: DeserializeOwned`) → lock + time-`Result`.
