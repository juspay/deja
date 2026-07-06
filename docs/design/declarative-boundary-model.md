# Declarative Boundary Model — Design Blueprint

**Goal:** the deja library is generic/maintainable/extensible (zero host-specific knowledge); the
Hyperswitch vendor integration is generic, maintainable, **very low footprint**; and because we use a
macro, *everything* (full-mock / execute / fence / RMW) is a **declared enum parameter** on the boundary
macro — the runtime decides nothing by guessing.

## 0. The core inversion

Today the runtime **guesses** an op's semantics from strings: channel from a `boundary`-tag set
(`replay.rs:883`), read/write from method-name verbs (`replay.rs:899`), the state key from the first
string scalar (`lib.rs:1168`), execute-eligibility from a comma-separated env set (`replay.rs:1616`),
miss-vs-present from `null || "Null"` (`replay.rs:1859`). The call site meanwhile **over-declares** replay
mechanics (`replay`/`replay_ok`/`replay_with`) and redundant identity (`component` ×36, `correlation=None`
×18).

**Inversion:** the boundary author declares the intrinsic op semantics **once per wrapper** (~37 places);
the runtime decision becomes a **pure table lookup** over those declared primitives × replay policy, with
**zero strings**. Every name-heuristic is deleted.

## 1. The taxonomy (the enums the macro declares)

```rust
enum Channel { State, Entropy, Egress, Ingress }       // where the effect goes
enum Effect  { Read, Write, ReadModifyWrite, Append, Pure, Opaque }   // what it does to the channel
enum Determinism { Deterministic, Entropic(EntropySource), Volatile } // reconstructability of the value
enum EntropySource { Clock, Id, Rng, Other(&'static str) }
// key: a declared path/closure over the args image → authoritative read_set/write_set
```

- **Channel** replaces `is_state_channel` string-matching. Only `State` is execute-eligible. `Ingress`
  (incoming HTTP) is split from `Egress` (per critic G4 — incoming is the correlation seed/driver, never
  re-executed *or* substituted the same way as an outbound response).
- **Effect** replaces `is_read_op` verb-matching. `Read` covers `count`/`filter`/`aggregate` that the verb
  list can't. `ReadModifyWrite` is the keystone (INCR/SETNX/SADD/`get_or_create`). **`Append`** (critic B2-adjacent)
  = append-only log (XADD: return is an assigned id, never re-read for diff). **`Opaque`** (critic B1) =
  arbitrary read+write whose return is unrelated to what it mutated (EVAL/Lua) → always `Substitute`, never
  execute. `Pure` = deterministic given args.
- **Determinism::Volatile** (critic B3) = `State`-channel but **time-decaying** (`get_ttl`, `set_expire_at`):
  value changes with wall-clock, so it must be `Substitute`d even under SelectiveExecute, never executed
  (else it diverges every run from elapsed time).
- **key** drives a real `read_set`/`write_set` (fixes the empty-read_set blocker) and supports multi-key
  (MGET, composite db keys).

### Effect is a constant OR a closure over args (critic's #1 refinement — load-bearing)

Several real vendor methods carry their effect in a **runtime arg**, not their identity:
`stream_read_with_options` is `Read` when `group: None`, `ReadModifyWrite` when `Some` (XREADGROUP advances
the PEL); `set_key_if_not_exists_and_get_value` returns the *prior* value on the "already present" branch.
A static attribute can't express this. So:

```rust
#[deja::redis(effect = Read, key = "key")]                              // 90% — constant
#[deja::redis(effect = |a| if a.group.is_some() { Rmw } else { Read })] // polymorphic, no method split
```

Without this, those ops are mis-declared (correctness bug) or forced into new wrapper fns (footprint
regression). The closure keeps the matrix pure AND the table total AND the footprint at ~37 points.

### Presets — `deja::id`/`time`/`http` stay one word

| Macro | Channel | Effect | Determinism |
|---|---|---|---|
| `deja::time` | Entropy | Pure | Entropic(Clock) |
| `deja::id` | Entropy | Pure | Entropic(Id) |
| `deja::http(outgoing)` | Egress | Read | Deterministic |
| `deja::http(incoming)` | Ingress | Read | Deterministic |
| `deja::redis` / `deja::db` | State | *(declared per method)* | Deterministic / Volatile |

## 2. The decision matrix (the entire runtime decision)

`strategy(Channel, Effect, Determinism, Policy) -> Strategy` — pure, no strings.
Strategies: `Substitute` (serve recorded, never touch live); `SeedAndExecute` (seed pre-image, run real op,
diff — catches **total** derivatives); `SeedPostState` (serve recorded return + seed post-state, no
pre-image — full-mock for RMW).

| Channel | Effect | AllLookup | SelectiveExecute |
|---|---|---|---|
| State | Read | Substitute | **SeedAndExecute** |
| State | Write | Substitute | **SeedAndExecute** |
| State | ReadModifyWrite | Substitute | **⟨DEFAULT DECISION — see §2.1⟩** |
| State | Append | Substitute | SeedPostState (serve recorded id, seed entry; never double-append) |
| State | Pure | Substitute | Substitute |
| State (Determinism=Volatile) | any | Substitute | **Substitute** (time-decaying; never execute) |
| State | Opaque | Substitute | **Substitute** (EVAL; opt-in `execute=true` only) |
| Entropy | any | Substitute | Substitute |
| Egress | any | Substitute | Substitute |
| Ingress | any | (drive) | (drive) |

### 2.1 The one genuine product decision — the RMW default under SelectiveExecute

(Critic G1 — this **reverses** the earlier "fence = SeedPostState is full-mock done right" framing.)

- **`SeedPostState` default** (the user's return-as-post-state idea): no pre-image, no double-apply, **no
  false positives** on non-deterministic RMW (locks, counters that legitimately differ). BUT it
  **masks total derivatives flowing through the RMW** — the exact failure class this project exists to
  catch. Also breaks for SETNX-get whose return may be the *prior* value, not the post-state (critic G2).
- **`SeedAndExecute` default**: **catches** total derivatives through RMW (aligns with the project thesis).
  BUT needs the **pre-image** (which we don't reliably capture for RMW today — needs join-to-prior-read or
  a StateProbe) and risks **false positives** on genuinely non-deterministic RMW.

Either way it is **per-op overridable** (`strategy = SeedPostState | SeedAndExecute`), matching "everything
is a tweaking parameter." The decision is only: which is the DEFAULT for an undeclared RMW.

## 3. Vendor footprint (the low-footprint win)

~37 declaration points behind hundreds–thousands of untouched call sites (`date_time::now` = 507 sites).
**0 call-site changes.** Per-wrapper collapse:
- `component` ×36 → 0 (defaults to `module_path!()`).
- `correlation=None` ×18 → 0 (defaults to ambient `current_correlation_id()`).
- `replay`/`replay_ok`/`replay_with` (×15/×19/×1) → 0 — replaced by one `effect=` word (+ optional
  `replay_codec=`, with `recon=` kept only as a legacy alias, for the one non-trivial HTTP rebuild).
- `deja::id`/`time`/`http` stay one word.

```rust
// redis GET:  #[deja::boundary(boundary="redis", component="...", correlation=None, replay_ok)]
//      →      #[deja::redis(effect = Read, key = "key")]
// redis INCR: (uninstrumented today)  →  #[deja::redis(effect = ReadModifyWrite, key = "key")]
// nonce:      #[deja::boundary(boundary="id", operation="GcmAes256::nonce", replay_ok)]  →  #[deja::id]
```

## 4. Runtime simplification (heuristics deleted)

| Deleted | Location | Replaced by |
|---|---|---|
| `is_state_channel` | replay.rs:883 | read declared `Channel` |
| `is_read_op` | replay.rs:899 | read declared `Effect` |
| `extract_primary_state_key` | lib.rs:1168 | declared `key` path |
| `primary_state_key` (mirror) | replay.rs:2095 | same declared `key` (one source) |
| `entropy_source` match | lib.rs:1324 | `Determinism::Entropic(src)` |
| `is_miss_result` (`null/"Null"`) | replay.rs:1859 | declared `Option` shape → structured miss |
| `DEJA_EXECUTE_OPS` name scoping | replay.rs:1616 | per-op `execute=true` / the matrix |

The library ends with **zero host-specific strings**. `canonical_args_hash` stays (sound; demoted from a
resolution key to a value-diff signal). **Thin back-compat fallback:** an undeclared boundary → safe
`Substitute` everywhere (can never wrongly execute) → old tapes replay exactly as today's AllLookup → the
green demo stays green during migration.

## 5. Capture / fidelity tie-in

- declared `key` → authoritative `read_set`/`write_set` (multi-key ok) — fixes the empty-read_set blocker.
- declared `Effect` → correct seeding (`build_seed_plan`/`join_pre_images` read it, don't re-guess).
- args captured **losslessly via serde by default** (not `value::debug`); a real `recon` stamped; structured
  miss replaces the `"Null"` sniff. Per-key miss for multi-key (critic G3).

## 6. Migration (ordered, demo stays green)

1. **deja:** add the enums + `key` to `BoundarySpec`/descriptor + `SemanticEvent` (additive `#[serde(default)]`).
2. **deja-derive:** add `channel`/`effect`/`key`/`determinism`/`strategy` params + `#[deja::key]` arg-attr +
   effect-closure; wire presets; keep `replay*` as **deprecated aliases** that desugar.
3. **deja-record:** add `strategy(channel, effect, determinism, policy)` matrix; make `execute_mode`,
   `build_seed_plan`, `join_pre_images`, `finish` read declared fields; keep heuristics ONLY in the
   undeclared-fallback path.
4. **vendor (~37 wrappers):** migrate boundary-by-boundary (demo eu_settlement + redis leaves first).
5. **add the missing redis boundaries** (INCR/SETNX/SADD/HINCRBY=RMW; SCAN/HSCAN=Read; XADD=Append;
   XREADGROUP=effect-closure; EVAL=Opaque; get_ttl=Volatile) — all arrive correct via the matrix.
6. **delete the heuristics** once all wrappers migrated; drop `DEJA_EXECUTE_OPS`. Verify green at each step.

## 7. Extensibility

New behavior = one enum variant + one matrix row. New boundary = one attribute. Worked: XADD = `Append`
variant + one row + `#[deja::redis(effect=Append, key="stream")]`. No host strings, no runtime branch.

## Decisions (LOCKED)
1. **RMW under SelectiveExecute = REQUIRE EXPLICIT DECLARATION** (user's choice). No global default: a boundary
   declaring `effect = ReadModifyWrite` MUST also declare `strategy = SeedPostState | SeedAndExecute`, else the macro
   emits a COMPILE ERROR. Safe-by-construction — adding an RMW op forces choosing how it reconstructs.
2. (Decided, technical) adopt: `Effect` as constant-or-closure; `Opaque` for EVAL; `Append` for XADD;
   `Determinism::Volatile` for TTL; `Ingress` split from `Egress`; per-key miss.
