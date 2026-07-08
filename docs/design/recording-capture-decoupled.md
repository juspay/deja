# Recording-Side Design (FINAL): The `BoundaryCrossing` Capture

**Status:** final design for the Hyperswitch `#[deja::boundary]` PR, reconciled against three adversarial critiques (future-replay-modes, forward-compat/versioning/migration, decoupling-purity).
**Scope:** the RECORDING side only. Replay/analysis are consumers, named only to prove decoupling.
**Guiding principle (adopted):** *recording captures raw observations; replay performs all interpretation.* The artifact is a faithful, versioned, self-describing log of each boundary crossing, annotated with stable call-site identity. Every decision — channel classification, lookup-vs-execute, divergence scoring, seeding, latency budgets, distribution-fitting — is deferred to consumers.
**Hard constraints preserved:** the live record path must not change observable behavior, and the matrix / total-derivative catch (`ValueDiverged` via args-free pairing, recent commits `41c0590`/`e37038a`/`aaf7819`) must keep working.

---

## 1. Decoupling thesis

### 1.1 The split

A boundary crossing is `imp(x) ≡ pur(x, h)` (handler model, project memory). Recording's only job is to **capture `h` as observed** at the crossing: the inputs `x`, the produced outputs, the side-effect images, the raw timing/lineage, and a stable address for the crossing. Every *decision* about what `h` means belongs to replay/analysis.

The test of correctness for this split: **a new replay or analysis mode must require ZERO re-recording.** If adding "classify Redis SCAN as Egress," "assert p99 latency," "fit a distribution over the entropy draws," or "diff panic messages" forces a re-record, the recorder leaked interpretation into capture (or under-captured the raw observation).

A second, equally binding test surfaced by the decoupling critique: **removing every replay hook from the codebase must change the macro's emitted tokens by ZERO and the macro must name ZERO replay-only operations.** The v1 `match decide()` failed this test — its three arms (`Substitute`/`ExecuteAndShadow`/`Execute`) *were* the replay taxonomy, and the macro still named `shadow_observe`, `from_value` reconstruction, and "suppress normal record." This is fixed in Section 3 by collapsing to a single closure-passing seam that owns all control flow internally.

### 1.2 What must LEAVE the macro / recording

The reader survey found concrete places where replay interpretation is baked into the record path. Each moves to a consumer:

| Leaked knowledge | Currently at | Where it belongs |
|---|---|---|
| **Result fidelity chosen by replay intent** (`result_serialize`/`result_serialize_ok`/`result_debug` selected by `replay`/`replay_ok`) | `instrument.rs:126-138` | Recording is **always lossless** (or best-available); fidelity is a captured *fact* (`recon`), never a replay-driven choice. |
| **`any_replay` gates the entire macro output shape** | `instrument.rs:43, 214-233` | The macro emits **one** shape unconditionally; one `dispatch()` closure-seam (Section 3) owns all replay branching at runtime. |
| **Replay-lookup + early-return + deserialize-or-fall-through (`#reconstruct`)** inlined into the body | `instrument.rs:166-200, 217-228` | The deserialize-and-return policy AND the fall-through-to-live-on-Err policy are replay concerns; the macro hands a typed result-extractor and a run-closure to the seam, which decides. |
| **Execute/shadow arm woven into the record path** (peek baseline → run real → shadow-observe → suppress normal record) | `instrument.rs:245-336`; delegate twin `recordable.rs:286-356` | The run/skip/shadow/record lifecycle lives **entirely inside** `dispatch()`; the macro never names `shadow_observe` and never branches on it. |
| **`is_state_channel` / `is_read_op`** channel & read/write verdicts | `replay.rs:866-892` (recomputed at replay — correct location) BUT the recorder ships nothing to feed a better classifier | Recorder ships raw **classification primitives** *into the recorded event only* (not into the decision seam — see Section 3 fix). The *verdict* stays in replay. |
| **`provenance` (Recorded vs ExecuteShadow) on the shared event** | `lib.rs:136, 167, 1250` | Moved OFF the shared `SemanticEvent`. The recorder writes a provenance-free event; the replay/execute path stamps shadow-ness in its OWN stream (`ObservedCall.provenance`, `replay.rs:841`). See Section 2.E. |
| **Per-site `replay_ok`/`replay`/custom `result=` extractors** in Hyperswitch sites; MGET drops `replay_ok`; time/id seams disagree | `commands.rs:430-450`, `common_utils/src/lib.rs:124-130`, `crypto.rs:45 vs 626` | **Delete the replay flags from call sites.** Capture everything losslessly always; substitute-vs-execute is replay policy keyed on captured primitives, decided once in the orchestrator. |

The keystone consequence: the macro becomes **dumb about replay**. Its expansion is byte-identical whether or not replay will ever run, and it names no replay-only operation.

---

## 2. The `BoundaryCrossing` capture — complete field inventory

This is the generic recorded event. It is a **superset-compatible evolution** of `SemanticEvent` (`lib.rs:75-151`): every existing field is retained with identical serde behavior; all new fields are `#[serde(default)]` (+ `skip_serializing_if` for `Option`s). Grouped by concern; each field notes **type**, **why**, **C**(urrent)/**F**(uture) consumer.

> **Versioning prerequisite (resolved up front — see Section 5).** Adding these fields is NOT silently additive. The wire is bumped to `event_schema_version = 2` and a real `upcast()` dispatcher ships in the SAME change, because otherwise pre-change and post-change records both carry `version=1` and become indistinguishable exactly when the upcaster needs to tell them apart (forward-compat critique, blocker #1; verified `event_schema_version: 1` hardcoded at `lib.rs:1248, 2124, 2152, 2196, 2224` and `replay.rs:1658`).

### 2.A Identity & ordering (the address of the crossing)

| Field | Type | Why | C/F |
|---|---|---|---|
| `global_sequence` | `u64` | gap-free total order; writer drop-range markers. | C |
| `request_sequence` | `u64` | per-correlation order; rank-6 positional address. | C |
| `correlation_id` | `Option<String>` | **test-case isolation key** (project memory). Scopes occurrence, sequence, sampling, and (NEW) drop accounting. | C |
| `callsite_identity` | `Option<CallsiteIdentity>` (`lib.rs:332-365`) | stable cross-version address: `source`, `scope`, `occurrence:u32`, `lexical_path`, `syntax_hash`, `logical_context`, and its own `version:u16`. | C |
| `callsite_identity.occurrence` | `u32` | allocated ONCE (`instrument.rs:113`), reused by record+replay so positional pairing survives. | C |
| `callsite_identity.version` | `u16` | **No longer a frozen literal `1`** (was hardcoded at `instrument.rs:109`, `recordable.rs:406/603`). Driven from the single `CURRENT_CALLSITE_IDENTITY_VERSION` constant so the recorder never stamps a version the design calls non-authoritative (decoupling critique, minor; versioning critique, minor). Upcast rules in Section 5. | C |
| `logical_context` | `Option<String>` | rank-2 span-path; disambiguates concurrent same-callsite calls under async interleaving. | C |
| `graph_node_id` / `tracing_span_id` | `Option<u64>` | structural span linkage when `ExecutionGraphLayer` installed (`graph.rs:31-40`). | C(diag)/F(causal graph) |
| `call_file` / `call_line` / `call_column` | `String`/`u32`/`u32` | rank-5 `#[track_caller]` invocation address. | C |
| **`def_file` / `def_line` (NEW)** | `Option<String>`/`Option<u32>` | the **definition** site of the instrumented fn (`proc_macro2::Span`), currently dropped. Cheap literals; lets an analyst navigate event→boundary def. | F |

### 2.B Boundary tuple & classification PRIMITIVES (not verdicts)

| Field | Type | Why | C/F |
|---|---|---|---|
| `boundary` | `String` | layer tag (`redis`/`storage`/`http_client`/`id`/`time`). Replay derives channel from this (`replay.rs:866`) — keep raw. | C |
| `trait_name` / `method_name` | `String` | operation identity; replay derives read/write from method (`replay.rs:882`). | C |
| **`boundary_kind` (NEW)** | `Option<BoundaryKind>`, serde enum with `#[serde(other)] Unknown` | classification PRIMITIVE. Macro emits the family it knows (`deja::id`→`Entropy`, `deja::time`→`Time`, `deja::http`→`Egress`, db→`State`); replay may override. Replaces 16 hand-chosen `replay`/`replay_ok` flags. | F |
| **`determinism_hint` (NEW)** | `Option<Determinism>` with `#[serde(other)] Unknown`: `Deterministic`/`Entropy`/`Clock`/`External` | raw hint that this crossing is non-deterministic. Solves `crypto.rs:45 vs 626` at the source. | F |
| **`effect_hint` (NEW)** | `Option<Effect>` with `#[serde(other)] Unknown`: `Read`/`Write`/`ReadModifyWrite` | the read/write PRIMITIVE. Hint, never authoritative — replay still owns the verdict (`replay.rs:882-892`). | F |

> **Why these are primitives, not verdicts:** they are *what the boundary author/macro observed about the operation's nature*, like a type annotation. They are NOT "this WILL be substituted." Capturing the primitive means a new taxonomy reclassifies the same tape with no re-record.
>
> **Serde-tolerance fix (versioning critique, major #3):** every wire enum that may grow variants — `BoundaryKind`, `Determinism`, `Effect`, `Outcome`, `Recon`, `Provenance`-successor — carries a `#[serde(other)] Unknown` catch-all NOW. `#[non_exhaustive]` is a Rust source-compat attribute and does **nothing** for serde wire tolerance; `#[serde(default)]` rescues only MISSING fields, never an unknown variant in a PRESENT field. Without `#[serde(other)]`, a new recorder writing `boundary_kind:"egress"` would make an old reader's `Deserialize` **fail the entire event**. We therefore document: *adding an enum variant is wire-safe iff the enum has `#[serde(other)] Unknown`.*
>
> **Decoupling fix (blocker #2):** `boundary_kind`/`effect_hint` are written ONLY into the recorded event. They are NOT passed into the decision seam (v1 fed them into `decide()`, which is the macro participating in replay's classification at the call boundary). Replay reads them off the tape, exactly as `is_state_channel`/`is_read_op` already read `boundary`/`method_name`. See Section 3.

### 2.C Inputs — structured args, not just hashes

| Field | Type | Why | C/F |
|---|---|---|---|
| `args` | `serde_json::Value` | the input image. Lossless serde where the type is `Serialize`. **Fidelity flip is a versioned breaking change** (Section 5/6), not a silent edit, because it alters the bytes feeding `canonical_args_hash` (`replay.rs:901`), the pairing key. | C(match)/F(reconstruct) |
| `request` | `serde_json::Value` | human-readable alias of `args` (`lib.rs:110`). | C |
| **`args_recon` (NEW)** | `Recon` (with `Unknown` variant) | args fidelity self-described, so a reader knows whether inputs round-trip. | F |
| **`arg_schema` (NEW)** | `Option<Vec<ArgDescriptor>>` where `ArgDescriptor { name, ty, captured }`, `Captured ∈ {Serde, Debug, Opaque{type,len?}, Skipped}` | arg names AND static types (known at expansion via `FnArg::Typed.ty`). Enables signature-drift detection, per-arg fidelity choice, and an explicit **`Skipped` marker** for non-ident patterns (today silently dropped, `recordable.rs:693-699`). **Stays INLINE on the event** (versioning critique, blocker #2 — see Section 5). | F |
| **`value_digest` (NEW, COMMITTED)** | `Option<u64>` | FNV over canonical args (+result) via the existing `canonical_args_hash` (`replay.rs:901`) — never a second hash. Promoted from v1's Section 7 "open question" to a committed field, computed on **EVERY crossing in pure-record mode**, because a digest never stored at crossing time is unrecoverable, and it is the cheapest honest dataflow signal (future-modes critique, major #4). Survives even when a large payload is offloaded (see 2.D `Opaque`). | F |

### 2.D Outputs — result image, side-effect images, error/panic images

| Field | Type | Why | C/F |
|---|---|---|---|
| `result` | `serde_json::Value` | the load-bearing replay payload; lossless serde. | C |
| `response` | `serde_json::Value` | readable alias (`lib.rs:115`). | C |
| `is_error` | `bool` | **Remains the sole authority going forward** (see `outcome`). Today derived from JSON shape (`serialized_result_is_error`, `lib.rs:1372`). | C |
| **`outcome` (NEW)** | `Option<Outcome>` (`#[serde(other)] Unknown`): `Ok`/`Err`/`SerializationFailed` (NOT `Panic` — see below) | a type-aware **derived view** alongside `is_error`. Replaces fragile JSON-shape inference as the *recommended* signal, but `is_error` stays authoritative to avoid the two-fields-disagree hazard (versioning critique, major #4). A test asserts `outcome` and `is_error` never disagree on freshly recorded events. | F |
| **`panic_info` (NEW)** | `Option<PanicImage { message: String, location: Option<String> }>` | the raw panic OBSERVATION (downcast `catch_unwind` payload to `&str`/`String`), not a bucket tag. Enables panic-message diff / crash-fingerprinting (future-modes critique, major #3). **`Panic` is deliberately NOT an `Outcome` variant**: capturing a real-block panic without violating the no-unwind-into-request shadow guarantee is its own control-flow change (decoupling critique, minor) — `panic_info` is populated ONLY where a `catch_unwind` already exists (`lib.rs:1359/1714/1743`), and the firewall test (`finish_boundary_event_firewall_contains_a_panicking_serializer`, `lib.rs:2075`) is extended to assert the panic never reaches the request. |
| **`error_image` (NEW)** | `Option<serde_json::Value>` | the fully-serialized error VALUE, distinct from `result`, so error-taxonomy/diff modes get the raw error rather than a bucket label (future-modes critique, major #3). | F |
| `recon` | `Recon`: `Lossless`/`Structured`/`Opaque`/**`Unknown` (NEW variant)** (`lib.rs:141`) | reconstructability of `result`. **Fix: stamp the recon the macro actually produced** (today always `Lossless` even for `result_debug`). The `Unknown` variant exists so the upcaster can mark legacy always-`Lossless` values as *not trustworthy* rather than back-filling them as truthful (versioning critique, minor — recon value-semantics change). | F (correct now) |
| `result_image` | `Option<serde_json::Value>` (`lib.rs:146`) | post-image of affected state. Populated by the execute/shadow path. | F |
| `pre_image` | `Option<serde_json::Value>` (`lib.rs:150`) | pre-image for RMW. Needed for seed+execute total-derivative diff. | F |
| **`read_set` (NEW)** | `Option<Vec<StateKey>>` | state keys this crossing READ. **Populated during PURE record too** for State boundaries (the key is in args at record time, not only at execute time) so cascade/dataflow analysis works on standard tapes (future-modes critique, major #4). | F |
| **`write_set` (NEW)** | `Option<Vec<StateKey>>` | state keys this crossing WROTE; also populated during pure record where derivable from args. Forces every write to declare what it mutated (fixes `sadd` omitting members, `commands.rs:1258`, vs `eu_settlement_write` capturing `value`, `payment_create.rs:113-125`). | F |

### 2.E Provenance, timing, environment

> **Provenance is REMOVED from the shared `SemanticEvent`** (decoupling critique, major; future-modes critique, major #6). Rationale: `ExecuteShadow` is produced exclusively by replay's execute path (`execute_shadow_observe`, `lib.rs:282-303`); a pure recorder with the replay crate absent can never stamp it, and `finish()` hardcodes `Provenance::default()` (Recorded) at `lib.rs:1250` — making the field a tautology in pure-record mode and a replay concept frozen into the recording type. **Lineage now lives where it is observed:** the replay/execute path stamps it on its OWN `ObservedCall` stream, whose `provenance` already exists (`replay.rs:841`) and which already carries `synthesized` (`replay.rs:820`) and `real_impl_will_fail` (`replay.rs:825`) with no event counterpart. That stream's `Provenance` gets `#[serde(other)] Unknown` plus the variants the codebase already proves it needs — `Synthesized`, `Seeded` — and carries `real_impl_will_fail` as an observed boolean, so partial-seeding / multi-version-diff read full lineage without re-record.
>
> If a unified tape must mark shadow rows, a neutral `capture_origin: Option<CaptureOrigin>` (defined in the replay crate, defaulted/ignored by the recorder) carries it — never a variant the recorder's own type system must name.

| Field | Type | Why | C/F |
|---|---|---|---|
| `timestamp_ns` | `u64` | wall-clock at START (`lib.rs:1231`, from `start_ns`). | C |
| **`end_timestamp_ns` (NEW)** | `Option<u64>` | wall-clock at completion. v1 threw the end timestamp away, collapsing everything into `duration_us` and making time-travel / log-interleave replay impossible (future-modes critique, blocker #1). | F |
| **`monotonic_start_ns` / `monotonic_end_ns` (NEW)** | `Option<u64>` | raw `Instant`-based spans that survive wall-clock skew (`duration_us` derived from wall clock is skew-fragile). | F |
| `duration_us` | `u64` | **demoted to a convenience field derived from the raw timestamps** (was `end_ns - start_ns` at `lib.rs:1220`, a collapsed verdict). Kept for back-compat; raw spans above are the load-bearing observation. | C |
| **`phase_timings` (NEW)** | `Option<Vec<(PhaseTag, u64)>>` (`PhaseTag` has `#[serde(other)] Unknown`) | optional wait/service/serialize breakdown so latency-aware and interleaving-fidelity modes read raw phases instead of one pre-collapsed scalar (future-modes critique, blocker #1). Populated only where the seam can cheaply measure phases; `None` otherwise. | F |
| **`observed_samples` (NEW)** | `Option<Vec<ObservedValue>>` | reserved additive shape so a future statistical/fuzz/property replay can represent a DISTRIBUTION of draws for one crossing instead of the single frozen value the design itself flags as non-deterministic (future-modes critique, blocker #2). Normally `None` (single-sample); a per-correlation aggregation pass or a repeated-call boundary may populate it. | F |
| **`entropy_source` (NEW)** | `Option<String>` | for Entropy/Time crossings, *what* produced the value (`OsRng`, `OffsetDateTime::now_utc`, `Uuid::new_v4`). | F |
| **`raw_draw` (NEW)** | `Option<serde_json::Value>` | the **resampleable raw observation** for Entropy/Clock crossings (the integer drawn, the ns clock read) BEFORE post-transform, where cheaply available. The post-transform `result` is the verdict; the raw draw is what a distribution-aware mode actually needs (future-modes critique, blocker #2). | F |
| `recording_run_id` | `Option<String>` | per-process run id (`lib.rs:86`). | C |
| **`recording_env` (NEW)** | `Option<RecordingEnv>` `{ deja_version, host_arch, recorded_at_version, tz, locale, active_policy, env_snapshot: Map<String,String> }` | self-describing artifact. Extended beyond v1 with timezone/locale/active policy (`AllLookup` vs `SelectiveExecute`) and a small `DEJA_*` env snapshot, since those control non-determinism and the policy a tape was recorded under is an observation, not an interpretation (future-modes critique, minor). **Stays inline on the event** (see Section 5 on why interning is rejected as the default). | F |
| `receiver` | `Option<serde_json::Value>` | decorator `self`/inner type context (`lib.rs:107`). Boundary macro may capture `&self` like the delegate path. | C(delegate)/F(boundary) |

### 2.F Versioning

| Field | Type | Why | C/F |
|---|---|---|---|
| `event_schema_version` | `u16` (`lib.rs:124`) | **BUMPED to 2 and made load-bearing.** A reader's first action is `match event_schema_version` → `upcast`. See Section 5. | C(tag)/F(dispatch) |

**Justification discipline:** every NEW field either (a) closes a documented live-site inconsistency, or (b) is the minimal raw observation unlocking a *named* future mode: seed+execute needs lossless `args`+`pre_image`+`write_set`; total-derivative diff needs `result_image`+`write_set`; signature-drift needs `arg_schema`+`recording_env`; **latency-aware/time-travel needs the raw timestamps+`phase_timings`**; **statistical/fuzz needs `observed_samples`+`raw_draw`**; **cascade/dataflow needs `value_digest`+pure-record `read_set`/`write_set`**; **crash-triage needs `panic_info`+`error_image`**. No field is speculative beyond a named consumer.

---

## 3. The narrow replay protocol seam — ONE closure, all control flow inside

The v1 `match decide()` was a relabeled leak: its three arms ARE the replay taxonomy, and the macro still named `shadow_observe`, `from_value`, and "suppress normal record" (decoupling critique, blocker #1). It also fed classification primitives into the decision (blocker #2) and eagerly bound args, defeating the inactive-hook fast path (major #5).

**Fix: the macro hands the seam the block as a closure and a typed result-extractor, and the seam owns run/skip/shadow/record entirely.** The macro contains exactly ONE replay-facing call and names zero replay-only operations.

```rust
// in deja-record, called by the macro. The ONLY replay-facing call the macro makes.
fn dispatch<T>(
    obs: CrossingObservation<'_>,        // matching-only inputs (NOT classification verdicts)
    args: impl FnOnce() -> serde_json::Value,   // LAZY: not evaluated unless something is recording/matching
    run: impl FnOnce() -> T,             // the real block, as a closure
    extract: impl Fn(&T) -> serde_json::Value,  // lossless result image (fidelity fixed, not replay-chosen)
) -> T;

struct CrossingObservation<'a> {
    spec: BoundarySpec<'a>,            // boundary, component, operation — to MATCH a recording
    identity: &'a CallsiteIdentity,    // occurrence allocated once, reused
    caller: &'static Location<'static>,
    // NOTE: boundary_kind / effect_hint / determinism_hint are NOT here.
    // They are written into the recorded event only; replay reads them off the tape.
}
```

Inside `dispatch()` (all in deja-record, never in the macro):
- **inactive hook →** call `run()`, return `T`, never evaluate `args` (preserves the fast path at `recordable.rs:374/585` and `start_boundary_event_lazy`, `lib.rs:1695`).
- **record / no-op / `AllLookup`-default →** evaluate `args` lazily, call `run()`, `finish_crossing` with `extract(&out)` (the existing record seam, `lib.rs:1733`). Provenance-free event.
- **lookup hit →** the seam itself attempts the typed reconstruction (via a type-erased deserializer the macro provides once as part of `extract`/`T`'s `DeserializeOwned` capability); on success returns the recorded value as `T` **without calling `run()`**; on deserialize failure it **falls back to calling `run()`** — preserving today's "deserialize-fail falls through to live" policy (`instrument.rs:166-200`) that a bare macro-arm `from_value` would have lost (decoupling critique, major #6).
- **execute/shadow →** call `run()`, then internally shadow-observe `extract(&out)` against the baseline and suppress the normal record. The macro never names this; the two-phase ordering and the suppress obligation live inside the seam (decoupling critique, minor #7).

**Macro body becomes mode-agnostic, order-trivial, and replay-name-free** (one shape replacing `instrument.rs:214-422`):

```rust
let __deja_identity = { #identity_build };          // occurrence allocated once (unchanged: instrument.rs:101-123)
::deja::__private::dispatch(
    __deja_obs,
    || { #args_expr },                              // LAZY, lossless. No replay branch.
    || { #run_block },                              // the real block as a closure
    |__deja_result| (#result_expr)(__deja_result),  // fidelity FIXED (lossless), not replay-flag-chosen
)
```

**Proof of "dumb about replay":** removing every replay hook makes `dispatch()` a `RuntimeHook::NoOp`/`Recording` method that runs `run()` and (in record mode) records; the macro tokens are unchanged, and the macro names no `Substitute`/`shadow_observe`/`from_value`/`suppress`. The optimizer collapses the inactive path.

**Subsumes the three current seams:** lookup-hit = `replay_boundary` (`lib.rs:1613`); execute/shadow = `boundary_execute_mode` + `execute_shadow_peek/observe` (`lib.rs:1637/1653/1676`) — all now internal to `dispatch()`.

> **Descriptor-completeness vs leakage (reconciling two critiques).** Future-modes critique (minor #8) wanted the descriptor to carry the FULL primitive set so future policies can route on determinism/key-set without a macro change; decoupling critique (blocker #2) wanted classification primitives OUT of the decision path. These reconcile cleanly: the primitives a future policy might route on are **already on the tape**; a policy routes on them by reading the recorded event, not by being fed them at record time. The live decision seam needs only matching inputs (spec/identity/caller). So `CrossingObservation` stays minimal AND no future routing is frozen — because routing happens in replay over captured facts, not in the macro.

---

## 4. Macro responsibilities — full capture for free

Adding a boundary should be **just the attribute**, everything below auto-derived from compile-time structure (the macro already does most, `instrument.rs:35-65`).

**Auto-derived from the signature (`sig`):**
- `boundary` ← family default (`deja::id`→`id`, `deja::time`→`time`, `deja::http`→`http_outgoing`).
- `component` ← `module_path!()`. **Stop requiring 16 redis sites to hand-pass `component="redis_interface::commands"` == the default.**
- `operation` ← fn name.
- `syntax_hash` ← FNV-1a of `boundary::operation`, signature DELIBERATELY excluded for cross-version stability (`instrument.rs:74-90`).
- `arg_schema` ← iterate `sig.inputs`, emit `ArgDescriptor`; non-ident patterns emit `Captured::Skipped` **with a marker**.
- `boundary_kind`/`determinism_hint`/`effect_hint`/`entropy_source` ← from attribute family or explicit `effect=`/`kind=` overrides. Replaces per-site `replay`/`replay_ok`.
- `ret_ty` ← `sig.output`, used for reconstruction AND recorded as the return descriptor.
- `def_file`/`def_line` ← fn `Span`, emitted as literals.
- `callsite_identity.version` ← the `CURRENT_CALLSITE_IDENTITY_VERSION` constant, NOT a literal `1`.
- `#[track_caller]` ← auto-added if absent (`instrument.rs:149-155`).

**Degradation & fidelity-honesty.** Per-arg the macro chooses `Serde` / `Debug` / `Opaque{type,len}` and **stamps the choice into `args_recon`/`arg_schema`**; the result side stamps `recon` to the serializer it actually used (ending the "mislabels Debug as Lossless" gap, `lib.rs:1251`). Fidelity is a captured FACT, not a replay-flag-driven choice.

**Large-payload policy — offload, do NOT drop (future-modes critique, major #5).** v1's size cap downgraded large bodies to `Opaque{type,len}`, destroying the bytes that speculative-execute / seed-and-execute / multi-version-diff need, and (interacting with the dataflow fix) leaving nothing to digest. Replaced by:
- **Default:** record full (the "recording observes, replay interprets" thesis — truncation is a consumer concern).
- **Optional opt-in:** offload the payload to a content-addressed sidecar blob referenced by digest from the event; the event stays small, the raw observation survives for modes that opt in.
- **Invariant:** `value_digest` of the FULL payload is ALWAYS stored even when offloaded, so identity/diff/dataflow modes survive regardless.

**Staying safe under nesting (the nesting-bug lesson).** `set_hash_fields` issues a RAW uninstrumented `pool.expire` to avoid orphaning a nested EXPIRE under substitution (`commands.rs:807-819`); `delete_multiple_keys` likewise. The single `dispatch()` closure-seam makes nesting **safe rather than forbidden**:
- On a lookup hit, `dispatch()` never calls `run()`, so nested crossings inside the skipped block never run — no orphan is structurally possible (the interleave that caused the bug, `instrument.rs:202-213`, is gone).
- On execute/shadow, nested crossings DO run and record as shadow children, tagged with the parent's `graph_node_id` (`graph.rs:31-40`) so the tally attributes them.

---

## 5. Forward-compatibility & versioning

**The version axis is made load-bearing in the same change that adds fields** (forward-compat critique, blocker #1). Steps:

1. `default_event_schema_version()` → returns **2**; introduce `const CURRENT_EVENT_SCHEMA_VERSION: u16 = 2` and replace every hardcoded `event_schema_version: 1` (`lib.rs:1248, 2124, 2152, 2196, 2224`; `replay.rs:1658`) with the constant.
2. Ship `fn upcast(raw: RawEvent) -> BoundaryCrossing` as a real version-dispatching function NOW (even though v1→v2 is mostly additive defaulting), so the migration seam exists *before* any tape with "version=1-but-new-fields" can be written.

**Additive discipline (house style, `lib.rs:85-150`):** every new field is `#[serde(default)]` (+ `skip_serializing_if`). The lenient u64 parse (`de_u64_opt_lenient`, `lib.rs:369`) is the tolerance model. BUT two v1 claims were mislabeled additive and are reclassified:

- **Enum-variant growth is NOT additive for serde** (critique major #3). Fixed structurally: every wire enum carries `#[serde(other)] Unknown` (Section 2.B). Documented rule: adding a variant is wire-safe iff the enum has `#[serde(other)]`. `#[non_exhaustive]` is orthogonal (source-compat only).
- **Header-interning of `arg_schema`/`recording_env` is a BREAKING wire-shape change, NOT a size optimization** (critique blocker #2). An event with `arg_schema_ref:7` and no inline `arg_schema` deserializes as `None` on an old reader — silent data loss — and couples event interpretability to the optional marker channel (`write_marker` default no-op, `writer.rs:152`). **Decision: keep `arg_schema` and `recording_env` INLINE.** Size is controlled by (a) `recording_env` being one-stamp-per-run via a header marker that is purely *advisory* (the event remains self-describing without it), and (b) large *payloads* (not schemas) using the blob-offload of Section 4. If interning is ever adopted it becomes its own explicitly-versioned schema change with a required, ordered marker stream and a loud-fail discriminant — never a silent `None` degrade.

**Reconcile the two version axes.** `event_schema_version` is the **authoritative artifact version**; the reader's first action is `match event_schema_version` → upcast path. `CallsiteIdentity.version` is a sub-struct detail. To avoid the latent inconsistency where the event upcaster cannot know the sub-struct layout (critique minor), we **state and enforce the invariant**: a given `event_schema_version` implies `callsite_identity.version ∈` a known set, AND the upcaster dispatches on `callsite_identity.version` *directly* for that sub-struct rather than assuming the event version determines it. A committed fixture (a real pre-change tape) round-trips through `upcast()` in CI.

**`is_error`/`recon`/`outcome` migration semantics (critique major #4, minor).**
- `is_error` stays the **sole authority**; `outcome` is a derived view; a test asserts they never disagree on fresh events. `serialized_result_is_error` (`lib.rs:1372`) call sites are NOT silently left to diverge — they continue to read `is_error`. We verify against the matrix payloads that no result serializes to an object containing a spurious `Err` key that the legacy shape-inference would flip (protecting the live demo).
- `recon` value-semantics change (always-`Lossless` → honest `Structured`/`Opaque`) is **gated by the v2 bump**. The upcaster does NOT back-fill old `Lossless` as trustworthy; it maps legacy `recon` → `Recon::Unknown` so consumers distinguish "truthfully lossless" from "legacy-always-lossless."

**Two concrete future modes that need ZERO re-record (kept from v1, both still sound):**

1. **Typed seed-and-execute (total derivative for State).** Needs lossless `args`, `pre_image`/`result_image`, `write_set` — all captured generically. Orchestrator reads the tape, seeds, executes, diffs. No re-record.
2. **Channel reclassification (SCAN→Egress, `generate_exp`→Clock).** Needs raw `boundary`/`method_name` + `boundary_kind`/`determinism_hint` *hints*. New classifier runs over the existing tape. No re-record.

**Three MORE future modes now provably zero-re-record (added per critique):**

3. **Latency-aware / time-travel / interleaving replay.** Needs `end_timestamp_ns`, `monotonic_start/end_ns`, optional `phase_timings` — all captured. Reconstruct exact completion wall-clock to interleave with logs; assert p99; inject latency. No re-record.
4. **Statistical / fuzz / property replay.** Needs `determinism_hint` + `raw_draw` + reserved `observed_samples`. A distribution-aware mode resamples raw draws across crossings. No re-record.
5. **Crash-triage / panic-diff / cascade-dataflow.** Needs `panic_info`+`error_image` and `value_digest`+pure-record `read_set`/`write_set`. No re-record.

---

## 6. Migration from current code (ordered, behavior-preserving, demo-safe)

The constraint: **the record path must not change observable behavior, and the matrix/total-derivative catch must still work.** Each step independently shippable.

**Step 0 — Bump the wire and ship the upcaster (deja-record).** Add `CURRENT_EVENT_SCHEMA_VERSION = 2`, replace all hardcoded `1`s (`lib.rs:1248/2124/2152/2196/2224`, `replay.rs:1658`), and ship `upcast()` with a committed pre-change fixture round-trip test. This MUST precede any field addition so old/new tapes are distinguishable.

**Step 1 — Make recording lossless & self-describing (deja-derive + deja-record).**
- Stop selecting `result_expr` by `replay`/`replay_ok` (`instrument.rs:126-138`); always emit the lossless serializer where the bound holds; stamp matching `recon`.
- Stamp `recon` from macro-passed fidelity instead of hardcoded `Recon::Lossless` (`lib.rs:1251`).
- `inferred_args_expr`: prefer serde over `::deja::value::debug` when the bound holds; emit `arg_schema` with `Captured` markers (incl. `Skipped`).
- **CRITICAL demo gate:** the args-fidelity flip changes the bytes feeding `canonical_args_hash` (`replay.rs:901`), the pairing key (critique major #5). Treat it as a **versioned breaking record change**: either re-record any tape used for cross-version pairing, OR keep `args` Debug-rendered and add a parallel `args_typed` lossless field so the existing hash input stays byte-stable. **Before declaring the demo safe, assert `canonical_args_hash` over migrated events is unchanged for the matrix scenarios.**

**Step 2 — Add new fields to `SemanticEvent` as `#[serde(default)]` (`lib.rs:75-151`).** `boundary_kind`, `effect_hint`, `determinism_hint`, `entropy_source`, `raw_draw`, `arg_schema`, `args_recon`, `value_digest`, `outcome`, `panic_info`, `error_image`, `read_set`, `write_set`, `end_timestamp_ns`, `monotonic_start/end_ns`, `phase_timings`, `observed_samples`, `def_file/def_line`, `recording_env`. All default/None, every wire enum carrying `#[serde(other)] Unknown`. `EventBuilder::finish` (`lib.rs:1218-1258`) fills them; **remove `provenance` from the event** and move lineage to the `ObservedCall` stream. Populate `value_digest` and (for State) `read_set`/`write_set` in pure-record. Capture `end_timestamp_ns`/monotonic spans in `finish`.

**Step 3 — Introduce `dispatch()` alongside the existing three seams (deja-record).** Implement `dispatch()` in terms of current `replay_boundary`/`boundary_execute_mode`/`execute_shadow_peek` (`lib.rs:1613/1637/1653`) so semantics are bit-identical; internalize the deserialize-or-fall-through and the run/shadow/suppress lifecycle. Keep old seams exported (deprecated).

**Step 4 — Rewrite the macro body to the single closure-seam (deja-derive `instrument.rs:214-422`).** Replace the `(args_bind, substitution, start_args)` tuple, the `execute_arm*` family (`instrument.rs:245-336`), and the interleaved body templates with the single `dispatch(obs, args_thunk, run_thunk, extract)` shape. Delete `any_replay` (`instrument.rs:43`). Verify the inactive-hook fast path: generated code must NOT serialize args when `DejaHook::is_active()` is false (match `recordable.rs:374`). **Demo gate:** confirm the execute/shadow path still produces shadow lineage so args-free pairing → `ValueDiverged` (`replay.rs:73`) still fires.

**Step 5 — Apply the single closure-seam to the delegate path (deja-derive `recordable.rs:286-356, 509-577`).** Collapse duplicated execute/replay arms into one `dispatch()`. The `DeserializeOwned` problem (critique major #6, v1 §7.4): a proc-macro **cannot prove** a where-bound is satisfiable at expansion. **Do NOT merge `delegate_X`/`delegate_X_with_replay` into one** if merging would force `DeserializeOwned` (`recordable.rs:271/661`) onto record-only return types and break pure-record builds. Resolution: keep a record-only delegate variant whose return types need no bound; route lookup-reconstruction through `dispatch()`'s type-erased path that is only reachable when the bound is actually present (concrete example required in the PR: a non-`DeserializeOwned` return type that still compiles record-only after the change).

**Step 6 — Sweep the Hyperswitch call sites (vendor).** Delete redundant `component=`/`correlation=None` from the 16 redis sites (`commands.rs:117-1269`). **Enumerate every remaining knob and give each a verdict** (decoupling critique, major #4):
- `replay`/`replay_ok` → deleted (family default / `effect=`/`kind=` primitives).
- `correlation` → derive from ambient `current_correlation_id()`; remove from sites.
- `replay_with` (custom reconstruction) → pure replay policy; move behind the seam or delete.
- custom `result=`/`args=` extractors → eliminate in favor of auto-derived lossless capture; if genuinely needed, document as the ONLY sanctioned escape hatch with a stated reason.
- Fix MGET (`commands.rs:430-450`) to capture losslessly like siblings; make `sadd` capture `write_set` (`commands.rs:1258`).
- db seam (`generics.rs:158-217`) and `request_id` seam (`request_id.rs:118-139`): **explicitly descoped to a follow-up** — capture is NOT yet fully uniform after this PR, and the doc says so rather than implying it.

**Step 7 — Remove deprecated seams** once macro + sites are migrated and the demo is re-verified green.

**Demo safety gate after Steps 1, 4, 5:** run the matrix scenarios (`41c0590`, `e37038a`, `aaf7819`) and confirm (a) the total-derivative catch fires, (b) diff colors unchanged, (c) `canonical_args_hash` equality over the scenarios pre/post Step 1.

---

## 7. Open questions / risks

**7.1 Cascade / dataflow — now PARTLY committed, not all-open.** `value_digest` (2.C) and pure-record `read_set`/`write_set` (2.D) are **committed** as non-authoritative hints (moved out of "open" per critique major #4):
- **`value_digest`** — one `canonical_args_hash` per crossing; spots "value WRITE at B carries a digest matching READ at A" (probable read→write edge) without taint tracking. Stored even when payloads offload.
- **`read_set`/`write_set` overlap** — structural state dependency on the same key; reliable for DB/Redis; captured from args at record time.
- **Still genuinely open:** true value provenance (which prior result *flowed into* this arg through app code) requires taint tracking a proc-macro cannot see. Ship the above as *hints*; leave full taint to a future instrumentation layer. **Risk/mitigation:** digests are reused from `canonical_args_hash` (`replay.rs:901`) so they are byte-stable across renderer and analyzer — never a second hash.

**7.2 Capture cost & size.** Lossless-always + images + raw timestamps inflate the tape.
- `pre_image`/`result_image` remain execute-only `Option`s (pure-record pays nothing); `read_set`/`write_set`/`value_digest` ARE paid in pure-record (cheap, and they are the irreversible dataflow signal).
- `arg_schema` and `recording_env` stay INLINE (interning rejected as a silent breaking change, §5); schema bloat is bounded by mostly-static type strings and a one-stamp-per-run advisory header.
- Large *payloads* use blob-offload (§4), never silent drop; full `value_digest` always retained.
- Raw timestamps add 3 `Option<u64>` per event — negligible vs the latency-mode capability they unlock.
- Writer is per-event JSONL, drop-accounting by `global_sequence` range not correlation (`writer.rs:277+`), so a per-test-case completeness check can't tell which case lost events. Not blocking, but `dispatch()` threads `correlation_id` into sink markers so this stays fixable; if markers ever carry interpretation-critical data, `write_marker`'s default no-op (`writer.rs:152`) must become mandatory for those sinks plus a header marker recording `event_schema_version` + interning state (critique minor) — which is exactly why interning is NOT the default.

**7.3 `effect_hint`/`boundary_kind`/`determinism_hint` could be wrong.** HINTS, never verdicts. Replay always overrides (`is_read_op`/`is_state_channel` stay authority, `replay.rs:866-892`). **Mitigation:** documented as hints; replay logs when its own classification disagrees with the recorded hint (a cheap drift signal). They are written only into the event, never into `dispatch()` — so a wrong hint can never silently steer a live decision.

**7.4 `panic_info` and the no-unwind-into-request guarantee.** Populating panic observations must NOT let a recording panic unwind into the real request (the shadow guarantee). `panic_info` is filled only inside existing `catch_unwind` firewalls (`lib.rs:1359/1714/1743`); the firewall test (`lib.rs:2075`) is extended to assert the panic is captured AND never reaches the request. `Panic` is deliberately excluded from the `Outcome` enum (it is observable only via that machinery), making `outcome` a pure derivation from the typed result and keeping the panic-handling contract a separate, explicit concern.

**7.5 `observed_samples` is reserved, not yet populated.** Single-sample capture remains the norm; the field exists so a future statistical mode can populate it (via a per-correlation aggregation pass or a repeated-call boundary) without re-record. `raw_draw` is the per-crossing resampleable observation that makes that mode meaningful. Risk: if `raw_draw` is omitted for cheapness on a given seam, that seam's crossings are not resampleable — documented as a per-seam capture choice, not a silent gap (the `args_recon`/`recon` honesty discipline extends here).

---

## Changes from v1 after review

**From the FUTURE-REPLAY-MODES critique:**
- **(blocker, timing)** Added `end_timestamp_ns`, `monotonic_start/end_ns`, and optional `phase_timings`; demoted `duration_us` to a derived convenience field; added latency-aware/time-travel/interleaving replay to the §5 zero-re-record proofs. (was: only start `timestamp_ns` + collapsed `duration_us`.)
- **(blocker, statistical)** Added reserved `observed_samples` and per-crossing `raw_draw`; added statistical/fuzz/property replay to the §5 proofs. (was: single frozen sample for fields the design itself labeled non-deterministic.)
- **(major, panic/error)** Added `panic_info` (raw payload+location) and `error_image` (raw error value); kept `outcome` but excluded `Panic` from it and scoped panic capture to existing `catch_unwind` firewalls. (was: a bare `Panic` bucket tag, no payload.)
- **(major, dataflow)** Committed `value_digest` on EVERY crossing and populated `read_set`/`write_set` during PURE record; moved them out of "open questions" into the field inventory. (was: `value_digest` an open question, key-sets execute-only.)
- **(major, large payloads)** Replaced the record-time `Opaque{type,len}` drop with offload-don't-drop (content-addressed sidecar) + always-store full `value_digest`. (was: destructive record-time size-cap verdict.)
- **(major, provenance)** Committed `#[serde(other)] Unknown` + `Synthesized`/`Seeded` variants and `real_impl_will_fail` on the lineage stream — and moved lineage OFF the shared event entirely. (was: 2-variant closed enum, "consider non_exhaustive.")
- **(minor, environment)** Extended `recording_env` with `tz`/`locale`/`active_policy`/`env_snapshot`. (was: version identity only.)
- **(minor, descriptor)** Reconciled with the decoupling fix: kept the decision seam minimal, since future routing happens in replay over captured facts, not via a widened descriptor. (was: only `boundary_kind`+`effect_hint` forwarded — both a leak AND a freeze.)

**From the FORWARD-COMPAT / VERSIONING / MIGRATION critique:**
- **(blocker, version axis)** Bumped `event_schema_version` to 2, replaced all hardcoded `1`s with a constant, and ship a real `upcast()` dispatcher in the same change (new Step 0). (was: ~11 fields added while version stayed hardcoded at 1, making old/new tapes indistinguishable.)
- **(blocker, interning)** Rejected header-interning of `arg_schema`/`recording_env` as the default — kept them INLINE; if ever adopted it is an explicit versioned change with a loud-fail discriminant. (was: presented as a benign size optimization that silently degrades to `None`.)
- **(major, enums)** Added `#[serde(other)] Unknown` to every wire enum and documented that variant-growth is wire-safe only with it. (was: enum growth treated as additive; serde rejects unknown variants.)
- **(major, is_error/outcome)** Kept `is_error` authoritative, made `outcome` a derived view, added a no-disagreement test, and verify matrix payloads have no spurious `Err`-key flip. (was: dual authorities that could disagree on the same event.)
- **(major, args fidelity)** Reclassified the args lossless-always flip as a versioned breaking record change with a mandatory pre/post `canonical_args_hash` equality gate on the matrix scenarios (or a parallel `args_typed`). (was: claimed "behavior-preserving," but it changes the pairing key bytes.)
- **(major, delegate merge)** Do NOT merge the two delegate macros if it forces `DeserializeOwned` onto record-only types; keep a record-only variant; PR must show a concrete non-`DeserializeOwned` return type that still compiles. (was: "unconditional" then hand-waved to a macro-provable bound a proc-macro cannot prove.)
- **(minor, two version axes)** Stated+enforced an invariant tying `event_schema_version` to the `callsite_identity.version` set, upcaster dispatches on the sub-struct version directly, committed-fixture round-trip in CI. (was: assumed lockstep without establishing it.)
- **(minor, recon semantics)** Gated the recon honesty change behind the v2 bump; upcaster maps legacy `Lossless` → new `Recon::Unknown` rather than trusting it. (was: a value-semantics change mislabeled purely additive.)
- **(minor, markers)** If markers ever carry interpretation-critical data, `write_marker` becomes mandatory for those sinks + a header marker records version/interning state — which is the stated reason interning is not the default.

**From the DECOUPLING-PURITY critique:**
- **(blocker, seam shape)** Collapsed the v1 `match decide()` (whose 3 arms were the replay taxonomy) into ONE `dispatch(obs, args_thunk, run_thunk, extract)` closure-seam that owns run/skip/shadow/record internally; the macro now names ZERO replay-only operations. (was: macro still branched lookup/execute/record and named `shadow_observe`/`from_value`/suppress.)
- **(blocker, descriptor)** Removed `boundary_kind`/`effect_hint` from the decision seam; classification primitives are written into the event only and read by replay off the tape. (was: macro fed classification primitives into the replay decision.)
- **(major, provenance)** Removed `provenance` from the shared `SemanticEvent`; lineage lives on the replay-owned `ObservedCall` stream. (was: a replay-only `ExecuteShadow` variant frozen into the recording type, tautological in pure-record.)
- **(major, remaining knobs)** Enumerated every surviving call-site knob (`correlation`/`result=`/`args=`/`replay_with`) with a verdict, and explicitly descoped the db/`request_id` seams. (was: silent about them; "fully in macro" overclaimed.)
- **(major, args laziness)** Restored the inactive-hook fast path — `dispatch()` takes an args THUNK and short-circuits before evaluating it. (was: eager `let __deja_args = {…}` forced serialization on every call.)
- **(major, deserialize fall-through)** The deserialize-or-fall-through-to-live policy lives inside `dispatch()`, which can call `run_thunk` on deserialize failure. (was: a bare `from_value` macro arm that had already foreclosed the live path.)
- **(minor, shadow lifecycle)** The run-then-shadow-then-suppress ordering is internal to `dispatch()`; the macro never names it. (was: macro encoded the two-phase shadow protocol.)
- **(minor, callsite version)** `callsite_identity.version` driven from a constant, not a baked literal `1`. (was: stale `1` at `instrument.rs:109`, `recordable.rs:406/603`.)
- **(minor, outcome/panic)** Scoped `outcome` to `Ok`/`Err`/`SerializationFailed` (recorder-observable) and deferred `Panic` to `panic_info` inside the existing firewall, preserving the no-unwind guarantee. (was: `Panic` as a free additive `Outcome` variant ignoring the panic-handling contract.)
