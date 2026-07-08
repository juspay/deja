# Declaration collapse: one knob — `Execute` | `Substitute`

**Status: IMPLEMENTED (task #28).** The Channel/Effect/EntropySource/Strategy
taxonomy and the `decide_strategy` matrix are deleted; routing is the per-site
`replay_strategy = Execute | Substitute` knob (default `Substitute`). The change is
BEHAVIOR-PRESERVING by construction — see "Implementation notes" below for the
exact per-site mapping and the one deliberate deviation from the migration-map
bullets (the mapping is derived from the OLD `decide_strategy` output, which is the
ground truth, not from the result-shape heuristics those bullets describe).

This doc scopes the model and records the decisions that were the user's to make.

## Thesis
The whole declarative taxonomy collapses to **one per-boundary knob**:

| Value | Meaning |
|---|---|
| `Execute` | Reconstruct state from the recorded results (seed by key/PK), then **run the real function**. Result-shape auto-downgrades to `Substitute` when the result is a non-seedable scalar. |
| `Substitute` *(default)* | **Don't run** the function — return the recorded result. |

`Channel{State,Entropy,Egress}`, `Effect{Read,Write,RMW,…}`, and `EntropySource`
go away. They were the "declare *what a boundary is*, derive *what to do*" thesis;
two later insights deflated them:
- **seed-from-result** made `Effect` / `read_set` / `write_set` redundant.
- **result-shape routing** made the seed-vs-lookup decision data-driven.

What remains is the single irreducible fact the runtime can't derive: **is it safe
to re-run this function?** — because an HTTP response *looks* seedable (object-shaped)
but re-executing it calls the real service. That bit is exactly `Execute` vs `Substitute`.

## What each does (proposed — confirm in OPEN DECISIONS)
- **Execute**: seed each recorded row/value (by PK for DB via the `type_name→(table,[pk])`
  map; by key for redis), then call through to the real fn. Scalars / misses / pure
  creates (`insert`, `set_key`) seed nothing. (This is what #26 implements.)
- **Substitute**: deserialize the recorded result back into the boundary's return type
  and return it without calling through. Covers entropy (clock/id), egress (http),
  and anything not opted into `Execute`.

## The knob is PER SITE, default Substitute — NOT per helper/class
There is no per-helper or per-channel default. **Every site defaults to
`Substitute`; the author opts specific sites into `Execute`.** The decision is
per *function*, not per *class* — e.g. within `db`:
- `find` / `find_one` / `filter` / `update_with_results` → `Execute`
- `count` / `delete` / affected-rows `update` → `Substitute` (default — write nothing)

Same granularity for redis: `get_key` → `Execute`; `exists` / `increment` /
`delete_key` → `Substitute`. So "db class" mixes both. (This corrects an earlier
wrong assumption that `db`/`redis` map wholesale to `Execute`.)

## Migration map (the existing ~37 declaration points)
- Default = `Substitute` everywhere; opt the reconstructable-internal-state sites
  into `Execute` one by one (the row-returning db/redis ops above).
- `deja::id` / `deja::time` / `deja::http` sites → stay `Substitute` (default).
- The old `Channel`/`Effect` enums are deleted; they did NOT map cleanly to the
  knob (a `State` op can be either `Execute` or `Substitute` per op — exactly the
  find-vs-count case).
- `AllLookup` policy = everything `Substitute` regardless of the knob (unchanged).
- Result-shape (#26) demotes to (a) row extraction for `Execute` sites and (b) a
  guard (an `Execute` site returning a scalar just runs without seeding); the
  per-site knob is the routing source of truth, not inference.

## Out of scope / orthogonal
- `replay_ok` / `replay_with` serialization (task #27) — that's *how* the result is
  captured/reconstructed, not *whether* to run. Independent axis.
- Collapsing the helper macros (`deja::id`/`time`/`http`/`db`/`redis`) into **one**
  general boundary macro + the knob — related and desirable, but a bigger change;
  call it a possible follow-on, decided separately.

## DECISIONS (resolved with the user)
1. **Knob name — RESOLVED: `replay_strategy = Execute | Substitute`.** Written only
   on opt-in `Execute` sites; a silent site = `Substitute`. e.g.
   `#[deja::db(replay_strategy = Execute)]` on `find`, nothing on `count`.
2. **Default = `Substitute`** — confirmed (safe-by-default; opt into Execute per SITE).
3. **One-macro vs helpers — RESOLVED in principle:** helpers carry no routing role
   (the knob is per-site), so their only job is the recording shape. They dissolve
   into one general macro **once serialization is generic** — gated on the
   serialization spike (autoref-spec: serde / Result-Ok / reqwest::Response /
   Debug-fallback). Until the spike proves that clean, keep thin helpers.

## STILL OPEN
4. **Run the serialization spike?** It's the keystone that decides whether the
   one-macro end state is reachable cleanly. Awaiting go/no-go.
3. **Keep any old taxonomy as non-routing metadata?** e.g. is "this was a clock/http
   boundary" worth keeping as a free-text label for the dashboard/provenance, even
   though it no longer drives routing? (Lean: drop it unless the UI needs it.)
4. **One general macro vs keep the preset helpers?** The user questioned why helpers
   exist. Decide whether this task also collapses them or leaves presets as thin
   sugar that just set the knob + recording shape.
5. **Execute's scalar/create downgrade** — keep fully automatic (result-shape), or
   expose an override for the rare case? (Lean: automatic, no override.)
6. **Migration mechanics** — flip declarations in place, or keep a compat shim mapping
   old Channel/Effect → the knob during transition?

## Implementation notes (#28)

### What landed
- `enum ReplayStrategy { Execute, Substitute }` (default `Substitute`,
  `#[serde(rename_all="snake_case")]`) replaces `Channel`/`Effect`/`EntropySource`/
  `Strategy`.
- `BoundarySemantics { replay_strategy: ReplayStrategy, kind: Option<String> }` —
  `kind` is a NON-routing descriptive label ("db"/"http"/"redis") for the
  dashboard/provenance. `SemanticEvent` carries `replay_strategy` + `kind` (the old
  `channel`/`effect`/`strategy` fields are gone).
- Routing = `replay_strategy_to_execute_mode(knob, policy)`: AllLookup → Lookup for
  everything (knob ignored — demo byte-identical); SelectiveExecute → `Execute`→
  Execute, `Substitute`→Lookup. `decide_strategy`/`strategy_to_execute_mode` deleted.
- Macro param: `replay_strategy = Execute | Substitute`. The `id`/`time`/`http`
  presets default to `Substitute` and stamp `kind`. `replay`/`replay_ok`/`replay_with`
  (serialization) are UNTOUCHED. Args capture is UNTOUCHED.

### The per-site mapping is derived from the OLD `decide_strategy`, not the bullets
The "Migration map" bullets above describe a result-shape rationale (scalar →
Substitute, row/value → Execute) that does NOT match the routing the OLD declared
sites actually had. The declared redis sites and the db seam routed through
`decide_strategy` (NOT the result-shape heuristic), so the only behavior-preserving
mapping is the OLD matrix's `ExecuteMode` output. The migration used:

| old `effect` (+strategy) | old `decide_strategy` → mode (SelectiveExecute) | `replay_strategy` |
|---|---|---|
| `Read` | SeedAndExecute → Execute | **Execute** |
| `Write` | SeedAndExecute → Execute | **Execute** |
| `ReadModifyWrite` (+`SeedAndExecute`) | SeedAndExecute → Execute | **Execute** |
| `Append` | LookupAndSeed → Lookup | **Substitute** |
| `VolatileRead` | Lookup | **Substitute** |
| `Opaque` | Lookup | **Substitute** |
| db seam (every op, declared State Read/Write) | SeedAndExecute → Execute | **Execute** |
| `id`/`time`/`http` (entropy/egress) | Lookup | **Substitute** (default) |
| `eu_settlement_read`/`_write` (undeclared, op-scoped) | heuristic Execute | **Execute** |

This is a DEVIATION from the bullets for: `exists`/`delete_key`/`set*`/`llen`/
`xlen`/`setnx` (the bullets say Substitute; the OLD routing executed them, so they
are **Execute**) and `xadd`/Append (bullets say Execute; OLD routing substituted it,
so it is **Substitute**). Behavior-preservation wins: a follow-up may re-tune these
to the result-shape intent, but that is a deliberate behavior change to demo-gate,
not part of this collapse.

### Legacy fallback retained (not deleted)
`is_state_channel` / `is_read_op` survive as STRING HEURISTICS (not the taxonomy):
they still feed (a) the undeclared-site `execute_mode` fallback + `DEJA_EXECUTE_OPS`
op-scoping (for hand-written trait delegates), (b) the reads-only arg-tolerant
lookup fallback, and (c) `read_set`/`write_set` derivation in `EventBuilder::finish`.
A site that declares NOTHING (default `Substitute`, no `kind`) routes through that
heuristic so its decision is byte-identical to before; a DECLARED site (any `kind`
label, or `Execute`) routes by the knob. Deleting these heuristics is a later step
(gated on every boundary declaring), per the declarative-boundary-model doc.

## ReplayCodec (deferred serialization generalization)

`replay`/`replay_ok`/`replay_with` are the CURRENT serialization surface and were
left intact by #28 (the knob is the routing axis; codec is the orthogonal capture
axis). The end state replaces all three with a codec marker:

```rust
trait ReplayCodec {
    type Value;                                   // the boundary's return type
    fn capture(value: &Self::Value) -> serde_json::Value;     // record side
    fn reconstruct(recorded: serde_json::Value) -> Option<Self::Value>; // replay side
}
```

- A blanket `SerdeCodec<R: Serialize + DeserializeOwned>` covers the common case
  (today's `replay`). Shipped specializations: `ResultOkCodec` (today's `replay_ok`
  — capture/reconstruct only the `Ok` arm, never the non-serde error) and
  `HttpResponseCodec` (today's `replay_with` for `reqwest::Response` — rebuild from
  recorded parts). A site overrides via `replay_codec = SomeCodec`; legacy
  `recon = SomeCodec` remains accepted as an alias.
- This is what lets the `id`/`time`/`http`/`db`/`redis` preset helpers collapse into
  ONE general boundary macro (the knob + a codec marker), which is the remaining
  "one-macro" follow-on. Gated on the autoref-specialization spike (serde /
  Result-Ok / reqwest::Response / Debug-fallback) proving the marker resolution is
  clean.

## Shapes: streaming/sink (DEFERRED BUT RESERVED)

v1 ships ONE shape: a `Call` — a boundary invocation that yields a single captured
result. The core model is deliberately phrased so richer shapes are additive:

> A boundary yields an **ordered set of captured items**; a `Call` is the size-1
> case (one args → one result).

Reserved (not built) shapes, via a `shape` seam on the boundary:
- **`Source`** — a boundary that yields a SEQUENCE of items over time (a stream /
  cursor / `XREAD` loop): the captured set has size > 1, ordered.
- **`Sink`** — a boundary that CONSUMES a sequence (a producer / batch writer): the
  captured set is the ordered inputs, replayed for diffing.
- **`Session`** — a stateful multi-call handle (a connection / transaction) whose
  items are correlated across calls.

The event schema must allow this ADDITIVELY: the per-event `result` stays the
size-1 item; a sequence is expressed as an ordered run of events sharing a `shape`
+ a session/sequence id, never by changing `result`'s type. No field is removed to
add a shape later — same additive discipline the `replay_strategy`/`kind` fields
followed. State v1 contract = **args + return serde**; the codec generalization
above layers on without touching the shape seam.
