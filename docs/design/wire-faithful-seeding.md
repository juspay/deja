# Wire-faithful seeding — capture at the protocol, once per protocol

**Status: DECIDED + pg half BUILT (deja side).** `DejaLoadConnection<C>` ships as the
`deja-diesel` crate (facade feature `diesel-pg`); the tape carries the physical image on the row
image's `wire`/`wire_format` fields; the seeder lands wire-carrying rows through binary COPY. Two
deltas against the sketch below, both from the spike: (1) the capture→boundary handoff is IN-BAND,
NOT ambient — the captured query helpers (`deja::db::get_result_captured` and friends) take the
wire rows inside the same `conn.run` closure that executed the statement and return them PAIRED
with the query result; the pair rides the future's value to the boundary
(`deja-runtime/src/wire_capture.rs`; two earlier ambient shapes of this handoff — a statement-keyed
bounded global queue, then a per-checkout connection slot found through a task-local scope — each
failed in production, see the handoff section); (2) the container-OID hazard is
resolved as a per-VALUE eligibility rule: verbatim COPY for captured type OIDs < 10000 (built-in,
cluster-stable, including built-in containers whose embedded element OIDs are equally stable) plus
user-defined values whose wire bytes equal the semantic string byte-for-byte (the pg enum label
case, proven per value); everything else falls back to the serde INSERT path, fail-closed arm
intact. The vendor swap (hyperswitch pool alias) is still open under #35. The db seed round trip moves from
serde JSON to the database's own wire format, captured by a `DejaLoadConnection<C>` wrapper at the
diesel connection seam and seeded back through postgres's paired input functions. Redis gets the
same treatment by completing the backward half of the transform `RedisWireValue` already carries
forward (#39). Follows the seeding audit (docs/design/seeding-audit.md); fixes the class behind
issue #35; the vocabulary cleanup riding the same stack is #40.

## Why the problem exists at all (the codec-mismatch theorem)

Today a db read's value makes this journey onto the tape and back into a store:

```
pg wire bytes → FromSql → Rust struct → serde_json → tape → hand-rolled JSON→SQL renderer → INSERT
```

The round trip the seeder NEEDS is `pg → value → pg`. The round trip it HAS is
`pg → diesel → serde → renderer → pg`. Serde and the database are two **independent projections of
the same Rust struct**: `#[derive(Serialize)]` decides the JSON shape, the vendor's
`ToSql`/`FromSql` impls decide the SQL shape, and nothing forces them to agree. Every point of
disagreement becomes one of exactly two failure classes:

1. **A seed failure**, when the renderer notices. Three patches have already shipped for these,
   one type-shape each: the bytea inner-array unwrap, the `json[]` array-constructor cast
   (`pg_json_array_literal`, lifecycle/mod.rs), and now the externally-tagged-enum case — 153
   `payment_attempt` entries failing because `ConnectorTransactionId` serializes as
   `{"TxnId": …}` into a `Nullable<Varchar>` column, which the renderer's fail-closed arm
   correctly refuses (#35: 51+ correlations fork at `find_one(payment_attempt) → NotFound`).
2. **A silent wrong value**, when it doesn't. This is the worse class because it is invisible to
   the fail-closed renderer: a `#[serde(rename)]` on a vendor field makes the JSON key stop
   matching the column name; a serde `with`-adapter changes a scalar's text form. The seeded row
   is well-formed SQL holding a value the database never produced, and the divergence it causes
   points at the candidate.

The tempting fix — teach the seeder more shapes — cannot terminate. The inverse of a serde shape
lives in the vendor type's `ToSql` impl, which the seeder does not have and must not re-implement:
a seeder-side transform that is complete is a second copy of every vendor codec, maintained by
hand, drifting independently. The three shipped patches above are the first three terms of that
infinite series. The fail-closed refusal stays (it is what made #35 visible instead of silent);
the input it refuses has to change.

## The decision: capture at the protocol, once per protocol

A value is guaranteed to round-trip through exactly one pair of functions: the ones the store
itself owns. For postgres that pair is the type's output/input (text) or send/receive (binary)
functions; for redis it is RESP itself. So the capture point moves to where those representations
are still intact — the protocol boundary — and the seeder replays the store's own output back at
it. No per-type knowledge on either side; the store parses what the store produced.

### Postgres: `DejaLoadConnection<C>`

A diesel `Connection`/`LoadConnection` wrapper **in the deja library**, generic over the wrapped
connection. It delegates everything; its one addition is a cursor adapter that, for each result
row, walks the columns through diesel's generic row API **before** `FromSql` consumes them:

```rust
// diesel 2.2.10, src/pg/connection/row.rs — the surface the adapter reads:
impl<'a> Field<'a, Pg> for PgField<'a> {
    fn field_name(&self) -> Option<&str> { … }
    fn value(&self) -> Option<PgValue<'_>> { … }   // PgValue: as_bytes() + get_oid()
}
```

Per column that yields `(name, type_oid, wire bytes)` — the exact bytes postgres sent, with the
authoritative type identity, captured without deserializing anything. The capture is a pure
observer (the row is handed onward untouched), the same discipline as the imc lookup tee
(docs/design/cache-isolation.md). The wire rows are stowed for the enclosing `#[deja::db]`
boundary to attach to its event (see tape shape below).

### The capture→boundary handoff: in-band, paired by lexical scope (#50)

The wrapper's cursor runs on a `spawn_blocking` thread (async-bb8-diesel) while the boundary's
result producer runs on the async task afterwards, so the capture must cross between the two. Two
ambient shapes of that crossing shipped and failed in production, each locally correct on both
halves:

1. **A process-global `VecDeque` keyed on the rendered statement**, bounded at `MAX_PENDING = 64`
   with oldest-first eviction and 10-second aging. PR #51 measured the failure exactly: the
   capture gate is process-level, so every statement the process runs publishes — including the
   majority from requests the sampler excluded, which never run a result producer and never take —
   and those unclaimed captures fill the queue and evict the recorded request's image. Survives 63
   competing statements in its window; lost at 64. On a live pod that is every recorded request:
   the reference recording carried zero physical images and seeding silently fell back to serde
   (45 `payment_attempt` refusals).
2. **A slot on the connection, found through ambient task state**: the host installed a fresh
   `WireSlot` per pool checkout and registered it in a tokio task-local whose scope actix
   middleware wrapped around each request; `recorded_output` took from the task's current slot.
   Nothing to evict — and the deployed router still produced ZERO captures. The most plausible
   cause is a scope that was never active around the paths that mattered; it was never diagnosed
   in the field because the handoff's self-counts were process-internal, exported nowhere. The
   root cause is moot once the mechanism is gone.

Both failures are one defect wearing two mechanisms: a producer/consumer contract carried by
AMBIENT state — a shared queue, a task-local handle — where each half can pass its own tests while
the connective tissue silently fails (evicted; out of scope). The replacement deletes the ambient
state instead of hardening it: **the handoff is IN-BAND**. async-bb8-diesel's combinators each
expand to `conn.run(|c| query.method(c))`; the deja captured helpers
(`deja::db::get_result_captured` / `get_results_captured` / `first_captured`) do the same run and,
still inside that closure — same blocking thread, same stack frame, `&mut` on the very connection
that captured — take the rows back out and return them PAIRED with the query result. The pair
rides the future's value to the boundary, whose `result =` expression hands both halves to
`recorded_output_with_wire`. Pairing is lexical scope, so cross-statement or cross-request
misattachment is not mitigated; it is unrepresentable. The `WireSlot` survives only as an
implementation detail INSIDE the wrapper (the cursor cannot borrow a field of the connection it
reads); `load` empties it before executing, so a take yields that statement's rows or nothing —
never a leftover from a pooled connection's previous lease. Deleted with the ambient designs: the
registry and its sql join key, the `LIMIT $n` normalization, `MAX_PENDING`/`MAX_AGE`, the FIFO
tie-break, the per-checkout install, the task-local scope and its middleware wrapper, and the
self-counters whose non-export made failure 2 undiagnosable.

The accepted waste, unchanged in kind from both predecessors: while the process records, capture
runs for sampled-out requests too (the gate is process-level because the blocking thread has no
ambient correlation); their rows ride the pair and are dropped immediately when the boundary
declines to record — one result set's bytes, freed within the same call, the simplest possible
lifecycle for the same waste.

One alternative was evaluated and rejected: emitting the wire image as its own durable record at
capture time (a tape-side join instead of an in-process handoff). A wire image is only meaningful
paired with a SPECIFIC boundary event, so a durable side-record re-imports offline exactly the
join this design deletes — (correlation, sql, occurrence) matching plus the `LIMIT` normalization
— and adds a new record kind through all six pipeline stages (sink, upload, compaction, ingest,
seed-planning, dashboard), including the ingest-probe class of bug that produced the graph-node
loss. The in-band pair keeps the pairing where both halves already exist, in process, at the
moment they are adjacent.

Seeding then becomes: hand postgres back its own output, with the cast the catalog already knows
(`INSERT … VALUES ('<wire text>'::<type>, …)` in the text world; see the binary section for what
diesel actually gives us). That statement is correct for **every column type, including ones that
do not exist yet**, because the only parser involved is the one that produced the value. The
renderer keeps its fail-closed arm for old tapes; wire-carrying tapes never reach it.

### Redis: complete the backward half (#39)

Redis already crossed this bridge on the forward side: `RedisWireValue` is the typed RESP value on
the tape (the "typed shared codec, not string re-parsing" decision), which is why the audit run
reports `skipped: 52` loudly instead of seeding wrapper text. What is missing is only the backward
transform — non-scalar variants have no write command. Part 3 of this stack completes it as an
exhaustive match (Map → `HSET`, Array → `RPUSH`/`ZADD`, Set → `SADD`, Null → nothing), with the
same discipline: an unmappable variant is a loud skip, never a stringified guess.

## Why an owned wire type, not the client library's (the `DejaRedisValue` question)

A fair challenge: `DejaRedisValue` is a type deja invented. Why mirror the
client's value enum instead of using the client's own types — wouldn't the
library's type information give drift-detection for free, the same way the pg
plan feeds back exactly what the protocol produced?

Three facts decide it, and the first one alone is sufficient:

1. **There are two client libraries in this codebase, today.** Hyperswitch's
   `redis_interface` compiles against fred (`module/fred/`) *and* redis-rs
   (`module/redis_rs/`). The tape must replay across builds regardless of which
   client produced it — a fred-shaped tape cannot seed a redis-rs build. The
   neutral type is not a stylistic choice; it is forced by the module structure.
   Client libraries are replaceable; the protocol is the stable interface. That
   is the same reason the pg side captures wire bytes + OID rather than
   diesel's internal value types.

2. **The drift alarm the client's types would provide already exists — at the
   conversion, where it belongs.** `From<fred::RedisValue> for DejaRedisValue`
   (`redis_interface/src/module/fred/commands.rs`) is an exhaustive match with
   no wildcard arm. If a client upgrade adds a variant, that `From` stops
   compiling and forces a mapping decision. The mirror plus an exhaustive
   conversion IS the compile-time drift detector; a `_` arm anywhere in these
   conversions would be a bug against this design.

3. **The tape is a persistence format, and its schema must be sovereign.**
   Serializing a third-party enum directly ties bytes-on-tape to that crate's
   semver: a minor bump that renames or restructures a variant would rewrite
   the tape format silently — drift landing in stored artifacts instead of
   surfacing as a compile error. Owning the wire type means the tape changes
   only when deja changes it, deliberately, with a schema version.

The symmetry the challenge asks for does hold — one level down. Capture
converts client → protocol-shaped tape value; replay converts tape value →
client (`TryFrom<DejaRedisValue> for RedisValue`) and lets the client write it.
Both directions pass through the client's own types at the edges, with the
protocol shape — not the client shape — as the thing that persists. Feed back
what the protocol produced: RESP values for redis, `typsend` bytes for pg.

One correction this review surfaced (tracked as issue #45): today the owned
type exists TWICE — `RedisWireValue` in the deja crate and a private
`DejaRedisValue` twin inside each of hyperswitch's client modules, coupled only
by variant-name agreement through serde. That is a one-definition violation and
exactly the unchecked drift this section argues against; the exhaustive `From`
protects client→twin but nothing checks twin→`RedisWireValue`. The end state:
`RedisWireValue` is the ONLY definition, and the client conversions live next
to it as optional deja cargo features (`fred`, `redis-rs`) — the orphan rule
permits it since the target type is deja's, and single-lockfile pinning keeps
client versions aligned. The vendor twins get deleted; capture and replay call
`.into()`/`.try_into()`, and the whole chain — client enum → tape → seeder →
client write — is checked by one compiler run.

## Tape shape: two representations, one escape hatch

The carrier already exists and is already dead code waiting for this:
`deja::db::row_image_payload_with_metadata` accepts a `&[DbColumnMetadata]` slice that
`deja::db::recorded_output` has passed as `&[]` since the day it landed, and the seeder's
`db_row_images_from_typed_payload` already prefers producer metadata when present and falls back
otherwise. The wire capture populates that slice: per column, alongside the serde value,

```json
{ "column": "connector_transaction_id", "type_oid": 1043, "wire": "…", "format": "binary" }
```

This is a deliberate **two-representation split**:

- **Semantic (serde)** — stays exactly as recorded today. It is what identity, diffing, and
  Substitute consume: human-readable, structurally comparable, the shape replay verdicts are
  computed over.
- **Physical (wire)** — consumed by seeding **only**. Never diffed, never substituted; it exists
  so the store can be rebuilt byte-faithfully.

Old tapes (no wire metadata) fall back to the current serde renderer path, fail-closed arm and
all. Per the no-legacy-compat policy this is a fallback, not a compatibility commitment:
re-record rather than teach the new path old shapes.

### Which image an RMW read seeds

A read-modify-write boundary (`generic_update_with_results`) carries the same state keys in its
read set and its write set, so its keys describe a row that existed *before* the update. The
pre-image is therefore the right precondition, and an explicit `pre_image` wins whenever the
producer captured one.

No producer captures one, and none can from the result alone: the pre-state is absent from an
`UPDATE … RETURNING` response, and postgres before 18 has no `RETURNING OLD.*`.

It does not need capturing. A correlation that updates a row has almost always READ it first, so
the pre-state is already on the tape a few events earlier — **216 of 217 read-modify-write rows on
a sandbox tape resolve to an earlier observation, and in every one of those the earlier state
differs from the post state.** Planning therefore carries the last image seen for each row and
resolves a read-modify-write key against it, per row, so a multi-row update mixes correctly. The
precedence is: an explicit `pre_image` when a producer ever captures one → the row's last observed
state → the post-image as a stand-in when nothing earlier was observed (that last tier fired once
on that tape; an absent row would make the replayed write vanish, which is worse).

Seeding the post-image instead is what made an earlier read diverge: two entries denoting one row
raced into the store, `ON CONFLICT DO NOTHING` kept whichever landed first, and reads of the
pre-state returned post-state values — 13 `payment_methods` timestamp divergences and their 13
readback misses. Materialization now also orders entries by the sequence of the event that
produced them, so "first" means first *observed* rather than first in key-string order.

### Row identity comes from the schema, and a key can have several columns

Resolving a read-modify-write key to the row's prior observation requires knowing which row an
image describes — row identity. That is a fact about the SCHEMA, so it is read from the schema
(`pg_index.indisprimary`, the constraint the database itself enforces) rather than listed in the
engine: the recorder asks at pool construction, the orchestrator asks during its catalog read, and
both feed one registry. A hardcoded `table → column` map preceded this and was wrong in two ways
at once — it put one application's tables inside a generic engine, and it could not express a
composite key at all.

Composite keys are not hypothetical here: `payment_intent` is keyed by `(payment_id, merchant_id)`
and `payment_attempt` by `(attempt_id, merchant_id)`. So a row key carries the key's columns and
values in schema order, and its wire form appends them —
`db_row:<table>:<column>:<value>[:<column>:<value>]*` — which renders a single-column key
byte-identically to the one-column grammar this replaced, so recordings made under it still read.

Everything requires the WHOLE key. A row carrying only part of one produces no row key and falls
back to the query fingerprint, because half a composite key does not identify a row: a predicate
on `payment_id` alone would read back a different merchant's row and call it a match.

## The binary-format question (decided: capture binary verbatim, seed through binary COPY)

The load-bearing complication: **diesel receives results in binary format, always.** This is not
configurable from a wrapper — diesel 2.2.10 hardcodes result format `1` (binary) as the last
argument of both prepared-statement execution paths (`Statement::execute`,
`src/pg/connection/stmt/mod.rs`, the literal `1` passed to `PQsendQueryPrepared` /
`PQsendQueryParams`), and its pg `FromSql` impls parse binary (e.g. integers via
`read_i32::<NetworkEndian>`, `src/pg/types/integers.rs:67`). So the bytes `DejaLoadConnection`
sees are `typsend` output, not the `typoutput` text psql shows. The options:

**(a) Request text-format results for captured queries — rejected.** The format flag sits two
private layers below the wrapper, and flipping it would break the app path anyway: diesel's
`FromSql` impls cannot parse text. The only way a wrapper gets text is to re-issue the query at
format 0 — a second execution of every read on the record path, which violates the pure-observer
rule (and doubles read load, and races the first execution for non-repeatable reads).

**(b) Convert binary→text ourselves — rejected.** A Rust-side decode table (hand-rolled or via
`postgres-protocol`'s decoders) is a third independent projection of the type system — the exact
class of artifact this design exists to delete. It would be correct today and drift tomorrow.

**(c) Capture binary verbatim, hex-encoded, and seed via `COPY … (FORMAT binary)` — RECOMMENDED.**
The symmetry that makes text seeding correct exists in binary too, as `typsend`/`typreceive`:
result-format-1 values and binary COPY fields are produced and consumed by the *same pair* of
per-type functions. A binary COPY field is `int32 length + typsend bytes` — precisely what
`PgValue::as_bytes()` captured. The seeder writes the COPY stream (fixed 19-byte header, then
per-row field counts and captured bytes) and ships it through the existing `psql` transport
(`\copy <schema>.<table> from … with (format binary)`), so no transport redesign (that's #26).
Postgres parses its own output; the renderer's type-shape knowledge requirement drops to zero.

Honest caveats, and the spike that settles them:

- **Cross-version/cross-cluster drift.** `typsend` output is stable for core types in practice,
  but the recording store and the replay sidecar are not the same cluster (sidecars pin the
  merchant-chart pg; the recorder may sit on a different major). The spike must run an OID census
  over a real tape (the audit run's artifacts suffice) and binary-COPY each observed type across
  the actual version pair.
- **Container types embed element OIDs.** A binary array value carries its element type's OID in
  the header; for user-defined element types (enums), the recording cluster's OID will not match
  the replay cluster's. The fix is protocol-level, not per-type — rewrite the one OID field in
  the container header from the catalog — but it must be built and tested, not assumed. Plain
  enum columns are safe (their binary form is the label text).
- **The wrapper's trait surface.** `LoadConnection::load` is public only under diesel's
  `i-implement-a-third-party-backend-and-opt-into-breaking-changes` feature
  (`src/connection/mod.rs:436-439`), so the deja library takes that feature on its diesel
  dependency; connection wrappers on this surface are established practice (diesel-tracing's
  instrumented connections). Both `DefaultLoadingMode` and `PgRowByRowLoadingMode` cursors need
  the tee. A spike proves the wrapper compiles against `PgConnection` and captures under both
  loading modes before anything lands in the vendor.

Until the spike closes, the recommendation stands as: binary verbatim capture is the only option
that is simultaneously zero-overhead on record, complete over the type system, and free of a
deja-owned decode table. If the version-pair census turns up a genuinely unstable type, the
fallback within the same design is per-column `format` tagging with a text conversion **performed
by postgres itself** at seed time for the affected columns — never a Rust-side decoder.

## Adoption inversion: one swap at pool construction

The macro instrumentation reaches call sites through ~37 declaration points because *meaning*
(operation, state axis, keys) lives at call sites. Wire capture needs no meaning — only bytes —
so it adopts at the narrowest point instead: the connection type named at pool construction.
In hyperswitch that is one type alias:

```rust
// crates/storage_impl/src/database/store.rs:15
pub type PgPool = bb8::Pool<async_bb8_diesel::ConnectionManager<PgConnection>>;
//                                     becomes, feature = "deja":
pub type PgPool = bb8::Pool<async_bb8_diesel::ConnectionManager<deja::DejaLoadConnection<PgConnection>>>;
```

Every query on every pooled connection is covered, including call sites no macro was ever written
for — which is the right shape for apps *without* a generic query layer, where per-site macro
adoption would be the whole codebase. The division of labor is: **adapters for coverage, macros
for meaning.** The macro layer keeps declaring what an operation is (identity, axis, replay
strategy); the adapter guarantees that whatever rows flowed, their physical images are on the
tape. Vendor footprint: that one line plus the deja pin bump.

## Scaling boundary: per-protocol, never per-type

The unit of work this design admits is **one adapter per store protocol** — pg's is
`DejaLoadConnection`, redis's is `RedisWireValue` + the #39 seeder, a future backend brings its
own wire pair. The unit of work it forbids is the one the old design demanded: one renderer patch
per vendor type shape, forever. Anything that looks like "teach the seeder about type X" is a
regression to the rejected series; the answer is "does X's protocol adapter exist yet".

## Relationship to the open issues

- **#35** (payment_attempt tagged enum): fixed by this design's fork (a) — the producer-metadata
  path comes alive with wire values; the renderer never sees the serde shape.
- **#39** (redis RESP seeder): part 3 of this stack; the same principle's redis half.
- **#40** (rename `masked`): part 2 of this stack; vocabulary cleanup in the seed certificate
  this design's accounting rides on.
- **#26** (store transport): unchanged; binary COPY flows through the existing `psql` exec.
