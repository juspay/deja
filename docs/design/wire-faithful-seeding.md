# Wire-faithful seeding — capture at the protocol, once per protocol

**Status: DECIDED (direction) + SPIKE ITEMS NAMED.** The db seed round trip moves from
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
(docs/design/cache-isolation.md). Correlation comes from `deja_context` ambient state, as at every
other boundary; the wire rows are stowed for the enclosing `#[deja::db]` boundary to attach to its
event (see tape shape below).

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
