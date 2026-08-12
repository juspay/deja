# The in-memory cache is a boundary — put the seam on the type

The router's L1 cache is a state channel: a read of process-local state whose
answer depends on what an earlier request left behind. Deja's model says every
such read crosses an instrumented boundary, and the tape decides its value on
replay. For one of four read paths that is true. For the rest, the read is
invisible, and the consequences are not subtle.

## What the gap costs, measured

Sandbox run `rp-sbx-fb699c1181-fb699c1-08101431-in-0810210113104`: **86 of 100
correlations returned `HE_02: Business profile with the given id … does not
exist`**, against a baseline that returned 200. Not one of those correlations
had a business_profile seed entry, and each made exactly one DB call the
recording never made. That call's span names the cause outright:

```
get_trackers > find_business_profile_by_profile_id
  > get_or_populate_in_memory_with_transform > get_or_populate_redis
```

`get_or_populate_in_memory_with_transform` reads the L1 cache directly
(`storage_impl/src/redis/cache.rs:492`, `cache.get_val::<T>(…)`), while its
sibling `get_or_populate_in_memory` (line 437) goes through the instrumented
`deja_in_memory_get`. So on record the L1 hit emits nothing — no event, no
read-set key — and on replay the same uninstrumented read misses a cold cache
and falls through to redis (empty namespace) and then pg (empty schema).

In the same request, the three cache reads that *do* cross the boundary — api
key, merchant key store, merchant account — substitute correctly from tape.
The instrumentation works. It is the placement that fails.

A day went into seed fidelity before this was found, because the visible symptom
was a seeding metric. The precondition was never a database row; it was cache
warmth.

## The blind set

A sweep of production code (the remaining `.get_val` sites are `#[cfg(test)]`):

| what | where | cached value |
| --- | --- | --- |
| L1 read | `cache.rs:492` (transform variant) | domain `Profile` — `Clone, Debug` only |
| L1 read | `router/core/payments/routing.rs:1821` | `Arc<CachedAlgorithm>` — no derives |
| L1 read | `router/core/payments/routing.rs:2056` | `Arc<ConstraintGraph<DirValue>>` — `Debug` only |
| L1 write | `Cache::push` (274) — every caller | — |
| L1 invalidate | `Cache::remove` (302) — every caller | — |
| process cache | `FX_EXCHANGE_RATES_CACHE`, `CACHED_JWKS` | — |

Population and invalidation being uninstrumented is its own hole: a candidate
that forgets to invalidate a cache cannot currently be caught.

## Decision 1: the seam goes on `Cache`, not on its callers

`Cache::get_val` / `push` / `remove` are the only methods that touch the
underlying store (`self.inner.get` / `insert`). Instrumenting the three methods
covers every present and future caller **by construction**, where
instrumenting call sites loses this race every time a fourth helper appears —
the recurring bug shape this repo already has standing defenses against (one
seam instead of a threaded parameter). CI greps `self.inner.` outside those
three methods, the same way the tape-path invariant is enforced.

The helper-level `deja_in_memory_get` boundary is deleted; its declaration
moves onto `get_val`.

A consequence worth stating: once `get_val` is substituted, **the replay pod's
own cache warmth stops affecting behaviour**. Cold, warm, or warmed by another
correlation — the tape answers every read. That removes the whole class, not
just this instance.

## Decision 2: what each cache holds decides whether it can be captured

The two variants exist for a reason, and it is exactly the reason one is
capturable:

- `get_or_populate_in_memory` caches the **pre-transform** value — the diesel
  model (`merchant_account.rs:207` caches diesel `MerchantAccount`; decryption
  runs *after* the cache read). Diesel models derive serde and hold
  `Option<Encryption>` / `Option<Secret<String>>`. Lossless through
  `SerdeCodec`, which is why the instrumented path works today.
- `get_or_populate_in_memory_with_transform` caches the **post-transform**
  value — the decrypted domain model — so decryption is paid once per
  population rather than once per read (its own doc comment says so). That
  value is not serde-able, which is why it was never instrumented.

So `get_val` carries `SerdeCodec` over `Option<T>` and requires
`T: Serialize + DeserializeOwned`. Every pre-transform cache already satisfies
it. Domain `Profile` must gain the derives.

### `Encryptable` must be captured exactly, not through its existing impls

`Encryptable<T>` (`common_utils/src/crypto.rs:678`) holds two halves: `inner`
(the decrypted value) and `encrypted` (the ciphertext). It already has serde
impls, and **they are lossy on purpose**:

```rust
// crypto.rs:748 — serializes ONLY the inner value
impl<T: Clone> Serialize for Encryptable<T> { … self.inner.serialize(s) }

// crypto.rs:760 — reconstructs with an EMPTY ciphertext
impl<'de, T: Clone> Deserialize<'de> for Encryptable<T> {
    … Ok(Self { inner, encrypted: Secret::new(Vec::new()) })
}
```

That asymmetry is correct for its purpose — API responses must expose the
value, never the ciphertext — and fatal for capture. A round trip through it
returns a value the recording never had, and `into_encrypted()` has 25 callers,
several on write-back paths. Reaching for those impls would ship the same class
of bug just removed from DB seeding: a codec that is locally reasonable and
silently lossy.

Therefore every `Encryptable` field on `Profile` captures through an **exact
wire helper** carrying both halves, applied per field with `#[serde(with = …)]`.
The public serde behaviour of `Encryptable` does not change; the helper is
additive. The fields, under both API versions:

| version | fields |
| --- | --- |
| v1 (`business_profile.rs:53,70,87`) | `outgoing_webhook_custom_http_headers`, `card_testing_secret_key`, `network_tokenization_credentials` |
| v2 (`business_profile.rs:620,640`) | `outgoing_webhook_custom_http_headers`, `card_testing_secret_key` |

Both `Profile` structs derive only `Clone, Debug` today, and both need the
derives — the cache is behind `accounts_cache` in both, so a v2 build has the
same blind read. `ProfileSetter` (v2) carries the same fields but is a
constructor input, not a cached value, and needs nothing.

Whether a payment flow ever reads the ciphertext off a cached profile is not
the question. An exact codec costs one helper module; a lossy one costs another
week the next time a field starts mattering.

## Decision 3: what this change deliberately does not do

**~~`ROUTING_CACHE` and `CGRAPH_CACHE` stay blind~~ — SUPERSEDED, same day.**
This section originally deferred those two caches because `ConstraintGraph`
derived only `Debug` and serializing a graph looked like a project of its own.
It turned out not to be: `value_map` is derivable from `nodes` and the two
diagnostic maps have no readers outside their crate, so the graph carries its
evaluation state and rebuilds the index on the way in. Both caches are
instrumented and substituted.

The cost of doing it is on record, because it was larger than this document
predicted. Making the graph substitutable put a value on the tape that could
not be read back — `DirValue::Connector` and `DirKeyKind::Connector` carried
`skip_deserializing`, harmless for years because nothing ever deserialized a
`DirValue` — and reconstruction failed for the whole graph, fail-stopping the
boundary and killing the worker mid-request. Thirty-three replayed requests in
one sandbox run, sixteen in the next, and two cycles to find. The guard that
would have caught it in half a second now exists
(`crates/router/tests/deja_tape_conformance.rs`): point it at a real recording
and every value replay would substitute must reconstruct. Run it before
changing a captured type or a codec.

A measured cost worth knowing: `CGRAPH_CACHE` values are ~311 KB each, ~2 reads
per affected request, and about 60% of all imc bytes on the tape. Sampled
traffic only — the boundary is inert for a sampled-out request — and the honest
optimization is content-addressed payload dedup (those reads are mostly the
same graph re-serialized), not dropping the capture: a forced rebuild would
make the candidate's routing only as good as the seeded inputs.

**No forced miss, and no cache-population divergence class.** Both were
considered and both are unnecessary here. A *forced miss* means returning
`None` on replay even though the recording hit, so the code rebuilds the value
from the store; it is only required when the codec cannot carry the cached
value. It drags in a second mechanism — the store reads that follow a forced
miss are calls the recording never made, so the scorer would need a divergence
kind meaning "expected consequence of a miss we forced". With a codec that
carries the value, a recorded hit simply substitutes, and neither exists.

**No case-versus-session scope work.** This deserves recording because it was
the plan an hour before this document: seeding shared setup rows into every
correlation, on the theory that a cold-cache fallthrough needs the store to
answer. Instrumenting the read removes the premise. Once the hit itself is on
the tape, **every correlation is self-sufficient** — correlation B substitutes
its own recorded cache hit and never queries for a profile row at all. The
scope question is real and will return for other reasons; it is not this bug's
fix, and building it here would have been a third partial fix aimed at a
symptom.

## Accounting

Every recorded cache read must resolve to a substituted value or a named
failure — the existing boundary accounting covers this once the reads are
boundaries at all. Two additions:

- The CI grep above, so a blind read is a build failure rather than a silent
  divergence discovered four deploys later.
- `push` and `remove` capture their identity (cache name + key), which makes a
  missing invalidation a visible divergence instead of a mystery.

## Verification, and what it showed

1. Feature-off build unchanged; `just verify` green; the three CI feature tuples
   (`deja,v1`, feature-off, `release,v2,redis-rs`).
2. Tape conformance before pushing a codec or captured-type change: every value
   replay would substitute must reconstruct (see Decision 3).
3. Sandbox: the claim was narrow and falsifiable — `HE_02` disappears and the 86
   walled correlations get past the profile lookup.

**Outcome (shipped, measured):** the wall is gone. On the next sandbox run
**novel calls went to zero** (from 57), `imc` matched 733, and the keymanager
boundary landed at 733/0 with no ciphertext divergences — so both seams this
document proposed do what it claimed. Matched correlations went 6/100 → 27/94.

What the run then exposed is the honest sequel: those requests died further in,
on the graph-deserialization defect described in Decision 3, and behind that on
schema drift in the recording environment (a column default and an un-applied
migration the replay database does not share). Neither is this change's doing;
both were invisible while the wall stood. That is the shape to expect from
closing a blind seam — it does not produce a clean run, it produces the next
honest question.
