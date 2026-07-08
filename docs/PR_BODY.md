# feat(deja): feature-gated record/replay instrumentation + Kafka recording sink

> Gated behind a new `deja` cargo feature. `deja` is consumed as a rev-pinned
> git dependency on the public repo (`juspay/deja`). Local proof: an
> 8-candidate isolated-parallel replay matrix (self/benign pass 9/9 with zero
> divergences; six seeded regression classes caught, including a 3-call
> transitive derivative chain), a 5x repeated-replay determinism soak with
> byte-identical verdicts, and an on-tape neutrality proof (recording never
> reshapes scheduling; fire-and-forget tasks record at their natural
> interleaving).

## What this is

[Déjà](https://github.com/juspay/deja) is a deterministic record/replay
harness for service boundaries. This PR adds the **record-side integration** to
Hyperswitch: with `--features deja` and `DEJA_MODE=record`, every storage/cache/
crypto/id/time boundary call emits a structured `BoundaryEvent`, published to
Kafka and landed in object storage. A separate harness then replays a recorded
request stream against a candidate build and scores divergences — a regression
gate that catches behavior changes the response alone wouldn't reveal (e.g. a
dropped cache write, an altered DB insert payload).

## What's in it

1. **Feature-gated instrumentation** across the db (diesel `generic_*`), redis,
   crypto, id, time, and HTTP seams, plus correlation/request-id propagation and
   an optional execution-graph layer. DB result row images now carry producer
   column metadata (`type_oid`, `type_name`, `nullable`) when available, while
   retaining the legacy value-only compatibility shape. Redis multi-key reads
   carry explicit key read attribution in both supported backends, and `SADD`
   carries explicit write attribution. Boundary declarations use one canonical
   macro surface (`codec =` capture/reconstruct selector, `replay =` routing
   knob, explicit state-key facts); row-returning DB seams and the ingress
   boundary additionally declare volatile-column canonicalization
   (`created_at`/`last_synced`/`modified_at`) as tape metadata — replay-side
   scoring policy, zero runtime effect. Fire-and-forget request work uses
   `deja::spawn_detached`, which is a **pure lineage stamp** (task/bucket
   provenance on emitted events): recording never defers, joins, or reschedules
   anything, in any mode. All annotations are attribute macros behind
   `#[cfg(feature = "deja")]` — no-ops when the feature is off.
2. **Kafka record sink + boot wiring + envelope v2.** A deja-owned `rdkafka`
   producer (`acks=all`, idempotent, real `flush()`) publishes
   `deja.artifact_record/v2` envelopes (producer/capture/code provenance) to a
   recording topic; `deja_boot` installs it as the sole record sink in record
   mode. Loss accounting rides the same topic as `deja_sink_marker` records.
3. **Superposition ingress sampling.** In record mode, `RequestIdentifier`
   resolves `deja_record_enabled` from Hyperswitch Superposition using the
   request id as the OpenFeature targeting key and method/path as trusted
   ingress context. A `false` decision is pushed into Deja's per-correlation gate
   before request handling, so sampled-out requests skip event allocation and
   request-body capture.
4. **Replay seed/readback certificates** in the harness: replay runs emit a
   `seed_certificate` artifact that records planned seed preconditions,
   materialization outcome, and DB/Redis readback status. This is harness-side
   proof plumbing; it does not add production runtime behavior.
5. **Opt-in local-dev record-transport overlay** — a `docker-compose.deja.yml` +
   Vector config that proves the Kafka → Vector → S3 path alongside the stock
   stack. Production Vector/IaC/runner ownership remains out-of-tree.

## Impact when off (the part to re-audit before final push)

- Intended shape: `deja` remains an **optional** dependency, and default builds
  do not compile/link the Deja runtime.
- Instrumentation sites are guarded by `#[cfg(feature = "deja")]`; final PR
  prep still needs the default-build dependency/codegen audit after the public
  git-dep repin.
- No intended change to default runtime behavior, configs, or public API.

## Dependency

`deja` is consumed as a **rev-pinned git dependency**:
`deja = { git = "https://github.com/juspay/deja", rev = "2f8e3bb52cd341e83d001e4f262eef4b3d54b4ff", optional = true, default-features = false }`
across the eight integrating crates. Default builds do not compile or link the
Deja runtime (verified: `cargo check -p router` and dependency-graph audit on
the pinned tree).

## Follow-up scope

Merchant-scoped sampling is still a follow-up: raw ingress middleware does not
parse bodies or trust merchant-like headers, so merchant dimensions must be
resolved later at an authenticated merchant-aware seam if upstream wants that
rollout shape. The current PR samples by request/correlation id only.

## Status / asks

- [ ] Direction: is a feature-gated record/replay hook something you'd take
      upstream, or prefer maintained as a fork/out-of-tree integration?
- [ ] Is the included local-dev compose/Vector overlay acceptable in-tree, or
      would you prefer it split after this review?
- [ ] Is request-id Superposition sampling sufficient for this PR, with
      merchant-scoped sampling left as a follow-up unless required?
- [ ] `Cargo.lock` delta — keep in the PR or split?
