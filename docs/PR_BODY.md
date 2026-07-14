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
Hyperswitch: with `--features deja` and typed router settings
(`[deja] mode = "record"`), every storage/cache/
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
2. **Typed settings + boot wiring + Kafka record sink (envelope v2).** All
   runtime configuration flows through `Settings.deja` (mode
   `disabled|record|replay`, `recording.kafka.*`, `replay.*`, `sampler`,
   `identity`, `writer`) — standard config files/env overrides, no bespoke
   process-env parsing. `deja_boot` installs the process-wide hook before
   state construction; **record** misconfiguration fails open (disabled hook,
   payments never blocked), **replay** misconfiguration fails loud and aborts
   boot (a replay rig must never silently run live). A deja-owned `rdkafka`
   producer (`acks=all`, idempotent, real `flush()`) publishes
   `deja.artifact_record/v2` envelopes (producer/capture/code provenance) to a
   recording topic. Loss accounting rides the same topic as `deja_sink_marker`
   records.
3. **Superposition ingress sampling, production-safe by default.** In record
   mode, `RequestIdentifier` resolves the `deja_record` key from Hyperswitch
   Superposition using the request id as the OpenFeature targeting key and
   method/path as trusted ingress context. The sampler ships disabled
   (`deja.sampler.enabled = false`), and when the sampling source is
   unavailable the configured `fail_closed` default decides (`true` — don't
   record — in every shipped config). A `false` decision is pushed into Deja's
   per-correlation gate before request handling, so sampled-out requests skip
   event allocation and request-body capture.
4. **Replay seed/readback certificates** in the harness: replay runs emit a
   `seed_certificate` artifact that records planned seed preconditions,
   materialization outcome, and DB/Redis readback status. This is harness-side
   proof plumbing; it does not add production runtime behavior.
5. **No harness artifacts in-tree.** The local record-transport rig
   (compose/Vector overlay) lives with the replay harness, out of this
   repository; the PR ships only code, sample-config `[deja]` blocks, and
   tests. Production Vector/IaC/runner ownership likewise remains
   out-of-tree.

## Impact when off

- `deja` is an **optional** dependency; default builds carry zero deja crates
  in the dependency graph (`cargo tree` gate) and every dep used only by
  instrumentation (`bytes`, `serde_json`, `deja`) is gated behind the feature.
- Instrumentation sites are `#[cfg(feature = "deja")]`; serde derives on
  shared models are unconditional, so enabling the feature in one crate cannot
  re-shape another crate's types through feature unification.
- Config purity is pinned by a test: a feature-off router loads byte-identical
  `Settings` with or without `[deja]` tables present in its config files.

## Dependency

`deja` is consumed as a **rev-pinned git dependency**:
`deja = { git = "https://github.com/juspay/deja", rev = "187650d84db02186ce13ca3134f8892243a63744", optional = true, default-features = false }`
across the seven integrating crates. Default builds do not compile or link the
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
- [ ] Is request-id Superposition sampling sufficient for this PR, with
      merchant-scoped sampling left as a follow-up unless required?
- [ ] `Cargo.lock` delta — keep in the PR or split?
