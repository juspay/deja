# feat(deja): feature-gated record/replay instrumentation + Kafka recording sink

> **Draft.** Opening early for direction/feedback. Gated behind a new `deja`
> cargo feature. Current local proof has three clean isolated-parallel
> active-default replay matrices (`1783099999`, `1783111111`, `1783133333`).
> Before final push, the branch still needs the public Deja git-rev repin, final
> vendor gates after the repin, and default-build dependency audit described in
> `docs/PR_FINALIZATION.md`.

## What this is

[Déjà](https://github.com/juspay/deja) is a deterministic record/replay
harness for service boundaries. This PR adds the **record-side integration** to
Hyperswitch: with `--features deja` and `DEJA_MODE=record`, every storage/cache/
crypto/id/time boundary call emits a structured `SemanticEvent`, published to
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
   carries explicit write attribution; MGET result capture remains ok/error-only
   until the broader `ReplayCodec`/DB-dispatch follow-up. All annotations are
   attribute macros behind `#[cfg(feature = "deja")]` — no-ops when the feature
   is off.
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

Final shape: `deja` should be consumed as a **rev-pinned git dependency** on
the public Deja repo, not vendored into this tree. See `docs/PR_FINALIZATION.md`
for the current publish/repin checklist.

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
