# Hyperswitch PR #12754 — assembly plan (single PR, maximal foldable scope)

Living plan for the one upstream PR. Principle: **fold in a roadmap item iff it has a
Hyperswitch-side leg that HS already has the substrate for** (Superposition client,
metrics infra, Vector, S3, the middleware stack). Standalone deja services (runner,
dashboard, compactor, store schema, ops IaC) have no HS leg — they stay in deja-lib and
are *not* "missing" from the PR.

**Secrets/PII posture (decided):** NOT solved by producer-side redaction. Record mode is
**off by default + staging-only**, and secret protection is handled as **encryption at the
Kafka-consumer boundary** (the recording infra holds the keys and decrypts). Field-level
redaction/masking is a **separate deferred workstream**, out of this PR and off the record
hot path.

## 1. Already in the PR (done)
Instrumentation across db/redis/crypto/id/time seams · hardened Kafka sink
(`deja_record_sink.rs`) · `deja_boot` wiring · envelope v2 · compose/Vector overlay ·
git-dep on all 7 crates.

## 2. Folding in (decided)

### 2a. Dependency → org + tag (closes bot Critical #1)
Move `deja-lib` to **`juspay/deja-lib`** (org-owned; user is an org member) and pin to a
signed **`vX.Y.Z` tag** instead of a bare rev. Reframes the "personal-repo supply chain"
critical as an internal tooling crate. *Gate: confirm org repo-creation access.*

### 2b. Deployment manifests — fold into the EXISTING files (not a parallel overlay)
HS already runs the substrate: `kafka0` (cp-kafka, existing topics), a `vector` service
(`docker-compose.yml:503`) mounting stock `config/vector.yaml` (3 Kafka sources →
OpenSearch sinks), and S3 via `file_storage/aws_s3.rs` + `[file_storage.aws_s3]`.
- `config/vector.yaml`: add `sources.deja_recording` (4th Kafka source, recording topic)
  + `sinks.deja_recording_s3` (`type: aws_s3`). Follows the existing source pattern; only
  the S3-type sink is new (today's sinks are all OpenSearch).
- `docker-compose.yml`: no new services — add the recording topic on `kafka0` and the
  router record-mode env **defaulting OFF** (`DEJA_MODE` unset).
- Drop the parallel kafka/vector/minio from `docker-compose.deja.yml`.

### 2c. Sink target → real S3 (configurable), MinIO only for local
The `aws_s3` sink takes `bucket`/`region` from config (HS's existing S3 convention) and
auths via the standard AWS provider chain (IAM role in deploy, keys locally).
`endpoint` overridable: **empty → real AWS S3; set → MinIO** for laptop dev.
- A **dedicated `hyperswitch-deja-recordings` bucket** (never the file-storage/product
  bucket), **sink off by default**. At-rest/in-transit secret protection rides the
  consumer-side encryption posture (above), not a producer scrub.

### 2d. Recording control plane → Superposition (HS-native, the §2.2 hook)
HS's `SuperpositionClient` (OpenFeature, dimension-aware, DB fallback, `get_cached_config`)
is the policy layer. Define keys following `consts::superposition`:
| Key | Type | Controls |
|---|---|---|
| `deja_record_enabled` | bool | per-merchant/connector/route kill-switch |
| `deja_record_sample_rate` | u32/f64 | % of matching requests recorded |
| `deja_sink_policy` | string | `block` vs `fail_open`, runtime-tunable (optional) |

**Three-layer model:** (1) cargo `deja` feature = compile (zero cost off); (2) static
TOML/secrets = sink install + Kafka/S3 endpoints ("where bytes go"); (3) Superposition =
per-request record?/rate?/for-whom? ("whether/what/how much"). Layer 3 IS the
gate-before-allocation M2 needs and the §2.2 sampling hook — same code.
**Guardrails:** fail-closed if Superposition is unreachable (never auto-start capture);
resolve once at ingress, propagate to spawned tasks via the correlation→decision registry.
- deja-side (deja-lib): `RecordSampler` trait (gate) + correlation→decision registry.
- HS-side (deja-pr): impl calling `SuperpositionClient`/`DatabaseBackedConfig` with ingress
  dimensions, gating at the `router/src/lib.rs` middleware site.

## 3. Convergence candidates — other roadmap items with an HS-side leg

| Roadmap item | HS-side leg | Converges with | Tier |
|---|---|---|---|
| **Envelope identity finalization** | `instance_id`/`session_id`/`window` fields in `deja_record_sink.rs` + `deja_boot` stamping canonical ids | M2 compound key · critique A4/A5 | **2** — pairs with deja-side emit-time gseq |
| **Producer metrics (§5.4)** | emit `deja.events_enqueued/delivered/dropped_queue_full/delivery_failed/sink_fatal` via HS metrics infra in sink/boot | M2 sink hardening · M5 observability | **2** — makes the sink observable/credible |
| **Compat version surfacing** | extend envelope `code.{deja_version,event_schema,policy}` (v2 already carries deja_version) | M3 compat gate | **3** — minor envelope extension |
| **Egress-blocked replay net** | replay service on a deny-by-default network (compose) | gap B2 (verified live) | **3** — cheap *if* compose in-tree |
| **Fresh-pg replay provisioning** | replay pg reset + candidate-ref migrations (compose) | gap B3 (verified live) | **3** — heavier; needs migration policy |

**Deferred / out of this PR:** field-level credential redaction & masking (→ handled as
Kafka-consumer-side encryption, separate workstream).

### Stays OUT (no HS leg — deja-lib only)
M1 runner/Executor/pull-protocol · M0 store schema · M4 compactor internals + catalog +
S3 lifecycle · M5 ops/ IaC (Terraform) · M6 dashboard (SPA + API + explorer + trace-up).

## 4. Recommended PR scope (tiered)
- **Tier 1 (fold now):** §2d **Superposition sampling hook** · §2a org+tag · §2b/2c
  vector+compose fold-in + real-S3 · the 2 cheap fixes (redis `expect()`, LockerMockUp comment).
- **Tier 2 (strong stretch):** envelope identity finalization + `deja_boot` ids · producer metrics.
- **Tier 3 (if appetite, compose in-tree):** egress-blocked replay net · fresh-pg provisioning · compat version surfacing.
- Discipline: bot already flagged scope — every Tier adds review surface. Keep Tier-1 the floor.

## 5. Bot review (XyneSpaces) — positions
- Critical #1 (external dep) → §2a (org + tag): closed, not deferred.
- Warning #2 (PII→S3) → record mode off-by-default + staging-only; transport/at-rest secret
  protection via **consumer-side encryption** (recording infra decrypts); field-level
  masking is a separate deferred workstream. (No producer scrub claimed.)
- Warning #3 (`{values:?}` Debug) → rebut: pre-existing upstream (`generics.rs` at tag); PR only reuses the string.
- Nit #4 (`#[instrument]` on redis) → decline: capture is via deja macros, not tracing spans (out of scope).
- Nits: redis `expect()` (replay-only path) → make fallible; LockerMockUp gate → add comment.
- Positives to lean on: zero-cost-off, crypto not instrumented (nonce-only), error_stack preserved, conditional serde.

## 6. Sequencing (because HS consumes deja via the pinned dep)
1. Land deja-side prereqs in deja-lib: `RecordSampler` trait + gate-before-allocation +
   correlation→decision registry. (Tier 2 adds: `instance_id` on `SemanticEvent` +
   emit-time gseq; writer drop/delivery counters.)
2. Move repo to `juspay/deja-lib`; cut tag `v0.2.0`.
3. HS-side on `deja-pr` (repin rev→tag): Superposition sampling+gate wiring, vector.yaml +
   docker-compose.yml fold-in + real-S3 sink, normalize `external_services` to
   `default-features=false`, the 2 cheap fixes. (Tier 2: envelope/metrics.)
4. `cargo check -p router --features deja,v1` against the public tag; update PR #12754.

## 7. Open decisions
1. Org repo-creation access for `juspay/deja-lib`.
2. Dedicated recording bucket name; consumer-side encryption scheme (topic/envelope) tracked separately.
3. Pin shared Vector to `0.54.0` vs re-verify zstd+acks on HS's `latest`.
4. How far up the Tier ladder to go given PR-size appetite.
