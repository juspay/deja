# Vendor-thin PR readiness checklist (#41)

Audit of `vendor/hyperswitch-deja-clean` deja delta (base `2026.04.21.0` → HEAD + session edits).
**pr_ready: NO.** 34 changed files are legitimately thin integration (KEEP). Actions below.
Correctness is GREEN (self-check pass=true 9/9) and must stay green — every code change is
paired with a self-check.

## REMOVE / demo-scope (demo-only, not thin integration)
1. `crates/router/src/core/payments/operations/payment_create.rs` — **VIOLATION (payment-core)**:
   +88-line EU-settlement demo (`eu_settlement_read/write` raw fred GET/SET + call in
   `PaymentCreate::get_trackers`). Revert to base. **HIGHEST RISK** — the eu-overcharge demo case is
   driven through this path; re-run self-check after (the 9/9 flow itself does not depend on it, but
   confirm).
2. `crates/redis_interface/src/lib.rs` — `KeysInterface` re-export added ONLY for the demo helpers.
   Revert after #1 (grep for other consumers first).
3. `docker-compose.deja.yml` (+ `config/vector.deja.yaml`) — demo record/replay stack. Demo-scope
   (relocate under `demo/`); update any harness paths that source them.

## MOVE to library / VERIFY placement (judgment calls — need decision)
4. `DEJA_ARCHITECTURE.md` (vendor root) → deja library docs.
5. `crates/router_env/src/logger/setup.rs::ExecutionGraphLayer` — writes a separate graph artifact
   (deja observability, not thin HS integration). Keep `DejaCorrelationLayer` in vendor; verify the
   graph-writer can move to the library.
6. `crates/router_env/src/request_id.rs` http-INGRESS capture (`capture_incoming_request`/
   `RecordingBody`/`http_incoming`) goes beyond request-id propagation (full body capture). Decide:
   keep as a thin middleware seam vs extract capture to the library (keep id seam + correlation
   anchoring in vendor). Behavior-affecting → gate on self-check.
7. `crates/router/src/connection.rs` — the router-copy replay routing hook is likely **dead** (the
   live seam is `storage_impl/src/utils.rs`, added this session). Verify no call sites, then remove.

## DOC FIXES — `docs/DEJA_RECORDING_ARCHITECTURE.md` badly drifted (apply all together)
- Sink: Kafka is THE only sink (no JSONL-primary CompositeSink). Producer: dedicated hardened
  `ThreadedProducer` (acks=all, idempotence, 30s timeout), shares only broker list. flush() is real.
- Envelope is **v2** (SCHEMA_VERSION=2: instance_id/event_time_ns/capture{mode,session_id}/code{sha,deja_version}).
- Add marker/loss-accounting (`MarkerEnvelope`, `DEJA_SINK_POLICY`, drop accounting).
- Install gate = `wants_recording() && EventsConfig::Kafka` (DEJA_SINK/DEJA_ARTIFACT_DIR not on record path).
- Vector: NO `deja_unwrap` transform; lands full v2 envelopes. S3: **zstd** (`ndjson.zst`),
  key layout `landing/v1/session=.../inst=.../`, acks=true, region us-east-1.
- §8.1 env table: drop `DEJA_SINK`/`DEJA_ARTIFACT_DIR`; add `DEJA_CODE_REF`, `DEJA_SINK_POLICY`.
- Leave ACCURATE claims (topic, broker, partition key, headers, MinIO bucket/creds, batch 2000/5s).

## PAYMENT-CORE flags
- payment_create.rs → excise (see #1).
- `hyperswitch_domain_models/src/payments/payment_attempt.rs` — cfg widen `all(v1,olap)`→`v1`;
  JUSTIFY it's deja-build-driven (build under feature=deja w/o olap), else revert.
- amazonpay/common_enums/adyen/connector_configs "premium"/"overcharge" hits = **upstream** grep
  false positives → confirm byte-identical to base, leave.
- `crates/diesel_models/src/schema.rs` — cosmetic reformat; revert to base to keep diff clean.

## Seed heuristic (OMP question) — ALREADY library, not vendor
`method_name.contains("insert")` is in `deja-record/src/replay.rs:~1779` (+ `divergence/mod.rs:~371`
update-substring) — LIBRARY, no vendor move. The vendor macro (`diesel_models/generics.rs:76-108`
`@state` arms) is the correct classification authority. **Follow-up (out of thin-PR scope):** stamp
an explicit `OpKind`/`creates_rows` onto `QuerySpec` + `SemanticEvent` so the library consumes
declared metadata instead of name substrings (this is task #18 — touches lookup keys; deferred).

## Ordered actions (each paired with verification)
1. Snapshot GREEN baseline (done: cycle 28 pass=true 9/9).
2. Revert payment_create.rs EU demo → self-check.
3. Revert redis lib.rs KeysInterface (after grep) → build ±deja + self-check.
4. Demo-scope compose/vector (update harness paths) → build.
5. Relocate DEJA_ARCHITECTURE.md.
6. Decide + apply request_id ingress + logger graph placement → self-check.
7. Verify + remove dead router/connection.rs hook → self-check (DB isolation).
8. Confirm payment_attempt.rs cfg + 4 false positives + schema.rs vs base.
9. Apply 11 doc fixes.
10. Final gate: self-check pass=true 9/9 + `git diff --stat` vs base = thin surface only.

## Risks
- payment_create removal: highest-risk; re-run self-check (don't assume).
- redis KeysInterface removal: compile break if adopted elsewhere — grep first.
- connection.rs vs utils.rs: removing the wrong copy → cross-correlation DB bleed; verify live seam.
- request_id ingress / logger graph moves: behavior-affecting (feed recording) — gate on self-check.
- payment_attempt.rs cfg: if deja-required and reverted → deja build breaks.
- doc drift is large; apply all fixes together (partial = half-stale).

## GREEN BASELINE (pre-#41, for regression attribution)
- Command: `set -a; source demo/.env; set +a; bash demo/run-self-check.sh`
- Cycle 28: pass=true, matched_correlations 9/9, VERDICT "self PASSES"; value_divergences=0,
  order_nondeterminism_warnings=0, idempotent_delete_warnings=1.
- Artifact: demo/harness-state/<run-tag>/runs/*.scorecard.json
- Every #41 checkpoint re-runs this and must stay pass=true 9/9.

## #41 CHECKPOINT PROGRESS (verified, GREEN preserved)
- CP1 payment_create.rs EU demo → reverted to base (SHA-identical); self-check pass=true 9/9 (cycle 29). DONE.
- CP2 redis_interface/lib.rs KeysInterface re-export → reverted (0 consumers post-CP1); compiles. DONE.
- CP3 schema.rs cosmetic reformat → reverted to base (SHA-identical); compiles. DONE.
- CP4 transport docs (docs/DEJA_RECORDING_ARCHITECTURE.md) → 25 code-verified fixes (doc-only). DONE.
- CP5 router/src/connection.rs hook → NOT dead. Call-site proof: ~30+ callers in router/src/db/*.rs
  (capture, role, user_role, authentication, dispute, health_check). KEEP + documented (distinct
  connection seam from storage_impl/utils.rs; both live).
- CP6 payment_attempt.rs cfg-widen (all(v1,olap)->v1) → JUSTIFIED, KEEP. Line 1861 uses bare
  Connector under `#[cfg(v1)]` impl (1702, not olap); base gate would fail under v1-no-olap;
  compile proof: `-p hyperswitch_domain_models --no-default-features --features v1` Finished. Behavior-neutral.
- CP7 four premium/overcharge grep hits (amazonpay/common_enums/adyen/connector_configs) → all
  byte-identical to base (git diff=0). Upstream false positives. No action.
- CP8 DEJA_ARCHITECTURE.md (vendor root) → relocated to docs/ (library); removed from vendor (git D);
  no vendor refs. Doc-only. DONE.
- HELD (stop-on-uncertainty): compose/vector demo-scope — harness hard-refs (demo/lib.sh:11,
  lifecycle/mod.rs:63) + vendor-root-relative mounts; AND packaging.md:192 says the HS PR SHOULD carry
  the local-dev overlay. Needs OMP decision (may be a no-op).
- HELD (OMP decision): request_id.rs ingress capture + logger ExecutionGraphLayer library extraction.

## HELD ITEMS — RESOLVED (user direction, 2026-07-02)
- compose/vector: **KEEP in vendor** (resolved as no-op). The record→Kafka→Vector→S3 push path is
  the real pipeline the hosted replay orchestrator will fetch recordings from; the overlay is how
  HS carries it for local dev (packaging.md:192 agrees). No relocation.
- request_id.rs ingress + correlation/graph layers: **defer extraction to OMP's DSL layer.**
  Hand-rolling the ~560-line move now would collide with the in-flight DSL design. Extraction map +
  DSL requirements delivered at docs/design/ingress-declarative-extraction-map.md (inventory: hook
  plumbing / id seam (#25) / framework-agnostic core / actix adapter; DSL must express deferred
  body-finalization, correlation ESTABLISHMENT, driver-on-replay, capture caps/redaction).

## REMAINING to PR-ready
1. Commit the working-tree reverts (payment_create.rs, redis lib.rs, schema.rs, DEJA_ARCHITECTURE.md
   removal) + session seam/routing fixes to the PR branch as a clean series.
2. Final gate: self-check pass=true 9/9 + git diff vs 2026.04.21.0 = thin surface only.
3. Publish mechanics (BOTH need explicit user approval — standing gate): regenerate public branch
   (export-public.sh), re-pin deja-lib rev, update PR #12754 (still pins pre-OMP maverox rev).
