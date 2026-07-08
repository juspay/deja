# Phase 2 — The recording store (session mode)

> Second implementation slice of [REPLAY_PLATFORM_DESIGN.md](REPLAY_PLATFORM_DESIGN.md):
> the recording path becomes the designed store — envelope v2 through Vector into a
> canonical, manifested S3 layout, consumed natively by the orchestrator — and
> **Kafka becomes the sole record sink** (JSONL deleted). Session mode only;
> window mode + the §1.1 identity contract (emit-time gseq, sampling gate,
> compound keys) are Phase 3 — they are the deepest deja-record surgery and the
> store layout is designed to receive them without re-partitioning.

## Definition of done

- The demo records through **Kafka → Vector → S3 only** (no JSONL primary
  anywhere); the recording's durable form is the compacted session in S3
  (`sessions/v1/{id}/` with manifest + zstd data parts).
- The orchestrator **pulls natively from S3** (no `mc` shell-outs): list,
  download, unwrap envelopes, dedup by `(recording_run_id, global_sequence)`,
  canonical sort, ingest report.
- The **manifest** records per-producer sequence coverage and seals the
  session; the catalog row (and the UI recordings page) is fed from it.
- The hardened sink honors `DEJA_SINK_POLICY = block | fail_open` with the
  drop decision at the **writer-enqueue layer**, a real `flush()`, no
  permanent disable on transient broker errors, and `deja_sink_marker`
  records (checkpoint/eof/dropped) for loss accounting.
- deja-tui, the visualizer, and `deja-semantic-metrics` keep working
  (compat shim: the pulled copy is also materialized at the legacy
  `recording/semantic-events.jsonl` path; tui discovery additionally learns
  the `recordings/{id}/events.jsonl` layout).
- **Gate at every step: the demo scorecard stays PASS 9/9 · rank₂ = 197.**

## Work breakdown (each independently demo-gated)

### P2.1 — S3-native ingest in the orchestrator (M)
`object_store` client (S3-compatible; MinIO at `DEJA_S3_ENDPOINT`, default
`http://127.0.0.1:9100`). Replaces `pull_recording`'s `mc find | sort | mc cat`
and `wait_minio_objects`' mc counting. Dedup by
`(recording_run_id, global_sequence)` (verified sufficient: `KeyStamper`
occurrences are correlation/address/args-scoped), sort by the same key,
write `events.jsonl` + an ingest report (objects, lines, duplicates dropped)
registered as a run artifact and folded into the catalog row.

> **Executed consolidated with P2.2** (user decision: nothing is shipped, so
> no compat layers). Ingest reads envelope v2 ONLY — the dual-shape parser
> and the legacy flat `recordings/{id}/` S3 layout were never built. The
> same decision deleted the legacy HTTP routes outright: the API is
> `/api/v1`-only (run create requires `X-Deja-Actor`), the Accept-negotiation
> hack and legacy importer are gone, scripts + SPA speak v1 natively, and
> the recording catalog row is upserted by the lifecycle (`system:lifecycle`)
> instead of a register endpoint.

### P2.2 — Envelope v2 + Vector v2 (vendor + config) (M)
Sink emits envelope v2: `schema_version: 2`, `instance_id`
(`{service}-{host}-{boot_ms}`), `capture: {mode: "session", session_id}`,
`code: {sha, deja_version}` (sha from `DEJA_CODE_REF` env, resolved by the
demo script from the vendor git head), `event_time_ns` at top level.
Vector: **no unwrap** (full envelope lands), session routing key
`landing/v1/session={id}/inst={instance_id}/`, zstd compression,
`acknowledgements` on the aws_s3 sink, pinned image tag. Ingest (P2.1)
unwraps. Vendor changes are deja-overlay/sink files, committed on deja-lean.

### P2.3 — `deja-compactor` (session mode) + manifests + catalog (L)
New workspace crate (lib + thin bin), invoked in-process by the record
lifecycle after quiesce: list `landing/v1/session={id}/`, stream-decode zstd,
parse envelopes (v1-tolerant), dedup, sort, write
`sessions/v1/{id}/data/part-NNNNN.ndjsonl.zst` (rotate), per-correlation
`index/correlations.ndjson.zst` (ingress summary, driveable flag), then
`manifest.json` LAST (its existence = the seal): per-instance gseq coverage
ranges + gaps + duplicates dropped, event/envelope schema versions, counts,
status. The catalog row upserts from the manifest (audited, machine actor
`system:lifecycle`). Replay prep consumes the manifest + data parts
(`pull_recording` v3); the UI recordings page shows coverage/seal badges.

### P2.4 — Hardened Kafka-only sink (deja-record + vendor) (L)
- deja-record `AsyncRecordWriter`: `DEJA_SINK_POLICY` — `block` (default,
  demo) keeps today's no-drop backpressure; `fail_open` makes the
  writer-enqueue drop decision (try_send → count + remember gseq range),
  never stalling request threads. Real `flush()` plumbed to the sink.
  Transient sink errors no longer disable the writer permanently (only
  fatal classification does). `deja_sink_marker` emission: periodic
  checkpoint (`last_gseq`), `eof` on shutdown flush, `dropped` ranges.
- Vendor sink: deja-owned producer (NOT the shared analytics constructor):
  `acks=all`, `enable.idempotence`, bounded buffering, real flush;
  `deja_boot` composes the Kafka sink as THE sink — `JsonlSink` leaves the
  record path; `DEJA_SINK` collapses (values jsonl/both removed).
- Demo overlay: `DEJA_SINK=kafka`; record lifecycle gains an explicit
  router stop/flush before sealing so `eof` actually fires.
- Consumer migration: lifecycle materializes the pulled copy ALSO at the
  legacy `recording/semantic-events.jsonl` path (deja-tui /
  deja-semantic-metrics keep working); deja-tui discovery learns
  `recordings/{id}/events.jsonl` as a first-class location.

### P2.5 — Validation
Full demo + matrix runs: scorecards identical to baseline; recording exists
ONLY via the Kafka path (assert no JSONL primary was written); manifest
sealed with zero gaps + zero duplicates on the 207-event fixture; UI shows
coverage badges; `just verify` + msrv green.

> **Demo-gate results** (each phase gated on PASS 9/9 · rank₂ = 197):
> - p2-gate-1 (P2.1+P2.2): native ingest over zstd envelope landing —
>   207/207 events, 0 dupes, PASS 9/9 · rank₂ 197.
> - p23-gate (P2.3): compactor sealed `sessions/v1/` — manifest gseq 0–206,
>   0 gaps, code.sha = vendor head, 10 correlation rows, PASS 9/9 · 197.
> - p24-gate2 (P2.4): Kafka-only record (no JSONL primary — the only JSONL
>   on disk is the byte-identical S3-pulled shim copy), graceful router
>   stop fired the shutdown flush, 4 sink markers landed (lines_in 211 vs
>   events 207) and were skipped at compaction, PASS 9/9 · 197.
> - workspace fmt/clippy/test green; runtime crates check on 1.85.
> - p25-matrix (P2.5): one Kafka-only recording, three replays —
>   self PASS 9/9 · benign PASS 9/9 · real DIVERGE 7/9 with 1 status +
>   1 body + 11 side-effect divergences (7 omitted + 4 novel) — every
>   number identical to the pre-Phase-2 baselines. Phase 2 COMPLETE.

## Sequencing notes

- P2.1 + P2.2 landed together (no compat shapes — see the note under P2.1):
  one demo gate covers the native ingest AND the envelope/Vector flip.
- P2.3 changes the durable layout; old landing prefixes need no migration
  (pre-v2 recordings already live on disk; S3 history is disposable) → gate.
- P2.4 last: deleting the JSONL sink only after the S3 path is the proven
  sole source of truth.
- Port hygiene: validation runs must not collide with a manually-running
  orchestrator on 8070 (kill it first; the script starts its own).
