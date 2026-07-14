# Typed ingestion + replay-side execution-graph recording

**Date:** 2026-07-14
**Status:** Approved (Approach A)
**Scope:** `deja-replay-core` (ingest rewrite), `deja-core` / `deja-runtime` (lenient
serde + extras maps), `replay-sandbox/chart` (graph recording flag). No new UI:
the existing `web/src/components/GraphView.tsx` tree is the consumer to verify.

## Problem

1. **Ingestion is stringly-typed.** `crates/deja-replay-core/src/ingest.rs` routes
   recording lines through five ad-hoc probe structs, carries events as
   `(Option<String>, u64, String)` tuples, injects `record_kind` by literal string
   splicing (`stamp_record_kind`), and normalizes Vector-stringified numbers by
   mutating untyped `serde_json::Value`. Bad lines are silently dropped with an
   `eprintln`. Meanwhile a typed `deja::DejaRecord` enum (internally tagged on
   `record_kind`: `BoundaryEvent | GraphNode | Observed`) already exists — the
   ingest tests parse into it, but the pipeline never does.
2. **Replay runs capture no execution graph.** The sandbox router configmap
   hardcodes `ROUTER__DEJA__RECORDING__GRAPH: "disabled"`
   (`replay-sandbox/chart/templates/stack/router-configmap.yaml`), so
   `set_graph_recording_enabled` is never turned on in-sandbox, `graph-replay.jsonl`
   stays empty, and the dashboard's `GraphView` tree has nothing to render on the
   replay side.

Note: the module doc's "raw event bytes preserved verbatim" claim is already
stale — `normalize_event_numbers` re-serializes every parseable line through
`serde_json::Value` (BTreeMap ⇒ alphabetical key order). Typed serialization is
no regression on byte fidelity.

## Decisions (user-confirmed)

- **Strictness:** typed + fail loud. Every line parses into typed records with
  extras maps so unknown fields survive round-trip. Unparseable lines are counted
  and reported; a tape yielding zero valid events is an error.
- **Graph flag:** chart value, default **enabled**.
- **Visualization:** verify the existing `GraphView` tree; no new UI.
- **Approach:** A — leniency lives on the canonical types, not ingest-local shims.

## Design

### 1. Lenient serde + extras on the canonical types

New module `deja_core::serde_lenient` with reusable deserializers accepting both
JSON numbers and stringified numbers (Vector stringifies values above `i64::MAX`
in transit):

- `u64`, `u32`, `u16`, `Option<u64>`, `Vec<u64>` variants.

Applied via `#[serde(deserialize_with)]` to exactly the fields
`normalize_event_numbers` coerces today:

- `BoundaryEvent` (deja-runtime): `global_sequence`, `request_sequence`,
  `timestamp_ns`, `graph_node_id`, `tracing_span_id`, `fork_seq`, `call_line`,
  `call_column`, `duration_us`, `event_schema_version`, `value_digest`,
  `end_timestamp_ns`.
- `CallsiteIdentity` (deja-runtime): `version`, `occurrence`, `syntax_hash`.
- `ExecutionGraphNode` (deja-core): `node_id`, `global_sequence`, `parent_id`,
  `causal_parent_ids`, `sequence`, `started_ns`, `closed_ns`.
- `ObservedCall` (deja-runtime): its numeric metadata fields (e.g.
  `source_event_global_sequence`, `policy_version`) — same list the current
  normalizer touches.

Extras maps so unknown fields survive typed round-trips (cross-version tapes):

```rust
#[serde(flatten, default, skip_serializing_if = "serde_json::Map::is_empty")]
pub extras: serde_json::Map<String, serde_json::Value>,
```

on **`BoundaryEvent`**, **`ExecutionGraphNode`**, and **`CallsiteIdentity`**
(identity feeds the address ladder — the worst place to silently lose fields
from a newer recorder; `logical_context` itself was added this way, see
`docs/LOGICAL_CONTEXT_ADDRESSING.md`). `skip_serializing_if` keeps serialization
of current events byte-identical when extras is empty. Sample-record validation
(three real sandbox records: boundary + declaration, time boundary with populated
callsite identity incl. `logical_context` → `span_path` rename, graph node)
confirmed all fields land in declared struct members; extras is purely a
forward-compat net.

### 2. Rewritten ingest pipeline (`deja-replay-core/src/ingest.rs`)

**Deleted:** `EventProbe`, `EnvelopeProbe`, `LineKindProbe`, `RecordKindProbe`,
`EventBreakdownProbe`, `stamp_record_kind`, `ensure_record_kind`,
`has_record_kind`, `normalize_event_numbers`, `normalized_kind`,
`de_u64_lenient` (moves to `deja_core::serde_lenient`), `coerce_u64_*`.

**Added:**

- `LandingEnvelope`: `artifact_type: Option<ArtifactType>`, `event` / `node`
  raw payloads, top-level `record_kind` / `marker_kind` (raw marker lines).
  Unknown envelope metadata (`instance_id`, `capture`, `code`, envelope-level
  `schema_version` — versions 1 and 2 both observed — etc.) is ignored, as today.
- `ArtifactType` enum (`BoundaryEvent | GraphNode | SinkMarker`) with a custom
  `Deserialize` that normalizes spelling (strip non-alphanumerics, lowercase)
  before matching — preserving today's tolerance for `"DejaGraph"`,
  `"deja_graph_node"`, `"GRAPH-NODE"`, etc. Unknown types are dropped **and
  counted**.
- Routing rules (semantics identical to today):
  1. Sink markers (typed envelope fields) → skipped, not counted as drops.
  2. Payload carries `record_kind` → parse as `deja::DejaRecord` (tag wins).
  3. Otherwise artifact type decides: parse `Box<BoundaryEvent>` or
     `ExecutionGraphNode`, wrap in `DejaRecord`. Raw non-envelope lines default
     to the boundary route.
  4. Serialization stamps `record_kind` automatically via the tagged enum — no
     string splicing.
- `legacy::rescue_missing_sequences` — the one quarantined `Value` shim: old
  direct-S3 tapes can omit `global_sequence` / `request_sequence` entirely.
  Since both are required fields and `0` is a legitimate value (observed
  `request_sequence: 0` on real tapes), "missing" cannot be expressed by a
  field default. When the typed parse fails **for that reason only**, patch the
  two fields (gseq ← 1-based position fallback, rseq ← gseq) and re-parse typed.
- Typed collate: events flow as `DejaRecord`. Dedup on the canonical serialized
  form (graph and boundary records share the gseq counter space, so
  `(recording_run_id, global_sequence)` alone must not collapse them — unchanged
  rule). Sort by typed `(recording_run_id, global_sequence)`.
  `correlation_count` (BoundaryEvent variant only) and `ingest_breakdown` use
  typed accessors. Serialize once, at `write_events`.

### 3. Fail-loud error surface

- `IngestError` (`thiserror`): `S3(String)` (compactor boundary), `Decode`,
  `Io`, `NoEvents { lines_in, lines_dropped }`.
- Public fns return `Result<_, IngestError>` instead of `Result<_, String>`.
  In-repo callers (agent `pull_recording`, orchestrator `s3` re-export) format
  via `Display`, so churn is signature-level.
- `IngestReport.lines_dropped: usize` — unparseable lines and unknown artifact
  types (still logged per line, now persisted in the report artifact).
- Zero valid events ⇒ `IngestError::NoEvents`; no empty `events.jsonl` is
  written (defense in depth ahead of the agent's empty-lookup-table refusal).

### 4. Graph recording flag (replay sandbox)

- `replay-sandbox/chart/values.yaml`: `router.dejaGraphRecording: "enabled"`.
- `templates/stack/router-configmap.yaml`:
  `ROUTER__DEJA__RECORDING__GRAPH: {{ .Values.router.dejaGraphRecording | quote }}`.
- Default **on**. No orchestrator change: the chart default applies to every
  run; `DEJA_SANDBOX_EXTRA_VALUES` remains the per-deployment off switch for
  high-volume runs.
- Existing plumbing downstream is untouched: router build calls
  `set_graph_recording_enabled` from this config; graph nodes ride the observed
  stream (no file sink by design); the agent extracts them to
  `graph-replay.jsonl`; `GraphView` merges record vs replay trees by span-path.
- Record-side graph capture for live recordings is configured in the
  environment's own deployment (outside this repo). Until enabled there, the
  tree renders replay-side nodes only — graceful degradation.

## Error handling summary

| Condition | Behavior |
|---|---|
| Stringified numeric metadata | Parses via lenient deserializers (all consumers, not just ingest) |
| Unknown fields on event/node/identity | Preserved in extras, round-trip intact |
| Unknown artifact type / unparseable line | Dropped + counted in `lines_dropped`, logged |
| Missing gseq/rseq (legacy tapes) | Quarantined rescue shim, then typed parse |
| Sink markers / junk whitespace | Skipped, not counted as drops |
| Zero valid events | `IngestError::NoEvents`, no output file |

## Testing

- All existing `collate_*` tests pass (adapted where they asserted string
  internals such as verbatim key order).
- New: extras round-trip (unknown fields on event, node, and callsite identity);
  lenient numbers incl. nested identity and `> i64::MAX` string values;
  missing-sequence rescue vs present-zero non-rescue; `NoEvents` on all-junk
  tape; `lines_dropped` accounting; artifact-type spelling tolerance;
  mixed envelope `schema_version` 1/2.
- `helm template` asserts the configmap renders
  `ROUTER__DEJA__RECORDING__GRAPH: "enabled"` by default and honors an override.
- `just verify` (fmt-check + clippy -D warnings + tests) across the workspace;
  agent tests already assert `graph.jsonl` / `graph-replay.jsonl`
  materialization; `web` builds against the unchanged `GraphView`.
