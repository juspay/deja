# Dashboard-native replay diff + execution-graph divergence view

**Date:** 2026-07-15
**Status:** Approved
**Scope:** `deja-orchestrator` (`main.rs` endpoints), `web/src` (DiffView, GraphView,
api client). No agent changes: the agent's artifacts are the data source.

## Problem

1. **The diff and graph tabs are blank for sandbox runs.**
   `GET /api/v1/runs/{id}/http-diffs`, `/calls`, and `/graph` read only the
   orchestrator's local `harness-state` files. Sandbox runs (k8s or compose)
   produce those files inside the agent container and upload them to S3; the
   orchestrator never has local copies, so the endpoints return `[]` and the
   tabs render empty. The artifacts tab works because `v1_artifact_raw`
   streams from the registered artifact URI (`read_artifact_bytes`).
2. **The diff tab shows only HTTP rows** even when populated; the per-call
   side-effect story (the call ledger) is what explains failures, and today it
   only exists in the standalone `diff-report.html` artifact.
3. **GraphView cannot align record vs replay trees**: record-mode wraps every
   request in deja's synthetic `deja::http_incoming` span, so record trees
   root there while replay trees root at `HTTP request`; the span-name merge
   finds no common root. (Display-level twin of the rank-2 addressing bug
   fixed in deja-runtime commit `67ff0dd`.)
4. **Bonus bug:** `v1_artifact_raw`'s content-type logic predates
   `diff_report`; `.html` artifacts other than `visualization_html` are served
   as `application/x-ndjson`, so the dashboard's "open" downloads instead of
   rendering.

## Decisions (user-confirmed)

- Diff renders **natively in React** (no iframe of the HTML report).
- Data layer: **hydrate-on-read with local cache** (retroactive for existing
  runs; first read costs one S3 round-trip, then local).

## Design

### 1. Backend: artifact-fallback reads (`crates/deja-orchestrator/src/main.rs`)

New helper:

```rust
/// Read a run stream: local file first; if missing/empty, fall back to the
/// run's registered artifact of `kind`, cache its bytes to `local_path`, and
/// parse. Returns parsed JSON values (one per line, or a JSON array).
async fn read_run_stream(
    st: &AppState,
    run_id: &str,
    local_path: PathBuf,
    kind: &str,
) -> Vec<serde_json::Value>
```

Mechanics:
- Local parse identical to today's inline logic (skip blank lines, tolerate
  unparseable lines).
- Fallback requires a connected store: `store.list_artifacts(run_id)` →
  first artifact with matching `kind` → `read_artifact_bytes(&uri)` in
  `spawn_blocking` → write bytes to `local_path` (create parent dirs;
  best-effort — a failed cache write still serves the response) → parse.
- Wired into:
  - `v1_http_diffs` → kind `http_diffs`, path `http_diff_path(run_id)`
  - `v1_calls` → kind `call_ledger`, path `call_ledger_path(run_id)`
  - `v1_graph` → record side: kind `graph` cached at
    `recording_graph_path(recording_id)`; replay side: kind `graph_replay`
    cached at `replay_graph_path(run_id)`. The existing
    sidecar-or-mixed-stream reading and `DejaRecord`/bare-node parsing is
    preserved on top of the fetched bytes.

Content-type fix in `v1_artifact_raw`: `kind == "visualization_html" ||
kind == "diff_report" || uri.ends_with(".html")` → `text/html; charset=utf-8`.

### 2. Diff tab: side-effect timeline (`web/src/components/DiffView.tsx`)

- New query alongside `httpDiffs`: `api.calls(runId)` (endpoint already
  exists and is already typed as `CallRecord` in `web/src/lib/api.ts`;
  extend that type only if fields the timeline needs are missing).
- Ledger rows grouped by `correlation_id`; under each HTTP request section:
  - Summary line in the request header: outcome counts
    (`12 matched · 1 environmental · 7 omitted`).
  - Timeline table in recorded order: `seq · boundary · trait::method ·
    outcome chip`. Chip palette: matched/recovered green (label shows
    `rank N`), value-diverged + novel + deterministic red, environmental
    amber, omitted grey. `origin: true` rows labeled `value diverged (origin)`.
  - Rows with a non-matched kind expand to a recorded-vs-replayed **args**
    diff reusing the existing `JsonDiff`/`LeafDiffRow` components; when the
    observed side carries an independent `result` (shadow provenance), show a
    result diff too.
- Requests stay collapsible; mismatched requests open by default (existing
  behavior preserved).
- Ledger rows whose correlation has no HTTP diff render in a trailing
  "calls outside driven requests" section (parity with the HTML report).

### 3. Graph tab: alignment + divergence overlay (`web/src/components/GraphView.tsx`)

- **Wrapper unwrap:** before `buildForest`, drop record-side nodes named
  `deja::http_incoming` and promote their children (child.parent_id becomes
  the wrapper's parent_id, usually none → root). Replay side untouched.
  Trees then align at `HTTP request`.
- **Divergence overlay:** fetch `api.calls(runId)`; build two maps
  `graph_node_id → worst outcome` (one from `recorded.graph_node_id`, one
  from `observed.graph_node_id`). Severity order: red (novel, value_diverged,
  deterministic) > amber (environmental, recovered, order/idempotent
  warnings) > grey (omitted) > green (matched). Each merged tree node gets a
  colored dot for the worst outcome in its own calls, and a hollow marker if
  any descendant carries one (so collapsed subtrees still signal).
- **Structural badges:** merged nodes where only `rec` is present get a
  `rec-only` badge, only `rep` present a `rep-only` badge (today they merge
  silently with blank timings).

### 4. Out of scope

- No agent or chart changes; no new artifact kinds.
- No pagination/virtualization for very large ledgers (current runs are
  hundreds of rows; revisit if runs grow 100×).
- The standalone `diff-report.html` artifact stays as-is (shareable file);
  the dashboard does not embed it.

## Error handling

| Condition | Behavior |
|---|---|
| No local file, no store connected | endpoint returns `[]` (today's behavior) |
| No registered artifact of the kind | `[]`, no error |
| Artifact bytes unreadable (S3 error) | `[]` + `eprintln`, no 500 |
| Cache write fails | response still served from fetched bytes |
| Ledger has correlations absent from http-diffs | trailing section, not dropped |
| Graph nodes with no ledger calls | rendered plain (no dot) |

## Testing

- **Backend (axum router tests, existing pattern in `main.rs`):**
  - http-diffs endpoint: local file absent + registered artifact (file:// URI
    fixture) → hydrated rows returned AND local cache file written; second
    call served without touching the artifact.
  - calls + graph endpoints: same fallback shape.
  - `v1_artifact_raw` serves `diff_report` as `text/html`.
- **Frontend:** pure helpers (`groupCalls`, `worstOutcome`, `unwrapIngress`)
  factored to `web/src/lib/` and exercised by `tsc` types + `npm run build`;
  no test runner exists in `web/` today and none is introduced.
- **Live verification:** existing sandbox run `run-18c26f1b1b9a9d9f` — after
  dashboard rebuild, diff and graph tabs must populate from S3 without a
  rerun, and the graph must show aligned trees with divergence dots.
