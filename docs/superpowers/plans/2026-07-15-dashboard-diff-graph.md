# Dashboard Diff + Graph Divergence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Un-blank the dashboard's diff/graph tabs for sandbox runs by hydrating run streams from registered S3 artifacts, fix HTML artifact rendering, align record-vs-replay graph trees, and add a per-request side-effect timeline to the diff tab.

**Architecture:** The orchestrator endpoints (`/http-diffs`, `/calls`, `/graph`) gain a hydrate-on-read fallback: local `harness-state` file first; if missing/empty, fetch the run's registered artifact bytes (`read_artifact_bytes`, S3 or file URI), cache to the local path, serve. The fetch-and-cache core is a store-free function so it unit-tests hermetically with `file://`-style URIs. Frontend: GraphView drops deja's record-only `deja::http_incoming` wrapper node before building the forest (trees then align at `HTTP request` — the existing divergence overlay starts working); DiffView gains a per-request collapsible timeline of ledger calls with outcome chips.

**Tech Stack:** Rust (axum, tokio, serde), existing `deja-store` Postgres rows, React + TypeScript + @tanstack/react-query in `web/`.

**Spec:** `docs/superpowers/specs/2026-07-15-dashboard-diff-graph-design.md`

## Global Constraints

- Workspace lints: clippy `dbg_macro`/`todo`/`unwrap_used` = deny (tests may `#![allow(clippy::unwrap_used)]`); verify with `just verify`.
- The branch has other committed work; `git add` only the files each task touches.
- Endpoint failure behavior: fallback errors degrade to `[]` + `eprintln`, never a 500 (spec "Error handling" table).
- No new artifact kinds, no agent changes, no new web dependencies or test runner.
- Frontend verification is `cd web && npm run build` (tsc + vite); there is no web test runner and none is introduced.

---

### Task 1: Hydration core + http-diffs fallback

**Files:**
- Modify: `crates/deja-orchestrator/src/main.rs` (near `read_artifact_bytes`, ~line 930; `v1_http_diffs`, ~line 782; tests module)

**Interfaces:**
- Produces (used by Task 2):
  - `fn hydrate_stream(local_path: &std::path::Path, artifact_uri: Option<&str>) -> Vec<serde_json::Value>` — sync, store-free: parse local file; if it yields zero rows and `artifact_uri` is `Some`, fetch bytes via `read_artifact_bytes`, best-effort cache to `local_path`, parse the fetched bytes.
  - `async fn artifact_uri_for(st: &AppState, run_id: &str, kind: &str) -> Option<String>` — `st.store` → `list_artifacts(run_id)` → first row with `kind`, else `None` (also `None` when no store or on query error, with `eprintln`).
  - `fn parse_jsonl_or_array(content: &str) -> Vec<serde_json::Value>` — JSONL lines (skip blanks/bad lines) or a whole-file JSON array.

- [ ] **Step 1: Write the failing tests**

Add to the `mod tests` block in `crates/deja-orchestrator/src/main.rs`:

```rust
#[test]
fn hydrate_stream_prefers_local_file() {
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("http-diffs.jsonl");
    std::fs::write(&local, "{\"a\":1}\n\n{\"a\":2}\n").unwrap();
    // artifact_uri points at a file that would panic the test if read
    let rows = hydrate_stream(&local, Some("/nonexistent/never-read.jsonl"));
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["a"], 1);
}

#[test]
fn hydrate_stream_falls_back_to_artifact_and_caches() {
    let dir = tempfile::tempdir().unwrap();
    let local = dir.path().join("nested").join("http-diffs.jsonl"); // parent missing
    let artifact = dir.path().join("uploaded.jsonl");
    std::fs::write(&artifact, "{\"b\":1}\n").unwrap();

    let rows = hydrate_stream(&local, Some(artifact.to_str().unwrap()));
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["b"], 1);
    // cached: second read works with the artifact gone
    std::fs::remove_file(&artifact).unwrap();
    let again = hydrate_stream(&local, None);
    assert_eq!(again.len(), 1);
}

#[test]
fn hydrate_stream_missing_everything_is_empty_not_error() {
    let dir = tempfile::tempdir().unwrap();
    assert!(hydrate_stream(&dir.path().join("nope.jsonl"), None).is_empty());
    assert!(hydrate_stream(&dir.path().join("nope.jsonl"), Some("/also/nope")).is_empty());
}

#[test]
fn parse_jsonl_or_array_accepts_both_shapes() {
    assert_eq!(parse_jsonl_or_array("{\"x\":1}\n{\"x\":2}\n").len(), 2);
    assert_eq!(parse_jsonl_or_array("[{\"x\":1},{\"x\":2}]").len(), 2);
    assert!(parse_jsonl_or_array("").is_empty());
}
```

Add `tempfile` to `[dev-dependencies]` in `crates/deja-orchestrator/Cargo.toml` if not present (`tempfile = "3"`).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p deja-orchestrator --bin deja-orchestrator hydrate_stream`
Expected: compile error — `hydrate_stream` not found.

- [ ] **Step 3: Implement**

Insert next to `read_artifact_bytes` in `main.rs`:

```rust
/// Parse a run stream: JSONL lines (blank/bad lines skipped) or a whole-file
/// JSON array (dashboard exports).
fn parse_jsonl_or_array(content: &str) -> Vec<serde_json::Value> {
    let trimmed = content.trim_start();
    if trimmed.starts_with('[') {
        if let Ok(serde_json::Value::Array(rows)) = serde_json::from_str(trimmed) {
            return rows;
        }
    }
    content
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str(l).ok())
        .collect()
}

/// Local file first; when it yields nothing and an artifact URI is known,
/// fetch the artifact bytes, cache them to `local_path` (best-effort), and
/// serve the fetched content. Sandbox runs write these files inside the agent
/// container and upload them as artifacts — the dashboard host never has the
/// local copy until this hydrates it.
fn hydrate_stream(
    local_path: &std::path::Path,
    artifact_uri: Option<&str>,
) -> Vec<serde_json::Value> {
    if let Ok(content) = std::fs::read_to_string(local_path) {
        let rows = parse_jsonl_or_array(&content);
        if !rows.is_empty() {
            return rows;
        }
    }
    let Some(uri) = artifact_uri else {
        return Vec::new();
    };
    let bytes = match read_artifact_bytes(uri) {
        Ok(b) => b,
        Err(e) => {
            eprintln!("deja-orchestrator: artifact hydrate failed for {uri}: {e}");
            return Vec::new();
        }
    };
    if let Some(parent) = local_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if let Err(e) = std::fs::write(local_path, &bytes) {
        eprintln!(
            "deja-orchestrator: artifact cache write failed for {}: {e}",
            local_path.display()
        );
    }
    parse_jsonl_or_array(&String::from_utf8_lossy(&bytes))
}

/// URI of the run's first registered artifact of `kind`, when a store is
/// connected. `None` (with a log line on query errors) otherwise.
async fn artifact_uri_for(st: &AppState, run_id: &str, kind: &str) -> Option<String> {
    let store = st.store.as_ref()?;
    match store.list_artifacts(run_id).await {
        Ok(rows) => rows.into_iter().find(|a| a.kind == kind).map(|a| a.uri),
        Err(e) => {
            eprintln!("deja-orchestrator: list artifacts for {run_id}: {e}");
            None
        }
    }
}
```

Rewrite `v1_http_diffs`:

```rust
/// `GET /api/v1/runs/{id}/http-diffs` — the kernel's per-request HTTP diffs
/// (status + field-level body diff). Local stream first; sandbox runs are
/// hydrated from the registered `http_diffs` artifact and cached locally.
async fn v1_http_diffs(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    let uri = artifact_uri_for(&st, &id, "http_diffs").await;
    let local = st.root.http_diff_path(&id);
    let rows = tokio::task::spawn_blocking(move || hydrate_stream(&local, uri.as_deref()))
        .await
        .unwrap_or_default();
    json_ok(serde_json::Value::Array(rows))
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p deja-orchestrator --bin deja-orchestrator 2>&1 | grep -E 'test result|FAILED'`
Expected: all PASS (hydrate tests + existing).

- [ ] **Step 5: Commit**

```bash
git add crates/deja-orchestrator/src/main.rs crates/deja-orchestrator/Cargo.toml
git commit -m "feat(deja-orchestrator): hydrate http-diffs from registered artifacts"
```

---

### Task 2: calls + graph fallback

**Files:**
- Modify: `crates/deja-orchestrator/src/main.rs` (`v1_calls` ~line 773, `v1_graph` ~line 799)

**Interfaces:**
- Consumes: `hydrate_stream`, `artifact_uri_for`, `parse_jsonl_or_array` (Task 1).
- Produces: same endpoint responses, now populated for sandbox runs.

- [ ] **Step 1: Rewire `v1_calls`**

The live path (`divergence::call_ledger`) recomputes the ledger from local
lookup-table/observed files, which sandbox runs don't have. Serve the live
computation when it yields rows; otherwise hydrate the agent's uploaded
`call-ledger.jsonl`:

```rust
/// `GET /api/v1/runs/{id}/calls` — the per-call divergence ledger. Computed
/// live from local artifacts when present; sandbox runs are served from the
/// agent's uploaded call-ledger artifact (cached locally).
async fn v1_calls(State(st): State<AppState>, Path(id): Path<String>) -> Response {
    if let Ok(rows) = divergence::call_ledger(&st.root, &id) {
        if !rows.is_empty() {
            return json_ok(serde_json::to_value(&rows).unwrap_or_default());
        }
    }
    let uri = artifact_uri_for(&st, &id, "call_ledger").await;
    let local = st.root.call_ledger_path(&id);
    let rows = tokio::task::spawn_blocking(move || hydrate_stream(&local, uri.as_deref()))
        .await
        .unwrap_or_default();
    json_ok(serde_json::Value::Array(rows))
}
```

- [ ] **Step 2: Rewire `v1_graph`**

Keep the existing `read_nodes` / sidecar-or-stream logic, but hydrate each
side's sidecar from its artifact first. Replace the body of `v1_graph` so the
record/replay assembly becomes:

```rust
    // Hydrate sidecars from registered artifacts before reading (no-ops when
    // the local files already have content).
    let record_uri = artifact_uri_for(&st, &id, "graph").await;
    let replay_uri = artifact_uri_for(&st, &id, "graph_replay").await;
    let (record, replay) = {
        let root = st.root.clone();
        let rec = rec.clone();
        tokio::task::spawn_blocking(move || {
            if let Some(r) = rec.as_deref() {
                let _ = hydrate_stream(&root.recording_graph_path(r), record_uri.as_deref());
            }
            let _ = hydrate_stream(&root.replay_graph_path(&id), replay_uri.as_deref());
            let record = rec
                .as_deref()
                .map(|r| {
                    read_sidecar_or_stream(
                        root.recording_graph_path(r),
                        root.recording_events_path(r),
                    )
                })
                .unwrap_or_default();
            let replay =
                read_sidecar_or_stream(root.replay_graph_path(&id), root.observed_path(&id));
            (record, replay)
        })
        .await
        .unwrap_or_default()
    };
    json_ok(serde_json::json!({ "record": record, "replay": replay }))
```

(`read_nodes`/`read_sidecar_or_stream` become plain `fn` items above the
handler so the closure can call them; `hydrate_stream`'s cache write is what
makes the subsequent `read_sidecar_or_stream` see content. The `DejaRecord`
vs bare-node parsing inside `read_nodes` is unchanged.)

- [ ] **Step 3: Build + full orchestrator tests**

Run: `cargo build -p deja-orchestrator --all-targets && cargo test -p deja-orchestrator 2>&1 | grep -E 'test result: FAILED|failures:'`
Expected: builds, no failures.

- [ ] **Step 4: Commit**

```bash
git add crates/deja-orchestrator/src/main.rs
git commit -m "feat(deja-orchestrator): hydrate calls + graph endpoints from artifacts"
```

---

### Task 3: HTML artifact content-type

**Files:**
- Modify: `crates/deja-orchestrator/src/main.rs` (`v1_artifact_raw`, ~line 910)

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn artifact_content_type_covers_html_kinds() {
    assert_eq!(artifact_content_type("visualization_html", "x.html"), "text/html; charset=utf-8");
    assert_eq!(artifact_content_type("diff_report", "s3://b/runs/r/diff-report.html"), "text/html; charset=utf-8");
    assert_eq!(artifact_content_type("scorecard", "s3://b/runs/r/scorecard.json"), "application/json");
    assert_eq!(artifact_content_type("http_diffs", "s3://b/runs/r/http-diffs.jsonl"), "application/x-ndjson");
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `cargo test -p deja-orchestrator --bin deja-orchestrator artifact_content_type`
Expected: compile error — fn not found.

- [ ] **Step 3: Implement**

Extract the inline content-type logic in `v1_artifact_raw` into:

```rust
fn artifact_content_type(kind: &str, uri: &str) -> &'static str {
    if kind == "visualization_html" || kind == "diff_report" || uri.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if uri.ends_with(".json") {
        "application/json"
    } else {
        "application/x-ndjson"
    }
}
```

and call it: `let content_type = artifact_content_type(&art.kind, &art.uri);`

- [ ] **Step 4: Run tests, commit**

Run: `cargo test -p deja-orchestrator --bin deja-orchestrator artifact_content_type`
Expected: PASS.

```bash
git add crates/deja-orchestrator/src/main.rs
git commit -m "fix(deja-orchestrator): serve HTML artifacts (diff_report) as text/html"
```

---

### Task 4: GraphView ingress-wrapper unwrap

**Files:**
- Create: `web/src/lib/graphalign.ts`
- Modify: `web/src/components/GraphView.tsx` (top of `model` memo, ~line 71)

**Interfaces:**
- Produces: `export function unwrapIngress(nodes: GraphNode[]): GraphNode[]` — removes nodes whose `span_name === "deja::http_incoming"`, re-parenting their children to the wrapper's `parent_id`.

**Why:** record-mode wraps each request in deja's synthetic
`deja::http_incoming` span; replay mode does not. `mergeLevel` merges by
span-name per level, so the mismatched roots make every record span render
"skipped" and every replay span "added on replay". Same asymmetry the
rank-2 address trim fixed in `deja-runtime` (`67ff0dd`), applied at display
level.

- [ ] **Step 1: Create the helper**

`web/src/lib/graphalign.ts`:

```ts
import { GraphNode } from "./api";

// Deja's record-mode ingress wrapper span. Replay mode never enters it, so it
// must not participate in the record-vs-replay span-name merge.
const INGRESS_WRAPPER = "deja::http_incoming";

// Remove wrapper nodes, re-parenting their children to the wrapper's parent
// (usually none -> the child becomes a root, aligning with the replay tree's
// "HTTP request" roots).
export function unwrapIngress(nodes: GraphNode[]): GraphNode[] {
  const wrappers = new Map<number, number | null>();
  for (const n of nodes) {
    if (n.span_name === INGRESS_WRAPPER) wrappers.set(n.node_id, n.parent_id);
  }
  if (wrappers.size === 0) return nodes;
  return nodes
    .filter((n) => !wrappers.has(n.node_id))
    .map((n) =>
      n.parent_id != null && wrappers.has(n.parent_id)
        ? { ...n, parent_id: wrappers.get(n.parent_id)! }
        : n,
    );
}
```

- [ ] **Step 2: Use it in GraphView**

In `web/src/components/GraphView.tsx`, import and apply to the record side
only:

```ts
import { unwrapIngress } from "../lib/graphalign";
// in the model memo:
const merged = mergeLevel(
  buildForest(unwrapIngress(graph.data.record)),
  buildForest(graph.data.replay),
  "",
);
```

- [ ] **Step 3: Verify with the type-checker + build**

Run: `cd web && npm run build`
Expected: `tsc -b && vite build` succeeds.

- [ ] **Step 4: Commit**

```bash
git add web/src/lib/graphalign.ts web/src/components/GraphView.tsx
git commit -m "fix(web): align record/replay graph trees by unwrapping the deja ingress span"
```

---

### Task 5: DiffView per-request side-effect timeline

**Files:**
- Modify: `web/src/components/DiffView.tsx`
- Modify: `web/src/styles.css` (chip/timeline styles)

**Interfaces:**
- Consumes: `api.calls`, `api.httpDiffs` (already queried in DiffView), `CallRecord`/`HttpDiff` types from `web/src/lib/api.ts`.

**Behavior (spec §2):** a new "Requests" section listing EVERY http-diff row
as a `<details>` (mismatched open by default), each with an outcome-count
summary and a timeline table of that correlation's ledger calls; non-matched
rows expand to a recorded-vs-replayed args diff via the existing
`diffArgs`/`LeafDiffRow`. A trailing "calls outside driven requests" section
holds ledger correlations with no http diff. Existing
scorestrip/root-cause/cascade sections stay.

- [ ] **Step 1: Add pure helpers + components to DiffView.tsx**

```tsx
const OUTCOME_LABEL: Record<string, string> = {
  matched: "matched",
  recovered: "recovered",
  value_diverged: "value diverged",
  novel: "novel",
  omitted: "omitted",
  environmental: "environmental",
  deterministic: "deterministic miss",
};
function outcomeClass(kind: string): string {
  if (kind === "matched") return "ok";
  if (kind === "omitted") return "muted";
  if (kind === "environmental" || kind === "recovered") return "warn";
  return "bad"; // value_diverged, novel, deterministic, anything unknown
}
function outcomeLabel(c: CallRecord): string {
  const base = OUTCOME_LABEL[c.kind] ?? c.kind.replace(/_/g, " ");
  if (c.kind === "matched" && c.resolved_rank != null) return `${base} (rank ${c.resolved_rank})`;
  if (c.kind === "value_diverged" && c.origin) return `${base} (origin)`;
  return base;
}
function groupCallsByCorrelation(calls: CallRecord[]): Map<string, CallRecord[]> {
  const by = new Map<string, CallRecord[]>();
  for (const c of calls) {
    if (!c.correlation_id) continue;
    (by.get(c.correlation_id) ?? by.set(c.correlation_id, []).get(c.correlation_id)!).push(c);
  }
  return by;
}
function outcomeCounts(calls: CallRecord[]): string {
  const n = new Map<string, number>();
  for (const c of calls) n.set(c.kind, (n.get(c.kind) ?? 0) + 1);
  return [...n.entries()].map(([k, v]) => `${v} ${(OUTCOME_LABEL[k] ?? k).replace(/_/g, " ")}`).join(" · ");
}

function TimelineRow({ c }: { c: CallRecord }) {
  const expandable = c.kind !== "matched" && (c.recorded?.args != null || c.observed?.args != null);
  const diffs = expandable ? diffArgs(c.recorded?.args, c.observed?.args) : [];
  const row = (
    <div className={`tlrow ${outcomeClass(c.kind)}`}>
      <span className="tlseq">{c.source_event_global_sequence ?? "—"}</span>
      <span className="tlboundary">{c.boundary}</span>
      <span className="tlcall"><code>{c.trait_name}::{c.method_name}</code></span>
      <span className={`chip ${outcomeClass(c.kind)}`}>{outcomeLabel(c)}</span>
    </div>
  );
  if (!expandable) return row;
  return (
    <details className="tldetails">
      <summary>{row}</summary>
      <div className="argdiff">
        {diffs.length === 0 && <p className="hint">recorded vs replayed args structurally differ</p>}
        {diffs.map((d, i) => <LeafDiffRow key={i} d={d} />)}
      </div>
    </details>
  );
}

function RequestSection({ d, calls }: { d: HttpDiff; calls: CallRecord[] }) {
  const ok = d.status_match && d.body_diff.length === 0;
  return (
    <details className={`reqsection ${ok ? "ok" : "bad"}`} open={!ok}>
      <summary>
        <span className="method">{d.request_path}</span>
        <span className="statuspill">
          <span className={ok ? "ok" : "bad"}>{d.status_baseline} → {d.status_candidate}</span>
        </span>
        <span className="meta">{d.correlation_id}{calls.length > 0 ? ` · ${outcomeCounts(calls)}` : ""}</span>
      </summary>
      {!ok && <HttpBlock d={d} />}
      {calls.length > 0 && (
        <div className="timeline">
          <h3>side-effect timeline</h3>
          {calls.map((c, i) => <TimelineRow key={i} c={c} />)}
        </div>
      )}
    </details>
  );
}
```

- [ ] **Step 2: Render the sections**

In `DiffView`'s return, after the `Resulting response divergence` section,
add (using `https.data` for ALL requests, not just `httpBad`):

```tsx
{(https.data ?? []).length > 0 && (
  <section>
    <h2>Requests</h2>
    {(https.data ?? []).map((d, i) => (
      <RequestSection key={i} d={d} calls={byCorr.get(d.correlation_id) ?? []} />
    ))}
    {orphanCalls.length > 0 && (
      <details className="reqsection">
        <summary>calls outside driven requests · {outcomeCounts(orphanCalls)}</summary>
        <div className="timeline">{orphanCalls.map((c, i) => <TimelineRow key={i} c={c} />)}</div>
      </details>
    )}
  </section>
)}
```

with, above the return:

```tsx
const byCorr = groupCallsByCorrelation(all);
const drivenCorrs = new Set((https.data ?? []).map((d) => d.correlation_id));
const orphanCalls = all.filter((c) => c.correlation_id && !drivenCorrs.has(c.correlation_id));
```

- [ ] **Step 3: Styles**

Append to `web/src/styles.css`:

```css
.reqsection { border: 1px solid var(--border); border-radius: 6px; margin: .5rem 0; padding: .3rem .7rem; }
.reqsection > summary { cursor: pointer; display: flex; gap: .6rem; align-items: baseline; }
.tlrow { display: flex; gap: .7rem; align-items: baseline; padding: 2px 0; font-size: 13px; }
.tlrow.muted { color: var(--text-muted); }
.tlseq { min-width: 3.5rem; text-align: right; color: var(--text-muted); }
.tlboundary { min-width: 6rem; }
.tldetails > summary { list-style: none; cursor: pointer; }
.chip.ok { background: var(--ok-bg, #d9efdd); }
.chip.warn { background: var(--warn-bg, #fdf7e8); }
.chip.bad { background: var(--bad-bg, #f6d5d2); }
.chip.muted { opacity: .65; }
```

(Reuse existing chip variables/classes where `styles.css` already defines
them — check before adding duplicates.)

- [ ] **Step 4: Build + commit**

Run: `cd web && npm run build`
Expected: success.

```bash
git add web/src/components/DiffView.tsx web/src/styles.css
git commit -m "feat(web): per-request side-effect timeline in the diff tab"
```

---

### Task 6: Full verification

**Files:** none.

- [ ] **Step 1:** `just verify` — fmt/clippy/tests all green.
- [ ] **Step 2:** `cd web && npm run build` — dashboard builds; `git add web/dist && git commit -m "chore(web): rebuild dist"` if dist is tracked.
- [ ] **Step 3 (live):** rebuild the dashboard stack
  (`docker compose -f demo/docker-compose.dashboard.yml --env-file demo/.env up -d --build`),
  open run `run-18c26f1b1b9a9d9f`: diff tab shows requests + timelines
  hydrated from S3, graph tab shows aligned trees with divergence markers,
  artifacts tab "open" on `diff_report` renders HTML in the browser.

---

## Self-Review (done at plan-writing time)

- **Spec coverage:** §1 backend fallback → Tasks 1–2; content-type → Task 3; §2 diff timeline → Task 5; §3 wrapper unwrap → Task 4; §3's divergence overlay + rec/rep-only badges already exist in GraphView (verified: `novelIds`/`omittedIds`/`valueDivByNode` maps and absent-cell "added on replay"/"skipped" chips) — the unwrap is what makes them fire, so no separate task; error-handling table → Task 1 code paths; live verification → Task 6.
- **Type consistency:** `hydrate_stream(&Path, Option<&str>) -> Vec<Value>` and `artifact_uri_for(&AppState, &str, &str) -> Option<String>` used identically in Tasks 1–2; frontend helpers consume existing `CallRecord`/`HttpDiff` fields only (`logical_span_path` naming quirk in api.ts is untouched — the timeline doesn't need span paths).
- **Placeholder scan:** none; all steps carry code or exact commands.
