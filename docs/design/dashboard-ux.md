> Full design produced by the planning workflow (2026-06-12). The adversarial review
> appended at the bottom contains corrections that SUPERSEDE the body where they
> conflict; the reconciled master plan is ../REPLAY_PLATFORM_DESIGN.md.

# Dashboard + Divergence Explorer — Design

Web UI grown on `deja-orchestrator`. TypeScript SPA served by the orchestrator; REST/JSON + one SSE stream. The explorer is a faithful web port of deja-tui's joins (verified against `crates/deja-tui/src/lib.rs` and `crates/deja-orchestrator/src/divergence/mod.rs`), moved server-side so the browser never downloads whole recordings.

---

## 0. System position and process architecture

```
                 ┌──────────────────────────────────────────────┐
 Browser (SPA)── │ deja-orchestrator (axum + tokio, replaces   │ ── Postgres (runs, stage_events,
   /ui/* static  │ tiny_http; coordination item with the        │     recordings catalog, divergence
   /api/v1/* JSON│ pipeline/orchestrator designer)              │     index, audit, runners)
   /api SSE      │  • static SPA assets (rust-embed)            │
                 │  • REST/JSON API                             │ ── Artifact store (S3-pulled
                 │  • SSE run progress (Postgres LISTEN/NOTIFY) │     events.jsonl, observed,
                 │  • explorer index builder (per-run cache)    │     http-diffs, lookup-table,
                 └──────────────────────────────────────────────┘     execution-graph)
```

Hard dependency to flag immediately: today's server is **single-threaded synchronous tiny_http** (main.rs:54-61, one request at a time). A dashboard (parallel asset fetches + long-lived SSE + API calls) cannot run on it. The Postgres/orchestrator rework must land the server on a concurrent runtime (recommend axum + tokio, same process). The SPA design below assumes that. Existing routes (`POST /recordings`, `GET /runs/{id}/...`, ingest POSTs) are kept verbatim for demo-script compat; all new surface is under `/api/v1`.

Rust-side reuse (no re-implementation in TS):
- `SemanticEvent` parsing with `de_u64_opt_lenient` (deja-record/src/lib.rs:186-216) — the server is the only thing that parses recordings, so the Vector u64-stringification quirk never reaches the browser.
- `build_diff_rows`, `json_field_diff`, `logical_key`, `is_side_effect_boundary` (deja-tui/src/lib.rs:883-1176) move from deja-tui into a shared module (`deja-orchestrator::explorer`, re-exported to deja-tui) so TUI and web render identical rows from identical inputs.
- `divergence::detect` classification stays the single source of truth for scorecard counters.

---

## 1. Information architecture + page map (v1: the manual PR replay gate)

URL map (every view deep-linkable; filters live in query params, entities in path):

```
/                                  → redirect /runs
/recordings                        Recordings catalog (windows + sessions)
/recordings/:recordingId           Window/session detail: manifest, completeness, request preview
/replays/new?recording=:id         Schedule replay (recording + code ref + params)
/runs                              Runs list (live status)
/runs/:runId                       Run detail: lifecycle timeline, build info, logs, failure
/runs/:runId/scorecard             Scorecard view
/runs/:runId/explorer              Divergence explorer (request list + tabs)
/runs/:runId/explorer?corr=:cid&tab=diff|timeline|events|http&row=g:123
/runs/:runId/explorer/events/:gseq Raw event detail (own URL → shareable)
/runs/:runId/explorer/trace/:gseq  Trace-upward graph view for one event
/audit                             Append-only audit log viewer
/runners                           Runner VM status (thin, from runner registry)
```

### 1.1 Recordings catalog (`/recordings`)
Backed by the Postgres recording catalog (decision 9) populated by the window bundler/manifest writer (pipeline design). Two kinds in one table, distinguished by `kind`:
- `window` — continuous time-windowed production/staging capture: a selected time range over the Kafka→Vector→S3 stream, with a manifest.
- `session` — discrete named sessions (today's demo model, local dev).

Columns: id, kind, env (staging/prod/local), time window, event count, byte size, **completeness** (manifest sequence-coverage %, gap count — decision 10b makes this the loss-detection mechanism of record), boundary mix sparkline, **masking status** (`staging-unmasked` / `masked` / `blocked`), sampled-request count, created. Filters: env, kind, date range, completeness threshold. Detail page shows the manifest verbatim (per-source-run sequence ranges, gaps, loss accounting from the hardened Kafka sink's delivery counters), boundary histogram, and a paginated preview of driveable requests (http_incoming method/path/status). A "Schedule replay" CTA pre-fills `/replays/new`. Policy: windows with `masking_status=blocked` (prod, pre-masking-workstream) render but cannot be scheduled — button disabled with the reason.

### 1.2 Schedule replay (`/replays/new`)
The v1 gate's entry form. Fields:
- Recording: picker (search the catalog) or carried from the CTA. Shows completeness inline; warns below a configurable coverage threshold, requires an explicit "replay incomplete window anyway" checkbox (recorded in audit params).
- Code ref: repo (default the Hyperswitch repo) + ref kind (tag / commit / branch / PR#) + ref value → maps to the existing `CandidateSpec` variants `repo_sha|repo_branch|repo_pr` (lib.rs:24-32), now actually resolved by the runner (build-from-source design, decision 5).
- Params: kernel body allowlist (comma-sep JSONPaths → `KERNEL_BODY_ALLOWLIST`), runner pool (default), optional note.
- Actor: free-text name persisted in localStorage, sent as `X-Deja-Actor` on every mutating call (decision 8: auth-light, audit-ready; SSO replaces this header source later).
Submit → `POST /api/v1/runs` → redirect to `/runs/:id`. Honest UX note baked into the form: candidate builds take ~35 min cold (fat LTO, codegen-units=1); the form shows an expected-duration estimate from recent build telemetry.

### 1.3 Runs list (`/runs`)
Postgres-backed list (fixes "no GET /runs" gap). Columns: run id, status chip (pending/resolving/building/running/completed/failed), current stage + step/total + a "stuck?" indicator (now − stage_updated > 3× stage p90), recording link, code ref + short sha, verdict chip (pass/fail/inconclusive once scored), actor, created/duration. Filters: status, recording, ref, actor, verdict. Refresh: poll every 5 s (cheap list query); per-run SSE only on the detail page.

### 1.4 Run detail (`/runs/:runId`)
- **Lifecycle timeline**: horizontal stage bar from the new `run_stage_events` history (each stage: label, started_at, finished_at, status) — fixes the "only latest stage_updated_ms" gap. Live stage animates via SSE.
- **Build panel**: resolved `candidate_image` (docker image, digest, source ref, build duration, runner id) — fixes the "candidate_image always null" gap; links the runner's build log.
- **Logs**: structured per-stage log viewer (replaces ephemeral `lifecycle:` eprintlns), follow-mode via SSE `log` events, stage filter, download raw.
- **Failure**: failure_reason prominently, with the embedded docker log tail rendered as a code block.
- **Outcome strip**: once scored, verdict + summary counters + "Open explorer" / "Open scorecard".

### 1.5 Scorecard view (`/runs/:runId/scorecard`)
Direct port of the TUI Run tab from the existing `replay-scorecard/v1` JSON (divergence/mod.rs:100-177), no new data needed:
- Verdict banner (pass/fail/inconclusive + reason), warnings list (parse problems surface here loudly, mirroring the renderer's "coverage INCOMPLETE" stance).
- Summary grid: matched/total correlations, side_effect_divergences, omitted/novel, environmental_misses, uncorrelated tolerated.
- Per-boundary table: matched/diverged bars, kind chips, tier label, per-boundary rank histogram.
- **Rank-resolution histogram** over `summary.resolved_by_rank`: ranks 1-2 green (identity/logical-context), 3-4 green (content), 5 cyan (location), **6 amber "positional fallback / Recovered"** — the fragility signal (`POSITIONAL_FALLBACK_RANK=6`; note the legacy `recovered_rank5_calls` field name counts rank-6 hits, divergence/mod.rs:129-135 — the UI labels it correctly as rank-6).
- Per-correlation table: each row links into the explorer at that correlation.

---

## 2. The Divergence Explorer (`/runs/:runId/explorer`) — the centerpiece

"Structured logs with proper diffs." Three-pane layout:

```
┌ Request rail ──────┬ Main pane (tabs) ─────────────────────┬ Detail drawer ─────┐
│ 9/9 correlations   │ [Split diff] [Timeline] [Events] [HTTP]│ Event JSON /       │
│ ▸ POST /payments ✓ │                                        │ field-diff detail /│
│ ▸ POST /confirm ✗ 3│  diff rows / event rows for the        │ trace-upward panel │
│   GET /payments ✓  │  selected correlation                  │                    │
│ filter: diverged ▾ │                                        │                    │
└────────────────────┴────────────────────────────────────────┴────────────────────┘
```

The scorecard persists only counters (no per-divergence rows), so the explorer **re-derives rows server-side from the raw streams** — exactly the TUI's approach, computed once per run into a cached "explorer index".

### 2.1 Server-side explorer index (the data spine)
Built lazily on first explorer request (and proactively at scoring time), LRU-cached, keyed by run_id. Inputs and their exact roles:

| Artifact | Shape | Used for |
|---|---|---|
| `recordings/{rec}/events.jsonl` | `SemanticEvent` NDJSON | recorded timeline, http_incoming request rows, left side of diffs, `gseq → event` resolution, graph_node_id refs |
| `observed/{run}.jsonl` | `ObservedCall` NDJSON | candidate side: resolved/rank, consumed `source_event_global_sequence`, novel calls |
| `http-diffs/{run}.jsonl` | `HttpDiff` NDJSON | HTTP status/body rows per correlation |
| `runs/{run}.scorecard.json` | scorecard v1 | per_correlation pass/fail, summary, ranks |
| `lookup-tables/{run}.jsonl` | whole-doc `LookupTable` (despite extension) | expected-entry view, address-rank inspection per event |
| `recordings/{rec}/graph/execution-graph.jsonl` | `ExecutionGraphNode` NDJSON | trace-upward (§3) |

Index contents: per-correlation event offsets, the request-outcome join, precomputed DiffRows, a flat divergence index (also written to Postgres at scoring time for filtering without artifact reads). Universal join keys, verbatim from ground truth: `correlation_id` (all streams), `global_sequence` (observed ↔ lookup ↔ events), `(recording_run_id, graph_node_id)` (events ↔ graph).

### 2.2 Request rail
Port of TUI `request_outcomes` (lib.rs:477): join `http_incoming` semantic events (method/path from `event.request`) × scorecard `per_correlation` × `http_diffs`, keyed by correlation_id. Row: method, path, baseline→candidate status, pass/fail chip, divergence count, "not driven" badge for correlations the kernel skipped (/health, background-only). Filters: diverged-only (default when verdict failed), boundary, text search on path. Selecting a row sets `?corr=` and loads the tabs.

### 2.3 Split diff tab (primary view)
Server returns the exact `DiffRow` model from `build_diff_rows` (deja-tui/src/lib.rs:1031-1176), serialized:

```json
{ "rows": [
  { "kind": "matched", "label": "db::generic_filter  user_auth_methods",
    "left":  { "args": {...}, "result": {...} },
    "right": { "args": {...}, "substituted": true },
    "gseq": 2, "resolved_rank": 2,
    "trace": { "available": true, "graph_node_id": 3833 } },
  { "kind": "changed", "label": "db::insert  payment_attempt",
    "left": {...}, "right": {...},
    "field_diffs": [ { "json_path": "amount", "baseline": 100, "candidate": 150 } ],
    "gseq": 41, "resolved_rank": null, "trace": {...} },
  { "kind": "omitted", "label": "redis::set_key  pa_xxx", "left": {...}, "right": null, "gseq": 57 },
  { "kind": "novel",   "label": "db::insert  refunds",    "left": null, "right": {...}, "gseq": null,
    "trace": { "available": false, "reason": "novel_call_no_identity" } },
  { "kind": "http_status", "baseline": 200, "candidate": 500 },
  { "kind": "http_body", "field_diffs": [ { "json_path": "status", "baseline": "succeeded", "candidate": "failed" } ] }
] }
```

Semantics preserved exactly: rows in recorded `global_sequence` order, one per recorded **side-effect** (filter excludes `http_incoming|time|id|id_generation|uuid|rng|crypto|function`, lib.rs:896-901); `consumed` = resolved observed calls' `source_event_global_sequence`; an omitted recorded event FIFO-paired with a novel observed call sharing the logical key (`boundary\u{1}method\u{1}discriminator`, db→table / redis→key / http→url|path) fuses into **Changed** with recursive `json_field_diff` over args; leftover novels append; HTTP status/body rows close the list. One server-side addition over the TUI: each Matched/Changed row is joined back to its ObservedCall to carry `resolved_rank` (rank badge per row; rank 6 renders the amber "positional" badge inline).

Rendering: git-style split view (recorded left / candidate right) with an inline-unified toggle; Matched rows collapsed to one-line context by default ("show 12 matched calls" expanders); Changed rows expand to a field-level diff table (json_path / baseline / candidate, long values pretty-printed with intra-value char-level highlight); Omitted = left-only red, Novel = right-only green. Each row: rank badge, gseq anchor, copy-deep-link, and **Trace up** button (§3) when `trace.available`.

Honesty in the UI (stated in a help popover, because it confuses everyone once):
- The candidate's "right side" for Matched rows is the **substituted recorded value** — replay injects the recorded result, so there is no independent candidate result to diff for matched calls. Result diffs exist only where reality can diverge: Changed rows (args diff), HTTP status/body (true candidate output), and `is_error` flips.
- Changed is a heuristic fusion (one omitted + one novel of the same logical key). The scorecard counts them as 1 OmittedCall + 1 NovelCall; the explorer shows 1 Changed row. The UI shows both numbers with a "why these differ" tooltip rather than pretending they reconcile.

### 2.4 Timeline tab
Port of TUI Timeline (`substitution_status`, lib.rs:560): **all** recorded events for the correlation (including pure time/id boundaries hidden from the split diff), in `request_sequence` order. Row: gseq, boundary chip, trait::method, duration, status pill — `substituted (rank N)` (observed.source_event_global_sequence → gseq), `omitted`, `pure/tolerated`, `environmental` — plus a red "Novel calls" strip at the end (novel observed calls have no recorded position). Click → detail drawer with the raw event.

### 2.5 Events tab + detail drawer
Raw semantic-event table (TUI Events tab): seq, boundary, trait::method, duration, err, callsite file:line, graph-node column. Filters: boundary, error-only, text. The drawer shows the full `SemanticEvent` JSON (collapsible tree), the lookup-table entries derived from it (all address ranks with their keys — rank ladder 1 Explicit → 6 Sequence), and `callsite_identity` incl. `logical_context` span path. URL: `/runs/:id/explorer/events/:gseq`.

### 2.6 HTTP tab
http-diffs for the correlation: baseline vs candidate status, body_diff field table reusing the same FieldDiff renderer; `status_candidate: 0` rendered explicitly as "transport error (no response)" per kernel semantics (kernel main.rs:176-180).

### 2.7 Run-level divergence list
Above the rail, a flattened cross-correlation list (`GET .../divergences?kind=&boundary=`): every divergence row with kind/boundary/label/correlation/rank, sortable — the triage entry point for runs with many failing requests. Backed by the Postgres divergence index written at scoring time.

---

## 3. Trace-upward graph navigation (`/runs/:runId/explorer/trace/:gseq`)

From a divergent (or any) recorded event up the execution graph to the request root. Verified mechanics from ground truth: `SemanticEvent.graph_node_id` + `recording_run_id` → `execution-graph.jsonl` node → `parent_id` chain → root `HTTP request` span whose `fields.request_id == correlation_id`. **Every join is scoped by `(recording_run_id, node_id)`** — node_ids repeat across runs (7,395 collisions observed in hs41-latest); the API takes both and refuses unscoped queries.

### 3.1 Prerequisite: the graph must be captured
- Local/demo: one compose-overlay change — set `DEJA_GRAPH_DIR` on `hyperswitch-server` (the layer is already wired and env-gated in router_env setup.rs:125-127) and have the record lifecycle copy `graph/execution-graph.jsonl` into `recordings/{id}/graph/`. This ships with v1.
- Production (Kafka-only sink world, decision 10): graph nodes need a transport — proposed contract with the pipeline designer: emit nodes through the same hardened producer as a second artifact type (`artifact_type: "deja_graph_node"`, same envelope schema), Vector routes on artifact_type to `recordings/{run}/graph/` prefix; window manifests list graph objects alongside event objects. Until that lands, production/staging windows simply have no graph and the UI degrades (below).

### 3.2 API
- `GET /api/v1/recordings/:rec/graph/trace?run={recording_run_id}&node={graph_node_id}` → `{ "chain": [ {node_id, parent_id, span_name, target, level, fields, started_ns, closed_ns, duration_us} ... root→leaf ], "complete": true|false, "truncated_at": nodeId?, "truncated_reason": "parent_never_closed|parent_missing", "root_request_id": "...", "root_matches_correlation": true|false, "causal_parents": { nodeId: [ids] } }`
- `GET /api/v1/recordings/:rec/graph/subtree?run=&node=&depth=2` → children for the collapsible tree (lazy expansion).
Server builds a per-recording graph index (run-scoped node map + children map) on first access, LRU-cached; hs41-scale files (~37k lines) index in memory trivially; large windows fall back to per-recording SQLite index built at pull time (later).

### 3.3 UI
- **Breadcrumb** across the top: `HTTP request (req_id=…) › ROOT_SPAN › server_wrap › server_wrap_util › list_user_authentication_methods… › [this event]` — each crumb clickable to recenter; a green check on the root when `root_matches_correlation` (the fields.request_id == correlation_id verification), a warning glyph when it does not.
- **Collapsible tree** beneath: ancestry chain expanded, siblings lazy-loaded via subtree, the source event's node highlighted; node detail (fields, timing, level, causal `follows_from` edges shown as dashed links) in the drawer.
- Entry points: "Trace up" on every diff/timeline/event row that has `trace.available`.

### 3.4 Honest gap handling (what the UI does when links are missing)
1. **No graph captured for the recording** (all current demo data: graph_node_id null on 207/207, no graph file): trace buttons render disabled with tooltip "No execution graph captured for this recording — enable DEJA_GRAPH_DIR / graph transport"; the recording detail page shows a "graph: not captured" capability badge so users know before drilling in.
2. **Event has `graph_node_id: null` on a graph-enabled recording** (~27% of events on hs41: calls outside any observed span): disabled button, tooltip "this call ran outside an instrumented span". The timeline's graph column makes coverage visible at a glance.
3. **Broken chain mid-walk** (ancestor never closed → never written; nodes emit on span close only): render the partial chain from the event upward, terminate with an explicit `⚠ chain truncated — ancestor span did not close before shutdown` node; never silently show a fake root.
4. **NovelCall rows can never be traced** — structural: `ObservedCall` carries no graph/span ids and no candidate-side graph exists. The button shows "cannot trace: candidate-side novel call carries no graph identity"; closing this requires the ObservedCall record-format enrichment (logical span path or graph ids, replay.rs:752) + optional candidate-side graph capture — explicitly **later** scope, flagged as a record-format dependency.
5. **Wrong-run cross-links**: impossible via the API by construction (run-scoped joins are mandatory parameters).

---

## 4. Tech stack and data-fetching

- **SPA**: React 18 + Vite + TypeScript (strict). React Router v6 (URL = state; loaders per route). TanStack Query for fetching/caching/invalidations. TanStack Virtual for event/diff lists (timelines can be thousands of rows). Tailwind + shadcn/ui primitives. Diff rendering custom (the FieldDiff model doesn't map to text-diff libs); JSON tree via a small in-house collapsible component.
- **Serving**: SPA built to `dist/`, embedded in the deja-orchestrator binary via `rust-embed`. Routing: `/api/*` → JSON API; `/assets/*` → immutable-cached static; any other GET → `index.html` (SPA fallback). Legacy routes preserved at their current paths. One binary to deploy, no CORS, no separate web server — consistent with "web UI grown on deja-orchestrator".
- **Live progress**: SSE, not WebSockets, not raw polling. `GET /api/v1/runs/:id/stream` emits `run` (snapshot on status change), `stage` ({step, steps_total, stage, status, ts}), `log` ({ts, stage, level, line}), `scorecard_ready`, `done`; supports `Last-Event-ID` resume. Backed by Postgres LISTEN/NOTIFY (the lifecycle worker already persists every transition; the notify rides the same write). The UI keeps today's poll loop as automatic fallback (TanStack Query refetchInterval 2 s) when the EventSource errors — so the dashboard works even against a degraded server. Runs *list* polls at 5 s in v1 (a global SSE feed is later).
- **Deep links**: every view above has a stable URL; diff rows anchor as `?row=g:123` (gseq) or `?row=n:4` (novel index) so a teammate opens the exact row; "copy link" on every row/event/trace. Scorecard and explorer URLs are the artifacts pasted into PR review threads — this is the v1 gate's collaboration primitive.
- **Actor header**: a tiny client interceptor attaches `X-Deja-Actor` (localStorage) to all mutating requests; the API rejects mutations without it (400) to keep the audit log honest in the auth-light era.

---

## 5. API contract (what the UI consumes)

### 5.1 New endpoints (all `/api/v1`, JSON; pagination = `?page=&per_page=`, cursor later)

| Endpoint | Backing | Notes |
|---|---|---|
| `GET /recordings?kind=&env=&from=&to=&min_coverage=` | Postgres catalog | list, fields in §1.1 |
| `GET /recordings/:id` | Postgres + manifest | manifest verbatim, completeness, boundary mix, capability flags `{has_graph, masking_status}` |
| `GET /recordings/:id/requests` | events index | http_incoming preview (method, path, status, correlation_id) |
| `POST /runs` | Postgres + audit | extended RunSpec: `{recording_id, candidate:{kind:"repo_sha"|"repo_branch"|"repo_pr"|"repo_tag", repo, ref}, params:{body_allowlist?, note?}, ack_incomplete?:bool}`; requires X-Deja-Actor |
| `GET /runs?status=&recording_id=&actor=&verdict=` | Postgres | list |
| `GET /runs/:id` | Postgres | extended Run: `created_at/started_at/finished_at`, `stages:[{stage,step,status,started_at,finished_at}]`, `candidate_image:{docker_image,digest,source_ref,build_duration_s,runner_id}`, failure_reason |
| `GET /runs/:id/stream` (SSE) | LISTEN/NOTIFY | §4 |
| `GET /runs/:id/logs?stage=&after_seq=` | Postgres/log files | structured log lines |
| `GET /runs/:id/scorecard` | existing computation | unchanged scorecard v1 shape |
| `GET /runs/:id/requests?outcome=&q=` | explorer index | request-outcome rows (§2.2) |
| `GET /runs/:id/requests/:corr/diff` | explorer index | DiffRow[] (§2.3 shape) |
| `GET /runs/:id/requests/:corr/timeline` | explorer index | substitution-status rows (§2.4) |
| `GET /runs/:id/events/:gseq` | events index | full SemanticEvent + derived lookup addresses |
| `GET /runs/:id/divergences?kind=&boundary=` | Postgres divergence index | flat triage list (§2.7) |
| `GET /recordings/:rec/graph/trace?run=&node=` | graph index | §3.2 |
| `GET /recordings/:rec/graph/subtree?run=&node=&depth=` | graph index | §3.2 |
| `GET /audit?actor=&action=&from=` | Postgres append-only | every mutation: `{id, ts, actor, action, params(full), result, request_ip}` |
| `GET /runners` | runner registry | id, status, current run, last heartbeat |

### 5.2 Data that must START being captured (flags for the pipeline/orchestrator designer)
1. **Stage history** — per-stage start/finish/status rows (Postgres `run_stage_events`), not just the overwritten `stage_updated_ms`. Powers the lifecycle timeline.
2. **Run timestamps** — created/started/finished on the run row (today only hex-nanos in the id).
3. **Candidate build telemetry** — resolved image, digest, source ref, build duration, runner id, build log location (today `candidate_image` is always null and no resolver exists).
4. **Structured per-run logs** — the worker's eprintlns become persisted log rows/files addressable per run+stage (today ephemeral stderr).
5. **Recording catalog + window manifests** — completeness/coverage, sizes, env, masking status (decision 10b: manifests are the loss-detection mechanism of record; the catalog row is the UI's read model). Also: recordings pulled by the worker must get metadata (today only `POST /recordings` writes meta JSON).
6. **Divergence index rows** — written by `detect_and_score` at scoring time (run_id, correlation_id, kind, boundary, method, label, gseq?, rank?) so run-level filtering doesn't reread artifacts. Payloads still derive from artifacts on demand.
7. **Execution graph capture + transport** — `DEJA_GRAPH_DIR` in the record overlay (v1, local) and graph-node artifact type through the hardened Kafka sink → Vector → `graph/` prefix (production windows). Without it trace-upward is dark, as it is on all current demo data.
8. **Audit events** — append-only on every mutation (decision 8).
9. *(Later, record-format change)* **ObservedCall enrichment** — logical span path and/or graph ids on ObservedCall (replay.rs:752) to make novel calls traceable and improve Changed-pairing fidelity.
10. **Runner-uploaded artifacts** — once replays run on dedicated runner VMs the shared-filesystem assumption dies; the dormant `POST /runs/:id/{observed,http-diff}` ingest endpoints (or the runner protocol's artifact upload) must deliver observed/http-diffs/lookup-table to where the explorer index reads them. The UI only needs them present under the run; it is agnostic to transport.

---

## 6. v1 vs later

### v1 (the manual PR replay gate slice)
Recordings catalog (windows + sessions, completeness, masking badges) · Schedule-replay form with code-ref candidate · Runs list + run detail (stage timeline, build panel, structured logs, SSE with poll fallback) · Scorecard view · Divergence explorer complete (request rail, split diff with rank badges + field-level diffs, timeline, events + detail drawer, HTTP tab, run-level divergence list) · Trace-upward UI + graph API, live on graph-enabled local recordings (DEJA_GRAPH_DIR overlay change), gracefully dark elsewhere · Deep links everywhere · Audit capture + viewer · Actor header · Legacy route compat.

### Later (explicitly deferred)
Cross-run comparison (same recording, two refs, diff-of-scorecards) · Trends/flakiness over time per boundary/endpoint · Annotations/triage workflow (assign, ack, suppress-with-reason) · Allowlist/suppression rule editor feeding KERNEL_BODY_ALLOWLIST · Webhook/PR-status integration (no webhooks in v1 by decision) · SSO (auth bolt-on) · Novel-call upward tracing (ObservedCall enrichment + candidate-side graph) · Global SSE feed for the runs list · Per-recording SQLite indexes for production-scale windows · Run cancellation UI (needs orchestrator support) · Saved views/filters · Masking inspection tooling.
---

# Adversarial review

BLOCKING:
- Run-id env mismatch breaks the v1 local trace-upward join as specified. The design ships graph capture locally with "one compose-overlay change" (set DEJA_GRAPH_DIR) + a lifecycle copy, and makes (recording_run_id, node_id) MANDATORY scoping for every graph query. But graph nodes get recording_run_id from current_recording_run_id(), which reads ONLY DEJA_RUN_ID (crates/deja-record/src/lib.rs:60-66, stamped at span close in crates/deja-record/src/graph.rs:240), while the demo overlay sets only DEJA_RECORDING_RUN_ID (vendor/hyperswitch-deja-clean/docker-compose.deja.yml:83), which events resolve via the separate RecordingHook::resolve_recording_run_id (deja-record/src/lib.rs:409-419, DEJA_RECORDING_RUN_ID then DEJA_RUN_ID). Result: with exactly the changes the design specifies, every graph node carries recording_run_id=null while events carry rec-<id> — the run-scoped trace join matches nothing and the centerpiece v1 graph feature is silently dark (or the API must treat null as wildcard, defeating the 7,395-collision protection the scoping exists for). hs41's graph only joins because the bench sets DEJA_RUN_ID (nodes carry instr-1..5). Remedy is trivial but must be in the design: also set DEJA_RUN_ID in the overlay, or unify graph.rs on the same resolver as RecordingHook — plus an explicit cross-check (event.recording_run_id == node.recording_run_id) in the index builder so this class of mismatch surfaces as a capability warning instead of empty traces.

CORRECTIONS:
- POST /api/v1/runs offers kind "repo_tag" but CandidateSpec has no such variant (crates/deja-orchestrator/src/lib.rs:25-31: LocalPath | PrebuiltImage | RepoSha | RepoBranch | RepoPr). The design's own §1.2 correctly lists only repo_sha|repo_branch|repo_pr; the §5.1 API table contradicts it. Either add a RepoTag variant (coordination item with the build-from-source design) or resolve tags to shas at form-submit time and drop repo_tag from the API.
- "is_error flips" is not a renderable divergence surface: ObservedCall (crates/deja-record/src/replay.rs:753-773) carries no result and no is_error — the candidate side has no execution result in any artifact, and for Matched rows the candidate received the substituted recorded value by construction, so is_error cannot flip there. The help-popover claim should be cut or rescoped to HTTP status/body only (HttpDiff is the only true candidate output).
- Decision 10c consumer gap: deja-tui's artifact discovery reads the JSONL-sink copy, not the S3-pulled one. find_semantic_artifact looks for semantic-events.jsonl under root/, root/semantic/, root/recording/ (crates/deja-tui/src/lib.rs:12, 282-286) — i.e. the DEJA_ARTIFACT_DIR=/harness-state/recording file the doomed JSONL sink writes — and find_graph_artifact looks under root/graph/, not recordings/{id}/graph/. When the JSONL sink is removed (decision 10, local demo included), the TUI finds no events despite the shared-module parity claim. The design must repoint deja-tui discovery at recordings/{id}/events.jsonl (different filename AND location) and recordings/{id}/graph/. For the record, the other consumers already comply: lookup rendering reads root.recording_events_path (lifecycle/mod.rs:257), divergence scoring reads lookup/observed/http-diffs only (divergence/mod.rs:456-473), and visualize-replay.py globs recordings/*/events.jsonl (demo/visualize-replay.py:72).
- Audit coverage hole vs decision 8: the legacy mutating routes kept "verbatim for demo-script compat" (POST /recordings, legacy POST /runs, POST /runs/:id/{observed,http-diff}) bypass the X-Deja-Actor requirement and therefore the audit log — and the ingest POSTs are on the runner critical path per the design's own §5.2 item 10, so production-relevant mutations would be unaudited. Needs an explicit policy: synthetic actor (e.g. runner id / "legacy-demo") recorded for these routes, or legacy routes restricted to local-dev mode.
- SSE Last-Event-ID resume cannot be "backed by Postgres LISTEN/NOTIFY" alone — NOTIFY has no replay/retention and an ~8 KB payload cap (log lines can exceed it). Resume must be served by reading run_stage_events / log tables by sequence on reconnect, with NOTIFY used only as a wake-up ping. The tables exist in the design (§5.2 items 1, 4), so this is a wording/mechanism fix, not new scope.
- Artifact shape nit in §2.1: graph/execution-graph.jsonl lines are ExecutionGraphRecord wrappers {"node": {...}}, not bare ExecutionGraphNode rows (deja-record/src/graph.rs:80, read_execution_graph_records:204-218). The explorer's graph index parser must unwrap .node, like read_execution_graph_records does.
- Production graph transport understated: "emit nodes through the same hardened producer ... same envelope schema" requires (a) an envelope change — the deja.artifact_record/v1 Envelope's payload field is typed event: &SemanticEvent (vendor/.../services/kafka/deja_record_sink.rs:36-43), so a deja_graph_node artifact type is a schema addition, not reuse — and (b) a deja-record API change: ExecutionGraphLayer is hardcoded to JsonlSink in its only constructor (graph.rs:51-60); routing nodes through a Kafka sink needs a with_sink-style constructor plus router_env wiring beyond DEJA_GRAPH_DIR. The design flags this as a pipeline contract item, which is the right call, but should name these two concrete changes so they get sized.
- Toolchain pin risk for the axum/tokio/Postgres/rust-embed stack: the workspace pins rustc 1.85.1 (rust-toolchain.toml; workspace rust-version 1.85), and the kernel already hand-rolled HTTP/1.1 specifically because the icu 2.2 chain (via url/idna) requires 1.86 (kernel main.rs:184-186 comment). axum/tokio/tokio-postgres/rust-embed are fine on 1.85, but sqlx and anything pulling url 2.5+ → idna → icu will hit the same wall — the rework must pick the Postgres client against the pin or bump the toolchain first.
- Local graph capture bypasses the Kafka-only path that decision 10 mandates for the demo: graph nodes ride a bind-mounted JSONL file (DEJA_GRAPH_DIR) while semantic events ride Kafka→Vector→S3. Defensible (decision 10's text targets the recording sink), but the design should state it explicitly and note that window-manifest completeness markers (decision 10b) will not cover the locally-copied graph artifact — graph loss is invisible until the production transport lands.
- Minor line-number drift (all claims substantively verified): CandidateSpec is lib.rs:23-31 not 24-32; scorecard model divergence/mod.rs:100-202 not 100-177; ObservedCall replay.rs:753 not 752; kernel status-0 arm ~main.rs:170-180; de_u64_opt_lenient fn at deja-record/src/lib.rs:201-216 (attribute use at 186). Substance correct in every case.

NOTES:
Adversarial verification done read-only against the worktree and vendored fork; no builds/docker run. The design's ground-truth claims are overwhelmingly accurate — I verified essentially every checkable assertion: tiny_http one-request-at-a-time loop (deja-orchestrator/src/main.rs:54-61) and the missing GET /runs; dormant POST /runs/:id/{observed,http-diff} ingest routes; candidate_image never set (api/runs.rs:25, no resolver anywhere); stage/step/stage_updated_ms overwritten in place (lib.rs:81-89, lifecycle set_stage:129-138) with ephemeral eprintln logs; lifecycle stages and statuses exactly as listed (record 6 steps, replay 6 steps incl. redis FLUSHALL and MinIO pull — replay already consumes the S3-pulled copy, consistent with decision 10); detect_and_score writes runs/{run}.scorecard.json; scorecard is counters-only (per_correlation has no row payloads) so server-side re-derivation is genuinely required; build_diff_rows/json_field_diff/logical_key/is_side_effect_boundary at deja-tui/src/lib.rs:896-1176 with exactly the claimed semantics (recorded-order rows, consumed = resolved source_event_global_sequence, FIFO logical-key fusion via \\u{1} keys, matched right side = substituted recorded args, HTTP rows last); request_outcomes at 477 and substitution_status at 560; POSITIONAL_FALLBACK_RANK=6 with legacy-named recovered_rank5_calls counting rank-6 (divergence/mod.rs:94, 129-135, 292-296); Address rank ladder 1 Explicit → 6 Sequence (deja-record/src/replay.rs:680-724); de_u64_opt_lenient for Vector u64-stringification (deja-record/src/lib.rs:186-216, syntax_hash only); lookup-table written as whole-doc pretty JSON via write_json despite .jsonl name, served as application/x-ndjson, with lenient dual-shape LocalFileLookupSource (replay.rs:953-976); ObservedCall carries resolved_rank but no graph/span identity → novel calls structurally untraceable; kernel: hand-rolled HTTP/1.1, no chunked encoding, status 0 on transport error, KERNEL_BODY_ALLOWLIST env; DEJA_GRAPH_DIR env-gated layer in vendored router_env/src/logger/setup.rs:125-127; graph nodes emit on span close only with per-process AtomicU64 node ids; Vector key_prefix templates on event fields ({{ .recording_run_id }}), so artifact_type routing is plausible. Data claims verified exactly: hs41-latest graph = 36,831 lines, 7,395 node_ids occur >1 time across 5 runs; 27.4% of 17,268 hs41 events have null graph_node_id; demo recordings 207/207 null graph_node_id with no graph file anywhere in harness-state; 4,949/4,954 'HTTP request' root spans' fields.request_id match event correlation_ids. Vendored Cargo profile has lto=true + codegen-units=1 (35-min estimate plausible, unverifiable without building). Scale math is honest: v1 is staging-only with an acknowledged in-memory size guard; deferred SQLite indexes are correctly scoped. The single blocking item is the DEJA_RUN_ID vs DEJA_RECORDING_RUN_ID resolver split, which as-specified nulls the graph-side join the design itself makes mandatory; fix is small but must be added to the §3.1 v1 scope. Everything else is corrections: a phantom repo_tag candidate kind, the non-derivable is_error-flip claim, deja-tui's discovery still pointing at the JSONL-sink file (the one decision-10c consumer the design missed), unaudited legacy mutation routes, SSE resume mechanics, the {\"node\":...} wrapper shape, the understated graph-transport plumbing, and the rustc 1.85 pin vs the icu-1.86 dependency wall already documented in the kernel.