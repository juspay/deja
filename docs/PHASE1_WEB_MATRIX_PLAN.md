# Phase 1 — The demo matrix from the web UI

> Execution plan for the first implementation slice of
> [REPLAY_PLATFORM_DESIGN.md](REPLAY_PLATFORM_DESIGN.md): everything the current
> `run-deja-matrix.sh` does, driven manually from a browser, with the candidate
> supplied as a **local binary path** — the same form field that later becomes
> PR/branch/commit/tag when build-from-source (M3) lands.

## Definition of done

From a browser against the local stack, a human can:

1. **Record** — start a record run (iterations param), watch live stage progress
   and logs, see the recording appear in a sessions list.
2. **Replay (self)** — pick the recording, paste the path to the unpatched router
   binary, run, get the scorecard: PASS 9/9, rank₂ resolution histogram.
3. **Patch + build in a terminal** (the form displays the copyable commands:
   `git -C vendor/… apply demo/cross-version/benign-line-shift.patch`, the
   fast-profile cargo build, the revert) — then **replay (benign)**: paste the
   new binary path, expect PASS.
4. Same for **real-change** — expect DIVERGE with the known signature
   (7/9 · 1 status + 1 body + 11 side-effects).
5. Every run is listed, deep-linkable, has stage timings + logs, and every
   mutation carries an actor and lands in the audit log.

**Scripts are first-class clients of the same orchestrator.** `run-deja-demo.sh`
and `run-deja-matrix.sh` keep working as the convenient one-shot drivers — and
because they POST through the same API, every script-driven run appears in the
web UI exactly like a UI-driven one: stages, logs, scorecard, and all artifacts
(recording, lookup table, observed calls, http-diffs, **execution graph**, the
static HTML visualization). Run the script; browse the results in the UI.

Gate: the UI-driven matrix reproduces the script-driven baselines exactly, and
the scripts (updated as API clients, §W4b) stay green.

## Decisions (this round)

| Decision | Choice |
|---|---|
| Toolchain | **Per-crate MSRV split**: workspace toolchain bumps (≥1.86 for the sqlx/url chain; pick current stable); the 5 runtime crates HS consumes keep a CI-enforced **1.85** `cargo check`. Unblocks axum/sqlx and retires the diesel-cli stable-toolchain workaround. |
| Matrix modeling | **Manual, UI-driven sequence** — no run-group entity this phase. The human runs the three replays in order, patching + rebuilding between them. An optional free-text `expectation` field on the replay form is stored + displayed (audit/context only, no verdict logic). |
| Candidates | **`local_binary` path only** — no helper script, no orchestrator builds. The form shows the build/patch commands to copy. |
| UI depth | Matrix flow + runs + scorecards + an **artifacts panel** per run (view/download everything, embedded static HTML visualization). The interactive divergence explorer + graph trace-up stay in deja-tui this phase (next slice). |
| Script parity | The demo scripts emit/register **all** artifacts (graph capture ON) and print the run's UI URL — script runs are indistinguishable from UI runs in the dashboard. |

## Scope vs the master design

- **Implements**: M0 (axum + Postgres + migrations + audit + local-pg bootstrap),
  the M6 slice above, and the `LocalPath` candidate (a variant that exists in
  `CandidateSpec` but was never resolvable).
- **Defers**: runner extraction (M1), event-identity/sink work (M2), build-from-
  source (M3), compactor/catalog (M4), store ops (M5), the explorer + graph
  trace-up, run-groups, SSO/webhooks.

## Work breakdown (ordered)

### W1 — Toolchain split (S)
`rust-toolchain.toml` → current stable. CI: existing jobs run on the new pin;
new `msrv-runtime` job runs `cargo check -p deja -p deja-context -p deja-core
-p deja-derive -p deja-record` on **1.85.0**. Remove the `cargo +stable`
diesel-cli workaround comment in `demo/lib.sh` if the bump covers it.
Risk: vendored HS stays on its own toolchain — unaffected (separate workspace).

### W2 — Server rework: tiny_http → axum/tokio (M)
`deja-orchestrator` becomes an axum app (graceful shutdown, tracing). All
existing routes preserved at their current paths/shapes (demo scripts are the
regression suite). New surface under `/api/v1`. SSE endpoint skeleton
(`GET /api/v1/runs/:id/stream`) emitting stage/log/done events; UI falls back
to polling when the stream errors.

### W3 — Postgres store (M)
New `crates/deja-store` (sqlx, embedded migrations — the single
migration crate the master design mandates). Schema slice:

```sql
recordings(recording_id PK, kind='session', source_path, event_count,
           correlation_count, byte_size, created_by, created_at, status)
replay_runs(run_id uuid PK, mode, recording_id FK?, candidate jsonb,
            candidate_sha256, params jsonb, config_snapshot jsonb,
            state, verdict, scorecard jsonb, failure jsonb,
            expectation text, created_by, created_at, started_at, finished_at)
run_stages(id PK, run_id FK, stage, status, started_at, finished_at, detail jsonb)
run_log_chunks(run_id, stage, seq, lines, ts, PK(run_id, stage, seq))
artifacts(id PK, run_id FK, kind, uri, bytes, sha256, created_at)
  -- kind: events|lookup_table|observed|http_diffs|scorecard|graph|graph_replay|
  --       visualization_html|log ; uri = HarnessRoot path in local mode
audit_events(id PK, ts, actor, action, object_type, object_id, params jsonb)
  -- INSERT-only grants + rules, per the master design
```

Local-mode bootstrap: the orchestrator starts/uses a dedicated
`deja-orchestrator-pg` container on a non-default host port at boot (compose
fragment in `demo/`), independent of the per-run demo stack. One-shot importer
backfills existing `runs/*.json` for continuity. File-based artifacts stay
(HarnessRoot) — only *state* moves to Postgres.

### W4 — Lifecycle upgrades (M)
- **Stage history**: every `set_stage` also appends a `run_stages` row; worker
  `eprintln!`s become persisted log chunks (and still go to stderr).
- **`local_binary` candidate**: run spec accepts
  `{kind: "local_binary", path: "/abs/path/to/router"}`. The lifecycle
  validates (exists, ELF, x86_64), computes + stores `candidate_sha256`
  (the UI's compile-neutral signal — same sha as the previous candidate ⇒
  warning badge, replacing the script's sha-compare), stages the binary into a
  per-run docker build context, bakes `deja-candidate:<run_id8>` from a thin
  Dockerfile (the existing `Dockerfile.hyperswitch-semantic` pattern with the
  binary COPY'd from the staging dir), and points the compose overlay at that
  image (`CANDIDATE_IMAGE` env → overlay `image:` template). `prebuilt_image`
  keeps working (the demo scripts' path).
- Record mode unchanged except stage/log persistence.
- **Graph capture ON in the demo overlay** (no deja-record changes needed this
  phase): set BOTH `DEJA_GRAPH_DIR` *and* `DEJA_RUN_ID` (alongside the existing
  `DEJA_RECORDING_RUN_ID`) on the record **and** replay services — setting both
  env vars sidesteps the resolver split that would otherwise null the
  event↔node join (the proper resolver unification stays in M2). The lifecycle
  copies `execution-graph.jsonl` into the run/recording artifact layout
  (`recordings/{id}/graph/` for record, `runs/{id}/graph-replay/` for replay).
- **Artifact registration**: at each stage end the lifecycle registers produced
  artifacts (kind, path, bytes, sha256) in the `artifacts` table, including the
  `visualize-replay.py` HTML it already generates. The UI reads this — nothing
  scans the filesystem.

### W4b — Scripts become first-class API clients (S)
`demo/lib.sh`'s `post_run`/`poll` switch to `/api/v1/runs` (legacy routes remain
for back-compat but the shipped scripts use v1): they send
`X-Deja-Actor: script:$USER`, and the matrix script passes its per-candidate
`expectation` (pass/diverge) in the run spec — so the audit log and the runs
list attribute and annotate script runs properly. After each run the scripts
print the deep link (`http://127.0.0.1:8070/runs/<id>`); the final matrix
summary prints the three scorecard URLs next to the TUI commands. `--keep`
semantics unchanged.

### W5 — API v1 (S/M)
| Endpoint | Notes |
|---|---|
| `GET /api/v1/recordings` | sessions list (Postgres) |
| `POST /api/v1/runs` | `{mode, recording_id?, candidate, params, expectation?}`; requires `X-Deja-Actor`; audit row in the same tx |
| `GET /api/v1/runs` / `/runs/:id` | list + detail (stage history, candidate sha, verdict) |
| `GET /api/v1/runs/:id/logs?stage=` | persisted chunks |
| `GET /api/v1/runs/:id/stream` | SSE (stage/log/done), poll fallback |
| `GET /api/v1/runs/:id/scorecard` | existing scorecard JSON |
| `GET /api/v1/runs/:id/artifacts` | registered artifacts; `GET …/artifacts/:id/raw` streams the file (HTML visualization served inline) |
| `GET /api/v1/audit` | append-only viewer |
Legacy routes stay verbatim; mutating legacy routes get actor `legacy-demo`.

### W6 — SPA (M/L)
React 18 + Vite + TS (strict), TanStack Query, served via rust-embed at `/`.
Pages:
- **Recordings** — sessions table, "Replay this" CTA.
- **New run** — record form (iterations) and replay form (recording picker,
  binary-path input with validation feedback, optional expectation note, params)
  with a **copyable command block**: the patch-apply, fast-profile build
  (`lto=false, codegen-units=256, opt-level=2, incremental, mold` — the
  `DEMO_CARGO_PROFILE` set), and patch-revert commands, templated for the
  selected cross-version scenario.
- **Runs** — list (state chips, verdict chips, candidate sha-prefix, actor);
  detail = stage timeline (from `run_stages`), live logs (SSE), failure tail,
  scorecard link.
- **Scorecard** — verdict banner, summary grid, per-boundary table,
  rank-resolution histogram (rank-6 amber = positional fragility), per-
  correlation pass/fail rows. Direct port of the TUI run tab's data.
- **Artifacts panel** (run detail) — every registered artifact with size/sha,
  view/download; the static `replay-visualization.html` embedded inline; graph
  files listed (their interactive viewer is the next slice); copyable
  "open in TUI" command for the run's state dir.
- Deep links for every view; `X-Deja-Actor` from localStorage on mutations.

### W7 — Validation (S)
- UI-driven matrix vs the recorded baselines: self PASS 9/9 rank₂=197,
  benign PASS 9/9, real DIVERGE 7/9 · 1/1/11.
- `run-deja-demo.sh` + `run-deja-matrix.sh` green, untouched.
- `just verify` green on the new toolchain; `msrv-runtime` green on 1.85.
- Audit log shows the full matrix session with actors and params.
- **Script-parity check**: one full `run-deja-matrix.sh` run, then verify in the
  UI: all four runs listed with `script:` actors and expectations, stages/logs
  present, graph artifacts registered for record AND replays (non-empty,
  joinable — spot-check `event.recording_run_id == node.recording_run_id`),
  HTML visualization renders inline.

Suggested order: W1 → W2 → W3 → W4 → W5 → W6 → W7, with W2/W3 parallelizable
and the SPA scaffold startable any time after W5's contracts settle.

## Risks / notes

- **Binary trust**: a pasted path is executed in the replay container — local
  single-operator mode only; the form says so. (Runner-era candidates come from
  builds, not paths.)
- **Docker context staging**: per-run build context avoids the repo-root
  `.dockerignore` coupling entirely; the staged dir holds exactly
  {Dockerfile, router, workload.sh, superposition_seed.toml}.
- **sha-equality warning** replaces the script's compile-neutral check 1:1.
- **Port hygiene**: orchestrator pg + API (8070) + replay (8090) — the pg port
  must not collide with the demo stack's unpublished pg (it won't: separate
  container, explicit non-default port).
- The phase deliberately does **not** touch deja-record or the vendor patch —
  zero replay-semantics risk; the scorecards must come out byte-identical to
  the script flow because the same lifecycle code produces them.

---

## Validation results (W7, executed 2026-06-13)

- **Demo parity**: `run-deja-demo.sh` through the full new stack (axum server,
  Postgres dual-writes, graph capture, artifact registration) — scorecard
  **identical to the baseline**: PASS 9/9, 0 mismatches, rank₂ = 197.
- **Script-as-client**: both runs in the store with `script:$USER` actors,
  full 6-stage history, persisted logs, deep links printed.
- **Artifacts**: 8 kinds registered incl. record graph (294 KB), replay graph
  (180 KB), embedded HTML visualization.
- **Graph join (first time ever)**: 207/207 events carry `graph_node_id`;
  831 nodes; event↔node `recording_run_id` scopes match exactly.
- **`local_binary` candidate**: sha256 → image bake → CANDIDATE_IMAGE
  injection → full lifecycle → scoring, all working. The standalone replay
  (fresh stack, no shared record-phase pg) surfaced the documented B3
  fidelity caveat — 3 novel calls (a cache-miss fallback chain) on one
  request; a control replay with identical code on the same stack passed
  clean, exonerating the candidate path. Single-session flows (the matrix
  scripts) are unaffected; the DB-state provisioning contract is Phase 2 /
  master-design §5.3 work.
- Gates: `just verify` + `msrv-runtime` green at every commit.
