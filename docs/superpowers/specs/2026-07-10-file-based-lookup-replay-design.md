# File-based lookup replay for the sandbox

**Date:** 2026-07-10
**Status:** Approved design
**Supersedes:** the IMC push-based lookup flow described in
`2026-07-08-sandbox-replay-design.md` (that doc remains valid for everything
else: orchestration, seeding, superposition, ECR, callbacks).

## Problem

The k8s replay sandbox pushes lookup data into the candidate router at
runtime: the agent renders the lookup table, then POSTs a per-correlation
slice into the router's `/deja/lookup` admin endpoint before driving each
request, and drains observed calls back over `/deja/observed`
(`ROUTER__DEJA__REPLAY__SOURCE=imc`).

The hyperswitch fork has changed: the router now reads the **entire lookup
table from a file at startup** and writes observed calls to a **file sink**.
The `/deja` HTTP admin endpoints no longer exist. The sandbox must conform.

The local demo already runs the target architecture: the orchestrator renders
the whole table to `{state_dir}/lookup-tables/{run_id}.jsonl` on a shared
bind mount, and the overlay compose sets
`ROUTER__DEJA__REPLAY__SOURCE=/harness-state/lookup-tables/${RUN_ID}.jsonl`
and `ROUTER__DEJA__REPLAY__OBSERVED_SINK=/harness-state/observed/${RUN_ID}.jsonl`.

## Contract

The candidate router process is configured entirely by env:

| Env var | Value |
|---|---|
| `ROUTER__DEJA__MODE` | `replay` |
| `ROUTER__DEJA__REPLAY__SOURCE` | `<state_dir>/lookup-tables/<run_id>.jsonl` (a file path; `imc` remains recognized as the legacy in-memory mode) |
| `ROUTER__DEJA__REPLAY__OBSERVED_SINK` | `<state_dir>/observed/<run_id>.jsonl` |

The router loads the full table once at startup and resolves boundary values
itself during replay. Whoever starts the router must guarantee the table file
exists first.

Path conventions are `HarnessRoot`'s existing layout
(`crates/deja-orchestrator/src/lib.rs`), so the divergence scorer
(`detect_and_score`) reads the observed file from where the router wrote it,
with no shuttling.

## Design

### 1. Pod topology: one replay Job

The sandbox chart's router Deployment and agent Job merge into a single
**Job** per run. Containers stage the run via k8s-native ordering:

1. **`prepare` (regular init container, agent image).** New agent mode.
   Pulls the recording from S3 (`recording_id` / `recording_uri`), renders
   the **entire** lookup table once, and writes to a shared `emptyDir`
   mounted at the state dir:
   - `<state_dir>/recordings/...` — events file (agent layout as today)
   - `<state_dir>/lookup-tables/<run_id>.jsonl` — the full table
   - `<state_dir>/observed/<run_id>.jsonl` — pre-created empty (also the
     http-diffs file), so the reset happens before the router boots
   Fails the pod if S3 pull fails or the rendered table is empty.
2. **`router` (native sidecar: init container with `restartPolicy: Always`,
   requires k8s ≥ 1.28).** The candidate image. Starts only after `prepare`
   succeeds. Mounts the same volume; env per the contract above. No lookup
   admin port, no admin service.
3. **`agent` (main container).** Waits for the router's plain `/health`,
   drives each recorded request in the existing correlation order with the
   existing `x-request-id` injection, then runs `detect_and_score` against
   the shared state dir, uploads artifacts, posts the verdict callback, and
   exits. Pod completes → Job completes → run is done.

Postgres, Redis, Superposition, card vault, migrations: unchanged, still
separate resources in the run namespace.

**Cluster requirement:** native sidecars need Kubernetes ≥ 1.28. Confirmed
acceptable for all target clusters.

### 2. Agent changes (`deja-replay-agent`)

- New `prepare` entrypoint (CLI mode on the same binary/image) that factors
  out today's steps 1–2 (pull recording, render table, reset artifact files)
  and stops there.
- The drive path drops the lookup surface: `SandboxClient` shrinks to
  `wait_healthy` + `drive`. `install_lookup`, `clear_lookup`,
  `drain_observed`, the ambient-slice install, and the per-correlation
  install/clear loop are deleted.
- Health check moves from `{lookup_admin}/health` to the router's `/health`;
  the `router.lookup_admin` config key is removed.
- Observed calls are no longer collected by the agent; the router writes
  them to the observed sink path directly. The scorer reads them from
  `HarnessRoot::observed_path` as it already does.
- HTTP response diffs: the agent still records `compare_response` output to
  `http-diffs.jsonl` per driven request, as today.
- Artifact upload set is unchanged (events, lookup-table, observed,
  http-diffs, scorecard, call-ledger).

### 3. Runtime changes (`deja-runtime`)

`lookup_replay_hook_from_env`:

- `ROUTER__DEJA__REPLAY__SOURCE=imc` → IMC store (unchanged, legacy).
- Any other non-empty value → treat as a file path →
  `LocalFileLookupSource::new(path)`.
- `ROUTER__DEJA__REPLAY__OBSERVED_SINK=<path>` → `FileObservedSink`
  (falls back to `InMemoryObservedSink` when unset).
- Legacy `DEJA_LOOKUP_TABLE` / `DEJA_OBSERVED_SINK` keep working with
  current precedence semantics.

This makes candidate images built from these crates honor the same env
contract the hyperswitch fork now uses.

### 4. Chart changes (`replay-sandbox/chart`)

- `stack/router-configmap.yaml`: drop `ROUTER__DEJA__REPLAY__SOURCE: "imc"`
  and `ROUTER__DEJA__REPLAY__LOOKUP_DIR`; the two file-path envs move onto
  the Job template (they embed the run id and the mounted state dir).
- `stack/router-deployment.yaml` + `agent.yaml` (Job) → replaced by one
  `replay-job.yaml` implementing the topology above. The agent ConfigMap
  (agent.toml) and Secret stay, minus the removed `lookup_admin` key.
- Router Service: retained for in-namespace access (agent uses localhost
  within the pod; the Service remains only if anything else needs the
  router; drop the lookup-admin port either way).
- `values.yaml`: remove lookup-admin knobs; add nothing new beyond an
  optional state-dir mount path (default `/harness-state`).
- Orchestrator (`lifecycle/sandbox.rs`): kill/status/log plumbing follows
  the resource rename (Job instead of Deployment+Job).

### 5. Local demo

No code change expected: `drive_replay` already renders the full table to
`{state_dir}/lookup-tables/{run_id}.jsonl` and the overlay compose already
sets both envs. Verify end-to-end once the runtime change lands (the local
candidate binary must resolve the path-valued `SOURCE` via the updated
`lookup_replay_hook_from_env`).

### 6. Failure handling

- `prepare` failure (S3 error, empty table) → init container fails → Job
  backoff/failure → existing orchestrator failure and kill paths apply.
- Router crash-loop → sidecar restartPolicy keeps restarting it; the agent's
  health deadline (`limits.health_deadline_secs`) turns a never-healthy
  router into a run failure as today.
- Agent failure mid-drive → Job fails; artifacts written so far remain in
  the emptyDir only if uploaded — the verdict callback simply never fires
  and the dashboard's existing timeout/kill path closes the run.

### 7. Testing

- `deja-runtime`: unit tests for env parsing — path-valued `SOURCE`,
  `OBSERVED_SINK` file sink, `imc` legacy, legacy var precedence.
- `deja-replay-agent`: existing `FakeClient` tests updated for the shrunken
  trait; new test that the prepare mode writes table + resets artifacts and
  that the drive path never calls a lookup API.
- Chart: `helm template` sanity check of the Job (container ordering,
  restartPolicy, env values, volume mounts).
- End-to-end: one sandbox run against a real recording; verify the router
  boots with the file source, observed lands in the shared file, verdict
  posts.

## Out of scope

- Removing the IMC code path from `deja-runtime` (kept for back-compat).
- Any change to recording, seeding, superposition, or ECR flows.
- The hyperswitch fork itself (its file-source support already exists).
