# File-Based Lookup Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the sandbox's push-based IMC lookup flow with a file-based one: an init container builds the whole lookup table to a shared volume, the router reads it via `ROUTER__DEJA__REPLAY__SOURCE=<path>` and writes observed calls to `ROUTER__DEJA__REPLAY__OBSERVED_SINK=<path>`, and the agent only drives HTTP requests.

**Architecture:** One replay Job per run: `prepare` init container (agent image) pulls the recording from S3 and renders the full lookup table onto an `emptyDir`; the candidate router runs as a native sidecar reading/writing that volume; the agent runs as the main container and just drives requests, scores, uploads, and posts the verdict. `deja-runtime` learns to treat a non-`imc` `ROUTER__DEJA__REPLAY__SOURCE` as a file path.

**Tech Stack:** Rust (workspace crates `deja-runtime`, `deja-replay-agent`, `deja-replay-core`), Helm chart `replay-sandbox/chart`, Kubernetes native sidecars.

**Spec:** `docs/superpowers/specs/2026-07-10-file-based-lookup-replay-design.md`

## Global Constraints

- Kubernetes ≥ 1.28 required (native sidecar: init container with `restartPolicy: Always`).
- Path convention is `HarnessRoot` layout (`crates/deja-orchestrator/src/lib.rs:157`): lookup table `{root}/lookup-tables/{run_id}.jsonl`, observed `{root}/observed/{run_id}.jsonl`, http-diffs `{root}/http-diffs/{run_id}.jsonl`, events `{root}/recordings/{recording_id}/events.jsonl`.
- `imc` stays a recognized value of `ROUTER__DEJA__REPLAY__SOURCE` (legacy); legacy `DEJA_LOOKUP_TABLE` / `DEJA_OBSERVED_SINK` keep working.
- No orchestrator (`lifecycle/sandbox.rs`) changes: it drives via `helm upgrade --install --wait` + verdict-callback polling and never names the router Deployment or agent Job.
- Commit after every task; run the named tests before each commit.
- Repo convention: test modules carry `#[allow(clippy::unwrap_used)] // tests panic on failure by design`.

---

### Task 1: deja-runtime — path-valued replay source + observed sink env var

**Files:**
- Modify: `crates/deja-runtime/src/lib.rs` (constants around line 68; `lookup_replay_hook_from_env` at lines 1347–1390; tests in the existing `#[cfg(test)]` module at the bottom of the file)

**Interfaces:**
- Consumes: `crate::replay::{LocalFileLookupSource, FileObservedSink, InMemoryObservedSink, LookupTableHook, ImcLookupStore}` (all existing).
- Produces: `pub const ROUTER_DEJA_REPLAY_OBSERVED_SINK_ENV_VAR: &str = "ROUTER__DEJA__REPLAY__OBSERVED_SINK"` and a private, unit-testable `lookup_replay_hook_from(LookupReplaySelection) -> Option<RuntimeHook>`. Behavior of `runtime_hook_from_env()` with `ROUTER__DEJA__MODE=replay` + path-valued `ROUTER__DEJA__REPLAY__SOURCE` is what Task 3's chart relies on.

- [ ] **Step 1: Write the failing tests**

Add to the existing `#[cfg(test)]` module in `crates/deja-runtime/src/lib.rs` (uses `tempfile`, already a dev-dependency of this crate — verify with `grep tempfile crates/deja-runtime/Cargo.toml`, add `tempfile = "3"` to `[dev-dependencies]` if absent):

```rust
fn selection(
    replay_source: Option<&str>,
    legacy_lookup_mode: Option<&str>,
    legacy_table: Option<&str>,
    observed_sink: Option<&str>,
) -> LookupReplaySelection {
    LookupReplaySelection {
        replay_source: replay_source.map(str::to_owned),
        legacy_lookup_mode: legacy_lookup_mode.map(str::to_owned),
        legacy_table: legacy_table.map(str::to_owned),
        observed_sink: observed_sink.map(str::to_owned),
    }
}

#[test]
fn imc_replay_source_selects_the_imc_store() {
    let hook = lookup_replay_hook_from(selection(Some("imc"), None, None, None));
    assert!(matches!(hook, Some(RuntimeHook::LookupReplay(_))));
    // legacy DEJA_LOOKUP_MODE=imc keeps working too
    let hook = lookup_replay_hook_from(selection(None, Some("imc"), None, None));
    assert!(matches!(hook, Some(RuntimeHook::LookupReplay(_))));
}

#[test]
fn path_valued_replay_source_loads_the_file_table_and_creates_the_observed_sink() {
    let dir = tempfile::tempdir().unwrap();
    let table_path = dir.path().join("lookup-tables").join("run-1.jsonl");
    std::fs::create_dir_all(table_path.parent().unwrap()).unwrap();
    std::fs::write(
        &table_path,
        r#"{"recording_id":"rec-1","policy_version":1,"entries":[]}"#,
    )
    .unwrap();
    let observed_path = dir.path().join("observed").join("run-1.jsonl");
    let hook = lookup_replay_hook_from(selection(
        Some(table_path.to_str().unwrap()),
        None,
        None,
        Some(observed_path.to_str().unwrap()),
    ));
    assert!(matches!(hook, Some(RuntimeHook::LookupReplay(_))));
    // FileObservedSink::create made the parent dir and the file
    assert!(observed_path.exists());
}

#[test]
fn missing_table_file_disables_the_lookup_hook() {
    let hook = lookup_replay_hook_from(selection(
        Some("/nonexistent/deja/lookup.jsonl"),
        None,
        None,
        None,
    ));
    assert!(hook.is_none());
}

#[test]
fn legacy_lookup_table_var_still_selects_file_mode() {
    let dir = tempfile::tempdir().unwrap();
    let table_path = dir.path().join("table.jsonl");
    std::fs::write(
        &table_path,
        r#"{"recording_id":"rec-1","policy_version":1,"entries":[]}"#,
    )
    .unwrap();
    let hook = lookup_replay_hook_from(selection(
        None,
        None,
        Some(table_path.to_str().unwrap()),
        None,
    ));
    assert!(matches!(hook, Some(RuntimeHook::LookupReplay(_))));
}

#[test]
fn no_replay_selectors_means_no_lookup_hook() {
    assert!(lookup_replay_hook_from(selection(None, None, None, None)).is_none());
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p deja-runtime imc_replay_source path_valued_replay_source missing_table_file legacy_lookup_table_var no_replay_selectors 2>&1 | tail -20`
Expected: compile error — `LookupReplaySelection` and `lookup_replay_hook_from` not defined.

- [ ] **Step 3: Implement the selection struct and refactor the hook constructor**

In `crates/deja-runtime/src/lib.rs`, add the new constant next to the existing ones (after line 68):

```rust
/// Router-prefixed observed-call sink path used by file-source replay.
pub const ROUTER_DEJA_REPLAY_OBSERVED_SINK_ENV_VAR: &str = "ROUTER__DEJA__REPLAY__OBSERVED_SINK";
```

Replace the body of `lookup_replay_hook_from_env` (lines 1347–1390) with:

```rust
/// Selector inputs for the lookup-replay hook. Extracted from process env by
/// [`lookup_replay_hook_from_env`]; a separate struct so unit tests can drive
/// the selection without mutating process env.
struct LookupReplaySelection {
    replay_source: Option<String>,
    legacy_lookup_mode: Option<String>,
    legacy_table: Option<String>,
    observed_sink: Option<String>,
}

fn lookup_replay_hook_from(sel: LookupReplaySelection) -> Option<RuntimeHook> {
    if sel.replay_source.as_deref() == Some("imc")
        || sel.legacy_lookup_mode.as_deref() == Some("imc")
    {
        return Some(RuntimeHook::LookupReplay(
            crate::replay::LookupTableHook::from_imc_store(crate::replay::ImcLookupStore::new()),
        ));
    }

    // A non-`imc` replay source is a lookup-table file path; the legacy
    // DEJA_LOOKUP_TABLE variable remains as the fallback selector.
    let table_path = sel.replay_source.or(sel.legacy_table)?;
    let hook = match sel.observed_sink {
        Some(observed_path) => match crate::replay::FileObservedSink::create(&observed_path) {
            Ok(sink) => crate::replay::LookupTableHook::from_source(
                crate::replay::LocalFileLookupSource::new(&table_path),
                sink,
            ),
            Err(err) => {
                eprintln!("deja: failed to open observed sink {observed_path}: {err}");
                return None;
            }
        },
        None => crate::replay::LookupTableHook::from_source(
            crate::replay::LocalFileLookupSource::new(&table_path),
            crate::replay::InMemoryObservedSink::new(),
        ),
    };
    match hook {
        Ok(h) => Some(RuntimeHook::LookupReplay(h)),
        Err(err) => {
            eprintln!("deja: failed to load lookup table {table_path}: {err}");
            None
        }
    }
}

/// Construct an `Option<RuntimeHook::LookupReplay>` from the router-prefixed
/// replay source (`ROUTER__DEJA__REPLAY__SOURCE`, either `imc` or a lookup
/// table file path) or the legacy `DEJA_LOOKUP_MODE=imc` /
/// `DEJA_LOOKUP_TABLE` selectors. Observed calls go to
/// `ROUTER__DEJA__REPLAY__OBSERVED_SINK` (or legacy `DEJA_OBSERVED_SINK`)
/// when set, else an in-memory sink.
fn lookup_replay_hook_from_env() -> Option<RuntimeHook> {
    lookup_replay_hook_from(LookupReplaySelection {
        replay_source: env_value(ROUTER_DEJA_REPLAY_SOURCE_ENV_VAR),
        legacy_lookup_mode: env_value(DEJA_LOOKUP_MODE_ENV_VAR),
        legacy_table: env_value(DEJA_LOOKUP_TABLE_ENV_VAR),
        observed_sink: env_value(ROUTER_DEJA_REPLAY_OBSERVED_SINK_ENV_VAR)
            .or_else(|| env_value(DEJA_OBSERVED_SINK_ENV_VAR)),
    })
}
```

Also update the stale comment inside `runtime_hook_from_env` (line ~1408): change "or the sandbox IMC path when ROUTER__DEJA__REPLAY__SOURCE=imc" to "or the ROUTER__DEJA__REPLAY__SOURCE selector (a lookup-table file path, or `imc`)".

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p deja-runtime 2>&1 | tail -5`
Expected: all tests pass (the whole crate, not just the new ones — the refactor must not break existing behavior).

- [ ] **Step 5: Commit**

```bash
git add crates/deja-runtime/src/lib.rs crates/deja-runtime/Cargo.toml
git commit -m "feat(runtime): path-valued ROUTER__DEJA__REPLAY__SOURCE + observed sink env"
```

---

### Task 2: deja-replay-agent — prepare/drive split, drop the lookup push surface

**Files:**
- Modify: `crates/deja-replay-agent/src/lib.rs`
- Modify: `crates/deja-replay-agent/src/main.rs`
- Modify: `crates/deja-replay-core/src/config.rs` (remove `RouterSection.lookup_admin`; fix test fixtures)

**Interfaces:**
- Consumes: `deja_replay_core::{ingest::pull_recording_source, lookup::render_lookup_table}`, `deja_orchestrator::HarnessRoot`, `deja_orchestrator::divergence::detect_and_score` (all existing, unchanged).
- Produces (Task 3's chart invokes these binary modes):
  - `deja-replay-agent prepare <config.toml>` → `pub fn prepare_from_config_path(path: &Path) -> Result<(), AgentError>` — pull recording + render whole table + reset artifact files; no router contact.
  - `deja-replay-agent drive <config.toml>` → `pub fn drive_from_config_path(path: &Path) -> Result<AgentSummary, AgentError>` — drive/score/upload/verdict against an already-prepared state dir.
  - `deja-replay-agent <config.toml>` (bare, legacy) → `run_from_config_path` = prepare then drive.
  - `pub trait SandboxClient { fn wait_healthy(&mut self, deadline: Duration) -> Result<(), AgentError>; fn drive(&mut self, request: &DriverRequest, timeout: Duration) -> Result<CandidateResponse, AgentError>; }` — `install_lookup`, `clear_lookup`, `drain_observed` are deleted.
  - `AgentConfig.router` has only `base_url` (no `lookup_admin`).

- [ ] **Step 1: Remove `lookup_admin` from the config**

In `crates/deja-replay-core/src/config.rs`:

```rust
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouterSection {
    pub base_url: String,
}
```

Delete the `lookup_admin = ...` lines from the two TOML fixtures in that file's tests (`AGENT_TOML` and `agent_config_accepts_direct_recording_uri`). Unknown TOML keys are ignored by serde's default, so old agent.toml files with `lookup_admin` still parse.

Run: `cargo test -p deja-replay-core 2>&1 | tail -5`
Expected: PASS.

- [ ] **Step 2: Rewrite the agent test to the prepare + drive shape (failing first)**

In `crates/deja-replay-agent/src/lib.rs` tests: delete `install_lookup`/`clear_lookup`/`drain_observed` from `FakeClient` (and its `installed`/`cleared` fields), delete the `lookup_admin` line from the `cfg()` TOML fixture, and replace the `loaded_recording_pushes_slices_drives_and_clears` test with:

```rust
#[test]
fn prepare_renders_the_whole_table_and_drive_replays_requests() {
    let cfg = cfg();
    let dir = tempfile::tempdir().unwrap();
    let root = HarnessRoot::new(dir.path()).unwrap();
    let events_path = root.recording_events_path(&cfg.run.recording_id);
    if let Some(parent) = events_path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let events = vec![
        event(None, "redis", 0),
        event(Some("c-1"), "http_incoming", 1),
        event(Some("c-1"), "redis", 2),
        event(Some("c-2"), "http_incoming", 3),
    ];
    let mut file = fs::File::create(&events_path).unwrap();
    for event in events {
        let record = deja::DejaRecord::BoundaryEvent(Box::new(event));
        writeln!(file, "{}", serde_json::to_string(&record).unwrap()).unwrap();
    }

    // prepare: renders the FULL table once and resets artifact files
    prepare_loaded(&cfg, &root).unwrap();
    assert!(root.lookup_table_path(&cfg.run.run_id).exists());
    assert_eq!(
        fs::read_to_string(root.observed_path(&cfg.run.run_id)).unwrap(),
        ""
    );

    // drive: no lookup traffic, just requests
    let mut client = FakeClient::new();
    let summary = run_loaded_recording_with_client(&cfg, dir.path(), &mut client).unwrap();
    assert_eq!(summary.driven, 2);
    assert_eq!(summary.skipped, 0);
    assert_eq!(client.driven, vec!["c-1", "c-2"]);
    assert!(root.scorecard_path(&cfg.run.run_id).exists());
}

#[test]
fn prepare_fails_on_an_empty_lookup_table() {
    let cfg = cfg();
    let dir = tempfile::tempdir().unwrap();
    let root = HarnessRoot::new(dir.path()).unwrap();
    let events_path = root.recording_events_path(&cfg.run.recording_id);
    fs::create_dir_all(events_path.parent().unwrap()).unwrap();
    fs::write(&events_path, "").unwrap();
    let err = prepare_loaded(&cfg, &root).unwrap_err();
    assert!(err.to_string().contains("empty"), "got: {err}");
}
```

Also fix `endpoint_parser_and_segment_encoder_handle_admin_paths`: keep the `HttpEndpoint::parse` assertions, drop nothing (it doesn't use the trait), but rename to `endpoint_parser_and_segment_encoder_parse_urls`.

Run: `cargo test -p deja-replay-agent 2>&1 | tail -10`
Expected: compile FAIL — `prepare_loaded` undefined, `FakeClient` no longer satisfies the (still-large) trait.

- [ ] **Step 3: Implement the split in `lib.rs`**

All edits in `crates/deja-replay-agent/src/lib.rs`:

**(a)** Shrink the trait — delete `install_lookup`, `clear_lookup`, `drain_observed` from `SandboxClient` and from `impl SandboxClient for HttpSandboxClient`. Delete `HttpSandboxClient`'s `lookup_admin` and `observed_admin` fields, `lookup_path`/`observed_path` methods, and the now-unused imports `deja::{ImcLookupInstallRequest, LookupTable}` (keep `ObservedCall` only if still referenced — after this task it is not; delete it and the `drain_observed`-only helpers). `from_config` becomes:

```rust
impl HttpSandboxClient {
    pub fn from_config(cfg: &AgentConfig) -> Result<Self, AgentError> {
        let router = HttpEndpoint::parse(&cfg.router.base_url)?;
        Ok(Self { router })
    }
}
```

`wait_healthy` polls the router's own health endpoint:

```rust
fn wait_healthy(&mut self, deadline: Duration) -> Result<(), AgentError> {
    let start = Instant::now();
    let path = format!("{}/health", self.router.path.trim_end_matches('/'));
    loop {
        let response = send_http(
            "GET",
            &self.router.host,
            self.router.port,
            &path,
            &[],
            None,
            Duration::from_secs(5),
        );
        if matches!(
            response,
            Ok(HttpResponse {
                status: 200..=299,
                ..
            })
        ) {
            return Ok(());
        }
        if start.elapsed() >= deadline {
            return Err(AgentError::new("router health deadline exceeded"));
        }
        std::thread::sleep(Duration::from_secs(1));
    }
}
```

**(b)** Add the prepare path (near `run_with_client`). Stage numbers 1–2 of the shared 7-step progression stay with prepare; drive owns 3–7:

```rust
/// Shared state-dir resolution: DEJA_AGENT_STATE_DIR, or a per-run tmp dir.
fn agent_state_dir(cfg: &AgentConfig) -> PathBuf {
    std::env::var("DEJA_AGENT_STATE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from("/tmp")
                .join("deja-replay-agent")
                .join(&cfg.run.run_id)
        })
}

/// Init-container entrypoint: pull the recording and render the WHOLE lookup
/// table into the shared state dir, then reset the artifact files the router
/// and the drive phase will append to. Never contacts the router.
pub fn prepare_from_config_path(path: &Path) -> Result<(), AgentError> {
    let cfg = load_agent_config(path).map_err(|e| AgentError::new(e.to_string()))?;
    let root = HarnessRoot::new(agent_state_dir(&cfg))
        .map_err(|e| AgentError::new(format!("root: {e}")))?;
    let reporter = StateReporter {
        cfg: &cfg,
        post: true,
    };
    reporter.report(
        1,
        "pulling recording",
        &cfg.run.recording_uri.as_ref().map_or_else(
            || {
                format!(
                    "recording {} from s3://{}/{}",
                    cfg.run.recording_id, cfg.s3.bucket, cfg.s3.prefix
                )
            },
            |uri| format!("recording {} from {uri}", cfg.run.recording_id),
        ),
    );
    let events_path = root.recording_events_path(&cfg.run.recording_id);
    ingest::pull_recording_source(
        &cfg.s3.to_compactor(),
        &cfg.run.recording_id,
        cfg.run.recording_uri.as_deref(),
        &events_path,
    )
    .map_err(|e| AgentError::new(format!("ingest: {e}")))?;
    prepare_loaded(&cfg, &root)
}

/// Render + write the lookup table from an already-pulled events file, and
/// reset the observed / http-diff artifacts (before the router boots, so a
/// restarting router never appends to a stale file).
fn prepare_loaded(cfg: &AgentConfig, root: &HarnessRoot) -> Result<(), AgentError> {
    let reporter = StateReporter { cfg, post: true };
    let events_path = root.recording_events_path(&cfg.run.recording_id);
    let events = load_events(&events_path)?;
    reporter.report(
        2,
        "rendering lookup table",
        &format!("{} recorded events", events.len()),
    );
    let table = lookup::render_lookup_table(&events_path, &cfg.run.recording_id, 1)
        .map_err(|e| AgentError::new(format!("lookup render: {e}")))?;
    if table.entries.is_empty() {
        return Err(AgentError::new("rendered lookup table is empty"));
    }
    write_json_file(&root.lookup_table_path(&cfg.run.run_id), &table)?;
    reset_file(&root.observed_path(&cfg.run.run_id))?;
    reset_file(&root.http_diff_path(&cfg.run.run_id))?;
    Ok(())
}

/// Main-container entrypoint: drive an already-prepared run.
pub fn drive_from_config_path(path: &Path) -> Result<AgentSummary, AgentError> {
    let cfg = load_agent_config(path).map_err(|e| AgentError::new(e.to_string()))?;
    let state_dir = agent_state_dir(&cfg);
    let mut client = HttpSandboxClient::from_config(&cfg)?;
    let root =
        HarnessRoot::new(&state_dir).map_err(|e| AgentError::new(format!("root: {e}")))?;
    run_loaded_recording_with_root(&cfg, &root, &mut client, AgentRunOptions::default())
}
```

`run_from_config_path` (legacy bare invocation) becomes prepare-then-drive:

```rust
pub fn run_from_config_path(path: &Path) -> Result<AgentSummary, AgentError> {
    prepare_from_config_path(path)?;
    drive_from_config_path(path)
}
```

`run_with_client` (kept for library callers/tests) mirrors that: replace its recording-pull + `run_loaded_recording_with_root` body with `prepare` steps then drive:

```rust
pub fn run_with_client<C: SandboxClient>(
    cfg: &AgentConfig,
    root_path: &Path,
    client: &mut C,
    options: AgentRunOptions,
) -> Result<AgentSummary, AgentError> {
    let root = HarnessRoot::new(root_path).map_err(|e| AgentError::new(format!("root: {e}")))?;
    let events_path = root.recording_events_path(&cfg.run.recording_id);
    ingest::pull_recording_source(
        &cfg.s3.to_compactor(),
        &cfg.run.recording_id,
        cfg.run.recording_uri.as_deref(),
        &events_path,
    )
    .map_err(|e| AgentError::new(format!("ingest: {e}")))?;
    prepare_loaded(cfg, &root)?;
    run_loaded_recording_with_root(cfg, &root, client, options)
}
```

**(c)** Slim the drive loop. In `run_loaded_recording_with_root`: delete the table render + `write_json_file(lookup_table_path…)` + the two `reset_file` calls (all moved to prepare), delete the ambient `install_lookup` block, the per-correlation `table_for_correlation`/`install_lookup`/`clear_lookup` calls, and change stage numbers: health wait stays step 3, driving stays 4, scoring 5, uploading 6, verdict 7 (unchanged numbering — only steps 1–2 moved to prepare). `drive_and_collect` no longer drains observed; it returns `Result<(), AgentError>`:

```rust
fn drive_and_collect<C: SandboxClient>(
    client: &mut C,
    root: &HarnessRoot,
    run_id: &str,
    driver: &DriverRequest,
    timeout: Duration,
) -> Result<(), AgentError> {
    let response = client.drive(driver, timeout)?;
    let diff = compare_response(driver, response.status, &response.body, &[]);
    append_jsonl(&root.http_diff_path(run_id), &diff)
}
```

The loop body for each correlation becomes:

```rust
prepare_driver_request(&mut driver, correlation_id);
drive_and_collect(client, root, &cfg.run.run_id, &driver, timeout)?;
driven += 1;
```

`observed_total` for the summary/stage-detail is counted from the router-written file after the loop (the router owns that file now):

```rust
let observed_total = fs::read_to_string(root.observed_path(&cfg.run.run_id))
    .map(|text| text.lines().filter(|l| !l.trim().is_empty()).count())
    .unwrap_or(0);
```

Update the module doc comment at the top of the file: the agent no longer "renders lookup entries / pushes one correlation into the router IMC"; it "drives the recorded HTTP requests against a router that loads the lookup table from the shared state dir (prepared by the `prepare` mode) and writes observed calls to the shared observed file".

**(d)** `run_loaded_recording_with_client` and `run_loaded_recording_with_options` keep their signatures (drive-only semantics — callers run `prepare_loaded` first; the test from Step 2 does exactly that).

- [ ] **Step 4: Update `main.rs` with the mode argument**

Replace `crates/deja-replay-agent/src/main.rs` content:

```rust
use std::path::PathBuf;
use std::process::ExitCode;

fn config_path(arg: Option<std::ffi::OsString>) -> PathBuf {
    arg.map(PathBuf::from)
        .or_else(|| std::env::var_os("DEJA_AGENT_CONFIG").map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("agent.toml"))
}

fn print_summary(summary: &deja_replay_agent::AgentSummary) {
    match serde_json::to_string_pretty(summary) {
        Ok(json) => println!("{json}"),
        Err(err) => eprintln!("deja-replay-agent: summary serialization failed: {err}"),
    }
}

fn main() -> ExitCode {
    let mut args = std::env::args_os().skip(1);
    let first = args.next();
    let result = match first.as_ref().and_then(|a| a.to_str()) {
        // `prepare <config>`: pull + render the lookup table, then exit
        // (init-container mode; the router boots only after this succeeds).
        Some("prepare") => {
            deja_replay_agent::prepare_from_config_path(&config_path(args.next())).map(|()| None)
        }
        // `drive <config>`: drive an already-prepared run (main container).
        Some("drive") => {
            deja_replay_agent::drive_from_config_path(&config_path(args.next())).map(Some)
        }
        // legacy: bare config path (or nothing) = prepare + drive in one process
        _ => deja_replay_agent::run_from_config_path(&config_path(first)).map(Some),
    };
    match result {
        Ok(summary) => {
            if let Some(summary) = summary {
                print_summary(&summary);
            }
            ExitCode::SUCCESS
        }
        Err(err) => {
            eprintln!("deja-replay-agent: {err}");
            ExitCode::FAILURE
        }
    }
}
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p deja-replay-agent -p deja-replay-core 2>&1 | tail -10`
Expected: PASS.

Run: `cargo build --workspace 2>&1 | tail -5`
Expected: clean build — catches any other caller of the removed trait methods / config field (e.g. `deja-tui`, `deja-kernel`; fix any compile error by deleting the dead lookup-push usage the same way).

- [ ] **Step 6: Commit**

```bash
git add crates/deja-replay-agent crates/deja-replay-core
git commit -m "feat(agent): prepare/drive split; drop IMC lookup push surface"
```

---

### Task 3: replay-sandbox chart — one replay Job (prepare init → router sidecar → agent)

**Files:**
- Modify: `replay-sandbox/chart/templates/agent.yaml` (Secret stays with edits; the Job is rewritten into the merged pod)
- Delete: `replay-sandbox/chart/templates/stack/router-deployment.yaml`
- Modify: `replay-sandbox/chart/templates/stack/router-configmap.yaml`
- Modify: `replay-sandbox/chart/values.yaml` (comment-only)
- Modify: `replay-sandbox/README.md`

**Interfaces:**
- Consumes: agent binary modes `prepare <config>` / `drive <config>` (Task 2); router env contract from Task 1 (`ROUTER__DEJA__REPLAY__SOURCE`, `ROUTER__DEJA__REPLAY__OBSERVED_SINK`).
- Produces: a single Job whose pod carries labels `app: hs-hyperswitch-server` + `app.kubernetes.io/instance: hs` so the existing `hs-hyperswitch-server` Service (`stack/router-service.yaml`, selector on those two labels) now selects the Job pod — the router's self-calls (`ROUTER__SERVER__BASE_URL: http://hs-hyperswitch-server:8080`) keep resolving.

- [ ] **Step 1: Edit the agent config Secret**

In `replay-sandbox/chart/templates/agent.yaml`, the `[router]` section of `agent.toml` becomes (agent and router now share a pod — localhost):

```toml
[router]
base_url = "http://localhost:8080"
```

(delete the `lookup_admin` line). Update the file's top comment: the agent Job now hosts the whole replay pod — prepare init container, candidate router as a native sidecar, agent as the main container.

- [ ] **Step 2: Rewrite the Job in `agent.yaml`**

Replace the Job manifest (below the Secret's `---`) with the merged pod. The router container block is **moved verbatim** from `stack/router-deployment.yaml` lines 62–356 (the whole `- name: hyperswitch-router` entry: env, envFrom, probes, ports, resources, securityContext, volumeMounts) with exactly these edits:

1. It becomes an entry under `initContainers:` (after `check-redis` and `prepare`) with one added line at its level: `restartPolicy: Always` (native sidecar — restarts on crash independently of the pod's `restartPolicy: Never`).
2. Two env vars appended to the END of its `env:` list (explicit env wins over the `hs-hyperswitch-configs` envFrom):

```yaml
            - name: ROUTER__DEJA__REPLAY__SOURCE
              value: "{{ .Values.agent.stateDir }}/lookup-tables/{{ .Values.run.id }}.jsonl"
            - name: ROUTER__DEJA__REPLAY__OBSERVED_SINK
              value: "{{ .Values.agent.stateDir }}/observed/{{ .Values.run.id }}.jsonl"
```

3. One volumeMount appended:

```yaml
            - name: agent-state
              mountPath: {{ .Values.agent.stateDir }}
```

The full Job skeleton (everything except the moved router block, which slots in where marked):

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: {{ include "replay-env.fullname" . }}-replay
  labels:
    {{- include "replay-env.labels" . | nindent 4 }}
    app.kubernetes.io/component: replay
spec:
  backoffLimit: {{ .Values.agent.backoffLimit }}
  template:
    metadata:
      labels:
        {{- include "replay-env.selectorLabels" . | nindent 8 }}
        app.kubernetes.io/component: replay
        deja.dev/run-id: {{ .Values.run.id | quote }}
        # the hs-hyperswitch-server Service selects on these two, so the
        # router's self-calls via its Service keep resolving to this pod
        app: hs-hyperswitch-server
        app.kubernetes.io/instance: hs
    spec:
      {{- include "replay-env.pullSecrets" . | nindent 6 }}
      restartPolicy: Never
      serviceAccountName: {{ .Values.router.serviceAccount.name }}
      terminationGracePeriodSeconds: 30
      initContainers:
        # 1. router's redis dependency gate (moved from router-deployment.yaml)
        - name: check-redis
          image: "{{ .Values.redis.image.repository }}:{{ .Values.redis.image.tag }}"
          imagePullPolicy: {{ .Values.redis.image.pullPolicy }}
          command: [ "/bin/sh", "-c" ]
          #language=sh
          args:
          - >
            MAX_ATTEMPTS=60;
            SLEEP_SECONDS=5;
            attempt=0;
            while ! redis-cli -h redis -p 6379 ping; do
              if [ $attempt -ge $MAX_ATTEMPTS ]; then
                echo "Redis did not become ready in time";
                exit 1;
              fi;
              attempt=$((attempt+1));
              echo "Waiting for Redis to be ready... Attempt: $attempt";
              sleep $SLEEP_SECONDS;
            done;
            echo "Redis is ready.";
        # 2. pull the recording from S3 and render the WHOLE lookup table into
        #    the shared state dir; the router only boots after this succeeds
        - name: prepare
          image: "{{ .Values.images.agent }}"
          imagePullPolicy: IfNotPresent
          args:
            - prepare
            - /etc/deja/agent.toml
          env:
            - name: DEJA_AGENT_STATE_DIR
              value: {{ .Values.agent.stateDir | quote }}
            {{- with .Values.agent.env }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          volumeMounts:
            - name: agent-config
              mountPath: /etc/deja
              readOnly: true
            - name: agent-state
              mountPath: {{ .Values.agent.stateDir }}
          resources:
            {{- toYaml .Values.agent.resources | nindent 12 }}
        # 3. the candidate router as a NATIVE SIDECAR (restartPolicy: Always;
        #    requires Kubernetes >= 1.28). Block moved from
        #    stack/router-deployment.yaml — see edits list in the plan.
        #    <<< moved hyperswitch-router container block goes here >>>
      containers:
        # drives the recorded requests against localhost, scores, uploads,
        # posts the verdict; its exit completes the Job
        - name: replay-agent
          image: "{{ .Values.images.agent }}"
          imagePullPolicy: IfNotPresent
          args:
            - drive
            - /etc/deja/agent.toml
          env:
            - name: DEJA_AGENT_STATE_DIR
              value: {{ .Values.agent.stateDir | quote }}
            {{- with .Values.agent.env }}
            {{- toYaml . | nindent 12 }}
            {{- end }}
          volumeMounts:
            - name: agent-config
              mountPath: /etc/deja
              readOnly: true
            - name: agent-state
              mountPath: {{ .Values.agent.stateDir }}
          resources:
            {{- toYaml .Values.agent.resources | nindent 12 }}
      volumes:
        - name: agent-config
          secret:
            secretName: {{ include "replay-env.fullname" . }}-agent-config
            items:
              - key: agent.toml
                path: agent.toml
        - name: agent-state
          emptyDir: {}
        - configMap:
            defaultMode: 420
            name: router-cm-hs
          name: router-config
```

Notes for the implementer:
- The moved router block keeps its `router-config` volumeMount (`/local/config/sandbox.toml`, subPath `router.toml`) — hence the `router-config` volume above.
- Keep the router's liveness/readiness probes as-is; readiness gates Service endpoints, liveness restarts a wedged router.
- Drop the router Deployment's `check-redis` duplication — it's the shared pod's init container now (step 1 above).
- The pod uses the router's ServiceAccount (IRSA for KMS when `kms.enabled`); the agent doesn't need one (S3 creds come from agent.toml).

- [ ] **Step 3: Delete the router Deployment and clean the configmap**

```bash
git rm replay-sandbox/chart/templates/stack/router-deployment.yaml
```

In `replay-sandbox/chart/templates/stack/router-configmap.yaml`, replace lines 56–60:

```yaml
  # The replay sandbox always runs router in replay mode. The lookup table
  # file path and observed sink are per-run env on the router container in
  # the replay Job (they embed the run id and shared state dir).
  ROUTER__DEJA__MODE: "replay"
```

(i.e. delete `ROUTER__DEJA__REPLAY__SOURCE: "imc"` and `ROUTER__DEJA__REPLAY__LOOKUP_DIR: "/tmp/deja-replay/lookup"`.)

- [ ] **Step 4: values.yaml + README**

`replay-sandbox/chart/values.yaml`: update the header comment ("replay agent Job" → "replay Job: prepare init container + candidate router sidecar + agent") and the `agent:` section comment: `stateDir` is now the shared `emptyDir` the router reads the lookup table from and writes observed calls to. No key changes.

`replay-sandbox/README.md`: rewrite the agent-flow paragraphs: prepare init container builds the whole lookup table to the shared state dir → router (native sidecar, k8s ≥ 1.28 required) boots with `ROUTER__DEJA__REPLAY__SOURCE` / `ROUTER__DEJA__REPLAY__OBSERVED_SINK` file paths → agent drives requests only (no `/deja/lookup` admin traffic). Mention that `helm --wait` no longer gates on router readiness (the router is inside the Job pod); a never-healthy router surfaces as the agent's health-deadline failure.

- [ ] **Step 5: Render-verify the chart**

Run:

```bash
helm template replay replay-sandbox/chart \
  --set run.id=run-test,run.recordingId=rec-test,s3.bucket=b,s3.accessKey=a,s3.secretKey=s,callback.baseUrl=http://cb,callback.token=t \
  > /tmp/replay-rendered.yaml && \
grep -c "kind: Deployment" /tmp/replay-rendered.yaml; \
grep -A2 "ROUTER__DEJA__REPLAY__SOURCE" /tmp/replay-rendered.yaml; \
grep "restartPolicy: Always" /tmp/replay-rendered.yaml
```

Expected: Deployment count excludes the router (postgres/redis/superposition remain); `ROUTER__DEJA__REPLAY__SOURCE` renders as `/tmp/deja-replay-agent/lookup-tables/run-test.jsonl`; exactly one `restartPolicy: Always` (the router sidecar). Also confirm no template errors and `imc` appears nowhere: `grep -c '"imc"' /tmp/replay-rendered.yaml` → 0.

- [ ] **Step 6: Commit**

```bash
git add replay-sandbox
git commit -m "feat(replay-sandbox): file-based lookup — single replay Job with router sidecar"
```

---

### Task 4: Whole-workspace verification + spec cross-check

**Files:**
- No new files; fixes only if verification fails.

**Interfaces:**
- Consumes: everything above.
- Produces: green workspace; confirmation the local demo path needs no change.

- [ ] **Step 1: Full test suite**

Run: `cargo test --workspace 2>&1 | tail -15`
Expected: PASS. Pay attention to `deja-tui`, `deja-kernel`, and `crates/deja` integration tests (`v2_regression`, `fail_stop`, `replay_integration`) — they reference lookup machinery and must still compile/pass; the IMC store itself is untouched, only the agent's push surface is gone.

- [ ] **Step 2: Confirm the local demo contract**

Run: `grep -n "REPLAY__SOURCE\|OBSERVED_SINK" demo/overlays/hyperswitch/docker-compose.deja.yml`
Expected (unchanged, now honored by the Task 1 runtime change when the candidate builds against these crates):

```
ROUTER__DEJA__REPLAY__SOURCE: /harness-state/lookup-tables/${RUN_ID:-demo}.jsonl
ROUTER__DEJA__REPLAY__OBSERVED_SINK: /harness-state/observed/${RUN_ID:-demo}.jsonl
```

No demo code change. (End-to-end sandbox verification — a real run against a cluster — happens outside this plan; note it in the PR description.)

- [ ] **Step 3: Commit any fixes and finish**

```bash
git status --short   # should be clean apart from intentional fixes
```

If fixes were needed: `git add -A && git commit -m "fix: post-integration fixes for file-based lookup replay"`.
