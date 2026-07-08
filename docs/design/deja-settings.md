# Deja Typed Settings — Design and Implementation Record

Status: **IMPLEMENTED** (vendor commit `5dc8c73cc5` on deja-lean-main, root commit `4995fe64`), with adversarial-review fixes in flight (replay must abort on misconfiguration rather than boot live; sampler's superposition-disabled branch must honor `fail_closed`). This file records the settings contract that implementation follows.

The contract is: Deja runtime behavior is configured through typed router settings, not direct `DEJA_*` process-env parsing. Runtime code reads only `ROUTER__DEJA__*` for Deja-owned settings.

---

## 1. Status and scope

### In scope

- Add typed `deja: DejaSettings` to the top-level `Settings` object.
- Make `Settings.deja.mode` the single source of truth for disabled/record/replay behavior.
- Install the Deja hook eagerly in `deja_boot` from `Settings`, eliminating the current three-layer `DEJA_MODE` parse.
- Move the correlation-layer gate currently around `setup.rs:149` to the typed runtime mode instead of a fresh env check.
- Delete the `DEJA_KAFKA_TOPIC` fallback; Kafka topic is a normal typed setting.
- Unify run-id spelling on `run_id`.
- Make the Superposition sampler fail closed and seed `[default-configs]` with Deja disabled.
- Use the `POD_NAME` and `VERGEN_GIT_SHA` idioms for producer identity.
- Document that graph artifact routing is W4-owned/tape-candidate work and remains outside W3 typed settings and log config.

### Out of scope

- No implementation in this W3 design-note task.
- No edits to live config files in this W3 design-note task.
- No vendor-tree reads or edits in this W3 design-note task.
- No compatibility layer that keeps direct `DEJA_*` env vars as supported runtime inputs.
- No changes to boundary macro semantics, replay scoring, event schema, or recording pipeline behavior except where they consume typed settings.

---

## 2. Goals and non-goals

### Goals

1. **One mode owner.** `Settings.deja.mode` decides Deja behavior everywhere.
2. **Normal router config.** TOML owns defaults; deployment overlays use `ROUTER__DEJA__...` for Deja-owned settings.
3. **Eager install.** Bootstrap installs exactly one hook: disabled, record, or replay. Request code observes installed state; it does not parse env.
4. **Safe default posture.** Defaults are disabled. Sampler failures mean not recorded. Missing Kafka settings mean recording is disabled loudly, not inferred.
5. **Clean cutover.** Each old env var is mapped to a typed overlay, moved to another config owner, or deleted.
6. **Reviewable surface.** Structs, TOML, env overlays, bootstrap order, and tests are explicit enough for implementation review.

### Non-goals

- Do not preserve `DEJA_MODE` as a second control plane.
- Do not auto-record because an artifact directory or graph directory is present.
- Do not infer Kafka topic names from deploy environment.
- Do not treat Superposition outage, timeout, missing key, or malformed value as permission to record.
- Do not keep both `recording_run_id` and `run_id` as user-facing spellings.

---

## 3. Typed settings structs

The implementation should follow the existing router settings derive/default conventions. The shapes below are the proposed semantic contract.

Feature-off purity is a blocker-class invariant: the top-level `Settings` shape must be byte-identical to the current default build when `feature = "deja"` is absent. `DejaSettings` is plain config data and can live without depending on the Deja runtime crate, but the `Settings.deja` field itself is gated.

```rust
#[derive(Clone, Debug, Deserialize)]
pub struct Settings {
    // existing fields...
    pub log: Log,

    #[cfg(feature = "deja")]
    #[serde(default)]
    pub deja: DejaSettings,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct DejaSettings {
    /// Single runtime-mode source of truth.
    pub mode: DejaMode,
    /// Canonical run identity. Replaces DEJA_RECORDING_RUN_ID and DEJA_RUN_ID.
    /// If absent in Record mode, deja_boot mints one once and stores it in the hook.
    pub run_id: Option<String>,

    /// Record-mode sink, capture, and run identity settings.
    pub recording: DejaRecordingSettings,

    /// Replay-mode lookup/source settings.
    pub replay: DejaReplaySettings,

    /// Per-request record sampler. Consulted only in Record mode.
    pub sampler: DejaSamplerSettings,

    /// Process/code identity stamped into envelopes and events.
    pub identity: DejaIdentitySettings,

    /// Writer batching/backpressure knobs.
    pub writer: DejaWriterSettings,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DejaMode {
    Disabled,
    Record,
    Replay,
}

impl Default for DejaMode {
    fn default() -> Self { Self::Disabled }
}

impl Default for DejaSettings {
    fn default() -> Self {
        Self {
            mode: DejaMode::Disabled,
            run_id: None,
            recording: DejaRecordingSettings::default(),
            replay: DejaReplaySettings::default(),
            sampler: DejaSamplerSettings::default(),
            identity: DejaIdentitySettings::default(),
            writer: DejaWriterSettings::default(),
        }
    }
}
```

### 3.1 Recording settings

```rust
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct DejaRecordingSettings {

    /// Required for Record mode. No DEJA_KAFKA_TOPIC fallback.
    pub kafka: DejaKafkaSettings,

    /// Session for local discrete captures; continuous for production-shaped windows.
    pub capture: DejaCaptureSettings,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct DejaKafkaSettings {
    /// Empty means not configured. Record boot with empty topic disables recording loudly.
    pub topic: String,

    /// Empty means use an existing shared Kafka client setting if the router already has one;
    /// if not available, Record boot disables recording loudly.
    pub brokers: Vec<String>,

    pub client_id: Option<String>,
    pub acks: String,                 // default "all"
    pub enable_idempotence: bool,     // default true
    pub compression: String,          // default "zstd"
    pub linger_ms: u64,               // default 20
    pub message_timeout_ms: u64,      // default 30_000
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct DejaCaptureSettings {
    pub mode: DejaCaptureMode,        // default Continuous
    pub service: String,              // default "hyperswitch-router"
    pub environment: String,          // default "local" unless router env supplies one
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DejaCaptureMode {
    Session,
    Continuous,
}
```

Default values:

```rust
impl Default for DejaRecordingSettings {
    fn default() -> Self {
        Self {
            kafka: DejaKafkaSettings::default(),
            capture: DejaCaptureSettings::default(),
        }
    }
}

impl Default for DejaKafkaSettings {
    fn default() -> Self {
        Self {
            topic: String::new(),
            brokers: Vec::new(),
            client_id: None,
            acks: "all".into(),
            enable_idempotence: true,
            compression: "zstd".into(),
            linger_ms: 20,
            message_timeout_ms: 30_000,
        }
    }
}

impl Default for DejaCaptureSettings {
    fn default() -> Self {
        Self {
            mode: DejaCaptureMode::Continuous,
            service: "hyperswitch-router".into(),
            environment: "local".into(),
        }
    }
}
```

### 3.2 Replay settings

```rust
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct DejaReplaySettings {
    /// Required when mode == Replay. Can be a recording id, window id, session id, or URI.
    pub source: Option<String>,

    /// Optional path/URI to pre-rendered lookup artifacts for local replay.
    pub lookup_dir: Option<String>,

    /// Optional replay observed-call sink. Replaces DEJA_OBSERVED_SINK.
    pub observed_sink: Option<String>,

    /// Substitute misses fail the request; they never fall through to live code.
    pub fail_stop_on_miss: bool,      // default true
}

impl Default for DejaReplaySettings {
    fn default() -> Self {
        Self { source: None, lookup_dir: None, observed_sink: None, fail_stop_on_miss: true }
    }
}
```

`mode = "replay"` without `replay.source` or `replay.lookup_dir` is a configuration error for replay runners. It is not a reason to run live boundaries.

### 3.3 Sampler settings

```rust
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct DejaSamplerSettings {
    /// Enables Superposition-backed per-request sampling. Default false.
    pub enabled: bool,

    /// Boolean Superposition key controlling recording.
    pub record_key: String,           // default "deja_record"


    /// Lookup timeout. Timeout means false.
    pub timeout_ms: u64,              // default 25

    /// Safety invariant: missing/unavailable/invalid sampler config means false.
    pub fail_closed: bool,            // default true; implementation must not allow prod false
}

impl Default for DejaSamplerSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            record_key: "deja_record".into(),
            timeout_ms: 25,
            fail_closed: true,
        }
    }
}
```

Sampler decision table:

| Condition | Result |
|---|---|
| `mode != Record` | Do not record; sampler not consulted. |
| `mode == Record`, `sampler.enabled == false` | Do not record unless an implementation explicitly supports a separate local-only always-record mode; this design does not require one. |
| Superposition returns boolean `true` for `record_key` | Record request. |
| Superposition returns boolean `false` | Do not record. |
| Key missing, type mismatch, timeout, datasource error, invalid backup seed | Do not record and emit a bounded warning/metric. |

### 3.4 Identity settings

```rust
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct DejaIdentitySettings {
    /// Kubernetes Downward API convention for process identity.
    pub pod_name_env: String,         // default "POD_NAME"

    /// Vergen convention for code provenance.
    pub git_sha_env: String,          // default "VERGEN_GIT_SHA"

    /// Optional explicit local/demo overrides.
    pub instance_id: Option<String>,
    pub code_sha: Option<String>,
    pub service_version: Option<String>,
}

impl Default for DejaIdentitySettings {
    fn default() -> Self {
        Self {
            pod_name_env: "POD_NAME".into(),
            git_sha_env: "VERGEN_GIT_SHA".into(),
            instance_id: None,
            code_sha: None,
            service_version: None,
        }
    }
}
```

Resolution order:

- `instance_id`: `identity.instance_id` → env var named by `identity.pod_name_env` (`POD_NAME`) → generated `pi-{hostname}-{pid}-{boot_ns}` fallback.
- `code_sha`: `identity.code_sha` → env var named by `identity.git_sha_env` (`VERGEN_GIT_SHA`) → compile-time `option_env!("VERGEN_GIT_SHA")` if already available → `"unknown"`.

The fallback is allowed for local/dev, but production deploys should provide `POD_NAME` and `VERGEN_GIT_SHA` through normal deployment mechanisms.

### 3.5 Writer settings

```rust
#[derive(Clone, Debug, Deserialize)]
#[serde(default)]
pub struct DejaWriterSettings {
    pub queue_capacity: usize,        // default 8192
    pub batch_size: usize,            // default 500
    pub flush_after_records: usize,   // default 500
    pub flush_interval_ms: u64,       // default 1000
    pub shutdown_flush_ms: u64,       // default 5000
}

impl Default for DejaWriterSettings {
    fn default() -> Self {
        Self {
            queue_capacity: 8192,
            batch_size: 500,
            flush_after_records: 500,
            flush_interval_ms: 1000,
            shutdown_flush_ms: 5000,
        }
    }
}
```

Runtime backpressure semantics are fixed, not configurable: request threads must never block on the Deja writer. If the in-process queue is full, Kafka is unavailable, broker acks time out, or the writer cannot accept a record for any operational reason, the runtime drops the Deja record and increments bounded loss counters/metrics. Boot-time invalid sink configuration still installs disabled/no-op with a configuration error; that boot behavior does not authorize blocking payment traffic after a valid writer is installed.

### 3.6 Graph artifact routing boundary

Graph artifact routing is W4-owned/tape-candidate work. W3 does not introduce a typed Deja setting or log config for graph output, and it does not migrate the existing graph layer's direct `DEJA_GRAPH_DIR` env path.

---

## 4. TOML keys and defaults across the four config surfaces

The implementation pass should add the same Deja schema to the Deja-enabled variants of the four router config surfaces Fable called out: `config/config.example.toml`, `config/development.toml`, `config/docker_compose.toml`, and `config/deployments/env_specific.toml`. Feature-off config loading must remain byte-identical: either those `[deja]` tables are not present in the default-build files loaded without `feature = "deja"`, or the implementation proves the existing loader ignores unknown `[deja]` tables without changing the deserialized default-build `Settings` bytes. The examples below are the design contract for the Deja-enabled surfaces; TOML may omit optional empty-string keys if that is the existing settings convention.

### 4.1 `config/config.example.toml` — safe base/default router TOML

Base defaults are safe and inert.

```toml
[deja]
mode = "disabled"
# Empty means boot mints a run id only if mode = "record" and recording is otherwise configured.
# Prefer setting this through ROUTER__DEJA__RUN_ID for one-off local sessions.
run_id = ""
[deja.recording.kafka]
# No default topic and no DEJA_KAFKA_TOPIC fallback.
topic = ""
brokers = []
client_id = ""
acks = "all"
enable_idempotence = true
compression = "zstd"
linger_ms = 20
message_timeout_ms = 30000

[deja.recording.capture]
mode = "continuous"
service = "hyperswitch-router"
environment = "local"

[deja.replay]
source = ""
lookup_dir = ""
observed_sink = ""
fail_stop_on_miss = true

[deja.sampler]
enabled = false
record_key = "deja_record"
timeout_ms = 25
fail_closed = true

[deja.identity]
pod_name_env = "POD_NAME"
git_sha_env = "VERGEN_GIT_SHA"
instance_id = ""
code_sha = ""
service_version = ""

[deja.writer]
queue_capacity = 8192
batch_size = 500
flush_after_records = 500
flush_interval_ms = 1000
shutdown_flush_ms = 5000

```

Implementation detail: empty strings in TOML should deserialize as `None` for optional fields if that is already the router convention; otherwise the implementation should use actual optional TOML omission. The semantic default is what matters: unset means absent.

### 4.2 `config/development.toml` — local developer defaults

Local config may include localhost Kafka/Superposition wiring, but checked-in defaults remain disabled. A developer opts into a record session with `ROUTER__DEJA__MODE=record` plus a run id.

```toml
[deja]
mode = "disabled"
run_id = ""

[deja.recording.kafka]
topic = "deja.recordings.local"
brokers = ["localhost:9092"]
client_id = "deja-router-local"

[deja.recording.capture]
mode = "session"
service = "hyperswitch-router"
environment = "local"

[deja.sampler]
enabled = true
record_key = "deja_record"
timeout_ms = 25
fail_closed = true

```

A developer who wants to record locally uses overlays such as:

```sh
ROUTER__DEJA__MODE=record
ROUTER__DEJA__RUN_ID=run-local-20260707
```

### 4.3 `config/docker_compose.toml` — compose/demo wiring

Compose/demo config may wire concrete local services while still requiring explicit mode selection. It should not rely on `DEJA_MODE` or `DEJA_KAFKA_TOPIC`.

```toml
[deja]
mode = "record"

[deja.recording.kafka]
topic = "deja.recordings.sandbox"
brokers = ["kafka-sandbox-0:9092", "kafka-sandbox-1:9092"]
client_id = "hyperswitch-router-deja-sandbox"
acks = "all"
enable_idempotence = true
compression = "zstd"
linger_ms = 20
message_timeout_ms = 30000

[deja.recording.capture]
mode = "continuous"
service = "hyperswitch-router"
environment = "sandbox"

[deja.sampler]
enabled = true
record_key = "deja_record"
timeout_ms = 25
fail_closed = true

[deja.identity]
pod_name_env = "POD_NAME"
git_sha_env = "VERGEN_GIT_SHA"
```

### 4.4 `config/deployments/env_specific.toml` — deployment overlay template

Deployment config must be explicit and fail closed. If an environment is not approved for Deja capture, keep `mode = "disabled"` even if Kafka keys are present.

```toml
[deja]
mode = "disabled" # change to "record" only with separate deployment approval

[deja.recording.kafka]
topic = "deja.recordings.production"
brokers = ["kafka-prod-0:9092", "kafka-prod-1:9092", "kafka-prod-2:9092"]
client_id = "hyperswitch-router-deja-production"
acks = "all"
enable_idempotence = true
compression = "zstd"
linger_ms = 20
message_timeout_ms = 30000

[deja.recording.capture]
mode = "continuous"
service = "hyperswitch-router"
environment = "production"

[deja.sampler]
enabled = true
record_key = "deja_record"
timeout_ms = 25
fail_closed = true

[deja.identity]
pod_name_env = "POD_NAME"
git_sha_env = "VERGEN_GIT_SHA"
```

---

## 5. Overlay environment spelling

Canonical overlay variables use the router settings overlay convention:

| Setting | Overlay env |
|---|---|
| `deja.mode` | `ROUTER__DEJA__MODE` |
| `deja.run_id` | `ROUTER__DEJA__RUN_ID` |
| `deja.recording.kafka.topic` | `ROUTER__DEJA__RECORDING__KAFKA__TOPIC` |
| `deja.recording.kafka.brokers` | `ROUTER__DEJA__RECORDING__KAFKA__BROKERS` |
| `deja.recording.kafka.client_id` | `ROUTER__DEJA__RECORDING__KAFKA__CLIENT_ID` |
| `deja.recording.capture.mode` | `ROUTER__DEJA__RECORDING__CAPTURE__MODE` |
| `deja.recording.capture.environment` | `ROUTER__DEJA__RECORDING__CAPTURE__ENVIRONMENT` |
| `deja.replay.source` | `ROUTER__DEJA__REPLAY__SOURCE` |
| `deja.replay.lookup_dir` | `ROUTER__DEJA__REPLAY__LOOKUP_DIR` |
| `deja.replay.observed_sink` | `ROUTER__DEJA__REPLAY__OBSERVED_SINK` |
| `deja.sampler.enabled` | `ROUTER__DEJA__SAMPLER__ENABLED` |
| `deja.sampler.record_key` | `ROUTER__DEJA__SAMPLER__RECORD_KEY` |
| `deja.identity.instance_id` | `ROUTER__DEJA__IDENTITY__INSTANCE_ID` |
| `deja.identity.code_sha` | `ROUTER__DEJA__IDENTITY__CODE_SHA` |
| `deja.writer.queue_capacity` | `ROUTER__DEJA__WRITER__QUEUE_CAPACITY` |
| `deja.writer.batch_size` | `ROUTER__DEJA__WRITER__BATCH_SIZE` |
| `deja.writer.flush_after_records` | `ROUTER__DEJA__WRITER__FLUSH_AFTER_RECORDS` |
| `deja.writer.flush_interval_ms` | `ROUTER__DEJA__WRITER__FLUSH_INTERVAL_MS` |

Examples:

```sh
# Local record session
ROUTER__DEJA__MODE=record
ROUTER__DEJA__RUN_ID=run-local-20260707
ROUTER__DEJA__RECORDING__KAFKA__TOPIC=deja.recordings.local
ROUTER__DEJA__RECORDING__KAFKA__BROKERS=localhost:9092
ROUTER__DEJA__SAMPLER__ENABLED=true

# Replay runner
ROUTER__DEJA__MODE=replay
ROUTER__DEJA__REPLAY__SOURCE=rec-20260707T120000Z
ROUTER__DEJA__REPLAY__LOOKUP_DIR=/var/lib/deja/lookup/rec-20260707T120000Z
```

Direct `DEJA_*` variables are not read for W3-owned runtime-mode, Kafka, replay, writer, sampler, or identity settings. The legacy graph-layer `DEJA_GRAPH_DIR` path is explicitly outside W3 and stays unmigrated until W4.

---

## 6. Bootstrap and install order

The order is part of the design contract because it removes the three independent env parses.

1. **Load `Settings`.** TOML defaults and router overlay envs are resolved once by the existing settings loader.
2. **Normalize typed Deja settings.** Empty optional strings become `None`; `DejaMode` is parsed once; defaults are applied.
3. **Initialize logging.** Existing logging initializes independently; W3 does not introduce graph artifact routing.
4. **Resolve identity.** `POD_NAME` and `VERGEN_GIT_SHA` are read through `DejaIdentitySettings` once at boot; resolved values are stored in the hook config.
5. **Install Deja hook eagerly in `deja_boot`.** There is exactly one install path:
   - `Disabled`: install disabled/no-op hook.
   - `Record`: validate Kafka topic/brokers, build sampler, build writer/sink, install recording hook. If required record config is missing, install disabled/no-op hook and log a clear configuration error; do not partially record.
   - `Replay`: validate replay source/lookup inputs, install replay hook. Substitute misses remain fail-stop.
6. **Build app state and middleware.** The correlation/request-id layer uses `settings.deja.mode.is_active()` or equivalent typed runtime state. It does not read `DEJA_MODE`.
7. **Serve requests.** Request path checks only the installed hook and sampler result. It does not parse env and does not mutate global mode.
8. **Shutdown.** The installed hook owns writer flush timeout from `deja.writer.shutdown_flush_ms`.

Pseudo-code:

```rust
pub fn boot(settings: Settings) -> RouterState {
    init_logging(&settings.log);

    let deja_runtime = deja_boot::install(&settings.deja, &settings.log);

    let correlation_enabled = settings.deja.mode != DejaMode::Disabled;
    let app = build_router(AppConfig {
        settings,
        deja_runtime,
        correlation_enabled,
    });

    app
}
```

`deja_runtime::runtime_hook_from_env()` should either disappear or become a test-only/internal helper that takes typed settings. It must not remain a production path for W3-owned runtime mode by reading `DEJA_MODE`; the legacy graph-layer `DEJA_GRAPH_DIR` path is a separate W4-owned exception.

---

## 7. Existing env-var cutover table

| Existing env var | Status | New owner / spelling | Notes |
|---|---:|---|---|
| `DEJA_MODE` | **Move** | `ROUTER__DEJA__MODE` | Single source of truth. Accepted values: `disabled`, `record`, `replay`. Do not keep `off`/`none` aliases. |
| `DEJA_RECORDING_RUN_ID` | **Move/rename** | `ROUTER__DEJA__RUN_ID` | Canonical name is `run_id`. |
| `DEJA_RUN_ID` | **Move/rename** | `ROUTER__DEJA__RUN_ID` | Both old names map to the same typed key; runtime code reads only `deja.run_id`. |
| `DEJA_KAFKA_TOPIC` | **Delete** | `ROUTER__DEJA__RECORDING__KAFKA__TOPIC` | No fallback from direct env. Empty topic means record mode is not configured. |
| `DEJA_KAFKA_BROKERS` / equivalent ad hoc broker env | **Move** | `ROUTER__DEJA__RECORDING__KAFKA__BROKERS` | Use existing list parsing convention. |
| `DEJA_CODE_REF` | **Delete/move to identity override only** | Prefer `VERGEN_GIT_SHA`; local override is `ROUTER__DEJA__IDENTITY__CODE_SHA` | Production provenance should use the standard Vergen build env, not a Deja-specific alias. |
| `DEJA_LOOKUP_TABLE` | **Move** | `ROUTER__DEJA__REPLAY__LOOKUP_DIR` | Router replay gets lookup artifacts from typed replay settings. Router runtime does not read the direct env var. |
| `DEJA_OBSERVED_SINK` | **Move** | `ROUTER__DEJA__REPLAY__OBSERVED_SINK` | Replay observed-call output is replay config, not global process env. |
| `DEJA_BATCH_SIZE` | **Move** | `ROUTER__DEJA__WRITER__BATCH_SIZE` | Writer batching knob. |
| `DEJA_FLUSH_INTERVAL_MS` | **Move** | `ROUTER__DEJA__WRITER__FLUSH_INTERVAL_MS` | Writer timer knob. |
| `DEJA_QUEUE_CAPACITY` | **Move** | `ROUTER__DEJA__WRITER__QUEUE_CAPACITY` | Writer channel/backpressure knob. |
| `DEJA_FLUSH_AFTER_RECORDS` | **Move** | `ROUTER__DEJA__WRITER__FLUSH_AFTER_RECORDS` | Keep distinct from `batch_size` if current runtime has both thresholds; otherwise implementation maps the typed setting to the single writer threshold. |
| `DEJA_SINK_POLICY` | **Delete** | none | Remove fail-open/block policy axis from router config. Boot-time invalid sink config installs disabled/no-op with a configuration error; runtime queue-full/broker-outage behavior is fixed: never block request threads, drop the record, and count loss. |
| `DEJA_ARTIFACT_DIR` | **Move if still needed** | `ROUTER__DEJA__REPLAY__LOOKUP_DIR` or future artifact-store config | Presence must not auto-enable record mode. |
| `DEJA_GRAPH_DIR` | **Leave for W4** | pre-W3 direct graph env path until W4/tape-candidate work | Graph artifact routing is W4-owned; W3 adds no typed Deja setting and no log-config surface. |
| `POD_NAME` | **Keep as identity idiom** | Named by `deja.identity.pod_name_env` | Still read as a process env because it is Kubernetes-provided identity, not a Deja control variable. |
| `VERGEN_GIT_SHA` | **Keep as identity idiom** | Named by `deja.identity.git_sha_env` | Still read as process/build env for provenance. |
| Superposition datasource/backup envs | **Keep under existing Superposition owner** | Existing `superposition.backup_file_path` / datasource config | Deja sampler must not duplicate Superposition global connection or backup-seed config. |

Cutover rule: deployment manifests/config generators emit only `ROUTER__DEJA__*` for Deja-owned settings. Runtime code does not dual-read old and new names.

---

## 8. Sampler fail-closed and Superposition `[default-configs]` seed

The sampler is a permission check. It must fail closed.

Required Superposition default seed entry:

```toml
[default-configs.deja_record]
value = false
schema = { type = "boolean" }
description = "Whether this request should be recorded by Deja. Defaults false so sampler failures or missing overrides do not record traffic."
change_reason = "Seed Deja sampler fail-closed default"
```

Optional future sampling keys can be added later, but the first cut should stay boolean:

```toml
# Optional future extension, not required for W3 implementation.
[default-configs.deja_sample_rate]
value = 0
schema = { type = "integer", minimum = 0, maximum = 1000000 }
description = "Parts-per-million Deja sample rate; 0 records nothing by default."
change_reason = "Future Deja sampler extension"
```

Runtime sampler behavior:

```rust
fn should_record(settings: &DejaSettings, ctx: &RequestContext) -> bool {
    if settings.mode != DejaMode::Record {
        return false;
    }
    if !settings.sampler.enabled {
        return false;
    }
    match superposition.bool_value(&settings.sampler.record_key, ctx, settings.sampler.timeout_ms) {
        Ok(true) => true,
        Ok(false) => false,
        Err(_) => false, // fail closed
    }
}
```

The sampler must not throw request-serving errors. It should emit bounded observability for unavailable/malformed config, but the returned decision is false.

---

## 9. Identity, run-id, and graph artifact boundary

### Run id

- Canonical user-facing spelling: `run_id`.
- TOML key: `deja.run_id`.
- Overlay env: `ROUTER__DEJA__RUN_ID`.
- Envelope/event field is `run_id` outright. Do not expose `recording_run_id` as a config or event spelling.
- If unset in record mode, `deja_boot` mints one run id once. All events from that process use the installed value. Request code does not generate run ids.

### Producer identity

- `POD_NAME` is the preferred production instance id source through `deja.identity.pod_name_env`.
- If `POD_NAME` is absent, local/dev may generate `pi-{hostname}-{pid}-{boot_ns}`.
- `VERGEN_GIT_SHA` is the preferred code-sha source through `deja.identity.git_sha_env`.
- Explicit `deja.identity.instance_id` and `deja.identity.code_sha` exist for local demos and deterministic tests.

Example resolved envelope metadata:

```json
{
  "run_id": "run-0199f6c4d8e97c22a17b",
  "producer": {
    "service": "hyperswitch-router",
    "environment": "sandbox",
    "instance_id": "router-7f9c4c8d6f-2lq9p",
    "code_sha": "9355950b99d4f2c7e...",
    "service_version": "2026.07.07.0"
  }
}
```

### Graph artifact routing

Graph artifact routing is W4-owned/tape-candidate work. W3 does not introduce a typed Deja setting or log-config key for graph output; any existing graph layer stays on its pre-W3 direct `DEJA_GRAPH_DIR` env path until W4 deletes or replaces it.

---

## 10. Test plan for the implementation pass

This design-note task does not run tests. The implementation pass should add focused tests around settings behavior and bootstrap routing.

### Settings/default tests

- With `feature = "deja"`, deserializing an empty/base config yields `Settings.deja.mode == Disabled`.
- Without `feature = "deja"`, `Settings` has no `deja` field and default-build config deserialization remains byte-identical to the pre-W3 shape.
- The feature-off test uses the real default-build config files. If those files contain `[deja]`, the test must prove the loader ignores unknown Deja tables and serializes/deserializes the same `Settings` bytes as before W3; preferred implementation is to keep `[deja]` only in Deja-enabled overlays.
- Empty optional TOML values normalize consistently with existing settings conventions.
- W3 adds no graph artifact routing config; that remains W4-owned/tape-candidate work.

### Overlay-env tests

- `ROUTER__DEJA__MODE=record` produces `DejaMode::Record`.
- `ROUTER__DEJA__RECORDING__KAFKA__TOPIC=...` populates the Kafka topic.
- `ROUTER__DEJA__RUN_ID=...` populates `run_id`.
- Direct `DEJA_MODE`, `DEJA_RUN_ID`, `DEJA_RECORDING_RUN_ID`, and `DEJA_KAFKA_TOPIC` do not affect settings.

### Bootstrap tests

- Disabled mode installs the disabled/no-op hook and does not build Kafka or sampler resources.
- Record mode with valid Kafka settings installs a recording hook using typed topic/run id/identity.
- Record mode with empty Kafka topic installs disabled/no-op hook and emits a configuration error; it does not fall back to `DEJA_KAFKA_TOPIC`.
- Replay mode with missing replay source/lookup fails configuration for replay runners and never falls through to live boundary execution.
- The correlation-layer gate uses typed mode state; changing `DEJA_MODE` after settings load has no effect.

### Writer/backpressure tests

- Full writer queue on a request path drops the Deja record, increments loss metrics/counters, and returns without blocking the request.
- Kafka/broker outage after successful boot drops records and counts loss; it does not park payment request tasks waiting for sink recovery.
- Boot-time invalid sink configuration still installs disabled/no-op with a configuration error and is tested separately from runtime backpressure.

### Sampler tests

- `mode != Record` never records, even if Superposition returns true.
- `sampler.enabled = false` never records.
- Superposition boolean `true` records and boolean `false` does not record.
- Missing key, timeout, datasource error, type mismatch, and invalid backup seed all return false.
- The seeded `[default-configs.deja_record].value = false` parses as false through the configured backup-file path.

### Identity and metadata tests

- Explicit `identity.instance_id`/`code_sha` override env-derived values.
- `POD_NAME` populates instance id when no explicit override is set.
- `VERGEN_GIT_SHA` populates code sha when no explicit override is set.
- Missing identity envs use generated local fallback and `unknown` code sha without panicking.
- Installed hook metadata uses `run_id` consistently.

### Cutover tests

- A cutover test fixture documents old env vars and their typed replacements.
- `DEJA_KAFKA_TOPIC` is not read by record boot.
- Direct `DEJA_CODE_REF`, `DEJA_LOOKUP_TABLE`, `DEJA_OBSERVED_SINK`, `DEJA_BATCH_SIZE`, `DEJA_FLUSH_INTERVAL_MS`, `DEJA_QUEUE_CAPACITY`, `DEJA_FLUSH_AFTER_RECORDS`, and `DEJA_SINK_POLICY` do not affect router settings.
- Graph artifact routing remains outside W3 settings tests and belongs to W4/tape-candidate work.

---

## 11. Review checklist

- `Settings.deja.mode` is the only runtime-mode source.
- `Settings.deja` is `#[cfg(feature = "deja")]`; feature-off `Settings` has no Deja field.
- `deja_boot` installs the hook eagerly from typed settings.
- The `setup.rs:149`-style correlation gate is moved to typed runtime mode.
- No production path uses a `runtime_hook_from_env()`-style parser for W3-owned runtime-mode settings; the legacy graph layer is the only direct-env exception until W4.
- `DEJA_KAFKA_TOPIC` fallback is deleted.
- User-facing run-id spelling is `run_id`.
- Superposition default seed contains `[default-configs.deja_record] value = false`.
- Sampler failures return false.
- Writer backpressure/outage never blocks request threads; runtime drops Deja records and counts loss.
- Identity follows `POD_NAME` and `VERGEN_GIT_SHA` idioms.
- Graph artifact routing remains W4-owned and is not introduced as W3 config.
- All deployment overrides use `ROUTER__DEJA__*` for Deja-owned settings; runtime code reads only typed settings.
