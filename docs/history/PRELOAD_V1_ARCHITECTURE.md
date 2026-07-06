> **Archived.** This document records the set-aside syscall-preload (LD_PRELOAD) v1 track. It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Déjà v1 Architecture

## The Core Insight

Most programs are deterministic in isolation. What makes them non-deterministic
is their interaction with **boundaries** — external systems that return different
values each time:

```
Your Program
    │
    ├── What time is it?          → SystemTime::now()
    ├── Give me random bytes      → /dev/urandom
    ├── What's this env var?      → getenv("API_KEY")
    └── Send this HTTP request    → TCP connect → server → response
```

If you **record** these boundary crossings and **replay** them by substituting
recorded values, the program behaves identically — without changing a single
line of source code.

## System Overview

```
┌─────────────────────────────────────────────────────────┐
│                        deja-cli                          │
│  Commands: record | replay | inspect                     │
│  Validates environment, injects preload, launches child  │
└─────────┬───────────────────────────────────┬───────────┘
          │ fork+exec with LD_PRELOAD         │ reads
          ▼                                   ▼
┌─────────────────────┐           ┌──────────────────────┐
│   deja-preload      │           │     Artifact Dir     │
│   (libdeja_preload  │◄─────────►│  ├─ metadata.json    │
│    .so)             │  persist  │  ├─ events.jsonl     │
│                     │           │  └─ inspection-      │
│  BoundaryRuntime    │           │     summary.json     │
│  ├─ record_*()      │           └──────────────────────┘
│  ├─ replay_*()      │                     ▲
│  └─ detect_*()      │                     │ reads
│                     │           ┌──────────┴───────────┐
│  .init_array ctor   │           │     deja-core         │
│  (runs before main) │           │  Types, schemas,      │
└─────────────────────┘           │  validation, I/O      │
          │                       └──────────────────────┘
          │ intercepts
          ▼
┌─────────────────────┐
│   Target Binary     │
│   (unchanged code)  │
│                     │
│   Calls boundaries: │
│   clock_gettime()   │
│   read(/dev/urandom)│
│   getenv()          │
│   HTTP connect/send │
└─────────────────────┘
```

## Component Roles

### deja-cli (`crates/deja-cli`)

The CLI is the **orchestrator**. It never touches boundaries directly.

**Record flow:**
1. Validate host environment (Linux x86_64 glibc)
2. Inspect target binary (64-bit ELF, dynamically linked, no setuid)
3. Set up environment variables for preload bootstrap
4. `exec()` the target with `LD_PRELOAD=libdeja_preload.so`
5. Wait for child to exit

**Replay flow:**
Same as record, but passes `DEJA_PRELOAD_MODE=replay` and requires
`--artifact <PATH>` pointing to a recorded artifact.

**Inspect flow:**
Reads the artifact directory and prints a human-readable `key=value` summary
of everything captured.

**Exit codes:**
- `0` — success
- `1` — internal error (child launch failed, I/O error)
- `2` — usage error (missing args, unknown flag)
- `3` — unsupported environment (wrong OS, static binary, etc.)
- `4` — invalid artifact (missing, corrupt, schema mismatch)

### deja-preload (`crates/deja-preload`)

The preload library is the **boundary interceptor**. It's the core of the system.

**How it gets loaded:**
```c
// This runs BEFORE main() via ELF .init_array
#[link_section = ".init_array"]
static DEJA_PRELOAD_INIT_ARRAY: extern "C" fn() = preload_ctor;
```

When the dynamic linker loads `libdeja_preload.so` (via `LD_PRELOAD`), it
calls `preload_ctor()` before the target's `main()`. The constructor:
1. Reads bootstrap env vars (`DEJA_PRELOAD_MODE`, `DEJA_PRELOAD_ARTIFACT_ROOT`, etc.)
2. In record mode: creates a new artifact directory with metadata
3. In replay mode: loads the existing artifact and its event stream
4. Initializes `BoundaryRuntime` — the state machine for recording/replaying

**BoundaryRuntime:**

The central struct that manages all boundary interception:

```rust
struct BoundaryRuntime {
    artifact_root: PathBuf,
    artifact: ArtifactBundle,      // the complete artifact in memory
    mode: PreloadMode,             // Record or Replay
    next_sequence: u64,            // monotonic event counter
    replay_cursor: usize,          // position in event stream during replay
}
```

**Record mode methods** — capture live values:
- `record_time_system_now(duration)` → logs actual timestamp
- `record_random_dev_urandom(bytes)` → logs actual random bytes
- `record_environment_get(key, value)` → logs actual env var value
- `record_http_exchange(exchange)` → logs full HTTP request/response

**Replay mode methods** — substitute recorded values:
- `replay_time_system_now()` → returns recorded timestamp
- `replay_random_dev_urandom(len)` → returns recorded random bytes
- `replay_environment_get(key)` → returns recorded env var value
- `replay_http_exchange(request)` → returns recorded HTTP response (no network!)

**Divergence detection:**
During replay, HTTP request fields are compared against the recording.
Any differences (authority, body, headers) are reported to stderr as
divergences but do NOT block the replay. This is by design — the system
detects changes rather than failing on them.

**Boundary guards:**
Unsupported boundary access is a hard error, not silent degradation:
- `CLOCK_MONOTONIC` → `UnsupportedBoundaryError::Time`
- `getrandom()` → `UnsupportedBoundaryError::Random`
- `setenv()` / `unsetenv()` → `UnsupportedBoundaryError::Environment`
- Unknown env keys → `UnsupportedBoundaryError::Environment`

### deja-core (`crates/deja-core`)

The core library defines **types, schemas, and contracts** shared by all crates.

**Artifact schema** (`deja.artifact/v1`):
```
artifact-root/
├── metadata.json              ← ArtifactMetadataDocument
├── events.jsonl               ← EventRecord per line
└── inspection-summary.json    ← InspectionSummaryDocument
```

**Event types** (the four v1 boundaries):
```rust
enum BoundaryEvent {
    Time(TimeBoundaryEvent),        // SystemTime::now()
    Random(RandomBoundaryEvent),    // /dev/urandom reads
    Environment(EnvironmentBoundaryEvent),  // getenv()
    Http(HttpExchangeEvent),        // full HTTP request/response
}
```

**Fidelity model** — every event carries metadata about how faithful the capture is:
```rust
struct RecordMetadata {
    event_id: String,               // e.g. "evt-time-system_time-1"
    sequence: u64,                  // monotonic ordering
    capture_fidelity: CaptureFidelity,        // Exact or Semantic
    replay_classification: ReplayClassification,  // how replay behaves
    divergence_markers: Vec<DivergenceMarker>,    // what's different
}
```

- **Exact** fidelity (time, random, env): bit-for-bit identical values
- **Semantic** fidelity (HTTP): meaning-preserving but not byte-identical
- **DeterministicEquivalent**: replay produces identical behavior
- **SemanticallyEquivalent**: replay produces functionally equivalent behavior
- **Divergent**: replay behavior differs from recording

**Environment validation:**
- `validate_supported_host_environment()` → Linux x86_64 glibc only
- `validate_supported_execution_environment()` → adds target binary checks
- `TargetBinaryMetadata::inspect()` → reads ELF headers (NOT the whole binary)

## Data Flow: Record

```
1. CLI validates environment
2. CLI exec()s target with LD_PRELOAD

3. .init_array constructor runs before main()
   ├─ Reads DEJA_PRELOAD_MODE=record
   ├─ Creates artifact directory
   └─ Writes metadata.json

4. Target's main() starts
   │
   ├─ getenv("HTTP_FIXTURE_FIXED_UNIX_TIME")
   │   └─ preload: record_environment_get() → append to events.jsonl
   │
   ├─ SystemTime::now()
   │   └─ preload: record_time_system_now() → append to events.jsonl
   │
   ├─ getenv("HTTP_FIXTURE_RANDOM_HEX")
   │   └─ preload: record_environment_get() → append to events.jsonl
   │
   ├─ read(/dev/urandom, 8 bytes)
   │   └─ preload: record_random_dev_urandom() → append to events.jsonl
   │
   ├─ getenv("HTTP_FIXTURE_GREETING")
   │   └─ preload: record_environment_get() → append to events.jsonl
   │
   └─ HTTP POST to server
       └─ preload: record_http_exchange() → append to events.jsonl
           (request goes to REAL server, response is captured)

5. Target exits
6. Artifact directory contains the complete recording
```

## Data Flow: Replay

```
1. CLI validates environment + artifact
2. CLI exec()s target with LD_PRELOAD

3. .init_array constructor runs before main()
   ├─ Reads DEJA_PRELOAD_MODE=replay
   ├─ Reads DEJA_PRELOAD_ARTIFACT_ROOT=/path/to/artifact
   └─ Loads all events from events.jsonl into memory

4. Target's main() starts (replay_cursor = 0)
   │
   ├─ getenv("HTTP_FIXTURE_FIXED_UNIX_TIME")
   │   └─ preload: replay_environment_get()
   │       cursor++ → returns recorded value (None)
   │
   ├─ SystemTime::now()
   │   └─ preload: replay_time_system_now()
   │       cursor++ → returns recorded timestamp
   │
   ├─ read(/dev/urandom, 8 bytes)
   │   └─ preload: replay_random_dev_urandom()
   │       cursor++ → returns recorded random bytes
   │
   └─ HTTP POST to "server" (nothing listens)
       └─ preload: replay_http_exchange()
           cursor++ → detects divergences, reports to stderr
                    → returns recorded HTTP response
                    → NO NETWORK CALL MADE

5. Target exits with same behavior as recording
```

## Key Design Decisions

### Why LD_PRELOAD, not ptrace?

ptrace (used by rr, strace) gives complete control but:
- Requires root or CAP_SYS_PTRACE
- Massive performance overhead (every syscall trapped)
- Complex state machine for deterministic scheduling
- Targets arbitrary-process replay (not our goal)

LD_PRELOAD is lightweight:
- No special permissions needed
- Only intercepts what we hook (boundaries)
- Works with any dynamically linked binary
- Simpler, more focused approach

Trade-off: We can't intercept statically linked binaries or vDSO calls.

### Why boundary-first, not syscall-first?

Capturing all syscalls produces enormous, opaque recordings.
Capturing boundaries produces small, human-readable, semantically
meaningful recordings. A 6-event artifact tells you exactly what
external state the program observed.

### Why detect-and-report, not fail-fast for HTTP divergences?

During replay, some fields legitimately differ:
- **Authority** (host:port): the real server isn't running
- **Host header**: mirrors authority
- **Body**: may contain PID, timestamps, or other runtime values

Rather than hard-failing, we report these as divergences to stderr.
The user sees exactly what changed and decides if it matters. This
makes Déjà a **detection system**, not a gating system.

### Why explicit unsupported-boundary errors?

If a program tries to use a boundary we don't support (e.g., CLOCK_MONOTONIC,
getrandom(), setenv()), we fail loudly with a typed error. No silent
degradation. The user knows immediately that their workload needs
capabilities beyond v1.

### Why directory-based artifacts, not a single file?

- `events.jsonl` supports streaming append (one line per event)
- `metadata.json` and `inspection-summary.json` can be read without
  parsing the event stream
- Human-readable with standard tools (`cat`, `jq`, `python -m json.tool`)
- Easy to diff between recordings

## v1 Constraints (intentional)

| Constraint | Why |
|---|---|
| Linux x86_64 only | LD_PRELOAD + ELF + glibc focus |
| Dynamically linked only | LD_PRELOAD requires dynamic linker |
| Launched-child only | Preload constructor must run before main() |
| Single-threaded fixtures | Thread-safety adds significant complexity |
| Plaintext HTTP/1.1 only | TLS interception is a separate hard problem |
| 3 whitelisted env vars | Prevents accidental capture of secrets |
| 8-byte /dev/urandom only | Scoped to the fixture's exact pattern |

Each constraint is enforced with explicit error messages and tested.
These are not missing features — they're intentional scope boundaries
for the v1 milestone.

## Testing Strategy

**17 tests across 3 levels:**

| Level | Tests | What they verify |
|---|---|---|
| CLI contract (`deja-cli`) | 4 | Help output, exit codes, error messages |
| Schema (`deja-core`) | 4 | Round-trip serialization, corruption, schema mismatch, HTTP normalization |
| E2E (`deja-e2e`) | 13 | Full record/replay, divergence detection, compatibility matrix, scope guardrails |

Key tests:
- `record_replay_http_fixture` — the complete vertical slice: record with real server, inspect, replay without server
- `replay_divergence_reported` — verifies divergence detection works
- `deferred_scope_guardrails_present` — prevents scope drift in docs
- `unsupported_matrix_entry_rejected` — 7 rejection paths (32-bit, big-endian, non-ELF, truncated, musl, Windows, setgid)

## Supported Surfaces (v1)

### Supported
- **Address families:** AF_INET / AF_INET6
- **Socket type:** SOCK_STREAM (TCP)
- **Content:** Plaintext only (no TLS interception)
- **Syscalls:** socket, connect, accept, accept4, send, recv, read, write, writev, sendmsg, recvmsg, close
- **Protocols detected:** HTTP/1.1, Redis RESP2/RESP3, PostgreSQL wire protocol

### Not Supported
- **TLS / SSL:** Raw socket bytes are ciphertext; cannot parse or meaningfully replay
- **UDP / SOCK_DGRAM:** No sendto/recvfrom hooks
- **Unix domain sockets (AF_UNIX):** Not tracked
- **Zero-copy I/O:** sendfile, splice — bypass send/recv hooks
- **Batched I/O:** sendmmsg, recvmmsg — not hooked
- **Ancillary data:** msg_control in msghdr — not captured
- **shutdown() semantics:** Not intercepted

Unsupported socket types emit a one-time `[deja] WARNING` to stderr when first encountered. The real syscall always proceeds — Déjà never blocks functionality, it just doesn't record.
