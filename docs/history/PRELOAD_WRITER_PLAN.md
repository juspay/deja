> **Archived.** This document records the preload-era background-writer plan; the shipped writer (deja-record AsyncRecordWriter) made different choices (blocking backpressure, not drop-and-count). It is kept for historical context and no longer matches the shipped system; the current reference is [DEJA_RECORDING_ARCHITECTURE.md](../DEJA_RECORDING_ARCHITECTURE.md).

# Phase B: Background Writer Thread Architecture

## Overview

This document outlines the architectural changes needed to implement a **background writer thread** for the Déjà recording system. This is a foundational change that moves event persistence from the synchronous hot path to an asynchronous background worker, reducing latency impact on the instrumented application.

## Decisions Locked In

These implementation decisions are now fixed for the first pass:

- **Writer isolation**: start with an **in-process background thread**, but keep the abstraction clean enough to swap in a helper process later.
- **Queue saturation**: use **bounded wait, then drop and count** rather than blocking indefinitely.
- **Artifact contract**: keep **live `events.jsonl` as the canonical artifact** in the first pass.
- **Durability target**: guarantee **graceful shutdown flushes**; abrupt kill may still lose the tail.
- **Mode split**: allow a **performance-oriented mode** and a **full-validation mode**.
- **Delivery constraint**: first pass should fit roughly **one focused implementation hour** and end with **one pipeline rerun** to compare scorecard/metrics.

## Scope Split

### Phase B1 — fast first pass (the thing we implement now)
- background writer thread
- bounded in-memory queue
- keep `events.jsonl` open for the life of the recording
- move JSON serialization + disk append off the hook hot path
- stop recomputing inspection summary on every event
- add queue/drop/flush metrics
- rerun one pipeline and compare latency/CPU/scorecard deltas

### Phase B2 — follow-on if B1 is not enough
- compact/binary in-memory event representation to cut hot-path allocation/copy cost further
- optional binary on-disk spool/export flow
- helper-process writer instead of in-process thread

## Current State Analysis

### Problem: Synchronous Disk I/O on Every Event

In `deja-preload/src/lib.rs`, the `persist_artifact()` method writes directly to disk synchronously:

```rust
fn persist_artifact(&mut self) -> Result<(), PreloadRuntimeError> {
    // ... serialization logic ...
    let file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&layout.events_path)?;
    // ... write to disk immediately ...
}
```

This happens on **every boundary crossing** (every socket send/recv, every clock_gettime, every getrandom). For high-throughput applications like Hyperswitch, this creates:

1. **Latency spikes**: Disk I/O blocks the application's syscall path
2. **I/O contention**: Competes with the application's own disk operations
3. **Throughput reduction**: Each syscall pays the full write cost

### Existing Metrics Showing the Pain

From `ROADMAP.md` Layer 5 (Production Readiness):
- Target: < 5% P50 latency increase, < 10% P99 increase
- Current synchronous writes make these targets difficult to achieve under load

## Proposed Architecture

### Design Goals

1. **Minimal latency impact**: Hot path should only copy to memory, never block on I/O
2. **Bounded memory usage**: Prevent unbounded growth if writer falls behind
3. **Durability guarantees**: Graceful handling of crashes, SIGTERM, process exit
4. **Backpressure**: Apply backpressure to the application if buffer is saturated
5. **Zero-allocation hot path**: Pre-allocated buffers, lock-free where possible

### High-Level Design

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Application Process                               │
│                                                                             │
│  ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────────┐   │
│  │   Syscall Hook  │────▶│  Event Buffer   │────▶│   Background        │   │
│  │   (hot path)    │     │  (bounded queue)│     │   Writer Thread     │   │
│  └─────────────────┘     └─────────────────┘     └─────────────────────┘   │
│          │                      │                         │                 │
│          │                      │                         │                 │
│          ▼                      ▼                         ▼                 │
│   Lock-free enqueue      Wait-free drain           Buffered fsync          │
│   (copy to slab)         (batch collect)           (periodic flush)        │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
                          ┌─────────────────┐
                          │  events.jsonl   │
                          │  (final output) │
                          └─────────────────┘
```

## Implementation Plan

### Phase B.1: Bounded Lock-Free Ring Buffer

**Location**: `crates/deja-preload/src/event_buffer.rs` (new file)

**Design**: Single-producer, single-consumer (SPSC) lock-free ring buffer

- **Producer**: The LD_PRELOAD hook (single thread per hook type, but multiple hooks can fire concurrently)
- **Consumer**: Background writer thread

Actually, we have multiple producers (different syscalls can fire concurrently on different threads). So we need MPSC.

**Revised Design**: Crossbeam's bounded MPMC channel or a custom sharded buffer.

```rust
use crossbeam::channel::{bounded, Receiver, Sender};
use deja_core::EventRecord;

/// Capacity is in events, not bytes. First pass keeps this simple and tunable.
const DEFAULT_BUFFER_CAPACITY: usize = 10_000;

pub struct EventBuffer {
    sender: Sender<QueuedRecord>,
}

/// Owned event handed off from the hook path to the writer thread.
///
/// First pass keeps `EventRecord` as the queue payload so JSON serialization
/// moves fully off the hook path while preserving the existing JSONL contract.
pub struct QueuedRecord {
    pub record: EventRecord,
    pub approx_bytes: usize,
}

impl EventBuffer {
    pub fn new(capacity: usize) -> (Self, EventReceiver) {
        let (sender, receiver) = bounded(capacity);
        (Self { sender }, EventReceiver { receiver })
    }

    pub fn try_push(&self, event: QueuedRecord) -> Result<(), BufferFull> {
        self.sender.try_send(event).map_err(|_| BufferFull)
    }

    pub fn push_timeout(
        &self,
        event: QueuedRecord,
        timeout: Duration,
    ) -> Result<(), BufferFull> {
        self.sender.send_timeout(event, timeout).map_err(|_| BufferFull)
    }
}

pub struct EventReceiver {
    receiver: Receiver<QueuedRecord>,
}

impl EventReceiver {
    /// Block for the first item, then drain more items without blocking.
    pub fn recv_batch(&self, max_batch: usize) -> Vec<QueuedRecord> {
        let first = match self.receiver.recv() {
            Ok(e) => e,
            Err(_) => return vec![],
        };

        let mut batch = vec![first];
        while batch.len() < max_batch {
            match self.receiver.try_recv() {
                Ok(e) => batch.push(e),
                Err(_) => break,
            }
        }
        batch
    }
}
```

**Why crossbeam**: Battle-tested, zero-allocation in steady state, excellent performance characteristics.

**Fallback if no crossbeam**: Use `std::sync::mpsc` with bounded channel (requires nightly or custom implementation).

### Phase B.2: Background Writer Thread

**Location**: `crates/deja-preload/src/writer.rs` (new file)

**Responsibilities**:
1. Drain events from buffer in batches
2. Serialize queued records to JSONL in the background thread
3. Write to disk with buffering
4. Handle periodic fsync (durability vs performance tradeoff)
5. Respond to shutdown signals

```rust
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{spawn, JoinHandle};

pub struct BackgroundWriter {
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl BackgroundWriter {
    pub fn spawn(
        receiver: EventReceiver,
        output_path: PathBuf,
        config: WriterConfig,
    ) -> Result<Self, WriterError> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_flag = shutdown.clone();
        
        let handle = spawn(move || {
            writer_loop(receiver, output_path, config, shutdown_flag);
        });
        
        Ok(Self {
            handle: Some(handle),
            shutdown,
        })
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
    
    pub fn join(self) -> Result<(), WriterError> {
        if let Some(handle) = self.handle {
            handle.join().map_err(|_| WriterError::ThreadPanic)?;
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct WriterConfig {
    /// Max events to batch before writing
    pub batch_size: usize,
    /// Max time to wait before flushing a partial batch (ms)
    pub flush_interval_ms: u64,
    /// Buffer size for BufWriter (KB)
    pub buffer_kb: usize,
    /// Whether to fsync after every batch
    pub fsync_every_batch: bool,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            flush_interval_ms: 100,
            buffer_kb: 64,
            fsync_every_batch: false, // Durability vs performance tradeoff
        }
    }
}

fn writer_loop(
    receiver: EventReceiver,
    output_path: PathBuf,
    config: WriterConfig,
    shutdown: Arc<AtomicBool>,
) {
    let file = match File::create(&output_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[deja:writer] Failed to open output file: {}", e);
            return;
        }
    };
    
    let mut writer = BufWriter::with_capacity(config.buffer_kb * 1024, file);
    let flush_interval = Duration::from_millis(config.flush_interval_ms);
    
    loop {
        // Check shutdown
        if shutdown.load(Ordering::SeqCst) {
            // Drain remaining events
            let remaining = drain_all(&receiver);
            if let Err(e) = write_batch(&mut writer, &remaining) {
                eprintln!("[deja:writer] Final write failed: {}", e);
            }
            let _ = writer.flush();
            break;
        }
        
        // Block for batch with timeout
        let batch = receiver.recv_batch_timeout(config.batch_size, flush_interval);
        
        if !batch.is_empty() {
            if let Err(e) = write_batch(&mut writer, &batch) {
                eprintln!("[deja:writer] Write failed: {}", e);
                // Continue trying - don't lose events
            }
            
            if config.fsync_every_batch {
                let _ = writer.flush();
                let _ = writer.get_ref().sync_all();
            }
        } else {
            // Timeout with no events - flush buffer anyway
            let _ = writer.flush();
        }
    }
}

fn write_batch(writer: &mut BufWriter<File>, batch: &[QueuedRecord]) -> io::Result<()> {
    for event in batch {
        serde_json::to_writer(&mut *writer, &event.record)?;
        writer.write_all(b"\n")?;
    }
    Ok(())
}
```

### Phase B.3: Integration with AgentRuntime

**Location**: `crates/deja-preload/src/agent.rs` and `crates/deja-preload/src/lib.rs`

**Changes to `AgentRuntime`**:

```rust
pub struct AgentRuntime {
    // ... existing fields ...
    
    // NEW: Event buffer for async writing
    event_buffer: Option<EventBuffer>,
    
    // NEW: Background writer handle
    background_writer: Option<BackgroundWriter>,
    
    // NEW: Shutdown coordination
    shutdown_triggered: AtomicBool,
}

impl AgentRuntime {
    pub fn new(mode: PreloadMode, runtime: BoundaryRuntime) -> Self {
        // ... existing init ...
        
        // NEW: Initialize buffer and writer in record mode
        let (event_buffer, background_writer) = if mode == PreloadMode::Record {
            let (buffer, receiver) = EventBuffer::new(DEFAULT_BUFFER_CAPACITY);
            let writer = BackgroundWriter::spawn(
                receiver,
                runtime.artifact_root().join("events.jsonl"),
                WriterConfig::default(),
            ).ok(); // Continue even if writer fails - degrade gracefully
            (Some(buffer), writer)
        } else {
            (None, None)
        };
        
        Self {
            // ... existing fields ...
            event_buffer,
            background_writer,
            shutdown_triggered: AtomicBool::new(false),
        }
    }
    
    /// Called on process exit / SIGTERM / atexit
    pub fn graceful_shutdown(&self) {
        if self.shutdown_triggered.swap(true, Ordering::SeqCst) {
            return; // Already shutting down
        }
        
        eprintln!("[deja] Initiating graceful shutdown...");
        
        // Signal writer to finish
        if let Some(ref writer) = self.background_writer {
            writer.shutdown();
        }
        
        // Wait for completion (with timeout)
        if let Some(writer) = self.background_writer.take() {
            // Use try_join with timeout in real implementation
            let _ = writer.join();
        }
        
        // Write final manifest
        self.write_manifest(...);
        
        eprintln!("[deja] Shutdown complete");
    }
}
```

**Changes to `BoundaryRuntime::persist_artifact()`**:

```rust
fn persist_artifact(&mut self) -> Result<(), PreloadRuntimeError> {
    if let Some(ref buffer) = self.agent.event_buffer {
        let last_event = self.artifact.events
            .last()
            .cloned()
            .ok_or(ArtifactError::EmptyArtifact)?;

        let queued = QueuedRecord {
            approx_bytes: estimate_event_size(&last_event),
            record: last_event,
        };

        // First pass policy: bounded wait, then drop + count.
        match buffer.push_timeout(queued, Duration::from_micros(100)) {
            Ok(()) => Ok(()),
            Err(BufferFull) => {
                self.agent.metrics.dropped_events.fetch_add(1, Ordering::Relaxed);
                eprintln!("[deja] WARNING: event queue full, dropping event");
                Ok(())
            }
        }
    } else {
        self.synchronous_persist()
    }
}
```

### Phase B.4: Signal Handling for Clean Shutdown

**Location**: `crates/deja-preload/src/lib.rs` (existing signal handlers)

Enhance existing SIGTERM and atexit handlers:

```rust
extern "C" fn sigterm_flush(_sig: libc::c_int) {
    eprintln!("[deja] SIGTERM received, flushing events...");
    
    if let Some(agent) = hooks::agent() {
        agent.graceful_shutdown();
    }
    
    // Restore default handler and re-raise
    unsafe {
        libc::signal(libc::SIGTERM, libc::SIG_DFL);
        libc::raise(libc::SIGTERM);
    }
}

extern "C" fn atexit_flush() {
    eprintln!("[deja] atexit: flushing events");
    
    if let Some(agent) = hooks::agent() {
        agent.graceful_shutdown();
    }
}
```

### Phase B.5: Configuration and Tuning

**Location**: Environment variables (existing pattern from `DEJA_TEST_CRASH_AFTER_N`)

```rust
pub struct AsyncWriterConfig {
    /// Enable background writer (default: true in record mode)
    pub enabled: bool,
    /// Ring buffer capacity (number of events)
    pub buffer_capacity: usize,
    /// Batch size for writing
    pub batch_size: usize,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
    /// Buffer size for file I/O (KB)
    pub file_buffer_kb: usize,
    /// Fsync after every batch (durability vs performance)
    pub fsync_enabled: bool,
}

impl AsyncWriterConfig {
    pub fn from_env() -> Self {
        Self {
            enabled: env::var("DEJA_ASYNC_WRITER")
                .map(|v| v != "0" && v != "false")
                .unwrap_or(true),
            buffer_capacity: env::var("DEJA_BUFFER_CAPACITY")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10_000),
            batch_size: env::var("DEJA_BATCH_SIZE")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            flush_interval_ms: env::var("DEJA_FLUSH_INTERVAL_MS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(100),
            file_buffer_kb: env::var("DEJA_FILE_BUFFER_KB")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(64),
            fsync_enabled: env::var("DEJA_FSYNC_ENABLED")
                .map(|v| v == "1" || v == "true")
                .unwrap_or(false),
        }
    }
}
```

## Trade-offs and Decisions

### 1. Crossbeam vs std::sync::mpsc

**Decision**: Use `crossbeam-channel` for the first pass.

**Rationale**:
- bounded queues with backpressure support now
- mature multi-producer behavior for a multi-threaded Tokio service
- much faster to ship than a custom queue inside the one-hour constraint

### 2. Queue payload shape

**Decision**: Queue **owned `EventRecord`s** in B1, and let the writer thread do JSON serialization.

**Rationale**:
- moves `serde_json` cost off the hook hot path immediately
- preserves the current `events.jsonl` artifact contract
- keeps the first pass much smaller than introducing an on-disk binary spool immediately

**Deferred optimization**:
- compact/binary in-memory payloads remain a B2 optimization if B1 does not move the scorecard enough

### 3. Queue full policy

**Decision**: **bounded wait, then drop and count**.

**Rationale**:
- respects the latency-first goal
- gives the writer a tiny chance to catch up during bursty load
- makes overload observable through metrics instead of silently stalling the app

### 4. Canonical artifact format

**Decision**: Keep live `events.jsonl` as the canonical artifact in B1.

**Rationale**:
- avoids breaking existing CLI/inspection/correlation tooling during the first pass
- still lets us remove the worst costs: per-event open/write and per-event summary recomputation
- keeps the validation rerun easy within the one-pipeline constraint

### 5. Durability target

**Decision**: Graceful shutdown correctness first; abrupt crash durability is deferred.

**Rationale**:
- matches the time budget
- avoids forcing expensive fsync/WAL work into the first pass
- still gives us clean behavior on `SIGTERM`/`atexit`

### 6. Isolation model

**Decision**: background **thread** first, but keep the writer interface process-ready.

**Rationale**:
- minimum implementation risk for the first rerun
- still leaves a clean path to a helper process if thread isolation is insufficient later

### 7. Product operating modes

**Decision**: support a **performance mode** and a **full-validation mode**.

**Rationale**:
- lets the fast path optimize for scorecard improvement
- preserves heavier diagnostic/validation behavior when needed
- avoids overfitting one mode to two incompatible goals

## Testing Strategy

### Unit Tests

1. **Ring buffer correctness**: Single/multi-producer, single consumer ordering
2. **Batch formation**: Correct batching under various load patterns
3. **Graceful shutdown**: All events flushed on SIGTERM/atexit
4. **Buffer saturation**: Proper dropping with accurate counters

### Integration Tests

1. **E2E record/replay**: Verify async writer doesn't affect correctness
2. **Crash recovery**: Partial artifact handling when process killed mid-write
3. **Performance**: Latency/throughput comparison sync vs async
4. **Stress test**: High event rate with small buffer to exercise saturation

### Benchmarks

```bash
# Baseline (synchronous writes)
DEJA_ASYNC_WRITER=0 cargo bench

# With background writer
DEJA_ASYNC_WRITER=1 DEJA_BUFFER_CAPACITY=10000 cargo bench

# Different buffer sizes
for cap in 1000 10000 100000; do
    DEJA_BUFFER_CAPACITY=$cap cargo bench
done
```

## First Implementation Cut (60-minute pass)

This is the smallest meaningful slice that can plausibly be built and rerun quickly.

### Workstream A — queue + writer module
- add `event_buffer.rs`
- add `writer.rs`
- add `crossbeam-channel` dependency
- writer owns one open `events.jsonl` file handle for the life of the run

### Workstream B — hot-path integration
- wire buffer + writer into `AgentRuntime`
- change `persist_artifact()` so it **queues** an owned record instead of opening/writing the file directly
- remove per-event `inspection_summary_from_events(...)` recomputation from the hot path
- flush summary/metadata only on graceful shutdown
- add minimal queue/drop/flush metrics

### Workstream C — validation rerun
- run targeted tests/build sanity
- run **one** Hyperswitch pipeline/benchmark rerun
- compare against the current reference log/scorecard:
  - `/tmp/deja-pipeline-redis-tokio-patch-20260507-024659.log`
- record whether P50/P99/CPU moved in the right direction

### Explicit non-goals for this first pass
- no helper process yet
- no on-disk binary spool yet
- no io_uring work
- no large CLI/tooling artifact migration
- no full feature-flag matrix unless needed to land safely

## Reuse

- `crates/deja-preload/src/lib.rs`
  - current `persist_artifact()` path
  - existing `atexit_flush` / `sigterm_flush` shutdown hooks
  - existing artifact initialization and flush behavior
- `crates/deja-preload/src/agent.rs`
  - current `AgentRuntime` ownership model
  - existing metrics counters (`hook_time_ns`, `dropped_events`, etc.)
  - existing `flush_all()` / `write_metrics()` behavior
- `crates/deja-core/src/lib.rs`
  - current `EventRecord` / artifact layout / `events.jsonl` contract
  - `inspection_summary_from_events(...)` for final summary generation
- demo validation harness already in repo
  - `demo/pipeline.sh`
  - `demo/run-hyperswitch-pipeline.sh`
  - `demo/hs41-harness.sh`
- latest benchmark reference log for before/after comparison
  - `/tmp/deja-pipeline-redis-tokio-patch-20260507-024659.log`

## Files to Create/Modify

### New Files
1. `crates/deja-preload/src/event_buffer.rs` - Lock-free event buffer
2. `crates/deja-preload/src/writer.rs` - Background writer thread
3. `crates/deja-preload/src/async_config.rs` - Configuration structures

### Modified Files
1. `crates/deja-preload/src/agent.rs` - Integrate buffer and writer
2. `crates/deja-preload/src/lib.rs` - Modify persist path, enhance shutdown
3. `crates/deja-preload/Cargo.toml` - Add crossbeam dependency

## Success Criteria

### For the first pass
1. **Hot path no longer opens/appends the artifact file per event**.
2. **Hot path no longer recomputes inspection summary per event**.
3. **JSON serialization and disk append happen on the background writer thread**.
4. **Graceful shutdown flushes queued events and writes final metadata/summary**.
5. **One pipeline rerun completes and shows measurable improvement in at least one of**:
   - P50 / P99 latency
   - CPU ticks
   - self-reported hook overhead
6. **Metrics expose whether the queue saturated** (`drops`, `flushes`, and ideally peak queue depth or batch count).

### For the follow-on pass
- decide whether B1 improvement is sufficient or whether we must add compact/binary queued payloads next.

## Verification

- quick build/test sanity for the touched preload crate path
- one full Hyperswitch pipeline rerun from the main worktree using the existing runner
- compare before/after against `/tmp/deja-pipeline-redis-tokio-patch-20260507-024659.log`
- specifically inspect:
  - scorecard latency deltas (P50/P99)
  - CPU ticks
  - any new queue/drop metrics
  - whether the pipeline still completes end-to-end and writes artifacts cleanly

## Deferred Topics (not required for the first pass)

1. Whether an **on-disk binary spool** is needed after we measure B1
2. Whether the writer should become a **helper process** instead of a thread
3. Whether we need **multiple writer threads** or queue sharding for higher throughput
4. Whether Linux-specific paths like **io_uring** are worth the complexity later

## References

- [Crossbeam Channels](https://docs.rs/crossbeam-channel/latest/crossbeam_channel/)
- [Rust MPSC vs Crossbeam Benchmarks](https://github.com/crossbeam-rs/crossbeam/tree/master/crossbeam-channel/benchmarks)
- [Linux AIO vs io_uring](https://kernel.dk/io_uring.pdf)
- Existing pattern in ROADMAP.md Layer 5.5 (Disk I/O impact)
