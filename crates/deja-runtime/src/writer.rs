use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Write as _};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use serde::{Deserialize, Serialize};

const DEFAULT_QUEUE_CAPACITY: usize = 8192;
const DEFAULT_BATCH_SIZE: usize = 256;
const DEFAULT_FLUSH_INTERVAL_MS: u64 = 100;
const DEFAULT_FLUSH_TIMEOUT_MS: u64 = 30_000;
/// Consecutive sink failures before the writer classifies the sink as dead
/// and disables itself. Below this, errors are transient: the failed batch is
/// accounted as dropped and the writer keeps consuming.
const FATAL_CONSECUTIVE_SINK_ERRORS: u32 = 8;

/// What happens at the enqueue layer when the bounded queue is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SinkPolicy {
    /// No-drop backpressure: the producing thread blocks until the writer
    /// catches up. **Opt-in only** — for offline/demo rigs where a byte-exact
    /// fixture matters more than latency. Never use in a request-serving
    /// process: a slow sink would stall real requests, breaking the shadow
    /// guarantee.
    Block,
    /// Never stall request threads: drop the record, count it, and remember its
    /// sequence range for the `dropped` sink marker. **The default** — recording
    /// is a shadow, so backpressure drops events rather than blocking the request.
    FailOpen,
}

/// Runtime configuration for the async recorder pipeline.
#[derive(Debug, Clone, Copy)]
pub struct WriterConfig {
    pub queue_capacity: usize,
    pub batch_size: usize,
    pub flush_interval: Duration,
    /// Maximum time a synchronous flush or shutdown waits for the writer thread.
    pub flush_timeout: Duration,
    /// Optional durability policy: force a `sink.flush()` after this many
    /// records have been written since the previous flush. `None` (default)
    /// disables this policy and relies only on the periodic timer or explicit
    /// `Flush`/`Shutdown` messages.
    pub flush_after_records: Option<usize>,
    /// Queue-full behavior at the enqueue layer.
    pub policy: SinkPolicy,
}

impl Default for WriterConfig {
    fn default() -> Self {
        Self {
            queue_capacity: DEFAULT_QUEUE_CAPACITY,
            batch_size: DEFAULT_BATCH_SIZE,
            flush_interval: Duration::from_millis(DEFAULT_FLUSH_INTERVAL_MS),
            flush_timeout: Duration::from_millis(DEFAULT_FLUSH_TIMEOUT_MS),
            flush_after_records: None,
            policy: SinkPolicy::FailOpen,
        }
    }
}

/// Loss-accounting markers the writer threads through the sink so a
/// downstream consumer can audit delivery (`deja_sink_marker` records on the
/// wire). Sinks opt in by overriding [`RecordSink::write_marker`]; the
/// default is a no-op so file sinks and tests are unaffected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MarkerKind {
    /// Periodic progress stamp (emitted after each successful flush).
    Checkpoint,
    /// Final marker on writer shutdown — "everything before this landed".
    Eof,
    /// One or more records were dropped (fail-open enqueue or a failed
    /// batch); the payload carries the sequence ranges.
    Dropped,
}

impl MarkerKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            MarkerKind::Checkpoint => "checkpoint",
            MarkerKind::Eof => "eof",
            MarkerKind::Dropped => "dropped",
        }
    }
}

/// Sink abstraction for completed records.
///
/// JSONL is the development sink. A Kafka sink can implement this trait without
/// changing macro expansion, event building, or Hyperswitch instrumentation.
pub trait RecordSink<T>: Send + 'static {
    fn write_batch(&mut self, records: &[T]) -> io::Result<()>;

    fn flush(&mut self) -> io::Result<()>;

    /// Loss-accounting marker (checkpoint/eof/dropped). Default no-op so only
    /// transports that carry markers on the wire need to care.
    fn write_marker(&mut self, _kind: MarkerKind, _payload: &serde_json::Value) -> io::Result<()> {
        Ok(())
    }
}

/// JSONL sink for full-fidelity record persistence.
pub struct JsonlSink<T> {
    writer: BufWriter<File>,
    _record: PhantomData<fn() -> T>,
}

impl<T> JsonlSink<T> {
    pub fn new(path: &Path) -> io::Result<Self> {
        // Create the parent dir so callers can't fail on a missing directory.
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            _record: PhantomData,
        })
    }
}

impl<T> RecordSink<T> for JsonlSink<T>
where
    T: Serialize + Send + 'static,
{
    fn write_batch(&mut self, records: &[T]) -> io::Result<()> {
        for record in records {
            serde_json::to_writer(&mut self.writer, record)?;
            self.writer.write_all(b"\n")?;
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

/// Fan-out sink that writes each batch to one primary sink and any number of
/// secondary sinks.
///
/// Primary errors propagate, so a broken primary keeps surfacing failures
/// through the regular writer error path. Secondary errors are counted via the
/// `secondary_failure_counter` Arc and otherwise swallowed — a failing
/// secondary sink can never poison the primary write path. The caller is
/// expected to wire the same Arc into [`AsyncRecordWriter::track_secondary_failures`]
/// so the failure count is visible through [`WriterStatsSnapshot`].
pub struct CompositeSink<T> {
    primary: Box<dyn RecordSink<T>>,
    secondaries: Vec<Box<dyn RecordSink<T>>>,
    secondary_failure_counter: Arc<AtomicU64>,
}

impl<T> CompositeSink<T> {
    pub fn new(primary: Box<dyn RecordSink<T>>) -> Self {
        Self {
            primary,
            secondaries: Vec::new(),
            secondary_failure_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn with_secondary(mut self, sink: Box<dyn RecordSink<T>>) -> Self {
        self.secondaries.push(sink);
        self
    }

    /// Returns a clone of the shared counter that tracks failed writes to any
    /// secondary sink. Pass this to
    /// [`AsyncRecordWriter::track_secondary_failures`] so the failures are
    /// reflected in [`WriterStatsSnapshot::secondary_send_failures`].
    pub fn secondary_failure_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.secondary_failure_counter)
    }
}

impl<T> RecordSink<T> for CompositeSink<T>
where
    T: Send + 'static,
{
    fn write_batch(&mut self, records: &[T]) -> io::Result<()> {
        self.primary.write_batch(records)?;
        for sink in &mut self.secondaries {
            if sink.write_batch(records).is_err() {
                self.secondary_failure_counter
                    .fetch_add(1, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        let primary = self.primary.flush();
        for sink in &mut self.secondaries {
            // Best-effort: secondaries cannot poison the primary durability
            // signal.
            let _ = sink.flush();
        }
        primary
    }

    fn write_marker(&mut self, kind: MarkerKind, payload: &serde_json::Value) -> io::Result<()> {
        let primary = self.primary.write_marker(kind, payload);
        for sink in &mut self.secondaries {
            let _ = sink.write_marker(kind, payload);
        }
        primary
    }
}

/// Snapshot of recorder health counters.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WriterStatsSnapshot {
    pub attempted_records: u64,
    pub enqueued_records: u64,
    pub inactive_records: u64,
    pub backpressure_blocks: u64,
    pub queued_records: u64,
    pub records_written: u64,
    pub flushes: u64,
    pub write_errors: u64,
    /// Records dropped instead of delivered: fail-open enqueue drops plus
    /// batches lost to transient sink failures. Loss accounting for these
    /// rides the `dropped` sink markers.
    #[serde(default)]
    pub records_dropped: u64,
    /// Cumulative failed writes against any secondary sink configured behind a
    /// [`CompositeSink`]. Populated when the writer has been wired up with
    /// [`AsyncRecordWriter::track_secondary_failures`]; otherwise stays zero.
    #[serde(default)]
    pub secondary_send_failures: u64,
}

#[derive(Debug, Default)]
struct WriterStats {
    attempted_records: AtomicU64,
    enqueued_records: AtomicU64,
    inactive_records: AtomicU64,
    backpressure_blocks: AtomicU64,
    queued_records: AtomicU64,
    records_written: AtomicU64,
    flushes: AtomicU64,
    write_errors: AtomicU64,
    records_dropped: AtomicU64,
}

impl WriterStats {
    fn snapshot(&self) -> WriterStatsSnapshot {
        WriterStatsSnapshot {
            attempted_records: self.attempted_records.load(Ordering::Relaxed),
            enqueued_records: self.enqueued_records.load(Ordering::Relaxed),
            inactive_records: self.inactive_records.load(Ordering::Relaxed),
            backpressure_blocks: self.backpressure_blocks.load(Ordering::Relaxed),
            queued_records: self.queued_records.load(Ordering::Relaxed),
            records_written: self.records_written.load(Ordering::Relaxed),
            flushes: self.flushes.load(Ordering::Relaxed),
            write_errors: self.write_errors.load(Ordering::Relaxed),
            records_dropped: self.records_dropped.load(Ordering::Relaxed),
            secondary_send_failures: 0,
        }
    }
}

/// Coalescing ledger of dropped sequence ranges, shared between the enqueue
/// side (fail-open drops) and the worker (failed batches + marker drain).
#[derive(Debug, Default)]
struct DropLedger {
    ranges: Mutex<Vec<(u64, u64)>>,
}

impl DropLedger {
    fn push(&self, seq: u64) {
        if let Ok(mut ranges) = self.ranges.lock() {
            match ranges.last_mut() {
                Some(last) if last.1 + 1 == seq => last.1 = seq,
                _ => ranges.push((seq, seq)),
            }
        }
    }

    fn push_range(&self, from: u64, to: u64) {
        if let Ok(mut ranges) = self.ranges.lock() {
            ranges.push((from, to));
        }
    }

    fn drain(&self) -> Vec<(u64, u64)> {
        self.ranges
            .lock()
            .map(|mut r| std::mem::take(&mut *r))
            .unwrap_or_default()
    }
}

enum WriterMessage<T> {
    Record(T),
    Flush(mpsc::Sender<io::Result<()>>),
    Shutdown(mpsc::Sender<io::Result<()>>),
}

/// Sequence extractor: lets the generic writer account drops and stamp
/// markers with the record's own sequence number (`global_sequence` for
/// `BoundaryEvent`s).
pub type SeqOf<T> = Arc<dyn Fn(&T) -> u64 + Send + Sync>;

/// Full-fidelity async writer.
///
/// Producers enqueue complete records. When the bounded queue is full the
/// behavior follows [`SinkPolicy`]: `FailOpen` (the safe default) drops the
/// record, counts it, and remembers its sequence range for the `dropped` sink
/// marker; `Block` is opt-in no-drop backpressure for offline/demo rigs.
/// Request threads never stall in `FailOpen`. Transient sink failures drop
/// the affected batch (accounted the same way) without disabling the writer; only
/// [`FATAL_CONSECUTIVE_SINK_ERRORS`] failures in a row classify the sink as
/// dead and turn future records into no-ops so instrumentation cannot fail
/// application requests.
pub struct AsyncRecordWriter<T> {
    sender: SyncSender<WriterMessage<T>>,
    handle: Mutex<Option<JoinHandle<()>>>,
    stats: Arc<WriterStats>,
    active: Arc<AtomicBool>,
    secondary_failure_counter: Mutex<Option<Arc<AtomicU64>>>,
    policy: SinkPolicy,
    seq_of: Option<SeqOf<T>>,
    drops: Arc<DropLedger>,
    flush_timeout: Duration,
}

impl<T> AsyncRecordWriter<T>
where
    T: Send + 'static,
{
    pub fn new<S>(sink: S, config: WriterConfig) -> Self
    where
        S: RecordSink<T>,
    {
        Self::with_seq_of(sink, config, None)
    }

    /// Like [`Self::new`], with a sequence extractor so drops and markers
    /// carry real sequence numbers.
    pub fn with_seq_of<S>(sink: S, config: WriterConfig, seq_of: Option<SeqOf<T>>) -> Self
    where
        S: RecordSink<T>,
    {
        let (sender, receiver) = mpsc::sync_channel(config.queue_capacity);
        let stats = Arc::new(WriterStats::default());
        let active = Arc::new(AtomicBool::new(true));
        let drops = Arc::new(DropLedger::default());
        let thread_stats = Arc::clone(&stats);
        let thread_active = Arc::clone(&active);
        let thread_drops = Arc::clone(&drops);
        let thread_seq_of = seq_of.clone();

        let handle = thread::Builder::new()
            .name("deja-record-writer".to_string())
            .spawn(move || {
                writer_loop(
                    sink,
                    receiver,
                    config,
                    thread_stats,
                    thread_active,
                    thread_seq_of,
                    thread_drops,
                );
            })
            .ok();

        if handle.is_none() {
            active.store(false, Ordering::Release);
            stats.write_errors.fetch_add(1, Ordering::Relaxed);
        }

        Self {
            sender,
            handle: Mutex::new(handle),
            stats,
            active,
            secondary_failure_counter: Mutex::new(None),
            policy: config.policy,
            seq_of,
            drops,
            flush_timeout: config.flush_timeout,
        }
    }

    /// Register a shared counter (typically owned by a [`CompositeSink`]) that
    /// tracks secondary-sink write failures. Once registered, the value is
    /// reflected in [`WriterStatsSnapshot::secondary_send_failures`].
    pub fn track_secondary_failures(&self, counter: Arc<AtomicU64>) {
        if let Ok(mut slot) = self.secondary_failure_counter.lock() {
            *slot = Some(counter);
        }
    }

    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }

    pub fn record(&self, record: T) -> bool {
        if !self.is_active() {
            self.stats.inactive_records.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        self.stats.attempted_records.fetch_add(1, Ordering::Relaxed);
        self.stats.queued_records.fetch_add(1, Ordering::Relaxed);

        match self.sender.try_send(WriterMessage::Record(record)) {
            Ok(()) => {
                self.stats.enqueued_records.fetch_add(1, Ordering::Relaxed);
                true
            }
            Err(TrySendError::Full(WriterMessage::Record(record))) => match self.policy {
                SinkPolicy::Block => {
                    self.stats
                        .backpressure_blocks
                        .fetch_add(1, Ordering::Relaxed);
                    match self.sender.send(WriterMessage::Record(record)) {
                        Ok(()) => {
                            self.stats.enqueued_records.fetch_add(1, Ordering::Relaxed);
                            true
                        }
                        Err(_) => {
                            self.stats.queued_records.fetch_sub(1, Ordering::Relaxed);
                            self.disable_after_error();
                            false
                        }
                    }
                }
                SinkPolicy::FailOpen => {
                    // The drop decision lives HERE, at the enqueue layer: the
                    // request thread never blocks on a slow sink. The dropped
                    // sequence is remembered for the `dropped` marker.
                    self.stats.queued_records.fetch_sub(1, Ordering::Relaxed);
                    self.stats.records_dropped.fetch_add(1, Ordering::Relaxed);
                    if let Some(seq_of) = &self.seq_of {
                        self.drops.push(seq_of(&record));
                    }
                    false
                }
            },
            Err(TrySendError::Disconnected(_)) => {
                self.stats.queued_records.fetch_sub(1, Ordering::Relaxed);
                self.disable_after_error();
                false
            }
            Err(TrySendError::Full(message)) => {
                self.stats.queued_records.fetch_sub(1, Ordering::Relaxed);
                let _ = message;
                self.disable_after_error();
                false
            }
        }
    }

    pub fn flush(&self) -> io::Result<()> {
        if !self.is_active() {
            return Ok(());
        }

        let (tx, rx) = mpsc::channel();
        self.sender
            .send(WriterMessage::Flush(tx))
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "record writer stopped"))?;
        rx.recv_timeout(self.flush_timeout).map_err(|_| {
            io::Error::new(io::ErrorKind::TimedOut, "timed out flushing record writer")
        })?
    }

    /// Request a flush WITHOUT blocking on its completion (fire-and-forget).
    ///
    /// Enqueues a `Flush` the writer thread honors on its own schedule and
    /// discards the reply. Callers must never wait on a writer round-trip;
    /// durability still comes from the periodic timer, the
    /// `flush_after_records` threshold, and the shutdown flush.
    ///
    /// The enqueue itself is `try_send`: when the bounded channel is full
    /// (a saturated sink) the advisory flush is simply DROPPED — a full
    /// channel means the writer is already busy, and a blocking send here
    /// would hold the calling thread hostage until the sink freed a slot.
    pub fn request_flush(&self) {
        if !self.is_active() {
            return;
        }
        // A dropped receiver is intentional: the writer flushes, then its reply
        // `send` to the closed channel fails silently — exactly fire-and-forget.
        let (tx, _rx) = mpsc::channel();
        let _ = self.sender.try_send(WriterMessage::Flush(tx));
    }

    pub fn stats(&self) -> WriterStatsSnapshot {
        let mut snapshot = self.stats.snapshot();
        if let Ok(slot) = self.secondary_failure_counter.lock() {
            if let Some(counter) = slot.as_ref() {
                snapshot.secondary_send_failures = counter.load(Ordering::Relaxed);
            }
        }
        snapshot
    }

    fn disable_after_error(&self) {
        self.active.store(false, Ordering::Release);
        self.stats.write_errors.fetch_add(1, Ordering::Relaxed);
    }
}

impl<T> Drop for AsyncRecordWriter<T> {
    fn drop(&mut self) {
        let (tx, rx) = mpsc::channel();
        let _ = self.sender.send(WriterMessage::Shutdown(tx));
        if rx.recv_timeout(self.flush_timeout).is_err() {
            return;
        }

        if let Ok(mut handle) = self.handle.lock() {
            if let Some(handle) = handle.take() {
                let _ = handle.join();
            }
        }
    }
}

/// Worker-side state threaded through the batch/flush helpers: transient
/// failure accounting, sequence tracking for markers, and the drop ledger.
struct WorkerCtx<T> {
    stats: Arc<WriterStats>,
    active: Arc<AtomicBool>,
    seq_of: Option<SeqOf<T>>,
    drops: Arc<DropLedger>,
    consecutive_errors: u32,
    last_written_seq: u64,
    records_since_flush: u64,
}

impl<T> WorkerCtx<T> {
    /// Count a sink failure. Returns true when the failure streak crosses the
    /// fatal threshold (sink classified dead → writer disables).
    fn sink_failed(&mut self) -> bool {
        self.stats.write_errors.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors += 1;
        if self.consecutive_errors >= FATAL_CONSECUTIVE_SINK_ERRORS {
            self.active.store(false, Ordering::Release);
            true
        } else {
            false
        }
    }
}

fn writer_loop<T, S>(
    mut sink: S,
    receiver: mpsc::Receiver<WriterMessage<T>>,
    config: WriterConfig,
    stats: Arc<WriterStats>,
    active: Arc<AtomicBool>,
    seq_of: Option<SeqOf<T>>,
    drops: Arc<DropLedger>,
) where
    T: Send + 'static,
    S: RecordSink<T>,
{
    let mut batch = Vec::with_capacity(config.batch_size);
    let mut ctx = WorkerCtx {
        stats: Arc::clone(&stats),
        active: Arc::clone(&active),
        seq_of,
        drops,
        consecutive_errors: 0,
        last_written_seq: 0,
        records_since_flush: 0,
    };

    loop {
        let message = if batch.is_empty() {
            match receiver.recv() {
                Ok(message) => Some(message),
                Err(_) => break,
            }
        } else {
            match receiver.recv_timeout(config.flush_interval) {
                Ok(message) => Some(message),
                Err(RecvTimeoutError::Timeout) => {
                    // Periodic flush: write any buffered batch and force a
                    // flush so durability matches the timer cadence.
                    let fatal = write_batch(&mut sink, &mut batch, &mut ctx).is_break()
                        || flush_sink(&mut sink, &mut ctx).is_break();
                    if fatal {
                        drain_after_sink_failure(&receiver, &stats);
                        break;
                    }
                    None
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        };

        let Some(message) = message else {
            continue;
        };

        match message {
            WriterMessage::Record(record) => {
                stats.queued_records.fetch_sub(1, Ordering::Relaxed);
                batch.push(record);
                if batch.len() >= config.batch_size
                    && write_batch(&mut sink, &mut batch, &mut ctx).is_break()
                {
                    drain_after_sink_failure(&receiver, &stats);
                    break;
                }
                if should_flush_after_records(ctx.records_since_flush, &config)
                    && flush_sink(&mut sink, &mut ctx).is_break()
                {
                    drain_after_sink_failure(&receiver, &stats);
                    break;
                }
            }
            WriterMessage::Flush(reply) => {
                let write_outcome = write_batch(&mut sink, &mut batch, &mut ctx);
                let flush_outcome = flush_sink(&mut sink, &mut ctx);
                let ok = !write_outcome.failed() && !flush_outcome.failed();
                let _ = reply.send(if ok {
                    Ok(())
                } else {
                    Err(io::Error::other("sink write/flush failed"))
                });
                if write_outcome.is_break() || flush_outcome.is_break() {
                    drain_after_sink_failure(&receiver, &stats);
                    break;
                }
            }
            WriterMessage::Shutdown(reply) => {
                let write_outcome = write_batch(&mut sink, &mut batch, &mut ctx);
                let flush_outcome = flush_sink(&mut sink, &mut ctx);
                emit_eof_marker(&mut sink, &ctx);
                let ok = !write_outcome.failed() && !flush_outcome.failed();
                let _ = reply.send(if ok {
                    Ok(())
                } else {
                    Err(io::Error::other("sink write/flush failed"))
                });
                return;
            }
        }
    }

    let _ = write_batch(&mut sink, &mut batch, &mut ctx);
    let _ = flush_sink(&mut sink, &mut ctx);
    emit_eof_marker(&mut sink, &ctx);
}

fn should_flush_after_records(records_since_flush: u64, config: &WriterConfig) -> bool {
    match config.flush_after_records {
        Some(limit) if limit > 0 => records_since_flush >= limit as u64,
        _ => false,
    }
}

/// Outcome of a sink operation: success, a transient failure (keep going), or
/// a fatal one (the failure streak crossed the threshold — stop the worker).
#[derive(Clone, Copy, PartialEq, Eq)]
enum SinkOutcome {
    Ok,
    TransientError,
    Fatal,
}

impl SinkOutcome {
    fn is_break(self) -> bool {
        self == SinkOutcome::Fatal
    }
    fn failed(self) -> bool {
        self != SinkOutcome::Ok
    }
}

/// Writes the buffered batch to the sink without flushing. A failed batch is
/// dropped (counted + ledgered for the `dropped` marker) — transient sink
/// errors must not wedge the queue or kill the writer.
fn write_batch<T, S>(sink: &mut S, batch: &mut Vec<T>, ctx: &mut WorkerCtx<T>) -> SinkOutcome
where
    S: RecordSink<T>,
{
    if batch.is_empty() {
        return SinkOutcome::Ok;
    }
    let count = batch.len() as u64;
    match sink.write_batch(batch) {
        Ok(()) => {
            ctx.stats
                .records_written
                .fetch_add(count, Ordering::Relaxed);
            ctx.records_since_flush = ctx.records_since_flush.saturating_add(count);
            ctx.consecutive_errors = 0;
            if let Some(seq_of) = &ctx.seq_of {
                for record in batch.iter() {
                    ctx.last_written_seq = ctx.last_written_seq.max(seq_of(record));
                }
            }
            batch.clear();
            SinkOutcome::Ok
        }
        Err(_) => {
            ctx.stats
                .records_dropped
                .fetch_add(count, Ordering::Relaxed);
            if let Some(seq_of) = &ctx.seq_of {
                if let (Some(first), Some(last)) = (batch.first(), batch.last()) {
                    ctx.drops.push_range(seq_of(first), seq_of(last));
                }
            }
            batch.clear();
            if ctx.sink_failed() {
                SinkOutcome::Fatal
            } else {
                SinkOutcome::TransientError
            }
        }
    }
}

/// Explicitly flushes the sink; on success emits the `dropped` (if any drops
/// accumulated) and `checkpoint` markers, so loss accounting rides the same
/// transport as the data.
fn flush_sink<T, S>(sink: &mut S, ctx: &mut WorkerCtx<T>) -> SinkOutcome
where
    S: RecordSink<T>,
{
    match sink.flush() {
        Ok(()) => {
            ctx.stats.flushes.fetch_add(1, Ordering::Relaxed);
            ctx.records_since_flush = 0;
            // NOTE: only a successful write_batch resets the failure streak —
            // a sink whose writes fail but whose flush succeeds is still dead.
            let dropped = ctx.drops.drain();
            if !dropped.is_empty() {
                let _ = sink.write_marker(
                    MarkerKind::Dropped,
                    &serde_json::json!({ "ranges": dropped }),
                );
            }
            let _ = sink.write_marker(MarkerKind::Checkpoint, &marker_payload(ctx));
            SinkOutcome::Ok
        }
        Err(_) => {
            if ctx.sink_failed() {
                SinkOutcome::Fatal
            } else {
                SinkOutcome::TransientError
            }
        }
    }
}

fn emit_eof_marker<T, S>(sink: &mut S, ctx: &WorkerCtx<T>)
where
    S: RecordSink<T>,
{
    let _ = sink.write_marker(MarkerKind::Eof, &marker_payload(ctx));
}

fn marker_payload<T>(ctx: &WorkerCtx<T>) -> serde_json::Value {
    serde_json::json!({
        "last_seq": ctx.last_written_seq,
        "records_written": ctx.stats.records_written.load(Ordering::Relaxed),
        "records_dropped": ctx.stats.records_dropped.load(Ordering::Relaxed),
    })
}

fn drain_after_sink_failure<T>(receiver: &mpsc::Receiver<WriterMessage<T>>, stats: &WriterStats) {
    while let Ok(message) = receiver.try_recv() {
        match message {
            WriterMessage::Record(_) => {
                stats.queued_records.fetch_sub(1, Ordering::Relaxed);
                stats.inactive_records.fetch_add(1, Ordering::Relaxed);
            }
            WriterMessage::Flush(reply) | WriterMessage::Shutdown(reply) => {
                let _ = reply.send(Ok(()));
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;

    #[derive(Debug, Serialize)]
    struct TestRecord {
        id: u64,
    }

    #[test]
    fn jsonl_writer_flushes_queued_records() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("events.jsonl");
        let sink = JsonlSink::new(&path).expect("sink");
        let writer = AsyncRecordWriter::new(
            sink,
            WriterConfig {
                queue_capacity: 2,
                batch_size: 8,
                flush_interval: Duration::from_secs(60),
                flush_timeout: Duration::from_secs(30),
                flush_after_records: None,
                policy: SinkPolicy::Block,
            },
        );

        assert!(writer.record(TestRecord { id: 1 }));
        assert!(writer.record(TestRecord { id: 2 }));
        writer.flush().expect("flush");

        let content = std::fs::read_to_string(path).expect("jsonl");
        let lines = content.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2);
        assert!(lines[0].contains("\"id\":1"));
        assert!(lines[1].contains("\"id\":2"));

        let stats = writer.stats();
        assert_eq!(stats.attempted_records, 2);
        assert_eq!(stats.enqueued_records, 2);
        assert_eq!(stats.records_written, 2);
    }

    #[test]
    fn dead_sink_disables_writer_after_fatal_streak_without_panicking() {
        struct FailingSink;

        impl RecordSink<TestRecord> for FailingSink {
            fn write_batch(&mut self, _records: &[TestRecord]) -> io::Result<()> {
                Err(io::Error::other("synthetic sink failure"))
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let writer = AsyncRecordWriter::new(
            FailingSink,
            WriterConfig {
                queue_capacity: 64,
                batch_size: 1,
                flush_interval: Duration::from_millis(1),
                flush_timeout: Duration::from_secs(30),
                flush_after_records: None,
                policy: SinkPolicy::Block,
            },
        );

        // Each record is its own failing batch; the writer tolerates the
        // first failures as transient, then classifies the sink as dead.
        for id in 0..(FATAL_CONSECUTIVE_SINK_ERRORS as u64 + 4) {
            writer.record(TestRecord { id });
            std::thread::sleep(Duration::from_millis(2));
        }

        for _ in 0..500 {
            if !writer.is_active() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }

        assert!(!writer.is_active());
        assert!(!writer.record(TestRecord { id: 999 }));
        assert!(writer.stats().write_errors >= FATAL_CONSECUTIVE_SINK_ERRORS as u64);
        assert!(writer.stats().records_dropped >= FATAL_CONSECUTIVE_SINK_ERRORS as u64);
    }

    #[test]
    fn transient_sink_failure_drops_batch_but_writer_recovers() {
        struct FlakySink {
            failures_left: u32,
            written: Arc<AtomicU64>,
        }

        impl RecordSink<TestRecord> for FlakySink {
            fn write_batch(&mut self, records: &[TestRecord]) -> io::Result<()> {
                if self.failures_left > 0 {
                    self.failures_left -= 1;
                    return Err(io::Error::other("transient"));
                }
                self.written
                    .fetch_add(records.len() as u64, Ordering::Relaxed);
                Ok(())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let written = Arc::new(AtomicU64::new(0));
        let writer = AsyncRecordWriter::with_seq_of(
            FlakySink {
                failures_left: 2,
                written: Arc::clone(&written),
            },
            WriterConfig {
                queue_capacity: 64,
                batch_size: 1,
                flush_interval: Duration::from_secs(60),
                flush_timeout: Duration::from_secs(30),
                flush_after_records: None,
                policy: SinkPolicy::Block,
            },
            Some(Arc::new(|r: &TestRecord| r.id)),
        );

        for id in 1..=4 {
            assert!(writer.record(TestRecord { id }));
        }
        writer.flush().expect("flush after recovery");

        assert!(writer.is_active(), "transient errors must not disable");
        let stats = writer.stats();
        assert_eq!(stats.records_dropped, 2, "two failed single-record batches");
        assert_eq!(written.load(Ordering::Relaxed), 2, "the rest landed");
        assert_eq!(stats.write_errors, 2);
    }

    #[test]
    fn fail_open_drops_at_enqueue_and_markers_carry_the_loss() {
        #[derive(Default)]
        struct MarkerLog {
            markers: Vec<(MarkerKind, serde_json::Value)>,
        }

        /// Blocks inside write_batch until released, so the queue backs up
        /// deterministically; records markers for inspection.
        struct GatedSink {
            gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
            log: Arc<Mutex<MarkerLog>>,
        }

        impl RecordSink<TestRecord> for GatedSink {
            fn write_batch(&mut self, _records: &[TestRecord]) -> io::Result<()> {
                let (lock, cvar) = &*self.gate;
                let mut open = lock.lock().expect("gate");
                while !*open {
                    open = cvar.wait(open).expect("gate wait");
                }
                Ok(())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }

            fn write_marker(
                &mut self,
                kind: MarkerKind,
                payload: &serde_json::Value,
            ) -> io::Result<()> {
                self.log
                    .lock()
                    .expect("marker log")
                    .markers
                    .push((kind, payload.clone()));
                Ok(())
            }
        }

        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let log = Arc::new(Mutex::new(MarkerLog::default()));
        let writer = AsyncRecordWriter::with_seq_of(
            GatedSink {
                gate: Arc::clone(&gate),
                log: Arc::clone(&log),
            },
            WriterConfig {
                queue_capacity: 1,
                batch_size: 1,
                flush_interval: Duration::from_secs(60),
                flush_timeout: Duration::from_secs(30),
                flush_after_records: None,
                policy: SinkPolicy::FailOpen,
            },
            Some(Arc::new(|r: &TestRecord| r.id)),
        );

        // First record: worker picks it up and blocks inside the sink.
        assert!(writer.record(TestRecord { id: 1 }));
        std::thread::sleep(Duration::from_millis(30));
        // Second record sits in the queue (capacity 1)...
        assert!(writer.record(TestRecord { id: 2 }));
        // ...so the third hits a full queue → fail-open DROP, no blocking.
        assert!(!writer.record(TestRecord { id: 3 }));
        assert!(!writer.record(TestRecord { id: 4 }));
        assert!(writer.is_active(), "fail-open drops never disable");
        assert_eq!(writer.stats().records_dropped, 2);

        // Release the sink, flush → dropped + checkpoint markers emitted.
        {
            let (lock, cvar) = &*gate;
            *lock.lock().expect("gate") = true;
            cvar.notify_all();
        }
        writer.flush().expect("flush");

        let markers = log.lock().expect("marker log").markers.clone();
        let dropped = markers
            .iter()
            .find(|(kind, _)| *kind == MarkerKind::Dropped)
            .expect("dropped marker present");
        assert_eq!(dropped.1["ranges"], serde_json::json!([[3, 4]]));
        assert!(
            markers
                .iter()
                .any(|(kind, _)| *kind == MarkerKind::Checkpoint),
            "checkpoint marker after flush"
        );
        drop(writer);
        let markers = log.lock().expect("marker log").markers.clone();
        assert!(
            markers.iter().any(|(kind, _)| *kind == MarkerKind::Eof),
            "eof marker on shutdown"
        );
    }

    #[test]
    fn composite_sink_failing_secondary_does_not_poison_primary() {
        struct FailingSink;

        impl RecordSink<i32> for FailingSink {
            fn write_batch(&mut self, _records: &[i32]) -> io::Result<()> {
                Err(io::Error::other("synthetic secondary failure"))
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("primary.jsonl");
        let primary: Box<dyn RecordSink<i32>> =
            Box::new(JsonlSink::<i32>::new(&path).expect("primary sink"));
        let mut composite =
            CompositeSink::<i32>::new(primary).with_secondary(Box::new(FailingSink));
        let counter = composite.secondary_failure_counter();

        let records: Vec<i32> = (0..5).collect();
        composite
            .write_batch(&records)
            .expect("primary write should succeed despite failing secondary");
        composite.flush().expect("primary flush should succeed");

        let content = std::fs::read_to_string(&path).expect("primary jsonl");
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 5);
        for (line, expected) in lines.iter().zip(records.iter()) {
            assert_eq!(line.trim(), expected.to_string());
        }
        assert_eq!(counter.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn flush_decoupled_from_batch() {
        #[derive(Default)]
        struct CountingSinkInner {
            write_calls: u64,
            flush_calls: u64,
            records: u64,
        }

        struct CountingSink {
            inner: Arc<Mutex<CountingSinkInner>>,
        }

        impl RecordSink<TestRecord> for CountingSink {
            fn write_batch(&mut self, records: &[TestRecord]) -> io::Result<()> {
                let mut inner = self.inner.lock().expect("counting sink lock");
                inner.write_calls += 1;
                inner.records += records.len() as u64;
                Ok(())
            }

            fn flush(&mut self) -> io::Result<()> {
                let mut inner = self.inner.lock().expect("counting sink lock");
                inner.flush_calls += 1;
                Ok(())
            }
        }

        let inner = Arc::new(Mutex::new(CountingSinkInner::default()));
        let sink = CountingSink {
            inner: Arc::clone(&inner),
        };
        let writer = AsyncRecordWriter::new(
            sink,
            WriterConfig {
                queue_capacity: 32,
                // Batch size of 1 forces one write per record so we observe
                // batch writes without triggering the periodic flush timer.
                batch_size: 1,
                flush_interval: Duration::from_secs(60),
                flush_timeout: Duration::from_secs(30),
                flush_after_records: None,
                policy: SinkPolicy::Block,
            },
        );

        for id in 0..5 {
            assert!(writer.record(TestRecord { id }));
        }

        // Give the worker time to drain all records into the sink. We poll the
        // shared sink state so we can observe writes without forcing a flush.
        for _ in 0..200 {
            if inner.lock().unwrap().records == 5 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        {
            let state = inner.lock().expect("read counting sink before flush");
            assert_eq!(state.records, 5, "all records should have been written");
            assert_eq!(
                state.flush_calls, 0,
                "sink.flush() must NOT be called per batch"
            );
            assert!(
                state.write_calls >= 1,
                "sink.write_batch() should have been called at least once",
            );
        }

        writer.flush().expect("explicit flush");

        {
            let state = inner.lock().expect("read counting sink after flush");
            assert_eq!(
                state.flush_calls, 1,
                "explicit flush should trigger exactly one sink.flush()"
            );
        }
    }

    /// `request_flush` must NEVER block the calling (request) thread, even
    /// when the writer channel is completely full behind a wedged sink: a
    /// blocking enqueue would stall request threads behind the sink for as
    /// long as the sink stays wedged.
    #[test]
    fn request_flush_never_blocks_on_a_full_queue() {
        /// Blocks inside write_batch until released — the "saturated broker".
        struct WedgedSink {
            gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
        }

        impl RecordSink<TestRecord> for WedgedSink {
            fn write_batch(&mut self, _records: &[TestRecord]) -> io::Result<()> {
                let (lock, cvar) = &*self.gate;
                let mut open = lock.lock().expect("gate");
                while !*open {
                    open = cvar.wait(open).expect("gate wait");
                }
                Ok(())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let writer = AsyncRecordWriter::with_seq_of(
            WedgedSink {
                gate: Arc::clone(&gate),
            },
            WriterConfig {
                queue_capacity: 1,
                batch_size: 1,
                flush_interval: Duration::from_secs(60),
                flush_timeout: Duration::from_secs(30),
                flush_after_records: None,
                policy: SinkPolicy::FailOpen,
            },
            Some(Arc::new(|r: &TestRecord| r.id)),
        );

        // Worker picks up record 1 and wedges inside the sink; record 2 fills
        // the capacity-1 queue; record 3 fail-open drops. Channel is now FULL.
        assert!(writer.record(TestRecord { id: 1 }));
        std::thread::sleep(Duration::from_millis(30));
        assert!(writer.record(TestRecord { id: 2 }));
        assert!(!writer.record(TestRecord { id: 3 }));

        // Safety valve: release the wedge after 300ms so a blocking
        // request_flush unblocks and the test FAILS with a measurement
        // instead of hanging forever.
        let release_gate = Arc::clone(&gate);
        let releaser = thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            let (lock, cvar) = &*release_gate;
            *lock.lock().expect("gate") = true;
            cvar.notify_all();
        });

        let start = std::time::Instant::now();
        writer.request_flush();
        let elapsed = start.elapsed();

        releaser.join().expect("releaser");
        assert!(
            elapsed < Duration::from_millis(50),
            "request_flush blocked the caller for {elapsed:?} on a full queue \
             (must be fire-and-forget even under sink saturation)"
        );
    }
}
