//! Runtime-independent context carrier for Déjà causal attribution.
//!
//! This crate intentionally does not depend on Tokio. Runtime integrations can call
//! these functions from task hooks to capture, enter, and adopt business/request
//! context without creating dependency cycles with framework-specific integration
//! crates.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock, RwLock};
use std::task::{Context, Poll};

use pin_project_lite::pin_project;

/// The per-correlation recording decision.
///
/// Today this is a binary record/skip, but it is an enum — not a bare `bool` —
/// so context-aware sampling resolved server-side in Superposition (sampling
/// rates, per-boundary selection, experiment arms) can extend it later without
/// re-plumbing the carrier through the context registry and task snapshots.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecordDecision {
    /// Record no boundary on this correlation.
    Skip,
    /// Record every boundary on this correlation.
    Record,
}

impl RecordDecision {
    /// Whether the recording hook should record boundaries under this decision.
    #[inline]
    pub fn should_record(self) -> bool {
        matches!(self, Self::Record)
    }
}

impl From<bool> for RecordDecision {
    #[inline]
    fn from(record: bool) -> Self {
        if record {
            Self::Record
        } else {
            Self::Skip
        }
    }
}

/// A captured causal context.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ContextSnapshot {
    correlation_id: Option<String>,
    recording_decision: Option<RecordDecision>,
}

impl ContextSnapshot {
    /// Create an empty context snapshot.
    pub fn empty() -> Self {
        Self {
            correlation_id: None,
            recording_decision: None,
        }
    }

    /// Create a context snapshot containing the provided correlation ID.
    pub fn new(correlation_id: impl Into<String>) -> Self {
        let correlation_id = correlation_id.into();
        let recording_decision = snapshot_recording_decision_for(&correlation_id);
        Self {
            correlation_id: Some(correlation_id),
            recording_decision,
        }
    }

    /// Return the correlation ID, if present.
    pub fn correlation_id(&self) -> Option<&str> {
        self.correlation_id.as_deref()
    }

    /// Return the propagated recording decision, if one was captured.
    pub fn recording_decision(&self) -> Option<RecordDecision> {
        self.recording_decision
    }

    /// Attach a recording decision to this snapshot.
    pub fn with_recording_decision(mut self, record: impl Into<RecordDecision>) -> Self {
        self.recording_decision = Some(record.into());
        self
    }

    /// Return true when the snapshot carries no correlation ID and no recording decision.
    pub fn is_empty(&self) -> bool {
        self.correlation_id.is_none() && self.recording_decision.is_none()
    }
}

thread_local! {
    /// Thread-visible context read by syscall/preload hooks.
    static CURRENT_CONTEXT: RefCell<Option<String>> = const { RefCell::new(None) };

    /// Per-correlation recording decision captured with the current context.
    /// `Some(RecordDecision::Skip)` must survive registry cleanup while spawned
    /// work is still running, so it lives independently of the global decision map.
    static CURRENT_RECORDING_DECISION: RefCell<Option<RecordDecision>> = const { RefCell::new(None) };

    /// Tokio task currently being polled on this OS thread, if Tokio has called
    /// the runtime task-hook entry point.
    static CURRENT_TASK_ID: RefCell<Option<String>> = const { RefCell::new(None) };

    /// Stack used to restore previous thread-visible context around nested poll
    /// hook calls.
    static POLL_STACK: RefCell<Vec<PollFrame>> = const { RefCell::new(Vec::new()) };
}

#[derive(Clone, Debug)]
struct PollFrame {
    previous_task_id: Option<String>,
    previous_context: ContextSnapshot,
}

static TASK_CONTEXTS: OnceLock<Mutex<HashMap<String, ContextSnapshot>>> = OnceLock::new();

fn task_contexts() -> &'static Mutex<HashMap<String, ContextSnapshot>> {
    TASK_CONTEXTS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn set_current_context(snapshot: &ContextSnapshot) {
    CURRENT_CONTEXT.with(|cell| {
        *cell.borrow_mut() = snapshot.correlation_id.clone();
    });
    CURRENT_RECORDING_DECISION.with(|cell| {
        *cell.borrow_mut() = snapshot.recording_decision;
    });
}

fn clear_current_context() {
    CURRENT_CONTEXT.with(|cell| {
        *cell.borrow_mut() = None;
    });
    CURRENT_RECORDING_DECISION.with(|cell| {
        *cell.borrow_mut() = None;
    });
}

/// Return the current thread-visible correlation ID.
pub fn current_correlation_id() -> Option<String> {
    CURRENT_CONTEXT.with(|cell| cell.borrow().clone())
}

fn current_recording_decision() -> Option<RecordDecision> {
    CURRENT_RECORDING_DECISION.with(|cell| *cell.borrow())
}

fn snapshot_recording_decision_for(correlation_id: &str) -> Option<RecordDecision> {
    let current_id_matches = current_correlation_id().as_deref() == Some(correlation_id);
    if current_id_matches {
        if let Some(record) = current_recording_decision() {
            return Some(record);
        }
    }
    recording_decision(correlation_id)
}

/// Capture the current thread-visible context.
pub fn capture_current() -> ContextSnapshot {
    let correlation_id = current_correlation_id();
    let recording_decision = current_recording_decision()
        .or_else(|| correlation_id.as_deref().and_then(recording_decision));
    ContextSnapshot {
        correlation_id,
        recording_decision,
    }
}

// ---------------------------------------------------------------------------
// Per-request recording decision registry (the sampling gate)
// ---------------------------------------------------------------------------

/// Stays `false` until a sampler first pushes a decision. The common path — no
/// sampler installed (e.g. the demo / matrix) — then pays a single relaxed
/// atomic load and never locks the registry, so recording behaves exactly as it
/// did before sampling existed.
static SAMPLER_ENGAGED: AtomicBool = AtomicBool::new(false);

/// `correlation_id -> record?`, populated at ingress by the host's sampler
/// (e.g. Hyperswitch resolving `deja_record_enabled` from Superposition). The
/// registry is a bounded ingress/teardown fallback; context snapshots freeze a
/// resolved decision so spawned work can keep an explicit `false` after teardown.
// A read-mostly `RwLock`, not a `Mutex`: the hot path is the per-boundary READ
// (`recording_decision_for_current`), which takes a shared read lock that never
// contends with other readers; writes happen only at ingress/teardown. Combined
// with the `SAMPLER_ENGAGED` fast-path, an un-sampled process touches neither.
static RECORD_DECISIONS: OnceLock<RwLock<HashMap<String, RecordDecision>>> = OnceLock::new();

fn record_decisions() -> &'static RwLock<HashMap<String, RecordDecision>> {
    RECORD_DECISIONS.get_or_init(|| RwLock::new(HashMap::new()))
}

/// Push the per-request recording decision for `correlation_id`.
///
/// The host decides *whether* to record (rate, targeting, experiments — all
/// server-side in Superposition) and pushes the resolved decision here at
/// ingress. Déjà only consumes it: `Skip` makes the recording hook a no-op for
/// every boundary on this correlation (gate-before-allocation); `Record` records
/// as usual. Recording is opt-in — with no decision the gate skips (see
/// [`recording_decision_for_current`]), so the read that *computes* the decision
/// (the Superposition sampler fetch, which runs before this is called)
/// self-excludes. Accepts a `bool` (via `From`) so binary call-sites stay terse.
pub fn set_recording_decision(
    correlation_id: impl Into<String>,
    decision: impl Into<RecordDecision>,
) {
    let correlation_id = correlation_id.into();
    let decision = decision.into();
    SAMPLER_ENGAGED.store(true, Ordering::Relaxed);
    if let Ok(mut map) = record_decisions().write() {
        map.insert(correlation_id.clone(), decision);
    }
    if current_correlation_id().as_deref() == Some(correlation_id.as_str()) {
        CURRENT_RECORDING_DECISION.with(|cell| {
            *cell.borrow_mut() = Some(decision);
        });
    }
}

/// Drop the decision for `correlation_id` (call at request teardown to bound the
/// registry).
pub fn clear_recording_decision(correlation_id: &str) {
    if SAMPLER_ENGAGED.load(Ordering::Relaxed) {
        if let Ok(mut map) = record_decisions().write() {
            map.remove(correlation_id);
        }
    }
}

/// The decision for an explicit `correlation_id`, or `None` if no sampler is
/// engaged or none was set.
pub fn recording_decision(correlation_id: &str) -> Option<RecordDecision> {
    if !SAMPLER_ENGAGED.load(Ordering::Relaxed) {
        return None;
    }
    record_decisions()
        .read()
        .ok()
        .and_then(|map| map.get(correlation_id).copied())
}

/// The decision for the current correlation, or `None` when no sampler is
/// engaged or the current correlation has none.
///
/// Hot-path gate:
/// `recording_decision_for_current().map(RecordDecision::should_record).unwrap_or(false)`
/// — recording is opt-in. `None` (no decision yet, or an orphan boundary with no
/// live correlation) skips; only an explicit `Record` pushed at ingress records.
/// This is what makes the sampler's own Superposition read self-exclude: it runs
/// before the decision is set, sees `None`, and is not recorded.
pub fn recording_decision_for_current() -> Option<RecordDecision> {
    if let Some(record) = current_recording_decision() {
        return Some(record);
    }
    if !SAMPLER_ENGAGED.load(Ordering::Relaxed) {
        return None;
    }
    let correlation_id = current_correlation_id()?;
    recording_decision(&correlation_id)
}

/// Enter a context for the lifetime of the returned guard.
pub fn enter(snapshot: ContextSnapshot) -> ContextGuard {
    let previous = capture_current();
    set_current_context(&snapshot);
    ContextGuard { previous }
}

/// Enter a correlation ID for the lifetime of the returned guard.
pub fn enter_correlation_id(correlation_id: impl Into<String>) -> ContextGuard {
    enter(ContextSnapshot::new(correlation_id))
}

/// Guard that restores the previous thread-visible context on drop.
#[derive(Debug)]
pub struct ContextGuard {
    previous: ContextSnapshot,
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        set_current_context(&self.previous);
    }
}

pin_project! {
    /// Future wrapper that enters a context for each poll only.
    pub struct ContextScopeFuture<F> {
        context: ContextSnapshot,
        #[pin]
        inner: F,
    }
}

impl<F> ContextScopeFuture<F> {
    /// Create a new context-scoped future.
    pub fn new(context: ContextSnapshot, inner: F) -> Self {
        Self { context, inner }
    }
}

impl<F: Future> Future for ContextScopeFuture<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let _guard = enter(this.context.clone());
        this.inner.poll(cx)
    }
}

/// Scope a future with a correlation ID for each poll.
pub fn scope<F>(correlation_id: impl Into<String>, inner: F) -> ContextScopeFuture<F> {
    ContextScopeFuture::new(ContextSnapshot::new(correlation_id), inner)
}

/// Scope a future with an existing snapshot for each poll.
pub fn scope_snapshot<F>(context: ContextSnapshot, inner: F) -> ContextScopeFuture<F> {
    ContextScopeFuture::new(context, inner)
}

/// Run synchronous code inside a context.
pub fn scope_sync<F, R>(context: ContextSnapshot, f: F) -> R
where
    F: FnOnce() -> R,
{
    let _guard = enter(context);
    f()
}

/// Tokio hook entry point: a task was spawned.
pub fn tokio_task_spawn(task_id: impl ToString) {
    let task_id = task_id.to_string();
    let context = capture_current();
    if let Ok(mut contexts) = task_contexts().lock() {
        if context.is_empty() {
            contexts.remove(&task_id);
        } else {
            contexts.insert(task_id, context);
        }
    }
}

/// Tokio hook entry point: a task is about to be polled.
pub fn tokio_task_poll_start(task_id: impl ToString) {
    let task_id = task_id.to_string();
    let previous_task_id = CURRENT_TASK_ID.with(|cell| {
        let previous = cell.borrow().clone();
        *cell.borrow_mut() = Some(task_id.clone());
        previous
    });

    let previous_context = capture_current();

    POLL_STACK.with(|stack| {
        stack.borrow_mut().push(PollFrame {
            previous_task_id,
            previous_context,
        });
    });

    let context = task_contexts()
        .lock()
        .ok()
        .and_then(|contexts| contexts.get(&task_id).cloned())
        .unwrap_or_else(ContextSnapshot::empty);

    set_current_context(&context);
}

/// Tokio hook entry point: a task poll finished.
pub fn tokio_task_poll_stop(_task_id: impl ToString) {
    let frame = POLL_STACK.with(|stack| stack.borrow_mut().pop());

    if let Some(frame) = frame {
        CURRENT_TASK_ID.with(|cell| {
            *cell.borrow_mut() = frame.previous_task_id;
        });
        set_current_context(&frame.previous_context);
    } else {
        CURRENT_TASK_ID.with(|cell| {
            *cell.borrow_mut() = None;
        });
        clear_current_context();
    }
}

/// Tokio hook entry point: a task terminated.
pub fn tokio_task_terminate(task_id: impl ToString) {
    let task_id = task_id.to_string();
    if let Ok(mut contexts) = task_contexts().lock() {
        contexts.remove(&task_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_and_enter_restore_context() {
        assert_eq!(current_correlation_id(), None);
        {
            let _guard = enter_correlation_id("req-1");
            assert_eq!(current_correlation_id().as_deref(), Some("req-1"));
        }
        assert_eq!(current_correlation_id(), None);
    }

    #[test]
    fn sampling_gate_push_resolve_and_clear() {
        // Pushing a decision engages the registry and records it per correlation.
        set_recording_decision("req-off", false);
        set_recording_decision("req-on", true);
        assert_eq!(recording_decision("req-off"), Some(RecordDecision::Skip));
        assert_eq!(recording_decision("req-on"), Some(RecordDecision::Record));
        // An unknown correlation has no decision → caller skips by default (opt-in).
        assert_eq!(recording_decision("req-unknown-zzz"), None);

        // The current-correlation resolver reads the ambient correlation id.
        {
            let _g = enter_correlation_id("req-off");
            assert_eq!(recording_decision_for_current(), Some(RecordDecision::Skip));
        }
        {
            let _g = enter_correlation_id("req-on");
            assert_eq!(recording_decision_for_current(), Some(RecordDecision::Record));
        }

        // Clearing bounds the registry; the gate then falls back to default.
        clear_recording_decision("req-off");
        assert_eq!(recording_decision("req-off"), None);
        clear_recording_decision("req-on");
    }

    #[test]
    fn spawned_context_preserves_false_decision_after_registry_cleanup() {
        let correlation_id = "req-propagated-off";
        let task_id = "task-propagated-off";

        set_recording_decision(correlation_id, false);
        {
            let _guard = enter_correlation_id(correlation_id);
            let snapshot = capture_current();
            assert_eq!(snapshot.correlation_id(), Some(correlation_id));
            assert_eq!(snapshot.recording_decision(), Some(RecordDecision::Skip));
            tokio_task_spawn(task_id);
        }

        clear_recording_decision(correlation_id);
        assert_eq!(recording_decision(correlation_id), None);

        tokio_task_poll_start(task_id);
        assert_eq!(current_correlation_id().as_deref(), Some(correlation_id));
        assert_eq!(recording_decision_for_current(), Some(RecordDecision::Skip));
        tokio_task_poll_stop(task_id);
        tokio_task_terminate(task_id);
    }

    #[test]
    fn decision_only_snapshot_is_propagated_to_spawned_task() {
        let task_id = "task-decision-only-off";

        {
            let _guard = enter(ContextSnapshot::empty().with_recording_decision(false));
            assert_eq!(current_correlation_id(), None);
            assert_eq!(recording_decision_for_current(), Some(RecordDecision::Skip));
            tokio_task_spawn(task_id);
        }

        tokio_task_poll_start(task_id);
        assert_eq!(current_correlation_id(), None);
        assert_eq!(recording_decision_for_current(), Some(RecordDecision::Skip));
        tokio_task_poll_stop(task_id);
        tokio_task_terminate(task_id);
    }
}
