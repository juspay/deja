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

    /// The recording decision on this thread, **and the correlation it belongs
    /// to**. `Some(RecordDecision::Skip)` must survive registry cleanup while
    /// spawned work is still running, so it lives independently of the global
    /// decision map.
    ///
    /// The owner is stored rather than implied, and that is load-bearing. This
    /// cell was once a bare `Option<RecordDecision>` documented as
    /// per-correlation while carrying no correlation, so a reader wanting to
    /// know whether the decision was its own had nothing to compare against and
    /// had to reconstruct the owner from thread state — the same thread state
    /// that had already moved on. A guard written that way compared a value
    /// derived from `current_correlation_id()` against `current_correlation_id()`
    /// and was unconditionally true; a build carrying it recorded every health
    /// check the pod served, because a worker thread applied one request's
    /// decision to whatever it polled next.
    ///
    /// With the owner in the cell there is nothing to reconstruct and nothing to
    /// get wrong: a reader supplies the correlation it is asking about, and a
    /// decision belonging to another one is simply not returned.
    static CURRENT_RECORDING_DECISION: RefCell<Option<(String, RecordDecision)>> =
        const { RefCell::new(None) };

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
    // A decision is installed only together with the correlation it speaks for.
    // A snapshot carrying a decision and no correlation names nobody, so it
    // installs nothing rather than installing something unattributable.
    CURRENT_RECORDING_DECISION.with(|cell| {
        *cell.borrow_mut() = snapshot
            .correlation_id
            .clone()
            .zip(snapshot.recording_decision);
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

/// What is physically in the cell, owner and all. Tests only: production code
/// must name the correlation it is asking about, which is the whole point of
/// the shape.
#[cfg(test)]
fn installed_decision() -> Option<(String, RecordDecision)> {
    CURRENT_RECORDING_DECISION.with(|cell| cell.borrow().clone())
}

/// The decision on this thread **if it belongs to `correlation_id`**.
///
/// There is no way to ask this question without naming a correlation, which is
/// the point: the previous shape allowed a caller to take the decision and
/// decide for itself whether it was entitled to it, and two callers decided
/// differently.
fn current_recording_decision_for(correlation_id: &str) -> Option<RecordDecision> {
    CURRENT_RECORDING_DECISION.with(|cell| {
        cell.borrow()
            .as_ref()
            .filter(|(owner, _)| owner == correlation_id)
            .map(|(_, decision)| *decision)
    })
}

/// The recording decision that belongs to `correlation_id`.
///
/// This is the ONE ownership rule. It is the **sole reader** of
/// [`current_recording_decision_for`] outside this crate's tests — the capture gate
/// ([`recording_decision_for_current`]) and the snapshot constructors
/// ([`capture_current`], [`ContextSnapshot::new`]) all resolve through here, so
/// they cannot drift apart. Keep it that way: a second direct read of the
/// thread-local is how this cell produced two shipped defects, one at the gate
/// and one frozen into spawned-task snapshots.
///
/// The thread-local decision is authoritative only for the correlation the
/// thread is currently serving: [`set_current_context`] writes the correlation
/// and its decision together, and the one place that writes the decision alone
/// ([`set_recording_decision`]) does so only when that correlation is already
/// current. So the two thread-locals always describe the same request, and when
/// they match `correlation_id` the decision demonstrably describes *this* one.
/// For any other correlation the answer comes from the correlation-keyed
/// registry, which cannot be attributed to the wrong request.
fn snapshot_recording_decision_for(correlation_id: &str) -> Option<RecordDecision> {
    // Ownership is established by the cell, not by comparing thread state
    // against itself. The comparison this replaced took the correlation from
    // `current_correlation_id()` and then asked whether it equalled
    // `current_correlation_id()`, which is a tautology — so the thread-local was
    // returned to whoever asked, regardless of whose it was.
    if let Some(record) = current_recording_decision_for(correlation_id) {
        return Some(record);
    }
    recording_decision(correlation_id)
}

/// Capture the current thread-visible context.
///
/// The decision is resolved through [`snapshot_recording_decision_for`], the same
/// ownership rule the capture gate uses, so a snapshot carries a decision only
/// when there is a correlation for that decision to belong to. This matters more
/// here than at the gate: a wrong answer from
/// [`recording_decision_for_current`] is transient — the next context entry
/// overwrites the thread-local — but a wrong answer captured here is frozen into
/// `TASK_CONTEXTS` by [`tokio_task_spawn`] and restored by
/// [`tokio_task_poll_start`] for the whole life of that task, pairing one
/// request's correlation with another request's decision.
///
/// A capture taken with no live correlation therefore yields an empty snapshot,
/// which `tokio_task_spawn` stores as "no context" rather than freezing a
/// decision that names no request.
pub fn capture_current() -> ContextSnapshot {
    let correlation_id = current_correlation_id();
    let recording_decision = correlation_id
        .as_deref()
        .and_then(snapshot_recording_decision_for);
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
            *cell.borrow_mut() = Some((correlation_id.clone(), decision));
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

/// The decision for the correlation this thread is currently serving, or `None`
/// when there is no such correlation or it carries no decision.
///
/// Hot-path gate:
/// `recording_decision_for_current().map(RecordDecision::should_record).unwrap_or(false)`
/// — recording is opt-in. `None` (no decision yet, or an orphan boundary with no
/// live correlation) skips; only an explicit `Record` pushed at ingress records.
/// This is what makes the sampler's own Superposition read self-exclude: it runs
/// before the decision is set, sees `None`, and is not recorded.
///
/// # A live correlation is required, and that requirement IS the gate
///
/// A decision with no correlation to own it describes no request being served,
/// so it cannot authorize capture — it is exactly the "orphan boundary" the
/// paragraph above promises will skip. Without this, whatever decision was last
/// installed on a worker thread applies to whatever that thread polls next: a
/// build shipped that way recorded 100% of its health checks (298 calls, 298
/// recordings) while the sampler was consulted for none of them.
///
/// This does NOT reject legitimately sampled requests, because of the ingress
/// ordering it has to survive: the host pushes the decision into the registry
/// BEFORE the correlation is entered — the correlation arrives later, from the
/// request's root span. The decision crosses that gap in the registry, keyed by
/// correlation, never as a bare thread-local; entering the correlation resolves
/// it back out via [`ContextSnapshot::new`] and installs both together. Work
/// detached from the request keeps recording for the same reason: its snapshot
/// carries the correlation along with the decision.
///
/// # Why `SAMPLER_ENGAGED` does not gate the thread-local read
///
/// The flag is a fast path for the *registry* — the only part expensive to
/// consult — and [`recording_decision`] still checks it before taking the lock,
/// so an un-sampled process touches no map here either. But the flag is set by
/// [`set_recording_decision`] alone, and a decision already installed on the
/// context is just as explicit and just as owned. Gating on it would promote a
/// performance flag into a second semantic rule that must agree with this one —
/// the failure shape this crate keeps hitting — and would silently drop
/// recordings from any host that scopes a decision onto a context directly.
pub fn recording_decision_for_current() -> Option<RecordDecision> {
    // Ownership first: a decision on this thread may only speak for the request
    // the thread is actually serving.
    let correlation_id = current_correlation_id()?;
    snapshot_recording_decision_for(&correlation_id)
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

/// Set the thread-visible correlation directly, resolving its recording decision
/// from the registry; `None` clears it. Unlike [`enter_correlation_id`] this
/// returns no restore guard — the caller owns restoration. The correlation layer
/// uses it for an on-change model where the previous value is restored from the
/// span tree rather than an RAII guard per span.
///
/// # The entry may already be gone
///
/// A host typically drops the registry entry when the response is built, so a
/// caller reaching this afterwards resolves nothing and the correlation arrives
/// on the thread with no decision at all. The capture gate reads that as "no
/// decision" and skips — and a skip writes nothing, so the result cannot be told
/// apart from a correlation that had nothing to record. That is what makes this
/// failure hard to notice: not a wrong value, but an absence that looks exactly
/// like the legitimate empty case.
///
/// Use this only where the entry is known to be live. Work that outlives the
/// response should resolve the decision while it still is, and pass it to
/// [`set_current_correlation_with_decision`].
pub fn set_current_correlation(correlation_id: Option<&str>) {
    match correlation_id {
        Some(id) => set_current_context(&ContextSnapshot::new(id)),
        None => clear_current_context(),
    }
}

/// Install `correlation_id` on this thread together with the decision that
/// belongs to it, taking the decision from the caller instead of the registry.
///
/// [`set_current_correlation`] resolves the decision itself. That is right at
/// ingress and wrong everywhere after it: the host drops the registry entry when
/// the response is built, so a caller re-resolving later finds nothing, and the
/// correlation arrives on the thread with its authorization stripped. Work that
/// outlives the response then crosses boundaries with a correlation and no
/// decision, and the opt-in capture gate skips it — silently, since a skip
/// writes nothing.
///
/// A caller holding a decision it resolved WHILE the entry was live passes it
/// here rather than asking again. The span tree is the one such caller today: it
/// resolves a correlation's decision when the root span is created and can hand
/// that same answer back on every later poll, on any worker, for as long as
/// anything still claims to belong to the request.
///
/// `None` for `correlation_id` clears the context, exactly as
/// [`set_current_correlation`] does. `None` for `decision` installs a
/// correlation with no decision, which the gate reads as "no decision" and skips
/// — and a skip writes nothing, so it cannot be told apart from a correlation
/// that had nothing to record. Recording stays opt-in, and an absent decision
/// fails quiet rather than loud.
pub fn set_current_correlation_with_decision(
    correlation_id: Option<&str>,
    decision: Option<RecordDecision>,
) {
    match correlation_id {
        Some(id) => set_current_context(&ContextSnapshot {
            correlation_id: Some(id.to_owned()),
            recording_decision: decision,
        }),
        None => clear_current_context(),
    }
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

    /// The capture gate exactly as every boundary spells it
    /// (`crates/deja-runtime/src/lib.rs`, `RecordingHook::capture_verdict`).
    /// Asserting through this, not just on the `Option`, is what keeps these
    /// tests honest about the direction that matters: does a boundary record?
    fn gate() -> bool {
        recording_decision_for_current()
            .map(RecordDecision::should_record)
            .unwrap_or(false)
    }

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
            assert_eq!(
                recording_decision_for_current(),
                Some(RecordDecision::Record)
            );
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

    /// The writers are unchanged: entering a decision-only snapshot still puts
    /// that decision on the thread. What changed is every *reader* — it does not
    /// travel into a capture and it does not answer the gate, so it can neither
    /// be frozen into a task nor authorize a boundary. This pins the boundary
    /// between the two, which is what stops the orphan state from spreading.
    #[test]
    fn decision_only_snapshot_is_installed_but_never_travels_or_captures() {
        let task_id = "task-decision-only-off";

        {
            let _guard = enter(ContextSnapshot::empty().with_recording_decision(false));
            assert_eq!(current_correlation_id(), None);
            // Not installed at all: a decision naming no correlation owns
            // nothing, so the cell has nowhere to put it. Stronger than
            // installing it and relying on every reader to decline it.
            assert_eq!(installed_decision(), None);
            assert_eq!(capture_current().recording_decision(), None);
            assert_eq!(recording_decision_for_current(), None);
            assert!(!gate(), "an orphan boundary must not capture");
            tokio_task_spawn(task_id);
        }

        // Nothing was frozen, so the task inherits no decision either.
        tokio_task_poll_start(task_id);
        assert_eq!(current_correlation_id(), None);
        assert_eq!(installed_decision(), None);
        assert_eq!(recording_decision_for_current(), None);
        assert!(!gate());
        tokio_task_poll_stop(task_id);
        tokio_task_terminate(task_id);
    }

    /// The health-check leak, as a unit test. A `Record` decision is left on the
    /// worker with no correlation to own it — what an unbalanced span exit or a
    /// detached task's retained decision leaves behind before the thread polls
    /// the next request. The next boundary must not capture on it.
    #[test]
    fn orphan_boundary_does_not_capture_on_a_leftover_decision() {
        let correlation_id = "req-orphan-owner";
        set_recording_decision(correlation_id, true);

        // Earlier work on this thread was sampled IN and captured, as it should.
        {
            let _owner = enter_correlation_id(correlation_id);
            assert!(gate(), "the owning request must capture");
        }

        // Its decision outlives its correlation. This is the exact state the old
        // first branch trusted, and the reason 298 health calls became 298
        // recordings against 4 bucketing decisions.
        let _leaked = enter(ContextSnapshot::empty().with_recording_decision(true));
        assert_eq!(current_correlation_id(), None);
        assert_eq!(
            installed_decision(),
            None,
            "an unowned decision is not installed"
        );

        assert_eq!(
            recording_decision_for_current(),
            None,
            "a decision with no live correlation belongs to no request"
        );
        assert!(!gate(), "an orphan boundary must not capture");

        clear_recording_decision(correlation_id);
    }

    /// A decision resolved for one correlation must not answer for another. The
    /// thread is serving `owner` (sampled IN); a boundary attributed to `other`
    /// must fall through to the registry, where `other` has no decision.
    #[test]
    fn a_decision_does_not_answer_for_a_different_correlation() {
        let owner = "req-mismatch-owner";
        let other = "req-mismatch-other";
        set_recording_decision(owner, true);

        let _owner_guard = enter_correlation_id(owner);
        assert_eq!(
            current_recording_decision_for(owner),
            Some(RecordDecision::Record)
        );
        assert!(gate(), "the owner itself still captures");

        // Resolving for a different correlation does not borrow the owner's
        // `Record` off the thread-local.
        assert_eq!(ContextSnapshot::new(other).recording_decision(), None);
        assert_eq!(recording_decision(other), None);

        // And entering it — which is how a boundary comes to be served under
        // `other` — leaves the gate closed rather than inheriting `Record`.
        {
            let _other_guard = enter_correlation_id(other);
            assert_eq!(current_correlation_id().as_deref(), Some(other));
            assert_eq!(recording_decision_for_current(), None);
            assert!(!gate(), "another request's decision must not capture here");
        }

        // The owner's own decision is undisturbed by the excursion.
        assert!(gate());
        clear_recording_decision(owner);
    }

    /// The direction that proves recording was not broken outright: a request the
    /// sampler selected records end to end — through the real ingress ordering
    /// (decision into the registry BEFORE the correlation is entered), on into a
    /// spawned task, and past registry teardown.
    #[test]
    fn a_sampled_request_still_captures_end_to_end() {
        let correlation_id = "req-sampled-in-e2e";
        let task_id = "task-sampled-in-e2e";

        // Ingress, in the order the router actually does it: the host resolves
        // the decision and pushes it to the registry while the correlation is
        // not yet entered — it arrives later, off the request's root span.
        set_recording_decision(correlation_id, true);
        assert!(
            !gate(),
            "no correlation entered yet, so nothing captures on it"
        );

        // The correlation is entered; entering resolves the decision back out of
        // the registry and installs the two together.
        let guard = enter_correlation_id(correlation_id);
        assert_eq!(current_correlation_id().as_deref(), Some(correlation_id));
        assert_eq!(
            recording_decision_for_current(),
            Some(RecordDecision::Record)
        );
        assert!(gate(), "a sampled-in request MUST capture");

        // Work detached from the request keeps the decision, because its snapshot
        // carries the correlation with it.
        tokio_task_spawn(task_id);
        drop(guard);

        // Ingress tears down and the registry entry goes away.
        clear_recording_decision(correlation_id);
        assert_eq!(recording_decision(correlation_id), None);

        // The detached task still captures.
        tokio_task_poll_start(task_id);
        assert_eq!(current_correlation_id().as_deref(), Some(correlation_id));
        assert_eq!(
            recording_decision_for_current(),
            Some(RecordDecision::Record)
        );
        assert!(
            gate(),
            "detached work on a sampled request MUST still capture"
        );
        tokio_task_poll_stop(task_id);
        tokio_task_terminate(task_id);

        // And the thread is clean again once the task is off it.
        assert!(!gate());
    }

    /// `capture_current()` must not freeze a decision that names no request.
    /// This is the durable half of the leak: the gate's wrong answer is
    /// overwritten by the next context entry, but a wrong answer captured here
    /// is stored in `TASK_CONTEXTS` and restored for the whole life of the task.
    #[test]
    fn capture_with_no_live_correlation_carries_no_decision() {
        let task_id = "task-orphan-capture";

        // A `Record` left on the thread with no correlation to own it.
        let _leaked = enter(ContextSnapshot::empty().with_recording_decision(true));
        assert_eq!(current_correlation_id(), None);
        assert_eq!(installed_decision(), None);

        let snapshot = capture_current();
        assert_eq!(snapshot.correlation_id(), None);
        assert_eq!(
            snapshot.recording_decision(),
            None,
            "a snapshot with no correlation must carry no decision"
        );
        assert!(
            snapshot.is_empty(),
            "so tokio_task_spawn stores no context at all"
        );

        // And the spawn therefore freezes nothing a later poll could capture on.
        tokio_task_spawn(task_id);
        tokio_task_poll_start(task_id);
        assert_eq!(current_correlation_id(), None);
        assert_eq!(recording_decision_for_current(), None);
        assert!(!gate(), "a task spawned from an orphan must not capture");
        tokio_task_poll_stop(task_id);
        tokio_task_terminate(task_id);
    }

    /// The other direction on the same axis: a capture taken while genuinely
    /// serving a sampled request still carries `Record`, from the thread-local
    /// and from the registry alike. Tightening `capture_current` a second time
    /// is exactly where an empty tape would come from, so both are pinned.
    #[test]
    fn capture_under_a_sampled_request_still_carries_record() {
        let from_local = "req-capture-from-thread-local";
        let from_registry = "req-capture-from-registry";

        // Resolved onto the context, then dropped from the registry — the
        // detached-task retention. The snapshot must still carry `Record`.
        set_recording_decision(from_local, true);
        {
            let _guard = enter_correlation_id(from_local);
            clear_recording_decision(from_local);
            assert_eq!(recording_decision(from_local), None);
            let snapshot = capture_current();
            assert_eq!(snapshot.correlation_id(), Some(from_local));
            assert_eq!(
                snapshot.recording_decision(),
                Some(RecordDecision::Record),
                "a frozen decision MUST survive registry teardown"
            );
        }

        // Correlation entered before the sampler publishes: the capture falls
        // through to the registry rather than reporting no decision.
        set_current_correlation(Some(from_registry));
        assert_eq!(capture_current().recording_decision(), None);
        set_recording_decision(from_registry, true);
        assert_eq!(
            capture_current().recording_decision(),
            Some(RecordDecision::Record),
            "a capture MUST see a decision published after the correlation was entered"
        );
        clear_recording_decision(from_registry);
        set_current_correlation(None);
    }

    /// An explicit `Skip` still skips — both from the registry and once frozen
    /// onto the context, including after the registry entry is cleared.
    #[test]
    fn an_explicit_skip_still_skips() {
        let correlation_id = "req-explicit-skip";
        let task_id = "task-explicit-skip";

        set_recording_decision(correlation_id, false);
        {
            let _guard = enter_correlation_id(correlation_id);
            assert_eq!(recording_decision_for_current(), Some(RecordDecision::Skip));
            assert!(!gate(), "a sampled-out request must not capture");
            tokio_task_spawn(task_id);
        }

        clear_recording_decision(correlation_id);
        tokio_task_poll_start(task_id);
        assert_eq!(
            recording_decision_for_current(),
            Some(RecordDecision::Skip),
            "a frozen Skip must survive teardown, not decay to no-decision"
        );
        assert!(!gate());
        tokio_task_poll_stop(task_id);
        tokio_task_terminate(task_id);
    }

    /// The sampler's own Superposition read runs at ingress under the request's
    /// correlation but BEFORE any decision exists for it, so it sees `None` and
    /// self-excludes. Recording being opt-in is what makes that work; this pins
    /// it, because a fix that defaulted an unknown correlation to `Record` would
    /// make the sampler record itself.
    /// The measured production failure, reduced to the invariant it violated.
    ///
    /// A worker thread that had served a sampled request applied that request's
    /// `Record` to whatever it polled next, so 298 health calls became 298
    /// recordings behind 4 bucketing decisions. The guard added to stop it
    /// compared `current_correlation_id()` against a value taken from
    /// `current_correlation_id()` — a tautology — so a second build did the same.
    ///
    /// The invariant underneath both: a decision installed for one correlation
    /// must not answer for another. It is checkable now only because the cell
    /// carries its owner; with a bare decision there was nothing to compare.
    #[test]
    fn a_decision_installed_for_one_correlation_does_not_answer_for_another() {
        let payment = "corr-payment";
        let health = "corr-health";

        set_recording_decision(payment, true);
        let _serving = enter_correlation_id(payment);

        assert_eq!(
            installed_decision(),
            Some((payment.to_owned(), RecordDecision::Record)),
            "the decision is on the thread, owner and all"
        );
        assert_eq!(
            current_recording_decision_for(payment),
            Some(RecordDecision::Record),
            "the request that was selected still records"
        );
        assert_eq!(
            current_recording_decision_for(health),
            None,
            "another correlation must not be answered by this one's decision"
        );

        clear_recording_decision(payment);
    }

    /// The same probe that fails against the shipped gate, run against this one.
    /// A stale decision is physically on the thread, owned by another request.
    #[test]
    fn probe_foreign_decision_on_thread_does_not_answer_the_gate() {
        let payment = "corr-payment";
        let health = "corr-health";

        set_recording_decision(payment, true);
        let serving = enter_correlation_id(payment);
        assert_eq!(
            recording_decision_for_current(),
            Some(RecordDecision::Record)
        );
        drop(serving);
        clear_recording_decision(payment);

        // Exactly the state a worker thread is left in: the payment's decision
        // still present, the thread now serving a health check.
        CURRENT_RECORDING_DECISION
            .with(|c| *c.borrow_mut() = Some((payment.to_owned(), RecordDecision::Record)));
        CURRENT_CONTEXT.with(|c| *c.borrow_mut() = Some(health.to_owned()));

        assert_eq!(
            recording_decision_for_current(),
            None,
            "a decision owned by another correlation must not answer for this one"
        );
    }

    #[test]
    fn the_samplers_own_read_self_excludes() {
        let correlation_id = "req-sampler-self-exclude";

        let _guard = enter_correlation_id(correlation_id);
        assert_eq!(recording_decision(correlation_id), None);
        assert_eq!(recording_decision_for_current(), None);
        assert!(
            !gate(),
            "the read that computes the decision must not be recorded"
        );

        // Only once the sampler publishes its verdict does the request capture —
        // and this is the other ingress order (decision installed while the
        // correlation is already current), which must work too.
        set_recording_decision(correlation_id, true);
        assert_eq!(
            recording_decision_for_current(),
            Some(RecordDecision::Record)
        );
        assert!(gate());

        clear_recording_decision(correlation_id);
    }
}
