//! Tracing layer that mirrors a request's correlation id into the ambient
//! deja-context, so boundary events fired from spawned tasks inherit the request
//! correlation instead of recording as uncorrelated.
//!
//! # Why a tracing layer (not tokio task hooks)
//!
//! The middleware wraps the request future in `scope_correlation`, so every
//! boundary that fires synchronously within a poll of that future is attributed.
//! But work moved onto a `tokio::spawn`ed task escapes that wrapper. Hyperswitch
//! runs handlers on actix's per-worker runtimes, which the main `#[tokio::main]`
//! runtime builder does not own — so tokio's task-lifecycle hooks cannot reach
//! them. A tracing layer can: hyperswitch already propagates the request span
//! into spawned tasks via `.in_current_span()`, and a layer's `on_enter` fires
//! wherever the task is polled, on any runtime.
//!
//! # Mechanism (lock-light hot path)
//!
//! `on_new_span` resolves the span's correlation, full logical path (root→leaf),
//! task lineage, and Skip-gate verdict ONCE into a single `SpanContext`
//! extension. That is the only extension *write*, once per span.
//!
//! The per-poll hot path is a brief extension *read* plus thread-local cursor
//! writes: `on_enter` sets the path and lineage cursors to this span's values;
//! `on_exit` restores them from the span PARENT via `ctx`. The payloads are
//! `Arc`, so a poll bracket moves no heap. Correlation is entered into
//! deja-context only when it CHANGES from the thread's current value (≈once per
//! request), saving the previous value so a spawned task polled on a fresh worker
//! reverts to nothing rather than to its parent. Because an `Instrumented` future
//! enters/exits its span on every poll, the cursors are re-established per-poll on
//! whichever worker thread polls the task — correct under work-stealing.

use std::cell::RefCell;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// The span field carrying the request correlation id (set by the ingress root
/// span — see `router_env::root_span`).
const CORRELATION_FIELD: &str = "request_id";

/// Everything `on_new_span` resolves for a span, written once into its extensions
/// and read on every enter/exit. `Arc` payloads keep the per-poll cursor moves
/// allocation-free.
#[derive(Clone)]
struct SpanContext {
    /// Logical span-path root→leaf (`"payments_core>update_trackers"`), built once
    /// from the parent's path plus this span's name.
    path: Arc<str>,
    /// Correlation id from this span's own `request_id` field, else inherited from
    /// the parent. `None` when no ancestor carried one.
    correlation: Option<Arc<str>>,
    /// Task lineage: a fresh bucket when this span is a fork boundary, else the
    /// parent's bucket.
    lineage: Arc<crate::TaskLineage>,
    /// Whether entering this span should engage the correlation scope — false when
    /// the ingress pushed a `Skip` decision for `correlation`. Cached so the hot
    /// path reads a bool, not a decision-registry lookup.
    observe: bool,
}

/// The span field that marks a spawned-task boundary: a span carrying
/// `deja.fork = true` (minted by [`crate::fork_span`] at the `tokio::spawn` site)
/// opens a NEW lineage bucket, so its subtree is an independent, unordered task
/// region. Every other span inherits its parent's bucket — synchronous nesting
/// stays in one bucket, exactly as the old task-local model did, but now derived
/// purely from the span tree instead of `spawn_detached`.
const FORK_FIELD: &str = "deja.fork";

thread_local! {
    /// The full logical span-path active on this thread. `on_enter` sets it to the
    /// entered span's path; `on_exit` restores it from the span parent. This is the
    /// SOURCE for the `SpanPath` address: same-callsite calls in DISTINCT spans get
    /// distinct paths → distinct occurrence buckets, fixing the positional
    /// `occurrence` swap async interleaving otherwise causes.
    static CURRENT_PATH: RefCell<Option<Arc<str>>> = const { RefCell::new(None) };

    /// The task lineage active on this thread, same set/restore-from-parent shape
    /// as `CURRENT_PATH`. `None` means the root region.
    static CURRENT_LINEAGE: RefCell<Option<Arc<crate::TaskLineage>>> =
        const { RefCell::new(None) };

    /// The correlation this layer has entered into deja-context on this thread,
    /// compared against each span's engaged correlation so the scope is entered
    /// only on a CHANGE.
    static CURRENT_CORRELATION: RefCell<Option<Arc<str>>> = const { RefCell::new(None) };

    /// Saved previous correlations, one frame per CHANGE (not per span), tagged
    /// with the span id that caused it. `on_exit` pops the frame its own enter
    /// pushed and restores the exact previous value — so a spawned task polled on a
    /// fresh worker reverts to nothing, which restore-from-parent could not do.
    /// Depth ≈ correlation nesting (≈1 per request).
    static CORRELATION_RESTORE: RefCell<Vec<(u64, Option<Arc<str>>)>> =
        const { RefCell::new(Vec::new()) };
}

/// Enter `target` into deja-context only when it differs from what this layer last
/// entered on this thread, saving the previous value tagged with `span_id` so the
/// matching `on_exit` reverts exactly it.
fn engage_correlation(span_id: u64, target: Option<Arc<str>>) {
    CURRENT_CORRELATION.with(|current| {
        let mut current = current.borrow_mut();
        if current.as_deref() == target.as_deref() {
            return;
        }
        CORRELATION_RESTORE.with(|stack| stack.borrow_mut().push((span_id, current.clone())));
        deja_context::set_current_correlation(target.as_deref());
        *current = target;
    });
}

/// Revert the correlation change `span_id`'s enter made, if it made one.
fn restore_correlation(span_id: u64) {
    let restored = CORRELATION_RESTORE.with(|stack| {
        let mut stack = stack.borrow_mut();
        if stack.last().is_some_and(|(id, _)| *id == span_id) {
            stack.pop()
        } else {
            None
        }
    });
    if let Some((_, previous)) = restored {
        deja_context::set_current_correlation(previous.as_deref());
        CURRENT_CORRELATION.with(|current| *current.borrow_mut() = previous);
    }
}

/// The logical span-path currently active on this thread — the entered span NAMES
/// joined root→leaf with `>` (e.g. `"payments_core>update_trackers"`). `None` when
/// no span is entered.
///
/// Read once per boundary call at `CallsiteIdentity` build time, on BOTH record and
/// replay (the layer is registered in both modes). The path is a rank-2 address that
/// resolves a call independently of source line/signature, AND scopes the per-key
/// occurrence to the span so concurrent same-callsite calls in DIFFERENT spans don't
/// swap rows under async interleaving.
///
/// # Limitations (why this is GRACEFUL DEGRADATION, not a guarantee)
///
/// The layer is installed unfiltered, so the path captures EVERY ambient `tracing`
/// span (framework, library, and `#[instrument]` spans), root→leaf. Two consequences:
///
///  * **Not robust to span-structure edits.** Adding, removing, or renaming ANY
///    enclosing instrumented span on V2 (e.g. a function rename — which renames its
///    default span — or an extracted helper) changes the path string, so the rank-2
///    `SpanPath` key misses on V2 and the call demotes to rank-3 `SyntacticHash`
///    (still line/signature-independent) or weaker. That is no WORSE than pre-P3
///    behavior; `args_hash` still guards distinct-arg correctness. So a benign edit
///    that leaves the span structure intact (a pure line shift) keeps rank-2; one that
///    reshapes spans falls back gracefully.
///  * **Disambiguates by span NAME, not instance.** Two concurrently-entered DISTINCT
///    span instances that share a name (e.g. two parallel tasks each entering an
///    identically-named span within one correlation) collapse to the SAME path and
///    SAME bucket — the residual "case C" that needs a finer, distinctly-named
///    `#[instrument]` span to resolve (a follow-up, not handled here). The headline
///    case (`update_payment_attempt` vs `update_payment_intent`) has distinct names
///    and IS disambiguated.
#[must_use]
pub fn current_span_path() -> Option<String> {
    CURRENT_PATH.with(|cell| cell.borrow().as_ref().map(|path| path.to_string()))
}

/// The task lineage active on this thread, derived from the entered span tree —
/// the span-based replacement for the `CURRENT_TASK_LINEAGE` task-local. Returns
/// the root region's lineage when no lineage-bearing span is entered.
pub(crate) fn current_span_lineage() -> crate::TaskLineage {
    CURRENT_LINEAGE
        .with(|cell| cell.borrow().as_ref().map(|lineage| (**lineage).clone()))
        .unwrap_or_else(crate::TaskLineage::root)
}

/// Tracing layer mirroring the ingress `request_id` span field into deja-context.
#[derive(Debug, Default)]
pub struct DejaCorrelationLayer;

impl DejaCorrelationLayer {
    /// Create a new correlation-propagation layer.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

/// Visitor that extracts the `request_id` field as a string.
struct CorrelationVisitor(Option<String>);

impl Visit for CorrelationVisitor {
    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == CORRELATION_FIELD {
            self.0 = Some(value.to_owned());
        }
    }

    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        // Spans often record fields via Display (`%x`) / Debug; accept that too,
        // but never overwrite a string-typed capture.
        if self.0.is_none() && field.name() == CORRELATION_FIELD {
            self.0 = Some(format!("{value:?}"));
        }
    }
}

/// Visitor that detects the `deja.fork` boundary marker on a span.
struct ForkVisitor(bool);

impl Visit for ForkVisitor {
    fn record_bool(&mut self, field: &Field, value: bool) {
        if field.name() == FORK_FIELD {
            self.0 = self.0 || value;
        }
    }

    fn record_debug(&mut self, _field: &Field, _value: &dyn std::fmt::Debug) {}
}

impl<S> Layer<S> for DejaCorrelationLayer
where
    S: Subscriber,
    S: for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(id) else {
            return;
        };

        // Everything this span inherits comes from the parent's resolved context;
        // the parent was processed before this child, so its context is set.
        let parent = span
            .parent()
            .and_then(|parent| parent.extensions().get::<SpanContext>().cloned());

        // Prefer this span's own `request_id`; contain a panic in its Debug/Display
        // so a bad field cannot kill the request (correlation stays inherited).
        let mut visitor = CorrelationVisitor(None);
        let _ = catch_unwind(AssertUnwindSafe(|| attrs.record(&mut visitor)));
        let own_correlation = visitor.0.map(|id| Arc::<str>::from(id.as_str()));
        let correlation = own_correlation
            .clone()
            .or_else(|| parent.as_ref().and_then(|c| c.correlation.clone()));

        // Path: the parent's path plus this span's static name.
        let name = span.name();
        let path: Arc<str> = match parent.as_ref() {
            Some(parent) => Arc::from(format!("{}>{name}", parent.path).as_str()),
            None => Arc::from(name),
        };

        // A `deja.fork` span opens a fresh, unordered lineage bucket; every other
        // span inherits its parent's, so synchronous nesting stays in one bucket.
        let mut fork_visitor = ForkVisitor(false);
        attrs.record(&mut fork_visitor);
        let lineage = if fork_visitor.0 {
            let base = parent
                .as_ref()
                .map_or_else(crate::TaskLineage::root, |c| (*c.lineage).clone());
            Arc::new(crate::TaskLineage::forked_child_of(
                base,
                correlation.as_deref(),
            ))
        } else {
            parent
                .as_ref()
                .map_or_else(|| Arc::new(crate::TaskLineage::root()), |c| Arc::clone(&c.lineage))
        };

        // Only a span carrying its OWN correlation pays a decision lookup; a span
        // that inherited its correlation inherits the verdict too.
        let observe = if own_correlation.is_some() {
            correlation.as_deref().is_some_and(|id| {
                !matches!(
                    deja_context::recording_decision(id),
                    Some(deja_context::RecordDecision::Skip)
                )
            })
        } else {
            parent.as_ref().is_some_and(|c| c.observe)
        };

        span.extensions_mut().insert(SpanContext {
            path,
            correlation,
            lineage,
            observe,
        });
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        let Some(cx) = ctx
            .span(id)
            .and_then(|span| span.extensions().get::<SpanContext>().cloned())
        else {
            return;
        };

        CURRENT_PATH.with(|cell| *cell.borrow_mut() = Some(Arc::clone(&cx.path)));
        CURRENT_LINEAGE.with(|cell| *cell.borrow_mut() = Some(Arc::clone(&cx.lineage)));

        // Engage the correlation scope unless the ingress sampled this request out.
        let engaged = cx.observe.then(|| cx.correlation.clone()).flatten();
        engage_correlation(id.into_u64(), engaged);
    }

    fn on_exit(&self, id: &Id, ctx: Context<'_, S>) {
        // Restore the path and lineage cursors from the span PARENT (clear at root).
        let parent = ctx
            .span(id)
            .and_then(|span| span.parent())
            .and_then(|parent| parent.extensions().get::<SpanContext>().cloned());
        CURRENT_PATH.with(|cell| {
            *cell.borrow_mut() = parent.as_ref().map(|c| Arc::clone(&c.path));
        });
        CURRENT_LINEAGE.with(|cell| {
            *cell.borrow_mut() = parent.as_ref().map(|c| Arc::clone(&c.lineage));
        });

        restore_correlation(id.into_u64());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deja_context::current_correlation_id;
    use tracing_subscriber::prelude::*;

    #[test]
    fn enters_and_restores_correlation_around_a_span() {
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            assert_eq!(current_correlation_id(), None);
            let span = tracing::info_span!("deja::http_incoming", request_id = "req-42");
            {
                let _entered = span.enter();
                assert_eq!(current_correlation_id().as_deref(), Some("req-42"));
            }
            assert_eq!(current_correlation_id(), None);
        });
    }

    #[test]
    fn child_span_inherits_root_correlation() {
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            let root = tracing::info_span!("deja::http_incoming", request_id = "req-7");
            let _root = root.enter();
            // A child span without its own request_id inherits the root's
            // correlation (resolved at creation), so entering it still attributes.
            let child = tracing::info_span!("child");
            let _child = child.enter();
            assert_eq!(current_correlation_id().as_deref(), Some("req-7"));
        });
    }

    #[test]
    fn nested_spans_restore_lifo() {
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            let outer = tracing::info_span!("deja::http_incoming", request_id = "outer");
            let _outer = outer.enter();
            assert_eq!(current_correlation_id().as_deref(), Some("outer"));
            {
                // A nested span with no request_id inherits "outer"; restoring it
                // must leave "outer" active, not None.
                let inner = tracing::info_span!("inner");
                let _inner = inner.enter();
                assert_eq!(current_correlation_id().as_deref(), Some("outer"));
            }
            assert_eq!(current_correlation_id().as_deref(), Some("outer"));
        });
    }

    #[test]
    fn logical_span_path_is_root_to_leaf_and_restores() {
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            assert_eq!(current_span_path(), None);
            let root = tracing::info_span!("payments_core");
            let _root = root.enter();
            assert_eq!(current_span_path().as_deref(), Some("payments_core"));
            {
                let leaf = tracing::info_span!("update_trackers");
                let _leaf = leaf.enter();
                // root→leaf order, joined by '>'.
                assert_eq!(
                    current_span_path().as_deref(),
                    Some("payments_core>update_trackers")
                );
            }
            // The leaf popped LIFO; the path is back to just the root.
            assert_eq!(current_span_path().as_deref(), Some("payments_core"));
        });
        // Fully unwound after the subscriber scope ends.
        assert_eq!(current_span_path(), None);
    }

    #[test]
    fn sibling_spans_yield_distinct_paths() {
        // The decisive property for the occurrence-swap fix: two boundaries firing
        // under SIBLING spans see DISTINCT logical paths, so they will address into
        // distinct occurrence buckets rather than racing one shared counter.
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            let root = tracing::info_span!("payments_core");
            let _root = root.enter();
            let path_a = {
                let a = tracing::info_span!("update_payment_attempt");
                let _a = a.enter();
                current_span_path()
            };
            let path_b = {
                let b = tracing::info_span!("update_payment_intent");
                let _b = b.enter();
                current_span_path()
            };
            assert_eq!(
                path_a.as_deref(),
                Some("payments_core>update_payment_attempt")
            );
            assert_eq!(
                path_b.as_deref(),
                Some("payments_core>update_payment_intent")
            );
            assert_ne!(path_a, path_b);
        });
    }

    #[test]
    fn skip_decision_leaves_correlation_disengaged() {
        // A sampled-out request (an ingress `Skip` decision on its correlation)
        // must not engage the correlation scope, so boundaries under it inherit no
        // correlation. Record/replay/no-decision engage (the tests above, whose ids
        // carry no decision, cover that).
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            let correlation_id = "req-skip-disengage";
            deja_context::set_recording_decision(correlation_id, false);
            let span = tracing::info_span!("deja::http_incoming", request_id = "req-skip-disengage");
            {
                let _entered = span.enter();
                assert_eq!(
                    current_correlation_id(),
                    None,
                    "a Skip request must leave the correlation scope disengaged"
                );
            }
            deja_context::clear_recording_decision(correlation_id);
        });
    }

    #[test]
    fn spawned_child_entered_without_parent_engages_then_reverts_correlation() {
        // A task span polled on a fresh worker: its parent (the request root) is in
        // the span tree but NOT entered on this thread. Entering the child engages
        // its inherited correlation and exposes the full-ancestry path. On exit the
        // two cursors deliberately differ: CORRELATION reverts faithfully to nothing
        // (a stale correlation would attribute a later boundary to the wrong
        // request), while PATH/LINEAGE restore from the tree parent (a cheap cursor
        // whose between-poll value is never read — boundaries read only within a
        // span, and the next poll's enter overwrites it).
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            let root = tracing::info_span!("deja::http_incoming", request_id = "req-spawn");
            let child = {
                let _root = root.enter();
                tracing::info_span!("spawned_work")
            };
            // Root has been exited; nothing is entered on this thread.
            assert_eq!(current_correlation_id(), None);
            assert_eq!(current_span_path(), None);
            {
                let _child = child.enter();
                assert_eq!(current_correlation_id().as_deref(), Some("req-spawn"));
                assert_eq!(
                    current_span_path().as_deref(),
                    Some("deja::http_incoming>spawned_work")
                );
            }
            // Correlation reverts faithfully to None (not the parent's "req-spawn").
            assert_eq!(current_correlation_id(), None);
            // Path is a parent-restored cursor, so it holds the tree parent here.
            assert_eq!(current_span_path().as_deref(), Some("deja::http_incoming"));
        });
    }

    #[test]
    fn fork_span_opens_a_new_lineage_bucket() {
        // The substrate's lineage proof: a `deja.fork`-marked span (what
        // `spawn_fork` instruments at the `tokio::spawn` site) opens a fresh,
        // non-root bucket — an unordered region — while ordinary spans inherit
        // their parent's. This replaces the removed task-local `spawn_detached`.
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            let root = tracing::info_span!("deja::http_incoming", request_id = "req-fork");
            let _root = root.enter();
            // The synchronous request path stays in the root bucket.
            let base = current_span_lineage();
            assert_eq!(base.bucket_id, crate::ROOT_TASK_ID);
            assert_eq!(base.fork_seq, 0);

            {
                let fork = crate::fork_span();
                let _fork = fork.enter();
                let forked = current_span_lineage();
                assert!(
                    forked.bucket_id.contains("::fork-"),
                    "fork bucket must carry the marker, got {:?}",
                    forked.bucket_id
                );
                assert_eq!(forked.fork_seq, 1, "first fork sequence is deterministic");
                assert_eq!(forked.parent_task_id.as_deref(), Some(crate::ROOT_TASK_ID));

                // A plain child under the fork inherits the fork bucket — the
                // unordered region propagates down the span tree.
                let child = tracing::info_span!("inside_fork");
                let _child = child.enter();
                assert_eq!(current_span_lineage().bucket_id, forked.bucket_id);
            }

            // Fork popped LIFO → back to the synchronous root bucket.
            assert_eq!(current_span_lineage().bucket_id, crate::ROOT_TASK_ID);
        });
    }
}
