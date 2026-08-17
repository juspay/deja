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
//! The per-poll hot path is a brief extension *read* plus a push/pop on a
//! thread-local stack of ENTERED spans: `on_enter` pushes this span's path and
//! lineage tagged with its span id, `on_exit` removes the frame that span's own
//! enter pushed. The payloads are `Arc`, so a poll bracket moves no heap.
//! Correlation follows the same discipline through `CORRELATION_RESTORE`,
//! differing only in that it is entered into deja-context on a CHANGE (≈once per
//! request) rather than once per span. Every cursor therefore reverts to the value
//! the thread held BEFORE the enter — which for a spawned task polled on a fresh
//! worker is nothing at all. Because an `Instrumented` future enters/exits its
//! span on every poll, the cursors are re-established per-poll on whichever worker
//! thread polls the task — correct under work-stealing.

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
pub(crate) const CORRELATION_FIELD: &str = "request_id";

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

/// One entered span's cursor values, tagged with the span that owns them.
///
/// The tag is the whole point. These were two bare cells that `on_exit` reset from
/// the span TREE PARENT — which answers "who encloses this span", not "what did
/// this thread hold before entering it". Those differ exactly when a span is
/// entered without its parent entered, the spawned-task case this layer exists to
/// serve: the parent is not on this thread at all, so the reset left the previous
/// request's path and bucket standing for every boundary that fired on that worker
/// until the next enter. Owning each frame by span id makes the pop exact, and
/// makes absence observable — an empty stack means no span is entered, which is
/// what [`current_span_path`]'s documented `None` always claimed to mean and
/// nothing enforced.
struct SpanCursor {
    /// The span whose `on_enter` pushed this frame; only that span's `on_exit`
    /// removes it.
    span_id: u64,
    /// Full logical span-path root→leaf, from the owning span's `SpanContext`.
    path: Arc<str>,
    /// Task lineage of the owning span.
    lineage: Arc<crate::TaskLineage>,
}

thread_local! {
    /// The spans ENTERED on this thread, innermost last. This is the SOURCE for the
    /// `SpanPath` address and the task lineage: same-callsite calls in DISTINCT
    /// spans get distinct paths → distinct occurrence buckets, fixing the positional
    /// `occurrence` swap async interleaving otherwise causes.
    ///
    /// One door in each direction, and nothing else may touch this cell: written
    /// only by `push_span_cursor` / `pop_span_cursor`, read only by
    /// `with_current_cursor`. An unchecked read is what the previous shape made
    /// writable, so the rule is enforced at the source level by
    /// `tests/span_cursor_invariant.rs` rather than left to review.
    static ENTERED_SPANS: RefCell<Vec<SpanCursor>> = const { RefCell::new(Vec::new()) };

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

/// Push `span_id`'s path and lineage as the innermost entered span. The only
/// writer that grows the stack.
fn push_span_cursor(span_id: u64, cx: &SpanContext) {
    ENTERED_SPANS.with(|stack| {
        stack.borrow_mut().push(SpanCursor {
            span_id,
            path: Arc::clone(&cx.path),
            lineage: Arc::clone(&cx.lineage),
        });
    });
}

/// Remove the frame `span_id`'s enter pushed, leaving this thread's cursors at
/// their exact pre-enter value.
///
/// Innermost-first, and a no-op when this span has no frame — the same shape as
/// [`restore_correlation`], and the same shape the registry's own entered-span
/// stack uses, so a guard dropped out of order removes its own frame rather than a
/// bystander's.
fn pop_span_cursor(span_id: u64) {
    ENTERED_SPANS.with(|stack| {
        let mut stack = stack.borrow_mut();
        if let Some(index) = stack.iter().rposition(|frame| frame.span_id == span_id) {
            stack.remove(index);
        }
    });
}

/// Read the innermost entered span's cursor. The only read path, and it hands out
/// no value when no span is entered — a caller cannot take a path or a bucket
/// without that question having been answered.
fn with_current_cursor<T>(read: impl FnOnce(&SpanCursor) -> T) -> Option<T> {
    ENTERED_SPANS.with(|stack| stack.borrow().last().map(read))
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
/// no span is entered, and that is now a fact rather than an aspiration: the cursor
/// lives on a stack of entered spans, so between polls there is nothing to read.
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
    with_current_cursor(|cursor| cursor.path.to_string())
}

/// The task lineage of the innermost span entered on this thread, derived from the
/// entered span tree — the span-based replacement for the `CURRENT_TASK_LINEAGE`
/// task-local.
///
/// `None` when NO span is entered, which is deliberately not the same answer as the
/// root region. The root region is a lineage a span actually carries; `None` means
/// this boundary is outside every instrumented scope and owns no bucket at all. The
/// distinction has to survive to the caller, because a bucket is not an inert label
/// — the scorer excuses a value divergence as an unordered race when two events'
/// buckets differ (`divergence::unordered_distinct_lineage`), so a borrowed bucket
/// writes a real regression off. A caller that must name a bucket anyway chooses
/// its fallback in the open; see [`crate::current_task_lineage`].
pub(crate) fn current_span_lineage() -> Option<crate::TaskLineage> {
    with_current_cursor(|cursor| (*cursor.lineage).clone())
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
/// Extracts [`CORRELATION_FIELD`] from a span's fields. Shared with the
/// execution-graph layer so both resolve a span's correlation from the same
/// definition of what carries one — the two layers must agree, and the graph
/// layer cannot read this layer's answer (see its `on_new_span`).
pub(crate) struct CorrelationVisitor(pub(crate) Option<String>);

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
            parent.as_ref().map_or_else(
                || Arc::new(crate::TaskLineage::root()),
                |c| Arc::clone(&c.lineage),
            )
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

        push_span_cursor(id.into_u64(), &cx);

        // Engage the correlation scope unless the ingress sampled this request out.
        let engaged = cx.observe.then(|| cx.correlation.clone()).flatten();
        engage_correlation(id.into_u64(), engaged);
    }

    /// Revert everything this span's enter established, addressed by span id.
    ///
    /// No span-tree lookup: `ctx` can name this span's PARENT, but the parent is
    /// not what the thread held before the enter, and the two differ precisely in
    /// the case this layer exists for — a task span polled on a worker that never
    /// entered its parent. Popping the frame this span pushed is the only question
    /// with a correct answer in both cases.
    fn on_exit(&self, id: &Id, _ctx: Context<'_, S>) {
        pop_span_cursor(id.into_u64());
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
            let span =
                tracing::info_span!("deja::http_incoming", request_id = "req-skip-disengage");
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
    fn spawned_child_entered_without_parent_reverts_every_cursor() {
        // A task span polled on a fresh worker: its parent (the request root) is in
        // the span tree but NOT entered on this thread. Entering the child engages
        // its inherited correlation and exposes the full-ancestry path — the path is
        // resolved from the span TREE at creation, so having never entered the parent
        // costs nothing while the child is entered. On exit all three cursors revert
        // to what this thread held before, which is nothing: the worker is now free
        // for unrelated work, and that work must not be stamped with this request's
        // address or bucket.
        //
        // The path assertion used to run the other way — `Some("deja::http_incoming")`
        // — justified as a cheap cursor whose between-poll value is never read.
        // Nothing enforced that. Four readers took it with no check that a span was
        // entered (`CallsiteIdentity.span_path` on both record and replay, and
        // `current_task_metadata` on the lineage side), so the leaked value became a
        // rank-2 `SpanPath` address the caller did not own and a lineage bucket the
        // scorer read as evidence of an unordered race.
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
            assert!(current_span_lineage().is_none());
            {
                let _child = child.enter();
                assert_eq!(current_correlation_id().as_deref(), Some("req-spawn"));
                assert_eq!(
                    current_span_path().as_deref(),
                    Some("deja::http_incoming>spawned_work")
                );
                assert!(
                    current_span_lineage().is_some(),
                    "an entered span always resolves a lineage"
                );
            }
            // Correlation reverts faithfully to None (not the parent's "req-spawn").
            assert_eq!(current_correlation_id(), None);
            assert_eq!(
                current_span_path(),
                None,
                "the tree parent is not entered on this thread, so no path is active"
            );
            assert!(
                current_span_lineage().is_none(),
                "and no bucket either — the next boundary here owns neither"
            );
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
            let base = entered_lineage();
            assert_eq!(base.bucket_id, crate::ROOT_TASK_ID);
            assert_eq!(base.fork_seq, 0);

            {
                let fork = crate::fork_span();
                let _fork = fork.enter();
                let forked = entered_lineage();
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
                assert_eq!(entered_lineage().bucket_id, forked.bucket_id);
            }

            // Fork popped LIFO → back to the synchronous root bucket.
            assert_eq!(entered_lineage().bucket_id, crate::ROOT_TASK_ID);
        });
    }

    /// The lineage of the span entered right now. Every call site below is inside
    /// an entered span, so `None` there is a failure of the test's own premise and
    /// says so rather than silently degrading to the root region.
    fn entered_lineage() -> crate::TaskLineage {
        current_span_lineage().expect("a span is entered")
    }

    /// The pair of cursors as a boundary would observe them, in the two forms that
    /// actually reach a tape: the rank-2 span-path address and the lineage bucket.
    fn observed_cursors() -> (Option<String>, Option<String>) {
        (
            current_span_path(),
            current_span_lineage().map(|lineage| lineage.bucket_id),
        )
    }

    #[test]
    fn an_entered_span_still_resolves_its_own_path_and_lineage() {
        // The counterweight, and the more dangerous direction to get wrong. Making
        // the cursors revert honestly must not make a NORMAL boundary stop resolving
        // a span path: `addresses_for` simply omits rank-2 when the path is `None`
        // (replay.rs), so an over-tightened cursor demotes every lookup to a weaker
        // address with no error at any layer, surfacing much later as unexplained
        // divergences. Both shapes must keep FULL ancestry — the synchronous nesting
        // the request middleware produces, and the spawned task polled on a worker
        // that never entered the parent.
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            let root = tracing::info_span!("deja::http_incoming", request_id = "req-live");
            let detached = {
                let _root = root.enter();
                // Minted under the root, as `.in_current_span()` does at a spawn site.
                let detached = tracing::info_span!("spawned_work");

                let inner = tracing::info_span!("update_trackers");
                let _inner = inner.enter();

                // Synchronous nesting: root→leaf, and a lineage is resolved.
                let (path, bucket) = observed_cursors();
                assert_eq!(
                    path.as_deref(),
                    Some("deja::http_incoming>update_trackers"),
                    "an in-span boundary keeps its rank-2 address"
                );
                assert_eq!(bucket.as_deref(), Some(crate::ROOT_TASK_ID));

                detached
            };

            // Same question for the case the layer exists to serve: entered alone,
            // on a thread holding nothing, the path is still the full ancestry.
            let _detached = detached.enter();
            let (path, bucket) = observed_cursors();
            assert_eq!(
                path.as_deref(),
                Some("deja::http_incoming>spawned_work"),
                "a task polled without its parent entered still addresses at rank 2"
            );
            assert_eq!(bucket.as_deref(), Some(crate::ROOT_TASK_ID));
        });
    }

    #[test]
    fn a_thread_that_served_a_request_resolves_neither_afterwards() {
        // The defect, stated as the worker lifecycle that produces it: one thread
        // serves a request, the request finishes, and the thread goes on to serve a
        // boundary belonging to nobody. Under restore-from-parent it kept the first
        // request's path and bucket, so that unrelated boundary registered a rank-2
        // `SpanPath` address it did not own.
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            // The last thing this worker does for the earlier request is poll a
            // spawned task, whose parent is in the span tree but was never entered
            // here. Restore-from-parent has a tree parent to reach for at that exit
            // and leaves its path standing, which is the defect; a bare nested span
            // under an entered root would not exercise it.
            let spawned = {
                let first = tracing::info_span!("deja::http_incoming", request_id = "req-earlier");
                let _first = first.enter();
                tracing::info_span!("payments_core")
            };
            {
                let _poll = spawned.enter();
                assert_eq!(
                    current_span_path().as_deref(),
                    Some("deja::http_incoming>payments_core"),
                    "premise: the poll resolves the full ancestry"
                );
            }

            assert_eq!(
                observed_cursors(),
                (None, None),
                "the poll is over; a boundary firing here owns no address and no bucket"
            );
        });
    }

    #[test]
    fn a_finished_fork_leaves_no_bucket_for_the_next_boundary_to_borrow() {
        // The consumer this protects. `divergence::unordered_distinct_lineage` calls
        // a value divergence an excusable unordered race when the two events' lineage
        // buckets merely DIFFER — a mismatch alone returns true, before any span-path
        // check. So a worker that had just polled a forked task and then stamped an
        // unrelated boundary with `root::fork-N`, while its replay counterpart stamped
        // `root`, handed the scorer a manufactured mismatch and wrote a real
        // regression off.
        //
        // Asserted at the producer, on the exact value that reaches that comparison:
        // `current_task_metadata`'s bucket, once the fork region has been left.
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            // `spawn_fork` mints the fork span at the spawn site, under the request
            // root; tokio polls the task on whatever worker is free, which has not
            // entered the root. The boundary that matters is one span deeper — work
            // inside the forked task — because THAT is the exit whose tree parent
            // carries a fork bucket for restore-from-parent to leave behind.
            let (inside_fork, forked_bucket) = {
                let root = tracing::info_span!("deja::http_incoming", request_id = "req-borrow");
                let _root = root.enter();
                let fork = crate::fork_span();
                let _fork = fork.enter();
                let bucket = entered_lineage().bucket_id;
                assert!(bucket.contains("::fork-"), "premise: a real fork bucket");
                (tracing::info_span!("inside_fork"), bucket)
            };

            // The poll, on a worker holding nothing else.
            {
                let _poll = inside_fork.enter();
                assert_eq!(
                    entered_lineage().bucket_id,
                    forked_bucket,
                    "premise: the poll is genuinely in the fork's unordered region"
                );
            }

            // The poll is over and the worker is free.
            let stamped = crate::current_task_metadata(None);
            assert_ne!(
                stamped.bucket_id.as_deref(),
                Some(forked_bucket.as_str()),
                "an orphan boundary must not inherit the finished fork's bucket"
            );
            assert_eq!(
                stamped.bucket_id.as_deref(),
                Some(crate::ROOT_TASK_ID),
                "it belongs to no fork, so it is ordered against root traffic, not \
                 excused against it"
            );
            assert_eq!(stamped.task_bucket, stamped.bucket_id, "the two agree");
        });
    }

    #[test]
    fn a_balanced_enter_and_exit_leaves_the_cursors_as_it_found_them() {
        // The invariant the defect violated, stated generically rather than per
        // shape, so the next author of `on_exit` cannot satisfy it by restoring from
        // somewhere that merely looks right in the common case. Restore-from-parent
        // passes this for a span whose parent is entered and fails it for one whose
        // parent is not — which is the whole finding.
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            let root = tracing::info_span!("deja::http_incoming", request_id = "req-balance");

            // Shape 1: a root span entered on a bare thread.
            let before = observed_cursors();
            let detached = {
                let _root = root.enter();
                tracing::info_span!("spawned_work")
            };
            assert_eq!(observed_cursors(), before, "root span");

            // Shape 2: a child whose parent is NOT entered — the spawned-task poll.
            let before = observed_cursors();
            {
                let _detached = detached.enter();
            }
            assert_eq!(
                observed_cursors(),
                before,
                "child entered without its parent"
            );

            // Shapes 3 and 4: a nested child and a fork, both under an entered root,
            // where the pre-enter value is a real value rather than absence.
            let _root = root.enter();
            let before = observed_cursors();
            assert!(before.0.is_some(), "premise: the root is entered");
            {
                let inner = tracing::info_span!("payments_core");
                let _inner = inner.enter();
            }
            assert_eq!(observed_cursors(), before, "nested child");
            {
                let fork = crate::fork_span();
                let _fork = fork.enter();
            }
            assert_eq!(observed_cursors(), before, "fork boundary");
        });
    }

    #[test]
    fn a_guard_dropped_out_of_order_removes_its_own_frame() {
        // `Span::enter` guards are RAII but nothing forces them to drop LIFO — a
        // `Vec<EnteredSpan>` drops in declaration order. The registry's own entered
        // -span stack removes the matching id from wherever it sits, and the cursor
        // stack has to agree, or an out-of-order exit would evict a bystander's frame
        // and leave the thread addressing under a span it had already left.
        let subscriber = tracing_subscriber::registry().with(DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, || {
            let root = tracing::info_span!("deja::http_incoming", request_id = "req-order");
            let _root = root.enter();
            let outer = tracing::info_span!("outer").entered();
            let inner = tracing::info_span!("inner").entered();
            assert_eq!(
                current_span_path().as_deref(),
                Some("deja::http_incoming>outer>inner")
            );

            // Drop the OUTER guard first; `inner` is still entered and still the
            // innermost frame, so it remains the active address.
            drop(outer);
            assert_eq!(
                current_span_path().as_deref(),
                Some("deja::http_incoming>outer>inner"),
                "the innermost entered span is unaffected by a sibling frame leaving"
            );

            drop(inner);
            assert_eq!(
                current_span_path().as_deref(),
                Some("deja::http_incoming"),
                "and the root, still entered, is what remains"
            );
        });
    }
}
