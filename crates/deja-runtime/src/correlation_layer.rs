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
//! `on_new_span` resolves the span's correlation ONCE — from its own `request_id`
//! field, else inherited from the parent's already-resolved value — and stores it
//! in the span's extensions. That is the only extension *write*, and it happens
//! once per span (the same shape the execution-graph layer uses safely).
//!
//! The per-poll hot path stays lock-light: `on_enter` does a brief extension
//! *read* of the pre-resolved value, enters it into deja-context, and parks the
//! restore guard on a **thread-local stack**; `on_exit` pops that stack. No
//! per-poll extension writes and no parent-walk. Because an `Instrumented` future
//! enters/exits its span on every poll, the context is re-established per-poll on
//! whichever worker thread polls the task — correct under tokio work-stealing.

use std::cell::RefCell;

use deja_context::{enter_correlation_id, ContextGuard};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id};
use tracing::Subscriber;
use tracing_subscriber::layer::{Context, Layer};
use tracing_subscriber::registry::LookupSpan;

/// The span field carrying the request correlation id (set by the ingress root
/// span — see `router_env::root_span`).
const CORRELATION_FIELD: &str = "request_id";

/// Correlation id resolved for a span (own field or inherited from parent),
/// stored in the span's extensions by `on_new_span`.
#[derive(Clone)]
struct SpanCorrelation(String);

/// The span field that marks a spawned-task boundary: a span carrying
/// `deja.fork = true` (minted by [`crate::fork_span`] at the `tokio::spawn` site)
/// opens a NEW lineage bucket, so its subtree is an independent, unordered task
/// region. Every other span inherits its parent's bucket — synchronous nesting
/// stays in one bucket, exactly as the old task-local model did, but now derived
/// purely from the span tree instead of `spawn_detached`.
const FORK_FIELD: &str = "deja.fork";

/// Task lineage resolved for a span (own bucket if it is a fork boundary, else
/// inherited from parent), stored in the span's extensions by `on_new_span` and
/// mirrored onto the current thread by `on_enter` — the span-derived replacement
/// for the `CURRENT_TASK_LINEAGE` task-local.
#[derive(Clone)]
struct SpanLineage(crate::TaskLineage);

thread_local! {
    /// LIFO stack of restore guards, one frame per `on_enter`/`on_exit` pair on
    /// this thread. `None` when the entered span resolved no correlation. Tracing
    /// brackets enter/exit per poll on a single thread, so this stays balanced and
    /// empty between polls (work-stealing safe — see module docs).
    static GUARD_STACK: RefCell<Vec<Option<ContextGuard>>> = const { RefCell::new(Vec::new()) };

    /// LIFO stack of entered span NAMES on this thread — the **logical span-path**
    /// (root→leaf) a boundary fires within. Pushed in `on_enter`, popped in
    /// `on_exit`, in lock-step with `GUARD_STACK` (one frame per poll-bracket), so
    /// it is balanced per poll and work-stealing safe for the same reason.
    ///
    /// Span names are `&'static str` (from `tracing` metadata), so pushing is
    /// allocation-free. The stack holds `Option<&'static str>` rather than `&str`
    /// only so a (spurious) enter with no resolvable span still pushes a frame and
    /// stays balanced with the unconditional `on_exit` pop.
    ///
    /// This is the SOURCE for the `SpanPath` address: concurrent same-callsite
    /// calls in DISTINCT spans get distinct paths → distinct occurrence buckets,
    /// which is what fixes the positional `occurrence` swap that async task
    /// interleaving otherwise causes (see `addresses_for` / the
    /// `Address::SpanPath` rank).
    static NAME_STACK: RefCell<Vec<Option<&'static str>>> = const { RefCell::new(Vec::new()) };

    /// LIFO stack of the task lineage active on this thread — one frame per
    /// `on_enter`/`on_exit` pair, in lock-step with `GUARD_STACK`/`NAME_STACK`.
    /// `on_enter` pushes the span's resolved `SpanLineage`; `on_exit` pops. The
    /// top frame is what `current_task_lineage` reads; an empty stack means the
    /// root region. Balanced per poll, so work-stealing safe for the same reason
    /// the correlation stack is.
    static LINEAGE_STACK: RefCell<Vec<crate::TaskLineage>> = const { RefCell::new(Vec::new()) };
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
///
/// (`None` frames from spurious enters are elided here; two stacks differing only by
/// elided `None`s collapse to the same path — harmless given the limitations above.)
#[must_use]
pub fn current_span_path() -> Option<String> {
    NAME_STACK.with(|stack| {
        let stack = stack.borrow();
        let parts: Vec<&str> = stack.iter().filter_map(|name| *name).collect();
        if parts.is_empty() {
            None
        } else {
            Some(parts.join(">"))
        }
    })
}

/// The task lineage active on this thread, derived from the entered span tree —
/// the span-based replacement for the `CURRENT_TASK_LINEAGE` task-local. Returns
/// the root region's lineage when no lineage-bearing span is entered.
pub(crate) fn current_span_lineage() -> crate::TaskLineage {
    LINEAGE_STACK
        .with(|stack| stack.borrow().last().cloned())
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

        // Prefer this span's own `request_id` field.
        let mut visitor = CorrelationVisitor(None);
        attrs.record(&mut visitor);
        let resolved = visitor.0.or_else(|| {
            // Else inherit the parent's already-resolved correlation. The parent
            // exists and was processed before this child, so its value is set.
            span.parent().and_then(|parent| {
                parent
                    .extensions()
                    .get::<SpanCorrelation>()
                    .map(|c| c.0.clone())
            })
        });

        if let Some(correlation) = resolved.clone() {
            span.extensions_mut().insert(SpanCorrelation(correlation));
        }

        // Resolve task lineage from the span tree. A `deja.fork`-marked span opens
        // a fresh bucket (an independent, unordered task region); every other span
        // inherits its parent's lineage, so synchronous nesting stays in one
        // bucket. This replaces the `spawn_detached`/task-local model outright.
        let mut fork_visitor = ForkVisitor(false);
        attrs.record(&mut fork_visitor);
        let parent_lineage = span.parent().and_then(|parent| {
            parent
                .extensions()
                .get::<SpanLineage>()
                .map(|l| l.0.clone())
        });
        let lineage = if fork_visitor.0 {
            let base = parent_lineage.unwrap_or_else(crate::TaskLineage::root);
            crate::TaskLineage::forked_child_of(base, resolved.as_deref())
        } else {
            parent_lineage.unwrap_or_else(crate::TaskLineage::root)
        };
        span.extensions_mut().insert(SpanLineage(lineage));
    }

    fn on_enter(&self, id: &Id, ctx: Context<'_, S>) {
        // Brief read of the pre-resolved correlation, the span's static name, and
        // the pre-resolved lineage; never a write, never a walk.
        let span = ctx.span(id);
        let name: Option<&'static str> = span.as_ref().map(|span| span.name());
        let correlation = span.as_ref().and_then(|span| {
            span.extensions()
                .get::<SpanCorrelation>()
                .map(|c| c.0.clone())
        });
        let lineage = span
            .as_ref()
            .and_then(|span| span.extensions().get::<SpanLineage>().map(|l| l.0.clone()))
            .unwrap_or_else(crate::TaskLineage::root);

        // Always push a frame to EVERY stack so `on_exit` can pop each
        // unconditionally, keeping them balanced across nested spans.
        let frame = correlation.map(enter_correlation_id);
        GUARD_STACK.with(|stack| stack.borrow_mut().push(frame));
        NAME_STACK.with(|stack| stack.borrow_mut().push(name));
        LINEAGE_STACK.with(|stack| stack.borrow_mut().push(lineage));
    }

    fn on_exit(&self, _id: &Id, _ctx: Context<'_, S>) {
        // Pop the name + lineage frames first (pure thread-local, no guard
        // restore), then pop this poll's correlation frame OUT and drop it — so the
        // guard's restore (which touches a different thread-local) never runs while
        // a stack borrow is held. A spurious exit with empty stacks is a no-op.
        NAME_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
        LINEAGE_STACK.with(|stack| {
            stack.borrow_mut().pop();
        });
        let frame = GUARD_STACK.with(|stack| stack.borrow_mut().pop());
        drop(frame);
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
