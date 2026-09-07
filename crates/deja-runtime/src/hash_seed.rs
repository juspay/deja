//! A `BuildHasher` whose keys are recorded, so collection iteration order replays.
//!
//! # The divergence this closes
//!
//! `std::hash::RandomState::new()` draws a fresh key pair per collection
//! construction. Iteration order over a `HashMap`/`HashSet` is a function of
//! those keys, and in an instrumented service that order reaches the wire — a
//! response array built by iterating a map comes out in a different order on
//! every run. Replay then diverges on the body, and no amount of boundary
//! substitution fixes it, because the divergence is not at a boundary at all.
//!
//! The only handle std offers is the `BuildHasher` type itself: `k0`/`k1` are
//! private and `RandomState::new()` is their only constructor. So deja owns the
//! type, records the keys through a boundary, and serves the recorded pair on
//! replay.
//!
//! # One seed per correlation
//!
//! Every collection inside one correlation shares one key pair, drawn once and
//! memoized. The memo is a cell on the correlation's SPAN context
//! (`correlation_layer::SpanContext`): the span that carries the `request_id`
//! mints it, every span created beneath clones the handle, and the innermost
//! entered span's handle rides on the thread's span cursor for this module to
//! read. So the cell lives exactly as long as the request's span tree — a
//! `spawn_fork` tail holds its `fork_span()`, a child of the request span, so it
//! holds the cell — and there is no registry, no eviction, and no clock to get
//! wrong. A correlation entered into deja-context WITHOUT a span
//! (`deja_context::enter` alone) is deliberately not seeded: per-correlation
//! state has one home, and a correlation-keyed map beside it is the shape #100
//! removed.
//!
//! The alternative — a per-collection counter, seed+N — is NOT
//! replayable: hyperswitch polls N connector futures inside a single task
//! (`join_all`), so a counter advances in network-completion order and a
//! collection that recorded `k0+3` replays as `k0+7`.
//!
//! What this gives up, deliberately: std adds a per-draw increment
//! (`keys.set((k0.wrapping_add(1), k1))`, rust#36481) specifically so two
//! collections in one process cannot share a key pair, because equal keys let an
//! attacker who can see one collection's ordering infer another's. Sharing keys
//! within a correlation gives that up *within one request*. It is NOT the much
//! worse regression of a fixed constant seed: keys are still drawn fresh per
//! correlation from the OS entropy std seeds itself with, so nothing is
//! predictable across requests, and outside a correlation std runs verbatim,
//! increment included.
//!
//! # Why SipHash-1-3 from `siphasher`, not `DefaultHasher`
//!
//! `DefaultHasher::new()` is hardcoded to `new_with_keys(0, 0)` and std exposes
//! no keyed constructor, so std's hasher cannot be rebuilt from keys we chose.
//! Prefixing a `DefaultHasher` with the keys would compile, but std explicitly
//! does not guarantee `DefaultHasher`'s algorithm across releases — and record
//! and replay are DIFFERENT PROCESSES, routinely built from different
//! toolchains. A toolchain bump would silently change iteration order while the
//! recorded keys still matched, which is this bug again with the fix in place to
//! hide it. `siphasher` exists precisely to provide the stable implementation
//! std declines to promise, and SipHash-1-3 is the same algorithm std uses, so
//! the only thing that changes versus stock `RandomState` is where the keys come
//! from.

use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::{Arc, OnceLock};

use siphasher::sip::SipHasher13;

use crate::{
    dispatch, BoundaryDeclaration, BoundarySemantics, BoundarySpec, CallsiteIdentity,
    CallsiteSource, CrossingObservation, OperationKind, Reconstructed, ReplayStrategy,
};

/// Boundary tag for the hash-key draw.
///
/// Deliberately NOT `"rng"`, `"id"` or any other tag in the orchestrator's `Pure`
/// tier: those are classified `is_nonblocking_boundary`, so a miss on them is
/// scored `DeterministicMiss` and does not block. A missing hash-key event is
/// the opposite of harmless — every collection in the correlation would iterate
/// on keys the recording never held — so this tag stays outside those tables and
/// its misses stay blocking.
///
/// Also deliberately not spelled "seed": in this codebase "seed" already means
/// materializing recorded STATE into containers before replay (`SeedEntry`,
/// `InconclusiveSeedGap`, the seeder crates). These are hash keys, a different
/// thing, and the vocabularies should not collide.
const BOUNDARY: &str = "hash_seed";
const COMPONENT: &str = "deja_runtime::hash_seed";
const OPERATION: &str = "draw_hash_keys";
/// Scope for the call-site identity. One draw per correlation at occurrence 0.
const SCOPE: &str = "deja::hash_seed::draw";

/// The two SipHash keys a correlation's collections all share.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct HashKeys {
    /// First SipHash key.
    pub k0: u64,
    /// Second SipHash key.
    pub k1: u64,
}

/// Masked, so a key pair cannot reach a log line through a `Debug` on some
/// enclosing struct.
///
/// Knowing the keys is what lets an attacker craft colliding entries, which is
/// the whole reason std randomizes them (rust#36481). Sharing one pair across a
/// correlation already gives up more than std does; leaking it through `{:?}`
/// would give up the rest.
///
/// The masking stops at `Debug`. The RECORDED value must be the real pair — a
/// tape holding `***` substitutes nothing on replay — so the boundary captures
/// the exposed `u64`s. A seam over a masked value is a seam in the wrong place.
impl std::fmt::Debug for HashKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashKeys")
            .field("k0", &"***")
            .field("k1", &"***")
            .finish()
    }
}

/// A correlation's memo cell: the one place its key pair lives.
///
/// `OnceLock` rather than a plain slot because the draw must happen exactly once
/// per correlation even when several threads inside the correlation race to
/// build their first collection: `get_or_init` runs its closure once and blocks
/// the losers until it returns. A check-then-set would record N events for N
/// racing threads, and the ordering nondeterminism would be back.
///
/// `Arc` because the cell is shared BY HANDLE, not found by lookup. The span
/// that owns the correlation mints it and every span beneath clones the handle
/// (`correlation_layer::on_new_span`), so the cell is reachable from whichever
/// thread is polling any part of the request and is dropped with the last span
/// that holds it.
pub(crate) type HashKeyCell = Arc<OnceLock<HashKeys>>;

/// `BuildHasher` that serves recorded keys inside a correlation and std outside.
///
/// Install it as a service's collection hasher (`HashMap<K, V, DejaRandomState>`);
/// every std construction path funnels through `S::default()`, so `HashMap::new`,
/// `with_capacity` and `FromIterator` are all covered by the `Default` impl.
#[derive(Debug, Clone)]
pub enum DejaRandomState {
    /// Inside a correlation: the recorded, correlation-wide key pair.
    Seeded(HashKeys),
    /// Outside one: std verbatim, per-draw increment included.
    Std(RandomState),
}

impl Default for DejaRandomState {
    /// Branches on whether a correlation is engaged on this thread.
    ///
    /// Outside a correlation this must be std EXACTLY — same type, same draw,
    /// same increment — because a process that is not recording should not pay
    /// for deja at all, and because dropping rust#36481's increment is only
    /// justified where a recording needs the order to be reproducible.
    fn default() -> Self {
        match keys_for_current_correlation() {
            Some(keys) => Self::Seeded(keys),
            None => Self::Std(RandomState::new()),
        }
    }
}

impl BuildHasher for DejaRandomState {
    type Hasher = DejaHasher;

    fn build_hasher(&self) -> Self::Hasher {
        match self {
            Self::Seeded(keys) => DejaHasher::Seeded(SipHasher13::new_with_keys(keys.k0, keys.k1)),
            Self::Std(state) => DejaHasher::Std(state.build_hasher()),
        }
    }
}

/// The hasher [`DejaRandomState`] builds. Delegates to whichever arm it came
/// from; it holds no keys of its own and draws nothing.
#[derive(Debug, Clone)]
pub enum DejaHasher {
    /// SipHash-1-3 under the correlation's recorded keys.
    Seeded(SipHasher13),
    /// std's own hasher, untouched.
    Std(std::collections::hash_map::DefaultHasher),
}

/// Forward every `write_*` explicitly rather than leaning on the trait defaults.
///
/// The defaults would funnel the integer writes through `write(&bytes)`, which
/// hashes the same values differently from the specialized methods the wrapped
/// hashers implement. Both sides of a record/replay pair run this same code so
/// consistency would hold either way, but the `Std` arm is supposed to be std
/// verbatim, and routing its integer writes through a byte slice would quietly
/// make it something else. (The signed `write_i*` defaults forward to the
/// unsigned methods on this same impl, so they are covered.)
impl Hasher for DejaHasher {
    fn finish(&self) -> u64 {
        match self {
            Self::Seeded(h) => h.finish(),
            Self::Std(h) => h.finish(),
        }
    }

    fn write(&mut self, bytes: &[u8]) {
        match self {
            Self::Seeded(h) => h.write(bytes),
            Self::Std(h) => h.write(bytes),
        }
    }

    fn write_u8(&mut self, i: u8) {
        match self {
            Self::Seeded(h) => h.write_u8(i),
            Self::Std(h) => h.write_u8(i),
        }
    }

    fn write_u16(&mut self, i: u16) {
        match self {
            Self::Seeded(h) => h.write_u16(i),
            Self::Std(h) => h.write_u16(i),
        }
    }

    fn write_u32(&mut self, i: u32) {
        match self {
            Self::Seeded(h) => h.write_u32(i),
            Self::Std(h) => h.write_u32(i),
        }
    }

    fn write_u64(&mut self, i: u64) {
        match self {
            Self::Seeded(h) => h.write_u64(i),
            Self::Std(h) => h.write_u64(i),
        }
    }

    fn write_u128(&mut self, i: u128) {
        match self {
            Self::Seeded(h) => h.write_u128(i),
            Self::Std(h) => h.write_u128(i),
        }
    }

    fn write_usize(&mut self, i: usize) {
        match self {
            Self::Seeded(h) => h.write_usize(i),
            Self::Std(h) => h.write_usize(i),
        }
    }
}

/// The correlation's keys, drawing and recording them on first use.
///
/// # The hot path allocates nothing and takes no lock
///
/// This runs on EVERY collection construction in an instrumented process —
/// `HashMap::new`, `with_capacity`, and every serde-deserialised map — on 100%
/// of traffic, sampled or not. Outside a span it is one fallible thread-local
/// read; inside one it is that read plus an `OnceLock::get`. Only the FIRST
/// collection in a correlation pays more, and what it pays is the draw itself:
/// the cell was allocated when the span was created, not here.
///
/// The draw runs OUTSIDE the cursor read. The cursor stack is a `RefCell`; a
/// span entered while the read's shared borrow is held — nothing stops the
/// recording hook from entering one — would `borrow_mut` it on the way in and
/// panic, inside a collection's `Default::default()`. Cloning the two handles
/// out and returning first costs one `Arc` clone each, once per correlation.
fn keys_for_current_correlation() -> Option<HashKeys> {
    enum Step {
        /// Keys already drawn — nothing more to do.
        Known(HashKeys),
        /// First collection in this correlation: the handles, cloned out so the
        /// draw runs with the cursor released.
        Draw(Arc<str>, HashKeyCell),
    }

    let step =
        crate::correlation_layer::with_current_hash_keys(|correlation, cell| match cell.get() {
            Some(keys) => Step::Known(*keys),
            None => Step::Draw(Arc::clone(correlation), Arc::clone(cell)),
        })?;

    Some(match step {
        Step::Known(keys) => keys,
        Step::Draw(correlation, cell) => *cell.get_or_init(|| draw_and_record(&correlation)),
    })
}

/// Draw a fresh key pair, or serve the recorded one, through a `Substitute`
/// boundary.
///
/// # This boundary FAIL-STOPS on a replay miss, deliberately
///
/// A miss here means the recording holds no keys for this correlation. The
/// default `Substitute` continuation — stop the request — is the right one and
/// must not be "fixed" into a graceful fallback. Absorbing the miss would hand
/// back a freshly drawn pair, every collection in the correlation would iterate
/// on keys the recording never held, and every downstream body diff would read
/// as a genuine divergence rather than as the artifact it is. A fail-stop's
/// unwind is at least recognisable as an unwind; that noise is not.
///
/// `ReplayStrategy::Execute` is wrong here for the same reason and is not a
/// remedy despite what the generic miss message suggests: executing re-runs the
/// draw, which is precisely the nondeterminism this seam exists to remove.
fn draw_and_record(correlation: &str) -> HashKeys {
    let caller = std::panic::Location::caller();
    let identity = CallsiteIdentity {
        version: 1,
        source: CallsiteSource::SyntacticHash,
        id: None,
        scope: Some(SCOPE.to_owned()),
        // Exactly one draw per correlation, so this is always 0. Allocated
        // through the shared counter anyway, so the numbering cannot drift from
        // every other boundary's.
        occurrence: crate::next_boundary_occurrence(
            Some(correlation),
            CallsiteSource::SyntacticHash,
            Some(SCOPE),
        ),
        caller_function: Some(OPERATION.to_owned()),
        lexical_path: Some(SCOPE.to_owned()),
        syntax_hash: Some(crate::stable_callsite_hash(SCOPE)),
        span_path: crate::current_span_path(),
    };
    let spec = BoundarySpec::with_semantics(
        BOUNDARY,
        COMPONENT,
        OPERATION,
        BoundarySemantics {
            replay_strategy: ReplayStrategy::Substitute,
            kind: Some(BOUNDARY.to_owned()),
            declaration: Some(BoundaryDeclaration::default().operation(OperationKind::Entropy)),
        },
    );
    let observation =
        CrossingObservation::with_correlation(spec, identity, caller, Some(correlation.to_owned()));

    dispatch(
        observation,
        // The draw is keyed by correlation alone — there is one per correlation,
        // and the lookup is already correlation-scoped.
        || serde_json::json!({}),
        draw_keys,
        |recorded| reconstruct_keys(&recorded),
        |keys: &HashKeys| (capture_keys(keys), false),
    )
}

/// The recorded image of a key pair. The pair is captured EXPOSED — a tape
/// holding a masked value substitutes nothing on replay.
fn capture_keys(keys: &HashKeys) -> serde_json::Value {
    serde_json::json!({ "k0": keys.k0, "k1": keys.k1 })
}

/// Rebuild a key pair from its recorded image.
///
/// Named rather than inlined so a test can drive it with a pair it CHOSE. Probed
/// through `DejaRandomState::default()` instead, the assertion would be the
/// constructor confirming its own output and could not tell a faithful
/// reconstruction from a fresh draw.
///
/// A malformed image is `Failed`, never a silent re-draw: `dispatch` turns that
/// into the unreconstructable fail-stop, which is right here for the same reason
/// the miss fail-stop is.
fn reconstruct_keys(recorded: &serde_json::Value) -> Reconstructed<HashKeys> {
    match (
        recorded.get("k0").and_then(serde_json::Value::as_u64),
        recorded.get("k1").and_then(serde_json::Value::as_u64),
    ) {
        (Some(k0), Some(k1)) => Reconstructed::Value(HashKeys { k0, k1 }),
        _ => Reconstructed::Failed(format!(
            "recorded hash-key event carried no k0/k1 pair: {recorded}"
        )),
    }
}

/// Draw a fresh key pair from the entropy std already seeded itself with.
///
/// `RandomState`'s own `k0`/`k1` are private, so the pair is derived by running
/// its hasher over two distinct tags. That is a PRF over the same OS-seeded
/// keys — it adds no dependency and inherits std's seeding quality — rather than
/// a second, weaker entropy source of our own.
fn draw_keys() -> HashKeys {
    let state = RandomState::new();
    let derive = |tag: u8| {
        let mut hasher = state.build_hasher();
        hasher.write_u8(tag);
        hasher.finish()
    };
    HashKeys {
        k0: derive(0),
        k1: derive(1),
    }
}

/// Install `keys` in the CURRENT span's cell WITHOUT going through the boundary.
///
/// Tests only, and the reason is the point: a test that probed this seam through
/// `DejaRandomState::default()` would be asking the constructor to confirm what
/// the constructor just did, and could not tell a real check from a tautology.
/// Writing the cell raw lets a test assert on keys it chose.
///
/// Returns whether there was an empty cell to write. A test that ignores a
/// `false` here goes on to assert against a fresh draw.
#[cfg(test)]
pub(crate) fn install_keys_for_test(keys: HashKeys) -> bool {
    crate::correlation_layer::with_current_hash_keys(|_, cell| cell.set(keys).is_ok())
        .unwrap_or(false)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    use tracing::Instrument;
    use tracing_subscriber::prelude::*;

    /// Run `f` with the correlation layer installed on this thread. Every span
    /// created inside gets a `SpanContext`, which is where a correlation's cell
    /// lives; a span created outside one is inert and seeds nothing.
    fn under_the_layer<T>(f: impl FnOnce() -> T) -> T {
        let subscriber = tracing_subscriber::registry().with(crate::DejaCorrelationLayer::new());
        tracing::subscriber::with_default(subscriber, f)
    }

    /// The ingress span carrying `correlation`, the shape `router_env::root_span`
    /// mints. Create it under the layer; it can then be entered from ANY thread,
    /// because a span handle carries its own subscriber.
    fn request_span(correlation: &str) -> tracing::Span {
        tracing::info_span!("deja::http_incoming", request_id = %correlation)
    }

    /// The keys `DejaRandomState::default()` would install, or `None` if it took
    /// the std arm. Reading the variant directly keeps the assertions about KEYS
    /// rather than about hash outputs, which would only compare equal or not.
    fn keys_now() -> Option<HashKeys> {
        match DejaRandomState::default() {
            DejaRandomState::Seeded(keys) => Some(keys),
            DejaRandomState::Std(_) => None,
        }
    }

    /// Property 1. Every collection in one correlation shares one key pair.
    ///
    /// This is the whole point of variant A: two maps built at different moments
    /// in one request must iterate the same way, so the order that reaches the
    /// wire is reproducible.
    #[test]
    fn one_correlation_shares_one_key_pair() {
        under_the_layer(|| {
            let request = request_span("corr-shared-pair");
            let _entered = request.enter();

            let first = keys_now().expect("inside a correlation the seeded arm must be taken");
            let second = keys_now().expect("inside a correlation the seeded arm must be taken");

            assert_eq!(
                first, second,
                "two collections in ONE correlation must share a key pair — if they \
                 differ, iteration order is per-collection again and the divergence \
                 this seam exists to close is back"
            );
        });
    }

    /// Property 1 across an await point, with the span entered and exited per
    /// poll the way `.instrument()` does in production.
    ///
    /// The cell must belong to the SPAN, not to an enter: a cell minted in
    /// `on_enter` would pass the test above and hand every poll a fresh pair.
    #[test]
    fn keys_survive_an_await() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");

        let (before, after) = under_the_layer(|| {
            runtime.block_on(
                async {
                    let before = keys_now().expect("seeded");
                    tokio::task::yield_now().await;
                    (before, keys_now().expect("seeded"))
                }
                .instrument(request_span("corr-across-await")),
            )
        });

        assert_eq!(
            before, after,
            "an await must not change the correlation's keys"
        );
    }

    /// Property 1 across `tokio::spawn` onto ANOTHER THREAD — its own test,
    /// because this is where a memo would silently degrade.
    ///
    /// The spawned task enters the request span for the first time on a worker
    /// thread. Two wrong shapes pass every test above and fail this one: a cell
    /// kept per thread, and a cell minted per enter. The thread ids are asserted
    /// distinct so the test cannot pass by the task landing on the spawning
    /// thread.
    #[test]
    fn keys_are_shared_across_tokio_spawn() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("multi-thread runtime");

        let (here, there) = under_the_layer(|| {
            runtime.block_on(async {
                let request = request_span("corr-across-spawn");
                let here = request.in_scope(|| (std::thread::current().id(), keys_now()));
                let there = tokio::spawn(
                    async { (std::thread::current().id(), keys_now()) }.instrument(request.clone()),
                )
                .await
                .expect("spawned task panicked");
                (here, there)
            })
        });

        assert_ne!(
            here.0, there.0,
            "precondition: the task must be polled on a worker thread, or this \
             cannot tell a per-thread cell from a shared one"
        );
        assert_eq!(
            here.1.expect("seeded"),
            there.1.expect("seeded on the worker"),
            "a spawned task in the same correlation must reuse the memoized keys \
             — a per-thread or per-enter cell would draw again here and \
             reintroduce the per-collection ordering this seam removes"
        );
    }

    /// Every span beneath the owner reads the owner's cell: a plain child, and a
    /// child that re-stamps the SAME `request_id`. An inner span carrying the
    /// field again is one request, not two; minting fresh on every stamped span
    /// would give one correlation two pairs and two `hash_seed` events.
    #[test]
    fn descendant_spans_share_the_owners_cell() {
        const CORRELATION: &str = "corr-descendants";
        under_the_layer(|| {
            let request = request_span(CORRELATION);
            let _entered = request.enter();
            let owner = keys_now().expect("seeded");

            let child = tracing::info_span!("child");
            assert_eq!(
                child
                    .in_scope(keys_now)
                    .expect("a child inherits the correlation"),
                owner,
                "a child span must read its parent's cell, not a fresh one"
            );

            let restamped = tracing::info_span!("inner", request_id = %CORRELATION);
            assert_eq!(
                restamped.in_scope(keys_now).expect("seeded"),
                owner,
                "a span re-stamping the same request_id is the same request and \
                 must not draw a second pair"
            );
        });
    }

    /// Property 2. Two correlations must NOT share keys.
    ///
    /// Both sides go through the real draw rather than an installed pair,
    /// because the shape this guards against is ONE cell for everyone — a
    /// `static OnceLock`, or a single cell cloned into every owner — which
    /// satisfies property 1 perfectly and is a constant seed for the life of the
    /// process, a far worse regression than dropping std's per-draw increment.
    /// A test that installed its own keys would bless it.
    #[test]
    fn two_correlations_get_different_keys() {
        under_the_layer(|| {
            let first = request_span("corr-distinct-a")
                .in_scope(keys_now)
                .expect("seeded");
            let second = request_span("corr-distinct-b")
                .in_scope(keys_now)
                .expect("seeded");

            assert_ne!(
                first, second,
                "two correlations sharing a key pair means the seed is effectively a \
                 constant, which hands every request the same iteration order and is \
                 a worse regression than the one this seam accepts within a request"
            );
        });
    }

    /// Property 3. Outside a correlation, std runs verbatim — including the
    /// per-draw increment, so two collections differ.
    ///
    /// Paired with property 1 on purpose: together they prove `Default::default`
    /// actually BRANCHES. Property 1 alone passes if everything is seeded;
    /// property 3 alone passes if nothing is.
    #[test]
    fn outside_a_correlation_std_runs_verbatim() {
        // No span: no correlation on this thread.
        let first = DejaRandomState::default();
        let second = DejaRandomState::default();

        assert!(
            matches!(first, DejaRandomState::Std(_)) && matches!(second, DejaRandomState::Std(_)),
            "outside a correlation deja must not seed anything — an unrecorded \
             process should not pay for, or be changed by, this type"
        );

        // std's keys are private, so the observable difference is the hash.
        let hash_with = |state: &DejaRandomState| {
            let mut hasher = state.build_hasher();
            hasher.write_u64(0xDEAD_BEEF);
            hasher.finish()
        };
        assert_ne!(
            hash_with(&first),
            hash_with(&second),
            "two std-arm states must hash differently — equal hashes mean the \
             per-draw increment (rust#36481) was dropped OUTSIDE a correlation, \
             where nothing justifies giving it up"
        );
    }

    /// A correlation entered into deja-context WITHOUT a span is not seeded.
    ///
    /// Per-correlation state lives on the span, and only there. A
    /// correlation-keyed registry beside it is the shape #100 removed: it needed
    /// its own eviction clock and got it wrong for every path that carried a
    /// correlation past the response. A bare `deja_context::enter` has no span
    /// to hold a cell, so the std arm is the right answer, not a fresh draw.
    #[test]
    fn a_correlation_entered_without_a_span_is_not_seeded() {
        let _guard = deja_context::enter(deja_context::ContextSnapshot::new("corr-no-span"));

        assert!(
            matches!(DejaRandomState::default(), DejaRandomState::Std(_)),
            "a correlation with no span has nowhere to keep a key pair; seeding it \
             means a second, correlation-keyed home for per-correlation state"
        );
    }

    /// A request the ingress sampled OUT is not seeded, though its span carries
    /// a correlation.
    ///
    /// The cursor carries the ENGAGED correlation, not the raw field. A `Skip`
    /// decision means nothing keyed by correlation happens under that span, and
    /// drawing a pair for it would be a boundary dispatch on a request that
    /// opted out of every boundary. The span path is asserted first so the test
    /// cannot pass by the span never having been live under the layer.
    #[test]
    fn a_sampled_out_request_is_not_seeded() {
        const CORRELATION: &str = "corr-sampled-out";
        deja_context::set_recording_decision(CORRELATION, deja_context::RecordDecision::Skip);
        let (path, seeded) = under_the_layer(|| {
            request_span(CORRELATION).in_scope(|| (crate::current_span_path(), keys_now()))
        });
        deja_context::clear_recording_decision(CORRELATION);

        assert_eq!(
            path.as_deref(),
            Some("deja::http_incoming"),
            "precondition: the request span must be live under the layer"
        );
        assert!(
            seeded.is_none(),
            "a sampled-out request must take the std arm — it engages no \
             correlation, so it has no cell to draw into"
        );
    }

    /// The cell feeds the hasher. Probed with a pair chosen HERE and written
    /// straight into the current span's cell, so a fresh draw cannot pass by
    /// coincidence.
    #[test]
    fn the_installed_pair_is_the_pair_the_hasher_uses() {
        let chosen = HashKeys {
            k0: 0x0123_4567_89AB_CDEF,
            k1: 0xFEDC_BA98_7654_3210,
        };
        under_the_layer(|| {
            let request = request_span("corr-installed-pair");
            let _entered = request.enter();

            assert!(
                install_keys_for_test(chosen),
                "precondition: the span must hold an empty cell to write, or the \
                 assertion below is against a fresh draw"
            );
            assert_eq!(
                keys_now().expect("seeded"),
                chosen,
                "the correlation's memoized pair must be served verbatim"
            );
        });
    }

    /// A recorded image is rebuilt into the pair it holds, and a malformed one
    /// fails rather than quietly re-drawing.
    #[test]
    fn a_recorded_pair_reconstructs_and_a_malformed_one_fails() {
        let chosen = HashKeys { k0: 11, k1: 22 };

        match reconstruct_keys(&capture_keys(&chosen)) {
            Reconstructed::Value(keys) => assert_eq!(
                keys, chosen,
                "replay must serve the pair the recording holds"
            ),
            Reconstructed::Failed(why) => panic!("a well-formed image must rebuild: {why}"),
        }

        assert!(
            matches!(
                reconstruct_keys(&serde_json::json!({ "k0": 1 })),
                Reconstructed::Failed(_)
            ),
            "a half-written image must fail-stop, not fall back to a fresh draw — \
             a drawn pair the recording never held is exactly what this seam exists \
             to prevent"
        );
    }

    /// `Debug` must not print the keys.
    #[test]
    fn debug_masks_the_keys() {
        let rendered = format!(
            "{:?}",
            HashKeys {
                k0: 0xAAAA_AAAA_AAAA_AAAA,
                k1: 0xBBBB_BBBB_BBBB_BBBB,
            }
        );
        assert!(
            !rendered.contains("aaaa") && !rendered.contains("12297829382473034410"),
            "a key pair must not reach a log line through Debug: {rendered}"
        );
        assert!(
            rendered.contains("***"),
            "the mask should be visible: {rendered}"
        );
    }

    /// Building a collection while thread-locals are being torn down must not
    /// take the process down.
    ///
    /// This is the hardest rule deja has: recording never fails the service. The
    /// span cursor stack is a thread-local with a destructor, and `LocalKey::with`
    /// panics once that has run — a panic inside a `Drop` during teardown is a
    /// double panic and an abort, which no `catch_unwind` can contain.
    ///
    /// The ordering is deliberate. TLS destructors run in reverse registration
    /// order, so the bomb is registered FIRST and the cursor stack SECOND; the
    /// stack is therefore destroyed while the bomb still has to run. Swap
    /// `try_with` back to `with` in `with_current_cursor` and this test aborts
    /// the whole test binary rather than failing — which is precisely the
    /// production failure it stands for. `correlation_layer` has the same test
    /// against the door itself; this one reaches it the way production does,
    /// through `Default::default()`.
    #[test]
    fn a_collection_built_during_tls_teardown_does_not_abort() {
        struct BuildsAMapWhileDying;

        impl Drop for BuildsAMapWhileDying {
            fn drop(&mut self) {
                let mut map: HashMap<u32, u32, DejaRandomState> = HashMap::default();
                map.insert(1, 2);
                assert_eq!(map.get(&1), Some(&2));
            }
        }

        thread_local! {
            static BOMB: BuildsAMapWhileDying = const { BuildsAMapWhileDying };
        }

        std::thread::spawn(|| {
            // Register the bomb first so it is dropped LAST...
            BOMB.with(|_| {});
            // ...and the cursor stack second, so it is already gone by then.
            under_the_layer(|| {
                let request = request_span("corr-tls-teardown");
                let _entered = request.enter();
                let _ = keys_now();
            });
        })
        .join()
        .expect("the thread must exit cleanly; a panic in a TLS destructor aborts");
    }

    /// A `spawn_fork` tail reuses its request's key pair.
    ///
    /// `spawn_fork` is the one detached path here that carries a correlation by
    /// CONTEXT SNAPSHOT rather than by a held span handle — `capture_current()` +
    /// `scope_snapshot`, instrumented with a fresh `fork_span()` rather than
    /// `in_current_span()`. With the cell on the span, the question is whether
    /// that fork span holds it: it does, because `fork_span()` is created while
    /// the request span is current and is therefore that span's CHILD, so
    /// `on_new_span` hands it the parent's handle. If a future change makes the
    /// fork span parentless, the tail takes the std arm and this fails, rather
    /// than the tape quietly gaining a second `hash_seed` event.
    ///
    /// Current-thread runtime and a thread-local subscriber, so the tail is polled
    /// on this thread after the request span's guard is gone — the ordering
    /// `fork_retains_request_context.rs` pins for the same reason.
    #[test]
    fn a_spawn_fork_tail_reuses_the_requests_keys() {
        const CORRELATION: &str = "corr-fork-tail";
        let tail_saw: Arc<Mutex<Option<HashKeys>>> = Arc::new(Mutex::new(None));
        let tail_ran_under: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");

        let request_keys = under_the_layer(|| {
            runtime.block_on(async {
                let request_keys = {
                    let request = request_span(CORRELATION);
                    let _entered = request.enter();

                    let drawn = keys_now().expect("the request itself must be seeded");

                    let saw = Arc::clone(&tail_saw);
                    let under = Arc::clone(&tail_ran_under);
                    crate::spawn_fork(async move {
                        *under.lock().unwrap_or_else(|p| p.into_inner()) =
                            crate::current_span_path();
                        *saw.lock().unwrap_or_else(|p| p.into_inner()) = keys_now();
                    });

                    drawn
                };
                // The request span's guard is dropped; let the tail be polled
                // only after that, which is what makes this a real question.
                tokio::task::yield_now().await;
                tokio::task::yield_now().await;
                request_keys
            })
        });

        // VACUITY GUARD. The tail must have run under its OWN fork span, as a
        // child of the request span — the path says both. Polled under the
        // request span's enter instead, "the tail matched" would prove nothing.
        assert_eq!(
            tail_ran_under
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .as_deref(),
            Some("deja::http_incoming>deja.fork"),
            "precondition: the tail must run under a fork span that is a child of \
             the request span"
        );

        let tail_keys = tail_saw
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .expect("the tail must have run and taken the seeded arm");

        assert_eq!(
            tail_keys, request_keys,
            "a detached tail must iterate the same way its request did. Drawing \
             again here gives one correlation two key pairs and puts a second \
             hash_seed event on the tape, which is the exactly-once property gone"
        );
    }
}
