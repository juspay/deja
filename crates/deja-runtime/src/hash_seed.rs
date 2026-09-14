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
//! memoized. The alternative — a per-collection counter, seed+N — is NOT
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

use std::collections::HashMap;
use std::hash::{BuildHasher, Hasher, RandomState};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};

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

/// Per-correlation memo cell.
///
/// `OnceLock` rather than a plain map entry because the draw must happen exactly
/// once per correlation even when several threads inside the correlation race to
/// build their first collection: `get_or_init` runs its closure once and blocks
/// the losers until it returns. A plain check-then-insert would record N events
/// for N racing threads, and the ordering nondeterminism would be back.
///
/// It is an `Arc` so the global lock is released BEFORE the draw runs. Holding it
/// across a recording dispatch would serialize every collection construction in
/// the process behind one boundary write.
type Cell = Arc<OnceLock<HashKeys>>;

static HASH_KEYS: LazyLock<Mutex<HashMap<String, Cell>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

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
/// # The hot path allocates nothing
///
/// This runs on EVERY collection construction in an instrumented process —
/// `HashMap::new`, `with_capacity`, and every serde-deserialised map — on 100%
/// of traffic, sampled or not. So the two common answers are both allocation
/// free: outside a correlation it is one fallible thread-local read; inside an
/// established one it is that read plus a lock and a lookup by `&str`. Only the
/// FIRST collection in a correlation allocates, and only to own the map key.
///
/// `current_correlation_id()` is deliberately not used here — it clones a
/// `String` per call, which on this path is an allocation added to traffic that
/// currently has none.
fn keys_for_current_correlation() -> Option<HashKeys> {
    enum Step {
        /// Established correlation, keys already drawn — nothing more to do.
        Known(HashKeys),
        /// First collection in this correlation: draw OUTSIDE the thread-local
        /// borrow, so the boundary dispatch never runs under it.
        Draw(String, Cell),
        /// No correlation, or the context could not be read at all.
        None,
    }

    let step = deja_context::with_current_correlation_id(|correlation| {
        let Some(correlation) = correlation else {
            return Step::None;
        };
        let cell = cell_for(correlation);
        match cell.get() {
            Some(keys) => Step::Known(*keys),
            None => Step::Draw(correlation.to_owned(), cell),
        }
    });

    match step {
        Step::Known(keys) => Some(keys),
        Step::Draw(correlation, cell) => Some(*cell.get_or_init(|| draw_and_record(&correlation))),
        Step::None => None,
    }
}

/// Get or create this correlation's memo cell, holding the global lock only for
/// the map operation itself.
fn cell_for(correlation: &str) -> Cell {
    let mut memo = HASH_KEYS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(cell) = memo.get(correlation) {
        return Arc::clone(cell);
    }
    let cell: Cell = Arc::new(OnceLock::new());
    memo.insert(correlation.to_owned(), Arc::clone(&cell));
    cell
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

/// Discard the memoized keys for `correlation_id`.
///
/// Called from the correlation layer when the span owning the correlation
/// closes — the same clock, and for the same reason, as
/// [`crate::clear_fork_counters_for_correlation`]: a span closes after every
/// task carrying it has finished, and it closes for sampled-out requests too.
/// Without this the memo grows by one entry per request forever.
///
/// Any `Arc` already handed out stays valid; only the map's own reference goes.
pub(crate) fn clear_hash_keys_for_correlation(correlation_id: Option<&str>) {
    let Some(correlation_id) = correlation_id else {
        return;
    };
    if let Ok(mut memo) = HASH_KEYS.lock() {
        memo.remove(correlation_id);
    }
}

/// Whether the memo currently holds a cell for `correlation_id`. Tests only:
/// eviction is otherwise unobservable, and an eviction nobody can see is an
/// eviction nobody can prove.
#[cfg(test)]
pub(crate) fn memo_holds(correlation_id: &str) -> bool {
    HASH_KEYS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains_key(correlation_id)
}

/// Install `keys` for `correlation_id` WITHOUT going through the boundary.
///
/// Tests only, and the reason is the point: a test that probed this seam through
/// `DejaRandomState::default()` would be asking the constructor to confirm what
/// the constructor just did, and could not tell a real check from a tautology.
/// Writing the cell raw lets a test assert on keys it chose.
#[cfg(test)]
pub(crate) fn install_keys_for_test(correlation_id: &str, keys: HashKeys) {
    let cell = cell_for(correlation_id);
    let _ = cell.set(keys);
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Enter `correlation` for the duration of the returned guard.
    fn in_correlation(correlation: &str) -> deja_context::ContextGuard {
        deja_context::enter(deja_context::ContextSnapshot::new(correlation))
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
        let _guard = in_correlation("corr-shared-pair");

        let first = keys_now().expect("inside a correlation the seeded arm must be taken");
        let second = keys_now().expect("inside a correlation the seeded arm must be taken");

        assert_eq!(
            first, second,
            "two collections in ONE correlation must share a key pair — if they \
             differ, iteration order is per-collection again and the divergence \
             this seam exists to close is back"
        );
    }

    /// Property 1, across an await point. The memo must not be tied to a
    /// contiguous stretch of synchronous execution.
    #[tokio::test]
    async fn keys_survive_an_await() {
        let _guard = in_correlation("corr-across-await");

        let before = keys_now().expect("seeded");
        tokio::task::yield_now().await;
        let after = keys_now().expect("seeded");

        assert_eq!(
            before, after,
            "an await must not change the correlation's keys"
        );
    }

    /// Property 1, across `tokio::spawn` — its own test, because this is where
    /// the memo would silently degrade.
    ///
    /// The failure this catches is a THREAD-LOCAL memo: it would pass every test
    /// above and still hand a spawned task its own key pair, so a response
    /// assembled partly on a worker thread would iterate two ways. A
    /// multi-threaded runtime is required or the spawned task may land back on
    /// the same thread and the bug hides.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn keys_are_shared_across_tokio_spawn() {
        const CORRELATION: &str = "corr-across-spawn";
        let _guard = in_correlation(CORRELATION);

        let on_this_thread = keys_now().expect("seeded");

        let on_worker = tokio::spawn(async move {
            // The spawned task re-enters the SAME correlation; the memo it finds
            // must be the process-wide one, not a fresh per-thread cell.
            let _guard = in_correlation(CORRELATION);
            keys_now()
        })
        .await
        .expect("spawned task panicked")
        .expect("seeded");

        assert_eq!(
            on_this_thread, on_worker,
            "a spawned task in the same correlation must reuse the memoized keys \
             — a thread-local memo would draw again here and reintroduce the \
             per-collection ordering this seam removes"
        );
    }

    /// Property 2. Two correlations must NOT share keys.
    ///
    /// Both sides go through the real draw path rather than an installed pair,
    /// because the implementation this is guarding against is a fixed constant
    /// seed — which would satisfy property 1 perfectly and is a far worse
    /// regression than dropping std's per-draw increment. A test that installed
    /// its own keys would bless it.
    #[test]
    fn two_correlations_get_different_keys() {
        let first = {
            let _guard = in_correlation("corr-distinct-a");
            keys_now().expect("seeded")
        };
        let second = {
            let _guard = in_correlation("corr-distinct-b");
            keys_now().expect("seeded")
        };

        assert_ne!(
            first, second,
            "two correlations sharing a key pair means the seed is effectively a \
             constant, which hands every request the same iteration order and is \
             a worse regression than the one this seam accepts within a request"
        );
    }

    /// Property 3. Outside a correlation, std runs verbatim — including the
    /// per-draw increment, so two collections differ.
    ///
    /// Paired with property 1 on purpose: together they prove `Default::default`
    /// actually BRANCHES. Property 1 alone passes if everything is seeded;
    /// property 3 alone passes if nothing is.
    #[test]
    fn outside_a_correlation_std_runs_verbatim() {
        // No guard: no correlation on this thread.
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

    /// The memo feeds the hasher. Probed with a pair chosen HERE and written
    /// straight into the cell, so a fresh draw cannot pass by coincidence.
    #[test]
    fn the_installed_pair_is_the_pair_the_hasher_uses() {
        const CORRELATION: &str = "corr-installed-pair";
        let chosen = HashKeys {
            k0: 0x0123_4567_89AB_CDEF,
            k1: 0xFEDC_BA98_7654_3210,
        };
        install_keys_for_test(CORRELATION, chosen);

        let _guard = in_correlation(CORRELATION);
        assert_eq!(
            keys_now().expect("seeded"),
            chosen,
            "the correlation's memoized pair must be served verbatim"
        );
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

    /// The memo is evicted when the correlation's span closes, or it grows by one
    /// entry per request for the life of the process.
    #[test]
    fn eviction_forgets_the_correlation() {
        const CORRELATION: &str = "corr-evicted";
        {
            let _guard = in_correlation(CORRELATION);
            let _ = keys_now();
        }
        assert!(
            memo_holds(CORRELATION),
            "precondition: the draw must have populated the memo, or the eviction \
             assertion below would pass against an empty map"
        );

        clear_hash_keys_for_correlation(Some(CORRELATION));

        assert!(
            !memo_holds(CORRELATION),
            "a closed correlation must leave nothing behind"
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
    /// context cell holds a `String`, so it HAS a destructor, and `LocalKey::with`
    /// panics once that has run — a panic inside a `Drop` during teardown is a
    /// double panic and an abort, which no `catch_unwind` can contain.
    ///
    /// The ordering is deliberate. TLS destructors run in reverse registration
    /// order, so the bomb is registered FIRST and the context cell SECOND; the
    /// context is therefore destroyed while the bomb still has to run. Swap
    /// `try_with` back to `with` in `deja_context` and this test aborts the whole
    /// test binary rather than failing — which is precisely the production
    /// failure it stands for.
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
            // ...and the context cell second, so it is already gone by then.
            let _guard = in_correlation("corr-tls-teardown");
            let _ = keys_now();
        })
        .join()
        .expect("the thread must exit cleanly; a panic in a TLS destructor aborts");
    }

    /// A `spawn_fork` tail reuses its request's key pair, and the memo is still
    /// there when it runs.
    ///
    /// `spawn_fork` is the one detached path here that carries a correlation by
    /// CONTEXT SNAPSHOT rather than by a held span handle — `capture_current()` +
    /// `scope_snapshot`, instrumented with a fresh `fork_span()` rather than
    /// `in_current_span()`. That made it the candidate for a real defect: if the
    /// request's span could close before the tail ran, eviction would fire, the
    /// tail would draw again, and one correlation would end up with two key pairs
    /// and two `hash_seed` events.
    ///
    /// It does not happen, and the assertion below records WHY rather than just
    /// that: `fork_span()` is created while the request span is current, so it is
    /// that span's CHILD, and tracing's registry keeps a parent open until its
    /// children close. The request span therefore cannot close while the tail is
    /// outstanding — the same structural protection `.in_current_span()` gives,
    /// reached by a different route. This test is the lock on that property; if a
    /// future change makes the fork span parentless, this fails rather than the
    /// tape quietly gaining a second event.
    ///
    /// Current-thread runtime and a thread-local subscriber, so the tail is polled
    /// on this thread and `on_close` can actually run — the ordering
    /// `fork_retains_request_context.rs` pins for the same reason.
    #[test]
    fn a_spawn_fork_tail_reuses_the_requests_keys() {
        use tracing_subscriber::prelude::*;

        const CORRELATION: &str = "corr-fork-tail";
        let tail_saw: Arc<Mutex<Option<HashKeys>>> = Arc::new(Mutex::new(None));
        let memo_held_when_tail_ran: Arc<Mutex<Option<bool>>> = Arc::new(Mutex::new(None));

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("current-thread runtime");

        let subscriber = tracing_subscriber::registry().with(crate::DejaCorrelationLayer::new());
        let request_keys = tracing::subscriber::with_default(subscriber, || {
            runtime.block_on(async {
                let request_keys = {
                    let request = tracing::info_span!(
                        "deja::http_incoming",
                        request_id = %CORRELATION
                    );
                    let _entered = request.enter();

                    let drawn = keys_now().expect("the request itself must be seeded");

                    let saw = Arc::clone(&tail_saw);
                    let held = Arc::clone(&memo_held_when_tail_ran);
                    crate::spawn_fork(async move {
                        // Record whether the memo was STILL populated at the
                        // moment the tail ran. That is the fact that decides
                        // which mechanism is protecting us.
                        *held.lock().unwrap_or_else(|p| p.into_inner()) =
                            Some(memo_holds(CORRELATION));
                        *saw.lock().unwrap_or_else(|p| p.into_inner()) = keys_now();
                    });

                    drawn
                };
                // The request span is dropped; let teardown win the race before
                // the tail is polled, which is what makes this a defect and not
                // a theory.
                tokio::task::yield_now().await;
                tokio::task::yield_now().await;
                request_keys
            })
        });

        // VACUITY GUARD. Eviction must be live in this setup, or "the tail
        // matched" would prove nothing — it could just mean nothing is ever
        // cleared. The span has closed by now, so the memo must be gone.
        assert!(
            !memo_holds(CORRELATION),
            "precondition: the request span must CLOSE and evict once the tail is \
             done, or this setup cannot tell a reused pair from a memo that is \
             simply never cleared"
        );

        let held = memo_held_when_tail_ran
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .expect("the tail must have run");
        assert!(
            held,
            "the memo must still be populated when the tail runs — that is the \
             span-lifetime guarantee this seam leans on. If this flips to false, \
             `fork_span()` has stopped being a child of the request span, and the \
             tail is now drawing its own pair"
        );

        let tail_keys = tail_saw
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .expect("the tail must have run and been inside the correlation");

        assert_eq!(
            tail_keys, request_keys,
            "a detached tail must iterate the same way its request did. Drawing \
             again here gives one correlation two key pairs and puts a second \
             hash_seed event on the tape, which is the exactly-once property gone"
        );
    }
}
