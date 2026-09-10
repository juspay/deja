//! Explicit, per-collection hash seeding.
//!
//! # The divergence this closes
//!
//! `std::hash::RandomState::new()` draws a fresh key pair per collection, so a
//! `HashMap` or `HashSet` iterates in a different order every process. Any order
//! that reaches the wire — a serialized object's key order, a list built by
//! iterating a set — differs between a recording and its replay for a reason
//! that has nothing to do with the candidate. Recording the keys and replaying
//! them removes that whole class of difference.
//!
//! `BuildHasher` is the seam, which is why ONE type covers it: `HashMap`,
//! `HashSet`, and anything generic over `S: BuildHasher` (IndexMap included).
//! Nothing per-structure is needed.
//!
//! # Why the seed is asked for, not defaulted
//!
//! A collection could be seeded implicitly, by making `Default` branch on
//! ambient correlation state — `HashMap::new()` would keep working and every
//! collection in the process would be covered. That was tried, and every cost it
//! carried flowed from that one decision: `Default` runs on the hot path (495
//! constructions in the vendor tree), so it needed a per-correlation memo; the
//! memo needed somewhere to live, which was a correlation-keyed registry; and
//! reading ambient state from inside `HashMap::new()` put a thread-local
//! borrow on a path that also runs during thread teardown, where it aborts the
//! process rather than panicking.
//!
//! None of that is essential to recording a seed. It is all the cost of not
//! being asked. So there is deliberately **no `Default` impl**: a collection
//! cannot become seeded by accident, and the type will not construct without the
//! caller saying which seed. That is more invasive per site and needs no
//! machinery at all — a good trade only because the set of collections whose
//! order reaches the wire is small, and is found from order-only body diffs
//! rather than guessed at.
//!
//! # Why SipHash-1-3 from `siphasher`, not `DefaultHasher`
//!
//! `DefaultHasher::new()` is hardcoded to `new_with_keys(0, 0)` and std exposes
//! no keyed constructor. Prefixing a `DefaultHasher` with the keys would
//! compile, but std explicitly does not guarantee `DefaultHasher`'s algorithm
//! across releases — and record and replay can run different toolchains, so a
//! silent algorithm change would reintroduce exactly the order divergence this
//! exists to remove, in a form nothing would attribute correctly. `siphasher`
//! provides the stable implementation std declines to promise, and SipHash-1-3
//! is the same algorithm std uses, so the only thing that differs from stock
//! `RandomState` is where the keys come from.
//!
//! # The security argument, and why it is structural
//!
//! Randomized keys exist to stop an attacker crafting colliding entries
//! (rust#36481). A seed this module SYNTHESIZES on a replay miss is derived from
//! the collection's name and the correlation, so anyone holding those can
//! predict it.
//!
//! That is safe for a reason that does not depend on anyone being careful:
//! synthesis is unreachable outside replay. Record and disabled modes never
//! perform a lookup, so they never miss, so they never reach the miss arm — they
//! draw fresh keys from the OS-seeded entropy std already holds. Predictable
//! keys exist only in a replay harness, never in front of real traffic.

use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

use siphasher::sip::SipHasher13;

use crate::{
    dispatch, BoundaryDeclaration, BoundarySemantics, BoundarySpec, CallsiteIdentity,
    CallsiteSource, CrossingObservation, OperationKind, ReconstructInput, Reconstructed,
    ReplayStrategy,
};

const BOUNDARY: &str = "hash_seed";
const COMPONENT: &str = "deja_runtime::hash_seed";
const OPERATION: &str = "draw_hash_keys";

/// The two SipHash keys one collection hashes by.
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
/// the whole reason std randomizes them; leaking them through `{:?}` would give
/// that away for free.
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

/// A `BuildHasher` whose keys are recorded and replayed.
///
/// Obtained from [`hash_seed`] and nowhere else. **There is no `Default` impl**,
/// on purpose — see the module docs. Clone it to give one seed to several
/// collections that must agree on iteration order.
#[derive(Debug, Clone, Copy)]
pub struct DejaBuildHasher {
    keys: HashKeys,
}

impl DejaBuildHasher {
    /// Build one from a key pair directly, bypassing the boundary.
    ///
    /// For tests that need to drive a KNOWN pair. Production code takes the pair
    /// from [`hash_seed`] so it is recorded; a pair minted here is recorded
    /// nowhere and replays as nothing.
    pub fn from_keys(keys: HashKeys) -> Self {
        Self { keys }
    }

    /// The keys this seed hashes by.
    pub fn keys(&self) -> HashKeys {
        self.keys
    }
}

impl BuildHasher for DejaBuildHasher {
    type Hasher = DejaHasher;

    fn build_hasher(&self) -> Self::Hasher {
        DejaHasher(SipHasher13::new_with_keys(self.keys.k0, self.keys.k1))
    }
}

/// The hasher [`DejaBuildHasher`] builds. Holds no keys of its own and draws
/// nothing.
#[derive(Debug, Clone)]
pub struct DejaHasher(SipHasher13);

/// Forward every `write_*` explicitly rather than leaning on the trait defaults.
///
/// The defaults funnel the integer writes through `write(&bytes)`, which hashes
/// the same value differently from the specialized methods `SipHasher13`
/// implements. Both sides of a record/replay pair run this same code, so
/// consistency would hold either way — but going through the wrapped hasher's
/// own methods keeps this a transparent wrapper rather than quietly a different
/// hash function. (The signed `write_i*` defaults forward to the unsigned
/// methods on this same impl, so they are covered.)
impl Hasher for DejaHasher {
    fn finish(&self) -> u64 {
        self.0.finish()
    }
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
    fn write_u8(&mut self, i: u8) {
        self.0.write_u8(i);
    }
    fn write_u16(&mut self, i: u16) {
        self.0.write_u16(i);
    }
    fn write_u32(&mut self, i: u32) {
        self.0.write_u32(i);
    }
    fn write_u64(&mut self, i: u64) {
        self.0.write_u64(i);
    }
    fn write_u128(&mut self, i: u128) {
        self.0.write_u128(i);
    }
    fn write_usize(&mut self, i: usize) {
        self.0.write_usize(i);
    }
}

/// A `HashMap` whose iteration order is recorded and replayed.
///
/// `std::collections::HashMap::with_hasher(deja::hash_seed("..."))` builds one.
/// The alias exists because the hasher is a third type parameter that would
/// otherwise have to be written at every mention of the type.
pub type SeededHashMap<K, V> = std::collections::HashMap<K, V, DejaBuildHasher>;

/// A `HashSet` whose iteration order is recorded and replayed. See
/// [`SeededHashMap`].
pub type SeededHashSet<T> = std::collections::HashSet<T, DejaBuildHasher>;

/// Draw the hash seed for one named collection, recording it.
///
/// ```ignore
/// let seed = deja::hash_seed("routing::eligible_connectors");
/// let mut map: deja::SeededHashMap<K, V> = HashMap::with_hasher(seed);
/// let mut set: deja::SeededHashSet<T> = HashSet::with_hasher(seed);
/// ```
///
/// `name` is the collection's ADDRESS — a rank-1 explicit tag on the existing
/// callsite ladder, not a new naming scheme. It must be a literal, so it is
/// stable across candidates: two candidates replaying one tape resolve the same
/// name to the same recorded keys and therefore iterate identically, which is
/// the entire point.
///
/// One seed per named collection rather than one per correlation. Sharing a
/// correlation-wide seed was a way to keep event volume down when every
/// collection in the process was covered; with a handful of named ones that
/// pressure is gone, and per-collection attributes an order difference to the
/// collection that caused it.
///
/// # Behaviour by mode
///
/// - **Record / disabled** — draw fresh keys and (when recording) capture them.
/// - **Replay, hit** — the recorded keys.
/// - **Replay, miss** — keys synthesized from the name and correlation. A miss
///   here means the candidate built a collection the recording never had, and
///   the alternative would be to stop the request over a hash seed. Synthesized
///   keys are stable run to run, so an order difference they cause is
///   attributable rather than noise — see [`crate::synth`].
#[track_caller]
pub fn hash_seed(name: &'static str) -> DejaBuildHasher {
    let caller = std::panic::Location::caller();
    let scope = format!("deja::hash_seed::{name}");
    let correlation = deja_context::current_correlation_id();
    let identity = CallsiteIdentity {
        version: 1,
        // Rank 1: the caller NAMED this collection, which is the strongest
        // address there is and the reason the name has to be a literal.
        source: CallsiteSource::Explicit,
        id: Some(name.to_owned()),
        scope: Some(scope.clone()),
        occurrence: crate::next_boundary_occurrence(
            correlation.as_deref(),
            CallsiteSource::Explicit,
            Some(&scope),
        ),
        caller_function: Some(OPERATION.to_owned()),
        lexical_path: Some(scope.clone()),
        syntax_hash: Some(crate::stable_callsite_hash(&scope)),
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
    let observation = CrossingObservation::with_correlation(spec, identity, caller, correlation);

    let keys = dispatch(
        observation,
        // The name rides the ARGS as well as the address, because the miss arm
        // derives the synthesized keys from the args image: without it two
        // differently-named collections would synthesize the same pair.
        move || serde_json::json!({ "name": name }),
        draw_keys,
        |input| match input {
            ReconstructInput::Hit(recorded) => reconstruct_keys(&recorded),
            // Synthesize rather than stop. `Execute` would be wrong for the same
            // reason a live miss answer is wrong everywhere else: re-running the
            // draw is precisely the nondeterminism this seam exists to remove.
            ReconstructInput::Miss(miss) => Reconstructed::Synthesized(synthesize_keys(miss)),
        },
        |keys: &HashKeys| (capture_keys(keys), false),
    );
    DejaBuildHasher { keys }
}

/// The recorded image of a key pair. Captured EXPOSED — a tape holding a masked
/// value substitutes nothing on replay.
fn capture_keys(keys: &HashKeys) -> serde_json::Value {
    serde_json::json!({ "k0": keys.k0, "k1": keys.k1 })
}

/// Rebuild a key pair from its recorded image.
///
/// Named rather than inlined so a test can drive it with a pair it CHOSE. Probed
/// through `hash_seed` instead, the assertion would be the constructor
/// confirming its own output and could not tell a faithful reconstruction from a
/// fresh draw.
///
/// A malformed image is `Failed`, never a silent re-draw: the seam turns that
/// into the unreconstructable fail-stop, which is right here for the same reason
/// it is right everywhere else.
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

/// Derive a key pair for a collection the recording never had.
///
/// A pure function of the miss — name, correlation, occurrence — so the same
/// unrecorded collection seeds identically on every replay, and two DIFFERENT
/// candidates replaying one tape agree on the order of a collection neither
/// recording covers.
fn synthesize_keys(miss: &crate::SubstituteMiss) -> HashKeys {
    let drawn = crate::synth::bytes::<16>(miss);
    let mut k0 = [0u8; 8];
    let mut k1 = [0u8; 8];
    k0.copy_from_slice(&drawn[..8]);
    k1.copy_from_slice(&drawn[8..]);
    HashKeys {
        k0: u64::from_le_bytes(k0),
        k1: u64::from_le_bytes(k1),
    }
}

/// Draw a fresh key pair from the entropy std already seeded itself with.
///
/// `RandomState`'s own keys are private, so the pair is derived by running its
/// hasher over two distinct tags. That is a PRF over the same OS-seeded keys —
/// it adds no entropy dependency and inherits std's seeding quality — rather
/// than a second, weaker source of our own.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    fn keys(k0: u64, k1: u64) -> DejaBuildHasher {
        DejaBuildHasher::from_keys(HashKeys { k0, k1 })
    }

    fn hash_of(seed: &DejaBuildHasher, value: &str) -> u64 {
        let mut hasher = seed.build_hasher();
        hasher.write(value.as_bytes());
        hasher.finish()
    }

    /// Without this the module is inert: everything else could pass while the
    /// keys were being ignored.
    #[test]
    fn the_keys_decide_the_hash() {
        assert_eq!(
            hash_of(&keys(1, 2), "k"),
            hash_of(&keys(1, 2), "k"),
            "one key pair must hash one input the same way every time"
        );
        assert_ne!(hash_of(&keys(1, 2), "k"), hash_of(&keys(3, 4), "k"));
        assert_ne!(
            hash_of(&keys(1, 2), "k"),
            hash_of(&keys(2, 1), "k"),
            "the two keys must not be interchangeable"
        );
    }

    /// THE POINT. Iteration order is a function of the seed, so replaying the
    /// seed replays the order.
    #[test]
    fn iteration_order_follows_the_seed() {
        let entries = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta", "eta"];
        let order = |seed: DejaBuildHasher| {
            let mut map: SeededHashMap<&str, u32> = HashMap::with_hasher(seed);
            for (i, e) in entries.iter().enumerate() {
                map.insert(*e, i as u32);
            }
            map.into_keys().collect::<Vec<_>>()
        };

        assert_eq!(
            order(keys(7, 11)),
            order(keys(7, 11)),
            "the same seed must give the same order — this is what replaying the \
             recorded keys buys"
        );

        // Deterministic search rather than one hopeful pair: SOME other seed must
        // reorder these entries, or the seed is not reaching the iteration order.
        let baseline = order(keys(7, 11));
        assert!(
            (1u64..64).any(|n| order(keys(n, n + 1)) != baseline),
            "no seed reordered the entries; the seed is not reaching iteration order"
        );
    }

    /// One seed can serve several collections that must agree with each other.
    #[test]
    fn one_seed_serves_a_map_and_a_set() {
        let seed = keys(7, 11);
        let mut set: SeededHashSet<&str> = HashSet::with_hasher(seed);
        let mut map: SeededHashMap<&str, u32> = HashMap::with_hasher(seed);
        for (i, e) in ["alpha", "beta", "gamma", "delta"].iter().enumerate() {
            set.insert(*e);
            map.insert(*e, i as u32);
        }
        assert_eq!(
            set.into_iter().collect::<Vec<_>>(),
            map.into_keys().collect::<Vec<_>>(),
            "a map and a set under one seed must agree on order"
        );
    }

    /// The recorded image round-trips, and is driven with a pair the test CHOSE
    /// so it cannot be the constructor confirming its own output.
    #[test]
    fn a_recorded_pair_rebuilds_exactly() {
        let original = HashKeys {
            k0: 0xdead_beef_1234_5678,
            k1: 0x0fed_cba9_8765_4321,
        };
        match reconstruct_keys(&capture_keys(&original)) {
            Reconstructed::Value(rebuilt) => assert_eq!(rebuilt, original),
            other => panic!("a well-formed image must rebuild: {other:?}"),
        }
    }

    /// A malformed image must be `Failed`, never a silent re-draw — a re-draw
    /// would look like a successful replay while iterating in a fresh order.
    #[test]
    fn a_malformed_image_fails_rather_than_redrawing() {
        for image in [
            serde_json::json!({}),
            serde_json::json!({ "k0": 1 }),
            serde_json::json!({ "k0": "not-a-number", "k1": 2 }),
            serde_json::json!(null),
        ] {
            assert!(
                matches!(reconstruct_keys(&image), Reconstructed::Failed(_)),
                "a malformed image must fail-stop, not redraw: {image}"
            );
        }
    }

    /// A collection the recording never had seeds identically on every replay,
    /// and two differently-named collections do not collide.
    #[test]
    fn a_synthesized_seed_is_deterministic_and_name_sensitive() {
        let miss = |name: &str| {
            crate::SubstituteMiss::new(
                BOUNDARY,
                COMPONENT,
                OPERATION,
                serde_json::json!({ "name": name }),
            )
        };
        assert_eq!(
            synthesize_keys(&miss("routing::eligible")),
            synthesize_keys(&miss("routing::eligible")),
            "the same unrecorded collection must seed the same way every replay, or \
             two replays of one candidate disagree with each other"
        );
        assert_ne!(
            synthesize_keys(&miss("routing::eligible")),
            synthesize_keys(&miss("routing::rejected")),
            "two collections must not share a synthesized seed"
        );

        let synthesized = synthesize_keys(&miss("routing::eligible"));
        assert_ne!(
            (synthesized.k0, synthesized.k1),
            (0, 0),
            "an all-zero pair would silently be `DefaultHasher`'s fixed keys"
        );
        assert_ne!(synthesized.k0, synthesized.k1, "the halves must differ");
    }

    /// A drawn pair is real entropy, not a constant.
    #[test]
    fn drawn_pairs_differ_from_each_other() {
        let (a, b) = (draw_keys(), draw_keys());
        assert_ne!((a.k0, a.k1), (b.k0, b.k1));
        assert_ne!(a.k0, a.k1, "the two keys must be separate draws");
    }

    /// A key pair must not reach a log line through a `Debug` on some enclosing
    /// struct.
    #[test]
    fn debug_masks_the_keys() {
        let rendered = format!("{:?}", HashKeys { k0: 42, k1: 43 });
        assert!(
            !rendered.contains("42") && !rendered.contains("43"),
            "{rendered}"
        );
        assert!(rendered.contains("***"), "{rendered}");

        // And through the seed that holds them, which is what a service's own
        // `#[derive(Debug)]` would actually print.
        let through_seed = format!("{:?}", keys(42, 43));
        assert!(
            !through_seed.contains("42") && !through_seed.contains("43"),
            "{through_seed}"
        );
    }
}
