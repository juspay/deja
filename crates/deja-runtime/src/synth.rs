//! Deterministic values derived from a Substitute miss.
//!
//! A recording is a partial function `(identity, args) -> result`. A miss is
//! where it is undefined, and a site that wants to continue has to total that
//! function without invalidating the experiment. These helpers are the sanctioned
//! way to do it.
//!
//! # The properties, in priority order
//!
//! 1. **Deterministic** — a pure function of the [`SubstituteMiss`]. Replay needs
//!    `same query -> same value, every run`, or two replays of one candidate
//!    against one tape disagree with each other and "the candidate changed"
//!    cannot be separated from "the fabrication changed".
//! 2. **Distinguishable** — different queries give different values, so a
//!    fabricated value cannot silently merge two distinct calls.
//! 3. **Type-valid** — parses as the domain type it is standing in for.
//! 4. **Non-colliding** — disjoint from any value the recording could plausibly
//!    hold. This one is easy to skip and expensive to skip: deja substitutes
//!    results, and a synthesized value flows into DOWNSTREAM args. If a
//!    synthesized uuid could equal a recorded one, the next Substitute lookup
//!    keyed on it would HIT — a false resync on fabricated data, silently. So
//!    the helpers whose values are IDENTIFIERS draw from a marked subspace:
//!    [`uuid_v8`] from a version no live generator emits, [`id`] behind
//!    [`SYNTH_PREFIX`]. The rest return bare values — an integer, bytes, a string
//!    over the caller's alphabet, a timestamp — which have no room for a marker,
//!    so keep them to seams nothing is keyed on.
//! 5. **Attributable** — the seam stamps
//!    [`SubstituteOutcome::Synthesized`](crate::SubstituteOutcome::Synthesized)
//!    on the observation; nothing here has to do that.
//!
//! Honesty ranks below all five. Answering a miss by running the real
//! computation is the most honest option available and the worst one: at a
//! `deja::id` seam a fresh uuid reintroduces exactly the entropy the seam exists
//! to remove, on precisely the calls the seam failed to cover.
//!
//! # Why these live here rather than on a type
//!
//! Non-collision and advancement are enforced once, here, and COMPOSED by call
//! sites. A codec could not do this job: it knows the TYPE, and the type does
//! not decide. `String` is the return type of `generate_id`, of
//! `generate_random_alphanumeric_string`, and of `get_temp_password` — and for
//! the last of those a content-addressed value is by construction a value anyone
//! who knows the query can predict. Same type, opposite answers. The site knows
//! the type too, so it strictly dominates on information.
//!
//! # What is derived from what
//!
//! Every helper hashes the same canonical image the LOOKUP KEY was built from
//! (`hash_value`: object keys sorted, array order significant). Two calls that
//! address the same recorded entry therefore synthesize the same value, by
//! construction rather than by coincidence.

use crate::SubstituteMiss;

/// Prefix marking a synthesized string. Reserved: nothing a service records
/// should begin with it, which is what keeps [`id`] out of the recorded value
/// space.
pub const SYNTH_PREFIX: &str = "deja-synth-";

/// Hash the miss under a domain separator.
///
/// The separator is what keeps two helpers from returning the same bits for one
/// miss — without it, `u64` and the low half of [`uuid_v8`] would be equal, and
/// a site using both would be fabricating a correlation between two values that
/// have nothing to do with each other.
fn digest(miss: &SubstituteMiss, domain: &str) -> u64 {
    let mut h = crate::fnv1a_str(crate::FNV_OFFSET_BASIS, domain);
    // A unit separator between every field, so `("ab", "c")` and `("a", "bc")`
    // cannot hash alike.
    for field in [miss.boundary, miss.component, miss.method] {
        h = crate::fnv1a_bytes(h, b"\x1f");
        h = crate::fnv1a_str(h, field);
    }
    h = crate::fnv1a_bytes(h, b"\x1f");
    // The SAME canonical args hash the lookup key uses.
    h = crate::replay::hash_value(h, &miss.args);
    h = crate::fnv1a_bytes(h, b"\x1f");
    h = crate::fnv1a_bytes(h, &miss.occurrence.to_le_bytes());
    match &miss.correlation_id {
        Some(correlation) => crate::fnv1a_str(crate::fnv1a_bytes(h, b"\x1f"), correlation),
        None => h,
    }
}

/// A deterministic `u64` for this miss.
///
/// The raw primitive the others are built from. Prefer a shaped helper where one
/// fits: a bare integer carries no marker, so it cannot be told apart from a
/// recorded one after the fact.
pub fn u64(miss: &SubstituteMiss) -> ::std::primitive::u64 {
    digest(miss, "u64")
}

/// `N` deterministic bytes for this miss.
///
/// For a salt, a nonce, or any opaque byte string whose CONTENT the service
/// never inspects — the PSS salt seam is the standing example. Never for a value
/// whose secrecy matters: everything here is derivable by anyone holding the
/// query.
///
/// Two draws of different `N` at ONE miss SHARE A PREFIX: `bytes::<12>` is the
/// first twelve bytes of `bytes::<32>`, because extending a draw deliberately
/// never rewrites what it already produced. That is the right trade where `N` is
/// one site's fixed width, and the wrong one where two widths meet in a single
/// arm — for that, reach for
/// [`SubstituteMiss::bytes_vec`](SubstituteMiss::bytes_vec), whose length is part
/// of its domain. Treat the pairing as live rather than hypothetical: a host arm
/// already draws two shapes at one miss, and a seam that wraps a key under a
/// nonce needs exactly the two widths this does not separate.
pub fn bytes<const N: usize>(miss: &SubstituteMiss) -> [u8; N] {
    let mut out = [0u8; N];
    // One digest per 8-byte block, each under its own domain, so extending the
    // length never rewrites the bytes already produced.
    for (block, chunk) in out.chunks_mut(8).enumerate() {
        let word = digest(miss, &format!("bytes/{block}")).to_le_bytes();
        chunk.copy_from_slice(&word[..chunk.len()]);
    }
    out
}

/// A deterministic UUID, formatted, in the **version 8** space.
///
/// Version 8 is RFC 9562's "custom" space: real code produces v4 (random) or v7
/// (time-ordered), so a synthesized uuid is STRUCTURALLY disjoint from anything
/// a recording can hold. That is the non-collision property made checkable
/// rather than assumed — a synthesized id can never satisfy a downstream lookup
/// keyed on a recorded one, so a false resync on fabricated data is impossible
/// rather than unlikely.
pub fn uuid_v8(miss: &SubstituteMiss) -> String {
    let hi = digest(miss, "uuid/hi").to_be_bytes();
    let lo = digest(miss, "uuid/lo").to_be_bytes();
    let mut b = [0u8; 16];
    b[..8].copy_from_slice(&hi);
    b[8..].copy_from_slice(&lo);
    // Version 8 in the high nibble of byte 6; RFC 4122 variant in the top two
    // bits of byte 8. Both overwrite digest bits, which costs 6 bits of the
    // space and buys the disjointness above.
    b[6] = (b[6] & 0x0f) | 0x80;
    b[8] = (b[8] & 0x3f) | 0x80;
    let hex = |r: &[u8]| {
        r.iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    format!(
        "{}-{}-{}-{}-{}",
        hex(&b[0..4]),
        hex(&b[4..6]),
        hex(&b[6..8]),
        hex(&b[8..10]),
        hex(&b[10..16])
    )
}

/// A deterministic identifier string, carrying [`SYNTH_PREFIX`].
///
/// For an opaque id whose SHAPE the service does not constrain. Where the shape
/// is a uuid, use [`uuid_v8`] — it stays parseable, which this is not.
///
/// The prefix is the marker: an operator reading a diff can see the value was
/// fabricated, and a downstream lookup keyed on it will miss rather than collide
/// with a recorded id. Where a service validates the shape strictly enough to
/// reject this, that is the signal to return
/// [`Reconstructed::NoValue`](crate::Reconstructed::NoValue) instead — a value
/// the service rejects is worse than a stop, because the rejection is attributed
/// to the candidate.
pub fn id(miss: &SubstituteMiss) -> String {
    format!("{SYNTH_PREFIX}{:016x}", digest(miss, "id"))
}

/// A deterministic timestamp that ADVANCES with the occurrence at this call site.
///
/// Read the guarantee precisely: successive misses at ONE call site within one
/// correlation strictly increase. Two DIFFERENT call sites both start from
/// `base_ns`, because a miss carries its per-site occurrence and no global
/// counter — deriving one would mean advancing shared replay state from the miss
/// path, which would perturb the very keys the lookup is built on.
///
/// So this is the right helper where a caller compares readings from one site
/// (a duration, a retry backoff, a cache TTL) and the wrong one where a caller
/// orders readings from different sites against each other. In that second case
/// there is nothing honest OR derivable to return, which is
/// [`Reconstructed::NoValue`](crate::Reconstructed::NoValue).
///
/// `base_ns` is the site's own origin — for a correlation-scoped clock, the
/// recorded time origin of that correlation. Saturating, so a pathological
/// `step_ns` cannot wrap into the past.
pub fn monotonic(miss: &SubstituteMiss, base_ns: i64, step_ns: i64) -> i64 {
    base_ns.saturating_add(i64::from(miss.occurrence).saturating_mul(step_ns))
}

/// How far a synthesized clock advances between two misses at one call site:
/// one millisecond, in nanoseconds.
///
/// [`monotonic`] is unit-agnostic, so this fixes the unit for a caller that
/// wants a clock rather than a policy. Deliberately small: a step long enough to
/// rival a real span's duration would let a derived reading be mistaken for a
/// measured one.
pub const CLOCK_STEP_NS: i64 = 1_000_000;

/// The shaped draws, as inherent methods.
///
/// Inherent rather than an extension trait because a miss arm is written inside
/// an attribute, far from anything that could give a free function a home.
/// Hanging the shapes off the miss is what keeps them findable: a reader holding
/// a `SubstituteMiss` can ask what it will answer with, rather than having to
/// know that a module of shapes exists and which path reaches it.
///
/// **An inherent method here silently wins over a host trait method of the same
/// name.** Not an ambiguity error and not a warning — the host's `impl` stays
/// valid, is simply never selected, and the call site compiles unchanged while
/// what it fabricates changes. A host that already carries its own `over` or
/// `epoch_nanos` on the miss must therefore delete that trait in the SAME change
/// that bumps its pin to a revision carrying these. Bump first and every arm
/// that reached the host's version starts drawing from deja's, with no error, no
/// warning, and nothing but a golden-value test to report it.
///
/// Each draw hashes the miss under its OWN domain, including every parameter that
/// changes the result. That is what the free helpers above do too, and the reason
/// is the same: two draws at one miss must not be two projections of one stream.
/// Hand a caller a 12-byte value that is the first twelve bytes of its 256-byte
/// value and both are the right size, the right encoding, and derived — and they
/// are correlated in a way nothing downstream can see.
impl SubstituteMiss {
    /// `length` characters drawn from `alphabet`, derived from this miss.
    ///
    /// For a generator whose caller constrains the SHAPE: an id over an alphabet
    /// that forbids `-`, a digits-only string, a nanoid a connector puts on the
    /// wire. [`id`] is the better answer wherever the shape is free, because it
    /// carries a visible marker and this cannot — but where a service validates
    /// the shape, a value it rejects is worse than a stop, since the rejection is
    /// attributed to the candidate.
    ///
    /// The alphabet stays the CALLER's and is not a convenience constant here:
    /// one host draws from a 64-symbol nanoid alphabet and another from a
    /// 62-symbol one, and a fixed alphabet would quietly put a symbol on the wire
    /// that the caller forbids.
    ///
    /// `length` and the alphabet's SYMBOLS are both in the domain, so two draws
    /// differing in either are independent. Without `length`, a shorter draw
    /// would be a PREFIX of a longer one at the same miss. Without the symbols —
    /// their size alone is not enough — two different 16-symbol alphabets would
    /// be one stream rendered twice, each value a symbol-for-symbol remapping of
    /// the other, which is a correlation a reader of either value cannot see.
    ///
    /// Two different things return an empty string: `length` of zero, which is a
    /// caller asking for nothing, and an empty `alphabet`, which is a caller
    /// error. They are not distinguished in the return, because every known
    /// caller passes a CONSTANT alphabet and so cannot reach the second; a caller
    /// that can has a bug at the call site, not a value to interpret. The empty
    /// alphabet answers rather than panicking because this runs on the miss path,
    /// where a panic ends the correlation the arm exists to keep alive.
    pub fn over(&self, alphabet: &[char], length: usize) -> String {
        // No symbols, no draw.
        let Some(&first) = alphabet.first() else {
            return String::new();
        };
        let Ok(span) = u64::try_from(alphabet.len()) else {
            return String::new();
        };
        // The alphabet goes in by its SYMBOLS, not its size. `length` carries no
        // `/`, so the two fields cannot run together whatever the symbols are.
        let alphabet_image: String = alphabet.iter().collect();
        let seed = digest(self, &format!("over/{length}/{alphabet_image}"));
        positions(seed, length)
            .map(|word| {
                // `word % span` is below `alphabet.len()`, so neither fallback is
                // reachable; they are here so no position can be dropped.
                let pick = usize::try_from(word % span).unwrap_or(0);
                alphabet.get(pick).copied().unwrap_or(first)
            })
            .collect()
    }

    /// `length` deterministic bytes, for a caller that picks the length at
    /// runtime.
    ///
    /// [`bytes`] is the const-generic form and the one to prefer where the width
    /// is known at compile time. This exists because a host's
    /// `generate_random_bytes(n)` and its AES nonce take their width from a value.
    ///
    /// `length` is part of the domain, which is the difference that matters: a
    /// 12-byte draw is NOT the first twelve bytes of a 256-byte draw at the same
    /// miss. An arm answering a JWE seam synthesizes both a GCM nonce and a
    /// wrapped key; drawn off one stream the nonce would be a prefix of the key —
    /// correctly sized, correctly encoded, deterministic, and silently correlated.
    /// [`bytes`] does not make this separation and says so.
    ///
    /// Two draws of the SAME length at one miss are the same value. That is
    /// determinism, not a defect, and it is the limit of what a miss can tell
    /// apart: an arm that needs two independent byte strings should take ONE draw
    /// of the combined length and split it, because positions within a draw are
    /// independent of each other.
    pub fn bytes_vec(&self, length: usize) -> Vec<u8> {
        let seed = digest(self, &format!("bytes_vec/{length}"));
        // `to_le_bytes()[0]` rather than a mask and a conversion: total, and no
        // position can be dropped on the way out.
        positions(seed, length)
            .map(|word| word.to_le_bytes()[0])
            .collect()
    }

    /// Nanoseconds since the Unix epoch for this miss, advancing with the
    /// occurrence at this call site.
    ///
    /// [`monotonic`] with a base of ZERO and a step of [`CLOCK_STEP_NS`]. Zero
    /// rather than a correlation's time origin: no such origin is available at a
    /// miss, and inventing one would reintroduce exactly the ambient dependency a
    /// Substitute seam exists to remove. It also makes the reading obviously
    /// synthetic to a human, which is a feature rather than a cost.
    ///
    /// Not a draw, so it carries no domain separator and needs none — it is a
    /// function of the occurrence alone. Read [`monotonic`]'s guarantee before
    /// ordering anything by it: it orders one call site against itself and nothing
    /// else.
    pub fn epoch_nanos(&self) -> i64 {
        monotonic(self, 0, CLOCK_STEP_NS)
    }
}

/// One word per position, from a seed that already carries the draw's domain.
///
/// Mixing the position in rather than walking one state means two positions never
/// correlate, while the whole draw still costs one digest of the miss rather than
/// one per byte. The counter is a `u64` from the start so no conversion can fail
/// and drop a position: the length is part of the promise.
fn positions(seed: u64, length: usize) -> impl Iterator<Item = u64> {
    (0..length).scan(0u64, move |position, _| {
        let word = mix(seed, *position);
        *position = position.wrapping_add(1);
        Some(word)
    })
}

/// Expand a seed into the word for one position.
fn mix(seed: u64, position: u64) -> u64 {
    // splitmix64's finalizer. Chosen for having no fixed points worth worrying
    // about at this size, not for any cryptographic property: everything here is
    // derivable by anyone holding the query, which is the point.
    let mut state = seed ^ position.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    state = (state ^ (state >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    state = (state ^ (state >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    state ^ (state >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn miss(args: serde_json::Value) -> SubstituteMiss {
        SubstituteMiss::new("imc", "Cache", "get_val", args)
    }

    const HEX: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
    const DIGITS: [char; 10] = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'];

    /// A second 16-symbol alphabet, disjoint from [`HEX`], so a shared stream
    /// shows up as a remapping rather than hiding behind a shared symbol.
    const SIXTEEN_LETTERS: [char; 16] = [
        'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v',
    ];

    /// A host alphabet, so the pin covers a span that is not a power of two.
    const ALPHANUMERIC: [char; 62] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
        'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R',
        'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
    ];

    /// Property 1. Nothing else here means anything without it.
    #[test]
    fn the_same_miss_always_synthesizes_the_same_value() {
        let a = miss(json!({"key": "k1"}));
        let b = miss(json!({"key": "k1"}));
        assert_eq!(uuid_v8(&a), uuid_v8(&b));
        assert_eq!(id(&a), id(&b));
        assert_eq!(u64(&a), u64(&b));
        assert_eq!(bytes::<16>(&a), bytes::<16>(&b));
        assert_eq!(a.over(&HEX, 12), b.over(&HEX, 12));
        assert_eq!(a.bytes_vec(12), b.bytes_vec(12));
        assert_eq!(a.epoch_nanos(), b.epoch_nanos());
    }

    /// Property 2, and the test that makes property 1 non-vacuous: a helper that
    /// returned a constant would pass every determinism assertion above.
    #[test]
    fn a_different_miss_synthesizes_a_different_value() {
        let base = miss(json!({"key": "k1"}));

        let by_args = miss(json!({"key": "k2"}));
        let by_boundary = SubstituteMiss::new("redis", "Cache", "get_val", json!({"key": "k1"}));
        let by_component = SubstituteMiss::new("imc", "Store", "get_val", json!({"key": "k1"}));
        let by_method = SubstituteMiss::new("imc", "Cache", "put_val", json!({"key": "k1"}));
        let by_occurrence = miss(json!({"key": "k1"})).with_call_context(1, None);
        let by_correlation =
            miss(json!({"key": "k1"})).with_call_context(0, Some("req-1".to_string()));

        for (label, other) in [
            ("args", &by_args),
            ("boundary", &by_boundary),
            ("component", &by_component),
            ("method", &by_method),
            ("occurrence", &by_occurrence),
            ("correlation", &by_correlation),
        ] {
            assert_ne!(u64(&base), u64(other), "{label} must change the value");
            assert_ne!(uuid_v8(&base), uuid_v8(other), "{label}");
            assert_ne!(id(&base), id(other), "{label}");
            assert_ne!(base.over(&HEX, 12), other.over(&HEX, 12), "{label}");
            assert_ne!(base.bytes_vec(12), other.bytes_vec(12), "{label}");
        }
    }

    /// NON-COLLISION, made structural rather than probable.
    ///
    /// Real code produces v4 or v7 uuids. A synthesized one is v8, so it cannot
    /// equal a recorded one — which matters because deja substitutes RESULTS and
    /// a synthesized value flows into downstream ARGS. If the two spaces
    /// overlapped, the next Substitute lookup keyed on a synthesized id could
    /// HIT: a false resync on fabricated data, silently.
    #[test]
    fn a_synthesized_uuid_is_in_the_reserved_version_8_space() {
        let rendered = uuid_v8(&miss(json!({"key": "k1"})));

        let groups: Vec<&str> = rendered.split('-').collect();
        assert_eq!(groups.len(), 5, "must be uuid-shaped: {rendered}");
        assert_eq!(
            groups.iter().map(|g| g.len()).collect::<Vec<_>>(),
            vec![8, 4, 4, 4, 12],
            "must be uuid-shaped: {rendered}"
        );
        assert!(
            rendered.chars().all(|c| c == '-' || c.is_ascii_hexdigit()),
            "must be parseable: {rendered}"
        );
        assert_eq!(
            groups[2].chars().next(),
            Some('8'),
            "version nibble must be 8 (RFC 9562 custom), NOT 4 or 7 — that is what \
             makes the synthesized space disjoint from the recorded one: {rendered}"
        );
        let variant = groups[3]
            .chars()
            .next()
            .and_then(|c| c.to_digit(16))
            .expect("variant nibble");
        assert_eq!(
            variant & 0b1100,
            0b1000,
            "variant must be RFC 4122, or the value is not a valid uuid: {rendered}"
        );
    }

    /// The id marker, for the same reason and with a weaker guarantee.
    #[test]
    fn a_synthesized_id_carries_the_reserved_prefix() {
        let rendered = id(&miss(json!({"key": "k1"})));
        assert!(
            rendered.starts_with(SYNTH_PREFIX),
            "an operator reading a diff must be able to see the value was \
             fabricated: {rendered}"
        );
    }

    /// Two helpers must never hand back the same bits for one miss.
    ///
    /// Without the domain separator they would, and a site using both would be
    /// fabricating a correlation between two values that have nothing to do with
    /// each other.
    #[test]
    fn the_helpers_are_domain_separated_from_each_other() {
        let m = miss(json!({"key": "k1"}));
        let as_u64 = u64(&m);
        let first_eight = bytes::<8>(&m);
        assert_ne!(
            as_u64.to_le_bytes(),
            first_eight,
            "`u64` and `bytes` must not be the same draw"
        );
        assert!(
            !uuid_v8(&m)
                .replace('-', "")
                .contains(&format!("{as_u64:016x}")),
            "the uuid must not embed the `u64` draw"
        );
        assert!(
            !id(&m).ends_with(&format!("{as_u64:016x}")),
            "`id` must not be the `u64` draw in hex"
        );

        // The shaped draws, against each other and against the primitives they
        // are NOT allowed to be a view of.
        let eight_bytes = m.bytes_vec(8);
        assert_ne!(
            eight_bytes[..],
            as_u64.to_le_bytes()[..],
            "`bytes_vec` must not be the `u64` draw"
        );
        assert_ne!(
            eight_bytes[..],
            first_eight[..],
            "`bytes_vec` and `bytes` must not be the same draw"
        );
        // 16 characters over a 16-symbol alphabet is exactly the width of the
        // `u64` in hex, which is what a draw that merely rendered the `u64` would
        // return.
        assert_ne!(
            m.over(&HEX, 16),
            format!("{as_u64:016x}"),
            "`over` must not be the `u64` draw spelled in the alphabet"
        );
    }

    /// The mutation this whole arrangement exists to kill: `over` and
    /// `bytes_vec` must not be two projections of ONE stream.
    ///
    /// Seeded from a shared `u64` and mixed per position with no separator — the
    /// shape both hosts shipped — a draw over a 16-symbol alphabet is EXACTLY the
    /// low nibble of the byte draw, character for character. Both values are the
    /// right size, in the right alphabet, and deterministic, so nothing else in
    /// this file notices; an arm using both would be fabricating a correlation
    /// between two values that have nothing to do with each other.
    #[test]
    fn a_character_draw_is_not_a_view_of_a_byte_draw() {
        let m = miss(json!({"key": "k1"}));
        let characters = m.over(&HEX, 32);
        let low_nibbles: String = m
            .bytes_vec(32)
            .iter()
            .map(|byte| HEX[usize::from(byte & 0x0f)])
            .collect();
        assert_ne!(
            characters, low_nibbles,
            "`over` is a projection of `bytes_vec`: {characters}"
        );
    }

    /// A shorter draw must not be a PREFIX of a longer one at one miss.
    ///
    /// This is the property whose absence would have shipped a correlated IV: an
    /// arm answering a JWE seam draws a 12-byte GCM nonce and a 256-byte wrapped
    /// key from one miss, and off a single position-indexed stream the nonce is
    /// the key's first twelve bytes. Correctly sized, correctly encoded,
    /// deterministic, and silently correlated.
    ///
    /// Note the deliberate asymmetry with [`bytes`], which DOES extend: there the
    /// width is one site's fixed property, and both halves are documented.
    #[test]
    fn a_shorter_shaped_draw_is_not_a_prefix_of_a_longer_one() {
        let m = miss(json!({"key": "k1"}));

        let nonce = m.bytes_vec(12);
        let wrapped = m.bytes_vec(256);
        assert_ne!(
            nonce[..],
            wrapped[..12],
            "a 12-byte draw is the head of a 256-byte draw at one miss"
        );

        let short = m.over(&HEX, 8);
        let long = m.over(&HEX, 40);
        assert!(
            !long.starts_with(&short),
            "an 8-character draw is the head of a 40-character draw: {short} / {long}"
        );
    }

    /// Two draws over DIFFERENT alphabets must be independent, and the alphabet's
    /// SIZE is not what separates them.
    ///
    /// Two 16-symbol alphabets are the case that matters, because it is the one a
    /// size-only domain cannot tell apart: off a shared stream each value is a
    /// symbol-for-symbol remapping of the other, position by position, and a
    /// reader of either cannot see it. This test is written as that remapping, so
    /// the thing it refutes is the thing the weaker design produces — a test
    /// comparing a 16-symbol draw with a 10-symbol one proves nothing, because
    /// `w % 16` and `w % 10` disagree even on one stream.
    #[test]
    fn two_alphabets_of_one_size_at_one_miss_are_not_one_stream() {
        let m = miss(json!({"key": "k1"}));
        let hex = m.over(&HEX, 16);
        let letters = m.over(&SIXTEEN_LETTERS, 16);
        let remapped: String = hex
            .chars()
            .filter_map(|c| HEX.iter().position(|h| *h == c))
            .filter_map(|at| SIXTEEN_LETTERS.get(at).copied())
            .collect();
        assert_eq!(
            remapped.chars().count(),
            16,
            "the remapping must be total, or this test cannot refute anything"
        );
        assert_ne!(
            letters, remapped,
            "the letter draw is the hex draw remapped: {letters}"
        );
    }

    /// The reason these exist rather than [`id`]: the value has to satisfy the
    /// alphabet and the length its caller promised, or the host rejects it and
    /// the rejection is attributed to the candidate.
    #[test]
    fn a_shaped_draw_honours_the_alphabet_and_the_length() {
        for length in [1_usize, 8, 32, 64] {
            let m = miss(json!({"n": length}));
            let value = m.over(&DIGITS, length);
            assert_eq!(
                value.chars().count(),
                length,
                "length is part of the promise"
            );
            assert!(
                value.chars().all(|c| DIGITS.contains(&c)),
                "a numeric generator must not return {value}"
            );
            assert_eq!(
                m.bytes_vec(length).len(),
                length,
                "length is part of the promise"
            );
        }
    }

    /// An empty alphabet is a caller error, and the miss path is the wrong place
    /// to panic: that would end the correlation the arm exists to keep alive.
    #[test]
    fn an_empty_alphabet_yields_an_empty_string_rather_than_a_panic() {
        assert_eq!(miss(json!({})).over(&[], 8), "");
        assert_eq!(miss(json!({})).bytes_vec(0), Vec::<u8>::new());
    }

    /// Positions must differ from each other, not merely be drawn from the
    /// alphabet.
    ///
    /// Written because every other assertion here passes when the position is
    /// ignored: `"0000000000000000"` is deterministic, in-alphabet, the right
    /// length and prefix-stable, so the suite stays green while the value carries
    /// four bits. A downstream uniqueness assumption would then be violated by a
    /// value that looks well-formed.
    #[test]
    fn the_positions_of_a_shaped_draw_do_not_collapse() {
        let m = miss(json!({}));
        let value = m.over(&HEX, 32);
        let distinct: std::collections::BTreeSet<char> = value.chars().collect();
        assert!(
            distinct.len() > 4,
            "32 characters over a 16-symbol alphabet collapsed to {}: {value}",
            distinct.len()
        );
        let drawn: std::collections::BTreeSet<u8> = m.bytes_vec(32).into_iter().collect();
        assert!(drawn.len() > 8, "32 bytes collapsed to {}", drawn.len());
    }

    /// The clock answers at the epoch and advances by one millisecond per call at
    /// the site, which is [`monotonic`]'s guarantee with the unit fixed.
    #[test]
    fn epoch_nanos_starts_at_the_epoch_and_advances_by_a_millisecond() {
        let at = |n: u32| {
            miss(json!({"key": "k1"}))
                .with_call_context(n, None)
                .epoch_nanos()
        };
        assert_eq!(at(0), 0, "the first reading at a site is the epoch itself");
        assert_eq!(at(1), CLOCK_STEP_NS);
        assert_eq!(at(3), 3 * CLOCK_STEP_NS);
        assert!(
            at(0) < at(1) && at(1) < at(2),
            "successive readings advance"
        );
    }

    /// Synthesis is keyed the way the LOOKUP is keyed, not the way the JSON was
    /// serialized.
    ///
    /// Both halves matter and both are inherited from `hash_value` rather than
    /// reimplemented: object keys are order-INDEPENDENT (two spellings of one
    /// args image address the same recorded entry, so they must synthesize the
    /// same value), and array elements are order-DEPENDENT (a permuted array
    /// misses at every rank, so it is a different call).
    #[test]
    fn synthesis_is_keyed_like_the_lookup() {
        assert_eq!(
            u64(&miss(json!({"a": 1, "b": 2}))),
            u64(&miss(json!({"b": 2, "a": 1}))),
            "object key order must not change the value — the same two calls \
             resolve to the same recorded entry"
        );
        assert_ne!(
            u64(&miss(json!([1, 2]))),
            u64(&miss(json!([2, 1]))),
            "array order must change the value — a permuted array is a different \
             call at every address rank"
        );
    }

    /// Lengthening a byte draw must not rewrite the bytes already produced.
    #[test]
    fn a_longer_byte_draw_extends_a_shorter_one() {
        let m = miss(json!({"key": "k1"}));
        assert_eq!(bytes::<8>(&m)[..], bytes::<32>(&m)[..8]);
        assert_eq!(bytes::<16>(&m)[..], bytes::<32>(&m)[..16]);
    }

    /// ...and the blocks it extends WITH must differ from each other.
    ///
    /// Without this, one digest reused for every block passes the extension test
    /// above unchanged: a 32-byte draw would be one 8-byte value repeated four
    /// times, which has 64 bits of entropy however long the caller asked for.
    #[test]
    fn the_blocks_of_a_byte_draw_differ_from_each_other() {
        let drawn = bytes::<32>(&miss(json!({"key": "k1"})));
        let blocks: Vec<&[u8]> = drawn.chunks(8).collect();
        for (i, a) in blocks.iter().enumerate() {
            for b in blocks.iter().skip(i + 1) {
                assert_ne!(a, b, "every 8-byte block must be its own draw: {drawn:?}");
            }
        }
    }

    /// A partial trailing block must still be filled.
    #[test]
    fn a_byte_draw_that_is_not_a_multiple_of_eight_is_still_filled() {
        let m = miss(json!({"key": "k1"}));
        assert_ne!(bytes::<12>(&m), [0u8; 12]);
        assert_eq!(bytes::<12>(&m)[..8], bytes::<8>(&m)[..]);
    }

    /// `monotonic` advances with the occurrence at ONE site, and says so.
    #[test]
    fn monotonic_advances_with_the_occurrence_at_one_site() {
        let base = 1_000i64;
        let step = 7i64;
        let at = |n: u32| {
            monotonic(
                &miss(json!({"key": "k1"})).with_call_context(n, None),
                base,
                step,
            )
        };

        assert_eq!(at(0), base, "the first call at a site starts at the origin");
        assert!(at(0) < at(1) && at(1) < at(2), "successive calls advance");
        assert_eq!(at(3), base + 3 * step);
    }

    /// The documented limit, asserted so nobody reads a stronger promise into it:
    /// two DIFFERENT sites both start from the origin, because a miss carries a
    /// per-site occurrence and no global counter.
    #[test]
    fn monotonic_does_not_order_two_different_sites() {
        let here = SubstituteMiss::new("imc", "Cache", "read_at", json!({}));
        let there = SubstituteMiss::new("imc", "Cache", "read_at_other", json!({}));
        assert_eq!(
            monotonic(&here, 1_000, 7),
            monotonic(&there, 1_000, 7),
            "this helper orders one site against itself and nothing else; a caller \
             that needs cross-site ordering has nothing derivable and should return \
             `NoValue`"
        );
    }

    /// A pathological step must not wrap into the past.
    #[test]
    fn monotonic_saturates_instead_of_wrapping() {
        let m = miss(json!({})).with_call_context(u32::MAX, None);
        assert_eq!(monotonic(&m, i64::MAX, i64::MAX), i64::MAX);
        assert!(monotonic(&m, 0, i64::MAX) > 0, "must not wrap negative");
    }

    /// Every synthesized value, pinned to the byte for one fully specified miss.
    ///
    /// Determinism within one build is not the property that matters here. Two
    /// hosts pin this crate and replay tapes recorded under an earlier pin, and a
    /// candidate is compared against a baseline that may have run under a
    /// different one. A change to the digest, to a domain string, or to the
    /// mixing would move EVERY synthesized value at once, and nothing else in
    /// this file would notice: the properties above (determinism,
    /// distinguishability, separation, shape) all hold just as well for a
    /// different set of values. So the values themselves are the assertion.
    ///
    /// The four free helpers are pinned alongside the new draws deliberately.
    /// Adding a separated draw must not move an existing output — a moved output
    /// is a tape that no longer matches on the host side — and these lines are
    /// what makes that claim checkable rather than asserted in a commit message.
    ///
    /// If this fails: do not update the literals to make it pass. Either the
    /// change is unintended, or it is a deliberate break that both hosts have to
    /// be told about before it lands.
    #[test]
    fn every_synthesized_value_is_pinned_to_the_byte() {
        // Occurrence and correlation set explicitly: a pin taken at the defaults
        // would not notice a change that only reaches the digest through them.
        let m = miss(json!({"key": "k1"})).with_call_context(2, Some("corr-7".to_string()));

        assert_eq!(u64(&m), 0x8dbb_2fba_051e_bde8);
        assert_eq!(
            bytes::<16>(&m),
            [29, 46, 167, 45, 90, 153, 73, 90, 130, 209, 188, 245, 207, 236, 94, 135]
        );
        assert_eq!(uuid_v8(&m), "2a40bb8a-771b-839e-a3d9-60e501260e60");
        assert_eq!(id(&m), "deja-synth-8978031a428d5802");

        assert_eq!(m.over(&HEX, 12), "c064c70af7a2");
        assert_eq!(m.over(&ALPHANUMERIC, 20), "qC8M1WWrx28yT5fiWY9R");
        assert_eq!(
            m.bytes_vec(12),
            [108, 235, 76, 1, 230, 235, 105, 173, 38, 99, 127, 200]
        );
        // A second width at the same miss, pinned next to the first so the
        // separation is visible in the literals: these two draws share nothing,
        // where one position-indexed stream would have made the shorter the head
        // of the longer.
        assert_eq!(m.bytes_vec(5), [84, 173, 42, 128, 211]);

        assert_eq!(m.epoch_nanos(), 2_000_000);
    }
}
