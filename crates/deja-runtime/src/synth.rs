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
//!    every helper draws from a marked subspace.
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
    format_uuid_v8(digest(miss, "uuid/hi"), digest(miss, "uuid/lo"))
}

/// The v8 formatting, shared by [`uuid_v8`] and [`reshape`].
///
/// Split out rather than duplicated: the version and variant bits below are the
/// whole non-collision guarantee, and a second copy is a second place for it to
/// be got wrong.
fn format_uuid_v8(hi_word: ::std::primitive::u64, lo_word: ::std::primitive::u64) -> String {
    let hi = hi_word.to_be_bytes();
    let lo = lo_word.to_be_bytes();
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

// ---------------------------------------------------------------------------
// Answering a miss from the recording's own shape
// ---------------------------------------------------------------------------

/// Is this string a formatted UUID?
///
/// Shape only — 8-4-4-4-12 hex with dashes. Deliberately not a parse into a
/// uuid type: this crate does not depend on one, and the question here is
/// whether the RECORDED value looked like a uuid, not whether it was a valid
/// one.
fn is_uuid_shaped(s: &str) -> bool {
    let groups = [8usize, 4, 4, 4, 12];
    let mut parts = s.split('-');
    for want in groups {
        match parts.next() {
            Some(part) if part.len() == want && part.bytes().all(|b| b.is_ascii_hexdigit()) => {}
            _ => return false,
        }
    }
    parts.next().is_none()
}

/// Does this object key name something that IDENTIFIES a thing?
///
/// The opinionated half of [`reshape`], and the half to change when it is
/// wrong. Keyed on the FIELD NAME rather than the value's shape, because the
/// name is the only place the recording states intent: `String` covers an id and
/// a status alike, and this repo has already been bitten by reading a type as a
/// meaning. A name is not proof either — it is a better guess, made in one place
/// where it can be read and argued with.
fn key_is_identity(key: &str) -> bool {
    let k = key.to_ascii_lowercase();
    k == "id"
        || k.ends_with("_id")
        || k.ends_with("_ids")
        || k.ends_with("_reference")
        || k.ends_with("_token")
        || k.ends_with("_key")
        || k == "reference"
        || k == "token"
}

/// Rewrite a recorded payload's IDENTITY leaves into the synthesized subspace,
/// leaving every other leaf exactly as recorded.
///
/// # Why borrow a payload at all
///
/// Answering a miss needs a value of the site's own return type, and nothing at
/// the seam knows how to build one — there is no reflection, and `Default` fails
/// both distinguishability and non-collision. But a miss at a site whose OTHER
/// calls were recorded has a real, well-formed value of exactly that type
/// sitting in the tape. Borrowing it and handing it back through the site's own
/// rebuild path makes type-validity structural rather than something this
/// function has to achieve.
///
/// # Why it cannot be handed back unchanged
///
/// A recorded payload is, by construction, made of values the recording holds.
/// Returned verbatim, its fields flow into DOWNSTREAM args, and a downstream
/// lookup keyed on them HITS — the request silently resyncs onto the borrowed
/// call's recorded trace and the scorecard reports matches. That is worse than
/// a stop: a fabricated run that reads as a clean one. So the identity leaves
/// are moved into the marked subspace, where a downstream lookup MISSES and
/// answers down the same ladder instead of pretending.
///
/// Everything that is not identity is kept. A status, an amount, a currency, an
/// enum: perturbing those buys no non-collision that the identity change has not
/// already bought, and costs a value the service may validate — and a value the
/// service REJECTS is worse than a stop, because the rejection is attributed to
/// the candidate.
///
/// Deterministic: every rewritten leaf is a function of the miss and the leaf's
/// own path, so the same miss against the same tape yields the same payload in
/// every run.
pub fn reshape(miss: &SubstituteMiss, borrowed: &serde_json::Value) -> serde_json::Value {
    fn walk(
        miss: &SubstituteMiss,
        value: &serde_json::Value,
        path: &str,
        under_identity: bool,
    ) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => serde_json::Value::Object(
                map.iter()
                    .map(|(k, v)| {
                        let child = format!("{path}.{k}");
                        (k.clone(), walk(miss, v, &child, key_is_identity(k)))
                    })
                    .collect(),
            ),
            // An array inherits its key's verdict: `payment_ids: [..]` names
            // identities, and each element is addressed by its own index so two
            // elements cannot be rewritten alike.
            serde_json::Value::Array(items) => serde_json::Value::Array(
                items
                    .iter()
                    .enumerate()
                    .map(|(i, v)| walk(miss, v, &format!("{path}[{i}]"), under_identity))
                    .collect(),
            ),
            serde_json::Value::String(s) => {
                // Already synthesized: leave it. Reshaping a reshaped payload
                // must be a no-op, or a value's identity would drift between
                // two misses that should agree.
                if s.starts_with(SYNTH_PREFIX) {
                    return value.clone();
                }
                // A uuid stays a uuid — moved to the v8 space, which no recorder
                // produces, so it is disjoint by construction rather than by
                // luck. Shape is preserved for services that parse it.
                if is_uuid_shaped(s) {
                    return serde_json::Value::String(uuid_v8_at(miss, path));
                }
                if under_identity {
                    return serde_json::Value::String(id_at(miss, path));
                }
                value.clone()
            }
            // Numbers, bools and null are never rewritten. A numeric id is
            // indistinguishable from an amount at this layer, and getting that
            // wrong turns a fabricated identity into a fabricated sum.
            _ => value.clone(),
        }
    }
    walk(miss, borrowed, "$", false)
}

/// [`uuid_v8`] domain-separated by a leaf's path, so two uuid leaves of one
/// payload do not become the same value.
fn uuid_v8_at(miss: &SubstituteMiss, path: &str) -> String {
    let hi = digest(miss, &format!("reshape/uuid/hi{path}"));
    let lo = digest(miss, &format!("reshape/uuid/lo{path}"));
    format_uuid_v8(hi, lo)
}

/// [`id`] domain-separated by a leaf's path.
fn id_at(miss: &SubstituteMiss, path: &str) -> String {
    format!(
        "{SYNTH_PREFIX}{:016x}",
        digest(miss, &format!("reshape/id{path}"))
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn miss(args: serde_json::Value) -> SubstituteMiss {
        SubstituteMiss::new("imc", "Cache", "get_val", args)
    }

    /// Property 1. Nothing else here means anything without it.
    #[test]
    fn the_same_miss_always_synthesizes_the_same_value() {
        let a = miss(json!({"key": "k1"}));
        let b = miss(json!({"key": "k1"}));
        assert_eq!(uuid_v8(&a), uuid_v8(&b));
        assert_eq!(id(&a), id(&b));
        assert_eq!(u64(&a), u64(&b));
        assert_eq!(bytes::<16>(&a), bytes::<16>(&b));
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

    /// A borrowed payload's IDENTITY leaves must all move, or the request
    /// silently resyncs onto the borrowed call's recorded trace.
    ///
    /// This is the property the whole borrow rests on. If a recorded id came
    /// back unchanged it would flow into a downstream call's args, that
    /// lookup would HIT, and the run would report matches against a trace it
    /// never earned — a fabricated replay reading as a clean one, which is
    /// worse than the stop it replaced.
    #[test]
    fn every_identity_leaf_of_a_borrowed_payload_moves() {
        let miss = miss(serde_json::json!({"q":1}));
        let borrowed = serde_json::json!({
            "payment_id": "pay_7c9e2a",
            "attempt": {
                "id": "01a0a9ff-4280-7c01-94c1-4f01ada45bf9",
                "connector_token": "tok_live_abc",
                "status": "succeeded",
                "amount": 1299,
                "currency": "USD"
            },
            "refund_ids": ["ref_1", "ref_2"]
        });

        let out = reshape(&miss, &borrowed);

        // Vacuity guard FIRST: this asserts a property about identity leaves,
        // so a payload holding none would pass it for free.
        let identities = [
            out["payment_id"]
                .as_str()
                .expect("a rewritten identity leaf is a string"),
            out["attempt"]["id"]
                .as_str()
                .expect("a rewritten identity leaf is a string"),
            out["attempt"]["connector_token"]
                .as_str()
                .expect("a rewritten identity leaf is a string"),
            out["refund_ids"][0]
                .as_str()
                .expect("a rewritten identity leaf is a string"),
            out["refund_ids"][1]
                .as_str()
                .expect("a rewritten identity leaf is a string"),
        ];
        assert_eq!(
            identities.len(),
            5,
            "the fixture must carry identity leaves"
        );

        assert_ne!(out["payment_id"], borrowed["payment_id"]);
        assert_ne!(out["attempt"]["id"], borrowed["attempt"]["id"]);
        assert_ne!(
            out["attempt"]["connector_token"],
            borrowed["attempt"]["connector_token"]
        );
        assert_ne!(out["refund_ids"][0], borrowed["refund_ids"][0]);
        assert_ne!(out["refund_ids"][1], borrowed["refund_ids"][1]);

        // Each rewritten leaf is MARKED, so it is disjoint from anything a
        // recorder produces and an operator reading a diff can see it.
        assert!(out["payment_id"]
            .as_str()
            .expect("a rewritten identity leaf is a string")
            .starts_with(SYNTH_PREFIX));
        assert!(out["attempt"]["connector_token"]
            .as_str()
            .expect("a rewritten identity leaf is a string")
            .starts_with(SYNTH_PREFIX));
        // The uuid stays a uuid — parseable for a service that validates the
        // shape — but in the v8 space no recorder writes.
        let rebuilt = out["attempt"]["id"]
            .as_str()
            .expect("a rewritten identity leaf is a string");
        assert!(is_uuid_shaped(rebuilt), "a uuid must stay uuid-shaped");
        assert_eq!(&rebuilt[14..15], "8", "version nibble must be 8");

        // Two identity leaves must not collapse onto one another.
        assert_ne!(out["refund_ids"][0], out["refund_ids"][1]);
        assert_ne!(out["payment_id"], out["attempt"]["connector_token"]);
    }

    /// Everything that is NOT identity is kept exactly. A value the service
    /// validates and rejects is worse than a stop, because the rejection is
    /// attributed to the candidate rather than to us.
    #[test]
    fn a_borrowed_payload_keeps_everything_that_is_not_identity() {
        let miss = miss(serde_json::json!({"q":1}));
        let borrowed = serde_json::json!({
            "status": "succeeded",
            "amount": 1299,
            "currency": "USD",
            "captured": true,
            "error": serde_json::Value::Null,
            "nested": {"method": "card"}
        });
        assert_eq!(
            reshape(&miss, &borrowed),
            borrowed,
            "a payload with no identity leaf must come back untouched"
        );
    }

    /// Same miss, same tape, same payload — every run. Without this, two
    /// replays of one candidate disagree with each other and "the candidate
    /// changed" cannot be separated from "the fabrication changed".
    #[test]
    fn reshaping_is_deterministic_and_reshaping_twice_changes_nothing() {
        let miss = miss(serde_json::json!({"q":1}));
        let borrowed = serde_json::json!({"payment_id": "pay_1", "id": "x"});
        let once = reshape(&miss, &borrowed);
        assert_eq!(once, reshape(&miss, &borrowed), "same miss, same payload");
        assert_eq!(
            reshape(&miss, &once),
            once,
            "already-synthesized leaves must not drift on a second pass"
        );
    }

    /// Two different misses must not fabricate the same identity, or two
    /// distinct calls merge silently.
    #[test]
    fn different_misses_fabricate_different_identities() {
        let borrowed = serde_json::json!({"payment_id": "pay_1"});
        let a = reshape(&miss(serde_json::json!({"q":1})), &borrowed);
        let b = reshape(&miss(serde_json::json!({"q":2})), &borrowed);
        assert_ne!(a["payment_id"], b["payment_id"]);
    }
}
