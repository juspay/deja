//! The identity form of a call's arguments: what makes two calls the same call.
//!
//! Replay decides whether two argument values are the same at every point a
//! value passes through: the candidate's lookup and the table the runner
//! renders hash it ([`identity_args_hash`]), and the scorer compares it
//! ([`identity_differences`], [`same`]). All of them read one rule, here, so
//! none can disagree with another.
//!
//! The rule: order carries no meaning. An array is a multiset, every array,
//! whatever it holds: a value's shape cannot say whether its order is data, so
//! order is tolerated everywhere and each toleration is counted where it
//! happened. Object members are read by key. A string holding a JSON object or
//! array is that document, kept apart from the same document sent as an object.
//!
//! A byte string is not an array: it is one value that capture renders as an
//! array of numbers, because JSON has no bytes. deja's own capture writes bytes
//! in two places, a captured body's `raw_bytes` and a redis wire `BulkString`
//! (`Bytes` in the other dialect), and those keep their order, as the exact
//! hash keeps them. Bytes are a string, so bytes that hold a JSON document are
//! that document, as a string holding one is.
//!
//! Nothing is sorted and no canonical document is built: comparison is a
//! multiset comparison, and the hash combines member hashes by wrapping
//! addition, which any order gives the same sum and which, unlike XOR, keeps a
//! repeated member apart from a single one.

use serde_json::Value;

/// A captured request body is hashed by its content (see
/// `replay::hash_request_body`); its identity is that content's, so two calls
/// the exact hash finds the same are always the same identity.
const JSON_REQUEST_BODY_KIND: &str = "JsonRequestBody";
const FORM_URLENCODED_REQUEST_BODY_KIND: &str = "FormUrlEncodedRequestBody";

/// Members of deja's own capture that hold a byte string.
const BYTE_STRING_MEMBERS: &[&str] = &["raw_bytes", "BulkString", "Bytes"];

/// What the identity set aside to find two values the same call, at the path
/// of the raw value it read differently.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum IdentityChange {
    /// An array holding the same members in another order.
    ArrayOrder(String),
    /// A string holding the same JSON document, written differently.
    DocumentText(String),
}

impl IdentityChange {
    /// The path of the value read differently.
    pub fn path(&self) -> &str {
        match self {
            Self::ArrayOrder(path) | Self::DocumentText(path) => path,
        }
    }
}

/// Whether another call could be written differently from `value` and still
/// have its identity: it holds an array of two or more members, or a string
/// read as a document. Such a call is addressed by its identity alone.
pub fn identity_applies(value: &Value) -> bool {
    match value {
        Value::String(text) => embedded_document(text).is_some(),
        Value::Object(map) if is_form_request_body(map) => false,
        Value::Object(map) if is_json_request_body(map) => {
            map.get("json").is_some_and(identity_applies)
        }
        Value::Object(map) => map.iter().any(|(key, member)| {
            if is_byte_string(key, member) {
                byte_document(member).is_some()
            } else {
                identity_applies(member)
            }
        }),
        Value::Array(items) => items.len() > 1 || items.iter().any(identity_applies),
        _ => false,
    }
}

/// The lookup hash of a call's identity: [`element_hash`] of its args.
pub fn identity_args_hash(args: &Value) -> u64 {
    element_hash(args)
}

/// `None` when `recorded` and `observed` are different calls. Otherwise what
/// was read differently to find them the same: empty when nothing was.
pub fn identity_differences(recorded: &Value, observed: &Value) -> Option<Vec<IdentityChange>> {
    if !same(recorded, observed) {
        return None;
    }
    let mut changes = Vec::new();
    differences(recorded, observed, "$", &mut changes);
    changes.sort();
    Some(changes)
}

/// Whether two values are one identity: arrays compared as multisets, objects
/// by key, a document in a string as that document, a byte string and a
/// number as written.
pub fn same(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::String(x), Value::String(y)) => {
            match (embedded_document(x), embedded_document(y)) {
                (Some(x), Some(y)) => same(&x, &y),
                (None, None) => x == y,
                _ => false,
            }
        }
        (Value::Object(x), Value::Object(y)) if is_form_request_body(x) => {
            is_form_request_body(y) && form_fields_same(x, y)
        }
        (Value::Object(x), Value::Object(y)) if is_json_request_body(x) => {
            is_json_request_body(y)
                && match (x.get("json"), y.get("json")) {
                    (Some(x), Some(y)) => same(x, y),
                    _ => false,
                }
        }
        (Value::Object(x), Value::Object(y)) => {
            !is_form_request_body(y)
                && !is_json_request_body(y)
                && x.len() == y.len()
                && x.iter().all(|(key, value)| {
                    y.get(key)
                        .is_some_and(|other| member_same(key, value, other))
                })
        }
        (Value::Array(x), Value::Array(y)) => matching(x, y).is_some(),
        // As the hash reads a number: by its text, so -0.0 and 0.0 are two.
        (Value::Number(x), Value::Number(y)) => x.to_string() == y.to_string(),
        (x, y) => x == y,
    }
}

/// One object member of each side: a byte string as written unless both hold
/// a document, anything else by [`same`].
fn member_same(key: &str, value: &Value, other: &Value) -> bool {
    if is_byte_string(key, value) || is_byte_string(key, other) {
        if is_byte_string(key, value) && is_byte_string(key, other) {
            if let (Some(a), Some(b)) = (byte_document(value), byte_document(other)) {
                return same(&a, &b);
            }
        }
        value == other
    } else {
        same(value, other)
    }
}

/// A byte string's text, when it is UTF-8 holding a JSON object or array.
fn byte_document(value: &Value) -> Option<Value> {
    let bytes: Vec<u8> = value
        .as_array()?
        .iter()
        .map(|byte| byte.as_u64().and_then(|byte| u8::try_from(byte).ok()))
        .collect::<Option<_>>()?;
    embedded_document(std::str::from_utf8(&bytes).ok()?)
}

/// A byte string in deja's capture: one of its byte members, holding bytes.
fn is_byte_string(key: &str, value: &Value) -> bool {
    BYTE_STRING_MEMBERS.contains(&key)
        && value.as_array().is_some_and(|items| {
            items
                .iter()
                .all(|item| item.as_u64().is_some_and(|byte| byte <= 255))
        })
}

/// The multiset matching of two arrays: for each member of `x`, the index of
/// the distinct member of `y` it is one identity with. `None` when there is no
/// such matching.
///
/// Two passes. First every member is paired with an exactly equal one, across
/// the whole array, so a member that kept its place or only moved is paired
/// with itself whichever side is read first. Then the rest are paired by
/// identity, candidates found by hash and confirmed. Each pass takes its
/// candidates from the front of a queue, so identical members pair in linear
/// time.
fn matching(x: &[Value], y: &[Value]) -> Option<Vec<usize>> {
    use std::collections::{HashMap, VecDeque};
    if x.len() != y.len() {
        return None;
    }
    let written = |value: &Value| value.to_string();
    let mut exact: HashMap<String, VecDeque<usize>> = HashMap::new();
    for (index, member) in y.iter().enumerate() {
        exact.entry(written(member)).or_default().push_back(index);
    }
    let mut pairs: Vec<Option<usize>> = x
        .iter()
        .map(|member| {
            exact
                .get_mut(&written(member))
                .and_then(VecDeque::pop_front)
        })
        .collect();
    let mut by_identity: HashMap<u64, VecDeque<usize>> = HashMap::new();
    for index in exact.into_values().flatten() {
        by_identity
            .entry(element_hash(&y[index]))
            .or_default()
            .push_back(index);
    }
    for queue in by_identity.values_mut() {
        queue.make_contiguous().sort_unstable();
    }
    for (member, pair) in x.iter().zip(pairs.iter_mut()) {
        if pair.is_some() {
            continue;
        }
        let queue = by_identity.get_mut(&element_hash(member))?;
        let found = queue.iter().position(|&index| same(member, &y[index]))?;
        *pair = queue.remove(found);
    }
    pairs.into_iter().collect()
}

fn form_fields_same(
    x: &serde_json::Map<String, Value>,
    y: &serde_json::Map<String, Value>,
) -> bool {
    let (x, y) = (
        crate::replay::request_body_text(x).unwrap_or_default(),
        crate::replay::request_body_text(y).unwrap_or_default(),
    );
    let fields = |text: &str| {
        let mut counts: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
        for field in text.split('&') {
            *counts.entry(field.to_owned()).or_default() += 1;
        }
        counts
    };
    fields(&x) == fields(&y)
}

/// A hash of `value` that any order of an array's members, or of an object's,
/// gives the same result. Member hashes (each already finalised) are summed
/// with wrapping addition; a repeated member adds twice, so multiplicity
/// counts. Objects are summed the same way, as `(key, value)` pair hashes, so
/// there is one combiner. For an object XOR would behave the same in practice:
/// its keys are unique, so no pair can cancel another, and an XOR collision
/// would need control of the hash outputs, which no test can exert (the
/// 210-multiset collision test is the measured support).
pub fn element_hash(value: &Value) -> u64 {
    const NULL: u64 = 0x6e75_6c6c;
    const ARRAY: u64 = 0x0061_7272_6179;
    const OBJECT: u64 = 0x6f62_6a65_6374;
    const DOCUMENT: u64 = 0x0064_6f63;
    const FORM: u64 = 0x666f_726d;
    const BODY: u64 = 0x626f_6479;
    const BYTES: u64 = 0x0062_7974_6573;
    let tagged = |tag: u64, inner: u64| finalise(tag ^ inner.rotate_left(17));
    match value {
        Value::Null => finalise(NULL),
        Value::Bool(b) => finalise(0x626f_6f6c ^ u64::from(*b)),
        Value::Number(n) => tagged(
            0x006e_756d,
            crate::fnv1a_str(crate::FNV_OFFSET_BASIS, &n.to_string()),
        ),
        Value::String(text) => match embedded_document(text) {
            Some(document) => tagged(DOCUMENT, element_hash(&document)),
            None => tagged(0x0073_7472, crate::fnv1a_str(crate::FNV_OFFSET_BASIS, text)),
        },
        Value::Object(map) if is_form_request_body(map) => {
            let text = crate::replay::request_body_text(map).unwrap_or_default();
            let sum = text.split('&').fold(0u64, |sum, field| {
                sum.wrapping_add(finalise(crate::fnv1a_str(crate::FNV_OFFSET_BASIS, field)))
            });
            tagged(FORM, sum)
        }
        Value::Object(map) if is_json_request_body(map) => {
            tagged(BODY, map.get("json").map_or(0, element_hash))
        }
        Value::Object(map) => {
            let sum = map.iter().fold(0u64, |sum, (key, member)| {
                let member = if is_byte_string(key, member) {
                    match byte_document(member) {
                        Some(document) => tagged(DOCUMENT, element_hash(&document)),
                        None => tagged(
                            BYTES,
                            crate::fnv1a_str(crate::FNV_OFFSET_BASIS, &member.to_string()),
                        ),
                    }
                } else {
                    element_hash(member)
                };
                let key = crate::fnv1a_str(crate::FNV_OFFSET_BASIS, key);
                sum.wrapping_add(finalise(key ^ member.rotate_left(29)))
            });
            tagged(OBJECT, sum)
        }
        Value::Array(items) => {
            // Each member's hash is already finalised, and the sum alone keeps
            // [], [a] and [a, a] apart.
            let sum = items
                .iter()
                .fold(0u64, |sum, member| sum.wrapping_add(element_hash(member)));
            tagged(ARRAY, sum)
        }
    }
}

/// splitmix64's finaliser: spreads every input bit across the output, so a sum
/// of finalised members does not collide the way a sum of raw hashes would.
fn finalise(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// A string that holds a JSON object or array, parsed. Anything else, a JSON
/// scalar included, stays text. Only a string that starts and ends like one is
/// parsed at all.
pub fn embedded_document(text: &str) -> Option<Value> {
    let trimmed = text.trim();
    let bracketed = (trimmed.starts_with('{') && trimmed.ends_with('}'))
        || (trimmed.starts_with('[') && trimmed.ends_with(']'));
    if !bracketed {
        return None;
    }
    match serde_json::from_str::<Value>(trimmed) {
        Ok(document @ (Value::Object(_) | Value::Array(_))) => Some(document),
        _ => None,
    }
}

fn is_form_request_body(map: &serde_json::Map<String, Value>) -> bool {
    map.get("kind").and_then(Value::as_str) == Some(FORM_URLENCODED_REQUEST_BODY_KIND)
        && crate::replay::request_body_text(map).is_some()
}

fn is_json_request_body(map: &serde_json::Map<String, Value>) -> bool {
    map.get("kind").and_then(Value::as_str) == Some(JSON_REQUEST_BODY_KIND)
        && map.get("json").is_some_and(|json| !json.is_null())
}

/// Where two values with one identity differ as written. Both sides have the
/// same shape wherever this descends, because they are one identity. A
/// reordered array is reported, and each of its members is still followed to
/// the member it matched, so a difference inside one is not lost.
fn differences(recorded: &Value, observed: &Value, path: &str, out: &mut Vec<IdentityChange>) {
    if recorded == observed {
        return;
    }
    match (recorded, observed) {
        // The exact hash already reads a form body's fields in any order.
        (Value::Object(r), Value::Object(_)) if is_form_request_body(r) => {}
        (Value::Object(r), Value::Object(o)) if is_json_request_body(r) => {
            if let (Some(r), Some(o)) = (r.get("json"), o.get("json")) {
                differences(r, o, &format!("{path}.json"), out);
            }
        }
        (Value::Object(r), Value::Object(o)) => {
            for (key, value) in r {
                if let Some(other) = o.get(key) {
                    let path = format!("{path}.{key}");
                    if !is_byte_string(key, value) {
                        differences(value, other, &path, out);
                    } else if value != other {
                        // Equal as documents, since the two are one identity.
                        out.push(IdentityChange::DocumentText(path));
                    }
                }
            }
        }
        (Value::Array(r), Value::Array(o)) => {
            let pairs = matching(r, o).unwrap_or_default();
            if pairs
                .iter()
                .enumerate()
                .any(|(index, &partner)| index != partner)
            {
                out.push(IdentityChange::ArrayOrder(path.to_owned()));
            }
            for (index, partner) in pairs.into_iter().enumerate() {
                differences(&r[index], &o[partner], &format!("{path}[{index}]"), out);
            }
        }
        (Value::String(_), Value::String(_)) => {
            out.push(IdentityChange::DocumentText(path.to_owned()));
        }
        _ => {}
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Arrays are multisets everywhere, with no exceptions: a tuple, a pair, a
    /// row, numbers and nulls are one identity in any order. Multiplicity
    /// counts: a repeated member is not the same as one.
    #[test]
    fn every_array_is_a_multiset_and_multiplicity_counts() {
        for (a, b) in [
            (json!(["a", "b"]), json!(["b", "a"])),
            (json!([1, 2, 3]), json!([3, 1, 2])),
            (json!([100, 5, "USD"]), json!([5, "USD", 100])),
            (json!(["a", null]), json!([null, "a"])),
            (json!([["k", "v"], ["x"]]), json!([["x"], ["v", "k"]])),
            (json!([{"a": 1}, {"b": 2}]), json!([{"b": 2}, {"a": 1}])),
        ] {
            assert_eq!(identity_args_hash(&a), identity_args_hash(&b), "{a} {b}");
            assert!(identity_differences(&a, &b).is_some(), "{a} {b}");
        }
        let a = json!("a");
        let (twice, once, none) = (json!([a, a]), json!([a]), json!([]));
        assert_ne!(identity_args_hash(&twice), identity_args_hash(&once));
        assert_ne!(identity_args_hash(&twice), identity_args_hash(&none));
        assert_ne!(identity_args_hash(&once), identity_args_hash(&none));
        assert_eq!(identity_differences(&twice, &once), None);
        let pair = |x: &str, y: &str| json!([x, x, y]);
        assert_ne!(
            identity_args_hash(&pair("a", "b")),
            identity_args_hash(&pair("b", "a")),
            "the multiplicity of each member counts, not only the set"
        );
    }

    /// Different shapes are different identities: an empty array, an empty
    /// object, a null, a string and the array holding it, a number's forms,
    /// a document sent as text and as an object, and nested multiplicity.
    #[test]
    fn different_shapes_hash_apart() {
        let values = [
            json!(null),
            json!([]),
            json!({}),
            json!([[]]),
            json!([{}]),
            json!("a"),
            json!(["a"]),
            json!([["a"]]),
            json!({"a": null}),
            json!({"a": []}),
            json!(1),
            json!(1.0),
            json!("1"),
            json!(true),
            json!(false),
            json!({"k": "v"}),
            json!(r#"{"k":"v"}"#),
            json!([["a"], ["a"]]),
            json!([["a", "a"]]),
            json!({"a": "b"}),
            json!({"b": "a"}),
            json!({"a": 1, "b": 2}),
            json!({"a": 2, "b": 1}),
            json!(["a", "a"]),
            json!(["a", "b"]),
        ];
        for (i, a) in values.iter().enumerate() {
            for b in &values[i + 1..] {
                assert_ne!(identity_args_hash(a), identity_args_hash(b), "{a} vs {b}");
                assert!(!same(a, b), "{a} vs {b}");
            }
        }
    }

    /// A commutative hash has one silent failure: two different multisets
    /// summing alike. Over every multiset of up to four members drawn from six
    /// small values, distinct multisets never share a hash.
    #[test]
    fn distinct_multisets_do_not_collide() {
        let pool = [
            json!("a"),
            json!("b"),
            json!(1),
            json!(null),
            json!(["a"]),
            json!({"a": 1}),
        ];
        let mut seen: std::collections::HashMap<u64, Vec<usize>> = std::collections::HashMap::new();
        let mut count = 0;
        for len in 0..=4usize {
            let mut picks = vec![0usize; len];
            loop {
                // Only non-decreasing picks, so each multiset appears once.
                if picks.windows(2).all(|w| w[0] <= w[1]) {
                    let array =
                        serde_json::Value::Array(picks.iter().map(|&i| pool[i].clone()).collect());
                    if let Some(other) = seen.insert(identity_args_hash(&array), picks.clone()) {
                        panic!("{other:?} and {picks:?} collide");
                    }
                    count += 1;
                }
                let mut i = 0;
                while i < len {
                    picks[i] += 1;
                    if picks[i] < pool.len() {
                        break;
                    }
                    picks[i] = 0;
                    i += 1;
                }
                if i == len {
                    break;
                }
            }
        }
        assert_eq!(
            count, 210,
            "precondition: every multiset of up to four was hashed"
        );
    }

    /// A byte string is one value that capture renders as an array of
    /// numbers, not a collection: its bytes keep their order, as the exact
    /// hash already keeps them. The shapes are deja's own: a captured body's
    /// `raw_bytes`, and a redis wire `BulkString`.
    #[test]
    fn a_byte_string_keeps_its_order() {
        let bytes = |text: &str| json!(text.as_bytes());
        for (name, a, b) in [
            (
                "a JSON body whose document was not captured",
                json!({"body": {"kind": "JsonRequestBody", "json": null, "raw_bytes": bytes("{\"amount\":120}")}}),
                json!({"body": {"kind": "JsonRequestBody", "json": null, "raw_bytes": bytes("{\"amount\":210}")}}),
            ),
            (
                "captured bytes",
                json!({"captured": true, "utf8": false, "text": null, "raw_bytes": bytes("120")}),
                json!({"captured": true, "utf8": false, "text": null, "raw_bytes": bytes("210")}),
            ),
            (
                "a redis bulk string",
                json!({"key": "k", "value": {"BulkString": bytes("120")}}),
                json!({"key": "k", "value": {"BulkString": bytes("210")}}),
            ),
            (
                "a redis bulk string under its other dialect's name",
                json!({"Bytes": bytes("120")}),
                json!({"Bytes": bytes("210")}),
            ),
        ] {
            assert_ne!(identity_args_hash(&a), identity_args_hash(&b), "{name}");
            assert_eq!(identity_differences(&a, &b), None, "{name}");
            if !name.starts_with("a JSON body") {
                // Bytes that are not a document have nothing to reorder; the
                // body's bytes are a document, which identity does read.
                assert!(!identity_applies(&a), "{name}: nothing to reorder");
            }
        }
    }

    /// A number is compared as it is hashed, by its text, so the comparison
    /// and the lookup agree: -0.0 and 0.0 are two values to both.
    #[test]
    fn a_number_is_compared_as_it_is_hashed() {
        for (a, b) in [
            (json!(-0.0), json!(0.0)),
            (json!([-0.0]), json!([0.0])),
            (json!({"k": -0.0}), json!({"k": 0.0})),
        ] {
            assert_eq!(
                same(&a, &b),
                identity_args_hash(&a) == identity_args_hash(&b),
                "{a} {b}"
            );
            assert!(!same(&a, &b), "{a} {b}");
        }
    }

    /// A reordering reported at an array does not hide what differs inside
    /// its members: each is followed to the member it matched.
    #[test]
    fn a_reordered_arrays_members_are_still_followed() {
        let (r, o) = (json!([["a", "b"], "c"]), json!(["c", ["b", "a"]]));
        assert_eq!(
            identity_differences(&r, &o),
            Some(vec![
                IdentityChange::ArrayOrder("$".to_owned()),
                IdentityChange::ArrayOrder("$[0]".to_owned()),
            ])
        );
        let (r, o) = (
            json!(["x", r#"{"a":["p","q"]}"#]),
            json!([r#"{"a":["q","p"]}"#, "x"]),
        );
        assert_eq!(
            identity_differences(&r, &o),
            Some(vec![
                IdentityChange::ArrayOrder("$".to_owned()),
                IdentityChange::DocumentText("$[1]".to_owned()),
            ])
        );
    }

    /// Members pair with their exact partner first, so a reorder is reported
    /// only where one happened: members of one identity written differently
    /// do not pair crosswise and report reorders nothing made.
    #[test]
    fn members_pair_with_their_exact_partner_first() {
        let (r, o) = (
            json!([["x", "y", "z"], ["y", "z", "x"], ["z", "x", "y"], "w"]),
            json!(["w", ["x", "y", "z"], ["y", "z", "x"], ["z", "x", "y"]]),
        );
        assert_eq!(
            identity_differences(&r, &o),
            Some(vec![IdentityChange::ArrayOrder("$".to_owned())])
        );
        let (r, o) = (
            json!([["a", "b"], ["b", "a"]]),
            json!([["b", "a"], ["a", "b"]]),
        );
        assert_eq!(
            identity_differences(&r, &o),
            Some(vec![IdentityChange::ArrayOrder("$".to_owned())]),
            "two members that swapped places are one reorder, at the array"
        );
    }

    /// Exact partners are paired across the whole array before any member is
    /// paired by identity, so the report does not depend on which member came
    /// first, and a member that kept its place is never counted as moved.
    #[test]
    fn exact_partners_are_paired_across_the_whole_array_first() {
        let (p, q, r) = (
            json!(["x", "y", "z"]),
            json!(["y", "z", "x"]),
            json!(["z", "x", "y"]),
        );
        assert_eq!(
            identity_differences(&json!([p, q]), &json!([q, r])),
            Some(vec![
                IdentityChange::ArrayOrder("$".to_owned()),
                IdentityChange::ArrayOrder("$[0]".to_owned()),
            ]),
            "q moved from 1 to 0, and p was written as r"
        );
        assert_eq!(
            identity_differences(&json!([q, r]), &json!([p, q])),
            Some(vec![
                IdentityChange::ArrayOrder("$".to_owned()),
                IdentityChange::ArrayOrder("$[1]".to_owned()),
            ]),
            "the same, from the other side"
        );
        assert_eq!(
            identity_differences(&json!([p, q, r]), &json!([p, r, q])),
            Some(vec![IdentityChange::ArrayOrder("$".to_owned())]),
            "q and r swapped places, exactly"
        );
        let (a, b, c) = (
            json!(["a", "b", "c"]),
            json!(["b", "c", "a"]),
            json!(["c", "a", "b"]),
        );
        let (b2, c2) = (json!(["c", "b", "a"]), json!(["a", "c", "b"]));
        assert_eq!(
            identity_differences(&json!([a, b, c]), &json!([a, b2, c2])),
            Some(vec![
                IdentityChange::ArrayOrder("$[1]".to_owned()),
                IdentityChange::ArrayOrder("$[2]".to_owned()),
            ]),
            "nothing moved at the outer array: members kept their places"
        );
    }

    /// Members rewritten in place pair with the member at their own place,
    /// every time: which of several candidates a member pairs with does not
    /// depend on a hash map's iteration order.
    #[test]
    fn rewritten_members_pair_in_place_every_time() {
        let (p, q) = (json!(["x", "y", "z"]), json!(["y", "z", "x"]));
        let (r, s) = (json!(["z", "x", "y"]), json!(["z", "y", "x"]));
        for _ in 0..64 {
            assert_eq!(
                identity_differences(&json!([p, q]), &json!([r, s])),
                Some(vec![
                    IdentityChange::ArrayOrder("$[0]".to_owned()),
                    IdentityChange::ArrayOrder("$[1]".to_owned()),
                ])
            );
        }
    }

    /// Bytes are a string, so bytes holding a JSON document are that document:
    /// a redis reply whose stored document was written in another order is
    /// the same value, as the write that stored it was.
    #[test]
    fn a_byte_string_holding_a_document_is_that_document() {
        let bulk = |text: &str| json!({"BulkString": text.as_bytes()});
        let (a, b) = (
            bulk(r#"{"a":1,"b":["x","y"]}"#),
            bulk(r#"{"b":["y","x"],"a":1}"#),
        );
        assert!(same(&a, &b));
        assert_eq!(identity_args_hash(&a), identity_args_hash(&b));
        assert_eq!(
            identity_differences(&a, &b),
            Some(vec![IdentityChange::DocumentText(
                "$.BulkString".to_owned()
            )])
        );
        assert_eq!(
            identity_differences(&a, &bulk(r#"{"a":2,"b":["y","x"]}"#)),
            None
        );
        assert_eq!(
            identity_differences(&bulk("120"), &bulk("210")),
            None,
            "bytes that are not a document keep their order"
        );
    }

    /// Identical members are paired in linear time.
    #[test]
    fn identical_members_pair_in_linear_time() {
        let many = serde_json::Value::Array(vec![json!(0); 200_000]);
        let started = std::time::Instant::now();
        assert!(same(&many, &many.clone()));
        assert!(
            started.elapsed() < std::time::Duration::from_secs(1),
            "{:?}",
            started.elapsed()
        );
    }

    /// Object key order is never read, by the hash or by the comparison.
    #[test]
    fn an_objects_key_order_is_never_read() {
        let (a, b) = (json!({"x": 1, "y": [2, 3]}), json!({"y": [3, 2], "x": 1}));
        assert_eq!(identity_args_hash(&a), identity_args_hash(&b));
        assert_eq!(
            identity_differences(&a, &b),
            Some(vec![IdentityChange::ArrayOrder("$.y".to_owned())])
        );
    }

    #[test]
    fn a_permuted_array_is_one_identity_and_names_its_path() {
        let (r, o) = (
            json!({"ids": ["a", "b", "c"]}),
            json!({"ids": ["c", "a", "b"]}),
        );
        assert_eq!(identity_args_hash(&r), identity_args_hash(&o));
        assert_eq!(
            identity_differences(&r, &o),
            Some(vec![IdentityChange::ArrayOrder("$.ids".to_owned())])
        );
    }

    #[test]
    fn a_document_in_a_string_is_one_identity_whatever_its_text_order() {
        let (r, o) = (
            json!({"b": r#"{"a":1,"b":["x","y"]}"#}),
            json!({"b": r#"{"b":["y","x"],"a":1}"#}),
        );
        assert_eq!(identity_args_hash(&r), identity_args_hash(&o));
        assert_eq!(
            identity_differences(&r, &o),
            Some(vec![IdentityChange::DocumentText("$.b".to_owned())])
        );
    }

    /// A reordering inside an array member is reported where it happened,
    /// not at the array holding it.
    #[test]
    fn a_nested_reordering_is_named_at_its_own_path() {
        let (r, o) = (
            json!({"g": [{"s": ["a", "b"]}, {"s": ["c"]}]}),
            json!({"g": [{"s": ["b", "a"]}, {"s": ["c"]}]}),
        );
        assert_eq!(
            identity_differences(&r, &o),
            Some(vec![IdentityChange::ArrayOrder("$.g[0].s".to_owned())])
        );
    }

    /// An enum set bound to a placeholder (`status = ANY($1)`), in the shape
    /// structured binds emit, is a set: a reordering is read by identity, not
    /// kept in order.
    #[test]
    fn an_enum_set_bound_to_a_placeholder_is_a_set() {
        let (r, o) = (
            json!({"sql": "…", "inputs": {"binds": {"$1": ["Failed", "Succeeded"], "$2": "m1"}}}),
            json!({"sql": "…", "inputs": {"binds": {"$1": ["Succeeded", "Failed"], "$2": "m1"}}}),
        );
        assert!(identity_applies(&r));
        assert_eq!(
            identity_differences(&r, &o),
            Some(vec![IdentityChange::ArrayOrder(
                "$.inputs.binds.$1".to_owned()
            )])
        );
    }

    /// Key order is not identity and is never reported.
    #[test]
    fn object_key_order_is_neither_identity_nor_reported() {
        let (r, o) = (json!({"a": 1, "b": 2}), json!({"b": 2, "a": 1}));
        assert!(!identity_applies(&o));
        assert_eq!(identity_differences(&r, &o), Some(vec![]));
    }

    #[test]
    fn what_identity_still_refuses() {
        for (name, r, o) in [
            ("a changed member", json!(["a", "b"]), json!(["a", "c"])),
            (
                "a form body's bytes in another order",
                json!({"kind": "FormUrlEncodedRequestBody", "raw_bytes": [97, 61, 49, 38, 98, 61, 50]}),
                json!({"kind": "FormUrlEncodedRequestBody", "raw_bytes": [97, 61, 50, 38, 98, 61, 49]}),
            ),
            (
                "a dropped duplicate",
                json!(["a", "a", "b"]),
                json!(["a", "b"]),
            ),
            (
                "a changed document",
                json!(r#"{"a":1}"#),
                json!(r#"{"a":2}"#),
            ),
            ("text is not a document", json!("b a"), json!("a b")),
            ("a scalar in a string is text", json!("1"), json!("1.0")),
            (
                "text is not an object",
                json!(r#"{"a":1}"#),
                json!({"a": 1}),
            ),
        ] {
            assert_eq!(identity_differences(&r, &o), None, "{name}");
            assert_ne!(identity_args_hash(&r), identity_args_hash(&o), "{name}");
        }
    }

    /// A call needs the second lookup only when another call could be written
    /// differently with its identity; in canonical order or not.
    #[test]
    fn identity_applies_wherever_a_call_could_be_written_another_way() {
        for (value, applies) in [
            (json!({"ids": ["a", "b"]}), true),
            (json!({"ids": ["b", "a"]}), true),
            (json!({"b": r#"{"a":1}"#}), true),
            (json!({"ids": ["a"]}), false),
            (json!({"n": [2, 1]}), true),
            (json!({"n": [2]}), false),
            (json!({"a": 1, "s": "text"}), false),
            (
                json!({"body": {"kind": "JsonRequestBody", "json": {"a": 1}, "text": "{\"a\":1}"}}),
                false,
            ),
            (
                json!({"body": {"kind": "JsonRequestBody", "json": {"a": ["x", "y"]}}}),
                true,
            ),
            (
                json!({"body": {"kind": "FormUrlEncodedRequestBody", "text": "a=1&b=2"}}),
                false,
            ),
        ] {
            assert_eq!(identity_applies(&value), applies, "{value}");
        }
    }

    /// Whatever the exact hash finds the same, identity finds the same and
    /// reports nothing: a form body's fields in another order, a JSON body's
    /// other renderings.
    #[test]
    fn identity_agrees_with_the_exact_hash_and_adds_nothing_to_it() {
        let form = |text: &str| json!({"kind": "FormUrlEncodedRequestBody", "text": text});
        let json_body = |text: &str| json!({"kind": "JsonRequestBody", "json": {"a": 1}, "text": text, "raw_bytes": [1, 2]});
        for (r, o) in [
            (form("a=1&b=2"), form("b=2&a=1")),
            (json_body("one"), json_body("another")),
        ] {
            assert_eq!(
                crate::replay::canonical_args_hash(&r),
                crate::replay::canonical_args_hash(&o),
                "precondition: the exact hash finds them the same"
            );
            assert_eq!(identity_differences(&r, &o), Some(vec![]));
        }
        let (r, o) = (form("a=1&b=2"), form("a=2&b=1"));
        assert_eq!(
            identity_differences(&r, &o),
            None,
            "different fields differ"
        );
    }

    /// A captured JSON body's identity is its document's, as its exact hash is.
    #[test]
    fn a_captured_request_body_is_read_as_its_document() {
        let body = |json: Value, text: &str| json!({"body": {"kind": "JsonRequestBody", "json": json, "text": text}});
        let r = body(json!({"ids": ["x", "y"]}), "one rendering");
        let o = body(json!({"ids": ["y", "x"]}), "another rendering");
        assert_eq!(
            identity_differences(&r, &o),
            Some(vec![IdentityChange::ArrayOrder(
                "$.body.json.ids".to_owned()
            )])
        );
    }
}
