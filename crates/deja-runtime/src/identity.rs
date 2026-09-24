//! The identity form of a call's arguments: what makes two calls the same call.
//!
//! Replay decides twice whether two argument values are the same, once to look
//! a call up and once to diff it. Both decisions read this one form, so they
//! cannot disagree. The candidate's lookup and the table the runner renders
//! hash it ([`identity_args_hash`]); the scorer compares it
//! ([`identity_differences`]).
//!
//! Two rules, both about order carrying no meaning:
//! - an array is a multiset: its members, in any order. Except an array of
//!   numbers, which is data (bytes, a captured body's `raw_bytes`, amounts),
//!   not a set: two byte strings with the same bytes are not the same bytes;
//! - a string holding a JSON object or array is that document, wrapped as
//!   `{"$json": …}` so a document sent as text never equals the same document
//!   sent as an object.
//!
//! Object keys are compared by key everywhere, so key order is never a rule
//! here and never reported: a recorded value's key order does not survive
//! transport, so there is nothing to compare it against.

use serde_json::Value;

/// A captured request body is hashed by its content (see
/// `replay::hash_request_body`); its identity is that content's, so two calls
/// the exact hash finds the same are always the same identity.
const JSON_REQUEST_BODY_KIND: &str = "JsonRequestBody";
const FORM_URLENCODED_REQUEST_BODY_KIND: &str = "FormUrlEncodedRequestBody";

/// What the identity form set aside to find two values the same call, at the
/// path of the raw value it read differently.
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

/// The identity form of `value`.
pub fn identity_form(value: &Value) -> Value {
    form(value)
}

/// Whether another call could be written differently from `value` and still
/// have its identity: it holds an array of two or more members that is not
/// numeric data, or a string read as a document. When it does not, the exact
/// lookup already addresses every call with its identity.
pub fn identity_applies(value: &Value) -> bool {
    match value {
        Value::String(text) => embedded_document(text).is_some(),
        Value::Object(map) if is_form_request_body(map) => false,
        Value::Object(map) if is_json_request_body(map) => {
            map.get("json").is_some_and(identity_applies)
        }
        Value::Object(map) => map.values().any(identity_applies),
        Value::Array(items) => {
            (items.len() > 1 && !items.iter().all(Value::is_number))
                || items.iter().any(identity_applies)
        }
        _ => false,
    }
}

/// The lookup hash of a call's identity. Identity keys are only ever looked up
/// among identity entries, never beside exact ones.
pub fn identity_args_hash(args: &Value) -> u64 {
    crate::replay::hash_value(crate::FNV_OFFSET_BASIS, &identity_form(args))
}

/// `None` when `recorded` and `observed` are different calls. Otherwise what
/// was read differently to find them the same: empty when they are equal as
/// JSON.
pub fn identity_differences(recorded: &Value, observed: &Value) -> Option<Vec<IdentityChange>> {
    if identity_form(recorded) != identity_form(observed) {
        return None;
    }
    let mut changes = Vec::new();
    differences(recorded, observed, "$", &mut changes);
    changes.sort();
    Some(changes)
}

fn form(value: &Value) -> Value {
    match value {
        Value::String(text) => match embedded_document(text) {
            Some(document) => {
                let mut wrapped = serde_json::Map::new();
                wrapped.insert("$json".to_owned(), form(&document));
                Value::Object(wrapped)
            }
            None => value.clone(),
        },
        Value::Object(map) if is_form_request_body(map) => {
            let mut fields: Vec<Value> = crate::replay::request_body_text(map)
                .unwrap_or_default()
                .split('&')
                .map(Value::from)
                .collect();
            fields.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
            let mut body = serde_json::Map::new();
            body.insert("fields".to_owned(), Value::Array(fields));
            body.insert(
                "kind".to_owned(),
                Value::from(FORM_URLENCODED_REQUEST_BODY_KIND),
            );
            Value::Object(body)
        }
        Value::Object(map) if is_json_request_body(map) => {
            let mut body = serde_json::Map::new();
            body.insert("kind".to_owned(), Value::from(JSON_REQUEST_BODY_KIND));
            if let Some(json) = map.get("json") {
                body.insert("json".to_owned(), form(json));
            }
            Value::Object(body)
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for key in keys {
                out.insert(key.clone(), form(&map[key]));
            }
            Value::Object(out)
        }
        Value::Array(items) => {
            let mut members: Vec<(String, Value)> = items
                .iter()
                .map(|item| {
                    let member = form(item);
                    (member.to_string(), member)
                })
                .collect();
            let data = !items.is_empty() && items.iter().all(Value::is_number);
            if !data {
                members.sort_by(|a, b| a.0.cmp(&b.0));
            }
            Value::Array(members.into_iter().map(|(_, member)| member).collect())
        }
        other => other.clone(),
    }
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

/// Where two values with one identity form differ as written. Both sides have
/// the same shape wherever this descends, because their forms are equal.
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
                    differences(value, other, &format!("{path}.{key}"), out);
                }
            }
        }
        (Value::Array(r), Value::Array(o)) => {
            let positional = r.len() == o.len()
                && r.iter()
                    .zip(o)
                    .all(|(a, b)| identity_form(a) == identity_form(b));
            if positional {
                for (index, (a, b)) in r.iter().zip(o).enumerate() {
                    differences(a, b, &format!("{path}[{index}]"), out);
                }
            } else {
                out.push(IdentityChange::ArrayOrder(path.to_owned()));
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
            ("numbers are data, not a set", json!([1, 2]), json!([2, 1])),
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
            (json!({"n": [2, 1]}), false),
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
