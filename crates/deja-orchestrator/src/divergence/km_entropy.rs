//! Classify one recorded/observed key-manager encryption request after the
//! candidate has already used the existing args-free serve. This never changes
//! lookup keys or the candidate: it judges what that candidate actually sent.
//!
//! A changed signature is only a declared derivative of a changed *rendering*
//! of the same JSON body. Deja does not have the HMAC key and does not claim
//! cryptographic verification. The raw arguments stay in the call ledger;
//! only digests of observed arguments reach the behaviour tree.

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

const MAX_DATA_BYTES: usize = 1024 * 1024;
const SIGNATURE: &str = "x-webhook-signature-512";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum KmClass {
    NotApplicable,
    Equal,
    Rendering,
    DeclaredDerived,
    Different,
}

/// Only digests leave the raw call ledger. `paired_serve` permits the delta to
/// normalize two independently tape-divergent calls only if both actually
/// served their same recorded counterpart, not an unrelated result.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KmEvidence {
    pub semantic_digest: String,
    pub raw_body_digest: String,
    pub signature_digest: String,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub paired_serve: bool,
}

#[derive(Debug)]
struct Envelope {
    raw_body: String,
    body: Value,
    non_signature_headers: Vec<(String, String)>,
    signature: String,
}

pub(super) fn is_km(boundary: &str, trait_name: &str, method_name: &str) -> bool {
    boundary == "km"
        && trait_name == "common_utils::keymanager"
        && method_name == "call_encryption_service"
}

fn envelope(args: &Value) -> Option<Envelope> {
    let data = args.get("request")?.get("data")?.as_str()?;
    if data.len() > MAX_DATA_BYTES * 2 {
        return None;
    }
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(data)
        .ok()?;
    if decoded.len() > MAX_DATA_BYTES {
        return None;
    }
    let wire: Value = serde_json::from_slice(&decoded).ok()?;
    let map = wire.as_object()?;
    if map.len() != 2 {
        return None;
    }
    let raw_body = map.get("body")?.as_str()?;
    if raw_body.len() > MAX_DATA_BYTES {
        return None;
    }
    let body: Value = serde_json::from_str(raw_body).ok()?;
    if !body.is_object() && !body.is_array() {
        return None;
    }
    let mut non_signature_headers = Vec::new();
    let mut signature = None;
    for pair in map.get("headers")?.as_array()? {
        let pair = pair.as_array()?;
        if pair.len() != 2 {
            return None;
        }
        let name = pair[0].as_str()?.to_ascii_lowercase();
        let value = pair[1].as_str()?;
        if name == SIGNATURE {
            if signature.replace(value.to_owned()).is_some() || value.is_empty() {
                return None;
            }
        } else {
            // Repeated header names with DISTINCT values can be order-sensitive
            // (e.g. forwarding chains). Identical pairs keep their multiplicity
            // and can still be safely permuted with other headers.
            if non_signature_headers
                .iter()
                .any(|(seen_name, seen_value)| seen_name == &name && seen_value != value)
            {
                return None;
            }
            non_signature_headers.push((name, value.to_owned()));
        }
    }
    non_signature_headers.sort_unstable();
    Some(Envelope {
        raw_body: raw_body.to_owned(),
        body,
        non_signature_headers,
        signature: signature?,
    })
}

fn same_except_data(left: &Value, right: &Value) -> bool {
    let (Some(l), Some(r)) = (left.as_object(), right.as_object()) else {
        return false;
    };
    if l.len() != r.len() || !same_members_except(l, r, "request") {
        return false;
    }
    let (Some(lr), Some(rr)) = (
        l.get("request").and_then(Value::as_object),
        r.get("request").and_then(Value::as_object),
    ) else {
        return false;
    };
    lr.len() == rr.len()
        && lr.contains_key("data")
        && rr.contains_key("data")
        && same_members_except(lr, rr, "data")
}

fn same_members_except(l: &Map<String, Value>, r: &Map<String, Value>, skip: &str) -> bool {
    l.iter()
        .filter(|(key, _)| key.as_str() != skip)
        .all(|(key, value)| r.get(key) == Some(value))
}

pub(super) fn classify(
    boundary: &str,
    trait_name: &str,
    method_name: &str,
    recorded: &Value,
    observed: &Value,
) -> KmClass {
    if !is_km(boundary, trait_name, method_name) {
        return KmClass::NotApplicable;
    }
    if recorded == observed {
        return KmClass::Equal;
    }
    if !same_except_data(recorded, observed) {
        return KmClass::Different;
    }
    let (Some(left), Some(right)) = (envelope(recorded), envelope(observed)) else {
        return KmClass::Different;
    };
    if left.body != right.body || left.non_signature_headers != right.non_signature_headers {
        return KmClass::Different;
    }
    if left.raw_body == right.raw_body {
        if left.signature != right.signature {
            KmClass::Different // Same signed bytes: a signature change is NOT rendering entropy.
        } else {
            KmClass::Rendering // Header-pair order or envelope JSON rendering moved.
        }
    } else if left.signature != right.signature {
        KmClass::DeclaredDerived
    } else {
        // Under the declared raw-byte signing rule, changed body bytes with
        // an unchanged signature are not a proved rendering equivalence.
        KmClass::Different
    }
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

/// Stable JSON digest: object keys sorted, arrays positional, and types tagged.
/// This is narrower than the generic identity's unordered-array policy: a
/// webhook body array may carry order, while only HTTP header pairs are a bag.
fn hash_json(hasher: &mut Sha256, value: &Value) {
    match value {
        Value::Null => hasher.update(b"n"),
        Value::Bool(b) => hasher.update(if *b { b"t" } else { b"f" }),
        Value::Number(n) => {
            hasher.update(b"d");
            hash_bytes(hasher, n.to_string().as_bytes());
        }
        Value::String(s) => {
            hasher.update(b"s");
            hash_bytes(hasher, s.as_bytes());
        }
        Value::Array(items) => {
            hasher.update(b"a");
            hasher.update((items.len() as u64).to_be_bytes());
            for item in items {
                hash_json(hasher, item);
            }
        }
        Value::Object(map) => {
            hasher.update(b"o");
            hasher.update((map.len() as u64).to_be_bytes());
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            for key in keys {
                hash_bytes(hasher, key.as_bytes());
                hash_json(hasher, &map[key]);
            }
        }
    }
}

fn hash_object_except(hasher: &mut Sha256, map: &Map<String, Value>, skip: &str) {
    let mut keys: Vec<&String> = map.keys().filter(|k| k.as_str() != skip).collect();
    keys.sort_unstable();
    hasher.update((keys.len() as u64).to_be_bytes());
    for key in keys {
        hash_bytes(hasher, key.as_bytes());
        hash_json(hasher, &map[key]);
    }
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

pub(super) fn evidence(
    boundary: &str,
    trait_name: &str,
    method_name: &str,
    args: &Value,
    paired_serve: bool,
) -> Option<KmEvidence> {
    if !is_km(boundary, trait_name, method_name) {
        return None;
    }
    let outer = args.as_object()?;
    let request = outer.get("request")?.as_object()?;
    let env = envelope(args)?;
    let mut hasher = Sha256::new();
    hasher.update(b"km-semantic-v1");
    hash_object_except(&mut hasher, outer, "request");
    hash_object_except(&mut hasher, request, "data");
    hash_json(&mut hasher, &env.body);
    hasher.update((env.non_signature_headers.len() as u64).to_be_bytes());
    for (name, value) in &env.non_signature_headers {
        hash_bytes(&mut hasher, name.as_bytes());
        hash_bytes(&mut hasher, value.as_bytes());
    }
    Some(KmEvidence {
        semantic_digest: hex::encode(hasher.finalize()),
        raw_body_digest: digest(env.raw_body.as_bytes()),
        signature_digest: digest(env.signature.as_bytes()),
        paired_serve,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn args(body: &str, signature: &str, headers: &[(&str, &str)]) -> Value {
        let mut pairs = vec![json!(["X-Webhook-Signature-512", signature])];
        pairs.extend(headers.iter().map(|(name, value)| json!([name, value])));
        let wire = json!({"body": body, "headers": pairs});
        json!({
            "method": "POST",
            "endpoint": "data/encrypt",
            "request": {
                "data_identifier": "Merchant",
                "key_identifier": "merchant_1",
                "data": base64::engine::general_purpose::STANDARD.encode(wire.to_string())
            }
        })
    }

    fn class(recorded: &Value, observed: &Value) -> KmClass {
        classify(
            "km",
            "common_utils::keymanager",
            "call_encryption_service",
            recorded,
            observed,
        )
    }

    #[test]
    fn only_rendering_and_declared_derivation_are_absorbed() {
        let recorded = args(
            r#"{"a":1,"b":2}"#,
            "sig-a",
            &[("content-type", "application/json"), ("x-extra", "same")],
        );
        let header_order = args(
            r#"{"a":1,"b":2}"#,
            "sig-a",
            &[("x-extra", "same"), ("Content-Type", "application/json")],
        );
        assert_eq!(class(&recorded, &header_order), KmClass::Rendering);

        let reordered_body = args(
            r#"{"b":2,"a":1}"#,
            "sig-b",
            &[("x-extra", "same"), ("content-type", "application/json")],
        );
        assert_eq!(class(&recorded, &reordered_body), KmClass::DeclaredDerived);
        let a = evidence(
            "km",
            "common_utils::keymanager",
            "call_encryption_service",
            &recorded,
            true,
        )
        .expect("recorded km envelope");
        let b = evidence(
            "km",
            "common_utils::keymanager",
            "call_encryption_service",
            &reordered_body,
            true,
        )
        .expect("observed km envelope");
        assert_eq!(a.semantic_digest, b.semantic_digest);
        assert_ne!(a.raw_body_digest, b.raw_body_digest);
        assert_ne!(a.signature_digest, b.signature_digest);
    }

    #[test]
    fn a_signature_only_change_or_real_input_change_still_blocks() {
        let recorded = args(
            r#"{"a":1,"b":2}"#,
            "sig-a",
            &[("content-type", "application/json")],
        );
        assert_eq!(
            class(
                &recorded,
                &args(
                    r#"{"a":1,"b":2}"#,
                    "sig-b",
                    &[("content-type", "application/json")]
                )
            ),
            KmClass::Different
        );
        assert_eq!(
            class(
                &recorded,
                &args(
                    r#"{"b":2,"a":1}"#,
                    "sig-a",
                    &[("content-type", "application/json")]
                )
            ),
            KmClass::Different,
            "changed body bytes without a changed signature are not proved derived"
        );
        assert_eq!(
            class(
                &recorded,
                &args(
                    r#"{"a":1,"b":2,"enable_avs_check":true}"#,
                    "sig-b",
                    &[("content-type", "application/json")]
                )
            ),
            KmClass::Different
        );
        assert_eq!(
            class(
                &recorded,
                &args(
                    r#"{"a":1,"b":2}"#,
                    "sig-a",
                    &[("content-type", "text/html")]
                )
            ),
            KmClass::Different
        );
        assert_eq!(
            class(
                &recorded,
                &args(
                    r#"{"a":1,"b":2}"#,
                    "sig-a",
                    &[("content-type", "application/json"), ("date", "today")]
                )
            ),
            KmClass::Different
        );
        assert_eq!(
            class(
                &recorded,
                &args(
                    r#"{"a":1,"b":[2,1]}"#,
                    "sig-a",
                    &[("content-type", "application/json")]
                )
            ),
            KmClass::Different,
            "JSON body arrays remain positional"
        );
        let mut different_key = recorded.clone();
        different_key["request"]["key_identifier"] = json!("merchant_2");
        assert_eq!(class(&recorded, &different_key), KmClass::Different);
    }

    #[test]
    fn malformed_or_unsupported_arguments_fail_closed() {
        let recorded = args(r#"{"a":1}"#, "sig-a", &[]);
        let mut bad = recorded.clone();
        bad["request"]["data"] = json!("not-base64");
        assert_eq!(class(&recorded, &bad), KmClass::Different);
        let duplicate = json!({
            "body": r#"{"a":1}"#,
            "headers": [["X-Webhook-Signature-512", "sig-a"], ["x-webhook-signature-512", "sig-b"]]
        });
        bad["request"]["data"] =
            json!(base64::engine::general_purpose::STANDARD.encode(duplicate.to_string()));
        assert_eq!(class(&recorded, &bad), KmClass::Different);
        let forwarded = args(
            r#"{"a":1}"#,
            "sig-a",
            &[("X-Forwarded-For", "client"), ("X-Forwarded-For", "proxy")],
        );
        let reversed = args(
            r#"{"a":1}"#,
            "sig-a",
            &[("X-Forwarded-For", "proxy"), ("X-Forwarded-For", "client")],
        );
        assert_eq!(class(&forwarded, &reversed), KmClass::Different);
        assert!(evidence(
            "km",
            "common_utils::keymanager",
            "call_encryption_service",
            &forwarded,
            true,
        )
        .is_none());
        let repeated_a = args(
            r#"{"a":1}"#,
            "sig-a",
            &[
                ("X-Tag", "v"),
                ("content-type", "application/json"),
                ("x-tag", "v"),
            ],
        );
        let repeated_b = args(
            r#"{"a":1}"#,
            "sig-a",
            &[
                ("content-type", "application/json"),
                ("X-Tag", "v"),
                ("X-Tag", "v"),
            ],
        );
        assert_eq!(class(&repeated_a, &repeated_b), KmClass::Rendering);
        assert_eq!(
            classify(
                "db",
                "common_utils::keymanager",
                "call_encryption_service",
                &recorded,
                &bad
            ),
            KmClass::NotApplicable
        );
        assert!(evidence(
            "db",
            "common_utils::keymanager",
            "call_encryption_service",
            &recorded,
            false
        )
        .is_none());
    }
}
