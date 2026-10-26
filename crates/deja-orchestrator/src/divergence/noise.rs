//! Revision-pinned, narrowly scoped argument noise. A rule never suppresses a
//! call: it compares the same recorded event's arguments and proves that every
//! remaining difference is confined to the explicitly approved targets.

use crate::policy::{BoundaryPolicySnapshot, BoundaryRule, NoiseTarget};
use base64::Engine as _;
use serde_json::Value;
use std::collections::BTreeMap;

const MAX_DATA_BYTES: usize = 1024 * 1024;
const MAX_HEADER_PAIRS: usize = 4096;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Context {
    Arguments,
    Inline,
    Decoded,
}

/// The paired-serve and independent-order checks belong to the caller.
/// Both scorecard equivalence and delta keys use the SAME canonical digest,
/// so a maintainer rule cannot be interpreted differently by those readers.
pub(super) fn equivalent(
    recorded: &Value,
    observed: &Value,
    boundary: &str,
    trait_name: &str,
    method_name: &str,
    snapshot: Option<&BoundaryPolicySnapshot>,
) -> bool {
    let Some(snapshot) = snapshot else {
        return false;
    };
    if recorded == observed
        || !snapshot.rules.iter().any(|rule| {
            rule.selector.boundary == boundary
                && rule.selector.trait_name == trait_name
                && rule.selector.method_name == method_name
        })
    {
        return false;
    }
    match (
        semantic_hash(recorded, boundary, trait_name, method_name, Some(snapshot)),
        semantic_hash(observed, boundary, trait_name, method_name, Some(snapshot)),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

fn decode_source(args: &Value, path: &str) -> Option<Value> {
    let text = args.pointer(path)?.as_str()?;
    if text.len() > MAX_DATA_BYTES * 2 {
        return None;
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(text)
        .ok()?;
    if bytes.len() > MAX_DATA_BYTES {
        return None;
    }
    let value: Value = serde_json::from_slice(&bytes).ok()?;
    (value.is_object() || value.is_array()).then_some(value)
}

fn rule_path(rule: &BoundaryRule) -> &str {
    match &rule.target {
        NoiseTarget::Scalar { json_pointer }
        | NoiseTarget::HeaderGroup { json_pointer }
        | NoiseTarget::HeaderValue { json_pointer, .. } => json_pointer,
    }
}

fn scalar_value(value: &Value) -> bool {
    !value.is_object() && !value.is_array()
}

fn escape_pointer(key: &str) -> String {
    if key.contains('~') || key.contains('/') {
        key.replace('~', "~0").replace('/', "~1")
    } else {
        key.to_owned()
    }
}

fn header_groups(items: &[Value]) -> Option<BTreeMap<String, Vec<&str>>> {
    if items.len() > MAX_HEADER_PAIRS {
        return None;
    }
    let mut groups: BTreeMap<String, Vec<&str>> = BTreeMap::new();
    for item in items {
        let pair = item.as_array()?;
        if pair.len() != 2 {
            return None;
        }
        let name = pair[0].as_str()?;
        let value = pair[1].as_str()?;
        if name.is_empty() || name.len() > 256 || value.len() > MAX_DATA_BYTES {
            return None;
        }
        groups
            .entry(name.to_ascii_lowercase())
            .or_default()
            .push(value);
    }
    Some(groups)
}

/// Stable candidate-argument key for a paired divergent call. The scorer has
/// already judged its tape-relative verdict; this key lets the three-way delta
/// inherit the SAME semantic drift across independently rendered replays.
/// Approved noise targets contribute a typed sentinel, never their raw value.
pub(super) fn semantic_digest(
    args: &Value,
    boundary: &str,
    trait_name: &str,
    method_name: &str,
    snapshot: Option<&BoundaryPolicySnapshot>,
) -> Option<String> {
    semantic_hash(args, boundary, trait_name, method_name, snapshot)
        .map(|digest| format!("v2:{}", hex::encode(digest)))
}

fn semantic_hash(
    args: &Value,
    boundary: &str,
    trait_name: &str,
    method_name: &str,
    snapshot: Option<&BoundaryPolicySnapshot>,
) -> Option<[u8; 32]> {
    use sha2::Digest as _;
    let mut matching: [Option<&BoundaryRule>; 32] = [None; 32];
    let mut count = 0;
    if let Some(snapshot) = snapshot {
        if snapshot.rules.len() > matching.len() {
            return None;
        }
        for rule in &snapshot.rules {
            if rule.selector.boundary == boundary
                && rule.selector.trait_name == trait_name
                && rule.selector.method_name == method_name
            {
                matching[count] = Some(rule);
                count += 1;
            }
        }
    }
    let rules = &matching[..count];
    let source = rules
        .first()
        .and_then(|rule| rule.and_then(|r| r.source_path.as_deref()));
    if rules
        .iter()
        .flatten()
        .any(|rule| rule.source_path.as_deref() != source)
    {
        return None;
    }
    let decoded = if let Some(path) = source {
        Some(decode_source(args, path)?)
    } else {
        None
    };
    let root = decoded.as_ref().unwrap_or(args);
    for rule in rules.iter().flatten() {
        let target = root.pointer(rule_path(rule))?;
        match &rule.target {
            NoiseTarget::Scalar { .. } if !scalar_value(target) => return None,
            NoiseTarget::HeaderGroup { .. } if header_groups(target.as_array()?).is_none() => {
                return None
            }
            NoiseTarget::HeaderValue { name, .. } => {
                let groups = header_groups(target.as_array()?)?;
                if groups
                    .get(&name.to_ascii_lowercase())
                    .is_none_or(|values| values.len() != 1)
                {
                    return None;
                }
            }
            _ => {}
        }
    }
    let mut hasher = sha2::Sha256::new();
    hasher.update(b"deja-semantic-argument-v2");
    hash_semantic_value(
        &mut hasher,
        args,
        "",
        Context::Arguments,
        source.zip(decoded.as_ref()),
        rules,
    )?;
    Some(hasher.finalize().into())
}

fn hash_bytes(hasher: &mut sha2::Sha256, bytes: &[u8]) {
    use sha2::Digest as _;
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

fn hash_semantic_value(
    hasher: &mut sha2::Sha256,
    value: &Value,
    path: &str,
    context: Context,
    source: Option<(&str, &Value)>,
    rules: &[Option<&BoundaryRule>],
) -> Option<()> {
    use sha2::Digest as _;
    if let Some((source_path, document)) = source {
        if path == source_path {
            hasher.update(b"encoded-document");
            return hash_semantic_value(hasher, document, "", Context::Decoded, None, rules);
        }
    }
    let mut scalar = false;
    let mut grouped = false;
    let mut ignored_header: Option<&str> = None;
    // A rule's pointer is relative to the decoded source, never the outer
    // arguments. Activate targets only after the source branch above enters
    // that document (or immediately when no source was configured).
    if source.is_none() {
        for rule in rules
            .iter()
            .flatten()
            .filter(|rule| rule_path(rule) == path)
        {
            match &rule.target {
                NoiseTarget::Scalar { .. } => scalar = true,
                NoiseTarget::HeaderGroup { .. } => grouped = true,
                NoiseTarget::HeaderValue { name, .. } => {
                    if ignored_header.replace(name).is_some() {
                        return None;
                    }
                }
            }
        }
    }

    if scalar {
        if grouped || ignored_header.is_some() || !scalar_value(value) {
            return None;
        }
        hasher.update(b"approved-scalar");
        return Some(());
    }
    if grouped || ignored_header.is_some() {
        return hash_headers(hasher, value.as_array()?, grouped, ignored_header);
    }
    if context != Context::Decoded {
        match deja::identity::captured_body_identity(value) {
            Some(deja::identity::CapturedBodyIdentity::Json(json)) => {
                hasher.update(b"json-request-body");
                return hash_semantic_value(
                    hasher,
                    json,
                    &format!("{path}/json"),
                    context,
                    source,
                    rules,
                );
            }
            Some(deja::identity::CapturedBodyIdentity::Form(fields)) => {
                hasher.update(b"form-request-body");
                hasher.update((fields.len() as u64).to_be_bytes());
                for field in fields {
                    hash_bytes(hasher, field.as_bytes());
                }
                return Some(());
            }
            None => {}
        }
    }
    match value {
        Value::Null => hasher.update(b"null"),
        Value::Bool(flag) => hasher.update(if *flag { b"true".as_slice() } else { b"false" }),
        Value::Number(number) => {
            hasher.update(b"number");
            hash_bytes(hasher, number.to_string().as_bytes());
        }
        Value::String(text) => {
            if context == Context::Decoded {
                hasher.update(b"string");
                hash_bytes(hasher, text.as_bytes());
            } else if let Some(document) = deja::identity::embedded_document(text) {
                hasher.update(b"inline-document");
                hash_semantic_value(hasher, &document, "", Context::Inline, None, &[])?;
            } else if let Some(document) = (context == Context::Arguments)
                .then(|| deja::identity::semantic_document(text))
                .flatten()
            {
                hasher.update(b"encoded-document");
                hash_semantic_value(hasher, &document, "", Context::Decoded, None, &[])?;
            } else {
                hasher.update(b"string");
                hash_bytes(hasher, text.as_bytes());
            }
        }
        Value::Object(map) => {
            hasher.update(b"object");
            hasher.update((map.len() as u64).to_be_bytes());
            let mut keys: Vec<_> = map.keys().collect();
            keys.sort_unstable();
            for key in keys {
                hash_bytes(hasher, key.as_bytes());
                let child = &map[key];
                let child_path = if rules.is_empty() && source.is_none() {
                    String::new()
                } else {
                    format!("{path}/{}", escape_pointer(key))
                };
                // Deja capture's own byte-string members have positional
                // bytes; do not mistake their JSON array representation for
                // an unordered collection.
                if context != Context::Decoded && deja::identity::is_byte_string(key, child) {
                    let targeted_bytes = rules.iter().flatten().any(|rule| {
                        rule_path(rule)
                            .strip_prefix(child_path.as_str())
                            .is_some_and(|tail| tail.starts_with('/'))
                    });
                    if let Some(document) = (!targeted_bytes)
                        .then(|| deja::identity::byte_document(child))
                        .flatten()
                    {
                        hasher.update(b"byte-document");
                        hash_semantic_value(hasher, &document, "", Context::Inline, None, &[])?;
                    } else {
                        hasher.update(b"byte-string");
                        hash_semantic_value(
                            hasher,
                            child,
                            &child_path,
                            Context::Decoded,
                            source,
                            rules,
                        )?;
                    }
                } else {
                    hash_semantic_value(hasher, child, &child_path, context, source, rules)?;
                }
            }
        }
        Value::Array(items) => {
            hasher.update(b"array");
            hasher.update((items.len() as u64).to_be_bytes());
            if context == Context::Decoded {
                for (index, child) in items.iter().enumerate() {
                    let child_path = if rules.is_empty() && source.is_none() {
                        String::new()
                    } else {
                        format!("{path}/{index}")
                    };
                    hash_semantic_value(hasher, child, &child_path, context, source, rules)?;
                }
            } else {
                let mut hashes = Vec::with_capacity(items.len());
                for (index, child) in items.iter().enumerate() {
                    let mut inner = sha2::Sha256::new();
                    let child_path = if rules.is_empty() && source.is_none() {
                        String::new()
                    } else {
                        format!("{path}/{index}")
                    };
                    hash_semantic_value(&mut inner, child, &child_path, context, source, rules)?;
                    hashes.push(inner.finalize());
                }
                hashes.sort_unstable();
                for hash in hashes {
                    hasher.update(hash);
                }
            }
        }
    }
    Some(())
}

fn hash_headers(
    hasher: &mut sha2::Sha256,
    items: &[Value],
    grouped: bool,
    ignored_header: Option<&str>,
) -> Option<()> {
    use sha2::Digest as _;
    let groups = header_groups(items)?;
    let ignored = ignored_header.map(str::to_ascii_lowercase);
    if ignored
        .as_ref()
        .is_some_and(|name| groups.get(name).is_none_or(|values| values.len() != 1))
    {
        return None;
    }
    if grouped {
        hasher.update(b"approved-header-groups");
        hasher.update((groups.len() as u64).to_be_bytes());
        for (name, values) in groups {
            hash_bytes(hasher, name.as_bytes());
            hasher.update((values.len() as u64).to_be_bytes());
            for value in values {
                if ignored.as_deref() == Some(name.as_str()) {
                    hasher.update(b"approved-header-value");
                } else {
                    hash_bytes(hasher, value.as_bytes());
                }
            }
        }
    } else {
        hasher.update(b"ordered-headers");
        hasher.update((items.len() as u64).to_be_bytes());
        for item in items {
            let pair = item.as_array()?;
            let name = pair[0].as_str()?;
            hash_bytes(hasher, name.as_bytes());
            if ignored
                .as_deref()
                .is_some_and(|ignored| name.eq_ignore_ascii_case(ignored))
            {
                hasher.update(b"approved-header-value");
            } else {
                hash_bytes(hasher, pair[1].as_str()?.as_bytes());
            }
        }
    }
    Some(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::{BoundaryPolicyDocument, BoundaryRule, BoundarySelector};
    fn snapshot(targets: Vec<NoiseTarget>, source: Option<&str>) -> BoundaryPolicySnapshot {
        let rules: Vec<_> = targets
            .into_iter()
            .map(|target| BoundaryRule {
                selector: BoundarySelector {
                    boundary: "api".into(),
                    trait_name: "service".into(),
                    method_name: "send".into(),
                },
                source_path: source.map(str::to_owned),
                target,
            })
            .collect();
        let document = BoundaryPolicyDocument {
            rules: rules.clone(),
        };
        BoundaryPolicySnapshot {
            revision_id: Some(1),
            digest: document.canonical_digest().expect("valid policy"),
            rules,
        }
    }
    fn matches(a: Value, b: Value, policy: &BoundaryPolicySnapshot) -> bool {
        equivalent(&a, &b, "api", "service", "send", Some(policy))
    }
    #[test]
    fn scalar_noise_approves_only_one_leaf_and_revocation_is_future_only() {
        let rule = NoiseTarget::Scalar {
            json_pointer: "/signature".into(),
        };
        let old = snapshot(vec![rule], None);
        assert!(matches(
            serde_json::json!({"body":1,"signature":"a"}),
            serde_json::json!({"body":1,"signature":"b"}),
            &old
        ));
        assert!(!matches(
            serde_json::json!({"body":1,"signature":"a"}),
            serde_json::json!({"body":2,"signature":"b"}),
            &old
        ));
        let new = snapshot(vec![], None);
        assert!(!matches(
            serde_json::json!({"body":1,"signature":"a"}),
            serde_json::json!({"body":1,"signature":"b"}),
            &new
        ));
        assert_eq!(old.rules.len(), 1);
    }
    #[test]
    fn header_groups_preserve_repeated_value_order_and_named_noise_is_narrow() {
        let policy = snapshot(
            vec![NoiseTarget::HeaderGroup {
                json_pointer: "/headers".into(),
            }],
            None,
        );
        assert!(matches(
            serde_json::json!({"headers":[["a","1"],["b","2"]]}),
            serde_json::json!({"headers":[["b","2"],["a","1"]]}),
            &policy
        ));
        assert!(!matches(
            serde_json::json!({"headers":[["a","1"],["a","2"]]}),
            serde_json::json!({"headers":[["a","2"],["a","1"]]}),
            &policy
        ));
        let policy = snapshot(
            vec![NoiseTarget::HeaderValue {
                json_pointer: "/headers".into(),
                name: "x-signature".into(),
            }],
            None,
        );
        assert!(matches(
            serde_json::json!({"headers":[["x-signature","a"],["x","1"]]}),
            serde_json::json!({"headers":[["x-signature","b"],["x","1"]]}),
            &policy
        ));
        assert!(!matches(
            serde_json::json!({"headers":[["x-signature","a"],["x","1"]]}),
            serde_json::json!({"headers":[["x","1"],["x-signature","b"]]}),
            &policy
        ));
    }
    #[test]
    fn composed_rules_approve_both_header_presentation_and_named_value_only() {
        let policy = snapshot(
            vec![
                NoiseTarget::HeaderGroup {
                    json_pointer: "/headers".into(),
                },
                NoiseTarget::HeaderValue {
                    json_pointer: "/headers".into(),
                    name: "x-signature".into(),
                },
            ],
            None,
        );
        let left = serde_json::json!({"headers":[["x-signature","a"],["x","1"]],"body":1});
        let right = serde_json::json!({"headers":[["x","1"],["x-signature","b"]],"body":1});
        assert!(matches(left.clone(), right, &policy));
        assert!(!matches(
            left,
            serde_json::json!({"headers":[["x","2"],["x-signature","b"]],"body":1}),
            &policy
        ));
    }
    #[test]
    fn semantic_digest_normalizes_only_approved_header_noise() {
        let header_policy = snapshot(
            vec![NoiseTarget::HeaderGroup {
                json_pointer: "/headers".into(),
            }],
            None,
        );
        let a = serde_json::json!({"headers":[["a","1"],["b","2"]],"body":{"x":1}});
        let reordered = serde_json::json!({"body":{"x":1},"headers":[["b","2"],["a","1"]]});
        let changed_value =
            serde_json::json!({"headers":[["a","changed"],["b","2"]],"body":{"x":1}});
        let digest = semantic_digest(&a, "api", "service", "send", Some(&header_policy));
        assert_eq!(
            digest,
            semantic_digest(&reordered, "api", "service", "send", Some(&header_policy))
        );
        assert_ne!(
            digest,
            semantic_digest(
                &changed_value,
                "api",
                "service",
                "send",
                Some(&header_policy)
            )
        );
        let signature_policy = snapshot(
            vec![NoiseTarget::HeaderValue {
                json_pointer: "/headers".into(),
                name: "x-signature".into(),
            }],
            None,
        );
        let signed_a =
            serde_json::json!({"headers":[["x-signature","s1"],["date","today"]],"body":{"x":1}});
        let signed_b =
            serde_json::json!({"headers":[["x-signature","s2"],["date","today"]],"body":{"x":1}});
        assert_eq!(
            semantic_digest(&signed_a, "api", "service", "send", Some(&signature_policy)),
            semantic_digest(&signed_b, "api", "service", "send", Some(&signature_policy))
        );
        assert_ne!(
            semantic_digest(&signed_a, "api", "service", "send", None),
            semantic_digest(&signed_b, "api", "service", "send", None),
            "an unapproved signature difference must affect the delta key"
        );
    }
    #[test]
    fn decoded_source_rule_never_masks_a_same_named_outer_argument() {
        use base64::Engine as _;
        let policy = snapshot(
            vec![NoiseTarget::Scalar {
                json_pointer: "/signature".into(),
            }],
            Some("/request/data"),
        );
        let args = |outer: &str, inner: &str| {
            let data = base64::engine::general_purpose::STANDARD
                .encode(format!(r#"{{"signature":"{inner}","body":1}}"#));
            serde_json::json!({"signature":outer,"request":{"data":data}})
        };
        let recorded = args("outer-a", "inner-a");
        let inner_only = args("outer-a", "inner-b");
        let outer_and_inner = args("outer-b", "inner-b");
        assert!(matches(recorded.clone(), inner_only.clone(), &policy));
        assert_eq!(
            semantic_digest(&recorded, "api", "service", "send", Some(&policy)),
            semantic_digest(&inner_only, "api", "service", "send", Some(&policy))
        );
        assert!(!matches(recorded.clone(), outer_and_inner.clone(), &policy));
        assert_ne!(
            semantic_digest(&recorded, "api", "service", "send", Some(&policy)),
            semantic_digest(&outer_and_inner, "api", "service", "send", Some(&policy)),
            "the outer signature was never approved"
        );
    }

    #[test]
    fn decoded_strings_remain_literal_under_an_unrelated_allowance() {
        let policy = snapshot(
            vec![NoiseTarget::Scalar {
                json_pointer: "/nonce".into(),
            }],
            Some("/data"),
        );
        let encode = |text: &str| base64::engine::general_purpose::STANDARD.encode(text);
        let args = |payload: &str| {
            serde_json::json!({
                "data": encode(&serde_json::json!({"nonce":"same", "payload":payload}).to_string())
            })
        };
        let recorded = args(r#"{"amount":1}"#);
        for changed in [r#"{ "amount":1}"#.to_owned(), encode(r#"{"amount":1}"#)] {
            let observed = args(&changed);
            assert!(deja::identity::semantic_differences(&recorded, &observed).is_none());
            assert!(!matches(recorded.clone(), observed.clone(), &policy));
            assert_ne!(
                semantic_digest(&recorded, "api", "service", "send", None),
                semantic_digest(&observed, "api", "service", "send", None),
            );
        }
    }

    #[test]
    fn digests_preserve_inline_tolerances_and_encoded_structure() {
        let digest = |args: &Value| semantic_digest(args, "api", "service", "send", None);
        let inline = serde_json::json!({"payload": r#"{"ids":[1,2]}"#});
        let reordered = serde_json::json!({"payload": r#"{"ids":[2,1]}"#});
        assert!(deja::identity::semantic_differences(&inline, &reordered).is_some());
        assert_eq!(digest(&inline), digest(&reordered));
        let encoded = |text: &str| {
            serde_json::json!({
                "payload": base64::engine::general_purpose::STANDARD.encode(text)
            })
        };
        let encoded_inline = encoded(r#"{"ids":[1,2]}"#);
        assert_ne!(digest(&inline), digest(&encoded_inline));
        assert_ne!(
            digest(&encoded_inline),
            digest(&encoded(r#"{"ids":[2,1]}"#))
        );
        assert_ne!(digest(&encoded("[-0.0]")), digest(&encoded("[0.0]")));
    }

    #[test]
    fn inline_documents_do_not_enable_nested_base64_normalization() {
        let encode = |text: &str| base64::engine::general_purpose::STANDARD.encode(text);
        let args = |text: &str| {
            serde_json::json!({
                "nonce":"same", "document":serde_json::json!({"payload":encode(text)}).to_string()
            })
        };
        let recorded = args(r#"{"a":1,"b":2}"#);
        let observed = args(r#"{"b":2,"a":1}"#);
        let policy = snapshot(
            vec![NoiseTarget::Scalar {
                json_pointer: "/nonce".into(),
            }],
            None,
        );
        assert!(deja::identity::semantic_differences(&recorded, &observed).is_none());
        assert!(!matches(recorded.clone(), observed.clone(), &policy));
        assert_ne!(
            semantic_digest(&recorded, "api", "service", "send", None),
            semantic_digest(&observed, "api", "service", "send", None),
        );
    }

    #[test]
    fn digests_share_legacy_capture_body_and_byte_document_identity() {
        let pairs = [
            (
                serde_json::json!({"Bytes": r#"{"a":1,"b":[1,2]}"#.as_bytes()}),
                serde_json::json!({"Bytes": r#"{"b":[2,1],"a":1}"#.as_bytes()}),
            ),
            (
                serde_json::json!({"kind":"JsonRequestBody", "json":{"ids":[1,2]}, "raw_bytes":[1]}),
                serde_json::json!({"kind":"JsonRequestBody", "json":{"ids":[2,1]}, "raw_bytes":[2]}),
            ),
            (
                serde_json::json!({"kind":"FormUrlEncodedRequestBody", "raw_bytes":b"a=1&b=2"}),
                serde_json::json!({"kind":"FormUrlEncodedRequestBody", "raw_bytes":b"b=2&a=1"}),
            ),
        ];
        for (recorded, observed) in pairs {
            assert!(deja::identity::semantic_differences(&recorded, &observed).is_some());
            assert_eq!(
                semantic_digest(&recorded, "api", "service", "send", None),
                semantic_digest(&observed, "api", "service", "send", None),
            );
        }
    }

    #[test]
    fn scalar_allowance_can_target_a_byte_inside_a_captured_document() {
        let policy = snapshot(
            vec![NoiseTarget::Scalar {
                json_pointer: "/Bytes/5".into(),
            }],
            None,
        );
        assert!(matches(
            serde_json::json!({"Bytes":br#"{"a":1}"#}),
            serde_json::json!({"Bytes":br#"{"a":2}"#}),
            &policy,
        ));
        assert!(!matches(
            serde_json::json!({"Bytes":br#"{"a":1}"#}),
            serde_json::json!({"Bytes":br#"{"b":2}"#}),
            &policy,
        ));
    }
}
