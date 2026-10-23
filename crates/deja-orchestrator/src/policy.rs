//! Immutable, per-system replay argument-noise policy. A run carries the
//! revision chosen at admission; scoring never rereads a mutable active rule.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BoundarySelector {
    pub boundary: String,
    pub trait_name: String,
    pub method_name: String,
}

/// One exact difference a maintainer approves. The JSON Pointer is relative
/// to the decoded source document, or to the root arguments when no source is
/// declared. Header rules require `[name, value]` pairs, never arbitrary bags.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum NoiseTarget {
    Scalar { json_pointer: String },
    HeaderGroup { json_pointer: String },
    HeaderValue { json_pointer: String, name: String },
}

impl NoiseTarget {
    fn pointer(&self) -> &str {
        match self {
            Self::Scalar { json_pointer }
            | Self::HeaderGroup { json_pointer }
            | Self::HeaderValue { json_pointer, .. } => json_pointer,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BoundaryRule {
    pub selector: BoundarySelector,
    /// JSON Pointer to a base64 UTF-8 JSON document within the call arguments.
    /// Absent means the target pointer is relative to the raw argument value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<String>,
    pub target: NoiseTarget,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryPolicyDocument {
    pub rules: Vec<BoundaryRule>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryPolicySnapshot {
    pub revision_id: Option<i64>,
    pub digest: String,
    pub rules: Vec<BoundaryRule>,
}

fn valid_pointer(path: &str) -> bool {
    if !path.starts_with('/') {
        return false;
    }
    let mut chars = path.chars();
    while let Some(ch) = chars.next() {
        if ch == '~' && !matches!(chars.next(), Some('0' | '1')) {
            return false;
        }
    }
    true
}

impl BoundaryPolicyDocument {
    pub fn validate(&self) -> Result<(), String> {
        if self.rules.len() > 32 {
            return Err("too many boundary noise rules".into());
        }
        for (index, rule) in self.rules.iter().enumerate() {
            let selector = &rule.selector;
            if [
                selector.boundary.as_str(),
                selector.trait_name.as_str(),
                selector.method_name.as_str(),
            ]
            .iter()
            .any(|name| {
                name.is_empty() || name.trim() != *name || name.len() > 256 || name.contains('*')
            }) {
                return Err("boundary selector needs bounded, exact names".into());
            }
            if rule
                .source_path
                .as_ref()
                .is_some_and(|path| path.len() > 256 || (!path.is_empty() && !valid_pointer(path)))
            {
                return Err("invalid encoded source JSON Pointer".into());
            }
            let pointer = rule.target.pointer();
            if pointer.len() > 256 || !valid_pointer(pointer) {
                return Err("noise target needs a non-root JSON Pointer".into());
            }
            if let NoiseTarget::HeaderValue { name, .. } = &rule.target {
                if name.is_empty() || name.len() > 256 {
                    return Err("invalid named header".into());
                }
            }
            for prior in &self.rules[..index] {
                if prior.selector != rule.selector {
                    continue;
                }
                if prior.source_path != rule.source_path {
                    return Err("one operation's noise rules need the same encoded source".into());
                }
                if prior.target.pointer() != pointer {
                    continue;
                }
                // One header-group presentation rule can compose with one
                // named-value rule. Any other overlap is ambiguous or broad.
                if !matches!(
                    (&prior.target, &rule.target),
                    (
                        NoiseTarget::HeaderGroup { .. },
                        NoiseTarget::HeaderValue { .. }
                    ) | (
                        NoiseTarget::HeaderValue { .. },
                        NoiseTarget::HeaderGroup { .. }
                    )
                ) {
                    return Err("overlapping boundary noise targets".into());
                }
            }
        }
        Ok(())
    }

    pub fn canonical_digest(&self) -> Result<String, String> {
        self.validate()?;
        let mut rules = self.rules.clone();
        rules.sort_unstable();
        let bytes = serde_json::to_vec(&Self { rules }).map_err(|error| error.to_string())?;
        use sha2::Digest;
        Ok(format!(
            "sha256:{}",
            hex::encode(sha2::Sha256::digest(bytes))
        ))
    }
}

impl BoundaryPolicySnapshot {
    pub fn validate(&self) -> Result<(), String> {
        let document = BoundaryPolicyDocument {
            rules: self.rules.clone(),
        };
        if document.canonical_digest()? != self.digest {
            return Err("boundary policy snapshot digest does not match its rules".into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(target: NoiseTarget) -> BoundaryRule {
        BoundaryRule {
            selector: BoundarySelector {
                boundary: "http_client".into(),
                trait_name: "client::Request".into(),
                method_name: "send".into(),
            },
            source_path: None,
            target,
        }
    }

    #[test]
    fn immutable_revisions_are_order_independent_and_removable() {
        let document = BoundaryPolicyDocument {
            rules: vec![
                rule(NoiseTarget::Scalar {
                    json_pointer: "/signature".into(),
                }),
                rule(NoiseTarget::HeaderGroup {
                    json_pointer: "/headers".into(),
                }),
            ],
        };
        let mut reordered = document.clone();
        reordered.rules.reverse();
        assert_eq!(document.canonical_digest(), reordered.canonical_digest());
        let old = BoundaryPolicySnapshot {
            revision_id: Some(1),
            digest: document.canonical_digest().expect("valid policy"),
            rules: document.rules,
        };
        let disabled = BoundaryPolicyDocument { rules: Vec::new() };
        let new = BoundaryPolicySnapshot {
            revision_id: Some(2),
            digest: disabled.canonical_digest().expect("valid empty revision"),
            rules: Vec::new(),
        };
        assert!(old.validate().is_ok() && new.validate().is_ok());
        assert_eq!(old.rules.len(), 2);
        assert!(new.rules.is_empty());
    }

    #[test]
    fn invalid_or_overlapping_paths_and_selectors_fail_closed() {
        let duplicate = rule(NoiseTarget::Scalar {
            json_pointer: "/x".into(),
        });
        assert!(BoundaryPolicyDocument {
            rules: vec![duplicate.clone(), duplicate]
        }
        .validate()
        .is_err());
        assert!(BoundaryPolicyDocument {
            rules: vec![rule(NoiseTarget::HeaderGroup {
                json_pointer: "not-a-pointer".into()
            })]
        }
        .validate()
        .is_err());
        let mut wildcard = rule(NoiseTarget::Scalar {
            json_pointer: "/x".into(),
        });
        wildcard.selector.boundary = "*".into();
        assert!(BoundaryPolicyDocument {
            rules: vec![wildcard]
        }
        .validate()
        .is_err());
        let mut different_source = rule(NoiseTarget::Scalar {
            json_pointer: "/x".into(),
        });
        different_source.source_path = Some("/request/data".into());
        assert!(BoundaryPolicyDocument {
            rules: vec![
                rule(NoiseTarget::HeaderGroup {
                    json_pointer: "/headers".into()
                }),
                different_source
            ]
        }
        .validate()
        .is_err());
    }
}
