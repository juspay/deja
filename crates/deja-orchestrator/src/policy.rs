//! Typed, versioned replay argument policy. A run carries the exact snapshot
//! selected at admission; scorer and delta never consult a mutable "current"
//! policy. This version supports only the KM encryption-service selector and
//! three fail-closed argument treatments, not arbitrary ignore paths.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Treatment {
    PlainBase64JsonOrder,
    HttpResponseHeaderGroups,
    SignedBodyDerivedSignature,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundarySelector {
    pub boundary: String,
    pub trait_name: String,
    pub method_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryRule {
    #[serde(flatten)]
    pub selector: BoundarySelector,
    pub treatments: Vec<Treatment>,
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

impl BoundaryPolicyDocument {
    pub fn validate(&self) -> Result<(), String> {
        if self.rules.len() > 16 {
            return Err("too many boundary policy rules".into());
        }
        let mut selectors = std::collections::BTreeSet::new();
        for rule in &self.rules {
            if rule.selector.boundary != "km"
                || rule.selector.trait_name != "common_utils::keymanager"
                || rule.selector.method_name != "call_encryption_service"
            {
                return Err("unsupported boundary policy selector".into());
            }
            if !selectors.insert((
                rule.selector.boundary.as_str(),
                rule.selector.trait_name.as_str(),
                rule.selector.method_name.as_str(),
            )) {
                return Err("duplicate boundary selector".into());
            }
            if rule.treatments.is_empty() {
                return Err("each rule must specify at least one treatment".into());
            }
            let mut treatments = std::collections::BTreeSet::new();
            for treatment in &rule.treatments {
                if !treatments.insert(*treatment) {
                    return Err("duplicate treatment".into());
                }
            }
        }
        Ok(())
    }

    /// Reordering rules or treatments does not change which arguments are
    /// allowed. The digest is over the typed policy, never unvalidated JSON.
    pub fn canonical_digest(&self) -> Result<String, String> {
        self.validate()?;
        let mut rules = self.rules.clone();
        rules.sort_by(|a, b| {
            (
                &a.selector.boundary,
                &a.selector.trait_name,
                &a.selector.method_name,
            )
                .cmp(&(
                    &b.selector.boundary,
                    &b.selector.trait_name,
                    &b.selector.method_name,
                ))
        });
        for rule in &mut rules {
            rule.treatments.sort_unstable();
        }
        let bytes = serde_json::to_vec(&Self { rules }).map_err(|e| e.to_string())?;
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

    pub fn allows(
        &self,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
        treatment: Treatment,
    ) -> bool {
        self.rules.iter().any(|rule| {
            rule.selector.boundary == boundary
                && rule.selector.trait_name == trait_name
                && rule.selector.method_name == method_name
                && rule.treatments.contains(&treatment)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn km_rule(treatments: Vec<Treatment>) -> BoundaryRule {
        BoundaryRule {
            selector: BoundarySelector {
                boundary: "km".into(),
                trait_name: "common_utils::keymanager".into(),
                method_name: "call_encryption_service".into(),
            },
            treatments,
        }
    }

    #[test]
    fn the_policy_digest_names_typed_semantics_not_list_order() {
        let rule = km_rule(vec![
            Treatment::PlainBase64JsonOrder,
            Treatment::HttpResponseHeaderGroups,
        ]);
        let reordered = km_rule(vec![
            Treatment::HttpResponseHeaderGroups,
            Treatment::PlainBase64JsonOrder,
        ]);
        let a = BoundaryPolicyDocument { rules: vec![rule] };
        let b = BoundaryPolicyDocument {
            rules: vec![reordered],
        };
        assert_eq!(a.canonical_digest(), b.canonical_digest());
        let snapshot = BoundaryPolicySnapshot {
            revision_id: Some(1),
            digest: a.canonical_digest().expect("valid policy"),
            rules: a.rules,
        };
        assert!(snapshot.validate().is_ok());
        assert!(snapshot.allows(
            "km",
            "common_utils::keymanager",
            "call_encryption_service",
            Treatment::PlainBase64JsonOrder
        ));
        assert!(!snapshot.allows(
            "km",
            "common_utils::keymanager",
            "call_encryption_service",
            Treatment::SignedBodyDerivedSignature
        ));
    }

    #[test]
    fn unsupported_and_duplicate_rules_fail_closed() {
        let mut unknown = km_rule(vec![Treatment::PlainBase64JsonOrder]);
        unknown.selector.method_name = "other".into();
        assert!(BoundaryPolicyDocument {
            rules: vec![unknown]
        }
        .validate()
        .is_err());
        assert!(BoundaryPolicyDocument {
            rules: vec![km_rule(vec![Treatment::PlainBase64JsonOrder; 2])]
        }
        .validate()
        .is_err());
        let rule = km_rule(vec![Treatment::PlainBase64JsonOrder]);
        assert!(BoundaryPolicyDocument {
            rules: vec![rule.clone(), rule]
        }
        .validate()
        .is_err());
        assert!(
            BoundaryPolicyDocument { rules: vec![] }.validate().is_ok(),
            "an explicit empty policy means strict treatment"
        );
    }
}
