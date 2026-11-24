//! Declared, review-only identity extracted from exact captured evidence.
//! This projection never participates in scoring or acknowledgment matching.

use std::collections::{BTreeMap, BTreeSet};

use deja_compactor::settings::{ReviewIdentityDeclaration, ReviewIdentitySelector};
use deja_core::ExecutionGraphNode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::divergence::ledger::{CallRecord, CallSide};

#[derive(Debug, Clone, Copy)]
pub enum EvidenceSide {
    Recorded,
    Observed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityPublication {
    pub version: u8,
    pub declaration: Option<DeclarationSource>,
    pub dimensions: BTreeMap<String, DimensionDefinition>,
    pub requests: BTreeMap<String, ReviewIdentity>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeclarationSource {
    pub digest: String,
    pub source: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReviewIdentity {
    pub dimensions: BTreeMap<String, Dimension>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DimensionDefinition {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dimension {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    pub evidence: Vec<IdentityEvidence>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct IdentityEvidence {
    pub selector: usize,
    pub value: String,
    pub boundary: String,
    pub operation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_event: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph_node_id: Option<u64>,
    pub location: String,
}

fn dimension(evidence: BTreeSet<IdentityEvidence>) -> Dimension {
    let values: BTreeSet<_> = evidence.iter().map(|entry| entry.value.clone()).collect();
    Dimension {
        status: match values.len() {
            0 => "unknown",
            1 => "known",
            _ => "ambiguous",
        }
        .to_owned(),
        value: if values.len() == 1 {
            values.into_iter().next()
        } else {
            None
        },
        evidence: evidence.into_iter().collect(),
    }
}

fn scalar(value: &serde_json::Value) -> Option<String> {
    value
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty() && value.len() <= 256)
        .map(str::to_owned)
}

fn selected_values(
    selector: &ReviewIdentitySelector,
    call: &CallRecord,
    side: &CallSide,
    graph: &BTreeMap<u64, &ExecutionGraphNode>,
) -> Vec<(String, String, Option<u64>)> {
    match selector {
        ReviewIdentitySelector::Argument { boundary, pointer } if boundary == &call.boundary => {
            side.args
                .as_ref()
                .and_then(|args| args.pointer(pointer))
                .and_then(scalar)
                .map(|value| vec![(value, format!("args{pointer}"), side.graph_node_id)])
                .unwrap_or_default()
        }
        ReviewIdentitySelector::Header {
            boundary,
            pointer,
            name,
        } if boundary == &call.boundary => {
            let Some(headers) = side.args.as_ref().and_then(|args| args.pointer(pointer)) else {
                return vec![];
            };
            let mut values = Vec::new();
            if let Some(pairs) = headers.as_array() {
                for (index, pair) in pairs.iter().enumerate() {
                    let Some(pair) = pair.as_array().filter(|pair| pair.len() == 2) else {
                        continue;
                    };
                    if pair[0]
                        .as_str()
                        .is_some_and(|key| key.eq_ignore_ascii_case(name))
                    {
                        if let Some(value) = scalar(&pair[1]) {
                            values.push((
                                value,
                                format!("args{pointer}/{index}/1"),
                                side.graph_node_id,
                            ));
                        }
                    }
                }
            } else if let Some(headers) = headers.as_object() {
                for (key, value) in headers {
                    if key.eq_ignore_ascii_case(name) {
                        if let Some(value) = scalar(value) {
                            let key = key.replace('~', "~0").replace('/', "~1");
                            values.push((
                                value,
                                format!("args{pointer}/{key}"),
                                side.graph_node_id,
                            ));
                        }
                    }
                }
            }
            values
        }
        ReviewIdentitySelector::GraphField { field } => {
            let mut next = side.graph_node_id;
            let mut seen = BTreeSet::new();
            let mut values = Vec::new();
            while let Some(id) = next {
                if !seen.insert(id) {
                    break;
                }
                let Some(node) = graph.get(&id) else {
                    break;
                };
                // An uncorrelated or foreign ancestor cannot establish this
                // request's identity. No timing or span-name inference.
                if node.correlation_id.as_deref() != call.correlation_id.as_deref() {
                    break;
                }
                if let Some(value) = node.fields.get(field).and_then(scalar) {
                    values.push((value, format!("graph.fields.{field}"), Some(id)));
                }
                next = node.parent_id;
            }
            values
        }
        _ => vec![],
    }
}

pub fn project(
    calls: &[CallRecord],
    graph: &[ExecutionGraphNode],
    declaration: Option<&ReviewIdentityDeclaration>,
    side: EvidenceSide,
) -> IdentityPublication {
    let Some(declaration) = declaration else {
        return IdentityPublication {
            version: 2,
            declaration: None,
            dimensions: BTreeMap::new(),
            requests: BTreeMap::new(),
            notes: vec![],
        };
    };
    let digest = hex::encode(Sha256::digest(
        serde_json::to_vec(declaration).unwrap_or_default(),
    ));
    let mut notes = Vec::new();
    for (dimension, definition) in &declaration.dimensions {
        for (index, selector) in definition.selectors.iter().enumerate() {
            let invalid = !selector.is_valid();
            if invalid {
                notes.push(format!(
                    "{dimension} selector {} is invalid and cannot establish identity.",
                    index + 1
                ));
            }
        }
    }
    // A duplicate node id must not choose whichever contradictory snapshot
    // happened to arrive last. Equal repeats are harmless; ambiguous ids are
    // removed from identity evidence altogether.
    let mut nodes = BTreeMap::new();
    let mut ambiguous = BTreeSet::new();
    for node in graph {
        if let Some(previous) = nodes.get(&node.node_id) {
            if *previous != node {
                ambiguous.insert(node.node_id);
            }
        } else {
            nodes.insert(node.node_id, node);
        }
    }
    for id in &ambiguous {
        nodes.remove(id);
    }
    let graph = nodes;
    let mut requests: BTreeMap<String, BTreeMap<String, BTreeSet<IdentityEvidence>>> =
        BTreeMap::new();
    for call in calls {
        let Some(correlation) = call.correlation_id.as_ref() else {
            continue;
        };
        let Some(captured) = (match side {
            EvidenceSide::Recorded => call.recorded.as_ref(),
            EvidenceSide::Observed => call.observed.as_ref(),
        }) else {
            continue;
        };
        let dimensions = requests.entry(correlation.clone()).or_default();
        for (id, definition) in &declaration.dimensions {
            let collected = dimensions.entry(id.clone()).or_default();
            for (index, selector) in definition.selectors.iter().enumerate() {
                if !selector.is_valid() {
                    continue;
                }
                for (value, location, node) in selected_values(selector, call, captured, &graph) {
                    collected.insert(IdentityEvidence {
                        selector: index,
                        value,
                        boundary: call.boundary.clone(),
                        operation: call.method_name.clone(),
                        recorded_event: call.source_event_global_sequence,
                        graph_node_id: node,
                        location,
                    });
                }
            }
        }
    }
    IdentityPublication {
        version: 2,
        dimensions: declaration
            .dimensions
            .iter()
            .map(|(id, definition)| {
                (
                    id.clone(),
                    DimensionDefinition {
                        label: definition.label.clone(),
                    },
                )
            })
            .collect(),
        declaration: Some(DeclarationSource {
            digest,
            source: "current_deployment".to_owned(),
        }),
        notes: {
            if !ambiguous.is_empty() {
                notes.push(format!(
                    "{} graph node identities are ambiguous; they were excluded.",
                    ambiguous.len()
                ));
            }
            notes
        },
        requests: requests
            .into_iter()
            .map(|(key, dimensions)| {
                (
                    key,
                    ReviewIdentity {
                        dimensions: dimensions
                            .into_iter()
                            .map(|(id, evidence)| (id, dimension(evidence)))
                            .collect(),
                    },
                )
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deja_compactor::settings::ReviewIdentityDimension;

    fn call(args: serde_json::Value) -> CallRecord {
        serde_json::from_value(serde_json::json!({
            "boundary":"transport", "trait_name":"Client", "method_name":"send", "kind":"matched", "blocking":false,
            "correlation_id":"r", "observed":{"args":args}, "recorded":{"args":{"tenant":"old"}}
        })).expect("valid semantic identity fixture")
    }

    fn declaration(value: serde_json::Value) -> ReviewIdentityDeclaration {
        serde_json::from_value(value).expect("valid semantic identity declaration")
    }

    #[test]
    fn arbitrary_nonpayment_dimensions_use_declared_evidence_without_inference() {
        let mut calls = [call(
            serde_json::json!({"url":"https://other.example/send", "headers":[["x-tenant","acme"]], "worker":"eu-west"}),
        )];
        calls[0].observed.as_mut().expect("fixture").graph_node_id = Some(2);
        let declaration = declaration(serde_json::json!({"dimensions": {
            "organization": {"label":"Organization", "selectors":[{"kind":"header","boundary":"transport","pointer":"/headers","name":"x-tenant"}]},
            "worker_region": {"selectors":[{"kind":"argument","boundary":"transport","pointer":"/worker"}]},
            "job_kind": {"label":"Job kind", "selectors":[{"kind":"graph_field","field":"task"}]}
        }}));
        let publication = project(
            &calls,
            &[node(2, None, "r", "index_document")],
            Some(&declaration),
            EvidenceSide::Observed,
        );
        assert_eq!(publication.version, 2);
        assert_eq!(publication.dimensions.len(), 3);
        assert_eq!(
            publication.dimensions["organization"].label.as_deref(),
            Some("Organization")
        );
        let dimensions = &publication.requests["r"].dimensions;
        assert_eq!(dimensions["organization"].value.as_deref(), Some("acme"));
        assert_eq!(
            dimensions["organization"].evidence[0].location,
            "args/headers/0/1"
        );
        assert_eq!(
            dimensions["worker_region"].value.as_deref(),
            Some("eu-west")
        );
        assert_eq!(
            dimensions["job_kind"].value.as_deref(),
            Some("index_document")
        );
        assert_eq!(dimensions["job_kind"].evidence[0].graph_node_id, Some(2));
        assert_eq!(
            publication.declaration.expect("fixture").source,
            "current_deployment"
        );
    }

    #[test]
    fn no_declaration_publishes_no_dimensions_and_does_not_scan_evidence() {
        let publication = project(
            &[call(
                serde_json::json!({"url":"https://guess.example", "tenant":"guess"}),
            )],
            &[node(2, None, "r", "one"), node(2, None, "r", "two")],
            None,
            EvidenceSide::Observed,
        );
        assert_eq!(publication.version, 2);
        assert!(publication.declaration.is_none());
        assert!(publication.requests.is_empty());
        assert!(publication.dimensions.is_empty());
        assert!(publication.notes.is_empty());
    }

    #[test]
    fn competing_declared_values_are_ambiguous_and_recorded_does_not_fill_candidate() {
        let calls = [
            call(serde_json::json!({"tenant":"one"})),
            call(serde_json::json!({"tenant":"two"})),
        ];
        let declaration = declaration(
            serde_json::json!({"dimensions":{"organization":{"selectors":[{"kind":"argument","boundary":"transport","pointer":"/tenant"}]}}}),
        );
        let result = project(&calls, &[], Some(&declaration), EvidenceSide::Observed);
        assert_eq!(
            result.requests["r"].dimensions["organization"].status,
            "ambiguous"
        );
        assert!(result.requests["r"].dimensions["organization"]
            .value
            .is_none());
        assert_eq!(
            result.requests["r"].dimensions["organization"]
                .evidence
                .len(),
            2
        );
        let no_candidate_value = [call(serde_json::json!({}))];
        assert_eq!(
            project(
                &no_candidate_value,
                &[],
                Some(&declaration),
                EvidenceSide::Observed
            )
            .requests["r"]
                .dimensions["organization"]
                .status,
            "unknown"
        );
        assert_eq!(
            project(
                &no_candidate_value,
                &[],
                Some(&declaration),
                EvidenceSide::Recorded
            )
            .requests["r"]
                .dimensions["organization"]
                .value
                .as_deref(),
            Some("old")
        );
    }

    fn node(id: u64, parent: Option<u64>, correlation: &str, task: &str) -> ExecutionGraphNode {
        serde_json::from_value(serde_json::json!({"node_id":id,"parent_id":parent,"sequence":id,"correlation_id":correlation,"span_name":"operation","target":"example","level":"INFO","fields":{"task":task},"started_ns":0})).expect("valid semantic identity fixture")
    }

    #[test]
    fn graph_identity_uses_exact_node_ancestry_and_refuses_foreign_or_duplicate_nodes() {
        let mut call = call(serde_json::json!({}));
        call.observed.as_mut().expect("fixture").graph_node_id = Some(2);
        let declaration = declaration(
            serde_json::json!({"dimensions":{"job_kind":{"selectors":[{"kind":"graph_field","field":"task"}]}}}),
        );
        let graph = [
            node(2, Some(1), "r", "index"),
            node(1, None, "foreign", "other-task"),
        ];
        let publication = project(
            &[call.clone()],
            &graph,
            Some(&declaration),
            EvidenceSide::Observed,
        );
        assert_eq!(
            publication.requests["r"].dimensions["job_kind"]
                .value
                .as_deref(),
            Some("index")
        );
        assert_eq!(
            publication.requests["r"].dimensions["job_kind"]
                .evidence
                .len(),
            1
        );
        let contradictory = [node(2, None, "r", "one"), node(2, None, "r", "two")];
        let publication = project(
            &[call],
            &contradictory,
            Some(&declaration),
            EvidenceSide::Observed,
        );
        assert_eq!(
            publication.requests["r"].dimensions["job_kind"].status,
            "unknown"
        );
        assert!(publication.notes[0].contains("ambiguous"));
    }

    #[test]
    fn invalid_programmatic_selector_is_reported_without_fallback() {
        let declaration = ReviewIdentityDeclaration {
            dimensions: BTreeMap::from([(
                "organization".into(),
                ReviewIdentityDimension {
                    label: None,
                    selectors: vec![ReviewIdentitySelector::Argument {
                        boundary: "transport".into(),
                        pointer: "not-a-pointer".into(),
                    }],
                },
            )]),
        };
        let publication = project(
            &[call(serde_json::json!({"url":"https://provider.example"}))],
            &[],
            Some(&declaration),
            EvidenceSide::Observed,
        );
        assert_eq!(
            publication.requests["r"].dimensions["organization"].status,
            "unknown"
        );
        assert!(publication.notes[0].contains("invalid"));
    }

    #[test]
    fn dimension_order_and_legacy_adapter_have_stable_canonical_digests() {
        let legacy = declaration(
            serde_json::json!({"connector":[{"kind":"argument","boundary":"transport","pointer":"/tenant"}],"flow":[]}),
        );
        let canonical = declaration(
            serde_json::json!({"dimensions":{"flow":{"selectors":[]},"connector":{"selectors":[{"kind":"argument","boundary":"transport","pointer":"/tenant"}]}}}),
        );
        let first = project(&[], &[], Some(&legacy), EvidenceSide::Observed);
        let second = project(&[], &[], Some(&canonical), EvidenceSide::Observed);
        assert_eq!(
            first.declaration.expect("fixture").digest,
            second.declaration.expect("fixture").digest
        );
        assert_eq!(
            first.dimensions.keys().collect::<Vec<_>>(),
            vec!["connector", "flow"]
        );
        let mut reversed = canonical.clone();
        reversed.dimensions = canonical.dimensions.into_iter().rev().collect();
        assert_eq!(
            serde_json::to_vec(&reversed).expect("fixture"),
            serde_json::to_vec(&legacy).expect("fixture")
        );
    }
}
