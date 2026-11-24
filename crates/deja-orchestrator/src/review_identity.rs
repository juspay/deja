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
    pub connector: Dimension,
    pub flow: Dimension,
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

fn valid_selector(selector: &ReviewIdentitySelector) -> bool {
    match selector {
        ReviewIdentitySelector::Argument { boundary, pointer }
        | ReviewIdentitySelector::Header {
            boundary, pointer, ..
        } => {
            !boundary.trim().is_empty()
                && (pointer.is_empty() || pointer.starts_with('/'))
                && !pointer.as_bytes().iter().enumerate().any(|(i, byte)| {
                    *byte == b'~' && !matches!(pointer.as_bytes().get(i + 1), Some(b'0' | b'1'))
                })
                && !matches!(selector, ReviewIdentitySelector::Header { name, .. } if name.trim().is_empty())
        }
        ReviewIdentitySelector::GraphField { field } => !field.trim().is_empty(),
    }
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
            version: 1,
            declaration: None,
            requests: BTreeMap::new(),
            notes: vec![],
        };
    };
    let digest = hex::encode(Sha256::digest(
        serde_json::to_vec(declaration).unwrap_or_default(),
    ));
    let mut notes = Vec::new();
    for (dimension, selectors) in [
        ("connector", &declaration.connector),
        ("flow", &declaration.flow),
    ] {
        for (index, selector) in selectors.iter().enumerate() {
            let invalid = !valid_selector(selector);
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
    let mut requests: BTreeMap<String, (BTreeSet<IdentityEvidence>, BTreeSet<IdentityEvidence>)> =
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
        for (selectors, collected) in [
            (&declaration.connector, &mut dimensions.0),
            (&declaration.flow, &mut dimensions.1),
        ] {
            for (index, selector) in selectors.iter().enumerate() {
                if !valid_selector(selector) {
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
        version: 1,
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
            .map(|(key, (connector, flow))| {
                (
                    key,
                    ReviewIdentity {
                        connector: dimension(connector),
                        flow: dimension(flow),
                    },
                )
            })
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(args: serde_json::Value) -> CallRecord {
        serde_json::from_value(serde_json::json!({
            "boundary":"transport", "trait_name":"Client", "method_name":"send", "kind":"matched", "blocking":false,
            "correlation_id":"r", "observed":{"args":args}, "recorded":{"args":{"connector":"old"}}
        })).expect("valid semantic identity fixture")
    }

    #[test]
    fn identities_require_declarations_and_never_guess_from_destination() {
        let calls = [call(
            serde_json::json!({"url":"https://hooks.zurich.example/send", "headers":[["x-provider","adyen"]]}),
        )];
        assert!(project(&calls, &[], None, EvidenceSide::Observed)
            .declaration
            .is_none());
        let declaration = ReviewIdentityDeclaration {
            connector: vec![ReviewIdentitySelector::Header {
                boundary: "transport".into(),
                pointer: "/headers".into(),
                name: "x-provider".into(),
            }],
            flow: vec![],
        };
        let publication = project(&calls, &[], Some(&declaration), EvidenceSide::Observed);
        assert_eq!(
            publication.requests["r"].connector.value.as_deref(),
            Some("adyen")
        );
        assert_eq!(
            publication.requests["r"].connector.evidence[0].location,
            "args/headers/0/1"
        );
        assert_eq!(publication.requests["r"].flow.status, "unknown");
        assert_eq!(
            publication
                .declaration
                .expect("valid semantic identity fixture")
                .source,
            "current_deployment"
        );
    }

    #[test]
    fn competing_declared_values_are_ambiguous_and_recorded_does_not_fill_candidate() {
        let calls = [
            call(serde_json::json!({"connector":"one"})),
            call(serde_json::json!({"connector":"two"})),
        ];
        let declaration = ReviewIdentityDeclaration {
            connector: vec![ReviewIdentitySelector::Argument {
                boundary: "transport".into(),
                pointer: "/connector".into(),
            }],
            flow: vec![],
        };
        let result = project(&calls, &[], Some(&declaration), EvidenceSide::Observed);
        assert_eq!(result.requests["r"].connector.status, "ambiguous");
        assert!(result.requests["r"].connector.value.is_none());
        let no_candidate_value = [call(serde_json::json!({}))];
        assert_eq!(
            project(
                &no_candidate_value,
                &[],
                Some(&declaration),
                EvidenceSide::Observed
            )
            .requests["r"]
                .connector
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
                .connector
                .value
                .as_deref(),
            Some("old")
        );
    }
    fn node(id: u64, parent: Option<u64>, correlation: &str, flow: &str) -> ExecutionGraphNode {
        serde_json::from_value(serde_json::json!({"node_id":id,"parent_id":parent,"sequence":id,"correlation_id":correlation,"span_name":"operation","target":"example","level":"INFO","fields":{"task":flow},"started_ns":0})).expect("valid semantic identity fixture")
    }
    #[test]
    fn graph_identity_uses_exact_node_ancestry_and_refuses_foreign_or_duplicate_nodes() {
        let mut call = call(serde_json::json!({}));
        call.observed
            .as_mut()
            .expect("valid semantic identity fixture")
            .graph_node_id = Some(2);
        let declaration = ReviewIdentityDeclaration {
            connector: vec![],
            flow: vec![ReviewIdentitySelector::GraphField {
                field: "task".into(),
            }],
        };
        let graph = [
            node(2, Some(1), "r", "authorize"),
            node(1, None, "foreign", "foreign-task"),
        ];
        let publication = project(
            &[call.clone()],
            &graph,
            Some(&declaration),
            EvidenceSide::Observed,
        );
        assert_eq!(
            publication.requests["r"].flow.value.as_deref(),
            Some("authorize")
        );
        assert_eq!(publication.requests["r"].flow.evidence.len(), 1);
        let contradictory = [node(2, None, "r", "one"), node(2, None, "r", "two")];
        let publication = project(
            &[call],
            &contradictory,
            Some(&declaration),
            EvidenceSide::Observed,
        );
        assert_eq!(publication.requests["r"].flow.status, "unknown");
        assert!(publication.notes[0].contains("ambiguous"));
    }
    #[test]
    fn invalid_declaration_is_reported_without_a_host_fallback() {
        let declaration = ReviewIdentityDeclaration {
            connector: vec![ReviewIdentitySelector::Argument {
                boundary: "transport".into(),
                pointer: "not-a-pointer".into(),
            }],
            flow: vec![],
        };
        let publication = project(
            &[call(serde_json::json!({"url":"https://provider.example"}))],
            &[],
            Some(&declaration),
            EvidenceSide::Observed,
        );
        assert_eq!(publication.requests["r"].connector.status, "unknown");
        assert!(publication.notes[0].contains("invalid"));
    }
}
