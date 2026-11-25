//! Captured document projections for inspection. The shared scorer parser
//! supplies document meaning; this module neither compares nor hashes values.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallInspection {
    pub version: u32,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub arguments: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub result: BTreeMap<String, Value>,
}

impl CallInspection {
    pub fn from_values(arguments: Option<&Value>, result: Option<&Value>) -> Option<Self> {
        let documents = |value| {
            let mut output = BTreeMap::new();
            let mut remaining = 2 * 1024 * 1024;
            if let Some(value) = value {
                collect(value, "$", 0, &mut remaining, &mut output);
            }
            output
        };
        let arguments = documents(arguments);
        let result = documents(result);
        (!arguments.is_empty() || !result.is_empty()).then_some(Self {
            version: 1,
            arguments,
            result,
        })
    }
}

fn collect(
    value: &Value,
    path: &str,
    depth: usize,
    remaining: &mut usize,
    output: &mut BTreeMap<String, Value>,
) {
    if depth > 24 || output.len() >= 64 {
        return;
    }
    match value {
        Value::String(text) if text.len() <= *remaining => {
            // This is the scorer's existing inline/base64 UTF-8 JSON chain,
            // not a frontend implementation of that grammar.
            if let Some(document) = deja::identity::semantic_document(text) {
                *remaining = remaining.saturating_sub(text.len());
                output.insert(path.to_owned(), document.clone());
                collect(&document, path, depth + 1, remaining, output);
            }
        }
        Value::Object(object) => {
            for (key, child) in object {
                collect(
                    child,
                    &deja_kernel::append_json_path_member(path, key),
                    depth + 1,
                    remaining,
                    output,
                );
            }
        }
        Value::Array(array) => {
            for (index, child) in array.iter().enumerate() {
                collect(
                    child,
                    &format!("{path}[{index}]"),
                    depth + 1,
                    remaining,
                    output,
                );
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use serde_json::json;

    #[test]
    fn publishes_existing_parser_documents_without_rewriting_capture() {
        let encoded = base64::engine::general_purpose::STANDARD
            .encode(r#"{"body":"{\"metadata\":{\"rapyd\":null}}"}"#);
        let args = json!({"request":{"data":encoded}, "opaque":"not a document"});
        let before = args.clone();
        let inspection =
            CallInspection::from_values(Some(&args), None).expect("published inspection fixture");
        assert_eq!(
            inspection.arguments["$.request.data.body"],
            json!({"metadata":{"rapyd":null}})
        );
        assert!(!inspection.arguments.contains_key("$.opaque"));
        assert_eq!(args, before);
        assert_eq!(
            deja::identity::identity_args_hash(&args),
            deja::identity::identity_args_hash(&before)
        );
    }

    #[test]
    fn no_documents_or_oversized_text_do_not_fabricate_a_projection() {
        assert!(CallInspection::from_values(Some(&json!({"a":null})), None).is_none());
        assert!(
            CallInspection::from_values(Some(&json!("x".repeat(2 * 1024 * 1024 + 1))), None)
                .is_none()
        );
    }
}
