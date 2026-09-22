//! Source gate for the one capture choice that fails silently.
//!
//! `canonical::to_value` marks a present `Option` over `null`, so a result
//! captured through it replays `Some(None)` as itself. Arguments must NOT be
//! marked — their bytes are a lookup key — so they go through
//! `canonical::to_args_value`. A result captured through the args variant, or
//! through a bare `serde_json::to_value`, collapses `Some(None)` onto `None`
//! again and no behavioural test notices until a replay takes the wrong arm.
//!
//! So both escapes are enumerated here, from the source, and a new one fails
//! this test until someone adds it below with the reason it is safe.

use std::path::{Path, PathBuf};

/// Every call of `to_args_value*` outside its definition. Each is an ARGUMENT
/// capture; the reason says why.
const ARGS_SITES: &[(&str, &str, &str)] = &[
    (
        "crates/deja/src/lib.rs",
        "crate::canonical::to_args_value(self.0)",
        "`capture!`'s serde arm, the capture of every inferred boundary argument",
    ),
    (
        "crates/deja/src/lib.rs",
        "crate::canonical::to_args_value_or_null(value)",
        "`value::serialize`, which the boundary macro emits for declared args",
    ),
    (
        "crates/deja-derive/src/recordable.rs",
        "::deja_runtime::canonical::to_args_value_or_null(&#val)",
        "the recordable delegate's per-argument capture",
    ),
];

/// Every bare `serde_json::to_value(` in non-test source of the crates that
/// capture values for replay. None may take a boundary's generic return value.
const BARE_SERDE_SITES: &[(&str, &str, &str)] = &[
    (
        "crates/deja/src/lib.rs",
        "let json = serde_json::to_value(&record)",
        "serialises the DejaDatabaseResult envelope, whose `value` was already captured by canonical::to_value",
    ),
    (
        "crates/deja/src/lib.rs",
        "serde_json::to_value(self).unwrap_or(serde_json::Value::Null)",
        "a row image of JSON column values, with no Rust Option left to collapse",
    ),
];

const HINT: &str = "If you are adding a site, add it to the table above with the reason \
it is safe and update the count; if it captures a boundary's RETURN value, use \
canonical::to_value instead.";

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {dir:?}: {e}")) {
        let path = entry.expect("dir entry").path();
        if path.is_dir() {
            // Integration tests and build output are not capture code; this
            // file's own tables would otherwise match themselves.
            if !path.ends_with("tests") && !path.ends_with("target") {
                rust_files(&path, out);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// `(workspace-relative path, trimmed line)` for every non-comment line before
/// the file's first `#[cfg(test)]` that contains `needle`, across `src_dirs`.
fn call_lines(root: &Path, src_dirs: &[&str], needle: &str) -> Vec<(String, String)> {
    let mut files = Vec::new();
    for dir in src_dirs {
        rust_files(&root.join(dir), &mut files);
    }
    assert!(
        !files.is_empty(),
        "searched {src_dirs:?} under {root:?} and found no .rs files: the gate is looking in the wrong tree"
    );
    let mut hits = Vec::new();
    for file in files {
        let text = std::fs::read_to_string(&file).expect("read source");
        let relative = file
            .strip_prefix(root)
            .expect("under root")
            .to_string_lossy()
            .into_owned();
        for line in text.lines().take_while(|l| l.trim() != "#[cfg(test)]") {
            let trimmed = line.trim();
            if trimmed.starts_with("//") || !trimmed.contains(needle) {
                continue;
            }
            hits.push((relative.clone(), trimmed.to_owned()));
        }
    }
    hits
}

fn assert_sites(found: &[(String, String)], expected: &[(&str, &str, &str)], what: &str) {
    assert!(
        !found.is_empty(),
        "found no {what} at all; the tables below list {} — the search, not the code, is wrong",
        expected.len()
    );
    for (file, line) in found {
        assert!(
            expected
                .iter()
                .any(|(f, needle, _)| f == file && line.contains(needle)),
            "unlisted {what} at {file}: `{line}`. {HINT}"
        );
    }
    for (file, needle, reason) in expected {
        let matches = found
            .iter()
            .filter(|(f, line)| f == file && line.contains(needle))
            .count();
        assert_eq!(
            matches, 1,
            "listed {what} `{needle}` in {file} ({reason}) matched {matches} lines; it moved or was removed. {HINT}"
        );
    }
    assert_eq!(found.len(), expected.len(), "{what}: {found:?}. {HINT}");
}

#[test]
fn only_argument_captures_skip_the_present_option_marker() {
    let root = workspace_root();
    let definition = std::fs::read_to_string(root.join("crates/deja-runtime/src/canonical.rs"))
        .expect("canonical.rs");
    assert!(
        definition.contains("pub fn to_args_value<"),
        "canonical::to_args_value is gone; this gate no longer guards anything"
    );
    let found: Vec<_> = call_lines(&root, &["crates"], "to_args_value")
        .into_iter()
        .filter(|(file, _)| file != "crates/deja-runtime/src/canonical.rs")
        .collect();
    assert_sites(&found, ARGS_SITES, "`to_args_value` call");
}

#[test]
fn no_result_capture_bypasses_the_canonical_serialiser() {
    let found = call_lines(
        &workspace_root(),
        &["crates/deja/src", "crates/deja-derive/src"],
        "serde_json::to_value(",
    );
    assert_sites(&found, BARE_SERDE_SITES, "bare `serde_json::to_value(`");
}
