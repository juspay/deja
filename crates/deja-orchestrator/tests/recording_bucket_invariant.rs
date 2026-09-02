//! Where a recording is must have ONE answer.
//!
//! `S3Config::from_env()` yields the deployment's own bucket. That is right for
//! things the deployment owns — run artifacts, the shared codebundle — and
//! wrong for a recording, which lives wherever its system declares. When the
//! listing endpoints learned to resolve per system and the replay pull path did
//! not, a prism recording could be listed, offered, and then fail "no landing
//! objects" on the pull: found by one reader and not by another.
//!
//! No type can express "this bucket is a recording's". So it is asserted by
//! accounting instead: every call is counted, per file, against a list where
//! each one is named and justified. A new call fails this test rather than
//! quietly reintroducing the split — the point is not the number, it is that
//! adding one makes somebody write down which kind it is.

use std::collections::BTreeMap;

/// Every `S3Config::from_env()` in the orchestrator, by file, with why.
///
/// RECORDING readers must resolve through `system::recording_scope` and appear
/// here only because they overwrite `cfg.bucket` immediately afterwards.
/// DEPLOYMENT-OWNED buckets are correct as they are and say so.
fn expected() -> BTreeMap<&'static str, (usize, &'static str)> {
    BTreeMap::from([
        (
            "src/main.rs",
            (
                5,
                "three recording readers that overwrite the bucket from `scan_scope` \
                 (the listing, its correlations sibling, and the listing's manifest \
                 enrichment, which must use the SCANNED bucket or it looks for a \
                 prism recording's seal in hyperswitch-art and reports every row \
                 unsealed), and two that take the bucket from a parsed `s3://` \
                 artifact URI",
            ),
        ),
        (
            "src/lifecycle/mod.rs",
            (
                3,
                "the run-artifact sink, which is deployment-owned; and the landing \
                 poll and the pull, which overwrite the bucket from \
                 `system::recording_scope`",
            ),
        ),
        (
            "src/lib.rs",
            (1, "overwrites the bucket from its caller's explicit choice"),
        ),
        (
            "src/codebundle.rs",
            (
                1,
                "the codebundle bucket is deployment-owned, not per-system",
            ),
        ),
        (
            "src/api/runs.rs",
            (1, "builds the codebundle URI, which is deployment-owned"),
        ),
    ])
}

/// Counting `from_env()` catches a NEW unscoped read but not an existing one
/// that stops being scoped — removing the resolution leaves the count identical.
/// So the recording readers are counted from the other side too: the pull path
/// must resolve, and this says how many times.
#[test]
fn the_recording_readers_still_resolve_per_system() {
    let lifecycle = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/lifecycle/mod.rs"),
    )
    .expect("read lifecycle");
    assert_eq!(
        lifecycle.matches("system::recording_scope(").count(),
        2,
        "the landing poll and the pull each resolve the recording's own bucket. If one stopped, a \
         run whose recording lives in another system's bucket polls and pulls the deployment's \
         own and reports a recording that never landed."
    );
}

/// The same reverse count for the API readers, which resolve differently: they
/// overwrite a `from_env()` config's bucket from `scan_scope`, so deleting the
/// resolution leaves the `from_env()` count identical and every other test green.
///
/// Measured, not assumed: removing the LISTING's resolution passed all fourteen
/// binary tests. The correlations endpoint happens to be covered by a handler
/// test that asserts its refusal, but the listing had nothing.
///
/// Counts only the shipped half. The bare substring is far higher because the
/// tests exercise `scan_scope` heavily, and an invariant that moves whenever
/// somebody adds a test case is not an invariant.
#[test]
fn the_api_recording_readers_still_resolve_per_system() {
    let main = std::fs::read_to_string(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src/main.rs"),
    )
    .expect("read main");
    let shipped = main
        .split_once("#[cfg(test)]")
        .map(|(before, _)| before)
        .unwrap_or(&main);
    assert_eq!(
        shipped.matches("scan_scope(").count(),
        3,
        "one definition and one call from each API reader of a recording — the listing and its \
         correlations sibling. If one stopped resolving, that endpoint would answer about the \
         deployment's own bucket while the other answered about the system's, which is the split \
         this whole change removes."
    );
}

#[test]
fn every_deployment_bucket_read_is_accounted_for() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let mut found: BTreeMap<String, usize> = BTreeMap::new();
    let mut stack = vec![root.join("src")];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read src") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let text = std::fs::read_to_string(&path).expect("read source");
            let n = text.matches("S3Config::from_env()").count();
            if n > 0 {
                let rel = path
                    .strip_prefix(root)
                    .expect("under the crate")
                    .to_string_lossy()
                    .replace('\\', "/");
                *found.entry(rel).or_insert(0) += n;
            }
        }
    }

    let expected = expected();
    for (file, count) in &found {
        let (allowed, why) = expected.get(file.as_str()).copied().unwrap_or_else(|| {
            panic!(
                "{file} reads the deployment's bucket and is not accounted for. If it reads a \
                 RECORDING, resolve it through `system::recording_scope` instead — a recording \
                 lives where its system declares, not where the deployment does. If the bucket \
                 really is deployment-owned, add it to this test's list saying so."
            )
        });
        assert_eq!(
            *count, allowed,
            "{file} has {count} `S3Config::from_env()` calls, expected {allowed} ({why}). A new \
             one must be classified: recording buckets resolve per system, deployment buckets do \
             not."
        );
    }
    for file in expected.keys() {
        assert!(
            found.contains_key(*file),
            "{file} no longer reads the deployment bucket — remove it from this test's list so \
             the accounting stays exact rather than aspirational"
        );
    }
}
