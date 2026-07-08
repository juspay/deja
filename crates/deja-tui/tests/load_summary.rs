use std::fs;
use std::time::{SystemTime, UNIX_EPOCH};

use deja_tui::{load_artifacts, summarize, GRAPH_FILE_NAME, SEMANTIC_FILE_NAME};

#[test]
fn summary_mode_loader_accepts_direct_directory() {
    let dir = temp_artifact_dir("direct");
    fs::create_dir_all(&dir).expect("artifact dir");
    fs::write(
        dir.join(SEMANTIC_FILE_NAME),
        concat!(
            r#"{"global_sequence":0,"request_sequence":0,"correlation_id":"req-a","timestamp_ns":1,"boundary":"http_incoming","trait_name":"RequestIdMiddleware","method_name":"call","call_file":"request_id.rs","call_line":10,"call_column":3,"request":{},"args":{},"response":{},"result":{},"is_error":false,"duration_us":4,"event_schema_version":7,"provenance":"recorded","recon":"lossless","replay_strategy":"substitute"}"#,
            "\n"
        ),
    )
    .expect("semantic file");
    fs::write(
        dir.join(GRAPH_FILE_NAME),
        concat!(
            r#"{"node_id":7,"sequence":3,"span_name":"request","target":"router","level":"INFO","fields":{"request_id":"req-a"},"started_ns":1,"closed_ns":4001}"#,
            "\n",
            "{bad}\n"
        ),
    )
    .expect("graph file");

    let loaded = load_artifacts(&dir).expect("load");
    let summary = summarize(&loaded);
    assert_eq!(loaded.semantic_events.len(), 1);
    assert_eq!(loaded.graph_records.len(), 1);
    assert_eq!(loaded.graph_stats.as_ref().expect("graph stats").skipped, 1);
    assert_eq!(summary.request_counts, vec![("req-a".to_owned(), 1, 1)]);

    fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn loader_finds_sibling_graph_from_semantic_path() {
    let dir = temp_artifact_dir("nested-sibling");
    let semantic_dir = dir.join("semantic");
    let graph_dir = dir.join("graph");
    fs::create_dir_all(&semantic_dir).expect("semantic dir");
    fs::create_dir_all(&graph_dir).expect("graph dir");

    let semantic_file = semantic_dir.join(SEMANTIC_FILE_NAME);
    fs::write(
        &semantic_file,
        concat!(
            r#"{"global_sequence":0,"request_sequence":0,"correlation_id":"req-nested","timestamp_ns":1,"boundary":"http_incoming","trait_name":"RequestIdMiddleware","method_name":"call","call_file":"request_id.rs","call_line":10,"call_column":3,"request":{},"args":{},"response":{},"result":{},"is_error":false,"duration_us":4,"event_schema_version":7,"provenance":"recorded","recon":"lossless","replay_strategy":"substitute"}"#,
            "\n"
        ),
    )
    .expect("semantic file");
    fs::write(
        graph_dir.join(GRAPH_FILE_NAME),
        concat!(
            r#"{"node_id":7,"sequence":3,"span_name":"request","target":"router","level":"INFO","fields":{"request_id":"req-nested"},"started_ns":1,"closed_ns":4001}"#,
            "\n"
        ),
    )
    .expect("graph file");

    for input in [&semantic_dir, &semantic_file] {
        let loaded = load_artifacts(input).expect("load");
        let summary = summarize(&loaded);
        assert_eq!(loaded.semantic_events.len(), 1);
        assert_eq!(loaded.graph_records.len(), 1);
        assert_eq!(
            summary.request_counts,
            vec![("req-nested".to_owned(), 1, 1)]
        );
    }

    fs::remove_dir_all(dir).expect("cleanup");
}

#[test]
fn loader_accepts_hs41_recording_layout() {
    let dir = temp_artifact_dir("hs41-recording");
    let recording_dir = dir.join("recording");
    let graph_dir = dir.join("graph");
    fs::create_dir_all(&recording_dir).expect("recording dir");
    fs::create_dir_all(&graph_dir).expect("graph dir");

    let semantic_file = recording_dir.join(SEMANTIC_FILE_NAME);
    fs::write(
        &semantic_file,
        concat!(
            r#"{"global_sequence":0,"request_sequence":0,"correlation_id":"req-hs41","timestamp_ns":1,"boundary":"http_incoming","trait_name":"RequestIdMiddleware","method_name":"call","call_file":"request_id.rs","call_line":10,"call_column":3,"request":{},"args":{},"response":{},"result":{},"is_error":false,"duration_us":4,"event_schema_version":7,"provenance":"recorded","recon":"lossless","replay_strategy":"substitute"}"#,
            "\n"
        ),
    )
    .expect("semantic file");
    fs::write(
        graph_dir.join(GRAPH_FILE_NAME),
        concat!(
            r#"{"node_id":7,"sequence":3,"span_name":"request","target":"router","level":"INFO","fields":{"request_id":"req-hs41"},"started_ns":1,"closed_ns":4001}"#,
            "\n"
        ),
    )
    .expect("graph file");

    for input in [&dir, &recording_dir, &semantic_file] {
        let loaded = load_artifacts(input).expect("load");
        let summary = summarize(&loaded);
        assert_eq!(loaded.semantic_events.len(), 1);
        assert_eq!(loaded.graph_records.len(), 1);
        assert_eq!(summary.request_counts, vec![("req-hs41".to_owned(), 1, 1)]);
    }

    fs::remove_dir_all(dir).expect("cleanup");
}

fn temp_artifact_dir(label: &str) -> std::path::PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or_default();
    std::env::temp_dir().join(format!(
        "deja-tui-integration-{label}-{}-{nanos}",
        std::process::id()
    ))
}
