//! The class fix.
//!
//! A replay run's correlation scope has to reach the lookup-table renderer, the
//! seed planner, the kernel (across a process boundary, as an env var), the
//! record-graph extractor, the scorer and the ledger. Before `scope.rs` it
//! reached them by four unrelated mechanisms and any new function that took a
//! `&Path` skipped all four. Three already had.
//!
//! No Rust type can express the property that matters, because part of the
//! scope's journey is an env var and an S3 ingest. So it is asserted here
//! instead, over the artifacts a run actually produces:
//!
//!   for every artifact a replay run produces, every correlation-bearing record
//!   is in the run's filter, and every record-graph node is reachable from an
//!   in-scope anchor.
//!
//! Plus a source-level assertion that the raw tape path has exactly one home.
//! Together these catch the next extractor the day it lands, rather than the
//! next time someone reads a 29 MB `/graph` payload.

use std::collections::{BTreeSet, HashMap, HashSet};

use deja_orchestrator::scope::{RunScope, ScopedRecording, TapeSlot};
use deja_orchestrator::{
    divergence, lookup, CandidateSpec, HarnessRoot, Run, RunMode, RunSpec, RunStatus,
};

const DRIVEN: [&str; 2] = ["c-driven-1", "c-driven-2"];
const FOREIGN: [&str; 3] = ["c-health-1", "c-health-2", "c-other-tenant"];

/// A tape in the production shape: a handful of driven requests interleaved
/// with the many single-hop health checks and other tenants' traffic that share
/// the session.
fn write_session_tape(root: &HarnessRoot, recording_id: &str) {
    let mut lines: Vec<String> = Vec::new();
    let mut node_id = 1u64;
    let mut seq = 1u64;
    let mut ambient_written = false;

    for correlation in DRIVEN.iter().chain(FOREIGN.iter()) {
        let root_node = node_id;
        node_id += 1;
        let child_node = node_id;
        node_id += 1;
        lines.push(graph_node(root_node, None, "ROOT_SPAN", correlation));
        lines.push(graph_node(
            child_node,
            Some(root_node),
            "charge",
            correlation,
        ));
        lines.push(boundary_event(seq, Some(correlation), Some(child_node)));
        seq += 1;
        if !ambient_written {
            // Uncorrelated background traffic: shared across cases, tolerated by
            // the scorer, and never attributed to any one case.
            lines.push(boundary_event(seq, None, None));
            seq += 1;
            ambient_written = true;
        }
    }

    let path = TapeSlot::for_write(root, recording_id);
    std::fs::create_dir_all(path.parent().expect("tape has a parent")).expect("mkdir");
    std::fs::write(&path, lines.join("\n") + "\n").expect("write tape");
}

fn graph_node(id: u64, parent: Option<u64>, name: &str, correlation: &str) -> String {
    graph_node_stating(id, parent, name, correlation, Some(correlation))
}

/// A node whose stated correlation is controlled separately from the one its
/// log field mentions, so a tape can be built the way older ones actually are:
/// nodes that belong to a case without saying so.
fn graph_node_stating(
    id: u64,
    parent: Option<u64>,
    name: &str,
    correlation: &str,
    states: Option<&str>,
) -> String {
    serde_json::json!({
        "record_kind": "graph_node",
        "node_id": id,
        "global_sequence": id,
        "parent_id": parent,
        "sequence": id,
        "correlation_id": states,
        "span_name": name,
        "target": "router",
        "level": "INFO",
        // Real ROOT_SPANs carry a golden_log_line; this is the field-value
        // exposure the unscoped extractor was publishing.
        "fields": { "golden_log_line": format!("REQUEST {correlation}") },
        "started_ns": id,
    })
    .to_string()
}

fn boundary_event(seq: u64, correlation: Option<&str>, graph_node_id: Option<u64>) -> String {
    serde_json::json!({
        "record_kind": "boundary_event",
        "global_sequence": seq,
        "request_sequence": 0,
        "correlation_id": correlation,
        "timestamp_ns": seq,
        "recording_run_id": "rec",
        "boundary": "redis",
        "trait_name": "RedisConnection",
        "method_name": "get_key",
        "call_file": "crates/router/src/x.rs",
        "call_line": 10,
        "call_column": 4,
        "request": null,
        "args": { "key": format!("k-{}", correlation.unwrap_or("ambient")) },
        "response": null,
        "result": format!("VALUE-FOR-{}", correlation.unwrap_or("ambient")),
        "is_error": false,
        "duration_us": 1,
        "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
        "provenance": "recorded",
        "recon": "lossless",
        "replay_strategy": "substitute",
        "graph_node_id": graph_node_id,
    })
    .to_string()
}

fn write_run(root: &HarnessRoot, run_id: &str, recording_id: &str, filter: Option<Vec<String>>) {
    let run = Run {
        run_id: run_id.to_owned(),
        spec: RunSpec {
            mode: RunMode::Replay,
            system_under_test: None,
            candidate_spec: CandidateSpec::PrebuiltImage {
                image: "deja-demo".to_owned(),
            },
            candidate_repo: None,
            recording_id: Some(recording_id.to_owned()),
            s3_source: None,
            correlation_filter: filter,
            workload: serde_json::Value::Null,
        },
        status: RunStatus::Completed,
        recording_id: Some(recording_id.to_owned()),
        candidate_image: None,
        failure_reason: None,
        stage: None,
        step: 0,
        steps_total: 0,
        stage_updated_ms: 0,
    };
    deja_orchestrator::write_json(&root.run_path(run_id), &run).expect("write run");
}

/// Every `correlation_id` anywhere in a JSON value, at any depth. The invariant
/// is about the artifact as published, not about the one field a particular
/// reader happens to look at.
fn correlation_ids(value: &serde_json::Value, out: &mut BTreeSet<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (k, v) in map {
                if k == "correlation_id" {
                    if let Some(s) = v.as_str() {
                        out.insert(s.to_owned());
                    }
                }
                correlation_ids(v, out);
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                correlation_ids(item, out);
            }
        }
        _ => {}
    }
}

/// THE invariant. Produce the artifacts a replay run produces, then assert that
/// nothing in any of them belongs to a correlation the run did not drive.
#[test]
fn every_artifact_a_scoped_run_produces_stays_inside_its_scope() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = HarnessRoot::new(dir.path()).expect("harness root");
    let run_id = "run-scope-invariant";
    let recording_id = "rec-scope-invariant";
    let filter: Vec<String> = DRIVEN.iter().map(|s| (*s).to_owned()).collect();

    write_session_tape(&root, recording_id);
    write_run(&root, run_id, recording_id, Some(filter.clone()));

    let run: Run = deja_orchestrator::read_json(&root.run_path(run_id)).expect("read run");
    let scope = RunScope::of(&run);
    let recording =
        ScopedRecording::open(&root, recording_id, scope.clone()).expect("open recording");

    // 1. The lookup table: the substitution material the candidate replays
    //    against, and the artifact with the worst blast radius of the three.
    let table = lookup::render_lookup_table(&recording, recording_id, 1).expect("render");
    deja_orchestrator::write_json(&root.lookup_table_path(run_id), &table).expect("write table");

    // 2. The record graph: published to S3 and served by an unauthenticated
    //    endpoint.
    let nodes = recording.graph_nodes().expect("scoped record graph");

    // 3. The scorecard and the per-call ledger.
    let card = divergence::detect_and_score(&root, run_id).expect("score");
    let ledger = divergence::call_ledger(&root, run_id).expect("ledger");

    let in_scope: BTreeSet<String> = filter.iter().cloned().collect();
    let artifacts: Vec<(&str, serde_json::Value)> = vec![
        (
            "lookup_table",
            serde_json::to_value(&table).expect("table json"),
        ),
        (
            "record_graph",
            serde_json::to_value(&nodes).expect("nodes json"),
        ),
        ("scorecard", serde_json::to_value(&card).expect("card json")),
        (
            "call_ledger",
            serde_json::to_value(&ledger).expect("ledger json"),
        ),
    ];

    for (name, value) in &artifacts {
        let mut seen = BTreeSet::new();
        correlation_ids(value, &mut seen);
        let leaked: Vec<&String> = seen.difference(&in_scope).collect();
        assert!(
            leaked.is_empty(),
            "artifact `{name}` carries correlations this run never drove: {leaked:?}"
        );
    }

    // The recorded VALUES of foreign cases must not ride along either — a
    // correlation can leak through a payload without its id being on the row.
    let dump = serde_json::to_string(&artifacts).expect("dump");
    for foreign in FOREIGN {
        assert!(
            !dump.contains(&format!("VALUE-FOR-{foreign}")),
            "a foreign correlation's recorded value reached a published artifact: {foreign}"
        );
        assert!(
            !dump.contains(&format!("REQUEST {foreign}")),
            "a foreign request's span fields reached the record graph: {foreign}"
        );
    }

    // Every graph node published must be reachable from an in-scope anchor.
    let anchors: HashSet<u64> = recording
        .events()
        .expect("events")
        .filter_map(|item| match item {
            deja_orchestrator::scope::TapeItem::Event(e) => e.graph_node_id,
            deja_orchestrator::scope::TapeItem::Malformed { .. } => None,
        })
        .collect();
    assert!(!anchors.is_empty(), "the fixture must anchor its cases");
    let parent_of: HashMap<u64, Option<u64>> =
        nodes.iter().map(|n| (n.node_id, n.parent_id)).collect();
    let mut reachable: HashSet<u64> = HashSet::new();
    // Down from each anchor's root, using only published nodes.
    let mut roots: HashSet<u64> = HashSet::new();
    for anchor in &anchors {
        let mut cur = *anchor;
        let mut guard = 0;
        while let Some(Some(parent)) = parent_of.get(&cur) {
            cur = *parent;
            guard += 1;
            assert!(guard < 1_000, "parent cycle in the published graph");
        }
        roots.insert(cur);
    }
    for id in parent_of.keys() {
        let mut cur = *id;
        while let Some(Some(parent)) = parent_of.get(&cur) {
            cur = *parent;
        }
        if roots.contains(&cur) {
            reachable.insert(*id);
        }
    }
    let orphans: Vec<&u64> = parent_of
        .keys()
        .filter(|id| !reachable.contains(id))
        .collect();
    assert!(
        orphans.is_empty(),
        "published graph nodes unreachable from any in-scope anchor: {orphans:?}"
    );
    assert!(
        !nodes.is_empty(),
        "an EMPTY record graph would satisfy every assertion above and is strictly \
         worse than the bug — it reads as `this run had no cascade`"
    );
    assert_eq!(
        nodes.len(),
        DRIVEN.len() * 2,
        "exactly the driven requests' span trees"
    );
}

/// The other half of the constraint: a run WITHOUT a filter still covers the
/// whole session. Record mode has no filter and the local demo relies on
/// "no filter = everything", so the containment fix must not have quietly
/// turned absence into emptiness.
#[test]
fn a_run_without_a_filter_still_covers_the_entire_session() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = HarnessRoot::new(dir.path()).expect("harness root");
    let run_id = "run-entire-session";
    let recording_id = "rec-entire-session";

    write_session_tape(&root, recording_id);
    write_run(&root, run_id, recording_id, None);

    let run: Run = deja_orchestrator::read_json(&root.run_path(run_id)).expect("read run");
    let scope = RunScope::of(&run);
    assert!(scope.is_entire_session());

    let recording = ScopedRecording::open(&root, recording_id, scope).expect("open recording");
    let table = lookup::render_lookup_table(&recording, recording_id, 1).expect("render");
    let seen: BTreeSet<Option<String>> = table
        .entries
        .iter()
        .map(|e| e.key.correlation_id.clone())
        .collect();
    for correlation in DRIVEN.iter().chain(FOREIGN.iter()) {
        assert!(
            seen.contains(&Some((*correlation).to_owned())),
            "an unfiltered run must render every recorded correlation, missing {correlation}"
        );
    }
    assert_eq!(
        recording.graph_nodes().expect("graph").len(),
        (DRIVEN.len() + FOREIGN.len()) * 2,
        "an unfiltered run publishes the whole graph"
    );
}

/// The scope crosses a process boundary as `KERNEL_CORRELATION_FILTER`, which no
/// Rust type reaches. The one door that emits the tape path emits the filter
/// with it, so a caller cannot point the kernel at a whole session by forgetting
/// a second `env()` call.
#[test]
fn the_kernel_never_gets_a_tape_path_without_its_filter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = HarnessRoot::new(dir.path()).expect("harness root");
    let scope = RunScope::from_filter(Some(&[
        "c-driven-2".to_owned(),
        "c-driven-1".to_owned(),
        " ".to_owned(),
    ]));
    let env = TapeSlot::subprocess(&root, "rec", &scope);
    let map: HashMap<&str, &str> = env.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
    assert!(map.contains_key("KERNEL_RECORDING_PATH"));
    assert_eq!(
        map.get("KERNEL_CORRELATION_FILTER").copied(),
        Some("c-driven-1,c-driven-2"),
        "sorted, deduped, blank ids dropped — the same normalization the scorer applies"
    );
    assert_eq!(
        TapeSlot::subprocess(&root, "rec", &RunScope::entire_session()).len(),
        1,
        "EntireSession sets no filter var; the kernel reads its absence as drive-everything"
    );
}

/// The CI grep. `HarnessRoot::recording_events_path` was a `pub fn` with 18 call
/// sites and no scope of its own; three readers were written against it with no
/// filter at all. It now has exactly one home, and this fails the build the day
/// a second one appears — including in a test, which is where the next one will
/// look most harmless.
#[test]
fn the_raw_tape_path_has_exactly_one_home() {
    let src = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut offenders: Vec<String> = Vec::new();
    let mut found_in_scope_rs = 0usize;

    let mut stack = vec![src.clone()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).expect("read_dir") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let body = std::fs::read_to_string(&path).expect("read source");
            let hits = body.matches("recording_events_path").count();
            if hits == 0 {
                continue;
            }
            if path.file_name().and_then(|f| f.to_str()) == Some("scope.rs") {
                found_in_scope_rs += hits;
            } else {
                offenders.push(format!("{} ({hits} hit(s))", path.display()));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "the recording tape's raw path escaped scope.rs — read recordings through \
         `scope::ScopedRecording`, and use `scope::TapeSlot` for ingest / existence / \
         subprocess. Offenders: {offenders:?}"
    );
    assert!(
        found_in_scope_rs > 0,
        "scope.rs no longer defines the tape path — this guard is now vacuous"
    );
}

// ---------------------------------------------------------------------------
// A node that states its own case can anchor it, with no join.
//
// Scoping a graph used to be possible only through a boundary event that named
// a node: correlation -> event.graph_node_id -> node.parent_id* -> root. That
// join has three independent ways to fail and one message for all of them. A
// node that states its correlation anchors its case directly, so a case is
// reachable if EITHER side of the join holds.
// ---------------------------------------------------------------------------

/// A tape whose events name no graph node at all — the shape that used to be
/// unscopable — but whose nodes state which case they belong to.
fn write_tape_anchored_only_by_nodes(root: &HarnessRoot, recording_id: &str) {
    let mut lines: Vec<String> = Vec::new();
    let mut node_id = 1u64;
    for (seq, correlation) in (1u64..).zip(DRIVEN.iter()) {
        let root_node = node_id;
        node_id += 1;
        let child_node = node_id;
        node_id += 1;
        lines.push(graph_node(root_node, None, "ROOT_SPAN", correlation));
        lines.push(graph_node(
            child_node,
            Some(root_node),
            "charge",
            correlation,
        ));
        // No `graph_node_id`: the only tie to the graph is the node's own claim.
        lines.push(boundary_event(seq, Some(correlation), None));
    }
    let path = TapeSlot::for_write(root, recording_id);
    std::fs::create_dir_all(path.parent().expect("tape has a parent")).expect("mkdir");
    std::fs::write(&path, lines.join("\n") + "\n").expect("write tape");
}

fn scoped(root: &HarnessRoot, recording_id: &str, run_id: &str) -> ScopedRecording {
    write_run(
        root,
        run_id,
        recording_id,
        Some(DRIVEN.map(str::to_owned).to_vec()),
    );
    let run: Run = deja_orchestrator::read_json(&root.run_path(run_id)).expect("read run");
    ScopedRecording::open(root, recording_id, RunScope::of(&run)).expect("open recording")
}

#[test]
fn a_case_is_reachable_when_only_its_nodes_name_it() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = HarnessRoot::new(tmp.path()).expect("harness root");
    write_tape_anchored_only_by_nodes(&root, "rec-node-anchored");
    let recording = scoped(&root, "rec-node-anchored", "run-node-anchored");

    let nodes = recording
        .graph_nodes()
        .expect("nodes that state their case anchor it without an event naming them");
    assert!(
        !nodes.is_empty(),
        "the scoped graph must carry the spans of a case its nodes claimed",
    );
    for node in &nodes {
        let stated = node.correlation_id.as_deref();
        assert!(
            stated.is_none() || DRIVEN.contains(&stated.unwrap_or_default()),
            "a scoped graph must not carry another case's spans, got {stated:?}",
        );
    }
}

#[test]
fn a_case_naming_nothing_is_refused_as_a_capture_gap() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = HarnessRoot::new(tmp.path()).expect("harness root");
    // One case anchors normally, so the whole-tape "nothing anchors anywhere"
    // guard does not fire and the PER-CASE check is what refuses. The other has
    // a node that claims nothing and an event that names nothing: tied to the
    // graph by neither side.
    let lines: Vec<String> = vec![
        graph_node(1, None, "ROOT_SPAN", DRIVEN[1]),
        boundary_event(1, Some(DRIVEN[1]), Some(1)),
        graph_node_stating(2, None, "ROOT_SPAN", DRIVEN[0], None),
        boundary_event(2, Some(DRIVEN[0]), None),
    ];
    let path = TapeSlot::for_write(&root, "rec-capture-gap");
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    std::fs::write(&path, lines.join("\n") + "\n").expect("write tape");

    let recording = scoped(&root, "rec-capture-gap", "run-capture-gap");
    let err = recording
        .graph_nodes()
        .expect_err("a case tied to the graph by nothing must be refused, not silently dropped");
    let msg = err.to_string();
    assert!(
        msg.contains("name NO execution-graph node") && msg.contains("capture gap"),
        "the refusal must say nothing tied the case to a span, so the reader looks \
         at capture rather than delivery: {msg}",
    );
}

#[test]
fn a_case_naming_absent_nodes_is_refused_as_a_delivery_gap() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = HarnessRoot::new(tmp.path()).expect("harness root");
    // The tie exists — the event names node 99 — but no such node is on the
    // tape. That is delivery losing a record, not capture never making one.
    let lines: Vec<String> = vec![
        graph_node(1, None, "ROOT_SPAN", DRIVEN[1]),
        boundary_event(1, Some(DRIVEN[1]), Some(1)),
        boundary_event(2, Some(DRIVEN[0]), Some(99)),
    ];
    let path = TapeSlot::for_write(&root, "rec-delivery-gap");
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    std::fs::write(&path, lines.join("\n") + "\n").expect("write tape");

    let recording = scoped(&root, "rec-delivery-gap", "run-delivery-gap");
    let err = recording
        .graph_nodes()
        .expect_err("a case naming a node the tape does not carry must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("the tape does not carry") && msg.contains("delivery gap"),
        "the refusal must distinguish a missing node from a missing tie: {msg}",
    );
    assert!(
        msg.contains(DRIVEN[0]) && !msg.contains(&format!("({}", DRIVEN[1])),
        "only the case that failed should be named: {msg}",
    );
}
