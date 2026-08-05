//! Lookup-table renderer — walks a recording and produces a `LookupTable` by
//! applying the current matching policy.
//!
//! The renderer and the candidate's `LookupTableHook` MUST construct keys
//! identically, or every lookup silently misses. That shared logic lives in
//! `deja-runtime` (`addresses_for`, `canonical_args_hash`, `KeyStamper`); this
//! renderer is just the recording-side driver that feeds it.
//!
//! For each non-`http_incoming` event the renderer emits ONE `LookupEntry` per
//! applicable address rank (explicit / logical span-path / syntactic /
//! lexical / location / sequence). The hook queries the ranks it can build strongest-first and takes
//! the first hit, so registering all ranks lets a single recording satisfy a
//! candidate however much call-site metadata it carries.

use std::collections::HashMap;
use std::io;

use deja::{addresses_for, canonical_args_hash, KeyStamper, LookupEntry, LookupTable};

use crate::scope::{ScopedRecording, TapeItem};

/// Walk a recording and produce a `LookupTable` for the run's scope.
///
/// Takes a [`ScopedRecording`], not a path: this function produces the
/// substitution table the replay candidate resolves against, so an unscoped
/// render puts every correlation in the recorded session — 42,310 of them on a
/// production tape — into a 5.95 MB artifact for a run that drives three. It
/// was benign only because the untouched correlations happened to contribute no
/// lookup material; that was luck, not design.
///
/// Scoping is safe for key identity: `KeyStamper`'s occurrence counter and the
/// per-request sequence are both keyed BY correlation, so omitting other
/// correlations' events does not shift the keys of the ones kept.
pub fn render_lookup_table(
    recording: &ScopedRecording,
    recording_id: &str,
    policy_version: u32,
) -> io::Result<LookupTable> {
    // Shared occurrence assigner — advanced for every rank on every event, in
    // lockstep with how the hook advances at replay.
    let mut stamper = KeyStamper::new();
    // Per-correlation sequence over the SAME event subset the hook sees (it
    // never looks up the kernel-driven `http_incoming` event), so the rank-6
    // `Address::Sequence` aligns instead of being offset by the incoming hop.
    let mut request_seq: HashMap<Option<String>, u64> = HashMap::new();
    let mut entries = Vec::new();
    let (mut dbg_ok, mut dbg_skip): (u64, u64) = (0, 0);
    let mut dbg_first_err: Option<String> = None;

    // Streams: `EntireSession` on a live recording is 171,234 events off a
    // 361 MB tape, so the renderer never holds the tape in memory.
    for item in recording.events()? {
        // Graph nodes and replay observations cohabit the stream but are never
        // lookup material, so the reader drops them WITHOUT counting them as
        // dropped boundary events. Malformed records still count against
        // coverage (the guard below).
        let event = match item {
            TapeItem::Event(event) => {
                dbg_ok += 1;
                *event
            }
            TapeItem::Malformed { error, excerpt, .. } => {
                dbg_skip += 1;
                if dbg_first_err.is_none() {
                    dbg_first_err = Some(format!("{error} :: {excerpt}"));
                }
                continue;
            }
        };

        // http_incoming is driven by the kernel, not resolved by the hook.
        if event.boundary == "http_incoming" {
            continue;
        }

        let seq_slot = request_seq.entry(event.correlation_id.clone()).or_insert(0);
        let request_sequence = *seq_slot;
        *seq_slot += 1;

        let args_hash = canonical_args_hash(&event.args);
        let location = Some((event.call_file.as_str(), event.call_line, event.call_column));
        let addresses = addresses_for(
            &event.boundary,
            &event.method_name,
            event.callsite_identity.as_ref(),
            location,
            request_sequence,
        );

        let bucket_id = event
            .bucket_id
            .as_deref()
            .or(event.task_bucket.as_deref())
            .unwrap_or("root");
        let fork_seq = event.fork_seq.unwrap_or(0);
        for key in stamper.stamp(
            event.correlation_id.as_deref(),
            Some(bucket_id),
            fork_seq,
            &addresses,
            args_hash,
        ) {
            entries.push(LookupEntry {
                key,
                result: event.result.clone(),
                source_event_global_sequence: event.global_sequence,
            });
        }
    }

    // Permanent guard: dropping unparseable events here silently mutilates the
    // lookup table (this exact path hid a Vector-stringified-u64 parse failure
    // that collapsed replay matching). A render that drops events is never a
    // clean run — surface it loudly, with the parsed/dropped ratio so the
    // magnitude is visible, so it can't masquerade as success.
    if dbg_skip > 0 {
        // Fail-closed: a dropped event is a hole in replay coverage, so the
        // resulting verdict would be silently incomplete — this exact path once
        // hid a Vector-stringified-u64 that discarded ~half a real recording. An
        // incomplete lookup table must never masquerade as a clean run, so this is
        // a hard error (not a warning); the message carries the parsed/dropped
        // ratio + the first parse error so the cause is immediately visible.
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "render dropped {dbg_skip} of {} recording event(s) from {} \
                 — replay coverage would be INCOMPLETE (first error: {:?})",
                dbg_ok + dbg_skip,
                recording.recording_id(),
                dbg_first_err
            ),
        ));
    }
    Ok(LookupTable {
        recording_id: recording_id.to_owned(),
        policy_version,
        entries,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;
    use std::io::Write;

    /// A recording on disk, opened at whole-session scope (what a render with
    /// no correlation filter has always meant).
    fn write_events(lines: &[serde_json::Value]) -> (tempfile::TempDir, ScopedRecording) {
        write_events_scoped(lines, crate::scope::RunScope::entire_session())
    }

    fn write_events_scoped(
        lines: &[serde_json::Value],
        scope: crate::scope::RunScope,
    ) -> (tempfile::TempDir, ScopedRecording) {
        let dir = tempfile::tempdir().unwrap();
        let root = crate::HarnessRoot::new(dir.path()).unwrap();
        let path = crate::scope::TapeSlot::for_write(&root, "rec-1");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{}", line).unwrap();
        }
        drop(f);
        let recording = ScopedRecording::open(&root, "rec-1", scope).unwrap();
        (dir, recording)
    }

    fn event(boundary: &str, seq: u64, identity: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "record_kind": "boundary_event",
            "global_sequence": seq,
            "request_sequence": seq,
            "correlation_id": "c-1",
            "timestamp_ns": 0,
            "recording_run_id": "r",
            "boundary": boundary,
            "trait_name": "T",
            "method_name": "m",
            "call_file": "x.rs",
            "call_line": 10,
            "call_column": 4,
            "request": null,
            "args": { "k": seq },
            "response": null,
            "result": "v",
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute",
            "callsite_identity": identity
        })
    }

    /// The lookup table IS the substitution material the candidate replays
    /// against, so it must carry the run's correlations and nothing else.
    /// Rendering it off the whole session put every recorded request's args and
    /// results into a run that drives three of them.
    #[test]
    fn renderer_emits_no_entry_for_a_correlation_outside_the_run_scope() {
        let mut driven = event("redis", 0, serde_json::Value::Null);
        driven["correlation_id"] = serde_json::json!("c-driven");
        driven["result"] = serde_json::json!("driven-value");
        let mut foreign = event("redis", 1, serde_json::Value::Null);
        foreign["correlation_id"] = serde_json::json!("c-foreign");
        foreign["result"] = serde_json::json!("FOREIGN_SECRET");

        let (_dir, recording) = write_events_scoped(
            &[driven, foreign],
            crate::scope::RunScope::from_filter(Some(&["c-driven".to_owned()])),
        );
        let table = render_lookup_table(&recording, "rec-1", 1).unwrap();
        assert!(
            table
                .entries
                .iter()
                .all(|e| e.key.correlation_id.as_deref() == Some("c-driven")),
            "out-of-scope correlations must not reach the substitution table: {:?}",
            table
                .entries
                .iter()
                .map(|e| e.key.correlation_id.clone())
                .collect::<Vec<_>>()
        );
        assert!(
            !serde_json::to_string(&table)
                .unwrap()
                .contains("FOREIGN_SECRET"),
            "an out-of-scope recorded value reached the lookup table"
        );
        assert!(!table.entries.is_empty(), "the driven case still renders");
    }

    #[test]
    fn renderer_skips_http_incoming_and_emits_one_entry_per_rank() {
        // http_incoming (skipped) + one redis event with no callsite identity,
        // so the redis event addresses at rank 5 (location) and rank 6 (sequence).
        let (_dir, recording) = write_events(&[
            event("http_incoming", 0, serde_json::Value::Null),
            event("redis", 1, serde_json::Value::Null),
        ]);

        let table = render_lookup_table(&recording, "rec-1", 1).unwrap();
        assert_eq!(
            table.entries.len(),
            2,
            "redis event yields rank-5 + rank-6 entries"
        );
        assert!(table
            .entries
            .iter()
            .all(|e| e.source_event_global_sequence == 1));
        let ranks: Vec<u8> = table.entries.iter().map(|e| e.key.address.rank()).collect();
        assert!(ranks.contains(&5) && ranks.contains(&6));
        assert!(
            table.entries.iter().any(|e| matches!(
                &e.key.address,
                deja::Address::Sequence { boundary, .. } if boundary == "redis"
            )),
            "rank-6 sequence address names the boundary"
        );
    }

    #[test]
    fn renderer_accepts_vector_stringified_u64_ids() {
        // The Kafka->Vector->S3 pipeline stringifies u64s > i64::MAX. A recording
        // whose boundary_event carries tracing_span_id / graph_node_id / value_digest
        // as JSON STRINGS must still parse — otherwise the event is dropped and
        // replay coverage silently collapses (this dropped ~48% of a real tape).
        let mut ev = event("redis", 1, serde_json::Value::Null);
        ev["tracing_span_id"] = serde_json::json!("9225624661302181899"); // > i64::MAX
        ev["graph_node_id"] = serde_json::json!("18000000000000000000"); // > i64::MAX
        ev["value_digest"] = serde_json::json!("12345678901234567890");
        let (_dir, recording) = write_events(&[ev]);
        // Must NOT drop the event -> render succeeds and yields entries.
        let table = render_lookup_table(&recording, "rec-1", 1).unwrap();
        assert!(
            !table.entries.is_empty(),
            "a stringified-u64 event must render, not drop"
        );
    }

    #[test]
    fn renderer_hard_fails_on_dropped_event() {
        // Fail-closed: a truly unparseable boundary record must FAIL the render,
        // never silently reduce coverage and pass as a clean run.
        let good = event("redis", 1, serde_json::Value::Null);
        let bad = serde_json::json!({
            "record_kind": "boundary_event",
            "global_sequence": "not-a-number"
        });
        let (_dir, recording) = write_events(&[good, bad]);
        let err = render_lookup_table(&recording, "rec-1", 1).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("INCOMPLETE"),
            "error names the coverage hole: {err}"
        );
    }

    #[test]
    fn renderer_emits_lexical_rank_when_identity_present() {
        // A redis event carrying a lexical path also gets a rank-3 entry.
        let identity = serde_json::json!({
            "version": 1,
            "source": "LexicalPath",
            "id": null,
            "scope": null,
            "occurrence": 0,
            "caller_function": null,
            "lexical_path": "crate::pay::confirm",
            "syntax_hash": null
        });
        let (_dir, recording) = write_events(&[event("redis", 0, identity)]);

        let table = render_lookup_table(&recording, "rec-1", 1).unwrap();
        let ranks: Vec<u8> = table.entries.iter().map(|e| e.key.address.rank()).collect();
        assert!(
            ranks.contains(&4),
            "lexical path yields a rank-4 entry: {ranks:?}"
        );
        assert!(ranks.contains(&5) && ranks.contains(&6));
    }

    #[test]
    fn renderer_scopes_same_callsite_occurrences_by_lineage_bucket() {
        let same_args = serde_json::json!({"payment_id": "pay_same"});
        let mut root = event("redis", 0, serde_json::Value::Null);
        root["args"] = same_args.clone();
        root["request"] = same_args.clone();
        root["task_id"] = serde_json::json!("root");
        root["task_bucket"] = serde_json::json!("root");
        root["bucket_id"] = serde_json::json!("root");
        root["fork_seq"] = serde_json::json!(0);

        let mut detached = event("redis", 1, serde_json::Value::Null);
        detached["args"] = same_args.clone();
        detached["request"] = same_args;
        detached["task_id"] = serde_json::json!("detached-1");
        detached["parent_task_id"] = serde_json::json!("root");
        detached["task_bucket"] = serde_json::json!("detached-bucket-1");
        detached["bucket_id"] = serde_json::json!("detached-bucket-1");
        detached["fork_seq"] = serde_json::json!(1);

        let (_dir, recording) = write_events(&[root, detached]);
        let table = render_lookup_table(&recording, "rec-1", 1).unwrap();
        let location_keys = table
            .entries
            .iter()
            .filter(|entry| entry.key.address.rank() == 5)
            .map(|entry| serde_json::to_value(&entry.key).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(location_keys.len(), 2, "one source-location key per event");
        assert_ne!(
            location_keys[0], location_keys[1],
            "same correlation/address/args keys in distinct task buckets must not collide"
        );
        assert!(
            location_keys
                .iter()
                .all(|key| key.get("occurrence") == Some(&serde_json::json!(0))),
            "each bucket starts its own occurrence sequence at zero: {location_keys:?}"
        );

        let has_lineage = |bucket: &str, fork_seq: u64| {
            location_keys.iter().any(|key| {
                key.get("bucket_id") == Some(&serde_json::json!(bucket))
                    && key.get("fork_seq") == Some(&serde_json::json!(fork_seq))
            })
        };
        assert!(
            has_lineage("root", 0),
            "root lookup key must serialize bucket_id=root with fork_seq=0: {location_keys:?}"
        );
        assert!(
            has_lineage("detached-bucket-1", 1),
            "detached lookup key must serialize its bucket with fork_seq=1: {location_keys:?}"
        );
    }
}
