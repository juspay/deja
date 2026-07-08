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
use std::path::Path;

use deja::{
    addresses_for, canonical_args_hash, BoundaryEvent, KeyStamper, LookupEntry, LookupTable,
};

/// Walk a recording (JSONL on disk) and produce a `LookupTable`.
pub fn render_lookup_table(
    recording_path: &Path,
    recording_id: &str,
    policy_version: u32,
) -> io::Result<LookupTable> {
    use std::io::{BufRead, BufReader};
    let file = std::fs::File::open(recording_path)?;
    let reader = BufReader::new(file);

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

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let event: BoundaryEvent = match serde_json::from_str(&line) {
            Ok(event) => {
                dbg_ok += 1;
                event
            }
            // Tolerate non-event lines (e.g. headers from a mixed stream); the
            // detector reports coverage so silent drops here don't masquerade
            // as a clean run.
            Err(e) => {
                dbg_skip += 1;
                if dbg_first_err.is_none() {
                    dbg_first_err = Some(format!(
                        "{e} :: {}",
                        &line.chars().take(200).collect::<String>()
                    ));
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
        eprintln!(
            "[deja] WARNING: render dropped {dbg_skip} of {} recording event(s) from {} \
             — replay coverage is INCOMPLETE (first error: {:?})",
            dbg_ok + dbg_skip,
            recording_path.display(),
            dbg_first_err
        );
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

    fn write_events(lines: &[serde_json::Value]) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        for line in lines {
            writeln!(f, "{}", line).unwrap();
        }
        drop(f);
        (dir, path)
    }

    fn event(boundary: &str, seq: u64, identity: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
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

    #[test]
    fn renderer_skips_http_incoming_and_emits_one_entry_per_rank() {
        // http_incoming (skipped) + one redis event with no callsite identity,
        // so the redis event addresses at rank 5 (location) and rank 6 (sequence).
        let (_dir, path) = write_events(&[
            event("http_incoming", 0, serde_json::Value::Null),
            event("redis", 1, serde_json::Value::Null),
        ]);

        let table = render_lookup_table(&path, "rec-1", 1).unwrap();
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
        let (_dir, path) = write_events(&[event("redis", 0, identity)]);

        let table = render_lookup_table(&path, "rec-1", 1).unwrap();
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

        let (_dir, path) = write_events(&[root, detached]);
        let table = render_lookup_table(&path, "rec-1", 1).unwrap();
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
