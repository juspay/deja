//! `#[deja::boundary(neutral_error = ...)]` through the macro and the global
//! seam: a replayed `Execute` site whose recorded call failed with an error the
//! site declares state-neutral returns that recorded error instead of running,
//! and one that declares nothing runs as it always has.
//!
//! Why: the recorded run's insert failed and wrote no row. Running it again on
//! replay can succeed and write a row the recording never had, so every later
//! statement in that request runs against a different database. Serving the
//! recorded error writes nothing, as the recording did.
//!
//! Own test binary: `set_global_runtime_hook` is a one-shot `OnceLock`.
#![allow(unused_braces)]

use std::sync::atomic::{AtomicUsize, Ordering};

static BODY_RUNS: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum DbError {
    UniqueViolation,
    SerializationFailure,
}

/// The envelope shape deja's `ResultCodec` writes: a `result` discriminator,
/// and the error's kind. That discriminator is all deja reads.
struct EnvelopeCodec;

impl deja::codec::ReplayCodec for EnvelopeCodec {
    type Value = Result<u64, DbError>;

    fn capture(value: &Self::Value) -> (serde_json::Value, bool) {
        match value {
            Ok(v) => (
                serde_json::json!({ "version": 1, "result": "Ok", "value": v }),
                false,
            ),
            Err(e) => (
                serde_json::json!({ "version": 1, "result": "Err", "kind": e }),
                true,
            ),
        }
    }

    fn reconstruct(recorded: serde_json::Value) -> Option<Self::Value> {
        match recorded.get("result")?.as_str()? {
            "Ok" => Some(Ok(recorded.get("value")?.as_u64()?)),
            "Err" => Some(Err(
                serde_json::from_value(recorded.get("kind")?.clone()).ok()?
            )),
            _ => None,
        }
    }
}

fn is_unique(out: &Result<u64, DbError>) -> bool {
    matches!(out, Err(DbError::UniqueViolation))
}

#[deja::boundary(
    boundary = "db",
    component = "tests::macro_neutral_error",
    operation = "declared_insert",
    replay = Execute,
    codec = EnvelopeCodec,
    args = serde_json::json!({ "row": row }),
    neutral_error = is_unique,
)]
async fn declared_insert(row: &str) -> Result<u64, DbError> {
    let _ = row;
    BODY_RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(1)
}

#[deja::boundary(
    boundary = "db",
    component = "tests::macro_neutral_error",
    operation = "undeclared_insert",
    replay = Execute,
    codec = EnvelopeCodec,
    args = serde_json::json!({ "row": row }),
)]
async fn undeclared_insert(row: &str) -> Result<u64, DbError> {
    let _ = row;
    BODY_RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(1)
}

#[deja::boundary(
    boundary = "db",
    component = "tests::macro_neutral_error",
    operation = "declared_sync_insert",
    replay = Execute,
    codec = EnvelopeCodec,
    args = serde_json::json!({ "row": row }),
    neutral_error = is_unique,
)]
fn declared_sync_insert(row: &str) -> Result<u64, DbError> {
    let _ = row;
    BODY_RUNS.fetch_add(1, Ordering::SeqCst);
    Ok(1)
}

fn block_on<F: std::future::Future>(fut: F) -> F::Output {
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    let mut fut = std::pin::pin!(fut);
    loop {
        if let std::task::Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
            return v;
        }
        std::hint::spin_loop();
    }
}

/// This call's recorded row, at the address every call carries.
fn recorded(operation: &str, row: &str, result: serde_json::Value) -> deja::LookupEntry {
    deja::LookupEntry {
        key: deja::LookupKey {
            correlation_id: None,
            bucket_id: Some("root".to_owned()),
            fork_seq: 0,
            boundary: "db".to_owned(),
            component: "tests::macro_neutral_error".to_owned(),
            operation: operation.to_owned(),
            locus: deja::Locus::Unlocated,
            args_hash: deja::canonical_args_hash(&serde_json::json!({ "row": row })),
            occurrence: 0,
        },
        result: std::sync::Arc::new(result),
        source_event_global_sequence: 1,
    }
}

#[test]
fn a_declared_neutral_recorded_error_is_served_and_an_undeclared_one_runs() {
    let unique = serde_json::json!({ "version": 1, "result": "Err", "kind": "UniqueViolation" });
    let other =
        serde_json::json!({ "version": 1, "result": "Err", "kind": "SerializationFailure" });
    let table = deja::LookupTable {
        recording_id: "neutral-error-test".to_owned(),
        policy_version: deja::POLICY_VERSION,
        event_schema_version: Some(deja::CURRENT_EVENT_SCHEMA_VERSION),
        entries: vec![
            recorded("declared_insert", "a", unique.clone()),
            recorded("declared_insert", "b", other),
            recorded("undeclared_insert", "a", unique.clone()),
            recorded("declared_sync_insert", "a", unique),
        ],
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("lookup.json");
    std::fs::write(&path, serde_json::to_vec(&table).expect("serialize table"))
        .expect("write table");
    let sink = deja::InMemoryObservedSink::new();
    let calls = sink.handle();
    let hook = deja::LookupTableHook::from_source(deja::LocalFileLookupSource::new(path), sink)
        .expect("hook");
    deja::set_global_runtime_hook(Some(deja::RuntimeHook::LookupReplay(hook)))
        .expect("install runtime hook");

    // Declared, and the recorded error is the declared kind: served, not run.
    assert_eq!(
        block_on(declared_insert("a")),
        Err(DbError::UniqueViolation)
    );
    assert_eq!(
        BODY_RUNS.load(Ordering::SeqCst),
        0,
        "the boundary did not run"
    );

    // Declared, but the recorded error is another kind: run, as today.
    assert_eq!(block_on(declared_insert("b")), Ok(1));
    assert_eq!(BODY_RUNS.load(Ordering::SeqCst), 1);

    // Undeclared: the same recorded error runs, as today.
    assert_eq!(block_on(undeclared_insert("a")), Ok(1));
    assert_eq!(BODY_RUNS.load(Ordering::SeqCst), 2);

    // The sync seam serves as the async one does.
    assert_eq!(declared_sync_insert("a"), Err(DbError::UniqueViolation));
    assert_eq!(
        BODY_RUNS.load(Ordering::SeqCst),
        2,
        "the sync boundary did not run"
    );

    let provenance: Vec<_> = calls
        .lock()
        .expect("observed calls")
        .iter()
        .map(|c| (c.method_name.clone(), c.resolved, c.provenance))
        .collect();
    assert_eq!(
        provenance,
        vec![
            (
                "declared_insert".to_owned(),
                true,
                deja::Provenance::ServedRecordedError
            ),
            ("declared_insert".to_owned(), true, deja::Provenance::Shadow),
            (
                "undeclared_insert".to_owned(),
                true,
                deja::Provenance::Shadow
            ),
            (
                "declared_sync_insert".to_owned(),
                true,
                deja::Provenance::ServedRecordedError
            ),
        ],
        "each call's own recorded row was found, and only the served one says so"
    );
}
