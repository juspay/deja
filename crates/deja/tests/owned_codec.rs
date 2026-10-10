//! `owned_codec = ...`: a boundary whose return value can only be read by
//! consuming it. Records through the codec, then replays the recorded rows in a
//! child process (the runtime hook is one-shot per process) and checks the
//! caller reads what the recording read.
#![allow(unused_braces)]

use std::sync::atomic::{AtomicUsize, Ordering};

static CAPTURES: AtomicUsize = AtomicUsize::new(0);
static BODY_RUNS: AtomicUsize = AtomicUsize::new(0);

const TABLE_ENV: &str = "DEJA_OWNED_CODEC_TABLE";

/// Readable only by consuming it, like `reqwest::Response`. Comparable, so the
/// recorder can check the caller's copy against what replay would rebuild.
#[derive(PartialEq)]
pub struct Body(Result<String, String>);

impl Body {
    async fn text(self) -> Result<String, String> {
        self.0
    }
}

struct BodyCodec;

impl deja::codec::OwnedReplayCodec for BodyCodec {
    type Value = Body;
    type Read = String;

    async fn read(value: Body) -> (Body, Result<String, String>) {
        CAPTURES.fetch_add(1, Ordering::SeqCst);
        let read = value.text().await;
        (Body(read.clone()), read)
    }

    fn record(text: String, _: &Body) -> (serde_json::Value, bool) {
        (serde_json::json!({ "text": text }), false)
    }

    fn reconstruct(recorded: serde_json::Value) -> Option<Body> {
        let text = recorded.get("text")?.as_str()?;
        Some(Body(Ok(text.to_owned())))
    }
}

/// Reads like `BodyCodec`, then panics while building the tape JSON.
struct PanickingRecordCodec;

impl deja::codec::OwnedReplayCodec for PanickingRecordCodec {
    type Value = Body;
    type Read = String;

    async fn read(value: Body) -> (Body, Result<String, String>) {
        BodyCodec::read(value).await
    }

    fn record(_: String, _: &Body) -> (serde_json::Value, bool) {
        panic!("a codec bug while building the tape JSON")
    }

    fn reconstruct(recorded: serde_json::Value) -> Option<Body> {
        BodyCodec::reconstruct(recorded)
    }
}

#[deja::boundary(
    boundary = "http",
    component = "tests::owned_codec",
    operation = "fetch_panicking_record",
    site = "fetch_panicking_record",
    owned_codec = PanickingRecordCodec,
    args = serde_json::json!({ "path": path }),
)]
async fn fetch_panicking_record(path: &str) -> Body {
    Body(Ok(format!("body of {path}")))
}

#[deja::boundary(
    boundary = "http",
    component = "tests::owned_codec",
    operation = "fetch",
    site = "fetch",
    owned_codec = BodyCodec,
    args = serde_json::json!({ "path": path }),
)]
async fn fetch(path: &str) -> Body {
    BODY_RUNS.fetch_add(1, Ordering::SeqCst);
    Body(Ok(format!("body of {path}")))
}

#[deja::boundary(
    boundary = "http",
    component = "tests::owned_codec",
    operation = "fetch_reset",
    site = "fetch_reset",
    owned_codec = BodyCodec,
    args = serde_json::json!({ "path": path }),
)]
async fn fetch_reset(path: &str) -> Body {
    let _ = path;
    BODY_RUNS.fetch_add(1, Ordering::SeqCst);
    Body(Err("stream reset".to_owned()))
}

#[deja::boundary(
    boundary = "http",
    component = "tests::owned_codec",
    operation = "fetch_executed",
    site = "fetch_executed",
    replay = Execute,
    owned_codec = BodyCodec,
    args = serde_json::json!({ "path": path }),
)]
async fn fetch_executed(path: &str) -> Body {
    BODY_RUNS.fetch_add(1, Ordering::SeqCst);
    Body(Ok(format!("body of {path}")))
}

#[deja::boundary(
    boundary = "http",
    component = "tests::owned_codec",
    operation = "fetch_boxed",
    site = "fetch_boxed",
    owned_codec = BodyCodec,
    future = "boxed",
    args = serde_json::json!({ "path": path }),
)]
fn fetch_boxed(path: String) -> std::pin::Pin<Box<dyn std::future::Future<Output = Body> + Send>> {
    Box::pin(async move {
        BODY_RUNS.fetch_add(1, Ordering::SeqCst);
        Body(Ok(format!("body of {path}")))
    })
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

fn row(event: &deja_runtime::BoundaryEvent) -> deja::LookupEntry {
    let site = event
        .callsite_identity
        .as_ref()
        .and_then(|identity| identity.id.clone())
        .expect("every site here is declared");
    deja::LookupEntry {
        key: deja::LookupKey {
            correlation_id: None,
            bucket_id: Some("root".to_owned()),
            fork_seq: 0,
            boundary: event.boundary.clone(),
            component: event.trait_name.clone(),
            operation: event.method_name.clone(),
            locus: deja::Locus::DeclaredSite(site),
            args_hash: deja::canonical_args_hash(&event.args.to_value()),
            occurrence: 0,
        },
        result: std::sync::Arc::new(event.result.to_value()),
        source_event_global_sequence: event.global_sequence,
    }
}

#[test]
fn an_owned_codec_records_what_the_caller_reads_and_replays_it() {
    if std::env::var_os(TABLE_ENV).is_some() {
        return;
    }
    let artifacts = tempfile::tempdir().expect("tempdir");
    deja_runtime::set_global_runtime_hook(Some(deja_runtime::RuntimeHook::Recording(
        std::sync::Arc::new(
            deja_runtime::RecordingHook::new(artifacts.path()).expect("recording hook"),
        ),
    )))
    .expect("install recording hook");

    // No recording decision on this request: the value is never consumed.
    assert_eq!(
        block_on(block_on(fetch("/skipped")).text()).as_deref(),
        Ok("body of /skipped")
    );
    assert_eq!(
        CAPTURES.load(Ordering::SeqCst),
        0,
        "a sampled-out call is not captured"
    );

    {
        let _rec = deja_context::enter(
            deja_context::ContextSnapshot::new("req-owned-codec").with_recording_decision(true),
        );
        assert_eq!(
            block_on(block_on(fetch("/a")).text()).as_deref(),
            Ok("body of /a")
        );
        // A read that fails reaches the caller as the same failure.
        assert_eq!(
            block_on(block_on(fetch_reset("/r")).text()),
            Err("stream reset".to_owned())
        );
        assert_eq!(
            block_on(block_on(fetch_executed("/e")).text()).as_deref(),
            Ok("body of /e")
        );
        assert_eq!(
            block_on(block_on(fetch_boxed("/b".to_owned())).text()).as_deref(),
            Ok("body of /b")
        );
        // A codec that panics building the tape JSON costs the event, never
        // the caller's value.
        assert_eq!(
            block_on(block_on(fetch_panicking_record("/p")).text()).as_deref(),
            Ok("body of /p")
        );
    }
    assert_eq!(
        CAPTURES.load(Ordering::SeqCst),
        5,
        "every recorded call was read once"
    );

    deja_runtime::flush_global_hook().expect("flush events");
    let events = deja_runtime::read_events(artifacts.path()).expect("events");
    let seen: Vec<_> = events
        .iter()
        .map(|e| (e.method_name.as_str(), e.result.to_value(), e.is_error))
        .collect();
    assert_eq!(
        seen,
        vec![
            ("fetch", serde_json::json!({ "text": "body of /a" }), false),
            (
                "fetch_reset",
                serde_json::json!({ "captured": false, "reason": "stream reset" }),
                false
            ),
            (
                "fetch_executed",
                serde_json::json!({ "text": "body of /e" }),
                false
            ),
            (
                "fetch_boxed",
                serde_json::json!({ "text": "body of /b" }),
                false
            ),
        ],
    );
    assert_eq!(
        events[1].fidelity,
        deja::Fidelity::Opaque,
        "an unread value does not rebuild"
    );
    assert_eq!(
        events[0].fidelity,
        deja::Fidelity::Lossless,
        "the caller's rebuilt copy is what replay rebuilds"
    );

    let table = deja::LookupTable {
        recording_id: "owned-codec-test".to_owned(),
        policy_version: deja::POLICY_VERSION,
        event_schema_version: Some(deja::CURRENT_EVENT_SCHEMA_VERSION),
        entries: events.iter().map(row).collect(),
        identity_entries: Vec::new(),
    };
    let path = artifacts.path().join("lookup.json");
    std::fs::write(&path, serde_json::to_vec(&table).expect("serialize table"))
        .expect("write table");

    let replay = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "replay_the_recorded_rows",
            "--ignored",
            "--nocapture",
        ])
        .env(TABLE_ENV, &path)
        .output()
        .expect("run the replay half");
    let stdout = String::from_utf8_lossy(&replay.stdout);
    assert!(
        replay.status.success() && stdout.contains("1 passed"),
        "replay half failed:\n{stdout}\n{}",
        String::from_utf8_lossy(&replay.stderr)
    );
}

#[test]
#[ignore = "the replay half; run by the recording test with the table it wrote"]
fn replay_the_recorded_rows() {
    let path = std::env::var_os(TABLE_ENV).expect("run by the recording test");
    let sink = deja::InMemoryObservedSink::new();
    let calls = sink.handle();
    let hook = deja::LookupTableHook::from_source(deja::LocalFileLookupSource::new(path), sink)
        .expect("hook");
    deja::set_global_runtime_hook(Some(deja::RuntimeHook::LookupReplay(hook)))
        .expect("install runtime hook");

    assert_eq!(
        block_on(block_on(fetch("/a")).text()).as_deref(),
        Ok("body of /a")
    );
    assert_eq!(
        block_on(block_on(fetch_boxed("/b".to_owned())).text()).as_deref(),
        Ok("body of /b")
    );
    assert_eq!(
        BODY_RUNS.load(Ordering::SeqCst),
        0,
        "a substituted body is the recorded one"
    );

    // Execute runs the body and observes it through the same capture.
    assert_eq!(
        block_on(block_on(fetch_executed("/e")).text()).as_deref(),
        Ok("body of /e")
    );
    assert_eq!(BODY_RUNS.load(Ordering::SeqCst), 1);
    assert_eq!(
        CAPTURES.load(Ordering::SeqCst),
        1,
        "only the executed call is captured"
    );
    let executed = calls
        .lock()
        .expect("observed calls")
        .iter()
        .find(|c| c.method_name == "fetch_executed")
        .and_then(|c| c.observed_result.clone());
    assert_eq!(executed, Some(serde_json::json!({ "text": "body of /e" })));

    // A recorded read failure stops the request and says why.
    let stopped = std::panic::catch_unwind(|| block_on(fetch_reset("/r")))
        .err()
        .expect("an unread recording cannot be substituted");
    let message = stopped
        .downcast_ref::<String>()
        .cloned()
        .unwrap_or_default();
    assert!(
        message.contains("the recorded call could not read its value: stream reset"),
        "{message}"
    );
}
