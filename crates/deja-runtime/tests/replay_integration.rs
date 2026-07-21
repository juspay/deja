#![allow(clippy::unwrap_used)] // tests panic on failure by design

//! Integration test: verify that `ReplayHook` intercepts calls and returns
//! recorded results instead of hitting the real implementation.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use deja_runtime::replay::{
    canonical_args_hash, Address, InMemoryObservedSink, LookupEntry, LookupKey, LookupTable,
    LookupTableHook, LookupTableSource,
};
use deja_runtime::{read_events, Provenance, RecordingHook, ReplayHook};

/// Recording is opt-in: enter a decision-only context so the record phase's
/// `RecordingHook` actually records. Only the record phase consults this; the
/// replay phase uses `ReplayHook`, which ignores the recording decision.
fn recording_enabled() -> deja_context::ContextGuard {
    deja_context::enter(deja_context::ContextSnapshot::empty().with_recording_decision(true))
}

// --- Define a trait ---

#[deja_derive::recordable]
#[async_trait::async_trait]
pub trait CounterService {
    async fn get_value(&self) -> Result<u64, String>;
    async fn increment(&self, delta: u64) -> Result<u64, String>;
    async fn tag(&self, name: String) -> Result<String, String>;
    async fn reset(&self) -> Result<(), String>;
}

// --- Real implementation that tracks invocations ---

#[derive(Clone)]
struct RealCounter {
    calls: Arc<AtomicUsize>,
}

impl RealCounter {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn call_count(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
impl CounterService for RealCounter {
    async fn get_value(&self) -> Result<u64, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(42)
    }

    async fn increment(&self, delta: u64) -> Result<u64, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(42 + delta)
    }

    async fn tag(&self, name: String) -> Result<String, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(format!("tag:{name}"))
    }

    async fn reset(&self) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

// --- Déjà wrapper ---

struct DejaCounter {
    inner: Box<dyn CounterService + Send + Sync>,
    hook: Arc<dyn deja_runtime::DejaHook>,
}

delegate_counter_service_with_replay!(DejaCounter, inner, hook, "service");

struct DejaCounterExecute {
    inner: Box<dyn CounterService + Send + Sync>,
    hook: Arc<dyn deja_runtime::DejaHook>,
}

delegate_counter_service_with_replay!(DejaCounterExecute, inner, hook, "service", replay = Execute);

struct StaticLookupSource(Option<LookupTable>);

impl LookupTableSource for StaticLookupSource {
    fn load(&mut self) -> std::io::Result<LookupTable> {
        self.0
            .take()
            .ok_or_else(|| std::io::Error::other("lookup table already consumed"))
    }
}

// --- Tests ---

#[tokio::test]
async fn replay_returns_recorded_value_without_calling_real_impl() {
    let _rec = recording_enabled();
    // Phase 1: Record.
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let real = RealCounter::new();
    let store = DejaCounter {
        inner: Box::new(real.clone()),
        hook: record_hook.clone(),
    };
    let v1 = store.get_value().await.unwrap();
    assert_eq!(v1, 42);
    drop(store);
    drop(record_hook);

    let events = read_events(record_dir.path()).expect("read");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].result["Ok"], 42);

    // Phase 2: Replay — real impl is behind the wrapper but never called.
    let replay_real = RealCounter::new();
    let replay_hook = Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay"));
    let replay_store = DejaCounter {
        inner: Box::new(replay_real.clone()),
        hook: replay_hook.clone(),
    };

    let v2 = replay_store.get_value().await.unwrap();
    assert_eq!(v2, 42); // recorded, not from real
    assert_eq!(
        replay_real.call_count(),
        0,
        "real impl should not be called"
    );
}

#[tokio::test]
async fn replay_sliding_window_recovers_skipped_calls() {
    let _rec = recording_enabled();
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let real = RealCounter::new();
    let store = DejaCounter {
        inner: Box::new(real.clone()),
        hook: record_hook.clone(),
    };
    let _ = store.increment(1).await.unwrap();
    let _ = store.increment(2).await.unwrap();
    let _ = store.get_value().await.unwrap();
    drop(store);
    drop(record_hook);

    // Replay but only call get_value — sliding window should skip the increments.
    let replay_real = RealCounter::new();
    let replay_hook = Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay"));
    let replay_store = DejaCounter {
        inner: Box::new(replay_real.clone()),
        hook: replay_hook.clone(),
    };

    let v = replay_store.get_value().await.unwrap();
    assert_eq!(v, 42);
    assert_eq!(replay_real.call_count(), 0);

    let report = replay_hook.take_report();
    assert!(report.has_divergences());
    assert_eq!(
        report.divergences[0].kind,
        deja_runtime::DivergenceKind::OmittedCall
    );
}

#[tokio::test]
async fn replay_logs_novel_call_and_fail_stops_before_real_impl() {
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let real = RealCounter::new();
    let store = DejaCounter {
        inner: Box::new(real.clone()),
        hook: record_hook.clone(),
    };
    let _ = store.get_value().await.unwrap();
    drop(store);
    drop(record_hook);

    // Replay but call reset (novel — not in recording). Substitute miss fail-stops
    // before the real implementation can mutate shared state.
    let replay_real = RealCounter::new();
    let replay_hook = Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay"));
    let replay_store = DejaCounter {
        inner: Box::new(replay_real.clone()),
        hook: replay_hook.clone(),
    };

    let join = tokio::spawn(async move { replay_store.reset().await });
    let err = join.await.expect_err("novel Substitute call must panic");
    assert!(err.is_panic());
    assert_eq!(
        replay_real.call_count(),
        0,
        "fail-stop must happen before the real impl is called"
    );

    let report = replay_hook.take_report();
    assert_eq!(report.divergences.len(), 1);
    assert_eq!(
        report.divergences[0].kind,
        deja_runtime::DivergenceKind::NovelCall
    );
}

#[tokio::test]
async fn replay_lookup_hit_with_unreconstructable_result_fail_stops_before_real_impl() {
    let args = serde_json::json!({});
    let table = LookupTable {
        recording_id: "malformed-substitute-hit".to_owned(),
        policy_version: 1,
        entries: vec![LookupEntry {
            key: LookupKey {
                correlation_id: None,
                bucket_id: Some("root".to_owned()),
                fork_seq: 0,
                address: Address::Sequence {
                    boundary: "service".to_owned(),
                    method: "get_value".to_owned(),
                    request_sequence: 0,
                },
                args_hash: canonical_args_hash(&args),
                occurrence: 0,
            },
            result: serde_json::json!("not-a-Result-u64-String"),
            source_event_global_sequence: 0,
        }],
    };
    let observed = InMemoryObservedSink::new();
    let observed_handle = observed.handle();
    let hook: Arc<dyn deja_runtime::DejaHook> = Arc::new(
        LookupTableHook::from_source(StaticLookupSource(Some(table)), observed)
            .expect("lookup-table hook"),
    );
    let real = RealCounter::new();
    let replay_store = DejaCounter {
        inner: Box::new(real.clone()),
        hook,
    };

    let join = tokio::spawn(async move { replay_store.get_value().await });
    let err = join
        .await
        .expect_err("unreconstructable Substitute hit must panic");
    assert!(err.is_panic());
    assert_eq!(
        real.call_count(),
        0,
        "malformed lookup hits must fail-stop before the real implementation is called"
    );

    let calls = observed_handle
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(calls.len(), 1, "lookup hit is observed before fail-stop");
    assert!(calls[0].resolved);
    assert_eq!(
        calls[0].recorded_result,
        Some(serde_json::json!("not-a-Result-u64-String"))
    );
}

#[tokio::test]
async fn replay_arg_mismatch_returns_recorded_result_anyway() {
    let _rec = recording_enabled();
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let real = RealCounter::new();
    let store = DejaCounter {
        inner: Box::new(real.clone()),
        hook: record_hook.clone(),
    };
    let v = store.increment(5).await.unwrap();
    assert_eq!(v, 47); // 42 + 5
    drop(store);
    drop(record_hook);

    // Replay with different args.
    let replay_real = RealCounter::new();
    let replay_hook = Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay"));
    let replay_store = DejaCounter {
        inner: Box::new(replay_real.clone()),
        hook: replay_hook.clone(),
    };

    // Arg mismatch but skip_arg_mismatch = true → return recorded result.
    let v = replay_store.increment(99).await.unwrap();
    assert_eq!(v, 47); // recorded result for increment(5)
    assert_eq!(replay_real.call_count(), 0);

    let report = replay_hook.take_report();
    assert_eq!(report.divergences.len(), 1);
    assert_eq!(
        report.divergences[0].kind,
        deja_runtime::DivergenceKind::ArgsDiverged
    );
}

#[tokio::test]
async fn replay_with_owned_arg_does_not_move_before_fallthrough() {
    let _rec = recording_enabled();
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let real = RealCounter::new();
    let store = DejaCounter {
        inner: Box::new(real.clone()),
        hook: record_hook.clone(),
    };
    let v = store.tag("alpha".to_string()).await.unwrap();
    assert_eq!(v, "tag:alpha");
    drop(store);
    drop(record_hook);

    let replay_real = RealCounter::new();
    let replay_hook = Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay"));
    let replay_store = DejaCounter {
        inner: Box::new(replay_real.clone()),
        hook: replay_hook,
    };

    let v = replay_store.tag("alpha".to_string()).await.unwrap();
    assert_eq!(v, "tag:alpha");
    assert_eq!(replay_real.call_count(), 0);
}

#[tokio::test]
async fn replay_execute_delegate_runs_real_impl_and_emits_shadow_observation() {
    let args = serde_json::json!({});
    let table = LookupTable {
        recording_id: "execute-delegate-recording".to_owned(),
        policy_version: 1,
        entries: vec![LookupEntry {
            key: LookupKey {
                correlation_id: None,
                bucket_id: Some("root".to_owned()),
                fork_seq: 0,
                address: Address::Sequence {
                    boundary: "service".to_owned(),
                    method: "get_value".to_owned(),
                    request_sequence: 0,
                },
                args_hash: canonical_args_hash(&args),
                occurrence: 0,
            },
            result: serde_json::json!({"Ok": 42}),
            source_event_global_sequence: 0,
        }],
    };
    let observed = InMemoryObservedSink::new();
    let observed_handle = observed.handle();
    let hook: Arc<dyn deja_runtime::DejaHook> = Arc::new(
        LookupTableHook::from_source(StaticLookupSource(Some(table)), observed)
            .expect("lookup-table hook"),
    );
    let real = RealCounter::new();
    let replay_store = DejaCounterExecute {
        inner: Box::new(real.clone()),
        hook,
    };

    let value = replay_store.get_value().await.unwrap();

    assert_eq!(value, 42);
    assert_eq!(
        real.call_count(),
        1,
        "Execute delegate replay must run the real implementation"
    );
    let calls = observed_handle
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    assert_eq!(calls.len(), 1, "one shadow observation must be emitted");
    let call = &calls[0];
    assert_eq!(call.provenance, Provenance::Shadow);
    assert_eq!(call.boundary, "service");
    assert_eq!(call.trait_name, "CounterService");
    assert_eq!(call.method_name, "get_value");
    assert_eq!(call.recorded_result, Some(serde_json::json!({"Ok": 42})));
    assert_eq!(call.observed_result, Some(serde_json::json!({"Ok": 42})));
}
