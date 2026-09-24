#![allow(clippy::unwrap_used)] // tests panic on failure by design

//! A cache hit holding "does not exist" records, and replays, as a hit.
//!
//! A cache read returns `Option<T>`; when `T` is itself an `Option`, the hit
//! `Some(None)` and the miss `None` both serialise to `null`, and replay used to
//! hand back the miss. This drives both through the delegate macro's real
//! capture and reconstruct closures, record then replay.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use deja_runtime::{read_events, RecordingHook, ReplayHook};

#[deja_derive::recordable]
#[async_trait::async_trait]
pub trait ConfigCache {
    async fn cached(
        &self,
        key: String,
        scope: Option<Option<String>>,
    ) -> Result<Option<Option<String>>, String>;
}

#[derive(Clone)]
struct RealCache {
    calls: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl ConfigCache for RealCache {
    async fn cached(
        &self,
        key: String,
        _scope: Option<Option<String>>,
    ) -> Result<Option<Option<String>>, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(match key.as_str() {
            "known-absent" => Some(None),
            _ => None,
        })
    }
}

struct DejaCache {
    inner: Box<dyn ConfigCache + Send + Sync>,
    hook: Arc<dyn deja_runtime::DejaHook>,
}

delegate_config_cache_with_replay!(DejaCache, inner, hook, "imc");

#[tokio::test]
async fn a_cached_absence_replays_as_a_hit_and_a_miss_as_a_miss() {
    let _rec = deja_context::enter(
        deja_context::ContextSnapshot::new("req-present-none").with_recording_decision(true),
    );
    let record_dir = tempfile::tempdir().expect("tempdir");
    let record_hook = Arc::new(RecordingHook::new(record_dir.path()).expect("hook"));
    let store = DejaCache {
        inner: Box::new(RealCache {
            calls: Arc::new(AtomicUsize::new(0)),
        }),
        hook: record_hook.clone(),
    };
    assert_eq!(
        store.cached("known-absent".into(), Some(None)).await,
        Ok(Some(None))
    );
    assert_eq!(
        store.cached("never-seen".into(), Some(None)).await,
        Ok(None)
    );
    drop(store);
    drop(record_hook);

    let events = read_events(record_dir.path()).expect("read");
    assert_eq!(events.len(), 2, "both reads are on the tape");
    assert!(
        events
            .iter()
            .all(|event| event.fidelity == deja_runtime::Fidelity::Lossless),
        "the recorder rebuilt both values and found them unchanged"
    );
    assert_ne!(
        events[0].result, events[1].result,
        "the hit and the miss must not record as the same value"
    );
    assert!(
        events[0].result.get("Ok").is_some_and(|ok| !ok.is_null()),
        "the hit is recorded as a marked value, or the next assertion compares nothing"
    );
    assert_eq!(
        events[0].args.get("scope"),
        events[0].result.get("Ok"),
        "an argument holding a present None encodes as the result does"
    );

    let replay_calls = Arc::new(AtomicUsize::new(0));
    let replay_store = DejaCache {
        inner: Box::new(RealCache {
            calls: replay_calls.clone(),
        }),
        hook: Arc::new(ReplayHook::from_artifact_dir(record_dir.path()).expect("replay")),
    };
    assert_eq!(
        replay_store.cached("known-absent".into(), Some(None)).await,
        Ok(Some(None)),
        "the recorded hit must replay as a hit"
    );
    assert_eq!(
        replay_store.cached("never-seen".into(), Some(None)).await,
        Ok(None),
        "the recorded miss must replay as a miss"
    );
    assert_eq!(
        replay_calls.load(Ordering::SeqCst),
        0,
        "both answers come from the tape, not the real cache"
    );
}
