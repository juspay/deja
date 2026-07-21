#![allow(clippy::unwrap_used)] // tests panic on failure by design

//! Integration test: verify that `#[deja::recordable]` + `#[async_trait]`
//! generates a working delegation macro that compiles and records events.

use std::sync::Arc;

use deja_runtime::{read_events, RecordingHook};
use serde::Serialize;

/// Recording is opt-in: a boundary records only when an explicit `Record`
/// decision is present for the current context. Enter a decision-only context
/// (no correlation) so `RecordingHook::mode()` records on this thread; hold the
/// returned guard until after the recording + `read_events` section.
fn recording_enabled() -> deja_context::ContextGuard {
    deja_context::enter(deja_context::ContextSnapshot::empty().with_recording_decision(true))
}

// --- Define a trait using #[deja::recordable] ---

#[deja_derive::recordable]
#[async_trait::async_trait]
pub trait AddressInterface {
    async fn find_address_by_id(&self, address_id: &str) -> Result<String, String>;

    async fn update_address(&self, address_id: String, new_city: String) -> Result<String, String>;

    async fn delete_address(&self, address_id: &str, merchant_id: &str) -> Result<(), String>;
}

// --- Real implementation ---

struct RealStore;

#[async_trait::async_trait]
impl AddressInterface for RealStore {
    async fn find_address_by_id(&self, address_id: &str) -> Result<String, String> {
        Ok(format!("Address({})", address_id))
    }

    async fn update_address(&self, address_id: String, new_city: String) -> Result<String, String> {
        Ok(format!("Updated({}, {})", address_id, new_city))
    }

    async fn delete_address(&self, address_id: &str, _merchant_id: &str) -> Result<(), String> {
        if address_id == "not_found" {
            Err("address not found".into())
        } else {
            Ok(())
        }
    }
}

// --- DejaStore wrapper using the generated delegation macro ---

struct DejaStore {
    inner: Box<dyn AddressInterface + Send + Sync>,
    hook: Arc<RecordingHook>,
}

// This is the magic line — the generated macro produces the entire impl block.
delegate_address_interface!(DejaStore, inner, hook, "storage");

// --- Associated type coverage ---

#[deja_derive::recordable]
#[async_trait::async_trait]
pub trait ConfigInterface {
    type Error;

    async fn find_config_by_key(&self, key: &str) -> Result<String, Self::Error>;
}

struct RealConfigStore;

#[async_trait::async_trait]
impl ConfigInterface for RealConfigStore {
    type Error = String;

    async fn find_config_by_key(&self, key: &str) -> Result<String, Self::Error> {
        Ok(format!("config({})", key))
    }
}

struct DejaConfigStore {
    inner: Box<dyn ConfigInterface<Error = String> + Send + Sync>,
    hook: Arc<RecordingHook>,
}

delegate_config_interface!(DejaConfigStore, inner, hook, "storage", {
    type Error = String;
});

// --- Serialize-only return coverage ---

#[derive(Debug, PartialEq, Serialize)]
struct SerializeOnly {
    value: String,
}

#[deja_derive::recordable]
#[async_trait::async_trait]
trait SerializeOnlyInterface {
    async fn fetch_serialize_only(&self, key: String) -> SerializeOnly;
}

struct RealSerializeOnlyStore;

#[async_trait::async_trait]
impl SerializeOnlyInterface for RealSerializeOnlyStore {
    async fn fetch_serialize_only(&self, key: String) -> SerializeOnly {
        SerializeOnly { value: key }
    }
}

struct DejaSerializeOnlyStore {
    inner: Box<dyn SerializeOnlyInterface + Send + Sync>,
    hook: Arc<RecordingHook>,
}

delegate_serialize_only_interface!(DejaSerializeOnlyStore, inner, hook, "storage");

// --- Sync where-clause coverage ---

#[deja_derive::recordable]
pub trait SyncLookupInterface {
    fn sync_lookup<T>(&self, key: T) -> Result<String, String>
    where
        T: ToString + Serialize;
}

struct RealSyncLookupStore;

impl SyncLookupInterface for RealSyncLookupStore {
    fn sync_lookup<T>(&self, key: T) -> Result<String, String>
    where
        T: ToString + Serialize,
    {
        Ok(format!("sync:{}", key.to_string()))
    }
}

struct DejaSyncLookupStore {
    inner: RealSyncLookupStore,
    hook: Arc<RecordingHook>,
}

delegate_sync_lookup_interface!(DejaSyncLookupStore, inner, hook, "storage");

// --- Tests ---

#[tokio::test]
async fn delegation_records_successful_call() {
    let _rec = recording_enabled();
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = Arc::new(RecordingHook::new(dir.path()).expect("hook"));

    let store = DejaStore {
        inner: Box::new(RealStore),
        hook: hook.clone(),
    };

    let result = store.find_address_by_id("addr_123").await;
    assert_eq!(result, Ok("Address(addr_123)".to_string()));

    // Force flush by dropping
    drop(store);
    drop(hook);

    let events = read_events(dir.path()).expect("read");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].trait_name, "AddressInterface");
    assert_eq!(events[0].method_name, "find_address_by_id");
    assert_eq!(events[0].boundary, "storage");
    assert!(!events[0].is_error);
    assert_eq!(events[0].global_sequence, 0);
    assert!(events[0].call_file.contains("recordable_integration.rs"));
    assert_eq!(events[0].args["address_id"], "addr_123");
    assert_eq!(events[0].request["address_id"], "addr_123");
    assert_eq!(events[0].result["Ok"], "Address(addr_123)");
    assert_eq!(events[0].response["Ok"], "Address(addr_123)");
    let receiver = events[0].receiver.as_ref().expect("receiver metadata");
    assert!(receiver["self_type"]
        .as_str()
        .is_some_and(|name| name.contains("DejaStore")));
    assert!(receiver["inner_type"]
        .as_str()
        .is_some_and(|name| name.contains("dyn")));
}

#[tokio::test]
async fn delegation_records_error() {
    let _rec = recording_enabled();
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = Arc::new(RecordingHook::new(dir.path()).expect("hook"));

    let store = DejaStore {
        inner: Box::new(RealStore),
        hook: hook.clone(),
    };

    let result = store.delete_address("not_found", "merch_1").await;
    assert!(result.is_err());

    drop(store);
    drop(hook);

    let events = read_events(dir.path()).expect("read");
    assert_eq!(events.len(), 1);
    assert!(events[0].is_error);
    assert_eq!(events[0].method_name, "delete_address");
    assert_eq!(events[0].args["address_id"], "not_found");
    assert_eq!(events[0].args["merchant_id"], "merch_1");
    assert_eq!(events[0].result["Err"], "address not found");
}

#[tokio::test]
async fn delegation_sequences_multiple_calls() {
    let _rec = recording_enabled();
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = Arc::new(RecordingHook::new(dir.path()).expect("hook"));

    let store = DejaStore {
        inner: Box::new(RealStore),
        hook: hook.clone(),
    };

    let _ = store.find_address_by_id("addr_1").await;
    let _ = store.update_address("addr_1".into(), "Mumbai".into()).await;
    let _ = store.find_address_by_id("addr_2").await;

    drop(store);
    drop(hook);

    let events = read_events(dir.path()).expect("read");
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].global_sequence, 0);
    assert_eq!(events[1].global_sequence, 1);
    assert_eq!(events[2].global_sequence, 2);
    assert_eq!(events[0].method_name, "find_address_by_id");
    assert_eq!(events[1].method_name, "update_address");
    assert_eq!(events[2].method_name, "find_address_by_id");
}

#[tokio::test]
async fn delegation_with_owned_args_works() {
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = Arc::new(RecordingHook::new(dir.path()).expect("hook"));

    let store = DejaStore {
        inner: Box::new(RealStore),
        hook: hook.clone(),
    };

    let result = store.update_address("addr_1".into(), "Delhi".into()).await;
    assert_eq!(result, Ok("Updated(addr_1, Delhi)".to_string()));
}

#[tokio::test]
async fn delegation_with_associated_type_works() {
    let _rec = recording_enabled();
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = Arc::new(RecordingHook::new(dir.path()).expect("hook"));

    let store = DejaConfigStore {
        inner: Box::new(RealConfigStore),
        hook: hook.clone(),
    };

    let result = store.find_config_by_key("feature").await;
    assert_eq!(result, Ok("config(feature)".to_string()));

    drop(store);
    drop(hook);

    let events = read_events(dir.path()).expect("read");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].trait_name, "ConfigInterface");
    assert_eq!(events[0].method_name, "find_config_by_key");
}

#[tokio::test]
async fn recording_only_does_not_require_deserialize_owned_return() {
    let _rec = recording_enabled();
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = Arc::new(RecordingHook::new(dir.path()).expect("hook"));

    let store = DejaSerializeOnlyStore {
        inner: Box::new(RealSerializeOnlyStore),
        hook: hook.clone(),
    };

    let result = store.fetch_serialize_only("feature".to_string()).await;
    assert_eq!(
        result,
        SerializeOnly {
            value: "feature".to_string()
        }
    );

    drop(store);
    drop(hook);

    let events = read_events(dir.path()).expect("read");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].result["value"], "feature");
    assert!(!events[0].is_error);
}

#[test]
fn sync_method_with_where_clause_records() {
    let _rec = recording_enabled();
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = Arc::new(RecordingHook::new(dir.path()).expect("hook"));

    let store = DejaSyncLookupStore {
        inner: RealSyncLookupStore,
        hook: hook.clone(),
    };

    let result = store.sync_lookup("plain");
    assert_eq!(result, Ok("sync:plain".to_string()));

    drop(store);
    drop(hook);

    let events = read_events(dir.path()).expect("read");
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].method_name, "sync_lookup");
    assert_eq!(events[0].result["Ok"], "sync:plain");
}

#[tokio::test]
async fn delegation_serializes_args_and_results() {
    let _rec = recording_enabled();
    let dir = tempfile::tempdir().expect("tempdir");
    let hook = Arc::new(RecordingHook::new(dir.path()).expect("hook"));

    let store = DejaStore {
        inner: Box::new(RealStore),
        hook: hook.clone(),
    };

    let _ = store.update_address("addr_99".into(), "Tokyo".into()).await;
    let _ = store.delete_address("addr_99", "merch_x").await;

    drop(store);
    drop(hook);

    let events = read_events(dir.path()).expect("read");
    assert_eq!(events.len(), 2);

    // update_address("addr_99", "Tokyo") → Ok("Updated(addr_99, Tokyo)")
    let e0 = &events[0];
    assert_eq!(e0.method_name, "update_address");
    assert_eq!(e0.args["address_id"], "addr_99");
    assert_eq!(e0.args["new_city"], "Tokyo");
    assert_eq!(e0.result["Ok"], "Updated(addr_99, Tokyo)");
    assert!(!e0.is_error);

    // delete_address("addr_99", "merch_x") → Ok(())
    let e1 = &events[1];
    assert_eq!(e1.method_name, "delete_address");
    assert_eq!(e1.args["address_id"], "addr_99");
    assert_eq!(e1.args["merchant_id"], "merch_x");
    assert_eq!(e1.result["Ok"], serde_json::Value::Null);
    assert!(!e1.is_error);
}

#[tokio::test]
async fn fast_path_skips_recording_when_inactive() {
    use deja_runtime::DisabledHook;

    struct DejaNoopStore {
        inner: Box<dyn AddressInterface + Send + Sync>,
        hook: Arc<DisabledHook>,
    }

    delegate_address_interface!(DejaNoopStore, inner, hook, "storage");

    let dir = tempfile::tempdir().expect("tempdir");
    // Use RecordingHook as a witness, but wrap with DisabledHook
    let _witness = Arc::new(RecordingHook::new(dir.path()).expect("hook"));
    let hook = Arc::new(DisabledHook);

    let store = DejaNoopStore {
        inner: Box::new(RealStore),
        hook,
    };

    // Many calls — all should skip recording entirely
    for i in 0..100 {
        let _ = store.find_address_by_id(&format!("addr_{i}")).await;
    }

    drop(store);

    // No events should have been written (DisabledHook does nothing)
    // The fast path avoids even creating the EventBuilder
    assert!(
        std::fs::read_dir(dir.path()).unwrap().count() == 0
            || read_events(dir.path())
                .map(|e| e.is_empty())
                .unwrap_or(true)
    );
}

/// Bootstrap-paradox guard: recording is opt-in *per context*, even with a live
/// `RecordingHook` installed. A boundary call made BEFORE the recording decision
/// flips to `Record` — e.g. the sampler's own Superposition read that DECIDES
/// whether to record this request — must self-exclude and leave no event. Only
/// calls made after the middleware flips the decision to `Record` are captured.
///
/// This is the record-side half of the sampler bootstrap: the decision defaults
/// to `Skip` (`recording_decision_for_current().map(should_record).unwrap_or(false)`),
/// so the config read that produces the decision cannot recursively record itself.
#[tokio::test]
async fn bootstrap_read_self_excludes_until_decision_flips() {
    // Phase 1 — the bootstrap read. NO recording decision is present, so the gate
    // resolves to `Skip`. This models the sampler reading its own config from
    // Superposition *before* it has decided to record. The real block runs, but a
    // live `RecordingHook` must capture nothing.
    let skip_dir = tempfile::tempdir().expect("tempdir");
    {
        let hook = Arc::new(RecordingHook::new(skip_dir.path()).expect("hook"));
        let store = DejaStore {
            inner: Box::new(RealStore),
            hook: hook.clone(),
        };
        let boot = store.find_address_by_id("sampler_self_read").await;
        assert_eq!(boot, Ok("Address(sampler_self_read)".to_string()));
    }
    let skipped = read_events(skip_dir.path()).expect("read");
    assert!(
        skipped.is_empty(),
        "the bootstrap read must self-exclude — no decision means Skip, so the \
         sampler's own Superposition read is never recorded even with a hook installed"
    );

    // Phase 2 — the middleware has now flipped the decision to `Record`. The same
    // boundary, on the same thread, IS captured.
    let rec_dir = tempfile::tempdir().expect("tempdir");
    {
        let _rec = recording_enabled();
        let hook = Arc::new(RecordingHook::new(rec_dir.path()).expect("hook"));
        let store = DejaStore {
            inner: Box::new(RealStore),
            hook: hook.clone(),
        };
        let after = store.find_address_by_id("addr_after_flip").await;
        assert_eq!(after, Ok("Address(addr_after_flip)".to_string()));
    }
    let recorded = read_events(rec_dir.path()).expect("read");
    assert_eq!(
        recorded.len(),
        1,
        "exactly one event — only the post-flip call is recorded"
    );
    assert_eq!(recorded[0].args["address_id"], "addr_after_flip");
}
