// The `result = { ... }` attribute blocks expand to braced expressions; the
// braces are the macro grammar, not style.
#![allow(unused_braces)]

use serde_json::json;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

#[deja::boundary(
    boundary = "unit",
    component = "BoundaryMacroTest",
    operation = "add_one",
    correlation = Some("req-boundary-1".to_string()),
    args = json!({ "input": value }),
    result = {
        (
            json!({ "output": *__deja_result }),
            false,
        )
    },
)]
fn add_one(value: u64) -> u64 {
    value + 1
}

#[deja::boundary(
    boundary = "unit_async",
    component = "BoundaryMacroTest",
    operation = "async_add_one",
    correlation = Some("req-boundary-2".to_string()),
    args = json!({ "input": value }),
    result = {
        (
            json!({ "output": __deja_result.as_ref().copied().ok() }),
            __deja_result.is_err(),
        )
    },
)]
async fn async_add_one(value: u64) -> Result<u64, &'static str> {
    Ok(value + 1)
}

struct Counter(u64);

impl Counter {
    #[deja::boundary(
        boundary = "unit_method",
        component = "BoundaryMacroTest",
        operation = "counter_add",
        correlation = Some("req-boundary-3".to_string()),
        args = json!({ "base": self.0, "input": value }),
        result = {
            (
                json!({ "output": *__deja_result }),
                false,
            )
        },
    )]
    fn add(&self, value: u64) -> u64 {
        self.0 + value
    }
}

#[deja::boundary(
    boundary = "unit_future",
    component = "BoundaryMacroTest",
    operation = "boxed_add_one",
    future = "boxed",
    correlation = Some("req-boundary-4".to_string()),
    args = json!({ "input": value }),
    result = {
        (
            json!({ "output": __deja_result.as_ref().copied().ok() }),
            __deja_result.is_err(),
        )
    },
)]
fn boxed_add_one(value: u64) -> Pin<Box<dyn Future<Output = Result<u64, &'static str>>>> {
    Box::pin(async move { Ok(value + 1) })
}

#[deja::instrument(
    correlation = Some("req-instrument-1".to_string()),
    skip(secret),
    fields(extra = value + 10),
)]
fn instrument_add(value: u64, secret: u64) -> Result<u64, &'static str> {
    let _ = secret;
    Ok(value + 1)
}

#[deja::instrument(
    correlation = Some("req-instrument-2".to_string()),
    skip_all,
    fields(kind = "async"),
)]
async fn instrument_async_error(value: u64) -> Result<u64, &'static str> {
    let _ = value;
    Err("boom")
}

#[deja::boundary(correlation = Some("req-boundary-default".to_string()))]
fn boundary_default(value: u64) -> u64 {
    value * 2
}

#[deja::redis(correlation = Some("req-redis-profile".to_string()), skip_all)]
fn redis_profile_get(key: &str) -> Result<&str, &'static str> {
    let _ = key;
    Ok("value")
}

#[deja::http(incoming, correlation = Some("req-http-profile".to_string()), skip_all)]
async fn http_profile_incoming() -> Result<u16, &'static str> {
    Ok(200)
}

fn block_on_ready<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    match Future::poll(future.as_mut(), &mut context) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("test future unexpectedly pending"),
    }
}

// De-signatured syntax hash (rank 3): two boundaries sharing the SAME
// `boundary::operation` but with DIFFERENT signatures must produce the SAME
// syntax_hash — a benign signature edit on V2 must not change a call-site's
// cross-version identity. These two differ in arity AND parameter types.
#[deja::boundary(
    boundary = "sigtest",
    component = "BoundaryMacroTest",
    operation = "sig_probe",
    correlation = Some("req-sig-a".to_string()),
    args = json!({ "x": x }),
    result = { (json!({ "output": *__deja_result }), false) },
)]
fn sig_probe_a(x: u64) -> u64 {
    x
}

#[deja::boundary(
    boundary = "sigtest",
    component = "BoundaryMacroTest",
    operation = "sig_probe",
    correlation = Some("req-sig-b".to_string()),
    args = json!({ "x": x }),
    result = { (json!({ "output": *__deja_result }), false) },
)]
fn sig_probe_b(x: u64, _y: &str) -> u64 {
    x
}

// RESULT CODEC knob (#27 / G2). `codec = SerdeCodec` is the whole-value
// serde built-in (no generic argument needed). `codec = TagCodec` is a
// CUSTOM `ReplayCodec` impl — the macro routes capture/reconstruct through the
// trait, so a non-serde-shaped capture plugs in through the canonical selector.
struct TagCodec;

impl deja::codec::ReplayCodec for TagCodec {
    type Value = u64;
    fn capture(value: &u64) -> (serde_json::Value, bool) {
        (json!({ "tagged": *value }), false)
    }
    fn reconstruct(recorded: serde_json::Value) -> Option<u64> {
        recorded.get("tagged").and_then(serde_json::Value::as_u64)
    }
}

#[deja::boundary(
    boundary = "codec_serde",
    component = "BoundaryMacroTest",
    operation = "serde_codec",
    correlation = Some("req-codec-serde".to_string()),
    args = json!({ "input": value }),
    codec = SerdeCodec,
)]
fn serde_codec(value: u64) -> u64 {
    value + 1
}

#[deja::boundary(
    boundary = "codec_custom",
    component = "BoundaryMacroTest",
    operation = "custom_codec",
    correlation = Some("req-codec-custom".to_string()),
    args = json!({ "input": value }),
    codec = TagCodec,
)]
fn custom_codec(value: u64) -> u64 {
    value + 100
}

#[test]
fn boundary_macro_records_sync_function() {
    let artifacts = tempfile::tempdir().expect("tempdir");
    std::env::set_var("DEJA_MODE", "record");
    std::env::set_var("DEJA_ARTIFACT_DIR", artifacts.path());

    assert_eq!(add_one(41), 42);
    assert_eq!(block_on_ready(async_add_one(99)), Ok(100));
    assert_eq!(Counter(5).add(9), 14);
    assert_eq!(block_on_ready(boxed_add_one(7)), Ok(8));
    let instrument_line = line!() + 1;
    assert_eq!(instrument_add(5, 999), Ok(6));
    assert_eq!(block_on_ready(instrument_async_error(10)), Err("boom"));
    assert_eq!(boundary_default(21), 42);
    let redis_line = line!() + 1;
    assert_eq!(redis_profile_get("k1"), Ok("value"));
    assert_eq!(block_on_ready(http_profile_incoming()), Ok(200));

    // De-signature probe calls: same boundary::operation ("sigtest::sig_probe"), different signatures.
    assert_eq!(sig_probe_a(7), 7);
    assert_eq!(sig_probe_b(8, "ignored"), 8);

    // Result codec knob (#27 / G2): the SerdeCodec built-in and a custom codec.
    assert_eq!(serde_codec(5), 6);
    assert_eq!(custom_codec(5), 105);

    deja_runtime::flush_global_hook().expect("flush events");
    let events = deja_runtime::read_events(artifacts.path()).expect("events");
    assert_eq!(events.len(), 13);

    // De-signatured syntax hash — the two "sigtest" boundaries share one
    // boundary::operation but differ in signature, so their syntax_hash MUST be
    // identical. If the signature ever creeps back into the hash, this fails.
    let sig_hashes: Vec<Option<u64>> = events
        .iter()
        .filter(|e| e.boundary == "sigtest")
        .map(|e| e.callsite_identity.as_ref().and_then(|id| id.syntax_hash))
        .collect();
    assert_eq!(sig_hashes.len(), 2, "two sigtest boundaries recorded");
    assert!(sig_hashes[0].is_some(), "rank-2 syntax_hash present");
    assert_eq!(
        sig_hashes[0], sig_hashes[1],
        "rank-2 syntax_hash must be signature-INDEPENDENT (de-signatured)"
    );
    assert_eq!(events[0].boundary, "unit");
    assert_eq!(events[0].trait_name, "BoundaryMacroTest");
    assert_eq!(events[0].method_name, "add_one");
    assert_eq!(events[0].correlation_id.as_deref(), Some("req-boundary-1"));
    assert_eq!(events[0].args, json!({ "input": 41 }));
    assert_eq!(events[0].result, json!({ "output": 42 }));
    assert_eq!(events[1].boundary, "unit_async");
    assert_eq!(events[1].method_name, "async_add_one");
    assert_eq!(events[1].correlation_id.as_deref(), Some("req-boundary-2"));
    assert_eq!(events[1].args, json!({ "input": 99 }));
    assert_eq!(events[1].result, json!({ "output": 100 }));
    assert_eq!(events[2].boundary, "unit_method");
    assert_eq!(events[2].method_name, "counter_add");
    assert_eq!(events[2].correlation_id.as_deref(), Some("req-boundary-3"));
    assert_eq!(events[2].args, json!({ "base": 5, "input": 9 }));
    assert_eq!(events[2].result, json!({ "output": 14 }));
    assert_eq!(events[3].boundary, "unit_future");
    assert_eq!(events[3].method_name, "boxed_add_one");
    assert_eq!(events[3].correlation_id.as_deref(), Some("req-boundary-4"));
    assert_eq!(events[3].args, json!({ "input": 7 }));
    assert_eq!(events[3].result, json!({ "output": 8 }));
    assert_eq!(events[4].boundary, "function");
    assert!(events[4].trait_name.ends_with("boundary_macro"));
    assert_eq!(events[4].method_name, "instrument_add");
    assert_eq!(
        events[4].correlation_id.as_deref(),
        Some("req-instrument-1")
    );
    // v1 "args via serde": inferred args + `fields(...)` exprs are now captured
    // as STRUCTURED serde JSON (`5`, `15`), not Debug-wrapped strings.
    assert_eq!(
        events[4].args,
        json!({
            "value": 5,
            "extra": 15,
        })
    );
    assert_eq!(
        events[4].result,
        json!({ "debug": "Ok(6)", "kind": "value" })
    );
    assert!(!events[4].is_error);
    assert!(events[4].call_file.ends_with("boundary_macro.rs"));
    assert_eq!(events[4].call_line, instrument_line);
    assert!(events[4].call_column > 0);
    assert_eq!(events[5].boundary, "function");
    assert_eq!(events[5].method_name, "instrument_async_error");
    assert_eq!(events[5].args, json!({ "kind": "async" }));
    assert_eq!(
        events[5].result,
        json!({ "debug": "Err(\"boom\")", "kind": "error" })
    );
    assert!(events[5].is_error);
    assert_eq!(events[6].boundary, "function");
    assert_eq!(events[6].method_name, "boundary_default");
    assert_eq!(events[6].args, json!({ "value": 21 }));
    assert_eq!(events[6].result, json!({ "debug": "42", "kind": "value" }));
    assert_eq!(events[7].boundary, "redis");
    assert_eq!(events[7].method_name, "redis_profile_get");
    assert_eq!(events[7].call_line, redis_line);
    assert_eq!(events[8].boundary, "http_incoming");
    assert_eq!(events[8].method_name, "http_profile_incoming");

    // DEFAULTS: an undeclared `#[deja::boundary]` event keeps the default
    // `Substitute` strategy with no `kind`. The `#[deja::redis]` KIT (G1), by
    // contrast, declares `kind = "redis"` while keeping the `Substitute` default —
    // so a redis site needs no explicit boundary/strategy/kind attributes. The
    // redis read has an empty read_set because its args were `skip_all`'d.
    assert!(
        events[0].replay_strategy == deja::ReplayStrategy::Substitute && events[0].kind.is_none(),
        "an undeclared unit boundary uses Substitute with no kind"
    );
    assert!(
        events[7].replay_strategy == deja::ReplayStrategy::Execute
            && events[7].kind.as_deref() == Some("redis"),
        "the #[deja::redis] kit declares Execute + kind = \"redis\""
    );

    // RESULT CODEC knob (#27 / G2). `codec = SerdeCodec` captures the
    // whole value losslessly via serde — a non-Result u64 records as the bare
    // number. `codec = TagCodec` routes capture through the custom
    // `ReplayCodec` impl, producing its bespoke `{"tagged": …}` shape.
    assert_eq!(events[11].boundary, "codec_serde");
    assert_eq!(events[11].method_name, "serde_codec");
    assert_eq!(events[11].args, json!({ "input": 5 }));
    assert_eq!(events[11].result, json!(6));
    assert!(!events[11].is_error);
    assert_eq!(events[12].boundary, "codec_custom");
    assert_eq!(events[12].method_name, "custom_codec");
    assert_eq!(events[12].args, json!({ "input": 5 }));
    assert_eq!(
        events[12].result,
        json!({ "tagged": 105 }),
        "custom ReplayCodec capture produces its own shape"
    );
}
