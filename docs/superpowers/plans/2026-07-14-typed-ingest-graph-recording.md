# Typed Ingest + Replay-Side Graph Recording Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the stringly-typed recording-ingest pipeline in `deja-replay-core` with a typed one (parse into `deja::DejaRecord`, fail loud), and enable replay-side execution-graph capture via a chart value so the dashboard's `GraphView` tree gets data.

**Architecture:** Leniency for Vector-stringified numbers moves onto the canonical wire types (`deja_core::serde_lenient` deserializers on `BoundaryEvent`, `CallsiteIdentity`, `ExecutionGraphNode`, `ObservedCall`) plus `#[serde(flatten)]` extras maps for unknown-field survival. Ingest routes a typed `LandingEnvelope` into `DejaRecord` values, with one quarantined `Value` shim for legacy tapes missing sequence fields. Errors become a `thiserror` `IngestError`; zero valid events is a hard error. The sandbox chart gains `router.dejaGraphRecording` (default `"enabled"`) feeding `ROUTER__DEJA__RECORDING__GRAPH`.

**Tech Stack:** Rust (edition 2021, rust 1.85), serde/serde_json, thiserror, Helm chart templating.

**Spec:** `docs/superpowers/specs/2026-07-14-typed-ingest-graph-recording-design.md`

## Global Constraints

- Workspace lints: `unsafe_code = "forbid"`, clippy `dbg_macro`/`todo`/`unwrap_used` = **deny**. Never call `.unwrap()` in non-test code (`#[cfg(test)]` code may).
- Verification command for every task: `cargo test -p <crate>`; full gate is `just verify` (fmt-check + `clippy --workspace --all-targets -- -D warnings` + `cargo test --workspace`).
- The branch (`feat/sandbox-replay-core`) has unrelated uncommitted changes. `git add` ONLY the files each task touches — never `git add -A`.
- Serialization of existing records must stay byte-identical when extras maps are empty (`skip_serializing_if = "serde_json::Map::is_empty"` everywhere an extras map is added).
- Semantics preserved from the old pipeline unless the spec says otherwise: sink markers skipped silently; graph+boundary records may share `global_sequence`, so dedup is on the full canonical serialized line, never `(recording_run_id, global_sequence)` alone; sort is by `(recording_run_id, global_sequence)` with `None` run ids first.

---

### Task 1: `deja_core::serde_lenient` module

**Files:**
- Create: `crates/deja-core/src/serde_lenient.rs`
- Modify: `crates/deja-core/src/lib.rs` (add `pub mod serde_lenient;` near the top, after the `use` block)

**Interfaces:**
- Produces (used by Tasks 2, 3): `deja_core::serde_lenient::{u64_lenient, u32_lenient, u16_lenient, opt_u64_lenient, opt_u32_lenient, vec_u64_lenient}` — all `fn <name><'de, D: serde::Deserializer<'de>>(d: D) -> Result<T, D::Error>` suitable for `#[serde(deserialize_with = "...")]`.

- [ ] **Step 1: Write the failing tests**

Create `crates/deja-core/src/serde_lenient.rs` with the module doc, empty implementations section, and this test module at the bottom:

```rust
#[cfg(test)]
mod tests {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct Probe {
        #[serde(deserialize_with = "super::u64_lenient")]
        big: u64,
        #[serde(deserialize_with = "super::u32_lenient")]
        small: u32,
        #[serde(deserialize_with = "super::u16_lenient")]
        tiny: u16,
        #[serde(default, deserialize_with = "super::opt_u64_lenient")]
        maybe: Option<u64>,
        #[serde(default, deserialize_with = "super::opt_u32_lenient")]
        maybe_small: Option<u32>,
        #[serde(default, deserialize_with = "super::vec_u64_lenient")]
        many: Vec<u64>,
    }

    #[test]
    fn accepts_numbers_and_numeric_strings() {
        let p: Probe = serde_json::from_str(
            r#"{"big":"13069351011358544953","small":"7","tiny":8,
                "maybe":"42","maybe_small":9,"many":["1","6",3]}"#,
        )
        .unwrap();
        assert_eq!(p.big, 13_069_351_011_358_544_953);
        assert_eq!(p.small, 7);
        assert_eq!(p.tiny, 8);
        assert_eq!(p.maybe, Some(42));
        assert_eq!(p.maybe_small, Some(9));
        assert_eq!(p.many, vec![1, 6, 3]);
    }

    #[test]
    fn null_options_deserialize_to_none() {
        let p: Probe = serde_json::from_str(
            r#"{"big":1,"small":1,"tiny":1,"maybe":null,"maybe_small":null,"many":[]}"#,
        )
        .unwrap();
        assert_eq!(p.maybe, None);
        assert_eq!(p.maybe_small, None);
    }

    #[test]
    fn rejects_garbage() {
        assert!(serde_json::from_str::<Probe>(
            r#"{"big":"not-a-number","small":1,"tiny":1,"many":[]}"#
        )
        .is_err());
        assert!(serde_json::from_str::<Probe>(r#"{"big":true,"small":1,"tiny":1,"many":[]}"#)
            .is_err());
        // u32 overflow via string must error, not wrap.
        assert!(serde_json::from_str::<Probe>(
            r#"{"big":1,"small":"4294967296","tiny":1,"many":[]}"#
        )
        .is_err());
    }

    #[test]
    fn survives_serde_content_buffering() {
        // #[serde(flatten)] siblings route fields through serde's private
        // Content buffer; the lenient fns must work through that path too.
        #[derive(Deserialize)]
        struct Flat {
            #[serde(deserialize_with = "super::u64_lenient")]
            n: u64,
            #[serde(flatten)]
            rest: serde_json::Map<String, serde_json::Value>,
        }
        let f: Flat = serde_json::from_str(r#"{"n":"99","other":"x"}"#).unwrap();
        assert_eq!(f.n, 99);
        assert_eq!(f.rest["other"], "x");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p deja-core serde_lenient`
Expected: compile error — `u64_lenient` etc. not found.

- [ ] **Step 3: Write the implementation**

Fill the module body (above the test module):

```rust
//! Lenient numeric deserializers for Deja wire types.
//!
//! String-preserving JSON pipelines (notably Vector) stringify unsigned
//! integers above `i64::MAX` in transit. These helpers accept both JSON
//! numbers and numeric strings so the canonical types parse tapes from
//! either path. Null/missing handling stays with `#[serde(default)]` on the
//! field; the `opt_*` variants additionally map an explicit `null` to `None`.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer};

fn u64_from_value<E: DeError>(value: serde_json::Value) -> Result<u64, E> {
    match value {
        serde_json::Value::Number(n) => n
            .as_u64()
            .ok_or_else(|| E::custom(format!("expected u64, got {n}"))),
        serde_json::Value::String(s) => s.parse::<u64>().map_err(E::custom),
        other => Err(E::custom(format!(
            "expected u64 number or string, got {other}"
        ))),
    }
}

pub fn u64_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    u64_from_value(serde_json::Value::deserialize(d)?)
}

pub fn u32_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<u32, D::Error> {
    let n = u64_lenient(d)?;
    u32::try_from(n).map_err(|_| D::Error::custom(format!("value {n} out of range for u32")))
}

pub fn u16_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<u16, D::Error> {
    let n = u64_lenient(d)?;
    u16::try_from(n).map_err(|_| D::Error::custom(format!("value {n} out of range for u16")))
}

pub fn opt_u64_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    match Option::<serde_json::Value>::deserialize(d)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(value) => u64_from_value(value).map(Some),
    }
}

pub fn opt_u32_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u32>, D::Error> {
    match opt_u64_lenient(d)? {
        None => Ok(None),
        Some(n) => u32::try_from(n)
            .map(Some)
            .map_err(|_| D::Error::custom(format!("value {n} out of range for u32"))),
    }
}

pub fn vec_u64_lenient<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u64>, D::Error> {
    Vec::<serde_json::Value>::deserialize(d)?
        .into_iter()
        .map(u64_from_value)
        .collect()
}
```

Add to `crates/deja-core/src/lib.rs` after the `use` block:

```rust
pub mod serde_lenient;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo test -p deja-core serde_lenient`
Expected: 4 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/deja-core/src/serde_lenient.rs crates/deja-core/src/lib.rs
git commit -m "feat(deja-core): lenient numeric deserializers for wire types"
```

---

### Task 2: `ExecutionGraphNode` — lenient numbers + extras map

**Files:**
- Modify: `crates/deja-core/src/lib.rs:122-152` (the `ExecutionGraphNode` struct)
- Modify: any construction sites the compiler flags (7 struct literals across `deja-core`, `deja-runtime`, `deja-tui`)

**Interfaces:**
- Consumes: `deja_core::serde_lenient` (Task 1).
- Produces: `ExecutionGraphNode` now parses stringified numbers directly and carries `pub extras: serde_json::Map<String, serde_json::Value>`. Its derive drops `Eq` (`serde_json::Value` is not `Eq`); `PartialEq` remains.

- [ ] **Step 1: Write the failing test**

Add to the existing test module in `crates/deja-core/src/lib.rs` (create `#[cfg(test)] mod tests` at the bottom if the graph types have none):

```rust
#[test]
fn execution_graph_node_parses_stringified_numbers_and_keeps_unknown_fields() {
    let json = r#"{
        "node_id":"7","global_sequence":"2","parent_id":"1",
        "causal_parent_ids":["1","6"],"sequence":"3",
        "recording_run_id":"r1","span_name":"request","target":"router",
        "level":"INFO","fields":{},"started_ns":"1783029410812345678",
        "closed_ns":"1783029410812345999","future_field":{"x":1}
    }"#;
    let node: ExecutionGraphNode = serde_json::from_str(json).unwrap();
    assert_eq!(node.node_id, 7);
    assert_eq!(node.global_sequence, 2);
    assert_eq!(node.parent_id, Some(1));
    assert_eq!(node.causal_parent_ids, vec![1, 6]);
    assert_eq!(node.sequence, 3);
    assert_eq!(node.started_ns, 1_783_029_410_812_345_678);
    assert_eq!(node.closed_ns, Some(1_783_029_410_812_345_999));
    assert_eq!(node.extras["future_field"]["x"], 1);

    // Unknown fields survive a typed round-trip.
    let out = serde_json::to_string(&node).unwrap();
    let reparsed: ExecutionGraphNode = serde_json::from_str(&out).unwrap();
    assert_eq!(reparsed.extras["future_field"]["x"], 1);
}

#[test]
fn execution_graph_node_without_extras_serializes_no_extras_key() {
    let json = r#"{"node_id":1,"sequence":0,"span_name":"s","target":"t",
        "level":"INFO","started_ns":5}"#;
    let node: ExecutionGraphNode = serde_json::from_str(json).unwrap();
    let out = serde_json::to_string(&node).unwrap();
    assert!(!out.contains("extras"));
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p deja-core execution_graph_node`
Expected: FAIL — no field `extras`, and the stringified-numbers parse errors.

- [ ] **Step 3: Modify the struct**

In `crates/deja-core/src/lib.rs`, change `ExecutionGraphNode`'s derive from
`#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]` to
`#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]` (extras holds
`serde_json::Value`, which is not `Eq`), then annotate the numeric fields and
append `extras`:

```rust
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecutionGraphNode {
    #[serde(deserialize_with = "crate::serde_lenient::u64_lenient")]
    pub node_id: u64,
    #[serde(default, deserialize_with = "crate::serde_lenient::u64_lenient")]
    pub global_sequence: u64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::serde_lenient::opt_u64_lenient"
    )]
    pub parent_id: Option<u64>,
    #[serde(default, deserialize_with = "crate::serde_lenient::vec_u64_lenient")]
    pub causal_parent_ids: Vec<u64>,
    #[serde(deserialize_with = "crate::serde_lenient::u64_lenient")]
    pub sequence: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_run_id: Option<String>,
    pub span_name: String,
    pub target: String,
    pub level: String,
    #[serde(default)]
    pub fields: BTreeMap<String, serde_json::Value>,
    #[serde(deserialize_with = "crate::serde_lenient::u64_lenient")]
    pub started_ns: u64,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "crate::serde_lenient::opt_u64_lenient"
    )]
    pub closed_ns: Option<u64>,
    /// Unknown sibling fields from newer/older recorders, preserved so typed
    /// round-trips (notably replay ingest) never drop cross-version data.
    #[serde(flatten, default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extras: serde_json::Map<String, serde_json::Value>,
}
```

Keep every doc comment already on those fields — only the serde attributes and
the trailing `extras` field change. Note `global_sequence` keeps its existing
`#[serde(default)]` and `fields` keeps its attributes exactly.

- [ ] **Step 4: Fix construction sites compiler-first**

Run: `cargo build --workspace --all-targets 2>&1 | grep -A2 'missing.*extras\|Eq'`
Add `extras: serde_json::Map::new(),` to each flagged `ExecutionGraphNode { ... }`
literal (7 sites across `deja-core`, `deja-runtime` graph layer, `deja-tui`).
If dropping `Eq` breaks a use (e.g. a `HashSet<ExecutionGraphNode>`), switch that
use to keying on `node_id` — but none is expected.

- [ ] **Step 5: Run tests**

Run: `cargo test -p deja-core && cargo build --workspace --all-targets`
Expected: PASS, workspace builds.

- [ ] **Step 6: Commit**

```bash
git add crates/deja-core/src/lib.rs crates/deja-runtime crates/deja-tui
git commit -m "feat(deja-core): lenient numbers + extras map on ExecutionGraphNode"
```

---

### Task 3: `BoundaryEvent`, `CallsiteIdentity`, `ObservedCall` — lenient numbers + extras

**Files:**
- Modify: `crates/deja-runtime/src/lib.rs` (`BoundaryEvent` at ~line 98, `CallsiteIdentity` at ~line 682)
- Modify: `crates/deja-runtime/src/replay.rs` (`ObservedCall` at ~line 1101)
- Modify: construction sites the compiler flags (~56 `BoundaryEvent`, ~16 `CallsiteIdentity` literals; many in tests, plus the `EventBuilder` in deja-runtime and macro-support code in `crates/deja/src`)

**Interfaces:**
- Consumes: `deja_core::serde_lenient` (Task 1).
- Produces: `BoundaryEvent.extras` and `CallsiteIdentity.extras` (`serde_json::Map<String, serde_json::Value>`); all numeric metadata fields parse stringified numbers. `ObservedCall` gets lenient numerics only (no extras — it never rides record tapes; ingest keeps it parseable for `DejaRecord::Observed` lines).

- [ ] **Step 1: Write the failing test**

Add to `crates/deja-runtime/src/lib.rs` tests (there is an existing `#[cfg(test)]` module):

```rust
#[test]
fn boundary_event_parses_stringified_numbers_and_keeps_unknown_fields() {
    let json = serde_json::json!({
        "global_sequence": "1", "request_sequence": "0", "correlation_id": "c1",
        "timestamp_ns": "1783029410812345678",
        "tracing_span_id": "9223372586610589699",
        "graph_node_id": "29751", "fork_seq": "0",
        "boundary": "db", "trait_name": "T", "method_name": "m",
        "call_file": "lib.rs", "call_line": "1", "call_column": "1",
        "request": {}, "args": {}, "response": {"ok": true}, "result": {"ok": true},
        "is_error": false, "duration_us": "5",
        "event_schema_version": CURRENT_EVENT_SCHEMA_VERSION.to_string(),
        "callsite_identity": {
            "version": "1", "source": "SyntacticHash", "id": null, "scope": null,
            "occurrence": "3", "caller_function": null, "lexical_path": null,
            "syntax_hash": "13069351011358544953",
            "logical_context": "a>b>c",
            "identity_future_field": true
        },
        "provenance": "recorded", "recon": "lossless",
        "value_digest": "958161998582843277",
        "end_timestamp_ns": "1783029410812345999",
        "replay_strategy": "substitute",
        "event_future_field": {"nested": 1}
    })
    .to_string();
    let event: BoundaryEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(event.global_sequence, 1);
    assert_eq!(event.timestamp_ns, 1_783_029_410_812_345_678);
    assert_eq!(event.tracing_span_id, Some(9_223_372_586_610_589_699));
    assert_eq!(event.graph_node_id, Some(29_751));
    assert_eq!(event.call_line, 1);
    assert_eq!(event.duration_us, 5);
    assert_eq!(event.value_digest, Some(958_161_998_582_843_277));
    assert_eq!(event.end_timestamp_ns, Some(1_783_029_410_812_345_999));
    let identity = event.callsite_identity.as_ref().unwrap();
    assert_eq!(identity.version, 1);
    assert_eq!(identity.occurrence, 3);
    assert_eq!(identity.syntax_hash, Some(13_069_351_011_358_544_953));
    assert_eq!(identity.span_path.as_deref(), Some("a>b>c"));
    assert_eq!(identity.extras["identity_future_field"], true);
    assert_eq!(event.extras["event_future_field"]["nested"], 1);

    // Round-trip: unknown fields survive; span_path re-emits as logical_context.
    let out = serde_json::to_string(&event).unwrap();
    let reparsed: BoundaryEvent = serde_json::from_str(&out).unwrap();
    assert_eq!(reparsed.extras["event_future_field"]["nested"], 1);
    assert_eq!(
        reparsed.callsite_identity.unwrap().extras["identity_future_field"],
        true
    );
    assert!(out.contains("\"logical_context\":\"a>b>c\""));
}

#[test]
fn boundary_event_without_extras_serializes_no_extras_key() {
    let event = EventBuilder::new("db", "T", "m")
        .args(serde_json::json!({}))
        .result(serde_json::json!({}), false)
        .build();
    let out = serde_json::to_string(&event).unwrap();
    assert!(!out.contains("extras"));
}
```

(If `EventBuilder`'s exact construction API differs, build the minimal
`BoundaryEvent` the same way the nearest existing test in the file does —
copy that pattern and add `extras: serde_json::Map::new()` where needed.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p deja-runtime boundary_event_parses_stringified`
Expected: FAIL — stringified numbers reject, no `extras` field.

- [ ] **Step 3: Annotate the structs**

In `crates/deja-runtime/src/lib.rs`, on `BoundaryEvent` add
`deserialize_with` attributes (merging with each field's existing attributes):

| Field | Attribute to add |
|---|---|
| `global_sequence`, `request_sequence`, `timestamp_ns`, `duration_us` | `deserialize_with = "deja_core::serde_lenient::u64_lenient"` |
| `graph_node_id`, `tracing_span_id`, `fork_seq`, `value_digest`, `end_timestamp_ns` | `deserialize_with = "deja_core::serde_lenient::opt_u64_lenient"` (these already have `default`) |
| `call_line`, `call_column` | `deserialize_with = "deja_core::serde_lenient::u32_lenient"` |
| `event_schema_version` | `deserialize_with = "deja_core::serde_lenient::u16_lenient"` |

Append to `BoundaryEvent` (last field):

```rust
    /// Unknown sibling fields from newer/older recorders, preserved so typed
    /// round-trips (notably replay ingest) never drop cross-version data.
    #[serde(flatten, default, skip_serializing_if = "serde_json::Map::is_empty")]
    pub extras: serde_json::Map<String, serde_json::Value>,
```

On `CallsiteIdentity`: `version` → `u16_lenient`, `occurrence` → `u32_lenient`,
`syntax_hash` → `opt_u64_lenient` (keep its `default`), and append the same
`extras` field with the same doc comment.

In `crates/deja-runtime/src/replay.rs`, on `ObservedCall`:
`timestamp_ns` → `u64_lenient` (keep `default` + `skip_serializing_if`),
`end_timestamp_ns`/`source_event_global_sequence`/`graph_node_id` →
`opt_u64_lenient`, `fork_seq` → `u64_lenient`, `call_line`/`call_column` →
`opt_u32_lenient`. No extras on `ObservedCall`.

- [ ] **Step 4: Fix construction sites compiler-first**

Run: `cargo build --workspace --all-targets 2>&1 | grep -B1 'missing.*extras' | head -40`
Add `extras: serde_json::Map::new(),` to every flagged `BoundaryEvent { ... }` /
`CallsiteIdentity { ... }` literal (~72 sites; most are tests in deja-runtime,
deja, deja-kernel, deja-orchestrator, deja-replay-agent). Literals using struct
update syntax (`..base`) need no change. Repeat until the workspace builds.

- [ ] **Step 5: Run tests**

Run: `cargo test -p deja-runtime && cargo test -p deja && cargo build --workspace --all-targets`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/deja-runtime crates/deja crates/deja-kernel crates/deja-orchestrator crates/deja-replay-agent
git commit -m "feat(deja-runtime): lenient numbers + extras on BoundaryEvent/CallsiteIdentity"
```

---

### Task 4: `IngestError` + `lines_dropped` (fail-loud surface on the existing pipeline)

**Files:**
- Modify: `crates/deja-replay-core/Cargo.toml` (add `thiserror = "1"` to `[dependencies]`)
- Modify: `crates/deja-replay-core/src/ingest.rs`
- Modify: `crates/deja-orchestrator/src/lifecycle/mod.rs:2684-2690` (map the new error type)
- Modify: `crates/deja-replay-agent/src/lib.rs:1135` (test `IngestReport` literal gains `lines_dropped: 0`)

**Interfaces:**
- Produces:
  - `pub enum IngestError { S3(String), Decode(String), Io { context: String, source: std::io::Error }, UnsupportedSource(String), NoEvents { recording_id: String, lines_in: usize, lines_dropped: usize } }` implementing `std::error::Error + Display` via `thiserror`.
  - `pub fn count_session_objects(...) -> Result<usize, IngestError>`
  - `pub fn pull_recording(...) -> Result<(IngestReport, SessionManifest), IngestError>`
  - `pub fn pull_recording_source(...) -> Result<PulledRecording, IngestError>`
  - `IngestReport.lines_dropped: usize` (serialized).
- Consumed by: Task 5 keeps these exact signatures.

- [ ] **Step 1: Write the failing test**

Add to `crates/deja-replay-core/src/ingest.rs` tests:

```rust
#[test]
fn collate_counts_dropped_lines() {
    let junk = b"not-json\n{\"artifact_type\":\"unexpected_record_type\",\"event\":{\"global_sequence\":1}}\n".to_vec();
    let (events, lines_in, _dupes, dropped) = collate(&[junk]);
    assert!(events.is_empty());
    assert_eq!(lines_in, 2);
    assert_eq!(dropped, 2);
}
```

And a `NoEvents` display test:

```rust
#[test]
fn no_events_error_reports_counts() {
    let err = IngestError::NoEvents {
        recording_id: "rec-1".into(),
        lines_in: 5,
        lines_dropped: 5,
    };
    assert_eq!(
        err.to_string(),
        "recording rec-1 produced no valid events (5 line(s) in, 5 dropped)"
    );
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p deja-replay-core ingest`
Expected: compile error — `IngestError` undefined, `collate` returns 3-tuple.

- [ ] **Step 3: Implement**

Add `thiserror = "1"` to `crates/deja-replay-core/Cargo.toml` `[dependencies]`.

In `ingest.rs`:

```rust
/// Typed ingest failure. `S3` wraps the compactor's string errors at that
/// crate boundary; everything downstream is structured.
#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("{0}")]
    S3(String),
    #[error("{0}")]
    Decode(String),
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
    #[error("unsupported recording source {0:?}; expected s3://bucket/key-or-prefix")]
    UnsupportedSource(String),
    #[error("recording {recording_id} produced no valid events ({lines_in} line(s) in, {lines_dropped} dropped)")]
    NoEvents {
        recording_id: String,
        lines_in: usize,
        lines_dropped: usize,
    },
}
```

- Change the three public fns' error type to `IngestError`; wrap every
  `deja_compactor::*` call and `parse_s3_uri` error with `.map_err(IngestError::S3)`
  / `IngestError::Decode`; `write_events`' `format!`-based IO errors become
  `IngestError::Io { context, source }` (thread the `std::io::Error` through
  instead of formatting it).
- `collate` returns `(events, lines_in, duplicates, dropped)`; increment
  `dropped` at every existing `eprintln!("ingest: dropping ...")` site (keep the
  eprintlns).
- Add `pub lines_dropped: usize` to `IngestReport` (fill from collate; the
  sealed-session path in `pull_recording` and the direct path in
  `pull_direct_s3_recording` both get it).
- After collate in BOTH pull paths: if `events.is_empty()`, return
  `Err(IngestError::NoEvents { recording_id: <id or source uri>, lines_in, lines_dropped })`
  before writing anything.

Callers:
- `crates/deja-orchestrator/src/lifecycle/mod.rs:2687`: change
  `crate::s3::pull_recording(&cfg, recording_id, &dest)?` to
  `crate::s3::pull_recording(&cfg, recording_id, &dest).map_err(|e| e.to_string())?`.
  Line 2655's `count_session_objects(...).unwrap_or(0)` compiles unchanged.
- `crates/deja-replay-agent/src/lib.rs:1135` test literal: add `lines_dropped: 0,`.
- The agent's `format!("ingest: {e}")` at `lib.rs:219` compiles unchanged
  (`Display`).

- [ ] **Step 4: Run tests**

Run: `cargo test -p deja-replay-core && cargo test -p deja-orchestrator && cargo test -p deja-replay-agent`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crates/deja-replay-core crates/deja-orchestrator/src/lifecycle/mod.rs crates/deja-replay-agent/src/lib.rs
git commit -m "feat(deja-replay-core): typed IngestError, dropped-line accounting, NoEvents guard"
```

---

### Task 5: Typed collate pipeline

**Files:**
- Modify: `crates/deja-replay-core/src/ingest.rs` (the core rewrite)
- Modify: `crates/deja/src/lib.rs` (add `pub use deja_core::ExecutionGraphNode;` next to the existing runtime re-exports at ~line 63)

**Interfaces:**
- Consumes: `deja::DejaRecord` (internally tagged on `record_kind`), `deja::BoundaryEvent`, `deja::ExecutionGraphNode` (new facade re-export), lenient/extras behavior from Tasks 1-3, `IngestError` from Task 4.
- Produces: same public fn signatures as Task 4. Internal shape:
  - `struct CollatedRecord { record: deja::DejaRecord, json: String }` with `fn run_id(&self) -> Option<&str>` and `fn global_sequence(&self) -> u64`.
  - `collate(&[Vec<u8>]) -> (Vec<CollatedRecord>, usize, usize, usize)`.

**Deleted in this task:** `EventProbe`, `EnvelopeProbe`, `LineKindProbe`,
`RecordKindProbe`, `EventBreakdownProbe`, `stamp_record_kind`,
`ensure_record_kind`, `has_record_kind`, `normalize_event_numbers`,
`normalized_kind`, `de_u64_lenient`, `coerce_u64_string`, `coerce_u64_field`,
`coerce_u64_array_field`, `missing_or_null`, `is_sink_marker_line`,
`is_sink_marker_kind`, `payload_from_envelope`, and the old `ArtifactKind`.

- [ ] **Step 1: Write the failing tests**

Add these tests (the existing `collate_*` tests stay and must also pass; adapt
only `collate_unwraps_dedups_and_sorts`' byte-verbatim assertion
`events[0].2.contains(r#""global_sequence":1,"k":"a""#)` — key order is now
canonical serde output, so assert on the parsed value instead:
`assert_eq!(v["global_sequence"], 1); assert_eq!(v["k"], "a");`; tuple access
`events[i].2`/`events[i].1`/`events[i].0` becomes `events[i].json` /
`events[i].global_sequence()` / `events[i].run_id()`):

```rust
#[test]
fn artifact_type_spelling_variants_all_route() {
    for spelling in ["deja_graph_node", "DejaGraph", "GRAPH-NODE", "graphnode"] {
        let envelope = serde_json::json!({
            "artifact_type": spelling,
            "node": {"node_id": 1, "sequence": 0, "span_name": "s",
                      "target": "t", "level": "INFO", "started_ns": 5}
        })
        .to_string();
        let (events, _, _, dropped) = collate(&[envelope.into_bytes()]);
        assert_eq!(events.len(), 1, "spelling {spelling:?} must route");
        assert_eq!(dropped, 0);
        assert!(matches!(events[0].record, deja::DejaRecord::GraphNode(_)));
    }
}

#[test]
fn unknown_payload_fields_survive_ingest() {
    let envelope = serde_json::json!({
        "artifact_type": "deja_artifact_record",
        "event": {
            "global_sequence": 1, "request_sequence": 1, "correlation_id": "c1",
            "timestamp_ns": 1, "boundary": "db", "trait_name": "T",
            "method_name": "m", "call_file": "f", "call_line": 1, "call_column": 1,
            "request": {}, "args": {}, "response": {}, "result": {},
            "is_error": false, "duration_us": 1,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded", "recon": "lossless",
            "replay_strategy": "substitute",
            "brand_new_field": "must-survive"
        }
    })
    .to_string();
    let (events, _, _, dropped) = collate(&[envelope.into_bytes()]);
    assert_eq!(dropped, 0);
    let v: serde_json::Value = serde_json::from_str(&events[0].json).unwrap();
    assert_eq!(v["brand_new_field"], "must-survive");
}

#[test]
fn rescue_applies_only_to_missing_sequences_not_present_zero() {
    // request_sequence present as 0 must stay 0 (real tapes carry it).
    let with_zero = serde_json::json!({
        "artifact_type": "deja_artifact_record",
        "event": {
            "global_sequence": 979, "request_sequence": 0, "timestamp_ns": 1,
            "boundary": "http_incoming", "trait_name": "T", "method_name": "m",
            "call_file": "f", "call_line": 1, "call_column": 1,
            "request": {}, "args": {}, "response": {}, "result": {},
            "is_error": false, "duration_us": 1,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded", "recon": "lossless",
            "replay_strategy": "substitute"
        }
    })
    .to_string();
    let (events, _, _, _) = collate(&[with_zero.into_bytes()]);
    match &events[0].record {
        deja::DejaRecord::BoundaryEvent(e) => {
            assert_eq!(e.global_sequence, 979);
            assert_eq!(e.request_sequence, 0);
        }
        other => panic!("expected boundary event, got {other:?}"),
    }
}

#[test]
fn junk_object_lines_are_dropped_not_minted_as_events() {
    // Previously any JSON object was stamped boundary_event; typed parse
    // rejects it and counts the drop.
    let raw = br#"{"foo": 1}"#.to_vec();
    let (events, lines_in, _, dropped) = collate(&[raw]);
    assert!(events.is_empty());
    assert_eq!(lines_in, 1);
    assert_eq!(dropped, 1);
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p deja-replay-core ingest`
Expected: compile errors (`.record`/`.json` don't exist yet) and/or failures.

- [ ] **Step 3: Implement the typed pipeline**

Add to `crates/deja/src/lib.rs` (next to the runtime re-exports):

```rust
pub use deja_core::ExecutionGraphNode;
```

In `ingest.rs`, replace the deleted items with:

```rust
/// Landing-line envelope, typed. Unknown envelope metadata (`instance_id`,
/// `capture`, `code`, envelope-level `schema_version` — v1 and v2 both occur)
/// is intentionally ignored; only the payload reaches `events.jsonl`.
#[derive(serde::Deserialize)]
struct LandingEnvelope<'a> {
    #[serde(default)]
    artifact_type: Option<ArtifactType>,
    #[serde(borrow, default)]
    event: Option<&'a serde_json::value::RawValue>,
    #[serde(borrow, default)]
    node: Option<&'a serde_json::value::RawValue>,
    /// Top-level `record_kind` on raw (non-enveloped) marker lines.
    #[serde(default)]
    record_kind: Option<String>,
    /// Presence alone marks a sink marker line.
    #[serde(default)]
    marker_kind: Option<serde_json::Value>,
}

impl LandingEnvelope<'_> {
    fn is_raw_line(&self) -> bool {
        self.artifact_type.is_none() && self.event.is_none() && self.node.is_none()
    }

    fn is_sink_marker(&self) -> bool {
        self.marker_kind.is_some()
            || self.artifact_type == Some(ArtifactType::SinkMarker)
            || self
                .record_kind
                .as_deref()
                .is_some_and(|kind| ArtifactType::from_wire(kind) == ArtifactType::SinkMarker)
    }
}

/// Artifact routing kind. Spelling-tolerant: `DejaGraph`, `deja_graph_node`,
/// and `GRAPH-NODE` all normalize to the same kind (parity with the old
/// `normalized_kind` matcher). Unrecognized types become `Unknown` so the
/// envelope still parses and the line is dropped WITH accounting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArtifactType {
    BoundaryEvent,
    GraphNode,
    SinkMarker,
    Unknown,
}

impl ArtifactType {
    fn from_wire(kind: &str) -> Self {
        let normalized: String = kind
            .chars()
            .filter(|c| c.is_ascii_alphanumeric())
            .flat_map(char::to_lowercase)
            .collect();
        match normalized.as_str() {
            "dejasinkmarker" | "sinkmarker" => Self::SinkMarker,
            "dejagraph" | "dejagraphnode" | "graph" | "graphnode" => Self::GraphNode,
            "dejarecord" | "dejaartifactrecord" | "artifactrecord" | "record" => {
                Self::BoundaryEvent
            }
            _ => Self::Unknown,
        }
    }
}

impl<'de> serde::Deserialize<'de> for ArtifactType {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(Self::from_wire(&String::deserialize(d)?))
    }
}

/// Typed routing probe: does the payload carry the `record_kind` tag? When it
/// does, `DejaRecord`'s internal tag wins over the envelope's artifact type.
#[derive(serde::Deserialize)]
struct PayloadTag {
    #[serde(default)]
    record_kind: Option<String>,
}

/// One collated output line: the typed record plus its canonical serialized
/// form (serialized exactly once; dedup compares the canonical form because
/// graph and boundary records share the gseq counter space).
pub struct CollatedRecord {
    pub record: deja::DejaRecord,
    pub json: String,
}

impl CollatedRecord {
    fn new(record: deja::DejaRecord) -> Result<Self, serde_json::Error> {
        let json = serde_json::to_string(&record)?;
        Ok(Self { record, json })
    }

    pub fn run_id(&self) -> Option<&str> {
        match &self.record {
            deja::DejaRecord::BoundaryEvent(event) => event.recording_run_id.as_deref(),
            deja::DejaRecord::GraphNode(node) => node.recording_run_id.as_deref(),
            deja::DejaRecord::Observed(_) => None,
        }
    }

    pub fn global_sequence(&self) -> u64 {
        self.record.global_sequence()
    }
}

mod legacy {
    //! The one quarantined `serde_json::Value` shim: old direct-S3 tapes can
    //! omit `global_sequence`/`request_sequence` entirely. Both are required
    //! fields and `0` is a legitimate value (real tapes carry
    //! `request_sequence: 0`), so "missing" cannot be expressed by a serde
    //! default. Attempted only after a typed boundary parse fails.

    pub(super) fn rescue_missing_sequences(payload: &str, fallback: u64) -> Option<String> {
        let mut value: serde_json::Value = serde_json::from_str(payload).ok()?;
        let object = value.as_object_mut()?;
        let missing = |object: &serde_json::Map<String, serde_json::Value>, field: &str| {
            object.get(field).is_none_or(serde_json::Value::is_null)
        };
        if missing(object, "global_sequence") {
            object.insert("global_sequence".to_owned(), fallback.into());
        }
        let sequence = match object.get("global_sequence") {
            Some(serde_json::Value::Number(n)) => n.as_u64().unwrap_or(fallback),
            Some(serde_json::Value::String(s)) => s.parse().unwrap_or(fallback),
            _ => fallback,
        };
        if missing(object, "request_sequence") {
            object.insert("request_sequence".to_owned(), sequence.into());
        }
        serde_json::to_string(&value).ok()
    }
}
```

Routing + collate (replaces the old `collate` body; keep the fn doc updated):

```rust
fn parse_payload(
    payload: &str,
    artifact_type: ArtifactType,
    fallback_sequence: u64,
) -> Option<deja::DejaRecord> {
    let tagged = serde_json::from_str::<PayloadTag>(payload)
        .ok()
        .and_then(|tag| tag.record_kind)
        .is_some();
    if tagged {
        if let Ok(record) = serde_json::from_str::<deja::DejaRecord>(payload) {
            return Some(record);
        }
        // A tagged boundary payload may still be missing its sequences.
        let rescued = legacy::rescue_missing_sequences(payload, fallback_sequence)?;
        return serde_json::from_str::<deja::DejaRecord>(&rescued).ok();
    }
    match artifact_type {
        ArtifactType::BoundaryEvent => {
            if let Ok(event) = serde_json::from_str::<deja::BoundaryEvent>(payload) {
                return Some(deja::DejaRecord::BoundaryEvent(Box::new(event)));
            }
            let rescued = legacy::rescue_missing_sequences(payload, fallback_sequence)?;
            serde_json::from_str::<deja::BoundaryEvent>(&rescued)
                .ok()
                .map(|event| deja::DejaRecord::BoundaryEvent(Box::new(event)))
        }
        ArtifactType::GraphNode => serde_json::from_str::<deja::ExecutionGraphNode>(payload)
            .ok()
            .map(deja::DejaRecord::GraphNode),
        ArtifactType::SinkMarker | ArtifactType::Unknown => None,
    }
}

#[allow(clippy::type_complexity)]
fn collate(raw_chunks: &[Vec<u8>]) -> (Vec<CollatedRecord>, usize, usize, usize) {
    let mut seen = std::collections::HashSet::new();
    let mut events: Vec<CollatedRecord> = Vec::new();
    let mut lines_in = 0usize;
    let mut duplicates = 0usize;
    let mut dropped = 0usize;
    for chunk in raw_chunks {
        for line_str in records_from_chunk(chunk) {
            lines_in += 1;
            let Ok(envelope) = serde_json::from_str::<LandingEnvelope>(&line_str) else {
                eprintln!("ingest: dropping non-JSON line");
                dropped += 1;
                continue;
            };
            if envelope.is_sink_marker() {
                continue;
            }
            let fallback_sequence = events.len() as u64 + 1;
            let record = if envelope.is_raw_line() {
                // Raw (non-enveloped) event line: route by its own tag, or
                // default to the boundary route like the old pipeline.
                parse_payload(&line_str, ArtifactType::BoundaryEvent, fallback_sequence)
            } else {
                let artifact_type = envelope.artifact_type.unwrap_or(ArtifactType::BoundaryEvent);
                if artifact_type == ArtifactType::Unknown {
                    eprintln!("ingest: dropping envelope with unknown artifact_type");
                    dropped += 1;
                    continue;
                }
                let payload = match artifact_type {
                    ArtifactType::BoundaryEvent => envelope.event.or(envelope.node),
                    ArtifactType::GraphNode => envelope.node.or(envelope.event),
                    ArtifactType::SinkMarker | ArtifactType::Unknown => None,
                };
                payload.and_then(|payload| {
                    parse_payload(payload.get(), artifact_type, fallback_sequence)
                })
            };
            let Some(record) = record else {
                eprintln!("ingest: dropping unparseable line");
                dropped += 1;
                continue;
            };
            let Ok(collated) = CollatedRecord::new(record) else {
                eprintln!("ingest: dropping unserializable record");
                dropped += 1;
                continue;
            };
            if !seen.insert(collated.json.clone()) {
                duplicates += 1;
                continue;
            }
            events.push(collated);
        }
    }
    events.sort_by(|a, b| {
        (a.run_id(), a.global_sequence()).cmp(&(b.run_id(), b.global_sequence()))
    });
    (events, lines_in, duplicates, dropped)
}
```

Typed helpers replacing the probe-based ones:

```rust
fn write_events(dest: &Path, events: &[CollatedRecord]) -> Result<(), IngestError> {
    // as Task 4, iterating `event.json` lines
}

fn correlation_count(events: &[CollatedRecord]) -> usize {
    events
        .iter()
        .filter_map(|event| match &event.record {
            deja::DejaRecord::BoundaryEvent(inner) => inner.correlation_id.as_deref(),
            _ => None,
        })
        .collect::<BTreeSet<_>>()
        .len()
}

fn ingest_breakdown(events: &[CollatedRecord]) -> IngestBreakdown {
    let mut breakdown = IngestBreakdown::default();
    for event in events {
        let kind = match &event.record {
            deja::DejaRecord::BoundaryEvent(_) => "boundary_event",
            deja::DejaRecord::GraphNode(_) => "graph_node",
            deja::DejaRecord::Observed(_) => "observed",
        };
        *breakdown.record_kinds.entry(kind.to_owned()).or_insert(0) += 1;
        if let deja::DejaRecord::BoundaryEvent(inner) = &event.record {
            *breakdown
                .boundaries
                .entry(inner.boundary.clone())
                .or_insert(0) += 1;
        }
    }
    breakdown
}
```

Update the module doc: replace the "raw event bytes preserved via `RawValue`,
no reserialization" claim with "payloads parse into `deja::DejaRecord` and are
serialized once to canonical form; unknown fields ride the extras maps".

- [ ] **Step 4: Adapt existing tests and run**

Adapt tuple accesses and the byte-verbatim assertion as described in Step 1.
Note `collate_backfills_missing_boundary_global_sequence` and
`collate_normalizes_stringified_*` must pass UNCHANGED in behavior (rescue and
lenient parse cover them).

Run: `cargo test -p deja-replay-core`
Expected: all ingest tests PASS (old + new).

- [ ] **Step 5: Workspace check + commit**

Run: `cargo build --workspace --all-targets && cargo test -p deja-replay-agent && cargo test -p deja-orchestrator`
Expected: PASS (agent/orchestrator consume the same public fns).

```bash
git add crates/deja-replay-core/src/ingest.rs crates/deja/src/lib.rs
git commit -m "feat(deja-replay-core): typed collate pipeline over DejaRecord"
```

---

### Task 6: Chart value for replay-side graph recording

**Files:**
- Modify: `replay-sandbox/chart/values.yaml` (the `router:` block, after `envOverrides: {}`)
- Modify: `replay-sandbox/chart/templates/stack/router-configmap.yaml:68`

**Interfaces:**
- Produces: `router.dejaGraphRecording` chart value (string `"enabled"`/`"disabled"`, default `"enabled"`) rendered into `ROUTER__DEJA__RECORDING__GRAPH`. No Rust changes; the router build already consumes this env via `set_graph_recording_enabled`, and the agent already extracts graph nodes to `graph-replay.jsonl`.

- [ ] **Step 1: Add the value**

In `replay-sandbox/chart/values.yaml`, inside the `router:` block directly after
`envOverrides: {}`:

```yaml
  # Replay-side execution-graph capture (ROUTER__DEJA__RECORDING__GRAPH).
  # "enabled" records span-tree nodes onto the observed stream; the agent
  # extracts them to graph-replay.jsonl for the dashboard's GraphView tree.
  # Flip to "disabled" via DEJA_SANDBOX_EXTRA_VALUES for high-volume runs.
  dejaGraphRecording: "enabled"
```

- [ ] **Step 2: Template the configmap**

In `replay-sandbox/chart/templates/stack/router-configmap.yaml`, change line 68:

```yaml
  ROUTER__DEJA__RECORDING__GRAPH: {{ .Values.router.dejaGraphRecording | default "enabled" | quote }}
```

(The `default` filter guards extra-values files that null the key.)

- [ ] **Step 3: Verify the render**

Run (skip with a note if `helm` is not installed locally — then verification is
the reviewer rendering it wherever helm exists):

```bash
helm template test-run replay-sandbox/chart | grep ROUTER__DEJA__RECORDING__GRAPH
```
Expected: `ROUTER__DEJA__RECORDING__GRAPH: "enabled"`

```bash
helm template test-run replay-sandbox/chart --set router.dejaGraphRecording=disabled | grep ROUTER__DEJA__RECORDING__GRAPH
```
Expected: `ROUTER__DEJA__RECORDING__GRAPH: "disabled"`

- [ ] **Step 4: Commit**

```bash
git add replay-sandbox/chart/values.yaml replay-sandbox/chart/templates/stack/router-configmap.yaml
git commit -m "feat(replay-sandbox): chart value for replay-side graph recording, default enabled"
```

---

### Task 7: Full verification

**Files:** none (verification only).

- [ ] **Step 1: Workspace gate**

Run: `just verify`
Expected: fmt-check clean, clippy clean (`-D warnings`), all workspace tests pass.

- [ ] **Step 2: Web build against unchanged GraphView**

Run: `cd web && npm run build`
Expected: `tsc -b && vite build` succeeds (no API-shape change was made, so
this is a regression check only).

- [ ] **Step 3: End-to-end spot check (data flow)**

Run: `cargo test -p deja-replay-agent`
Expected: PASS — the agent tests assert `graph.jsonl` materialization from a
tape containing `record_kind: "graph_node"` lines (`lib.rs:1350-1363`), which
now flows through the typed pipeline.

- [ ] **Step 4: Commit anything outstanding, report**

No code expected here; if fmt/clippy fixes were needed, commit them:

```bash
git add -u crates
git commit -m "chore: fmt/clippy fixes from verification"
```

---

## Self-Review (done at plan-writing time)

- **Spec coverage:** lenient module → Task 1; ExecutionGraphNode → Task 2; BoundaryEvent/CallsiteIdentity/ObservedCall + extras → Task 3; IngestError/lines_dropped/NoEvents → Task 4; LandingEnvelope/ArtifactType normalizing deserialize/PayloadTag tag-wins/legacy rescue/typed dedup-sort-report + probe deletion → Task 5; chart flag + helm render check → Task 6; `just verify` + web build → Task 7. Extras on `ObservedCall` is deliberately omitted per spec discussion (never rides record tapes; has its own wire struct).
- **Type consistency:** `CollatedRecord { record, json }` + `run_id()/global_sequence()` used consistently in Tasks 4-5 test code; `IngestError` variants match between Tasks 4 and 5; lenient fn names (`u64_lenient` etc.) match between Tasks 1-3.
- **Placeholder scan:** `write_events` body in Task 5 is elided with "as Task 4" — acceptable because Task 4 fully specifies it (same file, sequential tasks); no TBDs otherwise.
