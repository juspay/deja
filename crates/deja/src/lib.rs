//! Déjà — deterministic record/replay for service boundaries.
//!
//! Annotate a boundary with one of the feature-gated attribute macros
//! (`deja::redis`, `deja::id`, `deja::time`, `deja::http`, `deja::boundary`,
//! the `#[deja::recordable]` trait decorator, or framework-specific helper
//! modules such as [`db`]): on record, every call emits a [`BoundaryEvent`]
//! (args, result, correlation id, callsite identity) through the installed
//! [`RuntimeHook`];
//! on replay (`DEJA_MODE=replay`), the recorded result is substituted in
//! place of the live call and an `ObservedCall` is emitted for divergence
//! scoring.
//!
//! This crate is the facade: it re-exports the macros (`deja-derive`), the
//! recording/replay runtime (`deja-record`), correlation context helpers
//! (`deja-context`), and provides the payload-normalization helpers macros
//! expand against ([`value`], [`http`], [`db`]). Generated code reaches the
//! runtime through [`__private`].

pub use deja_derive::recordable;
pub use deja_derive::{boundary, http, id, instrument, redis, time};

/// Re-export the per-request recording sampling gate. The host (e.g. Hyperswitch)
/// resolves *whether* to record from Superposition at ingress and pushes the
/// decision here; Déjà only consumes it — `Skip` makes the recording hook a
/// no-op for the request, `Record` records as usual. With no decision the gate
/// currently records by default; the opt-in flip (default-skip, so the sampler's
/// own Superposition read self-excludes) is staged for the sampler-boundary work.
pub use deja_context::{
    clear_recording_decision, recording_decision, recording_decision_for_current,
    set_recording_decision, RecordDecision,
};
/// Re-export lookup-table replay primitives (hybrid architecture: in-process
/// lookup with per-site ReplayStrategy selecting Execute vs Substitute).
pub use deja_runtime::replay::{
    addresses_for, canonical_args_hash, Address, FileObservedSink, InMemoryObservedSink,
    KeyStamper, LocalFileLookupSource, LookupEntry, LookupKey, LookupTable, LookupTableHook,
    LookupTableSource, ObservedCall, ObservedCallSink, StateKey, StateKeyParseError,
};
pub use deja_runtime::replay::{boundary_execute_mode_for, replay_strategy_to_execute_mode};
/// Re-export the generic seed-plan pipeline (pure builder, diverged-read
/// classification, ambient template) so the harness materializes seeds from
/// explicit event read/write captures.
pub use deja_runtime::replay::{
    build_seed_plan, AmbientTemplate, ReadClassification, SeedEntry, SeedOrigin, SeedPlan,
};
/// Re-export the correlation-propagation tracing layer, which mirrors the ingress
/// `request_id` span field into deja-context so spawned-task boundary events
/// inherit the request correlation.
pub use deja_runtime::DejaCorrelationLayer;
/// Convenience re-export for the hook trait (needed by generated delegation).
pub use deja_runtime::DejaHook;
/// Re-export the execution graph tracing layer for framework logger setup.
pub use deja_runtime::ExecutionGraphLayer;
/// Re-export semantic recording primitives so downstream crates only need
/// one `deja` dependency.
pub use deja_runtime::{
    flush_global_hook, fork_span, global_hook_from_env, hook_from_env,
    installed_runtime_hook, spawn_fork, AsyncRecordWriter,
    BoundaryEvent, CompositeSink, DejaRecord, DisabledHook, EventBuilder, Fidelity, GraphNodeSink,
    JsonlSink, LazyEventFinalizer, MarkerKind, Provenance, RecordSink, RecordedOutput,
    RecordingHook, SinkPolicy, WriterConfig, WriterStatsSnapshot, CURRENT_EVENT_SCHEMA_VERSION,
};
/// Re-export callsite identity and runtime hook primitives for the
/// `DEJA_MODE=record|replay` foundation.
pub use deja_runtime::{
    flush_global_runtime_hook, global_runtime_hook_from_env, replay_is_active,
    runtime_hook_from_env, runtime_mode, set_global_runtime_hook, stable_callsite_hash,
    CallsiteIdentity, CallsiteSource, ExecuteMode, ExecuteShadowToken, ReplayLookup, RuntimeHook,
    RuntimeMode,
};
/// Re-export replay primitives so `deja::*` consumers get the full replay API.
pub use deja_runtime::{
    ArgMismatchPolicy, Divergence, DivergenceKind, ReplayConfig, ReplayHook, ReplayReport,
};
/// Re-export the declarative boundary primitives: the per-site
/// [`ReplayStrategy`] enum selects Execute or Substitute behavior, while
/// [`BoundarySemantics`] carries that declaration and the replay helpers map it
/// to the runtime `ExecuteMode`. [`BoundaryDeclaration`] and its enums are
/// metadata-only today; seed planning/reporting will consume them as the DSL
/// grows.
pub use deja_runtime::{
    BoundaryDeclaration, BoundarySemantics, CanonRef, CodecRef, EffectKind, OperationKind,
    Reconstructed, ReplayStrategy, ReturnSemantics,
};

/// The deja library version, for sinks that stamp provenance on the wire
/// (the recording envelope's `code.deja_version`).
pub const PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// During REPLAY only, the per-correlation namespace used to isolate each test
/// case's store (R1: per-correlation key-prefix). Returns `Some(correlation_id)`
/// iff replay is active AND a correlation is in scope; `None` during record/no-op
/// or when no correlation is set, so callers leave physical keys untouched outside
/// replay. A stateful boundary (e.g. the redis seam's `add_prefix`) prepends this
/// to the physical key so each correlation's seeded store stays isolated — no
/// cross-case collisions and no read-modify-write double-apply. The harness seeds
/// each correlation under the SAME `{correlation}:{physical}` namespace.
pub fn replay_key_namespace() -> Option<String> {
    if replay_is_active() {
        deja_context::current_correlation_id()
    } else {
        None
    }
}

/// The ambient correlation id (request / test-case identity) currently in scope,
/// or `None` outside any correlation. UNCONDITIONAL — unlike
/// [`replay_key_namespace`] it is not replay-gated. Instrumented boundaries call
/// this ONCE at boundary-build time to CAPTURE the correlation and thread it
/// explicitly (stamped into the `QuerySpec`, and used to derive the isolation
/// schema) instead of re-reading the ambient thread-local at query-execution
/// time — which can be stale on write paths that run off the request's span.
#[must_use]
pub fn current_correlation_id() -> Option<String> {
    deja_context::current_correlation_id()
}

/// The per-correlation **pg schema** name used for DB isolation during replay
/// (R1: schema-per-correlation + per-checkout `SET search_path`). A pure,
/// deterministic transform of a correlation id into a valid Postgres identifier:
/// `deja_` + a sanitized prefix + a 16-hex FNV-1a suffix of the full id, so it is
/// always ≤ 63 chars and distinct correlations never collide even after
/// truncation. BOTH sides must agree: the harness creates + seeds
/// `db_schema_for(correlation)`, and the router sets `search_path` to
/// `db_schema_for(current correlation)` on each connection lease.
pub fn db_schema_for(correlation: &str) -> String {
    let mut name = String::from("deja_");
    for ch in correlation.chars().take(40) {
        if ch.is_ascii_alphanumeric() {
            name.push(ch.to_ascii_lowercase());
        } else {
            name.push('_');
        }
    }
    // FNV-1a of the full correlation → collision-resistant suffix.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in correlation.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    name.push('_');
    name.push_str(&format!("{hash:016x}"));
    name
}

/// The `SET search_path` statement that routes a leased pg connection to the
/// active correlation's schema during replay (R1: schema-per-correlation DB
/// isolation). `Some(sql)` iff replay is active with a correlation in scope; the
/// router runs it on every connection checkout (the only vendor-side DB change).
///
/// Uses `"<schema>", public` so the (fully cloned) per-correlation tables resolve
/// FIRST — keeping writes isolated — while shared sequences / functions /
/// extensions still resolve via `public`. When the harness has NOT created the
/// schema (a run without DB isolation), the path simply falls back to `public`,
/// so it is always safe to emit during replay — no env gate, no behavior change.
pub fn replay_search_path_sql() -> Option<String> {
    let correlation = replay_key_namespace()?;
    Some(format!(
        "SET search_path TO \"{}\", public",
        db_schema_for(&correlation)
    ))
}

/// The `SET search_path` statement for an EXPLICIT correlation (R1 replay DB
/// isolation). Unlike [`replay_search_path_sql`], this does NOT read the ambient
/// thread-local — the caller supplies the correlation from a reliable, request-
/// scoped source (e.g. the store's `request_id`), which avoids the checkout-time
/// bleed when connection acquisition resumes off the request's correlation span.
/// The library owns the SQL shape; the vendor routing hook just applies it.
#[must_use]
pub fn replay_search_path_sql_for(correlation: &str) -> String {
    format!(
        "SET search_path TO \"{}\", public",
        db_schema_for(correlation)
    )
}

/// Small JSON helpers shared by framework-specific boundary hooks.
/// Capture any value for the tape with graceful degradation, resolved at
/// compile time via autoref specialization:
///
/// 1. `Serialize` → structured serde JSON (lossless, the preferred shape);
/// 2. else `Debug`  → `{"debug": "…"}` (lossy, tagged as such);
/// 3. else          → `{"deja_opaque_type": "…"}` (type name only).
///
/// This is the ONE calling convention for argument capture — the boundary
/// macros emit it for every inferred argument and `fields(...)` expression, so
/// instrumenting a boundary never fails to compile because of a non-serde
/// argument, and a hand-written `args = { … }` block is never needed just to
/// dodge a `Serialize` bound.
#[macro_export]
macro_rules! capture {
    ($value:expr) => {{
        #[allow(unused_imports)]
        use $crate::value::{CaptureDebug as _, CaptureOpaque as _, CaptureSerde as _};
        (&&$crate::value::Capture(&$value)).deja_capture()
    }};
}

pub mod value {
    use std::fmt::Debug;

    /// Receiver wrapper for the autoref-specialization capture below. Never
    /// constructed directly — use the [`crate::capture!`] macro, which is the
    /// single calling convention shared by hand-written sites and the code the
    /// boundary macros emit.
    pub struct Capture<'a, T: ?Sized>(pub &'a T);

    // Manual Copy/Clone: a derive would demand `T: Clone`, but `Capture` only
    // holds a reference and must stay `Copy` for ANY `T` (the last-resort
    // by-value arm below moves it out from behind the macro's `&&` receiver).
    impl<T: ?Sized> Clone for Capture<'_, T> {
        fn clone(&self) -> Self {
            *self
        }
    }
    impl<T: ?Sized> Copy for Capture<'_, T> {}

    /// Highest-priority capture: structured serde JSON. Selected whenever the
    /// value implements `Serialize`. A runtime serialization failure (e.g. a
    /// map with non-string keys) records a tagged marker instead of a silent
    /// `null`, so an unserializable arg is distinguishable on the tape.
    pub trait CaptureSerde {
        fn deja_capture(&self) -> serde_json::Value;
    }
    impl<T: serde::Serialize + ?Sized> CaptureSerde for &Capture<'_, T> {
        fn deja_capture(&self) -> serde_json::Value {
            serde_json::to_value(self.0).unwrap_or_else(|_| {
                serde_json::json!({
                    "deja_unserializable": std::any::type_name::<T>(),
                })
            })
        }
    }

    /// Middle-priority capture: the value is not `Serialize` but is `Debug`.
    /// Records the tagged Debug rendering — lossy but honest about being so
    /// (the `{"debug": …}` shape is what fidelity reporting keys on).
    pub trait CaptureDebug {
        fn deja_capture(&self) -> serde_json::Value;
    }
    impl<T: Debug + ?Sized> CaptureDebug for Capture<'_, T> {
        fn deja_capture(&self) -> serde_json::Value {
            serde_json::json!({
                "debug": format!("{:?}", self.0),
            })
        }
    }

    /// Last-resort capture: neither `Serialize` nor `Debug`. Records only the
    /// type name (same marker shape the `recordable(opaque)` delegate uses), so
    /// instrumenting a boundary can never be blocked by an opaque argument.
    pub trait CaptureOpaque {
        fn deja_capture(self) -> serde_json::Value;
    }
    impl<T: ?Sized> CaptureOpaque for Capture<'_, T> {
        fn deja_capture(self) -> serde_json::Value {
            serde_json::json!({
                "deja_opaque_type": std::any::type_name::<T>(),
            })
        }
    }

    /// Capture the full Rust debug representation of a value.
    pub fn debug<T: Debug + ?Sized>(value: &T) -> serde_json::Value {
        serde_json::json!({
            "debug": format!("{value:?}"),
        })
    }

    /// Capture a value as structured serde JSON (the v1 "args via serde"
    /// contract). Used by the boundary macro to record inferred arguments as
    /// structured data instead of a lossy Rust Debug string. Falls back to
    /// JSON-null if the value cannot be serialized (it never panics, so a
    /// serialize failure can't take down an instrumented call site).
    pub fn serialize<T: serde::Serialize + ?Sized>(value: &T) -> serde_json::Value {
        serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
    }

    /// Capture the full Rust debug representation of an error.
    pub fn error_debug<T: Debug + ?Sized>(error: &T) -> serde_json::Value {
        serde_json::json!({
            "debug": format!("{error:?}"),
        })
    }

    /// Capture a generic function return value as debug JSON and infer whether
    /// a `Result`-like value is an error from its standard debug shape.
    pub fn result_debug<T: Debug + ?Sized>(value: &T) -> (serde_json::Value, bool) {
        let debug = format!("{value:?}");
        let is_error = debug.starts_with("Err(") || debug.starts_with("Err {");
        (
            serde_json::json!({
                "debug": debug,
                "kind": if is_error { "error" } else { "value" },
            }),
            is_error,
        )
    }

    /// Capture a function return value LOSSLESSLY via `serde` for replay
    /// substitution. Unlike [`result_debug`] (which captures an unrecoverable
    /// Debug string), the produced JSON round-trips: replay can
    /// `serde_json::from_value` it back into the original type and return it
    /// without executing the real call. The boolean marks `Result::Err` using
    /// serde's `{"Err": …}` shape; non-`Result` values are never errors.
    ///
    /// Requires the value to implement `serde::Serialize`; the macro only emits
    /// a call to this for boundaries opted into replay (`#[deja::…(replay)]`).
    pub fn result_serialize<T: serde::Serialize + ?Sized>(value: &T) -> (serde_json::Value, bool) {
        let json = serde_json::to_value(value).unwrap_or(serde_json::Value::Null);
        let is_error = matches!(&json, serde_json::Value::Object(map) if map.contains_key("Err"));
        (json, is_error)
    }

    /// Lossless **Ok-only** recording for `Result`-returning boundaries whose
    /// error type is NOT serde-serializable (e.g. `error_stack::Report`). The
    /// OK value is recorded via `to_value` so replay can reconstruct it; an
    /// `Err` is recorded as a non-reconstructable sentinel (`{"deja_err": …}`)
    /// and marked `is_error`. On replay, `ResultOkCodec` treats that sentinel as
    /// reconstruction failure, so `Substitute` fail-stops instead of falling
    /// through to a live boundary.
    pub fn result_serialize_ok<T: serde::Serialize, E: std::fmt::Debug>(
        result: &Result<T, E>,
    ) -> (serde_json::Value, bool) {
        match result {
            Ok(value) => (
                serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
                false,
            ),
            Err(error) => (
                serde_json::json!({ "deja_err": format!("{error:?}") }),
                true,
            ),
        }
    }

    /// Versioned, structured record of a database boundary `Result`.
    ///
    /// Unlike [`result_serialize_ok`] (which records errors as an unrecoverable
    /// Debug-string sentinel, `{"deja_err": …}`), this captures the error in a
    /// STRUCTURED form: a stable `kind` discriminant (e.g. `"NotFound"`,
    /// `"UniqueViolation"`, `"Other"`) plus the human-readable `message`. Replay
    /// then matches on the `kind` discriminant rather than string-scanning a
    /// Debug blob, which is robust to message-text drift.
    ///
    /// IMPORTANT: the `Ok` payload is held as a raw `serde_json::Value` (NOT a
    /// typed generic) on purpose. The Kafka→Vector→MinIO transport serializes
    /// integers larger than `i64::MAX` as JSON STRINGS; a bare `u64` struct
    /// field would fail to round-trip through that path. A `serde_json::Value`
    /// tolerates a number that arrives back as either a number or a string.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    #[serde(tag = "result")]
    pub enum DejaDatabaseResultPayload {
        Ok {
            value: serde_json::Value,
            type_name: String,
        },
        Err {
            kind: String,
            message: String,
        },
    }

    /// The canonical redis boundary value, shared by the record side (via the
    /// codec), replay, and the seeder — the redis analogue of
    /// [`DejaDatabaseResultPayload`], which the DB path already shares.
    ///
    /// # Why this exists
    ///
    /// The router records a redis result as an externally-tagged enum, so the
    /// wire JSON is a single-key object naming the variant (`{"BulkString":[…]}`).
    /// That enum is defined PRIVATELY in the vendor's `redis_interface`, and in
    /// TWO backend dialects — `redis_rs` (`BulkString`/`SimpleString`/`Int`) and
    /// `fred` (`Bytes`/`String`/`Integer`) — for the same concepts. Consumers
    /// outside that crate (the seeder, replay tooling) otherwise have to re-parse
    /// the JSON by string-matching those tags, which silently rots the moment a
    /// variant is added. This is the one canonical, exhaustively-matchable type;
    /// `#[serde(alias)]` folds the two dialects into one, so a single
    /// `serde_json::from_value::<RedisWireValue>` decodes either backend.
    ///
    /// # Scope
    ///
    /// Only the SCALAR variants a plain redis string read (GET-family) can return
    /// are modelled — those are the values the seeder materializes. Non-scalar
    /// RESP3 shapes (arrays, maps, sets, push, verbatim, …) are deliberately
    /// absent: deserializing one fails, and the seeder treats that failure as an
    /// explicit, logged "cannot represent as a string SET" rather than guessing.
    /// The compiler enforces the seeder handles every variant here (no wildcard),
    /// so adding a scalar variant is a build error, not a 2am corruption.
    // No `Eq`: the `Double(f64)` variant is only `PartialEq`.
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    pub enum RedisWireValue {
        /// A cache miss / nil. Filtered upstream by the miss check, never seeded.
        Null,
        /// A signed integer, returned as its decimal ASCII form.
        /// (`fred` names this variant `Integer`.)
        #[serde(alias = "Integer")]
        Int(i64),
        /// A byte string — the dominant redis GET hit. The bytes ARE the value
        /// redis holds (UUIDs, tokens, serialized configs). (`fred`: `Bytes`.)
        #[serde(alias = "Bytes")]
        BulkString(Vec<u8>),
        /// An inline UTF-8 string. (`fred` names this variant `String`.)
        #[serde(alias = "String")]
        SimpleString(String),
        /// A floating-point number, returned in its recorded textual form.
        Double(f64),
        /// A boolean.
        Boolean(bool),
    }

    impl RedisWireValue {
        /// The raw string redis returns for this value, i.e. what a seed
        /// `SET key <value>` must write so the replayed router reads it back
        /// unchanged.
        ///
        /// `BulkString` bytes are decoded lossily to UTF-8: real payment values
        /// (UUIDs, tokens, JSON configs) are UTF-8, and the seed transport is a
        /// string-argument `redis-cli SET`; a genuinely binary value is a
        /// pre-existing limitation of that transport, not this decode. `Null`
        /// yields `None` (nothing to seed).
        pub fn to_redis_string(&self) -> Option<String> {
            match self {
                Self::Null => None,
                Self::Int(n) => Some(n.to_string()),
                Self::BulkString(bytes) => {
                    Some(String::from_utf8_lossy(bytes).into_owned())
                }
                Self::SimpleString(s) => Some(s.clone()),
                Self::Double(d) => Some(d.to_string()),
                Self::Boolean(b) => Some(if *b { "1" } else { "0" }.to_owned()),
            }
        }
    }

    /// A versioned envelope around [`DejaDatabaseResultPayload`].
    ///
    /// Keeping `version` separate lets the recorded shape evolve without
    /// breaking older recordings; replay can branch on `version` if/when the
    /// payload layout changes.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    pub struct DejaDatabaseResult {
        pub version: u8,
        #[serde(flatten)]
        pub payload: DejaDatabaseResultPayload,
    }

    impl DejaDatabaseResult {
        /// Current on-disk format version.
        pub const VERSION: u8 = 1;

        pub fn ok(value: serde_json::Value, type_name: impl Into<String>) -> Self {
            Self {
                version: Self::VERSION,
                payload: DejaDatabaseResultPayload::Ok {
                    value,
                    type_name: type_name.into(),
                },
            }
        }

        pub fn err(kind: impl Into<String>, message: impl Into<String>) -> Self {
            Self {
                version: Self::VERSION,
                payload: DejaDatabaseResultPayload::Err {
                    kind: kind.into(),
                    message: message.into(),
                },
            }
        }
    }

    /// Lossless, STRUCTURED recording for database boundaries.
    ///
    /// Emits the [`DejaDatabaseResult`] shape: an `Ok` records the value via
    /// `to_value` (so replay reconstructs it) tagged with its Rust `type_name`;
    /// an `Err` records a structured `{kind, message}` derived by the caller's
    /// `extract_kind` closure (which knows the concrete error type and can map
    /// it to a stable discriminant). The returned bool marks `Result::Err`.
    ///
    /// This replaces [`result_serialize_ok`] for the DB boundary ONLY; non-DB
    /// `replay_ok` boundaries (e.g. redis) keep using `result_serialize_ok`.
    pub fn result_serialize_db<T, E>(
        result: &Result<T, E>,
        extract_kind: impl Fn(&E) -> (String, String),
    ) -> (serde_json::Value, bool)
    where
        T: serde::Serialize,
    {
        let record = match result {
            Ok(value) => DejaDatabaseResult::ok(
                serde_json::to_value(value).unwrap_or(serde_json::Value::Null),
                std::any::type_name::<T>(),
            ),
            Err(error) => {
                let (kind, message) = extract_kind(error);
                DejaDatabaseResult::err(kind, message)
            }
        };
        let json = serde_json::to_value(&record).unwrap_or(serde_json::Value::Null);
        (json, result.is_err())
    }

    /// Capture raw bytes without redaction or truncation.
    pub fn bytes(bytes: &[u8]) -> serde_json::Value {
        let text = std::str::from_utf8(bytes).ok();
        let json = text.and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok());

        serde_json::json!({
            "captured": true,
            "bytes_len": bytes.len(),
            "utf8": text.is_some(),
            "text": text,
            "json": json,
            "raw_bytes": bytes.to_vec(),
        })
    }

    /// Capture optional bytes while preserving why capture was unavailable.
    pub fn optional_bytes(bytes: Option<&[u8]>, missing_reason: &'static str) -> serde_json::Value {
        bytes.map_or_else(
            || {
                serde_json::json!({
                    "captured": false,
                    "reason": missing_reason,
                })
            },
            self::bytes,
        )
    }
}

/// Result-type codecs (#27 / G2): how a boundary's return value is CAPTURED to the
/// tape and RECONSTRUCTED on replay. The two methods map exactly onto the dispatch
/// seam's `extract` (capture) and `reconstruct` closures, so the boundary macro can
/// emit `<C as ReplayCodec>::capture` / `::reconstruct` in place of the ad-hoc
/// `replay` / `replay_ok` / `replay_with` flags.
///
/// A boundary selects a codec via the macro's `codec = <Codec>` knob (or a kit
/// default). The two built-in names `SerdeCodec` and `ResultOkCodec` are
/// recognized by the macro and expand to the proven whole-value / Ok-only serde
/// codegen, so they need no generic arguments at the call site. Any OTHER
/// `codec = path` is a custom `ReplayCodec` impl whose `Value` must equal the
/// boundary's return type — this is how non-serde results (an HTTP response,
/// the DB result envelope) plug in without the bespoke `replay_with` flag.
pub mod codec {
    use std::marker::PhantomData;

    /// The capture/reconstruct contract for one boundary return type.
    pub trait ReplayCodec {
        /// The boundary's return type (what `dispatch` resolves `T` to).
        type Value;
        /// Record side: serialize the value to tape JSON and flag `Result::Err`.
        fn capture(value: &Self::Value) -> (serde_json::Value, bool);
        /// Replay side: rebuild the value from recorded JSON. `None` means the
        /// capture could not be reconstructed; the macro maps it to
        /// `Reconstructed::Failed`.
        fn reconstruct(recorded: serde_json::Value) -> Option<Self::Value>;
    }

    /// Whole-value serde codec — the canonical lossless codec for any return type
    /// that round-trips through serde. The macro maps the bare name `SerdeCodec` to
    /// this (no generic argument needed); it can also be named explicitly via
    /// `replay_codec = ::deja::codec::SerdeCodec<MyType>` for a custom site.
    pub struct SerdeCodec<R>(PhantomData<fn() -> R>);

    impl<R> ReplayCodec for SerdeCodec<R>
    where
        R: serde::Serialize + serde::de::DeserializeOwned,
    {
        type Value = R;

        fn capture(value: &R) -> (serde_json::Value, bool) {
            crate::value::result_serialize(value)
        }

        fn reconstruct(recorded: serde_json::Value) -> Option<R> {
            serde_json::from_value::<R>(recorded).ok()
        }
    }

    /// Typed `Result` codec — the uniform "recording threw ⇒ replay throws"
    /// contract. Captures the `Ok` arm losslessly OR the error **context** (the
    /// `error_stack::Report`'s current context — the value recovery code
    /// branches on), and reconstructs either arm: a recorded error replays as
    /// `Err(report!(E))` carrying the SAME typed context the recording threw.
    /// Report attachments/backtraces are diagnostics and do not round-trip.
    ///
    /// The wire envelope is byte-compatible with [`crate::value::DejaDatabaseResult`]
    /// (`{version, result: "Ok"|"Err", value/type_name | kind/message}`), so
    /// existing envelope consumers (seed planner, `db::row_state_keys`' visitor)
    /// keep parsing without change; for a fieldless error enum the serialized
    /// `kind` is the variant name string, exactly as the hand-rolled DB mapping
    /// produced.
    ///
    /// Requires the `error-stack` cargo feature.
    #[cfg(feature = "error-stack")]
    pub struct ResultCodec<T, E>(PhantomData<fn() -> (T, E)>);

    #[cfg(feature = "error-stack")]
    impl<T, E> ReplayCodec for ResultCodec<T, E>
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
        E: serde::Serialize + serde::de::DeserializeOwned + error_stack::Context,
    {
        type Value = Result<T, error_stack::Report<E>>;

        fn capture(value: &Self::Value) -> (serde_json::Value, bool) {
            match value {
                Ok(inner) => (
                    serde_json::json!({
                        "version": crate::value::DejaDatabaseResult::VERSION,
                        "result": "Ok",
                        "value": serde_json::to_value(inner)
                            .unwrap_or(serde_json::Value::Null),
                        "type_name": std::any::type_name::<T>(),
                    }),
                    false,
                ),
                Err(report) => (
                    serde_json::json!({
                        "version": crate::value::DejaDatabaseResult::VERSION,
                        "result": "Err",
                        // For a fieldless enum this serializes to the bare
                        // variant-name string ("NotFound"), keeping the wire
                        // `kind` identical to the legacy hand-rolled mapping.
                        "kind": serde_json::to_value(report.current_context())
                            .unwrap_or(serde_json::Value::Null),
                        "message": format!("{report:?}"),
                    }),
                    true,
                ),
            }
        }

        fn reconstruct(recorded: serde_json::Value) -> Option<Self::Value> {
            let object = recorded.as_object()?;
            match object.get("result").and_then(serde_json::Value::as_str) {
                Some("Ok") => {
                    let value = object.get("value")?;
                    let inner: T = serde_json::from_value(value.clone()).ok()?;
                    Some(Ok(inner))
                }
                Some("Err") => {
                    let kind = object.get("kind")?;
                    // A `kind` that no longer names a variant of the candidate's
                    // error type is a reconstruction FAILURE (never a silent
                    // fabrication) — the seam fail-stops on it.
                    let context: E = serde_json::from_value(kind.clone()).ok()?;
                    Some(Err(error_stack::report!(context)))
                }
                _ => None,
            }
        }
    }
}

/// Helpers for HTTP request/response boundary payloads.
pub mod http {
    /// Normalize headers as a map of header name to all observed values.
    pub fn headers<I, K, V>(headers: I) -> serde_json::Value
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        let mut output = serde_json::Map::new();
        for (name, value) in headers {
            // SHADOW GUARANTEE: this only ever inserts arrays, so the downcast
            // cannot realistically fail — but never panic on a recording path.
            // Skip a malformed entry rather than unwinding into the request.
            if let Some(array) = output
                .entry(name.into())
                .or_insert_with(|| serde_json::Value::Array(Vec::new()))
                .as_array_mut()
            {
                array.push(serde_json::Value::String(value.into()));
            }
        }
        serde_json::Value::Object(output)
    }

    /// Capture an HTTP body as text, parsed JSON when possible, and raw bytes.
    pub fn body(bytes: &[u8]) -> serde_json::Value {
        crate::value::bytes(bytes)
    }

    /// Capture a missing HTTP body with a reason.
    pub fn missing_body(reason: &'static str) -> serde_json::Value {
        serde_json::json!({
            "captured": false,
            "reason": reason,
        })
    }
}

/// Helpers for database boundary payloads.
pub mod db {
    use std::{collections::HashMap, fmt::Debug};

    /// Build the database request payload common to Diesel helpers. Borrows
    /// its inputs so boundary-attribute exprs can evaluate it eagerly while the
    /// same values remain available to the later capture closures.
    pub fn args(
        operation: &'static str,
        table: &str,
        sql: &str,
        inputs: &serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "operation": operation,
            "table": table,
            "sql": sql,
            "inputs": inputs,
        })
    }

    /// Build the typed v1 DB query fallback state key wire string.
    pub fn query_state_key(
        operation: &str,
        table: &str,
        sql: &str,
        inputs: &serde_json::Value,
    ) -> String {
        deja_runtime::replay::db_query_state_key(operation, table, sql, inputs).to_wire()
    }

    /// Return the DB table from a structured DB args/request envelope.
    pub fn table_from_event_args(args: &serde_json::Value) -> Option<&str> {
        deja_runtime::replay::db_table_from_event_args(args)
    }

    /// Metadata known by a producer for one database column. Matching is by
    /// [`name`](Self::name); absent fields are left unknown on row images.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    pub struct DbColumnMetadata {
        pub name: String,
        pub type_oid: Option<u32>,
        pub type_name: Option<String>,
        pub nullable: Option<bool>,
    }

    /// One typed database column image. Metadata is optional because a producer
    /// may know only names/values; consumers must prefer present metadata and may
    /// fill gaps from a catalog.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    pub struct DbColumnImage {
        pub name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub type_oid: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub type_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub nullable: Option<bool>,
        pub value: serde_json::Value,
    }

    type MetadataByName<'a> = HashMap<&'a str, &'a DbColumnMetadata>;

    fn metadata_lookup(metadata: &[DbColumnMetadata]) -> MetadataByName<'_> {
        metadata
            .iter()
            .map(|column| (column.name.as_str(), column))
            .collect()
    }

    fn column_image_from_value(
        name: &str,
        value: &serde_json::Value,
        metadata: Option<&DbColumnMetadata>,
    ) -> DbColumnImage {
        DbColumnImage {
            name: name.to_owned(),
            type_oid: metadata.and_then(|column| column.type_oid),
            type_name: metadata.and_then(|column| column.type_name.clone()),
            nullable: metadata.and_then(|column| column.nullable),
            value: value.clone(),
        }
    }

    fn row_image_from_json_object_with_lookup(
        table: &str,
        row: &serde_json::Map<String, serde_json::Value>,
        metadata_by_name: &MetadataByName<'_>,
    ) -> Option<DbRowImage> {
        if row.is_empty() {
            return None;
        }
        let columns = row
            .iter()
            .map(|(name, value)| {
                column_image_from_value(name, value, metadata_by_name.get(name.as_str()).copied())
            })
            .collect();
        Some(DbRowImage::new(table, columns))
    }

    /// Typed database row image carried by `RecordedOutput::{result_image,pre_image}`.
    #[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq)]
    pub struct DbRowImage {
        pub deja_image: String,
        pub version: u8,
        pub table: String,
        pub columns: Vec<DbColumnImage>,
    }

    impl DbRowImage {
        pub const KIND: &'static str = "db_row";
        pub const VERSION: u8 = 1;

        pub fn new(table: impl Into<String>, columns: Vec<DbColumnImage>) -> Self {
            Self {
                deja_image: Self::KIND.to_string(),
                version: Self::VERSION,
                table: table.into(),
                columns,
            }
        }

        pub fn from_json_object(
            table: &str,
            row: &serde_json::Map<String, serde_json::Value>,
        ) -> Option<Self> {
            Self::from_json_object_with_metadata(table, row, &[])
        }

        pub fn from_json_object_with_metadata(
            table: &str,
            row: &serde_json::Map<String, serde_json::Value>,
            metadata: &[DbColumnMetadata],
        ) -> Option<Self> {
            let metadata_by_name = metadata_lookup(metadata);
            row_image_from_json_object_with_lookup(table, row, &metadata_by_name)
        }

        pub fn to_value(&self) -> serde_json::Value {
            serde_json::to_value(self).unwrap_or(serde_json::Value::Null)
        }
    }

    /// Build typed row images from one row object or an array of row objects.
    ///
    /// Existing callers use this compatibility helper and therefore continue to
    /// emit column names and values with metadata left as `None`.
    pub fn row_images(table: &str, value: &serde_json::Value) -> Vec<DbRowImage> {
        row_images_with_metadata(table, value, &[])
    }

    /// Build typed row images from one row object or an array of row objects,
    /// copying producer-supplied metadata into columns with matching names.
    pub fn row_images_with_metadata(
        table: &str,
        value: &serde_json::Value,
        metadata: &[DbColumnMetadata],
    ) -> Vec<DbRowImage> {
        let metadata_by_name = metadata_lookup(metadata);
        match value {
            serde_json::Value::Object(map) => {
                row_image_from_json_object_with_lookup(table, map, &metadata_by_name)
                    .into_iter()
                    .collect()
            }
            serde_json::Value::Array(items) => items
                .iter()
                .filter_map(|value| {
                    value.as_object().and_then(|map| {
                        row_image_from_json_object_with_lookup(table, map, &metadata_by_name)
                    })
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Convert row images into the compact event image payload.
    pub fn row_image_payload(table: &str, value: &serde_json::Value) -> Option<serde_json::Value> {
        row_image_payload_with_metadata(table, value, &[])
    }

    /// Convert metadata-backed row images into the compact event image payload.
    pub fn row_image_payload_with_metadata(
        table: &str,
        value: &serde_json::Value,
        metadata: &[DbColumnMetadata],
    ) -> Option<serde_json::Value> {
        let mut images = row_images_with_metadata(table, value, metadata);
        match images.len() {
            0 => None,
            1 => images.pop().map(|image| image.to_value()),
            _ => Some(serde_json::Value::Array(
                images.into_iter().map(|image| image.to_value()).collect(),
            )),
        }
    }

    /// Build a typed DB row key from one structured row/object when a pragmatic
    /// primary-key column is present.
    pub fn row_state_key(table: &str, row: &serde_json::Value) -> Option<crate::StateKey> {
        deja_runtime::replay::db_row_state_key(table, row)
    }

    /// Extract all typed DB row keys from a structured DB Ok value or row array.
    pub fn row_state_keys(table: &str, value: &serde_json::Value) -> Vec<crate::StateKey> {
        deja_runtime::replay::db_row_state_keys(table, value)
    }

    /// Which side(s) of state one DB operation touches. Mirrors the event
    /// builder's `state_read_to`/`state_write_to`/`state_touch_to` axes; each
    /// generic query helper declares its axis as a compile-time constant.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum StateAxis {
        /// Row-returning or scalar read (`find*`, `filter`, `count`).
        Read,
        /// Pure write (`insert`, `update`, `delete`).
        Write,
        /// Read-then-write (`update_with_results`, `update_by_id`,
        /// `delete_one_with_result`).
        Touch,
    }

    /// Derive row-exact READ keys from the rendered SQL's trailing bind list
    /// (diesel `debug_query` emits `… -- binds: [...]`). This is the explicit
    /// producer for reads whose RESULT carries no row — most importantly a
    /// NotFound read, which otherwise records an empty read set and starves
    /// seed planning of the read's identity.
    ///
    /// Conservative by construction: only equality binds on the table's known
    /// pragmatic primary-key column produce keys, and any bind list that does
    /// not parse yields no keys (never a guess).
    pub fn binds_read_keys(table: &str, sql: &str) -> Vec<String> {
        let Some(pk) = deja_runtime::replay::db_pk_column(table) else {
            return Vec::new();
        };
        let Some(binds_at) = sql.rfind(" -- binds: ") else {
            return Vec::new();
        };
        let (query, binds_raw) = sql.split_at(binds_at);
        let binds_raw = binds_raw.trim_start_matches(" -- binds: ").trim();
        // Diesel debug-prints binds as a bracketed list; strings/numbers/bools
        // are JSON-compatible. Anything richer fails the parse and yields no
        // keys rather than a fabricated one.
        let Ok(binds) = serde_json::from_str::<Vec<serde_json::Value>>(binds_raw) else {
            return Vec::new();
        };

        let needle = format!("\"{pk}\" = $");
        let mut keys = Vec::new();
        let mut cursor = 0;
        while let Some(found) = query[cursor..].find(&needle) {
            let digits_start = cursor + found + needle.len();
            let digits: String = query[digits_start..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            cursor = digits_start;
            let Ok(position) = digits.parse::<usize>() else {
                continue;
            };
            let Some(value) = position.checked_sub(1).and_then(|i| binds.get(i)) else {
                continue;
            };
            let row = serde_json::json!({ pk: value });
            if let Some(key) = deja_runtime::replay::db_row_state_key(table, &row) {
                let wire = key.to_wire();
                if !keys.contains(&wire) {
                    keys.push(wire);
                }
            }
        }
        keys
    }

    /// Explicit producer API for one DB query result: the full
    /// [`crate::RecordedOutput`] — codec envelope + typed row state keys +
    /// row image — routed by the operation's [`StateAxis`].
    ///
    /// This is the fold replacement for the hand-rolled per-op capture
    /// closures: the boundary macro's `result = …` escape hatch calls it with
    /// the fn's own `table`/`sql` args. The recorder itself never infers state
    /// (`finish` stamps only explicit captures); this helper IS the explicit
    /// producer, and improving its derivation (e.g. the binds parser) is a
    /// deja-side change needing no vendor edit.
    ///
    /// Requires the `error-stack` cargo feature.
    #[cfg(feature = "error-stack")]
    pub fn recorded_output<T, E>(
        axis: StateAxis,
        table: &str,
        sql: &str,
        result: &Result<T, error_stack::Report<E>>,
    ) -> crate::RecordedOutput
    where
        T: serde::Serialize + serde::de::DeserializeOwned,
        E: serde::Serialize + serde::de::DeserializeOwned + error_stack::Context,
    {
        use crate::codec::{ReplayCodec, ResultCodec};

        let (envelope, is_error) = ResultCodec::<T, E>::capture(result);
        let ok_value = (!is_error)
            .then(|| envelope.get("value").cloned())
            .flatten();
        let mut output = crate::RecordedOutput::new(envelope, is_error);

        // Row-exact keys from rows the result actually carried.
        let row_keys: Vec<String> = ok_value
            .as_ref()
            .map(|value| {
                row_state_keys(table, value)
                    .into_iter()
                    .map(|key| key.to_wire())
                    .collect()
            })
            .unwrap_or_default();
        // Row-exact keys from the query's own binds — covers reads whose
        // result carries no row (NotFound) and writes addressed by PK.
        let bind_keys = binds_read_keys(table, sql);

        for key in row_keys.iter().chain(bind_keys.iter()) {
            output = match axis {
                StateAxis::Read => output.with_read_key(key.clone()),
                StateAxis::Write => output.with_write_key(key.clone()),
                StateAxis::Touch => output
                    .with_read_key(key.clone())
                    .with_write_key(key.clone()),
            };
        }

        if let Some(value) = ok_value {
            if let Some(image) = row_image_payload(table, &value) {
                output = output.with_result_image(image);
            }
        }
        output
    }

    /// Metadata for a database query boundary.
    #[derive(Debug, Clone)]
    pub struct QuerySpec {
        pub boundary: &'static str,
        pub component: &'static str,
        pub operation: &'static str,
        pub table: String,
        pub sql: String,
        pub inputs: serde_json::Value,
        pub correlation_id: Option<String>,
        pub read_set: Vec<String>,
        pub write_set: Vec<String>,
        pub declaration: crate::BoundaryDeclaration,
    }

    impl QuerySpec {
        pub fn new(
            operation: &'static str,
            table: impl Into<String>,
            sql: impl Into<String>,
            inputs: serde_json::Value,
        ) -> Self {
            Self {
                boundary: "db",
                component: "db",
                operation,
                table: table.into(),
                sql: sql.into(),
                inputs,
                correlation_id: None,
                read_set: Vec::new(),
                write_set: Vec::new(),
                declaration: crate::BoundaryDeclaration::default().effect(crate::EffectKind::Db),
            }
        }

        pub fn component(mut self, component: &'static str) -> Self {
            self.component = component;
            self
        }

        pub fn boundary(mut self, boundary: &'static str) -> Self {
            self.boundary = boundary;
            self
        }

        pub fn correlation_id(mut self, correlation_id: Option<String>) -> Self {
            self.correlation_id = correlation_id;
            self
        }

        pub fn declaration(mut self, declaration: crate::BoundaryDeclaration) -> Self {
            self.declaration = declaration;
            self
        }

        pub fn operation_kind(mut self, op: crate::OperationKind) -> Self {
            self.declaration = self.declaration.operation(op);
            self
        }

        pub fn return_semantics(mut self, returns: crate::ReturnSemantics) -> Self {
            self.declaration = self.declaration.returns(returns);
            self
        }

        pub fn codec(mut self, codec: crate::CodecRef) -> Self {
            self.declaration = self.declaration.codec(codec);
            self
        }

        pub fn with_read_set(mut self, keys: Vec<String>) -> Self {
            self.read_set = keys;
            self
        }

        pub fn with_write_set(mut self, keys: Vec<String>) -> Self {
            self.write_set = keys;
            self
        }

        pub fn state_read_to(mut self, key: impl Into<String>) -> Self {
            self.read_set = vec![key.into()];
            self.write_set.clear();
            self
        }

        pub fn state_write_to(mut self, key: impl Into<String>) -> Self {
            self.read_set.clear();
            self.write_set = vec![key.into()];
            self
        }

        pub fn state_touch_to(mut self, key: impl Into<String>) -> Self {
            let key = key.into();
            self.read_set = vec![key.clone()];
            self.write_set = vec![key];
            self
        }
    }

    /// Coarse result shape to record for a generic database helper.
    #[derive(Debug, Clone, Copy)]
    pub enum QueryResultKind {
        Value,
        Rows,
        Optional,
        Count,
        Bool,
        Unit,
    }
}

/// Private implementation details used by the macro-generated code.
/// Not part of the public API — the `deja::*` attribute macros call these.
pub mod __private {
    pub use deja_context::current_correlation_id;
    // The single boundary-crossing seam the `#[deja::boundary]` family emits.
    // `dispatch` owns ALL replay/record/execute control flow internally, so the
    // macro names no replay-only operation. The older `replay_boundary` /
    // `boundary_execute_mode` / `execute_shadow_*` seams are re-exported only for
    // backward compatibility (they are `#[deprecated]` in deja-record and are
    // subsumed by `dispatch`).
    #[allow(deprecated)]
    pub use deja_runtime::{
        boundary_execute_mode, capture_is_active, current_span_path, dispatch, dispatch_async,
        dispatch_async_or_miss, execute_shadow_observe_boundary, execute_shadow_peek_boundary,
        fail_stop_execute_shadow_unavailable, fail_stop_substitute_miss, finish_boundary_event,
        next_boundary_occurrence, record_boundary_async, record_boundary_async_lazy,
        record_boundary_sync, record_boundary_sync_lazy, replay_boundary, replay_is_active,
        runtime_mode, stable_callsite_hash, BoundarySpec, CallsiteIdentity, CallsiteSource,
        CrossingObservation, ExecuteMode, ExecuteShadowToken, Reconstructed, RecordedOutput,
        RuntimeMode,
    };
    // Declarative boundary model: the per-site `ReplayStrategy` enum selects
    // Execute or Substitute behavior, and `BoundarySemantics` is the descriptor
    pub use deja_runtime::{
        BoundaryDeclaration, BoundarySemantics, CanonRef, CodecRef, EffectKind, OperationKind,
        ReplayStrategy, ReturnSemantics,
    };
}

#[cfg(test)]
mod capture_tests {
    //! Compile-time proof of the autoref-specialization priority order:
    //! Serialize beats Debug beats opaque, decided per concrete type.

    /// `Serialize` but NOT `Debug` → structured serde JSON.
    #[derive(serde::Serialize)]
    struct SerdeOnly {
        amount: u32,
    }

    /// `Debug` but NOT `Serialize` → tagged Debug rendering.
    #[derive(Debug)]
    struct DebugOnly {
        // Read only through the derived Debug rendering the test asserts on.
        #[allow(dead_code)]
        amount: u32,
    }

    /// Both → serde wins (the higher-priority arm).
    #[derive(serde::Serialize, Debug)]
    struct Both {
        amount: u32,
    }

    /// Neither → opaque type-name marker.
    struct Opaque;

    #[test]
    fn serialize_only_captures_structured_json() {
        let captured = crate::capture!(SerdeOnly { amount: 7 });
        assert_eq!(captured, serde_json::json!({"amount": 7}));
    }

    #[test]
    fn debug_only_captures_tagged_debug_string() {
        let captured = crate::capture!(DebugOnly { amount: 7 });
        assert_eq!(
            captured,
            serde_json::json!({"debug": "DebugOnly { amount: 7 }"})
        );
    }

    #[test]
    fn serialize_beats_debug_when_both_available() {
        let captured = crate::capture!(Both { amount: 7 });
        assert_eq!(captured, serde_json::json!({"amount": 7}));
    }

    #[test]
    fn opaque_captures_type_name_marker() {
        let captured = crate::capture!(Opaque);
        let marker = captured
            .get("deja_opaque_type")
            .and_then(|v| v.as_str())
            .expect("opaque marker present");
        assert!(marker.ends_with("Opaque"), "marker names the type");
    }

    #[test]
    fn references_capture_through_to_the_value() {
        let value = Both { amount: 9 };
        let by_ref = crate::capture!(&value);
        assert_eq!(by_ref, serde_json::json!({"amount": 9}));
    }

    #[test]
    fn runtime_serialize_failure_records_tagged_marker_not_null() {
        // A map with non-string keys serializes to an Err in serde_json.
        let bad: std::collections::HashMap<Vec<u8>, u32> =
            std::collections::HashMap::from([(vec![1u8], 1u32)]);
        let captured = crate::capture!(bad);
        assert!(
            captured.get("deja_unserializable").is_some(),
            "runtime to_value failure must be tagged, never a silent null: {captured}"
        );
    }
}

#[cfg(test)]
mod db_row_image_tests {
    use crate::db::{
        row_image_payload, row_image_payload_with_metadata, row_images, row_images_with_metadata,
        DbColumnImage, DbColumnMetadata, DbRowImage,
    };

    fn column<'a>(columns: &'a [DbColumnImage], name: &str) -> &'a DbColumnImage {
        columns
            .iter()
            .find(|column| column.name == name)
            .unwrap_or_else(|| panic!("missing column {name}"))
    }

    #[test]
    fn row_images_with_metadata_attach_matching_columns_only() {
        let value = serde_json::json!({
            "id": 42,
            "customer": "Ada",
        });
        let metadata = vec![
            DbColumnMetadata {
                name: "id".to_string(),
                type_oid: Some(23),
                type_name: Some("int4".to_string()),
                nullable: Some(false),
            },
            DbColumnMetadata {
                name: "not_in_row".to_string(),
                type_oid: Some(25),
                type_name: Some("text".to_string()),
                nullable: Some(true),
            },
        ];

        let images = row_images_with_metadata("users", &value, &metadata);

        assert_eq!(images.len(), 1);
        let image = &images[0];
        assert_eq!(image.deja_image, "db_row");
        assert_eq!(image.version, 1);
        assert_eq!(image.table, "users");
        assert_eq!(image.columns.len(), 2);

        let id = column(&image.columns, "id");
        assert_eq!(id.value, serde_json::json!(42));
        assert_eq!(id.type_oid, Some(23));
        assert_eq!(id.type_name.as_deref(), Some("int4"));
        assert_eq!(id.nullable, Some(false));

        let customer = column(&image.columns, "customer");
        assert_eq!(customer.value, serde_json::json!("Ada"));
        assert_eq!(customer.type_oid, None);
        assert_eq!(customer.type_name, None);
        assert_eq!(customer.nullable, None);
    }

    #[test]
    fn row_image_compatibility_helpers_leave_metadata_unknown() {
        let value = serde_json::json!({
            "id": "pay_123",
            "amount": 1200,
        });

        let images = row_images("payments", &value);

        assert_eq!(images.len(), 1);
        let image = &images[0];
        assert_eq!(image.table, "payments");
        assert_eq!(image.columns.len(), 2);
        for expected_name in ["id", "amount"] {
            let column = column(&image.columns, expected_name);
            assert_eq!(column.type_oid, None);
            assert_eq!(column.type_name, None);
            assert_eq!(column.nullable, None);
        }
        assert_eq!(
            column(&image.columns, "id").value,
            serde_json::json!("pay_123")
        );
        assert_eq!(
            column(&image.columns, "amount").value,
            serde_json::json!(1200)
        );

        let payload = row_image_payload("payments", &value).expect("single row payload");
        let payload_image: DbRowImage =
            serde_json::from_value(payload.clone()).expect("payload decodes to a row image");
        assert_eq!(payload_image.table, "payments");
        assert_eq!(
            column(&payload_image.columns, "id").value,
            serde_json::json!("pay_123")
        );
        assert_eq!(
            column(&payload_image.columns, "amount").value,
            serde_json::json!(1200)
        );

        let payload_columns = payload["columns"]
            .as_array()
            .expect("row image payload has columns");
        for raw_column in payload_columns {
            let raw_column = raw_column.as_object().expect("column payload is an object");
            assert!(!raw_column.contains_key("type_oid"));
            assert!(!raw_column.contains_key("type_name"));
            assert!(!raw_column.contains_key("nullable"));
        }
    }

    #[test]
    fn row_image_payload_with_metadata_keeps_only_object_array_items() {
        let value = serde_json::json!([
            { "id": 1, "status": "created" },
            null,
            "ignored",
            [],
            { "id": 2, "status": "captured" },
        ]);
        let metadata = vec![DbColumnMetadata {
            name: "status".to_string(),
            type_oid: Some(25),
            type_name: Some("text".to_string()),
            nullable: Some(false),
        }];

        let payload = row_image_payload_with_metadata("payments", &value, &metadata)
            .expect("object items produce row images");
        let rows: Vec<DbRowImage> =
            serde_json::from_value(payload).expect("multiple rows decode from payload array");

        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].table, "payments");
        assert_eq!(rows[1].table, "payments");

        let first_id = column(&rows[0].columns, "id");
        assert_eq!(first_id.value, serde_json::json!(1));
        assert_eq!(first_id.type_oid, None);
        assert_eq!(first_id.type_name, None);
        assert_eq!(first_id.nullable, None);
        let first_status = column(&rows[0].columns, "status");
        assert_eq!(first_status.value, serde_json::json!("created"));
        assert_eq!(first_status.type_oid, Some(25));
        assert_eq!(first_status.type_name.as_deref(), Some("text"));
        assert_eq!(first_status.nullable, Some(false));

        let second_id = column(&rows[1].columns, "id");
        assert_eq!(second_id.value, serde_json::json!(2));
        assert_eq!(second_id.type_oid, None);
        assert_eq!(second_id.type_name, None);
        assert_eq!(second_id.nullable, None);
        let second_status = column(&rows[1].columns, "status");
        assert_eq!(second_status.value, serde_json::json!("captured"));
        assert_eq!(second_status.type_oid, Some(25));
        assert_eq!(second_status.type_name.as_deref(), Some("text"));
        assert_eq!(second_status.nullable, Some(false));
    }
}

#[cfg(test)]
mod db_result_tests {
    use crate::value::{result_serialize_db, DejaDatabaseResult, DejaDatabaseResultPayload};

    /// `DejaDatabaseResult` round-trips through serde for the Ok variant,
    /// preserving the (possibly large-integer) value and its type name.
    #[test]
    fn ok_round_trips_through_serde() {
        let original = DejaDatabaseResult::ok(serde_json::json!(42), "usize");
        let encoded = serde_json::to_value(&original).expect("encode");
        // Shape is the flattened, versioned, externally-tagged envelope.
        assert_eq!(
            encoded,
            serde_json::json!({
                "version": 1,
                "result": "Ok",
                "value": 42,
                "type_name": "usize",
            })
        );
        let decoded: DejaDatabaseResult = serde_json::from_value(encoded).expect("decode");
        assert_eq!(decoded, original);

        // A value that arrives back as a STRING (the Kafka/Vector big-int
        // stringification case) must still decode, because `value` is a raw
        // `serde_json::Value`.
        let stringified = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": "18446744073709551615",
            "type_name": "u64",
        });
        let decoded: DejaDatabaseResult =
            serde_json::from_value(stringified).expect("decode stringified big int");
        match decoded.payload {
            DejaDatabaseResultPayload::Ok { value, type_name } => {
                assert_eq!(value, serde_json::json!("18446744073709551615"));
                assert_eq!(type_name, "u64");
            }
            _ => panic!("expected Ok payload"),
        }
    }

    /// `DejaDatabaseResult` round-trips through serde for each Err kind.
    #[test]
    fn err_round_trips_for_each_kind() {
        for kind in ["NotFound", "UniqueViolation", "Other"] {
            let original = DejaDatabaseResult::err(kind, format!("{kind} message"));
            let encoded = serde_json::to_value(&original).expect("encode");
            assert_eq!(
                encoded,
                serde_json::json!({
                    "version": 1,
                    "result": "Err",
                    "kind": kind,
                    "message": format!("{kind} message"),
                })
            );
            let decoded: DejaDatabaseResult = serde_json::from_value(encoded).expect("decode");
            assert_eq!(decoded, original);
        }
    }

    /// `result_serialize_db` emits the structured shape and flags errors.
    #[test]
    fn result_serialize_db_emits_structured_shape() {
        let ok: Result<u8, &str> = Ok(7);
        let (json, is_err) = result_serialize_db(&ok, |_| ("Other".to_string(), String::new()));
        assert!(!is_err);
        assert_eq!(json["result"], serde_json::json!("Ok"));
        assert_eq!(json["value"], serde_json::json!(7));

        let err: Result<u8, &str> = Err("not found in the database");
        let (json, is_err) =
            result_serialize_db(&err, |e| ("NotFound".to_string(), (*e).to_string()));
        assert!(is_err);
        assert_eq!(json["result"], serde_json::json!("Err"));
        assert_eq!(json["kind"], serde_json::json!("NotFound"));
        assert_eq!(
            json["message"],
            serde_json::json!("not found in the database")
        );
    }

    // Stand-in for `errors::DatabaseError` (the deja crate does not depend on
    // diesel_models). The macro's structured `recover_err` maps on the recorded
    // `kind` STRING, so this mirrors that mapping exactly.
    #[derive(Debug, PartialEq)]
    enum FakeDatabaseError {
        NotFound,
        UniqueViolation,
    }

    /// Replicates the macro's structured `recover_err`: maps a recorded `kind`
    /// discriminant to a reconstructed error, returning `None` (live
    /// fall-through) for any unknown kind.
    fn structured_recover_err(kind: &str, _message: &str) -> Option<FakeDatabaseError> {
        match kind {
            "NotFound" => Some(FakeDatabaseError::NotFound),
            "UniqueViolation" => Some(FakeDatabaseError::UniqueViolation),
            _ => None,
        }
    }

    /// The structured recover_err maps NotFound/UniqueViolation correctly and
    /// falls through (returns None) on unknown kinds.
    #[test]
    fn structured_recover_err_maps_known_kinds_and_falls_through() {
        assert_eq!(
            structured_recover_err("NotFound", "msg"),
            Some(FakeDatabaseError::NotFound)
        );
        assert_eq!(
            structured_recover_err("UniqueViolation", "msg"),
            Some(FakeDatabaseError::UniqueViolation)
        );
        // Unknown discriminants → None → live fall-through (V1 behavior).
        assert_eq!(structured_recover_err("Other", "msg"), None);
        assert_eq!(structured_recover_err("Legacy", "msg"), None);
        assert_eq!(structured_recover_err("anything-else", "msg"), None);
    }

    /// A recorded Err produced by `result_serialize_db` decodes back into a
    /// structured kind that the recover_err can act on end-to-end.
    #[test]
    fn record_then_recover_round_trip() {
        let err: Result<u8, &str> = Err("dup key");
        let (json, _is_err) =
            result_serialize_db(&err, |e| ("UniqueViolation".to_string(), (*e).to_string()));
        let decoded: DejaDatabaseResult =
            serde_json::from_value(json).expect("decode structured err");
        match decoded.payload {
            DejaDatabaseResultPayload::Err { kind, message } => {
                assert_eq!(
                    structured_recover_err(&kind, &message),
                    Some(FakeDatabaseError::UniqueViolation)
                );
            }
            _ => panic!("expected Err payload"),
        }
    }
}
