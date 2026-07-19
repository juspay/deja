//! Semantic recording primitives for Déjà.
//!
//! Captures trait-level operations (DB, Redis, HTTP, gRPC) with full call-site
//! tracking, correlation IDs, atomic sequencing, and timestamps.
//!
//! Replay is fully wired: the orchestrator renders a `LookupTable` from a
//! recording, and the candidate runs a `LookupTableHook` (installed via
//! `RuntimeHook`/`DEJA_MODE=replay`) that substitutes recorded results
//! per-boundary and emits an `ObservedCall` per lookup for post-hoc
//! divergence scoring.
//!
//! # Architecture
//!
//! The recording layer sits at the trait-object DI boundary:
//!
//! ```text
//! Handler → DejaStore (decorator) → Real Store
//!              │
//!              └─ DejaHook::record(BoundaryEvent)
//!                    │
//!                    └─ semantic-events.jsonl
//! ```
//!
//! Each event carries:
//! - `global_sequence`: monotonic atomic counter across all requests
//! - `request_sequence`: per-correlation-id ordering
//! - `call_file:call_line:call_column`: from `#[track_caller]`
//! - `correlation_id`: from `deja_context::current_correlation_id()`
//! - `timestamp_ns`: nanoseconds since UNIX epoch

use std::collections::HashMap;
use std::future::Future;
use std::panic::Location;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::Instrument;

pub mod correlation_layer;
pub mod graph;
pub mod replay;
pub mod writer;
pub use correlation_layer::{current_span_path, DejaCorrelationLayer};
pub use graph::{
    current_execution_graph_context, read_execution_graph_records, ExecutionGraphLayer,
    GraphNodeSink,
};
pub use replay::{
    ArgMismatchPolicy, Divergence, DivergenceKind, ReplayConfig, ReplayHook, ReplayReport,
};
pub use writer::{
    AsyncRecordWriter, CompositeSink, JsonlSink, MarkerKind, RecordSink, SinkPolicy, WriterConfig,
    WriterStatsSnapshot,
};

/// Optional stable identifier for one process/run inside an appended artifact.
pub const DEJA_RUN_ID_ENV_VAR: &str = "DEJA_RUN_ID";

pub(crate) fn current_recording_run_id() -> Option<String> {
    std::env::var(DEJA_RUN_ID_ENV_VAR)
        .ok()
        .filter(|value| !value.is_empty())
}

// ---------------------------------------------------------------------------
// Core event type
// ---------------------------------------------------------------------------

/// A single semantic operation captured at the trait boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BoundaryEvent {
    /// Monotonically increasing counter across all requests (no gaps).
    pub global_sequence: u64,
    /// Per-request ordering (1st, 2nd, 3rd call within this correlation scope).
    pub request_sequence: u64,
    /// Correlation ID from `deja_context::current_correlation_id()`.
    pub correlation_id: Option<String>,
    /// Nanoseconds since UNIX epoch.
    pub timestamp_ns: u64,
    /// Process/run identity for append-only recordings that contain many router runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recording_run_id: Option<String>,
    /// Active execution graph node id, when the execution graph layer is installed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_node_id: Option<u64>,
    /// Active `tracing` span id. Useful for diagnosing missing graph-node joins.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing_span_id: Option<u64>,
    /// Stable replay task id for lineage/canonicalization consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Parent replay task id when this event was emitted inside spawned detached work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    /// Legacy task-bucket field. Stamped as a compatibility alias for `bucket_id`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_bucket: Option<String>,
    /// Canonical lineage bucket for occurrence/lookup partitioning.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_id: Option<String>,
    /// Monotonic fork sequence for the task bucket (`0` for root lineage).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fork_seq: Option<u64>,
    /// Boundary layer: "storage", "redis", "http_client", "grpc".
    pub boundary: String,
    /// Trait name: "PaymentIntentInterface", "AddressInterface", etc.
    pub trait_name: String,
    /// Method name: "find_payment_intent_by_id", "insert_address", etc.
    pub method_name: String,
    /// Source file of the caller (from `#[track_caller]`).
    pub call_file: String,
    /// Source line of the caller.
    pub call_line: u32,
    /// Source column of the caller.
    pub call_column: u32,
    /// Receiver/decorator context captured before dispatch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub receiver: Option<serde_json::Value>,
    /// Request-like method input payload. Kept alongside `args` for readability.
    pub request: serde_json::Value,
    /// Serialized key arguments (JSON).
    pub args: serde_json::Value,
    /// Response-like method output payload. Kept alongside `result` for readability.
    pub response: serde_json::Value,
    /// Serialized result (JSON). For errors, contains `{"error": "..."}`.
    pub result: serde_json::Value,
    /// Whether the operation returned an error.
    pub is_error: bool,
    /// Wall-clock duration in microseconds.
    pub duration_us: u64,
    /// Wire-format schema version for this event. Fresh events are stamped with
    /// [`CURRENT_EVENT_SCHEMA_VERSION`]; recordings must carry the field.
    pub event_schema_version: u16,
    /// Optional structured call-site identity (syntactic hash, lexical path,
    /// operation occurrence, etc.) used for stable replay matching when source
    /// line/column information shifts.
    #[serde(default)]
    pub callsite_identity: Option<CallsiteIdentity>,
    /// How this event entered the artifact: a primary recording capture, or a
    /// shadow capture written while an execute-mode dispatch ran the REAL
    /// boundary during replay. Lets the post-hoc tally pair recorded vs shadow
    /// events to classify [`ValueDiverged`](crate::DivergenceKind::ValueDiverged).
    pub provenance: Provenance,
    /// Reconstructability of `result`: whether it round-trips losslessly, only
    /// structurally, or is opaque. Inert in M1 (always [`Fidelity::Lossless`]);
    /// carried so later stages can mark partial captures. Wire name pinned to
    /// the `recon` wire name so current readers and writers agree.
    #[serde(rename = "recon")]
    pub fidelity: Fidelity,
    /// Post-image of affected state after this operation, when explicitly
    /// captured by the boundary instrumentation. Omitted for legacy/plain events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_image: Option<serde_json::Value>,
    /// Pre-image of affected state before this operation, when explicitly
    /// captured by the boundary instrumentation. Omitted for legacy/plain events.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pre_image: Option<serde_json::Value>,
    /// Explicit state keys this crossing READ, when supplied by instrumentation.
    /// Empty means the boundary did not provide read capture; the recorder never
    /// infers keys from boundary or method names.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub read_set: Vec<String>,
    /// Explicit state keys this crossing WROTE, when supplied by instrumentation.
    /// Empty means the boundary did not provide write capture.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub write_set: Vec<String>,
    /// Stable content digest over `(args, result)` — the cheapest dataflow hint
    /// (a write whose digest matches an upstream read's is a probable read→write
    /// edge). Reuses the canonical args hashing, never a second hash function.
    /// An FNV-1a u64 routinely exceeds `i64::MAX`; the Kafka→Vector→MinIO record
    /// pipeline stringifies such integers (to dodge JSON float-precision loss),
    /// so deserialize leniently (accept number OR string) — otherwise every event
    /// carrying a large digest fails to parse and is dropped from replay.
    #[serde(
        default,
        deserialize_with = "de_u64_opt_lenient",
        skip_serializing_if = "Option::is_none"
    )]
    pub value_digest: Option<u64>,
    /// For Entropy/Time crossings, the generator family that produced the value
    /// ("id", "time"). A classification PRIMITIVE recorded for replay to read;
    /// never a replay verdict.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub entropy_source: Option<String>,
    /// The per-site replay knob (`Execute` | `Substitute`) declared for this
    /// boundary (design #28). Stamped on every event; the routing source of truth
    /// at replay.
    pub replay_strategy: ReplayStrategy,
    /// Free-text descriptive label ("db"/"http"/"redis") for the dashboard /
    /// provenance. NOT routing. `None` when the site declared no label.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Typed declarative boundary metadata for seed planning/reporting. Metadata
    /// only; replay routing still uses `replay_strategy`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub declaration: Option<BoundaryDeclaration>,
    /// Resampleable pre-transform draw for Entropy/Clock crossings where the
    /// boundary is a direct source. Reserved (usually `None`): black-box wrapping
    /// observes only the post-transform `result`, so this is populated only when
    /// the boundary returns the raw draw itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_draw: Option<serde_json::Value>,
    /// Wall-clock completion time (ns since epoch). Paired with `timestamp_ns`
    /// it gives the true span without collapsing it into `duration_us`;
    /// un-back-fillable, so captured now for latency/interleaving replay modes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_timestamp_ns: Option<u64>,
}

/// One record on the recording stream. The tape carries every record kind
/// through the ONE transport — the JSONL file or the application-owned sink
/// (Kafka in the vendor integration): boundary events are the judgment
/// stream; execution-graph nodes are the causal enrichment stream powering
/// fork trees, span timelines, and record-vs-replay graph alignment.
///
/// Internally tagged so each JSONL line / message stays one flat object and
/// consumers route on `record_kind` alone. Both kinds share the hook's
/// global sequence counter, so drop accounting and ordering describe one
/// totally-ordered stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "record_kind", rename_all = "snake_case")]
pub enum DejaRecord {
    /// A semantic boundary crossing. Boxed: it is by far the largest variant
    /// (~1 KiB), so keeping it inline would bloat every queued `DejaRecord` in
    /// the writer channel and every `GraphNode`/`Observed` line to match.
    BoundaryEvent(Box<BoundaryEvent>),
    /// An execution-graph span node (open/close lifecycle with parent and
    /// `follows_from` edges).
    GraphNode(deja_core::ExecutionGraphNode),
    /// A replay-side observation (lookup resolution or shadow execution);
    /// carried on the replay observed stream, never on record-mode tapes.
    /// Boxed for the same reason as `BoundaryEvent` — it is the largest of the
    /// remaining variants, so inlining it would size the enum to it.
    Observed(Box<crate::replay::ObservedCall>),
}

impl DejaRecord {
    /// Global stream sequence of the record, whichever kind it is.
    pub fn global_sequence(&self) -> u64 {
        match self {
            Self::BoundaryEvent(event) => event.global_sequence,
            Self::GraphNode(node) => node.global_sequence,
            Self::Observed(call) => call.source_event_global_sequence.unwrap_or(0),
        }
    }
}

/// Wire-format schema version stamped on freshly recorded events. Bumped in
/// lock-step with changes to the captured field set so tapes are distinguishable;
/// older readers tolerate newer tapes via `#[serde(default)]` on each added field,
/// and newer readers tolerate older tapes the same way. v2 adds the
/// forward-looking handler-completeness fields (read_set/write_set/value_digest/
/// entropy_source/raw_draw/end_timestamp_ns). v3 adds the optional typed
/// declarative boundary metadata (`declaration`). v4 adds `detached` to identify
/// events emitted from fire-and-forget work. v5 makes `result_image` /
/// `pre_image` an explicit producer API. v6 switches inferred argument capture to
/// autoref-specialized `deja::capture!` (structured serde preferred, tagged
/// `{"debug": …}` fallback, tagged
/// `{"deja_unserializable"|"deja_opaque_type": …}` markers instead of silent
/// nulls) — args-hash lookup keys are self-consistent within a v6 recording but
/// not comparable to pre-v6 tapes at fallback-affected sites. v7 adds Phase F
/// task-lineage/canonicalization scaffolding. v8 switches detached spawning to a
/// stamp-only model and adds canonical `bucket_id` plus `fork_seq` lineage.
pub const CURRENT_EVENT_SCHEMA_VERSION: u16 = 8;

/// How a [`BoundaryEvent`] entered the artifact.
///
/// `Recorded` is the ordinary capture path (record mode, or a lookup-mode
/// replay that substitutes). `Shadow` marks a shadow event written while an
/// execute-mode dispatch ran the REAL boundary during replay — the post-hoc
/// tally joins recorded ↔ shadow by args-free identity + occurrence to classify
/// value divergences.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Provenance {
    /// Primary capture (record mode or substituted lookup replay).
    #[default]
    Recorded,
    /// Shadow capture from an execute-mode dispatch running the real boundary.
    #[serde(rename = "execute_shadow")]
    Shadow,
}

/// Reconstructability of a captured `result`.
///
/// Inert in M1 (always [`Fidelity::Lossless`]); carried additively so later
/// stages can flag captures that only round-trip structurally or not at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Fidelity {
    /// Result round-trips byte-for-byte / value-for-value.
    #[default]
    Lossless,
    /// Result round-trips structurally but not losslessly.
    Structured,
    /// Result cannot be reconstructed from the capture.
    Opaque,
}

// ---------------------------------------------------------------------------
// Declarative boundary model — explicit routing plus typed metadata
//
// A boundary author declares the replay decision separately from descriptive
// metadata. The replay decision is still one per-site knob: is it safe to RE-RUN
// this function at replay (`Execute`), or must the recorded result be substituted
// (`Substitute`, the default)? That bit — `replay_strategy` — is the routing
// SOURCE OF TRUTH (design `docs/design/execute-substitute-declaration.md`, task
// #28). Typed declaration metadata (`effect`/`op`/`returns`/`codec`) and explicit
// state capture (`read_set`/`write_set`) feed seed planning/reporting; they never
// override routing.
//
// `kind` is a FREE-TEXT descriptive label ("db"/"http"/"redis") kept for the
// dashboard / provenance only — it never drives routing.
//
// ADDITIVE: a site that declares nothing carries `replay_strategy = Substitute`
// (the safe default — never wrongly re-runs).
// ---------------------------------------------------------------------------

/// The per-site replay knob: is it safe to RE-RUN this boundary at replay?
///
/// This is the whole declarative routing decision (design #28). Default is
/// [`ReplayStrategy::Substitute`] (safe — never re-runs a real boundary).
///
/// - [`ReplayStrategy::Execute`] — reconstruct state from the recorded result
///   (seed by key/PK via the seed plan), then RUN the real function during replay.
///   Maps to [`ExecuteMode::Execute`].
/// - [`ReplayStrategy::Substitute`] *(default)* — DON'T run the function; return
///   the recorded result. Covers entropy (clock/id), egress (http), and anything
///   not opted into `Execute`. Maps to [`ExecuteMode::Lookup`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReplayStrategy {
    /// Re-run the real boundary at replay (seed from recorded result first).
    Execute,
    /// Substitute the recorded result; never re-run (the default).
    #[default]
    Substitute,
}

/// Coarse effect domain for a declared boundary.
///
/// This is metadata for seeding/reporting, not the replay-routing source of
/// truth. Routing remains [`ReplayStrategy`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EffectKind {
    Db,
    Redis,
    Http,
    /// gRPC egress (kind tag "grpc"; transport-layer boundary — see
    /// docs/design/grpc-egress-boundary.md).
    Grpc,
    Entropy,
    Time,
    Function,
}

/// Declarative operation semantics for seed planning and report classification.
///
/// This deliberately does not replace the boundary/trait/method identity tuple;
/// it describes what the operation does to state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationKind {
    Read,
    Write,
    Touch,
    Create,
    Update,
    Delete,
    Upsert,
    CompareAndSet,
    IdempotentDelete,
    ExternalCall,
    Entropy,
    Clock,
}

/// Coarse shape returned by a declared boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReturnSemantics {
    None,
    Unit,
    Value,
    Optional,
    Rows,
    Count,
    Bool,
    PreImage,
    PostImage,
    UpdateReturning,
    DeleteReturning,
    Raw,
}

/// Type-erased reference to the result/state codec a boundary declaration uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodecRef {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
}

impl CodecRef {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
        }
    }

    pub fn versioned(id: impl Into<String>, version: u32) -> Self {
        Self {
            id: id.into(),
            version: Some(version),
        }
    }
}

/// Type-erased reference to a canonical state/reply projection.
///
/// This is Phase F scaffolding only: declarations can name the canonicalization
/// contract they intend, but the runtime kernel does not score or transform
/// values from this metadata yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonRef {
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<u32>,
}

impl CanonRef {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            version: None,
        }
    }

    pub fn versioned(id: impl Into<String>, version: u32) -> Self {
        Self {
            id: id.into(),
            version: Some(version),
        }
    }
}

/// Typed declarative metadata stamped by macros/helpers.
///
/// All fields are optional so old recordings and legacy call sites remain valid.
/// A declaration is intentionally metadata-only today; replay routing continues to
/// use [`ReplayStrategy`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryDeclaration {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effect: Option<EffectKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<OperationKind>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub returns: Option<ReturnSemantics>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub codec: Option<CodecRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_canon: Option<CanonRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_canon: Option<CanonRef>,
}

impl BoundaryDeclaration {
    pub fn effect(mut self, effect: EffectKind) -> Self {
        self.effect = Some(effect);
        self
    }

    pub fn operation(mut self, op: OperationKind) -> Self {
        self.op = Some(op);
        self
    }

    pub fn returns(mut self, returns: ReturnSemantics) -> Self {
        self.returns = Some(returns);
        self
    }

    pub fn codec(mut self, codec: CodecRef) -> Self {
        self.codec = Some(codec);
        self
    }

    pub fn state_canon(mut self, canon: CanonRef) -> Self {
        self.state_canon = Some(canon);
        self
    }

    pub fn reply_canon(mut self, canon: CanonRef) -> Self {
        self.reply_canon = Some(canon);
        self
    }

    pub fn is_empty(&self) -> bool {
        self.effect.is_none()
            && self.op.is_none()
            && self.returns.is_none()
            && self.codec.is_none()
            && self.state_canon.is_none()
            && self.reply_canon.is_none()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeMode {
    Record,
    Replay,
    Disabled,
}

impl RuntimeMode {
    pub const fn is_record(self) -> bool {
        matches!(self, RuntimeMode::Record)
    }

    pub const fn is_replay(self) -> bool {
        matches!(self, RuntimeMode::Replay)
    }

    pub const fn is_disabled(self) -> bool {
        matches!(self, RuntimeMode::Disabled)
    }

    pub const fn consumes_args(self) -> bool {
        matches!(self, RuntimeMode::Record | RuntimeMode::Replay)
    }
}

/// The declared intrinsic semantics of a boundary, carried alongside the
/// [`BoundarySpec`]. The per-site [`ReplayStrategy`] knob is the routing source
/// of truth; `kind` is a non-routing descriptive label for the dashboard /
/// provenance ("db"/"http"/"redis"). [`BoundaryDeclaration`] carries typed
/// metadata for seed planning/reporting. A site that declares nothing gets the
/// default (`Substitute`, `kind = None`, `declaration = None`), so it is never
/// re-run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BoundarySemantics {
    /// The per-site replay knob (routing source of truth). Default `Substitute`.
    pub replay_strategy: ReplayStrategy,
    /// Free-text descriptive label ("db"/"http"/"redis") for dashboard /
    /// provenance. NOT routing. `None` when undeclared.
    pub kind: Option<String>,
    /// Typed declarative metadata. Metadata-only; routing remains
    /// `replay_strategy`.
    pub declaration: Option<BoundaryDeclaration>,
}

impl BoundarySemantics {
    /// The default semantics: `Substitute` with no descriptive label or typed
    /// declaration. A site that declares nothing gets these (safe — never
    /// re-runs).
    pub fn undeclared() -> Self {
        Self::default()
    }

    pub fn with_declaration(mut self, declaration: BoundaryDeclaration) -> Self {
        self.declaration = (!declaration.is_empty()).then_some(declaration);
        self
    }
}

// ---------------------------------------------------------------------------
// Per-boundary execute mode
// ---------------------------------------------------------------------------

/// Per-boundary dispatch mode chosen for one replay call.
///
/// [`ExecuteMode::Lookup`] (the default) serves the call from the recorded
/// table; [`ExecuteMode::Execute`] runs the REAL boundary and shadow-records the
/// result. Routing is derived solely from the boundary's [`ReplayStrategy`] when
/// the active hook is in replay mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteMode {
    /// Serve the call from the recorded lookup table (full mock).
    #[default]
    Lookup,
    /// Run the REAL boundary and shadow-record the fresh result.
    Execute,
}

/// Opaque handle returned by [`DejaHook::execute_shadow_peek`] and consumed by
/// [`DejaHook::execute_shadow_observe`].
///
/// The macro's execute-mode arm runs in two steps so the live boundary call can
/// sit BETWEEN them: `execute_shadow_peek` resolves the recorded baseline (and
/// advances the lookup-table stamper / occurrence counters in the SAME lock-step
/// the lookup path does, so numbering never drifts) WITHOUT substituting and
/// WITHOUT emitting a `Recorded` observation; the macro then runs the real
/// `self.$inner.$method()` against the live boundary; finally
/// `execute_shadow_observe` stamps the real result onto the carried
/// [`ObservedCall`] (provenance [`Provenance::Shadow`]) and emits it.
///
/// The token holds the fully-built `ObservedCall` with `observed_result` left
/// `None`; `observe` fills it. It is intentionally opaque to the macro — the
/// macro only moves it from `peek` into `observe`.
pub struct ExecuteShadowToken {
    /// The observation to emit once the real result is known. `observed_result`
    /// is `None` here and filled by [`DejaHook::execute_shadow_observe`].
    observed: crate::replay::ObservedCall,
}

impl ExecuteShadowToken {
    /// Build a token from a pre-resolved [`ObservedCall`]. The call should carry
    /// `provenance = Provenance::Shadow`, the resolved `recorded_result`
    /// (or `None` + `seed_gap = true` when no baseline was found), and a `None`
    /// `observed_result` (filled at observe time).
    pub fn new(observed: crate::replay::ObservedCall) -> Self {
        Self { observed }
    }

    /// Consume the token, attaching the real boundary's `observed_result`, and
    /// return the completed [`ObservedCall`] ready to be emitted.
    pub fn into_observed(
        mut self,
        observed_result: serde_json::Value,
    ) -> crate::replay::ObservedCall {
        self.observed.observed_result = Some(observed_result);
        self.observed
    }
}

// ---------------------------------------------------------------------------
// Call-site identity
// ---------------------------------------------------------------------------

/// Source kind for a `CallsiteIdentity`. Indicates how the identity was
/// derived (explicit annotation, syntactic hash, lexical path, operation
/// occurrence index, or legacy file/line/column).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum CallsiteSource {
    /// User-supplied callsite identity (explicit annotation).
    Explicit,
    /// Hash derived from surrounding syntax tokens.
    SyntacticHash,
    /// Stable module path / item path.
    LexicalPath,
    /// Per-operation occurrence index within a correlation scope.
    OperationOccurrence,
    /// Legacy file:line:column captured by `#[track_caller]`.
    LegacyLocation,
}

/// Structured identity describing a call-site for stable replay matching.
///
/// Carries enough metadata to disambiguate distinct logical call sites even
/// when source file/line numbers shift across recordings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallsiteIdentity {
    /// Wire-format version for this `CallsiteIdentity`.
    pub version: u16,
    /// How this identity was derived.
    pub source: CallsiteSource,
    /// Stable identifier (e.g. hash digest or explicit tag).
    pub id: Option<String>,
    /// Logical scope (module path, function path, etc.).
    pub scope: Option<String>,
    /// Per-source occurrence index within `scope`.
    pub occurrence: u32,
    /// Enclosing function name when known.
    pub caller_function: Option<String>,
    /// Lexical path (e.g. `crate::module::function`) when known.
    pub lexical_path: Option<String>,
    /// Syntactic hash of surrounding tokens when known.
    ///
    /// Deserialized leniently (number OR string): a `u64` hash can exceed 2^53,
    /// and JSON transports that route through JS-based tooling (e.g. Vector in the
    /// Kafka→S3 recording path) serialize such values as STRINGS to preserve
    /// precision. Accepting both keeps the recording round-trippable regardless of
    /// which sink wrote it.
    #[serde(default, deserialize_with = "de_u64_opt_lenient")]
    pub syntax_hash: Option<u64>,
    /// Logical span-path (root→leaf `tracing` span names, joined by `>`) the call
    /// fired within — the SOURCE for the rank-2 `Address::SpanPath`. Stable
    /// across benign V2 edits (line shifts, signature tweaks) that leave the span
    /// structure intact, and DISTINCT for concurrent same-callsite calls in
    /// different spans — which is what stops the positional `occurrence` from
    /// swapping under async interleaving. `None` when no span was entered (the call
    /// then degrades to weaker ranks — never worse than before this field existed).
    #[serde(default)]
    #[serde(rename = "logical_context")]
    pub span_path: Option<String>,
}

/// Deserialize an `Option<u64>` from either a JSON number or a JSON string,
/// tolerating transports that stringify large (>2^53) integers.
fn de_u64_opt_lenient<'de, D>(deserializer: D) -> Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    match Option::<serde_json::Value>::deserialize(deserializer)? {
        None | Some(serde_json::Value::Null) => Ok(None),
        Some(serde_json::Value::Number(n)) => Ok(n.as_u64()),
        Some(serde_json::Value::String(s)) => {
            s.parse::<u64>().map(Some).map_err(serde::de::Error::custom)
        }
        Some(other) => Err(serde::de::Error::custom(format!(
            "syntax_hash: expected u64 number or string, got {other}"
        ))),
    }
}

/// Lookup query carrying replay context (boundary, args, optional callsite
/// identity, optional caller location).
///
/// Hooks that opt into context-aware replay implement
/// [`DejaHook::try_replay_with_context`].
pub struct ReplayLookup<'a> {
    /// Boundary tag (e.g. `"storage"`, `"redis"`, `"http_client"`).
    pub boundary: &'a str,
    /// Trait name at the boundary.
    pub trait_name: &'a str,
    /// Method name being invoked.
    pub method_name: &'a str,
    /// Serialized arguments to match against.
    pub args: &'a serde_json::Value,
    /// Optional structured callsite identity for stable matching.
    pub callsite_identity: Option<&'a CallsiteIdentity>,
    /// Optional caller location for legacy file:line:column matching.
    pub caller_location: Option<&'a std::panic::Location<'a>>,
}

// ---------------------------------------------------------------------------
// Call-site helper
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Hook trait
// ---------------------------------------------------------------------------

/// Trait for receiving semantic events from the decorator layer.
///
/// Implementations handle recording (write to file) or replay (match + return).
pub trait DejaHook: Send + Sync {
    /// Return the hook's current runtime mode.
    fn mode(&self) -> RuntimeMode {
        RuntimeMode::Disabled
    }

    /// Return true when the hook is active (recording or replay is enabled).
    ///
    /// When false, the generated delegation skips all recording overhead
    /// (no JSON serialization, no file writes, no sequencing).
    fn is_active(&self) -> bool {
        !self.mode().is_disabled()
    }

    /// Whether this hook REPLAYS recorded results (vs records / no-op).
    fn is_replay(&self) -> bool {
        self.mode().is_replay()
    }

    /// Attempt to replay a previously recorded result without calling the
    /// real implementation.
    ///
    /// Returns `Some(result_json)` if a matching recorded event is found.
    /// Returns `None` if no match — the delegation should fall through to
    /// the real implementation.
    ///
    /// Default returns `None`, making this opt-in for replay-enabled hooks.
    fn try_replay(
        &self,
        _boundary: &str,
        _trait_name: &str,
        _method_name: &str,
        _args: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        None
    }

    /// Record a completed semantic event.
    fn record(&self, event: BoundaryEvent);

    /// Allocate the next global sequence number.
    fn next_global_sequence(&self) -> u64;

    /// Allocate the next per-request sequence number for the given correlation ID.
    fn next_request_sequence(&self, correlation_id: Option<&str>) -> u64;

    /// Attempt a context-aware replay lookup.
    ///
    /// Default delegation falls back to [`DejaHook::try_replay`] using the
    /// boundary/trait/method/args carried in `query`. Replay-capable hooks
    /// override this to consult the structured `callsite_identity` and
    /// `caller_location` for stable matching across source-line shifts.
    fn try_replay_with_context(&self, query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        self.try_replay(
            query.boundary,
            query.trait_name,
            query.method_name,
            query.args,
        )
    }

    /// First half of an execute-mode dispatch: resolve the recorded baseline for
    /// this call WITHOUT substituting and WITHOUT emitting a `Recorded`
    /// observation, returning a token the macro carries across the real boundary
    /// call. The token records the resolved `recorded_result` (or a seed gap when
    /// none is found) for the post-hoc value-divergence join.
    ///
    /// Implementations MUST advance the same per-call occurrence / sequence /
    /// stamper state the lookup path advances, so a run that mixes lookup and
    /// execute boundaries keeps identical numbering. The default returns `None`,
    /// meaning the hook does not support shadowing.
    fn execute_shadow_peek(&self, _query: ReplayLookup<'_>) -> Option<ExecuteShadowToken> {
        None
    }

    /// Second half of an execute-mode dispatch: stamp the REAL boundary's
    /// `observed_result` onto the token's carried observation and emit it
    /// (provenance [`Provenance::Shadow`]). Called by the macro AFTER the
    /// real `self.$inner.$method()` completes. The default is a no-op.
    fn execute_shadow_observe(
        &self,
        _token: ExecuteShadowToken,
        _observed_result: serde_json::Value,
    ) {
    }

    /// Allocate the next per-callsite occurrence index within a correlation
    /// scope.
    ///
    /// Replay/recording hooks use this to disambiguate repeated calls at the
    /// same logical call-site. The default returns `0` for hooks that do not
    /// track occurrences.
    fn next_callsite_occurrence(
        &self,
        _correlation_id: Option<&str>,
        _source: CallsiteSource,
        _scope: Option<&str>,
    ) -> u32 {
        0
    }

    /// Optional stable identifier for the current recording run.
    ///
    /// Recording hooks return `Some(&str)` to attach a stable run id to every
    /// emitted event. Non-recording hooks return `None` (the default).
    fn recording_run_id(&self) -> Option<&str> {
        None
    }

    /// Flush buffered hook output when the implementation owns an async sink.
    ///
    /// Default no-ops for disabled/replay cursor hooks; recording/lookup hooks
    /// override this so late request-boundary finalizers can make their driver
    /// rows durable before harness shutdown.
    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// No-op hook — pass-through with zero overhead
// ---------------------------------------------------------------------------

/// A `DejaHook` that does nothing.
///
/// Used when Déjà recording is disabled. The delegation macro's fast-path
/// (`self.hook.is_active()`) avoids even entering the async block that
/// contains this hook, so `DisabledHook` is typically never instantiated at
/// runtime — but it is useful for type-system wiring and tests.
#[derive(Debug, Clone, Copy, Default)]
pub struct DisabledHook;

impl DejaHook for DisabledHook {
    fn mode(&self) -> RuntimeMode {
        RuntimeMode::Disabled
    }
    fn record(&self, _event: BoundaryEvent) {}

    fn next_global_sequence(&self) -> u64 {
        0
    }

    fn next_request_sequence(&self, _correlation_id: Option<&str>) -> u64 {
        0
    }

    fn try_replay_with_context(&self, _query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        None
    }

    fn next_callsite_occurrence(
        &self,
        _correlation_id: Option<&str>,
        _source: CallsiteSource,
        _scope: Option<&str>,
    ) -> u32 {
        0
    }

    fn recording_run_id(&self) -> Option<&str> {
        None
    }
}

// ---------------------------------------------------------------------------
// Timestamp helper
// ---------------------------------------------------------------------------

/// Current wall-clock time as nanoseconds since UNIX epoch.
#[inline]
pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO)
        .as_nanos() as u64
}

// ---------------------------------------------------------------------------
// Recording implementation
// ---------------------------------------------------------------------------

/// Records semantic events to a JSONL file with atomic sequencing.
pub struct RecordingHook {
    writer: AsyncRecordWriter<DejaRecord>,
    global_counter: AtomicU64,
    /// Sequence space for tape-carried graph nodes — deliberately separate
    /// from `global_counter` so boundary-event numbering is graph-invariant
    /// (replay lookup addressing mirrors it).
    graph_counter: AtomicU64,
    request_counters: Mutex<HashMap<String, u64>>,
    /// Counter for events with no correlation ID.
    uncorrelated_counter: AtomicU64,
    /// Stable identifier for this recording run, attached to every event.
    recording_run_id: String,
    /// Per-callsite occurrence counters keyed by
    /// `(correlation_id, bucket_id, source, scope)`.
    callsite_occurrence: Mutex<CallsiteOccurrenceMap>,
}

/// Per-callsite occurrence counters keyed by `(correlation_id, bucket_id, source, scope)`.
pub(crate) type CallsiteOccurrenceMap = HashMap<
    (
        Option<String>,
        Option<String>,
        CallsiteSource,
        Option<String>,
    ),
    u32,
>;

impl RecordingHook {
    /// Resolve `recording_run_id` from the environment, falling back to a
    /// time-based id when neither `DEJA_RECORDING_RUN_ID` nor
    /// `DEJA_RUN_ID_ENV_VAR` is set.
    fn resolve_recording_run_id() -> String {
        std::env::var("DEJA_RECORDING_RUN_ID")
            .ok()
            .filter(|value| !value.is_empty())
            .or_else(|| {
                std::env::var(DEJA_RUN_ID_ENV_VAR)
                    .ok()
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| format!("run-{}", now_ns()))
    }

    /// Create a new recording hook writing to the given directory.
    ///
    /// Creates `semantic-events.jsonl` in the specified directory with default
    /// writer settings. This is the JSONL-only convenience constructor;
    /// applications with their own transport use [`RecordingHook::with_sink`].
    pub fn new(artifact_dir: &Path) -> std::io::Result<Self> {
        std::fs::create_dir_all(artifact_dir)?;
        let path = artifact_dir.join("semantic-events.jsonl");
        let sink = JsonlSink::new(&path)?;
        Ok(Self::with_sink(
            sink,
            Self::resolve_recording_run_id(),
            WriterConfig::default(),
        ))
    }

    /// Create a recording hook backed by a caller-supplied sink and writer
    /// config.
    ///
    /// This is the dependency-inversion entry point: the application owns both
    /// transport choice (JSONL alone, Kafka, S3, a fan-out via
    /// [`crate::writer::CompositeSink`], etc.) and the async writer settings
    /// (`queue_capacity`, `batch_size`, flush interval, flush timeout, and
    /// sink policy). The sink receives [`DejaRecord`]s: boundary events and —
    /// when the execution-graph layer is installed — graph nodes, one
    /// totally-ordered stream.
    ///
    /// `recording_run_id` is the stable identifier attached to every record
    /// emitted through this hook. Callers that want the standard env-var
    /// resolution can pass `RecordingHook::resolve_recording_run_id_default()`.
    pub fn with_sink<S>(sink: S, recording_run_id: String, writer_config: WriterConfig) -> Self
    where
        S: RecordSink<DejaRecord>,
    {
        Self {
            // The seq extractor lets the writer account drops and stamp the
            // sink markers (checkpoint/eof/dropped) with real global sequences.
            writer: AsyncRecordWriter::with_seq_of(
                sink,
                writer_config,
                Some(std::sync::Arc::new(DejaRecord::global_sequence)),
            ),
            global_counter: AtomicU64::new(0),
            graph_counter: AtomicU64::new(0),
            request_counters: Mutex::new(HashMap::new()),
            uncorrelated_counter: AtomicU64::new(0),
            recording_run_id,
            callsite_occurrence: Mutex::new(HashMap::new()),
        }
    }

    /// Convenience wrapper around [`Self::resolve_recording_run_id`] for
    /// callers of [`Self::with_sink`] that want the default env-var
    /// resolution without duplicating logic.
    pub fn resolve_recording_run_id_default() -> String {
        Self::resolve_recording_run_id()
    }

    /// Stable identifier for this recording run, attached to every emitted
    /// event.
    pub fn recording_run_id(&self) -> &str {
        &self.recording_run_id
    }

    /// Flush all queued records through the configured sink.
    ///
    /// Recording errors are intentionally not surfaced to request handlers, but
    /// tests and harnesses can call this to force JSONL visibility before
    /// reading artifact files.
    pub fn flush(&self) -> std::io::Result<()> {
        self.writer.flush()
    }

    /// Snapshot health counters for the async writer.
    pub fn writer_stats(&self) -> WriterStatsSnapshot {
        self.writer.stats()
    }
}

impl crate::graph::GraphNodeSink for RecordingHook {
    /// Graph nodes ride the recording tape next to boundary events. They get
    /// a DEDICATED sequence space: boundary-event `global_sequence` numbering
    /// must stay identical whether or not graph capture is on, because replay
    /// lookup addressing mirrors the recorder's sequence allocation.
    fn graph_node(&self, mut node: deja_core::ExecutionGraphNode) {
        node.global_sequence = self.graph_counter.fetch_add(1, Ordering::SeqCst);
        node.recording_run_id = Some(self.recording_run_id.clone());
        let _ = self.writer.record(DejaRecord::GraphNode(node));
    }
}

impl DejaHook for RecordingHook {
    fn mode(&self) -> RuntimeMode {
        // Process-level recording state AND the per-request sampling gate. The
        // per-correlation decision is now an enum-ready `RecordDecision` (see
        // deja-context). NOTE: the OPT-IN FLIP — defaulting undecided boundaries
        // to `Skip` (`unwrap_or(false)`) so the Superposition sampler's own config
        // read self-excludes — is deliberately DEFERRED to the sampler-boundary
        // work, where it lands with the feature that needs it and is validated
        // end-to-end (record+replay). Until then the gate keeps the pre-existing
        // default: record when no decision is present, so every current test/rig
        // is unaffected. An explicit `Skip` still makes every boundary a no-op
        // before any record-only helper allocates sequence numbers. Every
        // boundary — db, instrument id/time/crypto/http, redis, and the
        // `RuntimeHook::Recording` delegation — funnels through here.
        if self.writer.is_active()
            && deja_context::recording_decision_for_current()
                .map(deja_context::RecordDecision::should_record)
                .unwrap_or(true)
        {
            RuntimeMode::Record
        } else {
            RuntimeMode::Disabled
        }
    }

    fn record(&self, event: BoundaryEvent) {
        let _ = self
            .writer
            .record(DejaRecord::BoundaryEvent(Box::new(event)));
    }

    fn next_global_sequence(&self) -> u64 {
        self.global_counter.fetch_add(1, Ordering::SeqCst)
    }

    fn next_request_sequence(&self, correlation_id: Option<&str>) -> u64 {
        match correlation_id {
            Some(id) => {
                if let Ok(mut map) = self.request_counters.lock() {
                    let counter = map.entry(id.to_string()).or_insert(0);
                    let seq = *counter;
                    *counter += 1;
                    seq
                } else {
                    0
                }
            }
            None => self.uncorrelated_counter.fetch_add(1, Ordering::SeqCst),
        }
    }

    fn next_callsite_occurrence(
        &self,
        correlation_id: Option<&str>,
        source: CallsiteSource,
        scope: Option<&str>,
    ) -> u32 {
        let key = (
            correlation_id.map(String::from),
            Some(current_task_lineage().bucket_id),
            source,
            scope.map(String::from),
        );
        // SHADOW GUARANTEE: recover a poisoned lock instead of panicking — a
        // recording-side panic must never propagate into the real request.
        let mut guard = self
            .callsite_occurrence
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = guard.entry(key).or_insert(0);
        let value = *entry;
        *entry += 1;
        value
    }

    fn flush(&self) -> std::io::Result<()> {
        RecordingHook::flush(self)
    }

    fn recording_run_id(&self) -> Option<&str> {
        Some(&self.recording_run_id)
    }
}

// ---------------------------------------------------------------------------
// Runtime hook enum (record / replay / no-op)
// ---------------------------------------------------------------------------

/// Polymorphic runtime hook that selects between recording, replay, and
/// no-op behavior based on environment configuration.
// One value per process, built at boot — variant size imbalance is irrelevant
// and boxing would churn every constructor.
#[allow(clippy::large_enum_variant)]
pub enum RuntimeHook {
    /// Writes every event to a JSONL artifact.
    ///
    /// Held behind an `Arc` so the SAME recorder can be shared with
    /// `GLOBAL_RECORDING_HOOK` via [`global_hook_from_env`]. Without sharing,
    /// boundaries that resolve through the runtime hook (e.g. id generation)
    /// and boundaries that resolve through `global_hook_from_env` (e.g. db,
    /// http, redis) would use two independent `RecordingHook`s — two
    /// `global_sequence` counters and two sink sets — corrupting the recording
    /// (duplicate sequences, torn JSONL lines) and splitting any
    /// application-supplied secondary sink (e.g. Kafka) across only half the
    /// boundaries.
    Recording(Arc<RecordingHook>),
    /// Replays a previously recorded artifact using the in-process cascade.
    Replay(ReplayHook),
    /// Replays from a pre-rendered `LookupTable`. The hot path is O(1) lookup;
    /// divergence detection runs post-hoc by the orchestrator over the emitted
    /// `ObservedCall` stream.
    LookupReplay(crate::replay::LookupTableHook),
    /// No-op pass-through.
    Disabled(DisabledHook),
}

impl RuntimeHook {
    /// Flush any buffered writes. No-op for non-recording variants.
    pub fn flush(&self) -> std::io::Result<()> {
        match self {
            RuntimeHook::Recording(h) => h.flush(),
            RuntimeHook::LookupReplay(h) => h.flush(),
            RuntimeHook::Replay(_) | RuntimeHook::Disabled(_) => Ok(()),
        }
    }

    /// Snapshot writer stats when the underlying hook is a recorder.
    pub fn writer_stats(&self) -> Option<WriterStatsSnapshot> {
        match self {
            RuntimeHook::Recording(h) => Some(h.writer_stats()),
            _ => None,
        }
    }

    /// Returns a stable identifier for the hook variant for logging.
    pub fn variant_name(&self) -> &'static str {
        match self {
            RuntimeHook::Recording(_) => "recording",
            RuntimeHook::Replay(_) => "replay",
            RuntimeHook::LookupReplay(_) => "lookup_replay",
            RuntimeHook::Disabled(_) => "disabled",
        }
    }

    /// Return the active runtime mode for this hook.
    pub fn mode(&self) -> RuntimeMode {
        match self {
            RuntimeHook::Recording(h) => h.mode(),
            RuntimeHook::Replay(h) => h.mode(),
            RuntimeHook::LookupReplay(h) => h.mode(),
            RuntimeHook::Disabled(h) => h.mode(),
        }
    }

    /// Whether this hook is replaying recorded results (either the standalone
    /// `Replay` hook or the harness-driven `LookupReplay` hook).
    pub fn is_replay(&self) -> bool {
        self.mode().is_replay()
    }
}

impl DejaHook for RuntimeHook {
    fn mode(&self) -> RuntimeMode {
        RuntimeHook::mode(self)
    }

    fn try_replay(
        &self,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
        args: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        match self {
            RuntimeHook::Recording(h) => h.try_replay(boundary, trait_name, method_name, args),
            RuntimeHook::Replay(h) => h.try_replay(boundary, trait_name, method_name, args),
            RuntimeHook::LookupReplay(h) => h.try_replay(boundary, trait_name, method_name, args),
            RuntimeHook::Disabled(h) => h.try_replay(boundary, trait_name, method_name, args),
        }
    }

    fn try_replay_with_context(&self, query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        match self {
            RuntimeHook::Recording(h) => h.try_replay_with_context(query),
            RuntimeHook::Replay(h) => h.try_replay_with_context(query),
            RuntimeHook::LookupReplay(h) => h.try_replay_with_context(query),
            RuntimeHook::Disabled(h) => h.try_replay_with_context(query),
        }
    }

    fn execute_shadow_peek(&self, query: ReplayLookup<'_>) -> Option<ExecuteShadowToken> {
        match self {
            RuntimeHook::Recording(h) => h.execute_shadow_peek(query),
            RuntimeHook::Replay(h) => h.execute_shadow_peek(query),
            RuntimeHook::LookupReplay(h) => h.execute_shadow_peek(query),
            RuntimeHook::Disabled(h) => h.execute_shadow_peek(query),
        }
    }

    fn execute_shadow_observe(
        &self,
        token: ExecuteShadowToken,
        observed_result: serde_json::Value,
    ) {
        match self {
            RuntimeHook::Recording(h) => h.execute_shadow_observe(token, observed_result),
            RuntimeHook::Replay(h) => h.execute_shadow_observe(token, observed_result),
            RuntimeHook::LookupReplay(h) => h.execute_shadow_observe(token, observed_result),
            RuntimeHook::Disabled(h) => h.execute_shadow_observe(token, observed_result),
        }
    }

    fn record(&self, event: BoundaryEvent) {
        match self {
            RuntimeHook::Recording(h) => h.record(event),
            RuntimeHook::Replay(h) => h.record(event),
            RuntimeHook::LookupReplay(h) => h.record(event),
            RuntimeHook::Disabled(h) => h.record(event),
        }
    }

    fn next_global_sequence(&self) -> u64 {
        match self {
            RuntimeHook::Recording(h) => h.next_global_sequence(),
            RuntimeHook::Replay(h) => h.next_global_sequence(),
            RuntimeHook::LookupReplay(h) => h.next_global_sequence(),
            RuntimeHook::Disabled(h) => h.next_global_sequence(),
        }
    }

    fn next_request_sequence(&self, correlation_id: Option<&str>) -> u64 {
        match self {
            RuntimeHook::Recording(h) => h.next_request_sequence(correlation_id),
            RuntimeHook::Replay(h) => h.next_request_sequence(correlation_id),
            RuntimeHook::LookupReplay(h) => h.next_request_sequence(correlation_id),
            RuntimeHook::Disabled(h) => h.next_request_sequence(correlation_id),
        }
    }

    fn next_callsite_occurrence(
        &self,
        correlation_id: Option<&str>,
        source: CallsiteSource,
        scope: Option<&str>,
    ) -> u32 {
        match self {
            RuntimeHook::Recording(h) => h.next_callsite_occurrence(correlation_id, source, scope),
            RuntimeHook::Replay(h) => h.next_callsite_occurrence(correlation_id, source, scope),
            RuntimeHook::LookupReplay(h) => {
                h.next_callsite_occurrence(correlation_id, source, scope)
            }
            RuntimeHook::Disabled(h) => h.next_callsite_occurrence(correlation_id, source, scope),
        }
    }

    fn flush(&self) -> std::io::Result<()> {
        RuntimeHook::flush(self)
    }

    fn recording_run_id(&self) -> Option<&str> {
        match self {
            RuntimeHook::Recording(h) => Some(h.recording_run_id()),
            RuntimeHook::Replay(h) => DejaHook::recording_run_id(h),
            RuntimeHook::LookupReplay(h) => DejaHook::recording_run_id(h),
            RuntimeHook::Disabled(h) => DejaHook::recording_run_id(h),
        }
    }
}

/// Construct an `Option<RuntimeHook::LookupReplay>` from
/// `DEJA_LOOKUP_TABLE` (path to a JSONL or JSON `LookupTable`) and
/// optionally `DEJA_OBSERVED_SINK` (path to a JSONL file the candidate
/// writes per-call `ObservedCall` records to). When the sink env var is
/// unset, observations accumulate in memory and are lost unless the
/// application drains them explicitly via the hook's underlying sink.
fn lookup_replay_hook_from_env() -> Option<RuntimeHook> {
    let table_path = std::env::var("DEJA_LOOKUP_TABLE").ok()?;
    let hook = match std::env::var("DEJA_OBSERVED_SINK").ok() {
        Some(observed_path) => match crate::replay::FileObservedSink::create(&observed_path) {
            Ok(sink) => crate::replay::LookupTableHook::from_source(
                crate::replay::LocalFileLookupSource::new(&table_path),
                sink,
            ),
            Err(err) => {
                eprintln!("deja: failed to open DEJA_OBSERVED_SINK={observed_path}: {err}");
                return None;
            }
        },
        None => crate::replay::LookupTableHook::from_source(
            crate::replay::LocalFileLookupSource::new(&table_path),
            crate::replay::InMemoryObservedSink::new(),
        ),
    };
    match hook {
        Ok(h) => Some(RuntimeHook::LookupReplay(h)),
        Err(err) => {
            eprintln!("deja: failed to load DEJA_LOOKUP_TABLE={table_path}: {err}");
            None
        }
    }
}

/// Construct a [`RuntimeHook`] from environment variables.
///
/// Reads `DEJA_MODE` (`record` | `replay` | `disabled`) and
/// `DEJA_ARTIFACT_DIR`. Returns `None` when disabled or misconfigured.
pub fn runtime_hook_from_env() -> Option<RuntimeHook> {
    let mode = std::env::var("DEJA_MODE").ok();
    let artifact_dir = std::env::var("DEJA_ARTIFACT_DIR").ok();

    match mode.as_deref() {
        Some("record") => artifact_dir.and_then(|dir| {
            RecordingHook::new(Path::new(&dir))
                .ok()
                .map(|h| RuntimeHook::Recording(Arc::new(h)))
        }),
        Some("replay") => {
            // Prefer the lookup-table path when DEJA_LOOKUP_TABLE is set
            // (harness-driven runs); fall back to the classic in-process
            // ReplayHook for standalone use (local development loops).
            if let Some(hook) = lookup_replay_hook_from_env() {
                Some(hook)
            } else {
                artifact_dir.and_then(|dir| {
                    ReplayHook::from_artifact_dir(Path::new(&dir))
                        .ok()
                        .map(RuntimeHook::Replay)
                })
            }
        }
        Some("disabled") | Some("off") | Some("none") => None,
        // Mode must be explicit: an artifact dir alone never turns recording
        // on (recording live traffic is an opt-in switch, not an inference).
        None => None,
        Some(other) => {
            eprintln!(
                "deja: unknown DEJA_MODE='{}', expected record|replay|disabled",
                other
            );
            None
        }
    }
}

static GLOBAL_RUNTIME_HOOK: OnceLock<Option<Arc<RuntimeHook>>> = OnceLock::new();

/// Process-wide [`RuntimeHook`] initialized once from environment configuration.
pub fn global_runtime_hook_from_env() -> Option<Arc<RuntimeHook>> {
    GLOBAL_RUNTIME_HOOK
        .get_or_init(|| runtime_hook_from_env().map(Arc::new))
        .clone()
}

/// Install an explicitly-constructed [`RuntimeHook`] as the process-wide hook.
///
/// This is the injection point applications use when they want to construct
/// the hook with a custom sink (typically a [`crate::writer::CompositeSink`]
/// fanning a JSONL primary out to one or more secondaries supplied by the
/// application — e.g. Hyperswitch's Kafka producer). Must be called BEFORE
/// any code path invokes [`global_runtime_hook_from_env`]; returns
/// `Err` if the hook has already been initialized.
pub fn set_global_runtime_hook(hook: Option<RuntimeHook>) -> Result<(), &'static str> {
    GLOBAL_RUNTIME_HOOK
        .set(hook.map(Arc::new))
        .map_err(|_| "global runtime hook already initialized")
}

/// Flush the global [`RuntimeHook`] when one is configured.
pub fn flush_global_runtime_hook() -> std::io::Result<()> {
    if let Some(hook) = global_runtime_hook_from_env() {
        hook.flush()
    } else {
        Ok(())
    }
}

/// Peek the explicitly installed runtime hook WITHOUT initializing anything:
/// `None` both before install and when the installed hook is disabled-by-config.
/// This is the seam logger/layer setup uses to wire the execution-graph layer
/// to the mode's record stream after boot has installed the hook.
pub fn installed_runtime_hook() -> Option<Arc<RuntimeHook>> {
    GLOBAL_RUNTIME_HOOK.get().and_then(Clone::clone)
}

// Graph capture is NOT a separate dial: the execution-graph layer is coupled to
// the runtime mode (installed Record/Replay hook), exactly like the correlation
// layer. It rides whichever record/replay stream is active or it does not exist
// — there is no independent on/off knob to leave silently off.

// ---------------------------------------------------------------------------
// Builder for BoundaryEvent (used by generated delegation code)
// ---------------------------------------------------------------------------

/// Captured output of a boundary call.
///
/// Existing extractors that return `(serde_json::Value, bool)` convert into this
/// type unchanged. Boundaries that can explicitly observe state may additionally
/// attach dynamic read/write keys and typed pre/post images; the recorder never
/// infers these from method names, arguments, or result shapes.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedOutput {
    /// The serialized result stored in `BoundaryEvent::result` and sent to replay
    /// shadow observation.
    pub result: serde_json::Value,
    /// Whether `result` represents an error arm.
    pub is_error: bool,
    /// Additional explicit read keys derived by the extractor from the concrete
    /// output.
    pub read_set: Vec<String>,
    /// Additional explicit write keys derived by the extractor from the concrete
    /// output.
    pub write_set: Vec<String>,
    /// Explicit post-image of affected state after the boundary completed.
    pub result_image: Option<serde_json::Value>,
    /// Explicit pre-image of affected state before the boundary completed.
    pub pre_image: Option<serde_json::Value>,
}

impl RecordedOutput {
    /// Build a captured output from the recorded result and error flag.
    pub fn new(result: serde_json::Value, is_error: bool) -> Self {
        Self {
            result,
            is_error,
            read_set: Vec::new(),
            write_set: Vec::new(),
            result_image: None,
            pre_image: None,
        }
    }

    /// Attach additional explicit read keys.
    pub fn with_read_set(mut self, keys: Vec<String>) -> Self {
        self.read_set = keys;
        self
    }

    /// Attach additional explicit write keys.
    pub fn with_write_set(mut self, keys: Vec<String>) -> Self {
        self.write_set = keys;
        self
    }

    /// Append one explicit read key, preserving first occurrence order.
    pub fn with_read_key(mut self, key: impl Into<String>) -> Self {
        push_state_key_once(&mut self.read_set, key.into());
        self
    }

    /// Append one explicit write key, preserving first occurrence order.
    pub fn with_write_key(mut self, key: impl Into<String>) -> Self {
        push_state_key_once(&mut self.write_set, key.into());
        self
    }

    /// Attach an explicit post-image.
    pub fn with_result_image(mut self, image: serde_json::Value) -> Self {
        self.result_image = Some(image);
        self
    }

    /// Attach an explicit pre-image.
    pub fn with_pre_image(mut self, image: serde_json::Value) -> Self {
        self.pre_image = Some(image);
        self
    }
}

impl From<(serde_json::Value, bool)> for RecordedOutput {
    fn from((result, is_error): (serde_json::Value, bool)) -> Self {
        Self::new(result, is_error)
    }
}

impl From<RecordedOutput> for (serde_json::Value, bool) {
    fn from(output: RecordedOutput) -> Self {
        (output.result, output.is_error)
    }
}

fn push_state_key_once(keys: &mut Vec<String>, key: String) {
    if !keys.iter().any(|existing| existing == &key) {
        keys.push(key);
    }
}

/// Builder used by generated delegation macros to construct events.
///
/// Captures the "before" state (call site, args, timestamp), then finalizes
/// with the result after the inner call completes.
pub struct EventBuilder {
    pub global_sequence: u64,
    pub request_sequence: u64,
    pub correlation_id: Option<String>,
    pub start_ns: u64,
    pub boundary: &'static str,
    pub trait_name: &'static str,
    pub method_name: &'static str,
    pub call_file: &'static str,
    pub call_line: u32,
    pub call_column: u32,
    pub graph_node_id: Option<u64>,
    pub tracing_span_id: Option<u64>,
    pub receiver: Option<serde_json::Value>,
    pub args: serde_json::Value,
    explicit_args: Option<serde_json::Value>,
    explicit_read_set: Option<Vec<String>>,
    explicit_write_set: Option<Vec<String>>,
    explicit_output: Option<serde_json::Value>,
    explicit_result_image: Option<serde_json::Value>,
    explicit_pre_image: Option<serde_json::Value>,
    /// Optional structured call-site identity attached to the emitted event.
    pub callsite_identity: Option<CallsiteIdentity>,
    /// DECLARED boundary semantics (declarative boundary model). Defaults to
    /// undeclared (all `None`), so a builder constructed without declarations
    /// stamps the same event it did before this slice. Populated from the
    /// [`BoundarySpec`] in `start_boundary_event_lazy` and written into the
    /// emitted [`BoundaryEvent`] by [`Self::finish`].
    pub semantics: BoundarySemantics,
}

/// Stable content digest over `(args, result)`, reusing the same canonical
/// hashing as args-pairing so the value is byte-identical across binaries and is
/// never a second hash function. The cheapest dataflow hint — a write whose
/// digest matches an upstream read's is a probable read→write edge.
fn value_digest_of(args: &serde_json::Value, result: &serde_json::Value) -> u64 {
    let a = crate::replay::canonical_args_hash(args);
    let r = crate::replay::canonical_args_hash(result);
    fnv1a_u64(a, r)
}

impl EventBuilder {
    /// Start building an event. Call this before the inner method.
    ///
    /// `caller` must be `'static` — always satisfied by `Location::caller()`
    /// inside a `#[track_caller]` function.
    pub fn start(
        hook: &dyn DejaHook,
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
        caller: &'static Location<'static>,
        args: serde_json::Value,
    ) -> Self {
        Self::start_with_receiver(hook, boundary, trait_name, method_name, caller, None, args)
    }

    /// Start building an event with an explicit correlation ID.
    ///
    /// The explicit value is used when present; otherwise the ambient
    /// `deja_context` correlation ID is used.
    pub fn start_with_correlation_id(
        hook: &dyn DejaHook,
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
        caller: &'static Location<'static>,
        correlation_id: Option<String>,
        args: serde_json::Value,
    ) -> Self {
        Self::start_with_receiver_and_correlation_id(
            hook,
            boundary,
            trait_name,
            method_name,
            caller,
            None,
            correlation_id,
            args,
        )
    }

    /// Start building an event with receiver/decorator context.
    pub fn start_with_receiver(
        hook: &dyn DejaHook,
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
        caller: &'static Location<'static>,
        receiver: Option<serde_json::Value>,
        args: serde_json::Value,
    ) -> Self {
        Self::start_with_receiver_and_correlation_id(
            hook,
            boundary,
            trait_name,
            method_name,
            caller,
            receiver,
            None,
            args,
        )
    }

    /// Start building an event with receiver context and optional explicit correlation.
    #[allow(clippy::too_many_arguments)] // mirrors the macro-generated call shape
    pub fn start_with_receiver_and_correlation_id(
        hook: &dyn DejaHook,
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
        caller: &'static Location<'static>,
        receiver: Option<serde_json::Value>,
        explicit_correlation_id: Option<String>,
        args: serde_json::Value,
    ) -> Self {
        let correlation_id = explicit_correlation_id.or_else(deja_context::current_correlation_id);
        let (tracing_span_id, graph_node_id) = current_execution_graph_context();
        let global_sequence = hook.next_global_sequence();
        let request_sequence = hook.next_request_sequence(correlation_id.as_deref());

        Self {
            global_sequence,
            request_sequence,
            correlation_id,
            start_ns: now_ns(),
            boundary,
            trait_name,
            method_name,
            call_file: caller.file(),
            call_line: caller.line(),
            call_column: caller.column(),
            graph_node_id,
            tracing_span_id,
            receiver,
            args,
            explicit_args: None,
            explicit_read_set: None,
            explicit_write_set: None,
            explicit_output: None,
            explicit_result_image: None,
            explicit_pre_image: None,
            callsite_identity: None,
            semantics: BoundarySemantics::undeclared(),
        }
    }

    /// Attach a structured call-site identity to the event under construction.
    pub fn with_callsite_identity(mut self, identity: CallsiteIdentity) -> Self {
        self.callsite_identity = Some(identity);
        self
    }

    /// Attach DECLARED boundary semantics so they are stamped onto the emitted
    /// event. Additive: the default ([`BoundarySemantics::undeclared`]) leaves
    /// every declared field `None`, stamping the same event as before.
    pub fn with_semantics(mut self, semantics: BoundarySemantics) -> Self {
        self.semantics = semantics;
        self
    }

    /// Replace the emitted call payload before finalization.
    ///
    /// Both `request` and `args` in the emitted event use this value. State-key
    /// capture remains explicit; replacing args never infers read/write keys.
    pub fn record_call_to(mut self, args: serde_json::Value) -> Self {
        self.explicit_args = Some(args);
        self
    }

    /// Mark this boundary crossing as reading one state key.
    ///
    /// This is a convenience for single-key reads. It also makes the write side
    /// explicit empty unless a write set is supplied later.
    pub fn state_read_to(mut self, key: impl Into<String>) -> Self {
        self.explicit_read_set = Some(vec![key.into()]);
        if self.explicit_write_set.is_none() {
            self.explicit_write_set = Some(Vec::new());
        }
        self
    }

    /// Mark this boundary crossing as writing one state key.
    ///
    /// This is a convenience for single-key writes. It also makes the read side
    /// explicit empty unless a read set is supplied later.
    pub fn state_write_to(mut self, key: impl Into<String>) -> Self {
        if self.explicit_read_set.is_none() {
            self.explicit_read_set = Some(Vec::new());
        }
        self.explicit_write_set = Some(vec![key.into()]);
        self
    }

    /// Mark this boundary crossing as both reading and writing one state key.
    pub fn state_touch_to(mut self, key: impl Into<String>) -> Self {
        let key = key.into();
        self.explicit_read_set = Some(vec![key.clone()]);
        self.explicit_write_set = Some(vec![key]);
        self
    }

    /// Replace the emitted read set with an explicit key list.
    /// If no write set was supplied, the write side is made explicitly empty.
    pub fn with_read_set(mut self, keys: Vec<String>) -> Self {
        self.explicit_read_set = Some(keys);
        if self.explicit_write_set.is_none() {
            self.explicit_write_set = Some(Vec::new());
        }
        self
    }

    /// Replace the emitted write set with an explicit key list.
    /// If no read set was supplied, the read side is made explicitly empty.
    pub fn with_write_set(mut self, keys: Vec<String>) -> Self {
        if self.explicit_read_set.is_none() {
            self.explicit_read_set = Some(Vec::new());
        }
        self.explicit_write_set = Some(keys);
        self
    }

    /// Replace the emitted output before finalization.
    ///
    /// Both `response` and `result` in the emitted event use this value, and
    /// `value_digest` is computed from it rather than from the `finish` argument.
    pub fn with_output(mut self, output: serde_json::Value) -> Self {
        self.explicit_output = Some(output);
        self
    }

    /// Attach an explicit post-image to the emitted event.
    pub fn with_result_image(mut self, image: serde_json::Value) -> Self {
        self.explicit_result_image = Some(image);
        self
    }

    /// Attach an explicit pre-image to the emitted event.
    pub fn with_pre_image(mut self, image: serde_json::Value) -> Self {
        self.explicit_pre_image = Some(image);
        self
    }

    /// Finalize with an already-normalized capture payload.
    pub fn finish_recorded(self, hook: &dyn DejaHook, output: RecordedOutput) {
        let mut builder = self;
        for key in output.read_set {
            let read_set = builder.explicit_read_set.get_or_insert_with(Vec::new);
            push_state_key_once(read_set, key);
        }
        for key in output.write_set {
            let write_set = builder.explicit_write_set.get_or_insert_with(Vec::new);
            push_state_key_once(write_set, key);
        }
        if let Some(image) = output.result_image {
            builder.explicit_result_image = Some(image);
        }
        if let Some(image) = output.pre_image {
            builder.explicit_pre_image = Some(image);
        }
        builder.finish(hook, output.result, output.is_error);
    }

    /// Finalize the event with the result and send it to the hook.
    pub fn finish(self, hook: &dyn DejaHook, result: serde_json::Value, is_error: bool) {
        let EventBuilder {
            global_sequence,
            request_sequence,
            correlation_id,
            start_ns,
            boundary,
            trait_name,
            method_name,
            call_file,
            call_line,
            call_column,
            graph_node_id,
            tracing_span_id,
            receiver,
            args,
            explicit_args,
            explicit_read_set,
            explicit_write_set,
            explicit_output,
            explicit_result_image,
            explicit_pre_image,
            callsite_identity,
            semantics,
            ..
        } = self;

        let end_ns = now_ns();
        let duration_us = end_ns.saturating_sub(start_ns) / 1_000;

        let recording_run_id = hook
            .recording_run_id()
            .map(String::from)
            .or_else(current_recording_run_id);

        let args = explicit_args.unwrap_or(args);
        let result = explicit_output.unwrap_or(result);

        let read_set = explicit_read_set.unwrap_or_default();
        let write_set = explicit_write_set.unwrap_or_default();

        let value_digest = Some(value_digest_of(&args, &result));
        // Entropy source: retain the legacy id/time provenance label.
        let entropy_source = match boundary {
            "id" => Some("id".to_string()),
            "time" => Some("time".to_string()),
            _ => None,
        };
        let TaskMetadata {
            task_id,
            parent_task_id,
            task_bucket,
            bucket_id,
            fork_seq,
        } = current_task_metadata(correlation_id.as_deref());

        let event = BoundaryEvent {
            global_sequence,
            request_sequence,
            correlation_id,
            timestamp_ns: start_ns,
            recording_run_id,
            graph_node_id,
            tracing_span_id,
            task_id,
            parent_task_id,
            task_bucket,
            bucket_id,
            fork_seq,
            boundary: boundary.to_string(),
            trait_name: trait_name.to_string(),
            method_name: method_name.to_string(),
            call_file: call_file.to_string(),
            call_line,
            call_column,
            receiver,
            request: args.clone(),
            args,
            response: result.clone(),
            result,
            is_error,
            duration_us,
            event_schema_version: CURRENT_EVENT_SCHEMA_VERSION,
            callsite_identity,
            provenance: Provenance::default(),
            fidelity: Fidelity::default(),
            result_image: explicit_result_image,
            pre_image: explicit_pre_image,
            read_set,
            write_set,
            value_digest,
            entropy_source,
            // The per-site replay knob (#28). Defaults to `Substitute`; populated
            // from the declared `replay_strategy` on opt-in `Execute` sites. `kind`
            // is the non-routing descriptive label.
            replay_strategy: semantics.replay_strategy,
            kind: semantics.kind,
            declaration: semantics
                .declaration
                .filter(|declaration| !declaration.is_empty()),
            raw_draw: None,
            end_timestamp_ns: Some(end_ns),
        };

        hook.record(event);
    }
}

/// Inject captured body bytes into a result JSON object under the
/// `response_body` key.
fn inject_body_json(result: &mut serde_json::Value, bytes: Vec<u8>) {
    let body_json = if bytes.is_empty() {
        serde_json::json!({
            "captured": false,
            "reason": "empty body or stream incomplete",
        })
    } else {
        let bytes_len = bytes.len();
        let text = std::str::from_utf8(&bytes).ok().map(str::to_string);
        let parsed = text
            .as_deref()
            .and_then(|t| serde_json::from_str::<serde_json::Value>(t).ok());
        serde_json::json!({
            "captured": true,
            "bytes_len": bytes_len,
            "utf8": text.is_some(),
            "text": text,
            "json": parsed,
            "raw_bytes": bytes,
        })
    };

    if let serde_json::Value::Object(ref mut map) = result {
        map.insert("response_body".to_string(), body_json);
    }
}

/// A deferred event finalizer for boundaries where the complete result
/// is not known until after an async stream (e.g. HTTP response body)
/// has been fully consumed.
///
/// Typical usage:
/// 1. Start the event with `EventBuilder::start(...)`
/// 2. Create a `LazyEventFinalizer` with the partial result you know
///    at boundary-start time (status code, headers, etc.)
/// 3. Append streamed chunks via `finalizer.capture_chunk(...)`
/// 4. When the stream completes, call `finalizer.finalize()` to emit
///    the event with the full result including the buffered bytes.
pub struct LazyEventFinalizer {
    builder: Option<EventBuilder>,
    hook: Option<Arc<dyn DejaHook>>,
    partial_result: serde_json::Value,
    is_error: bool,
    body: Vec<u8>,
}

impl LazyEventFinalizer {
    /// Create a new lazy finalizer.
    pub fn new(
        builder: EventBuilder,
        hook: Arc<dyn DejaHook>,
        partial_result: serde_json::Value,
        is_error: bool,
    ) -> Self {
        Self {
            builder: Some(builder),
            hook: Some(hook),
            partial_result,
            is_error,
            body: Vec::new(),
        }
    }

    /// Append a response-body chunk to the full-fidelity capture buffer.
    pub fn capture_chunk(&mut self, chunk: &[u8]) {
        self.body.extend_from_slice(chunk);
    }

    /// Return the correlation id this finalizer will stamp, without consuming it.
    pub fn correlation_id(&self) -> Option<&str> {
        self.builder
            .as_ref()
            .and_then(|builder| builder.correlation_id.as_deref())
    }

    /// Consume the finalizer, build the complete result (partial result +
    /// captured body bytes), emit the event, and return the finalized
    /// correlation id for ingress cleanup/bookkeeping.
    pub fn finalize(mut self) -> Option<String> {
        let builder = self.builder.take().expect("already finalized");
        let correlation_id = builder.correlation_id.clone();
        let hook = self.hook.take().expect("already finalized");

        let mut result = self.partial_result.clone();
        inject_body_json(&mut result, std::mem::take(&mut self.body));

        builder.finish(&*hook, result, self.is_error);
        let _ = hook.flush();
        clear_fork_counter(correlation_id.as_deref(), ROOT_TASK_ID);
        correlation_id
    }
}

impl Drop for LazyEventFinalizer {
    fn drop(&mut self) {
        let cleanup_correlation_id = self
            .builder
            .as_ref()
            .map(|builder| builder.correlation_id.clone());
        // SHADOW GUARANTEE: never finalize while the thread is already unwinding.
        // If the real call panicked, this finalizer is dropped mid-unwind; running
        // `finish` (which can itself panic on serialization/locks) during an unwind
        // escalates to `abort()` and kills the whole process. Drop the event instead.
        if std::thread::panicking() {
            if let Some(correlation_id) = cleanup_correlation_id {
                clear_fork_counter(correlation_id.as_deref(), ROOT_TASK_ID);
            }
            return;
        }
        if self.builder.is_some() {
            if let (Some(builder), Some(hook)) = (self.builder.take(), self.hook.take()) {
                let mut result = self.partial_result.clone();
                inject_body_json(&mut result, std::mem::take(&mut self.body));

                // And firewall the normal-path finalize too, so a serialization
                // panic here never escapes into the caller.
                let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    builder.finish(&*hook, result, self.is_error);
                }));
                let _ = hook.flush();
            }
        }
        if let Some(correlation_id) = cleanup_correlation_id {
            clear_fork_counter(correlation_id.as_deref(), ROOT_TASK_ID);
        }
    }
}

/// Infer whether a serialized return payload represents an error.
///
/// Serde serializes `Result<T, E>` as `{"Ok": ...}` or `{"Err": ...}`. Using
/// that shape keeps the generated recording path independent of a concrete
/// `Result` return type while preserving error reporting for normal results.
pub fn serialized_result_is_error(result: &serde_json::Value) -> bool {
    matches!(
        result,
        serde_json::Value::Object(map) if map.contains_key("Err")
    )
}

// ---------------------------------------------------------------------------
// Environment-based factory
// ---------------------------------------------------------------------------

/// Initialize a recording hook from environment variables.
///
/// Reads:
/// - `DEJA_MODE`: "record" to enable recording (default: disabled)
/// - `DEJA_ARTIFACT_DIR`: directory for output files (required when recording)
///
/// Returns `None` if disabled or misconfigured.
pub fn hook_from_env() -> Option<RecordingHook> {
    let mode = std::env::var("DEJA_MODE").unwrap_or_default();
    if mode != "record" {
        return None;
    }
    let dir = std::env::var("DEJA_ARTIFACT_DIR").ok()?;
    RecordingHook::new(Path::new(&dir)).ok()
}

static GLOBAL_RECORDING_HOOK: OnceLock<Option<Arc<RecordingHook>>> = OnceLock::new();

/// Shared recording hook initialized once from `DEJA_MODE` and `DEJA_ARTIFACT_DIR`.
///
/// If an application has installed a recording [`RuntimeHook`] via
/// [`set_global_runtime_hook`] (e.g. Hyperswitch's `deja_boot`, which composes a
/// Kafka secondary onto the JSONL primary), this returns that hook's SHARED
/// `RecordingHook` so callers of this getter use the exact same recorder as
/// callers of [`global_runtime_hook_from_env`]. That unification is what keeps a
/// single `global_sequence` counter and a single sink set across every boundary
/// — regardless of which resolver a given boundary happens to call.
///
/// The runtime hook is only PEEKED (`get`, never `get_or_init`): we must not
/// pre-empt the install-before-getter ordering contract documented on
/// [`set_global_runtime_hook`]. When an explicit runtime hook has been installed,
/// that typed hook is authoritative: non-recording hooks suppress the legacy
/// standalone env recorder. Only when no runtime hook has been initialized does
/// this fall back to the env-derived `GLOBAL_RECORDING_HOOK`.
pub fn global_hook_from_env() -> Option<Arc<RecordingHook>> {
    if let Some(runtime) = GLOBAL_RUNTIME_HOOK.get() {
        return match runtime {
            Some(hook) => match hook.as_ref() {
                RuntimeHook::Recording(hook) => Some(Arc::clone(hook)),
                RuntimeHook::Replay(_)
                | RuntimeHook::LookupReplay(_)
                | RuntimeHook::Disabled(_) => None,
            },
            None => None,
        };
    }
    GLOBAL_RECORDING_HOOK
        .get_or_init(|| hook_from_env().map(Arc::new))
        .as_ref()
        .cloned()
}

/// Whether ANY hook that consumes a boundary's args is active — the runtime
/// (replay/execute) hook OR the standalone recording hook. The boundary macro
/// uses this to decide whether to EAGERLY evaluate the args expression into an
/// owned `serde_json::Value` *before* forming the run closure.
///
/// Why eager-when-active rather than a lazy thunk: a lazy args *thunk* and the
/// run *thunk* are handed to `dispatch` simultaneously, so a thunk that borrows
/// a value (e.g. an http boundary whose `args`/`correlation` borrow `&request`)
/// cannot coexist with a run thunk that MOVES that same value (the body sends
/// `request`). Evaluating args to an owned value first ends the borrow before
/// the move. Gating on this keeps the inactive path zero-cost: when nothing is
/// capturing, the macro never runs the args expression at all.
///
/// This mirrors the paths that actually consume args: `dispatch` evaluates args
/// when the runtime mode consumes args, and the record-only path evaluates args
/// only when the recording hook is still in [`RuntimeMode::Record`] after the
/// sampling gate. A sampled-out recorder must not serialize args just to return
/// before `EventBuilder::start`.
pub fn capture_is_active() -> bool {
    runtime_mode().consumes_args()
        || global_hook_from_env()
            .map(|hook| hook.mode().is_record())
            .unwrap_or(false)
}

/// The explicit process-wide runtime mode.
///
/// Returns the installed global runtime hook mode, or [`RuntimeMode::Disabled`] when
/// no runtime hook is configured.
pub fn runtime_mode() -> RuntimeMode {
    global_runtime_hook_from_env()
        .map(|hook| hook.mode())
        .unwrap_or(RuntimeMode::Disabled)
}

/// Whether this process is replaying recorded results.
///
/// Compatibility wrapper around [`runtime_mode`] for call sites that only need
/// the replay/no-replay predicate.
pub fn replay_is_active() -> bool {
    runtime_mode().is_replay()
}

const ROOT_TASK_ID: &str = "root";

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TaskLineage {
    task_id: String,
    parent_task_id: Option<String>,
    bucket_id: String,
    fork_seq: u64,
}

impl TaskLineage {
    fn root() -> Self {
        Self {
            task_id: ROOT_TASK_ID.to_string(),
            parent_task_id: None,
            bucket_id: ROOT_TASK_ID.to_string(),
            fork_seq: 0,
        }
    }

    /// Lineage for a spawned-task fork: a fresh bucket under the parent, keyed by
    /// a `(correlation, parent bucket)`-local sequence. Called by the correlation
    /// layer when it sees a `deja.fork`-marked span — never from a task-local.
    fn forked_child_of(parent: Self, correlation_id: Option<&str>) -> Self {
        let fork_seq = next_fork_seq(correlation_id, &parent.bucket_id);
        Self {
            task_id: format!("{}::fork-{fork_seq}", parent.task_id),
            parent_task_id: Some(parent.task_id),
            bucket_id: format!("{}::fork-{fork_seq}", parent.bucket_id),
            fork_seq,
        }
    }
}

/// Per-(correlation, parent bucket) counter key for detached fork sequences.
type ForkCounterKey = (Option<String>, String);

static FORK_COUNTERS: LazyLock<Mutex<HashMap<ForkCounterKey, u64>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

fn next_fork_seq(correlation_id: Option<&str>, parent_bucket_id: &str) -> u64 {
    let key = (
        correlation_id.map(str::to_owned),
        parent_bucket_id.to_owned(),
    );
    let mut counters = FORK_COUNTERS
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let next = counters.entry(key).or_insert(1);
    let fork_seq = *next;
    *next += 1;
    fork_seq
}

fn clear_fork_counter(correlation_id: Option<&str>, parent_bucket_id: &str) {
    if let Ok(mut counters) = FORK_COUNTERS.lock() {
        counters.remove(&(
            correlation_id.map(str::to_owned),
            parent_bucket_id.to_owned(),
        ));
    }
}

/// The task lineage active on this thread, derived from the entered `tracing`
/// span tree by [`crate::correlation_layer`] — the span-based replacement for the
/// former `CURRENT_TASK_LINEAGE` task-local and its `spawn_detached` writer.
pub(crate) fn current_task_lineage() -> TaskLineage {
    crate::correlation_layer::current_span_lineage()
}

/// Lineage facts stamped on every event, named so call sites cannot swap the
/// two bucket-labelled fields (`task_bucket` is the legacy wire label,
/// `bucket_id` the lineage cell).
pub(crate) struct TaskMetadata {
    pub task_id: Option<String>,
    pub parent_task_id: Option<String>,
    pub task_bucket: Option<String>,
    pub bucket_id: Option<String>,
    pub fork_seq: Option<u64>,
}

pub(crate) fn current_task_metadata(_correlation_id: Option<&str>) -> TaskMetadata {
    let lineage = current_task_lineage();
    let bucket_id = lineage.bucket_id;
    TaskMetadata {
        task_id: Some(lineage.task_id),
        parent_task_id: lineage.parent_task_id,
        task_bucket: Some(bucket_id.clone()),
        bucket_id: Some(bucket_id),
        fork_seq: Some(lineage.fork_seq),
    }
}

/// Spawn a fork boundary for fire-and-forget work: `tokio::spawn` the future
/// instrumented with [`fork_span`], so the execution-graph/correlation layer sees
/// a `deja.fork`-marked span, opens a fresh lineage bucket for the child, and
/// derives its `task_id`/`bucket_id`/`fork_seq` from the span tree — no captured
/// task-locals. Boundary events inside the child inherit the request correlation
/// via the same layer. Provided so callers keep one obvious spelling; a bare
/// `tokio::spawn(fut.instrument(deja::fork_span()))` is equivalent.
pub fn spawn_fork<F, T>(future: F)
where
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    // Dropping the JoinHandle is deliberate — fire-and-forget by contract.
    drop(tokio::spawn(
        async move {
            let _ = future.await;
        }
        .instrument(fork_span()),
    ));
}

/// The span that marks a spawned-task fork boundary for the correlation/graph
/// layer (`deja.fork = true`). Instrument a spawned future with it —
/// `tokio::spawn(fut.instrument(deja::fork_span()))` — to open a fresh lineage
/// bucket for that task.
#[must_use]
pub fn fork_span() -> tracing::Span {
    tracing::info_span!("deja.fork", deja.fork = true)
}

/// Flush the global recording hook, when one is configured.
///
/// The recording hook may live in EITHER `GLOBAL_RECORDING_HOOK` (when
/// resolved standalone) OR inside `GLOBAL_RUNTIME_HOOK` as
/// `RuntimeHook::Recording` (when the runtime hook was initialized first — e.g.
/// because a boundary allocated a callsite occurrence via the runtime hook
/// before any event was recorded). Flush whichever holds it so events are not
/// silently left unflushed.
pub fn flush_global_hook() -> std::io::Result<()> {
    if let Some(hook) = GLOBAL_RECORDING_HOOK.get().and_then(|hook| hook.as_ref()) {
        return hook.flush();
    }
    if let Some(Some(runtime)) = GLOBAL_RUNTIME_HOOK.get() {
        if let RuntimeHook::Recording(hook) = runtime.as_ref() {
            return hook.flush();
        }
    }
    Ok(())
}

/// Record a completed semantic event from hand-written boundary hooks.
#[track_caller]
pub fn record_semantic_event(
    boundary: &'static str,
    trait_name: &'static str,
    method_name: &'static str,
    request: serde_json::Value,
    response: serde_json::Value,
    is_error: bool,
) {
    let Some(hook) = global_hook_from_env() else {
        return;
    };

    record_semantic_event_with_hook(
        hook,
        boundary,
        trait_name,
        method_name,
        request,
        response,
        is_error,
        Location::caller(),
    );
}

#[allow(clippy::too_many_arguments)]
fn record_semantic_event_with_hook(
    hook: Arc<RecordingHook>,
    boundary: &'static str,
    trait_name: &'static str,
    method_name: &'static str,
    request: serde_json::Value,
    response: serde_json::Value,
    is_error: bool,
    caller: &'static Location<'static>,
) {
    if !hook.mode().is_record() {
        return;
    }

    let builder = EventBuilder::start(&*hook, boundary, trait_name, method_name, caller, request);
    builder.finish(&*hook, response, is_error);
}

/// Static semantic boundary metadata used by generated boundary wrappers.
///
/// Carries the matching tuple (boundary/trait/method), the per-site
/// [`ReplayStrategy`] knob (+ a non-routing `kind` label), and optional typed
/// declaration metadata for seed planning/reporting. [`Self::new`] gives the
/// default (`Substitute`, no label/declaration) so every legacy call site keeps
/// the safe never-re-run behavior; [`Self::with_semantics`] carries declared
/// metadata.
#[derive(Debug, Clone)]
pub struct BoundarySpec {
    pub boundary: &'static str,
    pub trait_name: &'static str,
    pub method_name: &'static str,
    /// The per-site replay knob (routing source of truth). Default `Substitute`.
    pub replay_strategy: ReplayStrategy,
    /// Free-text descriptive label ("db"/"http"/"redis") for dashboard /
    /// provenance. NOT routing. `None` when undeclared.
    pub kind: Option<String>,
    /// Typed declarative metadata for seed planning/reporting.
    pub declaration: Option<BoundaryDeclaration>,
}

impl BoundarySpec {
    /// Construct a boundary spec with the DEFAULT knob (`Substitute`, no label).
    /// Kept identical to the pre-declarative signature so every existing call site
    /// compiles unchanged and behaves safely (the boundary is never re-run).
    pub const fn new(
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
    ) -> Self {
        Self {
            boundary,
            trait_name,
            method_name,
            replay_strategy: ReplayStrategy::Substitute,
            kind: None,
            declaration: None,
        }
    }

    /// Construct a boundary spec carrying declared [`BoundarySemantics`] (the
    /// per-site knob + optional descriptive/typed metadata). Used by the declarative macro path.
    pub fn with_semantics(
        boundary: &'static str,
        trait_name: &'static str,
        method_name: &'static str,
        semantics: BoundarySemantics,
    ) -> Self {
        Self {
            boundary,
            trait_name,
            method_name,
            replay_strategy: semantics.replay_strategy,
            kind: semantics.kind,
            declaration: semantics.declaration,
        }
    }

    /// The declared semantics carried by this spec, as a [`BoundarySemantics`].
    pub fn semantics(&self) -> BoundarySemantics {
        BoundarySemantics {
            replay_strategy: self.replay_strategy,
            kind: self.kind.clone(),
            declaration: self.declaration.clone(),
        }
    }
}

/// Record an async function boundary without changing the function body.
pub async fn record_boundary_async<F, T, R, O>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: serde_json::Value,
    future: F,
    result: R,
) -> T
where
    F: Future<Output = T>,
    R: FnOnce(&T) -> O,
    O: Into<RecordedOutput>,
{
    let event = start_boundary_event(caller, spec, correlation_id, args, None);
    let output = future.await;
    finish_boundary_event(event, &output, result);
    output
}

/// Record an async function boundary while constructing args only when active.
pub async fn record_boundary_async_lazy<F, T, A, R, O>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: A,
    future: F,
    result: R,
) -> T
where
    F: Future<Output = T>,
    A: FnOnce() -> serde_json::Value,
    R: FnOnce(&T) -> O,
    O: Into<RecordedOutput>,
{
    let event = start_boundary_event_lazy(caller, spec, correlation_id, args, None);
    let output = future.await;
    finish_boundary_event(event, &output, result);
    output
}

/// Record a synchronous function boundary without changing the function body.
pub fn record_boundary_sync<F, T, R, O>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: serde_json::Value,
    function: F,
    result: R,
) -> T
where
    F: FnOnce() -> T,
    R: FnOnce(&T) -> O,
    O: Into<RecordedOutput>,
{
    let event = start_boundary_event(caller, spec, correlation_id, args, None);
    let output = function();
    finish_boundary_event(event, &output, result);
    output
}

/// Record a synchronous function boundary while constructing args only when active.
pub fn record_boundary_sync_lazy<F, T, A, R, O>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: A,
    function: F,
    result: R,
) -> T
where
    F: FnOnce() -> T,
    A: FnOnce() -> serde_json::Value,
    R: FnOnce(&T) -> O,
    O: Into<RecordedOutput>,
{
    let event = start_boundary_event_lazy(caller, spec, correlation_id, args, None);
    let output = function();
    finish_boundary_event(event, &output, result);
    output
}

/// Allocate the next per-callsite occurrence index for a boundary invocation.
///
/// Resolves the process-wide runtime hook (which uniformly implements
/// [`DejaHook::next_callsite_occurrence`] across record / replay / lookup
/// modes) and bumps the counter keyed by `(correlation_id, source, scope)`.
/// In record mode the runtime hook shares the SAME `RecordingHook` the
/// recording path uses, so a single call here yields one consistent occurrence
/// for both the replay-lookup key and the recorded event. Returns `0` when no
/// hook is configured (inactive / no-op), which is harmless because nothing is
/// recorded or replayed in that state.
///
/// MUST be called EXACTLY ONCE per boundary invocation; the result is reused
/// for both the replay prelude and the recording path to keep record/replay
/// occurrence numbering aligned (rank-4 `Address::LexicalPath`).
pub fn next_boundary_occurrence(
    correlation_id: Option<&str>,
    source: CallsiteSource,
    scope: Option<&str>,
) -> u32 {
    match global_runtime_hook_from_env() {
        Some(hook) => hook.next_callsite_occurrence(correlation_id, source, scope),
        None => 0,
    }
}

/// Replay substitution for an instrumented boundary (the macro `replay` flag).
///
/// Resolves the process runtime hook and asks it to replay this call from the
/// recorded lookup table. In replay mode (a lookup-table hook) this returns
/// `Some(result_json)` — the macro deserializes it into the function's return
/// type and skips the live call. In record / no-op mode the hook does not
/// replay, so this returns `None` and the caller executes + records as usual.
/// The correlation id is read from the ambient `deja_context` inside the hook,
/// so only the structured args + call site are passed here.
/// `identity` is the structured [`CallsiteIdentity`] computed ONCE by the
/// boundary macro (or hand-built caller) for this invocation. It is threaded
/// into the [`ReplayLookup`] so the candidate hook can resolve at the stable
/// content/identity ranks (logical-context / syntactic-hash / lexical-path)
/// rather than the rank-6 positional fallback.
/// The SAME identity value must be reused for the recording path so the
/// renderer and the hook stamp identical occurrence keys.
///
/// Deprecated: replay branching now lives behind [`dispatch`], which calls this
/// internally. Direct callers (the pre-`dispatch` macro shape) are being
/// removed; new code routes through [`dispatch`] so the macro names no
/// replay-only operation.
#[deprecated(
    since = "0.1.0",
    note = "internal replay seam; route boundary instrumentation through `dispatch` instead"
)]
#[track_caller]
pub fn replay_boundary(
    caller: &'static Location<'static>,
    spec: &BoundarySpec,
    args: &serde_json::Value,
    identity: Option<&CallsiteIdentity>,
) -> Option<serde_json::Value> {
    let hook = global_runtime_hook_from_env()?;
    if !hook.is_active() {
        return None;
    }
    hook.try_replay_with_context(ReplayLookup {
        boundary: spec.boundary,
        trait_name: spec.trait_name,
        method_name: spec.method_name,
        args,
        callsite_identity: identity,
        caller_location: Some(caller),
    })
}

/// Resolve whether an instrumented boundary should run in execute mode.
///
/// Returns [`ExecuteMode::Lookup`] when no hook is configured, when the hook is
/// inactive, or when the active hook is not in replay mode. In replay mode the
/// decision comes solely from the boundary's declared [`ReplayStrategy`] via
/// [`crate::replay::boundary_execute_mode_for`].
///
/// Deprecated: the execute/shadow lifecycle now lives inside [`dispatch`].
#[deprecated(
    since = "0.1.0",
    note = "internal execute-mode seam; route boundary instrumentation through `dispatch` instead"
)]
pub fn boundary_execute_mode(spec: &BoundarySpec) -> ExecuteMode {
    match global_runtime_hook_from_env() {
        Some(hook) if hook.is_active() => crate::replay::boundary_execute_mode_for(&*hook, spec),
        _ => ExecuteMode::Lookup,
    }
}

/// First half of an execute-mode dispatch for an instrumented boundary: peek the
/// recorded baseline WITHOUT substituting and WITHOUT emitting a `Recorded`
/// observation, returning a token to carry across the live block. Mirrors
/// [`DejaHook::execute_shadow_peek`]; returns `None` when no hook is configured,
/// the hook is inactive, or the hook does not support shadowing (so the macro
/// falls back to lookup behavior).
///
/// Deprecated: the execute/shadow lifecycle now lives inside [`dispatch`].
#[deprecated(
    since = "0.1.0",
    note = "internal execute-shadow seam; route boundary instrumentation through `dispatch` instead"
)]
#[track_caller]
pub fn execute_shadow_peek_boundary(
    caller: &'static Location<'static>,
    spec: &BoundarySpec,
    args: &serde_json::Value,
    identity: Option<&CallsiteIdentity>,
) -> Option<ExecuteShadowToken> {
    let hook = global_runtime_hook_from_env()?;
    if !hook.is_active() {
        return None;
    }
    hook.execute_shadow_peek(ReplayLookup {
        boundary: spec.boundary,
        trait_name: spec.trait_name,
        method_name: spec.method_name,
        args,
        callsite_identity: identity,
        caller_location: Some(caller),
    })
}

/// Second half of an execute-mode dispatch for an instrumented boundary: emit
/// the shadow observation with the real block's `observed_result`. Mirrors
/// [`DejaHook::execute_shadow_observe`]. No-op when no hook is configured.
///
/// Deprecated: the execute/shadow lifecycle now lives inside [`dispatch`].
#[deprecated(
    since = "0.1.0",
    note = "internal execute-shadow seam; route boundary instrumentation through `dispatch` instead"
)]
pub fn execute_shadow_observe_boundary(
    token: ExecuteShadowToken,
    observed_result: serde_json::Value,
) {
    if let Some(hook) = global_runtime_hook_from_env() {
        hook.execute_shadow_observe(token, observed_result);
    }
}

// ---------------------------------------------------------------------------
// The single boundary-crossing seam (`dispatch`)
// ---------------------------------------------------------------------------

/// Explicit state keys captured by an instrumented boundary crossing.
///
/// Empty captures are authoritative: absent explicit read/write keys emit empty
/// event sets. The recorder does not infer state from boundary or method names.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct StateCapture {
    pub read_set: Vec<String>,
    pub write_set: Vec<String>,
    pub result_image: Option<serde_json::Value>,
    pub pre_image: Option<serde_json::Value>,
}

impl StateCapture {
    /// Empty explicit state capture.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Replace the explicit read-set.
    pub fn with_read_set(mut self, keys: Vec<String>) -> Self {
        self.read_set = keys;
        self
    }

    /// Replace the explicit write-set.
    pub fn with_write_set(mut self, keys: Vec<String>) -> Self {
        self.write_set = keys;
        self
    }

    /// Attach an explicit post-image.
    pub fn with_result_image(mut self, image: serde_json::Value) -> Self {
        self.result_image = Some(image);
        self
    }

    /// Attach an explicit pre-image.
    pub fn with_pre_image(mut self, image: serde_json::Value) -> Self {
        self.pre_image = Some(image);
        self
    }

    /// Capture a single state key read.
    pub fn state_read_to(mut self, key: impl Into<String>) -> Self {
        self.read_set = vec![key.into()];
        self.write_set.clear();
        self
    }

    /// Capture a single state key write.
    pub fn state_write_to(mut self, key: impl Into<String>) -> Self {
        self.read_set.clear();
        self.write_set = vec![key.into()];
        self
    }

    /// Capture a single state key that was both read and written.
    pub fn state_touch_to(mut self, key: impl Into<String>) -> Self {
        let key = key.into();
        self.read_set = vec![key.clone()];
        self.write_set = vec![key];
        self
    }

    fn apply_to(self, event: EventBuilder) -> EventBuilder {
        let mut event = event
            .with_read_set(self.read_set)
            .with_write_set(self.write_set);
        if let Some(image) = self.result_image {
            event = event.with_result_image(image);
        }
        if let Some(image) = self.pre_image {
            event = event.with_pre_image(image);
        }
        event
    }
}

/// Matching inputs and optional recording state capture the boundary macro hands
/// to [`dispatch`].
///
/// This carries what is needed to ADDRESS / MATCH a crossing against a
/// recording: the boundary tuple ([`BoundarySpec`]), the call-site
/// [`CallsiteIdentity`] (with its `occurrence` allocated exactly once by the
/// caller), and the `#[track_caller]` invocation [`Location`]. Optional
/// [`StateCapture`] is recording metadata only: it stamps the emitted event, but
/// is never a replay routing verdict.
pub struct CrossingObservation {
    /// Boundary tuple (boundary / trait / method) — to match a recording.
    pub spec: BoundarySpec,
    /// Structured call-site identity; `occurrence` allocated ONCE by the caller
    /// and reused for both the replay lookup and the recorded event.
    ///
    /// Held BY VALUE (not borrowed): the boxed-future macro shape returns the
    /// `dispatch_async` future from a sync fn, so the future — and everything it
    /// captures, including `obs` — must own its data rather than borrow a local.
    /// The internal seams that want `Option<&CallsiteIdentity>` borrow it from
    /// here; nothing is cloned on the hot lookup/execute paths.
    pub identity: CallsiteIdentity,
    /// `#[track_caller]` invocation address for legacy file:line:column matching.
    pub caller: &'static Location<'static>,
    /// Explicit correlation id for the recorded event, when the call site set
    /// one (`correlation = ...`). `None` falls back to the ambient
    /// `deja_context` correlation inside the record seam — the same fallback the
    /// pre-`dispatch` macro used. This is a recording address, not a replay
    /// verdict (it is the test-case isolation key, project memory).
    pub correlation_id: Option<String>,
    /// Explicit state capture to stamp on a recorded event. `None` and
    /// `Some(StateCapture::empty())` both emit empty sets unless keys are added.
    state_capture: Option<StateCapture>,
    // Phase C compatibility note: `fall_through_silent()` is retained as a
    // source-compatible no-op, but no field is carried because Substitute
    // HIT-but-unreconstructable replay always fail-stops before live execution.
}

impl CrossingObservation {
    /// Build a crossing observation from its matching inputs (no explicit
    /// correlation — the record seam falls back to the ambient one).
    pub fn new(
        spec: BoundarySpec,
        identity: CallsiteIdentity,
        caller: &'static Location<'static>,
    ) -> Self {
        Self {
            spec,
            identity,
            caller,
            correlation_id: None,
            state_capture: None,
        }
    }

    /// Build a crossing observation with an explicit correlation id.
    pub fn with_correlation(
        spec: BoundarySpec,
        identity: CallsiteIdentity,
        caller: &'static Location<'static>,
        correlation_id: Option<String>,
    ) -> Self {
        Self {
            spec,
            identity,
            caller,
            correlation_id,
            state_capture: None,
        }
    }

    /// Source-compatible no-op retained for legacy call sites.
    ///
    /// In Phase C, a Substitute lookup HIT whose recorded value cannot be
    /// reconstructed fail-stops before live execution. This method no longer
    /// grants a silent live fallback; declaring [`ReplayStrategy::Execute`] is the
    /// only replay path that may run the real boundary.
    pub fn fall_through_silent(self) -> Self {
        self
    }

    /// Attach an explicit read-set to the event recorded for this crossing.
    pub fn with_read_set(mut self, keys: Vec<String>) -> Self {
        self.state_capture = Some(self.state_capture.unwrap_or_default().with_read_set(keys));
        self
    }

    /// Attach an explicit write-set to the event recorded for this crossing.
    pub fn with_write_set(mut self, keys: Vec<String>) -> Self {
        self.state_capture = Some(self.state_capture.unwrap_or_default().with_write_set(keys));
        self
    }

    /// Attach an explicit post-image to the event recorded for this crossing.
    pub fn with_result_image(mut self, image: serde_json::Value) -> Self {
        self.state_capture = Some(
            self.state_capture
                .unwrap_or_default()
                .with_result_image(image),
        );
        self
    }

    /// Attach an explicit pre-image to the event recorded for this crossing.
    pub fn with_pre_image(mut self, image: serde_json::Value) -> Self {
        self.state_capture = Some(self.state_capture.unwrap_or_default().with_pre_image(image));
        self
    }

    /// Mark this crossing as reading one state key.
    pub fn state_read_to(mut self, key: impl Into<String>) -> Self {
        self.state_capture = Some(self.state_capture.unwrap_or_default().state_read_to(key));
        self
    }

    /// Mark this crossing as writing one state key.
    pub fn state_write_to(mut self, key: impl Into<String>) -> Self {
        self.state_capture = Some(self.state_capture.unwrap_or_default().state_write_to(key));
        self
    }

    /// Mark this crossing as both reading and writing one state key.
    pub fn state_touch_to(mut self, key: impl Into<String>) -> Self {
        self.state_capture = Some(self.state_capture.unwrap_or_default().state_touch_to(key));
        self
    }
}

/// The ONE replay-facing seam the boundary macro calls.
///
/// Recording captures raw observations; replay performs all interpretation
/// (design §1). This function owns ALL of the run/skip/shadow/record control
/// flow internally, so the macro emits a single mode-agnostic shape and names
/// ZERO replay-only operations. Removing every replay hook would leave this a
/// plain "run + (maybe) record" function and change the macro's emitted tokens
/// by zero.
///
/// The four closures the macro supplies:
/// - `args` — LAZY structured-args serialization. NOT evaluated when the hook
///   is inactive, preserving the zero-overhead fast path
///   (`start_boundary_event_lazy`'s laziness, design §3 / major #5).
/// - `run` — the real boundary block.
/// - `reconstruct` — turns a recorded JSON value back into `T` on a lookup hit.
///   It returns [`Reconstructed::Value`] when the payload rebuilds cleanly and
///   [`Reconstructed::Failed`] when the payload is malformed or incompatible; a
///   failed reconstruction fail-stops before live execution.
/// - `extract` — the lossless result image AND the `is_error` flag, as the
///   existing record/shadow seams expect. Fidelity is fixed by the macro, not
///   chosen by a replay flag.
///
/// Internally this is implemented in terms of the recording event seam plus the
/// deprecated replay/shadow seams [`replay_boundary`] and `execute_shadow_*`.
/// The outer branch is always the explicit [`RuntimeMode`], so record/no-op never
/// ask replay helpers for a verdict.
///
/// Control flow (all owned here, never named by the macro):
/// - **NoOp** → call the lazy record-only path, which runs `run()` and evaluates
///   `args` only if a standalone recorder is actually active.
/// - **Record** → call the same lazy record-only path; no replay lookup and no
///   execute-shadow lookup run in record mode.
/// - **Replay + Execute** → run `run()` only after an execute-shadow token is
///   returned, shadow-observe `extract(&out)`, and suppress the normal record.
/// - **Replay + Substitute hit** → reconstruct the recorded value WITHOUT
///   calling `run()`; `Failed` fail-stops. Live replay execution is reserved for
///   declared `Execute`.
/// - **Replay + Substitute miss** → fail-stop; there is no recorded value to
///   serve and live execution would be unsafe.
// NOTE: no `#[track_caller]` — the authoritative invocation address is
// `obs.caller`, captured by the macro at its own `#[track_caller]` entry and
// threaded through `CrossingObservation`. The internal seams receive it
// explicitly, so their own `Location::caller()` is never consulted.
///
/// Result of rebuilding a typed return value from a recorded replay payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reconstructed<T> {
    Value(T),
    Failed,
}

/// Replay fail-stop on a Substitute-miss (the partial-function model).
///
/// A `Substitute` (Lookup-mode) boundary whose lookup MISSED has no honest value
/// to serve — the recording holds a result for DIFFERENT args, or none at all.
/// Serving the stale value would lie to every downstream boundary (deja
/// substitutes results, never args, so a stale value flows into downstream args
/// and poisons the subtree); running the real boundary would hit prod / mutate the
/// shared store. So the only faithful continuation is to STOP. The blocking
/// divergence (an unresolved `ObservedCall` → NovelCall) was already emitted by
/// `replay_boundary` / `try_replay` before this point; this panics to unwind the
/// one request, discarding its in-process downstream subtree by construction. Each
/// request is an isolated test case, so the host's per-request panic isolation
/// scopes the stop to the one correlation. Declare `replay_strategy = Execute` on
/// the boundary to recompute the new value instead of stopping.
///
/// The panic is type-erased: a true miss never constructs the boundary's return
/// type `T`, so this works for any return type (no `Err` synthesis, no `Default`).
#[cold]
#[inline(never)]
pub fn fail_stop_substitute_miss(boundary: &str, method: &str) -> ! {
    panic!(
        "deja replay fail-stop: Substitute boundary `{boundary}::{method}` missed \
         the recording (args diverged or novel call). No recorded value for these \
         args and re-running is unsafe; halting this request. Declare \
         `replay_strategy = Execute` to recompute instead of stopping."
    );
}

/// Replay fail-stop on a Substitute lookup hit whose recorded result cannot be reconstructed.
///
/// A `Substitute` boundary has already matched the recording, so running the live
/// boundary because the typed return could not be rebuilt would silently convert a
/// replay/capture incompatibility into production I/O. Stop before the real
/// boundary starts; callers that need to recompute must declare
/// [`ReplayStrategy::Execute`].
#[cold]
#[inline(never)]
pub fn fail_stop_substitute_unreconstructable(boundary: &str, method: &str) -> ! {
    panic!(
        "deja replay fail-stop: Substitute boundary `{boundary}::{method}` hit \
         the recording, but the recorded result could not be reconstructed into \
         the boundary return type. Re-running is unsafe; halting this request. \
         Declare `replay_strategy = Execute` to recompute instead of substituting."
    );
}

/// Fail-stop when an Execute replay cannot acquire the shadow-observation token.
///
/// Execute mode must observe the live boundary against reconstructed state. If
/// no token is available, running the real boundary would silently escape replay
/// accounting, so the boundary halts before the real operation starts.
#[cold]
#[inline(never)]
pub fn fail_stop_execute_shadow_unavailable(boundary: &str, method: &str) -> ! {
    panic!(
        "deja replay fail-stop: Execute boundary `{boundary}::{method}` could \
         not acquire an execute-shadow token before running the real boundary; \
         halting this request."
    );
}

#[allow(deprecated)] // implemented in terms of the deprecated seams it subsumes
/// Run the execute-shadow OBSERVER under a firewall that is loud, never silent.
///
/// The firewall guards ONLY the observer internals (token bookkeeping, sink
/// I/O): if they panic, the panic is re-raised after a diagnostic naming the
/// boundary — an execute-shadow call that produced live output but recorded no
/// observation would silently under-report divergence, which fail-stop replay
/// must never do. The caller serializes the live output BEFORE calling this,
/// unguarded: a panicking serializer is a code bug and propagates unchanged.
fn shadow_observe_loud<F: FnOnce()>(boundary: &str, method: &str, observe: F) {
    if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(observe)) {
        eprintln!(
            "deja: PANIC in execute-shadow observer at `{boundary}::{method}` — \
             observation lost; re-raising instead of silently continuing (a live \
             result without an observation under-reports divergence)"
        );
        std::panic::resume_unwind(payload);
    }
}

pub fn dispatch<T, A, F, C, R, O>(
    obs: CrossingObservation,
    args: A,
    run: F,
    reconstruct: C,
    extract: R,
) -> T
where
    A: FnOnce() -> serde_json::Value,
    F: FnOnce() -> T,
    C: FnOnce(serde_json::Value) -> Reconstructed<T>,
    R: Fn(&T) -> O,
    O: Into<RecordedOutput>,
{
    match runtime_mode() {
        RuntimeMode::Disabled => record_only_path(obs, args, run, extract),
        RuntimeMode::Record => record_only_path(obs, args, run, extract),
        RuntimeMode::Replay => {
            // Bind the structured args ONCE in replay mode. The same value feeds
            // the execute peek, the substitute lookup, and the deferred live
            // record path used only for explicit recorded-skip fall-throughs.
            let boundary_args: serde_json::Value = args();

            match crate::replay::replay_strategy_to_execute_mode(obs.spec.replay_strategy) {
                ExecuteMode::Execute => {
                    #[allow(deprecated)]
                    if let Some(token) = execute_shadow_peek_boundary(
                        obs.caller,
                        &obs.spec,
                        &boundary_args,
                        Some(&obs.identity),
                    ) {
                        let out = run();
                        // Serialization of the live output runs UNGUARDED: a
                        // panicking `extract` is a code bug and must propagate —
                        // returning live output without an observation would
                        // silently under-report divergence.
                        let result_json = extract(&out).into().result;
                        shadow_observe_loud(obs.spec.boundary, obs.spec.method_name, || {
                            #[allow(deprecated)]
                            execute_shadow_observe_boundary(token, result_json);
                        });
                        return out;
                    }
                    fail_stop_execute_shadow_unavailable(obs.spec.boundary, obs.spec.method_name);
                }
                ExecuteMode::Lookup => {
                    #[allow(deprecated)]
                    match replay_boundary(
                        obs.caller,
                        &obs.spec,
                        &boundary_args,
                        Some(&obs.identity),
                    ) {
                        Some(recorded) => match reconstruct(recorded) {
                            Reconstructed::Value(replayed) => replayed,
                            Reconstructed::Failed => fail_stop_substitute_unreconstructable(
                                obs.spec.boundary,
                                obs.spec.method_name,
                            ),
                        },
                        None => {
                            fail_stop_substitute_miss(obs.spec.boundary, obs.spec.method_name);
                        }
                    }
                }
            }
        }
    }
}

/// The inactive / pure-record branch of [`dispatch`].
///
/// Split out so the inactive fast path stays trivially the same shape as the
/// pre-`dispatch` lazy record seam: `args` is handed to
/// `start_boundary_event_lazy`, which evaluates it ONLY when the recording hook
/// is active. When nothing is recording, the hook short-circuits before `args`
/// runs, so the inactive path serializes no arguments.
fn record_only_path<T, A, F, R, O>(obs: CrossingObservation, args: A, run: F, extract: R) -> T
where
    A: FnOnce() -> serde_json::Value,
    F: FnOnce() -> T,
    R: Fn(&T) -> O,
    O: Into<RecordedOutput>,
{
    let event = start_boundary_event_lazy_with_state(
        obs.caller,
        obs.spec,
        obs.correlation_id,
        args,
        Some(obs.identity),
        obs.state_capture,
    );
    let out = run();
    finish_boundary_event(event, &out, &extract);
    out
}

async fn record_only_path_async<T, A, Fut, F, R, O>(
    obs: CrossingObservation,
    args: A,
    run: F,
    extract: R,
) -> T
where
    A: FnOnce() -> serde_json::Value,
    Fut: std::future::Future<Output = T>,
    F: FnOnce() -> Fut,
    R: Fn(&T) -> O,
    O: Into<RecordedOutput>,
{
    let event = start_boundary_event_lazy_with_state(
        obs.caller,
        obs.spec,
        obs.correlation_id,
        args,
        Some(obs.identity),
        obs.state_capture,
    );
    let out = run().await;
    finish_boundary_event(event, &out, &extract);
    out
}

/// Async twin of [`dispatch`] for `async fn` (and boxed-future) boundaries.
///
/// Identical control flow to [`dispatch`]; the only difference is that `run`
/// yields a future that is awaited to produce `T`, so the recording / shadow
/// observation happens AFTER the real future resolves. The boundary macro emits
/// this for `async fn` bodies and for `future = "boxed"` bodies (wrapping the
/// returned `T` in `Box::pin`). See [`dispatch`] for the full rationale.
pub async fn dispatch_async<T, A, Fut, F, C, R, O>(
    obs: CrossingObservation,
    args: A,
    run: F,
    reconstruct: C,
    extract: R,
) -> T
where
    A: FnOnce() -> serde_json::Value,
    Fut: Future<Output = T>,
    F: FnOnce() -> Fut,
    C: FnOnce(serde_json::Value) -> Reconstructed<T>,
    R: Fn(&T) -> O,
    O: Into<RecordedOutput>,
{
    // Egress default: a Substitute-miss has no honest value and re-running is
    // unsafe, so STOP (see `fail_stop_substitute_miss`). A boundary whose caller
    // has a deterministic degraded path uses `dispatch_async_or_miss` instead.
    let (boundary, method) = (obs.spec.boundary, obs.spec.method_name);
    dispatch_async_or_miss(obs, args, run, reconstruct, extract, move || {
        fail_stop_substitute_miss(boundary, method)
    })
    .await
}

/// [`dispatch_async`] with a caller-supplied `on_miss` closure for the
/// Substitute-miss branch. The blocking NovelCall divergence is STILL emitted by
/// `replay_boundary` before this point — the miss is always surfaced on the
/// scorecard; `on_miss` only decides the continuation. Use this for a read whose
/// caller has a deterministic degraded path: a Superposition config read returns
/// `Err(SuperpositionError)` so the app's DB→default fallback runs and replay
/// progresses, instead of the egress fail-stop. `on_miss` fires ONLY on a genuine
/// lookup miss — a HIT whose recorded value cannot be reconstructed still
/// fail-stops (a codec incompatibility is a bug, not graceful degradation).
#[allow(deprecated)] // implemented in terms of the deprecated seams it subsumes
pub async fn dispatch_async_or_miss<T, A, Fut, F, C, R, O, M>(
    obs: CrossingObservation,
    args: A,
    run: F,
    reconstruct: C,
    extract: R,
    on_miss: M,
) -> T
where
    A: FnOnce() -> serde_json::Value,
    Fut: Future<Output = T>,
    F: FnOnce() -> Fut,
    C: FnOnce(serde_json::Value) -> Reconstructed<T>,
    R: Fn(&T) -> O,
    O: Into<RecordedOutput>,
    M: FnOnce() -> T,
{
    match runtime_mode() {
        RuntimeMode::Disabled => record_only_path_async(obs, args, run, extract).await,
        RuntimeMode::Record => record_only_path_async(obs, args, run, extract).await,
        RuntimeMode::Replay => {
            let boundary_args: serde_json::Value = args();

            match crate::replay::replay_strategy_to_execute_mode(obs.spec.replay_strategy) {
                ExecuteMode::Execute => {
                    if let Some(token) = execute_shadow_peek_boundary(
                        obs.caller,
                        &obs.spec,
                        &boundary_args,
                        Some(&obs.identity),
                    ) {
                        let out = run().await;
                        // Serialization of the live output runs UNGUARDED: a
                        // panicking `extract` is a code bug and must propagate —
                        // returning live output without an observation would
                        // silently under-report divergence.
                        let result_json = extract(&out).into().result;
                        shadow_observe_loud(obs.spec.boundary, obs.spec.method_name, || {
                            #[allow(deprecated)]
                            execute_shadow_observe_boundary(token, result_json);
                        });
                        return out;
                    }
                    fail_stop_execute_shadow_unavailable(obs.spec.boundary, obs.spec.method_name);
                }
                ExecuteMode::Lookup => {
                    #[allow(deprecated)]
                    match replay_boundary(
                        obs.caller,
                        &obs.spec,
                        &boundary_args,
                        Some(&obs.identity),
                    ) {
                        Some(recorded) => match reconstruct(recorded) {
                            Reconstructed::Value(replayed) => replayed,
                            Reconstructed::Failed => fail_stop_substitute_unreconstructable(
                                obs.spec.boundary,
                                obs.spec.method_name,
                            ),
                        },
                        None => on_miss(),
                    }
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Hook-parameterized seam (`dispatch_with_hook` / `_async`) for the delegate path
// ---------------------------------------------------------------------------

/// Matching inputs for the per-instance delegate seam.
///
/// The delegate path (the `delegate_<trait>!` macros) records against a hook
/// INJECTED into the wrapper (`self.$hook`), not the process-global hook the
/// boundary macro's [`dispatch`] uses. So the delegate's seam carries an
/// explicit `&dyn DejaHook` plus the full [`BoundarySpec`] that describes both
/// the matching tuple and any declared replay semantics. Like
/// [`CrossingObservation`], this carries no replay verdict — replay still reads
/// the spec under the active hook's runtime mode.
pub struct DelegateObservation<'a> {
    /// The injected hook to record / replay through.
    pub hook: &'a dyn DejaHook,
    /// Boundary identity plus declared replay semantics.
    pub spec: BoundarySpec,
    /// `#[track_caller]` invocation address.
    pub caller: &'static Location<'static>,
    /// Call-site identity; `occurrence` allocated ONCE by the caller.
    pub identity: CallsiteIdentity,
    /// Decorator `self`/inner type context recorded on the event.
    pub receiver: Option<serde_json::Value>,
}

/// Sync per-instance delegate seam. Collapses the delegate macro's three
/// duplicated arms (execute / replay / record) into ONE call that owns all of
/// the run/skip/shadow/record control flow, so the delegate macro names no
/// replay-only operation.
///
/// Identical semantics to the pre-`dispatch` delegate expansion, but routed
/// through the injected `obs.hook` rather than the global hook. See [`dispatch`]
/// for the control-flow contract; the only difference is the hook source and
/// that the record path goes through [`EventBuilder`] directly (to attach the
/// `receiver`) inside a panic firewall.
///
/// `args` is the ALREADY-SERIALIZED args image. Unlike [`dispatch`], the delegate
/// keeps its `if !is_active` fast path in the macro (so args are still NOT
/// serialized when the hook is inactive — the delegate's async methods desugar to
/// a returned `Box::pin` future into which both args and the run block would have
/// to be moved, which forbids a borrowing args thunk; the macro therefore
/// computes args eagerly only on the active path, exactly as before). Once called,
/// this seam branches solely on the injected hook's [`RuntimeMode`].
pub fn dispatch_with_hook<T, F, C, R, O>(
    obs: DelegateObservation<'_>,
    args: serde_json::Value,
    run: F,
    reconstruct: C,
    extract: R,
) -> T
where
    F: FnOnce() -> T,
    C: FnOnce(serde_json::Value) -> Reconstructed<T>,
    R: Fn(&T) -> O,
    O: Into<RecordedOutput>,
{
    match obs.hook.mode() {
        RuntimeMode::Disabled => run(),
        RuntimeMode::Record => delegate_record_path(obs, args, run, extract),
        RuntimeMode::Replay => {
            let boundary_args = args;
            match crate::replay::replay_strategy_to_execute_mode(obs.spec.replay_strategy) {
                ExecuteMode::Execute => {
                    if let Some(token) = obs.hook.execute_shadow_peek(ReplayLookup {
                        boundary: obs.spec.boundary,
                        trait_name: obs.spec.trait_name,
                        method_name: obs.spec.method_name,
                        args: &boundary_args,
                        callsite_identity: Some(&obs.identity),
                        caller_location: Some(obs.caller),
                    }) {
                        let out = run();
                        // Serialization runs UNGUARDED (a panicking `extract` is a
                        // code bug and must propagate); only the delegate observer
                        // is firewalled, loudly — same invariant as `dispatch`:
                        // no live return without an observation.
                        let result_json = extract(&out).into().result;
                        shadow_observe_loud(obs.spec.boundary, obs.spec.method_name, || {
                            obs.hook.execute_shadow_observe(token, result_json);
                        });
                        return out;
                    }
                    fail_stop_execute_shadow_unavailable(obs.spec.boundary, obs.spec.method_name);
                }
                ExecuteMode::Lookup => {
                    match obs.hook.try_replay_with_context(ReplayLookup {
                        boundary: obs.spec.boundary,
                        trait_name: obs.spec.trait_name,
                        method_name: obs.spec.method_name,
                        args: &boundary_args,
                        callsite_identity: Some(&obs.identity),
                        caller_location: Some(obs.caller),
                    }) {
                        Some(recorded) => match reconstruct(recorded) {
                            Reconstructed::Value(replayed) => replayed,
                            Reconstructed::Failed => fail_stop_substitute_unreconstructable(
                                obs.spec.boundary,
                                obs.spec.method_name,
                            ),
                        },
                        None => {
                            fail_stop_substitute_miss(obs.spec.boundary, obs.spec.method_name);
                        }
                    }
                }
            }
        }
    }
}

/// Async twin of [`dispatch_with_hook`] for `async` delegate methods (which the
/// macro returns as `Pin<Box<dyn Future>>`). The `run` thunk yields the inner
/// future; the macro wraps the whole call in `Box::pin`. `args` is the
/// already-serialized image (see [`dispatch_with_hook`] for why the delegate
/// computes it eagerly on the active path).
pub async fn dispatch_async_with_hook<T, Fut, F, C, R, O>(
    obs: DelegateObservation<'_>,
    args: serde_json::Value,
    run: F,
    reconstruct: C,
    extract: R,
) -> T
where
    Fut: Future<Output = T>,
    F: FnOnce() -> Fut,
    C: FnOnce(serde_json::Value) -> Reconstructed<T>,
    R: Fn(&T) -> O,
    O: Into<RecordedOutput>,
{
    match obs.hook.mode() {
        RuntimeMode::Disabled => run().await,
        RuntimeMode::Record => delegate_record_path_async(obs, args, run, extract).await,
        RuntimeMode::Replay => {
            let boundary_args = args;
            match crate::replay::replay_strategy_to_execute_mode(obs.spec.replay_strategy) {
                ExecuteMode::Execute => {
                    if let Some(token) = obs.hook.execute_shadow_peek(ReplayLookup {
                        boundary: obs.spec.boundary,
                        trait_name: obs.spec.trait_name,
                        method_name: obs.spec.method_name,
                        args: &boundary_args,
                        callsite_identity: Some(&obs.identity),
                        caller_location: Some(obs.caller),
                    }) {
                        let out = run().await;
                        // Serialization runs UNGUARDED (a panicking `extract` is a
                        // code bug and must propagate); only the delegate observer
                        // is firewalled, loudly — same invariant as `dispatch`:
                        // no live return without an observation.
                        let result_json = extract(&out).into().result;
                        shadow_observe_loud(obs.spec.boundary, obs.spec.method_name, || {
                            obs.hook.execute_shadow_observe(token, result_json);
                        });
                        return out;
                    }
                    fail_stop_execute_shadow_unavailable(obs.spec.boundary, obs.spec.method_name);
                }
                ExecuteMode::Lookup => {
                    match obs.hook.try_replay_with_context(ReplayLookup {
                        boundary: obs.spec.boundary,
                        trait_name: obs.spec.trait_name,
                        method_name: obs.spec.method_name,
                        args: &boundary_args,
                        callsite_identity: Some(&obs.identity),
                        caller_location: Some(obs.caller),
                    }) {
                        Some(recorded) => match reconstruct(recorded) {
                            Reconstructed::Value(replayed) => replayed,
                            Reconstructed::Failed => fail_stop_substitute_unreconstructable(
                                obs.spec.boundary,
                                obs.spec.method_name,
                            ),
                        },
                        None => {
                            fail_stop_substitute_miss(obs.spec.boundary, obs.spec.method_name);
                        }
                    }
                }
            }
        }
    }
}

fn delegate_record_path<T, F, R, O>(
    obs: DelegateObservation<'_>,
    boundary_args: serde_json::Value,
    run: F,
    extract: R,
) -> T
where
    F: FnOnce() -> T,
    R: Fn(&T) -> O,
    O: Into<RecordedOutput>,
{
    let builder = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        EventBuilder::start_with_receiver(
            obs.hook,
            obs.spec.boundary,
            obs.spec.trait_name,
            obs.spec.method_name,
            obs.caller,
            obs.receiver,
            boundary_args,
        )
        .with_semantics(obs.spec.semantics())
        .with_callsite_identity(obs.identity)
    }))
    .ok();
    let out = run();
    if let Some(builder) = builder {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let output = extract(&out).into();
            builder.finish_recorded(obs.hook, output);
        }));
    }
    out
}

async fn delegate_record_path_async<T, Fut, F, R, O>(
    obs: DelegateObservation<'_>,
    boundary_args: serde_json::Value,
    run: F,
    extract: R,
) -> T
where
    Fut: std::future::Future<Output = T>,
    F: FnOnce() -> Fut,
    R: Fn(&T) -> O,
    O: Into<RecordedOutput>,
{
    let builder = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        EventBuilder::start_with_receiver(
            obs.hook,
            obs.spec.boundary,
            obs.spec.trait_name,
            obs.spec.method_name,
            obs.caller,
            obs.receiver,
            boundary_args,
        )
        .with_semantics(obs.spec.semantics())
        .with_callsite_identity(obs.identity)
    }))
    .ok();
    let out = run().await;
    if let Some(builder) = builder {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let output = extract(&out).into();
            builder.finish_recorded(obs.hook, output);
        }));
    }
    out
}

fn start_boundary_event(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: serde_json::Value,
    identity: Option<CallsiteIdentity>,
) -> Option<(Arc<RecordingHook>, EventBuilder)> {
    start_boundary_event_lazy_with_state(caller, spec, correlation_id, || args, identity, None)
}

pub fn start_boundary_event_lazy<A>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: A,
    identity: Option<CallsiteIdentity>,
) -> Option<(Arc<RecordingHook>, EventBuilder)>
where
    A: FnOnce() -> serde_json::Value,
{
    start_boundary_event_lazy_with_state(caller, spec, correlation_id, args, identity, None)
}

fn start_boundary_event_lazy_with_state<A>(
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: A,
    identity: Option<CallsiteIdentity>,
    state_capture: Option<StateCapture>,
) -> Option<(Arc<RecordingHook>, EventBuilder)>
where
    A: FnOnce() -> serde_json::Value,
{
    let hook = global_hook_from_env()?;
    start_boundary_event_lazy_with_hook(
        hook,
        caller,
        spec,
        correlation_id,
        args,
        identity,
        state_capture,
    )
}

fn start_boundary_event_lazy_with_hook<A>(
    hook: Arc<RecordingHook>,
    caller: &'static Location<'static>,
    spec: BoundarySpec,
    correlation_id: Option<String>,
    args: A,
    identity: Option<CallsiteIdentity>,
    state_capture: Option<StateCapture>,
) -> Option<(Arc<RecordingHook>, EventBuilder)>
where
    A: FnOnce() -> serde_json::Value,
{
    if !hook.mode().is_record() {
        return None;
    }
    // SHADOW GUARANTEE: `args()` runs user `Serialize`/`Debug` impls and the
    // builder setup may touch poisoned locks — either could panic. Catch it so a
    // recording panic can NEVER unwind into the real request; on panic we simply
    // skip recording this boundary and the caller proceeds with the real call.
    let semantics = spec.semantics();
    let event = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut event = EventBuilder::start_with_correlation_id(
            &*hook,
            spec.boundary,
            spec.trait_name,
            spec.method_name,
            caller,
            correlation_id,
            args(),
        )
        .with_semantics(semantics);
        if let Some(identity) = identity {
            event = event.with_callsite_identity(identity);
        }
        if let Some(state_capture) = state_capture {
            event = state_capture.apply_to(event);
        }
        event
    }))
    .ok()?;
    Some((hook, event))
}

pub fn finish_boundary_event<T, R, O>(
    event: Option<(Arc<RecordingHook>, EventBuilder)>,
    output: &T,
    result: R,
) where
    R: FnOnce(&T) -> O,
    O: Into<RecordedOutput>,
{
    // SHADOW GUARANTEE: result serialization + the sink enqueue run AFTER the real
    // call already produced `output`. Catch any panic so a recording failure can
    // never turn a successful request into a failed one — it just drops the event.
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if let Some((hook, event)) = event {
            let output = result(output).into();
            event.finish_recorded(&*hook, output);
        }
    }));
}

/// Path to the semantic events file within an artifact directory.
pub fn semantic_events_path(artifact_dir: &Path) -> PathBuf {
    artifact_dir.join("semantic-events.jsonl")
}

// ---------------------------------------------------------------------------
// Artifact reader (for analysis and future replay)
// ---------------------------------------------------------------------------

/// Read all semantic events from a JSONL file.
pub fn read_events(artifact_dir: &Path) -> std::io::Result<Vec<BoundaryEvent>> {
    let path = semantic_events_path(artifact_dir);
    let content = std::fs::read_to_string(path)?;
    let mut events = Vec::new();
    for line in content.lines() {
        if line.trim().is_empty() {
            continue;
        }
        // The stream is tagged DejaRecords; other kinds (graph nodes) and
        // non-record lines (sink markers) are someone else's to read.
        if let Ok(DejaRecord::BoundaryEvent(event)) = serde_json::from_str::<DejaRecord>(line) {
            events.push(*event);
        }
    }
    Ok(events)
}

// ---------------------------------------------------------------------------
// Replay/deviation lookup
// ---------------------------------------------------------------------------

/// Replay match strictness for a recorded semantic event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReplayConfidence {
    /// Same boundary, trait, method, call file, call line, and args.
    Exact,
    /// Same boundary, trait, method, call file, and args, but a different line.
    LineShifted,
    /// Same boundary, trait, method, call file, and line, but different args.
    ArgsChanged,
}

/// Query used to find a recorded semantic operation during replay analysis.
#[derive(Debug, Clone, Copy)]
pub struct ReplayQuery<'a> {
    pub correlation_id: Option<&'a str>,
    pub boundary: &'a str,
    pub trait_name: &'a str,
    pub method_name: &'a str,
    pub call_file: &'a str,
    pub call_line: u32,
    pub args: &'a serde_json::Value,
}

/// A replay/deviation match against a recorded event.
#[derive(Debug, Clone, Copy)]
pub struct ReplayMatch<'a> {
    pub event: &'a BoundaryEvent,
    pub confidence: ReplayConfidence,
    pub reason: &'static str,
}

/// In-memory index over recorded semantic events.
///
/// This is a diagnostic and matching primitive. It deliberately does not
/// implement `DejaHook` because returning typed application values from JSON is
/// boundary-specific work, not something the generic recorder can do safely.
#[derive(Debug, Clone)]
pub struct ReplayIndex {
    events: Vec<BoundaryEvent>,
}

impl ReplayIndex {
    pub fn new(events: Vec<BoundaryEvent>) -> Self {
        Self { events }
    }

    pub fn from_artifact_dir(artifact_dir: &Path) -> std::io::Result<Self> {
        read_events(artifact_dir).map(Self::new)
    }

    pub fn events(&self) -> &[BoundaryEvent] {
        &self.events
    }

    /// Find the best match for a replay query using a strict-to-loose cascade.
    pub fn find(&self, query: ReplayQuery<'_>) -> Option<ReplayMatch<'_>> {
        self.find_by(query, |event| {
            event.call_file == query.call_file
                && event.call_line == query.call_line
                && event.args == *query.args
        })
        .map(|event| ReplayMatch {
            event,
            confidence: ReplayConfidence::Exact,
            reason: "exact call-site and args match",
        })
        .or_else(|| {
            self.find_by(query, |event| {
                event.call_file == query.call_file && event.args == *query.args
            })
            .map(|event| ReplayMatch {
                event,
                confidence: ReplayConfidence::LineShifted,
                reason: "same call file and args, line shifted",
            })
        })
        .or_else(|| {
            self.find_by(query, |event| {
                event.call_file == query.call_file && event.call_line == query.call_line
            })
            .map(|event| ReplayMatch {
                event,
                confidence: ReplayConfidence::ArgsChanged,
                reason: "same call-site, args changed",
            })
        })
    }

    fn find_by(
        &self,
        query: ReplayQuery<'_>,
        predicate: impl Fn(&BoundaryEvent) -> bool,
    ) -> Option<&BoundaryEvent> {
        self.events.iter().find(|event| {
            correlation_matches(event, query.correlation_id)
                && event.boundary == query.boundary
                && event.trait_name == query.trait_name
                && event.method_name == query.method_name
                && predicate(event)
        })
    }

    /// Deterministic call-graph fingerprint for one correlation scope.
    ///
    /// The hash includes operation order, boundary, trait/method name, and call
    /// location. It is intended for change detection, not cryptographic use.
    pub fn call_graph_fingerprint(&self, correlation_id: Option<&str>) -> u64 {
        let mut hash = FNV_OFFSET_BASIS;
        for event in self
            .events
            .iter()
            .filter(|event| correlation_matches(event, correlation_id))
        {
            hash = fnv1a_u64(hash, event.request_sequence);
            hash = fnv1a_str(hash, &event.boundary);
            hash = fnv1a_str(hash, &event.trait_name);
            hash = fnv1a_str(hash, &event.method_name);
            hash = fnv1a_str(hash, &event.call_file);
            hash = fnv1a_u64(hash, event.call_line as u64);
            hash = fnv1a_u64(hash, event.call_column as u64);
        }
        hash
    }
}

fn correlation_matches(event: &BoundaryEvent, correlation_id: Option<&str>) -> bool {
    match correlation_id {
        Some(id) => event.correlation_id.as_deref() == Some(id),
        None => event.correlation_id.is_none(),
    }
}

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn fnv1a_str(hash: u64, value: &str) -> u64 {
    let hash = fnv1a_bytes(hash, value.as_bytes());
    fnv1a_bytes(hash, &[0xff])
}

fn fnv1a_u64(hash: u64, value: u64) -> u64 {
    fnv1a_bytes(hash, &value.to_le_bytes())
}

/// Deterministic FNV-1a hash of a string, used to derive a stable
/// `CallsiteIdentity::syntax_hash` (rank-3 `Address::SyntacticHash`).
///
/// The boundary proc-macro replicates this exact algorithm at expansion time
/// to emit a `syntax_hash` literal from the boundary/component/operation
/// tuple; the hand-written DB boundary calls this at runtime. Using one shared,
/// version-stable algorithm guarantees the same input string always yields the
/// same hash regardless of rustc/syn version, so record and replay agree.
pub fn stable_callsite_hash(input: &str) -> u64 {
    fnv1a_str(FNV_OFFSET_BASIS, input)
}

// ---------------------------------------------------------------------------
// Metrics summary
// ---------------------------------------------------------------------------

/// Summary metrics from a recording session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingMetrics {
    pub total_events: u64,
    pub correlated_events: u64,
    pub uncorrelated_events: u64,
    pub unique_correlation_ids: u64,
    pub unique_traits: u64,
    pub unique_methods: u64,
    pub unique_call_sites: u64,
    pub error_events: u64,
    pub boundaries: HashMap<String, u64>,
}

/// Compute metrics from a set of recorded events.
pub fn compute_metrics(events: &[BoundaryEvent]) -> RecordingMetrics {
    use std::collections::HashSet;

    let mut correlation_ids = HashSet::new();
    let mut traits = HashSet::new();
    let mut methods = HashSet::new();
    let mut call_sites = HashSet::new();
    let mut boundaries: HashMap<String, u64> = HashMap::new();
    let mut correlated = 0u64;
    let mut uncorrelated = 0u64;
    let mut errors = 0u64;

    for event in events {
        if let Some(ref id) = event.correlation_id {
            correlation_ids.insert(id.clone());
            correlated += 1;
        } else {
            uncorrelated += 1;
        }
        traits.insert(event.trait_name.clone());
        methods.insert(format!("{}::{}", event.trait_name, event.method_name));
        call_sites.insert(format!(
            "{}:{}:{}",
            event.call_file, event.call_line, event.call_column
        ));
        *boundaries.entry(event.boundary.clone()).or_insert(0) += 1;
        if event.is_error {
            errors += 1;
        }
    }

    RecordingMetrics {
        total_events: events.len() as u64,
        correlated_events: correlated,
        uncorrelated_events: uncorrelated,
        unique_correlation_ids: correlation_ids.len() as u64,
        unique_traits: traits.len() as u64,
        unique_methods: methods.len() as u64,
        unique_call_sites: call_sites.len() as u64,
        error_events: errors,
        boundaries,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use std::panic::Location;

    // -----------------------------------------------------------------------
    // DejaRecord — the one-stream wire shape (tag routes, fields stay flat).
    // -----------------------------------------------------------------------

    #[test]
    fn deja_record_wire_shape_is_flat_and_tagged() {
        let node = deja_core::ExecutionGraphNode {
            node_id: 7,
            global_sequence: 42,
            parent_id: Some(3),
            causal_parent_ids: vec![1],
            sequence: 5,
            recording_run_id: Some("run-x".to_owned()),
            span_name: "payment.request".to_owned(),
            target: "router".to_owned(),
            level: "INFO".to_owned(),
            fields: std::collections::BTreeMap::new(),
            started_ns: 10,
            closed_ns: Some(20),
        };
        let json = serde_json::to_value(DejaRecord::GraphNode(node.clone())).unwrap();
        // One flat object: the tag sits beside the node's own fields.
        assert_eq!(json["record_kind"], "graph_node");
        assert_eq!(json["node_id"], 7);
        assert_eq!(json["global_sequence"], 42);

        let back: DejaRecord = serde_json::from_value(json).unwrap();
        assert_eq!(back.global_sequence(), 42);
        match back {
            DejaRecord::GraphNode(n) => assert_eq!(n, node),
            other => panic!("expected GraphNode, got {other:?}"),
        }
    }

    #[test]
    fn deja_record_boundary_event_round_trips_with_tag() {
        let event: BoundaryEvent = serde_json::from_value(serde_json::json!({
            "global_sequence": 9,
            "request_sequence": 1,
            "correlation_id": "c1",
            "timestamp_ns": 1,
            "boundary": "db",
            "trait_name": "T",
            "method_name": "m",
            "request": {}, "args": {},
            "response": {"ok": true}, "result": {"ok": true},
            "is_error": false,
            "duration_us": 5,
            "event_schema_version": CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute",
            "call_file": "lib.rs",
            "call_line": 1,
            "call_column": 1,
        }))
        .expect("minimal boundary event");
        let json = serde_json::to_value(DejaRecord::BoundaryEvent(Box::new(event))).unwrap();
        assert_eq!(json["record_kind"], "boundary_event");
        assert_eq!(json["boundary"], "db");
        let back: DejaRecord = serde_json::from_value(json).unwrap();
        assert_eq!(back.global_sequence(), 9);
    }

    // -----------------------------------------------------------------------
    // Declarative boundary model (#28) — the `replay_strategy` knob serde + spec.
    // -----------------------------------------------------------------------

    #[test]
    fn replay_strategy_round_trips_through_serde() {
        for s in [ReplayStrategy::Execute, ReplayStrategy::Substitute] {
            let json = serde_json::to_string(&s).expect("ser");
            let back: ReplayStrategy = serde_json::from_str(&json).expect("de");
            assert_eq!(s, back, "ReplayStrategy round-trip for {s:?}");
        }
        // Wire spelling is snake_case.
        assert_eq!(
            serde_json::to_string(&ReplayStrategy::Execute).unwrap(),
            "\"execute\""
        );
        assert_eq!(
            serde_json::to_string(&ReplayStrategy::Substitute).unwrap(),
            "\"substitute\""
        );
    }

    /// Current event JSON must carry the replay knob; it round-trips with the
    /// dashboard-only label.
    #[test]
    fn semantic_event_carries_replay_strategy_on_current_wire() {
        let current = serde_json::json!({
            "global_sequence": 0, "request_sequence": 0, "correlation_id": null,
            "timestamp_ns": 0, "boundary": "redis", "trait_name": "T", "method_name": "get",
            "call_file": "x.rs", "call_line": 1, "call_column": 1,
            "request": [], "args": [], "response": null, "result": null,
            "is_error": false, "duration_us": 0,
            "event_schema_version": CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "execute",
            "kind": "redis"
        });
        let ev: BoundaryEvent = serde_json::from_value(current).expect("de current event");
        assert_eq!(ev.replay_strategy, ReplayStrategy::Execute);
        assert_eq!(ev.kind.as_deref(), Some("redis"));

        let json = serde_json::to_string(&ev).expect("ser");
        let back: BoundaryEvent = serde_json::from_str(&json).expect("de");
        assert_eq!(back.replay_strategy, ReplayStrategy::Execute);
        assert_eq!(back.kind.as_deref(), Some("redis"));
    }

    /// `BoundarySpec::new` is the default (`Substitute`, no label);
    /// `with_semantics` carries the knob and `semantics()` reflects it.
    #[test]
    fn boundary_spec_new_defaults_and_with_semantics_carries() {
        let plain = BoundarySpec::new("redis", "T", "get");
        assert_eq!(plain.semantics().kind, None);
        assert_eq!(plain.replay_strategy, ReplayStrategy::Substitute);

        let declared = BoundarySpec::with_semantics(
            "redis",
            "T",
            "get_key",
            BoundarySemantics {
                replay_strategy: ReplayStrategy::Execute,
                kind: Some("redis".to_string()),
                declaration: Some(BoundaryDeclaration::default().effect(EffectKind::Redis)),
            },
        );
        let s = declared.semantics();
        assert_eq!(s.kind.as_deref(), Some("redis"));
        assert_eq!(s.replay_strategy, ReplayStrategy::Execute);
        assert_eq!(
            s.declaration.as_ref().and_then(|d| d.effect),
            Some(EffectKind::Redis)
        );
    }

    #[test]
    fn now_ns_returns_reasonable_value() {
        let ts = now_ns();
        // Should be after 2024-01-01 (in nanoseconds)
        assert!(ts > 1_704_067_200_000_000_000);
    }

    #[test]
    fn recording_hook_sequences_atomically() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = RecordingHook::new(dir.path()).expect("hook");

        assert_eq!(hook.next_global_sequence(), 0);
        assert_eq!(hook.next_global_sequence(), 1);
        assert_eq!(hook.next_global_sequence(), 2);

        assert_eq!(hook.next_request_sequence(Some("req-1")), 0);
        assert_eq!(hook.next_request_sequence(Some("req-1")), 1);
        assert_eq!(hook.next_request_sequence(Some("req-2")), 0);
        assert_eq!(hook.next_request_sequence(Some("req-1")), 2);
    }

    #[test]
    fn recording_hook_mode_obeys_scoped_false_decision() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = RecordingHook::new(dir.path()).expect("hook");
        let correlation_id = "req-recording-hook-mode-off";

        deja_context::set_recording_decision(correlation_id, false);
        {
            let _guard = deja_context::enter_correlation_id(correlation_id);
            assert_eq!(hook.mode(), RuntimeMode::Disabled);
        }
        deja_context::clear_recording_decision(correlation_id);
    }

    #[test]
    fn manual_record_helpers_skip_allocation_when_sampled_out() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = std::sync::Arc::new(RecordingHook::new(dir.path()).expect("hook"));
        let correlation_id = "req-boundary-helper-sampled-out";

        deja_context::set_recording_decision(correlation_id, false);
        {
            let _guard = deja_context::enter_correlation_id(correlation_id);
            let args_evaluated = std::cell::Cell::new(false);
            let event = start_boundary_event_lazy_with_hook(
                std::sync::Arc::clone(&hook),
                Location::caller(),
                BoundarySpec::new("unit", "T", "m"),
                Some(correlation_id.to_string()),
                || {
                    args_evaluated.set(true);
                    serde_json::json!({"unexpected": true})
                },
                None,
                None,
            );

            assert!(event.is_none());
            assert!(!args_evaluated.get());
            assert_eq!(hook.mode(), RuntimeMode::Disabled);
            record_semantic_event_with_hook(
                std::sync::Arc::clone(&hook),
                "manual",
                "T",
                "record",
                serde_json::json!({"request": true}),
                serde_json::json!({"response": true}),
                false,
                Location::caller(),
            );
        }
        deja_context::clear_recording_decision(correlation_id);

        assert_eq!(hook.next_global_sequence(), 0);
        assert_eq!(hook.next_request_sequence(Some(correlation_id)), 0);
    }

    #[test]
    fn event_builder_roundtrip() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = RecordingHook::new(dir.path()).expect("hook");

        // Simulate what generated delegation code does
        #[track_caller]
        fn simulate_call(hook: &RecordingHook) {
            let caller = Location::caller();
            let builder = EventBuilder::start(
                hook,
                "storage",
                "AddressInterface",
                "find_address_by_address_id",
                caller,
                serde_json::json!({"address_id": "addr_123"}),
            );
            builder.finish(
                hook,
                serde_json::json!({"id": "addr_123", "city": "Mumbai"}),
                false,
            );
        }

        simulate_call(&hook);
        simulate_call(&hook);

        // Flush and read back
        drop(hook);
        let events = read_events(dir.path()).expect("read");
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].global_sequence, 0);
        assert_eq!(events[1].global_sequence, 1);
        assert_eq!(events[0].trait_name, "AddressInterface");
        assert_eq!(events[0].method_name, "find_address_by_address_id");
        assert_eq!(events[0].boundary, "storage");
        assert!(!events[0].is_error);
        assert!(events[0].call_file.contains("lib.rs"));
    }

    #[test]
    fn finish_prefers_explicit_builder_captures_without_db_inference() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = RecordingHook::new(dir.path()).expect("hook");
        let caller = Location::caller();
        let explicit_args = serde_json::json!({
            "operation": "generic_find_one_core",
            "table": "users",
            "sql": "SELECT * FROM \"users\" WHERE \"user_id\" = $1",
            "inputs": ["inferred_user"]
        });
        let explicit_output = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": {
                "user_id": "inferred_user",
                "merchant_id": "merch_ignored"
            },
            "type_name": "User"
        });
        let explicit_read = crate::replay::StateKey::DbRow {
            table: "users".to_owned(),
            pk_column: "user_id".to_owned(),
            pk_value: "explicit_read_user".to_owned(),
        }
        .to_wire();
        let explicit_write = crate::replay::StateKey::DbRow {
            table: "users".to_owned(),
            pk_column: "user_id".to_owned(),
            pk_value: "explicit_write_user".to_owned(),
        }
        .to_wire();
        let result_image = serde_json::json!({"image": "post"});
        let pre_image = serde_json::json!({"image": "pre"});

        EventBuilder::start(
            &hook,
            "db",
            "Execute",
            "generic_find_one_core",
            caller,
            serde_json::json!({"unused": true}),
        )
        .record_call_to(explicit_args.clone())
        .with_read_set(vec![explicit_read.clone()])
        .with_write_set(vec![explicit_write.clone()])
        .with_output(explicit_output.clone())
        .with_result_image(result_image.clone())
        .with_pre_image(pre_image.clone())
        .finish(&hook, serde_json::json!({ "value": "ignored" }), false);

        drop(hook);
        let events = read_events(dir.path()).expect("read");
        assert_eq!(events.len(), 1);
        let event = &events[0];
        assert_eq!(event.request, explicit_args);
        assert_eq!(event.args, explicit_args);
        assert_eq!(event.response, explicit_output);
        assert_eq!(event.result, explicit_output);
        assert_eq!(
            event.read_set,
            vec![explicit_read],
            "DB-shaped args/result must not add inferred row keys"
        );
        assert_eq!(
            event.write_set,
            vec![explicit_write],
            "DB-shaped args/result must not add inferred row keys"
        );
        assert_eq!(event.result_image, Some(result_image));
        assert_eq!(event.pre_image, Some(pre_image));
        assert_eq!(
            event.value_digest,
            Some(value_digest_of(&event.args, &event.result))
        );
    }

    #[test]
    fn state_single_key_helpers_set_only_the_requested_side() {
        fn record_with(
            boundary: &'static str,
            method: &'static str,
            builder: impl FnOnce(EventBuilder) -> EventBuilder,
        ) -> BoundaryEvent {
            let dir = tempfile::tempdir().expect("tempdir");
            let hook = RecordingHook::new(dir.path()).expect("hook");
            let event = EventBuilder::start(
                &hook,
                boundary,
                "State",
                method,
                Location::caller(),
                serde_json::json!(["unused-call-key"]),
            );
            builder(event).finish(&hook, serde_json::json!("result"), false);
            drop(hook);
            read_events(dir.path())
                .expect("read")
                .into_iter()
                .next()
                .expect("event")
        }

        let read = record_with("db", "select_one", |event| {
            event.state_read_to("explicit-read")
        });
        assert_eq!(read.read_set, vec!["explicit-read"]);
        assert!(read.write_set.is_empty());

        let write = record_with("redis", "get_key", |event| {
            event.state_write_to("explicit-write")
        });
        assert!(write.read_set.is_empty());
        assert_eq!(write.write_set, vec!["explicit-write"]);
    }

    #[test]
    fn crossing_observation_state_capture_is_authoritative() {
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = RecordingHook::new(dir.path()).expect("hook");
        let obs = CrossingObservation::new(
            BoundarySpec::new("redis", "Redis", "get_key"),
            test_identity(),
            Location::caller(),
        )
        .state_write_to("explicit-write");

        let builder = EventBuilder::start(
            &hook,
            obs.spec.boundary,
            obs.spec.trait_name,
            obs.spec.method_name,
            obs.caller,
            serde_json::json!(["unused-read-key"]),
        );
        let builder = obs
            .state_capture
            .expect("observation state capture")
            .apply_to(builder);
        builder.finish(&hook, serde_json::json!("result"), false);

        drop(hook);
        let event = read_events(dir.path())
            .expect("read")
            .into_iter()
            .next()
            .expect("event");
        assert!(event.read_set.is_empty());
        assert_eq!(event.write_set, vec!["explicit-write"]);
    }

    /// `EventBuilder::finish` with no explicit builder captures stamps empty
    /// read/write sets while preserving the declared replay knob, kind, and
    /// entropy provenance.
    #[test]
    fn finish_stamps_empty_sets_and_the_replay_knob_without_explicit_capture() {
        fn record_one(
            dir: &std::path::Path,
            boundary: &'static str,
            method: &'static str,
            args: serde_json::Value,
            semantics: Option<BoundarySemantics>,
        ) -> BoundaryEvent {
            let hook = RecordingHook::new(dir).expect("hook");
            let caller = Location::caller();
            let mut b = EventBuilder::start(&hook, boundary, "T", method, caller, args);
            if let Some(s) = semantics {
                b = b.with_semantics(s);
            }
            b.finish(&hook, serde_json::json!("0.10"), false);
            drop(hook);
            let events = read_events(dir).expect("read");
            assert_eq!(events.len(), 1);
            events.into_iter().next().unwrap()
        }

        let d1 = tempfile::tempdir().unwrap();
        let declared = record_one(
            d1.path(),
            "redis",
            "get_key",
            serde_json::json!(["settlement_rate_default"]),
            Some(BoundarySemantics {
                replay_strategy: ReplayStrategy::Execute,
                kind: Some("redis".to_string()),
                declaration: Some(BoundaryDeclaration::default().effect(EffectKind::Redis)),
            }),
        );
        assert_eq!(declared.replay_strategy, ReplayStrategy::Execute);
        assert_eq!(declared.kind.as_deref(), Some("redis"));
        assert_eq!(
            declared.declaration.as_ref().and_then(|d| d.effect),
            Some(EffectKind::Redis)
        );
        assert!(declared.read_set.is_empty());
        assert!(declared.write_set.is_empty());

        let d2 = tempfile::tempdir().unwrap();
        let undeclared = record_one(
            d2.path(),
            "db",
            "select_one",
            serde_json::json!({ "operation": "select_one" }),
            None,
        );
        assert_eq!(undeclared.replay_strategy, ReplayStrategy::Substitute);
        assert_eq!(undeclared.kind, None);
        assert!(undeclared.read_set.is_empty());
        assert!(undeclared.write_set.is_empty());

        let d3 = tempfile::tempdir().unwrap();
        let id_ev = record_one(d3.path(), "id", "nonce", serde_json::json!([]), None);
        assert_eq!(id_ev.entropy_source.as_deref(), Some("id"));
        assert!(id_ev.read_set.is_empty() && id_ev.write_set.is_empty());

        let d4 = tempfile::tempdir().unwrap();
        let clock_ev = record_one(d4.path(), "time", "now", serde_json::json!([]), None);
        assert_eq!(clock_ev.entropy_source.as_deref(), Some("time"));
        assert!(clock_ev.read_set.is_empty() && clock_ev.write_set.is_empty());
    }

    #[test]
    fn finish_boundary_event_firewall_contains_a_panicking_serializer() {
        // SHADOW GUARANTEE: result serialization runs AFTER the real call, inside
        // the firewall in `finish_boundary_event`. A serializer that panics must be
        // contained — never allowed to turn a successful request into a failure.
        let dir = tempfile::tempdir().expect("tempdir");
        let hook = std::sync::Arc::new(RecordingHook::new(dir.path()).expect("hook"));
        let event = EventBuilder::start(
            &*hook,
            "storage",
            "T",
            "m",
            Location::caller(),
            serde_json::json!({}),
        );
        // If the firewall were missing, this would unwind and fail the test.
        finish_boundary_event(
            Some((std::sync::Arc::clone(&hook), event)),
            &(),
            |_: &()| -> (serde_json::Value, bool) { panic!("serializer blew up") },
        );
        // Reached only because the panic was swallowed (the event is simply
        // dropped). The hook remains usable afterwards.
        assert!(hook.flush().is_ok());
    }

    #[test]
    fn value_digest_is_stable_and_folds_in_the_result() {
        let args = serde_json::json!(["settlement_rate_premium"]);
        let r1 = serde_json::json!("0.10");
        let r2 = serde_json::json!("0.20");
        // deterministic for the same (args, result)
        assert_eq!(value_digest_of(&args, &r1), value_digest_of(&args, &r1));
        // a changed RESULT changes the digest (so a value divergence is visible
        // even when args match) — this is the dataflow-hint property
        assert_ne!(value_digest_of(&args, &r1), value_digest_of(&args, &r2));
    }

    #[test]
    fn current_schema_events_round_trip_v8_fields_and_wire_names() {
        let event = BoundaryEvent {
            global_sequence: 1,
            request_sequence: 1,
            correlation_id: Some("c1".to_string()),
            timestamp_ns: 1_780_000_000_000_000_000u64,
            recording_run_id: Some("run-1".to_string()),
            graph_node_id: Some(7),
            tracing_span_id: Some(9),
            task_id: Some("detached-1".to_string()),
            parent_task_id: Some("root".to_string()),
            task_bucket: Some("detached-bucket-1".to_string()),
            bucket_id: Some("detached-bucket-1".to_string()),
            fork_seq: Some(1),
            boundary: "redis".to_string(),
            trait_name: "T".to_string(),
            method_name: "eu_settlement_read".to_string(),
            call_file: "x.rs".to_string(),
            call_line: 1,
            call_column: 1,
            receiver: None,
            request: serde_json::json!({"key": "settlement_rate_default"}),
            args: serde_json::json!(["settlement_rate_default"]),
            response: serde_json::json!("0.10"),
            result: serde_json::json!("0.10"),
            is_error: false,
            duration_us: 1,
            event_schema_version: CURRENT_EVENT_SCHEMA_VERSION,
            callsite_identity: None,
            provenance: Provenance::default(),
            fidelity: Fidelity::Structured,
            result_image: None,
            pre_image: None,
            read_set: vec!["settlement_rate_premium".to_string()],
            write_set: Vec::new(),
            value_digest: None,
            entropy_source: Some("id".to_string()),
            replay_strategy: ReplayStrategy::Execute,
            kind: Some("redis".to_string()),
            declaration: Some(
                BoundaryDeclaration::default()
                    .effect(EffectKind::Redis)
                    .operation(OperationKind::Read)
                    .returns(ReturnSemantics::Raw)
                    .codec(CodecRef::new("redis_raw_value")),
            ),
            raw_draw: None,
            end_timestamp_ns: Some(1_780_000_000_000_000_123u64),
        };

        let round: BoundaryEvent =
            serde_json::from_value(serde_json::to_value(&event).unwrap()).unwrap();
        assert_eq!(round.event_schema_version, CURRENT_EVENT_SCHEMA_VERSION);
        assert_eq!(round.read_set, vec!["settlement_rate_premium".to_string()]);
        assert_eq!(round.declaration, event.declaration);
        assert_eq!(round.entropy_source.as_deref(), Some("id"));
        assert_eq!(round.timestamp_ns, 1_780_000_000_000_000_000u64);
        assert_eq!(round.end_timestamp_ns, Some(1_780_000_000_000_000_123u64));
        assert_eq!(round.fidelity, Fidelity::Structured);
        assert_eq!(round.task_id.as_deref(), Some("detached-1"));
        assert_eq!(round.parent_task_id.as_deref(), Some("root"));
        assert_eq!(round.task_bucket.as_deref(), Some("detached-bucket-1"));
        assert_eq!(round.bucket_id.as_deref(), Some("detached-bucket-1"));
        assert_eq!(round.fork_seq, Some(1));

        let round_wire = serde_json::to_value(&round).unwrap();
        assert_eq!(
            round_wire.get("recon"),
            Some(&serde_json::json!("structured")),
            "fidelity wire name stays pinned to `recon`"
        );
        assert!(
            round_wire.get("fidelity").is_none(),
            "the Rust field name is not a wire alias"
        );
    }

    #[test]
    fn value_digest_parses_from_vector_stringified_u64() {
        // The Kafka→Vector→MinIO record pipeline stringifies u64 > i64::MAX (a
        // value_digest is an FNV-1a hash that routinely exceeds it). Such an
        // event MUST still deserialize — otherwise the renderer/kernel drop it
        // and replay coverage silently collapses (the bug that 401'd /payments).
        let big = 15_482_056_560_522_895_781u64; // > i64::MAX, as seen on disk
        let json = serde_json::json!({
            "global_sequence": 1, "request_sequence": 1, "correlation_id": "c1",
            "timestamp_ns": 1_780_000_000_000_000_000u64,
            "boundary": "db", "trait_name": "T", "method_name": "generic_filter",
            "call_file": "x.rs", "call_line": 1, "call_column": 1,
            "request": {}, "args": {}, "response": {}, "result": {},
            "is_error": false, "duration_us": 1,
            "event_schema_version": CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute",
            "value_digest": big.to_string()  // STRINGIFIED, as Vector emits it
        });
        let ev: BoundaryEvent =
            serde_json::from_value(json).expect("stringified value_digest must parse");
        assert_eq!(ev.value_digest, Some(big));
        // and the bare-number form still works
        let ev2: BoundaryEvent = serde_json::from_value(serde_json::json!({
            "global_sequence": 2, "request_sequence": 1,
            "timestamp_ns": 0, "boundary": "db", "trait_name": "T",
            "method_name": "m", "call_file": "x", "call_line": 1, "call_column": 1,
            "request": {}, "args": {}, "response": {}, "result": {},
            "is_error": false, "duration_us": 0,
            "event_schema_version": CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute",
            "value_digest": 42
        }))
        .unwrap();
        assert_eq!(ev2.value_digest, Some(42));
    }

    #[test]
    fn compute_metrics_from_events() {
        let events = vec![
            BoundaryEvent {
                global_sequence: 0,
                request_sequence: 0,
                correlation_id: Some("req-1".into()),
                timestamp_ns: now_ns(),
                recording_run_id: None,
                graph_node_id: None,
                tracing_span_id: None,
                task_id: Some(ROOT_TASK_ID.to_string()),
                parent_task_id: None,
                task_bucket: Some(ROOT_TASK_ID.to_string()),
                bucket_id: Some(ROOT_TASK_ID.to_string()),
                fork_seq: Some(0),
                boundary: "storage".into(),
                trait_name: "PaymentIntentInterface".into(),
                method_name: "find".into(),
                call_file: "payments.rs".into(),
                call_line: 42,
                call_column: 9,
                receiver: None,
                request: serde_json::json!({}),
                args: serde_json::json!({}),
                response: serde_json::json!({}),
                result: serde_json::json!({}),
                is_error: false,
                duration_us: 100,
                event_schema_version: CURRENT_EVENT_SCHEMA_VERSION,
                callsite_identity: None,
                provenance: Provenance::default(),
                fidelity: Fidelity::default(),
                result_image: None,
                pre_image: None,
                read_set: Vec::new(),
                write_set: Vec::new(),
                value_digest: None,
                entropy_source: None,
                replay_strategy: ReplayStrategy::default(),
                kind: None,
                declaration: None,
                raw_draw: None,
                end_timestamp_ns: None,
            },
            BoundaryEvent {
                global_sequence: 1,
                request_sequence: 0,
                correlation_id: None,
                timestamp_ns: now_ns(),
                recording_run_id: None,
                graph_node_id: None,
                tracing_span_id: None,
                task_id: Some(ROOT_TASK_ID.to_string()),
                parent_task_id: None,
                task_bucket: Some(ROOT_TASK_ID.to_string()),
                bucket_id: Some(ROOT_TASK_ID.to_string()),
                fork_seq: Some(0),
                boundary: "redis".into(),
                trait_name: "RedisPool".into(),
                method_name: "get_key".into(),
                call_file: "cache.rs".into(),
                call_line: 10,
                call_column: 5,
                receiver: None,
                request: serde_json::json!({}),
                args: serde_json::json!({}),
                response: serde_json::json!({"error": "not found"}),
                result: serde_json::json!({"error": "not found"}),
                is_error: true,
                duration_us: 50,
                event_schema_version: CURRENT_EVENT_SCHEMA_VERSION,
                callsite_identity: None,
                provenance: Provenance::default(),
                fidelity: Fidelity::default(),
                result_image: None,
                pre_image: None,
                read_set: Vec::new(),
                write_set: Vec::new(),
                value_digest: None,
                entropy_source: None,
                replay_strategy: ReplayStrategy::default(),
                kind: None,
                declaration: None,
                raw_draw: None,
                end_timestamp_ns: None,
            },
        ];

        let metrics = compute_metrics(&events);
        assert_eq!(metrics.total_events, 2);
        assert_eq!(metrics.correlated_events, 1);
        assert_eq!(metrics.uncorrelated_events, 1);
        assert_eq!(metrics.unique_correlation_ids, 1);
        assert_eq!(metrics.unique_traits, 2);
        assert_eq!(metrics.error_events, 1);
        assert_eq!(metrics.boundaries.get("storage"), Some(&1));
        assert_eq!(metrics.boundaries.get("redis"), Some(&1));
    }

    #[test]
    fn replay_index_uses_strict_to_loose_matching() {
        let events = vec![
            BoundaryEvent {
                global_sequence: 0,
                request_sequence: 0,
                correlation_id: Some("req-1".into()),
                timestamp_ns: now_ns(),
                recording_run_id: None,
                graph_node_id: None,
                tracing_span_id: None,
                task_id: Some(ROOT_TASK_ID.to_string()),
                parent_task_id: None,
                task_bucket: Some(ROOT_TASK_ID.to_string()),
                bucket_id: Some(ROOT_TASK_ID.to_string()),
                fork_seq: Some(0),
                boundary: "storage".into(),
                trait_name: "AddressInterface".into(),
                method_name: "find_address_by_address_id".into(),
                call_file: "payments.rs".into(),
                call_line: 42,
                call_column: 9,
                receiver: None,
                request: serde_json::json!({"address_id": "addr_1"}),
                args: serde_json::json!({"address_id": "addr_1"}),
                response: serde_json::json!({"ok": true}),
                result: serde_json::json!({"ok": true}),
                is_error: false,
                duration_us: 100,
                event_schema_version: CURRENT_EVENT_SCHEMA_VERSION,
                callsite_identity: None,
                provenance: Provenance::default(),
                fidelity: Fidelity::default(),
                result_image: None,
                pre_image: None,
                read_set: Vec::new(),
                write_set: Vec::new(),
                value_digest: None,
                entropy_source: None,
                replay_strategy: ReplayStrategy::default(),
                kind: None,
                declaration: None,
                raw_draw: None,
                end_timestamp_ns: None,
            },
            BoundaryEvent {
                global_sequence: 1,
                request_sequence: 1,
                correlation_id: Some("req-1".into()),
                timestamp_ns: now_ns(),
                recording_run_id: None,
                graph_node_id: None,
                tracing_span_id: None,
                task_id: Some(ROOT_TASK_ID.to_string()),
                parent_task_id: None,
                task_bucket: Some(ROOT_TASK_ID.to_string()),
                bucket_id: Some(ROOT_TASK_ID.to_string()),
                fork_seq: Some(0),
                boundary: "storage".into(),
                trait_name: "AddressInterface".into(),
                method_name: "find_address_by_address_id".into(),
                call_file: "payments.rs".into(),
                call_line: 50,
                call_column: 9,
                receiver: None,
                request: serde_json::json!({"address_id": "addr_2"}),
                args: serde_json::json!({"address_id": "addr_2"}),
                response: serde_json::json!({"ok": true}),
                result: serde_json::json!({"ok": true}),
                is_error: false,
                duration_us: 100,
                event_schema_version: CURRENT_EVENT_SCHEMA_VERSION,
                callsite_identity: None,
                provenance: Provenance::default(),
                fidelity: Fidelity::default(),
                result_image: None,
                pre_image: None,
                read_set: Vec::new(),
                write_set: Vec::new(),
                value_digest: None,
                entropy_source: None,
                replay_strategy: ReplayStrategy::default(),
                kind: None,
                declaration: None,
                raw_draw: None,
                end_timestamp_ns: None,
            },
        ];
        let index = ReplayIndex::new(events);

        let exact_args = serde_json::json!({"address_id": "addr_1"});
        let exact = index
            .find(ReplayQuery {
                correlation_id: Some("req-1"),
                boundary: "storage",
                trait_name: "AddressInterface",
                method_name: "find_address_by_address_id",
                call_file: "payments.rs",
                call_line: 42,
                args: &exact_args,
            })
            .expect("exact match");
        assert_eq!(exact.confidence, ReplayConfidence::Exact);

        let shifted = index
            .find(ReplayQuery {
                correlation_id: Some("req-1"),
                boundary: "storage",
                trait_name: "AddressInterface",
                method_name: "find_address_by_address_id",
                call_file: "payments.rs",
                call_line: 43,
                args: &exact_args,
            })
            .expect("line-shifted match");
        assert_eq!(shifted.confidence, ReplayConfidence::LineShifted);

        let changed_args = serde_json::json!({"address_id": "addr_changed"});
        let changed = index
            .find(ReplayQuery {
                correlation_id: Some("req-1"),
                boundary: "storage",
                trait_name: "AddressInterface",
                method_name: "find_address_by_address_id",
                call_file: "payments.rs",
                call_line: 42,
                args: &changed_args,
            })
            .expect("args-changed match");
        assert_eq!(changed.confidence, ReplayConfidence::ArgsChanged);

        assert_ne!(
            index.call_graph_fingerprint(Some("req-1")),
            FNV_OFFSET_BASIS
        );
    }

    // -----------------------------------------------------------------------
    // `dispatch` seam tests (all four cases via the hook-parameterized twin)
    //
    // The four control-flow cases live identically in `dispatch` (global hook)
    // and `dispatch_with_hook` (injected hook). The injected variant is what the
    // delegate path uses and is fully deterministic (no process-global OnceLock /
    // env state), so the four cases are exercised against it with an in-memory
    // fake `DejaHook`. A separate test covers the global `dispatch` inactive
    // fast-path (the one global case that needs no env mutation).
    // -----------------------------------------------------------------------

    /// In-memory fake hook with knobs to drive each `dispatch` case.
    struct FakeHook {
        active: bool,
        /// When `Some`, `try_replay_with_context` returns it (a lookup hit).
        replay_value: Option<serde_json::Value>,
        /// When true, the fake reports replay mode and can hand back an
        /// execute-shadow token if the caller supplies an Execute boundary spec.
        execute: bool,
        // Observations the test asserts on.
        recorded: Mutex<Vec<BoundaryEvent>>,
        shadow_observed: Mutex<Vec<serde_json::Value>>,
    }

    impl FakeHook {
        fn new(active: bool) -> Self {
            Self {
                active,
                replay_value: None,
                execute: false,
                recorded: Mutex::new(Vec::new()),
                shadow_observed: Mutex::new(Vec::new()),
            }
        }
    }

    impl DejaHook for FakeHook {
        fn mode(&self) -> RuntimeMode {
            if !self.active {
                RuntimeMode::Disabled
            } else if self.replay_value.is_some() || self.execute {
                RuntimeMode::Replay
            } else {
                RuntimeMode::Record
            }
        }
        fn record(&self, event: BoundaryEvent) {
            self.recorded.lock().unwrap().push(event);
        }
        fn next_global_sequence(&self) -> u64 {
            0
        }
        fn next_request_sequence(&self, _correlation_id: Option<&str>) -> u64 {
            0
        }
        fn try_replay_with_context(&self, _query: ReplayLookup<'_>) -> Option<serde_json::Value> {
            self.replay_value.clone()
        }
        fn execute_shadow_peek(&self, query: ReplayLookup<'_>) -> Option<ExecuteShadowToken> {
            if !self.execute {
                return None;
            }
            // Minimal observation; `execute_shadow_observe` fills the result.
            Some(ExecuteShadowToken::new(crate::replay::ObservedCall {
                correlation_id: None,
                boundary: query.boundary.to_string(),
                trait_name: query.trait_name.to_string(),
                method_name: query.method_name.to_string(),
                args: query.args.clone(),
                resolved: false,
                resolved_rank: None,
                source_event_global_sequence: None,
                timestamp_ns: now_ns(),
                end_timestamp_ns: None,
                task_id: Some(ROOT_TASK_ID.to_string()),
                parent_task_id: None,
                task_bucket: Some(ROOT_TASK_ID.to_string()),
                bucket_id: Some(ROOT_TASK_ID.to_string()),
                fork_seq: 0,
                call_file: None,
                call_line: None,
                call_column: None,
                span_path: None,
                graph_node_id: None,
                synthesized: false,
                real_impl_will_fail: false,
                recorded_result: None,
                observed_result: None,
                provenance: crate::Provenance::Shadow,
                seed_gap: false,
            }))
        }
        fn execute_shadow_observe(
            &self,
            token: ExecuteShadowToken,
            observed_result: serde_json::Value,
        ) {
            let _ = token;
            self.shadow_observed.lock().unwrap().push(observed_result);
        }
    }

    fn test_identity() -> CallsiteIdentity {
        CallsiteIdentity {
            version: 1,
            source: CallsiteSource::SyntacticHash,
            id: None,
            scope: Some("T::m".to_string()),
            occurrence: 0,
            caller_function: None,
            lexical_path: Some("crate::m".to_string()),
            syntax_hash: Some(123),
            span_path: None,
        }
    }

    fn delegate_obs<'a>(hook: &'a FakeHook) -> DelegateObservation<'a> {
        delegate_obs_with_spec(hook, BoundarySpec::new("unit", "T", "m"))
    }

    fn delegate_obs_with_spec<'a>(
        hook: &'a FakeHook,
        spec: BoundarySpec,
    ) -> DelegateObservation<'a> {
        DelegateObservation {
            hook,
            spec,
            caller: Location::caller(),
            identity: test_identity(),
            receiver: None,
        }
    }

    /// Case 1 — INACTIVE hook is handled by the GLOBAL `dispatch` fast path and by
    /// the delegate MACRO's `if !is_active` gate (the seam itself assumes the
    /// caller has gated activity). The authoritative inactive-laziness proof for
    /// the boundary path is `dispatch_global_inactive_runs_without_evaluating_args_thunk`
    /// below; for the delegate path it is the integration test
    /// `fast_path_skips_recording_when_inactive`. Here we only assert the seam
    /// contract: `run` always executes and returns its value.
    #[test]
    fn dispatch_with_hook_always_runs_the_block() {
        let hook = FakeHook::new(true);
        let out = dispatch_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || 7u64,
            |_v| Reconstructed::Failed,
            |r: &u64| (serde_json::json!(*r), false),
        );
        assert_eq!(out, 7);
    }

    /// Case 2 — RECORD (active, no replay hit, lookup mode): a legacy extractor
    /// returning `(Value, bool)` still records exactly that result and no images.
    #[test]
    fn dispatch_with_hook_legacy_tuple_extractor_records_result_and_empty_images() {
        let hook = FakeHook::new(true);
        let out = dispatch_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || 42u64,
            |_v| Reconstructed::Failed,
            |r: &u64| (serde_json::json!(*r), false),
        );
        assert_eq!(out, 42);
        let recorded = hook
            .recorded
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].result, serde_json::json!(42));
        assert_eq!(recorded[0].args, serde_json::json!({"k": "v"}));
        assert!(recorded[0].read_set.is_empty());
        assert!(recorded[0].write_set.is_empty());
        assert_eq!(recorded[0].result_image, None);
        assert_eq!(recorded[0].pre_image, None);
        assert!(hook
            .shadow_observed
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .is_empty());
    }

    #[test]
    fn dispatch_with_hook_recorded_output_stamps_explicit_keys_images_without_db_inference() {
        let hook = FakeHook::new(true);
        let explicit_read = crate::replay::StateKey::DbRow {
            table: "users".to_owned(),
            pk_column: "user_id".to_owned(),
            pk_value: "explicit_read_user".to_owned(),
        }
        .to_wire();
        let explicit_write = crate::replay::StateKey::DbRow {
            table: "users".to_owned(),
            pk_column: "user_id".to_owned(),
            pk_value: "explicit_write_user".to_owned(),
        }
        .to_wire();
        let pre_image = serde_json::json!({"phase": "pre", "user_id": "explicit_read_user"});
        let result_image = serde_json::json!({"phase": "post", "user_id": "explicit_write_user"});
        let live_result = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": {
                "user_id": "inferred_user",
                "merchant_id": "merch_ignored"
            },
            "type_name": "User"
        });

        let out = dispatch_with_hook(
            delegate_obs_with_spec(
                &hook,
                BoundarySpec::new("db", "Execute", "generic_update_with_results"),
            ),
            serde_json::json!({
                "operation": "generic_update_with_results",
                "table": "users",
                "sql": "UPDATE \"users\" SET \"merchant_id\" = $1 WHERE \"user_id\" = $2 RETURNING *",
                "inputs": ["merch_ignored", "inferred_user"]
            }),
            || live_result.clone(),
            |_v| Reconstructed::Failed,
            |_| {
                RecordedOutput::new(live_result.clone(), false)
                    .with_read_set(vec![explicit_read.clone()])
                    .with_write_set(vec![explicit_write.clone()])
                    .with_result_image(result_image.clone())
                    .with_pre_image(pre_image.clone())
            },
        );
        assert_eq!(out, live_result);

        let recorded = hook
            .recorded
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert_eq!(recorded.len(), 1);
        let event = &recorded[0];
        assert_eq!(event.result, live_result);
        assert_eq!(
            event.read_set,
            vec![explicit_read],
            "DB-shaped args/result must not infer any read key beyond the extractor payload"
        );
        assert_eq!(
            event.write_set,
            vec![explicit_write],
            "DB-shaped args/result must not infer any write key beyond the extractor payload"
        );
        assert_eq!(event.pre_image, Some(pre_image));
        assert_eq!(event.result_image, Some(result_image));
    }

    /// Case 3a — LOOKUP HIT that reconstructs: `run` is NEVER called, the recorded
    /// value is returned, nothing new is recorded.
    #[test]
    fn dispatch_with_hook_lookup_hit_skips_run() {
        let mut hook = FakeHook::new(true);
        hook.replay_value = Some(serde_json::json!(99));
        let ran = std::cell::Cell::new(false);
        let out = dispatch_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || {
                ran.set(true);
                7u64
            },
            |v| match serde_json::from_value::<u64>(v) {
                Ok(value) => Reconstructed::Value(value),
                Err(_) => Reconstructed::Failed,
            },
            |r: &u64| (serde_json::json!(*r), false),
        );
        assert_eq!(out, 99, "returned the reconstructed recorded value");
        assert!(!ran.get(), "the real block must NOT run on a lookup hit");
        assert!(hook.recorded.lock().unwrap().is_empty());
    }

    /// Case 3b — LOOKUP HIT containing the ResultOk error sentinel: fail-stops
    /// instead of rerunning and re-recording the live boundary.
    #[test]
    fn dispatch_with_hook_lookup_hit_result_ok_error_sentinel_fail_stops_before_run() {
        let mut hook = FakeHook::new(true);
        hook.replay_value = Some(serde_json::json!({"deja_err": "boom"}));
        let ran = std::cell::Cell::new(false);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dispatch_with_hook(
                delegate_obs(&hook),
                serde_json::json!({"k": "v"}),
                || {
                    ran.set(true);
                    5u64
                },
                |v| {
                    if v.as_object()
                        .is_some_and(|map| map.contains_key("deja_err"))
                    {
                        return Reconstructed::Failed;
                    }
                    match serde_json::from_value::<u64>(v) {
                        Ok(value) => Reconstructed::Value(value),
                        Err(_) => Reconstructed::Failed,
                    }
                },
                |r: &u64| (serde_json::json!(*r), false),
            )
        }));

        assert!(
            result.is_err(),
            "ResultOk error sentinels must fail-stop instead of falling through"
        );
        assert!(
            !ran.get(),
            "fail-stop must happen before the real delegate closure is called"
        );
        assert!(
            hook.recorded.lock().unwrap().is_empty(),
            "fail-stop must not emit a replacement recording"
        );
    }

    #[test]
    fn dispatch_with_hook_lookup_hit_unreconstructable_fail_stops_before_run() {
        let mut hook = FakeHook::new(true);
        hook.replay_value = Some(serde_json::json!("not-a-u64"));
        let ran = std::cell::Cell::new(false);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dispatch_with_hook(
                delegate_obs(&hook),
                serde_json::json!({"k": "v"}),
                || {
                    ran.set(true);
                    5u64
                },
                |_v| Reconstructed::Failed,
                |r: &u64| (serde_json::json!(*r), false),
            )
        }));

        assert!(
            result.is_err(),
            "malformed Substitute lookup hits must fail-stop instead of falling through"
        );
        assert!(
            !ran.get(),
            "fail-stop must happen before the real delegate closure is called"
        );
        assert!(
            hook.recorded.lock().unwrap().is_empty(),
            "fail-stopped replay must not record a live fall-through result"
        );
    }

    #[test]
    fn dispatch_with_hook_delegate_observation_execute_semantics_runs_real_and_observes_shadow() {
        let mut hook = FakeHook::new(true);
        hook.execute = true;
        let ran = std::cell::Cell::new(false);

        let out = dispatch_with_hook(
            delegate_obs_with_spec(
                &hook,
                BoundarySpec::with_semantics(
                    "unit",
                    "T",
                    "m",
                    BoundarySemantics {
                        replay_strategy: ReplayStrategy::Execute,
                        kind: Some("unit".to_string()),
                        declaration: None,
                    },
                ),
            ),
            serde_json::json!({"k": "v"}),
            || {
                ran.set(true);
                7u64
            },
            |v| match serde_json::from_value::<u64>(v) {
                Ok(value) => Reconstructed::Value(value),
                Err(_) => Reconstructed::Failed,
            },
            |r: &u64| (serde_json::json!(*r), false),
        );

        assert_eq!(out, 7);
        assert!(
            ran.get(),
            "Execute replay semantics must run the real delegate closure"
        );
        assert!(hook.recorded.lock().unwrap().is_empty());
        assert_eq!(
            hook.shadow_observed.lock().unwrap().as_slice(),
            [serde_json::json!(7)],
            "Execute replay semantics must observe the live result through the shadow path"
        );
    }

    #[test]
    fn dispatch_with_hook_record_and_execute_shadow_use_same_result_extractor() {
        #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
        enum RedisLikeValue {
            Null,
            // Present so the serde shape mirrors fred's RedisValue (the Null
            // unit variant must NOT serialize like the tagged String form —
            // the exact Gate-3 asymmetry this test pins).
            #[allow(dead_code)]
            String(String),
        }

        fn capture_redis_like(value: &Result<RedisLikeValue, ()>) -> (serde_json::Value, bool) {
            match value {
                Ok(value) => (serde_json::to_value(value).unwrap(), false),
                Err(()) => (serde_json::json!({"deja_err": "()"}), true),
            }
        }

        let record_hook = FakeHook::new(true);
        let recorded_out = dispatch_with_hook(
            delegate_obs(&record_hook),
            serde_json::json!({"key": "merchant_key_store_default"}),
            || Ok(RedisLikeValue::Null),
            |_v| Reconstructed::Failed,
            capture_redis_like,
        );
        assert_eq!(recorded_out, Ok(RedisLikeValue::Null));
        let recorded_result = {
            let recorded = record_hook.recorded.lock().unwrap();
            assert_eq!(recorded.len(), 1);
            recorded[0].result.clone()
        };
        assert_eq!(
            recorded_result,
            serde_json::json!("Null"),
            "the Redis raw nil unit variant serializes as the current bare string shape"
        );

        let mut shadow_hook = FakeHook::new(true);
        shadow_hook.execute = true;
        let shadow_out = dispatch_with_hook(
            delegate_obs_with_spec(
                &shadow_hook,
                BoundarySpec::with_semantics(
                    "redis",
                    "RedisStore",
                    "get_key",
                    BoundarySemantics {
                        replay_strategy: ReplayStrategy::Execute,
                        kind: Some("redis".to_string()),
                        declaration: Some(BoundaryDeclaration::default().effect(EffectKind::Redis)),
                    },
                ),
            ),
            serde_json::json!({"key": "merchant_key_store_default"}),
            || Ok(RedisLikeValue::Null),
            |_v| Reconstructed::Failed,
            capture_redis_like,
        );
        assert_eq!(shadow_out, Ok(RedisLikeValue::Null));
        assert_eq!(
            shadow_hook.shadow_observed.lock().unwrap().as_slice(),
            [recorded_result],
            "recorded and execute-shadow provenances must serialize the same value through the same extractor"
        );
    }

    #[test]
    fn dispatch_with_hook_delegate_observation_default_spec_uses_lookup_substitute() {
        let mut hook = FakeHook::new(true);
        hook.replay_value = Some(serde_json::json!(99));
        let ran = std::cell::Cell::new(false);

        let out = dispatch_with_hook(
            delegate_obs_with_spec(&hook, BoundarySpec::new("unit", "T", "m")),
            serde_json::json!({"k": "v"}),
            || {
                ran.set(true);
                7u64
            },
            |v| match serde_json::from_value::<u64>(v) {
                Ok(value) => Reconstructed::Value(value),
                Err(_) => Reconstructed::Failed,
            },
            |r: &u64| (serde_json::json!(*r), false),
        );

        assert_eq!(
            out, 99,
            "default Substitute semantics must replay lookup hits"
        );
        assert!(
            !ran.get(),
            "default Substitute semantics must not run the real delegate closure on a lookup hit"
        );
        assert!(hook.recorded.lock().unwrap().is_empty());
        assert!(
            hook.shadow_observed.lock().unwrap().is_empty(),
            "default Substitute semantics must not enter execute-shadow observation"
        );
    }

    /// The async twin routes the same four-case control flow; smoke-test the
    /// record and lookup-hit cases through `dispatch_async_with_hook`.
    #[tokio::test]
    async fn dispatch_async_with_hook_records_and_replays() {
        // record
        let hook = FakeHook::new(true);
        let out = dispatch_async_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || async { 21u64 },
            |_v| Reconstructed::Failed,
            |r: &u64| (serde_json::json!(*r), false),
        )
        .await;
        assert_eq!(out, 21);
        assert_eq!(hook.recorded.lock().unwrap().len(), 1);

        // lookup hit
        let mut hook = FakeHook::new(true);
        hook.replay_value = Some(serde_json::json!(100));
        let out = dispatch_async_with_hook(
            delegate_obs(&hook),
            serde_json::json!({"k": "v"}),
            || async { 0u64 },
            |v| match serde_json::from_value::<u64>(v) {
                Ok(value) => Reconstructed::Value(value),
                Err(_) => Reconstructed::Failed,
            },
            |r: &u64| (serde_json::json!(*r), false),
        )
        .await;
        assert_eq!(out, 100);
        assert!(hook.recorded.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dispatch_async_with_hook_delegate_observation_execute_semantics_runs_real_and_observes_shadow(
    ) {
        let mut hook = FakeHook::new(true);
        hook.execute = true;
        let ran = std::cell::Cell::new(false);

        let out = dispatch_async_with_hook(
            delegate_obs_with_spec(
                &hook,
                BoundarySpec::with_semantics(
                    "unit",
                    "T",
                    "m",
                    BoundarySemantics {
                        replay_strategy: ReplayStrategy::Execute,
                        kind: Some("unit".to_string()),
                        declaration: None,
                    },
                ),
            ),
            serde_json::json!({"k": "v"}),
            || async {
                ran.set(true);
                11u64
            },
            |v| match serde_json::from_value::<u64>(v) {
                Ok(value) => Reconstructed::Value(value),
                Err(_) => Reconstructed::Failed,
            },
            |r: &u64| (serde_json::json!(*r), false),
        )
        .await;

        assert_eq!(out, 11);
        assert!(
            ran.get(),
            "Execute replay semantics must run the real async delegate closure"
        );
        assert!(hook.recorded.lock().unwrap().is_empty());
        assert_eq!(
            hook.shadow_observed.lock().unwrap().as_slice(),
            [serde_json::json!(11)],
            "Execute replay semantics must observe the async live result through the shadow path"
        );
    }

    /// The global `dispatch` inactive fast-path: with no runtime hook AND no
    /// recording hook configured for this process, `dispatch` runs `run`, returns
    /// the value, and NEVER evaluates the `args` thunk (zero-overhead inactive
    /// path). This is the only global-`dispatch` case testable without mutating
    /// the process-wide env/OnceLock state.
    #[test]
    fn dispatch_global_inactive_runs_without_evaluating_args_thunk() {
        // No DEJA_MODE / DEJA_ARTIFACT_DIR set in this unit-test binary, so both
        // the runtime hook and the recording hook resolve to None.
        if global_runtime_hook_from_env().is_some() || global_hook_from_env().is_some() {
            // A sibling test installed a hook; skip rather than assert on shared
            // global state.
            return;
        }
        let identity = test_identity();
        let args_evaluated = std::cell::Cell::new(false);
        let out = dispatch(
            CrossingObservation::new(
                BoundarySpec::new("unit", "T", "m"),
                identity,
                Location::caller(),
            ),
            || {
                args_evaluated.set(true);
                serde_json::json!({"k": "v"})
            },
            || 55u64,
            |_v| Reconstructed::Failed,
            |r: &u64| (serde_json::json!(*r), false),
        );
        assert_eq!(out, 55);
        assert!(
            !args_evaluated.get(),
            "inactive `dispatch` must NOT evaluate the args thunk"
        );
    }

    /// `fall_through_silent` remains source-compatible for legacy call sites, but
    /// Phase C makes it a no-op: Substitute HIT-but-unreconstructable replay
    /// fail-stops instead of authorizing any live fallback.
    #[test]
    fn crossing_observation_fall_through_silent_is_source_compatible_noop() {
        let obs = CrossingObservation::with_correlation(
            BoundarySpec::new("unit", "T", "m"),
            test_identity(),
            Location::caller(),
            Some("req-1".to_string()),
        )
        .fall_through_silent();

        assert_eq!(obs.correlation_id.as_deref(), Some("req-1"));
    }

    /// The execute-shadow observer firewall is loud, never silent: a
    /// non-panicking observer runs exactly once, and a panicking observer
    /// RE-RAISES (after a diagnostic) instead of letting the dispatch arm
    /// return live output with no observation (silent divergence
    /// under-reporting).
    #[test]
    fn shadow_observe_loud_runs_observer_and_repropagates_panics() {
        use std::cell::Cell;

        let ran = Cell::new(0u32);
        shadow_observe_loud("db", "generic_insert", || ran.set(ran.get() + 1));
        assert_eq!(ran.get(), 1, "non-panicking observer runs exactly once");

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shadow_observe_loud("db", "generic_insert", || panic!("observer sink broke"));
        }));
        let payload = result.expect_err("observer panic must propagate, never be swallowed");
        let msg = payload
            .downcast_ref::<&str>()
            .copied()
            .map(str::to_owned)
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_default();
        assert!(
            msg.contains("observer sink broke"),
            "the ORIGINAL panic payload is re-raised (got: {msg:?})"
        );
    }

    /// Same invariant for the DELEGATE dispatch variants
    /// (`dispatch_with_hook`/`dispatch_async_with_hook`): their Execute arms
    /// route `obs.hook.execute_shadow_observe(...)` through the same
    /// `shadow_observe_loud` firewall, so a panicking hook observer propagates
    /// (loudly) instead of the arm returning live output with no observation.
    /// Mirrors the delegate call shape: a method call on a captured receiver.
    #[test]
    fn shadow_observe_loud_propagates_delegate_hook_observer_panics() {
        struct PanickyHook;
        impl PanickyHook {
            fn execute_shadow_observe(&self, _token: u32, _observed: serde_json::Value) {
                panic!("delegate hook observer broke");
            }
        }
        let hook = PanickyHook;
        let token = 7u32;
        let result_json = serde_json::json!({"ok": true});
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            shadow_observe_loud("redis", "set_key", || {
                hook.execute_shadow_observe(token, result_json);
            });
        }));
        assert!(
            result.is_err(),
            "delegate hook observer panic must propagate, never be swallowed"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn spawn_fork_always_spawns_immediately() {
        let (tx, rx) = std::sync::mpsc::channel();

        spawn_fork(async move {
            tx.send(()).expect("test receiver alive");
        });

        rx.recv_timeout(std::time::Duration::from_secs(1))
            .expect("fork spawn did not run immediately like tokio::spawn");
    }
}
