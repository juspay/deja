//! Replay engine for Déjà semantic events.
//!
//! Provides `ReplayHook` — a `DejaHook` that substitutes recorded responses
//! instead of letting the real implementation hit external systems.
//!
//! Uses resilient replay: divergence is logged but control flow continues.
//! Missing calls are recovered via sliding-window search; novel calls trigger
//! graceful synthesis.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};

use crate::{
    correlation_matches, read_events, BoundaryEvent, BoundarySpec, CallsiteIdentity,
    CallsiteSource, DejaHook, EffectKind, ExecuteMode, OperationKind, ReplayLookup, ReplayStrategy,
    ReturnSemantics, RuntimeMode,
};

const STATE_KEY_V1_PREFIX: &str = "deja:state:v1";

/// Typed representation of a boundary state key.
///
/// The on-event wire fields (`BoundaryEvent.read_set` / `write_set`) intentionally
/// remain `Vec<String>` for compatibility; this type is the canonical internal
/// representation for newly emitted keys. Unknown legacy strings parse as
/// [`StateKey::Opaque`] and are never mined for DB table identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StateKey {
    /// A legacy or non-typed key. Seed planning preserves it exactly, but treats it
    /// as opaque (no `table:sql` splitting).
    Opaque(String),
    /// Exact database row identity. `pk_column` is part of the identity so a
    /// non-unique value from the wrong column can never group unrelated rows.
    DbRow {
        table: String,
        pk_column: String,
        pk_value: String,
    },
    /// Database query fallback identity, used when the row primary key is not
    /// available from the structured result.
    DbQuery { table: String, fingerprint: String },
    /// Redis physical key identity. `key` is the UTF-8 rendering used by existing
    /// Redis instrumentation; byte-exact images can carry raw bytes separately.
    RedisKey { key: String },
}

/// Parse error for malformed typed v1 state keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateKeyParseError {
    message: String,
}

impl StateKeyParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl std::fmt::Display for StateKeyParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StateKeyParseError {}

impl StateKey {
    /// Parse a wire key. Unknown/legacy strings, including old typed-v1 shapes
    /// that no longer carry enough identity, become [`StateKey::Opaque`].
    pub fn parse(wire: &str) -> Result<Self, StateKeyParseError> {
        let Some(rest) = wire.strip_prefix(STATE_KEY_V1_PREFIX) else {
            return Ok(StateKey::Opaque(wire.to_owned()));
        };
        let rest = rest
            .strip_prefix(':')
            .ok_or_else(|| StateKeyParseError::new("typed state key missing kind separator"))?;
        let parts: Vec<&str> = rest.split(':').collect();
        match parts.as_slice() {
            ["db_row", table, pk_column, pk_value] => Ok(StateKey::DbRow {
                table: decode_hex_component(table)?,
                pk_column: decode_hex_component(pk_column)?,
                pk_value: decode_hex_component(pk_value)?,
            }),
            ["db_query", table, fingerprint] => Ok(StateKey::DbQuery {
                table: decode_hex_component(table)?,
                fingerprint: decode_hex_component(fingerprint)?,
            }),
            ["redis", key] => Ok(StateKey::RedisKey {
                key: decode_hex_component(key)?,
            }),
            ["db_row", ..] | ["db_query", ..] | ["redis", ..] | [_, ..] => {
                Ok(StateKey::Opaque(wire.to_owned()))
            }
            [] => Err(StateKeyParseError::new("typed state key missing kind")),
        }
    }

    /// Render the deterministic wire string. Typed variants use the v1 prefix;
    /// opaque keys preserve the original legacy string byte-for-byte.
    pub fn to_wire(&self) -> String {
        match self {
            StateKey::Opaque(raw) => raw.clone(),
            StateKey::DbRow {
                table,
                pk_column,
                pk_value,
            } => format!(
                "{STATE_KEY_V1_PREFIX}:db_row:{}:{}:{}",
                encode_hex_component(table),
                encode_hex_component(pk_column),
                encode_hex_component(pk_value)
            ),
            StateKey::DbQuery { table, fingerprint } => format!(
                "{STATE_KEY_V1_PREFIX}:db_query:{}:{}",
                encode_hex_component(table),
                encode_hex_component(fingerprint)
            ),
            StateKey::RedisKey { key } => {
                format!("{STATE_KEY_V1_PREFIX}:redis:{}", encode_hex_component(key))
            }
        }
    }

    /// Returns the database table for DB state keys. Opaque legacy strings return
    /// `None`; this is what keeps state keys out of lookup identity and prevents
    /// legacy `table:sql` strings from being parsed as typed DB keys.
    pub fn db_table(&self) -> Option<&str> {
        match self {
            StateKey::DbRow { table, .. } | StateKey::DbQuery { table, .. } => Some(table),
            StateKey::Opaque(_) | StateKey::RedisKey { .. } => None,
        }
    }
}

fn encode_hex_component(input: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = input.as_bytes();
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

fn decode_hex_component(input: &str) -> Result<String, StateKeyParseError> {
    if input.len() % 2 != 0 {
        return Err(StateKeyParseError::new("hex component has odd length"));
    }
    let mut bytes = Vec::with_capacity(input.len() / 2);
    for chunk in input.as_bytes().chunks_exact(2) {
        let hi = hex_nibble(chunk[0])?;
        let lo = hex_nibble(chunk[1])?;
        bytes.push((hi << 4) | lo);
    }
    String::from_utf8(bytes).map_err(|_| StateKeyParseError::new("hex component is not utf-8"))
}

fn hex_nibble(byte: u8) -> Result<u8, StateKeyParseError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(StateKeyParseError::new(
            "hex component contains non-hex digit",
        )),
    }
}

fn canonical_state_key_wire(key: &str) -> String {
    StateKey::parse(key)
        .map(|state_key| state_key.to_wire())
        .unwrap_or_else(|_| key.to_owned())
}

fn state_key_fnv1a_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// Deterministic fingerprint for a DB query fallback state key.
pub fn db_query_fingerprint(
    operation: &str,
    table: &str,
    sql: &str,
    inputs: &serde_json::Value,
) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    hash = state_key_fnv1a_bytes(hash, b"db-query-v1");
    hash = state_key_fnv1a_bytes(hash, operation.as_bytes());
    hash = state_key_fnv1a_bytes(hash, b"\0");
    hash = state_key_fnv1a_bytes(hash, table.as_bytes());
    hash = state_key_fnv1a_bytes(hash, b"\0");
    hash = state_key_fnv1a_bytes(hash, sql.as_bytes());
    hash = state_key_fnv1a_bytes(hash, b"\0");
    hash = hash_value(hash, inputs);
    format!("{hash:016x}")
}

/// Build the typed v1 DB query fallback key for a boundary event.
pub fn db_query_state_key(
    operation: &str,
    table: &str,
    sql: &str,
    inputs: &serde_json::Value,
) -> StateKey {
    StateKey::DbQuery {
        table: table.to_owned(),
        fingerprint: db_query_fingerprint(operation, table, sql, inputs),
    }
}

/// Extract the DB table from the structured DB event args/request envelope.
pub fn db_table_from_event_args(args: &serde_json::Value) -> Option<&str> {
    args.get("table").and_then(serde_json::Value::as_str)
}

/// Return the known primary-key column for tables where Phase C can prove row
/// uniqueness from a serialized result. Unknown tables deliberately fall back to
/// [`StateKey::DbQuery`] so Rule A cannot group unrelated rows by a merely common
/// column such as `merchant_id`.
fn db_pk_column_for_table(
    table: &str,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Option<&'static str> {
    let candidate = match table {
        "payment_attempt" => "attempt_id",
        "payment_intent" => "payment_id",
        "merchant_account" | "merchant_key_store" => "merchant_id",
        "business_profile" => "profile_id",
        "merchant_connector_account" => "merchant_connector_id",
        "customers" => "customer_id",
        "organization" | "organizations" => "organization_id",
        "users" => "user_id",
        "api_keys" => "key_id",
        "user_authentication_methods" => "id",
        _ => return None,
    };
    object
        .get(candidate)
        .filter(|value| !value.is_null())
        .map(|_| candidate)
}

/// Build a row-exact DB state key from a serialized row/object only when this
/// table's real primary-key column is known and present. Unknown tables return
/// `None` and must use the query-fingerprint fallback.
pub fn db_row_state_key(table: &str, row: &serde_json::Value) -> Option<StateKey> {
    let object = row.as_object()?;
    let column = db_pk_column_for_table(table, object)?;
    let value = object.get(column)?;
    Some(StateKey::DbRow {
        table: table.to_owned(),
        pk_column: column.to_owned(),
        pk_value: db_pk_wire_value(value),
    })
}

/// Extract all row-exact DB state keys carried by a structured DB `Ok` value or
/// row/object array. Non-row shapes and rows without pragmatic PK columns are
/// ignored; callers can fall back to [`db_query_state_key`].
pub fn db_row_state_keys(table: &str, value: &serde_json::Value) -> Vec<StateKey> {
    fn visit(table: &str, value: &serde_json::Value, keys: &mut Vec<StateKey>) {
        if let Some(key) = db_row_state_key(table, value) {
            keys.push(key);
            return;
        }
        match value {
            serde_json::Value::Array(values) => {
                for value in values {
                    visit(table, value, keys);
                }
            }
            serde_json::Value::Object(object) => {
                if object.get("result").and_then(serde_json::Value::as_str) == Some("Ok") {
                    if let Some(ok_value) = object.get("value") {
                        visit(table, ok_value, keys);
                    }
                }
                if let Some(ok_value) = object.get("Ok") {
                    visit(table, ok_value, keys);
                }
            }
            _ => {}
        }
    }

    let mut keys = Vec::new();
    visit(table, value, &mut keys);
    keys
}

fn db_pk_wire_value(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(value) => value.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_owned()),
    }
}

// ---------------------------------------------------------------------------
// Divergence tracking
// ---------------------------------------------------------------------------

/// A single divergence detected during replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Divergence {
    pub kind: DivergenceKind,
    pub boundary: String,
    pub trait_name: String,
    pub method_name: String,
    pub detail: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub baseline: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate: Option<serde_json::Value>,
    pub global_sequence: u64,
}

/// Classification of a replay divergence.
///
/// Note: not `Copy` because [`ValueDiverged`](DivergenceKind::ValueDiverged) and
/// [`InconclusiveSeedGap`](DivergenceKind::InconclusiveSeedGap) carry owned
/// payloads (recorded vs observed values, callsite). The unit variants are
/// unaffected; comparisons still use `==`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DivergenceKind {
    /// Arguments differ from the recorded baseline at the same position.
    #[serde(rename = "field_mismatch")]
    ArgsDiverged,
    /// A call present in V1 was skipped in V2.
    OmittedCall,
    /// V2 made a call not present in V1.
    NovelCall,
    /// The recorded result could not be deserialized into the expected type.
    DeserializationFailure,
    /// Recovery succeeded but something was different along the way.
    Recovered,
    /// Correlation ID mismatch.
    CorrelationMismatch,
    /// A recorded result was available with mismatched args, but the
    /// configured [`ArgMismatchPolicy`] forbade returning it. The cursor was
    /// NOT advanced and the call falls through to the real implementation
    /// (or a graceful synthesis) instead of silently lying.
    ArgSkipBlocked,
    /// The candidate ran the REAL boundary (execute mode) and produced a result
    /// that differs in VALUE from the recorded baseline at the same args-free
    /// call-site + occurrence. This is the total-derivative signal: a recorded
    /// WRITE (Omitted) and the execute WRITE (Novel) are paired by args-free
    /// identity into ONE divergence, since the diverging value (e.g. a doubled
    /// amount) changes the args and would otherwise split them. `args_hash` is
    /// used here as a DIFF signal, not a resolution key.
    ValueDiverged {
        /// The recorded baseline value for this call-site.
        recorded: serde_json::Value,
        /// The value the real boundary produced under execute mode.
        observed: serde_json::Value,
        /// Args-free call-site identity the two sides were paired on
        /// (`boundary::trait::method`).
        callsite: String,
        /// Occurrence index within the correlation scope used for pairing.
        occurrence: u32,
    },
    /// The candidate's execute-mode call could not be conclusively classified
    /// because the recorded baseline it needed to compare against was absent — a
    /// seed gap. Surfaced explicitly so a missing baseline is not silently
    /// counted as a match (false negative) nor as a divergence (false positive).
    InconclusiveSeedGap {
        /// Args-free call-site identity (`boundary::trait::method`).
        callsite: String,
        /// Occurrence index within the correlation scope.
        occurrence: u32,
    },
}

/// Accumulated replay report for one session.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayReport {
    pub total_calls: u64,
    pub matched_calls: u64,
    pub divergence_count: u64,
    pub divergences: Vec<Divergence>,
}

impl ReplayReport {
    pub fn has_divergences(&self) -> bool {
        !self.divergences.is_empty()
    }

    /// Append a divergence and increment the counter.
    pub fn push(&mut self, div: Divergence) {
        self.divergence_count += 1;
        self.divergences.push(div);
    }
}

// ---------------------------------------------------------------------------
// Replay configuration
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayConfig {
    /// Maximum number of events to scan forward when an omitted call is
    /// suspected.
    pub sliding_window_size: usize,
    /// Controls whether and when a recorded result is returned despite the
    /// V2 args differing from the recorded baseline.
    ///
    /// Default (`OnlyForArgful`) fails closed on argless
    /// boundaries (time, id, random) so they cannot silently lie, but allows
    /// recovery for genuine business-logic boundaries that took meaningful
    /// args.
    pub arg_mismatch_policy: ArgMismatchPolicy,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            sliding_window_size: 20,
            arg_mismatch_policy: Default::default(),
        }
    }
}

/// Governs whether a recorded result may be returned when the V2 arguments
/// differ from the recorded V1 arguments at the same call-site.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArgMismatchPolicy {
    /// Never return a recorded result if args don't match. Strictest. Required
    /// for argless boundaries (time, id, random) to prevent silent lies.
    Never,
    /// Return a recorded result on arg mismatch ONLY if the method had args to
    /// begin with. Default. Argless calls (null args, empty object) fall back
    /// to `Never`.
    #[default]
    OnlyForArgful,
    /// Return any plausible recorded result on arg mismatch. Permissive;
    /// matches the pre-P2 behavior of `skip_arg_mismatch: true`.
    Always,
}

impl ArgMismatchPolicy {
    fn allow_arg_mismatch(self, args: &serde_json::Value) -> bool {
        match self {
            Self::Never => false,
            Self::OnlyForArgful => !args_are_empty(args),
            Self::Always => true,
        }
    }
}

/// Returns true if `args` is JSON-null or an empty object (treated as
/// "argless" for arg-mismatch policy purposes).
fn args_are_empty(args: &serde_json::Value) -> bool {
    args.is_null() || args.as_object().is_some_and(|m| m.is_empty())
}

/// Decides whether a given arg-mismatch case is allowed to fall back to the
/// recorded result.
fn allow_arg_mismatch(setting: ArgMismatchPolicy, args: &serde_json::Value) -> bool {
    setting.allow_arg_mismatch(args)
}

// ---------------------------------------------------------------------------
// Per-request replay state
// ---------------------------------------------------------------------------

/// Mutable cursor for one correlation scope.
#[derive(Debug)]
struct RequestCursor {
    /// Index into the sorted event list for this request.
    position: usize,
    /// Events belonging to this correlation_id, sorted by request_sequence.
    events: Vec<BoundaryEvent>,
}

/// Internal result of a single match attempt.
#[derive(Debug, Clone)]
enum MatchOutcome {
    Exact,
    RecoveredSkip(usize),         // advanced past N omitted calls
    RecoveredWithMismatch(usize), // same but args differed
    /// A method+args-relaxed match was available but the configured
    /// [`ArgMismatchPolicy`] forbade returning it. Carries the recorded args
    /// so the caller can record an [`DivergenceKind::ArgSkipBlocked`]
    /// divergence with both baseline and candidate.
    ArgSkipBlocked(serde_json::Value),
    Novel,
}

// ---------------------------------------------------------------------------
// ReplayHook
// ---------------------------------------------------------------------------

/// A `DejaHook` that replays recorded semantic events.
///
/// On each incoming call, searches the recorded tape for a matching event.
/// Returns the recorded `result` JSON if found; otherwise logs a divergence
/// and falls back to `None` (letting the delegation call the real impl).
pub struct ReplayHook {
    config: ReplayConfig,
    /// All events loaded from the artifact.
    all_events: Vec<BoundaryEvent>,
    /// Per-correlation-id cursors.
    cursors: Mutex<BTreeMap<Option<String>, RequestCursor>>,
    /// Accumulated divergence report.
    report: Mutex<ReplayReport>,
    /// Global sequence counter so we still produce monotonic seq numbers.
    global_seq: Mutex<u64>,
    /// Per-(correlation, bucket, source, scope) monotonic occurrence counter mirroring
    /// `RecordingHook::next_callsite_occurrence`. Replay-time occurrence
    /// numbering MUST advance in lock-step with recording-time so that
    /// `CallsiteIdentity { source: OperationOccurrence, occurrence }` lookups
    /// land on the same event.
    callsite_occurrence: Mutex<crate::CallsiteOccurrenceMap>,
}

impl ReplayHook {
    /// Load a replay hook from a recorded artifact directory using
    /// [`ReplayConfig::default`].
    ///
    /// Used by the env-driven runtime hook (`DEJA_MODE=replay`) where no
    /// custom config is wired through. Construct with [`Self::with_config`]
    /// when callers need to override the config.
    pub fn from_artifact_dir(artifact_dir: &Path) -> std::io::Result<Self> {
        Self::with_config(artifact_dir, ReplayConfig::default())
    }

    /// Load a replay hook from a recorded artifact directory with an explicit
    /// [`ReplayConfig`].
    pub fn with_config(artifact_dir: &Path, config: ReplayConfig) -> std::io::Result<Self> {
        let events = read_events(artifact_dir)?;
        let max_seq = events.iter().map(|e| e.global_sequence).max().unwrap_or(0);
        Ok(Self::new(events, config, max_seq + 1))
    }

    /// Create a replay hook from an in-memory event list (useful for tests).
    pub fn new(events: Vec<BoundaryEvent>, config: ReplayConfig, starting_global_seq: u64) -> Self {
        Self {
            config,
            all_events: events,
            cursors: Mutex::new(BTreeMap::new()),
            report: Mutex::new(ReplayReport::default()),
            global_seq: Mutex::new(starting_global_seq),
            callsite_occurrence: Mutex::new(HashMap::new()),
        }
    }

    /// Take the accumulated replay report.
    pub fn take_report(&self) -> ReplayReport {
        self.report
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    // -----------------------------------------------------------------
    // Internal matching
    // -----------------------------------------------------------------

    fn cursor_for(
        &self,
        correlation_id: &Option<String>,
    ) -> std::sync::MutexGuard<'_, BTreeMap<Option<String>, RequestCursor>> {
        let mut guard = self.cursors.lock().unwrap_or_else(|p| p.into_inner());
        if !guard.contains_key(correlation_id) {
            let mut events: Vec<BoundaryEvent> = self
                .all_events
                .iter()
                .filter(|e| correlation_matches(e, correlation_id.as_deref()))
                .cloned()
                .collect();
            events.sort_by_key(|e| e.request_sequence);
            guard.insert(
                correlation_id.clone(),
                RequestCursor {
                    position: 0,
                    events,
                },
            );
        }
        guard
    }

    fn method_matches(
        event: &BoundaryEvent,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
    ) -> bool {
        event.boundary == boundary
            && event.trait_name == trait_name
            && event.method_name == method_name
    }

    /// Core matching logic. Returns the matched event (if any) plus an outcome
    /// describing how the match was achieved.
    fn find_match(
        &self,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
        args: &serde_json::Value,
        correlation_id: &Option<String>,
    ) -> (Option<BoundaryEvent>, MatchOutcome) {
        let mut cursors = self.cursor_for(correlation_id);
        let cursor = cursors
            .get_mut(correlation_id)
            .expect("cursor_for inserts the entry before returning the guard");
        let pos = cursor.position;
        let events = &cursor.events;

        if pos >= events.len() {
            return (None, MatchOutcome::Novel);
        }

        // 1. Exact match at current position.
        if let Some(candidate) = events.get(pos) {
            if Self::method_matches(candidate, boundary, trait_name, method_name)
                && candidate.args == *args
            {
                cursor.position = pos + 1;
                return (Some(candidate.clone()), MatchOutcome::Exact);
            }
        }

        // 2. Sliding window: look for method + args match.
        //
        // First-pass: exact (method+args) match. If none is found, fall back
        // to method-only and consult the arg-mismatch policy. Splitting the
        // passes keeps a later same-method exact match from being shadowed
        // by an earlier mismatched candidate.
        let window_end = (pos + self.config.sliding_window_size).min(events.len());
        for (idx, candidate) in events.iter().enumerate().take(window_end).skip(pos) {
            if Self::method_matches(candidate, boundary, trait_name, method_name)
                && candidate.args == *args
            {
                cursor.position = idx + 1;
                return (
                    Some(candidate.clone()),
                    if idx == pos {
                        MatchOutcome::Exact
                    } else {
                        MatchOutcome::RecoveredSkip(idx - pos)
                    },
                );
            }
        }

        // Second-pass: method-only. Either return the recorded result with a
        // mismatch divergence, or report `ArgSkipBlocked` WITHOUT advancing the
        // cursor so the call falls through to the real implementation.
        for (idx, candidate) in events.iter().enumerate().take(window_end).skip(pos) {
            if Self::method_matches(candidate, boundary, trait_name, method_name) {
                if allow_arg_mismatch(self.config.arg_mismatch_policy, args) {
                    cursor.position = idx + 1;
                    return (
                        Some(candidate.clone()),
                        MatchOutcome::RecoveredWithMismatch(idx - pos),
                    );
                } else {
                    // The arg-mismatch guard blocks the fallback. Surface the
                    // recorded args as the baseline so the divergence is
                    // actionable. Do NOT
                    // advance the cursor — the recorded event is still on
                    // deck for a future (correctly-argued) call.
                    let recorded_args = candidate.args.clone();
                    return (None, MatchOutcome::ArgSkipBlocked(recorded_args));
                }
            }
        }

        // 3. Nothing found in window — novel.
        (None, MatchOutcome::Novel)
    }

    fn push_divergence(&self, div: Divergence) {
        let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
        report.push(div);
    }

    /// Stable-identity lookup: scan the per-correlation event tape for
    /// the first event whose `callsite_identity` matches `(source, id,
    /// occurrence)`. On match, validate args under the policy and advance
    /// the cursor.
    ///
    /// Returns `Some(result_json)` when the recorded event is appropriate to
    /// hand back. Returns `None` when no identity match is found, when the
    /// arg-mismatch policy blocks the fallback (an `ArgSkipBlocked`
    /// divergence is recorded), or when no `id` is present on the identity.
    fn lookup_by_identity(
        &self,
        identity: &CallsiteIdentity,
        args: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        let id = identity.id.as_deref()?;
        let correlation_id = deja_context::current_correlation_id();
        let corr = correlation_id.clone();

        // Phase 1: hold the cursors guard JUST long enough to locate the
        // event, clone it, and advance the cursor when fallback is allowed.
        // Release before touching the report mutex so the two locks are
        // never held simultaneously.
        let outcome = {
            let mut cursors = self.cursor_for(&corr);
            let cursor = cursors
                .get_mut(&corr)
                .expect("cursor_for inserts the entry before returning the guard");
            let pos = cursor.position;

            // Scan from the current cursor forward (not just the window) — a
            // stable id must not be silently lost behind unrelated calls.
            let mut found_idx: Option<usize> = None;
            for idx in pos..cursor.events.len() {
                let ev = &cursor.events[idx];
                let Some(ev_identity) = ev.callsite_identity.as_ref() else {
                    continue;
                };
                if ev_identity.source == identity.source
                    && ev_identity.id.as_deref() == Some(id)
                    && ev_identity.occurrence == identity.occurrence
                {
                    found_idx = Some(idx);
                    break;
                }
            }

            let idx = found_idx?;
            let candidate = cursor.events[idx].clone();

            if candidate.args == *args {
                cursor.position = idx + 1;
                IdentityOutcome::Exact(candidate)
            } else if allow_arg_mismatch(self.config.arg_mismatch_policy, args) {
                cursor.position = idx + 1;
                IdentityOutcome::Mismatch(candidate)
            } else {
                IdentityOutcome::Blocked(candidate)
            }
        };

        match outcome {
            IdentityOutcome::Exact(candidate) => {
                let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
                report.matched_calls += 1;
                Some(candidate.result)
            }
            IdentityOutcome::Mismatch(candidate) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::ArgsDiverged,
                    boundary: candidate.boundary.clone(),
                    trait_name: candidate.trait_name.clone(),
                    method_name: candidate.method_name.clone(),
                    detail: "args differed; returned identity-matched recorded result anyway"
                        .to_string(),
                    baseline: Some(candidate.args.clone()),
                    candidate: Some(args.clone()),
                    global_sequence: candidate.global_sequence,
                });
                let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
                report.matched_calls += 1;
                Some(candidate.result)
            }
            IdentityOutcome::Blocked(candidate) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::ArgSkipBlocked,
                    boundary: candidate.boundary.clone(),
                    trait_name: candidate.trait_name.clone(),
                    method_name: candidate.method_name.clone(),
                    detail: "arg mismatch fallback blocked".to_string(),
                    baseline: Some(candidate.args.clone()),
                    candidate: Some(args.clone()),
                    global_sequence: candidate.global_sequence,
                });
                None
            }
        }
    }
}

/// Outcome of a stable-identity lookup, used to keep the
/// cursors lock and the report lock acquisitions disjoint.
enum IdentityOutcome {
    Exact(BoundaryEvent),
    Mismatch(BoundaryEvent),
    Blocked(BoundaryEvent),
}

impl DejaHook for ReplayHook {
    fn mode(&self) -> RuntimeMode {
        RuntimeMode::Replay
    }

    fn record(&self, _event: BoundaryEvent) {
        let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
        report.total_calls += 1;
        // During replay, total_calls tracks how many calls the V2 made.
    }

    fn next_global_sequence(&self) -> u64 {
        let mut seq = self.global_seq.lock().unwrap_or_else(|p| p.into_inner());
        let current = *seq;
        *seq += 1;
        current
    }

    fn next_request_sequence(&self, correlation_id: Option<&str>) -> u64 {
        let cursors = self.cursor_for(&correlation_id.map(String::from));
        let cursor = cursors
            .get(&correlation_id.map(String::from))
            .expect("cursor_for inserts the entry before returning the guard");
        cursor.position as u64
    }

    fn try_replay(
        &self,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
        args: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        let correlation_id = deja_context::current_correlation_id();
        let corr = correlation_id.clone();

        let (maybe_event, outcome) =
            self.find_match(boundary, trait_name, method_name, args, &corr);

        match (maybe_event, outcome) {
            (Some(event), MatchOutcome::Exact) => {
                let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
                report.matched_calls += 1;
                Some(event.result)
            }
            (Some(event), MatchOutcome::RecoveredSkip(skipped)) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::OmittedCall,
                    boundary: boundary.to_string(),
                    trait_name: trait_name.to_string(),
                    method_name: method_name.to_string(),
                    detail: format!("skipped {} recorded call(s) to recover", skipped),
                    baseline: None,
                    candidate: None,
                    global_sequence: event.global_sequence,
                });
                let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
                report.matched_calls += 1;
                Some(event.result)
            }
            (Some(event), MatchOutcome::RecoveredWithMismatch(skipped)) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::ArgsDiverged,
                    boundary: boundary.to_string(),
                    trait_name: trait_name.to_string(),
                    method_name: method_name.to_string(),
                    detail: format!(
                        "args differed; skipped {} call(s) and returned recorded result anyway",
                        skipped
                    ),
                    baseline: Some(event.args.clone()),
                    candidate: Some(args.clone()),
                    global_sequence: event.global_sequence,
                });
                let mut report = self.report.lock().unwrap_or_else(|p| p.into_inner());
                report.matched_calls += 1;
                Some(event.result)
            }
            (None, MatchOutcome::Novel) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::NovelCall,
                    boundary: boundary.to_string(),
                    trait_name: trait_name.to_string(),
                    method_name: method_name.to_string(),
                    detail: "call not found in recording — falling through to real implementation"
                        .to_string(),
                    baseline: None,
                    candidate: Some(args.clone()),
                    global_sequence: 0,
                });
                None
            }
            (None, MatchOutcome::ArgSkipBlocked(recorded_args)) => {
                self.push_divergence(Divergence {
                    kind: DivergenceKind::ArgSkipBlocked,
                    boundary: boundary.to_string(),
                    trait_name: trait_name.to_string(),
                    method_name: method_name.to_string(),
                    detail: "arg mismatch fallback blocked".to_string(),
                    baseline: Some(recorded_args),
                    candidate: Some(args.clone()),
                    global_sequence: 0,
                });
                None
            }
            _ => None,
        }
    }

    fn try_replay_with_context(&self, query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        // Identity-first cascade for this legacy in-process hook. (Its stages
        // are independent of the 6-rank `Address` ladder used by lookup-table
        // replay.) A stable callsite-identity match is tried first; the
        // positional strategies in `try_replay` (location-exact /
        // sequence-method-args / sliding-window) are the fallback.

        // Stage 1: stable identity — callsite.id-based, requires that the identity
        // was derived from an annotation, a syntactic hash, or a lexical
        // path (i.e. genuinely stable across line shifts).
        if let Some(identity) = query.callsite_identity {
            let stable_source = matches!(
                identity.source,
                CallsiteSource::Explicit
                    | CallsiteSource::SyntacticHash
                    | CallsiteSource::LexicalPath
            );
            if stable_source && identity.id.is_some() {
                if let Some(result) = self.lookup_by_identity(identity, query.args) {
                    return Some(result);
                }
            }
        }

        // Delegate Ranks 3/5/6 to the existing cursor-based matcher.
        self.try_replay(
            query.boundary,
            query.trait_name,
            query.method_name,
            query.args,
        )
    }

    fn next_callsite_occurrence(
        &self,
        correlation_id: Option<&str>,
        source: CallsiteSource,
        scope: Option<&str>,
    ) -> u32 {
        let crate::TaskMetadata {
            task_bucket,
            bucket_id,
            ..
        } = crate::current_task_metadata(correlation_id);
        let bucket_id = bucket_id
            .or(task_bucket)
            .or_else(|| Some(crate::ROOT_TASK_ID.to_string()));
        let key = (
            correlation_id.map(String::from),
            bucket_id,
            source,
            scope.map(String::from),
        );
        let mut guard = self
            .callsite_occurrence
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let entry = guard.entry(key).or_insert(0);
        let value = *entry;
        *entry += 1;
        value
    }
}

// ---------------------------------------------------------------------------
// Lookup-table replay (pre-rendered table + observed-call capture)
// ---------------------------------------------------------------------------
//
// The orchestrator pre-renders a `LookupTable` by walking the recording. The
// candidate carries a thin `LookupTableHook` that does O(1) key→result lookups.
// No cascade logic, no `ArgMismatchPolicy`, no `DivergenceKind` classification
// lives in the candidate. Each call emits a `ObservedCall` to the configured
// `ObservedCallSink`; the orchestrator runs post-hoc divergence detection
// against the recording.
//
// Trait surface is dependency-inversion: deja-record ships local-file
// implementations of both source and sink. HTTP/Kafka variants are supplied
// by the application (same pattern as the JSONL → KafkaSink split for
// recording).

/// A frozen lookup table produced by the orchestrator and consumed by the
/// candidate's `LookupTableHook`. Serialized as a single JSON document or
/// JSONL stream (one `LookupEntry` per line).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupTable {
    pub recording_id: String,
    pub policy_version: u32,
    pub entries: Vec<LookupEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LookupEntry {
    pub key: LookupKey,
    pub result: serde_json::Value,
    pub source_event_global_sequence: u64,
}

/// How a call site is addressed for replay matching, strongest (most stable)
/// rank first. The renderer emits one `LookupEntry` per applicable rank; the
/// hook queries the ranks it can construct strongest-first and takes the first
/// hit. The decisive property is **iteration-order independence**: ranks 1–5
/// identify a call by *what it is* (annotation, logical span-path, syntax,
/// lexical position, source location) rather than *when it ran*, so a loop that
/// visits its items in a different order than the recording still resolves —
/// each iteration self-addresses by its args (see [`LookupKey::args_hash`]).
/// Rank 6 is the positional last resort; a run that leans on it is fragile,
/// which the divergence detector surfaces via per-rank counts.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum Address {
    /// Rank 1 — user-supplied explicit annotation (`CallsiteSource::Explicit`).
    Explicit(String),
    /// Rank 2 — logical span-path: the root→leaf chain of `tracing` span NAMES
    /// the call fired within (from [`crate::current_span_path`]). The
    /// most version-independent address: it survives source-line shifts and
    /// benign signature edits, and — crucially — is DISTINCT for concurrent
    /// same-callsite calls in different spans, so the per-key `occurrence` is
    /// scoped to the span and cannot swap under async task interleaving. No
    /// embedded occurrence: the path IS the disambiguator, and genuine same-path
    /// repeats are tiebroken by [`LookupKey::occurrence`] (sequential, stable).
    SpanPath { path: String },
    /// Rank 3 — hash of the surrounding syntax tokens (`boundary::operation`).
    SyntacticHash(u64),
    /// Rank 4 — stable lexical path plus its per-scope occurrence index.
    LexicalPath { path: String, scope_occurrence: u32 },
    /// Rank 5 — `#[track_caller]` source location.
    SourceLocation {
        file: String,
        line: u32,
        column: u32,
    },
    /// Rank 6 — positional last resort: boundary + method + per-correlation
    /// request sequence. Fragile to any upstream edit that shifts positions.
    Sequence {
        boundary: String,
        method: String,
        request_sequence: u64,
    },
}

impl Address {
    /// Stability rank: 1 (strongest) … 6 (weakest). Used by the hook to query
    /// strongest-first and by the divergence detector to score fragility.
    pub fn rank(&self) -> u8 {
        match self {
            Address::Explicit(_) => 1,
            Address::SpanPath { .. } => 2,
            Address::SyntacticHash(_) => 3,
            Address::LexicalPath { .. } => 4,
            Address::SourceLocation { .. } => 5,
            Address::Sequence { .. } => 6,
        }
    }
}

/// Composite key the orchestrator uses to register an entry and the candidate
/// uses to look one up. A call is identified by its `address` (rank-specific),
/// lineage bucket, the canonical hash of its arguments, and a tiebreaking
/// `occurrence` index scoped to `(correlation_id, bucket_id, address, args_hash)`
/// — so two argless-impure calls to the same site (e.g. `time::now`) only
/// collide when the candidate makes the same call with the same args the same
/// number of times inside the same lineage bucket.
#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct LookupKey {
    pub correlation_id: Option<String>,
    /// Canonical lineage bucket. Additive/default so old lookup tables without
    /// bucket-scoped keys still deserialize.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_id: Option<String>,
    /// Monotonic fork sequence for the task lineage that made this call.
    #[serde(default)]
    pub fork_seq: u64,
    /// Rank-specific call-site address (see [`Address`]).
    pub address: Address,
    /// Canonical, order-independent hash of the call's serialized args.
    pub args_hash: u64,
    /// Nth call to `(correlation_id, bucket_id, address, args_hash)`; 0 for a unique call.
    pub occurrence: u32,
}

/// A call the candidate actually made, with the lookup outcome. Streamed to
/// an `ObservedCallSink` end-of-request; the orchestrator's post-hoc
/// divergence detector compares the observed stream against the recording.
///
/// `boundary`/`trait_name`/`method_name` are carried explicitly (rather than
/// being read off the resolved key) because ranks 1–5 don't encode the
/// boundary — yet the detector must attribute every call, hit or miss, to a
/// boundary. `resolved_rank` records which [`Address`] rank won, so the
/// detector can report how much of a run leans on fragile rank-6 matches.
fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(from = "ObservedCallWire")]
pub struct ObservedCall {
    pub correlation_id: Option<String>,
    pub boundary: String,
    pub trait_name: String,
    pub method_name: String,
    pub args: serde_json::Value,
    pub resolved: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_rank: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_event_global_sequence: Option<u64>,
    /// Replay-side wall-clock start timestamp for this observed call. This is
    /// stamped by the candidate, not copied from the recording, so the scorer can
    /// identify work that fires after the router response finalizer.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub timestamp_ns: u64,
    /// Replay-side wall-clock completion timestamp, when the observed row is a
    /// finalizer marker (`http_incoming`) or another boundary can provide it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_timestamp_ns: Option<u64>,
    /// Stable replay task id stamped by the runtime kernel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_id: Option<String>,
    /// Parent replay task id for calls made inside spawned detached work.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_task_id: Option<String>,
    /// Correlation/task bucket for future lineage grouping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task_bucket: Option<String>,
    /// Canonical lineage bucket. New consumers should read this first and fall
    /// back to `task_bucket` for old tapes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket_id: Option<String>,
    /// Monotonic fork sequence for the task lineage that made this call.
    #[serde(default)]
    pub fork_seq: u64,
    /// Where the candidate made this call, captured at replay time so a
    /// divergence (especially a NOVEL call with no recorded counterpart) can be
    /// deep-linked to a callsite + placed on the replay execution graph. All
    /// `#[serde(default)]` so pre-enrichment artifacts still parse.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_line: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub call_column: Option<u32>,
    /// Root→leaf tracing span-name chain the call fired within — the same
    /// span-path address used for lookup (rank 2), so a UI can align this
    /// call to its node in BOTH the record and replay execution-graph trees.
    /// Wire name pinned to `logical_context`.
    #[serde(rename = "logical_context")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span_path: Option<String>,
    /// Replay-side execution-graph node id the call fired under (joins to
    /// `ExecutionGraphNode.node_id` in the replay graph) — lets a novel call
    /// self-place on the replay tree even though it has no recorded event.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph_node_id: Option<u64>,
    /// V2 scaffold (Tier 2): set when the hook synthesized a safe default on a
    /// miss. Always false in V1 (full mock) — the hook never synthesizes yet.
    #[serde(default)]
    pub synthesized: bool,
    /// V2 scaffold (Tier 3): set when a miss falls through to a real impl that
    /// is expected to fail in the harness environment (egress blocked). Always
    /// false in V1.
    #[serde(default)]
    pub real_impl_will_fail: bool,
    /// The result the recording carried for this call-site (substituted under
    /// lookup mode). Under lookup this equals `observed_result`, so a value diff
    /// is inert; under execute mode it is the recorded baseline to compare the
    /// real boundary's fresh result against.
    #[serde(default)]
    pub recorded_result: Option<serde_json::Value>,
    /// The result actually produced for this call. Under lookup this is the
    /// substituted (== recorded) value; under execute mode it is the REAL
    /// boundary's fresh result. The post-hoc tally classifies
    /// [`ValueDiverged`](crate::DivergenceKind::ValueDiverged) when these differ.
    #[serde(default)]
    pub observed_result: Option<serde_json::Value>,
    /// How this observed call was served: ordinary recorded substitution, or an
    /// execute-shadow dispatch that ran the real boundary.
    #[serde(default)]
    pub provenance: crate::Provenance,
    /// Set when the call could not be conclusively classified because the
    /// recorded baseline needed to compare against was missing (a seed gap) —
    /// surfaced as [`InconclusiveSeedGap`](crate::DivergenceKind::InconclusiveSeedGap)
    /// rather than a false positive. Always false in M1 lookup mode.
    #[serde(default)]
    pub seed_gap: bool,
}

#[derive(Deserialize)]
struct ObservedCallWire {
    correlation_id: Option<String>,
    boundary: String,
    trait_name: String,
    method_name: String,
    args: serde_json::Value,
    resolved: bool,
    #[serde(default)]
    resolved_rank: Option<u8>,
    #[serde(default)]
    source_event_global_sequence: Option<u64>,
    #[serde(default)]
    timestamp_ns: u64,
    #[serde(default)]
    end_timestamp_ns: Option<u64>,
    #[serde(default)]
    task_id: Option<String>,
    #[serde(default)]
    parent_task_id: Option<String>,
    #[serde(default)]
    task_bucket: Option<String>,
    #[serde(default)]
    bucket_id: Option<String>,
    #[serde(default)]
    fork_seq: u64,
    #[serde(default)]
    call_file: Option<String>,
    #[serde(default)]
    call_line: Option<u32>,
    #[serde(default)]
    call_column: Option<u32>,
    #[serde(default, rename = "logical_context")]
    span_path: Option<String>,
    #[serde(default)]
    graph_node_id: Option<u64>,
    #[serde(default)]
    synthesized: bool,
    #[serde(default)]
    real_impl_will_fail: bool,
    #[serde(default)]
    recorded_result: Option<serde_json::Value>,
    #[serde(default)]
    observed_result: Option<serde_json::Value>,
    #[serde(default)]
    provenance: crate::Provenance,
    #[serde(default)]
    seed_gap: bool,
}

impl From<ObservedCallWire> for ObservedCall {
    fn from(wire: ObservedCallWire) -> Self {
        let bucket_id = wire.bucket_id.or_else(|| wire.task_bucket.clone());
        let task_bucket = wire.task_bucket.or_else(|| bucket_id.clone());
        Self {
            correlation_id: wire.correlation_id,
            boundary: wire.boundary,
            trait_name: wire.trait_name,
            method_name: wire.method_name,
            args: wire.args,
            resolved: wire.resolved,
            resolved_rank: wire.resolved_rank,
            source_event_global_sequence: wire.source_event_global_sequence,
            timestamp_ns: wire.timestamp_ns,
            end_timestamp_ns: wire.end_timestamp_ns,
            task_id: wire.task_id,
            parent_task_id: wire.parent_task_id,
            task_bucket,
            bucket_id,
            fork_seq: wire.fork_seq,
            call_file: wire.call_file,
            call_line: wire.call_line,
            call_column: wire.call_column,
            span_path: wire.span_path,
            graph_node_id: wire.graph_node_id,
            synthesized: wire.synthesized,
            real_impl_will_fail: wire.real_impl_will_fail,
            recorded_result: wire.recorded_result,
            observed_result: wire.observed_result,
            provenance: wire.provenance,
            seed_gap: wire.seed_gap,
        }
    }
}

// ---------------------------------------------------------------------------
// Shared key construction (used by BOTH the renderer and the hook)
//
// The renderer lives in `deja-orchestrator` and the hook is compiled into the
// candidate router — two separate binaries. If they constructed keys even
// slightly differently (args canonicalization, rank selection, occurrence
// numbering) every lookup would silently miss. So the canonical logic lives
// here, in `deja-record`, and both sides call it.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// The decision: the per-site `replay_strategy` knob (design #28)
//
// The runtime routing decision is now: in replay mode, read the declared
// [`ReplayStrategy`] off the [`BoundarySpec`]. `Execute` runs the real boundary;
// `Substitute` serves the recorded result (and fails closed on a miss).
// Outside replay, every site is Lookup because there is no recorded baseline to
// shadow-compare against.
// ---------------------------------------------------------------------------

/// Map a per-site [`ReplayStrategy`] knob to the runtime [`ExecuteMode`].
pub fn replay_strategy_to_execute_mode(strategy: ReplayStrategy) -> ExecuteMode {
    match strategy {
        ReplayStrategy::Execute => ExecuteMode::Execute,
        ReplayStrategy::Substitute => ExecuteMode::Lookup,
    }
}

/// Runtime entry point for the boundary-macro execute-mode decision under a
/// concrete hook.
pub fn boundary_execute_mode_for(hook: &dyn DejaHook, spec: &BoundarySpec) -> ExecuteMode {
    if !hook.mode().is_replay() {
        return ExecuteMode::Lookup;
    }

    replay_strategy_to_execute_mode(spec.semantics().replay_strategy)
}

/// Stable, order-independent hash of a call's serialized args.
///
/// Object keys are sorted recursively, so `{"a":1,"b":2}` and `{"b":2,"a":1}`
/// hash identically. Type-tag bytes (`n`/`t`/`f`/`#`/`s`/`[`/`{`) disambiguate
/// e.g. the string `"1"` from the number `1` and an empty array from an empty
/// object. Built on the crate's FNV-1a basis so the value is identical across
/// binaries on the same target — no random seed, no platform dependence.
pub fn canonical_args_hash(args: &serde_json::Value) -> u64 {
    hash_value(crate::FNV_OFFSET_BASIS, args)
}

fn hash_value(hash: u64, value: &serde_json::Value) -> u64 {
    use serde_json::Value;
    match value {
        Value::Null => crate::fnv1a_bytes(hash, b"n"),
        Value::Bool(true) => crate::fnv1a_bytes(hash, b"t"),
        Value::Bool(false) => crate::fnv1a_bytes(hash, b"f"),
        Value::Number(n) => crate::fnv1a_str(crate::fnv1a_bytes(hash, b"#"), &n.to_string()),
        Value::String(s) => crate::fnv1a_str(crate::fnv1a_bytes(hash, b"s"), s),
        Value::Array(items) => {
            let mut h = crate::fnv1a_bytes(hash, b"[");
            for item in items {
                h = hash_value(h, item);
            }
            crate::fnv1a_bytes(h, b"]")
        }
        Value::Object(map) => {
            // Sort keys for canonical order regardless of serde_json's map impl
            // (BTreeMap by default, IndexMap under `preserve_order`).
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut h = crate::fnv1a_bytes(hash, b"{");
            for key in keys {
                h = crate::fnv1a_str(h, key);
                if let Some(v) = map.get(key) {
                    h = hash_value(h, v);
                }
            }
            crate::fnv1a_bytes(h, b"}")
        }
    }
}

/// Build the rank-ordered list of addresses a call site supports, strongest
/// first. Emits only the ranks for which identifying material exists: ranks
/// 1–4 require the corresponding `CallsiteIdentity` fields, rank 5 requires a
/// caller location, and rank 6 (sequence) is always present as the last
/// resort. The renderer feeds this from a recorded `BoundaryEvent`; the hook
/// feeds it from a live `ReplayLookup`. Identical inputs → identical output.
pub fn addresses_for(
    boundary: &str,
    method_name: &str,
    identity: Option<&crate::CallsiteIdentity>,
    location: Option<(&str, u32, u32)>,
    request_sequence: u64,
) -> Vec<Address> {
    let mut out = Vec::with_capacity(6);
    if let Some(id) = identity {
        if matches!(id.source, crate::CallsiteSource::Explicit) {
            if let Some(tag) = &id.id {
                out.push(Address::Explicit(tag.clone()));
            }
        }
        // Rank 2 — logical span-path. Strongest non-explicit address: stable
        // across line/signature edits AND distinct per concurrent span, so the
        // occurrence tiebreak is span-scoped (no positional swap).
        if let Some(path) = &id.span_path {
            out.push(Address::SpanPath { path: path.clone() });
        }
        if let Some(hash) = id.syntax_hash {
            out.push(Address::SyntacticHash(hash));
        }
        if let Some(path) = &id.lexical_path {
            out.push(Address::LexicalPath {
                path: path.clone(),
                scope_occurrence: id.occurrence,
            });
        }
    }
    if let Some((file, line, column)) = location {
        out.push(Address::SourceLocation {
            file: file.to_owned(),
            line,
            column,
        });
    }
    out.push(Address::Sequence {
        boundary: boundary.to_owned(),
        method: method_name.to_owned(),
        request_sequence,
    });
    out
}

/// Assigns the tiebreaking `occurrence` index to each address, turning a call
/// site's rank-ordered addresses into fully-qualified [`LookupKey`]s.
///
/// MUST be advanced on every call/event — for **all** ranks, not just the one
/// that resolves — so the renderer and hook keep identical occurrence
/// numbering even when a stronger rank is absent from some events.
#[derive(Default)]
pub struct KeyStamper {
    occurrences: std::collections::HashMap<(Option<String>, Option<String>, Address, u64), u32>,
}

impl KeyStamper {
    pub fn new() -> Self {
        Self::default()
    }

    /// Stamp occurrence indices onto each address, returning rank-ordered keys.
    pub fn stamp(
        &mut self,
        correlation_id: Option<&str>,
        bucket_id: Option<&str>,
        fork_seq: u64,
        addresses: &[Address],
        args_hash: u64,
    ) -> Vec<LookupKey> {
        let correlation_id = correlation_id.map(str::to_owned);
        let bucket_id = bucket_id.map(str::to_owned);
        addresses
            .iter()
            .map(|address| {
                let bucket = (
                    correlation_id.clone(),
                    bucket_id.clone(),
                    address.clone(),
                    args_hash,
                );
                let counter = self.occurrences.entry(bucket).or_insert(0);
                let occurrence = *counter;
                *counter += 1;
                LookupKey {
                    correlation_id: correlation_id.clone(),
                    bucket_id: bucket_id.clone(),
                    fork_seq,
                    address: address.clone(),
                    args_hash,
                    occurrence,
                }
            })
            .collect()
    }
}

/// Loader for a `LookupTable`. Called ONCE at candidate boot.
pub trait LookupTableSource: Send {
    fn load(&mut self) -> std::io::Result<LookupTable>;
}

/// Sink for `ObservedCall` records emitted by the candidate hook.
///
/// Implementations must be cheap on the hot path: `observed` runs inside
/// every `#[deja::*]` call. Batching, flushing, or sending across the
/// network should be done in `flush` (called at request scope exit) — not
/// inline.
pub trait ObservedCallSink: Send + Sync {
    fn observed(&self, call: ObservedCall);
    /// Execution-graph node captured during replay; rides the observed
    /// stream. Default no-op: sinks that persist the stream must override,
    /// assertion-only test sinks may ignore graph traffic.
    fn graph_node(&self, _node: deja_core::ExecutionGraphNode) {}
    fn flush(&self) -> std::io::Result<()>;
}

/// Local-file `LookupTableSource`. Reads either a single JSON document or
/// a JSONL stream of `LookupEntry` records (auto-detected by the first
/// non-whitespace character).
pub struct LocalFileLookupSource {
    path: std::path::PathBuf,
}

impl LocalFileLookupSource {
    pub fn new(path: impl Into<std::path::PathBuf>) -> Self {
        Self { path: path.into() }
    }
}

impl LookupTableSource for LocalFileLookupSource {
    fn load(&mut self) -> std::io::Result<LookupTable> {
        let bytes = std::fs::read(&self.path)?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Try the whole-document LookupTable form first; fall back to JSONL
        // (one LookupEntry per line) if that fails. Robust against either
        // shape without needing a magic byte or extension.
        if let Ok(table) = serde_json::from_str::<LookupTable>(text) {
            return Ok(table);
        }
        let entries = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(serde_json::from_str::<LookupEntry>)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Ok(LookupTable {
            recording_id: String::new(),
            policy_version: 1,
            entries,
        })
    }
}

/// In-memory `ObservedCallSink` for tests and standalone harness use.
pub struct InMemoryObservedSink {
    calls: std::sync::Arc<Mutex<Vec<ObservedCall>>>,
    graph_nodes: std::sync::Arc<Mutex<Vec<deja_core::ExecutionGraphNode>>>,
}

impl Default for InMemoryObservedSink {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryObservedSink {
    pub fn new() -> Self {
        Self {
            calls: std::sync::Arc::new(Mutex::new(Vec::new())),
            graph_nodes: std::sync::Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Graph nodes captured so far; useful for assertions in tests.
    pub fn graph_nodes(&self) -> Vec<deja_core::ExecutionGraphNode> {
        self.graph_nodes
            .lock()
            .map(|buf| buf.clone())
            .unwrap_or_default()
    }

    /// Clone of the underlying buffer; useful for assertions in tests.
    pub fn handle(&self) -> std::sync::Arc<Mutex<Vec<ObservedCall>>> {
        std::sync::Arc::clone(&self.calls)
    }

    pub fn drain(&self) -> Vec<ObservedCall> {
        self.calls
            .lock()
            .map(|mut buf| std::mem::take(&mut *buf))
            .unwrap_or_default()
    }
}

impl ObservedCallSink for InMemoryObservedSink {
    fn observed(&self, call: ObservedCall) {
        if let Ok(mut buf) = self.calls.lock() {
            buf.push(call);
        }
    }
    fn graph_node(&self, node: deja_core::ExecutionGraphNode) {
        if let Ok(mut buf) = self.graph_nodes.lock() {
            buf.push(node);
        }
    }
    fn flush(&self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Append-only JSONL `ObservedCallSink`. One line per call.
pub struct FileObservedSink {
    file: Mutex<std::fs::File>,
}

impl FileObservedSink {
    pub fn create(path: impl AsRef<Path>) -> std::io::Result<Self> {
        // Create the parent dir so a missing observed/ doesn't fail replay boot
        // (which would silently fall back to the legacy ReplayHook).
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(Self {
            file: Mutex::new(file),
        })
    }
}

impl FileObservedSink {
    fn write_record(&self, record: &crate::DejaRecord) {
        use std::io::Write;
        if let Ok(mut guard) = self.file.lock() {
            if let Ok(line) = serde_json::to_string(record) {
                let _ = guard.write_all(line.as_bytes());
                let _ = guard.write_all(b"\n");
            }
        }
    }
}

impl ObservedCallSink for FileObservedSink {
    fn observed(&self, call: ObservedCall) {
        self.write_record(&crate::DejaRecord::Observed(Box::new(call)));
    }
    fn graph_node(&self, node: deja_core::ExecutionGraphNode) {
        self.write_record(&crate::DejaRecord::GraphNode(node));
    }
    fn flush(&self) -> std::io::Result<()> {
        use std::io::Write;
        if let Ok(mut guard) = self.file.lock() {
            guard.flush()?;
        }
        Ok(())
    }
}

/// In-process side-effect player driven by a frozen `LookupTable`.
///
/// Does NOT run a cascade and does NOT classify divergences. It looks up a key
/// by (correlation, boundary, trait, method, occurrence) — with optional
/// `callsite_identity_id` fallback — emits an `ObservedCall`, and returns the
/// result if found.
pub struct LookupTableHook {
    table: HashMap<LookupKey, LookupEntry>,
    /// Per-correlation request_sequence counter; bumps on each lookup. Feeds
    /// the rank-6 `Address::Sequence` and mirrors the recorder's own
    /// per-correlation sequence (both start at 0 and step by one per call).
    next_sequence: Mutex<HashMap<Option<String>, u64>>,
    /// Shared occurrence assigner; advanced for every rank on every call so its
    /// numbering stays in lockstep with the renderer's.
    stamper: Mutex<KeyStamper>,
    /// Per-correlation global-event counter; sourced from `next_global_sequence`.
    global_counter: std::sync::atomic::AtomicU64,
    /// Per-(correlation, bucket, source, scope) occurrence counter mirroring
    /// `RecordingHook::next_callsite_occurrence`. The boundary macro re-derives
    /// the per-callsite occurrence at REPLAY time by calling
    /// `next_callsite_occurrence` on this hook (the same hook that does the
    /// lookup). It MUST advance in lock-step with recording — one bump per call
    /// per scope and lineage bucket — so that the `CallsiteIdentity::occurrence`
    /// the macro stamps into the rank-4 `Address::LexicalPath { scope_occurrence }` matches the
    /// occurrence the renderer read off the recorded event. Without this the
    /// macro would receive the default `0` for every call and only the first
    /// (occurrence-0) call at each callsite would resolve.
    callsite_occurrence: Mutex<crate::CallsiteOccurrenceMap>,
    observed_sink: Box<dyn ObservedCallSink>,
    /// Sequence space for replay-side graph nodes on the observed stream;
    /// separate from the lookup counters so replay addressing stays in
    /// lockstep with the recorder whether or not graph capture is on.
    graph_counter: std::sync::atomic::AtomicU64,
}

impl LookupTableHook {
    /// Construct from any `LookupTableSource` (typically `LocalFileLookupSource`)
    /// and any `ObservedCallSink` (typically `InMemoryObservedSink` for tests
    /// or `FileObservedSink` for harness runs). Loading happens once at
    /// construction; failures bubble up as `io::Error`.
    pub fn from_source<S, K>(mut source: S, sink: K) -> std::io::Result<Self>
    where
        S: LookupTableSource,
        K: ObservedCallSink + 'static,
    {
        let table = source.load()?;
        let mut map = HashMap::with_capacity(table.entries.len());
        for entry in table.entries {
            map.insert(entry.key.clone(), entry);
        }
        Ok(Self {
            table: map,
            next_sequence: Mutex::new(HashMap::new()),
            stamper: Mutex::new(KeyStamper::new()),
            global_counter: std::sync::atomic::AtomicU64::new(0),
            callsite_occurrence: Mutex::new(HashMap::new()),
            observed_sink: Box::new(sink),
            graph_counter: std::sync::atomic::AtomicU64::new(0),
        })
    }

    /// Number of entries loaded. Useful for assertions.
    pub fn entry_count(&self) -> usize {
        self.table.len()
    }

    /// Force-flush the underlying observed-call sink. The hook does NOT
    /// auto-flush on drop; orchestrators should call this at run end.
    pub fn flush(&self) -> std::io::Result<()> {
        self.observed_sink.flush()
    }

    /// Replay-side execution-graph capture: nodes ride the observed stream.
    pub fn record_graph_node(&self, mut node: deja_core::ExecutionGraphNode) {
        node.global_sequence = self
            .graph_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.observed_sink.graph_node(node);
    }

    fn bump_request_sequence(&self, correlation_id: Option<&str>) -> u64 {
        let key = correlation_id.map(str::to_owned);
        if let Ok(mut map) = self.next_sequence.lock() {
            let counter = map.entry(key).or_insert(0);
            let seq = *counter;
            *counter += 1;
            seq
        } else {
            0
        }
    }

    /// Resolve one replay call to its recorded baseline.
    ///
    /// SINGLE source of truth shared by [`Self::try_replay_with_context`]
    /// (lookup) and [`Self::execute_shadow_peek`] (execute): it bumps the
    /// per-correlation request sequence, stamps occurrences for EVERY rank, and
    /// queries the table strongest-first. Because BOTH modes route through here,
    /// the stamper / sequence counters advance EXACTLY ONCE per call regardless
    /// of mode, so numbering never drifts between a lookup boundary and an execute
    /// boundary in the same run. It does NOT emit an observation — the caller
    /// shapes and emits the `ObservedCall` (Recorded vs Shadow).
    fn resolve(&self, query: &ReplayLookup<'_>) -> Resolution {
        // The candidate carries no notion of "current correlation" in
        // ReplayLookup; pull it from the ambient deja-context scope set up
        // by the request middleware.
        let correlation_id = deja_context::current_correlation_id();
        let crate::TaskMetadata {
            task_id,
            parent_task_id,
            task_bucket,
            bucket_id,
            fork_seq,
        } = crate::current_task_metadata(correlation_id.as_deref());
        let bucket_id = bucket_id
            .or_else(|| task_bucket.clone())
            .or_else(|| Some(crate::ROOT_TASK_ID.to_string()));
        let fork_seq = fork_seq.unwrap_or(0);
        // Bumped once per call for the rank-6 positional address; mirrors the
        // recorder's per-correlation request_sequence.
        let request_sequence = self.bump_request_sequence(correlation_id.as_deref());
        let args_hash = canonical_args_hash(query.args);

        let location = query
            .caller_location
            .map(|loc| (loc.file(), loc.line(), loc.column()));
        let addresses = addresses_for(
            query.boundary,
            query.method_name,
            query.callsite_identity,
            location,
            request_sequence,
        );

        // Stamp occurrences for EVERY rank (not just the one that resolves) so
        // the numbering stays aligned with the renderer, then query
        // strongest-first and take the first hit.
        let keys = match self.stamper.lock() {
            Ok(mut stamper) => stamper.stamp(
                correlation_id.as_deref(),
                bucket_id.as_deref(),
                fork_seq,
                &addresses,
                args_hash,
            ),
            Err(_) => Vec::new(),
        };
        let mut hit: Option<(&LookupEntry, u8)> = None;
        for key in &keys {
            if let Some(entry) = self.table.get(key) {
                hit = Some((entry, key.address.rank()));
                break;
            }
        }

        // There is intentionally no arg-tolerant args-free fallback. Serving a
        // re-keyed call its recorded value was the partial-derivative substitution
        // that masked transitive effects (the eu-overcharge lie: "the function
        // behaves as if the arg is the original, not the changed one"). Under the
        // partial-function model a re-keyed call is an honest Lookup miss, and the
        // seam fail-stops on it instead of serving stale.

        // "Where" for the diff UI + graph placement. `location` is already
        // resolved above (rank-5 SourceLocation); the span path is the rank-2
        // logical address; the graph node is the replay-side execution-graph
        // node this call fired under.
        let (_, graph_node_id) = crate::current_execution_graph_context();
        Resolution {
            correlation_id,
            task_id,
            parent_task_id,
            task_bucket: task_bucket.or_else(|| bucket_id.clone()),
            bucket_id,
            fork_seq,
            location: location.map(|(f, l, c)| (f.to_owned(), l, c)),
            graph_node_id,
            resolved_rank: hit.map(|(_, rank)| rank),
            source_event_global_sequence: hit.map(|(entry, _)| entry.source_event_global_sequence),
            recorded_result: hit.map(|(entry, _)| entry.result.clone()),
        }
    }
}

/// Outcome of [`LookupTableHook::resolve`]: the recorded baseline for one call
/// plus the call-site metadata both modes carry into their `ObservedCall`.
struct Resolution {
    correlation_id: Option<String>,
    task_id: Option<String>,
    parent_task_id: Option<String>,
    task_bucket: Option<String>,
    bucket_id: Option<String>,
    fork_seq: u64,
    location: Option<(String, u32, u32)>,
    graph_node_id: Option<u64>,
    resolved_rank: Option<u8>,
    source_event_global_sequence: Option<u64>,
    recorded_result: Option<serde_json::Value>,
}

impl Resolution {
    /// The recorded baseline value, if the call resolved.
    fn recorded_result(&self) -> Option<serde_json::Value> {
        self.recorded_result.clone()
    }

    /// Shape an [`ObservedCall`] from this resolution, the originating query, the
    /// `observed_result` for this mode (the substituted recorded value under
    /// lookup, or `None`/the-real-result under execute), and the `provenance`.
    fn into_observed_call(
        self,
        query: &ReplayLookup<'_>,
        observed_result: Option<serde_json::Value>,
        provenance: crate::Provenance,
    ) -> ObservedCall {
        ObservedCall {
            correlation_id: self.correlation_id,
            boundary: query.boundary.to_owned(),
            trait_name: query.trait_name.to_owned(),
            method_name: query.method_name.to_owned(),
            args: query.args.clone(),
            resolved: self.recorded_result.is_some(),
            resolved_rank: self.resolved_rank,
            source_event_global_sequence: self.source_event_global_sequence,
            timestamp_ns: crate::now_ns(),
            end_timestamp_ns: None,
            task_id: self.task_id,
            parent_task_id: self.parent_task_id,
            task_bucket: self.task_bucket,
            bucket_id: self.bucket_id,
            fork_seq: self.fork_seq,
            call_file: self.location.as_ref().map(|(f, _, _)| f.clone()),
            call_line: self.location.as_ref().map(|(_, l, _)| *l),
            call_column: self.location.as_ref().map(|(_, _, c)| *c),
            span_path: crate::current_span_path(),
            graph_node_id: self.graph_node_id,
            // V1 full mock never synthesizes and never relies on the real impl;
            // these stay false until the V2 tiered-miss work lands.
            synthesized: false,
            real_impl_will_fail: false,
            recorded_result: self.recorded_result,
            observed_result,
            provenance,
            seed_gap: false,
        }
    }
}

impl crate::graph::GraphNodeSink for LookupTableHook {
    fn graph_node(&self, node: deja_core::ExecutionGraphNode) {
        self.record_graph_node(node);
    }
}

impl DejaHook for LookupTableHook {
    fn mode(&self) -> RuntimeMode {
        RuntimeMode::Replay
    }

    fn try_replay_with_context(&self, query: ReplayLookup<'_>) -> Option<serde_json::Value> {
        // Resolve the recorded baseline (advancing the stamper / sequence
        // counters EXACTLY ONCE, shared with the execute path so numbering never
        // drifts between lookup and execute boundaries), then emit a `Recorded`
        // observation: under lookup mode the observed result IS the substituted
        // recorded result, so the two sides are identical and ValueDiverged is
        // inert.
        let resolution = self.resolve(&query);
        let recorded = resolution.recorded_result();
        self.observed_sink.observed(resolution.into_observed_call(
            &query,
            // Lookup mode: observed == recorded (the substituted value).
            recorded.clone(),
            crate::Provenance::Recorded,
        ));
        recorded
    }

    fn flush(&self) -> std::io::Result<()> {
        LookupTableHook::flush(self)
    }

    fn execute_shadow_peek(&self, query: ReplayLookup<'_>) -> Option<crate::ExecuteShadowToken> {
        // First half of an execute-mode dispatch. Resolve the recorded baseline
        // through the SAME path the lookup uses (so the stamper / sequence /
        // occurrence counters advance identically — a run mixing lookup and
        // execute boundaries keeps aligned numbering), but do NOT substitute and
        // do NOT emit yet. Build the shadow observation with `observed_result =
        // None`; the macro fills it after the real boundary call and hands the
        // token back to `execute_shadow_observe`.
        let resolution = self.resolve(&query);
        let observed = resolution.into_observed_call(
            &query,
            // Filled in by `execute_shadow_observe` from the real result.
            None,
            crate::Provenance::Shadow,
        );
        // NO `seed_gap` flagging here. At this layer `recorded_result.is_none()`
        // means `resolve` found NO recorded event for this call AT ALL — i.e. the
        // candidate made a NOVEL call with no recorded counterpart (e.g. the
        // extra-call scenario: an extra db `find` the recording never had). That
        // MUST surface as a blocking NovelCall divergence, NOT be swallowed as a
        // non-blocking InconclusiveSeedGap (which masked the catch under #28's
        // unconditional-Execute knob). With `seed_gap = false` and `resolved =
        // false`, the post-hoc tally — finding no recorded twin to pair against —
        // classifies it as NovelCall, exactly as the #26 lookup-miss path did.
        // A recorded counterpart, when present, is resolved by the lookup table;
        // seed planning is a separate precondition-materialization pass and does
        // not decide whether this observation has a baseline.
        Some(crate::ExecuteShadowToken::new(observed))
    }

    fn execute_shadow_observe(
        &self,
        token: crate::ExecuteShadowToken,
        observed_result: serde_json::Value,
    ) {
        // Second half: stamp the real boundary's result onto the carried
        // observation and emit it. The post-hoc tally pairs this Shadow
        // observation against the recorded baseline by args-free identity +
        // occurrence and classifies ValueDiverged on a value diff.
        let observed = token.into_observed(observed_result);
        self.observed_sink.observed(observed);
    }

    fn try_replay(
        &self,
        boundary: &str,
        trait_name: &str,
        method_name: &str,
        args: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        // Delegate to try_replay_with_context with a stub query so legacy
        // call paths still get a lookup attempt.
        self.try_replay_with_context(ReplayLookup {
            boundary,
            trait_name,
            method_name,
            args,
            callsite_identity: None,
            caller_location: None,
        })
    }

    fn record(&self, event: BoundaryEvent) {
        // Lookup-table replay does not record back; the orchestrator's
        // post-hoc divergence detector consumes the ObservedCall stream instead.
        //
        // The one exception is the router-side response finalizer. It reaches this
        // hook through `LazyEventFinalizer::finalize()` rather than the lookup
        // path, and its replay-side end timestamp is the scorer's authoritative
        // "response finalized" boundary for undeclared-concurrency warnings.
        if event.boundary != "http_incoming" {
            return;
        }
        let event_lineage = serde_json::to_value(&event).unwrap_or(serde_json::Value::Null);
        let raw_bucket_id = event_lineage
            .get("bucket_id")
            .and_then(serde_json::Value::as_str);
        let fork_seq = event_lineage
            .get("fork_seq")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let bucket_id = raw_bucket_id
            .or(event.task_bucket.as_deref())
            .unwrap_or(crate::ROOT_TASK_ID)
            .to_owned();
        let task_bucket = event
            .task_bucket
            .clone()
            .or_else(|| Some(bucket_id.clone()));
        self.observed_sink.observed(ObservedCall {
            correlation_id: event.correlation_id,
            boundary: event.boundary,
            trait_name: event.trait_name,
            method_name: event.method_name,
            args: event.args,
            resolved: false,
            resolved_rank: None,
            source_event_global_sequence: None,
            timestamp_ns: event.timestamp_ns,
            end_timestamp_ns: event.end_timestamp_ns,
            task_id: event.task_id,
            parent_task_id: event.parent_task_id,
            task_bucket,
            bucket_id: Some(bucket_id),
            fork_seq,
            call_file: Some(event.call_file),
            call_line: Some(event.call_line),
            call_column: Some(event.call_column),
            span_path: None,
            graph_node_id: event.graph_node_id,
            synthesized: false,
            real_impl_will_fail: false,
            recorded_result: None,
            observed_result: Some(event.result),
            provenance: crate::Provenance::Recorded,
            seed_gap: false,
        });
    }

    fn next_global_sequence(&self) -> u64 {
        self.global_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    fn next_request_sequence(&self, _correlation_id: Option<&str>) -> u64 {
        // The hot path (try_replay_with_context) bumps its own counter for
        // key construction. This method is called by codegen that records
        // events — we don't record at replay time, so the return value
        // doesn't matter, but it must be a valid u64.
        0
    }

    fn next_callsite_occurrence(
        &self,
        correlation_id: Option<&str>,
        source: CallsiteSource,
        scope: Option<&str>,
    ) -> u32 {
        // SINGLE source of truth for per-callsite occurrence at replay. The
        // boundary macro / DB codegen calls this once per call to build the
        // `CallsiteIdentity::occurrence` it stamps into the lookup identity.
        // It MUST advance in lock-step with `RecordingHook` (one bump per call
        // per `(correlation, bucket, source, scope)`) so the occurrence the renderer
        // read off each recorded event lines up with the occurrence re-derived
        // here at replay. The default trait impl returns a constant `0`, which
        // would collapse every repeated callsite onto occurrence 0 and break
        // rank-4 (`LexicalPath`) resolution after the first call.
        let crate::TaskMetadata {
            task_bucket,
            bucket_id,
            ..
        } = crate::current_task_metadata(correlation_id);
        let bucket_id = bucket_id
            .or(task_bucket)
            .or_else(|| Some(crate::ROOT_TASK_ID.to_string()));
        let key = (
            correlation_id.map(String::from),
            bucket_id,
            source,
            scope.map(String::from),
        );
        let mut guard = self
            .callsite_occurrence
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        let entry = guard.entry(key).or_insert(0);
        let value = *entry;
        *entry += 1;
        value
    }
}

// ===========================================================================
// Generic SEEDING pipeline (PURE, replay-side)
// Replay re-runs selected boundaries against reconstructed state. The
// preconditions come from facts recorded on each event: explicit `read_set` keys,
// explicit `write_set` keys, typed state-key images, and additive declaration
// metadata. Legacy string/method-name fallbacks are deliberately gone: old
// opaque keys stay opaque, string values stay values, and DB creates are
// recognized only when declared as [`OperationKind::Create`]. The functions
// below read only `&[BoundaryEvent]` and produce plain data, so they are fully
// unit-testable without docker. Materialization
// is a separate, thin wiring step (see `deja-orchestrator` lifecycle) that walks
// a `SeedPlan`.
//
// Design source: docs/design/recording-capture-decoupled.md §2.D, §5, §7.1.
// ===========================================================================

/// One precondition to materialize before a correlation is re-executed: the
/// recorded `boundary`/`key` must hold `value`. Derived from explicit read-set
/// captures and/or merged from an ambient template.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SeedEntry {
    /// Recorded boundary this key belongs to.
    pub boundary: String,
    /// The state key from the recorded `read_set`.
    pub key: String,
    /// The recorded legacy value the key held when the correlation read it.
    pub value: serde_json::Value,
    /// Optional typed state image/precondition to materialize instead of `value`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub image: Option<serde_json::Value>,
    /// How this entry entered the plan: derived from the recording's read-set,
    /// or supplied by the ambient/config template.
    pub origin: SeedOrigin,
}

/// Where a [`SeedEntry`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SeedOrigin {
    /// Reconstructed from a recorded event's `read_set` + `result`.
    #[default]
    Recording,
    /// Supplied by the static ambient/config template (deliverable 4).
    Ambient,
}

/// The set of `(boundary, key, value)` preconditions to materialize for a
/// correlation before re-execution, keyed by `(boundary, key)` so a later
/// read-set occurrence (or an ambient default) resolves deterministically.
///
/// Built by [`build_seed_plan`] (deliverable 1) over a recording's events,
/// optionally pre-loaded with an [`AmbientTemplate`] (deliverable 4). Consult it
/// with [`Self::resolve`] / [`Self::classify_read`] (deliverable 3) to decide
/// whether a candidate's diverged read is reconstructable.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SeedPlan {
    /// `(boundary, key) -> entry`. A BTreeMap keeps materialization order stable
    /// (so a `redis-cli SET` sequence is deterministic across runs).
    entries: BTreeMap<(String, String), SeedEntry>,
}

impl SeedPlan {
    /// An empty plan.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert (or overwrite) one precondition. Recording-derived entries take
    /// precedence over ambient defaults for the SAME key (so a key actually
    /// observed in the recording is seeded with what the recording saw, not the
    /// template default); ambient never clobbers a recording entry.
    pub fn upsert(&mut self, entry: SeedEntry) {
        let k = (entry.boundary.clone(), entry.key.clone());
        match self.entries.get(&k) {
            // Recording always wins over Ambient; Recording-over-Recording keeps
            // the FIRST recorded value within the correlation (the precondition
            // the correlation observed before it began mutating the key).
            Some(existing)
                if existing.origin == SeedOrigin::Recording
                    && entry.origin == SeedOrigin::Ambient => {}
            Some(existing) if existing.origin == SeedOrigin::Recording => {}
            _ => {
                self.entries.insert(k, entry);
            }
        }
    }

    /// Number of preconditions in the plan.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the plan is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Resolve the seeded value for a `(boundary, key)`, if the plan has one.
    pub fn resolve(&self, boundary: &str, key: &str) -> Option<&SeedEntry> {
        self.entries.get(&(boundary.to_owned(), key.to_owned()))
    }

    /// Whether the plan (recording-derived OR ambient) covers this key.
    pub fn contains(&self, boundary: &str, key: &str) -> bool {
        self.resolve(boundary, key).is_some()
    }

    /// Iterate the preconditions in deterministic `(boundary, key)` order — the
    /// materialization order the harness shells into the store.
    pub fn iter(&self) -> impl Iterator<Item = &SeedEntry> {
        self.entries.values()
    }

    /// Merge an [`AmbientTemplate`] into this plan (deliverable 4). Ambient
    /// entries fill keys the recording never observed (e.g. a config rate a
    /// re-keyed read reaches for); they never overwrite a recording-derived
    /// precondition. Returns `self` for chaining.
    pub fn with_ambient(mut self, template: &AmbientTemplate) -> Self {
        for entry in template.entries() {
            self.upsert(entry.clone());
        }
        self
    }

    /// Classify a candidate's observed read against this plan (deliverable 3).
    /// See [`ReadClassification`]. NEVER returns `Reconstructable` for a key the
    /// plan does not cover — a key the recording never observed and the template
    /// does not define is a seed-gap, surfaced rather than served stale.
    pub fn classify_read(&self, boundary: &str, key: &str) -> ReadClassification {
        match self.resolve(boundary, key) {
            Some(entry) => ReadClassification::Reconstructable {
                value: entry.value.clone(),
                origin: entry.origin,
            },
            None => ReadClassification::NotReconstructable {
                boundary: boundary.to_owned(),
                key: key.to_owned(),
            },
        }
    }
}

/// Verdict for a candidate's diverged read of a State key (deliverable 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadClassification {
    /// The key is in the seed plan (recording-derived or ambient): the read can
    /// be reconstructed from `value`. `origin` distinguishes a real recorded
    /// precondition from an ambient/config default.
    Reconstructable {
        value: serde_json::Value,
        origin: SeedOrigin,
    },
    /// The key is NOT in the plan AND NOT in the ambient template — a seed-gap.
    /// The harness must surface this (it maps to
    /// [`DivergenceKind::InconclusiveSeedGap`]) rather than silently serving a
    /// stale value for a key the recording never observed.
    NotReconstructable { boundary: String, key: String },
}

impl ReadClassification {
    /// Whether the read can be reconstructed (plan or template covers the key).
    pub fn is_reconstructable(&self) -> bool {
        matches!(self, ReadClassification::Reconstructable { .. })
    }
}

/// Build a [`SeedPlan`] from a recording's events for ONE correlation.
///
/// For each matching event, every explicit `write_set` key marks that
/// `(boundary, key)` as mutated. A non-error, non-miss event then seeds every
/// explicit `read_set` key that has not already been written in the correlation
/// to that event's recorded `result`.
///
/// PURE: reads only `&[BoundaryEvent]`, allocates plain data, performs no I/O.
/// The FIRST recorded read of a key within the correlation wins (its value is
/// the precondition that existed before the correlation began mutating it); a
/// later read of the same key after a write reflects the mutation, not the
/// precondition, so it must not overwrite the seed.
///
/// `correlation_id == None` selects events with no correlation (mirrors
/// [`correlation_matches`]), so a single-case tape still builds a plan.
///
/// Whether a recorded read `result` structurally represents a MISS (no value
/// present), which must NOT be seeded. JSON `null` is always absence. Declared
/// Redis READ events also use the current serde encoding of `DejaRedisValue::Null`
/// (`"Null"`), which is a typed Redis nil, not the literal string to seed. For
/// boundaries that explicitly declare [`ReturnSemantics::Optional`], a successful
/// `{"Ok": null}` result is also absence.
fn is_miss_result(event: &BoundaryEvent) -> bool {
    if event.result.is_null() {
        return true;
    }

    if is_declared_redis_null_read(event) {
        return true;
    }

    let returns_optional = event
        .declaration
        .as_ref()
        .and_then(|declaration| declaration.returns)
        .is_some_and(|returns| returns == ReturnSemantics::Optional);
    if !returns_optional {
        return false;
    }

    event
        .result
        .as_object()
        .is_some_and(|object| object.get("Ok").is_some_and(serde_json::Value::is_null))
}

fn is_declared_redis_null_read(event: &BoundaryEvent) -> bool {
    let Some(declaration) = event.declaration.as_ref() else {
        return false;
    };
    declaration.effect == Some(EffectKind::Redis)
        && !event.read_set.is_empty()
        && event.result.as_str() == Some("Null")
}

fn is_db_create_event(event: &BoundaryEvent) -> bool {
    event
        .declaration
        .as_ref()
        .and_then(|declaration| declaration.op)
        .is_some_and(|op| op == OperationKind::Create)
}

fn db_event_table(event: &BoundaryEvent) -> Option<&str> {
    db_table_from_event_args(&event.args).or_else(|| db_table_from_event_args(&event.request))
}

fn db_table_for_state_key(key: &str) -> Option<String> {
    StateKey::parse(key)
        .ok()
        .and_then(|state_key| state_key.db_table().map(str::to_owned))
}

fn db_read_table(event: &BoundaryEvent, key: &str) -> Option<String> {
    db_table_for_state_key(key).or_else(|| db_event_table(event).map(str::to_owned))
}

fn db_created_table(event: &BoundaryEvent) -> Option<String> {
    db_event_table(event).map(str::to_owned).or_else(|| {
        event
            .write_set
            .iter()
            .find_map(|key| db_table_for_state_key(key))
    })
}

fn preferred_seed_image(event: &BoundaryEvent, canonical_key: &str) -> Option<serde_json::Value> {
    let read_write_same_key = event.boundary == "db"
        && event
            .write_set
            .iter()
            .any(|key| canonical_state_key_wire(key) == canonical_key);
    if read_write_same_key {
        // A DB read+write on the same state key is an RMW precondition. The
        // post-write `result_image` is the wrong state to seed; use an explicit
        // pre-image when the producer captured one, otherwise fall back to the
        // legacy `value = event.result` path by leaving `image` absent.
        event.pre_image.clone()
    } else {
        event.result_image.clone()
    }
}

pub fn build_seed_plan(events: &[BoundaryEvent], correlation_id: Option<&str>) -> SeedPlan {
    let mut plan = SeedPlan::new();
    // A key is "pristine" until the correlation first WRITES it; only reads
    // before the first write to a key describe the precondition.
    let mut written: std::collections::HashSet<(String, String)> = std::collections::HashSet::new();
    // Tables this correlation has explicitly CREATED rows into, by
    // `(boundary, table)`. Typed DB keys (`StateKey::DbRow` / `StateKey::DbQuery`)
    // carry table identity directly; CREATE events also carry a structured DB
    // args/request envelope with `"table"`. We deliberately do NOT mine legacy
    // opaque `"{table}:{sql}"` strings or method names for table identity anymore.
    //
    // Once a correlation CREATES rows in a table, we stop seeding its subsequent
    // reads of that table: it reconstructs its own rows via its writes on replay.
    // UPDATE/DELETE mutate PRE-EXISTING rows, which remain genuine preconditions
    // to seed. Built in event order, so a read BEFORE any create still seeds.
    let mut created_tables: std::collections::HashSet<(String, String)> =
        std::collections::HashSet::new();

    for event in events {
        if !correlation_matches(event, correlation_id) {
            continue;
        }

        // Seed THIS event's reads FIRST — masked only by writes from PRIOR events,
        // not this event's own write. This matters for a mutating op whose read-set
        // key equals its write-set key. Such an op reads a PRE-EXISTING row (the one
        // it mutates) — a genuine precondition — even when the correlation never
        // issued a separate SELECT. If we marked the write before seeding, that
        // pre-image read would be masked as "already written", the pre-existing row
        // would never materialize into the correlation's schema, and the replayed
        // UPDATE would hit an empty table. `created_tables` still skips reads of a
        // table this correlation created (it reconstructs those via its own replayed
        // create), so create-then-update of the same table is unaffected.
        if !(event.is_error || is_miss_result(event)) {
            for key in &event.read_set {
                let canonical_key = canonical_state_key_wire(key);
                let written_key = (event.boundary.clone(), canonical_key.clone());
                if written.contains(&written_key) {
                    continue;
                }
                // Don't seed a read of a table this correlation has already created
                // rows in (it would collide with the replayed INSERT).
                if event.boundary == "db" {
                    if let Some(table) = db_read_table(event, key) {
                        if created_tables.contains(&(event.boundary.clone(), table)) {
                            continue;
                        }
                    }
                }

                plan.upsert(SeedEntry {
                    boundary: event.boundary.clone(),
                    key: canonical_key.clone(),
                    value: event.result.clone(),
                    image: preferred_seed_image(event, &canonical_key),
                    origin: SeedOrigin::Recording,
                });
            }
        }

        // THEN mark this event's writes: subsequent reads of these keys observe the
        // post-write value (no longer a precondition), and a create additionally
        // masks later read-backs of the whole table (they'd collide with the
        // replayed create).
        for key in &event.write_set {
            written.insert((event.boundary.clone(), canonical_state_key_wire(key)));
        }
        if event.boundary == "db" && is_db_create_event(event) {
            if let Some(table) = db_created_table(event) {
                created_tables.insert((event.boundary.clone(), table));
            }
        }
    }

    plan
}

// ---------------------------------------------------------------------------
// Ambient template (deliverable 4) — static config/ambient state
// ---------------------------------------------------------------------------

/// A static template of ambient/config State that is NOT part of any one
/// recording's observed read-set but that a re-keyed / diverged read may reach
/// for (e.g. `settlement_rate_premium`). Merged into a [`SeedPlan`] via
/// [`SeedPlan::with_ambient`] so such reads resolve from the template rather
/// than being flagged as seed-gaps.
///
/// The default ([`AmbientTemplate::demo_defaults`]) carries the EU-settlement
/// demo's premium rate, replacing the hand-coded `redis-cli SET
/// settlement_rate_premium 0.20` in the lifecycle driver.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AmbientTemplate {
    /// Ambient entries, each an `Ambient`-origin precondition.
    entries: Vec<SeedEntry>,
}

impl AmbientTemplate {
    /// An empty template.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register an ambient `(boundary, key, value)`. Stamped `SeedOrigin::Ambient`.
    pub fn insert(
        &mut self,
        boundary: impl Into<String>,
        key: impl Into<String>,
        value: serde_json::Value,
    ) {
        self.entries.push(SeedEntry {
            boundary: boundary.into(),
            key: key.into(),
            value,
            image: None,
            origin: SeedOrigin::Ambient,
        });
    }

    /// The ambient entries.
    pub fn entries(&self) -> &[SeedEntry] {
        &self.entries
    }

    /// Whether the template defines anything.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Parse a template from simple `boundary\tkey\tvalue` lines (one per line,
    /// `#`-comments and blanks ignored). `value` is parsed as JSON if it is
    /// valid JSON, else treated as a JSON string — so `0.20` becomes a number
    /// and `usd` becomes `"usd"`. Lets the demo's ambient config live in a file
    /// (deliverable 4) instead of being hard-coded.
    pub fn from_tsv(text: &str) -> Self {
        let mut template = Self::new();
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut cols = line.splitn(3, '\t');
            let (Some(boundary), Some(key), Some(raw)) = (cols.next(), cols.next(), cols.next())
            else {
                continue;
            };
            let value = serde_json::from_str::<serde_json::Value>(raw.trim())
                .unwrap_or_else(|_| serde_json::Value::String(raw.trim().to_owned()));
            template.insert(boundary.trim(), key.trim(), value);
        }
        template
    }

    /// The EU-settlement demo's ambient defaults. The premium rate is the value
    /// a re-keyed settlement read reaches for during replay; sourcing it here
    /// (instead of a hand-coded `redis-cli SET`) is deliverable 4. The
    /// value is stored raw as it would sit in redis (a string `"0.20"`), so the
    /// materializer writes byte-identical bytes to the old literal seed.
    pub fn demo_defaults() -> Self {
        let mut template = Self::new();
        // The premium settlement rate the divergent (re-keyed) read observes.
        // Was a hand-coded `redis-cli SET settlement_rate_premium 0.20`.
        template.insert(
            "redis",
            "settlement_rate_premium",
            serde_json::Value::String("0.20".to_owned()),
        );
        template
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;
    use crate::{now_ns, BoundaryEvent};

    fn make_event(
        req_seq: u64,
        correlation_id: Option<&str>,
        method: &str,
        args: serde_json::Value,
        result: serde_json::Value,
        is_error: bool,
    ) -> BoundaryEvent {
        BoundaryEvent {
            global_sequence: req_seq,
            request_sequence: req_seq,
            correlation_id: correlation_id.map(String::from),
            timestamp_ns: now_ns(),
            recording_run_id: None,
            graph_node_id: None,
            tracing_span_id: None,
            task_id: Some(crate::ROOT_TASK_ID.to_string()),
            parent_task_id: None,
            task_bucket: Some(crate::ROOT_TASK_ID.to_string()),
            bucket_id: Some(crate::ROOT_TASK_ID.to_string()),
            fork_seq: Some(0),
            boundary: "storage".into(),
            trait_name: "PaymentStore".into(),
            method_name: method.into(),
            call_file: "test.rs".into(),
            call_line: 10,
            call_column: 5,
            receiver: None,
            request: args.clone(),
            args,
            response: result.clone(),
            result,
            is_error,
            duration_us: 100,
            event_schema_version: crate::CURRENT_EVENT_SCHEMA_VERSION,
            callsite_identity: None,
            provenance: crate::Provenance::default(),
            fidelity: crate::Fidelity::default(),
            result_image: None,
            pre_image: None,
            read_set: Vec::new(),
            write_set: Vec::new(),
            value_digest: None,
            entropy_source: None,
            replay_strategy: crate::ReplayStrategy::default(),
            kind: None,
            declaration: None,
            raw_draw: None,
            end_timestamp_ns: None,
        }
    }

    #[test]
    fn replay_exact_match() {
        let events = vec![make_event(
            0,
            None,
            "find_user",
            serde_json::json!({"id": 42}),
            serde_json::json!({"Ok": "Alice"}),
            false,
        )];

        let hook = ReplayHook::new(events, ReplayConfig::default(), 100);

        let result = hook.try_replay(
            "storage",
            "PaymentStore",
            "find_user",
            &serde_json::json!({"id": 42}),
        );

        assert_eq!(result, Some(serde_json::json!({"Ok": "Alice"})));
        let report = hook.take_report();
        assert_eq!(report.matched_calls, 1);
        assert!(report.divergences.is_empty());
    }

    #[test]
    fn replay_novel_call_logged_as_divergence() {
        let events = vec![make_event(
            0,
            None,
            "find_user",
            serde_json::json!({"id": 42}),
            serde_json::json!({"Ok": "Alice"}),
            false,
        )];

        let hook = ReplayHook::new(events, ReplayConfig::default(), 100);

        let result = hook.try_replay(
            "storage",
            "PaymentStore",
            "delete_user",
            &serde_json::json!({"id": 42}),
        );

        assert!(result.is_none());
        let report = hook.take_report();
        assert_eq!(report.divergences.len(), 1);
        assert_eq!(report.divergences[0].kind, DivergenceKind::NovelCall);
    }

    #[test]
    fn replay_sliding_window_recovery() {
        let events = vec![
            make_event(
                0,
                None,
                "step_a",
                serde_json::json!({}),
                serde_json::json!({"Ok": true}),
                false,
            ),
            make_event(
                1,
                None,
                "step_b",
                serde_json::json!({}),
                serde_json::json!({"Ok": true}),
                false,
            ),
            make_event(
                2,
                None,
                "step_c",
                serde_json::json!({"x": 1}),
                serde_json::json!({"Ok": "found"}),
                false,
            ),
        ];

        let hook = ReplayHook::new(events, ReplayConfig::default(), 100);

        // Simulate: V2 skips step_a and step_b, goes straight to step_c
        let result = hook.try_replay(
            "storage",
            "PaymentStore",
            "step_c",
            &serde_json::json!({"x": 1}),
        );

        assert_eq!(result, Some(serde_json::json!({"Ok": "found"})));
        let report = hook.take_report();
        assert_eq!(report.divergences.len(), 1);
        assert_eq!(report.divergences[0].kind, DivergenceKind::OmittedCall);
        assert!(report.divergences[0].detail.contains("2"));
    }

    #[test]
    fn replay_arg_mismatch_argful_default() {
        let events = vec![make_event(
            0,
            None,
            "find_user",
            serde_json::json!({"id": 42}),
            serde_json::json!({"Ok": "Alice"}),
            false,
        )];

        // `OnlyForArgful` (default) lets argful calls fall back to a recorded
        // result on arg mismatch.
        let hook = ReplayHook::new(events, ReplayConfig::default(), 100);

        let result = hook.try_replay(
            "storage",
            "PaymentStore",
            "find_user",
            &serde_json::json!({"id": 99}),
        );

        assert_eq!(result, Some(serde_json::json!({"Ok": "Alice"})));
        let report = hook.take_report();
        assert_eq!(report.divergences.len(), 1);
        assert_eq!(report.divergences[0].kind, DivergenceKind::ArgsDiverged);
    }

    /// P2 correctness gate: a call whose recorded args are JSON-null (the
    /// "argless boundary" shape used by time / id / random) MUST NOT silently
    /// hand back the recorded result when V2 calls with EMPTY-OBJECT args
    /// (the other "argless" shape) under the default `OnlyForArgful` policy.
    /// A clock or id generator that lied about its arg signature is the
    /// worst possible failure mode for replay.
    #[test]
    fn argless_call_fails_closed_under_default_policy() {
        let events = vec![make_event(
            0,
            None,
            "current_time",
            serde_json::Value::Null,
            serde_json::json!({"Ok": 1_700_000_000_u64}),
            false,
        )];

        let hook = ReplayHook::new(events, ReplayConfig::default(), 100);

        // V2 calls with an empty-object arg shape — DIFFERENT from the
        // recorded `null` (so the first-pass exact match misses) but still
        // "argless" by policy. `allow_arg_mismatch` MUST return false.
        let result = hook.try_replay(
            "storage",
            "PaymentStore",
            "current_time",
            &serde_json::json!({}),
        );

        assert!(
            result.is_none(),
            "default policy must NOT return a recorded argless result on mismatch"
        );

        let report = hook.take_report();
        assert!(
            report.divergence_count > 0,
            "argless mismatch must register at least one divergence"
        );
        assert!(
            report
                .divergences
                .iter()
                .any(|d| d.kind == DivergenceKind::ArgSkipBlocked),
            "expected at least one ArgSkipBlocked divergence; got: {:?}",
            report
                .divergences
                .iter()
                .map(|d| d.kind.clone())
                .collect::<Vec<_>>()
        );
    }

    // -----------------------------------------------------------------
    // Lookup-table replay tests
    // -----------------------------------------------------------------

    fn entry_with(
        correlation_id: Option<&str>,
        address: Address,
        args: &serde_json::Value,
        occurrence: u32,
        result: serde_json::Value,
        source_event_global_sequence: u64,
    ) -> LookupEntry {
        LookupEntry {
            key: LookupKey {
                correlation_id: correlation_id.map(str::to_owned),
                bucket_id: Some(crate::ROOT_TASK_ID.to_string()),
                fork_seq: 0,
                address,
                args_hash: canonical_args_hash(args),
                occurrence,
            },
            result,
            source_event_global_sequence,
        }
    }

    #[test]
    fn lookup_entry_without_bucket_fields_deserializes_with_defaults() {
        let mut legacy = serde_json::to_value(LookupEntry {
            key: LookupKey {
                correlation_id: Some("corr-legacy".to_owned()),
                bucket_id: Some("bucket-ignored-before-removal".to_owned()),
                fork_seq: 42,
                address: explicit("legacy-site"),
                args_hash: 7,
                occurrence: 0,
            },
            result: serde_json::json!("v"),
            source_event_global_sequence: 11,
        })
        .unwrap();
        let key = legacy
            .get_mut("key")
            .and_then(serde_json::Value::as_object_mut)
            .unwrap();
        key.remove("bucket_id");
        key.remove("fork_seq");

        let entry: LookupEntry =
            serde_json::from_value(legacy).expect("legacy lookup entry must deserialize");
        assert_eq!(entry.key.bucket_id, None);
        assert_eq!(entry.key.fork_seq, 0);
    }

    fn explicit(tag: &str) -> Address {
        Address::Explicit(tag.to_owned())
    }

    fn lexical_identity(path: &str) -> CallsiteIdentity {
        CallsiteIdentity {
            version: 1,
            source: CallsiteSource::LexicalPath,
            id: None,
            scope: None,
            occurrence: 0,
            caller_function: None,
            lexical_path: Some(path.to_owned()),
            syntax_hash: None,
            span_path: None,
        }
    }

    fn explicit_identity(tag: &str) -> CallsiteIdentity {
        CallsiteIdentity {
            version: 1,
            source: CallsiteSource::Explicit,
            id: Some(tag.to_owned()),
            scope: None,
            occurrence: 0,
            caller_function: None,
            lexical_path: None,
            syntax_hash: None,
            span_path: None,
        }
    }

    struct VecSource(Option<LookupTable>);
    impl LookupTableSource for VecSource {
        fn load(&mut self) -> std::io::Result<LookupTable> {
            self.0
                .take()
                .ok_or_else(|| std::io::Error::other("double-load not supported in test"))
        }
    }

    #[test]
    fn local_file_lookup_source_reads_jsonl() {
        use std::io::Write;
        let dir = tempfile::tempdir().expect("tmp");
        let path = dir.path().join("table.jsonl");
        let mut file = std::fs::File::create(&path).expect("create");
        let entry = entry_with(
            Some("c-1"),
            Address::Sequence {
                boundary: "redis".to_owned(),
                method: "get_key".to_owned(),
                request_sequence: 0,
            },
            &serde_json::json!({}),
            0,
            serde_json::json!("hello"),
            42,
        );
        writeln!(file, "{}", serde_json::to_string(&entry).unwrap()).unwrap();
        drop(file);

        let mut source = LocalFileLookupSource::new(&path);
        let table = source.load().expect("load");
        assert_eq!(table.entries.len(), 1);
        assert_eq!(table.entries[0].result, serde_json::json!("hello"));
    }

    #[test]
    fn lookup_table_hook_resolves_by_args_hash_and_records_observation() {
        // Two calls to the same explicit site, distinguished only by their
        // args. Each has occurrence 0 because the (address, args_hash) buckets
        // differ — so resolution is keyed by *what* was called, not *when*.
        let table = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries: vec![
                entry_with(
                    None,
                    explicit("site"),
                    &serde_json::json!({ "id": 1 }),
                    0,
                    serde_json::json!("alpha"),
                    7,
                ),
                entry_with(
                    None,
                    explicit("site"),
                    &serde_json::json!({ "id": 2 }),
                    0,
                    serde_json::json!("beta"),
                    9,
                ),
            ],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        let identity = explicit_identity("site");
        let call = |args: serde_json::Value| {
            hook.try_replay_with_context(ReplayLookup {
                boundary: "redis",
                trait_name: "RedisStore",
                method_name: "get_key",
                args: &args,
                callsite_identity: Some(&identity),
                caller_location: None,
            })
        };

        // Drive args id:2 BEFORE id:1 — the opposite of recorded order — to
        // prove order independence even within this small case.
        assert_eq!(
            call(serde_json::json!({ "id": 2 })),
            Some(serde_json::json!("beta"))
        );
        assert_eq!(
            call(serde_json::json!({ "id": 1 })),
            Some(serde_json::json!("alpha"))
        );
        // A novel-arg call at this existing call-site now misses. The
        // arg-tolerant fallback is removed; under the partial-function model a
        // re-keyed call is an honest miss. At the dispatch seam this fail-stops;
        // at the hook layer it is a plain None.
        assert_eq!(
            call(serde_json::json!({ "id": 3 })),
            None,
            "a re-keyed call now misses instead of serving a stale value"
        );

        let calls = handle.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert_eq!(
            calls[0].resolved_rank,
            Some(1),
            "explicit address is rank 1"
        );
        assert_eq!(calls[0].source_event_global_sequence, Some(9));
        assert_eq!(calls[1].source_event_global_sequence, Some(7));
        // call 3 (novel args) is now an unresolved miss.
        assert!(!calls[2].resolved);
        assert_eq!(calls[2].resolved_rank, None);
        assert_eq!(
            calls[0].boundary, "redis",
            "boundary carried on the observation"
        );
    }

    #[test]
    fn lookup_resolves_iteration_order_independent() {
        // Simulate the renderer: walk a connector loop in order [1, 2, 3],
        // building the table via the SHARED key-construction path.
        let identity = lexical_identity("crate::pay::confirm::loop");
        let mut stamper = KeyStamper::new();
        let mut entries = Vec::new();
        for (i, connector) in [1u64, 2, 3].into_iter().enumerate() {
            let args = serde_json::json!({ "connector": connector });
            let addresses = addresses_for("redis", "get_key", Some(&identity), None, i as u64);
            for key in stamper.stamp(
                None,
                Some(crate::ROOT_TASK_ID),
                0,
                &addresses,
                canonical_args_hash(&args),
            ) {
                entries.push(LookupEntry {
                    key,
                    result: serde_json::json!(format!("v{connector}")),
                    source_event_global_sequence: i as u64,
                });
            }
        }
        let table = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries,
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        // Replay in a DIFFERENT iteration order: [3, 1, 2]. All must resolve
        // (rank-4 lexical path + args_hash), proving order independence.
        let call = |connector: u64| {
            hook.try_replay_with_context(ReplayLookup {
                boundary: "redis",
                trait_name: "RedisStore",
                method_name: "get_key",
                args: &serde_json::json!({ "connector": connector }),
                callsite_identity: Some(&identity),
                caller_location: None,
            })
        };
        assert_eq!(call(3), Some(serde_json::json!("v3")));
        assert_eq!(call(1), Some(serde_json::json!("v1")));
        assert_eq!(call(2), Some(serde_json::json!("v2")));

        let calls = handle.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert!(
            calls
                .iter()
                .all(|c| c.resolved && c.resolved_rank == Some(4)),
            "every call resolves at rank 4 regardless of iteration order"
        );
    }

    // -----------------------------------------------------------------
    // Boundary-path identity tests: a BOUNDARY-path identity
    // (CallsiteSource::SyntacticHash + syntax_hash + lexical_path) is
    // emitted by the macro/DB codegen and must resolve at ranks 3/4
    // (SpanPath occupies rank 2).
    // -----------------------------------------------------------------

    /// An identity shaped exactly like the boundary macro / DB codegen emits:
    /// `SyntacticHash` source carrying BOTH a `syntax_hash` (rank 3) and a
    /// `lexical_path` (rank 4), with a per-callsite `occurrence`.
    fn boundary_identity(scope: &str, occurrence: u32) -> CallsiteIdentity {
        CallsiteIdentity {
            version: 1,
            source: CallsiteSource::SyntacticHash,
            id: None,
            scope: Some(scope.to_owned()),
            occurrence,
            caller_function: Some("crate::module".to_owned()),
            lexical_path: Some("crate::module".to_owned()),
            syntax_hash: Some(crate::stable_callsite_hash(scope)),
            span_path: None,
        }
    }

    /// Mirror the renderer (`deja-orchestrator`): walk recorded events and
    /// build a lookup table via the SHARED `addresses_for` + `KeyStamper`.
    fn render_table(events: &[BoundaryEvent]) -> LookupTable {
        let mut stamper = KeyStamper::new();
        let mut request_seq: HashMap<Option<String>, u64> = HashMap::new();
        let mut entries = Vec::new();
        for event in events {
            let slot = request_seq.entry(event.correlation_id.clone()).or_insert(0);
            let request_sequence = *slot;
            *slot += 1;
            let location = Some((event.call_file.as_str(), event.call_line, event.call_column));
            let addresses = addresses_for(
                &event.boundary,
                &event.method_name,
                event.callsite_identity.as_ref(),
                location,
                request_sequence,
            );
            let args_hash = canonical_args_hash(&event.args);
            let bucket_id = event
                .bucket_id
                .as_deref()
                .or(event.task_bucket.as_deref())
                .unwrap_or(crate::ROOT_TASK_ID);
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
        LookupTable {
            recording_id: "rec-boundary".to_owned(),
            policy_version: 1,
            entries,
        }
    }

    #[test]
    fn lookup_table_replay_is_correlation_isolated_across_parallel_requests() {
        let identity = explicit_identity("redis-get-key-parallel");
        let args = serde_json::json!({ "key": "shared" });
        let event = |global_sequence: u64, correlation_id: &str, value: &str| {
            let mut event = make_event(
                global_sequence,
                Some(correlation_id),
                "get_key",
                args.clone(),
                serde_json::json!({ "Ok": value }),
                false,
            );
            event.boundary = "redis".to_owned();
            event.trait_name = "RedisStore".to_owned();
            event.callsite_identity = Some(identity.clone());
            event
        };
        let table = render_table(&[event(0, "corr-a", "value-a"), event(1, "corr-b", "value-b")]);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook = std::sync::Arc::new(
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source"),
        );

        let mut threads = Vec::new();
        for (correlation_id, expected_value) in [
            ("corr-a", serde_json::json!({ "Ok": "value-a" })),
            ("corr-b", serde_json::json!({ "Ok": "value-b" })),
        ] {
            let hook = std::sync::Arc::clone(&hook);
            let identity = identity.clone();
            let args = args.clone();
            threads.push(std::thread::spawn(move || {
                let _guard = deja_context::enter_correlation_id(correlation_id);
                let result = hook.try_replay_with_context(ReplayLookup {
                    boundary: "redis",
                    trait_name: "RedisStore",
                    method_name: "get_key",
                    args: &args,
                    callsite_identity: Some(&identity),
                    caller_location: None,
                });
                (correlation_id.to_owned(), expected_value, result)
            }));
        }

        let mut returned = threads
            .into_iter()
            .map(|thread| thread.join().expect("parallel replay thread panicked"))
            .collect::<Vec<_>>();
        returned.sort_by(|left, right| left.0.cmp(&right.0));
        for (correlation_id, expected_value, result) in &returned {
            assert_eq!(
                result.as_ref(),
                Some(expected_value),
                "{correlation_id} must replay its own recorded result"
            );
        }
        assert_ne!(
            returned[0].2, returned[1].2,
            "different correlations intentionally recorded different values"
        );

        let mut calls = match handle.lock() {
            Ok(calls) => calls.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        };
        calls.sort_by(|left, right| left.correlation_id.cmp(&right.correlation_id));
        assert_eq!(calls.len(), 2);
        for (call, (correlation_id, expected_value)) in calls.iter().zip([
            ("corr-a", serde_json::json!({ "Ok": "value-a" })),
            ("corr-b", serde_json::json!({ "Ok": "value-b" })),
        ]) {
            assert_eq!(call.correlation_id.as_deref(), Some(correlation_id));
            assert_eq!(call.boundary, "redis");
            assert_eq!(call.trait_name, "RedisStore");
            assert_eq!(call.method_name, "get_key");
            assert_eq!(call.args, args);
            assert!(call.resolved, "{correlation_id} should resolve");
            assert_eq!(call.resolved_rank, Some(1));
            assert_eq!(call.recorded_result.as_ref(), Some(&expected_value));
            assert_eq!(call.observed_result.as_ref(), Some(&expected_value));
        }
    }

    #[test]
    fn stable_callsite_hash_is_deterministic_and_line_shift_independent() {
        // (1) Determinism: same input → same hash, every time.
        let a = crate::stable_callsite_hash("redis::RedisStore::get_key");
        let b = crate::stable_callsite_hash("redis::RedisStore::get_key");
        assert_eq!(a, b, "syntax hash must be deterministic");

        // (2) Line-shift independence: the hash is a pure function of the
        // boundary/component/operation string, NOT of any file:line. Two
        // "recordings" taken with the call site at different source lines hash
        // identically because the input string is unchanged.
        let record_time = crate::stable_callsite_hash("redis::RedisStore::get_key");
        let replay_time_after_edits_shifted_lines =
            crate::stable_callsite_hash("redis::RedisStore::get_key");
        assert_eq!(
            record_time, replay_time_after_edits_shifted_lines,
            "syntax hash must survive source line shifts"
        );

        // (3) Distinctness: different operation → different hash.
        assert_ne!(
            crate::stable_callsite_hash("redis::RedisStore::get_key"),
            crate::stable_callsite_hash("redis::RedisStore::set_key"),
            "distinct operations must hash differently"
        );
    }

    #[test]
    fn boundary_path_event_resolves_at_rank_three() {
        // A recorded BOUNDARY event now carries callsite_identity: Some(_) with
        // syntax_hash: Some(_). Prove it both (a) carries the identity and (b)
        // resolves at rank 3 (SyntacticHash) through the renderer→hook pipeline.
        // (Rank 3, not 2: P3 inserted SpanPath at rank 2, and this identity
        // carries no span_path.)
        let identity = boundary_identity("redis::RedisStore::get_key", 0);
        assert!(
            identity.syntax_hash.is_some(),
            "boundary identity must carry a syntax_hash"
        );

        let mut event = make_event(
            0,
            Some("corr-1"),
            "get_key",
            serde_json::json!({ "key": "k1" }),
            serde_json::json!({ "Ok": "v1" }),
            false,
        );
        event.boundary = "redis".into();
        event.callsite_identity = Some(identity.clone());
        assert!(
            event.callsite_identity.is_some(),
            "on-disk boundary event must carry Some(callsite_identity), not None"
        );

        let table = render_table(&[event]);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        let _guard = deja_context::enter_correlation_id("corr-1");
        let result = hook.try_replay_with_context(ReplayLookup {
            boundary: "redis",
            trait_name: "RedisStore",
            method_name: "get_key",
            args: &serde_json::json!({ "key": "k1" }),
            callsite_identity: Some(&identity),
            caller_location: None,
        });
        assert_eq!(result, Some(serde_json::json!({ "Ok": "v1" })));

        let calls = handle.lock().unwrap().clone();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].resolved_rank,
            Some(3),
            "boundary syntax-hash identity must resolve at rank 3"
        );
    }

    #[test]
    fn boundary_path_resolves_at_rank_four_when_only_lexical_path_present() {
        // When syntax_hash is absent but lexical_path is present (e.g. a
        // recording produced before rank-3 emission), the SAME boundary call
        // still resolves — at rank 4 — proving the lexical path is an additive
        // fallback below SyntacticHash.
        let mut identity = boundary_identity("redis::RedisStore::get_key", 0);
        identity.syntax_hash = None; // force the rank-3 SyntacticHash address absent
        identity.source = CallsiteSource::LexicalPath;

        let mut event = make_event(
            0,
            Some("corr-1"),
            "get_key",
            serde_json::json!({ "key": "k1" }),
            serde_json::json!({ "Ok": "v1" }),
            false,
        );
        event.boundary = "redis".into();
        event.callsite_identity = Some(identity.clone());

        let table = render_table(&[event]);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        let _guard = deja_context::enter_correlation_id("corr-1");
        let result = hook.try_replay_with_context(ReplayLookup {
            boundary: "redis",
            trait_name: "RedisStore",
            method_name: "get_key",
            args: &serde_json::json!({ "key": "k1" }),
            callsite_identity: Some(&identity),
            caller_location: None,
        });
        assert_eq!(result, Some(serde_json::json!({ "Ok": "v1" })));
        assert_eq!(
            handle.lock().unwrap()[0].resolved_rank,
            Some(4),
            "lexical-path-only identity must resolve at rank 4"
        );
    }

    #[test]
    fn span_path_disambiguates_concurrent_same_callsite_calls() {
        // The concurrent-occurrence-swap fix in miniature. Two calls to the SAME boundary/op (so an
        // IDENTICAL syntax_hash) with IDENTICAL args, distinguished ONLY by the
        // span they fired in (their `span_path`). Recorded in the order
        // [attempt, intent]; replayed in the SWAPPED order [intent, attempt], as
        // async task interleaving would. Each must resolve to ITS OWN recorded
        // result at rank 2 (SpanPath) — NOT swapped.
        //
        // Without SpanPath both calls would share the
        // (correlation, SyntacticHash, args_hash) bucket and be tiebroken by a
        // positional `occurrence` (0,1) that swaps on reorder → attempt would get
        // intent's recorded row. The span-scoped SpanPath address puts them
        // in DISTINCT buckets (occ 0 each), so the match is order-independent.
        let scope = "db::Store::update";
        let make = |logical: &str| CallsiteIdentity {
            version: 1,
            source: CallsiteSource::SyntacticHash,
            id: None,
            scope: Some(scope.to_owned()),
            occurrence: 0,
            caller_function: Some("crate::module".to_owned()),
            lexical_path: Some("crate::module".to_owned()),
            syntax_hash: Some(crate::stable_callsite_hash(scope)),
            span_path: Some(logical.to_owned()),
        };
        let id_attempt = make("payments_core>update_payment_attempt");
        let id_intent = make("payments_core>update_payment_intent");
        // IDENTICAL args for both calls — so args_hash can't distinguish them.
        let args = serde_json::json!({ "id": 1 });

        let event = |gseq: u64, id: &CallsiteIdentity, result: &str| {
            let mut e = make_event(
                gseq,
                Some("c1"),
                "update",
                args.clone(),
                serde_json::json!({ "Ok": result }),
                false,
            );
            e.boundary = "db".into();
            e.callsite_identity = Some(id.clone());
            e
        };
        // Record order: attempt → intent.
        let table = render_table(&[
            event(0, &id_attempt, "attempt-row"),
            event(1, &id_intent, "intent-row"),
        ]);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        let _guard = deja_context::enter_correlation_id("c1");
        let replay = |id: &CallsiteIdentity| {
            hook.try_replay_with_context(ReplayLookup {
                boundary: "db",
                trait_name: "Store",
                method_name: "update",
                args: &args,
                callsite_identity: Some(id),
                caller_location: None,
            })
        };
        // Replay in the SWAPPED order: intent first, then attempt.
        assert_eq!(
            replay(&id_intent),
            Some(serde_json::json!({ "Ok": "intent-row" })),
            "the intent call must get the INTENT row even though it replays first"
        );
        assert_eq!(
            replay(&id_attempt),
            Some(serde_json::json!({ "Ok": "attempt-row" })),
            "the attempt call must get the ATTEMPT row — NOT swapped by a shared \
             positional occurrence"
        );

        let calls = handle.lock().unwrap().clone();
        assert!(
            calls
                .iter()
                .all(|c| c.resolved && c.resolved_rank == Some(2)),
            "both resolve at rank 2 (SpanPath), not a weaker positional fallback; \
             got resolved/rank = {:?}",
            calls
                .iter()
                .map(|c| (c.resolved, c.resolved_rank))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn callsite_occurrence_is_single_bump_sequence() {
        // Repeated calls at the SAME logical callsite (same correlation, source,
        // scope) within one correlation must yield occurrence 0, 1, 2 — i.e. a
        // single increment per call. This is what keeps record and replay keys
        // aligned for repeated same-callsite invocations. `ReplayHook` shares
        // the same per-(correlation, source, scope) counter logic the recorder
        // and runtime hook use.
        let hook = ReplayHook::new(Vec::new(), ReplayConfig::default(), 100);
        let occ = || {
            DejaHook::next_callsite_occurrence(
                &hook,
                Some("corr-1"),
                CallsiteSource::SyntacticHash,
                Some("redis::RedisStore::get_key"),
            )
        };
        assert_eq!(occ(), 0);
        assert_eq!(occ(), 1);
        assert_eq!(occ(), 2);

        // A DIFFERENT scope is a different bucket, restarting at 0.
        assert_eq!(
            DejaHook::next_callsite_occurrence(
                &hook,
                Some("corr-1"),
                CallsiteSource::SyntacticHash,
                Some("redis::RedisStore::set_key"),
            ),
            0,
            "distinct scope must have an independent occurrence counter"
        );
    }

    /// Regression guard for the Phase-1 occurrence DOUBLE-BUMP.
    ///
    /// The same logical callsite is hit 3 times within one correlation with
    /// the SAME args. On RECORD the occurrence counter advances per call, so
    /// the three events carry occurrence 0, 1, 2 and the renderer stamps three
    /// distinct rank-4 (`LexicalPath { scope_occurrence }`) addresses. On
    /// REPLAY the boundary macro RE-DERIVES the occurrence for each call by
    /// calling `DejaHook::next_callsite_occurrence` on the SAME hook that does
    /// the lookup (here `LookupTableHook`) — exactly as the generated code in
    /// `recordable.rs` / `instrument.rs` does.
    ///
    /// Before the fix, `LookupTableHook` did not implement
    /// `next_callsite_occurrence`, so the default `0` was returned for EVERY
    /// call: the replay identities all carried occurrence 0 while the record
    /// identities carried 0, 1, 2. Only the first (occurrence-0) call resolved;
    /// calls 2 and 3 missed at every rank. This asymmetry once collapsed a
    /// nearly-fully-resolved replay to a handful of matches. The fix makes the
    /// hook advance the occurrence in
    /// lock-step with the recorder, so record sequence == replay sequence and
    /// all three calls resolve.
    #[test]
    fn repeated_callsite_resolves_when_occurrence_is_rederived_on_replay() {
        let scope = "redis::RedisStore::get_key";
        let correlation = Some("corr-1");
        let args = serde_json::json!({ "key": "k" });

        // --- RECORD pass: a per-(correlation, source, scope) counter advances
        // once per call, exactly like `RecordingHook::next_callsite_occurrence`.
        let recorder = ReplayHook::new(Vec::new(), ReplayConfig::default(), 0);
        let mut events = Vec::new();
        for i in 0..3u64 {
            let occurrence = DejaHook::next_callsite_occurrence(
                &recorder,
                correlation,
                CallsiteSource::SyntacticHash,
                Some(scope),
            );
            // Record-side occurrence sequence MUST be 0, 1, 2.
            assert_eq!(occurrence, i as u32);
            let mut event = make_event(
                i,
                correlation,
                "get_key",
                args.clone(),
                serde_json::json!({ "Ok": format!("v{i}") }),
                false,
            );
            event.boundary = "redis".into();
            event.callsite_identity = Some(boundary_identity(scope, occurrence));
            events.push(event);
        }

        // --- RENDER: build the lookup table from the recorded events.
        let table = render_table(&events);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        // --- REPLAY pass: the macro re-derives the occurrence per call through
        // the SAME hook that performs the lookup, then looks up with that
        // identity. Drive the three calls and assert all resolve.
        let _guard = deja_context::enter_correlation_id("corr-1");
        for i in 0..3u64 {
            let occurrence = DejaHook::next_callsite_occurrence(
                &hook,
                correlation,
                CallsiteSource::SyntacticHash,
                Some(scope),
            );
            // The whole point: replay occurrence sequence MUST equal the record
            // sequence (0, 1, 2), not 0, 0, 0.
            assert_eq!(
                occurrence, i as u32,
                "replay-side occurrence must advance in lock-step with record"
            );
            let identity = boundary_identity(scope, occurrence);
            let result = hook.try_replay_with_context(ReplayLookup {
                boundary: "redis",
                trait_name: "RedisStore",
                method_name: "get_key",
                args: &args,
                callsite_identity: Some(&identity),
                caller_location: None,
            });
            assert_eq!(
                result,
                Some(serde_json::json!({ "Ok": format!("v{i}") })),
                "call #{i} (occurrence {occurrence}) must resolve to its recorded result"
            );
        }

        let calls = handle.lock().unwrap().clone();
        assert_eq!(calls.len(), 3);
        assert!(
            calls.iter().all(|c| c.resolved),
            "all three repeated-callsite calls must resolve (no double-bump miss): {:?}",
            calls
                .iter()
                .map(|c| (c.resolved, c.resolved_rank))
                .collect::<Vec<_>>()
        );
    }

    /// An identity shaped EXACTLY like `instrument.rs` emits at expansion time:
    /// `source: SyntacticHash`, `id: None`, `syntax_hash: Some`,
    /// `lexical_path: Some(module_path)`, and the per-callsite `occurrence`
    /// allocated ONCE via `next_boundary_occurrence`. This is the boundary-macro
    /// identity, distinct from the hand-built `boundary_identity` helper which
    /// the prior fix attempts leaned on.
    ///
    /// `lexical_path` is the runtime `module_path!()` (e.g.
    /// `common_utils::date_time`) — which is NOT the same string as `scope`
    /// (`common_utils::date_time::now`). The occurrence is bucketed on `scope`
    /// at record/replay, but the rank-4 `LexicalPath` address is keyed on
    /// `lexical_path`.
    fn macro_emitted_identity(
        scope: &str,
        lexical_path: &str,
        occurrence: u32,
    ) -> CallsiteIdentity {
        CallsiteIdentity {
            version: 1,
            source: CallsiteSource::SyntacticHash,
            id: None, // <-- macro ALWAYS sets id: None (see instrument.rs)
            scope: Some(scope.to_owned()),
            occurrence,
            caller_function: Some(lexical_path.to_owned()),
            lexical_path: Some(lexical_path.to_owned()),
            syntax_hash: Some(crate::stable_callsite_hash(scope)),
            span_path: None,
        }
    }

    /// PIPELINE-FIDELITY REPRODUCTION of the 197 -> 11 regression, modelled on
    /// the real `time::date_time::now` recording (10 argless calls within one
    /// correlation, SAME boundary/method/scope, all funnelling through the
    /// single `#[track_caller]` boundary fn).
    ///
    /// This drives the EXACT dockerized-replay path:
    ///   recorded events (macro-style identity)
    ///     -> `render_table` (the real `addresses_for` + `KeyStamper` renderer)
    ///     -> `LookupTableHook::try_replay_with_context`
    /// where the boundary macro RE-DERIVES the per-callsite `occurrence` at
    /// replay through the SAME hook that performs the lookup (exactly as
    /// `instrument.rs` / `recordable.rs` generate).
    ///
    /// KEY FIDELITY POINT — why the prior reproductions stayed green while the
    /// pipeline was red: the `SyntacticHash` address (rank 3) is
    /// occurrence-INDEPENDENT (the hash is in the address, the KeyStamper
    /// occurrence aligns on its own), so as long as syntax-hash entries exist a
    /// test resolves even with a broken occurrence counter. The BROKEN pipeline
    /// artifact had NO syntax-hash entries for these repeated argless callsites,
    /// so resolution fell to `LexicalPath { scope_occurrence }` (rank 4) — which
    /// embeds the re-derived `occurrence` directly into the address. If the
    /// hook does not advance `next_callsite_occurrence` in lock-step with the
    /// recorder, EVERY replay query carries occurrence 0, so only the recorded
    /// occurrence-0 event matches and calls #2..N miss at every rank — the
    /// 197 -> 11 collapse.
    ///
    /// This test therefore models the lexical path explicitly: the identity
    /// carries a `lexical_path` (rank 4) but NO `syntax_hash` (no rank 3), so
    /// resolution DEPENDS on the re-derived occurrence being correct.
    #[test]
    fn repro_date_time_now_repeated_argless_calls_all_resolve() {
        let scope = "common_utils::date_time::now";
        let lexical = "common_utils::date_time";
        let correlation = Some("corr-1");
        let args = serde_json::json!({}); // argless boundary

        // Macro-style identity WITHOUT a syntax_hash, forcing lexical-path (rank 4)
        // resolution — the path the broken pipeline artifact actually exercised.
        // (When the syntax_hash is present, the rank-3 SyntacticHash masks the
        // occurrence bug; see the doc comment above.)
        let identity_for = |occurrence: u32| {
            let mut id = macro_emitted_identity(scope, lexical, occurrence);
            id.syntax_hash = None; // no rank-3 — resolution must use rank-4 lexical
            id.source = CallsiteSource::LexicalPath;
            id
        };

        // --- RECORD pass: occurrence advances once per call, bucketed on
        // (correlation, source, scope) — exactly like
        // `RecordingHook::next_callsite_occurrence`.
        let recorder = ReplayHook::new(Vec::new(), ReplayConfig::default(), 0);
        let mut events = Vec::new();
        for i in 0..10u64 {
            let occurrence = DejaHook::next_callsite_occurrence(
                &recorder,
                correlation,
                CallsiteSource::LexicalPath,
                Some(scope),
            );
            assert_eq!(occurrence, i as u32, "record occurrence must be 0..N");
            let mut event = make_event(
                i,
                correlation,
                "date_time::now",
                args.clone(),
                serde_json::json!({ "Ok": format!("t{i}") }),
                false,
            );
            event.boundary = "time".into();
            event.callsite_identity = Some(identity_for(occurrence));
            events.push(event);
        }

        // --- RENDER: the real renderer (one entry per rank per event).
        let table = render_table(&events);
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        // --- REPLAY pass: the macro re-derives the per-callsite occurrence via
        // the SAME hook that performs the lookup, then queries with that
        // identity. The candidate visits the callsite the SAME number of times
        // but in a DIFFERENT order than the recording (the lexical-path rank is
        // supposed to be iteration-order independent — each occurrence
        // self-addresses). This removes the rank-6 positional safety net, so a
        // desynced occurrence produces a genuine MISS (the "responses broke"
        // symptom) rather than a lucky rank-6 rescue.
        //
        // With the occurrence fix in place every repeated argless call resolves
        // at rank 3; without it, only the occurrence-0 lookup matches and the
        // other nine calls miss entirely.
        // The candidate visits the 10 occurrences in a shuffled order; the
        // concrete permutation does not matter (rank-4 is order-independent),
        // only that each occurrence 0..9 is visited exactly once.
        let _guard = deja_context::enter_correlation_id("corr-1");
        for _ in 0..10u64 {
            let occurrence = DejaHook::next_callsite_occurrence(
                &hook,
                correlation,
                CallsiteSource::LexicalPath,
                Some(scope),
            );
            let identity = identity_for(occurrence);
            let result = hook.try_replay_with_context(ReplayLookup {
                boundary: "time",
                trait_name: "Time",
                method_name: "date_time::now",
                args: &args,
                callsite_identity: Some(&identity),
                caller_location: None,
            });
            // NOTE: results are recorded per-occurrence, and the candidate hits
            // occurrences 0..9 (just in a shuffled visitation order), so the
            // recorded result for occurrence `occurrence` is `t{occurrence}`.
            assert_eq!(
                result,
                Some(serde_json::json!({ "Ok": format!("t{occurrence}") })),
                "repeated argless call (occurrence {occurrence}) must resolve at rank 4"
            );
        }

        let calls = handle.lock().unwrap().clone();
        assert_eq!(calls.len(), 10);
        assert!(
            calls
                .iter()
                .all(|c| c.resolved && c.resolved_rank == Some(4)),
            "all repeated argless calls must resolve at rank 4 (order-independent); \
             got resolved/rank = {:?}",
            calls
                .iter()
                .map(|c| (c.resolved, c.resolved_rank))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn lookup_table_hook_prefers_stronger_rank_over_sequence() {
        // The same call can match both an explicit (rank 1) entry and a
        // sequence (rank 5) entry. Strongest-first querying must pick rank 1.
        let args = serde_json::json!({});
        let table = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries: vec![
                entry_with(
                    None,
                    Address::Sequence {
                        boundary: "redis".to_owned(),
                        method: "m".to_owned(),
                        request_sequence: 0,
                    },
                    &args,
                    0,
                    serde_json::json!("by_sequence"),
                    1,
                ),
                entry_with(
                    None,
                    explicit("stable-X"),
                    &args,
                    0,
                    serde_json::json!("by_explicit"),
                    2,
                ),
            ],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook = LookupTableHook::from_source(VecSource(Some(table)), observed).expect("hook");

        let identity = explicit_identity("stable-X");
        let value = hook.try_replay_with_context(ReplayLookup {
            boundary: "redis",
            trait_name: "S",
            method_name: "m",
            args: &args,
            callsite_identity: Some(&identity),
            caller_location: None,
        });
        assert_eq!(value, Some(serde_json::json!("by_explicit")));
        assert_eq!(handle.lock().unwrap()[0].resolved_rank, Some(1));
    }

    // -----------------------------------------------------------------------
    // Arg mismatch lookup (partial-function replay)
    // -----------------------------------------------------------------------

    /// A re-keyed call (its args changed since recording) now MISSES. The
    /// arg-tolerant fallback that used to serve the stale recorded value is
    /// REMOVED (it was the serve-stale lie that masked transitive effects). Under
    /// the partial-function model a re-keyed call is an honest miss: the dispatch
    /// seam fail-stops on it; at the hook layer it is a plain `None` with an
    /// unresolved observation.
    #[test]
    fn rekeyed_call_now_misses() {
        let recorded_args = serde_json::json!({ "id": "pi_recorded" });
        let table = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries: vec![entry_with(
                None,
                explicit("find_pi"),
                &recorded_args,
                0,
                serde_json::json!({ "Ok": "row_recorded" }),
                1,
            )],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook = LookupTableHook::from_source(VecSource(Some(table)), observed).expect("hook");

        // Candidate calls the SAME call-site with DIFFERENT (re-keyed) args.
        let rekeyed_args = serde_json::json!({ "id": "pi_doubled" });
        let identity = explicit_identity("find_pi");
        let value = hook.try_replay_with_context(ReplayLookup {
            boundary: "storage",
            trait_name: "PaymentIntentInterface",
            method_name: "find_payment_intent_by_id",
            args: &rekeyed_args,
            callsite_identity: Some(&identity),
            caller_location: None,
        });
        assert_eq!(
            value, None,
            "a re-keyed call now misses instead of serving a stale value"
        );
        assert!(
            !handle.lock().unwrap()[0].resolved,
            "the miss is observed as unresolved (→ NovelCall + fail-stop at the seam)"
        );
    }

    #[test]
    fn lookup_table_hook_record_emits_observed_http_finalizer_only() {
        let empty = LookupTable {
            recording_id: "rec-1".to_owned(),
            policy_version: 1,
            entries: vec![],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook = LookupTableHook::from_source(VecSource(Some(empty)), observed).expect("hook");

        let mut non_finalizer = make_event(
            6,
            Some("corr-1"),
            "set_key",
            serde_json::json!({"k": "v"}),
            serde_json::json!({"ok": true}),
            false,
        );
        non_finalizer.boundary = "redis".to_owned();
        hook.record(non_finalizer);

        let mut finalizer = make_event(
            7,
            Some("corr-1"),
            "finalize",
            serde_json::json!({"path": "/payments/confirm"}),
            serde_json::json!({"status": 200}),
            false,
        );
        finalizer.boundary = "http_incoming".to_owned();
        finalizer.trait_name = "router_env::request".to_owned();
        finalizer.timestamp_ns = 1_000;
        finalizer.end_timestamp_ns = Some(12_345);
        finalizer.graph_node_id = Some(42);
        hook.record(finalizer);

        let calls = handle
            .lock()
            .map(|guard| guard.clone())
            .unwrap_or_else(|poisoned| poisoned.into_inner().clone());
        assert_eq!(calls.len(), 1, "non-finalizer records stay ignored");
        let call = &calls[0];
        assert_eq!(call.correlation_id.as_deref(), Some("corr-1"));
        assert_eq!(call.boundary, "http_incoming");
        assert_eq!(call.method_name, "finalize");
        assert!(!call.resolved);
        assert_eq!(call.timestamp_ns, 1_000);
        assert_eq!(call.end_timestamp_ns, Some(12_345));
        assert_eq!(call.graph_node_id, Some(42));
        assert_eq!(
            call.observed_result,
            Some(serde_json::json!({"status": 200}))
        );
        assert_eq!(call.recorded_result, None);
    }

    #[test]
    fn observed_call_lineage_and_wire_names_are_additive() {
        let legacy = serde_json::json!({
            "correlation_id": "corr-1",
            "boundary": "redis",
            "trait_name": "RedisStore",
            "method_name": "get_key",
            "args": {"key": "k"},
            "resolved": true,
            "recorded_result": "v",
            "observed_result": "v"
        });

        let call: ObservedCall =
            serde_json::from_value(legacy).expect("legacy observed call must deserialize");
        assert_eq!(call.task_id, None);
        assert_eq!(call.parent_task_id, None);
        assert_eq!(call.task_bucket, None);
        assert_eq!(call.span_path, None);
        assert_eq!(call.provenance, crate::Provenance::Recorded);

        let current = serde_json::json!({
            "correlation_id": "corr-2",
            "boundary": "db",
            "trait_name": "Store",
            "method_name": "find",
            "args": {"id": 1},
            "resolved": false,
            "task_id": "detached-7",
            "parent_task_id": "root",
            "task_bucket": "corr-2",
            "logical_context": "router>worker",
            "provenance": "execute_shadow"
        });
        let call: ObservedCall =
            serde_json::from_value(current).expect("current observed call must deserialize");
        assert_eq!(call.task_id.as_deref(), Some("detached-7"));
        assert_eq!(call.parent_task_id.as_deref(), Some("root"));
        assert_eq!(call.task_bucket.as_deref(), Some("corr-2"));
        assert_eq!(call.span_path.as_deref(), Some("router>worker"));
        assert_eq!(call.provenance, crate::Provenance::Shadow);

        let wire = serde_json::to_value(&call).unwrap();
        assert_eq!(
            wire.get("logical_context"),
            Some(&serde_json::json!("router>worker")),
            "span_path wire name stays pinned to logical_context"
        );
        assert!(wire.get("span_path").is_none());
        assert!(wire.get("logical_span_path").is_none());
        assert_eq!(
            wire.get("provenance"),
            Some(&serde_json::json!("execute_shadow")),
            "Shadow provenance wire name stays pinned to execute_shadow"
        );
    }

    #[test]
    fn observed_call_v7_lineage_deserializes_and_v8_serializes_bucket_alias() {
        let legacy_v7 = serde_json::json!({
            "correlation_id": "corr-legacy",
            "boundary": "redis",
            "trait_name": "RedisStore",
            "method_name": "get_key",
            "args": {"key": "k"},
            "resolved": true,
            "task_id": "detached-legacy",
            "parent_task_id": "root",
            "task_bucket": "legacy-bucket",
            "recorded_result": "v",
            "observed_result": "v"
        });
        let legacy_call: ObservedCall =
            serde_json::from_value(legacy_v7).expect("v7 observed call must deserialize");
        let legacy_wire = serde_json::to_value(&legacy_call).unwrap();
        assert_eq!(
            legacy_wire.get("bucket_id"),
            Some(&serde_json::json!("legacy-bucket")),
            "v7 task_bucket must be upgraded to the reader bucket_id"
        );
        assert_eq!(
            legacy_wire.get("task_bucket"),
            legacy_wire.get("bucket_id"),
            "legacy task_bucket must stay coherent after v7 fallback"
        );

        let current_v8 = serde_json::json!({
            "correlation_id": "corr-current",
            "boundary": "db",
            "trait_name": "Store",
            "method_name": "find",
            "args": {"id": 1},
            "resolved": false,
            "task_id": "detached-7",
            "parent_task_id": "root",
            "task_bucket": "detached-bucket-7",
            "bucket_id": "detached-bucket-7",
            "fork_seq": 7,
            "provenance": "execute_shadow"
        });
        let call: ObservedCall =
            serde_json::from_value(current_v8).expect("v8 observed call must deserialize");
        let wire = serde_json::to_value(&call).unwrap();

        assert_eq!(
            wire.get("bucket_id"),
            Some(&serde_json::json!("detached-bucket-7")),
            "v8 observed calls must serialize the new bucket_id"
        );
        assert_eq!(
            wire.get("task_bucket"),
            wire.get("bucket_id"),
            "legacy task_bucket must remain a coherent alias for bucket_id"
        );
        assert_eq!(
            wire.get("fork_seq"),
            Some(&serde_json::json!(7)),
            "v8 observed calls must serialize the fork sequence"
        );
    }

    // -----------------------------------------------------------------------
    // Declarative boundary model (#28) — the per-site `replay_strategy` knob.
    // -----------------------------------------------------------------------

    /// `replay_strategy_to_execute_mode`: `Execute`→Execute,
    /// `Substitute`→Lookup.
    #[test]
    fn replay_strategy_maps_to_execute_mode() {
        use crate::{ExecuteMode, ReplayStrategy};

        assert_eq!(
            replay_strategy_to_execute_mode(ReplayStrategy::Execute),
            ExecuteMode::Execute
        );
        assert_eq!(
            replay_strategy_to_execute_mode(ReplayStrategy::Substitute),
            ExecuteMode::Lookup
        );
    }

    /// Under the partial-function model, a DECLARED `Execute` site on any replay
    /// hook resolves to `Execute` directly. In record / no-op mode it is `Lookup`
    /// (Execute is a replay-only concept), gated by `hook.mode().is_replay()`.
    #[test]
    fn declared_execute_is_honored_on_any_replay_hook() {
        let empty = LookupTable {
            recording_id: "r".to_owned(),
            policy_version: 1,
            entries: vec![],
        };
        let hook =
            LookupTableHook::from_source(VecSource(Some(empty)), InMemoryObservedSink::new())
                .expect("hook");
        let spec = BoundarySpec::with_semantics(
            "redis",
            "Cache",
            "get",
            crate::BoundarySemantics {
                replay_strategy: crate::ReplayStrategy::Execute,
                kind: Some("redis".to_string()),
                declaration: Some(
                    crate::BoundaryDeclaration::default().effect(crate::EffectKind::Redis),
                ),
            },
        );
        // LookupTableHook is a replay hook (`mode().is_replay() == true`), so the
        // declared knob is honored directly.
        assert_eq!(
            boundary_execute_mode_for(&hook, &spec),
            crate::ExecuteMode::Execute,
            "a declared Execute site is honored on a replay hook"
        );
    }

    /// A DECLARED `Execute` site routes to Execute via the knob, and a DECLARED
    /// `Substitute` site stays Lookup.
    #[test]
    fn declared_knob_drives_routing() {
        let empty = LookupTable {
            recording_id: "r".to_owned(),
            policy_version: 1,
            entries: vec![],
        };
        let hook =
            LookupTableHook::from_source(VecSource(Some(empty)), InMemoryObservedSink::new())
                .expect("hook");

        let execute = BoundarySpec::with_semantics(
            "redis",
            "Cache",
            "get_key",
            crate::BoundarySemantics {
                replay_strategy: crate::ReplayStrategy::Execute,
                kind: Some("redis".to_string()),
                declaration: Some(
                    crate::BoundaryDeclaration::default().effect(crate::EffectKind::Redis),
                ),
            },
        );
        assert_eq!(
            boundary_execute_mode_for(&hook, &execute),
            crate::ExecuteMode::Execute,
            "declared Execute → Execute"
        );

        // A declared Substitute site stays Lookup.
        let substitute = BoundarySpec::with_semantics(
            "redis",
            "Cache",
            "get_ttl",
            crate::BoundarySemantics {
                replay_strategy: crate::ReplayStrategy::Substitute,
                kind: Some("redis".to_string()),
                declaration: Some(
                    crate::BoundaryDeclaration::default().effect(crate::EffectKind::Redis),
                ),
            },
        );
        assert_eq!(
            boundary_execute_mode_for(&hook, &substitute),
            crate::ExecuteMode::Lookup,
            "declared Substitute → Lookup"
        );
    }

    /// REGRESSION (#28 eu-overcharge): the GLOBAL replay path wraps the
    /// `LookupTableHook` in a `RuntimeHook::LookupReplay`, and `dispatch`'s
    /// execute-mode gate is `boundary_execute_mode(spec)` ==
    /// `boundary_execute_mode_for(&*global_hook, spec)` where `global_hook` is that
    /// `RuntimeHook`. A declared `replay_strategy = Execute` boundary must resolve
    /// to `Execute` THROUGH the `RuntimeHook` wrapper.
    #[test]
    fn declared_execute_routes_through_runtime_hook_wrapper() {
        let empty = LookupTable {
            recording_id: "r".to_owned(),
            policy_version: 1,
            entries: vec![],
        };
        let inner =
            LookupTableHook::from_source(VecSource(Some(empty)), InMemoryObservedSink::new())
                .expect("hook");
        // The SAME wrapper the global replay path installs.
        let runtime = crate::RuntimeHook::LookupReplay(inner);

        // The eu_settlement_read site: declared Execute, no kind label (the explicit
        // `replay_strategy = Execute` without a preset → kind None).
        let spec = BoundarySpec::with_semantics(
            "redis",
            "router::eu_settlement",
            "eu_settlement_read",
            crate::BoundarySemantics {
                replay_strategy: crate::ReplayStrategy::Execute,
                kind: None,
                declaration: None,
            },
        );
        assert_eq!(
            boundary_execute_mode_for(&runtime, &spec),
            crate::ExecuteMode::Execute,
            "a declared Execute boundary must EXECUTE through the RuntimeHook wrapper"
        );

        // A second inner hook through the SAME wrapper also honors the declared
        // Execute. The wrapper is a replay hook, so `mode().is_replay()` is true
        // and the knob wins.
        let inner_default = LookupTableHook::from_source(
            VecSource(Some(LookupTable {
                recording_id: "r".to_owned(),
                policy_version: 1,
                entries: vec![],
            })),
            InMemoryObservedSink::new(),
        )
        .expect("hook");
        let runtime_default = crate::RuntimeHook::LookupReplay(inner_default);
        assert_eq!(
            boundary_execute_mode_for(&runtime_default, &spec),
            crate::ExecuteMode::Execute,
            "declared Execute is honored through the wrapper"
        );
    }

    // -----------------------------------------------------------------------
    // Seed-plan pipeline tests (deliverables 1-4, 6) — all PURE, no docker.
    // -----------------------------------------------------------------------

    /// Build a State event with explicit read/write sets, boundary, and seq.
    #[allow(clippy::too_many_arguments)]
    fn state_event(
        global_seq: u64,
        correlation_id: Option<&str>,
        boundary: &str,
        method: &str,
        args: serde_json::Value,
        result: serde_json::Value,
        read_set: &[&str],
        write_set: &[&str],
        is_error: bool,
    ) -> BoundaryEvent {
        let mut ev = make_event(global_seq, correlation_id, method, args, result, is_error);
        ev.global_sequence = global_seq;
        ev.boundary = boundary.into();
        ev.read_set = read_set.iter().map(|s| (*s).to_owned()).collect();
        ev.write_set = write_set.iter().map(|s| (*s).to_owned()).collect();
        ev
    }

    fn test_db_query_key(operation: &str, table: &str, sql: &str) -> String {
        db_query_state_key(operation, table, sql, &serde_json::Value::Null).to_wire()
    }

    #[test]
    fn state_key_v1_roundtrips_and_legacy_is_opaque() {
        let key = StateKey::DbRow {
            table: "payment_intent".to_owned(),
            pk_column: "payment_id".to_owned(),
            pk_value: "pay_123".to_owned(),
        };
        let wire = key.to_wire();
        assert_eq!(StateKey::parse(&wire).unwrap(), key);
        let old_three_field = format!(
            "{STATE_KEY_V1_PREFIX}:db_row:{}:{}",
            encode_hex_component("payment_attempt"),
            encode_hex_component("pay_123")
        );
        assert_eq!(
            StateKey::parse(&old_three_field).unwrap(),
            StateKey::Opaque(old_three_field),
            "old typed-v1 row keys without pk_column are advisory-only opaque keys"
        );
        assert_eq!(
            StateKey::parse("payment_intent:SELECT * FROM payment_intent")
                .unwrap()
                .db_table(),
            None,
            "legacy table:sql strings stay opaque"
        );
    }

    #[test]
    fn db_row_state_keys_use_only_known_unique_pk_columns() {
        let keys = db_row_state_keys(
            "payment_attempt",
            &serde_json::json!([
                {"attempt_id": "att_1"},
                {"payment_id": "pay_1"},
                {"merchant_id": "merch_1"},
                {"id": 42}
            ]),
        );
        assert_eq!(
            keys,
            vec![StateKey::DbRow {
                table: "payment_attempt".to_owned(),
                pk_column: "attempt_id".to_owned(),
                pk_value: "att_1".to_owned(),
            }],
            "payment_attempt must group only by its real PK, not any *_id column"
        );
        assert!(
            db_row_state_key("unknown_table", &serde_json::json!({"id": 42})).is_none(),
            "unknown tables fall back to DbQuery instead of assuming `id` is unique"
        );
    }

    #[test]
    fn db_result_envelopes_emit_non_id_row_keys_and_keep_query_fallback() {
        let users_query = db_query_state_key(
            "find_user_by_id",
            "users",
            "SELECT * FROM \"users\" WHERE \"user_id\" = $1",
            &serde_json::json!(["user_123"]),
        )
        .to_wire();
        let users_rows = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": {
                "id": "not-the-user-pk",
                "user_id": "user_123",
                "merchant_id": "merch_ignored"
            },
            "type_name": "User"
        });
        let user_row_keys = db_row_state_keys("users", &users_rows);

        assert_eq!(
            user_row_keys,
            vec![StateKey::DbRow {
                table: "users".to_owned(),
                pk_column: "user_id".to_owned(),
                pk_value: "user_123".to_owned(),
            }],
            "users must key by user_id, never by the generic id field or merchant_id"
        );

        let merchant_rows = serde_json::json!({
            "version": 1,
            "result": "Ok",
            "value": [
                {
                    "merchant_id": "merch_123",
                    "key": {"inner": [1, 2, 3]}
                }
            ],
            "type_name": "Vec<MerchantKeyStore>"
        });
        assert_eq!(
            db_row_state_keys("merchant_key_store", &merchant_rows),
            vec![StateKey::DbRow {
                table: "merchant_key_store".to_owned(),
                pk_column: "merchant_id".to_owned(),
                pk_value: "merch_123".to_owned(),
            }],
            "merchant_key_store must key by merchant_id even though the table has no id column"
        );

        let mut read_set = vec![users_query.clone()];
        read_set.extend(user_row_keys.into_iter().map(|key| key.to_wire()));
        assert!(
            read_set.contains(&users_query),
            "row-exact keys augment the query fallback instead of replacing it"
        );
        assert!(
            read_set.iter().any(|key| matches!(
                StateKey::parse(key).unwrap(),
                StateKey::DbRow {
                    ref table,
                    ref pk_column,
                    ref pk_value
                } if table == "users" && pk_column == "user_id" && pk_value == "user_123"
            )),
            "augmented read_set must carry the exact users row key"
        );
    }

    /// Explicit read-set captures seed their recorded result on any boundary
    /// name. Events with no read capture seed nothing.
    #[test]
    fn seed_plan_built_from_explicit_read_set_and_result() {
        let events = vec![
            state_event(
                0,
                Some("c1"),
                "custom_store",
                "arbitrary_fetch",
                serde_json::json!(["k"]),
                serde_json::json!("v"),
                &["k"],
                &[],
                false,
            ),
            state_event(
                1,
                Some("c1"),
                "http_client",
                "request",
                serde_json::json!(["url"]),
                serde_json::json!({"status": 200}),
                &[],
                &[],
                false,
            ),
        ];

        let plan = build_seed_plan(&events, Some("c1"));
        assert_eq!(plan.len(), 1);
        assert_eq!(
            plan.resolve("custom_store", "k").unwrap().value,
            serde_json::json!("v")
        );

        assert!(
            !plan.contains("http_client", "url"),
            "an event without an explicit read_set seeds nothing"
        );
        assert_eq!(
            plan.resolve("custom_store", "k").unwrap().origin,
            SeedOrigin::Recording
        );
    }

    /// A DB read+write on the same key is an RMW precondition: seed planning must
    /// use the explicit pre-image when present, never the post-write image. A
    /// read-only DB row event uses the result image as its materialization image.
    #[test]
    fn seed_plan_prefers_pre_image_for_rmw_and_result_image_for_read_only_db_rows() {
        let update_key = StateKey::DbRow {
            table: "merchant_account".to_owned(),
            pk_column: "merchant_id".to_owned(),
            pk_value: "m1".to_owned(),
        }
        .to_wire();
        let read_key = StateKey::DbRow {
            table: "users".to_owned(),
            pk_column: "user_id".to_owned(),
            pk_value: "u1".to_owned(),
        }
        .to_wire();
        let pre_image = serde_json::json!({"merchant_id": "m1", "status": "before"});
        let post_image = serde_json::json!({"merchant_id": "m1", "status": "after"});
        let read_image = serde_json::json!({"user_id": "u1", "email": "u1@example.test"});

        let mut rmw = state_event(
            0,
            Some("c1"),
            "db",
            "generic_update_with_results",
            serde_json::json!({"table": "merchant_account"}),
            serde_json::json!({"merchant_id": "m1", "status": "after"}),
            &[update_key.as_str()],
            &[update_key.as_str()],
            false,
        );
        rmw.pre_image = Some(pre_image.clone());
        rmw.result_image = Some(post_image);

        let mut read_only = state_event(
            1,
            Some("c1"),
            "db",
            "generic_find_one_core",
            serde_json::json!({"table": "users"}),
            serde_json::json!({"user_id": "u1", "email": "legacy@example.test"}),
            &[read_key.as_str()],
            &[],
            false,
        );
        read_only.result_image = Some(read_image.clone());

        let plan = build_seed_plan(&[rmw, read_only], Some("c1"));
        let update_seed = plan
            .resolve("db", update_key.as_str())
            .expect("self-referential UPDATE must seed the row it mutates");
        assert_eq!(
            update_seed.image,
            Some(pre_image),
            "RMW seeds must materialize the pre-image, not the post-write result image"
        );
        let read_seed = plan
            .resolve("db", read_key.as_str())
            .expect("read-only DB row must seed");
        assert_eq!(
            read_seed.image,
            Some(read_image),
            "read-only DB row seeds should use the explicit result image"
        );
    }

    #[test]
    fn seed_plan_does_not_use_result_image_for_rmw_without_pre_image() {
        let update_key = StateKey::DbRow {
            table: "merchant_account".to_owned(),
            pk_column: "merchant_id".to_owned(),
            pk_value: "m2".to_owned(),
        }
        .to_wire();
        let mut rmw = state_event(
            0,
            Some("c1"),
            "db",
            "generic_update_with_results",
            serde_json::json!({"table": "merchant_account"}),
            serde_json::json!({"merchant_id": "m2", "status": "after"}),
            &[update_key.as_str()],
            &[update_key.as_str()],
            false,
        );
        rmw.result_image = Some(serde_json::json!({"merchant_id": "m2", "status": "after"}));

        let plan = build_seed_plan(&[rmw], Some("c1"));
        let update_seed = plan
            .resolve("db", update_key.as_str())
            .expect("self-referential UPDATE still seeds through legacy value");
        assert_eq!(
            update_seed.image, None,
            "RMW without a pre-image must not fall back to a post-write result image"
        );
        assert_eq!(
            update_seed.value,
            serde_json::json!({"merchant_id": "m2", "status": "after"}),
            "legacy raw value remains available for old tapes"
        );
    }

    /// But an UPDATE of a table this correlation explicitly declared as created
    /// is NOT seeded — it reconstructs its own rows via the replayed CREATE, so
    /// create-then-update of the same table stays unaffected by the reorder.
    #[test]
    fn seed_plan_skips_update_of_explicitly_declared_created_table() {
        let insert_key = test_db_query_key(
            "generic_insert",
            "payment_intent",
            "INSERT INTO \"payment_intent\" VALUES (…) ",
        );
        let update_key = test_db_query_key(
            "generic_update_with_results",
            "payment_intent",
            "UPDATE \"payment_intent\" SET \"status\" = $1",
        );
        let mut create = state_event(
            0,
            Some("c1"),
            "db",
            "generic_insert",
            serde_json::json!({"table": "payment_intent"}),
            serde_json::json!([{"id": "p1"}]),
            &[],
            &[insert_key.as_str()],
            false,
        );
        create.declaration =
            Some(crate::BoundaryDeclaration::default().operation(crate::OperationKind::Create));
        let update = state_event(
            1,
            Some("c1"),
            "db",
            "generic_update_with_results",
            serde_json::json!({"table": "payment_intent"}),
            serde_json::json!([{"id": "p1"}]),
            &[update_key.as_str()],
            &[update_key.as_str()],
            false,
        );

        let plan = build_seed_plan(&[create, update], Some("c1"));
        assert!(
            !plan.contains("db", update_key.as_str()),
            "an UPDATE of an explicitly-created table must not seed (reconstructed via CREATE)"
        );
    }

    #[test]
    fn seed_plan_declared_create_masks_later_db_reads_without_insert_name() {
        let create_key = test_db_query_key(
            "persist_payment_intent",
            "payment_intent",
            "INSERT INTO \"payment_intent\" VALUES (…) ",
        );
        let read_key = test_db_query_key(
            "find_payment_intent_by_id",
            "payment_intent",
            "SELECT * FROM \"payment_intent\" WHERE id = $1",
        );
        let mut create = state_event(
            0,
            Some("c1"),
            "db",
            "persist_payment_intent",
            serde_json::json!({"table": "payment_intent"}),
            serde_json::json!([{"id": "p1"}]),
            &[],
            &[create_key.as_str()],
            false,
        );
        create.declaration =
            Some(crate::BoundaryDeclaration::default().operation(crate::OperationKind::Create));
        let read_back = state_event(
            1,
            Some("c1"),
            "db",
            "find_payment_intent_by_id",
            serde_json::json!({"table": "payment_intent"}),
            serde_json::json!([{"id": "p1"}]),
            &[read_key.as_str()],
            &[],
            false,
        );

        let plan = build_seed_plan(&[create, read_back], Some("c1"));

        assert!(
            !plan.contains("db", read_key.as_str()),
            "a declared Create masks later DB reads of the created table even when the method name lacks `insert`"
        );
    }

    #[test]
    fn seed_plan_legacy_insert_without_declaration_no_longer_masks_later_db_reads() {
        let insert_key = "payment_intent:INSERT INTO \"payment_intent\" VALUES (…)";
        let read_key = "payment_intent:SELECT * FROM \"payment_intent\" WHERE id = $1";
        let insert = state_event(
            0,
            Some("c1"),
            "db",
            "generic_insert",
            serde_json::json!({"table": "payment_intent"}),
            serde_json::json!([{"id": "p1"}]),
            &[],
            &[insert_key],
            false,
        );
        let read_back = state_event(
            1,
            Some("c1"),
            "db",
            "find_payment_intent_by_id",
            serde_json::json!({"table": "payment_intent"}),
            serde_json::json!([{"id": "p1"}]),
            &[read_key],
            &[],
            false,
        );

        let plan = build_seed_plan(&[insert, read_back], Some("c1"));

        assert!(
            plan.contains("db", read_key),
            "an undeclared legacy insert no longer uses method-name fallback masking"
        );
    }

    #[test]
    fn seed_plan_declared_non_create_does_not_mask_later_db_reads() {
        let write_key = test_db_query_key(
            "generic_insert_named_but_declared_update",
            "payment_intent",
            "UPDATE \"payment_intent\" SET \"status\" = $1",
        );
        let read_key = test_db_query_key(
            "find_payment_intent_by_id",
            "payment_intent",
            "SELECT * FROM \"payment_intent\" WHERE id = $1",
        );
        let mut update = state_event(
            0,
            Some("c1"),
            "db",
            "generic_insert_named_but_declared_update",
            serde_json::json!({"table": "payment_intent"}),
            serde_json::json!([{"id": "p1", "status": "requires_capture"}]),
            &[],
            &[write_key.as_str()],
            false,
        );
        update.declaration =
            Some(crate::BoundaryDeclaration::default().operation(crate::OperationKind::Update));
        let later_read = state_event(
            1,
            Some("c1"),
            "db",
            "find_payment_intent_by_id",
            serde_json::json!({"table": "payment_intent"}),
            serde_json::json!([{"id": "p1", "status": "requires_capture"}]),
            &[read_key.as_str()],
            &[],
            false,
        );

        let plan = build_seed_plan(&[update, later_read], Some("c1"));

        assert!(
            plan.contains("db", read_key.as_str()),
            "a declared non-Create must not use the legacy `insert` method-name fallback"
        );
    }

    /// Method names do not classify reads or writes: explicit fields do. A
    /// read_set on a write-named method seeds, while a write_set on a read-named
    /// method only marks the key mutated.
    #[test]
    fn seed_plan_uses_explicit_sets_not_method_names() {
        let read_named_write = state_event(
            0,
            Some("c1"),
            "custom_store",
            "get_key",
            serde_json::json!(["mutated"]),
            serde_json::json!("after"),
            &[],
            &["mutated"],
            false,
        );
        let write_named_read = state_event(
            1,
            Some("c1"),
            "custom_store",
            "set_key",
            serde_json::json!(["precondition"]),
            serde_json::json!("before"),
            &["precondition"],
            &[],
            false,
        );
        let post_write_read = state_event(
            2,
            Some("c1"),
            "custom_store",
            "get_key",
            serde_json::json!(["mutated"]),
            serde_json::json!("after"),
            &["mutated"],
            &[],
            false,
        );

        let plan = build_seed_plan(
            &[read_named_write, write_named_read, post_write_read],
            Some("c1"),
        );

        assert_eq!(
            plan.resolve("custom_store", "precondition").unwrap().value,
            serde_json::json!("before"),
            "explicit read_set seeds even when the method name looks like a write"
        );
        assert!(
            !plan.contains("custom_store", "mutated"),
            "explicit write_set prevents a later read_set for the same key from seeding"
        );
    }

    #[test]
    fn seed_plan_skips_declared_redis_raw_null_without_op_metadata() {
        let mut redis_null = state_event(
            0,
            Some("c1"),
            "redis",
            "get_key",
            serde_json::json!({"key": "merchant_key_store_default"}),
            serde_json::json!("Null"),
            &["merchant_key_store_default"],
            &[],
            false,
        );
        redis_null.declaration =
            Some(crate::BoundaryDeclaration::default().effect(EffectKind::Redis));

        let mut redis_string_null = state_event(
            1,
            Some("c1"),
            "redis",
            "get_key",
            serde_json::json!({"key": "literal_null_string"}),
            serde_json::json!({"String": "Null"}),
            &["literal_null_string"],
            &[],
            false,
        );
        redis_string_null.declaration =
            Some(crate::BoundaryDeclaration::default().effect(EffectKind::Redis));

        let custom_null = state_event(
            2,
            Some("c1"),
            "custom_store",
            "get_key",
            serde_json::json!({"key": "custom"}),
            serde_json::json!("Null"),
            &["custom"],
            &[],
            false,
        );

        let plan = build_seed_plan(&[redis_null, redis_string_null, custom_null], Some("c1"));

        assert!(
            !plan.contains("redis", "merchant_key_store_default"),
            "current Redis raw null (`DejaRedisValue::Null` => \"Null\") must materialize as absence, not a literal string"
        );
        assert_eq!(
            plan.resolve("redis", "literal_null_string").unwrap().value,
            serde_json::json!({"String": "Null"}),
            "a real Redis string named Null remains seedable through the tagged enum shape"
        );
        assert_eq!(
            plan.resolve("custom_store", "custom").unwrap().value,
            serde_json::json!("Null"),
            "non-Redis string values named Null are not reinterpreted"
        );
    }

    /// Deliverable 1: a read AFTER the correlation wrote a key reflects the
    /// mutation, not the precondition — so the FIRST (pre-write) read wins and a
    /// post-write read never overwrites the seed.
    #[test]
    fn seed_plan_first_pre_write_read_wins() {
        let events = vec![
            state_event(
                0,
                Some("c1"),
                "custom_store",
                "load",
                serde_json::json!(["k"]),
                serde_json::json!("before"),
                &["k"],
                &[],
                false,
            ),
            state_event(
                1,
                Some("c1"),
                "custom_store",
                "store",
                serde_json::json!(["k", "after"]),
                serde_json::json!("after"),
                &[],
                &["k"],
                false,
            ),
            // Read-after-write: returns the mutated value, must NOT reseed.
            state_event(
                2,
                Some("c1"),
                "custom_store",
                "load",
                serde_json::json!(["k"]),
                serde_json::json!("after"),
                &["k"],
                &[],
                false,
            ),
        ];
        let plan = build_seed_plan(&events, Some("c1"));
        assert_eq!(
            plan.resolve("custom_store", "k").unwrap().value,
            serde_json::json!("before"),
            "the precondition is the pre-write value, not the post-write read"
        );
    }

    /// Deliverable 1: an errored read carries no value and must not seed.
    #[test]
    fn seed_plan_skips_error_reads() {
        let events = vec![state_event(
            0,
            Some("c1"),
            "redis",
            "get",
            serde_json::json!(["k"]),
            serde_json::json!({"error": "boom"}),
            &["k"],
            &[],
            true,
        )];
        let plan = build_seed_plan(&events, Some("c1"));
        assert!(plan.is_empty(), "an error read seeds nothing");
    }

    #[test]
    fn seed_plan_skips_structural_miss_reads_without_null_string_magic() {
        let mut optional_none = state_event(
            2,
            Some("c1"),
            "redis",
            "get_key",
            serde_json::json!(["optional_missing"]),
            serde_json::json!({"Ok": null}),
            &["optional_missing"],
            &[],
            false,
        );
        optional_none.declaration =
            Some(crate::BoundaryDeclaration::default().returns(crate::ReturnSemantics::Optional));
        let events = vec![
            state_event(
                0,
                Some("c1"),
                "redis",
                "get_key",
                serde_json::json!(["legacy_null_string"]),
                serde_json::json!("Null"),
                &["legacy_null_string"],
                &[],
                false,
            ),
            state_event(
                1,
                Some("c1"),
                "db",
                "find_x",
                serde_json::json!(["json_null_absent"]),
                serde_json::Value::Null,
                &["json_null_absent"],
                &[],
                false,
            ),
            optional_none,
            // a real HIT alongside, to prove only structural misses are skipped
            state_event(
                3,
                Some("c1"),
                "redis",
                "get_key",
                serde_json::json!(["present"]),
                serde_json::json!({"String": "v"}),
                &["present"],
                &[],
                false,
            ),
        ];
        let plan = build_seed_plan(&events, Some("c1"));
        assert!(
            plan.classify_read("redis", "legacy_null_string")
                .is_reconstructable(),
            "the legacy string \"Null\" is no longer a magic miss sentinel"
        );
        assert_eq!(
            plan.resolve("redis", "legacy_null_string").unwrap().value,
            serde_json::json!("Null")
        );
        assert!(
            !plan
                .classify_read("db", "json_null_absent")
                .is_reconstructable(),
            "JSON null is a structural miss and must not be seeded"
        );
        assert!(
            !plan
                .classify_read("redis", "optional_missing")
                .is_reconstructable(),
            "an explicitly Optional successful null is a structural miss"
        );
        assert!(
            plan.classify_read("redis", "present").is_reconstructable(),
            "a real hit IS seeded"
        );
    }

    /// Deliverable 1: correlation isolation — only the requested case's reads
    /// build the plan.
    #[test]
    fn seed_plan_is_correlation_scoped() {
        let events = vec![
            state_event(
                0,
                Some("c1"),
                "redis",
                "get",
                serde_json::json!(["k"]),
                serde_json::json!("c1val"),
                &["k"],
                &[],
                false,
            ),
            state_event(
                1,
                Some("c2"),
                "redis",
                "get",
                serde_json::json!(["k"]),
                serde_json::json!("c2val"),
                &["k"],
                &[],
                false,
            ),
        ];
        let plan = build_seed_plan(&events, Some("c1"));
        assert_eq!(plan.len(), 1);
        assert_eq!(
            plan.resolve("redis", "k").unwrap().value,
            serde_json::json!("c1val")
        );
    }

    /// Deliverable 3: a key in the plan classifies as Reconstructable; a key
    /// neither in the plan nor the template is a seed-gap (never served stale).
    #[test]
    fn classify_read_flags_seed_gap() {
        let events = vec![state_event(
            0,
            Some("c1"),
            "redis",
            "get",
            serde_json::json!(["known"]),
            serde_json::json!("v"),
            &["known"],
            &[],
            false,
        )];
        let plan = build_seed_plan(&events, Some("c1"));

        let hit = plan.classify_read("redis", "known");
        assert!(hit.is_reconstructable());
        match hit {
            ReadClassification::Reconstructable { value, origin } => {
                assert_eq!(value, serde_json::json!("v"));
                assert_eq!(origin, SeedOrigin::Recording);
            }
            _ => panic!("expected reconstructable"),
        }

        let gap = plan.classify_read("redis", "never_seen");
        assert!(!gap.is_reconstructable());
        assert!(matches!(gap, ReadClassification::NotReconstructable { .. }));
    }

    /// Deliverable 4: an ambient template resolves a diverged read to a config
    /// key the recording never observed — turning a would-be seed-gap into a
    /// reconstructable read.
    #[test]
    fn ambient_template_resolves_config_key() {
        // Recording only observed the DEFAULT rate.
        let events = vec![state_event(
            0,
            Some("c1"),
            "redis",
            "get",
            serde_json::json!(["settlement_rate_default"]),
            serde_json::json!("0.10"),
            &["settlement_rate_default"],
            &[],
            false,
        )];
        let plan = build_seed_plan(&events, Some("c1"));

        // Without the template the premium key is a seed-gap.
        assert!(!plan
            .classify_read("redis", "settlement_rate_premium")
            .is_reconstructable());

        // Merge the demo ambient defaults.
        let plan = plan.with_ambient(&AmbientTemplate::demo_defaults());
        let resolved = plan.classify_read("redis", "settlement_rate_premium");
        match resolved {
            ReadClassification::Reconstructable { value, origin } => {
                assert_eq!(value, serde_json::json!("0.20"));
                assert_eq!(origin, SeedOrigin::Ambient);
            }
            _ => panic!("ambient template should resolve the premium key"),
        }
    }

    /// Deliverable 4: a recording-derived precondition wins over an ambient
    /// default for the SAME key (ambient never clobbers what was observed).
    #[test]
    fn ambient_does_not_clobber_recording() {
        let events = vec![state_event(
            0,
            Some("c1"),
            "redis",
            "get",
            serde_json::json!(["settlement_rate_premium"]),
            serde_json::json!("0.15"),
            &["settlement_rate_premium"],
            &[],
            false,
        )];
        let plan =
            build_seed_plan(&events, Some("c1")).with_ambient(&AmbientTemplate::demo_defaults());
        assert_eq!(
            plan.resolve("redis", "settlement_rate_premium")
                .unwrap()
                .value,
            serde_json::json!("0.15"),
            "the observed value (0.15) wins over the ambient default (0.20)"
        );
        assert_eq!(
            plan.resolve("redis", "settlement_rate_premium")
                .unwrap()
                .origin,
            SeedOrigin::Recording
        );
    }

    /// Deliverable 4: the ambient template parses from a TSV file body, JSON-
    /// typing values (so the demo's config can live in a file).
    #[test]
    fn ambient_template_from_tsv() {
        let body = "\
# demo ambient config
redis\tsettlement_rate_premium\t0.20
redis\tcurrency\tusd
";
        let template = AmbientTemplate::from_tsv(body);
        assert_eq!(template.entries().len(), 2);
        let plan = SeedPlan::new().with_ambient(&template);
        assert_eq!(
            plan.resolve("redis", "settlement_rate_premium")
                .unwrap()
                .value,
            serde_json::json!(0.20),
            "0.20 parses as a JSON number"
        );
        assert_eq!(
            plan.resolve("redis", "currency").unwrap().value,
            serde_json::json!("usd"),
            "bare token becomes a JSON string"
        );
    }

    /// The execute-shadow path on the concrete `LookupTableHook` resolves the
    /// recorded baseline at peek (advancing the stampers in lock-step with the
    /// lookup path) and stamps the REAL boundary result at observe, emitting one
    /// `Shadow` observation whose `recorded_result`/`observed_result` are
    /// what the post-hoc divergence tally diffs.
    #[test]
    fn hook_execute_shadow_emits_observation_with_real_result() {
        let table = LookupTable {
            recording_id: "rec-shadow".to_owned(),
            policy_version: 1,
            entries: vec![entry_with(
                None,
                Address::Sequence {
                    boundary: "redis".to_owned(),
                    method: "incr".to_owned(),
                    request_sequence: 0,
                },
                &serde_json::json!(["counter"]),
                0,
                serde_json::json!(2), // recorded baseline result
                7,
            )],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        let args = serde_json::json!(["counter"]);
        let query = ReplayLookup {
            boundary: "redis",
            trait_name: "RedisStore",
            method_name: "incr",
            args: &args,
            callsite_identity: None,
            caller_location: None,
        };

        let token = hook.execute_shadow_peek(query).expect("peek token");
        // The macro would now run the real op; hand its fresh result to observe.
        DejaHook::execute_shadow_observe(&hook, token, serde_json::json!(3));

        let calls = handle.lock().unwrap();
        assert_eq!(calls.len(), 1, "one shadow observation emitted");
        let call = &calls[0];
        assert_eq!(call.provenance, crate::Provenance::Shadow);
        assert_eq!(call.recorded_result, Some(serde_json::json!(2)));
        assert_eq!(call.observed_result, Some(serde_json::json!(3)));
    }

    /// REGRESSION (#28 extra-call): a declared-`Execute` boundary CALL with NO
    /// recorded counterpart (a novel call the recording never had) must emit an
    /// observation the post-hoc tally classifies as a NovelCall divergence —
    /// `resolved == false` AND `seed_gap == false` — NOT a swallowed
    /// InconclusiveSeedGap. Under #28 the execute-shadow peek set
    /// `seed_gap = recorded_result.is_none()`, so a novel Execute call was emitted
    /// with `seed_gap = true` and the tally treated it as non-blocking — masking
    /// the extra-call catch. The fix mirrors #26: a baseline-less Execute call
    /// surfaces as NovelCall (the tally's final branch fires when there is no
    /// recorded twin to pair against).
    #[test]
    fn novel_execute_call_surfaces_as_novel_not_swallowed_seed_gap() {
        // Empty table → NO recorded baseline for the candidate's extra call.
        let table = LookupTable {
            recording_id: "rec-novel".to_owned(),
            policy_version: 1,
            entries: vec![],
        };
        let observed = InMemoryObservedSink::new();
        let handle = observed.handle();
        let hook =
            LookupTableHook::from_source(VecSource(Some(table)), observed).expect("from_source");

        let args = serde_json::json!(["extra_key"]);
        let query = ReplayLookup {
            boundary: "db",
            trait_name: "PI",
            method_name: "generic_find_one_core",
            args: &args,
            callsite_identity: None,
            caller_location: None,
        };

        // Declared-Execute boundary routes to execute; peek returns a token even
        // with no baseline (the real boundary still runs), then observe emits.
        let token = hook.execute_shadow_peek(query).expect("peek token");
        DejaHook::execute_shadow_observe(&hook, token, serde_json::json!({"id": "fresh"}));

        let calls = handle.lock().unwrap();
        assert_eq!(calls.len(), 1, "one observation emitted for the novel call");
        let call = &calls[0];
        assert!(
            !call.resolved,
            "a novel call has no recorded baseline → resolved == false"
        );
        assert_eq!(
            call.recorded_result, None,
            "no recorded baseline for a novel call"
        );
        assert!(
            !call.seed_gap,
            "a novel call (no recorded event) must NOT be flagged seed_gap — it is a \
             NovelCall, not a swallowed InconclusiveSeedGap (the extra-call regression)"
        );
    }
}
