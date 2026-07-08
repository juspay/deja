//! Post-hoc divergence detector + scorecard renderer (V1 full mock).
//!
//! Consumes three artifacts produced during a replay run and reconciles the
//! orchestrator's model of what SHOULD have happened (the lookup table, itself
//! rendered from the recording) with what the candidate ACTUALLY did (its
//! `ObservedCall` stream) and how its HTTP responses compared (the kernel's
//! `HttpDiff` stream):
//!
//!   - lookup table   → `HarnessRoot::lookup_table_path(run_id)`
//!   - observed calls → `HarnessRoot::observed_path(run_id)`
//!   - http diffs     → `HarnessRoot::http_diff_path(run_id)`
//!
//! Classification (V1):
//!   - resolved hit                         → matched (recorded per address rank)
//!   - resolved only at rank 6 (sequence)   → Recovered (fragility flag)
//!   - candidate call with no table hit     → NovelCall (blocking)
//!     …on an egress boundary               → EnvironmentalMiss (tolerated)
//!   - table entry the candidate never hit  → OmittedCall (blocking)
//!   - http status / body diffs             → StatusMismatch / BodyMismatch
//!
//! V1 is "full mock": the table is the complete source of truth, containers are
//! empty, and a miss is a divergence — never a legitimate data source. The
//! tiered miss strategy (seeded containers, synthesis, content-addressed
//! fallback) is deferred future work. The
//! `synthesized` / `real_impl_will_fail` fields on `ObservedCall` are the inert
//! scaffold for that work and are always false here.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;

use deja::{Address, LocalFileLookupSource, LookupTable, LookupTableSource, ObservedCall};
use deja_kernel::{HttpDiff, JsonFieldDiff};
use serde::{Deserialize, Serialize};

use crate::HarnessRoot;

pub mod ledger;
pub use ledger::CallRecord;

/// Boundaries whose live calls cannot run in the harness (egress is blocked).
/// A *novel* call here is an `EnvironmentalMiss`, never a candidate bug.
fn tier_for(boundary: &str) -> Tier {
    match boundary {
        "http_outgoing" | "http_client" | "grpc" => Tier::Environmental,
        "redis" | "db" | "database" | "storage" | "pg" => Tier::Stateful,
        "time" | "id" | "id_generation" | "uuid" | "rng" => Tier::Pure,
        _ => Tier::Unknown,
    }
}

/// A boundary whose recorded-vs-replayed mismatch is NOT a real divergence and so
/// must not block the verdict:
///   - `Tier::Pure` (time/id/rng): an entropy SEAM whose recorded value is
///     substituted on replay, after which everything downstream is pure. These are
///     fully substituted in practice (they never miss), so the non-blocking status
///     is a safety net, not a load-bearing exclusion.
///   - `http_incoming`: the request boundary the kernel re-drives by construction,
///     not a side effect at all.
///
/// NB there is deliberately no `crypto` tier. Crypto is pure computation, not a
/// seam: its only entropy is the AEAD nonce, recorded at its own seam
/// (`common_utils::crypto::NonceSequence::new`), so AES reproduces byte-identically
/// when run live. It carries no boundary and therefore needs no exclusion — see the
/// note on `crypto_operation` in `hyperswitch_domain_models::type_encryption`.
fn is_nonblocking_boundary(boundary: &str) -> bool {
    tier_for(boundary) == Tier::Pure || boundary == "http_incoming"
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Tier {
    Environmental,
    Stateful,
    Pure,
    Unknown,
}

impl Tier {
    fn label(self) -> &'static str {
        match self {
            Tier::Environmental => "environmental",
            Tier::Stateful => "stateful",
            Tier::Pure => "pure",
            Tier::Unknown => "unknown",
        }
    }
}

fn rank_label(rank: u8) -> String {
    format!("rank_{rank}")
}

/// The weakest, positional `Address` rank (`Address::Sequence`) — a match here
/// means the call resolved only by its boundary+method+request-sequence position,
/// which is fragile to any upstream reorder. Tracked as "Recovered" (a fragility
/// signal), not a divergence. MUST equal `Address::Sequence`'s `rank()`; bump this
/// in lock-step if the rank ladder is renumbered again.
const POSITIONAL_FALLBACK_RANK: u8 = 6;

const UNDECLARED_CONCURRENCY_WARNING: &str = "undeclared_concurrency";

// ---------------------------------------------------------------------------
// Scorecard data model (`replay-scorecard/v1`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Scorecard {
    pub schema_version: u32,
    pub r#type: String,
    pub run_id: String,
    pub recording_id: Option<String>,
    pub summary: Summary,
    pub per_boundary: BTreeMap<String, BoundaryStats>,
    pub per_correlation: Vec<CorrelationOutcome>,
    pub verdict: Verdict,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Summary {
    pub total_correlations: u64,
    pub matched_correlations: u64,
    pub http_status_mismatches: u64,
    pub http_body_mismatches: u64,
    /// Blocking side-effect divergences (Omitted + Novel on non-egress,
    /// correlated boundaries).
    pub side_effect_divergences: u64,
    pub matched_side_effect_calls: u64,
    pub omitted_calls: u64,
    pub novel_calls: u64,
    /// Execute-mode value divergences: the candidate ran the REAL boundary and
    /// produced a result differing in VALUE from the recorded baseline at the
    /// same args-free call-site + occurrence (the total-derivative catch). A
    /// re-keyed write's would-be Omitted+Novel split is collapsed into ONE entry
    /// here. Calls resolved by lookup/substitution keep observed == recorded.
    #[serde(default)]
    pub value_divergences: u64,
    /// Execute-mode value differences DEMOTED to a non-blocking warning because
    /// they are order-nondeterminism artifacts: two concurrent writes to the SAME
    /// correlation+table+primary-key row (overlapping wall-clock windows) whose
    /// final row state (a matched write) reproduces the recorded final state, so an
    /// earlier write's `RETURNING` row differs only by interleaving. NOT counted in
    /// `value_divergences`/`side_effect_divergences`; does NOT fail the verdict.
    #[serde(default)]
    pub order_nondeterminism_warnings: u64,
    /// Redis idempotent-delete divergences DEMOTED to a non-blocking warning: a
    /// `delete_key`/DEL that recorded `KeyDeleted` but observed `KeyNotDeleted` —
    /// the key is ABSENT afterward either way, so only the "did it exist" reply
    /// differs. NOT counted in `value_divergences`/`side_effect_divergences`; does
    /// NOT fail the verdict. The reverse (unexpected deletion) stays blocking.
    #[serde(default)]
    pub idempotent_delete_warnings: u64,
    /// Correlated, non-detached work that started after the replayed HTTP
    /// response finalized for that correlation. This is a warning only: it identifies
    /// request-path concurrency that should have been declared detached, but it
    /// does NOT contribute to `side_effect_divergences` or fail the verdict.
    #[serde(default)]
    pub undeclared_concurrency_warnings: u64,
    /// Execute-mode calls that could not be conclusively classified because the
    /// recorded baseline to compare against was absent (a seed gap). Surfaced
    /// separately so a missing baseline is neither a false match nor a false
    /// divergence. Substitute hits do not contribute seed gaps.
    #[serde(default)]
    pub inconclusive_seed_gaps: u64,
    /// Value-divergence rows that were recognized as a narrow read/write race:
    /// HTTP-clean, same typed DB row, distinct overlapping task buckets. These are
    /// not counted as blocking side-effect divergences; the verdict is explicitly
    /// inconclusive so the orchestrator can auto-rerun instead of red-failing.
    #[serde(default)]
    pub inconclusive_races: u64,
    /// Novel calls on egress boundaries — tolerated, surfaced separately so a
    /// blocked outbound integration is never read as a candidate bug.
    pub environmental_misses: u64,
    /// Calls that resolved only at the positional `Sequence` rank (rank 6).
    /// A healthy run resolves almost everything at ranks 1–5;
    /// heavy positional reliance is fragile. (The `rank5` field name is
    /// legacy, from before `Sequence` was renumbered to 6 — kept so the
    /// serialized scorecard shape stays stable; see `POSITIONAL_FALLBACK_RANK`.)
    pub recovered_rank5_calls: u64,
    /// Histogram of resolved calls by address rank — the fragility metric.
    pub resolved_by_rank: BTreeMap<String, u64>,
    pub uncorrelated_events_seen: u64,
    pub uncorrelated_events_tolerated: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BoundaryStats {
    pub matched: u64,
    pub diverged: u64,
    pub kinds: BTreeMap<String, u64>,
    pub resolved_by_rank: BTreeMap<String, u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl BoundaryStats {
    /// Record a divergence of `kind` (also bumps `diverged`).
    fn bump_kind(&mut self, kind: &str) {
        *self.kinds.entry(kind.to_owned()).or_insert(0) += 1;
        self.diverged += 1;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorrelationOutcome {
    pub correlation_id: String,
    pub http_status_match: bool,
    pub http_body_match: bool,
    pub side_effect_divergences: u64,
    pub passed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Verdict {
    pub pass: bool,
    /// True when there is nothing to judge yet (no artifacts ingested) or a
    /// structurally-required artifact is missing — distinct from a real fail.
    pub inconclusive: bool,
    pub reason: String,
}

impl Scorecard {
    /// An empty, not-yet-judged scorecard. Retained for callers that want a
    /// well-typed placeholder before a run has produced artifacts.
    pub fn empty(run_id: String) -> Self {
        Self {
            schema_version: 1,
            r#type: "replay-scorecard".to_owned(),
            run_id,
            recording_id: None,
            summary: Summary {
                uncorrelated_events_tolerated: true,
                ..Summary::default()
            },
            per_boundary: BTreeMap::new(),
            per_correlation: Vec::new(),
            verdict: Verdict {
                pass: false,
                inconclusive: true,
                reason: "run not yet completed".to_owned(),
            },
            warnings: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Detection
// ---------------------------------------------------------------------------

/// The three artifact streams a run produces, loaded into memory.
pub struct RunArtifacts {
    pub run_id: String,
    pub recording_id: Option<String>,
    pub table: LookupTable,
    pub observed: Vec<ObservedCall>,
    pub http_diffs: Vec<HttpDiff>,
    /// The recording's semantic events (recorded side). Carried so the classifier
    /// can reason about wall-clock windows + row identity for the concurrent
    /// same-row write (order-nondeterminism) demotion. Empty when unavailable.
    pub events: Vec<deja::BoundaryEvent>,
    pub warnings: Vec<String>,
}

/// Get-or-create a boundary's stats, stamping its tier (and an egress note) the
/// first time it is seen.
/// Whether a boundary tag is the database channel (which assigns serial PKs).
fn is_db_boundary(boundary: &str) -> bool {
    matches!(boundary, "db" | "storage")
}

/// Two db results are equivalent modulo replay-local DB infrastructure.
///
/// Normalizations are deliberately narrow:
/// - integer `id` fields are postgres SERIAL values assigned by the replay DB's
///   fresh sequence;
/// - structured DB `Err` payloads compare by stable `kind`; their `message` is
///   diagnostics-only text and can drift across binary versions through embedded
///   source locations or error-stack formatting.
///
/// App-set ids (`payment_id`, uuids) are strings, not integers, so they stay
/// compared and a real value divergence is still caught. DB error diagnostics are
/// only ignored inside structured `{result:"Err", kind, message}` payloads; `Ok`
/// rows and error `kind` changes remain strict.
fn db_equiv_modulo_infra(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    fn is_structured_db_err(m: &serde_json::Map<String, serde_json::Value>) -> bool {
        m.get("result").and_then(serde_json::Value::as_str) == Some("Err")
            && m.get("kind").and_then(serde_json::Value::as_str).is_some()
            && m.get("message")
                .and_then(serde_json::Value::as_str)
                .is_some()
    }

    fn normalize(v: &serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(m) => {
                let structured_err = is_structured_db_err(m);
                serde_json::Value::Object(
                    m.iter()
                        .filter(|(k, val)| !(k.as_str() == "id" && (val.is_i64() || val.is_u64())))
                        .map(|(k, val)| {
                            let normalized = if structured_err && k == "message" {
                                serde_json::Value::String("<diagnostic>".to_owned())
                            } else {
                                normalize(val)
                            };
                            (k.clone(), normalized)
                        })
                        .collect(),
                )
            }
            serde_json::Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(normalize).collect())
            }
            other => other.clone(),
        }
    }

    normalize(a) == normalize(b)
        || matches!(
            (a.as_object(), b.as_object()),
            (Some(a_obj), Some(b_obj))
                if is_structured_db_err(a_obj)
                    && is_structured_db_err(b_obj)
                    && projected_db_error_equiv(a, b)
        )
}

// ---------------------------------------------------------------------------
// Scorer-local Canon presets
// ---------------------------------------------------------------------------

/// Canonicalization lives in the scorer only. Runtime routing still follows the
/// stamped replay strategy; a `CanonRef` merely tells divergence scoring which
/// equivalence relation is valid for a declared boundary result/state.
trait Canon {
    fn preset_name(&self) -> &str;
    fn equivalent(&self, recorded: &serde_json::Value, observed: &serde_json::Value) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CanonPreset {
    Sequence,
    Bag,
    FinalState,
    AbsentAfter,
    Project {
        include: Vec<String>,
        exclude: Vec<String>,
    },
}

impl Canon for CanonPreset {
    fn preset_name(&self) -> &str {
        match self {
            Self::Sequence => "sequence",
            Self::Bag => "bag",
            Self::FinalState => "final_state",
            Self::AbsentAfter => "absent_after",
            Self::Project { .. } => "project",
        }
    }

    fn equivalent(&self, recorded: &serde_json::Value, observed: &serde_json::Value) -> bool {
        match self {
            Self::Sequence => recorded == observed,
            Self::Bag => bag_canon(recorded) == bag_canon(observed),
            Self::FinalState => final_state_canon(recorded) == final_state_canon(observed),
            Self::AbsentAfter => {
                let recorded_reply = delete_reply(&Some(recorded.clone()));
                let observed_reply = delete_reply(&Some(observed.clone()));
                recorded == observed
                    || matches!(
                        (recorded_reply.as_deref(), observed_reply.as_deref()),
                        (Some("KeyDeleted"), Some("KeyNotDeleted"))
                    )
            }
            Self::Project { include, exclude } => {
                project_canon(recorded, include, exclude)
                    == project_canon(observed, include, exclude)
            }
        }
    }
}

fn resolve_canon(canon: Option<&deja::CanonRef>) -> Option<CanonPreset> {
    let id = canon?.id.trim();
    match id {
        "sequence" => Some(CanonPreset::Sequence),
        "bag" => Some(CanonPreset::Bag),
        "final_state" => Some(CanonPreset::FinalState),
        "absent_after" => Some(CanonPreset::AbsentAfter),
        "project" => Some(CanonPreset::Project {
            include: Vec::new(),
            exclude: Vec::new(),
        }),
        _ => parse_project_canon(id),
    }
}

fn event_state_canon(ev: &deja::BoundaryEvent) -> Option<CanonPreset> {
    resolve_canon(ev.declaration.as_ref()?.state_canon.as_ref())
}

fn event_reply_canon(ev: &deja::BoundaryEvent) -> Option<CanonPreset> {
    resolve_canon(ev.declaration.as_ref()?.reply_canon.as_ref())
}

pub(crate) fn event_reply_canon_kind(ev: &deja::BoundaryEvent) -> Option<String> {
    event_reply_canon(ev).map(|canon| canon.preset_name().to_owned())
}

fn event_value_canon(ev: &deja::BoundaryEvent) -> Option<CanonPreset> {
    event_state_canon(ev).or_else(|| event_reply_canon(ev))
}

fn declared_value_equivalent(
    canon: &CanonPreset,
    recorded: &serde_json::Value,
    observed: &serde_json::Value,
) -> bool {
    if let CanonPreset::Project { include, .. } = canon {
        if !include.is_empty()
            && !include.iter().any(|field| {
                json_path_get(recorded, field).is_some() || json_path_get(observed, field).is_some()
            })
        {
            return false;
        }
    }
    // `absent_after` is still surfaced as the existing idempotent-delete warning:
    // it is a non-blocking classification, not a silent value-match absorber.
    !matches!(canon, CanonPreset::AbsentAfter) && canon.equivalent(recorded, observed)
}

pub(crate) fn values_diverge_under_event(
    boundary: &str,
    recorded: &serde_json::Value,
    observed: &serde_json::Value,
    event: Option<&deja::BoundaryEvent>,
) -> bool {
    if let Some(canon) = event.and_then(event_value_canon) {
        if declared_value_equivalent(&canon, recorded, observed) {
            return false;
        }
    }
    if is_db_boundary(boundary) && db_equiv_modulo_infra(recorded, observed) {
        return false;
    }
    recorded != observed
}

pub(crate) fn observed_value_diverged(
    obs: &ObservedCall,
    event: Option<&deja::BoundaryEvent>,
) -> bool {
    obs.resolved
        && obs.provenance == deja::Provenance::Shadow
        && match (&obs.recorded_result, &obs.observed_result) {
            (Some(recorded), Some(observed)) => {
                values_diverge_under_event(&obs.boundary, recorded, observed, event)
            }
            _ => false,
        }
}

fn is_unit_value(value: &serde_json::Value) -> bool {
    matches!(value, serde_json::Value::Null)
}

pub(crate) fn args_free_effective_values(
    recorded_result: &serde_json::Value,
    obs: &ObservedCall,
    event: Option<&deja::BoundaryEvent>,
) -> (serde_json::Value, serde_json::Value) {
    let mut recorded = recorded_result.clone();
    let mut observed = obs
        .observed_result
        .clone()
        .unwrap_or(serde_json::Value::Null);
    if is_unit_value(&recorded) && is_unit_value(&observed) {
        if let Some(value) = event.and_then(|ev| ev.args.get("value")).cloned() {
            recorded = value;
        }
        if let Some(value) = obs.args.get("value").cloned() {
            observed = value;
        }
    }
    (recorded, observed)
}

fn event_canon_labels(ev: &deja::BoundaryEvent) -> Vec<String> {
    let Some(declaration) = ev.declaration.as_ref() else {
        return Vec::new();
    };
    [
        ("state", declaration.state_canon.as_ref()),
        ("reply", declaration.reply_canon.as_ref()),
    ]
    .into_iter()
    .filter_map(|(slot, canon)| {
        let canon = canon?;
        resolve_canon(Some(canon)).map(|preset| format!("{slot}:{}", preset.preset_name()))
    })
    .collect()
}

fn parse_project_canon(id: &str) -> Option<CanonPreset> {
    let raw = id
        .strip_prefix("project:")
        .or_else(|| id.strip_prefix("project="))
        .or_else(|| {
            id.strip_prefix("project(")
                .and_then(|s| s.strip_suffix(')'))
        })?;
    let mut include = Vec::new();
    let mut exclude = Vec::new();
    for token in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        if let Some(field) = token.strip_prefix('!').or_else(|| token.strip_prefix('-')) {
            if !field.is_empty() {
                exclude.push(field.to_owned());
            }
        } else {
            include.push(token.to_owned());
        }
    }
    Some(CanonPreset::Project { include, exclude })
}

fn bag_canon(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => {
            let mut items: Vec<_> = items.iter().map(bag_canon).collect();
            items.sort_by(|a, b| {
                serde_json::to_string(a)
                    .unwrap_or_default()
                    .cmp(&serde_json::to_string(b).unwrap_or_default())
            });
            serde_json::Value::Array(items)
        }
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), bag_canon(value)))
                .collect(),
        ),
        other => other.clone(),
    }
}

fn final_state_canon(value: &serde_json::Value) -> serde_json::Value {
    let value = value.get("value").unwrap_or(value);
    match value {
        serde_json::Value::Array(items) => items.last().cloned().unwrap_or(serde_json::Value::Null),
        other => other.clone(),
    }
}

fn project_canon(
    value: &serde_json::Value,
    include: &[String],
    exclude: &[String],
) -> serde_json::Value {
    if !include.is_empty() {
        return serde_json::Value::Object(
            include
                .iter()
                .filter_map(|field| json_path_get(value, field).map(|v| (field.clone(), v.clone())))
                .collect(),
        );
    }
    if exclude.is_empty() {
        return value.clone();
    }
    project_exclude_canon(value, exclude, "")
}

fn project_exclude_canon(
    value: &serde_json::Value,
    exclude: &[String],
    path: &str,
) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => serde_json::Value::Object(
            map.iter()
                .filter_map(|(key, value)| {
                    let child_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{path}.{key}")
                    };
                    (!project_excludes_path(exclude, key, &child_path)).then(|| {
                        (
                            key.clone(),
                            project_exclude_canon(value, exclude, &child_path),
                        )
                    })
                })
                .collect(),
        ),
        serde_json::Value::Array(items) => serde_json::Value::Array(
            items
                .iter()
                .enumerate()
                .map(|(idx, item)| {
                    let child_path = if path.is_empty() {
                        format!("[{idx}]")
                    } else {
                        format!("{path}[{idx}]")
                    };
                    project_exclude_canon(item, exclude, &child_path)
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

fn project_excludes_path(exclude: &[String], key: &str, path: &str) -> bool {
    let normalized_path = normalize_project_path(path);
    let unindexed_path = remove_json_indexes(&normalized_path);
    let leaf = unindexed_path
        .rsplit('.')
        .next()
        .filter(|segment| !segment.is_empty())
        .unwrap_or(&unindexed_path);
    exclude.iter().any(|field| {
        let normalized_field = normalize_project_path(field);
        normalized_field == normalized_path
            || normalized_field == unindexed_path
            || normalized_field == key
            || (!normalized_field.contains('.') && normalized_field == leaf)
    })
}

fn project_excludes_json_diff_path(exclude: &[String], json_path: &str) -> bool {
    project_excludes_path(exclude, "", json_path)
}

const HTTP_REPLY_PROJECT_FIELD_ALIASES: &[(&str, &str)] = &[("created", "created_at")];

fn http_project_excludes_json_diff_path(exclude: &[String], json_path: &str) -> bool {
    if project_excludes_json_diff_path(exclude, json_path) {
        return true;
    }
    let normalized_path = normalize_project_path(json_path);
    let unindexed_path = remove_json_indexes(&normalized_path);
    let leaf = unindexed_path.rsplit('.').next().unwrap_or(&unindexed_path);
    HTTP_REPLY_PROJECT_FIELD_ALIASES
        .iter()
        .find_map(|(reply_field, declared_field)| (*reply_field == leaf).then_some(*declared_field))
        .is_some_and(|declared_field| {
            project_excludes_path(exclude, declared_field, declared_field)
        })
}

fn normalize_project_path(path: &str) -> String {
    path.trim()
        .strip_prefix('$')
        .unwrap_or(path.trim())
        .trim_start_matches('.')
        .to_owned()
}

fn remove_json_indexes(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut in_index = false;
    for ch in path.chars() {
        match ch {
            '[' => in_index = true,
            ']' if in_index => in_index = false,
            _ if !in_index => out.push(ch),
            _ => {}
        }
    }
    out
}

fn json_path_get<'a>(value: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    path.split('.')
        .try_fold(value, |current, segment| current.get(segment))
}

fn projected_db_error_equiv(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    let project_kind = CanonPreset::Project {
        include: vec!["result".to_owned(), "kind".to_owned()],
        exclude: Vec::new(),
    };
    project_kind.equivalent(a, b)
}

fn rows_equal_for_order_evidence(
    ev: &deja::BoundaryEvent,
    a: &serde_json::Map<String, serde_json::Value>,
    b: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let a = serde_json::Value::Object(a.clone());
    let b = serde_json::Value::Object(b.clone());
    match event_reply_canon(ev).or_else(|| event_state_canon(ev)) {
        Some(CanonPreset::Project { include, exclude }) => {
            CanonPreset::Project { include, exclude }.equivalent(&a, &b)
        }
        Some(CanonPreset::Bag) => CanonPreset::Bag.equivalent(&a, &b),
        Some(CanonPreset::FinalState)
        | Some(CanonPreset::Sequence)
        | Some(CanonPreset::AbsentAfter)
        | None => rows_equal_modulo_volatile(
            a.as_object().expect("row object"),
            b.as_object().expect("row object"),
        ),
    }
}

fn boundary_entry<'a>(
    map: &'a mut BTreeMap<String, BoundaryStats>,
    boundary: &str,
) -> &'a mut BoundaryStats {
    let stats = map.entry(boundary.to_owned()).or_default();
    if stats.tier.is_none() {
        let tier = tier_for(boundary);
        stats.tier = Some(tier.label().to_owned());
        if tier == Tier::Environmental {
            stats.note = Some(
                "egress blocked; novel calls are environmental misses, not candidate bugs"
                    .to_owned(),
            );
        }
    }
    stats
}

#[derive(Debug, Clone)]
struct UndeclaredConcurrencyWarning {
    source_event_global_sequence: Option<u64>,
    correlation_id: String,
    boundary: String,
    method: String,
    timestamp_ns: u64,
    response_finalized_ns: u64,
}

fn observed_end_timestamp_ns(obs: &ObservedCall) -> u64 {
    obs.end_timestamp_ns.unwrap_or(obs.timestamp_ns)
}

fn undeclared_concurrency_warnings(observed: &[ObservedCall]) -> Vec<UndeclaredConcurrencyWarning> {
    let mut finalization_by_correlation: HashMap<String, u64> = HashMap::new();
    for obs in observed {
        if obs.boundary != "http_incoming" {
            continue;
        }
        let Some(correlation_id) = &obs.correlation_id else {
            continue;
        };
        let finalized_ns = observed_end_timestamp_ns(obs);
        finalization_by_correlation
            .entry(correlation_id.clone())
            .and_modify(|existing| *existing = (*existing).max(finalized_ns))
            .or_insert(finalized_ns);
    }

    observed
        .iter()
        .filter_map(|obs| {
            if obs.detached || obs.boundary == "http_incoming" || obs.timestamp_ns == 0 {
                return None;
            }
            let correlation_id = obs.correlation_id.as_ref()?;
            let response_finalized_ns = *finalization_by_correlation.get(correlation_id)?;
            if obs.timestamp_ns <= response_finalized_ns {
                return None;
            }
            Some(UndeclaredConcurrencyWarning {
                source_event_global_sequence: obs.source_event_global_sequence,
                correlation_id: correlation_id.clone(),
                boundary: obs.boundary.clone(),
                method: obs.method_name.clone(),
                timestamp_ns: obs.timestamp_ns,
                response_finalized_ns,
            })
        })
        .collect()
}

fn returns_row(result: &serde_json::Value) -> bool {
    match result.get("value") {
        Some(serde_json::Value::Array(a)) => !a.is_empty(),
        Some(serde_json::Value::Object(_)) => true,
        _ => false,
    }
}

fn declared_update_returning(ev: &deja::BoundaryEvent) -> Option<bool> {
    let declaration = ev.declaration.as_ref()?;
    let effect = declaration.effect?;
    let op = declaration.op?;
    let returns = declaration.returns?;
    Some(
        effect == deja::EffectKind::Db
            && matches!(op, deja::OperationKind::Update | deja::OperationKind::Touch)
            && returns == deja::ReturnSemantics::UpdateReturning,
    )
}

fn is_update_returning_event(ev: &deja::BoundaryEvent) -> bool {
    declared_update_returning(ev).unwrap_or_else(|| {
        ev.boundary == "db" && ev.method_name.contains("update") && returns_row(&ev.result)
    })
}

fn declared_idempotent_delete(ev: &deja::BoundaryEvent) -> Option<bool> {
    let declaration = ev.declaration.as_ref()?;
    let effect = declaration.effect?;
    let op = declaration.op?;
    Some(effect == deja::EffectKind::Redis && op == deja::OperationKind::IdempotentDelete)
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DbRowKey {
    table: String,
    pk_column: String,
    pk_value: String,
    wire: String,
}

impl DbRowKey {
    fn label(&self) -> String {
        format!(
            "{} {}={} ({})",
            self.table, self.pk_column, self.pk_value, self.wire
        )
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct OrderNondeterministicDemotions {
    sequences: HashSet<u64>,
    row_labels: BTreeMap<u64, String>,
    canon_labels: BTreeMap<u64, String>,
}

impl OrderNondeterministicDemotions {
    fn insert(&mut self, seq: u64, row_key: &DbRowKey, ev: &deja::BoundaryEvent) {
        self.sequences.insert(seq);
        self.row_labels
            .entry(seq)
            .or_insert_with(|| row_key.label());
        let labels = event_canon_labels(ev);
        if !labels.is_empty() {
            self.canon_labels
                .entry(seq)
                .or_insert_with(|| labels.join(","));
        }
    }

    fn contains(&self, seq: &u64) -> bool {
        self.sequences.contains(seq)
    }

    fn canon_label(&self, seq: &u64) -> Option<&str> {
        self.canon_labels.get(seq).map(String::as_str)
    }
}

fn db_row_key_from_state_key(raw: &str) -> Option<DbRowKey> {
    let parsed = deja::StateKey::parse(raw).ok()?;
    let wire = parsed.to_wire();
    let table = parsed.db_table()?.to_owned();
    match parsed {
        deja::StateKey::DbRow {
            pk_column,
            pk_value,
            ..
        } => Some(DbRowKey {
            table,
            pk_column,
            pk_value,
            wire,
        }),
        _ => None,
    }
}

fn event_db_row_key(ev: &deja::BoundaryEvent) -> Option<DbRowKey> {
    let mut row_key: Option<DbRowKey> = None;
    for raw in ev.write_set.iter().chain(ev.read_set.iter()) {
        let Some(next) = db_row_key_from_state_key(raw) else {
            continue;
        };
        if row_key.as_ref().is_some_and(|seen| {
            seen.table != next.table
                || seen.pk_column != next.pk_column
                || seen.pk_value != next.pk_value
        }) {
            return None;
        }
        row_key.get_or_insert(next);
    }
    row_key
}

/// Reconcile the artifact streams into a `replay-scorecard/v1`.
/// Rule A — order-nondeterminism demotion (concurrent same-row UPDATE RETURNING).
///
/// Returns the recorded event sequences whose execute-mode value divergence is a
/// benign INTERLEAVING artifact and must be DEMOTED to a non-blocking warning
/// (not a gate failure), plus row labels for diagnostics. STRICTLY guarded so a
/// real lost-update can never hide:
///   0. `http_clean` — the run's HTTP layer is 9/9 (no status/body mismatch). If
///      any HTTP diverged, NOTHING is demoted (the response itself is wrong).
///   1. Declared `Db` + `Update`/`Touch` + `UpdateReturning`; old/incomplete tapes
///      may still identify the operation by `db` boundary + update-ish method +
///      RETURNING row shape, but they MUST carry a typed `StateKey::DbRow` in
///      the event state sets. Without a typed row key, Rule A stays conservative.
///   2. Two+ writes to the SAME correlation + typed table + typed primary key.
///      Row values still have to line up modulo the explicit volatile-column
///      allowlist; the allowlist is only a row-value comparison guard, never a
///      grouping key.
///   3. The demoted earlier write's wall-clock window OVERLAPS the FINAL write's
///      (genuinely concurrent, not sequential).
///   4. The FINAL/LAST write of that same-row set (max `global_sequence`) is
///      MATCHED on replay — it reproduces the recorded final row. If the final
///      write diverges (final state lost), NOTHING in the set is demoted, so a real
///      lost-update stays a blocking divergence.
pub(crate) fn order_nondeterministic_demotions(
    events: &[deja::BoundaryEvent],
    observed: &[ObservedCall],
    http_clean: bool,
) -> OrderNondeterministicDemotions {
    // Guard 0: demotion is only ever considered on an otherwise HTTP-clean run.
    if !http_clean {
        return OrderNondeterministicDemotions::default();
    }

    // matched-on-replay: an observed call for a recorded seq that resolved and did
    // NOT value-diverge after applying the event-scoped declaration.
    let events_by_seq: HashMap<u64, &deja::BoundaryEvent> =
        events.iter().map(|ev| (ev.global_sequence, ev)).collect();
    let mut matched_seq: HashSet<u64> = HashSet::new();
    for obs in observed {
        let Some(seq) = obs.source_event_global_sequence else {
            continue;
        };
        if obs.resolved && !observed_value_diverged(obs, events_by_seq.get(&seq).copied()) {
            matched_seq.insert(seq);
        }
    }

    // Guard 1: an UPDATE whose recorded result carries a RETURNING row and whose
    // event state sets contain exactly one typed DB-row identity. Typed row keys
    // are the only grouping input; legacy table strings and row JSON never form
    // identity. If a PK row key is absent or ambiguous, Rule A refuses demotion.
    type Key = (Option<String>, String, String, String);
    let mut groups: HashMap<Key, Vec<(&deja::BoundaryEvent, DbRowKey)>> = HashMap::new();
    for ev in events {
        if !is_update_returning_event(ev) {
            continue;
        }
        let Some(row_key) = event_db_row_key(ev) else {
            continue;
        };
        groups
            .entry((
                ev.correlation_id.clone(),
                row_key.table.clone(),
                row_key.pk_column.clone(),
                row_key.pk_value.clone(),
            ))
            .or_default()
            .push((ev, row_key));
    }

    let overlaps = |a: &deja::BoundaryEvent, b: &deja::BoundaryEvent| -> bool {
        let a_e = a.end_timestamp_ns.unwrap_or(a.timestamp_ns);
        let b_e = b.end_timestamp_ns.unwrap_or(b.timestamp_ns);
        a.timestamp_ns.max(b.timestamp_ns) < a_e.min(b_e)
    };

    let mut demote = OrderNondeterministicDemotions::default();
    for members in groups.values() {
        if members.len() < 2 {
            continue;
        }
        // Guard 4: the FINAL/LAST write (max global_sequence) must be matched — it
        // reproduces the recorded final row. Otherwise the final state is lost.
        let Some((final_write, final_key)) = members.iter().max_by_key(|(e, _)| e.global_sequence)
        else {
            continue;
        };
        if !matched_seq.contains(&final_write.global_sequence) {
            continue;
        }
        let Some(final_row) = db_returning_row(&final_write.result) else {
            continue;
        };
        // Demote each NON-matched (diverged) earlier write whose window OVERLAPS
        // the final write's (guard 3) and whose recorded row is the same final row
        // modulo the narrow volatile-column allowlist. The group already proved
        // exact typed row identity; row comparison only proves the interleaving
        // evidence, never the identity.
        for (m, row_key) in members {
            if m.global_sequence == final_write.global_sequence
                || matched_seq.contains(&m.global_sequence)
            {
                continue;
            }
            let Some(row) = db_returning_row(&m.result) else {
                continue;
            };
            if overlaps(m, final_write) && rows_equal_for_order_evidence(m, row, final_row) {
                demote.insert(m.global_sequence, row_key, m);
            }
        }
        if demote.contains(&final_write.global_sequence) {
            demote
                .row_labels
                .insert(final_write.global_sequence, final_key.label());
        }
    }

    // ORDER-SWAP arm: when the RECORDING captured the opposite interleaving, the
    // diverged earlier write's recorded row is the PRE-final state, so the
    // same-recorded-row evidence above cannot pair it. Evidence here is the
    // inverse: the earlier write's OBSERVED row equals the RECORDED row of an
    // overlapping, MATCHED, same-correlation+typed-row final write — i.e. on replay
    // the earlier write simply saw the final state early. Rows are compared
    // MODULO VOLATILE_COLUMNS; general row comparison everywhere else stays strict.
    let update_events: Vec<(&deja::BoundaryEvent, DbRowKey)> = events
        .iter()
        .filter(|ev| is_update_returning_event(ev))
        .filter_map(|ev| event_db_row_key(ev).map(|row_key| (ev, row_key)))
        .collect();
    let by_seq: HashMap<u64, &(&deja::BoundaryEvent, DbRowKey)> = update_events
        .iter()
        .map(|p| (p.0.global_sequence, p))
        .collect();
    for obs in observed {
        let Some(seq) = obs.source_event_global_sequence else {
            continue;
        };
        if demote.contains(&seq) || matched_seq.contains(&seq) {
            continue;
        }
        let diverged = observed_value_diverged(obs, events_by_seq.get(&seq).copied());
        if !diverged {
            continue;
        }
        let Some((ev, row_key)) = by_seq.get(&seq).map(|p| (p.0, &p.1)) else {
            continue;
        };
        let Some(observed_row) = obs.observed_result.as_ref().and_then(db_returning_row) else {
            continue;
        };
        // The evidence write must be strictly LATER than the diverged one (the
        // swap story is "the earlier write saw the final state early"; an EARLIER
        // matched row equal to the observed value is NOT final-state evidence),
        // plus matched, overlapping, exact same typed row key, with its RECORDED
        // row equal to the diverged OBSERVED row modulo volatile columns.
        let swap_evidenced = update_events.iter().any(|(other, other_key)| {
            other.global_sequence > seq
                && other.correlation_id == ev.correlation_id
                && other_key.table == row_key.table
                && other_key.pk_column == row_key.pk_column
                && other_key.pk_value == row_key.pk_value
                && matched_seq.contains(&other.global_sequence)
                && overlaps(ev, other)
                && db_returning_row(&other.result).is_some_and(|final_row| {
                    rows_equal_for_order_evidence(ev, observed_row, final_row)
                })
        });
        if swap_evidenced {
            demote.insert(seq, row_key, ev);
        }
    }
    demote
}

/// Columns the racing writes themselves stamp (their own `now()`), so the twin
/// rows of a concurrent same-row UPDATE pair differ there by construction. Used
/// ONLY inside the order-swap evidence comparison — never in general row scoring.
const VOLATILE_COLUMNS: &[&str] = &["modified_at", "last_synced"];

/// Unwrap a structured db result envelope (`{result:"Ok", value: [row] | row}`)
/// to its single RETURNING row, if that is its shape.
fn db_returning_row(v: &serde_json::Value) -> Option<&serde_json::Map<String, serde_json::Value>> {
    let value = v.get("value").unwrap_or(v);
    match value {
        serde_json::Value::Object(m) => Some(m),
        serde_json::Value::Array(a) if a.len() == 1 => a[0].as_object(),
        _ => None,
    }
}

/// Row equality modulo [`VOLATILE_COLUMNS`] (order-swap evidence check only).
fn rows_equal_modulo_volatile(
    a: &serde_json::Map<String, serde_json::Value>,
    b: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let keys: std::collections::BTreeSet<&str> = a
        .keys()
        .chain(b.keys())
        .map(String::as_str)
        .filter(|k| !VOLATILE_COLUMNS.contains(k))
        .collect();
    keys.into_iter().all(|k| a.get(k) == b.get(k))
}

/// Read a redis delete reply (`KeyDeleted` / `KeyNotDeleted`) from a result value.
/// The reply serializes as a bare enum-name string; tolerate an envelope wrapper.
fn delete_reply(v: &Option<serde_json::Value>) -> Option<String> {
    match v {
        Some(serde_json::Value::String(s)) => Some(s.clone()),
        Some(serde_json::Value::Object(m)) => m
            .get("value")
            .or_else(|| m.get("result"))
            .and_then(|x| x.as_str())
            .map(str::to_owned),
        _ => None,
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct InconclusiveRaceEvidence {
    sequences: HashSet<u64>,
    row_labels: BTreeMap<u64, String>,
    downstream_values: BTreeMap<String, Vec<serde_json::Value>>,
    correlations: BTreeSet<String>,
}

impl InconclusiveRaceEvidence {
    fn insert_origin(
        &mut self,
        seq: u64,
        correlation_id: &str,
        row_key: &DbRowKey,
        recorded_value: serde_json::Value,
        observed_value: serde_json::Value,
    ) {
        self.sequences.insert(seq);
        self.row_labels
            .entry(seq)
            .or_insert_with(|| row_key.label());
        self.downstream_values
            .entry(correlation_id.to_owned())
            .or_default()
            .push(observed_value);
        self.downstream_values
            .entry(correlation_id.to_owned())
            .or_default()
            .push(recorded_value);
        self.correlations.insert(correlation_id.to_owned());
    }

    pub(crate) fn contains(&self, seq: &u64) -> bool {
        self.sequences.contains(seq)
    }

    pub(crate) fn attributable_downstream(
        &self,
        correlation_id: Option<&str>,
        value: &serde_json::Value,
    ) -> bool {
        let Some(values) = correlation_id.and_then(|corr| self.downstream_values.get(corr)) else {
            return false;
        };
        values
            .iter()
            .any(|race_value| json_contains_value(value, race_value))
    }

    fn http_body_diff_attributable(&self, correlation_id: &str, diff: &JsonFieldDiff) -> bool {
        self.contains_attributable_leaf(correlation_id, &diff.baseline)
            && self.contains_attributable_leaf(correlation_id, &diff.candidate)
    }

    fn contains_attributable_leaf(&self, correlation_id: &str, value: &serde_json::Value) -> bool {
        if !is_specific_http_diff_value(value) {
            return false;
        }
        let Some(values) = self.downstream_values.get(correlation_id) else {
            return false;
        };
        values
            .iter()
            .any(|race_value| json_contains_value(race_value, value))
    }
}

fn is_specific_http_diff_value(value: &serde_json::Value) -> bool {
    matches!(
        value,
        serde_json::Value::String(_) | serde_json::Value::Array(_) | serde_json::Value::Object(_)
    )
}

fn http_incoming_events_by_correlation(
    events: &[deja::BoundaryEvent],
) -> HashMap<String, &deja::BoundaryEvent> {
    events
        .iter()
        .filter(|ev| ev.boundary == "http_incoming")
        .filter_map(|ev| ev.correlation_id.as_ref().map(|corr| (corr.clone(), ev)))
        .collect()
}

fn http_diff_absorbed_by_reply_canon(
    diff: &HttpDiff,
    recorded_http: Option<&deja::BoundaryEvent>,
    body: &JsonFieldDiff,
) -> bool {
    let Some(CanonPreset::Project { include, exclude }) = recorded_http.and_then(event_reply_canon)
    else {
        return false;
    };
    if let (Some(baseline), Some(candidate)) = (&diff.baseline_body, &diff.candidate_body) {
        if project_canon(baseline, &include, &exclude)
            == project_canon(candidate, &include, &exclude)
        {
            return true;
        }
    }
    http_project_excludes_json_diff_path(&exclude, &body.json_path)
}

fn blocking_http_body_diff_count(
    diff: &HttpDiff,
    recorded_http: Option<&deja::BoundaryEvent>,
    race: &InconclusiveRaceEvidence,
) -> usize {
    diff.body_diff
        .iter()
        .filter(|body| {
            !http_diff_absorbed_by_reply_canon(diff, recorded_http, body)
                && !race.http_body_diff_attributable(&diff.correlation_id, body)
        })
        .count()
}

fn json_contains_value(haystack: &serde_json::Value, needle: &serde_json::Value) -> bool {
    if haystack == needle {
        return true;
    }
    match haystack {
        serde_json::Value::Array(items) => {
            items.iter().any(|item| json_contains_value(item, needle))
        }
        serde_json::Value::Object(map) => {
            map.values().any(|item| json_contains_value(item, needle))
        }
        _ => false,
    }
}

fn db_row_keys_from_set(raw_keys: &[String]) -> Vec<DbRowKey> {
    raw_keys
        .iter()
        .filter_map(|raw| db_row_key_from_state_key(raw))
        .collect()
}

fn single_db_row_key(raw_keys: &[String]) -> Option<DbRowKey> {
    let mut keys = db_row_keys_from_set(raw_keys);
    keys.dedup_by(|a, b| {
        a.table == b.table && a.pk_column == b.pk_column && a.pk_value == b.pk_value
    });
    match keys.as_slice() {
        [key] => Some(key.clone()),
        _ => None,
    }
}

fn same_db_row(a: &DbRowKey, b: &DbRowKey) -> bool {
    a.table == b.table && a.pk_column == b.pk_column && a.pk_value == b.pk_value
}

fn lineage_bucket(ev: &deja::BoundaryEvent) -> Option<&str> {
    ev.bucket_id
        .as_deref()
        .or(ev.task_bucket.as_deref())
        .or(ev.task_id.as_deref())
}

fn unordered_distinct_lineage(
    a: &deja::BoundaryEvent,
    b: &deja::BoundaryEvent,
    span_paths: &HashMap<u64, String>,
) -> bool {
    if a.task_id.is_some() && a.task_id == b.task_id {
        return unordered_distinct_span_path(a.global_sequence, b.global_sequence, span_paths);
    }
    match (lineage_bucket(a), lineage_bucket(b)) {
        (Some(a_bucket), Some(b_bucket)) if a_bucket != b_bucket => true,
        (Some(_), Some(_)) => {
            unordered_distinct_span_path(a.global_sequence, b.global_sequence, span_paths)
        }
        _ => unordered_distinct_span_path(a.global_sequence, b.global_sequence, span_paths),
    }
}

fn unordered_distinct_span_path(a_seq: u64, b_seq: u64, span_paths: &HashMap<u64, String>) -> bool {
    let (Some(a), Some(b)) = (span_paths.get(&a_seq), span_paths.get(&b_seq)) else {
        return false;
    };
    span_paths_are_unordered(a, b)
}

fn span_paths_are_unordered(a: &str, b: &str) -> bool {
    !(a == b || span_path_is_prefix(a, b) || span_path_is_prefix(b, a))
}

fn span_path_is_prefix(parent: &str, child: &str) -> bool {
    child
        .strip_prefix(parent)
        .is_some_and(|suffix| suffix.starts_with('>'))
}

fn event_windows_overlap(a: &deja::BoundaryEvent, b: &deja::BoundaryEvent) -> bool {
    let a_end = a.end_timestamp_ns.unwrap_or(a.timestamp_ns);
    let b_end = b.end_timestamp_ns.unwrap_or(b.timestamp_ns);
    a.timestamp_ns.max(b.timestamp_ns) < a_end.min(b_end)
}

pub(crate) fn inconclusive_race_evidence(
    events: &[deja::BoundaryEvent],
    observed: &[ObservedCall],
    race_evidence_allowed: bool,
    span_paths: &HashMap<u64, String>,
) -> InconclusiveRaceEvidence {
    if !race_evidence_allowed {
        return InconclusiveRaceEvidence::default();
    }
    let events_by_seq: HashMap<u64, &deja::BoundaryEvent> =
        events.iter().map(|ev| (ev.global_sequence, ev)).collect();
    let mut evidence = InconclusiveRaceEvidence::default();
    for obs in observed {
        let event = obs
            .source_event_global_sequence
            .and_then(|seq| events_by_seq.get(&seq).copied());
        let diverged = observed_value_diverged(obs, event);
        if !diverged {
            continue;
        }
        let Some(seq) = obs.source_event_global_sequence else {
            continue;
        };
        let Some(read_event) = events_by_seq.get(&seq).copied() else {
            continue;
        };
        let Some(correlation_id) = read_event.correlation_id.as_deref() else {
            continue;
        };
        let Some(read_key) = single_db_row_key(&read_event.read_set) else {
            continue;
        };
        let conflict = events.iter().any(|write_event| {
            write_event.global_sequence != read_event.global_sequence
                && write_event.correlation_id.as_deref() == Some(correlation_id)
                && unordered_distinct_lineage(read_event, write_event, span_paths)
                && event_windows_overlap(read_event, write_event)
                && db_row_keys_from_set(&write_event.write_set)
                    .iter()
                    .any(|write_key| same_db_row(&read_key, write_key))
        });
        if conflict {
            evidence.insert_origin(
                seq,
                correlation_id,
                &read_key,
                obs.recorded_result
                    .clone()
                    .unwrap_or(serde_json::Value::Null),
                obs.observed_result
                    .clone()
                    .unwrap_or(serde_json::Value::Null),
            );
        }
    }
    evidence
}

/// Rule B — idempotent-delete demotion. Returns the recorded event sequences whose
/// execute-mode value divergence is a benign idempotent redis DELETE and must be
/// DEMOTED to a non-blocking warning. STRICTLY guarded — deliberately narrow:
///   0. `http_clean` — the run's HTTP layer is 9/9. Otherwise nothing is demoted.
///   1. `Redis` + `IdempotentDelete` in the recorded source event declaration.
///      Old/incomplete tapes fall back to exact `redis.delete_key` matching.
///   2. `obs.resolved` — the call args-aligned to its recorded baseline, so it is
///      the SAME recorded source/correlation/key (a re-keyed op would not resolve).
///   3. recorded reply is `KeyDeleted` AND observed reply is `KeyNotDeleted`.
///
/// Both outcomes leave the key ABSENT afterward, so an idempotent DEL differs only
/// in "did the key exist to delete". The REVERSE (`KeyNotDeleted` -> `KeyDeleted`,
/// an unexpected deletion), any non-`delete_key` op, and re-keyed/unresolved calls
/// are NOT demoted.
pub(crate) fn idempotent_delete_demotions(
    events: &[deja::BoundaryEvent],
    observed: &[ObservedCall],
    http_clean: bool,
) -> HashSet<u64> {
    if !http_clean {
        return HashSet::new();
    }
    let events_by_seq: HashMap<u64, &deja::BoundaryEvent> =
        events.iter().map(|ev| (ev.global_sequence, ev)).collect();
    observed
        .iter()
        .filter(|obs| {
            let ev = obs
                .source_event_global_sequence
                .and_then(|seq| events_by_seq.get(&seq))
                .copied();
            let reply_canon = ev.and_then(event_reply_canon);
            let is_absent_after = matches!(reply_canon, Some(CanonPreset::AbsentAfter));
            let is_idempotent_delete = ev
                .and_then(declared_idempotent_delete)
                .unwrap_or_else(|| obs.boundary == "redis" && obs.method_name == "delete_key");

            obs.resolved
                && obs.provenance == deja::Provenance::Shadow
                && (is_idempotent_delete || is_absent_after)
                && delete_reply(&obs.recorded_result).as_deref() == Some("KeyDeleted")
                && delete_reply(&obs.observed_result).as_deref() == Some("KeyNotDeleted")
        })
        .filter_map(|obs| obs.source_event_global_sequence)
        .collect()
}

pub fn detect(art: &RunArtifacts) -> Scorecard {
    // V1: uncorrelated (background-task) events are tolerated; the deja-tokio
    // correlation-propagation fix is a separate plan.
    let uncorrelated_tolerated = true;

    let mut per_boundary: BTreeMap<String, BoundaryStats> = BTreeMap::new();

    // --- expected side-effect calls, deduped by source event -----------------
    // Each recorded event yields up to one entry per address rank; we collapse
    // them by `source_event_global_sequence`. The boundary AND method live on the
    // rank-6 `Sequence` address, which every event always emits. We also carry the
    // recorded `result` here — the recorded operand the args-free pairing compares
    // an execute-shadow `observed_result` against to classify ValueDiverged.
    struct Expected {
        boundary: Option<String>,
        method: Option<String>,
        correlation: Option<String>,
        result: serde_json::Value,
    }
    let mut expected: BTreeMap<u64, Expected> = BTreeMap::new();
    for entry in &art.table.entries {
        let slot = expected
            .entry(entry.source_event_global_sequence)
            .or_insert(Expected {
                boundary: None,
                method: None,
                correlation: entry.key.correlation_id.clone(),
                result: entry.result.clone(),
            });
        if let Address::Sequence {
            boundary, method, ..
        } = &entry.key.address
        {
            slot.boundary = Some(boundary.clone());
            slot.method = Some(method.clone());
        }
    }
    let uncorrelated_events_seen = expected
        .values()
        .filter(|e| e.correlation.is_none())
        .count() as u64;

    // --- args-free pairing for execute-mode value divergence -----------------
    // GOTCHA #1: a diverged WRITE carries a mutated operand (e.g. a doubled
    // amount), so its `args_hash` no longer matches the recorded baseline. Under
    // the strict-args lookup path that miss splits the SAME logical write into a
    // recorded OmittedCall + an execute NovelCall. To recover the single truth —
    // ONE ValueDiverged — we pair the unresolved observed calls to the unconsumed
    // expected events by ARGS-FREE call-site identity (`correlation, boundary,
    // method`) + occurrence (the Nth such call in stream / source order). args_hash
    // is the DIFF signal here, never the resolution key.
    //
    // NO-REGRESSION: this pairing only reaches calls that did NOT resolve normally.
    // Substitute hits resolve through lookup with observed_result == recorded_result,
    // so they never enter this path and ValueDiverged stays inert.

    // Recorded side: unconsumed expected events grouped by args-free identity,
    // ordered by source sequence, occurrence = position within the group.
    type Identity = (Option<String>, String, String);
    let identity_of = |corr: &Option<String>, boundary: &str, method: &str| -> Identity {
        (corr.clone(), boundary.to_owned(), method.to_owned())
    };
    // (identity -> queue of (source_seq, recorded_result)); FIFO by source order.
    let mut recorded_pairing: BTreeMap<
        Identity,
        std::collections::VecDeque<(u64, serde_json::Value)>,
    > = BTreeMap::new();
    for (seq, exp) in &expected {
        // Only events that carry a concrete boundary+method (every event does, via
        // the rank-6 Sequence address) are pair-able; uncorrelated/tolerated events
        // still queue but are filtered out when we decide to emit (see below).
        let (Some(boundary), Some(method)) = (&exp.boundary, &exp.method) else {
            continue;
        };
        recorded_pairing
            .entry(identity_of(&exp.correlation, boundary, method))
            .or_default()
            .push_back((*seq, exp.result.clone()));
    }
    let events_by_seq: HashMap<u64, &deja::BoundaryEvent> = art
        .events
        .iter()
        .map(|ev| (ev.global_sequence, ev))
        .collect();
    let http_incoming_by_correlation = http_incoming_events_by_correlation(&art.events);

    let mut value_divergences = 0u64;
    let mut order_nondeterminism_warnings = 0u64;
    let mut idempotent_delete_warnings = 0u64;
    // Race evidence needs to be discovered before HTTP body classification:
    // a race can flow into the response body itself. Status mismatches still
    // block evidence up front; body mismatches are neutralized only when their
    // leaf values are proven attributable to the same race evidence.
    let http_status_clean =
        !art.http_diffs.is_empty() && art.http_diffs.iter().all(|d| d.status_match);
    let recorded_span_paths = ledger::recorded_span_paths(&art.table);
    let inconclusive_race = inconclusive_race_evidence(
        &art.events,
        &art.observed,
        http_status_clean,
        &recorded_span_paths,
    );
    let blocking_http_body_mismatches = art
        .http_diffs
        .iter()
        .filter(|diff| {
            blocking_http_body_diff_count(
                diff,
                http_incoming_by_correlation
                    .get(&diff.correlation_id)
                    .copied(),
                &inconclusive_race,
            ) > 0
        })
        .count();
    let http_clean = http_status_clean && blocking_http_body_mismatches == 0;
    // Rule A: concurrent same-row UPDATE-RETURNING interleaving artifacts.
    let order_nondet_demote =
        order_nondeterministic_demotions(&art.events, &art.observed, http_clean);
    // Rule B: idempotent redis DELETE (recorded KeyDeleted vs observed KeyNotDeleted).
    let idempotent_delete_demote =
        idempotent_delete_demotions(&art.events, &art.observed, http_clean);
    let undeclared_concurrency = undeclared_concurrency_warnings(&art.observed);
    let undeclared_concurrency_warnings = undeclared_concurrency.len() as u64;
    let mut inconclusive_seed_gaps = 0u64;
    let mut inconclusive_races = 0u64;
    // Expected events claimed by a ValueDiverged pairing: counted as the
    // divergence, NOT as an OmittedCall in the omitted pass below.
    let mut paired_consumed: HashSet<u64> = HashSet::new();

    // --- observed calls: matched (+ recovered) and novel ---------------------
    let mut consumed: HashSet<u64> = HashSet::new();
    let mut resolved_by_rank: BTreeMap<String, u64> = BTreeMap::new();
    let mut matched_side_effect_calls = 0u64;
    let mut recovered_rank5_calls = 0u64;
    let mut novel_calls = 0u64;
    let mut environmental_misses = 0u64;
    let mut blocking_side_effect = 0u64;
    let mut corr_side_effect: BTreeMap<String, u64> = BTreeMap::new();

    for obs in &art.observed {
        if obs.boundary == "http_incoming" {
            continue;
        }
        let stats = boundary_entry(&mut per_boundary, &obs.boundary);
        if obs.resolved {
            // The recorded baseline was found (args still aligned). Under lookup
            // mode observed_result == recorded_result (substituted) so this is a
            // plain match. Under execute mode the recorded baseline was located by
            // args-aligned occurrence but the REAL boundary ran: if its
            // observed_result differs from the recorded baseline this is a
            // ValueDiverged (the args-aligned flavor — a READ, or a WRITE whose
            // operand did not change). The re-keyed WRITE whose operand DID change
            // misses args and is paired args-free in the Novel branch below.
            let diverged = observed_value_diverged(
                obs,
                obs.source_event_global_sequence
                    .and_then(|seq| events_by_seq.get(&seq).copied()),
            );
            if diverged {
                // Rule A: a concurrent same-row UPDATE-RETURNING interleaving
                // artifact is NOT a blocking divergence — the final row state is
                // reproduced by a matched write; only this earlier write's RETURNING
                // row differs by ordering. Demote to a non-blocking warning.
                if let Some(seq) = obs.source_event_global_sequence {
                    if order_nondet_demote.contains(&seq) {
                        stats.bump_kind("OrderNondeterministicWarning");
                        order_nondeterminism_warnings += 1;
                        consumed.insert(seq);
                        continue;
                    }
                    // Rule B: benign idempotent redis DELETE (recorded KeyDeleted vs
                    // observed KeyNotDeleted — key absent afterward either way).
                    if idempotent_delete_demote.contains(&seq) {
                        let kind = art
                            .events
                            .iter()
                            .find(|ev| ev.global_sequence == seq)
                            .and_then(event_reply_canon)
                            .map(|canon| format!("{}_warning", canon.preset_name()))
                            .unwrap_or_else(|| "IdempotentDeleteWarning".to_owned());
                        stats.bump_kind(&kind);
                        idempotent_delete_warnings += 1;
                        consumed.insert(seq);
                        continue;
                    }
                    if inconclusive_race.contains(&seq) {
                        stats.bump_kind("InconclusiveRace");
                        inconclusive_races += 1;
                        consumed.insert(seq);
                        continue;
                    }
                }
                // The args-aligned execute divergence is the ORIGIN of a
                // total-derivative cascade: the candidate ran the REAL boundary
                // (typically a READ) and got a value differing from the recorded
                // baseline (e.g. re-keyed read 0.10 -> 0.20). Tag it distinctly
                // (`ValueDivergedOrigin`) so the UI can tell the CAUSE (this read)
                // from the CONSEQUENCE (a downstream write paired args-free below).
                stats.bump_kind("ValueDivergedOrigin");
                value_divergences += 1;
                blocking_side_effect += 1;
                if let Some(corr) = &obs.correlation_id {
                    *corr_side_effect.entry(corr.clone()).or_insert(0) += 1;
                }
                if let Some(seq) = obs.source_event_global_sequence {
                    // Claim the recorded twin so the omitted pass does not also
                    // flag it; this is one logical write, classified once.
                    consumed.insert(seq);
                }
                continue;
            }
            stats.matched += 1;
            matched_side_effect_calls += 1;
            if let Some(seq) = obs.source_event_global_sequence {
                consumed.insert(seq);
            }
            let rank = obs.resolved_rank.unwrap_or(0);
            *resolved_by_rank.entry(rank_label(rank)).or_insert(0) += 1;
            *stats.resolved_by_rank.entry(rank_label(rank)).or_insert(0) += 1;
            if rank == POSITIONAL_FALLBACK_RANK {
                // The `rank5` field name is legacy (pre-renumber); it counts
                // positional (rank-6 `Sequence`) matches. Kept so persisted
                // scorecard JSON keeps one stable shape across runs.
                recovered_rank5_calls += 1;
                // Recovered is a fragility signal, not a divergence — track it
                // without bumping `diverged`.
                *stats.kinds.entry("Recovered".to_owned()).or_insert(0) += 1;
            }
        } else if tier_for(&obs.boundary) == Tier::Environmental {
            stats.bump_kind("EnvironmentalMiss");
            environmental_misses += 1;
        } else if is_nonblocking_boundary(&obs.boundary) {
            // Deterministic-live (crypto/time/id/rng) or the request boundary
            // (http_incoming) — not a real divergence. See is_nonblocking_boundary.
            stats.bump_kind("DeterministicMiss");
        } else if obs.correlation_id.is_none() && uncorrelated_tolerated {
            // Background-task call with no correlation — tolerated in V1.
            stats.bump_kind("NovelCall");
        } else if let Some((twin_seq, recorded)) = recorded_pairing
            .get_mut(&identity_of(
                &obs.correlation_id,
                &obs.boundary,
                &obs.method_name,
            ))
            .and_then(|q| {
                // Pop the next recorded twin for this identity, skipping any that a
                // resolved (args-aligned) call already claimed — so a mixed run that
                // resolves some calls normally and re-keys others never double-binds
                // a single recorded event.
                while let Some((seq, _)) = q.front() {
                    if consumed.contains(seq) {
                        q.pop_front();
                    } else {
                        return q.pop_front();
                    }
                }
                None
            })
        {
            // GOTCHA #1 resolution: this unresolved observed call pairs args-free
            // (correlation+boundary+method, FIFO occurrence) with a recorded twin
            // that the candidate "omitted" because its args were re-keyed. The
            // recorded WRITE (would-be Omitted) and the execute WRITE (would-be
            // Novel) are ONE logical write — classify it once.
            let twin_event = events_by_seq.get(&twin_seq).copied();
            let (recorded_val, observed_val) =
                args_free_effective_values(&recorded, obs, twin_event);
            let value_diverged =
                values_diverge_under_event(&obs.boundary, &recorded_val, &observed_val, twin_event);
            if value_diverged {
                if inconclusive_race
                    .attributable_downstream(obs.correlation_id.as_deref(), &obs.args)
                {
                    stats.bump_kind("InconclusiveRace");
                    inconclusive_races += 1;
                } else {
                    // Value diff under execute mode: the total-derivative catch.
                    stats.bump_kind("ValueDiverged");
                    value_divergences += 1;
                    blocking_side_effect += 1;
                    if let Some(corr) = &obs.correlation_id {
                        *corr_side_effect.entry(corr.clone()).or_insert(0) += 1;
                    }
                }
            } else {
                // Re-keyed but identical value — the write reproduced. Count it as
                // a (recovered) match rather than a Novel+Omitted split.
                stats.matched += 1;
                matched_side_effect_calls += 1;
            }
            // Either way the recorded twin is accounted for here, not omitted.
            paired_consumed.insert(twin_seq);
        } else if obs.seed_gap {
            // Execute-mode State call that ran the REAL boundary but found no
            // recorded baseline to compare against (no pairing either). Surface as
            // inconclusive rather than a false Novel — see InconclusiveSeedGap.
            stats.bump_kind("InconclusiveSeedGap");
            inconclusive_seed_gaps += 1;
        } else {
            stats.bump_kind("NovelCall");
            novel_calls += 1;
            blocking_side_effect += 1;
            if let Some(corr) = &obs.correlation_id {
                *corr_side_effect.entry(corr.clone()).or_insert(0) += 1;
            }
        }
    }

    // --- omitted calls: expected events the candidate never resolved ---------
    // `paired_consumed` are recorded twins already classified as ValueDiverged
    // (their execute-mode counterpart was paired args-free above); excluding them
    // here is what collapses a re-keyed write's Omitted+Novel split into ONE
    // ValueDiverged instead of double-counting.
    let mut omitted_calls = 0u64;
    for (seq, exp) in &expected {
        if consumed.contains(seq) || paired_consumed.contains(seq) {
            continue;
        }
        let boundary = exp.boundary.clone().unwrap_or_else(|| "unknown".to_owned());
        let stats = boundary_entry(&mut per_boundary, &boundary);
        stats.bump_kind("OmittedCall");
        if exp.correlation.is_none() && uncorrelated_tolerated {
            // tolerated
        } else if is_nonblocking_boundary(&boundary) {
            // tolerated: deterministic-live (crypto/time/id/rng) or the request
            // boundary (http_incoming). See is_nonblocking_boundary.
        } else {
            omitted_calls += 1;
            blocking_side_effect += 1;
            if let Some(corr) = &exp.correlation {
                *corr_side_effect.entry(corr.clone()).or_insert(0) += 1;
            }
        }
    }

    // --- post-finalization correlated work warnings --------------------------
    for warning in &undeclared_concurrency {
        let stats = boundary_entry(&mut per_boundary, &warning.boundary);
        *stats
            .kinds
            .entry(UNDECLARED_CONCURRENCY_WARNING.to_owned())
            .or_insert(0) += 1;
    }

    // --- HTTP response dimension (from the kernel) ---------------------------
    let mut http_status_mismatches = 0u64;
    let mut http_body_mismatches = 0u64;
    let mut corr_http: BTreeMap<String, (bool, bool)> = BTreeMap::new();
    {
        let stats = boundary_entry(&mut per_boundary, "http_incoming");
        for diff in &art.http_diffs {
            let blocking_body_diffs = blocking_http_body_diff_count(
                diff,
                http_incoming_by_correlation
                    .get(&diff.correlation_id)
                    .copied(),
                &inconclusive_race,
            );
            if diff.status_match && blocking_body_diffs == 0 {
                stats.matched += 1;
            }
            if !diff.status_match {
                http_status_mismatches += 1;
                stats.bump_kind("StatusMismatch");
            }
            if blocking_body_diffs > 0 {
                http_body_mismatches += 1;
                for _ in 0..blocking_body_diffs {
                    stats.bump_kind("BodyMismatch");
                }
            }
            let slot = corr_http
                .entry(diff.correlation_id.clone())
                .or_insert((true, true));
            slot.0 &= diff.status_match;
            slot.1 &= blocking_body_diffs == 0;
        }
    }

    // --- per-correlation outcomes --------------------------------------------
    let mut per_correlation = Vec::new();
    let mut matched_correlations = 0u64;
    for (corr, (status_match, body_match)) in &corr_http {
        let side_effect_divergences = corr_side_effect.get(corr).copied().unwrap_or(0);
        let passed = *status_match && *body_match && side_effect_divergences == 0;
        if passed {
            matched_correlations += 1;
        }
        per_correlation.push(CorrelationOutcome {
            correlation_id: corr.clone(),
            http_status_match: *status_match,
            http_body_match: *body_match,
            side_effect_divergences,
            passed,
        });
    }
    let total_correlations = per_correlation.len() as u64;

    // --- verdict --------------------------------------------------------------
    let nothing =
        art.table.entries.is_empty() && art.observed.is_empty() && art.http_diffs.is_empty();
    let mut reasons = Vec::new();
    if http_status_mismatches > 0 {
        reasons.push(format!("{http_status_mismatches} http status mismatch(es)"));
    }
    if http_body_mismatches > 0 {
        reasons.push(format!("{http_body_mismatches} http body mismatch(es)"));
    }
    if omitted_calls > 0 {
        reasons.push(format!("{omitted_calls} omitted side-effect call(s)"));
    }
    if novel_calls > 0 {
        reasons.push(format!("{novel_calls} novel side-effect call(s)"));
    }
    if value_divergences > 0 {
        // The total-derivative catch: a real-boundary value diff flips the
        // correlation to diverged (per-correlation `passed` already saw it via
        // `corr_side_effect`).
        reasons.push(format!("{value_divergences} value divergence(s)"));
    }
    // Seed gaps are reported but do NOT by themselves fail the verdict — a
    // missing baseline is inconclusive, not a divergence.
    if inconclusive_seed_gaps > 0 {
        reasons.push(format!(
            "{inconclusive_seed_gaps} inconclusive seed gap(s) (non-blocking)"
        ));
    }
    if inconclusive_races > 0 {
        reasons.push(format!(
            "{inconclusive_races} inconclusive_race row(s) recognized; auto-rerun recommended"
        ));
    }
    // Order-nondeterminism demotions (Rule A) are reported but non-blocking: a
    // concurrent same-row UPDATE-RETURNING interleaving whose final state matches
    // the recording is not a divergence.
    if order_nondeterminism_warnings > 0 {
        reasons.push(format!(
            "{order_nondeterminism_warnings} order-nondeterminism warning(s) (non-blocking)"
        ));
    }
    if idempotent_delete_warnings > 0 {
        reasons.push(format!(
            "{idempotent_delete_warnings} idempotent-delete warning(s) (non-blocking)"
        ));
    }
    if undeclared_concurrency_warnings > 0 {
        reasons.push(format!(
            "{undeclared_concurrency_warnings} undeclared_concurrency warning(s) (non-blocking)"
        ));
    }
    // Seed-gap + race + order-nondeterminism + idempotent-delete +
    // undeclared_concurrency lines are informational, not divergences; exclude
    // them from the blocking count so a run whose only "reasons" are those still
    // avoids a blocking failure (race becomes an explicit inconclusive verdict).
    let blocking_reasons = reasons.len()
        - usize::from(inconclusive_seed_gaps > 0)
        - usize::from(inconclusive_races > 0)
        - usize::from(order_nondeterminism_warnings > 0)
        - usize::from(idempotent_delete_warnings > 0)
        - usize::from(undeclared_concurrency_warnings > 0);
    let inconclusive = nothing || (inconclusive_races > 0 && blocking_reasons == 0);
    let pass = !inconclusive && blocking_reasons == 0;
    let reason = if nothing {
        "no artifacts ingested for this run yet".to_owned()
    } else if inconclusive {
        reasons.join("; ")
    } else if pass && reasons.is_empty() {
        "full-mock replay clean: http responses match and every side-effect call resolved"
            .to_owned()
    } else {
        reasons.join("; ")
    };

    let mut warnings = art.warnings.clone();
    for (seq, row) in &order_nondet_demote.row_labels {
        let canon = order_nondet_demote
            .canon_label(seq)
            .map(|label| format!(" canon={label}"))
            .unwrap_or_default();
        warnings.push(format!(
            "Rule A order-nondeterminism demoted event {seq} on db row {row}{canon}"
        ));
    }
    for (seq, row) in &inconclusive_race.row_labels {
        warnings.push(format!(
            "inconclusive_race event {seq} on db row {row}: auto-rerun recommended"
        ));
    }
    for warning in &undeclared_concurrency {
        warnings.push(format!(
            "{}: event_seq={} correlation_id={} boundary={} method={} timestamp_ns={} response_finalized_ns={}",
            UNDECLARED_CONCURRENCY_WARNING,
            warning
                .source_event_global_sequence
                .map(|seq| seq.to_string())
                .unwrap_or_else(|| "unknown".to_owned()),
            warning.correlation_id,
            warning.boundary,
            warning.method,
            warning.timestamp_ns,
            warning.response_finalized_ns
        ));
    }

    Scorecard {
        schema_version: 1,
        r#type: "replay-scorecard".to_owned(),
        run_id: art.run_id.clone(),
        recording_id: art.recording_id.clone(),
        summary: Summary {
            total_correlations,
            matched_correlations,
            http_status_mismatches,
            http_body_mismatches,
            side_effect_divergences: blocking_side_effect,
            matched_side_effect_calls,
            omitted_calls,
            novel_calls,
            value_divergences,
            order_nondeterminism_warnings,
            idempotent_delete_warnings,
            undeclared_concurrency_warnings,
            inconclusive_seed_gaps,
            inconclusive_races,
            environmental_misses,
            recovered_rank5_calls,
            resolved_by_rank,
            uncorrelated_events_seen,
            uncorrelated_events_tolerated: uncorrelated_tolerated,
        },
        per_boundary,
        per_correlation,
        verdict: Verdict {
            pass,
            inconclusive,
            reason,
        },
        warnings,
    }
}

// ---------------------------------------------------------------------------
// Loading + scoring
// ---------------------------------------------------------------------------

/// Load a run's three artifact streams off disk. Missing files are treated as
/// empty (a run mid-flight); parse failures are surfaced as `warnings` rather
/// than silently dropped, so a corrupt stream can't masquerade as a clean run.
pub fn load_artifacts(root: &HarnessRoot, run_id: &str) -> io::Result<RunArtifacts> {
    let recording_id = crate::read_json::<crate::Run>(&root.run_path(run_id))
        .ok()
        .and_then(|run| run.recording_id.or(run.spec.recording_id));

    let mut warnings = Vec::new();
    let table = load_table(&root.lookup_table_path(run_id), &mut warnings);
    let observed = load_jsonl::<ObservedCall>(&root.observed_path(run_id), &mut warnings);
    let http_diffs = load_jsonl::<HttpDiff>(&root.http_diff_path(run_id), &mut warnings);
    let events = match &recording_id {
        Some(rec) => {
            load_jsonl::<deja::BoundaryEvent>(&root.recording_events_path(rec), &mut warnings)
        }
        None => Vec::new(),
    };

    Ok(RunArtifacts {
        run_id: run_id.to_owned(),
        recording_id,
        table,
        observed,
        http_diffs,
        events,
        warnings,
    })
}

/// Load + detect (read-through). Used by `GET /runs/{id}/scorecard`.
pub fn scorecard(root: &HarnessRoot, run_id: &str) -> io::Result<Scorecard> {
    let art = load_artifacts(root, run_id)?;
    Ok(detect(&art))
}

/// Compute the scorecard and persist it next to the run record. Called by the
/// lifecycle worker when a run completes. Also builds + persists the per-call
/// ledger sidecar (best-effort — a ledger failure never fails scoring).
pub fn detect_and_score(root: &HarnessRoot, run_id: &str) -> io::Result<Scorecard> {
    let art = load_artifacts(root, run_id)?;
    let card = detect(&art);
    let path = root
        .root
        .join("runs")
        .join(format!("{run_id}.scorecard.json"));
    crate::write_json(&path, &card)?;

    // Ledger: the per-call detail the scorecard summary drops. Best-effort.
    match build_ledger(root, &art) {
        Ok(rows) => {
            if let Err(e) = write_ledger(&root.call_ledger_path(run_id), &rows) {
                eprintln!("divergence: ledger write failed for {run_id}: {e}");
            }
        }
        Err(e) => eprintln!("divergence: ledger build failed for {run_id}: {e}"),
    }
    Ok(card)
}

/// Build the per-call ledger for a run: join the recording's events (recorded
/// side) to the candidate's observed calls, classified like `detect()`.
pub fn build_ledger(root: &HarnessRoot, art: &RunArtifacts) -> io::Result<Vec<CallRecord>> {
    let events = match &art.recording_id {
        Some(rec) => {
            let mut warnings = Vec::new();
            load_jsonl::<deja::BoundaryEvent>(&root.recording_events_path(rec), &mut warnings)
        }
        None => Vec::new(),
    };
    let expected = ledger::expected_sequences(&art.table);
    let span_paths = ledger::recorded_span_paths(&art.table);
    // Mirror scorecard classification: discover race evidence under status-clean
    // HTTP first, then treat only unattributable body diffs as blocking.
    let http_status_clean =
        !art.http_diffs.is_empty() && art.http_diffs.iter().all(|d| d.status_match);
    let http_incoming_by_correlation = http_incoming_events_by_correlation(&events);
    let inconclusive_race =
        inconclusive_race_evidence(&events, &art.observed, http_status_clean, &span_paths);
    let blocking_http_body_mismatches = art
        .http_diffs
        .iter()
        .filter(|diff| {
            blocking_http_body_diff_count(
                diff,
                http_incoming_by_correlation
                    .get(&diff.correlation_id)
                    .copied(),
                &inconclusive_race,
            ) > 0
        })
        .count();
    let http_clean = http_status_clean && blocking_http_body_mismatches == 0;
    let demote = order_nondeterministic_demotions(&events, &art.observed, http_clean);
    let idempotent_delete = idempotent_delete_demotions(&events, &art.observed, http_clean);
    Ok(ledger::build_with_inconclusive(
        &events,
        &art.observed,
        &expected,
        &span_paths,
        &demote.sequences,
        &idempotent_delete,
        &inconclusive_race,
    ))
}

/// Read-through ledger for `GET /runs/{id}/calls` (recomputes from artifacts;
/// works for runs scored before the sidecar existed).
pub fn call_ledger(root: &HarnessRoot, run_id: &str) -> io::Result<Vec<CallRecord>> {
    let art = load_artifacts(root, run_id)?;
    build_ledger(root, &art)
}

fn write_ledger(path: &std::path::Path, rows: &[CallRecord]) -> io::Result<()> {
    use std::io::Write as _;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut out = std::io::BufWriter::new(std::fs::File::create(path)?);
    for row in rows {
        let line = serde_json::to_vec(row).map_err(io::Error::other)?;
        out.write_all(&line)?;
        out.write_all(b"\n")?;
    }
    out.flush()
}

fn load_table(path: &std::path::Path, warnings: &mut Vec<String>) -> LookupTable {
    let empty = || LookupTable {
        recording_id: String::new(),
        policy_version: 0,
        entries: Vec::new(),
    };
    if !path.exists() {
        return empty();
    }
    let mut source = LocalFileLookupSource::new(path);
    match source.load() {
        Ok(table) => table,
        Err(e) => {
            warnings.push(format!(
                "lookup-table load failed ({}): {e}",
                path.display()
            ));
            empty()
        }
    }
}

fn load_jsonl<T: for<'de> Deserialize<'de>>(
    path: &std::path::Path,
    warnings: &mut Vec<String>,
) -> Vec<T> {
    let content = match std::fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Vec::new(),
        Err(e) => {
            warnings.push(format!("read {} failed: {e}", path.display()));
            return Vec::new();
        }
    };
    let mut out = Vec::new();
    for (i, line) in content.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<T>(line) {
            Ok(value) => out.push(value),
            Err(e) => warnings.push(format!("{}:{}: parse error: {e}", path.display(), i + 1)),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use deja::{LookupEntry, LookupKey};
    use deja_kernel::JsonFieldDiff;

    #[test]
    fn canon_presets_resolve_and_compare_their_declared_shapes() {
        let final_state = resolve_canon(Some(&deja::CanonRef::new("final_state")))
            .expect("final_state preset resolves");
        assert!(
            final_state.equivalent(
                &serde_json::json!({"value": [{"status": "pending"}, {"status": "charged"}]}),
                &serde_json::json!({"value": [{"status": "authorized"}, {"status": "charged"}]})
            ),
            "final_state compares the terminal row, not every transient row"
        );
        assert!(
            !final_state.equivalent(
                &serde_json::json!({"value": [{"status": "charged"}]}),
                &serde_json::json!({"value": [{"status": "pending"}]})
            ),
            "final_state must not hide a different terminal row"
        );

        let absent_after = resolve_canon(Some(&deja::CanonRef::new("absent_after")))
            .expect("absent_after preset resolves");
        assert!(
            absent_after.equivalent(
                &serde_json::json!("KeyDeleted"),
                &serde_json::json!("KeyNotDeleted")
            ),
            "absent_after treats both delete replies as absent-after outcomes"
        );
        assert!(
            !absent_after.equivalent(
                &serde_json::json!("KeyNotDeleted"),
                &serde_json::json!("Value")
            ),
            "absent_after must not hide a present value"
        );

        let project = resolve_canon(Some(&deja::CanonRef::new("project:result,kind")))
            .expect("project preset resolves");
        assert!(
            project.equivalent(
                &serde_json::json!({"result": "Err", "kind": "NotFound", "message": "line 1"}),
                &serde_json::json!({"result": "Err", "kind": "NotFound", "message": "line 2"})
            ),
            "project compares only the selected fields"
        );
        assert!(
            !project.equivalent(
                &serde_json::json!({"result": "Err", "kind": "NotFound"}),
                &serde_json::json!({"result": "Err", "kind": "UniqueViolation"})
            ),
            "project must not hide selected-field changes"
        );
    }

    #[test]
    fn db_infra_only_diff_is_not_a_divergence() {
        // A db insert that differs ONLY in its integer serial id is equivalent
        // (the replay DB assigned id=1 from its fresh sequence; record saw id=2).
        let rec = serde_json::json!({"result":"Ok","type_name":"UserRole",
            "value":{"id":2,"user_id":"u-abc","role_id":"org_admin","status":"Active"}});
        let obs = serde_json::json!({"result":"Ok","type_name":"UserRole",
            "value":{"id":1,"user_id":"u-abc","role_id":"org_admin","status":"Active"}});
        assert!(
            db_equiv_modulo_infra(&rec, &obs),
            "serial-id-only diff must be equivalent"
        );

        // A diff in a REAL field (string id, or any value) is a genuine divergence.
        let obs_real = serde_json::json!({"result":"Ok","type_name":"UserRole",
            "value":{"id":1,"user_id":"u-DIFFERENT","role_id":"org_admin","status":"Active"}});
        assert!(
            !db_equiv_modulo_infra(&rec, &obs_real),
            "a real field diff must NOT be masked"
        );

        // An app-set STRING id is not an integer → stays compared.
        let s1 = serde_json::json!({"value":{"id":"pay_aaa"}});
        let s2 = serde_json::json!({"value":{"id":"pay_bbb"}});
        assert!(
            !db_equiv_modulo_infra(&s1, &s2),
            "string ids are app-set, not serial → compared"
        );

        let err_a = serde_json::json!({"result":"Err","kind":"NotFound","version":1,
            "message":"The requested resource was not found\n├╴at crates/diesel_models/src/query/generics.rs:601:38\n╰╴at crates/diesel_models/src/query/generics.rs:601:25"});
        let err_b = serde_json::json!({"result":"Err","kind":"NotFound","version":1,
            "message":"The requested resource was not found\n├╴at crates/diesel_models/src/query/generics.rs:648:38\n╰╴at crates/diesel_models/src/query/generics.rs:648:25"});
        assert!(
            db_equiv_modulo_infra(&err_a, &err_b),
            "structured DB errors with the same kind ignore diagnostic source locations"
        );

        let err_message_drift = serde_json::json!({"result":"Err","kind":"NotFound","version":1,
            "message":"different diagnostics for the same deterministic DB error kind"});
        assert!(
            db_equiv_modulo_infra(&err_a, &err_message_drift),
            "structured DB errors with the same kind ignore diagnostic message drift"
        );

        let err_real = serde_json::json!({"result":"Err","kind":"UniqueViolation","version":1,
            "message":"The requested resource was not found\n├╴at crates/diesel_models/src/query/generics.rs:648:38"});
        assert!(
            !db_equiv_modulo_infra(&err_a, &err_real),
            "structured DB error kind changes must remain divergent"
        );

        // Identical rows are trivially equivalent; redis (non-db) is unaffected here.
        assert!(db_equiv_modulo_infra(&rec, &rec));
    }

    fn obs(
        boundary: &str,
        corr: Option<&str>,
        resolved: bool,
        rank: Option<u8>,
        src: Option<u64>,
    ) -> ObservedCall {
        ObservedCall {
            correlation_id: corr.map(str::to_owned),
            boundary: boundary.to_owned(),
            trait_name: "T".to_owned(),
            method_name: "m".to_owned(),
            args: serde_json::json!({}),
            resolved,
            resolved_rank: rank,
            source_event_global_sequence: src,
            timestamp_ns: 0,
            end_timestamp_ns: None,
            detached: false,
            task_id: Some("root".to_owned()),
            parent_task_id: None,
            task_bucket: Some("root".to_owned()),
            bucket_id: Some("root".to_owned()),
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
            provenance: deja::Provenance::default(),
            seed_gap: false,
            pre_image: None,
            result_image: None,
        }
    }

    fn seq_entry(corr: Option<&str>, boundary: &str, src: u64) -> LookupEntry {
        seq_entry_res(corr, boundary, src, serde_json::json!("v"))
    }

    /// A rank-6 `Sequence` entry with an explicit recorded `result` — lets a test
    /// set the recorded operand the args-free value pairing compares against.
    fn seq_entry_res(
        corr: Option<&str>,
        boundary: &str,
        src: u64,
        result: serde_json::Value,
    ) -> LookupEntry {
        LookupEntry {
            key: LookupKey {
                correlation_id: corr.map(str::to_owned),
                bucket_id: Some("root".to_owned()),
                fork_seq: 0,
                address: Address::Sequence {
                    boundary: boundary.to_owned(),
                    method: "m".to_owned(),
                    request_sequence: 0,
                },
                args_hash: 0,
                occurrence: 0,
            },
            result,
            source_event_global_sequence: src,
        }
    }

    fn span_entry_res(
        corr: Option<&str>,
        src: u64,
        path: &str,
        result: serde_json::Value,
    ) -> LookupEntry {
        LookupEntry {
            key: LookupKey {
                correlation_id: corr.map(str::to_owned),
                bucket_id: Some("root".to_owned()),
                fork_seq: 0,
                address: Address::SpanPath {
                    path: path.to_owned(),
                },
                args_hash: 0,
                occurrence: 0,
            },
            result,
            source_event_global_sequence: src,
        }
    }

    fn write_jsonl_rows<T: serde::Serialize>(path: &std::path::Path, rows: &[T]) {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).unwrap();
        }
        let mut file = std::fs::File::create(path).unwrap();
        for row in rows {
            serde_json::to_writer(&mut file, row).unwrap();
            use std::io::Write;
            file.write_all(b"\n").unwrap();
        }
    }

    /// An execute-shadow observed call: the candidate ran the REAL boundary
    /// (`provenance = Shadow`) and produced `observed`. `recorded` is the
    /// baseline the hook located (or `None` => `seed_gap`), `resolved` reflects
    /// whether args still aligned to that baseline.
    fn exec_obs(
        boundary: &str,
        corr: Option<&str>,
        resolved: bool,
        src: Option<u64>,
        recorded: Option<serde_json::Value>,
        observed: serde_json::Value,
    ) -> ObservedCall {
        let mut o = obs(boundary, corr, resolved, resolved.then_some(3), src);
        o.provenance = deja::Provenance::Shadow;
        o.seed_gap = recorded.is_none();
        o.recorded_result = recorded;
        o.observed_result = Some(observed);
        o
    }

    fn seq_entry_method_res(
        corr: Option<&str>,
        boundary: &str,
        method: &str,
        src: u64,
        result: serde_json::Value,
    ) -> LookupEntry {
        let mut entry = seq_entry_res(corr, boundary, src, result);
        if let Address::Sequence { method: m, .. } = &mut entry.key.address {
            *m = method.to_owned();
        }
        entry
    }

    fn exec_obs_method(
        boundary: &str,
        corr: Option<&str>,
        method: &str,
        resolved: bool,
        src: Option<u64>,
        recorded: Option<serde_json::Value>,
        observed: serde_json::Value,
    ) -> ObservedCall {
        let mut o = exec_obs(boundary, corr, resolved, src, recorded, observed);
        o.method_name = method.to_owned();
        o
    }

    fn substituted_obs_method(
        boundary: &str,
        corr: Option<&str>,
        method: &str,
        src: u64,
        result: serde_json::Value,
    ) -> ObservedCall {
        let mut o = obs(boundary, corr, true, Some(3), Some(src));
        o.method_name = method.to_owned();
        o.recorded_result = Some(result.clone());
        o.observed_result = Some(result);
        o
    }

    fn kind_count(card: &Scorecard, boundary: &str, kind: &str) -> u64 {
        card.per_boundary
            .get(boundary)
            .and_then(|stats| stats.kinds.get(kind))
            .copied()
            .unwrap_or(0)
    }

    fn http(corr: &str, status_match: bool, body: Vec<JsonFieldDiff>) -> HttpDiff {
        HttpDiff {
            correlation_id: corr.to_owned(),
            request_sequence: 0,
            request_path: "/p".to_owned(),
            status_baseline: 200,
            status_candidate: if status_match { 200 } else { 500 },
            status_match,
            body_diff: body,
            baseline_body: None,
            candidate_body: None,
        }
    }

    fn art(
        entries: Vec<LookupEntry>,
        observed: Vec<ObservedCall>,
        http: Vec<HttpDiff>,
    ) -> RunArtifacts {
        RunArtifacts {
            run_id: "run-1".to_owned(),
            recording_id: Some("rec-1".to_owned()),
            table: LookupTable {
                recording_id: "rec-1".to_owned(),
                policy_version: 1,
                entries,
            },
            observed,
            http_diffs: http,
            events: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Like `art` but with recording events attached (for order-nondeterminism tests).
    fn art_with_events(
        entries: Vec<LookupEntry>,
        observed: Vec<ObservedCall>,
        http: Vec<HttpDiff>,
        events: Vec<deja::BoundaryEvent>,
    ) -> RunArtifacts {
        let mut a = art(entries, observed, http);
        a.events = events;
        a
    }

    fn http_with_bodies(
        corr: &str,
        status_match: bool,
        body: Vec<JsonFieldDiff>,
        baseline_body: serde_json::Value,
        candidate_body: serde_json::Value,
    ) -> HttpDiff {
        let mut diff = http(corr, status_match, body);
        diff.baseline_body = Some(baseline_body);
        diff.candidate_body = Some(candidate_body);
        diff
    }

    fn db_read_ev_with_state_canon(
        corr: &str,
        table: &str,
        seq: u64,
        row: serde_json::Value,
        canon: &str,
    ) -> deja::BoundaryEvent {
        let mut ev = db_read_ev(corr, table, seq, row, 100, 110, "root", 0);
        let declaration = ev
            .declaration
            .take()
            .expect("db_read_ev stamps a declaration")
            .state_canon(deja::CanonRef::new(canon));
        ev.declaration = Some(declaration);
        ev
    }

    fn http_incoming_ev_with_reply_canon(
        corr: &str,
        seq: u64,
        reply_canon: Option<&str>,
        recorded_body: serde_json::Value,
    ) -> deja::BoundaryEvent {
        let mut ev = db_read_ev(
            corr,
            "http_response",
            seq,
            serde_json::json!({"id": "not-db-state"}),
            100,
            110,
            "root",
            0,
        );
        ev.boundary = "http_incoming".to_owned();
        ev.trait_name = "HttpIngress".to_owned();
        ev.method_name = "reply".to_owned();
        ev.result = recorded_body;
        ev.read_set.clear();
        ev.write_set.clear();
        ev.declaration = reply_canon.map(|canon| {
            deja::BoundaryDeclaration::default().reply_canon(deja::CanonRef::new(canon))
        });
        ev
    }

    #[test]
    fn declared_db_project_canon_keeps_volatile_row_drift_nonblocking_and_guards_real_columns() {
        const DB_VOLATILE_PROJECT_CANON: &str = "project:!created_at,!last_synced,!modified_at";
        let corr = "declared-db-project-canon";
        let volatile_seq = 401;
        let guard_seq = 402;

        let recorded_volatile = serde_json::json!({
            "attempt_id": "pay_1",
            "status": "charged",
            "amount": 100,
            "created_at": "2026-07-06T10:00:00.000Z",
            "last_synced": "2026-07-06T10:00:01.000Z",
            "modified_at": "2026-07-06T10:00:02.000Z",
        });
        let observed_volatile = serde_json::json!({
            "attempt_id": "pay_1",
            "status": "charged",
            "amount": 100,
            "created_at": "2026-07-06T10:10:00.000Z",
            "last_synced": "2026-07-06T10:10:01.000Z",
            "modified_at": "2026-07-06T10:10:02.000Z",
        });
        let recorded_guard = serde_json::json!({
            "attempt_id": "pay_2",
            "status": "authorized",
            "amount": 100,
            "created_at": "2026-07-06T10:00:00.000Z",
            "last_synced": "2026-07-06T10:00:01.000Z",
            "modified_at": "2026-07-06T10:00:02.000Z",
        });
        let observed_guard = serde_json::json!({
            "attempt_id": "pay_2",
            "status": "charged",
            "amount": 100,
            "created_at": "2026-07-06T10:10:00.000Z",
            "last_synced": "2026-07-06T10:10:01.000Z",
            "modified_at": "2026-07-06T10:10:02.000Z",
        });

        let volatile_recorded_result = envelope(recorded_volatile.clone());
        let volatile_observed_result = envelope(observed_volatile.clone());
        let guard_recorded_result = envelope(recorded_guard.clone());
        let guard_observed_result = envelope(observed_guard.clone());
        let entries = vec![
            seq_entry_method_res(
                Some(corr),
                "db",
                "generic_find_one",
                volatile_seq,
                volatile_recorded_result.clone(),
            ),
            seq_entry_method_res(
                Some(corr),
                "db",
                "generic_find_one",
                guard_seq,
                guard_recorded_result.clone(),
            ),
        ];
        let observed = vec![
            exec_obs_method(
                "db",
                Some(corr),
                "generic_find_one",
                true,
                Some(volatile_seq),
                Some(volatile_recorded_result.clone()),
                volatile_observed_result,
            ),
            exec_obs_method(
                "db",
                Some(corr),
                "generic_find_one",
                true,
                Some(guard_seq),
                Some(guard_recorded_result.clone()),
                guard_observed_result,
            ),
        ];
        let events = vec![
            db_read_ev_with_state_canon(
                corr,
                "payment_attempt",
                volatile_seq,
                recorded_volatile,
                DB_VOLATILE_PROJECT_CANON,
            ),
            db_read_ev_with_state_canon(
                corr,
                "payment_attempt",
                guard_seq,
                recorded_guard,
                DB_VOLATILE_PROJECT_CANON,
            ),
        ];

        let card = detect(&art_with_events(
            entries.clone(),
            observed.clone(),
            vec![http(corr, true, vec![])],
            events.clone(),
        ));
        assert_eq!(
            card.summary.value_divergences, 1,
            "only the non-volatile status drift should be a value divergence"
        );
        assert_eq!(card.summary.side_effect_divergences, 1);
        assert_eq!(
            card.summary.matched_side_effect_calls, 1,
            "volatile-only row drift is a successful DB side-effect match"
        );
        assert_eq!(kind_count(&card, "db", "ValueDivergedOrigin"), 1);
        assert!(!card.verdict.pass, "real status drift must still block");

        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        write_jsonl_rows(&root.recording_events_path("rec-1"), &events);
        let rows = build_ledger(
            &root,
            &RunArtifacts {
                run_id: "run-db-volatile-canon-ledger".to_owned(),
                recording_id: Some("rec-1".to_owned()),
                table: LookupTable {
                    recording_id: "rec-1".to_owned(),
                    policy_version: 1,
                    entries,
                },
                observed,
                http_diffs: vec![http(corr, true, vec![])],
                events: Vec::new(),
                warnings: Vec::new(),
            },
        )
        .unwrap();
        let volatile_row = rows
            .iter()
            .find(|row| row.source_event_global_sequence == Some(volatile_seq))
            .unwrap();
        assert_eq!(volatile_row.kind, "matched");
        assert!(
            !volatile_row.blocking,
            "declared volatile DB row drift must not block in the ledger"
        );
        let guard_row = rows
            .iter()
            .find(|row| row.source_event_global_sequence == Some(guard_seq))
            .unwrap();
        assert_eq!(guard_row.kind, "value_diverged");
        assert!(guard_row.origin);
        assert!(
            guard_row.blocking,
            "the same Project canon must not hide non-volatile row drift"
        );
    }

    #[test]
    fn undeclared_db_timestamp_drift_remains_blocking() {
        let corr = "undeclared-db-timestamp-drift";
        let seq = 410;
        let recorded = serde_json::json!({
            "attempt_id": "pay_1",
            "status": "charged",
            "created_at": "2026-07-06T10:00:00.000Z",
        });
        let observed_row = serde_json::json!({
            "attempt_id": "pay_1",
            "status": "charged",
            "created_at": "2026-07-06T10:10:00.000Z",
        });
        let recorded_result = envelope(recorded.clone());

        let card = detect(&art_with_events(
            vec![seq_entry_method_res(
                Some(corr),
                "db",
                "generic_find_one",
                seq,
                recorded_result.clone(),
            )],
            vec![exec_obs_method(
                "db",
                Some(corr),
                "generic_find_one",
                true,
                Some(seq),
                Some(recorded_result.clone()),
                envelope(observed_row),
            )],
            vec![http(corr, true, vec![])],
            vec![db_read_ev(
                corr,
                "payment_attempt",
                seq,
                recorded,
                100,
                110,
                "root",
                0,
            )],
        ));

        assert_eq!(
            card.summary.value_divergences, 1,
            "timestamp drift is blocking unless the DB event declares the Project canon"
        );
        assert_eq!(card.summary.side_effect_divergences, 1);
        assert!(!card.verdict.pass);
    }

    #[test]
    fn declared_http_reply_project_canon_absorbs_created_body_diff_without_race() {
        let corr = "declared-http-reply-canon";
        let baseline_body = serde_json::json!({
            "id": "resp_1",
            "created": "2026-07-06T10:00:00.000Z",
            "amount": 100,
        });
        let candidate_body = serde_json::json!({
            "id": "resp_1",
            "created": "2026-07-06T10:00:01.000Z",
            "amount": 100,
        });

        let card = detect(&art_with_events(
            vec![],
            vec![],
            vec![http_with_bodies(
                corr,
                true,
                vec![JsonFieldDiff {
                    json_path: "$.created".to_owned(),
                    baseline: serde_json::json!("2026-07-06T10:00:00.000Z"),
                    candidate: serde_json::json!("2026-07-06T10:00:01.000Z"),
                }],
                baseline_body.clone(),
                candidate_body,
            )],
            vec![http_incoming_ev_with_reply_canon(
                corr,
                501,
                Some("project:!created_at,!last_synced,!modified_at"),
                baseline_body,
            )],
        ));

        assert_eq!(card.summary.http_status_mismatches, 0);
        assert_eq!(
            card.summary.http_body_mismatches, 0,
            "declared HTTP reply Project canon absorbs only the created field drift"
        );
        assert_eq!(
            card.summary.inconclusive_races, 0,
            "$.created absorption is declared reply canon behavior, not race attribution"
        );
        assert_eq!(card.summary.value_divergences, 0);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    #[test]
    fn http_created_body_diff_without_reply_canon_remains_blocking() {
        let corr = "undeclared-http-created-drift";
        let baseline_body = serde_json::json!({
            "id": "resp_1",
            "created": "2026-07-06T10:00:00.000Z",
            "amount": 100,
        });
        let candidate_body = serde_json::json!({
            "id": "resp_1",
            "created": "2026-07-06T10:00:01.000Z",
            "amount": 100,
        });

        let card = detect(&art_with_events(
            vec![],
            vec![],
            vec![http_with_bodies(
                corr,
                true,
                vec![JsonFieldDiff {
                    json_path: "$.created".to_owned(),
                    baseline: serde_json::json!("2026-07-06T10:00:00.000Z"),
                    candidate: serde_json::json!("2026-07-06T10:00:01.000Z"),
                }],
                baseline_body.clone(),
                candidate_body,
            )],
            vec![http_incoming_ev_with_reply_canon(
                corr,
                502,
                None,
                baseline_body,
            )],
        ));

        assert_eq!(card.summary.inconclusive_races, 0);
        assert_eq!(
            card.summary.http_body_mismatches, 1,
            "$.created drift blocks when the recorded http_incoming event lacks reply_canon"
        );
        assert!(!card.verdict.inconclusive);
        assert!(!card.verdict.pass);
        assert!(
            card.verdict.reason.contains("http body mismatch"),
            "{}",
            card.verdict.reason
        );
    }

    #[test]
    fn clean_self_replay_passes() {
        let card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 7)],
            vec![obs("redis", Some("c1"), true, Some(3), Some(7))],
            vec![http("c1", true, vec![])],
        ));
        assert!(card.verdict.pass, "{}", card.verdict.reason);
        assert_eq!(card.summary.omitted_calls, 0);
        assert_eq!(card.summary.novel_calls, 0);
        assert_eq!(card.summary.matched_correlations, 1);
        assert_eq!(card.summary.resolved_by_rank.get("rank_3"), Some(&1));
    }

    fn observed_finalizer(corr: &str, response_finalized_ns: u64) -> ObservedCall {
        let mut o = obs("http_incoming", Some(corr), false, None, None);
        o.method_name = "finalize".to_owned();
        o.timestamp_ns = response_finalized_ns.saturating_sub(10_000);
        o.end_timestamp_ns = Some(response_finalized_ns);
        o
    }

    fn observed_at(
        boundary: &str,
        corr: Option<&str>,
        method: &str,
        src: Option<u64>,
        timestamp_ns: u64,
        detached: bool,
    ) -> ObservedCall {
        let mut o = obs(boundary, corr, src.is_some(), src.map(|_| 3), src);
        o.method_name = method.to_owned();
        o.timestamp_ns = timestamp_ns;
        o.detached = detached;
        o
    }

    #[test]
    fn undeclared_concurrency_warns_for_correlated_post_finalization_work() {
        let card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 2)],
            vec![
                observed_finalizer("c1", 11_000),
                observed_at("redis", Some("c1"), "set_key", Some(2), 11_001, false),
            ],
            vec![http("c1", true, vec![])],
        ));

        assert!(card.verdict.pass, "{}", card.verdict.reason);
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert_eq!(card.summary.undeclared_concurrency_warnings, 1);
        assert_eq!(
            kind_count(&card, "redis", UNDECLARED_CONCURRENCY_WARNING),
            1
        );
        assert_eq!(
            kind_count(&card, "http_incoming", "DeterministicMiss"),
            0,
            "finalizer sentinel must not be classified as an observed call"
        );
        assert!(card
            .warnings
            .iter()
            .any(|warning| warning.starts_with("undeclared_concurrency: event_seq=2 ")));
    }

    #[test]
    fn undeclared_concurrency_ignores_detached_post_finalization_work() {
        let card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 2)],
            vec![
                observed_finalizer("c1", 11_000),
                observed_at("redis", Some("c1"), "set_key", Some(2), 11_001, true),
            ],
            vec![http("c1", true, vec![])],
        ));

        assert!(card.verdict.pass, "{}", card.verdict.reason);
        assert_eq!(card.summary.undeclared_concurrency_warnings, 0);
        assert_eq!(
            kind_count(&card, "redis", UNDECLARED_CONCURRENCY_WARNING),
            0
        );
        assert_eq!(
            kind_count(&card, "http_incoming", "DeterministicMiss"),
            0,
            "finalizer sentinel must not be classified as an observed call"
        );
        assert!(!card
            .warnings
            .iter()
            .any(|warning| warning.starts_with("undeclared_concurrency:")));
    }

    // ---- Rule A: order-nondeterminism demotion (cycle-25 payment_attempt case) --

    fn envelope(row: serde_json::Value) -> serde_json::Value {
        serde_json::json!({"result": "Ok", "value": [row]})
    }
    fn db_update_ev(
        corr: &str,
        table: &str,
        seq: u64,
        row: serde_json::Value,
        start_ns: u64,
        end_ns: u64,
    ) -> deja::BoundaryEvent {
        let result = envelope(row);
        let write_set = deja::db::row_state_keys(table, &result)
            .into_iter()
            .map(|key| key.to_wire())
            .collect::<Vec<_>>();
        serde_json::from_value(serde_json::json!({
            "global_sequence": seq,
            "request_sequence": 0,
            "correlation_id": corr,
            "timestamp_ns": start_ns,
            "end_timestamp_ns": end_ns,
            "boundary": "db",
            "trait_name": "diesel_models::query::generics",
            "method_name": "generic_update_with_results",
            "call_file": "crates/diesel_models/src/query/generics.rs",
            "call_line": 344,
            "call_column": 0,
            "request": {},
            "args": {"table": table},
            "response": {},
            "result": result,
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "read_set": [],
            "write_set": write_set,
            "replay_strategy": "execute",
        }))
        .expect("valid BoundaryEvent")
    }

    fn declared_db_update_ev(
        corr: &str,
        table: &str,
        seq: u64,
        row: serde_json::Value,
        start_ns: u64,
        end_ns: u64,
    ) -> deja::BoundaryEvent {
        let mut ev = db_update_ev(corr, table, seq, row, start_ns, end_ns);
        ev.method_name = "commit_payment_attempt_row".to_owned();
        ev.declaration = Some(
            deja::BoundaryDeclaration::default()
                .effect(deja::EffectKind::Db)
                .operation(deja::OperationKind::Update)
                .returns(deja::ReturnSemantics::UpdateReturning),
        );
        ev
    }

    fn declared_db_update_ev_with_state_canon(
        corr: &str,
        table: &str,
        seq: u64,
        row: serde_json::Value,
        start_ns: u64,
        end_ns: u64,
        canon: &str,
    ) -> deja::BoundaryEvent {
        let mut ev = declared_db_update_ev(corr, table, seq, row, start_ns, end_ns);
        let declaration = ev
            .declaration
            .take()
            .expect("declared_db_update_ev stamps a declaration")
            .state_canon(deja::CanonRef::new(canon));
        ev.declaration = Some(declaration);
        ev
    }

    // Test fixture builder: positional args mirror the event's wire order.
    #[allow(clippy::too_many_arguments)]
    fn db_read_ev(
        corr: &str,
        table: &str,
        seq: u64,
        row: serde_json::Value,
        start_ns: u64,
        end_ns: u64,
        bucket_id: &str,
        fork_seq: u64,
    ) -> deja::BoundaryEvent {
        let result = envelope(row);
        let read_set = deja::db::row_state_keys(table, &result)
            .into_iter()
            .map(|key| key.to_wire())
            .collect::<Vec<_>>();
        serde_json::from_value(serde_json::json!({
            "global_sequence": seq,
            "request_sequence": 0,
            "correlation_id": corr,
            "timestamp_ns": start_ns,
            "end_timestamp_ns": end_ns,
            "boundary": "db",
            "trait_name": "diesel_models::query::generics",
            "method_name": "generic_find_one",
            "call_file": "crates/diesel_models/src/query/generics.rs",
            "call_line": 344,
            "call_column": 0,
            "request": {},
            "args": {"table": table},
            "response": {},
            "result": result,
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "read_set": read_set,
            "write_set": [],
            "replay_strategy": "execute",
            "task_bucket": bucket_id,
            "bucket_id": bucket_id,
            "fork_seq": fork_seq,
            "declaration": {
                "effect": "db",
                "op": "read",
                "returns": "rows",
                "state_canon": {"id": "sequence"}
            },
        }))
        .expect("valid BoundaryEvent")
    }

    fn with_event_lineage(
        ev: deja::BoundaryEvent,
        task_id: &str,
        parent_task_id: Option<&str>,
        bucket_id: &str,
        fork_seq: u64,
    ) -> deja::BoundaryEvent {
        let mut wire = serde_json::to_value(ev).expect("event to json");
        wire["task_id"] = serde_json::json!(task_id);
        if let Some(parent_task_id) = parent_task_id {
            wire["parent_task_id"] = serde_json::json!(parent_task_id);
        }
        wire["task_bucket"] = serde_json::json!(bucket_id);
        wire["bucket_id"] = serde_json::json!(bucket_id);
        wire["fork_seq"] = serde_json::json!(fork_seq);
        serde_json::from_value(wire).expect("event with lineage")
    }

    #[test]
    fn rule_a_demotes_declared_renamed_update_returning() {
        let charged = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let pending = serde_json::json!({"attempt_id": "pay_1", "status": "pending"});
        let card = detect(&art_with_events(
            vec![],
            vec![
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(202),
                    Some(envelope(charged.clone())),
                    envelope(pending),
                ),
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(204),
                    Some(envelope(charged.clone())),
                    envelope(charged.clone()),
                ),
            ],
            vec![http("c1", true, vec![])],
            vec![
                declared_db_update_ev("c1", "payment_attempt", 202, charged.clone(), 100, 300),
                declared_db_update_ev("c1", "payment_attempt", 204, charged.clone(), 150, 250),
            ],
        ));
        assert_eq!(card.summary.order_nondeterminism_warnings, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    #[test]
    fn canon_final_state_preserves_rule_a_demotion_and_lost_update_guard() {
        let charged = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let pending = serde_json::json!({"attempt_id": "pay_1", "status": "pending"});

        let demoted = detect(&art_with_events(
            vec![],
            vec![
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(202),
                    Some(envelope(charged.clone())),
                    envelope(pending.clone()),
                ),
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(204),
                    Some(envelope(charged.clone())),
                    envelope(charged.clone()),
                ),
            ],
            vec![http("c1", true, vec![])],
            vec![
                declared_db_update_ev_with_state_canon(
                    "c1",
                    "payment_attempt",
                    202,
                    charged.clone(),
                    100,
                    300,
                    "final_state",
                ),
                declared_db_update_ev_with_state_canon(
                    "c1",
                    "payment_attempt",
                    204,
                    charged.clone(),
                    150,
                    250,
                    "final_state",
                ),
            ],
        ));
        assert_eq!(demoted.summary.order_nondeterminism_warnings, 1);
        assert_eq!(demoted.summary.value_divergences, 0);
        assert_eq!(demoted.summary.side_effect_divergences, 0);
        assert!(demoted.verdict.pass, "{}", demoted.verdict.reason);

        let lost_update = detect(&art_with_events(
            vec![],
            vec![
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(202),
                    Some(envelope(charged.clone())),
                    envelope(pending.clone()),
                ),
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(204),
                    Some(envelope(charged.clone())),
                    envelope(pending),
                ),
            ],
            vec![http("c1", true, vec![])],
            vec![
                declared_db_update_ev_with_state_canon(
                    "c1",
                    "payment_attempt",
                    202,
                    charged.clone(),
                    100,
                    300,
                    "final_state",
                ),
                declared_db_update_ev_with_state_canon(
                    "c1",
                    "payment_attempt",
                    204,
                    charged,
                    150,
                    250,
                    "final_state",
                ),
            ],
        ));
        assert_eq!(lost_update.summary.order_nondeterminism_warnings, 0);
        assert!(
            lost_update.summary.value_divergences >= 1,
            "final_state canon must not mask a lost update"
        );
        assert!(!lost_update.verdict.pass);
    }

    // Mirrors cycle 25: seq 204 (final, sets Charged) matches; seq 202 (earlier,
    // net_amount only) runs concurrently on the SAME row and its RETURNING diverges
    // by interleaving (observed pending vs recorded charged). Demoted → pass.
    #[test]
    fn rule_a_legacy_fallback_demotes_concurrent_same_row_update_when_final_matches_and_http_clean()
    {
        let charged = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let pending = serde_json::json!({"attempt_id": "pay_1", "status": "pending"});
        let card = detect(&art_with_events(
            vec![],
            vec![
                // seq 202: earlier concurrent write, RETURNING diverges (pending).
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(202),
                    Some(envelope(charged.clone())),
                    envelope(pending),
                ),
                // seq 204: final write, matches recorded charged row.
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(204),
                    Some(envelope(charged.clone())),
                    envelope(charged.clone()),
                ),
            ],
            vec![http("c1", true, vec![])],
            vec![
                db_update_ev("c1", "payment_attempt", 202, charged.clone(), 100, 300),
                db_update_ev("c1", "payment_attempt", 204, charged.clone(), 150, 250),
            ],
        ));
        assert_eq!(
            card.summary.order_nondeterminism_warnings, 1,
            "seq 202 demoted"
        );
        assert_eq!(
            card.summary.value_divergences, 0,
            "no blocking value divergence"
        );
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    // Guard: the FINAL write also diverges (a real lost update) → NOT demoted.
    #[test]
    fn rule_a_keeps_blocking_when_final_write_diverges() {
        let charged = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let pending = serde_json::json!({"attempt_id": "pay_1", "status": "pending"});
        let card = detect(&art_with_events(
            vec![],
            vec![
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(202),
                    Some(envelope(charged.clone())),
                    envelope(pending.clone()),
                ),
                // final write diverges too → final state lost, must stay blocking.
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(204),
                    Some(envelope(charged.clone())),
                    envelope(pending),
                ),
            ],
            vec![http("c1", true, vec![])],
            vec![
                db_update_ev("c1", "payment_attempt", 202, charged.clone(), 100, 300),
                db_update_ev("c1", "payment_attempt", 204, charged.clone(), 150, 250),
            ],
        ));
        assert_eq!(card.summary.order_nondeterminism_warnings, 0);
        assert!(
            card.summary.value_divergences >= 1,
            "lost update stays blocking"
        );
        assert!(!card.verdict.pass);
    }

    // Guard: sequential (non-overlapping) writes are NOT concurrent → NOT demoted.
    #[test]
    fn rule_a_keeps_blocking_when_windows_do_not_overlap() {
        let charged = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let pending = serde_json::json!({"attempt_id": "pay_1", "status": "pending"});
        let card = detect(&art_with_events(
            vec![],
            vec![
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(202),
                    Some(envelope(charged.clone())),
                    envelope(pending),
                ),
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(204),
                    Some(envelope(charged.clone())),
                    envelope(charged.clone()),
                ),
            ],
            vec![http("c1", true, vec![])],
            vec![
                db_update_ev("c1", "payment_attempt", 202, charged.clone(), 100, 140), // ends before 204 starts
                db_update_ev("c1", "payment_attempt", 204, charged.clone(), 150, 250),
            ],
        ));
        assert_eq!(card.summary.order_nondeterminism_warnings, 0);
        assert!(
            card.summary.value_divergences >= 1,
            "sequential divergence stays blocking"
        );
        assert!(!card.verdict.pass);
    }

    // Guard: HTTP not 9/9 → no demotion at all (the response itself is wrong).
    #[test]
    fn rule_a_never_demotes_when_http_diverges() {
        let charged = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let pending = serde_json::json!({"attempt_id": "pay_1", "status": "pending"});
        let card = detect(&art_with_events(
            vec![],
            vec![
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(202),
                    Some(envelope(charged.clone())),
                    envelope(pending),
                ),
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(204),
                    Some(envelope(charged.clone())),
                    envelope(charged.clone()),
                ),
            ],
            vec![http("c1", false, vec![])], // HTTP status mismatch → not 9/9
            vec![
                db_update_ev("c1", "payment_attempt", 202, charged.clone(), 100, 300),
                db_update_ev("c1", "payment_attempt", 204, charged.clone(), 150, 250),
            ],
        ));
        assert_eq!(card.summary.order_nondeterminism_warnings, 0);
        assert!(!card.verdict.pass);
    }

    // ORDER-SWAP arm (cycle-34c fixture): the RECORDING captured the opposite
    // interleaving — the earlier write (seq 200) recorded the PRE-charge row, and
    // on replay observed the post-charge row that the matched final write (202)
    // recorded, differing only in `modified_at` by 1ms (each write's own clock).
    // Identical-recorded-row grouping cannot pair these; the observed==final
    // evidence (modulo volatile columns) must demote it.
    #[test]
    fn rule_a_demotes_order_swap_when_observed_equals_recorded_final() {
        let pre = serde_json::json!({"attempt_id": "pay_1", "status": "pending",
            "connector_transaction_id": null, "modified_at": "2026-07-02T18:43:47.101Z"});
        let final_rec = serde_json::json!({"attempt_id": "pay_1", "status": "charged",
            "connector_transaction_id": {"TxnId": "pi_x"}, "modified_at": "2026-07-02T18:43:47.959Z"});
        let observed_early = serde_json::json!({"attempt_id": "pay_1", "status": "charged",
            "connector_transaction_id": {"TxnId": "pi_x"}, "modified_at": "2026-07-02T18:43:47.958Z"});
        let card = detect(&art_with_events(
            vec![],
            vec![
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(200),
                    Some(envelope(pre.clone())),
                    envelope(observed_early),
                ),
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(202),
                    Some(envelope(final_rec.clone())),
                    envelope(final_rec.clone()),
                ),
            ],
            vec![http("c1", true, vec![])],
            vec![
                db_update_ev("c1", "payment_attempt", 200, pre, 100, 300),
                db_update_ev("c1", "payment_attempt", 202, final_rec, 150, 250),
            ],
        ));
        assert_eq!(
            card.summary.order_nondeterminism_warnings, 1,
            "order-swap demoted"
        );
        assert_eq!(card.summary.value_divergences, 0);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    // Guard: a REAL column difference (not just volatile clock stamps) between the
    // observed row and the recorded final row stays BLOCKING.
    #[test]
    fn rule_a_order_swap_keeps_blocking_on_real_column_difference() {
        let pre = serde_json::json!({"attempt_id": "pay_1", "status": "pending", "amount": 100});
        let final_rec =
            serde_json::json!({"attempt_id": "pay_1", "status": "charged", "amount": 100});
        let observed_early =
            serde_json::json!({"attempt_id": "pay_1", "status": "charged", "amount": 200});
        let card = detect(&art_with_events(
            vec![],
            vec![
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(200),
                    Some(envelope(pre.clone())),
                    envelope(observed_early),
                ),
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(202),
                    Some(envelope(final_rec.clone())),
                    envelope(final_rec.clone()),
                ),
            ],
            vec![http("c1", true, vec![])],
            vec![
                db_update_ev("c1", "payment_attempt", 200, pre, 100, 300),
                db_update_ev("c1", "payment_attempt", 202, final_rec, 150, 250),
            ],
        ));
        assert_eq!(card.summary.order_nondeterminism_warnings, 0);
        assert!(
            card.summary.value_divergences >= 1,
            "real amount drift stays blocking"
        );
        assert!(!card.verdict.pass);
    }

    // Guard: the order-swap evidence write must be LATER and FINAL. Here the
    // observed row equals an EARLIER matched write's recorded row, and the
    // diverged write IS the latest — no later final-state evidence exists, so it
    // stays BLOCKING (demoting would mask a real later divergence).
    #[test]
    fn rule_a_order_swap_requires_later_final_evidence_write() {
        let charged = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let drifted = serde_json::json!({"attempt_id": "pay_1", "status": "pending"});
        let card = detect(&art_with_events(
            vec![],
            vec![
                // seq 198: earlier write, matched (recorded == observed == charged).
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(198),
                    Some(envelope(charged.clone())),
                    envelope(charged.clone()),
                ),
                // seq 202: LATEST write diverges — recorded `drifted`, observed equals
                // the EARLIER row. No later evidence write exists.
                exec_obs(
                    "db",
                    Some("c1"),
                    true,
                    Some(202),
                    Some(envelope(drifted.clone())),
                    envelope(charged.clone()),
                ),
            ],
            vec![http("c1", true, vec![])],
            vec![
                db_update_ev("c1", "payment_attempt", 198, charged, 100, 300),
                db_update_ev("c1", "payment_attempt", 202, drifted, 150, 250),
            ],
        ));
        assert_eq!(card.summary.order_nondeterminism_warnings, 0);
        assert!(
            card.summary.value_divergences >= 1,
            "latest-write divergence stays blocking"
        );
        assert!(!card.verdict.pass);
    }

    // ---- Rule B: idempotent redis delete demotion (cycle-25 delete_key case) ----

    fn redis_op_obs(
        method: &str,
        corr: &str,
        src: u64,
        rec: serde_json::Value,
        observed: serde_json::Value,
    ) -> ObservedCall {
        let mut o = exec_obs("redis", Some(corr), true, Some(src), Some(rec), observed);
        o.method_name = method.to_owned();
        o
    }

    fn redis_delete_ev(
        corr: &str,
        seq: u64,
        method: &str,
        op: deja::OperationKind,
    ) -> deja::BoundaryEvent {
        serde_json::from_value(serde_json::json!({
            "global_sequence": seq,
            "request_sequence": 0,
            "correlation_id": corr,
            "timestamp_ns": 100,
            "end_timestamp_ns": 101,
            "boundary": "redis",
            "trait_name": "RedisConnInterface",
            "method_name": method,
            "call_file": "redis.rs",
            "call_line": 1,
            "call_column": 0,
            "request": {},
            "args": {"key": "k"},
            "response": {},
            "result": "KeyDeleted",
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "read_set": [],
            "write_set": [],
            "replay_strategy": "execute",
            "declaration": {
                "effect": "redis",
                "op": op,
            },
        }))
        .expect("valid BoundaryEvent")
    }

    fn redis_delete_ev_with_reply_canon(
        corr: &str,
        seq: u64,
        method: &str,
        op: deja::OperationKind,
        canon: &str,
    ) -> deja::BoundaryEvent {
        let mut ev = redis_delete_ev(corr, seq, method, op);
        let declaration = ev
            .declaration
            .take()
            .expect("redis_delete_ev stamps a declaration")
            .reply_canon(deja::CanonRef::new(canon));
        ev.declaration = Some(declaration);
        ev
    }

    #[test]
    fn rule_b_demotes_declared_renamed_idempotent_delete() {
        let card = detect(&art_with_events(
            vec![],
            vec![redis_op_obs(
                "remove_cache_entry",
                "c1",
                101,
                serde_json::json!("KeyDeleted"),
                serde_json::json!("KeyNotDeleted"),
            )],
            vec![http("c1", true, vec![])],
            vec![redis_delete_ev(
                "c1",
                101,
                "remove_cache_entry",
                deja::OperationKind::IdempotentDelete,
            )],
        ));
        assert_eq!(card.summary.idempotent_delete_warnings, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    #[test]
    fn canon_absent_after_and_project_preserve_delete_guards() {
        let absent_after = detect(&art_with_events(
            vec![],
            vec![redis_op_obs(
                "remove_cache_entry",
                "c1",
                101,
                serde_json::json!("KeyDeleted"),
                serde_json::json!("KeyNotDeleted"),
            )],
            vec![http("c1", true, vec![])],
            vec![redis_delete_ev_with_reply_canon(
                "c1",
                101,
                "remove_cache_entry",
                deja::OperationKind::IdempotentDelete,
                "absent_after",
            )],
        ));
        assert_eq!(absent_after.summary.idempotent_delete_warnings, 1);
        assert_eq!(absent_after.summary.value_divergences, 0);
        assert_eq!(absent_after.summary.side_effect_divergences, 0);
        assert!(absent_after.verdict.pass, "{}", absent_after.verdict.reason);

        let unexpected_deletion = detect(&art_with_events(
            vec![],
            vec![redis_op_obs(
                "remove_cache_entry",
                "c1",
                101,
                serde_json::json!("KeyNotDeleted"),
                serde_json::json!("KeyDeleted"),
            )],
            vec![http("c1", true, vec![])],
            vec![redis_delete_ev_with_reply_canon(
                "c1",
                101,
                "remove_cache_entry",
                deja::OperationKind::IdempotentDelete,
                "project:key_exists",
            )],
        ));
        assert_eq!(unexpected_deletion.summary.idempotent_delete_warnings, 0);
        assert!(
            unexpected_deletion.summary.value_divergences >= 1,
            "project canon must not hide an unexpected deletion"
        );
        assert!(!unexpected_deletion.verdict.pass);
    }

    #[test]
    fn rule_b_declared_non_idempotent_delete_stays_blocking() {
        let card = detect(&art_with_events(
            vec![],
            vec![redis_op_obs(
                "delete_key",
                "c1",
                101,
                serde_json::json!("KeyDeleted"),
                serde_json::json!("KeyNotDeleted"),
            )],
            vec![http("c1", true, vec![])],
            vec![redis_delete_ev(
                "c1",
                101,
                "delete_key",
                deja::OperationKind::Delete,
            )],
        ));
        assert_eq!(card.summary.idempotent_delete_warnings, 0);
        assert!(
            card.summary.value_divergences >= 1,
            "complete non-idempotent declaration must not fall back to delete_key"
        );
        assert!(!card.verdict.pass);
    }

    // Positive: delete_key recorded KeyDeleted, observed KeyNotDeleted, HTTP clean.
    #[test]
    fn rule_b_demotes_idempotent_delete_key_when_http_clean() {
        let card = detect(&art(
            vec![],
            vec![redis_op_obs(
                "delete_key",
                "c1",
                101,
                serde_json::json!("KeyDeleted"),
                serde_json::json!("KeyNotDeleted"),
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.idempotent_delete_warnings, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    // Reverse (KeyNotDeleted -> KeyDeleted) is an UNEXPECTED deletion → blocking.
    #[test]
    fn rule_b_keeps_blocking_on_reverse_unexpected_deletion() {
        let card = detect(&art(
            vec![],
            vec![redis_op_obs(
                "delete_key",
                "c1",
                101,
                serde_json::json!("KeyNotDeleted"),
                serde_json::json!("KeyDeleted"),
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.idempotent_delete_warnings, 0);
        assert!(
            card.summary.value_divergences >= 1,
            "unexpected deletion stays blocking"
        );
        assert!(!card.verdict.pass);
    }

    // A non-delete redis op with the same reply values is NOT demoted.
    #[test]
    fn rule_b_keeps_blocking_for_non_delete_redis_op() {
        let card = detect(&art(
            vec![],
            vec![redis_op_obs(
                "set_key",
                "c1",
                101,
                serde_json::json!("KeyDeleted"),
                serde_json::json!("KeyNotDeleted"),
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.idempotent_delete_warnings, 0);
        assert!(card.summary.value_divergences >= 1);
        assert!(!card.verdict.pass);
    }

    // Another delete-ISH op (delete_multiple_keys) is NOT demoted — only exact delete_key.
    #[test]
    fn rule_b_keeps_blocking_for_other_deleteish_op() {
        let card = detect(&art(
            vec![],
            vec![redis_op_obs(
                "delete_multiple_keys",
                "c1",
                101,
                serde_json::json!("KeyDeleted"),
                serde_json::json!("KeyNotDeleted"),
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.idempotent_delete_warnings, 0);
        assert!(
            card.summary.value_divergences >= 1,
            "only exact delete_key demotes"
        );
        assert!(!card.verdict.pass);
    }

    // HTTP not 9/9 → never demoted.
    #[test]
    fn rule_b_never_demotes_when_http_diverges() {
        let card = detect(&art(
            vec![],
            vec![redis_op_obs(
                "delete_key",
                "c1",
                101,
                serde_json::json!("KeyDeleted"),
                serde_json::json!("KeyNotDeleted"),
            )],
            vec![http("c1", false, vec![])],
        ));
        assert_eq!(card.summary.idempotent_delete_warnings, 0);
        assert!(!card.verdict.pass);
    }

    // A re-keyed / unresolved delete (mismatched key/correlation → not args-aligned)
    // is NOT demoted (the resolved guard).
    #[test]
    fn rule_b_does_not_demote_unresolved_rekeyed_delete() {
        let mut o = redis_op_obs(
            "delete_key",
            "c1",
            101,
            serde_json::json!("KeyDeleted"),
            serde_json::json!("KeyNotDeleted"),
        );
        o.resolved = false;
        let card = detect(&art(vec![], vec![o], vec![http("c1", true, vec![])]));
        assert_eq!(card.summary.idempotent_delete_warnings, 0);
    }

    #[test]
    fn omitted_call_fails() {
        let card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 7)],
            vec![],
            vec![http("c1", true, vec![])],
        ));
        assert!(!card.verdict.pass);
        assert_eq!(card.summary.omitted_calls, 1);
        assert_eq!(card.summary.matched_correlations, 0);
        assert_eq!(
            card.per_boundary["redis"].kinds.get("OmittedCall"),
            Some(&1)
        );
    }

    #[test]
    fn novel_call_fails() {
        let card = detect(&art(
            vec![],
            vec![obs("redis", Some("c1"), false, None, None)],
            vec![],
        ));
        assert!(!card.verdict.pass);
        assert_eq!(card.summary.novel_calls, 1);
    }

    #[test]
    fn novel_egress_call_is_tolerated() {
        let card = detect(&art(
            vec![],
            vec![obs("http_outgoing", Some("c1"), false, None, None)],
            vec![http("c1", true, vec![])],
        ));
        assert!(card.verdict.pass, "{}", card.verdict.reason);
        assert_eq!(card.summary.environmental_misses, 1);
        assert_eq!(card.summary.novel_calls, 0);
        assert_eq!(
            card.per_boundary["http_outgoing"].tier.as_deref(),
            Some("environmental")
        );
    }

    #[test]
    fn http_body_mismatch_fails() {
        let card = detect(&art(
            vec![],
            vec![],
            vec![http(
                "c1",
                true,
                vec![JsonFieldDiff {
                    json_path: "$.amount".to_owned(),
                    baseline: serde_json::json!(100),
                    candidate: serde_json::json!(200),
                }],
            )],
        ));
        assert!(!card.verdict.pass);
        assert_eq!(card.summary.http_body_mismatches, 1);
    }

    #[test]
    fn positional_rank6_resolution_flagged_recovered_but_passes() {
        // A match at the weakest positional rank (Sequence == rank 6 after the P3
        // renumber) is a fragility signal, tracked as "Recovered", not a divergence.
        let card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 7)],
            vec![obs("redis", Some("c1"), true, Some(6), Some(7))],
            vec![http("c1", true, vec![])],
        ));
        assert!(card.verdict.pass, "{}", card.verdict.reason);
        // Field name kept for dashboard stability; now counts rank-6 positional hits.
        assert_eq!(card.summary.recovered_rank5_calls, 1);
        assert_eq!(card.summary.resolved_by_rank.get("rank_6"), Some(&1));
    }

    #[test]
    fn empty_run_is_inconclusive_not_pass() {
        let card = detect(&art(vec![], vec![], vec![]));
        assert!(!card.verdict.pass);
        assert!(card.verdict.inconclusive);
    }

    #[test]
    fn uncorrelated_omitted_is_tolerated() {
        // A background-task (null-correlation) recorded event the candidate
        // didn't reproduce is counted but does not block.
        let card = detect(&art(vec![seq_entry(None, "redis", 7)], vec![], vec![]));
        assert_eq!(card.summary.uncorrelated_events_seen, 1);
        assert_eq!(
            card.summary.omitted_calls, 0,
            "uncorrelated omission not blocking"
        );
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    // --- M1: ValueDiverged + args-free pairing -------------------------------

    #[test]
    fn transitive_dependency_execute_chain_divergence_is_blocking() {
        // Item-16 shape: A writes state, B reads the A-derived state under
        // Execute, B writes B′ from that read, and C reads B′. The A write returns
        // an ack in both runs; the candidate mutation is observed through B's
        // execute read changing from the recorded A value to the candidate A
        // value. B′ and C are re-keyed by that changed value, so their recorded
        // twins must pair args-free as downstream consequences instead of
        // splitting into Novel+Omitted noise.
        let corr = "dependency-chain";
        let a_write_ack = serde_json::json!({"ok": true});
        let b_recorded_read = serde_json::json!({"a_value": "recorded"});
        let b_candidate_read = serde_json::json!({"a_value": "candidate"});
        let b_prime_recorded = serde_json::json!({"b_prime": "derived-from-recorded"});
        let b_prime_candidate = serde_json::json!({"b_prime": "derived-from-candidate"});
        let c_recorded_read = serde_json::json!({"c_seen": "derived-from-recorded"});
        let c_candidate_read = serde_json::json!({"c_seen": "derived-from-candidate"});

        let card = detect(&art(
            vec![
                seq_entry_method_res(Some(corr), "storage", "write_a", 10, a_write_ack.clone()),
                seq_entry_method_res(Some(corr), "redis", "read_a", 11, b_recorded_read.clone()),
                seq_entry_method_res(
                    Some(corr),
                    "storage",
                    "write_b_prime",
                    12,
                    b_prime_recorded.clone(),
                ),
                seq_entry_method_res(Some(corr), "db", "read_b_prime", 13, c_recorded_read),
            ],
            vec![
                exec_obs_method(
                    "storage",
                    Some(corr),
                    "write_a",
                    true,
                    Some(10),
                    Some(a_write_ack),
                    serde_json::json!({"ok": true}),
                ),
                exec_obs_method(
                    "redis",
                    Some(corr),
                    "read_a",
                    true,
                    Some(11),
                    Some(b_recorded_read),
                    b_candidate_read,
                ),
                exec_obs_method(
                    "storage",
                    Some(corr),
                    "write_b_prime",
                    false,
                    None,
                    None,
                    b_prime_candidate,
                ),
                exec_obs_method(
                    "db",
                    Some(corr),
                    "read_b_prime",
                    false,
                    None,
                    None,
                    c_candidate_read,
                ),
            ],
            vec![http(corr, true, vec![])],
        ));

        assert_eq!(card.summary.http_status_mismatches, 0);
        assert_eq!(card.summary.http_body_mismatches, 0);
        assert_eq!(card.summary.order_nondeterminism_warnings, 0);
        assert_eq!(card.summary.idempotent_delete_warnings, 0);
        assert_eq!(card.summary.value_divergences, 3);
        assert_eq!(card.summary.side_effect_divergences, 3);
        assert_eq!(card.summary.novel_calls, 0, "consequences pair args-free");
        assert_eq!(
            card.summary.omitted_calls, 0,
            "paired consequences consume recorded twins"
        );
        assert_eq!(
            kind_count(&card, "redis", "ValueDivergedOrigin"),
            1,
            "B's execute read of A-derived state is the cascade origin"
        );
        assert_eq!(
            kind_count(&card, "storage", "ValueDiverged"),
            1,
            "B′'s derived write is paired as a downstream consequence"
        );
        assert_eq!(
            kind_count(&card, "db", "ValueDiverged"),
            1,
            "C's re-keyed read of B′ is paired as a downstream consequence"
        );
        assert_eq!(kind_count(&card, "storage", "NovelCall"), 0);
        assert_eq!(kind_count(&card, "storage", "OmittedCall"), 0);
        assert_eq!(kind_count(&card, "db", "NovelCall"), 0);
        assert_eq!(kind_count(&card, "db", "OmittedCall"), 0);

        let chain = card
            .per_correlation
            .iter()
            .find(|c| c.correlation_id == corr)
            .unwrap();
        assert!(chain.http_status_match);
        assert!(chain.http_body_match);
        assert_eq!(chain.side_effect_divergences, 3);
        assert!(!chain.passed);
        assert!(
            !card.verdict.pass,
            "HTTP is clean, but state drift must stay blocking"
        );
        assert!(
            card.verdict.reason.contains("value divergence"),
            "{}",
            card.verdict.reason
        );
    }

    #[test]
    fn recognized_read_write_lineage_race_is_inconclusive_with_auto_rerun() {
        let corr = "race-corr";
        let recorded_row = serde_json::json!({"attempt_id": "pay_1", "status": "pending"});
        let raced_row = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let downstream_recorded = serde_json::json!({"branch": "pending"});
        let downstream_observed =
            serde_json::json!({"branch": "charged", "source": raced_row.clone()});

        let read_event = with_event_lineage(
            db_read_ev(
                corr,
                "payment_attempt",
                300,
                recorded_row.clone(),
                100,
                300,
                "root",
                0,
            ),
            "root",
            None,
            "root",
            0,
        );
        let conflicting_write = with_event_lineage(
            declared_db_update_ev(corr, "payment_attempt", 301, raced_row.clone(), 150, 250),
            "detached-writer",
            Some("root"),
            "detached-writer-bucket",
            1,
        );
        let read_observation = exec_obs(
            "db",
            Some(corr),
            true,
            Some(300),
            Some(envelope(recorded_row)),
            envelope(raced_row.clone()),
        );
        let mut downstream_observation = exec_obs_method(
            "storage",
            Some(corr),
            "write_branch",
            false,
            None,
            None,
            downstream_observed,
        );
        downstream_observation.args = serde_json::json!({"source": envelope(raced_row.clone())});

        let card = detect(&art_with_events(
            vec![seq_entry_method_res(
                Some(corr),
                "storage",
                "write_branch",
                302,
                downstream_recorded,
            )],
            vec![read_observation, downstream_observation],
            vec![http(corr, true, vec![])],
            vec![read_event, conflicting_write],
        ));
        let wire = serde_json::to_value(&card).unwrap();

        assert_eq!(card.summary.inconclusive_races, 2);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert!(card.verdict.inconclusive, "{}", card.verdict.reason);
        assert!(!card.verdict.pass);
        assert!(
            card.verdict.reason.contains("inconclusive_race")
                && card.verdict.reason.contains("auto-rerun"),
            "{}",
            card.verdict.reason
        );
        assert_eq!(
            wire["summary"]["inconclusive_races"],
            serde_json::json!(2),
            "scorecard JSON must expose the inconclusive_race counter"
        );
        assert!(
            card.warnings
                .iter()
                .any(|warning| warning.contains("inconclusive_race")
                    && warning.contains("auto-rerun")),
            "warnings should carry an auto-rerun diagnostic: {:?}",
            card.warnings
        );
    }

    #[test]
    fn race_attributed_http_body_diff_is_inconclusive_not_blocking() {
        let corr = "race-body-corr";
        let recorded_row = serde_json::json!({
            "attempt_id": "pay_1",
            "created_at": "2026-07-06T10:03:01.481Z"
        });
        let raced_row = serde_json::json!({
            "attempt_id": "pay_1",
            "created_at": "2026-07-06T10:03:01.480Z"
        });
        let recorded_result = envelope(recorded_row.clone());
        let raced_result = envelope(raced_row.clone());
        let read_event = with_event_lineage(
            db_read_ev(
                corr,
                "payment_attempt",
                300,
                recorded_row.clone(),
                100,
                300,
                "root",
                0,
            ),
            "root",
            None,
            "root",
            0,
        );
        let conflicting_write = with_event_lineage(
            declared_db_update_ev(corr, "payment_attempt", 301, raced_row.clone(), 150, 250),
            "root",
            None,
            "root",
            0,
        );
        let read_observation = exec_obs(
            "db",
            Some(corr),
            true,
            Some(300),
            Some(recorded_result.clone()),
            raced_result.clone(),
        );
        let write_observation = exec_obs(
            "db",
            Some(corr),
            true,
            Some(301),
            Some(raced_result.clone()),
            raced_result.clone(),
        );
        let redis_delete = redis_op_obs(
            "delete_key",
            corr,
            101,
            serde_json::json!("KeyDeleted"),
            serde_json::json!("KeyNotDeleted"),
        );

        let card = detect(&art_with_events(
            vec![
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_find_one_core",
                    300,
                    recorded_result.clone(),
                ),
                span_entry_res(Some(corr), 300, "request>read_branch>read", recorded_result),
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_update_with_results",
                    301,
                    raced_result.clone(),
                ),
                span_entry_res(Some(corr), 301, "request>write_branch>write", raced_result),
            ],
            vec![read_observation, write_observation, redis_delete],
            vec![http(
                corr,
                true,
                vec![JsonFieldDiff {
                    json_path: "$.created".to_owned(),
                    baseline: serde_json::json!("2026-07-06T10:03:01.481Z"),
                    candidate: serde_json::json!("2026-07-06T10:03:01.480Z"),
                }],
            )],
            vec![read_event, conflicting_write],
        ));

        assert_eq!(card.summary.http_body_mismatches, 0);
        assert_eq!(card.summary.inconclusive_races, 1);
        assert_eq!(card.summary.idempotent_delete_warnings, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert!(card.verdict.inconclusive, "{}", card.verdict.reason);
        assert!(!card.verdict.pass);
    }

    #[test]
    fn build_ledger_mirrors_race_attributed_http_body_classification() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let run_id = "run-ledger-race-body";
        let recording_id = "rec-ledger-race-body";
        let corr = "race-body-ledger-corr";
        let recorded_row = serde_json::json!({
            "attempt_id": "pay_1",
            "created_at": "2026-07-06T10:03:01.481Z"
        });
        let raced_row = serde_json::json!({
            "attempt_id": "pay_1",
            "created_at": "2026-07-06T10:03:01.480Z"
        });
        let recorded_result = envelope(recorded_row.clone());
        let raced_result = envelope(raced_row.clone());
        let read_event = with_event_lineage(
            db_read_ev(
                corr,
                "payment_attempt",
                300,
                recorded_row.clone(),
                100,
                300,
                "root",
                0,
            ),
            "root",
            None,
            "root",
            0,
        );
        let conflicting_write = with_event_lineage(
            declared_db_update_ev(corr, "payment_attempt", 301, raced_row.clone(), 150, 250),
            "root",
            None,
            "root",
            0,
        );
        write_jsonl_rows(
            &root.recording_events_path(recording_id),
            &[read_event, conflicting_write],
        );

        let table = LookupTable {
            recording_id: recording_id.to_owned(),
            policy_version: 1,
            entries: vec![
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_find_one_core",
                    300,
                    recorded_result.clone(),
                ),
                span_entry_res(
                    Some(corr),
                    300,
                    "request>read_branch>read",
                    recorded_result.clone(),
                ),
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_update_with_results",
                    301,
                    raced_result.clone(),
                ),
                span_entry_res(
                    Some(corr),
                    301,
                    "request>write_branch>write",
                    raced_result.clone(),
                ),
            ],
        };
        let observed = vec![
            exec_obs(
                "db",
                Some(corr),
                true,
                Some(300),
                Some(recorded_result),
                raced_result.clone(),
            ),
            exec_obs(
                "db",
                Some(corr),
                true,
                Some(301),
                Some(raced_result.clone()),
                raced_result.clone(),
            ),
            redis_op_obs(
                "delete_key",
                corr,
                101,
                serde_json::json!("KeyDeleted"),
                serde_json::json!("KeyNotDeleted"),
            ),
        ];
        let http_diffs = vec![http(
            corr,
            true,
            vec![JsonFieldDiff {
                json_path: "$.created".to_owned(),
                baseline: serde_json::json!("2026-07-06T10:03:01.481Z"),
                candidate: serde_json::json!("2026-07-06T10:03:01.480Z"),
            }],
        )];
        let art = RunArtifacts {
            run_id: run_id.to_owned(),
            recording_id: Some(recording_id.to_owned()),
            table,
            observed,
            http_diffs,
            events: Vec::new(),
            warnings: Vec::new(),
        };

        let rows = build_ledger(&root, &art).unwrap();
        let race_row = rows
            .iter()
            .find(|row| row.source_event_global_sequence == Some(300))
            .unwrap();
        assert_eq!(race_row.kind, "inconclusive_race");
        assert!(race_row.origin);
        assert!(!race_row.blocking);

        let delete_row = rows
            .iter()
            .find(|row| row.boundary == "redis" && row.method_name == "delete_key")
            .unwrap();
        assert_eq!(delete_row.kind, "idempotent_delete");
        assert!(!delete_row.blocking);
    }

    #[test]
    fn unattributed_http_body_diff_keeps_race_run_blocking() {
        let corr = "race-body-blocking-corr";
        let recorded_row = serde_json::json!({"attempt_id": "pay_1", "status": "pending"});
        let raced_row = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let recorded_result = envelope(recorded_row.clone());
        let raced_result = envelope(raced_row.clone());
        let read_event = with_event_lineage(
            db_read_ev(
                corr,
                "payment_attempt",
                300,
                recorded_row.clone(),
                100,
                300,
                "root",
                0,
            ),
            "root",
            None,
            "root",
            0,
        );
        let conflicting_write = with_event_lineage(
            declared_db_update_ev(corr, "payment_attempt", 301, raced_row.clone(), 150, 250),
            "root",
            None,
            "root",
            0,
        );
        let read_observation = exec_obs(
            "db",
            Some(corr),
            true,
            Some(300),
            Some(recorded_result.clone()),
            raced_result.clone(),
        );
        let write_observation = exec_obs(
            "db",
            Some(corr),
            true,
            Some(301),
            Some(raced_result.clone()),
            raced_result.clone(),
        );

        let card = detect(&art_with_events(
            vec![
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_find_one_core",
                    300,
                    recorded_result.clone(),
                ),
                span_entry_res(Some(corr), 300, "request>read_branch>read", recorded_result),
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_update_with_results",
                    301,
                    raced_result.clone(),
                ),
                span_entry_res(Some(corr), 301, "request>write_branch>write", raced_result),
            ],
            vec![read_observation, write_observation],
            vec![http(
                corr,
                true,
                vec![JsonFieldDiff {
                    json_path: "$.amount".to_owned(),
                    baseline: serde_json::json!("unrelated-old"),
                    candidate: serde_json::json!("unrelated-new"),
                }],
            )],
            vec![read_event, conflicting_write],
        ));

        assert_eq!(card.summary.inconclusive_races, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.http_body_mismatches, 1);
        assert!(!card.verdict.inconclusive);
        assert!(!card.verdict.pass);
        assert!(
            card.verdict.reason.contains("http body mismatch"),
            "{}",
            card.verdict.reason
        );
    }

    #[test]
    fn non_race_value_divergence_remains_blocking() {
        let corr = "not-a-race";
        let recorded_row = serde_json::json!({"attempt_id": "pay_1", "status": "pending"});
        let observed_row = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let read_event = db_read_ev(
            corr,
            "payment_attempt",
            310,
            recorded_row.clone(),
            100,
            300,
            "root",
            0,
        );
        let read_observation = exec_obs(
            "db",
            Some(corr),
            true,
            Some(310),
            Some(envelope(recorded_row)),
            envelope(observed_row),
        );

        let card = detect(&art_with_events(
            vec![],
            vec![read_observation],
            vec![http(corr, true, vec![])],
            vec![read_event],
        ));

        assert_eq!(card.summary.inconclusive_races, 0);
        assert_eq!(card.summary.value_divergences, 1);
        assert_eq!(card.summary.side_effect_divergences, 1);
        assert!(!card.verdict.inconclusive);
        assert!(!card.verdict.pass);
        assert!(
            card.verdict.reason.contains("value divergence"),
            "{}",
            card.verdict.reason
        );
    }

    #[test]
    fn transitive_dependency_substitute_chain_stays_quiet() {
        // Negative control for the same A→B→C graph: in Substitute/Recorded mode
        // B is served the recorded A-derived value, so B′ and C stay on the
        // recorded branch. The cascade is intentionally invisible and the
        // scorecard remains clean.
        let corr = "dependency-chain";
        let a_write_ack = serde_json::json!({"ok": true});
        let b_recorded_read = serde_json::json!({"a_value": "recorded"});
        let b_prime_recorded = serde_json::json!({"b_prime": "derived-from-recorded"});
        let c_recorded_read = serde_json::json!({"c_seen": "derived-from-recorded"});

        let card = detect(&art(
            vec![
                seq_entry_method_res(Some(corr), "storage", "write_a", 10, a_write_ack.clone()),
                seq_entry_method_res(Some(corr), "redis", "read_a", 11, b_recorded_read.clone()),
                seq_entry_method_res(
                    Some(corr),
                    "storage",
                    "write_b_prime",
                    12,
                    b_prime_recorded.clone(),
                ),
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "read_b_prime",
                    13,
                    c_recorded_read.clone(),
                ),
            ],
            vec![
                substituted_obs_method("storage", Some(corr), "write_a", 10, a_write_ack),
                substituted_obs_method("redis", Some(corr), "read_a", 11, b_recorded_read),
                substituted_obs_method(
                    "storage",
                    Some(corr),
                    "write_b_prime",
                    12,
                    b_prime_recorded,
                ),
                substituted_obs_method("db", Some(corr), "read_b_prime", 13, c_recorded_read),
            ],
            vec![http(corr, true, vec![])],
        ));

        assert_eq!(card.summary.http_status_mismatches, 0);
        assert_eq!(card.summary.http_body_mismatches, 0);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert_eq!(card.summary.novel_calls, 0);
        assert_eq!(card.summary.omitted_calls, 0);
        assert_eq!(card.summary.order_nondeterminism_warnings, 0);
        assert_eq!(card.summary.idempotent_delete_warnings, 0);
        assert_eq!(kind_count(&card, "redis", "ValueDivergedOrigin"), 0);
        assert_eq!(kind_count(&card, "storage", "ValueDiverged"), 0);
        assert_eq!(kind_count(&card, "db", "ValueDiverged"), 0);

        let chain = card
            .per_correlation
            .iter()
            .find(|c| c.correlation_id == corr)
            .unwrap();
        assert!(chain.http_status_match);
        assert!(chain.http_body_match);
        assert_eq!(chain.side_effect_divergences, 0);
        assert!(chain.passed);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    #[test]
    fn rekeyed_write_pairs_args_free_into_one_value_divergence() {
        // GOTCHA #1: the diverged WRITE carries a mutated operand, so its args
        // miss the recorded baseline → recorded twin would be Omitted, the execute
        // call would be Novel. The args-free pairing must collapse them into ONE
        // ValueDiverged (NOT Novel+Omitted), and flip the correlation to diverged.
        let card = detect(&art(
            vec![seq_entry_res(
                Some("c1"),
                "storage",
                7,
                serde_json::json!(100),
            )],
            vec![exec_obs(
                "storage",
                Some("c1"),
                false,                  // re-keyed args missed the baseline → unresolved
                None,                   // no source_event_global_sequence (it didn't resolve)
                None, // hook found no args-aligned baseline (seed_gap on hook side)
                serde_json::json!(200), // the doubled amount
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 1, "one value divergence");
        assert_eq!(card.summary.novel_calls, 0, "not a Novel");
        assert_eq!(card.summary.omitted_calls, 0, "not an Omitted");
        assert_eq!(
            card.per_boundary["storage"].kinds.get("ValueDiverged"),
            Some(&1)
        );
        assert!(!card.verdict.pass, "value divergence flips the verdict");
        assert!(
            card.verdict.reason.contains("value divergence"),
            "{}",
            card.verdict.reason
        );
        // The correlation outcome must show the divergence.
        let c1 = card
            .per_correlation
            .iter()
            .find(|c| c.correlation_id == "c1")
            .unwrap();
        assert!(!c1.passed);
        assert_eq!(c1.side_effect_divergences, 1);
    }

    #[test]
    fn args_aligned_execute_value_diff_is_value_diverged() {
        // Execute mode where args STILL align (a READ, or a write whose operand
        // did not change): the baseline resolves (resolved=true) but the REAL
        // boundary's observed_result differs → ValueDiverged via the resolved arm.
        let card = detect(&art(
            vec![seq_entry_res(
                Some("c1"),
                "storage",
                7,
                serde_json::json!("old"),
            )],
            vec![exec_obs(
                "storage",
                Some("c1"),
                true,    // args aligned → baseline resolved
                Some(7), // consumed the recorded twin
                Some(serde_json::json!("old")),
                serde_json::json!("new"), // real boundary diverged in value
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 1);
        assert_eq!(card.summary.matched_side_effect_calls, 0);
        assert_eq!(card.summary.omitted_calls, 0, "twin consumed, not omitted");
        assert!(!card.verdict.pass);
    }

    #[test]
    fn execute_value_match_is_matched_not_diverged() {
        // Execute mode, real boundary reproduced the recorded value exactly:
        // inert — a plain match, not a divergence.
        let card = detect(&art(
            vec![seq_entry_res(
                Some("c1"),
                "storage",
                7,
                serde_json::json!("same"),
            )],
            vec![exec_obs(
                "storage",
                Some("c1"),
                true,
                Some(7),
                Some(serde_json::json!("same")),
                serde_json::json!("same"),
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.matched_side_effect_calls, 1);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    #[test]
    fn execute_seed_gap_is_inconclusive_not_blocking() {
        // Execute-mode State call ran the real boundary but found NO recorded
        // baseline AND no args-free twin to pair with → InconclusiveSeedGap, which
        // is reported but does NOT fail the verdict.
        let card = detect(&art(
            vec![], // nothing recorded → no twin to pair
            vec![exec_obs(
                "storage",
                Some("c1"),
                false,
                None,
                None, // seed gap
                serde_json::json!("fresh"),
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.inconclusive_seed_gaps, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.novel_calls, 0, "seed gap is not a Novel");
        assert!(
            card.verdict.pass,
            "seed gap is non-blocking: {}",
            card.verdict.reason
        );
        assert!(card.verdict.reason.contains("seed gap"));
    }

    /// REGRESSION (#28 extra-call): an execute-shadow call with NO recorded
    /// baseline AND NO seed_gap flag (the FIXED `execute_shadow_peek` behavior:
    /// a novel call no longer self-flags seed_gap) and NO recorded twin to pair
    /// with must be a BLOCKING NovelCall — the extra-call catch. Before the fix the
    /// peek set seed_gap=true for this case, so the tally swallowed it as a
    /// non-blocking InconclusiveSeedGap (verdict PASS, catch masked).
    #[test]
    fn novel_execute_call_without_seed_gap_is_a_blocking_novel() {
        // Build the observation exactly as the FIXED execute-shadow path emits it:
        // Shadow provenance, no baseline, resolved=false, seed_gap=false.
        let mut o = exec_obs(
            "storage",
            Some("c1"),
            false, // unresolved (no baseline)
            None,
            None,                       // no recorded baseline
            serde_json::json!("fresh"), // real boundary result
        );
        o.seed_gap = false; // the fix: a novel call is NOT a seed gap
        let card = detect(&art(
            vec![], // nothing recorded → no twin to pair
            vec![o],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.inconclusive_seed_gaps, 0, "not a seed gap");
        assert_eq!(card.summary.novel_calls, 1, "novel call is a NovelCall");
        assert!(
            !card.verdict.pass,
            "a novel Execute call with no recording must FAIL the verdict (blocking): {}",
            card.verdict.reason
        );
    }

    #[test]
    fn lookup_mode_observed_equals_recorded_keeps_value_diverged_inert() {
        // NO-REGRESSION: a substituted hit has observed_result == recorded_result,
        // so the ValueDiverged classifier stays inert.
        let card = detect(&art(
            vec![seq_entry_res(
                Some("c1"),
                "redis",
                7,
                serde_json::json!("v"),
            )],
            vec![exec_obs(
                "redis",
                Some("c1"),
                true,
                Some(7),
                Some(serde_json::json!("v")),
                serde_json::json!("v"), // lookup: observed == recorded
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.matched_side_effect_calls, 1);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    #[test]
    fn rekeyed_write_with_same_value_is_recovered_match_not_split() {
        // A re-keyed call (args missed) whose VALUE nonetheless reproduced is
        // paired args-free and counted as a match — never a Novel+Omitted split.
        let card = detect(&art(
            vec![seq_entry_res(
                Some("c1"),
                "storage",
                7,
                serde_json::json!("v"),
            )],
            vec![exec_obs(
                "storage",
                Some("c1"),
                false,
                None,
                None,
                serde_json::json!("v"),
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.novel_calls, 0);
        assert_eq!(card.summary.omitted_calls, 0);
        assert_eq!(card.summary.matched_side_effect_calls, 1);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }
}
