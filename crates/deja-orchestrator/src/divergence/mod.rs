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
//!     …uncorrelated (background work)      → NovelCallTolerated
//!     …on an egress boundary               → EnvironmentalMiss (tolerated)
//!   - table entry the candidate never hit  → OmittedCall (blocking)
//!     …uncorrelated, or non-blocking       → OmittedCallTolerated
//!   - db value diff confined to columns
//!     both statements fill with `DEFAULT`  → SchemaDefaultDivergence (tolerated)
//!     …the statement wrote none of them, and
//!     the correlation's history says the
//!     schema did                           → SchemaDefaultInherited (tolerated)
//!   - http status / body diffs             → StatusMismatch / BodyMismatch
//!
//! Every classification lands in `per_boundary`, and the summary's counters are
//! FOLDS of that table (see [`Scorecard::counter_disagreements`]) rather than
//! tallies kept beside it. A blocking kind and the tolerated kind that shares
//! its shape are named apart for the same reason: a report whose headline and
//! whose breakdown both say "omitted" while counting different sets of calls
//! gives two answers for one run.
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

/// Whether an unconsumed recorded call is a BLOCKING omission — the candidate
/// failing to do something the recording says it did.
///
/// Two omissions are tolerated, and neither is a failure of the candidate: an
/// UNCORRELATED one belongs to background work no test case owns (the V1
/// toleration the summary reports as `uncorrelated_events_tolerated`), and one
/// on a non-blocking boundary was never a side effect to reproduce — see
/// [`is_nonblocking_boundary`].
///
/// THE definition, shared with [`ledger::build`], so a ledger row's `blocking`
/// flag and the scorecard's count cannot come to mean two different things.
fn omission_is_blocking(correlation: Option<&str>, boundary: &str) -> bool {
    correlation.is_some() && !is_nonblocking_boundary(boundary)
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

/// A declared `project` reply canon that resolved to nothing on both the
/// recorded and the candidate body. The canon cannot absorb anything in that
/// state (see [`Projection::agrees_with`]); this names the declaration so a
/// broken one is visible rather than merely inert. It is a fact about the
/// declaration, not about the candidate, so it is counted without being charged
/// as a divergence.
const INAPPLICABLE_REPLY_CANON_WARNING: &str = "inapplicable_reply_canon";

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
    /// The driven test-case subset when the run used a correlation filter —
    /// the verdict judges ONLY these cases; absent = the full recording.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_scope: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Summary {
    pub total_correlations: u64,
    pub matched_correlations: u64,
    pub http_status_mismatches: u64,
    /// RESPONSES whose body diverged. Counts responses, while the per-boundary
    /// `BodyMismatch` kind counts diverging FIELDS — one response can carry
    /// several — so the two are deliberately not the same number.
    pub http_body_mismatches: u64,
    /// Every blocking side-effect divergence:
    /// `omitted_calls + novel_calls + value_divergences`.
    pub side_effect_divergences: u64,
    pub matched_side_effect_calls: u64,
    /// BLOCKING omissions: recorded calls the candidate never made, on a
    /// correlated, blocking boundary. These are what the verdict acts on.
    ///
    /// This is a PROJECTION of `per_boundary[*].kinds["OmittedCall"]`, not a
    /// count kept beside it — the two used to be maintained independently and
    /// gave a report two different numbers for one ledger.
    pub omitted_calls: u64,
    /// Omissions the verdict does NOT act on: uncorrelated background work, and
    /// non-blocking boundaries. Named separately because they are a different
    /// thing, not a different count of the same thing — a call ledger's
    /// `omitted` rows are `omitted_calls + omitted_calls_tolerated`, of which
    /// only the first carry `blocking`.
    ///
    /// Projection of `per_boundary[*].kinds["OmittedCallTolerated"]`.
    #[serde(default)]
    pub omitted_calls_tolerated: u64,
    /// BLOCKING novel calls: the candidate did something the recording has no
    /// baseline for, on a correlated, blocking, non-egress boundary.
    ///
    /// Projection of `per_boundary[*].kinds["NovelCall"]`.
    pub novel_calls: u64,
    /// Novel calls the verdict does NOT act on: uncorrelated background work.
    /// (An egress miss is counted as an `environmental_misses` instead, and a
    /// missing baseline as an `inconclusive_seed_gaps`.)
    ///
    /// Projection of `per_boundary[*].kinds["NovelCallTolerated"]`.
    #[serde(default)]
    pub novel_calls_tolerated: u64,
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
    /// Db value divergences classified as SCHEMA-DERIVED: every column that
    /// differed was one the statement filled with the literal SQL keyword
    /// `DEFAULT`, on both the recorded and the observed side, so the value came
    /// from the schema and not from the application. These are evidence that the
    /// two databases disagree about a column default — a fact about the
    /// environment — so they are counted and named here rather than counted in
    /// `value_divergences`/`side_effect_divergences`, and they do NOT fail the
    /// verdict. A divergence touching one bound (`$n`) column as well stays
    /// blocking; see `schema_default_divergence`.
    #[serde(default)]
    pub schema_default_divergences: u64,
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
    /// Record a call of `kind` that did not match (also bumps `diverged`).
    /// `diverged` counts everything that was not a match; `kinds` says why, and
    /// which of those the verdict acts on.
    fn bump_kind(&mut self, kind: &str) {
        *self.kinds.entry(kind.to_owned()).or_insert(0) += 1;
        self.diverged += 1;
    }
}

/// How many calls across every boundary were classified `kind`.
///
/// `per_boundary` is the classified call ledger; the summary's counters are
/// folds of it. A summary counter maintained BESIDE this table instead of
/// derived from it is how one run reported 47 omitted calls in its headline and
/// 62 in its per-boundary breakdown, for one set of calls.
fn kind_total(per_boundary: &BTreeMap<String, BoundaryStats>, kind: &str) -> u64 {
    per_boundary
        .values()
        .filter_map(|stats| stats.kinds.get(kind))
        .sum()
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
            correlation_scope: None,
            warnings: Vec::new(),
        }
    }

    /// Where the summary and the per-boundary ledger it projects disagree.
    /// Empty when the scorecard tells one story; each entry names a counter that
    /// does not, so a reader is told which number to distrust rather than being
    /// left to notice that the report contradicts itself.
    ///
    /// Counters that are deliberately NOT projections are absent:
    /// `http_body_mismatches` counts responses where the per-boundary
    /// `BodyMismatch` counts fields, and an idempotent-delete demotion names its
    /// kind after the recorded reply's canon preset, so it has no fixed key to
    /// fold.
    pub fn counter_disagreements(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut folds = |what: &str, summary: u64, kinds: &[&str]| {
            let ledger: u64 = kinds
                .iter()
                .map(|kind| kind_total(&self.per_boundary, kind))
                .sum();
            if summary != ledger {
                out.push(format!(
                    "summary.{what} = {summary}, but per_boundary {} = {ledger}",
                    kinds.join(" + ")
                ));
            }
        };
        let s = &self.summary;
        folds("omitted_calls", s.omitted_calls, &["OmittedCall"]);
        folds(
            "omitted_calls_tolerated",
            s.omitted_calls_tolerated,
            &["OmittedCallTolerated"],
        );
        folds("novel_calls", s.novel_calls, &["NovelCall"]);
        folds(
            "novel_calls_tolerated",
            s.novel_calls_tolerated,
            &["NovelCallTolerated"],
        );
        folds(
            "environmental_misses",
            s.environmental_misses,
            &["EnvironmentalMiss"],
        );
        folds(
            "value_divergences",
            s.value_divergences,
            &["ValueDivergedOrigin", "ValueDiverged"],
        );
        folds(
            "inconclusive_seed_gaps",
            s.inconclusive_seed_gaps,
            &["InconclusiveSeedGap"],
        );
        folds(
            "inconclusive_races",
            s.inconclusive_races,
            &["InconclusiveRace"],
        );
        folds(
            "order_nondeterminism_warnings",
            s.order_nondeterminism_warnings,
            &["OrderNondeterministicWarning"],
        );
        folds(
            "schema_default_divergences",
            s.schema_default_divergences,
            &["SchemaDefaultDivergence", "SchemaDefaultInherited"],
        );
        folds(
            "undeclared_concurrency_warnings",
            s.undeclared_concurrency_warnings,
            &[UNDECLARED_CONCURRENCY_WARNING],
        );
        folds(
            "recovered_rank5_calls",
            s.recovered_rank5_calls,
            &["Recovered"],
        );
        folds(
            "http_status_mismatches",
            s.http_status_mismatches,
            &["StatusMismatch"],
        );

        // The headline number: every blocking side-effect divergence, and
        // nothing else. A demotion that stopped excluding itself here would show
        // up as a verdict nobody could account for from the breakdown.
        let blocking = s.omitted_calls + s.novel_calls + s.value_divergences;
        if s.side_effect_divergences != blocking {
            out.push(format!(
                "summary.side_effect_divergences = {}, but omitted + novel + value = {blocking}",
                s.side_effect_divergences
            ));
        }

        // Matched side-effect calls exclude the request boundary, which the
        // kernel re-drives by construction rather than substituting.
        let matched: u64 = self
            .per_boundary
            .iter()
            .filter(|(boundary, _)| boundary.as_str() != "http_incoming")
            .map(|(_, stats)| stats.matched)
            .sum();
        if s.matched_side_effect_calls != matched {
            out.push(format!(
                "summary.matched_side_effect_calls = {}, but per_boundary matched = {matched}",
                s.matched_side_effect_calls
            ));
        }

        // The fragility histogram, rank by rank.
        let mut ranks: BTreeMap<&str, u64> = BTreeMap::new();
        for stats in self.per_boundary.values() {
            for (rank, n) in &stats.resolved_by_rank {
                *ranks.entry(rank.as_str()).or_insert(0) += n;
            }
        }
        for (rank, n) in &s.resolved_by_rank {
            let ledger = ranks.remove(rank.as_str()).unwrap_or(0);
            if *n != ledger {
                out.push(format!(
                    "summary.resolved_by_rank[{rank}] = {n}, but per_boundary = {ledger}"
                ));
            }
        }
        for (rank, n) in ranks {
            out.push(format!(
                "summary.resolved_by_rank[{rank}] is absent, but per_boundary = {n}"
            ));
        }
        out
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
    /// Replay scope: when the run drove a correlation SUBSET (the spec's
    /// `correlation_filter`), recorded expectations outside the subset are
    /// dropped at load — an undriven test case is excluded, never counted
    /// omitted. `None` = the full recording was driven.
    pub correlation_scope: Option<std::collections::BTreeSet<String>>,
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
    db_normalize_infra(a) == db_normalize_infra(b)
        || matches!(
            (a.as_object(), b.as_object()),
            (Some(a_obj), Some(b_obj))
                if is_structured_db_err(a_obj)
                    && is_structured_db_err(b_obj)
                    && projected_db_error_equiv(a, b)
        )
}

/// Whether a value is a structured DB `Err` payload (`{result:"Err", kind, message}`).
fn is_structured_db_err(m: &serde_json::Map<String, serde_json::Value>) -> bool {
    m.get("result").and_then(serde_json::Value::as_str) == Some("Err")
        && m.get("kind").and_then(serde_json::Value::as_str).is_some()
        && m.get("message")
            .and_then(serde_json::Value::as_str)
            .is_some()
}

/// A db result with replay-local DB infrastructure normalized away — the relation
/// [`db_equiv_modulo_infra`] tests, exposed as a value.
///
/// Lifted out of that function because [`db_row_column_diff`] has to answer WHICH
/// columns differ under the SAME relation that decided they differ at all.
/// Compared raw, a fresh postgres SERIAL `id` would show up as a differing column
/// and make a divergence the equality itself ignores look like it reached a
/// column the application supplied.
fn db_normalize_infra(v: &serde_json::Value) -> serde_json::Value {
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
                            db_normalize_infra(val)
                        };
                        (k.clone(), normalized)
                    })
                    .collect(),
            )
        }
        serde_json::Value::Array(arr) => {
            serde_json::Value::Array(arr.iter().map(db_normalize_infra).collect())
        }
        other => other.clone(),
    }
}

// ---------------------------------------------------------------------------
// Schema-derived divergence: columns the SQL fills with the DEFAULT keyword
// ---------------------------------------------------------------------------

/// The statement without its operands. Diesel renders a query as
/// `<sql> -- binds: [...]`, so everything before that tail is the statement and
/// everything after it is the values it was handed. One definition, because
/// [`pairing_shape`] and [`parse_write_statement`] have to agree on where the
/// statement ends.
fn sql_statement(sql: &str) -> &str {
    match sql.rfind(" -- binds: ") {
        Some(at) => &sql[..at],
        None => sql,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WriteKind {
    Insert,
    Update,
}

/// One INSERT or UPDATE, read as a record of WHO supplied each column it writes.
///
/// Diesel emits `DEFAULT` as the VALUES entry (or the SET right-hand side) for a
/// `None` field, so those columns are filled by the SCHEMA; everything else it
/// writes is a bind (`$n`) and is filled by the APPLICATION. A recorded
/// `INSERT INTO "payment_intent"` carries 80 columns, 48 schema-filled and 32
/// application-filled. That split is the whole basis of the classification: a
/// candidate that supplies a value can never land in the schema-filled set.
///
/// Positions in the VALUES list do NOT line up with positions in the bind list —
/// a `DEFAULT` entry consumes no bind — so the VALUES list is read directly and
/// the binds are never consulted. Reading the binds positionally is what once put
/// an `Encryption {…}` where `business_label` should have been.
///
/// Conservative in the shape of `deja::db::binds_read_keys`: anything that does
/// not parse exactly yields nothing rather than a guess. That includes an
/// identifier carrying an escaped `""`, which ends the parse instead of being
/// decoded.
#[derive(Debug, Clone, PartialEq, Eq)]
struct WriteStatement {
    kind: WriteKind,
    table: String,
    /// Columns whose value is the literal `DEFAULT` keyword.
    schema_filled: BTreeSet<String>,
    /// Every other column this statement writes — a bind, or an expression.
    application_filled: BTreeSet<String>,
}

impl WriteStatement {
    fn writes(&self, column: &str) -> bool {
        self.schema_filled.contains(column) || self.application_filled.contains(column)
    }
}

fn parse_write_statement(sql: &str) -> Option<WriteStatement> {
    /// Sort `(column, value)` pairs into the two provenance sets.
    fn split_provenance<'a>(
        assignments: impl Iterator<Item = (&'a str, &'a str)>,
    ) -> (BTreeSet<String>, BTreeSet<String>) {
        let (mut schema, mut application) = (BTreeSet::new(), BTreeSet::new());
        for (column, value) in assignments {
            let Some(column) = unquote_identifier(column) else {
                continue;
            };
            if is_default_keyword(value) {
                schema.insert(column);
            } else {
                application.insert(column);
            }
        }
        (schema, application)
    }

    let statement = sql_statement(sql).trim_start();
    let (verb, rest) = split_leading_word(statement);
    if verb.eq_ignore_ascii_case("INSERT") {
        let (into, rest) = split_leading_word(rest.trim_start());
        if !into.eq_ignore_ascii_case("INTO") {
            return None;
        }
        let (table, rest) = quoted_identifier(rest.trim_start())?;
        let (columns, rest) = parenthesized(rest.trim_start())?;
        let (values, rest) = split_leading_word(rest.trim_start());
        if !values.eq_ignore_ascii_case("VALUES") {
            return None;
        }
        let (values, _) = parenthesized(rest.trim_start())?;
        let columns = split_top_level(columns);
        let values = split_top_level(values);
        // A column list and a VALUES list of different lengths is a statement
        // this parser did not understand; refuse rather than pair them up by
        // position and name the wrong column.
        if columns.len() != values.len() {
            return None;
        }
        let (schema_filled, application_filled) = split_provenance(columns.into_iter().zip(values));
        Some(WriteStatement {
            kind: WriteKind::Insert,
            table,
            schema_filled,
            application_filled,
        })
    } else if verb.eq_ignore_ascii_case("UPDATE") {
        let (table, rest) = quoted_identifier(rest.trim_start())?;
        let (set, rest) = split_leading_word(rest.trim_start());
        if !set.eq_ignore_ascii_case("SET") {
            return None;
        }
        let assignments = match top_level_keyword(rest, &["WHERE", "RETURNING"]) {
            Some(at) => &rest[..at],
            None => rest,
        };
        let (schema_filled, application_filled) = split_provenance(
            split_top_level(assignments)
                .into_iter()
                .filter_map(split_assignment),
        );
        Some(WriteStatement {
            kind: WriteKind::Update,
            table,
            schema_filled,
            application_filled,
        })
    } else {
        None
    }
}

fn is_default_keyword(value: &str) -> bool {
    value.trim().eq_ignore_ascii_case("DEFAULT")
}

fn split_leading_word(s: &str) -> (&str, &str) {
    let end = s.find(char::is_whitespace).unwrap_or(s.len());
    (&s[..end], &s[end..])
}

/// A `"quoted identifier"` at the head of `s`, plus what follows it.
fn quoted_identifier(s: &str) -> Option<(String, &str)> {
    let rest = s.strip_prefix('"')?;
    let end = rest.find('"')?;
    Some((rest[..end].to_owned(), &rest[end + 1..]))
}

fn unquote_identifier(s: &str) -> Option<String> {
    let (name, rest) = quoted_identifier(s.trim())?;
    if name.is_empty() || !rest.trim().is_empty() {
        return None;
    }
    Some(name)
}

/// The contents of the parenthesized group at the head of `s`, plus what follows
/// its closing paren.
fn parenthesized(s: &str) -> Option<(&str, &str)> {
    if !s.starts_with('(') {
        return None;
    }
    let mut depth = 0usize;
    let mut quoted = false;
    for (at, ch) in s.char_indices() {
        if quoted {
            quoted = ch != '"';
            continue;
        }
        match ch {
            '"' => quoted = true,
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some((&s[1..at], &s[at + 1..]));
                }
            }
            _ => {}
        }
    }
    None
}

/// Split on commas that are outside both parentheses and quoted identifiers.
fn split_top_level(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0usize;
    let mut quoted = false;
    let mut start = 0usize;
    for (at, ch) in s.char_indices() {
        if quoted {
            quoted = ch != '"';
            continue;
        }
        match ch {
            '"' => quoted = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            ',' if depth == 0 => {
                parts.push(&s[start..at]);
                start = at + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

/// Split one `"column" = value` assignment on the `=` that binds it, ignoring any
/// `=` inside a quoted identifier or a parenthesized expression.
fn split_assignment(assignment: &str) -> Option<(&str, &str)> {
    let mut depth = 0usize;
    let mut quoted = false;
    for (at, ch) in assignment.char_indices() {
        if quoted {
            quoted = ch != '"';
            continue;
        }
        match ch {
            '"' => quoted = true,
            '(' => depth += 1,
            ')' => depth = depth.saturating_sub(1),
            '=' if depth == 0 => {
                return Some((&assignment[..at], &assignment[at + 1..]));
            }
            _ => {}
        }
    }
    None
}

/// Where the first of `keywords` appears as a whole word outside parentheses and
/// quoted identifiers. Keywords are ASCII, so the scan compares bytes and can
/// never slice through a character.
fn top_level_keyword(s: &str, keywords: &[&str]) -> Option<usize> {
    let bytes = s.as_bytes();
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut depth = 0usize;
    let mut quoted = false;
    for (at, ch) in s.char_indices() {
        if quoted {
            quoted = ch != '"';
            continue;
        }
        match ch {
            '"' => {
                quoted = true;
                continue;
            }
            '(' => {
                depth += 1;
                continue;
            }
            ')' => {
                depth = depth.saturating_sub(1);
                continue;
            }
            _ => {}
        }
        if depth != 0 || (at > 0 && is_word(bytes[at - 1])) {
            continue;
        }
        for keyword in keywords {
            let end = at + keyword.len();
            if end <= bytes.len()
                && bytes[at..end].eq_ignore_ascii_case(keyword.as_bytes())
                && bytes.get(end).is_none_or(|b| !is_word(*b))
            {
                return Some(at);
            }
        }
    }
    None
}

/// Which columns of two db results differ, under the same relation
/// [`db_equiv_modulo_infra`] used to decide that they differ at all. `None` when
/// either side is not a single row, so a caller refuses rather than guesses.
fn db_row_column_diff(
    recorded: &serde_json::Value,
    observed: &serde_json::Value,
) -> Option<BTreeSet<String>> {
    let recorded = db_normalize_infra(recorded);
    let observed = db_normalize_infra(observed);
    let recorded = db_returning_row(&recorded)?;
    let observed = db_returning_row(&observed)?;
    Some(
        recorded
            .keys()
            .chain(observed.keys())
            .filter(|column| recorded.get(*column) != observed.get(*column))
            .cloned()
            .collect(),
    )
}

/// Who supplied each column, per correlation and table, across every statement
/// the run ran on either side.
///
/// This exists because a returned row is not only what its own statement wrote.
/// An `UPDATE … RETURNING` hands back the whole row, so a column the statement
/// never mentions comes back carrying INHERITED state — whatever put it there
/// earlier. Attributing that value needs the row's history, and deja cannot
/// currently name a `payment_intent` row (RC4: the payment tables record no
/// typed row keys, because `binds_read_keys` looks for `"merchant_id" = $` and
/// their predicate is `"processor_merchant_id" = $n`). The CORRELATION is the
/// available approximation of the row: one request's statements.
///
/// So this index is a stand-in for a row-provenance index deja should have
/// anyway, and it is deliberately built to fail closed:
///   - `bound` is the union across BOTH sides — one statement anywhere in the
///     correlation supplying a value disqualifies the column for the whole
///     correlation, in either direction and regardless of order.
///   - `inserted_schema_filled` requires the row's CREATION to be in scope. A
///     correlation that only updates a pre-existing row proves nothing about
///     where that row's untouched columns came from — they may have come from
///     the seed — so it yields no claim at all.
#[derive(Debug, Clone, Default)]
pub(crate) struct CorrelationColumnProvenance {
    /// (correlation, table) -> columns some INSERT left to the schema.
    inserted_schema_filled: HashMap<(String, String), BTreeSet<String>>,
    /// (correlation, table) -> columns some statement supplied a value for.
    bound: HashMap<(String, String), BTreeSet<String>>,
}

impl CorrelationColumnProvenance {
    fn observe(&mut self, correlation: Option<&str>, sql: Option<&str>) {
        let (Some(correlation), Some(statement)) =
            (correlation, sql.and_then(parse_write_statement))
        else {
            return;
        };
        let key = (correlation.to_owned(), statement.table.clone());
        if statement.kind == WriteKind::Insert {
            self.inserted_schema_filled
                .entry(key.clone())
                .or_default()
                .extend(statement.schema_filled.iter().cloned());
        }
        self.bound
            .entry(key)
            .or_default()
            .extend(statement.application_filled);
    }

    /// Whether `column` of `table` was created by the schema inside this
    /// correlation and never supplied a value by anything in it.
    fn inherited_from_schema(&self, correlation: Option<&str>, table: &str, column: &str) -> bool {
        let Some(correlation) = correlation else {
            // Background work has no request scope to reason within.
            return false;
        };
        let key = (correlation.to_owned(), table.to_owned());
        self.inserted_schema_filled
            .get(&key)
            .is_some_and(|columns| columns.contains(column))
            && !self
                .bound
                .get(&key)
                .is_some_and(|columns| columns.contains(column))
    }
}

/// Build the index from both sides of the run: the recorded tape and the
/// candidate's own calls. A column either side supplied is disqualified.
pub(crate) fn correlation_column_provenance(
    events: &[deja::BoundaryEvent],
    observed: &[ObservedCall],
) -> CorrelationColumnProvenance {
    let mut provenance = CorrelationColumnProvenance::default();
    for ev in events {
        provenance.observe(
            ev.correlation_id.as_deref(),
            ev.args.get("sql").and_then(|s| s.as_str()),
        );
    }
    for obs in observed {
        provenance.observe(
            obs.correlation_id.as_deref(),
            obs.args.get("sql").and_then(|s| s.as_str()),
        );
    }
    provenance
}

/// How strong the evidence for a schema-derived divergence is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SchemaDefaultProvenance {
    /// Read straight off this statement: it fills the column with `DEFAULT`.
    Statement,
    /// INFERRED: this statement does not write the column at all, and within
    /// this correlation the row was created with the column left to the schema
    /// and nothing ever supplied a value for it.
    InheritedInCorrelation,
}

impl SchemaDefaultProvenance {
    /// Named apart in the ledger so a reader can see how much of a clean run
    /// rests on a direct reading and how much on an inference.
    fn kind(self) -> &'static str {
        match self {
            Self::Statement => "SchemaDefaultDivergence",
            Self::InheritedInCorrelation => "SchemaDefaultInherited",
        }
    }
}

/// A db divergence whose every differing column was filled by the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaDefaultDivergence {
    table: String,
    columns: Vec<String>,
    provenance: SchemaDefaultProvenance,
}

impl SchemaDefaultDivergence {
    /// `table.column` for a single column, `table.(a,b)` for several — the
    /// grouping key the warning counts by, so thirty divergences in one column
    /// read as one fact.
    pub(crate) fn label(&self) -> String {
        match self.columns.as_slice() {
            [column] => format!("{}.{column}", self.table),
            columns => format!("{}.({})", self.table, columns.join(",")),
        }
    }

    pub(crate) fn kind(&self) -> &'static str {
        self.provenance.kind()
    }

    fn is_inherited(&self) -> bool {
        self.provenance == SchemaDefaultProvenance::InheritedInCorrelation
    }
}

/// What the statements say about a db divergence's provenance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SchemaDefaultVerdict {
    /// Both statements fill every differing column with `DEFAULT`. The divergence
    /// is evidence about the two databases' schemas, not about the candidate.
    Confirmed(SchemaDefaultDivergence),
    /// The candidate's statement says schema-filled, but the recorded statement
    /// is not on hand to confirm it. Stays blocking, and says so — without the
    /// recorded side we cannot tell a schema-filled column from one the candidate
    /// STOPPED supplying.
    RecordedStatementUnavailable,
    /// Not schema-derived.
    No,
}

/// Classify a db divergence by the provenance the run's own statements declare.
///
/// A column whose `VALUES` entry (or SET right-hand side) is the literal keyword
/// `DEFAULT` was filled by the schema. A divergence confined to such columns
/// therefore says the two databases disagree about a column default, which is a
/// fact about the environment and not about the candidate — so it is counted and
/// named rather than blocking.
///
/// Two arms, and they are not equally strong:
///   - STATEMENT: this statement fills every differing column with `DEFAULT`.
///     Read directly off the artifact, no inference.
///   - INHERITED: this statement writes none of the differing columns, so their
///     values came back out of stored state. The claim is then about the ROW's
///     history, and it is granted only where
///     [`CorrelationColumnProvenance`] can stand in for it. Strictly weaker, and
///     named apart for that reason.
///
/// What keeps a real divergence out, in both arms:
///   - EVERY differing column must qualify. One column the application supplied
///     and the whole divergence stays blocking, because a statement that supplies
///     a value is the candidate speaking.
///   - BOTH statements must agree. A recorded statement that BOUND the column
///     against a candidate one that left it to the schema is a candidate that
///     stopped supplying a value — the most interesting divergence of all, and it
///     stays blocking.
///   - Neither arm is gated on an otherwise-clean run, unlike the interleaving
///     demotions, because the evidence is the statements themselves rather than
///     some other call's outcome. Nothing about the rest of the run changes what
///     `DEFAULT` means.
pub(crate) fn schema_default_divergence(
    boundary: &str,
    correlation: Option<&str>,
    recorded_sql: Option<&str>,
    observed_sql: Option<&str>,
    recorded: &serde_json::Value,
    observed: &serde_json::Value,
    provenance: &CorrelationColumnProvenance,
) -> SchemaDefaultVerdict {
    if !is_db_boundary(boundary) {
        return SchemaDefaultVerdict::No;
    }
    let Some(observed_statement) = observed_sql.and_then(parse_write_statement) else {
        return SchemaDefaultVerdict::No;
    };
    let Some(differing) = db_row_column_diff(recorded, observed) else {
        return SchemaDefaultVerdict::No;
    };
    if differing.is_empty() {
        return SchemaDefaultVerdict::No;
    }
    let table = observed_statement.table.as_str();
    // A column is schema-filled for THIS statement either because the statement
    // says so, or because the statement did not write it and the correlation's
    // history says the schema did. The two are tracked apart: a divergence that
    // needs the inference anywhere is reported as inferred, not as read.
    let qualifies = |statement: &WriteStatement, column: &String| -> Option<bool> {
        if statement.schema_filled.contains(column) {
            Some(false)
        } else if !statement.writes(column)
            && provenance.inherited_from_schema(correlation, &statement.table, column)
        {
            Some(true)
        } else {
            None
        }
    };
    let Some(observed_inference) = differing
        .iter()
        .map(|column| qualifies(&observed_statement, column))
        .try_fold(false, |acc, inferred| Some(acc | inferred?))
    else {
        return SchemaDefaultVerdict::No;
    };
    match recorded_sql.and_then(parse_write_statement) {
        Some(recorded_statement) if recorded_statement.table == observed_statement.table => {
            let Some(recorded_inference) = differing
                .iter()
                .map(|column| qualifies(&recorded_statement, column))
                .try_fold(false, |acc, inferred| Some(acc | inferred?))
            else {
                // The recorded statement supplied one of these columns: the
                // candidate stopped supplying a value it used to supply.
                return SchemaDefaultVerdict::No;
            };
            SchemaDefaultVerdict::Confirmed(SchemaDefaultDivergence {
                table: table.to_owned(),
                columns: differing.into_iter().collect(),
                provenance: if observed_inference || recorded_inference {
                    SchemaDefaultProvenance::InheritedInCorrelation
                } else {
                    SchemaDefaultProvenance::Statement
                },
            })
        }
        // The recorded statement parsed and addressed another table.
        Some(_) => SchemaDefaultVerdict::No,
        None => SchemaDefaultVerdict::RecordedStatementUnavailable,
    }
}

/// [`schema_default_divergence`] for an args-aligned call, whose two operands are
/// the call's own recorded and observed results. Mirrors
/// [`observed_value_diverged`], and shares its precondition: call it only where
/// that one already said the values diverge.
pub(crate) fn observed_schema_default_divergence(
    obs: &ObservedCall,
    event: Option<&deja::BoundaryEvent>,
    provenance: &CorrelationColumnProvenance,
) -> SchemaDefaultVerdict {
    let (Some(recorded), Some(observed)) = (&obs.recorded_result, &obs.observed_result) else {
        return SchemaDefaultVerdict::No;
    };
    schema_default_divergence(
        &obs.boundary,
        obs.correlation_id.as_deref(),
        event
            .and_then(|ev| ev.args.get("sql"))
            .and_then(|s| s.as_str()),
        obs.args.get("sql").and_then(|s| s.as_str()),
        recorded,
        observed,
        provenance,
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
            Self::Project { include, exclude } => project_canon(recorded, include, exclude)
                .agrees_with(&project_canon(observed, include, exclude)),
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
    // A `project` canon whose paths resolve on neither side used to be caught
    // here, by a guard local to this one call site. It is now a property of the
    // projection itself ([`Projection::agrees_with`]), so every consumer of a
    // `project` canon gets it — including the HTTP body diff, which had the same
    // hole and no such guard.
    //
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

/// The part of a call's args that a VALUE divergence cannot change.
///
/// The args-free pairing exists so that a write whose operand diverged (a
/// doubled amount) still pairs with its recorded twin instead of splitting into
/// OmittedCall + NovelCall. Dropping args *entirely* to achieve that made the
/// pairing pool too wide — every call through one kit function at one span
/// shared one FIFO queue — so a call could pop a recorded event describing a
/// completely different statement. Pairing on the statement instead of its
/// operands keeps the recovery and removes the cross-claim.
///
/// A SQL boundary carries its operands in diesel's ` -- binds: [...]` tail, so
/// the text before that tail is exactly "the statement without its values":
/// identical for a re-keyed write, different for a different statement, and
/// different across tables because the table name is in the statement. A
/// boundary with no SQL falls back to the structural skeleton of its args — key
/// paths with leaf values elided — which has the same property by construction.
fn pairing_shape(args: &serde_json::Value) -> String {
    // Fields deja's own args contract defines as WHAT KIND of call this is,
    // rather than what it operates on. `key` is deliberately absent: a re-keyed
    // write is precisely the divergence this pairing must still recover, so the
    // key cannot be part of the identity that finds its twin.
    const IDENTITY_FIELDS: [&str; 4] = ["operation", "table", "cache", "endpoint"];
    let mut parts = Vec::new();
    for field in IDENTITY_FIELDS {
        if let Some(value) = args.get(field).and_then(serde_json::Value::as_str) {
            parts.push(format!("{field}={value}"));
        }
    }
    match args.get("sql").and_then(serde_json::Value::as_str) {
        Some(sql) => parts.push(format!("sql={}", sql_statement(sql))),
        None => {
            let mut paths = Vec::new();
            collect_args_shape(args, String::new(), &mut paths);
            paths.sort();
            parts.push(paths.join(","));
        }
    }
    parts.join("|")
}

/// Key paths of `value`, leaf values elided. An array contributes its length
/// rather than its contents: the contents are operands.
fn collect_args_shape(value: &serde_json::Value, prefix: String, out: &mut Vec<String>) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, child) in map {
                let next = if prefix.is_empty() {
                    key.clone()
                } else {
                    format!("{prefix}.{key}")
                };
                collect_args_shape(child, next, out);
            }
        }
        serde_json::Value::Array(items) => out.push(format!("{prefix}[{}]", items.len())),
        _ => out.push(prefix),
    }
}

/// What a call must agree on to be the same call: correlation, span path,
/// boundary, method, and the shape of its args. Every component is one the
/// lookup ladder itself addresses by; the shape is `None` when the args are not
/// visible.
type PairingIdentity = (Option<String>, String, String, String, Option<String>);

/// Which recorded twin an unresolved (re-keyed) observed call may claim.
///
/// THE args-free pairing, shared by [`detect`] and [`ledger::build`]. It used to
/// be two implementations of one rule, and they drifted. The scorecard's copy
/// grew the discriminators that two production incidents forced — the span path
/// after the run-0810 phantom, the statement shape after run-0812's cross-table
/// claims — while the ledger's copy stayed keyed on `(correlation, boundary,
/// method)` alone. A method name is shared by every call through the same kit
/// function, so that key married a recorded event to any unrelated observed
/// call of the same method: on run-0813 it produced eight `value_diverged` rows
/// whose two sides ran DIFFERENT SQL statements, inside a report whose own
/// scorecard had already refused those pairs. The ledger and the scorecard
/// describing the same run differently is the failure this type exists to make
/// impossible — one rule, one place, both callers.
///
/// The identity is the rank-2 lookup address (correlation + span path) plus the
/// boundary and method the rank-6 `Sequence` address carries, plus the part of
/// the args a VALUE divergence cannot change ([`pairing_shape`]). Pairing at
/// anything weaker than what the lookup ladder already tried and rejected can
/// only manufacture pairs. An event with no rank-2 span address does not
/// participate — it stays an honest `OmittedCall`, and a span-less observed call
/// stays a `NovelCall`.
///
/// `None` shape means "we cannot see this call's args" — a WILDCARD that still
/// pairs the way it always did, not a claim that the call had no args. Only a
/// KNOWN shape narrows the pool. In a real run every table-covered sequence has
/// its event, so the wildcard queue is empty and only the shape-matched arm
/// fires; it is reachable when the tape is missing an event the table covers.
pub(crate) struct ArgsFreePairing {
    /// Recorded source sequences per identity, FIFO by source order.
    queues: BTreeMap<PairingIdentity, std::collections::VecDeque<u64>>,
}

impl ArgsFreePairing {
    /// Build the pool from the run's own two streams: the lookup table (which
    /// says which sequences are expected, and at which address) and the tape
    /// (which says what each call's args looked like). Both callers pass the
    /// SAME two, so neither can see a pool the other does not.
    pub(crate) fn build(table: &LookupTable, events: &[deja::BoundaryEvent]) -> Self {
        let events_by_seq: HashMap<u64, &deja::BoundaryEvent> =
            events.iter().map(|ev| (ev.global_sequence, ev)).collect();
        let span_paths = ledger::recorded_span_paths(table);

        // The addressable identity per recorded sequence: correlation off the
        // entry, boundary and method off the rank-6 `Sequence` address (which
        // every event emits). A sequence the table covers only at a weaker rank
        // has no boundary/method and does not pair.
        struct Addressed {
            correlation: Option<String>,
            boundary: Option<String>,
            method: Option<String>,
        }
        let mut addressed: BTreeMap<u64, Addressed> = BTreeMap::new();
        for entry in &table.entries {
            let slot = addressed
                .entry(entry.source_event_global_sequence)
                .or_insert(Addressed {
                    correlation: entry.key.correlation_id.clone(),
                    boundary: None,
                    method: None,
                });
            if let Address::Sequence {
                boundary, method, ..
            } = &entry.key.address
            {
                slot.boundary = Some(boundary.clone());
                slot.method = Some(method.clone());
            }
        }

        // `addressed` is ordered by sequence, so each queue comes out in source
        // order and `take_twin`'s pop_front is FIFO occurrence.
        let mut queues: BTreeMap<_, std::collections::VecDeque<u64>> = BTreeMap::new();
        for (seq, entry) in &addressed {
            let (Some(boundary), Some(method)) = (&entry.boundary, &entry.method) else {
                continue;
            };
            let Some(span) = span_paths.get(seq) else {
                continue;
            };
            let shape = events_by_seq.get(seq).map(|ev| pairing_shape(&ev.args));
            queues
                .entry((
                    entry.correlation.clone(),
                    span.clone(),
                    boundary.clone(),
                    method.clone(),
                    shape,
                ))
                .or_default()
                .push_back(*seq);
        }
        Self { queues }
    }

    /// Pop the next unclaimed recorded twin for `obs`, or `None` if this call
    /// has no twin it is entitled to. Skips any sequence a resolved
    /// (args-aligned) call already claimed, so a mixed run that resolves some
    /// calls normally and re-keys others never double-binds one recorded event.
    pub(crate) fn take_twin(&mut self, obs: &ObservedCall, consumed: &HashSet<u64>) -> Option<u64> {
        let span = obs.span_path.as_deref()?;
        // Same statement first; then the shape-unknown queue, whose events carry
        // no args to compare and so pair as they always did.
        let shape = pairing_shape(&obs.args);
        for candidate in [Some(shape.clone()), None] {
            let key = (
                obs.correlation_id.clone(),
                span.to_owned(),
                obs.boundary.clone(),
                obs.method_name.clone(),
                candidate,
            );
            let Some(queue) = self.queues.get_mut(&key) else {
                continue;
            };
            while let Some(seq) = queue.front().copied() {
                if consumed.contains(&seq) {
                    queue.pop_front();
                } else {
                    return queue.pop_front();
                }
            }
        }
        None
    }
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

/// One side of a `project` canon comparison: the projected value, together with
/// whether the projection retained anything of the value it came from.
///
/// The two facts have to travel together, because the projected value alone
/// cannot tell them apart. A projection that resolved no path at all and a
/// projection whose resolved path happens to hold `{}` are both `{}`, and only
/// the first means the canon had nothing to compare — the declaration named
/// fields this value does not carry. Two such projections compare equal, and
/// reading that as agreement absorbs *every* difference between the two values,
/// including the ones the declaration's author most wanted compared.
#[derive(Debug, Clone)]
struct Projection {
    value: serde_json::Value,
    /// Whether the include/exclude lists left anything of the original value.
    matched: bool,
}

impl Projection {
    /// Whether two projections agree — which requires that the canon actually
    /// applied to at least one of the two values. An inapplicable canon is
    /// evidence that the declaration is wrong, never evidence that the values
    /// are the same.
    fn agrees_with(&self, other: &Self) -> bool {
        (self.matched || other.matched) && self.value == other.value
    }
}

fn project_canon(value: &serde_json::Value, include: &[String], exclude: &[String]) -> Projection {
    if !include.is_empty() {
        // A non-empty include list is a declaration that only these paths
        // matter. It applies when at least one of them resolves; a path that
        // resolves to an empty value still counts, because the comparison did
        // happen.
        let projected: serde_json::Map<String, serde_json::Value> = include
            .iter()
            .filter_map(|field| json_path_get(value, field).map(|v| (field.clone(), v.clone())))
            .collect();
        return Projection {
            matched: !projected.is_empty(),
            value: serde_json::Value::Object(projected),
        };
    }
    if exclude.is_empty() {
        // Neither list: `project` degenerates to identity, which always applies.
        return Projection {
            value: value.clone(),
            matched: true,
        };
    }
    let projected = project_exclude_canon(value, exclude, "");
    Projection {
        matched: !projection_kept_nothing(&projected),
        value: projected,
    }
}

/// Whether an exclude list stripped a value down to nothing, leaving the
/// comparison no field to act on.
fn projection_kept_nothing(projected: &serde_json::Value) -> bool {
    match projected {
        serde_json::Value::Object(map) => map.is_empty(),
        serde_json::Value::Array(items) => items.is_empty(),
        // A scalar survives any exclude list intact, so the canon applied to it.
        _ => false,
    }
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

/// Whether an observed call ran inside a spawned fork region — a non-root
/// lineage bucket minted by the correlation layer for a `deja.fork` span. Such
/// buckets are `{parent}::fork-{seq}`, so their id carries the `::fork-` marker.
/// Fork regions are unordered relative to the request's synchronous path.
fn is_fork_region(obs: &ObservedCall) -> bool {
    obs.bucket_id
        .as_deref()
        .is_some_and(|bucket| bucket.contains("::fork-"))
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
            // Fork work (a non-root lineage bucket) is an unordered region —
            // expected to run past the HTTP response finalization — so it is
            // excluded here, exactly the role the removed `detached` flag played.
            if is_fork_region(obs) || obs.boundary == "http_incoming" || obs.timestamp_ns == 0 {
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
    /// The row's identity: the schema key's columns and values, in key order.
    /// Several, because a primary key genuinely has several columns.
    key: Vec<(String, String)>,
    wire: String,
}

impl DbRowKey {
    fn label(&self) -> String {
        let key = self
            .key
            .iter()
            .map(|(column, value)| format!("{column}={value}"))
            .collect::<Vec<_>>()
            .join(",");
        format!("{} {key} ({})", self.table, self.wire)
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
        deja::StateKey::DbRow { key, .. } => Some(DbRowKey { table, key, wire }),
        _ => None,
    }
}

fn event_db_row_key(ev: &deja::BoundaryEvent) -> Option<DbRowKey> {
    let mut row_key: Option<DbRowKey> = None;
    for raw in ev.write_set.iter().chain(ev.read_set.iter()) {
        let Some(next) = db_row_key_from_state_key(raw) else {
            continue;
        };
        if row_key
            .as_ref()
            .is_some_and(|seen| seen.table != next.table || seen.key != next.key)
        {
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
    // Grouped by correlation and the row's canonical wire key — which already
    // encodes the table and every key column, so it identifies the row whether
    // its primary key has one column or several.
    type Key = (Option<String>, String);
    let mut groups: HashMap<Key, Vec<(&deja::BoundaryEvent, DbRowKey)>> = HashMap::new();
    for ev in events {
        if !is_update_returning_event(ev) {
            continue;
        }
        let Some(row_key) = event_db_row_key(ev) else {
            continue;
        };
        groups
            .entry((ev.correlation_id.clone(), row_key.wire.clone()))
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
                && other_key.key == row_key.key
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

/// Whether the recorded response's declared reply canon says this body
/// difference does not matter.
///
/// Two ways it can. Either the canon projects both bodies to the same value —
/// a non-empty include list is a declaration that only those paths matter, so a
/// difference outside them is absorbed by design — or the difference sits on a
/// path the exclude list names. Neither can fire on a projection that resolved
/// nothing on both sides: `Projection::agrees_with` refuses that comparison, so
/// an inapplicable canon leaves every difference blocking.
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
            .agrees_with(&project_canon(candidate, &include, &exclude))
        {
            return true;
        }
    }
    http_project_excludes_json_diff_path(&exclude, &body.json_path)
}

/// The id of a declared `project` reply canon that resolved to nothing on both
/// bodies of this response pair, if that is what happened.
///
/// Absorption already refuses to act on such a canon, so nothing is hidden.
/// What is left is to say so: a declaration naming paths that no body it governs
/// carries is a defect in the declaration, and staying quiet about it is how it
/// would remain one.
fn http_reply_canon_inapplicable(
    diff: &HttpDiff,
    recorded_http: Option<&deja::BoundaryEvent>,
) -> Option<String> {
    let recorded_http = recorded_http?;
    let canon_id = recorded_http
        .declaration
        .as_ref()?
        .reply_canon
        .as_ref()?
        .id
        .clone();
    let CanonPreset::Project { include, exclude } = event_reply_canon(recorded_http)? else {
        return None;
    };
    let baseline = diff.baseline_body.as_ref()?;
    let candidate = diff.candidate_body.as_ref()?;
    (!project_canon(baseline, &include, &exclude).matched
        && !project_canon(candidate, &include, &exclude).matched)
        .then_some(canon_id)
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
    keys.dedup_by(|a, b| a.table == b.table && a.key == b.key);
    match keys.as_slice() {
        [key] => Some(key.clone()),
        _ => None,
    }
}

fn same_db_row(a: &DbRowKey, b: &DbRowKey) -> bool {
    a.table == b.table && a.key == b.key
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

    // Recorded side: unconsumed expected events grouped by args-free CALL
    // identity, ordered by source sequence, occurrence = position within the
    // group (FIFO). The rule and its scars live on `ArgsFreePairing`, which the
    // ledger builds from the same two streams so the scorecard and the per-call
    // table cannot describe one run two ways.
    let recorded_span_paths = ledger::recorded_span_paths(&art.table);
    let events_by_seq: HashMap<u64, &deja::BoundaryEvent> = art
        .events
        .iter()
        .map(|ev| (ev.global_sequence, ev))
        .collect();
    let mut recorded_pairing = ArgsFreePairing::build(&art.table, &art.events);
    let http_incoming_by_correlation = http_incoming_events_by_correlation(&art.events);

    let mut value_divergences = 0u64;
    let mut order_nondeterminism_warnings = 0u64;
    let mut idempotent_delete_warnings = 0u64;
    // Schema-derived divergences, counted by the column they name so that
    // fifteen inserts disagreeing about one column default read as one fact —
    // and, beside them, the ones we could not confirm because the recorded
    // statement was missing, so an empty class says which cause applies.
    let mut schema_default_divergences = 0u64;
    let mut schema_default_inherited = 0u64;
    let mut schema_default_columns_seen: BTreeMap<String, u64> = BTreeMap::new();
    let mut schema_default_unconfirmed = 0u64;
    // Who supplied each column, per correlation — the stand-in for the row
    // provenance deja cannot yet name on the payment tables.
    let column_provenance = correlation_column_provenance(&art.events, &art.observed);
    // Race evidence needs to be discovered before HTTP body classification:
    // a race can flow into the response body itself. Status mismatches still
    // block evidence up front; body mismatches are neutralized only when their
    // leaf values are proven attributable to the same race evidence.
    let http_status_clean =
        !art.http_diffs.is_empty() && art.http_diffs.iter().all(|d| d.status_match);
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
    let mut environmental_misses = 0u64;
    let mut blocking_side_effect = 0u64;
    let mut corr_side_effect: BTreeMap<String, u64> = BTreeMap::new();

    // PASS 1 — resolved calls claim their recorded events. The verdict must be
    // a function of the two SETS (recorded events × observed calls), never of
    // their stream interleaving: the args-free pairing consults `consumed`, so
    // every lookup resolution must be complete before the first pairing
    // decision. A mid-stream guard let an unresolved call processed early
    // steal a recorded event that a later resolved call owned — one event
    // classified twice (ValueDiverged AND matched), which is how the run-0810
    // phantom entered the scorecard.
    let mut deferred: Vec<&ObservedCall> = Vec::new();
    for obs in &art.observed {
        if obs.boundary == "http_incoming" {
            continue;
        }
        if !obs.resolved {
            deferred.push(obs);
            continue;
        }
        let stats = boundary_entry(&mut per_boundary, &obs.boundary);
        {
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
                // Rule C: every column that differs is one the statement filled
                // with the SQL keyword DEFAULT, on both sides — the schema
                // supplied the value, so this describes the two databases and
                // not the candidate. Checked before the sequence-keyed rules
                // because it needs no recorded sequence: its evidence is the
                // statement, which both sides carry on the call itself.
                match observed_schema_default_divergence(
                    obs,
                    obs.source_event_global_sequence
                        .and_then(|seq| events_by_seq.get(&seq).copied()),
                    &column_provenance,
                ) {
                    SchemaDefaultVerdict::Confirmed(schema_default) => {
                        stats.bump_kind(schema_default.kind());
                        schema_default_divergences += 1;
                        schema_default_inherited += u64::from(schema_default.is_inherited());
                        *schema_default_columns_seen
                            .entry(schema_default.label())
                            .or_insert(0) += 1;
                        if let Some(seq) = obs.source_event_global_sequence {
                            consumed.insert(seq);
                        }
                        continue;
                    }
                    SchemaDefaultVerdict::RecordedStatementUnavailable => {
                        schema_default_unconfirmed += 1;
                    }
                    SchemaDefaultVerdict::No => {}
                }
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
        }
    }

    // PASS 2 — unresolved calls, in stream order (which keeps args-free FIFO
    // occurrence stable). `consumed` is complete: no pairing decision below
    // can bind a recorded event that a resolved call owns.
    for obs in deferred {
        let stats = boundary_entry(&mut per_boundary, &obs.boundary);
        if tier_for(&obs.boundary) == Tier::Environmental {
            stats.bump_kind("EnvironmentalMiss");
            environmental_misses += 1;
        } else if is_nonblocking_boundary(&obs.boundary) {
            // Deterministic-live (crypto/time/id/rng) or the request boundary
            // (http_incoming) — not a real divergence. See is_nonblocking_boundary.
            stats.bump_kind("DeterministicMiss");
        } else if obs.correlation_id.is_none() && uncorrelated_tolerated {
            // Background-task call with no correlation — tolerated in V1. Named
            // apart from the blocking `NovelCall` because it is a different
            // thing, not a different count of the same thing.
            stats.bump_kind("NovelCallTolerated");
        } else if let Some((twin_seq, recorded)) =
            recorded_pairing.take_twin(obs, &consumed).map(|seq| {
                // The recorded operand this twin compares against is the lookup
                // entry's own result, the same one the strict-args path would
                // have substituted.
                let result = expected
                    .get(&seq)
                    .map(|exp| exp.result.clone())
                    .unwrap_or(serde_json::Value::Null);
                (seq, result)
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
            let schema_default = if value_diverged {
                schema_default_divergence(
                    &obs.boundary,
                    obs.correlation_id.as_deref(),
                    twin_event
                        .and_then(|ev| ev.args.get("sql"))
                        .and_then(|s| s.as_str()),
                    obs.args.get("sql").and_then(|s| s.as_str()),
                    &recorded_val,
                    &observed_val,
                    &column_provenance,
                )
            } else {
                SchemaDefaultVerdict::No
            };
            if matches!(
                schema_default,
                SchemaDefaultVerdict::RecordedStatementUnavailable
            ) {
                schema_default_unconfirmed += 1;
            }
            if let SchemaDefaultVerdict::Confirmed(schema_default) = schema_default {
                // Rule C, on the args-free arm: same statement-borne evidence,
                // same non-blocking class.
                stats.bump_kind(schema_default.kind());
                schema_default_divergences += 1;
                schema_default_inherited += u64::from(schema_default.is_inherited());
                *schema_default_columns_seen
                    .entry(schema_default.label())
                    .or_insert(0) += 1;
            } else if value_diverged {
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
    for (seq, exp) in &expected {
        if consumed.contains(seq) || paired_consumed.contains(seq) {
            continue;
        }
        let boundary = exp.boundary.clone().unwrap_or_else(|| "unknown".to_owned());
        // One classification, named for what it counts. Lumping the tolerated
        // omissions — uncorrelated background work, and non-blocking boundaries
        // — under the same name as the blocking ones is what let this table and
        // the summary give a report two answers for one set of calls.
        let blocking = omission_is_blocking(exp.correlation.as_deref(), &boundary);
        let stats = boundary_entry(&mut per_boundary, &boundary);
        stats.bump_kind(if blocking {
            "OmittedCall"
        } else {
            "OmittedCallTolerated"
        });
        if blocking {
            blocking_side_effect += 1;
            if let Some(corr) = &exp.correlation {
                *corr_side_effect.entry(corr.clone()).or_insert(0) += 1;
            }
        }
    }

    // The accounting identity, asserted: every recorded event resolves through
    // EXACTLY one arm — resolved (`consumed`), args-free paired
    // (`paired_consumed`), or the omitted pass above. The two claim sets
    // intersecting means one event was scored twice (matched AND diverged) —
    // the double-claim that manufactured the run-0810 phantom lock
    // divergences. Two-pass resolution makes this disjoint by construction;
    // this assertion is the backstop that turns the next pairing defect into
    // a loud failure at scoring time instead of a fabricated divergence that
    // misdirects an investigation.
    let double_claimed: Vec<u64> = consumed.intersection(&paired_consumed).copied().collect();
    assert!(
        double_claimed.is_empty(),
        "scorer accounting violation: recorded event(s) {double_claimed:?} were classified by \
         both the resolved arm and the args-free pairing — one event, two verdict outcomes"
    );

    // The summary's call counters are PROJECTIONS of the per-boundary ledger
    // above, folded out of it once every call has been classified — never a
    // second tally kept alongside it, which is what let them disagree.
    let omitted_calls = kind_total(&per_boundary, "OmittedCall");
    let omitted_calls_tolerated = kind_total(&per_boundary, "OmittedCallTolerated");
    let novel_calls = kind_total(&per_boundary, "NovelCall");
    let novel_calls_tolerated = kind_total(&per_boundary, "NovelCallTolerated");

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
    // Declarations, not responses: one broken reply canon governing forty
    // responses is one fact about the declaration, and it is the fact that says
    // where to look. Keyed by canon id, counting the responses it governed
    // vacuously.
    let mut inapplicable_reply_canons: BTreeMap<String, u64> = BTreeMap::new();
    {
        let stats = boundary_entry(&mut per_boundary, "http_incoming");
        for diff in &art.http_diffs {
            let recorded_http = http_incoming_by_correlation
                .get(&diff.correlation_id)
                .copied();
            if let Some(canon) = http_reply_canon_inapplicable(diff, recorded_http) {
                // Counted in `kinds` without touching `diverged`, the way
                // `undeclared_concurrency` is: the candidate did not cause this,
                // so it must be visible without being charged to it.
                *stats
                    .kinds
                    .entry(INAPPLICABLE_REPLY_CANON_WARNING.to_owned())
                    .or_insert(0) += 1;
                *inapplicable_reply_canons.entry(canon).or_insert(0) += 1;
            }
            let blocking_body_diffs =
                blocking_http_body_diff_count(diff, recorded_http, &inconclusive_race);
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
    // Rule C: a divergence the statements themselves attribute to the schema
    // describes the two databases, not the candidate. Reported, non-blocking.
    if schema_default_divergences > 0 {
        // The split is in the headline, not buried: a reader has to be able to
        // see how much of this rests on a statement and how much on an
        // inference, without opening the breakdown.
        reasons.push(format!(
            "{schema_default_divergences} schema-derived divergence(s) (non-blocking; \
             {} read off the statement, {schema_default_inherited} inherited within a \
             correlation)",
            schema_default_divergences - schema_default_inherited
        ));
    }
    if undeclared_concurrency_warnings > 0 {
        reasons.push(format!(
            "{undeclared_concurrency_warnings} undeclared_concurrency warning(s) (non-blocking)"
        ));
    }
    // Seed-gap + race + order-nondeterminism + idempotent-delete +
    // schema-derived + undeclared_concurrency lines are informational, not
    // divergences the candidate caused; exclude
    // them from the blocking count so a run whose only "reasons" are those still
    // avoids a blocking failure (race becomes an explicit inconclusive verdict).
    let blocking_reasons = reasons.len()
        - usize::from(inconclusive_seed_gaps > 0)
        - usize::from(inconclusive_races > 0)
        - usize::from(order_nondeterminism_warnings > 0)
        - usize::from(idempotent_delete_warnings > 0)
        - usize::from(schema_default_divergences > 0)
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
    // One line per COLUMN, not per call: fifteen inserts disagreeing about one
    // column default is one fact about the two schemas, and it is the fact that
    // says where to look.
    for (column, n) in &schema_default_columns_seen {
        warnings.push(format!(
            "{n} db divergence(s) confined to {column} classified as schema-derived: the \
             recording's database and the replay's disagree about that column's default, and \
             no statement on either side ever supplied a value for it — the candidate did not \
             cause this"
        ));
    }
    // The inference names itself and its limit. It stands in for a row
    // provenance deja cannot yet express on these tables, and a reader who does
    // not know that cannot judge how far to trust the green.
    if schema_default_inherited > 0 {
        warnings.push(format!(
            "{schema_default_inherited} of those were INFERRED, not read: the statement did not \
             write the column, so its value came out of stored state, and the claim rests on \
             the correlation having created the row with that column left to the schema. \
             Correlation stands in for the row here because the payment tables record no typed \
             row keys; a write to the same row from another correlation would not be seen"
        ));
    }
    // A declaration that governs nothing is reported as the defect it is. The
    // bodies were still compared in full — an inapplicable canon absorbs
    // nothing — so this costs no coverage; what it costs is a declaration
    // someone believed was in force.
    for (canon, responses) in &inapplicable_reply_canons {
        warnings.push(format!(
            "{INAPPLICABLE_REPLY_CANON_WARNING}: {responses} response(s) declare the reply canon \
             {canon}, which resolves to nothing on either the recorded or the candidate body. It \
             cannot say the two agree, so every body difference on those responses stays \
             blocking; the declaration names fields these bodies do not carry and is itself what \
             needs fixing"
        ));
    }
    // An empty class has two causes and they need opposite fixes; say which.
    if schema_default_unconfirmed > 0 {
        warnings.push(format!(
            "{schema_default_unconfirmed} db divergence(s) look schema-derived from the \
             candidate's statement alone, but the recorded statement was unavailable to confirm \
             it, so they stay blocking — without it a schema-filled column cannot be told from \
             one the candidate stopped supplying"
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
            omitted_calls_tolerated,
            novel_calls,
            novel_calls_tolerated,
            value_divergences,
            order_nondeterminism_warnings,
            schema_default_divergences,
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
        correlation_scope: art
            .correlation_scope
            .as_ref()
            .map(|scope| scope.iter().cloned().collect()),
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
    let run = crate::read_json::<crate::Run>(&root.run_path(run_id)).ok();
    let recording_id = run.as_ref().and_then(|run| {
        run.recording_id
            .clone()
            .or_else(|| run.spec.recording_id.clone())
    });
    // The kernel drove only this subset (KERNEL_CORRELATION_FILTER); scope
    // recorded expectations to it so undriven cases don't score as omitted.
    let scope = run
        .as_ref()
        .map(crate::scope::RunScope::of)
        .unwrap_or_else(crate::scope::RunScope::entire_session);
    let correlation_scope: Option<std::collections::BTreeSet<String>> = scope.ids().cloned();

    let mut warnings = Vec::new();
    let mut table = load_table(&root.lookup_table_path(run_id), &mut warnings);
    let observed = load_observed_calls(&root.observed_path(run_id), &mut warnings);
    let http_diffs = load_jsonl::<HttpDiff>(&root.http_diff_path(run_id), &mut warnings);

    // The record graph could not be built for this run: the extract left the
    // reason in a note instead of failing the run, and this is where the note
    // becomes part of the verdict's own record. Scoring does not read the
    // graph, so the verdict below is unaffected — the warning says what the
    // report's execution view will be missing, and why.
    if let Ok(note) = std::fs::read_to_string(root.record_graph_note_path(run_id)) {
        let note = note.trim();
        if !note.is_empty() {
            warnings.push(note.to_owned());
        }
    }

    // Seeding failures change what a divergence MEANS: a candidate that 404s
    // because its precondition row never materialized is not a behaviour
    // change. The certificate records every failure per entry; this is where
    // that fact reaches the same page as the verdict it re-frames. Parsed
    // loosely (the certificate is the lifecycle's type, this is a reader) —
    // absence of the file, an old shape, or zero failures all mean no warning.
    if let Ok(text) = std::fs::read_to_string(root.seed_certificate_path(run_id)) {
        if let Ok(cert) = serde_json::from_str::<serde_json::Value>(&text) {
            let failed = cert["summary"]["failed"].as_u64().unwrap_or(0);
            if failed > 0 {
                let mut tables: Vec<String> = cert["entries"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|e| e["materialization"] == "failed" && e["boundary"] == "db")
                    .filter_map(|e| e["logical_key"].as_str())
                    .filter_map(|k| {
                        deja::StateKey::parse(k)
                            .ok()
                            .and_then(|sk| sk.db_table().map(str::to_owned))
                    })
                    .collect();
                tables.sort();
                tables.dedup();
                // The distinct reasons, not one per entry: thirty-five entries
                // refusing the same column is one fact, and it is the fact that
                // says whether the seed or the candidate is at fault.
                let mut reasons: Vec<String> = cert["entries"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter(|e| e["materialization"] == "failed" && e["boundary"] == "db")
                    .filter_map(|e| e["readback"]["message"].as_str())
                    // A store's own error text can carry row data and run long;
                    // the warning is a pointer to the certificate, not a copy.
                    .map(|message| match message.char_indices().nth(240) {
                        Some((cut, _)) => format!("{}…", &message[..cut]),
                        None => message.to_owned(),
                    })
                    .collect();
                reasons.sort();
                reasons.dedup();
                reasons.truncate(3);
                warnings.push(format!(
                    "{failed} seed entr{} FAILED to materialize{} — reads of those rows replay \
                     against an empty table, so their divergences describe the missing seed, \
                     not the candidate (full detail per entry in the seed certificate){}",
                    if failed == 1 { "y" } else { "ies" },
                    if tables.is_empty() {
                        String::new()
                    } else {
                        format!(" (tables: {})", tables.join(", "))
                    },
                    if reasons.is_empty() {
                        String::new()
                    } else {
                        format!("; {}", reasons.join("; "))
                    },
                ));
            }
        }
    }

    // A request the candidate never answered is one finding, not fifty-four:
    // its field diffs describe an absence. Name each failure and its reason at
    // the top of the report, grouped by reason so three identical timeouts
    // read as one fact.
    {
        let mut by_reason: std::collections::BTreeMap<&str, Vec<&str>> =
            std::collections::BTreeMap::new();
        for diff in &http_diffs {
            if let Some(reason) = diff.transport_error.as_deref() {
                by_reason
                    .entry(reason)
                    .or_default()
                    .push(diff.correlation_id.as_str());
            }
        }
        for (reason, correlations) in by_reason {
            warnings.push(format!(
                "no response from the candidate for {} request(s) ({}): {reason} — their body \
                 diffs describe the missing response, not changed behaviour",
                correlations.len(),
                correlations.join(", "),
            ));
        }
    }

    // The tape is read THROUGH the scope, not read and then trimmed: the
    // events this function returns are the only ones any consumer sees, so
    // there is no second, wider view of the same run to disagree with.
    let mut events = Vec::new();
    if let Some(rec) = &recording_id {
        match crate::scope::ScopedRecording::open(root, rec, scope.clone()) {
            Ok(recording) => match recording.events() {
                // A run mid-flight has no tape yet; that is not a corrupt run.
                Ok(stream) => {
                    for item in stream {
                        match item {
                            crate::scope::TapeItem::Event(event) => events.push(*event),
                            crate::scope::TapeItem::Malformed { line_no, error, .. } => warnings
                                .push(format!("recording {rec}:{line_no}: parse error: {error}")),
                        }
                    }
                }
                Err(e) => warnings.push(format!("read recording {rec} failed: {e}")),
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => warnings.push(format!("open recording {rec} failed: {e}")),
        }
    }

    if let Some(ids) = scope.ids() {
        // The lookup table on disk is still the whole session's (it is written
        // before the scope is known), so it is trimmed here. Uncorrelated
        // (background) entries stay: they are tolerated by the scorer and
        // shared across cases. No-silent-caps: say what was cut.
        let entries_before = table.entries.len();
        table
            .entries
            .retain(|e| scope.contains(e.key.correlation_id.as_deref()));
        warnings.push(format!(
            "correlation scope: {} id(s) driven; excluded {} lookup entries outside the subset; \
             {} recorded event(s) in scope",
            ids.len(),
            entries_before - table.entries.len(),
            events.len(),
        ));
    }

    Ok(RunArtifacts {
        run_id: run_id.to_owned(),
        recording_id,
        table,
        observed,
        http_diffs,
        events,
        correlation_scope,
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
    match build_ledger(&art) {
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
///
/// Reads `art.events`, which `load_artifacts` has ALREADY scoped to the run's
/// `correlation_filter`. It used to reload the tape from `root` unscoped, so on
/// any run carrying a filter the scorecard classified one event set and
/// `GET /runs/{id}/calls` classified a different, larger one — same run, same
/// data, two answers, and recorded payloads from correlations the run never
/// drove attached to its ledger rows.
pub fn build_ledger(art: &RunArtifacts) -> io::Result<Vec<CallRecord>> {
    let events = &art.events;
    let span_paths = ledger::recorded_span_paths(&art.table);
    // Mirror scorecard classification: discover race evidence under status-clean
    // HTTP first, then treat only unattributable body diffs as blocking.
    let http_status_clean =
        !art.http_diffs.is_empty() && art.http_diffs.iter().all(|d| d.status_match);
    let http_incoming_by_correlation = http_incoming_events_by_correlation(events);
    let inconclusive_race =
        inconclusive_race_evidence(events, &art.observed, http_status_clean, &span_paths);
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
    let demote = order_nondeterministic_demotions(events, &art.observed, http_clean);
    let idempotent_delete = idempotent_delete_demotions(events, &art.observed, http_clean);
    Ok(ledger::build_with_inconclusive(
        events,
        &art.observed,
        &art.table,
        &demote.sequences,
        &idempotent_delete,
        &inconclusive_race,
    ))
}

/// Read-through ledger for `GET /runs/{id}/calls` (recomputes from artifacts;
/// works for runs scored before the sidecar existed).
pub fn call_ledger(root: &HarnessRoot, run_id: &str) -> io::Result<Vec<CallRecord>> {
    let art = load_artifacts(root, run_id)?;
    build_ledger(&art)
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

/// Load the shared graph-as-events wire stream from JSONL. The stream is
/// internally tagged as [`deja::DejaRecord`], so callers match variants instead
/// of routing by raw `record_kind` strings.
fn load_deja_records(path: &std::path::Path, warnings: &mut Vec<String>) -> Vec<deja::DejaRecord> {
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
        match serde_json::from_str::<deja::DejaRecord>(line) {
            Ok(record) => out.push(record),
            Err(e) => warnings.push(format!("{}:{}: parse error: {e}", path.display(), i + 1)),
        }
    }
    out
}

// NOTE: there is deliberately no `load_boundary_events(path)` here any more.
// It read a recording tape from a raw `&Path` with no notion of the run's
// scope, which is how `build_ledger` came to classify a different, wider event
// set than the scorecard it was supposed to mirror. Recordings are read through
// `scope::ScopedRecording`.

fn load_observed_calls(path: &std::path::Path, warnings: &mut Vec<String>) -> Vec<ObservedCall> {
    load_deja_records(path, warnings)
        .into_iter()
        .filter_map(|record| match record {
            deja::DejaRecord::Observed(call) => Some(*call),
            deja::DejaRecord::BoundaryEvent(_) | deja::DejaRecord::GraphNode(_) => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use deja::{LookupEntry, LookupKey};
    use deja_kernel::JsonFieldDiff;

    /// Row identity is read from the schema at run time (see
    /// `register_table_identity`); tests stand in for that read with the same
    /// shape the statement returns. Idempotent, so every test that needs row
    /// keys can call it without ordering assumptions.
    fn register_test_schema_identity() {
        deja::register_table_identity([
            ("payment_attempt".to_owned(), vec!["attempt_id".to_owned()]),
            ("payment_intent".to_owned(), vec!["payment_id".to_owned()]),
            (
                "payment_methods".to_owned(),
                vec!["payment_method_id".to_owned()],
            ),
            (
                "merchant_account".to_owned(),
                vec!["merchant_id".to_owned()],
            ),
            (
                "merchant_key_store".to_owned(),
                vec!["merchant_id".to_owned()],
            ),
            ("business_profile".to_owned(), vec!["profile_id".to_owned()]),
            (
                "merchant_connector_account".to_owned(),
                vec!["merchant_connector_id".to_owned()],
            ),
            ("customers".to_owned(), vec!["customer_id".to_owned()]),
            ("users".to_owned(), vec!["user_id".to_owned()]),
            ("address".to_owned(), vec!["address_id".to_owned()]),
            ("configs".to_owned(), vec!["key".to_owned()]),
        ]);
    }
    /// [`super::detect`], with the report's INTERNAL CONSISTENCY checked on
    /// every fixture in this module.
    ///
    /// This shadows the glob-imported `detect` deliberately, so the ~50 cases
    /// below all carry the guard without repeating it: whatever a case is
    /// asserting, a summary that disagrees with the per-boundary ledger it
    /// projects is a scorer bug. One run reported 47 omitted calls in its
    /// headline and 62 in its breakdown; a scorecard that contradicts itself
    /// must not survive a single test in here.
    fn detect(art: &RunArtifacts) -> Scorecard {
        let card = super::detect(art);
        let disagreements = card.counter_disagreements();
        assert!(
            disagreements.is_empty(),
            "the scorecard contradicts itself: {}",
            disagreements.join("; ")
        );
        card
    }

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
        }
    }

    fn seq_entry(corr: Option<&str>, boundary: &str, src: u64) -> LookupEntry {
        seq_entry_res(corr, boundary, src, serde_json::json!("v"))
    }

    /// Rank-2 `SpanPath` table entry for `src`. Args-free pairing is
    /// span-scoped — the recorded event (via this address) and the observed
    /// call (via [`ObservedCall::span_path`]) must present the same call
    /// identity to pair; a bare method name is not an identity. Append AFTER
    /// the event's `Sequence` entry: the `expected` fold keeps the FIRST
    /// entry's result per source sequence.
    fn span_entry(corr: Option<&str>, src: u64, path: &str) -> LookupEntry {
        let mut entry = seq_entry(corr, "span", src);
        entry.key.address = Address::SpanPath {
            path: path.to_owned(),
        };
        entry
    }

    /// Stamp the span path an observed call fired within (pairing identity).
    fn with_span(mut o: ObservedCall, path: &str) -> ObservedCall {
        o.span_path = Some(path.to_owned());
        o
    }

    /// A correlation filter must scope scoring to the DRIVEN subset: an
    /// undriven case's recorded calls are excluded at load (never omitted),
    /// while a driven-but-unobserved call still counts as a real omission.
    #[test]
    fn correlation_filter_scopes_expectations_at_load() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let run_id = "run-scope";
        crate::write_json(
            &root.run_path(run_id),
            &crate::Run {
                run_id: run_id.to_owned(),
                spec: crate::RunSpec {
                    mode: crate::RunMode::Replay,
                    candidate_spec: crate::CandidateSpec::PrebuiltImage {
                        image: "deja-demo".to_owned(),
                    },
                    candidate_repo: None,
                    recording_id: Some("rec-scope".to_owned()),
                    s3_source: None,
                    correlation_filter: Some(vec!["c-keep".to_owned(), " ".to_owned()]),
                    workload: serde_json::Value::Null,
                },
                status: crate::RunStatus::Completed,
                recording_id: Some("rec-scope".to_owned()),
                candidate_image: None,
                failure_reason: None,
                stage: None,
                step: 0,
                steps_total: 0,
                stage_updated_ms: 0,
            },
        )
        .unwrap();
        crate::write_json(
            &root.lookup_table_path(run_id),
            &LookupTable {
                recording_id: "rec-scope".to_owned(),
                policy_version: 1,
                entries: vec![
                    seq_entry(Some("c-keep"), "db", 1),
                    seq_entry(Some("c-drop"), "db", 2),
                    seq_entry(None, "db", 3),
                ],
            },
        )
        .unwrap();

        let art = load_artifacts(&root, run_id).unwrap();
        let scope = art.correlation_scope.as_ref().expect("scope set");
        assert_eq!(
            scope.iter().collect::<Vec<_>>(),
            ["c-keep"],
            "blank filter ids are dropped"
        );
        let corrs: Vec<Option<&str>> = art
            .table
            .entries
            .iter()
            .map(|e| e.key.correlation_id.as_deref())
            .collect();
        assert_eq!(
            corrs,
            [Some("c-keep"), None],
            "out-of-scope entries are dropped; uncorrelated background stays"
        );

        let card = detect(&art);
        assert_eq!(
            card.correlation_scope.as_deref(),
            Some(&["c-keep".to_owned()][..])
        );
        assert_eq!(
            card.summary.omitted_calls, 1,
            "the driven-but-unobserved c-keep call is a real omission; \
             the undriven c-drop call must not count"
        );
    }

    /// The ledger and the scorecard must classify the SAME events. The ledger
    /// used to RELOAD the tape from `root` — unscoped — while `detect()` read
    /// the already-scoped `art.events`, so one run got two recorded sides.
    ///
    /// The observable damage: the replay kernel resolves against the WHOLE
    /// lookup table (`render_lookup_table` is unscoped), so an in-scope call
    /// can carry a `source_event_global_sequence` belonging to a correlation
    /// this run never drove. With the reload, `recorded_for` found that event
    /// and the `/calls` row published another production request's recorded
    /// args and result. With `art.events` it cannot: the event is not in scope,
    /// so there is nothing to attach.
    #[test]
    fn build_ledger_never_attaches_a_recorded_side_from_outside_the_run_scope() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let run_id = "run-ledger-scope";
        let recording_id = "rec-ledger-scope";
        let driven_row = serde_json::json!({ "attempt_id": "pay_driven" });
        let foreign_row = serde_json::json!({ "attempt_id": "pay_NOT_IN_SCOPE" });
        // The tape on disk holds BOTH cases: one session's tape, a run driving
        // a subset of it. That is the production shape.
        write_recording_tape(
            &crate::scope::TapeSlot::for_write(&root, recording_id),
            &[
                db_read_ev(
                    "c-keep",
                    "payment_attempt",
                    1,
                    driven_row.clone(),
                    100,
                    110,
                    "root",
                    0,
                ),
                db_read_ev(
                    "c-drop",
                    "payment_attempt",
                    2,
                    foreign_row.clone(),
                    100,
                    110,
                    "root",
                    0,
                ),
            ],
        );
        crate::write_json(
            &root.run_path(run_id),
            &crate::Run {
                run_id: run_id.to_owned(),
                spec: crate::RunSpec {
                    mode: crate::RunMode::Replay,
                    candidate_spec: crate::CandidateSpec::PrebuiltImage {
                        image: "deja-demo".to_owned(),
                    },
                    candidate_repo: None,
                    recording_id: Some(recording_id.to_owned()),
                    s3_source: None,
                    correlation_filter: Some(vec!["c-keep".to_owned()]),
                    workload: serde_json::Value::Null,
                },
                status: crate::RunStatus::Completed,
                recording_id: Some(recording_id.to_owned()),
                candidate_image: None,
                failure_reason: None,
                stage: None,
                step: 0,
                steps_total: 0,
                stage_updated_ms: 0,
            },
        )
        .unwrap();
        crate::write_json(
            &root.lookup_table_path(run_id),
            &LookupTable {
                recording_id: recording_id.to_owned(),
                policy_version: 1,
                entries: vec![
                    seq_entry(Some("c-keep"), "db", 1),
                    seq_entry(Some("c-drop"), "db", 2),
                ],
            },
        )
        .unwrap();
        // The driven case resolved against the FOREIGN case's baseline (seq 2) —
        // the kernel consults the whole unscoped lookup table, so this happens.
        write_jsonl_rows(
            &root.observed_path(run_id),
            &[deja::DejaRecord::Observed(Box::new(exec_obs(
                "db",
                Some("c-keep"),
                true,
                Some(2),
                Some(envelope(foreign_row.clone())),
                envelope(foreign_row.clone()),
            )))],
        );

        let art = load_artifacts(&root, run_id).unwrap();
        assert_eq!(
            art.events.len(),
            1,
            "load_artifacts scopes the recorded side to the driven subset"
        );
        let rows = build_ledger(&art).unwrap();
        let dump = serde_json::to_string(&rows).unwrap();
        assert!(
            !dump.contains("pay_NOT_IN_SCOPE"),
            "the ledger published a recorded payload from a correlation the run \
             never drove: {dump}"
        );
        assert!(
            rows.iter()
                .all(|r| r.correlation_id.as_deref() != Some("c-drop")),
            "no ledger row may be attributed to an out-of-scope correlation"
        );
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

    /// Write a recording tape fixture in the tagged one-stream wire shape
    /// (each boundary event flat beside its `record_kind` tag).
    fn write_recording_tape(path: &std::path::Path, events: &[deja::BoundaryEvent]) {
        let rows: Vec<deja::DejaRecord> = events
            .iter()
            .cloned()
            .map(|event| deja::DejaRecord::BoundaryEvent(Box::new(event)))
            .collect();
        write_jsonl_rows(path, &rows);
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
            transport_error: None,
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
            correlation_scope: None,
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

        let rows = build_ledger(&RunArtifacts {
            run_id: "run-db-volatile-canon-ledger".to_owned(),
            recording_id: Some("rec-1".to_owned()),
            table: LookupTable {
                recording_id: "rec-1".to_owned(),
                policy_version: 1,
                entries,
            },
            observed,
            http_diffs: vec![http(corr, true, vec![])],
            events: events.clone(),
            correlation_scope: None,
            warnings: Vec::new(),
        })
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

    /// One response pair carrying the given reply canon and one body difference
    /// on `json_path`, scored. The shape every reply-canon absorption test needs.
    fn http_reply_canon_card(
        corr: &str,
        seq: u64,
        canon: &str,
        json_path: &str,
        baseline_body: serde_json::Value,
        candidate_body: serde_json::Value,
    ) -> Scorecard {
        let leaf = json_path.trim_start_matches("$.");
        let baseline_leaf = baseline_body.get(leaf).cloned().unwrap_or_default();
        let candidate_leaf = candidate_body.get(leaf).cloned().unwrap_or_default();
        detect(&art_with_events(
            vec![],
            vec![],
            vec![http_with_bodies(
                corr,
                true,
                vec![JsonFieldDiff {
                    json_path: json_path.to_owned(),
                    baseline: baseline_leaf,
                    candidate: candidate_leaf,
                }],
                baseline_body.clone(),
                candidate_body,
            )],
            vec![http_incoming_ev_with_reply_canon(
                corr,
                seq,
                Some(canon),
                baseline_body,
            )],
        ))
    }

    /// The absorber's failure mode: an include list naming paths that neither
    /// body carries projects both sides to `{}`, and two empty projections used
    /// to compare equal — which absorbed every difference between the bodies,
    /// including a payment's terminal status.
    #[test]
    fn http_reply_canon_include_matching_neither_body_absorbs_nothing() {
        let card = http_reply_canon_card(
            "vacuous-http-reply-canon",
            503,
            "project:settlement.state,settlement.reference",
            "$.status",
            serde_json::json!({"id": "resp_1", "status": "succeeded", "amount": 100}),
            serde_json::json!({"id": "resp_1", "status": "failed", "amount": 100}),
        );

        assert_eq!(
            card.summary.http_body_mismatches, 1,
            "an empty projection is evidence the canon did not apply, never evidence \
             that the bodies agree"
        );
        assert!(!card.verdict.pass, "{}", card.verdict.reason);
        assert_eq!(
            kind_count(&card, "http_incoming", INAPPLICABLE_REPLY_CANON_WARNING),
            1,
            "the declaration that governs nothing is counted, not silently ignored"
        );
        assert!(
            card.warnings.iter().any(|warning| warning
                .starts_with(INAPPLICABLE_REPLY_CANON_WARNING)
                && warning.contains("project:settlement.state,settlement.reference")),
            "the warning must name the declaration to fix: {:?}",
            card.warnings
        );
    }

    /// The other half of the same rule: a non-empty include list is a
    /// declaration that only those paths matter, so a difference outside it is
    /// absorbed by design. Refusing empty projections must not cost this.
    #[test]
    fn http_reply_canon_include_absorbs_a_difference_outside_the_declared_paths() {
        let card = http_reply_canon_card(
            "http-reply-canon-outside-include",
            504,
            "project:id,status",
            "$.created",
            serde_json::json!({"id": "resp_1", "status": "succeeded", "created": "10:00:00"}),
            serde_json::json!({"id": "resp_1", "status": "succeeded", "created": "10:00:01"}),
        );

        assert_eq!(
            card.summary.http_body_mismatches, 0,
            "the include list resolves on both bodies and agrees; a difference outside \
             it is what the declaration asked to ignore"
        );
        assert_eq!(
            kind_count(&card, "http_incoming", INAPPLICABLE_REPLY_CANON_WARNING),
            0,
            "a canon that applied is not an inapplicable canon"
        );
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    /// An include list that resolves still guards the paths it names.
    #[test]
    fn http_reply_canon_include_present_in_both_bodies_absorbs_only_when_it_agrees() {
        let agreeing = http_reply_canon_card(
            "http-reply-canon-include-agrees",
            505,
            "project:id",
            "$.amount",
            serde_json::json!({"id": "resp_1", "amount": 100}),
            serde_json::json!({"id": "resp_1", "amount": 101}),
        );
        assert_eq!(
            agreeing.summary.http_body_mismatches, 0,
            "the declared path is present in both bodies and equal"
        );
        assert!(agreeing.verdict.pass, "{}", agreeing.verdict.reason);

        let disagreeing = http_reply_canon_card(
            "http-reply-canon-include-disagrees",
            506,
            "project:id",
            "$.id",
            serde_json::json!({"id": "resp_1", "amount": 100}),
            serde_json::json!({"id": "resp_2", "amount": 100}),
        );
        assert_eq!(
            disagreeing.summary.http_body_mismatches, 1,
            "a project canon must not hide a change to a path it declared"
        );
        assert_eq!(
            kind_count(
                &disagreeing,
                "http_incoming",
                INAPPLICABLE_REPLY_CANON_WARNING
            ),
            0,
            "the canon applied and disagreed; the declaration is fine, the bodies are not"
        );
        assert!(!disagreeing.verdict.pass);
    }

    /// Exclude semantics, including the case where the exclude list strips the
    /// whole body: the difference is absorbed because it sits on an excluded
    /// path, never because two stripped bodies both projected to `{}`.
    #[test]
    fn http_reply_canon_exclude_absorbs_the_path_it_names() {
        let partial = http_reply_canon_card(
            "http-reply-canon-exclude",
            507,
            "project:!trace_id",
            "$.trace_id",
            serde_json::json!({"id": "resp_1", "trace_id": "trace-a"}),
            serde_json::json!({"id": "resp_1", "trace_id": "trace-b"}),
        );
        assert_eq!(
            partial.summary.http_body_mismatches, 0,
            "a difference on an excluded path is what the declaration asked to ignore"
        );
        assert!(partial.verdict.pass, "{}", partial.verdict.reason);

        let whole_body = http_reply_canon_card(
            "http-reply-canon-exclude-everything",
            508,
            "project:!trace_id",
            "$.trace_id",
            serde_json::json!({"trace_id": "trace-a"}),
            serde_json::json!({"trace_id": "trace-b"}),
        );
        assert_eq!(
            whole_body.summary.http_body_mismatches, 0,
            "the excluded path still absorbs when it is the body's only field"
        );
        assert!(whole_body.verdict.pass, "{}", whole_body.verdict.reason);
    }

    /// The same rule at the value level, where a `project` canon governs a
    /// recorded result rather than an HTTP body.
    #[test]
    fn project_canon_with_no_resolving_path_is_not_a_value_match() {
        let canon = resolve_canon(Some(&deja::CanonRef::new("project:settlement.state")))
            .expect("project preset resolves");
        assert!(
            !canon.equivalent(
                &serde_json::json!({"status": "charged"}),
                &serde_json::json!({"status": "failed"})
            ),
            "neither value carries the declared path, so the canon has nothing to say"
        );
        assert!(
            !canon.equivalent(&serde_json::json!("charged"), &serde_json::json!("failed")),
            "a non-object projects to nothing under an include list, which is not agreement"
        );

        let resolving = resolve_canon(Some(&deja::CanonRef::new("project:status")))
            .expect("project preset resolves");
        assert!(
            resolving.equivalent(
                &serde_json::json!({"status": "charged", "updated_at": 1}),
                &serde_json::json!({"status": "charged", "updated_at": 2})
            ),
            "a resolving include list still absorbs differences outside itself"
        );
        assert!(
            resolving.equivalent(
                &serde_json::json!({"status": {}}),
                &serde_json::json!({"status": {}, "updated_at": 2})
            ),
            "a declared path whose value is empty MATCHED; only a path that resolves \
             nowhere is inapplicability"
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
    ) -> ObservedCall {
        let mut o = obs(boundary, corr, src.is_some(), src.map(|_| 3), src);
        o.method_name = method.to_owned();
        o.timestamp_ns = timestamp_ns;
        o
    }

    #[test]
    fn undeclared_concurrency_warns_for_correlated_post_finalization_work() {
        let card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 2)],
            vec![
                observed_finalizer("c1", 11_000),
                observed_at("redis", Some("c1"), "set_key", Some(2), 11_001),
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
    fn undeclared_concurrency_ignores_fork_region_post_finalization_work() {
        // Work in a spawned fork region (a non-root `::fork-` bucket) is an
        // unordered region — expected to run past finalization — so it must not
        // be flagged as undeclared concurrency.
        let mut forked = observed_at("redis", Some("c1"), "set_key", Some(2), 11_001);
        forked.bucket_id = Some("root::fork-1".to_owned());
        let card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 2)],
            vec![observed_finalizer("c1", 11_000), forked],
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
        register_test_schema_identity();
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
        register_test_schema_identity();
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
        register_test_schema_identity();
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

    /// A recorded event the candidate never reproduced, for the omitted pass.
    fn omitted_ev(seq: u64, boundary: &str, corr: Option<&str>) -> deja::BoundaryEvent {
        serde_json::from_value(serde_json::json!({
            "global_sequence": seq,
            "request_sequence": 0,
            "correlation_id": corr,
            "timestamp_ns": 0,
            "boundary": boundary,
            "trait_name": "T",
            "method_name": "m",
            "call_file": "x.rs",
            "call_line": 1,
            "call_column": 0,
            "request": {},
            "args": {},
            "response": {},
            "result": "v",
            "is_error": false,
            "duration_us": 0,
            "event_schema_version": deja::CURRENT_EVENT_SCHEMA_VERSION,
            "provenance": "recorded",
            "recon": "lossless",
            "replay_strategy": "substitute",
            "bucket_id": "root",
            "fork_seq": 0,
        }))
        .expect("valid BoundaryEvent")
    }

    /// The defect this guards against: one run reported 47 omitted calls in its
    /// summary while its per-boundary breakdown and its `/calls` ledger both
    /// reported 62. Nothing was miscounted — the summary counted the BLOCKING
    /// omissions, the other two counted every omission, and all three called it
    /// "omitted". The three now name what they count, and the relationship
    /// between them is arithmetic rather than a coincidence.
    #[test]
    fn omitted_means_the_same_thing_in_the_summary_the_breakdown_and_the_ledger() {
        // Two omissions the verdict acts on, and two it does not: background
        // work no test case owns, and a pure entropy seam.
        let a = art_with_events(
            vec![
                seq_entry(Some("c1"), "redis", 1),
                seq_entry(Some("c1"), "db", 2),
                seq_entry(None, "redis", 3),
                seq_entry(Some("c1"), "time", 4),
            ],
            vec![],
            vec![http("c1", true, vec![])],
            vec![
                omitted_ev(1, "redis", Some("c1")),
                omitted_ev(2, "db", Some("c1")),
                omitted_ev(3, "redis", None),
                omitted_ev(4, "time", Some("c1")),
            ],
        );
        let card = detect(&a);

        assert_eq!(
            card.summary.omitted_calls, 2,
            "the headline counts what fails the verdict"
        );
        assert_eq!(card.summary.omitted_calls_tolerated, 2);
        assert_eq!(kind_count(&card, "redis", "OmittedCall"), 1);
        assert_eq!(kind_count(&card, "db", "OmittedCall"), 1);
        assert_eq!(kind_count(&card, "redis", "OmittedCallTolerated"), 1);
        assert_eq!(kind_count(&card, "time", "OmittedCallTolerated"), 1);

        // The `/calls` ledger classifies the same four events, and its split is
        // the summary's two numbers — not a third answer.
        let rows = ledger::build(
            &a.events,
            &a.observed,
            &a.table,
            &HashSet::new(),
            &HashSet::new(),
        );
        let omitted: Vec<&CallRecord> = rows.iter().filter(|r| r.kind == "omitted").collect();
        assert_eq!(
            omitted.len() as u64,
            card.summary.omitted_calls + card.summary.omitted_calls_tolerated,
            "every omission the ledger shows is one of the two the summary names"
        );
        assert_eq!(
            omitted.iter().filter(|r| r.blocking).count() as u64,
            card.summary.omitted_calls,
            "and the blocking ones are exactly the headline number"
        );
    }

    /// The invariant itself: a summary counter that drifts from the ledger it
    /// projects is reported, not served as if the report agreed with itself.
    #[test]
    fn a_summary_that_drifts_from_its_breakdown_is_caught() {
        let mut card = detect(&art(
            vec![seq_entry(Some("c1"), "redis", 7)],
            vec![],
            vec![http("c1", true, vec![])],
        ));
        assert!(card.counter_disagreements().is_empty());

        // The original shape of the bug: a headline number maintained beside the
        // per-boundary ledger instead of folded out of it, drifting from it.
        card.summary.omitted_calls = 47;
        let found = card.counter_disagreements();
        assert!(
            found
                .iter()
                .any(|line| line.starts_with("summary.omitted_calls = 47")),
            "the disagreement must name the counter to distrust: {found:?}"
        );

        // The tolerated omissions are a projection too, and so is the headline
        // side-effect total the verdict is written from.
        let mut card = detect(&art(vec![seq_entry(None, "redis", 7)], vec![], vec![]));
        assert_eq!(card.summary.omitted_calls_tolerated, 1);
        card.summary.omitted_calls_tolerated = 0;
        assert!(!card.counter_disagreements().is_empty());
    }

    /// The same split on the novel side, where `NovelCall` had the same defect:
    /// an uncorrelated background call was counted under the blocking name.
    #[test]
    fn a_tolerated_novel_call_is_not_counted_under_the_blocking_name() {
        let card = detect(&art(
            vec![],
            vec![
                obs("redis", Some("c1"), false, None, None),
                obs("redis", None, false, None, None),
            ],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.novel_calls, 1, "the correlated one blocks");
        assert_eq!(card.summary.novel_calls_tolerated, 1);
        assert_eq!(kind_count(&card, "redis", "NovelCall"), 1);
        assert_eq!(kind_count(&card, "redis", "NovelCallTolerated"), 1);
        assert_eq!(
            card.summary.side_effect_divergences, 1,
            "background work does not fail a candidate"
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
                // Span identities for the re-keyed consequences: pairing is
                // span-scoped, so each unresolved call pairs only with its own
                // call site's recorded twin.
                span_entry(Some(corr), 12, "root>flow>write_b_prime"),
                span_entry(Some(corr), 13, "root>flow>read_b_prime"),
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
                with_span(
                    exec_obs_method(
                        "storage",
                        Some(corr),
                        "write_b_prime",
                        false,
                        None,
                        None,
                        b_prime_candidate,
                    ),
                    "root>flow>write_b_prime",
                ),
                with_span(
                    exec_obs_method(
                        "db",
                        Some(corr),
                        "read_b_prime",
                        false,
                        None,
                        None,
                        c_candidate_read,
                    ),
                    "root>flow>read_b_prime",
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
        register_test_schema_identity();
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
        let mut downstream_observation = with_span(
            exec_obs_method(
                "storage",
                Some(corr),
                "write_branch",
                false,
                None,
                None,
                downstream_observed,
            ),
            "root>flow>write_branch",
        );
        downstream_observation.args = serde_json::json!({"source": envelope(raced_row.clone())});

        let card = detect(&art_with_events(
            vec![
                seq_entry_method_res(
                    Some(corr),
                    "storage",
                    "write_branch",
                    302,
                    downstream_recorded,
                ),
                span_entry(Some(corr), 302, "root>flow>write_branch"),
            ],
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
        register_test_schema_identity();
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
        register_test_schema_identity();
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
        let recorded_events = vec![read_event, conflicting_write];

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
            events: recorded_events,
            correlation_scope: None,
            warnings: Vec::new(),
        };

        let rows = build_ledger(&art).unwrap();
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
        register_test_schema_identity();
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
    fn pairing_shape_separates_statements_and_tables_but_not_rekeyed_operands() {
        // A re-keyed write must keep its twin: same statement, different binds.
        let confirm = serde_json::json!({
            "operation": "generic_update_with_results", "table": "payment_attempt",
            "sql": "UPDATE \"payment_attempt\" SET \"status\" = $1 WHERE \"attempt_id\" = $2 \
                    -- binds: [Pending, \"a_1\"]"});
        let confirm_rekeyed = serde_json::json!({
            "operation": "generic_update_with_results", "table": "payment_attempt",
            "sql": "UPDATE \"payment_attempt\" SET \"status\" = $1 WHERE \"attempt_id\" = $2 \
                    -- binds: [Charged, \"a_9\"]"});
        assert_eq!(
            pairing_shape(&confirm),
            pairing_shape(&confirm_rekeyed),
            "operands live in the binds tail; a re-keyed write must still pair"
        );

        // A DIFFERENT statement at the same call site must not claim it. This is
        // run-0812: a 10-column connector-response UPDATE popped an 18-column
        // confirm UPDATE out of the same FIFO queue.
        let connector_response = serde_json::json!({
            "operation": "generic_update_with_results", "table": "payment_attempt",
            "sql": "UPDATE \"payment_attempt\" SET \"connector_transaction_id\" = $1 \
                    WHERE \"attempt_id\" = $2 -- binds: [TxnId(\"D4P\"), \"a_1\"]"});
        assert_ne!(pairing_shape(&confirm), pairing_shape(&connector_response));

        // And a different TABLE must not claim it, with or without SQL — the
        // ledger showed a recorded payment_attempt row scored against an
        // observed payment_intent row.
        let intent = serde_json::json!({
            "operation": "generic_update_with_results", "table": "payment_intent",
            "sql": "UPDATE \"payment_intent\" SET \"status\" = $1 WHERE \"payment_id\" = $2 \
                    -- binds: [Pending, \"p_1\"]"});
        assert_ne!(pairing_shape(&confirm), pairing_shape(&intent));
        assert_ne!(
            pairing_shape(&serde_json::json!({"table": "payment_attempt"})),
            pairing_shape(&serde_json::json!({"table": "payment_intent"})),
            "table identity must survive the no-SQL fallback"
        );

        // A re-keyed cache write keeps its twin: `key` is an operand, not identity.
        assert_eq!(
            pairing_shape(&serde_json::json!({"cache": "ACCOUNTS_CACHE", "key": "a"})),
            pairing_shape(&serde_json::json!({"cache": "ACCOUNTS_CACHE", "key": "b"}))
        );
    }

    #[test]
    fn rekeyed_write_pairs_args_free_into_one_value_divergence() {
        // GOTCHA #1: the diverged WRITE carries a mutated operand, so its args
        // miss the recorded baseline → recorded twin would be Omitted, the execute
        // call would be Novel. The args-free pairing must collapse them into ONE
        // ValueDiverged (NOT Novel+Omitted), and flip the correlation to diverged.
        let card = detect(&art(
            vec![
                seq_entry_res(Some("c1"), "storage", 7, serde_json::json!(100)),
                span_entry(Some("c1"), 7, "root>write_amount"),
            ],
            vec![with_span(
                exec_obs(
                    "storage",
                    Some("c1"),
                    false,                  // re-keyed args missed the baseline → unresolved
                    None,                   // no source_event_global_sequence (it didn't resolve)
                    None, // hook found no args-aligned baseline (seed_gap on hook side)
                    serde_json::json!(200), // the doubled amount
                ),
                "root>write_amount", // same call site — the identity that pairs
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
            vec![
                seq_entry_res(Some("c1"), "storage", 7, serde_json::json!("v")),
                span_entry(Some("c1"), 7, "root>write_v"),
            ],
            vec![with_span(
                exec_obs(
                    "storage",
                    Some("c1"),
                    false,
                    None,
                    None,
                    serde_json::json!("v"),
                ),
                "root>write_v",
            )],
            vec![http("c1", true, vec![])],
        ));
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.novel_calls, 0);
        assert_eq!(card.summary.omitted_calls, 0);
        assert_eq!(card.summary.matched_side_effect_calls, 1);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
    }

    /// The run-0810 phantom-lock shape. An error path makes a novel call of a
    /// COMMON method (`get_key`) the recording never made — the uninstrumented
    /// cache fallthrough reading `business_profile_…` and getting `Null` —
    /// while the recording holds a same-method event at a DIFFERENT call site
    /// (the API_LOCK release GET) that a later observed call resolves
    /// normally. Method-name pairing let the novel call steal the lock event
    /// (`ValueDiverged`: Null vs lock id), which the resolved call then ALSO
    /// claimed as matched — one recorded event, two verdict outcomes, 24
    /// fabricated lock divergences in the sandbox scorecard. Span-scoped
    /// pairing plus two-pass resolution make the theft unrepresentable —
    /// whichever side of the stream the novel call arrives on, which is the
    /// other half of the defect: the verdict must be a function of the sets,
    /// not the interleaving.
    #[test]
    fn novel_call_of_a_common_method_cannot_steal_another_call_sites_event() {
        let corr = "c1";
        let lock_value = serde_json::json!({"BulkString": "recording-request-id"});
        let build = |novel_first: bool| {
            let mut novel = obs("redis", Some(corr), false, None, None);
            novel.method_name = "get_key".to_owned();
            novel.observed_result = Some(serde_json::json!("Null"));
            let novel = with_span(
                novel,
                "root>get_trackers>find_business_profile>get_or_populate_redis",
            );
            let resolved = with_span(
                exec_obs_method(
                    "redis",
                    Some(corr),
                    "get_key",
                    true,
                    Some(127),
                    Some(lock_value.clone()),
                    lock_value.clone(),
                ),
                "root>server_wrap>release_lock",
            );
            let observed = if novel_first {
                vec![novel, resolved]
            } else {
                vec![resolved, novel]
            };
            detect(&art(
                vec![
                    seq_entry_method_res(Some(corr), "redis", "get_key", 127, lock_value.clone()),
                    span_entry(Some(corr), 127, "root>server_wrap>release_lock"),
                ],
                observed,
                vec![http(corr, true, vec![])],
            ))
        };
        for (label, card) in [("novel first", build(true)), ("novel last", build(false))] {
            assert_eq!(
                card.summary.value_divergences, 0,
                "{label}: no fabricated divergence on the lock event"
            );
            assert_eq!(
                card.summary.matched_side_effect_calls, 1,
                "{label}: the real lock GET matches"
            );
            assert_eq!(
                kind_count(&card, "redis", "NovelCall"),
                1,
                "{label}: the fallthrough call reports as ITSELF — a novel call"
            );
            assert_eq!(
                card.summary.omitted_calls, 0,
                "{label}: the lock event is claimed exactly once"
            );
        }
    }

    // ---- Rule C: schema-derived divergence (columns filled with DEFAULT) ----

    /// The statement shape diesel actually emits, abridged to the columns the
    /// tests reason about but keeping the property that makes it interesting:
    /// the VALUES list interleaves binds and `DEFAULT`, so a column's position
    /// in the column list does NOT index the bind list. `business_label` is the
    /// fourth column and the SECOND `DEFAULT`; the bind list has three entries
    /// and no third bind to mis-read it from.
    const PAYMENT_INTENT_INSERT: &str = "INSERT INTO \"payment_intent\" (\"payment_id\", \
        \"merchant_id\", \"amount_captured\", \"business_label\", \"currency\") VALUES ($1, $2, \
        DEFAULT, DEFAULT, $3) -- binds: [PaymentId(\"pay_1\"), MerchantId(\"m_1\"), USD]";

    /// The same statement from a candidate that SUPPLIES `business_label`: the
    /// column is a bind, not `DEFAULT`.
    const PAYMENT_INTENT_INSERT_BINDING_LABEL: &str =
        "INSERT INTO \"payment_intent\" (\"payment_id\", \"merchant_id\", \"amount_captured\", \
        \"business_label\", \"currency\") VALUES ($1, $2, DEFAULT, $3, $4) -- binds: \
        [PaymentId(\"pay_1\"), MerchantId(\"m_1\"), \"retail\", USD]";

    fn payment_intent_row(business_label: serde_json::Value) -> serde_json::Value {
        serde_json::json!({
            "payment_id": "pay_1",
            "merchant_id": "m_1",
            "amount_captured": serde_json::Value::Null,
            "business_label": business_label,
            "currency": "USD",
        })
    }

    /// A recorded db INSERT carrying its rendered SQL, the way a real tape does.
    fn db_insert_ev(
        corr: &str,
        seq: u64,
        sql: &str,
        row: serde_json::Value,
    ) -> deja::BoundaryEvent {
        let mut ev = db_update_ev(corr, "payment_intent", seq, row, 100, 110);
        ev.method_name = "generic_insert".to_owned();
        ev.args = serde_json::json!({"table": "payment_intent", "sql": sql});
        ev
    }

    /// An args-aligned execute-shadow call against `sql` whose real result
    /// differs from the recorded baseline in the given row values.
    fn db_exec_obs_with_sql(
        corr: &str,
        seq: u64,
        sql: &str,
        recorded: serde_json::Value,
        observed: serde_json::Value,
    ) -> ObservedCall {
        let mut o = exec_obs_method(
            "db",
            Some(corr),
            "generic_insert",
            true,
            Some(seq),
            Some(envelope(recorded)),
            envelope(observed),
        );
        o.args = serde_json::json!({"table": "payment_intent", "sql": sql});
        o
    }

    /// One correlation, one db INSERT that diverges from `recorded` to
    /// `observed`. The tape carries the recorded statement `recorded_sql`, which
    /// defaults to the candidate's `observed_sql` — the byte-identical case a
    /// same-image replay actually produces.
    fn schema_default_card(
        recorded_sql: Option<&str>,
        observed_sql: &str,
        recorded: serde_json::Value,
        observed: serde_json::Value,
    ) -> Scorecard {
        let corr = "c1";
        let ev = db_insert_ev(
            corr,
            7,
            recorded_sql.unwrap_or(observed_sql),
            recorded.clone(),
        );
        detect(&art_with_events(
            vec![seq_entry_method_res(
                Some(corr),
                "db",
                "generic_insert",
                7,
                envelope(recorded.clone()),
            )],
            vec![db_exec_obs_with_sql(
                corr,
                7,
                observed_sql,
                recorded,
                observed,
            )],
            vec![http(corr, true, vec![])],
            vec![ev],
        ))
    }

    #[test]
    fn insert_values_list_names_the_columns_the_schema_filled() {
        let defaults =
            parse_write_statement(PAYMENT_INTENT_INSERT).expect("the INSERT shape parses");
        assert_eq!(defaults.table, "payment_intent");
        assert_eq!(
            defaults.schema_filled,
            ["amount_captured", "business_label"]
                .into_iter()
                .map(str::to_owned)
                .collect::<BTreeSet<_>>(),
            "only the columns whose VALUES entry is the DEFAULT keyword — read off the VALUES \
             list, which the binds list does not index because a DEFAULT consumes no bind"
        );
        // A candidate that supplies the value emits $n, so the column moves out
        // of the schema-filled set and into the application-filled one.
        let supplied = parse_write_statement(PAYMENT_INTENT_INSERT_BINDING_LABEL).expect("parses");
        assert_eq!(
            supplied.schema_filled,
            ["amount_captured"]
                .into_iter()
                .map(str::to_owned)
                .collect::<BTreeSet<_>>()
        );
        assert!(supplied.application_filled.contains("business_label"));
    }

    /// The parser, calibrated against a statement lifted verbatim off a tape
    /// rather than one written to suit it. Writing this parser was the easy
    /// half; the shape it has to survive is 80 columns whose VALUES list
    /// interleaves 32 binds with 48 `DEFAULT`s across 2.3 KB of text, and a
    /// fixture invented alongside the code proves nothing about that.
    #[test]
    fn the_parser_reads_a_real_recorded_payment_intent_insert() {
        let defaults = parse_write_statement(include_str!("fixtures/payment_intent_insert.sql"))
            .expect("the recorded statement parses");
        assert_eq!(defaults.table, "payment_intent");
        assert_eq!(
            defaults.schema_filled.len(),
            48,
            "every column diesel left to the schema, not just the one that differs today"
        );
        assert!(defaults.schema_filled.contains("business_label"));
        // Columns the request supplied are binds, and stay out.
        for supplied in ["payment_id", "merchant_id", "status", "amount", "currency"] {
            assert!(
                !defaults.schema_filled.contains(supplied),
                "{supplied} is bound in this statement"
            );
        }
    }

    #[test]
    fn update_set_clause_names_the_columns_the_schema_filled() {
        let defaults = parse_write_statement(
            "UPDATE \"payment_intent\" SET \"status\" = $1, \"business_label\" = DEFAULT, \
             \"modified_at\" = $2 WHERE ((\"payment_intent\".\"payment_id\" = $3) AND \
             (\"payment_intent\".\"processor_merchant_id\" = $4)) RETURNING * \
             -- binds: [Pending, 2026-08-13, \"pay_1\", \"m_1\"]",
        )
        .expect("the UPDATE shape parses");
        assert_eq!(defaults.table, "payment_intent");
        assert_eq!(
            defaults.schema_filled,
            ["business_label"]
                .into_iter()
                .map(str::to_owned)
                .collect::<BTreeSet<_>>(),
            "the SET list ends at the top-level WHERE — a quoted column inside the predicate is \
             not an assignment"
        );
    }

    #[test]
    fn a_statement_this_parser_does_not_understand_names_no_columns() {
        // A column list and a VALUES list of different lengths cannot be paired
        // by position without naming the wrong column, so it names none.
        assert_eq!(
            parse_write_statement(
                "INSERT INTO \"payment_intent\" (\"a\", \"b\") VALUES ($1, DEFAULT, DEFAULT)"
            ),
            None
        );
        // Not an INSERT or an UPDATE at all.
        assert_eq!(
            parse_write_statement("SELECT \"payment_intent\".\"business_label\" FROM x"),
            None
        );
    }

    #[test]
    fn a_divergence_confined_to_schema_filled_columns_is_named_and_does_not_block() {
        let card = schema_default_card(
            None,
            PAYMENT_INTENT_INSERT,
            payment_intent_row(serde_json::Value::Null),
            payment_intent_row(serde_json::json!("default")),
        );
        assert_eq!(
            kind_count(&card, "db", "SchemaDefaultDivergence"),
            1,
            "the column the statement left to the schema is its own class"
        );
        assert_eq!(card.summary.schema_default_divergences, 1);
        assert_eq!(
            card.summary.value_divergences, 0,
            "and it is NOT a value divergence"
        );
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert!(card.verdict.pass, "reason: {}", card.verdict.reason);
        assert!(
            card.verdict.reason.contains(
                "1 schema-derived divergence(s) (non-blocking; 1 read off the statement, 0 \
                 inherited within a correlation)"
            ),
            "counted and named in the verdict, never silently dropped, and the strength of the \
             evidence is in the headline: {}",
            card.verdict.reason
        );
        assert!(
            card.warnings.iter().any(
                |w| w.contains("payment_intent.business_label") && w.contains("schema-derived")
            ),
            "the warning names the column, which is what says where to look: {:?}",
            card.warnings
        );
        assert!(
            card.counter_disagreements().is_empty(),
            "{:?}",
            card.counter_disagreements()
        );
    }

    #[test]
    fn a_divergence_in_a_bound_column_stays_blocking() {
        // `currency` is $3 in this statement: the application supplied it.
        let card = schema_default_card(
            None,
            PAYMENT_INTENT_INSERT,
            payment_intent_row(serde_json::Value::Null),
            serde_json::json!({
                "payment_id": "pay_1",
                "merchant_id": "m_1",
                "amount_captured": serde_json::Value::Null,
                "business_label": serde_json::Value::Null,
                "currency": "EUR",
            }),
        );
        assert_eq!(card.summary.schema_default_divergences, 0);
        assert_eq!(card.summary.value_divergences, 1);
        assert_eq!(card.summary.side_effect_divergences, 1);
        assert!(!card.verdict.pass);
    }

    #[test]
    fn a_divergence_spanning_a_schema_filled_and_a_bound_column_stays_blocking() {
        let card = schema_default_card(
            None,
            PAYMENT_INTENT_INSERT,
            payment_intent_row(serde_json::Value::Null),
            serde_json::json!({
                "payment_id": "pay_1",
                "merchant_id": "m_1",
                "amount_captured": serde_json::Value::Null,
                "business_label": "default",
                "currency": "EUR",
            }),
        );
        assert_eq!(
            card.summary.schema_default_divergences, 0,
            "one bound column in the set and the whole divergence is the candidate's"
        );
        assert_eq!(card.summary.value_divergences, 1);
        assert_eq!(card.summary.side_effect_divergences, 1);
        assert!(!card.verdict.pass);
    }

    #[test]
    fn a_candidate_that_stopped_supplying_a_value_stays_blocking() {
        // The RECORDING bound `business_label`; the candidate left it to the
        // schema. The column is schema-filled on the observed side alone, and
        // that is exactly the divergence that must not be absorbed.
        let card = schema_default_card(
            Some(PAYMENT_INTENT_INSERT_BINDING_LABEL),
            PAYMENT_INTENT_INSERT,
            payment_intent_row(serde_json::json!("retail")),
            payment_intent_row(serde_json::json!("default")),
        );
        assert_eq!(card.summary.schema_default_divergences, 0);
        assert_eq!(card.summary.value_divergences, 1);
        assert!(!card.verdict.pass);
    }

    #[test]
    fn an_unavailable_recorded_statement_stays_blocking_and_says_why() {
        // Same divergence as the passing case, but the tape carries no event, so
        // the recorded statement cannot confirm the provenance.
        let corr = "c1";
        let recorded = payment_intent_row(serde_json::Value::Null);
        let card = detect(&art(
            vec![seq_entry_method_res(
                Some(corr),
                "db",
                "generic_insert",
                7,
                envelope(recorded.clone()),
            )],
            vec![db_exec_obs_with_sql(
                corr,
                7,
                PAYMENT_INTENT_INSERT,
                recorded,
                payment_intent_row(serde_json::json!("default")),
            )],
            vec![http(corr, true, vec![])],
        ));
        assert_eq!(card.summary.schema_default_divergences, 0);
        assert_eq!(card.summary.value_divergences, 1);
        assert!(
            card.warnings
                .iter()
                .any(|w| w.contains("recorded statement was unavailable")),
            "an empty class names which of its causes applies: {:?}",
            card.warnings
        );
    }

    // ---- the inherited arm: a column the statement did not write -----------

    /// An UPDATE that writes some columns and returns the whole row, so every
    /// other column in the RETURNING row is inherited stored state.
    const PAYMENT_INTENT_UPDATE: &str = "UPDATE \"payment_intent\" SET \"currency\" = $1 WHERE \
        (\"payment_intent\".\"payment_id\" = $2) RETURNING * -- binds: [USD, \"pay_1\"]";

    /// The same UPDATE, but this one also supplies `business_label`.
    const PAYMENT_INTENT_UPDATE_BINDING_LABEL: &str =
        "UPDATE \"payment_intent\" SET \"currency\" = $1, \"business_label\" = $2 WHERE \
        (\"payment_intent\".\"payment_id\" = $3) RETURNING * -- binds: [USD, \"retail\", \"pay_1\"]";

    /// A correlation whose INSERT created the row and whose UPDATE then returns
    /// it with `business_label` diverging. `also_ran` are extra statements in
    /// the same correlation, which is what the inference is scoped to.
    fn inherited_card(also_ran: &[&str], insert_sql: Option<&str>) -> Scorecard {
        let corr = "c1";
        let recorded = payment_intent_row(serde_json::Value::Null);
        let mut update = db_insert_ev(corr, 8, PAYMENT_INTENT_UPDATE, recorded.clone());
        update.method_name = "generic_update_with_results".to_owned();
        let mut events = vec![update];
        if let Some(insert_sql) = insert_sql {
            events.push(db_insert_ev(corr, 7, insert_sql, recorded.clone()));
        }
        let mut observed = vec![{
            let mut o = db_exec_obs_with_sql(
                corr,
                8,
                PAYMENT_INTENT_UPDATE,
                recorded.clone(),
                payment_intent_row(serde_json::json!("default")),
            );
            o.method_name = "generic_update_with_results".to_owned();
            o
        }];
        // The INSERT itself matched on replay; only the later UPDATE diverges,
        // so the inference is doing the work rather than the statement rule.
        if insert_sql.is_some() {
            observed.push(substituted_obs_method(
                "db",
                Some(corr),
                "generic_insert",
                7,
                envelope(recorded.clone()),
            ));
        }
        for (n, sql) in also_ran.iter().enumerate() {
            let seq = 20 + n as u64;
            let mut ev = db_insert_ev(corr, seq, sql, recorded.clone());
            ev.method_name = "generic_update_with_results".to_owned();
            events.push(ev);
            observed.push(substituted_obs_method(
                "db",
                Some(corr),
                "generic_update_with_results",
                seq,
                envelope(recorded.clone()),
            ));
        }
        let entries = events
            .iter()
            .map(|ev| {
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    &ev.method_name,
                    ev.global_sequence,
                    envelope(recorded.clone()),
                )
            })
            .collect();
        detect(&art_with_events(
            entries,
            observed,
            vec![http(corr, true, vec![])],
            events,
        ))
    }

    #[test]
    fn a_column_the_statement_never_wrote_is_inherited_from_the_correlations_insert() {
        let card = inherited_card(&[], Some(PAYMENT_INTENT_INSERT));
        assert_eq!(
            kind_count(&card, "db", "SchemaDefaultInherited"),
            1,
            "the UPDATE writes only `currency`; `business_label` came back out of the row the \
             correlation's INSERT left to the schema"
        );
        assert_eq!(
            kind_count(&card, "db", "SchemaDefaultDivergence"),
            0,
            "and it is NOT reported as read off the statement — the two are named apart"
        );
        assert_eq!(card.summary.schema_default_divergences, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert!(card.verdict.pass, "reason: {}", card.verdict.reason);
        assert!(
            card.verdict
                .reason
                .contains("0 read off the statement, 1 inherited within a correlation"),
            "the headline says how much rests on the inference: {}",
            card.verdict.reason
        );
        assert!(
            card.warnings
                .iter()
                .any(|w| w.contains("INFERRED, not read")),
            "the inference names itself and its limit: {:?}",
            card.warnings
        );
        assert!(card.counter_disagreements().is_empty());
    }

    #[test]
    fn a_correlation_that_ever_supplied_the_column_gets_no_inference() {
        // One statement elsewhere in the same correlation binds
        // `business_label`. The application wrote that column in this request,
        // so nothing in the request can claim the schema owns it — regardless of
        // whether that statement ran before or after the diverging one.
        let card = inherited_card(
            &[PAYMENT_INTENT_UPDATE_BINDING_LABEL],
            Some(PAYMENT_INTENT_INSERT),
        );
        assert_eq!(card.summary.schema_default_divergences, 0);
        assert_eq!(card.summary.value_divergences, 1);
        assert!(!card.verdict.pass);
    }

    #[test]
    fn an_inherited_claim_needs_a_schema_filled_insert_in_the_same_correlation() {
        // The correlation's INSERT supplies `business_label` rather than leaving
        // it to the schema, so the row's stored value is the application's and
        // the later UPDATE's divergence in it is blocking.
        let card = inherited_card(&[], Some(PAYMENT_INTENT_INSERT_BINDING_LABEL));
        assert_eq!(card.summary.schema_default_divergences, 0);
        assert_eq!(card.summary.value_divergences, 1);
        assert!(!card.verdict.pass);
    }

    #[test]
    fn an_update_on_a_row_this_correlation_did_not_create_gets_no_inference() {
        // THE limit of the approximation, pinned. Nothing in this correlation
        // binds `business_label` and nothing contradicts the inference — but the
        // correlation never created the row either, so the row is a SEEDED one
        // whose stored value came from outside the request. Where that value came
        // from is exactly what the inference cannot see, so it must not be made:
        // a seed carrying a real value would otherwise be laundered into "the
        // schema did it".
        let card = inherited_card(&[], None);
        assert_eq!(
            card.summary.schema_default_divergences, 0,
            "no INSERT in scope means no claim about where the row's columns came from"
        );
        assert_eq!(card.summary.value_divergences, 1);
        assert!(!card.verdict.pass);
    }

    #[test]
    fn the_ledger_and_the_scorecard_agree_a_schema_derived_row_is_not_blocking() {
        let corr = "c1";
        let recorded = payment_intent_row(serde_json::Value::Null);
        let ev = db_insert_ev(corr, 7, PAYMENT_INTENT_INSERT, recorded.clone());
        let art = art_with_events(
            vec![seq_entry_method_res(
                Some(corr),
                "db",
                "generic_insert",
                7,
                envelope(recorded.clone()),
            )],
            vec![db_exec_obs_with_sql(
                corr,
                7,
                PAYMENT_INTENT_INSERT,
                recorded,
                payment_intent_row(serde_json::json!("default")),
            )],
            vec![http(corr, true, vec![])],
            vec![ev],
        );
        let rows = build_ledger(&art).expect("ledger builds");
        let schema_default: Vec<_> = rows.iter().filter(|r| r.kind == "schema_default").collect();
        assert_eq!(schema_default.len(), 1, "rows: {rows:?}");
        assert!(!schema_default[0].blocking);
        assert!(
            rows.iter().all(|r| r.kind != "value_diverged"),
            "the ledger must not call blocking what the scorecard called schema-derived"
        );
    }

    /// One `payment_attempt` UPDATE per side at one span, running DIFFERENT
    /// statements: the tape's sets `status`, the candidate's sets
    /// `connector_transaction_id`.
    const ATTEMPT_UPDATE_STATUS: &str = "UPDATE \"payment_attempt\" SET \"status\" = $1 WHERE \
                                         \"attempt_id\" = $2 -- binds: [\"charged\", \"pay_1\"]";
    const ATTEMPT_UPDATE_TXN_ID: &str = "UPDATE \"payment_attempt\" SET \
                                         \"connector_transaction_id\" = $1 WHERE \"attempt_id\" = \
                                         $2 -- binds: [\"txn_9\", \"pay_1\"]";
    /// The SAME statement as `ATTEMPT_UPDATE_STATUS`, differing only in its bind
    /// values — GOTCHA #1's re-keyed write, which must still pair.
    const ATTEMPT_UPDATE_STATUS_REKEYED: &str =
        "UPDATE \"payment_attempt\" SET \"status\" = $1 WHERE \"attempt_id\" = $2 -- binds: \
         [\"refunded\", \"pay_1\"]";

    fn attempt_update_ev(
        corr: &str,
        seq: u64,
        sql: &str,
        row: serde_json::Value,
    ) -> deja::BoundaryEvent {
        let mut ev = db_update_ev(corr, "payment_attempt", seq, row, 100, 110);
        ev.method_name = "generic_update".to_owned();
        ev.args = serde_json::json!({"table": "payment_attempt", "sql": sql});
        ev
    }

    /// A re-keyed WRITE: it ran the real boundary and its args missed the
    /// recorded baseline, so it arrives unresolved and must find its twin (or
    /// not) through the args-free pairing alone.
    fn attempt_update_obs(corr: &str, sql: &str, row: serde_json::Value) -> ObservedCall {
        let mut o = exec_obs_method(
            "db",
            Some(corr),
            "generic_update",
            false,
            None,
            None,
            envelope(row),
        );
        // The hook ran the real write and had a baseline to compare against; the
        // ARGS are what missed, which is the whole premise of this pairing.
        o.seed_gap = false;
        o.args = serde_json::json!({"table": "payment_attempt", "sql": sql});
        with_span(o, "root>update_attempt")
    }

    fn one_update_each(recorded_sql: &str, observed_sql: &str) -> RunArtifacts {
        let corr = "c1";
        let recorded = serde_json::json!({"attempt_id": "pay_1", "status": "charged"});
        let observed = serde_json::json!({"attempt_id": "pay_1", "status": "refunded"});
        art_with_events(
            vec![
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_update",
                    7,
                    envelope(recorded.clone()),
                ),
                span_entry(Some(corr), 7, "root>update_attempt"),
            ],
            vec![attempt_update_obs(corr, observed_sql, observed)],
            vec![http(corr, true, vec![])],
            vec![attempt_update_ev(corr, 7, recorded_sql, recorded)],
        )
    }

    /// Two writes at one span running different statements are not one logical
    /// write, so nothing may marry them: the recorded event is an omission and
    /// the observed call is novel — on the scorecard AND in the ledger.
    ///
    /// This is run-0813's eight fabricated pairs. The scorecard had already
    /// refused them (the statement shape separates the pool); the ledger, keyed
    /// on `(correlation, boundary, method)` alone, made them anyway and shipped
    /// eight `value_diverged` rows whose two sides ran DIFFERENT SQL. What this
    /// pins is not that either half is right on its own — it is that ONE rule
    /// answers for both, so the pool can never again be narrowed on one side of
    /// the report and left wide on the other.
    #[test]
    fn a_different_statement_at_the_same_span_pairs_on_neither_side() {
        let art = one_update_each(ATTEMPT_UPDATE_STATUS, ATTEMPT_UPDATE_TXN_ID);

        let card = detect(&art);
        assert_eq!(
            card.summary.value_divergences, 0,
            "two different statements are not one write with a diverged operand"
        );
        assert_eq!(
            card.summary.novel_calls, 1,
            "the candidate's write is novel"
        );
        assert_eq!(card.summary.omitted_calls, 1, "the tape's write is omitted");

        let rows = build_ledger(&art).expect("ledger builds");
        assert!(
            rows.iter().all(|r| r.kind != "value_diverged"),
            "the ledger fabricated a pair the scorecard refused: {rows:?}"
        );
        assert_eq!(rows.iter().filter(|r| r.kind == "novel").count(), 1);
        assert_eq!(rows.iter().filter(|r| r.kind == "omitted").count(), 1);
    }

    /// The other half of the same rule, so narrowing the pool can never be
    /// mistaken for closing it: the SAME statement differing only in its bind
    /// values is GOTCHA #1's re-keyed write, and it must still collapse into ONE
    /// `value_diverged` — again on both sides of the report.
    #[test]
    fn a_rekeyed_statement_at_the_same_span_still_pairs_on_both_sides() {
        let art = one_update_each(ATTEMPT_UPDATE_STATUS, ATTEMPT_UPDATE_STATUS_REKEYED);

        let card = detect(&art);
        assert_eq!(
            card.summary.value_divergences, 1,
            "one logical write whose operand diverged"
        );
        assert_eq!(card.summary.novel_calls, 0, "not a Novel");
        assert_eq!(card.summary.omitted_calls, 0, "not an Omitted");

        let rows = build_ledger(&art).expect("ledger builds");
        let diverged: Vec<_> = rows.iter().filter(|r| r.kind == "value_diverged").collect();
        assert_eq!(diverged.len(), 1, "rows: {rows:?}");
        assert!(
            !diverged[0].origin,
            "the write is the consequence, not the cause"
        );
        assert_eq!(
            diverged[0].source_event_global_sequence,
            Some(7),
            "paired to the recorded twin at its own span"
        );
        assert!(
            rows.iter().all(|r| r.kind != "omitted"),
            "the twin is accounted for by the pair, not omitted as well"
        );
    }
}
