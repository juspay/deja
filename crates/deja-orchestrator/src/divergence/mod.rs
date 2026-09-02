//! Post-hoc divergence detector + scorecard renderer (V1 full mock).
//!
//! Consumes the replay artifacts and reconciles the orchestrator's model of
//! what SHOULD have happened (the lookup table, itself rendered from the
//! recording) with what the candidate ACTUALLY did (its `ObservedCall` stream)
//! and how its HTTP responses compared (the kernel's `HttpDiff` stream).
//! Raw record/replay execution-graph nodes are carried alongside those inputs
//! for downstream reporting but do not participate in classification.
//!
//! Classification (V1):
//!   - resolved hit                         → matched (recorded per address rank)
//!   - resolved only at rank 6 (sequence)   → Recovered (fragility flag)
//!   - candidate call with no table hit     → NovelCall (blocking)
//!     …uncorrelated (background work)      → NovelCallTolerated
//!     …on an egress boundary               → EnvironmentalMiss (tolerated)
//!     …after a truncated recording tail    → InconclusiveTailGap (inconclusive)
//!   - table entry the candidate never hit  → OmittedCall (blocking)
//!     …uncorrelated, or non-blocking       → OmittedCallTolerated
//!   - schema-derived DB/response occurrences → SchemaDefaultDivergence (tolerated)
//!   - http status / unabsorbed body diffs    → StatusMismatch / BodyMismatch
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
use std::fmt::Write as _;
use std::io::{self, BufRead};

use deja::{Address, LocalFileLookupSource, LookupTable, LookupTableSource, ObservedCall};
use deja_kernel::{HttpDiff, JsonFieldDiff};
use serde::{Deserialize, Serialize};

use crate::HarnessRoot;

pub mod ledger;
pub mod span_shape;
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
///   - ingress: the request boundary the kernel re-drives by construction,
///     not a side effect at all. Self-described by `role: "ingress"`; the
///     legacy `http_incoming` name keeps pre-`role` tapes working.
///
/// NB there is deliberately no `crypto` tier. Crypto is pure computation, not a
/// seam: its only entropy is the AEAD nonce, recorded at its own seam
/// (`common_utils::crypto::NonceSequence::new`), so AES reproduces byte-identically
/// when run live. It carries no boundary and therefore needs no exclusion — see the
/// note on `crypto_operation` in `hyperswitch_domain_models::type_encryption`.
/// Callers pass the row's `role` when they have one (events, observed calls);
/// lookup-table entries never carry ingress (the renderer skips it), so `None`
/// is correct there.
fn is_nonblocking_boundary(boundary: &str, role: Option<&str>) -> bool {
    tier_for(boundary) == Tier::Pure
        || role == Some(deja::ROLE_INGRESS)
        || boundary == "http_incoming"
}

/// Whether replay is *incapable* of producing a counterpart for this boundary,
/// however faithfully the candidate behaves.
///
/// Only `http_incoming` qualifies. The kernel re-drives the request itself, so
/// the replay hook is never asked to resolve that boundary and no `ObservedCall`
/// is ever emitted for it — see the skip in `lookup::render`. Every other
/// boundary produces an observation on replay, hit or miss.
///
/// **Do not reach for [`is_nonblocking_boundary`] here**, however close it
/// looks. That predicate answers a scoring question — may an unconsumed recorded
/// call block the verdict — and its `Tier::Pure` half covers entropy seams,
/// which are *substituted* on replay and therefore very much do have
/// counterparts. The two predicates agree on `http_incoming` and nowhere else.
/// Merging them would route every `time` / `id` / `uuid` / `rng` event into the
/// annex and strip the highest-volume events we capture out of both graphs, with
/// the accounting balancing perfectly the whole way.
fn lacks_replay_counterpart_by_construction(boundary: &str) -> bool {
    boundary == "http_incoming"
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
fn omission_is_blocking(correlation: Option<&str>, boundary: &str, role: Option<&str>) -> bool {
    correlation.is_some() && !is_nonblocking_boundary(boundary, role)
}

/// Whether an observed replay row is the ingress response-finalizer marker.
/// Mirrors [`deja::BoundaryEvent::is_ingress`]: the self-described `role` first,
/// the legacy `http_incoming` name for artifacts that predate it.
fn observed_is_ingress(obs: &ObservedCall) -> bool {
    obs.role.as_deref() == Some(deja::ROLE_INGRESS) || obs.boundary == "http_incoming"
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
    /// `omitted_calls + novel_calls + value_divergences + identity_skews`.
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
    /// Graph alignments whose serving identity disagreed with their structural
    /// counterpart. Projection of `per_boundary[*].kinds["IdentitySkew"]`.
    #[serde(default)]
    pub identity_skews: u64,
    /// Legacy persisted-scorecard field. UPDATE results now compare only columns
    /// assigned by the statement, so inherited-row ordering noise is equivalent
    /// directly and every remaining UPDATE value mismatch is blocking. New
    /// scorecards always write zero.
    #[serde(default)]
    pub order_nondeterminism_warnings: u64,
    /// Schema-derived DB and response occurrences. DB INSERT evidence is
    /// confirmed only when every differing column was filled with literal SQL
    /// `DEFAULT` by both statements. That same established provenance may
    /// absorb a response leaf with the same column name only in the same
    /// correlation. These occurrences describe an environment/schema
    /// difference, not an application divergence, so they are named here
    /// instead of `value_divergences` or `side_effect_divergences` and do not
    /// fail the verdict. UPDATE assigned-column mismatches remain strict.
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
    /// Calls that could not be conclusively classified because the RECORDING for
    /// their correlation stops at request teardown — the recorder releases the
    /// correlation at the API-lock release, so the post-response work the request
    /// path goes on to do was never captured. The candidate performs that work on
    /// replay and it necessarily misses the lookup table. Counted here rather
    /// than as `novel_calls` so a recording limit is not reported as a candidate
    /// bug, and never as a pass: the correlation's verdict becomes INCONCLUSIVE.
    ///
    /// Guarded on the correlation's HTTP response matching in BOTH status and
    /// body — a tail gap that changed the response is a divergence, not a tail
    /// gap — and on the call sitting after the reproduced teardown marker. See
    /// [`tail_gap_evidence`].
    ///
    /// Projection of `per_boundary[*].kinds["InconclusiveTailGap"]`.
    #[serde(default)]
    pub inconclusive_tail_gaps: u64,
    /// Value-divergence rows that were recognized as a narrow read/write race:
    /// HTTP-clean, same typed DB row, distinct overlapping task buckets. These are
    /// not counted as blocking side-effect divergences; the verdict is explicitly
    /// inconclusive so the orchestrator can auto-rerun instead of red-failing.
    #[serde(default)]
    pub inconclusive_races: u64,
    /// Recorded scored spans (the run's declared `scored_span_namespaces`
    /// instrumentation contract) the replay never executed. Blocking.
    ///
    /// Projection of `per_boundary["graph"].kinds["MissingScoredSpan"]`.
    #[serde(default)]
    pub missing_scored_spans: u64,
    /// Replayed scored spans the tape has no baseline for. Blocking: the tape
    /// is the contract in both directions, so an un-instrumented (older) tape
    /// fails against an instrumented candidate by design — re-record it.
    ///
    /// Projection of `per_boundary["graph"].kinds["NovelScoredSpan"]`.
    #[serde(default)]
    pub novel_scored_spans: u64,
    /// Paired scored spans whose captured field values differ (the chokepoint
    /// check: `connector`, `flow`, …). Blocking.
    ///
    /// Projection of `per_boundary["graph"].kinds["SpanFieldDiverged"]`.
    #[serde(default)]
    pub span_field_divergences: u64,
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
    pub scoring_mode: deja_forest::ScoringMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alignment: Option<deja_forest::Alignment>,
    /// The scored-span shape comparison, present only when the run declared
    /// `scored_span_namespaces` AND this correlation carries at least one
    /// namespaced span on either side — absence keeps opted-out scorecards
    /// byte-identical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub span_shape: Option<span_shape::CorrelationSpanShape>,
    /// This correlation carried at least one `InconclusiveTailGap`: its
    /// recording stops at request teardown, so the candidate's post-response
    /// work has no baseline and the correlation CANNOT be judged clean. Held
    /// apart from `passed` rather than folded into it — an unjudgeable
    /// correlation is not a failing one, and it is emphatically not a passing
    /// one. `passed` is false whenever this is true.
    #[serde(default)]
    pub inconclusive: bool,
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
        folds(
            "omitted_calls",
            s.omitted_calls,
            &["OmittedCall", "PrunedSubtree"],
        );
        folds(
            "omitted_calls_tolerated",
            s.omitted_calls_tolerated,
            &["OmittedCallTolerated"],
        );
        folds("novel_calls", s.novel_calls, &["NovelCall", "NovelSubtree"]);
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
        folds("identity_skews", s.identity_skews, &["IdentitySkew"]);
        folds(
            "inconclusive_seed_gaps",
            s.inconclusive_seed_gaps,
            &["InconclusiveSeedGap"],
        );
        folds(
            "inconclusive_tail_gaps",
            s.inconclusive_tail_gaps,
            &["InconclusiveTailGap"],
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
            &["SchemaDefaultDivergence"],
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
        folds(
            "missing_scored_spans",
            s.missing_scored_spans,
            &["MissingScoredSpan"],
        );
        folds(
            "novel_scored_spans",
            s.novel_scored_spans,
            &["NovelScoredSpan"],
        );
        folds(
            "span_field_divergences",
            s.span_field_divergences,
            &["SpanFieldDiverged"],
        );

        // The headline number: every blocking side-effect divergence, and
        // nothing else. A demotion that stopped excluding itself here would show
        // up as a verdict nobody could account for from the breakdown.
        let blocking = s.omitted_calls + s.novel_calls + s.value_divergences + s.identity_skews;
        if s.side_effect_divergences != blocking {
            out.push(format!(
                "summary.side_effect_divergences = {}, but omitted + novel + value + identity = {blocking}",
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

/// The artifact streams a run produces, loaded into memory.
pub struct RunArtifacts {
    pub run_id: String,
    pub recording_id: Option<String>,
    pub table: LookupTable,
    pub observed: Vec<ObservedCall>,
    pub http_diffs: Vec<HttpDiff>,
    /// Raw record-side execution graph. `None` means unavailable or refused;
    /// `Some(Vec::new())` means the artifact was present but empty.
    pub record_graph: Option<Vec<deja_core::ExecutionGraphNode>>,
    /// Raw replay-side execution graph, in observed-stream order.
    pub replay_graph: Vec<deja_core::ExecutionGraphNode>,
    /// The recording's semantic events (recorded side). Carried so the classifier
    /// can reason about wall-clock windows + row identity for the concurrent
    /// same-row write (order-nondeterminism) demotion. Empty when unavailable.
    pub events: Vec<deja::BoundaryEvent>,
    /// Replay scope: when the run drove a correlation SUBSET (the spec's
    /// `correlation_filter`), recorded expectations outside the subset are
    /// dropped at load — an undriven test case is excluded, never counted
    /// omitted. `None` = the full recording was driven.
    pub correlation_scope: Option<std::collections::BTreeSet<String>>,
    /// The run's declared instrumentation contract (`RunSpec::scored_span_namespaces`),
    /// read off `run.json` at load. Empty = no span-shape check.
    pub scored_span_namespaces: Vec<String>,
    /// Reply canons the run's SYSTEM declares per boundary, resolved from its
    /// declaration at load. Empty = the recorder's declaration is the whole
    /// canon, which is the behaviour for a system that declares nothing.
    pub reply_canons: std::collections::BTreeMap<String, String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone)]
struct GraphCorrelationPlan {
    pruned_record_events: HashSet<u64>,
    novel_replay_events: HashSet<usize>,
    scoring_mode: deja_forest::ScoringMode,
    alignment: Option<deja_forest::Alignment>,
    record: Option<deja_forest::ActivationForest>,
    replay: Option<deja_forest::ActivationForest>,
    flat_replay_nodes: HashSet<u64>,
    flat_record_nodes: HashSet<u64>,
    flat_record_events: HashSet<u64>,
    flat_replay_events: HashSet<usize>,
}

/// The single record/replay graph seam shared by scorecard and call-ledger
/// classification. Replay forest event sequences are stable indices into
/// `RunArtifacts::observed`; record forest event sequences are tape-global
/// boundary-event sequences.
#[derive(Debug, Clone, Default)]
pub(crate) struct GraphScoringPlan {
    correlations: BTreeMap<String, GraphCorrelationPlan>,
}

impl GraphScoringPlan {
    pub(crate) fn build(art: &RunArtifacts) -> Self {
        let mut correlation_ids = BTreeSet::new();
        correlation_ids.extend(
            art.http_diffs
                .iter()
                .map(|diff| diff.correlation_id.clone()),
        );
        correlation_ids.extend(
            art.events
                .iter()
                .filter_map(|event| event.correlation_id.clone()),
        );
        correlation_ids.extend(
            art.observed
                .iter()
                .filter_map(|observed| observed.correlation_id.clone()),
        );
        correlation_ids.extend(
            art.record_graph
                .iter()
                .flatten()
                .filter_map(|node| node.correlation_id.clone()),
        );
        correlation_ids.extend(
            art.replay_graph
                .iter()
                .filter_map(|node| node.correlation_id.clone()),
        );

        let mut correlations = BTreeMap::new();
        for correlation_id in correlation_ids {
            let record_build = art.record_graph.as_ref().map(|nodes| {
                let nodes: Vec<_> = nodes
                    .iter()
                    .filter(|node| node.correlation_id.as_deref() == Some(correlation_id.as_str()))
                    .cloned()
                    .collect();
                let events: Vec<_> = art
                    .events
                    .iter()
                    .filter(|event| {
                        event.correlation_id.as_deref() == Some(correlation_id.as_str())
                    })
                    .map(|event| deja_forest::EventRef {
                        global_sequence: event.global_sequence,
                        graph_node_id: event.graph_node_id,
                        correlation_id_present: true,
                        counterpart_possible: !lacks_replay_counterpart_by_construction(
                            &event.boundary,
                        ),
                    })
                    .collect();
                deja_forest::build(&nodes, &events).inspect(|set| {
                    set.balance(events.len() as u64)
                        .expect("record forest balance changed after construction");
                })
            });
            let replay_nodes: Vec<_> = art
                .replay_graph
                .iter()
                .filter(|node| node.correlation_id.as_deref() == Some(correlation_id.as_str()))
                .cloned()
                .collect();
            let replay_events: Vec<_> = art
                .observed
                .iter()
                .enumerate()
                .filter(|(_, observed)| {
                    observed.correlation_id.as_deref() == Some(correlation_id.as_str())
                })
                // Symmetrical with the record side on purpose. Replay never
                // emits an `http_incoming` observation at all, so this filter
                // is expected to match nothing here — but the two sides must
                // classify by the same rule, or a future boundary that does
                // reach both would be annexed on one side and attached on the
                // other, which is the asymmetry this whole change removes.
                .map(|(index, observed)| deja_forest::EventRef {
                    global_sequence: index as u64,
                    graph_node_id: observed.graph_node_id,
                    correlation_id_present: true,
                    counterpart_possible: !lacks_replay_counterpart_by_construction(
                        &observed.boundary,
                    ),
                })
                .collect();
            let replay_build = deja_forest::build(&replay_nodes, &replay_events).inspect(|set| {
                set.balance(replay_events.len() as u64)
                    .expect("replay forest balance changed after construction");
            });
            let record = record_build
                .as_ref()
                .and_then(|result| result.as_ref().ok())
                .and_then(|set| set.by_correlation.get(&correlation_id))
                .cloned();
            let replay = replay_build
                .as_ref()
                .ok()
                .and_then(|set| set.by_correlation.get(&correlation_id))
                .cloned();
            let cycle = record_build
                .as_ref()
                .and_then(|result| result.as_ref().ok())
                .is_some_and(|set| set.unusable.contains_key(&correlation_id))
                || replay_build
                    .as_ref()
                    .ok()
                    .is_some_and(|set| set.unusable.contains_key(&correlation_id));
            let reason = if cycle {
                Some(deja_forest::FlatReason::CycleDetected)
            } else if record.is_none() || replay.is_none() {
                Some(deja_forest::FlatReason::MissingForest)
            } else if event_bearing_ingress_root(
                record
                    .as_ref()
                    .expect("missing record forest handled above"),
            ) != event_bearing_ingress_root(
                replay
                    .as_ref()
                    .expect("missing replay forest handled above"),
            ) {
                Some(deja_forest::FlatReason::IngressRootAsymmetry)
            } else {
                None
            };
            let flat_record_events: HashSet<u64> = record_build
                .as_ref()
                .and_then(|result| result.as_ref().ok())
                .map(|set| {
                    // Every annexed event is scored by the flat tier — it has no
                    // node, so the graph tier has nothing to say about it. That
                    // includes the by-construction bucket: `http_incoming` is
                    // already `is_nonblocking_boundary`, so it is a tolerated
                    // omission there rather than a finding.
                    //
                    // Through `annexed_sequences` rather than a chain of its
                    // own, and this is the reason the method exists: a bucket
                    // left out here is annexed, balances, and is then scored by
                    // nobody. `build` and `balance` shout about a bucket they do
                    // not know; this path would say nothing at all.
                    set.annex.annexed_sequences().collect()
                })
                .unwrap_or_default();
            let flat_replay_events: HashSet<usize> = replay_build
                .as_ref()
                .ok()
                .map(|set| {
                    set.annex
                        .annexed_sequences()
                        .map(|index| index as usize)
                        .collect()
                })
                .unwrap_or_default();

            let (scoring_mode, alignment, flat_replay_nodes, flat_record_nodes) =
                if let Some(reason) = reason {
                    (
                        deja_forest::ScoringMode::Flat { reason },
                        None,
                        HashSet::new(),
                        HashSet::new(),
                    )
                } else {
                    let record_forest = record.as_ref().expect("graph mode has a record forest");
                    let replay_forest = replay.as_ref().expect("graph mode has a replay forest");
                    let mut alignment = deja_forest::align(record_forest, replay_forest);
                    alignment
                        .flat_tier_events
                        .extend(flat_record_events.iter().copied());
                    alignment
                        .flat_tier_events
                        .extend(flat_replay_events.iter().map(|index| *index as u64));
                    alignment.flat_tier_events.sort_unstable();
                    alignment.flat_tier_events.dedup();
                    let mut flat_replay_nodes = HashSet::new();
                    let mut flat_record_nodes = HashSet::new();
                    let mut paired_fallback_metadata = Vec::new();
                    for row in &alignment.nodes {
                        let (Some(record_node), Some(replay_node)) =
                            (row.record_node, row.replay_node)
                        else {
                            continue;
                        };
                        if record_forest.nodes[&record_node].events.len() != 1
                            || replay_forest.nodes[&replay_node].events.len() != 1
                        {
                            flat_record_nodes.insert(record_node);
                            flat_replay_nodes.insert(replay_node);
                            paired_fallback_metadata
                                .extend(record_forest.nodes[&record_node].events.iter().copied());
                            paired_fallback_metadata
                                .extend(replay_forest.nodes[&replay_node].events.iter().copied());
                        }
                    }
                    alignment.flat_tier_events.extend(paired_fallback_metadata);
                    alignment.flat_tier_events.sort_unstable();
                    alignment.flat_tier_events.dedup();
                    let served: BTreeMap<u64, u64> = replay_forest
                        .nodes
                        .values()
                        .filter(|node| !flat_replay_nodes.contains(&node.node_id))
                        .filter_map(|node| match node.events.as_slice() {
                            [index] => art
                                .observed
                                .get(*index as usize)
                                .and_then(|observed| observed.source_event_global_sequence)
                                .map(|sequence| (node.node_id, sequence)),
                            _ => None,
                        })
                        .collect();
                    deja_forest::reconcile_serving(&mut alignment, &served, record_forest);
                    let record_skeleton_nodes = record_forest
                        .nodes
                        .values()
                        .filter(|node| node.subtree_events > 0)
                        .count() as u64;
                    let replay_skeleton_nodes = replay_forest
                        .nodes
                        .values()
                        .filter(|node| node.subtree_events > 0)
                        .count() as u64;
                    alignment
                        .balance(record_skeleton_nodes, replay_skeleton_nodes)
                        .expect("graph alignment must account for both event-bearing skeletons");
                    (
                        deja_forest::ScoringMode::Graph,
                        Some(alignment),
                        flat_replay_nodes,
                        flat_record_nodes,
                    )
                };
            let mut pruned_record_events = HashSet::new();
            let mut novel_replay_events = HashSet::new();
            if let Some(alignment) = &alignment {
                for row in &alignment.nodes {
                    match &row.outcome {
                        deja_forest::NodeOutcome::PrunedSubtree { .. } => {
                            if let (Some(forest), Some(node)) = (&record, row.record_node) {
                                pruned_record_events
                                    .extend(forest_event_sequences(forest, node, true));
                            }
                        }
                        deja_forest::NodeOutcome::NovelSubtree { .. } => {
                            if let (Some(forest), Some(node)) = (&replay, row.replay_node) {
                                novel_replay_events.extend(
                                    forest_event_sequences(forest, node, true)
                                        .into_iter()
                                        .map(|index| index as usize),
                                );
                            }
                        }
                        _ => {}
                    }
                }
            }
            correlations.insert(
                correlation_id,
                GraphCorrelationPlan {
                    scoring_mode,
                    alignment,
                    record,
                    replay,
                    flat_replay_nodes,
                    flat_record_nodes,
                    flat_record_events,
                    flat_replay_events,
                    pruned_record_events,
                    novel_replay_events,
                },
            );
        }
        Self { correlations }
    }

    pub(crate) fn mode(&self, correlation_id: &str) -> Option<&deja_forest::ScoringMode> {
        self.correlations
            .get(correlation_id)
            .map(|entry| &entry.scoring_mode)
    }

    pub(crate) fn alignment(&self, correlation_id: &str) -> Option<&deja_forest::Alignment> {
        self.correlations
            .get(correlation_id)
            .and_then(|entry| entry.alignment.as_ref())
    }

    pub(crate) fn is_graph(&self, correlation_id: Option<&str>) -> bool {
        correlation_id
            .and_then(|id| self.mode(id))
            .is_some_and(|mode| matches!(mode, deja_forest::ScoringMode::Graph))
    }

    pub(crate) fn alignment_row_for_replay_node(
        &self,
        correlation_id: &str,
        replay_node: u64,
    ) -> Option<&deja_forest::AlignedNode> {
        self.alignment(correlation_id)?
            .nodes
            .iter()
            .find(|row| row.replay_node == Some(replay_node))
    }

    pub(crate) fn recorded_sequence_for_replay_node(
        &self,
        correlation_id: &str,
        replay_node: u64,
    ) -> Option<u64> {
        let entry = self.correlations.get(correlation_id)?;
        let record_node = self
            .alignment_row_for_replay_node(correlation_id, replay_node)?
            .record_node?;
        let events = &entry.record.as_ref()?.nodes.get(&record_node)?.events;
        (events.len() == 1).then(|| events[0])
    }

    pub(crate) fn replay_node_uses_flat_tier(
        &self,
        correlation_id: &str,
        replay_node: u64,
    ) -> bool {
        self.correlations
            .get(correlation_id)
            .is_some_and(|entry| entry.flat_replay_nodes.contains(&replay_node))
    }

    pub(crate) fn alignment_row_uses_flat_tier(
        &self,
        correlation_id: &str,
        row: &deja_forest::AlignedNode,
    ) -> bool {
        self.correlations.get(correlation_id).is_some_and(|entry| {
            row.record_node
                .is_some_and(|node| entry.flat_record_nodes.contains(&node))
                || row
                    .replay_node
                    .is_some_and(|node| entry.flat_replay_nodes.contains(&node))
        })
    }

    pub(crate) fn record_event_uses_flat_tier(&self, correlation_id: &str, sequence: u64) -> bool {
        self.correlations
            .get(correlation_id)
            .is_some_and(|entry| entry.flat_record_events.contains(&sequence))
    }

    fn recorded_event_is_pruned(&self, correlation_id: &str, sequence: u64) -> bool {
        self.correlations
            .get(correlation_id)
            .is_some_and(|entry| entry.pruned_record_events.contains(&sequence))
    }

    fn replay_event_is_novel(&self, correlation_id: &str, index: usize) -> bool {
        self.correlations
            .get(correlation_id)
            .is_some_and(|entry| entry.novel_replay_events.contains(&index))
    }

    pub(crate) fn replay_event_uses_flat_tier(&self, correlation_id: &str, index: usize) -> bool {
        self.correlations
            .get(correlation_id)
            .is_some_and(|entry| entry.flat_replay_events.contains(&index))
    }

    pub(crate) fn recorded_event_sequences(
        &self,
        correlation_id: &str,
        record_node: u64,
        include_subtree: bool,
    ) -> Vec<u64> {
        let Some(forest) = self
            .correlations
            .get(correlation_id)
            .and_then(|entry| entry.record.as_ref())
        else {
            return Vec::new();
        };
        forest_event_sequences(forest, record_node, include_subtree)
    }

    pub(crate) fn replay_event_indices(
        &self,
        correlation_id: &str,
        replay_node: u64,
        include_subtree: bool,
    ) -> Vec<usize> {
        let Some(forest) = self
            .correlations
            .get(correlation_id)
            .and_then(|entry| entry.replay.as_ref())
        else {
            return Vec::new();
        };
        forest_event_sequences(forest, replay_node, include_subtree)
            .into_iter()
            .map(|index| index as usize)
            .collect()
    }

    pub(crate) fn alignment_recorded_sequences(
        &self,
        correlation_id: &str,
        row: &deja_forest::AlignedNode,
    ) -> Vec<u64> {
        row.record_node
            .map(|node| {
                self.recorded_event_sequences(
                    correlation_id,
                    node,
                    matches!(&row.outcome, deja_forest::NodeOutcome::PrunedSubtree { .. }),
                )
            })
            .unwrap_or_default()
    }

    pub(crate) fn alignment_replay_indices(
        &self,
        correlation_id: &str,
        row: &deja_forest::AlignedNode,
    ) -> Vec<usize> {
        row.replay_node
            .map(|node| {
                self.replay_event_indices(
                    correlation_id,
                    node,
                    matches!(&row.outcome, deja_forest::NodeOutcome::NovelSubtree { .. }),
                )
            })
            .unwrap_or_default()
    }

    fn scored_alignment(
        &self,
        correlation_id: &str,
        divergent_nodes: Option<&HashSet<u64>>,
    ) -> Option<deja_forest::Alignment> {
        let entry = self.correlations.get(correlation_id)?;
        let mut alignment = entry.alignment.clone()?;
        let Some(divergent_nodes) = divergent_nodes else {
            return Some(alignment);
        };
        let replay = entry.replay.as_ref()?;
        for row in &mut alignment.nodes {
            let Some(replay_node) = row.replay_node else {
                continue;
            };
            if !divergent_nodes.contains(&replay_node)
                || matches!(&row.outcome, deja_forest::NodeOutcome::IdentitySkew { .. })
            {
                continue;
            }
            let mut parent = replay.nodes[&replay_node].parent_id;
            let mut origin = true;
            while let Some(parent_id) = parent {
                if divergent_nodes.contains(&parent_id) {
                    origin = false;
                    break;
                }
                parent = replay.nodes.get(&parent_id).and_then(|node| node.parent_id);
            }
            row.outcome = deja_forest::NodeOutcome::ValueDiverged { origin };
        }
        Some(alignment)
    }
}

fn event_bearing_ingress_root(forest: &deja_forest::ActivationForest) -> bool {
    forest.roots.iter().any(|root| {
        let node = &forest.nodes[root];
        // Ingress recorders open their correlated span as `deja::<boundary>`
        // (`deja::http_incoming`, `deja::grpc_incoming`, ...). The span carries
        // no role field, so the naming convention is the recognizer here.
        let is_ingress_span = node
            .span_name
            .strip_prefix("deja::")
            .is_some_and(|name| name.ends_with("_incoming"));
        is_ingress_span && node.subtree_events > 0
    })
}

fn forest_event_sequences(
    forest: &deja_forest::ActivationForest,
    root: u64,
    include_subtree: bool,
) -> Vec<u64> {
    let mut events = Vec::new();
    let mut pending = vec![root];
    while let Some(node_id) = pending.pop() {
        let node = forest
            .nodes
            .get(&node_id)
            .expect("alignment names a node in its forest");
        events.extend(node.events.iter().copied());
        if include_subtree {
            pending.extend(node.children.iter().rev().copied());
        }
    }
    events.sort_unstable();
    events
}
fn graph_identity_skew(
    plan: &GraphScoringPlan,
    observed: &ObservedCall,
) -> Option<(Option<u64>, Option<u64>)> {
    let correlation_id = observed.correlation_id.as_deref()?;
    if !plan.is_graph(Some(correlation_id)) {
        return None;
    }
    let row = plan.alignment_row_for_replay_node(correlation_id, observed.graph_node_id?)?;
    match &row.outcome {
        deja_forest::NodeOutcome::IdentitySkew {
            aligned_event,
            served_event,
        } => Some((*aligned_event, *served_event)),
        _ => None,
    }
}

/// Get-or-create a boundary's stats, stamping its tier (and an egress note) the
/// first time it is seen.
/// Whether a boundary tag is the database channel (which assigns serial PKs).
fn is_db_boundary(boundary: &str) -> bool {
    matches!(boundary, "db" | "storage")
}

fn graph_recorded_event_is_pruned(
    plan: &GraphScoringPlan,
    correlation_id: &str,
    sequence: u64,
) -> bool {
    plan.recorded_event_is_pruned(correlation_id, sequence)
}

/// Two db results are equivalent modulo replay-local DB infrastructure.
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
        let assignments = split_top_level(assignments);
        if assignments.is_empty() {
            return None;
        }
        let (mut schema_filled, mut application_filled) = (BTreeSet::new(), BTreeSet::new());
        for assignment in assignments {
            let (column, value) = split_assignment(assignment)?;
            let column = unquote_identifier(column)?;
            if value.trim().is_empty() {
                return None;
            }
            if is_default_keyword(value) {
                schema_filled.insert(column);
            } else {
                application_filled.insert(column);
            }
        }
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

/// Schema-derived columns proven by resolved INSERT divergences, keyed by
/// correlation and table. This is the pairing-shape authority: later UPDATE
/// shapes may omit only columns established by the direct INSERT evidence.
#[derive(Debug, Clone, Default)]
pub(crate) struct CorrelationColumnProvenance {
    established_schema_derived: HashMap<(String, String), BTreeSet<String>>,
}

impl CorrelationColumnProvenance {
    fn establish_schema_default(
        &mut self,
        correlation: Option<&str>,
        divergence: &SchemaDefaultDivergence,
    ) {
        let Some(correlation) = correlation else {
            return;
        };
        self.established_schema_derived
            .entry((correlation.to_owned(), divergence.table.clone()))
            .or_default()
            .extend(divergence.columns.iter().cloned());
    }

    fn is_established_schema_derived(
        &self,
        correlation: Option<&str>,
        table: &str,
        column: &str,
    ) -> bool {
        let Some(correlation) = correlation else {
            return false;
        };
        self.established_schema_derived
            .get(&(correlation.to_owned(), table.to_owned()))
            .is_some_and(|columns| columns.contains(column))
    }

    /// Whether direct, confirmed schema-default evidence established `column`
    /// on any table in this correlation. Response bodies do not carry a table,
    /// so this deliberately performs only a same-correlation, established-only
    /// lookup: it neither reclassifies SQL nor infers provenance from a name.
    fn is_established_in_correlation(&self, correlation: Option<&str>, column: &str) -> bool {
        let Some(correlation) = correlation else {
            return false;
        };
        self.established_schema_derived
            .iter()
            .any(|((established_correlation, _), columns)| {
                established_correlation == correlation && columns.contains(column)
            })
    }
}

/// Build the direct schema-default evidence used by pairing-shape normalization.
pub(crate) fn correlation_column_provenance(
    events: &[deja::BoundaryEvent],
    observed: &[ObservedCall],
) -> CorrelationColumnProvenance {
    let mut provenance = CorrelationColumnProvenance::default();
    let events_by_seq: HashMap<u64, &deja::BoundaryEvent> = events
        .iter()
        .map(|event| (event.global_sequence, event))
        .collect();
    for obs in observed {
        let event = obs
            .source_event_global_sequence
            .and_then(|seq| events_by_seq.get(&seq).copied());
        if !observed_value_diverged(obs, event) {
            continue;
        }
        if let SchemaDefaultVerdict::Confirmed(divergence) =
            observed_schema_default_divergence(obs, event)
        {
            provenance.establish_schema_default(obs.correlation_id.as_deref(), &divergence);
        }
    }
    provenance
}

/// A db divergence whose every differing column was filled by the schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaDefaultDivergence {
    table: String,
    columns: Vec<String>,
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
        "SchemaDefaultDivergence"
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
/// This classification applies only to INSERT. UPDATE returned fields assigned
/// by `SET` remain strict even when the right-hand side is `DEFAULT`; unassigned
/// returned fields were already projected out by [`update_returning_equivalent`].
pub(crate) fn schema_default_divergence(
    boundary: &str,
    recorded_sql: Option<&str>,
    observed_sql: Option<&str>,
    recorded: &serde_json::Value,
    observed: &serde_json::Value,
) -> SchemaDefaultVerdict {
    if !is_db_boundary(boundary) {
        return SchemaDefaultVerdict::No;
    }
    let Some(observed_statement) = observed_sql.and_then(parse_write_statement) else {
        return SchemaDefaultVerdict::No;
    };
    if observed_statement.kind != WriteKind::Insert {
        return SchemaDefaultVerdict::No;
    }
    let Some(differing) = db_row_column_diff(recorded, observed) else {
        return SchemaDefaultVerdict::No;
    };
    if differing.is_empty()
        || !differing
            .iter()
            .all(|column| observed_statement.schema_filled.contains(column))
    {
        return SchemaDefaultVerdict::No;
    }
    match recorded_sql.and_then(parse_write_statement) {
        Some(recorded_statement)
            if recorded_statement.kind == WriteKind::Insert
                && recorded_statement.table == observed_statement.table
                && differing
                    .iter()
                    .all(|column| recorded_statement.schema_filled.contains(column)) =>
        {
            SchemaDefaultVerdict::Confirmed(SchemaDefaultDivergence {
                table: observed_statement.table,
                columns: differing.into_iter().collect(),
            })
        }
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
) -> SchemaDefaultVerdict {
    let (Some(recorded), Some(observed)) = (&obs.recorded_result, &obs.observed_result) else {
        return SchemaDefaultVerdict::No;
    };
    schema_default_divergence(
        &obs.boundary,
        event
            .and_then(|ev| ev.args.get("sql"))
            .and_then(|s| s.as_str()),
        obs.args.get("sql").and_then(|s| s.as_str()),
        recorded,
        observed,
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
    /// `bag:` with paths — those collections carry no order; the rest of the
    /// body is compared as usual. Bare `bag` stays whole-body (`Bag`), so every
    /// string recorded before clauses existed keeps its meaning exactly.
    BagPaths(Vec<String>),
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
            Self::BagPaths(_) => "bag",
            Self::FinalState => "final_state",
            Self::AbsentAfter => "absent_after",
            Self::Project { .. } => "project",
        }
    }

    fn equivalent(&self, recorded: &serde_json::Value, observed: &serde_json::Value) -> bool {
        match self {
            Self::Sequence => recorded == observed,
            Self::Bag => bag_canon(recorded) == bag_canon(observed),
            // A per-path clause makes no whole-body claim; it is consulted per
            // difference, not here.
            Self::BagPaths(_) => false,
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

/// Every clause of a canon declaration. A declaration is one or more clauses
/// separated by `;` — `project:!created_at;bag:$.a[]`. A string with no `;`
/// is a one-clause list whose meaning is exactly what it was before clauses
/// existed, which is what keeps every recorded declaration valid.
fn canon_clauses(canon: Option<&deja::CanonRef>) -> Vec<CanonPreset> {
    let Some(id) = canon.map(|c| c.id.trim()) else {
        return Vec::new();
    };
    id.split(';')
        .map(str::trim)
        .filter(|clause| !clause.is_empty())
        .filter_map(parse_canon_clause)
        .collect()
}

fn parse_canon_clause(id: &str) -> Option<CanonPreset> {
    if let Some(raw) = id.strip_prefix("bag:") {
        let paths: Vec<String> = raw
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(canonical_set_path)
            .collect();
        // `bag:` naming nothing is the whole-body preset, not an empty clause.
        return Some(if paths.is_empty() {
            CanonPreset::Bag
        } else {
            CanonPreset::BagPaths(paths)
        });
    }
    resolve_canon_preset(id)
}

fn resolve_canon(canon: Option<&deja::CanonRef>) -> Option<CanonPreset> {
    resolve_canon_preset(canon?.id.trim())
}

fn resolve_canon_preset(id: &str) -> Option<CanonPreset> {
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
/// Compare an UPDATE's single returned row on exactly the columns named in its
/// SET list. The statement pair must describe the same table and assigned-column
/// set; unsupported SQL and non-row result shapes fail closed.
///
/// The result envelope remains strict. Only fields of the returned row that the
/// UPDATE did not assign are projected away, because those fields carry inherited
/// state and can reflect the order of concurrent writes to the same row.
fn update_returning_equivalent(
    recorded_sql: Option<&str>,
    observed_sql: Option<&str>,
    recorded: &serde_json::Value,
    observed: &serde_json::Value,
) -> bool {
    let (Some(recorded_statement), Some(observed_statement)) = (
        recorded_sql.and_then(parse_write_statement),
        observed_sql.and_then(parse_write_statement),
    ) else {
        return false;
    };
    if recorded_statement.kind != WriteKind::Update
        || observed_statement.kind != WriteKind::Update
        || recorded_statement.table != observed_statement.table
    {
        return false;
    }

    let recorded_column_count =
        recorded_statement.schema_filled.len() + recorded_statement.application_filled.len();
    let observed_column_count =
        observed_statement.schema_filled.len() + observed_statement.application_filled.len();
    if recorded_column_count != observed_column_count
        || !recorded_statement
            .schema_filled
            .iter()
            .chain(&recorded_statement.application_filled)
            .all(|column| observed_statement.writes(column))
    {
        return false;
    }

    let recorded = db_normalize_infra(recorded);
    let observed = db_normalize_infra(observed);
    let row_container_is_array =
        |result: &serde_json::Value| match result.get("value").unwrap_or(result) {
            serde_json::Value::Object(_) => Some(false),
            serde_json::Value::Array(rows) if rows.len() == 1 && rows[0].is_object() => Some(true),
            _ => None,
        };
    let (Some(recorded_is_array), Some(observed_is_array)) = (
        row_container_is_array(&recorded),
        row_container_is_array(&observed),
    ) else {
        return false;
    };
    if recorded_is_array != observed_is_array {
        return false;
    }
    let (Some(recorded_row), Some(observed_row)) =
        (db_returning_row(&recorded), db_returning_row(&observed))
    else {
        return false;
    };

    // A structured result envelope is not row state. Keep all of its metadata
    // strict while projecting only the nested `value` row.
    let envelope_equivalent = |left: &serde_json::Value, right: &serde_json::Value| match (
        left.as_object(),
        right.as_object(),
    ) {
        (Some(left), Some(right)) if left.contains_key("value") && right.contains_key("value") => {
            left.keys()
                .chain(right.keys())
                .filter(|key| key.as_str() != "value")
                .all(|key| left.get(key) == right.get(key))
        }
        (Some(left), Some(right))
            if !left.contains_key("result")
                && !right.contains_key("result")
                && !left.contains_key("value")
                && !right.contains_key("value") =>
        {
            true
        }
        _ => false,
    };
    envelope_equivalent(&recorded, &observed)
        && recorded_statement
            .schema_filled
            .iter()
            .chain(&recorded_statement.application_filled)
            .all(
                |column| match (recorded_row.get(column), observed_row.get(column)) {
                    (Some(recorded), Some(observed)) => recorded == observed,
                    _ => false,
                },
            )
}

pub(crate) fn values_diverge_under_event(
    boundary: &str,
    recorded: &serde_json::Value,
    observed: &serde_json::Value,
    event: Option<&deja::BoundaryEvent>,
    observed_sql: Option<&str>,
) -> bool {
    if let Some(canon) = event.and_then(event_value_canon) {
        if declared_value_equivalent(&canon, recorded, observed) {
            return false;
        }
    }
    if is_db_boundary(boundary)
        && (db_equiv_modulo_infra(recorded, observed)
            || update_returning_equivalent(
                event
                    .and_then(|event| event.args.get("sql"))
                    .and_then(serde_json::Value::as_str),
                observed_sql,
                recorded,
                observed,
            ))
    {
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
            (Some(recorded), Some(observed)) => values_diverge_under_event(
                &obs.boundary,
                recorded,
                observed,
                event,
                obs.args.get("sql").and_then(serde_json::Value::as_str),
            ),
            _ => false,
        }
}

fn is_unit_value(value: &serde_json::Value) -> bool {
    matches!(value, serde_json::Value::Null)
}

/// Remove UPDATE assignments whose column was already proven schema-derived in
/// this correlation. Every other byte of the statement remains identity.
///
/// Diesel renumbers later binds when an `AsChangeset` field appears or
/// disappears, so the remaining `$N` ordinals are canonicalized by first
/// occurrence. Unsupported SQL returns `None`; callers then retain the original
/// statement and pairing fails closed.
fn schema_normalized_update_shape(
    sql: &str,
    correlation: Option<&str>,
    provenance: &CorrelationColumnProvenance,
) -> Option<String> {
    let statement = sql_statement(sql).trim_start();
    let parsed = parse_write_statement(statement)?;
    if parsed.kind != WriteKind::Update {
        return None;
    }

    let (verb, rest) = split_leading_word(statement);
    if !verb.eq_ignore_ascii_case("UPDATE") {
        return None;
    }
    let (_, rest) = quoted_identifier(rest.trim_start())?;
    let (set, assignments_and_suffix) = split_leading_word(rest.trim_start());
    if !set.eq_ignore_ascii_case("SET") {
        return None;
    }
    let suffix_at = top_level_keyword(assignments_and_suffix, &["WHERE", "RETURNING"])
        .unwrap_or(assignments_and_suffix.len());
    let assignments = &assignments_and_suffix[..suffix_at];
    let suffix = &assignments_and_suffix[suffix_at..];
    let assignments_at = statement.len() - assignments_and_suffix.len();

    let mut removed = false;
    let mut kept = Vec::new();
    for assignment in split_top_level(assignments) {
        let (column, value) = split_assignment(assignment)?;
        let column = unquote_identifier(column)?;
        if value.trim().is_empty() {
            return None;
        }
        if provenance.is_established_schema_derived(correlation, &parsed.table, &column) {
            removed = true;
        } else {
            kept.push(assignment.trim());
        }
    }
    if !removed || kept.is_empty() {
        return None;
    }

    let leading_len = assignments.len() - assignments.trim_start().len();
    let trailing_at = assignments.trim_end().len();
    let mut normalized = String::with_capacity(statement.len());
    normalized.push_str(&statement[..assignments_at]);
    normalized.push_str(&assignments[..leading_len]);
    for (index, assignment) in kept.into_iter().enumerate() {
        if index != 0 {
            normalized.push_str(", ");
        }
        normalized.push_str(assignment);
    }
    normalized.push_str(&assignments[trailing_at..]);
    normalized.push_str(suffix);
    canonicalize_bind_ordinals(&normalized)
}

fn canonicalize_bind_ordinals(statement: &str) -> Option<String> {
    let bytes = statement.as_bytes();
    let mut ordinals = BTreeMap::<u32, usize>::new();
    let mut next = 1usize;
    let mut normalized = String::with_capacity(statement.len());
    let mut copied_through = 0usize;
    let mut at = 0usize;

    while at < bytes.len() {
        match bytes[at] {
            b'"' => {
                at += 1;
                loop {
                    match bytes.get(at).copied() {
                        Some(b'"') if bytes.get(at + 1) == Some(&b'"') => at += 2,
                        Some(b'"') => {
                            at += 1;
                            break;
                        }
                        Some(_) => at += 1,
                        None => return None,
                    }
                }
            }
            b'\'' => return None,
            b'$' => {
                let start = at;
                at += 1;
                let first = *bytes.get(at)?;
                if !first.is_ascii_digit() || first == b'0' {
                    return None;
                }
                let mut ordinal = 0u32;
                while let Some(digit) = bytes.get(at).filter(|digit| digit.is_ascii_digit()) {
                    ordinal = ordinal
                        .checked_mul(10)?
                        .checked_add(u32::from(*digit - b'0'))?;
                    at += 1;
                }
                let canonical = *ordinals.entry(ordinal).or_insert_with(|| {
                    let current = next;
                    next += 1;
                    current
                });
                normalized.push_str(&statement[copied_through..start]);
                write!(&mut normalized, "${canonical}").ok()?;
                copied_through = at;
            }
            _ => at += 1,
        }
    }
    normalized.push_str(&statement[copied_through..]);
    Some(normalized)
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
/// the text before that tail is exactly "the statement without its values".
/// The only exception is an UPDATE assignment for a column whose earlier value
/// divergence the existing schema-default classifier already confirmed was
/// environment-derived. That assignment is removed by
/// [`schema_normalized_update_shape`]; every unproven column remains identity.
/// A boundary with no SQL falls back to the structural skeleton of its args.
fn pairing_shape(
    args: &serde_json::Value,
    correlation: Option<&str>,
    provenance: &CorrelationColumnProvenance,
) -> String {
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
        Some(sql) => {
            let statement = schema_normalized_update_shape(sql, correlation, provenance)
                .unwrap_or_else(|| sql_statement(sql).to_owned());
            parts.push(format!("sql={statement}"));
        }
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
pub(crate) struct ArgsFreePairing<'a> {
    /// Recorded twins per identity, FIFO by source order. Known entries borrow
    /// full args solely to detect an observed occurrence arriving out of order.
    queues: BTreeMap<PairingIdentity, std::collections::VecDeque<ArgsFreeTwin<'a>>>,
    provenance: &'a CorrelationColumnProvenance,
}

struct ArgsFreeTwin<'a> {
    sequence: u64,
    args: Option<&'a serde_json::Value>,
}

pub(crate) struct ArgsFreePairingResult {
    pub(crate) sequence: u64,
    pub(crate) order_mismatch: bool,
}

impl<'a> ArgsFreePairing<'a> {
    /// Build the pool from the run's own two streams: the lookup table (which
    /// says which sequences are expected, and at which address) and the tape
    /// (which says what each call's args looked like). Both callers pass the
    /// SAME two, so neither can see a pool the other does not.
    pub(crate) fn build(
        table: &LookupTable,
        events: &'a [deja::BoundaryEvent],
        provenance: &'a CorrelationColumnProvenance,
    ) -> Self {
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
        // order and `take_twin` always pops the FIFO occurrence. Full args only
        // reveal when the observed args belong to a later live occurrence; they
        // never select that later occurrence.
        let mut queues: BTreeMap<_, std::collections::VecDeque<ArgsFreeTwin<'a>>> = BTreeMap::new();
        for (seq, entry) in &addressed {
            let (Some(boundary), Some(method)) = (&entry.boundary, &entry.method) else {
                continue;
            };
            let Some(span) = span_paths.get(seq) else {
                continue;
            };
            let event = events_by_seq.get(seq).copied();
            let shape =
                event.map(|ev| pairing_shape(&ev.args, entry.correlation.as_deref(), provenance));
            queues
                .entry((
                    entry.correlation.clone(),
                    span.clone(),
                    boundary.clone(),
                    method.clone(),
                    shape,
                ))
                .or_default()
                .push_back(ArgsFreeTwin {
                    sequence: *seq,
                    args: event.map(|ev| &ev.args),
                });
        }
        Self { queues, provenance }
    }

    /// Pop the next unclaimed recorded twin for `obs`, or `None` if this call
    /// has no twin it is entitled to. Skips any sequence a resolved
    /// (args-aligned) call already claimed, so a mixed run that resolves some
    /// calls normally and re-keys others never double-binds one recorded event.
    ///
    /// The FIFO head remains the twin even when the observed full args exactly
    /// match a later live entry. That condition is reported as an order mismatch
    /// rather than used to rematch, preserving occurrence pairing while making a
    /// same-shape swap visible. Unknown-args queues cannot report this signal.
    pub(crate) fn take_twin(
        &mut self,
        obs: &ObservedCall,
        consumed: &HashSet<u64>,
    ) -> Option<ArgsFreePairingResult> {
        let span = obs.span_path.as_deref()?;
        // Same statement first; then the shape-unknown queue, whose events carry
        // no args to compare and so pair as they always did.
        let shape = pairing_shape(&obs.args, obs.correlation_id.as_deref(), self.provenance);
        for candidate in [Some(shape.clone()), None] {
            let args_known = candidate.is_some();
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
            while queue
                .front()
                .is_some_and(|twin| consumed.contains(&twin.sequence))
            {
                queue.pop_front();
            }
            let order_mismatch = args_known
                && queue.front().is_some_and(|head| {
                    head.args != Some(&obs.args)
                        && queue.iter().skip(1).any(|later| {
                            !consumed.contains(&later.sequence) && later.args == Some(&obs.args)
                        })
                });
            if let Some(twin) = queue.pop_front() {
                return Some(ArgsFreePairingResult {
                    sequence: twin.sequence,
                    order_mismatch,
                });
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

/// The canonical order for a multiset of JSON values: by serialized form. A
/// SORT, never a dedup, so two collections agree only when their members agree
/// WITH MULTIPLICITY — losing one of two identical members is still a
/// difference.
fn sort_as_bag(items: &mut [serde_json::Value]) {
    items.sort_by_cached_key(|item| serde_json::to_string(item).unwrap_or_default());
}

fn bag_canon(value: &serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Array(items) => {
            let mut items: Vec<_> = items.iter().map(bag_canon).collect();
            sort_as_bag(&mut items);
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
        if !observed_is_ingress(obs) {
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
            if is_fork_region(obs) || observed_is_ingress(obs) || obs.timestamp_ns == 0 {
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

fn db_row_key_from_state_key(raw: &str) -> Option<DbRowKey> {
    let parsed = deja::StateKey::parse(raw).ok()?;
    let wire = parsed.to_wire();
    let table = parsed.db_table()?.to_owned();
    match parsed {
        deja::StateKey::DbRow { key, .. } => Some(DbRowKey { table, key, wire }),
        _ => None,
    }
}

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
        .filter(|ev| ev.is_ingress())
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
/// The document's clauses for one boundary, parsed. Empty when the system
/// declares none, which is every system until a deployment writes one.
fn document_clauses_for(
    reply_canons: &std::collections::BTreeMap<String, String>,
    boundary: &str,
) -> Vec<CanonPreset> {
    reply_canons
        .get(boundary)
        .map(|raw| canon_clauses(Some(&deja::CanonRef::new(raw.as_str()))))
        .unwrap_or_default()
}

/// Which source supplied the clause that governed a path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClauseSource {
    Recorder,
    Document,
    /// Both named it identically — the document entry is now redundant and can
    /// be deleted, which is how a deployment learns the vendor declaration has
    /// landed.
    Both,
}

impl ClauseSource {
    fn label(self) -> &'static str {
        match self {
            Self::Recorder => "recorder",
            Self::Document => "document",
            Self::Both => "both",
        }
    }
}

/// What the composed canon says about one difference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CanonVerdict {
    /// No clause governs it; classify as usual.
    NotGoverned,
    Absorbed(ClauseSource),
    /// Both sources named the path with DIFFERENT clauses. Never absorbed — a
    /// disagreement about what a path means must not decide that a difference
    /// does not exist.
    Conflict,
}

/// Whether a `project` clause in these excludes the path a difference sits at —
/// the clause type that can disagree with a `bag` clause about one path.
///
/// Asks the EXISTING project matcher rather than comparing normalised strings:
/// a project exclusion is written as a field name and matched by rules of its
/// own, so a second normalisation here would answer a different question from
/// the one the absorber answers.
fn project_clause_excludes(clauses: &[CanonPreset], json_path: &str) -> bool {
    clauses.iter().any(|clause| match clause {
        CanonPreset::Project { exclude, .. } => {
            http_project_excludes_json_diff_path(exclude, json_path)
        }
        _ => false,
    })
}

/// Paths a `bag` clause names as sets.
fn bag_clause_paths(clauses: &[CanonPreset]) -> Vec<String> {
    clauses
        .iter()
        .filter_map(|c| match c {
            CanonPreset::BagPaths(paths) => Some(paths.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}

/// The composed canon's verdict on one body difference: the recorder's clauses
/// and the document's clauses over one boundary, merged per PATH. The document
/// is a second contributor, not a fallback — a fallback would never be
/// consulted, because the only HTTP ingress boundary already carries a
/// declaration.
fn canon_verdict_for(
    diff: &HttpDiff,
    recorded_http: Option<&deja::BoundaryEvent>,
    body: &JsonFieldDiff,
    document_clauses: &[CanonPreset],
) -> CanonVerdict {
    let recorder_clauses: Vec<CanonPreset> = recorded_http
        .and_then(|ev| ev.declaration.as_ref())
        .map(|d| canon_clauses(d.reply_canon.as_ref()))
        .unwrap_or_default();
    if recorder_clauses.is_empty() && document_clauses.is_empty() {
        return CanonVerdict::NotGoverned;
    }
    let path = canonical_set_path(&body.json_path);
    let (rec_bags, doc_bags) = (
        bag_clause_paths(&recorder_clauses),
        bag_clause_paths(document_clauses),
    );
    let (in_rec, in_doc) = (
        rec_bags.iter().any(|p| p == &path),
        doc_bags.iter().any(|p| p == &path),
    );
    if in_rec || in_doc {
        // A path one source calls a set and the other excludes from comparison
        // entirely is two different claims about it. Report, do not absorb.
        let excluded_elsewhere = if in_doc {
            project_clause_excludes(&recorder_clauses, &body.json_path)
        } else {
            project_clause_excludes(document_clauses, &body.json_path)
        };
        if excluded_elsewhere {
            return CanonVerdict::Conflict;
        }
        // Absorb only what the existing test already proves is a permutation.
        if !is_order_only_difference(body) {
            return CanonVerdict::NotGoverned;
        }
        return CanonVerdict::Absorbed(match (in_rec, in_doc) {
            (true, true) => ClauseSource::Both,
            (true, false) => ClauseSource::Recorder,
            _ => ClauseSource::Document,
        });
    }
    // Whole-body clauses keep their existing meaning, and only the recorder can
    // state one today.
    for canon in &recorder_clauses {
        if http_diff_absorbed_by_whole_body_canon(diff, canon, body) {
            return CanonVerdict::Absorbed(ClauseSource::Recorder);
        }
    }
    CanonVerdict::NotGoverned
}

fn http_diff_absorbed_by_whole_body_canon(
    diff: &HttpDiff,
    canon: &CanonPreset,
    body: &JsonFieldDiff,
) -> bool {
    let canon = canon.clone();
    // `bag` is the generic declaration that a boundary's collections carry no
    // order, and it is the one place knowledge of a particular payload belongs:
    // stated by whoever owns the semantics, against the boundary it describes,
    // rather than compiled into the comparison. Without a declaration an
    // ordering difference is reported like any other.
    if matches!(canon, CanonPreset::Bag) {
        return match (&diff.baseline_body, &diff.candidate_body) {
            (Some(baseline), Some(candidate)) => canon.equivalent(baseline, candidate),
            _ => false,
        };
    }
    let CanonPreset::Project { include, exclude } = canon else {
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

#[derive(PartialEq, Eq)]
struct HiddenFormProjection {
    body_without_hidden_inputs: String,
    hidden_inputs_by_form: Vec<Vec<String>>,
}

/// Canonicalize only hidden-input ordering inside redirect forms. The
/// surrounding document and every byte of every input tag remain significant;
/// sorting the tag strings preserves duplicate name/value pairs.
fn hidden_form_projection(html: &str) -> Option<HiddenFormProjection> {
    let bytes = html.as_bytes();
    let mut body_without_hidden_inputs = String::with_capacity(html.len());
    let mut hidden_inputs_by_form = Vec::new();
    let mut current_form: Option<Vec<String>> = None;
    let mut found_hidden_input = false;
    let mut cursor = 0;

    while cursor < bytes.len() {
        let Some(relative_start) = bytes[cursor..].iter().position(|byte| *byte == b'<') else {
            body_without_hidden_inputs.push_str(&html[cursor..]);
            break;
        };
        let tag_start = cursor + relative_start;
        body_without_hidden_inputs.push_str(&html[cursor..tag_start]);
        let tag_end = html_tag_end(bytes, tag_start)?;
        let tag = &html[tag_start..=tag_end];

        match html_tag_name(tag) {
            Some((false, name)) if name.eq_ignore_ascii_case("form") => {
                if current_form.is_some() {
                    return None;
                }
                current_form = Some(Vec::new());
                body_without_hidden_inputs.push_str(tag);
            }
            Some((true, name)) if name.eq_ignore_ascii_case("form") => {
                let mut inputs = current_form.take()?;
                inputs.sort();
                hidden_inputs_by_form.push(inputs);
                body_without_hidden_inputs.push_str(tag);
            }
            Some((false, name))
                if current_form.is_some()
                    && (name.eq_ignore_ascii_case("script")
                        || name.eq_ignore_ascii_case("style")) =>
            {
                return None;
            }
            Some((false, name))
                if name.eq_ignore_ascii_case("input")
                    && current_form.is_some()
                    && html_attribute_equals(tag, "type", "hidden") =>
            {
                current_form
                    .as_mut()
                    .expect("form presence checked above")
                    .push(tag.to_owned());
                found_hidden_input = true;
            }
            _ => body_without_hidden_inputs.push_str(tag),
        }
        cursor = tag_end + 1;
    }

    if current_form.is_some() || !found_hidden_input {
        return None;
    }
    Some(HiddenFormProjection {
        body_without_hidden_inputs,
        hidden_inputs_by_form,
    })
}

fn html_tag_end(bytes: &[u8], tag_start: usize) -> Option<usize> {
    let mut quote = None;
    for (offset, byte) in bytes[tag_start + 1..].iter().copied().enumerate() {
        match (quote, byte) {
            (Some(active), current) if current == active => quote = None,
            (None, b'\'' | b'"') => quote = Some(byte),
            (None, b'>') => return Some(tag_start + offset + 1),
            _ => {}
        }
    }
    None
}

fn html_tag_name(tag: &str) -> Option<(bool, &str)> {
    let bytes = tag.as_bytes();
    let mut cursor = 1;
    while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
        cursor += 1;
    }
    if matches!(bytes.get(cursor), Some(b'!' | b'?')) {
        return None;
    }
    let closing = bytes.get(cursor) == Some(&b'/');
    if closing {
        cursor += 1;
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
    }
    let start = cursor;
    while bytes
        .get(cursor)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b':' | b'_'))
    {
        cursor += 1;
    }
    (cursor > start).then_some((closing, &tag[start..cursor]))
}

fn html_attribute_equals(tag: &str, wanted_name: &str, wanted_value: &str) -> bool {
    let bytes = tag.as_bytes();
    let Some((_, tag_name)) = html_tag_name(tag) else {
        return false;
    };
    let mut cursor = 1 + tag[1..].find(tag_name).unwrap_or(0) + tag_name.len();

    while cursor < bytes.len() {
        while bytes
            .get(cursor)
            .is_some_and(|byte| byte.is_ascii_whitespace() || *byte == b'/')
        {
            cursor += 1;
        }
        if bytes.get(cursor).is_none_or(|byte| *byte == b'>') {
            break;
        }
        let name_start = cursor;
        while bytes
            .get(cursor)
            .is_some_and(|byte| !byte.is_ascii_whitespace() && !matches!(byte, b'=' | b'/' | b'>'))
        {
            cursor += 1;
        }
        let name = &tag[name_start..cursor];
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        if bytes.get(cursor) != Some(&b'=') {
            continue;
        }
        cursor += 1;
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor += 1;
        }
        let (value_start, value_end) = match bytes.get(cursor).copied() {
            Some(quote @ (b'\'' | b'"')) => {
                cursor += 1;
                let start = cursor;
                while bytes.get(cursor).is_some_and(|byte| *byte != quote) {
                    cursor += 1;
                }
                let end = cursor;
                cursor += usize::from(bytes.get(cursor) == Some(&quote));
                (start, end)
            }
            Some(_) => {
                let start = cursor;
                while bytes
                    .get(cursor)
                    .is_some_and(|byte| !byte.is_ascii_whitespace() && !matches!(byte, b'/' | b'>'))
                {
                    cursor += 1;
                }
                (start, cursor)
            }
            None => return false,
        };
        if name.eq_ignore_ascii_case(wanted_name)
            && tag[value_start..value_end].eq_ignore_ascii_case(wanted_value)
        {
            return true;
        }
    }
    false
}

fn hidden_form_bodies_equivalent(diff: &HttpDiff) -> bool {
    let (Some(baseline), Some(candidate)) = (
        diff.baseline_body
            .as_ref()
            .and_then(serde_json::Value::as_str),
        diff.candidate_body
            .as_ref()
            .and_then(serde_json::Value::as_str),
    ) else {
        return false;
    };
    match (
        hidden_form_projection(baseline),
        hidden_form_projection(candidate),
    ) {
        (Some(baseline), Some(candidate)) => baseline == candidate,
        _ => false,
    }
}

/// Compare two JSON values with array order treated STRUCTURALLY rather than
/// positionally. Knows nothing about any particular schema, service or field
/// name: it is a property of comparing JSON, not of what the JSON describes.
///
/// Two rules, and both of them report rather than forgive:
///
/// - Two arrays holding the SAME members in a different order are one
///   difference, at the array's own path, instead of a difference at every
///   position the two orders happen to disagree on. Nine values in a shuffled
///   order are one fact about ordering, not seven facts about values.
/// - Two arrays whose members genuinely differ are aligned by canonical order
///   before being compared, so what surfaces is the membership difference
///   rather than the positional shift it caused downstream of it.
///
/// The point is FAITHFULNESS, not tolerance. An ordering difference is still a
/// difference and still blocks; it is merely reported once, at the level where
/// it is true, so the report says the same thing every run. A boundary whose
/// order genuinely carries no meaning says so with a `bag` reply canon, which
/// is the generic declaration for exactly that and is honoured below — schema
/// knowledge belongs in a declaration made by whoever owns the semantics, never
/// in this file.
fn order_canonical_diff(
    baseline: &serde_json::Value,
    candidate: &serde_json::Value,
    path: &str,
    out: &mut Vec<JsonFieldDiff>,
) {
    if baseline == candidate {
        return;
    }
    match (baseline, candidate) {
        (serde_json::Value::Object(b), serde_json::Value::Object(c)) => {
            let mut keys: Vec<&String> = b.keys().chain(c.keys()).collect();
            keys.sort();
            keys.dedup();
            for key in keys {
                let next = format!("{path}.{key}");
                order_canonical_diff(
                    b.get(key).unwrap_or(&serde_json::Value::Null),
                    c.get(key).unwrap_or(&serde_json::Value::Null),
                    &next,
                    out,
                );
            }
        }
        (serde_json::Value::Array(b), serde_json::Value::Array(c)) => {
            // Same members, different order. Report it HERE and do not descend:
            // the ordering is the whole of the difference, and descending would
            // re-describe it as however many positions this run's two orders
            // happened to disagree on — which is the count that will not sit
            // still between runs.
            if bag_canon(baseline) == bag_canon(candidate) {
                out.push(JsonFieldDiff {
                    json_path: path.to_owned(),
                    baseline: baseline.clone(),
                    candidate: candidate.clone(),
                });
                return;
            }
            // Members genuinely differ. Report the MULTISET DIFFERENCE — the
            // members one side has and the other does not — rather than walking
            // the two sequences in step. Members present on both sides cancel
            // however far apart they sit, so substituting one element of nine
            // is one difference and not the run of positional disagreements
            // that the substitution shifted everything else into.
            let key = |value: &serde_json::Value| {
                serde_json::to_string(&bag_canon(value)).unwrap_or_default()
            };
            let mut unmatched_candidates: BTreeMap<String, Vec<&serde_json::Value>> =
                BTreeMap::new();
            for item in c {
                unmatched_candidates
                    .entry(key(item))
                    .or_default()
                    .push(item);
            }
            let mut only_baseline: Vec<&serde_json::Value> = Vec::new();
            for item in b {
                // Cancel against an equal member wherever it sits. Popping one
                // occurrence rather than all of them is what keeps this a
                // multiset difference: a lost duplicate still surfaces.
                match unmatched_candidates.get_mut(&key(item)) {
                    Some(pool) if !pool.is_empty() => {
                        pool.pop();
                    }
                    _ => only_baseline.push(item),
                }
            }
            let mut only_candidate: Vec<&serde_json::Value> =
                unmatched_candidates.into_values().flatten().collect();
            // Both residues in canonical order, so the pairing below — and
            // every path it names — is the same on every run.
            only_baseline.sort_by_cached_key(|item| key(item));
            only_candidate.sort_by_cached_key(|item| key(item));
            for index in 0..only_baseline.len().max(only_candidate.len()) {
                // The index is into the residue, not into the original array:
                // once order carries no information, a position cannot be
                // reported as though it did.
                let next = format!("{path}[{index}]");
                order_canonical_diff(
                    only_baseline
                        .get(index)
                        .copied()
                        .unwrap_or(&serde_json::Value::Null),
                    only_candidate
                        .get(index)
                        .copied()
                        .unwrap_or(&serde_json::Value::Null),
                    &next,
                    out,
                );
            }
        }
        (b, c) => out.push(JsonFieldDiff {
            json_path: path.to_owned(),
            baseline: b.clone(),
            candidate: c.clone(),
        }),
    }
}

/// Is this row an ordering difference — two arrays with identical members in a
/// different order? Derived from the row itself, so it needs no flag threaded
/// alongside it and cannot disagree with the values it describes.
fn is_order_only_difference(row: &JsonFieldDiff) -> bool {
    matches!(
        (&row.baseline, &row.candidate),
        (serde_json::Value::Array(_), serde_json::Value::Array(_))
    ) && row.baseline != row.candidate
        && bag_canon(&row.baseline) == bag_canon(&row.candidate)
}

/// The response's body difference, recomputed with array order handled
/// structurally. `None` when the run predates full bodies being recorded
/// alongside the diff, in which case the kernel's own rows are used unchanged.
fn order_canonical_body_diff(diff: &HttpDiff) -> Option<Vec<JsonFieldDiff>> {
    let (baseline, candidate) = (diff.baseline_body.as_ref()?, diff.candidate_body.as_ref()?);
    let mut rows = Vec::new();
    order_canonical_diff(baseline, candidate, "$", &mut rows);
    Some(rows)
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct HttpBodyClassification {
    blocking_leaf_count: usize,
    schema_derived_paths: Vec<String>,
    /// Paths whose difference is a permutation and nothing else. Still counted
    /// as blocking — they are named here so the report can SAY that a
    /// difference was ordering, not so it can stop reporting it.
    order_only_paths: Vec<String>,
    /// Differences a reply-canon clause governed, with the source that supplied
    /// it. Named and NOT counted as blocking.
    canon_absorbed: Vec<(String, ClauseSource)>,
    /// Paths the two sources described differently. Named AND still blocking —
    /// a disagreement about what a path means must never decide that a
    /// difference does not exist.
    canon_conflicts: Vec<String>,
}

/// A JSON path reduced to the form a declaration is written in: every array
/// index becomes `[]`, and a trailing `[]` is dropped so that
/// `$.payment_methods_enabled` and `$.payment_methods_enabled[]` are the same
/// declaration.
///
/// Forgiving on purpose. The canonical differ names a permuted array at the
/// array's own path (`$.a`) and names a residue element positionally
/// (`$.a[0].b`), so a deployment writing the array form with `[]` and a
/// deployment writing it without would otherwise differ in whether their
/// declaration was ever read — and a declaration that is silently never read is
/// the failure this codebase keeps paying for.
fn canonical_set_path(path: &str) -> String {
    let mut out = String::with_capacity(path.len());
    let mut in_index = false;
    for ch in path.chars() {
        match ch {
            '[' => {
                in_index = true;
                out.push_str("[]");
            }
            ']' => in_index = false,
            _ if in_index => {}
            _ => out.push(ch),
        }
    }
    out.strip_suffix("[]").unwrap_or(&out).to_owned()
}

fn json_diff_leaf_field(json_path: &str) -> Option<&str> {
    let mut path = json_path.trim();
    while path.ends_with(']') {
        let index = path.rfind('[')?;
        path = &path[..index];
    }
    path.rsplit('.')
        .next()
        .map(str::trim)
        .filter(|leaf| !leaf.is_empty() && *leaf != "$")
}

/// What a body difference is judged against that is the same for every diff in
/// a run. Grouped so the next run-wide input is a field here rather than a
/// fifth parameter at four call sites.
#[derive(Clone, Copy)]
struct BodyClassificationContext<'a> {
    race: &'a InconclusiveRaceEvidence,
    provenance: &'a CorrelationColumnProvenance,
    /// Reply-canon clauses the SYSTEM DOCUMENT declares for this boundary, in
    /// the same grammar the recorder mints. A second contributor to the
    /// boundary's canon, not a fallback.
    document_clauses: &'a [CanonPreset],
}

fn classify_http_body_diff(
    diff: &HttpDiff,
    recorded_http: Option<&deja::BoundaryEvent>,
    ctx: BodyClassificationContext<'_>,
) -> HttpBodyClassification {
    let (race, provenance) = (ctx.race, ctx.provenance);
    if hidden_form_bodies_equivalent(diff) {
        return HttpBodyClassification::default();
    }
    let mut classification = HttpBodyClassification::default();
    // Ordering is resolved BEFORE anything is classified, so every absorber
    // below sees the path a difference will be reported at rather than
    // whichever positions this run's ordering scattered it across.
    let canonical = order_canonical_body_diff(diff);
    let rows = canonical.as_deref().unwrap_or(&diff.body_diff);
    for body in rows {
        // Existing explicit absorptions retain precedence over schema
        // provenance; one leaf is classified exactly once.
        match canon_verdict_for(diff, recorded_http, body, ctx.document_clauses) {
            CanonVerdict::Absorbed(source) => {
                classification
                    .canon_absorbed
                    .push((body.json_path.clone(), source));
                continue;
            }
            // Falls through to ordinary classification, so it still blocks.
            CanonVerdict::Conflict => classification.canon_conflicts.push(body.json_path.clone()),
            CanonVerdict::NotGoverned => {}
        }
        if race.http_body_diff_attributable(&diff.correlation_id, body) {
            continue;
        }
        if json_diff_leaf_field(&body.json_path).is_some_and(|column| {
            provenance.is_established_in_correlation(Some(&diff.correlation_id), column)
        }) {
            classification
                .schema_derived_paths
                .push(body.json_path.clone());
        } else {
            if is_order_only_difference(body) {
                classification.order_only_paths.push(body.json_path.clone());
            }
            classification.blocking_leaf_count += 1;
        }
    }
    classification
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

/// Key prefix of the router's per-request API lock. The lock is taken at request
/// entry and RELEASED as the last thing the request path does before the response
/// is finalized, so a `delete_key` on it is the request-teardown marker.
const API_LOCK_KEY_PREFIX: &str = "API_LOCK_";

/// Whether this recorded event is the request-teardown marker — the API-lock
/// release the router performs at response finalization.
fn is_request_teardown_marker(ev: &deja::BoundaryEvent) -> bool {
    ev.boundary == "redis"
        && ev.method_name == "delete_key"
        && ev
            .args
            .get("key")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|key| key.starts_with(API_LOCK_KEY_PREFIX))
}

/// Correlations whose RECORDING was truncated at request teardown, and the point
/// in the candidate's observed stream after which its calls therefore have no
/// recorded baseline to be judged against.
///
/// The recorder stops capturing a correlation at the API-lock release, so the
/// post-response work the request path goes on to do — persisting and notifying
/// an outgoing webhook, roughly a second later — never reaches the tape. On
/// replay the candidate does that work for real and every call in it misses the
/// lookup table. Charging those to the candidate as blocking novel calls reports
/// a RECORDING limit as a candidate bug; tolerating them silently launders a real
/// capture gap into a clean pass. Neither is true, so they are their own class:
/// INCONCLUSIVE, and never passing.
///
/// This is [`Summary::inconclusive_seed_gaps`] on a different axis — a missing
/// baseline that is neither a false match nor a false divergence — and it is
/// deliberately built the same way: named, counted, folded out of `per_boundary`,
/// and excluded from the blocking total without being excluded from the verdict.
#[derive(Debug, Clone, Default)]
pub(crate) struct TailGapEvidence {
    /// correlation -> index of the FIRST observed call past the one that
    /// reproduced that correlation's last recorded event. From here on the
    /// recording has nothing left to say about the correlation.
    tail_begins_at: HashMap<String, usize>,
}

impl TailGapEvidence {
    /// Whether an otherwise-BLOCKING novel call sits in the unrecorded tail.
    ///
    /// This positional test is the load-bearing half of the class — see the
    /// measurement on [`tail_gap_evidence`]. Callers must consult it only after
    /// every other classification has declined the call, so it can demote
    /// nothing but a would-be `NovelCall` / `NovelSubtree`.
    pub(crate) fn covers(&self, correlation: Option<&str>, observed_index: usize) -> bool {
        correlation
            .and_then(|correlation| self.tail_begins_at.get(correlation))
            .is_some_and(|tail_begins_at| observed_index >= *tail_begins_at)
    }

    /// Re-express these indices against a FILTERED observed stream, where
    /// `retained` lists — ascending — the original indices that survived.
    ///
    /// The graph-mode ledger scores flat-tier correlations through a rebuilt
    /// sub-stream, and an index means nothing outside the stream it was taken
    /// from. Handing the original indices to that sub-stream would demote
    /// whichever calls happened to land on those positions, which is the
    /// divergence-hiding failure this class exists to avoid.
    pub(crate) fn remap_to(&self, retained: &[usize]) -> TailGapEvidence {
        TailGapEvidence {
            tail_begins_at: self
                .tail_begins_at
                .iter()
                .map(|(correlation, original)| {
                    (
                        correlation.clone(),
                        retained.partition_point(|index| index < original),
                    )
                })
                .collect(),
        }
    }
}

/// Build [`TailGapEvidence`]. A correlation qualifies only when ALL of these
/// hold; each is doing work, and dropping any one of them widens this from a
/// truncation class into a divergence-hiding machine.
///
///   1. Its recording ENDS at the request-teardown marker. If the tape carries
///      post-teardown events for the correlation then the recorder did NOT stop
///      there, and a novel call is a novel call.
///   2. That last event is not the tape's last event. Per-correlation truncation
///      is the measured shape; a correlation running to the end of the tape is
///      left BLOCKING, which is the safe direction to be wrong in.
///   3. The candidate actually reproduced the teardown marker. Without an
///      observed counterpart there is no point in the stream to call the tail
///      start, and the recorded prefix was not reproduced anyway.
///   4. THE GUARD — the correlation's HTTP status AND body both matched. A tail
///      gap that changed the response is not a tail gap, it is a divergence. A
///      correlation with no HTTP diff at all fails this too: absent evidence
///      that the response matched, the calls stay blocking.
///
/// Condition (1) is near-vacuous ALONE and must never be used alone: in a
/// measured main-app run 71 of 77 correlations ended at the API-lock release, so
/// it establishes only that the recorder behaves this way generally. What
/// discriminates is the POSITIONAL test in [`TailGapEvidence::covers`] — the call
/// must come after the teardown marker was reproduced. That same run carried 16
/// BLOCKING novel `update_payment_intent` calls inside `payments_operation_core`,
/// mid-request and HTTP-clean: conditions (1)–(4) all hold for their
/// correlations, and only their POSITION keeps them blocking, which is correct.
fn tail_gap_evidence(
    events: &[deja::BoundaryEvent],
    observed: &[ObservedCall],
    http_clean_by_correlation: &HashMap<String, bool>,
) -> TailGapEvidence {
    let Some(tape_last_sequence) = events.iter().map(|ev| ev.global_sequence).max() else {
        return TailGapEvidence::default();
    };
    let mut last_recorded: HashMap<&str, &deja::BoundaryEvent> = HashMap::new();
    for ev in events {
        let Some(correlation) = ev.correlation_id.as_deref() else {
            continue;
        };
        last_recorded
            .entry(correlation)
            .and_modify(|existing| {
                if ev.global_sequence > existing.global_sequence {
                    *existing = ev;
                }
            })
            .or_insert(ev);
    }
    let mut tail_begins_at = HashMap::new();
    for (correlation, last) in last_recorded {
        if !is_request_teardown_marker(last) || last.global_sequence >= tape_last_sequence {
            continue;
        }
        if http_clean_by_correlation.get(correlation) != Some(&true) {
            continue;
        }
        // The observed call that reproduced the teardown marker. Take the LAST
        // such index: a duplicate resolution must not drag the tail start
        // earlier and widen the class.
        let Some(index) = observed
            .iter()
            .enumerate()
            .filter(|(_, obs)| {
                obs.correlation_id.as_deref() == Some(correlation)
                    && obs.source_event_global_sequence == Some(last.global_sequence)
            })
            .map(|(index, _)| index)
            .max()
        else {
            continue;
        };
        tail_begins_at.insert(correlation.to_owned(), index + 1);
    }
    TailGapEvidence { tail_begins_at }
}

/// Per-correlation HTTP cleanliness: status matched AND no blocking body leaf.
/// A correlation carrying several responses is clean only if every one of them
/// is, mirroring the `corr_http` fold the per-correlation outcomes use.
fn http_clean_by_correlation(
    http_diffs: &[HttpDiff],
    http_incoming_by_correlation: &HashMap<String, &deja::BoundaryEvent>,
    inconclusive_race: &InconclusiveRaceEvidence,
    column_provenance: &CorrelationColumnProvenance,
    document_clauses: &[CanonPreset],
) -> HashMap<String, bool> {
    let mut clean_by_correlation: HashMap<String, bool> = HashMap::new();
    for diff in http_diffs {
        let clean = diff.status_match
            && classify_http_body_diff(
                diff,
                http_incoming_by_correlation
                    .get(&diff.correlation_id)
                    .copied(),
                BodyClassificationContext {
                    race: inconclusive_race,
                    provenance: column_provenance,
                    document_clauses,
                },
            )
            .blocking_leaf_count
                == 0;
        clean_by_correlation
            .entry(diff.correlation_id.clone())
            .and_modify(|existing| *existing &= clean)
            .or_insert(clean);
    }
    clean_by_correlation
}

pub fn detect(art: &RunArtifacts) -> Scorecard {
    let graph_plan = GraphScoringPlan::build(art);
    detect_with_plan(art, &graph_plan)
}

pub(crate) fn detect_with_plan(art: &RunArtifacts, graph_plan: &GraphScoringPlan) -> Scorecard {
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
    // Direct, confirmed INSERT schema-default evidence normalizes later UPDATE
    // pairing shapes and can absorb same-correlation response leaves. It never
    // classifies inherited returned-row state or infers provenance.
    let column_provenance = correlation_column_provenance(&art.events, &art.observed);
    let mut recorded_pairing = ArgsFreePairing::build(&art.table, &art.events, &column_provenance);
    let http_incoming_by_correlation = http_incoming_events_by_correlation(&art.events);

    let mut value_divergences = 0u64;
    let mut graph_value_nodes: BTreeMap<String, HashSet<u64>> = BTreeMap::new();
    let mut identity_skews = 0u64;
    let order_nondeterminism_warnings = 0u64;
    let mut idempotent_delete_warnings = 0u64;
    // Schema-derived divergences, counted by the column they name so that
    // fifteen inserts disagreeing about one column default read as one fact —
    // and, beside them, the ones we could not confirm because the recorded
    // statement was missing, so an empty class says which cause applies.
    let mut schema_default_divergences = 0u64;
    let mut schema_default_columns_seen: BTreeMap<String, u64> = BTreeMap::new();
    let mut schema_default_unconfirmed = 0u64;
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
            classify_http_body_diff(
                diff,
                http_incoming_by_correlation
                    .get(&diff.correlation_id)
                    .copied(),
                BodyClassificationContext {
                    race: &inconclusive_race,
                    provenance: &column_provenance,
                    document_clauses: &document_clauses_for(&art.reply_canons, "http_incoming"),
                },
            )
            .blocking_leaf_count
                > 0
        })
        .count();
    let http_clean = http_status_clean && blocking_http_body_mismatches == 0;
    // Rule B: idempotent redis DELETE (recorded KeyDeleted vs observed KeyNotDeleted).
    let idempotent_delete_demote =
        idempotent_delete_demotions(&art.events, &art.observed, http_clean);
    let undeclared_concurrency = undeclared_concurrency_warnings(&art.observed);
    let undeclared_concurrency_warnings = undeclared_concurrency.len() as u64;
    // Truncated-recording tails. Built BEFORE classification because its guard is
    // a per-correlation HTTP verdict and the novel arms below need it in hand;
    // the per-correlation outcomes recompute the same cleanliness downstream from
    // the same two predicates, so the guard and the reported outcome agree.
    let tail_gap = tail_gap_evidence(
        &art.events,
        &art.observed,
        &http_clean_by_correlation(
            &art.http_diffs,
            &http_incoming_by_correlation,
            &inconclusive_race,
            &column_provenance,
            &document_clauses_for(&art.reply_canons, "http_incoming"),
        ),
    );
    let mut tail_gap_correlations: BTreeSet<String> = BTreeSet::new();
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
    let mut deferred: Vec<(usize, &ObservedCall)> = Vec::new();
    for (observed_index, obs) in art.observed.iter().enumerate() {
        if observed_is_ingress(obs) {
            continue;
        }
        if obs.correlation_id.as_deref().is_some_and(|correlation_id| {
            graph_plan.replay_event_is_novel(correlation_id, observed_index)
        }) {
            let stats = boundary_entry(&mut per_boundary, &obs.boundary);
            if tier_for(&obs.boundary) == Tier::Environmental {
                stats.bump_kind("EnvironmentalMiss");
                environmental_misses += 1;
            } else if is_nonblocking_boundary(&obs.boundary, obs.role.as_deref()) {
                stats.bump_kind("DeterministicMiss");
            } else if tail_gap.covers(obs.correlation_id.as_deref(), observed_index) {
                // The recording for this correlation stopped at request teardown
                // and this call comes after it. There is no baseline to judge it
                // against, so it is neither novel nor matched — see TailGapEvidence.
                stats.bump_kind("InconclusiveTailGap");
                if let Some(correlation_id) = &obs.correlation_id {
                    tail_gap_correlations.insert(correlation_id.clone());
                }
            } else {
                stats.bump_kind("NovelSubtree");
                blocking_side_effect += 1;
                if let Some(correlation_id) = &obs.correlation_id {
                    *corr_side_effect.entry(correlation_id.clone()).or_insert(0) += 1;
                }
            }
            continue;
        }
        if let Some((aligned_event, served_event)) = graph_identity_skew(graph_plan, obs) {
            let stats = boundary_entry(&mut per_boundary, &obs.boundary);
            stats.bump_kind("IdentitySkew");
            identity_skews += 1;
            blocking_side_effect += 1;
            if let Some(correlation_id) = &obs.correlation_id {
                *corr_side_effect.entry(correlation_id.clone()).or_insert(0) += 1;
            }
            consumed.extend(aligned_event);
            consumed.extend(served_event);
            continue;
        }
        if !obs.resolved {
            deferred.push((observed_index, obs));
            continue;
        }
        let stats = boundary_entry(&mut per_boundary, &obs.boundary);
        {
            // The recorded baseline was found (args still aligned). Under lookup
            // mode observed_result == recorded_result (substituted) so this is a
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
                if let (Some(correlation_id), Some(node_id)) =
                    (obs.correlation_id.as_ref(), obs.graph_node_id)
                {
                    if graph_plan.is_graph(Some(correlation_id))
                        && !graph_plan.replay_node_uses_flat_tier(correlation_id, node_id)
                    {
                        graph_value_nodes
                            .entry(correlation_id.clone())
                            .or_default()
                            .insert(node_id);
                    }
                }
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
                ) {
                    SchemaDefaultVerdict::Confirmed(schema_default) => {
                        stats.bump_kind(schema_default.kind());
                        schema_default_divergences += 1;
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
                if let Some(seq) = obs.source_event_global_sequence {
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
    for (observed_index, obs) in deferred {
        if let Some((aligned_event, served_event)) = graph_identity_skew(graph_plan, obs) {
            let stats = boundary_entry(&mut per_boundary, &obs.boundary);
            stats.bump_kind("IdentitySkew");
            identity_skews += 1;
            blocking_side_effect += 1;
            if let Some(correlation_id) = &obs.correlation_id {
                *corr_side_effect.entry(correlation_id.clone()).or_insert(0) += 1;
            }
            paired_consumed.extend(aligned_event);
            paired_consumed.extend(served_event);
            continue;
        }
        let graph_mode = graph_plan.is_graph(obs.correlation_id.as_deref())
            && obs.correlation_id.as_deref().is_some_and(|correlation_id| {
                !graph_plan.replay_event_uses_flat_tier(correlation_id, observed_index)
                    && obs.graph_node_id.is_some_and(|node_id| {
                        !graph_plan.replay_node_uses_flat_tier(correlation_id, node_id)
                    })
            });
        let paired_twin = if graph_mode {
            obs.correlation_id
                .as_deref()
                .zip(obs.graph_node_id)
                .and_then(|(correlation_id, node_id)| {
                    graph_plan.recorded_sequence_for_replay_node(correlation_id, node_id)
                })
                .map(|sequence| ArgsFreePairingResult {
                    sequence,
                    order_mismatch: false,
                })
        } else {
            recorded_pairing.take_twin(obs, &consumed)
        }
        .map(|twin| {
            let result = expected
                .get(&twin.sequence)
                .map(|exp| exp.result.clone())
                .unwrap_or(serde_json::Value::Null);
            (twin.sequence, twin.order_mismatch, result)
        });
        let stats = boundary_entry(&mut per_boundary, &obs.boundary);
        if tier_for(&obs.boundary) == Tier::Environmental {
            stats.bump_kind("EnvironmentalMiss");
            environmental_misses += 1;
        } else if is_nonblocking_boundary(&obs.boundary, obs.role.as_deref()) {
            // Deterministic-live (crypto/time/id/rng) or the request boundary
            // (ingress) — not a real divergence. See is_nonblocking_boundary.
            stats.bump_kind("DeterministicMiss");
        } else if obs.correlation_id.is_none() && uncorrelated_tolerated {
            // Background-task call with no correlation — tolerated in V1. Named
            // apart from the blocking `NovelCall` because it is a different
            // thing, not a different count of the same thing.
            stats.bump_kind("NovelCallTolerated");
        } else if let Some((twin_seq, order_mismatch, recorded)) = paired_twin {
            // GOTCHA #1 resolution: this unresolved observed call pairs args-free
            // (correlation+boundary+method, FIFO occurrence) with a recorded twin
            // that the candidate "omitted" because its args were re-keyed. The
            // recorded WRITE (would-be Omitted) and the execute WRITE (would-be
            // Novel) are ONE logical write — classify it once.
            let twin_event = events_by_seq.get(&twin_seq).copied();
            let (recorded_val, observed_val) =
                args_free_effective_values(&recorded, obs, twin_event);
            let value_diverged = order_mismatch
                || values_diverge_under_event(
                    &obs.boundary,
                    &recorded_val,
                    &observed_val,
                    twin_event,
                    obs.args.get("sql").and_then(serde_json::Value::as_str),
                );
            if value_diverged {
                if let (Some(correlation_id), Some(node_id)) =
                    (obs.correlation_id.as_ref(), obs.graph_node_id)
                {
                    if graph_plan.is_graph(Some(correlation_id))
                        && !graph_plan.replay_node_uses_flat_tier(correlation_id, node_id)
                    {
                        graph_value_nodes
                            .entry(correlation_id.clone())
                            .or_default()
                            .insert(node_id);
                    }
                }
            }
            // An exact later-args match is direct order evidence. Result/schema
            // equivalence and race demotion must not absorb that blocking signal.
            let schema_default = if value_diverged && !order_mismatch {
                schema_default_divergence(
                    &obs.boundary,
                    twin_event
                        .and_then(|ev| ev.args.get("sql"))
                        .and_then(|s| s.as_str()),
                    obs.args.get("sql").and_then(|s| s.as_str()),
                    &recorded_val,
                    &observed_val,
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
                // same non-blocking class.
                stats.bump_kind(schema_default.kind());
                schema_default_divergences += 1;
                *schema_default_columns_seen
                    .entry(schema_default.label())
                    .or_insert(0) += 1;
            } else if value_diverged {
                if !order_mismatch
                    && inconclusive_race
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
        } else if tail_gap.covers(obs.correlation_id.as_deref(), observed_index) {
            // Last resort before charging the candidate: the recording for this
            // correlation stopped at request teardown and this call comes after
            // it, so there is no baseline to judge it against. Reached only once
            // every other arm has declined the call, which is what keeps the
            // demotion to would-be NovelCalls alone — see TailGapEvidence.
            stats.bump_kind("InconclusiveTailGap");
            if let Some(corr) = &obs.correlation_id {
                tail_gap_correlations.insert(corr.clone());
            }
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
        let pruned = exp.correlation.as_deref().is_some_and(|correlation_id| {
            graph_recorded_event_is_pruned(graph_plan, correlation_id, *seq)
        });
        if !pruned && (consumed.contains(seq) || paired_consumed.contains(seq)) {
            continue;
        }
        let boundary = exp.boundary.clone().unwrap_or_else(|| "unknown".to_owned());
        // One classification, named for what it counts. Lumping the tolerated
        // omissions — uncorrelated background work, and non-blocking boundaries
        // — under the same name as the blocking ones is what let this table and
        // the summary give a report two answers for one set of calls.
        let blocking = omission_is_blocking(exp.correlation.as_deref(), &boundary, None);
        let stats = boundary_entry(&mut per_boundary, &boundary);
        stats.bump_kind(if blocking && pruned {
            "PrunedSubtree"
        } else if blocking {
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
    let omitted_calls =
        kind_total(&per_boundary, "OmittedCall") + kind_total(&per_boundary, "PrunedSubtree");
    let omitted_calls_tolerated = kind_total(&per_boundary, "OmittedCallTolerated");
    let novel_calls =
        kind_total(&per_boundary, "NovelCall") + kind_total(&per_boundary, "NovelSubtree");
    let novel_calls_tolerated = kind_total(&per_boundary, "NovelCallTolerated");
    let inconclusive_tail_gaps = kind_total(&per_boundary, "InconclusiveTailGap");

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
    let mut schema_default_response_paths_seen: BTreeMap<String, u64> = BTreeMap::new();
    // Response paths whose difference was a permutation and nothing else.
    // Counted as divergences like any other body difference; named separately
    // so the report can say WHAT KIND of difference it was.
    let mut order_only_response_paths_seen: BTreeMap<String, u64> = BTreeMap::new();
    // Differences a reply-canon clause governed, keyed by path and the source
    // that supplied the clause. A deployment reads this to see that its
    // document entry is doing something — and, once the source reads `both`,
    // that the entry is redundant and can go.
    let mut canon_absorbed_seen: BTreeMap<(String, &'static str), u64> = BTreeMap::new();
    let mut canon_conflict_paths_seen: BTreeMap<String, u64> = BTreeMap::new();
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
            let body_classification = classify_http_body_diff(
                diff,
                recorded_http,
                BodyClassificationContext {
                    race: &inconclusive_race,
                    provenance: &column_provenance,
                    document_clauses: &document_clauses_for(&art.reply_canons, "http_incoming"),
                },
            );
            let blocking_body_diffs = body_classification.blocking_leaf_count;
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
            for path in body_classification.schema_derived_paths {
                stats.bump_kind("SchemaDefaultDivergence");
                schema_default_divergences += 1;
                *schema_default_response_paths_seen.entry(path).or_insert(0) += 1;
            }
            for path in body_classification.order_only_paths {
                *order_only_response_paths_seen.entry(path).or_insert(0) += 1;
            }
            for (path, source) in body_classification.canon_absorbed {
                stats.bump_kind("ReplyCanonAbsorbed");
                *canon_absorbed_seen
                    .entry((path, source.label()))
                    .or_insert(0) += 1;
            }
            for path in body_classification.canon_conflicts {
                *canon_conflict_paths_seen.entry(path).or_insert(0) += 1;
            }
            let slot = corr_http
                .entry(diff.correlation_id.clone())
                .or_insert((true, true));
            slot.0 &= diff.status_match;
            slot.1 &= blocking_body_diffs == 0;
        }
    }

    // --- span-shape check (the declared instrumentation contract) ------------
    // Runs off the RAW graphs, not the forest: the forest's skeleton prune is
    // exactly the blind spot this closes (see `span_shape` module doc). Off
    // unless the run declared namespaces, so opted-out scorecards stay
    // byte-identical.
    let mut missing_scored_spans = 0u64;
    let mut novel_scored_spans = 0u64;
    let mut span_field_divergences = 0u64;
    let mut warnings_extra: Vec<String> = Vec::new();
    let mut span_shapes: BTreeMap<String, span_shape::CorrelationSpanShape> = BTreeMap::new();
    if !art.scored_span_namespaces.is_empty() {
        match art.record_graph.as_ref() {
            None => warnings_extra
                .push("scored-span shape check skipped: record graph unavailable".to_owned()),
            Some(record_graph) => {
                for corr in corr_http.keys() {
                    let rec: Vec<&deja_core::ExecutionGraphNode> = record_graph
                        .iter()
                        .filter(|n| n.correlation_id.as_deref() == Some(corr.as_str()))
                        .collect();
                    let rep: Vec<&deja_core::ExecutionGraphNode> = art
                        .replay_graph
                        .iter()
                        .filter(|n| n.correlation_id.as_deref() == Some(corr.as_str()))
                        .collect();
                    if let Some(shape) =
                        span_shape::compare(&rec, &rep, &art.scored_span_namespaces)
                    {
                        missing_scored_spans += shape.missing;
                        novel_scored_spans += shape.novel;
                        span_field_divergences += shape.field_diverged;
                        let stats = boundary_entry(&mut per_boundary, "graph");
                        if stats.note.is_none() {
                            stats.note = Some(
                                "scored-span shape: the run's declared instrumentation \
                                 contract, not a call boundary"
                                    .to_owned(),
                            );
                        }
                        for _ in 0..shape.missing {
                            stats.bump_kind("MissingScoredSpan");
                        }
                        for _ in 0..shape.novel {
                            stats.bump_kind("NovelScoredSpan");
                        }
                        for _ in 0..shape.field_diverged {
                            stats.bump_kind("SpanFieldDiverged");
                        }
                        // Matched spans ride a kind DIRECTLY: `bump_kind` would
                        // count them as diverged, and `stats.matched` feeds the
                        // `matched_side_effect_calls` fold, which counts CALLS —
                        // a matched span is neither.
                        if shape.matched > 0 {
                            *stats
                                .kinds
                                .entry("MatchedScoredSpan".to_owned())
                                .or_insert(0) += shape.matched;
                        }
                        span_shapes.insert(corr.clone(), shape);
                    }
                }
            }
        }
    }

    // --- per-correlation outcomes --------------------------------------------
    let mut per_correlation = Vec::new();
    let mut matched_correlations = 0u64;
    for (corr, (status_match, body_match)) in &corr_http {
        let side_effect_divergences = corr_side_effect.get(corr).copied().unwrap_or(0);
        let span_shape_clean = span_shapes.get(corr).is_none_or(|shape| shape.clean());
        // A tail gap costs the correlation its pass without charging it a
        // divergence. Demoting those calls out of `corr_side_effect` and stopping
        // there would have handed this correlation a clean `passed` — the
        // silent-absorption failure — so the unjudgeable state is carried
        // explicitly instead.
        let inconclusive = tail_gap_correlations.contains(corr);
        let passed = *status_match
            && *body_match
            && side_effect_divergences == 0
            && span_shape_clean
            && !inconclusive;
        if passed {
            matched_correlations += 1;
        }
        per_correlation.push(CorrelationOutcome {
            correlation_id: corr.clone(),
            http_status_match: *status_match,
            http_body_match: *body_match,
            side_effect_divergences,
            scoring_mode: graph_plan.mode(corr).cloned().unwrap_or(
                deja_forest::ScoringMode::Flat {
                    reason: deja_forest::FlatReason::MissingForest,
                },
            ),
            alignment: graph_plan.scored_alignment(corr, graph_value_nodes.get(corr)),
            span_shape: span_shapes.remove(corr),
            inconclusive,
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
    if identity_skews > 0 {
        reasons.push(format!("{identity_skews} graph identity skew(s)"));
    }
    // Span-shape findings are all BLOCKING: the tape's scored spans are the
    // candidate's declared contract, in both directions — a replay of a tape
    // recorded before the candidate was instrumented fails as novel, by design.
    if missing_scored_spans > 0 {
        reasons.push(format!("{missing_scored_spans} missing scored span(s)"));
    }
    if novel_scored_spans > 0 {
        reasons.push(format!(
            "{novel_scored_spans} novel scored span(s) — tape predates the declared instrumentation?"
        ));
    }
    if span_field_divergences > 0 {
        reasons.push(format!(
            "{span_field_divergences} scored-span field divergence(s)"
        ));
    }
    // Seed gaps are reported but do NOT by themselves fail the verdict — a
    // missing baseline is inconclusive, not a divergence.
    if inconclusive_seed_gaps > 0 {
        reasons.push(format!(
            "{inconclusive_seed_gaps} inconclusive seed gap(s) (non-blocking)"
        ));
    }
    // A truncated recording tail is reported and does NOT fail the verdict — but
    // it does not pass either, so unlike a seed gap it forces `inconclusive`.
    if inconclusive_tail_gaps > 0 {
        reasons.push(format!(
            "{inconclusive_tail_gaps} inconclusive tail-gap call(s) across \
             {} correlation(s): recording truncated at request teardown",
            tail_gap_correlations.len()
        ));
    }
    if inconclusive_races > 0 {
        reasons.push(format!(
            "{inconclusive_races} inconclusive_race row(s) recognized; auto-rerun recommended"
        ));
    }
    if idempotent_delete_warnings > 0 {
        reasons.push(format!(
            "{idempotent_delete_warnings} idempotent-delete warning(s) (non-blocking)"
        ));
    }
    // Rule C: confirmed schema provenance describes the databases and response
    // values derived from them, not the candidate. Reported, non-blocking.
    if schema_default_divergences > 0 {
        reasons.push(format!(
            "{schema_default_divergences} schema-derived DB/response occurrence(s) (non-blocking)"
        ));
    }
    if undeclared_concurrency_warnings > 0 {
        reasons.push(format!(
            "{undeclared_concurrency_warnings} undeclared_concurrency warning(s) (non-blocking)"
        ));
    }
    // Seed-gap + race + idempotent-delete + schema-derived +
    // undeclared_concurrency lines are informational, not divergences the
    // candidate caused; exclude them from the blocking count so a run whose
    // only "reasons" are those still avoids a blocking failure (race becomes an
    // explicit inconclusive verdict).
    let blocking_reasons = reasons.len()
        - usize::from(inconclusive_seed_gaps > 0)
        - usize::from(inconclusive_tail_gaps > 0)
        - usize::from(inconclusive_races > 0)
        - usize::from(idempotent_delete_warnings > 0)
        - usize::from(schema_default_divergences > 0)
        - usize::from(undeclared_concurrency_warnings > 0);
    // A tail gap joins a race in forcing INCONCLUSIVE rather than merely
    // declining to block: the candidate's post-response work went unrecorded, so
    // a run carrying one has not been shown to be clean and must never report a
    // pass. A seed gap deliberately does not do this — its missing baseline is a
    // single call's, not a whole correlation's unjudged tail.
    let inconclusive = nothing
        || ((inconclusive_races > 0 || inconclusive_tail_gaps > 0) && blocking_reasons == 0);
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
    warnings.extend(warnings_extra);
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
    // Response evidence is grouped by its original JSON path so absorption
    // remains visible without mutating the kernel's raw HttpDiff evidence.
    for (path, n) in &schema_default_response_paths_seen {
        warnings.push(format!(
            "{n} response body leaf occurrence(s) at {path} classified as schema-derived from \
             confirmed same-correlation column provenance — the candidate did not cause this"
        ));
    }
    // Say which differences were ordering. They are counted as divergences
    // above like any other body difference; what this adds is WHICH KIND they
    // are, so a reader can see that a collection came back with the same
    // members in a different order and decide whether that boundary should
    // carry a `bag:` clause naming it — a judgement about the payload, which
    // belongs to whoever owns it and not to the comparison.
    for (path, responses) in &order_only_response_paths_seen {
        warnings.push(format!(
            "response body path {path} holds the same members in a different order on \
             {responses} response(s) — the collections are equal as multisets, so the difference \
             is ordering alone. It is reported once, at the collection, rather than at each \
             position the two orders disagree on, and it still blocks: add a \
             `bag:{path}` clause to that boundary's reply canon if its order genuinely carries \
             no meaning. Name the path — a bare `bag` is the WHOLE body, which would absorb \
             every other collection in the reply and would replace the boundary's existing \
             clause rather than join it"
        ));
    }
    // What the canon absorbed, and which source said so. Named rather than
    // subtracted: a difference that stopped counting is still a difference that
    // happened, and a reader has to be able to see which declaration decided it
    // did not matter.
    for ((path, source), responses) in &canon_absorbed_seen {
        warnings.push(format!(
            "response body path {path} differed by ordering alone on {responses} response(s) and \
             was absorbed by a `bag` reply-canon clause declared by the {source}; the members are \
             identical as a multiset, so any added, removed or altered member would still block"
        ));
    }
    // Two sources describing one path differently. Absorbed by neither, on
    // purpose: a disagreement about what a path MEANS must not be resolved into
    // a decision that a difference does not exist.
    for (path, responses) in &canon_conflict_paths_seen {
        warnings.push(format!(
            "response body path {path} is described differently by the recorder's declaration and \
             by this deployment's document on {responses} response(s) — one calls it a set, the \
             other excludes it from comparison. Neither was applied and the difference still \
             blocks; make the two declarations agree"
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
            identity_skews,
            order_nondeterminism_warnings,
            schema_default_divergences,
            idempotent_delete_warnings,
            undeclared_concurrency_warnings,
            inconclusive_seed_gaps,
            inconclusive_tail_gaps,
            inconclusive_races,
            missing_scored_spans,
            novel_scored_spans,
            span_field_divergences,
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

/// Load a run's artifact streams off disk. Missing files are treated as empty
/// (or unavailable for the optional record graph); parse failures are surfaced
/// as `warnings` rather than silently dropped, so corruption cannot masquerade
/// as a clean run.
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
    let scored_span_namespaces = run
        .as_ref()
        .map(|run| run.spec.scored_span_namespaces.clone())
        .unwrap_or_default();
    // The system's own declaration, not the run's: whether an array is a set is
    // a property of the recorded system's contract, so it is read from the
    // system rather than restated per run.
    let reply_canons = run
        .as_ref()
        .map(|run| crate::system::system_config(run.spec.system()).reply_canons)
        .unwrap_or_default();

    let mut warnings = Vec::new();
    let mut table = load_table(&root.lookup_table_path(run_id), &mut warnings);
    let (observed, mut replay_graph) =
        load_replay_stream(&root.observed_path(run_id), &mut warnings);
    let mut record_graph = load_record_graph(&root.record_graph_path(run_id), &mut warnings);
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
        replay_graph.retain(|node| scope.contains(node.correlation_id.as_deref()));
        if let Some(nodes) = &mut record_graph {
            nodes.retain(|node| scope.contains(node.correlation_id.as_deref()));
        }
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
        record_graph,
        replay_graph,
        events,
        correlation_scope,
        scored_span_namespaces,
        reply_canons,
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
    let graph_plan = GraphScoringPlan::build(&art);
    let card = detect_with_plan(&art, &graph_plan);
    let path = root
        .root
        .join("runs")
        .join(format!("{run_id}.scorecard.json"));
    crate::write_json(&path, &card)?;

    // Ledger: the per-call detail the scorecard summary drops. Best-effort.
    match build_ledger_with_plan(&art, &graph_plan) {
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
    let graph_plan = GraphScoringPlan::build(art);
    build_ledger_with_plan(art, &graph_plan)
}

pub(crate) fn build_ledger_with_plan(
    art: &RunArtifacts,
    graph_plan: &GraphScoringPlan,
) -> io::Result<Vec<CallRecord>> {
    let events = &art.events;
    let span_paths = ledger::recorded_span_paths(&art.table);
    // Mirror scorecard classification: discover race evidence under status-clean
    // HTTP first, then treat only unattributable body diffs as blocking.
    let http_status_clean =
        !art.http_diffs.is_empty() && art.http_diffs.iter().all(|d| d.status_match);
    let http_incoming_by_correlation = http_incoming_events_by_correlation(events);
    let inconclusive_race =
        inconclusive_race_evidence(events, &art.observed, http_status_clean, &span_paths);
    let column_provenance = correlation_column_provenance(events, &art.observed);
    let blocking_http_body_mismatches = art
        .http_diffs
        .iter()
        .filter(|diff| {
            classify_http_body_diff(
                diff,
                http_incoming_by_correlation
                    .get(&diff.correlation_id)
                    .copied(),
                BodyClassificationContext {
                    race: &inconclusive_race,
                    provenance: &column_provenance,
                    document_clauses: &document_clauses_for(&art.reply_canons, "http_incoming"),
                },
            )
            .blocking_leaf_count
                > 0
        })
        .count();
    let http_clean = http_status_clean && blocking_http_body_mismatches == 0;
    let idempotent_delete = idempotent_delete_demotions(events, &art.observed, http_clean);
    // The SAME evidence the scorecard classifies with, from the same streams, so
    // a tail-gap row and a tail-gap count cannot come apart.
    let tail_gap = tail_gap_evidence(
        events,
        &art.observed,
        &http_clean_by_correlation(
            &art.http_diffs,
            &http_incoming_by_correlation,
            &inconclusive_race,
            &column_provenance,
            &document_clauses_for(&art.reply_canons, "http_incoming"),
        ),
    );
    Ok(ledger::build_with_plan(
        events,
        &art.observed,
        &art.table,
        &idempotent_delete,
        &inconclusive_race,
        &tail_gap,
        graph_plan,
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

/// Stream a tagged JSONL file, routing each parsed record before reading the
/// next line. Returns whether the file was present and completely readable.
fn stream_deja_records(
    path: &std::path::Path,
    warnings: &mut Vec<String>,
    mut route: impl FnMut(deja::DejaRecord),
) -> bool {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return false,
        Err(e) => {
            warnings.push(format!("read {} failed: {e}", path.display()));
            return false;
        }
    };
    for (i, line) in io::BufReader::new(file).lines().enumerate() {
        let line = match line {
            Ok(line) => line,
            Err(e) => {
                warnings.push(format!("read {} failed: {e}", path.display()));
                return false;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<deja::DejaRecord>(&line) {
            Ok(record) => route(record),
            Err(e) => warnings.push(format!("{}:{}: parse error: {e}", path.display(), i + 1)),
        }
    }
    true
}

/// Split the replay's shared tagged stream in one pass.
fn load_replay_stream(
    path: &std::path::Path,
    warnings: &mut Vec<String>,
) -> (Vec<ObservedCall>, Vec<deja_core::ExecutionGraphNode>) {
    let mut observed = Vec::new();
    let mut graph = Vec::new();
    if !stream_deja_records(path, warnings, |record| match record {
        deja::DejaRecord::Observed(call) => observed.push(*call),
        deja::DejaRecord::GraphNode(node) => graph.push(*node),
        deja::DejaRecord::BoundaryEvent(_) => {}
    }) {
        // Match the previous all-or-nothing read: a truncated stream cannot
        // masquerade as a complete, smaller replay.
        observed.clear();
        graph.clear();
    }
    (observed, graph)
}

/// Load the optional record-side tagged graph stream. Missing or unreadable is
/// unavailable (`None`); a readable empty artifact remains `Some(empty)`.
fn load_record_graph(
    path: &std::path::Path,
    warnings: &mut Vec<String>,
) -> Option<Vec<deja_core::ExecutionGraphNode>> {
    let mut graph = Vec::new();
    stream_deja_records(path, warnings, |record| {
        if let deja::DejaRecord::GraphNode(node) = record {
            graph.push(*node);
        }
    })
    .then_some(graph)
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

    #[test]
    fn update_returning_ignores_only_unassigned_row_columns() {
        let sql = "update \"payment_attempt\" set \"status\" = $1, \
                   \"modified_at\" = now() where \"attempt_id\" = $2 returning * \
                   -- binds: [Charged, \"a_1\"]";
        let recorded = serde_json::json!({
            "result": "Ok",
            "type_name": "PaymentAttempt",
            "value": [{
                "attempt_id": "a_1",
                "status": "Charged",
                "modified_at": "2026-08-16T10:00:00Z",
                "connector_transaction_id": "txn_recorded"
            }]
        });
        let observed = serde_json::json!({
            "result": "Ok",
            "type_name": "PaymentAttempt",
            "value": [{
                "attempt_id": "a_1",
                "status": "Charged",
                "modified_at": "2026-08-16T10:00:00Z",
                "connector_transaction_id": "txn_from_racing_update"
            }]
        });
        let mut event = db_update_ev(
            "corr",
            "payment_attempt",
            1,
            serde_json::json!({"attempt_id": "a_1"}),
            0,
            1,
        );
        event.args = serde_json::json!({"table": "payment_attempt", "sql": sql});

        assert!(
            !values_diverge_under_event("db", &recorded, &observed, Some(&event), Some(sql)),
            "the scorer must tolerate a whole-row mismatch inherited from an unassigned column"
        );
        assert!(
            !update_returning_equivalent(
                Some("UPDATE payment_attempt SET status = $1 RETURNING *"),
                Some("UPDATE payment_attempt SET status = $1 RETURNING *"),
                &recorded,
                &observed,
            ),
            "unsupported unquoted identifiers must fail closed"
        );
        assert!(
            !update_returning_equivalent(
                Some(
                    "UPDATE \"payment_attempt\" SET \"status\" = $1, malformed \
                     WHERE \"attempt_id\" = $2 RETURNING *",
                ),
                Some(
                    "UPDATE \"payment_attempt\" SET \"status\" = $1, malformed \
                     WHERE \"attempt_id\" = $2 RETURNING *",
                ),
                &recorded,
                &observed,
            ),
            "a partially parsed SET list must fail closed instead of projecting on a subset"
        );
    }

    #[test]
    fn update_returning_keeps_assigned_columns_strict() {
        let sql = "UPDATE \"payment_attempt\" SET \"status\" = $1, \
                   \"modified_at\" = now() WHERE \"attempt_id\" = $2 RETURNING * \
                   -- binds: [Charged, \"a_1\"]";
        let recorded = serde_json::json!({
            "result": "Ok",
            "type_name": "PaymentAttempt",
            "value": {
                "attempt_id": "a_1",
                "status": "Charged",
                "modified_at": "2026-08-16T10:00:00Z"
            }
        });
        let observed = serde_json::json!({
            "result": "Ok",
            "type_name": "PaymentAttempt",
            "value": {
                "attempt_id": "a_1",
                "status": "Pending",
                "modified_at": "2026-08-16T10:00:00Z"
            }
        });
        let mut event = db_update_ev(
            "corr",
            "payment_attempt",
            1,
            serde_json::json!({"attempt_id": "a_1"}),
            0,
            1,
        );
        event.args = serde_json::json!({"table": "payment_attempt", "sql": sql});

        assert!(
            values_diverge_under_event("db", &recorded, &observed, Some(&event), Some(sql)),
            "the scorer must keep a mismatch in quoted SET column status divergent"
        );
        let default_sql = "UPDATE \"payment_attempt\" SET \"status\" = DEFAULT, \
                           \"modified_at\" = now() WHERE \"attempt_id\" = $1 RETURNING * \
                           -- binds: [\"a_1\"]";
        assert_eq!(
            schema_default_divergence(
                "db",
                Some(default_sql),
                Some(default_sql),
                &recorded,
                &observed,
            ),
            SchemaDefaultVerdict::No,
            "SET column mismatches stay strict even when the assignment uses DEFAULT"
        );
        let missing_assigned = serde_json::json!({
            "result": "Ok",
            "type_name": "PaymentAttempt",
            "value": {
                "attempt_id": "a_1",
                "status": "Charged"
            }
        });
        assert!(
            !update_returning_equivalent(
                Some(sql),
                Some(sql),
                &missing_assigned,
                &missing_assigned,
            ),
            "projection requires every assigned column on both returned rows"
        );
        assert!(
            !update_returning_equivalent(
                Some(sql),
                Some(
                    "UPDATE \"payment_attempt\" SET \"connector_transaction_id\" = $1 \
                     WHERE \"attempt_id\" = $2 RETURNING * -- binds: [\"txn\", \"a_1\"]",
                ),
                &recorded,
                &recorded,
            ),
            "different assigned-column sets are not result-equivalent"
        );
        let array_container = serde_json::json!({
            "result": "Ok",
            "type_name": "PaymentAttempt",
            "value": [{
                "attempt_id": "a_1",
                "status": "Charged",
                "modified_at": "2026-08-16T10:00:00Z"
            }]
        });
        assert!(
            !update_returning_equivalent(Some(sql), Some(sql), &recorded, &array_container,),
            "object and single-element-array row containers are not equivalent"
        );
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
            role: None,
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
                    scored_span_namespaces: Vec::new(),
                    mode: crate::RunMode::Replay,
                    system_under_test: None,
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
                    scored_span_namespaces: Vec::new(),
                    mode: crate::RunMode::Replay,
                    system_under_test: None,
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

    fn graph_node(node_id: u64, correlation_id: Option<&str>) -> deja_core::ExecutionGraphNode {
        deja_core::ExecutionGraphNode {
            node_id,
            global_sequence: node_id,
            parent_id: None,
            causal_parent_ids: Vec::new(),
            sequence: node_id,
            correlation_id: correlation_id.map(str::to_owned),
            recording_run_id: Some("rec-graph".to_owned()),
            span_name: format!("span-{node_id}"),
            target: "test".to_owned(),
            level: "INFO".to_owned(),
            fields: BTreeMap::new(),
            started_ns: node_id * 10,
            closed_ns: Some(node_id * 10 + 1),
        }
    }

    fn write_graph_test_run(root: &HarnessRoot, run_id: &str, filter: Option<Vec<String>>) {
        crate::write_json(
            &root.run_path(run_id),
            &crate::Run {
                run_id: run_id.to_owned(),
                spec: crate::RunSpec {
                    scored_span_namespaces: Vec::new(),
                    mode: crate::RunMode::Replay,
                    system_under_test: None,
                    candidate_spec: crate::CandidateSpec::PrebuiltImage {
                        image: "deja-demo".to_owned(),
                    },
                    candidate_repo: None,
                    recording_id: None,
                    s3_source: None,
                    correlation_filter: filter,
                    workload: serde_json::Value::Null,
                },
                status: crate::RunStatus::Completed,
                recording_id: None,
                candidate_image: None,
                failure_reason: None,
                stage: None,
                step: 0,
                steps_total: 0,
                stage_updated_ms: 0,
            },
        )
        .unwrap();
    }

    #[test]
    fn load_artifacts_carries_both_execution_graph_streams() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let run_id = "run-both-graphs";
        let replay_rows = vec![
            deja::DejaRecord::GraphNode(Box::new(graph_node(11, Some("c1")))),
            deja::DejaRecord::Observed(Box::new(obs("db", Some("c1"), true, Some(3), Some(1)))),
            deja::DejaRecord::GraphNode(Box::new(graph_node(12, Some("c1")))),
        ];
        let record_rows = vec![
            deja::DejaRecord::GraphNode(Box::new(graph_node(1, Some("c1")))),
            deja::DejaRecord::GraphNode(Box::new(graph_node(2, Some("c1")))),
            deja::DejaRecord::GraphNode(Box::new(graph_node(3, Some("c1")))),
        ];
        write_jsonl_rows(&root.observed_path(run_id), &replay_rows);
        write_jsonl_rows(&root.record_graph_path(run_id), &record_rows);

        let art = load_artifacts(&root, run_id).unwrap();
        assert_eq!(art.observed.len(), 1);
        assert_eq!(art.replay_graph.len(), 2);
        assert_eq!(art.record_graph.as_ref().unwrap().len(), 3);
    }

    #[test]
    fn unavailable_record_graph_preserves_detection_and_distinguishes_present_empty() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let run_id = "run-record-graph-absent";
        write_graph_test_run(&root, run_id, None);
        write_jsonl_rows::<deja::DejaRecord>(&root.record_graph_path(run_id), &[]);

        let present = load_artifacts(&root, run_id).unwrap();
        assert_eq!(present.record_graph, Some(Vec::new()));
        let present_card = detect(&present);

        std::fs::remove_file(root.record_graph_path(run_id)).unwrap();
        let absent = load_artifacts(&root, run_id).unwrap();
        assert!(absent.record_graph.is_none());
        assert!(!absent
            .warnings
            .iter()
            .any(|warning| warning.contains("record graph refused")));
        let absent_card = detect(&absent);
        assert_eq!(
            serde_json::to_value(&absent_card.summary).unwrap(),
            serde_json::to_value(&present_card.summary).unwrap()
        );
        assert_eq!(
            serde_json::to_value(&absent_card.verdict).unwrap(),
            serde_json::to_value(&present_card.verdict).unwrap()
        );

        std::fs::write(
            root.record_graph_note_path(run_id),
            "record graph refused: incomplete correlation coverage\n",
        )
        .unwrap();
        let refused = load_artifacts(&root, run_id).unwrap();
        assert!(refused.record_graph.is_none());
        assert!(refused
            .warnings
            .iter()
            .any(|warning| warning == "record graph refused: incomplete correlation coverage"));

        std::fs::create_dir(root.record_graph_path(run_id)).unwrap();
        let unreadable = load_artifacts(&root, run_id).unwrap();
        assert!(unreadable.record_graph.is_none());
        assert!(unreadable
            .warnings
            .iter()
            .any(|warning| { warning.contains("read") && warning.contains("record-graph.jsonl") }));
    }

    #[test]
    fn graph_streams_follow_filtered_run_scope_and_keep_ambient_nodes() {
        let dir = tempfile::tempdir().unwrap();
        let root = HarnessRoot::new(dir.path()).unwrap();
        let run_id = "run-graph-scope";
        write_graph_test_run(&root, run_id, Some(vec!["c-keep".to_owned()]));
        let rows = |offset| {
            vec![
                deja::DejaRecord::GraphNode(Box::new(graph_node(offset, Some("c-keep")))),
                deja::DejaRecord::GraphNode(Box::new(graph_node(offset + 1, Some("c-drop")))),
                deja::DejaRecord::GraphNode(Box::new(graph_node(offset + 2, None))),
            ]
        };
        write_jsonl_rows(&root.record_graph_path(run_id), &rows(1));
        write_jsonl_rows(&root.observed_path(run_id), &rows(11));

        let art = load_artifacts(&root, run_id).unwrap();
        let record = art.record_graph.unwrap();
        assert_eq!(
            record.iter().map(|node| node.node_id).collect::<Vec<_>>(),
            [1, 3]
        );
        assert_eq!(
            art.replay_graph
                .iter()
                .map(|node| node.node_id)
                .collect::<Vec<_>>(),
            [11, 13]
        );
        assert!(record.iter().any(|node| node.correlation_id.is_none()));
        assert!(art
            .replay_graph
            .iter()
            .any(|node| node.correlation_id.is_none()));
    }

    #[test]
    fn interleaved_replay_records_split_in_source_order_in_one_pass() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("observed.jsonl");
        let rows = vec![
            deja::DejaRecord::Observed(Box::new(obs("first", Some("c1"), true, None, Some(1)))),
            deja::DejaRecord::GraphNode(Box::new(graph_node(21, Some("c1")))),
            deja::DejaRecord::Observed(Box::new(obs("second", Some("c1"), true, None, Some(2)))),
            deja::DejaRecord::GraphNode(Box::new(graph_node(22, Some("c1")))),
        ];
        write_jsonl_rows(&path, &rows);
        let mut warnings = Vec::new();

        let (observed, graph) = load_replay_stream(&path, &mut warnings);
        assert!(warnings.is_empty());
        assert_eq!(
            observed
                .iter()
                .map(|call| call.boundary.as_str())
                .collect::<Vec<_>>(),
            ["first", "second"]
        );
        assert_eq!(
            graph.iter().map(|node| node.node_id).collect::<Vec<_>>(),
            [21, 22]
        );
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
            scored_span_namespaces: Vec::new(),
            reply_canons: Default::default(),
            run_id: "run-1".to_owned(),
            recording_id: Some("rec-1".to_owned()),
            table: LookupTable {
                recording_id: "rec-1".to_owned(),
                policy_version: 1,
                entries,
            },
            observed,
            http_diffs: http,
            record_graph: None,
            replay_graph: Vec::new(),
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

    fn scorer_html_diff(baseline: &str, candidate: &str) -> HttpDiff {
        let baseline = serde_json::Value::String(baseline.to_owned());
        let candidate = serde_json::Value::String(candidate.to_owned());
        http_with_bodies(
            "redirect-form",
            true,
            vec![JsonFieldDiff {
                json_path: "$".to_owned(),
                baseline: baseline.clone(),
                candidate: candidate.clone(),
            }],
            baseline,
            candidate,
        )
    }

    const REDIRECT_FORM: &str = r#"<!DOCTYPE html><html><body><form method="post" action="https://psp/pay"><input type="hidden" name="amount" value="100"><input type="hidden" name="currency" value="USD"><input type="hidden" name="merchant" value="shop"></form></body></html>"#;

    #[test]
    fn scorer_forgives_reordered_hidden_form_inputs() {
        let candidate = r#"<!DOCTYPE html><html><body><form method="post" action="https://psp/pay"><input type="hidden" name="merchant" value="shop"><input type="hidden" name="amount" value="100"><input type="hidden" name="currency" value="USD"></form></body></html>"#;
        let diff = scorer_html_diff(REDIRECT_FORM, candidate);
        assert_eq!(
            diff.body_diff.len(),
            1,
            "raw HttpDiff evidence must remain intact"
        );
        assert_eq!(
            classify_http_body_diff(
                &diff,
                None,
                BodyClassificationContext {
                    race: &InconclusiveRaceEvidence::default(),
                    provenance: &CorrelationColumnProvenance::default(),
                    document_clauses: &[],
                },
            )
            .blocking_leaf_count,
            0
        );
    }

    #[test]
    fn scorer_keeps_changed_hidden_form_value_blocking() {
        let candidate = r#"<!DOCTYPE html><html><body><form method="post" action="https://psp/pay"><input type="hidden" name="currency" value="EUR"><input type="hidden" name="merchant" value="shop"><input type="hidden" name="amount" value="100"></form></body></html>"#;
        let diff = scorer_html_diff(REDIRECT_FORM, candidate);
        assert_eq!(
            classify_http_body_diff(
                &diff,
                None,
                BodyClassificationContext {
                    race: &InconclusiveRaceEvidence::default(),
                    provenance: &CorrelationColumnProvenance::default(),
                    document_clauses: &[],
                },
            )
            .blocking_leaf_count,
            1
        );
    }

    #[test]
    fn scorer_keeps_missing_hidden_form_input_blocking() {
        let candidate = r#"<!DOCTYPE html><html><body><form method="post" action="https://psp/pay"><input type="hidden" name="merchant" value="shop"><input type="hidden" name="amount" value="100"></form></body></html>"#;
        let diff = scorer_html_diff(REDIRECT_FORM, candidate);
        assert_eq!(
            classify_http_body_diff(
                &diff,
                None,
                BodyClassificationContext {
                    race: &InconclusiveRaceEvidence::default(),
                    provenance: &CorrelationColumnProvenance::default(),
                    document_clauses: &[],
                },
            )
            .blocking_leaf_count,
            1
        );
    }

    #[test]
    fn scorer_keeps_extra_hidden_form_input_blocking() {
        let candidate = r#"<!DOCTYPE html><html><body><form method="post" action="https://psp/pay"><input type="hidden" name="merchant" value="shop"><input type="hidden" name="signature" value="abc"><input type="hidden" name="amount" value="100"><input type="hidden" name="currency" value="USD"></form></body></html>"#;
        let diff = scorer_html_diff(REDIRECT_FORM, candidate);
        assert_eq!(
            classify_http_body_diff(
                &diff,
                None,
                BodyClassificationContext {
                    race: &InconclusiveRaceEvidence::default(),
                    provenance: &CorrelationColumnProvenance::default(),
                    document_clauses: &[],
                },
            )
            .blocking_leaf_count,
            1
        );
    }

    #[test]
    fn scorer_does_not_collapse_duplicate_hidden_form_inputs() {
        let baseline = r#"<!DOCTYPE html><html><body><form method="post" action="https://psp/pay"><input type="hidden" name="item" value="book"><input type="hidden" name="item" value="book"></form></body></html>"#;
        let candidate = r#"<!DOCTYPE html><html><body><form method="post" action="https://psp/pay"><input type="hidden" name="item" value="book"></form></body></html>"#;
        let diff = scorer_html_diff(baseline, candidate);
        assert_eq!(
            classify_http_body_diff(
                &diff,
                None,
                BodyClassificationContext {
                    race: &InconclusiveRaceEvidence::default(),
                    provenance: &CorrelationColumnProvenance::default(),
                    document_clauses: &[],
                },
            )
            .blocking_leaf_count,
            1
        );
    }

    // --- array order is compared structurally, not positionally -------------
    //
    // Field names below are arbitrary on purpose. Nothing in the comparison
    // knows what a payload means; a test that only passed for one service's
    // schema would be testing the wrong thing.

    /// A real `/payments/{id}/client` response pair from the failing
    /// same-image run, reduced to the subtree every difference sits under. The
    /// other top-level keys were byte-identical on both sides — they
    /// contributed nothing to the comparison and are where the merchant data
    /// lived, so they are not carried into the tree.
    fn run_fixture(raw: &str) -> HttpDiff {
        let v: serde_json::Value = serde_json::from_str(raw).expect("fixture parses");
        let baseline = v["baseline_body"].clone();
        let candidate = v["candidate_body"].clone();
        let body: Vec<JsonFieldDiff> =
            serde_json::from_value(v["body_diff"].clone()).expect("kernel rows parse");
        http_with_bodies("order", true, body, baseline, candidate)
    }

    /// Acceptance against the run this change exists for.
    ///
    /// Stated honestly: in THIS run whole-body `bag` would also have absorbed
    /// these, because no response mixed a permutation with a real difference —
    /// measured, 26 permutation-only bodies and 30 `business_label`-only, none
    /// carrying both. Per-path is not justified by this run's contents. It is
    /// justified by the two things that hold regardless: the router's one
    /// ingress boundary has a single canon slot already holding
    /// `project:!created_at,!last_synced,!modified_at`, so whole-body `bag`
    /// cannot be declared there without giving up the timestamp exclusion; and
    /// whole-body `bag` absorbs EVERY array in the body including ones whose
    /// order carries meaning, which asserts nothing and is unbounded, where a
    /// path list asserts exactly what it names.
    #[test]
    fn the_runs_own_permutations_are_absorbed_by_the_declared_paths() {
        let declared =
            clauses("bag:$.payment_methods_enabled[],$.payment_methods_enabled[].card_networks[]");
        for (name, raw) in [
            (
                "outer array permuted",
                include_str!("fixtures/payment_methods_permuted_outer.json"),
            ),
            (
                "inner card_networks permuted",
                include_str!("fixtures/payment_methods_permuted_inner.json"),
            ),
        ] {
            let diff = run_fixture(raw);
            assert!(
                !diff.body_diff.is_empty(),
                "{name}: the kernel really did report differences"
            );
            let c = classify_with_sources(&diff, None, &declared);
            assert_eq!(c.blocking_leaf_count, 0, "{name} must not block");
            // The canonical differ reports a permuted collection ONCE, at the
            // collection, rather than at each position the two orders disagree
            // on — so the kernel's dozen positional rows become one absorbed
            // path, which is the count that stays still between runs.
            assert_eq!(
                c.canon_absorbed,
                vec![(
                    "$.payment_methods_enabled".to_owned(),
                    ClauseSource::Document
                )],
                "{name}"
            );
            assert!(c.order_only_paths.is_empty(), "{name}");
        }
    }

    fn clauses(declaration: &str) -> Vec<CanonPreset> {
        canon_clauses(Some(&deja::CanonRef::new(declaration)))
    }

    /// A recorded ingress carrying a reply-canon declaration, the way the
    /// router's middleware mints one.
    fn ingress_declaring(declaration: &str) -> deja::BoundaryEvent {
        let mut ev: deja::BoundaryEvent = serde_json::from_value(serde_json::json!({
            "global_sequence": 1,
            "request_sequence": 0,
            "correlation_id": "order",
            "timestamp_ns": 0,
            "boundary": "http_incoming",
            "trait_name": "RequestIdMiddleware",
            "method_name": "call",
            "call_file": "request_id.rs",
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
        .expect("valid BoundaryEvent");
        ev.declaration = Some(
            deja::BoundaryDeclaration::default().reply_canon(deja::CanonRef::new(declaration)),
        );
        ev
    }

    fn classify_with_sources(
        diff: &HttpDiff,
        recorder: Option<&deja::BoundaryEvent>,
        document: &[CanonPreset],
    ) -> HttpBodyClassification {
        classify_http_body_diff(
            diff,
            recorder,
            BodyClassificationContext {
                race: &InconclusiveRaceEvidence::default(),
                provenance: &CorrelationColumnProvenance::default(),
                document_clauses: document,
            },
        )
    }

    /// Every declaration written before clauses existed parses to a one-clause
    /// list meaning exactly what it meant. The first string is the one the
    /// router actually mints today.
    #[test]
    fn declarations_written_before_clauses_keep_their_meaning() {
        for existing in [
            "project:!created_at,!last_synced,!modified_at",
            "bag",
            "sequence",
            "final_state",
        ] {
            let one = resolve_canon(Some(&deja::CanonRef::new(existing)))
                .expect("the preset still resolves");
            assert_eq!(
                clauses(existing),
                vec![one],
                "{existing} must parse to exactly its old meaning"
            );
        }
    }

    #[test]
    fn a_declared_set_absorbs_a_permutation_and_names_its_source() {
        let diff = body_pair(
            serde_json::json!({"a": [{"x": 1}, {"x": 2}]}),
            serde_json::json!({"a": [{"x": 2}, {"x": 1}]}),
        );
        let c = classify_with_sources(&diff, None, &clauses("bag:$.a[]"));
        assert_eq!(c.blocking_leaf_count, 0, "a permutation of a declared set");
        assert_eq!(
            c.canon_absorbed,
            vec![("$.a".to_owned(), ClauseSource::Document)]
        );
        // This is the case that was dead under a fallback rule: the document
        // governs even though the recorder declared nothing.
        assert!(c.order_only_paths.is_empty());
    }

    #[test]
    fn a_changed_member_at_a_declared_set_still_blocks() {
        for candidate in [
            serde_json::json!({"a": [1, 3]}),    // altered
            serde_json::json!({"a": [1, 2, 3]}), // added
            serde_json::json!({"a": [1]}),       // removed
        ] {
            let diff = body_pair(serde_json::json!({"a": [1, 2]}), candidate.clone());
            let c = classify_with_sources(&diff, None, &clauses("bag:$.a[]"));
            assert!(
                c.blocking_leaf_count > 0,
                "{candidate} is not a permutation and must still block"
            );
            assert!(c.canon_absorbed.is_empty());
        }
    }

    #[test]
    fn an_undeclared_permutation_still_blocks_and_is_still_named() {
        let diff = body_pair(
            serde_json::json!({"a": [1, 2]}),
            serde_json::json!({"a": [2, 1]}),
        );
        let c = classify_with_sources(&diff, None, &[]);
        assert_eq!(c.blocking_leaf_count, 1);
        assert_eq!(c.order_only_paths, vec!["$.a"]);
        assert!(c.canon_absorbed.is_empty());
    }

    /// The per-path point: whole-body `bag` absorbs nothing here, because the
    /// bodies are not bag-equal. A clause naming one path absorbs that path and
    /// leaves the real difference beside it blocking.
    #[test]
    fn a_permutation_is_absorbed_while_a_real_difference_beside_it_still_blocks() {
        let baseline = serde_json::json!({"a": [1, 2], "label": "old"});
        let candidate = serde_json::json!({"a": [2, 1], "label": "new"});
        let whole_body = classify_with_sources(
            &body_pair(baseline.clone(), candidate.clone()),
            None,
            &clauses("bag"),
        );
        assert!(
            whole_body.canon_absorbed.is_empty(),
            "whole-body bag cannot help a body that also differs for real"
        );
        let per_path =
            classify_with_sources(&body_pair(baseline, candidate), None, &clauses("bag:$.a[]"));
        assert_eq!(per_path.canon_absorbed.len(), 1, "the permutation");
        assert_eq!(per_path.blocking_leaf_count, 1, "the label still blocks");
    }

    #[test]
    fn both_sources_naming_one_path_is_redundant_and_names_both() {
        let diff = body_pair(
            serde_json::json!({"a": [1, 2]}),
            serde_json::json!({"a": [2, 1]}),
        );
        let c = classify_with_sources(
            &diff,
            Some(&ingress_declaring("bag:$.a[]")),
            &clauses("bag:$.a[]"),
        );
        assert_eq!(
            c.canon_absorbed,
            vec![("$.a".to_owned(), ClauseSource::Both)],
            "reported as redundant so the document line can be deleted"
        );
        assert_eq!(c.blocking_leaf_count, 0);
    }

    #[test]
    fn sources_describing_one_path_differently_conflict_and_absorb_nothing() {
        let diff = body_pair(
            serde_json::json!({"a": [1, 2]}),
            serde_json::json!({"a": [2, 1]}),
        );
        let c = classify_with_sources(
            &diff,
            Some(&ingress_declaring("project:!a")),
            &clauses("bag:$.a[]"),
        );
        assert_eq!(c.canon_conflicts, vec!["$.a"]);
        assert!(c.canon_absorbed.is_empty(), "a conflict absorbs nothing");
        assert!(
            c.blocking_leaf_count > 0,
            "and the difference still blocks — a disagreement must not decide it away"
        );
    }

    fn body_pair(baseline: serde_json::Value, candidate: serde_json::Value) -> HttpDiff {
        // Built the way the pipeline builds it: the kernel's own positional
        // diff, so these tests are fed the rows the kernel really emits.
        let body_diff = deja_kernel::diff_json(&baseline, &candidate, "$", &[]);
        http_with_bodies("order", true, body_diff, baseline, candidate)
    }

    fn classify_body(diff: &HttpDiff) -> HttpBodyClassification {
        classify_body_declaring(diff, &[])
    }

    /// The same classification, for a system that declares these paths as sets.
    fn classify_body_declaring(
        diff: &HttpDiff,
        document: &[CanonPreset],
    ) -> HttpBodyClassification {
        classify_http_body_diff(
            diff,
            None,
            BodyClassificationContext {
                race: &InconclusiveRaceEvidence::default(),
                provenance: &CorrelationColumnProvenance::default(),
                document_clauses: document,
            },
        )
    }

    /// The fix itself. A permutation is ONE difference, at the collection, for
    /// every permutation — not however many positions this run's two orders
    /// happened to disagree on.
    #[test]
    fn a_permuted_array_is_one_difference_at_the_collection_whatever_the_permutation() {
        let baseline = serde_json::json!({ "tags": ["a", "b", "c", "d", "e"] });
        let mut counts = Vec::new();
        for rotation in 1..5 {
            let mut items = ["a", "b", "c", "d", "e"];
            items.rotate_left(rotation);
            let diff = body_pair(baseline.clone(), serde_json::json!({ "tags": items }));
            assert!(
                diff.body_diff.len() > 1,
                "the positional diff must be the many-row one being replaced"
            );
            let rows = order_canonical_body_diff(&diff).expect("bodies present");
            assert_eq!(
                rows.len(),
                1,
                "one ordering difference, not one per position"
            );
            assert_eq!(rows[0].json_path, "$.tags");
            counts.push(classify_body(&diff).blocking_leaf_count);
        }
        assert_eq!(counts, vec![1, 1, 1, 1], "same count for every permutation");
    }

    /// THE GUARD. An ordering difference is still a difference. Nothing here
    /// makes the comparison order-blind — it makes it say the same thing every
    /// run.
    #[test]
    fn a_reordering_is_still_a_divergence() {
        let diff = body_pair(
            serde_json::json!({ "steps": ["authenticate", "authorize", "capture"] }),
            serde_json::json!({ "steps": ["capture", "authorize", "authenticate"] }),
        );
        assert_eq!(classify_body(&diff).blocking_leaf_count, 1);
        assert_eq!(classify_body(&diff).order_only_paths, vec!["$.steps"]);
    }

    /// Members are compared WITH MULTIPLICITY: canonical order is a sort, never
    /// a dedup, so a dropped duplicate is a real difference and not a
    /// reordering.
    #[test]
    fn a_dropped_duplicate_member_is_not_an_ordering_difference() {
        let diff = body_pair(
            serde_json::json!({ "items": ["book", "book"] }),
            serde_json::json!({ "items": ["book"] }),
        );
        let classification = classify_body(&diff);
        assert_eq!(classification.blocking_leaf_count, 1);
        assert!(
            classification.order_only_paths.is_empty(),
            "losing a member is a membership difference, not an ordering one"
        );
    }

    /// Multiplicity is the whole difference between a multiset and a set. These
    /// two arrays hold the same DISTINCT members, in the same order, and are
    /// not the same collection.
    #[test]
    fn the_same_members_in_different_quantities_are_not_a_reordering() {
        let diff = body_pair(
            serde_json::json!({ "items": ["a", "a", "b"] }),
            serde_json::json!({ "items": ["a", "b", "b"] }),
        );
        let classification = classify_body(&diff);
        assert!(
            classification.order_only_paths.is_empty(),
            "a changed count is a membership difference, not an ordering one"
        );
        assert_eq!(classification.blocking_leaf_count, 1);
        let rows = order_canonical_body_diff(&diff).expect("bodies present");
        assert_eq!(rows[0].baseline, serde_json::json!("a"));
        assert_eq!(rows[0].candidate, serde_json::json!("b"));
    }

    /// A genuinely changed member survives, and lands at a canonical path
    /// rather than wherever the two orders drifted apart.
    #[test]
    fn a_changed_member_survives_canonical_alignment() {
        let diff = body_pair(
            serde_json::json!({ "tags": ["a", "b", "c"] }),
            serde_json::json!({ "tags": ["c", "z", "a"] }),
        );
        let rows = order_canonical_body_diff(&diff).expect("bodies present");
        assert_eq!(rows.len(), 1, "one member changed, one row");
        assert_eq!(rows[0].baseline, serde_json::json!("b"));
        assert_eq!(rows[0].candidate, serde_json::json!("z"));
        assert!(classify_body(&diff).order_only_paths.is_empty());
    }

    /// A reordering of an OUTER collection is reported at the outer collection
    /// and not descended into — descending would re-describe one fact as
    /// however many nested positions this run's orders disagreed on.
    #[test]
    fn a_nested_reordering_is_reported_at_the_outermost_collection() {
        let group = |first: &str, second: &str, inner: [&str; 3]| {
            serde_json::json!({ "groups": [
                { "name": first, "members": inner },
                { "name": second, "members": inner },
            ]})
        };
        let diff = body_pair(
            group("x", "y", ["1", "2", "3"]),
            group("y", "x", ["3", "1", "2"]),
        );
        let rows = order_canonical_body_diff(&diff).expect("bodies present");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].json_path, "$.groups");
    }

    /// The real response pair this came from: two runs of the same binary
    /// returning the same nine card networks in different orders. Fifteen
    /// positional rows become one, and the same one whichever way it is shuffled.
    #[test]
    fn the_measured_response_reduces_to_one_stable_difference() {
        let recorded = serde_json::json!({ "payment_methods_enabled": [
            { "payment_method_type": "credit", "card_networks": [
                "AmericanExpress", "DinersClub", "Discover", "JCB", "CartesBancaires",
                "Visa", "UnionPay", "Mastercard", "Interac"]},
            { "payment_method_type": "debit", "card_networks": [
                "Mastercard", "Interac", "DinersClub", "Discover", "JCB",
                "Visa", "AmericanExpress", "UnionPay", "CartesBancaires"]},
        ]});
        let replayed = serde_json::json!({ "payment_methods_enabled": [
            { "payment_method_type": "debit", "card_networks": [
                "CartesBancaires", "Mastercard", "Visa", "Discover", "AmericanExpress",
                "Interac", "JCB", "DinersClub", "UnionPay"]},
            { "payment_method_type": "credit", "card_networks": [
                "UnionPay", "DinersClub", "Mastercard", "JCB", "Discover",
                "Interac", "CartesBancaires", "AmericanExpress", "Visa"]},
        ]});
        let diff = body_pair(recorded, replayed);
        assert!(
            diff.body_diff.len() >= 15,
            "the positional diff is the noisy one"
        );
        let classification = classify_body(&diff);
        assert_eq!(classification.blocking_leaf_count, 1);
        assert_eq!(
            classification.order_only_paths,
            vec!["$.payment_methods_enabled"]
        );
    }

    /// A body carrying no arrays at all reaches exactly the rows the kernel
    /// emitted.
    #[test]
    fn a_body_without_arrays_is_unchanged() {
        let diff = body_pair(
            serde_json::json!({ "amount": 100, "currency": "USD" }),
            serde_json::json!({ "amount": 101, "currency": "USD" }),
        );
        assert_eq!(
            order_canonical_body_diff(&diff).expect("bodies present"),
            diff.body_diff
        );
    }

    /// A boundary that declares `bag` says its collections carry no order. THAT
    /// is where knowledge of a payload lives — in a declaration made by whoever
    /// owns the semantics, against the boundary it describes.
    #[test]
    fn a_declared_bag_reply_canon_absorbs_a_reordering() {
        let corr = "bag-declared";
        let recorded = serde_json::json!({ "tags": ["a", "b", "c"] });
        let replayed = serde_json::json!({ "tags": ["c", "a", "b"] });
        let card = detect(&art_with_events(
            vec![],
            vec![],
            vec![http_with_bodies(
                corr,
                true,
                deja_kernel::diff_json(&recorded, &replayed, "$", &[]),
                recorded.clone(),
                replayed,
            )],
            vec![http_incoming_ev_with_reply_canon(
                corr,
                901,
                Some("bag"),
                recorded,
            )],
        ));
        assert_eq!(kind_count(&card, "http_incoming", "BodyMismatch"), 0);
        assert_eq!(card.summary.http_body_mismatches, 0);
    }

    /// …and it absorbs ONLY reordering. A declaration that order is
    /// insignificant is not a declaration that membership is.
    #[test]
    fn a_declared_bag_reply_canon_keeps_a_changed_member_blocking() {
        let corr = "bag-declared-changed";
        let recorded = serde_json::json!({ "tags": ["a", "b", "c"] });
        let replayed = serde_json::json!({ "tags": ["c", "a", "z"] });
        let card = detect(&art_with_events(
            vec![],
            vec![],
            vec![http_with_bodies(
                corr,
                true,
                deja_kernel::diff_json(&recorded, &replayed, "$", &[]),
                recorded.clone(),
                replayed,
            )],
            vec![http_incoming_ev_with_reply_canon(
                corr,
                902,
                Some("bag"),
                recorded,
            )],
        ));
        assert_eq!(kind_count(&card, "http_incoming", "BodyMismatch"), 1);
        assert_eq!(card.summary.http_body_mismatches, 1);
    }

    /// The report SAYS a difference was ordering, naming the collection.
    #[test]
    fn the_scorecard_names_an_ordering_difference_as_one() {
        let recorded = serde_json::json!({ "tags": ["a", "b", "c"] });
        let replayed = serde_json::json!({ "tags": ["c", "a", "b"] });
        let card = detect(&art(vec![], vec![], vec![body_pair(recorded, replayed)]));
        assert_eq!(kind_count(&card, "http_incoming", "BodyMismatch"), 1);
        assert!(
            card.warnings.iter().any(|w| w.contains("$.tags")
                && w.contains("same members in a different order")
                && w.contains("still blocks")),
            "warnings: {:?}",
            card.warnings
        );
    }

    #[test]
    fn scorer_does_not_treat_script_text_as_form_inputs() {
        let baseline = r#"<!DOCTYPE html><html><body><form><script>const fields = ['<input type="hidden" name="a" value="1">', '<input type="hidden" name="b" value="2">'];</script><input type="hidden" name="token" value="abc"></form></body></html>"#;
        let candidate = r#"<!DOCTYPE html><html><body><form><script>const fields = ['<input type="hidden" name="b" value="2">', '<input type="hidden" name="a" value="1">'];</script><input type="hidden" name="token" value="abc"></form></body></html>"#;
        let diff = scorer_html_diff(baseline, candidate);
        assert_eq!(
            classify_http_body_diff(
                &diff,
                None,
                BodyClassificationContext {
                    race: &InconclusiveRaceEvidence::default(),
                    provenance: &CorrelationColumnProvenance::default(),
                    document_clauses: &[],
                },
            )
            .blocking_leaf_count,
            1
        );
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
            scored_span_namespaces: Vec::new(),
            reply_canons: Default::default(),
            run_id: "run-db-volatile-canon-ledger".to_owned(),
            recording_id: Some("rec-1".to_owned()),
            table: LookupTable {
                recording_id: "rec-1".to_owned(),
                policy_version: 1,
                entries,
            },
            observed,
            http_diffs: vec![http(corr, true, vec![])],
            record_graph: None,
            replay_graph: Vec::new(),
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
        let rows = ledger::build(&a.events, &a.observed, &a.table, &HashSet::new());
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
            scored_span_namespaces: Vec::new(),
            reply_canons: Default::default(),
            run_id: run_id.to_owned(),
            recording_id: Some(recording_id.to_owned()),
            table,
            observed,
            http_diffs,
            record_graph: None,
            replay_graph: Vec::new(),
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
        let provenance = CorrelationColumnProvenance::default();
        let shape = |args: &serde_json::Value| pairing_shape(args, None, &provenance);
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
            shape(&confirm),
            shape(&confirm_rekeyed),
            "operands live in the binds tail; a re-keyed write must still pair"
        );

        // A DIFFERENT statement at the same call site must not claim it. This is
        // run-0812: a 10-column connector-response UPDATE popped an 18-column
        // confirm UPDATE out of the same FIFO queue.
        let connector_response = serde_json::json!({
            "operation": "generic_update_with_results", "table": "payment_attempt",
            "sql": "UPDATE \"payment_attempt\" SET \"connector_transaction_id\" = $1 \
                    WHERE \"attempt_id\" = $2 -- binds: [TxnId(\"D4P\"), \"a_1\"]"});
        assert_ne!(shape(&confirm), shape(&connector_response));

        // And a different TABLE must not claim it, with or without SQL — the
        // ledger showed a recorded payment_attempt row scored against an
        // observed payment_intent row.
        let intent = serde_json::json!({
            "operation": "generic_update_with_results", "table": "payment_intent",
            "sql": "UPDATE \"payment_intent\" SET \"status\" = $1 WHERE \"payment_id\" = $2 \
                    -- binds: [Pending, \"p_1\"]"});
        assert_ne!(shape(&confirm), shape(&intent));
        assert_ne!(
            shape(&serde_json::json!({"table": "payment_attempt"})),
            shape(&serde_json::json!({"table": "payment_intent"})),
            "table identity must survive the no-SQL fallback"
        );

        // A re-keyed cache write keeps its twin: `key` is an operand, not identity.
        assert_eq!(
            shape(&serde_json::json!({"cache": "ACCOUNTS_CACHE", "key": "a"})),
            shape(&serde_json::json!({"cache": "ACCOUNTS_CACHE", "key": "b"}))
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

    // -----------------------------------------------------------------------
    // Truncated recording tails
    // -----------------------------------------------------------------------

    /// A recorded event at `seq` on `boundary`/`method`, carrying `args`.
    fn tail_ev(
        seq: u64,
        corr: &str,
        boundary: &str,
        method: &str,
        args: serde_json::Value,
    ) -> deja::BoundaryEvent {
        let mut ev = omitted_ev(seq, boundary, Some(corr));
        ev.method_name = method.to_owned();
        ev.args = args;
        ev
    }

    /// The router's API-lock release — the request-teardown marker a truncated
    /// recording ends at.
    fn teardown_ev(seq: u64, corr: &str) -> deja::BoundaryEvent {
        tail_ev(
            seq,
            corr,
            "redis",
            "delete_key",
            serde_json::json!({"command": "DEL", "key": "API_LOCK_merchant_1_payments_pay_x"}),
        )
    }

    fn resolved_obs(boundary: &str, corr: &str, seq: u64) -> ObservedCall {
        obs(boundary, Some(corr), true, Some(1), Some(seq))
    }

    /// The measured shape. `c1`'s recording ends at its API-lock release while
    /// the tape runs on for `c2`, so the truncation is per-correlation. The
    /// candidate reproduces every recorded `c1` call and then makes one more —
    /// the post-response webhook work the recorder never captured.
    ///
    /// The parameters are exactly what the class must stay sensitive to:
    /// `status_match` and `body` are THE GUARD's inputs, and `extra_at_tail`
    /// puts the candidate's unrecorded call after the teardown marker (a tail
    /// gap) or before it (mid-request work, which is a real novel call).
    fn tail_gap_art(
        status_match: bool,
        body: Vec<JsonFieldDiff>,
        extra_at_tail: bool,
    ) -> RunArtifacts {
        let extra = obs("db", Some("c1"), false, None, None);
        let mut observed = vec![resolved_obs("db", "c1", 1)];
        if !extra_at_tail {
            observed.push(extra.clone());
        }
        observed.push(resolved_obs("redis", "c1", 2));
        if extra_at_tail {
            observed.push(extra);
        }
        observed.push(resolved_obs("db", "c2", 3));
        art_with_events(
            vec![
                seq_entry(Some("c1"), "db", 1),
                seq_entry(Some("c1"), "redis", 2),
                seq_entry(Some("c2"), "db", 3),
            ],
            observed,
            vec![http("c1", status_match, body), http("c2", true, vec![])],
            vec![
                tail_ev(1, "c1", "db", "m", serde_json::json!({})),
                teardown_ev(2, "c1"),
                tail_ev(3, "c2", "db", "m", serde_json::json!({})),
            ],
        )
    }

    /// The measured defect. The recorder stops capturing a correlation at the
    /// API-lock release, so the post-response webhook work the request path goes
    /// on to do never reaches the tape. The candidate does it for real on replay
    /// and every call in it misses the table. That is a RECORDING limit, so it
    /// is neither a blocking novel call nor a pass.
    #[test]
    fn truncated_recording_tail_is_inconclusive_not_a_novel_call() {
        let card = detect(&tail_gap_art(true, vec![], true));
        assert_eq!(card.summary.inconclusive_tail_gaps, 1);
        assert_eq!(card.summary.novel_calls, 0, "a tail gap is not a Novel");
        assert_eq!(card.summary.side_effect_divergences, 0);
        // Laundering check: the demotion must not have been bought by matching
        // FEWER calls.
        assert_eq!(card.summary.matched_side_effect_calls, 3);
        assert!(
            !card.verdict.pass,
            "an unrecorded tail is not a pass: {}",
            card.verdict.reason
        );
        assert!(card.verdict.inconclusive, "{}", card.verdict.reason);
        assert!(
            card.verdict.reason.contains("tail-gap"),
            "{}",
            card.verdict.reason
        );

        let c1 = card
            .per_correlation
            .iter()
            .find(|c| c.correlation_id == "c1")
            .expect("c1 scored");
        assert!(c1.inconclusive, "the correlation cannot be judged");
        assert!(!c1.passed, "and must never read as passing");
        assert_eq!(c1.side_effect_divergences, 0);

        // A correlation the recorder captured whole is untouched.
        let c2 = card
            .per_correlation
            .iter()
            .find(|c| c.correlation_id == "c2")
            .expect("c2 scored");
        assert!(!c2.inconclusive);
        assert!(c2.passed);
        assert_eq!(card.summary.matched_correlations, 1, "c1 does not count");
        assert!(
            card.counter_disagreements().is_empty(),
            "{:?}",
            card.counter_disagreements()
        );
    }

    /// THE GUARD. Every correlation this class was built from answered with a
    /// byte-identical response, which is the only thing that makes calling their
    /// unrecorded tails inconclusive honest. A tail gap that CHANGED the
    /// response is not a tail gap, it is a divergence — so a status mismatch
    /// keeps the very same call BLOCKING.
    #[test]
    fn a_status_mismatch_keeps_the_unrecorded_tail_call_blocking() {
        let card = detect(&tail_gap_art(false, vec![], true));
        assert_eq!(
            card.summary.inconclusive_tail_gaps, 0,
            "a diverged response is not an inconclusive tail"
        );
        assert_eq!(
            card.summary.novel_calls, 1,
            "the tail call stays a BLOCKING novel call"
        );
        assert_eq!(card.summary.side_effect_divergences, 1);
        assert!(!card.verdict.pass);
        assert!(
            !card.verdict.inconclusive,
            "a diverged response is a real fail, not an unjudged run"
        );
        assert!(
            card.counter_disagreements().is_empty(),
            "{:?}",
            card.counter_disagreements()
        );
    }

    /// THE GUARD, other half: the status agreed but the BODY did not. Same
    /// answer — the response differed, so the tail is a divergence.
    #[test]
    fn a_body_mismatch_keeps_the_unrecorded_tail_call_blocking() {
        let card = detect(&tail_gap_art(
            true,
            vec![JsonFieldDiff {
                json_path: "$.amount".to_owned(),
                baseline: serde_json::json!(100),
                candidate: serde_json::json!(200),
            }],
            true,
        ));
        assert_eq!(card.summary.inconclusive_tail_gaps, 0);
        assert_eq!(card.summary.novel_calls, 1);
        assert_eq!(card.summary.http_body_mismatches, 1);
        assert!(!card.verdict.pass);
        assert!(!card.verdict.inconclusive);
    }

    /// The condition that does the real work. "The recording ends at a teardown
    /// marker" is very nearly universal — in the measured main-app run it held
    /// for 71 of 77 correlations — so it cannot be what selects a tail gap. That
    /// same run carried 16 BLOCKING novel `update_payment_intent` calls inside
    /// HTTP-clean, teardown-ending correlations, mid-request. Only their
    /// POSITION keeps them blocking, and it must.
    #[test]
    fn a_novel_call_before_the_teardown_marker_stays_blocking() {
        let card = detect(&tail_gap_art(true, vec![], false));
        assert_eq!(
            card.summary.inconclusive_tail_gaps, 0,
            "mid-request work has a recorded baseline region; it is not a tail"
        );
        assert_eq!(card.summary.novel_calls, 1);
        assert_eq!(card.summary.side_effect_divergences, 1);
        assert!(!card.verdict.pass);
        assert!(
            card.counter_disagreements().is_empty(),
            "{:?}",
            card.counter_disagreements()
        );
    }

    /// Truncation is the claim, so it has to be evidenced. When the tape carries
    /// recorded work PAST the correlation's lock release the recorder plainly
    /// did not stop there, and an extra candidate call is a genuine novel call.
    #[test]
    fn a_recording_that_continues_past_teardown_is_not_truncated() {
        // c1 records work AFTER its lock release, so the recorder plainly did
        // not stop there. The candidate reproduces all of it and THEN makes an
        // extra call, in the same trailing position a genuine tail gap would
        // occupy and with the tape still running on for c2 — so the marker test
        // is the only thing left that can tell the two apart. (An earlier
        // version of this fixture put the extra call mid-stream and ended the
        // tape at c1, and passed under a deleted marker test: the position and
        // tape-end conditions were catching it instead.)
        let a = art_with_events(
            vec![
                seq_entry(Some("c1"), "db", 1),
                seq_entry(Some("c1"), "redis", 2),
                seq_entry(Some("c1"), "db", 3),
                seq_entry(Some("c2"), "db", 4),
            ],
            vec![
                resolved_obs("db", "c1", 1),
                resolved_obs("redis", "c1", 2),
                resolved_obs("db", "c1", 3),
                obs("db", Some("c1"), false, None, None),
                resolved_obs("db", "c2", 4),
            ],
            vec![http("c1", true, vec![]), http("c2", true, vec![])],
            vec![
                tail_ev(1, "c1", "db", "m", serde_json::json!({})),
                teardown_ev(2, "c1"),
                tail_ev(3, "c1", "db", "m", serde_json::json!({})),
                tail_ev(4, "c2", "db", "m", serde_json::json!({})),
            ],
        );
        let card = detect(&a);
        assert_eq!(card.summary.inconclusive_tail_gaps, 0);
        assert_eq!(card.summary.novel_calls, 1);
        assert!(!card.verdict.pass);
    }

    /// A correlation running to the very end of the tape is left BLOCKING. The
    /// evidence for truncation is that the recorder kept capturing OTHER work
    /// after this correlation's lock release; when the tape simply stops there
    /// that evidence is absent, and the safe reading is the strict one.
    #[test]
    fn a_teardown_at_the_end_of_the_tape_is_not_evidence_of_truncation() {
        let mut a = tail_gap_art(true, vec![], true);
        // Drop c2, so c1's lock release IS the tape's last recorded event.
        a.events
            .retain(|ev| ev.correlation_id.as_deref() != Some("c2"));
        a.table
            .entries
            .retain(|entry| entry.key.correlation_id.as_deref() != Some("c2"));
        a.observed
            .retain(|call| call.correlation_id.as_deref() != Some("c2"));
        a.http_diffs.retain(|diff| diff.correlation_id != "c2");
        let card = detect(&a);
        assert_eq!(card.summary.inconclusive_tail_gaps, 0);
        assert_eq!(card.summary.novel_calls, 1);
        assert!(!card.verdict.pass);
    }

    /// The tail begins where the recording ended, so the candidate has to have
    /// GOT there. One that never reproduced the lock release has an omission on
    /// its hands, not an unrecorded tail to be excused.
    #[test]
    fn a_candidate_that_never_reached_teardown_has_no_tail_to_excuse() {
        let mut a = tail_gap_art(true, vec![], true);
        a.observed
            .retain(|call| call.source_event_global_sequence != Some(2));
        let card = detect(&a);
        assert_eq!(card.summary.inconclusive_tail_gaps, 0);
        assert_eq!(
            card.summary.omitted_calls, 1,
            "the unreproduced lock release is a real omission"
        );
        assert_eq!(card.summary.novel_calls, 1);
        assert!(!card.verdict.pass);
    }

    /// The ledger and the scorecard must tell ONE story: a demoted tail call
    /// carries a non-blocking row naming the same class the summary counts.
    #[test]
    fn the_ledger_names_a_tail_gap_the_same_way_the_scorecard_counts_it() {
        let a = tail_gap_art(true, vec![], true);
        let card = detect(&a);
        let rows = build_ledger(&a).expect("ledger builds");
        let tail: Vec<_> = rows
            .iter()
            .filter(|row| row.kind == "inconclusive_tail_gap")
            .collect();
        assert_eq!(tail.len() as u64, card.summary.inconclusive_tail_gaps);
        assert!(
            tail.iter().all(|row| !row.blocking),
            "an unjudgeable call is not charged to the candidate"
        );
        assert!(
            !rows.iter().any(|row| row.kind == "novel" && row.blocking),
            "no blocking novel row may survive alongside the demotion"
        );
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
            card.verdict
                .reason
                .contains("1 schema-derived DB/response occurrence(s) (non-blocking)"),
            "counted and named in the verdict, never silently dropped: {}",
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

    const PAYMENT_INTENT_UPDATE_BINDING_DESCRIPTION: &str =
        "UPDATE \"payment_intent\" SET \"currency\" = $1, \"description\" = $2 WHERE \
        (\"payment_intent\".\"payment_id\" = $3) RETURNING * -- binds: [USD, \"changed\", \
        \"pay_1\"]";

    const PAYMENT_INTENT_UPDATE_BINDING_LABEL_AND_DESCRIPTION: &str =
        "UPDATE \"payment_intent\" SET \"currency\" = $1, \"business_label\" = $2, \
        \"description\" = $3 WHERE (\"payment_intent\".\"payment_id\" = $4) RETURNING * -- binds: \
        [USD, \"default\", \"changed\", \"pay_1\"]";

    #[test]
    fn update_unassigned_returned_difference_matches_without_correlation_history() {
        let corr = "c1";
        let recorded = payment_intent_row(serde_json::Value::Null);
        let mut update = db_insert_ev(corr, 8, PAYMENT_INTENT_UPDATE, recorded.clone());
        update.method_name = "generic_update_with_results".to_owned();
        let mut observed = db_exec_obs_with_sql(
            corr,
            8,
            PAYMENT_INTENT_UPDATE,
            recorded.clone(),
            payment_intent_row(serde_json::json!("different inherited value")),
        );
        observed.method_name = "generic_update_with_results".to_owned();

        let card = detect(&art_with_events(
            vec![seq_entry_method_res(
                Some(corr),
                "db",
                "generic_update_with_results",
                8,
                envelope(recorded),
            )],
            vec![observed],
            vec![http(corr, true, vec![])],
            vec![update],
        ));

        assert_eq!(card.summary.matched_side_effect_calls, 1);
        assert_eq!(card.summary.schema_default_divergences, 0);
        assert_eq!(card.summary.value_divergences, 0);
        assert!(card.verdict.pass, "{}", card.verdict.reason);
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

    /// The resolved INSERT proves `business_label` schema-derived. The later
    /// UPDATE misses strict args lookup, so only pairing shape can recover it.
    fn schema_rekeyed_update_card(observed_update_sql: &str) -> Scorecard {
        let corr = "c1";
        let recorded = payment_intent_row(serde_json::Value::Null);
        let observed = payment_intent_row(serde_json::json!("default"));
        let insert_event = db_insert_ev(corr, 7, PAYMENT_INTENT_INSERT, recorded.clone());
        let mut update_event = db_insert_ev(corr, 8, PAYMENT_INTENT_UPDATE, recorded.clone());
        update_event.method_name = "generic_update_with_results".to_owned();

        let insert_observed = db_exec_obs_with_sql(
            corr,
            7,
            PAYMENT_INTENT_INSERT,
            recorded.clone(),
            observed.clone(),
        );
        let mut update_observed = exec_obs_method(
            "db",
            Some(corr),
            "generic_update_with_results",
            false,
            None,
            None,
            envelope(observed),
        );
        update_observed.seed_gap = false;
        update_observed.args =
            serde_json::json!({"table": "payment_intent", "sql": observed_update_sql});
        let update_observed = with_span(update_observed, "root>update_payment_intent");

        detect(&art_with_events(
            vec![
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_insert",
                    7,
                    envelope(recorded.clone()),
                ),
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_update_with_results",
                    8,
                    envelope(recorded),
                ),
                span_entry(Some(corr), 8, "root>update_payment_intent"),
            ],
            vec![insert_observed, update_observed],
            vec![http(corr, true, vec![])],
            vec![insert_event, update_event],
        ))
    }

    #[test]
    fn schema_derived_set_column_does_not_rekey_args_free_pairing() {
        let card = schema_rekeyed_update_card(PAYMENT_INTENT_UPDATE_BINDING_LABEL);

        assert_eq!(
            card.summary.schema_default_divergences, 1,
            "the resolved INSERT establishes the environment-derived column"
        );
        assert_eq!(
            card.summary.value_divergences, 1,
            "the UPDATE is one paired call with a value difference"
        );
        assert_eq!(card.summary.novel_calls, 0, "not a novel UPDATE");
        assert_eq!(card.summary.omitted_calls, 0, "not an omitted UPDATE");
        assert_eq!(kind_count(&card, "db", "ValueDiverged"), 1);
    }

    #[test]
    fn non_schema_derived_set_column_still_separates_pairing() {
        let card = schema_rekeyed_update_card(PAYMENT_INTENT_UPDATE_BINDING_DESCRIPTION);

        assert_eq!(card.summary.schema_default_divergences, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.novel_calls, 1);
        assert_eq!(card.summary.omitted_calls, 1);
        assert_eq!(kind_count(&card, "db", "ValueDiverged"), 0);
    }

    #[test]
    fn mixed_schema_and_non_schema_set_columns_still_separate_pairing() {
        let card = schema_rekeyed_update_card(PAYMENT_INTENT_UPDATE_BINDING_LABEL_AND_DESCRIPTION);

        assert_eq!(card.summary.schema_default_divergences, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(
            card.summary.novel_calls, 1,
            "the non-schema column keeps the observed UPDATE novel"
        );
        assert_eq!(
            card.summary.omitted_calls, 1,
            "the non-schema column keeps the recorded UPDATE omitted"
        );
        assert_eq!(kind_count(&card, "db", "ValueDiverged"), 0);
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
    fn graph_span(
        node_id: u64,
        correlation_id: &str,
        parent_id: Option<u64>,
        sequence: u64,
        span_name: &str,
    ) -> deja_core::ExecutionGraphNode {
        let mut node = graph_node(node_id, Some(correlation_id));
        node.parent_id = parent_id;
        node.sequence = sequence;
        node.span_name = span_name.to_owned();
        node
    }

    fn graph_event(
        correlation_id: &str,
        global_sequence: u64,
        graph_node_id: u64,
        method: &str,
        args: serde_json::Value,
        result: serde_json::Value,
    ) -> deja::BoundaryEvent {
        let mut event = omitted_ev(global_sequence, "db", Some(correlation_id));
        event.method_name = method.to_owned();
        event.args = args;
        event.result = result;
        event.graph_node_id = Some(graph_node_id);
        event
    }

    fn graph_observed(
        correlation_id: &str,
        graph_node_id: u64,
        served_event: u64,
        method: &str,
        args: serde_json::Value,
        result: serde_json::Value,
    ) -> ObservedCall {
        let mut observed =
            substituted_obs_method("db", Some(correlation_id), method, served_event, result);
        observed.args = args;
        observed.graph_node_id = Some(graph_node_id);
        observed
    }

    fn with_graphs(
        mut artifacts: RunArtifacts,
        record: Vec<deja_core::ExecutionGraphNode>,
        replay: Vec<deja_core::ExecutionGraphNode>,
    ) -> RunArtifacts {
        artifacts.record_graph = Some(record);
        artifacts.replay_graph = replay;
        artifacts
    }

    fn strip_scoring_mode(mut value: serde_json::Value) -> serde_json::Value {
        for outcome in value["per_correlation"]
            .as_array_mut()
            .expect("serialized per_correlation array")
        {
            outcome
                .as_object_mut()
                .expect("serialized correlation outcome")
                .remove("scoring_mode")
                .expect("every correlation declares its scoring mode");
        }
        value
    }

    // --- span-shape check (scored_span_namespaces) ---------------------------

    fn scored_span(
        node_id: u64,
        correlation_id: &str,
        parent_id: Option<u64>,
        sequence: u64,
        span_name: &str,
        fields: &[(&str, &str)],
    ) -> deja_core::ExecutionGraphNode {
        let mut node = graph_span(node_id, correlation_id, parent_id, sequence, span_name);
        node.fields = fields
            .iter()
            .map(|(k, v)| ((*k).to_owned(), serde_json::Value::String((*v).to_owned())))
            .collect();
        node
    }

    fn with_scored_namespaces(mut artifacts: RunArtifacts) -> RunArtifacts {
        artifacts.scored_span_namespaces = vec!["ucs::".to_owned(), "connector::".to_owned()];
        artifacts
    }

    /// The instrumented lattice both sides record when nothing regressed:
    /// scored spans anchored under plain spans, with chokepoint fields.
    fn instrumented_graph(corr: &str) -> Vec<deja_core::ExecutionGraphNode> {
        vec![
            graph_span(1, corr, None, 0, "request"),
            scored_span(
                2,
                corr,
                Some(1),
                1,
                "ucs::flow_orchestration",
                &[("connector", "stripe"), ("flow", "Authorize")],
            ),
            graph_span(3, corr, Some(2), 2, "execute_connector_processing_step"),
            scored_span(
                4,
                corr,
                Some(3),
                3,
                "connector::request_body",
                &[("connector", "Stripe"), ("flow", "Authorize")],
            ),
        ]
    }

    #[test]
    fn declaring_no_namespaces_keeps_the_scorecard_byte_identical() {
        let _lock = crate::test_env::env_guard();
        // The opt-out guarantee: with `scored_span_namespaces` undeclared, a
        // tape full of scored spans must serialize the SAME scorecard as one
        // with no scored spans at all — the check may not leak a section, a
        // pseudo-boundary, or a counter into runs that never asked for it
        // (hyperswitch runs never ask).
        let corr = "shape-off";
        let instrumented = with_graphs(
            art(vec![], vec![], vec![http(corr, true, vec![])]),
            instrumented_graph(corr),
            instrumented_graph(corr),
        );
        let plain: Vec<deja_core::ExecutionGraphNode> = instrumented_graph(corr)
            .into_iter()
            .map(|mut n| {
                n.span_name = format!("plain_{}", n.node_id);
                n.fields.clear();
                n
            })
            .collect();
        let uninstrumented = with_graphs(
            art(vec![], vec![], vec![http(corr, true, vec![])]),
            plain.clone(),
            plain,
        );
        let a = serde_json::to_string(&detect(&instrumented)).unwrap();
        let b = serde_json::to_string(&detect(&uninstrumented)).unwrap();
        assert_eq!(
            a, b,
            "undeclared check must leave no trace in the scorecard"
        );
        assert!(!a.contains("span_shape"));
    }

    #[test]
    fn matched_scored_spans_pass_and_surface_the_contract_section() {
        let corr = "shape-clean";
        let artifacts = with_scored_namespaces(with_graphs(
            art(vec![], vec![], vec![http(corr, true, vec![])]),
            instrumented_graph(corr),
            instrumented_graph(corr),
        ));
        let card = detect(&artifacts);
        assert!(
            card.verdict.pass,
            "clean shape must pass: {}",
            card.verdict.reason
        );
        let outcome = &card.per_correlation[0];
        let shape = outcome
            .span_shape
            .as_ref()
            .expect("span_shape section present");
        assert_eq!(shape.matched, 2);
        assert!(shape.clean());
        assert_eq!(
            card.per_boundary["graph"].kinds.get("MatchedScoredSpan"),
            Some(&2),
            "matched spans ride a kind, not stats.matched (that fold counts calls)"
        );
        // The path must contract the plain anchor spans away.
        assert!(shape
            .outcomes
            .iter()
            .any(|o| o.path == "ucs::flow_orchestration>connector::request_body"));
    }

    #[test]
    fn a_scored_span_the_replay_dropped_fails_the_verdict() {
        let corr = "shape-missing";
        let mut replay = instrumented_graph(corr);
        replay.retain(|n| n.span_name != "connector::request_body");
        let artifacts = with_scored_namespaces(with_graphs(
            art(vec![], vec![], vec![http(corr, true, vec![])]),
            instrumented_graph(corr),
            replay,
        ));
        let card = detect(&artifacts);
        assert!(!card.verdict.pass);
        assert!(
            card.verdict.reason.contains("1 missing scored span(s)"),
            "reason names the finding: {}",
            card.verdict.reason
        );
        assert_eq!(card.summary.missing_scored_spans, 1);
        assert!(!card.per_correlation[0].passed);
        let shape = card.per_correlation[0].span_shape.as_ref().unwrap();
        let miss = shape
            .outcomes
            .iter()
            .find(|o| o.status == span_shape::SpanShapeStatus::Missing)
            .expect("missing outcome reported");
        assert_eq!(miss.span_name, "connector::request_body");
    }

    #[test]
    fn an_uninstrumented_tape_against_an_instrumented_candidate_fails_as_novel() {
        // Deliberate policy: the tape is the contract in BOTH directions. A
        // recording made before the candidate grew its instrumentation fails
        // (novel) rather than silently passing — re-record it. UCS accepts
        // this; hyperswitch never declares namespaces, so it cannot hit it.
        let corr = "shape-novel";
        let artifacts = with_scored_namespaces(with_graphs(
            art(vec![], vec![], vec![http(corr, true, vec![])]),
            vec![graph_span(1, corr, None, 0, "request")],
            instrumented_graph(corr),
        ));
        let card = detect(&artifacts);
        assert!(!card.verdict.pass);
        assert!(
            card.verdict.reason.contains("2 novel scored span(s)"),
            "reason: {}",
            card.verdict.reason
        );
        assert_eq!(card.summary.novel_scored_spans, 2);
    }

    #[test]
    fn record_graph_unavailable_skips_the_check_with_a_warning() {
        // An absent record graph is an artifact gap, not a shape finding: the
        // check cannot run, and saying so beats inventing a verdict. The run's
        // other checks still decide pass/fail.
        let corr = "shape-nograph";
        let mut artifacts =
            with_scored_namespaces(art(vec![], vec![], vec![http(corr, true, vec![])]));
        artifacts.replay_graph = instrumented_graph(corr);
        assert!(artifacts.record_graph.is_none());
        let card = detect(&artifacts);
        assert!(
            card.verdict.pass,
            "skip must not fail: {}",
            card.verdict.reason
        );
        assert!(card
            .warnings
            .iter()
            .any(|w| w.contains("scored-span shape check skipped")));
        assert!(card.per_correlation[0].span_shape.is_none());
    }

    #[test]
    fn a_changed_chokepoint_field_fails_with_the_key_named() {
        let corr = "shape-field";
        let mut replay = instrumented_graph(corr);
        for n in &mut replay {
            if n.span_name == "ucs::flow_orchestration" {
                n.fields.insert(
                    "connector".to_owned(),
                    serde_json::Value::String("adyen".to_owned()),
                );
            }
        }
        let artifacts = with_scored_namespaces(with_graphs(
            art(vec![], vec![], vec![http(corr, true, vec![])]),
            instrumented_graph(corr),
            replay,
        ));
        let card = detect(&artifacts);
        assert!(!card.verdict.pass);
        assert!(
            card.verdict
                .reason
                .contains("1 scored-span field divergence(s)"),
            "reason: {}",
            card.verdict.reason
        );
        assert_eq!(card.summary.span_field_divergences, 1);
        let shape = card.per_correlation[0].span_shape.as_ref().unwrap();
        let div = shape
            .outcomes
            .iter()
            .find(|o| o.status == span_shape::SpanShapeStatus::FieldDiverged)
            .expect("diverged outcome reported");
        assert_eq!(div.field_diffs[0].key, "connector");
    }

    #[test]
    fn both_forests_choose_and_serialize_graph_mode() {
        let corr = "graph-both";
        let result = serde_json::json!({"result": "Ok", "value": 7});
        let event = graph_event(
            corr,
            101,
            1,
            "load",
            serde_json::json!({"id": 7}),
            result.clone(),
        );
        let observed = graph_observed(
            corr,
            11,
            101,
            "load",
            serde_json::json!({"id": 7}),
            result.clone(),
        );
        let artifacts = with_graphs(
            art_with_events(
                vec![seq_entry_method_res(Some(corr), "db", "load", 101, result)],
                vec![observed],
                vec![http(corr, true, vec![])],
                vec![event],
            ),
            vec![graph_span(1, corr, None, 0, "request")],
            vec![graph_span(11, corr, None, 0, "request")],
        );
        let card = detect(&artifacts);
        let outcome = card
            .per_correlation
            .iter()
            .find(|outcome| outcome.correlation_id == corr)
            .expect("graph correlation is reported");
        assert_eq!(outcome.scoring_mode, deja_forest::ScoringMode::Graph);
        assert!(outcome
            .alignment
            .as_ref()
            .is_some_and(|a| !a.nodes.is_empty()));
        let wire = serde_json::to_value(outcome).unwrap();
        assert_eq!(wire["scoring_mode"], serde_json::json!({"mode": "graph"}));
        assert!(wire["alignment"]["nodes"].is_array());
    }

    #[test]
    fn missing_forest_is_declared_flat_without_changing_flat_scorecard_bytes() {
        let corr = "flat-missing";
        let fixture = || {
            let result = serde_json::json!({"result": "Ok", "value": "same"});
            let mut observed = graph_observed(
                corr,
                12,
                201,
                "load",
                serde_json::json!({"id": 1}),
                result.clone(),
            );
            observed.graph_node_id = None;
            art_with_events(
                vec![seq_entry_method_res(
                    Some(corr),
                    "db",
                    "load",
                    201,
                    result.clone(),
                )],
                vec![observed],
                vec![http(corr, true, vec![])],
                vec![graph_event(
                    corr,
                    201,
                    2,
                    "load",
                    serde_json::json!({"id": 1}),
                    result,
                )],
            )
        };
        let flat_fixture = fixture();
        let one_missing = with_graphs(
            fixture(),
            vec![graph_span(2, corr, None, 0, "request")],
            Vec::new(),
        );
        let card = detect(&one_missing);
        let outcome = &card.per_correlation[0];
        assert_eq!(
            outcome.scoring_mode,
            deja_forest::ScoringMode::Flat {
                reason: deja_forest::FlatReason::MissingForest
            }
        );
        assert!(outcome.alignment.is_none());
        assert_eq!(
            serde_json::to_value(outcome).unwrap()["scoring_mode"],
            serde_json::json!({"mode": "flat", "reason": "missing_forest"})
        );
        let graph_bytes =
            serde_json::to_vec(&strip_scoring_mode(serde_json::to_value(&card).unwrap())).unwrap();
        let explicit_flat_bytes = serde_json::to_vec(&strip_scoring_mode(
            serde_json::to_value(detect(&flat_fixture)).unwrap(),
        ))
        .unwrap();
        assert_eq!(
            graph_bytes, explicit_flat_bytes,
            "apart from the isolated mode declaration, demotion must be byte-identical \
             to the established flat scorer"
        );
    }

    #[test]
    fn one_sided_event_bearing_http_ingress_root_demotes_instead_of_misaligning() {
        let corr = "ingress-asymmetry";
        let result = serde_json::json!({"result": "Ok"});
        let event = graph_event(corr, 301, 3, "load", serde_json::json!({}), result.clone());
        let observed = graph_observed(corr, 13, 301, "load", serde_json::json!({}), result.clone());
        let artifacts = with_graphs(
            art_with_events(
                vec![seq_entry_method_res(Some(corr), "db", "load", 301, result)],
                vec![observed],
                vec![http(corr, true, vec![])],
                vec![event],
            ),
            vec![graph_span(3, corr, None, 0, "deja::http_incoming")],
            vec![graph_span(13, corr, None, 0, "request")],
        );
        let outcome = &detect(&artifacts).per_correlation[0];
        assert_eq!(
            outcome.scoring_mode,
            deja_forest::ScoringMode::Flat {
                reason: deja_forest::FlatReason::IngressRootAsymmetry
            }
        );
        assert!(outcome.alignment.is_none());
    }
    #[test]
    fn mixed_graph_and_flat_correlations_keep_weighted_accounting_independent() {
        let graph_corr = "mixed-graph";
        let flat_corr = "mixed-flat";
        let result = serde_json::json!({"result": "Ok"});
        let mut flat_event = omitted_ev(404, "db", Some(flat_corr));
        flat_event.method_name = "flat".to_owned();
        let mut flat_observed =
            substituted_obs_method("db", Some(flat_corr), "flat", 404, result.clone());
        flat_observed.graph_node_id = None;
        let mut graph_novel = obs("db", Some(graph_corr), false, None, None);
        graph_novel.method_name = "novel".to_owned();
        graph_novel.graph_node_id = Some(15);
        let artifacts = with_graphs(
            art_with_events(
                vec![
                    seq_entry_method_res(Some(graph_corr), "db", "root", 401, result.clone()),
                    seq_entry_method_res(Some(graph_corr), "db", "old_a", 402, result.clone()),
                    seq_entry_method_res(Some(graph_corr), "db", "old_b", 403, result.clone()),
                    seq_entry_method_res(Some(flat_corr), "db", "flat", 404, result.clone()),
                ],
                vec![
                    graph_observed(
                        graph_corr,
                        14,
                        401,
                        "root",
                        serde_json::json!({}),
                        result.clone(),
                    ),
                    graph_novel,
                    flat_observed,
                ],
                vec![
                    http(graph_corr, true, vec![]),
                    http(flat_corr, true, vec![]),
                ],
                vec![
                    graph_event(
                        graph_corr,
                        401,
                        4,
                        "root",
                        serde_json::json!({}),
                        result.clone(),
                    ),
                    graph_event(
                        graph_corr,
                        402,
                        5,
                        "old_a",
                        serde_json::json!({}),
                        result.clone(),
                    ),
                    graph_event(graph_corr, 403, 5, "old_b", serde_json::json!({}), result),
                    flat_event,
                ],
            ),
            vec![
                graph_span(4, graph_corr, None, 0, "request"),
                graph_span(5, graph_corr, Some(4), 1, "old-subtree"),
            ],
            vec![
                graph_span(14, graph_corr, None, 0, "request"),
                graph_span(15, graph_corr, Some(14), 1, "new-subtree"),
            ],
        );
        let card = detect(&artifacts);
        let graph_outcome = card
            .per_correlation
            .iter()
            .find(|outcome| outcome.correlation_id == graph_corr)
            .unwrap();
        let flat_outcome = card
            .per_correlation
            .iter()
            .find(|outcome| outcome.correlation_id == flat_corr)
            .unwrap();
        assert_eq!(graph_outcome.scoring_mode, deja_forest::ScoringMode::Graph);
        assert_eq!(
            flat_outcome.scoring_mode,
            deja_forest::ScoringMode::Flat {
                reason: deja_forest::FlatReason::MissingForest
            }
        );
        let alignment = graph_outcome.alignment.as_ref().unwrap();
        assert_eq!(
            alignment
                .nodes
                .iter()
                .filter(|row| matches!(
                    &row.outcome,
                    deja_forest::NodeOutcome::PrunedSubtree { events_below: 2 }
                ))
                .count(),
            1
        );
        assert_eq!(
            alignment
                .nodes
                .iter()
                .filter(|row| matches!(
                    &row.outcome,
                    deja_forest::NodeOutcome::NovelSubtree { events_below: 1 }
                ))
                .count(),
            1
        );
        assert_eq!(kind_count(&card, "db", "PrunedSubtree"), 2);
        assert_eq!(kind_count(&card, "db", "NovelSubtree"), 1);
        assert_eq!(kind_count(&card, "db", "OmittedCall"), 0);
        assert_eq!(kind_count(&card, "db", "NovelCall"), 0);
        assert_eq!(card.summary.matched_side_effect_calls, 2);
        assert_eq!(card.summary.omitted_calls, 2);
        assert_eq!(card.summary.novel_calls, 1);
        assert_eq!(card.summary.side_effect_divergences, 3);
        assert!(card.counter_disagreements().is_empty());
        let rows = build_ledger(&artifacts).expect("mixed-tier ledger builds");
        assert_eq!(
            rows.iter()
                .filter(|row| row.kind == "pruned_subtree")
                .count(),
            2
        );
        assert_eq!(
            rows.iter()
                .filter(|row| row.kind == "novel_subtree")
                .count(),
            1
        );
    }
    #[test]
    fn same_statement_bind_order_swap_is_blocking_identity_skew() {
        let corr = "identity-swap";
        let result = serde_json::json!({"result": "Ok", "value": []});
        let events = vec![
            graph_event(
                corr,
                501,
                51,
                "same_statement",
                serde_json::json!({"bind": "a"}),
                result.clone(),
            ),
            graph_event(
                corr,
                502,
                52,
                "same_statement",
                serde_json::json!({"bind": "b"}),
                result.clone(),
            ),
        ];
        let observed = vec![
            graph_observed(
                corr,
                61,
                502,
                "same_statement",
                serde_json::json!({"bind": "b"}),
                result.clone(),
            ),
            graph_observed(
                corr,
                62,
                501,
                "same_statement",
                serde_json::json!({"bind": "a"}),
                result.clone(),
            ),
        ];
        let artifacts = with_graphs(
            art_with_events(
                vec![
                    seq_entry_method_res(Some(corr), "db", "same_statement", 501, result.clone()),
                    seq_entry_method_res(Some(corr), "db", "same_statement", 502, result),
                ],
                observed,
                vec![http(corr, true, vec![])],
                events,
            ),
            vec![
                graph_span(50, corr, None, 0, "request"),
                graph_span(51, corr, Some(50), 1, "same-span"),
                graph_span(52, corr, Some(50), 2, "same-span"),
            ],
            vec![
                graph_span(60, corr, None, 0, "request"),
                graph_span(61, corr, Some(60), 1, "same-span"),
                graph_span(62, corr, Some(60), 2, "same-span"),
            ],
        );
        let card = detect(&artifacts);
        let outcome = &card.per_correlation[0];
        assert_eq!(outcome.scoring_mode, deja_forest::ScoringMode::Graph);
        let bindings: BTreeSet<_> = outcome
            .alignment
            .as_ref()
            .expect("graph alignment is serialized")
            .nodes
            .iter()
            .filter_map(|node| match &node.outcome {
                deja_forest::NodeOutcome::IdentitySkew {
                    aligned_event,
                    served_event,
                } => Some((*aligned_event, *served_event)),
                _ => None,
            })
            .collect();
        assert_eq!(
            bindings,
            BTreeSet::from([(Some(501), Some(502)), (Some(502), Some(501))])
        );
        assert_eq!(card.summary.identity_skews, 2);
        assert_eq!(card.summary.side_effect_divergences, 2);
        assert_eq!(kind_count(&card, "db", "IdentitySkew"), 2);
        assert_eq!(kind_count(&card, "db", "ValueDiverged"), 0);
        assert_eq!(kind_count(&card, "db", "ValueDivergedOrigin"), 0);
        assert_eq!(card.summary.matched_side_effect_calls, 0);
        assert!(!outcome.passed);
        assert!(!card.verdict.pass);
        assert!(card.counter_disagreements().is_empty());
    }
    #[test]
    fn scorecard_and_ledger_classifications_agree_in_graph_and_flat_tiers() {
        let graph_corr = "agreement-graph";
        let flat_corr = "agreement-flat";
        let result = serde_json::json!({"result": "Ok", "value": 9});
        let graph_event = graph_event(
            graph_corr,
            601,
            71,
            "load",
            serde_json::json!({"id": 9}),
            result.clone(),
        );
        let mut flat_event = omitted_ev(602, "db", Some(flat_corr));
        flat_event.method_name = "load".to_owned();
        flat_event.result = result.clone();
        let graph_observed = graph_observed(
            graph_corr,
            81,
            601,
            "load",
            serde_json::json!({"id": 9}),
            result.clone(),
        );
        let mut flat_observed =
            substituted_obs_method("db", Some(flat_corr), "load", 602, result.clone());
        flat_observed.args = serde_json::json!({"id": 9});
        let artifacts = with_graphs(
            art_with_events(
                vec![
                    seq_entry_method_res(Some(graph_corr), "db", "load", 601, result.clone()),
                    seq_entry_method_res(Some(flat_corr), "db", "load", 602, result),
                ],
                vec![graph_observed, flat_observed],
                vec![
                    http(graph_corr, true, vec![]),
                    http(flat_corr, true, vec![]),
                ],
                vec![graph_event, flat_event],
            ),
            vec![graph_span(71, graph_corr, None, 0, "request")],
            vec![graph_span(81, graph_corr, None, 0, "request")],
        );
        let card = detect(&artifacts);
        let rows = build_ledger(&artifacts).expect("shared graph plan builds a ledger");
        assert_eq!(card.summary.matched_side_effect_calls, 2);
        assert_eq!(card.per_boundary["db"].matched, 2);
        assert_eq!(rows.iter().filter(|row| row.kind == "matched").count(), 2);
        for corr in [graph_corr, flat_corr] {
            assert_eq!(
                rows.iter()
                    .filter(|row| {
                        row.correlation_id.as_deref() == Some(corr) && row.kind == "matched"
                    })
                    .count(),
                1
            );
        }
        assert_eq!(
            card.per_correlation
                .iter()
                .find(|outcome| outcome.correlation_id == graph_corr)
                .unwrap()
                .scoring_mode,
            deja_forest::ScoringMode::Graph
        );
        assert_eq!(
            card.per_correlation
                .iter()
                .find(|outcome| outcome.correlation_id == flat_corr)
                .unwrap()
                .scoring_mode,
            deja_forest::ScoringMode::Flat {
                reason: deja_forest::FlatReason::MissingForest
            }
        );
        assert!(card.counter_disagreements().is_empty());
    }
    #[test]
    fn multiple_events_on_one_aligned_span_fall_back_without_panicking_or_double_counting() {
        let corr = "multi-event-span";
        let result = serde_json::json!({"result": "Ok"});
        let artifacts = with_graphs(
            art_with_events(
                vec![
                    seq_entry_method_res(Some(corr), "db", "first", 701, result.clone()),
                    seq_entry_method_res(Some(corr), "db", "second", 702, result.clone()),
                ],
                vec![
                    graph_observed(
                        corr,
                        101,
                        701,
                        "first",
                        serde_json::json!({"n": 1}),
                        result.clone(),
                    ),
                    graph_observed(
                        corr,
                        101,
                        702,
                        "second",
                        serde_json::json!({"n": 2}),
                        result.clone(),
                    ),
                ],
                vec![http(corr, true, vec![])],
                vec![
                    graph_event(
                        corr,
                        701,
                        91,
                        "first",
                        serde_json::json!({"n": 1}),
                        result.clone(),
                    ),
                    graph_event(corr, 702, 91, "second", serde_json::json!({"n": 2}), result),
                ],
            ),
            vec![graph_span(91, corr, None, 0, "request")],
            vec![graph_span(101, corr, None, 0, "request")],
        );

        let card = detect(&artifacts);
        let outcome = &card.per_correlation[0];
        assert_eq!(outcome.scoring_mode, deja_forest::ScoringMode::Graph);
        assert!(
            !outcome
                .alignment
                .as_ref()
                .expect("graph mode publishes alignment")
                .flat_tier_events
                .is_empty(),
            "a many-event node is explicitly routed through the flat tier"
        );
        assert_eq!(card.summary.matched_side_effect_calls, 2);
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert!(card.counter_disagreements().is_empty());
        let rows = build_ledger(&artifacts).expect("multi-event span is valid ledger input");
        assert_eq!(rows.iter().filter(|row| row.kind == "matched").count(), 2);
        assert_eq!(rows.len(), 2, "neither side is counted a second time");
    }

    #[test]
    fn graph_alignment_records_value_difference_before_schema_demotion() {
        let corr = "graph-schema-demotion";
        let sequence = 801;
        let recorded = payment_intent_row(serde_json::Value::Null);
        let observed = payment_intent_row(serde_json::json!("default"));
        let mut event = db_insert_ev(corr, sequence, PAYMENT_INTENT_INSERT, recorded.clone());
        event.graph_node_id = Some(111);
        let mut call = db_exec_obs_with_sql(
            corr,
            sequence,
            PAYMENT_INTENT_INSERT,
            recorded.clone(),
            observed,
        );
        call.graph_node_id = Some(211);
        let artifacts = with_graphs(
            art_with_events(
                vec![seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_insert",
                    sequence,
                    envelope(recorded),
                )],
                vec![call],
                vec![http(corr, true, vec![])],
                vec![event],
            ),
            vec![graph_span(111, corr, None, 0, "db-call")],
            vec![graph_span(211, corr, None, 0, "db-call")],
        );

        let card = detect(&artifacts);
        assert_eq!(card.summary.schema_default_divergences, 1);
        assert_eq!(card.summary.value_divergences, 0);
        assert_eq!(card.summary.side_effect_divergences, 0);
        assert!(card.verdict.pass);
        let aligned = card.per_correlation[0]
            .alignment
            .as_ref()
            .expect("graph mode publishes its scored alignment")
            .nodes
            .iter()
            .find(|node| node.replay_node == Some(211))
            .expect("the divergent graph node is aligned");
        assert_eq!(
            aligned.outcome,
            deja_forest::NodeOutcome::ValueDiverged { origin: true },
            "blocking policy demotes the verdict, not the fact that the values differ"
        );
        let rows = build_ledger(&artifacts).expect("graph schema-demotion ledger builds");
        let row = rows
            .iter()
            .find(|row| row.correlation_id.as_deref() == Some(corr))
            .expect("the graph call has one ledger row");
        assert_eq!(row.kind, "schema_default");
        assert!(!row.blocking);
    }

    #[test]
    fn same_span_order_swap_is_one_blocking_ledger_value_divergence() {
        let corr = "order-ledger";
        let first_seq = 808;
        let second_seq = 809;
        let result = serde_json::json!({"rows_affected": 1});
        let art = art_with_events(
            vec![
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_update",
                    first_seq,
                    envelope(result.clone()),
                ),
                span_entry(Some(corr), first_seq, "root>update_attempt"),
                seq_entry_method_res(
                    Some(corr),
                    "db",
                    "generic_update",
                    second_seq,
                    envelope(result.clone()),
                ),
                span_entry(Some(corr), second_seq, "root>update_attempt"),
            ],
            vec![
                attempt_update_obs(corr, ATTEMPT_UPDATE_STATUS_REKEYED, result.clone()),
                attempt_update_obs(corr, ATTEMPT_UPDATE_STATUS, result.clone()),
            ],
            vec![http(corr, true, vec![])],
            vec![
                attempt_update_ev(corr, first_seq, ATTEMPT_UPDATE_STATUS, result.clone()),
                attempt_update_ev(corr, second_seq, ATTEMPT_UPDATE_STATUS_REKEYED, result),
            ],
        );

        let card = detect(&art);
        assert_eq!(card.summary.value_divergences, 1);
        assert_eq!(card.summary.novel_calls, 0);
        assert_eq!(card.summary.omitted_calls, 0);

        let rows = build_ledger(&art).expect("ledger builds");
        let blocking_value_diverged: Vec<_> = rows
            .iter()
            .filter(|row| row.kind == "value_diverged" && row.blocking)
            .collect();
        assert_eq!(blocking_value_diverged.len(), 1, "rows: {rows:?}");
        assert_eq!(
            blocking_value_diverged[0].source_event_global_sequence,
            Some(first_seq),
            "FIFO pairing remains intact even though the observed args match the later twin"
        );
        assert!(
            rows.iter()
                .all(|row| row.kind != "novel" && row.kind != "omitted"),
            "the two paired calls must not also split into novel/omitted rows: {rows:?}"
        );
    }
    // A same-image replay is the zero calibration; these tests are the other
    // half of the instrument check. Each case changes exactly one observable
    // and pins both the blocking count and the class that explains it.
    mod sensitivity {
        use super::*;

        fn blocking_divergences(card: &Scorecard) -> u64 {
            card.summary.http_status_mismatches
                + card.summary.http_body_mismatches
                + card.summary.side_effect_divergences
        }

        fn assert_one_blocking(
            card: &Scorecard,
            boundary: &str,
            kind: &str,
            protected_signal: &str,
        ) {
            assert_eq!(
                blocking_divergences(card),
                1,
                "{protected_signal}: one injection must produce exactly one blocking divergence"
            );
            assert_eq!(
                kind_count(card, boundary, kind),
                1,
                "{protected_signal}: the scorer must name the expected {kind} class"
            );
            assert!(
                !card.verdict.pass,
                "{protected_signal}: the injected difference must fail the verdict"
            );
        }

        fn schema_default_body_card(
            provenance_corr: Option<&str>,
            response_corr: &str,
            recorded_label: serde_json::Value,
            candidate_label: serde_json::Value,
            body_diff: Vec<JsonFieldDiff>,
        ) -> Scorecard {
            let (entries, observed, events) = match provenance_corr {
                Some(corr) => {
                    let sequence = 808;
                    let recorded = payment_intent_row(recorded_label);
                    let candidate = payment_intent_row(candidate_label);
                    (
                        vec![seq_entry_method_res(
                            Some(corr),
                            "db",
                            "generic_insert",
                            sequence,
                            envelope(recorded.clone()),
                        )],
                        vec![db_exec_obs_with_sql(
                            corr,
                            sequence,
                            PAYMENT_INTENT_INSERT,
                            recorded.clone(),
                            candidate,
                        )],
                        vec![db_insert_ev(
                            corr,
                            sequence,
                            PAYMENT_INTENT_INSERT,
                            recorded,
                        )],
                    )
                }
                None => (vec![], vec![], vec![]),
            };
            detect(&art_with_events(
                entries,
                observed,
                vec![http(response_corr, true, body_diff)],
                events,
            ))
        }

        fn body_diff(
            json_path: &str,
            baseline: serde_json::Value,
            candidate: serde_json::Value,
        ) -> JsonFieldDiff {
            JsonFieldDiff {
                json_path: json_path.to_owned(),
                baseline,
                candidate,
            }
        }

        #[test]
        fn bound_database_column_value_change_deflects_once() {
            let card = schema_default_card(
                Some(PAYMENT_INTENT_INSERT_BINDING_LABEL),
                PAYMENT_INTENT_INSERT_BINDING_LABEL,
                payment_intent_row(serde_json::json!("retail")),
                payment_intent_row(serde_json::json!("wholesale")),
            );

            assert_one_blocking(
                &card,
                "db",
                "ValueDivergedOrigin",
                "bound column `business_label` sensitivity",
            );
            assert_eq!(card.summary.value_divergences, 1);
            assert_eq!(
                card.summary.schema_default_divergences, 0,
                "a bound column is application-owned, never schema-filled"
            );
            assert_eq!(kind_count(&card, "db", "ValueDiverged"), 0);
            assert_eq!(kind_count(&card, "db", "OmittedCall"), 0);
            assert_eq!(kind_count(&card, "db", "NovelCall"), 0);
        }

        #[test]
        fn http_response_status_change_deflects_once() {
            let card = detect(&art(
                vec![],
                vec![],
                vec![http("status-sensitivity", false, vec![])],
            ));

            assert_one_blocking(
                &card,
                "http_incoming",
                "StatusMismatch",
                "HTTP status sensitivity",
            );
            assert_eq!(card.summary.http_status_mismatches, 1);
            assert_eq!(card.summary.http_body_mismatches, 0);
            assert_eq!(card.summary.side_effect_divergences, 0);
            assert_eq!(kind_count(&card, "http_incoming", "BodyMismatch"), 0);
        }

        #[test]
        fn body_field_outside_declared_reply_canon_deflects_once() {
            let corr = "uncovered-body-sensitivity";
            let baseline = serde_json::json!({
                "id": "resp_1",
                "amount": 100,
                "trace_id": "stable",
            });
            let candidate = serde_json::json!({
                "id": "resp_1",
                "amount": 101,
                "trace_id": "stable",
            });
            let card = detect(&art_with_events(
                vec![],
                vec![],
                vec![http_with_bodies(
                    corr,
                    true,
                    vec![JsonFieldDiff {
                        json_path: "$.amount".to_owned(),
                        baseline: serde_json::json!(100),
                        candidate: serde_json::json!(101),
                    }],
                    baseline.clone(),
                    candidate,
                )],
                vec![http_incoming_ev_with_reply_canon(
                    corr,
                    802,
                    Some("project:!trace_id"),
                    baseline,
                )],
            ));

            assert_one_blocking(
                &card,
                "http_incoming",
                "BodyMismatch",
                "HTTP body path `$.amount` outside the declared `trace_id` exclusion",
            );
            assert_eq!(card.summary.http_body_mismatches, 1);
            assert_eq!(card.summary.http_status_mismatches, 0);
            assert_eq!(card.summary.side_effect_divergences, 0);
            assert_eq!(kind_count(&card, "http_incoming", "StatusMismatch"), 0);
        }

        #[test]
        fn recorded_call_absent_from_observed_side_deflects_once() {
            let corr = "omission-sensitivity";
            let card = detect(&art(
                vec![seq_entry(Some(corr), "db", 803)],
                vec![],
                vec![http(corr, true, vec![])],
            ));

            assert_one_blocking(
                &card,
                "db",
                "OmittedCall",
                "recorded DB call omission sensitivity",
            );
            assert_eq!(card.summary.omitted_calls, 1);
            assert_eq!(card.summary.novel_calls, 0);
            assert_eq!(card.summary.value_divergences, 0);
            assert_eq!(kind_count(&card, "db", "NovelCall"), 0);
            assert_eq!(kind_count(&card, "db", "ValueDiverged"), 0);
            assert_eq!(kind_count(&card, "db", "ValueDivergedOrigin"), 0);
        }

        #[test]
        fn observed_call_without_recorded_counterpart_deflects_once() {
            let corr = "novel-sensitivity";
            let card = detect(&art(
                vec![],
                vec![obs("redis", Some(corr), false, None, None)],
                vec![http(corr, true, vec![])],
            ));

            assert_one_blocking(
                &card,
                "redis",
                "NovelCall",
                "observed Redis call novelty sensitivity",
            );
            assert_eq!(card.summary.novel_calls, 1);
            assert_eq!(card.summary.omitted_calls, 0);
            assert_eq!(card.summary.value_divergences, 0);
            assert_eq!(kind_count(&card, "redis", "OmittedCall"), 0);
            assert_eq!(kind_count(&card, "redis", "ValueDiverged"), 0);
            assert_eq!(kind_count(&card, "redis", "ValueDivergedOrigin"), 0);
        }

        #[test]
        fn same_span_same_statement_order_swap_deflects_once() {
            let corr = "order-sensitivity";
            let first_seq = 804;
            let second_seq = 805;
            let result = serde_json::json!({"rows_affected": 1});
            let art = art_with_events(
                vec![
                    seq_entry_method_res(
                        Some(corr),
                        "db",
                        "generic_update",
                        first_seq,
                        envelope(result.clone()),
                    ),
                    span_entry(Some(corr), first_seq, "root>update_attempt"),
                    seq_entry_method_res(
                        Some(corr),
                        "db",
                        "generic_update",
                        second_seq,
                        envelope(result.clone()),
                    ),
                    span_entry(Some(corr), second_seq, "root>update_attempt"),
                ],
                vec![
                    attempt_update_obs(corr, ATTEMPT_UPDATE_STATUS_REKEYED, result.clone()),
                    attempt_update_obs(corr, ATTEMPT_UPDATE_STATUS, result.clone()),
                ],
                vec![http(corr, true, vec![])],
                vec![
                    attempt_update_ev(corr, first_seq, ATTEMPT_UPDATE_STATUS, result.clone()),
                    attempt_update_ev(corr, second_seq, ATTEMPT_UPDATE_STATUS_REKEYED, result),
                ],
            );
            let card = detect(&art);

            assert_one_blocking(
                &card,
                "db",
                "ValueDiverged",
                "same-span same-statement call-order sensitivity",
            );
            assert_eq!(card.summary.value_divergences, 1);
            assert_eq!(card.summary.omitted_calls, 0);
            assert_eq!(card.summary.novel_calls, 0);
            assert_eq!(kind_count(&card, "db", "OmittedCall"), 0);
            assert_eq!(kind_count(&card, "db", "NovelCall"), 0);
        }

        #[test]
        fn seeded_database_row_readback_change_deflects_once() {
            let corr = "seed-readback-sensitivity";
            let seq = 806;
            let recorded = serde_json::json!({
                "attempt_id": "pay_seeded",
                "status": "charged",
                "amount": 100,
            });
            let observed = serde_json::json!({
                "attempt_id": "pay_seeded",
                "status": "charged",
                "amount": 101,
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
                    Some(recorded_result),
                    envelope(observed),
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

            assert_one_blocking(
                &card,
                "db",
                "ValueDivergedOrigin",
                "seeded row column `amount` readback sensitivity",
            );
            assert_eq!(card.summary.value_divergences, 1);
            assert_eq!(card.summary.schema_default_divergences, 0);
            assert_eq!(kind_count(&card, "db", "ValueDiverged"), 0);
            assert_eq!(kind_count(&card, "db", "OmittedCall"), 0);
            assert_eq!(kind_count(&card, "db", "NovelCall"), 0);
        }

        #[test]
        fn established_same_correlation_body_path_absorbs_and_is_named_counted() {
            let corr = "same-correlation-body-provenance";
            for (direction, recorded, candidate) in [
                (
                    "NULL to default",
                    serde_json::Value::Null,
                    serde_json::json!("default"),
                ),
                (
                    "default to NULL",
                    serde_json::json!("default"),
                    serde_json::Value::Null,
                ),
            ] {
                let card = schema_default_body_card(
                    Some(corr),
                    corr,
                    recorded.clone(),
                    candidate.clone(),
                    vec![body_diff("$.business_label", recorded, candidate)],
                );

                assert_eq!(
                    blocking_divergences(&card),
                    0,
                    "{direction}: the provenanced response leaf is non-blocking"
                );
                assert_eq!(
                    kind_count(&card, "db", "SchemaDefaultDivergence"),
                    1,
                    "{direction}: the establishing DB occurrence remains counted"
                );
                assert_eq!(
                    kind_count(&card, "http_incoming", "SchemaDefaultDivergence"),
                    1,
                    "{direction}: the absorbed response leaf is counted separately"
                );
                assert_eq!(kind_count(&card, "http_incoming", "BodyMismatch"), 0);
                assert_eq!(card.summary.schema_default_divergences, 2);
                assert_eq!(card.summary.http_body_mismatches, 0);
                assert_eq!(card.summary.http_status_mismatches, 0);
                assert_eq!(card.summary.side_effect_divergences, 0);
                assert_eq!(card.summary.value_divergences, 0);
                assert!(
                    card.warnings.iter().any(|warning| {
                        warning.contains("$.business_label") && warning.contains("schema-derived")
                    }),
                    "{direction}: absorbed response evidence must name its JSON path: {:?}",
                    card.warnings
                );
                assert!(card.verdict.pass, "{direction}: {}", card.verdict.reason);
                assert!(card.counter_disagreements().is_empty());
            }
        }

        #[test]
        fn same_field_proven_only_in_another_correlation_blocks() {
            for (direction, recorded, candidate) in [
                (
                    "NULL to default",
                    serde_json::Value::Null,
                    serde_json::json!("default"),
                ),
                (
                    "default to NULL",
                    serde_json::json!("default"),
                    serde_json::Value::Null,
                ),
            ] {
                let card = schema_default_body_card(
                    Some("database-provenance-correlation"),
                    "response-correlation",
                    recorded.clone(),
                    candidate.clone(),
                    vec![body_diff("$.business_label", recorded, candidate)],
                );

                assert_one_blocking(&card, "http_incoming", "BodyMismatch", direction);
                assert_eq!(
                    kind_count(&card, "db", "SchemaDefaultDivergence"),
                    1,
                    "{direction}: the unrelated DB occurrence is still counted"
                );
                assert_eq!(
                    kind_count(&card, "http_incoming", "SchemaDefaultDivergence"),
                    0,
                    "{direction}: provenance must not cross correlations"
                );
                assert_eq!(card.summary.schema_default_divergences, 1);
                assert_eq!(card.summary.http_body_mismatches, 1);
                assert_eq!(card.summary.http_status_mismatches, 0);
                assert_eq!(card.summary.side_effect_divergences, 0);
                assert_eq!(card.summary.value_divergences, 0);
                assert!(card.counter_disagreements().is_empty());
            }
        }

        #[test]
        fn body_path_without_schema_default_provenance_blocks() {
            for (direction, recorded, candidate) in [
                (
                    "NULL to default",
                    serde_json::Value::Null,
                    serde_json::json!("default"),
                ),
                (
                    "default to NULL",
                    serde_json::json!("default"),
                    serde_json::Value::Null,
                ),
            ] {
                let card = schema_default_body_card(
                    None,
                    "no-body-provenance",
                    recorded.clone(),
                    candidate.clone(),
                    vec![body_diff("$.business_label", recorded, candidate)],
                );

                assert_one_blocking(&card, "http_incoming", "BodyMismatch", direction);
                assert_eq!(kind_count(&card, "db", "SchemaDefaultDivergence"), 0);
                assert_eq!(
                    kind_count(&card, "http_incoming", "SchemaDefaultDivergence"),
                    0
                );
                assert_eq!(card.summary.schema_default_divergences, 0);
                assert_eq!(card.summary.http_body_mismatches, 1);
                assert_eq!(card.summary.http_status_mismatches, 0);
                assert_eq!(card.summary.side_effect_divergences, 0);
                assert_eq!(card.summary.value_divergences, 0);
                assert!(card.counter_disagreements().is_empty());
            }
        }

        #[test]
        fn mixed_provenanced_and_unprovenanced_body_paths_blocks() {
            let corr = "mixed-body-provenance";
            for (direction, recorded, candidate) in [
                (
                    "NULL to default",
                    serde_json::Value::Null,
                    serde_json::json!("default"),
                ),
                (
                    "default to NULL",
                    serde_json::json!("default"),
                    serde_json::Value::Null,
                ),
            ] {
                let card = schema_default_body_card(
                    Some(corr),
                    corr,
                    recorded.clone(),
                    candidate.clone(),
                    vec![
                        body_diff("$.business_label", recorded.clone(), candidate.clone()),
                        body_diff("$.currency", recorded, candidate),
                    ],
                );

                assert_one_blocking(&card, "http_incoming", "BodyMismatch", direction);
                assert_eq!(
                    kind_count(&card, "db", "SchemaDefaultDivergence"),
                    1,
                    "{direction}: the establishing DB occurrence remains counted"
                );
                assert_eq!(
                    kind_count(&card, "http_incoming", "SchemaDefaultDivergence"),
                    1,
                    "{direction}: the provenanced response leaf remains counted while its sibling blocks"
                );
                assert_eq!(card.summary.schema_default_divergences, 2);
                assert_eq!(card.summary.http_body_mismatches, 1);
                assert_eq!(card.summary.http_status_mismatches, 0);
                assert_eq!(card.summary.side_effect_divergences, 0);
                assert_eq!(card.summary.value_divergences, 0);
                assert!(
                    card.warnings.iter().any(|warning| {
                        warning.contains("$.business_label") && warning.contains("schema-derived")
                    }),
                    "{direction}: absorbed response evidence must survive a blocking sibling: {:?}",
                    card.warnings
                );
                assert!(!card.verdict.pass, "{direction}: mixed body must block");
                assert!(card.counter_disagreements().is_empty());
            }
        }

        #[test]
        fn literal_default_column_difference_absorbs_without_blocking() {
            let card = schema_default_card(
                None,
                PAYMENT_INTENT_INSERT,
                payment_intent_row(serde_json::Value::Null),
                payment_intent_row(serde_json::json!("default")),
            );

            assert_eq!(
                blocking_divergences(&card),
                0,
                "literal DEFAULT provenance rule: schema-filled `business_label` must not block"
            );
            assert_eq!(
                kind_count(&card, "db", "SchemaDefaultDivergence"),
                1,
                "literal DEFAULT provenance rule must remain explicitly named"
            );
            assert_eq!(card.summary.schema_default_divergences, 1);
            assert_eq!(card.summary.value_divergences, 0);
            assert_eq!(kind_count(&card, "db", "ValueDivergedOrigin"), 0);
            assert_eq!(kind_count(&card, "db", "ValueDiverged"), 0);
            assert!(card.verdict.pass, "{}", card.verdict.reason);
        }

        #[test]
        fn reply_canon_excluded_body_path_absorbs_without_blocking() {
            let card = http_reply_canon_card(
                "reply-canon-absorption-sensitivity",
                807,
                "project:!trace_id",
                "$.trace_id",
                serde_json::json!({
                    "id": "resp_1",
                    "amount": 100,
                    "trace_id": "recorded",
                }),
                serde_json::json!({
                    "id": "resp_1",
                    "amount": 100,
                    "trace_id": "observed",
                }),
            );

            assert_eq!(
                blocking_divergences(&card),
                0,
                "declared reply-canon exclusion rule: `$.trace_id` must be absorbed"
            );
            assert_eq!(card.summary.http_body_mismatches, 0);
            assert_eq!(kind_count(&card, "http_incoming", "BodyMismatch"), 0);
            assert_eq!(card.summary.http_status_mismatches, 0);
            assert_eq!(card.summary.side_effect_divergences, 0);
            assert!(card.verdict.pass, "{}", card.verdict.reason);
        }
    }
}
