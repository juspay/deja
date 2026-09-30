//! Per-call divergence ledger — the persisted detail the scorecard summary
//! drops.
//!
//! The scorecard answers "did it pass and by how much" (counts + per-correlation
//! booleans). The ledger answers "WHAT differed, HOW, and WHERE" for every
//! side-effect call, so a UI can render an interactive recorded-vs-observed diff
//! without re-deriving anything. It reconciles three things:
//!
//!   - the RECORDED side: the recording's `BoundaryEvent`s (full args, result,
//!     callsite, graph node), keyed by `global_sequence`
//!   - the OBSERVED side: the candidate's `ObservedCall`s (args, resolution,
//!     and — post-enrichment — callsite, span path, replay graph node)
//!   - the EXPECTED set: which `global_sequence`s the lookup table covers
//!     (so http_incoming and uncovered events aren't miscounted as omitted)
//!
//! Classification mirrors `detect()` exactly so the ledger and the scorecard
//! never disagree:
//!   resolved              → matched (or `recovered` if it only hit rank 6)
//!   unresolved, egress    → environmental (tolerated)
//!   unresolved, pure/req  → deterministic (tolerated)
//!   unresolved, blocking  → novel
//!     …after a truncated recording tail → inconclusive_tail_gap (tolerated)
//!   recorded ∧ unconsumed → omitted, or why it was not re-made:
//!     inside a served call → nested_in_served_call; under a span the replay
//!     never reached → pruned_subtree; cut off by a seed gap →
//!     inconclusive_seed_gap_cascade
//!
//! Each row carries `blocking` so the UI can show the same pass/fail split the
//! verdict used. `kind` alone does NOT: an `omitted` row may be a divergence the
//! verdict acted on or one it tolerated, and the scorecard reports those as two
//! numbers (`omitted_calls` and `omitted_calls_tolerated`) for exactly that
//! reason. Counting `kind == "omitted"` rows and comparing that total to the
//! headline is how a report came to give two answers for one run.
//!
//! The value of a *matched* side-effect call is identical on both sides by
//! construction (replay substitutes the recorded result), so the row shows both
//! sides for context; the genuine value divergence lives in the HTTP diff stream
//! and in the novel/omitted set-deltas.

use std::collections::{HashMap, HashSet};

use deja::{BoundaryEvent, Locus, ObservedCall, Payload};
use serde::{Deserialize, Serialize};

use super::{
    args_free_effective_values, correlation_column_provenance, event_reply_canon_kind,
    observed_miss_is_excused, observed_schema_default_divergence, observed_value_diverged,
    omission_is_blocking, schema_default_divergence, tier_for, ArgDivergenceReach,
    GraphScoringPlan, InconclusiveRaceEvidence, ReachPlacement, SchemaDefaultVerdict, ServeReach,
    TailGapEvidence, Tier, POSITIONAL_FALLBACK_RANK,
};

/// One side (recorded or observed) of a call, with everything a diff/graph UI
/// needs: the value, where it happened, and which graph node it sits under.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CallSide {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_error: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_file: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_line: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_column: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub graph_node_id: Option<u64>,
}

impl CallSide {
    fn is_empty(&self) -> bool {
        self.args.is_none()
            && self.result.is_none()
            && self.call_file.is_none()
            && self.span_path.is_none()
            && self.graph_node_id.is_none()
    }
    fn or_none(self) -> Option<Self> {
        if self.is_empty() {
            None
        } else {
            Some(self)
        }
    }
}

/// One reconciled call: its identity, classification, and both sides.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_event_global_sequence: Option<u64>,
    /// The recorded event actually served by the replay lookup ladder, where it
    /// is not `source_event_global_sequence`. Present for two reasons, told
    /// apart by `kind`: on an identity skew, graph alignment found that serving
    /// and structural identity differ, and `source_event_global_sequence`
    /// remains the structurally aligned event; on a call served by its address
    /// alone, it is the recording the candidate took the value from, and
    /// `source_event_global_sequence` is the twin the pairing judged it against.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub served_event_global_sequence: Option<u64>,
    pub boundary: String,
    pub trait_name: String,
    pub method_name: String,
    /// matched | recovered | served_recorded_error | novel | novel_absorbed | inconclusive_seed_gap |
    /// inconclusive_tail_gap | omitted | environmental | deterministic |
    /// value_diverged | idempotent_delete | inconclusive_race | schema_default |
    /// identity_skew | pruned_subtree | novel_subtree | nested_in_served_call |
    /// inconclusive_seed_gap_cascade
    ///
    /// A recorded call nothing claimed takes exactly one of four kinds, and they
    /// are four different reasons it did not run:
    ///
    /// - `nested_in_served_call`: it ran INSIDE a recorded call the replay
    ///   served from the recording, so the served call's body never executed.
    ///   Substitution working as intended, not a finding. `served_ancestor`
    ///   names the call.
    /// - `pruned_subtree`: the span it ran under never opened on the replay
    ///   side, and no served call accounts for that: the candidate's path left
    ///   the recording's above it, by a stop, a divergence or another branch.
    /// - `omitted`: its span did run (or structure could not say), and the
    ///   candidate made no such call there.
    /// - `inconclusive_seed_gap_cascade`: cut off by the correlation's seed gap.
    ///
    /// Which of them counts toward the verdict is `blocking`, which the
    /// scorecard decides; the kind only says why the call is absent.
    ///
    /// Every kind the scorecard tolerates is non-blocking HERE too: this row is
    /// what the viewer routes on, and the scorecard and the ledger are two
    /// answers to one question. `identity_skew`, `novel_absorbed` and
    /// `inconclusive_seed_gap` were once blocking here while charged to nothing
    /// there, and the viewer showed the wrong answer.
    pub kind: String,
    /// Whether this row counts toward the fail verdict (mirrors the scorecard).
    pub blocking: bool,
    /// `true` on the ORIGIN of a cascade, `false` elsewhere. Lets the UI render
    /// origin -> consequence instead of a list of peers.
    ///
    /// For a `value_diverged` row: the executed read whose value differed from
    /// the baseline. For a `novel` / `novel_subtree` row: an added call with a
    /// divergence after it in the same correlation. Absent on every other
    /// kind.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub origin: bool,
    /// The candidate's request STOPPED at this call: a `Substitute` boundary
    /// missed the tape (or its hit would not rebuild) and failed closed, so the
    /// call never ran and there is no replayed result to show. The finding is
    /// in the ARGUMENTS — what the candidate asked for that the recording does
    /// not hold. Absent when the call went through.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub stopped: bool,
    /// On a `nested_in_served_call` row, the recorded call this one ran inside.
    /// The replay served that call from the recording, so its body did not run
    /// and this call with it. Absent on every other kind.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_ancestor: Option<ServedAncestor>,
    /// This call was served the recording's value because only its ARGUMENTS
    /// moved, and this is how far that substitution could have carried. A reach
    /// of zero is an empty reach — nothing followed the serve — and is reported,
    /// not omitted. Absent on every row that is not such a serve.
    ///
    /// The serve's own divergence is in `kind` and `blocking`; this field adds
    /// no charge. See `ArgDivergenceReach`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg_divergence_reach: Option<ServeReach>,
    /// An argument-divergent serve ran earlier in this call's correlation, so the
    /// substituted value COULD have reached this work. It is not a claim that it
    /// did, nor that anything divergent was observed here: deja records the
    /// execution graph, not value provenance. This row keeps its own `kind` and
    /// its own `blocking`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub could_be_affected_by_arg_divergence: bool,
    /// Where the graph puts this call relative to that serve — inside its span,
    /// outside it, or nowhere it could say. Three states rather than a flag,
    /// because "outside the serve's span" is a structural claim and a flat-scored
    /// correlation is the absence of one, and a reader who cannot tell those apart
    /// is being told the run decided something it never looked at. Absent on a row
    /// nothing reached.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arg_divergence_placement: Option<ReachPlacement>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_rank: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded: Option<CallSide>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed: Option<CallSide>,
}

/// The recorded call a `nested_in_served_call` row ran inside: enough to name
/// it without a second lookup, and its sequence to find its own row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServedAncestor {
    pub global_sequence: u64,
    pub boundary: String,
    pub method_name: String,
    pub call_file: String,
    pub call_line: u32,
}

/// Which unclaimed recorded calls ran inside a call the replay SERVED.
///
/// A served call returns the recording's value without running its body, so
/// every boundary call its body made on the recording side has no counterpart
/// by construction. Structure alone labels those calls `pruned_subtree` when
/// the body opened a span of its own and `omitted` when it did not, and both
/// read as the candidate going missing when nothing failed.
///
/// Nesting is read off the recording: the recorder takes a call's sequence and
/// start time before the call runs and its end time after, so a call made from
/// inside another starts no earlier, ends no later, and holds a later sequence,
/// on the same task of the same correlation. Nothing here knows what any
/// boundary is. What the tape cannot separate is two futures polled
/// concurrently on one task: a call from one of them that falls inside a served
/// call of the other is read as nested in it. The kind it is given then says
/// the call was not re-made, which is still true; `blocking` is unaffected.
///
/// A call is served when its value came from the recording and the request
/// went on: resolved, or served by address with other arguments, and neither
/// run for real (`Shadow`) nor stopped.
pub(crate) struct ServedNesting<'a> {
    served: HashMap<(Option<&'a str>, Option<&'a str>), Vec<&'a BoundaryEvent>>,
}

/// A served call's recorded window, when the tape stamped one. A tape written
/// before `end_timestamp_ns` carries only a duration truncated to microseconds,
/// too coarse to tell a call made inside from one made just after, so such a
/// call contains nothing and its neighbours keep their structural kind.
fn recorded_window(ev: &BoundaryEvent) -> Option<(u64, u64)> {
    let end = ev.end_timestamp_ns?;
    (ev.timestamp_ns > 0 && end > ev.timestamp_ns).then_some((ev.timestamp_ns, end))
}

impl<'a> ServedNesting<'a> {
    pub(crate) fn build(
        observed: &[ObservedCall],
        by_seq: &HashMap<u64, &'a BoundaryEvent>,
    ) -> Self {
        let mut served: HashMap<(Option<&'a str>, Option<&'a str>), Vec<&'a BoundaryEvent>> =
            HashMap::new();
        for obs in observed {
            let from_recording = obs.resolved || obs.arg_divergent;
            if !from_recording
                || obs.provenance == deja::Provenance::Shadow
                || obs.outcome != deja::SubstituteOutcome::Substituted
            {
                continue;
            }
            let sequence = obs
                .served_event_global_sequence
                .or(obs.source_event_global_sequence);
            let Some(ev) = sequence.and_then(|seq| by_seq.get(&seq).copied()) else {
                continue;
            };
            if recorded_window(ev).is_none() {
                continue;
            }
            served
                .entry((ev.correlation_id.as_deref(), ev.task_id.as_deref()))
                .or_default()
                .push(ev);
        }
        for calls in served.values_mut() {
            calls.sort_by_key(|ev| (ev.timestamp_ns, ev.global_sequence));
            calls.dedup_by_key(|ev| ev.global_sequence);
        }
        Self { served }
    }

    /// The innermost served call `ev` ran inside, if any.
    pub(crate) fn ancestor_of(&self, ev: &BoundaryEvent) -> Option<&'a BoundaryEvent> {
        ev.correlation_id.as_ref()?;
        let calls = self
            .served
            .get(&(ev.correlation_id.as_deref(), ev.task_id.as_deref()))?;
        let start = ev.timestamp_ns;
        let end = ev.end_timestamp_ns.unwrap_or(start);
        let started_before = calls.partition_point(|parent| parent.timestamp_ns <= start);
        // Latest start first, so the first that contains `ev` is the innermost.
        calls[..started_before]
            .iter()
            .rev()
            .copied()
            .find(|parent| {
                parent.global_sequence < ev.global_sequence
                    && recorded_window(parent).is_some_and(|(_, parent_end)| end <= parent_end)
            })
    }
}

/// How the unclaimed recorded calls were labelled, asserted to cover them all.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct UnclaimedTally {
    pub nested_in_served_call: usize,
    pub pruned_subtree: usize,
    pub omitted: usize,
    pub seed_gap_cascade: usize,
    /// Covered by the table, unclaimed, and absent from the loaded tape, so
    /// there is no recorded side to write a row from.
    pub no_recorded_event: usize,
}

impl UnclaimedTally {
    fn total(self) -> usize {
        self.nested_in_served_call
            + self.pruned_subtree
            + self.omitted
            + self.seed_gap_cascade
            + self.no_recorded_event
    }
}

/// The ledger's snake_case name for a schema-derived INSERT row.
fn schema_default_row_kind(_scorecard_kind: &str) -> String {
    "schema_default".to_owned()
}

fn recorded_side(ev: &BoundaryEvent) -> CallSide {
    CallSide {
        args: Some(ev.args.to_value()),
        result: Some(ev.result.to_value()),
        is_error: Some(ev.is_error),
        call_file: Some(ev.call_file.clone()),
        call_line: Some(ev.call_line),
        call_column: Some(ev.call_column),
        // The recorded logical span path lives on the rank-2 lookup address,
        // not the event; the graph node is the event's own.
        span_path: None,
        graph_node_id: ev.graph_node_id,
    }
}

fn observed_side(obs: &ObservedCall) -> CallSide {
    // For a value-divergence the candidate ran the REAL boundary, so its
    // `observed_result` IS an independent value (e.g. 0.20 vs recorded 0.10) and
    // must be carried so the Calls tab + graph can show old -> new. For a plain
    // (substituted) match the observed value equals the recorded result by
    // construction, so we leave `result` to the recorded side to avoid implying
    // an independent value.
    let result = if obs.provenance == deja::Provenance::Shadow
        && obs.observed_result.is_some()
        && obs.observed_result != obs.recorded_result
    {
        obs.observed_result.clone()
    } else {
        None
    };
    CallSide {
        args: Some(obs.args.clone()),
        result,
        is_error: None,
        call_file: obs.call_file.clone(),
        call_line: obs.call_line,
        call_column: obs.call_column,
        span_path: obs.span_path.clone(),
        graph_node_id: obs.graph_node_id,
    }
}

fn stopped_at(obs: &ObservedCall) -> bool {
    obs.outcome == deja::SubstituteOutcome::Stopped
}

/// Per correlation, the EARLIEST position at which an executed boundary
/// returned a value differing from the recorded baseline — the origins a later
/// re-keyed call can be a consequence of.
///
/// A re-keyed call (one whose arguments miss the tape) is a consequence only
/// when there is something upstream for it to be a consequence of. Absent
/// that, the changed arguments are themselves the finding — the candidate built
/// a different request from the same inputs — and the row is the origin. The
/// ledger used to label every such call a consequence and the viewer then said
/// "the cause is at an origin above it" about rows with no origin anywhere.
/// The POSITION is what makes "upstream" mean upstream. Keyed on the
/// correlation alone, a divergence occurring AFTER the re-keyed call demotes it
/// to a consequence of a cause that had not happened yet — the same complaint
/// this function exists to answer, with the order reversed.
fn correlations_with_a_value_origin(
    observed: &[ObservedCall],
    by_seq: &HashMap<u64, &BoundaryEvent>,
) -> HashMap<String, usize> {
    let mut earliest: HashMap<String, usize> = HashMap::new();
    for (index, obs) in observed.iter().enumerate() {
        let source = obs
            .source_event_global_sequence
            .and_then(|seq| by_seq.get(&seq).copied());
        if !observed_value_diverged(obs, source) {
            continue;
        }
        if let Some(id) = obs.correlation_id.clone() {
            earliest.entry(id).or_insert(index);
        }
    }
    earliest
}

/// The index of each correlation's last divergence — what an earlier novel call
/// in that correlation can have caused.
///
/// A novel call enters only by STOPPING the request, and a request stops once,
/// so novel calls cannot form a chain of each other's causes.
/// Whether the call at `index` is an args-free PAIRED divergence: it claimed a
/// recorded twin by locus rather than by args, and its request changed.
///
/// Read from one place by the row builder and by the attribution map below.
/// They answer the same question, and a ledger whose rows and whose cascade
/// disagree about what diverged is the failure `CallPairing` exists to prevent.
fn paired_value_diverged(
    obs: &ObservedCall,
    index: usize,
    by_seq: &HashMap<u64, &BoundaryEvent>,
    pairing: &super::CallPairing,
) -> bool {
    let Some(twin) = pairing.twin(index) else {
        return false;
    };
    let twin_event = by_seq.get(&twin.sequence).copied();
    pairing.changed(
        index,
        matches!(
            super::pair_request_verdict(obs, twin_event),
            super::ValueVerdict::Diverged
        ),
        twin.order_mismatch,
    )
}

fn last_divergence_per_correlation(
    observed: &[ObservedCall],
    by_seq: &HashMap<u64, &BoundaryEvent>,
    tail_gap: &TailGapEvidence,
    pairing: &super::CallPairing,
) -> HashMap<String, usize> {
    let mut last: HashMap<String, usize> = HashMap::new();
    for (index, obs) in observed.iter().enumerate() {
        let Some(corr) = obs.correlation_id.as_deref() else {
            continue;
        };
        if tail_gap.covers(Some(corr), index) || obs.seed_gap {
            // No baseline for this call, so it is not evidence of anything.
            continue;
        }
        let source = obs
            .source_event_global_sequence
            .and_then(|seq| by_seq.get(&seq).copied());
        // Both signals have a position in the observed stream, which is what
        // attribution needs. An omitted call is a divergence too, but it has no
        // observed counterpart and so no index to be after.
        // The paired arm is not a widening: an args-free pair is the SHAPE
        // most divergences take — a call whose input changed, which the
        // address ladder could not bind — and the map that decides what an
        // added call caused could not see any of them.
        if observed_value_diverged(obs, source)
            || stopped_at(obs)
            || paired_value_diverged(obs, index, by_seq, pairing)
        {
            last.insert(corr.to_owned(), index);
        }
    }
    last
}

/// Build the per-call ledger from the recording's events (recorded side), the
/// candidate's observed calls, and the lookup table.
///
/// The table is passed whole rather than pre-digested into "the covered
/// sequences" + "the span path per sequence" + "the pairing pool". Those three
/// are one fact read three ways, and a caller that can hand over a mismatched
/// set of them is a caller that can make the ledger disagree with the scorecard
/// — which is the bug this signature was tightened to prevent.
pub fn build(
    events: &[BoundaryEvent],
    observed: &[ObservedCall],
    table: &deja::LookupTable,
    idempotent_delete_demote: &HashSet<u64>,
) -> Vec<CallRecord> {
    build_with_inconclusive(
        events,
        observed,
        table,
        idempotent_delete_demote,
        &InconclusiveRaceEvidence::default(),
        &TailGapEvidence::default(),
    )
}

/// Collecting wrapper over [`build_with_inconclusive_into`], for a caller that
/// wants every row in memory and has no graph plan to offer.
///
/// That is the test fixtures and nothing else. Production builds the ledger
/// through `build_with_plan_into` WITH a plan, and the API serves the streamed
/// artifact rather than calling either — so rows from here differ from shipped
/// rows on whatever the plan contributes, `arg_divergence_placement` included.
/// This comment used to name `/calls` as the caller, which made that gap look
/// live when it is confined to older fixtures.
pub(crate) fn build_with_inconclusive(
    events: &[BoundaryEvent],
    observed: &[ObservedCall],
    table: &deja::LookupTable,
    idempotent_delete_demote: &HashSet<u64>,
    inconclusive_race: &InconclusiveRaceEvidence,
    tail_gap: &TailGapEvidence,
) -> Vec<CallRecord> {
    let mut rows = Vec::new();
    let _ = build_with_inconclusive_into(
        events,
        observed,
        table,
        idempotent_delete_demote,
        inconclusive_race,
        tail_gap,
        &super::UnplantedPresence::default(),
        &super::SeedGapCascade::default(),
        None,
        &mut |row| {
            rows.push(row);
            Ok(())
        },
    );
    rows
}

/// Emit each ledger row to `sink` as it is produced.
///
/// Streaming rather than returning a `Vec<CallRecord>`: every resolved row
/// carries the recorded side's full `args` and `result`, so a run with
/// thousands of resolved calls held a second copy of its own recording in
/// memory before a byte reached disk. That OOMKilled the runner at 16 GiB on a
/// 287-correlation tape, while the SAME tape scored fine for a candidate whose
/// rows were overwhelmingly payload-free — 82 resolved calls against thousands.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_with_inconclusive_into(
    events: &[BoundaryEvent],
    observed: &[ObservedCall],
    table: &deja::LookupTable,
    idempotent_delete_demote: &HashSet<u64>,
    inconclusive_race: &InconclusiveRaceEvidence,
    tail_gap: &TailGapEvidence,
    unplanted: &super::UnplantedPresence,
    cascade: &super::SeedGapCascade,
    plan: Option<&GraphScoringPlan>,
    sink: &mut dyn FnMut(CallRecord) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let expected_seqs = &expected_sequences(table);
    let span_paths = &recorded_span_paths(table);
    let by_seq: HashMap<u64, &BoundaryEvent> =
        events.iter().map(|e| (e.global_sequence, e)).collect();
    // Same index the scorecard builds, from the same two streams, so a
    // schema-derived row and a schema-derived count cannot come apart.
    let column_provenance = correlation_column_provenance(events, observed);
    let recorded_for = |seq: u64| -> Option<CallSide> {
        by_seq.get(&seq).map(|ev| {
            let mut side = recorded_side(ev);
            side.span_path = span_paths.get(&seq).cloned();
            side
        })
    };

    let mut consumed: HashSet<u64> = HashSet::new();
    let value_origins = correlations_with_a_value_origin(observed, &by_seq);

    // How every call pairs, decided once from the addresses and shared with the
    // scorecard (see `CallPairing`): a resolved call with the event it served, an
    // unresolved one by its address minus its args. A call whose input changed
    // is ONE row, not a novel call beside an omitted one, at whatever tier its
    // boundary sits; and structure never pairs, because a span holding one event
    // on each side says nothing about whether they are one call.
    let pairing = super::CallPairing::build(table, events, observed, &column_provenance);
    // How far each argument-divergent substitution could have carried, from the
    // same seam the scorecard reads so the rows and the counts cannot come apart.
    // Attribution only: it sets no `kind` and no `blocking`.
    let arg_divergence = ArgDivergenceReach::build(observed, plan);
    // Recorded twins claimed by a value_diverged consequence, so the omitted pass
    // doesn't also flag them (collapses the would-be novel+omitted split).
    let mut paired_consumed: HashSet<u64> = HashSet::new();

    // --- observed calls (candidate side) ------------------------------------
    // Enumerated because a truncated-recording tail is a POSITIONAL fact: the
    // call must come after the correlation's last recorded event was reproduced.
    // A novel call before a correlation's last divergence is an origin; one
    // with nothing diverging after it is not a finding at all.
    let last_divergence = last_divergence_per_correlation(observed, &by_seq, tail_gap, &pairing);
    for (observed_index, obs) in observed.iter().enumerate() {
        let arg_divergence_reach = arg_divergence.serve(observed_index);
        let reached = arg_divergence.reached(observed_index);
        let could_be_affected_by_arg_divergence = reached.is_some();
        let arg_divergence_placement = reached.map(|call| call.placement);
        // ORIGIN: an args-aligned executed boundary (typically a READ) whose REAL
        // result differs from the recorded baseline — the cause of a cascade.
        let source_event = obs
            .source_event_global_sequence
            .and_then(|seq| by_seq.get(&seq).copied());
        let is_value_origin = observed_value_diverged(obs, source_event);

        if is_value_origin {
            consumed.extend(obs.source_event_global_sequence);
            let recorded = obs
                .source_event_global_sequence
                .and_then(recorded_for)
                .and_then(CallSide::or_none);
            // Rule B and narrow race recognition demote selected execute
            // divergences to non-blocking rows, mirroring the scorecard.
            let seq = obs.source_event_global_sequence;
            let schema_default = observed_schema_default_divergence(obs, source_event);
            let (kind, blocking) = if let SchemaDefaultVerdict::Confirmed(d) = &schema_default {
                (schema_default_row_kind(d.kind()), false)
            } else if seq.is_some_and(|s| idempotent_delete_demote.contains(&s)) {
                let kind = seq
                    .and_then(|s| by_seq.get(&s).and_then(|ev| event_reply_canon_kind(ev)))
                    .unwrap_or_else(|| "idempotent_delete".to_owned());
                (kind, false)
            } else if seq.is_some_and(|s| inconclusive_race.contains(&s)) {
                ("inconclusive_race".to_owned(), false)
            } else if unplanted.read_by(obs.correlation_id.as_deref(), source_event) {
                ("inconclusive_seed_gap".to_owned(), false)
            } else {
                ("value_diverged".to_owned(), true)
            };
            sink(CallRecord {
                correlation_id: obs.correlation_id.clone(),
                source_event_global_sequence: obs.source_event_global_sequence,
                served_event_global_sequence: None,
                boundary: obs.boundary.clone(),
                trait_name: obs.trait_name.clone(),
                method_name: obs.method_name.clone(),
                kind,
                blocking,
                origin: true,
                stopped: stopped_at(obs),
                served_ancestor: None,
                arg_divergence_reach,
                could_be_affected_by_arg_divergence,
                arg_divergence_placement,
                resolved_rank: obs.resolved_rank,
                recorded,
                observed: observed_side(obs).or_none(),
            })?;
            continue;
        }

        // PAIRED by its address minus its args: one call whose input changed.
        // Emit ONE row (recorded twin + observed) instead of a phantom novel +
        // omitted split. Whether it is the origin or a consequence is decided by
        // what diverged upstream, not by which boundary it is.
        if let Some(twin) = pairing.twin(observed_index) {
            let twin_seq = twin.sequence;
            paired_consumed.insert(twin_seq);
            let mut recorded = recorded_for(twin_seq);
            let mut observed = observed_side(obs);
            // A WRITE returns unit, so the divergence signal lives in its
            // OPERAND, not its result. When both sides' result is empty,
            // surface the diverging `value` argument as the displayed value so
            // the cascade chip reads 0.10 -> 0.20 (the recorded twin's operand
            // vs the executed write's operand) instead of ∅ -> ∅.
            let is_unit =
                |r: &Option<serde_json::Value>| matches!(r, None | Some(serde_json::Value::Null));
            let recorded_unit = recorded.as_ref().is_none_or(|s| is_unit(&s.result));
            if recorded_unit && is_unit(&observed.result) {
                if let Some(v) = obs.args.get("value").cloned() {
                    observed.result = Some(v);
                }
                if let Some(side) = recorded.as_mut() {
                    if let Some(v) = side.args.as_ref().and_then(|a| a.get("value")).cloned() {
                        side.result = Some(v);
                    }
                }
            }
            let recorded_result = by_seq
                .get(&twin_seq)
                .map(|ev| ev.result.clone())
                .unwrap_or(Payload::from(serde_json::Value::Null));
            let twin_event = by_seq.get(&twin_seq).copied();
            let (recorded_val, observed_val) =
                args_free_effective_values(&recorded_result, obs, twin_event);
            let value_diverged = paired_value_diverged(obs, observed_index, &by_seq, &pairing);
            let race_downstream = !twin.order_mismatch
                && !obs.arg_divergent
                && value_diverged
                && inconclusive_race
                    .attributable_downstream(obs.correlation_id.as_deref(), &obs.args);
            // Rule C on the args-free arm, mirroring the scorecard: the two
            // statements say the schema filled every differing column.
            // Exact later-args evidence is instead always value-diverged and
            // blocking; equivalent result envelopes cannot absorb the swap.
            let schema_default = (value_diverged && !twin.order_mismatch && !obs.arg_divergent)
                .then(|| {
                    schema_default_divergence(
                        &obs.boundary,
                        twin_event
                            .and_then(|ev| ev.args.get("sql"))
                            .and_then(|s| s.as_str()),
                        obs.args.get("sql").and_then(|s| s.as_str()),
                        &recorded_val,
                        &observed_val,
                    )
                })
                .and_then(|verdict| match verdict {
                    SchemaDefaultVerdict::Confirmed(d) => Some(schema_default_row_kind(d.kind())),
                    _ => None,
                });
            sink(CallRecord {
                correlation_id: obs.correlation_id.clone(),
                source_event_global_sequence: Some(twin_seq),
                // The recorded event the candidate served, where it served one
                // by address alone: auditable against the twin paired here.
                served_event_global_sequence: obs.served_event_global_sequence,
                boundary: obs.boundary.clone(),
                trait_name: obs.trait_name.clone(),
                method_name: obs.method_name.clone(),
                kind: match &schema_default {
                    Some(kind) => kind.clone(),
                    None if !value_diverged => "matched".to_owned(),
                    None if race_downstream => "inconclusive_race".to_owned(),
                    None => "value_diverged".to_owned(),
                },
                blocking: value_diverged && schema_default.is_none() && !race_downstream,
                // A consequence needs an origin, and the origin has to
                // come FIRST. With nothing diverged before it in this
                // correlation the re-keyed call is the finding itself.
                origin: value_diverged
                    && !obs.correlation_id.as_deref().is_some_and(|id| {
                        value_origins
                            .get(id)
                            .is_some_and(|first| *first < observed_index)
                    }),
                stopped: stopped_at(obs),
                served_ancestor: None,
                arg_divergence_reach,
                could_be_affected_by_arg_divergence,
                arg_divergence_placement,
                resolved_rank: obs.resolved_rank,
                recorded,
                observed: observed.or_none(),
            })?;
            continue;
        }

        // A resolved call is paired with the event its address served. Where the
        // aligner bound its span crosswise to that event, the row names the skew
        // and still charges it to nothing.
        let skewed = plan.is_some_and(|plan| super::graph_identity_skew(plan, obs).is_some());
        let (kind, blocking) = if obs.resolved {
            consumed.extend(obs.source_event_global_sequence);
            let recovered = obs.resolved_rank == Some(POSITIONAL_FALLBACK_RANK);
            if obs.provenance == deja::Provenance::ServedRecordedError {
                // Named, not "matched": the candidate did not run this call.
                ("served_recorded_error", false)
            } else if skewed {
                ("identity_skew", false)
            } else {
                (if recovered { "recovered" } else { "matched" }, false)
            }
        } else if obs.arg_divergent {
            // Served by its address alone with no recorded call left to pair it
            // with: blocking, as the scorecard's `ArgsServedUnpaired`, ahead of
            // every arm that tolerates a miss.
            ("args_served_unpaired", true)
        } else if plan.is_some_and(|plan| {
            obs.correlation_id
                .as_deref()
                .is_some_and(|id| plan.replay_event_is_novel(id, observed_index))
        }) {
            // Unpaired, inside a subtree the recording never had: structure names
            // where the added work is. It blocks as an unpaired call at this
            // boundary would, unless there is no baseline to judge it against.
            if tail_gap.covers(obs.correlation_id.as_deref(), observed_index) {
                ("inconclusive_tail_gap", false)
            } else if obs.absorbed
                && tier_for(&obs.boundary) != Tier::Environmental
                && !observed_miss_is_excused(obs)
            {
                // As the scorecard's novel-subtree arm: a miss the request
                // survived is an absorbed miss wherever it lands, once it is
                // neither an egress miss nor an excused one, which the
                // scorecard asks about first.
                ("novel_absorbed", false)
            } else {
                (
                    "novel_subtree",
                    tier_for(&obs.boundary) != Tier::Environmental
                        && !observed_miss_is_excused(obs),
                )
            }
        } else if tier_for(&obs.boundary) == Tier::Environmental {
            ("environmental", false)
        } else if observed_miss_is_excused(obs) {
            ("deterministic", false)
        } else if obs.correlation_id.is_none() {
            // uncorrelated background-task novel call — tolerated in V1
            ("novel", false)
        } else if obs.seed_gap {
            // No baseline for the container this call read, so nothing to be
            // novel against. Mirrors the scorecard's `InconclusiveSeedGap`, in
            // the scorecard's own precedence: seed gap before tail gap before
            // an absorbed miss.
            ("inconclusive_seed_gap", false)
        } else if tail_gap.covers(obs.correlation_id.as_deref(), observed_index) {
            // The recording for this correlation stops at request teardown and
            // this call comes after it: no baseline, so neither matched nor
            // novel. Mirrors the scorecard's `InconclusiveTailGap`.
            ("inconclusive_tail_gap", false)
        } else if obs.absorbed {
            // A novel call the process survived on a synthesized value.
            // Named apart from the unabsorbed novel calls and charged to
            // nothing, as the scorecard's `NovelCallAbsorbed` is.
            ("novel_absorbed", false)
        } else {
            // Blocking only when the miss STOPPED the request. A novel call on
            // its own is charged to nothing by the scorecard — adding a call is
            // what a change is — and this row is what the viewer routes on, so
            // the two must agree.
            ("novel", stopped_at(obs))
        };
        // Origin only when a divergence follows it in the same correlation.
        // Not span containment: an added call changes what happens after it
        // returns as much as what happens beneath it.
        let novel_origin = matches!(kind, "novel" | "novel_subtree")
            && obs
                .correlation_id
                .as_deref()
                .and_then(|corr| last_divergence.get(corr))
                .is_some_and(|last| *last > observed_index);
        let recorded = obs
            .source_event_global_sequence
            .and_then(recorded_for)
            .and_then(CallSide::or_none);
        sink(CallRecord {
            correlation_id: obs.correlation_id.clone(),
            source_event_global_sequence: obs.source_event_global_sequence,
            served_event_global_sequence: if skewed {
                obs.source_event_global_sequence
            } else {
                obs.served_event_global_sequence
            },
            boundary: obs.boundary.clone(),
            trait_name: obs.trait_name.clone(),
            method_name: obs.method_name.clone(),
            kind: kind.to_owned(),
            blocking,
            origin: novel_origin,
            stopped: stopped_at(obs),
            served_ancestor: None,
            arg_divergence_reach,
            could_be_affected_by_arg_divergence,
            arg_divergence_placement,
            resolved_rank: obs.resolved_rank,
            recorded,
            observed: observed_side(obs).or_none(),
        })?;
    }

    // --- unclaimed: expected (table-covered) recorded events never consumed ---
    let unclaimed_seqs: Vec<u64> = expected_seqs
        .iter()
        .filter(|s| !consumed.contains(s) && !paired_consumed.contains(s))
        .copied()
        .collect();
    let unclaimed = unclaimed_seqs.len();
    let mut tally = UnclaimedTally::default();
    // A covered sequence with no event on the loaded tape (a tape that failed
    // to load, or lost lines) has no recorded side to write. Named, not dropped.
    let mut omitted: Vec<&BoundaryEvent> = Vec::with_capacity(unclaimed);
    for seq in &unclaimed_seqs {
        match by_seq.get(seq) {
            Some(ev) => omitted.push(ev),
            None => tally.no_recorded_event += 1,
        }
    }
    omitted.sort_by_key(|e| e.global_sequence);
    let nesting = ServedNesting::build(observed, &by_seq);
    for ev in omitted {
        // Claimed by no address. Why it did not run is one of four answers, in
        // this order: cut off by the seed gap; inside a call the replay served,
        // so its body never ran; under a span the replay never reached; or its
        // span ran without it.
        let pruned = plan.is_some_and(|plan| {
            ev.correlation_id
                .as_deref()
                .is_some_and(|id| plan.recorded_event_is_pruned(id, ev.global_sequence))
        });
        let blocking = omission_is_blocking(
            ev.correlation_id.as_deref(),
            &ev.boundary,
            ev.role.as_deref(),
        );
        // Cut off by the correlation's seed gap, mirroring the scorecard.
        let cascaded = blocking && cascade.covers(ev.correlation_id.as_deref(), ev.global_sequence);
        let served_ancestor = if cascaded {
            None
        } else {
            nesting.ancestor_of(ev).map(|parent| ServedAncestor {
                global_sequence: parent.global_sequence,
                boundary: parent.boundary.clone(),
                method_name: parent.method_name.clone(),
                call_file: parent.call_file.clone(),
                call_line: parent.call_line,
            })
        };
        let kind = if cascaded {
            tally.seed_gap_cascade += 1;
            "inconclusive_seed_gap_cascade"
        } else if served_ancestor.is_some() {
            tally.nested_in_served_call += 1;
            "nested_in_served_call"
        } else if pruned {
            tally.pruned_subtree += 1;
            "pruned_subtree"
        } else {
            tally.omitted += 1;
            "omitted"
        };
        sink(CallRecord {
            correlation_id: ev.correlation_id.clone(),
            source_event_global_sequence: Some(ev.global_sequence),
            served_event_global_sequence: None,
            boundary: ev.boundary.clone(),
            trait_name: ev.trait_name.clone(),
            method_name: ev.method_name.clone(),
            kind: kind.to_owned(),
            // The kind says why the call is absent; whether that counts is the
            // scorecard's, and it is not re-decided here.
            blocking: blocking && !cascaded,
            origin: false,
            stopped: false,
            served_ancestor,
            // A recorded event nothing claimed has no position in the candidate's
            // stream, so there is no "after the serve" for it to be in. The
            // reach speaks about the calls the candidate MADE.
            arg_divergence_reach: None,
            could_be_affected_by_arg_divergence: false,
            arg_divergence_placement: None,
            resolved_rank: None,
            recorded: recorded_for(ev.global_sequence),
            observed: None,
        })?;
    }
    // Every recorded call the table covers is claimed by the candidate, written
    // here under exactly one of the four kinds, or named as having no event.
    // An imbalance is returned rather than panicked on: the scorecard is already
    // written by the time the ledger is, and a ledger failure must not end the
    // run that produced it.
    let claimed = expected_seqs
        .iter()
        .filter(|s| consumed.contains(s) || paired_consumed.contains(s))
        .count();
    if tally.total() != unclaimed || claimed + unclaimed != expected_seqs.len() {
        return Err(std::io::Error::other(format!(
            "ledger accounting violation: {} expected recorded calls, {claimed} claimed, \
             {unclaimed} unclaimed, labelled {tally:?}",
            expected_seqs.len()
        )));
    }
    if tally.no_recorded_event > 0 {
        eprintln!(
            "divergence: ledger has no row for {} unclaimed recorded call(s) the lookup table \
             covers but the loaded tape does not carry",
            tally.no_recorded_event
        );
    }

    Ok(())
}

/// Build a ledger with the run's graph plan, through the one builder every
/// correlation shares. Pairing comes from the addresses (`CallPairing`); the plan
/// contributes structure only — which unpaired calls sit in a subtree the
/// recording never had, which unclaimed events sat under one that never ran, and
/// which resolved calls the aligner bound crosswise.
/// Emit each ledger row to `sink` as it is produced.
///
/// Streaming rather than returning a `Vec<CallRecord>`: every resolved row
/// carries the recorded side's full `args` and `result`, so a run with
/// thousands of resolved calls held a second copy of its own recording in
/// memory before a byte reached disk. That OOMKilled the runner at 16 GiB on a
/// 287-correlation tape, while the SAME tape scored fine for a candidate whose
/// rows were overwhelmingly payload-free — 82 resolved calls against thousands.
// Eight because the sink is an eighth parameter on a function that already took
// seven; bundling them into a struct would be a larger change than the one being
// made and would obscure that this is the same function, streaming.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_with_plan_into(
    events: &[BoundaryEvent],
    observed: &[ObservedCall],
    table: &deja::LookupTable,
    idempotent_delete_demote: &HashSet<u64>,
    inconclusive_race: &InconclusiveRaceEvidence,
    tail_gap: &TailGapEvidence,
    unplanted: &super::UnplantedPresence,
    cascade: &super::SeedGapCascade,
    plan: &GraphScoringPlan,
    sink: &mut dyn FnMut(CallRecord) -> std::io::Result<()>,
) -> std::io::Result<()> {
    // One builder over every call. The shared `CallPairing` decides what pairs;
    // the plan only labels what the addresses left unpaired or unclaimed, so a
    // graph-scored correlation and a flat one produce the same rows for the same
    // calls.
    build_with_inconclusive_into(
        events,
        observed,
        table,
        idempotent_delete_demote,
        inconclusive_race,
        tail_gap,
        unplanted,
        cascade,
        Some(plan),
        sink,
    )
}

/// The set of `global_sequence`s the lookup table covers (so http_incoming and
/// any uncovered events are never miscounted as omitted) — mirrors `detect()`'s
/// `expected` keying.
pub fn expected_sequences(table: &deja::LookupTable) -> HashSet<u64> {
    table
        .entries
        .iter()
        .map(|e| e.source_event_global_sequence)
        .collect()
}

/// Logical span path per recorded event, harvested from the rank-2
/// `SpanPath` lookup addresses (the event itself doesn't carry it). Lets
/// the UI align recorded calls onto the record-side execution-graph tree the
/// same way it aligns observed calls via `ObservedCall.span_path`.
pub fn recorded_span_paths(table: &deja::LookupTable) -> HashMap<u64, String> {
    let mut out = HashMap::new();
    for entry in &table.entries {
        if let Locus::SpanPath { path } = &entry.key.locus {
            out.entry(entry.source_event_global_sequence)
                .or_insert_with(|| path.clone());
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    fn event(seq: u64, boundary: &str, corr: Option<&str>) -> BoundaryEvent {
        BoundaryEvent {
            global_sequence: seq,
            request_sequence: 0,
            correlation_id: corr.map(str::to_owned),
            timestamp_ns: 0,
            recording_run_id: Some("rec".to_owned()),
            graph_node_id: Some(seq),
            tracing_span_id: None,
            task_id: Some("root".to_owned()),
            parent_task_id: None,
            task_bucket: Some("root".to_owned()),
            bucket_id: Some("root".to_owned()),
            fork_seq: Some(0),
            boundary: boundary.to_owned(),
            trait_name: "T".to_owned(),
            method_name: "m".to_owned(),
            call_file: "x.rs".to_owned(),
            call_line: 1,
            call_column: 1,
            receiver: None,
            request: Payload::from(serde_json::Value::Null),
            args: serde_json::json!({"k": seq}).into(),
            response: Payload::from(serde_json::Value::Null),
            result: serde_json::json!({"r": seq}).into(),
            is_error: false,
            duration_us: 0,
            event_schema_version: deja::CURRENT_EVENT_SCHEMA_VERSION,
            callsite_identity: None,
            provenance: deja::Provenance::default(),
            fidelity: deja::Fidelity::default(),
            result_image: None,
            pre_image: None,
            read_set: Vec::new(),
            write_set: Vec::new(),
            value_digest: None,
            entropy_source: None,
            replay_strategy: deja::ReplayStrategy::default(),
            kind: None,
            role: None,
            declaration: None,
            raw_draw: None,
            end_timestamp_ns: None,
        }
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
            args: serde_json::json!({"obs": true}),
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
            call_file: Some("y.rs".to_owned()),
            call_line: Some(9),
            call_column: Some(2),
            span_path: Some("root>handler".to_owned()),
            graph_node_id: Some(42),
            synthesized: false,
            outcome: deja::SubstituteOutcome::default(),
            real_impl_will_fail: false,
            recorded_result: None,
            observed_result: None,
            provenance: deja::Provenance::default(),
            seed_gap: false,
            absorbed: false,
            arg_divergent: false,
            served_event_global_sequence: None,
            lookup_ordinal: None,
            cancelled: false,
        }
    }

    fn find<'a>(rows: &'a [CallRecord], kind: &str) -> Vec<&'a CallRecord> {
        rows.iter().filter(|r| r.kind == kind).collect()
    }

    /// A lookup table shaped like the one a real recording writes for `events`:
    /// the rank-6 `Sequence` address every event emits, plus the rank-2
    /// `SpanPath` address for each span the run harvested. The args-free pairing
    /// pool is addressed off this table, so a fixture that omits it is a fixture
    /// in which no observed call has a twin it is entitled to claim.
    fn table_for(events: &[BoundaryEvent], spans: &HashMap<u64, String>) -> deja::LookupTable {
        let mut entries = Vec::new();
        for ev in events {
            let key = |locus| deja::LookupKey {
                correlation_id: ev.correlation_id.clone(),
                bucket_id: ev.bucket_id.clone(),
                boundary: ev.boundary.clone(),
                component: ev.trait_name.clone(),
                operation: ev.method_name.clone(),
                fork_seq: 0,
                locus,
                args_hash: 0,
                occurrence: 0,
            };
            entries.push(deja::LookupEntry {
                key: key(Locus::Unlocated),
                result: std::sync::Arc::new(ev.result.to_value()),
                source_event_global_sequence: ev.global_sequence,
            });
            if let Some(path) = spans.get(&ev.global_sequence) {
                entries.push(deja::LookupEntry {
                    key: key(Locus::SpanPath { path: path.clone() }),
                    result: std::sync::Arc::new(ev.result.to_value()),
                    source_event_global_sequence: ev.global_sequence,
                });
            }
        }
        deja::LookupTable {
            recording_id: "rec".to_owned(),
            policy_version: deja::POLICY_VERSION,
            event_schema_version: Some(deja::CURRENT_EVENT_SCHEMA_VERSION),
            entries,
            identity_entries: Vec::new(),
        }
    }

    /// A call whose executed result differs from the recorded baseline.
    fn diverging(boundary: &str, corr: Option<&str>, src: u64) -> ObservedCall {
        let mut o = obs(boundary, corr, true, Some(6), Some(src));
        o.provenance = deja::Provenance::Shadow;
        o.recorded_result = Some(serde_json::json!({"v": "recorded"}));
        o.observed_result = Some(serde_json::json!({"v": "different"}));
        o
    }

    /// A novel call is not a finding on its own, so with nothing diverging
    /// after it there is no cause to point at.
    /// The twin of the test below: the same two novel calls, but the second
    /// STOPPED the request. That makes it a divergence, so the first is a cause
    /// of it — and it proves the other test passes on the rule, not because
    /// nothing could ever enter the map.
    #[test]
    fn a_novel_call_before_one_that_stopped_the_request_is_an_origin() {
        let events: Vec<BoundaryEvent> = vec![];
        let table = table_for(&events, &HashMap::new());

        let added_read = obs("imc", Some("c1"), false, None, None);
        let mut stopped = obs("redis", Some("c1"), false, None, None);
        stopped.outcome = deja::SubstituteOutcome::Stopped;

        let rows = build(&events, &[added_read, stopped], &table, &HashSet::new());
        let novel = find(&rows, "novel");
        assert_eq!(novel.len(), 2, "precondition: both are novel: {rows:?}");
        assert!(novel[0].origin, "the earlier call is the cause: {novel:?}");
        assert!(
            novel[1].blocking,
            "a novel call that stopped the request blocks: {novel:?}"
        );
        assert!(
            !novel[0].blocking,
            "but the earlier one did not stop anything: {novel:?}"
        );
    }

    #[test]
    fn a_novel_call_with_nothing_diverging_after_it_is_not_an_origin() {
        let events: Vec<BoundaryEvent> = vec![];
        let table = table_for(&events, &HashMap::new());

        let added_read = obs("imc", Some("c1"), false, None, None);
        let fallback = obs("redis", Some("c1"), false, None, None);

        let rows = build(&events, &[added_read, fallback], &table, &HashSet::new());
        let novel = find(&rows, "novel");
        assert_eq!(novel.len(), 2, "precondition: both are novel: {rows:?}");
        assert!(
            novel.iter().all(|r| !r.origin),
            "nothing diverged, so nothing is a cause: {novel:?}"
        );
    }

    /// With a divergence after it, the added call is what to look at.
    /// The divergence that follows an added call is usually an ARGS-FREE PAIR —
    /// 224 of 267 value divergences on a real corpus carry no address rank. The
    /// added call is still its cause.
    #[test]
    fn a_novel_call_is_an_origin_when_an_args_free_pair_diverges_after_it() {
        let events = vec![event(7, "db", Some("c1"))];
        let spans: HashMap<u64, String> = [(7u64, "root>handler".to_owned())].into_iter().collect();
        let table = table_for(&events, &spans);

        let added_read = obs("imc", Some("c1"), false, None, None);
        // Pairs by locus rather than args, so it is unresolved and rankless —
        // which is what the origin map could not see.
        let mut paired = obs("db", Some("c1"), false, None, None);
        paired.args = serde_json::json!({"k": 999});
        paired.observed_result = Some(serde_json::json!({"r": "different"}));

        let rows = build(&events, &[added_read, paired], &table, &HashSet::new());
        let diverged = find(&rows, "value_diverged");
        assert_eq!(diverged.len(), 1, "precondition: the pair formed: {rows:?}");
        assert!(
            diverged[0].blocking && diverged[0].resolved_rank.is_none(),
            "precondition: blocking and rankless, i.e. args-free: {:?}",
            diverged[0]
        );

        let novel = find(&rows, "novel");
        assert_eq!(novel.len(), 1, "{rows:?}");
        assert!(
            novel[0].origin,
            "the added call is the cause of what diverged after it: {:?}",
            novel[0]
        );
    }

    #[test]
    fn a_novel_call_is_an_origin_when_a_divergence_follows_it() {
        let events = vec![event(7, "db", Some("c1"))];
        let table = table_for(&events, &HashMap::new());

        let added_read = obs("imc", Some("c1"), false, None, None);
        let later = diverging("db", Some("c1"), 7);

        let rows = build(&events, &[added_read, later], &table, &HashSet::new());
        let novel = find(&rows, "novel");
        assert_eq!(novel.len(), 1, "{rows:?}");
        assert!(
            novel[0].origin,
            "an added call with a divergence after it is the cause to show: {:?}",
            novel[0]
        );
    }

    /// A divergence before the added call cannot have been caused by it.
    #[test]
    fn a_divergence_before_the_novel_call_does_not_make_it_an_origin() {
        let events = vec![event(7, "db", Some("c1"))];
        let table = table_for(&events, &HashMap::new());

        let earlier = diverging("db", Some("c1"), 7);
        let added_read = obs("imc", Some("c1"), false, None, None);

        let rows = build(&events, &[earlier, added_read], &table, &HashSet::new());
        let novel = find(&rows, "novel");
        assert_eq!(novel.len(), 1, "{rows:?}");
        assert!(!novel[0].origin, "{:?}", novel[0]);
    }

    /// Two requests run the same code, so one's divergence says nothing about
    /// another's added call.
    #[test]
    fn attribution_does_not_cross_correlations() {
        let events = vec![event(7, "db", Some("c2"))];
        let table = table_for(&events, &HashMap::new());

        let added_read = obs("imc", Some("c1"), false, None, None);
        let other_request = diverging("db", Some("c2"), 7);

        let rows = build(
            &events,
            &[added_read, other_request],
            &table,
            &HashSet::new(),
        );
        let novel = find(&rows, "novel");
        assert_eq!(novel.len(), 1, "{rows:?}");
        assert!(
            !novel[0].origin,
            "another correlation's divergence is not this call's consequence: {:?}",
            novel[0]
        );
    }

    /// The scorecard tolerates an ABSORBED novel call (`NovelCallAbsorbed`,
    /// charged to nothing) and an inconclusive seed gap; the ledger reads
    /// neither `obs.absorbed` nor `obs.seed_gap`, so both fall through to
    /// `("novel", blocking = true)` and the viewer shows a blocking finding
    /// for a call the scorer forgave. The invalid state is written into the
    /// cell directly, not produced by any path that maintains the invariant.
    #[test]
    fn an_absorbed_novel_call_and_a_seed_gap_are_not_blocking_ledger_rows() {
        let events: Vec<BoundaryEvent> = vec![];
        let table = table_for(&events, &HashMap::new());

        let mut absorbed = obs("db", Some("c1"), false, None, None);
        absorbed.absorbed = true;
        let mut seed_gap = obs("redis", Some("c1"), false, None, None);
        seed_gap.seed_gap = true;

        let rows = build(&events, &[absorbed, seed_gap], &table, &HashSet::new());
        assert_eq!(rows.len(), 2, "precondition: both calls produce a row");

        let absorbed_row = rows.iter().find(|r| r.boundary == "db").expect("db row");
        assert!(
            !absorbed_row.blocking,
            "an absorbed miss is a novel call the process survived; the scorecard \
             charges it to nothing, so its ledger row must not block: {absorbed_row:?}"
        );
        assert_ne!(
            absorbed_row.kind, "novel",
            "and it must be NAMED as absorbed, not filed with the unabsorbed novel calls"
        );

        let gap_row = rows
            .iter()
            .find(|r| r.boundary == "redis")
            .expect("redis row");
        assert!(
            !gap_row.blocking,
            "a seed gap is inconclusive on the scorecard; the ledger must not call it \
             blocking: {gap_row:?}"
        );
        assert_ne!(gap_row.kind, "novel");
    }

    /// A call the candidate served from its own recorded error is named on
    /// its row, not reported as a match, and blocks nothing.
    #[test]
    fn a_served_recorded_error_is_named_on_its_row() {
        let events = vec![event(1, "db", Some("c1"))];
        let mut served = obs("db", Some("c1"), true, Some(2), Some(1));
        served.provenance = deja::Provenance::ServedRecordedError;
        let rows = build(
            &events,
            &[served],
            &table_for(&events, &HashMap::new()),
            &HashSet::new(),
        );
        let named = find(&rows, "served_recorded_error");
        assert_eq!(named.len(), 1, "{rows:?}");
        assert!(!named[0].blocking);
        assert!(find(&rows, "matched").is_empty());
    }

    #[test]
    fn ledger_classifies_and_carries_both_sides() {
        // recorded events: seq 1 (db, matched), seq 2 (redis, omitted)
        let events = vec![event(1, "db", Some("c1")), event(2, "redis", Some("c1"))];
        let spans: HashMap<u64, String> = [(1, "root>db".to_owned())].into_iter().collect();
        // observed: matched call to seq 1, plus a novel db call (unresolved)
        let observed = vec![
            obs("db", Some("c1"), true, Some(2), Some(1)),
            obs("db", Some("c1"), false, None, None),
        ];
        let rows = build(
            &events,
            &observed,
            &table_for(&events, &spans),
            &HashSet::new(),
        );

        let matched = find(&rows, "matched");
        assert_eq!(matched.len(), 1);
        let m = matched[0];
        assert!(!m.blocking);
        // both sides present; recorded carries value + span-path, observed carries location
        let rec = m.recorded.as_ref().unwrap();
        assert_eq!(rec.result, Some(serde_json::json!({"r": 1})));
        assert_eq!(rec.span_path.as_deref(), Some("root>db"));
        let obs_side = m.observed.as_ref().unwrap();
        assert_eq!(obs_side.call_file.as_deref(), Some("y.rs"));
        assert_eq!(obs_side.graph_node_id, Some(42));

        let novel = find(&rows, "novel");
        assert_eq!(novel.len(), 1);
        assert!(
            !novel[0].blocking,
            "a novel call that did not stop the request is charged to nothing by \
             the scorecard, and this row is what the viewer routes on"
        );
        assert!(novel[0].recorded.is_none(), "novel has no recorded side");
        assert!(novel[0].observed.is_some());

        let omitted = find(&rows, "omitted");
        assert_eq!(omitted.len(), 1, "seq 2 was never consumed");
        assert!(omitted[0].blocking);
        assert!(
            omitted[0].observed.is_none(),
            "omitted has no observed side"
        );
        assert_eq!(
            omitted[0].recorded.as_ref().unwrap().args,
            Some(serde_json::json!({"k": 2}))
        );
    }

    #[test]
    fn rank6_match_is_recovered_not_matched() {
        let events = vec![event(1, "db", Some("c1"))];
        let rows = build(
            &events,
            &[obs("db", Some("c1"), true, Some(6), Some(1))],
            &table_for(&events, &HashMap::new()),
            &HashSet::new(),
        );
        assert_eq!(find(&rows, "recovered").len(), 1);
        assert!(find(&rows, "matched").is_empty());
    }

    #[test]
    fn egress_and_pure_misses_are_nonblocking() {
        let rows = build(
            &[],
            &[
                obs("http_outgoing", Some("c1"), false, None, None),
                obs("time", Some("c1"), false, None, None),
            ],
            &table_for(&[], &HashMap::new()),
            &HashSet::new(),
        );
        assert_eq!(find(&rows, "environmental").len(), 1);
        assert_eq!(find(&rows, "deterministic").len(), 1);
        assert!(rows.iter().all(|r| !r.blocking));
    }

    /// A table entry whose event the loaded tape does not carry has no recorded
    /// side to write. It is named in the tally, and the ledger still builds:
    /// it is written after the scorecard and must not end the run.
    #[test]
    fn a_covered_call_missing_from_the_tape_does_not_fail_the_ledger() {
        let events = vec![event(1, "db", Some("c1")), event(2, "db", Some("c1"))];
        let table = table_for(&events, &HashMap::new());
        let loaded = &events[..1];
        let mut rows = Vec::new();
        let built = build_with_inconclusive_into(
            loaded,
            &[],
            &table,
            &HashSet::new(),
            &InconclusiveRaceEvidence::default(),
            &TailGapEvidence::default(),
            &super::super::UnplantedPresence::default(),
            &super::super::SeedGapCascade::default(),
            None,
            &mut |row| {
                rows.push(row);
                Ok(())
            },
        );
        assert!(built.is_ok(), "{built:?}");
        assert_eq!(
            rows.len(),
            1,
            "the loaded event still has its row: {rows:?}"
        );
    }

    /// A recorded call with a window, for the nesting tests below.
    fn timed(seq: u64, boundary: &str, task: &str, start: u64, end: u64) -> BoundaryEvent {
        let mut ev = event(seq, boundary, Some("c1"));
        ev.task_id = Some(task.to_owned());
        ev.timestamp_ns = start;
        ev.end_timestamp_ns = Some(end);
        ev
    }

    /// The label for the child of a parent the candidate called as `parent`.
    fn child_kind(parent: ObservedCall, child: BoundaryEvent) -> (String, Option<u64>) {
        child_kind_of(timed(1, "km", "t", 100, 200), parent, child)
    }

    fn child_kind_of(
        recorded_parent: BoundaryEvent,
        parent: ObservedCall,
        child: BoundaryEvent,
    ) -> (String, Option<u64>) {
        let events = vec![recorded_parent, child];
        let rows = build(
            &events,
            &[parent],
            &table_for(&events, &HashMap::new()),
            &HashSet::new(),
        );
        let row = rows
            .iter()
            .find(|r| r.source_event_global_sequence == Some(2) && r.observed.is_none())
            .expect("the child is unclaimed");
        (
            row.kind.clone(),
            row.served_ancestor.as_ref().map(|a| a.global_sequence),
        )
    }

    /// Served means the body did not run. A parent that ran for real, or that
    /// stopped the request, accounts for nothing beneath it; nor does a served
    /// call account for work on another task, or for a call that outlived it.
    #[test]
    fn only_a_served_call_accounts_for_the_calls_inside_it() {
        let served = || obs("km", Some("c1"), true, Some(2), Some(1));
        let inside = || timed(2, "time", "t", 150, 151);
        assert_eq!(
            child_kind(served(), inside()),
            ("nested_in_served_call".to_owned(), Some(1))
        );

        let mut executed = served();
        executed.provenance = deja::Provenance::Shadow;
        let mut stopped = served();
        stopped.outcome = deja::SubstituteOutcome::Stopped;
        for parent in [executed, stopped] {
            assert_eq!(child_kind(parent, inside()), ("omitted".to_owned(), None));
        }

        for child in [
            timed(2, "time", "other-task", 150, 151),
            timed(2, "time", "t", 150, 250),
            timed(2, "time", "t", 250, 251),
        ] {
            assert_eq!(child_kind(served(), child), ("omitted".to_owned(), None));
        }

        // A tape without end stamps cannot say what was inside what.
        let mut unstamped = timed(1, "km", "t", 100, 200);
        unstamped.end_timestamp_ns = None;
        unstamped.duration_us = 1;
        assert_eq!(
            child_kind_of(unstamped, served(), inside()),
            ("omitted".to_owned(), None)
        );

        // Served by its address with other arguments: still the recording's
        // value, so still a body that never ran.
        let mut by_address = obs("km", Some("c1"), false, None, None);
        by_address.arg_divergent = true;
        by_address.served_event_global_sequence = Some(1);
        assert_eq!(
            child_kind(by_address, inside()),
            ("nested_in_served_call".to_owned(), Some(1))
        );
    }
}
