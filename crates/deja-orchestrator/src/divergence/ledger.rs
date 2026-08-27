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
//!   recorded ∧ unconsumed → omitted
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

use std::collections::{BTreeSet, HashMap, HashSet};

use deja::{Address, BoundaryEvent, ObservedCall};
use serde::{Deserialize, Serialize};

use super::{
    args_free_effective_values, correlation_column_provenance, event_reply_canon_kind,
    is_nonblocking_boundary, observed_schema_default_divergence, observed_value_diverged,
    omission_is_blocking, schema_default_divergence, tier_for, values_diverge_under_event,
    GraphScoringPlan, InconclusiveRaceEvidence, SchemaDefaultVerdict, TailGapEvidence, Tier,
    POSITIONAL_FALLBACK_RANK,
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
    /// The recorded event actually served by the replay lookup ladder. Present
    /// when graph alignment found that serving and structural identity differ;
    /// `source_event_global_sequence` remains the structurally aligned event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub served_event_global_sequence: Option<u64>,
    pub boundary: String,
    pub trait_name: String,
    pub method_name: String,
    /// matched | recovered | novel | inconclusive_tail_gap | omitted |
    /// environmental | deterministic |
    /// value_diverged | idempotent_delete | inconclusive_race | schema_default |
    /// identity_skew | pruned_subtree | novel_subtree
    pub kind: String,
    /// Whether this row counts toward the fail verdict (mirrors the scorecard).
    pub blocking: bool,
    /// For a `value_diverged` row: `true` on the ORIGIN (the executed read whose
    /// real-boundary value differed from the recorded baseline — the cause),
    /// `false` on the CONSEQUENCE (a downstream write paired args-free). Absent on
    /// every other kind. Lets the UI render the origin -> consequence cascade.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub origin: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resolved_rank: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded: Option<CallSide>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub observed: Option<CallSide>,
}

/// The ledger's snake_case name for a schema-derived INSERT row.
fn schema_default_row_kind(_scorecard_kind: &str) -> String {
    "schema_default".to_owned()
}

fn recorded_side(ev: &BoundaryEvent) -> CallSide {
    CallSide {
        args: Some(ev.args.clone()),
        result: Some(ev.result.clone()),
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

pub(crate) fn build_with_inconclusive(
    events: &[BoundaryEvent],
    observed: &[ObservedCall],
    table: &deja::LookupTable,
    idempotent_delete_demote: &HashSet<u64>,
    inconclusive_race: &InconclusiveRaceEvidence,
    tail_gap: &TailGapEvidence,
) -> Vec<CallRecord> {
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

    let mut rows: Vec<CallRecord> = Vec::new();
    let mut consumed: HashSet<u64> = HashSet::new();

    // Args-free pairing of recorded twins for execute-mode write consequences:
    // a re-keyed write misses its baseline by args, so it would otherwise split
    // into a phantom novel (observed) + omitted (recorded twin). Pair them so
    // the ledger shows ONE value_diverged consequence row carrying recorded 0.10
    // + observed 0.20.
    //
    // The rule is `ArgsFreePairing` — THE one the scorecard uses, not a second
    // copy of it. This used to key on (correlation, boundary, method) alone,
    // which is the pool the scorecard had already narrowed twice; the two
    // drifted, and a run whose scorecard correctly refused eight pairs shipped a
    // ledger that made them anyway, each marrying two DIFFERENT SQL statements.
    let mut recorded_pairing = super::ArgsFreePairing::build(table, events, &column_provenance);
    // Recorded twins claimed by a value_diverged consequence, so the omitted pass
    // doesn't also flag them (collapses the would-be novel+omitted split).
    let mut paired_consumed: HashSet<u64> = HashSet::new();

    // --- observed calls (candidate side) ------------------------------------
    // Enumerated because a truncated-recording tail is a POSITIONAL fact: the
    // call must come after the correlation's last recorded event was reproduced.
    for (observed_index, obs) in observed.iter().enumerate() {
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
            } else {
                ("value_diverged".to_owned(), true)
            };
            rows.push(CallRecord {
                correlation_id: obs.correlation_id.clone(),
                source_event_global_sequence: obs.source_event_global_sequence,
                served_event_global_sequence: None,
                boundary: obs.boundary.clone(),
                trait_name: obs.trait_name.clone(),
                method_name: obs.method_name.clone(),
                kind,
                blocking,
                origin: true,
                resolved_rank: obs.resolved_rank,
                recorded,
                observed: observed_side(obs).or_none(),
            });
            continue;
        }

        // CONSEQUENCE: an unresolved execute-mode call (a re-keyed WRITE) that
        // pairs args-free with an unconsumed recorded twin. Emit ONE
        // value_diverged consequence row (recorded twin value + observed value)
        // instead of a phantom novel + omitted split.
        if !obs.resolved
            && obs.provenance == deja::Provenance::Shadow
            && obs.correlation_id.is_some()
            && !is_nonblocking_boundary(&obs.boundary)
            && tier_for(&obs.boundary) != Tier::Environmental
        {
            if let Some(twin) = recorded_pairing.take_twin(obs, &consumed) {
                let twin_seq = twin.sequence;
                paired_consumed.insert(twin_seq);
                let mut recorded = recorded_for(twin_seq);
                let mut observed = observed_side(obs);
                // A WRITE returns unit, so the divergence signal lives in its
                // OPERAND, not its result. When both sides' result is empty,
                // surface the diverging `value` argument as the displayed value so
                // the cascade chip reads 0.10 -> 0.20 (the recorded twin's operand
                // vs the executed write's operand) instead of ∅ -> ∅.
                let is_unit = |r: &Option<serde_json::Value>| {
                    matches!(r, None | Some(serde_json::Value::Null))
                };
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
                    .unwrap_or(serde_json::Value::Null);
                let twin_event = by_seq.get(&twin_seq).copied();
                let (recorded_val, observed_val) =
                    args_free_effective_values(&recorded_result, obs, twin_event);
                let value_diverged = twin.order_mismatch
                    || values_diverge_under_event(
                        &obs.boundary,
                        &recorded_val,
                        &observed_val,
                        twin_event,
                        obs.args.get("sql").and_then(serde_json::Value::as_str),
                    );
                let race_downstream = !twin.order_mismatch
                    && value_diverged
                    && inconclusive_race
                        .attributable_downstream(obs.correlation_id.as_deref(), &obs.args);
                // Rule C on the args-free arm, mirroring the scorecard: the two
                // statements say the schema filled every differing column.
                // Exact later-args evidence is instead always value-diverged and
                // blocking; equivalent result envelopes cannot absorb the swap.
                let schema_default = (value_diverged && !twin.order_mismatch)
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
                        SchemaDefaultVerdict::Confirmed(d) => {
                            Some(schema_default_row_kind(d.kind()))
                        }
                        _ => None,
                    });
                rows.push(CallRecord {
                    correlation_id: obs.correlation_id.clone(),
                    source_event_global_sequence: Some(twin_seq),
                    served_event_global_sequence: None,
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
                    origin: false,
                    resolved_rank: obs.resolved_rank,
                    recorded,
                    observed: observed.or_none(),
                });
                continue;
            }
        }

        let (kind, blocking) = if obs.resolved {
            consumed.extend(obs.source_event_global_sequence);
            let recovered = obs.resolved_rank == Some(POSITIONAL_FALLBACK_RANK);
            (if recovered { "recovered" } else { "matched" }, false)
        } else if tier_for(&obs.boundary) == Tier::Environmental {
            ("environmental", false)
        } else if is_nonblocking_boundary(&obs.boundary) {
            ("deterministic", false)
        } else if obs.correlation_id.is_none() {
            // uncorrelated background-task novel call — tolerated in V1
            ("novel", false)
        } else if tail_gap.covers(obs.correlation_id.as_deref(), observed_index) {
            // The recording for this correlation stops at request teardown and
            // this call comes after it: no baseline, so neither matched nor
            // novel. Mirrors the scorecard's `InconclusiveTailGap`.
            ("inconclusive_tail_gap", false)
        } else {
            ("novel", true)
        };
        let recorded = obs
            .source_event_global_sequence
            .and_then(recorded_for)
            .and_then(CallSide::or_none);
        rows.push(CallRecord {
            correlation_id: obs.correlation_id.clone(),
            source_event_global_sequence: obs.source_event_global_sequence,
            served_event_global_sequence: None,
            boundary: obs.boundary.clone(),
            trait_name: obs.trait_name.clone(),
            method_name: obs.method_name.clone(),
            kind: kind.to_owned(),
            blocking,
            origin: false,
            resolved_rank: obs.resolved_rank,
            recorded,
            observed: observed_side(obs).or_none(),
        });
    }

    // --- omitted: expected (table-covered) recorded events never consumed ----
    let mut omitted: Vec<&BoundaryEvent> = expected_seqs
        .iter()
        .filter(|s| !consumed.contains(s) && !paired_consumed.contains(s))
        .filter_map(|s| by_seq.get(s).copied())
        .collect();
    omitted.sort_by_key(|e| e.global_sequence);
    for ev in omitted {
        let blocking = omission_is_blocking(ev.correlation_id.as_deref(), &ev.boundary);
        rows.push(CallRecord {
            correlation_id: ev.correlation_id.clone(),
            source_event_global_sequence: Some(ev.global_sequence),
            served_event_global_sequence: None,
            boundary: ev.boundary.clone(),
            trait_name: ev.trait_name.clone(),
            method_name: ev.method_name.clone(),
            kind: "omitted".to_owned(),
            blocking,
            origin: false,
            resolved_rank: None,
            recorded: recorded_for(ev.global_sequence),
            observed: None,
        });
    }

    rows
}

/// Build a ledger through the same per-correlation graph/flat seam as the
/// scorecard. The all-flat arm delegates directly to the legacy builder; mixed
/// runs remove graph correlations before doing so, so args-free pairing cannot
/// claim an event owned by graph alignment.
pub(crate) fn build_with_plan(
    events: &[BoundaryEvent],
    observed: &[ObservedCall],
    table: &deja::LookupTable,
    idempotent_delete_demote: &HashSet<u64>,
    inconclusive_race: &InconclusiveRaceEvidence,
    tail_gap: &TailGapEvidence,
    plan: &GraphScoringPlan,
) -> Vec<CallRecord> {
    let graph_correlations: BTreeSet<&str> = events
        .iter()
        .filter_map(|event| event.correlation_id.as_deref())
        .chain(
            observed
                .iter()
                .filter_map(|call| call.correlation_id.as_deref()),
        )
        .filter(|correlation_id| plan.is_graph(Some(correlation_id)))
        .collect();

    if graph_correlations.is_empty() {
        return build_with_inconclusive(
            events,
            observed,
            table,
            idempotent_delete_demote,
            inconclusive_race,
            tail_gap,
        );
    }
    let by_seq: HashMap<u64, &BoundaryEvent> = events
        .iter()
        .map(|event| (event.global_sequence, event))
        .collect();

    let mut graph_sequence = HashSet::new();
    let mut graph_observed_indices = HashSet::new();
    for correlation_id in &graph_correlations {
        let alignment = plan
            .alignment(correlation_id)
            .expect("graph scoring mode owns a reconciled alignment");
        for row in &alignment.nodes {
            if plan.alignment_row_uses_flat_tier(correlation_id, row) {
                continue;
            }
            match &row.outcome {
                deja_forest::NodeOutcome::PrunedSubtree { .. } => {
                    graph_sequence.extend(plan.alignment_recorded_sequences(correlation_id, row));
                }
                deja_forest::NodeOutcome::NovelSubtree { .. } => {
                    graph_observed_indices
                        .extend(plan.alignment_replay_indices(correlation_id, row));
                }
                deja_forest::NodeOutcome::IdentitySkew {
                    aligned_event,
                    served_event,
                } => {
                    let sequences = plan.alignment_recorded_sequences(correlation_id, row);
                    let indices = plan.alignment_replay_indices(correlation_id, row);
                    if sequences.len() == 1 && indices.len() == 1 {
                        graph_sequence.extend(aligned_event.iter().copied());
                        graph_sequence.extend(served_event.iter().copied());
                        graph_observed_indices.insert(indices[0]);
                    }
                }
                _ => {
                    let sequences = plan.alignment_recorded_sequences(correlation_id, row);
                    let indices = plan.alignment_replay_indices(correlation_id, row);
                    if sequences.len() == 1 && indices.len() == 1 {
                        graph_sequence.insert(sequences[0]);
                        graph_observed_indices.insert(indices[0]);
                    }
                }
            }
        }
    }
    graph_sequence.retain(|sequence| {
        let correlation_id = by_seq
            .get(sequence)
            .and_then(|event| event.correlation_id.as_deref());
        !correlation_id.is_some_and(|id| plan.record_event_uses_flat_tier(id, *sequence))
    });
    graph_observed_indices.retain(|index| {
        let correlation_id = observed[*index].correlation_id.as_deref();
        !correlation_id.is_some_and(|id| plan.replay_event_uses_flat_tier(id, *index))
    });
    let flat_events: Vec<BoundaryEvent> = events
        .iter()
        .filter(|event| !graph_sequence.contains(&event.global_sequence))
        .cloned()
        .collect();
    // Retained ORIGINAL indices, ascending — the map from this sub-stream back
    // to the stream the tail-gap evidence was measured against.
    let flat_retained: Vec<usize> = (0..observed.len())
        .filter(|index| !graph_observed_indices.contains(index))
        .collect();
    let flat_observed: Vec<ObservedCall> = flat_retained
        .iter()
        .map(|index| observed[*index].clone())
        .collect();
    let flat_tail_gap = tail_gap.remap_to(&flat_retained);
    let mut flat_table = table.clone();
    flat_table
        .entries
        .retain(|entry| !graph_sequence.contains(&entry.source_event_global_sequence));

    let mut rows = build_with_inconclusive(
        &flat_events,
        &flat_observed,
        &flat_table,
        idempotent_delete_demote,
        inconclusive_race,
        &flat_tail_gap,
    );
    let span_paths = recorded_span_paths(table);

    for correlation_id in graph_correlations {
        let alignment = plan
            .alignment(correlation_id)
            .expect("graph scoring mode owns a reconciled alignment");
        for alignment_row in &alignment.nodes {
            use deja_forest::NodeOutcome;

            match &alignment_row.outcome {
                NodeOutcome::PrunedSubtree { events_below } => {
                    let sequences =
                        plan.alignment_recorded_sequences(correlation_id, alignment_row);
                    assert_eq!(sequences.len() as u64, *events_below);
                    for sequence in sequences {
                        let event = by_seq[&sequence];
                        let mut side = recorded_side(event);
                        side.span_path = span_paths.get(&sequence).cloned();
                        rows.push(CallRecord {
                            correlation_id: event.correlation_id.clone(),
                            source_event_global_sequence: Some(sequence),
                            served_event_global_sequence: None,
                            boundary: event.boundary.clone(),
                            trait_name: event.trait_name.clone(),
                            method_name: event.method_name.clone(),
                            kind: "pruned_subtree".to_owned(),
                            blocking: omission_is_blocking(
                                event.correlation_id.as_deref(),
                                &event.boundary,
                            ),
                            origin: false,
                            resolved_rank: None,
                            recorded: side.or_none(),
                            observed: None,
                        });
                    }
                }
                NodeOutcome::NovelSubtree { events_below } => {
                    let indices = plan.alignment_replay_indices(correlation_id, alignment_row);
                    assert_eq!(indices.len() as u64, *events_below);
                    for index in indices {
                        let call = &observed[index];
                        // `indices` are ORIGINAL stream positions, which is the
                        // space the evidence was measured in — no remap here.
                        let unrecorded_tail =
                            tail_gap.covers(call.correlation_id.as_deref(), index);
                        rows.push(CallRecord {
                            correlation_id: call.correlation_id.clone(),
                            source_event_global_sequence: None,
                            served_event_global_sequence: None,
                            boundary: call.boundary.clone(),
                            trait_name: call.trait_name.clone(),
                            method_name: call.method_name.clone(),
                            kind: if unrecorded_tail {
                                "inconclusive_tail_gap".to_owned()
                            } else {
                                "novel_subtree".to_owned()
                            },
                            blocking: !unrecorded_tail
                                && tier_for(&call.boundary) != Tier::Environmental
                                && !is_nonblocking_boundary(&call.boundary),
                            origin: false,
                            resolved_rank: call.resolved_rank,
                            recorded: None,
                            observed: observed_side(call).or_none(),
                        });
                    }
                }
                outcome => {
                    let replay_node = alignment_row
                        .replay_node
                        .expect("paired graph outcome has a replay node");
                    let indices = plan.alignment_replay_indices(correlation_id, alignment_row);
                    if indices.len() != 1 || !graph_observed_indices.contains(&indices[0]) {
                        // Multiple events attached to one span have no
                        // unambiguous event-level graph pairing. The plan marks
                        // them flat-tier and the legacy builder above owns them.
                        continue;
                    }
                    let call = &observed[indices[0]];
                    let (aligned_sequence, served_sequence) = match outcome {
                        NodeOutcome::IdentitySkew {
                            aligned_event,
                            served_event,
                        } => (*aligned_event, *served_event),
                        _ => (
                            plan.recorded_sequence_for_replay_node(correlation_id, replay_node),
                            None,
                        ),
                    };
                    let aligned_event = aligned_sequence.and_then(|seq| by_seq.get(&seq).copied());
                    let mut recorded = aligned_event.map(recorded_side);
                    if let (Some(sequence), Some(side)) = (aligned_sequence, recorded.as_mut()) {
                        side.span_path = span_paths.get(&sequence).cloned();
                    }

                    let computed_divergence = match outcome {
                        NodeOutcome::ValueDiverged { origin } => Some(*origin),
                        NodeOutcome::Matched if observed_value_diverged(call, aligned_event) => {
                            Some(true)
                        }
                        NodeOutcome::Matched if !call.resolved => aligned_event.and_then(|event| {
                            let (recorded_value, observed_value) =
                                args_free_effective_values(&event.result, call, Some(event));
                            values_diverge_under_event(
                                &call.boundary,
                                &recorded_value,
                                &observed_value,
                                Some(event),
                                call.args.get("sql").and_then(serde_json::Value::as_str),
                            )
                            .then_some(false)
                        }),
                        _ => None,
                    };
                    let mut observed = observed_side(call);
                    let (kind, blocking, origin) =
                        if matches!(outcome, NodeOutcome::IdentitySkew { .. }) {
                            ("identity_skew".to_owned(), true, false)
                        } else if computed_divergence == Some(true) {
                            let schema_default =
                                observed_schema_default_divergence(call, aligned_event);
                            let (kind, blocking) =
                                if let SchemaDefaultVerdict::Confirmed(divergence) = schema_default
                                {
                                    (schema_default_row_kind(divergence.kind()), false)
                                } else if aligned_sequence
                                    .is_some_and(|seq| idempotent_delete_demote.contains(&seq))
                                {
                                    let kind = aligned_event
                                        .and_then(event_reply_canon_kind)
                                        .unwrap_or_else(|| "idempotent_delete".to_owned());
                                    (kind, false)
                                } else if aligned_sequence
                                    .is_some_and(|seq| inconclusive_race.contains(&seq))
                                {
                                    ("inconclusive_race".to_owned(), false)
                                } else {
                                    ("value_diverged".to_owned(), true)
                                };
                            (kind, blocking, true)
                        } else if computed_divergence == Some(false) {
                            let event =
                                aligned_event.expect("aligned divergence owns a recorded event");
                            let (recorded_value, observed_value) =
                                args_free_effective_values(&event.result, call, Some(event));
                            if matches!(
                                recorded.as_ref().and_then(|side| side.result.as_ref()),
                                None | Some(serde_json::Value::Null)
                            ) && matches!(
                                observed.result.as_ref(),
                                None | Some(serde_json::Value::Null)
                            ) {
                                observed.result = call.args.get("value").cloned();
                                if let Some(side) = recorded.as_mut() {
                                    side.result = side
                                        .args
                                        .as_ref()
                                        .and_then(|args| args.get("value"))
                                        .cloned();
                                }
                            }
                            let schema_default = schema_default_divergence(
                                &call.boundary,
                                event.args.get("sql").and_then(|sql| sql.as_str()),
                                call.args.get("sql").and_then(|sql| sql.as_str()),
                                &recorded_value,
                                &observed_value,
                            );
                            let schema_kind = match schema_default {
                                SchemaDefaultVerdict::Confirmed(divergence) => {
                                    Some(schema_default_row_kind(divergence.kind()))
                                }
                                _ => None,
                            };
                            let race_downstream = inconclusive_race.attributable_downstream(
                                call.correlation_id.as_deref(),
                                &call.args,
                            );
                            let blocking = schema_kind.is_none() && !race_downstream;
                            let kind = schema_kind.unwrap_or_else(|| {
                                if race_downstream {
                                    "inconclusive_race".to_owned()
                                } else {
                                    "value_diverged".to_owned()
                                }
                            });
                            (kind, blocking, false)
                        } else {
                            let recovered = call.resolved_rank == Some(POSITIONAL_FALLBACK_RANK);
                            (
                                if recovered { "recovered" } else { "matched" }.to_owned(),
                                false,
                                false,
                            )
                        };

                    rows.push(CallRecord {
                        correlation_id: call.correlation_id.clone(),
                        source_event_global_sequence: aligned_sequence,
                        served_event_global_sequence: served_sequence,
                        boundary: call.boundary.clone(),
                        trait_name: call.trait_name.clone(),
                        method_name: call.method_name.clone(),
                        kind,
                        blocking,
                        origin,
                        resolved_rank: call.resolved_rank,
                        recorded: recorded.and_then(CallSide::or_none),
                        observed: observed.or_none(),
                    });
                }
            }
        }
    }

    rows
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
        if let Address::SpanPath { path } = &entry.key.address {
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
            request: serde_json::Value::Null,
            args: serde_json::json!({"k": seq}),
            response: serde_json::Value::Null,
            result: serde_json::json!({"r": seq}),
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
            real_impl_will_fail: false,
            recorded_result: None,
            observed_result: None,
            provenance: deja::Provenance::default(),
            seed_gap: false,
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
            let key = |address| deja::LookupKey {
                correlation_id: ev.correlation_id.clone(),
                bucket_id: ev.bucket_id.clone(),
                fork_seq: 0,
                address,
                args_hash: 0,
                occurrence: 0,
            };
            entries.push(deja::LookupEntry {
                key: key(Address::Sequence {
                    boundary: ev.boundary.clone(),
                    method: ev.method_name.clone(),
                    request_sequence: 0,
                }),
                result: ev.result.clone(),
                source_event_global_sequence: ev.global_sequence,
            });
            if let Some(path) = spans.get(&ev.global_sequence) {
                entries.push(deja::LookupEntry {
                    key: key(Address::SpanPath { path: path.clone() }),
                    result: ev.result.clone(),
                    source_event_global_sequence: ev.global_sequence,
                });
            }
        }
        deja::LookupTable {
            recording_id: "rec".to_owned(),
            policy_version: 1,
            entries,
        }
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
        assert!(novel[0].blocking, "correlated novel call blocks");
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
}
