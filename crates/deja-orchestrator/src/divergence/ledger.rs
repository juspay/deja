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
//!   recorded ∧ unconsumed → omitted
//!
//! Each row carries `blocking` so the UI can show the same pass/fail split the
//! verdict used. The value of a *matched* side-effect call is identical on both
//! sides by construction (replay substitutes the recorded result), so the row
//! shows both sides for context; the genuine value divergence lives in the HTTP
//! diff stream and in the novel/omitted set-deltas.

use std::collections::{HashMap, HashSet};

use deja::{Address, BoundaryEvent, ObservedCall};
use serde::{Deserialize, Serialize};

use super::{
    args_free_effective_values, event_reply_canon_kind, is_nonblocking_boundary,
    observed_value_diverged, tier_for, values_diverge_under_event, InconclusiveRaceEvidence, Tier,
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
    pub boundary: String,
    pub trait_name: String,
    pub method_name: String,
    /// matched | recovered | novel | omitted | environmental | deterministic |
    /// value_diverged | order_nondeterministic | idempotent_delete |
    /// inconclusive_race
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
/// candidate's observed calls, the set of sequences the lookup table covers,
/// and the recorded span path per sequence (rank-2 address, for graph
/// alignment).
pub fn build(
    events: &[BoundaryEvent],
    observed: &[ObservedCall],
    expected_seqs: &HashSet<u64>,
    span_paths: &HashMap<u64, String>,
    order_nondet_demote: &HashSet<u64>,
    idempotent_delete_demote: &HashSet<u64>,
) -> Vec<CallRecord> {
    build_with_inconclusive(
        events,
        observed,
        expected_seqs,
        span_paths,
        order_nondet_demote,
        idempotent_delete_demote,
        &InconclusiveRaceEvidence::default(),
    )
}

pub(crate) fn build_with_inconclusive(
    events: &[BoundaryEvent],
    observed: &[ObservedCall],
    expected_seqs: &HashSet<u64>,
    span_paths: &HashMap<u64, String>,
    order_nondet_demote: &HashSet<u64>,
    idempotent_delete_demote: &HashSet<u64>,
    inconclusive_race: &InconclusiveRaceEvidence,
) -> Vec<CallRecord> {
    let by_seq: HashMap<u64, &BoundaryEvent> =
        events.iter().map(|e| (e.global_sequence, e)).collect();
    let recorded_for = |seq: u64| -> Option<CallSide> {
        by_seq.get(&seq).map(|ev| {
            let mut side = recorded_side(ev);
            side.span_path = span_paths.get(&seq).cloned();
            side
        })
    };

    let mut rows: Vec<CallRecord> = Vec::new();
    let mut consumed: HashSet<u64> = HashSet::new();

    // Args-free pairing of recorded twins for execute-mode write consequences,
    // mirroring `detect()`: a re-keyed write misses its baseline by args, so it
    // would otherwise split into a phantom novel (observed) + omitted (recorded
    // twin). Pair them by call-site identity (correlation, boundary, method) in
    // FIFO source order so the ledger shows ONE value_diverged consequence row
    // carrying recorded 0.10 + observed 0.20 — parity with the scorecard.
    type Identity = (Option<String>, String, String);
    let mut recorded_pairing: HashMap<Identity, std::collections::VecDeque<u64>> = HashMap::new();
    {
        let mut by_identity: Vec<(Identity, u64)> = expected_seqs
            .iter()
            .filter_map(|s| by_seq.get(s).copied())
            .map(|ev| {
                (
                    (
                        ev.correlation_id.clone(),
                        ev.boundary.clone(),
                        ev.method_name.clone(),
                    ),
                    ev.global_sequence,
                )
            })
            .collect();
        by_identity.sort_by_key(|(_, seq)| *seq);
        for (id, seq) in by_identity {
            recorded_pairing.entry(id).or_default().push_back(seq);
        }
    }
    // Recorded twins claimed by a value_diverged consequence, so the omitted pass
    // doesn't also flag them (collapses the would-be novel+omitted split).
    let mut paired_consumed: HashSet<u64> = HashSet::new();

    // --- observed calls (candidate side) ------------------------------------
    for obs in observed {
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
            // Rules A/B and narrow race recognition demote selected execute
            // divergences to non-blocking rows, mirroring the scorecard.
            let seq = obs.source_event_global_sequence;
            let (kind, blocking) = if seq.is_some_and(|s| order_nondet_demote.contains(&s)) {
                ("order_nondeterministic".to_owned(), false)
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
            let id: Identity = (
                obs.correlation_id.clone(),
                obs.boundary.clone(),
                obs.method_name.clone(),
            );
            let twin = recorded_pairing.get_mut(&id).and_then(|q| {
                while let Some(seq) = q.front().copied() {
                    if consumed.contains(&seq) {
                        q.pop_front();
                    } else {
                        return q.pop_front();
                    }
                }
                None
            });
            if let Some(twin_seq) = twin {
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
                let value_diverged = values_diverge_under_event(
                    &obs.boundary,
                    &recorded_val,
                    &observed_val,
                    twin_event,
                );
                let race_downstream = value_diverged
                    && inconclusive_race
                        .attributable_downstream(obs.correlation_id.as_deref(), &obs.args);
                rows.push(CallRecord {
                    correlation_id: obs.correlation_id.clone(),
                    source_event_global_sequence: Some(twin_seq),
                    boundary: obs.boundary.clone(),
                    trait_name: obs.trait_name.clone(),
                    method_name: obs.method_name.clone(),
                    kind: if !value_diverged {
                        "matched"
                    } else if race_downstream {
                        "inconclusive_race"
                    } else {
                        "value_diverged"
                    }
                    .to_owned(),
                    blocking: value_diverged && !race_downstream,
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
        let blocking = ev.correlation_id.is_some() && !is_nonblocking_boundary(&ev.boundary);
        rows.push(CallRecord {
            correlation_id: ev.correlation_id.clone(),
            source_event_global_sequence: Some(ev.global_sequence),
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
            detached: false,
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
            detached: false,
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

    #[test]
    fn ledger_classifies_and_carries_both_sides() {
        // recorded events: seq 1 (db, matched), seq 2 (redis, omitted)
        let events = vec![event(1, "db", Some("c1")), event(2, "redis", Some("c1"))];
        let expected: HashSet<u64> = [1, 2].into_iter().collect();
        let spans: HashMap<u64, String> = [(1, "root>db".to_owned())].into_iter().collect();
        // observed: matched call to seq 1, plus a novel db call (unresolved)
        let observed = vec![
            obs("db", Some("c1"), true, Some(2), Some(1)),
            obs("db", Some("c1"), false, None, None),
        ];
        let rows = build(
            &events,
            &observed,
            &expected,
            &spans,
            &HashSet::new(),
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
        let expected: HashSet<u64> = [1].into_iter().collect();
        let rows = build(
            &events,
            &[obs("db", Some("c1"), true, Some(6), Some(1))],
            &expected,
            &HashMap::new(),
            &HashSet::new(),
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
            &HashSet::new(),
            &HashMap::new(),
            &HashSet::new(),
            &HashSet::new(),
        );
        assert_eq!(find(&rows, "environmental").len(), 1);
        assert_eq!(find(&rows, "deterministic").len(), 1);
        assert!(rows.iter().all(|r| !r.blocking));
    }
}
