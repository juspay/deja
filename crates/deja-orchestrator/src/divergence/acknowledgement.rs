//! Acknowledgements: a divergence a pull request introduces, accepted as
//! intended, and how that acceptance is laid over a delta.
//!
//! An acknowledgement must outlive the run it was given on. Runs of the same
//! pull request happen on whichever recording is newest, and a delta row's
//! [`Address`] names the recorded request it sits in, so the raw address
//! changes from one run to the next even when the divergence is the same.
//! The key is therefore the lane (connector and flow) plus a [`Pattern`]: the
//! address with everything recording-specific removed.
//!
//! What the pattern drops, and why: the correlation (the recorded request's
//! id), the recorded event id (the event the call paired to), and for a call
//! its occurrence (the nth call at that span path in the request). The last
//! one goes because an upstream retry shifts it: the same logical call is the
//! second in one recording and the third in another. The cost is that one
//! acknowledgement covers every occurrence of that call shape in a request,
//! which is what "this call diverges, intentionally" means in practice. A
//! body pattern keeps its JSON path as is, array indices included; a path
//! into a list is a known limitation.
//!
//! An acknowledgement also carries the `change_id` of the pull request's own
//! diff when it was proposed. A run whose change id differs shows it as
//! stale rather than dropping it: the author changed the change, so a
//! maintainer looks again, but the note and the trail stay.
//!
//! The overlay never touches the cached delta: [`apply`] runs on every read,
//! over the pure three-way, so a confirmation or a withdrawal is visible at
//! once and the cache stays a function of the two trees alone.

use serde::{Deserialize, Serialize};

use super::behaviour_tree::{Address, Lane};
use super::delta::{Delta, Row, Side};

/// An address with its recording-specific parts removed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Pattern {
    Call {
        span_path: String,
        boundary: String,
        operation: String,
    },
    Status,
    Body {
        json_path: String,
    },
}

impl Pattern {
    pub fn of(address: &Address) -> Pattern {
        match address {
            Address::Call {
                span_path,
                boundary,
                operation,
                ..
            } => Pattern::Call {
                span_path: span_path.clone(),
                boundary: boundary.clone(),
                operation: operation.clone(),
            },
            Address::Status { .. } => Pattern::Status,
            Address::Body { json_path, .. } => Pattern::Body {
                json_path: json_path.clone(),
            },
        }
    }
}

/// What an acknowledgement is matched on.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Key {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<Lane>,
    pub pattern: Pattern,
}

impl Key {
    pub fn of_row(row: &Row) -> Key {
        Key {
            lane: row.lane.clone(),
            pattern: Pattern::of(&row.address),
        }
    }
}

/// One acknowledgement as the overlay sees it; the store row, decoded.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Acknowledgement {
    pub id: i64,
    pub key: Key,
    pub change_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_hash: Option<String>,
    pub note: String,
    pub proposed_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_by: Option<String>,
    #[serde(default)]
    pub withdrawn: bool,
    /// The sign-in subjects behind the two names; `None` when the action was
    /// taken with sign-in off or by the service token.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proposed_by_sub: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledged_by_sub: Option<String>,
}

impl Acknowledgement {
    /// Whether this counts as confirmed. With sign-in required, a
    /// confirmation given by nobody in particular — before sign-in, under a
    /// typed name — is only a proposal: the row keeps its history and asks
    /// to be decided again by someone who can be named.
    fn confirmed(&self, require_subjects: bool) -> bool {
        self.acknowledged_by.is_some()
            && (!require_subjects
                || self
                    .acknowledged_by_sub
                    .as_deref()
                    .is_some_and(|s| !s.is_empty()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum State {
    /// Proposed, not yet confirmed by a second person.
    Proposed,
    /// Confirmed: covers the row.
    Acknowledged,
    /// Given on an earlier version of the change; needs a second look.
    Stale,
}

/// What a charged row carries after the overlay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowAcknowledgement {
    pub id: i64,
    pub state: State,
    /// The confirmer when confirmed, else the proposer.
    pub by: String,
    pub note: String,
    /// The candidate's value differs from the one acknowledged. Shown, not
    /// held against the row: the shape of the divergence is what was accepted.
    pub value_changed: bool,
}

/// The verdict once acknowledgements are counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Effective {
    /// Nothing charged on a blocking address.
    Pass,
    /// Every blocking charged row is covered by a confirmed acknowledgement.
    Acknowledged,
    /// A blocking charged row is uncovered, only proposed, or stale.
    Fail,
}

impl Effective {
    pub fn word(self) -> &'static str {
        match self {
            Effective::Pass => "pass",
            Effective::Acknowledged => "acknowledged",
            Effective::Fail => "fail",
        }
    }
}

/// Lay `acks` over `delta`'s charged rows and settle the effective verdict.
/// `change_id` is the run's; `None` means the run does not know which
/// version of the change it ran, so every acknowledgement is stale: a
/// decision cannot be carried to a version nobody can name. Run creation
/// refuses a pull request without one, so this is only runs from before.
///
/// For a row, the newest live acknowledgement with its key wins, a confirmed
/// one over a proposal. Withdrawn acknowledgements are ignored. Rows the
/// delta does not charge to the candidate are never touched.
///
/// `require_subjects` is sign-in being on: then only a confirmation given by
/// a signed-in person counts, and one from before sign-in reads as a
/// proposal, so a decision nobody authenticated never passes the gate.
pub fn apply(
    delta: &mut Delta,
    acks: &[Acknowledgement],
    change_id: Option<&str>,
    require_subjects: bool,
) -> Effective {
    let live: Vec<&Acknowledgement> = acks.iter().filter(|a| !a.withdrawn).collect();
    let (mut acknowledged, mut proposed, mut stale, mut uncovered) = (0, 0, 0, 0);
    for row in delta.rows.iter_mut() {
        if !row.bucket.charges_y() {
            row.acknowledgement = None;
            continue;
        }
        let key = Key::of_row(row);
        let best = live
            .iter()
            .filter(|a| a.key == key)
            .max_by_key(|a| (a.confirmed(require_subjects), a.id));
        let Some(ack) = best else {
            row.acknowledgement = None;
            if row.blocking {
                uncovered += 1;
            }
            continue;
        };
        let state = match (change_id, ack.confirmed(require_subjects)) {
            (None, _) => State::Stale,
            (Some(c), _) if c != ack.change_id => State::Stale,
            (_, true) => State::Acknowledged,
            (_, false) => State::Proposed,
        };
        let value_changed = match (&row.y, &ack.value_hash) {
            (Side::Hash(h), Some(v)) => h != v,
            _ => false,
        };
        row.acknowledgement = Some(RowAcknowledgement {
            id: ack.id,
            state,
            by: ack
                .acknowledged_by
                .clone()
                .unwrap_or_else(|| ack.proposed_by.clone()),
            note: ack.note.clone(),
            value_changed,
        });
        if row.blocking {
            match state {
                State::Acknowledged => acknowledged += 1,
                State::Proposed => proposed += 1,
                State::Stale => stale += 1,
            }
        }
    }
    // Every blocking charged row is counted exactly once above, and
    // `introduced` + `changed` is the same set counted when the delta was
    // built — a new state or a new bucket must keep the two in step. Checked
    // in the shipped binary too: a mismatch is a bug, and a bug in the
    // accounting is not allowed to pass as a decision.
    let counted = acknowledged + proposed + stale + uncovered;
    let charged = delta.verdict.introduced + delta.verdict.changed;
    debug_assert_eq!(counted, charged);
    let v = &mut delta.verdict;
    v.acknowledged = acknowledged;
    v.proposed = proposed;
    v.stale = stale;
    if counted != charged {
        eprintln!(
            "deja-orchestrator: acknowledgement accounting does not balance for {}: {counted} rows counted, {charged} charged",
            delta.y_run
        );
        v.overlay_failure = Some(format!(
            "the acknowledgement accounting does not balance: {counted} rows counted, {charged} charged"
        ));
        v.effective = None;
        return Effective::Fail;
    }
    let effective = if v.pass {
        Effective::Pass
    } else if uncovered == 0 && proposed == 0 && stale == 0 && acknowledged > 0 {
        Effective::Acknowledged
    } else {
        Effective::Fail
    };
    v.effective = Some(effective);
    effective
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::divergence::behaviour_tree::Lane;
    use crate::divergence::delta::{Bucket, DeltaVerdict, Uncovered};
    use std::collections::BTreeMap;

    fn body(corr: &str, path: &str) -> Address {
        Address::Body {
            correlation: corr.into(),
            json_path: path.into(),
        }
    }

    fn call(corr: &str, occurrence: u32, event: Option<u64>) -> Address {
        Address::Call {
            correlation: corr.into(),
            span_path: "request>grpc>authorize".into(),
            boundary: "http_outgoing".into(),
            operation: "call".into(),
            recorded_event: event,
            occurrence,
        }
    }

    fn lane() -> Lane {
        Lane {
            connector: "stripe".into(),
            flow: "authorize".into(),
        }
    }

    fn row(address: Address, bucket: Bucket, y: &str) -> Row {
        Row {
            address,
            bucket,
            m: Side::Tape,
            y: Side::Hash(y.into()),
            blocking: true,
            lane: Some(lane()),
            acknowledgement: None,
        }
    }

    fn delta(rows: Vec<Row>) -> Delta {
        let pass = !rows.iter().any(|r| r.blocking && r.bucket.charges_y());
        Delta {
            m_run: "m".into(),
            y_run: "y".into(),
            canon_version: 4,
            verdict: DeltaVerdict {
                pass,
                introduced: rows
                    .iter()
                    .filter(|r| r.bucket.family() == "introduced")
                    .count(),
                changed: 0,
                inherited: 0,
                resolved: 0,
                introduced_requests: 0,
                changed_requests: 0,
                inherited_requests: 0,
                resolved_requests: 0,
                reason: String::new(),
                acknowledged: 0,
                proposed: 0,
                stale: 0,
                effective: None,
                overlay_failure: None,
                unread_acknowledgements: 0,
            },
            buckets: BTreeMap::new(),
            lanes: Vec::new(),
            rows,
            clean: 0,
            covered_correlations: 1,
            uncovered: Uncovered {
                m_only: Vec::new(),
                y_only: Vec::new(),
                addresses: 0,
            },
            requests: BTreeMap::new(),
            request_lanes: BTreeMap::new(),
        }
    }

    fn ack(id: i64, key: Key, confirmed: bool) -> Acknowledgement {
        Acknowledgement {
            id,
            key,
            change_id: "c1".into(),
            value_hash: Some("v1".into()),
            note: "intended".into(),
            proposed_by: "author".into(),
            acknowledged_by: confirmed.then(|| "maintainer".to_owned()),
            withdrawn: false,
            proposed_by_sub: Some("sub-author".into()),
            acknowledged_by_sub: confirmed.then(|| "sub-maintainer".to_owned()),
        }
    }

    /// Sign-in on: a confirmation nobody authenticated is a proposal, and
    /// it does not outrank a newer signed-in proposal for the same shape.
    #[test]
    fn with_sign_in_on_a_confirmation_from_before_it_is_only_a_proposal() {
        let r = row(body("c", "$.suffix"), Bucket::Introduced, "v1");
        let key = Key::of_row(&r);
        let mut before = ack(1, key.clone(), true);
        before.proposed_by_sub = None;
        before.acknowledged_by_sub = None;
        // Off, it counts as it always did.
        let mut d = delta(vec![r.clone()]);
        assert_eq!(
            apply(&mut d, std::slice::from_ref(&before), Some("c1"), false),
            Effective::Acknowledged
        );
        // On, it is a proposal that asks to be decided again.
        let mut d = delta(vec![r.clone()]);
        assert_eq!(
            apply(&mut d, std::slice::from_ref(&before), Some("c1"), true),
            Effective::Fail
        );
        assert_eq!(
            d.rows[0].acknowledgement.as_ref().unwrap().state,
            State::Proposed
        );
        // A newer signed-in proposal wins over it, and a signed-in
        // confirmation passes.
        let signed = ack(2, key.clone(), false);
        let mut d = delta(vec![r.clone()]);
        apply(&mut d, &[before.clone(), signed], Some("c1"), true);
        assert_eq!(d.rows[0].acknowledgement.as_ref().unwrap().id, 2);
        let mut d = delta(vec![r]);
        assert_eq!(
            apply(&mut d, &[before, ack(3, key, true)], Some("c1"), true),
            Effective::Acknowledged
        );
    }

    #[test]
    fn a_pattern_drops_what_a_recording_decides() {
        assert_eq!(
            Pattern::of(&call("corr-1", 2, Some(7))),
            Pattern::of(&call("corr-9", 3, None))
        );
        assert_eq!(
            Pattern::of(&body("a", "$.x")),
            Pattern::of(&body("b", "$.x"))
        );
        assert_ne!(
            Pattern::of(&body("a", "$.x")),
            Pattern::of(&body("a", "$.y"))
        );
    }

    #[test]
    fn a_confirmed_acknowledgement_covers_the_row_on_a_later_recording() {
        let mut d = delta(vec![row(
            body("corr-2", "$.suffix"),
            Bucket::Introduced,
            "v1",
        )]);
        let key = Key::of_row(&row(body("corr-1", "$.suffix"), Bucket::Introduced, "v1"));
        let e = apply(&mut d, &[ack(1, key, true)], Some("c1"), false);
        assert_eq!(e, Effective::Acknowledged);
        let a = d.rows[0].acknowledgement.as_ref().unwrap();
        assert_eq!(a.state, State::Acknowledged);
        assert_eq!(a.by, "maintainer");
        assert!(!a.value_changed);
        assert_eq!(
            (d.verdict.acknowledged, d.verdict.proposed, d.verdict.stale),
            (1, 0, 0)
        );
    }

    #[test]
    fn a_proposal_alone_does_not_pass() {
        let r = row(body("c", "$.suffix"), Bucket::Introduced, "v1");
        let key = Key::of_row(&r);
        let mut d = delta(vec![r]);
        assert_eq!(
            apply(&mut d, &[ack(1, key, false)], Some("c1"), false),
            Effective::Fail
        );
        assert_eq!(
            d.rows[0].acknowledgement.as_ref().unwrap().state,
            State::Proposed
        );
        assert_eq!(d.verdict.proposed, 1);
    }

    #[test]
    fn a_changed_change_makes_it_stale() {
        let r = row(body("c", "$.suffix"), Bucket::Introduced, "v1");
        let key = Key::of_row(&r);
        let mut d = delta(vec![r]);
        assert_eq!(
            apply(&mut d, &[ack(1, key, true)], Some("c2"), false),
            Effective::Fail
        );
        assert_eq!(
            d.rows[0].acknowledgement.as_ref().unwrap().state,
            State::Stale
        );
        assert_eq!(d.verdict.stale, 1);
    }

    #[test]
    fn a_withdrawn_acknowledgement_is_as_if_absent() {
        let r = row(body("c", "$.suffix"), Bucket::Introduced, "v1");
        let key = Key::of_row(&r);
        let mut a = ack(1, key, true);
        a.withdrawn = true;
        let mut d = delta(vec![r]);
        assert_eq!(apply(&mut d, &[a], Some("c1"), false), Effective::Fail);
        assert!(d.rows[0].acknowledgement.is_none());
    }

    #[test]
    fn a_different_value_is_flagged_but_still_covered() {
        let r = row(body("c", "$.suffix"), Bucket::Introduced, "v2");
        let key = Key::of_row(&r);
        let mut d = delta(vec![r]);
        assert_eq!(
            apply(&mut d, &[ack(1, key, true)], Some("c1"), false),
            Effective::Acknowledged
        );
        assert!(d.rows[0].acknowledgement.as_ref().unwrap().value_changed);
    }

    #[test]
    fn the_newest_confirmed_acknowledgement_wins_over_a_later_proposal() {
        let r = row(body("c", "$.suffix"), Bucket::Introduced, "v1");
        let key = Key::of_row(&r);
        let mut d = delta(vec![r]);
        let acks = [ack(1, key.clone(), true), ack(2, key, false)];
        assert_eq!(
            apply(&mut d, &acks, Some("c1"), false),
            Effective::Acknowledged
        );
        assert_eq!(d.rows[0].acknowledgement.as_ref().unwrap().id, 1);
    }

    #[test]
    fn inherited_rows_and_passing_deltas_are_left_alone() {
        let r = row(body("c", "$.suffix"), Bucket::Inherited, "v1");
        let key = Key::of_row(&r);
        let mut d = delta(vec![r]);
        assert_eq!(
            apply(&mut d, &[ack(1, key, true)], Some("c1"), false),
            Effective::Pass
        );
        assert!(d.rows[0].acknowledgement.is_none());
        assert_eq!(d.verdict.acknowledged, 0);
    }

    #[test]
    fn a_run_without_a_change_id_marks_everything_stale() {
        let r = row(call("c", 1, Some(3)), Bucket::Introduced, "v1");
        let key = Key::of_row(&r);
        let mut d = delta(vec![r]);
        assert_eq!(
            apply(&mut d, &[ack(1, key, true)], None, false),
            Effective::Fail
        );
        assert_eq!(
            d.rows[0].acknowledgement.as_ref().unwrap().state,
            State::Stale
        );
        assert_eq!(d.verdict.stale, 1);
    }
}
