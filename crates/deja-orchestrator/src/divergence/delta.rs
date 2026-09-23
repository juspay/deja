//! The delta between two runs of one tape, three-way against the tape.
//!
//! Every address has up to three values: A, whether the tape holds it; M, what
//! the first run produced; Y, what the second run produced. That is the shape
//! of a three-way merge, and the buckets fall out the way git's do:
//!
//! | A vs M | M vs Y | A vs Y | bucket                       |
//! |--------|--------|--------|------------------------------|
//! | same   | same   | same   | clean                        |
//! | differ | same   | differ | inherited: main already moved it, Y carries it |
//! | same   | differ | differ | introduced: only Y moved it  |
//! | differ | differ | same   | resolved: Y restored the tape |
//! | differ | differ | differ | changed: both moved it, differently — flagged |
//!
//! plus the novel and omission variants for addresses only one side has.
//! `changed` is the conflict case of a merge: it is reported apart from both
//! inherited and introduced, never folded into either.
//!
//! The comparison is over the correlations BOTH runs drove. A run that stopped
//! early has no rows for the requests it never reached; those addresses are
//! reported as uncovered, never scored as if the run had reproduced them.
//!
//! The delta verdict counts only what Y introduced or changed, on blocking
//! addresses. The tape-relative verdict is untouched and shown beside it.

use std::collections::{BTreeMap, BTreeSet, HashSet};

use serde::{Deserialize, Serialize};

use super::behaviour_tree::{Address, BehaviourTree, Lane, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Bucket {
    Clean,
    Inherited,
    Introduced,
    Resolved,
    Changed,
    InheritedNovel,
    IntroducedNovel,
    ResolvedNovel,
    InheritedOmission,
    IntroducedOmission,
    ResolvedOmission,
}

impl Bucket {
    /// Whether this bucket charges Y.
    pub fn charges_y(self) -> bool {
        matches!(
            self,
            Bucket::Introduced
                | Bucket::Changed
                | Bucket::IntroducedNovel
                | Bucket::IntroducedOmission
        )
    }
    pub fn family(self) -> &'static str {
        match self {
            Bucket::Clean => "clean",
            Bucket::Inherited | Bucket::InheritedNovel | Bucket::InheritedOmission => "inherited",
            Bucket::Introduced | Bucket::IntroducedNovel | Bucket::IntroducedOmission => {
                "introduced"
            }
            Bucket::Resolved | Bucket::ResolvedNovel | Bucket::ResolvedOmission => "resolved",
            Bucket::Changed => "changed",
        }
    }
}

/// One side's value at an address, as the delta reports it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    /// The run reproduced the tape.
    Tape,
    /// The run never reached the address (or the tape does not hold it).
    Absent,
    /// The run produced this instead.
    Hash(String),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    pub address: Address,
    pub bucket: Bucket,
    pub m: Side,
    pub y: Side,
    pub blocking: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lane: Option<Lane>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LaneSummary {
    pub lane: Lane,
    pub requests: usize,
    /// Bucket family → addresses.
    pub buckets: BTreeMap<String, usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaVerdict {
    /// True when Y introduced or changed nothing on a blocking address.
    pub pass: bool,
    pub introduced: usize,
    pub changed: usize,
    pub inherited: usize,
    pub resolved: usize,
    pub reason: String,
}

/// What the comparison could not cover: requests only one run drove. Their
/// addresses are outside every bucket.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Uncovered {
    /// Correlations M drove that Y never reached.
    pub m_only: Vec<String>,
    /// Correlations Y drove that M never reached.
    pub y_only: Vec<String>,
    /// Addresses under those correlations, left out of the buckets.
    pub addresses: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Delta {
    pub m_run: String,
    pub y_run: String,
    pub canon_version: u32,
    pub verdict: DeltaVerdict,
    pub buckets: BTreeMap<Bucket, usize>,
    pub lanes: Vec<LaneSummary>,
    /// Every non-clean address, flagged ones first.
    pub rows: Vec<Row>,
    pub clean: usize,
    /// Correlations both runs drove: the comparison's domain.
    pub covered_correlations: usize,
    pub uncovered: Uncovered,
}

fn side_of(v: Option<&Value>, tape_has: bool) -> Side {
    match v {
        Some(Value::Reproduced) => Side::Tape,
        Some(Value::Absent) => Side::Absent,
        Some(Value::Diverged { hash }) | Some(Value::Novel { hash }) => Side::Hash(hash.clone()),
        // not in this run's tree, for a request the run DID drive: the scorer
        // lists every recorded call it classified, so an address it left out
        // is one the run reproduced. If the tape has no such address, the run
        // never had it.
        None if tape_has => Side::Tape,
        None => Side::Absent,
    }
}

fn classify(tape_has: bool, m: &Side, y: &Side) -> Bucket {
    use Side::*;
    if !tape_has {
        return match (m, y) {
            (Absent, Absent) | (Tape, Tape) => Bucket::Clean,
            (Hash(a), Hash(b)) if a == b => Bucket::InheritedNovel,
            (Hash(_), Hash(_)) => Bucket::Changed,
            (Hash(_), _) => Bucket::ResolvedNovel,
            (_, Hash(_)) => Bucket::IntroducedNovel,
            _ => Bucket::Clean,
        };
    }
    match (m, y) {
        (Tape, Tape) => Bucket::Clean,
        (Absent, Absent) => Bucket::InheritedOmission,
        (_, Absent) => Bucket::IntroducedOmission,
        (Absent, _) => Bucket::ResolvedOmission,
        (Hash(a), Hash(b)) if a == b => Bucket::Inherited,
        (Hash(_), Hash(_)) => Bucket::Changed,
        (Hash(_), Tape) => Bucket::Resolved,
        (Tape, Hash(_)) => Bucket::Introduced,
    }
}

/// Whether a cached delta document is the one a request for `(y, m)` would
/// compute now: the same two runs, built under the current rules. Both runs'
/// trees are fixed once scored, so a current document never goes stale on
/// its own; only a rule change retires it.
pub fn cached_is_current(doc: &serde_json::Value, y: &str, m: &str, canon_version: u32) -> bool {
    doc.get("y_run").and_then(|v| v.as_str()) == Some(y)
        && doc.get("m_run").and_then(|v| v.as_str()) == Some(m)
        && doc.get("canon_version").and_then(|v| v.as_u64()) == Some(u64::from(canon_version))
        && doc.get("verdict").is_some()
}

/// Compare Y against M with the tape as ancestor.
pub fn three_way(m: &BehaviourTree, y: &BehaviourTree) -> Result<Delta, String> {
    if m.canon_version != y.canon_version {
        return Err(format!(
            "the two trees were built under different rules ({} and {}); rebuild one before comparing",
            m.canon_version, y.canon_version
        ));
    }
    // The trees hash what each candidate captured, in the encoding of the
    // deja it links. Across two encodings every affected address would read
    // as changed behaviour, so the tape verdict is the one to read there.
    if m.event_schema_versions != y.event_schema_versions {
        return Err(format!(
            "the candidates capture under different event schemas ({:?} and {:?}), so an encoding change would read as a behaviour change; read each run's tape verdict instead",
            m.event_schema_versions, y.event_schema_versions
        ));
    }
    // The domain: requests both runs drove. A tree that names no correlations
    // drove nothing, and there is nothing to compare it on.
    let covered: BTreeSet<&String> = m.correlations.intersection(&y.correlations).collect();
    if covered.is_empty() {
        return Err(format!(
            "the runs share no request: M drove {} and Y drove {}; a run that stopped before its first request has no behaviour to compare",
            m.correlations.len(),
            y.correlations.len()
        ));
    }
    let uncovered = Uncovered {
        m_only: m
            .correlations
            .difference(&y.correlations)
            .cloned()
            .collect(),
        y_only: y
            .correlations
            .difference(&m.correlations)
            .cloned()
            .collect(),
        addresses: 0,
    };
    let mut uncovered = uncovered;

    let m_by: BTreeMap<&Address, &super::behaviour_tree::Entry> =
        m.entries.iter().map(|e| (&e.address, e)).collect();
    let y_by: BTreeMap<&Address, &super::behaviour_tree::Entry> =
        y.entries.iter().map(|e| (&e.address, e)).collect();
    let addresses: HashSet<&Address> = m_by.keys().chain(y_by.keys()).copied().collect();
    let total = addresses.len();
    let mut rows = Vec::new();
    let mut buckets: BTreeMap<Bucket, usize> = BTreeMap::new();
    let mut clean = 0usize;
    let mut lanes: BTreeMap<Lane, (HashSet<String>, BTreeMap<String, usize>)> = BTreeMap::new();
    for address in addresses {
        if !covered.contains(&address.correlation().to_owned()) {
            uncovered.addresses += 1;
            continue;
        }
        let me = m_by.get(address).copied();
        let ye = y_by.get(address).copied();
        // the tape holds the address unless every side that has it calls it novel
        let tape_has = [me, ye]
            .iter()
            .flatten()
            .any(|e| !matches!(e.value, Value::Novel { .. }));
        let m_side = side_of(me.map(|e| &e.value), tape_has);
        let y_side = side_of(ye.map(|e| &e.value), tape_has);
        let bucket = classify(tape_has, &m_side, &y_side);
        let blocking =
            me.map(|e| e.blocking).unwrap_or(false) || ye.map(|e| e.blocking).unwrap_or(false);
        let lane = y
            .lanes
            .get(address.correlation())
            .or_else(|| m.lanes.get(address.correlation()))
            .cloned();
        if let Some(l) = &lane {
            let slot = lanes.entry(l.clone()).or_default();
            slot.0.insert(address.correlation().to_owned());
            *slot.1.entry(bucket.family().to_owned()).or_default() += 1;
        }
        if bucket == Bucket::Clean {
            clean += 1;
            continue;
        }
        *buckets.entry(bucket).or_default() += 1;
        rows.push(Row {
            address: address.clone(),
            bucket,
            m: m_side,
            y: y_side,
            blocking,
            lane,
        });
    }
    // every address lands in exactly one place: a bucket row, clean, or
    // uncovered — a new bucket must keep this true
    debug_assert_eq!(rows.len() + clean + uncovered.addresses, total);

    let order = |b: Bucket| match b.family() {
        "changed" => 0,
        "introduced" => 1,
        "resolved" => 2,
        _ => 3,
    };
    rows.sort_by(|a, b| {
        order(a.bucket)
            .cmp(&order(b.bucket))
            .then_with(|| a.address.cmp(&b.address))
    });
    let count = |f: &str| rows.iter().filter(|r| r.bucket.family() == f).count();
    let introduced = rows
        .iter()
        .filter(|r| r.blocking && r.bucket.charges_y() && r.bucket != Bucket::Changed)
        .count();
    let changed = rows
        .iter()
        .filter(|r| r.blocking && r.bucket == Bucket::Changed)
        .count();
    let inherited = count("inherited");
    let resolved = count("resolved");
    let pass = introduced == 0 && changed == 0;
    let mut reason = if pass {
        if inherited > 0 {
            format!("Y introduced nothing beyond what M already carries ({inherited} inherited)")
        } else {
            "Y behaves as M does on every address".to_owned()
        }
    } else {
        let mut parts = Vec::new();
        if introduced > 0 {
            parts.push(format!("{introduced} introduced"));
        }
        if changed > 0 {
            parts.push(format!(
                "{changed} changed where M had already moved (flagged)"
            ));
        }
        parts.join("; ")
    };
    if !uncovered.m_only.is_empty() || !uncovered.y_only.is_empty() {
        reason.push_str(&format!(
            "; {} request(s) only one run drove are not compared",
            uncovered.m_only.len() + uncovered.y_only.len()
        ));
    }
    let mut lane_summaries: Vec<LaneSummary> = lanes
        .into_iter()
        .map(|(lane, (reqs, b))| LaneSummary {
            lane,
            requests: reqs.len(),
            buckets: b,
        })
        .collect();
    lane_summaries.sort_by_key(|l| {
        let hot: usize = l
            .buckets
            .iter()
            .filter(|(k, _)| k.as_str() != "clean")
            .map(|(_, n)| *n)
            .sum();
        (std::cmp::Reverse(hot), std::cmp::Reverse(l.requests))
    });
    Ok(Delta {
        m_run: m.run_id.clone(),
        y_run: y.run_id.clone(),
        canon_version: m.canon_version,
        verdict: DeltaVerdict {
            pass,
            introduced,
            changed,
            inherited,
            resolved,
            reason,
        },
        buckets,
        lanes: lane_summaries,
        rows,
        clean,
        covered_correlations: covered.len(),
        uncovered,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::divergence::behaviour_tree::{Entry, CANON_VERSION};

    const SPAN: &str = "request>deja::grpc_incoming>payment_sync>x";

    /// A call that paired to recorded event `event`.
    fn call(corr: &str, event: u64) -> Address {
        Address::Call {
            correlation: corr.into(),
            span_path: SPAN.into(),
            boundary: "http_outgoing".into(),
            operation: "call".into(),
            recorded_event: Some(event),
            occurrence: 0,
        }
    }
    /// The n-th novel call under the span: no recorded counterpart.
    fn novel(corr: &str, n: u32) -> Address {
        Address::Call {
            correlation: corr.into(),
            span_path: SPAN.into(),
            boundary: "http_outgoing".into(),
            operation: "call".into(),
            recorded_event: None,
            occurrence: n,
        }
    }
    fn tree(run: &str, entries: Vec<(Address, Value)>) -> BehaviourTree {
        let correlations = entries
            .iter()
            .map(|(a, _)| a.correlation().to_owned())
            .chain(std::iter::once("c".to_owned()))
            .collect();
        BehaviourTree {
            run_id: run.into(),
            canon_version: CANON_VERSION,
            entries: entries
                .into_iter()
                .map(|(address, value)| Entry {
                    address,
                    value,
                    blocking: true,
                })
                .collect(),
            lanes: [(
                "c".to_owned(),
                Lane {
                    connector: "paypal".into(),
                    flow: "sync".into(),
                },
            )]
            .into_iter()
            .collect(),
            correlations,
            event_schema_versions: [10].into_iter().collect(),
        }
    }
    fn div(h: &str) -> Value {
        Value::Diverged { hash: h.into() }
    }
    fn nov(h: &str) -> Value {
        Value::Novel { hash: h.into() }
    }

    #[test]
    fn every_row_of_the_bucket_table() {
        let m = tree(
            "m",
            vec![
                (call("c", 0), Value::Reproduced), // clean
                (call("c", 1), div("v2")),         // inherited: Y sends v2 too
                (call("c", 2), Value::Reproduced), // introduced: only Y moves
                (call("c", 3), div("v2")),         // resolved: Y back to tape
                (call("c", 4), div("v2")),         // changed: Y sends v3
                (call("c", 5), Value::Absent),     // inherited omission
                (call("c", 6), Value::Reproduced), // introduced omission
                (call("c", 7), Value::Absent),     // resolved omission
                (novel("c", 0), nov("n")),         // inherited novel
                (novel("c", 1), nov("n")),         // resolved novel
            ],
        );
        let y = tree(
            "y",
            vec![
                (call("c", 0), Value::Reproduced),
                (call("c", 1), div("v2")),
                (call("c", 2), div("v9")),
                (call("c", 3), Value::Reproduced),
                (call("c", 4), div("v3")),
                (call("c", 5), Value::Absent),
                (call("c", 6), Value::Absent),
                (call("c", 7), Value::Reproduced),
                (novel("c", 0), nov("n")),
                (novel("c", 2), nov("q")), // introduced novel
            ],
        );
        let d = three_way(&m, &y).unwrap();
        let by = |a: Address| d.rows.iter().find(|r| r.address == a).map(|r| r.bucket);
        assert_eq!(d.clean, 1);
        assert_eq!(by(call("c", 1)), Some(Bucket::Inherited));
        assert_eq!(by(call("c", 2)), Some(Bucket::Introduced));
        assert_eq!(by(call("c", 3)), Some(Bucket::Resolved));
        assert_eq!(by(call("c", 4)), Some(Bucket::Changed));
        assert_eq!(by(call("c", 5)), Some(Bucket::InheritedOmission));
        assert_eq!(by(call("c", 6)), Some(Bucket::IntroducedOmission));
        assert_eq!(by(call("c", 7)), Some(Bucket::ResolvedOmission));
        assert_eq!(by(novel("c", 0)), Some(Bucket::InheritedNovel));
        assert_eq!(by(novel("c", 1)), Some(Bucket::ResolvedNovel));
        assert_eq!(by(novel("c", 2)), Some(Bucket::IntroducedNovel));
        assert!(!d.verdict.pass);
        assert_eq!((d.verdict.introduced, d.verdict.changed), (3, 1));
        assert_eq!(d.rows[0].bucket, Bucket::Changed, "flagged rows come first");
        assert_eq!(d.lanes[0].requests, 1);
        assert_eq!(
            d.rows.len() + d.clean,
            11,
            "every address lands in exactly one place"
        );
        assert_eq!(d.uncovered.addresses, 0);
    }

    #[test]
    fn the_same_run_twice_is_a_passing_delta_however_far_it_left_the_tape() {
        let entries = vec![(call("c", 0), div("v2")), (call("c", 1), Value::Absent)];
        let m = tree("m", entries.clone());
        let y = tree("y", entries);
        let d = three_way(&m, &y).unwrap();
        assert!(d.verdict.pass, "{}", d.verdict.reason);
        assert_eq!(d.verdict.inherited, 2);
        assert!(d.verdict.reason.contains("inherited"));
    }

    #[test]
    fn an_address_missing_from_one_tree_means_that_run_reproduced_the_tape() {
        // M's tree lists only what M diverged on; an address Y diverged on and
        // M's tree omits is one M reproduced — for a request M drove.
        let m = tree("m", vec![]);
        let y = tree("y", vec![(call("c", 0), div("v9"))]);
        let d = three_way(&m, &y).unwrap();
        assert_eq!(d.rows[0].bucket, Bucket::Introduced);
        assert_eq!(d.rows[0].m, Side::Tape);
    }

    #[test]
    fn an_added_call_does_not_shift_the_calls_after_it() {
        // M makes A then B, where B diverges. Y makes X, A, B: the same
        // divergence at B, plus one extra call first. B is inherited; X is the
        // only thing Y introduced; nothing is resolved.
        let m = tree(
            "m",
            vec![
                (call("c", 10), Value::Reproduced),
                (call("c", 11), div("b-moved")),
            ],
        );
        let y = tree(
            "y",
            vec![
                (novel("c", 0), nov("x")),
                (call("c", 10), Value::Reproduced),
                (call("c", 11), div("b-moved")),
            ],
        );
        let d = three_way(&m, &y).unwrap();
        let by = |a: Address| d.rows.iter().find(|r| r.address == a).map(|r| r.bucket);
        assert_eq!(by(call("c", 11)), Some(Bucket::Inherited));
        assert_eq!(by(novel("c", 0)), Some(Bucket::IntroducedNovel));
        assert_eq!(d.buckets.get(&Bucket::Resolved), None);
        assert_eq!(d.buckets.get(&Bucket::Introduced), None);
        assert_eq!(d.verdict.introduced, 1, "only the added call charges Y");
        assert_eq!(d.verdict.inherited, 1);
    }

    #[test]
    fn a_request_only_one_run_drove_is_uncovered_not_clean() {
        // Y stopped before c2. Its addresses are neither reproduced nor
        // resolved: they are outside the comparison, and the reason says so.
        let mut m = tree(
            "m",
            vec![(call("c", 0), div("v")), (call("c2", 0), div("w"))],
        );
        m.correlations = ["c", "c2"].into_iter().map(String::from).collect();
        let mut y = tree("y", vec![(call("c", 0), div("v"))]);
        y.correlations = ["c"].into_iter().map(String::from).collect();
        let d = three_way(&m, &y).unwrap();
        assert!(d.verdict.pass);
        assert_eq!(d.verdict.inherited, 1);
        assert_eq!(d.verdict.resolved, 0);
        assert_eq!(d.uncovered.m_only, vec!["c2".to_owned()]);
        assert_eq!(d.uncovered.addresses, 1);
        assert_eq!(d.covered_correlations, 1);
        assert!(
            d.verdict.reason.contains("not compared"),
            "{}",
            d.verdict.reason
        );
    }

    #[test]
    fn runs_that_share_no_request_do_not_compare() {
        let mut m = tree("m", vec![(call("c", 0), div("v"))]);
        m.correlations = ["c"].into_iter().map(String::from).collect();
        let mut y = tree("y", vec![]);
        y.correlations = BTreeSet::new();
        assert!(three_way(&m, &y).is_err());
    }

    #[test]
    fn a_cached_delta_is_current_only_for_its_own_runs_and_rules() {
        let doc = serde_json::json!({"y_run": "y", "m_run": "m", "canon_version": CANON_VERSION, "verdict": {"pass": true}});
        assert!(cached_is_current(&doc, "y", "m", CANON_VERSION));
        assert!(
            !cached_is_current(&doc, "y", "other", CANON_VERSION),
            "a different baseline"
        );
        assert!(
            !cached_is_current(&doc, "y", "m", CANON_VERSION + 1),
            "a rule change retires it"
        );
        let pending = serde_json::json!({"unavailable": "baseline not scored"});
        assert!(
            !cached_is_current(&pending, "y", "m", CANON_VERSION),
            "an unavailable answer is never cached as a delta"
        );
    }

    #[test]
    fn trees_from_different_canon_versions_do_not_compare() {
        let m = tree("m", vec![]);
        let mut y = tree("y", vec![]);
        y.canon_version += 1;
        assert!(three_way(&m, &y).is_err());
    }

    #[test]
    fn candidates_on_different_event_schemas_do_not_compare() {
        let at = |seq| Address::Status {
            correlation: "c".into(),
            request_sequence: seq,
        };
        let m = tree("m", vec![(at(0), Value::Reproduced)]);
        let same = tree("y", vec![(at(0), Value::Reproduced)]);
        assert!(
            three_way(&m, &same).is_ok(),
            "one schema on both sides compares"
        );

        let mut bumped = same.clone();
        bumped.event_schema_versions = [11].into_iter().collect();
        let why = three_way(&m, &bumped).err().unwrap();
        assert!(why.contains("different event schemas"), "{why}");
    }
}
