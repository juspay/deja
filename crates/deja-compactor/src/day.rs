//! A recording is sealed one UTC day at a time.
//!
//! A recorder session lives as long as its pod, and a pod can live for days. A
//! seal per session therefore grows for the pod's whole life, re-seals the old
//! days on every pass, and is grouped under the day the pod booted rather than
//! the day the traffic was served. A session whose landing spans more than one
//! date partition is split into one recording per day instead, named
//! `<session>.<YYYY-MM-DD>`. A session still within one partition keeps its
//! bare id; if it later spans dates, replay refuses that older bare id rather
//! than silently reading a stale single-day seal.
//!
//! The partition only says WHERE to read. Which day a record belongs to is the
//! UTC date its request's ingress — `request_sequence` 0 — was recorded at, so a
//! request that started before midnight is sealed whole into the earlier day
//! even when its tail lands in a later partition. A day's selection considers
//! every listed partition, so a request has no duration bound and its ingress
//! remains available to assign arbitrarily late tails to the starting day.

use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

/// A recording id split into the session it reads and the day it keeps, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordingDay<'a> {
    pub session_id: &'a str,
    pub day: Option<&'a str>,
}

/// Split a recording id. An id without a well-formed date suffix is a whole
/// session, whatever else it contains.
pub fn split_recording_id(recording_id: &str) -> RecordingDay<'_> {
    match recording_id.rsplit_once('.') {
        Some((session_id, day)) if !session_id.is_empty() && is_date(day) => RecordingDay {
            session_id,
            day: Some(day),
        },
        _ => RecordingDay {
            session_id: recording_id,
            day: None,
        },
    }
}

/// The id of one day of a session.
pub fn day_recording_id(session_id: &str, day: &str) -> String {
    format!("{session_id}.{day}")
}

/// `YYYY-MM-DD`, the shape the aggregator writes into `dt=`.
fn is_date(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 10
        && b[4] == b'-'
        && b[7] == b'-'
        && b.iter()
            .enumerate()
            .all(|(i, c)| i == 4 || i == 7 || c.is_ascii_digit())
}

use std::time::{SystemTime, UNIX_EPOCH};

// Civil-from-days (H. Hinnant), shared by captured timestamps and the
// publication clock so both agree on which UTC calendar day has ended.
fn civil_from_days(days: i64) -> String {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!("{year:04}-{month:02}-{day:02}")
}

/// The UTC date of a nanosecond timestamp, as `YYYY-MM-DD`.
pub fn utc_date(unix_nanos: u64) -> String {
    civil_from_days((unix_nanos / 86_400_000_000_000) as i64)
}

/// The current UTC calendar day in canonical `YYYY-MM-DD` form.
pub fn current_utc_day(now: SystemTime) -> Result<String, String> {
    let days = now
        .duration_since(UNIX_EPOCH)
        .map_err(|error| format!("current time precedes Unix epoch: {error}"))?
        .as_secs()
        / 86_400;
    let days = i64::try_from(days).map_err(|_| "current UTC day exceeds i64 range")?;
    Ok(civil_from_days(days))
}

/// Whether a canonical UTC calendar day has ended at the supplied current day.
pub fn day_has_ended(day: &str, current_utc_day: &str) -> Result<bool, String> {
    fn valid(value: &str) -> bool {
        if value.len() != 10
            || !value.as_bytes().iter().enumerate().all(|(i, b)| {
                if i == 4 || i == 7 {
                    *b == b'-'
                } else {
                    b.is_ascii_digit()
                }
            })
        {
            return false;
        }
        let (year, month, date) = (&value[..4], &value[5..7], &value[8..]);
        let (Ok(year), Ok(month), Ok(date)) = (
            year.parse::<u32>(),
            month.parse::<u32>(),
            date.parse::<u32>(),
        ) else {
            return false;
        };
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let max_day = match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 if leap => 29,
            2 => 28,
            _ => return false,
        };
        date > 0 && date <= max_day
    }
    if !valid(day) {
        return Err(format!(
            "invalid UTC day {day:?}; expected canonical YYYY-MM-DD"
        ));
    }
    if !valid(current_utc_day) {
        return Err(format!(
            "invalid current UTC day {current_utc_day:?}; expected canonical YYYY-MM-DD"
        ));
    }
    Ok(day < current_utc_day)
}

#[cfg(test)]
mod utc_day_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn utc_day_boundary_is_strict() {
        let day_start = UNIX_EPOCH + Duration::from_secs(1_798_761_600); // 2027-01-01T00:00:00Z
        let cutoff = |day, current| day_has_ended(day, current).expect("valid UTC calendar days");
        assert_eq!(
            current_utc_day(day_start - Duration::from_secs(1)).expect("valid clock"),
            "2026-12-31"
        );
        assert_eq!(
            current_utc_day(day_start).expect("valid clock"),
            "2027-01-01"
        );
        assert!(!cutoff("2026-12-31", "2026-12-31"));
        assert!(cutoff("2026-12-31", "2027-01-01"));
        assert!(!cutoff("2027-01-01", "2026-12-31"));
        assert!(cutoff("2026-12-30", "2026-12-31"));
        assert!(cutoff("2024-02-29", "2024-03-01"));
        assert!(day_has_ended("2026-02-30", "2026-03-01").is_err());
    }
}
#[derive(Deserialize)]
struct LineProbe {
    #[serde(default)]
    artifact_type: Option<String>,
    #[serde(default)]
    instance_id: Option<String>,
    #[serde(default)]
    event: Option<RecordProbe>,
    #[serde(default)]
    node: Option<RecordProbe>,
}

#[derive(Deserialize)]
struct RecordProbe {
    #[serde(default)]
    global_sequence: Option<u64>,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    request_sequence: Option<u64>,
    /// A boundary event's start.
    #[serde(default)]
    timestamp_ns: Option<u64>,
    /// A graph node's start.
    #[serde(default)]
    started_ns: Option<u64>,
    /// Legacy boundary envelopes used this name for the same UTC nanoseconds.
    #[serde(default)]
    started_at_unix_nanos: Option<u64>,
}

impl LineProbe {
    fn record(&self) -> Option<&RecordProbe> {
        match self.artifact_type.as_deref() {
            Some(crate::ARTIFACT_TYPE_GRAPH_NODE) => self.node.as_ref(),
            _ => self.event.as_ref(),
        }
    }
}

/// What one line is, for the purpose of choosing its day.
struct Placed {
    correlation_id: Option<String>,
    request_sequence: Option<u64>,
    started_ns: Option<u64>,
    /// `(instance, gseq)` of a boundary event, the space coverage is counted in.
    boundary_seq: Option<(String, u64)>,
}

fn place(line: &str) -> Option<Placed> {
    let probe: LineProbe = serde_json::from_str(line).ok()?;
    let record = probe.record()?;
    let graph = probe.artifact_type.as_deref() == Some(crate::ARTIFACT_TYPE_GRAPH_NODE);
    let boundary_seq = match graph {
        true => None,
        false => record.global_sequence.map(|g| {
            (
                probe
                    .instance_id
                    .clone()
                    .unwrap_or_else(|| "unknown".to_owned()),
                g,
            )
        }),
    };
    Some(Placed {
        correlation_id: record.correlation_id.clone(),
        // A graph node's `sequence` is its own; only a boundary event says
        // where it falls in its request.
        request_sequence: record.request_sequence.filter(|_| !graph),
        started_ns: record
            .timestamp_ns
            .or(record.started_ns)
            .or(record.started_at_unix_nanos),
        boundary_seq,
    })
}

/// Latest envelope timestamp when an older bare recording lacks sequence-zero
/// ingress metadata. It cannot prove the request's start day, but it can refute
/// a completed-day claim when a dated source contains a newer event.
pub(crate) fn latest_observed_utc_day(objects: &[(&str, &[u8])]) -> Option<String> {
    let mut latest: Option<String> = None;
    for (_, chunk) in objects {
        for line in chunk.split(|&byte| byte == b'\n') {
            let text = String::from_utf8_lossy(line);
            if let Some(timestamp) = place(&text).and_then(|record| record.started_ns) {
                let observed = utc_date(timestamp);
                if latest
                    .as_ref()
                    .is_none_or(|prior| observed.as_str() > prior.as_str())
                {
                    latest = Some(observed);
                }
            }
        }
    }
    latest
}

/// Membership information derived from one durable object.
pub(crate) struct ObjectMembership {
    pub(crate) requests: Vec<deja_store::RequestDayMembership>,
    pub(crate) correlation_ids: Vec<String>,
    pub(crate) object_days: Vec<String>,
    pub(crate) unresolved_correlations: Vec<String>,
}

/// Derive durable object memberships from actual envelopes, not their
/// aggregator partition paths. Correlation ids are stored on every contributing
/// object; only sequence-zero ingress records carry a day assignment. An object
/// with an unresolved correlated record must be refused by the caller.
pub(crate) fn index_objects(objects: &[(&str, &[u8])]) -> Vec<ObjectMembership> {
    let mut request_days = BTreeMap::<String, String>::new();
    for (_, chunk) in objects {
        for line in chunk.split(|&b| b == b'\n') {
            let line = String::from_utf8_lossy(line);
            if let Some(p) = place(&line) {
                if p.request_sequence == Some(0) {
                    if let (Some(correlation), Some(timestamp)) = (p.correlation_id, p.started_ns) {
                        request_days.insert(correlation, utc_date(timestamp));
                    }
                }
            }
        }
    }
    objects
        .iter()
        .map(|(_, chunk)| {
            let mut requests = BTreeMap::<String, String>::new();
            let mut correlations = BTreeSet::new();
            let mut unresolved = BTreeSet::new();
            let mut object_days = BTreeSet::new();
            for line in chunk.split(|&b| b == b'\n') {
                let line = String::from_utf8_lossy(line);
                let Some(p) = place(&line) else { continue };
                if let Some(correlation) = p.correlation_id {
                    correlations.insert(correlation.clone());
                    if let Some(day) = request_days.get(&correlation) {
                        requests.insert(correlation, day.clone());
                    } else {
                        unresolved.insert(correlation);
                    }
                } else if let Some(timestamp) = p.started_ns {
                    object_days.insert(utc_date(timestamp));
                }
            }
            ObjectMembership {
                requests: requests
                    .into_iter()
                    .map(|(correlation_id, day)| deja_store::RequestDayMembership {
                        correlation_id,
                        day,
                    })
                    .collect(),
                correlation_ids: correlations.into_iter().collect(),
                object_days: object_days.into_iter().collect(),
                unresolved_correlations: unresolved.into_iter().collect(),
            }
        })
        .collect()
}
#[cfg(test)]
mod indexing_tests {
    use super::*;

    #[test]
    fn orphan_correlations_are_explicitly_reported_by_object_index() {
        let valid = format!(
            r#"{{"schema_version":2,"instance_id":"i1","event":{{"correlation_id":"valid","request_sequence":0,"timestamp_ns":{}}}}}"#,
            1_791_417_598_000_000_000u64
        );
        let orphan =
            br#"{"schema_version":2,"instance_id":"i1","event":{"correlation_id":"orphan","request_sequence":1}}"#;
        let objects = [
            ("2026-10-07", valid.as_bytes()),
            ("2026-10-08", orphan.as_slice()),
        ];
        let indexed = index_objects(&objects);
        assert!(indexed[0].unresolved_correlations.is_empty());
        assert_eq!(
            indexed[1].unresolved_correlations,
            vec!["orphan".to_owned()]
        );
    }
}

/// The lines of ONE day, out of objects read across the partitions that can
/// hold it.
pub(crate) struct DaySelection {
    /// Per object, the lines that belong to the day, in object order.
    pub kept: Vec<Vec<String>>,
    /// Lines read that belong to another day — sealed there, not lost.
    pub other_day_lines: usize,
    /// Retained lines of requests whose sequence-zero ingress was not found.
    pub lines_without_ingress: usize,
    /// At least one correlated record has no sequence-zero ingress, and cannot
    /// be assigned to an exact logical day.
    pub has_unresolved_ingress: bool,
    /// Correlations without ingress in these source objects. Their objects may
    /// be absent from every indexed day dependency, so callers must refuse to
    /// publish any day until these are resolved.
    pub unresolved_correlations: BTreeSet<String>,
    /// Boundary-event sequences set aside for another day, per instance.
    pub elsewhere: BTreeMap<String, BTreeSet<u64>>,
}

/// Keep the lines of `day` out of `objects`, each paired with the partition
/// date its key named. `days` is every day the session is listed under.
///
/// A correlated record goes where its request's ingress falls, so a request is
/// never split across two seals; a request whose ingress is not among the
/// objects began before them. An uncorrelated record goes where its own
/// timestamp falls. A record with no timestamp, and every marker, goes where
/// the aggregator put it: the partition is the only date it has. Every listed
/// partition is considered when selecting a day; if no selected lines belong
/// to that day, an empty-day manifest records that terminal result.
/// Missing-ingress request lines are retained with the earliest partition that
/// contains them, but remain explicitly counted as incomplete in the manifest.
pub(crate) fn select_day(
    objects: &[(&str, &[u8])],
    day: &str,
    _days: &BTreeSet<String>,
) -> DaySelection {
    select_day_with_request_days(objects, day, &BTreeMap::new())
}

/// Resolve a day's object lines using the durable ingress index. An S3 object
/// can mix tails from previous days with today's ingresses; fetching only its
/// indexed day dependencies does not necessarily fetch the earlier ingress.
pub(crate) fn select_day_with_request_days(
    objects: &[(&str, &[u8])],
    day: &str,
    known_request_days: &BTreeMap<String, String>,
) -> DaySelection {
    #[derive(Default)]
    struct Request {
        ingress_ns: Option<u64>,
        dated: bool,
        earliest_partition: Option<String>,
    }
    let mut requests: BTreeMap<String, Request> = BTreeMap::new();
    for (partition, chunk) in objects {
        for line in chunk.split(|&b| b == b'\n') {
            let line = String::from_utf8_lossy(line);
            let Some(p) = place(&line) else { continue };
            let Some(c) = p.correlation_id else { continue };
            let r = requests.entry(c).or_default();
            r.dated |= p.started_ns.is_some() || p.request_sequence.is_some();
            if r.earliest_partition
                .as_deref()
                .is_none_or(|d| *partition < d)
            {
                r.earliest_partition = Some((*partition).to_owned());
            }
            if p.request_sequence == Some(0) {
                r.ingress_ns = p.started_ns.or(r.ingress_ns);
            }
        }
    }
    // `Some(day)` where the request belongs; `None` only when its ingress
    // timestamp cannot be recovered. Preserve the request on its earliest
    // observed partition in that case; the manifest still counts its lines as
    // lacking ingress, so this is not represented as a complete request.
    let mut request_day: BTreeMap<String, (Option<String>, bool)> = requests
        .into_iter()
        .filter_map(|(c, r)| match (r.ingress_ns, r.dated) {
            (Some(ns), _) => Some((c, (Some(utc_date(ns)), false))),
            (None, true) => Some((c, (r.earliest_partition, true))),
            (None, false) => None,
        })
        .collect();
    let conflicting_ingress = known_request_days.iter().any(|(correlation, indexed_day)| {
        request_day
            .get(correlation)
            .is_some_and(|(local_day, missing)| {
                !missing && local_day.as_deref() != Some(indexed_day.as_str())
            })
    });
    for (correlation, indexed_day) in known_request_days {
        request_day.insert(correlation.clone(), (Some(indexed_day.clone()), false));
    }

    let mut kept = Vec::with_capacity(objects.len());
    let mut other_day_lines = 0usize;
    let mut lines_without_ingress = 0usize;
    let mut has_unresolved_ingress = conflicting_ingress;
    let mut unresolved_correlations = BTreeSet::new();
    let mut elsewhere: BTreeMap<String, BTreeSet<u64>> = BTreeMap::new();
    for (partition, chunk) in objects {
        let mut mine = Vec::new();
        for line in chunk.split(|&b| b == b'\n') {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let line = String::from_utf8_lossy(line).into_owned();
            let placed = place(&line);
            // Outer `None`: the line's own timestamp (or partition) decides.
            // `Some(None)`: a request whose ingress was not read.
            let by_request: Option<(Option<String>, bool)> = placed
                .as_ref()
                .and_then(|p| p.correlation_id.as_ref())
                .and_then(|c| request_day.get(c).cloned());
            let unresolved_correlation = placed
                .as_ref()
                .and_then(|p| p.correlation_id.as_ref())
                .filter(|correlation| !request_day.contains_key(*correlation));
            let unresolved_ingress = unresolved_correlation.is_some();
            if let Some(correlation) = unresolved_correlation {
                unresolved_correlations.insert(correlation.clone());
            }
            let missing_ingress = by_request
                .as_ref()
                .is_some_and(|(_, missing_ingress)| *missing_ingress);
            if missing_ingress {
                if let Some(correlation) = placed.as_ref().and_then(|p| p.correlation_id.as_ref()) {
                    unresolved_correlations.insert(correlation.clone());
                }
            }
            has_unresolved_ingress |= unresolved_ingress || missing_ingress;
            let line_day = match by_request {
                Some((d, _)) => d,
                None if unresolved_ingress => None,
                None => Some(
                    placed
                        .as_ref()
                        .and_then(|p| p.started_ns)
                        .map(utc_date)
                        .unwrap_or_else(|| (*partition).to_owned()),
                ),
            };
            if line_day.as_deref() == Some(day) {
                if missing_ingress {
                    lines_without_ingress += 1;
                }
                mine.push(line);
                continue;
            }
            if unresolved_ingress {
                continue;
            }
            other_day_lines += 1;
            if let Some((inst, gseq)) = placed.and_then(|p| p.boundary_seq) {
                elsewhere.entry(inst).or_default().insert(gseq);
            }
        }
        kept.push(mine);
    }
    DaySelection {
        kept,
        other_day_lines,
        lines_without_ingress,
        has_unresolved_ingress,
        unresolved_correlations,
        elsewhere,
    }
}

/// `gaps` with every sequence another day holds taken out of them.
pub(crate) fn subtract_elsewhere(gaps: &[[u64; 2]], elsewhere: &BTreeSet<u64>) -> Vec<[u64; 2]> {
    let mut out = Vec::new();
    for &[from, to] in gaps {
        let mut start = from;
        for &seq in elsewhere.range(from..=to) {
            if seq > start {
                out.push([start, seq - 1]);
            }
            start = seq + 1;
        }
        if start <= to {
            out.push([start, to]);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const NS_PER_SEC: u64 = 1_000_000_000;
    /// 2026-10-07T23:59:58Z.
    const BEFORE_MIDNIGHT: u64 = 1_791_417_598 * NS_PER_SEC;
    /// 2026-10-08T00:00:03Z.
    const AFTER_MIDNIGHT: u64 = 1_791_417_603 * NS_PER_SEC;

    /// A boundary event. `rseq` is its place in its request; `None` for an
    /// uncorrelated event, which has none.
    fn event(gseq: u64, corr: Option<(&str, u64)>, ns: u64) -> String {
        let (corr, rseq) = match corr {
            Some((c, r)) => (format!("\"{c}\""), format!(r#","request_sequence":{r}"#)),
            None => ("null".to_owned(), String::new()),
        };
        format!(
            r#"{{"schema_version":2,"instance_id":"i1","capture":{{"mode":"session","session_id":"s1"}},"event":{{"global_sequence":{gseq},"correlation_id":{corr}{rseq},"boundary":"b","timestamp_ns":{ns}}}}}"#
        )
    }

    fn object(lines: &[String]) -> Vec<u8> {
        lines.join("\n").into_bytes()
    }

    fn days(list: &[&str]) -> BTreeSet<String> {
        list.iter().map(|d| (*d).to_owned()).collect()
    }

    #[test]
    fn utc_date_names_the_day_either_side_of_midnight() {
        assert_eq!(utc_date(BEFORE_MIDNIGHT), "2026-10-07");
        assert_eq!(utc_date(AFTER_MIDNIGHT), "2026-10-08");
        assert_eq!(utc_date(0), "1970-01-01");
        // A leap day, where an off-by-one in the month table shows.
        assert_eq!(utc_date(1_709_164_800 * NS_PER_SEC), "2024-02-29");
    }

    #[test]
    fn a_recording_id_names_its_day_only_with_a_date_suffix() {
        let d = split_recording_id("rec-2091a49-09291507-xx.2026-10-08");
        assert_eq!(d.session_id, "rec-2091a49-09291507-xx");
        assert_eq!(d.day, Some("2026-10-08"));
        for whole in [
            "rec-2091a49-09291507-xx",
            "run-1789076520165195354",
            "a.b",
            ".2026-10-08",
        ] {
            let d = split_recording_id(whole);
            assert_eq!((d.session_id, d.day), (whole, None), "{whole}");
        }
        assert_eq!(
            split_recording_id(&day_recording_id("s1", "2026-10-08")),
            RecordingDay {
                session_id: "s1",
                day: Some("2026-10-08")
            }
        );
    }

    #[test]
    fn a_request_across_midnight_is_sealed_whole_into_the_day_it_began() {
        // c1 begins before midnight and finishes after, in the next partition.
        // c2 begins after; gseq 3 is c1's tail, interleaved with it. Both days
        // select from all listed partitions, so ownership can be checked
        // without relying on an incomplete single-partition view.
        let earlier_partition = object(&[event(1, Some(("c1", 0)), BEFORE_MIDNIGHT)]);
        let later_partition = object(&[
            event(2, Some(("c2", 0)), AFTER_MIDNIGHT),
            event(3, Some(("c1", 1)), AFTER_MIDNIGHT),
            event(4, Some(("c2", 1)), AFTER_MIDNIGHT),
            event(5, None, AFTER_MIDNIGHT),
        ]);

        let both = days(&["2026-10-07", "2026-10-08"]);
        let objects = [
            ("2026-10-07", earlier_partition.as_slice()),
            ("2026-10-08", later_partition.as_slice()),
        ];
        let earlier = select_day(&objects, "2026-10-07", &both);
        let later = select_day(&objects, "2026-10-08", &both);
        assert_eq!(
            earlier.kept.iter().map(Vec::len).collect::<Vec<_>>(),
            [1, 1]
        );
        assert_eq!(later.kept.iter().map(Vec::len).collect::<Vec<_>>(), [0, 3]);
        assert_eq!(
            (
                earlier.kept.iter().map(Vec::len).sum::<usize>(),
                later.kept.iter().map(Vec::len).sum::<usize>(),
            ),
            (2, 3)
        );
        assert_eq!(earlier.lines_without_ingress, 0);
        assert_eq!(later.lines_without_ingress, 0);
        assert_eq!(earlier.other_day_lines, 3);
        assert_eq!(later.other_day_lines, 2);
        assert_eq!(later.elsewhere["i1"], BTreeSet::from([1, 3]));
        let sealed = earlier
            .kept
            .iter()
            .chain(&later.kept)
            .flatten()
            .filter_map(|line| place(line).and_then(|p| p.boundary_seq).map(|(_, seq)| seq))
            .collect::<Vec<_>>();
        assert_eq!(sealed.len(), 5);
        assert_eq!(
            sealed.into_iter().collect::<BTreeSet<_>>(),
            BTreeSet::from([1, 2, 3, 4, 5])
        );
    }

    #[test]
    fn a_graph_node_follows_its_requests_ingress_not_its_own_start() {
        // c1 began before midnight; one of its spans started after it. Both
        // day selections include both partitions, and the node follows c1's
        // ingress rather than being assigned by its own start timestamp.
        let node = format!(
            r#"{{"artifact_type":"deja_graph_node","instance_id":"i1","node":{{"global_sequence":7,"correlation_id":"c1","started_ns":{AFTER_MIDNIGHT}}}}}"#
        );
        let later = object(std::slice::from_ref(&node));
        let earlier = object(&[event(1, Some(("c1", 0)), BEFORE_MIDNIGHT)]);
        let objects = [
            ("2026-10-07", earlier.as_slice()),
            ("2026-10-08", later.as_slice()),
        ];
        let both = days(&["2026-10-07", "2026-10-08"]);
        let sealed_earlier = select_day(&objects, "2026-10-07", &both);
        let sealed_later = select_day(&objects, "2026-10-08", &both);
        assert_eq!(sealed_earlier.kept[1], [node]);
        assert!(sealed_later.kept.iter().all(Vec::is_empty));
        assert_eq!(sealed_earlier.lines_without_ingress, 0);
        assert_eq!(sealed_later.lines_without_ingress, 0);
    }

    #[test]
    fn a_dropped_ingress_is_preserved_and_counted_on_earliest_observed_day() {
        let first = object(&[event(1, Some(("c1", 1)), BEFORE_MIDNIGHT)]);
        let later = object(&[event(2, Some(("c1", 2)), AFTER_MIDNIGHT)]);
        let objects = [
            ("2026-10-07", first.as_slice()),
            ("2026-10-08", later.as_slice()),
        ];
        let days = days(&["2026-10-07", "2026-10-08"]);
        let earliest = select_day(&objects, "2026-10-07", &days);
        let kept = earliest.kept.iter().map(Vec::len).sum::<usize>();
        assert_eq!(kept, 2);
        assert_eq!(earliest.other_day_lines, 0);
        assert_eq!(earliest.lines_without_ingress, 2);
        assert!(earliest.lines_without_ingress <= kept);
        let later = select_day(&objects, "2026-10-08", &days);
        let kept = later.kept.iter().map(Vec::len).sum::<usize>();
        assert_eq!(kept, 0);
        assert_eq!(later.other_day_lines, 2);
        assert_eq!(later.lines_without_ingress, 0);
        assert!(later.lines_without_ingress <= kept);
    }

    #[test]
    fn orphan_correlated_object_is_reported_alongside_valid_day() {
        let valid = object(&[event(1, Some(("valid", 0)), BEFORE_MIDNIGHT)]);
        let orphan = object(&[event(2, Some(("orphan", 1)), AFTER_MIDNIGHT)]);
        let objects = [
            ("2026-10-07", valid.as_slice()),
            ("2026-10-08", orphan.as_slice()),
        ];
        let indexed = BTreeMap::from([("valid".to_owned(), "2026-10-07".to_owned())]);

        let selected = select_day_with_request_days(&objects, "2026-10-07", &indexed);

        assert_eq!(selected.kept[0].len(), 1);
        assert!(selected.has_unresolved_ingress);
        assert_eq!(
            selected.unresolved_correlations,
            BTreeSet::from(["orphan".to_owned()])
        );
    }

    #[test]
    fn ingress_day_is_independent_of_the_physical_partition() {
        let partition = object(&[
            event(1, Some(("c1", 0)), BEFORE_MIDNIGHT),
            event(2, None, BEFORE_MIDNIGHT),
        ]);
        let listed = days(&["2026-10-06", "2026-10-08"]);
        let objects = [("2026-10-08", partition.as_slice())];
        assert_eq!(select_day(&objects, "2026-10-07", &listed).kept[0].len(), 2);
        assert_eq!(select_day(&objects, "2026-10-08", &listed).kept[0].len(), 0);
    }

    #[test]
    fn indexed_ingress_keeps_a_cross_day_tail_in_a_mixed_object() {
        let mixed = object(&[
            event(2, Some(("c1", 1)), AFTER_MIDNIGHT),
            event(3, Some(("c2", 0)), AFTER_MIDNIGHT),
        ]);
        let objects = [("2026-10-08", mixed.as_slice())];
        let indexed = BTreeMap::from([
            ("c1".to_owned(), "2026-10-07".to_owned()),
            ("c2".to_owned(), "2026-10-08".to_owned()),
        ]);
        let earlier = select_day_with_request_days(&objects, "2026-10-07", &indexed);
        let later = select_day_with_request_days(&objects, "2026-10-08", &indexed);
        assert_eq!(earlier.kept[0].len(), 1);
        assert_eq!(later.kept[0].len(), 1);
        assert!(!earlier.has_unresolved_ingress);
        assert!(!later.has_unresolved_ingress);
    }

    #[test]
    fn missing_request_day_is_not_invented_from_its_partition() {
        let bare = br#"{"schema_version":2,"instance_id":"i1","event":{"global_sequence":1,"correlation_id":"c9","boundary":"b"}}"#;
        let objects: Vec<(&str, &[u8])> = vec![("2026-10-08", bare)];
        let listed = days(&["2026-10-07", "2026-10-08"]);
        let selected = select_day(&objects, "2026-10-08", &listed);
        assert!(selected.has_unresolved_ingress);
        assert!(selected.kept[0].is_empty());
    }

    #[test]
    fn another_days_sequences_are_not_holes_in_this_one() {
        let elsewhere = BTreeSet::from([3, 5, 6]);
        assert_eq!(
            subtract_elsewhere(&[[2, 7]], &elsewhere),
            vec![[2, 2], [4, 4], [7, 7]]
        );
        assert_eq!(
            subtract_elsewhere(&[[3, 3]], &elsewhere),
            Vec::<[u64; 2]>::new()
        );
        assert_eq!(subtract_elsewhere(&[[8, 9]], &elsewhere), vec![[8, 9]]);
    }
}
