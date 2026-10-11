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

/// The UTC date of a nanosecond timestamp, as `YYYY-MM-DD`.
pub fn utc_date(unix_nanos: u64) -> String {
    // Civil-from-days (H. Hinnant), valid for every date a u64 of nanoseconds
    // can name.
    let days = (unix_nanos / 86_400_000_000_000) as i64;
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
        started_ns: record.timestamp_ns.or(record.started_ns),
        boundary_seq,
    })
}

/// The lines of ONE day, out of objects read across the partitions that can
/// hold it.
pub(crate) struct DaySelection {
    /// Per object, the lines that belong to the day, in object order.
    pub kept: Vec<Vec<String>>,
    /// Lines read that belong to another day — sealed there, not lost.
    pub other_day_lines: usize,
    /// Retained lines of requests whose sequence-zero ingress was not found.
    /// This is a subset of `kept`, not a separate part of the read-line total.
    pub lines_without_ingress: usize,
    /// Boundary-event sequences set aside for another day, per instance, so
    /// the coverage scan does not report them as holes in this one.
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
    days: &BTreeSet<String>,
) -> DaySelection {
    let listed =
        |date: String| -> String { days.range(date.clone()..).next().cloned().unwrap_or(date) };
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
    let request_day: BTreeMap<String, (Option<String>, bool)> = requests
        .into_iter()
        .filter_map(|(c, r)| match (r.ingress_ns, r.dated) {
            (Some(ns), _) => Some((c, (Some(listed(utc_date(ns))), false))),
            (None, true) => Some((c, (r.earliest_partition, true))),
            (None, false) => None,
        })
        .collect();

    let mut kept = Vec::with_capacity(objects.len());
    let mut other_day_lines = 0usize;
    let mut lines_without_ingress = 0usize;
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
            let missing_ingress = by_request
                .as_ref()
                .is_some_and(|(_, missing_ingress)| *missing_ingress);
            let line_day = match by_request {
                Some((d, _)) => d,
                None => Some(
                    placed
                        .as_ref()
                        .and_then(|p| p.started_ns)
                        .map(|ns| listed(utc_date(ns)))
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
    fn a_day_with_no_partition_is_sealed_with_the_next_one() {
        // The session's only traffic on 10-07 was at 23:59:58, and it flushed
        // into the 10-08 partition, so there is no 10-07 recording to hold it.
        let partition = object(&[
            event(1, Some(("c1", 0)), BEFORE_MIDNIGHT),
            event(2, None, BEFORE_MIDNIGHT),
        ]);
        let listed = days(&["2026-10-06", "2026-10-08"]);
        let selected = select_day(&[("2026-10-08", &partition)], "2026-10-08", &listed);
        assert_eq!(selected.kept[0].len(), 2);
        assert_eq!(selected.other_day_lines, 0);
    }

    #[test]
    fn a_line_with_no_timestamp_keeps_the_partition_it_landed_in() {
        let bare = br#"{"schema_version":2,"instance_id":"i1","event":{"global_sequence":1,"correlation_id":"c9","boundary":"b"}}"#;
        let objects: Vec<(&str, &[u8])> = vec![("2026-10-08", bare)];
        let listed = days(&["2026-10-07", "2026-10-08"]);
        assert_eq!(select_day(&objects, "2026-10-08", &listed).kept[0].len(), 1);
        assert_eq!(select_day(&objects, "2026-10-07", &listed).kept[0].len(), 0);
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
