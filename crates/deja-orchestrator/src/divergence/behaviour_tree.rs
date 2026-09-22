//! A run as a tree of behaviour: every address the tape holds, with what the
//! run produced there.
//!
//! Git never stores diffs; it stores snapshots and derives every diff by
//! walking two trees. A replay has the same shape once the recording is seen
//! for what it is — a run captured live rather than under substitution. The
//! scorecard is the diff between the tape's run and a candidate's run; the
//! delta a pull request needs is the same diff between two candidates' runs,
//! with the tape as the common ancestor. Both need the same object: a map from
//! address to canonical value, which this module builds.
//!
//! The tree is a PROJECTION of what the scorer already produced — the call
//! ledger and the kernel's http diffs — so it inherits the scorer's
//! canonicalisation and its classification of every row. It stores whether the
//! run reproduced the tape at each address and, when it did not, a hash of
//! what the run produced instead. It never stores the tape's own payloads, so
//! it can leave the pod.
//!
//! Pure seams (`time`, `id`) are substituted and therefore identical by
//! construction; they are not addresses here. The inconclusive classes are
//! left out too, so they can never flip a bucket in a comparison.

use std::collections::{BTreeSet, HashMap};

use deja_kernel::HttpDiff;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::ledger::CallRecord;

/// Which addressing and canonicalisation produced the tree. Two trees compare
/// only when they agree, so a delta never mixes keys or hashes from different
/// rules.
///
/// 1: calls keyed positionally under their span.
/// 2: calls keyed by the recorded event they paired to; the tree carries the
///    correlations the run drove.
pub const CANON_VERSION: u32 = 2;

/// One place a run's behaviour can be observed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Address {
    /// A side-effect call. A call that paired to a recorded event is keyed by
    /// that event's global sequence: two runs of one tape pairing to the same
    /// recorded event are at the same place in the tape whatever else either
    /// run did, so an added or removed call elsewhere never shifts it. A novel
    /// call has no recorded counterpart and is keyed by its position among the
    /// novel calls under the same span, boundary and operation.
    Call {
        correlation: String,
        span_path: String,
        boundary: String,
        operation: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        recorded_event: Option<u64>,
        occurrence: u32,
    },
    /// The status the request's response came back with.
    Status {
        correlation: String,
        request_sequence: u64,
    },
    /// One path of the response body the kernel compared.
    Body {
        correlation: String,
        json_path: String,
    },
}

impl Address {
    pub fn correlation(&self) -> &str {
        match self {
            Address::Call { correlation, .. }
            | Address::Status { correlation, .. }
            | Address::Body { correlation, .. } => correlation,
        }
    }
}

/// What the run produced at an address.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Value {
    /// The run reproduced the tape's value here.
    Reproduced,
    /// The tape holds this address; the run never reached it.
    Absent,
    /// The tape holds this address; the run produced something else.
    Diverged { hash: String },
    /// The tape does not hold this address; the run produced it.
    Novel { hash: String },
}

/// The lane an address belongs to: the connector the request went to and
/// the flow it ran under, read off the call itself. Attribution rolls
/// addresses up by lane, so a divergence can be placed next to the code that
/// could have caused it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Lane {
    pub connector: String,
    pub flow: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Entry {
    pub address: Address,
    pub value: Value,
    /// Whether this address blocks a verdict when it diverges — mirrors the
    /// ledger row's `blocking` for calls; responses always block.
    pub blocking: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BehaviourTree {
    pub run_id: String,
    pub canon_version: u32,
    pub entries: Vec<Entry>,
    /// The lane each correlation ran in, from its connector call.
    pub lanes: HashMap<String, Lane>,
    /// The correlations the run drove: every one the scorer wrote a row or a
    /// diff for. A run that stopped early has no rows for the requests it
    /// never reached, and their absence must read as "not covered", never as
    /// "reproduced the tape"; the comparator restricts itself to the
    /// correlations both trees drove.
    #[serde(default)]
    pub correlations: BTreeSet<String>,
}

/// The canonical form of an argument value: keys sorted, and a header list
/// (`[[name, value], …]`) ordered by name, because the sending process's map
/// order is not behaviour.
///
/// Header NAMES are lowercased, in the sort and in the output, so a case-only
/// difference between two runs hashes identically. HTTP header names are
/// case-insensitive, so the wire treats them as the same header too.
fn canonical(v: &serde_json::Value) -> serde_json::Value {
    match v {
        serde_json::Value::Object(map) => {
            // insert in key order: with `preserve_order` the map keeps
            // insertion order, so the order of insertion is the wire order
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            let mut out = serde_json::Map::new();
            for k in keys {
                let x = &map[k];
                let x = if k == "headers" && is_pair_list(x) {
                    let mut pairs: Vec<(String, serde_json::Value)> = x
                        .as_array()
                        .map(|a| {
                            a.iter()
                                .filter_map(|p| {
                                    let p = p.as_array()?;
                                    Some((p[0].as_str()?.to_ascii_lowercase(), canonical(&p[1])))
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    pairs.sort_by(|a, b| {
                        a.0.cmp(&b.0)
                            .then_with(|| a.1.to_string().cmp(&b.1.to_string()))
                    });
                    serde_json::Value::Array(
                        pairs
                            .into_iter()
                            .map(|(k, v)| serde_json::json!([k, v]))
                            .collect(),
                    )
                } else {
                    canonical(x)
                };
                out.insert(k.clone(), x);
            }
            serde_json::Value::Object(out)
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.iter().map(canonical).collect())
        }
        other => other.clone(),
    }
}

fn is_pair_list(v: &serde_json::Value) -> bool {
    v.as_array().is_some_and(|a| {
        !a.is_empty()
            && a.iter().all(|p| {
                p.as_array()
                    .is_some_and(|p| p.len() == 2 && p[0].is_string())
            })
    })
}

/// A short, stable hash of a canonical value.
pub fn hash_of(v: &serde_json::Value) -> String {
    let bytes = serde_json::to_vec(&canonical(v)).unwrap_or_default();
    let digest = Sha256::digest(&bytes);
    hex::encode(&digest[..8])
}

/// The lane a call ran in: the host's most specific label as the connector
/// (`api-m.sandbox.paypal.com` → `paypal`), and the flow span on its path.
pub fn lane_of(row: &CallRecord) -> Option<Lane> {
    if row.boundary != "http_outgoing" {
        return None;
    }
    let side = row.observed.as_ref().or(row.recorded.as_ref())?;
    let url = side.args.as_ref()?.get("url")?.as_str()?;
    let host = url
        .split("//")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or_default();
    let labels: Vec<&str> = host.split('.').collect();
    // the registrable label: the one before the public suffix
    let connector = if labels.len() >= 2 {
        labels[labels.len() - 2]
    } else {
        host
    }
    .to_owned();
    let span_path = side.span_path.clone().unwrap_or_default();
    let flow = span_path
        .split('>')
        .skip_while(|s| !s.starts_with("deja::"))
        .nth(1)
        .unwrap_or("other")
        .to_owned();
    Some(Lane {
        connector,
        flow: flow.trim_start_matches("payment_").to_owned(),
    })
}

/// Build the tree from the scorer's ledger rows and the kernel's http diffs.
pub fn build(run_id: &str, rows: &[CallRecord], diffs: &[HttpDiff]) -> BehaviourTree {
    let mut entries = Vec::new();
    let mut lanes: HashMap<String, Lane> = HashMap::new();
    let mut correlations = BTreeSet::new();
    // position among the NOVEL calls under one (correlation, span, boundary,
    // operation); calls with a recorded counterpart are keyed by that event
    let mut novel_occurrence: HashMap<(String, String, String, String), u32> = HashMap::new();
    for row in rows {
        let Some(correlation) = row.correlation_id.clone() else {
            continue;
        };
        // any row at all is evidence the run drove this request, seams included
        correlations.insert(correlation.clone());
        if matches!(
            row.boundary.as_str(),
            "time" | "id" | "id_generation" | "uuid" | "rng"
        ) {
            continue;
        }
        if let Some(lane) = lane_of(row) {
            lanes.entry(correlation.clone()).or_insert(lane);
        }
        let observed_hash = || {
            row.observed
                .as_ref()
                .and_then(|s| s.args.as_ref())
                .map(hash_of)
                .unwrap_or_else(|| "∅".to_owned())
        };
        let value = match row.kind.as_str() {
            "matched" | "recovered" | "identity_skew" | "deterministic" => Value::Reproduced,
            "omitted" | "pruned_subtree" => Value::Absent,
            "novel" | "novel_subtree" | "environmental" | "novel_absorbed" => Value::Novel {
                hash: observed_hash(),
            },
            k if k.starts_with("inconclusive") || k.starts_with("schema_default") => continue,
            _ => Value::Diverged {
                hash: observed_hash(),
            },
        };
        // A row that paired to a recorded event takes the RECORDED span path,
        // which is the same in every run of the tape; a novel row has only the
        // observed one.
        let recorded_event = if matches!(value, Value::Novel { .. }) {
            None
        } else {
            row.source_event_global_sequence
        };
        let span_path = match recorded_event {
            Some(_) => row.recorded.as_ref().and_then(|s| s.span_path.clone()),
            None => row.observed.as_ref().and_then(|s| s.span_path.clone()),
        }
        .or_else(|| row.observed.as_ref().and_then(|s| s.span_path.clone()))
        .or_else(|| row.recorded.as_ref().and_then(|s| s.span_path.clone()))
        .unwrap_or_default();
        let occurrence = match recorded_event {
            Some(_) => 0,
            None => {
                let key = (
                    correlation.clone(),
                    span_path.clone(),
                    row.boundary.clone(),
                    row.method_name.clone(),
                );
                let n = novel_occurrence.entry(key).or_insert(0);
                let this = *n;
                *n += 1;
                this
            }
        };
        entries.push(Entry {
            address: Address::Call {
                correlation,
                span_path,
                boundary: row.boundary.clone(),
                operation: row.method_name.clone(),
                recorded_event,
                occurrence,
            },
            value,
            blocking: row.blocking,
        });
    }
    for d in diffs {
        correlations.insert(d.correlation_id.clone());
        entries.push(Entry {
            address: Address::Status {
                correlation: d.correlation_id.clone(),
                request_sequence: d.request_sequence,
            },
            value: if d.status_match {
                Value::Reproduced
            } else {
                Value::Diverged {
                    hash: format!("status={}", d.status_candidate),
                }
            },
            blocking: true,
        });
        for p in &d.body_diff {
            entries.push(Entry {
                address: Address::Body {
                    correlation: d.correlation_id.clone(),
                    json_path: p.json_path.clone(),
                },
                value: Value::Diverged {
                    hash: hash_of(&p.candidate),
                },
                blocking: true,
            });
        }
    }
    BehaviourTree {
        run_id: run_id.to_owned(),
        canon_version: CANON_VERSION,
        entries,
        lanes,
        correlations,
    }
}

impl BehaviourTree {
    /// One JSON object per line: the header, then every entry. The header
    /// says how many entries follow, so a reader can tell a whole file from a
    /// prefix of one.
    pub fn to_jsonl(&self) -> String {
        let mut out = String::new();
        out.push_str(
            &serde_json::json!({
                "run_id": self.run_id,
                "canon_version": self.canon_version,
                "entries": self.entries.len(),
                "lanes": self.lanes,
                "correlations": self.correlations,
            })
            .to_string(),
        );
        out.push('\n');
        for e in &self.entries {
            if let Ok(line) = serde_json::to_string(e) {
                out.push_str(&line);
                out.push('\n');
            }
        }
        out
    }

    /// The whole tree, or nothing. A line that does not parse, or a count
    /// that does not match the header, means the file is not a tree this
    /// code wrote — a prefix, a corruption, an older layout — and a tree
    /// served short would report addresses as uncovered or absent that are
    /// simply not in the file. The caller rebuilds instead.
    pub fn from_jsonl(text: &str) -> Option<Self> {
        let mut lines = text.lines().filter(|l| !l.trim().is_empty());
        let head: serde_json::Value = serde_json::from_str(lines.next()?).ok()?;
        let expected = head.get("entries")?.as_u64()? as usize;
        let entries: Vec<Entry> = lines
            .map(|l| serde_json::from_str::<Entry>(l).ok())
            .collect::<Option<_>>()?;
        if entries.len() != expected {
            return None;
        }
        Some(Self {
            run_id: head.get("run_id")?.as_str()?.to_owned(),
            canon_version: head.get("canon_version")?.as_u64()? as u32,
            entries,
            lanes: serde_json::from_value(head.get("lanes")?.clone()).ok()?,
            correlations: serde_json::from_value(head.get("correlations")?.clone()).ok()?,
        })
    }

    /// Write the tree so that a concurrent reader sees either the previous
    /// file or the whole new one, never a prefix: the bytes go to a
    /// temporary sibling and are renamed onto `path`, which is atomic on
    /// POSIX. The sibling is removed if anything fails before the rename.
    pub fn write_atomic(&self, path: &std::path::Path) -> std::io::Result<()> {
        use std::io::Write;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let tmp = path.with_extension(format!("tmp-{}-{nanos}", std::process::id()));
        let written = (|| {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&tmp)?;
            file.write_all(self.to_jsonl().as_bytes())?;
            file.sync_all()
        })();
        if let Err(e) = written {
            let _ = std::fs::remove_file(&tmp);
            return Err(e);
        }
        std::fs::rename(&tmp, path).inspect_err(|_| {
            let _ = std::fs::remove_file(&tmp);
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use crate::divergence::ledger::CallSide;

    fn row(
        kind: &str,
        corr: &str,
        span: &str,
        source_seq: Option<u64>,
        args: serde_json::Value,
    ) -> CallRecord {
        CallRecord {
            correlation_id: Some(corr.to_owned()),
            source_event_global_sequence: source_seq,
            served_event_global_sequence: None,
            boundary: "http_outgoing".to_owned(),
            trait_name: "svc".to_owned(),
            method_name: "call_connector_api".to_owned(),
            kind: kind.to_owned(),
            blocking: kind == "value_diverged",
            origin: false,
            stopped: false,
            resolved_rank: None,
            recorded: Some(CallSide {
                args: Some(args.clone()),
                span_path: Some(span.to_owned()),
                ..Default::default()
            }),
            observed: Some(CallSide {
                args: Some(args),
                span_path: Some(span.to_owned()),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn header_order_and_name_case_are_not_behaviour() {
        let a =
            serde_json::json!({"url": "https://api.x.com/v1", "headers": [["B", "2"], ["a", "1"]]});
        let b =
            serde_json::json!({"headers": [["A", "1"], ["b", "2"]], "url": "https://api.x.com/v1"});
        assert_eq!(hash_of(&a), hash_of(&b));
        let c =
            serde_json::json!({"url": "https://api.x.com/v1", "headers": [["a", "1"], ["b", "3"]]});
        assert_ne!(hash_of(&a), hash_of(&c), "a header VALUE is behaviour");
    }

    #[test]
    fn a_ledger_becomes_addresses_keyed_by_recorded_event() {
        let args = serde_json::json!({"url": "https://api-m.sandbox.paypal.com/v2/checkout/orders", "method": "POST"});
        let span = "request>deja::grpc_incoming>payment_authorize>ucs::flow_orchestration>execute";
        let rows = vec![
            row("matched", "c1", span, Some(10), args.clone()),
            row("value_diverged", "c1", span, Some(11), args.clone()),
            row("omitted", "c2", span, Some(20), args.clone()),
            row("novel", "c2", span, None, args.clone()),
            row("novel", "c2", span, None, args),
        ];
        let diffs = vec![HttpDiff {
            correlation_id: "c1".into(),
            request_sequence: 0,
            request_path: "/p".into(),
            status_baseline: 200,
            status_candidate: 0,
            status_match: false,
            body_diff: vec![],
            baseline_body: None,
            candidate_body: None,
            transport_error: None,
        }];
        let tree = build("run", &rows, &diffs);
        assert_eq!(
            tree.lanes["c1"],
            Lane {
                connector: "paypal".into(),
                flow: "authorize".into()
            }
        );
        assert_eq!(
            tree.correlations,
            ["c1", "c2"].into_iter().map(String::from).collect()
        );
        let calls: Vec<_> = tree
            .entries
            .iter()
            .filter(|e| matches!(e.address, Address::Call { .. }))
            .collect();
        assert_eq!(calls.len(), 5);
        assert!(matches!(calls[0].value, Value::Reproduced));
        assert!(matches!(
            &calls[0].address,
            Address::Call {
                recorded_event: Some(10),
                occurrence: 0,
                ..
            }
        ));
        assert!(
            matches!(
                &calls[1].address,
                Address::Call {
                    recorded_event: Some(11),
                    occurrence: 0,
                    ..
                }
            ),
            "a paired call is keyed by its recorded event, not by position"
        );
        assert!(matches!(calls[1].value, Value::Diverged { .. }));
        assert!(matches!(calls[2].value, Value::Absent));
        assert!(matches!(
            &calls[2].address,
            Address::Call {
                recorded_event: Some(20),
                ..
            }
        ));
        assert!(matches!(calls[3].value, Value::Novel { .. }));
        assert!(
            matches!(
                &calls[3].address,
                Address::Call {
                    recorded_event: None,
                    occurrence: 0,
                    ..
                }
            ),
            "a novel call has no recorded counterpart and is keyed by position"
        );
        assert!(matches!(
            &calls[4].address,
            Address::Call {
                recorded_event: None,
                occurrence: 1,
                ..
            }
        ));
        let status = tree
            .entries
            .iter()
            .find(|e| matches!(e.address, Address::Status { .. }))
            .unwrap();
        assert_eq!(
            status.value,
            Value::Diverged {
                hash: "status=0".into()
            }
        );
        let text = tree.to_jsonl();
        let back = BehaviourTree::from_jsonl(&text).unwrap();
        assert_eq!(back.entries.len(), tree.entries.len());
        assert_eq!(back.lanes, tree.lanes);
        assert_eq!(back.correlations, tree.correlations);
        assert_eq!(back.entries[1].address, tree.entries[1].address);
    }

    #[test]
    fn a_prefix_or_a_bad_line_is_not_a_tree() {
        let span = "request>deja::grpc_incoming>payment_sync>x";
        let rows = vec![
            row(
                "value_diverged",
                "c1",
                span,
                Some(1),
                serde_json::json!({"u": 1}),
            ),
            row(
                "value_diverged",
                "c1",
                span,
                Some(2),
                serde_json::json!({"u": 2}),
            ),
        ];
        let tree = build("run", &rows, &[]);
        let text = tree.to_jsonl();
        assert_eq!(BehaviourTree::from_jsonl(&text).unwrap().entries.len(), 2);
        // a reader that arrives after the header and the first entry, before
        // the second, must not be served a one-entry tree
        let lines: Vec<&str> = text.lines().collect();
        let prefix = format!("{}\n{}\n", lines[0], lines[1]);
        assert!(BehaviourTree::from_jsonl(&prefix).is_none());
        // a header with a truncated line after it
        let torn = format!("{}\n{}", lines[0], &lines[1][..lines[1].len() / 2]);
        assert!(BehaviourTree::from_jsonl(&torn).is_none());
        // a header alone, count says two
        assert!(BehaviourTree::from_jsonl(&format!("{}\n", lines[0])).is_none());
        // an older layout without the count is rebuilt, not trusted
        let no_count = text.replacen("\"entries\":2,", "", 1);
        assert!(BehaviourTree::from_jsonl(&no_count).is_none());
    }

    #[test]
    fn write_atomic_leaves_a_whole_file_and_no_temporary() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("run.call-ledger.behaviour-tree.jsonl");
        let tree = build("run", &[], &[]);
        tree.write_atomic(&path).unwrap();
        let back = BehaviourTree::from_jsonl(&std::fs::read_to_string(&path).unwrap()).unwrap();
        assert_eq!(back.run_id, "run");
        let names: Vec<String> = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec!["run.call-ledger.behaviour-tree.jsonl".to_owned()]
        );
    }

    #[test]
    fn seams_and_inconclusive_rows_are_not_addresses_but_still_mark_the_request_driven() {
        let span = "request>deja::grpc_incoming>payment_sync>x";
        let mut seam = row("matched", "c1", span, Some(1), serde_json::json!({}));
        seam.boundary = "time".into();
        let mut inconclusive = row(
            "inconclusive_race",
            "c1",
            span,
            Some(2),
            serde_json::json!({}),
        );
        inconclusive.boundary = "db".into();
        let tree = build("run", &[seam, inconclusive], &[]);
        assert!(tree.entries.is_empty());
        assert_eq!(
            tree.correlations.len(),
            1,
            "the request was driven even though nothing in it is an address"
        );
    }
}
