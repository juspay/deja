//! Self-contained HTML diff report: the per-request replay story.
//!
//! One file, inline CSS, no external assets — it must render when opened
//! straight from local disk. Per correlation it shows the HTTP verdict
//! (status + changed body fields + side-by-side bodies) and the side-effect
//! timeline (one row per ledger call, color-coded by outcome), so a failing
//! request reads as a story: which calls matched at which rank, and where the
//! first divergence/omission happened.

use std::collections::BTreeMap;

use deja_kernel::HttpDiff;
use deja_orchestrator::divergence::ledger::CallRecord;

/// Escape text for HTML text/attribute context.
fn esc(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

fn pretty(value: &serde_json::Value) -> String {
    serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
}

/// Outcome label + CSS class for a ledger row.
fn outcome(record: &CallRecord) -> (String, &'static str) {
    match record.kind.as_str() {
        "matched" => (
            match record.resolved_rank {
                Some(rank) => format!("matched (rank {rank})"),
                None => "matched".to_owned(),
            },
            "ok",
        ),
        "recovered" => ("recovered".to_owned(), "warn"),
        "value_diverged" => (
            if record.origin {
                "value diverged (origin)".to_owned()
            } else {
                "value diverged".to_owned()
            },
            "bad",
        ),
        "novel" => ("novel (no recorded counterpart)".to_owned(), "bad"),
        "omitted" => ("omitted (never made on replay)".to_owned(), "muted"),
        "environmental" => ("environmental (egress blocked)".to_owned(), "warn"),
        "deterministic" => ("deterministic miss".to_owned(), "bad"),
        other => (other.replace('_', " "), "warn"),
    }
}

/// Side-by-side <pre> panels with differing lines highlighted by simple
/// line-wise comparison of the pretty-printed JSON.
fn side_by_side(left: &serde_json::Value, right: &serde_json::Value) -> String {
    let left = pretty(left);
    let right = pretty(right);
    let l: Vec<&str> = left.lines().collect();
    let r: Vec<&str> = right.lines().collect();
    let rows = l.len().max(r.len());
    let mut lb = String::new();
    let mut rb = String::new();
    for i in 0..rows {
        let lv = l.get(i).copied().unwrap_or("");
        let rv = r.get(i).copied().unwrap_or("");
        let class = if lv == rv { "" } else { " class=\"dl\"" };
        lb.push_str(&format!("<span{class}>{}</span>\n", esc(lv)));
        rb.push_str(&format!("<span{class}>{}</span>\n", esc(rv)));
    }
    format!(
        "<div class=\"sbs\"><div><h4>recorded</h4><pre>{lb}</pre></div>\
         <div><h4>replayed</h4><pre>{rb}</pre></div></div>"
    )
}

fn http_section(diff: &HttpDiff) -> String {
    let ok = diff.status_match && diff.body_diff.is_empty();
    let status_class = if diff.status_match { "ok" } else { "bad" };
    let mut out = format!(
        "<p class=\"status\"><span class=\"pill {status_class}\">{} → {}</span> \
         <code>{}</code></p>",
        diff.status_baseline,
        diff.status_candidate,
        esc(&diff.request_path),
    );
    if !diff.body_diff.is_empty() {
        out.push_str("<table><tr><th>json path</th><th>recorded</th><th>replayed</th></tr>");
        for field in &diff.body_diff {
            out.push_str(&format!(
                "<tr><td><code>{}</code></td><td>{}</td><td>{}</td></tr>",
                esc(&field.json_path),
                esc(&field.baseline.to_string()),
                esc(&field.candidate.to_string()),
            ));
        }
        out.push_str("</table>");
    }
    match (&diff.baseline_body, &diff.candidate_body) {
        (Some(baseline), Some(candidate)) if !ok => {
            out.push_str(&side_by_side(baseline, candidate));
        }
        _ => {}
    }
    out
}

fn ledger_row(record: &CallRecord) -> String {
    let (label, class) = outcome(record);
    let seq = record
        .source_event_global_sequence
        .map(|s| s.to_string())
        .unwrap_or_else(|| "—".to_owned());
    let mut row = format!(
        "<tr class=\"{class}\"><td>{seq}</td><td>{}</td><td><code>{}::{}</code></td>\
         <td>{}</td></tr>",
        esc(&record.boundary),
        esc(&record.trait_name),
        esc(&record.method_name),
        esc(&label),
    );
    // Expandable recorded-vs-observed detail only where it explains something.
    if record.kind != "matched" {
        let recorded = record
            .recorded
            .as_ref()
            .and_then(|s| s.args.clone())
            .unwrap_or(serde_json::Value::Null);
        let observed = record
            .observed
            .as_ref()
            .and_then(|s| s.args.clone())
            .unwrap_or(serde_json::Value::Null);
        if !recorded.is_null() || !observed.is_null() {
            row.push_str(&format!(
                "<tr class=\"detail\"><td colspan=\"4\"><details><summary>recorded vs \
                 replayed args</summary>{}</details></td></tr>",
                side_by_side(&recorded, &observed),
            ));
        }
    }
    row
}

/// Render the whole report. `diffs` and `ledger` are the run's http-diffs and
/// call-ledger rows; grouping is by correlation id, in first-seen diff order.
pub fn render_report(
    run_id: &str,
    recording_id: &str,
    diffs: &[HttpDiff],
    ledger: &[CallRecord],
) -> String {
    let mut calls_by_corr: BTreeMap<&str, Vec<&CallRecord>> = BTreeMap::new();
    for record in ledger {
        if let Some(cid) = record.correlation_id.as_deref() {
            calls_by_corr.entry(cid).or_default().push(record);
        }
    }

    let matched = diffs
        .iter()
        .filter(|d| d.status_match && d.body_diff.is_empty())
        .count();
    let mut body = format!(
        "<h1>Deja replay diff report</h1>\
         <p class=\"meta\">run <code>{}</code> · recording <code>{}</code> · \
         {} request(s): <span class=\"pill ok\">{} matched</span> \
         <span class=\"pill bad\">{} mismatched</span></p>",
        esc(run_id),
        esc(recording_id),
        diffs.len(),
        matched,
        diffs.len() - matched,
    );

    for diff in diffs {
        let ok = diff.status_match && diff.body_diff.is_empty();
        let calls = calls_by_corr
            .remove(diff.correlation_id.as_str())
            .unwrap_or_default();
        let counts = {
            let mut by_kind: BTreeMap<&str, usize> = BTreeMap::new();
            for c in &calls {
                *by_kind.entry(c.kind.as_str()).or_insert(0) += 1;
            }
            by_kind
                .iter()
                .map(|(k, n)| format!("{n} {}", k.replace('_', " ")))
                .collect::<Vec<_>>()
                .join(", ")
        };
        body.push_str(&format!(
            "<details class=\"req {}\"{}><summary><code>{}</code> \
             <span class=\"pill {}\">{} → {}</span> <span class=\"meta\">{} · {}</span>\
             </summary>{}",
            if ok { "ok" } else { "bad" },
            if ok { "" } else { " open" },
            esc(&diff.request_path),
            if diff.status_match { "ok" } else { "bad" },
            diff.status_baseline,
            diff.status_candidate,
            esc(&diff.correlation_id),
            if counts.is_empty() {
                "no ledger calls".to_owned()
            } else {
                counts
            },
            http_section(diff),
        ));
        if !calls.is_empty() {
            body.push_str(
                "<h3>side-effect timeline</h3>\
                 <table><tr><th>seq</th><th>boundary</th><th>call</th><th>outcome</th></tr>",
            );
            for record in &calls {
                body.push_str(&ledger_row(record));
            }
            body.push_str("</table>");
        }
        body.push_str("</details>");
    }

    // Ledger rows whose correlation never produced an HTTP diff (background
    // work): keep them visible rather than dropping data on the floor.
    if !calls_by_corr.is_empty() {
        body.push_str("<details class=\"req\"><summary>calls outside driven requests</summary>");
        body.push_str("<table><tr><th>seq</th><th>boundary</th><th>call</th><th>outcome</th></tr>");
        for calls in calls_by_corr.values() {
            for record in calls {
                body.push_str(&ledger_row(record));
            }
        }
        body.push_str("</table></details>");
    }

    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\">\
         <title>deja diff report · {}</title><style>{}</style></head>\
         <body>{body}</body></html>",
        esc(run_id),
        CSS,
    )
}

const CSS: &str = "\
body{font:14px/1.45 -apple-system,'Segoe UI',sans-serif;margin:2rem auto;max-width:70rem;\
padding:0 1rem;color:#1a1a2e;background:#fafafa}\
code{font:12px ui-monospace,monospace;background:#eee;padding:0 3px;border-radius:3px}\
.meta{color:#666}\
.pill{display:inline-block;padding:1px 8px;border-radius:9px;font-size:12px;font-weight:600}\
.pill.ok{background:#d9efdd;color:#1c6b2f}\
.pill.bad{background:#f6d5d2;color:#96271a}\
details.req{border:1px solid #ddd;border-radius:6px;margin:.6rem 0;padding:.4rem .8rem;\
background:#fff}\
details.req>summary{cursor:pointer;font-weight:600}\
table{border-collapse:collapse;margin:.5rem 0;width:100%}\
th,td{border:1px solid #e2e2e2;padding:3px 8px;text-align:left;font-size:13px}\
tr.ok td{background:#f2faf3}tr.bad td{background:#fdf0ee}tr.warn td{background:#fdf7e8}\
tr.muted td{color:#888}tr.detail td{background:#fff}\
.sbs{display:flex;gap:1rem}.sbs>div{flex:1;min-width:0}\
.sbs pre{background:#f6f6f6;border:1px solid #e2e2e2;border-radius:4px;padding:6px;\
overflow-x:auto;font-size:12px}\
.sbs h4{margin:.3rem 0}\
.dl{background:#ffe3a8;display:inline-block;width:100%}\
";

/// Read the run's http-diffs + call-ledger JSONL files (tolerating a missing
/// file as empty — e.g. a run that died before scoring) and write the rendered
/// report to `out_path`.
pub fn write_report(
    run_id: &str,
    recording_id: &str,
    http_diffs_path: &std::path::Path,
    ledger_path: &std::path::Path,
    out_path: &std::path::Path,
) -> Result<(), String> {
    fn read_jsonl<T: serde::de::DeserializeOwned>(path: &std::path::Path) -> Vec<T> {
        let Ok(content) = std::fs::read_to_string(path) else {
            return Vec::new();
        };
        // Dashboard exports are whole-file JSON arrays; agent artifacts are
        // JSONL. Accept both.
        if content.trim_start().starts_with('[') {
            if let Ok(rows) = serde_json::from_str::<Vec<T>>(&content) {
                return rows;
            }
        }
        content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .filter_map(|line| serde_json::from_str(line).ok())
            .collect()
    }
    let diffs: Vec<HttpDiff> = read_jsonl(http_diffs_path);
    let ledger: Vec<CallRecord> = read_jsonl(ledger_path);
    let html = render_report(run_id, recording_id, &diffs, &ledger);
    std::fs::write(out_path, html).map_err(|e| format!("write {}: {e}", out_path.display()))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;
    use deja_kernel::{BaselineResponse, JsonFieldDiff};
    use deja_orchestrator::divergence::ledger::CallSide;

    fn diff(correlation: &str, ok: bool) -> HttpDiff {
        HttpDiff {
            correlation_id: correlation.to_owned(),
            request_sequence: 0,
            request_path: "/payments".to_owned(),
            status_baseline: 200,
            status_candidate: if ok { 200 } else { 599 },
            status_match: ok,
            body_diff: if ok {
                Vec::new()
            } else {
                vec![JsonFieldDiff {
                    json_path: "$.status".to_owned(),
                    baseline: serde_json::json!("succeeded"),
                    candidate: serde_json::json!("<failed>"),
                }]
            },
            baseline_body: Some(serde_json::json!({"status": "succeeded"})),
            candidate_body: Some(serde_json::json!({"status": "<failed>"})),
        }
    }

    fn call(correlation: &str, kind: &str, rank: Option<u8>) -> CallRecord {
        CallRecord {
            correlation_id: Some(correlation.to_owned()),
            source_event_global_sequence: Some(7),
            boundary: "db".to_owned(),
            trait_name: "Store".to_owned(),
            method_name: "find".to_owned(),
            kind: kind.to_owned(),
            blocking: false,
            origin: false,
            resolved_rank: rank,
            recorded: Some(CallSide {
                args: Some(serde_json::json!({"id": "<tag>"})),
                ..CallSide::default()
            }),
            observed: Some(CallSide {
                args: Some(serde_json::json!({"id": "other"})),
                ..CallSide::default()
            }),
        }
    }

    #[test]
    fn report_tells_the_request_story() {
        let html = render_report(
            "run-1",
            "rec-1",
            &[diff("c-fail", false), diff("c-ok", true)],
            &[
                call("c-fail", "matched", Some(2)),
                call("c-fail", "novel", None),
                call("c-fail", "omitted", None),
                call("c-orphan", "matched", Some(3)),
            ],
        );
        // Header counts.
        assert!(html.contains("2 request(s)"));
        assert!(html.contains("1 matched"));
        assert!(html.contains("1 mismatched"));
        // Outcome labels, color classes, rank surfaced.
        assert!(html.contains("matched (rank 2)"));
        assert!(html.contains("novel (no recorded counterpart)"));
        assert!(html.contains("omitted (never made on replay)"));
        // Mismatched request opens by default; matched stays collapsed.
        assert!(html.contains("class=\"req bad\" open"));
        assert!(html.contains("class=\"req ok\">"));
        // Ledger calls with no driven request stay visible.
        assert!(html.contains("calls outside driven requests"));
        // Body/args content is HTML-escaped.
        assert!(html.contains("&lt;failed&gt;"));
        assert!(html.contains("&lt;tag&gt;"));
        assert!(!html.contains("<failed>"));
        // Self-contained document.
        assert!(html.starts_with("<!doctype html>"));
        assert!(!html.contains("http://") && !html.contains("https://"));
    }

    #[test]
    fn matched_only_report_has_no_open_sections() {
        let html = render_report("run-2", "rec-1", &[diff("c-ok", true)], &[]);
        assert!(!html.contains(" open"));
        assert!(html.contains("1 matched"));
        assert!(html.contains("0 mismatched"));
    }

    #[allow(dead_code)]
    fn baseline_shape_guard(b: BaselineResponse) -> BaselineResponse {
        b
    }

    #[test]
    fn write_report_builds_file_from_jsonl_and_tolerates_missing_ledger() {
        let dir = tempfile::tempdir().unwrap();
        let diffs_path = dir.path().join("http-diffs.jsonl");
        let ledger_path = dir.path().join("call-ledger.jsonl"); // never created
        let out_path = dir.path().join("diff-report.html");
        std::fs::write(
            &diffs_path,
            format!("{}\n", serde_json::to_string(&diff("c1", false)).unwrap()),
        )
        .unwrap();

        write_report("run-9", "rec-1", &diffs_path, &ledger_path, &out_path).unwrap();

        let html = std::fs::read_to_string(&out_path).unwrap();
        assert!(html.contains("run-9"));
        assert!(html.contains("1 mismatched"));
    }
}
