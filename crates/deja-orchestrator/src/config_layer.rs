//! Layering the candidate's own config UNDER the recorded one.
//!
//! A replay boots the candidate from a single config file, so two independent
//! sources have to become one file before it starts:
//!
//!   * the RECORDED config — what the recorded session's process actually
//!     booted from. This is the environment being reproduced.
//!   * the CANDIDATE's config — the same-shaped file at the candidate's own
//!     ref, carried in the CodeBundle. It is the only thing that knows about
//!     keys the candidate ADDED after the recording was taken, which the
//!     recording cannot possibly carry and without which the candidate may
//!     refuse to boot at all.
//!
//! Precedence is fixed and one-directional: **the recording wins wherever it
//! has an opinion**. The candidate's file supplies only what the recording is
//! silent about. A merge that let candidate defaults displace recorded values
//! would quietly change what is being reproduced and still score as a clean
//! run — strictly worse than the boot failure it would be papering over.
//!
//! That invariant is not a comment: [`layer_toml`] tags every leaf with its
//! origin and refuses to emit a result unless three counts balance —
//!
//!   1. every leaf of the merged output came from exactly one side;
//!   2. every leaf of the RECORDING survives into the output (nothing the
//!      recording said is lost);
//!   3. every leaf of the CANDIDATE is either carried, overridden by a
//!      recorded leaf at the same path, or named in `shadowed`.
//!
//! Nothing is dropped without a name.
//!
//! # What this module deliberately does not know
//!
//! Which file the system under test boots, where that system keeps its own
//! copy of it, and what selects between several — all of that belongs to the
//! deployment, which knows both ends already because it mounts one and stages
//! the other. Every path arrives as an argument. The merge, the precedence
//! direction, the accounting and the refusal are generic; a table of somebody
//! else's filenames would not be, and would have to be edited every time that
//! system rearranged its own directory.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use toml::Value;

/// Where each key in the merged config came from, and what the merge dropped.
///
/// `carried` is the interesting list: those are the keys the recording could
/// not know about, filled in from the candidate's own defaults. They are named
/// individually (not just counted) so a reader of the Job's init log can see
/// exactly what the recording did not specify.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct LayerReport {
    /// Dotted paths present only in the BASE layer, carried into the merged
    /// output. Sorted. In a replay these are exactly the keys the recording
    /// could not have known about.
    pub carried: Vec<String>,
    /// Leaf count taken from the OVER layer (every one of its leaves, by
    /// invariant — the whole point of the direction).
    pub from_over: usize,
    /// Base leaves displaced by an over leaf at the SAME path. The ordinary
    /// case, and the reason a replay stays faithful.
    pub overridden: usize,
    /// Base leaves dropped because the over layer put a value of a different
    /// SHAPE at or above that path (a scalar where the base has a table, or a
    /// table where it has a scalar). Rare and worth reading. Named, never
    /// merely counted.
    pub shadowed: Vec<String>,
}

impl LayerReport {
    /// Total leaves in the merged output.
    pub fn total(&self) -> usize {
        self.from_over + self.carried.len()
    }

    /// A human summary for the Job's init log: the counts, then every key the
    /// merge took from the base layer rather than the overriding one. Both
    /// paths are printed, so which file played which role is never in doubt.
    pub fn render(&self, base_label: &str, over_label: &str) -> String {
        let mut s = String::new();
        let _ = writeln!(
            s,
            "config layering: {} keys in the merged config = {} from the overriding layer ({}) \
             + {} from the base layer ({})",
            self.total(),
            self.from_over,
            over_label,
            self.carried.len(),
            base_label,
        );
        let _ = writeln!(
            s,
            "config layering: {} base keys were overridden by the overriding layer",
            self.overridden,
        );
        for k in &self.carried {
            let _ = writeln!(s, "config layering: from the base layer: {k}");
        }
        for k in &self.shadowed {
            let _ = writeln!(
                s,
                "config layering: base key structurally shadowed by the overriding layer: {k}",
            );
        }
        s
    }
}

/// Layer `candidate` (lower precedence) under `recorded` (higher precedence)
/// and return the merged TOML text plus the provenance report.
///
/// Merge semantics are the ordinary config-layering ones: tables merge
/// recursively; every non-table value (including arrays and arrays-of-tables)
/// is a leaf and is taken wholesale from the recording when the recording has
/// one at that path.
pub fn layer_toml(base_text: &str, over_text: &str) -> Result<(String, LayerReport), String> {
    let base: toml::Table = toml::from_str(base_text)
        .map_err(|e| format!("the --base config is not valid TOML: {e}"))?;
    let over: toml::Table = toml::from_str(over_text)
        .map_err(|e| format!("the --over config is not valid TOML: {e}"))?;

    let base_leaves = count_leaves_table(&base);
    let over_leaves = count_leaves_table(&over);

    let mut report = LayerReport::default();
    let mut path = Vec::new();
    let merged = merge_table(base, &over, &mut path, &mut report);

    report.carried.sort();
    report.shadowed.sort();

    // ── the three balances ──────────────────────────────────────────────────
    // Asserted, not logged: a merge whose accounting does not close has lost or
    // duplicated configuration, and booting a router from it would produce a
    // confidently wrong replay.
    let merged_leaves = count_leaves_table(&merged);
    if merged_leaves != report.total() {
        return Err(format!(
            "config layering accounting failed: merged output has {merged_leaves} leaves but \
             provenance accounts for {} ({} from --over + {} carried from --base)",
            report.total(),
            report.from_over,
            report.carried.len(),
        ));
    }
    if report.from_over != over_leaves {
        return Err(format!(
            "config layering accounting failed: the --over config has {over_leaves} leaves but \
             only {} survived into the merged output — the authoritative layer must never lose \
             a key",
            report.from_over,
        ));
    }
    let accounted_base = report.carried.len() + report.overridden + report.shadowed.len();
    if accounted_base != base_leaves {
        return Err(format!(
            "config layering accounting failed: the --base config has {base_leaves} leaves but \
             {accounted_base} were accounted for ({} carried + {} overridden + {} shadowed)",
            report.carried.len(),
            report.overridden,
            report.shadowed.len(),
        ));
    }

    let text = toml::to_string(&Value::Table(merged))
        .map_err(|e| format!("serialize merged config: {e}"))?;
    Ok((text, report))
}

/// Recursive deep merge. `over` wins at every path where it has a value; where
/// both sides hold a table, the tables merge.
fn merge_table(
    base: toml::Table,
    over: &toml::Table,
    path: &mut Vec<String>,
    report: &mut LayerReport,
) -> toml::Table {
    let mut out: toml::Table = toml::Table::new();

    for (key, base_value) in base {
        path.push(key.clone());
        match over.get(&key) {
            // Candidate-only subtree: carried whole.
            None => {
                collect_leaves(&base_value, path, &mut report.carried);
                out.insert(key, base_value);
            }
            // Both tables: recurse.
            Some(Value::Table(over_table)) if base_value.is_table() => {
                let base_table = match base_value {
                    Value::Table(t) => t,
                    // Unreachable: guarded by `base_value.is_table()`.
                    other => {
                        out.insert(key, other);
                        path.pop();
                        continue;
                    }
                };
                let merged = merge_table(base_table, over_table, path, report);
                out.insert(key, Value::Table(merged));
            }
            // The recording has a value here and at least one side is not a
            // table: the recording wins wholesale.
            Some(over_value) => {
                if base_value.is_table() || over_value.is_table() {
                    // Shapes differ — the candidate's leaves under this path
                    // are dropped, so name every one of them.
                    collect_leaves(&base_value, path, &mut report.shadowed);
                } else {
                    report.overridden += 1;
                }
                report.from_over += count_leaves(over_value);
                out.insert(key, over_value.clone());
            }
        }
        path.pop();
    }

    // Recording-only keys: appended, all of them from the recording.
    for (key, over_value) in over {
        if out.contains_key(key) {
            continue;
        }
        report.from_over += count_leaves(over_value);
        out.insert(key.clone(), over_value.clone());
    }

    out
}

/// Every non-table value under `value`, as a dotted path, pushed onto `sink`.
///
/// An empty table carries no configuration and is therefore not a leaf: it
/// contributes nothing to any of the three balances, on either side.
fn collect_leaves(value: &Value, path: &mut Vec<String>, sink: &mut Vec<String>) {
    match value {
        Value::Table(t) => {
            for (k, v) in t {
                path.push(k.clone());
                collect_leaves(v, path, sink);
                path.pop();
            }
        }
        _ => sink.push(path.join(".")),
    }
}

fn count_leaves(value: &Value) -> usize {
    match value {
        Value::Table(t) => t.values().map(count_leaves).sum(),
        _ => 1,
    }
}

fn count_leaves_table(table: &toml::Table) -> usize {
    table.values().map(count_leaves).sum()
}

/// Group the carried keys by their first path segment, for a compact log line
/// when the carried set is large (`connectors` → 12, `grpc_client` → 3).
pub fn carried_by_section(report: &LayerReport) -> BTreeMap<String, usize> {
    let mut out = BTreeMap::new();
    for k in &report.carried {
        let head = k.split('.').next().unwrap_or(k).to_owned();
        *out.entry(head).or_insert(0) += 1;
    }
    out
}

/// The sidecar file written next to the merged config, naming every key the
/// merge took from the candidate. The Job's init log carries the same content;
/// the file is there so a post-mortem on a finished run can read it without the
/// logs.
pub fn provenance_path(out: &Path) -> PathBuf {
    let mut p = out.as_os_str().to_owned();
    p.push(".provenance");
    PathBuf::from(p)
}

/// The nearest existing ancestor directory of a path that is missing, and what
/// it actually contains (sorted, capped).
///
/// This is how a missing candidate config explains itself without this module
/// knowing what any file is FOR. "Expected X, and here is what the delivery
/// actually produced" is the whole diagnosis, and it reads the same whether the
/// system under test keeps its config in one layout or another.
fn nearest_existing_listing(missing: &Path) -> Option<(PathBuf, Vec<String>)> {
    const MAX: usize = 24;
    let mut dir = missing.parent()?;
    loop {
        if dir.is_dir() {
            let mut names: Vec<String> = std::fs::read_dir(dir)
                .ok()?
                .filter_map(|e| e.ok())
                .map(|e| {
                    let name = e.file_name().to_string_lossy().into_owned();
                    if e.path().is_dir() {
                        format!("{name}/")
                    } else {
                        name
                    }
                })
                .collect();
            names.sort();
            if names.len() > MAX {
                let extra = names.len() - MAX;
                names.truncate(MAX);
                names.push(format!("… and {extra} more"));
            }
            return Some((dir.to_path_buf(), names));
        }
        dir = dir.parent()?;
    }
}

/// Read the candidate's config and the recorded config, layer them, and write
/// the single merged file the candidate boots from.
///
/// All three paths come from the CALLER. This module does not know which file
/// the system under test boots, where that system keeps its own copy, or what
/// selects between them — the deployment that mounts one and stages the other
/// knows both ends already, and telling it to a merge tool is cheaper and more
/// honest than a table in here that has to be right about somebody else's
/// layout.
///
/// `source_label` is whatever names the delivery mechanism for `candidate` in
/// this deployment (deja's CodeBundle URI). It appears in the error so the
/// message points at the thing that failed to deliver the file rather than at
/// the file that is merely missing.
///
/// There is deliberately NO fallback. If the candidate's config is not there,
/// this fails and the Job fails with it. Substituting some other file — or
/// proceeding with the recorded config alone — fills a config gap from
/// somewhere the recorded system never used, and that scores as a clean run
/// against the wrong endpoints, which is worse than the boot failure it would
/// be papering over.
pub fn layer_config_files(
    base: &Path,
    over: &Path,
    out: &Path,
    source_label: Option<&str>,
) -> Result<LayerReport, String> {
    let base_text = std::fs::read_to_string(base).map_err(|e| {
        let delivered_by = match source_label {
            Some(s) => format!(" It is delivered by {s}."),
            None => String::new(),
        };
        let found = match nearest_existing_listing(base) {
            Some((dir, names)) => format!(
                " The nearest directory that does exist is {}, and it contains {names:?} — if \
                 that is not what you expected to be staged, the delivery is the thing to look \
                 at, not this step.",
                dir.display()
            ),
            None => String::new(),
        };
        format!(
            "--base: expected a config file at {} but it is not there ({e}).{delivered_by}\
             {found} Refusing: without it the --over layer has nothing to layer over, and this \
             step will not substitute another file.",
            base.display(),
        )
    })?;

    let over_text = std::fs::read_to_string(over).map_err(|e| {
        format!(
            "--over: expected a config file at {} but it is not there ({e}). That layer is the \
             authoritative one — in a replay it IS the environment being reproduced, so a run \
             that booted without it would not be a replay.",
            over.display(),
        )
    })?;

    let (merged, report) = layer_toml(&base_text, &over_text)?;

    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    std::fs::write(out, merged.as_bytes()).map_err(|e| format!("write {}: {e}", out.display()))?;
    // The candidate may run as a different, unprivileged user than this step
    // (they are different images). An owner-only file would be unreadable at
    // boot, so set the mode explicitly rather than inheriting a umask.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(out, std::fs::Permissions::from_mode(0o644))
            .map_err(|e| format!("chmod {}: {e}", out.display()))?;
    }

    let prov = provenance_path(out);
    std::fs::write(
        &prov,
        report.render(&base.display().to_string(), &over.display().to_string()),
    )
    .map_err(|e| format!("write {}: {e}", prov.display()))?;

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn merged_table(candidate: &str, recorded: &str) -> (toml::Table, LayerReport) {
        let (text, report) = layer_toml(candidate, recorded).expect("layering succeeds");
        let table: toml::Table = toml::from_str(&text).expect("merged output is valid TOML");
        (table, report)
    }

    #[test]
    fn recorded_value_wins_over_candidate_default() {
        let (t, r) = merged_table(
            "[connectors]\nadyen.base_url = \"https://dev.adyen\"\n",
            "[connectors]\nadyen.base_url = \"https://sbx.adyen\"\n",
        );
        assert_eq!(
            t["connectors"]["adyen"]["base_url"].as_str(),
            Some("https://sbx.adyen"),
            "the recording must win wherever it has an opinion"
        );
        assert_eq!(r.overridden, 1);
        assert!(r.carried.is_empty());
    }

    #[test]
    fn candidate_only_key_survives_and_is_named() {
        let (t, r) = merged_table(
            "[connectors]\nadyen.base_url = \"https://dev.adyen\"\nilixium.base_url = \"https://ili\"\n",
            "[connectors]\nadyen.base_url = \"https://sbx.adyen\"\n",
        );
        assert_eq!(
            t["connectors"]["ilixium"]["base_url"].as_str(),
            Some("https://ili"),
            "a key only the candidate knows about must survive the merge"
        );
        assert_eq!(r.carried, vec!["connectors.ilixium.base_url".to_owned()]);
        assert_eq!(r.overridden, 1);
    }

    /// The precedence relation has TWO halves and both are load-bearing, so
    /// both are asserted here: where the recording has an opinion it wins, and
    /// where it is silent the candidate's value must survive rather than be
    /// blanked. Breaking either half fails this one test.
    ///
    /// The values are the real shape of the bug. `connectors.wise.base_url`
    /// genuinely changed between the recorded config and candidate 65e1ccbb85:
    /// if the candidate won there, every wise call would diverge on URL and
    /// score as a regression that is really a config layering fault.
    /// `connectors.ilixium.base_url` exists only in the candidate: if the
    /// recording's silence won there, the router panics at boot with
    /// `base_url must not be empty for ilixium`.
    #[test]
    fn precedence_is_one_directional() {
        let candidate = concat!(
            "[connectors]\n",
            "wise.base_url = \"https://api.wise-sandbox.com/\"\n",
            "ilixium.base_url = \"https://prprocessing.ilixium.com/platform/ili\"\n",
        );
        let recorded = concat!(
            "[connectors]\n",
            "wise.base_url = \"https://api.sandbox.transferwise.tech/\"\n",
        );
        let (t, r) = merged_table(candidate, recorded);

        // Half one: a key BOTH sides define comes from the recording.
        assert_eq!(
            t["connectors"]["wise"]["base_url"].as_str(),
            Some("https://api.sandbox.transferwise.tech/"),
            "a key present in both must come from the recording, not the candidate"
        );
        // Half two: a key ONLY the candidate defines survives.
        assert_eq!(
            t["connectors"]["ilixium"]["base_url"].as_str(),
            Some("https://prprocessing.ilixium.com/platform/ili"),
            "a key only the candidate defines must survive; the recording's silence is not an \
             opinion"
        );
        // And the provenance says which was which, by name.
        assert_eq!(r.carried, vec!["connectors.ilixium.base_url".to_owned()]);
        assert_eq!(r.overridden, 1);
        assert_eq!(r.from_over, 1);
        assert!(r.shadowed.is_empty());
    }

    #[test]
    fn sibling_tables_merge_rather_than_replace() {
        // The whole reason concatenation is not enough: two `[connectors]`
        // tables must become one, keeping both sides' children.
        let (t, _) = merged_table(
            "[connectors]\na.base_url = \"dev-a\"\nb.base_url = \"dev-b\"\n",
            "[connectors]\na.base_url = \"sbx-a\"\n",
        );
        assert_eq!(t["connectors"]["a"]["base_url"].as_str(), Some("sbx-a"));
        assert_eq!(t["connectors"]["b"]["base_url"].as_str(), Some("dev-b"));
    }

    #[test]
    fn recording_only_section_is_preserved() {
        let (t, r) = merged_table(
            "[a]\nx = 1\n",
            "[a]\nx = 2\n[network_tokenization_service]\ngenerate_token_url = \"u\"\n",
        );
        assert_eq!(
            t["network_tokenization_service"]["generate_token_url"].as_str(),
            Some("u")
        );
        assert_eq!(r.from_over, 2);
        assert!(r.carried.is_empty());
    }

    #[test]
    fn arrays_are_leaves_and_the_recording_replaces_them_wholesale() {
        let (t, r) = merged_table("wallets = [\"a\", \"b\", \"c\"]\n", "wallets = [\"z\"]\n");
        let got: Vec<&str> = t["wallets"]
            .as_array()
            .expect("array")
            .iter()
            .map(|v| v.as_str().expect("str"))
            .collect();
        assert_eq!(
            got,
            vec!["z"],
            "an array is one opinion: the recording's replaces the candidate's, never appends"
        );
        assert_eq!(r.overridden, 1);
        assert_eq!(r.from_over, 1);
    }

    #[test]
    fn shape_conflict_names_every_dropped_candidate_leaf() {
        // The recording has a scalar where the candidate has a table: the
        // candidate's subtree cannot survive, so each of its leaves is named.
        let (t, r) = merged_table("[locker]\nhost = \"h\"\nport = 3\n", "locker = \"off\"\n");
        assert_eq!(t["locker"].as_str(), Some("off"));
        assert_eq!(
            r.shadowed,
            vec!["locker.host".to_owned(), "locker.port".to_owned()],
            "a structurally displaced candidate key must be named, not silently dropped"
        );
        assert!(r.carried.is_empty());
        assert_eq!(r.overridden, 0);
    }

    #[test]
    fn accounting_balances_on_a_mixed_document() {
        let candidate = r#"
top = 1
only_candidate = 2
[t]
a = 1
b = 2
[t.deep]
c = 3
[candidate_section]
x = 1
y = 2
"#;
        let recorded = r#"
top = 10
[t]
a = 100
[t.deep]
c = 300
d = 400
[recorded_section]
z = 1
"#;
        let (_, r) = merged_table(candidate, recorded);
        // Candidate leaves: top, only_candidate, t.a, t.b, t.deep.c, cs.x, cs.y = 7
        assert_eq!(r.carried.len() + r.overridden + r.shadowed.len(), 7);
        // Recorded leaves: top, t.a, t.deep.c, t.deep.d, rs.z = 5
        assert_eq!(r.from_over, 5);
        assert_eq!(
            r.carried,
            vec![
                "candidate_section.x".to_owned(),
                "candidate_section.y".to_owned(),
                "only_candidate".to_owned(),
                "t.b".to_owned(),
            ]
        );
        assert_eq!(r.total(), 9);
    }

    #[test]
    fn merged_output_reparses_with_tables_after_values() {
        let _lock = crate::test_env::env_guard();
        // TOML forbids a bare value after a table header at the same level; a
        // merge that reorders keys can produce a document that does not parse.
        // Serializing through `toml::to_string` must keep the output loadable.
        let (text, _) = layer_toml(
            "[table_first]\nk = 1\n\n[other]\nk = 2\n",
            "scalar_after = \"v\"\nsecond_scalar = 3\n",
        )
        .expect("layering succeeds");
        let round: toml::Table = toml::from_str(&text).expect("merged output must reparse");
        assert_eq!(round["scalar_after"].as_str(), Some("v"));
        assert_eq!(round["table_first"]["k"].as_integer(), Some(1));
    }

    #[test]
    fn invalid_base_toml_is_named_as_such() {
        let _lock = crate::test_env::env_guard();
        let err = layer_toml("this is not toml", "a = 1").expect_err("must fail");
        assert!(
            err.contains("--base config is not valid TOML"),
            "error must name WHICH side failed to parse, got: {err}"
        );
    }

    #[test]
    fn invalid_over_toml_is_named_as_such() {
        let _lock = crate::test_env::env_guard();
        let err = layer_toml("a = 1", "this is not toml").expect_err("must fail");
        assert!(
            err.contains("--over config is not valid TOML"),
            "error must name WHICH side failed to parse, got: {err}"
        );
    }

    #[test]
    fn report_renders_every_carried_key_by_name() {
        let (_, r) = merged_table(
            "[connectors]\nilixium.base_url = \"u\"\nadyen.base_url = \"d\"\n",
            "[connectors]\nadyen.base_url = \"s\"\n",
        );
        let text = r.render("base.toml", "over.toml");
        assert!(text.contains("from the base layer: connectors.ilixium.base_url"));
        assert!(text.contains("1 from the base layer (base.toml)"));
        assert!(text.contains("from the overriding layer (over.toml)"));
    }

    // ── the file layer: paths in, no layout knowledge ──────────────────────

    fn write(dir: &std::path::Path, rel: &str, body: &str) {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(p, body).expect("write");
    }

    #[test]
    fn a_missing_candidate_config_is_refused_and_nothing_is_written() {
        let tmp = tempfile::tempdir().expect("tmp");
        write(tmp.path(), "recorded.toml", "b = 2\n");
        let out = tmp.path().join("out/boot.toml");

        let err = layer_config_files(
            &tmp.path().join("bundle/some/where/base.toml"),
            &tmp.path().join("recorded.toml"),
            &out,
            Some("s3://bucket/codebundles/deadbeef/migrations.tar"),
        )
        .expect_err("must refuse");
        assert!(
            err.contains("bundle/some/where/base.toml"),
            "must name the file it expected: {err}"
        );
        assert!(
            err.contains("s3://bucket/codebundles/deadbeef/migrations.tar"),
            "must name what was supposed to deliver it: {err}"
        );
        assert!(
            !out.exists() && !provenance_path(&out).exists(),
            "nothing may be written when the base layer is missing"
        );
    }

    /// The stale-bundle diagnosis, without knowing what any file MEANS: say
    /// what was expected and show what the delivery actually produced. A reader
    /// who knows the system recognises an old bundle instantly; this module
    /// never has to.
    #[test]
    fn a_missing_candidate_config_shows_what_was_delivered_instead() {
        let tmp = tempfile::tempdir().expect("tmp");
        write(tmp.path(), "recorded.toml", "b = 2\n");
        write(tmp.path(), "bundle/cfg/older-thing.toml", "a = 1\n");
        write(tmp.path(), "bundle/cfg/another.toml", "a = 1\n");

        let err = layer_config_files(
            &tmp.path().join("bundle/cfg/wanted.toml"),
            &tmp.path().join("recorded.toml"),
            &tmp.path().join("out/boot.toml"),
            None,
        )
        .expect_err("must refuse");
        assert!(err.contains("wanted.toml"), "{err}");
        assert!(
            err.contains("another.toml") && err.contains("older-thing.toml"),
            "must list what the delivery actually produced: {err}"
        );
    }

    /// The expected file's own directory may not exist at all — that is the
    /// shape a bundle takes when a whole subtree is missing. Walk up to
    /// something real rather than reporting nothing.
    #[test]
    fn the_listing_walks_up_to_a_directory_that_exists() {
        let tmp = tempfile::tempdir().expect("tmp");
        write(tmp.path(), "recorded.toml", "b = 2\n");
        write(tmp.path(), "bundle/cfg/present.toml", "a = 1\n");

        let err = layer_config_files(
            &tmp.path().join("bundle/cfg/deeper/still/wanted.toml"),
            &tmp.path().join("recorded.toml"),
            &tmp.path().join("out/boot.toml"),
            None,
        )
        .expect_err("must refuse");
        assert!(
            err.contains("present.toml"),
            "must climb to the nearest existing directory and list it: {err}"
        );
    }

    #[test]
    fn a_missing_over_config_is_refused_too() {
        let tmp = tempfile::tempdir().expect("tmp");
        write(tmp.path(), "base.toml", "a = 1\n");
        let err = layer_config_files(
            &tmp.path().join("base.toml"),
            &tmp.path().join("nope.toml"),
            &tmp.path().join("out/boot.toml"),
            None,
        )
        .expect_err("must refuse");
        assert!(err.contains("--over"), "{err}");
    }

    #[test]
    fn the_written_file_is_the_merge_and_carries_its_provenance() {
        let tmp = tempfile::tempdir().expect("tmp");
        write(
            tmp.path(),
            "base.toml",
            "[connectors]\nwise.base_url = \"https://cand.wise\"\nnewthing.base_url = \"https://new\"\n",
        );
        write(
            tmp.path(),
            "recorded.toml",
            "[connectors]\nwise.base_url = \"https://rec.wise\"\n",
        );
        let out = tmp.path().join("out/boot.toml");
        let report = layer_config_files(
            &tmp.path().join("base.toml"),
            &tmp.path().join("recorded.toml"),
            &out,
            None,
        )
        .expect("layering succeeds");

        let merged: toml::Table =
            toml::from_str(&std::fs::read_to_string(&out).expect("read out")).expect("valid toml");
        assert_eq!(
            merged["connectors"]["wise"]["base_url"].as_str(),
            Some("https://rec.wise"),
            "the recording stays authoritative for a key it sets"
        );
        assert_eq!(
            merged["connectors"]["newthing"]["base_url"].as_str(),
            Some("https://new"),
            "the candidate fills a key the recording never knew about"
        );
        assert_eq!(
            report.carried,
            vec!["connectors.newthing.base_url".to_owned()]
        );

        let prov = std::fs::read_to_string(provenance_path(&out)).expect("provenance written");
        assert!(prov.contains("connectors.newthing.base_url"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&out).expect("stat").permissions().mode();
            assert_eq!(
                mode & 0o777,
                0o644,
                "the candidate may run as a different, unprivileged user and must be able to \
                 read this"
            );
        }
    }

    #[test]
    fn carried_keys_group_by_section() {
        let (_, r) = merged_table(
            "[connectors]\na.base_url = \"1\"\nb.base_url = \"2\"\n[grpc_client]\nx = 1\n",
            "[connectors]\nc.base_url = \"3\"\n",
        );
        let by = carried_by_section(&r);
        assert_eq!(by.get("connectors"), Some(&2));
        assert_eq!(by.get("grpc_client"), Some(&1));
    }
}
