//! Layering the candidate's own router config UNDER the recorded one.
//!
//! The replay Job boots the candidate router from a single config file. The
//! router reads exactly one (`Settings::with_config_path` adds one
//! `config::File` source, selected by `RUN_ENV`), so "just pass both" is not
//! available: two independent sources have to become one file before the
//! container starts.
//!
//! The two sources are:
//!
//!   * the RECORDED config — the sandbox baseline the recorded session's router
//!     actually booted from, rendered by the recording-era chart into a
//!     ConfigMap. This is the environment being reproduced.
//!   * the CANDIDATE's config — `config/deployments/<run_env>.toml` at the
//!     candidate's own ref, carried in the CodeBundle. This is the only thing
//!     that knows about keys the candidate ADDED after the recording was taken
//!     (a new connector's `base_url`, a new required section).
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

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
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
    /// Dotted paths present only in the candidate's config, carried into the
    /// merged output. Sorted.
    pub carried: Vec<String>,
    /// Leaf count taken from the recording (every recorded leaf, by invariant).
    pub from_recording: usize,
    /// Candidate leaves displaced by a recorded leaf at the SAME path. The
    /// ordinary case, and the reason the replay stays faithful.
    pub overridden: usize,
    /// Candidate leaves dropped because the recording put a value of a
    /// different SHAPE at or above that path (a scalar where the candidate has
    /// a table, or a table where it has a scalar). Rare and worth reading:
    /// these are candidate defaults the recording structurally displaced.
    /// Named, never merely counted.
    pub shadowed: Vec<String>,
}

impl LayerReport {
    /// Total leaves in the merged output.
    pub fn total(&self) -> usize {
        self.from_recording + self.carried.len()
    }

    /// A human summary for the Job's init log: the counts, then every key the
    /// merge took from the candidate rather than the recording.
    pub fn render(&self, candidate_label: &str, recorded_label: &str) -> String {
        let mut s = String::new();
        let _ = writeln!(
            s,
            "config layering: {} keys in the merged config = {} from the recording ({}) \
             + {} from the candidate ({})",
            self.total(),
            self.from_recording,
            recorded_label,
            self.carried.len(),
            candidate_label,
        );
        let _ = writeln!(
            s,
            "config layering: {} candidate keys were overridden by the recording",
            self.overridden,
        );
        for k in &self.carried {
            let _ = writeln!(s, "config layering: from candidate: {k}");
        }
        for k in &self.shadowed {
            let _ = writeln!(
                s,
                "config layering: candidate key structurally shadowed by the recording: {k}",
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
pub fn layer_toml(candidate: &str, recorded: &str) -> Result<(String, LayerReport), String> {
    let base: toml::Table = toml::from_str(candidate)
        .map_err(|e| format!("candidate config is not valid TOML: {e}"))?;
    let over: toml::Table =
        toml::from_str(recorded).map_err(|e| format!("recorded config is not valid TOML: {e}"))?;

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
             provenance accounts for {} ({} recorded + {} carried)",
            report.total(),
            report.from_recording,
            report.carried.len(),
        ));
    }
    if report.from_recording != over_leaves {
        return Err(format!(
            "config layering accounting failed: the recorded config has {over_leaves} leaves but \
             only {} survived into the merged output — the recording must never lose a key",
            report.from_recording,
        ));
    }
    let accounted_base = report.carried.len() + report.overridden + report.shadowed.len();
    if accounted_base != base_leaves {
        return Err(format!(
            "config layering accounting failed: the candidate config has {base_leaves} leaves but \
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
                report.from_recording += count_leaves(over_value);
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
        report.from_recording += count_leaves(over_value);
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

/// One replay environment: the router's `RUN_ENV` value, the config FILE NAME
/// the router resolves for it, and where that file lives in the candidate's
/// repo.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayEnvConfig {
    /// The `RUN_ENV` value.
    pub run_env: &'static str,
    /// The file name `router_env::Config::config_path` resolves under
    /// `CONFIG_DIR` for that `RUN_ENV`.
    pub config_file_name: &'static str,
    /// The candidate repo path holding that environment's config.
    pub repo_path: &'static str,
}

/// The router's environment→config-file mapping, plus the candidate repo path
/// for each. ONE table, read by BOTH halves: the CodeBundle producer decides
/// what to carry from it (`codebundle::CANDIDATE_CONFIG_FILES`) and the layering
/// step decides what applies from it. An environment cannot be carried without
/// being resolvable, or resolvable without being carried.
///
/// The environments are the router's own three config files (`router_env`'s
/// `config_path`: `production` → `production.toml`, `sandbox` → `sandbox.toml`,
/// anything else → `development.toml`). `RUN_ENV=integ` therefore boots from
/// `development.toml` and layers the candidate's `config/development.toml` under
/// it — the router's mapping, mirrored, not a choice made here.
pub const REPLAY_ENV_CONFIGS: [ReplayEnvConfig; 3] = [
    ReplayEnvConfig {
        run_env: "sandbox",
        config_file_name: "sandbox.toml",
        repo_path: "config/deployments/sandbox.toml",
    },
    ReplayEnvConfig {
        run_env: "production",
        config_file_name: "production.toml",
        repo_path: "config/deployments/production.toml",
    },
    ReplayEnvConfig {
        run_env: "development",
        config_file_name: "development.toml",
        repo_path: "config/development.toml",
    },
];

/// The config file name the ROUTER resolves for a `RUN_ENV`, mirroring
/// `router_env::Config::config_path`: an unrecognised `RUN_ENV` falls to
/// `development.toml`, exactly as the router does.
pub fn router_config_file_name(run_env: &str) -> &'static str {
    match run_env {
        "production" => "production.toml",
        "sandbox" => "sandbox.toml",
        _ => "development.toml",
    }
}

/// The replay environment a config FILE NAME denotes — the inverse of the
/// router's own mapping.
///
/// This is where the environment comes from: not a new parameter, but the name
/// of the file the router boots from. `/local/config/sandbox.toml` says
/// `sandbox`, and the candidate's `config/deployments/sandbox.toml` is what goes
/// underneath it. Nothing else in the Job has to be told.
pub fn env_config_for_file_name(file_name: &str) -> Option<&'static ReplayEnvConfig> {
    REPLAY_ENV_CONFIGS
        .iter()
        .find(|e| e.config_file_name == file_name)
}

/// Resolve which of the candidate's configs layers under a config file the
/// router will boot from at `out`, and where that file sits inside a staged
/// CodeBundle at `bundle_dir`.
///
/// `run_env_hint` is the `RUN_ENV` the Job also gives the candidate container,
/// when it is set. It is not the source of the answer — it is a CHECK on it. If
/// the environment the boot path names and the environment `RUN_ENV` names
/// disagree, the two halves of the Job would boot different environments, so
/// this refuses rather than picking one.
pub fn resolve_candidate_config(
    bundle_dir: &Path,
    out: &Path,
    run_env_hint: Option<&str>,
) -> Result<(&'static ReplayEnvConfig, PathBuf), String> {
    let file_name = out
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("--out has no file name: {}", out.display()))?;
    let env = env_config_for_file_name(file_name).ok_or_else(|| {
        let known: Vec<&str> = REPLAY_ENV_CONFIGS
            .iter()
            .map(|e| e.config_file_name)
            .collect();
        format!(
            "the router boots from {file_name}, which names no replay environment. The config \
             file name IS the environment (the router resolves RUN_ENV to one of {known:?}); \
             refusing rather than guessing which of the candidate's configs to layer under it."
        )
    })?;
    if let Some(hint) = run_env_hint {
        let expected = router_config_file_name(hint);
        if expected != file_name {
            return Err(format!(
                "environment mismatch: the router boots from {file_name} (environment \
                 {env_named}) but RUN_ENV={hint} resolves to {expected}. The candidate container \
                 and this layering step would use different environments; refusing. Make the \
                 Job's RUN_ENV and the config mount path name the same environment.",
                env_named = env.run_env,
            ));
        }
    }
    Ok((env, bundle_dir.join(env.repo_path)))
}

/// The sidecar file written next to the merged config, naming every key the
/// merge took from the candidate. The Job's init log carries the same content;
/// the file is there so a post-mortem on a finished run can read it without
/// the logs.
pub fn provenance_path(out: &Path) -> PathBuf {
    let mut p = out.as_os_str().to_owned();
    p.push(".provenance");
    PathBuf::from(p)
}

/// The bundle entry that CodeBundles staged BEFORE the candidate config became
/// per-environment carry instead of it. Bundles are cached in S3 forever, keyed
/// by candidate sha, so a ref replayed before this change still has one; its
/// presence next to a missing environment config is that bundle's signature.
///
/// Detecting it is all this does. There is no migration, no backfill and no
/// sweep: a stale bundle is diagnosed by name and refused, which costs whoever
/// hits it one re-run after the stale object is dropped. A re-staging path for
/// something this rare would be code nobody exercises.
const LEGACY_BUNDLE_CONFIG: &str = "config/docker_compose.toml";

/// Read the candidate's config for this replay's environment and the recorded
/// config, layer them, and write the single merged file the candidate boots
/// from.
///
/// Which candidate config applies is derived from `out` — the path the router
/// boots from already names the environment — and cross-checked against
/// `run_env_hint`. See [`resolve_candidate_config`].
///
/// `bundle_hint` is whatever names the CodeBundle in this deployment (its S3
/// URI). It appears in the error when the candidate config is absent, so the
/// message points at the thing that failed to deliver it rather than at the
/// file that is merely missing.
///
/// There is deliberately NO fallback — not to another environment's config, not
/// to the recorded config alone. Filling a production replay's gaps with
/// sandbox endpoints would score as a clean run while calling test endpoints,
/// which is worse than the boot panic it would be papering over; and proceeding
/// with the recorded config alone is precisely the silent behaviour that
/// produced `base_url must not be empty for <connector>` at boot, with a
/// message that named a connector instead of the mechanism.
pub fn layer_config_files(
    bundle_dir: &Path,
    recorded: &Path,
    out: &Path,
    run_env_hint: Option<&str>,
    bundle_hint: Option<&str>,
) -> Result<(&'static ReplayEnvConfig, LayerReport), String> {
    let (env, candidate) = resolve_candidate_config(bundle_dir, out, run_env_hint)?;

    let candidate_text = fs::read_to_string(&candidate).map_err(|e| {
        // Name the ref (the bundle URI is `…/codebundles/<sha>/…`), the exact
        // file this environment expected, and the one thing to do about it.
        let source = match bundle_hint {
            Some(uri) => format!("the CodeBundle staged for this candidate ref, {uri}"),
            None => "no CodeBundle URI was supplied to this Job at all".to_owned(),
        };
        let remedy = if bundle_dir.join(LEGACY_BUNDLE_CONFIG).is_file() {
            let drop_it = match bundle_hint {
                Some(uri) => format!("Drop that stale object ({uri}) and re-run"),
                None => "Drop the stale bundle object for this ref and re-run".to_owned(),
            };
            format!(
                " That bundle carries {LEGACY_BUNDLE_CONFIG} instead, so it was staged before \
                 the candidate config became per-environment. {LEGACY_BUNDLE_CONFIG} is the \
                 local docker-compose config, not this environment's, so it is not used as a \
                 substitute. {drop_it}: the bundle is a pure function of the candidate ref, so \
                 the next run stages it again with this file."
            )
        } else {
            String::new()
        };
        format!(
            "this replay boots the {run_env} environment (the router resolves RUN_ENV={run_env} \
             to {file}), but the candidate's own {repo_path} is not in {source} — looked for it \
             at {looked} ({e}).{remedy} Refusing: without it the recorded config has nothing to \
             layer over, and no other environment's config is an acceptable substitute — \
             filling a sandbox replay's gaps from a dev config, or a production replay's from a \
             sandbox one, scores as a clean run against the wrong endpoints.",
            run_env = env.run_env,
            file = env.config_file_name,
            repo_path = env.repo_path,
            looked = candidate.display(),
        )
    })?;

    let recorded_text = fs::read_to_string(recorded).map_err(|e| {
        format!(
            "the recorded router config is not at {} ({e}). It is mounted from the ConfigMap the \
             recording-era chart rendered; a Job whose config mount is missing would boot the \
             candidate under its own defaults, which is not a replay.",
            recorded.display(),
        )
    })?;

    let (merged, report) = layer_toml(&candidate_text, &recorded_text)?;

    if let Some(parent) = out.parent() {
        fs::create_dir_all(parent).map_err(|e| format!("mkdir {}: {e}", parent.display()))?;
    }
    fs::write(out, merged.as_bytes()).map_err(|e| format!("write {}: {e}", out.display()))?;
    // The hyperswitch runtime image runs as the unprivileged `app` user while
    // this step runs as the init container's user; an owner-only file would be
    // unreadable at boot. Make it world-readable explicitly rather than relying
    // on whatever umask the image happens to set.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(out, fs::Permissions::from_mode(0o644))
            .map_err(|e| format!("chmod {}: {e}", out.display()))?;
    }

    let prov = provenance_path(out);
    fs::write(
        &prov,
        report.render(
            &candidate.display().to_string(),
            &recorded.display().to_string(),
        ),
    )
    .map_err(|e| format!("write {}: {e}", prov.display()))?;

    Ok((env, report))
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
        assert_eq!(r.from_recording, 1);
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
        assert_eq!(r.from_recording, 2);
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
        assert_eq!(r.from_recording, 1);
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
        assert_eq!(r.from_recording, 5);
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
    fn invalid_candidate_toml_is_named_as_such() {
        let err = layer_toml("this is not toml", "a = 1").expect_err("must fail");
        assert!(
            err.contains("candidate config is not valid TOML"),
            "error must name WHICH side failed to parse, got: {err}"
        );
    }

    #[test]
    fn invalid_recorded_toml_is_named_as_such() {
        let err = layer_toml("a = 1", "this is not toml").expect_err("must fail");
        assert!(
            err.contains("recorded config is not valid TOML"),
            "error must name WHICH side failed to parse, got: {err}"
        );
    }

    #[test]
    fn report_renders_every_carried_key_by_name() {
        let (_, r) = merged_table(
            "[connectors]\nilixium.base_url = \"u\"\nadyen.base_url = \"d\"\n",
            "[connectors]\nadyen.base_url = \"s\"\n",
        );
        let text = r.render("candidate.toml", "recorded.toml");
        assert!(text.contains("from candidate: connectors.ilixium.base_url"));
        assert!(text.contains("1 from the candidate (candidate.toml)"));
    }

    // ── which candidate config applies: derived, never enumerated ──────────

    #[test]
    fn the_boot_path_names_the_environment() {
        let dir = std::path::Path::new("/workspace/state/codebundle");
        let (env, candidate) = resolve_candidate_config(
            dir,
            std::path::Path::new("/local/config/sandbox.toml"),
            None,
        )
        .expect("sandbox resolves");
        assert_eq!(env.run_env, "sandbox");
        assert_eq!(
            candidate,
            dir.join("config/deployments/sandbox.toml"),
            "the sandbox boot path must pull the candidate's sandbox config, not a dev one"
        );

        let (env, candidate) = resolve_candidate_config(
            dir,
            std::path::Path::new("/local/config/production.toml"),
            None,
        )
        .expect("production resolves");
        assert_eq!(env.run_env, "production");
        assert_eq!(candidate, dir.join("config/deployments/production.toml"));
    }

    #[test]
    fn an_unknown_boot_file_name_is_refused_not_guessed() {
        let err = resolve_candidate_config(
            std::path::Path::new("/b"),
            std::path::Path::new("/local/config/router.toml"),
            None,
        )
        .expect_err("must refuse");
        assert!(err.contains("router.toml"), "{err}");
        assert!(err.contains("names no replay environment"), "{err}");
    }

    #[test]
    fn run_env_disagreeing_with_the_boot_path_is_refused() {
        // The Job would boot the candidate under production while this step
        // layered sandbox defaults. Refuse rather than pick one.
        let err = resolve_candidate_config(
            std::path::Path::new("/b"),
            std::path::Path::new("/local/config/sandbox.toml"),
            Some("production"),
        )
        .expect_err("must refuse a mismatch");
        assert!(err.contains("environment mismatch"), "{err}");
        assert!(
            err.contains("sandbox.toml") && err.contains("production.toml"),
            "{err}"
        );

        // Agreement passes.
        resolve_candidate_config(
            std::path::Path::new("/b"),
            std::path::Path::new("/local/config/sandbox.toml"),
            Some("sandbox"),
        )
        .expect("agreement resolves");
    }

    #[test]
    fn the_mapping_mirrors_the_routers_own() {
        // router_env::Config::config_path: production/sandbox by name, anything
        // else falls to development.toml.
        assert_eq!(router_config_file_name("production"), "production.toml");
        assert_eq!(router_config_file_name("sandbox"), "sandbox.toml");
        assert_eq!(router_config_file_name("development"), "development.toml");
        assert_eq!(router_config_file_name("integ"), "development.toml");
        // Every environment the bundle carries is resolvable from its file name,
        // and back — the producer and the consumer read one table.
        for env in REPLAY_ENV_CONFIGS {
            assert_eq!(
                env_config_for_file_name(env.config_file_name).map(|e| e.run_env),
                Some(env.run_env)
            );
            assert_eq!(router_config_file_name(env.run_env), env.config_file_name);
        }
    }

    // ── fail closed ────────────────────────────────────────────────────────

    fn write(dir: &std::path::Path, rel: &str, body: &str) {
        let p = dir.join(rel);
        std::fs::create_dir_all(p.parent().expect("parent")).expect("mkdir");
        std::fs::write(p, body).expect("write");
    }

    #[test]
    fn a_bundle_without_this_environments_config_is_refused_by_name() {
        let tmp = tempfile::tempdir().expect("tmp");
        let bundle = tmp.path().join("codebundle");
        std::fs::create_dir_all(&bundle).expect("mkdir bundle");
        // Only production is present; this replay is sandbox.
        write(&bundle, "config/deployments/production.toml", "a = 1\n");
        write(tmp.path(), "recorded.toml", "b = 2\n");

        let err = layer_config_files(
            &bundle,
            &tmp.path().join("recorded.toml"),
            &tmp.path().join("out/sandbox.toml"),
            Some("sandbox"),
            Some("s3://bucket/codebundles/deadbeef/bundle-v2.tar"),
        )
        .expect_err("must refuse");
        assert!(err.contains("sandbox"), "must name the environment: {err}");
        assert!(
            err.contains("config/deployments/sandbox.toml"),
            "must name the file it looked for: {err}"
        );
        assert!(
            err.contains("s3://bucket/codebundles/deadbeef/bundle-v2.tar"),
            "must name the bundle that failed to deliver it: {err}"
        );
        assert!(
            !tmp.path().join("out/sandbox.toml").exists(),
            "nothing may be written when the base layer is missing"
        );
    }

    #[test]
    fn a_pre_change_bundle_with_a_uri_names_the_object_to_drop() {
        let tmp = tempfile::tempdir().expect("tmp");
        let bundle = tmp.path().join("codebundle");
        write(&bundle, "config/docker_compose.toml", "a = 1\n");
        write(tmp.path(), "recorded.toml", "b = 2\n");
        let uri = "s3://bucket/codebundles/65e1ccbb85/migrations.tar";
        let err = layer_config_files(
            &bundle,
            &tmp.path().join("recorded.toml"),
            &tmp.path().join("out/sandbox.toml"),
            Some("sandbox"),
            Some(uri),
        )
        .expect_err("must refuse");
        assert!(
            err.contains(&format!("Drop that stale object ({uri})")),
            "the remedy must name the exact object, which also names the ref: {err}"
        );
    }

    #[test]
    fn a_pre_change_bundle_is_named_as_such() {
        let tmp = tempfile::tempdir().expect("tmp");
        let bundle = tmp.path().join("codebundle");
        std::fs::create_dir_all(&bundle).expect("mkdir bundle");
        // The v1 bundle shape: docker_compose.toml and no per-environment config.
        write(&bundle, "config/docker_compose.toml", "a = 1\n");
        write(tmp.path(), "recorded.toml", "b = 2\n");

        let err = layer_config_files(
            &bundle,
            &tmp.path().join("recorded.toml"),
            &tmp.path().join("out/sandbox.toml"),
            Some("sandbox"),
            None,
        )
        .expect_err("must refuse");
        assert!(
            err.contains("config/docker_compose.toml") && err.contains("staged before"),
            "a bundle that predates the change must say so, not report a bare missing file: {err}"
        );
        assert!(
            err.contains("config/deployments/sandbox.toml"),
            "must name the environment's config file it expected: {err}"
        );
        assert!(
            err.contains("Drop the stale bundle object") && err.contains("re-run"),
            "must say what to do about it — detection without a remedy is just a bare error: \
             {err}"
        );
    }

    #[test]
    fn a_missing_recorded_config_is_refused_too() {
        let tmp = tempfile::tempdir().expect("tmp");
        let bundle = tmp.path().join("codebundle");
        write(&bundle, "config/deployments/sandbox.toml", "a = 1\n");
        let err = layer_config_files(
            &bundle,
            &tmp.path().join("nope.toml"),
            &tmp.path().join("out/sandbox.toml"),
            Some("sandbox"),
            None,
        )
        .expect_err("must refuse");
        assert!(err.contains("recorded router config"), "{err}");
    }

    #[test]
    fn the_written_file_is_the_merge_and_carries_its_provenance() {
        let tmp = tempfile::tempdir().expect("tmp");
        let bundle = tmp.path().join("codebundle");
        write(
            &bundle,
            "config/deployments/sandbox.toml",
            "[connectors]\nadyen.base_url = \"https://cand.adyen\"\nilixium.base_url = \"https://ili\"\n",
        );
        write(
            tmp.path(),
            "recorded.toml",
            "[connectors]\nadyen.base_url = \"https://rec.adyen\"\n",
        );
        let out = tmp.path().join("out/sandbox.toml");
        let (env, report) = layer_config_files(
            &bundle,
            &tmp.path().join("recorded.toml"),
            &out,
            Some("sandbox"),
            None,
        )
        .expect("layering succeeds");
        assert_eq!(env.run_env, "sandbox");

        let merged: toml::Table =
            toml::from_str(&std::fs::read_to_string(&out).expect("read out")).expect("valid toml");
        assert_eq!(
            merged["connectors"]["adyen"]["base_url"].as_str(),
            Some("https://rec.adyen"),
            "the recording stays authoritative for a key it sets"
        );
        assert_eq!(
            merged["connectors"]["ilixium"]["base_url"].as_str(),
            Some("https://ili"),
            "the candidate fills a key the recording never knew about"
        );
        assert_eq!(
            report.carried,
            vec!["connectors.ilixium.base_url".to_owned()]
        );

        let prov = std::fs::read_to_string(provenance_path(&out)).expect("provenance written");
        assert!(prov.contains("connectors.ilixium.base_url"));

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&out).expect("stat").permissions().mode();
            assert_eq!(
                mode & 0o777,
                0o644,
                "the router runs as an unprivileged user and must be able to read this"
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
