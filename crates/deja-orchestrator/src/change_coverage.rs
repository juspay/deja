//! Did the replay reach what the candidate changed?
//!
//! A verdict says whether behaviour changed on the recorded traffic. It says
//! nothing about code the traffic never reached: a change to a connector the
//! tape does not carry replays clean, and that pass is worth nothing to the
//! author reading it. Three probes in prism's validation matrix passed exactly
//! this way — an edited status mapping on a function the tape never called, an
//! edited URL on a branch the tape never took, an edited helper used only by
//! connectors that were not recorded.
//!
//! This module crosses the candidate's CHANGE SET with the replay's EVIDENCE.
//!
//! The change set is the diff from the merge-base with the system's base
//! branch to the candidate's sha, read from the git host's compare endpoint —
//! the orchestrator is the one process with git-host egress; the sealed replay
//! pod never makes this call, which is why this runs on the control plane after
//! the run, not in the scorer. The changed files are read at the candidate sha
//! from the same codeload tarball the CodeBundle producer fetches, so a change
//! is located to its enclosing Rust item: the `fn`, and above it the
//! `impl ConnectorIntegration<Flow, …> for X` or `macro_connector_implementation!
//! (… flow_name: … )` block that names the flow. A path under `connectors/<name>`
//! names the connector.
//!
//! The evidence is the run's own replay execution graph — every span's module
//! (`target`), its name, and the lattice's `connector` and `flow` fields — and
//! the call ledger's call sites. From those: which modules opened a span, which
//! (connector, flow) pairs ran and in how many requests, which named spans ran,
//! and which source lines made a boundary call.
//!
//! Per changed item, most specific evidence first:
//!
//! - `not_exercised`: the connector never ran; or it ran but never that flow;
//!   or the item is a helper with no span whose only callers (one hop, by
//!   name) are connectors and modules that never ran. A pass says NOTHING
//!   about this item.
//! - `exercised`: a span declared inside the `fn` ran, or a boundary call site
//!   inside its lines ran. A pass speaks to this item.
//! - `flow_ran` / `flow_ran_weak`: the (connector, flow) pair ran, in n
//!   requests (weak below three), but the `fn` has no probe of its own — which
//!   branch inside it ran is unproven without line coverage.
//! - `module_ran`: the module (or, for a transformers file, the connector) ran;
//!   the item is not tied to a flow and has no probe.
//! - `unknown`: no span in the module and no caller found by name.
//!
//! The result is a separate artifact beside the scorecard, never a change to
//! the verdict: the verdict is what the traffic showed, this is how much of the
//! change that traffic reached. A caller that cannot assess (no repository
//! declared, no egress, a layout the scanner does not read) gets a reason, not
//! an error, so nothing downstream ever fails because of this.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Read;

use serde::{Deserialize, Serialize};

// ── result shape ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reach {
    NotExercised,
    Exercised,
    FlowRan,
    FlowRanWeak,
    ModuleRan,
    Unknown,
}

impl Reach {
    fn is_proven(self) -> bool {
        matches!(self, Reach::Exercised)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangedItem {
    pub path: String,
    /// The enclosing items, outer to inner: `impl … for Paypal / get_url`.
    pub item: String,
    pub lines: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow: Option<String>,
    pub reach: Reach,
    pub why: String,
}

/// The assessment, or the reason there is none. Serialized as the run's
/// `change_coverage.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChangeCoverage {
    pub system: String,
    pub repo: String,
    pub base_ref: String,
    pub merge_base: String,
    pub head: String,
    pub driven_requests: usize,
    pub items: Vec<ChangedItem>,
    pub never_ran: usize,
    pub unproven: usize,
    /// The lines a reader acts on: empty when every item was exercised.
    pub caveats: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Assessment {
    Assessed(ChangeCoverage),
    Unavailable { unavailable: String },
}

// ── change set ──────────────────────────────────────────────────────────────

/// One changed source file: its path and the new-side line ranges its hunks
/// cover, with the file's content at the head.
#[derive(Debug, Clone)]
pub struct ChangedFile {
    pub path: String,
    pub ranges: Vec<(usize, usize)>,
    pub content: String,
}

#[derive(Debug, Clone)]
pub struct ChangeSet {
    pub merge_base: String,
    pub head: String,
    pub files: Vec<ChangedFile>,
    /// Every `.rs` file and `Cargo.toml` at the head: manifests name the crate
    /// a file belongs to, sources feed the one-hop caller search.
    pub tree: BTreeMap<String, String>,
}

/// New-side line ranges of the CHANGED lines in a unified-diff `patch`, one
/// run of consecutive added or removed lines per range. Context lines are
/// skipped: a git host's patch carries three of them around every hunk, and a
/// hunk that starts in the previous function's tail must not be charged to it.
/// A removed line is attributed to the new-side line it left behind.
pub fn hunk_ranges(patch: &str) -> Vec<(usize, usize)> {
    let mut out: Vec<(usize, usize)> = Vec::new();
    let mut new_line = 0usize;
    let push = |line: usize, out: &mut Vec<(usize, usize)>| match out.last_mut() {
        Some((_, end)) if *end + 1 >= line => *end = (*end).max(line),
        _ => out.push((line, line)),
    };
    for line in patch.lines() {
        if let Some(rest) = line.strip_prefix("@@ ") {
            let Some(plus) = rest.split(' ').find(|s| s.starts_with('+')) else {
                continue;
            };
            new_line = plus[1..]
                .split_once(',')
                .map(|(s, _)| s)
                .unwrap_or(&plus[1..])
                .parse::<usize>()
                .unwrap_or(0);
            continue;
        }
        if new_line == 0 {
            continue;
        }
        match line.chars().next() {
            Some('+') => {
                push(new_line, &mut out);
                new_line += 1;
            }
            Some('-') => push(new_line.max(1), &mut out),
            Some('\\') => {} // "\ No newline at end of file"
            _ => new_line += 1,
        }
    }
    out
}

#[derive(Deserialize)]
struct CompareResponse {
    merge_base_commit: CompareCommit,
    #[serde(default)]
    files: Vec<CompareFile>,
}
#[derive(Deserialize)]
struct CompareCommit {
    sha: String,
}
#[derive(Deserialize)]
struct CompareFile {
    filename: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    patch: Option<String>,
}

fn github_api_base() -> String {
    std::env::var("DEJA_GITHUB_API_URL")
        .ok()
        .map(|v| v.trim().trim_end_matches('/').to_owned())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_owned())
}

/// The compare endpoint: merge-base and the changed files with their patches.
/// Renamed and deleted files are kept only if they still exist at the head.
fn fetch_compare(repo: &str, base_ref: &str, head: &str) -> Result<CompareResponse, String> {
    let url = format!(
        "{}/repos/{repo}/compare/{base_ref}...{head}",
        github_api_base()
    );
    let mut req = crate::codebundle::tarball_agent()
        .get(&url)
        .set("Accept", "application/vnd.github+json")
        .set("User-Agent", "deja-orchestrator");
    if let Some(token) = std::env::var("DEJA_GITHUB_TOKEN")
        .ok()
        .map(|t| t.trim().to_owned())
        .filter(|t| !t.is_empty())
    {
        req = req.set("Authorization", &format!("Bearer {token}"));
    }
    let resp = req
        .call()
        .map_err(|e| format!("compare {base_ref}...{} on {repo}: {e}", short(head)))?;
    resp.into_json::<CompareResponse>()
        .map_err(|e| format!("compare response from {repo}: {e}"))
}

/// Every `.rs` file and `Cargo.toml` in a codeload-style tarball (paths with
/// the single `{repo}-{sha}/` top segment stripped), as text.
pub fn rust_tree_from_targz<R: Read>(src: R) -> Result<BTreeMap<String, String>, String> {
    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(src));
    let mut tree = BTreeMap::new();
    for entry in archive
        .entries()
        .map_err(|e| format!("read tarball: {e}"))?
    {
        let mut entry = entry.map_err(|e| format!("tarball entry: {e}"))?;
        let path = entry
            .path()
            .map_err(|e| format!("tarball entry path: {e}"))?
            .to_string_lossy()
            .into_owned();
        if !(path.ends_with(".rs") || path.ends_with("Cargo.toml")) {
            continue;
        }
        let Some((_, rel)) = path.split_once('/') else {
            continue;
        };
        let mut text = String::new();
        if entry.read_to_string(&mut text).is_ok() {
            tree.insert(rel.to_owned(), text);
        }
    }
    Ok(tree)
}

fn fetch_tree(template: &str, repo: &str, sha: &str) -> Result<BTreeMap<String, String>, String> {
    let url =
        crate::api::runs::resolve_tarball_url(template, Some(repo), sha).ok_or_else(|| {
            "the tarball URL template needs a repository and none resolved".to_owned()
        })?;
    let resp = crate::codebundle::tarball_agent()
        .get(&url)
        .call()
        .map_err(|e| format!("fetch repo tarball for {}: {e}", short(sha)))?;
    rust_tree_from_targz(resp.into_reader())
}

/// The change set for a candidate: compare from the git host, sources from the
/// codeload tarball. `tarball_template` is `DEJA_CANDIDATE_TARBALL_URL`.
pub fn fetch_change_set(
    repo: &str,
    base_ref: &str,
    head: &str,
    tarball_template: &str,
) -> Result<ChangeSet, String> {
    let compare = fetch_compare(repo, base_ref, head)?;
    let tree = fetch_tree(tarball_template, repo, head)?;
    let files = compare
        .files
        .into_iter()
        .filter(|f| f.filename.ends_with(".rs") && f.status != "removed")
        .filter_map(|f| {
            let content = tree.get(&f.filename)?.clone();
            let ranges = f.patch.as_deref().map(hunk_ranges).unwrap_or_default();
            Some(ChangedFile {
                path: f.filename,
                ranges,
                content,
            })
        })
        .collect();
    Ok(ChangeSet {
        merge_base: compare.merge_base_commit.sha,
        head: head.to_owned(),
        files,
        tree,
    })
}

// ── Rust item scan ──────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ItemKind {
    Fn,
    Impl,
    Macro,
    Other,
}

/// A source item with its line range. Brace-depth based, which reads the
/// formatting these repositories use; it is a locator, not a parser.
#[derive(Debug, Clone)]
pub struct Item {
    pub kind: ItemKind,
    pub name: String,
    pub start: usize,
    pub end: usize,
    /// For an `impl ConnectorIntegration<Flow, …>` or a macro invocation with
    /// `flow_name: Flow`, the flow it implements.
    pub flow: Option<String>,
}

fn item_head(line: &str) -> Option<(ItemKind, String)> {
    let t = line.trim_start();
    let mut rest = t;
    for prefix in ["pub(crate) ", "pub(super) ", "pub "] {
        if let Some(r) = rest.strip_prefix(prefix) {
            rest = r;
            break;
        }
    }
    if let Some(r) = rest.strip_prefix("pub(") {
        // `pub(in path) `
        rest = r.split_once(") ").map(|(_, r)| r).unwrap_or(r);
    }
    loop {
        let mut stripped = false;
        for prefix in ["async ", "const ", "unsafe ", "extern \"C\" "] {
            if let Some(r) = rest.strip_prefix(prefix) {
                rest = r;
                stripped = true;
            }
        }
        if !stripped {
            break;
        }
    }
    if let Some(r) = rest.strip_prefix("fn ") {
        let name: String = r
            .chars()
            .take_while(|c| c.is_alphanumeric() || *c == '_')
            .collect();
        return (!name.is_empty()).then_some((ItemKind::Fn, name));
    }
    if rest.starts_with("impl ") || rest.starts_with("impl<") {
        return Some((ItemKind::Impl, String::new()));
    }
    for kw in ["struct ", "enum ", "trait ", "mod "] {
        if let Some(r) = rest.strip_prefix(kw) {
            let name: String = r
                .chars()
                .take_while(|c| c.is_alphanumeric() || *c == '_')
                .collect();
            return Some((ItemKind::Other, name));
        }
    }
    // a macro invocation at the top level: `macros::name!(` or `name! {`
    let head: String = t
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_' || *c == ':')
        .collect();
    if !head.is_empty() && t[head.len()..].starts_with('!') {
        let after = t[head.len() + 1..].trim_start();
        if after.starts_with('(') || after.starts_with('{') {
            return Some((ItemKind::Macro, format!("{head}!")));
        }
    }
    None
}

fn flow_of_impl(header: &str) -> Option<String> {
    let idx = header.find("ConnectorIntegration")?;
    let after = &header[idx..];
    let lt = after.find('<')?;
    let name: String = after[lt + 1..]
        .trim_start()
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then_some(name)
}

fn flow_of_macro(body: &str) -> Option<String> {
    // Only a per-flow implementation names its flow; `create_all_prerequisites!`
    // lists every flow and belongs to all of them.
    let idx = body.find("flow_name")?;
    let after = body[idx + "flow_name".len()..].trim_start();
    let after = after.strip_prefix(':')?.trim_start();
    let name: String = after
        .chars()
        .take_while(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    (!name.is_empty()).then_some(name)
}

/// Locate the `fn`, `impl` and macro-invocation items of a source file.
pub fn scan_items(text: &str) -> Vec<Item> {
    struct Open {
        kind: ItemKind,
        name: String,
        start: usize,
        depth: usize,
        header: String,
    }
    let lines: Vec<&str> = text.lines().collect();
    let mut items = Vec::new();
    let mut stack: Vec<Open> = Vec::new();
    let mut pending: Option<Open> = None;
    let mut depth = 0usize; // braces
    let mut pdepth = 0usize; // parens
    for (idx, raw) in lines.iter().enumerate() {
        let no = idx + 1;
        let line = raw.split("//").next().unwrap_or("");
        let in_macro = stack.last().is_some_and(|o| o.kind == ItemKind::Macro);
        let item_scope = (depth <= 1 && pdepth == 0) || (in_macro && pdepth == 1 && depth <= 1);
        if pending.is_none() && item_scope {
            if let Some((kind, name)) = item_head(line) {
                if kind == ItemKind::Macro && !(depth == 0 && pdepth == 0) {
                    // nested macro calls are expressions, not items
                } else {
                    pending = Some(Open {
                        kind,
                        name,
                        start: no,
                        depth,
                        header: String::new(),
                    });
                }
            }
        }
        if let Some(p) = pending.as_mut() {
            p.header.push_str(line);
            p.header.push(' ');
        }
        for ch in line.chars() {
            match ch {
                '{' => {
                    if let Some(mut p) = pending.take() {
                        if p.kind == ItemKind::Macro {
                            pending = Some(p);
                        } else {
                            p.depth = depth;
                            stack.push(p);
                        }
                    }
                    depth += 1;
                }
                '}' => {
                    depth = depth.saturating_sub(1);
                    if stack
                        .last()
                        .is_some_and(|o| o.kind != ItemKind::Macro && o.depth == depth)
                    {
                        let o = stack.pop().expect("checked");
                        let flow = (o.kind == ItemKind::Impl)
                            .then(|| flow_of_impl(&o.header))
                            .flatten();
                        let name = match o.kind {
                            ItemKind::Impl => {
                                let h = o.header.split('{').next().unwrap_or("").trim();
                                let h: String = h.split_whitespace().collect::<Vec<_>>().join(" ");
                                h.chars().take(72).collect()
                            }
                            _ => o.name,
                        };
                        items.push(Item {
                            kind: o.kind,
                            name,
                            start: o.start,
                            end: no,
                            flow,
                        });
                    }
                }
                '(' => {
                    if let Some(p) = pending.take() {
                        if p.kind == ItemKind::Macro && pdepth == 0 {
                            stack.push(p);
                        } else {
                            pending = Some(p);
                        }
                    }
                    pdepth += 1;
                }
                ')' => {
                    pdepth = pdepth.saturating_sub(1);
                    if pdepth == 0 && stack.last().is_some_and(|o| o.kind == ItemKind::Macro) {
                        let o = stack.pop().expect("checked");
                        let body: String = lines[o.start - 1..no].join("\n");
                        items.push(Item {
                            kind: ItemKind::Macro,
                            name: o.name,
                            start: o.start,
                            end: no,
                            flow: flow_of_macro(&body),
                        });
                    }
                }
                _ => {}
            }
        }
        // a trait method declaration without a body
        if pending
            .as_ref()
            .is_some_and(|p| p.kind == ItemKind::Fn && line.trim_end().ends_with(';'))
        {
            pending = None;
        }
    }
    items
}

/// The innermost `fn` at `line` (or the one whose attribute block the line sits
/// in), and the `impl` / macro item around it.
pub fn enclosing(items: &[Item], line: usize) -> (Option<&Item>, Option<&Item>) {
    let mut inner: Vec<&Item> = items
        .iter()
        .filter(|it| it.start <= line && line <= it.end)
        .collect();
    inner.sort_by_key(|it| it.end - it.start);
    let mut fn_item = inner.iter().find(|it| it.kind == ItemKind::Fn).copied();
    if fn_item.is_none() {
        // a hunk on `#[instrument(name = …)]` sits just above its fn
        fn_item = items
            .iter()
            .filter(|it| it.kind == ItemKind::Fn && it.start > line && it.start - line <= 12)
            .min_by_key(|it| it.start);
    }
    let outer = inner
        .iter()
        .find(|it| matches!(it.kind, ItemKind::Impl | ItemKind::Macro))
        .copied();
    (fn_item, outer)
}

/// Span names declared on or inside a `fn`: `#[instrument(name = "…")]` in the
/// attribute block above its signature, and `*_span!("…")` in its body.
pub fn spans_declared(text: &str, start: usize, end: usize) -> BTreeSet<String> {
    let lines: Vec<&str> = text.lines().collect();
    let mut top = start.saturating_sub(1);
    // walk up through the attribute block (and comment lines between
    // attributes) to the first blank or code line
    while top > 0 && start - top < 40 {
        let prev = lines[top - 1].trim();
        let attr_ish = prev.starts_with("#[")
            || prev.starts_with("//")
            || prev.starts_with(')')
            || prev.starts_with(']')
            || prev.starts_with("tracing::")
            || prev.starts_with("name")
            || prev.starts_with("skip")
            || prev.starts_with("fields")
            || prev.starts_with("feature")
            || prev.starts_with("level");
        if !attr_ish {
            break;
        }
        top -= 1;
    }
    let mut out = BTreeSet::new();
    for line in &lines[top..end.min(lines.len())] {
        for key in [
            "name = \"",
            "info_span!(\"",
            "debug_span!(\"",
            "trace_span!(\"",
            "warn_span!(\"",
            "error_span!(\"",
            "span!(\"",
        ] {
            let mut rest = *line;
            while let Some(idx) = rest.find(key) {
                let after = &rest[idx + key.len()..];
                if let Some(close) = after.find('"') {
                    out.insert(after[..close].to_owned());
                }
                rest = after;
            }
        }
    }
    out
}

// ── layout ──────────────────────────────────────────────────────────────────

/// `crates/<group>/<crate>/src/<mods>.rs` → `crate_name::mods`, with the crate
/// name read from the nearest `Cargo.toml` in the tree.
pub fn module_of(tree_cargo: &BTreeMap<String, String>, path: &str) -> Option<String> {
    let parts: Vec<&str> = path.split('/').collect();
    for i in (1..parts.len()).rev() {
        let cargo = format!("{}/Cargo.toml", parts[..i].join("/"));
        let Some(manifest) = tree_cargo.get(&cargo) else {
            continue;
        };
        let name = manifest
            .lines()
            .find_map(|l| {
                let l = l.trim();
                l.strip_prefix("name")
                    .map(|r| r.trim_start())
                    .and_then(|r| r.strip_prefix('='))
                    .map(|r| r.trim().trim_matches('"').to_owned())
            })?
            .replace('-', "_");
        let mut rel: Vec<&str> = parts[i..].to_vec();
        if rel.first() == Some(&"src") {
            rel.remove(0);
        }
        let mods: Vec<String> = rel
            .iter()
            .map(|p| p.strip_suffix(".rs").unwrap_or(p).to_owned())
            .filter(|m| !matches!(m.as_str(), "lib" | "main" | "mod"))
            .collect();
        return Some(
            std::iter::once(name)
                .chain(mods)
                .collect::<Vec<_>>()
                .join("::"),
        );
    }
    None
}

/// `…/connectors/<name>.rs` or `…/connectors/<name>/…` → `<name>`.
pub fn connector_of(path: &str) -> Option<String> {
    let idx = path.find("/connectors/")?;
    let rest = &path[idx + "/connectors/".len()..];
    let name: String = rest
        .chars()
        .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect();
    let after = &rest[name.len()..];
    (!name.is_empty() && (after == ".rs" || after.starts_with('/'))).then_some(name)
}

fn norm(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .flat_map(|c| c.to_lowercase())
        .collect()
}

fn short(sha: &str) -> &str {
    &sha[..sha.len().min(10)]
}

// ── evidence ────────────────────────────────────────────────────────────────

/// What the replay ran, read off the run's replay graph and call ledger.
#[derive(Debug, Default)]
pub struct Evidence {
    pub driven: usize,
    modules: HashSet<String>,
    spans: HashSet<(String, String)>,
    connectors: HashMap<String, BTreeSet<String>>,
    connector_flows: HashMap<(String, String), BTreeSet<String>>,
    sites: HashMap<(String, u32), usize>,
}

impl Evidence {
    pub fn from_graph_and_calls(
        replay: &[deja_core::ExecutionGraphNode],
        calls: &[serde_json::Value],
    ) -> Self {
        let mut ev = Evidence::default();
        let mut correlations = HashSet::new();
        for n in replay.iter().filter(|n| n.correlation_id.is_some()) {
            let corr = n.correlation_id.clone().unwrap_or_default();
            correlations.insert(corr.clone());
            ev.modules.insert(n.target.clone());
            ev.spans.insert((n.target.clone(), n.span_name.clone()));
            let connector = n.fields.get("connector").and_then(|v| v.as_str()).map(|c| {
                // `Payment(Paypal)` on the orchestration span, `Paypal` below it
                match (c.find('('), c.strip_suffix(')')) {
                    (Some(i), Some(inner)) => inner[i + 1..].to_owned(),
                    _ => c.to_owned(),
                }
            });
            let flow = n.fields.get("flow").and_then(|v| v.as_str());
            if let Some(c) = connector {
                ev.connectors
                    .entry(norm(&c))
                    .or_default()
                    .insert(corr.clone());
                if let Some(f) = flow {
                    ev.connector_flows
                        .entry((norm(&c), norm(f)))
                        .or_default()
                        .insert(corr.clone());
                }
            }
        }
        for row in calls {
            let Some(o) = row.get("observed") else {
                continue;
            };
            if let (Some(file), Some(line)) = (
                o.get("call_file").and_then(|v| v.as_str()),
                o.get("call_line").and_then(|v| v.as_u64()),
            ) {
                *ev.sites.entry((file.to_owned(), line as u32)).or_default() += 1;
            }
        }
        ev.driven = correlations.len();
        ev
    }

    fn module_ran(&self, module: &str) -> bool {
        self.modules
            .iter()
            .any(|t| t == module || t.starts_with(&format!("{module}::")))
    }

    fn span_ran(&self, module: &str, name: &str) -> bool {
        self.spans
            .iter()
            .any(|(t, sn)| sn == name && (t == module || t.starts_with(&format!("{module}::"))))
    }

    fn sites_in(&self, path: &str, start: usize, end: usize) -> usize {
        self.sites
            .iter()
            .filter(|((f, l), _)| f == path && (*l as usize) >= start && (*l as usize) <= end)
            .map(|(_, n)| *n)
            .sum()
    }
}

// ── classification ──────────────────────────────────────────────────────────

/// Connectors and modules whose source calls `fn_name` (one hop, by name — so
/// it can over-match a common name; it only ever explains a function that has
/// no probe, never promotes one).
fn callers_of(
    tree: &BTreeMap<String, String>,
    manifests: &BTreeMap<String, String>,
    fn_name: &str,
    own_path: &str,
) -> BTreeSet<String> {
    let call = format!("{fn_name}(");
    let def = format!("fn {fn_name}");
    let mut out = BTreeSet::new();
    for (path, text) in tree {
        if path == own_path || !path.ends_with(".rs") || path.contains("/tests/") {
            continue;
        }
        let hit = text.lines().any(|l| {
            l.contains(&call)
                && !l.contains(&def)
                && l.split(|c: char| !(c.is_alphanumeric() || c == '_'))
                    .any(|w| w == fn_name)
        });
        if hit {
            out.insert(
                connector_of(path)
                    .or_else(|| module_of(manifests, path))
                    .unwrap_or_else(|| path.clone()),
            );
        }
    }
    out
}

pub fn classify(
    change: &ChangeSet,
    ev: &Evidence,
    manifests: &BTreeMap<String, String>,
) -> Vec<ChangedItem> {
    let mut out = Vec::new();
    for file in &change.files {
        let module = module_of(manifests, &file.path).unwrap_or_else(|| file.path.clone());
        let items = scan_items(&file.content);
        let connector = connector_of(&file.path);
        let mut seen = HashSet::new();
        for &(a, b) in &file.ranges {
            let (fn_item, outer) = enclosing(&items, a);
            let key = (fn_item.map(|f| f.start), outer.map(|o| o.start));
            if !seen.insert(key) {
                continue;
            }
            let flow = outer.and_then(|o| o.flow.clone());
            let name = [
                outer.map(|o| o.name.clone()),
                fn_item.map(|f| f.name.clone()),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(" / ");
            let name = if name.is_empty() {
                format!("lines {a}-{b}")
            } else {
                name
            };
            let probe = fn_item.and_then(|f| {
                let mut declared = spans_declared(&file.content, f.start, f.end);
                declared.insert(f.name.clone());
                let hits: Vec<&String> = declared
                    .iter()
                    .filter(|s| ev.span_ran(&module, s))
                    .collect();
                if !hits.is_empty() {
                    return Some(format!(
                        "span {}",
                        hits.iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ));
                }
                let sites = ev.sites_in(&file.path, f.start, f.end);
                (sites > 0).then(|| format!("boundary call site x{sites}"))
            });
            let (reach, why) = match &connector {
                Some(c) => {
                    let cn = norm(c);
                    if !ev.connectors.contains_key(&cn) {
                        (
                            Reach::NotExercised,
                            format!("connector {c} never ran on this tape"),
                        )
                    } else if let Some(f) = flow
                        .as_ref()
                        .filter(|f| !ev.connector_flows.contains_key(&(cn.clone(), norm(f))))
                    {
                        let ran: Vec<String> = ev
                            .connector_flows
                            .keys()
                            .filter(|(k, _)| *k == cn)
                            .map(|(_, f)| f.clone())
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect();
                        (
                            Reach::NotExercised,
                            format!("{c} ran, but never its {f} flow (ran: {})", ran.join(", ")),
                        )
                    } else if let Some(p) = probe {
                        (Reach::Exercised, p)
                    } else if let Some(f) = &flow {
                        let n = ev
                            .connector_flows
                            .get(&(cn.clone(), norm(f)))
                            .map(|s| s.len())
                            .unwrap_or(0);
                        (
                            if n >= 3 { Reach::FlowRan } else { Reach::FlowRanWeak },
                            format!(
                                "{c} {f} ran in {n} of {} driven requests; which branch inside it ran is unproven without line coverage",
                                ev.driven
                            ),
                        )
                    } else {
                        let flows: Vec<String> = ev
                            .connector_flows
                            .iter()
                            .filter(|((k, _), _)| *k == cn)
                            .map(|((_, f), ids)| format!("{f} x{}", ids.len()))
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect();
                        (
                            Reach::ModuleRan,
                            format!(
                                "{c} ran ({}); this item is not tied to one flow and has no span, so its own execution is unproven",
                                flows.join(", ")
                            ),
                        )
                    }
                }
                None => {
                    if let Some(p) = probe {
                        (Reach::Exercised, format!("{p} in {module}"))
                    } else if ev.module_ran(&module) {
                        (
                            Reach::ModuleRan,
                            format!("module {module} ran, but this function has no span or call site of its own"),
                        )
                    } else if let Some(f) = fn_item {
                        let callers = callers_of(&change.tree, manifests, &f.name, &file.path);
                        if callers.is_empty() {
                            (
                                Reach::Unknown,
                                format!("no span in {module} and no caller found by name"),
                            )
                        } else {
                            let live: Vec<&String> = callers
                                .iter()
                                .filter(|c| {
                                    ev.connectors.contains_key(&norm(c)) || ev.module_ran(c)
                                })
                                .collect();
                            if live.is_empty() {
                                (
                                    Reach::NotExercised,
                                    format!(
                                        "no span in {module}; its only callers are {}, none of which ran on this tape",
                                        callers.iter().cloned().collect::<Vec<_>>().join(", ")
                                    ),
                                )
                            } else {
                                (
                                    Reach::ModuleRan,
                                    format!(
                                        "no span in {module}; called from {} which ran",
                                        live.iter()
                                            .map(|s| s.as_str())
                                            .collect::<Vec<_>>()
                                            .join(", ")
                                    ),
                                )
                            }
                        }
                    } else {
                        (Reach::Unknown, format!("no span in {module}"))
                    }
                }
            };
            out.push(ChangedItem {
                path: file.path.clone(),
                item: name,
                lines: format!("{a}-{b}"),
                connector: connector.clone(),
                flow: flow.clone(),
                reach,
                why,
            });
        }
    }
    out
}

fn finish(
    system: &str,
    repo: &str,
    base_ref: &str,
    change: &ChangeSet,
    ev: &Evidence,
    items: Vec<ChangedItem>,
) -> ChangeCoverage {
    let never_ran = items
        .iter()
        .filter(|i| i.reach == Reach::NotExercised)
        .count();
    let unproven = items
        .iter()
        .filter(|i| i.reach != Reach::NotExercised && !i.reach.is_proven())
        .count();
    let mut caveats = Vec::new();
    if items.is_empty() {
        caveats.push("no Rust source changed between the base and the candidate, so there is nothing to reach".to_owned());
    }
    if never_ran > 0 {
        caveats.push(format!(
            "{never_ran} of {} changed item(s) never ran on this tape — a pass says nothing about them",
            items.len()
        ));
    }
    if unproven > 0 {
        caveats.push(format!(
            "{unproven} of {} changed item(s) sit on a path that ran, but their own execution is unproven at span granularity",
            items.len()
        ));
    }
    ChangeCoverage {
        system: system.to_owned(),
        repo: repo.to_owned(),
        base_ref: base_ref.to_owned(),
        merge_base: change.merge_base.clone(),
        head: change.head.clone(),
        driven_requests: ev.driven,
        items,
        never_ran,
        unproven,
        caveats,
    }
}

/// The whole assessment for a run: the change set (from the git host) against
/// the evidence (from the run's artifacts). Crate names for module paths come
/// from the manifests in the tree.
pub fn assess(
    system: &str,
    repo: &str,
    base_ref: &str,
    change: ChangeSet,
    ev: &Evidence,
) -> ChangeCoverage {
    let manifests: BTreeMap<String, String> = change
        .tree
        .iter()
        .filter(|(p, _)| p.ends_with("Cargo.toml"))
        .map(|(p, t)| (p.clone(), t.clone()))
        .collect();
    let items = classify(&change, ev, &manifests);
    finish(system, repo, base_ref, &change, ev, items)
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const CONNECTOR_FILE: &str = r#"
use x::y;

macros::create_all_prerequisites!(
    connector_name: Paypal,
    api: [ (flow: Authorize, ...), (flow: PSync, ...) ],
    amount_converters: [],
    member_functions: {
        pub fn build_headers(&self) -> Vec<(String, String)> {
            let x = 1; // a comment with { a brace
            vec![]
        }
    }
);

macros::macro_connector_implementation!(
    connector_default_implementations: [get_content_type, get_error_response_v2],
    connector: Paypal,
    curl_request: Json(PaypalSyncRequest),
    flow_name: PSync,
    resource_common_data: PaymentFlowData,
    http_method: Get,
    other_functions: {
        fn get_headers(&self, req: &Req) -> Result<Vec<(String, String)>, E> {
            self.build_headers()
        }
        fn get_url(&self, req: &Req) -> Result<String, E> {
            Ok(format!("{}v2/checkout/orders/{}", base, id))
        }
    }
);

impl ConnectorIntegrationV2<Refund, RefundFlowData, RefundsData, RefundsResponseData> for Paypal {
    fn get_url(&self, req: &Req) -> Result<String, E> {
        Ok("refund".to_owned())
    }
}

pub(crate) fn get_order_status(item: Status, intent: Intent) -> AttemptStatus {
    match item {
        Status::Completed => AttemptStatus::Authorized,
        _ => AttemptStatus::Pending,
    }
}
"#;

    const CORE_FILE: &str = r#"
impl Payments {
    // A comment about the span.
    #[cfg_attr(
        feature = "deja",
        tracing::instrument(
            name = "ucs::flow_orchestration",
            skip_all,
            fields(connector = ?connector, flow = "Authorize")
        )
    )]
    async fn process_authorization_internal(&self, req: Req) -> Result<Resp, E> {
        let _probe = tracing::info_span!("art_canary::probe").entered();
        Ok(Resp::default())
    }

    fn helper(&self) -> u32 {
        1
    }
}
"#;

    fn node(
        corr: &str,
        target: &str,
        span: &str,
        fields: &[(&str, &str)],
    ) -> deja_core::ExecutionGraphNode {
        deja_core::ExecutionGraphNode {
            node_id: 1,
            global_sequence: 0,
            parent_id: None,
            causal_parent_ids: Vec::new(),
            sequence: 1,
            correlation_id: Some(corr.to_owned()),
            recording_run_id: None,
            span_name: span.to_owned(),
            target: target.to_owned(),
            level: "INFO".to_owned(),
            fields: fields
                .iter()
                .map(|(k, v)| ((*k).to_owned(), serde_json::Value::String((*v).to_owned())))
                .collect(),
            started_ns: 0,
            closed_ns: None,
        }
    }

    /// A tape on which paypal ran Authorize once and PSync twice, adyen ran
    /// Authorize once, and the core orchestration span opened for each.
    fn evidence() -> Evidence {
        let replay = vec![
            node(
                "c1",
                "connector_integration::connectors::paypal",
                "connector::request_body",
                &[("connector", "Paypal"), ("flow", "Authorize")],
            ),
            node(
                "c2",
                "connector_integration::connectors::paypal",
                "connector::request_body",
                &[("connector", "Paypal"), ("flow", "PSync")],
            ),
            node(
                "c3",
                "connector_integration::connectors::paypal",
                "connector::request_body",
                &[("connector", "Paypal"), ("flow", "PSync")],
            ),
            node(
                "c4",
                "connector_integration::connectors::adyen",
                "connector::request_body",
                &[("connector", "Adyen"), ("flow", "Authorize")],
            ),
            node(
                "c1",
                "grpc_server::server::payments",
                "ucs::flow_orchestration",
                &[("connector", "Payment(Paypal)"), ("flow", "Authorize")],
            ),
            node(
                "c1",
                "external_services::service",
                "execute_connector_processing_step",
                &[],
            ),
        ];
        let calls = vec![serde_json::json!({
            "observed": {"call_file": "crates/common/external-services/src/service.rs", "call_line": 1377}
        })];
        Evidence::from_graph_and_calls(&replay, &calls)
    }

    fn manifests() -> BTreeMap<String, String> {
        [
            (
                "crates/integrations/connector-integration/Cargo.toml",
                "[package]\nname = \"connector-integration\"\n",
            ),
            (
                "crates/grpc-server/grpc-server/Cargo.toml",
                "[package]\nname = \"grpc-server\"\n",
            ),
            (
                "crates/types-traits/domain_types/Cargo.toml",
                "[package]\nname = \"domain_types\"\n",
            ),
            (
                "crates/common/external-services/Cargo.toml",
                "[package]\nname = \"external-services\"\n",
            ),
        ]
        .into_iter()
        .map(|(p, t)| (p.to_owned(), t.to_owned()))
        .collect()
    }

    /// `(path, content, changed ranges)`.
    type FileSpec<'a> = (&'a str, &'a str, Vec<(usize, usize)>);

    fn change(files: Vec<FileSpec<'_>>, extra_tree: Vec<(&str, &str)>) -> ChangeSet {
        let mut tree = manifests();
        for (p, t) in &extra_tree {
            tree.insert((*p).to_owned(), (*t).to_owned());
        }
        for (p, t, _) in &files {
            tree.insert((*p).to_owned(), (*t).to_owned());
        }
        ChangeSet {
            merge_base: "base".to_owned(),
            head: "head".to_owned(),
            files: files
                .into_iter()
                .map(|(p, t, r)| ChangedFile {
                    path: p.to_owned(),
                    ranges: r,
                    content: t.to_owned(),
                })
                .collect(),
            tree,
        }
    }

    fn only(items: Vec<ChangedItem>) -> ChangedItem {
        assert_eq!(items.len(), 1, "{items:?}");
        items.into_iter().next().unwrap()
    }

    #[test]
    fn hunk_headers_give_new_side_ranges_and_a_deletion_keeps_its_line() {
        // context lines around the change are not the change
        let patch = "@@ -10,5 +12,6 @@ fn a()\n ctx\n ctx\n+x\n+y\n ctx\n ctx\n@@ -20 +25 @@\n-old\n+new\n@@ -30,4 +33,3 @@\n ctx\n-gone\n ctx\n ctx\n";
        assert_eq!(hunk_ranges(patch), vec![(14, 15), (25, 25), (34, 34)]);
    }

    #[test]
    fn the_scanner_locates_fns_inside_impls_and_macro_invocations_with_their_flow() {
        let items = scan_items(CONNECTOR_FILE);
        let by_name = |n: &str| {
            items
                .iter()
                .find(|i| i.name == n)
                .unwrap_or_else(|| panic!("{n} in {items:?}"))
        };
        let prereq = by_name("macros::create_all_prerequisites!");
        assert_eq!(prereq.kind, ItemKind::Macro);
        assert_eq!(prereq.flow, None, "a shared block belongs to every flow");
        let psync = by_name("macros::macro_connector_implementation!");
        assert_eq!(psync.flow.as_deref(), Some("PSync"));
        let get_url_in_macro = items
            .iter()
            .find(|i| i.name == "get_url" && i.start > psync.start && i.end < psync.end)
            .expect("get_url inside the macro invocation");
        assert!(get_url_in_macro.start < get_url_in_macro.end);
        let refund_impl = items
            .iter()
            .find(|i| i.kind == ItemKind::Impl)
            .expect("the impl");
        assert_eq!(refund_impl.flow.as_deref(), Some("Refund"));
        assert!(refund_impl
            .name
            .starts_with("impl ConnectorIntegrationV2<Refund"));
        let status = by_name("get_order_status");
        assert_eq!(status.kind, ItemKind::Fn);
        assert!(
            by_name("build_headers").end > by_name("build_headers").start,
            "a brace inside a comment does not unbalance"
        );
    }

    #[test]
    fn a_hunk_on_an_attribute_belongs_to_the_fn_below_and_its_span_counts_as_declared() {
        let items = scan_items(CORE_FILE);
        let attr_line = CORE_FILE
            .lines()
            .position(|l| l.contains("name = \"ucs::flow_orchestration\""))
            .unwrap()
            + 1;
        let (fn_item, _) = enclosing(&items, attr_line);
        let fn_item = fn_item.expect("attributed to the fn below");
        assert_eq!(fn_item.name, "process_authorization_internal");
        let declared = spans_declared(CORE_FILE, fn_item.start, fn_item.end);
        assert!(declared.contains("ucs::flow_orchestration"), "{declared:?}");
        assert!(declared.contains("art_canary::probe"), "{declared:?}");
    }

    #[test]
    fn a_change_to_a_connector_the_tape_never_ran_is_not_exercised() {
        let c = change(
            vec![(
                "crates/integrations/connector-integration/src/connectors/stripe/transformers.rs",
                "fn a() {\n 1\n}\n",
                vec![(2, 2)],
            )],
            vec![],
        );
        let item = only(classify(&c, &evidence(), &manifests()));
        assert_eq!(item.reach, Reach::NotExercised);
        assert!(item.why.contains("stripe never ran"), "{}", item.why);
    }

    #[test]
    fn a_change_to_a_flow_the_tape_never_ran_is_not_exercised_and_the_flows_that_ran_are_named() {
        let line = CONNECTOR_FILE
            .lines()
            .position(|l| l.contains("Ok(\"refund\""))
            .unwrap()
            + 1;
        let c = change(
            vec![(
                "crates/integrations/connector-integration/src/connectors/paypal.rs",
                CONNECTOR_FILE,
                vec![(line, line)],
            )],
            vec![],
        );
        let item = only(classify(&c, &evidence(), &manifests()));
        assert_eq!(item.reach, Reach::NotExercised);
        assert_eq!(item.flow.as_deref(), Some("Refund"));
        assert!(
            item.why
                .contains("never its Refund flow (ran: authorize, psync)"),
            "{}",
            item.why
        );
    }

    #[test]
    fn a_change_inside_a_flow_that_ran_is_flow_ran_with_the_request_count() {
        let line = CONNECTOR_FILE
            .lines()
            .position(|l| l.contains("v2/checkout/orders"))
            .unwrap()
            + 1;
        let c = change(
            vec![(
                "crates/integrations/connector-integration/src/connectors/paypal.rs",
                CONNECTOR_FILE,
                vec![(line, line)],
            )],
            vec![],
        );
        let item = only(classify(&c, &evidence(), &manifests()));
        assert_eq!(item.reach, Reach::FlowRanWeak, "{item:?}");
        assert!(item.item.ends_with("/ get_url"), "{}", item.item);
        assert!(item.why.contains("PSync ran in 2 of 4"), "{}", item.why);
    }

    #[test]
    fn a_transformers_fn_with_no_probe_reports_the_connector_ran_but_stays_unproven() {
        let line = CONNECTOR_FILE
            .lines()
            .position(|l| l.contains("Status::Completed =>"))
            .unwrap()
            + 1;
        let c = change(
            vec![(
                "crates/integrations/connector-integration/src/connectors/paypal/transformers.rs",
                CONNECTOR_FILE,
                vec![(line, line)],
            )],
            vec![],
        );
        let item = only(classify(&c, &evidence(), &manifests()));
        assert_eq!(item.reach, Reach::ModuleRan);
        assert!(item.why.contains("authorize x1, psync x2"), "{}", item.why);
    }

    #[test]
    fn a_core_fn_whose_declared_span_ran_is_exercised() {
        let line = CORE_FILE
            .lines()
            .position(|l| l.contains("Ok(Resp::default())"))
            .unwrap()
            + 1;
        let c = change(
            vec![(
                "crates/grpc-server/grpc-server/src/server/payments.rs",
                CORE_FILE,
                vec![(line, line)],
            )],
            vec![],
        );
        let item = only(classify(&c, &evidence(), &manifests()));
        assert_eq!(item.reach, Reach::Exercised, "{item:?}");
        assert!(item.why.contains("ucs::flow_orchestration"), "{}", item.why);
    }

    #[test]
    fn a_core_fn_with_no_probe_in_a_module_that_ran_is_module_ran() {
        let line = CORE_FILE.lines().position(|l| l.trim() == "1").unwrap() + 1;
        let c = change(
            vec![(
                "crates/grpc-server/grpc-server/src/server/payments.rs",
                CORE_FILE,
                vec![(line, line)],
            )],
            vec![],
        );
        let item = only(classify(&c, &evidence(), &manifests()));
        assert_eq!(item.reach, Reach::ModuleRan, "{item:?}");
    }

    #[test]
    fn a_helper_with_no_span_is_judged_by_its_callers() {
        let helper = "pub fn get_amount_as_string(a: u64) -> String {\n    format!(\"{a}0\")\n}\n";
        let path = "crates/types-traits/domain_types/src/utils.rs";
        // its only caller is a connector that never ran
        let c = change(
            vec![(path, helper, vec![(2, 2)])],
            vec![(
                "crates/integrations/connector-integration/src/connectors/fiuu.rs",
                "fn x() { let s = amount.get_amount_as_string(); }\n",
            )],
        );
        let item = only(classify(&c, &evidence(), &manifests()));
        assert_eq!(item.reach, Reach::NotExercised, "{item:?}");
        assert!(item.why.contains("only callers are fiuu"), "{}", item.why);
        // a caller that ran keeps it unproven rather than not exercised
        let c = change(
            vec![(path, helper, vec![(2, 2)])],
            vec![(
                "crates/integrations/connector-integration/src/connectors/paypal/transformers.rs",
                "fn x() { amount.get_amount_as_string() }\n",
            )],
        );
        let item = only(classify(&c, &evidence(), &manifests()));
        assert_eq!(item.reach, Reach::ModuleRan, "{item:?}");
        assert!(item.why.contains("called from paypal"), "{}", item.why);
    }

    #[test]
    fn a_boundary_call_site_inside_the_fn_is_exercised() {
        let service = "pub async fn execute_connector_processing_step() {\n    a();\n    b();\n}\n";
        let c = change(
            vec![(
                "crates/common/external-services/src/service.rs",
                service,
                vec![(2, 2)],
            )],
            vec![],
        );
        // the ledger names service.rs:1377, outside this tiny file's fn — so no
        // site; the span by the fn's own name ran instead
        let item = only(classify(&c, &evidence(), &manifests()));
        assert_eq!(item.reach, Reach::Exercised, "{item:?}");
        assert!(item.why.contains("execute_connector_processing_step"));
    }

    #[test]
    fn the_caveats_count_never_ran_and_unproven_apart() {
        let c = change(
            vec![
                (
                    "crates/integrations/connector-integration/src/connectors/stripe.rs",
                    "fn a() {\n 1\n}\n",
                    vec![(2, 2)],
                ),
                (
                    "crates/grpc-server/grpc-server/src/server/payments.rs",
                    CORE_FILE,
                    vec![(
                        CORE_FILE.lines().position(|l| l.trim() == "1").unwrap() + 1,
                        0,
                    )],
                ),
            ],
            vec![],
        );
        let cov = assess("prism", "juspay/hyperswitch-prism", "main", c, &evidence());
        assert_eq!((cov.never_ran, cov.unproven), (1, 1));
        assert_eq!(cov.caveats.len(), 2, "{:?}", cov.caveats);
        assert!(cov.caveats[0].contains("1 of 2 changed item(s) never ran"));
        assert!(cov.caveats[1].contains("unproven at span granularity"));
        assert_eq!(cov.driven_requests, 4);
    }

    #[test]
    fn layout_helpers_read_both_repositories() {
        assert_eq!(
            connector_of("crates/hyperswitch_connectors/src/connectors/adyen.rs").as_deref(),
            Some("adyen")
        );
        assert_eq!(connector_of("crates/integrations/connector-integration/src/connectors/tsys_transit/transformers.rs").as_deref(), Some("tsys_transit"));
        assert_eq!(
            connector_of("crates/router/src/core/connectors_helper.rs"),
            None
        );
        assert_eq!(
            module_of(
                &manifests(),
                "crates/grpc-server/grpc-server/src/server/payments.rs"
            )
            .as_deref(),
            Some("grpc_server::server::payments")
        );
        assert_eq!(
            module_of(&manifests(), "crates/common/external-services/src/lib.rs").as_deref(),
            Some("external_services")
        );
    }

    #[test]
    fn a_codeload_tarball_yields_sources_and_manifests_without_the_top_segment() {
        let mut tar_bytes = Vec::new();
        {
            let mut builder = tar::Builder::new(&mut tar_bytes);
            for (path, body) in [
                ("repo-abc/crates/x/src/lib.rs", "fn a() {}\n"),
                ("repo-abc/crates/x/Cargo.toml", "[package]\nname = \"x\"\n"),
                ("repo-abc/README.md", "hi\n"),
            ] {
                let mut header = tar::Header::new_gnu();
                header.set_size(body.len() as u64);
                header.set_mode(0o644);
                header.set_cksum();
                builder
                    .append_data(&mut header, path, body.as_bytes())
                    .unwrap();
            }
            builder.finish().unwrap();
        }
        let mut gz = Vec::new();
        {
            let mut enc = flate2::write::GzEncoder::new(&mut gz, flate2::Compression::fast());
            std::io::Write::write_all(&mut enc, &tar_bytes).unwrap();
            enc.finish().unwrap();
        }
        let tree = rust_tree_from_targz(std::io::Cursor::new(gz)).unwrap();
        assert_eq!(
            tree.keys().cloned().collect::<Vec<_>>(),
            vec!["crates/x/Cargo.toml", "crates/x/src/lib.rs"]
        );
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod net_tests {
    //! Against the real git host and a real run's artifacts. Skipped unless
    //! `DEJA_NET_TESTS=1`; `DEJA_CC_GRAPH` and `DEJA_CC_CALLS` name a run's
    //! `/graph` and `/calls` responses saved to disk, `DEJA_CC_HEAD` the
    //! candidate sha.
    use super::*;

    #[test]
    fn a_real_candidate_against_a_real_run() {
        if std::env::var("DEJA_NET_TESTS").ok().as_deref() != Some("1") {
            eprintln!("skipping: set DEJA_NET_TESTS=1 to run against the git host");
            return;
        }
        let graph: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(std::env::var("DEJA_CC_GRAPH").unwrap()).unwrap(),
        )
        .unwrap();
        let replay: Vec<deja_core::ExecutionGraphNode> =
            serde_json::from_value(graph["replay"].clone()).unwrap();
        let calls: Vec<serde_json::Value> = serde_json::from_str(
            &std::fs::read_to_string(std::env::var("DEJA_CC_CALLS").unwrap()).unwrap(),
        )
        .unwrap();
        let ev = Evidence::from_graph_and_calls(&replay, &calls);
        let head = std::env::var("DEJA_CC_HEAD").unwrap();
        let repo =
            std::env::var("DEJA_CC_REPO").unwrap_or_else(|_| "juspay/hyperswitch-prism".to_owned());
        let change = fetch_change_set(
            &repo,
            "main",
            &head,
            "https://codeload.github.com/{repo}/tar.gz/{sha}",
        )
        .unwrap();
        let cov = assess("prism", &repo, "main", change, &ev);
        println!("{}", serde_json::to_string_pretty(&cov).unwrap());
        assert!(!cov.items.is_empty());
    }
}
