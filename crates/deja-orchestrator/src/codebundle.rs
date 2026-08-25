//! CodeBundle: the candidate-ref-derived facts the frozen image lacks —
//! foremost the `migrations/` set at `sha_C`. This module computes the
//! candidate's expected schema fingerprint (the P1 gate's *expected* side) from
//! the candidate's own migrations, so it is a function of the candidate ref and
//! never a harness constant (closes the A1 resolution half, docs/design/
//! candidate-migration-resolution.md).
//!
//! Delivery is Option B (the ratified fork): the control plane produces the
//! bundle and stages it to S3 by sha; a Job initContainer pulls it. This module
//! owns the part that must be correct regardless of delivery — turning a set of
//! migration paths/dirs into a [`SchemaFingerprint`] — plus two producers for
//! the control plane:
//!   * git-backed (a local checkout at `sha_C`), for compose/dev; and
//!   * git-host-backed (fetch the repo tarball at `sha_C` from a codeload-style
//!     URL and keep only its `migrations/` subtree), the primary for in-cluster
//!     — migrations are then a pure function of `(repo_url, sha_C)` with no
//!     local checkout, no CI dependency, and no host/repo names in this module.
//!
//! Either producer emits the SAME canonical bundle (top-level `migrations/…`),
//! and the orchestrator (not the sealed replay pod) is the one with git-host
//! egress; the pod only ever pulls the staged bundle from S3.

use std::io;
use std::io::Read;
use std::path::Path;
use std::process::Command;

use crate::s3::S3Config;
use crate::SchemaFingerprint;

/// The S3 key a candidate's migration bundle lives at, keyed by sha (so it is
/// fetched once per sha, not per run).
pub fn bundle_s3_key(sha: &str) -> String {
    format!("codebundles/{sha}/migrations.tar")
}

/// The full `s3://bucket/key` URI the Job's migrations initContainer pulls the
/// candidate's bundle from. The executor injects this into that init per-run.
pub fn bundle_s3_uri(cfg: &S3Config, sha: &str) -> String {
    format!("s3://{}/{}", cfg.bucket, bundle_s3_key(sha))
}

/// Split an `s3://bucket/key` URI into `(bucket, key)`. The bucket the URI names
/// is authoritative for the object's location — it may differ from the ambient
/// `DEJA_S3_BUCKET` (e.g. a shared codebundle bucket).
pub fn parse_s3_uri(uri: &str) -> Result<(String, String), String> {
    let rest = uri
        .strip_prefix("s3://")
        .ok_or_else(|| format!("not an s3:// URI: {uri}"))?;
    let (bucket, key) = rest
        .split_once('/')
        .ok_or_else(|| format!("s3 URI has no key: {uri}"))?;
    if bucket.is_empty() || key.is_empty() {
        return Err(format!("s3 URI missing bucket or key: {uri}"));
    }
    Ok((bucket.to_owned(), key.to_owned()))
}

/// Extract an (uncompressed) tar's entries under `dest`, returning the count of
/// files actually unpacked. The `tar` crate's `unpack_in` refuses entries whose
/// path would escape `dest` (absolute paths, `..`), so a hostile bundle cannot
/// write outside the target; such entries are skipped (not counted), never a
/// silent overwrite elsewhere. A `git archive` tar's `pax_global_header` pseudo-
/// entry carries no path and is likewise skipped.
pub fn extract_tar_bytes(bytes: &[u8], dest: &Path) -> Result<usize, String> {
    std::fs::create_dir_all(dest).map_err(|e| format!("mkdir {}: {e}", dest.display()))?;
    let mut archive = tar::Archive::new(io::Cursor::new(bytes));
    let mut count = 0usize;
    for entry in archive.entries().map_err(|e| format!("read tar: {e}"))? {
        let mut entry = entry.map_err(|e| format!("tar entry: {e}"))?;
        if entry
            .unpack_in(dest)
            .map_err(|e| format!("unpack into {}: {e}", dest.display()))?
        {
            count += 1;
        }
    }
    Ok(count)
}

/// Fetch a candidate's CodeBundle tar from S3 and extract it under `dest` (so a
/// `migrations/` top-level lands at `<dest>/migrations`). This is the runner's
/// `stage-codebundle` step — the migrations initContainer runs it, then the
/// runner's migrate command applies the extracted migrations. The URI's bucket
/// overrides the ambient one (it may live in a shared bundle bucket); all other
/// S3 settings (endpoint, region, credentials/IRSA) come from the environment.
pub fn stage_bundle(uri: &str, dest: &Path) -> Result<usize, String> {
    let (bucket, key) = parse_s3_uri(uri)?;
    let mut cfg = S3Config::from_env();
    cfg.bucket = bucket;
    let bytes = deja_compactor::get_object_decoded(&cfg, &key)?;
    if bytes.is_empty() {
        return Err(format!("codebundle at {uri} is empty"));
    }
    extract_tar_bytes(&bytes, dest)
}

/// The diesel migration version for a directory name: everything before the
/// first `_`. `2022-09-29-084920_create_initial_tables` → `2022-09-29-084920`;
/// `00000000000000_diesel_initial_setup` → `00000000000000`. Diesel records
/// exactly this prefix in `__diesel_schema_migrations.version`, so a fingerprint
/// built from dir names compares directly against one read back from the store.
fn version_of(dir_name: &str) -> &str {
    dir_name.split_once('_').map(|(v, _)| v).unwrap_or(dir_name)
}

/// The migration-dir component of a repo-relative path: the segment right after
/// a `migrations` path segment (e.g. `migrations/2022-..._foo/up.sql` → the
/// `2022-..._foo`). `None` for a path that is not under `migrations/`.
fn migration_dir_of(path: &str) -> Option<&str> {
    let mut comps = path.split('/');
    while let Some(c) = comps.next() {
        if c == "migrations" {
            return comps.next().filter(|d| !d.is_empty());
        }
    }
    None
}

/// Build the expected fingerprint from a set of migration file paths (as emitted
/// by `git ls-tree -r --name-only <sha> -- migrations`). Paths not under
/// `migrations/` are ignored; the result is sorted + deduped.
pub fn fingerprint_from_migration_paths<I, S>(paths: I) -> SchemaFingerprint
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let versions = paths
        .into_iter()
        .filter_map(|p| migration_dir_of(p.as_ref()).map(|d| version_of(d).to_owned()))
        .collect::<Vec<_>>();
    SchemaFingerprint::new(versions)
}

/// Build the expected fingerprint by listing a staged `migrations/` directory's
/// immediate subdirectories. Used when the migrations are already on disk (a
/// checked-out repo, or a bundle an initContainer extracted).
pub fn fingerprint_from_migrations_dir(dir: &Path) -> io::Result<SchemaFingerprint> {
    let mut versions = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            if let Some(name) = entry.file_name().to_str() {
                versions.push(version_of(name).to_owned());
            }
        }
    }
    Ok(SchemaFingerprint::new(versions))
}

/// The control-plane producer: the candidate's expected migration set at
/// `sha_C`, read from a git checkout via `git ls-tree` (no working-tree
/// checkout needed — reads the tree object directly). This is the *independent*
/// expected side of the P1 gate: it comes from the candidate's source of truth,
/// not from whatever the runner happened to apply.
pub fn manifest_from_repo(repo_dir: &Path, sha: &str) -> Result<SchemaFingerprint, String> {
    let out = Command::new("git")
        .arg("-C")
        .arg(repo_dir)
        .args(["ls-tree", "-r", "--name-only", sha, "--", "migrations"])
        .output()
        .map_err(|e| format!("git ls-tree ({}): {e}", repo_dir.display()))?;
    if !out.status.success() {
        return Err(format!(
            "git ls-tree {sha} in {}: {}",
            repo_dir.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let stdout = String::from_utf8_lossy(&out.stdout);
    let fp = fingerprint_from_migration_paths(stdout.lines());
    if fp.count() == 0 {
        return Err(format!(
            "no migrations under 'migrations/' at {sha} in {} (empty manifest)",
            repo_dir.display()
        ));
    }
    Ok(fp)
}

/// The env var naming the candidate-ref files the bundle carries ALONGSIDE
/// `migrations/`, as repo-relative paths separated by commas or whitespace.
pub const CANDIDATE_CONFIG_FILES_ENV: &str = "DEJA_CANDIDATE_CONFIG_FILES";

/// The candidate-ref files the bundle carries alongside `migrations/` — the
/// system under test's own config at `sha_C`, whatever that system calls it and
/// wherever it keeps it.
///
/// This is DEPLOYMENT DATA, not a constant. Which files a candidate needs, and
/// where in its repo they live, is a fact about the system being replayed, and
/// baking it here would mean this crate had to be edited every time that system
/// rearranged its own directory — and would silently mean the wrong thing the
/// first time a second system was replayed. The deployment that mounts these
/// files into the Job already knows their names; it says them once, here.
///
/// Empty (the default) means the bundle carries migrations only. That is not
/// silent: the layering step in the Job then refuses by name, saying which path
/// it expected and listing what the bundle actually delivered.
///
/// Per-system extension, when `system_under_test` lands: read
/// `DEJA_<SYSTEM>_CANDIDATE_CONFIG_FILES` for a non-default system and fall
/// back to *nothing* rather than to this list — borrowing another system's
/// paths is the failure mode `config_source_for` already refuses to have. That
/// needs `RunSpec::system()` threaded to [`produce_tar`]; until then there is
/// one list, which is exactly as many systems as the bundle producer is
/// reached for today.
fn candidate_config_files() -> Vec<String> {
    candidate_config_files_from(std::env::var(CANDIDATE_CONFIG_FILES_ENV).ok().as_deref())
}

/// Parse the setting: comma- or whitespace-separated repo-relative paths.
/// Separated from the environment so it is testable, and so a malformed entry
/// (absolute, or climbing out of the repo) is dropped HERE rather than becoming
/// a `git archive` pathspec that means something unintended.
fn candidate_config_files_from(raw: Option<&str>) -> Vec<String> {
    raw.unwrap_or_default()
        .split([',', ' ', '\t', '\n'])
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .filter(|p| !p.starts_with('/') && !p.split('/').any(|seg| seg == ".."))
        .map(str::to_owned)
        .collect()
}

/// The candidate's `migrations/` tree at `sha` as a tar (git archive reads the
/// tree object directly — no working-tree checkout). This is the bundle the
/// Job initContainer pulls and extracts so the runner APPLIES the candidate's
/// migrations, not the harness's.
pub fn produce_tar(repo_dir: &Path, sha: &str) -> Result<Vec<u8>, String> {
    // The candidate's own per-environment configs ride alongside migrations, so
    // the Job's layering step can put this environment's one UNDER the recorded
    // config and reach the candidate's FULL (delta-complete) key set — config
    // structure is a function of the candidate, exactly like migrations. The
    // recorded VALUES stay authoritative for every key they set.
    // `git archive` fails on a pathspec that matches nothing, so each configured
    // file is included only when the ref actually has it.
    let configured = candidate_config_files();
    if configured.is_empty() {
        // Not a silent omission: the layering step in the Job refuses by name
        // when the file it was told to expect is not in the bundle, but that is
        // one pod away and this is where the decision was made.
        eprintln!(
            "codebundle: {CANDIDATE_CONFIG_FILES_ENV} is unset, so the bundle for {sha} carries \
             migrations only and no candidate config"
        );
    }
    let mut pathspecs: Vec<&str> = vec!["migrations"];
    for cfg_file in &configured {
        let blob = format!("{sha}:{cfg_file}");
        let present = Command::new("git")
            .arg("-C")
            .arg(repo_dir)
            .args(["cat-file", "-e", &blob])
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if present {
            pathspecs.push(cfg_file);
        }
    }
    let out = Command::new("git")
        .arg("-C")
        .arg(repo_dir)
        .args(["archive", "--format=tar", sha, "--"])
        .args(&pathspecs)
        .output()
        .map_err(|e| format!("git archive ({}): {e}", repo_dir.display()))?;
    if !out.status.success() {
        return Err(format!(
            "git archive {sha} in {}: {}",
            repo_dir.display(),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    if out.stdout.is_empty() {
        return Err(format!("git archive {sha} produced an empty tar"));
    }
    Ok(out.stdout)
}

/// Which of `configured` a bundle tar does NOT carry, in the order asked for.
///
/// Answered from bytes already in hand: the reuse path downloads the object
/// anyway to fingerprint its migrations, so this is one more pass over the same
/// buffer and no extra request.
pub fn configs_missing_from_bundle_tar(
    bytes: &[u8],
    configured: &[String],
) -> Result<Vec<String>, String> {
    let mut present = std::collections::BTreeSet::new();
    let mut archive = tar::Archive::new(io::Cursor::new(bytes));
    for entry in archive
        .entries()
        .map_err(|e| format!("read bundle tar: {e}"))?
    {
        let entry = entry.map_err(|e| format!("bundle tar entry: {e}"))?;
        if entry.header().entry_type() != tar::EntryType::Regular {
            continue;
        }
        present.insert(
            entry
                .path()
                .map_err(|e| format!("entry path: {e}"))?
                .to_string_lossy()
                .into_owned(),
        );
    }
    Ok(configured
        .iter()
        .filter(|want| !present.contains(*want))
        .cloned()
        .collect())
}

/// Where a run's bundle came from. The two cases are indistinguishable from the
/// outside and cost wildly different things to debug, so the caller says which.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BundleSource {
    /// An object already staged for this sha carried the migrations AND every
    /// file the deployment asked for, so it was reused untouched.
    Reused,
    /// Produced from the candidate ref and uploaded. `replaced_missing` names
    /// the configured files a previously staged object lacked; empty when there
    /// was no previous object at all.
    Staged { replaced_missing: Vec<String> },
}

impl BundleSource {
    /// The clause the run log appends, so "I fetched this" and "I found this
    /// and did nothing" stop reading identically.
    pub fn render(&self) -> String {
        match self {
            Self::Reused => "REUSED the bundle already staged for this sha (not re-fetched)".into(),
            Self::Staged { replaced_missing } if replaced_missing.is_empty() => {
                "STAGED a new bundle (none was present for this sha)".into()
            }
            Self::Staged { replaced_missing } => format!(
                "RE-STAGED: the bundle already present for this sha did not carry {replaced_missing:?}, \
                 which this deployment asks for via {CANDIDATE_CONFIG_FILES_ENV}"
            ),
        }
    }
}

/// Say so when a freshly produced bundle STILL lacks something the deployment
/// asked for.
///
/// This is reachable and worth a line: a configured path that does not exist at
/// the candidate ref makes every run re-stage (the reuse check keeps rejecting
/// the object, the producer keeps rebuilding it without the file) and then fail
/// at the layering step. That is correct — it fails loudly rather than booting
/// wrong — but without this line the repeated re-staging looks like a bug in
/// the cache rather than a path that is simply not in the repo.
fn warn_if_still_missing(tar: &[u8], configured: &[String], sha: &str) {
    match configs_missing_from_bundle_tar(tar, configured) {
        Ok(missing) if !missing.is_empty() => eprintln!(
            "codebundle: the bundle just produced for {sha} still does not carry {missing:?} — \
             those paths do not exist at that ref, so every run will re-stage and then refuse at \
             the config layering step. Fix {CANDIDATE_CONFIG_FILES_ENV} or the ref."
        ),
        _ => {}
    }
}

/// Can an object already staged for this sha be reused as-is?
///
/// `Reused` means exactly "do not fetch, do not put" — the caller returns it
/// untouched. Anything else is the list of files the deployment asked for that
/// the object does not carry, which is both the reason to re-stage and the text
/// the run log prints.
///
/// Separated from the S3 plumbing so the decision is testable on bytes: the
/// question that was being got wrong is not "does the object exist" but "does
/// it contain what was asked for", and that question is pure.
pub fn reuse_decision(bytes: &[u8], configured: &[String]) -> Result<BundleSource, String> {
    let missing = configs_missing_from_bundle_tar(bytes, configured)?;
    Ok(if missing.is_empty() {
        BundleSource::Reused
    } else {
        BundleSource::Staged {
            replaced_missing: missing,
        }
    })
}

/// Ensure the candidate's migration bundle is staged in S3 (idempotent by sha)
/// and return its manifest (the P1 gate's expected set). A bundle already
/// present for the sha is NOT re-uploaded — the fetch-once-per-sha cache the
/// Option B design specifies. The manifest is always computed from the repo (the
/// independent source of truth), whether or not the tar was (re)staged.
pub fn ensure_bundle_staged(
    cfg: &S3Config,
    repo_dir: &Path,
    sha: &str,
) -> Result<(SchemaFingerprint, BundleSource), String> {
    let manifest = manifest_from_repo(repo_dir, sha)?;
    let key = bundle_s3_key(sha);
    let mut replaced_missing = Vec::new();
    if deja_compactor::object_exists(cfg, &key)? {
        // Reusing an object means asserting it is usable, and "it exists" is a
        // weaker claim than "it carries what this deployment asked for". A
        // bundle staged before a file was added to the configured set has the
        // right migrations and the wrong contents; reusing it strands the run
        // on a missing base layer that nobody with S3 write access may be
        // around to clear.
        let bytes = deja_compactor::get_object_decoded(cfg, &key)?;
        match reuse_decision(&bytes, &candidate_config_files())? {
            BundleSource::Reused => return Ok((manifest, BundleSource::Reused)),
            BundleSource::Staged {
                replaced_missing: m,
            } => replaced_missing = m,
        }
    }
    let tar = produce_tar(repo_dir, sha)?;
    warn_if_still_missing(&tar, &candidate_config_files(), sha);
    deja_compactor::put_object(cfg, &key, tar)?;
    Ok((manifest, BundleSource::Staged { replaced_missing }))
}

// ── git-host producer (fetch the repo tarball at a ref, keep migrations/) ────

/// `<top>/migrations/<rest…>` → `migrations/<rest…>`, where `<top>` is a single
/// repo-root segment (a codeload tarball wraps everything in `{repo}-{sha}/`).
/// `None` for anything not directly under a root-level `migrations/`.
fn strip_to_root_migrations(path: &str) -> Option<String> {
    let idx = path.find("/migrations/")?;
    // The `migrations/` must sit at the repo root: the top segment before it
    // must itself contain no `/` (so `crates/x/migrations/…` is ignored).
    if path[..idx].contains('/') {
        return None;
    }
    Some(path[idx + 1..].to_owned())
}

/// `<top>/<repo-path>` → `<repo-path>` for each repo-relative path the
/// deployment configured (see [`candidate_config_files`]), carried alongside
/// `migrations/`. `<top>` must be a single repo-root segment (so
/// `crates/x/config/…` is ignored). `None` for anything else.
///
/// The list is a parameter rather than read from the environment here: the
/// entry points read the setting once, which keeps this pure and lets a test
/// state the list instead of mutating process-global env.
fn strip_to_root_config(path: &str, configured: &[String]) -> Option<String> {
    for cfg_file in configured {
        let needle = format!("/{cfg_file}");
        if let Some(idx) = path.find(&needle) {
            // Must sit at the repo root: the top segment before it has no '/'.
            if path[..idx].contains('/') {
                return None;
            }
            return Some(path[idx + 1..].to_owned());
        }
    }
    None
}

/// Build the canonical migration bundle from a gzipped repo tarball (the shape
/// a git host's codeload serves: every path wrapped in a single `{repo}-{sha}/`
/// top dir). Keeps root-level `migrations/` files plus each repo-relative path
/// in `configured`, rewrites their paths to the canonical form, and returns
/// `(bundle_tar, fingerprint)` (the fingerprint is migrations-only).
///
/// Streamed: the gzip is decoded on the fly and non-migration entries are
/// skipped without buffering, so only the (small) `migrations/` content is held
/// — not the whole repo. Separated from the network for testing.
pub fn bundle_migrations_from_targz<R: Read>(
    src: R,
    configured: &[String],
) -> Result<(Vec<u8>, SchemaFingerprint), String> {
    let gz = flate2::read::GzDecoder::new(src);
    let mut archive = tar::Archive::new(gz);
    let mut out = Vec::new();
    let mut versions = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut out);
        for entry in archive
            .entries()
            .map_err(|e| format!("read source tar: {e}"))?
        {
            let mut entry = entry.map_err(|e| format!("source tar entry: {e}"))?;
            // Only regular files — extraction recreates parent dirs.
            if entry.header().entry_type() != tar::EntryType::Regular {
                continue;
            }
            let path = entry
                .path()
                .map_err(|e| format!("entry path: {e}"))?
                .to_string_lossy()
                .into_owned();
            let rel = if let Some(m) = strip_to_root_migrations(&path) {
                if let Some(dir) = migration_dir_of(&m) {
                    versions.push(version_of(dir).to_owned());
                }
                m
            } else if let Some(c) = strip_to_root_config(&path, configured) {
                // The candidate's base config rides alongside migrations; it does
                // NOT contribute to the migration fingerprint.
                c
            } else {
                continue;
            };
            let mut data = Vec::new();
            entry
                .read_to_end(&mut data)
                .map_err(|e| format!("read {rel}: {e}"))?;
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, &rel, &data[..])
                .map_err(|e| format!("write bundle entry {rel}: {e}"))?;
        }
        builder
            .finish()
            .map_err(|e| format!("finish bundle tar: {e}"))?;
    }
    let fp = SchemaFingerprint::new(versions);
    if fp.count() == 0 {
        return Err("source tarball had no root-level migrations/ entries".to_owned());
    }
    Ok((out, fp))
}

/// A `ureq` agent that honors an outbound HTTP proxy for the codeload fetch.
///
/// The orchestrator's hosting environment may have no direct internet egress —
/// all outbound traffic goes through a forward proxy (e.g. squid) — and `ureq`
/// does NOT read proxy environment variables on its own, so a bare `ureq::get`
/// ignores the proxy and the connection times out. The proxy is read from
/// `DEJA_HTTP_PROXY` first — a DEDICATED var so it scopes to this one outbound
/// call and never redirects the in-cluster k8s API / S3 clients — then falls
/// back to the conventional `HTTPS_PROXY`/`HTTP_PROXY`. Unset → a direct agent,
/// so local/demo/CI keep working unchanged.
fn tarball_agent() -> ureq::Agent {
    let mut builder = ureq::AgentBuilder::new().timeout_connect(std::time::Duration::from_secs(15));
    let proxy = [
        "DEJA_HTTP_PROXY",
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
    ]
    .into_iter()
    .find_map(|k| std::env::var(k).ok())
    .map(|v| v.trim().to_owned())
    .filter(|v| !v.is_empty())
    .and_then(|v| ureq::Proxy::new(&v).ok());
    if let Some(proxy) = proxy {
        builder = builder.proxy(proxy);
    }
    builder.build()
}

/// Fetch a repo tarball from `url` (a codeload-style `…/tar.gz/<ref>`; the caller
/// substitutes the ref, so no host/repo/project name lives here) and build the
/// candidate's migration bundle from it. The orchestrator — never the sealed
/// replay pod — makes this outbound call, through the forward proxy when one is
/// configured (see [`tarball_agent`]).
pub fn bundle_from_tarball_url(url: &str) -> Result<(Vec<u8>, SchemaFingerprint), String> {
    let resp = tarball_agent()
        .get(url)
        .call()
        .map_err(|e| format!("fetch repo tarball {url}: {e}"))?;
    bundle_migrations_from_targz(resp.into_reader(), &candidate_config_files())
}

/// The candidate's expected migration set read back from an ALREADY-STAGED
/// bundle tar (canonical shape, uncompressed). Used to arm P1 from the S3 cache
/// without re-fetching the source tarball.
pub fn fingerprint_from_bundle_tar_bytes(bytes: &[u8]) -> Result<SchemaFingerprint, String> {
    let mut archive = tar::Archive::new(io::Cursor::new(bytes));
    let mut versions = Vec::new();
    for entry in archive
        .entries()
        .map_err(|e| format!("read bundle tar: {e}"))?
    {
        let entry = entry.map_err(|e| format!("bundle tar entry: {e}"))?;
        let path = entry
            .path()
            .map_err(|e| format!("entry path: {e}"))?
            .to_string_lossy()
            .into_owned();
        if let Some(dir) = migration_dir_of(&path) {
            versions.push(version_of(dir).to_owned());
        }
    }
    Ok(SchemaFingerprint::new(versions))
}

/// Ensure the candidate's migration bundle (staged from its ref's repo tarball)
/// is in S3 (idempotent by sha) and return its manifest. A bundle already
/// present is read back for its fingerprint — the source tarball is fetched only
/// on a cache miss. The git-host counterpart to [`ensure_bundle_staged`].
pub fn ensure_bundle_staged_from_url(
    cfg: &S3Config,
    url: &str,
    sha: &str,
) -> Result<(SchemaFingerprint, BundleSource), String> {
    let key = bundle_s3_key(sha);
    let mut replaced_missing = Vec::new();
    if deja_compactor::object_exists(cfg, &key)? {
        let bytes = deja_compactor::get_object_decoded(cfg, &key)?;
        let fp = fingerprint_from_bundle_tar_bytes(&bytes)?;
        if fp.count() == 0 {
            return Err(format!("staged bundle for {sha} has no migrations"));
        }
        // The object is already downloaded and already being inspected; ask it
        // the second question too. A bundle that does not carry the files this
        // deployment asked for is not one this deployment can use, so fall
        // through to the produce-and-put below rather than returning it.
        match reuse_decision(&bytes, &candidate_config_files())? {
            BundleSource::Reused => return Ok((fp, BundleSource::Reused)),
            BundleSource::Staged {
                replaced_missing: m,
            } => replaced_missing = m,
        }
    }
    let (tar, fp) = bundle_from_tarball_url(url)?;
    warn_if_still_missing(&tar, &candidate_config_files(), sha);
    deja_compactor::put_object(cfg, &key, tar)?;
    Ok((fp, BundleSource::Staged { replaced_missing }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_extraction_matches_diesel_recorded_prefix() {
        assert_eq!(
            version_of("2022-09-29-084920_create_initial_tables"),
            "2022-09-29-084920"
        );
        assert_eq!(
            version_of("00000000000000_diesel_initial_setup"),
            "00000000000000"
        );
        // a name with many underscores keeps only the timestamp prefix
        assert_eq!(
            version_of("2026-04-16-000001_remove_legacy_recon_permission_groups"),
            "2026-04-16-000001"
        );
    }

    #[test]
    fn paths_to_fingerprint_dedups_up_and_down() {
        let paths = [
            "migrations/2022-09-29-084920_create_initial_tables/up.sql",
            "migrations/2022-09-29-084920_create_initial_tables/down.sql",
            "migrations/00000000000000_diesel_initial_setup/up.sql",
            "README.md", // ignored — not under migrations/
            "src/lib.rs",
        ];
        let fp = fingerprint_from_migration_paths(paths);
        assert_eq!(fp.count(), 2);
        assert_eq!(
            fp.applied,
            vec!["00000000000000".to_string(), "20220929084920".to_string()]
        );
    }

    #[test]
    fn bundle_key_is_sha_scoped() {
        assert_eq!(
            bundle_s3_key("ff191d7f"),
            "codebundles/ff191d7f/migrations.tar"
        );
    }

    #[test]
    fn dir_walk_extracts_versions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mig = dir.path().join("migrations");
        std::fs::create_dir(&mig).expect("mkdir migrations");
        for name in [
            "2022-09-29-084920_create_initial_tables",
            "2026-04-16-000001_remove_legacy",
        ] {
            std::fs::create_dir(mig.join(name)).expect("mkdir migration");
            std::fs::write(mig.join(name).join("up.sql"), b"-- up").expect("write");
        }
        // a stray file, not a dir, is ignored
        std::fs::write(mig.join("notes.txt"), b"x").expect("write notes");
        let fp = fingerprint_from_migrations_dir(&mig).expect("walk");
        assert_eq!(fp.count(), 2);
        assert_eq!(fp.head(), Some("20260416000001"));
    }

    // The git-backed producer against a real temporary repo: manifest + tar.
    #[test]
    fn git_manifest_and_tar_from_a_real_repo() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo = dir.path();
        let git = |args: &[&str]| Command::new("git").arg("-C").arg(repo).args(args).output();
        let inited = git(&["init", "-q"])
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !inited {
            eprintln!("skipping: git unavailable");
            return;
        }
        let _ = git(&["config", "user.email", "t@example.com"]);
        let _ = git(&["config", "user.name", "deja-test"]);

        let mig = repo.join("migrations/2022-09-29-084920_create_initial_tables");
        std::fs::create_dir_all(&mig).expect("mkdir migration");
        std::fs::write(mig.join("up.sql"), b"-- up").expect("write up");
        let _ = git(&["add", "-A"]);
        let committed = git(&["-c", "commit.gpgsign=false", "commit", "-q", "-m", "init"])
            .map(|o| o.status.success())
            .unwrap_or(false);
        if !committed {
            eprintln!("skipping: git commit failed (identity/config)");
            return;
        }

        let fp = manifest_from_repo(repo, "HEAD").expect("manifest");
        assert_eq!(fp.applied, vec!["20220929084920".to_string()]);

        let tar = produce_tar(repo, "HEAD").expect("tar");
        assert!(!tar.is_empty());
        assert!(String::from_utf8_lossy(&tar)
            .contains("migrations/2022-09-29-084920_create_initial_tables"));

        // produce → extract → fingerprint: the git-archive tar (with its
        // pax_global_header) unpacks cleanly and the extracted migrations/ dir
        // fingerprints back to exactly the manifest — the full Option B loop.
        let out = tempfile::tempdir().expect("out tempdir");
        let n = extract_tar_bytes(&tar, out.path()).expect("extract git tar");
        assert!(n >= 1, "at least the up.sql should unpack");
        let extracted = fingerprint_from_migrations_dir(&out.path().join("migrations"))
            .expect("fingerprint extracted migrations");
        assert_eq!(extracted.applied, fp.applied);
    }

    #[test]
    fn parses_s3_uris_and_rejects_malformed() {
        assert_eq!(
            parse_s3_uri("s3://bundles/codebundles/ff191d7f/migrations.tar").expect("valid uri"),
            (
                "bundles".to_owned(),
                "codebundles/ff191d7f/migrations.tar".to_owned()
            )
        );
        assert!(parse_s3_uri("https://x/y").is_err(), "wrong scheme");
        assert!(parse_s3_uri("s3://bucket-only").is_err(), "no key");
        assert!(parse_s3_uri("s3:///key").is_err(), "empty bucket");
    }

    /// Build a gzipped tar shaped like a git host's codeload archive: every
    /// path wrapped in a single `{repo}-{sha}/` top dir.
    fn fake_codeload_targz(top: &str, files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut tar_bytes = Vec::new();
        {
            let mut b = tar::Builder::new(&mut tar_bytes);
            for (rel, body) in files {
                let mut hdr = tar::Header::new_gnu();
                hdr.set_size(body.len() as u64);
                hdr.set_mode(0o644);
                hdr.set_cksum();
                b.append_data(&mut hdr, format!("{top}/{rel}"), *body)
                    .expect("append");
            }
            b.finish().expect("finish tar");
        }
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        std::io::Write::write_all(&mut gz, &tar_bytes).expect("gzip");
        gz.finish().expect("finish gzip")
    }

    #[test]
    fn targz_producer_keeps_root_migrations_plus_config_and_rewrites_paths() {
        let targz = fake_codeload_targz(
            "hyperswitch-ff191d7f",
            &[
                (
                    "migrations/2022-09-29-084920_create_initial_tables/up.sql",
                    b"-- up",
                ),
                (
                    "migrations/2022-09-29-084920_create_initial_tables/down.sql",
                    b"-- down",
                ),
                (
                    "migrations/00000000000000_diesel_initial_setup/up.sql",
                    b"-- up",
                ),
                // Whatever the deployment configured — these paths are DATA
                // here, deliberately not names this crate knows.
                ("cfg/wanted-a.toml", b"[server]\nhost = \"0.0.0.0\"\n"),
                (
                    "cfg/nested/wanted-b.toml",
                    b"[server]\nhost = \"0.0.0.0\"\n",
                ),
                // Present in the repo but NOT configured: must not ride along.
                ("cfg/not-configured.toml", b"[server]\nhost = \"0.0.0.0\"\n"),
                // NOT under root migrations/ — must be ignored.
                ("crates/diesel_models/migrations/x/up.sql", b"-- nope"),
                // NOT root-level config — must be ignored.
                ("crates/x/cfg/wanted-a.toml", b"-- nope"),
                ("crates/x/cfg/nested/wanted-b.toml", b"-- nope"),
                ("README.md", b"readme"),
            ],
        );
        let configured = [
            "cfg/wanted-a.toml".to_owned(),
            "cfg/nested/wanted-b.toml".to_owned(),
        ];
        let (bundle, fp) = bundle_migrations_from_targz(&targz[..], &configured).expect("produce");
        // Two distinct versions, root-level only (the crates/ ones dropped). The
        // config file does NOT contribute to the migration fingerprint.
        assert_eq!(
            fp.applied,
            vec!["00000000000000".to_string(), "20220929084920".to_string()]
        );
        // The bundle is canonical: extracting it yields dest/migrations/… and
        // fingerprints back to the same set (produce → extract → fingerprint).
        let dest = tempfile::tempdir().expect("dest");
        extract_tar_bytes(&bundle, dest.path()).expect("extract bundle");
        let extracted =
            fingerprint_from_migrations_dir(&dest.path().join("migrations")).expect("fingerprint");
        assert_eq!(extracted.applied, fp.applied);
        // Each CONFIGURED path rode along at its canonical root path, and only
        // the root-level ones — the crates/ decoys were dropped.
        for want in &configured {
            assert!(
                dest.path().join(want).is_file(),
                "{want} was configured and must be carried in the bundle"
            );
        }
        // A config file the deployment did NOT name is not carried. Which files
        // matter is the deployment's call, not this crate's.
        assert!(
            !dest.path().join("cfg/not-configured.toml").exists(),
            "an unconfigured file must not ride along"
        );
        // And reading the bundle back directly (the S3-cache path) agrees.
        let cached = fingerprint_from_bundle_tar_bytes(&bundle).expect("cache fp");
        assert_eq!(cached.applied, fp.applied);
    }

    /// Which files matter in the candidate's repo is the DEPLOYMENT's call, so
    /// the setting is parsed as data — and a path that could mean something
    /// unintended as a `git archive` pathspec is dropped here rather than
    /// handed to git.
    #[test]
    fn the_configured_file_list_is_parsed_as_data_and_sanitised() {
        assert!(candidate_config_files_from(None).is_empty());
        assert!(candidate_config_files_from(Some("   ")).is_empty());
        assert_eq!(
            candidate_config_files_from(Some("a/one.toml, b/two.toml")),
            vec!["a/one.toml".to_owned(), "b/two.toml".to_owned()]
        );
        // Whitespace and newlines separate too, so the value can be written as a
        // YAML block in a chart without becoming one long path.
        assert_eq!(
            candidate_config_files_from(Some("a/one.toml\n  b/two.toml\t c/three.toml")),
            vec![
                "a/one.toml".to_owned(),
                "b/two.toml".to_owned(),
                "c/three.toml".to_owned()
            ]
        );
        // Escaping the repo is not expressible: an absolute path or a `..`
        // segment is dropped, not passed to git.
        assert_eq!(
            candidate_config_files_from(Some("/etc/passwd, ../../secrets.toml, ok/keep.toml")),
            vec!["ok/keep.toml".to_owned()]
        );
    }

    #[test]
    fn the_exact_rendered_chart_value_parses() {
        let rendered = "config/deployments/sandbox.toml, config/deployments/production.toml, config/development.toml, config/superposition_seed.toml";
        assert_eq!(
            candidate_config_files_from(Some(rendered)),
            vec![
                "config/deployments/sandbox.toml".to_owned(),
                "config/deployments/production.toml".to_owned(),
                "config/development.toml".to_owned(),
                "config/superposition_seed.toml".to_owned(),
            ]
        );
    }

    /// A real bundle, produced the way the producer produces one, carrying the
    /// files `configured` asks for.
    fn bundle_carrying(configured: &[String], extra: &[(&str, &[u8])]) -> Vec<u8> {
        let mut files: Vec<(&str, &[u8])> = vec![(
            "migrations/2022-09-29-084920_create_initial_tables/up.sql",
            b"-- up",
        )];
        let owned: Vec<String> = configured.to_vec();
        for c in &owned {
            files.push((c.as_str(), b"[server]\nhost = \"0.0.0.0\"\n"));
        }
        files.extend_from_slice(extra);
        let targz = fake_codeload_targz("repo-ff191d7f", &files);
        let (bundle, _) = bundle_migrations_from_targz(&targz[..], configured).expect("produce");
        bundle
    }

    /// An object that carries everything asked for is REUSED — the whole point
    /// of caching by sha. If this ever returns `Staged`, every run re-fetches
    /// and re-uploads a bundle that was already correct.
    #[test]
    fn a_bundle_carrying_everything_asked_for_is_reused() {
        let configured = vec!["cfg/a.toml".to_owned(), "cfg/nested/b.toml".to_owned()];
        let bundle = bundle_carrying(&configured, &[]);
        assert_eq!(
            reuse_decision(&bundle, &configured).expect("decide"),
            BundleSource::Reused
        );
        // And with nothing configured, any bundle is reusable — the check asks
        // only about what was actually asked for.
        assert_eq!(
            reuse_decision(&bundle, &[]).expect("decide"),
            BundleSource::Reused
        );
    }

    /// An object staged before a file joined the configured set has the right
    /// migrations and the wrong contents. "It exists" is the weaker claim;
    /// reusing it strands the run on a missing base layer that nobody without
    /// S3 write access can clear.
    #[test]
    fn a_bundle_missing_a_requested_config_re_stages_and_names_what_was_missing() {
        let had = vec!["cfg/a.toml".to_owned()];
        let bundle = bundle_carrying(&had, &[]);
        let now_wants = vec![
            "cfg/a.toml".to_owned(),
            "cfg/added-later.toml".to_owned(),
            "cfg/also-added.toml".to_owned(),
        ];
        assert_eq!(
            reuse_decision(&bundle, &now_wants).expect("decide"),
            BundleSource::Staged {
                replaced_missing: vec![
                    "cfg/added-later.toml".to_owned(),
                    "cfg/also-added.toml".to_owned()
                ]
            },
            "a bundle lacking a requested file must re-stage, naming exactly what it lacked"
        );
    }

    /// The migrations are present either way, so a check that only asked about
    /// migrations would call this bundle fine. That is the bug this closes.
    #[test]
    fn migrations_alone_do_not_make_a_bundle_reusable() {
        let bundle = bundle_carrying(&[], &[]);
        assert!(
            fingerprint_from_bundle_tar_bytes(&bundle)
                .expect("fp")
                .count()
                > 0,
            "fixture must have migrations, so only the config check can reject it"
        );
        assert_eq!(
            reuse_decision(&bundle, &["cfg/wanted.toml".to_owned()]).expect("decide"),
            BundleSource::Staged {
                replaced_missing: vec!["cfg/wanted.toml".to_owned()]
            }
        );
    }

    /// The run log has to distinguish work that happened from work that did
    /// not. All three cases previously printed the word "staged".
    #[test]
    fn the_run_log_says_which_it_did() {
        let reused = BundleSource::Reused.render();
        assert!(
            reused.contains("REUSED") && reused.contains("not re-fetched"),
            "{reused}"
        );

        let fresh = BundleSource::Staged {
            replaced_missing: Vec::new(),
        }
        .render();
        assert!(
            fresh.contains("STAGED") && !fresh.contains("RE-STAGED"),
            "{fresh}"
        );

        let restaged = BundleSource::Staged {
            replaced_missing: vec!["cfg/added-later.toml".to_owned()],
        }
        .render();
        assert!(restaged.contains("RE-STAGED"), "{restaged}");
        assert!(
            restaged.contains("cfg/added-later.toml"),
            "a re-stage must name what was missing: {restaged}"
        );
        assert!(
            restaged.contains(CANDIDATE_CONFIG_FILES_ENV),
            "and point at the setting that asked for it: {restaged}"
        );
        // The three are mutually distinguishable, which is the property that
        // failed before: they must not read the same.
        assert_ne!(reused, fresh);
        assert_ne!(fresh, restaged);
    }

    #[test]
    fn strip_to_root_migrations_only_matches_repo_root() {
        assert_eq!(
            strip_to_root_migrations("hyperswitch-abc/migrations/2022_x/up.sql"),
            Some("migrations/2022_x/up.sql".to_string())
        );
        // nested migrations dir (not repo root) → ignored
        assert_eq!(
            strip_to_root_migrations("hyperswitch-abc/crates/y/migrations/z/up.sql"),
            None
        );
        // no migrations segment
        assert_eq!(strip_to_root_migrations("hyperswitch-abc/src/lib.rs"), None);
    }

    // Opt-in real-network producer test: fetch the candidate ref's migrations
    // straight from the git host and prove the count. Skipped unless
    // DEJA_NET_TESTS=1 (keeps the suite offline/deterministic by default).
    #[test]
    fn targz_producer_against_real_codeload() {
        if std::env::var("DEJA_NET_TESTS").ok().as_deref() != Some("1") {
            eprintln!("skipping: set DEJA_NET_TESTS=1 to run the codeload fetch");
            return;
        }
        let url = "https://codeload.github.com/juspay/hyperswitch/tar.gz/ff191d7f79";
        let (bundle, fp) = bundle_from_tarball_url(url).expect("fetch + produce");
        assert!(
            fp.count() >= 461,
            "expected the candidate's full set, got {}",
            fp.count()
        );
        assert!(!bundle.is_empty());
        // canonical shape: extracts to migrations/…
        let dest = tempfile::tempdir().expect("dest");
        extract_tar_bytes(&bundle, dest.path()).expect("extract");
        assert!(dest.path().join("migrations").is_dir());
    }

    #[test]
    fn extract_tar_unpacks_migrations_under_dest() {
        // A tar built in-process with two migration files: extraction must place
        // them under dest/migrations and count exactly the files unpacked.
        let mut buf = Vec::new();
        {
            let mut b = tar::Builder::new(&mut buf);
            for name in [
                "migrations/2022-09-29-084920_create_initial_tables/up.sql",
                "migrations/2022-09-29-084920_create_initial_tables/down.sql",
            ] {
                let body = b"-- sql";
                let mut hdr = tar::Header::new_gnu();
                hdr.set_size(body.len() as u64);
                hdr.set_mode(0o644);
                hdr.set_cksum();
                b.append_data(&mut hdr, name, &body[..])
                    .expect("append migration file");
            }
            b.finish().expect("finish tar");
        }
        let dest = tempfile::tempdir().expect("dest");
        let n = extract_tar_bytes(&buf, dest.path()).expect("extract");
        assert_eq!(n, 2, "both files unpack");
        let fp =
            fingerprint_from_migrations_dir(&dest.path().join("migrations")).expect("fingerprint");
        assert_eq!(fp.applied, vec!["20220929084920".to_string()]);
        assert!(dest
            .path()
            .join("migrations/2022-09-29-084920_create_initial_tables/up.sql")
            .exists());
    }

    // Integration guard: the extraction must handle EVERY real hyperswitch
    // migration name, giving one version per directory. Skips if the vendored
    // tree is absent (a slim checkout).
    #[test]
    fn extracts_all_real_hyperswitch_migrations() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../vendor/hyperswitch-deja-clean/migrations");
        if !dir.exists() {
            eprintln!(
                "skipping: vendored migrations not present at {}",
                dir.display()
            );
            return;
        }
        let subdirs = std::fs::read_dir(&dir)
            .expect("read migrations")
            .filter_map(Result::ok)
            .filter(|e| e.file_type().map(|t| t.is_dir()).unwrap_or(false))
            .count();
        let fp = fingerprint_from_migrations_dir(&dir).expect("walk real migrations");
        // one unique version per directory — no collisions, no drops.
        assert_eq!(
            fp.count(),
            subdirs,
            "every migration dir must yield exactly one distinct version"
        );
        assert!(fp.count() >= 461, "expected the full recorded set");
    }
}
