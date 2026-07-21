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

/// The candidate config files the replay Job boots the router against, carried in
/// the bundle alongside `migrations/` (config structure = f(candidate)):
///   - `config/docker_compose.toml`     — the router's self-sufficient base config
///     (passed as `-f`); the delta-complete key set a newer candidate may need.
///   - `config/superposition_seed.toml` — the offline Superposition fallback the
///     router reads when the live service is unreachable (always, in the sealed
///     replay pod).
/// The frozen image bakes neither at a usable path (only `payment_required_fields
/// _v2.toml`), so both ride the bundle rather than being copied into infra.
const CANDIDATE_CONFIG_FILES: [&str; 2] =
    ["config/docker_compose.toml", "config/superposition_seed.toml"];

/// The candidate's `migrations/` tree at `sha` as a tar (git archive reads the
/// tree object directly — no working-tree checkout). This is the bundle the
/// Job initContainer pulls and extracts so the runner APPLIES the candidate's
/// migrations, not the harness's.
pub fn produce_tar(repo_dir: &Path, sha: &str) -> Result<Vec<u8>, String> {
    // The candidate's self-sufficient config rides alongside migrations, so the
    // Job boots the router `-f` it and gets the candidate's FULL (delta-complete)
    // key set — config structure is a function of the candidate, exactly like
    // migrations; sbx-specific VALUES layer on as ROUTER__* env at run time.
    // `git archive` fails on a pathspec that matches nothing, so each config file
    // in CANDIDATE_CONFIG_FILES is included only when the ref actually has it.
    let mut pathspecs: Vec<&str> = vec!["migrations"];
    for cfg_file in CANDIDATE_CONFIG_FILES {
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

/// Ensure the candidate's migration bundle is staged in S3 (idempotent by sha)
/// and return its manifest (the P1 gate's expected set). A bundle already
/// present for the sha is NOT re-uploaded — the fetch-once-per-sha cache the
/// Option B design specifies. The manifest is always computed from the repo (the
/// independent source of truth), whether or not the tar was (re)staged.
pub fn ensure_bundle_staged(
    cfg: &S3Config,
    repo_dir: &Path,
    sha: &str,
) -> Result<SchemaFingerprint, String> {
    let manifest = manifest_from_repo(repo_dir, sha)?;
    let key = bundle_s3_key(sha);
    if deja_compactor::object_exists(cfg, &key)? {
        return Ok(manifest);
    }
    let tar = produce_tar(repo_dir, sha)?;
    deja_compactor::put_object(cfg, &key, tar)?;
    Ok(manifest)
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

/// `<top>/config/<f>` → `config/<f>` for each candidate config file in
/// [`CANDIDATE_CONFIG_FILES`] (the base router config and the offline Superposition
/// fallback), carried alongside `migrations/` (config structure = f(candidate);
/// see [`produce_tar`]). `<top>` must be a single repo-root segment (so
/// `crates/x/config/…` is ignored). `None` for anything else.
fn strip_to_root_config(path: &str) -> Option<String> {
    for cfg_file in CANDIDATE_CONFIG_FILES {
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
/// top dir). Keeps root-level `migrations/` files plus `config/docker_compose.toml`
/// (the candidate's base config), rewrites their paths to the canonical form, and
/// returns `(bundle_tar, fingerprint)` (the fingerprint is migrations-only).
///
/// Streamed: the gzip is decoded on the fly and non-migration entries are
/// skipped without buffering, so only the (small) `migrations/` content is held
/// — not the whole repo. Separated from the network for testing.
pub fn bundle_migrations_from_targz<R: Read>(
    src: R,
) -> Result<(Vec<u8>, SchemaFingerprint), String> {
    let gz = flate2::read::GzDecoder::new(src);
    let mut archive = tar::Archive::new(gz);
    let mut out = Vec::new();
    let mut versions = Vec::new();
    {
        let mut builder = tar::Builder::new(&mut out);
        for entry in archive.entries().map_err(|e| format!("read source tar: {e}"))? {
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
            } else if let Some(c) = strip_to_root_config(&path) {
                // The candidate's base config rides alongside migrations; it does
                // NOT contribute to the migration fingerprint.
                c
            } else {
                continue;
            };
            let mut data = Vec::new();
            entry.read_to_end(&mut data).map_err(|e| format!("read {rel}: {e}"))?;
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, &rel, &data[..])
                .map_err(|e| format!("write bundle entry {rel}: {e}"))?;
        }
        builder.finish().map_err(|e| format!("finish bundle tar: {e}"))?;
    }
    let fp = SchemaFingerprint::new(versions);
    if fp.count() == 0 {
        return Err("source tarball had no root-level migrations/ entries".to_owned());
    }
    Ok((out, fp))
}

/// Fetch a repo tarball from `url` (a codeload-style `…/tar.gz/<ref>`; the caller
/// substitutes the ref, so no host/repo/project name lives here) and build the
/// candidate's migration bundle from it. The orchestrator — never the sealed
/// replay pod — makes this outbound call.
pub fn bundle_from_tarball_url(url: &str) -> Result<(Vec<u8>, SchemaFingerprint), String> {
    let resp = ureq::get(url)
        .call()
        .map_err(|e| format!("fetch repo tarball {url}: {e}"))?;
    bundle_migrations_from_targz(resp.into_reader())
}

/// The candidate's expected migration set read back from an ALREADY-STAGED
/// bundle tar (canonical shape, uncompressed). Used to arm P1 from the S3 cache
/// without re-fetching the source tarball.
pub fn fingerprint_from_bundle_tar_bytes(bytes: &[u8]) -> Result<SchemaFingerprint, String> {
    let mut archive = tar::Archive::new(io::Cursor::new(bytes));
    let mut versions = Vec::new();
    for entry in archive.entries().map_err(|e| format!("read bundle tar: {e}"))? {
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
) -> Result<SchemaFingerprint, String> {
    let key = bundle_s3_key(sha);
    if deja_compactor::object_exists(cfg, &key)? {
        let bytes = deja_compactor::get_object_decoded(cfg, &key)?;
        let fp = fingerprint_from_bundle_tar_bytes(&bytes)?;
        if fp.count() == 0 {
            return Err(format!("staged bundle for {sha} has no migrations"));
        }
        return Ok(fp);
    }
    let (tar, fp) = bundle_from_tarball_url(url)?;
    deja_compactor::put_object(cfg, &key, tar)?;
    Ok(fp)
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
            vec![
                "00000000000000".to_string(),
                "2022-09-29-084920".to_string()
            ]
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
        assert_eq!(fp.head(), Some("2026-04-16-000001"));
    }

    // The git-backed producer against a real temporary repo: manifest + tar.
    #[test]
    fn git_manifest_and_tar_from_a_real_repo() {
        let dir = tempfile::tempdir().expect("tempdir");
        let repo = dir.path();
        let git = |args: &[&str]| {
            Command::new("git").arg("-C").arg(repo).args(args).output()
        };
        let inited = git(&["init", "-q"]).map(|o| o.status.success()).unwrap_or(false);
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
        assert_eq!(fp.applied, vec!["2022-09-29-084920".to_string()]);

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
            parse_s3_uri("s3://bundles/codebundles/ff191d7f/migrations.tar")
                .expect("valid uri"),
            ("bundles".to_owned(), "codebundles/ff191d7f/migrations.tar".to_owned())
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
                ("migrations/2022-09-29-084920_create_initial_tables/up.sql", b"-- up"),
                ("migrations/2022-09-29-084920_create_initial_tables/down.sql", b"-- down"),
                ("migrations/00000000000000_diesel_initial_setup/up.sql", b"-- up"),
                // The candidate's self-sufficient config — both files kept alongside migrations.
                ("config/docker_compose.toml", b"[server]\nhost = \"0.0.0.0\"\n"),
                ("config/superposition_seed.toml", b"[superposition]\nenabled = false\n"),
                // NOT under root migrations/ — must be ignored.
                ("crates/diesel_models/migrations/x/up.sql", b"-- nope"),
                // NOT root-level config — must be ignored.
                ("crates/x/config/docker_compose.toml", b"-- nope"),
                ("crates/x/config/superposition_seed.toml", b"-- nope"),
                ("README.md", b"readme"),
            ],
        );
        let (bundle, fp) = bundle_migrations_from_targz(&targz[..]).expect("produce");
        // Two distinct versions, root-level only (the crates/ ones dropped). The
        // config file does NOT contribute to the migration fingerprint.
        assert_eq!(
            fp.applied,
            vec![
                "00000000000000".to_string(),
                "2022-09-29-084920".to_string()
            ]
        );
        // The bundle is canonical: extracting it yields dest/migrations/… and
        // fingerprints back to the same set (produce → extract → fingerprint).
        let dest = tempfile::tempdir().expect("dest");
        extract_tar_bytes(&bundle, dest.path()).expect("extract bundle");
        let extracted = fingerprint_from_migrations_dir(&dest.path().join("migrations"))
            .expect("fingerprint");
        assert_eq!(extracted.applied, fp.applied);
        // The candidate's config rode along at the canonical root paths (so the Job
        // boots the router `-f config/docker_compose.toml` and points its offline
        // Superposition fallback at config/superposition_seed.toml), and only the
        // root-level ones — the crates/ decoys were dropped.
        assert!(
            dest.path().join("config/docker_compose.toml").is_file(),
            "config/docker_compose.toml must be carried in the bundle"
        );
        assert!(
            dest.path().join("config/superposition_seed.toml").is_file(),
            "config/superposition_seed.toml must be carried in the bundle"
        );
        // And reading the bundle back directly (the S3-cache path) agrees.
        let cached = fingerprint_from_bundle_tar_bytes(&bundle).expect("cache fp");
        assert_eq!(cached.applied, fp.applied);
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
        assert!(fp.count() >= 461, "expected the candidate's full set, got {}", fp.count());
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
        let fp = fingerprint_from_migrations_dir(&dest.path().join("migrations"))
            .expect("fingerprint");
        assert_eq!(fp.applied, vec!["2022-09-29-084920".to_string()]);
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
            eprintln!("skipping: vendored migrations not present at {}", dir.display());
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
