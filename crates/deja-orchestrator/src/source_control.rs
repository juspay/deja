//! Read-only source identity. A captured PR head is not a live PR head, and an
//! image digest or arbitrary image tag is not a Git commit.

use crate::{CandidateSpec, GithubRef};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Read;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

pub fn valid_repo(repo: &str) -> bool {
    let parts: Vec<_> = repo.split('/').collect();
    parts.len() == 2
        && parts.iter().all(|part| {
            !part.is_empty()
                && !matches!(*part, "." | "..")
                && part.len() <= 100
                && part
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        })
}

fn commit_id(value: &str) -> bool {
    (7..=40).contains(&value.len()) && value.bytes().all(|b| b.is_ascii_hexdigit())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRevision {
    pub sha: String,
    /// Declared identity consistency, not an attestation of image contents.
    pub evidence: String,
}

/// Source declarations must name the same repository before a commit or a
/// live PR head is used as evidence. Equal SHAs in unrelated repositories do
/// not establish that the candidate is the pull request being reviewed.
pub fn validate_source_repository(
    repository: &str,
    candidate: &CandidateSpec,
    github: Option<&GithubRef>,
) -> Result<(), String> {
    if !valid_repo(repository) {
        return Err("the source repository is not a valid owner/name identity".into());
    }
    if let Some(github) = github {
        if !valid_repo(&github.repo) || !repository.eq_ignore_ascii_case(&github.repo) {
            return Err(
                "the source repository disagrees with the captured pull request repository".into(),
            );
        }
    }
    if let CandidateSpec::RepoSha { repo, .. }
    | CandidateSpec::RepoBranch { repo, .. }
    | CandidateSpec::RepoPr { repo, .. } = candidate
    {
        if !valid_repo(repo) || !repository.eq_ignore_ascii_case(repo) {
            return Err("the source repository disagrees with the candidate repository".into());
        }
    }
    Ok(())
}

pub fn candidate_source_revision(
    candidate: &CandidateSpec,
    github: Option<&GithubRef>,
) -> Result<SourceRevision, String> {
    if let Some(github) = github {
        validate_source_repository(&github.repo, candidate, Some(github))?;
    }
    let declared = match candidate {
        CandidateSpec::RepoSha { sha, .. } => sha.as_str(),
        CandidateSpec::PrebuiltImage { image } => {
            if image.contains('@') {
                return Err("the image is pinned by a registry digest, which does not identify its source commit".into());
            }
            let tag = image
                .rsplit_once(':')
                .filter(|(_, tail)| !tail.contains('/'))
                .map(|(_, tag)| tag)
                .unwrap_or(image);
            // Build profiles belong to CI, not this reader. A delimited Git
            // prefix is the identity token regardless of the profile suffix.
            let end = tag
                .find(|c: char| !c.is_ascii_hexdigit())
                .unwrap_or(tag.len());
            if end < tag.len() && !matches!(tag.as_bytes()[end], b'-' | b'_') {
                return Err("the image tag does not declare a delimited source commit".into());
            }
            &tag[..end]
        }
        _ => return Err("the candidate does not declare an immutable source commit".into()),
    };
    if !commit_id(declared) {
        return Err("the candidate's image tag or source ref is not a Git commit; an arbitrary image label cannot be used as a Git ref".into());
    }
    if let Some(gh) = github {
        if !commit_id(&gh.head_sha)
            || !(gh
                .head_sha
                .to_ascii_lowercase()
                .starts_with(&declared.to_ascii_lowercase())
                || declared
                    .to_ascii_lowercase()
                    .starts_with(&gh.head_sha.to_ascii_lowercase()))
        {
            return Err(
                "the captured PR head does not agree with the candidate's declared source commit"
                    .into(),
            );
        }
        let sha = if gh.head_sha.len() >= declared.len() {
            gh.head_sha.clone()
        } else {
            declared.to_owned()
        };
        return Ok(SourceRevision { sha, evidence: "captured PR head agrees with the declared candidate source; image contents are not attested".into() });
    }
    Ok(SourceRevision {
        sha: declared.into(),
        evidence: "candidate declares a source commit; image contents are not attested".into(),
    })
}

pub(crate) fn github_api_base() -> String {
    std::env::var("DEJA_GITHUB_API_URL")
        .ok()
        .map(|s| s.trim().trim_end_matches('/').to_owned())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "https://api.github.com".into())
}

const MAX_ACTIVE_GITHUB_READS: usize = 4;
const MAX_GITHUB_JSON_BYTES: usize = 1024 * 1024;
const GITHUB_CAPACITY_UNAVAILABLE: &str = "GitHub read capacity is busy; retry shortly";
static ACTIVE_GITHUB_READS: AtomicUsize = AtomicUsize::new(0);

/// A cold cache must not let dashboard traffic queue unbounded outbound work.
/// Hold the permit through reading and decoding; every return releases it.
struct GithubReadPermit<'a>(&'a AtomicUsize);

impl<'a> GithubReadPermit<'a> {
    fn try_acquire(active: &'a AtomicUsize, limit: usize) -> Option<Self> {
        active
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < limit).then_some(count + 1)
            })
            .ok()
            .map(|_| Self(active))
    }
}

impl Drop for GithubReadPermit<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

fn bounded_github_json<T: DeserializeOwned>(reader: impl Read) -> Result<T, String> {
    let mut bytes = Vec::new();
    reader
        .take((MAX_GITHUB_JSON_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|_| "GitHub response could not be read".to_owned())?;
    if bytes.len() > MAX_GITHUB_JSON_BYTES {
        return Err("GitHub JSON response exceeds the 1 MiB inspection limit".into());
    }
    serde_json::from_slice(&bytes)
        .map_err(|_| "GitHub response did not contain the expected JSON".to_owned())
}

pub(crate) fn github_get<T: DeserializeOwned>(path: &str) -> Result<T, String> {
    let _permit = GithubReadPermit::try_acquire(&ACTIVE_GITHUB_READS, MAX_ACTIVE_GITHUB_READS)
        .ok_or_else(|| GITHUB_CAPACITY_UNAVAILABLE.to_owned())?;
    let mut request = crate::codebundle::tarball_agent()
        .get(&format!("{}{path}", github_api_base()))
        .timeout(Duration::from_secs(5))
        .set("Accept", "application/vnd.github+json")
        .set("User-Agent", "deja-orchestrator");
    if let Ok(token) = std::env::var("DEJA_GITHUB_TOKEN") {
        let token = token.trim();
        if !token.is_empty() {
            request = request.set("Authorization", &format!("Bearer {token}"));
        }
    }
    let response = request.call().map_err(|error| match error {
        ureq::Error::Status(status, _) => format!("GitHub read failed (HTTP {status})"),
        // Avoid exposing deployment URLs, proxy credentials or response bodies.
        ureq::Error::Transport(_) => "GitHub read failed (transport error)".to_owned(),
    })?;
    bounded_github_json(response.into_reader())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum CurrentHead {
    Verified { sha: String, checked_at: String },
    Unavailable { reason: String, checked_at: String },
}

#[derive(Deserialize)]
struct PullRequest {
    head: Commit,
}
#[derive(Deserialize)]
struct Commit {
    sha: String,
}

type HeadCache = Mutex<HashMap<(String, String, u64), (Instant, CurrentHead)>>;
static HEAD_CACHE: OnceLock<HeadCache> = OnceLock::new();

/// Live verification is timestamped and cached briefly. A failed read remains
/// unknown; it never falls back to the head captured when a run was created.
pub fn current_pr_head(repo: &str, number: u64) -> CurrentHead {
    let key = (github_api_base(), repo.to_owned(), number);
    let cache = HEAD_CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Ok(guard) = cache.lock() {
        if let Some((at, value)) = guard.get(&key) {
            let ttl = if matches!(value, CurrentHead::Verified { .. }) {
                60
            } else {
                10
            };
            if at.elapsed() < Duration::from_secs(ttl) {
                return value.clone();
            }
        }
    }
    let response = if !valid_repo(repo) || number == 0 {
        Err("invalid GitHub repository or pull request number".into())
    } else {
        github_get::<PullRequest>(&format!("/repos/{repo}/pulls/{number}")).and_then(|p| {
            if p.head.sha.len() == 40 && commit_id(&p.head.sha) {
                Ok(p.head.sha)
            } else {
                Err("GitHub did not publish a full commit id".into())
            }
        })
    };
    let cacheable = !matches!(&response, Err(reason) if reason == GITHUB_CAPACITY_UNAVAILABLE);
    let checked_at = chrono::Utc::now().to_rfc3339();
    let value = match response {
        Ok(sha) => CurrentHead::Verified { sha, checked_at },
        Err(reason) => CurrentHead::Unavailable { reason, checked_at },
    };
    if !cacheable {
        // Another in-flight read may shortly populate a verified result. Do not
        // overwrite it or pin a transient local-capacity refusal for ten seconds.
        return value;
    }
    if let Ok(mut guard) = cache.lock() {
        // Bound process memory even if a reader traverses a large PR history.
        if guard.len() >= 256 {
            guard.retain(|_, (at, _)| at.elapsed() < Duration::from_secs(60));
        }
        if guard.len() >= 256 {
            guard.clear();
        }
        guard.insert(key, (Instant::now(), value.clone()));
    }
    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn github_read_permits_bound_concurrent_work_and_release_after_errors() {
        let active = AtomicUsize::new(0);
        let release = std::sync::Barrier::new(MAX_ACTIVE_GITHUB_READS + 1);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel();
        std::thread::scope(|scope| {
            for _ in 0..MAX_ACTIVE_GITHUB_READS {
                let ready = ready_tx.clone();
                let active = &active;
                let release = &release;
                scope.spawn(move || {
                    let _permit = GithubReadPermit::try_acquire(active, MAX_ACTIVE_GITHUB_READS)
                        .expect("available test read permit");
                    ready.send(()).expect("test receiver remains open");
                    release.wait();
                });
            }
            for _ in 0..MAX_ACTIVE_GITHUB_READS {
                ready_rx.recv().expect("worker holds its read permit");
            }
            assert!(GithubReadPermit::try_acquire(&active, MAX_ACTIVE_GITHUB_READS).is_none());
            release.wait();
        });
        assert_eq!(active.load(Ordering::Acquire), 0);
        let failed_read = || -> Result<serde_json::Value, String> {
            let _permit = GithubReadPermit::try_acquire(&active, MAX_ACTIVE_GITHUB_READS)
                .expect("released read permit");
            bounded_github_json(b"not JSON".as_slice())
        };
        assert!(failed_read().is_err());
        assert_eq!(active.load(Ordering::Acquire), 0);
        assert!(GithubReadPermit::try_acquire(&active, MAX_ACTIVE_GITHUB_READS).is_some());
    }

    #[test]
    fn github_json_body_is_bounded_before_deserialization() {
        let valid = format!("\"{}\"", "x".repeat(MAX_GITHUB_JSON_BYTES - 2));
        let value: String = bounded_github_json(valid.as_bytes()).expect("exact-limit JSON");
        assert_eq!(value.len(), MAX_GITHUB_JSON_BYTES - 2);
        let mut source = std::io::Cursor::new(vec![b' '; MAX_GITHUB_JSON_BYTES * 2]);
        let error = bounded_github_json::<serde_json::Value>(&mut source)
            .expect_err("oversized JSON must be refused");
        assert!(error.contains("inspection limit"));
        assert_eq!(source.position(), (MAX_GITHUB_JSON_BYTES + 1) as u64);
        let error = bounded_github_json::<serde_json::Value>(b"private-response-text".as_slice())
            .expect_err("invalid JSON must be refused");
        assert!(!error.contains("private-response-text"));
    }

    #[test]
    fn source_revision_uses_corroborated_full_head_and_refuses_tags_digests_and_conflicts() {
        let head = "d7d54364dc43650fae1429c71b7d1b4193948477";
        let gh = GithubRef {
            repo: "example/repo".into(),
            pr_number: 42,
            head_sha: head.into(),
            change_id: None,
        };
        let image = |image: &str| CandidateSpec::PrebuiltImage {
            image: image.into(),
        };
        assert_eq!(
            candidate_source_revision(
                &image("registry:5000/repo:d7d54364dc-release-fast"),
                Some(&gh)
            )
            .map(|r| r.sha)
            .ok(),
            Some(head.into())
        );
        for tag in [
            "d7d54364dc-release-fast-art",
            "repo:d7d54364dc-release-art",
            "d7d54364dc-new-profile",
            "d7d54364dc_profile",
        ] {
            assert_eq!(
                candidate_source_revision(&image(tag), Some(&gh))
                    .map(|r| r.sha)
                    .ok(),
                Some(head.into()),
                "{tag}"
            );
        }
        for tag in [
            "latest",
            "release-2026.11",
            "repo@sha256:0123456789abcdef",
            "d7d54364dc.special",
            "aaaaaaaaaa-dev",
        ] {
            assert!(
                candidate_source_revision(&image(tag), Some(&gh)).is_err(),
                "{tag}"
            );
        }
        assert!(candidate_source_revision(
            &CandidateSpec::RepoBranch {
                repo: gh.repo.clone(),
                branch: "main".into()
            },
            Some(&gh)
        )
        .is_err());
        assert!(candidate_source_revision(&image("d7d54364dc"), None).is_ok());
    }

    #[test]
    fn repository_names_cannot_change_request_origin_or_path() {
        assert!(valid_repo("example/my.repo-name"));
        for invalid in [
            "https://evil.test",
            "owner/../other",
            "owner/repo?secret",
            "owner/repo#fragment",
            "../repo",
            "owner/repo%2fother",
            "owner/repo\\other",
        ] {
            assert!(!valid_repo(invalid));
        }
    }

    #[test]
    fn matching_commits_cannot_hide_conflicting_repositories() {
        let github = GithubRef {
            repo: "example/service".into(),
            pr_number: 1,
            head_sha: "a".repeat(40),
            change_id: None,
        };
        let candidate = CandidateSpec::RepoSha {
            repo: "another/service".into(),
            sha: github.head_sha.clone(),
        };
        assert!(candidate_source_revision(&candidate, Some(&github)).is_err());
        let matching_candidate = CandidateSpec::RepoSha {
            repo: github.repo.clone(),
            sha: github.head_sha.clone(),
        };
        assert!(candidate_source_revision(&matching_candidate, Some(&github)).is_ok());
        let image = CandidateSpec::PrebuiltImage {
            image: "registry/service:aaaaaaaaaa-release-art".into(),
        };
        assert!(validate_source_repository("another/service", &image, Some(&github)).is_err());
        assert!(validate_source_repository("Example/Service", &image, Some(&github)).is_ok());
        assert!(
            validate_source_repository("https://example.test/service", &image, Some(&github))
                .is_err()
        );
    }
}
