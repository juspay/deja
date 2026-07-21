//! Executor selection + k8s executor configuration, read from the environment
//! ONCE at startup. This is the single canonical home for `DEJA_EXECUTOR` (the
//! name had drifted across design docs); code refers only to [`ExecutorKind`].

use super::env::CandidateBinding;
use super::launch::ExecutorError;
use crate::CandidateSpec;

/// Which executor drives a run. Local dev drives it in-process over docker
/// compose; in-cluster it is a k8s Job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutorKind {
    Compose,
    K8s,
}

impl ExecutorKind {
    /// Read `DEJA_EXECUTOR` (`compose` | `k8s`), defaulting to compose. An
    /// unknown value is an error, not a silent default — a typo must not
    /// silently run the wrong executor in production.
    pub fn from_env() -> Result<Self, ExecutorError> {
        match std::env::var("DEJA_EXECUTOR").ok().as_deref() {
            None | Some("") | Some("compose") => Ok(ExecutorKind::Compose),
            Some("k8s") => Ok(ExecutorKind::K8s),
            Some(other) => Err(ExecutorError::Template(format!(
                "DEJA_EXECUTOR='{other}' is not one of: compose, k8s"
            ))),
        }
    }
}

/// Coordinates the k8s executor needs, all from env (the env profile sets them).
/// The candidate binding carries the candidate's env-var names — data, so no
/// candidate specifics are compiled in.
#[derive(Debug, Clone)]
pub struct K8sExecutorConfig {
    /// Namespace the Job is created in (data plane).
    pub jobs_namespace: String,
    /// Where the Job template ConfigMap lives + its data key.
    pub template_namespace: String,
    pub template_configmap: String,
    pub template_key: String,
    /// Template container names.
    pub runner_container: String,
    /// The initContainer that pulls + extracts the candidate's CodeBundle
    /// (migrations) from S3, and the env var it reads the bundle URI from. The
    /// executor injects the per-run URI here (Option B). Names are config so no
    /// candidate/template specifics are compiled in.
    pub migrations_init_container: String,
    pub code_bundle_uri_env: String,
    pub candidate_binding: CandidateBinding,
}

impl K8sExecutorConfig {
    pub fn from_env() -> Self {
        let var = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_owned());
        let candidate_container = var("DEJA_CANDIDATE_CONTAINER", "candidate");
        K8sExecutorConfig {
            jobs_namespace: var("DEJA_JOBS_NAMESPACE", "replay-sbx"),
            template_namespace: var("DEJA_JOB_TEMPLATE_NAMESPACE", "replay-env"),
            template_configmap: var("DEJA_JOB_TEMPLATE_CONFIGMAP", "job-template"),
            template_key: var("DEJA_JOB_TEMPLATE_KEY", "job.json"),
            runner_container: var("DEJA_RUNNER_CONTAINER", "runner"),
            migrations_init_container: var("DEJA_MIGRATIONS_INIT_CONTAINER", "migrations"),
            code_bundle_uri_env: var("DEJA_CODE_BUNDLE_URI_ENV", "DEJA_CODE_BUNDLE_URI"),
            // Defaults are the Hyperswitch-router binding; a different candidate
            // overrides these. They are config defaults (a deployment concern),
            // not names baked into the patch/artifact logic.
            candidate_binding: CandidateBinding {
                container: candidate_container,
                mode_env: var("DEJA_CANDIDATE_MODE_ENV", "ROUTER__DEJA__MODE"),
                run_id_env: var("DEJA_CANDIDATE_RUN_ID_ENV", "ROUTER__DEJA__RUN_ID"),
                source_env: var("DEJA_CANDIDATE_SOURCE_ENV", "ROUTER__DEJA__REPLAY__SOURCE"),
                observed_env: var(
                    "DEJA_CANDIDATE_OBSERVED_ENV",
                    "ROUTER__DEJA__REPLAY__OBSERVED_SINK",
                ),
                code_sha_env: var(
                    "DEJA_CANDIDATE_CODE_SHA_ENV",
                    "ROUTER__DEJA__IDENTITY__CODE_SHA",
                ),
            },
        }
    }
}

/// Resolve a candidate spec to `(image, code_sha)` for the k8s executor. Today
/// only `PrebuiltImage` is launchable: CI builds an image tagged by git sha, so
/// the tag IS `sha_C`. The repo-ref variants need the image resolver (candidate
/// ref → CI image), which is not built yet — they error clearly rather than
/// guess. `LocalPath` is a compose-only mode.
pub fn resolve_candidate_image(spec: &CandidateSpec) -> Result<(String, String), ExecutorError> {
    match spec {
        CandidateSpec::PrebuiltImage { image } => {
            let sha = image_tag(image).to_owned();
            Ok((image.clone(), sha))
        }
        CandidateSpec::RepoSha { .. }
        | CandidateSpec::RepoBranch { .. }
        | CandidateSpec::RepoPr { .. } => Err(ExecutorError::Template(
            "k8s executor needs a prebuilt image (CI builds one tagged by sha); \
             the repo-ref → image resolver is not wired yet"
                .into(),
        )),
        CandidateSpec::LocalPath { .. } => Err(ExecutorError::Template(
            "local_path candidates run only under the compose executor".into(),
        )),
    }
}

/// The tag portion of an image ref (after the last `:`), skipping a `:port` in
/// the registry host. `repo/img@sha256:...` → the digest; `repo:5000/img:tag`
/// → `tag`; bare `img` → `latest`.
fn image_tag(image: &str) -> &str {
    if let Some((_, digest)) = image.split_once('@') {
        return digest;
    }
    match image.rsplit_once(':') {
        // A ':' that belongs to a registry :port has a '/' after it — not a tag.
        Some((_, tag)) if !tag.contains('/') => tag,
        _ => "latest",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_kind_defaults_to_compose_when_unset() {
        // DEJA_EXECUTOR is unset in the test env → compose default, no error.
        assert!(ExecutorKind::from_env().is_ok());
    }

    #[test]
    fn resolve_prebuilt_image_takes_tag_as_sha() {
        let (img, sha) = resolve_candidate_image(&CandidateSpec::PrebuiltImage {
            image: "ecr.io/hyperswitch:ff191d7f".into(),
        })
        .expect("prebuilt resolves");
        assert_eq!(img, "ecr.io/hyperswitch:ff191d7f");
        assert_eq!(sha, "ff191d7f");
    }

    #[test]
    fn resolve_digest_image_uses_digest_as_sha() {
        let (_, sha) = resolve_candidate_image(&CandidateSpec::PrebuiltImage {
            image: "ecr.io/hyperswitch@sha256:abcd".into(),
        })
        .expect("digest resolves");
        assert_eq!(sha, "sha256:abcd");
    }

    #[test]
    fn registry_port_is_not_mistaken_for_a_tag() {
        let (_, sha) = resolve_candidate_image(&CandidateSpec::PrebuiltImage {
            image: "registry:5000/hyperswitch".into(),
        })
        .expect("bare image resolves");
        assert_eq!(sha, "latest");
    }

    #[test]
    fn repo_refs_error_until_resolver_exists() {
        assert!(resolve_candidate_image(&CandidateSpec::RepoSha {
            repo: "juspay/hyperswitch".into(),
            sha: "ff191d7f".into(),
        })
        .is_err());
        assert!(resolve_candidate_image(&CandidateSpec::LocalPath {
            binary_or_source: "/x".into(),
        })
        .is_err());
    }
}
