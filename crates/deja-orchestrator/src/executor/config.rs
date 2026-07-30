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
    /// Where the candidate's config env is COPIED from: the recorded system's own
    /// rendered workload (artifact-only, its chart at the record version).
    pub config_source: ConfigSource,
    /// The candidate's on-disk config baseline for the record version.
    ///
    /// Together with `config_source` and `candidate_binding`, every name that
    /// belongs to the recorded system lives here as data. Replaying a different
    /// system — a different router, or a different service entirely — is a profile
    /// change, not a code change: nothing in this crate names one.
    pub config_file: ConfigFile,
}

/// The token a per-version name pattern uses for the run's record code-sha.
const RECORD_SHA_PLACEHOLDER: &str = "{record_sha}";

/// Expand a per-version name pattern for one run.
///
/// `None` means "no name": either the pattern is empty (the feature is switched
/// off) or it is version-keyed and this run has no record sha to key it — in which
/// case the caller must refuse rather than guess at another version's object.
fn expand_record_sha(pattern: &str, record_sha: Option<&str>) -> Option<String> {
    if pattern.is_empty() {
        return None;
    }
    if !pattern.contains(RECORD_SHA_PLACEHOLDER) {
        return Some(pattern.to_owned());
    }
    record_sha.map(|sha| pattern.replace(RECORD_SHA_PLACEHOLDER, sha))
}

/// Is a per-version name pattern version-keyed (enabled, and useless without a
/// record sha)? A run that cannot supply one must then be refused, not silently
/// launched without its recorded config.
fn is_version_keyed(pattern: &str) -> bool {
    !pattern.is_empty() && pattern.contains(RECORD_SHA_PLACEHOLDER)
}

/// Which rendered workload the candidate's config env is copied from. Both names
/// belong to the RECORDED SYSTEM (its own workload and container names), so they
/// are deployment profile data, never compiled in. An empty deployment name
/// disables the copy — the Job then boots with whatever env its template carries.
#[derive(Debug, Clone)]
pub struct ConfigSource {
    /// Workload name; may contain `{record_sha}` (expanded per run).
    pub deployment: String,
    /// Container within it whose `env` + `envFrom` are copied.
    pub container: String,
}

impl ConfigSource {
    /// The workload to read for this run — see [`expand_record_sha`].
    pub fn deployment_for(&self, record_sha: Option<&str>) -> Option<String> {
        expand_record_sha(&self.deployment, record_sha)
    }

    /// Version-keyed, so a run with no record sha must be refused.
    pub fn needs_record_sha(&self) -> bool {
        is_version_keyed(&self.deployment)
    }
}

/// The candidate's on-disk config baseline: the ConfigMap holding it, and the pod
/// volume that mounts it. Both belong to the RECORDED SYSTEM and its deployment
/// (the ConfigMap is named by whatever renders that system's config artifacts; the
/// volume is named by the Job template), so both are profile data. This service
/// names neither — it only substitutes the run's version into the pattern.
#[derive(Debug, Clone)]
pub struct ConfigFile {
    /// ConfigMap name; may contain `{record_sha}` (expanded per run). Empty =
    /// leave whatever ConfigMap the Job template references.
    pub config_map: String,
    /// The Job template's volume that mounts it, repointed at the above.
    pub volume: String,
}

impl ConfigFile {
    /// The ConfigMap to mount for this run — see [`expand_record_sha`].
    pub fn config_map_for(&self, record_sha: Option<&str>) -> Option<String> {
        expand_record_sha(&self.config_map, record_sha)
    }
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
            // Defaults describe the Hyperswitch sandbox render; another recorded
            // system sets its own. They are deployment defaults (a deployment
            // concern), not names baked into the copy/patch logic. An empty
            // deployment name disables copying.
            config_source: ConfigSource {
                deployment: var(
                    "DEJA_CONFIG_SOURCE_DEPLOYMENT",
                    "replay-{record_sha}-hyperswitch-server",
                ),
                container: var("DEJA_CONFIG_SOURCE_CONTAINER", "hyperswitch-router"),
            },
            config_file: ConfigFile {
                config_map: var(
                    "DEJA_CONFIG_FILE_CONFIGMAP",
                    "router-cm-replay-{record_sha}",
                ),
                volume: var("DEJA_CONFIG_FILE_VOLUME", "router-config"),
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

/// Derive the record code-sha from a recording router image ref, for selecting
/// that version's config artifacts from the version-keyed pool. The sha is the
/// image TAG (CI tags each build by its git sha), same as the candidate side. A
/// digest-pinned ref (`@sha256:…`) or an untagged/`latest` ref carries no git sha
/// we can map to the pool's `cus-<sha>` / `replay-<sha>` naming, so it yields None
/// and the Job keeps its template-default (currently-pooled) config artifacts.
pub fn record_sha_from_image(image: &str) -> Option<String> {
    let tag = image_tag(image);
    if tag == "latest" || tag.starts_with("sha256:") {
        return None;
    }
    Some(tag.to_owned())
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
    fn record_sha_from_tagged_recording_image() {
        assert_eq!(
            record_sha_from_image("ecr.io/hyperswitch-router:dcb9f9e955").as_deref(),
            Some("dcb9f9e955")
        );
        // a registry :port is not mistaken for the tag
        assert_eq!(
            record_sha_from_image("registry:5000/hyperswitch-router:ff191d7f").as_deref(),
            Some("ff191d7f")
        );
    }

    #[test]
    fn record_sha_is_none_without_a_usable_git_sha_tag() {
        // digest-pinned, untagged, and `latest` all carry no git sha → None → the
        // Job keeps its template-default config artifacts.
        assert_eq!(
            record_sha_from_image("ecr.io/hyperswitch@sha256:abcd"),
            None
        );
        assert_eq!(record_sha_from_image("ecr.io/hyperswitch:latest"), None);
        assert_eq!(record_sha_from_image("registry:5000/hyperswitch"), None);
    }

    #[test]
    fn config_source_expands_the_record_sha_placeholder() {
        let cs = ConfigSource {
            deployment: "replay-{record_sha}-some-system-server".into(),
            container: "some-system-server".into(),
        };
        assert_eq!(
            cs.deployment_for(Some("abc123")).as_deref(),
            Some("replay-abc123-some-system-server")
        );
        // a run with no record sha must NOT read some other version's workload
        assert_eq!(cs.deployment_for(None), None);
    }

    #[test]
    fn config_source_without_placeholder_is_version_independent() {
        let cs = ConfigSource {
            deployment: "one-fixed-render".into(),
            container: "c".into(),
        };
        assert_eq!(cs.deployment_for(None).as_deref(), Some("one-fixed-render"));
        assert_eq!(
            cs.deployment_for(Some("abc")).as_deref(),
            Some("one-fixed-render")
        );
    }

    #[test]
    fn empty_config_source_disables_copying() {
        let cs = ConfigSource {
            deployment: String::new(),
            container: "c".into(),
        };
        assert_eq!(cs.deployment_for(Some("abc")), None);
    }

    /// The config-file ConfigMap is named by a profile-supplied pattern, so this
    /// crate imposes no naming convention of its own — any recorded system's
    /// scheme works, including one with no version in the name at all.
    #[test]
    fn config_file_config_map_comes_from_the_configured_pattern() {
        let cf = ConfigFile {
            config_map: "any-scheme-{record_sha}-suffix".into(),
            volume: "some-volume".into(),
        };
        assert_eq!(
            cf.config_map_for(Some("abc123")).as_deref(),
            Some("any-scheme-abc123-suffix")
        );
        // version-keyed pattern + no record sha: no name, so the caller leaves the
        // template's own reference alone rather than guessing a version.
        assert_eq!(cf.config_map_for(None), None);

        let fixed = ConfigFile {
            config_map: "one-fixed-configmap".into(),
            volume: "some-volume".into(),
        };
        assert_eq!(
            fixed.config_map_for(None).as_deref(),
            Some("one-fixed-configmap")
        );

        let off = ConfigFile {
            config_map: String::new(),
            volume: "some-volume".into(),
        };
        assert_eq!(off.config_map_for(Some("abc123")), None);
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
