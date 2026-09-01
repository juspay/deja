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
    /// The initContainer that layers the candidate's own router config (from the
    /// same CodeBundle) UNDER the recorded one and writes the single file the
    /// candidate boots from. It reads the bundle URI from the SAME env var, and
    /// only to name the object in its error when the bundle lacks this
    /// environment's config — so a config failure says which S3 object failed to
    /// deliver it, not just which file was missing.
    pub config_init_container: String,
    pub code_bundle_uri_env: String,
    /// The Job's shared state mount — where the runner writes a run's artifacts
    /// and the candidate reads and writes them.
    ///
    /// This is NOT the control plane's own state directory. The orchestrator
    /// derives run paths under its own root to track a run; the pod has a
    /// different filesystem entirely, and pointing the candidate at a control
    /// plane path gives it something it cannot open. Must match the Job
    /// template's workspace `mountPath` and the runner's `HARNESS_STATE_DIR`.
    pub job_state_dir: String,
    /// How many trailing lines of each container's output the failure diagnostics
    /// keep. A failure is usually explained by the last few lines, but a seeding
    /// or migration failure is explained by a line emitted near the START of a
    /// long, chatty stage — a tail small enough to cut those off makes the run
    /// undiagnosable from its own record.
    pub diagnostics_tail_lines: u32,
    pub candidate_binding: CandidateBinding,
    /// Where the candidate's config env is COPIED from: the recorded system's own
    /// rendered workload (artifact-only, one current render).
    ///
    /// Together with `candidate_binding`, every name that belongs to the recorded
    /// system lives here as data. Replaying a different system — a different
    /// router, or a different service entirely — is a profile change, not a code
    /// change: nothing in this crate names one.
    pub config_source: ConfigSource,
}

/// Which rendered workload the candidate's config env is copied from. Both names
/// belong to the RECORDED SYSTEM (its own workload and container names), so they
/// are deployment profile data, never compiled in. An empty deployment name
/// disables the copy — the Job then boots with whatever env its template carries.
#[derive(Debug, Clone)]
pub struct ConfigSource {
    /// Workload name — one fixed render, the same for every run.
    pub deployment: String,
    /// Container within it whose `env` + `envFrom` are copied.
    pub container: String,
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
            config_init_container: var("DEJA_CONFIG_INIT_CONTAINER", "router-config"),
            code_bundle_uri_env: var("DEJA_CODE_BUNDLE_URI_ENV", "DEJA_CODE_BUNDLE_URI"),
            job_state_dir: var("DEJA_JOB_STATE_DIR", "/workspace/state"),
            diagnostics_tail_lines: var("DEJA_DIAGNOSTICS_TAIL_LINES", "2000")
                .parse()
                .unwrap_or(2000),
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
                    "replay-sbx-hyperswitch-server",
                ),
                container: var("DEJA_CONFIG_SOURCE_CONTAINER", "hyperswitch-router"),
            },
        }
    }

    /// The candidate env binding for a run's `system_under_test`. The default
    /// system keeps the base binding (`DEJA_CANDIDATE_*_ENV`, Hyperswitch
    /// defaults). Any other system is a PROFILE looked up from
    /// `DEJA_<SYSTEM>_CANDIDATE_{MODE,RUN_ID,SOURCE,OBSERVED,CODE_SHA}_ENV`,
    /// with shipped defaults for `prism` (the `CS__DEJA__*` keys
    /// hyperswitch-prism reads). Still data end to end: a new system needs env
    /// vars, not a recompile.
    pub fn candidate_binding_for(&self, system: &str) -> CandidateBinding {
        if crate::is_default_system(system) {
            return self.candidate_binding.clone();
        }
        let var = |suffix: &str, prism_default: &str| {
            let name = crate::system_env_var(system, &format!("CANDIDATE_{suffix}"));
            std::env::var(name).unwrap_or_else(|_| {
                if system == "prism" {
                    prism_default.to_owned()
                } else {
                    // Unconfigured non-prism system: fall back to the base
                    // binding's name for this slot so behavior is at worst the
                    // pre-profile behavior, never a silent empty var name.
                    match suffix {
                        "MODE_ENV" => self.candidate_binding.mode_env.clone(),
                        "RUN_ID_ENV" => self.candidate_binding.run_id_env.clone(),
                        "SOURCE_ENV" => self.candidate_binding.source_env.clone(),
                        "OBSERVED_ENV" => self.candidate_binding.observed_env.clone(),
                        _ => self.candidate_binding.code_sha_env.clone(),
                    }
                }
            })
        };
        CandidateBinding {
            container: self.candidate_binding.container.clone(),
            mode_env: var("MODE_ENV", "CS__DEJA__MODE"),
            run_id_env: var("RUN_ID_ENV", "CS__DEJA__RUN_ID"),
            source_env: var("SOURCE_ENV", "CS__DEJA__REPLAY__SOURCE"),
            observed_env: var("OBSERVED_ENV", "CS__DEJA__REPLAY__OBSERVED_SINK"),
            code_sha_env: var("CODE_SHA_ENV", "CS__DEJA__IDENTITY__CODE_SHA"),
        }
    }

    /// The Job template key for a run's `system_under_test`. The default system
    /// keeps the deployment's configured key (`DEJA_JOB_TEMPLATE_KEY`, itself
    /// defaulting to `job.json`). Another system reads
    /// `DEJA_<SYSTEM>_JOB_TEMPLATE_KEY`, falling back to the `job.<system>.json`
    /// convention the replay-env ConfigMap already ships alongside `job.json`.
    ///
    /// Deriving the name rather than requiring one is safe here because the
    /// consequence of getting it wrong is loud: `fetch_template` refuses by
    /// name, saying which ConfigMap and which key it looked for. A system whose
    /// template is named something else sets the variable; nothing has to be
    /// configured for a system that follows the convention.
    ///
    /// Note this is deliberately NOT the base key for a non-default system.
    /// Falling back to `job.json` would boot a prism run against the router's
    /// Job — migrations, a database and a config-compose init it does not have
    /// — and the pod would fail somewhere far from the cause.
    pub fn template_key_for(&self, system: &str) -> String {
        if crate::is_default_system(system) {
            return self.template_key.clone();
        }
        std::env::var(crate::system_env_var(system, "JOB_TEMPLATE_KEY"))
            .unwrap_or_else(|_| format!("job.{system}.json"))
    }

    /// The config-copy source for a run's `system_under_test`. Non-default
    /// systems read `DEJA_<SYSTEM>_CONFIG_SOURCE_{DEPLOYMENT,CONTAINER}`; unset
    /// means NO config copy (empty deployment) — booting a prism candidate off
    /// hyperswitch's rendered env would be wrong, so absent profile data
    /// disables the copy rather than borrowing the default system's.
    pub fn config_source_for(&self, system: &str) -> ConfigSource {
        if crate::is_default_system(system) {
            return self.config_source.clone();
        }
        ConfigSource {
            deployment: std::env::var(crate::system_env_var(system, "CONFIG_SOURCE_DEPLOYMENT"))
                .unwrap_or_default(),
            container: std::env::var(crate::system_env_var(system, "CONFIG_SOURCE_CONTAINER"))
                .unwrap_or_default(),
        }
    }
}

/// Resolve a candidate spec to `(image, code_sha)` for the k8s executor. Today
/// only `PrebuiltImage` is launchable: CI builds an image tagged by git sha, so
/// the tag IS `sha_C`. The repo-ref variants need the image resolver (candidate
/// ref → CI image), which is not built yet — they error clearly rather than
/// guess. `LocalPath` is a compose-only mode.
pub fn resolve_candidate_image(
    spec: &CandidateSpec,
    system: &str,
) -> Result<(String, String), ExecutorError> {
    match spec {
        CandidateSpec::PrebuiltImage { image } => {
            let image = qualify_candidate_image(image, system);
            let sha = build_ref_from_tag(image_tag(&image)).to_owned();
            Ok((image, sha))
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

/// Expand a bare candidate REF into a full image reference using the
/// deployment's registry, leaving an already-qualified reference alone.
///
/// Which registry holds the candidate's images belongs to the deployment, not
/// to a run: it is the same for every run against a given system, and a caller
/// retyping it is a caller who can get it wrong in a way nothing checks. So a
/// run names a build — a tag, a git sha — and this resolves where that build
/// lives, from `DEJA_CANDIDATE_IMAGE_REPO`.
///
/// Where a system's candidate images live. The default system reads
/// `DEJA_CANDIDATE_IMAGE_REPO`; another system reads
/// `DEJA_<SYSTEM>_CANDIDATE_IMAGE_REPO` and does NOT fall back to it.
///
/// The absent fallback is the point. Prism candidates live in their own
/// registry, so a prism sha qualified against the router's repo names an image
/// that cannot exist — a guaranteed pull failure that reads as a broken
/// candidate rather than as unconfigured deployment. Returning `None` instead
/// leaves the bare reference verbatim, and the pull then fails naming exactly
/// the image that was asked for. Same principle as `config_source_for`:
/// borrowing the default system's profile is worse than having none.
fn candidate_image_repo(system: &str) -> Option<String> {
    let name = if crate::is_default_system(system) {
        "DEJA_CANDIDATE_IMAGE_REPO".to_owned()
    } else {
        crate::system_env_var(system, "CANDIDATE_IMAGE_REPO")
    };
    std::env::var(name)
        .ok()
        .filter(|repo| !repo.trim().is_empty())
        .map(|repo| repo.trim().trim_end_matches('/').to_owned())
}

/// A reference containing `/` or `@`, or a `:` that is not the registry's port,
/// is taken as already qualified and used verbatim. That keeps a fork, another
/// registry, or a digest reachable without a config change — the convention is
/// the default, not the only option. With no repo configured a bare ref is also
/// left alone, so compose (where the "image" is a local tag) is unaffected.
fn qualify_candidate_image(reference: &str, system: &str) -> String {
    let reference = reference.trim();
    let already_qualified = reference.contains('/') || reference.contains('@');
    if already_qualified {
        return reference.to_owned();
    }
    let repo = match candidate_image_repo(system) {
        Some(repo) => repo,
        None => return reference.to_owned(),
    };
    match reference.split_once(':') {
        // `name:tag` with no registry: a caller restating the repo by its last
        // path segment (`hyperswitch-router:671034e3eb`). Prepending the repo
        // to THAT — as this function used to — minted a double-tag reference
        // (`…/hyperswitch-router:hyperswitch-router:671034e3eb`) that no
        // registry serves; the pod sat in ImagePullBackOff until the watch
        // deadline killed the run. When the name IS the configured repo's last
        // segment, the caller means the configured repo; a name that is not is
        // a different repo and is left verbatim rather than silently repointed
        // — the pull then fails naming exactly the image that was asked for.
        Some((name, tag)) if !name.is_empty() && !tag.is_empty() => {
            if repo.rsplit('/').next() == Some(name) {
                format!("{repo}:{tag}")
            } else {
                reference.to_owned()
            }
        }
        _ => format!("{repo}:{reference}"),
    }
}

/// The git ref that built a tag: a recognized build-profile suffix is dropped,
/// everything else is itself. The build pipelines tag non-release images
/// `<sha>-<profile>` so the bare tag in the registry always means the release
/// build — but the SOURCE of a fast build is the same commit, so everything
/// keyed by code identity (the codeload migrations fetch, the staged bundle,
/// `code_sha` on the candidate) wants the suffix gone. The image reference
/// itself keeps the full tag; only the ref derived from it is stripped.
fn build_ref_from_tag(tag: &str) -> &str {
    tag.strip_suffix("-release-fast")
        .or_else(|| tag.strip_suffix("-dev"))
        .unwrap_or(tag)
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

    /// These variables are process-global, so the template-key cases run under
    /// one test rather than as separate ones racing the same names.
    #[test]
    fn template_key_is_per_system_and_never_borrows_the_routers_job() {
        let cfg = K8sExecutorConfig::from_env();

        // A non-default system follows the convention the replay-env ConfigMap
        // already ships, and must NOT fall back to the router's Job.
        assert_eq!(cfg.template_key_for("prism"), "job.prism.json");
        assert_ne!(cfg.template_key_for("prism"), cfg.template_key);
        // The ConfigMap KEY keeps a hyphen; only the variable name folds it.
        assert_eq!(
            cfg.template_key_for("payment-core"),
            "job.payment-core.json"
        );

        // The default system keeps the deployment's own key, and is not
        // divertible by a profile variable — including one named for it.
        std::env::set_var("DEJA_HYPERSWITCH_JOB_TEMPLATE_KEY", "job.WRONG.json");
        assert_eq!(
            cfg.template_key_for(crate::DEFAULT_SYSTEM_UNDER_TEST),
            cfg.template_key,
            "the default system short-circuits before reading any profile var"
        );
        assert_eq!(cfg.template_key_for("hyperswitch"), "job.json");
        std::env::remove_var("DEJA_HYPERSWITCH_JOB_TEMPLATE_KEY");

        // An explicit profile key overrides the derived convention.
        std::env::set_var("DEJA_PRISM_JOB_TEMPLATE_KEY", "job.ucs.json");
        assert_eq!(cfg.template_key_for("prism"), "job.ucs.json");
        std::env::remove_var("DEJA_PRISM_JOB_TEMPLATE_KEY");
    }

    use super::*;

    #[test]
    fn executor_kind_defaults_to_compose_when_unset() {
        // DEJA_EXECUTOR is unset in the test env → compose default, no error.
        assert!(ExecutorKind::from_env().is_ok());
    }

    #[test]
    fn resolve_prebuilt_image_takes_tag_as_sha() {
        let (img, sha) = resolve_candidate_image(
            &CandidateSpec::PrebuiltImage {
                image: "ecr.io/hyperswitch:ff191d7f".into(),
            },
            crate::DEFAULT_SYSTEM_UNDER_TEST,
        )
        .expect("prebuilt resolves");
        assert_eq!(img, "ecr.io/hyperswitch:ff191d7f");
        assert_eq!(sha, "ff191d7f");
    }

    #[test]
    fn a_profile_suffix_is_stripped_from_the_ref_but_kept_on_the_image() {
        for (tag, want_ref) in [
            ("7cd937aa1c-release-fast", "7cd937aa1c"),
            ("7cd937aa1c-dev", "7cd937aa1c"),
            // Not a profile suffix — a hyphen alone must not truncate a tag.
            ("2026.08.01.0-hotfix", "2026.08.01.0-hotfix"),
        ] {
            let (img, sha) = resolve_candidate_image(
                &CandidateSpec::PrebuiltImage {
                    image: format!("ecr.io/hyperswitch:{tag}"),
                },
                crate::DEFAULT_SYSTEM_UNDER_TEST,
            )
            .expect("prebuilt resolves");
            assert_eq!(
                img,
                format!("ecr.io/hyperswitch:{tag}"),
                "image keeps the tag"
            );
            assert_eq!(sha, want_ref, "ref drops only a recognized profile suffix");
        }
    }

    #[test]
    fn resolve_digest_image_uses_digest_as_sha() {
        let (_, sha) = resolve_candidate_image(
            &CandidateSpec::PrebuiltImage {
                image: "ecr.io/hyperswitch@sha256:abcd".into(),
            },
            crate::DEFAULT_SYSTEM_UNDER_TEST,
        )
        .expect("digest resolves");
        assert_eq!(sha, "sha256:abcd");
    }

    #[test]
    fn registry_port_is_not_mistaken_for_a_tag() {
        let (_, sha) = resolve_candidate_image(
            &CandidateSpec::PrebuiltImage {
                image: "registry:5000/hyperswitch".into(),
            },
            crate::DEFAULT_SYSTEM_UNDER_TEST,
        )
        .expect("bare image resolves");
        assert_eq!(sha, "latest");
    }

    #[test]
    fn repo_refs_error_until_resolver_exists() {
        assert!(resolve_candidate_image(
            &CandidateSpec::RepoSha {
                repo: "juspay/hyperswitch".into(),
                sha: "ff191d7f".into(),
            },
            crate::DEFAULT_SYSTEM_UNDER_TEST
        )
        .is_err());
        assert!(resolve_candidate_image(
            &CandidateSpec::LocalPath {
                binary_or_source: "/x".into(),
            },
            crate::DEFAULT_SYSTEM_UNDER_TEST
        )
        .is_err());
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod candidate_reference_tests {
    use super::*;

    /// `DEJA_CANDIDATE_IMAGE_REPO` is process-global, so these run under one
    /// lock rather than as separate tests racing the same variable.
    #[test]
    fn a_bare_ref_resolves_against_the_deployment_registry() {
        const REPO: &str = "2236.dkr.ecr.ap-south-1.amazonaws.com/hyperswitch-router";
        std::env::set_var("DEJA_CANDIDATE_IMAGE_REPO", REPO);

        // The only part a run chooses is the build.
        assert_eq!(
            qualify_candidate_image("dcb9f9e955", crate::DEFAULT_SYSTEM_UNDER_TEST),
            format!("{REPO}:dcb9f9e955")
        );

        // An already-qualified reference is left alone, so another registry, a
        // fork, or a digest stays reachable without changing configuration.
        let elsewhere = "ghcr.io/someone/hyperswitch-router:abc123";
        assert_eq!(
            qualify_candidate_image(elsewhere, crate::DEFAULT_SYSTEM_UNDER_TEST),
            elsewhere
        );
        let digest = "2236.dkr.ecr.ap-south-1.amazonaws.com/router@sha256:beef";
        assert_eq!(
            qualify_candidate_image(digest, crate::DEFAULT_SYSTEM_UNDER_TEST),
            digest
        );

        // And the tag is read from the resolved reference, not the bare one.
        let (image, sha) = resolve_candidate_image(
            &CandidateSpec::PrebuiltImage {
                image: "dcb9f9e955".into(),
            },
            crate::DEFAULT_SYSTEM_UNDER_TEST,
        )
        .unwrap();
        assert_eq!(image, format!("{REPO}:dcb9f9e955"));
        assert_eq!(sha, "dcb9f9e955");

        // `name:tag` with no registry, where the name is the repo's own last
        // segment: a caller restating where builds live. This used to get the
        // repo prepended wholesale, minting a double-tag reference no registry
        // serves — the pod sat in ImagePullBackOff until the watch deadline.
        assert_eq!(
            qualify_candidate_image(
                "hyperswitch-router:671034e3eb",
                crate::DEFAULT_SYSTEM_UNDER_TEST
            ),
            format!("{REPO}:671034e3eb")
        );

        // A name that is NOT the configured repo names a different repo: left
        // verbatim, never silently repointed — the pull then fails naming
        // exactly what was asked for.
        assert_eq!(
            qualify_candidate_image(
                "hyperswitch-app:671034e3eb",
                crate::DEFAULT_SYSTEM_UNDER_TEST
            ),
            "hyperswitch-app:671034e3eb"
        );

        // A build-profile suffix stays on the image (the registry copy really
        // is tagged that way) and comes OFF the code identity — the source of
        // a fast build is the same commit, and the migrations fetch treats
        // this as a git ref.
        let (image, sha) = resolve_candidate_image(
            &CandidateSpec::PrebuiltImage {
                image: "7cd937aa1c-release-fast".into(),
            },
            crate::DEFAULT_SYSTEM_UNDER_TEST,
        )
        .unwrap();
        assert_eq!(image, format!("{REPO}:7cd937aa1c-release-fast"));
        assert_eq!(sha, "7cd937aa1c");

        // With no registry configured a bare ref is untouched — compose builds
        // a local tag and has no registry to resolve against.
        // -- per-system registry -------------------------------------------
        // A non-default system resolves against ITS OWN registry.
        const PRISM_REPO: &str = "2236.dkr.ecr.ap-south-1.amazonaws.com/connector-service";
        std::env::set_var("DEJA_PRISM_CANDIDATE_IMAGE_REPO", PRISM_REPO);
        assert_eq!(
            qualify_candidate_image("dcb9f9e955", "prism"),
            format!("{PRISM_REPO}:dcb9f9e955"),
            "a bare prism ref qualifies against the prism registry"
        );
        // …and the default system is NOT diverted by it.
        assert_eq!(
            qualify_candidate_image("dcb9f9e955", crate::DEFAULT_SYSTEM_UNDER_TEST),
            format!("{REPO}:dcb9f9e955")
        );
        std::env::remove_var("DEJA_PRISM_CANDIDATE_IMAGE_REPO");

        // THE no-fallback property. With no prism registry configured, a bare
        // prism ref must NOT borrow the router's repo — that names an image
        // which cannot exist and reads as a broken candidate. It is left
        // verbatim so the pull fails naming exactly what was asked for.
        assert_eq!(
            qualify_candidate_image("dcb9f9e955", "prism"),
            "dcb9f9e955",
            "an unconfigured system must not inherit the default system's registry"
        );
        assert_ne!(
            qualify_candidate_image("dcb9f9e955", "prism"),
            format!("{REPO}:dcb9f9e955")
        );

        std::env::remove_var("DEJA_CANDIDATE_IMAGE_REPO");
        assert_eq!(
            qualify_candidate_image("deja-router-local", crate::DEFAULT_SYSTEM_UNDER_TEST),
            "deja-router-local"
        );
    }
}
