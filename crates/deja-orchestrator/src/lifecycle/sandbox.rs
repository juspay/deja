//! Helm-sandbox replay driver.
//!
//! Enabled with `DEJA_SANDBOX=helm` (the Dockerized dashboard sets it): a
//! replay run resolves its candidate router image from the run spec's git
//! ref (branch / commit / tag → an ECR tag CI pushed for that ref), installs
//! one chart release into a per-run namespace, and then waits for the
//! in-sandbox replay agent to complete the run through the verdict callback
//! (`POST /api/v1/runs/{id}/verdict`). The driver only finishes the run
//! itself when something goes wrong before that callback can happen
//! (deploy failure, deadline exceeded).
//!
//! Environment (set on the dashboard container):
//!   DEJA_SANDBOX=helm                        enable this driver
//!   DEJA_SANDBOX_CHART                       chart path (default /charts/replay-sandbox)
//!   DEJA_SANDBOX_NAMESPACE_PREFIX            namespace prefix (default deja-run-)
//!   DEJA_SANDBOX_SOURCE_NAMESPACE            namespace secrets are copied from
//!                                            (default hyperswitch-sandbox)
//!   DEJA_SANDBOX_RUN_DEADLINE_SECS           agent verdict deadline (default 1800)
//!   DEJA_SANDBOX_KEEP                        keep the namespace after the run (debugging)
//!   DEJA_CALLBACK_BASE_URL                   dashboard URL reachable FROM PODS (required)
//!   DEJA_API_SERVICE_TOKEN                   bearer token the agent presents on callbacks
//!   DEJA_CANDIDATE_IMAGE_REPO                ECR repository holding candidate router images
//!   DEJA_CANDIDATE_IMAGE_TAG_TEMPLATE        ref → tag template (default "{ref}")
//!   DEJA_ROUTER_IRSA_ROLE_ARN                IRSA role for KMS-decrypting copied secrets
//!   DEJA_KMS_KEY_ID / DEJA_KMS_REGION        KMS key the copied secrets use
//!   DEJA_ECR_REGISTRY                        private registry for optional
//!                                            helper images
//!                                            (default: candidate repo's host)
//!   DEJA_AGENT_IMAGE                         full replay-agent image reference
//!   DEJA_SANDBOX_EXTRA_VALUES                raw YAML merged last into the helm
//!                                            values (post-deploy escape hatch)
//!   DEJA_S3_REGION/BUCKET/PREFIX/ENDPOINT/ACCESS_KEY/SECRET_KEY/SESSION_TOKEN
//!                                                         recording storage

use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use crate::lifecycle::StoreCtx;
use crate::{
    read_json, CandidateImage, CandidateSpec, HarnessRoot, MigrationSource, Run, RunStatus,
};

pub fn enabled() -> bool {
    std::env::var("DEJA_SANDBOX").is_ok_and(|v| v.trim() == "helm")
}

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| default.to_owned())
}

/// Everything the driver needs from the dashboard environment.
pub struct SandboxCfg {
    pub chart: String,
    pub namespace_prefix: String,
    pub callback_base_url: String,
    pub callback_token: String,
    pub image_repo: String,
    pub tag_template: String,
    pub deadline: Duration,
    pub keep: bool,
    pub s3_region: String,
    pub s3_bucket: String,
    pub s3_prefix: String,
    pub s3_endpoint: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_session_token: String,
    /// AWS credentials for minting ECR pull tokens (see lifecycle::ecr).
    /// Empty on EKS, where the node IAM role authorizes pulls directly.
    pub ecr_access_key_id: String,
    pub ecr_secret_access_key: String,
    pub ecr_session_token: String,
    pub ecr_region: String,
    /// Namespace the sandbox environment's secrets are copied from.
    pub source_namespace: String,
    /// IRSA role + KMS key for decrypting the copied secrets (replay-sandbox
    /// chart values; empty keeps the chart's SET-ME defaults).
    pub router_role_arn: String,
    pub kms_key_id: String,
    pub kms_region: String,
    /// Private registry for optional helper images. Empty derives it from the
    /// candidate image repository's host. Router and replay-agent repositories
    /// are already full image paths, and Superposition defaults to GHCR.
    pub registry: String,
    /// Replay agent image. Empty keeps the chart default.
    pub agent_image: String,
    /// Raw YAML passed as an extra helm values file — the post-deploy escape
    /// hatch for ANY chart value without a code or image change.
    pub extra_values: String,
    /// The three real hyperswitch secrets (chart mints the rest as dummies).
    pub hs_master_enc_key: String,
    pub hs_admin_api_key: String,
    pub hs_kms_encrypted_hash_key: String,
    /// "false" disables KMS decryption of the run's secrets (plaintext mode).
    pub kms_enabled: String,
}

impl SandboxCfg {
    pub fn from_env() -> Self {
        Self {
            chart: env_or("DEJA_SANDBOX_CHART", "/charts/replay-sandbox"),
            namespace_prefix: env_or("DEJA_SANDBOX_NAMESPACE_PREFIX", "deja-run-"),
            callback_base_url: env_or("DEJA_CALLBACK_BASE_URL", ""),
            callback_token: env_or("DEJA_API_SERVICE_TOKEN", ""),
            image_repo: env_or("DEJA_CANDIDATE_IMAGE_REPO", ""),
            tag_template: env_or("DEJA_CANDIDATE_IMAGE_TAG_TEMPLATE", "{ref}"),
            deadline: Duration::from_secs(
                env_or("DEJA_SANDBOX_RUN_DEADLINE_SECS", "1800")
                    .parse()
                    .unwrap_or(1800),
            ),
            keep: std::env::var("DEJA_SANDBOX_KEEP").is_ok_and(|v| v == "1" || v == "true"),
            s3_region: env_or("DEJA_S3_REGION", "us-east-1"),
            s3_bucket: env_or("DEJA_S3_BUCKET", ""),
            s3_prefix: env_or("DEJA_S3_PREFIX", ""),
            s3_endpoint: env_or("DEJA_S3_ENDPOINT", ""),
            s3_access_key: env_or("DEJA_S3_ACCESS_KEY", ""),
            s3_secret_key: env_or("DEJA_S3_SECRET_KEY", ""),
            s3_session_token: env_or("DEJA_S3_SESSION_TOKEN", ""),
            ecr_access_key_id: env_or("DEJA_ECR_ACCESS_KEY_ID", ""),
            ecr_secret_access_key: env_or("DEJA_ECR_SECRET_ACCESS_KEY", ""),
            ecr_session_token: env_or("DEJA_ECR_SESSION_TOKEN", ""),
            ecr_region: env_or("DEJA_ECR_REGION", ""),
            source_namespace: env_or("DEJA_SANDBOX_SOURCE_NAMESPACE", "hyperswitch-sandbox"),
            router_role_arn: env_or("DEJA_ROUTER_IRSA_ROLE_ARN", ""),
            kms_key_id: env_or("DEJA_KMS_KEY_ID", ""),
            kms_region: env_or("DEJA_KMS_REGION", "ap-south-1"),
            registry: env_or("DEJA_ECR_REGISTRY", ""),
            agent_image: env_or("DEJA_AGENT_IMAGE", ""),
            extra_values: env_or("DEJA_SANDBOX_EXTRA_VALUES", ""),
            hs_master_enc_key: env_or("DEJA_HS_MASTER_ENC_KEY", ""),
            hs_admin_api_key: env_or("DEJA_HS_ADMIN_API_KEY", ""),
            hs_kms_encrypted_hash_key: env_or("DEJA_HS_KMS_ENCRYPTED_HASH_KEY", ""),
            kms_enabled: env_or("DEJA_KMS_ENABLED", ""),
        }
    }

    /// The registry sandboxes pull environment images from: explicit
    /// DEJA_ECR_REGISTRY, or derived from the candidate image repository.
    pub fn effective_registry(&self) -> String {
        if !self.registry.is_empty() {
            return self.registry.clone();
        }
        self.image_repo
            .split('/')
            .next()
            .filter(|host| host.contains('.'))
            .unwrap_or_default()
            .to_owned()
    }
}

/// Registry login the sandbox needs for its candidate image, if any:
/// `Some((registry_host, token))` when the candidate lives in ECR and the
/// dashboard carries DEJA_ECR_* credentials; `None` when the image is not in
/// ECR or when pulls are the cluster's job (EKS node role).
pub fn registry_login(
    candidate_repository: &str,
    cfg: &SandboxCfg,
) -> Result<Option<(String, String)>, String> {
    let Some(registry) = super::ecr::parse_ecr_registry(candidate_repository) else {
        return Ok(None);
    };
    if cfg.ecr_access_key_id.is_empty() || cfg.ecr_secret_access_key.is_empty() {
        eprintln!(
            "lifecycle(sandbox): candidate {} is in ECR and DEJA_ECR_ACCESS_KEY_ID / \
             DEJA_ECR_SECRET_ACCESS_KEY are not set; relying on the cluster's node IAM \
             role for the pull",
            candidate_repository
        );
        return Ok(None);
    }
    let region = if cfg.ecr_region.is_empty() {
        registry.region.clone()
    } else {
        cfg.ecr_region.clone()
    };
    let token = super::ecr::mint_token(
        &region,
        &cfg.ecr_access_key_id,
        &cfg.ecr_secret_access_key,
        &cfg.ecr_session_token,
    )?;
    Ok(Some((registry.host, token)))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegistryCredential {
    pub server: String,
    pub username: String,
    pub password: String,
}

pub fn registry_pull_credentials(
    candidate_repository: &str,
    cfg: &SandboxCfg,
) -> Result<Vec<RegistryCredential>, String> {
    let Some((server, password)) = registry_login(candidate_repository, cfg)? else {
        return Ok(Vec::new());
    };
    Ok(vec![RegistryCredential {
        server,
        username: "AWS".to_owned(),
        password,
    }])
}

/// The candidate resolved to a pullable image plus the source-matched
/// migration inputs for the chart.
#[derive(Debug, PartialEq, Eq)]
pub struct ResolvedCandidate {
    pub repository: String,
    pub tag: String,
    /// Set for branch candidates: the chart runs migrations + seed from it.
    pub branch: Option<String>,
    /// Set for sha/tag candidates: (hyperswitchRefType, hyperswitchVersion).
    pub migration_ref: Option<(&'static str, String)>,
    pub source_ref: String,
}

/// Docker tags allow [A-Za-z0-9_.-]; git refs (feature/x) do not. CI must
/// apply the same mapping when it pushes the image.
fn tag_sanitize(git_ref: &str) -> String {
    git_ref
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

pub fn resolve_candidate(
    spec: &CandidateSpec,
    migration_source: Option<&MigrationSource>,
    cfg: &SandboxCfg,
) -> Result<ResolvedCandidate, String> {
    let templated = |git_ref: &str| cfg.tag_template.replace("{ref}", &tag_sanitize(git_ref));
    let need_repo = || {
        if cfg.image_repo.trim().is_empty() {
            Err("DEJA_CANDIDATE_IMAGE_REPO is not configured; git-ref candidates need the ECR repository CI pushes router images to".to_owned())
        } else {
            Ok(cfg.image_repo.clone())
        }
    };
    let explicit_migration = migration_source.map(migration_pin);
    match spec {
        CandidateSpec::PrebuiltImage { image } => {
            // Split repo:tag — the colon after the last slash (registries carry ports).
            let split = image.rfind(':').filter(|i| *i > image.rfind('/').unwrap_or(0));
            let (repository, tag) = match split {
                Some(i) => (image[..i].to_owned(), image[i + 1..].to_owned()),
                None => (image.clone(), "latest".to_owned()),
            };
            let Some(migration) = explicit_migration else {
                return Err(
                    "prebuilt_image sandbox candidates must include migration_source so migrations match the image's source branch/commit/tag".to_owned(),
                );
            };
            Ok(ResolvedCandidate {
                repository,
                tag,
                branch: migration.branch,
                migration_ref: migration.ref_pin,
                source_ref: migration.source_ref,
            })
        }
        CandidateSpec::RepoBranch { branch, .. } => {
            let migration =
                explicit_migration.unwrap_or_else(|| migration_pin(&MigrationSource::Branch {
                    branch: branch.clone(),
                }));
            Ok(ResolvedCandidate {
                repository: need_repo()?,
                tag: templated(branch),
                branch: migration.branch,
                migration_ref: migration.ref_pin,
                source_ref: migration.source_ref,
            })
        }
        CandidateSpec::RepoSha { sha, .. } => {
            let migration =
                explicit_migration.unwrap_or_else(|| migration_pin(&MigrationSource::Sha {
                    sha: sha.clone(),
                }));
            Ok(ResolvedCandidate {
                repository: need_repo()?,
                tag: templated(sha),
                branch: migration.branch,
                migration_ref: migration.ref_pin,
                source_ref: migration.source_ref,
            })
        }
        CandidateSpec::RepoTag { tag, .. } => {
            let migration =
                explicit_migration.unwrap_or_else(|| migration_pin(&MigrationSource::Tag {
                    tag: tag.clone(),
                }));
            Ok(ResolvedCandidate {
                repository: need_repo()?,
                tag: templated(tag),
                branch: migration.branch,
                migration_ref: migration.ref_pin,
                source_ref: migration.source_ref,
            })
        }
        CandidateSpec::RepoPr { .. } => {
            Err("pr candidates are not supported by the sandbox driver yet; pass the PR branch".to_owned())
        }
        CandidateSpec::LocalPath { .. } | CandidateSpec::S3Build { .. } => Err(
            "local-path/s3-build candidates are not supported in sandbox mode yet; pass a prebuilt router image plus migration_source"
                .to_owned(),
        ),
    }
}

struct MigrationPin {
    branch: Option<String>,
    ref_pin: Option<(&'static str, String)>,
    source_ref: String,
}

fn migration_pin(source: &MigrationSource) -> MigrationPin {
    match source {
        MigrationSource::Branch { branch } => MigrationPin {
            branch: Some(branch.clone()),
            ref_pin: None,
            source_ref: branch.clone(),
        },
        MigrationSource::Sha { sha } => MigrationPin {
            branch: None,
            ref_pin: Some(("commit", sha.clone())),
            source_ref: sha.clone(),
        },
        MigrationSource::Tag { tag } => MigrationPin {
            branch: None,
            ref_pin: Some(("tags", tag.clone())),
            source_ref: tag.clone(),
        },
    }
}

fn yaml_quote(s: &str) -> String {
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

fn nonempty(value: Option<&String>) -> Option<&str> {
    value
        .map(String::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn probable_git_sha(value: &str) -> bool {
    (7..=40).contains(&value.len()) && value.chars().all(|c| c.is_ascii_hexdigit())
}

fn superposition_source_ref(image_tag: &str) -> (&'static str, String) {
    if probable_git_sha(image_tag) {
        ("commit", image_tag.to_owned())
    } else if image_tag.starts_with('v') {
        ("tags", image_tag.to_owned())
    } else {
        ("tags", format!("v{image_tag}"))
    }
}

fn superposition_image_for_tag(tag: &str) -> String {
    format!("ghcr.io/juspay/superposition:{tag}")
}

/// Per-run database password: unique per run, carried only in the 0600
/// values file and the run namespace's own secrets — nuked with the run.
pub fn run_db_password(run_id: &str) -> String {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(format!("{run_id}:{}", crate::now_ms()));
    hex::encode(&digest[..12])
}

/// Per-run Helm values for replay-sandbox/chart. Secrets travel via this
/// 0600 file, never argv. On EKS the candidate image pull is authorized by
/// the node IAM role, so no registry credentials are rendered.
pub fn render_values(
    run: &Run,
    candidate: &ResolvedCandidate,
    cfg: &SandboxCfg,
    db_password: &str,
    registry_credentials: &[RegistryCredential],
) -> String {
    let recording = run.spec.recording_id.clone().unwrap_or_default();
    let router_sa = if cfg.router_role_arn.is_empty() {
        String::new()
    } else {
        format!(
            "  serviceAccount:\n    annotations:\n      eks.amazonaws.com/role-arn: {}\n",
            yaml_quote(&cfg.router_role_arn)
        )
    };
    let recording_uri = run
        .spec
        .recording_uri
        .as_deref()
        .filter(|uri| !uri.trim().is_empty())
        .map(|uri| format!("  recordingUri: {}\n", yaml_quote(uri)))
        .unwrap_or_default();
    let postgres_tag = nonempty(run.spec.runtime_versions.postgres.as_ref());
    let redis_tag = nonempty(run.spec.runtime_versions.redis.as_ref());
    let superposition_tag = nonempty(run.spec.runtime_versions.superposition.as_ref());
    let database_image = postgres_tag
        .map(|tag| {
            format!(
                "  image:\n    repository: \"postgres\"\n    tag: {}\n",
                yaml_quote(tag)
            )
        })
        .unwrap_or_default();
    let mut out = format!(
        "run:\n  id: {id}\n  recordingId: {rec}\n{recording_uri}\n\
         router:\n  image:\n    repository: {repo}\n    tag: {tag}\n{router_sa}\n\
         database:\n  password: {dbpw}\n{database_image}\n\
         secretCopy:\n  sourceNamespace: {srcns}\n\n\
         s3:\n  region: {region}\n  bucket: {bucket}\n  prefix: {prefix}\n  endpoint: {endpoint}\n  accessKey: {ak}\n  secretKey: {sk}\n  sessionToken: {st}\n\n\
         callback:\n  baseUrl: {cb}\n  token: {ct}\n",
        id = yaml_quote(&run.run_id),
        rec = yaml_quote(&recording),
        repo = yaml_quote(&candidate.repository),
        tag = yaml_quote(&candidate.tag),
        dbpw = yaml_quote(db_password),
        database_image = database_image,
        srcns = yaml_quote(&cfg.source_namespace),
        region = yaml_quote(&cfg.s3_region),
        bucket = yaml_quote(&cfg.s3_bucket),
        prefix = yaml_quote(&cfg.s3_prefix),
        endpoint = yaml_quote(&cfg.s3_endpoint),
        ak = yaml_quote(&cfg.s3_access_key),
        sk = yaml_quote(&cfg.s3_secret_key),
        st = yaml_quote(&cfg.s3_session_token),
        cb = yaml_quote(&cfg.callback_base_url),
        ct = yaml_quote(&cfg.callback_token),
    );
    // Schema migrations follow the candidate's source ref.
    let mut migration_lines = Vec::new();
    if let Some(branch) = &candidate.branch {
        migration_lines.push(format!("  branch: {}\n", yaml_quote(branch)));
    } else if let Some((ref_type, version)) = &candidate.migration_ref {
        migration_lines.push(format!(
            "  refType: {}\n  version: {}\n",
            yaml_quote(ref_type),
            yaml_quote(version),
        ));
    }
    if let Some(tag) = postgres_tag {
        migration_lines.push(format!(
            "  checkImage:\n    repository: \"postgres\"\n    tag: {}\n",
            yaml_quote(tag)
        ));
    }
    if !migration_lines.is_empty() {
        out.push_str("\nmigrations:\n");
        for line in migration_lines {
            out.push_str(&line);
        }
    }
    if let Some(tag) = redis_tag {
        out.push_str(&format!(
            "\nredis:\n  image:\n    repository: \"redis\"\n    tag: {}\n",
            yaml_quote(tag)
        ));
    }
    if let Some(tag) = superposition_tag {
        let (ref_type, version) = superposition_source_ref(tag);
        out.push_str("\nsuperpositionSource:\n");
        out.push_str(&format!(
            "  refType: {}\n  version: {}\n",
            yaml_quote(ref_type),
            yaml_quote(&version),
        ));
        out.push_str("\nsuperpositionMigrations:\n");
        out.push_str(&format!(
            "  refType: {}\n  version: {}\n",
            yaml_quote(ref_type),
            yaml_quote(&version),
        ));
    }
    // KMS key + mode for the run's secrets.
    if !cfg.kms_key_id.is_empty() || !cfg.kms_enabled.is_empty() {
        out.push_str("\nkms:\n");
        if !cfg.kms_enabled.is_empty() {
            out.push_str(&format!(
                "  enabled: {}\n",
                if cfg.kms_enabled == "false" {
                    "false"
                } else {
                    "true"
                }
            ));
        }
        if !cfg.kms_key_id.is_empty() {
            out.push_str(&format!(
                "  keyId: {}\n  region: {}\n",
                yaml_quote(&cfg.kms_key_id),
                yaml_quote(&cfg.kms_region),
            ));
        }
    }
    // The real hyperswitch secrets (the chart fills the rest with dummies).
    let real = [
        ("masterEncKey", &cfg.hs_master_enc_key),
        ("adminApiKey", &cfg.hs_admin_api_key),
        ("kmsEncryptedHashKey", &cfg.hs_kms_encrypted_hash_key),
    ];
    if real.iter().any(|(_, v)| !v.is_empty()) {
        out.push_str("\nreplaySecrets:\n");
        for (key, value) in real {
            if !value.is_empty() {
                out.push_str(&format!("  {key}: {}\n", yaml_quote(value)));
            }
        }
    }
    // Private registry for optional helper images (chart default stays when
    // neither DEJA_ECR_REGISTRY nor a candidate repo host is known). Router and
    // replay agent use full repositories; Superposition uses GHCR.
    let registry = cfg.effective_registry();
    if !registry.is_empty() {
        out.push_str(&format!("\nregistry: {}\n", yaml_quote(&registry)));
    }
    if !registry_credentials.is_empty() {
        out.push_str("\nregistryCredentials:\n  enabled: true\n  registries:\n");
        for credential in registry_credentials {
            out.push_str(&format!(
                "    - server: {}\n      username: {}\n      password: {}\n",
                yaml_quote(&credential.server),
                yaml_quote(&credential.username),
                yaml_quote(&credential.password),
            ));
        }
    }
    let mut image_lines = Vec::new();
    if !cfg.agent_image.is_empty() {
        image_lines.push(format!("  agent: {}\n", yaml_quote(&cfg.agent_image)));
    }
    if let Some(tag) = superposition_tag {
        image_lines.push(format!(
            "  superposition: {}\n",
            yaml_quote(&superposition_image_for_tag(tag))
        ));
    }
    if !image_lines.is_empty() {
        out.push_str("\nimages:\n");
        for line in image_lines {
            out.push_str(&line);
        }
    }
    out
}

/// RFC 1123 namespace label from the run id.
pub fn namespace_for(run_id: &str, cfg: &SandboxCfg) -> String {
    let mut suffix: String = run_id
        .to_ascii_lowercase()
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' {
                c
            } else {
                '-'
            }
        })
        .collect();
    suffix.truncate(48);
    let suffix = suffix.trim_matches('-');
    format!(
        "{}{}",
        cfg.namespace_prefix,
        if suffix.is_empty() { "run" } else { suffix }
    )
}

fn sh(cmd: &mut Command, what: &str, ctx: &StoreCtx) -> Result<(), String> {
    let output = cmd
        .output()
        .map_err(|e| format!("{what}: spawn {:?}: {e}", cmd.get_program()))?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    for line in stderr
        .lines()
        .rev()
        .take(5)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
    {
        ctx.log(what, line);
    }
    if output.status.success() {
        Ok(())
    } else {
        let tail: Vec<&str> = stderr.lines().rev().take(8).collect();
        Err(format!(
            "{what} failed ({}): {}",
            output.status,
            tail.into_iter().rev().collect::<Vec<_>>().join(" | ")
        ))
    }
}

fn command_text(cmd: &mut Command, what: &str) -> String {
    match cmd.output() {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let text = if stdout.trim().is_empty() {
                stderr.trim()
            } else {
                stdout.trim()
            };
            if output.status.success() {
                text.to_owned()
            } else if text.is_empty() {
                format!("{what} failed ({})", output.status)
            } else {
                format!("{what} failed ({}): {text}", output.status)
            }
        }
        Err(e) => format!("{what}: spawn {:?}: {e}", cmd.get_program()),
    }
}

fn parse_pod_container_rows(rows: &str) -> Vec<(String, Vec<String>)> {
    rows.lines()
        .filter_map(|line| {
            let (pod, containers) = line.split_once('\t')?;
            let pod = pod.trim();
            if pod.is_empty() {
                return None;
            }
            let containers = containers
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>();
            Some((pod.to_owned(), containers))
        })
        .collect()
}

fn last_lines(text: &str, max: usize) -> String {
    let lines = text.lines().collect::<Vec<_>>();
    let start = lines.len().saturating_sub(max);
    lines[start..].join("\n")
}

fn push_section(sections: &mut Vec<String>, title: &str, body: String) {
    if body.trim().is_empty() {
        return;
    }
    sections.push(format!("{title}\n{}", body.trim()));
}

fn log_diagnostics(ctx: &StoreCtx, diagnostics: &str) {
    for line in diagnostics.lines() {
        ctx.log("sandbox-diagnostics", line);
    }
}

fn collect_deploy_diagnostics(namespace: &str, ctx: &StoreCtx) -> String {
    let mut sections = Vec::new();
    let replay_pods_selector = "app.kubernetes.io/component in (replay,router)";

    let mut get_pods = Command::new("kubectl");
    get_pods.args(["get", "pods", "-n", namespace, "-o", "wide"]);
    push_section(
        &mut sections,
        "$ kubectl get pods",
        command_text(&mut get_pods, "kubectl get pods"),
    );

    let mut pod_rows = Command::new("kubectl");
    pod_rows.args([
        "get",
        "pods",
        "-n",
        namespace,
        "-l",
        replay_pods_selector,
        "-o",
        r#"jsonpath={range .items[*]}{.metadata.name}{"\t"}{range .spec.initContainers[*]}{.name}{","}{end}{range .spec.containers[*]}{.name}{","}{end}{"\n"}{end}"#,
    ]);
    let rows = command_text(&mut pod_rows, "kubectl get pod containers");
    for (pod, containers) in parse_pod_container_rows(&rows) {
        for container in containers {
            let mut logs = Command::new("kubectl");
            logs.args(["logs", "-n", namespace, &pod, "-c", &container, "--tail=80"]);
            let text = command_text(
                &mut logs,
                &format!("kubectl logs pod/{pod} container/{container}"),
            );
            push_section(
                &mut sections,
                &format!("$ kubectl logs pod/{pod} -c {container} --tail=80"),
                text,
            );
        }
    }

    let mut events = Command::new("kubectl");
    events.args(["get", "events", "-n", namespace, "--sort-by=.lastTimestamp"]);
    push_section(
        &mut sections,
        "$ kubectl get events",
        last_lines(&command_text(&mut events, "kubectl get events"), 40),
    );

    let diagnostics = sections.join("\n\n");
    if !diagnostics.is_empty() {
        log_diagnostics(ctx, &diagnostics);
    }
    diagnostics
}

/// True when the run file already carries a terminal state (the agent's
/// verdict callback writes Completed/Failed independently of this driver).
fn already_terminal(root: &HarnessRoot, run_id: &str) -> bool {
    read_json::<Run>(&root.run_path(run_id))
        .map(|r| matches!(r.status, RunStatus::Completed | RunStatus::Failed))
        .unwrap_or(false)
}

fn cleanup(namespace: &str, cfg: &SandboxCfg, ctx: &StoreCtx) {
    if cfg.keep {
        ctx.log(
            "sandbox",
            &format!("keeping namespace {namespace} (DEJA_SANDBOX_KEEP)"),
        );
        return;
    }
    let _ = sh(
        Command::new("helm").args(["uninstall", "replay", "--namespace", namespace]),
        "sandbox-cleanup",
        ctx,
    );
    let _ = sh(
        Command::new("kubectl").args([
            "delete",
            "namespace",
            namespace,
            "--ignore-not-found",
            "--wait=false",
        ]),
        "sandbox-cleanup",
        ctx,
    );
}

fn command_tail(output: &std::process::Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let text = if stderr.trim().is_empty() {
        stdout.trim()
    } else {
        stderr.trim()
    };
    text.lines()
        .rev()
        .take(5)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<Vec<_>>()
        .join(" | ")
}

fn run_cleanup_command(cmd: &mut Command, what: &str) -> Result<String, String> {
    let output = cmd
        .output()
        .map_err(|e| format!("{what}: spawn {:?}: {e}", cmd.get_program()))?;
    let tail = command_tail(&output);
    if output.status.success() {
        Ok(if tail.is_empty() {
            format!("{what}: ok")
        } else {
            format!("{what}: {tail}")
        })
    } else {
        Err(format!("{what} failed ({}): {tail}", output.status))
    }
}

/// Best-effort immediate sandbox teardown for a dashboard "kill" action.
/// Unlike normal cleanup this ignores DEJA_SANDBOX_KEEP: the operator clicked
/// the button because they want this run namespace gone now.
pub fn terminate_namespace_for_run(run_id: &str) -> Result<(String, Vec<String>), String> {
    let cfg = SandboxCfg::from_env();
    let namespace = namespace_for(run_id, &cfg);
    let mut details = Vec::new();

    match run_cleanup_command(
        Command::new("helm").args(["uninstall", "replay", "--namespace", &namespace]),
        "sandbox-kill-helm",
    ) {
        Ok(detail) => details.push(detail),
        Err(detail) => details.push(detail),
    }

    let delete_detail = run_cleanup_command(
        Command::new("kubectl").args([
            "delete",
            "namespace",
            &namespace,
            "--ignore-not-found",
            "--wait=false",
        ]),
        "sandbox-kill-namespace",
    )?;
    details.push(delete_detail);

    Ok((namespace, details))
}

/// Drive one replay run through a Helm sandbox. Owns all status transitions
/// EXCEPT the successful terminal one, which the agent's verdict callback
/// writes; this function then observes it and returns.
pub fn drive(root: &HarnessRoot, run: &mut Run, ctx: &StoreCtx) {
    let cfg = SandboxCfg::from_env();
    let fail = |root: &HarnessRoot, run: &mut Run, ctx: &StoreCtx, e: String| {
        eprintln!("lifecycle(sandbox): run {} failed: {e}", run.run_id);
        ctx.finish(false, Some(&e));
        super::set_status(root, run, RunStatus::Failed, Some(e));
    };

    if cfg.callback_base_url.is_empty() {
        return fail(
            root,
            run,
            ctx,
            "DEJA_CALLBACK_BASE_URL is not configured; sandbox agents cannot call back".into(),
        );
    }
    if run.spec.recording_id.as_deref().unwrap_or("").is_empty() {
        return fail(root, run, ctx, "replay run has no recording_id".into());
    }

    super::set_status(root, run, RunStatus::Resolving, None);
    super::set_stage(root, run, ctx, 1, 3, "resolving candidate image");
    let candidate = match resolve_candidate(
        &run.spec.candidate_spec,
        run.spec.migration_source.as_ref(),
        &cfg,
    ) {
        Ok(c) => c,
        Err(e) => return fail(root, run, ctx, e),
    };
    run.candidate_image = Some(CandidateImage {
        docker_image: format!("{}:{}", candidate.repository, candidate.tag),
        source_ref: candidate.source_ref.clone(),
    });

    let registry_credentials = match registry_pull_credentials(&candidate.repository, &cfg) {
        Ok(credentials) => credentials,
        Err(e) => return fail(root, run, ctx, e),
    };
    if !registry_credentials.is_empty() {
        ctx.log(
            "sandbox",
            &format!(
                "minted registry pull secret for {}",
                registry_credentials
                    .iter()
                    .map(|credential| credential.server.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        );
    }

    // Each run gets its own throwaway database password.
    let db_password = run_db_password(&run.run_id);

    let namespace = namespace_for(&run.run_id, &cfg);
    let values_dir: PathBuf = root.root.join("sandboxes").join(&run.run_id);
    if let Err(e) = std::fs::create_dir_all(&values_dir) {
        return fail(
            root,
            run,
            ctx,
            format!("state dir {}: {e}", values_dir.display()),
        );
    }
    let values_path = values_dir.join("values.yaml");
    if let Err(e) = std::fs::write(
        &values_path,
        render_values(run, &candidate, &cfg, &db_password, &registry_credentials),
    ) {
        return fail(root, run, ctx, format!("write values: {e}"));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&values_path, std::fs::Permissions::from_mode(0o600));
    }
    // DEJA_SANDBOX_EXTRA_VALUES: operator-supplied YAML merged last, so any
    // chart value can be adjusted after the dashboard image is frozen.
    let extra_values_path = values_dir.join("extra-values.yaml");
    if !cfg.extra_values.is_empty() {
        if let Err(e) = std::fs::write(&extra_values_path, &cfg.extra_values) {
            return fail(root, run, ctx, format!("write extra values: {e}"));
        }
    }

    super::set_status(root, run, RunStatus::Running, None);
    super::set_stage(root, run, ctx, 2, 3, "deploying sandbox");
    let mut helm = Command::new("helm");
    helm.args([
        "upgrade",
        "--install",
        "replay",
        &cfg.chart,
        "--namespace",
        &namespace,
        "--create-namespace",
        "-f",
        values_path.to_str().unwrap_or_default(),
    ]);
    if !cfg.extra_values.is_empty() {
        helm.args(["-f", extra_values_path.to_str().unwrap_or_default()]);
    }
    helm.args(["--wait", "--timeout", "10m"]);
    if let Err(e) = sh(&mut helm, "sandbox-deploy", ctx) {
        let diagnostics = collect_deploy_diagnostics(&namespace, ctx);
        cleanup(&namespace, &cfg, ctx);
        // Pods start while `helm --wait` is still blocking, so the agent can
        // legitimately finish (verdict callback → terminal state) before the
        // wait gives up. Never overwrite a terminal verdict with a deploy
        // error.
        if already_terminal(root, &run.run_id) {
            eprintln!(
                "lifecycle(sandbox): run {} reached a verdict before the deploy wait settled; ignoring: {e}",
                run.run_id
            );
            return;
        }
        let failure = if diagnostics.trim().is_empty() {
            e
        } else {
            format!("{e}\n\nSandbox diagnostics:\n{diagnostics}")
        };
        return fail(root, run, ctx, failure);
    }

    super::set_stage(
        root,
        run,
        ctx,
        3,
        3,
        "replaying (waiting for agent verdict)",
    );
    let started = Instant::now();
    let outcome = loop {
        std::thread::sleep(Duration::from_secs(5));
        match read_json::<Run>(&root.run_path(&run.run_id)) {
            Ok(latest) if latest.status == RunStatus::Completed => break Ok(()),
            Ok(latest) if latest.status == RunStatus::Failed => {
                break Err(latest
                    .failure_reason
                    .unwrap_or_else(|| "agent reported failure".into()))
            }
            Ok(_) => {}
            Err(e) => eprintln!("lifecycle(sandbox): poll {}: {e}", run.run_id),
        }
        if started.elapsed() > cfg.deadline {
            break Err(format!(
                "sandbox run exceeded deadline ({}s) without an agent verdict",
                cfg.deadline.as_secs()
            ));
        }
    };
    cleanup(&namespace, &cfg, ctx);

    match outcome {
        // Verdict callback already persisted terminal state + store result.
        Ok(()) => eprintln!("lifecycle(sandbox): run {} completed", run.run_id),
        Err(e) => {
            // Re-read: if the agent's callback already marked it Failed, the
            // store rows are written; only finish runs WE are failing.
            if already_terminal(root, &run.run_id) {
                eprintln!("lifecycle(sandbox): run {} failed: {e}", run.run_id);
            } else {
                fail(root, run, ctx, e);
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod tests {
    use super::*;

    fn cfg() -> SandboxCfg {
        SandboxCfg {
            chart: "/charts/replay-sandbox".into(),
            namespace_prefix: "deja-run-".into(),
            callback_base_url: "http://dash:8070".into(),
            callback_token: "tok".into(),
            image_repo: "123.dkr.ecr.us-east-1.amazonaws.com/router-candidate".into(),
            tag_template: "{ref}".into(),
            deadline: Duration::from_secs(1800),
            keep: false,
            s3_region: "us-east-1".into(),
            s3_bucket: "bkt".into(),
            s3_prefix: "deja/v1".into(),
            s3_endpoint: String::new(),
            s3_access_key: "ak".into(),
            s3_secret_key: "sk".into(),
            s3_session_token: "session".into(),
            ecr_access_key_id: String::new(),
            ecr_secret_access_key: String::new(),
            ecr_session_token: String::new(),
            ecr_region: String::new(),
            source_namespace: "hyperswitch-sandbox".into(),
            router_role_arn: String::new(),
            kms_key_id: String::new(),
            kms_region: "ap-south-1".into(),
            registry: String::new(),
            agent_image: String::new(),
            extra_values: String::new(),
            hs_master_enc_key: String::new(),
            hs_admin_api_key: String::new(),
            hs_kms_encrypted_hash_key: String::new(),
            kms_enabled: String::new(),
        }
    }

    #[test]
    fn branch_candidate_maps_to_sanitized_ecr_tag_and_chart_branch() {
        let got = resolve_candidate(
            &CandidateSpec::RepoBranch {
                repo: "juspay/hyperswitch".into(),
                branch: "feature/pay-fix".into(),
            },
            None,
            &cfg(),
        )
        .unwrap();
        assert_eq!(got.tag, "feature-pay-fix");
        assert_eq!(got.branch.as_deref(), Some("feature/pay-fix"));
        assert_eq!(got.migration_ref, None);
    }

    #[test]
    fn sha_candidate_pins_commit_migrations() {
        let got = resolve_candidate(
            &CandidateSpec::RepoSha {
                repo: "juspay/hyperswitch".into(),
                sha: "abc1234".into(),
            },
            None,
            &cfg(),
        )
        .unwrap();
        assert_eq!(got.tag, "abc1234");
        assert_eq!(got.migration_ref, Some(("commit", "abc1234".into())));
    }

    #[test]
    fn tag_candidate_pins_tag_migrations() {
        let got = resolve_candidate(
            &CandidateSpec::RepoTag {
                repo: "juspay/hyperswitch".into(),
                tag: "v1.121.0".into(),
            },
            None,
            &cfg(),
        )
        .unwrap();
        assert_eq!(got.tag, "v1.121.0");
        assert_eq!(got.migration_ref, Some(("tags", "v1.121.0".into())));
    }

    #[test]
    fn superposition_runtime_tag_selects_source_ref() {
        assert_eq!(
            superposition_source_ref("0.112.0"),
            ("tags", "v0.112.0".into())
        );
        assert_eq!(
            superposition_source_ref("v0.112.0"),
            ("tags", "v0.112.0".into())
        );
        assert_eq!(
            superposition_source_ref("ff191d7f79"),
            ("commit", "ff191d7f79".into())
        );
    }

    #[test]
    fn parses_pod_container_rows_from_kubectl_jsonpath() {
        let rows = "replay-abc\tprepare,replay-agent,\nrouter-def\twait-for-replay-prepare,check-redis,hyperswitch-router,\n";

        assert_eq!(
            parse_pod_container_rows(rows),
            vec![
                (
                    "replay-abc".to_owned(),
                    vec!["prepare".to_owned(), "replay-agent".to_owned()]
                ),
                (
                    "router-def".to_owned(),
                    vec![
                        "wait-for-replay-prepare".to_owned(),
                        "check-redis".to_owned(),
                        "hyperswitch-router".to_owned()
                    ]
                ),
            ]
        );
    }

    #[test]
    fn prebuilt_image_bypasses_repo_config_and_registry_port_survives() {
        let got = resolve_candidate(
            &CandidateSpec::PrebuiltImage {
                image: "registry:5000/router:v9".into(),
            },
            Some(&MigrationSource::Branch {
                branch: "feature/pay-fix".into(),
            }),
            &SandboxCfg {
                image_repo: String::new(),
                ..cfg()
            },
        )
        .unwrap();
        assert_eq!(got.repository, "registry:5000/router");
        assert_eq!(got.tag, "v9");
        assert_eq!(got.branch.as_deref(), Some("feature/pay-fix"));
        assert_eq!(got.source_ref, "feature/pay-fix");
    }

    #[test]
    fn prebuilt_image_requires_explicit_migration_source() {
        let err = resolve_candidate(
            &CandidateSpec::PrebuiltImage {
                image: "registry:5000/router:v9".into(),
            },
            None,
            &SandboxCfg {
                image_repo: String::new(),
                ..cfg()
            },
        )
        .unwrap_err();
        assert!(err.contains("migration_source"));
    }

    #[test]
    fn git_ref_without_configured_repo_is_an_error() {
        let err = resolve_candidate(
            &CandidateSpec::RepoBranch {
                repo: "juspay/hyperswitch".into(),
                branch: "main".into(),
            },
            None,
            &SandboxCfg {
                image_repo: String::new(),
                ..cfg()
            },
        )
        .unwrap_err();
        assert!(err.contains("DEJA_CANDIDATE_IMAGE_REPO"));
    }

    #[test]
    fn values_carry_run_s3_callback_and_migration_pin() {
        let run = Run {
            run_id: "run-1".into(),
            spec: crate::RunSpec {
                mode: crate::RunMode::Replay,
                candidate_spec: CandidateSpec::RepoTag {
                    repo: "juspay/hyperswitch".into(),
                    tag: "v1.121.0".into(),
                },
                migration_source: None,
                recording_id: Some("rec-9".into()),
                recording_uri: Some("s3://hyperswitch-art/2026/07/09/file.log.gz".into()),
                runtime_versions: crate::RuntimeVersions {
                    postgres: Some("17-alpine".into()),
                    redis: Some("7.2-alpine".into()),
                    superposition: Some("0.112.0".into()),
                },
                workload: serde_json::Value::Null,
            },
            status: RunStatus::Pending,
            recording_id: None,
            candidate_image: None,
            failure_reason: None,
            stage: None,
            step: 0,
            steps_total: 0,
            stage_updated_ms: 0,
        };
        let candidate = resolve_candidate(&run.spec.candidate_spec, None, &cfg()).unwrap();
        let yaml = render_values(&run, &candidate, &cfg(), "pw-run-1", &[]);
        for needle in [
            "recordingId: \"rec-9\"",
            "recordingUri: \"s3://hyperswitch-art/2026/07/09/file.log.gz\"",
            "tag: \"v1.121.0\"",
            "password: \"pw-run-1\"",
            "sourceNamespace: \"hyperswitch-sandbox\"",
            "accessKey: \"ak\"",
            "sessionToken: \"session\"",
            "baseUrl: \"http://dash:8070\"",
            "refType: \"tags\"",
            "version: \"v1.121.0\"",
            "repository: \"postgres\"",
            "tag: \"17-alpine\"",
            "repository: \"redis\"",
            "superposition: \"ghcr.io/juspay/superposition:0.112.0\"",
            "superpositionSource:",
            "superpositionMigrations:",
            "version: \"v0.112.0\"",
        ] {
            assert!(yaml.contains(needle), "missing {needle} in:\n{yaml}");
        }
        // branch candidates migrate from the branch instead of a pin
        let branch_candidate = resolve_candidate(
            &CandidateSpec::RepoBranch {
                repo: "juspay/hyperswitch".into(),
                branch: "feature/x".into(),
            },
            None,
            &cfg(),
        )
        .unwrap();
        let branch_yaml = render_values(&run, &branch_candidate, &cfg(), "pw", &[]);
        assert!(branch_yaml.contains("branch: \"feature/x\""));
        assert!(!branch_yaml.contains("migrations:\n  refType"));
        let explicit_migration_candidate = resolve_candidate(
            &CandidateSpec::PrebuiltImage {
                image: "repo/router:v2".into(),
            },
            Some(&MigrationSource::Sha {
                sha: "abc123".into(),
            }),
            &cfg(),
        )
        .unwrap();
        let explicit_yaml = render_values(&run, &explicit_migration_candidate, &cfg(), "pw", &[]);
        assert!(explicit_yaml.contains("refType: \"commit\""));
        assert!(explicit_yaml.contains("version: \"abc123\""));
        // router IRSA + KMS ride in only when configured
        assert!(!yaml.contains("eks.amazonaws.com/role-arn"));
        let mut irsa_cfg = cfg();
        irsa_cfg.router_role_arn = "arn:aws:iam::1:role/r".into();
        irsa_cfg.kms_key_id = "arn:aws:kms:k".into();
        let irsa_yaml = render_values(&run, &candidate, &irsa_cfg, "pw", &[]);
        assert!(irsa_yaml.contains("eks.amazonaws.com/role-arn: \"arn:aws:iam::1:role/r\""));
        assert!(irsa_yaml.contains("keyId: \"arn:aws:kms:k\""));
        // registry derives from the candidate repo host; agent image only
        // rides in when configured. Runtime Superposition can still render an
        // images block.
        assert!(yaml.contains("registry: \"123.dkr.ecr.us-east-1.amazonaws.com\""));
        assert!(!yaml.contains("agent:"));
        let mut img_cfg = cfg();
        img_cfg.registry = "999.dkr.ecr.eu-west-1.amazonaws.com".into();
        img_cfg.agent_image = "deja-replay-agent:v2".into();
        let img_yaml = render_values(&run, &candidate, &img_cfg, "pw", &[]);
        assert!(img_yaml.contains("registry: \"999.dkr.ecr.eu-west-1.amazonaws.com\""));
        assert!(img_yaml.contains("agent: \"deja-replay-agent:v2\""));

        let ecr_yaml = render_values(
            &run,
            &candidate,
            &cfg(),
            "pw",
            &[RegistryCredential {
                server: "123.dkr.ecr.us-east-1.amazonaws.com".into(),
                username: "AWS".into(),
                password: "pull-password".into(),
            }],
        );
        assert!(ecr_yaml.contains("registryCredentials:"));
        assert!(ecr_yaml.contains("enabled: true"));
        assert!(ecr_yaml.contains("server: \"123.dkr.ecr.us-east-1.amazonaws.com\""));
        assert!(ecr_yaml.contains("username: \"AWS\""));
        assert!(ecr_yaml.contains("password: \"pull-password\""));
    }

    #[test]
    fn registry_derivation_prefers_explicit_env() {
        let mut c = cfg();
        assert_eq!(
            c.effective_registry(),
            "123.dkr.ecr.us-east-1.amazonaws.com"
        );
        c.registry = "explicit.registry".into();
        assert_eq!(c.effective_registry(), "explicit.registry");
        c.registry = String::new();
        c.image_repo = "local-name".into(); // no registry host
        assert_eq!(c.effective_registry(), "");
    }

    #[test]
    fn db_password_is_unique_per_run() {
        assert_ne!(run_db_password("run-a"), run_db_password("run-b"));
        assert_eq!(run_db_password("run-a").len(), 24);
    }

    #[test]
    fn namespace_is_rfc1123_safe() {
        assert_eq!(namespace_for("run_ABC.9", &cfg()), "deja-run-run-abc-9");
    }
}
