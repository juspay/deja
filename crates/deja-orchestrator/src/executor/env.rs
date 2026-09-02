//! Computing a run's env upserts from the harness artifacts + the candidate's
//! binding. This is the keystone that keeps candidate specifics OUT of the
//! library: the mapping "artifact → which env var this candidate reads it from"
//! is DATA (`CandidateBinding`, supplied by the env profile), not hardcoded
//! names. `ReplayContract` provides the artifact paths, `SchemaFingerprint` the
//! candidate's expected migration set; this module turns them into the
//! per-container `EnvUpsert`s the Job patch applies.

use super::patch::EnvUpsert;
use crate::{ReplayContract, SchemaFingerprint};

/// How one candidate service is wired: which env var it reads each replay
/// artifact from. For the Hyperswitch router these are the `ROUTER__DEJA__*`
/// keys; a different candidate supplies its own. Because it is config, no
/// candidate-specific env-var name is baked into the binary.
#[derive(Debug, Clone)]
pub struct CandidateBinding {
    /// The candidate container's name in the Job template.
    pub container: String,
    /// Env var that puts the candidate in replay mode (set to `replay`).
    pub mode_env: String,
    /// Env var carrying the run id.
    pub run_id_env: String,
    /// Env var pointing at the lookup table (← `ReplayContract::lookup_table`).
    pub source_env: String,
    /// Env var the candidate writes observed calls to (← `observed_sink`).
    pub observed_env: String,
    /// Env var carrying the candidate code sha, so the recording is not
    /// anonymous (← `sha_C`).
    pub code_sha_env: String,
}

impl CandidateBinding {
    /// The candidate container's env for this run: mode + run id + the two
    /// artifact paths + the code sha. All values are derived from the contract
    /// and the resolved candidate sha — never a constant.
    pub fn env_for(&self, contract: &ReplayContract, code_sha: &str) -> Vec<EnvUpsert> {
        vec![
            EnvUpsert::new(&self.container, &self.mode_env, "replay"),
            EnvUpsert::new(&self.container, &self.run_id_env, &contract.run_id),
            EnvUpsert::new(
                &self.container,
                &self.source_env,
                contract.lookup_table.display().to_string(),
            ),
            EnvUpsert::new(
                &self.container,
                &self.observed_env,
                contract.observed_sink.display().to_string(),
            ),
            EnvUpsert::new(&self.container, &self.code_sha_env, code_sha),
        ]
    }
}

/// The declared systems document, carried from the orchestrator into the Job.
///
/// The lifecycle stages run IN THE POD (`drive_replay_in_pod`), and the pull
/// path resolves a recording's bucket from the declaration rather than from
/// `DEJA_S3_*`. A runner without the document therefore sees no systems at all
/// and refuses stage 1 by name — "system 'hyperswitch' has no recording bucket
/// declared" — for a system the orchestrator can see perfectly well. The
/// declaration was resolved, consulted, and never delivered to the process that
/// needed it.
///
/// One source, carried by the launcher. NOT duplicated into the replay-env
/// chart: two copies of a document is how they come to disagree.
///
/// All three layers `settings::load` reads are forwarded, because the effective
/// declaration is their composition and not any one of them:
///   - `DEJA_CONFIG_TOML` verbatim when the deployment sets it inline
///   - otherwise the config FILE's contents, inline, because the Job cannot
///     mount the orchestrator's filesystem
///   - and every `DEJA__` override, so a value the orchestrator resolved from
///     the environment resolves the same way in the pod
fn declared_config_env(container: &str) -> Vec<EnvUpsert> {
    let mut out = Vec::new();
    let inline = std::env::var("DEJA_CONFIG_TOML")
        .ok()
        .filter(|d| !d.trim().is_empty());
    let document = inline.or_else(|| {
        let path = std::env::var("DEJA_CONFIG_FILE")
            .ok()
            .map(|p| p.trim().to_owned())
            .filter(|p| !p.is_empty())
            .unwrap_or_else(|| crate::settings::DEFAULT_CONFIG_FILE.to_owned());
        match std::fs::read_to_string(&path) {
            Ok(text) if !text.trim().is_empty() => Some(text),
            Ok(_) => None,
            Err(e) => {
                // Absent is normal — the deployment may configure inline or not
                // at all. Present-but-unreadable is not, and it would surface
                // as the pod refusing a system the orchestrator can see.
                if e.kind() != std::io::ErrorKind::NotFound {
                    eprintln!(
                        "executor: config file {path} could not be read ({e}); the Job will \
                         launch without the systems document and will refuse to resolve a \
                         recording's bucket"
                    );
                }
                None
            }
        }
    });
    if let Some(document) = document {
        out.push(EnvUpsert::new(container, "DEJA_CONFIG_TOML", &document));
    }
    // Overrides travel with it: `DEJA__SYSTEMS__PRISM__S3_BUCKET` resolved in
    // the orchestrator must resolve in the pod, or the two disagree about a
    // system the document alone does not fully describe.
    let mut overrides: Vec<(String, String)> = std::env::vars()
        .filter(|(k, _)| k.starts_with("DEJA__"))
        .collect();
    overrides.sort();
    for (key, value) in overrides {
        out.push(EnvUpsert::new(container, &key, &value));
    }
    out
}

/// The runner container's PER-RUN env. The static runner wiring (DB/redis
/// sidecar coords, HARNESS_STATE_DIR, orchestrator URL) belongs to the Job
/// template; only these vary run-to-run.
///
/// `expected_migrations` is the candidate's own migration set — passed only when
/// resolved (Option B: staged CodeBundle), so it stays a parameter. `None` runs
/// the P1 gate in record-only mode.
pub fn runner_env(
    container: &str,
    run_id: &str,
    run_spec_json: &str,
    expected_migrations: Option<&SchemaFingerprint>,
) -> Vec<EnvUpsert> {
    let mut env = vec![
        EnvUpsert::new(container, "DEJA_RUN_ID", run_id),
        EnvUpsert::new(container, "DEJA_RUN_SPEC", run_spec_json),
    ];
    env.extend(declared_config_env(container));
    if let Some(fp) = expected_migrations {
        // Newline-separated — the runner parses RUNNER_EXPECTED_MIGRATIONS the
        // same way (one version per line).
        env.push(EnvUpsert::new(
            container,
            "RUNNER_EXPECTED_MIGRATIONS",
            fp.applied.join("\n"),
        ));
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HarnessRoot;

    fn binding() -> CandidateBinding {
        CandidateBinding {
            container: "candidate".into(),
            mode_env: "ROUTER__DEJA__MODE".into(),
            run_id_env: "ROUTER__DEJA__RUN_ID".into(),
            source_env: "ROUTER__DEJA__REPLAY__SOURCE".into(),
            observed_env: "ROUTER__DEJA__REPLAY__OBSERVED_SINK".into(),
            code_sha_env: "ROUTER__DEJA__IDENTITY__CODE_SHA".into(),
        }
    }

    fn find<'a>(env: &'a [EnvUpsert], name: &str) -> &'a EnvUpsert {
        env.iter()
            .find(|e| e.name == name)
            .unwrap_or_else(|| panic!("env {name} present"))
    }

    #[test]
    fn candidate_env_maps_artifacts_to_the_bound_vars() {
        let _lock = crate::test_env::env_guard();
        let root =
            HarnessRoot::new(std::env::temp_dir().join("deja-test-env-binding")).expect("root");
        let contract = root.replay_contract("run-5");
        let env = binding().env_for(&contract, "abc123");

        assert_eq!(find(&env, "ROUTER__DEJA__MODE").value, "replay");
        assert_eq!(find(&env, "ROUTER__DEJA__RUN_ID").value, "run-5");
        assert_eq!(
            find(&env, "ROUTER__DEJA__REPLAY__SOURCE").value,
            contract.lookup_table.display().to_string()
        );
        assert_eq!(
            find(&env, "ROUTER__DEJA__REPLAY__OBSERVED_SINK").value,
            contract.observed_sink.display().to_string()
        );
        assert_eq!(
            find(&env, "ROUTER__DEJA__IDENTITY__CODE_SHA").value,
            "abc123"
        );
        // every pair targets the candidate container
        assert!(env.iter().all(|e| e.container == "candidate"));
    }

    #[test]
    fn runner_env_includes_expected_migrations_only_when_supplied() {
        let none = runner_env("runner", "run-5", "{}", None);
        assert!(none.iter().all(|e| e.name != "RUNNER_EXPECTED_MIGRATIONS"));

        let fp = SchemaFingerprint::new(vec!["0001".into(), "0002".into()]);
        let with = runner_env("runner", "run-5", "{}", Some(&fp));
        assert_eq!(
            find(&with, "RUNNER_EXPECTED_MIGRATIONS").value,
            "0001\n0002"
        );
        assert_eq!(find(&with, "DEJA_RUN_ID").value, "run-5");
        assert!(with.iter().all(|e| e.container == "runner"));
    }

    /// The declaration has to REACH the pod, not merely exist.
    ///
    /// The stages that resolve a recording's bucket run in the runner inside
    /// the Job. Before this, the Job's env carried the `DEJA_S3_*` slots and
    /// not the systems document, so the pod's `settings::load()` saw no systems
    /// and refused stage 1 by name for a system the orchestrator had resolved
    /// perfectly well — every replay failed in twelve seconds.
    #[test]
    fn the_job_carries_the_declaration_the_pod_resolves_from() {
        let _lock = crate::test_env::env_guard();
        const DOCUMENT: &str = "default_system = \"hyperswitch\"\n\
                                [systems.hyperswitch]\ns3_bucket = \"hyperswitch-art\"\n";
        std::env::set_var("DEJA_CONFIG_TOML", DOCUMENT);
        std::env::set_var("DEJA__SYSTEMS__PRISM__S3_BUCKET", "ucs-deja");
        let env = runner_env("runner", "run-1", "{}", None);
        std::env::remove_var("DEJA__SYSTEMS__PRISM__S3_BUCKET");

        let carried = env
            .iter()
            .find(|e| e.name == "DEJA_CONFIG_TOML")
            .expect("the Job must receive the document");
        assert_eq!(carried.value, DOCUMENT, "verbatim, not re-serialised");
        assert_eq!(carried.container, "runner");

        // Overrides travel with it, or the pod resolves a different roster from
        // the same document.
        assert!(
            env.iter()
                .any(|e| e.name == "DEJA__SYSTEMS__PRISM__S3_BUCKET" && e.value == "ucs-deja"),
            "a DEJA__ override the orchestrator resolved must reach the pod"
        );

        // And what it receives is enough to resolve from: this is the call the
        // pull path makes at stage 1.
        assert_eq!(
            crate::system::recording_scope("hyperswitch").map(|(b, _)| b),
            Ok("hyperswitch-art".to_owned()),
            "the pod's resolution succeeds on the carried document"
        );
        std::env::remove_var("DEJA_CONFIG_TOML");
    }

    /// A deployment that configures by FILE gets its contents inline, because
    /// the Job cannot mount the orchestrator's filesystem.
    #[test]
    fn a_file_configured_deployment_sends_its_contents() {
        let _lock = crate::test_env::env_guard();
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("deja.toml");
        std::fs::write(&path, "default_system = \"hyperswitch\"\n").expect("write config");
        std::env::remove_var("DEJA_CONFIG_TOML");
        std::env::set_var("DEJA_CONFIG_FILE", &path);
        let env = runner_env("runner", "run-1", "{}", None);
        std::env::remove_var("DEJA_CONFIG_FILE");

        let carried = env
            .iter()
            .find(|e| e.name == "DEJA_CONFIG_TOML")
            .expect("the file's contents travel inline");
        assert!(carried.value.contains("default_system"));
    }
}
