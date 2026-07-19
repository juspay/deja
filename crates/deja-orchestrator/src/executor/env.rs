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
        let root = HarnessRoot::new(std::env::temp_dir().join("deja-test-env-binding"))
            .expect("root");
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
        assert_eq!(find(&env, "ROUTER__DEJA__IDENTITY__CODE_SHA").value, "abc123");
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
}
