//! Replay-harness orchestrator library.
//!
//! Types and store layer shared between the HTTP handlers (in `main.rs`)
//! and the future fill-in modules (lookup-table renderer, divergence
//! detector, candidate resolvers). Kept dependency-light for now —
//! filesystem-JSON metadata, no SQLite yet, no async runtime.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

pub mod api;
pub mod codebundle;
pub mod divergence;
pub mod executor;
pub mod lifecycle;
pub mod lookup;
pub mod s3;
pub mod store;

/// Specification of a candidate Hyperswitch identity. All five resolution
/// modes promised in the plan; only `LocalPath` has a real backing impl in
/// the first cut (task #7 lands the rest).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CandidateSpec {
    LocalPath { binary_or_source: PathBuf },
    PrebuiltImage { image: String },
    RepoSha { repo: String, sha: String },
    RepoBranch { repo: String, branch: String },
    RepoPr { repo: String, pr: u32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CandidateImage {
    pub docker_image: String,
    pub source_ref: String,
}

/// The set of schema migrations actually applied to a store, read back FROM the
/// store — the ground truth of "whose schema is live". A replay verdict is only
/// trustworthy if the live schema is the CANDIDATE's. If some other migration
/// set is applied — most insidiously the harness runner image's own baked
/// migrations, which have no reason to match any particular candidate — the
/// candidate runs against a schema that is neither the recording's nor its own,
/// and every resulting difference reads as a candidate regression. That is the
/// A1 failure: a wrong verdict, not a refusal.
///
/// This type makes the applied set an explicit, comparable value. The EXPECTED
/// fingerprint is a function of the candidate ref (the versions present in the
/// candidate's own `migrations/` tree at its code sha) — never a constant baked
/// into the harness. The applied fingerprint is measured. A mismatch is a
/// fail-closed refusal (preflight P1), which is a true statement about the
/// environment rather than a false statement about the candidate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaFingerprint {
    /// Applied migration versions, ascending. For diesel this is the timestamp
    /// prefix of each `migrations/<version>_<name>/` directory, as recorded in
    /// `__diesel_schema_migrations.version`.
    pub applied: Vec<String>,
}

impl SchemaFingerprint {
    pub fn new(mut applied: Vec<String>) -> Self {
        applied.sort();
        applied.dedup();
        Self { applied }
    }
    pub fn count(&self) -> usize {
        self.applied.len()
    }
    /// The highest applied version (the schema "head"), if any.
    pub fn head(&self) -> Option<&str> {
        self.applied.last().map(String::as_str)
    }
    /// Is the live schema EXACTLY the candidate's expected set? Order-independent
    /// (an out-of-order apply is still the same schema), but exact: an applied
    /// superset means a newer/foreign schema, a subset means an incomplete one —
    /// both untrustworthy for a verdict.
    pub fn matches(&self, expected: &SchemaFingerprint) -> bool {
        self.applied == expected.applied
    }
    /// Versions present in one set but not the other, for a refusal message that
    /// names the drift rather than just its size. `(missing, extra)` =
    /// (expected-not-applied, applied-not-expected).
    pub fn diff(&self, expected: &SchemaFingerprint) -> (Vec<String>, Vec<String>) {
        let applied: std::collections::BTreeSet<&str> =
            self.applied.iter().map(String::as_str).collect();
        let want: std::collections::BTreeSet<&str> =
            expected.applied.iter().map(String::as_str).collect();
        let missing = want.difference(&applied).map(|s| s.to_string()).collect();
        let extra = applied.difference(&want).map(|s| s.to_string()).collect();
        (missing, extra)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunMode {
    Record,
    Replay,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Pending,
    Resolving,
    Building,
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSpec {
    pub mode: RunMode,
    pub candidate_spec: CandidateSpec,
    /// The candidate's source repo (e.g. `juspay/hyperswitch`) — a per-run
    /// PARAMETER, because a candidate image can be built from any repo/fork. The
    /// orchestrator substitutes it into `DEJA_CANDIDATE_TARBALL_URL` (with the
    /// image's sha) to fetch that ref's `migrations/` and stage the P1 bundle
    /// (Option B). Unset → the orchestrator's `DEJA_CANDIDATE_REPO` default.
    #[serde(default)]
    pub candidate_repo: Option<String>,
    /// For mode=replay: which recording to drive. With `s3_source` set this is
    /// the SESSION FILTER (the envelope's `capture.session_id`); leave unset
    /// to auto-resolve when the scanned prefix holds exactly one session.
    pub recording_id: Option<String>,
    /// For mode=replay: pull the recording from an arbitrary S3 prefix in the
    /// deployed aggregator layout (date-partitioned gzip envelope NDJSON)
    /// instead of the demo MinIO session layout.
    #[serde(default)]
    pub s3_source: Option<S3Source>,
    /// For mode=replay: drive only these recorded correlations (each request
    /// is an independent test case). Applied at the kernel drive-list, and
    /// scoring scopes to the same subset — an undriven case is excluded, not
    /// counted omitted. Unset/empty = drive everything.
    #[serde(default)]
    pub correlation_filter: Option<Vec<String>>,
    /// For mode=record: workload arguments (kept opaque for now).
    #[serde(default)]
    pub workload: serde_json::Value,
}

/// Where a replay's recording lives when it is NOT in the demo MinIO session
/// layout: a deployed aggregator's bucket/prefix. Credentials come from the
/// orchestrator's environment (`DEJA_S3_ACCESS_KEY` / `DEJA_S3_SECRET_KEY`,
/// same as the session-layout path).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S3Source {
    /// `s3://bucket/prefix` (scheme optional).
    pub path: String,
    /// AWS region; defaults to the orchestrator env's `DEJA_S3_REGION`.
    #[serde(default)]
    pub region: Option<String>,
    /// Custom endpoint (MinIO etc.); defaults to the region's AWS endpoint.
    #[serde(default)]
    pub endpoint: Option<String>,
}

impl S3Source {
    /// Split into the S3 client config + the scan prefix. Bucket and region
    /// override the env-derived defaults; endpoint defaults to the region's
    /// AWS endpoint (so the env's demo-MinIO endpoint never leaks into a
    /// deployed-bucket pull).
    pub fn to_config(&self) -> Result<(s3::S3Config, String), String> {
        let rest = self
            .path
            .trim()
            .strip_prefix("s3://")
            .unwrap_or(self.path.trim());
        let (bucket, prefix) = rest.split_once('/').unwrap_or((rest, ""));
        if bucket.is_empty() {
            return Err(format!("s3 path '{}' has no bucket", self.path));
        }
        let mut cfg = s3::S3Config::from_env();
        cfg.bucket = bucket.to_owned();
        if let Some(region) = &self.region {
            cfg.region = region.clone();
        }
        cfg.endpoint = self
            .endpoint
            .clone()
            .unwrap_or_else(|| format!("https://s3.{}.amazonaws.com", cfg.region));
        cfg.allow_http = cfg.endpoint.starts_with("http://");
        Ok((cfg, prefix.trim_matches('/').to_owned()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    pub run_id: String,
    pub spec: RunSpec,
    pub status: RunStatus,
    pub recording_id: Option<String>,
    pub candidate_image: Option<CandidateImage>,
    pub failure_reason: Option<String>,
    /// Human-facing progress (separate from the coarse `status`): the current
    /// sub-step label, its 1-based index, and the total for this run's mode, so
    /// a client can render `[step/total] stage`. `stage_updated_ms` is the wall
    /// clock when the stage last changed — a climbing "time in stage" with a
    /// static step is how you tell "slow" from "stuck".
    #[serde(default)]
    pub stage: Option<String>,
    #[serde(default)]
    pub step: u32,
    #[serde(default)]
    pub steps_total: u32,
    #[serde(default)]
    pub stage_updated_ms: u64,
}

/// Milliseconds since the UNIX epoch (best-effort; 0 on clock error).
pub fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Minimal "give me a unique id" helper. Time-based for now; SQLite/UUID
/// can swap in later.
pub fn new_id(prefix: &str) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("{prefix}-{nanos:x}")
}

/// On-disk root for harness state. Defaults to `./harness-state` relative
/// to the working directory. Layout:
///   {root}/runs/{run_id}.json
///   {root}/recordings/{recording_id}/events.jsonl
///   {root}/lookup-tables/{run_id}.jsonl
///   {root}/observed/{run_id}.jsonl
///   {root}/http-diffs/{run_id}.jsonl
///   {root}/ready/{run_id}          readiness sentinel (see [`ReplayContract`], A2)
pub struct HarnessRoot {
    pub root: PathBuf,
}

impl HarnessRoot {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        for sub in [
            "runs",
            "recordings",
            "lookup-tables",
            "observed",
            "http-diffs",
            "ready",
        ] {
            fs::create_dir_all(root.join(sub))?;
        }
        Ok(Self { root })
    }

    pub fn run_path(&self, run_id: &str) -> PathBuf {
        self.root.join("runs").join(format!("{run_id}.json"))
    }
    pub fn recording_events_path(&self, recording_id: &str) -> PathBuf {
        self.root
            .join("recordings")
            .join(recording_id)
            .join("events.jsonl")
    }
    pub fn lookup_table_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("lookup-tables")
            .join(format!("{run_id}.jsonl"))
    }
    pub fn observed_path(&self, run_id: &str) -> PathBuf {
        self.root.join("observed").join(format!("{run_id}.jsonl"))
    }
    pub fn http_diff_path(&self, run_id: &str) -> PathBuf {
        self.root.join("http-diffs").join(format!("{run_id}.jsonl"))
    }
    /// Per-run docker build context for `local_binary` candidates.
    pub fn candidate_stage_dir(&self, run_id: &str) -> PathBuf {
        self.root.join("candidates").join(run_id)
    }
    pub fn scorecard_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("runs")
            .join(format!("{run_id}.scorecard.json"))
    }
    /// Per-call divergence ledger sidecar (one CallRecord per line).
    pub fn call_ledger_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("runs")
            .join(format!("{run_id}.call-ledger.jsonl"))
    }
    /// Record-side execution-graph nodes for a run, extracted from the recording
    /// tape (span STRUCTURE only — no boundary payloads). Published as a run
    /// artifact so the dashboard's `/graph` record side renders for in-pod runs
    /// without copying the sensitive recording tape off the pod.
    pub fn record_graph_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("runs")
            .join(format!("{run_id}.record-graph.jsonl"))
    }
    /// Seed/readback certificate sidecar written before the replay kernel runs.
    pub fn seed_certificate_path(&self, run_id: &str) -> PathBuf {
        self.root
            .join("runs")
            .join(format!("{run_id}.seed-certificate.json"))
    }
    /// Readiness sentinel the runner publishes AFTER seeding (stage 4). The
    /// candidate service blocks on this before it exec's, so it can never serve
    /// traffic against an unseeded store (A2). Per-run: a Job pod hosts one run.
    pub fn ready_sentinel_path(&self, run_id: &str) -> PathBuf {
        self.root.join("ready").join(run_id)
    }
    /// THE single derivation of a replay run's artifact contract from this
    /// harness root + run id. The runner writes these files; whatever candidate
    /// service is under test is pointed at the SAME paths. Both sides call this
    /// one function, so the two independently-configured processes cannot drift
    /// apart (A3 — previously two hand-concatenated string paths that had to
    /// agree only by convention).
    pub fn replay_contract(&self, run_id: &str) -> ReplayContract {
        ReplayContract {
            run_id: run_id.to_owned(),
            lookup_table: self.lookup_table_path(run_id),
            observed_sink: self.observed_path(run_id),
            ready_sentinel: self.ready_sentinel_path(run_id),
        }
    }
}

/// A replay run's harness-side artifact contract: the files the runner produces
/// on the shared workspace volume, all derived from one [`HarnessRoot`] + run id
/// (see [`HarnessRoot::replay_contract`]).
///
/// This is candidate-agnostic on purpose: it names the ARTIFACTS, never any
/// particular service's config schema. WHICH env var a given candidate reads
/// each artifact from (for the Hyperswitch router: `ROUTER__DEJA__REPLAY__SOURCE`
/// etc.) is that candidate's binding, and lives in the env profile / executor —
/// not in this library.
pub struct ReplayContract {
    pub run_id: String,
    /// The rendered lookup table. The candidate loads it eagerly at boot and
    /// must find it present, with content, before its process starts.
    pub lookup_table: PathBuf,
    /// Where the candidate writes observed calls; the scorer reads back this
    /// exact path. A mismatch here is silent (zero observed ⇒ false full-red
    /// verdict), which is why it is derived here, not concatenated per call site.
    pub observed_sink: PathBuf,
    /// The A2 readiness sentinel the candidate's boot guard waits on.
    pub ready_sentinel: PathBuf,
}

impl ReplayContract {
    /// A shell guard the candidate container runs before `exec`: block until the
    /// runner has published the readiness sentinel (finished seeding). Keeps the
    /// candidate from booting into an unseeded store (A2). Candidate-agnostic —
    /// any shell-capable container can use it.
    pub fn wait_for_seed_snippet(&self) -> String {
        format!(
            "until [ -f {p} ]; do sleep 0.5; done",
            p = self.ready_sentinel.display()
        )
    }
}

pub fn write_json<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let bytes = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    fs::write(path, bytes)
}

pub fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<T> {
    let bytes = fs::read(path)?;
    serde_json::from_slice::<T>(&bytes).map_err(io::Error::other)
}

#[cfg(test)]
mod harness_root_tests {
    use super::*;

    // A3: the runner writes the lookup table / observed sink at HarnessRoot's
    // own paths; the candidate is pointed at the paths `replay_contract` derives.
    // If these two ever diverge, the scorer reads zero observed calls and emits
    // a false full-red verdict. This test pins them to ONE derivation.
    #[test]
    fn contract_paths_match_the_runners_own_write_paths() {
        let root = HarnessRoot::new(std::env::temp_dir().join("deja-test-contract"))
            .expect("create harness root");
        let run_id = "run-abc";
        let c = root.replay_contract(run_id);

        // The candidate's lookup table is exactly where the runner writes it.
        assert_eq!(c.lookup_table, root.lookup_table_path(run_id));
        // The candidate's observed sink is exactly where the scorer reads back.
        assert_eq!(c.observed_sink, root.observed_path(run_id));
        // The sentinel lives under {root}/ready/{run_id}.
        assert_eq!(c.ready_sentinel, root.root.join("ready").join(run_id));
    }

    #[test]
    fn contract_lookup_table_is_absolute_when_root_is() {
        let root = HarnessRoot::new(std::env::temp_dir().join("deja-test-contract-abs"))
            .expect("create harness root");
        let c = root.replay_contract("run-xyz");
        // A deployment gives an absolute state root (/workspace/state); the
        // candidate then resolves an absolute lookup path directly and never
        // consults its (untested, footgun) relative lookup_dir branch.
        let table = c.lookup_table.display().to_string();
        assert!(table.starts_with('/'));
        assert!(table.ends_with("lookup-tables/run-xyz.jsonl"));
    }

    // A2: the wait snippet must reference the exact sentinel the runner
    // publishes after seeding — a candidate that waits on the wrong path would
    // hang until the Job times out.
    #[test]
    fn wait_for_seed_snippet_targets_the_published_sentinel() {
        let root = HarnessRoot::new(std::env::temp_dir().join("deja-test-wait-seed"))
            .expect("create harness root");
        let c = root.replay_contract("run-777");
        let snippet = c.wait_for_seed_snippet();
        let sentinel = root.ready_sentinel_path("run-777");
        assert!(snippet.contains(&sentinel.display().to_string()));
        assert!(snippet.starts_with("until [ -f "));
    }
}

#[cfg(test)]
mod schema_fingerprint_tests {
    use super::*;

    #[test]
    fn matches_is_order_independent_but_exact() {
        let expected = SchemaFingerprint::new(vec!["001".into(), "002".into(), "003".into()]);
        // Same set, different order at construction → still matches.
        let applied = SchemaFingerprint::new(vec!["003".into(), "001".into(), "002".into()]);
        assert!(applied.matches(&expected));
        assert_eq!(applied.count(), 3);
        assert_eq!(applied.head(), Some("003"));
    }

    // The A1 case: the harness runner's stale baked set (fewer migrations) is
    // applied instead of the candidate's. It must NOT match, and the diff must
    // name exactly what is missing — so the refusal is specific.
    #[test]
    fn stale_runner_set_does_not_match_candidate_and_diff_names_the_gap() {
        let candidate = SchemaFingerprint::new((1..=496).map(|n| format!("{n:04}")).collect());
        let stale_runner = SchemaFingerprint::new((1..=461).map(|n| format!("{n:04}")).collect());
        assert!(!stale_runner.matches(&candidate));

        let (missing, extra) = stale_runner.diff(&candidate);
        assert_eq!(missing.len(), 35); // 462..=496 never applied
        assert!(missing.contains(&"0496".to_string()));
        assert!(extra.is_empty()); // the stale set is a strict subset here
    }

    // An applied SUPERSET (a newer/foreign schema) is also untrustworthy.
    #[test]
    fn applied_superset_is_rejected_and_reported_as_extra() {
        let expected = SchemaFingerprint::new(vec!["001".into(), "002".into()]);
        let applied = SchemaFingerprint::new(vec!["001".into(), "002".into(), "003".into()]);
        assert!(!applied.matches(&expected));
        let (missing, extra) = applied.diff(&expected);
        assert!(missing.is_empty());
        assert_eq!(extra, vec!["003".to_string()]);
    }

    #[test]
    fn new_sorts_and_dedups() {
        let fp = SchemaFingerprint::new(vec!["002".into(), "001".into(), "002".into()]);
        assert_eq!(fp.applied, vec!["001".to_string(), "002".to_string()]);
    }
}
