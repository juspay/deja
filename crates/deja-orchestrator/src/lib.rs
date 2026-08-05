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
pub mod scope;
pub mod store;

/// Specification of a candidate Hyperswitch identity. All five resolution
/// modes promised in the plan; only `LocalPath` has a real backing impl in
/// the first cut (task #7 lands the rest).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Applied migration versions, ascending, in CANONICAL form (separators
    /// stripped — see `canonical_version`). The candidate side is built from
    /// `migrations/<version>_<name>/` directory names, which carry dashes (e.g.
    /// `2026-07-16-000001`); the store side is read from
    /// `__diesel_schema_migrations.version`, where diesel records the same
    /// versions digits-only (`20260716000001`). Canonicalizing both to the
    /// digits-only form makes the P1 comparison a true set-equality rather than
    /// a spurious dashed-vs-undashed mismatch (499 "missing" + 499 "extra" for
    /// what is really the same 500 migrations).
    pub applied: Vec<String>,
}

/// Canonicalize a migration version to diesel's recorded form by dropping the
/// `-`/`_` separators a directory-name prefix may carry, so `2026-07-16-000001`
/// and `20260716000001` (what diesel stores in `__diesel_schema_migrations`)
/// compare equal.
fn canonical_version(v: &str) -> String {
    v.chars().filter(|c| *c != '-' && *c != '_').collect()
}

impl SchemaFingerprint {
    pub fn new(applied: Vec<String>) -> Self {
        let mut applied: Vec<String> = applied.iter().map(|v| canonical_version(v)).collect();
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

impl RunSpec {
    /// How many times the record workload is driven. THE definition of the
    /// default: the lifecycle worker and the persisted run record both read it
    /// here, so the record cannot name a different number than the one that ran.
    pub fn iterations(&self) -> u64 {
        self.workload
            .get("iterations")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(1)
    }

    /// The workload arguments with their defaults filled in — what the run
    /// actually drives, rather than the absence that stood for it. Keys nothing
    /// reads yet are kept: they are still part of what was asked. Replay has no
    /// workload at all, and an empty object there would claim otherwise.
    fn resolved_workload(&self) -> serde_json::Value {
        if self.mode != RunMode::Record {
            return serde_json::Value::Null;
        }
        let mut args = match &self.workload {
            serde_json::Value::Object(map) => map.clone(),
            _ => serde_json::Map::new(),
        };
        args.insert("iterations".to_owned(), self.iterations().into());
        serde_json::Value::Object(args)
    }
}

/// What a run was asked to do, as persisted on its store row
/// (`replay_runs.params`).
///
/// The row's other columns carry identity and outcome. This carries the
/// REQUEST, which is what makes a finished run reproducible from its own record
/// and what lets a report state "these correlations, from this recording,
/// against this candidate" instead of leaving a reader to assume it.
///
/// Values are recorded RESOLVED, so the record names what ran rather than what
/// was typed: the correlation filter after the same normalization the run
/// itself applies ([`scope::RunScope`] — blanks dropped, deduped, and an empty
/// filter degraded to the whole session), the workload after its defaults, and
/// the recording id updated to the concrete session once a run that was given
/// only an `s3_source` prefix has resolved one.
///
/// The candidate ref stays AS DECLARED. Resolving a tag to a digest reads the
/// executor's environment, and the row already carries what that resolved to
/// (`candidate_sha256`); a guess here would reintroduce exactly the ambiguity
/// this record exists to remove.
///
/// The shape is [`RunSpec`] plus `expectation` — which is the body
/// `POST /api/v1/runs` accepts, so a stored row can be posted straight back to
/// re-run it. `expectation` also has its own column for querying; both are
/// written by the same insert and neither is ever updated, so they cannot drift.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunParams {
    pub mode: RunMode,
    pub candidate_spec: CandidateSpec,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub candidate_repo: Option<String>,
    /// The recording the run drives. Serialized even when absent: for an
    /// `s3_source` run that has not resolved a session yet, "not resolved" is a
    /// fact about the run, not a field that happens to be missing.
    #[serde(default)]
    pub recording_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub s3_source: Option<S3Source>,
    /// The driven test-case subset, normalized. `None` = the entire session,
    /// which is a real answer rather than an absent filter.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub correlation_filter: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub workload: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expectation: Option<String>,
}

impl RunParams {
    /// The record of an accepted request, with every default already applied.
    pub fn resolved(spec: &RunSpec, expectation: Option<&str>) -> Self {
        Self {
            mode: spec.mode,
            candidate_spec: spec.candidate_spec.clone(),
            candidate_repo: spec.candidate_repo.clone(),
            recording_id: spec.recording_id.clone(),
            s3_source: spec.s3_source.clone(),
            correlation_filter: scope::RunScope::of_spec(spec)
                .ids()
                .map(|ids| ids.iter().cloned().collect()),
            workload: spec.resolved_workload(),
            expectation: expectation.map(str::to_owned),
        }
    }

    /// Read a persisted `params` value back.
    ///
    /// Rows created before the request was persisted carry `{"workload": null}`
    /// and nothing else. That is a MISSING request, not a malformed one: the row
    /// is still a real run and a caller still wants the rest of it, so this
    /// reports `None` rather than failing the row — or the list query it arrived
    /// in — over a value that was never written.
    pub fn from_stored(params: &serde_json::Value) -> Option<Self> {
        // `candidate_spec` is the load-bearing field and doubles as the marker:
        // a params object without one carries no request to read.
        params.get("candidate_spec")?;
        serde_json::from_value(params.clone()).ok()
    }

    /// The request as the create endpoint would take it back.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or_default()
    }
}

/// Where a replay's recording lives when it is NOT in the demo MinIO session
/// layout: a deployed aggregator's bucket/prefix. Credentials come from the
/// orchestrator's environment (`DEJA_S3_ACCESS_KEY` / `DEJA_S3_SECRET_KEY`,
/// same as the session-layout path).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// What a recording's id says about the recording, when it says anything.
///
/// Two shapes exist and both are permanent. A recorder that knows the revision
/// it is running names it:
///
/// ```text
/// rec-dcb9f9e-07291352-a3     revision, when, which instance
/// ```
///
/// A recorder that does NOT know its revision must not pretend to. The code
/// sha resolves through a chain ending in the literal `"unknown"`, and
/// `rec-unknown-07291352-a3` would be worse than an opaque id: it claims a
/// provenance it does not have. So an unnamed recorder keeps the older form,
/// which at least admits it carries nothing:
///
/// ```text
/// run-1785331134782268537     a timestamp, and no claim beyond it
/// ```
///
/// Every reader therefore has to handle both, forever — recordings made before
/// this existed are still replayable, and a build without revision information
/// still records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordingIdentity {
    /// The id names the revision that produced the recording.
    Described {
        /// Short git sha of the recorded system.
        revision: String,
        /// `MMDDhhmm` UTC, when recording began.
        recorded_at: String,
        /// Discriminator for the instance, so two pods starting in the same
        /// minute stay distinct.
        instance: String,
    },
    /// The id carries no provenance. Its parts live in the recording's
    /// envelopes (`code.sha`, `instance_id`) rather than in its name.
    Opaque,
}

/// The `rec-` / `run-` prefix carries no information — the position already
/// says what this is — so composition drops it and both spellings are stripped.
pub fn recording_id_body(recording_id: &str) -> &str {
    recording_id
        .strip_prefix("rec-")
        .or_else(|| recording_id.strip_prefix("run-"))
        .unwrap_or(recording_id)
}

/// Read what a recording's id claims about itself.
///
/// Deliberately strict: a shape that does not match is [`Opaque`] rather than
/// half-parsed. An id is a convenience, and the recording's envelopes carry the
/// same facts authoritatively — so guessing here would trade a small
/// convenience for a wrong answer.
///
/// [`Opaque`]: RecordingIdentity::Opaque
pub fn parse_recording_id(recording_id: &str) -> RecordingIdentity {
    let Some(body) = recording_id.strip_prefix("rec-") else {
        return RecordingIdentity::Opaque;
    };
    let parts: Vec<&str> = body.split('-').collect();
    let [revision, recorded_at, instance] = parts[..] else {
        return RecordingIdentity::Opaque;
    };
    let shaped = !revision.is_empty()
        && revision.chars().all(|c| c.is_ascii_hexdigit())
        && recorded_at.len() == 8
        && recorded_at.chars().all(|c| c.is_ascii_digit())
        && !instance.is_empty();
    if !shaped {
        return RecordingIdentity::Opaque;
    }
    RecordingIdentity::Described {
        revision: revision.to_owned(),
        recorded_at: recorded_at.to_owned(),
        instance: instance.to_owned(),
    }
}

/// The most a run id may be. A k8s Job is named `deja-replay-{run_id}` and
/// DNS-1123 caps a name at 63, so 51 is the ceiling — and `job_name_for`
/// truncates the TAIL, which is where uniqueness lives. Composition below
/// therefore shortens a segment itself rather than letting the name builder
/// cut the end off.
pub const RUN_ID_MAX: usize = 51;

/// Segment budgets, summing with separators to exactly [`RUN_ID_MAX`]. The
/// recording keeps all 19 digits a `run-`-stripped id has, because the whole
/// point of carrying it is that it can be searched for verbatim.
const ENV_MAX: usize = 3;
const CANDIDATE_MAX: usize = 10;
const RECORDING_MAX: usize = 19;
/// `MMDDhhmmssSSS` — see [`run_id_stamp`].
const STAMP_MAX: usize = 13;

/// Compose a replay run id from the things that identify the run.
///
/// `rp-sbx-dcb9f9e955-1785331134782268537-0805t1408`
///
/// Every segment is an identifier someone already has, so the id can be read
/// and — more importantly — SEARCHED. `rp-sbx-dcb9f9e955-` finds every run of
/// a candidate, `-1785331134782268537-` every run of a recording, without
/// resolving anything first. A digest would be shorter and would answer
/// neither question.
///
/// The trailing stamp is what makes it unique, and it is a time rather than an
/// attempt counter deliberately: an attempt number has to be allocated
/// atomically, so two concurrent fires of the same inputs race for the same
/// ordinal. A time needs no coordination, and the attempt a person actually
/// wants to see ("the 3rd run of this pair") is derived for display from the
/// runs that share the same subject.
///
/// NORMALIZATION IS LOSSY, SO THE STAMP CARRIES THE DISTINCTNESS. Collapsing
/// punctuation maps `2026.08.04` and `2026-08-04` onto one segment, and
/// shortening maps every ref sharing a prefix onto one. That matters because a
/// run id is a STORAGE KEY — `s3://…/replay-runs/{id}/…` and
/// `{root}/runs/{id}.json` — so two runs sharing an id do not merely look
/// alike, the second overwrites the first's artifacts.
///
/// Distinctness therefore comes from the RESOLUTION of the stamp, not from
/// asking a store whether an id is free. Asking would be stateful, would need
/// the store at mint time, and would still be a read-then-write race between
/// two concurrent creates. A millisecond stamp needs none of that: composition
/// is a pure function of its inputs and the clock. Two runs collide only if
/// they are minted in the same millisecond with inputs that normalize alike —
/// and a lossy segment costs a little fidelity in the address either way, never
/// correctness, because the run record keeps the spec verbatim.
pub fn replay_run_id(env: &str, candidate_ref: &str, recording_id: &str, stamp: &str) -> String {
    // The prefix says only "this is a recording", which the position already
    // says. Both spellings are dropped — see [`RecordingIdentity`].
    let recording = recording_id_body(recording_id).to_owned();
    let env_seg = clamp_segment(env, ENV_MAX, Trim::Tail);
    let cand_seg = clamp_segment(candidate_ref, CANDIDATE_MAX, Trim::Tail);
    // Keep the DISTINCTIVE end of a recording id: these are timestamps, so the
    // leading digits are shared by everything recorded the same week.
    let rec_seg = clamp_segment(&recording, RECORDING_MAX, Trim::Lead);
    let stamp_seg = clamp_segment(stamp, STAMP_MAX, Trim::Tail);

    // An address that does not quite match what was typed should say so, once,
    // rather than leave someone to wonder why their tag reads differently.
    for (what, original, kept) in [
        ("environment", env, &env_seg),
        ("candidate", candidate_ref, &cand_seg),
        ("recording", recording.as_str(), &rec_seg),
    ] {
        if kept.as_str() != original {
            eprintln!(
                "run id: {what} {original:?} is addressed as {kept:?} — the run record keeps the \
                 original"
            );
        }
    }

    let id = format!("rp-{env_seg}-{cand_seg}-{rec_seg}-{stamp_seg}");
    debug_assert!(
        id.len() <= RUN_ID_MAX,
        "composed run id {id} is {} chars, over the {RUN_ID_MAX} budget",
        id.len()
    );
    id
}

enum Trim {
    /// Drop the end — for values whose start identifies them.
    Tail,
    /// Drop the start — for values whose end distinguishes them.
    Lead,
}

/// Reduce one segment to something a DNS-1123 name and an S3 key both accept,
/// within `max`. Lowercased, runs of anything else collapsed to a single `-`,
/// no leading or trailing `-` (which would double a separator or end the id on
/// one).
fn clamp_segment(value: &str, max: usize, trim: Trim) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        let ch = ch.to_ascii_lowercase();
        if ch.is_ascii_alphanumeric() {
            out.push(ch);
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    let cleaned = out.trim_matches('-');
    let clamped = if cleaned.len() <= max {
        cleaned.to_owned()
    } else {
        match trim {
            Trim::Tail => cleaned[..max].to_owned(),
            Trim::Lead => cleaned[cleaned.len() - max..].to_owned(),
        }
    };
    let clamped = clamped.trim_matches('-').to_owned();
    if clamped.is_empty() {
        "none".to_owned()
    } else {
        clamped
    }
}

/// `MMDDhhmmssSSS` in UTC — the stamp that makes a run id distinct.
///
/// Millisecond resolution, and the precision is the whole mechanism: it is what
/// lets composition stay a pure function instead of consulting a store to find
/// a free name. Written without a separator inside the time so all thirteen
/// characters fit the budget; it still reads as `08-05 14:08:32.123`.
pub fn run_id_stamp() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let (month, day) = civil_month_day(secs / 86_400);
    let today = secs % 86_400;
    format!(
        "{month:02}{day:02}{:02}{:02}{:02}{:03}",
        today / 3600,
        (today % 3600) / 60,
        today % 60,
        now.subsec_millis()
    )
}

/// Days since the epoch → (month, day), via the civil-from-days algorithm.
/// Avoids a date dependency for the one place a calendar is needed.
fn civil_month_day(days_since_epoch: u64) -> (u64, u64) {
    let z = days_since_epoch as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (m as u64, d as u64)
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
    // NOTE: the recording tape's location deliberately has NO accessor here. It
    // is private to `scope`, because a `pub fn` handing out a `&Path` to the
    // tape is what let three readers ship with no correlation scope at all —
    // one of them publishing every request in a production session through an
    // unauthenticated endpoint. Read a recording through
    // `scope::ScopedRecording`; the three uses that genuinely need the path
    // (ingest, an existence check, the kernel subprocess) go through
    // `scope::TapeSlot`. `tests/scope_invariant.rs` enforces it.
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

    // The real-sandbox P1 bug: the candidate fingerprint is built from dashed
    // migration dir names (`2026-07-16-000001`) while the store is read from
    // diesel digits-only (`20260716000001`). Same migrations — must match, not
    // report 499 missing + 499 extra. Canonicalization in `new` makes both sides
    // digits-only.
    #[test]
    fn dashed_candidate_matches_undashed_diesel_store() {
        let candidate = SchemaFingerprint::new(vec![
            "2026-07-16-000001".into(),
            "2022-09-29-084920".into(),
            "00000000000000".into(),
        ]);
        let store = SchemaFingerprint::new(vec![
            "20260716000001".into(),
            "20220929084920".into(),
            "00000000000000".into(),
        ]);
        assert!(store.matches(&candidate));
        assert_eq!(store.diff(&candidate), (Vec::new(), Vec::new()));
        assert_eq!(store.head(), Some("20260716000001"));
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

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod run_params_tests {
    use super::*;

    fn replay_spec() -> RunSpec {
        RunSpec {
            mode: RunMode::Replay,
            candidate_spec: CandidateSpec::PrebuiltImage {
                image: "registry/hyperswitch:pr-42".to_owned(),
            },
            candidate_repo: None,
            recording_id: None,
            s3_source: Some(S3Source {
                path: "s3://deja/recordings/2026-08-05".to_owned(),
                region: None,
                endpoint: None,
            }),
            correlation_filter: Some(vec![
                " c-2 ".to_owned(),
                "c-1".to_owned(),
                "  ".to_owned(),
                "c-2".to_owned(),
            ]),
            workload: serde_json::Value::Null,
        }
    }

    #[test]
    fn the_record_carries_the_whole_request() {
        let params = RunParams::resolved(&replay_spec(), Some("pass"));
        let json = params.to_json();
        // Everything a reader needs to say what was run, and what a re-run needs.
        assert_eq!(json["mode"], "replay");
        assert_eq!(
            json["candidate_spec"]["image"],
            "registry/hyperswitch:pr-42"
        );
        assert_eq!(json["s3_source"]["path"], "s3://deja/recordings/2026-08-05");
        assert_eq!(json["expectation"], "pass");
        // The recording is not resolved yet, and the record says so out loud
        // rather than by omission.
        assert!(json.get("recording_id").is_some());
        assert!(json["recording_id"].is_null());
        // A record that says `null` where a default was applied has the defect
        // this record exists to fix, so the filter is stored NORMALIZED — the
        // subset the run actually drives, not the string that was typed.
        assert_eq!(
            params.correlation_filter.as_deref(),
            Some(&["c-1".to_owned(), "c-2".to_owned()][..]),
            "blanks dropped, deduped, sorted — the same normalization the run applies"
        );
    }

    #[test]
    fn the_workload_records_the_iteration_count_that_ran() {
        let mut spec = replay_spec();
        spec.mode = RunMode::Record;
        spec.s3_source = None;
        spec.correlation_filter = None;

        // An implicit iteration count is recorded as the number that ran.
        let params = RunParams::resolved(&spec, None);
        assert_eq!(spec.iterations(), 1);
        assert_eq!(params.workload, serde_json::json!({ "iterations": 1 }));

        // An explicit one is kept, and so are arguments nothing reads yet.
        spec.workload = serde_json::json!({ "iterations": 25, "scenario": "3ds" });
        let params = RunParams::resolved(&spec, None);
        assert_eq!(
            params.workload,
            serde_json::json!({ "iterations": 25, "scenario": "3ds" })
        );

        // Replay drives no workload; an empty object would claim it drove one.
        assert!(RunParams::resolved(&replay_spec(), None).workload.is_null());
    }

    #[test]
    fn a_stored_request_reads_back_and_can_be_posted_again() {
        let params = RunParams::resolved(&replay_spec(), Some("pass"));
        let stored = params.to_json();

        // Round-trips as itself…
        assert_eq!(RunParams::from_stored(&stored), Some(params.clone()));
        // …and as the request body the create endpoint accepts, so a finished
        // run is reproducible from its own record.
        let spec: RunSpec = serde_json::from_value(stored).unwrap();
        assert_eq!(spec.mode, RunMode::Replay);
        assert_eq!(spec.correlation_filter, params.correlation_filter);
        assert_eq!(
            serde_json::to_value(&spec.candidate_spec).unwrap(),
            serde_json::to_value(&params.candidate_spec).unwrap()
        );
    }

    #[test]
    fn a_row_written_before_the_request_was_persisted_is_missing_not_broken() {
        // What every existing row holds. There is no request in it to read, and
        // the row is still a real run — so this is a missing value, never an
        // error that would take the row (or its list query) down with it.
        assert_eq!(
            RunParams::from_stored(&serde_json::json!({ "workload": null })),
            None
        );
        assert_eq!(RunParams::from_stored(&serde_json::json!({})), None);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)] // tests panic on failure by design
mod run_identity_tests {
    use super::*;

    #[test]
    fn ids_minted_in_sequence_are_distinct() {
        // Composition is a pure function of its inputs and the clock, so
        // distinctness comes from the stamp's resolution rather than from
        // asking a store whether a name is free. That matters because the id is
        // the key a run's artifacts are stored under: two runs sharing one do
        // not merely look alike, the second overwrites the first.
        let ids: std::collections::BTreeSet<String> = (0..50)
            .map(|_| {
                std::thread::sleep(std::time::Duration::from_millis(1));
                replay_run_id(
                    "sbx",
                    "dcb9f9e955",
                    "run-1785331134782268537",
                    &run_id_stamp(),
                )
            })
            .collect();
        assert_eq!(ids.len(), 50, "a stamp of this resolution must not repeat");
        assert!(ids.iter().all(|id| id.len() <= RUN_ID_MAX));
    }

    #[test]
    fn a_recording_id_is_read_in_both_shapes_and_neither_is_guessed_at() {
        // A recorder that knows its revision names it.
        assert_eq!(
            parse_recording_id("rec-dcb9f9e-07291352-a3"),
            RecordingIdentity::Described {
                revision: "dcb9f9e".into(),
                recorded_at: "07291352".into(),
                instance: "a3".into(),
            }
        );

        // A recorder that does NOT know its revision keeps the older form
        // rather than minting `rec-unknown-…`, which would claim a provenance
        // it does not have. Every recording made before ids carried one reads
        // this way, and stays replayable.
        assert_eq!(
            parse_recording_id("run-1785331134782268537"),
            RecordingIdentity::Opaque
        );

        // Anything that does not match the shape is opaque, never half-read:
        // the envelopes carry these facts authoritatively, so a guess here
        // would trade a small convenience for a wrong answer.
        for malformed in [
            "rec-dcb9f9e-07291352",     // missing the instance
            "rec-dcb9f9e-072913520-a3", // wrong width for a timestamp
            "rec-nothex-07291352-a3",   // not a revision
            "rec--07291352-a3",         // empty revision
            "rec-dcb9f9e-july2913-a3",  // not digits
            "1785331134782268537",      // no prefix at all
        ] {
            assert_eq!(
                parse_recording_id(malformed),
                RecordingIdentity::Opaque,
                "{malformed} should not have parsed"
            );
        }
    }

    #[test]
    fn both_recording_id_shapes_embed_in_a_replay_id() {
        // Whichever shape a recording has, the replay id carries it — the
        // prefix is dropped because the position already says what it is.
        let described = replay_run_id(
            "sbx",
            "dcb9f9e955",
            "rec-dcb9f9e-07291352-a3",
            "0805140832123",
        );
        assert_eq!(
            described,
            "rp-sbx-dcb9f9e955-dcb9f9e-07291352-a3-0805140832123"
        );
        assert!(described.len() <= RUN_ID_MAX, "{} chars", described.len());

        let opaque = replay_run_id(
            "sbx",
            "dcb9f9e955",
            "run-1785331134782268537",
            "0805140832123",
        );
        assert_eq!(
            opaque,
            "rp-sbx-dcb9f9e955-1785331134782268537-0805140832123"
        );
        assert!(opaque.len() <= RUN_ID_MAX, "{} chars", opaque.len());

        // The described form reads BOTH revisions: recorded by dcb9f9e,
        // replayed with dcb9f9e955 — which is the question a regression tool
        // exists to answer, and today needs two lookups.
        assert!(described.contains("dcb9f9e955"), "the candidate");
        assert!(described.contains("dcb9f9e-0729"), "the recorded revision");
    }

    #[test]
    fn a_run_id_names_what_it_replayed() {
        let id = replay_run_id("sbx", "dcb9f9e955", "run-1785331134782268537", "0805t1408");
        assert_eq!(id, "rp-sbx-dcb9f9e955-1785331134782268537-0805t1408");

        // The point of not hashing: both identifiers are found by substring,
        // with no lookup and no resolution.
        assert!(id.contains("dcb9f9e955"), "every run of a candidate");
        assert!(
            id.contains("1785331134782268537"),
            "every run of a recording"
        );
    }

    #[test]
    fn a_run_id_always_survives_the_k8s_name_builder() {
        // `job_name_for` prefixes 12 chars and truncates at 63 — from the TAIL,
        // which is exactly where uniqueness lives. Composition must therefore
        // never produce something that needs cutting. Worst case on every axis:
        let id = replay_run_id(
            "production",                           // over budget
            "2026.08.04.1-release-candidate-build", // over budget, punctuated
            "run-99999999999999999999999999999999", // over budget
            "1231t2359",
        );
        assert!(
            id.len() <= RUN_ID_MAX,
            "id {id} is {} chars, over the {RUN_ID_MAX} budget",
            id.len()
        );
        let job = crate::executor::job_name_for(&id);
        assert!(job.len() <= 63);
        assert!(
            job.ends_with(&id),
            "the k8s name must carry the id whole: {job} vs {id}"
        );
    }

    #[test]
    fn a_recording_keeps_its_distinctive_end_and_a_candidate_its_start() {
        // Recording ids are timestamps: everything recorded the same week shares
        // a prefix, so shortening one must drop the front.
        let a = replay_run_id("sbx", "abc", "run-17853311347822220000", "0805t1408");
        let b = replay_run_id("sbx", "abc", "run-17853311347811110000", "0805t1408");
        assert_ne!(a, b, "two recordings must not collapse to one id");

        // A candidate ref is distinguished by its start (a git sha prefix), so
        // shortening one drops the end — asserted as the property, not against
        // whatever the cap happens to be.
        let long = replay_run_id("sbx", "dcb9f9e955aaaaaaaaaa", "run-1", "0805t1408");
        let segment = long.split('-').nth(2).unwrap();
        assert!(
            "dcb9f9e955aaaaaaaaaa".starts_with(segment),
            "the candidate segment {segment:?} must be a PREFIX of the ref"
        );
        assert!(
            segment.len() >= 7,
            "a short git sha must survive whole: {segment:?}"
        );
    }

    #[test]
    fn segments_are_normalised_to_what_a_name_and_a_key_both_accept() {
        let id = replay_run_id("SBX", "2026.08.04", "run-abc_DEF", "0805t1408");
        assert_eq!(id, "rp-sbx-2026-08-04-abc-def-0805t1408");
        assert!(id
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-'));
        assert!(!id.contains("--"), "no empty segment: {id}");
        assert!(!id.ends_with('-') && !id.starts_with('-'));
    }

    #[test]
    fn an_empty_segment_is_named_rather_than_left_blank() {
        // An unresolved recording (an s3_source spec resolves it later) must not
        // produce `rp-sbx-abc--0805t1408`.
        let id = replay_run_id("sbx", "abc", "", "0805t1408");
        assert!(!id.contains("--"), "{id}");
        assert!(id.contains("none"), "{id}");
    }

    #[test]
    fn the_stamp_is_a_readable_utc_millisecond() {
        let stamp = run_id_stamp();
        assert_eq!(stamp.len(), STAMP_MAX, "MMDDhhmmssSSS: {stamp}");
        assert!(stamp.chars().all(|c| c.is_ascii_digit()), "{stamp}");
        let field = |from: usize, to: usize| stamp[from..to].parse::<u32>().unwrap();
        assert!((1..=12).contains(&field(0, 2)), "month: {stamp}");
        assert!((1..=31).contains(&field(2, 4)), "day: {stamp}");
        assert!(field(4, 6) < 24, "hour: {stamp}");
        assert!(field(6, 8) < 60, "minute: {stamp}");
        assert!(field(8, 10) < 60, "second: {stamp}");
        assert!(field(10, 13) < 1000, "millisecond: {stamp}");
    }

    #[test]
    fn the_civil_calendar_matches_known_dates() {
        // 2026-08-05 is 20670 days after the epoch; the algorithm is the one
        // place a date library would otherwise be needed, so pin it.
        assert_eq!(civil_month_day(0), (1, 1)); // 1970-01-01
        assert_eq!(civil_month_day(59), (3, 1)); // 1970-03-01
        assert_eq!(civil_month_day(20_669), (8, 4)); // 2026-08-04
        assert_eq!(civil_month_day(20_670), (8, 5));
    }

    #[test]
    fn two_refs_that_normalize_alike_are_separated_by_the_stamp() {
        // `2026.08.04` and `2026-08-04` collapse to one segment — normalization
        // is lossy on purpose, for readability. What keeps them from sharing a
        // storage key is that they are not minted in the same millisecond.
        assert_eq!(
            replay_run_id("sbx", "2026.08.04", "run-1", "0805140832123"),
            replay_run_id("sbx", "2026-08-04", "run-1", "0805140832123"),
            "at one instant they SHOULD be equal — that is what the stamp is for"
        );
        let a = replay_run_id("sbx", "2026.08.04", "run-1", &run_id_stamp());
        std::thread::sleep(std::time::Duration::from_millis(2));
        let b = replay_run_id("sbx", "2026-08-04", "run-1", &run_id_stamp());
        assert_ne!(a, b);
    }
}
