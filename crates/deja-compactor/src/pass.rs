//! One sealing PASS: a walk over every declared system, and a ledger that
//! accounts for every recording the walk considered.
//!
//! # Why this stopped being a shell loop
//!
//! The pass used to be a `while read` loop in a CronJob template whose only
//! durable record was a failure COUNT. A count cannot name what was dropped, so
//! the two things an operator most needs to tell apart — "there was nothing to
//! seal" and "the pass stopped after two of fifty" — produced the same output:
//! none. The unsealed total climbed and no line said why.
//!
//! The failure that forced the rewrite is worth stating, because it rules out
//! the obvious fixes. The container is OOM-killed while compacting one large
//! recording, and the kill is CGROUP-scoped: `exitCode 137` lands on the
//! container, so the shell dies with the compaction it launched and the loop's
//! `if ! deja-compactor seal …; then echo FAILED` never runs. A subprocess per
//! recording would die the same way, and so would per-system isolation inside
//! one process — everything in a container dies together. Only two things
//! survive a kill: what was already written down, and not dying.
//!
//! So this module does both.
//!
//! * **Not dying.** [`crate::compact_session_within`] refuses a landing larger
//!   than a budget instead of loading it, which turns the killing case into an
//!   ordinary named outcome ([`Outcome::TooLarge`]) that the next recording
//!   survives.
//! * **Written down first.** Each row goes to the pass's observer BEFORE the
//!   next recording is touched, so a kill leaves a complete record up to the
//!   moment of death rather than a tally that never got printed.
//!
//! # Accounting
//!
//! The unit here is not "seal a recording", it is "account for a recording".
//! [`seal_one`] has no error case: every path through it produces a [`Row`],
//! and a failure is a row that NAMES the drop. What a pass then checks is not
//! that nothing failed, but that EXTRACT and LOAD agree — every recording
//! planned was accounted for exactly once and nothing was accounted for that
//! was never planned. That is the equality a silent drop breaks and a failure
//! count never could.

use serde::{Deserialize, Serialize};

use crate::{Compaction, DynStore, S3Config, SealDecision, SealReadiness, SessionManifest};

/// What a pass did about ONE recording.
///
/// Flat rather than nested so a row survives `jq` without a schema: the
/// discriminant is the `outcome` field and every number a reader needs sits
/// beside it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum Outcome {
    /// A manifest was written. `resealed` means one already existed and the
    /// landing had grown past it.
    Sealed {
        correlations: usize,
        landing_objects: usize,
        /// Decompressed bytes compaction held for this recording. Reported on
        /// success so the ledger supplies the size distribution that sizes the
        /// budget and, later, an external merge sort's spill.
        landing_bytes_read: u64,
        /// Whether those bytes are this recording's alone — see
        /// [`Outcome::TooLarge`].
        shared_prefix: bool,
        resealed: bool,
        /// Producers that never wrote an end-of-stream marker. Carried into
        /// the ledger rather than left in a log line because it changes what
        /// the seal MEANS — the recording is whatever reached the bucket.
        ///
        /// Precisely: producers whose NEWEST landing object(s) carried no marker
        /// — plural, because objects tied at that timestamp are all scanned.
        /// Readiness reads one object per instance rather than all of them,
        /// because a marker is emitted from the writer's Shutdown arm after its
        /// final write and flush and so can only be in the last object that
        /// instance wrote. The value is the same today — no deployed recorder
        /// emits a marker at all — but the sentence is narrower than "never
        /// wrote one anywhere", and a reader deciding how much to trust a seal
        /// should have the narrower one.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        instances_without_eof: Vec<String>,
    },
    /// A seal already covers everything that has landed.
    AlreadyCurrent { sealed_objects: usize },
    /// Still being written. Not a drop: the recording is fine and the next
    /// pass will find it finished.
    NotReady { state: String, objects: usize },
    /// The landing passed the pass's memory budget while it was being read, so
    /// no seal was attempted. A DROP: this recording will not seal until
    /// either the budget rises or compaction stops holding the whole landing.
    TooLarge {
        budget_bytes: u64,
        read_bytes: u64,
        objects_read: usize,
        objects_total: usize,
        /// False means the bytes are this recording's. True means they are the
        /// whole shared partition parent's, and this recording may be small.
        shared_prefix: bool,
    },
    /// A clean failure — the store, the layout, or the recording's contents. A
    /// DROP, and the message is the whole point of the row.
    Failed { error: String },
}

impl Outcome {
    /// The tag, matching what serde writes, so a human line and a JSON row
    /// cannot come to name the same outcome differently.
    pub fn name(&self) -> &'static str {
        match self {
            Outcome::Sealed { .. } => "sealed",
            Outcome::AlreadyCurrent { .. } => "already_current",
            Outcome::NotReady { .. } => "not_ready",
            Outcome::TooLarge { .. } => "too_large",
            Outcome::Failed { .. } => "failed",
        }
    }

    /// Whether this recording was considered and left unsealed for a reason
    /// somebody has to act on.
    ///
    /// `not_ready` is deliberately NOT a drop. A recording still being written
    /// is the steady state of a live system, and a pass that reported it as a
    /// failure would alert on every tick — which is how a sealer trains its
    /// readers to ignore it.
    pub fn is_drop(&self) -> bool {
        matches!(self, Outcome::TooLarge { .. } | Outcome::Failed { .. })
    }
}

/// One recording's line in the ledger.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Row {
    pub system: String,
    pub recording_id: String,
    #[serde(flatten)]
    pub outcome: Outcome,
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}: ", self.system, self.recording_id)?;
        match &self.outcome {
            Outcome::Sealed {
                correlations,
                landing_objects,
                landing_bytes_read,
                shared_prefix,
                resealed,
                instances_without_eof,
            } => {
                let verb = if *resealed { "re-sealed" } else { "sealed" };
                let whose = if *shared_prefix {
                    " read from a SHARED partition parent, so not this recording's alone"
                } else {
                    ""
                };
                write!(
                    f,
                    "{verb} ({correlations} correlation(s), {landing_objects} landing object(s), \
                     {landing_bytes_read} decompressed byte(s){whose})"
                )?;
                if !instances_without_eof.is_empty() {
                    write!(
                        f,
                        "; instance(s) {} never wrote an end-of-stream marker, so the seal covers \
                         what landed and may be short of what was recorded",
                        instances_without_eof.join(", ")
                    )?;
                }
                Ok(())
            }
            Outcome::AlreadyCurrent { sealed_objects } => {
                write!(f, "already current ({sealed_objects} object(s) covered)")
            }
            Outcome::NotReady { state, objects } => {
                write!(f, "not ready ({state}, {objects} object(s))")
            }
            Outcome::TooLarge {
                budget_bytes,
                read_bytes,
                objects_read,
                objects_total,
                shared_prefix,
            } => {
                write!(
                    f,
                    "DROPPED too_large — {read_bytes} decompressed byte(s) past a {budget_bytes} \
                     byte budget at object {objects_read} of {objects_total}; nothing was written \
                     and it will not seal in this container until compaction stops holding the \
                     whole landing"
                )?;
                if *shared_prefix {
                    write!(
                        f,
                        ". Those objects are a SHARED partition parent's, not this recording's — \
                         it spans more than one date partition, so the bytes above include every \
                         other session under the root and say nothing about how large this one is"
                    )?;
                }
                Ok(())
            }
            Outcome::Failed { error } => write!(f, "DROPPED failed — {error}"),
        }
    }
}

/// One system's half of a pass.
///
/// `planned` holds the IDS rather than a count, because the check that matters
/// is set equality with what was accounted for, not that two numbers agree. A
/// count is satisfied by a pass that drops one recording and double-counts
/// another; a set is not.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemLedger {
    pub system: String,
    pub planned: Vec<String>,
    pub rows: Vec<Row>,
    /// Why this system produced no plan at all — an undeclared bucket, an
    /// unreadable listing. The pass goes on to the next system: a system that
    /// cannot be reached must not starve the ones after it, which is the whole
    /// reason a failure here is a FIELD rather than an early return.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unreachable: Option<String>,
}

impl SystemLedger {
    /// Every way EXTRACT and LOAD disagree about this system, named.
    ///
    /// Today the walk maintains this by construction — it accounts for each
    /// planned id exactly once — so the check passes trivially, and saying so
    /// is the honest description of it. It is here for the change that breaks
    /// that: a `continue` added inside the loop, a filter that reaches the
    /// plan but not the rows, a retry that accounts twice. Those are the
    /// silent-drop shape this module exists to make loud, and they are cheap
    /// to catch and expensive to notice.
    pub fn disagreements(&self) -> Vec<String> {
        if self.unreachable.is_some() {
            // An unreachable system has no plan to reconcile against. It is
            // reported as a failure elsewhere; what would be wrong is rows
            // without a plan, which means the plan was lost, not absent.
            if self.rows.is_empty() {
                return Vec::new();
            }
            return vec![format!(
                "{}: {} row(s) accounted for a system whose plan could not be read",
                self.system,
                self.rows.len()
            )];
        }

        let mut seen: std::collections::BTreeMap<&str, usize> = std::collections::BTreeMap::new();
        for row in &self.rows {
            *seen.entry(row.recording_id.as_str()).or_insert(0) += 1;
        }
        let planned: std::collections::BTreeSet<&str> =
            self.planned.iter().map(String::as_str).collect();

        let mut out = Vec::new();
        for id in &planned {
            match seen.get(id) {
                None => out.push(format!(
                    "{}: planned {id} but never accounted for",
                    self.system
                )),
                Some(1) => {}
                Some(n) => out.push(format!("{}: accounted for {id} {n} times", self.system)),
            }
        }
        for id in seen.keys() {
            if !planned.contains(id) {
                out.push(format!(
                    "{}: accounted for {id}, which was never planned",
                    self.system
                ));
            }
        }
        out
    }
}

/// What a whole pass did.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PassLedger {
    pub systems: Vec<SystemLedger>,
    /// The declared roster itself could not be read, so no system was walked.
    /// Distinct from every system being unreachable: that is a deployment with
    /// broken buckets, this is a deployment that never said what to seal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roster_error: Option<String>,
}

/// A pass in numbers. `planned` is EXTRACT's count and the four outcome counts
/// are LOAD's; they are reported side by side so an imbalance is visible in the
/// summary itself rather than only in [`PassLedger::disagreements`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Totals {
    pub systems: usize,
    pub systems_unreachable: usize,
    pub planned: usize,
    pub sealed: usize,
    pub already_current: usize,
    pub not_ready: usize,
    pub dropped: usize,
}

impl PassLedger {
    pub fn totals(&self) -> Totals {
        let mut t = Totals {
            systems: self.systems.len(),
            ..Totals::default()
        };
        for system in &self.systems {
            if system.unreachable.is_some() {
                t.systems_unreachable += 1;
            }
            t.planned += system.planned.len();
            for row in &system.rows {
                match row.outcome {
                    Outcome::Sealed { .. } => t.sealed += 1,
                    Outcome::AlreadyCurrent { .. } => t.already_current += 1,
                    Outcome::NotReady { .. } => t.not_ready += 1,
                    Outcome::TooLarge { .. } | Outcome::Failed { .. } => t.dropped += 1,
                }
            }
        }
        t
    }

    /// Every EXTRACT/LOAD disagreement across every system.
    pub fn disagreements(&self) -> Vec<String> {
        self.systems
            .iter()
            .flat_map(SystemLedger::disagreements)
            .collect()
    }

    /// The rows a human has to do something about.
    pub fn drops(&self) -> Vec<&Row> {
        self.systems
            .iter()
            .flat_map(|s| s.rows.iter())
            .filter(|r| r.outcome.is_drop())
            .collect()
    }

    /// Whether the pass may report success.
    ///
    /// A pass that swallows a drop stops sealing something forever with only an
    /// unread log line as evidence, so a drop, an unreachable system, a missing
    /// roster and an accounting disagreement all fail it.
    pub fn clean(&self) -> bool {
        self.roster_error.is_none()
            && self.systems.iter().all(|s| s.unreachable.is_none())
            && self.drops().is_empty()
            && self.disagreements().is_empty()
    }
}

/// A system's bucket and key root, resolved from the declared document.
///
/// Shared with the CLI's own scope resolution so a pass and a hand-run `seal`
/// cannot come to disagree about where a system's recordings are.
pub fn scope_for_system(system: &str) -> Result<(S3Config, String), String> {
    let mut cfg = S3Config::from_env();
    cfg.bucket = crate::bucket_for_system(system)?;
    let root = crate::recording_root_for(system)?;
    Ok((cfg, root))
}

/// Seal one recording and say what happened. Cannot fail: a failure is a
/// [`Row`] naming the drop, which is what makes "every recording is accounted
/// for" a property of the type rather than of the caller's diligence.
pub fn seal_one(
    cfg: &S3Config,
    system: &str,
    recording_id: &str,
    root: &str,
    quiet_after_secs: u64,
    max_landing_bytes: Option<u64>,
) -> Row {
    let store = match cfg.build() {
        Ok(store) => store,
        Err(error) => return failed_row(system, recording_id, error),
    };
    let rt = match crate::runtime() {
        Ok(rt) => rt,
        Err(error) => return failed_row(system, recording_id, error),
    };
    rt.block_on(seal_one_in(
        &store,
        system,
        recording_id,
        root,
        quiet_after_secs,
        max_landing_bytes,
    ))
}

fn failed_row(system: &str, recording_id: &str, error: String) -> Row {
    Row {
        system: system.to_owned(),
        recording_id: recording_id.to_owned(),
        outcome: Outcome::Failed { error },
    }
}

/// [`seal_one`]'s store-facing half, so the decision it makes can be tested
/// against a store rather than asserted about a bucket.
pub(crate) async fn seal_one_in(
    store: &DynStore,
    system: &str,
    recording_id: &str,
    root: &str,
    quiet_after_secs: u64,
    max_landing_bytes: Option<u64>,
) -> Row {
    let outcome = seal_outcome_in(
        store,
        recording_id,
        root,
        quiet_after_secs,
        max_landing_bytes,
    )
    .await
    .unwrap_or_else(|error| Outcome::Failed { error });
    Row {
        system: system.to_owned(),
        recording_id: recording_id.to_owned(),
        outcome,
    }
}

/// The one place a `?` becomes a named drop. Everything above it is total.
async fn seal_outcome_in(
    store: &DynStore,
    recording_id: &str,
    root: &str,
    quiet_after_secs: u64,
    max_landing_bytes: Option<u64>,
) -> Result<Outcome, String> {
    let existing = crate::manifest_of(store, recording_id).await?;
    let readiness = crate::readiness_of(store, recording_id, root, quiet_after_secs).await?;
    // The decision stays in `seal_decision`: a pass that re-derived it would be
    // a second spelling of the rule the CLI applies, and the two would drift.
    let resealed = match crate::seal_decision(existing.as_ref(), &readiness) {
        SealDecision::AlreadyCurrent { sealed_objects } => {
            return Ok(Outcome::AlreadyCurrent { sealed_objects });
        }
        SealDecision::NotReady => {
            return Ok(Outcome::NotReady {
                state: readiness_state(&readiness).to_owned(),
                objects: readiness.objects(),
            });
        }
        SealDecision::Seal { resealing } => resealing,
    };
    match crate::compact_session_inner(store, recording_id, root, max_landing_bytes).await? {
        Compaction::Sealed {
            manifest,
            landing_bytes_read,
            shared_prefix,
        } => Ok(sealed_outcome(
            &manifest,
            resealed,
            &readiness,
            landing_bytes_read,
            shared_prefix,
        )),
        Compaction::RefusedTooLarge {
            budget_bytes,
            read_bytes,
            objects_read,
            objects_total,
            shared_prefix,
        } => Ok(Outcome::TooLarge {
            budget_bytes,
            read_bytes,
            objects_read,
            objects_total,
            shared_prefix,
        }),
    }
}

fn sealed_outcome(
    manifest: &SessionManifest,
    resealed: bool,
    readiness: &SealReadiness,
    landing_bytes_read: u64,
    shared_prefix: bool,
) -> Outcome {
    Outcome::Sealed {
        correlations: manifest.counts.correlations,
        landing_objects: manifest.counts.landing_objects,
        landing_bytes_read,
        shared_prefix,
        resealed,
        instances_without_eof: match readiness {
            SealReadiness::Quiesced {
                instances_without_eof,
                ..
            } => instances_without_eof.clone(),
            _ => Vec::new(),
        },
    }
}

/// The readiness tag, spelled once. Matches `SealReadiness`'s serde tag so a
/// ledger row and a `readiness` command name the same state identically.
fn readiness_state(readiness: &SealReadiness) -> &'static str {
    match readiness {
        SealReadiness::Absent => "absent",
        SealReadiness::Active { .. } => "active",
        SealReadiness::Complete { .. } => "complete",
        SealReadiness::Quiesced { .. } => "quiesced",
    }
}

/// EXTRACT: which of a system's landed recordings this pass will consider.
///
/// A recording is planned when it has no seal, OR when its landing has grown
/// past the one it has. The second half is new here and costs nothing — the
/// listing already reads every manifest to report `sealed`, so `objects` and
/// `counts.landing_objects` are both in hand. The shell loop filtered on
/// `.sealed | not` with `jq`, which made [`SealDecision::Seal`]'s `resealing`
/// arm unreachable in production: a recording that resumed after being sealed
/// could never be re-sealed by the cron, and the code that handles it had no
/// caller. Deciding in `seal_decision` instead of in the query is the same
/// principle the CronJob's own comment stated and the `jq` filter broke.
pub(crate) async fn plan_in(store: &DynStore, root: &str) -> Result<Vec<String>, String> {
    let keys: Vec<String> = crate::list_keys(store, root)
        .await?
        .into_iter()
        .map(|p| p.as_ref().to_owned())
        .collect();
    let landed = crate::index_landed_keys(root, &keys);
    let ids: Vec<String> = landed.iter().map(|r| r.session_id.clone()).collect();
    let manifests = crate::manifests_of(store, &ids).await;
    Ok(landed
        .into_iter()
        .zip(manifests)
        .filter(|(rec, manifest)| match manifest {
            None => true,
            Some(m) => rec.objects > m.counts.landing_objects,
        })
        .map(|(rec, _)| rec.session_id)
        .collect())
}

/// One system's pass, isolated: anything that goes wrong here is recorded in
/// this system's ledger and never propagated to the walk.
pub(crate) async fn seal_system_in(
    store: &DynStore,
    system: &str,
    root: &str,
    quiet_after_secs: u64,
    max_landing_bytes: Option<u64>,
    observe: &mut dyn FnMut(&Row),
) -> SystemLedger {
    let planned = match plan_in(store, root).await {
        Ok(planned) => planned,
        Err(error) => {
            return SystemLedger {
                system: system.to_owned(),
                unreachable: Some(error),
                ..SystemLedger::default()
            };
        }
    };
    let mut ledger = SystemLedger {
        system: system.to_owned(),
        planned: planned.clone(),
        rows: Vec::with_capacity(planned.len()),
        unreachable: None,
    };
    for recording_id in planned {
        let row = seal_one_in(
            store,
            system,
            &recording_id,
            root,
            quiet_after_secs,
            max_landing_bytes,
        )
        .await;
        // Handed over BEFORE the next recording is touched. If the container is
        // killed compacting the next one, this row has already been emitted —
        // which is the only reason the pass can name what it was doing when it
        // died.
        observe(&row);
        ledger.rows.push(row);
    }
    ledger
}

/// The whole pass: every declared system, each isolated from the others.
///
/// `observe` sees each row as it is decided. It is the pass's durable output;
/// the returned ledger is the same information for a caller that survives to
/// read it, and a killed pass has only the former.
pub fn run_pass(
    quiet_after_secs: u64,
    max_landing_bytes: Option<u64>,
    observe: &mut dyn FnMut(&Row),
) -> PassLedger {
    let systems = match crate::declared_systems() {
        Ok(systems) => systems,
        Err(roster_error) => {
            return PassLedger {
                systems: Vec::new(),
                roster_error: Some(roster_error),
            };
        }
    };
    walk_systems(&systems, |system| {
        seal_system(system, quiet_after_secs, max_landing_bytes, observe)
    })
}

/// The walk, separated from what it walks.
///
/// Ask 1 of this work — a failure in one declared system must not starve the
/// rest — is a property of THIS function, and separating it is what lets the
/// property be asserted without a roster, a bucket, or the environment. Note
/// what it can and cannot promise: it isolates every failure a system can
/// RETURN. It cannot isolate a system that kills the process, because there is
/// no such thing as isolation inside one container — see the module header.
pub(crate) fn walk_systems(
    systems: &[String],
    mut seal: impl FnMut(&str) -> SystemLedger,
) -> PassLedger {
    PassLedger {
        systems: systems.iter().map(|system| seal(system)).collect(),
        roster_error: None,
    }
}

fn seal_system(
    system: &str,
    quiet_after_secs: u64,
    max_landing_bytes: Option<u64>,
    observe: &mut dyn FnMut(&Row),
) -> SystemLedger {
    let unreachable = |error: String| SystemLedger {
        system: system.to_owned(),
        unreachable: Some(error),
        ..SystemLedger::default()
    };
    let (cfg, root) = match scope_for_system(system) {
        Ok(scope) => scope,
        Err(error) => return unreachable(error),
    };
    let store = match cfg.build() {
        Ok(store) => store,
        Err(error) => return unreachable(error),
    };
    let rt = match crate::runtime() {
        Ok(rt) => rt,
        Err(error) => return unreachable(error),
    };
    rt.block_on(seal_system_in(
        &store,
        system,
        &root,
        quiet_after_secs,
        max_landing_bytes,
        observe,
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    const ROOT: &str = crate::DEFAULT_RECORDING_ROOT;

    fn store() -> DynStore {
        Arc::new(object_store::memory::InMemory::new())
    }

    fn block<T>(f: impl std::future::Future<Output = T>) -> T {
        crate::runtime().unwrap().block_on(f)
    }

    /// One boundary-event envelope. `pad` is an ignored field that lets a test
    /// say how many bytes an object contributes, which is the only way to
    /// assert a byte budget without depending on the envelope's exact shape.
    fn envelope(session: &str, gseq: u64, pad: usize) -> String {
        format!(
            r#"{{"schema_version":2,"artifact_type":"deja_artifact_record","instance_id":"i1","capture":{{"mode":"session","session_id":"{session}"}},"code":{{"sha":"abc","deja_version":"0.1.0"}},"pad":"{}","event":{{"recording_run_id":"{session}","global_sequence":{gseq},"correlation_id":"c1","boundary":"http_incoming","event_schema_version":1}}}}"#,
            "x".repeat(pad)
        )
    }

    fn eof(session: &str) -> String {
        format!(
            r#"{{"schema_version":2,"artifact_type":"deja_sink_marker","instance_id":"i1","capture":{{"mode":"session","session_id":"{session}"}},"marker":{{"kind":"eof","last_seq":9,"records_written":9,"records_dropped":0}}}}"#
        )
    }

    fn land(store: &DynStore, session: &str, object: usize, lines: &[String]) {
        let key = format!("{ROOT}/session={session}/inst=i1/part-{object}.json");
        block(crate::put(store, &key, lines.join("\n").into_bytes())).unwrap();
    }

    /// Land under a DATE partition, the layout the deployed Vector aggregator
    /// actually writes. A session under two of them is addressed at the root.
    fn land_dated(store: &DynStore, date: &str, session: &str, object: usize, lines: &[String]) {
        let key = format!("{ROOT}/dt={date}/session={session}/inst=i1/part-{object}.json");
        block(crate::put(store, &key, lines.join("\n").into_bytes())).unwrap();
    }

    fn row(id: &str, outcome: Outcome) -> Row {
        Row {
            system: "sys".to_owned(),
            recording_id: id.to_owned(),
            outcome,
        }
    }

    fn sealed_row(id: &str) -> Row {
        row(
            id,
            Outcome::Sealed {
                correlations: 1,
                landing_objects: 1,
                landing_bytes_read: 0,
                shared_prefix: false,
                resealed: false,
                instances_without_eof: Vec::new(),
            },
        )
    }

    // -- accounting: EXTRACT and LOAD must agree ------------------------------
    //
    // These build a ledger by writing its fields directly rather than by
    // running a pass, because a pass maintains the invariant on the way past.
    // A test that could only reach these states through the walk could not
    // tell a real check from one the walk makes true by construction.

    #[test]
    fn a_planned_recording_nobody_accounted_for_is_named() {
        let ledger = SystemLedger {
            system: "sys".to_owned(),
            planned: vec!["r1".to_owned(), "r2".to_owned()],
            rows: vec![sealed_row("r1")],
            unreachable: None,
        };
        let found = ledger.disagreements();
        assert_eq!(found.len(), 1, "expected exactly one complaint: {found:?}");
        assert!(
            found[0].contains("r2") && found[0].contains("never accounted for"),
            "the complaint must name the recording and what went wrong: {found:?}"
        );
    }

    #[test]
    fn a_recording_accounted_for_twice_is_named() {
        // The shape a retry introduces: the recording is not lost, it is
        // counted twice, and a bare `planned == rows.len()` would be satisfied
        // by that plus one loss. This is why `planned` holds ids.
        let ledger = SystemLedger {
            system: "sys".to_owned(),
            planned: vec!["r1".to_owned(), "r2".to_owned()],
            rows: vec![sealed_row("r1"), sealed_row("r1")],
            unreachable: None,
        };
        let found = ledger.disagreements();
        assert_eq!(found.len(), 2, "one loss and one double count: {found:?}");
        assert!(found
            .iter()
            .any(|d| d.contains("r1") && d.contains("2 times")));
        assert!(found
            .iter()
            .any(|d| d.contains("r2") && d.contains("never accounted for")));
        assert_eq!(
            ledger.planned.len(),
            ledger.rows.len(),
            "the counts agree, which is exactly what makes a count the wrong check"
        );
    }

    #[test]
    fn a_row_for_something_never_planned_is_named() {
        let ledger = SystemLedger {
            system: "sys".to_owned(),
            planned: vec!["r1".to_owned()],
            rows: vec![sealed_row("r9")],
            unreachable: None,
        };
        let found = ledger.disagreements();
        assert_eq!(found.len(), 2, "{found:?}");
        assert!(found
            .iter()
            .any(|d| d.contains("r9") && d.contains("never planned")));
    }

    #[test]
    fn a_pass_that_accounted_for_everything_has_nothing_to_say() {
        // The vacuity guard for the three above: `disagreements` must be
        // capable of being empty, or they would pass on any ledger at all.
        let ledger = SystemLedger {
            system: "sys".to_owned(),
            planned: vec!["r1".to_owned(), "r2".to_owned()],
            rows: vec![sealed_row("r2"), sealed_row("r1")],
            unreachable: None,
        };
        assert!(
            ledger.disagreements().is_empty(),
            "order is not a disagreement: {:?}",
            ledger.disagreements()
        );
    }

    #[test]
    fn an_unreachable_system_reconciles_but_still_fails_the_pass() {
        // Two different questions. "Did the books balance" is yes: there was no
        // plan to lose anything from. "May the pass report success" is no.
        let ledger = PassLedger {
            systems: vec![SystemLedger {
                system: "prism".to_owned(),
                unreachable: Some("no bucket declared".to_owned()),
                ..SystemLedger::default()
            }],
            roster_error: None,
        };
        assert!(ledger.disagreements().is_empty());
        assert!(!ledger.clean(), "an unreachable system is not a clean pass");
        assert_eq!(ledger.totals().systems_unreachable, 1);
    }

    #[test]
    fn a_drop_fails_the_pass_and_a_recording_still_being_written_does_not() {
        let dropped = PassLedger {
            systems: vec![SystemLedger {
                system: "sys".to_owned(),
                planned: vec!["r1".to_owned()],
                rows: vec![row(
                    "r1",
                    Outcome::Failed {
                        error: "s3 list: refused".to_owned(),
                    },
                )],
                unreachable: None,
            }],
            roster_error: None,
        };
        assert!(!dropped.clean());
        assert_eq!(dropped.totals().dropped, 1);

        let still_writing = PassLedger {
            systems: vec![SystemLedger {
                system: "sys".to_owned(),
                planned: vec!["r1".to_owned()],
                rows: vec![row(
                    "r1",
                    Outcome::NotReady {
                        state: "active".to_owned(),
                        objects: 3,
                    },
                )],
                unreachable: None,
            }],
            roster_error: None,
        };
        assert!(
            still_writing.clean(),
            "a live recording is the steady state; a pass that failed on it would alert every tick"
        );
        assert_eq!(still_writing.totals().dropped, 0);
    }

    // -- isolation ------------------------------------------------------------

    #[test]
    fn a_system_that_cannot_be_reached_does_not_stop_the_next_one() {
        // Ask 1, as a property of the walk: prism is second in the roster and
        // must be walked even though hyperswitch produced nothing.
        let roster = vec!["hyperswitch".to_owned(), "prism".to_owned()];
        let mut walked = Vec::new();
        let ledger = walk_systems(&roster, |system| {
            walked.push(system.to_owned());
            if system == "hyperswitch" {
                return SystemLedger {
                    system: system.to_owned(),
                    unreachable: Some("list failed".to_owned()),
                    ..SystemLedger::default()
                };
            }
            SystemLedger {
                system: system.to_owned(),
                planned: vec!["r1".to_owned()],
                rows: vec![sealed_row("r1")],
                unreachable: None,
            }
        });
        assert_eq!(walked, vec!["hyperswitch", "prism"]);
        assert_eq!(
            ledger.totals().sealed,
            1,
            "prism sealed behind a dead system"
        );
        assert_eq!(ledger.totals().systems_unreachable, 1);
    }

    #[test]
    fn a_recording_that_fails_does_not_stop_the_ones_after_it() {
        // The middle recording carries only an end-of-stream marker, so it is
        // quiet and finished but collates to nothing, and sealing it as an
        // empty recording is refused. The two either side must still seal.
        let store = store();
        for id in ["r1", "r3"] {
            land(&store, id, 0, &[envelope(id, 1, 0), eof(id)]);
        }
        land(&store, "r2", 0, &[eof("r2")]);

        let mut seen = Vec::new();
        let ledger = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |row| {
            seen.push(row.recording_id.clone())
        }));

        assert_eq!(ledger.planned.len(), 3, "all three were planned");
        assert_eq!(ledger.rows.len(), 3, "all three were accounted for");
        assert!(ledger.disagreements().is_empty());
        let failed: Vec<&Row> = ledger.rows.iter().filter(|r| r.outcome.is_drop()).collect();
        assert_eq!(failed.len(), 1, "exactly one drop: {:?}", ledger.rows);
        assert_eq!(failed[0].recording_id, "r2");
        // The listing is newest-first and these carry no date, so it falls to
        // id descending: r3, r2, r1. The failure is in the MIDDLE, which is
        // what makes this a test of continuing rather than of ordering.
        assert_eq!(seen, vec!["r3", "r2", "r1"]);
        assert_eq!(
            ledger
                .rows
                .iter()
                .filter(|r| matches!(r.outcome, Outcome::Sealed { .. }))
                .count(),
            2
        );
    }

    /// Query the store from inside the pass's observer.
    ///
    /// The observer is called from within a runtime, and starting a second one
    /// on the same thread panics, so this one gets a thread of its own. Worth
    /// the awkwardness: it is what turns "the row was emitted while the walk
    /// was still going" from a comment into an assertion.
    fn off_thread<T: Send>(f: impl FnOnce() -> T + Send) -> T {
        std::thread::scope(|scope| scope.spawn(f).join().unwrap())
    }

    #[test]
    fn a_row_is_emitted_before_the_next_recording_is_touched() {
        // The durability claim in the module header, made observable. The pass
        // is killed mid-walk, so a ledger batched at the end is a ledger
        // written never; only rows already handed over survive.
        let store = store();
        for id in ["r1", "r2", "r3"] {
            land(&store, id, 0, &[envelope(id, 1, 0), eof(id)]);
        }

        // How many recordings are sealed AT THE MOMENT each row is handed
        // over. Interleaved that is 1, 2, 3 — each row emitted as soon as its
        // own seal landed and before the next was read. Batched at the end it
        // would be 3, 3, 3: the same rows, in the same order, from a pass that
        // would have reported nothing at all had it died on the second.
        let mut sealed_when_seen = Vec::new();
        let ledger = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |_row| {
            let count = off_thread(|| {
                ["r1", "r2", "r3"]
                    .iter()
                    .filter(|id| block(crate::manifest_of(&store, id)).unwrap().is_some())
                    .count()
            });
            sealed_when_seen.push(count);
        }));

        assert_eq!(sealed_when_seen, vec![1, 2, 3]);
        assert_eq!(ledger.rows.len(), 3);
    }

    // -- the memory budget -----------------------------------------------------

    #[test]
    fn a_landing_past_the_budget_is_refused_and_nothing_is_written() {
        // Three objects of roughly 4 KiB each against a 6000-byte budget: the
        // first fits, the second does not, and the third is never fetched.
        let store = store();
        for object in 0..3 {
            land(
                &store,
                "big",
                object,
                &[envelope("big", object as u64, 4000)],
            );
        }
        let row = block(seal_one_in(&store, "sys", "big", ROOT, 0, Some(6000)));
        match row.outcome {
            Outcome::TooLarge {
                budget_bytes,
                read_bytes,
                objects_read,
                objects_total,
                shared_prefix,
            } => {
                assert_eq!(budget_bytes, 6000);
                assert!(read_bytes > 6000, "refused on the object that passed it");
                assert_eq!(objects_read, 2);
                assert_eq!(objects_total, 3);
                assert!(!shared_prefix, "one session, one prefix");
            }
            other => panic!("expected a size refusal, got {other:?}"),
        }
        assert!(row.outcome.is_drop(), "a refusal is a drop, not a success");
        assert!(
            block(crate::manifest_of(&store, "big")).unwrap().is_none(),
            "a refused recording must be left exactly as it was found"
        );
    }

    #[test]
    fn the_same_landing_seals_when_it_is_not_budgeted() {
        // The vacuity guard for the refusal above. Without it, that test would
        // pass just as well against a compaction that could no longer seal
        // anything at all, and the budget would look load-bearing when the
        // seal path was simply broken.
        let store = store();
        for object in 0..3 {
            land(
                &store,
                "big",
                object,
                &[envelope("big", object as u64, 4000)],
            );
        }
        let row = block(seal_one_in(&store, "sys", "big", ROOT, 0, None));
        match row.outcome {
            Outcome::Sealed {
                correlations,
                landing_objects,
                landing_bytes_read,
                shared_prefix,
                ..
            } => {
                assert_eq!(correlations, 1);
                assert_eq!(landing_objects, 3);
                // The size the ledger has to report for a recording that
                // SEALED — three ~4 KiB objects. Without it the pass could
                // only ever report bytes for recordings it refused, which is
                // the tail and not the distribution.
                assert!(
                    (12_000..14_000).contains(&landing_bytes_read),
                    "three padded objects, got {landing_bytes_read}"
                );
                assert!(!shared_prefix);
            }
            other => panic!("expected a seal, got {other:?}"),
        }
        assert!(block(crate::manifest_of(&store, "big")).unwrap().is_some());
    }

    #[test]
    fn a_refusal_says_when_the_bytes_are_not_this_recordings() {
        // A session that ran across midnight lands under two date partitions
        // and is addressed at their PARENT, so compaction reads every other
        // session under the root as well. Without the flag, its refusal reads
        // as "this recording is enormous" when it is two small objects, and
        // the wrong recording gets investigated.
        let store = store();
        land_dated(
            &store,
            "2026-09-08",
            "straddle",
            0,
            &[envelope("straddle", 1, 0)],
        );
        land_dated(
            &store,
            "2026-09-09",
            "straddle",
            1,
            &[envelope("straddle", 2, 0)],
        );
        for object in 0..3 {
            land_dated(
                &store,
                "2026-09-09",
                "neighbour",
                object,
                &[envelope("neighbour", object as u64, 4000)],
            );
        }

        let row = block(seal_one_in(&store, "sys", "straddle", ROOT, 0, Some(6000)));
        match &row.outcome {
            Outcome::TooLarge {
                shared_prefix,
                objects_total,
                ..
            } => {
                assert!(
                    shared_prefix,
                    "the bytes belong to the whole partition parent"
                );
                assert_eq!(
                    *objects_total, 5,
                    "and the object count is the root's, not this recording's two"
                );
            }
            other => panic!("expected a size refusal, got {other:?}"),
        }
        assert!(
            format!("{row}").contains("SHARED partition parent"),
            "the human line has to carry it too, or the JSON is the only place it is said: {row}"
        );
    }

    // -- extract ---------------------------------------------------------------

    #[test]
    fn a_sealed_recording_is_planned_again_only_once_it_has_grown() {
        // The `jq 'select(.sealed | not)'` the shell did made this impossible:
        // a recording that resumed after being sealed could never be re-sealed
        // by the cron, so `SealDecision::Seal { resealing: true }` had no
        // production caller. Deciding here instead of in the query restores it.
        let store = store();
        land(&store, "r1", 0, &[envelope("r1", 1, 0), eof("r1")]);
        assert_eq!(block(plan_in(&store, ROOT)).unwrap(), vec!["r1".to_owned()]);

        let row = block(seal_one_in(&store, "sys", "r1", ROOT, 0, None));
        assert!(matches!(row.outcome, Outcome::Sealed { .. }), "{row:?}");
        assert!(
            block(plan_in(&store, ROOT)).unwrap().is_empty(),
            "a current seal is not work"
        );

        land(&store, "r1", 1, &[envelope("r1", 2, 0)]);
        assert_eq!(
            block(plan_in(&store, ROOT)).unwrap(),
            vec!["r1".to_owned()],
            "the landing grew past the seal, so it is work again"
        );
        let again = block(seal_one_in(&store, "sys", "r1", ROOT, 0, None));
        match again.outcome {
            Outcome::Sealed {
                resealed,
                landing_objects,
                ..
            } => {
                assert!(resealed, "and the row says it superseded a manifest");
                assert_eq!(landing_objects, 2);
            }
            other => panic!("expected a re-seal, got {other:?}"),
        }
    }

    #[test]
    fn a_recording_still_being_written_is_planned_but_not_sealed() {
        let store = store();
        land(&store, "r1", 0, &[envelope("r1", 1, 0)]);
        // An hour of required quiet against a landing written a moment ago.
        let row = block(seal_one_in(&store, "sys", "r1", ROOT, 3600, None));
        match &row.outcome {
            Outcome::NotReady { state, objects } => {
                assert_eq!(state, "active");
                assert_eq!(*objects, 1);
            }
            other => panic!("expected not_ready, got {other:?}"),
        }
        assert!(!row.outcome.is_drop());
        assert!(block(crate::manifest_of(&store, "r1")).unwrap().is_none());
    }
}
