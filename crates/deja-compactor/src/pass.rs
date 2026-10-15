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

use futures::TryStreamExt;
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
    /// A replayable manifest was written. `resealed` means an earlier replayable
    /// tape existed and the source snapshot changed.
    Sealed {
        correlations: usize,
        landing_objects: usize,
        /// Decompressed bytes compaction held for this recording. Reported on
        /// success so the ledger supplies the size distribution that sizes the
        /// budget and, later, an external merge sort's spill.
        landing_bytes_read: u64,
        /// Compressed bytes fetched for the same landing. Recorded beside the
        /// decompressed total because ONE of them sizes a memory budget and
        /// only the PAIR gives a compression ratio — the number every
        /// "decide it from the listing" proposal needs and that nothing has
        /// ever written down.
        landing_bytes_fetched: u64,
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
    /// The current manifest's source snapshot matches the landing.
    AlreadyCurrent { source_objects: usize },
    /// Still being written. Not a drop: the recording is fine and the next
    /// pass will find it finished.
    NotReady { state: String, objects: usize },
    /// The landing passed the pass's memory budget while it was being read, so
    /// no seal was attempted. A DROP: this recording will not seal until
    /// either the budget rises or compaction stops holding the whole landing.
    TooLarge {
        budget_bytes: u64,
        read_bytes: u64,
        /// Stored bytes fetched before the ceiling was hit. A lower bound, the
        /// same as `read_bytes` — but the RATIO between the two is exact for
        /// the objects that were read, and this is the population any
        /// decide-from-the-listing gate would be judged on.
        fetched_bytes: u64,
        objects_read: usize,
        objects_total: usize,
        /// False means the bytes are this recording's. True means they are the
        /// whole shared partition parent's, and this recording may be small.
        shared_prefix: bool,
    },
    /// A day currently assigned no lines of its own. The terminal empty
    /// manifest supersedes any previously published tape for this day.
    EmptyDay {
        other_day_lines: usize,
        lines_without_ingress: usize,
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
            Outcome::EmptyDay { .. } => "empty_day",
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

    /// Whether this drop is a REFUSAL: a determination the pass made about a
    /// recording, rather than something that went wrong while handling it.
    ///
    /// The distinction decides the exit code, and it is the same argument that
    /// already excludes `not_ready` from [`Outcome::is_drop`] one step further
    /// on. A refusal is deterministic: the same recording against the same
    /// budget refuses identically every pass, forever, until a human changes
    /// the budget or the recording goes away. So it is news exactly once, and
    /// after that a pass that fails on it reports "the sealer is broken" on
    /// every tick while the sealer is in fact working — which is how a sealer
    /// trains its readers to ignore it, the very outcome that comment guards
    /// against.
    ///
    /// It cost us the ability to read the sealer at all. Eight August
    /// recordings, 0.8-2.6 GB each against a 512 MiB budget, made
    /// `lastSuccessfulTime` null permanently; the pass that sealed three
    /// recordings and the pass that ran on a stale image and sealed nothing
    /// both read as `BackoffLimitExceeded`, and telling them apart needed the
    /// container logs.
    ///
    /// A refusal stays a drop, stays printed by name, and stays counted — this
    /// is NOT a swallow. What changes is only whether it alone fails the pass.
    /// `Failed` is the opposite case and still does: it says the store, the
    /// layout or the contents surprised us, which is not deterministic, may
    /// clear on its own, and is worth waking up for.
    pub fn is_refusal(&self) -> bool {
        matches!(self, Outcome::TooLarge { .. })
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
                landing_bytes_fetched,
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
                     {landing_bytes_read} byte(s) held from {landing_bytes_fetched} fetched{whose})"
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
            Outcome::AlreadyCurrent { source_objects } => {
                write!(
                    f,
                    "already current ({source_objects} source object(s) covered)"
                )
            }
            Outcome::NotReady { state, objects } => {
                write!(f, "not ready ({state}, {objects} object(s))")
            }
            Outcome::TooLarge {
                budget_bytes,
                read_bytes,
                objects_read,
                fetched_bytes,
                objects_total,
                shared_prefix,
            } => {
                write!(
                    f,
                    "DROPPED too_large — {read_bytes} byte(s) held from {fetched_bytes} fetched, past a {budget_bytes} \
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
            Outcome::EmptyDay {
                other_day_lines,
                lines_without_ingress,
            } => write!(
                f,
                "empty day — {other_day_lines} line(s) belong to requests assigned elsewhere; \
                 {lines_without_ingress} are retained without their ingress"
            ),
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

/// A pass in numbers. `planned` is EXTRACT's count and the outcome counts are
/// LOAD's; they are reported side by side so an imbalance is visible in the
/// summary itself rather than only in [`PassLedger::disagreements`].
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Totals {
    pub systems: usize,
    pub systems_unreachable: usize,
    pub planned: usize,
    pub sealed: usize,
    pub already_current: usize,
    pub not_ready: usize,
    /// Terminal empty day manifests published when selection moves elsewhere.
    #[serde(default)]
    pub empty_days: usize,
    /// Refusals plus failures. Kept as the total it always was, so a reader of
    /// the JSON that predates the split still sees every unsealed recording in
    /// the field it already reads.
    pub dropped: usize,
    /// Drops that are determinations: deterministic, permanent until something
    /// changes, and on their own not a failed pass.
    pub refused: usize,
    /// Drops that say something went wrong. These fail the pass.
    pub failed: usize,
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
                    Outcome::EmptyDay { .. } => t.empty_days += 1,
                    Outcome::TooLarge { .. } => {
                        t.dropped += 1;
                        t.refused += 1;
                    }
                    Outcome::Failed { .. } => {
                        t.dropped += 1;
                        t.failed += 1;
                    }
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

    /// The drops that are determinations rather than malfunctions — see
    /// [`Outcome::is_refusal`]. A subset of [`PassLedger::drops`], never
    /// removed from it.
    pub fn refusals(&self) -> Vec<&Row> {
        self.drops()
            .into_iter()
            .filter(|r| r.outcome.is_refusal())
            .collect()
    }

    /// The drops that say something went wrong: the store, the layout or a
    /// recording's contents surprised us. The complement of
    /// [`PassLedger::refusals`] within [`PassLedger::drops`].
    pub fn failures(&self) -> Vec<&Row> {
        self.drops()
            .into_iter()
            .filter(|r| !r.outcome.is_refusal())
            .collect()
    }

    /// Whether the pass may report success.
    ///
    /// A pass that swallows a drop stops sealing something forever with only an
    /// unread log line as evidence, so a failure, an unreachable system, a
    /// missing roster and an accounting disagreement all fail it.
    ///
    /// A REFUSAL does not, and that is the one asymmetry here. It is still a
    /// drop, still named in the summary and still in the JSON; what it no
    /// longer does is make every subsequent pass indistinguishable from a
    /// broken one. See [`Outcome::is_refusal`] for why deterministic and
    /// permanent is the property that earns the exemption — and note the
    /// exemption is about the VERDICT only, never about visibility.
    pub fn clean(&self) -> bool {
        self.roster_error.is_none()
            && self.systems.iter().all(|s| s.unreachable.is_none())
            && self.failures().is_empty()
            && self.disagreements().is_empty()
    }
}

/// A system's bucket, its prefix inside it, and its key root, resolved from the
/// declared document.
///
/// Shared with the CLI's own scope resolution so a pass and a hand-run `seal`
/// cannot come to disagree about where a system's recordings are.
pub fn scope_for_system(system: &str) -> Result<(S3Config, String), String> {
    let mut cfg = S3Config::from_env();
    crate::RecordingBucket {
        bucket: crate::bucket_for_system(system)?,
        prefix: crate::prefix_for_system(system)?,
    }
    .apply(&mut cfg);
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
    seal_one_indexed_in(
        store,
        system,
        recording_id,
        root,
        "",
        quiet_after_secs,
        max_landing_bytes,
        None,
        "",
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn seal_one_indexed_in(
    store: &DynStore,
    system: &str,
    recording_id: &str,
    root: &str,
    scope_root: &str,
    quiet_after_secs: u64,
    max_landing_bytes: Option<u64>,
    index: Option<&deja_store::Store>,
    bucket: &str,
) -> Row {
    let outcome = seal_outcome_indexed_in(
        store,
        recording_id,
        root,
        scope_root,
        quiet_after_secs,
        max_landing_bytes,
        index,
        system,
        bucket,
    )
    .await
    .unwrap_or_else(|error| Outcome::Failed { error });
    Row {
        system: system.to_owned(),
        recording_id: recording_id.to_owned(),
        outcome,
    }
}

#[allow(clippy::too_many_arguments)]
async fn seal_outcome_indexed_in(
    store: &DynStore,
    recording_id: &str,
    root: &str,
    scope_root: &str,
    quiet_after_secs: u64,
    max_landing_bytes: Option<u64>,
    index: Option<&deja_store::Store>,
    system: &str,
    bucket: &str,
) -> Result<Outcome, String> {
    let mut cfg = S3Config::from_env();
    cfg.bucket = bucket.to_owned();
    let existing = if index.is_some() && crate::day::split_recording_id(recording_id).day.is_some()
    {
        None
    } else {
        crate::manifest_of(store, recording_id).await?
    };
    let readiness = crate::readiness_of(store, recording_id, root, quiet_after_secs).await?;
    let resealed = match crate::seal_decision(existing.as_ref(), &readiness) {
        SealDecision::AlreadyCurrent { source_objects } => {
            return Ok(Outcome::AlreadyCurrent { source_objects });
        }
        SealDecision::NotReady => {
            return Ok(Outcome::NotReady {
                state: readiness_state(&readiness).to_owned(),
                objects: readiness.objects(),
            });
        }
        SealDecision::Seal { resealing } => resealing,
    };
    if let Some(db) = index {
        if let Some(day) = crate::day::split_recording_id(recording_id).day {
            let session_id = crate::day::split_recording_id(recording_id).session_id;
            crate::materialize_indexed_days_scoped(
                store,
                bucket,
                system,
                root,
                scope_root,
                session_id,
                max_landing_bytes,
                db,
            )
            .await?;
            let scope = deja_store::LandingScope {
                system: system.to_owned(),
                bucket: bucket.to_owned(),
                root: scope_root.to_owned(),
                session_id: session_id.to_owned(),
            };
            let view = db
                .day_view_head(&scope, day)
                .await
                .map_err(|_| format!("read published day head {day} failed"))?
                .ok_or_else(|| format!("day {day} has no current published head"))?;
            let manifest: SessionManifest = serde_json::from_value(view.manifest)
                .map_err(|e| format!("decode published day manifest: {e}"))?;
            if manifest.status == "empty" {
                return Ok(Outcome::EmptyDay {
                    other_day_lines: manifest.counts.other_day_lines,
                    lines_without_ingress: manifest.counts.lines_without_ingress,
                });
            }
            return Ok(sealed_outcome(&manifest, resealed, &readiness, 0, 0, false));
        }
    }
    let compacted =
        crate::compact_session_inner(store, recording_id, root, max_landing_bytes).await?;
    match compacted {
        Compaction::Sealed {
            manifest,
            landing_bytes_read,
            landing_bytes_fetched,
            shared_prefix,
            ..
        } => Ok(sealed_outcome(
            &manifest,
            resealed,
            &readiness,
            landing_bytes_read,
            landing_bytes_fetched,
            shared_prefix,
        )),
        Compaction::RefusedTooLarge {
            budget_bytes,
            read_bytes,
            fetched_bytes,
            objects_read,
            objects_total,
            shared_prefix,
        } => Ok(Outcome::TooLarge {
            budget_bytes,
            read_bytes,
            fetched_bytes,
            objects_read,
            objects_total,
            shared_prefix,
        }),
        Compaction::RefusedSourceIncomplete { day, .. } => Ok(Outcome::Failed {
            error: format!("day {day} has correlated records with no ingress; no seal was written"),
        }),
        Compaction::EmptyDay {
            other_day_lines,
            lines_without_ingress,
            ..
        } => Ok(Outcome::EmptyDay {
            other_day_lines,
            lines_without_ingress,
        }),
    }
}

fn sealed_outcome(
    manifest: &SessionManifest,
    resealed: bool,
    readiness: &SealReadiness,
    landing_bytes_read: u64,
    landing_bytes_fetched: u64,
    shared_prefix: bool,
) -> Outcome {
    Outcome::Sealed {
        correlations: manifest.counts.correlations,
        landing_objects: manifest.counts.landing_objects,
        landing_bytes_read,
        landing_bytes_fetched,
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
#[cfg(test)]
pub(crate) async fn plan_in(store: &DynStore, root: &str) -> Result<Vec<String>, String> {
    let keys: Vec<String> = crate::list_keys(store, root)
        .await?
        .into_iter()
        .map(|p| p.as_ref().to_owned())
        .collect();
    let landed = crate::index_landed_keys(root, &keys);
    let ids: Vec<String> = landed.iter().map(|r| r.session_id.clone()).collect();
    // Sealing does not need to tell "not sealed" from "could not tell": either
    // way there is a seal to attempt, so both plan the recording the same way
    // the collapsed shape used to.
    let manifests = crate::manifests_of(store, &ids).await;
    let mut planned = Vec::new();
    for (rec, manifest) in landed.into_iter().zip(manifests) {
        match manifest {
            Err(error) => {
                return Err(format!(
                    "read existing manifest for {}: {error}",
                    rec.session_id
                ));
            }
            Ok(None) => planned.push(rec.session_id),
            Ok(Some(m)) => {
                let dependency_objects = crate::day::split_recording_id(&rec.session_id)
                    .day
                    .map_or(0, |day| {
                        keys.iter()
                            .filter(|key| {
                                crate::key_names_session(
                                    key,
                                    crate::day::split_recording_id(&rec.session_id).session_id,
                                ) && crate::key_date(key).is_some_and(|date| date != day)
                            })
                            .count()
                    });
                if rec.objects != m.counts.landing_objects
                    || dependency_objects != m.counts.day_dependency_objects
                {
                    planned.push(rec.session_id);
                }
            }
        }
    }
    Ok(planned)
}

/// One system's pass, isolated: anything that goes wrong here is recorded in
/// this system's ledger and never propagated to the walk.
#[cfg(test)]
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
    let runtime = match crate::runtime() {
        Ok(runtime) => runtime,
        Err(error) => return unreachable(error),
    };
    let db_url = match std::env::var("DEJA_DB_URL")
        .ok()
        .filter(|url| !url.trim().is_empty())
    {
        Some(url) => url,
        None => return unreachable("DEJA_DB_URL is required for durable day indexing".to_owned()),
    };
    let db = match runtime.block_on(deja_store::Store::connect(&db_url)) {
        Ok(db) => db,
        Err(_) => return unreachable("connect durable index failed".to_owned()),
    };
    runtime.block_on(async {
        let scope_root = cfg.bucket_key(&root);
        let known_generations = match db.active_landing_session_generations(system, &cfg.bucket, &scope_root).await {
            Ok(generations) => generations,
            Err(error) => return unreachable(format!("read indexed sessions before S3 LIST: {error}")),
        };
        let prefix = object_store::path::Path::from(root.as_str());
        let metas: Vec<object_store::ObjectMeta> = match store.list(Some(&prefix)).try_collect().await {
            Ok(metas) => metas,
            Err(error) => return unreachable(format!("list system landing {root}: {error}")),
        };
        let keys: Vec<String> = metas.iter().map(|meta| meta.location.as_ref().to_owned()).collect();
        let timestamps: std::collections::BTreeMap<&str, i64> = metas.iter().map(|meta| {
            (meta.location.as_ref(), meta.last_modified.timestamp())
        }).collect();
        let mut sessions: std::collections::BTreeSet<String> = crate::index_landed_keys(&root, &keys)
            .into_iter()
            .map(|recording| {
                crate::day::split_recording_id(&recording.session_id)
                    .session_id
                    .to_owned()
            })
            .collect();
        sessions.extend(known_generations.keys().cloned());
        let mut ledger = SystemLedger {
            system: system.to_owned(),
            ..SystemLedger::default()
        };
        // A session absent from S3 remains in the indexed set until this pass
        // records its source deletion; it must not retain a falsely current head.
        for session in sessions {
            let (scope, days, fingerprints) = match crate::index_landing_session_from_listing(
                &store,
                &cfg.bucket,
                system,
                &scope_root,
                &session,
                max_landing_bytes,
                &db,
                &metas,
                known_generations.get(&session).copied(),
            )
            .await
            {
                Ok(indexed) => indexed,
                Err(error) => {
                    let row = failed_row(system, &session, error);
                    observe(&row);
                    ledger.planned.push(session);
                    ledger.rows.push(row);
                    continue;
                }
            };
            if !metas.iter().any(|meta| crate::key_names_session(meta.location.as_ref(), &session)) {
                let row = failed_row(
                    system,
                    &session,
                    "all raw source objects for this indexed session disappeared; current day views are unavailable".into(),
                );
                observe(&row);
                ledger.planned.push(session);
                ledger.rows.push(row);
                continue;
            }
            if days.is_empty() {
                let row = seal_one_in(
                    &store,
                    system,
                    &session,
                    &root,
                    quiet_after_secs,
                    max_landing_bytes,
                )
                .await;
                observe(&row);
                ledger.planned.push(session);
                ledger.rows.push(row);
                continue;
            }
            for day in &days {
                let current = match db.day_view_head(&scope, &day.day).await {
                    Ok(current) => current,
                    Err(error) => {
                        let id = crate::day::day_recording_id(&session, &day.day);
                        let row =
                            failed_row(system, &id, format!("read indexed day head: {error}"));
                        observe(&row);
                        ledger.planned.push(id);
                        ledger.rows.push(row);
                        continue;
                    }
                };
                if current
                    .as_ref()
                    .is_some_and(|view| view.dependency_token == day.dependency_token)
                {
                    continue;
                }
                let id = crate::day::day_recording_id(&session, &day.day);
                ledger.planned.push(id.clone());
                let outcome = async {
                    let newest = {
                        let mut newest = 0;
                        for key in &day.object_keys {
                            let timestamp = timestamps.get(key.as_str())
                                .ok_or_else(|| format!("indexed day dependency {key} disappeared from the source listing"))?;
                            newest = newest.max(*timestamp);
                        }
                        newest
                    };
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map_err(|e| format!("clock before Unix epoch: {e}"))?
                        .as_secs() as i64;
                    if now.saturating_sub(newest).max(0) < quiet_after_secs as i64 {
                        return Ok(Outcome::NotReady {
                            state: "active".to_owned(),
                            objects: day.object_keys.len(),
                        });
                    }
                    let resealed = db.has_day_view_history(&scope, &day.day).await
                        .map_err(|e| format!("read prior day-view history: {e}"))?;
                    let eligible = std::iter::once(day.day.clone()).collect();
                    let (written, bytes_read, bytes_fetched) = crate::materialize_indexed_days_from_index(
                        &store,
                        max_landing_bytes,
                        &db,
                        &scope,
                        &days,
                        &fingerprints,
                        &eligible,
                    )
                    .await?;
                    if written == 0 {
                        return Err(format!("day {} source changed during publication", day.day));
                    }
                    let view = db
                        .day_view_head(&scope, &day.day)
                        .await
                        .map_err(|e| format!("read published day head: {e}"))?
                        .ok_or_else(|| format!("published day {} is not current", day.day))?;
                    let manifest: SessionManifest = serde_json::from_value(view.manifest)
                        .map_err(|e| format!("decode published day manifest: {e}"))?;
                    if manifest.status == "empty" {
                        Ok(Outcome::EmptyDay {
                            other_day_lines: manifest.counts.other_day_lines,
                            lines_without_ingress: manifest.counts.lines_without_ingress,
                        })
                    } else {
                        Ok(Outcome::Sealed {
                            correlations: manifest.counts.correlations,
                            landing_objects: manifest.counts.landing_objects,
                            landing_bytes_read: bytes_read,
                            landing_bytes_fetched: bytes_fetched,
                            shared_prefix: false,
                            resealed,
                            instances_without_eof: Vec::new(),
                        })
                    }
                }
                .await
                .unwrap_or_else(|error: String| Outcome::Failed { error });
                let row = Row {
                    system: system.to_owned(),
                    recording_id: id,
                    outcome,
                };
                observe(&row);
                ledger.rows.push(row);
            }
        }
        ledger
    })
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

    /// Land a GZIP object, which is what the deployed aggregators write when
    /// `compression: gzip` is set. The only fixture in which a landing's
    /// fetched and held sizes differ — every plain fixture makes them equal,
    /// so a row that reported one in the other's slot would look right in all
    /// of them.
    fn land_gzip(store: &DynStore, session: &str, object: usize, lines: &[String]) -> (u64, u64) {
        use std::io::Write as _;
        let payload = lines.join("\n").into_bytes();
        let mut gz = Vec::new();
        {
            let mut enc = flate2::write::GzEncoder::new(&mut gz, flate2::Compression::default());
            enc.write_all(&payload).unwrap();
            enc.finish().unwrap();
        }
        let key = format!("{ROOT}/session={session}/inst=i1/part-{object}.log.gz");
        let stored = gz.len() as u64;
        block(crate::put(store, &key, gz)).unwrap();
        (stored, payload.len() as u64)
    }

    /// Land under a DATE partition, the layout the deployed Vector aggregator
    /// actually writes. A session under two of them is addressed at the root.
    fn land_dated(store: &DynStore, date: &str, session: &str, object: usize, lines: &[String]) {
        let key = format!("{ROOT}/dt={date}/session={session}/inst=i1/part-{object}.json");
        block(crate::put(store, &key, lines.join("\n").into_bytes())).unwrap();
    }

    /// The ROW reports the two sizes in the right slots.
    ///
    /// `sealed_outcome` takes both as bare `u64` and hands them to a struct
    /// whose fields are also both `u64`, so swapping them is invisible to the
    /// compiler and to every plain fixture — where the two numbers are equal
    /// by construction. This is the only pass-level test where they differ,
    /// which makes it the only one that can tell the slots apart.
    #[test]
    fn a_sealed_row_reports_fetched_and_held_in_the_right_slots() {
        let store = store();
        let lines: Vec<String> = (0..40).map(|n| envelope("big", n as u64, 200)).collect();
        let (stored, decompressed) = land_gzip(&store, "big", 0, &lines);
        assert!(
            stored < decompressed,
            "the fixture must actually compress, or it proves nothing: {stored} vs {decompressed}"
        );

        let row = block(seal_one_in(&store, "sys", "big", ROOT, 0, None));
        match row.outcome {
            Outcome::Sealed {
                landing_bytes_read,
                landing_bytes_fetched,
                ..
            } => {
                assert_eq!(
                    landing_bytes_read, decompressed,
                    "held = the decompressed payload"
                );
                assert_eq!(landing_bytes_fetched, stored, "fetched = the bytes stored");
            }
            other => panic!("expected a seal, got {other:?}"),
        }
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
                landing_bytes_fetched: 0,
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

    /// A refusal does not fail the pass, and is not hidden by that.
    ///
    /// The two halves are one test on purpose. Exempting a refusal from the
    /// verdict is only safe while it stays visible, so a change that made the
    /// pass green by dropping the row would satisfy the first half and has to
    /// be caught by the second.
    ///
    /// Concretely: eight August recordings past the budget made every sealer
    /// pass exit non-zero forever, so `lastSuccessfulTime` was permanently null
    /// and a genuinely broken pass looked exactly like a working one.
    #[test]
    fn a_refusal_does_not_fail_the_pass_but_is_still_reported() {
        let refused = PassLedger {
            systems: vec![SystemLedger {
                system: "sys".to_owned(),
                planned: vec!["r1".to_owned()],
                rows: vec![row(
                    "r1",
                    Outcome::TooLarge {
                        budget_bytes: 536_870_912,
                        read_bytes: 561_616_085,
                        // Illustrative, not measured: no fetched figure exists for this
                        // refusal, so it is set equal to read_bytes (an uncompressed
                        // landing). This test does not assert on it.
                        fetched_bytes: 561_616_085,
                        objects_read: 18,
                        objects_total: 67,
                        shared_prefix: false,
                    },
                )],
                unreachable: None,
            }],
            roster_error: None,
        };
        assert!(
            refused.clean(),
            "a determination the pass made is not a broken pass"
        );
        // Still a drop, still enumerable, still counted. This is the half that
        // keeps the exemption from becoming a swallow.
        assert_eq!(refused.drops().len(), 1, "a refusal remains a drop");
        assert_eq!(refused.refusals().len(), 1);
        assert!(refused.failures().is_empty());
        let t = refused.totals();
        assert_eq!(
            (t.dropped, t.refused, t.failed),
            (1, 1, 0),
            "dropped stays the total so an old reader loses nothing"
        );

        // A real failure alongside a refusal still fails, and the two are
        // counted apart so the message cannot report one as the other.
        let mut both = refused.clone();
        both.systems[0].planned.push("r2".to_owned());
        both.systems[0].rows.push(row(
            "r2",
            Outcome::Failed {
                error: "s3 list: refused".to_owned(),
            },
        ));
        assert!(!both.clean(), "a failure still fails the pass");
        let t = both.totals();
        assert_eq!((t.dropped, t.refused, t.failed), (2, 1, 1));
    }

    #[test]
    fn a_refusal_line_says_what_the_landing_cost_to_fetch() {
        let refused = row(
            "r1",
            Outcome::TooLarge {
                budget_bytes: 1_000,
                read_bytes: 4_321,
                fetched_bytes: 987,
                objects_read: 2,
                objects_total: 5,
                shared_prefix: false,
            },
        );
        let line = refused.to_string();
        assert!(line.contains("4321 byte(s) held"), "{line}");
        assert!(line.contains("from 987 fetched"), "{line}");
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

    /// The sealer, run through a system's prefixed store, plans from the
    /// landing under the prefix and writes its seal there, and nowhere else.
    #[test]
    fn a_pass_over_a_prefixed_system_seals_under_its_prefix() {
        let bucket = store();
        let mut cfg = S3Config::from_env();
        cfg.prefix = "prism/".to_owned();
        let scoped = cfg.scope_store(bucket.clone());
        land(&scoped, "r1", 0, &[envelope("r1", 1, 0), eof("r1")]);

        let ledger = block(seal_system_in(&scoped, "prism", ROOT, 0, None, &mut |_| {}));
        assert_eq!(ledger.planned, vec!["r1".to_owned()]);
        assert!(
            matches!(ledger.rows.as_slice(), [r] if matches!(r.outcome, Outcome::Sealed { .. })),
            "{ledger:?}"
        );

        let keys: Vec<String> = block(crate::list_keys(&bucket, ""))
            .unwrap()
            .into_iter()
            .map(|p| p.as_ref().to_owned())
            .collect();
        assert!(
            keys.contains(&format!("prism/{}", crate::layout::manifest_key("r1"))),
            "{keys:?}"
        );
        assert!(keys.iter().all(|k| k.starts_with("prism/")), "{keys:?}");
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
                fetched_bytes: _,
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
    fn a_session_across_midnight_is_not_charged_for_its_neighbours() {
        // This exact fixture used to produce a `too_large` DROP. A session
        // under two date partitions was addressed at their PARENT, so
        // compaction listed and decompressed every other session that landed
        // in that window and refused on bytes that were never its own —
        // 32,519 objects for a two-object recording, in sandbox.
        //
        // Addressing each partition separately makes it what it always was.
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
        // Large enough that reading the parent passes the budget, and it sorts
        // BETWEEN the two partitions, so a root-addressed read reaches it
        // before it finishes the session.
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
            Outcome::Sealed {
                landing_objects,
                landing_bytes_read,
                shared_prefix,
                ..
            } => {
                assert_eq!(
                    *landing_objects, 2,
                    "its own two objects, not the partition parent's five"
                );
                assert!(
                    !shared_prefix,
                    "a per-partition prefix names the session, so nothing else is under it"
                );
                assert!(
                    *landing_bytes_read < 6000,
                    "read {landing_bytes_read} bytes; the neighbour was never fetched"
                );
            }
            other => panic!("expected a seal, got {other:?}"),
        }
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

    /// 2026-10-07T23:59:58Z and 2026-10-08T00:00:03Z, in nanoseconds.
    const BEFORE_MIDNIGHT: u64 = 1_791_417_598_000_000_000;
    const AFTER_MIDNIGHT: u64 = 1_791_417_603_000_000_000;

    fn timed(session: &str, gseq: u64, corr: &str, rseq: u64, ns: u64) -> String {
        format!(
            r#"{{"schema_version":2,"artifact_type":"deja_artifact_record","instance_id":"i1","capture":{{"mode":"session","session_id":"{session}"}},"code":{{"sha":"abc","deja_version":"0.1.0"}},"event":{{"recording_run_id":"{session}","global_sequence":{gseq},"request_sequence":{rseq},"correlation_id":"{corr}","boundary":"b","timestamp_ns":{ns}}}}}"#
        )
    }

    fn correlations_of(store: &DynStore, id: &str) -> Vec<String> {
        let manifest = block(crate::manifest_of(store, id)).unwrap().unwrap();
        let lines = block(crate::session_lines(store, &manifest)).unwrap();
        let mut out: Vec<String> = lines
            .iter()
            .map(|l| {
                let v: serde_json::Value = serde_json::from_str(l).unwrap();
                format!(
                    "{}#{}",
                    v["event"]["correlation_id"].as_str().unwrap(),
                    v["event"]["global_sequence"]
                )
            })
            .collect();
        out.sort();
        out
    }

    /// A pod's session runs across midnight. The pass seals it as two days,
    /// each holding the requests that BEGAN on it — the one in flight at
    /// midnight goes whole into the earlier day, tail included — and the next
    /// pass has nothing left to plan.
    #[test]
    fn a_session_across_midnight_seals_as_one_recording_per_day() {
        let store = store();
        land_dated(
            &store,
            "2026-10-07",
            "s1",
            0,
            &[
                timed("s1", 0, "c0", 0, BEFORE_MIDNIGHT - 60_000_000_000),
                timed("s1", 1, "c1", 0, BEFORE_MIDNIGHT),
            ],
        );
        land_dated(
            &store,
            "2026-10-08",
            "s1",
            1,
            &[
                timed("s1", 2, "c2", 0, AFTER_MIDNIGHT),
                timed("s1", 3, "c1", 1, AFTER_MIDNIGHT),
                timed("s1", 4, "c2", 1, AFTER_MIDNIGHT),
            ],
        );

        let ledger = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |_| {}));
        assert_eq!(ledger.planned, ["s1.2026-10-08", "s1.2026-10-07"]);
        assert!(ledger.disagreements().is_empty());
        assert!(
            ledger
                .rows
                .iter()
                .all(|r| matches!(r.outcome, Outcome::Sealed { .. })),
            "{:?}",
            ledger.rows
        );

        assert_eq!(
            correlations_of(&store, "s1.2026-10-07"),
            ["c0#0", "c1#1", "c1#3"],
            "the request in flight at midnight is sealed whole with the day it began"
        );
        assert_eq!(correlations_of(&store, "s1.2026-10-08"), ["c2#2", "c2#4"]);

        let earlier = block(crate::manifest_of(&store, "s1.2026-10-07"))
            .unwrap()
            .unwrap();
        let later = block(crate::manifest_of(&store, "s1.2026-10-08"))
            .unwrap()
            .unwrap();
        // Every input line across all listed partitions is kept or explicitly
        // attributed to another day/request with no visible ingress.
        let named = |c: &crate::Counts| (c.lines_in, c.other_day_lines, c.lines_without_ingress);
        assert_eq!(named(&earlier.counts), (3, 2, 0));
        assert_eq!(named(&later.counts), (2, 3, 0));
        // A day's own partition is its landing; the next one is only read.
        assert_eq!(earlier.counts.landing_objects, 1);
        assert_eq!(later.counts.landing_objects, 1);
        // gseq 2 is the later day's and 3 is the earlier day's: neither is a
        // hole in the other.
        assert!(
            earlier.instances[0].gaps.is_empty(),
            "{:?}",
            earlier.instances
        );
        assert!(later.instances[0].gaps.is_empty(), "{:?}", later.instances);
        assert!(block(crate::manifest_of(&store, "s1")).unwrap().is_none());

        let again = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |_| {}));
        assert!(again.planned.is_empty(), "{:?}", again.planned);
        // A request has no duration bound: a late tail may arrive several
        // partitions after its ingress. The old day's manifest must be
        // reconsidered from the dependency partition count and refreshed.
        land_dated(
            &store,
            "2026-10-09",
            "s1",
            2,
            &[timed("s1", 5, "c1", 2, AFTER_MIDNIGHT + 86_400_000_000_000)],
        );
        let planned = block(plan_in(&store, ROOT)).unwrap();
        assert!(planned.contains(&"s1.2026-10-07".to_owned()), "{planned:?}");
        let update = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |_| {}));
        assert!(
            update.rows.iter().any(|r| {
                r.recording_id == "s1.2026-10-07"
                    && matches!(r.outcome, Outcome::Sealed { resealed: true, .. })
            }),
            "{:?}",
            update.rows
        );
        assert_eq!(
            correlations_of(&store, "s1.2026-10-07"),
            ["c0#0", "c1#1", "c1#3", "c1#5"]
        );
    }

    /// A day whose partition holds only a tail publishes a terminal empty
    /// manifest, so later readers cannot replay a previously assigned stale tape.
    #[test]
    fn a_day_holding_only_anothers_tail_is_named_not_dropped() {
        let store = store();
        land_dated(
            &store,
            "2026-10-07",
            "s1",
            0,
            &[timed("s1", 0, "c1", 0, BEFORE_MIDNIGHT)],
        );
        land_dated(
            &store,
            "2026-10-08",
            "s1",
            1,
            &[timed("s1", 1, "c1", 1, AFTER_MIDNIGHT)],
        );

        let ledger = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |_| {}));
        let row = |id: &str| {
            &ledger
                .rows
                .iter()
                .find(|r| r.recording_id == id)
                .unwrap()
                .outcome
        };
        assert_eq!(
            row("s1.2026-10-08"),
            &Outcome::EmptyDay {
                other_day_lines: 2,
                lines_without_ingress: 0
            }
        );
        assert!(!row("s1.2026-10-08").is_drop());
        assert!(matches!(row("s1.2026-10-07"), Outcome::Sealed { .. }));
        assert_eq!(correlations_of(&store, "s1.2026-10-07"), ["c1#0", "c1#1"]);
        let empty = block(crate::manifest_of(&store, "s1.2026-10-08"))
            .unwrap()
            .unwrap();
        assert_eq!(empty.status, "empty");
        assert!(empty.data_parts.is_empty());
        assert_eq!(empty.counts.correlations, 0);
        let again = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |_| {}));
        assert!(again.planned.is_empty(), "{:?}", again.planned);
        land_dated(
            &store,
            "2026-10-08",
            "s1",
            2,
            &[timed("s1", 2, "c2", 0, AFTER_MIDNIGHT)],
        );
        let reopened = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |_| {}));
        assert!(matches!(
            reopened
                .rows
                .iter()
                .find(|r| r.recording_id == "s1.2026-10-08")
                .unwrap()
                .outcome,
            Outcome::Sealed { .. }
        ));
        let reopened = block(crate::manifest_of(&store, "s1.2026-10-08"))
            .unwrap()
            .unwrap();
        assert_eq!(reopened.status, "sealed");
        assert_eq!(correlations_of(&store, "s1.2026-10-08"), ["c2#2"]);
    }

    #[test]
    fn late_ingress_unblocks_days_without_publishing_an_orphan_tape() {
        for include_day_two_tail in [false, true] {
            let store = store();
            land_dated(
                &store,
                "2026-10-07",
                "s1",
                0,
                &[timed("s1", 0, "c0", 0, BEFORE_MIDNIGHT)],
            );
            land_dated(
                &store,
                "2026-10-08",
                "s1",
                1,
                &[timed("s1", 1, "orphan", 1, AFTER_MIDNIGHT)],
            );
            if include_day_two_tail {
                land_dated(
                    &store,
                    "2026-10-09",
                    "s1",
                    2,
                    &[timed(
                        "s1",
                        3,
                        "orphan",
                        2,
                        AFTER_MIDNIGHT + 86_400_000_000_000,
                    )],
                );
            }
            let initial = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |_| {}));
            for day in ["2026-10-07", "2026-10-08"] {
                let id = format!("s1.{day}");
                let outcome = &initial
                    .rows
                    .iter()
                    .find(|row| row.recording_id == id)
                    .expect("orphan day was accounted for")
                    .outcome;
                assert!(
                    matches!(outcome, Outcome::Failed { error } if error.contains("no ingress"))
                );
                assert!(block(crate::manifest_of(&store, &id)).unwrap().is_none());
            }

            land_dated(
                &store,
                "2026-10-07",
                "s1",
                2,
                &[timed("s1", 2, "orphan", 0, BEFORE_MIDNIGHT)],
            );
            let revised = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |_| {}));
            assert!(revised.disagreements().is_empty());
            assert!(matches!(
                revised
                    .rows
                    .iter()
                    .find(|r| r.recording_id == "s1.2026-10-08")
                    .unwrap()
                    .outcome,
                Outcome::EmptyDay { .. }
            ));
            let empty = block(crate::manifest_of(&store, "s1.2026-10-08"))
                .unwrap()
                .unwrap();
            assert_eq!(empty.status, "empty");
            assert!(empty.data_parts.is_empty());
            assert_eq!(empty.counts.correlations, 0);
            assert!(block(crate::session_lines(&store, &empty))
                .unwrap()
                .is_empty());
        }
    }

    /// Dependency snapshots cover every non-own partition while readiness
    /// remains based only on the day's own partition.
    #[test]
    fn a_days_readiness_counts_only_its_own_partition() {
        let store = store();
        land_dated(
            &store,
            "2026-10-07",
            "s1",
            0,
            &[timed("s1", 0, "c1", 0, BEFORE_MIDNIGHT)],
        );
        land_dated(
            &store,
            "2026-10-08",
            "s1",
            1,
            &[timed("s1", 1, "c2", 0, AFTER_MIDNIGHT)],
        );
        land_dated(
            &store,
            "2026-10-08",
            "s1",
            2,
            &[timed("s1", 2, "c3", 0, AFTER_MIDNIGHT)],
        );
        let readiness = block(crate::readiness_of(&store, "s1.2026-10-07", ROOT, 0)).unwrap();
        assert_eq!(readiness.objects(), 1, "{readiness:?}");
        assert_eq!(readiness.dependency_objects(), 2, "{readiness:?}");
        let readiness = block(crate::readiness_of(&store, "s1.2026-10-08", ROOT, 0)).unwrap();
        assert_eq!(readiness.objects(), 2, "{readiness:?}");
        assert_eq!(readiness.dependency_objects(), 1, "{readiness:?}");
        assert_eq!(
            block(crate::readiness_of(&store, "s1.2026-10-09", ROOT, 0)).unwrap(),
            SealReadiness::Absent,
            "a day the session never wrote is not there"
        );
    }

    /// A session within one partition keeps its bare id, so every recording
    /// sealed before days were split still reads, and is not sealed again.
    #[test]
    fn a_session_within_one_day_keeps_its_id() {
        let store = store();
        land_dated(
            &store,
            "2026-10-07",
            "s1",
            0,
            &[timed("s1", 0, "c1", 0, BEFORE_MIDNIGHT)],
        );
        let ledger = block(seal_system_in(&store, "sys", ROOT, 0, None, &mut |_| {}));
        assert_eq!(ledger.planned, ["s1"]);
        assert!(block(crate::manifest_of(&store, "s1")).unwrap().is_some());
    }
}
