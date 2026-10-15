//! Postgres state store for the replay harness.
//!
//! Owns the single migration set (master design §3 / Phase 1 §W3) and the
//! query surface the orchestrator needs: runs + stage history + log chunks +
//! artifacts + the recordings catalog + the append-only audit log.
//!
//! Connection is optional by design in local mode: the orchestrator runs
//! file-backed (legacy routes) even without a database, and dual-writes here
//! when connected. `DEJA_DB_URL` selects the database; the demo scripts boot
//! a dedicated `deja-orchestrator-pg` container (see demo/docker-compose.
//! orchestrator.yml) on a non-default port so nothing collides with the
//! per-run demo stack.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;

pub const DEFAULT_DB_URL: &str = "postgres://deja:deja@127.0.0.1:55432/deja";

/// Re-export so callers can name store errors without depending on sqlx.
pub use sqlx::Error as StoreError;

#[derive(Debug, Clone)]
pub struct Store {
    pool: PgPool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordingRow {
    pub recording_id: String,
    pub kind: String,
    pub source_path: Option<String>,
    pub event_count: Option<i64>,
    pub correlation_count: Option<i64>,
    pub byte_size: Option<i64>,
    pub status: String,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    /// The compactor's session manifest (None until the session is sealed).
    pub manifest: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunRow {
    pub run_id: String,
    pub mode: String,
    pub recording_id: Option<String>,
    pub candidate: serde_json::Value,
    pub candidate_sha256: Option<String>,
    pub params: serde_json::Value,
    pub state: String,
    pub verdict: Option<String>,
    pub scorecard: Option<serde_json::Value>,
    pub failure: Option<serde_json::Value>,
    pub expectation: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    /// What the run changed relative to the baseline it was created against:
    /// 'pass' | 'fail' | 'pending'. None when it names no baseline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_verdict: Option<String>,
}

/// A run as the LIST renders it: every column except `scorecard`, plus the few
/// scalars the list actually decides on out of it.
///
/// The scorecard is deliberately absent rather than emptied. On 2026-09-17 the
/// orchestrator entered a CrashLoopBackOff — OOMKilled at a 2Gi limit, ten
/// restarts, roughly two minutes of life each — because `GET /api/v1/runs`
/// returned 46.5 MB and the dashboard polls it every five seconds from every
/// open tab on every machine. 97.5% of those bytes were `scorecard`, and
/// `per_correlation` alone was 36.8 MB of it. The list renders ONE field out of
/// the whole structure.
///
/// The cost was not the bytes on the wire but the three materialisations behind
/// them: sqlx decodes the JSONB column into a `serde_json::Value` tree (larger
/// than its own text, since every string and map node carries allocation
/// overhead), the handler then deep-CLONES that tree with `to_value`, and only
/// then serialises it. One request was hundreds of megabytes; four concurrent
/// ones were the pod.
///
/// So the column is not selected at all, and the five scalars `resultOf` reads
/// are extracted by Postgres into `scorecard_digest`. Nothing large is read,
/// cloned, or sent. A caller that wants a scorecard asks for one run — `GET
/// /runs/{id}` and `GET /runs/{id}/scorecard` both still carry it whole, which
/// is what the report page has always used.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunSummaryRow {
    pub run_id: String,
    pub mode: String,
    pub recording_id: Option<String>,
    pub candidate: serde_json::Value,
    pub candidate_sha256: Option<String>,
    pub params: serde_json::Value,
    pub state: String,
    pub verdict: Option<String>,
    /// The five scalars the list's result badge decides on, extracted in SQL,
    /// and `None` when there is no scorecard at all.
    ///
    /// A DIGEST under its own name, not a stubbed `scorecard`: a value shaped
    /// like a scorecard but holding five fields would read as a scorecard to
    /// every consumer and be wrong for all of them but this one. Under its own
    /// name it says what it is, and widening it is a deliberate act.
    ///
    /// `None` is load-bearing and not merely "empty". `resultOf` in the
    /// dashboard distinguishes "completed but produced no scorecard" (a warning
    /// — terminal with no evidence) from "scored", and it makes that call on
    /// the absence of this value. A digest of nulls would collapse the two.
    pub scorecard_digest: Option<serde_json::Value>,
    pub failure: Option<serde_json::Value>,
    pub expectation: Option<String>,
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_verdict: Option<String>,
}

/// A run that has NOT reached a terminal state — the k8s reconciler's work
/// list. Minimal by design: the reconciler only needs the id (to correlate
/// with its Job), the current state (for log context), and how old the run is
/// (to age out orphans).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveRun {
    pub run_id: String,
    pub state: String,
    /// Seconds since the run row was created, measured by the DATABASE clock
    /// (`now() - created_at`), so the reconciler's grace period is immune to
    /// app/DB clock skew. Clamped non-negative by the consumer.
    pub age_secs: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StageRow {
    pub id: i64,
    pub run_id: String,
    pub stage: String,
    pub status: String,
    pub step: Option<i32>,
    pub steps_total: Option<i32>,
    pub started_at: DateTime<Utc>,
    pub finished_at: Option<DateTime<Utc>>,
    pub detail: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactRow {
    pub id: i64,
    pub run_id: Option<String>,
    pub recording_id: Option<String>,
    pub kind: String,
    pub uri: String,
    pub bytes: Option<i64>,
    pub sha256: Option<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRow {
    pub id: i64,
    pub ts: DateTime<Utc>,
    pub actor: String,
    pub action: String,
    pub object_type: String,
    pub object_id: String,
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AcknowledgementRow {
    pub id: i64,
    pub repo: String,
    pub pr_number: i64,
    pub change_id: String,
    pub lane: Option<serde_json::Value>,
    pub pattern: serde_json::Value,
    pub value_hash: Option<String>,
    pub note: String,
    pub origin_run_id: String,
    pub proposed_by: String,
    pub proposed_at: DateTime<Utc>,
    pub acknowledged_by: Option<String>,
    pub acknowledged_at: Option<DateTime<Utc>>,
    pub withdrawn_by: Option<String>,
    pub withdrawn_at: Option<DateTime<Utc>>,
    /// The sign-in issuer's stable id beside each name; null when the
    /// action was taken with sign-in off or by the service token. Never
    /// serialised: a Google subject is a stable account id across
    /// services, and no reader of the API needs it.
    #[serde(default, skip_serializing)]
    pub proposed_by_sub: Option<String>,
    #[serde(default, skip_serializing)]
    pub acknowledged_by_sub: Option<String>,
    #[serde(default, skip_serializing)]
    pub withdrawn_by_sub: Option<String>,
}

/// One divergence to acknowledge: the row's key, and its value for display.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewAcknowledgement {
    pub lane: Option<serde_json::Value>,
    pub pattern: serde_json::Value,
    pub value_hash: Option<String>,
}

/// One thing a system can run: a merge (one pull request, named by its short
/// sha) or a release (a CalVer tag on one of those commits).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdVersionRow {
    pub system: String,
    pub version: String,
    pub kind: String,
    pub commit_sha: Option<String>,
    pub image_tag: Option<String>,
    pub chart: Option<String>,
    /// For a release, the merge version it was tagged on.
    pub released_from: Option<String>,
    pub merged_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    /// What the version carries, as posted with it.
    #[serde(default)]
    pub prs: Vec<CdVersionPr>,
}

/// A version as the pipeline posts it: everything but what the database
/// fills in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewCdVersion {
    pub system: String,
    pub version: String,
    pub kind: String,
    #[serde(default)]
    pub commit_sha: Option<String>,
    #[serde(default)]
    pub image_tag: Option<String>,
    #[serde(default)]
    pub chart: Option<String>,
    #[serde(default)]
    pub released_from: Option<String>,
    #[serde(default)]
    pub merged_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub prs: Vec<CdVersionPr>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdVersionPr {
    pub repo: String,
    pub pr_number: i64,
    pub title: Option<String>,
    pub author: Option<String>,
    pub merged_at: Option<DateTime<Utc>>,
}

/// One change to what an environment is asked to run or observed running.
/// Append-only; an environment's state is its newest row.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdDeploymentRow {
    pub id: i64,
    pub system: String,
    pub environment: String,
    pub version: String,
    pub state: String,
    pub requested_by: String,
    /// The sign-in subject beside the name, never serialised (see
    /// `AcknowledgementRow`).
    #[serde(default, skip_serializing)]
    pub requested_by_sub: Option<String>,
    pub request_id: Option<i64>,
    pub jenkins_build: Option<String>,
    pub detail: serde_json::Value,
    pub at: DateTime<Utc>,
}

/// Who asked for a version on an environment: the job's actor and build,
/// from its newest `requested`/`committed` row since the previous version
/// went live. Copied onto the observation row that records the arrival, so
/// a `live` row says who it was for without a second lookup.
#[derive(Debug, Clone)]
pub struct CdRequester {
    pub requested_by: String,
    pub requested_by_sub: Option<String>,
    pub jenkins_build: Option<String>,
}

/// A deployment row to append, everything but what the database fills in.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewCdDeployment {
    pub system: String,
    pub environment: String,
    pub version: String,
    pub state: String,
    pub requested_by: String,
    #[serde(default)]
    pub requested_by_sub: Option<String>,
    #[serde(default)]
    pub request_id: Option<i64>,
    #[serde(default)]
    pub jenkins_build: Option<String>,
    #[serde(default)]
    pub detail: serde_json::Value,
}

/// A system's pause switch (migration 0011). No row reads as running.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdControlRow {
    pub system: String,
    pub paused: bool,
    pub reason: Option<String>,
    pub changed_by: String,
    /// The sign-in subject beside the name, never serialised.
    #[serde(default, skip_serializing)]
    pub changed_by_sub: Option<String>,
    pub changed_at: DateTime<Utc>,
}

impl Store {
    /// Connect and run migrations. Errors are returned (the caller decides
    /// whether the store is mandatory).
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(url)
            .await?;
        sqlx::migrate!("./migrations").run(&pool).await?;
        Ok(Self { pool })
    }

    // -- audit ---------------------------------------------------------------

    pub async fn audit(
        &self,
        actor: &str,
        action: &str,
        object_type: &str,
        object_id: &str,
        params: &serde_json::Value,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO audit_events (actor, action, object_type, object_id, params)
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(actor)
        .bind(action)
        .bind(object_type)
        .bind(object_id)
        .bind(params)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn audit_list(&self, limit: i64) -> Result<Vec<AuditRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, ts, actor, action, object_type, object_id, params
             FROM audit_events ORDER BY id DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| AuditRow {
                id: r.get(0),
                ts: r.get(1),
                actor: r.get(2),
                action: r.get(3),
                object_type: r.get(4),
                object_id: r.get(5),
                params: r.get(6),
            })
            .collect())
    }

    // -- acknowledgements ----------------------------------------------------

    /// Propose `items` as intended divergences of one pull request, in one
    /// transaction, all with the same note and proposer. Returns the ids in
    /// the order given.
    ///
    /// A shape that already has a live (not withdrawn) acknowledgement on
    /// this version of the change -- proposed or confirmed, by anyone -- is
    /// not proposed again: its existing id is returned instead. A second
    /// proposal of the same shape only splits the decision across two rows
    /// (one person confirms one, the other stays open forever); and the same
    /// request sent twice, a double click, must not leave two. Proposals on
    /// one pull request are serialized by an advisory lock so two requests
    /// at once cannot both see "none yet".
    #[allow(clippy::too_many_arguments)] // one call per proposal, every column named
    pub async fn acknowledgements_propose(
        &self,
        repo: &str,
        pr_number: i64,
        change_id: &str,
        origin_run_id: &str,
        proposed_by: &str,
        proposed_by_sub: Option<&str>,
        note: &str,
        items: &[NewAcknowledgement],
    ) -> Result<Vec<i64>, sqlx::Error> {
        let repo = repo.to_ascii_lowercase();
        let mut tx = self.pool.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext($1))")
            .bind(format!("acknowledgements:{repo}#{pr_number}"))
            .execute(&mut *tx)
            .await?;
        let mut ids = Vec::with_capacity(items.len());
        for item in items {
            let existing: Option<i64> = sqlx::query_scalar(
                "SELECT id FROM acknowledgements
                  WHERE repo = $1 AND pr_number = $2 AND change_id = $3
                    AND lane IS NOT DISTINCT FROM $4 AND pattern = $5
                    AND withdrawn_at IS NULL
                  ORDER BY (acknowledged_at IS NOT NULL) DESC, id DESC
                  LIMIT 1",
            )
            .bind(&repo)
            .bind(pr_number)
            .bind(change_id)
            .bind(&item.lane)
            .bind(&item.pattern)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(id) = existing {
                ids.push(id);
                continue;
            }
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO acknowledgements
                   (repo, pr_number, change_id, lane, pattern, value_hash, note,
                    origin_run_id, proposed_by, proposed_by_sub)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                 RETURNING id",
            )
            .bind(&repo)
            .bind(pr_number)
            .bind(change_id)
            .bind(&item.lane)
            .bind(&item.pattern)
            .bind(&item.value_hash)
            .bind(note)
            .bind(origin_run_id)
            .bind(proposed_by)
            .bind(proposed_by_sub)
            .fetch_one(&mut *tx)
            .await?;
            ids.push(id);
        }
        tx.commit().await?;
        Ok(ids)
    }

    pub async fn acknowledgement_get(
        &self,
        id: i64,
    ) -> Result<Option<AcknowledgementRow>, sqlx::Error> {
        let row = sqlx::query(&format!("{ACK_SELECT} WHERE id = $1"))
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(ack_row))
    }

    /// Confirm a proposal. False when it was already confirmed or withdrawn:
    /// the first decision stands, a second press changes nothing.
    pub async fn acknowledgement_confirm(
        &self,
        id: i64,
        by: &str,
        by_sub: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        let n = sqlx::query(
            "UPDATE acknowledgements
             SET acknowledged_by = $2, acknowledged_by_sub = $3, acknowledged_at = now()
             WHERE id = $1 AND acknowledged_at IS NULL AND withdrawn_at IS NULL",
        )
        .bind(id)
        .bind(by)
        .bind(by_sub)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n == 1)
    }

    /// Withdraw a proposal or a confirmed acknowledgement. False when already
    /// withdrawn. The row stays: who proposed, who confirmed and who withdrew
    /// remain readable.
    pub async fn acknowledgement_withdraw(
        &self,
        id: i64,
        by: &str,
        by_sub: Option<&str>,
    ) -> Result<bool, sqlx::Error> {
        let n = sqlx::query(
            "UPDATE acknowledgements
             SET withdrawn_by = $2, withdrawn_by_sub = $3, withdrawn_at = now()
             WHERE id = $1 AND withdrawn_at IS NULL",
        )
        .bind(id)
        .bind(by)
        .bind(by_sub)
        .execute(&self.pool)
        .await?
        .rows_affected();
        Ok(n == 1)
    }

    /// Every acknowledgement of one pull request, withdrawn ones included,
    /// oldest first. The caller decides what a withdrawn one means. The
    /// repository is matched in lower case, as it is written: GitHub names
    /// are case-insensitive.
    pub async fn acknowledgements_for_pull_request(
        &self,
        repo: &str,
        pr_number: i64,
    ) -> Result<Vec<AcknowledgementRow>, sqlx::Error> {
        let rows = sqlx::query(&format!(
            "{ACK_SELECT} WHERE repo = $1 AND pr_number = $2 ORDER BY id"
        ))
        .bind(repo.to_ascii_lowercase())
        .bind(pr_number)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(ack_row).collect())
    }

    /// The newest `limit` runs created for one pull request
    /// (`params.github`), whatever their state. The repository is matched as
    /// run creation stores it, in lower case, so the expression index on the
    /// raw value is still the one used.
    pub async fn runs_for_pull_request(
        &self,
        repo: &str,
        pr_number: i64,
        limit: i64,
    ) -> Result<Vec<String>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT run_id FROM replay_runs
             WHERE params -> 'github' ->> 'repo' = $1
               AND params -> 'github' ->> 'pr_number' = $2
             ORDER BY created_at DESC
             LIMIT $3",
        )
        .bind(repo.to_ascii_lowercase())
        .bind(pr_number.to_string())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    // -- continuous deployment -----------------------------------------------

    /// Create or update a version with its pull requests, in one transaction.
    /// Idempotent: the pipeline posts a version when it builds it and again
    /// when it learns more (a release's chart, say). A posted field replaces
    /// the stored one; an absent one leaves it alone, so a later post that
    /// knows less than an earlier one does not erase anything. The pull
    /// requests are replaced as a set when any are given.
    pub async fn cd_version_upsert(&self, v: &NewCdVersion) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO cd_versions
               (system, version, kind, commit_sha, image_tag, chart, released_from, merged_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
             ON CONFLICT (system, version) DO UPDATE
               SET kind = EXCLUDED.kind,
                   commit_sha = COALESCE(EXCLUDED.commit_sha, cd_versions.commit_sha),
                   image_tag = COALESCE(EXCLUDED.image_tag, cd_versions.image_tag),
                   chart = COALESCE(EXCLUDED.chart, cd_versions.chart),
                   released_from = COALESCE(EXCLUDED.released_from, cd_versions.released_from),
                   merged_at = COALESCE(EXCLUDED.merged_at, cd_versions.merged_at)",
        )
        .bind(&v.system)
        .bind(&v.version)
        .bind(&v.kind)
        .bind(&v.commit_sha)
        .bind(&v.image_tag)
        .bind(&v.chart)
        .bind(&v.released_from)
        .bind(v.merged_at)
        .execute(&mut *tx)
        .await?;
        if !v.prs.is_empty() {
            sqlx::query("DELETE FROM cd_version_prs WHERE system = $1 AND version = $2")
                .bind(&v.system)
                .bind(&v.version)
                .execute(&mut *tx)
                .await?;
            for pr in &v.prs {
                sqlx::query(
                    "INSERT INTO cd_version_prs
                       (system, version, repo, pr_number, title, author, merged_at)
                     VALUES ($1, $2, $3, $4, $5, $6, $7)
                     ON CONFLICT (system, version, repo, pr_number) DO UPDATE
                       SET title = EXCLUDED.title, author = EXCLUDED.author,
                           merged_at = EXCLUDED.merged_at",
                )
                .bind(&v.system)
                .bind(&v.version)
                .bind(pr.repo.to_ascii_lowercase())
                .bind(pr.pr_number)
                .bind(&pr.title)
                .bind(&pr.author)
                .bind(pr.merged_at)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// A version known only from an observation: the row exists so a
    /// deployment can reference it, with no contents until the pipeline
    /// posts them. Does nothing if the version is already known.
    pub async fn cd_version_ensure(
        &self,
        system: &str,
        version: &str,
        kind: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO cd_versions (system, version, kind) VALUES ($1, $2, $3)
             ON CONFLICT (system, version) DO NOTHING",
        )
        .bind(system)
        .bind(version)
        .bind(kind)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn cd_version_get(
        &self,
        system: &str,
        version: &str,
    ) -> Result<Option<CdVersionRow>, sqlx::Error> {
        let row = sqlx::query(&format!(
            "{CD_VERSION_SELECT} WHERE system = $1 AND version = $2"
        ))
        .bind(system)
        .bind(version)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else { return Ok(None) };
        let mut v = cd_version_row(row);
        v.prs = self
            .cd_prs_of(&[v.version.clone()], system)
            .await?
            .remove(&v.version)
            .unwrap_or_default();
        Ok(Some(v))
    }

    /// The newest `limit` versions of a system, by merge time then by when
    /// they were posted, with their pull requests.
    pub async fn cd_versions_list(
        &self,
        system: &str,
        limit: i64,
    ) -> Result<Vec<CdVersionRow>, sqlx::Error> {
        let rows = sqlx::query(&format!(
            "{CD_VERSION_SELECT} WHERE system = $1
             ORDER BY merged_at DESC NULLS LAST, created_at DESC LIMIT $2"
        ))
        .bind(system)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        let mut versions: Vec<CdVersionRow> = rows.into_iter().map(cd_version_row).collect();
        let names: Vec<String> = versions.iter().map(|v| v.version.clone()).collect();
        let mut prs = self.cd_prs_of(&names, system).await?;
        for v in &mut versions {
            v.prs = prs.remove(&v.version).unwrap_or_default();
        }
        Ok(versions)
    }

    async fn cd_prs_of(
        &self,
        versions: &[String],
        system: &str,
    ) -> Result<std::collections::HashMap<String, Vec<CdVersionPr>>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT version, repo, pr_number, title, author, merged_at
             FROM cd_version_prs WHERE system = $1 AND version = ANY($2)
             ORDER BY merged_at DESC NULLS LAST, pr_number DESC",
        )
        .bind(system)
        .bind(versions)
        .fetch_all(&self.pool)
        .await?;
        let mut out: std::collections::HashMap<String, Vec<CdVersionPr>> = Default::default();
        for r in rows {
            out.entry(r.get(0)).or_default().push(CdVersionPr {
                repo: r.get(1),
                pr_number: r.get(2),
                title: r.get(3),
                author: r.get(4),
                merged_at: r.get(5),
            });
        }
        Ok(out)
    }

    /// Every version carrying one pull request: its merge and each release.
    pub async fn cd_versions_of_pull_request(
        &self,
        repo: &str,
        pr_number: i64,
    ) -> Result<Vec<CdVersionRow>, sqlx::Error> {
        let rows = sqlx::query(&format!(
            "{CD_VERSION_SELECT} WHERE (system, version) IN
               (SELECT system, version FROM cd_version_prs WHERE repo = $1 AND pr_number = $2)
             ORDER BY kind, merged_at DESC NULLS LAST, created_at DESC"
        ))
        .bind(repo.to_ascii_lowercase())
        .bind(pr_number)
        .fetch_all(&self.pool)
        .await?;
        let mut out: Vec<CdVersionRow> = rows.into_iter().map(cd_version_row).collect();
        // With what each carries: the page names the pull request by the
        // title a version recorded for it.
        for v in &mut out {
            v.prs = self
                .cd_prs_of(std::slice::from_ref(&v.version), &v.system)
                .await?
                .remove(&v.version)
                .unwrap_or_default();
        }
        Ok(out)
    }

    pub async fn cd_deployment_append(
        &self,
        d: &NewCdDeployment,
    ) -> Result<CdDeploymentRow, sqlx::Error> {
        let row = sqlx::query(&format!(
            "INSERT INTO cd_deployments
               (system, environment, version, state, requested_by, requested_by_sub,
                request_id, jenkins_build, detail)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
             RETURNING {CD_DEPLOYMENT_COLUMNS}"
        ))
        .bind(&d.system)
        .bind(&d.environment)
        .bind(&d.version)
        .bind(&d.state)
        .bind(&d.requested_by)
        .bind(&d.requested_by_sub)
        .bind(d.request_id)
        .bind(&d.jenkins_build)
        .bind(&d.detail)
        .fetch_one(&self.pool)
        .await?;
        Ok(cd_deployment_row(row))
    }

    /// An environment's history, newest first.
    pub async fn cd_deployments_for_environment(
        &self,
        system: &str,
        environment: &str,
        limit: i64,
    ) -> Result<Vec<CdDeploymentRow>, sqlx::Error> {
        let rows = sqlx::query(&format!(
            "SELECT {CD_DEPLOYMENT_COLUMNS} FROM cd_deployments
             WHERE system = $1 AND environment = $2 ORDER BY id DESC LIMIT $3"
        ))
        .bind(system)
        .bind(environment)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(cd_deployment_row).collect())
    }

    /// Every environment's newest row: where each one is now.
    pub async fn cd_latest_per_environment(
        &self,
        system: &str,
    ) -> Result<Vec<CdDeploymentRow>, sqlx::Error> {
        let rows = sqlx::query(&format!(
            "SELECT DISTINCT ON (environment) {CD_DEPLOYMENT_COLUMNS} FROM cd_deployments
             WHERE system = $1 ORDER BY environment, id DESC"
        ))
        .bind(system)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(cd_deployment_row).collect())
    }

    /// Every environment's newest `live` row: what each one last ran for
    /// certain, whatever it is moving to now.
    pub async fn cd_latest_live_per_environment(
        &self,
        system: &str,
    ) -> Result<Vec<CdDeploymentRow>, sqlx::Error> {
        let rows = sqlx::query(&format!(
            "SELECT DISTINCT ON (environment) {CD_DEPLOYMENT_COLUMNS} FROM cd_deployments
             WHERE system = $1 AND state IN ('live', 'rolled_back')
             ORDER BY environment, id DESC"
        ))
        .bind(system)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(cd_deployment_row).collect())
    }
    /// The newest confirmed live version for one environment.
    pub async fn cd_latest_live_for_environment(
        &self,
        system: &str,
        environment: &str,
    ) -> Result<Option<CdDeploymentRow>, sqlx::Error> {
        let row = sqlx::query(&format!(
            "SELECT {CD_DEPLOYMENT_COLUMNS} FROM cd_deployments
             WHERE system = $1 AND environment = $2 AND state IN ('live', 'rolled_back')
             ORDER BY id DESC LIMIT 1"
        ))
        .bind(system)
        .bind(environment)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(cd_deployment_row))
    }

    /// Each version's newest row on each environment: where every version
    /// is, in one query, for a list.
    pub async fn cd_latest_per_environment_version(
        &self,
        system: &str,
    ) -> Result<Vec<CdDeploymentRow>, sqlx::Error> {
        let rows = sqlx::query(&format!(
            "SELECT DISTINCT ON (environment, version) {CD_DEPLOYMENT_COLUMNS} FROM cd_deployments
             WHERE system = $1 ORDER BY environment, version, id DESC"
        ))
        .bind(system)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(cd_deployment_row).collect())
    }

    /// Whether a version has ever been live on an environment.
    pub async fn cd_was_live(
        &self,
        system: &str,
        environment: &str,
        version: &str,
    ) -> Result<bool, sqlx::Error> {
        let n: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM cd_deployments
             WHERE system = $1 AND environment = $2 AND version = $3
               AND state IN ('live', 'rolled_back')",
        )
        .bind(system)
        .bind(environment)
        .bind(version)
        .fetch_one(&self.pool)
        .await?;
        Ok(n > 0)
    }

    /// The newest non-observation deployment row for this version must itself
    /// be `requested` or `committed`, after the last confirmed live version
    /// on this environment. A failed or completed request cannot attribute a
    /// later observation, but a fresh retry has its own requester.
    pub async fn cd_requested_by(
        &self,
        system: &str,
        environment: &str,
        version: &str,
    ) -> Result<Option<CdRequester>, sqlx::Error> {
        let row: Option<(String, Option<String>, Option<String>)> = sqlx::query_as(
            "SELECT d.requested_by, d.requested_by_sub, d.jenkins_build FROM cd_deployments d
             WHERE d.system = $1 AND d.environment = $2 AND d.version = $3
               AND d.state IN ('requested', 'committed')
               AND d.id = (SELECT max(current.id) FROM cd_deployments current
                           WHERE current.system = $1 AND current.environment = $2
                             AND current.version = $3
                             AND current.requested_by <> 'observed')
               AND d.id > COALESCE((SELECT max(id) FROM cd_deployments
                                    WHERE system = $1 AND environment = $2
                                      AND state IN ('live', 'rolled_back')), 0)
             ORDER BY d.id DESC LIMIT 1",
        )
        .bind(system)
        .bind(environment)
        .bind(version)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(requested_by, requested_by_sub, jenkins_build)| CdRequester {
                requested_by,
                requested_by_sub,
                jenkins_build,
            },
        ))
    }

    /// The newest rows naming one version, across environments, newest
    /// first: where it went and what became of it.
    pub async fn cd_deployments_of_version(
        &self,
        system: &str,
        version: &str,
        limit: i64,
    ) -> Result<Vec<CdDeploymentRow>, sqlx::Error> {
        let rows = sqlx::query(&format!(
            "SELECT {CD_DEPLOYMENT_COLUMNS} FROM cd_deployments
             WHERE system = $1 AND version = $2 ORDER BY id DESC LIMIT $3"
        ))
        .bind(system)
        .bind(version)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(cd_deployment_row).collect())
    }

    // -- cd pause switch -----------------------------------------------------

    pub async fn cd_control_get(&self, system: &str) -> Result<Option<CdControlRow>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT system, paused, reason, changed_by, changed_by_sub, changed_at
             FROM cd_controls WHERE system = $1",
        )
        .bind(system)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(cd_control_row))
    }

    pub async fn cd_control_set(
        &self,
        system: &str,
        paused: bool,
        reason: Option<&str>,
        by: &str,
        by_sub: Option<&str>,
    ) -> Result<CdControlRow, sqlx::Error> {
        let row = sqlx::query(
            "INSERT INTO cd_controls (system, paused, reason, changed_by, changed_by_sub, changed_at)
             VALUES ($1, $2, $3, $4, $5, now())
             ON CONFLICT (system) DO UPDATE
               SET paused = EXCLUDED.paused, reason = EXCLUDED.reason,
                   changed_by = EXCLUDED.changed_by, changed_by_sub = EXCLUDED.changed_by_sub,
                   changed_at = EXCLUDED.changed_at
             RETURNING system, paused, reason, changed_by, changed_by_sub, changed_at",
        )
        .bind(system)
        .bind(paused)
        .bind(reason)
        .bind(by)
        .bind(by_sub)
        .fetch_one(&self.pool)
        .await?;
        Ok(cd_control_row(row))
    }

    // -- recordings ----------------------------------------------------------

    #[allow(clippy::too_many_arguments)] // mirrors the catalog row shape
    pub async fn upsert_recording(
        &self,
        recording_id: &str,
        source_path: Option<&str>,
        event_count: Option<i64>,
        correlation_count: Option<i64>,
        byte_size: Option<i64>,
        manifest: Option<&serde_json::Value>,
        created_by: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO recordings (recording_id, source_path, event_count, correlation_count,
                                     byte_size, manifest, created_by)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT (recording_id) DO UPDATE
               SET source_path = EXCLUDED.source_path,
                   event_count = EXCLUDED.event_count,
                   correlation_count = EXCLUDED.correlation_count,
                   byte_size = EXCLUDED.byte_size,
                   manifest = COALESCE(EXCLUDED.manifest, recordings.manifest)",
        )
        .bind(recording_id)
        .bind(source_path)
        .bind(event_count)
        .bind(correlation_count)
        .bind(byte_size)
        .bind(manifest)
        .bind(created_by)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// How many correlations the catalog recorded for one recording, if it has
    /// been ingested at all.
    ///
    /// Answers the count for a recording whose session is NOT sealed, where the
    /// manifest cannot. Two `None`s are possible and mean different things — no
    /// such row, or a row that never carried a count — and neither is zero.
    pub async fn recording_correlation_count(
        &self,
        recording_id: &str,
    ) -> Result<Option<i64>, sqlx::Error> {
        let row = sqlx::query("SELECT correlation_count FROM recordings WHERE recording_id = $1")
            .bind(recording_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|r| {
            r.try_get::<Option<i64>, _>("correlation_count")
                .ok()
                .flatten()
        }))
    }

    pub async fn list_recordings(&self, limit: i64) -> Result<Vec<RecordingRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT recording_id, kind, source_path, event_count, correlation_count,
                    byte_size, status, created_by, created_at, manifest
             FROM recordings ORDER BY created_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(recording_row).collect())
    }

    // -- runs ----------------------------------------------------------------

    #[allow(clippy::too_many_arguments)] // mirrors the run-creation wire shape
    pub async fn insert_run(
        &self,
        run_id: &str,
        mode: &str,
        recording_id: Option<&str>,
        candidate: &serde_json::Value,
        params: &serde_json::Value,
        expectation: Option<&str>,
        created_by: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO replay_runs
               (run_id, mode, recording_id, candidate, params, expectation, created_by, state, started_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', now())
             ON CONFLICT (run_id) DO NOTHING",
        )
        .bind(run_id)
        .bind(mode)
        .bind(recording_id)
        .bind(candidate)
        .bind(params)
        .bind(expectation)
        .bind(created_by)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_run_state(
        &self,
        run_id: &str,
        state: &str,
        failure: Option<&serde_json::Value>,
    ) -> Result<(), sqlx::Error> {
        // V4 terminal-guard: a terminal state (completed/failed) is the settled
        // verdict — the first one wins. Under at-least-once, possibly-reordered
        // push-back, a stale `running` or a second/conflicting `finish` must not
        // regress or flip a run that has already finished. The WHERE clause makes
        // any post-terminal transition a zero-row no-op, so the store is
        // idempotent without the caller having to read-modify-write.
        sqlx::query(
            "UPDATE replay_runs SET
               state = $2,
               failure = COALESCE($3, failure),
               finished_at = CASE WHEN $2 IN ('completed','failed') THEN now() ELSE finished_at END
             WHERE run_id = $1
               AND state NOT IN ('completed','failed')",
        )
        .bind(run_id)
        .bind(state)
        .bind(failure)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Name the recording a run drives, once it is known.
    ///
    /// A run given only an S3 prefix asks for a session it cannot name yet; the
    /// runner resolves it during the scan. The persisted request (`params`) is
    /// updated with it too, so what the run was asked to do keeps naming the
    /// concrete recording that ran rather than the prefix that was typed. Rows
    /// written before the request was persisted carry no request to update and
    /// are left exactly as they are.
    pub async fn set_run_recording(
        &self,
        run_id: &str,
        recording_id: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE replay_runs SET
               recording_id = $2,
               params = CASE
                          WHEN jsonb_exists(params, 'candidate_spec')
                          THEN params || jsonb_build_object('recording_id', $2::text)
                          ELSE params
                        END
             WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(recording_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_run_candidate_sha(
        &self,
        run_id: &str,
        sha256: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE replay_runs SET candidate_sha256 = $2 WHERE run_id = $1")
            .bind(run_id)
            .bind(sha256)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn set_run_result(
        &self,
        run_id: &str,
        verdict: Option<&str>,
        scorecard: Option<&serde_json::Value>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE replay_runs SET verdict = COALESCE($2, verdict),
                                    scorecard = COALESCE($3, scorecard)
             WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(verdict)
        .bind(scorecard)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// The delta verdict: `pass` or `fail` once the run's delta against its
    /// baseline is computed, else `pending` (a side is still running) or
    /// `refused` (the pairing can never give one). Idempotent; the newest
    /// computation wins.
    /// Record what the run's delta against `against` said. A verdict is a fact
    /// about a pair, so it lands only while the row still names that baseline:
    /// a settlement that was computing against the old baseline when the run
    /// was re-pointed finds the row naming the new one and writes nothing.
    /// `false` says it did not land.
    pub async fn set_delta_verdict(
        &self,
        run_id: &str,
        against: &str,
        verdict: &str,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE replay_runs SET delta_verdict = $2
             WHERE run_id = $1 AND params ->> 'delta_against' = $3",
        )
        .bind(run_id)
        .bind(verdict)
        .bind(against)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Re-point a run at another baseline. The delta verdict goes back to
    /// pending with it: whatever was settled was settled against the old
    /// baseline, and the row must not keep saying so.
    pub async fn set_run_delta_against(
        &self,
        run_id: &str,
        baseline_run_id: &str,
    ) -> Result<bool, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE replay_runs
             SET params = jsonb_set(params, '{delta_against}', to_jsonb($2::text), true),
                 delta_verdict = 'pending'
             WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(baseline_run_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    /// Every completed run created to be measured against `baseline_run_id`
    /// (`params.delta_against`), so their deltas can be settled when the
    /// baseline finishes.
    pub async fn runs_measured_against(
        &self,
        baseline_run_id: &str,
    ) -> Result<Vec<String>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT run_id FROM replay_runs
             WHERE params ->> 'delta_against' = $1 AND state = 'completed'
             ORDER BY created_at",
        )
        .bind(baseline_run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    pub async fn get_run(&self, run_id: &str) -> Result<Option<RunRow>, sqlx::Error> {
        let row = sqlx::query(RUN_SELECT)
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(run_row))
    }

    /// The run list, WITHOUT the scorecard column. See [`RunSummaryRow`] for
    /// why the column is not selected rather than dropped afterwards.
    ///
    /// Postgres walks the JSONB and returns five scalars, so nothing large is
    /// read, decoded or cloned. `#>` yields `jsonb`, keeping booleans and
    /// numbers typed rather than stringified, and a path that is not there
    /// yields NULL rather than an error — so a scorecard with no verdict, or a
    /// verdict with no reason, degrades field by field.
    ///
    /// The `CASE` is what keeps "no scorecard" distinguishable from "a
    /// scorecard whose fields are all absent": without it `jsonb_build_object`
    /// would return an object of nulls for a run that was never scored, and the
    /// dashboard would report it as scored-with-nothing instead of unscored.
    pub async fn list_run_summaries(&self, limit: i64) -> Result<Vec<RunSummaryRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT run_id, mode, recording_id, candidate, candidate_sha256, params, state,
                    verdict,
                    CASE WHEN scorecard IS NULL THEN NULL ELSE jsonb_build_object(
                        'pass',                 scorecard #> '{verdict,pass}',
                        'inconclusive',         scorecard #> '{verdict,inconclusive}',
                        'reason',               scorecard #> '{verdict,reason}',
                        'total_correlations',   scorecard #> '{summary,total_correlations}',
                        'matched_correlations', scorecard #> '{summary,matched_correlations}'
                    ) END AS scorecard_digest,
                    failure, expectation, created_by, created_at,
                    started_at, finished_at, delta_verdict
             FROM replay_runs ORDER BY created_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(run_summary_row).collect())
    }

    /// Every run that is NOT terminal (state not in 'completed'/'failed'), with
    /// its DB-clock age. The k8s reconciler polls this after an orchestrator
    /// restart to re-derive the verdict of runs whose in-flight watcher was lost.
    pub async fn list_active_runs(&self) -> Result<Vec<ActiveRun>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT run_id, state,
                    EXTRACT(EPOCH FROM (now() - created_at))::float8 AS age_secs
             FROM replay_runs
             WHERE state NOT IN ('completed','failed')
             ORDER BY created_at",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| ActiveRun {
                run_id: r.get(0),
                state: r.get(1),
                age_secs: r.get(2),
            })
            .collect())
    }

    // -- stage history + logs -------------------------------------------------

    /// Close the currently-running stage (if any) and open a new one.
    pub async fn stage_transition(
        &self,
        run_id: &str,
        stage: &str,
        step: Option<i32>,
        steps_total: Option<i32>,
        prev_status: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE run_stages SET status = $2, finished_at = now()
             WHERE run_id = $1 AND status = 'running'",
        )
        .bind(run_id)
        .bind(prev_status)
        .execute(&self.pool)
        .await?;
        sqlx::query(
            "INSERT INTO run_stages (run_id, stage, step, steps_total) VALUES ($1, $2, $3, $4)",
        )
        .bind(run_id)
        .bind(stage)
        .bind(step)
        .bind(steps_total)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Close the final running stage with a terminal status.
    pub async fn stage_finish(&self, run_id: &str, status: &str) -> Result<(), sqlx::Error> {
        sqlx::query(
            "UPDATE run_stages SET status = $2, finished_at = now()
             WHERE run_id = $1 AND status = 'running'",
        )
        .bind(run_id)
        .bind(status)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_stages(&self, run_id: &str) -> Result<Vec<StageRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, run_id, stage, status, step, steps_total, started_at, finished_at, detail
             FROM run_stages WHERE run_id = $1 ORDER BY id",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| StageRow {
                id: r.get(0),
                run_id: r.get(1),
                stage: r.get(2),
                status: r.get(3),
                step: r.get(4),
                steps_total: r.get(5),
                started_at: r.get(6),
                finished_at: r.get(7),
                detail: r.get(8),
            })
            .collect())
    }

    pub async fn append_log(
        &self,
        run_id: &str,
        stage: &str,
        seq: i64,
        lines: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO run_log_chunks (run_id, stage, seq, lines) VALUES ($1, $2, $3, $4)
             ON CONFLICT DO NOTHING",
        )
        .bind(run_id)
        .bind(stage)
        .bind(seq)
        .bind(lines)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_logs(
        &self,
        run_id: &str,
        stage: Option<&str>,
        after_seq: i64,
    ) -> Result<Vec<(String, i64, String)>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT stage, seq, lines FROM run_log_chunks
             WHERE run_id = $1 AND ($2::text IS NULL OR stage = $2) AND seq > $3
             ORDER BY ts, seq",
        )
        .bind(run_id)
        .bind(stage)
        .bind(after_seq)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get(0), r.get(1), r.get(2)))
            .collect())
    }

    // -- artifacts -------------------------------------------------------------

    pub async fn register_artifact(
        &self,
        run_id: Option<&str>,
        recording_id: Option<&str>,
        kind: &str,
        uri: &str,
        bytes: Option<i64>,
        sha256: Option<&str>,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO artifacts (run_id, recording_id, kind, uri, bytes, sha256)
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(run_id)
        .bind(recording_id)
        .bind(kind)
        .bind(uri)
        .bind(bytes)
        .bind(sha256)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn list_artifacts(&self, run_id: &str) -> Result<Vec<ArtifactRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT id, run_id, recording_id, kind, uri, bytes, sha256, created_at
             FROM artifacts WHERE run_id = $1 OR recording_id = $1 ORDER BY id",
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|r| ArtifactRow {
                id: r.get(0),
                run_id: r.get(1),
                recording_id: r.get(2),
                kind: r.get(3),
                uri: r.get(4),
                bytes: r.get(5),
                sha256: r.get(6),
                created_at: r.get(7),
            })
            .collect())
    }

    pub async fn get_artifact(&self, id: i64) -> Result<Option<ArtifactRow>, sqlx::Error> {
        let row = sqlx::query(
            "SELECT id, run_id, recording_id, kind, uri, bytes, sha256, created_at
             FROM artifacts WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|r| ArtifactRow {
            id: r.get(0),
            run_id: r.get(1),
            recording_id: r.get(2),
            kind: r.get(3),
            uri: r.get(4),
            bytes: r.get(5),
            sha256: r.get(6),
            created_at: r.get(7),
        }))
    }
}

const ACK_SELECT: &str = "SELECT id, repo, pr_number, change_id, lane, pattern, value_hash, note, \
     origin_run_id, proposed_by, proposed_at, acknowledged_by, acknowledged_at, \
     withdrawn_by, withdrawn_at, proposed_by_sub, acknowledged_by_sub, withdrawn_by_sub \
     FROM acknowledgements";

fn ack_row(r: sqlx::postgres::PgRow) -> AcknowledgementRow {
    AcknowledgementRow {
        id: r.get(0),
        repo: r.get(1),
        pr_number: r.get(2),
        change_id: r.get(3),
        lane: r.get(4),
        pattern: r.get(5),
        value_hash: r.get(6),
        note: r.get(7),
        origin_run_id: r.get(8),
        proposed_by: r.get(9),
        proposed_at: r.get(10),
        acknowledged_by: r.get(11),
        acknowledged_at: r.get(12),
        withdrawn_by: r.get(13),
        withdrawn_at: r.get(14),
        proposed_by_sub: r.get(15),
        acknowledged_by_sub: r.get(16),
        withdrawn_by_sub: r.get(17),
    }
}

const CD_VERSION_SELECT: &str = "SELECT system, version, kind, commit_sha, image_tag, chart, \
     released_from, merged_at, created_at FROM cd_versions";

fn cd_version_row(r: sqlx::postgres::PgRow) -> CdVersionRow {
    CdVersionRow {
        system: r.get(0),
        version: r.get(1),
        kind: r.get(2),
        commit_sha: r.get(3),
        image_tag: r.get(4),
        chart: r.get(5),
        released_from: r.get(6),
        merged_at: r.get(7),
        created_at: r.get(8),
        prs: Vec::new(),
    }
}

const CD_DEPLOYMENT_COLUMNS: &str = "id, system, environment, version, state, requested_by, \
     requested_by_sub, request_id, jenkins_build, detail, at";

fn cd_deployment_row(r: sqlx::postgres::PgRow) -> CdDeploymentRow {
    CdDeploymentRow {
        id: r.get(0),
        system: r.get(1),
        environment: r.get(2),
        version: r.get(3),
        state: r.get(4),
        requested_by: r.get(5),
        requested_by_sub: r.get(6),
        request_id: r.get(7),
        jenkins_build: r.get(8),
        detail: r.get(9),
        at: r.get(10),
    }
}

fn cd_control_row(r: sqlx::postgres::PgRow) -> CdControlRow {
    CdControlRow {
        system: r.get(0),
        paused: r.get(1),
        reason: r.get(2),
        changed_by: r.get(3),
        changed_by_sub: r.get(4),
        changed_at: r.get(5),
    }
}

const RUN_SELECT: &str =
    "SELECT run_id, mode, recording_id, candidate, candidate_sha256, params, state,
        verdict, scorecard, failure, expectation, created_by, created_at, started_at, finished_at,
        delta_verdict
 FROM replay_runs WHERE run_id = $1";

fn run_row(r: sqlx::postgres::PgRow) -> RunRow {
    RunRow {
        run_id: r.get(0),
        mode: r.get(1),
        recording_id: r.get(2),
        candidate: r.get(3),
        candidate_sha256: r.get(4),
        params: r.get(5),
        state: r.get(6),
        verdict: r.get(7),
        scorecard: r.get(8),
        failure: r.get(9),
        expectation: r.get(10),
        created_by: r.get(11),
        created_at: r.get(12),
        started_at: r.get(13),
        finished_at: r.get(14),
        delta_verdict: r.get(15),
    }
}

fn run_summary_row(r: sqlx::postgres::PgRow) -> RunSummaryRow {
    RunSummaryRow {
        run_id: r.get(0),
        mode: r.get(1),
        recording_id: r.get(2),
        candidate: r.get(3),
        candidate_sha256: r.get(4),
        params: r.get(5),
        state: r.get(6),
        verdict: r.get(7),
        scorecard_digest: r.get(8),
        failure: r.get(9),
        expectation: r.get(10),
        created_by: r.get(11),
        created_at: r.get(12),
        started_at: r.get(13),
        finished_at: r.get(14),
        delta_verdict: r.get(15),
    }
}

fn recording_row(r: sqlx::postgres::PgRow) -> RecordingRow {
    RecordingRow {
        recording_id: r.get(0),
        kind: r.get(1),
        source_path: r.get(2),
        event_count: r.get(3),
        correlation_count: r.get(4),
        byte_size: r.get(5),
        status: r.get(6),
        created_by: r.get(7),
        created_at: r.get(8),
        manifest: r.get(9),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    /// The digest is a CONTRACT ACROSS TWO LANGUAGES with no compiler between
    /// its halves: Postgres builds the object, TypeScript reads it, and nothing
    /// checks that they name the same fields.
    ///
    /// Getting it wrong is silent in the worst way. `resultOf` reads every
    /// field with `?? null`, so a key renamed on one side alone does not throw
    /// — every run renders "no verdict"/0 of 0 and the dashboard looks like the
    /// scorer stopped working. That is the producer/consumer split this repo
    /// keeps rediscovering, so it is asserted rather than reviewed.
    ///
    /// Reading the sources is the point: any check written against a constant
    /// in THIS file would pass while the two real halves disagreed.
    fn source(rel: &str) -> String {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(rel);
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
    }

    #[test]
    fn the_digest_postgres_builds_is_the_digest_the_dashboard_reads() {
        let sql = source("src/lib.rs");
        // Anchored from the END and walked backwards: this file has another
        // `jsonb_build_object` (the recording_id patch at ~line 371), and
        // taking the first one made the slice span both queries and pick up a
        // sixth key. The count assertion below caught that, which is the whole
        // reason it is there.
        let build = sql
            .split_once(") END AS scorecard_digest")
            .expect("the projection no longer closes as scorecard_digest")
            .0
            .rsplit_once("jsonb_build_object(")
            .expect("the digest projection is gone from list_run_summaries")
            .1;
        let mut from_sql: Vec<&str> = build
            .lines()
            .filter_map(|l| l.trim().strip_prefix('\''))
            .filter_map(|l| l.split_once('\''))
            .map(|(key, _)| key)
            .collect();
        from_sql.sort_unstable();

        let ts = source("../../web/src/lib/api.ts");
        let decl = ts
            .split_once("export type ScorecardDigest = {")
            .expect("ScorecardDigest is gone from the dashboard")
            .1
            .split_once("};")
            .expect("ScorecardDigest declaration is unterminated")
            .0;
        let mut from_ts: Vec<&str> = decl
            .lines()
            .filter_map(|l| l.trim().split_once(':'))
            .map(|(name, _)| name.trim())
            .filter(|name| !name.is_empty() && !name.starts_with("//"))
            .collect();
        from_ts.sort_unstable();

        // Non-emptiness first: two empty lists compare equal, so without this
        // the assertion below passes when a parse silently matches nothing —
        // which is how a source-inspection test turns into a tautology.
        assert_eq!(
            from_sql.len(),
            5,
            "parsed {from_sql:?} out of the SQL; expected the five scalars"
        );
        assert_eq!(
            from_sql, from_ts,
            "the SQL digest and the dashboard's ScorecardDigest name different \
             fields. Every mismatched field reads as null in the dashboard and \
             renders as 'no verdict' with no error anywhere."
        );
    }

    #[test]
    fn the_list_row_still_carries_no_scorecard() {
        let src = source("src/lib.rs");
        let decl = src
            .split_once("pub struct RunSummaryRow {")
            .expect("RunSummaryRow is gone")
            .1
            .split_once('}')
            .expect("RunSummaryRow declaration is unterminated")
            .0;
        let fields: Vec<&str> = decl
            .lines()
            .filter_map(|l| l.trim().strip_prefix("pub "))
            .filter_map(|l| l.split_once(':'))
            .map(|(name, _)| name)
            .collect();
        assert!(
            fields.contains(&"scorecard_digest"),
            "the digest is gone; found {fields:?}"
        );
        assert!(
            !fields.contains(&"scorecard"),
            "RunSummaryRow has regained a `scorecard` field. That single field \
             was 97.5% of a 46.5 MB list response, polled every five seconds \
             from every open tab, and it OOMKilled the orchestrator ten times \
             on 2026-09-17. A caller that needs a scorecard fetches one run."
        );
    }

    /// Against a real Postgres, named by `DEJA_TEST_DB_URL`; absent, these
    /// tests pass without running, and say so. The race they pin is in the
    /// SQL, so nothing short of the store can pin it.
    async fn store_under_test() -> Option<super::Store> {
        let url = std::env::var("DEJA_TEST_DB_URL").ok()?;
        Some(
            super::Store::connect(&url)
                .await
                .expect("DEJA_TEST_DB_URL names a reachable Postgres"),
        )
    }

    fn unique(prefix: &str) -> String {
        format!("{prefix}-{}", uuid::Uuid::now_v7())
    }

    async fn completed_run(store: &super::Store, run_id: &str, against: &str) {
        let params = serde_json::json!({ "mode": "replay", "delta_against": against });
        store
            .insert_run(
                run_id,
                "replay",
                Some("rec-test"),
                &serde_json::json!({ "kind": "prebuilt_image", "image": "test" }),
                &params,
                None,
                "test",
            )
            .await
            .unwrap();
        store
            .update_run_state(run_id, "completed", None)
            .await
            .unwrap();
    }

    async fn row(store: &super::Store, run_id: &str) -> (Option<String>, Option<String>) {
        let row = store
            .get_run(run_id)
            .await
            .unwrap()
            .expect("the run exists");
        let against = row
            .params
            .get("delta_against")
            .and_then(|v| v.as_str())
            .map(str::to_owned);
        (against, row.delta_verdict)
    }

    async fn remove(store: &super::Store, run_id: &str) {
        sqlx::query("DELETE FROM replay_runs WHERE run_id = $1")
            .bind(run_id)
            .execute(&store.pool)
            .await
            .unwrap();
    }

    /// A settlement against the old baseline that was in flight when the run
    /// was re-pointed must not land: the row would say A's verdict while
    /// naming B. The verdict update is conditioned on the baseline it was
    /// computed against, and the re-point's reset to pending is in the same
    /// statement as the re-point, so there is no row that names B and carries
    /// A's word.
    #[tokio::test]
    async fn a_verdict_against_the_old_baseline_does_not_land_after_a_repoint() {
        let Some(store) = store_under_test().await else {
            eprintln!("skipped: DEJA_TEST_DB_URL is not set");
            return;
        };
        let (y, a, b) = (unique("run-y"), unique("run-a"), unique("run-b"));
        completed_run(&store, &y, &a).await;

        assert!(store.set_delta_verdict(&y, &a, "fail").await.unwrap());
        assert_eq!(
            row(&store, &y).await,
            (Some(a.clone()), Some("fail".into()))
        );

        assert!(store.set_run_delta_against(&y, &b).await.unwrap());
        assert_eq!(
            row(&store, &y).await,
            (Some(b.clone()), Some("pending".into())),
            "the re-point names the new baseline and forgets the old verdict in one statement"
        );

        assert!(
            !store.set_delta_verdict(&y, &a, "fail").await.unwrap(),
            "the late settlement against the old baseline lands nowhere"
        );
        assert_eq!(
            row(&store, &y).await,
            (Some(b.clone()), Some("pending".into()))
        );

        assert!(store.set_delta_verdict(&y, &b, "pass").await.unwrap());
        assert_eq!(
            row(&store, &y).await,
            (Some(b.clone()), Some("pass".into()))
        );

        assert!(
            !store
                .set_run_delta_against(&unique("run-nowhere"), &b)
                .await
                .unwrap(),
            "a run the store does not hold is not re-pointed"
        );
        remove(&store, &y).await;
    }

    /// The new baseline's finish settles the re-pointed run, and the old
    /// baseline's finish no longer does: both read the key the re-point writes.
    #[tokio::test]
    async fn a_repointed_run_is_measured_against_its_new_baseline() {
        let Some(store) = store_under_test().await else {
            eprintln!("skipped: DEJA_TEST_DB_URL is not set");
            return;
        };
        let (y, a, b) = (unique("run-y"), unique("run-a"), unique("run-b"));
        completed_run(&store, &y, &a).await;
        assert_eq!(
            store.runs_measured_against(&a).await.unwrap(),
            vec![y.clone()]
        );

        assert!(store.set_run_delta_against(&y, &b).await.unwrap());
        assert_eq!(
            store.runs_measured_against(&a).await.unwrap(),
            Vec::<String>::new()
        );
        assert_eq!(
            store.runs_measured_against(&b).await.unwrap(),
            vec![y.clone()]
        );
        remove(&store, &y).await;
    }

    async fn append_cd_deployment(
        store: &super::Store,
        system: &str,
        environment: &str,
        version: &str,
        state: &str,
        requested_by: &str,
        jenkins_build: Option<&str>,
    ) {
        store
            .cd_deployment_append(&super::NewCdDeployment {
                system: system.to_owned(),
                environment: environment.to_owned(),
                version: version.to_owned(),
                state: state.to_owned(),
                requested_by: requested_by.to_owned(),
                requested_by_sub: None,
                request_id: None,
                jenkins_build: jenkins_build.map(str::to_owned),
                detail: serde_json::json!({}),
            })
            .await
            .unwrap();
    }
    #[tokio::test]
    async fn latest_confirmed_cd_version_is_scoped_to_one_environment() {
        let Some(store) = store_under_test().await else {
            eprintln!("skipped: DEJA_TEST_DB_URL is not set");
            return;
        };
        let (system, environment, other, a, b) = (
            unique("cd-system"),
            unique("cd-env"),
            unique("cd-env"),
            unique("cd-version-a"),
            unique("cd-version-b"),
        );
        store.cd_version_ensure(&system, &a, "merge").await.unwrap();
        store.cd_version_ensure(&system, &b, "merge").await.unwrap();
        append_cd_deployment(&store, &system, &environment, &a, "live", "observed", None).await;
        append_cd_deployment(
            &store,
            &system,
            &environment,
            &b,
            "requested",
            "alice",
            None,
        )
        .await;
        append_cd_deployment(&store, &system, &other, &b, "live", "observed", None).await;

        assert_eq!(
            store
                .cd_latest_live_for_environment(&system, &environment)
                .await
                .unwrap()
                .map(|row| row.version),
            Some(a.clone())
        );
        assert_eq!(
            store
                .cd_latest_live_for_environment(&system, &other)
                .await
                .unwrap()
                .map(|row| row.version),
            Some(b.clone())
        );
        assert!(store
            .cd_latest_live_for_environment(&system, &unique("cd-env"))
            .await
            .unwrap()
            .is_none());
        sqlx::query("DELETE FROM cd_deployments WHERE system = $1")
            .bind(&system)
            .execute(&store.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM cd_versions WHERE system = $1")
            .bind(&system)
            .execute(&store.pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn failed_cd_request_is_not_attributed_to_later_observation_or_retry() {
        let Some(store) = store_under_test().await else {
            eprintln!("skipped: DEJA_TEST_DB_URL is not set");
            return;
        };
        let (system, environment, a, b) = (
            unique("cd-system"),
            unique("cd-env"),
            unique("cd-version-a"),
            unique("cd-version-b"),
        );

        store.cd_version_ensure(&system, &a, "merge").await.unwrap();
        store.cd_version_ensure(&system, &b, "merge").await.unwrap();

        append_cd_deployment(&store, &system, &environment, &a, "live", "observed", None).await;
        append_cd_deployment(
            &store,
            &system,
            &environment,
            &b,
            "requested",
            "alice",
            Some("build-1"),
        )
        .await;
        assert_eq!(
            store
                .cd_requested_by(&system, &environment, &b)
                .await
                .unwrap()
                .map(|r| (r.requested_by, r.jenkins_build)),
            Some(("alice".into(), Some("build-1".into())))
        );

        append_cd_deployment(&store, &system, &environment, &b, "failed", "alice", None).await;
        append_cd_deployment(&store, &system, &environment, &b, "live", "observed", None).await;
        assert!(
            store
                .cd_requested_by(&system, &environment, &b)
                .await
                .unwrap()
                .is_none(),
            "a failed request must not attribute the later observation"
        );

        append_cd_deployment(
            &store,
            &system,
            &environment,
            &b,
            "requested",
            "bob",
            Some("build-2"),
        )
        .await;
        assert_eq!(
            store
                .cd_requested_by(&system, &environment, &b)
                .await
                .unwrap()
                .map(|r| (r.requested_by, r.jenkins_build)),
            Some(("bob".into(), Some("build-2".into()))),
            "a fresh retry request carries its own requester"
        );

        let other_environment = unique("cd-env");
        append_cd_deployment(
            &store,
            &system,
            &other_environment,
            &a,
            "live",
            "observed",
            None,
        )
        .await;
        append_cd_deployment(
            &store,
            &system,
            &other_environment,
            &b,
            "requested",
            "carol",
            Some("build-3"),
        )
        .await;
        append_cd_deployment(
            &store,
            &system,
            &other_environment,
            &a,
            "live",
            "observed",
            None,
        )
        .await;
        assert!(
            store
                .cd_requested_by(&system, &other_environment, &b)
                .await
                .unwrap()
                .is_none(),
            "a different version becoming live supersedes the earlier request"
        );

        let completed_environment = unique("cd-env");
        append_cd_deployment(
            &store,
            &system,
            &completed_environment,
            &b,
            "requested",
            "dan",
            Some("build-4"),
        )
        .await;
        append_cd_deployment(
            &store,
            &system,
            &completed_environment,
            &b,
            "live",
            "observed",
            None,
        )
        .await;
        assert!(
            store
                .cd_requested_by(&system, &completed_environment, &b)
                .await
                .unwrap()
                .is_none(),
            "an already observed arrival must not reuse its completed request"
        );

        sqlx::query("DELETE FROM cd_deployments WHERE system = $1")
            .bind(&system)
            .execute(&store.pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM cd_versions WHERE system = $1")
            .bind(&system)
            .execute(&store.pool)
            .await
            .unwrap();
    }
}
