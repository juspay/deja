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
        sqlx::query(
            "UPDATE replay_runs SET
               state = $2,
               failure = COALESCE($3, failure),
               finished_at = CASE WHEN $2 IN ('completed','failed') THEN now() ELSE finished_at END
             WHERE run_id = $1",
        )
        .bind(run_id)
        .bind(state)
        .bind(failure)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn set_run_recording(
        &self,
        run_id: &str,
        recording_id: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE replay_runs SET recording_id = $2 WHERE run_id = $1")
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

    pub async fn get_run(&self, run_id: &str) -> Result<Option<RunRow>, sqlx::Error> {
        let row = sqlx::query(RUN_SELECT)
            .bind(run_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(run_row))
    }

    pub async fn list_runs(&self, limit: i64) -> Result<Vec<RunRow>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT run_id, mode, recording_id, candidate, candidate_sha256, params, state,
                    verdict, scorecard, failure, expectation, created_by, created_at,
                    started_at, finished_at
             FROM replay_runs ORDER BY created_at DESC LIMIT $1",
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(run_row).collect())
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

const RUN_SELECT: &str =
    "SELECT run_id, mode, recording_id, candidate, candidate_sha256, params, state,
        verdict, scorecard, failure, expectation, created_by, created_at, started_at, finished_at
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
