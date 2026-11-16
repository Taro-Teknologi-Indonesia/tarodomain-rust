//! The SQL saga store (spec §9.1).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use tarodomain::adapters::{SagaQuery, SagaRecord, SagaSaveInput, SagaStatus, SagaStoreAdapter};
use tarodomain::async_trait;
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::Uuid;

use crate::db::{Db, Param, Row};

const COLUMNS: &str = "saga_type, saga_id, correlation_id, state, status, current_step, \
     failure_reason, started_at, updated_at, completed_at";
const ORDER: &str = "ORDER BY updated_at, saga_type, saga_id";

/// Sagas on SQL, keyed by `(saga_type, saga_id)`. The write rules are
/// [`SagaRecord::saved`], `complete` and `fail`, the same as in memory.
#[derive(Clone)]
pub struct SqlxSagaStore {
    db: Db,
    clock: Arc<dyn Clock>,
}

impl SqlxSagaStore {
    /// A store over `db`. Timestamps come from `clock`.
    pub fn new(db: Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }

    /// Writes the whole record (everything but `started_at`, which stays).
    async fn put(&self, record: SagaRecord) -> Result<()> {
        self.db
            .execute(
                &format!(
                    "INSERT INTO tarodomain_sagas ({COLUMNS}) \
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                     ON CONFLICT (saga_type, saga_id) DO UPDATE SET \
                     correlation_id = excluded.correlation_id, state = excluded.state, \
                     status = excluded.status, current_step = excluded.current_step, \
                     failure_reason = excluded.failure_reason, updated_at = excluded.updated_at, \
                     completed_at = excluded.completed_at"
                ),
                vec![
                    Param::text(record.saga_type),
                    record.saga_id.into(),
                    record.correlation_id.into(),
                    Param::Json(record.state),
                    Param::text(record.status.as_str()),
                    Param::text(record.current_step),
                    Param::opt_text(record.failure_reason),
                    record.started_at.into(),
                    record.updated_at.into(),
                    Param::opt_time(record.completed_at),
                ],
            )
            .await
            .map(|_| ())
    }

    /// Applies `change` to the stored record; saves it if it changed.
    async fn end(
        &self,
        saga_type: &str,
        saga_id: Uuid,
        change: impl FnOnce(&mut SagaRecord, DateTime<Utc>) -> bool,
    ) -> Result<()> {
        let Some(mut record) = self.load(saga_type, saga_id).await? else {
            return Ok(());
        };
        if change(&mut record, self.clock.now()) {
            self.put(record).await?;
        }
        Ok(())
    }

    async fn list(
        &self,
        mut conditions: Vec<&'static str>,
        mut params: Vec<Param>,
        query: &SagaQuery,
    ) -> Result<Vec<SagaRecord>> {
        if let Some(saga_type) = &query.saga_type {
            conditions.push("saga_type = ?");
            params.push(Param::text(saga_type.as_str()));
        }
        params.push(self.db.limit(query.limit));
        let filter = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let rows = self
            .db
            .fetch(
                &format!("SELECT {COLUMNS} FROM tarodomain_sagas {filter} {ORDER} LIMIT ?"),
                params,
            )
            .await?;
        rows.iter().map(parse).collect()
    }
}

fn parse(row: &Row) -> Result<SagaRecord> {
    let status = row.str("status")?;
    Ok(SagaRecord {
        saga_id: row.uuid("saga_id")?,
        saga_type: row.str("saga_type")?,
        correlation_id: row.uuid("correlation_id")?,
        state: row.json("state")?,
        status: status.parse().map_err(|_| {
            FrameworkError::serialization_error(format!("unknown saga status {status:?}"))
        })?,
        current_step: row.str("current_step")?,
        failure_reason: row.opt_str("failure_reason")?,
        started_at: row.time("started_at")?,
        updated_at: row.time("updated_at")?,
        completed_at: row.opt_time("completed_at")?,
    })
}

#[async_trait]
impl SagaStoreAdapter for SqlxSagaStore {
    async fn save(&self, input: SagaSaveInput) -> Result<()> {
        let previous = self.load(&input.saga_type, input.saga_id).await?;
        let record = SagaRecord::saved(input, previous.as_ref(), self.clock.now());
        self.put(record).await
    }

    async fn load(&self, saga_type: &str, saga_id: Uuid) -> Result<Option<SagaRecord>> {
        let row = self
            .db
            .fetch_one(
                &format!(
                    "SELECT {COLUMNS} FROM tarodomain_sagas WHERE saga_type = ? AND saga_id = ?"
                ),
                vec![Param::text(saga_type), saga_id.into()],
            )
            .await?;
        row.as_ref().map(parse).transpose()
    }

    async fn complete(&self, saga_type: &str, saga_id: Uuid) -> Result<()> {
        self.end(saga_type, saga_id, |record, now| record.complete(now))
            .await
    }

    async fn fail(&self, saga_type: &str, saga_id: Uuid, reason: &str) -> Result<()> {
        self.end(saga_type, saga_id, |record, now| record.fail(reason, now))
            .await
    }

    async fn load_by_status(
        &self,
        status: SagaStatus,
        query: SagaQuery,
    ) -> Result<Vec<SagaRecord>> {
        self.list(
            vec!["status = ?"],
            vec![Param::text(status.as_str())],
            &query,
        )
        .await
    }

    async fn load_timed_out(
        &self,
        before: DateTime<Utc>,
        query: SagaQuery,
    ) -> Result<Vec<SagaRecord>> {
        self.list(
            vec!["status IN ('RUNNING', 'COMPENSATING')", "updated_at < ?"],
            vec![before.into()],
            &query,
        )
        .await
    }

    async fn delete(&self, saga_type: &str, saga_id: Uuid) -> Result<()> {
        self.db
            .execute(
                "DELETE FROM tarodomain_sagas WHERE saga_type = ? AND saga_id = ?",
                vec![Param::text(saga_type), saga_id.into()],
            )
            .await
            .map(|_| ())
    }

    async fn count(&self, saga_type: Option<&str>, status: Option<SagaStatus>) -> Result<u64> {
        let mut conditions = Vec::new();
        let mut params = Vec::new();
        if let Some(saga_type) = saga_type {
            conditions.push("saga_type = ?");
            params.push(Param::text(saga_type));
        }
        if let Some(status) = status {
            conditions.push("status = ?");
            params.push(Param::text(status.as_str()));
        }
        let filter = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let row = self
            .db
            .fetch_one(
                &format!("SELECT COUNT(*) AS n FROM tarodomain_sagas {filter}"),
                params,
            )
            .await?;
        Ok(row
            .map_or(Ok(0), |row| row.i64("n"))?
            .try_into()
            .unwrap_or(0))
    }
}
