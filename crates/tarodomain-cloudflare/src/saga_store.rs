//! The D1 saga store (spec §9.1).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tarodomain::adapters::{SagaQuery, SagaRecord, SagaSaveInput, SagaStatus, SagaStoreAdapter};
use tarodomain::async_trait;
use tarodomain::domain::format_timestamp;
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::Uuid;

use crate::d1::{D1Db, Row, RowExt, Statement, json_text};
use crate::unit_of_work::D1UnitOfWork;

const COLUMNS: &str = "saga_type, saga_id, correlation_id, state, status, current_step, \
     failure_reason, started_at, updated_at, completed_at";
const ORDER: &str = "ORDER BY updated_at, saga_type, saga_id";

/// Sagas on D1, keyed by `(saga_type, saga_id)`. The write rules are
/// [`SagaRecord::saved`], `complete` and `fail`, the same as in memory.
///
/// Inside a unit of work, `load` sees the records the unit saved (the
/// overlay), so a strong-mode command that starts a saga and a handler that
/// continues it in the same unit read their own writes.
#[derive(Clone)]
pub struct D1SagaStore {
    db: D1Db,
    clock: Arc<dyn Clock>,
}

impl D1SagaStore {
    /// A store over `db`. Timestamps come from `clock`.
    pub fn new(db: D1Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }

    /// Writes the whole record (everything but `started_at`, which stays).
    async fn put(&self, record: SagaRecord) -> Result<()> {
        let statement = Statement::new(
            format!(
                "INSERT INTO tarodomain_sagas ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                 ON CONFLICT (saga_type, saga_id) DO UPDATE SET \
                 correlation_id = excluded.correlation_id, state = excluded.state, \
                 status = excluded.status, current_step = excluded.current_step, \
                 failure_reason = excluded.failure_reason, updated_at = excluded.updated_at, \
                 completed_at = excluded.completed_at"
            ),
            vec![
                json!(record.saga_type),
                json!(record.saga_id.to_string()),
                json!(record.correlation_id.to_string()),
                json_text(&record.state),
                json!(record.status.as_str()),
                json!(record.current_step),
                json!(record.failure_reason),
                json!(format_timestamp(record.started_at)),
                json!(format_timestamp(record.updated_at)),
                json!(record.completed_at.map(format_timestamp)),
            ],
        );
        let key = (record.saga_type.clone(), record.saga_id);
        D1UnitOfWork::with_active(&self.db, |overlay| {
            overlay.sagas.insert(key, record);
        });
        self.db.write(vec![statement]).await
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
        mut params: Vec<Value>,
        query: &SagaQuery,
    ) -> Result<Vec<SagaRecord>> {
        if let Some(saga_type) = &query.saga_type {
            conditions.push("saga_type = ?");
            params.push(json!(saga_type));
        }
        params.push(json!(
            query
                .limit
                .map_or(-1, |n| i64::try_from(n).unwrap_or(i64::MAX))
        ));
        let filter = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let rows = self
            .db
            .query(Statement::new(
                format!("SELECT {COLUMNS} FROM tarodomain_sagas {filter} {ORDER} LIMIT ?"),
                params,
            ))
            .await?;
        rows.iter().map(parse).collect()
    }
}

fn parse(row: &Row) -> Result<SagaRecord> {
    let status = row.require_str("status")?;
    Ok(SagaRecord {
        saga_id: row.require_uuid("saga_id")?,
        saga_type: row.require_str("saga_type")?.to_owned(),
        correlation_id: row.require_uuid("correlation_id")?,
        state: row.require_json("state")?,
        status: status.parse().map_err(|_| {
            FrameworkError::serialization_error(format!("unknown saga status {status:?}"))
        })?,
        current_step: row.require_str("current_step")?.to_owned(),
        failure_reason: row.str("failure_reason").map(str::to_owned),
        started_at: row.require_time("started_at")?,
        updated_at: row.require_time("updated_at")?,
        completed_at: row.optional_time("completed_at")?,
    })
}

#[async_trait]
impl SagaStoreAdapter for D1SagaStore {
    async fn save(&self, input: SagaSaveInput) -> Result<()> {
        let previous = self.load(&input.saga_type, input.saga_id).await?;
        let record = SagaRecord::saved(input, previous.as_ref(), self.clock.now());
        self.put(record).await
    }

    async fn load(&self, saga_type: &str, saga_id: Uuid) -> Result<Option<SagaRecord>> {
        let staged = D1UnitOfWork::with_active(&self.db, |overlay| {
            overlay.sagas.get(&(saga_type.to_owned(), saga_id)).cloned()
        })
        .flatten();
        if staged.is_some() {
            return Ok(staged);
        }
        let row = self
            .db
            .query_one(Statement::new(
                format!(
                    "SELECT {COLUMNS} FROM tarodomain_sagas WHERE saga_type = ? AND saga_id = ?"
                ),
                vec![json!(saga_type), json!(saga_id.to_string())],
            ))
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
        self.list(vec!["status = ?"], vec![json!(status.as_str())], &query)
            .await
    }

    async fn load_timed_out(
        &self,
        before: DateTime<Utc>,
        query: SagaQuery,
    ) -> Result<Vec<SagaRecord>> {
        self.list(
            vec!["status IN ('RUNNING', 'COMPENSATING')", "updated_at < ?"],
            vec![json!(format_timestamp(before))],
            &query,
        )
        .await
    }

    async fn delete(&self, saga_type: &str, saga_id: Uuid) -> Result<()> {
        D1UnitOfWork::with_active(&self.db, |overlay| {
            overlay.sagas.remove(&(saga_type.to_owned(), saga_id));
        });
        self.db
            .write(vec![Statement::new(
                "DELETE FROM tarodomain_sagas WHERE saga_type = ? AND saga_id = ?",
                vec![json!(saga_type), json!(saga_id.to_string())],
            )])
            .await
    }

    async fn count(&self, saga_type: Option<&str>, status: Option<SagaStatus>) -> Result<u64> {
        let mut conditions = Vec::new();
        let mut params = Vec::new();
        if let Some(saga_type) = saga_type {
            conditions.push("saga_type = ?");
            params.push(json!(saga_type));
        }
        if let Some(status) = status {
            conditions.push("status = ?");
            params.push(json!(status.as_str()));
        }
        let filter = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        let row = self
            .db
            .query_one(Statement::new(
                format!("SELECT COUNT(*) AS n FROM tarodomain_sagas {filter}"),
                params,
            ))
            .await?;
        Ok(row.map_or(0, |row| {
            u64::try_from(row.i64("n").unwrap_or(0)).unwrap_or(0)
        }))
    }
}
