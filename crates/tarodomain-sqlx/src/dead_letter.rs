//! The SQL dead-letter store (spec §21.1).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use tarodomain::adapters::{DeadLetterEntry, DeadLetterInput, DeadLetterStore};
use tarodomain::async_trait;
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::Uuid;

use crate::db::{Db, Param, Row};

const COLUMNS: &str = "id, handler_name, event_id, event_type, payload, error_code, \
     error_message, attempts, created_at";

/// Dead letters on SQL. IDs are auto-increment, so they increase in write
/// order and are never reused.
#[derive(Clone)]
pub struct SqlxDeadLetterStore {
    db: Db,
    clock: Arc<dyn Clock>,
}

impl SqlxDeadLetterStore {
    /// A store over `db`. `created_at` comes from `clock`.
    pub fn new(db: Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }
}

fn parse(row: &Row) -> Result<DeadLetterEntry> {
    Ok(DeadLetterEntry {
        id: u64::try_from(row.i64("id")?).unwrap_or(0),
        handler_name: row.str("handler_name")?,
        event_id: row.uuid("event_id")?,
        event_type: row.str("event_type")?,
        payload: row.json("payload")?,
        error_code: row.str("error_code")?,
        error_message: row.str("error_message")?,
        attempts: u32::try_from(row.i64("attempts")?).unwrap_or(u32::MAX),
        created_at: row.time("created_at")?,
    })
}

#[async_trait]
impl DeadLetterStore for SqlxDeadLetterStore {
    async fn write(&self, input: DeadLetterInput) -> Result<u64> {
        let rows = self
            .db
            .fetch(
                "INSERT INTO tarodomain_dead_letters (handler_name, event_id, event_type, \
                 payload, error_code, error_message, attempts, created_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
                vec![
                    Param::text(input.handler_name),
                    input.event_id.into(),
                    Param::text(input.event_type),
                    Param::Json(input.payload),
                    Param::text(input.error_code),
                    Param::text(input.error_message),
                    input.attempts.into(),
                    self.clock.now().into(),
                ],
            )
            .await?;
        let id = rows
            .first()
            .ok_or_else(|| FrameworkError::dead_letter("the insert returned no ID"))?
            .i64("id")?;
        Ok(u64::try_from(id).unwrap_or(0))
    }

    async fn list(&self, handler_name: &str, limit: Option<usize>) -> Result<Vec<DeadLetterEntry>> {
        let rows = self
            .db
            .fetch(
                &format!(
                    "SELECT {COLUMNS} FROM tarodomain_dead_letters \
                     WHERE handler_name = ? ORDER BY id LIMIT ?"
                ),
                vec![Param::text(handler_name), self.db.limit(limit)],
            )
            .await?;
        rows.iter().map(parse).collect()
    }

    async fn get(&self, event_id: Uuid, handler_name: &str) -> Result<Option<DeadLetterEntry>> {
        let row = self
            .db
            .fetch_one(
                &format!(
                    "SELECT {COLUMNS} FROM tarodomain_dead_letters \
                     WHERE event_id = ? AND handler_name = ? ORDER BY id DESC LIMIT 1"
                ),
                vec![event_id.into(), Param::text(handler_name)],
            )
            .await?;
        row.as_ref().map(parse).transpose()
    }

    async fn get_by_id(&self, id: u64) -> Result<Option<DeadLetterEntry>> {
        let row = self
            .db
            .fetch_one(
                &format!("SELECT {COLUMNS} FROM tarodomain_dead_letters WHERE id = ?"),
                vec![id.into()],
            )
            .await?;
        row.as_ref().map(parse).transpose()
    }

    async fn delete(&self, id: u64) -> Result<()> {
        self.db
            .execute(
                "DELETE FROM tarodomain_dead_letters WHERE id = ?",
                vec![id.into()],
            )
            .await
            .map(|_| ())
    }

    async fn count(&self, handler_name: Option<&str>) -> Result<u64> {
        let (filter, params) = match handler_name {
            Some(name) => ("WHERE handler_name = ?", vec![Param::text(name)]),
            None => ("", Vec::new()),
        };
        let row = self
            .db
            .fetch_one(
                &format!("SELECT COUNT(*) AS n FROM tarodomain_dead_letters {filter}"),
                params,
            )
            .await?;
        Ok(row
            .map_or(Ok(0), |row| row.i64("n"))?
            .try_into()
            .unwrap_or(0))
    }

    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64> {
        self.db
            .execute(
                "DELETE FROM tarodomain_dead_letters WHERE created_at < ?",
                vec![older_than.into()],
            )
            .await
    }
}
