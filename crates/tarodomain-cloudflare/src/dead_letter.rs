//! The D1 dead-letter store (spec §21.1).

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::json;
use tarodomain::adapters::{DeadLetterEntry, DeadLetterInput, DeadLetterStore};
use tarodomain::async_trait;
use tarodomain::domain::format_timestamp;
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::Uuid;

use crate::d1::{D1Db, Row, RowExt, Statement, json_text};

const COLUMNS: &str = "id, handler_name, event_id, event_type, payload, error_code, \
     error_message, attempts, created_at";

/// Dead letters on D1. IDs are `AUTOINCREMENT`, so they increase in write
/// order and are never reused.
#[derive(Clone)]
pub struct D1DeadLetterStore {
    db: D1Db,
    clock: Arc<dyn Clock>,
}

impl D1DeadLetterStore {
    /// A store over `db`. `created_at` comes from `clock`.
    pub fn new(db: D1Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }
}

fn parse(row: &Row) -> Result<DeadLetterEntry> {
    Ok(DeadLetterEntry {
        id: u64::try_from(row.require_i64("id")?).unwrap_or(0),
        handler_name: row.require_str("handler_name")?.to_owned(),
        event_id: row.require_uuid("event_id")?,
        event_type: row.require_str("event_type")?.to_owned(),
        payload: row.require_json("payload")?,
        error_code: row.require_str("error_code")?.to_owned(),
        error_message: row.require_str("error_message")?.to_owned(),
        attempts: u32::try_from(row.require_i64("attempts")?).unwrap_or(u32::MAX),
        created_at: row.require_time("created_at")?,
    })
}

#[async_trait]
impl DeadLetterStore for D1DeadLetterStore {
    /// Returns the new ID. Inside a unit of work the write is staged and the
    /// ID isn't known before the commit, so it returns `0`.
    async fn write(&self, input: DeadLetterInput) -> Result<u64> {
        let rows = self
            .db
            .write_returning(Statement::new(
                "INSERT INTO tarodomain_dead_letters (handler_name, event_id, event_type, \
                 payload, error_code, error_message, attempts, created_at) \
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?) RETURNING id",
                vec![
                    json!(input.handler_name),
                    json!(input.event_id.to_string()),
                    json!(input.event_type),
                    json_text(&input.payload),
                    json!(input.error_code),
                    json!(input.error_message),
                    json!(input.attempts),
                    json!(format_timestamp(self.clock.now())),
                ],
            ))
            .await?;
        match rows {
            None => Ok(0),
            Some(rows) => {
                let id = rows
                    .first()
                    .ok_or_else(|| FrameworkError::dead_letter("the insert returned no ID"))?
                    .require_i64("id")?;
                Ok(u64::try_from(id).unwrap_or(0))
            }
        }
    }

    async fn list(&self, handler_name: &str, limit: Option<usize>) -> Result<Vec<DeadLetterEntry>> {
        let rows = self
            .db
            .query(Statement::new(
                format!(
                    "SELECT {COLUMNS} FROM tarodomain_dead_letters \
                     WHERE handler_name = ? ORDER BY id LIMIT ?"
                ),
                vec![
                    json!(handler_name),
                    json!(limit.map_or(-1, |n| i64::try_from(n).unwrap_or(i64::MAX))),
                ],
            ))
            .await?;
        rows.iter().map(parse).collect()
    }

    async fn get(&self, event_id: Uuid, handler_name: &str) -> Result<Option<DeadLetterEntry>> {
        let row = self
            .db
            .query_one(Statement::new(
                format!(
                    "SELECT {COLUMNS} FROM tarodomain_dead_letters \
                     WHERE event_id = ? AND handler_name = ? ORDER BY id DESC LIMIT 1"
                ),
                vec![json!(event_id.to_string()), json!(handler_name)],
            ))
            .await?;
        row.as_ref().map(parse).transpose()
    }

    async fn get_by_id(&self, id: u64) -> Result<Option<DeadLetterEntry>> {
        let row = self
            .db
            .query_one(Statement::new(
                format!("SELECT {COLUMNS} FROM tarodomain_dead_letters WHERE id = ?"),
                vec![json!(id)],
            ))
            .await?;
        row.as_ref().map(parse).transpose()
    }

    async fn delete(&self, id: u64) -> Result<()> {
        self.db
            .write(vec![Statement::new(
                "DELETE FROM tarodomain_dead_letters WHERE id = ?",
                vec![json!(id)],
            )])
            .await
    }

    async fn count(&self, handler_name: Option<&str>) -> Result<u64> {
        let (filter, params) = match handler_name {
            Some(name) => ("WHERE handler_name = ?", vec![json!(name)]),
            None => ("", Vec::new()),
        };
        let row = self
            .db
            .query_one(Statement::new(
                format!("SELECT COUNT(*) AS n FROM tarodomain_dead_letters {filter}"),
                params,
            ))
            .await?;
        Ok(row.map_or(0, |row| {
            u64::try_from(row.i64("n").unwrap_or(0)).unwrap_or(0)
        }))
    }

    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let params = vec![json!(format_timestamp(older_than))];
        self.db
            .write_counting(
                Statement::new(
                    "DELETE FROM tarodomain_dead_letters WHERE created_at < ?",
                    params.clone(),
                ),
                Statement::new(
                    "SELECT COUNT(*) AS n FROM tarodomain_dead_letters WHERE created_at < ?",
                    params,
                ),
            )
            .await
    }
}
