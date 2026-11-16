//! The D1 idempotency stores: processed events (spec §12.3) and processed
//! commands (§14.1).
//!
//! Marks are idempotent writes (`ON CONFLICT DO NOTHING`, §14.3), so marking
//! twice keeps the first record. Inside a unit of work, `exists` reads the
//! unit's own marks first.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::{Value, json};
use tarodomain::adapters::{CommandIdempotencyStore, ProcessedCommand, ProcessedEventStore};
use tarodomain::async_trait;
use tarodomain::domain::format_timestamp;
use tarodomain::error::Result;
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::Uuid;

use crate::d1::{D1Db, RowExt, Statement};
use crate::unit_of_work::D1UnitOfWork;

fn count_of(row: Option<crate::d1::Row>) -> u64 {
    row.map_or(0, |row| {
        u64::try_from(row.i64("n").unwrap_or(0)).unwrap_or(0)
    })
}

/// Processed-event markers on D1, keyed by `(handler_name, handler_instance,
/// event_id)`.
#[derive(Clone)]
pub struct D1ProcessedEventStore {
    db: D1Db,
    clock: Arc<dyn Clock>,
}

impl D1ProcessedEventStore {
    /// A store over `db`. Timestamps come from `clock`.
    pub fn new(db: D1Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }
}

#[async_trait]
impl ProcessedEventStore for D1ProcessedEventStore {
    async fn exists(
        &self,
        handler_name: &str,
        handler_instance: &str,
        event_id: Uuid,
    ) -> Result<bool> {
        let key = (
            handler_name.to_owned(),
            handler_instance.to_owned(),
            event_id,
        );
        let staged =
            D1UnitOfWork::with_active(&self.db, |overlay| overlay.processed_events.contains(&key));
        if staged == Some(true) {
            return Ok(true);
        }
        let row = self
            .db
            .query_one(Statement::new(
                "SELECT 1 AS n FROM tarodomain_processed_events \
                 WHERE handler_name = ? AND handler_instance = ? AND event_id = ?",
                vec![
                    json!(handler_name),
                    json!(handler_instance),
                    json!(event_id.to_string()),
                ],
            ))
            .await?;
        Ok(row.is_some())
    }

    async fn mark_processed(
        &self,
        handler_name: &str,
        handler_instance: &str,
        event_id: Uuid,
    ) -> Result<()> {
        let key = (
            handler_name.to_owned(),
            handler_instance.to_owned(),
            event_id,
        );
        D1UnitOfWork::with_active(&self.db, |overlay| {
            overlay.processed_events.insert(key);
        });
        self.db
            .write(vec![Statement::new(
                "INSERT INTO tarodomain_processed_events \
                 (handler_name, handler_instance, event_id, processed_at) VALUES (?, ?, ?, ?) \
                 ON CONFLICT DO NOTHING",
                vec![
                    json!(handler_name),
                    json!(handler_instance),
                    json!(event_id.to_string()),
                    json!(format_timestamp(self.clock.now())),
                ],
            )])
            .await
    }

    async fn count(&self, handler_name: &str, handler_instance: Option<&str>) -> Result<u64> {
        let (filter, params) = handler_filter(handler_name, handler_instance);
        let row = self
            .db
            .query_one(Statement::new(
                format!("SELECT COUNT(*) AS n FROM tarodomain_processed_events WHERE {filter}"),
                params,
            ))
            .await?;
        Ok(count_of(row))
    }

    async fn delete_by_handler(
        &self,
        handler_name: &str,
        handler_instance: Option<&str>,
    ) -> Result<u64> {
        let (filter, params) = handler_filter(handler_name, handler_instance);
        self.db
            .write_counting(
                Statement::new(
                    format!("DELETE FROM tarodomain_processed_events WHERE {filter}"),
                    params.clone(),
                ),
                Statement::new(
                    format!("SELECT COUNT(*) AS n FROM tarodomain_processed_events WHERE {filter}"),
                    params,
                ),
            )
            .await
    }

    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let params = vec![json!(format_timestamp(older_than))];
        self.db
            .write_counting(
                Statement::new(
                    "DELETE FROM tarodomain_processed_events WHERE processed_at < ?",
                    params.clone(),
                ),
                Statement::new(
                    "SELECT COUNT(*) AS n FROM tarodomain_processed_events WHERE processed_at < ?",
                    params,
                ),
            )
            .await
    }
}

fn handler_filter(
    handler_name: &str,
    handler_instance: Option<&str>,
) -> (&'static str, Vec<Value>) {
    match handler_instance {
        Some(instance) => (
            "handler_name = ? AND handler_instance = ?",
            vec![json!(handler_name), json!(instance)],
        ),
        None => ("handler_name = ?", vec![json!(handler_name)]),
    }
}

/// Processed-command markers on D1, keyed by `command_id`.
#[derive(Clone)]
pub struct D1CommandIdempotencyStore {
    db: D1Db,
    clock: Arc<dyn Clock>,
}

impl D1CommandIdempotencyStore {
    /// A store over `db`. Timestamps come from `clock`.
    pub fn new(db: D1Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }
}

#[async_trait]
impl CommandIdempotencyStore for D1CommandIdempotencyStore {
    async fn mark_processed(
        &self,
        command_id: Uuid,
        command_type: &str,
        handler_name: &str,
    ) -> Result<()> {
        let now = self.clock.now();
        D1UnitOfWork::with_active(&self.db, |overlay| {
            overlay.processed_commands.entry(command_id).or_insert(now);
        });
        self.db
            .write(vec![Statement::new(
                "INSERT INTO tarodomain_processed_commands \
                 (command_id, command_type, handler_name, processed_at) VALUES (?, ?, ?, ?) \
                 ON CONFLICT DO NOTHING",
                vec![
                    json!(command_id.to_string()),
                    json!(command_type),
                    json!(handler_name),
                    json!(format_timestamp(now)),
                ],
            )])
            .await
    }

    async fn get_processed_at(&self, command_id: Uuid) -> Result<Option<DateTime<Utc>>> {
        let staged = D1UnitOfWork::with_active(&self.db, |overlay| {
            overlay.processed_commands.get(&command_id).copied()
        })
        .flatten();
        if staged.is_some() {
            return Ok(staged);
        }
        let row = self
            .db
            .query_one(Statement::new(
                "SELECT processed_at FROM tarodomain_processed_commands WHERE command_id = ?",
                vec![json!(command_id.to_string())],
            ))
            .await?;
        row.map(|row| row.require_time("processed_at")).transpose()
    }

    async fn count(&self, handler_name: Option<&str>) -> Result<u64> {
        let (filter, params) = match handler_name {
            Some(name) => ("WHERE handler_name = ?", vec![json!(name)]),
            None => ("", Vec::new()),
        };
        let row = self
            .db
            .query_one(Statement::new(
                format!("SELECT COUNT(*) AS n FROM tarodomain_processed_commands {filter}"),
                params,
            ))
            .await?;
        Ok(count_of(row))
    }

    async fn list(
        &self,
        handler_name: &str,
        limit: Option<usize>,
    ) -> Result<Vec<ProcessedCommand>> {
        let rows = self
            .db
            .query(Statement::new(
                "SELECT command_id, command_type, handler_name, processed_at \
                 FROM tarodomain_processed_commands WHERE handler_name = ? \
                 ORDER BY processed_at, command_id LIMIT ?",
                vec![
                    json!(handler_name),
                    json!(limit.map_or(-1, |n| i64::try_from(n).unwrap_or(i64::MAX))),
                ],
            ))
            .await?;
        rows.iter()
            .map(|row| {
                Ok(ProcessedCommand {
                    command_id: row.require_uuid("command_id")?,
                    command_type: row.require_str("command_type")?.to_owned(),
                    handler_name: row.require_str("handler_name")?.to_owned(),
                    processed_at: row.require_time("processed_at")?,
                })
            })
            .collect()
    }

    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let params = vec![json!(format_timestamp(older_than))];
        self.db
            .write_counting(
                Statement::new(
                    "DELETE FROM tarodomain_processed_commands WHERE processed_at < ?",
                    params.clone(),
                ),
                Statement::new(
                    "SELECT COUNT(*) AS n FROM tarodomain_processed_commands WHERE processed_at < ?",
                    params,
                ),
            )
            .await
    }
}
