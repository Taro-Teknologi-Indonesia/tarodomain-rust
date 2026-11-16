//! The SQL idempotency stores: processed events (spec §12.3) and processed
//! commands (§14.1).
//!
//! Marks are idempotent writes (`ON CONFLICT DO NOTHING`, §14.3), so marking
//! twice keeps the first record.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use tarodomain::adapters::{CommandIdempotencyStore, ProcessedCommand, ProcessedEventStore};
use tarodomain::async_trait;
use tarodomain::error::Result;
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::Uuid;

use crate::db::{Db, Param, Row};

fn count_of(row: Option<Row>) -> Result<u64> {
    let n = row.map_or(Ok(0), |row| row.i64("n"))?;
    Ok(u64::try_from(n).unwrap_or(0))
}

/// Which handler (and instance) a processed-event query is about.
fn handler_filter(
    handler_name: &str,
    handler_instance: Option<&str>,
) -> (&'static str, Vec<Param>) {
    match handler_instance {
        Some(instance) => (
            "handler_name = ? AND handler_instance = ?",
            vec![Param::text(handler_name), Param::text(instance)],
        ),
        None => ("handler_name = ?", vec![Param::text(handler_name)]),
    }
}

/// Processed events on SQL (§12.3), keyed by handler, instance and event.
#[derive(Clone)]
pub struct SqlxProcessedEventStore {
    db: Db,
    clock: Arc<dyn Clock>,
}

impl SqlxProcessedEventStore {
    /// A store over `db`. `processed_at` comes from `clock`.
    pub fn new(db: Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }
}

#[async_trait]
impl ProcessedEventStore for SqlxProcessedEventStore {
    async fn exists(
        &self,
        handler_name: &str,
        handler_instance: &str,
        event_id: Uuid,
    ) -> Result<bool> {
        let row = self
            .db
            .fetch_one(
                "SELECT event_id FROM tarodomain_processed_events \
                 WHERE handler_name = ? AND handler_instance = ? AND event_id = ?",
                vec![
                    Param::text(handler_name),
                    Param::text(handler_instance),
                    event_id.into(),
                ],
            )
            .await?;
        Ok(row.is_some())
    }

    async fn mark_processed(
        &self,
        handler_name: &str,
        handler_instance: &str,
        event_id: Uuid,
    ) -> Result<()> {
        self.db
            .execute(
                "INSERT INTO tarodomain_processed_events \
                 (handler_name, handler_instance, event_id, processed_at) VALUES (?, ?, ?, ?) \
                 ON CONFLICT DO NOTHING",
                vec![
                    Param::text(handler_name),
                    Param::text(handler_instance),
                    event_id.into(),
                    self.clock.now().into(),
                ],
            )
            .await
            .map(|_| ())
    }

    async fn count(&self, handler_name: &str, handler_instance: Option<&str>) -> Result<u64> {
        let (filter, params) = handler_filter(handler_name, handler_instance);
        let row = self
            .db
            .fetch_one(
                &format!("SELECT COUNT(*) AS n FROM tarodomain_processed_events WHERE {filter}"),
                params,
            )
            .await?;
        count_of(row)
    }

    async fn delete_by_handler(
        &self,
        handler_name: &str,
        handler_instance: Option<&str>,
    ) -> Result<u64> {
        let (filter, params) = handler_filter(handler_name, handler_instance);
        self.db
            .execute(
                &format!("DELETE FROM tarodomain_processed_events WHERE {filter}"),
                params,
            )
            .await
    }

    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64> {
        self.db
            .execute(
                "DELETE FROM tarodomain_processed_events WHERE processed_at < ?",
                vec![older_than.into()],
            )
            .await
    }
}

/// Processed commands on SQL (§14.1), keyed by command ID.
#[derive(Clone)]
pub struct SqlxCommandIdempotencyStore {
    db: Db,
    clock: Arc<dyn Clock>,
}

impl SqlxCommandIdempotencyStore {
    /// A store over `db`. `processed_at` comes from `clock`.
    pub fn new(db: Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }
}

#[async_trait]
impl CommandIdempotencyStore for SqlxCommandIdempotencyStore {
    async fn mark_processed(
        &self,
        command_id: Uuid,
        command_type: &str,
        handler_name: &str,
    ) -> Result<()> {
        self.db
            .execute(
                "INSERT INTO tarodomain_processed_commands \
                 (command_id, command_type, handler_name, processed_at) VALUES (?, ?, ?, ?) \
                 ON CONFLICT DO NOTHING",
                vec![
                    command_id.into(),
                    Param::text(command_type),
                    Param::text(handler_name),
                    self.clock.now().into(),
                ],
            )
            .await
            .map(|_| ())
    }

    async fn get_processed_at(&self, command_id: Uuid) -> Result<Option<DateTime<Utc>>> {
        let row = self
            .db
            .fetch_one(
                "SELECT processed_at FROM tarodomain_processed_commands WHERE command_id = ?",
                vec![command_id.into()],
            )
            .await?;
        row.map(|row| row.time("processed_at")).transpose()
    }

    async fn count(&self, handler_name: Option<&str>) -> Result<u64> {
        let (filter, params) = match handler_name {
            Some(name) => ("WHERE handler_name = ?", vec![Param::text(name)]),
            None => ("", Vec::new()),
        };
        let row = self
            .db
            .fetch_one(
                &format!("SELECT COUNT(*) AS n FROM tarodomain_processed_commands {filter}"),
                params,
            )
            .await?;
        count_of(row)
    }

    async fn list(
        &self,
        handler_name: &str,
        limit: Option<usize>,
    ) -> Result<Vec<ProcessedCommand>> {
        let rows = self
            .db
            .fetch(
                "SELECT command_id, command_type, handler_name, processed_at \
                 FROM tarodomain_processed_commands WHERE handler_name = ? \
                 ORDER BY processed_at, command_id LIMIT ?",
                vec![Param::text(handler_name), self.db.limit(limit)],
            )
            .await?;
        rows.iter()
            .map(|row| {
                Ok(ProcessedCommand {
                    command_id: row.uuid("command_id")?,
                    command_type: row.str("command_type")?,
                    handler_name: row.str("handler_name")?,
                    processed_at: row.time("processed_at")?,
                })
            })
            .collect()
    }

    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64> {
        self.db
            .execute(
                "DELETE FROM tarodomain_processed_commands WHERE processed_at < ?",
                vec![older_than.into()],
            )
            .await
    }
}
