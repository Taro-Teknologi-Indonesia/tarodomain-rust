//! The in-memory idempotency stores (spec §12.3, §14.1).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use tarodomain::adapters::{CommandIdempotencyStore, ProcessedCommand, ProcessedEventStore};
use tarodomain::async_trait;
use tarodomain::error::Result;
use tarodomain::kernel::ids::Uuid;

use crate::LocalDatabase;

/// The local provider's record of the events each handler has handled, in a
/// [`LocalDatabase`].
#[derive(Clone, Debug)]
pub struct InMemoryProcessedEventStore {
    db: LocalDatabase,
}

impl InMemoryProcessedEventStore {
    /// A store over `db`'s processed event table.
    pub fn new(db: LocalDatabase) -> Self {
        Self { db }
    }
}

/// When each event was marked, by `(handler_name, handler_instance,
/// event_id)`.
#[derive(Clone, Default)]
struct ProcessedEventTable(BTreeMap<(String, String, Uuid), DateTime<Utc>>);

impl ProcessedEventTable {
    fn is_handler(key: &(String, String, Uuid), name: &str, instance: Option<&str>) -> bool {
        key.0 == name && instance.is_none_or(|instance| key.1 == instance)
    }
}

#[async_trait]
impl ProcessedEventStore for InMemoryProcessedEventStore {
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
        Ok(self
            .db
            .read(|table: &ProcessedEventTable| table.0.contains_key(&key)))
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
        let clock = self.db.clock();
        self.db
            .write(|table: &mut ProcessedEventTable| {
                table.0.entry(key).or_insert_with(|| clock.now());
            })
            .await;
        Ok(())
    }

    async fn count(&self, handler_name: &str, handler_instance: Option<&str>) -> Result<u64> {
        Ok(self.db.read(|table: &ProcessedEventTable| {
            let keys = table.0.keys();
            let handled = keys
                .filter(|key| ProcessedEventTable::is_handler(key, handler_name, handler_instance));
            handled.count() as u64
        }))
    }

    async fn delete_by_handler(
        &self,
        handler_name: &str,
        handler_instance: Option<&str>,
    ) -> Result<u64> {
        let deleted = self
            .db
            .write(|table: &mut ProcessedEventTable| {
                let before = table.0.len();
                table.0.retain(|key, _| {
                    !ProcessedEventTable::is_handler(key, handler_name, handler_instance)
                });
                (before - table.0.len()) as u64
            })
            .await;
        Ok(deleted)
    }

    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let deleted = self
            .db
            .write(|table: &mut ProcessedEventTable| {
                let before = table.0.len();
                table.0.retain(|_, &mut marked_at| marked_at >= older_than);
                (before - table.0.len()) as u64
            })
            .await;
        Ok(deleted)
    }
}

/// The local provider's record of processed commands, in a
/// [`LocalDatabase`].
#[derive(Clone, Debug)]
pub struct InMemoryCommandIdempotencyStore {
    db: LocalDatabase,
}

impl InMemoryCommandIdempotencyStore {
    /// A store over `db`'s processed command table.
    pub fn new(db: LocalDatabase) -> Self {
        Self { db }
    }
}

/// The processed commands by ID.
#[derive(Clone, Default)]
struct ProcessedCommandTable(BTreeMap<Uuid, ProcessedCommand>);

#[async_trait]
impl CommandIdempotencyStore for InMemoryCommandIdempotencyStore {
    async fn mark_processed(
        &self,
        command_id: Uuid,
        command_type: &str,
        handler_name: &str,
    ) -> Result<()> {
        let clock = self.db.clock();
        self.db
            .write(|table: &mut ProcessedCommandTable| {
                table
                    .0
                    .entry(command_id)
                    .or_insert_with(|| ProcessedCommand {
                        command_id,
                        command_type: command_type.to_owned(),
                        handler_name: handler_name.to_owned(),
                        processed_at: clock.now(),
                    });
            })
            .await;
        Ok(())
    }

    async fn get_processed_at(&self, command_id: Uuid) -> Result<Option<DateTime<Utc>>> {
        Ok(self.db.read(|table: &ProcessedCommandTable| {
            table.0.get(&command_id).map(|command| command.processed_at)
        }))
    }

    async fn count(&self, handler_name: Option<&str>) -> Result<u64> {
        Ok(self.db.read(|table: &ProcessedCommandTable| {
            let commands = table.0.values();
            let handled =
                commands.filter(|command| handler_name.is_none_or(|h| command.handler_name == h));
            handled.count() as u64
        }))
    }

    async fn list(
        &self,
        handler_name: &str,
        limit: Option<usize>,
    ) -> Result<Vec<ProcessedCommand>> {
        Ok(self.db.read(|table: &ProcessedCommandTable| {
            let commands = table.0.values();
            let mut handled: Vec<_> = commands
                .filter(|command| command.handler_name == handler_name)
                .collect();
            // The map is in ID order already, and the sort is stable.
            handled.sort_by_key(|command| command.processed_at);
            let limited = handled.into_iter().take(limit.unwrap_or(usize::MAX));
            limited.cloned().collect()
        }))
    }

    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let deleted = self
            .db
            .write(|table: &mut ProcessedCommandTable| {
                let before = table.0.len();
                table
                    .0
                    .retain(|_, command| command.processed_at >= older_than);
                (before - table.0.len()) as u64
            })
            .await;
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeDelta;
    use tarodomain::kernel::clock::{Clock, ManualClock};
    use tarodomain::kernel::ids::new_v7;

    use super::*;

    fn database() -> (LocalDatabase, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(DateTime::UNIX_EPOCH));
        (LocalDatabase::with_clock(clock.clone()), clock)
    }

    #[tokio::test]
    async fn processed_events_are_per_handler_instance() {
        let (db, clock) = database();
        let store = InMemoryProcessedEventStore::new(db);
        let (first, second) = (new_v7(), new_v7());
        store
            .mark_processed("Balance", "Balance", first)
            .await
            .unwrap();
        clock.advance(TimeDelta::seconds(10));
        store
            .mark_processed("Balance", "Balance", first)
            .await
            .unwrap();
        store
            .mark_processed("Balance", "shard-2", second)
            .await
            .unwrap();
        store
            .mark_processed("Transfer", "saga-1", first)
            .await
            .unwrap();

        assert!(store.exists("Balance", "Balance", first).await.unwrap());
        assert!(!store.exists("Balance", "shard-2", first).await.unwrap());
        assert_eq!(store.count("Balance", None).await.unwrap(), 2);
        assert_eq!(store.count("Balance", Some("shard-2")).await.unwrap(), 1);

        // The first mark kept its time, so it is the only old one.
        let deleted = store.delete_old(clock.now()).await.unwrap();
        assert_eq!(deleted, 1);
        assert!(!store.exists("Balance", "Balance", first).await.unwrap());
        assert_eq!(store.delete_by_handler("Balance", None).await.unwrap(), 1);
        assert_eq!(store.count("Transfer", None).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn processed_commands_keep_the_first_mark() {
        let (db, clock) = database();
        let store = InMemoryCommandIdempotencyStore::new(db);
        let (a, b, c) = (new_v7(), new_v7(), new_v7());
        assert!(!store.exists(a).await.unwrap());
        store
            .mark_processed(b, "Deposit", "DepositHandler")
            .await
            .unwrap();
        clock.advance(TimeDelta::seconds(1));
        store
            .mark_processed(a, "Deposit", "DepositHandler")
            .await
            .unwrap();
        store
            .mark_processed(b, "Other", "OtherHandler")
            .await
            .unwrap();
        store
            .mark_processed(c, "Open", "OpenHandler")
            .await
            .unwrap();

        assert!(store.exists(a).await.unwrap());
        let processed_at = store.get_processed_at(b).await.unwrap();
        assert_eq!(processed_at, Some(DateTime::UNIX_EPOCH));
        assert_eq!(store.count(None).await.unwrap(), 3);
        assert_eq!(store.count(Some("DepositHandler")).await.unwrap(), 2);

        let listed = store.list("DepositHandler", None).await.unwrap();
        let ids: Vec<_> = listed.iter().map(|command| command.command_id).collect();
        assert_eq!(ids, [b, a]);
        assert_eq!(listed[0].command_type, "Deposit");
        assert_eq!(
            store.list("DepositHandler", Some(1)).await.unwrap().len(),
            1
        );

        assert_eq!(store.delete_old(clock.now()).await.unwrap(), 1);
        assert!(!store.exists(b).await.unwrap());
    }
}
