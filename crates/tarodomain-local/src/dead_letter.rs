//! The in-memory dead-letter store (spec §21.1).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use tarodomain::adapters::{DeadLetterEntry, DeadLetterInput, DeadLetterStore};
use tarodomain::async_trait;
use tarodomain::error::Result;
use tarodomain::kernel::ids::Uuid;

use crate::LocalDatabase;

/// The local provider's dead-letter store, in a [`LocalDatabase`]. IDs count
/// up from 1 and are never reused, as with SQL `AUTOINCREMENT`.
#[derive(Clone, Debug)]
pub struct InMemoryDeadLetterStore {
    db: LocalDatabase,
}

impl InMemoryDeadLetterStore {
    /// A store over `db`'s dead-letter table.
    pub fn new(db: LocalDatabase) -> Self {
        Self { db }
    }
}

/// The entries by ID, and the last ID given out.
#[derive(Clone, Default)]
struct DeadLetterTable {
    entries: BTreeMap<u64, DeadLetterEntry>,
    last_id: u64,
}

impl DeadLetterTable {
    fn of<'a>(
        &'a self,
        handler_name: &'a str,
    ) -> impl DoubleEndedIterator<Item = &'a DeadLetterEntry> {
        let entries = self.entries.values();
        entries.filter(move |entry| entry.handler_name == handler_name)
    }
}

#[async_trait]
impl DeadLetterStore for InMemoryDeadLetterStore {
    async fn write(&self, input: DeadLetterInput) -> Result<u64> {
        let clock = self.db.clock();
        let id = self
            .db
            .write(|table: &mut DeadLetterTable| {
                table.last_id += 1;
                let id = table.last_id;
                let entry = DeadLetterEntry::new(id, input, clock.now());
                table.entries.insert(id, entry);
                id
            })
            .await;
        Ok(id)
    }

    async fn list(&self, handler_name: &str, limit: Option<usize>) -> Result<Vec<DeadLetterEntry>> {
        Ok(self.db.read(|table: &DeadLetterTable| {
            let entries = table.of(handler_name);
            entries.take(limit.unwrap_or(usize::MAX)).cloned().collect()
        }))
    }

    async fn get(&self, event_id: Uuid, handler_name: &str) -> Result<Option<DeadLetterEntry>> {
        Ok(self.db.read(|table: &DeadLetterTable| {
            let mut entries = table.of(handler_name);
            entries.rfind(|entry| entry.event_id == event_id).cloned()
        }))
    }

    async fn get_by_id(&self, id: u64) -> Result<Option<DeadLetterEntry>> {
        Ok(self
            .db
            .read(|table: &DeadLetterTable| table.entries.get(&id).cloned()))
    }

    async fn delete(&self, id: u64) -> Result<()> {
        self.db
            .write(|table: &mut DeadLetterTable| table.entries.remove(&id))
            .await;
        Ok(())
    }

    async fn count(&self, handler_name: Option<&str>) -> Result<u64> {
        Ok(self.db.read(|table: &DeadLetterTable| match handler_name {
            Some(handler_name) => table.of(handler_name).count() as u64,
            None => table.entries.len() as u64,
        }))
    }

    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let deleted = self
            .db
            .write(|table: &mut DeadLetterTable| {
                let before = table.entries.len();
                table
                    .entries
                    .retain(|_, entry| entry.created_at >= older_than);
                (before - table.entries.len()) as u64
            })
            .await;
        Ok(deleted)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeDelta;
    use serde_json::json;
    use tarodomain::kernel::clock::{Clock, ManualClock};
    use tarodomain::kernel::ids::new_v7;

    use super::*;

    fn input(handler_name: &str, event_id: Uuid, attempts: u32) -> DeadLetterInput {
        DeadLetterInput {
            handler_name: handler_name.into(),
            event_id,
            event_type: "MoneyDeposited".into(),
            payload: json!({ "amount": 10 }),
            error_code: "PROJECTION_HANDLER_FAILED".into(),
            error_message: "boom".into(),
            attempts,
        }
    }

    #[tokio::test]
    async fn ids_increase_and_are_never_reused() {
        let store = InMemoryDeadLetterStore::new(LocalDatabase::new());
        let event = new_v7();
        assert_eq!(store.write(input("Balance", event, 1)).await.unwrap(), 1);
        assert_eq!(store.write(input("Balance", event, 2)).await.unwrap(), 2);
        store.delete(2).await.unwrap();
        store.delete(2).await.unwrap();
        assert_eq!(store.write(input("Audit", event, 1)).await.unwrap(), 3);
        assert_eq!(store.get_by_id(2).await.unwrap(), None);
        assert_eq!(
            store.get_by_id(3).await.unwrap().unwrap().handler_name,
            "Audit"
        );
    }

    #[tokio::test]
    async fn reads_are_per_handler_in_id_order() {
        let store = InMemoryDeadLetterStore::new(LocalDatabase::new());
        let (a, b) = (new_v7(), new_v7());
        store.write(input("Balance", a, 1)).await.unwrap();
        store.write(input("Audit", a, 1)).await.unwrap();
        store.write(input("Balance", b, 1)).await.unwrap();
        store.write(input("Balance", a, 3)).await.unwrap();

        let ids = |entries: Vec<DeadLetterEntry>| -> Vec<u64> {
            entries.into_iter().map(|entry| entry.id).collect()
        };
        assert_eq!(ids(store.list("Balance", None).await.unwrap()), [1, 3, 4]);
        assert_eq!(ids(store.list("Balance", Some(2)).await.unwrap()), [1, 3]);
        let latest = store.get(a, "Balance").await.unwrap().unwrap();
        assert_eq!((latest.id, latest.attempts), (4, 3));
        assert_eq!(store.get(b, "Audit").await.unwrap(), None);
        assert_eq!(store.count(None).await.unwrap(), 4);
        assert_eq!(store.count(Some("Balance")).await.unwrap(), 3);
    }

    #[tokio::test]
    async fn delete_old_uses_the_write_time() {
        let clock = Arc::new(ManualClock::new(DateTime::UNIX_EPOCH));
        let store = InMemoryDeadLetterStore::new(LocalDatabase::with_clock(clock.clone()));
        store.write(input("Balance", new_v7(), 1)).await.unwrap();
        clock.advance(TimeDelta::days(31));
        store.write(input("Balance", new_v7(), 1)).await.unwrap();
        let entry = store.get_by_id(2).await.unwrap().unwrap();
        assert_eq!(entry.created_at, clock.now());
        let cut_off = clock.now() - TimeDelta::days(30);
        assert_eq!(store.delete_old(cut_off).await.unwrap(), 1);
        assert_eq!(store.count(None).await.unwrap(), 1);
    }
}
