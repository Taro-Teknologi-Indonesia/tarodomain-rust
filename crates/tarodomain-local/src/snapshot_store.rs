//! The in-memory snapshot store adapter (spec §8.1).

use std::collections::BTreeMap;

use tarodomain::adapters::{Snapshot, SnapshotInput, SnapshotStoreAdapter};
use tarodomain::async_trait;
use tarodomain::error::Result;
use tarodomain::kernel::ids::Uuid;

use crate::LocalDatabase;

/// The local provider's snapshot store, in a [`LocalDatabase`].
#[derive(Clone, Debug)]
pub struct InMemorySnapshotStore {
    db: LocalDatabase,
}

impl InMemorySnapshotStore {
    /// A store over `db`'s snapshot table.
    pub fn new(db: LocalDatabase) -> Self {
        Self { db }
    }
}

/// The snapshots by `(source_uuid, source_version)`, so one aggregate's are
/// adjacent and in version order.
#[derive(Clone, Default)]
struct SnapshotTable(BTreeMap<(Uuid, u64), Snapshot>);

impl SnapshotTable {
    /// The aggregate's snapshots under `source_type`, lowest version first.
    fn of<'a>(
        &'a self,
        source_uuid: Uuid,
        source_type: &'a str,
    ) -> impl DoubleEndedIterator<Item = &'a Snapshot> {
        self.0
            .range((source_uuid, 0)..=(source_uuid, u64::MAX))
            .map(|(_, snapshot)| snapshot)
            .filter(move |snapshot| snapshot.source_type == source_type)
    }

    /// Deletes the aggregate's snapshots under `source_type` that `doomed`
    /// picks, and returns how many.
    fn delete_where(
        &mut self,
        source_uuid: Uuid,
        source_type: &str,
        doomed: impl Fn(u64) -> bool,
    ) -> u64 {
        let before = self.0.len();
        self.0.retain(|&(uuid, version), snapshot| {
            uuid != source_uuid || snapshot.source_type != source_type || !doomed(version)
        });
        (before - self.0.len()) as u64
    }
}

#[async_trait]
impl SnapshotStoreAdapter for InMemorySnapshotStore {
    async fn save(&self, input: SnapshotInput) -> Result<()> {
        let clock = self.db.clock();
        self.db
            .write(|table: &mut SnapshotTable| {
                let key = (input.source_uuid, input.source_version);
                table.0.insert(key, Snapshot::new(input, clock.now()));
            })
            .await;
        Ok(())
    }

    async fn load_latest(&self, source_uuid: Uuid, source_type: &str) -> Result<Option<Snapshot>> {
        Ok(self
            .db
            .read(|table: &SnapshotTable| table.of(source_uuid, source_type).next_back().cloned()))
    }

    async fn load_at_version(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        version: u64,
    ) -> Result<Option<Snapshot>> {
        Ok(self.db.read(|table: &SnapshotTable| {
            let snapshot = table.0.get(&(source_uuid, version));
            snapshot
                .filter(|snapshot| snapshot.source_type == source_type)
                .cloned()
        }))
    }

    async fn list(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Snapshot>> {
        Ok(self.db.read(|table: &SnapshotTable| {
            let newest_first = table.of(source_uuid, source_type).rev();
            newest_first
                .take(limit.unwrap_or(usize::MAX))
                .cloned()
                .collect()
        }))
    }

    async fn delete_all(&self, source_uuid: Uuid, source_type: &str) -> Result<()> {
        self.db
            .write(|table: &mut SnapshotTable| {
                table.delete_where(source_uuid, source_type, |_| true)
            })
            .await;
        Ok(())
    }

    async fn delete_before(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        version: u64,
    ) -> Result<u64> {
        let deleted = self
            .db
            .write(|table: &mut SnapshotTable| {
                table.delete_where(source_uuid, source_type, |v| v < version)
            })
            .await;
        Ok(deleted)
    }

    async fn count(&self, source_uuid: Uuid, source_type: &str) -> Result<u64> {
        Ok(self
            .db
            .read(|table: &SnapshotTable| table.of(source_uuid, source_type).count() as u64))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::{DateTime, TimeDelta};
    use serde_json::json;
    use tarodomain::domain::Metadata;
    use tarodomain::kernel::clock::{Clock, ManualClock};
    use tarodomain::kernel::ids::new_v7;

    use super::*;

    fn input(source_uuid: Uuid, source_type: &str, version: u64) -> SnapshotInput {
        SnapshotInput {
            source_uuid,
            source_version: version,
            source_type: source_type.into(),
            data: json!({ "balance": version }),
            metadata: Metadata::new(),
        }
    }

    fn versions(snapshots: &[Snapshot]) -> Vec<u64> {
        snapshots.iter().map(|s| s.source_version).collect()
    }

    #[tokio::test]
    async fn save_is_an_upsert_that_restamps() {
        let clock = Arc::new(ManualClock::new(DateTime::UNIX_EPOCH));
        let store = InMemorySnapshotStore::new(LocalDatabase::with_clock(clock.clone()));
        let id = new_v7();
        store.save(input(id, "BankAccount", 5)).await.unwrap();
        clock.advance(TimeDelta::seconds(1));
        let mut again = input(id, "BankAccount", 5);
        again.data = json!({ "balance": 50 });
        store.save(again).await.unwrap();

        let latest = store.load_latest(id, "BankAccount").await.unwrap().unwrap();
        assert_eq!(latest.data, json!({ "balance": 50 }));
        assert_eq!(latest.created_at, clock.now());
        assert_eq!(store.count(id, "BankAccount").await.unwrap(), 1);
    }

    #[tokio::test]
    async fn reads_are_per_aggregate_and_type() {
        let store = InMemorySnapshotStore::new(LocalDatabase::new());
        let (id, other) = (new_v7(), new_v7());
        for version in [3, 10, 7] {
            store.save(input(id, "BankAccount", version)).await.unwrap();
        }
        store.save(input(other, "BankAccount", 20)).await.unwrap();

        let latest = store.load_latest(id, "BankAccount").await.unwrap();
        assert_eq!(latest.unwrap().source_version, 10);
        assert_eq!(store.load_latest(id, "Transfer").await.unwrap(), None);
        let at = store.load_at_version(id, "BankAccount", 7).await.unwrap();
        assert_eq!(at.unwrap().source_version, 7);
        assert_eq!(
            store.load_at_version(id, "Transfer", 7).await.unwrap(),
            None
        );
        let all = store.list(id, "BankAccount", None).await.unwrap();
        assert_eq!(versions(&all), [10, 7, 3]);
        let two = store.list(id, "BankAccount", Some(2)).await.unwrap();
        assert_eq!(versions(&two), [10, 7]);
    }

    #[tokio::test]
    async fn deletes_keep_other_aggregates_and_the_cut_off_version() {
        let store = InMemorySnapshotStore::new(LocalDatabase::new());
        let (id, other) = (new_v7(), new_v7());
        for version in [3, 7, 10] {
            store.save(input(id, "BankAccount", version)).await.unwrap();
        }
        store.save(input(other, "BankAccount", 1)).await.unwrap();

        let deleted = store.delete_before(id, "BankAccount", 7).await.unwrap();
        assert_eq!(deleted, 1);
        let left = store.list(id, "BankAccount", None).await.unwrap();
        assert_eq!(versions(&left), [10, 7]);
        assert_eq!(store.delete_before(id, "Transfer", 100).await.unwrap(), 0);

        store.delete_all(id, "BankAccount").await.unwrap();
        assert_eq!(store.count(id, "BankAccount").await.unwrap(), 0);
        assert_eq!(store.count(other, "BankAccount").await.unwrap(), 1);
    }
}
