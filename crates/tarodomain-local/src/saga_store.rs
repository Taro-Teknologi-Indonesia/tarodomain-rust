//! The in-memory saga store adapter (spec §9.1).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use tarodomain::adapters::{SagaQuery, SagaRecord, SagaSaveInput, SagaStatus, SagaStoreAdapter};
use tarodomain::async_trait;
use tarodomain::error::Result;
use tarodomain::kernel::ids::Uuid;

use crate::LocalDatabase;

/// The local provider's saga store, in a [`LocalDatabase`]. Timestamps come
/// from the database's clock.
#[derive(Clone, Debug)]
pub struct InMemorySagaStore {
    db: LocalDatabase,
}

impl InMemorySagaStore {
    /// A store over `db`'s saga table.
    pub fn new(db: LocalDatabase) -> Self {
        Self { db }
    }
}

/// The sagas by `(saga_type, saga_id)`.
#[derive(Clone, Default)]
struct SagaTable(BTreeMap<(String, Uuid), SagaRecord>);

impl SagaTable {
    /// The records `keep` picks, in the contract's list order (oldest
    /// `updated_at`, then type and ID), up to the query's limit.
    fn list(&self, query: &SagaQuery, keep: impl Fn(&SagaRecord) -> bool) -> Vec<SagaRecord> {
        let mut records: Vec<_> = self.0.values().filter(|record| keep(record)).collect();
        // The map is already in type and ID order, and the sort is stable.
        records.sort_by_key(|record| record.updated_at);
        let limited = records.into_iter().take(query.limit.unwrap_or(usize::MAX));
        limited.cloned().collect()
    }

    fn update(&mut self, saga_type: &str, saga_id: Uuid, f: impl FnOnce(&mut SagaRecord)) {
        if let Some(record) = self.0.get_mut(&(saga_type.to_owned(), saga_id)) {
            f(record);
        }
    }
}

#[async_trait]
impl SagaStoreAdapter for InMemorySagaStore {
    async fn save(&self, input: SagaSaveInput) -> Result<()> {
        let clock = self.db.clock();
        self.db
            .write(|table: &mut SagaTable| {
                let key = (input.saga_type.clone(), input.saga_id);
                let record = SagaRecord::saved(input, table.0.get(&key), clock.now());
                table.0.insert(key, record);
            })
            .await;
        Ok(())
    }

    async fn load(&self, saga_type: &str, saga_id: Uuid) -> Result<Option<SagaRecord>> {
        Ok(self
            .db
            .read(|table: &SagaTable| table.0.get(&(saga_type.to_owned(), saga_id)).cloned()))
    }

    async fn complete(&self, saga_type: &str, saga_id: Uuid) -> Result<()> {
        let clock = self.db.clock();
        self.db
            .write(|table: &mut SagaTable| {
                table.update(saga_type, saga_id, |record| {
                    record.complete(clock.now());
                });
            })
            .await;
        Ok(())
    }

    async fn fail(&self, saga_type: &str, saga_id: Uuid, reason: &str) -> Result<()> {
        let clock = self.db.clock();
        self.db
            .write(|table: &mut SagaTable| {
                table.update(saga_type, saga_id, |record| {
                    record.fail(reason, clock.now());
                });
            })
            .await;
        Ok(())
    }

    async fn load_by_status(
        &self,
        status: SagaStatus,
        query: SagaQuery,
    ) -> Result<Vec<SagaRecord>> {
        Ok(self.db.read(|table: &SagaTable| {
            table.list(&query, |record| {
                record.status == status && query.matches_type(&record.saga_type)
            })
        }))
    }

    async fn load_timed_out(
        &self,
        before: DateTime<Utc>,
        query: SagaQuery,
    ) -> Result<Vec<SagaRecord>> {
        Ok(self.db.read(|table: &SagaTable| {
            table.list(&query, |record| record.is_timed_out(before, &query))
        }))
    }

    async fn delete(&self, saga_type: &str, saga_id: Uuid) -> Result<()> {
        self.db
            .write(|table: &mut SagaTable| table.0.remove(&(saga_type.to_owned(), saga_id)))
            .await;
        Ok(())
    }

    async fn count(&self, saga_type: Option<&str>, status: Option<SagaStatus>) -> Result<u64> {
        Ok(self.db.read(|table: &SagaTable| {
            let records = table.0.values();
            let matching = records.filter(|record| {
                saga_type.is_none_or(|t| record.saga_type == t)
                    && status.is_none_or(|s| record.status == s)
            });
            matching.count() as u64
        }))
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

    fn input(saga_type: &str, saga_id: Uuid, status: SagaStatus) -> SagaSaveInput {
        SagaSaveInput {
            saga_id,
            saga_type: saga_type.into(),
            correlation_id: saga_id,
            state: json!({ "step": 1 }),
            status,
            current_step: "startsWhenTransferRequested".into(),
            failure_reason: None,
        }
    }

    fn store() -> (InMemorySagaStore, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(DateTime::UNIX_EPOCH));
        let db = LocalDatabase::with_clock(clock.clone());
        (InMemorySagaStore::new(db), clock)
    }

    fn ids(records: &[SagaRecord]) -> Vec<Uuid> {
        records.iter().map(|record| record.saga_id).collect()
    }

    #[tokio::test]
    async fn save_keeps_started_at_and_moves_updated_at() {
        let (store, clock) = store();
        let id = new_v7();
        store
            .save(input("Transfer", id, SagaStatus::Running))
            .await
            .unwrap();
        clock.advance(TimeDelta::seconds(5));
        store
            .save(input("Transfer", id, SagaStatus::Compensating))
            .await
            .unwrap();
        let record = store.load("Transfer", id).await.unwrap().unwrap();
        assert_eq!(record.status, SagaStatus::Compensating);
        assert_eq!(record.started_at, DateTime::UNIX_EPOCH);
        assert_eq!(record.updated_at, clock.now());
        assert_eq!(store.load("Other", id).await.unwrap(), None);
        let by_correlation = store.load_by_correlation_id("Transfer", id).await;
        assert_eq!(by_correlation.unwrap(), Some(record));
    }

    #[tokio::test]
    async fn complete_and_fail_end_a_saga_once() {
        let (store, clock) = store();
        let id = new_v7();
        store
            .save(input("Transfer", id, SagaStatus::Running))
            .await
            .unwrap();
        clock.advance(TimeDelta::seconds(1));
        store
            .fail("Transfer", id, "insufficient funds")
            .await
            .unwrap();
        clock.advance(TimeDelta::seconds(1));
        store.complete("Transfer", id).await.unwrap();
        store.fail("Transfer", new_v7(), "missing").await.unwrap();

        let record = store.load("Transfer", id).await.unwrap().unwrap();
        assert_eq!(record.status, SagaStatus::Failed);
        assert_eq!(record.failure_reason.as_deref(), Some("insufficient funds"));
        let ended = DateTime::UNIX_EPOCH + TimeDelta::seconds(1);
        assert_eq!(record.completed_at, Some(ended));
        assert_eq!(record.state, json!({ "step": 1 }));
    }

    #[tokio::test]
    async fn lists_are_oldest_first_filtered_then_limited() {
        let (store, clock) = store();
        let (a, b, c, d) = (new_v7(), new_v7(), new_v7(), new_v7());
        for (saga_type, id) in [("Transfer", a), ("Refund", b), ("Transfer", c)] {
            store
                .save(input(saga_type, id, SagaStatus::Running))
                .await
                .unwrap();
            clock.advance(TimeDelta::seconds(1));
        }
        store
            .save(input("Transfer", d, SagaStatus::Completed))
            .await
            .unwrap();
        // `a` is written again, so it is now the newest.
        store
            .save(input("Transfer", a, SagaStatus::Running))
            .await
            .unwrap();

        let running = store
            .load_by_status(SagaStatus::Running, SagaQuery::default())
            .await
            .unwrap();
        assert_eq!(ids(&running), [b, c, a]);
        let transfers = SagaQuery {
            saga_type: Some("Transfer".into()),
            limit: Some(1),
        };
        let first = store.load_by_status(SagaStatus::Running, transfers.clone());
        assert_eq!(ids(&first.await.unwrap()), [c]);

        let timed_out = store.load_timed_out(clock.now(), SagaQuery::default());
        assert_eq!(ids(&timed_out.await.unwrap()), [b, c]);
        let timed_out = store.load_timed_out(clock.now(), transfers);
        assert_eq!(ids(&timed_out.await.unwrap()), [c]);

        assert_eq!(store.count(None, None).await.unwrap(), 4);
        assert_eq!(store.count(Some("Transfer"), None).await.unwrap(), 3);
        let running_transfers = store.count(Some("Transfer"), Some(SagaStatus::Running));
        assert_eq!(running_transfers.await.unwrap(), 2);
        store.delete("Transfer", a).await.unwrap();
        store.delete("Transfer", a).await.unwrap();
        assert_eq!(store.count(None, None).await.unwrap(), 3);
    }
}
