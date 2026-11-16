//! The in-memory event store adapter (spec §3.2).

use std::collections::{BTreeMap, HashMap, HashSet};

use chrono::{DateTime, Utc};
use tarodomain::adapters::{
    AppendResult, EventStoreAdapter, StreamIds, StreamKind, StreamMetadata, Subscription,
};
use tarodomain::async_trait;
use tarodomain::domain::SerializedEvent;
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::ids::Uuid;

use crate::LocalDatabase;

/// The local provider's event store: an append-only log in a
/// [`LocalDatabase`], with the streams over it and the subscription cursors.
///
/// Every event gets a position from a counter that only goes up, and reads
/// across streams follow it, so they come back in append order. Stream and
/// subscription timestamps come from the database's clock.
#[derive(Clone, Debug)]
pub struct InMemoryEventStoreAdapter {
    db: LocalDatabase,
}

impl InMemoryEventStoreAdapter {
    /// An adapter over `db`'s event table.
    pub fn new(db: LocalDatabase) -> Self {
        Self { db }
    }
}

/// The event log, the aggregate streams over it, and the subscriptions.
#[derive(Clone, Default)]
struct EventTable {
    /// Every stored event, by append position.
    rows: BTreeMap<u64, Row>,
    /// Each stored event's position, by event ID.
    positions: HashMap<Uuid, u64>,
    /// Each aggregate stream with events, by stream ID.
    streams: HashMap<String, Stream>,
    /// The subscriptions, by name.
    subscriptions: BTreeMap<String, Subscription>,
    /// The last position given out. Positions are never reused, even after
    /// their stream is deleted.
    last_position: u64,
}

#[derive(Clone)]
struct Row {
    stream_id: String,
    category: String,
    event: SerializedEvent,
}

#[derive(Clone)]
struct Stream {
    /// The positions of the stream's events, in version order: version `n`
    /// is at index `n - 1`, so the length is the stream's version.
    positions: Vec<u64>,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

impl EventTable {
    fn version(&self, stream_id: &str) -> u64 {
        self.streams
            .get(stream_id)
            .map_or(0, |stream| stream.positions.len() as u64)
    }

    fn append(
        &mut self,
        stream_id: &str,
        category: &str,
        events: Vec<SerializedEvent>,
        expected_version: u64,
        now: DateTime<Utc>,
    ) -> Result<AppendResult> {
        let actual = self.version(stream_id);
        if actual != expected_version {
            return Err(FrameworkError::optimistic_concurrency(
                stream_id,
                expected_version,
                actual,
            ));
        }
        let mut seen = HashSet::new();
        for event in &events {
            if self.positions.contains_key(&event.event_id) || !seen.insert(event.event_id) {
                let message = format!("event {} is already stored", event.event_id);
                return Err(FrameworkError::event_append_failed(message)
                    .with_context("streamId", stream_id)
                    .with_context("eventId", event.event_id.to_string()));
            }
        }
        if events.is_empty() {
            return Ok(AppendResult {
                version: expected_version,
            });
        }

        let stream = self
            .streams
            .entry(stream_id.to_owned())
            .or_insert_with(|| Stream {
                positions: Vec::new(),
                created_at: now,
                updated_at: now,
            });
        stream.updated_at = now;
        for event in events {
            self.last_position += 1;
            let position = self.last_position;
            stream.positions.push(position);
            self.positions.insert(event.event_id, position);
            let row = Row {
                stream_id: stream_id.to_owned(),
                category: category.to_owned(),
                event,
            };
            self.rows.insert(position, row);
        }
        Ok(AppendResult {
            version: stream.positions.len() as u64,
        })
    }

    fn load_stream(
        &self,
        stream_id: &str,
        after_version: u64,
        max_count: Option<usize>,
    ) -> Vec<SerializedEvent> {
        let Some(stream) = self.streams.get(stream_id) else {
            return Vec::new();
        };
        let skip = usize::try_from(after_version).unwrap_or(usize::MAX);
        stream
            .positions
            .iter()
            .skip(skip)
            .take(max_count.unwrap_or(usize::MAX))
            .map(|position| self.rows[position].event.clone())
            .collect()
    }

    /// Every row after the cursor, in append order: after its position if the
    /// event is stored, else (its stream was deleted) after its ID.
    fn rows_after(&self, after_event_id: Option<Uuid>) -> impl Iterator<Item = &Row> {
        let (start, id_floor) = match after_event_id {
            None => (0, None),
            Some(id) => match self.positions.get(&id) {
                Some(&position) => (position + 1, None),
                None => (0, Some(id)),
            },
        };
        self.rows
            .range(start..)
            .map(|(_, row)| row)
            .filter(move |row| id_floor.is_none_or(|id| row.event.event_id > id))
    }

    fn load_after(
        &self,
        stream_id: &str,
        after_event_id: Option<Uuid>,
        max_count: Option<usize>,
    ) -> Vec<SerializedEvent> {
        let kind = StreamIds::parse(stream_id);
        self.rows_after(after_event_id)
            .filter(|row| match kind {
                StreamKind::All => true,
                StreamKind::Category(category) => row.category == category,
                StreamKind::Aggregate { .. } => row.stream_id == stream_id,
            })
            .take(max_count.unwrap_or(usize::MAX))
            .map(|row| row.event.clone())
            .collect()
    }

    fn metadata(&self, stream_id: &str) -> Option<StreamMetadata> {
        let stream = self.streams.get(stream_id)?;
        Some(StreamMetadata {
            stream_id: stream_id.to_owned(),
            version: stream.positions.len() as u64,
            created_at: stream.created_at,
            updated_at: stream.updated_at,
        })
    }

    fn delete_stream(&mut self, stream_id: &str) {
        let Some(stream) = self.streams.remove(stream_id) else {
            return;
        };
        for position in stream.positions {
            if let Some(row) = self.rows.remove(&position) {
                self.positions.remove(&row.event.event_id);
            }
        }
    }

    fn subscribe(
        &mut self,
        name: &str,
        stream_id: Option<&str>,
        now: DateTime<Utc>,
    ) -> Subscription {
        let stream_id = stream_id.filter(|&stream_id| stream_id != StreamIds::ALL);
        let subscription = self
            .subscriptions
            .entry(name.to_owned())
            .or_insert_with(|| Subscription {
                name: name.to_owned(),
                stream_id: stream_id.map(str::to_owned),
                last_seen: None,
                created_at: now,
                failed_event_id: None,
                failed_attempts: 0,
            });
        subscription.clone()
    }

    fn ack(&mut self, name: &str, event_id: Uuid) {
        let positions = &self.positions;
        let Some(subscription) = self.subscriptions.get_mut(name) else {
            return;
        };
        let moves = subscription
            .last_seen
            .is_none_or(|seen| is_after(positions, event_id, seen));
        if moves {
            subscription.last_seen = Some(event_id);
            subscription.failed_event_id = None;
            subscription.failed_attempts = 0;
        }
    }

    fn record_failed_attempt(&mut self, name: &str, event_id: Uuid) -> u32 {
        let Some(subscription) = self.subscriptions.get_mut(name) else {
            return 0;
        };
        if subscription.failed_event_id == Some(event_id) {
            subscription.failed_attempts = subscription.failed_attempts.saturating_add(1);
        } else {
            subscription.failed_event_id = Some(event_id);
            subscription.failed_attempts = 1;
        }
        subscription.failed_attempts
    }
}

/// Whether event `a` comes after event `b` in append order, comparing IDs
/// when either is no longer stored.
fn is_after(positions: &HashMap<Uuid, u64>, a: Uuid, b: Uuid) -> bool {
    match (positions.get(&a), positions.get(&b)) {
        (Some(a), Some(b)) => a > b,
        _ => a > b,
    }
}

#[async_trait]
impl EventStoreAdapter for InMemoryEventStoreAdapter {
    async fn append_to_stream(
        &self,
        stream_id: &str,
        events: Vec<SerializedEvent>,
        expected_version: u64,
    ) -> Result<AppendResult> {
        let StreamKind::Aggregate { category, .. } = StreamIds::parse(stream_id) else {
            let message = format!("events are appended to aggregate streams only: {stream_id:?}");
            return Err(
                FrameworkError::event_append_failed(message).with_context("streamId", stream_id)
            );
        };
        let clock = self.db.clock();
        self.db
            .write(|table: &mut EventTable| {
                table.append(stream_id, category, events, expected_version, clock.now())
            })
            .await
    }

    async fn load_stream(
        &self,
        stream_id: &str,
        after_version: u64,
        max_count: Option<usize>,
    ) -> Result<Vec<SerializedEvent>> {
        Ok(self
            .db
            .read(|table: &EventTable| table.load_stream(stream_id, after_version, max_count)))
    }

    async fn load_stream_after_event(
        &self,
        stream_id: &str,
        after_event_id: Option<Uuid>,
        max_count: Option<usize>,
    ) -> Result<Vec<SerializedEvent>> {
        Ok(self
            .db
            .read(|table: &EventTable| table.load_after(stream_id, after_event_id, max_count)))
    }

    async fn load_by_correlation_id(&self, correlation_id: Uuid) -> Result<Vec<SerializedEvent>> {
        Ok(self.db.read(|table: &EventTable| {
            let rows = table.rows.values();
            let events = rows.map(|row| &row.event);
            let correlated = events.filter(|event| event.correlation_id == correlation_id);
            correlated.cloned().collect()
        }))
    }

    async fn get_stream_version(&self, stream_id: &str) -> Result<u64> {
        Ok(self.db.read(|table: &EventTable| table.version(stream_id)))
    }

    async fn get_stream_metadata(&self, stream_id: &str) -> Result<Option<StreamMetadata>> {
        Ok(self.db.read(|table: &EventTable| table.metadata(stream_id)))
    }

    async fn delete_stream(&self, stream_id: &str) -> Result<()> {
        self.db
            .write(|table: &mut EventTable| table.delete_stream(stream_id))
            .await;
        Ok(())
    }

    async fn subscribe(&self, name: &str, stream_id: Option<&str>) -> Result<Subscription> {
        let clock = self.db.clock();
        let subscription = self
            .db
            .write(|table: &mut EventTable| table.subscribe(name, stream_id, clock.now()))
            .await;
        Ok(subscription)
    }

    async fn get_subscription(&self, name: &str) -> Result<Option<Subscription>> {
        Ok(self
            .db
            .read(|table: &EventTable| table.subscriptions.get(name).cloned()))
    }

    async fn ack_subscription(&self, name: &str, last_seen_event_id: Uuid) -> Result<()> {
        self.db
            .write(|table: &mut EventTable| table.ack(name, last_seen_event_id))
            .await;
        Ok(())
    }

    async fn delete_subscription(&self, name: &str) -> Result<()> {
        self.db
            .write(|table: &mut EventTable| table.subscriptions.remove(name))
            .await;
        Ok(())
    }

    async fn list_subscriptions(&self) -> Result<Vec<Subscription>> {
        Ok(self
            .db
            .read(|table: &EventTable| table.subscriptions.values().cloned().collect()))
    }

    async fn record_failed_attempt(&self, name: &str, event_id: Uuid) -> Result<u32> {
        let attempts = self
            .db
            .write(|table: &mut EventTable| table.record_failed_attempt(name, event_id))
            .await;
        Ok(attempts)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono::TimeDelta;
    use serde_json::json;
    use tarodomain::domain::Metadata;
    use tarodomain::error::ErrorCode;
    use tarodomain::kernel::clock::{Clock, ManualClock};
    use tarodomain::kernel::ids::derived_v7;

    use super::*;

    const ACCOUNT: &str = "BankAccount-01909e3c-5a4b-7c3d-8e2f-123456789abc";
    const OTHER: &str = "BankAccount-01909e3c-5a4b-7c3d-8e2f-cba987654321";
    const TRANSFER: &str = "Transfer-01909e3c-5a4b-7c3d-8e2f-000000000001";
    const MS: u64 = 1_763_272_710_206;

    fn id(index: u32) -> Uuid {
        derived_v7("event-store", index, MS)
    }

    fn event(event_id: Uuid) -> SerializedEvent {
        SerializedEvent {
            event_id,
            event_type: "MoneyDeposited".into(),
            event_version: "1.0.0".into(),
            causation_id: None,
            correlation_id: event_id,
            command_causation_id: None,
            data: json!({ "amount": 10 }),
            metadata: Metadata::new(),
            occurred_at: DateTime::UNIX_EPOCH,
        }
    }

    fn ids(events: &[SerializedEvent]) -> Vec<Uuid> {
        events.iter().map(|event| event.event_id).collect()
    }

    fn adapter() -> (InMemoryEventStoreAdapter, Arc<ManualClock>) {
        let clock = Arc::new(ManualClock::new(DateTime::UNIX_EPOCH));
        let db = LocalDatabase::with_clock(clock.clone());
        (InMemoryEventStoreAdapter::new(db), clock)
    }

    #[tokio::test]
    async fn versions_count_from_one_and_a_stale_append_conflicts() {
        let (store, _) = adapter();
        assert_eq!(store.get_stream_version(ACCOUNT).await.unwrap(), 0);
        let events = vec![event(id(1)), event(id(2))];
        let result = store.append_to_stream(ACCOUNT, events, 0).await.unwrap();
        assert_eq!(result.version, 2);

        let error = store
            .append_to_stream(ACCOUNT, vec![event(id(3))], 1)
            .await
            .unwrap_err();
        assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
        assert_eq!(error.context()["streamId"], ACCOUNT);
        assert_eq!(error.context()["expectedVersion"], 1);
        assert_eq!(error.context()["actualVersion"], 2);
        assert_eq!(store.get_stream_version(ACCOUNT).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn an_empty_append_writes_nothing_but_checks_the_version() {
        let (store, _) = adapter();
        let result = store.append_to_stream(ACCOUNT, Vec::new(), 0).await;
        assert_eq!(result.unwrap().version, 0);
        assert!(!store.stream_exists(ACCOUNT).await.unwrap());
        let error = store.append_to_stream(ACCOUNT, Vec::new(), 3).await;
        assert_eq!(*error.unwrap_err().code(), ErrorCode::OptimisticConcurrency);
    }

    #[tokio::test]
    async fn only_aggregate_streams_take_appends() {
        let (store, _) = adapter();
        for stream_id in ["$all", "BankAccount"] {
            let error = store
                .append_to_stream(stream_id, vec![event(id(1))], 0)
                .await
                .unwrap_err();
            assert_eq!(*error.code(), ErrorCode::EventAppendFailed, "{stream_id}");
        }
        assert!(store.load_all(None, None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_stored_event_id_fails_the_whole_append() {
        let (store, _) = adapter();
        store
            .append_to_stream(ACCOUNT, vec![event(id(1))], 0)
            .await
            .unwrap();
        for events in [
            vec![event(id(2)), event(id(1))],
            vec![event(id(2)), event(id(2))],
        ] {
            let error = store.append_to_stream(OTHER, events, 0).await.unwrap_err();
            assert_eq!(*error.code(), ErrorCode::EventAppendFailed);
        }
        assert_eq!(store.get_stream_version(OTHER).await.unwrap(), 0);
        assert_eq!(ids(&store.load_all(None, None).await.unwrap()), [id(1)]);
    }

    #[tokio::test]
    async fn load_stream_reads_after_a_version_up_to_a_count() {
        let (store, _) = adapter();
        let events = (1..=4).map(|n| event(id(n))).collect();
        store.append_to_stream(ACCOUNT, events, 0).await.unwrap();
        let read = |after, max| store.load_stream(ACCOUNT, after, max);
        assert_eq!(
            ids(&read(0, None).await.unwrap()),
            [id(1), id(2), id(3), id(4)]
        );
        assert_eq!(ids(&read(2, None).await.unwrap()), [id(3), id(4)]);
        assert_eq!(ids(&read(1, Some(2)).await.unwrap()), [id(2), id(3)]);
        assert!(read(4, None).await.unwrap().is_empty());
        assert!(store.load_stream(OTHER, 0, None).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn cross_stream_reads_follow_append_order_not_ids() {
        let (store, _) = adapter();
        // Same millisecond: id(9) and id(1) sort either way, append order wins.
        let (first, second) = if id(9) > id(1) {
            (id(9), id(1))
        } else {
            (id(1), id(9))
        };
        store
            .append_to_stream(ACCOUNT, vec![event(first)], 0)
            .await
            .unwrap();
        store
            .append_to_stream(TRANSFER, vec![event(second)], 0)
            .await
            .unwrap();
        store
            .append_to_stream(OTHER, vec![event(id(3))], 0)
            .await
            .unwrap();

        let all = store.load_all(None, None).await.unwrap();
        assert_eq!(ids(&all), [first, second, id(3)]);
        let after_first = store.load_all(Some(first), None).await.unwrap();
        assert_eq!(ids(&after_first), [second, id(3)]);
        let category = store.load_stream_after_event("BankAccount", None, None);
        assert_eq!(ids(&category.await.unwrap()), [first, id(3)]);
        let one = store.load_stream_after_event(OTHER, None, None);
        assert_eq!(ids(&one.await.unwrap()), [id(3)]);
        let limited = store.load_stream_after_event("$all", Some(first), Some(1));
        assert_eq!(ids(&limited.await.unwrap()), [second]);
    }

    #[tokio::test]
    async fn a_cursor_whose_event_was_deleted_falls_back_to_ids() {
        let (store, _) = adapter();
        let low = |n| derived_v7("low", n, MS - 1);
        let high = |n| derived_v7("high", n, MS + 1);
        store
            .append_to_stream(TRANSFER, vec![event(id(1))], 0)
            .await
            .unwrap();
        store
            .append_to_stream(ACCOUNT, vec![event(low(1)), event(high(1))], 0)
            .await
            .unwrap();
        store.delete_stream(TRANSFER).await.unwrap();
        let after = store.load_all(Some(id(1)), None).await.unwrap();
        assert_eq!(ids(&after), [high(1)]);
    }

    #[tokio::test]
    async fn delete_stream_removes_it_everywhere_and_positions_move_on() {
        let (store, _) = adapter();
        store
            .append_to_stream(ACCOUNT, vec![event(id(1))], 0)
            .await
            .unwrap();
        store.delete_stream(ACCOUNT).await.unwrap();
        store.delete_stream(ACCOUNT).await.unwrap();
        assert_eq!(store.get_stream_version(ACCOUNT).await.unwrap(), 0);
        assert!(store.load_all(None, None).await.unwrap().is_empty());
        assert!(
            store
                .load_by_correlation_id(id(1))
                .await
                .unwrap()
                .is_empty()
        );

        store
            .append_to_stream(ACCOUNT, vec![event(id(1))], 0)
            .await
            .unwrap();
        let positions = store.db.read(|table: &EventTable| table.positions.clone());
        assert_eq!(positions[&id(1)], 2);
    }

    #[tokio::test]
    async fn correlated_events_come_back_in_append_order() {
        let (store, _) = adapter();
        let correlation = id(1);
        let mut later = event(id(2));
        later.correlation_id = correlation;
        store
            .append_to_stream(ACCOUNT, vec![event(id(1)), event(id(3))], 0)
            .await
            .unwrap();
        store.append_to_stream(OTHER, vec![later], 0).await.unwrap();
        let events = store.load_by_correlation_id(correlation).await.unwrap();
        assert_eq!(ids(&events), [id(1), id(2)]);
    }

    #[tokio::test]
    async fn stream_metadata_is_stamped_by_the_database_clock() {
        let (store, clock) = adapter();
        assert_eq!(store.get_stream_metadata(ACCOUNT).await.unwrap(), None);
        store
            .append_to_stream(ACCOUNT, vec![event(id(1))], 0)
            .await
            .unwrap();
        clock.advance(TimeDelta::seconds(5));
        store
            .append_to_stream(ACCOUNT, vec![event(id(2))], 1)
            .await
            .unwrap();
        let metadata = store.get_stream_metadata(ACCOUNT).await.unwrap().unwrap();
        assert_eq!(metadata.version, 2);
        assert_eq!(metadata.created_at, DateTime::UNIX_EPOCH);
        assert_eq!(metadata.updated_at, clock.now());
    }

    #[tokio::test]
    async fn subscriptions_are_idempotent_and_listed_by_name() {
        let (store, _) = adapter();
        let all = store.subscribe("b", Some("$all")).await.unwrap();
        assert_eq!(all.stream_id, None);
        let category = store.subscribe("a", Some("BankAccount")).await.unwrap();
        assert_eq!(category.stream(), "BankAccount");
        let again = store.subscribe("a", None).await.unwrap();
        assert_eq!(again, category);

        let names: Vec<_> = store.list_subscriptions().await.unwrap();
        let names: Vec<_> = names.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, ["a", "b"]);

        store.delete_subscription("a").await.unwrap();
        store.delete_subscription("a").await.unwrap();
        assert_eq!(store.get_subscription("a").await.unwrap(), None);
    }

    #[tokio::test]
    async fn acks_only_move_forward_and_clear_failures() {
        let (store, _) = adapter();
        let (first, second) = if id(9) > id(1) {
            (id(9), id(1))
        } else {
            (id(1), id(9))
        };
        let events = vec![event(first), event(second)];
        store.append_to_stream(ACCOUNT, events, 0).await.unwrap();
        store.subscribe("balances", None).await.unwrap();

        assert_eq!(
            store
                .record_failed_attempt("balances", first)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .record_failed_attempt("balances", first)
                .await
                .unwrap(),
            2
        );
        store.ack_subscription("balances", second).await.unwrap();
        let subscription = store.get_subscription("balances").await.unwrap().unwrap();
        assert_eq!(subscription.last_seen, Some(second));
        assert_eq!(subscription.failed_event_id, None);
        assert_eq!(subscription.failed_attempts, 0);

        // `first` has the higher ID in reverse order, but an earlier position.
        store.ack_subscription("balances", first).await.unwrap();
        let subscription = store.get_subscription("balances").await.unwrap().unwrap();
        assert_eq!(subscription.last_seen, Some(second));
    }

    #[tokio::test]
    async fn failed_attempts_count_per_event() {
        let (store, _) = adapter();
        store.subscribe("balances", None).await.unwrap();
        assert_eq!(
            store
                .record_failed_attempt("balances", id(1))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store
                .record_failed_attempt("balances", id(1))
                .await
                .unwrap(),
            2
        );
        assert_eq!(
            store
                .record_failed_attempt("balances", id(2))
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            store.record_failed_attempt("missing", id(1)).await.unwrap(),
            0
        );
        store.ack_subscription("missing", id(1)).await.unwrap();
        assert_eq!(store.list_subscriptions().await.unwrap().len(), 1);
    }
}
