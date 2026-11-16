//! The event store adapter contract (spec §3.1–3.2, §7, §18.1).

use chrono::{DateTime, Utc};

use crate::async_trait;
use crate::domain::{AggregateId, SerializedEvent};
use crate::error::Result;
use crate::kernel::ids::{Uuid, parse_uuid};

/// The stream ID conventions of spec §3.1.
///
/// - An aggregate's own stream is `"<type>-<id>"`, such as
///   `"BankAccount-01909e3c-…"`, from [`for_aggregate`](Self::for_aggregate).
///   Events are only ever appended to these.
/// - A category, such as `"BankAccount"`, is every aggregate stream of that
///   type.
/// - [`ALL`](Self::ALL), `"$all"`, is every event.
///
/// Categories and `$all` are read-only views over the aggregate streams.
#[derive(Debug)]
pub struct StreamIds {
    _private: (),
}

impl StreamIds {
    /// The global stream: every event, in append order.
    pub const ALL: &'static str = "$all";

    /// The stream ID of one aggregate: its type, a hyphen, and its ID in
    /// lower case.
    pub fn for_aggregate(aggregate_type: &str, id: impl Into<AggregateId>) -> String {
        format!("{aggregate_type}-{}", id.into())
    }

    /// The category of an aggregate stream ID, such as `"BankAccount"` for
    /// `"BankAccount-01909e3c-…"`. `None` for `$all` and for category IDs.
    pub fn category_of(stream_id: &str) -> Option<&str> {
        match Self::parse(stream_id) {
            StreamKind::Aggregate { category, .. } => Some(category),
            StreamKind::All | StreamKind::Category(_) => None,
        }
    }

    /// Which kind of stream an ID names.
    ///
    /// An ID is an aggregate stream when it ends in a hyphen and a UUID in
    /// the lower-case hyphenated form [`for_aggregate`](Self::for_aggregate)
    /// writes, after a non-empty type. The type may itself contain hyphens.
    /// Any other ID except `$all` is a category.
    pub fn parse(stream_id: &str) -> StreamKind<'_> {
        if stream_id == Self::ALL {
            return StreamKind::All;
        }
        Self::parse_aggregate(stream_id).unwrap_or(StreamKind::Category(stream_id))
    }

    fn parse_aggregate(stream_id: &str) -> Option<StreamKind<'_>> {
        const UUID_LEN: usize = 36;
        let split = stream_id.len().checked_sub(UUID_LEN + 1)?;
        let (category, rest) = (stream_id.get(..split)?, stream_id.get(split..)?);
        let text = rest.strip_prefix('-')?;
        if category.is_empty() || text.bytes().any(|b| b.is_ascii_uppercase()) {
            return None;
        }
        let id = parse_uuid(text)?;
        Some(StreamKind::Aggregate { category, id })
    }
}

/// What a stream ID names, from [`StreamIds::parse`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StreamKind<'a> {
    /// `$all`: every event.
    All,
    /// Every stream of one aggregate type, such as `"BankAccount"`.
    Category(&'a str),
    /// One aggregate's stream.
    Aggregate {
        /// The aggregate type, which is the stream's category.
        category: &'a str,
        /// The aggregate's ID.
        id: Uuid,
    },
}

/// The outcome of [`EventStoreAdapter::append_to_stream`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AppendResult {
    /// The stream's version after the append: the expected version plus the
    /// number of events appended. It is the next append's expected version.
    pub version: u64,
}

/// What the store knows about one aggregate stream, from
/// [`EventStoreAdapter::get_stream_metadata`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamMetadata {
    /// The stream ID.
    pub stream_id: String,
    /// The stream's version: the number of events in it.
    pub version: u64,
    /// When the stream's first event was appended, by the store's clock.
    pub created_at: DateTime<Utc>,
    /// When the stream's last event was appended, by the store's clock.
    pub updated_at: DateTime<Utc>,
}

/// A persistent subscription cursor (spec §18.1).
///
/// The catch-up dispatcher reads the subscription's stream after
/// `last_seen`, hands each event to the subscribed handler, and acks it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Subscription {
    /// The subscription's name, its key. By convention the handler's name.
    pub name: String,
    /// The stream it reads: an aggregate stream, a category, or `None` for
    /// `$all`.
    pub stream_id: Option<String>,
    /// The last event acked, or `None` if the subscription starts from the
    /// beginning.
    pub last_seen: Option<Uuid>,
    /// When the subscription was created, by the store's clock.
    pub created_at: DateTime<Utc>,
    /// The event that has failed since the last ack, if any.
    pub failed_event_id: Option<Uuid>,
    /// How many times `failed_event_id` has failed. 0 when nothing has.
    pub failed_attempts: u32,
}

impl Subscription {
    /// The stream ID to read: [`stream_id`](Self::stream_id), or
    /// [`StreamIds::ALL`] when that is `None`.
    pub fn stream(&self) -> &str {
        self.stream_id.as_deref().unwrap_or(StreamIds::ALL)
    }
}

/// The event store's storage (spec §3.2): an append-only log of
/// [`SerializedEvent`]s in streams, plus subscription cursors.
///
/// The framework's `EventStore` wraps an adapter and converts between typed
/// and serialized events, so an adapter never sees a
/// [`DomainEvent`](crate::domain::DomainEvent). The semantics below are the
/// contract, and the `tarodomain-testing` suites check them on every
/// provider.
///
/// # Versions
///
/// A stream's version is the number of events in it. The first event of a
/// stream is version 1, and a stream that has no events (or was deleted) is
/// at version 0. Appends name the version they expect the stream to be at,
/// so two writers that loaded the same version can't both append.
///
/// # Append order
///
/// Reads across streams ([`load_stream_after_event`] on a category or `$all`,
/// [`load_all`], [`load_by_correlation_id`]) return events in global
/// **append order**: the order the store gave events their positions in,
/// which is the order their appends committed. That is not event-ID order:
/// two UUIDv7s from the same millisecond, or from servers whose clocks
/// disagree, can sort either way. Within one stream, append order is version
/// order.
///
/// Cursors are event IDs. An adapter resolves a cursor to its event's
/// position and continues from there. It compares IDs only when the store no
/// longer has that event, because its stream was deleted.
///
/// # Units of work
///
/// When the provider's unit of work (§11.4) is active in the current task,
/// writes join it, and reads see the unit's own staged writes.
///
/// # Missing rows
///
/// Deleting a stream or a subscription that doesn't exist does nothing. So
/// do acking a missing subscription and recording a failure for one, which
/// returns 0.
///
/// [`load_stream_after_event`]: Self::load_stream_after_event
/// [`load_all`]: Self::load_all
/// [`load_by_correlation_id`]: Self::load_by_correlation_id
#[async_trait]
pub trait EventStoreAdapter: Send + Sync {
    /// Appends events to an aggregate stream, if the stream is at
    /// `expected_version` (spec §7.2–7.3).
    ///
    /// The events get versions `expected_version + 1`, `+ 2`, … in the order
    /// given, and positions after every event already stored. The append is
    /// atomic: every event is stored, or none is.
    ///
    /// Fails with `OPTIMISTIC_CONCURRENCY` when the stream is at another
    /// version, with the stream ID, the expected version and the actual one
    /// as context ([`FrameworkError::optimistic_concurrency`]). Appending no
    /// events writes nothing, but still checks the version. Fails with
    /// `EVENT_APPEND_FAILED` when `stream_id` isn't an aggregate stream
    /// ([`StreamIds::parse`]) or an event ID is already stored, and nothing is
    /// written.
    ///
    /// [`FrameworkError::optimistic_concurrency`]: crate::error::FrameworkError::optimistic_concurrency
    async fn append_to_stream(
        &self,
        stream_id: &str,
        events: Vec<SerializedEvent>,
        expected_version: u64,
    ) -> Result<AppendResult>;

    /// The events of an aggregate stream with versions after
    /// `after_version`, in version order, at most `max_count` of them.
    ///
    /// This is the spec's `loadStream(streamId, fromVersion?, maxCount?)`,
    /// whose D1 query reads `stream_version > fromVersion` (§24.3), named for
    /// what it does: `after_version` 0 reads the whole stream, and a
    /// repository that restored a snapshot at version 5 passes 5. A missing
    /// stream has no events.
    async fn load_stream(
        &self,
        stream_id: &str,
        after_version: u64,
        max_count: Option<usize>,
    ) -> Result<Vec<SerializedEvent>>;

    /// The events of any stream (an aggregate stream, a category or `$all`)
    /// after the event `after_event_id`, in append order, at most `max_count`
    /// of them. `None` reads from the beginning.
    ///
    /// This is how a subscription reads (§18.3).
    async fn load_stream_after_event(
        &self,
        stream_id: &str,
        after_event_id: Option<Uuid>,
        max_count: Option<usize>,
    ) -> Result<Vec<SerializedEvent>>;

    /// Every event after `after_event_id`, in append order, at most
    /// `max_count` of them. `None` reads from the beginning.
    ///
    /// Defaults to [`load_stream_after_event`](Self::load_stream_after_event)
    /// on [`StreamIds::ALL`].
    async fn load_all(
        &self,
        after_event_id: Option<Uuid>,
        max_count: Option<usize>,
    ) -> Result<Vec<SerializedEvent>> {
        self.load_stream_after_event(StreamIds::ALL, after_event_id, max_count)
            .await
    }

    /// Every event with this correlation ID, in append order (§28.3).
    async fn load_by_correlation_id(&self, correlation_id: Uuid) -> Result<Vec<SerializedEvent>>;

    /// Whether the aggregate stream has any events.
    ///
    /// Defaults to [`get_stream_version`](Self::get_stream_version) being
    /// above 0.
    async fn stream_exists(&self, stream_id: &str) -> Result<bool> {
        Ok(self.get_stream_version(stream_id).await? > 0)
    }

    /// The aggregate stream's version, 0 if it has no events.
    ///
    /// Defaults to the version in
    /// [`get_stream_metadata`](Self::get_stream_metadata).
    async fn get_stream_version(&self, stream_id: &str) -> Result<u64> {
        let metadata = self.get_stream_metadata(stream_id).await?;
        Ok(metadata.map_or(0, |metadata| metadata.version))
    }

    /// What the store knows about the aggregate stream, or `None` if it has
    /// no events.
    async fn get_stream_metadata(&self, stream_id: &str) -> Result<Option<StreamMetadata>>;

    /// Deletes the aggregate stream's events and metadata. Its events
    /// disappear from category and `$all` reads too, and the stream is back
    /// at version 0. Positions are never reused.
    async fn delete_stream(&self, stream_id: &str) -> Result<()>;

    /// Creates a subscription to `stream_id` (`None` or `Some("$all")` for
    /// every event), starting from the beginning, and returns it.
    ///
    /// Idempotent: if a subscription with this name exists, it is returned
    /// unchanged, even if it reads another stream.
    async fn subscribe(&self, name: &str, stream_id: Option<&str>) -> Result<Subscription>;

    /// The subscription with this name, if there is one.
    async fn get_subscription(&self, name: &str) -> Result<Option<Subscription>>;

    /// Records that the subscription has handled every event up to and
    /// including `last_seen_event_id`.
    ///
    /// The cursor only moves forward, in append order: acking an event at or
    /// before `last_seen` changes nothing. Moving it forward also clears
    /// `failed_event_id` and `failed_attempts`.
    async fn ack_subscription(&self, name: &str, last_seen_event_id: Uuid) -> Result<()>;

    /// Deletes the subscription. A later [`subscribe`](Self::subscribe)
    /// starts from the beginning again.
    async fn delete_subscription(&self, name: &str) -> Result<()>;

    /// Every subscription, ordered by name.
    async fn list_subscriptions(&self) -> Result<Vec<Subscription>>;

    /// Records that handling `event_id` failed once more, and returns how
    /// many times it has failed since the last ack.
    ///
    /// The count is per event: a failure of another event than
    /// `failed_event_id` starts again at 1. The catch-up dispatcher uses it
    /// to dead-letter an event after a maximum number of attempts.
    async fn record_failed_attempt(&self, name: &str, event_id: Uuid) -> Result<u32>;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;

    use super::*;
    use crate::domain::{EventEnvelope, Metadata};
    use crate::kernel::ids::new_v7;

    const ID: &str = "01909e3c-5a4b-7c3d-8e2f-123456789abc";

    #[test]
    fn aggregate_stream_ids_are_type_hyphen_lower_case_id() {
        let id = AggregateId::from_existing(&ID.to_uppercase()).unwrap();
        let stream_id = StreamIds::for_aggregate("BankAccount", id);
        assert_eq!(stream_id, format!("BankAccount-{ID}"));
        assert_eq!(StreamIds::category_of(&stream_id), Some("BankAccount"));
    }

    #[test]
    fn parse_tells_the_three_kinds_apart() {
        let id = parse_uuid(ID).unwrap();
        assert_eq!(StreamIds::parse("$all"), StreamKind::All);
        assert_eq!(
            StreamIds::parse("BankAccount"),
            StreamKind::Category("BankAccount")
        );
        assert_eq!(
            StreamIds::parse(&format!("BankAccount-{ID}")),
            StreamKind::Aggregate {
                category: "BankAccount",
                id
            }
        );
        assert_eq!(
            StreamIds::parse(&format!("bank-account-{ID}")),
            StreamKind::Aggregate {
                category: "bank-account",
                id
            }
        );
    }

    #[test]
    fn only_the_canonical_form_is_an_aggregate_stream() {
        let not_aggregates = [
            format!("-{ID}"),
            ID.to_owned(),
            format!("BankAccount-{}", ID.to_uppercase()),
            format!("BankAccount_{ID}"),
            format!("BankAccount-{}", ID.replace('-', "")),
            format!("BankAccount-{}x", &ID[..35]),
            // Multi-byte characters where the split would fall.
            format!("é{}", "é".repeat(20)),
            String::new(),
        ];
        for stream_id in &not_aggregates {
            assert_eq!(
                StreamIds::parse(stream_id),
                StreamKind::Category(stream_id),
                "{stream_id}"
            );
            assert_eq!(StreamIds::category_of(stream_id), None, "{stream_id}");
        }
        assert_eq!(StreamIds::category_of(StreamIds::ALL), None);
    }

    #[test]
    fn a_subscription_without_a_stream_reads_all() {
        let mut subscription = Subscription {
            name: "balances".into(),
            stream_id: None,
            last_seen: None,
            created_at: DateTime::UNIX_EPOCH,
            failed_event_id: None,
            failed_attempts: 0,
        };
        assert_eq!(subscription.stream(), "$all");
        subscription.stream_id = Some("BankAccount".into());
        assert_eq!(subscription.stream(), "BankAccount");
    }

    /// A `load_stream_after_event` call: stream, cursor, limit.
    type Read = (String, Option<Uuid>, Option<usize>);

    /// Implements the required methods over a fixed stream, to check the
    /// provided ones.
    #[derive(Default)]
    struct Fake {
        reads: Mutex<Vec<Read>>,
        version: u64,
    }

    #[async_trait]
    impl EventStoreAdapter for Fake {
        async fn append_to_stream(
            &self,
            _: &str,
            _: Vec<SerializedEvent>,
            _: u64,
        ) -> Result<AppendResult> {
            unreachable!()
        }
        async fn load_stream(
            &self,
            _: &str,
            _: u64,
            _: Option<usize>,
        ) -> Result<Vec<SerializedEvent>> {
            unreachable!()
        }
        async fn load_stream_after_event(
            &self,
            stream_id: &str,
            after_event_id: Option<Uuid>,
            max_count: Option<usize>,
        ) -> Result<Vec<SerializedEvent>> {
            let read = (stream_id.to_owned(), after_event_id, max_count);
            self.reads.lock().unwrap().push(read);
            let envelope = EventEnvelope {
                id: new_v7(),
                correlation_id: new_v7(),
                causation_id: None,
                command_causation_id: None,
                metadata: Metadata::default(),
                occurred_at: DateTime::UNIX_EPOCH,
            };
            Ok(vec![SerializedEvent::new(
                &envelope,
                "E",
                "1.0.0",
                json!({}),
            )])
        }
        async fn load_by_correlation_id(&self, _: Uuid) -> Result<Vec<SerializedEvent>> {
            unreachable!()
        }
        async fn get_stream_metadata(&self, stream_id: &str) -> Result<Option<StreamMetadata>> {
            Ok((self.version > 0).then(|| StreamMetadata {
                stream_id: stream_id.to_owned(),
                version: self.version,
                created_at: DateTime::UNIX_EPOCH,
                updated_at: DateTime::UNIX_EPOCH,
            }))
        }
        async fn delete_stream(&self, _: &str) -> Result<()> {
            unreachable!()
        }
        async fn subscribe(&self, _: &str, _: Option<&str>) -> Result<Subscription> {
            unreachable!()
        }
        async fn get_subscription(&self, _: &str) -> Result<Option<Subscription>> {
            unreachable!()
        }
        async fn ack_subscription(&self, _: &str, _: Uuid) -> Result<()> {
            unreachable!()
        }
        async fn delete_subscription(&self, _: &str) -> Result<()> {
            unreachable!()
        }
        async fn list_subscriptions(&self) -> Result<Vec<Subscription>> {
            unreachable!()
        }
        async fn record_failed_attempt(&self, _: &str, _: Uuid) -> Result<u32> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn load_all_reads_the_all_stream() {
        let fake = Arc::new(Fake::default());
        let adapter: Arc<dyn EventStoreAdapter> = fake.clone();
        let cursor = new_v7();
        let events = adapter.load_all(Some(cursor), Some(10)).await.unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(
            *fake.reads.lock().unwrap(),
            [("$all".to_owned(), Some(cursor), Some(10))]
        );
    }

    #[tokio::test]
    async fn version_and_existence_come_from_the_metadata() {
        let stream_id = format!("BankAccount-{ID}");
        let missing: Arc<dyn EventStoreAdapter> = Arc::new(Fake::default());
        assert_eq!(missing.get_stream_version(&stream_id).await.unwrap(), 0);
        assert!(!missing.stream_exists(&stream_id).await.unwrap());

        let existing: Arc<dyn EventStoreAdapter> = Arc::new(Fake {
            version: 3,
            ..Fake::default()
        });
        assert_eq!(existing.get_stream_version(&stream_id).await.unwrap(), 3);
        assert!(existing.stream_exists(&stream_id).await.unwrap());
    }
}
