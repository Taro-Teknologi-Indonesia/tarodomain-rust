//! The SQL event store adapter (spec §3.2, §24).
//!
//! Events live in `tarodomain_events`. An append is one real transaction: a
//! conditional `UPDATE` of the stream's version (or an insert for a new
//! stream), then the events, whose global `position` stays NULL until the
//! [`Sequencer`] numbers them after commit. Reads across streams return only
//! positioned events, in position order, and number what is waiting first
//! unless a unit of work is active.

use std::collections::HashSet;
use std::sync::Arc;

use tarodomain::adapters::{
    AppendResult, EventStoreAdapter, StreamIds, StreamKind, StreamMetadata, Subscription,
};
use tarodomain::async_trait;
use tarodomain::domain::SerializedEvent;
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::Uuid;

use crate::db::{Db, Param, Row, is_conflict};
use crate::sequencer::Sequencer;

const EVENT_COLUMNS: &str = "event_id, stream_id, stream_version, event_type, event_version, \
     causation_id, correlation_id, command_causation_id, data, metadata, occurred_at";

/// Events per `INSERT`, so a large append stays under the bound-parameter
/// limit (12 parameters per event).
const INSERT_CHUNK: usize = 200;

/// The event store on SQL.
#[derive(Clone)]
pub struct SqlxEventStoreAdapter {
    db: Db,
    clock: Arc<dyn Clock>,
    sequencer: Sequencer,
}

impl SqlxEventStoreAdapter {
    /// An adapter over `db`. Stream and subscription timestamps come from
    /// `clock`.
    pub fn new(db: Db, clock: Arc<dyn Clock>) -> Self {
        let sequencer = Sequencer::new(db.clone());
        Self {
            db,
            clock,
            sequencer,
        }
    }

    /// The sequencer that numbers this store's events.
    pub fn sequencer(&self) -> &Sequencer {
        &self.sequencer
    }

    /// Numbers the events that are waiting, unless a unit of work is active
    /// (it can't see other transactions' events, and the sequencer must not
    /// wait on the unit's own locks).
    async fn sequence(&self) -> Result<()> {
        if !self.db.in_unit() {
            self.sequencer.try_run().await?;
        }
        Ok(())
    }

    async fn stored_metadata(&self, stream_id: &str) -> Result<Option<StreamMetadata>> {
        let row = self
            .db
            .fetch_one(
                "SELECT version, created_at, updated_at FROM tarodomain_streams WHERE stream_id = ?",
                vec![Param::text(stream_id)],
            )
            .await?;
        row.map(|row| {
            Ok(StreamMetadata {
                stream_id: stream_id.to_owned(),
                version: to_u64(row.i64("version")?),
                created_at: row.time("created_at")?,
                updated_at: row.time("updated_at")?,
            })
        })
        .transpose()
    }

    async fn version_of(&self, stream_id: &str) -> Result<u64> {
        Ok(self
            .stored_metadata(stream_id)
            .await?
            .map_or(0, |metadata| metadata.version))
    }

    /// Fails with `EVENT_APPEND_FAILED` if any of `events` is already stored
    /// or appears twice.
    async fn check_new(&self, stream_id: &str, events: &[SerializedEvent]) -> Result<()> {
        let duplicate = |event_id: Uuid| {
            FrameworkError::event_append_failed(format!("event {event_id} is already stored"))
                .with_context("streamId", stream_id)
                .with_context("eventId", event_id.to_string())
        };
        let mut seen = HashSet::new();
        for event in events {
            if !seen.insert(event.event_id) {
                return Err(duplicate(event.event_id));
            }
        }
        for chunk in events.chunks(INSERT_CHUNK) {
            let marks = vec!["?"; chunk.len()].join(", ");
            let rows = self
                .db
                .fetch(
                    &format!("SELECT event_id FROM tarodomain_events WHERE event_id IN ({marks})"),
                    chunk
                        .iter()
                        .map(|event| Param::Uuid(event.event_id))
                        .collect(),
                )
                .await?;
            if let Some(row) = rows.first() {
                return Err(duplicate(row.uuid("event_id")?));
            }
        }
        Ok(())
    }

    /// Moves the stream from `expected_version` to `new_version`, or fails
    /// with `OPTIMISTIC_CONCURRENCY`.
    async fn claim_stream(
        &self,
        stream_id: &str,
        expected_version: u64,
        new_version: u64,
    ) -> Result<()> {
        let now = self.clock.now();
        let changed = if expected_version == 0 {
            self.db
                .execute(
                    "INSERT INTO tarodomain_streams (stream_id, version, created_at, updated_at) \
                     VALUES (?, ?, ?, ?) ON CONFLICT (stream_id) DO NOTHING",
                    vec![
                        Param::text(stream_id),
                        new_version.into(),
                        now.into(),
                        now.into(),
                    ],
                )
                .await?
        } else {
            self.db
                .execute(
                    "UPDATE tarodomain_streams SET version = ?, updated_at = ? \
                     WHERE stream_id = ? AND version = ?",
                    vec![
                        new_version.into(),
                        now.into(),
                        Param::text(stream_id),
                        expected_version.into(),
                    ],
                )
                .await?
        };
        if changed == 1 {
            return Ok(());
        }
        let actual = self.version_of(stream_id).await?;
        Err(FrameworkError::optimistic_concurrency(
            stream_id,
            expected_version,
            actual,
        ))
    }

    async fn append_in_transaction(
        &self,
        stream_id: &str,
        category: &str,
        events: &[SerializedEvent],
        expected_version: u64,
    ) -> Result<AppendResult> {
        if events.is_empty() {
            let actual = self.version_of(stream_id).await?;
            if actual != expected_version {
                return Err(FrameworkError::optimistic_concurrency(
                    stream_id,
                    expected_version,
                    actual,
                ));
            }
            return Ok(AppendResult {
                version: expected_version,
            });
        }
        let new_version = expected_version + events.len() as u64;
        self.claim_stream(stream_id, expected_version, new_version)
            .await?;
        self.check_new(stream_id, events).await?;
        let mut version = expected_version;
        for chunk in events.chunks(INSERT_CHUNK) {
            let mut params = Vec::with_capacity(chunk.len() * 12);
            for event in chunk {
                version += 1;
                params.extend(event_params(stream_id, category, version, event)?);
            }
            let row = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            let values = vec![row; chunk.len()].join(", ");
            self.db
                .execute(
                    &format!(
                        "INSERT INTO tarodomain_events (event_id, stream_id, stream_category, \
                         stream_version, event_type, event_version, causation_id, correlation_id, \
                         command_causation_id, data, metadata, occurred_at) VALUES {values}"
                    ),
                    params,
                )
                .await?;
        }
        Ok(AppendResult {
            version: new_version,
        })
    }
}

#[async_trait]
impl EventStoreAdapter for SqlxEventStoreAdapter {
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
        let result = self
            .db
            .atomically(self.append_in_transaction(stream_id, category, &events, expected_version))
            .await;
        match result {
            // A racing writer that inserted the same event ID, or the same
            // stream version, got there first.
            Err(error) if is_conflict(&error) => {
                let actual = self.version_of(stream_id).await?;
                Err(
                    FrameworkError::optimistic_concurrency(stream_id, expected_version, actual)
                        .with_source(error),
                )
            }
            other => other,
        }
    }

    async fn load_stream(
        &self,
        stream_id: &str,
        after_version: u64,
        max_count: Option<usize>,
    ) -> Result<Vec<SerializedEvent>> {
        let rows = self
            .db
            .fetch(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM tarodomain_events \
                     WHERE stream_id = ? AND stream_version > ? ORDER BY stream_version LIMIT ?"
                ),
                vec![
                    Param::text(stream_id),
                    after_version.into(),
                    self.db.limit(max_count),
                ],
            )
            .await?;
        rows.iter().map(parse_event).collect()
    }

    async fn load_stream_after_event(
        &self,
        stream_id: &str,
        after_event_id: Option<Uuid>,
        max_count: Option<usize>,
    ) -> Result<Vec<SerializedEvent>> {
        let mut conditions = Vec::new();
        let mut params = Vec::new();
        let order = match StreamIds::parse(stream_id) {
            kind @ (StreamKind::All | StreamKind::Category(_)) => {
                self.sequence().await?;
                conditions.push("position IS NOT NULL");
                if let StreamKind::Category(category) = kind {
                    conditions.push("stream_category = ?");
                    params.push(Param::text(category));
                }
                if let Some(id) = after_event_id {
                    let cursor = self
                        .db
                        .fetch_one(
                            "SELECT position FROM tarodomain_events WHERE event_id = ?",
                            vec![Param::Uuid(id)],
                        )
                        .await?
                        .map(|row| row.opt_i64("position"))
                        .transpose()?
                        .flatten();
                    match cursor {
                        Some(position) => {
                            conditions.push("position > ?");
                            params.push(Param::Int(position));
                        }
                        None => {
                            // The cursor's event was deleted: fall back to its ID.
                            conditions.push("event_id > ?");
                            params.push(Param::Uuid(id));
                        }
                    }
                }
                "position"
            }
            StreamKind::Aggregate { .. } => {
                conditions.push("stream_id = ?");
                params.push(Param::text(stream_id));
                if let Some(id) = after_event_id {
                    let cursor = self
                        .db
                        .fetch_one(
                            "SELECT stream_version FROM tarodomain_events \
                             WHERE event_id = ? AND stream_id = ?",
                            vec![Param::Uuid(id), Param::text(stream_id)],
                        )
                        .await?;
                    match cursor {
                        Some(row) => {
                            conditions.push("stream_version > ?");
                            params.push(Param::Int(row.i64("stream_version")?));
                        }
                        None => {
                            conditions.push("event_id > ?");
                            params.push(Param::Uuid(id));
                        }
                    }
                }
                "stream_version"
            }
        };
        params.push(self.db.limit(max_count));
        let rows = self
            .db
            .fetch(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM tarodomain_events WHERE {} \
                     ORDER BY {order} LIMIT ?",
                    conditions.join(" AND ")
                ),
                params,
            )
            .await?;
        rows.iter().map(parse_event).collect()
    }

    async fn load_by_correlation_id(&self, correlation_id: Uuid) -> Result<Vec<SerializedEvent>> {
        self.sequence().await?;
        // Events that aren't numbered yet (a unit's own) come last, in insert order.
        let rows = self
            .db
            .fetch(
                &format!(
                    "SELECT {EVENT_COLUMNS} FROM tarodomain_events WHERE correlation_id = ? \
                     ORDER BY (position IS NULL), position, seq"
                ),
                vec![Param::Uuid(correlation_id)],
            )
            .await?;
        rows.iter().map(parse_event).collect()
    }

    async fn get_stream_version(&self, stream_id: &str) -> Result<u64> {
        self.version_of(stream_id).await
    }

    async fn get_stream_metadata(&self, stream_id: &str) -> Result<Option<StreamMetadata>> {
        self.stored_metadata(stream_id).await
    }

    async fn delete_stream(&self, stream_id: &str) -> Result<()> {
        self.db
            .atomically(async {
                self.db
                    .execute(
                        "DELETE FROM tarodomain_events WHERE stream_id = ?",
                        vec![Param::text(stream_id)],
                    )
                    .await?;
                self.db
                    .execute(
                        "DELETE FROM tarodomain_streams WHERE stream_id = ?",
                        vec![Param::text(stream_id)],
                    )
                    .await?;
                Ok(())
            })
            .await
    }

    async fn subscribe(&self, name: &str, stream_id: Option<&str>) -> Result<Subscription> {
        let stream_id = stream_id
            .filter(|&stream_id| stream_id != StreamIds::ALL)
            .map(str::to_owned);
        self.db
            .execute(
                "INSERT INTO tarodomain_subscriptions (subscription_name, stream_id, created_at) \
                 VALUES (?, ?, ?) ON CONFLICT (subscription_name) DO NOTHING",
                vec![
                    Param::text(name),
                    Param::opt_text(stream_id),
                    self.clock.now().into(),
                ],
            )
            .await?;
        self.get_subscription(name).await?.ok_or_else(|| {
            FrameworkError::event_store_error(format!("subscription {name:?} was not stored"))
        })
    }

    async fn get_subscription(&self, name: &str) -> Result<Option<Subscription>> {
        let row = self
            .db
            .fetch_one(
                &format!("{SUBSCRIPTION_SELECT} WHERE subscription_name = ?"),
                vec![Param::text(name)],
            )
            .await?;
        row.as_ref().map(parse_subscription).transpose()
    }

    async fn ack_subscription(&self, name: &str, last_seen_event_id: Uuid) -> Result<()> {
        // Moves forward in append order only. If either event is no longer
        // stored (its stream was deleted) or isn't numbered yet, the IDs are
        // compared instead.
        // Number what is waiting, so both events have positions to compare.
        self.sequence().await?;
        let id = Param::Uuid(last_seen_event_id);
        let position = "(SELECT position FROM tarodomain_events WHERE event_id = ?)";
        self.db
            .execute(
                &format!(
                    "UPDATE tarodomain_subscriptions SET last_seen = ?, \
                     last_position = COALESCE({position}, last_position), \
                     failed_event_id = NULL, failed_attempts = 0 \
                     WHERE subscription_name = ? AND (last_seen IS NULL \
                       OR {position} > last_position \
                       OR ({position} IS NULL AND ? > last_seen))"
                ),
                vec![
                    id.clone(),
                    id.clone(),
                    Param::text(name),
                    id.clone(),
                    id.clone(),
                    id,
                ],
            )
            .await
            .map(|_| ())
    }

    async fn delete_subscription(&self, name: &str) -> Result<()> {
        self.db
            .execute(
                "DELETE FROM tarodomain_subscriptions WHERE subscription_name = ?",
                vec![Param::text(name)],
            )
            .await
            .map(|_| ())
    }

    async fn list_subscriptions(&self) -> Result<Vec<Subscription>> {
        let rows = self
            .db
            .fetch(
                &format!("{SUBSCRIPTION_SELECT} ORDER BY subscription_name"),
                vec![],
            )
            .await?;
        rows.iter().map(parse_subscription).collect()
    }

    async fn record_failed_attempt(&self, name: &str, event_id: Uuid) -> Result<u32> {
        let id = Param::Uuid(event_id);
        self.db
            .execute(
                "UPDATE tarodomain_subscriptions SET \
                 failed_attempts = CASE WHEN failed_event_id = ? THEN failed_attempts + 1 ELSE 1 END, \
                 failed_event_id = ? WHERE subscription_name = ?",
                vec![id.clone(), id, Param::text(name)],
            )
            .await?;
        Ok(self
            .get_subscription(name)
            .await?
            .map_or(0, |subscription| subscription.failed_attempts))
    }
}

const SUBSCRIPTION_SELECT: &str = "SELECT subscription_name, stream_id, last_seen, created_at, \
     failed_event_id, failed_attempts FROM tarodomain_subscriptions";

fn to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn event_params(
    stream_id: &str,
    category: &str,
    version: u64,
    event: &SerializedEvent,
) -> Result<Vec<Param>> {
    Ok(vec![
        Param::Uuid(event.event_id),
        Param::text(stream_id),
        Param::text(category),
        version.into(),
        Param::text(event.event_type.as_str()),
        Param::text(event.event_version.as_str()),
        Param::opt_uuid(event.causation_id),
        Param::Uuid(event.correlation_id),
        Param::opt_uuid(event.command_causation_id),
        Param::Json(event.data.clone()),
        Param::json(&event.metadata)?,
        event.occurred_at.into(),
    ])
}

fn parse_event(row: &Row) -> Result<SerializedEvent> {
    Ok(SerializedEvent {
        event_id: row.uuid("event_id")?,
        event_type: row.str("event_type")?,
        event_version: row.str("event_version")?,
        causation_id: row.opt_uuid("causation_id")?,
        correlation_id: row.uuid("correlation_id")?,
        command_causation_id: row.opt_uuid("command_causation_id")?,
        data: row.json("data")?,
        metadata: row.metadata("metadata")?,
        occurred_at: row.time("occurred_at")?,
    })
}

fn parse_subscription(row: &Row) -> Result<Subscription> {
    Ok(Subscription {
        name: row.str("subscription_name")?,
        stream_id: row.opt_str("stream_id")?,
        last_seen: row.opt_uuid("last_seen")?,
        created_at: row.time("created_at")?,
        failed_event_id: row.opt_uuid("failed_event_id")?,
        failed_attempts: u32::try_from(row.opt_i64("failed_attempts")?.unwrap_or(0)).unwrap_or(0),
    })
}
