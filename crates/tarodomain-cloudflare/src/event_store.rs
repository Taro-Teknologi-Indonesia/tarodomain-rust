//! The D1 event store adapter (spec §3.2, §24.3).
//!
//! Events live in `tarodomain_events`, ordered by an `AUTOINCREMENT`
//! position: D1 runs one write at a time, so insert order is commit order and
//! D1 needs no sequencer. `(stream_id, stream_version)` is unique, which is
//! how a stale writer is detected.

use std::collections::HashSet;
use std::sync::Arc;

use serde_json::{Value, json};
use tarodomain::adapters::{
    AppendResult, EventStoreAdapter, StreamIds, StreamKind, StreamMetadata, Subscription,
};
use tarodomain::async_trait;
use tarodomain::domain::{Metadata, SerializedEvent, format_timestamp};
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::Uuid;

use crate::d1::{D1Db, Row, RowExt, Statement, insert_rows, is_constraint_violation};
use crate::unit_of_work::{D1UnitOfWork, StagedEvent, StagedStream};

const EVENT_COLUMNS: &str = "event_id, stream_id, stream_version, event_type, event_version, \
     causation_id, correlation_id, command_causation_id, data, metadata, occurred_at";

/// The event store on D1.
///
/// Appends go in one batch, or are staged in the active unit of work with
/// their stream versions. Inside a unit, reads include the events the unit
/// staged (the overlay), because D1 can't show them before the commit.
#[derive(Clone)]
pub struct D1EventStoreAdapter {
    db: D1Db,
    clock: Arc<dyn Clock>,
}

impl D1EventStoreAdapter {
    /// An adapter over `db`. Stream and subscription timestamps come from
    /// `clock`.
    pub fn new(db: D1Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }

    /// The version of `stream_id`, counting what the active unit staged.
    async fn current_version(&self, stream_id: &str) -> Result<u64> {
        let staged = D1UnitOfWork::with_active(&self.db, |overlay| {
            overlay.streams.get(stream_id).map(|stream| stream.version)
        });
        if let Some(Some(version)) = staged {
            return Ok(version);
        }
        self.stored_metadata(stream_id)
            .await
            .map(|metadata| metadata.map_or(0, |metadata| metadata.version))
    }

    async fn stored_metadata(&self, stream_id: &str) -> Result<Option<StreamMetadata>> {
        let row = self
            .db
            .query_one(Statement::new(
                "SELECT version, created_at, updated_at FROM tarodomain_streams WHERE stream_id = ?",
                vec![json!(stream_id)],
            ))
            .await?;
        row.map(|row| {
            Ok(StreamMetadata {
                stream_id: stream_id.to_owned(),
                version: to_u64(row.require_i64("version")?),
                created_at: row.require_time("created_at")?,
                updated_at: row.require_time("updated_at")?,
            })
        })
        .transpose()
    }

    /// Fails with `EVENT_APPEND_FAILED` if any of `events` is already stored
    /// (or staged in the unit), or appears twice.
    async fn check_new(&self, stream_id: &str, events: &[SerializedEvent]) -> Result<()> {
        let mut seen = HashSet::new();
        let duplicate = |event_id: Uuid| {
            FrameworkError::event_append_failed(format!("event {event_id} is already stored"))
                .with_context("streamId", stream_id)
                .with_context("eventId", event_id.to_string())
        };
        for event in events {
            if !seen.insert(event.event_id) {
                return Err(duplicate(event.event_id));
            }
        }
        let staged = D1UnitOfWork::with_active(&self.db, |overlay| {
            overlay
                .events
                .iter()
                .map(|staged| staged.event.event_id)
                .collect::<HashSet<_>>()
        })
        .unwrap_or_default();
        if let Some(event) = events.iter().find(|event| staged.contains(&event.event_id)) {
            return Err(duplicate(event.event_id));
        }
        let ids: Vec<Uuid> = events.iter().map(|event| event.event_id).collect();
        for chunk in ids.chunks(90) {
            let marks = vec!["?"; chunk.len()].join(", ");
            let rows = self
                .db
                .query(Statement::new(
                    format!("SELECT event_id FROM tarodomain_events WHERE event_id IN ({marks})"),
                    chunk.iter().map(|id| json!(id.to_string())).collect(),
                ))
                .await?;
            if let Some(row) = rows.first() {
                return Err(duplicate(row.require_uuid("event_id")?));
            }
        }
        Ok(())
    }

    /// The events the active unit staged that belong to `stream_id`.
    fn staged_in(&self, stream_id: &str) -> Vec<StagedEvent> {
        let kind = StreamIds::parse(stream_id);
        D1UnitOfWork::with_active(&self.db, |overlay| {
            overlay
                .events
                .iter()
                .filter(|staged| match kind {
                    StreamKind::All => true,
                    StreamKind::Category(category) => {
                        StreamIds::category_of(&staged.stream_id) == Some(category)
                    }
                    StreamKind::Aggregate { .. } => staged.stream_id == stream_id,
                })
                .cloned()
                .collect()
        })
        .unwrap_or_default()
    }
}

#[async_trait]
impl EventStoreAdapter for D1EventStoreAdapter {
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
        let actual = self.current_version(stream_id).await?;
        if actual != expected_version {
            return Err(FrameworkError::optimistic_concurrency(
                stream_id,
                expected_version,
                actual,
            ));
        }
        self.check_new(stream_id, &events).await?;
        if events.is_empty() {
            return Ok(AppendResult {
                version: expected_version,
            });
        }

        let now = self.clock.now();
        let new_version = expected_version + events.len() as u64;
        let created_at = self
            .stored_metadata(stream_id)
            .await?
            .map_or(now, |metadata| metadata.created_at);
        let rows = events
            .iter()
            .zip(expected_version + 1..)
            .map(|(event, version)| event_row(stream_id, category, version, event))
            .collect();
        let mut statements = vec![guard(stream_id, expected_version)];
        statements.extend(insert_rows(
            "tarodomain_events",
            &[
                "event_id",
                "stream_id",
                "stream_category",
                "stream_version",
                "event_type",
                "event_version",
                "causation_id",
                "correlation_id",
                "command_causation_id",
                "data",
                "metadata",
                "occurred_at",
            ],
            rows,
            "",
        ));
        statements.push(Statement::new(
            "INSERT INTO tarodomain_streams (stream_id, version, created_at, updated_at) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT (stream_id) DO UPDATE SET version = excluded.version, \
             updated_at = excluded.updated_at",
            vec![
                json!(stream_id),
                json!(new_version),
                json!(format_timestamp(created_at)),
                json!(format_timestamp(now)),
            ],
        ));

        // Inside a unit this stages, and the overlay shows the events to later
        // reads in the unit.
        let staged = D1UnitOfWork::with_active(&self.db, |overlay| {
            for (event, version) in events.iter().zip(expected_version + 1..) {
                overlay.events.push(StagedEvent {
                    stream_id: stream_id.to_owned(),
                    stream_version: version,
                    event: event.clone(),
                });
            }
            let stream = overlay
                .streams
                .entry(stream_id.to_owned())
                .or_insert(StagedStream {
                    base_version: expected_version,
                    version: 0,
                    created_at,
                    updated_at: now,
                });
            stream.version = new_version;
            stream.updated_at = now;
        });
        match self.db.write(statements).await {
            Ok(()) => Ok(AppendResult {
                version: new_version,
            }),
            Err(error) if is_constraint_violation(&error) => {
                // A stale writer: someone else appended first. (A duplicate
                // event ID was ruled out above.)
                let actual = self.current_version(stream_id).await?;
                debug_assert!(staged.is_none(), "a unit stages, so it can't fail here");
                if actual == expected_version {
                    Err(
                        FrameworkError::event_append_failed("the events could not be appended")
                            .with_context("streamId", stream_id)
                            .with_source(error),
                    )
                } else {
                    Err(FrameworkError::optimistic_concurrency(
                        stream_id,
                        expected_version,
                        actual,
                    ))
                }
            }
            Err(error) => Err(error),
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
            .query(Statement::new(
                format!(
                    "SELECT {EVENT_COLUMNS} FROM tarodomain_events \
                     WHERE stream_id = ? AND stream_version > ? ORDER BY stream_version LIMIT ?"
                ),
                vec![
                    json!(stream_id),
                    json!(after_version),
                    json!(limit(max_count)),
                ],
            ))
            .await?;
        let mut events = rows.iter().map(parse_event).collect::<Result<Vec<_>>>()?;
        events.extend(
            self.staged_in(stream_id)
                .into_iter()
                .filter(|staged| staged.stream_version > after_version)
                .map(|staged| staged.event),
        );
        events.truncate(max_count.unwrap_or(usize::MAX));
        Ok(events)
    }

    async fn load_stream_after_event(
        &self,
        stream_id: &str,
        after_event_id: Option<Uuid>,
        max_count: Option<usize>,
    ) -> Result<Vec<SerializedEvent>> {
        let take = max_count.unwrap_or(usize::MAX);
        let staged: Vec<SerializedEvent> = self
            .staged_in(stream_id)
            .into_iter()
            .map(|staged| staged.event)
            .collect();
        // A cursor inside the unit's own events: only later staged events follow.
        if let Some(id) = after_event_id {
            if let Some(index) = staged.iter().position(|event| event.event_id == id) {
                return Ok(staged.into_iter().skip(index + 1).take(take).collect());
            }
        }

        let mut conditions = Vec::new();
        let mut params = Vec::new();
        match StreamIds::parse(stream_id) {
            StreamKind::All => {}
            StreamKind::Category(category) => {
                conditions.push("stream_category = ?");
                params.push(json!(category));
            }
            StreamKind::Aggregate { .. } => {
                conditions.push("stream_id = ?");
                params.push(json!(stream_id));
            }
        }
        if let Some(id) = after_event_id {
            let cursor = self
                .db
                .query_one(Statement::new(
                    "SELECT position FROM tarodomain_events WHERE event_id = ?",
                    vec![json!(id.to_string())],
                ))
                .await?;
            match cursor {
                Some(row) => {
                    conditions.push("position > ?");
                    params.push(json!(row.require_i64("position")?));
                }
                None => {
                    // The cursor's event was deleted: fall back to its ID.
                    conditions.push("event_id > ?");
                    params.push(json!(id.to_string()));
                }
            }
        }
        let filter = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        params.push(json!(limit(max_count)));
        let rows = self
            .db
            .query(Statement::new(
                format!(
                    "SELECT {EVENT_COLUMNS} FROM tarodomain_events {filter} \
                     ORDER BY position LIMIT ?"
                ),
                params,
            ))
            .await?;
        let mut events = rows.iter().map(parse_event).collect::<Result<Vec<_>>>()?;
        events.extend(staged);
        events.truncate(take);
        Ok(events)
    }

    async fn load_by_correlation_id(&self, correlation_id: Uuid) -> Result<Vec<SerializedEvent>> {
        let rows = self
            .db
            .query(Statement::new(
                format!(
                    "SELECT {EVENT_COLUMNS} FROM tarodomain_events \
                     WHERE correlation_id = ? ORDER BY position"
                ),
                vec![json!(correlation_id.to_string())],
            ))
            .await?;
        let mut events = rows.iter().map(parse_event).collect::<Result<Vec<_>>>()?;
        events.extend(
            self.staged_in(StreamIds::ALL)
                .into_iter()
                .map(|staged| staged.event)
                .filter(|event| event.correlation_id == correlation_id),
        );
        Ok(events)
    }

    async fn get_stream_version(&self, stream_id: &str) -> Result<u64> {
        self.current_version(stream_id).await
    }

    async fn get_stream_metadata(&self, stream_id: &str) -> Result<Option<StreamMetadata>> {
        let stored = self.stored_metadata(stream_id).await?;
        let staged =
            D1UnitOfWork::with_active(&self.db, |overlay| overlay.streams.get(stream_id).cloned())
                .flatten();
        Ok(match (stored, staged) {
            (stored, None) => stored,
            (None, Some(staged)) => Some(StreamMetadata {
                stream_id: stream_id.to_owned(),
                version: staged.version,
                created_at: staged.created_at,
                updated_at: staged.updated_at,
            }),
            (Some(stored), Some(staged)) => Some(StreamMetadata {
                version: staged.version,
                updated_at: staged.updated_at,
                ..stored
            }),
        })
    }

    async fn delete_stream(&self, stream_id: &str) -> Result<()> {
        D1UnitOfWork::with_active(&self.db, |overlay| {
            overlay
                .events
                .retain(|staged| staged.stream_id != stream_id);
            overlay.streams.remove(stream_id);
        });
        self.db
            .write(vec![
                Statement::new(
                    "DELETE FROM tarodomain_events WHERE stream_id = ?",
                    vec![json!(stream_id)],
                ),
                Statement::new(
                    "DELETE FROM tarodomain_streams WHERE stream_id = ?",
                    vec![json!(stream_id)],
                ),
            ])
            .await
    }

    async fn subscribe(&self, name: &str, stream_id: Option<&str>) -> Result<Subscription> {
        let stream_id = stream_id
            .filter(|&stream_id| stream_id != StreamIds::ALL)
            .map(str::to_owned);
        let created_at = self.clock.now();
        self.db
            .write(vec![Statement::new(
                "INSERT INTO tarodomain_subscriptions (subscription_name, stream_id, created_at) \
                 VALUES (?, ?, ?) ON CONFLICT (subscription_name) DO NOTHING",
                vec![
                    json!(name),
                    json!(stream_id),
                    json!(format_timestamp(created_at)),
                ],
            )])
            .await?;
        Ok(match self.get_subscription(name).await? {
            Some(subscription) => subscription,
            // Staged in a unit of work, so not stored yet.
            None => Subscription {
                name: name.to_owned(),
                stream_id,
                last_seen: None,
                created_at,
                failed_event_id: None,
                failed_attempts: 0,
            },
        })
    }

    async fn get_subscription(&self, name: &str) -> Result<Option<Subscription>> {
        let row = self
            .db
            .query_one(Statement::new(
                format!("{SUBSCRIPTION_SELECT} WHERE subscription_name = ?"),
                vec![json!(name)],
            ))
            .await?;
        row.as_ref().map(parse_subscription).transpose()
    }

    async fn ack_subscription(&self, name: &str, last_seen_event_id: Uuid) -> Result<()> {
        // Moves forward in append order only. If either event is no longer
        // stored (its stream was deleted), the IDs are compared instead.
        let id = json!(last_seen_event_id.to_string());
        let position = "(SELECT position FROM tarodomain_events WHERE event_id = ?)";
        self.db
            .write(vec![Statement::new(
                format!(
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
                    json!(name),
                    id.clone(),
                    id.clone(),
                    id,
                ],
            )])
            .await
    }

    async fn delete_subscription(&self, name: &str) -> Result<()> {
        self.db
            .write(vec![Statement::new(
                "DELETE FROM tarodomain_subscriptions WHERE subscription_name = ?",
                vec![json!(name)],
            )])
            .await
    }

    async fn list_subscriptions(&self) -> Result<Vec<Subscription>> {
        let rows = self
            .db
            .query(Statement::plain(format!(
                "{SUBSCRIPTION_SELECT} ORDER BY subscription_name"
            )))
            .await?;
        rows.iter().map(parse_subscription).collect()
    }

    async fn record_failed_attempt(&self, name: &str, event_id: Uuid) -> Result<u32> {
        let id = json!(event_id.to_string());
        self.db
            .write(vec![Statement::new(
                "UPDATE tarodomain_subscriptions SET \
                 failed_attempts = CASE WHEN failed_event_id = ? THEN failed_attempts + 1 ELSE 1 END, \
                 failed_event_id = ? WHERE subscription_name = ?",
                vec![id.clone(), id, json!(name)],
            )])
            .await?;
        Ok(self
            .get_subscription(name)
            .await?
            .map_or(0, |subscription| subscription.failed_attempts))
    }
}

const SUBSCRIPTION_SELECT: &str = "SELECT subscription_name, stream_id, last_seen, created_at, \
     failed_event_id, failed_attempts FROM tarodomain_subscriptions";

/// A statement that fails the batch (a NOT NULL violation, which D1 reports
/// as a constraint) unless the stream is at `expected_version`. The unique
/// `(stream_id, stream_version)` key catches a writer that is behind; this
/// catches one that is ahead of the stream (for example after a delete).
fn guard(stream_id: &str, expected_version: u64) -> Statement {
    let violation = "INSERT INTO tarodomain_locks (name, holder, expires_at) \
                     SELECT NULL, '', 0";
    if expected_version == 0 {
        Statement::new(
            format!(
                "{violation} WHERE EXISTS \
                 (SELECT 1 FROM tarodomain_streams WHERE stream_id = ? AND version > 0)"
            ),
            vec![json!(stream_id)],
        )
    } else {
        Statement::new(
            format!(
                "{violation} WHERE NOT EXISTS \
                 (SELECT 1 FROM tarodomain_streams WHERE stream_id = ? AND version = ?)"
            ),
            vec![json!(stream_id), json!(expected_version)],
        )
    }
}

fn limit(max_count: Option<usize>) -> i64 {
    // SQLite reads a negative LIMIT as "no limit".
    max_count.map_or(-1, |count| i64::try_from(count).unwrap_or(i64::MAX))
}

fn to_u64(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn event_row(stream_id: &str, category: &str, version: u64, event: &SerializedEvent) -> Vec<Value> {
    vec![
        json!(event.event_id.to_string()),
        json!(stream_id),
        json!(category),
        json!(version),
        json!(event.event_type),
        json!(event.event_version),
        json!(event.causation_id.map(|id| id.to_string())),
        json!(event.correlation_id.to_string()),
        json!(event.command_causation_id.map(|id| id.to_string())),
        json!(event.data.to_string()),
        json!(serde_json::to_string(&event.metadata).unwrap_or_else(|_| "{}".into())),
        json!(format_timestamp(event.occurred_at)),
    ]
}

fn parse_event(row: &Row) -> Result<SerializedEvent> {
    let metadata: Metadata =
        serde_json::from_value(row.require_json("metadata")?).map_err(|error| {
            FrameworkError::serialization_error("stored event metadata is invalid")
                .with_source(error)
        })?;
    Ok(SerializedEvent {
        event_id: row.require_uuid("event_id")?,
        event_type: row.require_str("event_type")?.to_owned(),
        event_version: row.require_str("event_version")?.to_owned(),
        causation_id: row.optional_uuid("causation_id")?,
        correlation_id: row.require_uuid("correlation_id")?,
        command_causation_id: row.optional_uuid("command_causation_id")?,
        data: row.require_json("data")?,
        metadata,
        occurred_at: row.require_time("occurred_at")?,
    })
}

fn parse_subscription(row: &Row) -> Result<Subscription> {
    Ok(Subscription {
        name: row.require_str("subscription_name")?.to_owned(),
        stream_id: row.str("stream_id").map(str::to_owned),
        last_seen: row.optional_uuid("last_seen")?,
        created_at: row.require_time("created_at")?,
        failed_event_id: row.optional_uuid("failed_event_id")?,
        failed_attempts: u32::try_from(row.i64("failed_attempts").unwrap_or(0)).unwrap_or(0),
    })
}
