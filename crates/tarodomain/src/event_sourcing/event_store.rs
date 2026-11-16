//! The event store (spec §3.3, §7.1): an event store adapter behind the
//! serialization boundary, so the rest of the framework sees typed events.

use std::fmt;
use std::sync::Arc;

use super::EventRegistry;
use crate::adapters::{AppendResult, EventStoreAdapter, StreamIds, StreamMetadata};
use crate::domain::{DomainEvent, SerializedEvent};
use crate::error::{FrameworkError, Result};
use crate::kernel::ids::Uuid;

/// The event store (spec §3.3): an [`EventStoreAdapter`] that takes and
/// returns [`DomainEvent`]s, serializing them on the way in and reading them
/// back through the [`EventRegistry`], upcasters included, on the way out.
///
/// It appends only events the registry can read back: one whose type isn't
/// registered, or is registered at another version, is refused before
/// anything is written. Reads, versions, cursors and conflicts follow the
/// adapter contract; see [`EventStoreAdapter`] for those semantics.
///
/// Subscriptions aren't wrapped: the catch-up dispatcher reads raw events
/// through [`adapter`](Self::adapter) and deserializes them one at a time,
/// so that one unreadable event doesn't block the others.
///
/// Cloning is cheap: clones share the adapter and the registry.
#[derive(Clone)]
pub struct EventStore {
    adapter: Arc<dyn EventStoreAdapter>,
    registry: Arc<EventRegistry>,
}

impl EventStore {
    /// An event store over `adapter` that reads events through `registry`.
    pub fn new(adapter: Arc<dyn EventStoreAdapter>, registry: Arc<EventRegistry>) -> Self {
        Self { adapter, registry }
    }

    /// The adapter, for what this wrapper doesn't cover, such as
    /// subscriptions and raw reads.
    pub fn adapter(&self) -> &Arc<dyn EventStoreAdapter> {
        &self.adapter
    }

    /// The registry events are read through.
    pub fn registry(&self) -> &Arc<EventRegistry> {
        &self.registry
    }

    /// The storable form of `event` (§3.3: `event.serialize()`), if the
    /// registry can read it back.
    ///
    /// Fails with `UNKNOWN_EVENT_TYPE` if the event's type isn't registered,
    /// with `EVENT_VERSION_MISMATCH` if it is registered at another version
    /// than the event's, and with `SERIALIZATION_ERROR` if the payload has
    /// no JSON form.
    pub fn serialize(&self, event: &DomainEvent) -> Result<SerializedEvent> {
        let (name, version) = (event.name(), event.version());
        match self.registry.registered_version(name) {
            Some(registered) if registered == version => event.serialize(),
            Some(registered) => Err(FrameworkError::event_version_mismatch(format!(
                "event {name} {} is version {version}, but version {registered} is registered, \
                 so it can't be stored",
                event.id()
            ))
            .with_context("eventType", name)
            .with_context("eventId", event.id().to_string())
            .with_context("version", version)
            .with_context("registeredVersion", registered)),
            None => Err(FrameworkError::unknown_event_type(format!(
                "event type {name} isn't registered, so event {} can't be stored",
                event.id()
            ))
            .with_context("eventType", name)
            .with_context("eventId", event.id().to_string())),
        }
    }

    /// Reads a stored event through the registry (§3.4), upcasters included.
    pub fn deserialize(&self, serialized: &SerializedEvent) -> Result<DomainEvent> {
        self.registry.deserialize(serialized)
    }

    /// Appends `events` to an aggregate stream at `expected_version`
    /// (§7.1–7.3), returning the stream's new version.
    ///
    /// Every event is serialized before anything is written, so if one
    /// fails [`serialize`](Self::serialize), none is stored; the error gets
    /// `streamId` as context. A stale `expected_version` fails with
    /// `OPTIMISTIC_CONCURRENCY`, unchanged from the adapter.
    pub async fn append_to_stream(
        &self,
        stream_id: &str,
        events: &[DomainEvent],
        expected_version: u64,
    ) -> Result<AppendResult> {
        let serialized = events
            .iter()
            .map(|event| self.serialize(event))
            .collect::<Result<Vec<_>>>()
            .map_err(|error| error.with_context("streamId", stream_id))?;
        self.adapter
            .append_to_stream(stream_id, serialized, expected_version)
            .await
    }

    /// The events of an aggregate stream with versions after
    /// `after_version`, at most `max_count` of them, in version order.
    ///
    /// A stored event the registry can't read fails the whole load, with
    /// `streamId` added to the registry's error.
    pub async fn load_stream(
        &self,
        stream_id: &str,
        after_version: u64,
        max_count: Option<usize>,
    ) -> Result<Vec<DomainEvent>> {
        let events = self
            .adapter
            .load_stream(stream_id, after_version, max_count)
            .await?;
        self.deserialize_all(&events)
            .map_err(|error| error.with_context("streamId", stream_id))
    }

    /// The events of any stream (an aggregate stream, a category or `$all`)
    /// after the event `after_event_id`, at most `max_count` of them, in
    /// append order.
    ///
    /// A stored event the registry can't read fails the whole load, with
    /// `streamId` added to the registry's error.
    pub async fn load_stream_after_event(
        &self,
        stream_id: &str,
        after_event_id: Option<Uuid>,
        max_count: Option<usize>,
    ) -> Result<Vec<DomainEvent>> {
        let events = self
            .adapter
            .load_stream_after_event(stream_id, after_event_id, max_count)
            .await?;
        self.deserialize_all(&events)
            .map_err(|error| error.with_context("streamId", stream_id))
    }

    /// Every event after `after_event_id`, at most `max_count` of them, in
    /// append order.
    pub async fn load_all(
        &self,
        after_event_id: Option<Uuid>,
        max_count: Option<usize>,
    ) -> Result<Vec<DomainEvent>> {
        let events = self.adapter.load_all(after_event_id, max_count).await?;
        self.deserialize_all(&events)
            .map_err(|error| error.with_context("streamId", StreamIds::ALL))
    }

    /// Every event with this correlation ID, in append order (§28.3).
    pub async fn load_by_correlation_id(&self, correlation_id: Uuid) -> Result<Vec<DomainEvent>> {
        let events = self.adapter.load_by_correlation_id(correlation_id).await?;
        self.deserialize_all(&events)
            .map_err(|error| error.with_context("correlationId", correlation_id.to_string()))
    }

    /// Whether the aggregate stream has any events.
    pub async fn stream_exists(&self, stream_id: &str) -> Result<bool> {
        self.adapter.stream_exists(stream_id).await
    }

    /// The aggregate stream's version, 0 if it has no events.
    pub async fn get_stream_version(&self, stream_id: &str) -> Result<u64> {
        self.adapter.get_stream_version(stream_id).await
    }

    /// What the store knows about the aggregate stream, or `None` if it has
    /// no events.
    pub async fn get_stream_metadata(&self, stream_id: &str) -> Result<Option<StreamMetadata>> {
        self.adapter.get_stream_metadata(stream_id).await
    }

    /// Deletes the aggregate stream's events and metadata.
    pub async fn delete_stream(&self, stream_id: &str) -> Result<()> {
        self.adapter.delete_stream(stream_id).await
    }

    fn deserialize_all(&self, events: &[SerializedEvent]) -> Result<Vec<DomainEvent>> {
        events
            .iter()
            .map(|event| self.registry.deserialize(event))
            .collect()
    }
}

impl fmt::Debug for EventStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventStore")
            .field("registry", &self.registry)
            .finish_non_exhaustive()
    }
}
