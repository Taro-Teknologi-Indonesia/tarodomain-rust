//! The event-sourced repository (spec §6.2): aggregates stored as their
//! events, with optional snapshots.

use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use super::{
    ErrorHook, Repository, adopt_context, ignore_errors, mark_saved, publish_written, recovered,
};
use crate::adapters::{Snapshot, SnapshotInput, SnapshotStoreAdapter, StreamIds};
use crate::async_trait;
use crate::bus::EventBus;
use crate::domain::{Aggregate, AggregateId, Metadata};
use crate::error::{FrameworkError, Result};
use crate::event_sourcing::EventStore;
use crate::kernel::ids::Uuid;

/// A [`Repository`] that stores an aggregate as the events in its stream,
/// `"<TYPE>-<id>"` (spec §6.2).
///
/// **Load:** restore the latest snapshot, if a snapshot store is set and has
/// one, then replay the events after it. An aggregate with neither doesn't
/// exist. Snapshots are only a cache: a snapshot that can't be loaded or
/// restored goes to [`on_snapshot_error`](Self::on_snapshot_error), and the
/// whole stream is replayed instead.
///
/// **Save:** append the uncommitted events at the aggregate's
/// [`committed_version`](Aggregate::committed_version), publish them on the
/// bus, and mark them committed. Then, if the save crossed a multiple of
/// `snapshot_every`, save a snapshot at the new version; a failure there is
/// reported to `on_snapshot_error` too and doesn't fail the save. "Crossed",
/// because a save of several events can jump over the exact multiple: with
/// `snapshot_every` 10, a save from version 9 to 12 snapshots at 12.
///
/// Cloning is cheap: clones share the store, bus and hooks.
pub struct EventSourcedRepository<A> {
    event_store: EventStore,
    event_bus: Arc<dyn EventBus>,
    snapshots: Option<Arc<dyn SnapshotStoreAdapter>>,
    snapshot_every: u64,
    on_snapshot_error: ErrorHook,
    on_publish_error: ErrorHook,
    _aggregate: PhantomData<fn() -> A>,
}

impl<A: Aggregate> EventSourcedRepository<A> {
    /// A repository over `event_store` that publishes on `event_bus`, without
    /// snapshots.
    pub fn new(event_store: EventStore, event_bus: Arc<dyn EventBus>) -> Self {
        Self {
            event_store,
            event_bus,
            snapshots: None,
            snapshot_every: 0,
            on_snapshot_error: ignore_errors(),
            on_publish_error: ignore_errors(),
            _aggregate: PhantomData,
        }
    }

    /// Loads from the latest snapshot in `store`, and saves one whenever a
    /// save crosses a multiple of `snapshot_every`. With `snapshot_every` 0,
    /// snapshots already in the store are still used, but none is taken.
    pub fn with_snapshots(
        mut self,
        store: Arc<dyn SnapshotStoreAdapter>,
        snapshot_every: u64,
    ) -> Self {
        self.snapshots = Some(store);
        self.snapshot_every = snapshot_every;
        self
    }

    /// Called when loading, restoring or saving a snapshot fails. The
    /// error's `operation` context is `loadSnapshot`, `restoreSnapshot` or
    /// `saveSnapshot`. Ignored by default.
    pub fn on_snapshot_error(
        mut self,
        hook: impl Fn(&FrameworkError) + Send + Sync + 'static,
    ) -> Self {
        self.on_snapshot_error = Arc::new(hook);
        self
    }

    /// Called when publishing fails after a durable append, outside a unit
    /// of work (see [`Repository::save`]). The error's `operation` context is
    /// `publish`. Ignored by default.
    pub fn on_publish_error(
        mut self,
        hook: impl Fn(&FrameworkError) + Send + Sync + 'static,
    ) -> Self {
        self.on_publish_error = Arc::new(hook);
        self
    }

    /// The event store.
    pub fn event_store(&self) -> &EventStore {
        &self.event_store
    }

    /// The bus saved events are published on.
    pub fn event_bus(&self) -> &Arc<dyn EventBus> {
        &self.event_bus
    }

    /// How often a snapshot is taken, in events; 0 for never.
    pub fn snapshot_every(&self) -> u64 {
        self.snapshot_every
    }

    /// The stream an aggregate's events are stored in.
    pub fn stream_id(id: A::Id) -> String {
        StreamIds::for_aggregate(A::TYPE, id)
    }

    /// The latest snapshot, or `None` if there is none or it can't be
    /// loaded.
    async fn latest_snapshot(&self, id: A::Id) -> Option<Snapshot> {
        let store = self.snapshots.as_ref()?;
        match store.load_latest(uuid_of::<A>(id), A::TYPE).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                (self.on_snapshot_error)(&recovered::<A>(error, id, "loadSnapshot"));
                None
            }
        }
    }

    /// Saves a snapshot of `aggregate` at its version, reporting a failure
    /// instead of returning it.
    async fn save_snapshot(&self, aggregate: &A) {
        let Some(store) = &self.snapshots else {
            return;
        };
        let id = aggregate.id();
        let saved = match aggregate.to_snapshot() {
            Ok(snapshot) => {
                let input = SnapshotInput {
                    source_uuid: uuid_of::<A>(id),
                    source_version: snapshot.version,
                    source_type: A::TYPE.to_owned(),
                    data: snapshot.data,
                    metadata: Metadata::new(),
                };
                store.save(input).await
            }
            Err(error) => Err(error),
        };
        if let Err(error) = saved {
            (self.on_snapshot_error)(&recovered::<A>(error, id, "saveSnapshot"));
        }
    }
}

#[async_trait]
impl<A: Aggregate> Repository<A> for EventSourcedRepository<A> {
    async fn load_or_none(&self, id: A::Id) -> Result<Option<A>> {
        let mut aggregate = A::new(id);
        let mut restored = false;
        if let Some(snapshot) = self.latest_snapshot(id).await {
            match aggregate.restore_snapshot(snapshot.data, snapshot.source_version) {
                Ok(()) => restored = true,
                Err(error) => {
                    (self.on_snapshot_error)(&recovered::<A>(error, id, "restoreSnapshot"));
                    aggregate = A::new(id);
                }
            }
        }
        let events = self
            .event_store
            .load_stream(&Self::stream_id(id), aggregate.version(), None)
            .await?;
        if !restored && events.is_empty() {
            return Ok(None);
        }
        aggregate.load_from_history(&events)?;
        adopt_context(&mut aggregate);
        Ok(Some(aggregate))
    }

    async fn save(&self, aggregate: &mut A) -> Result<()> {
        if !aggregate.has_uncommitted_changes() {
            return Ok(());
        }
        let id = aggregate.id();
        let committed = aggregate.committed_version();
        let events = aggregate.uncommitted_changes();
        self.event_store
            .append_to_stream(&Self::stream_id(id), events, committed)
            .await?;
        publish_written::<A>(&*self.event_bus, events, id, &self.on_publish_error).await?;
        mark_saved(aggregate);
        if crosses_multiple(self.snapshot_every, committed, aggregate.version()) {
            self.save_snapshot(aggregate).await;
        }
        Ok(())
    }

    async fn exists(&self, id: A::Id) -> Result<bool> {
        self.event_store.stream_exists(&Self::stream_id(id)).await
    }
}

impl<A> Clone for EventSourcedRepository<A> {
    fn clone(&self) -> Self {
        Self {
            event_store: self.event_store.clone(),
            event_bus: Arc::clone(&self.event_bus),
            snapshots: self.snapshots.clone(),
            snapshot_every: self.snapshot_every,
            on_snapshot_error: Arc::clone(&self.on_snapshot_error),
            on_publish_error: Arc::clone(&self.on_publish_error),
            _aggregate: PhantomData,
        }
    }
}

impl<A: Aggregate> fmt::Debug for EventSourcedRepository<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EventSourcedRepository")
            .field("aggregate_type", &A::TYPE)
            .field("event_bus", &self.event_bus.name())
            .field("snapshots", &self.snapshots.is_some())
            .field("snapshot_every", &self.snapshot_every)
            .finish_non_exhaustive()
    }
}

/// Whether a save from version `from` to `to` crossed a multiple of
/// `every`. Never, when `every` is 0.
fn crosses_multiple(every: u64, from: u64, to: u64) -> bool {
    every > 0 && to / every > from / every
}

fn uuid_of<A: Aggregate>(id: A::Id) -> Uuid {
    let id: AggregateId = id.into();
    id.uuid()
}

#[cfg(test)]
mod tests {
    use super::crosses_multiple;

    #[test]
    fn a_save_snapshots_when_it_crosses_a_multiple() {
        assert!(crosses_multiple(10, 9, 10), "reaches the multiple");
        assert!(crosses_multiple(10, 9, 12), "jumps over it");
        assert!(crosses_multiple(10, 0, 25), "jumps over two");
        assert!(!crosses_multiple(10, 10, 19), "starts at one");
        assert!(!crosses_multiple(10, 3, 9));
        assert!(crosses_multiple(1, 4, 5), "every save");
        assert!(!crosses_multiple(0, 0, 100), "never");
    }
}
