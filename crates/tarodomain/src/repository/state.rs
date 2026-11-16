//! State-stored aggregates (spec §6.3, §15.4): the state lives in a table of
//! the application's own, and the events are only published.

use std::fmt;
use std::sync::Arc;

use super::{ErrorHook, Repository, adopt_context, ignore_errors, mark_saved, publish_written};
use crate::async_trait;
use crate::bus::EventBus;
use crate::domain::Aggregate;
use crate::error::{FrameworkError, Result};

/// Persistence for a state-stored aggregate, implemented by the application
/// (spec §6.3): each aggregate type gets a table of its own, and there is no
/// generic implementation.
///
/// The framework wraps it in a [`StateStoredRepository`], which publishes
/// the aggregate's events and marks them committed after
/// [`save_state`](Self::save_state), and gives loaded aggregates the
/// execution context. One implementation then serves both pipelines' buses.
///
/// [`Aggregate::to_snapshot`] and [`Aggregate::restore_snapshot`] are a
/// ready-made way to turn the state into a row and back, version included.
/// The events of a state-stored aggregate aren't in the event store, so
/// nothing can deliver them again if publishing fails outside a unit of
/// work (see [`Repository::save`]); run commands whose events must reach
/// projections or sagas in strong mode.
#[async_trait]
pub trait StateRepository<A: Aggregate>: Send + Sync {
    /// The aggregate with this ID, at its stored version, or `None` if it
    /// isn't stored.
    ///
    /// Restore the version with [`Aggregate::restore_snapshot`] or
    /// [`Aggregate::restore_version`], so the next save's concurrency check
    /// works.
    async fn load(&self, id: A::Id) -> Result<Option<A>>;

    /// Stores the aggregate's state at [`version`](Aggregate::version), if
    /// the stored version is still its
    /// [`committed_version`](Aggregate::committed_version) (0 for a new
    /// aggregate). Otherwise another writer got there first: fail with
    /// [`FrameworkError::optimistic_concurrency`], so the command is retried.
    ///
    /// Write in the active unit of work, if there is one (§11.4).
    async fn save_state(&self, aggregate: &A) -> Result<()>;

    /// Whether the aggregate is stored. Defaults to [`load`](Self::load);
    /// override it with a cheaper query.
    async fn exists(&self, id: A::Id) -> Result<bool> {
        Ok(self.load(id).await?.is_some())
    }
}

/// The [`Repository`] of a state-stored aggregate (spec §6.3): the
/// application's [`StateRepository`] plus the bus that saved events are
/// published on.
///
/// Save calls [`save_state`](StateRepository::save_state), publishes the
/// uncommitted events, and marks them committed; publish failures follow
/// [`Repository::save`]. Load gives the aggregate the execution context in
/// scope.
///
/// Cloning is cheap: clones share the state repository, bus and hook.
pub struct StateStoredRepository<A> {
    state: Arc<dyn StateRepository<A>>,
    event_bus: Arc<dyn EventBus>,
    on_publish_error: ErrorHook,
}

impl<A: Aggregate> StateStoredRepository<A> {
    /// The repository that stores state through `state` and publishes on
    /// `event_bus`.
    pub fn new(state: Arc<dyn StateRepository<A>>, event_bus: Arc<dyn EventBus>) -> Self {
        Self {
            state,
            event_bus,
            on_publish_error: ignore_errors(),
        }
    }

    /// Called when publishing fails after a durable save, outside a unit of
    /// work (see [`Repository::save`]). The error's `operation` context is
    /// `publish`. Ignored by default.
    pub fn on_publish_error(
        mut self,
        hook: impl Fn(&FrameworkError) + Send + Sync + 'static,
    ) -> Self {
        self.on_publish_error = Arc::new(hook);
        self
    }

    /// The application's state repository.
    pub fn state(&self) -> &Arc<dyn StateRepository<A>> {
        &self.state
    }

    /// The bus saved events are published on.
    pub fn event_bus(&self) -> &Arc<dyn EventBus> {
        &self.event_bus
    }
}

#[async_trait]
impl<A: Aggregate> Repository<A> for StateStoredRepository<A> {
    async fn load_or_none(&self, id: A::Id) -> Result<Option<A>> {
        let mut aggregate = self.state.load(id).await?;
        if let Some(aggregate) = &mut aggregate {
            adopt_context(aggregate);
        }
        Ok(aggregate)
    }

    async fn save(&self, aggregate: &mut A) -> Result<()> {
        if !aggregate.has_uncommitted_changes() {
            return Ok(());
        }
        self.state.save_state(aggregate).await?;
        let (id, events) = (aggregate.id(), aggregate.uncommitted_changes());
        publish_written::<A>(&*self.event_bus, events, id, &self.on_publish_error).await?;
        mark_saved(aggregate);
        Ok(())
    }

    async fn exists(&self, id: A::Id) -> Result<bool> {
        self.state.exists(id).await
    }
}

impl<A> Clone for StateStoredRepository<A> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
            event_bus: Arc::clone(&self.event_bus),
            on_publish_error: Arc::clone(&self.on_publish_error),
        }
    }
}

impl<A: Aggregate> fmt::Debug for StateStoredRepository<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StateStoredRepository")
            .field("aggregate_type", &A::TYPE)
            .field("event_bus", &self.event_bus.name())
            .finish_non_exhaustive()
    }
}
