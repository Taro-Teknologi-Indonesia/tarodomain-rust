//! Repositories (spec §6): load an aggregate, let it decide, save what it
//! raised and publish it.
//!
//! [`EventSourcedRepository`] keeps aggregates as their events (§6.2).
//! A state-stored aggregate keeps its state in a table of the application's
//! own, through a [`StateRepository`] the application implements, which
//! [`StateStoredRepository`] connects to an event bus (§6.3, §15.4). Both are
//! a [`Repository`], which is what command handlers use.

mod event_sourced;
mod state;

use std::sync::Arc;

pub use event_sourced::EventSourcedRepository;
pub use state::{StateRepository, StateStoredRepository};

use crate::adapters::UnitOfWork;
use crate::async_trait;
use crate::bus::EventBus;
use crate::domain::{Aggregate, DomainEvent, ExecutionContext};
use crate::error::{FrameworkError, Result};

/// Loads and saves aggregates of one type (spec §6.1).
///
/// Loading gives the aggregate the execution context in scope, if there is
/// one, so the events it raises carry the command's correlation and
/// causation IDs even when raised outside that scope (§5.5). Saving stores
/// the aggregate's uncommitted events (or state), publishes the events on
/// the repository's bus, and marks them committed.
#[async_trait]
pub trait Repository<A: Aggregate>: Send + Sync {
    /// The aggregate with this ID.
    ///
    /// Fails with `AGGREGATE_NOT_FOUND` if it doesn't exist, with the type
    /// and ID as context. Defaults to [`load_or_none`](Self::load_or_none).
    async fn load(&self, id: A::Id) -> Result<A> {
        match self.load_or_none(id).await? {
            Some(aggregate) => Ok(aggregate),
            None => Err(not_found::<A>(id)),
        }
    }

    /// The aggregate with this ID, or `None` if it doesn't exist.
    async fn load_or_none(&self, id: A::Id) -> Result<Option<A>>;

    /// Stores the aggregate's uncommitted changes, publishes its uncommitted
    /// events, and marks them committed. Does nothing if there are none.
    ///
    /// A stale aggregate fails with `OPTIMISTIC_CONCURRENCY` and keeps its
    /// uncommitted events; load it again and retry. A publish failure fails
    /// the save only inside a unit of work, whose rollback undoes the write.
    /// Outside one the write is already durable, so failing would make the
    /// caller retry a change that is already stored: the error goes to the
    /// repository's `on_publish_error` hook instead, and the save succeeds.
    async fn save(&self, aggregate: &mut A) -> Result<()>;

    /// Whether the aggregate exists.
    async fn exists(&self, id: A::Id) -> Result<bool>;
}

/// A callback for an error a repository recovered from, such as a snapshot
/// that couldn't be read. The error's context names the aggregate and the
/// `operation` that failed.
pub type ErrorHook = Arc<dyn Fn(&FrameworkError) + Send + Sync>;

/// The default [`ErrorHook`]: ignores the error. The framework builder
/// routes repository errors to its warning callback instead.
pub(crate) fn ignore_errors() -> ErrorHook {
    Arc::new(|_| {})
}

/// `AGGREGATE_NOT_FOUND` for `id`.
pub(crate) fn not_found<A: Aggregate>(id: A::Id) -> FrameworkError {
    FrameworkError::aggregate_not_found(format!("{} {id} doesn't exist", A::TYPE))
        .with_context("aggregateType", A::TYPE)
        .with_context("aggregateId", id.to_string())
}

/// `error`, with the aggregate and the failed operation as context, for an
/// [`ErrorHook`].
pub(crate) fn recovered<A: Aggregate>(
    error: FrameworkError,
    id: A::Id,
    operation: &'static str,
) -> FrameworkError {
    error
        .with_context("aggregateType", A::TYPE)
        .with_context("aggregateId", id.to_string())
        .with_context("operation", operation)
}

/// Gives a loaded aggregate the execution context in scope, if any.
pub(crate) fn adopt_context<A: Aggregate>(aggregate: &mut A) {
    if let Some(context) = ExecutionContext::current() {
        aggregate.set_execution_context(context);
    }
}

/// Publishes the events of an aggregate whose write just succeeded.
///
/// Inside a unit of work a failure is returned, and the unit's rollback
/// undoes the write. Outside one it goes to `on_error`, and the catch-up
/// dispatcher delivers the events from the store later (§11.2, §15.2).
pub(crate) async fn publish_written<A: Aggregate>(
    bus: &dyn EventBus,
    events: &[DomainEvent],
    id: A::Id,
    on_error: &ErrorHook,
) -> Result<()> {
    match bus.publish_all(events).await {
        Ok(()) => Ok(()),
        Err(error) if <dyn UnitOfWork>::active().is_some() => Err(error),
        Err(error) => {
            on_error(&recovered::<A>(error, id, "publish").with_context("eventBus", bus.name()));
            Ok(())
        }
    }
}

/// Marks a saved aggregate's events committed and drops the execution
/// context it was loaded with.
pub(crate) fn mark_saved<A: Aggregate>(aggregate: &mut A) {
    aggregate.mark_changes_as_committed();
    aggregate.clear_execution_context();
}
