//! Command handlers (spec §4.1) and the context a pipeline gives them.

use std::fmt;
use std::sync::Arc;

use super::registry::RepositoryRegistry;
use crate::adapters::ConsistencyMode;
use crate::async_trait;
use crate::bus::EventBus;
use crate::domain::{Aggregate, Command, CommandMessage};
use crate::error::Result;
use crate::repository::Repository;

/// Handles one type of command (spec §4.1).
///
/// A handler loads the aggregates it needs through its [`CommandContext`],
/// lets them decide, and saves them. It doesn't mark the command processed
/// or retry: the pipeline does both. The pipeline creates a fresh handler for
/// each attempt, from the factory the handler was registered with, so a
/// handler may keep per-attempt state.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use tarodomain::async_trait;
/// use tarodomain::command::CommandHandler;
/// use tarodomain::domain::{Command, CommandMessage};
/// use tarodomain::error::Result;
///
/// #[derive(Clone, Debug, Serialize, Deserialize)]
/// struct Ping;
///
/// impl Command for Ping {
///     const NAME: &'static str = "Ping";
/// }
///
/// struct PingHandler;
///
/// #[async_trait]
/// impl CommandHandler<Ping> for PingHandler {
///     async fn handle(&self, _command: &CommandMessage<Ping>) -> Result<()> {
///         Ok(())
///     }
/// }
/// ```
#[async_trait]
pub trait CommandHandler<C: Command>: Send + Sync {
    /// Runs the command.
    ///
    /// Return an error to reject it. A domain error is final; an
    /// `OPTIMISTIC_CONCURRENCY` error makes the pipeline run the handler
    /// again on fresh state.
    async fn handle(&self, command: &CommandMessage<C>) -> Result<()>;
}

/// What a pipeline hands to the handlers it creates: the consistency mode it
/// runs in, its event bus, and the repositories of the registered aggregates.
///
/// Repositories are built on the pipeline's bus, so the events a handler
/// saves reach the projections and sagas of the right mode (§11.3). Cloning
/// is cheap.
#[derive(Clone)]
pub struct CommandContext {
    consistency: ConsistencyMode,
    bus: Arc<dyn EventBus>,
    repositories: Arc<RepositoryRegistry>,
}

impl CommandContext {
    /// A context for a pipeline in `consistency` mode, publishing on `bus`.
    pub fn new(
        consistency: ConsistencyMode,
        bus: Arc<dyn EventBus>,
        repositories: Arc<RepositoryRegistry>,
    ) -> Self {
        Self {
            consistency,
            bus,
            repositories,
        }
    }

    /// The consistency mode of the pipeline running the handler.
    pub fn consistency(&self) -> ConsistencyMode {
        self.consistency
    }

    /// The event bus the pipeline publishes on.
    pub fn bus(&self) -> &Arc<dyn EventBus> {
        &self.bus
    }

    /// A repository for aggregate type `A`, publishing on this context's bus.
    ///
    /// Fails with `AGGREGATE_NOT_REGISTERED` if `A` wasn't registered.
    pub fn repository<A: Aggregate>(&self) -> Result<Arc<dyn Repository<A>>> {
        self.repositories.create::<A>(Arc::clone(&self.bus))
    }
}

impl fmt::Debug for CommandContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandContext")
            .field("consistency", &self.consistency)
            .field("bus", &self.bus.name())
            .finish_non_exhaustive()
    }
}
