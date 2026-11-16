//! The strong and eventual command pipelines (spec §11.1–11.2).

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use super::concurrency::ConcurrencyChecker;
use super::handler::CommandContext;
use super::registry::{CommandRegistration, RepositoryRegistry};
use super::result::CommandDispatchResult;
use crate::adapters::{CommandIdempotencyStore, ConsistencyMode, Provider, UnitOfWork};
use crate::async_trait;
use crate::bus::EventBus;
use crate::domain::{AnyCommand, DomainEvent, ExecutionContext};
use crate::error::Result;

/// Runs commands in one consistency mode: retry on conflicts, check and mark
/// idempotency, and give the handler a fresh [`CommandContext`] for each
/// attempt.
///
/// **Strong** (§11.1): each attempt runs in a unit of work, so the append,
/// the projections and sagas on the bus, and the idempotency mark commit or
/// roll back together. A provider without units runs the attempt without one.
///
/// **Eventual** (§11.2): no unit of work. The append is atomic on its own and
/// the bus only enqueues; if the command fails after its append, the events
/// stay stored and the command may be dispatched again, so handlers should be
/// safe to repeat.
///
/// A pipeline that starts inside an active unit of work (a saga dispatching
/// from a strong command) joins it and doesn't retry on its own: a conflict
/// goes up to the outer command, which restarts as a whole.
///
/// Idempotency is checked at the start of every attempt, so a retry after a
/// lost race sees a duplicate that another worker has since committed.
/// The mark is written under the name `<Command>Handler`.
#[derive(Clone)]
pub struct CommandPipeline {
    consistency: ConsistencyMode,
    bus: Arc<dyn EventBus>,
    repositories: Arc<RepositoryRegistry>,
    idempotency: Arc<dyn CommandIdempotencyStore>,
    provider: Arc<dyn Provider>,
    checker: ConcurrencyChecker,
}

impl CommandPipeline {
    /// A pipeline in `consistency` mode, whose handlers publish on `bus`.
    ///
    /// The provider supplies the idempotency store and the units of work.
    pub fn new(
        consistency: ConsistencyMode,
        provider: Arc<dyn Provider>,
        bus: Arc<dyn EventBus>,
        repositories: Arc<RepositoryRegistry>,
        checker: ConcurrencyChecker,
    ) -> Self {
        Self {
            consistency,
            idempotency: provider.create_command_idempotency_store(),
            provider,
            bus,
            repositories,
            checker,
        }
    }

    /// The consistency mode this pipeline runs in.
    pub fn consistency(&self) -> ConsistencyMode {
        self.consistency
    }

    /// Runs `command` with the handler of `registration`.
    ///
    /// Returns `Duplicate` if the command was processed before. Errors come
    /// back unchanged, after the retries for conflicts have run out.
    pub async fn dispatch(
        &self,
        registration: &CommandRegistration,
        command: &AnyCommand,
    ) -> Result<CommandDispatchResult> {
        if <dyn UnitOfWork>::active().is_some() {
            return self.attempt(registration, command).await;
        }
        self.checker
            .with_retry(|| self.run(registration, command))
            .await
    }

    /// One attempt, in its own unit of work if the mode and provider have one.
    async fn run(
        &self,
        registration: &CommandRegistration,
        command: &AnyCommand,
    ) -> Result<CommandDispatchResult> {
        let unit = match self.consistency {
            ConsistencyMode::Strong => self.provider.create_unit_of_work(),
            ConsistencyMode::Eventual => None,
        };
        match unit {
            Some(unit) => unit.execute(self.attempt(registration, command)).await,
            None => self.attempt(registration, command).await,
        }
    }

    async fn attempt(
        &self,
        registration: &CommandRegistration,
        command: &AnyCommand,
    ) -> Result<CommandDispatchResult> {
        let id = command.id();
        if self.idempotency.exists(id).await? {
            return Ok(CommandDispatchResult::duplicate(id));
        }
        let recorder = Arc::new(RecordingBus::new(Arc::clone(&self.bus)));
        let context = CommandContext::new(
            self.consistency,
            Arc::clone(&recorder) as Arc<dyn EventBus>,
            Arc::clone(&self.repositories),
        );
        let handler = registration.create_handler(context);
        let name = registration.name();
        ExecutionContext::for_command(command.envelope())
            .scope(async {
                handler.handle(command).await?;
                self.idempotency
                    .mark_processed(id, name, &format!("{name}Handler"))
                    .await
            })
            .await?;
        Ok(CommandDispatchResult::dispatched(id, recorder.take()))
    }
}

impl fmt::Debug for CommandPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandPipeline")
            .field("consistency", &self.consistency)
            .field("bus", &self.bus.name())
            .finish_non_exhaustive()
    }
}

/// Passes events on to the pipeline's bus and remembers them, so the result
/// can list what the handler saved. A new one is made for each attempt, so
/// events of a failed attempt are never reported.
struct RecordingBus {
    inner: Arc<dyn EventBus>,
    events: Mutex<Vec<DomainEvent>>,
}

impl RecordingBus {
    fn new(inner: Arc<dyn EventBus>) -> Self {
        Self {
            inner,
            events: Mutex::new(Vec::new()),
        }
    }

    fn take(&self) -> Vec<DomainEvent> {
        std::mem::take(&mut *self.events.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

#[async_trait]
impl EventBus for RecordingBus {
    fn name(&self) -> &str {
        self.inner.name()
    }

    /// Records the events before publishing: in eventual mode they are
    /// stored even if the publish fails, so they belong in the result.
    async fn publish_all(&self, events: &[DomainEvent]) -> Result<()> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .extend_from_slice(events);
        self.inner.publish_all(events).await
    }
}
