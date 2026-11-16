//! The built framework (spec §25.2): [`TaroDomain`].

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::TimeDelta;

use super::builder::{ProjectionPrepared, SagaWiring, TaroDomainBuilder, TimeoutJob};
use super::validation::{
    DispatchGuard, FrameworkValidator, ProjectionMode, ValidationInput, ValidationReport,
};
use crate::adapters::{
    CommandIdempotencyStore, ConsistencyMode, DeadLetterInput, DeadLetterStore, EventBusConfig,
    ProcessedEventStore, Provider,
};
use crate::async_trait;
use crate::bus::{EventBus, EventHandler};
use crate::catch_up::{CatchUpResult, CronDispatcher, SubscriptionManager};
use crate::command::{
    BatchDispatchResult, CommandDispatchResult, CommandDispatcher, CommandPipeline,
    CommandRegistry, ConcurrencyChecker, RepositoryRegistry,
};
use crate::dead_letter::RetryPolicy;
use crate::di::{Container, Token};
use crate::domain::{AnyCommand, Command, DomainEvent, SerializedEvent};
use crate::error::{FrameworkError, Result};
use crate::event_sourcing::{EventRegistry, EventStore};
use crate::framework::aggregate_config::RepoWiring;
use crate::kernel::clock::{self, Clock};
use crate::kernel::ids::Uuid;
use crate::saga::{SagaCommandDispatch, TimeoutRun};

/// The token under which the built framework is bound in its own container.
pub const TARO_DOMAIN: Token<TaroDomain> = Token::new("taroDomain");

/// The token under which the provider is bound in the container.
pub const PROVIDER: Token<dyn Provider> = Token::new("provider");

/// How long the cleanup job keeps processed commands (§14).
pub const COMMAND_RETENTION: Duration = Duration::from_secs(7 * 24 * 3600);

/// How long the cleanup job keeps processed-event marks and dead letters
/// (§14, §21).
pub const EVENT_RETENTION: Duration = Duration::from_secs(30 * 24 * 3600);

/// The name dead letters of the queue consumer are filed under.
pub const QUEUE_CONSUMER: &str = "event-queue";

/// One message of the event queue: the event and how often it has been
/// delivered, counting this delivery from 1.
#[derive(Clone, Debug)]
pub struct QueueMessage {
    /// The event as it was enqueued.
    pub event: SerializedEvent,
    /// The delivery attempt, from 1. Sets the retry delay.
    pub attempts: u32,
}

impl From<SerializedEvent> for QueueMessage {
    fn from(event: SerializedEvent) -> Self {
        Self { event, attempts: 1 }
    }
}

/// What the queue should do with a message (§10.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageOutcome {
    /// Every handler took it: acknowledge.
    Ack,
    /// A retryable failure: redeliver after `delay`.
    Retry {
        /// How long to wait.
        delay: Duration,
    },
    /// A final failure, recorded as a dead letter: acknowledge.
    DeadLettered,
}

/// The result for one queue message.
#[derive(Debug)]
pub struct MessageResult {
    /// The event's ID.
    pub event_id: Uuid,
    /// What to do with the message.
    pub outcome: MessageOutcome,
    /// Why it failed, if it did.
    pub error: Option<FrameworkError>,
}

/// What one scheduled run did (§25.2).
#[derive(Debug, Default)]
pub struct ScheduledRun {
    /// The catch-up result of each handler.
    pub catch_up: Vec<(String, Result<CatchUpResult>)>,
    /// The timeout job result of each saga type with a timeout.
    pub timeouts: Vec<(&'static str, Result<TimeoutRun>)>,
}

impl ScheduledRun {
    /// Whether any catch-up or timeout job failed, or a handler is stuck.
    pub fn has_errors(&self) -> bool {
        self.catch_up
            .iter()
            .any(|(_, r)| r.as_ref().map_or(true, |c| c.failed.is_some()))
            || self.timeouts.iter().any(|(_, r)| {
                r.as_ref()
                    .map_or(true, |t| t.failed > 0 || !t.retry_later.is_empty())
            })
    }
}

/// What the cleanup job deleted.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CleanupReport {
    /// Processed command records.
    pub commands: u64,
    /// Processed event records.
    pub events: u64,
    /// Dead letters.
    pub dead_letters: u64,
}

struct Inner {
    provider: Arc<dyn Provider>,
    clock: Arc<dyn Clock>,
    di: Container,
    event_store: EventStore,
    strong_bus: Arc<dyn EventBus>,
    eventual_bus: Arc<dyn EventBus>,
    default_consistency: ConsistencyMode,
    commands: Arc<CommandRegistry>,
    dispatcher: Arc<CommandDispatcher>,
    cron: Arc<CronDispatcher>,
    timeouts: Vec<Arc<dyn TimeoutJob>>,
    guard: DispatchGuard,
    retry: RetryPolicy,
    processed: Arc<dyn ProcessedEventStore>,
    idempotency: Arc<dyn CommandIdempotencyStore>,
    dead_letters: Arc<dyn DeadLetterStore>,
}

/// The built framework: dispatches commands, consumes the event queue, and
/// runs the scheduled jobs (§25.2). Cloning is cheap.
///
/// Create it with [`TaroDomain::builder`]. The HTTP API and the platform
/// entry points ([`HttpApi`](super::HttpApi), the Cloudflare worker, the axum
/// server) are thin layers over the methods here.
#[derive(Clone)]
pub struct TaroDomain {
    inner: Arc<Inner>,
}

impl fmt::Debug for TaroDomain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaroDomain")
            .field("provider", &self.inner.provider.provider_type())
            .field("commands", &self.inner.commands)
            .finish_non_exhaustive()
    }
}

impl TaroDomain {
    /// An empty [`TaroDomainBuilder`].
    pub fn builder() -> TaroDomainBuilder {
        TaroDomainBuilder::new()
    }

    /// Runs `command` in the pipeline of its consistency mode (§11.3).
    ///
    /// Any command, or [`CommandMessage`](crate::domain::CommandMessage),
    /// converts into an [`AnyCommand`]. Fails with
    /// `COMMAND_HANDLER_NOT_REGISTERED` if nothing handles it.
    pub async fn dispatch(&self, command: impl Into<AnyCommand>) -> Result<CommandDispatchResult> {
        let command = command.into();
        self.in_clock(async {
            self.inner.guard.validate_command(command.name())?;
            self.inner.dispatcher.dispatch(command).await
        })
        .await
    }

    /// Runs `commands` one after another. See
    /// [`CommandDispatcher::dispatch_all`].
    pub async fn dispatch_all(
        &self,
        commands: impl IntoIterator<Item = AnyCommand>,
        continue_on_error: bool,
    ) -> BatchDispatchResult {
        self.in_clock(
            self.inner
                .dispatcher
                .dispatch_all(commands, continue_on_error),
        )
        .await
    }

    /// The queue consumer (§10.5): gives each event to the projections and
    /// sagas, and says what the queue should do with its message.
    ///
    /// Messages are handled in order. A handler error that is retryable
    /// (a conflict or an infrastructure error) asks for a retry with the
    /// retry policy's backoff for the delivery count. Any other error,
    /// including an event that can't be read, is written to the dead letter
    /// store and the message is acknowledged; if even that write fails, the
    /// message is retried.
    pub async fn handle_events<M: Into<QueueMessage>>(
        &self,
        messages: impl IntoIterator<Item = M>,
    ) -> Vec<MessageResult> {
        let mut results = Vec::new();
        for message in messages {
            let message = message.into();
            results.push(self.in_clock(self.handle_one(message)).await);
        }
        results
    }

    async fn handle_one(&self, message: QueueMessage) -> MessageResult {
        let event_id = message.event.event_id;
        let delivered = async {
            let event = self.inner.event_store.deserialize(&message.event)?;
            self.inner.guard.validate_event(&event)?;
            self.inner.strong_bus.publish(&event).await
        }
        .await;
        let Err(error) = delivered else {
            return MessageResult {
                event_id,
                outcome: MessageOutcome::Ack,
                error: None,
            };
        };
        let retry = MessageOutcome::Retry {
            delay: self
                .inner
                .retry
                .compute_delay(message.attempts.saturating_sub(1)),
        };
        let outcome = if error.retryable() {
            retry
        } else {
            let letter = DeadLetterInput {
                handler_name: QUEUE_CONSUMER.to_owned(),
                event_id,
                event_type: message.event.event_type.clone(),
                payload: message.event.data.clone(),
                error_code: error.code().as_str().to_owned(),
                error_message: error.message().to_owned(),
                attempts: message.attempts,
            };
            match self.inner.dead_letters.write(letter).await {
                Ok(_) => MessageOutcome::DeadLettered,
                Err(_) => retry,
            }
        };
        MessageResult {
            event_id,
            outcome,
            error: Some(error),
        }
    }

    /// The scheduled job (§25.2): catches every handler up from the event
    /// store, then runs the saga timeouts.
    pub async fn run_scheduled(&self) -> ScheduledRun {
        self.in_clock(async {
            let catch_up = self.inner.cron.run_all().await;
            let timeouts = self.timeouts().await;
            ScheduledRun { catch_up, timeouts }
        })
        .await
    }

    /// Only the saga timeouts of [`run_scheduled`](Self::run_scheduled): for
    /// each saga type with a timeout, acts on the sagas that have stood still
    /// for that long (§19). For a host that schedules the two jobs apart.
    pub async fn run_timeouts(&self) -> Vec<(&'static str, Result<TimeoutRun>)> {
        self.in_clock(self.timeouts()).await
    }

    async fn timeouts(&self) -> Vec<(&'static str, Result<TimeoutRun>)> {
        let mut timeouts = Vec::new();
        for job in &self.inner.timeouts {
            timeouts.push((job.saga_type(), job.run().await));
        }
        timeouts
    }

    /// The cleanup job: deletes processed commands older than
    /// [`COMMAND_RETENTION`], and processed events and dead letters older
    /// than [`EVENT_RETENTION`] (§14, §21).
    pub async fn run_cleanup(&self) -> Result<CleanupReport> {
        let now = self.inner.clock.now();
        let before = |age: Duration| now - TimeDelta::from_std(age).unwrap_or(TimeDelta::MAX);
        Ok(CleanupReport {
            commands: self
                .inner
                .idempotency
                .delete_old(before(COMMAND_RETENTION))
                .await?,
            events: self
                .inner
                .processed
                .delete_old(before(EVENT_RETENTION))
                .await?,
            dead_letters: self
                .inner
                .dead_letters
                .delete_old(before(EVENT_RETENTION))
                .await?,
        })
    }

    /// Runs the provider's migrations again (`Provider::initialize`). Safe to
    /// repeat.
    pub async fn migrate(&self) -> Result<()> {
        self.inner.provider.initialize().await
    }

    /// The consistency mode commands of type `C` run in (§25.2).
    ///
    /// Fails with `COMMAND_HANDLER_NOT_REGISTERED` if `C` has no handler.
    pub fn consistency_for<C: Command>(&self) -> Result<ConsistencyMode> {
        self.consistency_for_name(C::NAME)
    }

    /// Like [`consistency_for`](Self::consistency_for), by command name.
    pub fn consistency_for_name(&self, name: &str) -> Result<ConsistencyMode> {
        let registration = self.inner.commands.resolve(name)?;
        Ok(registration
            .consistency()
            .unwrap_or(self.inner.default_consistency))
    }

    /// The dependency container, with the framework ([`TARO_DOMAIN`]) and
    /// the provider ([`PROVIDER`]) bound.
    pub fn di(&self) -> &Container {
        &self.inner.di
    }

    /// The provider the framework was built on.
    pub fn provider(&self) -> &Arc<dyn Provider> {
        &self.inner.provider
    }

    /// The event store.
    pub fn event_store(&self) -> &EventStore {
        &self.inner.event_store
    }

    /// The bus of the default consistency mode.
    pub fn event_bus(&self) -> &Arc<dyn EventBus> {
        match self.inner.default_consistency {
            ConsistencyMode::Strong => &self.inner.strong_bus,
            ConsistencyMode::Eventual => &self.inner.eventual_bus,
        }
    }

    /// The bus the projections and sagas listen on.
    pub fn strong_event_bus(&self) -> &Arc<dyn EventBus> {
        &self.inner.strong_bus
    }

    /// The bus eventual commands publish on.
    pub fn eventual_event_bus(&self) -> &Arc<dyn EventBus> {
        &self.inner.eventual_bus
    }

    /// The command registry.
    pub fn commands(&self) -> &Arc<CommandRegistry> {
        &self.inner.commands
    }

    /// The command dispatcher.
    pub fn dispatcher(&self) -> &Arc<CommandDispatcher> {
        &self.inner.dispatcher
    }

    /// The catch-up dispatcher.
    pub fn cron(&self) -> &Arc<CronDispatcher> {
        &self.inner.cron
    }

    /// The subscription cursors.
    pub fn subscriptions(&self) -> &Arc<SubscriptionManager> {
        self.inner.cron.subscriptions()
    }

    /// The runtime validation guard.
    pub fn guard(&self) -> &DispatchGuard {
        &self.inner.guard
    }

    async fn in_clock<T>(&self, work: impl std::future::Future<Output = T>) -> T {
        clock::scope(self.inner.clock.clone(), work).await
    }
}

/// Applies the dispatch guard's projection mode before a projection.
struct GuardedHandler {
    inner: Arc<dyn EventHandler>,
    guard: DispatchGuard,
    handled: Vec<&'static str>,
}

#[async_trait]
impl EventHandler for GuardedHandler {
    fn name(&self) -> &str {
        self.inner.name()
    }

    async fn handle(&self, event: &DomainEvent) -> Result<()> {
        self.guard
            .validate_projection_dispatch(self.inner.name(), &self.handled, event)?;
        self.inner.handle(event).await
    }
}

fn configuration(message: impl Into<String>) -> FrameworkError {
    FrameworkError::invalid_configuration(message)
}

impl TaroDomainBuilder {
    /// Validates the configuration (§29.1) and wires everything: the
    /// provider is initialized, and the event store, two buses, two
    /// repositories per aggregate, two pipelines, the dispatcher, the
    /// projections and sagas (on the strong bus), the catch-up dispatcher and
    /// the saga timeout jobs are created.
    ///
    /// Fails with `INVALID_CONFIGURATION`, listing every problem, if the
    /// configuration is wrong; if no provider was given; or if a command asks
    /// for strong consistency and the provider has no unit of work.
    pub async fn build(self) -> Result<TaroDomain> {
        let Self {
            provider,
            consistency,
            transport,
            snapshot_every,
            aggregates,
            events,
            upcasters,
            commands,
            projections,
            sagas,
            container,
            projection_mode,
            skip_validation,
            on_warning,
            concurrency,
            clock,
            sleeper,
            catch_up_batch_size,
            catch_up_max_attempts,
        } = self;
        let provider = provider.ok_or_else(|| configuration("no provider: call use_provider"))?;
        let clock = clock.unwrap_or_else(|| provider.clock());
        let sleeper = sleeper.unwrap_or_else(|| provider.sleeper());

        // Instances first: validation needs the projections' names and routes.
        let processed = provider.create_processed_event_store();
        let prepared: Vec<ProjectionPrepared> = projections
            .into_iter()
            .map(|prepare| prepare(&container, processed.clone()))
            .collect::<Result<_>>()?;

        let mut input = ValidationInput {
            aggregates: aggregates.iter().map(|a| a.info.clone()).collect(),
            commands: commands.iter().map(|(name, _)| *name).collect(),
            projections: prepared.iter().map(|p| p.info.clone()).collect(),
            sagas: sagas.iter().map(|(_, s)| s.info.clone()).collect(),
            events: Vec::new(),
        };
        for aggregate in &aggregates {
            input.events.extend_from_slice(aggregate.events);
        }
        for (names, _) in &events {
            input.events.extend_from_slice(names);
        }
        let mut report = if skip_validation {
            ValidationReport::default()
        } else {
            FrameworkValidator::validate(&input)
        };
        report
            .warnings
            .extend(aggregates.iter().flat_map(|a| a.warnings.iter().cloned()));
        report.finish(&on_warning)?;

        provider.initialize().await?;

        // Events.
        let mut event_registry = EventRegistry::new();
        for aggregate in &aggregates {
            (aggregate.register_events)(&mut event_registry)?;
        }
        for (_, register) in events {
            register(&mut event_registry)?;
        }
        for register in upcasters {
            register(&mut event_registry)?;
        }
        let event_registry = Arc::new(event_registry);
        let adapter = provider.create_event_store_adapter();
        let event_store = EventStore::new(adapter.clone(), event_registry.clone());

        // Buses and repositories.
        let strong_bus =
            provider.create_event_bus(&EventBusConfig::new(ConsistencyMode::Strong))?;
        let eventual_config = EventBusConfig {
            consistency: ConsistencyMode::Eventual,
            transport,
        };
        let eventual_bus = provider.create_event_bus(&eventual_config)?;
        let wiring = RepoWiring {
            store: event_store.clone(),
            snapshots: provider.create_snapshot_store_adapter(),
            snapshot_every,
            warn: on_warning.clone(),
            container: container.clone(),
        };
        let mut repositories = RepositoryRegistry::new();
        for aggregate in aggregates {
            (aggregate.install)(&mut repositories, &wiring)?;
        }
        let repositories = Arc::new(repositories);

        // Commands and pipelines.
        let mut command_registry = CommandRegistry::new();
        for (_, register) in commands {
            register(&mut command_registry, &container)?;
        }
        let needs_unit = command_registry.names().any(|name| {
            command_registry
                .get(name)
                .and_then(|r| r.consistency())
                .unwrap_or(consistency)
                == ConsistencyMode::Strong
        });
        if needs_unit && provider.create_unit_of_work().is_none() {
            return Err(configuration(format!(
                "the {} provider has no unit of work, so commands can't run in strong mode",
                provider.provider_type()
            )));
        }
        let command_registry = Arc::new(command_registry);
        let checker = concurrency.unwrap_or_else(|| ConcurrencyChecker::new(sleeper.clone()));
        let pipeline = |mode, bus: &Arc<dyn EventBus>| {
            CommandPipeline::new(
                mode,
                provider.clone(),
                bus.clone(),
                repositories.clone(),
                checker.clone(),
            )
        };
        let dispatcher = Arc::new(CommandDispatcher::new(
            command_registry.clone(),
            pipeline(ConsistencyMode::Strong, &strong_bus),
            pipeline(ConsistencyMode::Eventual, &eventual_bus),
            consistency,
        ));

        // Catch-up, projections and sagas.
        let dead_letters = provider.create_dead_letter_store();
        let mut cron = CronDispatcher::new(
            event_store.clone(),
            Arc::new(SubscriptionManager::new(adapter)),
            dead_letters.clone(),
        )
        .with_units_of_work(provider.clone());
        if let Some(size) = catch_up_batch_size {
            cron = cron.with_batch_size(size);
        }
        if let Some(attempts) = catch_up_max_attempts {
            cron = cron.with_max_attempts(attempts);
        }
        let cron = Arc::new(cron);
        let guard = DispatchGuard::new(
            command_registry.clone(),
            event_registry,
            projection_mode,
            on_warning.clone(),
        );
        let subscribable = strong_bus.as_subscribable().ok_or_else(|| {
            configuration("the provider's strong event bus can't be subscribed to")
        })?;
        let wire = |handler: Arc<dyn EventHandler>| -> Result<()> {
            subscribable.subscribe(handler.clone())?;
            cron.register(handler, None)
        };
        for projection in prepared {
            let handler = if projection_mode == ProjectionMode::Skip {
                projection.handler
            } else {
                Arc::new(GuardedHandler {
                    inner: projection.handler,
                    guard: guard.clone(),
                    handled: projection.info.events,
                })
            };
            wire(handler)?;
        }
        let mut timeouts = Vec::new();
        for (_, saga) in sagas {
            let finished = (saga.finish)(SagaWiring {
                store: provider.create_saga_store_adapter(),
                processed: processed.clone(),
                dispatch: SagaCommandDispatch::to_dispatcher(dispatcher.clone()),
                clock: clock.clone(),
            })?;
            wire(finished.handler)?;
            timeouts.extend(finished.timeout);
        }

        let retry = RetryPolicy::new(sleeper);
        let di = container;
        di.bind_instance(PROVIDER, provider.clone());
        let domain = TaroDomain {
            inner: Arc::new(Inner {
                idempotency: provider.create_command_idempotency_store(),
                provider,
                clock,
                di: di.clone(),
                event_store,
                strong_bus,
                eventual_bus,
                default_consistency: consistency,
                commands: command_registry,
                dispatcher,
                cron,
                timeouts,
                guard,
                retry,
                processed,
                dead_letters,
            }),
        };
        di.bind_instance(TARO_DOMAIN, Arc::new(domain.clone()));
        Ok(domain)
    }
}
