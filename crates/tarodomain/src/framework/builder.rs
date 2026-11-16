//! The builder DSL (spec §25.1).
//!
//! [`TaroDomain::builder()`](super::TaroDomain::builder) returns a
//! [`TaroDomainBuilder`]. Every method takes and returns the builder, so a
//! whole application reads as one chain, and nothing runs until
//! [`build`](TaroDomainBuilder::build).

use std::sync::Arc;
use std::time::Duration;

use super::aggregate_config::{AggregateConfig, AggregateDecl};
use super::validation::{
    ProjectionInfo, ProjectionMode, SagaInfo, WarningHook, default_warning_hook,
};
use crate::adapters::{ConsistencyMode, ProcessedEventStore, Provider, SagaStoreAdapter};
use crate::async_trait;
use crate::bus::EventHandler;
use crate::command::{CommandContext, CommandHandler, CommandRegistry, ConcurrencyChecker};
use crate::di::{Container, Lifetime, Token};
use crate::domain::{Aggregate, Command, Event, EventFamily};
use crate::error::Result;
use crate::event_sourcing::EventRegistry;
use crate::kernel::backoff::Sleeper;
use crate::kernel::clock::Clock;
use crate::projection::{Projection, ProjectionHandler};
use crate::saga::{
    HandlerKind, ProcessManager, SagaCommandDispatch, SagaEventHandler, SagaRoutes, SagaTimeout,
    SagaTimeoutScheduler, TimeoutRun,
};

/// Per-saga settings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SagaOptions {
    /// How long a saga may sit without an update before the timeout job acts
    /// on it (§19). Must be positive. `None` means no timeout.
    pub timeout: Option<Duration>,
}

impl SagaOptions {
    /// Options with a timeout.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
        }
    }
}

/// One saga's scheduled timeout job, with its type erased.
#[async_trait]
pub(crate) trait TimeoutJob: Send + Sync {
    fn saga_type(&self) -> &'static str;
    async fn run(&self) -> Result<TimeoutRun>;
}

#[async_trait]
impl<P: ProcessManager> TimeoutJob for SagaTimeoutScheduler<P> {
    fn saga_type(&self) -> &'static str {
        P::SAGA_TYPE
    }

    async fn run(&self) -> Result<TimeoutRun> {
        SagaTimeoutScheduler::run(self).await
    }
}

/// What a saga declaration needs from the framework to finish itself.
pub(crate) struct SagaWiring {
    pub store: Arc<dyn SagaStoreAdapter>,
    pub processed: Arc<dyn ProcessedEventStore>,
    pub dispatch: SagaCommandDispatch,
    pub clock: Arc<dyn Clock>,
}

pub(crate) struct SagaFinished {
    pub handler: Arc<dyn EventHandler>,
    pub timeout: Option<Arc<dyn TimeoutJob>>,
}

/// A saga whose routes are known but whose handler isn't built yet.
pub(crate) struct SagaDecl {
    pub info: SagaInfo,
    pub finish: Box<dyn FnOnce(SagaWiring) -> Result<SagaFinished> + Send>,
}

/// A projection whose instance exists but isn't wired yet.
pub(crate) struct ProjectionPrepared {
    pub info: ProjectionInfo,
    pub handler: Arc<dyn EventHandler>,
}

type ProjectionDecl =
    Box<dyn FnOnce(&Container, Arc<dyn ProcessedEventStore>) -> Result<ProjectionPrepared> + Send>;
type EventDecl = Box<dyn FnOnce(&mut EventRegistry) -> Result<()> + Send>;
type CommandDecl = Box<dyn FnOnce(&mut CommandRegistry, &Container) -> Result<()> + Send>;

/// Collects the application's registrations and settings.
///
/// ```text
/// let app = TaroDomain::builder()
///     .use_provider(Arc::new(LocalProvider::new()))
///     .consistency(ConsistencyMode::Strong, None)
///     .aggregate::<BankAccount>(|a| a.event_sourced().snapshot_every(100))
///     .command::<OpenAccount, _>(|_c, ctx| OpenAccountHandler { ctx }, None)
///     .projection(|c| Ok(Arc::new(BalanceProjection::new(c.resolve(STORE)?))))
///     .saga::<TransferSaga>(SagaOptions::with_timeout(Duration::from_secs(300)))
///     .build()
///     .await?;
/// ```
pub struct TaroDomainBuilder {
    pub(crate) provider: Option<Arc<dyn Provider>>,
    pub(crate) consistency: ConsistencyMode,
    pub(crate) transport: Option<String>,
    pub(crate) snapshot_every: Option<u64>,
    pub(crate) aggregates: Vec<AggregateDecl>,
    /// The event names each registration declares, for validation, and how to register them.
    pub(crate) events: Vec<(Vec<&'static str>, EventDecl)>,
    pub(crate) upcasters: Vec<EventDecl>,
    pub(crate) commands: Vec<(&'static str, CommandDecl)>,
    pub(crate) projections: Vec<ProjectionDecl>,
    pub(crate) sagas: Vec<(SagaOptions, SagaDecl)>,
    pub(crate) container: Container,
    pub(crate) projection_mode: ProjectionMode,
    pub(crate) skip_validation: bool,
    pub(crate) on_warning: WarningHook,
    pub(crate) concurrency: Option<ConcurrencyChecker>,
    pub(crate) clock: Option<Arc<dyn Clock>>,
    pub(crate) sleeper: Option<Arc<dyn Sleeper>>,
    pub(crate) catch_up_batch_size: Option<usize>,
    pub(crate) catch_up_max_attempts: Option<u32>,
}

impl Default for TaroDomainBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl TaroDomainBuilder {
    /// An empty builder: strong consistency by default, no provider yet.
    pub fn new() -> Self {
        Self {
            provider: None,
            consistency: ConsistencyMode::Strong,
            transport: None,
            snapshot_every: None,
            aggregates: Vec::new(),
            events: Vec::new(),
            upcasters: Vec::new(),
            commands: Vec::new(),
            projections: Vec::new(),
            sagas: Vec::new(),
            container: Container::new(),
            projection_mode: ProjectionMode::default(),
            skip_validation: false,
            on_warning: default_warning_hook(),
            concurrency: None,
            clock: None,
            sleeper: None,
            catch_up_batch_size: None,
            catch_up_max_attempts: None,
        }
    }

    /// The provider that supplies the stores, buses and units of work.
    #[must_use]
    pub fn use_provider(mut self, provider: Arc<dyn Provider>) -> Self {
        self.provider = Some(provider);
        self
    }

    /// The consistency mode of commands that don't ask for one, and the
    /// event bus transport for the eventual bus (§25.1). Without this
    /// call, commands run in strong mode.
    #[must_use]
    pub fn consistency(mut self, mode: ConsistencyMode, transport: Option<&str>) -> Self {
        self.consistency = mode;
        self.transport = transport.map(str::to_owned);
        self
    }

    /// Takes a snapshot of every event-sourced aggregate whenever a save
    /// crosses a multiple of `every`. A value set on an aggregate wins.
    #[must_use]
    pub fn snapshot_every(mut self, every: u64) -> Self {
        self.snapshot_every = Some(every);
        self
    }

    /// Registers an aggregate type and its event family. See
    /// [`AggregateConfig`].
    #[must_use]
    pub fn aggregate<A: Aggregate>(
        mut self,
        configure: impl FnOnce(AggregateConfig<A>) -> AggregateConfig<A>,
    ) -> Self {
        self.aggregates
            .push(configure(AggregateConfig::new()).into_decl());
        self
    }

    /// Registers an event type that no aggregate's family declares, such as
    /// one only a saga raises.
    #[must_use]
    pub fn event<E: Event>(mut self) -> Self {
        self.events
            .push((vec![E::NAME], Box::new(|registry| registry.register::<E>())));
        self
    }

    /// Registers every event of a family that isn't an aggregate's own.
    #[must_use]
    pub fn events<F: EventFamily>(mut self) -> Self {
        self.events
            .push((F::names().to_vec(), Box::new(F::register_all)));
        self
    }

    /// Registers an upcaster from `from` to `to` for `event_type` (§3.4).
    #[must_use]
    pub fn upcaster(
        mut self,
        event_type: &str,
        from: &str,
        to: &str,
        upcast: impl Fn(serde_json::Value) -> Result<serde_json::Value> + Send + Sync + 'static,
    ) -> Self {
        let (event_type, from, to) = (event_type.to_owned(), from.to_owned(), to.to_owned());
        self.upcasters.push(Box::new(move |registry| {
            registry.register_upcaster(event_type, from, to, upcast)
        }));
        self
    }

    /// Registers the handler of command `C`. `factory` builds a handler for
    /// each attempt from the container and the pipeline's context. `mode`
    /// overrides the default consistency for this command (§11.3).
    #[must_use]
    pub fn command<C, H>(
        mut self,
        factory: impl Fn(&Container, CommandContext) -> H + Send + Sync + 'static,
        mode: Option<ConsistencyMode>,
    ) -> Self
    where
        C: Command + Clone,
        H: CommandHandler<C> + 'static,
    {
        self.commands.push((
            C::NAME,
            Box::new(move |registry, container| {
                let container = container.clone();
                registry.register_with::<C, H>(mode, move |context| factory(&container, context))
            }),
        ));
        self
    }

    /// Registers a projection. `factory` builds it from the container, once,
    /// at build time. It listens on the strong event bus, and the catch-up
    /// dispatcher delivers what the eventual bus missed.
    #[must_use]
    pub fn projection<P: Projection>(
        mut self,
        factory: impl FnOnce(&Container) -> Result<Arc<P>> + Send + 'static,
    ) -> Self {
        self.projections.push(Box::new(move |container, processed| {
            let projection = factory(container)?;
            let handler = ProjectionHandler::new(projection, processed)?;
            let info = ProjectionInfo {
                name: handler.record_name().to_owned(),
                events: handler.handled_events(),
            };
            Ok(ProjectionPrepared {
                info,
                handler: Arc::new(handler),
            })
        }));
        self
    }

    /// Registers a saga type.
    ///
    /// A saga whose routes are invalid (two handlers for one event, a
    /// missing compensation) is reported by [`build`](Self::build).
    #[must_use]
    pub fn saga<P: ProcessManager>(mut self, options: SagaOptions) -> Self {
        let decl = match SagaRoutes::<P>::build() {
            Ok(routes) => {
                let info = SagaInfo {
                    saga_type: P::SAGA_TYPE,
                    events: routes.event_names(),
                    starts: routes
                        .event_names()
                        .iter()
                        .filter(|e| routes.starts_on(e))
                        .count(),
                    completions: routes
                        .event_names()
                        .iter()
                        .filter(|e| routes.handles(HandlerKind::CompletedWhen, e))
                        .count(),
                    handles_timeout: routes
                        .handles(HandlerKind::CompensateWhen, <SagaTimeout as Event>::NAME)
                        || routes.handles(HandlerKind::ContinueWhen, <SagaTimeout as Event>::NAME),
                    timeout: options.timeout,
                };
                SagaDecl {
                    info,
                    finish: Box::new(move |wiring| {
                        let handler = Arc::new(SagaEventHandler::<P>::new(
                            wiring.store.clone(),
                            wiring.processed,
                            wiring.dispatch,
                        )?);
                        let timeout = options.timeout.map(|timeout| {
                            Arc::new(SagaTimeoutScheduler::new(
                                wiring.store,
                                handler.clone(),
                                wiring.clock,
                                timeout,
                            )) as Arc<dyn TimeoutJob>
                        });
                        Ok(SagaFinished { handler, timeout })
                    }),
                }
            }
            Err(error) => SagaDecl {
                info: SagaInfo {
                    saga_type: P::SAGA_TYPE,
                    events: Vec::new(),
                    starts: 1,
                    completions: 1,
                    handles_timeout: false,
                    timeout: options.timeout,
                },
                finish: Box::new(move |_| Err(error)),
            },
        };
        self.sagas.push((options, decl));
        self
    }

    /// The container the application's dependencies are bound in.
    pub fn container(&self) -> &Container {
        &self.container
    }

    /// Binds `token` to `factory` (§23.2).
    #[must_use]
    pub fn bind<T, F>(self, token: Token<T>, factory: F, lifetime: Lifetime) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: Fn(&Container) -> Result<Arc<T>> + Send + Sync + 'static,
    {
        self.container.bind(token, factory, lifetime);
        self
    }

    /// Binds `token` as a singleton.
    #[must_use]
    pub fn bind_singleton<T, F>(self, token: Token<T>, factory: F) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: Fn(&Container) -> Result<Arc<T>> + Send + Sync + 'static,
    {
        self.bind(token, factory, Lifetime::Singleton)
    }

    /// Binds `token` as transient.
    #[must_use]
    pub fn bind_transient<T, F>(self, token: Token<T>, factory: F) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: Fn(&Container) -> Result<Arc<T>> + Send + Sync + 'static,
    {
        self.bind(token, factory, Lifetime::Transient)
    }

    /// Binds `token` as scoped.
    #[must_use]
    pub fn bind_scoped<T, F>(self, token: Token<T>, factory: F) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: Fn(&Container) -> Result<Arc<T>> + Send + Sync + 'static,
    {
        self.bind(token, factory, Lifetime::Scoped)
    }

    /// Binds an existing value.
    #[must_use]
    pub fn bind_instance<T>(self, token: Token<T>, instance: Arc<T>) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.container.bind_instance(token, instance);
        self
    }

    /// Sets what the dispatch guard does when a projection is given an event
    /// it has no handler for (§29.2).
    #[must_use]
    pub fn validation(mut self, projection_mode: ProjectionMode) -> Self {
        self.projection_mode = projection_mode;
        self
    }

    /// Skips the startup validation (§29.1). Problems then show up as
    /// failures when the framework runs.
    #[must_use]
    pub fn skip_validation(mut self) -> Self {
        self.skip_validation = true;
        self
    }

    /// Where warnings go. Defaults to `tracing`. Repository errors that the
    /// framework recovers from, such as a failed snapshot, come here too.
    #[must_use]
    pub fn on_warning(mut self, hook: impl Fn(&super::Warning) + Send + Sync + 'static) -> Self {
        self.on_warning = Arc::new(hook);
        self
    }

    /// The retry policy for optimistic concurrency conflicts. Defaults to
    /// [`ConcurrencyChecker::new`] with the provider's sleeper.
    #[must_use]
    pub fn concurrency(mut self, checker: ConcurrencyChecker) -> Self {
        self.concurrency = Some(checker);
        self
    }

    /// The clock for timestamps. Overrides the provider's.
    #[must_use]
    pub fn clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// How to wait between retries. Overrides the provider's.
    #[must_use]
    pub fn sleeper(mut self, sleeper: Arc<dyn Sleeper>) -> Self {
        self.sleeper = Some(sleeper);
        self
    }

    /// How many events one catch-up run reads per handler (default 100).
    #[must_use]
    pub fn catch_up_batch_size(mut self, size: usize) -> Self {
        self.catch_up_batch_size = Some(size);
        self
    }

    /// Dead-letters an event after a catch-up handler has failed it `n`
    /// times. Without this, catch-up retries a failing event forever.
    #[must_use]
    pub fn catch_up_max_attempts(mut self, attempts: u32) -> Self {
        self.catch_up_max_attempts = Some(attempts);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_strong_without_a_provider() {
        let builder = TaroDomainBuilder::new();
        assert_eq!(builder.consistency, ConsistencyMode::Strong);
        assert!(builder.provider.is_none());
        assert!(!builder.skip_validation);
        assert_eq!(builder.projection_mode, ProjectionMode::Skip);
    }

    #[test]
    fn settings_are_recorded() {
        let builder = TaroDomainBuilder::new()
            .consistency(ConsistencyMode::Eventual, Some("cloudflare_queue"))
            .snapshot_every(50)
            .catch_up_batch_size(7)
            .catch_up_max_attempts(3)
            .validation(ProjectionMode::Strict)
            .skip_validation();
        assert_eq!(builder.consistency, ConsistencyMode::Eventual);
        assert_eq!(builder.transport.as_deref(), Some("cloudflare_queue"));
        assert_eq!(builder.snapshot_every, Some(50));
        assert_eq!(builder.catch_up_batch_size, Some(7));
        assert_eq!(builder.catch_up_max_attempts, Some(3));
        assert_eq!(builder.projection_mode, ProjectionMode::Strict);
        assert!(builder.skip_validation);
    }

    #[test]
    fn bindings_go_to_the_container() {
        const N: Token<u32> = Token::new("n");
        let builder = TaroDomainBuilder::new().bind_instance(N, Arc::new(4));
        assert_eq!(*builder.container().resolve(N).unwrap(), 4);
    }

    #[test]
    fn saga_options() {
        assert_eq!(SagaOptions::default().timeout, None);
        assert_eq!(
            SagaOptions::with_timeout(Duration::from_secs(1)).timeout,
            Some(Duration::from_secs(1))
        );
    }
}
