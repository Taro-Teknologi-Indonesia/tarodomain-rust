//! The command registry (spec §4.3) and the registry of aggregate
//! repositories that handlers reach through their context.

use std::any::{Any, TypeId};
use std::collections::BTreeMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use super::handler::{CommandContext, CommandHandler};
use crate::adapters::ConsistencyMode;
use crate::async_trait;
use crate::bus::EventBus;
use crate::domain::{AnyCommand, Command, CommandMessage, SerializedCommand};
use crate::error::{FrameworkError, Result};
use crate::repository::Repository;

/// A command handler with its command type erased, so the registry can keep
/// handlers of every command type in one map.
#[async_trait]
pub trait AnyCommandHandler: Send + Sync {
    /// Runs `command` on the handler it wraps.
    ///
    /// Fails with `INVALID_CONFIGURATION` if `command` isn't of the type the
    /// handler was registered for, which only a bug in the framework can
    /// cause.
    async fn handle(&self, command: &AnyCommand) -> Result<()>;
}

/// Adapts a typed [`CommandHandler`] to [`AnyCommandHandler`].
struct Typed<C, H> {
    handler: H,
    _command: PhantomData<fn() -> C>,
}

#[async_trait]
impl<C, H> AnyCommandHandler for Typed<C, H>
where
    C: Command + Clone,
    H: CommandHandler<C>,
{
    async fn handle(&self, command: &AnyCommand) -> Result<()> {
        let payload = command.downcast_ref::<C>().ok_or_else(|| {
            FrameworkError::invalid_configuration(format!(
                "the handler of {} was given a {}",
                C::NAME,
                command.name()
            ))
        })?;
        let message = CommandMessage {
            envelope: command.envelope().clone(),
            command: payload.clone(),
        };
        self.handler.handle(&message).await
    }
}

type HandlerFactory = dyn Fn(CommandContext) -> Arc<dyn AnyCommandHandler> + Send + Sync;
type Decoder = fn(&SerializedCommand) -> Result<AnyCommand>;

/// What the registry knows about one command type: its name, an optional
/// consistency override, how to create its handler and how to read it back.
///
/// Cloning is cheap.
#[derive(Clone)]
pub struct CommandRegistration {
    name: &'static str,
    consistency: Option<ConsistencyMode>,
    factory: Arc<HandlerFactory>,
    decode: Decoder,
}

impl CommandRegistration {
    /// The command type name ([`Command::NAME`]).
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The consistency mode this command asks for, or `None` to use the
    /// application's default (§11.3).
    pub fn consistency(&self) -> Option<ConsistencyMode> {
        self.consistency
    }

    /// A new handler for one attempt, built from `context`.
    pub fn create_handler(&self, context: CommandContext) -> Arc<dyn AnyCommandHandler> {
        (self.factory)(context)
    }
}

impl fmt::Debug for CommandRegistration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommandRegistration")
            .field("name", &self.name)
            .field("consistency", &self.consistency)
            .finish_non_exhaustive()
    }
}

/// Reads a serialized command back as a `C` (spec §4.3).
fn decode<C: Command>(serialized: &SerializedCommand) -> Result<AnyCommand> {
    let command: C = serde_json::from_value(serialized.data.clone()).map_err(|error| {
        FrameworkError::command_validation(format!(
            "{} can't be read from its data: {error}",
            serialized.command_type
        ))
        .with_context("commandType", serialized.command_type.clone())
        .with_context("commandId", serialized.command_id.to_string())
        .with_source(error)
    })?;
    Ok(AnyCommand::new(serialized.envelope(), command))
}

/// Maps command names to their handlers (spec §4.3).
///
/// Each command type has one handler, registered with a factory that builds
/// a fresh handler from the pipeline's [`CommandContext`] for every attempt.
/// A command may ask for strong or eventual consistency; without a
/// preference it follows the application's default (§11.3).
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use tarodomain::adapters::ConsistencyMode;
/// use tarodomain::async_trait;
/// use tarodomain::command::{CommandHandler, CommandRegistry};
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
///
/// let mut registry = CommandRegistry::new();
/// registry
///     .register_with::<Ping, _>(Some(ConsistencyMode::Strong), |_context| PingHandler)
///     .unwrap();
/// let registration = registry.resolve("Ping").unwrap();
/// assert_eq!(registration.consistency(), Some(ConsistencyMode::Strong));
/// assert!(registry.resolve("Pong").is_err());
/// ```
#[derive(Clone, Default)]
pub struct CommandRegistry {
    registrations: BTreeMap<&'static str, CommandRegistration>,
}

impl CommandRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers the handler of `C`, which follows the application's default
    /// consistency mode. `factory` builds a fresh handler for each attempt.
    ///
    /// Commands must be `Clone`, because each attempt hands the handler its
    /// own message. Fails with `INVALID_CONFIGURATION` if `C` already has a
    /// handler.
    pub fn register<C, H>(
        &mut self,
        factory: impl Fn(CommandContext) -> H + Send + Sync + 'static,
    ) -> Result<()>
    where
        C: Command + Clone,
        H: CommandHandler<C> + 'static,
    {
        self.register_with::<C, H>(None, factory)
    }

    /// Like [`register`](Self::register), with the consistency mode the
    /// command asks for (`None` for the application's default).
    pub fn register_with<C, H>(
        &mut self,
        consistency: Option<ConsistencyMode>,
        factory: impl Fn(CommandContext) -> H + Send + Sync + 'static,
    ) -> Result<()>
    where
        C: Command + Clone,
        H: CommandHandler<C> + 'static,
    {
        if self.registrations.contains_key(C::NAME) {
            return Err(FrameworkError::invalid_configuration(format!(
                "{} already has a command handler",
                C::NAME
            ))
            .with_context("commandType", C::NAME));
        }
        let registration = CommandRegistration {
            name: C::NAME,
            consistency,
            factory: Arc::new(move |context| {
                Arc::new(Typed {
                    handler: factory(context),
                    _command: PhantomData::<fn() -> C>,
                })
            }),
            decode: decode::<C>,
        };
        self.registrations.insert(C::NAME, registration);
        Ok(())
    }

    /// The registration of the command named `name`, if there is one.
    pub fn get(&self, name: &str) -> Option<&CommandRegistration> {
        self.registrations.get(name)
    }

    /// Whether a handler is registered for `name`.
    pub fn contains(&self, name: &str) -> bool {
        self.registrations.contains_key(name)
    }

    /// The registration of the command named `name`.
    ///
    /// Fails with `COMMAND_HANDLER_NOT_REGISTERED` if there is none.
    pub fn resolve(&self, name: &str) -> Result<&CommandRegistration> {
        self.get(name).ok_or_else(|| not_registered(name))
    }

    /// Reads a stored or received command back as an [`AnyCommand`] (§4.3).
    ///
    /// Fails with `COMMAND_HANDLER_NOT_REGISTERED` for an unknown command
    /// type, and with `COMMAND_VALIDATION` if its data doesn't fit the
    /// command.
    pub fn deserialize(&self, serialized: &SerializedCommand) -> Result<AnyCommand> {
        let registration = self.resolve(&serialized.command_type)?;
        (registration.decode)(serialized)
    }

    /// The registered command names, in alphabetical order.
    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.registrations.keys().copied()
    }
}

impl fmt::Debug for CommandRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.registrations.keys()).finish()
    }
}

fn not_registered(name: &str) -> FrameworkError {
    FrameworkError::command_handler_not_registered(format!("no handler for command {name}"))
        .with_context("commandType", name)
}

type RepositoryFactory<A> = Arc<dyn Fn(Arc<dyn EventBus>) -> Arc<dyn Repository<A>> + Send + Sync>;

/// How to build the repository of each registered aggregate type, given the
/// event bus of the pipeline that asks.
///
/// The bus is a parameter because strong and eventual commands publish on
/// different buses (§11.3), while an aggregate has one repository setup.
#[derive(Default)]
pub struct RepositoryRegistry {
    factories: BTreeMap<TypeId, (&'static str, Arc<dyn Any + Send + Sync>)>,
}

impl RepositoryRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers how to build a repository for `A` on a given bus.
    ///
    /// Fails with `INVALID_CONFIGURATION` if `A` is registered already.
    pub fn register<A: crate::domain::Aggregate>(
        &mut self,
        factory: impl Fn(Arc<dyn EventBus>) -> Arc<dyn Repository<A>> + Send + Sync + 'static,
    ) -> Result<()> {
        let key = TypeId::of::<A>();
        if self.factories.contains_key(&key) {
            return Err(FrameworkError::invalid_configuration(format!(
                "{} already has a repository",
                A::TYPE
            ))
            .with_context("aggregateType", A::TYPE));
        }
        let factory: RepositoryFactory<A> = Arc::new(factory);
        self.factories.insert(key, (A::TYPE, Arc::new(factory)));
        Ok(())
    }

    /// Whether `A` is registered.
    pub fn contains<A: crate::domain::Aggregate>(&self) -> bool {
        self.factories.contains_key(&TypeId::of::<A>())
    }

    /// A repository for `A` that publishes on `bus`.
    ///
    /// Fails with `AGGREGATE_NOT_REGISTERED` if `A` isn't registered.
    pub fn create<A: crate::domain::Aggregate>(
        &self,
        bus: Arc<dyn EventBus>,
    ) -> Result<Arc<dyn Repository<A>>> {
        let (_, factory) = self.factories.get(&TypeId::of::<A>()).ok_or_else(|| {
            FrameworkError::aggregate_not_registered(format!(
                "no repository is registered for {}",
                A::TYPE
            ))
            .with_context("aggregateType", A::TYPE)
        })?;
        let factory = factory
            .downcast_ref::<RepositoryFactory<A>>()
            .expect("the factory under an aggregate's TypeId is for that aggregate");
        Ok(factory(bus))
    }

    /// The registered aggregate type names, in no particular order.
    pub fn types(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.factories.values().map(|(name, _)| *name)
    }
}

impl fmt::Debug for RepositoryRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.types()).finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;
    use crate::bus::InMemoryEventBus;
    use crate::domain::{Aggregate, AggregateCore, Event};
    use crate::kernel::ids::new_v7;
    use crate::{aggregate_id, event_family};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Deposit {
        cents: i64,
    }

    impl Command for Deposit {
        const NAME: &'static str = "Deposit";
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Withdraw {
        cents: i64,
    }

    impl Command for Withdraw {
        const NAME: &'static str = "Withdraw";
    }

    /// Counts the cents of the deposits it was handed.
    struct Counting {
        total: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl CommandHandler<Deposit> for Counting {
        async fn handle(&self, command: &CommandMessage<Deposit>) -> Result<()> {
            self.total
                .fetch_add(command.cents as usize, Ordering::SeqCst);
            Ok(())
        }
    }

    #[async_trait]
    impl CommandHandler<Withdraw> for Counting {
        async fn handle(&self, _command: &CommandMessage<Withdraw>) -> Result<()> {
            Err(FrameworkError::command_validation("no money"))
        }
    }

    aggregate_id!(pub AccountId);

    #[derive(Debug, Serialize, Deserialize)]
    struct Opened;

    impl Event for Opened {
        const NAME: &'static str = "Opened";
    }

    event_family!(
        enum AccountEvent {
            Opened,
        }
    );

    #[derive(Debug)]
    struct Account {
        id: AccountId,
        core: AggregateCore,
    }

    impl Aggregate for Account {
        type Id = AccountId;
        type Event = AccountEvent;
        type Snapshot = ();
        const TYPE: &'static str = "Account";

        fn new(id: AccountId) -> Self {
            Self {
                id,
                core: AggregateCore::default(),
            }
        }
        fn id(&self) -> AccountId {
            self.id
        }
        fn core(&self) -> &AggregateCore {
            &self.core
        }
        fn core_mut(&mut self) -> &mut AggregateCore {
            &mut self.core
        }
        fn apply(&mut self, _event: &AccountEvent) {}
        fn snapshot_state(&self) {}
        fn restore_snapshot_state(&mut self, _state: ()) {}
    }

    /// A repository that holds nothing, which records the bus it was given.
    struct Empty {
        bus: Arc<dyn EventBus>,
    }

    #[async_trait]
    impl Repository<Account> for Empty {
        async fn load_or_none(&self, _id: AccountId) -> Result<Option<Account>> {
            Ok(None)
        }
        async fn save(&self, _aggregate: &mut Account) -> Result<()> {
            Ok(())
        }
        async fn exists(&self, _id: AccountId) -> Result<bool> {
            Ok(self.bus.name() == "strong-bus")
        }
    }

    fn context(registry: RepositoryRegistry, bus: &str) -> CommandContext {
        CommandContext::new(
            ConsistencyMode::Strong,
            Arc::new(InMemoryEventBus::new(bus)),
            Arc::new(registry),
        )
    }

    #[tokio::test]
    async fn a_registered_handler_runs_through_the_erased_form() {
        let total = Arc::new(AtomicUsize::new(0));
        let mut registry = CommandRegistry::new();
        let shared = Arc::clone(&total);
        registry
            .register::<Deposit, _>(move |_| Counting {
                total: Arc::clone(&shared),
            })
            .unwrap();

        let registration = registry.resolve("Deposit").unwrap();
        assert_eq!(registration.name(), "Deposit");
        assert_eq!(registration.consistency(), None);

        let handler = registration.create_handler(context(RepositoryRegistry::new(), "bus"));
        handler.handle(&Deposit { cents: 40 }.into()).await.unwrap();
        handler.handle(&Deposit { cents: 2 }.into()).await.unwrap();
        assert_eq!(total.load(Ordering::SeqCst), 42);
    }

    #[tokio::test]
    async fn the_handler_gets_the_commands_own_envelope() {
        struct Echo(Arc<std::sync::Mutex<Option<crate::kernel::ids::Uuid>>>);

        #[async_trait]
        impl CommandHandler<Deposit> for Echo {
            async fn handle(&self, command: &CommandMessage<Deposit>) -> Result<()> {
                *self.0.lock().unwrap() = Some(command.envelope.id);
                Ok(())
            }
        }

        let seen = Arc::new(std::sync::Mutex::new(None));
        let mut registry = CommandRegistry::new();
        let shared = Arc::clone(&seen);
        registry
            .register::<Deposit, _>(move |_| Echo(Arc::clone(&shared)))
            .unwrap();
        let command: AnyCommand = Deposit { cents: 1 }.into();
        let handler = registry
            .resolve("Deposit")
            .unwrap()
            .create_handler(context(RepositoryRegistry::new(), "bus"));
        handler.handle(&command).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), Some(command.id()));
    }

    #[tokio::test]
    async fn a_handler_error_comes_back_unchanged_and_a_wrong_payload_is_refused() {
        let mut registry = CommandRegistry::new();
        let total = Arc::new(AtomicUsize::new(0));
        registry
            .register::<Withdraw, _>(move |_| Counting {
                total: Arc::clone(&total),
            })
            .unwrap();
        let handler = registry
            .resolve("Withdraw")
            .unwrap()
            .create_handler(context(RepositoryRegistry::new(), "bus"));

        let error = handler
            .handle(&Withdraw { cents: 1 }.into())
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "COMMAND_VALIDATION");

        let error = handler
            .handle(&Deposit { cents: 1 }.into())
            .await
            .unwrap_err();
        assert_eq!(error.code().as_str(), "INVALID_CONFIGURATION");
    }

    #[test]
    fn an_unknown_command_is_not_registered() {
        let registry = CommandRegistry::new();
        let error = registry.resolve("Deposit").unwrap_err();
        assert_eq!(error.code().as_str(), "COMMAND_HANDLER_NOT_REGISTERED");
        assert_eq!(error.context()["commandType"], "Deposit");
        assert!(registry.get("Deposit").is_none());
        assert!(!registry.contains("Deposit"));

        let serialized = AnyCommand::from(Deposit { cents: 1 })
            .to_serialized()
            .unwrap();
        let error = registry.deserialize(&serialized).unwrap_err();
        assert_eq!(error.code().as_str(), "COMMAND_HANDLER_NOT_REGISTERED");
    }

    #[test]
    fn a_command_registers_once_and_names_are_listed() {
        let mut registry = CommandRegistry::new();
        let make = |_| Counting {
            total: Arc::new(AtomicUsize::new(0)),
        };
        registry.register::<Withdraw, _>(make).unwrap();
        registry
            .register_with::<Deposit, _>(Some(ConsistencyMode::Eventual), make)
            .unwrap();
        assert_eq!(
            registry.resolve("Deposit").unwrap().consistency(),
            Some(ConsistencyMode::Eventual)
        );

        let error = registry.register::<Deposit, _>(make).unwrap_err();
        assert_eq!(error.code().as_str(), "INVALID_CONFIGURATION");
        assert_eq!(
            registry.resolve("Deposit").unwrap().consistency(),
            Some(ConsistencyMode::Eventual),
            "the first registration stays"
        );
        assert_eq!(
            registry.names().collect::<Vec<_>>(),
            ["Deposit", "Withdraw"]
        );
    }

    #[test]
    fn deserialize_rebuilds_the_command_with_its_envelope() {
        let mut registry = CommandRegistry::new();
        registry
            .register::<Deposit, _>(|_| Counting {
                total: Arc::new(AtomicUsize::new(0)),
            })
            .unwrap();

        let original: AnyCommand = Deposit { cents: 250 }.into();
        let serialized = original.to_serialized().unwrap();
        let restored = registry.deserialize(&serialized).unwrap();
        assert_eq!(
            restored.downcast_ref::<Deposit>(),
            Some(&Deposit { cents: 250 })
        );
        assert_eq!(restored.envelope(), original.envelope());

        let broken = SerializedCommand {
            data: json!({"cents": "lots"}),
            ..serialized
        };
        let error = registry.deserialize(&broken).unwrap_err();
        assert_eq!(error.code().as_str(), "COMMAND_VALIDATION");
        assert_eq!(error.context()["commandType"], "Deposit");
        assert_eq!(error.context()["commandId"], broken.command_id.to_string());
    }

    #[tokio::test]
    async fn repositories_are_built_on_the_bus_of_the_context() {
        let mut repositories = RepositoryRegistry::new();
        repositories
            .register::<Account>(|bus| Arc::new(Empty { bus }))
            .unwrap();
        assert!(repositories.contains::<Account>());
        assert_eq!(repositories.types().collect::<Vec<_>>(), ["Account"]);

        let strong = context(repositories, "strong-bus");
        let id = AccountId::from_uuid(new_v7());
        let repository = strong.repository::<Account>().unwrap();
        assert!(
            repository.exists(id).await.unwrap(),
            "it got the strong bus"
        );
        assert!(repository.load_or_none(id).await.unwrap().is_none());
        assert_eq!(strong.consistency(), ConsistencyMode::Strong);
        assert_eq!(strong.bus().name(), "strong-bus");

        let mut again = RepositoryRegistry::new();
        again
            .register::<Account>(|bus| Arc::new(Empty { bus }))
            .unwrap();
        let eventual = context(again, "eventual-bus");
        let repository = eventual.repository::<Account>().unwrap();
        assert!(
            !repository.exists(id).await.unwrap(),
            "it got the other bus"
        );
    }

    #[test]
    fn an_unregistered_aggregate_has_no_repository() {
        let strong = context(RepositoryRegistry::new(), "bus");
        let error = strong.repository::<Account>().err().unwrap();
        assert_eq!(error.code().as_str(), "AGGREGATE_NOT_REGISTERED");
        assert_eq!(error.context()["aggregateType"], "Account");

        let mut repositories = RepositoryRegistry::new();
        repositories
            .register::<Account>(|bus| Arc::new(Empty { bus }))
            .unwrap();
        let error = repositories
            .register::<Account>(|bus| Arc::new(Empty { bus }))
            .unwrap_err();
        assert_eq!(error.code().as_str(), "INVALID_CONFIGURATION");
    }
}
