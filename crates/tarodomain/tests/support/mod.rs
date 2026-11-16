//! Shared fixtures for the integration tests: a bank on the local provider,
//! wired the way the framework builder will wire it.

#![allow(dead_code)]

pub mod bank;
pub mod transfer;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tarodomain::adapters::{
    CommandIdempotencyStore, ConsistencyMode, EventBusConfig, Provider, SagaRecord,
    SagaStoreAdapter, StreamIds,
};
use tarodomain::async_trait;
use tarodomain::bus::{EventBus, EventHandler};
use tarodomain::command::{
    CommandDispatcher, CommandPipeline, CommandRegistry, ConcurrencyChecker, RepositoryRegistry,
};
use tarodomain::domain::{AnyCommand, DomainEvent, EventFamily};
use tarodomain::error::{FrameworkError, Result};
use tarodomain::event_sourcing::{EventRegistry, EventStore};
use tarodomain::kernel::backoff::RecordingSleeper;
use tarodomain::kernel::ids::Uuid;
use tarodomain::repository::{EventSourcedRepository, Repository};
use tarodomain::saga::{SagaCommandDispatch, SagaEventHandler};
use tarodomain_local::LocalProvider;

use bank::{
    AccountEvent, AccountId, BankAccount, Counters, Deposit, DepositHandler, OpenAccount,
    OpenAccountHandler, Withdraw, WithdrawHandler,
};
use transfer::{
    CompleteTransfer, CompleteTransferHandler, FailTransfer, FailTransferHandler, MoneyTransfer,
    RequestTransfer, RequestTransferHandler, TransferEvent, TransferSaga,
};

/// The name the read model is stored under.
pub const READ_MODEL: &str = "balance-read-model";

/// A strong-mode read model: it records each event it sees, in a table the
/// unit of work covers, and then fails if told to.
pub struct ReadModel {
    provider: Arc<LocalProvider>,
    pub fail: AtomicBool,
}

#[async_trait]
impl EventHandler for ReadModel {
    fn name(&self) -> &str {
        READ_MODEL
    }

    async fn handle(&self, event: &DomainEvent) -> Result<()> {
        let store = self.provider.create_processed_event_store();
        store.mark_processed(READ_MODEL, "", event.id()).await?;
        if self.fail.load(Ordering::SeqCst) {
            return Err(FrameworkError::projection_handler_failed(
                "the read model is down",
            ));
        }
        Ok(())
    }
}

/// The eventual bus: records what it is given, or fails when told to.
#[derive(Default)]
pub struct QueueBus {
    pub published: Mutex<Vec<DomainEvent>>,
    pub fail: AtomicBool,
    pub calls: AtomicUsize,
}

#[async_trait]
impl EventBus for QueueBus {
    fn name(&self) -> &str {
        "queue"
    }

    async fn publish_all(&self, events: &[DomainEvent]) -> Result<()> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            return Err(FrameworkError::queue_error("the queue is down"));
        }
        self.published.lock().unwrap().extend_from_slice(events);
        Ok(())
    }
}

/// A bank on the local provider, with both pipelines and a dispatcher.
pub struct Bank {
    pub provider: Arc<LocalProvider>,
    pub store: EventStore,
    pub read_model: Arc<ReadModel>,
    pub queue: Arc<QueueBus>,
    pub counters: Arc<Counters>,
    pub sleeper: Arc<RecordingSleeper>,
    pub dispatcher: Arc<CommandDispatcher>,
    pub saga: Arc<SagaEventHandler<TransferSaga>>,
    pub saga_store: Arc<dyn SagaStoreAdapter>,
    /// Makes the next saga dispatch fail after its commands have committed.
    pub crash_after_dispatch: Arc<AtomicBool>,
    /// How many queue events the saga has been given (eventual mode).
    pub delivered: AtomicUsize,
    pub idempotency: Arc<dyn CommandIdempotencyStore>,
}

impl Bank {
    /// A bank whose commands run in `default` mode, except `Deposit`, which
    /// asks for `deposit_mode` if given.
    pub fn new(default: ConsistencyMode, deposit_mode: Option<ConsistencyMode>) -> Self {
        let provider = Arc::new(LocalProvider::new());
        let mut events = EventRegistry::new();
        AccountEvent::register_all(&mut events).unwrap();
        TransferEvent::register_all(&mut events).unwrap();
        let store = EventStore::new(provider.create_event_store_adapter(), Arc::new(events));

        let strong_bus = provider
            .create_event_bus(&EventBusConfig::new(ConsistencyMode::Strong))
            .unwrap();
        let read_model = Arc::new(ReadModel {
            provider: provider.clone(),
            fail: AtomicBool::new(false),
        });
        strong_bus
            .as_subscribable()
            .unwrap()
            .subscribe(read_model.clone())
            .unwrap();
        let queue = Arc::new(QueueBus::default());

        let mut repositories = RepositoryRegistry::new();
        let repository_store = store.clone();
        repositories
            .register::<BankAccount>(move |bus| {
                Arc::new(EventSourcedRepository::new(repository_store.clone(), bus))
                    as Arc<dyn Repository<BankAccount>>
            })
            .unwrap();
        let transfer_store = store.clone();
        repositories
            .register::<MoneyTransfer>(move |bus| {
                Arc::new(EventSourcedRepository::new(transfer_store.clone(), bus))
                    as Arc<dyn Repository<MoneyTransfer>>
            })
            .unwrap();
        let repositories = Arc::new(repositories);

        let counters = Arc::new(Counters::default());
        let mut registry = CommandRegistry::new();
        registry
            .register::<OpenAccount, _>(|context| OpenAccountHandler { context })
            .unwrap();
        let handler_counters = counters.clone();
        registry
            .register_with::<Deposit, _>(deposit_mode, move |context| DepositHandler {
                context,
                counters: handler_counters.clone(),
            })
            .unwrap();

        registry
            .register::<Withdraw, _>(|context| WithdrawHandler { context })
            .unwrap();
        registry
            .register::<RequestTransfer, _>(|context| RequestTransferHandler { context })
            .unwrap();
        registry
            .register::<CompleteTransfer, _>(|context| CompleteTransferHandler { context })
            .unwrap();
        registry
            .register::<FailTransfer, _>(|context| FailTransferHandler { context })
            .unwrap();

        let sleeper = Arc::new(RecordingSleeper::new());
        let checker = ConcurrencyChecker::new(sleeper.clone());
        let pipeline = |mode, bus: Arc<dyn EventBus>| {
            CommandPipeline::new(
                mode,
                provider.clone(),
                bus,
                repositories.clone(),
                checker.clone(),
            )
        };
        let dispatcher = Arc::new(CommandDispatcher::new(
            Arc::new(registry),
            pipeline(ConsistencyMode::Strong, strong_bus.clone()),
            pipeline(ConsistencyMode::Eventual, queue.clone()),
            default,
        ));

        let cell: Arc<OnceLock<Arc<CommandDispatcher>>> = Arc::new(OnceLock::new());
        let crash_after_dispatch = Arc::new(AtomicBool::new(false));
        let (dispatch_cell, crash) = (cell.clone(), crash_after_dispatch.clone());
        let dispatch = SagaCommandDispatch::new(move |commands| {
            let (cell, crash) = (dispatch_cell.clone(), crash.clone());
            async move {
                let dispatcher = cell.get().expect("the bank is built").clone();
                for command in commands {
                    dispatcher.dispatch(command).await?;
                }
                if crash.swap(false, Ordering::SeqCst) {
                    return Err(FrameworkError::queue_error("crashed before the saga save"));
                }
                Ok(())
            }
        });
        let saga_store = provider.create_saga_store_adapter();
        let saga = Arc::new(
            SagaEventHandler::<TransferSaga>::new(
                saga_store.clone(),
                provider.create_processed_event_store(),
                dispatch,
            )
            .unwrap(),
        );
        strong_bus
            .as_subscribable()
            .unwrap()
            .subscribe(saga.clone())
            .unwrap();
        cell.set(dispatcher.clone()).ok();
        let idempotency = provider.create_command_idempotency_store();
        Self {
            provider,
            store,
            read_model,
            queue,
            counters,
            sleeper,
            dispatcher,
            saga,
            saga_store,
            crash_after_dispatch,
            delivered: AtomicUsize::new(0),
            idempotency,
        }
    }

    /// Eventual mode: gives the saga each event the queue holds, one at a
    /// time, until none are new. Stops at the first failure and returns it;
    /// a later call delivers that event again.
    pub async fn pump(&self) -> Result<()> {
        loop {
            let next = {
                let published = self.queue.published.lock().unwrap();
                published
                    .get(self.delivered.load(Ordering::SeqCst))
                    .cloned()
            };
            let Some(event) = next else {
                return Ok(());
            };
            self.saga.handle(&event).await?;
            self.delivered.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The processed-event store of the bank's provider.
    pub fn provider_processed(&self) -> Arc<dyn tarodomain::adapters::ProcessedEventStore> {
        self.provider.create_processed_event_store()
    }

    /// The saga stored under `saga_id`.
    pub async fn saga_record(&self, saga_id: Uuid) -> Option<SagaRecord> {
        self.saga_store.load("TransferSaga", saga_id).await.unwrap()
    }

    /// Opens an account and returns its ID.
    pub async fn open(&self, owner: &str) -> AccountId {
        let account = AccountId::generate();
        let command = OpenAccount {
            account,
            owner: owner.into(),
        };
        self.dispatcher.dispatch(command).await.unwrap();
        account
    }

    /// The account's balance, replayed from its stream.
    pub async fn balance(&self, account: AccountId) -> i64 {
        let repository =
            EventSourcedRepository::<BankAccount>::new(self.store.clone(), self.queue.clone());
        repository.load(account).await.unwrap().state.balance
    }

    /// How many events the account's stream holds.
    pub async fn stream_version(&self, account: AccountId) -> u64 {
        let stream_id = StreamIds::for_aggregate("BankAccount", account);
        self.store.get_stream_version(&stream_id).await.unwrap()
    }

    /// How many events the read model has seen.
    pub async fn read_model_count(&self) -> u64 {
        let store = self.provider.create_processed_event_store();
        store.count(READ_MODEL, None).await.unwrap()
    }

    /// Whether the command was marked processed.
    pub async fn processed(&self, command: &AnyCommand) -> bool {
        self.idempotency.exists(command.id()).await.unwrap()
    }
}

/// A deposit command.
pub fn deposit(account: AccountId, cents: i64) -> AnyCommand {
    Deposit { account, cents }.into()
}

/// The IDs of the events in a dispatch result.
pub fn event_ids(events: &[DomainEvent]) -> Vec<Uuid> {
    events.iter().map(DomainEvent::id).collect()
}
