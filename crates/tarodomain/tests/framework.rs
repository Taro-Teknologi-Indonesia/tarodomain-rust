//! The built framework (spec §25.2): the builder, dispatch, the queue
//! consumer, scheduled jobs, cleanup and validation, on the local provider.

mod support;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, TimeDelta, TimeZone, Utc};
use tarodomain::adapters::{
    CommandIdempotencyStore, ConsistencyMode, DeadLetterStore, EventBusConfig, EventStoreAdapter,
    ProcessedEventStore, Provider, SagaStatus, SagaStoreAdapter, SnapshotStoreAdapter, StreamIds,
    UnitOfWork,
};
use tarodomain::async_trait;
use tarodomain::bus::EventBus;
use tarodomain::domain::{CommandMessage, Recorded};
use tarodomain::error::{ErrorCategory, ErrorCode, FrameworkError, Result};
use tarodomain::framework::{
    MessageOutcome, PROVIDER, ProjectionMode, SagaOptions, TARO_DOMAIN, TaroDomain,
    TaroDomainBuilder,
};
use tarodomain::kernel::clock::{Clock, ManualClock};
use tarodomain::kernel::ids::new_v7;
use tarodomain::projection::{Projection, ProjectionRoutes};
use tarodomain_local::{LocalDatabase, LocalProvider};

use support::bank::{
    AccountId, BankAccount, Counters, Deposit, DepositHandler, MoneyDeposited, MoneyWithdrawn,
    OpenAccount, OpenAccountHandler, Withdraw, WithdrawHandler,
};
use support::transfer::{
    CompleteTransfer, CompleteTransferHandler, FailTransfer, FailTransferHandler, MoneyTransfer,
    RequestTransfer, RequestTransferHandler, TransferId, TransferSaga,
};

/// The sum of every deposit and withdrawal.
#[derive(Default)]
struct Totals {
    cents: Mutex<i64>,
    down: AtomicBool,
}

impl Totals {
    async fn on_deposited(self: Arc<Self>, e: Recorded<MoneyDeposited>) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            return Err(FrameworkError::custom(
                "DOWN",
                ErrorCategory::Infrastructure,
                "the read model is down",
            ));
        }
        *self.cents.lock().unwrap() += e.cents;
        Ok(())
    }

    async fn on_withdrawn(self: Arc<Self>, e: Recorded<MoneyWithdrawn>) -> Result<()> {
        *self.cents.lock().unwrap() -= e.cents;
        Ok(())
    }

    fn total(&self) -> i64 {
        *self.cents.lock().unwrap()
    }
}

#[async_trait]
impl Projection for Totals {
    fn name(&self) -> &str {
        "totals"
    }

    fn routes(r: &mut ProjectionRoutes<Self>) {
        r.on::<MoneyDeposited, _, _>(Self::on_deposited);
        r.on::<MoneyWithdrawn, _, _>(Self::on_withdrawn);
    }
}

/// A provider that has no unit of work, like the AWS one.
struct NoUnits(Arc<LocalProvider>);

#[async_trait]
impl Provider for NoUnits {
    fn provider_type(&self) -> &str {
        "no-units"
    }
    fn create_event_store_adapter(&self) -> Arc<dyn EventStoreAdapter> {
        self.0.create_event_store_adapter()
    }
    fn create_snapshot_store_adapter(&self) -> Arc<dyn SnapshotStoreAdapter> {
        self.0.create_snapshot_store_adapter()
    }
    fn create_saga_store_adapter(&self) -> Arc<dyn SagaStoreAdapter> {
        self.0.create_saga_store_adapter()
    }
    fn create_processed_event_store(&self) -> Arc<dyn ProcessedEventStore> {
        self.0.create_processed_event_store()
    }
    fn create_command_idempotency_store(&self) -> Arc<dyn CommandIdempotencyStore> {
        self.0.create_command_idempotency_store()
    }
    fn create_dead_letter_store(&self) -> Arc<dyn DeadLetterStore> {
        self.0.create_dead_letter_store()
    }
    fn create_event_bus(&self, config: &EventBusConfig) -> Result<Arc<dyn EventBus>> {
        self.0.create_event_bus(config)
    }
    fn create_unit_of_work(&self) -> Option<Arc<dyn UnitOfWork>> {
        None
    }
}

fn bank_builder(provider: Arc<dyn Provider>, mode: ConsistencyMode) -> TaroDomainBuilder {
    TaroDomain::builder()
        .use_provider(provider)
        .consistency(mode, None)
        .aggregate::<BankAccount>(|a| a.event_sourced().snapshot_every(10))
        .aggregate::<MoneyTransfer>(|a| a)
        .command::<OpenAccount, _>(|_, context| OpenAccountHandler { context }, None)
        .command::<Deposit, _>(
            |_, context| DepositHandler {
                context,
                counters: Arc::new(Counters::default()),
            },
            None,
        )
        .command::<Withdraw, _>(|_, context| WithdrawHandler { context }, None)
        .command::<RequestTransfer, _>(|_, context| RequestTransferHandler { context }, None)
        .command::<CompleteTransfer, _>(|_, context| CompleteTransferHandler { context }, None)
        .command::<FailTransfer, _>(|_, context| FailTransferHandler { context }, None)
}

async fn bank(mode: ConsistencyMode) -> (TaroDomain, Arc<Totals>, Arc<LocalProvider>) {
    let provider = Arc::new(LocalProvider::new());
    let totals = Arc::new(Totals::default());
    let shared = totals.clone();
    let domain = bank_builder(provider.clone(), mode)
        .projection(move |_| Ok(shared))
        .saga::<TransferSaga>(SagaOptions::with_timeout(Duration::from_secs(600)))
        .build()
        .await
        .unwrap();
    (domain, totals, provider)
}

async fn open(domain: &TaroDomain) -> AccountId {
    let account = AccountId::generate();
    domain
        .dispatch(OpenAccount {
            account,
            owner: "Ann".into(),
        })
        .await
        .unwrap();
    account
}

fn deposit(account: AccountId, cents: i64) -> Deposit {
    Deposit { account, cents }
}

#[tokio::test]
async fn strong_commands_reach_the_projection_and_the_saga_moves_money() {
    let (domain, totals, provider) = bank(ConsistencyMode::Strong).await;
    let (ann, bob) = (open(&domain).await, open(&domain).await);
    domain.dispatch(deposit(ann, 100)).await.unwrap();
    assert_eq!(totals.total(), 100);

    let saga_id = new_v7();
    let request = RequestTransfer {
        transfer: TransferId::generate(),
        from: ann,
        to: bob,
        cents: 30,
        fail_after: None,
    };
    domain
        .dispatch(CommandMessage::new(request).with_correlation_id(saga_id))
        .await
        .unwrap();

    // 100 deposited, 30 out of Ann and 30 into Bob.
    assert_eq!(totals.total(), 100);
    let saga = provider
        .create_saga_store_adapter()
        .load("TransferSaga", saga_id)
        .await
        .unwrap()
        .expect("the saga started");
    assert_eq!(saga.status, SagaStatus::Completed);
    let stream = StreamIds::for_aggregate("BankAccount", bob);
    assert_eq!(
        domain
            .event_store()
            .get_stream_version(&stream)
            .await
            .unwrap(),
        2
    );
}

#[tokio::test]
async fn the_container_holds_the_framework_and_the_provider() {
    let (domain, _, _) = bank(ConsistencyMode::Strong).await;
    let again = domain.di().resolve(TARO_DOMAIN).unwrap();
    assert!(Arc::ptr_eq(again.provider(), domain.provider()));
    let provider = domain.di().resolve(PROVIDER).unwrap();
    assert_eq!(provider.provider_type(), "local");
}

#[tokio::test]
async fn consistency_follows_the_override_then_the_default() {
    let provider = Arc::new(LocalProvider::new());
    let domain = bank_builder(provider, ConsistencyMode::Eventual)
        .command::<support::bank::CloseAccount, _>(
            |_, context| OpenAccountHandlerClose { context },
            Some(ConsistencyMode::Strong),
        )
        .build()
        .await
        .unwrap();
    assert_eq!(
        domain.consistency_for::<Deposit>().unwrap(),
        ConsistencyMode::Eventual
    );
    assert_eq!(
        domain
            .consistency_for::<support::bank::CloseAccount>()
            .unwrap(),
        ConsistencyMode::Strong
    );
    assert_eq!(
        *domain.consistency_for_name("Nope").unwrap_err().code(),
        ErrorCode::CommandHandlerNotRegistered
    );
}

struct OpenAccountHandlerClose {
    #[allow(dead_code)]
    context: tarodomain::command::CommandContext,
}

#[async_trait]
impl tarodomain::command::CommandHandler<support::bank::CloseAccount> for OpenAccountHandlerClose {
    async fn handle(&self, _: &CommandMessage<support::bank::CloseAccount>) -> Result<()> {
        Ok(())
    }
}

#[tokio::test]
async fn eventual_events_wait_for_the_queue_consumer_and_arrive_once() {
    let (domain, totals, _) = bank(ConsistencyMode::Eventual).await;
    let ann = open(&domain).await;
    let result = domain.dispatch(deposit(ann, 50)).await.unwrap();
    assert_eq!(totals.total(), 0, "nothing listens on the eventual bus");

    let serialized: Vec<_> = result
        .events
        .iter()
        .map(|e| e.serialize().unwrap())
        .collect();
    let outcomes = domain.handle_events(serialized.clone()).await;
    assert_eq!(outcomes[0].outcome, MessageOutcome::Ack);
    assert_eq!(totals.total(), 50);

    // The same message again, and then the catch-up job: no double count.
    domain.handle_events(serialized).await;
    let run = domain.run_scheduled().await;
    assert!(!run.has_errors(), "{run:?}");
    assert_eq!(totals.total(), 50);
}

#[tokio::test]
async fn catch_up_delivers_what_the_queue_never_did() {
    let (domain, totals, _) = bank(ConsistencyMode::Eventual).await;
    let ann = open(&domain).await;
    domain.dispatch(deposit(ann, 70)).await.unwrap();
    assert_eq!(totals.total(), 0);
    let run = domain.run_scheduled().await;
    assert!(!run.has_errors(), "{run:?}");
    assert_eq!(totals.total(), 70);
    assert_eq!(run.timeouts.len(), 1);
    assert_eq!(run.timeouts[0].0, "TransferSaga");
}

#[tokio::test]
async fn a_failing_handler_asks_for_a_retry_with_a_growing_delay() {
    let (domain, totals, _) = bank(ConsistencyMode::Eventual).await;
    let ann = open(&domain).await;
    let result = domain.dispatch(deposit(ann, 5)).await.unwrap();
    let event = result.events[0].serialize().unwrap();
    totals.down.store(true, Ordering::SeqCst);

    let first = domain.handle_events([event.clone()]).await.remove(0);
    let MessageOutcome::Retry { delay: first_delay } = first.outcome else {
        panic!("{:?}", first.outcome);
    };
    assert!(first.error.is_some());
    let mut later = tarodomain::framework::QueueMessage::from(event.clone());
    later.attempts = 4;
    let MessageOutcome::Retry { delay: later_delay } =
        domain.handle_events([later]).await.remove(0).outcome
    else {
        panic!("not a retry");
    };
    assert!(later_delay > first_delay);

    totals.down.store(false, Ordering::SeqCst);
    let retried = domain.handle_events([event]).await.remove(0);
    assert_eq!(retried.outcome, MessageOutcome::Ack);
    assert_eq!(totals.total(), 5);
}

#[tokio::test]
async fn an_unreadable_event_is_dead_lettered_and_acknowledged() {
    let (domain, _, provider) = bank(ConsistencyMode::Eventual).await;
    let ann = open(&domain).await;
    let result = domain.dispatch(deposit(ann, 5)).await.unwrap();
    let mut event = result.events[0].serialize().unwrap();
    event.event_type = "NeverHeardOfIt".into();

    let outcome = domain.handle_events([event.clone()]).await.remove(0);
    assert_eq!(outcome.outcome, MessageOutcome::DeadLettered);
    assert_eq!(*outcome.error.unwrap().code(), ErrorCode::UnknownEventType);
    let letters = provider.create_dead_letter_store();
    let letter = letters
        .get(event.event_id, "event-queue")
        .await
        .unwrap()
        .expect("a dead letter");
    assert_eq!(letter.error_code, "UNKNOWN_EVENT_TYPE");
}

#[tokio::test]
async fn cleanup_deletes_old_records_only() {
    let clock = Arc::new(ManualClock::new(
        Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
    ));
    let db = LocalDatabase::with_clock(clock.clone());
    let provider = Arc::new(LocalProvider::with_database(db));
    let domain = bank_builder(provider.clone(), ConsistencyMode::Strong)
        .build()
        .await
        .unwrap();
    open(&domain).await;
    let report = domain.run_cleanup().await.unwrap();
    assert_eq!(report.commands, 0);

    clock.set(clock.now() + TimeDelta::days(8));
    let report = domain.run_cleanup().await.unwrap();
    assert_eq!(report.commands, 1);
    assert_eq!(report.dead_letters, 0);
}

#[tokio::test]
async fn timestamps_come_from_the_provider_clock() {
    let at: DateTime<Utc> = Utc.timestamp_opt(1_600_000_000, 0).unwrap();
    let db = LocalDatabase::with_clock(Arc::new(ManualClock::new(at)));
    let provider = Arc::new(LocalProvider::with_database(db));
    let domain = bank_builder(provider, ConsistencyMode::Strong)
        .build()
        .await
        .unwrap();
    let account = AccountId::generate();
    let result = domain
        .dispatch(OpenAccount {
            account,
            owner: "Ann".into(),
        })
        .await
        .unwrap();
    assert_eq!(result.events[0].envelope().occurred_at, at);
}

#[tokio::test]
async fn migrate_is_safe_to_repeat() {
    let (domain, _, _) = bank(ConsistencyMode::Strong).await;
    domain.migrate().await.unwrap();
    domain.migrate().await.unwrap();
}

#[tokio::test]
async fn an_unknown_command_is_rejected() {
    let (domain, _, _) = bank(ConsistencyMode::Strong).await;
    let error = domain
        .dispatch(support::bank::CloseAccount)
        .await
        .unwrap_err();
    assert_eq!(*error.code(), ErrorCode::CommandHandlerNotRegistered);
}

#[tokio::test]
async fn a_missing_provider_fails_the_build() {
    let error = TaroDomain::builder().build().await.unwrap_err();
    assert_eq!(*error.code(), ErrorCode::InvalidConfiguration);
    assert!(error.message().contains("provider"));
}

#[tokio::test]
async fn strong_commands_need_a_unit_of_work() {
    let provider = Arc::new(NoUnits(Arc::new(LocalProvider::new())));
    let error = bank_builder(provider.clone(), ConsistencyMode::Strong)
        .build()
        .await
        .unwrap_err();
    assert_eq!(*error.code(), ErrorCode::InvalidConfiguration);
    assert!(error.message().contains("unit of work"));
    // Eventual-only is fine.
    bank_builder(provider, ConsistencyMode::Eventual)
        .build()
        .await
        .unwrap();
}

#[tokio::test]
async fn every_configuration_problem_is_reported_together() {
    let error = bank_builder(Arc::new(LocalProvider::new()), ConsistencyMode::Strong)
        .command::<Deposit, _>(
            |_, context| DepositHandler {
                context,
                counters: Arc::new(Counters::default()),
            },
            None,
        )
        .aggregate::<BankAccount>(|a| a)
        .saga::<support::transfer::QuietSaga>(SagaOptions::with_timeout(Duration::ZERO))
        .build()
        .await
        .unwrap_err();
    assert_eq!(*error.code(), ErrorCode::InvalidConfiguration);
    let problems = error.context()["errors"].as_array().unwrap().clone();
    let text = format!("{problems:?}");
    assert!(
        text.contains("command Deposit is registered 2 times"),
        "{text}"
    );
    assert!(
        text.contains("aggregate BankAccount is registered 2 times"),
        "{text}"
    );
    assert!(text.contains("QuietSaga"), "{text}");
    assert!(problems.len() >= 4, "{text}");
}

#[tokio::test]
async fn skipping_validation_lets_a_bad_configuration_through_to_the_registries() {
    // The duplicate is still caught, by the registry, one error at a time.
    let error = bank_builder(Arc::new(LocalProvider::new()), ConsistencyMode::Strong)
        .command::<Deposit, _>(
            |_, context| DepositHandler {
                context,
                counters: Arc::new(Counters::default()),
            },
            None,
        )
        .skip_validation()
        .build()
        .await
        .unwrap_err();
    assert_eq!(*error.code(), ErrorCode::InvalidConfiguration);
    assert!(error.context().get("errors").is_none());
}

#[tokio::test]
async fn warnings_reach_the_hook() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let sink = seen.clone();
    bank_builder(Arc::new(LocalProvider::new()), ConsistencyMode::Strong)
        .saga::<support::transfer::QuietSaga>(SagaOptions::default())
        .on_warning(move |w| sink.lock().unwrap().push(w.message.clone()))
        .build()
        .await
        .unwrap();
    let seen = seen.lock().unwrap();
    assert!(
        seen.iter()
            .any(|m| m.contains("QuietSaga") && m.contains("never completes")),
        "{seen:?}"
    );
}

#[tokio::test]
async fn strict_projection_mode_fails_events_a_projection_doesnt_handle() {
    let provider = Arc::new(LocalProvider::new());
    let domain = bank_builder(provider, ConsistencyMode::Strong)
        .projection(|_| Ok(Arc::new(Totals::default())))
        .validation(ProjectionMode::Strict)
        .build()
        .await
        .unwrap();
    // AccountOpened has no route in Totals.
    let error = domain
        .dispatch(OpenAccount {
            account: AccountId::generate(),
            owner: "Ann".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(*error.code(), ErrorCode::MissingEventHandler);
}

#[tokio::test]
async fn state_stored_aggregates_need_a_repository() {
    let error = TaroDomain::builder()
        .use_provider(Arc::new(LocalProvider::new()))
        .aggregate::<BankAccount>(|a| {
            a.state(|_| Err(FrameworkError::missing_dependency("no repo")))
        })
        .build()
        .await
        .unwrap_err();
    // The factory runs at build, so its own error comes back.
    assert_eq!(*error.code(), ErrorCode::MissingDependency);
}
