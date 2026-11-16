//! The unit of work contract (spec §11.4): one transaction over every store.

use std::sync::Arc;

use serde_json::json;
use tarodomain::adapters::{
    CommandIdempotencyStore, DeadLetterInput, DeadLetterStore, EventStoreAdapter,
    ProcessedEventStore, Provider, SagaSaveInput, SagaStatus, SagaStoreAdapter, SnapshotInput,
    SnapshotStoreAdapter, UnitOfWork,
};
use tarodomain::domain::Metadata;
use tarodomain::error::{ErrorCategory, ErrorCode, FrameworkError, Result};
use tokio::sync::oneshot;

use crate::ProviderFactory;
use crate::fixtures::{aggregate_id, event_id, events, ids, stream_id, uuid};

/// Checks a provider's [`UnitOfWork`] through
/// [`execute`](tarodomain::adapters::UnitOfWork#method.execute): a commit
/// keeps the writes of every store and a rollback discards them all, the
/// work's error comes back unchanged, the unit reads its own writes (a
/// stream's version, a saga, idempotency marks) while readers outside it
/// don't, a nested `execute` joins the active unit, and
/// conflicts inside units (also between two racing units) are
/// `OPTIMISTIC_CONCURRENCY`.
///
/// A provider without units of work (`create_unit_of_work()` returns
/// `None`) has nothing to check, and passes.
///
/// Panics on the first case that fails.
pub async fn unit_of_work_contract(factory: impl ProviderFactory) {
    if factory.create().await.create_unit_of_work().is_none() {
        return;
    }
    commit_keeps_the_writes_of_every_store(factory.create().await).await;
    rollback_discards_the_writes_of_every_store(factory.create().await).await;
    a_unit_reads_its_own_writes_and_others_do_not(factory.create().await).await;
    a_nested_execute_joins(factory.create().await).await;
    a_conflict_inside_a_unit_rolls_it_back(factory.create().await).await;
    racing_units_let_exactly_one_win(factory.create().await).await;
}

const ACCOUNT: &str = "BankAccount";
const HANDLER: &str = "BalanceProjection";
const SAGA: &str = "TransferSaga";

/// One adapter of each store, from one provider.
struct Stores {
    events: Arc<dyn EventStoreAdapter>,
    snapshots: Arc<dyn SnapshotStoreAdapter>,
    sagas: Arc<dyn SagaStoreAdapter>,
    processed: Arc<dyn ProcessedEventStore>,
    commands: Arc<dyn CommandIdempotencyStore>,
    dead_letters: Arc<dyn DeadLetterStore>,
}

/// What [`Stores::write_one_of_each`] leaves behind, as the stores see it.
#[derive(Debug, PartialEq)]
struct Seen {
    stream_version: u64,
    subscriptions: usize,
    snapshots: u64,
    sagas: u64,
    processed_events: u64,
    commands: u64,
    dead_letters: u64,
}

/// What a unit must show of its own writes: what the framework reads back
/// inside a unit (a stream's version, a saga record, an idempotency mark).
///
/// A provider that stages a unit's writes until the commit, as D1 does,
/// can't show the rest (snapshots, subscriptions, dead letters, and counts)
/// before the commit. The framework never reads those back inside a unit, so
/// the contract doesn't ask for it.
#[derive(Debug, PartialEq)]
struct SeenInside {
    stream_version: u64,
    saga: bool,
    processed_event: bool,
    command: bool,
}

impl SeenInside {
    const ONE_OF_EACH: SeenInside = SeenInside {
        stream_version: 1,
        saga: true,
        processed_event: true,
        command: true,
    };
}

impl Seen {
    const NOTHING: Seen = Seen {
        stream_version: 0,
        subscriptions: 0,
        snapshots: 0,
        sagas: 0,
        processed_events: 0,
        commands: 0,
        dead_letters: 0,
    };

    const ONE_OF_EACH: Seen = Seen {
        stream_version: 1,
        subscriptions: 1,
        snapshots: 1,
        sagas: 1,
        processed_events: 1,
        commands: 1,
        dead_letters: 1,
    };
}

impl Stores {
    fn of(provider: &dyn Provider) -> Self {
        Self {
            events: provider.create_event_store_adapter(),
            snapshots: provider.create_snapshot_store_adapter(),
            sagas: provider.create_saga_store_adapter(),
            processed: provider.create_processed_event_store(),
            commands: provider.create_command_idempotency_store(),
            dead_letters: provider.create_dead_letter_store(),
        }
    }

    /// Writes one row to every store, as a strong command with a projection
    /// and a saga would.
    async fn write_one_of_each(&self) -> Result<()> {
        let stream = stream_id(ACCOUNT, 1);
        self.events
            .append_to_stream(&stream, events([event_id(1)]), 0)
            .await?;
        self.events.subscribe(HANDLER, Some(ACCOUNT)).await?;
        let snapshot = SnapshotInput {
            source_uuid: aggregate_id(1),
            source_version: 1,
            source_type: ACCOUNT.into(),
            data: json!({ "balance": 125 }),
            metadata: Metadata::new(),
        };
        self.snapshots.save(snapshot).await?;
        let saga = SagaSaveInput {
            saga_id: uuid("saga", 1),
            saga_type: SAGA.into(),
            correlation_id: uuid("saga", 1),
            state: json!({ "amount": 125 }),
            status: SagaStatus::Running,
            current_step: "startsWhenTransferRequested".into(),
            failure_reason: None,
        };
        self.sagas.save(saga).await?;
        self.processed
            .mark_processed(HANDLER, HANDLER, event_id(1))
            .await?;
        let command = uuid("command", 1);
        self.commands
            .mark_processed(command, "DepositMoney", "DepositHandler")
            .await?;
        let dead_letter = DeadLetterInput {
            handler_name: HANDLER.into(),
            event_id: event_id(1),
            event_type: "MoneyDeposited".into(),
            payload: json!({}),
            error_code: "PROJECTION_HANDLER_FAILED".into(),
            error_message: "boom".into(),
            attempts: 1,
        };
        self.dead_letters.write(dead_letter).await?;
        Ok(())
    }

    /// The unit's own view of what [`write_one_of_each`](Self::write_one_of_each)
    /// wrote.
    async fn seen_inside(&self) -> SeenInside {
        let stream = stream_id(ACCOUNT, 1);
        SeenInside {
            stream_version: self.events.get_stream_version(&stream).await.unwrap(),
            saga: self
                .sagas
                .load(SAGA, uuid("saga", 1))
                .await
                .unwrap()
                .is_some(),
            processed_event: self
                .processed
                .exists(HANDLER, HANDLER, event_id(1))
                .await
                .unwrap(),
            command: self.commands.exists(uuid("command", 1)).await.unwrap(),
        }
    }

    async fn seen(&self) -> Seen {
        let stream = stream_id(ACCOUNT, 1);
        Seen {
            stream_version: self.events.get_stream_version(&stream).await.unwrap(),
            subscriptions: self.events.list_subscriptions().await.unwrap().len(),
            snapshots: (self.snapshots.count(aggregate_id(1), ACCOUNT).await).unwrap(),
            sagas: self.sagas.count(Some(SAGA), None).await.unwrap(),
            processed_events: self.processed.count(HANDLER, None).await.unwrap(),
            commands: self.commands.count(None).await.unwrap(),
            dead_letters: self.dead_letters.count(None).await.unwrap(),
        }
    }
}

fn unit(provider: &dyn Provider) -> Arc<dyn UnitOfWork> {
    let unit = provider.create_unit_of_work();
    unit.expect("the provider created a unit of work before")
}

fn work_failed() -> FrameworkError {
    FrameworkError::custom("WORK_FAILED", ErrorCategory::Domain, "the work failed")
}

async fn commit_keeps_the_writes_of_every_store(provider: Arc<dyn Provider>) {
    let stores = Stores::of(&*provider);
    let unit = unit(&*provider);
    let inside = Arc::clone(&unit)
        .execute(async {
            stores.write_one_of_each().await?;
            Ok(stores.seen_inside().await)
        })
        .await
        .unwrap();
    assert_eq!(inside, SeenInside::ONE_OF_EACH);
    assert!(!unit.is_active(), "the unit ends with the commit");
    assert_eq!(
        stores.seen().await,
        Seen::ONE_OF_EACH,
        "the commit kept every write"
    );
}

async fn rollback_discards_the_writes_of_every_store(provider: Arc<dyn Provider>) {
    let stores = Stores::of(&*provider);
    let unit = unit(&*provider);
    let result = Arc::clone(&unit)
        .execute(async {
            stores.write_one_of_each().await?;
            assert!(unit.is_active(), "the unit is active during its work");
            assert_eq!(stores.seen_inside().await, SeenInside::ONE_OF_EACH);
            Err::<(), _>(work_failed())
        })
        .await;
    let error = result.unwrap_err();
    assert_eq!(
        error.code().as_str(),
        "WORK_FAILED",
        "the work's error, unchanged"
    );
    assert_eq!(error.message(), "the work failed");
    assert!(error.context().is_empty(), "{error:?}");
    assert!(!unit.is_active());
    assert_eq!(
        stores.seen().await,
        Seen::NOTHING,
        "the rollback discarded every write"
    );
}

async fn a_unit_reads_its_own_writes_and_others_do_not(provider: Arc<dyn Provider>) {
    let stores = Stores::of(&*provider);
    let (written, read) = (oneshot::channel(), oneshot::channel());
    let work = unit(&*provider).execute(async {
        stores.write_one_of_each().await?;
        written.0.send(()).unwrap();
        read.1.await.unwrap();
        Ok(stores.seen_inside().await)
    });
    let outside = async {
        written.1.await.unwrap();
        let seen = stores.seen().await;
        read.0.send(()).unwrap();
        seen
    };
    let (inside, outside) = tokio::join!(work, outside);
    assert_eq!(
        inside.unwrap(),
        SeenInside::ONE_OF_EACH,
        "the unit reads its own writes"
    );
    assert_eq!(
        outside,
        Seen::NOTHING,
        "nobody else sees them before the commit"
    );
    assert_eq!(stores.seen().await, Seen::ONE_OF_EACH);
}

async fn a_nested_execute_joins(provider: Arc<dyn Provider>) {
    let stores = Stores::of(&*provider);
    let (outer, inner) = (unit(&*provider), unit(&*provider));
    let result = Arc::clone(&outer)
        .execute(async {
            let joined = Arc::clone(&inner).execute(async {
                stores.write_one_of_each().await?;
                Err::<(), _>(work_failed())
            });
            joined.await
        })
        .await;
    assert_eq!(result.unwrap_err().code().as_str(), "WORK_FAILED");
    assert_eq!(
        stores.seen().await,
        Seen::NOTHING,
        "the outer unit rolled back"
    );

    let result = Arc::clone(&outer)
        .execute(async {
            let joined = Arc::clone(&inner).execute(stores.write_one_of_each());
            joined.await?;
            assert!(!inner.is_active(), "the joined unit never began");
            Ok(stores.seen_inside().await)
        })
        .await;
    assert_eq!(result.unwrap(), SeenInside::ONE_OF_EACH);
    assert_eq!(
        stores.seen().await,
        Seen::ONE_OF_EACH,
        "the outer unit committed"
    );
}

async fn a_conflict_inside_a_unit_rolls_it_back(provider: Arc<dyn Provider>) {
    let stores = Stores::of(&*provider);
    let stream = stream_id(ACCOUNT, 1);
    let first = stores
        .events
        .append_to_stream(&stream, events([event_id(1)]), 0);
    first.await.unwrap();
    let result = unit(&*provider)
        .execute(async {
            let command = uuid("command", 1);
            stores
                .commands
                .mark_processed(command, "DepositMoney", "DepositHandler")
                .await?;
            stores
                .events
                .append_to_stream(&stream, events([event_id(2)]), 0)
                .await
        })
        .await;
    let error = result.unwrap_err();
    assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
    assert!(error.retryable());
    assert_eq!(stores.commands.count(None).await.unwrap(), 0);
    let loaded = stores.events.load_stream(&stream, 0, None).await.unwrap();
    assert_eq!(ids(&loaded), [event_id(1)]);
}

async fn racing_units_let_exactly_one_win(provider: Arc<dyn Provider>) {
    let events_store = provider.create_event_store_adapter();
    let stream = stream_id(ACCOUNT, 1);
    let append = |n| {
        let (store, stream) = (&events_store, &stream);
        unit(&*provider).execute(async move {
            store
                .append_to_stream(stream, events([event_id(n)]), 0)
                .await
        })
    };
    let (a, b) = tokio::join!(append(1), append(2));
    let winner = match (a, b) {
        (Ok(_), Err(error)) => {
            assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
            event_id(1)
        }
        (Err(error), Ok(_)) => {
            assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
            event_id(2)
        }
        other => panic!("racing units: expected one winner, got {other:?}"),
    };
    let loaded = events_store.load_stream(&stream, 0, None).await.unwrap();
    assert_eq!(ids(&loaded), [winner]);
}
