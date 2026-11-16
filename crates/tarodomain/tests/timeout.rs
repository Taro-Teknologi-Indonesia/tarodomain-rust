//! Saga timeouts (spec §19) on the transfer saga, in eventual mode.

mod support;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use chrono::{TimeDelta, Utc};
use tarodomain::adapters::{ConsistencyMode, SagaStatus};
use tarodomain::bus::EventHandler;
use tarodomain::domain::CommandMessage;
use tarodomain::kernel::clock::ManualClock;
use tarodomain::kernel::ids::{Uuid, new_v7};
use tarodomain::saga::{
    ProcessManager, SagaCommandDispatch, SagaEventHandler, SagaTimeoutScheduler,
};

use support::bank::{AccountId, Withdraw};
use support::transfer::{QuietSaga, RequestTransfer, TransferId, TransferSaga};
use support::{Bank, deposit};

const TIMEOUT: Duration = Duration::from_secs(600);

/// A bank where Ann has 100 cents, and a transfer of 30 to Bob that has
/// stalled after the withdrawal: the saga is `RUNNING` with a refund to its
/// name, and Ann has been debited, but nothing has moved on.
async fn stalled() -> (Bank, AccountId, AccountId, Uuid) {
    let bank = Bank::new(ConsistencyMode::Eventual, None);
    let ann = bank.open("Ann").await;
    let bob = bank.open("Bob").await;
    bank.dispatcher.dispatch(deposit(ann, 100)).await.unwrap();
    bank.pump().await.unwrap();

    let saga_id = new_v7();
    let request = RequestTransfer {
        transfer: TransferId::generate(),
        from: ann,
        to: bob,
        cents: 30,
        fail_after: None,
    };
    let message = CommandMessage::new(request).with_correlation_id(saga_id);
    bank.dispatcher.dispatch(message).await.unwrap();

    // A saga whose commands go nowhere, to bring it to the stalled state.
    let stuck = SagaEventHandler::<TransferSaga>::new(
        bank.saga_store.clone(),
        bank.provider_processed(),
        SagaCommandDispatch::discard(),
    )
    .unwrap();
    let published = |n: usize| bank.queue.published.lock().unwrap()[n].clone();
    let after_setup = bank.queue.published.lock().unwrap().len();
    stuck.handle(&published(after_setup - 1)).await.unwrap(); // TransferRequested
    let withdraw = CommandMessage::new(Withdraw {
        account: ann,
        cents: 30,
    })
    .with_correlation_id(saga_id);
    bank.dispatcher.dispatch(withdraw).await.unwrap();
    let last = bank.queue.published.lock().unwrap().len() - 1;
    let withdrawn = published(last);
    stuck.handle(&withdrawn).await.unwrap(); // MoneyWithdrawn
    // The bank's own saga doesn't see these two.
    bank.delivered
        .store(bank.queue.published.lock().unwrap().len(), Ordering::SeqCst);

    assert_eq!(bank.balance(ann).await, 70);
    let record = bank.saga_record(saga_id).await.unwrap();
    assert_eq!(record.status, SagaStatus::Running);
    assert_eq!(record.state["compensations"], serde_json::json!(["refund"]));
    (bank, ann, bob, saga_id)
}

fn scheduler<P: ProcessManager>(
    bank: &Bank,
    handler: Arc<SagaEventHandler<P>>,
    hours_from_now: i64,
) -> SagaTimeoutScheduler<P> {
    let clock = Arc::new(ManualClock::new(
        Utc::now() + TimeDelta::hours(hours_from_now),
    ));
    SagaTimeoutScheduler::new(bank.saga_store.clone(), handler, clock, TIMEOUT)
}

#[tokio::test]
async fn a_timed_out_saga_compensates_and_then_fails() {
    let (bank, ann, bob, saga_id) = stalled().await;

    // Not quiet for long enough yet.
    let early = scheduler(&bank, bank.saga.clone(), 0).run().await.unwrap();
    assert_eq!(early.examined, 0);

    let first = scheduler(&bank, bank.saga.clone(), 1).run().await.unwrap();
    assert_eq!((first.examined, first.handled, first.failed), (1, 1, 0));
    assert_eq!(bank.balance(ann).await, 100, "the refund ran");
    assert_eq!(bank.balance(bob).await, 0);
    let record = bank.saga_record(saga_id).await.unwrap();
    assert_eq!(record.status, SagaStatus::Compensating);
    assert_eq!(record.state["compensations"], serde_json::json!([]));

    // Compensating, and quiet for another timeout: the saga ends.
    let second = scheduler(&bank, bank.saga.clone(), 2).run().await.unwrap();
    assert_eq!((second.examined, second.handled, second.failed), (1, 0, 1));
    let record = bank.saga_record(saga_id).await.unwrap();
    assert_eq!(record.status, SagaStatus::Failed);
    assert_eq!(record.failure_reason.as_deref(), Some("timeout"));
    assert_eq!(bank.balance(ann).await, 100, "and nothing ran again");

    let third = scheduler(&bank, bank.saga.clone(), 3).run().await.unwrap();
    assert_eq!(third.examined, 0, "an ended saga isn't timed out");
}

#[tokio::test]
async fn a_saga_without_a_timeout_handler_is_failed() {
    let bank = Bank::new(ConsistencyMode::Eventual, None);
    let ann = bank.open("Ann").await;
    let bob = bank.open("Bob").await;
    let saga_id = new_v7();
    let request = RequestTransfer {
        transfer: TransferId::generate(),
        from: ann,
        to: bob,
        cents: 5,
        fail_after: None,
    };
    bank.dispatcher
        .dispatch(CommandMessage::new(request).with_correlation_id(saga_id))
        .await
        .unwrap();
    let quiet = Arc::new(
        SagaEventHandler::<QuietSaga>::new(
            bank.saga_store.clone(),
            bank.provider_processed(),
            SagaCommandDispatch::discard(),
        )
        .unwrap(),
    );
    let requested = bank.queue.published.lock().unwrap().last().unwrap().clone();
    quiet.handle(&requested).await.unwrap();
    let record = bank
        .saga_store
        .load("QuietSaga", saga_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, SagaStatus::Running);

    let run = scheduler(&bank, quiet, 1).run().await.unwrap();
    assert_eq!((run.examined, run.handled, run.failed), (1, 0, 1));
    let record = bank
        .saga_store
        .load("QuietSaga", saga_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(record.status, SagaStatus::Failed);
    assert_eq!(record.failure_reason.as_deref(), Some("timeout"));
}

#[tokio::test]
async fn a_crash_while_saving_the_compensating_saga_refunds_once() {
    let (bank, ann, _bob, saga_id) = stalled().await;

    // The refund commits, then the saga save "crashes".
    bank.crash_after_dispatch.store(true, Ordering::SeqCst);
    let crashed = scheduler(&bank, bank.saga.clone(), 1).run().await.unwrap();
    assert_eq!((crashed.handled, crashed.failed), (0, 0));
    assert_eq!(crashed.retry_later.len(), 1);
    assert_eq!(crashed.retry_later[0].0, saga_id);
    assert_eq!(bank.balance(ann).await, 100, "the refund committed");
    assert_eq!(
        bank.saga_record(saga_id).await.unwrap().status,
        SagaStatus::Running,
        "but the saga didn't move"
    );

    // The retry makes the same event and the same refund command.
    let retried = scheduler(&bank, bank.saga.clone(), 1).run().await.unwrap();
    assert_eq!((retried.handled, retried.retry_later.len()), (1, 0));
    assert_eq!(bank.balance(ann).await, 100, "refunded once, not twice");
    assert_eq!(
        bank.saga_record(saga_id).await.unwrap().status,
        SagaStatus::Compensating
    );
}

#[tokio::test]
async fn the_scheduler_handles_at_most_max_count_sagas_of_its_type() {
    let (bank, _ann, _bob, _saga_id) = stalled().await;
    let limited = scheduler(&bank, bank.saga.clone(), 1).with_max_count(0);
    assert_eq!(limited.run().await.unwrap().examined, 0);
}
