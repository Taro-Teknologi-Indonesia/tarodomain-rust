//! The command pipelines (spec §11, §14, §16) on the local provider: a bank
//! whose commands run strong or eventual.

mod support;

use std::sync::atomic::Ordering;
use std::time::Duration;

use tarodomain::adapters::ConsistencyMode::{Eventual, Strong};
use tarodomain::command::DispatchStatus;
use tarodomain::domain::AnyCommand;
use tarodomain::error::ErrorCode;

use support::bank::{CloseAccount, MoneyDeposited};
use support::{Bank, deposit, event_ids};

#[tokio::test]
async fn a_dispatched_command_reports_the_events_it_saved() {
    for mode in [Strong, Eventual] {
        let bank = Bank::new(mode, None);
        let account = bank.open("Ann").await;

        let command = deposit(account, 500);
        let result = bank.dispatcher.dispatch(command.clone()).await.unwrap();

        assert_eq!(result.status, DispatchStatus::Dispatched);
        assert_eq!(result.command_id, command.id());
        assert_eq!(result.events.len(), 1);
        assert!(result.events[0].is::<MoneyDeposited>());
        assert_eq!(bank.balance(account).await, 500);
        assert_eq!(bank.stream_version(account).await, 2);
        assert!(bank.processed(&command).await);
        // The events are the ones stored, in the bus's hands too.
        if mode == Eventual {
            let published = bank.queue.published.lock().unwrap();
            assert_eq!(event_ids(&published)[1..], event_ids(&result.events));
        }
    }
}

#[tokio::test]
async fn an_eventual_conflict_is_retried_on_fresh_state() {
    let bank = Bank::new(Eventual, None);
    let account = bank.open("Ann").await;
    bank.counters.conflicts.store(1, Ordering::SeqCst);
    bank.counters.interference.store(30, Ordering::SeqCst);

    let result = bank
        .dispatcher
        .dispatch(deposit(account, 100))
        .await
        .unwrap();

    assert_eq!(result.status, DispatchStatus::Dispatched);
    assert_eq!(bank.counters.deposit_attempts.load(Ordering::SeqCst), 2);
    // The rival's deposit is stored, and the retry built on it.
    assert_eq!(bank.balance(account).await, 130);
    assert_eq!(bank.stream_version(account).await, 3);
    // Only the successful attempt's events are reported.
    assert_eq!(result.events.len(), 1);
    assert_eq!(bank.sleeper.slept().len(), 1);
}

#[tokio::test]
async fn a_strong_conflict_rolls_the_attempt_back_and_is_retried() {
    let bank = Bank::new(Strong, None);
    let account = bank.open("Ann").await;
    bank.counters.conflicts.store(1, Ordering::SeqCst);
    bank.counters.interference.store(30, Ordering::SeqCst);

    let result = bank
        .dispatcher
        .dispatch(deposit(account, 100))
        .await
        .unwrap();

    assert_eq!(bank.counters.deposit_attempts.load(Ordering::SeqCst), 2);
    // The first attempt's rival deposit went with its rollback.
    assert_eq!(bank.balance(account).await, 100);
    assert_eq!(bank.stream_version(account).await, 2);
    assert_eq!(result.events.len(), 1);
    assert_eq!(bank.read_model_count().await, 2, "the open and the deposit");
    assert_eq!(bank.sleeper.slept().len(), 1);
}

#[tokio::test]
async fn conflicts_give_up_after_three_retries() {
    let bank = Bank::new(Eventual, None);
    let account = bank.open("Ann").await;
    bank.counters.conflicts.store(100, Ordering::SeqCst);
    bank.counters.interference.store(1, Ordering::SeqCst);
    let command = deposit(account, 100);

    let error = bank.dispatcher.dispatch(command.clone()).await.unwrap_err();

    assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
    assert_eq!(bank.counters.deposit_attempts.load(Ordering::SeqCst), 4);
    let slept = bank.sleeper.slept();
    assert_eq!(slept.len(), 3);
    assert!(slept[0] < slept[1] && slept[1] < slept[2]);
    assert!(slept[0] <= Duration::from_millis(11));
    assert!(!bank.processed(&command).await);
}

#[tokio::test]
async fn a_domain_error_is_final() {
    let bank = Bank::new(Strong, None);
    let account = bank.open("Ann").await;

    let error = bank
        .dispatcher
        .dispatch(deposit(account, -5))
        .await
        .unwrap_err();

    assert_eq!(error.code().as_str(), "NEGATIVE_DEPOSIT");
    assert_eq!(bank.counters.deposit_attempts.load(Ordering::SeqCst), 1);
    assert!(bank.sleeper.slept().is_empty());
}

#[tokio::test]
async fn a_failing_projection_rolls_back_events_and_the_idempotency_record() {
    let bank = Bank::new(Strong, None);
    let account = bank.open("Ann").await;
    let command = deposit(account, 100);
    bank.read_model.fail.store(true, Ordering::SeqCst);

    let error = bank.dispatcher.dispatch(command.clone()).await.unwrap_err();

    assert_eq!(*error.code(), ErrorCode::ProjectionHandlerFailed);
    assert_eq!(
        bank.stream_version(account).await,
        1,
        "the append is undone"
    );
    assert_eq!(
        bank.read_model_count().await,
        1,
        "so is the read model's write"
    );
    assert!(!bank.processed(&command).await);

    // Nothing was left behind, so the same command can run once it recovers.
    bank.read_model.fail.store(false, Ordering::SeqCst);
    let result = bank.dispatcher.dispatch(command.clone()).await.unwrap();
    assert_eq!(result.status, DispatchStatus::Dispatched);
    assert_eq!(bank.balance(account).await, 100);
    assert!(bank.processed(&command).await);
}

#[tokio::test]
async fn a_duplicate_command_id_does_nothing() {
    for mode in [Strong, Eventual] {
        let bank = Bank::new(mode, None);
        let account = bank.open("Ann").await;
        let command = deposit(account, 100);

        let first = bank.dispatcher.dispatch(command.clone()).await.unwrap();
        let second = bank.dispatcher.dispatch(command).await.unwrap();

        assert_eq!(first.status, DispatchStatus::Dispatched);
        assert!(second.is_duplicate());
        assert!(second.events.is_empty());
        assert_eq!(second.command_id, first.command_id);
        assert_eq!(bank.counters.deposit_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(bank.balance(account).await, 100);
    }
}

#[tokio::test]
async fn an_eventual_append_survives_a_failing_bus() {
    let bank = Bank::new(Eventual, None);
    let account = bank.open("Ann").await;
    bank.queue.fail.store(true, Ordering::SeqCst);
    let command = deposit(account, 100);

    let result = bank.dispatcher.dispatch(command.clone()).await.unwrap();

    assert_eq!(result.status, DispatchStatus::Dispatched);
    assert_eq!(result.events.len(), 1, "the stored event is still reported");
    assert!(bank.queue.calls.load(Ordering::SeqCst) >= 2);
    assert_eq!(bank.stream_version(account).await, 2);
    assert_eq!(bank.balance(account).await, 100);
    assert!(bank.processed(&command).await);
    // The catch-up job delivers it later: the queue never took it.
    assert_eq!(
        bank.queue.published.lock().unwrap().len(),
        1,
        "only the open"
    );
}

#[tokio::test]
async fn a_command_can_ask_for_the_other_pipeline() {
    // Commands default to strong, but deposits are eventual (§11.3).
    let bank = Bank::new(Strong, Some(Eventual));
    let account = bank.open("Ann").await;
    bank.read_model.fail.store(true, Ordering::SeqCst);

    let result = bank
        .dispatcher
        .dispatch(deposit(account, 100))
        .await
        .unwrap();

    // The strong read model never saw it, so its failure didn't matter.
    assert_eq!(result.events.len(), 1);
    assert_eq!(bank.read_model_count().await, 1);
    assert_eq!(bank.queue.published.lock().unwrap().len(), 1);
    assert_eq!(bank.balance(account).await, 100);
}

#[tokio::test]
async fn an_unknown_command_has_no_handler() {
    let bank = Bank::new(Strong, None);
    assert!(bank.dispatcher.can_dispatch("Deposit"));
    assert!(!bank.dispatcher.can_dispatch("CloseAccount"));

    let error = bank.dispatcher.dispatch(CloseAccount).await.unwrap_err();
    assert_eq!(*error.code(), ErrorCode::CommandHandlerNotRegistered);
}

#[tokio::test]
async fn a_batch_stops_at_the_first_failure_unless_told_to_continue() {
    for mode in [Strong, Eventual] {
        let bank = Bank::new(mode, None);
        let account = bank.open("Ann").await;
        let batch = || -> Vec<AnyCommand> {
            vec![
                deposit(account, 100),
                deposit(support::bank::AccountId::generate(), 5),
                deposit(account, 7),
            ]
        };

        let stopped = batch();
        let third = stopped[2].id();
        let result = bank.dispatcher.dispatch_all(stopped, false).await;
        assert_eq!(result.outcomes.len(), 2);
        assert_eq!(result.skipped, [third]);
        assert!(!result.is_success());
        let failure = result.failures().next().unwrap();
        assert_eq!(failure.command_name, "Deposit");
        assert_eq!(
            *failure.error().unwrap().code(),
            ErrorCode::AggregateNotFound
        );
        assert_eq!(bank.balance(account).await, 100);

        let result = bank.dispatcher.dispatch_all(batch(), true).await;
        assert_eq!(result.outcomes.len(), 3);
        assert!(result.skipped.is_empty());
        assert_eq!(result.failures().count(), 1);
        assert_eq!(result.successes().count(), 2);
        assert_eq!(bank.balance(account).await, 100 + 100 + 7);
    }
}
