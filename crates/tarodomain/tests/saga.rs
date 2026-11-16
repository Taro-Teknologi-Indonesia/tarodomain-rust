//! The transfer saga (spec §13, §17) end to end on the local provider, in
//! both consistency modes.

mod support;

use std::sync::atomic::Ordering;

use tarodomain::adapters::ConsistencyMode::{self, Eventual, Strong};
use tarodomain::adapters::{SagaSaveInput, SagaStatus};
use tarodomain::bus::EventHandler;
use tarodomain::domain::{CommandMessage, DomainEvent, EventEnvelope};
use tarodomain::kernel::ids::{Uuid, new_v7};

use support::Bank;
use support::bank::{AccountId, MoneyDeposited, MoneyWithdrawn};
use support::transfer::{RequestTransfer, TransferId, TransferRequested};

const MODES: [ConsistencyMode; 2] = [Strong, Eventual];

/// A bank with two accounts: Ann with 100 cents, Bob with none.
async fn bank(mode: ConsistencyMode) -> (Bank, AccountId, AccountId) {
    let bank = Bank::new(mode, None);
    let ann = bank.open("Ann").await;
    let bob = bank.open("Bob").await;
    bank.dispatcher
        .dispatch(support::deposit(ann, 100))
        .await
        .unwrap();
    bank.pump().await.unwrap();
    (bank, ann, bob)
}

fn request(
    from: AccountId,
    to: AccountId,
    cents: i64,
    fail_after: Option<&str>,
) -> (Uuid, RequestTransfer) {
    let saga_id = new_v7();
    let command = RequestTransfer {
        transfer: TransferId::generate(),
        from,
        to,
        cents,
        fail_after: fail_after.map(str::to_owned),
    };
    (saga_id, command)
}

async fn run(bank: &Bank, saga_id: Uuid, command: RequestTransfer) {
    let message = CommandMessage::new(command).with_correlation_id(saga_id);
    bank.dispatcher.dispatch(message).await.unwrap();
    bank.pump().await.unwrap();
}

/// The names of the events of one saga, in append order.
async fn trail(bank: &Bank, saga_id: Uuid) -> Vec<String> {
    let events = bank.store.load_by_correlation_id(saga_id).await.unwrap();
    events.iter().map(|e| e.name().to_owned()).collect()
}

#[tokio::test]
async fn the_happy_path_moves_the_money_and_completes() {
    for mode in MODES {
        let (bank, ann, bob) = bank(mode).await;
        let (saga_id, command) = request(ann, bob, 30, None);
        run(&bank, saga_id, command).await;

        assert_eq!(bank.balance(ann).await, 70, "{mode:?}");
        assert_eq!(bank.balance(bob).await, 30, "{mode:?}");
        let record = bank.saga_record(saga_id).await.unwrap();
        assert_eq!(record.status, SagaStatus::Completed);
        assert_eq!(record.current_step, "completedWhenTransferCompleted");
        assert!(record.completed_at.is_some());
        assert_eq!(
            trail(&bank, saga_id).await,
            [
                "TransferRequested",
                "MoneyWithdrawn",
                "MoneyDeposited",
                "TransferCompleted"
            ],
            "{mode:?}"
        );
    }
}

#[tokio::test]
async fn a_rejected_first_step_has_nothing_to_undo() {
    for mode in MODES {
        let (bank, ann, bob) = bank(mode).await;
        let (saga_id, command) = request(ann, bob, 500, None);
        run(&bank, saga_id, command).await;

        assert_eq!(bank.balance(ann).await, 100, "{mode:?}");
        assert_eq!(bank.balance(bob).await, 0, "{mode:?}");
        let record = bank.saga_record(saga_id).await.unwrap();
        assert_eq!(record.status, SagaStatus::Compensating);
        assert_eq!(record.state["compensations"], serde_json::json!([]));
        assert_eq!(
            trail(&bank, saga_id).await,
            ["TransferRequested", "WithdrawalRejected", "TransferFailed"]
        );
    }
}

#[tokio::test]
async fn one_step_of_compensation_refunds_the_withdrawal() {
    for mode in MODES {
        let (bank, ann, bob) = bank(mode).await;
        let (saga_id, command) = request(ann, bob, 30, Some("withdrawal"));
        run(&bank, saga_id, command).await;

        assert_eq!(bank.balance(ann).await, 100, "{mode:?}");
        assert_eq!(bank.balance(bob).await, 0, "{mode:?}");
        let record = bank.saga_record(saga_id).await.unwrap();
        assert_eq!(record.status, SagaStatus::Compensating);
        assert_eq!(
            trail(&bank, saga_id).await,
            [
                "TransferRequested",
                "MoneyWithdrawn",
                "TransferFailed",
                "MoneyDeposited"
            ]
        );
    }
}

#[tokio::test]
async fn two_steps_of_compensation_run_in_reverse_order() {
    for mode in MODES {
        let (bank, ann, bob) = bank(mode).await;
        let (saga_id, command) = request(ann, bob, 30, Some("deposit"));
        run(&bank, saga_id, command).await;

        assert_eq!(bank.balance(ann).await, 100, "{mode:?}");
        assert_eq!(bank.balance(bob).await, 0, "{mode:?}");
        // The deposit is reversed (Bob's withdrawal) before the refund (Ann's deposit).
        assert_eq!(
            trail(&bank, saga_id).await,
            [
                "TransferRequested",
                "MoneyWithdrawn",
                "MoneyDeposited",
                "TransferFailed",
                "MoneyWithdrawn",
                "MoneyDeposited"
            ],
            "{mode:?}"
        );
        let events = bank.store.load_by_correlation_id(saga_id).await.unwrap();
        let reversed = events[4].downcast::<MoneyWithdrawn>().unwrap();
        assert_eq!(reversed.cents, 30);
        assert!(events[5].is::<MoneyDeposited>());
        let record = bank.saga_record(saga_id).await.unwrap();
        assert_eq!(record.state["compensations"], serde_json::json!([]));
    }
}

#[tokio::test]
async fn a_redelivered_event_is_skipped() {
    for mode in MODES {
        let (bank, ann, bob) = bank(mode).await;
        let (saga_id, command) = request(ann, bob, 30, None);
        run(&bank, saga_id, command).await;

        for event in bank.store.load_by_correlation_id(saga_id).await.unwrap() {
            bank.saga.handle(&event).await.unwrap();
        }
        assert_eq!(bank.balance(ann).await, 70, "{mode:?}");
        assert_eq!(bank.balance(bob).await, 30, "{mode:?}");
        assert_eq!(trail(&bank, saga_id).await.len(), 4);
    }
}

#[tokio::test]
async fn a_crash_between_the_commit_and_the_save_moves_the_money_once() {
    let (bank, ann, bob) = bank(Eventual).await;
    let (saga_id, command) = request(ann, bob, 30, None);
    bank.crash_after_dispatch.store(true, Ordering::SeqCst);
    let message = CommandMessage::new(command).with_correlation_id(saga_id);
    bank.dispatcher.dispatch(message).await.unwrap();

    // The first delivery commits the withdrawal, then "crashes" before saving.
    assert!(bank.pump().await.is_err());
    assert_eq!(bank.balance(ann).await, 70);
    assert!(bank.saga_record(saga_id).await.is_none());

    // The redelivery recreates the same command, which is skipped as a duplicate.
    bank.pump().await.unwrap();
    assert_eq!(bank.balance(ann).await, 70);
    assert_eq!(bank.balance(bob).await, 30);
    assert_eq!(
        bank.saga_record(saga_id).await.unwrap().status,
        SagaStatus::Completed
    );
    let withdrawals = trail(&bank, saga_id)
        .await
        .iter()
        .filter(|n| *n == "MoneyWithdrawn")
        .count();
    assert_eq!(withdrawals, 1);
}

#[tokio::test]
async fn a_strong_failure_rolls_back_the_whole_saga() {
    let (bank, ann, bob) = bank(Strong).await;
    let (saga_id, command) = request(ann, bob, 30, None);
    let message = CommandMessage::new(command).with_correlation_id(saga_id);
    bank.crash_after_dispatch.store(true, Ordering::SeqCst);

    assert!(bank.dispatcher.dispatch(message.clone()).await.is_err());
    assert_eq!(bank.balance(ann).await, 100);
    assert_eq!(bank.balance(bob).await, 0);
    assert!(bank.saga_record(saga_id).await.is_none());
    assert!(trail(&bank, saga_id).await.is_empty());

    // The same command runs once the fault is gone.
    bank.dispatcher.dispatch(message).await.unwrap();
    assert_eq!(bank.balance(ann).await, 70);
    assert_eq!(bank.balance(bob).await, 30);
    assert_eq!(
        bank.saga_record(saga_id).await.unwrap().status,
        SagaStatus::Completed
    );
}

fn requested(saga_id: Uuid, from: AccountId, to: AccountId) -> DomainEvent {
    let mut envelope = EventEnvelope::new(chrono::Utc::now());
    envelope.correlation_id = saga_id;
    DomainEvent::new(
        envelope,
        TransferRequested {
            transfer: TransferId::generate(),
            from,
            to,
            cents: 1,
            fail_after: None,
        },
    )
}

async fn save_status(bank: &Bank, saga_id: Uuid, status: SagaStatus) {
    let details = serde_json::json!({
        "details": null,
        "phase": "x",
    });
    bank.saga_store
        .save(SagaSaveInput {
            saga_id,
            saga_type: "TransferSaga".into(),
            correlation_id: saga_id,
            state: serde_json::json!({ "state": details, "compensations": [] }),
            status,
            current_step: "startsWhenTransferRequested".into(),
            failure_reason: None,
        })
        .await
        .unwrap();
}

#[tokio::test]
async fn invalid_transitions_surface_through_the_handler() {
    let (bank, ann, bob) = bank(Strong).await;

    // Not started, and the event can't start a saga: skipped, nothing saved.
    let id = new_v7();
    let mut envelope = EventEnvelope::new(chrono::Utc::now());
    envelope.correlation_id = id;
    let withdrawn = DomainEvent::new(envelope, MoneyWithdrawn { cents: 1 });
    bank.saga.handle(&withdrawn).await.unwrap();
    assert!(bank.saga_record(id).await.is_none());

    // Running + starts_when.
    let running = new_v7();
    save_status(&bank, running, SagaStatus::Running).await;
    let error = bank
        .saga
        .handle(&requested(running, ann, bob))
        .await
        .unwrap_err();
    assert_eq!(error.code().as_str(), "SAGA_ALREADY_STARTED");

    // Compensating + starts_when, which §13.2 leaves out.
    let compensating = new_v7();
    save_status(&bank, compensating, SagaStatus::Compensating).await;
    let error = bank
        .saga
        .handle(&requested(compensating, ann, bob))
        .await
        .unwrap_err();
    assert_eq!(error.code().as_str(), "INVALID_SAGA_TRANSITION");

    // Completed and failed: recorded and skipped, whatever the event.
    for status in [SagaStatus::Completed, SagaStatus::Failed] {
        let ended = new_v7();
        save_status(&bank, ended, status).await;
        let event = requested(ended, ann, bob);
        bank.saga.handle(&event).await.unwrap();
        let store = bank.provider_processed();
        let key = ended.to_string();
        assert!(
            store
                .exists("TransferSaga", &key, event.id())
                .await
                .unwrap()
        );
        assert_eq!(bank.saga_record(ended).await.unwrap().status, status);
    }

    // A running saga ignores an event it has no handler for, and isn't saved.
    let quiet = new_v7();
    save_status(&bank, quiet, SagaStatus::Running).await;
    let before = bank.saga_record(quiet).await.unwrap().updated_at;
    let unknown = DomainEvent::new(
        {
            let mut e = EventEnvelope::new(chrono::Utc::now());
            e.correlation_id = quiet;
            e
        },
        support::bank::AccountOpened { owner: "x".into() },
    );
    bank.saga.handle(&unknown).await.unwrap();
    assert_eq!(bank.saga_record(quiet).await.unwrap().updated_at, before);
}
