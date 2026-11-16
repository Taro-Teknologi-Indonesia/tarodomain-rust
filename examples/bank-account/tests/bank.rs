//! The bank scenario in both consistency modes: transfers, failures, a
//! projection rebuild and a stalled transfer that the timeout undoes.

use std::sync::Arc;

use bank_account::TRANSFER_TIMEOUT;
use bank_account::account::{AccountId, Deposit, OpenAccount, Withdraw};
use bank_account::projection::{BALANCES, BalanceProjection, BalanceStore, InMemoryBalanceStore};
use bank_account::saga::TransferSaga;
use bank_account::transfer::{MoneyTransfer, RequestTransfer, TransferId, TransferStatus};
use bank_account::{register_bank, transfer::TransferEvent};
use chrono::{TimeDelta, TimeZone, Utc};
use tarodomain::adapters::{ConsistencyMode, Provider, SagaStatus};
use tarodomain::bus::EventHandler;
use tarodomain::domain::{CommandMessage, EventFamily};
use tarodomain::framework::TaroDomain;
use tarodomain::kernel::clock::{Clock, ManualClock};
use tarodomain::kernel::ids::{Uuid, new_v7};
use tarodomain::projection::ProjectionHandler;
use tarodomain::rebuild::{ProjectionRebuilder, RebuildOptions};
use tarodomain::repository::Repository;
use tarodomain::saga::{ProcessManager, SagaCommandDispatch, SagaEventHandler};
use tarodomain::trace::CorrelationTrace;
use tarodomain_local::{LocalDatabase, LocalProvider};

struct Bank {
    domain: TaroDomain,
    balances: Arc<InMemoryBalanceStore>,
    provider: Arc<LocalProvider>,
    clock: Arc<ManualClock>,
}

async fn bank(mode: ConsistencyMode) -> Bank {
    let clock = Arc::new(ManualClock::new(
        Utc.timestamp_opt(1_700_000_000, 0).unwrap(),
    ));
    let provider = Arc::new(LocalProvider::with_database(LocalDatabase::with_clock(
        clock.clone(),
    )));
    let balances = Arc::new(InMemoryBalanceStore::new());
    let builder = TaroDomain::builder()
        .use_provider(provider.clone())
        .consistency(mode, None);
    let domain = register_bank(builder, balances.clone())
        .build()
        .await
        .unwrap();
    Bank {
        domain,
        balances,
        provider,
        clock,
    }
}

impl Bank {
    /// Eventual mode: runs the scheduled catch-up until nothing is left to
    /// deliver. (In strong mode there is nothing to deliver.)
    async fn settle(&self) {
        for _ in 0..30 {
            let run = self.domain.run_scheduled().await;
            assert!(!run.has_errors(), "{run:?}");
            if run
                .catch_up
                .iter()
                .all(|(_, r)| r.as_ref().unwrap().processed == 0)
            {
                return;
            }
        }
        panic!("the bank never settled");
    }

    async fn open(&self, owner: &str) -> AccountId {
        let account = AccountId::generate();
        let command = OpenAccount {
            account,
            owner: owner.into(),
        };
        self.domain.dispatch(command).await.unwrap();
        account
    }

    async fn funded(&self, owner: &str, cents: i64) -> AccountId {
        let account = self.open(owner).await;
        self.domain
            .dispatch(Deposit { account, cents })
            .await
            .unwrap();
        account
    }

    async fn transfer(&self, from: AccountId, to: AccountId, cents: i64) -> (Uuid, TransferId) {
        let (correlation, transfer) = (new_v7(), TransferId::generate());
        let request = RequestTransfer {
            transfer,
            from,
            to,
            cents,
        };
        let message = CommandMessage::new(request).with_correlation_id(correlation);
        self.domain.dispatch(message).await.unwrap();
        (correlation, transfer)
    }

    async fn cents(&self, account: AccountId) -> i64 {
        self.balances
            .get(account)
            .await
            .unwrap()
            .map_or(0, |row| row.cents)
    }

    async fn saga(&self, id: Uuid) -> tarodomain::adapters::SagaRecord {
        let store = self.provider.create_saga_store_adapter();
        store
            .load(TransferSaga::SAGA_TYPE, id)
            .await
            .unwrap()
            .expect("a saga")
    }

    async fn transfer_status(&self, transfer: TransferId) -> TransferStatus {
        let repository = tarodomain::repository::EventSourcedRepository::<MoneyTransfer>::new(
            self.domain.event_store().clone(),
            self.domain.strong_event_bus().clone(),
        );
        repository.load(transfer).await.unwrap().status
    }
}

const MODES: [ConsistencyMode; 2] = [ConsistencyMode::Strong, ConsistencyMode::Eventual];

#[tokio::test]
async fn a_transfer_moves_the_money_in_both_modes() {
    for mode in MODES {
        let bank = bank(mode).await;
        let ann = bank.funded("Ann", 10_000).await;
        let bob = bank.open("Bob").await;
        let (correlation, transfer) = bank.transfer(ann, bob, 3_000).await;
        bank.settle().await;

        assert_eq!(
            (bank.cents(ann).await, bank.cents(bob).await),
            (7_000, 3_000),
            "{mode}"
        );
        let saga = bank.saga(correlation).await;
        assert_eq!(saga.status, SagaStatus::Completed, "{mode}");
        assert_eq!(saga.state["state"]["phase"], "completed");
        assert_eq!(
            bank.transfer_status(transfer).await,
            TransferStatus::Completed
        );
        let owner = bank.balances.get(ann).await.unwrap().unwrap().owner;
        assert_eq!(owner, "Ann");
    }
}

#[tokio::test]
async fn a_transfer_traces_as_one_chain_in_both_modes() {
    for mode in MODES {
        let bank = bank(mode).await;
        let ann = bank.funded("Ann", 500).await;
        let bob = bank.open("Bob").await;
        let (correlation, _) = bank.transfer(ann, bob, 200).await;
        bank.settle().await;

        let adapter = bank.domain.event_store().adapter().clone();
        let entries = CorrelationTrace::trace(adapter.as_ref(), correlation)
            .await
            .unwrap();
        let types: Vec<_> = entries.iter().map(|e| e.event_type.as_str()).collect();
        assert_eq!(
            types,
            [
                "TransferRequested",
                "MoneyWithdrawn",
                "MoneyDeposited",
                "TransferCompleted"
            ],
            "{mode}"
        );
        let tree = CorrelationTrace::build_tree(&entries);
        assert_eq!(
            tree.roots.len(),
            1,
            "{mode}: {}",
            CorrelationTrace::format_tree(&tree)
        );
        let text = CorrelationTrace::format_tree(&tree);
        assert!(text.contains("event TransferCompleted"), "{text}");
    }
}

#[tokio::test]
async fn a_transfer_without_funds_fails_and_moves_nothing() {
    for mode in MODES {
        let bank = bank(mode).await;
        let ann = bank.funded("Ann", 1_000).await;
        let bob = bank.open("Bob").await;
        let (correlation, transfer) = bank.transfer(ann, bob, 3_000).await;
        bank.settle().await;

        assert_eq!(
            (bank.cents(ann).await, bank.cents(bob).await),
            (1_000, 0),
            "{mode}"
        );
        let saga = bank.saga(correlation).await;
        assert_eq!(saga.status, SagaStatus::Completed, "{mode}");
        assert_eq!(saga.state["state"]["phase"], "rolled_back");
        assert_eq!(bank.transfer_status(transfer).await, TransferStatus::Failed);
    }
}

#[tokio::test]
async fn a_strict_withdrawal_is_a_domain_error() {
    let bank = bank(ConsistencyMode::Strong).await;
    let ann = bank.funded("Ann", 100).await;
    let command = Withdraw {
        account: ann,
        cents: 500,
        strict: true,
    };
    let error = bank.domain.dispatch(command).await.unwrap_err();
    assert_eq!(error.code().as_str(), "INSUFFICIENT_FUNDS");
    assert_eq!(error.http_status(), 400);
    assert_eq!(bank.cents(ann).await, 100);
}

#[tokio::test]
async fn opening_an_account_twice_is_refused() {
    let bank = bank(ConsistencyMode::Strong).await;
    let ann = bank.open("Ann").await;
    let again = OpenAccount {
        account: ann,
        owner: "Impostor".into(),
    };
    let error = bank.domain.dispatch(again).await.unwrap_err();
    assert_eq!(error.code().as_str(), "ACCOUNT_ALREADY_EXISTS");
    let owner = bank.balances.get(ann).await.unwrap().unwrap().owner;
    assert_eq!(owner, "Ann");
}

#[tokio::test]
async fn the_projection_is_rebuilt_from_the_events() {
    for mode in MODES {
        let bank = bank(mode).await;
        let ann = bank.funded("Ann", 10_000).await;
        let bob = bank.open("Bob").await;
        bank.transfer(ann, bob, 2_500).await;
        bank.settle().await;
        let before = bank.balances.list().await.unwrap();

        // The read model drifts, then is rebuilt without any new events.
        bank.balances.apply(new_v7(), ann, 999_999).await.unwrap();
        assert_ne!(bank.balances.list().await.unwrap(), before);
        let events_before = bank
            .domain
            .event_store()
            .load_all(None, None)
            .await
            .unwrap()
            .len();

        let handler = ProjectionHandler::new(
            Arc::new(BalanceProjection::new(bank.balances.clone())),
            bank.provider.create_processed_event_store(),
        )
        .unwrap();
        assert_eq!(handler.record_name(), BALANCES);
        let rebuilder = ProjectionRebuilder::new(
            bank.domain.cron().clone(),
            bank.provider.create_processed_event_store(),
        );
        let result = rebuilder
            .rebuild(&handler, &RebuildOptions::default())
            .await
            .unwrap();
        assert!(result.completed, "{mode}: {result:?}");

        assert_eq!(bank.balances.list().await.unwrap(), before, "{mode}");
        let events_after = bank
            .domain
            .event_store()
            .load_all(None, None)
            .await
            .unwrap()
            .len();
        assert_eq!(events_before, events_after, "a rebuild raises no events");
    }
}

#[tokio::test]
async fn a_stalled_transfer_is_refunded_by_the_timeout() {
    let bank = bank(ConsistencyMode::Eventual).await;
    let ann = bank.funded("Ann", 10_000).await;
    let bob = bank.open("Bob").await;
    bank.settle().await;

    // Bring the transfer to where a lost message leaves it: the withdrawal has
    // happened and the saga has registered its refund, but the deposit never
    // ran. A saga whose commands go nowhere stands in for the lost step.
    let (correlation, transfer) = (new_v7(), TransferId::generate());
    let request = RequestTransfer {
        transfer,
        from: ann,
        to: bob,
        cents: 3_000,
    };
    let requested = bank
        .domain
        .dispatch(CommandMessage::new(request).with_correlation_id(correlation))
        .await
        .unwrap();
    let stuck = SagaEventHandler::<TransferSaga>::new(
        bank.provider.create_saga_store_adapter(),
        bank.provider.create_processed_event_store(),
        SagaCommandDispatch::discard(),
    )
    .unwrap();
    stuck.handle(&requested.events[0]).await.unwrap();
    let withdraw = Withdraw {
        account: ann,
        cents: 3_000,
        strict: false,
    };
    let withdrawn = bank
        .domain
        .dispatch(CommandMessage::new(withdraw).with_correlation_id(correlation))
        .await
        .unwrap();
    stuck.handle(&withdrawn.events[0]).await.unwrap();
    assert_eq!(bank.saga(correlation).await.status, SagaStatus::Running);

    // Not yet due.
    let early = bank.domain.run_timeouts().await;
    assert_eq!(early[0].1.as_ref().unwrap().examined, 0);

    bank.clock.set(
        bank.clock.now() + TimeDelta::from_std(TRANSFER_TIMEOUT).unwrap() + TimeDelta::seconds(1),
    );
    let late = bank.domain.run_timeouts().await;
    assert_eq!(late[0].1.as_ref().unwrap().handled, 1, "{late:?}");
    bank.settle().await;

    assert_eq!((bank.cents(ann).await, bank.cents(bob).await), (10_000, 0));
    let saga = bank.saga(correlation).await;
    assert_eq!(saga.status, SagaStatus::Completed);
    assert_eq!(saga.state["state"]["phase"], "timed_out");
    assert_eq!(bank.transfer_status(transfer).await, TransferStatus::Failed);
}

#[tokio::test]
async fn every_event_the_bank_raises_is_registered() {
    let bank = bank(ConsistencyMode::Strong).await;
    let registry = bank.domain.event_store().registry();
    for name in TransferEvent::names()
        .iter()
        .chain(bank_account::account::AccountEvent::names())
    {
        assert!(registry.knows(name, "1.0.0"), "{name}");
    }
}
