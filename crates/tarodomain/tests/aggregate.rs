//! The aggregate root (spec §5), on a bank account declared from outside the
//! crate, as a domain crate would.

use std::sync::Arc;

use chrono::DateTime;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tarodomain::domain::{
    Aggregate, AggregateCore, DomainEvent, Event, EventEnvelope, ExecutionContext, Metadata,
};
use tarodomain::error::{ErrorCategory, ErrorCode, FrameworkError, Result};
use tarodomain::kernel::clock::{self, ManualClock};
use tarodomain::kernel::ids::new_v7;
use tarodomain::{aggregate_id, event_family};

aggregate_id! {
    /// Identifies a bank account.
    pub AccountId
}

/// A bank account was opened.
#[derive(Debug, Serialize, Deserialize)]
pub struct AccountOpened {
    owner: String,
}

impl Event for AccountOpened {
    const NAME: &'static str = "AccountOpened";
}

/// Money was paid in.
#[derive(Debug, Serialize, Deserialize)]
pub struct MoneyDeposited {
    cents: i64,
}

impl Event for MoneyDeposited {
    const NAME: &'static str = "MoneyDeposited";
}

/// Money was paid out.
#[derive(Debug, Serialize, Deserialize)]
pub struct MoneyWithdrawn {
    cents: i64,
}

impl Event for MoneyWithdrawn {
    const NAME: &'static str = "MoneyWithdrawn";
}

/// An event of another aggregate.
#[derive(Debug, Serialize, Deserialize)]
pub struct TransferRequested {
    cents: i64,
}

impl Event for TransferRequested {
    const NAME: &'static str = "TransferRequested";
}

event_family! {
    /// Everything that happens to a bank account.
    pub enum AccountEvent {
        AccountOpened,
        MoneyDeposited,
        MoneyWithdrawn,
    }
}

#[derive(Debug)]
struct BankAccount {
    id: AccountId,
    core: AggregateCore,
    owner: String,
    balance: i64,
}

#[derive(Serialize, Deserialize)]
struct BankAccountState {
    owner: String,
    balance: i64,
}

impl BankAccount {
    fn open(id: AccountId, owner: &str) -> Self {
        let mut account = Self::new(id);
        account.raise(AccountOpened {
            owner: owner.to_owned(),
        });
        account
    }

    fn deposit(&mut self, cents: i64) {
        self.raise(MoneyDeposited { cents });
    }

    fn withdraw(&mut self, cents: i64) -> Result<()> {
        if cents > self.balance {
            return Err(FrameworkError::custom(
                "INSUFFICIENT_FUNDS",
                ErrorCategory::Domain,
                "balance too low",
            ));
        }
        self.raise(MoneyWithdrawn { cents });
        Ok(())
    }
}

impl Aggregate for BankAccount {
    type Id = AccountId;
    type Event = AccountEvent;
    type Snapshot = BankAccountState;
    const TYPE: &'static str = "BankAccount";

    fn new(id: AccountId) -> Self {
        Self {
            id,
            core: AggregateCore::new(),
            owner: String::new(),
            balance: 0,
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

    fn apply(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::AccountOpened(e) => self.owner.clone_from(&e.owner),
            AccountEvent::MoneyDeposited(e) => self.balance += e.cents,
            AccountEvent::MoneyWithdrawn(e) => self.balance -= e.cents,
        }
    }

    fn snapshot_state(&self) -> BankAccountState {
        BankAccountState {
            owner: self.owner.clone(),
            balance: self.balance,
        }
    }

    fn restore_snapshot_state(&mut self, state: BankAccountState) {
        self.owner = state.owner;
        self.balance = state.balance;
    }
}

/// An account with three stored events: opened, +100, -30.
fn history() -> (AccountId, Vec<DomainEvent>) {
    let id = AccountId::generate();
    let mut account = BankAccount::open(id, "ana");
    account.deposit(100);
    account.withdraw(30).unwrap();
    (id, account.uncommitted_changes().to_vec())
}

fn context() -> ExecutionContext {
    ExecutionContext {
        causation_id: Some(new_v7()),
        command_causation_id: Some(new_v7()),
        metadata: Metadata::from([("userId", "u-1")]),
        ..ExecutionContext::new(new_v7())
    }
}

#[test]
fn version_arithmetic() {
    let mut account = BankAccount::open(AccountId::generate(), "ana");
    assert_eq!((account.version(), account.committed_version()), (1, 0));
    assert!(account.has_uncommitted_changes());

    account.deposit(100);
    assert_eq!((account.version(), account.committed_version()), (2, 0));

    account.mark_changes_as_committed();
    assert_eq!((account.version(), account.committed_version()), (2, 2));
    assert!(!account.has_uncommitted_changes());

    account.withdraw(50).unwrap();
    assert_eq!((account.version(), account.committed_version()), (3, 2));
    assert_eq!(account.balance, 50);
}

#[test]
fn a_rejected_change_raises_nothing() {
    let mut account = BankAccount::open(AccountId::generate(), "ana");
    let error = account.withdraw(1).unwrap_err();
    assert_eq!(error.code().as_str(), "INSUFFICIENT_FUNDS");
    assert_eq!(account.version(), 1);
    assert_eq!(account.uncommitted_changes().len(), 1);
}

#[test]
fn raise_queues_typed_events_in_order() {
    let (_, events) = history();
    let names: Vec<_> = events.iter().map(DomainEvent::name).collect();
    assert_eq!(names, ["AccountOpened", "MoneyDeposited", "MoneyWithdrawn"]);
    assert_eq!(
        events[1].downcast_ref::<MoneyDeposited>().unwrap().cents,
        100
    );
}

#[test]
fn raise_inside_a_context_carries_its_ids() {
    let ctx = context();
    let account = ctx
        .clone()
        .sync_scope(|| BankAccount::open(AccountId::generate(), "ana"));
    let envelope = account.uncommitted_changes()[0].envelope();
    assert_eq!(envelope.correlation_id, ctx.correlation_id);
    assert_eq!(envelope.causation_id, ctx.causation_id);
    assert_eq!(envelope.command_causation_id, ctx.command_causation_id);
    assert_eq!(envelope.metadata, ctx.metadata);
}

#[test]
fn the_aggregates_own_context_comes_first() {
    let (own, ambient) = (context(), context());
    let mut account = BankAccount::new(AccountId::generate());
    account.set_execution_context(own.clone());
    ambient.clone().sync_scope(|| account.deposit(5));
    account.clear_execution_context();
    ambient.clone().sync_scope(|| account.deposit(6));

    let events = account.uncommitted_changes();
    assert_eq!(events[0].envelope().correlation_id, own.correlation_id);
    assert_eq!(events[0].envelope().causation_id, own.causation_id);
    assert_eq!(events[1].envelope().correlation_id, ambient.correlation_id);
}

#[test]
fn raise_outside_a_context_starts_a_root() {
    let account = BankAccount::open(AccountId::generate(), "ana");
    let envelope = account.uncommitted_changes()[0].envelope();
    assert_eq!(envelope.causation_id, None);
    assert_eq!(envelope.command_causation_id, None);
    assert!(envelope.metadata.is_empty());
    assert_ne!(envelope.correlation_id, envelope.id);
}

#[test]
fn raise_takes_the_time_from_the_clock_in_scope() {
    let at = DateTime::from_timestamp_millis(1_763_280_000_123).unwrap();
    let account = clock::sync_scope(Arc::new(ManualClock::new(at)), || {
        BankAccount::open(AccountId::generate(), "ana")
    });
    assert_eq!(account.uncommitted_changes()[0].envelope().occurred_at, at);
}

#[test]
fn history_replay_does_not_queue() {
    let (id, events) = history();
    let mut account = BankAccount::new(id);
    account.load_from_history(&events).unwrap();
    assert_eq!((account.owner.as_str(), account.balance), ("ana", 70));
    assert_eq!((account.version(), account.committed_version()), (3, 3));
    assert!(!account.has_uncommitted_changes());
}

#[test]
fn an_event_outside_the_family_is_missing_a_handler() {
    let at = DateTime::from_timestamp_millis(0).unwrap();
    let outsider = DomainEvent::new(
        EventEnvelope::from_context(None, at),
        TransferRequested { cents: 1 },
    );
    let mut account = BankAccount::new(AccountId::generate());
    let error = account.load_from_history([outsider.clone()]).unwrap_err();
    assert_eq!(*error.code(), ErrorCode::MissingEventHandler);
    assert_eq!(error.context()["aggregateType"], "BankAccount");
    assert_eq!(error.context()["eventType"], "TransferRequested");
    assert_eq!(error.context()["eventId"], outsider.id().to_string());
}

#[test]
fn replay_and_restore_refuse_uncommitted_changes() {
    let (_, events) = history();
    let mut account = BankAccount::open(AccountId::generate(), "ana");
    let errors = [
        account.load_from_history(&events).unwrap_err(),
        account
            .restore_snapshot(json!({"owner": "x", "balance": 1}), 9)
            .unwrap_err(),
        account.restore_version(9).unwrap_err(),
    ];
    for error in errors {
        assert_eq!(*error.code(), ErrorCode::InvalidAggregateState);
    }
    assert_eq!((account.version(), account.balance), (1, 0));
}

#[test]
fn snapshot_round_trip() {
    let (id, events) = history();
    let mut original = BankAccount::new(id);
    original.load_from_history(&events[..2]).unwrap();
    let snapshot = original.to_snapshot().unwrap();
    assert_eq!(snapshot.data, json!({"owner": "ana", "balance": 100}));
    assert_eq!(snapshot.version, 2);

    let mut restored = BankAccount::new(id);
    restored
        .restore_snapshot(snapshot.data, snapshot.version)
        .unwrap();
    assert_eq!((restored.balance, restored.version()), (100, 2));

    restored.load_from_history(&events[2..]).unwrap();
    assert_eq!(
        (restored.balance, restored.version()),
        (70, 3),
        "replay continues"
    );
}

#[test]
fn a_snapshot_that_does_not_fit_is_a_serialization_error() {
    let mut account = BankAccount::new(AccountId::generate());
    let error = account
        .restore_snapshot(json!({"balance": "lots"}), 4)
        .unwrap_err();
    assert_eq!(*error.code(), ErrorCode::SerializationError);
    assert_eq!(error.context()["aggregateType"], "BankAccount");
    assert_eq!((account.version(), account.balance), (0, 0), "unchanged");
}

#[test]
fn restore_version_sets_the_version() {
    let mut account = BankAccount::new(AccountId::generate());
    account.restore_version(7).unwrap();
    account.deposit(1);
    assert_eq!((account.version(), account.committed_version()), (8, 7));
}
