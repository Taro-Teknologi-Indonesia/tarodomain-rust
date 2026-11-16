//! A small bank: one aggregate, three commands and their handlers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use serde::{Deserialize, Serialize};
use tarodomain::command::{CommandContext, CommandHandler};
use tarodomain::domain::{Aggregate, AggregateCore, Command, CommandMessage, Event};
use tarodomain::error::{ErrorCategory, FrameworkError, Result};
use tarodomain::{aggregate_id, async_trait, event_family};

aggregate_id! {
    /// Identifies a bank account.
    pub AccountId
}

/// A bank account was opened.
#[derive(Debug, Serialize, Deserialize)]
pub struct AccountOpened {
    pub owner: String,
}

impl Event for AccountOpened {
    const NAME: &'static str = "AccountOpened";
}

/// Money was paid in.
#[derive(Debug, Serialize, Deserialize)]
pub struct MoneyDeposited {
    pub cents: i64,
}

impl Event for MoneyDeposited {
    const NAME: &'static str = "MoneyDeposited";
}

/// Money was taken out.
#[derive(Debug, Serialize, Deserialize)]
pub struct MoneyWithdrawn {
    pub cents: i64,
}

impl Event for MoneyWithdrawn {
    const NAME: &'static str = "MoneyWithdrawn";
}

/// A withdrawal was refused because the funds are short.
#[derive(Debug, Serialize, Deserialize)]
pub struct WithdrawalRejected {
    pub cents: i64,
}

impl Event for WithdrawalRejected {
    const NAME: &'static str = "WithdrawalRejected";
}

event_family!(
    /// Everything that happens to a bank account.
    pub enum AccountEvent {
        AccountOpened,
        MoneyDeposited,
        MoneyWithdrawn,
        WithdrawalRejected,
    }
);

/// The account's business state, which is also its snapshot.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AccountState {
    pub owner: String,
    pub balance: i64,
}

/// A bank account.
#[derive(Debug)]
pub struct BankAccount {
    id: AccountId,
    core: AggregateCore,
    pub state: AccountState,
}

impl Aggregate for BankAccount {
    type Id = AccountId;
    type Event = AccountEvent;
    type Snapshot = AccountState;
    const TYPE: &'static str = "BankAccount";

    fn new(id: AccountId) -> Self {
        Self {
            id,
            core: AggregateCore::new(),
            state: AccountState::default(),
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
            AccountEvent::AccountOpened(e) => self.state.owner = e.owner.clone(),
            AccountEvent::MoneyDeposited(e) => self.state.balance += e.cents,
            AccountEvent::MoneyWithdrawn(e) => self.state.balance -= e.cents,
            AccountEvent::WithdrawalRejected(_) => {}
        }
    }

    fn snapshot_state(&self) -> AccountState {
        self.state.clone()
    }

    fn restore_snapshot_state(&mut self, state: AccountState) {
        self.state = state;
    }
}

/// Opens an account.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenAccount {
    pub account: AccountId,
    pub owner: String,
}

impl Command for OpenAccount {
    const NAME: &'static str = "OpenAccount";
}

/// Pays money into an account.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Deposit {
    pub account: AccountId,
    pub cents: i64,
}

impl Command for Deposit {
    const NAME: &'static str = "Deposit";
}

/// Takes money out of an account, or records a rejection if funds are short.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Withdraw {
    pub account: AccountId,
    pub cents: i64,
}

impl Command for Withdraw {
    const NAME: &'static str = "Withdraw";
}

/// A command nothing handles.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CloseAccount;

impl Command for CloseAccount {
    const NAME: &'static str = "CloseAccount";
}

/// What the handlers did, shared with the test.
#[derive(Default)]
pub struct Counters {
    /// How many times a deposit handler ran, retries included.
    pub deposit_attempts: AtomicU32,
    /// How many of the next deposit attempts lose a race to another deposit
    /// of `interference` cents, made just before they save.
    pub conflicts: AtomicU32,
    /// The size of the competing deposit.
    pub interference: AtomicU32,
}

/// Opens the account.
pub struct OpenAccountHandler {
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<OpenAccount> for OpenAccountHandler {
    async fn handle(&self, command: &CommandMessage<OpenAccount>) -> Result<()> {
        let repository = self.context.repository::<BankAccount>()?;
        let mut account = BankAccount::new(command.account);
        account.raise(AccountOpened {
            owner: command.owner.clone(),
        });
        repository.save(&mut account).await
    }
}

/// Deposits money, and can be made to lose a race (see [`Counters`]).
pub struct DepositHandler {
    pub context: CommandContext,
    pub counters: Arc<Counters>,
}

#[async_trait]
impl CommandHandler<Deposit> for DepositHandler {
    async fn handle(&self, command: &CommandMessage<Deposit>) -> Result<()> {
        self.counters
            .deposit_attempts
            .fetch_add(1, Ordering::SeqCst);
        let repository = self.context.repository::<BankAccount>()?;
        let mut account = repository.load(command.account).await?;
        if command.cents < 0 {
            return Err(FrameworkError::custom(
                "NEGATIVE_DEPOSIT",
                ErrorCategory::Domain,
                "a deposit can't be negative",
            ));
        }
        let counter = &self.counters.conflicts;
        // Each test dispatches one command at a time, so load then subtract is safe.
        let losing = counter.load(Ordering::SeqCst) > 0;
        if losing {
            counter.fetch_sub(1, Ordering::SeqCst);
        }
        if losing {
            // Someone else saves first, on a copy loaded before ours is saved.
            let mut rival = repository.load(command.account).await?;
            let cents = i64::from(self.counters.interference.load(Ordering::SeqCst));
            rival.raise(MoneyDeposited { cents });
            repository.save(&mut rival).await?;
        }
        account.raise(MoneyDeposited {
            cents: command.cents,
        });
        repository.save(&mut account).await
    }
}

/// Withdraws money, or raises `WithdrawalRejected` when funds are short.
pub struct WithdrawHandler {
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<Withdraw> for WithdrawHandler {
    async fn handle(&self, command: &CommandMessage<Withdraw>) -> Result<()> {
        let repository = self.context.repository::<BankAccount>()?;
        let mut account = repository.load(command.account).await?;
        if account.state.balance < command.cents {
            account.raise(WithdrawalRejected {
                cents: command.cents,
            });
        } else {
            account.raise(MoneyWithdrawn {
                cents: command.cents,
            });
        }
        repository.save(&mut account).await
    }
}
