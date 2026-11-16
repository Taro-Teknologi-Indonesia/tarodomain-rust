//! The `BankAccount` aggregate, its events, commands and handlers.

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
    /// The account the event belongs to, so a projection can tell.
    pub account: AccountId,
    /// Who owns it.
    pub owner: String,
}

impl Event for AccountOpened {
    const NAME: &'static str = "AccountOpened";
}

/// Money was paid in.
#[derive(Debug, Serialize, Deserialize)]
pub struct MoneyDeposited {
    /// The account the event belongs to, so a projection can tell.
    pub account: AccountId,
    /// How much, in cents.
    pub cents: i64,
}

impl Event for MoneyDeposited {
    const NAME: &'static str = "MoneyDeposited";
}

/// Money was taken out.
#[derive(Debug, Serialize, Deserialize)]
pub struct MoneyWithdrawn {
    /// The account the event belongs to, so a projection can tell.
    pub account: AccountId,
    /// How much, in cents.
    pub cents: i64,
}

impl Event for MoneyWithdrawn {
    const NAME: &'static str = "MoneyWithdrawn";
}

/// A withdrawal was refused because the funds are short. A saga reacts to
/// this event, which is why a refusal is an event and not an error.
#[derive(Debug, Serialize, Deserialize)]
pub struct WithdrawalRejected {
    /// The account the event belongs to, so a projection can tell.
    pub account: AccountId,
    /// How much was asked for, in cents.
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

/// The domain error code for a strict withdrawal that the balance can't
/// cover.
pub const INSUFFICIENT_FUNDS: &str = "INSUFFICIENT_FUNDS";

/// The domain error code for an amount that isn't positive.
pub const INVALID_AMOUNT: &str = "INVALID_AMOUNT";

/// The domain error code for opening an account that exists.
pub const ACCOUNT_ALREADY_EXISTS: &str = "ACCOUNT_ALREADY_EXISTS";

fn domain_error(code: &'static str, message: String) -> FrameworkError {
    FrameworkError::custom(code, ErrorCategory::Domain, message)
}

/// `INSUFFICIENT_FUNDS`, with the balance and the amount as context.
pub fn insufficient_funds(balance: i64, requested: i64) -> FrameworkError {
    domain_error(
        INSUFFICIENT_FUNDS,
        format!("the balance is {balance} cents, which doesn't cover {requested}"),
    )
    .with_context("balance", balance)
    .with_context("requested", requested)
}

/// The account's business state, which is also its snapshot.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountState {
    /// Who owns the account.
    pub owner: String,
    /// The balance in cents.
    pub balance: i64,
}

/// A bank account.
#[derive(Debug)]
pub struct BankAccount {
    id: AccountId,
    core: AggregateCore,
    /// The state the events have built up.
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

fn positive(cents: i64) -> Result<()> {
    if cents > 0 {
        Ok(())
    } else {
        Err(domain_error(
            INVALID_AMOUNT,
            format!("{cents} cents isn't a positive amount"),
        ))
    }
}

impl BankAccount {
    /// A new account, with its `AccountOpened` event raised.
    pub fn open(id: AccountId, owner: impl Into<String>) -> Self {
        let mut account = Self::new(id);
        account.raise(AccountOpened {
            account: id,
            owner: owner.into(),
        });
        account
    }

    /// Pays money in. The amount must be positive.
    pub fn deposit(&mut self, cents: i64) -> Result<()> {
        positive(cents)?;
        self.raise(MoneyDeposited {
            account: self.id,
            cents,
        });
        Ok(())
    }

    /// Takes money out, or, when the balance is short, refuses.
    ///
    /// A refusal raises `WithdrawalRejected` (which a saga can react to),
    /// unless `strict` is set, in which case it is an `INSUFFICIENT_FUNDS`
    /// error and nothing is raised.
    pub fn withdraw(&mut self, cents: i64, strict: bool) -> Result<()> {
        positive(cents)?;
        if self.state.balance >= cents {
            self.raise(MoneyWithdrawn {
                account: self.id,
                cents,
            });
        } else if strict {
            return Err(insufficient_funds(self.state.balance, cents));
        } else {
            self.raise(WithdrawalRejected {
                account: self.id,
                cents,
            });
        }
        Ok(())
    }
}

/// Opens an account.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenAccount {
    /// The new account's ID.
    pub account: AccountId,
    /// Who owns it.
    pub owner: String,
}

impl Command for OpenAccount {
    const NAME: &'static str = "OpenAccount";
}

/// Pays money into an account.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Deposit {
    /// The account.
    pub account: AccountId,
    /// How much, in cents.
    pub cents: i64,
}

impl Command for Deposit {
    const NAME: &'static str = "Deposit";
}

/// Takes money out of an account.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Withdraw {
    /// The account.
    pub account: AccountId,
    /// How much, in cents.
    pub cents: i64,
    /// Fail with `INSUFFICIENT_FUNDS` instead of raising
    /// `WithdrawalRejected` when the balance is short.
    #[serde(default)]
    pub strict: bool,
}

impl Command for Withdraw {
    const NAME: &'static str = "Withdraw";
}

/// Handles [`OpenAccount`].
pub struct OpenAccountHandler {
    /// The pipeline's context.
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<OpenAccount> for OpenAccountHandler {
    async fn handle(&self, command: &CommandMessage<OpenAccount>) -> Result<()> {
        let repository = self.context.repository::<BankAccount>()?;
        if repository.exists(command.account).await? {
            return Err(domain_error(
                ACCOUNT_ALREADY_EXISTS,
                format!("account {} already exists", command.account),
            ));
        }
        let mut account = BankAccount::open(command.account, command.owner.clone());
        repository.save(&mut account).await
    }
}

/// Handles [`Deposit`].
pub struct DepositHandler {
    /// The pipeline's context.
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<Deposit> for DepositHandler {
    async fn handle(&self, command: &CommandMessage<Deposit>) -> Result<()> {
        let repository = self.context.repository::<BankAccount>()?;
        let mut account = repository.load(command.account).await?;
        account.deposit(command.cents)?;
        repository.save(&mut account).await
    }
}

/// Handles [`Withdraw`].
pub struct WithdrawHandler {
    /// The pipeline's context.
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<Withdraw> for WithdrawHandler {
    async fn handle(&self, command: &CommandMessage<Withdraw>) -> Result<()> {
        let repository = self.context.repository::<BankAccount>()?;
        let mut account = repository.load(command.account).await?;
        account.withdraw(command.cents, command.strict)?;
        repository.save(&mut account).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tarodomain::error::ErrorCode;

    fn account(balance: i64) -> BankAccount {
        let mut account = BankAccount::open(AccountId::generate(), "Ann");
        if balance > 0 {
            account.deposit(balance).unwrap();
        }
        account.mark_changes_as_committed();
        account
    }

    #[test]
    fn deposits_and_withdrawals_move_the_balance() {
        let mut account = account(100);
        account.withdraw(30, false).unwrap();
        account.deposit(5).unwrap();
        assert_eq!(account.state.balance, 75);
        assert_eq!(account.version(), 4);
    }

    #[test]
    fn a_short_withdrawal_is_rejected_as_an_event() {
        let mut account = account(10);
        account.withdraw(30, false).unwrap();
        assert_eq!(account.state.balance, 10);
        let events = account.uncommitted_changes();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name(), "WithdrawalRejected");
    }

    #[test]
    fn a_strict_short_withdrawal_is_insufficient_funds() {
        let mut account = account(10);
        let error = account.withdraw(30, true).unwrap_err();
        assert_eq!(error.code().as_str(), INSUFFICIENT_FUNDS);
        assert_eq!(error.code().category(), ErrorCategory::Domain);
        assert_eq!(error.context()["balance"], 10);
        assert_eq!(error.context()["requested"], 30);
        assert!(!account.has_uncommitted_changes());
    }

    #[test]
    fn amounts_must_be_positive() {
        let mut account = account(10);
        for cents in [0, -5] {
            assert_eq!(
                account.deposit(cents).unwrap_err().code().as_str(),
                INVALID_AMOUNT
            );
            assert_eq!(
                account.withdraw(cents, false).unwrap_err().code().as_str(),
                INVALID_AMOUNT
            );
        }
        assert!(matches!(
            account.deposit(0).unwrap_err().code(),
            ErrorCode::Custom { .. }
        ));
    }

    #[test]
    fn a_snapshot_restores_the_state() {
        let account = account(40);
        let snapshot = account.to_snapshot().unwrap();
        let mut restored = BankAccount::new(account.id());
        restored
            .restore_snapshot(snapshot.data, snapshot.version)
            .unwrap();
        assert_eq!(restored.state, account.state);
        assert_eq!(restored.version(), 2);
    }
}
