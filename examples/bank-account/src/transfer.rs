//! The `MoneyTransfer` aggregate: the record of a transfer between two
//! accounts, which a saga moves along.

use serde::{Deserialize, Serialize};
use tarodomain::command::{CommandContext, CommandHandler};
use tarodomain::domain::{Aggregate, AggregateCore, Command, CommandMessage, Event};
use tarodomain::error::Result;
use tarodomain::{aggregate_id, async_trait, event_family};

use crate::account::AccountId;

aggregate_id! {
    /// Identifies a transfer.
    pub TransferId
}

/// A transfer was asked for.
#[derive(Debug, Serialize, Deserialize)]
pub struct TransferRequested {
    /// The transfer.
    pub transfer: TransferId,
    /// The account the money leaves.
    pub from: AccountId,
    /// The account the money reaches.
    pub to: AccountId,
    /// How much, in cents.
    pub cents: i64,
}

impl Event for TransferRequested {
    const NAME: &'static str = "TransferRequested";
}

/// The money has moved.
#[derive(Debug, Serialize, Deserialize)]
pub struct TransferCompleted;

impl Event for TransferCompleted {
    const NAME: &'static str = "TransferCompleted";
}

/// The transfer didn't happen, and anything taken was given back.
#[derive(Debug, Serialize, Deserialize)]
pub struct TransferFailed;

impl Event for TransferFailed {
    const NAME: &'static str = "TransferFailed";
}

event_family!(
    /// Everything that happens to a transfer.
    #[allow(clippy::enum_variant_names)]
    pub enum TransferEvent {
        TransferRequested,
        TransferCompleted,
        TransferFailed,
    }
);

/// Where a transfer stands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TransferStatus {
    /// Not requested yet.
    #[default]
    New,
    /// Asked for; the saga is moving the money.
    Requested,
    /// Done.
    Completed,
    /// Given up.
    Failed,
}

/// A money transfer.
#[derive(Debug)]
pub struct MoneyTransfer {
    id: TransferId,
    core: AggregateCore,
    /// Where the transfer stands.
    pub status: TransferStatus,
}

impl Aggregate for MoneyTransfer {
    type Id = TransferId;
    type Event = TransferEvent;
    type Snapshot = TransferStatus;
    const TYPE: &'static str = "MoneyTransfer";

    fn new(id: TransferId) -> Self {
        Self {
            id,
            core: AggregateCore::new(),
            status: TransferStatus::New,
        }
    }

    fn id(&self) -> TransferId {
        self.id
    }

    fn core(&self) -> &AggregateCore {
        &self.core
    }

    fn core_mut(&mut self) -> &mut AggregateCore {
        &mut self.core
    }

    fn apply(&mut self, event: &TransferEvent) {
        self.status = match event {
            TransferEvent::TransferRequested(_) => TransferStatus::Requested,
            TransferEvent::TransferCompleted(_) => TransferStatus::Completed,
            TransferEvent::TransferFailed(_) => TransferStatus::Failed,
        };
    }

    fn snapshot_state(&self) -> TransferStatus {
        self.status
    }

    fn restore_snapshot_state(&mut self, state: TransferStatus) {
        self.status = state;
    }
}

impl MoneyTransfer {
    /// A new transfer, with `TransferRequested` raised.
    pub fn request(id: TransferId, from: AccountId, to: AccountId, cents: i64) -> Self {
        let mut transfer = Self::new(id);
        transfer.raise(TransferRequested {
            transfer: id,
            from,
            to,
            cents,
        });
        transfer
    }

    /// Marks the transfer done. Does nothing if it has ended already, so a
    /// repeated command can't raise a second ending.
    pub fn complete(&mut self) {
        if self.status == TransferStatus::Requested {
            self.raise(TransferCompleted);
        }
    }

    /// Marks the transfer failed. Does nothing if it has ended already.
    pub fn fail(&mut self) {
        if self.status == TransferStatus::Requested {
            self.raise(TransferFailed);
        }
    }
}

/// Asks for a transfer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestTransfer {
    /// The new transfer's ID.
    pub transfer: TransferId,
    /// The account the money leaves.
    pub from: AccountId,
    /// The account the money reaches.
    pub to: AccountId,
    /// How much, in cents.
    pub cents: i64,
}

impl Command for RequestTransfer {
    const NAME: &'static str = "RequestTransfer";
}

/// Marks a transfer completed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteTransfer {
    /// The transfer.
    pub transfer: TransferId,
}

impl Command for CompleteTransfer {
    const NAME: &'static str = "CompleteTransfer";
}

/// Marks a transfer failed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailTransfer {
    /// The transfer.
    pub transfer: TransferId,
}

impl Command for FailTransfer {
    const NAME: &'static str = "FailTransfer";
}

/// Handles [`RequestTransfer`].
pub struct RequestTransferHandler {
    /// The pipeline's context.
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<RequestTransfer> for RequestTransferHandler {
    async fn handle(&self, command: &CommandMessage<RequestTransfer>) -> Result<()> {
        let repository = self.context.repository::<MoneyTransfer>()?;
        let mut transfer =
            MoneyTransfer::request(command.transfer, command.from, command.to, command.cents);
        repository.save(&mut transfer).await
    }
}

/// Handles [`CompleteTransfer`].
pub struct CompleteTransferHandler {
    /// The pipeline's context.
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<CompleteTransfer> for CompleteTransferHandler {
    async fn handle(&self, command: &CommandMessage<CompleteTransfer>) -> Result<()> {
        let repository = self.context.repository::<MoneyTransfer>()?;
        let mut transfer = repository.load(command.transfer).await?;
        transfer.complete();
        repository.save(&mut transfer).await
    }
}

/// Handles [`FailTransfer`].
pub struct FailTransferHandler {
    /// The pipeline's context.
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<FailTransfer> for FailTransferHandler {
    async fn handle(&self, command: &CommandMessage<FailTransfer>) -> Result<()> {
        let repository = self.context.repository::<MoneyTransfer>()?;
        let mut transfer = repository.load(command.transfer).await?;
        transfer.fail();
        repository.save(&mut transfer).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transfer() -> MoneyTransfer {
        let mut t = MoneyTransfer::request(
            TransferId::generate(),
            AccountId::generate(),
            AccountId::generate(),
            30,
        );
        t.mark_changes_as_committed();
        t
    }

    #[test]
    fn a_transfer_ends_once() {
        let mut t = transfer();
        assert_eq!(t.status, TransferStatus::Requested);
        t.complete();
        t.fail();
        t.complete();
        assert_eq!(t.status, TransferStatus::Completed);
        assert_eq!(t.uncommitted_changes().len(), 1);
    }

    #[test]
    fn a_failed_transfer_stays_failed() {
        let mut t = transfer();
        t.fail();
        t.complete();
        assert_eq!(t.status, TransferStatus::Failed);
        assert_eq!(t.uncommitted_changes().len(), 1);
    }

    #[test]
    fn a_new_transfer_can_not_end() {
        let mut t = MoneyTransfer::new(TransferId::generate());
        t.complete();
        t.fail();
        assert!(!t.has_uncommitted_changes());
    }
}
