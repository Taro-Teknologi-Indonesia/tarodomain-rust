//! A money transfer: an aggregate that records it, and the saga that moves
//! the money with compensation.

use serde::{Deserialize, Serialize};
use tarodomain::command::{CommandContext, CommandHandler};
use tarodomain::domain::{
    Aggregate, AggregateCore, AnyCommand, Command, CommandMessage, Event, Recorded,
};
use tarodomain::error::Result;
use tarodomain::saga::{ProcessManager, SagaCtx, SagaRoutes};
use tarodomain::{aggregate_id, async_trait, event_family};

use super::bank::{
    AccountId, Deposit, MoneyDeposited, MoneyWithdrawn, Withdraw, WithdrawalRejected,
};

aggregate_id! {
    /// Identifies a transfer.
    pub TransferId
}

/// A transfer was asked for.
#[derive(Debug, Serialize, Deserialize)]
pub struct TransferRequested {
    pub transfer: TransferId,
    pub from: AccountId,
    pub to: AccountId,
    pub cents: i64,
    /// Test hook: `"withdrawal"` or `"deposit"` makes the saga fail the
    /// transfer after that step.
    pub fail_after: Option<String>,
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

/// The transfer failed.
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

/// A transfer between two accounts.
#[derive(Debug)]
pub struct MoneyTransfer {
    id: TransferId,
    core: AggregateCore,
    pub status: String,
}

impl Aggregate for MoneyTransfer {
    type Id = TransferId;
    type Event = TransferEvent;
    type Snapshot = String;
    const TYPE: &'static str = "MoneyTransfer";

    fn new(id: TransferId) -> Self {
        Self {
            id,
            core: AggregateCore::new(),
            status: String::new(),
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
            TransferEvent::TransferRequested(_) => "requested",
            TransferEvent::TransferCompleted(_) => "completed",
            TransferEvent::TransferFailed(_) => "failed",
        }
        .into();
    }
    fn snapshot_state(&self) -> String {
        self.status.clone()
    }
    fn restore_snapshot_state(&mut self, state: String) {
        self.status = state;
    }
}

/// Asks for a transfer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestTransfer {
    pub transfer: TransferId,
    pub from: AccountId,
    pub to: AccountId,
    pub cents: i64,
    pub fail_after: Option<String>,
}

impl Command for RequestTransfer {
    const NAME: &'static str = "RequestTransfer";
}

/// Marks a transfer completed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompleteTransfer {
    pub transfer: TransferId,
}

impl Command for CompleteTransfer {
    const NAME: &'static str = "CompleteTransfer";
}

/// Marks a transfer failed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FailTransfer {
    pub transfer: TransferId,
}

impl Command for FailTransfer {
    const NAME: &'static str = "FailTransfer";
}

pub struct RequestTransferHandler {
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<RequestTransfer> for RequestTransferHandler {
    async fn handle(&self, c: &CommandMessage<RequestTransfer>) -> Result<()> {
        let repository = self.context.repository::<MoneyTransfer>()?;
        let mut transfer = MoneyTransfer::new(c.transfer);
        transfer.raise(TransferRequested {
            transfer: c.transfer,
            from: c.from,
            to: c.to,
            cents: c.cents,
            fail_after: c.fail_after.clone(),
        });
        repository.save(&mut transfer).await
    }
}

pub struct CompleteTransferHandler {
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<CompleteTransfer> for CompleteTransferHandler {
    async fn handle(&self, c: &CommandMessage<CompleteTransfer>) -> Result<()> {
        let repository = self.context.repository::<MoneyTransfer>()?;
        let mut transfer = repository.load(c.transfer).await?;
        transfer.raise(TransferCompleted);
        repository.save(&mut transfer).await
    }
}

pub struct FailTransferHandler {
    pub context: CommandContext,
}

#[async_trait]
impl CommandHandler<FailTransfer> for FailTransferHandler {
    async fn handle(&self, c: &CommandMessage<FailTransfer>) -> Result<()> {
        let repository = self.context.repository::<MoneyTransfer>()?;
        let mut transfer = repository.load(c.transfer).await?;
        transfer.raise(TransferFailed);
        repository.save(&mut transfer).await
    }
}

/// What the saga remembers about its transfer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Details {
    pub transfer: TransferId,
    pub from: AccountId,
    pub to: AccountId,
    pub cents: i64,
    pub fail_after: Option<String>,
}

/// The saga's state.
#[derive(Default, Serialize, Deserialize)]
pub struct TransferState {
    pub details: Option<Details>,
    pub phase: String,
}

/// Withdraw, then deposit, then complete; undo on failure.
pub struct TransferSaga;

type Commands = Result<Vec<AnyCommand>>;

fn details(ctx: &SagaCtx<TransferSaga>) -> Details {
    ctx.state.details.clone().expect("the saga has started")
}

impl TransferSaga {
    fn start(ctx: &mut SagaCtx<Self>, e: &Recorded<TransferRequested>) -> Commands {
        ctx.state.details = Some(Details {
            transfer: e.transfer,
            from: e.from,
            to: e.to,
            cents: e.cents,
            fail_after: e.fail_after.clone(),
        });
        ctx.state.phase = "withdrawing".into();
        Ok(vec![
            Withdraw {
                account: e.from,
                cents: e.cents,
            }
            .into(),
        ])
    }

    fn withdrawn(ctx: &mut SagaCtx<Self>, _: &Recorded<MoneyWithdrawn>) -> Commands {
        if ctx.state.phase != "withdrawing" {
            return Ok(vec![]);
        }
        let d = details(ctx);
        ctx.register_compensation("refund");
        if d.fail_after.as_deref() == Some("withdrawal") {
            ctx.state.phase = "failing".into();
            return Ok(vec![
                FailTransfer {
                    transfer: d.transfer,
                }
                .into(),
            ]);
        }
        ctx.state.phase = "depositing".into();
        Ok(vec![
            Deposit {
                account: d.to,
                cents: d.cents,
            }
            .into(),
        ])
    }

    fn deposited(ctx: &mut SagaCtx<Self>, _: &Recorded<MoneyDeposited>) -> Commands {
        if ctx.state.phase != "depositing" {
            return Ok(vec![]);
        }
        let d = details(ctx);
        ctx.register_compensation("reverse_deposit");
        if d.fail_after.as_deref() == Some("deposit") {
            ctx.state.phase = "failing".into();
            return Ok(vec![
                FailTransfer {
                    transfer: d.transfer,
                }
                .into(),
            ]);
        }
        ctx.state.phase = "completing".into();
        Ok(vec![
            CompleteTransfer {
                transfer: d.transfer,
            }
            .into(),
        ])
    }

    fn rejected(ctx: &mut SagaCtx<Self>, _: &Recorded<WithdrawalRejected>) -> Commands {
        let d = details(ctx);
        ctx.state.phase = "failing".into();
        Ok(vec![
            FailTransfer {
                transfer: d.transfer,
            }
            .into(),
        ])
    }

    fn failed(ctx: &mut SagaCtx<Self>, _: &Recorded<TransferFailed>) -> Commands {
        ctx.state.phase = "compensating".into();
        ctx.compensation_commands()
    }
}

impl ProcessManager for TransferSaga {
    const SAGA_TYPE: &'static str = "TransferSaga";
    type State = TransferState;

    fn routes(r: &mut SagaRoutes<Self>) {
        r.starts_when::<TransferRequested>(Self::start);
        r.continue_when::<MoneyWithdrawn>(Self::withdrawn);
        r.continue_when::<MoneyDeposited>(Self::deposited);
        r.compensate_when::<WithdrawalRejected>(Self::rejected);
        r.compensate_when::<TransferFailed>(Self::failed);
        r.completed_when::<TransferCompleted>(|ctx, _| {
            ctx.state.phase = "completed".into();
            Ok(vec![])
        });
        r.define_compensation("refund", |s: &TransferState| {
            let d = s.details.clone().expect("started");
            vec![
                Deposit {
                    account: d.from,
                    cents: d.cents,
                }
                .into(),
            ]
        });
        r.define_compensation("reverse_deposit", |s: &TransferState| {
            let d = s.details.clone().expect("started");
            vec![
                Withdraw {
                    account: d.to,
                    cents: d.cents,
                }
                .into(),
            ]
        });
    }
}
