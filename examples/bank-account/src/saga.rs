//! The transfer saga: withdraw, deposit, complete, and undo what was done if
//! something goes wrong or the transfer stalls.

use serde::{Deserialize, Serialize};
use tarodomain::domain::{AnyCommand, Recorded};
use tarodomain::error::Result;
use tarodomain::saga::{ProcessManager, SagaCtx, SagaRoutes, SagaTimeout};

use crate::account::{
    AccountId, Deposit, MoneyDeposited, MoneyWithdrawn, Withdraw, WithdrawalRejected,
};
use crate::transfer::{
    CompleteTransfer, FailTransfer, TransferCompleted, TransferFailed, TransferId,
    TransferRequested,
};

/// The name of the compensation that gives the withdrawn money back.
pub const REFUND: &str = "refund";

/// The name of the compensation that takes a deposit back.
pub const REVERSE_DEPOSIT: &str = "reverse_deposit";

/// Where the saga stands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    /// Not started.
    #[default]
    New,
    /// Waiting for the withdrawal.
    Withdrawing,
    /// Waiting for the deposit.
    Depositing,
    /// Waiting for the transfer to be marked completed.
    Completing,
    /// Waiting for the transfer to be marked failed.
    Failing,
    /// The transfer stalled and was undone.
    TimedOut,
    /// Done.
    Completed,
    /// Undone after a failure.
    RolledBack,
}

/// What the saga remembers about its transfer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Details {
    /// The transfer.
    pub transfer: TransferId,
    /// The account the money leaves.
    pub from: AccountId,
    /// The account the money reaches.
    pub to: AccountId,
    /// How much, in cents.
    pub cents: i64,
}

/// The saga's state.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TransferState {
    /// The transfer, once the saga has started.
    pub details: Option<Details>,
    /// Where the saga stands.
    pub phase: Phase,
}

/// Moves money from one account to another, one step at a time.
///
/// ```text
/// TransferRequested ──▶ Withdraw ──▶ MoneyWithdrawn ──▶ Deposit
///   ──▶ MoneyDeposited ──▶ CompleteTransfer ──▶ TransferCompleted
/// ```
///
/// A refused withdrawal fails the transfer. A failed transfer runs the
/// compensations in reverse order: a deposit is taken back, then a
/// withdrawal is given back. A transfer that stops moving for the saga's
/// timeout is undone the same way.
pub struct TransferSaga;

type Commands = Result<Vec<AnyCommand>>;

fn details(ctx: &SagaCtx<TransferSaga>) -> Details {
    ctx.state
        .details
        .clone()
        .expect("a saga that handles an event has started")
}

fn fail(ctx: &mut SagaCtx<TransferSaga>) -> Commands {
    let transfer = details(ctx).transfer;
    ctx.state.phase = Phase::Failing;
    Ok(vec![FailTransfer { transfer }.into()])
}

impl TransferSaga {
    fn start(ctx: &mut SagaCtx<Self>, event: &Recorded<TransferRequested>) -> Commands {
        ctx.state.details = Some(Details {
            transfer: event.transfer,
            from: event.from,
            to: event.to,
            cents: event.cents,
        });
        ctx.state.phase = Phase::Withdrawing;
        Ok(vec![
            Withdraw {
                account: event.from,
                cents: event.cents,
                strict: false,
            }
            .into(),
        ])
    }

    fn withdrawn(ctx: &mut SagaCtx<Self>, _: &Recorded<MoneyWithdrawn>) -> Commands {
        if ctx.state.phase != Phase::Withdrawing {
            return Ok(vec![]);
        }
        let details = details(ctx);
        ctx.register_compensation(REFUND);
        ctx.state.phase = Phase::Depositing;
        Ok(vec![
            Deposit {
                account: details.to,
                cents: details.cents,
            }
            .into(),
        ])
    }

    fn deposited(ctx: &mut SagaCtx<Self>, _: &Recorded<MoneyDeposited>) -> Commands {
        if ctx.state.phase != Phase::Depositing {
            return Ok(vec![]);
        }
        let transfer = details(ctx).transfer;
        ctx.register_compensation(REVERSE_DEPOSIT);
        ctx.state.phase = Phase::Completing;
        Ok(vec![CompleteTransfer { transfer }.into()])
    }

    fn rejected(ctx: &mut SagaCtx<Self>, _: &Recorded<WithdrawalRejected>) -> Commands {
        fail(ctx)
    }

    fn timed_out(ctx: &mut SagaCtx<Self>, _: &Recorded<SagaTimeout>) -> Commands {
        let mut commands = ctx.compensation_commands()?;
        let transfer = details(ctx).transfer;
        ctx.state.phase = Phase::TimedOut;
        commands.push(FailTransfer { transfer }.into());
        Ok(commands)
    }

    /// `TransferFailed` ends the saga: it gives back whatever is still
    /// registered. After a timeout the stack is already empty.
    fn failed(ctx: &mut SagaCtx<Self>, _: &Recorded<TransferFailed>) -> Commands {
        let commands = ctx.compensation_commands()?;
        if ctx.state.phase != Phase::TimedOut {
            ctx.state.phase = Phase::RolledBack;
        }
        Ok(commands)
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
        r.compensate_when::<SagaTimeout>(Self::timed_out);
        r.completed_when::<TransferFailed>(Self::failed);
        r.completed_when::<TransferCompleted>(|ctx, _| {
            ctx.state.phase = Phase::Completed;
            Ok(vec![])
        });
        r.define_compensation(REFUND, |state: &TransferState| {
            let details = state.details.clone().expect("started");
            vec![
                Deposit {
                    account: details.from,
                    cents: details.cents,
                }
                .into(),
            ]
        });
        r.define_compensation(REVERSE_DEPOSIT, |state: &TransferState| {
            let details = state.details.clone().expect("started");
            vec![
                Withdraw {
                    account: details.to,
                    cents: details.cents,
                    strict: false,
                }
                .into(),
            ]
        });
    }
}
