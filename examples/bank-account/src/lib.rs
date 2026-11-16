//! The TaroDomain spec's bank example: accounts, and money transfers run by a
//! saga.
//!
//! * [`account`]: the `BankAccount` aggregate and its commands.
//! * [`transfer`]: the `MoneyTransfer` aggregate and its commands.
//! * [`saga`]: `TransferSaga`, which moves the money and undoes it on failure.
//! * [`projection`]: `BalanceProjection`, the balance read model.
//!
//! [`register_bank`] adds all of it to a framework builder. The crate has no
//! runtime of its own, so a Cloudflare worker can reuse it; the binary
//! (`cargo run -p bank-account`) wires it to the local provider.

pub mod account;
pub mod projection;
pub mod saga;
pub mod transfer;

use std::sync::Arc;
use std::time::Duration;

use tarodomain::framework::{SagaOptions, TaroDomainBuilder};

use account::{
    BankAccount, Deposit, DepositHandler, OpenAccount, OpenAccountHandler, Withdraw,
    WithdrawHandler,
};
use projection::{BalanceProjection, BalanceStore};
use saga::TransferSaga;
use transfer::{
    CompleteTransfer, CompleteTransferHandler, FailTransfer, FailTransferHandler, MoneyTransfer,
    RequestTransfer, RequestTransferHandler,
};

/// How long a transfer may stand still before the saga undoes it.
pub const TRANSFER_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// Registers the bank's aggregates, commands, balance projection and transfer
/// saga on `builder`. The caller chooses the provider, the consistency mode
/// and the [`BalanceStore`].
pub fn register_bank(
    builder: TaroDomainBuilder,
    balances: Arc<dyn BalanceStore>,
) -> TaroDomainBuilder {
    builder
        .aggregate::<BankAccount>(|a| a.event_sourced().snapshot_every(100))
        .aggregate::<MoneyTransfer>(|a| a)
        .command::<OpenAccount, _>(|_, context| OpenAccountHandler { context }, None)
        .command::<Deposit, _>(|_, context| DepositHandler { context }, None)
        .command::<Withdraw, _>(|_, context| WithdrawHandler { context }, None)
        .command::<RequestTransfer, _>(|_, context| RequestTransferHandler { context }, None)
        .command::<CompleteTransfer, _>(|_, context| CompleteTransferHandler { context }, None)
        .command::<FailTransfer, _>(|_, context| FailTransferHandler { context }, None)
        .projection(move |_| Ok(Arc::new(BalanceProjection::new(balances))))
        .saga::<TransferSaga>(SagaOptions::with_timeout(TRANSFER_TIMEOUT))
}
