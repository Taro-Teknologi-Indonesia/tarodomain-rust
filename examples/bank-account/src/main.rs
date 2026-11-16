//! Runs the spec's bank scenario on the local provider: open two accounts,
//! deposit, transfer through the saga, query the projection and trace the
//! transfer.

use std::sync::Arc;

use bank_account::account::{AccountId, Deposit, OpenAccount};
use bank_account::projection::{BalanceStore, InMemoryBalanceStore};
use bank_account::register_bank;
use bank_account::transfer::{RequestTransfer, TransferId};
use tarodomain::adapters::ConsistencyMode;
use tarodomain::domain::CommandMessage;
use tarodomain::error::FrameworkError;
use tarodomain::framework::TaroDomain;
use tarodomain::kernel::ids::new_v7;
use tarodomain::trace::CorrelationTrace;
use tarodomain_local::LocalProvider;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), FrameworkError> {
    let balances = Arc::new(InMemoryBalanceStore::new());
    let builder = TaroDomain::builder()
        .use_provider(Arc::new(LocalProvider::new()))
        .consistency(ConsistencyMode::Strong, None);
    let bank = register_bank(builder, balances.clone()).build().await?;

    let (ann, bob) = (AccountId::generate(), AccountId::generate());
    for (account, owner) in [(ann, "Ann"), (bob, "Bob")] {
        bank.dispatch(OpenAccount {
            account,
            owner: owner.into(),
        })
        .await?;
    }
    bank.dispatch(Deposit {
        account: ann,
        cents: 10_000,
    })
    .await?;
    println!("opened two accounts, and paid $100.00 into Ann's");

    // One correlation ID ties the whole transfer together.
    let correlation = new_v7();
    let request = RequestTransfer {
        transfer: TransferId::generate(),
        from: ann,
        to: bob,
        cents: 3_000,
    };
    bank.dispatch(CommandMessage::new(request).with_correlation_id(correlation))
        .await?;
    println!("transferred $30.00 from Ann to Bob\n");

    println!("balances (the projection):");
    for row in balances.list().await? {
        println!(
            "  {:<4} ${}.{:02}",
            row.owner,
            row.cents / 100,
            row.cents % 100
        );
    }

    let entries =
        CorrelationTrace::trace(bank.event_store().adapter().as_ref(), correlation).await?;
    let tree = CorrelationTrace::build_tree(&entries);
    println!("\ntrace of the transfer ({correlation}):");
    print!("{}", CorrelationTrace::format_tree(&tree));
    Ok(())
}
