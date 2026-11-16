//! The balance read model: a projection over account events, and the small
//! store trait it writes through.

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use serde::{Deserialize, Serialize};
use tarodomain::async_trait;
use tarodomain::domain::Recorded;
use tarodomain::error::Result;
use tarodomain::kernel::ids::Uuid;
use tarodomain::projection::{Projection, ProjectionRoutes};

use crate::account::{AccountId, AccountOpened, MoneyDeposited, MoneyWithdrawn};

/// One row of the read model.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountBalance {
    /// The account.
    pub account: AccountId,
    /// Who owns it.
    pub owner: String,
    /// The balance in cents.
    pub cents: i64,
}

/// Where the balance projection keeps its rows.
///
/// Every write must be idempotent (§14.3), because a crash between a write
/// and the processed-event mark replays the event. A SQL store does that with
/// an upsert for `open` and with a table of applied event IDs for `apply`,
/// in the same transaction as the balance change.
#[async_trait]
pub trait BalanceStore: Send + Sync {
    /// Creates the account's row, or sets its owner if the row exists. The
    /// balance is kept.
    async fn open(&self, account: AccountId, owner: &str) -> Result<()>;

    /// Adds `delta_cents` to the account's balance, once per `event_id`.
    async fn apply(&self, event_id: Uuid, account: AccountId, delta_cents: i64) -> Result<()>;

    /// The account's row.
    async fn get(&self, account: AccountId) -> Result<Option<AccountBalance>>;

    /// Every row, ordered by account ID.
    async fn list(&self) -> Result<Vec<AccountBalance>>;

    /// Deletes everything, before a rebuild.
    async fn clear(&self) -> Result<()>;
}

#[derive(Default)]
struct Tables {
    rows: BTreeMap<AccountId, AccountBalance>,
    applied: HashSet<Uuid>,
}

/// A [`BalanceStore`] in memory.
#[derive(Default)]
pub struct InMemoryBalanceStore {
    tables: Mutex<Tables>,
}

impl InMemoryBalanceStore {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    fn tables(&self) -> std::sync::MutexGuard<'_, Tables> {
        self.tables.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[async_trait]
impl BalanceStore for InMemoryBalanceStore {
    async fn open(&self, account: AccountId, owner: &str) -> Result<()> {
        let mut tables = self.tables();
        let row = tables
            .rows
            .entry(account)
            .or_insert_with(|| AccountBalance {
                account,
                owner: String::new(),
                cents: 0,
            });
        row.owner = owner.to_owned();
        Ok(())
    }

    async fn apply(&self, event_id: Uuid, account: AccountId, delta_cents: i64) -> Result<()> {
        let mut tables = self.tables();
        if !tables.applied.insert(event_id) {
            return Ok(());
        }
        let row = tables
            .rows
            .entry(account)
            .or_insert_with(|| AccountBalance {
                account,
                owner: String::new(),
                cents: 0,
            });
        row.cents += delta_cents;
        Ok(())
    }

    async fn get(&self, account: AccountId) -> Result<Option<AccountBalance>> {
        Ok(self.tables().rows.get(&account).cloned())
    }

    async fn list(&self) -> Result<Vec<AccountBalance>> {
        Ok(self.tables().rows.values().cloned().collect())
    }

    async fn clear(&self) -> Result<()> {
        *self.tables() = Tables::default();
        Ok(())
    }
}

/// The name of the balance projection.
pub const BALANCES: &str = "balances";

/// Keeps one row per account: its owner and balance.
pub struct BalanceProjection {
    store: Arc<dyn BalanceStore>,
}

impl BalanceProjection {
    /// A projection writing to `store`.
    pub fn new(store: Arc<dyn BalanceStore>) -> Self {
        Self { store }
    }

    async fn on_opened(self: Arc<Self>, event: Recorded<AccountOpened>) -> Result<()> {
        self.store.open(event.account, &event.owner).await
    }

    async fn on_deposited(self: Arc<Self>, event: Recorded<MoneyDeposited>) -> Result<()> {
        let id = event.envelope.id;
        self.store.apply(id, event.account, event.cents).await
    }

    async fn on_withdrawn(self: Arc<Self>, event: Recorded<MoneyWithdrawn>) -> Result<()> {
        let id = event.envelope.id;
        self.store.apply(id, event.account, -event.cents).await
    }
}

#[async_trait]
impl Projection for BalanceProjection {
    fn name(&self) -> &str {
        BALANCES
    }

    fn routes(routes: &mut ProjectionRoutes<Self>) {
        routes.on::<AccountOpened, _, _>(Self::on_opened);
        routes.on::<MoneyDeposited, _, _>(Self::on_deposited);
        routes.on::<MoneyWithdrawn, _, _>(Self::on_withdrawn);
    }

    async fn on_rebuild_start(&self) -> Result<()> {
        self.store.clear().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tarodomain::kernel::ids::new_v7;

    #[tokio::test]
    async fn an_event_applies_once() {
        let store = InMemoryBalanceStore::new();
        let account = AccountId::generate();
        let event = new_v7();
        store.open(account, "Ann").await.unwrap();
        for _ in 0..3 {
            store.apply(event, account, 50).await.unwrap();
        }
        assert_eq!(store.get(account).await.unwrap().unwrap().cents, 50);
    }

    #[tokio::test]
    async fn opening_again_keeps_the_balance() {
        let store = InMemoryBalanceStore::new();
        let account = AccountId::generate();
        store.open(account, "Ann").await.unwrap();
        store.apply(new_v7(), account, 20).await.unwrap();
        store.open(account, "Ann B.").await.unwrap();
        let row = store.get(account).await.unwrap().unwrap();
        assert_eq!((row.owner.as_str(), row.cents), ("Ann B.", 20));
    }

    #[tokio::test]
    async fn clear_forgets_rows_and_applied_events() {
        let store = InMemoryBalanceStore::new();
        let account = AccountId::generate();
        let event = new_v7();
        store.apply(event, account, 20).await.unwrap();
        store.clear().await.unwrap();
        assert!(store.list().await.unwrap().is_empty());
        store.apply(event, account, 20).await.unwrap();
        assert_eq!(store.get(account).await.unwrap().unwrap().cents, 20);
    }
}
