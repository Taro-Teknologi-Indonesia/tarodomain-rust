//! The bank example on Cloudflare Workers: the template to copy.
//!
//! * D1 stores the events, sagas and the balance read model.
//! * A Queue carries events to the projection in eventual mode.
//! * Two Cron Triggers run catch-up (every minute) and cleanup (daily).
//!
//! [`D1BalanceStore`] writes through the provider's [`D1Db`], so in strong
//! mode its writes join the command's batch and commit or roll back with the
//! events. The Worker glue (`fetch`, `queue`, `scheduled`) is at the bottom
//! and only builds for wasm32.

use bank_account::account::AccountId;
use bank_account::projection::{AccountBalance, BalanceStore};
use serde_json::json;
use tarodomain::async_trait;
use tarodomain::error::Result;
use tarodomain::kernel::ids::Uuid;
use tarodomain_cloudflare::d1::{D1Db, RowExt, Statement};

/// The application's own tables: the balance read model and the events it
/// has applied. They are not part of the framework's migrations.
const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS bank_balances (\
       account TEXT PRIMARY KEY, owner TEXT NOT NULL DEFAULT '', cents INTEGER NOT NULL DEFAULT 0\
     ) WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS bank_applied_events (event_id TEXT PRIMARY KEY) WITHOUT ROWID",
];

/// The balance read model on D1.
///
/// Every write is idempotent (§14.3) and written as an upsert or an
/// increment, never read-then-write: inside a unit of work reads see the
/// database as it was before the unit began.
#[derive(Clone)]
pub struct D1BalanceStore {
    db: D1Db,
}

impl D1BalanceStore {
    /// A store over `db`, which should be the provider's (`provider.db()`).
    pub fn new(db: D1Db) -> Self {
        Self { db }
    }

    /// Creates the tables if they don't exist. Call it once per isolate.
    pub async fn init(&self) -> Result<()> {
        for sql in SCHEMA {
            self.db.run(Statement::plain(*sql)).await?;
        }
        Ok(())
    }
}

fn row_to_balance(row: &tarodomain_cloudflare::d1::Row) -> Result<AccountBalance> {
    let account = row.require_str("account")?;
    Ok(AccountBalance {
        account: AccountId::from_existing(account)?,
        owner: row.require_str("owner")?.to_owned(),
        cents: row.require_i64("cents")?,
    })
}

#[async_trait]
impl BalanceStore for D1BalanceStore {
    async fn open(&self, account: AccountId, owner: &str) -> Result<()> {
        self.db
            .write(vec![Statement::new(
                "INSERT INTO bank_balances (account, owner, cents) VALUES (?, ?, 0) \
                 ON CONFLICT (account) DO UPDATE SET owner = excluded.owner",
                vec![json!(account.to_string()), json!(owner)],
            )])
            .await
    }

    async fn apply(&self, event_id: Uuid, account: AccountId, delta_cents: i64) -> Result<()> {
        let event = json!(event_id.to_string());
        self.db
            .write(vec![
                // Adds the delta unless this event was applied before. It runs
                // before the marker is written, in the same batch.
                Statement::new(
                    "INSERT INTO bank_balances (account, owner, cents) \
                     SELECT ?, '', ? WHERE NOT EXISTS \
                       (SELECT 1 FROM bank_applied_events WHERE event_id = ?) \
                     ON CONFLICT (account) DO UPDATE SET cents = cents + excluded.cents",
                    vec![
                        json!(account.to_string()),
                        json!(delta_cents),
                        event.clone(),
                    ],
                ),
                Statement::new(
                    "INSERT INTO bank_applied_events (event_id) VALUES (?) ON CONFLICT DO NOTHING",
                    vec![event],
                ),
            ])
            .await
    }

    async fn get(&self, account: AccountId) -> Result<Option<AccountBalance>> {
        let row = self
            .db
            .query_one(Statement::new(
                "SELECT account, owner, cents FROM bank_balances WHERE account = ?",
                vec![json!(account.to_string())],
            ))
            .await?;
        row.as_ref().map(row_to_balance).transpose()
    }

    async fn list(&self) -> Result<Vec<AccountBalance>> {
        let rows = self
            .db
            .query(Statement::plain(
                "SELECT account, owner, cents FROM bank_balances ORDER BY account",
            ))
            .await?;
        rows.iter().map(row_to_balance).collect()
    }

    async fn clear(&self) -> Result<()> {
        self.db
            .write(vec![
                Statement::plain("DELETE FROM bank_balances"),
                Statement::plain("DELETE FROM bank_applied_events"),
            ])
            .await
    }
}

#[cfg(target_arch = "wasm32")]
mod worker_glue {
    use std::sync::Arc;

    use bank_account::projection::BalanceStore;
    use bank_account::register_bank;
    use tarodomain::adapters::{ConsistencyMode, Provider};
    use tarodomain::framework::TaroDomain;
    use tarodomain_cloudflare::entry::CloudflareWorker;
    use tarodomain_cloudflare::provider::{Bindings, CloudflareProvider};
    use tokio::sync::OnceCell;
    use worker::{Context, Env, MessageBatch, Request, Response, Result, ScheduledEvent, event};

    use super::D1BalanceStore;

    /// Converts a framework error into the Worker runtime's.
    fn framework(error: impl std::fmt::Display) -> worker::Error {
        worker::Error::RustError(error.to_string())
    }

    /// Built once per isolate, on the first request.
    static BANK: OnceCell<(CloudflareWorker, D1BalanceStore)> = OnceCell::const_new();

    async fn bank(env: &Env) -> Result<&'static (CloudflareWorker, D1BalanceStore)> {
        BANK.get_or_try_init(|| async {
            let mode = match env.var("CONSISTENCY").map(|var| var.to_string()).as_deref() {
                Ok("strong") => ConsistencyMode::Strong,
                _ => ConsistencyMode::Eventual,
            };
            let bindings = Bindings::new().d1("DB").queue("EVENTS");
            let provider = Arc::new(CloudflareProvider::new(env, &bindings).map_err(framework)?);
            provider.initialize().await.map_err(framework)?;
            let balances = D1BalanceStore::new(provider.db().clone());
            balances.init().await.map_err(framework)?;
            let builder = TaroDomain::builder()
                .use_provider(provider.clone())
                .consistency(mode, None);
            let domain =
                register_bank(builder, Arc::new(balances.clone()) as Arc<dyn BalanceStore>)
                    .build()
                    .await
                    .map_err(framework)?;
            Ok::<_, worker::Error>((CloudflareWorker::new(domain, &provider), balances))
        })
        .await
    }

    #[event(fetch)]
    async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
        console_error_panic_hook::set_once();
        let (worker, balances) = bank(&env).await?;
        // The read model isn't part of the framework's API: add a route.
        if req.path().trim_end_matches('/') == "/balances" {
            let rows = balances.list().await.map_err(framework)?;
            return Response::from_json(&rows);
        }
        worker.fetch(req).await
    }

    #[event(queue)]
    async fn queue(batch: MessageBatch<String>, env: Env, _ctx: Context) -> Result<()> {
        console_error_panic_hook::set_once();
        bank(&env).await?.0.queue(batch).await
    }

    #[event(scheduled)]
    async fn scheduled(event: ScheduledEvent, env: Env, _ctx: worker::ScheduleContext) {
        console_error_panic_hook::set_once();
        if let Ok((worker, _)) = bank(&env).await {
            worker.scheduled(event).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bank_account::account::{Deposit, OpenAccount};
    use bank_account::register_bank;
    use tarodomain::adapters::{ConsistencyMode, Provider};
    use tarodomain::framework::TaroDomain;
    use tarodomain::kernel::ids::new_v7;
    use tarodomain_cloudflare::provider::CloudflareProvider;
    use tarodomain_cloudflare::queue_bus::RecordingQueue;
    use tarodomain_cloudflare::sqlite_backend::SqliteBackend;

    use super::*;

    async fn store() -> (D1BalanceStore, CloudflareProvider) {
        let provider = CloudflareProvider::with_backend(
            Arc::new(SqliteBackend::in_memory().unwrap()),
            Some(Arc::new(RecordingQueue::new())),
        );
        provider.initialize().await.unwrap();
        let store = D1BalanceStore::new(provider.db().clone());
        store.init().await.unwrap();
        (store, provider)
    }

    #[tokio::test]
    async fn an_event_applies_once_and_opening_again_keeps_the_balance() {
        let (store, _) = store().await;
        let account = AccountId::generate();
        store.open(account, "Ann").await.unwrap();
        let event = new_v7();
        for _ in 0..3 {
            store.apply(event, account, 50).await.unwrap();
        }
        store.open(account, "Ann B.").await.unwrap();
        let row = store.get(account).await.unwrap().unwrap();
        assert_eq!((row.owner.as_str(), row.cents), ("Ann B.", 50));
        assert_eq!(store.list().await.unwrap().len(), 1);
        store.clear().await.unwrap();
        assert!(store.get(account).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn in_strong_mode_the_projection_commits_with_the_events() {
        let (balances, provider) = store().await;
        let bank = register_bank(
            TaroDomain::builder()
                .use_provider(Arc::new(provider))
                .consistency(ConsistencyMode::Strong, None),
            Arc::new(balances.clone()),
        )
        .build()
        .await
        .unwrap();
        let account = AccountId::generate();
        bank.dispatch(OpenAccount {
            account,
            owner: "Ann".into(),
        })
        .await
        .unwrap();
        bank.dispatch(Deposit {
            account,
            cents: 1_000,
        })
        .await
        .unwrap();
        bank.dispatch(Deposit {
            account,
            cents: 250,
        })
        .await
        .unwrap();
        let row = balances.get(account).await.unwrap().unwrap();
        assert_eq!((row.owner.as_str(), row.cents), ("Ann", 1_250));
    }
}
