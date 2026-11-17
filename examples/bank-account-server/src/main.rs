//! The bank example as a server: axum over SQLite through `tarodomain-sqlx`.
//!
//! * Commands: `POST /commands/{name}`, `GET /health` (the core's HTTP API).
//! * The read model: `GET /balances`, kept in the same database. It's written
//!   through the provider's statement layer, so in strong mode it commits
//!   with the events.
//! * A [`Worker`] runs catch-up, saga timeouts and cleanup, and catches up at
//!   once when the eventual bus signals.
//!
//! Environment: `DATABASE_URL` (default `sqlite://bank.db`), `ADDR` (default
//! `127.0.0.1:3000`), `CONSISTENCY` (`eventual`, the default, or `strong`).

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::routing::get;
use bank_account::account::AccountId;
use bank_account::projection::{AccountBalance, BalanceStore};
use bank_account::register_bank;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tarodomain::adapters::{ConsistencyMode, Provider};
use tarodomain::async_trait;
use tarodomain::error::Result;
use tarodomain::framework::{TaroDomain, Worker};
use tarodomain::kernel::ids::Uuid;
use tarodomain_sqlx::db::{Db, Param, Row};
use tarodomain_sqlx::provider::SqlxProvider;

/// The application's own tables, beside the framework's.
const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS bank_balances (\
       account TEXT PRIMARY KEY, owner TEXT NOT NULL DEFAULT '', cents INTEGER NOT NULL DEFAULT 0\
     ) WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS bank_applied_events (event_id TEXT PRIMARY KEY) WITHOUT ROWID",
];

/// The balance read model in SQL. Every write is idempotent (§14.3).
#[derive(Clone)]
struct SqlBalanceStore {
    db: Db,
}

impl SqlBalanceStore {
    async fn init(&self) -> Result<()> {
        for sql in SCHEMA {
            self.db.execute(sql, vec![]).await?;
        }
        Ok(())
    }
}

fn balance(row: &Row) -> Result<AccountBalance> {
    Ok(AccountBalance {
        account: AccountId::from_existing(&row.str("account")?)?,
        owner: row.str("owner")?,
        cents: row.i64("cents")?,
    })
}

#[async_trait]
impl BalanceStore for SqlBalanceStore {
    async fn open(&self, account: AccountId, owner: &str) -> Result<()> {
        self.db
            .execute(
                "INSERT INTO bank_balances (account, owner, cents) VALUES (?, ?, 0) \
                 ON CONFLICT (account) DO UPDATE SET owner = excluded.owner",
                vec![Param::text(account.to_string()), Param::text(owner)],
            )
            .await?;
        Ok(())
    }

    async fn apply(&self, event_id: Uuid, account: AccountId, delta_cents: i64) -> Result<()> {
        // The change and its marker commit together, or not at all.
        self.db
            .atomically(async {
                let applied = self
                    .db
                    .execute(
                        "INSERT INTO bank_applied_events (event_id) VALUES (?) \
                         ON CONFLICT DO NOTHING",
                        vec![Param::text(event_id.to_string())],
                    )
                    .await?;
                if applied == 1 {
                    self.db
                        .execute(
                            "INSERT INTO bank_balances (account, owner, cents) VALUES (?, '', ?) \
                             ON CONFLICT (account) DO UPDATE SET cents = cents + excluded.cents",
                            vec![Param::text(account.to_string()), Param::Int(delta_cents)],
                        )
                        .await?;
                }
                Ok(())
            })
            .await
    }

    async fn get(&self, account: AccountId) -> Result<Option<AccountBalance>> {
        let row = self
            .db
            .fetch_one(
                "SELECT account, owner, cents FROM bank_balances WHERE account = ?",
                vec![Param::text(account.to_string())],
            )
            .await?;
        row.as_ref().map(balance).transpose()
    }

    async fn list(&self) -> Result<Vec<AccountBalance>> {
        let rows = self
            .db
            .fetch(
                "SELECT account, owner, cents FROM bank_balances ORDER BY account",
                vec![],
            )
            .await?;
        rows.iter().map(balance).collect()
    }

    async fn clear(&self) -> Result<()> {
        self.db.execute("DELETE FROM bank_balances", vec![]).await?;
        self.db
            .execute("DELETE FROM bank_applied_events", vec![])
            .await?;
        Ok(())
    }
}

async fn balances(store: SqlBalanceStore) -> Json<serde_json::Value> {
    match store.list().await {
        Ok(rows) => Json(serde_json::to_value(rows).unwrap_or_default()),
        Err(error) => Json(error.to_http_response().body),
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let url = std::env::var("DATABASE_URL").unwrap_or_else(|_| "sqlite://bank.db".into());
    let addr = std::env::var("ADDR").unwrap_or_else(|_| "127.0.0.1:3000".into());
    let mode = match std::env::var("CONSISTENCY").as_deref() {
        Ok("strong") => ConsistencyMode::Strong,
        _ => ConsistencyMode::Eventual,
    };

    let options = SqliteConnectOptions::from_str(&url)?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await?;

    let provider = Arc::new(SqlxProvider::sqlite(pool));
    provider.initialize().await?;
    let store = SqlBalanceStore {
        db: provider.db().clone(),
    };
    store.init().await?;

    let builder = TaroDomain::builder()
        .use_provider(provider.clone())
        .consistency(mode, None);
    let domain = register_bank(builder, Arc::new(store.clone()) as Arc<dyn BalanceStore>)
        .build()
        .await?;

    let worker = Worker::new(domain.clone())
        .catch_up_every(Duration::from_secs(1))
        .wake_on(provider.wakeups());
    let reads = store.clone();
    let app =
        tarodomain_axum::router(domain).route("/balances", get(move || balances(reads.clone())));

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    tracing::info!(%addr, ?mode, "serving the bank");

    let (stop, stopped) = tokio::sync::watch::channel(false);
    let jobs = tokio::spawn(async move {
        let mut stopped = stopped;
        worker
            .run(async move {
                let _ = stopped.wait_for(|stop| *stop).await;
            })
            .await;
    });
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    let _ = stop.send(true);
    let _ = jobs.await;
    Ok(())
}
