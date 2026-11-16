//! Every contract suite of `tarodomain-testing` against the SQL provider on
//! SQLite. The database is a new file per case, with several connections, so
//! the concurrency suites see real contention (WAL mode, `BEGIN IMMEDIATE`).

#![cfg(feature = "sqlite")]

use std::sync::atomic::{AtomicU32, Ordering};

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tarodomain::adapters::Provider;
use tarodomain_sqlx::provider::SqlxProvider;

static NEXT: AtomicU32 = AtomicU32::new(0);

/// A provider over a new, migrated database file.
async fn provider() -> SqlxProvider {
    let path = std::env::temp_dir().join(format!(
        "tarodomain-sqlx-{}-{}.db",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_file(&path);
    let options = SqliteConnectOptions::new()
        .filename(&path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    let provider = SqlxProvider::sqlite(pool);
    provider.initialize().await.unwrap();
    // Initializing twice is harmless.
    provider.initialize().await.unwrap();
    provider
}

tarodomain_testing::provider_contract_tests!(provider);
