//! Every contract suite of `tarodomain-testing` against the SQL provider on
//! PostgreSQL, plus the cases that only PostgreSQL can show.
//!
//! The tests need a database: set `DATABASE_URL`, for example to the compose
//! service (`postgres://tarodomain:tarodomain@localhost:54329/tarodomain`).
//! Without it they print a message and pass. Each case runs in a schema of its
//! own, so cases don't see each other's rows.

#![cfg(feature = "postgres")]

use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use tarodomain::adapters::{EventStoreAdapter, Provider};
use tarodomain_sqlx::provider::SqlxProvider;
use tarodomain_testing::fixtures::{event_id, events, ids, stream_id};

static NEXT: AtomicU32 = AtomicU32::new(0);

fn database_url() -> Option<String> {
    let url = std::env::var("DATABASE_URL")
        .ok()
        .filter(|url| !url.is_empty());
    if url.is_none() {
        eprintln!("DATABASE_URL is not set: skipping the PostgreSQL contract tests");
    }
    url
}

/// A provider over a new schema of the database, migrated.
async fn provider() -> SqlxProvider {
    let url = database_url().expect("checked by the caller");
    let schema = format!(
        "tarodomain_test_{}_{}_{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
    );
    let options = PgConnectOptions::from_str(&url).unwrap();
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect_with(options.options([("search_path", schema.as_str())]))
        .await
        .unwrap();
    let provider = SqlxProvider::postgres(pool);
    provider.initialize().await.unwrap();
    provider.initialize().await.unwrap();
    provider
}

macro_rules! suites {
    ($($suite:ident)+) => {$(
        #[test]
        fn $suite() {
            if database_url().is_none() {
                return;
            }
            tarodomain_testing::__private::run(tarodomain_testing::$suite(provider));
        }
    )+};
}

suites!(
    event_store_contract
    snapshot_contract
    saga_store_contract
    idempotency_contract
    dead_letter_contract
    unit_of_work_contract
    concurrency_contract
    provider_contract
);

/// A transaction that stays open must not stop other readers and writers, or
/// make them skip its events (plan Risks: global ordering).
#[test]
fn a_reader_makes_progress_while_another_transaction_stays_open() {
    if database_url().is_none() {
        return;
    }
    tarodomain_testing::__private::run(async {
        let provider = provider().await;
        let store: Arc<dyn EventStoreAdapter> = provider.create_event_store_adapter();
        let unit = provider.create_unit_of_work().unwrap();
        let (one, two) = (stream_id("BankAccount", 1), stream_id("BankAccount", 2));

        // Inside the open unit: append, then let another task (which doesn't
        // inherit the unit) write and read.
        let outcome = unit
            .execute(async {
                store
                    .append_to_stream(&one, events([event_id(1)]), 0)
                    .await?;
                let other = Arc::clone(&store);
                let two = two.clone();
                let task = tokio::spawn(async move {
                    other
                        .append_to_stream(&two, events([event_id(2)]), 0)
                        .await?;
                    other.load_all(None, None).await
                });
                let seen = tokio::time::timeout(Duration::from_secs(10), task)
                    .await
                    .expect("the reader isn't blocked by the open transaction")
                    .unwrap()?;
                Ok(ids(&seen))
            })
            .await
            .unwrap();
        // The open unit's event wasn't committed, so the reader didn't see it.
        assert_eq!(outcome, [event_id(2)]);

        // After the commit it is positioned after the event that committed first.
        let all = store.load_all(None, None).await.unwrap();
        assert_eq!(ids(&all), [event_id(2), event_id(1)]);
        // A cursor at the first event now finds the late one, not skipping it.
        let after = store.load_all(Some(event_id(2)), None).await.unwrap();
        assert_eq!(ids(&after), [event_id(1)]);
    });
}
