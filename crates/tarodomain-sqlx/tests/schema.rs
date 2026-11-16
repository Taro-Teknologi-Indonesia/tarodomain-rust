//! The SQLite schema applies cleanly and has every §27.1 table.

use sqlx::sqlite::SqlitePoolOptions;
use tarodomain_sqlx::SQLITE_MIGRATIONS;

#[tokio::test]
async fn sqlite_migrations_create_every_table() {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    SQLITE_MIGRATIONS.run(&pool).await.unwrap();
    // Running twice is a no-op.
    SQLITE_MIGRATIONS.run(&pool).await.unwrap();
    let names: Vec<(String,)> = sqlx::query_as(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'tarodomain_%' ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let names: Vec<_> = names.into_iter().map(|n| n.0).collect();
    for table in [
        "counters",
        "dead_letters",
        "events",
        "locks",
        "processed_commands",
        "processed_events",
        "sagas",
        "snapshots",
        "streams",
        "subscriptions",
    ] {
        assert!(
            names.contains(&format!("tarodomain_{table}")),
            "missing {table}: {names:?}"
        );
    }
}
