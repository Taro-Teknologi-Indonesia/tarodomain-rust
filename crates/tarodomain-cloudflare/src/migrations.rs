//! Schema migrations (spec §27.1 table 10).
//!
//! The SQL is embedded in the crate and each applied migration is recorded in
//! `tarodomain_schema_migrations`, so deploying needs no separate migration
//! step: [`migrate`] applies whatever is missing.

use serde_json::json;
use tarodomain::error::{FrameworkError, Result};

use crate::d1::{D1Db, RowExt, Statement};

/// One migration: a version and the SQL that brings the schema to it.
#[derive(Clone, Copy, Debug)]
pub struct Migration {
    /// `(major, minor, patch)`, the key in the schema migrations table.
    pub version: (u32, u32, u32),
    /// What the migration does.
    pub name: &'static str,
    /// The SQL: statements that each end with a semicolon at the end of a
    /// line. Every statement must be safe to run twice (`IF NOT EXISTS`).
    pub sql: &'static str,
}

/// Every migration, in order.
pub const MIGRATIONS: &[Migration] = &[Migration {
    version: (1, 0, 0),
    name: "init",
    sql: include_str!("../migrations/0001_init.sql"),
}];

/// Every table of the schema except the migrations table, for tools that
/// empty a database (the contract harness).
pub const TABLES: &[&str] = &[
    "tarodomain_events",
    "tarodomain_streams",
    "tarodomain_snapshots",
    "tarodomain_subscriptions",
    "tarodomain_processed_events",
    "tarodomain_sagas",
    "tarodomain_dead_letters",
    "tarodomain_processed_commands",
    "tarodomain_locks",
];

/// Deletes every row of every [`TABLES`] table, so a test database starts
/// empty. The schema and the migration records stay. The position and
/// dead-letter counters keep counting (IDs are never reused, and D1 doesn't
/// let a Worker rewrite `sqlite_sequence`).
pub async fn clear_all(db: &D1Db) -> Result<()> {
    let statements: Vec<Statement> = TABLES
        .iter()
        .map(|table| Statement::plain(format!("DELETE FROM {table}")))
        .collect();
    db.write(statements).await
}

const MIGRATIONS_TABLE: &str = "tarodomain_schema_migrations";

/// Splits a migration into statements, dropping `--` comment lines.
pub fn split_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    for line in sql.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }
        current.push_str(line);
        current.push('\n');
        if trimmed.ends_with(';') {
            statements.push(current.trim().trim_end_matches(';').to_owned());
            current.clear();
        }
    }
    if !current.trim().is_empty() {
        statements.push(current.trim().to_owned());
    }
    statements
}

/// Applies every migration that isn't recorded yet and returns the names of
/// those it applied. Running it twice applies nothing the second time.
///
/// Each migration runs in one batch together with its record, so it either
/// applies fully or not at all. When two isolates migrate at once, the loser's
/// batch fails on the record's primary key; it then checks again, and succeeds
/// if the winner applied the migration.
pub async fn migrate(db: &D1Db) -> Result<Vec<&'static str>> {
    migrate_with(db, MIGRATIONS).await
}

pub(crate) async fn migrate_with(db: &D1Db, migrations: &[Migration]) -> Result<Vec<&'static str>> {
    db.run(Statement::plain(format!(
        "CREATE TABLE IF NOT EXISTS {MIGRATIONS_TABLE} (\
         major INTEGER NOT NULL, minor INTEGER NOT NULL, patch INTEGER NOT NULL, \
         name TEXT NOT NULL, applied_at TEXT NOT NULL, \
         PRIMARY KEY (major, minor, patch)) WITHOUT ROWID"
    )))
    .await?;
    let mut applied = Vec::new();
    for migration in migrations {
        if is_applied(db, migration).await? {
            continue;
        }
        let (major, minor, patch) = migration.version;
        // The record goes first, so a loser fails before doing anything else.
        let mut statements = vec![Statement::new(
            format!(
                "INSERT INTO {MIGRATIONS_TABLE} (major, minor, patch, name, applied_at) \
                 VALUES (?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))"
            ),
            vec![
                json!(major),
                json!(minor),
                json!(patch),
                json!(migration.name),
            ],
        )];
        statements.extend(
            split_statements(migration.sql)
                .into_iter()
                .map(Statement::plain),
        );
        match db.write(statements).await {
            Ok(()) => applied.push(migration.name),
            Err(error) => {
                // Lost the race to another isolate, or a real failure.
                if !is_applied(db, migration).await? {
                    return Err(FrameworkError::invalid_configuration(format!(
                        "migration {major}.{minor}.{patch} ({}) failed",
                        migration.name
                    ))
                    .with_source(error));
                }
            }
        }
    }
    Ok(applied)
}

async fn is_applied(db: &D1Db, migration: &Migration) -> Result<bool> {
    let (major, minor, patch) = migration.version;
    let row = db
        .query_one(Statement::new(
            format!(
                "SELECT major FROM {MIGRATIONS_TABLE} WHERE major = ? AND minor = ? AND patch = ?"
            ),
            vec![json!(major), json!(minor), json!(patch)],
        ))
        .await?;
    Ok(row.is_some_and(|row| row.i64("major").is_some()))
}

#[cfg(all(test, feature = "native-sqlite"))]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::sqlite_backend::SqliteBackend;

    fn db() -> D1Db {
        D1Db::new(Arc::new(SqliteBackend::in_memory().unwrap()))
    }

    #[test]
    fn comments_and_blank_lines_are_dropped_and_statements_split_at_line_end_semicolons() {
        let statements = split_statements("-- a\nCREATE TABLE a (\n  x INTEGER\n);\n\nSELECT 1;\n");
        assert_eq!(
            statements,
            vec!["CREATE TABLE a (\n  x INTEGER\n)", "SELECT 1"]
        );
    }

    #[test]
    fn the_embedded_schema_has_every_table() {
        let statements = split_statements(MIGRATIONS[0].sql);
        let tables = statements
            .iter()
            .filter(|s| s.starts_with("CREATE TABLE"))
            .count();
        // Eight spec tables (events folds in stream_events; the migrations
        // table is created by `migrate`) plus locks.
        assert_eq!(tables, 9);
    }

    #[test]
    fn the_table_list_is_the_schema() {
        let created: Vec<String> = split_statements(MIGRATIONS[0].sql)
            .iter()
            .filter_map(|s| s.strip_prefix("CREATE TABLE IF NOT EXISTS "))
            .map(|s| s.split_whitespace().next().unwrap().to_owned())
            .collect();
        assert_eq!(created, TABLES);
    }

    #[tokio::test]
    async fn clear_all_empties_every_table_and_keeps_the_counters() {
        let db = db();
        migrate(&db).await.unwrap();
        db.write(vec![Statement::plain(
            "INSERT INTO tarodomain_dead_letters (handler_name, event_id, event_type, payload, \
             error_code, error_message, attempts, created_at) VALUES ('h', 'e', 't', '{}', 'c', 'm', 1, 'x')",
        )])
        .await
        .unwrap();
        clear_all(&db).await.unwrap();
        db.write(vec![Statement::plain(
            "INSERT INTO tarodomain_dead_letters (handler_name, event_id, event_type, payload, \
             error_code, error_message, attempts, created_at) VALUES ('h', 'e', 't', '{}', 'c', 'm', 1, 'x')",
        )])
        .await
        .unwrap();
        let row = db
            .query_one(Statement::plain("SELECT id FROM tarodomain_dead_letters"))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(row.i64("id"), Some(2), "the counter kept counting");
        assert!(
            migrate(&db).await.unwrap().is_empty(),
            "the migration records stay"
        );
    }

    #[tokio::test]
    async fn migrating_twice_applies_once() {
        let db = db();
        assert_eq!(migrate(&db).await.unwrap(), vec!["init"]);
        assert!(migrate(&db).await.unwrap().is_empty());
        let rows = db
            .query(Statement::plain("SELECT event_id FROM tarodomain_events"))
            .await
            .unwrap();
        assert!(rows.is_empty(), "the schema exists");
    }

    #[tokio::test]
    async fn losing_the_race_is_not_an_error() {
        let db = db();
        let (a, b) = tokio::join!(migrate(&db), migrate(&db));
        assert!(a.is_ok() && b.is_ok());
        let applied = a.unwrap().len() + b.unwrap().len();
        assert_eq!(applied, 1);
    }

    #[tokio::test]
    async fn a_migration_that_fails_is_a_configuration_error_and_leaves_no_record() {
        let db = db();
        let broken = [Migration {
            version: (9, 0, 0),
            name: "broken",
            sql: "SELECT * FROM nowhere;",
        }];
        let error = migrate_with(&db, &broken).await.unwrap_err();
        assert_eq!(error.code().as_str(), "INVALID_CONFIGURATION");
        assert!(!is_applied(&db, &broken[0]).await.unwrap());
    }
}
