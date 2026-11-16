//! The statement layer: the one place that runs SQL.
//!
//! [`Db`] wraps a SQLite or PostgreSQL pool. Every adapter goes through it,
//! and a source-scan test (`tests/db_scan.rs`) enforces that, because an
//! adapter that ran a query on the pool instead of the active transaction
//! would escape the unit of work (plan Risks: a query that forgets the
//! transaction). On PostgreSQL it would commit on its own; on SQLite it would
//! wait on the open transaction and hang.
//!
//! SQL is written once, with `?` placeholders, and runs on both databases.
//! The few differences live here: placeholders become `$1`, `$2`, … on
//! PostgreSQL, and [`Param`] and [`Row`] hide how a UUID, a timestamp or JSON
//! is stored (text on SQLite, `uuid`, `timestamptz` and `jsonb` on
//! PostgreSQL).

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::Value;
use sqlx::Row as _;
use tarodomain::domain::{Metadata, format_timestamp, parse_timestamp};
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::ids::{Uuid, parse_uuid};

use crate::unit_of_work::{SqlxUnitOfWork, Tx};

/// Which database a [`Db`] talks to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dialect {
    /// SQLite.
    Sqlite,
    /// PostgreSQL.
    Postgres,
}

/// The type of a `NULL` parameter. PostgreSQL types every parameter, so a
/// bare `NULL` bound to a `uuid` column is rejected.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NullKind {
    /// A `NULL` integer.
    Int,
    /// A `NULL` string.
    Text,
    /// A `NULL` UUID.
    Uuid,
    /// A `NULL` timestamp.
    Time,
    /// A `NULL` JSON value.
    Json,
}

/// A value bound to a `?` placeholder.
#[derive(Clone, Debug, PartialEq)]
pub enum Param {
    /// A typed `NULL`.
    Null(NullKind),
    /// A boolean (stored as 0 or 1 on SQLite).
    Bool(bool),
    /// An integer.
    Int(i64),
    /// Text.
    Text(String),
    /// A UUID.
    Uuid(Uuid),
    /// A timestamp, kept to the millisecond.
    Time(DateTime<Utc>),
    /// A JSON value.
    Json(Value),
}

impl Param {
    /// Text.
    pub fn text(value: impl Into<String>) -> Self {
        Self::Text(value.into())
    }

    /// Text, or a `NULL` string.
    pub fn opt_text(value: Option<impl Into<String>>) -> Self {
        value.map_or(Self::Null(NullKind::Text), |v| Self::Text(v.into()))
    }

    /// A UUID, or a `NULL` UUID.
    pub fn opt_uuid(value: Option<Uuid>) -> Self {
        value.map_or(Self::Null(NullKind::Uuid), Self::Uuid)
    }

    /// A timestamp, or a `NULL` timestamp.
    pub fn opt_time(value: Option<DateTime<Utc>>) -> Self {
        value.map_or(Self::Null(NullKind::Time), Self::Time)
    }

    /// An integer, or a `NULL` integer.
    pub fn opt_int(value: Option<i64>) -> Self {
        value.map_or(Self::Null(NullKind::Int), Self::Int)
    }

    /// The JSON form of `value`.
    pub fn json(value: &impl serde::Serialize) -> Result<Self> {
        serde_json::to_value(value)
            .map(Self::Json)
            .map_err(|error| {
                FrameworkError::serialization_error("a value has no JSON form").with_source(error)
            })
    }
}

impl From<i64> for Param {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<u64> for Param {
    fn from(value: u64) -> Self {
        Self::Int(i64::try_from(value).unwrap_or(i64::MAX))
    }
}

impl From<u32> for Param {
    fn from(value: u32) -> Self {
        Self::Int(i64::from(value))
    }
}

impl From<&str> for Param {
    fn from(value: &str) -> Self {
        Self::Text(value.to_owned())
    }
}

impl From<String> for Param {
    fn from(value: String) -> Self {
        Self::Text(value)
    }
}

impl From<Uuid> for Param {
    fn from(value: Uuid) -> Self {
        Self::Uuid(value)
    }
}

impl From<DateTime<Utc>> for Param {
    fn from(value: DateTime<Utc>) -> Self {
        Self::Time(value)
    }
}

/// One row of a result.
pub enum Row {
    /// A SQLite row.
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::sqlite::SqliteRow),
    /// A PostgreSQL row.
    #[cfg(feature = "postgres")]
    Postgres(sqlx::postgres::PgRow),
}

fn decode_error(column: &str, error: sqlx::Error) -> FrameworkError {
    FrameworkError::serialization_error(format!("column `{column}` can't be read"))
        .with_source(error)
}

fn missing(column: &str) -> FrameworkError {
    FrameworkError::serialization_error(format!("column `{column}` is NULL"))
}

impl Row {
    /// The integer in `column`, or `None` for `NULL`.
    pub fn opt_i64(&self, column: &str) -> Result<Option<i64>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(row) => row.try_get(column),
            #[cfg(feature = "postgres")]
            Self::Postgres(row) => row.try_get(column),
        }
        .map_err(|error| decode_error(column, error))
    }

    /// The integer in `column`.
    pub fn i64(&self, column: &str) -> Result<i64> {
        self.opt_i64(column)?.ok_or_else(|| missing(column))
    }

    /// The text in `column`, or `None` for `NULL`.
    pub fn opt_str(&self, column: &str) -> Result<Option<String>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(row) => row.try_get(column),
            #[cfg(feature = "postgres")]
            Self::Postgres(row) => row.try_get(column),
        }
        .map_err(|error| decode_error(column, error))
    }

    /// The text in `column`.
    pub fn str(&self, column: &str) -> Result<String> {
        self.opt_str(column)?.ok_or_else(|| missing(column))
    }

    /// The UUID in `column`, or `None` for `NULL`.
    pub fn opt_uuid(&self, column: &str) -> Result<Option<Uuid>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => self
                .opt_str(column)?
                .map(|text| {
                    parse_uuid(&text).ok_or_else(|| {
                        FrameworkError::serialization_error(format!(
                            "column `{column}` is not a UUID: {text}"
                        ))
                    })
                })
                .transpose(),
            #[cfg(feature = "postgres")]
            Self::Postgres(row) => row
                .try_get::<Option<Uuid>, _>(column)
                .map_err(|error| decode_error(column, error)),
        }
    }

    /// The UUID in `column`.
    pub fn uuid(&self, column: &str) -> Result<Uuid> {
        self.opt_uuid(column)?.ok_or_else(|| missing(column))
    }

    /// The timestamp in `column`, or `None` for `NULL`.
    pub fn opt_time(&self, column: &str) -> Result<Option<DateTime<Utc>>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => self
                .opt_str(column)?
                .map(|text| parse_timestamp(&text))
                .transpose(),
            #[cfg(feature = "postgres")]
            Self::Postgres(row) => row
                .try_get::<Option<DateTime<Utc>>, _>(column)
                .map_err(|error| decode_error(column, error)),
        }
    }

    /// The timestamp in `column`.
    pub fn time(&self, column: &str) -> Result<DateTime<Utc>> {
        self.opt_time(column)?.ok_or_else(|| missing(column))
    }

    /// The JSON value in `column`, or `None` for `NULL`.
    pub fn opt_json(&self, column: &str) -> Result<Option<Value>> {
        match self {
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => self
                .opt_str(column)?
                .map(|text| {
                    serde_json::from_str(&text).map_err(|error| {
                        FrameworkError::serialization_error(format!(
                            "column `{column}` is not JSON"
                        ))
                        .with_source(error)
                    })
                })
                .transpose(),
            #[cfg(feature = "postgres")]
            Self::Postgres(row) => row
                .try_get::<Option<Value>, _>(column)
                .map_err(|error| decode_error(column, error)),
        }
    }

    /// The JSON value in `column`.
    pub fn json(&self, column: &str) -> Result<Value> {
        self.opt_json(column)?.ok_or_else(|| missing(column))
    }

    /// The JSON object in `column` as [`Metadata`].
    pub fn metadata(&self, column: &str) -> Result<Metadata> {
        serde_json::from_value(self.json(column)?).map_err(|error| {
            FrameworkError::serialization_error(format!("column `{column}` is not metadata"))
                .with_source(error)
        })
    }
}

/// The pool behind a [`Db`].
pub(crate) enum Pool {
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool),
    #[cfg(feature = "postgres")]
    Postgres(sqlx::PgPool),
}

/// The way every adapter reaches the database.
///
/// Inside a unit of work (§11.4) a query runs on the unit's transaction;
/// otherwise it runs on the pool. Clones are handles to the same pool.
#[derive(Clone)]
pub struct Db {
    pool: Arc<Pool>,
}

impl fmt::Debug for Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Db({:?})", self.dialect())
    }
}

/// `?` placeholders as PostgreSQL's `$1`, `$2`, …
fn numbered(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len() + 8);
    let mut n = 0;
    for c in sql.chars() {
        if c == '?' {
            n += 1;
            out.push('$');
            out.push_str(&n.to_string());
        } else {
            out.push(c);
        }
    }
    out
}

/// Converts a driver error. A violated unique key or a transaction the
/// database asks to retry is `CONCURRENT_MODIFICATION` (retryable), which
/// adapters map to their own conflict error; anything else is an
/// infrastructure error.
pub(crate) fn to_framework_error(what: &str, error: sqlx::Error) -> FrameworkError {
    if let Some(database) = error.as_database_error() {
        let serialization = matches!(database.code().as_deref(), Some("40001" | "40P01"));
        if database.is_unique_violation() || serialization {
            return FrameworkError::concurrent_modification(format!("{what}: {database}"))
                .with_source(error);
        }
    }
    FrameworkError::event_store_error(format!("{what}: {error}")).with_source(error)
}

/// Whether `error` is the typed conflict the statement layer returns for a violated unique key.
pub fn is_conflict(error: &FrameworkError) -> bool {
    *error.code() == tarodomain::error::ErrorCode::ConcurrentModification
}

#[cfg(feature = "sqlite")]
fn bind_sqlite<'q>(
    sql: &'q str,
    params: &'q [Param],
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments> {
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
    for param in params {
        query = match param {
            Param::Null(_) => query.bind(None::<i64>),
            Param::Bool(b) => query.bind(i64::from(*b)),
            Param::Int(i) => query.bind(*i),
            Param::Text(s) => query.bind(s.as_str()),
            Param::Uuid(u) => query.bind(u.hyphenated().to_string()),
            Param::Time(t) => query.bind(format_timestamp(*t)),
            Param::Json(v) => query.bind(v.to_string()),
        };
    }
    query
}

#[cfg(feature = "postgres")]
fn bind_postgres<'q>(
    sql: &'q str,
    params: &'q [Param],
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    let mut query = sqlx::query(sqlx::AssertSqlSafe(sql));
    for param in params {
        query = match param {
            Param::Null(NullKind::Int) => query.bind(None::<i64>),
            Param::Null(NullKind::Text) => query.bind(None::<String>),
            Param::Null(NullKind::Uuid) => query.bind(None::<Uuid>),
            Param::Null(NullKind::Time) => query.bind(None::<DateTime<Utc>>),
            Param::Null(NullKind::Json) => query.bind(None::<Value>),
            Param::Bool(b) => query.bind(*b),
            Param::Int(i) => query.bind(*i),
            Param::Text(s) => query.bind(s.as_str()),
            Param::Uuid(u) => query.bind(*u),
            Param::Time(t) => query.bind(*t),
            Param::Json(v) => query.bind(v),
        };
    }
    query
}

impl Db {
    /// A statement layer over a SQLite pool.
    #[cfg(feature = "sqlite")]
    pub fn sqlite(pool: sqlx::SqlitePool) -> Self {
        Self {
            pool: Arc::new(Pool::Sqlite(pool)),
        }
    }

    /// A statement layer over a PostgreSQL pool.
    #[cfg(feature = "postgres")]
    pub fn postgres(pool: sqlx::PgPool) -> Self {
        Self {
            pool: Arc::new(Pool::Postgres(pool)),
        }
    }

    /// Which database this is.
    pub fn dialect(&self) -> Dialect {
        match &*self.pool {
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(_) => Dialect::Sqlite,
            #[cfg(feature = "postgres")]
            Pool::Postgres(_) => Dialect::Postgres,
        }
    }

    /// Whether `other` talks to the same pool.
    pub(crate) fn same_pool(&self, other: &Db) -> bool {
        Arc::ptr_eq(&self.pool, &other.pool)
    }

    /// Applies the migrations that are missing. Running it again, or from
    /// several processes at once, is safe (sqlx records each migration and
    /// locks while it runs them).
    pub async fn migrate(&self) -> Result<()> {
        match &*self.pool {
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(pool) => crate::SQLITE_MIGRATIONS.run(pool).await,
            #[cfg(feature = "postgres")]
            Pool::Postgres(pool) => crate::POSTGRES_MIGRATIONS.run(pool).await,
        }
        .map_err(|error| {
            FrameworkError::event_store_error(format!("migration failed: {error}"))
                .with_source(error)
        })
    }

    /// Whether the task's active unit of work is a SQL unit over this pool.
    pub fn in_unit(&self) -> bool {
        SqlxUnitOfWork::active_for(self).is_some()
    }

    /// Runs `work` in a transaction: joins the active unit, or, outside one,
    /// begins a unit of its own that commits when `work` succeeds and rolls
    /// back when it fails.
    pub async fn atomically<T, F>(&self, work: F) -> Result<T>
    where
        F: std::future::Future<Output = Result<T>>,
    {
        let unit: Arc<dyn tarodomain::adapters::UnitOfWork> =
            Arc::new(SqlxUnitOfWork::new(self.clone()));
        unit.execute(work).await
    }

    /// The bound for a `LIMIT ?`: `None` means no limit.
    pub fn limit(&self, max_count: Option<usize>) -> Param {
        match (self.dialect(), max_count) {
            (_, Some(count)) => Param::Int(i64::try_from(count).unwrap_or(i64::MAX)),
            // SQLite reads a negative LIMIT as "no limit", PostgreSQL a NULL one.
            (Dialect::Sqlite, None) => Param::Int(-1),
            (Dialect::Postgres, None) => Param::Null(NullKind::Int),
        }
    }

    /// Runs a query and returns its rows: on the active unit's transaction
    /// if there is one over this pool, on the pool otherwise.
    pub async fn fetch(&self, sql: &str, params: Vec<Param>) -> Result<Vec<Row>> {
        if let Some(unit) = SqlxUnitOfWork::active_for(self) {
            return unit.fetch(sql, &params).await;
        }
        self.fetch_on_pool(sql, &params).await
    }

    /// Runs a query that returns at most one row.
    pub async fn fetch_one(&self, sql: &str, params: Vec<Param>) -> Result<Option<Row>> {
        Ok(self.fetch(sql, params).await?.into_iter().next())
    }

    /// Runs a statement and returns the number of rows it changed.
    pub async fn execute(&self, sql: &str, params: Vec<Param>) -> Result<u64> {
        if let Some(unit) = SqlxUnitOfWork::active_for(self) {
            return unit.execute(sql, &params).await;
        }
        self.execute_on_pool(sql, &params).await
    }

    async fn fetch_on_pool(&self, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
        match &*self.pool {
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(pool) => bind_sqlite(sql, params)
                .fetch_all(pool)
                .await
                .map(|rows| rows.into_iter().map(Row::Sqlite).collect()),
            #[cfg(feature = "postgres")]
            Pool::Postgres(pool) => bind_postgres(&numbered(sql), params)
                .fetch_all(pool)
                .await
                .map(|rows| rows.into_iter().map(Row::Postgres).collect()),
        }
        .map_err(|error| to_framework_error("query", error))
    }

    async fn execute_on_pool(&self, sql: &str, params: &[Param]) -> Result<u64> {
        match &*self.pool {
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(pool) => bind_sqlite(sql, params)
                .execute(pool)
                .await
                .map(|done| done.rows_affected()),
            #[cfg(feature = "postgres")]
            Pool::Postgres(pool) => bind_postgres(&numbered(sql), params)
                .execute(pool)
                .await
                .map(|done| done.rows_affected()),
        }
        .map_err(|error| to_framework_error("statement", error))
    }

    /// Starts a transaction that belongs to the caller, whatever unit of
    /// work is active. SQLite takes the write lock at once (`BEGIN
    /// IMMEDIATE`), so two writers queue instead of failing at their first
    /// write.
    pub(crate) async fn begin(&self) -> Result<Tx> {
        match &*self.pool {
            #[cfg(feature = "sqlite")]
            Pool::Sqlite(pool) => pool.begin_with("BEGIN IMMEDIATE").await.map(Tx::Sqlite),
            #[cfg(feature = "postgres")]
            Pool::Postgres(pool) => pool.begin().await.map(Tx::Postgres),
        }
        .map_err(|error| to_framework_error("begin", error))
    }
}

/// Runs a query on a transaction.
pub(crate) async fn fetch_in(tx: &mut Tx, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
    match tx {
        #[cfg(feature = "sqlite")]
        Tx::Sqlite(tx) => bind_sqlite(sql, params)
            .fetch_all(&mut **tx)
            .await
            .map(|rows| rows.into_iter().map(Row::Sqlite).collect()),
        #[cfg(feature = "postgres")]
        Tx::Postgres(tx) => bind_postgres(&numbered(sql), params)
            .fetch_all(&mut **tx)
            .await
            .map(|rows| rows.into_iter().map(Row::Postgres).collect()),
    }
    .map_err(|error| to_framework_error("query", error))
}

/// Runs a statement on a transaction.
pub(crate) async fn execute_in(tx: &mut Tx, sql: &str, params: &[Param]) -> Result<u64> {
    match tx {
        #[cfg(feature = "sqlite")]
        Tx::Sqlite(tx) => bind_sqlite(sql, params)
            .execute(&mut **tx)
            .await
            .map(|done| done.rows_affected()),
        #[cfg(feature = "postgres")]
        Tx::Postgres(tx) => bind_postgres(&numbered(sql), params)
            .execute(&mut **tx)
            .await
            .map(|done| done.rows_affected()),
    }
    .map_err(|error| to_framework_error("statement", error))
}

/// Commits a transaction.
pub(crate) async fn commit(tx: Tx) -> Result<()> {
    match tx {
        #[cfg(feature = "sqlite")]
        Tx::Sqlite(tx) => tx.commit().await,
        #[cfg(feature = "postgres")]
        Tx::Postgres(tx) => tx.commit().await,
    }
    .map_err(|error| to_framework_error("commit", error))
}

/// Rolls a transaction back.
pub(crate) async fn rollback(tx: Tx) -> Result<()> {
    match tx {
        #[cfg(feature = "sqlite")]
        Tx::Sqlite(tx) => tx.rollback().await,
        #[cfg(feature = "postgres")]
        Tx::Postgres(tx) => tx.rollback().await,
    }
    .map_err(|error| to_framework_error("rollback", error))
}

/// A transaction that isn't part of any unit of work: the Sequencer's, and
/// maintenance that must commit on its own (job leases).
pub struct DetachedTx {
    tx: Tx,
}

impl Db {
    /// Starts a [`DetachedTx`].
    pub async fn begin_detached(&self) -> Result<DetachedTx> {
        Ok(DetachedTx {
            tx: self.begin().await?,
        })
    }
}

impl DetachedTx {
    /// Runs a query in the transaction.
    pub async fn fetch(&mut self, sql: &str, params: Vec<Param>) -> Result<Vec<Row>> {
        fetch_in(&mut self.tx, sql, &params).await
    }

    /// Runs a statement in the transaction and returns the rows it changed.
    pub async fn execute(&mut self, sql: &str, params: Vec<Param>) -> Result<u64> {
        execute_in(&mut self.tx, sql, &params).await
    }

    /// Commits.
    pub async fn commit(self) -> Result<()> {
        commit(self.tx).await
    }

    /// Rolls back.
    pub async fn rollback(self) -> Result<()> {
        rollback(self.tx).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_are_numbered_for_postgres() {
        assert_eq!(
            numbered("UPDATE t SET a = ?, b = ? WHERE c = ?"),
            "UPDATE t SET a = $1, b = $2 WHERE c = $3"
        );
    }
}
