//! The statement layer: the one place that runs SQL.
//!
//! [`D1Backend`] is what a database offers (`all`, `run`, `batch`). It has two
//! implementations: workers-rs's `D1Database` on wasm32, and SQLite through
//! rusqlite behind the `native-sqlite` feature, so the adapters' real SQL runs
//! under `cargo test`. [`D1Db`] is the only code that calls a backend; every
//! adapter goes through it. A source-scan test (`tests/d1_scan.rs`) enforces
//! that, because an adapter that ran a query on its own would escape the unit
//! of work (see the plan's Risks: a query that forgets the transaction).

use std::fmt;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use tarodomain::async_trait;
use tarodomain::domain::{Metadata, parse_timestamp};
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::ids::{Uuid, parse_uuid};

use crate::unit_of_work::D1UnitOfWork;

/// D1 allows at most this many bound parameters in one statement.
pub const MAX_BOUND_PARAMS: usize = 100;

/// One row of a result: column name to JSON value (`null`, number, string).
pub type Row = Map<String, Value>;

/// A SQL statement with its bound parameters (`?` placeholders).
///
/// Parameters are JSON values: `null`, booleans (bound as 0 or 1), numbers
/// and strings.
#[derive(Clone, Debug, PartialEq)]
pub struct Statement {
    /// The SQL text.
    pub sql: String,
    /// The values bound to the `?` placeholders, in order.
    pub params: Vec<Value>,
}

impl Statement {
    /// A statement with its parameters.
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }

    /// A statement without parameters.
    pub fn plain(sql: impl Into<String>) -> Self {
        Self::new(sql, Vec::new())
    }
}

/// Reads a column of a [`Row`].
pub trait RowExt {
    /// The text in `column`, if it is a string.
    fn str(&self, column: &str) -> Option<&str>;
    /// The integer in `column`, if it is one.
    fn i64(&self, column: &str) -> Option<i64>;
    /// Like [`str`](Self::str), but a missing or non-text column is an error.
    fn require_str(&self, column: &str) -> Result<&str>;
    /// Like [`i64`](Self::i64), but a missing or non-integer column is an error.
    fn require_i64(&self, column: &str) -> Result<i64>;
    /// The UUID in `column`; a missing or malformed one is an error.
    fn require_uuid(&self, column: &str) -> Result<Uuid>;
    /// The UUID in `column`, or `None` for `NULL`; a malformed one is an error.
    fn optional_uuid(&self, column: &str) -> Result<Option<Uuid>>;
    /// The ISO 8601 timestamp in `column`; a missing or malformed one is an error.
    fn require_time(&self, column: &str) -> Result<DateTime<Utc>>;
    /// The timestamp in `column`, or `None` for `NULL`.
    fn optional_time(&self, column: &str) -> Result<Option<DateTime<Utc>>>;
    /// The JSON text in `column`, parsed.
    fn require_json(&self, column: &str) -> Result<Value>;
    /// The JSON object in `column` as [`Metadata`].
    fn require_metadata(&self, column: &str) -> Result<Metadata>;
}

impl RowExt for Row {
    fn str(&self, column: &str) -> Option<&str> {
        self.get(column).and_then(Value::as_str)
    }

    fn i64(&self, column: &str) -> Option<i64> {
        self.get(column).and_then(|value| {
            value
                .as_i64()
                // D1 may hand integers back as floats.
                .or_else(|| value.as_f64().map(|f| f as i64))
        })
    }

    fn require_str(&self, column: &str) -> Result<&str> {
        self.str(column).ok_or_else(|| {
            FrameworkError::serialization_error(format!("column `{column}` is not text"))
        })
    }

    fn require_i64(&self, column: &str) -> Result<i64> {
        self.i64(column).ok_or_else(|| {
            FrameworkError::serialization_error(format!("column `{column}` is not an integer"))
        })
    }

    fn require_uuid(&self, column: &str) -> Result<Uuid> {
        let text = self.require_str(column)?;
        parse_uuid(text).ok_or_else(|| {
            FrameworkError::serialization_error(format!("column `{column}` is not a UUID: {text}"))
        })
    }

    fn optional_uuid(&self, column: &str) -> Result<Option<Uuid>> {
        match self.get(column) {
            None | Some(Value::Null) => Ok(None),
            Some(_) => self.require_uuid(column).map(Some),
        }
    }

    fn require_time(&self, column: &str) -> Result<DateTime<Utc>> {
        parse_timestamp(self.require_str(column)?)
    }

    fn optional_time(&self, column: &str) -> Result<Option<DateTime<Utc>>> {
        match self.get(column) {
            None | Some(Value::Null) => Ok(None),
            Some(_) => self.require_time(column).map(Some),
        }
    }

    fn require_metadata(&self, column: &str) -> Result<Metadata> {
        serde_json::from_value(self.require_json(column)?).map_err(|error| {
            FrameworkError::serialization_error(format!("column `{column}` is not metadata"))
                .with_source(error)
        })
    }

    fn require_json(&self, column: &str) -> Result<Value> {
        serde_json::from_str(self.require_str(column)?).map_err(|error| {
            FrameworkError::serialization_error(format!("column `{column}` is not JSON"))
                .with_source(error)
        })
    }
}

/// Why a backend call failed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BackendError {
    /// A constraint (unique key, primary key, not null, check) was violated.
    /// A batch that hits one is rolled back as a whole.
    Constraint(String),
    /// Any other failure.
    Other(String),
}

impl fmt::Display for BackendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Constraint(message) => write!(f, "constraint violation: {message}"),
            Self::Other(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for BackendError {}

/// What a database must offer. Implemented for D1 (wasm32) and SQLite
/// (`native-sqlite`).
#[async_trait]
pub trait D1Backend: Send + Sync {
    /// Runs a query and returns its rows.
    async fn all(&self, statement: &Statement) -> Result<Vec<Row>, BackendError>;

    /// Runs a statement and returns the number of rows it changed.
    async fn run(&self, statement: &Statement) -> Result<u64, BackendError>;

    /// Runs the statements in order as one transaction: either all of them
    /// take effect or none do.
    async fn batch(&self, statements: &[Statement]) -> Result<(), BackendError>;
}

/// The way every adapter reaches the database.
///
/// Reads run at once. Writes run at once outside a unit of work, and are
/// staged in the active unit inside one (§11.4), to be sent in a single batch
/// at commit. Clones are handles to the same backend.
#[derive(Clone)]
pub struct D1Db {
    backend: Arc<dyn D1Backend>,
}

impl D1Db {
    /// A statement layer over `backend`.
    pub fn new(backend: Arc<dyn D1Backend>) -> Self {
        Self { backend }
    }

    /// Runs a query and returns its rows, at once. Inside a unit of work this
    /// sees the database as it was before the unit began (the adapters add the
    /// unit's own writes from its overlay).
    pub async fn query(&self, statement: Statement) -> Result<Vec<Row>> {
        self.backend
            .all(&statement)
            .await
            .map_err(|error| to_framework_error("query", error))
    }

    /// Runs a query that returns at most one row.
    pub async fn query_one(&self, statement: Statement) -> Result<Option<Row>> {
        Ok(self.query(statement).await?.into_iter().next())
    }

    /// Writes `statements` atomically.
    ///
    /// Outside a unit of work they run at once as one batch. A violated
    /// constraint fails the batch and comes back as `CONCURRENT_MODIFICATION`
    /// (retryable), so an adapter can map it to its own conflict error.
    pub async fn write(&self, statements: Vec<Statement>) -> Result<()> {
        if statements.is_empty() {
            return Ok(());
        }
        // Inside a unit of work the statements join its batch.
        let staged = D1UnitOfWork::with_active(self, |overlay| {
            overlay.statements.extend(statements.iter().cloned());
        });
        if staged.is_some() {
            return Ok(());
        }
        self.flush(&statements).await
    }

    /// Writes one statement that returns rows (`INSERT … RETURNING`).
    ///
    /// Outside a unit of work it runs at once and returns the rows. Inside
    /// one it is staged like any write and returns `None`, because the rows
    /// aren't known before the commit.
    pub async fn write_returning(&self, statement: Statement) -> Result<Option<Vec<Row>>> {
        let staged = D1UnitOfWork::with_active(self, |overlay| {
            overlay.statements.push(statement.clone());
        });
        if staged.is_some() {
            return Ok(None);
        }
        self.query(statement).await.map(Some)
    }

    /// Writes a `DELETE` and returns how many rows it removed.
    ///
    /// Outside a unit of work that is the number of rows the statement
    /// changed. Inside one the delete is staged, and the count comes from
    /// `count` (a query with a `n` column) run before the unit commits.
    pub async fn write_counting(&self, delete: Statement, count: Statement) -> Result<u64> {
        let in_unit = D1UnitOfWork::with_active(self, |_| ()).is_some();
        if !in_unit {
            return self.run(delete).await;
        }
        let counted = self
            .query_one(count)
            .await?
            .map_or(Ok(0), |row| row.require_i64("n"))?;
        self.write(vec![delete]).await?;
        Ok(u64::try_from(counted).unwrap_or(0))
    }

    /// Sends `statements` to the backend as one batch, whatever the active
    /// unit is. The unit of work's commit uses this; adapters use
    /// [`write`](Self::write).
    pub(crate) async fn flush(&self, statements: &[Statement]) -> Result<()> {
        self.backend
            .batch(statements)
            .await
            .map_err(|error| to_framework_error("write", error))
    }

    /// Whether `other` talks to the same database.
    pub(crate) fn same_backend(&self, other: &D1Db) -> bool {
        Arc::ptr_eq(&self.backend, &other.backend)
    }

    /// Runs one statement at once, outside any batch, and returns the number
    /// of rows it changed. For maintenance that never joins a unit (job
    /// leases, migrations).
    pub async fn run(&self, statement: Statement) -> Result<u64> {
        self.backend
            .run(&statement)
            .await
            .map_err(|error| to_framework_error("write", error))
    }
}

impl fmt::Debug for D1Db {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("D1Db")
    }
}

/// Converts a backend error. A constraint violation is the typed error the
/// adapters map; anything else is an infrastructure error.
pub(crate) fn to_framework_error(what: &str, error: BackendError) -> FrameworkError {
    match error {
        BackendError::Constraint(message) => {
            FrameworkError::concurrent_modification(format!("D1 {what}: {message}"))
        }
        BackendError::Other(message) => {
            crate::platform::infrastructure(&format!("D1 {what}"), message)
        }
    }
}

/// Whether `error` is the typed constraint violation of
/// [`D1Db::write`].
pub fn is_constraint_violation(error: &FrameworkError) -> bool {
    *error.code() == tarodomain::error::ErrorCode::ConcurrentModification
}

/// The JSON text of `value`, for a `TEXT` column.
pub fn json_text(value: &impl serde::Serialize) -> Value {
    Value::String(serde_json::to_string(value).unwrap_or_else(|_| "null".into()))
}

/// Builds the `INSERT` statements for `rows`, splitting them so that no
/// statement binds more than [`MAX_BOUND_PARAMS`] values.
///
/// `table` and `columns` are SQL text written by the adapter, never user
/// input. `suffix` follows the `VALUES` list, for example
/// `"ON CONFLICT DO NOTHING"`.
pub fn insert_rows(
    table: &str,
    columns: &[&str],
    rows: Vec<Vec<Value>>,
    suffix: &str,
) -> Vec<Statement> {
    assert!(
        !columns.is_empty() && columns.len() <= MAX_BOUND_PARAMS,
        "a table needs 1 to {MAX_BOUND_PARAMS} columns"
    );
    let per_statement = MAX_BOUND_PARAMS / columns.len();
    let mut statements = Vec::new();
    let mut rows = rows.into_iter().peekable();
    while rows.peek().is_some() {
        let chunk: Vec<Vec<Value>> = rows.by_ref().take(per_statement).collect();
        let placeholders = format!("({})", vec!["?"; columns.len()].join(", "));
        let values = vec![placeholders; chunk.len()].join(", ");
        let sql = format!(
            "INSERT INTO {table} ({}) VALUES {values} {suffix}",
            columns.join(", ")
        )
        .trim_end()
        .to_owned();
        let params = chunk
            .into_iter()
            .inspect(|row| debug_assert_eq!(row.len(), columns.len()))
            .flatten()
            .collect();
        statements.push(Statement::new(sql, params));
    }
    statements
}

#[cfg(target_arch = "wasm32")]
pub use worker_backend::WorkerD1;

#[cfg(target_arch = "wasm32")]
mod worker_backend {
    use serde_json::Value;
    use tarodomain::async_trait;
    use worker::send::SendFuture;
    use worker::wasm_bindgen::JsValue;

    use super::{BackendError, D1Backend, Row, Statement};
    use crate::platform::Handle;

    /// The Cloudflare D1 backend: a `D1Database` binding of the Worker.
    pub struct WorkerD1 {
        db: Handle<worker::D1Database>,
    }

    impl WorkerD1 {
        /// Wraps a D1 binding.
        pub fn new(db: worker::D1Database) -> Self {
            Self {
                db: Handle::new(db),
            }
        }

        fn prepare(
            &self,
            statement: &Statement,
        ) -> Result<worker::D1PreparedStatement, BackendError> {
            let params: Vec<JsValue> = statement.params.iter().map(to_js).collect();
            self.db
                .prepare(statement.sql.as_str())
                .bind(&params)
                .map_err(convert)
        }
    }

    fn to_js(value: &Value) -> JsValue {
        match value {
            Value::Null => JsValue::NULL,
            Value::Bool(flag) => JsValue::from_f64(f64::from(u8::from(*flag))),
            Value::Number(number) => JsValue::from_f64(number.as_f64().unwrap_or_default()),
            Value::String(text) => JsValue::from_str(text),
            other => JsValue::from_str(&other.to_string()),
        }
    }

    fn convert(error: worker::Error) -> BackendError {
        let message = error.to_string();
        if message.to_ascii_lowercase().contains("constraint") {
            BackendError::Constraint(message)
        } else {
            BackendError::Other(message)
        }
    }

    #[async_trait]
    impl D1Backend for WorkerD1 {
        async fn all(&self, statement: &Statement) -> Result<Vec<Row>, BackendError> {
            let prepared = self.prepare(statement)?;
            SendFuture::new(async move {
                let result = prepared.all().await.map_err(convert)?;
                result.results::<Row>().map_err(convert)
            })
            .await
        }

        async fn run(&self, statement: &Statement) -> Result<u64, BackendError> {
            let prepared = self.prepare(statement)?;
            SendFuture::new(async move {
                let result = prepared.run().await.map_err(convert)?;
                let changes = result
                    .meta()
                    .map_err(convert)?
                    .and_then(|meta| meta.changes)
                    .unwrap_or(0);
                Ok(changes as u64)
            })
            .await
        }

        async fn batch(&self, statements: &[Statement]) -> Result<(), BackendError> {
            let prepared = statements
                .iter()
                .map(|statement| self.prepare(statement))
                .collect::<Result<Vec<_>, _>>()?;
            SendFuture::new(async move { self.db.batch(prepared).await.map(drop).map_err(convert) })
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn inserts_are_split_to_fit_the_bound_parameter_limit() {
        let rows: Vec<Vec<Value>> = (0..120)
            .map(|n| vec![json!(n), json!("x"), json!(null)])
            .collect();
        let statements = insert_rows("t", &["a", "b", "c"], rows, "ON CONFLICT DO NOTHING");
        // 100 / 3 = 33 rows per statement: 33 + 33 + 33 + 21.
        assert_eq!(statements.len(), 4);
        assert!(
            statements
                .iter()
                .all(|s| s.params.len() <= MAX_BOUND_PARAMS)
        );
        assert_eq!(
            statements.iter().map(|s| s.params.len()).sum::<usize>(),
            360
        );
        assert!(
            statements[0]
                .sql
                .starts_with("INSERT INTO t (a, b, c) VALUES (?, ?, ?), (?, ?, ?)")
        );
        assert!(statements[0].sql.ends_with("ON CONFLICT DO NOTHING"));
    }

    #[test]
    fn no_rows_make_no_statements_and_an_empty_suffix_leaves_no_trailing_space() {
        assert!(insert_rows("t", &["a"], vec![], "").is_empty());
        let one = insert_rows("t", &["a"], vec![vec![json!(1)]], "");
        assert_eq!(one[0].sql, "INSERT INTO t (a) VALUES (?)");
    }

    #[test]
    fn rows_read_typed_columns() {
        let row: Row = serde_json::from_value(json!({"s": "x", "n": 3, "f": 4.0})).unwrap();
        assert_eq!(row.str("s"), Some("x"));
        assert_eq!(row.i64("n"), Some(3));
        assert_eq!(row.i64("f"), Some(4));
        assert!(row.require_str("n").is_err());
        assert!(row.require_i64("missing").is_err());
    }

    #[test]
    fn a_constraint_violation_is_the_typed_error() {
        let error = to_framework_error("write", BackendError::Constraint("UNIQUE".into()));
        assert!(is_constraint_violation(&error));
        assert!(error.retryable());
        let other = to_framework_error("write", BackendError::Other("boom".into()));
        assert!(!is_constraint_violation(&other));
    }
}
