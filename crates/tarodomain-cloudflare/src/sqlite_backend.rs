//! SQLite behind [`D1Backend`], so the adapters' SQL runs under `cargo test`.
//!
//! D1 is SQLite, and runs one write at a time; so does this backend (one
//! connection behind a mutex). `batch` is one transaction, as on D1.

use std::sync::{Mutex, MutexGuard, PoisonError};

use rusqlite::types::{Value as SqlValue, ValueRef};
use rusqlite::{Connection, ErrorCode, params_from_iter};
use serde_json::Value;
use tarodomain::async_trait;

use crate::d1::{BackendError, D1Backend, Row, Statement};

/// An in-memory (or file) SQLite database as a [`D1Backend`].
pub struct SqliteBackend {
    connection: Mutex<Connection>,
}

impl SqliteBackend {
    /// A new, empty in-memory database.
    pub fn in_memory() -> Result<Self, BackendError> {
        Connection::open_in_memory()
            .map(Self::from_connection)
            .map_err(convert)
    }

    /// Wraps an open connection.
    pub fn from_connection(connection: Connection) -> Self {
        Self {
            connection: Mutex::new(connection),
        }
    }

    fn connection(&self) -> MutexGuard<'_, Connection> {
        self.connection
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

fn convert(error: rusqlite::Error) -> BackendError {
    match &error {
        rusqlite::Error::SqliteFailure(failure, _)
            if failure.code == ErrorCode::ConstraintViolation =>
        {
            BackendError::Constraint(error.to_string())
        }
        _ => BackendError::Other(error.to_string()),
    }
}

fn bind(params: &[Value]) -> Vec<SqlValue> {
    params
        .iter()
        .map(|value| match value {
            Value::Null => SqlValue::Null,
            Value::Bool(flag) => SqlValue::Integer(i64::from(*flag)),
            Value::Number(number) => match number.as_i64() {
                Some(integer) => SqlValue::Integer(integer),
                None => SqlValue::Real(number.as_f64().unwrap_or_default()),
            },
            Value::String(text) => SqlValue::Text(text.clone()),
            other => SqlValue::Text(other.to_string()),
        })
        .collect()
}

fn run_one(connection: &Connection, statement: &Statement) -> Result<usize, rusqlite::Error> {
    let mut prepared = connection.prepare_cached(&statement.sql)?;
    // `query`, not `execute`: a staged `INSERT … RETURNING` returns rows,
    // which `execute` rejects. They are dropped here.
    let mut rows = prepared.query(params_from_iter(bind(&statement.params)))?;
    while rows.next()?.is_some() {}
    drop(rows);
    Ok(connection.changes() as usize)
}

#[async_trait]
impl D1Backend for SqliteBackend {
    async fn all(&self, statement: &Statement) -> Result<Vec<Row>, BackendError> {
        let connection = self.connection();
        let mut prepared = connection.prepare_cached(&statement.sql).map_err(convert)?;
        let names: Vec<String> = prepared.column_names().map(str::to_owned).collect();
        let mut rows = prepared
            .query(params_from_iter(bind(&statement.params)))
            .map_err(convert)?;
        let mut out = Vec::new();
        while let Some(row) = rows.next().map_err(convert)? {
            let mut map = Row::new();
            for (index, name) in names.iter().enumerate() {
                let value = match row.get_ref(index).map_err(convert)? {
                    ValueRef::Null => Value::Null,
                    ValueRef::Integer(n) => Value::from(n),
                    ValueRef::Real(f) => Value::from(f),
                    ValueRef::Text(text) => Value::from(String::from_utf8_lossy(text).into_owned()),
                    ValueRef::Blob(_) => Value::Null,
                };
                map.insert(name.clone(), value);
            }
            out.push(map);
        }
        Ok(out)
    }

    async fn run(&self, statement: &Statement) -> Result<u64, BackendError> {
        let connection = self.connection();
        run_one(&connection, statement)
            .map(|changes| changes as u64)
            .map_err(convert)
    }

    async fn batch(&self, statements: &[Statement]) -> Result<(), BackendError> {
        let mut connection = self.connection();
        let transaction = connection.transaction().map_err(convert)?;
        for statement in statements {
            run_one(&transaction, statement).map_err(convert)?;
        }
        transaction.commit().map_err(convert)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    async fn backend() -> SqliteBackend {
        let backend = SqliteBackend::in_memory().unwrap();
        backend
            .batch(&[Statement::plain(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE, note TEXT)",
            )])
            .await
            .unwrap();
        backend
    }

    fn insert(id: i64, name: &str) -> Statement {
        Statement::new(
            "INSERT INTO t (id, name) VALUES (?, ?)",
            vec![json!(id), json!(name)],
        )
    }

    #[tokio::test]
    async fn rows_come_back_as_json() {
        let backend = backend().await;
        backend.batch(&[insert(1, "a")]).await.unwrap();
        let rows = backend
            .all(&Statement::plain("SELECT id, name, note FROM t"))
            .await
            .unwrap();
        assert_eq!(
            rows,
            vec![
                serde_json::from_value::<Row>(json!({"id": 1, "name": "a", "note": null})).unwrap()
            ]
        );
    }

    #[tokio::test]
    async fn run_reports_changed_rows() {
        let backend = backend().await;
        backend
            .batch(&[insert(1, "a"), insert(2, "b")])
            .await
            .unwrap();
        let changed = backend
            .run(&Statement::plain("UPDATE t SET note = 'x'"))
            .await
            .unwrap();
        assert_eq!(changed, 2);
    }

    #[tokio::test]
    async fn a_failing_batch_rolls_back_every_statement_and_reports_the_constraint() {
        let backend = backend().await;
        backend.batch(&[insert(1, "a")]).await.unwrap();
        let error = backend
            .batch(&[insert(2, "b"), insert(3, "a")])
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Constraint(_)));
        let rows = backend
            .all(&Statement::plain("SELECT id FROM t"))
            .await
            .unwrap();
        assert_eq!(
            rows.len(),
            1,
            "the first statement of the batch was rolled back"
        );
    }

    #[tokio::test]
    async fn other_failures_are_not_constraints() {
        let backend = backend().await;
        let error = backend
            .all(&Statement::plain("SELECT * FROM missing"))
            .await
            .unwrap_err();
        assert!(matches!(error, BackendError::Other(_)));
    }

    #[tokio::test]
    async fn bools_bind_as_integers() {
        let backend = backend().await;
        let rows = backend
            .all(&Statement::new("SELECT ? AS flag", vec![json!(true)]))
            .await
            .unwrap();
        assert_eq!(rows[0]["flag"], json!(1));
    }
}
