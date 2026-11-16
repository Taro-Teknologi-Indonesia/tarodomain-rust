//! The SQL unit of work (spec §11.4): one real database transaction.
//!
//! `begin` opens a transaction, and while
//! [`execute`](tarodomain::adapters::UnitOfWork#method.execute) runs its work
//! the unit is the task's active unit, so [`Db`] runs every query on that
//! transaction. `commit` commits it, `rollback` (or dropping a begun unit)
//! undoes it, and a nested `execute` joins it. Nothing is staged or copied,
//! unlike on D1, so reads inside a unit see its own writes.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use tarodomain::adapters::UnitOfWork;
use tarodomain::async_trait;
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::BoxFuture;

use crate::db::{Db, Param, Row, commit, execute_in, fetch_in, rollback};

/// A transaction on either database.
pub(crate) enum Tx {
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::Transaction<'static, sqlx::Sqlite>),
    #[cfg(feature = "postgres")]
    Postgres(sqlx::Transaction<'static, sqlx::Postgres>),
}

/// A callback that runs after the unit committed.
type AfterCommit = Box<dyn FnOnce() -> BoxFuture<'static, Result<()>> + Send>;

/// A transaction over every store of one [`Db`].
pub struct SqlxUnitOfWork {
    db: Db,
    tx: tokio::sync::Mutex<Option<Tx>>,
    active: AtomicBool,
    after_commit: Mutex<Vec<AfterCommit>>,
}

/// The task's active unit, as the statement layer uses it.
pub(crate) struct ActiveUnit(Arc<dyn UnitOfWork>);

impl ActiveUnit {
    fn unit(&self) -> &SqlxUnitOfWork {
        // `active_for` only builds an `ActiveUnit` for this type.
        self.0
            .downcast_ref::<SqlxUnitOfWork>()
            .expect("an ActiveUnit always holds a SqlxUnitOfWork")
    }

    pub(crate) async fn fetch(&self, sql: &str, params: &[Param]) -> Result<Vec<Row>> {
        let mut tx = self.unit().tx.lock().await;
        let tx = tx.as_mut().ok_or_else(not_begun)?;
        fetch_in(tx, sql, params).await
    }

    pub(crate) async fn execute(&self, sql: &str, params: &[Param]) -> Result<u64> {
        let mut tx = self.unit().tx.lock().await;
        let tx = tx.as_mut().ok_or_else(not_begun)?;
        execute_in(tx, sql, params).await
    }
}

fn not_begun() -> FrameworkError {
    FrameworkError::invalid_configuration("the unit of work hasn't begun")
}

impl SqlxUnitOfWork {
    /// A unit over `db` that hasn't begun.
    pub fn new(db: Db) -> Self {
        Self {
            db,
            tx: tokio::sync::Mutex::new(None),
            active: AtomicBool::new(false),
            after_commit: Mutex::new(Vec::new()),
        }
    }

    /// The task's active unit, if it is a SQL unit over `db` that has begun.
    pub(crate) fn active_for(db: &Db) -> Option<ActiveUnit> {
        let active = <dyn UnitOfWork>::active()?;
        let unit = active.downcast_ref::<Self>()?;
        if !unit.db.same_pool(db) || !unit.is_active() {
            return None;
        }
        Some(ActiveUnit(active))
    }

    /// Runs `work` after the active unit commits, or at once if there is no
    /// unit. A unit that rolls back never runs it.
    ///
    /// A failure of `work` after a commit is returned by `commit`, but the
    /// transaction is already durable: the catch-up delivers whatever `work`
    /// didn't (§11.2).
    pub async fn after_commit<F, Fut>(db: &Db, work: F) -> Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        match Self::active_for(db) {
            Some(active) => {
                active
                    .unit()
                    .after_commit
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner)
                    .push(Box::new(move || Box::pin(work())));
                Ok(())
            }
            None => work().await,
        }
    }
}

#[async_trait]
impl UnitOfWork for SqlxUnitOfWork {
    /// Opens the transaction. Fails with `INVALID_CONFIGURATION` if the unit
    /// has already begun.
    async fn begin(&self) -> Result<()> {
        let mut slot = self.tx.lock().await;
        if slot.is_some() {
            return Err(FrameworkError::invalid_configuration(
                "the unit of work has already begun",
            ));
        }
        *slot = Some(self.db.begin().await?);
        self.active.store(true, Ordering::SeqCst);
        Ok(())
    }

    /// Commits, then runs the `after_commit` callbacks. Fails with
    /// `INVALID_CONFIGURATION` if the unit isn't active. A violated unique
    /// key or serialization failure is `CONCURRENT_MODIFICATION`.
    async fn commit(&self) -> Result<()> {
        let tx = self.tx.lock().await.take().ok_or_else(|| {
            FrameworkError::invalid_configuration(
                "commit called on a unit of work that hasn't begun",
            )
        })?;
        self.active.store(false, Ordering::SeqCst);
        let callbacks = std::mem::take(
            &mut *self
                .after_commit
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        commit(tx).await?;
        for work in callbacks {
            work().await?;
        }
        Ok(())
    }

    /// Rolls the transaction back and drops the `after_commit` callbacks.
    /// Does nothing if the unit isn't active.
    async fn rollback(&self) -> Result<()> {
        self.active.store(false, Ordering::SeqCst);
        self.after_commit
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
        match self.tx.lock().await.take() {
            Some(tx) => rollback(tx).await,
            None => Ok(()),
        }
    }

    fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::sync::atomic::AtomicUsize;

    use sqlx::sqlite::SqlitePoolOptions;
    use tarodomain::error::ErrorCode;

    use super::*;

    async fn db() -> Db {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let db = Db::sqlite(pool);
        db.execute("CREATE TABLE t (n INTEGER PRIMARY KEY)", vec![])
            .await
            .unwrap();
        db
    }

    async fn count(db: &Db) -> i64 {
        db.fetch_one("SELECT COUNT(*) AS n FROM t", vec![])
            .await
            .unwrap()
            .unwrap()
            .i64("n")
            .unwrap()
    }

    async fn insert(db: &Db, n: i64) -> Result<()> {
        db.execute("INSERT INTO t (n) VALUES (?)", vec![n.into()])
            .await
            .map(|_| ())
    }

    fn unit(db: &Db) -> Arc<dyn UnitOfWork> {
        Arc::new(SqlxUnitOfWork::new(db.clone()))
    }

    #[tokio::test]
    async fn commit_keeps_the_writes_and_the_unit_reads_its_own() {
        let db = db().await;
        let seen = unit(&db)
            .execute(async {
                insert(&db, 1).await?;
                Ok(count(&db).await)
            })
            .await
            .unwrap();
        assert_eq!(seen, 1);
        assert_eq!(count(&db).await, 1);
    }

    #[tokio::test]
    async fn a_failed_unit_rolls_back() {
        let db = db().await;
        let result: Result<()> = unit(&db)
            .execute(async {
                insert(&db, 1).await?;
                Err(FrameworkError::invalid_configuration("no"))
            })
            .await;
        assert_eq!(*result.unwrap_err().code(), ErrorCode::InvalidConfiguration);
        assert_eq!(count(&db).await, 0);
    }

    #[tokio::test]
    async fn a_nested_execute_joins_and_its_error_rolls_back_the_outer_unit() {
        let db = db().await;
        let outer = unit(&db);
        let inner = unit(&db);
        let result: Result<()> = outer
            .execute(async {
                insert(&db, 1).await?;
                inner
                    .execute(async {
                        insert(&db, 2).await?;
                        Err(FrameworkError::invalid_configuration("no"))
                    })
                    .await
            })
            .await;
        assert!(result.is_err());
        assert_eq!(count(&db).await, 0);
    }

    #[tokio::test]
    async fn after_commit_runs_only_after_a_commit() {
        let db = db().await;
        let ran = Arc::new(AtomicUsize::new(0));
        let run = |db: Db, ran: Arc<AtomicUsize>| async move {
            SqlxUnitOfWork::after_commit(&db, move || async move {
                ran.fetch_add(1, Ordering::SeqCst);
                Ok(())
            })
            .await
        };
        let _: Result<()> = unit(&db)
            .execute(async {
                run(db.clone(), ran.clone()).await?;
                Err(FrameworkError::invalid_configuration("no"))
            })
            .await;
        assert_eq!(ran.load(Ordering::SeqCst), 0);
        unit(&db)
            .execute(run(db.clone(), ran.clone()))
            .await
            .unwrap();
        assert_eq!(ran.load(Ordering::SeqCst), 1);
        // Outside a unit it runs at once.
        run(db.clone(), ran.clone()).await.unwrap();
        assert_eq!(ran.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn state_checks() {
        let db = db().await;
        let unit = SqlxUnitOfWork::new(db);
        assert!(!unit.is_active());
        assert_eq!(
            *unit.commit().await.unwrap_err().code(),
            ErrorCode::InvalidConfiguration
        );
        unit.rollback().await.unwrap();
        unit.begin().await.unwrap();
        assert!(unit.is_active());
        assert!(unit.begin().await.is_err());
        unit.rollback().await.unwrap();
        assert!(!unit.is_active());
    }

    #[tokio::test]
    async fn a_unique_violation_is_a_retryable_conflict() {
        let db = db().await;
        insert(&db, 1).await.unwrap();
        let error = insert(&db, 1).await.unwrap_err();
        assert!(crate::db::is_conflict(&error));
        assert!(error.retryable());
    }
}
