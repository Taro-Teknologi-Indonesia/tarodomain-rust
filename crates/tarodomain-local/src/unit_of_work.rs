//! The in-memory unit of work (spec §11.4).

use std::fmt;
use std::sync::{Mutex, MutexGuard, PoisonError};

use tarodomain::adapters::UnitOfWork;
use tarodomain::async_trait;
use tarodomain::error::{FrameworkError, Result};
use tokio::sync::OwnedMutexGuard;

use crate::LocalDatabase;
use crate::database::Tables;

/// A transaction over every store of a [`LocalDatabase`].
///
/// `begin` waits until no other unit holds the database, then takes it and
/// copies the tables. Inside
/// [`execute`](tarodomain::adapters::UnitOfWork#method.execute), the
/// database's adapters work on that copy. `commit` makes the copy the
/// database's tables; `rollback`, or dropping a unit that has begun, throws
/// it away. Either way the database is free again.
///
/// Units on one database run one at a time, and writes outside a unit wait
/// for it to end (see [Units of work](LocalDatabase#units-of-work)), so a
/// commit never overwrites, and a rollback never undoes, anyone else's
/// writes.
pub struct InMemoryUnitOfWork {
    db: LocalDatabase,
    /// The copy being worked on, while the unit is active.
    staged: Mutex<Option<Staged>>,
}

struct Staged {
    tables: Tables,
    /// The database's turn, held until the unit ends.
    turn: OwnedMutexGuard<()>,
}

impl InMemoryUnitOfWork {
    /// A unit over `db` that hasn't begun.
    pub fn new(db: LocalDatabase) -> Self {
        Self {
            db,
            staged: Mutex::new(None),
        }
    }

    fn staged(&self) -> MutexGuard<'_, Option<Staged>> {
        self.staged.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs `f` on the copy of the task's active unit, if that is an
    /// in-memory unit over `db` that has begun. Otherwise gives `f` back.
    pub(crate) fn with_active<F, R>(db: &LocalDatabase, f: F) -> Result<R, F>
    where
        F: FnOnce(&mut Tables) -> R,
    {
        let Some(active) = <dyn UnitOfWork>::active() else {
            return Err(f);
        };
        let Some(unit) = active.downcast_ref::<Self>() else {
            return Err(f);
        };
        if !unit.db.ptr_eq(db) {
            return Err(f);
        }
        let mut staged = unit.staged();
        match staged.as_mut() {
            Some(staged) => Ok(f(&mut staged.tables)),
            None => Err(f),
        }
    }
}

#[async_trait]
impl UnitOfWork for InMemoryUnitOfWork {
    /// Waits for the database, then copies its tables. Fails with
    /// `INVALID_CONFIGURATION` if the unit is already active, which would
    /// otherwise wait for itself.
    async fn begin(&self) -> Result<()> {
        if self.is_active() {
            return Err(FrameworkError::invalid_configuration(
                "the unit of work has already begun",
            ));
        }
        let turn = self.db.turn().lock_owned().await;
        let tables = self.db.tables().clone();
        *self.staged() = Some(Staged { tables, turn });
        Ok(())
    }

    /// Replaces the database's tables with the unit's copy. Fails with
    /// `INVALID_CONFIGURATION` if the unit isn't active.
    async fn commit(&self) -> Result<()> {
        let Some(Staged { tables, turn }) = self.staged().take() else {
            return Err(FrameworkError::invalid_configuration(
                "commit called on a unit of work that hasn't begun",
            ));
        };
        *self.db.tables() = tables;
        drop(turn);
        Ok(())
    }

    /// Throws the unit's copy away. Does nothing if the unit isn't active.
    async fn rollback(&self) -> Result<()> {
        self.staged().take();
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.staged().is_some()
    }
}

impl fmt::Debug for InMemoryUnitOfWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryUnitOfWork")
            .field("db", &self.db)
            .field("active", &self.is_active())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tarodomain::error::ErrorCode;
    use tokio::sync::oneshot;

    use super::*;

    #[derive(Clone, Default)]
    struct Names(Vec<&'static str>);

    #[derive(Clone, Default)]
    struct Count(u32);

    fn names(db: &LocalDatabase) -> Vec<&'static str> {
        db.read(|names: &Names| names.0.clone())
    }

    async fn push(db: &LocalDatabase, name: &'static str) {
        db.write(|names: &mut Names| names.0.push(name)).await;
    }

    fn unit(db: &LocalDatabase) -> Arc<dyn UnitOfWork> {
        Arc::new(InMemoryUnitOfWork::new(db.clone()))
    }

    #[tokio::test]
    async fn commit_keeps_the_writes_of_every_table() {
        let db = LocalDatabase::new();
        let seen = unit(&db)
            .execute(async {
                push(&db, "a").await;
                db.write(|count: &mut Count| count.0 += 1).await;
                Ok(names(&db))
            })
            .await;
        assert_eq!(seen.unwrap(), ["a"]);
        assert_eq!(names(&db), ["a"]);
        assert_eq!(db.read(|count: &Count| count.0), 1);
    }

    #[tokio::test]
    async fn rollback_discards_the_writes_of_every_table() {
        let db = LocalDatabase::new();
        push(&db, "before").await;
        let result = unit(&db)
            .execute(async {
                push(&db, "a").await;
                db.write(|count: &mut Count| count.0 += 1).await;
                Err::<(), _>(FrameworkError::aggregate_not_found("missing"))
            })
            .await;
        assert_eq!(*result.unwrap_err().code(), ErrorCode::AggregateNotFound);
        assert_eq!(names(&db), ["before"]);
        assert_eq!(db.read(|count: &Count| count.0), 0);
    }

    #[tokio::test]
    async fn others_see_the_writes_only_after_commit() {
        let db = LocalDatabase::new();
        let (written, read) = (oneshot::channel(), oneshot::channel());
        let work = unit(&db).execute(async {
            push(&db, "a").await;
            written.0.send(()).unwrap();
            read.1.await.unwrap();
            Ok(names(&db))
        });
        let outside = async {
            written.1.await.unwrap();
            let seen = names(&db);
            read.0.send(()).unwrap();
            seen
        };
        let (inside, outside) = tokio::join!(work, outside);
        assert_eq!(inside.unwrap(), ["a"]);
        assert!(outside.is_empty());
        assert_eq!(names(&db), ["a"]);
    }

    /// A write outside the unit, started while it is active, ends up after
    /// the unit's writes if it commits, and survives a rollback.
    async fn write_during(commit: bool) -> Vec<&'static str> {
        let db = LocalDatabase::new();
        let (written, started) = (oneshot::channel(), oneshot::channel());
        let work = unit(&db).execute(async {
            push(&db, "unit").await;
            written.0.send(()).unwrap();
            started.1.await.unwrap();
            tokio::task::yield_now().await;
            if commit {
                Ok(())
            } else {
                Err(FrameworkError::aggregate_not_found("missing"))
            }
        });
        let outside = async {
            written.1.await.unwrap();
            let write = push(&db, "outside");
            started.0.send(()).unwrap();
            write.await;
        };
        let _ = tokio::join!(work, outside);
        names(&db)
    }

    #[tokio::test]
    async fn a_write_outside_waits_for_the_unit() {
        assert_eq!(write_during(true).await, ["unit", "outside"]);
        assert_eq!(write_during(false).await, ["outside"]);
    }

    #[tokio::test]
    async fn units_on_one_database_run_one_at_a_time() {
        let db = LocalDatabase::new();
        let (a, b) = (unit(&db), unit(&db));
        a.begin().await.unwrap();
        let begin_b = b.begin();
        tokio::pin!(begin_b);
        tokio::select! {
            biased;
            _ = &mut begin_b => panic!("b began while a was active"),
            () = tokio::task::yield_now() => {}
        }
        a.rollback().await.unwrap();
        begin_b.await.unwrap();
        assert!(b.is_active());
    }

    #[tokio::test]
    async fn a_unit_over_another_database_is_not_used() {
        let (db, other) = (LocalDatabase::new(), LocalDatabase::new());
        let result = unit(&other)
            .execute(async {
                push(&db, "a").await;
                Err::<(), _>(FrameworkError::aggregate_not_found("missing"))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(names(&db), ["a"]);
    }

    #[tokio::test]
    async fn dropping_a_begun_unit_frees_the_database() {
        let db = LocalDatabase::new();
        let unit = unit(&db);
        unit.begin().await.unwrap();
        drop(unit);
        push(&db, "a").await;
        assert_eq!(names(&db), ["a"]);
    }

    #[tokio::test]
    async fn begin_and_commit_check_the_state() {
        let db = LocalDatabase::new();
        let unit = InMemoryUnitOfWork::new(db);
        assert!(!unit.is_active());
        let error = unit.commit().await.unwrap_err();
        assert_eq!(*error.code(), ErrorCode::InvalidConfiguration);
        unit.rollback().await.unwrap();

        unit.begin().await.unwrap();
        assert!(unit.is_active());
        assert_eq!(
            format!("{unit:?}"),
            "InMemoryUnitOfWork { db: LocalDatabase { tables: 0, .. }, active: true }"
        );
        let error = unit.begin().await.unwrap_err();
        assert_eq!(*error.code(), ErrorCode::InvalidConfiguration);
        unit.commit().await.unwrap();
        assert!(!unit.is_active());
    }
}
