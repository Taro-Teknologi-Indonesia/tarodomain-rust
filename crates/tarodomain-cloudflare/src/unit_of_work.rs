//! The D1 unit of work (spec §11.4).
//!
//! D1 can't hold a transaction open across awaits; only one `batch` is atomic.
//! So a unit stages every write and sends them in a single batch at commit.
//! Anything read inside the unit sees the database as it was before the unit
//! began, which is why the unit also keeps an overlay: the stream events,
//! saga records and idempotency markers it staged, which the adapters read
//! before they read D1.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::future::Future;
use std::sync::{Mutex, MutexGuard, PoisonError};

use chrono::{DateTime, Utc};
use tarodomain::adapters::{SagaRecord, UnitOfWork};
use tarodomain::async_trait;
use tarodomain::domain::SerializedEvent;
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::BoxFuture;
use tarodomain::kernel::ids::Uuid;

use crate::d1::{D1Db, RowExt, Statement};

/// A callback that runs after the unit's batch committed.
type AfterCommit = Box<dyn FnOnce() -> BoxFuture<'static, Result<()>> + Send>;

/// An event staged in the unit, with where it goes.
#[derive(Clone, Debug)]

pub(crate) struct StagedEvent {
    pub stream_id: String,
    pub stream_version: u64,
    pub event: SerializedEvent,
}

/// A stream the unit has appended to.
#[derive(Clone, Debug)]

pub(crate) struct StagedStream {
    /// The version the unit's first append expected.
    pub base_version: u64,
    pub version: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// What a begun unit has staged, readable by the adapters until it ends.
#[derive(Default)]

pub(crate) struct Overlay {
    /// The writes, in order, for the commit batch.
    pub statements: Vec<Statement>,
    /// Appended events, in append order.
    pub events: Vec<StagedEvent>,
    /// Streams appended to, with their new versions.
    pub streams: HashMap<String, StagedStream>,
    /// Saga records saved or changed in the unit.
    pub sagas: HashMap<(String, Uuid), SagaRecord>,
    /// Processed-event markers: (handler name, instance or "", event ID).
    pub processed_events: HashSet<(String, String, Uuid)>,
    /// Processed-command markers.
    pub processed_commands: HashMap<Uuid, DateTime<Utc>>,
    after_commit: Vec<AfterCommit>,
}

/// A transaction over every D1 store of one database.
///
/// `begin` starts staging. Inside
/// [`execute`](tarodomain::adapters::UnitOfWork#method.execute), the adapters
/// write through [`D1Db`], which stages into the unit instead of writing.
/// `commit` sends every staged statement in one `batch`, so they all apply or
/// none does; `rollback` drops them. A unit doesn't lock anything: two units
/// that conflict are told apart at commit by the database's unique keys,
/// which fails the batch with `CONCURRENT_MODIFICATION`, and the pipeline
/// retries the command.
pub struct D1UnitOfWork {
    db: D1Db,
    overlay: Mutex<Option<Overlay>>,
}

impl D1UnitOfWork {
    /// A unit over `db` that hasn't begun.
    pub fn new(db: D1Db) -> Self {
        Self {
            db,
            overlay: Mutex::new(None),
        }
    }

    fn overlay(&self) -> MutexGuard<'_, Option<Overlay>> {
        self.overlay.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Runs `f` on the overlay of the task's active unit, if that is a D1
    /// unit over `db` that has begun.
    pub(crate) fn with_active<R>(db: &D1Db, f: impl FnOnce(&mut Overlay) -> R) -> Option<R> {
        let active = <dyn UnitOfWork>::active()?;
        let unit = active.downcast_ref::<Self>()?;
        if !unit.db.same_backend(db) {
            return None;
        }
        unit.overlay().as_mut().map(f)
    }

    /// Runs `work` after the active unit commits, or at once if there is no
    /// unit. A unit that rolls back never runs it.
    ///
    /// A failure of `work` after a commit is logged and otherwise ignored:
    /// the batch is already durable, and the cron catch-up delivers whatever
    /// `work` didn't (§11.2).
    pub async fn after_commit<F, Fut>(db: &D1Db, work: F) -> Result<()>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let mut work = Some(work);
        let staged = Self::with_active(db, |overlay| {
            if let Some(work) = work.take() {
                overlay
                    .after_commit
                    .push(Box::new(move || Box::pin(work())));
            }
        });
        match (staged, work) {
            (None, Some(work)) => work().await,
            _ => Ok(()),
        }
    }
}

impl D1UnitOfWork {
    /// A constraint violation at commit means another writer got there
    /// first. If it appended to a stream this unit appended to, say so as
    /// `OPTIMISTIC_CONCURRENCY` (retryable) with the versions; otherwise the
    /// error stays as it is.
    async fn conflict_of(
        &self,
        streams: &HashMap<String, StagedStream>,
        error: FrameworkError,
    ) -> FrameworkError {
        if !crate::d1::is_constraint_violation(&error) {
            return error;
        }
        for (stream_id, staged) in streams {
            let stored = self
                .db
                .query_one(Statement::new(
                    "SELECT version FROM tarodomain_streams WHERE stream_id = ?",
                    vec![serde_json::json!(stream_id)],
                ))
                .await;
            let actual = match stored {
                Ok(row) => row.and_then(|row| row.i64("version")).unwrap_or(0),
                Err(_) => continue,
            };
            let actual = u64::try_from(actual).unwrap_or(0);
            if actual != staged.base_version {
                return FrameworkError::optimistic_concurrency(
                    stream_id,
                    staged.base_version,
                    actual,
                )
                .with_source(error);
            }
        }
        error
    }
}

#[async_trait]
impl UnitOfWork for D1UnitOfWork {
    /// Starts staging. Fails with `INVALID_CONFIGURATION` if the unit has
    /// already begun.
    async fn begin(&self) -> Result<()> {
        let mut overlay = self.overlay();
        if overlay.is_some() {
            return Err(FrameworkError::invalid_configuration(
                "the unit of work has already begun",
            ));
        }
        *overlay = Some(Overlay::default());
        Ok(())
    }

    /// Sends every staged statement in one batch, then runs the
    /// `after_commit` callbacks. A violated unique key fails the batch as a
    /// whole with `CONCURRENT_MODIFICATION`. Fails with
    /// `INVALID_CONFIGURATION` if the unit isn't active.
    async fn commit(&self) -> Result<()> {
        let Some(overlay) = self.overlay().take() else {
            return Err(FrameworkError::invalid_configuration(
                "commit called on a unit of work that hasn't begun",
            ));
        };
        if let Err(error) = self.db.flush(&overlay.statements).await {
            return Err(self.conflict_of(&overlay.streams, error).await);
        }
        for work in overlay.after_commit {
            if let Err(error) = work().await {
                tracing::warn!(%error, "a callback after the commit failed; catch-up will redeliver");
            }
        }
        Ok(())
    }

    /// Drops the staged writes. Does nothing if the unit isn't active.
    async fn rollback(&self) -> Result<()> {
        self.overlay().take();
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.overlay().is_some()
    }
}

impl fmt::Debug for D1UnitOfWork {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("D1UnitOfWork")
            .field("active", &self.is_active())
            .finish()
    }
}

#[cfg(all(test, feature = "native-sqlite"))]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use serde_json::json;

    use super::*;
    use crate::d1::{D1Db, RowExt};
    use crate::sqlite_backend::SqliteBackend;

    async fn db() -> D1Db {
        let db = D1Db::new(Arc::new(SqliteBackend::in_memory().unwrap()));
        db.run(Statement::plain(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
        ))
        .await
        .unwrap();
        db
    }

    fn insert(id: i64, name: &str) -> Statement {
        Statement::new(
            "INSERT INTO t (id, name) VALUES (?, ?)",
            vec![json!(id), json!(name)],
        )
    }

    async fn count(db: &D1Db) -> i64 {
        db.query_one(Statement::plain("SELECT COUNT(*) AS n FROM t"))
            .await
            .unwrap()
            .unwrap()
            .require_i64("n")
            .unwrap()
    }

    fn unit(db: &D1Db) -> Arc<dyn UnitOfWork> {
        Arc::new(D1UnitOfWork::new(db.clone()))
    }

    #[tokio::test]
    async fn writes_outside_a_unit_run_at_once() {
        let db = db().await;
        db.write(vec![insert(1, "a")]).await.unwrap();
        assert_eq!(count(&db).await, 1);
    }

    #[tokio::test]
    async fn a_commit_sends_everything_staged_and_reads_see_it_only_afterwards() {
        let db = db().await;
        let seen_inside = unit(&db)
            .execute(async {
                db.write(vec![insert(1, "a"), insert(2, "b")]).await?;
                count(&db).await.pipe_ok()
            })
            .await
            .unwrap();
        assert_eq!(
            seen_inside, 0,
            "D1 reads inside a unit see the database before it"
        );
        assert_eq!(count(&db).await, 2);
    }

    #[tokio::test]
    async fn a_rollback_drops_the_staged_writes() {
        let db = db().await;
        let result: Result<()> = unit(&db)
            .execute(async {
                db.write(vec![insert(1, "a")]).await?;
                Err(FrameworkError::invalid_configuration("no"))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(count(&db).await, 0);
    }

    #[tokio::test]
    async fn a_conflict_at_commit_fails_the_whole_batch_and_is_retryable() {
        let db = db().await;
        db.write(vec![insert(1, "a")]).await.unwrap();
        let ran = Arc::new(AtomicUsize::new(0));
        let ran_in = Arc::clone(&ran);
        let error = unit(&db)
            .execute(async {
                db.write(vec![insert(2, "b"), insert(3, "a")]).await?;
                let ran = Arc::clone(&ran_in);
                D1UnitOfWork::after_commit(&db, move || async move {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
            })
            .await
            .unwrap_err();
        assert!(crate::d1::is_constraint_violation(&error));
        assert!(error.retryable());
        assert_eq!(count(&db).await, 1, "the batch rolled back as a whole");
        assert_eq!(
            ran.load(Ordering::SeqCst),
            0,
            "no callback after a failed commit"
        );
    }

    #[tokio::test]
    async fn after_commit_runs_after_the_commit_and_never_after_a_rollback() {
        let db = db().await;
        let ran = Arc::new(AtomicUsize::new(0));
        for fail in [false, true] {
            let ran = Arc::clone(&ran);
            let db_in = db.clone();
            let _ = unit(&db)
                .execute(async move {
                    D1UnitOfWork::after_commit(&db_in, move || async move {
                        ran.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .await?;
                    if fail {
                        Err(FrameworkError::invalid_configuration("no"))
                    } else {
                        Ok(())
                    }
                })
                .await;
        }
        assert_eq!(ran.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn after_commit_outside_a_unit_runs_at_once_and_a_failing_callback_does_not_fail_the_commit()
     {
        let db = db().await;
        let ran = Arc::new(AtomicUsize::new(0));
        let ran_in = Arc::clone(&ran);
        D1UnitOfWork::after_commit(&db, move || async move {
            ran_in.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await
        .unwrap();
        assert_eq!(ran.load(Ordering::SeqCst), 1);

        let db_in = db.clone();
        unit(&db)
            .execute(async move {
                db_in.write(vec![insert(1, "a")]).await?;
                D1UnitOfWork::after_commit(&db_in, || async {
                    Err(FrameworkError::queue_error("down"))
                })
                .await
            })
            .await
            .unwrap();
        assert_eq!(count(&db).await, 1);
    }

    #[tokio::test]
    async fn a_nested_execute_joins_the_unit() {
        let db = db().await;
        let outer = unit(&db);
        let inner = unit(&db);
        let db_in = db.clone();
        let result: Result<()> = outer
            .execute(async move {
                inner
                    .execute(async {
                        db_in.write(vec![insert(1, "a")]).await?;
                        Err(FrameworkError::invalid_configuration("inner fails"))
                    })
                    .await
            })
            .await;
        assert!(result.is_err());
        assert_eq!(
            count(&db).await,
            0,
            "the inner failure rolled the outer unit back"
        );
    }

    #[tokio::test]
    async fn the_overlay_is_readable_inside_the_unit_and_gone_after() {
        let db = db().await;
        let id = Uuid::now_v7();
        let seen = unit(&db)
            .execute(async {
                D1UnitOfWork::with_active(&db, |overlay| {
                    overlay.processed_commands.insert(id, Utc::now());
                });
                Ok(D1UnitOfWork::with_active(&db, |overlay| {
                    overlay.processed_commands.contains_key(&id)
                }))
            })
            .await
            .unwrap();
        assert_eq!(seen, Some(true));
        assert_eq!(D1UnitOfWork::with_active(&db, |_| ()), None);
    }

    #[tokio::test]
    async fn a_unit_over_another_database_is_ignored() {
        let one = db().await;
        let other = db().await;
        let seen = unit(&one)
            .execute(async { Ok(D1UnitOfWork::with_active(&other, |_| ()).is_some()) })
            .await
            .unwrap();
        assert!(!seen);
    }

    #[tokio::test]
    async fn state_errors() {
        let db = db().await;
        let unit = D1UnitOfWork::new(db);
        assert!(!unit.is_active());
        assert!(unit.commit().await.is_err());
        unit.rollback().await.unwrap();
        unit.begin().await.unwrap();
        assert!(unit.is_active());
        assert!(unit.begin().await.is_err());
        unit.rollback().await.unwrap();
        assert!(!unit.is_active());
    }

    trait PipeOk: Sized {
        fn pipe_ok(self) -> Result<Self> {
            Ok(self)
        }
    }
    impl PipeOk for i64 {}
}
