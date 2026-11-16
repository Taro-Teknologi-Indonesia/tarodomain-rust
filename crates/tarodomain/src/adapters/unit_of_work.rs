//! The unit of work contract (spec §11.4): the transaction boundary over a
//! provider's stores.

use std::any::Any;
use std::future::Future;
use std::sync::Arc;

use crate::async_trait;
use crate::error::Result;
use crate::kernel::context;

/// Lets a trait object be downcast to its concrete type. Implemented for
/// every `Send + Sync + 'static` type, so implementers never write it.
pub trait AsAny: Any + Send + Sync {
    /// `self` as `&dyn Any`.
    fn as_any(&self) -> &dyn Any;
}

impl<T: Any + Send + Sync> AsAny for T {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// One transaction over a provider's stores (spec §11.4). The strong
/// pipeline runs each command, with its projections and sagas, in one.
///
/// A provider returns a new unit from each `create_unit_of_work()` call. It
/// implements [`begin`](Self::begin), [`commit`](Self::commit) and
/// [`rollback`](Self::rollback); the framework calls them through
/// [`execute`](#method.execute), which is what makes the unit visible to the
/// provider's adapters.
///
/// # How adapters join a unit
///
/// While `execute` runs its work, the unit is the task's active unit:
/// [`active`](#method.active) returns it, and an adapter
/// [downcasts](#method.downcast_ref) it to its provider's type to stage writes
/// in it and to read them back. An adapter called outside any unit writes
/// at once. The active unit lives in task-local storage, so concurrent
/// commands each see their own, and a future passed to `tokio::spawn` sees
/// none.
///
/// # Joining
///
/// An `execute` that starts while a unit is active doesn't begin a unit of
/// its own: its work joins the active one. This is how a saga that a strong
/// command triggers dispatches its own commands inside the same
/// transaction. An error from joined work reaches the outer `execute`,
/// which rolls everything back.
#[async_trait]
pub trait UnitOfWork: AsAny {
    /// Starts the transaction.
    async fn begin(&self) -> Result<()>;

    /// Makes every write since [`begin`](Self::begin) durable, atomically.
    /// The unit is no longer active afterwards, even if this fails.
    async fn commit(&self) -> Result<()>;

    /// Discards every write since [`begin`](Self::begin), in every store.
    /// The unit is no longer active afterwards.
    async fn rollback(&self) -> Result<()>;

    /// Whether the unit has begun and not yet committed or rolled back.
    fn is_active(&self) -> bool;
}

/// The task's active unit, in context storage.
#[derive(Clone)]
struct Active(Arc<dyn UnitOfWork>);

impl dyn UnitOfWork {
    /// Runs `work` in this unit: begins, runs it with the unit active,
    /// then commits if it succeeded and rolls back if it failed.
    ///
    /// Returns the work's result. If the work fails, its error is returned
    /// unchanged (so a conflict stays retryable), with a `rollbackError`
    /// context entry if the rollback failed too. A failed begin or commit is
    /// returned as it is.
    ///
    /// If a unit is already active in this task, `work` joins it instead
    /// (see [Joining](trait@UnitOfWork#joining)), and this unit is never
    /// begun.
    ///
    /// If the returned future is dropped part-way, the unit is left begun;
    /// a provider's unit rolls back when it is dropped.
    pub async fn execute<T, F>(self: Arc<Self>, work: F) -> Result<T>
    where
        F: Future<Output = Result<T>>,
    {
        if Self::active().is_some() {
            return work.await;
        }
        self.begin().await?;
        match context::scope(Active(Arc::clone(&self)), work).await {
            Ok(value) => {
                self.commit().await?;
                Ok(value)
            }
            Err(error) => match self.rollback().await {
                Ok(()) => Err(error),
                Err(rollback) => Err(error.with_context("rollbackError", rollback.to_string())),
            },
        }
    }

    /// The unit the current task's work runs in, if any.
    pub fn active() -> Option<Arc<dyn UnitOfWork>> {
        context::get::<Active>().map(|active| active.0)
    }

    /// The unit as its concrete type, if it is a `T`.
    ///
    /// Call it on the unit itself (`unit.downcast_ref::<MyUnit>()` on an
    /// `Arc<dyn UnitOfWork>` works), not on the result of `as_any()` on an
    /// `Arc`, which would be the `Arc`.
    pub fn downcast_ref<T: UnitOfWork>(&self) -> Option<&T> {
        self.as_any().downcast_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::error::{ErrorCode, FrameworkError};

    type Log = Arc<Mutex<Vec<String>>>;

    /// Records its calls in a shared log, and fails where told to.
    #[derive(Default)]
    struct Recording {
        name: &'static str,
        log: Log,
        active: AtomicBool,
        fail: Option<&'static str>,
    }

    impl Recording {
        fn unit(name: &'static str, log: &Log) -> Arc<dyn UnitOfWork> {
            Arc::new(Self::failing(name, log, None))
        }

        fn failing(name: &'static str, log: &Log, fail: Option<&'static str>) -> Self {
            Self {
                name,
                log: Arc::clone(log),
                fail,
                ..Self::default()
            }
        }

        fn call(&self, step: &'static str, active: bool) -> Result<()> {
            self.log
                .lock()
                .unwrap()
                .push(format!("{} {step}", self.name));
            self.active.store(active, Ordering::SeqCst);
            match self.fail {
                Some(failing) if failing == step => {
                    Err(FrameworkError::event_store_error(format!("{step} failed")))
                }
                _ => Ok(()),
            }
        }
    }

    #[async_trait]
    impl UnitOfWork for Recording {
        async fn begin(&self) -> Result<()> {
            self.call("begin", self.fail != Some("begin"))
        }
        async fn commit(&self) -> Result<()> {
            self.call("commit", false)
        }
        async fn rollback(&self) -> Result<()> {
            self.call("rollback", false)
        }
        fn is_active(&self) -> bool {
            self.active.load(Ordering::SeqCst)
        }
    }

    fn log() -> Log {
        Arc::default()
    }

    fn entries(log: &Log) -> Vec<String> {
        log.lock().unwrap().clone()
    }

    /// The name of the active unit, as the work sees it.
    fn active_name() -> Option<&'static str> {
        let unit = <dyn UnitOfWork>::active()?;
        Some(unit.downcast_ref::<Recording>()?.name)
    }

    #[tokio::test]
    async fn success_commits_with_the_unit_active_during_the_work() {
        let log = log();
        let unit = Recording::unit("a", &log);
        assert_eq!(active_name(), None);
        let result = Arc::clone(&unit)
            .execute(async {
                assert!(unit.is_active());
                Ok(active_name())
            })
            .await;
        assert_eq!(result.unwrap(), Some("a"));
        assert_eq!(entries(&log), ["a begin", "a commit"]);
        assert!(!unit.is_active());
        assert_eq!(active_name(), None);
    }

    #[tokio::test]
    async fn failure_rolls_back_and_returns_the_error_unchanged() {
        let log = log();
        let unit = Recording::unit("a", &log);
        let conflict = FrameworkError::optimistic_concurrency("BankAccount-1", 1, 2);
        let error = unit.execute(async { Err::<(), _>(conflict) }).await;
        let error = error.unwrap_err();
        assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
        assert!(error.retryable());
        assert!(!error.context().contains_key("rollbackError"));
        assert_eq!(entries(&log), ["a begin", "a rollback"]);
    }

    #[tokio::test]
    async fn a_failed_rollback_is_added_to_the_work_error() {
        let log = log();
        let unit: Arc<dyn UnitOfWork> = Arc::new(Recording::failing("a", &log, Some("rollback")));
        let failure = FrameworkError::aggregate_not_found("missing");
        let error = unit.execute(async { Err::<(), _>(failure) }).await;
        let error = error.unwrap_err();
        assert_eq!(*error.code(), ErrorCode::AggregateNotFound);
        assert_eq!(
            error.context()["rollbackError"],
            "EVENT_STORE_ERROR: rollback failed"
        );
    }

    #[tokio::test]
    async fn a_failed_begin_skips_the_work_and_a_failed_commit_is_returned() {
        let log = log();
        let unit: Arc<dyn UnitOfWork> = Arc::new(Recording::failing("a", &log, Some("begin")));
        let ran = AtomicBool::new(false);
        let error = unit
            .execute(async {
                ran.store(true, Ordering::SeqCst);
                Ok(())
            })
            .await;
        assert_eq!(error.unwrap_err().message(), "begin failed");
        assert!(!ran.load(Ordering::SeqCst));

        let log = Arc::default();
        let unit: Arc<dyn UnitOfWork> = Arc::new(Recording::failing("b", &log, Some("commit")));
        let error = unit.execute(async { Ok(()) }).await;
        assert_eq!(error.unwrap_err().message(), "commit failed");
        assert_eq!(entries(&log), ["b begin", "b commit"]);
    }

    #[tokio::test]
    async fn a_nested_execute_joins_the_active_unit() {
        let log = log();
        let (outer, inner) = (
            Recording::unit("outer", &log),
            Recording::unit("inner", &log),
        );
        let seen = outer
            .execute(async {
                let seen = inner.execute(async { Ok(active_name()) }).await?;
                Ok(seen)
            })
            .await;
        assert_eq!(seen.unwrap(), Some("outer"));
        assert_eq!(entries(&log), ["outer begin", "outer commit"]);
    }

    #[tokio::test]
    async fn an_error_in_joined_work_rolls_back_the_outer_unit() {
        let log = log();
        let (outer, inner) = (
            Recording::unit("outer", &log),
            Recording::unit("inner", &log),
        );
        let error = outer
            .execute(async {
                let failure = FrameworkError::invalid_saga_transition("no");
                inner.execute(async { Err::<(), _>(failure) }).await
            })
            .await;
        assert_eq!(*error.unwrap_err().code(), ErrorCode::InvalidSagaTransition);
        assert_eq!(entries(&log), ["outer begin", "outer rollback"]);
    }

    #[tokio::test]
    async fn concurrent_units_each_see_their_own() {
        let log = log();
        let (a, b) = (Recording::unit("a", &log), Recording::unit("b", &log));
        let (seen_a, seen_b) = tokio::join!(
            a.execute(async {
                tokio::task::yield_now().await;
                Ok(active_name())
            }),
            b.execute(async {
                tokio::task::yield_now().await;
                Ok(active_name())
            }),
        );
        assert_eq!(seen_a.unwrap(), Some("a"));
        assert_eq!(seen_b.unwrap(), Some("b"));
    }
}
