//! The eventual-mode event bus on SQL (spec §10.5, §15.2).
//!
//! The events are already durable in the store when a command publishes them,
//! so this bus carries nothing: publishing only wakes the catch-up worker
//! ([`Notify`]), which reads the store from each handler's cursor. The
//! scheduled catch-up is the safety net: a wake-up that nobody awaits, or a
//! process that dies before it runs, loses nothing.

use std::sync::Arc;

use tarodomain::async_trait;
use tarodomain::bus::EventBus;
use tarodomain::domain::DomainEvent;
use tarodomain::error::Result;
use tokio::sync::Notify;

use crate::db::Db;
use crate::unit_of_work::SqlxUnitOfWork;

/// A bus that wakes the catch-up worker.
///
/// Inside a unit of work it wakes after the commit, so the worker never runs
/// before the events are visible.
#[derive(Clone)]
pub struct SqlxEventBus {
    db: Db,
    wakeups: Arc<Notify>,
}

impl SqlxEventBus {
    /// A bus over `db` that notifies `wakeups`.
    pub fn new(db: Db, wakeups: Arc<Notify>) -> Self {
        Self { db, wakeups }
    }
}

#[async_trait]
impl EventBus for SqlxEventBus {
    fn name(&self) -> &str {
        "sqlx-notify"
    }

    /// Wakes the worker (after the commit, inside a unit of work). Publishing
    /// no events wakes nothing. It doesn't fail: the events are stored.
    async fn publish_all(&self, events: &[DomainEvent]) -> Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let wakeups = Arc::clone(&self.wakeups);
        SqlxUnitOfWork::after_commit(&self.db, move || async move {
            // `notify_one` stores a permit, so a worker that isn't waiting yet
            // still wakes at its next wait.
            wakeups.notify_one();
            Ok(())
        })
        .await
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use std::time::Duration;

    use sqlx::sqlite::SqlitePoolOptions;
    use tarodomain::adapters::UnitOfWork;
    use tarodomain::domain::EventEnvelope;
    use tarodomain::error::FrameworkError;

    use super::*;

    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct Ping;

    impl tarodomain::domain::Event for Ping {
        const NAME: &'static str = "Ping";
    }

    fn ping() -> DomainEvent {
        DomainEvent::new(EventEnvelope::new(chrono::Utc::now()), Ping)
    }

    async fn bus() -> (SqlxEventBus, Arc<Notify>, Db) {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let db = Db::sqlite(pool);
        let wakeups = Arc::new(Notify::new());
        (SqlxEventBus::new(db.clone(), wakeups.clone()), wakeups, db)
    }

    async fn woken(wakeups: &Notify) -> bool {
        tokio::time::timeout(Duration::from_millis(50), wakeups.notified())
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn publishing_wakes_the_worker_even_before_it_waits() {
        let (bus, wakeups, _) = bus().await;
        bus.publish_all(&[ping()]).await.unwrap();
        assert!(woken(&wakeups).await, "the permit is stored");
        assert!(!woken(&wakeups).await, "and used up");
    }

    #[tokio::test]
    async fn publishing_nothing_wakes_nobody() {
        let (bus, wakeups, _) = bus().await;
        bus.publish_all(&[]).await.unwrap();
        assert!(!woken(&wakeups).await);
    }

    #[tokio::test]
    async fn inside_a_unit_the_wakeup_waits_for_the_commit_and_a_rollback_drops_it() {
        let (bus, wakeups, db) = bus().await;
        let unit: Arc<dyn UnitOfWork> = Arc::new(SqlxUnitOfWork::new(db.clone()));
        let result: Result<()> = unit
            .execute(async {
                bus.publish_all(&[ping()]).await?;
                assert!(!woken(&wakeups).await, "not before the commit");
                Err(FrameworkError::invalid_configuration("no"))
            })
            .await;
        assert!(result.is_err());
        assert!(!woken(&wakeups).await, "a rolled-back unit wakes nobody");

        let unit: Arc<dyn UnitOfWork> = Arc::new(SqlxUnitOfWork::new(db));
        unit.execute(async {
            bus.publish_all(&[ping()]).await?;
            assert!(!woken(&wakeups).await);
            Ok(())
        })
        .await
        .unwrap();
        assert!(woken(&wakeups).await, "after the commit");
    }
}
