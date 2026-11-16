//! Job leases (spec §18.3 via `tarodomain_locks`): an overlapping scheduled
//! run skips instead of running the same job twice.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tarodomain::error::Result;
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::new_v7;

use crate::d1::{D1Db, Statement};

/// A lease on a named job. Held until released or until it expires, so a run
/// that dies without releasing it doesn't block the job for longer than its
/// time to live.
#[derive(Clone)]
pub struct Leases {
    db: D1Db,
    clock: Arc<dyn Clock>,
}

/// A lease that was acquired. Release it when the job ends.
#[derive(Debug)]
pub struct Lease {
    name: String,
    holder: String,
}

impl Leases {
    /// Leases in `db`'s lock table, with expiry judged by `clock`.
    pub fn new(db: D1Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }

    /// Takes the lease on `name` for `ttl`, or returns `None` if someone
    /// else holds it and it hasn't expired.
    pub async fn try_acquire(&self, name: &str, ttl: Duration) -> Result<Option<Lease>> {
        let now = self.clock.now().timestamp_millis();
        let expires_at = now.saturating_add(i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX));
        let holder = new_v7().to_string();
        // Insert, or take over an expired lease. A live one is left alone, so
        // the statement changes no row.
        let changed = self
            .db
            .run(Statement::new(
                "INSERT INTO tarodomain_locks (name, holder, expires_at) VALUES (?, ?, ?) \
                 ON CONFLICT (name) DO UPDATE SET holder = excluded.holder, \
                 expires_at = excluded.expires_at WHERE tarodomain_locks.expires_at <= ?",
                vec![json!(name), json!(holder), json!(expires_at), json!(now)],
            ))
            .await?;
        Ok((changed > 0).then(|| Lease {
            name: name.to_owned(),
            holder,
        }))
    }

    /// Gives the lease back, so the next run can start at once. Does nothing
    /// if it expired and someone else took it over.
    pub async fn release(&self, lease: Lease) -> Result<()> {
        self.db
            .run(Statement::new(
                "DELETE FROM tarodomain_locks WHERE name = ? AND holder = ?",
                vec![json!(lease.name), json!(lease.holder)],
            ))
            .await
            .map(drop)
    }
}

#[cfg(all(test, feature = "native-sqlite"))]
mod tests {
    use chrono::{TimeDelta, Utc};
    use tarodomain::kernel::clock::ManualClock;

    use super::*;
    use crate::migrations::migrate;
    use crate::sqlite_backend::SqliteBackend;

    async fn leases() -> (Leases, Arc<ManualClock>) {
        let db = D1Db::new(Arc::new(SqliteBackend::in_memory().unwrap()));
        migrate(&db).await.unwrap();
        let clock = Arc::new(ManualClock::new(Utc::now()));
        (Leases::new(db, clock.clone()), clock)
    }

    const TTL: Duration = Duration::from_secs(60);

    #[tokio::test]
    async fn a_held_lease_blocks_an_overlapping_run() {
        let (leases, _) = leases().await;
        let first = leases.try_acquire("catch-up", TTL).await.unwrap();
        assert!(first.is_some());
        assert!(leases.try_acquire("catch-up", TTL).await.unwrap().is_none());
        assert!(
            leases.try_acquire("cleanup", TTL).await.unwrap().is_some(),
            "other jobs are independent"
        );
    }

    #[tokio::test]
    async fn release_frees_the_job_at_once() {
        let (leases, _) = leases().await;
        let lease = leases.try_acquire("catch-up", TTL).await.unwrap().unwrap();
        leases.release(lease).await.unwrap();
        assert!(leases.try_acquire("catch-up", TTL).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn an_expired_lease_is_taken_over_and_the_old_holder_cannot_release_it() {
        let (leases, clock) = leases().await;
        let old = leases.try_acquire("catch-up", TTL).await.unwrap().unwrap();
        clock.advance(TimeDelta::seconds(59));
        assert!(
            leases.try_acquire("catch-up", TTL).await.unwrap().is_none(),
            "not yet expired"
        );
        clock.advance(TimeDelta::seconds(1));
        let new = leases.try_acquire("catch-up", TTL).await.unwrap();
        assert!(new.is_some(), "expired at its deadline");
        leases.release(old).await.unwrap();
        assert!(
            leases.try_acquire("catch-up", TTL).await.unwrap().is_none(),
            "the stale holder's release left the new lease alone"
        );
    }
}
