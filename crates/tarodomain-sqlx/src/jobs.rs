//! Background jobs (spec §18.3, §25.2): catch-up, saga timeouts and cleanup.
//!
//! Each job runs under a lease in `tarodomain_locks`, so two processes (or two
//! overlapping runs) don't run the same job at once: the second skips. The
//! entry points of Phase 12 call [`run_job`] on their intervals and when the
//! [`SqlxEventBus`](crate::event_bus::SqlxEventBus) wakes them.

use std::sync::Arc;
use std::time::Duration;

use tarodomain::error::Result;
use tarodomain::framework::{CleanupReport, ScheduledRun, TaroDomain};
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::new_v7;

use crate::db::{Db, Param};
use crate::sequencer::Sequencer;

/// How long a job holds its lease at most, if it dies without releasing it.
pub const LEASE_TTL: Duration = Duration::from_secs(5 * 60);

/// A job the host runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScheduledJob {
    /// Numbers committed events, then catches every handler up and runs the
    /// saga timeouts.
    CatchUp,
    /// Deletes processed commands, processed events and dead letters past
    /// their retention (§14, §21).
    Cleanup,
}

impl ScheduledJob {
    /// The job's lease name.
    pub fn lease_name(self) -> &'static str {
        match self {
            Self::CatchUp => "catch-up",
            Self::Cleanup => "cleanup",
        }
    }
}

/// What a job did.
#[derive(Debug)]
pub enum JobReport {
    /// Another run holds the lease: this one skipped.
    Skipped,
    /// A catch-up run, and how many events the sequencer numbered first.
    CatchUp {
        /// Events given a position.
        sequenced: u64,
        /// The catch-up and timeout results.
        run: ScheduledRun,
    },
    /// A cleanup run.
    Cleanup(CleanupReport),
}

/// A lease on a named job. Held until released or until it expires.
#[derive(Debug)]
pub struct Lease {
    name: String,
    holder: String,
}

/// Leases in the database's lock table, with expiry judged by a clock.
#[derive(Clone)]
pub struct Leases {
    db: Db,
    clock: Arc<dyn Clock>,
}

impl Leases {
    /// Leases in `db`.
    pub fn new(db: Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }

    /// Takes the lease on `name` for `ttl`, or returns `None` if someone else
    /// holds it and it hasn't expired.
    pub async fn try_acquire(&self, name: &str, ttl: Duration) -> Result<Option<Lease>> {
        let now = self.clock.now().timestamp_millis();
        let expires_at = now.saturating_add(i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX));
        let holder = new_v7().to_string();
        // Insert, or take over an expired lease. A live one is left alone, so
        // the statement changes no row.
        let changed = self
            .db
            .execute(
                "INSERT INTO tarodomain_locks (name, holder, expires_at) VALUES (?, ?, ?) \
                 ON CONFLICT (name) DO UPDATE SET holder = excluded.holder, \
                 expires_at = excluded.expires_at WHERE tarodomain_locks.expires_at <= ?",
                vec![
                    Param::text(name),
                    Param::text(holder.as_str()),
                    Param::Int(expires_at),
                    Param::Int(now),
                ],
            )
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
            .execute(
                "DELETE FROM tarodomain_locks WHERE name = ? AND holder = ?",
                vec![Param::text(lease.name), Param::text(lease.holder)],
            )
            .await
            .map(drop)
    }
}

/// Runs `job` under its lease. An overlapping run, which finds the lease held,
/// skips. The lease is released when the job ends, also when it fails.
pub async fn run_job(domain: &TaroDomain, db: &Db, job: ScheduledJob) -> Result<JobReport> {
    let leases = Leases::new(db.clone(), domain.provider().clock());
    let Some(lease) = leases.try_acquire(job.lease_name(), LEASE_TTL).await? else {
        return Ok(JobReport::Skipped);
    };
    let report = match job {
        ScheduledJob::CatchUp => match Sequencer::new(db.clone()).run().await {
            Ok(sequenced) => Ok(JobReport::CatchUp {
                sequenced,
                run: domain.run_scheduled().await,
            }),
            Err(error) => Err(error),
        },
        ScheduledJob::Cleanup => domain.run_cleanup().await.map(JobReport::Cleanup),
    };
    leases.release(lease).await?;
    report
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use chrono::{TimeDelta, Utc};
    use sqlx::sqlite::SqlitePoolOptions;
    use tarodomain::adapters::Provider;
    use tarodomain::kernel::clock::ManualClock;
    use tarodomain_testing::fixtures::{event_id, events, stream_id};

    use super::*;
    use crate::provider::SqlxProvider;

    const TTL: Duration = Duration::from_secs(60);

    async fn provider() -> SqlxProvider {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        let provider = SqlxProvider::sqlite(pool);
        provider.initialize().await.unwrap();
        provider
    }

    #[tokio::test]
    async fn a_held_lease_blocks_an_overlapping_run_and_release_frees_it() {
        let provider = provider().await;
        let leases = Leases::new(provider.db().clone(), provider.clock());
        let first = leases.try_acquire("catch-up", TTL).await.unwrap().unwrap();
        assert!(leases.try_acquire("catch-up", TTL).await.unwrap().is_none());
        assert!(
            leases.try_acquire("cleanup", TTL).await.unwrap().is_some(),
            "other jobs are independent"
        );
        leases.release(first).await.unwrap();
        assert!(leases.try_acquire("catch-up", TTL).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn an_expired_lease_is_taken_over_and_the_old_holder_cannot_release_it() {
        let provider = provider().await;
        let clock = Arc::new(ManualClock::new(Utc::now()));
        let leases = Leases::new(provider.db().clone(), clock.clone());
        let old = leases.try_acquire("catch-up", TTL).await.unwrap().unwrap();
        clock.advance(TimeDelta::seconds(59));
        assert!(leases.try_acquire("catch-up", TTL).await.unwrap().is_none());
        clock.advance(TimeDelta::seconds(1));
        assert!(leases.try_acquire("catch-up", TTL).await.unwrap().is_some());
        leases.release(old).await.unwrap();
        assert!(
            leases.try_acquire("catch-up", TTL).await.unwrap().is_none(),
            "the stale holder's release left the new lease alone"
        );
    }

    #[tokio::test]
    async fn catch_up_numbers_events_first_and_a_held_lease_skips_the_run() {
        let provider = provider().await;
        let domain = TaroDomain::builder()
            .use_provider(Arc::new(provider.clone()))
            .skip_validation()
            .build()
            .await
            .unwrap();
        let db = provider.db();
        // Events appended through the adapter wait for a position.
        let store = provider.create_event_store_adapter();
        let stream = stream_id("BankAccount", 1);
        store
            .append_to_stream(&stream, events([event_id(1), event_id(2)]), 0)
            .await
            .unwrap();

        let leases = Leases::new(db.clone(), provider.clock());
        let held = leases.try_acquire("catch-up", TTL).await.unwrap().unwrap();
        let skipped = run_job(&domain, db, ScheduledJob::CatchUp).await.unwrap();
        assert!(matches!(skipped, JobReport::Skipped));
        leases.release(held).await.unwrap();

        let report = run_job(&domain, db, ScheduledJob::CatchUp).await.unwrap();
        assert!(matches!(report, JobReport::CatchUp { sequenced: 2, .. }));
        let again = run_job(&domain, db, ScheduledJob::CatchUp).await.unwrap();
        assert!(
            matches!(again, JobReport::CatchUp { sequenced: 0, .. }),
            "the lease was released, and nothing was left to number"
        );
        let cleanup = run_job(&domain, db, ScheduledJob::Cleanup).await.unwrap();
        assert!(matches!(cleanup, JobReport::Cleanup(_)));
    }
}
