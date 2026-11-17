//! The background worker of a native server (spec §26): runs the scheduled
//! jobs on intervals. On Cloudflare, Cron Triggers do this job instead.

use std::future::{Future, pending};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::{Instant, sleep_until};

use super::TaroDomain;

/// Runs catch-up, saga timeouts and cleanup on intervals until told to stop.
///
/// Catch-up also runs as soon as the eventual bus signals new events, when
/// [`wake_on`](Self::wake_on) names the signal. Every run starts with all
/// three jobs, so a restarted server first catches up on what it missed.
///
/// A job's failure is logged and the next run tries again. A shutdown lets
/// the job in progress finish.
///
/// ```no_run
/// # async fn serve(domain: tarodomain::framework::TaroDomain) {
/// use std::time::Duration;
/// use tarodomain::framework::Worker;
///
/// let worker = Worker::new(domain).catch_up_every(Duration::from_secs(2));
/// // Pass any future: a signal handler, a channel, `ctrl_c()`.
/// worker.run(std::future::pending()).await;
/// # }
/// ```
#[derive(Clone, Debug)]
pub struct Worker {
    domain: TaroDomain,
    catch_up_every: Duration,
    timeouts_every: Duration,
    cleanup_every: Duration,
    wake: Option<Arc<Notify>>,
}

impl Worker {
    /// A worker with the default intervals: catch-up every 5 seconds,
    /// timeouts every 30 seconds and cleanup every hour.
    pub fn new(domain: TaroDomain) -> Self {
        Self {
            domain,
            catch_up_every: Duration::from_secs(5),
            timeouts_every: Duration::from_secs(30),
            cleanup_every: Duration::from_secs(3600),
            wake: None,
        }
    }

    /// How often every handler catches up from the event store.
    pub fn catch_up_every(mut self, every: Duration) -> Self {
        self.catch_up_every = every;
        self
    }

    /// How often the saga timeout jobs run.
    pub fn timeouts_every(mut self, every: Duration) -> Self {
        self.timeouts_every = every;
        self
    }

    /// How often the cleanup job runs.
    pub fn cleanup_every(mut self, every: Duration) -> Self {
        self.cleanup_every = every;
        self
    }

    /// Runs catch-up as soon as `signal` is notified, instead of waiting for
    /// the interval. The eventual bus of a provider hands out this signal
    /// (for `tarodomain-sqlx`: `SqlxProvider::wakeups()`).
    pub fn wake_on(mut self, signal: Arc<Notify>) -> Self {
        self.wake = Some(signal);
        self
    }

    /// Runs until `shutdown` completes.
    pub async fn run(&self, shutdown: impl Future<Output = ()>) {
        tokio::pin!(shutdown);
        let now = Instant::now();
        let mut catch_up_at = now;
        let mut timeouts_at = now;
        let mut cleanup_at = now;
        let mut woken = false;
        loop {
            let now = Instant::now();
            if woken || now >= catch_up_at {
                self.catch_up().await;
                catch_up_at = Instant::now() + self.catch_up_every;
                woken = false;
            }
            if now >= timeouts_at {
                for (saga, result) in self.domain.run_timeouts().await {
                    if let Err(error) = result {
                        tracing::error!(saga, %error, "saga timeout job failed");
                    }
                }
                timeouts_at = Instant::now() + self.timeouts_every;
            }
            if now >= cleanup_at {
                match self.domain.run_cleanup().await {
                    Ok(report) => tracing::debug!(?report, "cleanup done"),
                    Err(error) => tracing::error!(%error, "cleanup job failed"),
                }
                cleanup_at = Instant::now() + self.cleanup_every;
            }

            let next = catch_up_at.min(timeouts_at).min(cleanup_at);
            tokio::select! {
                biased;
                () = &mut shutdown => return,
                () = self.signal() => woken = true,
                () = sleep_until(next) => {}
            }
        }
    }

    async fn signal(&self) {
        match &self.wake {
            Some(notify) => notify.notified().await,
            None => pending().await,
        }
    }

    async fn catch_up(&self) {
        for (handler, result) in self.domain.run_catch_up().await {
            match result {
                Err(error) => tracing::error!(handler, %error, "catch-up failed"),
                Ok(done) if done.failed.is_some() => {
                    tracing::warn!(handler, "catch-up is stuck on an event");
                }
                Ok(_) => {}
            }
        }
    }
}
