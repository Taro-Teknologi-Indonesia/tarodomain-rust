//! Optimistic concurrency checks and retries (spec §11.5, §16).

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{ErrorCode, FrameworkError, Result};
use crate::kernel::backoff::{Backoff, RandomSource, Sleeper, SplitMix64};

/// Called before each retry with the error that caused it, the retry's
/// number (from 1) and the delay about to be slept.
pub type RetryHook = Arc<dyn Fn(&FrameworkError, u32, Duration) + Send + Sync>;

/// Checks expected stream versions and retries operations that lose a race
/// (spec §11.5, §16.2).
///
/// Only `OPTIMISTIC_CONCURRENCY` is retried. Domain, configuration and other
/// infrastructure errors come back at once, because running the operation
/// again wouldn't change them (infrastructure errors are retried by the queue
/// and the catch-up job instead). When the retries run out, the last conflict
/// is returned unchanged.
///
/// The defaults are 3 retries after 10, 20 and 40 ms, ±10% jitter, capped at
/// one second. Cloning is cheap.
///
/// ```
/// use std::sync::Arc;
/// use tarodomain::command::ConcurrencyChecker;
/// use tarodomain::error::Result;
/// use tarodomain::kernel::backoff::RecordingSleeper;
///
/// async fn run() -> Result<u32> {
///     let checker = ConcurrencyChecker::new(Arc::new(RecordingSleeper::new()));
///     checker.with_retry(|| async { Ok(7) }).await
/// }
/// ```
#[derive(Clone)]
pub struct ConcurrencyChecker {
    max_retries: u32,
    backoff: Backoff,
    sleeper: Arc<dyn Sleeper>,
    random: Arc<dyn RandomSource>,
    on_retry: Option<RetryHook>,
}

impl ConcurrencyChecker {
    /// The default number of retries after the first attempt.
    pub const DEFAULT_MAX_RETRIES: u32 = 3;

    /// A checker with the default policy and a randomly seeded jitter source.
    /// `sleeper` is how to wait between retries: pass the provider's
    /// (`Provider::sleeper()`), since timers depend on the platform.
    pub fn new(sleeper: Arc<dyn Sleeper>) -> Self {
        Self {
            max_retries: Self::DEFAULT_MAX_RETRIES,
            backoff: Backoff {
                initial: Duration::from_millis(10),
                multiplier: 2.0,
                max: Duration::from_secs(1),
                jitter: 0.1,
            },
            sleeper,
            random: Arc::new(SplitMix64::from_entropy()),
            on_retry: None,
        }
    }

    /// Sets the number of retries after the first attempt (0 never retries).
    #[must_use]
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Sets the delays between retries.
    #[must_use]
    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Sets the jitter's random source.
    #[must_use]
    pub fn with_random(mut self, random: Arc<dyn RandomSource>) -> Self {
        self.random = random;
        self
    }

    /// Sets a hook that runs before each retry, for logging or metrics.
    #[must_use]
    pub fn on_retry(
        mut self,
        hook: impl Fn(&FrameworkError, u32, Duration) + Send + Sync + 'static,
    ) -> Self {
        self.on_retry = Some(Arc::new(hook));
        self
    }

    /// The number of retries after the first attempt.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Fails with `OPTIMISTIC_CONCURRENCY` unless the stream is at the
    /// version the caller expected (spec §11.5 `assert`).
    pub fn check(&self, expected: u64, actual: u64, stream_id: &str) -> Result<()> {
        if expected == actual {
            Ok(())
        } else {
            Err(FrameworkError::optimistic_concurrency(
                stream_id, expected, actual,
            ))
        }
    }

    /// The delay before retry `attempt`, counting from 0.
    pub fn compute_delay(&self, attempt: u32) -> Duration {
        self.backoff.delay(attempt, self.random.as_ref())
    }

    /// Runs `operation`, running it again after a delay each time it fails
    /// with `OPTIMISTIC_CONCURRENCY`, up to the retry limit.
    ///
    /// `operation` is called once per attempt and must start from fresh
    /// state: reload the aggregate, decide again, save again (§16.2).
    pub async fn with_retry<T, F, Fut>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut retries = 0;
        loop {
            match operation().await {
                Err(error)
                    if *error.code() == ErrorCode::OptimisticConcurrency
                        && retries < self.max_retries =>
                {
                    let delay = self.compute_delay(retries);
                    retries += 1;
                    if let Some(hook) = &self.on_retry {
                        hook(&error, retries, delay);
                    }
                    self.sleeper.sleep(delay).await;
                }
                result => return result,
            }
        }
    }
}

impl fmt::Debug for ConcurrencyChecker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConcurrencyChecker")
            .field("max_retries", &self.max_retries)
            .field("backoff", &self.backoff)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};

    use super::*;
    use crate::kernel::backoff::RecordingSleeper;

    const MS: Duration = Duration::from_millis(1);

    /// Always returns the same number: no jitter spread.
    struct Middle;

    impl RandomSource for Middle {
        fn next_f64(&self) -> f64 {
            0.5
        }
    }

    fn checker(sleeper: &Arc<RecordingSleeper>) -> ConcurrencyChecker {
        ConcurrencyChecker::new(sleeper.clone()).with_random(Arc::new(Middle))
    }

    fn conflict() -> FrameworkError {
        FrameworkError::optimistic_concurrency("BankAccount-1", 1, 2)
    }

    #[test]
    fn check_passes_on_equal_versions_and_reports_both_otherwise() {
        let checker = checker(&Arc::new(RecordingSleeper::new()));
        assert!(checker.check(3, 3, "s").is_ok());
        let error = checker.check(3, 5, "BankAccount-1").unwrap_err();
        assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
        assert_eq!(error.context()["streamId"], "BankAccount-1");
        assert_eq!(error.context()["expectedVersion"], 3);
        assert_eq!(error.context()["actualVersion"], 5);
    }

    #[test]
    fn delays_double_from_ten_milliseconds() {
        let checker = checker(&Arc::new(RecordingSleeper::new()));
        let delays: Vec<_> = (0..3).map(|n| checker.compute_delay(n)).collect();
        assert_eq!(delays, [10 * MS, 20 * MS, 40 * MS]);
        assert_eq!(checker.compute_delay(30), Duration::from_secs(1));
    }

    #[tokio::test]
    async fn a_first_success_sleeps_never() {
        let sleeper = Arc::new(RecordingSleeper::new());
        let value = checker(&sleeper)
            .with_retry(|| async { Ok(1) })
            .await
            .unwrap();
        assert_eq!(value, 1);
        assert!(sleeper.slept().is_empty());
    }

    #[tokio::test]
    async fn conflicts_are_retried_after_growing_delays() {
        let sleeper = Arc::new(RecordingSleeper::new());
        let calls = AtomicU32::new(0);
        let value = checker(&sleeper)
            .with_retry(|| {
                let call = calls.fetch_add(1, Ordering::SeqCst);
                async move { if call < 3 { Err(conflict()) } else { Ok(call) } }
            })
            .await
            .unwrap();
        assert_eq!(value, 3);
        assert_eq!(sleeper.slept(), [10 * MS, 20 * MS, 40 * MS]);
    }

    #[tokio::test]
    async fn gives_up_after_three_retries_with_the_last_conflict() {
        let sleeper = Arc::new(RecordingSleeper::new());
        let calls = AtomicU32::new(0);
        let error =
            checker(&sleeper)
                .with_retry(|| {
                    let call = u64::from(calls.fetch_add(1, Ordering::SeqCst));
                    async move {
                        Err::<(), _>(FrameworkError::optimistic_concurrency("s", call, call + 1))
                    }
                })
                .await
                .unwrap_err();
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert_eq!(sleeper.slept().len(), 3);
        assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
        assert_eq!(error.context()["expectedVersion"], 3, "the last attempt's");
    }

    #[tokio::test]
    async fn other_errors_are_not_retried() {
        let sleeper = Arc::new(RecordingSleeper::new());
        let makers: [fn() -> FrameworkError; 2] = [
            || {
                FrameworkError::custom(
                    "INSUFFICIENT_FUNDS",
                    crate::error::ErrorCategory::Domain,
                    "too low",
                )
            },
            || FrameworkError::queue_error("unavailable"),
        ];
        for make in makers {
            let calls = AtomicU32::new(0);
            let result = checker(&sleeper)
                .with_retry(|| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move { Err::<(), _>(make()) }
                })
                .await;
            assert_eq!(result.unwrap_err().code(), make().code());
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
        assert!(sleeper.slept().is_empty());
    }

    #[tokio::test]
    async fn zero_retries_runs_once() {
        let sleeper = Arc::new(RecordingSleeper::new());
        let calls = AtomicU32::new(0);
        let result = checker(&sleeper)
            .with_max_retries(0)
            .with_retry(|| {
                calls.fetch_add(1, Ordering::SeqCst);
                async { Err::<(), _>(conflict()) }
            })
            .await;
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn on_retry_sees_each_retry() {
        let sleeper = Arc::new(RecordingSleeper::new());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let result = checker(&sleeper)
            .on_retry(move |error, retry, delay| {
                log.lock().unwrap().push((error.to_string(), retry, delay));
            })
            .with_max_retries(2)
            .with_retry(|| async { Err::<(), _>(conflict()) })
            .await;
        assert!(result.is_err());
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!((seen[0].1, seen[0].2), (1, 10 * MS));
        assert_eq!((seen[1].1, seen[1].2), (2, 20 * MS));
        assert!(seen[0].0.contains("BankAccount-1"));
    }
}
