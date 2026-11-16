//! The retry policy (spec §21.3).

use std::fmt;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{FrameworkError, Result};
use crate::kernel::backoff::{Backoff, RandomSource, Sleeper, SplitMix64};

/// Called before each retry with the error that caused it, the retry's
/// number (from 1) and the delay about to be slept.
pub type RetryHook = Arc<dyn Fn(&FrameworkError, u32, Duration) + Send + Sync>;

/// The outcome of [`RetryPolicy::run`]: the result, and how many times the
/// operation ran.
#[derive(Debug)]
pub struct Attempted<T> {
    /// The last attempt's result.
    pub result: Result<T>,
    /// How many times the operation ran, at least 1.
    pub attempts: u32,
}

/// Retries an operation that fails with a retryable error (spec §21.3).
///
/// Whether an error is retryable comes from its category
/// ([`FrameworkError::retryable`]): concurrency and infrastructure errors
/// are, domain and configuration errors aren't, and the operation's last
/// error comes back unchanged when the retries run out.
///
/// The defaults are 3 retries after 100, 200 and 400 ms with ±10% jitter.
/// Cloning is cheap.
#[derive(Clone)]
pub struct RetryPolicy {
    max_retries: u32,
    backoff: Backoff,
    sleeper: Arc<dyn Sleeper>,
    random: Arc<dyn RandomSource>,
    on_retry: Option<RetryHook>,
}

impl RetryPolicy {
    /// The default number of retries after the first attempt.
    pub const DEFAULT_MAX_RETRIES: u32 = 3;

    /// A policy with the defaults. `sleeper` is how to wait between
    /// retries: pass the provider's (`Provider::sleeper()`).
    pub fn new(sleeper: Arc<dyn Sleeper>) -> Self {
        Self {
            max_retries: Self::DEFAULT_MAX_RETRIES,
            backoff: Backoff {
                initial: Duration::from_millis(100),
                multiplier: 2.0,
                max: Duration::from_secs(10),
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

    /// Whether to retry after `error` failed attempt number `attempt`
    /// (counting from 1): the error must be retryable and a retry left.
    pub fn should_retry(&self, error: &FrameworkError, attempt: u32) -> bool {
        error.retryable() && attempt <= self.max_retries
    }

    /// The delay before retry `attempt`, counting from 0.
    pub fn compute_delay(&self, attempt: u32) -> Duration {
        self.backoff.delay(attempt, self.random.as_ref())
    }

    /// Runs `operation` until it succeeds, fails with an error that isn't
    /// retried, or runs out of retries; and says how many attempts it took.
    ///
    /// `operation` is called once per attempt.
    pub async fn run<T, F, Fut>(&self, mut operation: F) -> Attempted<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let mut attempts = 0;
        loop {
            attempts += 1;
            match operation().await {
                Err(error) if self.should_retry(&error, attempts) => {
                    let delay = self.compute_delay(attempts - 1);
                    if let Some(hook) = &self.on_retry {
                        hook(&error, attempts, delay);
                    }
                    self.sleeper.sleep(delay).await;
                }
                result => return Attempted { result, attempts },
            }
        }
    }

    /// [`run`](Self::run) without the attempt count.
    pub async fn with_retry<T, F, Fut>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        self.run(operation).await.result
    }
}

impl fmt::Debug for RetryPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RetryPolicy")
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
    use crate::error::ErrorCategory;
    use crate::kernel::backoff::RecordingSleeper;

    const MS: Duration = Duration::from_millis(1);

    /// No jitter, so delays are exact.
    fn policy(sleeper: &Arc<RecordingSleeper>) -> RetryPolicy {
        RetryPolicy::new(sleeper.clone()).with_backoff(Backoff {
            initial: 100 * MS,
            multiplier: 2.0,
            max: Duration::from_secs(10),
            jitter: 0.0,
        })
    }

    fn failing(category: ErrorCategory) -> FrameworkError {
        FrameworkError::custom("BOOM", category, "boom")
    }

    #[tokio::test]
    async fn retries_a_retryable_error_after_100_200_400_ms() {
        let sleeper = Arc::new(RecordingSleeper::new());
        let calls = AtomicU32::new(0);
        let attempted = policy(&sleeper)
            .run(|| async {
                calls.fetch_add(1, Ordering::SeqCst);
                Err::<(), _>(failing(ErrorCategory::Infrastructure))
            })
            .await;
        assert_eq!(attempted.attempts, 4);
        assert_eq!(calls.load(Ordering::SeqCst), 4);
        assert!(attempted.result.is_err());
        assert_eq!(sleeper.slept(), vec![100 * MS, 200 * MS, 400 * MS]);
    }

    #[tokio::test]
    async fn a_non_retryable_error_is_returned_at_once() {
        for category in [ErrorCategory::Domain, ErrorCategory::Configuration] {
            let sleeper = Arc::new(RecordingSleeper::new());
            let attempted = policy(&sleeper)
                .run(|| async { Err::<(), _>(failing(category)) })
                .await;
            assert_eq!(attempted.attempts, 1);
            assert!(sleeper.slept().is_empty());
        }
    }

    #[tokio::test]
    async fn succeeds_after_a_transient_failure() {
        let sleeper = Arc::new(RecordingSleeper::new());
        let calls = AtomicU32::new(0);
        let attempted = policy(&sleeper)
            .run(|| async {
                if calls.fetch_add(1, Ordering::SeqCst) < 2 {
                    Err(failing(ErrorCategory::Concurrency))
                } else {
                    Ok("done")
                }
            })
            .await;
        assert_eq!((attempted.result.unwrap(), attempted.attempts), ("done", 3));
        assert_eq!(sleeper.slept().len(), 2);
    }

    #[tokio::test]
    async fn zero_retries_runs_once_and_the_hook_sees_each_retry() {
        let sleeper = Arc::new(RecordingSleeper::new());
        let attempted = policy(&sleeper)
            .with_max_retries(0)
            .run(|| async { Err::<(), _>(failing(ErrorCategory::Infrastructure)) })
            .await;
        assert_eq!(attempted.attempts, 1);

        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let policy = policy(&sleeper)
            .with_max_retries(2)
            .on_retry(move |_, retry, delay| sink.lock().unwrap().push((retry, delay)));
        let _ = policy
            .with_retry(|| async { Err::<(), _>(failing(ErrorCategory::Infrastructure)) })
            .await;
        assert_eq!(*seen.lock().unwrap(), vec![(1, 100 * MS), (2, 200 * MS)]);
    }

    #[test]
    fn jitter_stays_within_ten_percent_by_default() {
        let policy = RetryPolicy::new(Arc::new(RecordingSleeper::new()));
        for _ in 0..50 {
            let delay = policy.compute_delay(1).as_secs_f64() * 1000.0;
            assert!((180.0..=220.0).contains(&delay), "{delay}");
        }
    }
}
