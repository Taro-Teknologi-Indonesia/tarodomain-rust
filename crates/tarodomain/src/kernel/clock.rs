//! Time, behind a trait so that tests can fix it.
//!
//! Code that has a clock at hand calls [`Clock::now`]. Code that can't take
//! one as an argument, such as building a command or raising an event, calls
//! [`now`], which asks the clock in scope: [`scope`] sets it for a future, as
//! the runtime does with the provider's clock and a test with a
//! [`ManualClock`].

use std::future::Future;
use std::sync::{Arc, Mutex, PoisonError};

use chrono::{DateTime, TimeDelta, Utc};

use super::context;

/// A source of the current time.
///
/// The framework never calls `Utc::now()` directly: it asks the clock it was
/// built with, or the clock in scope ([`now`]), so tests can use a
/// [`ManualClock`].
pub trait Clock: Send + Sync {
    /// The current time, in UTC.
    fn now(&self) -> DateTime<Utc>;
}

/// The system clock.
///
/// On `wasm32-unknown-unknown` it reads `Date.now()`. Inside a Cloudflare
/// Worker that time only advances after I/O.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

/// A clock that only moves when told to, for tests.
#[derive(Debug)]
pub struct ManualClock {
    now: Mutex<DateTime<Utc>>,
}

impl ManualClock {
    /// A clock stopped at `start`.
    pub fn new(start: DateTime<Utc>) -> Self {
        Self {
            now: Mutex::new(start),
        }
    }

    /// Moves the clock to `at`, forwards or backwards.
    pub fn set(&self, at: DateTime<Utc>) {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner) = at;
    }

    /// Moves the clock forward by `by`.
    pub fn advance(&self, by: TimeDelta) {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner) += by;
    }
}

impl Clock for ManualClock {
    fn now(&self) -> DateTime<Utc> {
        *self.now.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The clock in scope, stored in task-local storage.
#[derive(Clone)]
struct ScopedClock(Arc<dyn Clock>);

/// The current time from the clock in scope, or from [`SystemClock`] outside
/// any [`scope`].
pub fn now() -> DateTime<Utc> {
    context::try_with(|scoped: &ScopedClock| scoped.0.now()).unwrap_or_else(|| SystemClock.now())
}

/// Runs `fut` with `clock` as the clock in scope, which [`now`] reads.
///
/// Nested scopes replace it until they end, like every value in task-local
/// storage ([`context::scope`]).
pub fn scope<F: Future>(clock: Arc<dyn Clock>, fut: F) -> impl Future<Output = F::Output> {
    context::scope(ScopedClock(clock), fut)
}

/// Runs `f` with `clock` as the clock in scope.
pub fn sync_scope<R>(clock: Arc<dyn Clock>, f: impl FnOnce() -> R) -> R {
    context::sync_scope(ScopedClock(clock), f)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(text: &str) -> DateTime<Utc> {
        text.parse().unwrap()
    }

    #[test]
    fn manual_clock_moves_only_when_told() {
        let clock = ManualClock::new(at("2026-11-16T00:00:00Z"));
        assert_eq!(clock.now(), at("2026-11-16T00:00:00Z"));
        assert_eq!(clock.now(), clock.now());

        clock.advance(TimeDelta::milliseconds(1_500));
        assert_eq!(clock.now(), at("2026-11-16T00:00:01.500Z"));

        clock.set(at("2026-01-01T00:00:00Z"));
        assert_eq!(clock.now(), at("2026-01-01T00:00:00Z"));
    }

    #[test]
    fn system_clock_is_current() {
        let before = Utc::now();
        let now = SystemClock.now();
        assert!(now >= before);
        assert!(now - before < TimeDelta::seconds(5));
    }

    #[test]
    fn clocks_work_as_trait_objects() {
        let clocks: [Box<dyn Clock>; 2] = [
            Box::new(SystemClock),
            Box::new(ManualClock::new(DateTime::UNIX_EPOCH)),
        ];
        assert!(clocks[0].now() > clocks[1].now());
    }

    #[test]
    fn now_reads_the_system_clock_outside_a_scope() {
        let before = Utc::now();
        let now = now();
        assert!(now >= before);
        assert!(now - before < TimeDelta::seconds(5));
    }

    #[tokio::test]
    async fn now_reads_the_clock_in_scope() {
        let clock = Arc::new(ManualClock::new(at("2026-11-16T00:00:00Z")));
        let seen = scope(clock.clone(), async {
            let first = now();
            clock.advance(TimeDelta::seconds(1));
            let nested = sync_scope(Arc::new(ManualClock::new(DateTime::UNIX_EPOCH)), now);
            (first, now(), nested)
        })
        .await;
        assert_eq!(
            seen,
            (
                at("2026-11-16T00:00:00Z"),
                at("2026-11-16T00:00:01Z"),
                DateTime::UNIX_EPOCH
            )
        );
        assert!(
            now() > at("2026-01-01T00:00:00Z"),
            "back to the system clock"
        );
    }
}
