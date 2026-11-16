//! Time, behind a trait so that tests can fix it.

use std::sync::{Mutex, PoisonError};

use chrono::{DateTime, TimeDelta, Utc};

/// A source of the current time.
///
/// The framework never calls `Utc::now()` directly: it asks the clock it was
/// built with, so tests can use a [`ManualClock`].
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
}
