//! Exponential backoff with jitter, and the sleeper that waits it out.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use super::BoxFuture;
use super::ids::new_v7;

/// A source of uniform random numbers, for jitter.
pub trait RandomSource: Send + Sync {
    /// A number in `[0, 1)`.
    fn next_f64(&self) -> f64;
}

/// The SplitMix64 generator: fast, small, and good enough for jitter. It is
/// not cryptographically secure.
///
/// [`SplitMix64::from_entropy`] seeds it from a UUIDv7's random bits, so the
/// framework needs no `rand` or `getrandom` dependency, which keeps its
/// WebAssembly build simple.
#[derive(Debug)]
pub struct SplitMix64 {
    state: AtomicU64,
}

impl SplitMix64 {
    const GAMMA: u64 = 0x9E37_79B9_7F4A_7C15;

    /// A generator with a fixed seed, for reproducible tests.
    pub fn new(seed: u64) -> Self {
        Self {
            state: AtomicU64::new(seed),
        }
    }

    /// A generator seeded from a fresh UUIDv7.
    pub fn from_entropy() -> Self {
        let bits = new_v7().as_u128();
        // Both halves: the high one holds the timestamp, the low one random bits.
        Self::new((bits >> 64) as u64 ^ bits as u64)
    }

    fn next_u64(&self) -> u64 {
        let mut z = self
            .state
            .fetch_add(Self::GAMMA, Ordering::Relaxed)
            .wrapping_add(Self::GAMMA);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

impl RandomSource for SplitMix64 {
    fn next_f64(&self) -> f64 {
        // The top 53 bits fill an f64 mantissa exactly.
        (self.next_u64() >> 11) as f64 / (1_u64 << 53) as f64
    }
}

/// Exponential backoff: the delay before retry `attempt` (counting from 0) is
/// `initial * multiplier^attempt`, moved by up to ±`jitter` (a fraction, so
/// `0.1` is ±10%), and never more than `max`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Backoff {
    /// The delay before the first retry.
    pub initial: Duration,
    /// The growth factor per attempt.
    pub multiplier: f64,
    /// The longest delay.
    pub max: Duration,
    /// The random spread, as a fraction of the delay.
    pub jitter: f64,
}

impl Backoff {
    /// The delay before retry `attempt`, counting from 0.
    pub fn delay(&self, attempt: u32, random: &dyn RandomSource) -> Duration {
        let exponent = i32::try_from(attempt).unwrap_or(i32::MAX);
        let base = self.initial.as_nanos() as f64 * self.multiplier.powi(exponent);
        let spread = self.jitter * (2.0 * random.next_f64() - 1.0);
        let nanos = (base * (1.0 + spread)).clamp(0.0, self.max.as_nanos() as f64);
        Duration::from_nanos(nanos.round() as u64)
    }
}

/// Waits for a duration.
///
/// The framework sleeps only through this trait, because how to wait depends
/// on the platform: tokio's timers don't work inside a Cloudflare Worker.
pub trait Sleeper: Send + Sync {
    /// Waits for `duration`.
    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()>;
}

/// Sleeps with tokio's timer. The default outside WebAssembly.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone, Copy, Debug, Default)]
pub struct TokioSleeper;

#[cfg(not(target_arch = "wasm32"))]
impl Sleeper for TokioSleeper {
    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
        Box::pin(tokio::time::sleep(duration))
    }
}

/// Records each requested sleep and returns at once, for tests.
#[derive(Debug, Default)]
pub struct RecordingSleeper {
    slept: Mutex<Vec<Duration>>,
}

impl RecordingSleeper {
    /// A sleeper with no recorded sleeps.
    pub fn new() -> Self {
        Self::default()
    }

    /// The durations requested so far, in order.
    pub fn slept(&self) -> Vec<Duration> {
        self.slept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Sleeper for RecordingSleeper {
    fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
        self.slept
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(duration);
        Box::pin(std::future::ready(()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Always returns the same number.
    struct Fixed(f64);

    impl RandomSource for Fixed {
        fn next_f64(&self) -> f64 {
            self.0
        }
    }

    const MS: Duration = Duration::from_millis(1);

    fn concurrency_backoff() -> Backoff {
        Backoff {
            initial: 10 * MS,
            multiplier: 2.0,
            max: Duration::from_secs(1),
            jitter: 0.1,
        }
    }

    #[test]
    fn doubles_without_jitter() {
        let backoff = Backoff {
            jitter: 0.0,
            ..concurrency_backoff()
        };
        let delays: Vec<_> = (0..3).map(|n| backoff.delay(n, &Fixed(0.7))).collect();
        assert_eq!(delays, [10 * MS, 20 * MS, 40 * MS]);
    }

    #[test]
    fn never_exceeds_the_cap() {
        let backoff = concurrency_backoff();
        assert_eq!(backoff.delay(10, &Fixed(0.99)), Duration::from_secs(1));
        assert_eq!(backoff.delay(u32::MAX, &Fixed(0.5)), Duration::from_secs(1));
    }

    #[test]
    fn jitter_stays_within_its_spread() {
        let backoff = concurrency_backoff();
        assert_eq!(backoff.delay(0, &Fixed(0.0)), 9 * MS);
        assert_eq!(backoff.delay(0, &Fixed(0.5)), 10 * MS);
        let random = SplitMix64::from_entropy();
        for _ in 0..1_000 {
            let delay = backoff.delay(1, &random);
            assert!((18 * MS..=22 * MS).contains(&delay), "{delay:?}");
        }
    }

    #[test]
    fn generator_stays_in_the_unit_interval() {
        let random = SplitMix64::new(42);
        let samples: Vec<f64> = (0..10_000).map(|_| random.next_f64()).collect();
        assert!(samples.iter().all(|x| (0.0..1.0).contains(x)));
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        assert!((0.45..0.55).contains(&mean), "mean {mean}");
    }

    #[test]
    fn same_seed_same_sequence() {
        let (a, b) = (SplitMix64::new(7), SplitMix64::new(7));
        for _ in 0..10 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
        assert_ne!(SplitMix64::new(7).next_u64(), SplitMix64::new(8).next_u64());
    }

    #[tokio::test]
    async fn recording_sleeper_records_and_returns_at_once() {
        let sleeper = RecordingSleeper::new();
        sleeper.sleep(Duration::from_secs(60)).await;
        sleeper.sleep(10 * MS).await;
        assert_eq!(sleeper.slept(), [Duration::from_secs(60), 10 * MS]);
    }

    #[tokio::test]
    async fn tokio_sleeper_waits() {
        let start = std::time::Instant::now();
        TokioSleeper.sleep(5 * MS).await;
        assert!(start.elapsed() >= 5 * MS);
    }
}
