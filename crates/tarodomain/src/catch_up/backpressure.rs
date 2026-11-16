//! The backpressure controller (spec §18.4): catch-up within a budget.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use super::CronDispatcher;
use crate::error::FrameworkError;
use crate::kernel::backoff::Sleeper;
use crate::kernel::clock::Clock;

/// The limits of one [`BackpressureController::run`] (spec §18.4).
#[derive(Clone, Debug)]
pub struct BackpressureConfig {
    /// Stop starting batches after this long. Defaults to 25 seconds, which
    /// fits a Cloudflare Worker's scheduled run.
    pub max_duration: Duration,
    /// Stop starting batches after this many events (handled or
    /// dead-lettered). Defaults to 10 000. It is checked before each batch,
    /// so a run can pass it by up to one batch.
    pub max_events_per_run: usize,
    /// How many batches one subscription may take in a run. Defaults to 100.
    pub max_passes_per_subscription: usize,
    /// Pause after each batch that leaves more waiting. Defaults to none.
    pub throttle: Duration,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_duration: Duration::from_secs(25),
            max_events_per_run: 10_000,
            max_passes_per_subscription: 100,
            throttle: Duration::ZERO,
        }
    }
}

/// Why a run ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StopReason {
    /// Every subscription caught up, or stopped on a failure.
    Completed,
    /// The time budget ran out.
    TimeBudget,
    /// The event budget ran out.
    EventBudget,
    /// Every subscription that still had events reached its pass limit.
    PassLimit,
}

/// How one subscription ended a run.
#[derive(Debug)]
pub struct SubscriptionOutcome {
    /// Events handled.
    pub processed: usize,
    /// Events dead-lettered.
    pub dead_lettered: usize,
    /// Batches taken.
    pub passes: usize,
    /// Whether events were still waiting when the run ended.
    pub has_more: bool,
    /// The failure that stopped the subscription, if one did: a handler's
    /// retryable error, or the store's.
    pub failed: Option<FrameworkError>,
}

/// What a run did.
#[derive(Debug)]
pub struct BackpressureReport {
    /// Why it ended.
    pub stopped: StopReason,
    /// Events handled or dead-lettered, all subscriptions together.
    pub events: usize,
    /// How long it took, by the controller's clock.
    pub elapsed: Duration,
    /// Per subscription, by name.
    pub subscriptions: BTreeMap<String, SubscriptionOutcome>,
}

/// Progress after a batch, for the callback.
#[derive(Clone, Debug)]
pub struct Progress {
    /// Events handled or dead-lettered so far.
    pub processed: usize,
    /// Time since the run began.
    pub elapsed: Duration,
    /// `processed` per second, 0 before any time has passed.
    pub events_per_second: f64,
    /// The subscriptions that still have events waiting.
    pub remaining: Vec<String>,
}

type ProgressFn = dyn Fn(&Progress) + Send + Sync;

/// Runs catch-up for every registered handler within a budget (spec §18.4).
///
/// Each round takes one batch from each subscription in name order, so a
/// busy subscription can't starve the others. A subscription leaves the
/// rotation when it catches up, fails, or reaches its pass limit. The run
/// ends when none is left, or when the time or event budget runs out.
pub struct BackpressureController {
    dispatcher: Arc<CronDispatcher>,
    clock: Arc<dyn Clock>,
    sleeper: Arc<dyn Sleeper>,
    config: BackpressureConfig,
    on_progress: Option<Arc<ProgressFn>>,
}

impl BackpressureController {
    /// A controller over `dispatcher`. The provider's clock and sleeper
    /// belong here.
    pub fn new(
        dispatcher: Arc<CronDispatcher>,
        clock: Arc<dyn Clock>,
        sleeper: Arc<dyn Sleeper>,
        config: BackpressureConfig,
    ) -> Self {
        Self {
            dispatcher,
            clock,
            sleeper,
            config,
            on_progress: None,
        }
    }

    /// Calls `callback` after every batch.
    pub fn on_progress(mut self, callback: impl Fn(&Progress) + Send + Sync + 'static) -> Self {
        self.on_progress = Some(Arc::new(callback));
        self
    }

    /// Runs catch-up within the budget.
    pub async fn run(&self) -> BackpressureReport {
        let start = self.clock.now();
        let elapsed = || {
            (self.clock.now() - start)
                .to_std()
                .unwrap_or(Duration::ZERO)
        };
        let mut outcomes: BTreeMap<String, SubscriptionOutcome> = BTreeMap::new();
        let mut active: Vec<String> = Vec::new();
        for name in self.dispatcher.names() {
            outcomes.insert(
                name.clone(),
                SubscriptionOutcome {
                    processed: 0,
                    dead_lettered: 0,
                    passes: 0,
                    has_more: true,
                    failed: None,
                },
            );
            if self.config.max_passes_per_subscription > 0 {
                active.push(name);
            }
        }
        let mut events = 0usize;
        let stopped = 'run: loop {
            if active.is_empty() {
                let pass_limited = outcomes
                    .values()
                    .any(|outcome| outcome.has_more && outcome.failed.is_none());
                break if pass_limited {
                    StopReason::PassLimit
                } else {
                    StopReason::Completed
                };
            }
            let mut next_round = Vec::new();
            for name in std::mem::take(&mut active) {
                if elapsed() >= self.config.max_duration {
                    break 'run StopReason::TimeBudget;
                }
                if events >= self.config.max_events_per_run {
                    break 'run StopReason::EventBudget;
                }
                let outcome = outcomes.get_mut(&name).expect("every name has an outcome");
                outcome.passes += 1;
                match self.dispatcher.catch_up(&name).await {
                    Ok(result) => {
                        events += result.processed + result.dead_lettered;
                        outcome.processed += result.processed;
                        outcome.dead_lettered += result.dead_lettered;
                        outcome.has_more = result.has_more;
                        outcome.failed = result.failed.map(|failure| failure.error);
                    }
                    Err(error) => {
                        outcome.has_more = false;
                        outcome.failed = Some(error);
                    }
                }
                let keeps_going = outcome.has_more
                    && outcome.failed.is_none()
                    && outcome.passes < self.config.max_passes_per_subscription;
                if keeps_going {
                    next_round.push(name.clone());
                }
                self.report(events, elapsed(), &outcomes);
                if outcomes[&name].has_more && !self.config.throttle.is_zero() {
                    self.sleeper.sleep(self.config.throttle).await;
                }
            }
            active = next_round;
        };
        // Subscriptions cut off by a budget still have events waiting.
        BackpressureReport {
            stopped,
            events,
            elapsed: elapsed(),
            subscriptions: outcomes,
        }
    }

    fn report(
        &self,
        events: usize,
        elapsed: Duration,
        outcomes: &BTreeMap<String, SubscriptionOutcome>,
    ) {
        let Some(callback) = &self.on_progress else {
            return;
        };
        let seconds = elapsed.as_secs_f64();
        callback(&Progress {
            processed: events,
            elapsed,
            events_per_second: if seconds > 0.0 {
                events as f64 / seconds
            } else {
                0.0
            },
            remaining: outcomes
                .iter()
                .filter(|(_, outcome)| outcome.has_more && outcome.failed.is_none())
                .map(|(name, _)| name.clone())
                .collect(),
        });
    }
}

impl fmt::Debug for BackpressureController {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BackpressureController")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
