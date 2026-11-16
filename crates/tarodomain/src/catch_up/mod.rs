//! Catch-up (spec §18): delivering stored events to handlers that missed
//! them, from persistent subscription cursors.
//!
//! [`SubscriptionManager`] owns the cursors, and [`CronDispatcher`] reads
//! after them, runs the handlers and moves them.

mod cron_dispatcher;
mod subscription_manager;

pub use cron_dispatcher::{CatchUpFailure, CatchUpResult, CronDispatcher, DEFAULT_BATCH_SIZE};
pub use subscription_manager::{LagReport, SubscriptionLag, SubscriptionManager};
