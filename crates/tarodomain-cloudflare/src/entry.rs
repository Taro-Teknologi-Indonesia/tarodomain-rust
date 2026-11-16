//! The Worker entry points (spec §25.2, §26.1): `fetch`, `queue` and
//! `scheduled` over a built [`TaroDomain`].
//!
//! The decisions are plain functions that run natively under `cargo test`:
//! what to do with a queue message ([`queue_action`]), which job a cron
//! expression means ([`select_job`]), and what a leased job did ([`run_job`]).
//! `CloudflareWorker` (wasm32 only) converts `worker` types to and from
//! them.
//!
//! # Using it
//!
//! Build the framework once per isolate and hand it to a worker in each
//! `#[event(...)]` function:
//!
//! ```ignore
//! use worker::*;
//!
//! #[event(fetch)]
//! async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
//!     bank_worker(&env).await?.fetch(req).await
//! }
//! ```
//!
//! The backpressure budget of catch-up (25 s by default) is wall-clock time,
//! and stays well below a Worker's limits; lower it with the builder if the
//! plan's CPU limit is tighter.

use std::time::Duration;

use tarodomain::domain::SerializedEvent;
use tarodomain::error::{FrameworkError, Result};
use tarodomain::framework::{
    CleanupReport, MessageOutcome, MessageResult, QueueMessage, ScheduledRun, TaroDomain,
};

use crate::lease::Leases;

/// The cron expression that runs the cleanup job unless the app says
/// otherwise: daily at 03:00 UTC. Any other expression runs catch-up and the
/// saga timeouts.
pub const DEFAULT_CLEANUP_CRON: &str = "0 3 * * *";

/// The longest a Cloudflare Queue can delay a retry: 12 hours.
pub const MAX_RETRY_DELAY_SECONDS: u32 = 12 * 60 * 60;

/// How long a scheduled job holds its lease at most, if it dies without
/// releasing it.
pub const LEASE_TTL: Duration = Duration::from_secs(5 * 60);

/// What to do with a queue message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueAction {
    /// `message.ack()`.
    Ack,
    /// `message.retry_with_options(delay_seconds)`.
    Retry {
        /// How long the queue waits before redelivering.
        delay_seconds: u32,
    },
}

/// Maps the framework's verdict on a message to the queue's action (§10.5):
/// a dead-lettered message is acknowledged, because its dead letter is
/// already stored; a retry waits as long as the retry policy says, at least
/// one second (rounded up) and at most [`MAX_RETRY_DELAY_SECONDS`].
pub fn queue_action(outcome: MessageOutcome) -> QueueAction {
    match outcome {
        MessageOutcome::Ack | MessageOutcome::DeadLettered => QueueAction::Ack,
        MessageOutcome::Retry { delay } => {
            let seconds = delay.as_secs() + u64::from(delay.subsec_nanos() > 0);
            let seconds = seconds.clamp(1, u64::from(MAX_RETRY_DELAY_SECONDS));
            QueueAction::Retry {
                delay_seconds: u32::try_from(seconds).unwrap_or(MAX_RETRY_DELAY_SECONDS),
            }
        }
    }
}

/// Reads a queue message body: the JSON of a [`SerializedEvent`], as the
/// queue bus sends it. Fails with `SERIALIZATION_ERROR`.
pub fn parse_message(body: &str, attempts: u32) -> Result<QueueMessage> {
    let event: SerializedEvent = serde_json::from_str(body).map_err(|error| {
        FrameworkError::serialization_error("a queue message is not a serialized event")
            .with_source(error)
    })?;
    Ok(QueueMessage {
        event,
        attempts: attempts.max(1),
    })
}

/// Handles one batch of queue messages, given as `(body, delivery count)`,
/// and says what to do with each, in order.
///
/// A body that isn't an event can never succeed, so it is acknowledged
/// (and logged) instead of retried forever.
pub async fn consume(domain: &TaroDomain, messages: &[(String, u32)]) -> Vec<QueueAction> {
    let mut parsed = Vec::new();
    let mut slots = Vec::new();
    for (body, attempts) in messages {
        match parse_message(body, *attempts) {
            Ok(message) => {
                slots.push(Some(parsed.len()));
                parsed.push(message);
            }
            Err(error) => {
                tracing::error!(%error, "dropping a queue message that is not an event");
                slots.push(None);
            }
        }
    }
    let results: Vec<MessageResult> = domain.handle_events(parsed).await;
    for result in &results {
        if let Some(error) = &result.error {
            tracing::warn!(event_id = %result.event_id, %error, outcome = ?result.outcome, "a queue message failed");
        }
    }
    slots
        .into_iter()
        .map(|slot| slot.map_or(QueueAction::Ack, |i| queue_action(results[i].outcome)))
        .collect()
}

/// A scheduled job.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScheduledJob {
    /// Catch-up of every handler, then the saga timeouts.
    CatchUp,
    /// The §14 and §21 retention.
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

/// Picks the job for the cron expression that fired: `cleanup_cron` means
/// cleanup, anything else catch-up. Whitespace between fields doesn't matter.
pub fn select_job(cron: &str, cleanup_cron: &str) -> ScheduledJob {
    let normalize = |text: &str| text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalize(cron) == normalize(cleanup_cron) {
        ScheduledJob::Cleanup
    } else {
        ScheduledJob::CatchUp
    }
}

/// What a scheduled job did.
#[derive(Debug)]
pub enum JobReport {
    /// Another run holds the lease: this one skipped.
    Skipped,
    /// A catch-up run.
    CatchUp(ScheduledRun),
    /// A cleanup run.
    Cleanup(CleanupReport),
}

/// Runs `job` under its lease. An overlapping run, which finds the lease
/// held, skips. The lease is released when the job ends, also when it fails.
pub async fn run_job(domain: &TaroDomain, leases: &Leases, job: ScheduledJob) -> Result<JobReport> {
    let Some(lease) = leases.try_acquire(job.lease_name(), LEASE_TTL).await? else {
        return Ok(JobReport::Skipped);
    };
    let report = match job {
        ScheduledJob::CatchUp => Ok(JobReport::CatchUp(domain.run_scheduled().await)),
        ScheduledJob::Cleanup => domain.run_cleanup().await.map(JobReport::Cleanup),
    };
    leases.release(lease).await?;
    report
}

#[cfg(target_arch = "wasm32")]
pub use worker_entry::CloudflareWorker;

#[cfg(target_arch = "wasm32")]
mod worker_entry {
    use tarodomain::framework::{HttpApi, HttpRequest, TaroDomain};
    use worker::{
        MessageBatch, MessageExt, QueueRetryOptionsBuilder, Request, Response, ScheduledEvent,
    };

    use super::{DEFAULT_CLEANUP_CRON, JobReport, QueueAction, consume, run_job, select_job};
    use crate::lease::Leases;
    use crate::provider::CloudflareProvider;

    /// TaroDomain's three Worker entry points over one built framework.
    pub struct CloudflareWorker {
        domain: TaroDomain,
        api: HttpApi,
        leases: Leases,
        cleanup_cron: String,
    }

    impl CloudflareWorker {
        /// Wraps `domain`, which must run on `provider`.
        pub fn new(domain: TaroDomain, provider: &CloudflareProvider) -> Self {
            use tarodomain::adapters::Provider;
            Self {
                api: HttpApi::new(domain.clone()),
                leases: Leases::new(provider.db().clone(), provider.clock()),
                domain,
                cleanup_cron: DEFAULT_CLEANUP_CRON.to_owned(),
            }
        }

        /// Turns `POST /migrate` on. It is off by default.
        #[must_use]
        pub fn allow_migrate(mut self, allow: bool) -> Self {
            self.api = self.api.allow_migrate(allow);
            self
        }

        /// The cron trigger that runs cleanup; the others run catch-up.
        #[must_use]
        pub fn cleanup_cron(mut self, cron: impl Into<String>) -> Self {
            self.cleanup_cron = cron.into();
            self
        }

        /// The `fetch` handler: the §26.1 routes of [`HttpApi`].
        pub async fn fetch(&self, mut request: Request) -> worker::Result<Response> {
            let mut http = HttpRequest::new(request.method().to_string(), request.path());
            for (name, value) in request.headers().entries() {
                http = http.with_header(&name, value);
            }
            let body = request.text().await.unwrap_or_default();
            let response = self.api.handle(http.with_body(body)).await;
            let mut out = Response::from_json(&response.body)?.with_status(response.status);
            for (name, value) in &response.headers {
                out.headers_mut().set(name, value)?;
            }
            Ok(out)
        }

        /// The `queue` handler: gives each event to the projections and
        /// sagas, then acknowledges or retries each message (§10.5).
        pub async fn queue(&self, batch: MessageBatch<String>) -> worker::Result<()> {
            let messages = batch.messages()?;
            let bodies: Vec<(String, u32)> = messages
                .iter()
                .map(|message| (message.body().clone(), message.attempts()))
                .collect();
            let actions = consume(&self.domain, &bodies).await;
            for (message, action) in messages.iter().zip(actions) {
                match action {
                    QueueAction::Ack => message.ack(),
                    QueueAction::Retry { delay_seconds } => message.retry_with_options(
                        &QueueRetryOptionsBuilder::new()
                            .with_delay_seconds(delay_seconds)
                            .build(),
                    ),
                }
            }
            Ok(())
        }

        /// The `scheduled` handler: runs the job the cron trigger means,
        /// under a lease.
        pub async fn scheduled(&self, event: ScheduledEvent) {
            let job = select_job(&event.cron(), &self.cleanup_cron);
            match run_job(&self.domain, &self.leases, job).await {
                Ok(JobReport::Skipped) => {
                    tracing::info!(?job, "skipped: another run holds the lease")
                }
                Ok(report) => tracing::info!(?job, ?report, "scheduled job finished"),
                Err(error) => tracing::error!(?job, %error, "scheduled job failed"),
            }
        }
    }
}

#[cfg(all(test, feature = "native-sqlite"))]
mod tests {
    use std::sync::Arc;

    use tarodomain::adapters::Provider;

    use super::*;
    use crate::provider::CloudflareProvider;
    use crate::queue_bus::RecordingQueue;
    use crate::sqlite_backend::SqliteBackend;

    #[test]
    fn retry_delays_round_up_and_are_clamped() {
        let retry = |millis| {
            queue_action(MessageOutcome::Retry {
                delay: Duration::from_millis(millis),
            })
        };
        assert_eq!(retry(0), QueueAction::Retry { delay_seconds: 1 });
        assert_eq!(retry(1), QueueAction::Retry { delay_seconds: 1 });
        assert_eq!(retry(1_001), QueueAction::Retry { delay_seconds: 2 });
        assert_eq!(retry(30_000), QueueAction::Retry { delay_seconds: 30 });
        assert_eq!(
            retry(10 * 24 * 3_600_000),
            QueueAction::Retry {
                delay_seconds: MAX_RETRY_DELAY_SECONDS
            }
        );
    }

    #[test]
    fn acked_and_dead_lettered_messages_are_acknowledged() {
        assert_eq!(queue_action(MessageOutcome::Ack), QueueAction::Ack);
        assert_eq!(queue_action(MessageOutcome::DeadLettered), QueueAction::Ack);
    }

    #[test]
    fn the_cleanup_cron_selects_cleanup_and_every_other_one_catch_up() {
        assert_eq!(
            select_job("0 3 * * *", DEFAULT_CLEANUP_CRON),
            ScheduledJob::Cleanup
        );
        assert_eq!(
            select_job("0  3 * * *", DEFAULT_CLEANUP_CRON),
            ScheduledJob::Cleanup
        );
        assert_eq!(
            select_job("* * * * *", DEFAULT_CLEANUP_CRON),
            ScheduledJob::CatchUp
        );
        assert_eq!(
            select_job("*/5 * * * *", "*/5 * * * *"),
            ScheduledJob::Cleanup
        );
    }

    #[test]
    fn a_message_body_is_a_serialized_event() {
        assert!(parse_message("not json", 1).is_err());
        assert!(parse_message("{}", 1).is_err());
        let event = tarodomain_testing::fixtures::event(tarodomain_testing::fixtures::event_id(1));
        let body = serde_json::to_string(&event).unwrap();
        let message = parse_message(&body, 0).unwrap();
        assert_eq!(message.event, event);
        assert_eq!(message.attempts, 1, "attempts count from 1");
    }

    async fn domain() -> (TaroDomain, Leases) {
        let provider = CloudflareProvider::with_backend(
            Arc::new(SqliteBackend::in_memory().unwrap()),
            Some(Arc::new(RecordingQueue::new())),
        );
        provider.initialize().await.unwrap();
        let leases = Leases::new(provider.db().clone(), provider.clock());
        let domain = TaroDomain::builder()
            .use_provider(Arc::new(provider))
            .skip_validation()
            .build()
            .await
            .unwrap();
        (domain, leases)
    }

    #[tokio::test]
    async fn a_job_under_a_held_lease_is_skipped_and_the_lease_is_released_after_a_run() {
        let (domain, leases) = domain().await;
        let held = leases
            .try_acquire("catch-up", LEASE_TTL)
            .await
            .unwrap()
            .unwrap();
        let report = run_job(&domain, &leases, ScheduledJob::CatchUp)
            .await
            .unwrap();
        assert!(matches!(report, JobReport::Skipped));
        leases.release(held).await.unwrap();

        let report = run_job(&domain, &leases, ScheduledJob::CatchUp)
            .await
            .unwrap();
        assert!(matches!(report, JobReport::CatchUp(_)));
        let again = run_job(&domain, &leases, ScheduledJob::CatchUp)
            .await
            .unwrap();
        assert!(
            matches!(again, JobReport::CatchUp(_)),
            "the lease was released"
        );
        let cleanup = run_job(&domain, &leases, ScheduledJob::Cleanup)
            .await
            .unwrap();
        assert!(matches!(cleanup, JobReport::Cleanup(_)));
    }

    #[tokio::test]
    async fn a_garbage_message_is_acknowledged_and_an_unknown_event_type_is_dead_lettered() {
        let (domain, _) = domain().await;
        let event = tarodomain_testing::fixtures::event(tarodomain_testing::fixtures::event_id(1));
        let body = serde_json::to_string(&event).unwrap();
        let actions = consume(&domain, &[("garbage".to_owned(), 1), (body, 1)]).await;
        assert_eq!(actions, [QueueAction::Ack, QueueAction::Ack]);
    }
}
