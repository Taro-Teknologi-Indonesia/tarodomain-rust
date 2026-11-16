//! Saga timeouts (spec §19).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use chrono::TimeDelta;
use serde::{Deserialize, Serialize};

use super::{HandlerKind, ProcessManager, SagaEventHandler};
use crate::adapters::{SagaQuery, SagaRecord, SagaStatus, SagaStoreAdapter};
use crate::bus::EventHandler;
use crate::domain::{DomainEvent, Event, EventEnvelope, Metadata};
use crate::error::{FrameworkError, Result};
use crate::kernel::clock::Clock;
use crate::kernel::ids::{Uuid, derived_v7};

/// The synthetic event that tells a saga it has timed out (spec §19.2).
///
/// It is created by [`SagaTimeoutScheduler`] and given straight to the
/// saga's [`SagaEventHandler`]. It is **never stored**, so it isn't
/// registered in the `EventRegistry`. A saga reacts to it with
/// `r.compensate_when::<SagaTimeout>(…)`.
///
/// Its envelope is derived from the saga's type, ID and `updated_at`: the
/// correlation ID is the saga's ID, and the event ID and `occurred_at` are
/// the same every time the scheduler sees the same saga at the same
/// `updated_at`. A retry after a failure therefore creates the same event,
/// and the commands the saga derives from it get the same IDs, which
/// command idempotency skips.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SagaTimeout {
    /// The saga's type.
    pub saga_type: String,
    /// The saga's ID.
    pub saga_id: Uuid,
    /// The last step the saga ran, if any.
    pub last_step: Option<String>,
}

impl Event for SagaTimeout {
    const NAME: &'static str = "SagaTimeout";
}

impl SagaTimeout {
    /// The timeout event for a stored saga.
    pub fn for_record(record: &SagaRecord) -> DomainEvent {
        let millis = record.updated_at.timestamp_millis();
        let seed = format!(
            "SagaTimeout/{}/{}/{millis}",
            record.saga_type, record.saga_id
        );
        let envelope = EventEnvelope {
            id: derived_v7(seed, 0, u64::try_from(millis).unwrap_or(0)),
            correlation_id: record.saga_id,
            causation_id: None,
            command_causation_id: None,
            metadata: Metadata::default(),
            occurred_at: record.updated_at,
        };
        DomainEvent::new(
            envelope,
            Self {
                saga_type: record.saga_type.clone(),
                saga_id: record.saga_id,
                last_step: Some(record.current_step.clone()).filter(|step| !step.is_empty()),
            },
        )
    }
}

/// What one [`SagaTimeoutScheduler::run`] did.
#[derive(Debug, Default)]
pub struct TimeoutRun {
    /// Timed-out sagas found.
    pub examined: usize,
    /// Sagas whose `SagaTimeout` handler ran.
    pub handled: usize,
    /// Sagas ended as `FAILED`, because they have no timeout handler or the
    /// handler failed for good.
    pub failed: usize,
    /// Sagas left for the next run: a retryable error, as `(saga ID, error)`.
    pub retry_later: Vec<(Uuid, FrameworkError)>,
}

/// Finds sagas of one type that have gone quiet and times them out
/// (spec §19.1). Run it periodically (a Cron Trigger), one scheduler per
/// saga type, each with its own timeout (§19.3).
///
/// A saga has timed out when it is `RUNNING` or `COMPENSATING` and its
/// `updated_at` is older than the timeout. For each:
///
/// - A `RUNNING` saga with a `compensate_when::<SagaTimeout>` route is given
///   a [`SagaTimeout`] through its [`SagaEventHandler`]. The handler returns
///   the compensating commands, which run, and the saga becomes
///   `COMPENSATING`.
/// - A `RUNNING` saga without such a route is ended as `FAILED` with the
///   reason `"timeout"`.
/// - A `COMPENSATING` saga that goes quiet for another timeout is ended as
///   `FAILED` with the reason `"timeout"`: its compensation has had its
///   chance. This is what ends a saga that failed on its own and was left
///   compensating.
/// - A **non-retryable** error from the handler ends the saga as `FAILED`
///   too (the reason says why). A **retryable** one leaves it as it was, to
///   be found again by the next run, with the same event and command IDs,
///   so commands that already ran are skipped as duplicates.
pub struct SagaTimeoutScheduler<P: ProcessManager> {
    store: Arc<dyn SagaStoreAdapter>,
    handler: Arc<SagaEventHandler<P>>,
    clock: Arc<dyn Clock>,
    timeout: Duration,
    max_count: Option<usize>,
}

impl<P: ProcessManager> SagaTimeoutScheduler<P> {
    /// A scheduler that times out sagas quiet for longer than `timeout`,
    /// measured by `clock` against the store's `updated_at`.
    pub fn new(
        store: Arc<dyn SagaStoreAdapter>,
        handler: Arc<SagaEventHandler<P>>,
        clock: Arc<dyn Clock>,
        timeout: Duration,
    ) -> Self {
        Self {
            store,
            handler,
            clock,
            timeout,
            max_count: None,
        }
    }

    /// Handles at most `max_count` sagas per run, oldest first.
    #[must_use]
    pub fn with_max_count(mut self, max_count: usize) -> Self {
        self.max_count = Some(max_count);
        self
    }

    /// The timeout.
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// Times out the sagas that are due. Fails only when the store can't
    /// list them; a saga's own failure is reported in the result.
    pub async fn run(&self) -> Result<TimeoutRun> {
        let cutoff = self.clock.now() - TimeDelta::from_std(self.timeout).unwrap_or(TimeDelta::MAX);
        let query = SagaQuery {
            saga_type: Some(P::SAGA_TYPE.to_string()),
            limit: self.max_count,
        };
        let due = self.store.load_timed_out(cutoff, query).await?;
        let has_handler = self
            .handler
            .routes()
            .handles(HandlerKind::CompensateWhen, SagaTimeout::NAME);
        let mut run = TimeoutRun {
            examined: due.len(),
            ..TimeoutRun::default()
        };
        for record in due {
            if !has_handler || record.status == SagaStatus::Compensating {
                self.fail(&record, "timeout".to_string(), &mut run).await;
                continue;
            }
            match self.handler.handle(&SagaTimeout::for_record(&record)).await {
                Ok(()) => run.handled += 1,
                Err(error) if error.retryable() => run.retry_later.push((record.saga_id, error)),
                Err(error) => {
                    let reason = format!("timeout handler failed: {error}");
                    self.fail(&record, reason, &mut run).await;
                }
            }
        }
        Ok(run)
    }

    async fn fail(&self, record: &SagaRecord, reason: String, run: &mut TimeoutRun) {
        match self.store.fail(P::SAGA_TYPE, record.saga_id, &reason).await {
            Ok(()) => run.failed += 1,
            Err(error) => run.retry_later.push((record.saga_id, error)),
        }
    }
}

impl<P: ProcessManager> fmt::Debug for SagaTimeoutScheduler<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SagaTimeoutScheduler")
            .field("sagaType", &P::SAGA_TYPE)
            .field("timeout", &self.timeout)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};

    use super::*;
    use crate::adapters::SagaStatus;
    use crate::kernel::ids::{is_v7, new_v7};

    fn record(at_secs: i64) -> SagaRecord {
        let at = Utc.timestamp_opt(1_700_000_000 + at_secs, 0).unwrap();
        SagaRecord {
            saga_id: new_v7(),
            saga_type: "TransferSaga".into(),
            correlation_id: new_v7(),
            state: serde_json::Value::Null,
            status: SagaStatus::Running,
            current_step: "startsWhenTransferRequested".into(),
            failure_reason: None,
            started_at: at,
            updated_at: at,
            completed_at: None,
        }
    }

    #[test]
    fn the_event_belongs_to_the_saga_and_is_stable() {
        let saga = record(0);
        let (a, b) = (
            SagaTimeout::for_record(&saga),
            SagaTimeout::for_record(&saga),
        );
        assert_eq!(a.id(), b.id());
        assert!(is_v7(&a.id()));
        let envelope = a.envelope();
        assert_eq!(envelope.correlation_id, saga.saga_id);
        assert_eq!(envelope.causation_id, None);
        assert_eq!(envelope.occurred_at, saga.updated_at);
        let timeout = a.downcast_ref::<SagaTimeout>().unwrap();
        assert_eq!(timeout.saga_id, saga.saga_id);
        assert_eq!(
            timeout.last_step.as_deref(),
            Some("startsWhenTransferRequested")
        );
    }

    #[test]
    fn the_id_depends_on_the_saga_and_its_updated_at() {
        let saga = record(0);
        let mut later = saga.clone();
        later.updated_at += TimeDelta::seconds(1);
        let mut other = saga.clone();
        other.saga_id = new_v7();
        let id = SagaTimeout::for_record(&saga).id();
        assert_ne!(id, SagaTimeout::for_record(&later).id());
        assert_ne!(id, SagaTimeout::for_record(&other).id());
    }

    #[test]
    fn a_saga_without_a_step_has_no_last_step() {
        let mut saga = record(0);
        saga.current_step.clear();
        let event = SagaTimeout::for_record(&saga);
        assert_eq!(event.downcast_ref::<SagaTimeout>().unwrap().last_step, None);
    }
}
