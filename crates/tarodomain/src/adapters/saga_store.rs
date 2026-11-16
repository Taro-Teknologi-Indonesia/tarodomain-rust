//! The saga store adapter contract (spec §9, §19.1).

use std::fmt;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::async_trait;
use crate::error::{FrameworkError, Result};
use crate::kernel::ids::Uuid;

/// Where a saga is in its life, as stored (spec §9.2, §13.2).
///
/// Stored and serialized as `RUNNING`, `COMPENSATING`, `COMPLETED` or
/// `FAILED`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SagaStatus {
    /// Started and moving forward.
    Running,
    /// Undoing its completed steps.
    Compensating,
    /// Ended successfully.
    Completed,
    /// Ended unsuccessfully, with a reason.
    Failed,
}

impl SagaStatus {
    /// Every status, in declaration order.
    pub const ALL: [SagaStatus; 4] = [
        SagaStatus::Running,
        SagaStatus::Compensating,
        SagaStatus::Completed,
        SagaStatus::Failed,
    ];

    /// The stored text, such as `"RUNNING"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            SagaStatus::Running => "RUNNING",
            SagaStatus::Compensating => "COMPENSATING",
            SagaStatus::Completed => "COMPLETED",
            SagaStatus::Failed => "FAILED",
        }
    }

    /// Whether the saga is still going: `Running` or `Compensating`. Only
    /// these can time out (§19.1).
    pub const fn is_active(self) -> bool {
        matches!(self, SagaStatus::Running | SagaStatus::Compensating)
    }

    /// Whether the saga has ended: `Completed` or `Failed`.
    pub const fn is_terminal(self) -> bool {
        !self.is_active()
    }
}

impl fmt::Display for SagaStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SagaStatus {
    type Err = FrameworkError;

    /// Parses the stored text. Anything else is `SERIALIZATION_ERROR`.
    fn from_str(text: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|status| status.as_str() == text)
            .ok_or_else(|| {
                FrameworkError::serialization_error(format!("not a saga status: {text:?}"))
                    .with_context("value", text)
            })
    }
}

/// A saga's state to save (spec §9.2).
#[derive(Clone, Debug, PartialEq)]
pub struct SagaSaveInput {
    /// The saga's ID: the correlation ID of the event that started it.
    pub saga_id: Uuid,
    /// The saga type (`ProcessManager::SAGA_TYPE`).
    pub saga_type: String,
    /// The correlation ID the saga's events and commands share, equal to
    /// `saga_id`.
    pub correlation_id: Uuid,
    /// The saga's own state as JSON, as the process manager stores it.
    pub state: Value,
    /// Where the saga is in its life.
    pub status: SagaStatus,
    /// The last step it ran, such as `"startsWhenTransferRequested"`.
    pub current_step: String,
    /// Why the saga failed, when it has.
    pub failure_reason: Option<String>,
}

/// A stored saga (spec §9.2): the [`SagaSaveInput`] fields plus its
/// timestamps, all by the store's clock.
///
/// [`saved`](Self::saved), [`complete`](Self::complete) and
/// [`fail`](Self::fail) apply the contract's write rules, so an adapter that
/// keeps records in memory (or stages them) can use them instead of
/// restating the rules.
#[derive(Clone, Debug, PartialEq)]
pub struct SagaRecord {
    /// The saga's ID.
    pub saga_id: Uuid,
    /// The saga type.
    pub saga_type: String,
    /// The correlation ID, equal to `saga_id`.
    pub correlation_id: Uuid,
    /// The saga's own state as JSON.
    pub state: Value,
    /// Where the saga is in its life.
    pub status: SagaStatus,
    /// The last step it ran.
    pub current_step: String,
    /// Why the saga failed, when it has.
    pub failure_reason: Option<String>,
    /// When the saga was first saved.
    pub started_at: DateTime<Utc>,
    /// When it was last written. Timeouts are measured from here (§19.1).
    pub updated_at: DateTime<Utc>,
    /// When it reached `Completed` or `Failed`, if it has.
    pub completed_at: Option<DateTime<Utc>>,
}

impl SagaRecord {
    /// The record after [`SagaStoreAdapter::save`] writes `input` at `now`
    /// over `previous`, the record stored before, if any.
    ///
    /// `started_at` is kept from `previous`, `updated_at` is `now`, and
    /// `completed_at` is set when the status is terminal: kept from
    /// `previous` if it had one, else `now`.
    pub fn saved(input: SagaSaveInput, previous: Option<&SagaRecord>, now: DateTime<Utc>) -> Self {
        let completed_at = input.status.is_terminal().then(|| {
            previous
                .and_then(|record| record.completed_at)
                .unwrap_or(now)
        });
        Self {
            saga_id: input.saga_id,
            saga_type: input.saga_type,
            correlation_id: input.correlation_id,
            state: input.state,
            status: input.status,
            current_step: input.current_step,
            failure_reason: input.failure_reason,
            started_at: previous.map_or(now, |record| record.started_at),
            updated_at: now,
            completed_at,
        }
    }

    /// Applies [`SagaStoreAdapter::complete`] at `now`. Returns whether the
    /// record changed: a saga that has already ended is left as it is.
    pub fn complete(&mut self, now: DateTime<Utc>) -> bool {
        self.end(SagaStatus::Completed, None, now)
    }

    /// Applies [`SagaStoreAdapter::fail`] at `now`. Returns whether the
    /// record changed: a saga that has already ended is left as it is.
    pub fn fail(&mut self, reason: impl Into<String>, now: DateTime<Utc>) -> bool {
        self.end(SagaStatus::Failed, Some(reason.into()), now)
    }

    fn end(&mut self, status: SagaStatus, reason: Option<String>, now: DateTime<Utc>) -> bool {
        if self.status.is_terminal() {
            return false;
        }
        self.status = status;
        if reason.is_some() {
            self.failure_reason = reason;
        }
        self.updated_at = now;
        self.completed_at = Some(now);
        true
    }

    /// Whether [`SagaStoreAdapter::load_timed_out`] returns the record for
    /// this cut-off and query.
    pub fn is_timed_out(&self, before: DateTime<Utc>, query: &SagaQuery) -> bool {
        self.status.is_active() && self.updated_at < before && query.matches_type(&self.saga_type)
    }
}

/// Narrows [`SagaStoreAdapter::load_by_status`] and
/// [`SagaStoreAdapter::load_timed_out`]. The default matches every saga type
/// with no limit.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SagaQuery {
    /// Only sagas of this type, if set. A timeout scheduler runs per saga
    /// type (§19.3), so it filters here rather than after the limit.
    pub saga_type: Option<String>,
    /// At most this many records, if set.
    pub limit: Option<usize>,
}

impl SagaQuery {
    /// Whether a saga of type `saga_type` passes the type filter.
    pub fn matches_type(&self, saga_type: &str) -> bool {
        self.saga_type.as_deref().is_none_or(|t| t == saga_type)
    }
}

/// The saga store's storage (spec §9.1): one record per saga, keyed by
/// `(saga_type, saga_id)`.
///
/// Timestamps come from the store's clock. Lists come back ordered by
/// `updated_at`, oldest first, then by type and ID. Deleting a saga that
/// doesn't exist does nothing, and so do [`complete`](Self::complete) and
/// [`fail`](Self::fail) on one.
#[async_trait]
pub trait SagaStoreAdapter: Send + Sync {
    /// Saves a saga, replacing the stored record if there is one (an
    /// upsert), with the timestamps of [`SagaRecord::saved`].
    async fn save(&self, input: SagaSaveInput) -> Result<()>;

    /// The saga with this type and ID, if any.
    async fn load(&self, saga_type: &str, saga_id: Uuid) -> Result<Option<SagaRecord>>;

    /// The saga of this type for a correlation ID, if any.
    ///
    /// A saga's ID is the correlation ID of the event that started it
    /// (§9.2), so this defaults to [`load`](Self::load).
    async fn load_by_correlation_id(
        &self,
        saga_type: &str,
        correlation_id: Uuid,
    ) -> Result<Option<SagaRecord>> {
        self.load(saga_type, correlation_id).await
    }

    /// Marks the saga `Completed`, leaving its state and step as they are
    /// ([`SagaRecord::complete`]). Idempotent: a saga that has already ended
    /// is left unchanged.
    async fn complete(&self, saga_type: &str, saga_id: Uuid) -> Result<()>;

    /// Marks the saga `Failed` with `reason`, leaving its state and step as
    /// they are ([`SagaRecord::fail`]). Idempotent: a saga that has already
    /// ended keeps its status and reason.
    async fn fail(&self, saga_type: &str, saga_id: Uuid, reason: &str) -> Result<()>;

    /// The sagas with this status, oldest `updated_at` first.
    async fn load_by_status(&self, status: SagaStatus, query: SagaQuery)
    -> Result<Vec<SagaRecord>>;

    /// The `Running` and `Compensating` sagas last written before `before`,
    /// oldest first ([`SagaRecord::is_timed_out`]). The type filter applies
    /// before the limit.
    async fn load_timed_out(
        &self,
        before: DateTime<Utc>,
        query: SagaQuery,
    ) -> Result<Vec<SagaRecord>>;

    /// Deletes the saga.
    async fn delete(&self, saga_type: &str, saga_id: Uuid) -> Result<()>;

    /// How many sagas there are, of one type and in one status if given.
    async fn count(&self, saga_type: Option<&str>, status: Option<SagaStatus>) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::kernel::ids::new_v7;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_763_280_000 + seconds, 0).unwrap()
    }

    fn input(status: SagaStatus) -> SagaSaveInput {
        let id = new_v7();
        SagaSaveInput {
            saga_id: id,
            saga_type: "TransferSaga".into(),
            correlation_id: id,
            state: json!({"step": 1}),
            status,
            current_step: "startsWhenTransferRequested".into(),
            failure_reason: None,
        }
    }

    #[test]
    fn status_text_round_trips() {
        for status in SagaStatus::ALL {
            assert_eq!(status.as_str().parse::<SagaStatus>().unwrap(), status);
            assert_eq!(status.to_string(), status.as_str());
            let json = serde_json::to_value(status).unwrap();
            assert_eq!(json, status.as_str());
            assert_eq!(serde_json::from_value::<SagaStatus>(json).unwrap(), status);
        }
        let error = "running".parse::<SagaStatus>().unwrap_err();
        assert_eq!(error.code().as_str(), "SERIALIZATION_ERROR");
    }

    #[test]
    fn only_running_and_compensating_are_active() {
        let active: Vec<_> = SagaStatus::ALL
            .into_iter()
            .filter(|s| s.is_active())
            .collect();
        assert_eq!(active, [SagaStatus::Running, SagaStatus::Compensating]);
        assert!(
            SagaStatus::ALL
                .iter()
                .all(|s| s.is_active() != s.is_terminal())
        );
    }

    #[test]
    fn a_save_keeps_started_at_and_moves_updated_at() {
        let first = SagaRecord::saved(input(SagaStatus::Running), None, at(0));
        assert_eq!((first.started_at, first.updated_at), (at(0), at(0)));
        assert_eq!(first.completed_at, None);

        let next = SagaSaveInput {
            status: SagaStatus::Compensating,
            ..input(SagaStatus::Running)
        };
        let second = SagaRecord::saved(next, Some(&first), at(5));
        assert_eq!((second.started_at, second.updated_at), (at(0), at(5)));
        assert_eq!(second.status, SagaStatus::Compensating);
        assert_eq!(second.completed_at, None);
    }

    #[test]
    fn a_terminal_save_sets_completed_at_once() {
        let running = SagaRecord::saved(input(SagaStatus::Running), None, at(0));
        let done = SagaRecord::saved(input(SagaStatus::Completed), Some(&running), at(5));
        assert_eq!(done.completed_at, Some(at(5)));
        let again = SagaRecord::saved(input(SagaStatus::Completed), Some(&done), at(9));
        assert_eq!(again.completed_at, Some(at(5)));
        assert_eq!(again.updated_at, at(9));
    }

    #[test]
    fn complete_and_fail_end_an_active_saga_once() {
        let mut record = SagaRecord::saved(input(SagaStatus::Running), None, at(0));
        assert!(record.fail("timeout", at(5)));
        assert_eq!(record.status, SagaStatus::Failed);
        assert_eq!(record.failure_reason.as_deref(), Some("timeout"));
        assert_eq!(
            (record.updated_at, record.completed_at),
            (at(5), Some(at(5)))
        );
        assert_eq!(record.state, json!({"step": 1}));
        assert_eq!(record.current_step, "startsWhenTransferRequested");

        let ended = record.clone();
        assert!(!record.fail("other", at(9)));
        assert!(!record.complete(at(9)));
        assert_eq!(record, ended);

        let mut record = SagaRecord::saved(input(SagaStatus::Compensating), None, at(0));
        assert!(record.complete(at(5)));
        assert_eq!(record.status, SagaStatus::Completed);
        assert_eq!(record.failure_reason, None);
    }

    #[test]
    fn timed_out_means_active_older_than_the_cut_off_and_of_the_type() {
        let record = SagaRecord::saved(input(SagaStatus::Running), None, at(0));
        let any = SagaQuery::default();
        assert!(record.is_timed_out(at(1), &any));
        assert!(
            !record.is_timed_out(at(0), &any),
            "the cut-off is exclusive"
        );
        let other_type = SagaQuery {
            saga_type: Some("OtherSaga".into()),
            limit: None,
        };
        assert!(!record.is_timed_out(at(1), &other_type));
        let same_type = SagaQuery {
            saga_type: Some("TransferSaga".into()),
            limit: Some(1),
        };
        assert!(record.is_timed_out(at(1), &same_type));

        let mut ended = record.clone();
        ended.complete(at(0));
        assert!(!ended.is_timed_out(at(1), &any));
    }

    #[test]
    fn the_trait_is_dyn_compatible() {
        fn accepts(_: Option<Arc<dyn SagaStoreAdapter>>) {}
        accepts(None);
    }
}
