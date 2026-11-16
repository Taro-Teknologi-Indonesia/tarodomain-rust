//! Retries and dead letters (spec §21) on the local provider.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use chrono::{DateTime, Utc};
use tarodomain::adapters::{
    ConsistencyMode, DeadLetterEntry, DeadLetterInput, DeadLetterStore, Provider,
};
use tarodomain::async_trait;
use tarodomain::dead_letter::{DeadLetterWriter, RetryPolicy};
use tarodomain::domain::DomainEvent;
use tarodomain::error::{ErrorCategory, ErrorCode, FrameworkError, Result};
use tarodomain::kernel::backoff::RecordingSleeper;
use tarodomain::kernel::ids::Uuid;

use support::Bank;

/// A dead letter store whose writes can be made to fail.
struct Flaky {
    inner: Arc<dyn DeadLetterStore>,
    fail: bool,
}

#[async_trait]
impl DeadLetterStore for Flaky {
    async fn write(&self, input: DeadLetterInput) -> Result<u64> {
        if self.fail {
            return Err(FrameworkError::event_store_error("the store is down"));
        }
        self.inner.write(input).await
    }
    async fn list(&self, handler: &str, limit: Option<usize>) -> Result<Vec<DeadLetterEntry>> {
        self.inner.list(handler, limit).await
    }
    async fn get(&self, event_id: Uuid, handler: &str) -> Result<Option<DeadLetterEntry>> {
        self.inner.get(event_id, handler).await
    }
    async fn get_by_id(&self, id: u64) -> Result<Option<DeadLetterEntry>> {
        self.inner.get_by_id(id).await
    }
    async fn delete(&self, id: u64) -> Result<()> {
        self.inner.delete(id).await
    }
    async fn count(&self, handler: Option<&str>) -> Result<u64> {
        self.inner.count(handler).await
    }
    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64> {
        self.inner.delete_old(older_than).await
    }
}

struct Fixture {
    writer: DeadLetterWriter,
    store: Arc<dyn DeadLetterStore>,
    sleeper: Arc<RecordingSleeper>,
    event: DomainEvent,
}

async fn fixture(fail_writes: bool) -> Fixture {
    let bank = Bank::new(ConsistencyMode::Eventual, None);
    bank.open("ada").await;
    let event = bank.store.load_all(None, None).await.unwrap().remove(0);
    let store = bank.provider.create_dead_letter_store();
    let sleeper = Arc::new(RecordingSleeper::new());
    let flaky = Arc::new(Flaky {
        inner: store.clone(),
        fail: fail_writes,
    });
    Fixture {
        writer: DeadLetterWriter::new(RetryPolicy::new(sleeper.clone()), flaky),
        store,
        sleeper,
        event,
    }
}

fn error(category: ErrorCategory) -> FrameworkError {
    FrameworkError::custom("HANDLER_DOWN", category, "down")
}

#[tokio::test]
async fn success_writes_nothing() {
    let f = fixture(false).await;
    let value = f
        .writer
        .execute_with_retry("h", &f.event, || async { Ok(5) })
        .await
        .unwrap();
    assert_eq!(value, 5);
    assert_eq!(f.store.count(None).await.unwrap(), 0);
}

#[tokio::test]
async fn a_transient_failure_is_retried_without_a_dead_letter() {
    let f = fixture(false).await;
    let calls = AtomicU32::new(0);
    f.writer
        .execute_with_retry("h", &f.event, || async {
            if calls.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(error(ErrorCategory::Infrastructure))
            } else {
                Ok(())
            }
        })
        .await
        .unwrap();
    assert_eq!(f.sleeper.slept().len(), 1);
    assert_eq!(f.store.count(None).await.unwrap(), 0);
}

#[tokio::test]
async fn exhausted_retries_dead_letter_the_event_and_return_the_error() {
    let f = fixture(false).await;
    let failure = f
        .writer
        .execute_with_retry("projector", &f.event, || async {
            Err::<(), _>(error(ErrorCategory::Infrastructure))
        })
        .await
        .unwrap_err();
    assert_eq!(failure.code().as_str(), "HANDLER_DOWN");
    assert_eq!(f.sleeper.slept().len(), 3);
    let entry = f
        .store
        .get(f.event.id(), "projector")
        .await
        .unwrap()
        .expect("dead-lettered");
    assert_eq!(entry.attempts, 4);
    assert_eq!(entry.event_type, "AccountOpened");
    assert_eq!(entry.error_code, "HANDLER_DOWN");
    assert_eq!(entry.payload["owner"], "ada");
}

#[tokio::test]
async fn a_non_retryable_error_is_dead_lettered_at_once() {
    let f = fixture(false).await;
    f.writer
        .execute_with_retry("projector", &f.event, || async {
            Err::<(), _>(error(ErrorCategory::Domain))
        })
        .await
        .unwrap_err();
    assert!(f.sleeper.slept().is_empty());
    let entry = f
        .store
        .get(f.event.id(), "projector")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.attempts, 1);
}

#[tokio::test]
async fn a_failed_dead_letter_write_is_a_dead_letter_error_so_the_caller_does_not_ack() {
    let f = fixture(true).await;
    let failure = f
        .writer
        .execute_with_retry("projector", &f.event, || async {
            Err::<(), _>(error(ErrorCategory::Domain))
        })
        .await
        .unwrap_err();
    assert_eq!(failure.code(), &ErrorCode::DeadLetter);
    assert!(failure.retryable());
    assert!(
        failure.context()["originalError"]
            .as_str()
            .unwrap()
            .contains("HANDLER_DOWN")
    );
    assert_eq!(f.store.count(None).await.unwrap(), 0);
}
