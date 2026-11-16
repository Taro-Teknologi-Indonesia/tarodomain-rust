//! The dead letter writer (spec §21.2).

use std::fmt;
use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use super::RetryPolicy;
use crate::adapters::{DeadLetterInput, DeadLetterStore};
use crate::domain::DomainEvent;
use crate::error::{FrameworkError, Result};

/// Runs a handler with retries and files the event in the dead letter store
/// when it can't be handled (spec §21.2).
///
/// 1. The [`RetryPolicy`] retries retryable errors.
/// 2. If the operation still fails, because the retries ran out or the
///    error isn't retryable, the event is written to the store with the
///    handler's name, the error and the number of attempts.
/// 3. The operation's error is returned, so the caller knows the event
///    failed. The event is safe in the store, so the caller should ack its
///    message instead of redelivering it.
///
/// If writing the dead letter fails too, the result is a `DEAD_LETTER`
/// error (infrastructure, so retryable) whose `originalError` context holds
/// the handler's error. The event isn't stored anywhere then, so the caller
/// must **not** ack.
#[derive(Clone)]
pub struct DeadLetterWriter {
    policy: RetryPolicy,
    store: Arc<dyn DeadLetterStore>,
}

impl DeadLetterWriter {
    /// A writer that retries with `policy` and files failures in `store`.
    pub fn new(policy: RetryPolicy, store: Arc<dyn DeadLetterStore>) -> Self {
        Self { policy, store }
    }

    /// The retry policy.
    pub fn policy(&self) -> &RetryPolicy {
        &self.policy
    }

    /// Runs `operation` for `handler_name` and `event`, as described on the
    /// type. `operation` is called once per attempt.
    pub async fn execute_with_retry<T, F, Fut>(
        &self,
        handler_name: &str,
        event: &DomainEvent,
        operation: F,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        let attempted = self.policy.run(operation).await;
        let Err(error) = attempted.result else {
            return attempted.result;
        };
        let input = DeadLetterInput {
            handler_name: handler_name.to_string(),
            event_id: event.id(),
            event_type: event.name().to_string(),
            // An event without a JSON form still gets its dead letter.
            payload: event.serialize().map_or(Value::Null, |event| event.data),
            error_code: error.code().as_str().to_string(),
            error_message: error.message().to_string(),
            attempts: attempted.attempts,
        };
        match self.store.write(input).await {
            Ok(_) => Err(error),
            Err(write_error) => Err(FrameworkError::dead_letter(format!(
                "event {} failed for handler {handler_name} and couldn't be dead-lettered: \
                 {write_error}",
                event.id()
            ))
            .with_context("handler", handler_name)
            .with_context("eventId", event.id().to_string())
            .with_context("originalError", error.to_string())
            .with_source(write_error)),
        }
    }
}

impl fmt::Debug for DeadLetterWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeadLetterWriter")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}
