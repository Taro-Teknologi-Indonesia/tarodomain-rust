//! Errors (spec §22).
//!
//! The framework has one error type, [`FrameworkError`]. Its [`ErrorCode`]
//! belongs to an [`ErrorCategory`], and the category decides whether the error
//! is retried and which HTTP status it maps to. User code adds its own codes
//! with [`ErrorCode::custom`] or [`FrameworkError::custom`].

mod codes;
mod report;

use std::borrow::Cow;
use std::error::Error as StdError;
use std::fmt;

use serde_json::{Map, Value};

pub use codes::{ErrorCategory, ErrorCode, ParseErrorCodeError};
pub use report::{HttpErrorResponse, LogEntry, LogLevel};

/// The result type used throughout the framework.
pub type Result<T, E = FrameworkError> = std::result::Result<T, E>;

/// A boxed error that can move between threads, used as an error's source.
pub type BoxError = Box<dyn StdError + Send + Sync + 'static>;

/// The framework's error: a code, a message, structured context, and an
/// optional source error.
///
/// Create one with the constructor named after its code, such as
/// [`FrameworkError::aggregate_not_found`], or with [`FrameworkError::new`].
/// Add details with [`with_context`](Self::with_context) and
/// [`with_source`](Self::with_source).
pub struct FrameworkError {
    // Boxed so that `Result<T, FrameworkError>` stays one pointer wide.
    inner: Box<Inner>,
}

struct Inner {
    code: ErrorCode,
    message: String,
    context: Map<String, Value>,
    source: Option<BoxError>,
}

impl FrameworkError {
    /// Creates an error with the given code and message.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            inner: Box::new(Inner {
                code,
                message: message.into(),
                context: Map::new(),
                source: None,
            }),
        }
    }

    /// Creates an error with a code defined by user code, such as
    /// `FrameworkError::custom("INSUFFICIENT_FUNDS", ErrorCategory::Domain, "…")`.
    pub fn custom(
        code: impl Into<Cow<'static, str>>,
        category: ErrorCategory,
        message: impl Into<String>,
    ) -> Self {
        Self::new(ErrorCode::custom(code, category), message)
    }

    /// Creates an `OPTIMISTIC_CONCURRENCY` error: the stream is at `actual`,
    /// but the writer expected `expected`.
    ///
    /// The context holds `streamId`, `expectedVersion` and `actualVersion`.
    pub fn optimistic_concurrency(
        stream_id: impl Into<String>,
        expected: u64,
        actual: u64,
    ) -> Self {
        let stream_id = stream_id.into();
        let message = format!("stream {stream_id} is at version {actual}, expected {expected}");
        Self::new(ErrorCode::OptimisticConcurrency, message)
            .with_context("streamId", stream_id)
            .with_context("expectedVersion", expected)
            .with_context("actualVersion", actual)
    }

    /// The error code.
    pub fn code(&self) -> &ErrorCode {
        &self.inner.code
    }

    /// The category of the error code.
    pub fn category(&self) -> ErrorCategory {
        self.inner.code.category()
    }

    /// The human-readable message.
    pub fn message(&self) -> &str {
        &self.inner.message
    }

    /// Structured details, such as the stream and versions of a conflict.
    pub fn context(&self) -> &Map<String, Value> {
        &self.inner.context
    }

    /// Whether the error is worth retrying, from its category.
    pub fn retryable(&self) -> bool {
        self.category().retryable()
    }

    /// The HTTP status, from its category.
    pub fn http_status(&self) -> u16 {
        self.category().http_status()
    }

    /// Adds a context entry, replacing an earlier entry with the same key.
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<Value>) -> Self {
        self.inner.context.insert(key.into(), value.into());
        self
    }

    /// Sets the error that caused this one.
    pub fn with_source(mut self, source: impl Into<BoxError>) -> Self {
        self.inner.source = Some(source.into());
        self
    }
}

impl fmt::Display for FrameworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.inner.code, self.inner.message)
    }
}

impl fmt::Debug for FrameworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FrameworkError")
            .field("code", &self.inner.code)
            .field("message", &self.inner.message)
            .field("context", &self.inner.context)
            .field("source", &self.inner.source)
            .finish()
    }
}

impl StdError for FrameworkError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.inner
            .source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

/// Whether an error is worth retrying (spec §22.1).
///
/// A [`FrameworkError`] answers with [`FrameworkError::retryable`]. Any other
/// error, such as a network or driver failure, counts as infrastructure and is
/// retried.
pub fn should_retry(error: &(dyn StdError + 'static)) -> bool {
    error
        .downcast_ref::<FrameworkError>()
        .is_none_or(FrameworkError::retryable)
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    #[test]
    fn retryable_and_status_follow_the_category_for_every_code() {
        for code in ErrorCode::built_in() {
            let error = FrameworkError::new(code.clone(), "test");
            let (retryable, status) = match code.category() {
                ErrorCategory::Domain => (false, 400),
                ErrorCategory::Concurrency => (true, 409),
                ErrorCategory::Infrastructure => (true, 500),
                ErrorCategory::Configuration => (false, 500),
            };
            assert_eq!(error.retryable(), retryable, "{code}");
            assert_eq!(error.http_status(), status, "{code}");
        }
    }

    #[test]
    fn constructors_set_their_code() {
        let error = FrameworkError::aggregate_not_found("no such account");
        assert_eq!(*error.code(), ErrorCode::AggregateNotFound);
        assert_eq!(error.message(), "no such account");
        assert_eq!(error.to_string(), "AGGREGATE_NOT_FOUND: no such account");

        let error = FrameworkError::circular_dependency("a -> b -> a");
        assert_eq!(error.category(), ErrorCategory::Configuration);
    }

    #[test]
    fn optimistic_concurrency_carries_the_versions() {
        let error = FrameworkError::optimistic_concurrency("BankAccount-1", 3, 5);
        assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
        assert!(error.retryable());
        assert_eq!(error.http_status(), 409);
        assert_eq!(error.context()["streamId"], "BankAccount-1");
        assert_eq!(error.context()["expectedVersion"], 3);
        assert_eq!(error.context()["actualVersion"], 5);
    }

    #[test]
    fn custom_error_uses_its_category() {
        let category = ErrorCategory::Domain;
        let error = FrameworkError::custom("INSUFFICIENT_FUNDS", category, "balance too low");
        assert_eq!(error.code().as_str(), "INSUFFICIENT_FUNDS");
        assert!(!error.retryable());
        assert_eq!(error.http_status(), 400);
    }

    #[test]
    fn context_and_source_are_kept() {
        let error = FrameworkError::event_store_error("append failed")
            .with_context("streamId", "BankAccount-1")
            .with_context("streamId", "BankAccount-2")
            .with_source(io::Error::other("disk full"));
        assert_eq!(error.context().len(), 1);
        assert_eq!(error.context()["streamId"], "BankAccount-2");
        assert_eq!(error.source().unwrap().to_string(), "disk full");
    }

    #[test]
    fn should_retry_treats_other_errors_as_infrastructure() {
        let retried = [
            FrameworkError::optimistic_concurrency("s", 1, 2),
            FrameworkError::queue_error("unavailable"),
        ];
        for error in &retried {
            assert!(should_retry(error), "{error}");
        }
        let not_retried = [
            FrameworkError::aggregate_not_found("missing"),
            FrameworkError::invalid_configuration("bad"),
        ];
        for error in &not_retried {
            assert!(!should_retry(error), "{error}");
        }
        assert!(should_retry(&io::Error::other("connection reset")));
    }

    #[test]
    fn result_stays_small() {
        assert_eq!(
            size_of::<Result<()>>(),
            size_of::<usize>(),
            "FrameworkError should be one pointer"
        );
    }
}
