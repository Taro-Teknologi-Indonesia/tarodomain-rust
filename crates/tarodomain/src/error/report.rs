//! Rendering errors as JSON, HTTP responses and log entries (spec §22.3).

use std::error::Error as StdError;

use serde::Serialize;
use serde_json::{Map, Value, json};

use super::{ErrorCategory, FrameworkError};

/// An HTTP error response: a status and a JSON body.
///
/// The body is `{"error": {"code", "category", "message", "retryable"}}`, plus
/// `context` for client errors (4xx) only. It never includes the source error.
#[derive(Clone, Debug, PartialEq)]
pub struct HttpErrorResponse {
    /// The HTTP status, from the error's category.
    pub status: u16,
    /// The JSON body.
    pub body: Value,
}

/// The severity of a [`LogEntry`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    /// A rejection, not a fault: domain errors.
    Warn,
    /// A fault: every other category.
    Error,
}

/// A structured log entry for an error.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
    /// `Warn` for domain errors, `Error` for everything else.
    pub level: LogLevel,
    /// The error code, such as `"OPTIMISTIC_CONCURRENCY"`.
    pub code: String,
    /// The category of the code.
    pub category: ErrorCategory,
    /// The human-readable message.
    pub message: String,
    /// Whether the error is worth retrying.
    pub retryable: bool,
    /// The error's structured context.
    pub context: Map<String, Value>,
    /// The messages of the source chain, the direct source first.
    pub causes: Vec<String>,
}

impl FrameworkError {
    /// The error as JSON: `code`, `category`, `message`, `retryable`,
    /// `httpStatus` and `context`. The source error is left out.
    pub fn to_json(&self) -> Value {
        json!({
            "code": self.code(),
            "category": self.category(),
            "message": self.message(),
            "retryable": self.retryable(),
            "httpStatus": self.http_status(),
            "context": self.context(),
        })
    }

    /// The error as an HTTP response. Client errors (4xx) include the
    /// context; server errors (5xx) don't, so server-side details never reach
    /// the caller. The source error is never included.
    pub fn to_http_response(&self) -> HttpErrorResponse {
        let status = self.http_status();
        let mut error = json!({
            "code": self.code(),
            "category": self.category(),
            "message": self.message(),
            "retryable": self.retryable(),
        });
        if (400..500).contains(&status) {
            error["context"] = Value::Object(self.context().clone());
        }
        HttpErrorResponse {
            status,
            body: json!({ "error": error }),
        }
    }

    /// The error as a log entry. Domain errors are rejections, so they log
    /// as warnings; everything else logs as an error, with the source chain.
    pub fn to_log_entry(&self) -> LogEntry {
        let level = match self.category() {
            ErrorCategory::Domain => LogLevel::Warn,
            _ => LogLevel::Error,
        };
        let mut causes = Vec::new();
        let mut next = self.source();
        while let Some(cause) = next {
            causes.push(cause.to_string());
            next = cause.source();
        }
        LogEntry {
            level,
            code: self.code().to_string(),
            category: self.category(),
            message: self.message().to_owned(),
            retryable: self.retryable(),
            context: self.context().clone(),
            causes,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io;

    use super::*;

    fn conflict() -> FrameworkError {
        FrameworkError::optimistic_concurrency("BankAccount-1", 3, 5)
    }

    fn store_failure() -> FrameworkError {
        FrameworkError::event_store_error("append failed")
            .with_context("table", "tarodomain_events")
            .with_source(io::Error::other("password=secret rejected"))
    }

    #[test]
    fn json_has_the_spec_fields() {
        let json = conflict().to_json();
        assert_eq!(json["code"], "OPTIMISTIC_CONCURRENCY");
        assert_eq!(json["category"], "concurrency");
        assert_eq!(json["retryable"], true);
        assert_eq!(json["httpStatus"], 409);
        assert_eq!(json["context"]["actualVersion"], 5);
        assert!(json["message"].is_string());
    }

    #[test]
    fn client_errors_include_the_context() {
        let response = conflict().to_http_response();
        assert_eq!(response.status, 409);
        assert_eq!(response.body["error"]["code"], "OPTIMISTIC_CONCURRENCY");
        assert_eq!(
            response.body["error"]["context"]["streamId"],
            "BankAccount-1"
        );

        let response = FrameworkError::aggregate_not_found("no account").to_http_response();
        assert_eq!(response.status, 400);
        assert_eq!(response.body["error"]["retryable"], false);
        assert!(response.body["error"]["context"].is_object());
    }

    #[test]
    fn server_errors_omit_the_context() {
        let response = store_failure().to_http_response();
        assert_eq!(response.status, 500);
        assert_eq!(response.body["error"]["category"], "infrastructure");
        assert!(response.body["error"].get("context").is_none());

        let response = FrameworkError::invalid_configuration("bad").to_http_response();
        assert_eq!(response.status, 500);
        assert!(response.body["error"].get("context").is_none());
    }

    #[test]
    fn the_source_never_leaks() {
        let error = store_failure();
        assert!(!error.to_json().to_string().contains("secret"));
        assert!(!error.to_http_response().body.to_string().contains("secret"));
    }

    #[test]
    fn domain_errors_log_as_warnings() {
        let entry = FrameworkError::aggregate_not_found("no account").to_log_entry();
        assert_eq!(entry.level, LogLevel::Warn);

        for error in [conflict(), store_failure()] {
            assert_eq!(error.to_log_entry().level, LogLevel::Error, "{error}");
        }
        let entry = FrameworkError::missing_dependency("clock").to_log_entry();
        assert_eq!(entry.level, LogLevel::Error);
    }

    #[test]
    fn log_entries_carry_the_source_chain() {
        let error =
            FrameworkError::event_append_failed("could not append").with_source(store_failure());
        let entry = error.to_log_entry();
        assert_eq!(entry.code, "EVENT_APPEND_FAILED");
        assert_eq!(
            entry.causes,
            [
                "EVENT_STORE_ERROR: append failed",
                "password=secret rejected",
            ]
        );

        let json = serde_json::to_value(&entry).unwrap();
        assert_eq!(json["level"], "error");
        assert_eq!(json["category"], "infrastructure");
    }
}
