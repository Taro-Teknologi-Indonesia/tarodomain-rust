//! Error categories (spec §22.1) and error codes (spec §22.2).

use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize, Serializer};

/// The four error categories of spec §22.1.
///
/// The category decides whether an error is retried and which HTTP status it
/// maps to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ErrorCategory {
    /// A business rule violation. Retrying won't help. HTTP 400.
    Domain,
    /// A transient conflict. A retry might succeed. HTTP 409.
    Concurrency,
    /// A transient failure. A retry might succeed. HTTP 500.
    Infrastructure,
    /// A framework misconfiguration. Retrying won't help. HTTP 500.
    Configuration,
}

impl ErrorCategory {
    /// Every category, in the order of spec §22.1.
    pub const ALL: [ErrorCategory; 4] = [
        Self::Domain,
        Self::Concurrency,
        Self::Infrastructure,
        Self::Configuration,
    ];

    /// Whether errors of this category are worth retrying.
    pub const fn retryable(self) -> bool {
        matches!(self, Self::Concurrency | Self::Infrastructure)
    }

    /// The HTTP status for errors of this category.
    pub const fn http_status(self) -> u16 {
        match self {
            Self::Domain => 400,
            Self::Concurrency => 409,
            Self::Infrastructure | Self::Configuration => 500,
        }
    }

    /// The text form used in JSON, such as `"domain"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Domain => "domain",
            Self::Concurrency => "concurrency",
            Self::Infrastructure => "infrastructure",
            Self::Configuration => "configuration",
        }
    }
}

impl fmt::Display for ErrorCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The error returned when parsing text that isn't a built-in error code.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown error code `{0}`")]
pub struct ParseErrorCodeError(String);

/// Generates [`ErrorCode`] from the table below: the variants, their text form
/// and category, the list of built-in codes, and one `FrameworkError`
/// constructor per code that has a constructor name.
macro_rules! error_codes {
    (
        $(
            $category:ident {
                $(
                    $(#[$meta:meta])*
                    $variant:ident = $code:literal $(=> $ctor:ident)?;
                )+
            }
        )+
    ) => {
        /// An error code: one of the 51 codes of spec §22.2, or a code defined
        /// by user code.
        ///
        /// Each code belongs to one [`ErrorCategory`]. User code adds its own
        /// codes with [`ErrorCode::custom`], for example
        /// `ErrorCode::custom("INSUFFICIENT_FUNDS", ErrorCategory::Domain)`.
        #[derive(Clone, Debug, PartialEq, Eq, Hash)]
        pub enum ErrorCode {
            $( $( $(#[$meta])* $variant, )+ )+
            /// A code defined by user code.
            Custom {
                /// The code, in `SCREAMING_SNAKE_CASE`.
                code: Cow<'static, str>,
                /// The category, which decides retries and the HTTP status.
                category: ErrorCategory,
            },
        }

        impl ErrorCode {
            /// The 51 built-in codes, in the order of spec §22.2.
            pub fn built_in() -> &'static [ErrorCode] {
                static CODES: &[ErrorCode] = &[ $( $( ErrorCode::$variant, )+ )+ ];
                CODES
            }

            /// The text form, such as `"OPTIMISTIC_CONCURRENCY"`.
            pub fn as_str(&self) -> &str {
                match self {
                    $( $( Self::$variant => $code, )+ )+
                    Self::Custom { code, .. } => code,
                }
            }

            /// The category, which decides retries and the HTTP status.
            pub fn category(&self) -> ErrorCategory {
                match self {
                    $( $( Self::$variant => ErrorCategory::$category, )+ )+
                    Self::Custom { category, .. } => *category,
                }
            }

            fn from_built_in(code: &str) -> Option<Self> {
                match code {
                    $( $( $code => Some(Self::$variant), )+ )+
                    _ => None,
                }
            }
        }

        impl super::FrameworkError {
            $( $( $(
                #[doc = concat!("Creates a `", $code, "` error with the given message.")]
                pub fn $ctor(message: impl Into<String>) -> Self {
                    Self::new(ErrorCode::$variant, message)
                }
            )? )+ )+
        }
    };
}

error_codes! {
    Domain {
        /// An event ID that isn't a valid UUID.
        InvalidEventId = "INVALID_EVENT_ID" => invalid_event_id;
        /// An event correlation ID that isn't a valid UUID.
        InvalidCorrelationId = "INVALID_CORRELATION_ID" => invalid_correlation_id;
        /// An event causation ID that isn't a valid UUID.
        InvalidCausationId = "INVALID_CAUSATION_ID" => invalid_causation_id;
        /// An aggregate was created with an ID that is already taken.
        AggregateAlreadyExists = "AGGREGATE_ALREADY_EXISTS" => aggregate_already_exists;
        /// No aggregate exists with the requested ID.
        AggregateNotFound = "AGGREGATE_NOT_FOUND" => aggregate_not_found;
        /// The aggregate's current state doesn't allow the operation.
        InvalidAggregateState = "INVALID_AGGREGATE_STATE" => invalid_aggregate_state;
        /// A command failed validation.
        CommandValidation = "COMMAND_VALIDATION" => command_validation;
        /// A command ID that isn't a valid UUID.
        InvalidCommandId = "INVALID_COMMAND_ID" => invalid_command_id;
        /// A command correlation ID that isn't a valid UUID.
        InvalidCommandCorrelationId = "INVALID_COMMAND_CORRELATION_ID" => invalid_command_correlation_id;
        /// A command causation ID that isn't a valid UUID.
        InvalidCommandCausationId = "INVALID_COMMAND_CAUSATION_ID" => invalid_command_causation_id;
        /// The saga has already completed.
        SagaAlreadyCompleted = "SAGA_ALREADY_COMPLETED" => saga_already_completed;
        /// The saga has already started.
        SagaAlreadyStarted = "SAGA_ALREADY_STARTED" => saga_already_started;
        /// The saga hasn't started yet.
        SagaNotStarted = "SAGA_NOT_STARTED" => saga_not_started;
        /// A compensating command failed.
        CompensationFailed = "COMPENSATION_FAILED" => compensation_failed;
        /// The saga ran past its timeout.
        SagaTimeout = "SAGA_TIMEOUT" => saga_timeout;
        /// The saga state machine doesn't allow the transition (§13.2).
        InvalidSagaTransition = "INVALID_SAGA_TRANSITION" => invalid_saga_transition;
        /// A value object failed its validation.
        ValueObjectInvalid = "VALUE_OBJECT_INVALID" => value_object_invalid;
        /// Two value objects without an ordering were compared.
        IncomparableValue = "INCOMPARABLE_VALUE" => incomparable_value;
        /// Building a value object failed for a reason other than validation.
        ValueObjectConstructionFailed = "VALUE_OBJECT_CONSTRUCTION_FAILED" => value_object_construction_failed;
    }
    Concurrency {
        /// The stream's version isn't the expected one. Created with
        /// `FrameworkError::optimistic_concurrency`.
        OptimisticConcurrency = "OPTIMISTIC_CONCURRENCY";
        /// The command was already processed.
        CommandAlreadyProcessed = "COMMAND_ALREADY_PROCESSED" => command_already_processed;
        /// Another writer changed the data at the same time.
        ConcurrentModification = "CONCURRENT_MODIFICATION" => concurrent_modification;
    }
    Infrastructure {
        /// The event store failed.
        EventStoreError = "EVENT_STORE_ERROR" => event_store_error;
        /// Appending events failed.
        EventAppendFailed = "EVENT_APPEND_FAILED" => event_append_failed;
        /// Loading events failed.
        EventLoadFailed = "EVENT_LOAD_FAILED" => event_load_failed;
        /// The stream doesn't exist.
        StreamNotFound = "STREAM_NOT_FOUND" => stream_not_found;
        /// The snapshot store failed.
        SnapshotStoreError = "SNAPSHOT_STORE_ERROR" => snapshot_store_error;
        /// Loading a snapshot failed.
        SnapshotLoadFailed = "SNAPSHOT_LOAD_FAILED" => snapshot_load_failed;
        /// Saving a snapshot failed.
        SnapshotSaveFailed = "SNAPSHOT_SAVE_FAILED" => snapshot_save_failed;
        /// The event bus failed.
        EventBusError = "EVENT_BUS_ERROR" => event_bus_error;
        /// Publishing events failed.
        EventPublishFailed = "EVENT_PUBLISH_FAILED" => event_publish_failed;
        /// An event handler failed.
        EventHandlerFailed = "EVENT_HANDLER_FAILED" => event_handler_failed;
        /// The saga store failed.
        SagaStoreError = "SAGA_STORE_ERROR" => saga_store_error;
        /// A projection failed.
        ProjectionError = "PROJECTION_ERROR" => projection_error;
        /// A projection's event handler failed.
        ProjectionHandlerFailed = "PROJECTION_HANDLER_FAILED" => projection_handler_failed;
        /// A projection's idempotency record is inconsistent.
        ProjectionIdempotencyViolation = "PROJECTION_IDEMPOTENCY_VIOLATION" => projection_idempotency_violation;
        /// The message queue failed.
        QueueError = "QUEUE_ERROR" => queue_error;
        /// Serializing or deserializing data failed.
        SerializationError = "SERIALIZATION_ERROR" => serialization_error;
        /// An operation took too long.
        OperationTimeout = "OPERATION_TIMEOUT" => operation_timeout;
        /// A message was dead-lettered, or dead-lettering it failed.
        DeadLetter = "DEAD_LETTER" => dead_letter;
    }
    Configuration {
        /// An aggregate has no handler for a stored event.
        MissingEventHandler = "MISSING_EVENT_HANDLER" => missing_event_handler;
        /// The event type isn't registered.
        UnknownEventType = "UNKNOWN_EVENT_TYPE" => unknown_event_type;
        /// The event type is registered twice.
        DuplicateEventRegistration = "DUPLICATE_EVENT_REGISTRATION" => duplicate_event_registration;
        /// No upcaster chain leads from the stored version to the registered one.
        EventVersionMismatch = "EVENT_VERSION_MISMATCH" => event_version_mismatch;
        /// No handler is registered for the command.
        CommandHandlerNotRegistered = "COMMAND_HANDLER_NOT_REGISTERED" => command_handler_not_registered;
        /// The framework configuration is invalid.
        InvalidConfiguration = "INVALID_CONFIGURATION" => invalid_configuration;
        /// A dependency isn't bound in the container.
        MissingDependency = "MISSING_DEPENDENCY" => missing_dependency;
        /// The aggregate type isn't registered.
        AggregateNotRegistered = "AGGREGATE_NOT_REGISTERED" => aggregate_not_registered;
        /// The projection isn't registered.
        ProjectionNotRegistered = "PROJECTION_NOT_REGISTERED" => projection_not_registered;
        /// The saga type isn't registered.
        SagaNotRegistered = "SAGA_NOT_REGISTERED" => saga_not_registered;
        /// Resolving a dependency led back to itself.
        CircularDependency = "CIRCULAR_DEPENDENCY" => circular_dependency;
    }
}

impl ErrorCode {
    /// A code defined by user code, such as
    /// `ErrorCode::custom("INSUFFICIENT_FUNDS", ErrorCategory::Domain)`.
    ///
    /// The text must not be a built-in code: parse those with `str::parse`
    /// instead.
    pub fn custom(code: impl Into<Cow<'static, str>>, category: ErrorCategory) -> Self {
        let code = code.into();
        debug_assert!(
            Self::from_built_in(&code).is_none(),
            "`{code}` is a built-in error code"
        );
        Self::Custom { code, category }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ErrorCode {
    type Err = ParseErrorCodeError;

    /// Parses a built-in code. User codes can't be parsed, because their text
    /// doesn't say which category they belong to.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from_built_in(s).ok_or_else(|| ParseErrorCodeError(s.to_owned()))
    }
}

impl Serialize for ErrorCode {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    fn count(category: ErrorCategory) -> usize {
        ErrorCode::built_in()
            .iter()
            .filter(|code| code.category() == category)
            .count()
    }

    #[test]
    fn counts_per_category_match_the_spec() {
        assert_eq!(count(ErrorCategory::Domain), 19);
        assert_eq!(count(ErrorCategory::Concurrency), 3);
        assert_eq!(count(ErrorCategory::Infrastructure), 18);
        assert_eq!(count(ErrorCategory::Configuration), 11);
        assert_eq!(ErrorCode::built_in().len(), 51);
    }

    #[test]
    fn category_matrix() {
        let expected = [
            (ErrorCategory::Domain, false, 400, "domain"),
            (ErrorCategory::Concurrency, true, 409, "concurrency"),
            (ErrorCategory::Infrastructure, true, 500, "infrastructure"),
            (ErrorCategory::Configuration, false, 500, "configuration"),
        ];
        assert_eq!(expected.map(|row| row.0), ErrorCategory::ALL);
        for (category, retryable, status, name) in expected {
            assert_eq!(category.retryable(), retryable, "{category}");
            assert_eq!(category.http_status(), status, "{category}");
            assert_eq!(category.as_str(), name);
        }
    }

    #[test]
    fn text_form_round_trips() {
        for code in ErrorCode::built_in() {
            assert_eq!(code.as_str().parse::<ErrorCode>(), Ok(code.clone()));
            assert_eq!(code.to_string(), code.as_str());
        }
    }

    #[test]
    fn unknown_text_does_not_parse() {
        assert_eq!(
            "INSUFFICIENT_FUNDS".parse::<ErrorCode>(),
            Err(ParseErrorCodeError("INSUFFICIENT_FUNDS".to_owned()))
        );
        assert!("optimistic_concurrency".parse::<ErrorCode>().is_err());
    }

    #[test]
    fn codes_are_unique_screaming_snake_case() {
        let mut seen = HashSet::new();
        for code in ErrorCode::built_in() {
            let text = code.as_str();
            assert!(seen.insert(text), "{text} appears twice");
            assert!(
                text.chars().all(|c| c.is_ascii_uppercase() || c == '_'),
                "{text} isn't SCREAMING_SNAKE_CASE"
            );
        }
    }

    #[test]
    fn custom_code_keeps_its_category() {
        let code = ErrorCode::custom("INSUFFICIENT_FUNDS", ErrorCategory::Domain);
        assert_eq!(code.as_str(), "INSUFFICIENT_FUNDS");
        assert_eq!(code.category(), ErrorCategory::Domain);
        assert_ne!(
            code,
            ErrorCode::custom("INSUFFICIENT_FUNDS", ErrorCategory::Infrastructure)
        );
    }

    #[test]
    fn serializes_as_text() {
        let json = serde_json::to_value(ErrorCode::OptimisticConcurrency).unwrap();
        assert_eq!(json, "OPTIMISTIC_CONCURRENCY");
        let json = serde_json::to_value(ErrorCategory::Infrastructure).unwrap();
        assert_eq!(json, "infrastructure");
    }
}
