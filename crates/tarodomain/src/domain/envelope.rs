//! Envelopes: the framework-owned fields of events and commands (spec §2.1,
//! §2.2, §28.1). The business fields live in the event or command itself.

use chrono::{DateTime, SubsecRound, Utc};

use super::{ExecutionContext, Metadata};
use crate::error::{ErrorCode, FrameworkError, Result};
use crate::kernel::ids::{Uuid, new_v7, parse_uuid};

/// The framework-owned fields of an event (spec §2.1, §28.1).
///
/// [`EventEnvelope::new`] and [`EventEnvelope::from_context`] fill in the
/// spec's defaults: a new UUIDv7 ID, and the tracing IDs and metadata of the
/// execution context, or a new trace without one. IDs read from text go
/// through [`parse_id`](Self::parse_id) and its siblings, which fail with the
/// spec's error codes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventEnvelope {
    /// Globally unique ID. A UUIDv7 for an event raised here; an event read
    /// back may carry any UUID version.
    pub id: Uuid,
    /// End-to-end trace ID, shared by every message of one business
    /// transaction.
    pub correlation_id: Uuid,
    /// ID of the command or event that caused this event, or `None` at the
    /// root of a trace.
    pub causation_id: Option<Uuid>,
    /// For an event raised while handling a command, that command's own
    /// causation ID, such as the event a saga reacted to.
    ///
    /// Commands aren't stored, so this is what lets a correlation trace
    /// (§28.3) link the command in `causation_id` back to its cause. It is
    /// omitted from JSON when `None`.
    pub command_causation_id: Option<Uuid>,
    /// Free-form metadata, such as a user or tenant ID.
    pub metadata: Metadata,
    /// When the event was raised.
    pub occurred_at: DateTime<Utc>,
}

impl EventEnvelope {
    /// An envelope for an event raised now, in the execution context in scope
    /// ([`ExecutionContext::current`]), or starting a new trace outside one.
    ///
    /// See [`from_context`](Self::from_context) for the fields.
    pub fn new(occurred_at: DateTime<Utc>) -> Self {
        Self::from_context(ExecutionContext::current().as_ref(), occurred_at)
    }

    /// An envelope for an event raised in `context`, ignoring the context in
    /// scope.
    ///
    /// The ID is a new UUIDv7. The correlation ID, causation ID, command
    /// causation ID and metadata come from `context`. Without a context the
    /// event is a root: a new correlation ID, no causation and no metadata.
    /// `occurred_at` is cut to whole milliseconds, the precision stores keep
    /// (§2.1), so an envelope read back equals the one written.
    pub fn from_context(context: Option<&ExecutionContext>, occurred_at: DateTime<Utc>) -> Self {
        let occurred_at = to_millis(occurred_at);
        match context {
            Some(context) => Self {
                id: new_v7(),
                correlation_id: context.correlation_id,
                causation_id: context.causation_id,
                command_causation_id: context.command_causation_id,
                metadata: context.metadata.clone(),
                occurred_at,
            },
            None => Self {
                id: new_v7(),
                correlation_id: new_v7(),
                causation_id: None,
                command_causation_id: None,
                metadata: Metadata::new(),
                occurred_at,
            },
        }
    }

    /// Parses an event ID: any UUID version, hyphenated, in either case.
    ///
    /// Fails with `INVALID_EVENT_ID`.
    pub fn parse_id(text: &str) -> Result<Uuid> {
        parse_id(text, "event ID", ErrorCode::InvalidEventId)
    }

    /// Parses an event's correlation ID.
    ///
    /// Fails with `INVALID_CORRELATION_ID`.
    pub fn parse_correlation_id(text: &str) -> Result<Uuid> {
        parse_id(text, "correlation ID", ErrorCode::InvalidCorrelationId)
    }

    /// Parses an event's causation ID or command causation ID.
    ///
    /// Fails with `INVALID_CAUSATION_ID`.
    pub fn parse_causation_id(text: &str) -> Result<Uuid> {
        parse_id(text, "causation ID", ErrorCode::InvalidCausationId)
    }
}

/// The framework-owned fields of a command (spec §2.2, §28.1).
///
/// [`CommandEnvelope::new`] and [`CommandEnvelope::from_context`] fill in the
/// defaults, as for events. A command created while a saga handles an event
/// is therefore caused by that event (§28.2).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandEnvelope {
    /// Unique ID, also the idempotency key (§14.1). A UUIDv7 by default; an ID
    /// a client supplies may be any UUID version.
    pub id: Uuid,
    /// End-to-end trace ID.
    pub correlation_id: Uuid,
    /// ID of the event or command that caused this command, or `None` at the
    /// root of a trace.
    pub causation_id: Option<Uuid>,
    /// Free-form metadata, such as a user or tenant ID.
    pub metadata: Metadata,
    /// When the command was created.
    pub timestamp: DateTime<Utc>,
}

impl CommandEnvelope {
    /// An envelope for a command created now, in the execution context in
    /// scope ([`ExecutionContext::current`]), or starting a new trace outside
    /// one.
    ///
    /// See [`from_context`](Self::from_context) for the fields.
    pub fn new(timestamp: DateTime<Utc>) -> Self {
        Self::from_context(ExecutionContext::current().as_ref(), timestamp)
    }

    /// An envelope for a command created in `context`, ignoring the context
    /// in scope.
    ///
    /// The ID is a new UUIDv7. The correlation ID, causation ID and metadata
    /// come from `context`. Without a context the command is a root: a new
    /// correlation ID, no causation and no metadata. `timestamp` is cut to
    /// whole milliseconds, as for events.
    pub fn from_context(context: Option<&ExecutionContext>, timestamp: DateTime<Utc>) -> Self {
        let timestamp = to_millis(timestamp);
        match context {
            Some(context) => Self {
                id: new_v7(),
                correlation_id: context.correlation_id,
                causation_id: context.causation_id,
                metadata: context.metadata.clone(),
                timestamp,
            },
            None => Self {
                id: new_v7(),
                correlation_id: new_v7(),
                causation_id: None,
                metadata: Metadata::new(),
                timestamp,
            },
        }
    }

    /// Parses a command ID, such as one a client supplies: any UUID version,
    /// hyphenated, in either case.
    ///
    /// Fails with `INVALID_COMMAND_ID`.
    pub fn parse_id(text: &str) -> Result<Uuid> {
        parse_id(text, "command ID", ErrorCode::InvalidCommandId)
    }

    /// Parses a command's correlation ID, such as an `x-request-id` header.
    ///
    /// Fails with `INVALID_COMMAND_CORRELATION_ID`.
    pub fn parse_correlation_id(text: &str) -> Result<Uuid> {
        parse_id(
            text,
            "command correlation ID",
            ErrorCode::InvalidCommandCorrelationId,
        )
    }

    /// Parses a command's causation ID.
    ///
    /// Fails with `INVALID_COMMAND_CAUSATION_ID`.
    pub fn parse_causation_id(text: &str) -> Result<Uuid> {
        parse_id(
            text,
            "command causation ID",
            ErrorCode::InvalidCommandCausationId,
        )
    }
}

fn parse_id(text: &str, field: &str, code: ErrorCode) -> Result<Uuid> {
    parse_uuid(text).ok_or_else(|| {
        FrameworkError::new(code, format!("{field} is not a valid UUID: {text:?}"))
            .with_context("value", text)
    })
}

/// Cuts `at` to whole milliseconds.
fn to_millis(at: DateTime<Utc>) -> DateTime<Utc> {
    at.trunc_subsecs(3)
}

#[cfg(test)]
mod tests {
    use chrono::Timelike;

    use super::*;
    use crate::kernel::ids::is_v7;

    const V4: &str = "550e8400-e29b-41d4-a716-446655440000";

    fn at() -> DateTime<Utc> {
        DateTime::from_timestamp(1_763_280_000, 123_456_789).unwrap()
    }

    fn context() -> ExecutionContext {
        ExecutionContext {
            correlation_id: new_v7(),
            causation_id: Some(new_v7()),
            command_causation_id: Some(new_v7()),
            metadata: Metadata::from([("tenant", "acme")]),
        }
    }

    #[test]
    fn event_defaults_outside_a_context() {
        let envelope = EventEnvelope::new(at());
        assert!(is_v7(&envelope.id));
        assert!(is_v7(&envelope.correlation_id));
        assert_ne!(envelope.id, envelope.correlation_id);
        assert_eq!(envelope.causation_id, None);
        assert_eq!(envelope.command_causation_id, None);
        assert!(envelope.metadata.is_empty());
        assert_eq!(envelope.occurred_at.nanosecond(), 123_000_000);
    }

    #[test]
    fn command_defaults_outside_a_context() {
        let envelope = CommandEnvelope::new(at());
        assert!(is_v7(&envelope.id));
        assert!(is_v7(&envelope.correlation_id));
        assert_ne!(envelope.id, envelope.correlation_id);
        assert_eq!(envelope.causation_id, None);
        assert!(envelope.metadata.is_empty());
        assert_eq!(envelope.timestamp.nanosecond(), 123_000_000);
    }

    #[tokio::test]
    async fn envelopes_inherit_the_context_in_scope() {
        let ctx = context();
        let (event, command) = ExecutionContext::scope(ctx.clone(), async {
            (EventEnvelope::new(at()), CommandEnvelope::new(at()))
        })
        .await;

        assert_eq!(event.correlation_id, ctx.correlation_id);
        assert_eq!(event.causation_id, ctx.causation_id);
        assert_eq!(event.command_causation_id, ctx.command_causation_id);
        assert_eq!(event.metadata, ctx.metadata);
        assert!(is_v7(&event.id));

        assert_eq!(command.correlation_id, ctx.correlation_id);
        assert_eq!(command.causation_id, ctx.causation_id);
        assert_eq!(command.metadata, ctx.metadata);
        assert!(is_v7(&command.id));
    }

    #[test]
    fn from_context_ignores_the_context_in_scope() {
        let ambient = context();
        let own = context();
        let (event, root) = ExecutionContext::sync_scope(ambient, || {
            (
                EventEnvelope::from_context(Some(&own), at()),
                CommandEnvelope::from_context(None, at()),
            )
        });
        assert_eq!(event.correlation_id, own.correlation_id);
        assert_eq!(event.causation_id, own.causation_id);
        assert_eq!(root.causation_id, None);
        assert!(root.metadata.is_empty());
    }

    type Parser = fn(&str) -> Result<Uuid>;

    #[test]
    fn each_id_has_its_own_error_code() {
        let parsers: [(Parser, ErrorCode); 6] = [
            (EventEnvelope::parse_id, ErrorCode::InvalidEventId),
            (
                EventEnvelope::parse_correlation_id,
                ErrorCode::InvalidCorrelationId,
            ),
            (
                EventEnvelope::parse_causation_id,
                ErrorCode::InvalidCausationId,
            ),
            (CommandEnvelope::parse_id, ErrorCode::InvalidCommandId),
            (
                CommandEnvelope::parse_correlation_id,
                ErrorCode::InvalidCommandCorrelationId,
            ),
            (
                CommandEnvelope::parse_causation_id,
                ErrorCode::InvalidCommandCausationId,
            ),
        ];
        for (parse, code) in parsers {
            let error = parse("not-a-uuid").unwrap_err();
            assert_eq!(*error.code(), code);
            assert_eq!(error.http_status(), 400);
            assert_eq!(error.context()["value"], "not-a-uuid");

            assert_eq!(parse(V4).unwrap().to_string(), V4, "any version parses");
            assert_eq!(parse(&V4.to_uppercase()).unwrap().to_string(), V4);
        }
    }
}
