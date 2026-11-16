//! Commands (spec §2.2): the `Command` trait for business payloads, the typed
//! `CommandMessage<C>`, the type-erased `AnyCommand` that dispatchers and
//! sagas pass around, and the storable `SerializedCommand`.

use std::any::Any;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{CommandEnvelope, Metadata};
use crate::error::{FrameworkError, Result};
use crate::kernel::clock;
use crate::kernel::ids::{Uuid, derived_v7};

/// The business fields of a command: an intent to change the system, which
/// can be rejected (spec §2.2).
///
/// As for events, the name is declared, not taken from the type, so renaming
/// the struct doesn't break clients or stored commands.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use tarodomain::domain::{Command, CommandMessage};
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct Deposit {
///     cents: i64,
/// }
///
/// impl Command for Deposit {
///     const NAME: &'static str = "Deposit";
/// }
///
/// let message = CommandMessage::from(Deposit { cents: 250 });
/// assert_eq!(message.cents, 250);
/// assert_eq!(message.envelope.causation_id, None, "a root command");
/// ```
pub trait Command: Serialize + DeserializeOwned + fmt::Debug + Send + Sync + 'static {
    /// The command type name, stored as `command_type` and used to find the
    /// handler (§4.3).
    const NAME: &'static str;
}

/// A typed command with its envelope.
///
/// [`new`](Self::new), and `From<C>`, fill in the envelope: a new UUIDv7 ID,
/// the tracing IDs and metadata of the execution context in scope, and the
/// time from the clock in scope ([`clock::now`]). Builder methods override
/// the parts a client supplies. Derefs to the command.
#[derive(Clone, Debug)]
pub struct CommandMessage<C> {
    /// ID, tracing IDs, metadata and timestamp.
    pub envelope: CommandEnvelope,
    /// The business fields.
    pub command: C,
}

impl<C: Command> CommandMessage<C> {
    /// Wraps `command` with an envelope from the context in scope.
    pub fn new(command: C) -> Self {
        Self {
            envelope: CommandEnvelope::new(clock::now()),
            command,
        }
    }

    /// Sets the ID, such as one the client supplied for idempotency (§14.1).
    #[must_use]
    pub fn with_id(mut self, id: Uuid) -> Self {
        self.envelope.id = id;
        self
    }

    /// Sets the correlation ID, such as an `x-request-id` header.
    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: Uuid) -> Self {
        self.envelope.correlation_id = correlation_id;
        self
    }

    /// Sets the causation ID.
    #[must_use]
    pub fn with_causation_id(mut self, causation_id: Option<Uuid>) -> Self {
        self.envelope.causation_id = causation_id;
        self
    }

    /// Replaces the metadata.
    #[must_use]
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.envelope.metadata = metadata;
        self
    }
}

impl<C: Command> From<C> for CommandMessage<C> {
    fn from(command: C) -> Self {
        Self::new(command)
    }
}

impl<C> Deref for CommandMessage<C> {
    type Target = C;

    fn deref(&self) -> &C {
        &self.command
    }
}

/// What an [`AnyCommand`] holds: any [`Command`], behind a trait object that
/// can still serialize it.
trait Payload: fmt::Debug + Send + Sync + 'static {
    fn as_any(&self) -> &(dyn Any + Send + Sync);
    fn to_json(&self) -> serde_json::Result<Value>;
}

impl<C: Command> Payload for C {
    fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self
    }

    fn to_json(&self) -> serde_json::Result<Value> {
        serde_json::to_value(self)
    }
}

/// A command of any type with its envelope: what sagas return and the
/// dispatcher accepts.
///
/// Any [`Command`] or [`CommandMessage`] converts into one with `.into()`.
/// Cloning is cheap, since clones share the payload.
#[derive(Clone)]
pub struct AnyCommand {
    envelope: CommandEnvelope,
    name: &'static str,
    payload: Arc<dyn Payload>,
}

impl AnyCommand {
    /// Wraps `command` with `envelope`.
    pub fn new<C: Command>(envelope: CommandEnvelope, command: C) -> Self {
        Self {
            envelope,
            name: C::NAME,
            payload: Arc::new(command),
        }
    }

    /// The envelope: ID, tracing IDs, metadata and timestamp.
    pub fn envelope(&self) -> &CommandEnvelope {
        &self.envelope
    }

    /// The command ID.
    pub fn id(&self) -> Uuid {
        self.envelope.id
    }

    /// The command type name ([`Command::NAME`]).
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The payload, for downcasting.
    pub fn as_any(&self) -> &(dyn Any + Send + Sync) {
        self.payload.as_any()
    }

    /// Whether the payload is a `C`.
    pub fn is<C: Command>(&self) -> bool {
        self.as_any().is::<C>()
    }

    /// The payload, if it is a `C`.
    pub fn downcast_ref<C: Command>(&self) -> Option<&C> {
        self.as_any().downcast_ref()
    }

    /// The same command with a deterministic UUIDv7 ID (see
    /// [`derived_v7`]): the timestamp of `at`, and the other bits from
    /// `seed` and `index`.
    ///
    /// A saga gives each command it returns such an ID, derived from the
    /// event it handles, so a redelivered event recreates the same command
    /// IDs and command idempotency (§14.1) skips them.
    #[must_use]
    pub fn with_derived_id(
        mut self,
        seed: impl AsRef<[u8]>,
        index: u32,
        at: DateTime<Utc>,
    ) -> Self {
        let unix_ms = u64::try_from(at.timestamp_millis()).unwrap_or(0);
        self.envelope.id = derived_v7(seed, index, unix_ms);
        self
    }

    /// The storable form.
    ///
    /// Fails with `SERIALIZATION_ERROR` if the payload can't be represented as
    /// JSON.
    pub fn to_serialized(&self) -> Result<SerializedCommand> {
        let data = self.payload.to_json().map_err(|error| {
            FrameworkError::serialization_error(format!(
                "command {} can't be serialized: {error}",
                self.name
            ))
            .with_context("commandType", self.name)
            .with_context("commandId", self.envelope.id.to_string())
            .with_source(error)
        })?;
        Ok(SerializedCommand::new(&self.envelope, self.name, data))
    }
}

impl fmt::Debug for AnyCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AnyCommand")
            .field("name", &self.name)
            .field("envelope", &self.envelope)
            .field("command", &self.payload)
            .finish()
    }
}

impl<C: Command> From<CommandMessage<C>> for AnyCommand {
    fn from(message: CommandMessage<C>) -> Self {
        Self::new(message.envelope, message.command)
    }
}

impl<C: Command> From<C> for AnyCommand {
    /// Wraps `command` with an envelope from the context in scope, as
    /// [`CommandMessage::new`] does.
    fn from(command: C) -> Self {
        CommandMessage::new(command).into()
    }
}

/// A command in storable form (spec §2.2): the fields of §2.2, with
/// `timestamp` written as ISO 8601 with milliseconds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedCommand {
    /// The command ID, also the idempotency key.
    pub command_id: Uuid,
    /// The command's name ([`Command::NAME`]).
    pub command_type: String,
    /// ID of the event or command that caused the command, or `None` at the
    /// root.
    pub causation_id: Option<Uuid>,
    /// End-to-end trace ID.
    pub correlation_id: Uuid,
    /// The business fields.
    pub data: Value,
    /// Free-form metadata.
    pub metadata: Metadata,
    /// When the command was created.
    #[serde(with = "super::serialized::iso_millis")]
    pub timestamp: DateTime<Utc>,
}

impl SerializedCommand {
    /// A serialized command from its envelope, name and business fields.
    pub fn new(envelope: &CommandEnvelope, command_type: impl Into<String>, data: Value) -> Self {
        Self {
            command_id: envelope.id,
            command_type: command_type.into(),
            causation_id: envelope.causation_id,
            correlation_id: envelope.correlation_id,
            data,
            metadata: envelope.metadata.clone(),
            timestamp: envelope.timestamp,
        }
    }

    /// The envelope fields.
    pub fn envelope(&self) -> CommandEnvelope {
        CommandEnvelope {
            id: self.command_id,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
            metadata: self.metadata.clone(),
            timestamp: self.timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::domain::ExecutionContext;
    use crate::kernel::clock::ManualClock;
    use crate::kernel::ids::{is_v7, new_v7};

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Deposit {
        cents: i64,
    }

    impl Command for Deposit {
        const NAME: &'static str = "Deposit";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Withdraw {
        cents: i64,
    }

    impl Command for Withdraw {
        const NAME: &'static str = "Withdraw";
    }

    fn at(millis: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(millis).unwrap()
    }

    #[test]
    fn a_root_command_outside_any_context() {
        let message = CommandMessage::from(Deposit { cents: 5 });
        assert!(is_v7(&message.envelope.id));
        assert_ne!(message.envelope.id, message.envelope.correlation_id);
        assert_eq!(message.envelope.causation_id, None);
        assert!(message.envelope.metadata.is_empty());
        assert_eq!(message.cents, 5, "derefs to the command");
    }

    #[tokio::test]
    async fn defaults_come_from_the_context_and_clock_in_scope() {
        let context = ExecutionContext {
            causation_id: Some(new_v7()),
            metadata: Metadata::from([("tenant", "acme")]),
            ..ExecutionContext::new(new_v7())
        };
        let clock = Arc::new(ManualClock::new(at(1_763_280_000_123)));
        let work = context
            .clone()
            .scope(async { CommandMessage::from(Deposit { cents: 5 }) });
        let message = clock::scope(clock, work).await;

        assert_eq!(message.envelope.correlation_id, context.correlation_id);
        assert_eq!(message.envelope.causation_id, context.causation_id);
        assert_eq!(message.envelope.metadata, context.metadata);
        assert_eq!(message.envelope.timestamp, at(1_763_280_000_123));
    }

    #[test]
    fn builder_methods_override_the_envelope() {
        let (id, correlation_id, causation_id) = (new_v7(), new_v7(), new_v7());
        let message = CommandMessage::from(Deposit { cents: 5 })
            .with_id(id)
            .with_correlation_id(correlation_id)
            .with_causation_id(Some(causation_id))
            .with_metadata(Metadata::from([("userId", "u-1")]));
        assert_eq!(message.envelope.id, id);
        assert_eq!(message.envelope.correlation_id, correlation_id);
        assert_eq!(message.envelope.causation_id, Some(causation_id));
        assert_eq!(message.envelope.metadata.len(), 1);
    }

    #[test]
    fn any_command_downcasts() {
        let message = CommandMessage::from(Deposit { cents: 5 });
        let envelope = message.envelope.clone();
        let command = AnyCommand::from(message);
        assert_eq!(command.name(), "Deposit");
        assert_eq!(
            command.envelope(),
            &envelope,
            "keeps the message's envelope"
        );
        assert_eq!(command.id(), envelope.id);
        assert!(command.is::<Deposit>());
        assert_eq!(
            command.downcast_ref::<Deposit>(),
            Some(&Deposit { cents: 5 })
        );
        assert!(!command.is::<Withdraw>());
        assert!(command.downcast_ref::<Withdraw>().is_none());

        let direct: AnyCommand = Withdraw { cents: 1 }.into();
        assert_eq!(direct.name(), "Withdraw");
    }

    #[test]
    fn serialized_shape() {
        let envelope = CommandEnvelope::from_context(None, at(1_763_280_000_206));
        let command = AnyCommand::new(envelope.clone(), Deposit { cents: 250 });
        let serialized = command.to_serialized().unwrap();
        let json = serde_json::to_value(&serialized).unwrap();
        assert_eq!(
            json,
            json!({
                "command_id": envelope.id.to_string(),
                "command_type": "Deposit",
                "causation_id": null,
                "correlation_id": envelope.correlation_id.to_string(),
                "data": {"cents": 250},
                "metadata": {},
                "timestamp": "2025-11-16T08:00:00.206Z",
            })
        );
        let back: SerializedCommand = serde_json::from_value(json).unwrap();
        assert_eq!(back, serialized);
        assert_eq!(back.envelope(), envelope);
    }

    #[test]
    fn derived_ids_are_stable() {
        let event_at = at(1_763_280_000_123);
        let derive = |index| {
            AnyCommand::from(Deposit { cents: 5 })
                .with_derived_id(
                    "TransferSaga/0190f0a2-7b3c-7d4e-8f00-123456789abc",
                    index,
                    event_at,
                )
                .id()
        };
        assert_eq!(derive(0), derive(0));
        assert_ne!(derive(0), derive(1));
        assert!(is_v7(&derive(0)));
        let unix_ms = derive(0).as_u128() >> 80;
        assert_eq!(unix_ms, 1_763_280_000_123, "the event's time");
    }
}
