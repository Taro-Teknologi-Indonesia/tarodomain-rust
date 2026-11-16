//! The domain layer (spec §2, §5): envelopes, metadata, the execution
//! context, value objects, aggregate IDs, events, event families, commands
//! and the aggregate root.

mod aggregate;
mod aggregate_id;
mod command;
mod envelope;
mod event;
mod execution_context;
mod family;
mod metadata;
mod serialized;
mod value_object;

pub use aggregate::{Aggregate, AggregateCore, SnapshotData};
pub use aggregate_id::AggregateId;
pub use command::{AnyCommand, Command, CommandMessage, SerializedCommand};
pub use envelope::{CommandEnvelope, EventEnvelope};
pub use event::{DomainEvent, Event, Recorded};
pub use execution_context::ExecutionContext;
pub use family::EventFamily;
pub use metadata::{Metadata, MetadataValue};
pub use serialized::{SerializedEvent, format_timestamp, parse_timestamp};
pub use value_object::ValueObject;
