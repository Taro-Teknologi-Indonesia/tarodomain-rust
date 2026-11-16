//! The dead-letter store contract (spec §21.1).

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::async_trait;
use crate::error::Result;
use crate::kernel::ids::Uuid;

/// An event a handler gave up on, to quarantine (spec §21.1).
#[derive(Clone, Debug, PartialEq)]
pub struct DeadLetterInput {
    /// The handler that failed, such as a projection's name.
    pub handler_name: String,
    /// The event's ID.
    pub event_id: Uuid,
    /// The event's name.
    pub event_type: String,
    /// The event as it was delivered: normally a
    /// [`SerializedEvent`](crate::domain::SerializedEvent) as JSON, or the
    /// raw message body when it couldn't be read as one.
    pub payload: Value,
    /// The last error's code, such as `"PROJECTION_HANDLER_FAILED"`.
    pub error_code: String,
    /// The last error's message.
    pub error_message: String,
    /// How many times the handler ran the event.
    pub attempts: u32,
}

/// A quarantined event (spec §21.1): the [`DeadLetterInput`] fields plus the
/// ID the store gave it and when it was written.
#[derive(Clone, Debug, PartialEq)]
pub struct DeadLetterEntry {
    /// The store's ID for the entry, increasing in write order.
    pub id: u64,
    /// The handler that failed.
    pub handler_name: String,
    /// The event's ID.
    pub event_id: Uuid,
    /// The event's name.
    pub event_type: String,
    /// The event as it was delivered.
    pub payload: Value,
    /// The last error's code.
    pub error_code: String,
    /// The last error's message.
    pub error_message: String,
    /// How many times the handler ran the event.
    pub attempts: u32,
    /// When the entry was written, by the store's clock.
    pub created_at: DateTime<Utc>,
}

impl DeadLetterEntry {
    /// The stored form of `input`, with the ID and time the store gave it.
    pub fn new(id: u64, input: DeadLetterInput, created_at: DateTime<Utc>) -> Self {
        Self {
            id,
            handler_name: input.handler_name,
            event_id: input.event_id,
            event_type: input.event_type,
            payload: input.payload,
            error_code: input.error_code,
            error_message: input.error_message,
            attempts: input.attempts,
            created_at,
        }
    }
}

/// The dead-letter store's storage (spec §21.1): events that handlers gave
/// up on, kept for inspection and replay.
///
/// Every [`write`](Self::write) adds an entry with a new ID, even for an
/// event the handler already dead-lettered; IDs increase in write order and
/// are never reused. Deleting an entry that doesn't exist does nothing.
///
/// The 30-day retention (§21.1) belongs to the framework's cleanup job,
/// which calls [`delete_old`](Self::delete_old). The store doesn't expire
/// entries on its own.
#[async_trait]
pub trait DeadLetterStore: Send + Sync {
    /// Adds an entry and returns its ID.
    async fn write(&self, input: DeadLetterInput) -> Result<u64>;

    /// The handler's entries in write order (by ID), at most `limit` of them.
    async fn list(&self, handler_name: &str, limit: Option<usize>) -> Result<Vec<DeadLetterEntry>>;

    /// The handler's latest entry for the event, if any.
    async fn get(&self, event_id: Uuid, handler_name: &str) -> Result<Option<DeadLetterEntry>>;

    /// The entry with this ID, if any.
    async fn get_by_id(&self, id: u64) -> Result<Option<DeadLetterEntry>>;

    /// Deletes the entry with this ID.
    async fn delete(&self, id: u64) -> Result<()>;

    /// How many entries there are, for one handler if given.
    async fn count(&self, handler_name: Option<&str>) -> Result<u64>;

    /// Deletes the entries written before `older_than`, and returns how many.
    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::kernel::ids::new_v7;

    #[test]
    fn an_entry_keeps_the_input_fields() {
        let input = DeadLetterInput {
            handler_name: "balances".into(),
            event_id: new_v7(),
            event_type: "MoneyDeposited".into(),
            payload: json!({"data": {"cents": 5}}),
            error_code: "PROJECTION_HANDLER_FAILED".into(),
            error_message: "boom".into(),
            attempts: 4,
        };
        let entry = DeadLetterEntry::new(7, input.clone(), DateTime::UNIX_EPOCH);
        assert_eq!(entry.id, 7);
        assert_eq!(entry.handler_name, input.handler_name);
        assert_eq!(entry.event_id, input.event_id);
        assert_eq!(entry.event_type, input.event_type);
        assert_eq!(entry.payload, input.payload);
        assert_eq!(entry.error_code, input.error_code);
        assert_eq!(entry.error_message, input.error_message);
        assert_eq!(entry.attempts, 4);
        assert_eq!(entry.created_at, DateTime::UNIX_EPOCH);
    }

    #[test]
    fn the_trait_is_dyn_compatible() {
        fn accepts(_: Option<Arc<dyn DeadLetterStore>>) {}
        accepts(None);
    }
}
