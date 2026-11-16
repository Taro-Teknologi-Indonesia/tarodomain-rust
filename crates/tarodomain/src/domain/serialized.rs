//! The storable form of events (spec §2.1), and the timestamp format stores
//! use.

use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{EventEnvelope, Metadata};
use crate::error::{FrameworkError, Result};
use crate::kernel::ids::Uuid;

/// An event in storable form (spec §2.1): its type, schema version, envelope
/// fields and business fields as JSON.
///
/// Adapters store and return this type, and the event store converts between
/// it and [`DomainEvent`](super::DomainEvent). The fields are the storage
/// columns of §2.1, plus `command_causation_id`, which is omitted from JSON
/// when `None`. `occurred_at` is written as ISO 8601 with milliseconds, such
/// as `2026-11-16T05:58:30.206Z`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SerializedEvent {
    /// The event ID, the primary key.
    pub event_id: Uuid,
    /// The event's name ([`Event::NAME`](super::Event::NAME)).
    pub event_type: String,
    /// The schema version `data` was written with
    /// ([`Event::VERSION`](super::Event::VERSION)), which upcasters read.
    pub event_version: String,
    /// ID of the command or event that caused the event, or `None` at the
    /// root.
    pub causation_id: Option<Uuid>,
    /// End-to-end trace ID.
    pub correlation_id: Uuid,
    /// The causation ID of the command that raised the event, if any (see
    /// [`EventEnvelope::command_causation_id`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub command_causation_id: Option<Uuid>,
    /// The business fields.
    pub data: Value,
    /// Free-form metadata.
    pub metadata: Metadata,
    /// When the event was raised.
    #[serde(with = "iso_millis")]
    pub occurred_at: DateTime<Utc>,
}

impl SerializedEvent {
    /// A serialized event from its envelope, name, schema version and
    /// business fields.
    pub fn new(
        envelope: &EventEnvelope,
        event_type: impl Into<String>,
        event_version: impl Into<String>,
        data: Value,
    ) -> Self {
        Self {
            event_id: envelope.id,
            event_type: event_type.into(),
            event_version: event_version.into(),
            causation_id: envelope.causation_id,
            correlation_id: envelope.correlation_id,
            command_causation_id: envelope.command_causation_id,
            data,
            metadata: envelope.metadata.clone(),
            occurred_at: envelope.occurred_at,
        }
    }

    /// The envelope fields.
    pub fn envelope(&self) -> EventEnvelope {
        EventEnvelope {
            id: self.event_id,
            correlation_id: self.correlation_id,
            causation_id: self.causation_id,
            command_causation_id: self.command_causation_id,
            metadata: self.metadata.clone(),
            occurred_at: self.occurred_at,
        }
    }
}

/// Formats a timestamp the way stores keep it: ISO 8601 in UTC with exactly
/// three fractional digits, such as `2026-11-16T05:58:30.206Z`.
///
/// Strings in this form sort in time order.
pub fn format_timestamp(at: DateTime<Utc>) -> String {
    at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

/// Parses an ISO 8601 (RFC 3339) timestamp with any offset and precision,
/// such as one [`format_timestamp`] wrote.
///
/// Fails with `SERIALIZATION_ERROR`.
pub fn parse_timestamp(text: &str) -> Result<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(text)
        .map(|at| at.with_timezone(&Utc))
        .map_err(|error| {
            FrameworkError::serialization_error(format!("not an ISO 8601 timestamp: {text:?}"))
                .with_context("value", text)
                .with_source(error)
        })
}

/// Serde support for timestamps in the [`format_timestamp`] form, for
/// `#[serde(with = "…")]`.
pub(crate) mod iso_millis {
    use chrono::{DateTime, Utc};
    use serde::de::Error as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(crate) fn serialize<S: Serializer>(
        at: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&super::format_timestamp(*at))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error> {
        let text = String::deserialize(deserializer)?;
        super::parse_timestamp(&text).map_err(|error| D::Error::custom(error.message()))
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::kernel::ids::new_v7;

    fn envelope() -> EventEnvelope {
        EventEnvelope {
            id: new_v7(),
            correlation_id: new_v7(),
            causation_id: None,
            command_causation_id: None,
            metadata: Metadata::from([("userId", "u-1")]),
            occurred_at: DateTime::from_timestamp_millis(1_763_280_000_206).unwrap(),
        }
    }

    #[test]
    fn json_uses_the_storage_column_names() {
        let envelope = envelope();
        let serialized =
            SerializedEvent::new(&envelope, "MoneyDeposited", "1.0.0", json!({"cents": 5}));
        let json = serde_json::to_value(&serialized).unwrap();
        assert_eq!(
            json,
            json!({
                "event_id": envelope.id.to_string(),
                "event_type": "MoneyDeposited",
                "event_version": "1.0.0",
                "causation_id": null,
                "correlation_id": envelope.correlation_id.to_string(),
                "data": {"cents": 5},
                "metadata": {"userId": "u-1"},
                "occurred_at": "2025-11-16T08:00:00.206Z",
            })
        );
        assert_eq!(
            serde_json::from_value::<SerializedEvent>(json).unwrap(),
            serialized
        );
    }

    #[test]
    fn command_causation_id_appears_only_when_set() {
        let caused = EventEnvelope {
            causation_id: Some(new_v7()),
            command_causation_id: Some(new_v7()),
            ..envelope()
        };
        let serialized = SerializedEvent::new(&caused, "E", "1.0.0", json!({}));
        let json = serde_json::to_value(&serialized).unwrap();
        assert_eq!(
            json["command_causation_id"],
            caused.command_causation_id.unwrap().to_string()
        );
        assert_eq!(
            serde_json::from_value::<SerializedEvent>(json).unwrap(),
            serialized
        );
        assert_eq!(serialized.envelope(), caused);
    }

    #[test]
    fn timestamps_have_exactly_three_fractional_digits() {
        let whole = DateTime::from_timestamp(1_763_280_000, 0).unwrap();
        assert_eq!(format_timestamp(whole), "2025-11-16T08:00:00.000Z");
        let fine = DateTime::from_timestamp(1_763_280_000, 123_456_789).unwrap();
        assert_eq!(format_timestamp(fine), "2025-11-16T08:00:00.123Z");
    }

    #[test]
    fn timestamps_parse_with_any_offset_and_precision() {
        let at = parse_timestamp("2025-11-16T15:00:00.5+07:00").unwrap();
        assert_eq!(format_timestamp(at), "2025-11-16T08:00:00.500Z");

        let error = parse_timestamp("16/11/2025").unwrap_err();
        assert_eq!(error.code().as_str(), "SERIALIZATION_ERROR");
        assert_eq!(error.context()["value"], "16/11/2025");
    }
}
