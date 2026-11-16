//! Fixed IDs, times and events for the contract suites, so a failing case
//! reads the same on every run and on every provider.

use chrono::{DateTime, Utc};
use serde_json::json;
use tarodomain::adapters::StreamIds;
use tarodomain::domain::{AggregateId, Metadata, MetadataValue, SerializedEvent};
use tarodomain::kernel::ids::{Uuid, derived_v7};

/// The fixtures' epoch, 2026-11-16T00:00:00.000Z, in Unix milliseconds.
pub const EPOCH_MS: u64 = 1_794_787_200_000;

/// The fixtures' epoch plus `offset_ms` milliseconds.
pub fn time(offset_ms: u64) -> DateTime<Utc> {
    let millis = i64::try_from(EPOCH_MS + offset_ms).expect("a fixture time fits in i64");
    DateTime::from_timestamp_millis(millis).expect("a fixture time is in range")
}

/// Event ID `n`: a UUIDv7 from `n` milliseconds after the epoch, so a higher
/// `n` sorts later.
pub fn event_id(n: u32) -> Uuid {
    derived_v7("tarodomain-testing/event", n, EPOCH_MS + u64::from(n))
}

/// Two event IDs from the same millisecond where the first sorts *after* the
/// second. Appended in this order, append order and ID order disagree.
pub fn same_millisecond_ids() -> (Uuid, Uuid) {
    let a = derived_v7("tarodomain-testing/same-ms", 0, EPOCH_MS);
    let b = derived_v7("tarodomain-testing/same-ms", 1, EPOCH_MS);
    if a > b { (a, b) } else { (b, a) }
}

/// Any other ID the suites need, such as a correlation or command ID: a
/// UUIDv7 derived from `seed` and `n`.
pub fn uuid(seed: &str, n: u32) -> Uuid {
    derived_v7(format!("tarodomain-testing/{seed}"), n, EPOCH_MS)
}

/// Aggregate ID `n`.
pub fn aggregate_id(n: u32) -> Uuid {
    uuid("aggregate", n)
}

/// The stream of aggregate `n` of `aggregate_type`, such as
/// `"BankAccount-0199…"`.
pub fn stream_id(aggregate_type: &str, n: u32) -> String {
    StreamIds::for_aggregate(aggregate_type, AggregateId::from_uuid(aggregate_id(n)))
}

/// A `MoneyDeposited` event with this ID, its own correlation ID.
///
/// Every optional field is set (causation IDs, metadata of each primitive
/// kind, nested data), so a round trip through a store checks them all.
pub fn event(event_id: Uuid) -> SerializedEvent {
    let metadata = Metadata::new()
        .with("userId", "user-1")
        .with("attempt", 2)
        .with("replayed", false)
        .with("ratio", 0.5)
        .with("tenant", MetadataValue::Null);
    SerializedEvent {
        event_id,
        event_type: "MoneyDeposited".into(),
        event_version: "1.0.0".into(),
        causation_id: Some(uuid("causation", 0)),
        correlation_id: event_id,
        command_causation_id: Some(uuid("command-causation", 0)),
        data: json!({ "amount": 125, "currency": "IDR", "tags": ["a", "b"], "note": null }),
        metadata,
        occurred_at: time(250),
    }
}

/// [`event`] for each ID, in order.
pub fn events(ids: impl IntoIterator<Item = Uuid>) -> Vec<SerializedEvent> {
    ids.into_iter().map(event).collect()
}

/// The IDs of `events`, in order, to compare what a read returned.
pub fn ids(events: &[SerializedEvent]) -> Vec<Uuid> {
    events.iter().map(|event| event.event_id).collect()
}

#[cfg(test)]
mod tests {
    use tarodomain::kernel::ids::is_v7;

    use super::*;

    #[test]
    fn the_epoch_is_the_documented_date() {
        assert_eq!(time(0).to_rfc3339(), "2026-11-16T00:00:00+00:00");
    }

    #[test]
    fn event_ids_are_v7_and_increase() {
        assert!(is_v7(&event_id(1)));
        assert!(event_id(1) < event_id(2));
        assert_eq!(event_id(7), event_id(7));
    }

    #[test]
    fn same_millisecond_ids_sort_in_reverse() {
        let (first, second) = same_millisecond_ids();
        assert!(first > second);
        let millis = |id: Uuid| id.get_timestamp().unwrap().to_unix();
        assert_eq!(millis(first), millis(second));
    }

    #[test]
    fn streams_are_aggregate_streams() {
        let stream_id = stream_id("BankAccount", 1);
        assert_eq!(StreamIds::category_of(&stream_id), Some("BankAccount"));
        assert_ne!(stream_id, self::stream_id("BankAccount", 2));
    }

    #[test]
    fn events_have_every_field_set() {
        let event = event(event_id(1));
        assert_eq!(event.correlation_id, event.event_id);
        assert!(event.causation_id.is_some() && event.command_causation_id.is_some());
        assert_eq!(event.metadata.len(), 5);
        assert_eq!(
            ids(&events([event_id(2), event_id(1)])),
            [event_id(2), event_id(1)]
        );
    }
}
