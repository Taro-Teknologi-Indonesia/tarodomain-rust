//! Events (spec §2.1): the `Event` trait for business payloads, the
//! type-erased `DomainEvent` that buses and handlers see, and the typed
//! `Recorded<E>` view.

use std::any::Any;
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{EventEnvelope, SerializedEvent};
use crate::error::{FrameworkError, Result};
use crate::kernel::ids::Uuid;

/// The business fields of an event: something that happened in the domain
/// (spec §2.1).
///
/// The name is declared, not taken from the type, so renaming the struct
/// never orphans the events already stored under the old name.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use tarodomain::domain::Event;
///
/// #[derive(Debug, Serialize, Deserialize)]
/// struct MoneyDeposited {
///     cents: i64,
/// }
///
/// impl Event for MoneyDeposited {
///     const NAME: &'static str = "MoneyDeposited";
/// }
/// ```
pub trait Event: Serialize + DeserializeOwned + fmt::Debug + Send + Sync + 'static {
    /// The event type name, stored as `event_type`. Must not change once
    /// events are stored.
    const NAME: &'static str;

    /// The schema version, stored as `event_version` and read by upcasters
    /// (§3.4). Semver.
    const VERSION: &'static str = "1.0.0";
}

/// What a [`DomainEvent`] holds: any [`Event`], behind a trait object that can
/// still serialize it.
trait Payload: fmt::Debug + Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync>;
    fn to_json(&self) -> serde_json::Result<Value>;
}

impl<E: Event> Payload for E {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn into_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync> {
        self
    }

    fn to_json(&self) -> serde_json::Result<Value> {
        serde_json::to_value(self)
    }
}

/// An event of any type with its envelope: what event buses, handlers and the
/// event store pass around.
///
/// Immutable: nothing hands out `&mut` to its parts. Cloning is cheap, since
/// clones share the payload. [`downcast`](Self::downcast) gives the typed
/// view, [`Recorded<E>`].
#[derive(Clone)]
pub struct DomainEvent {
    envelope: EventEnvelope,
    name: &'static str,
    version: &'static str,
    payload: Arc<dyn Payload>,
}

impl DomainEvent {
    /// Wraps `event` with `envelope`.
    pub fn new<E: Event>(envelope: EventEnvelope, event: E) -> Self {
        Self::from_arc(envelope, Arc::new(event))
    }

    fn from_arc<E: Event>(envelope: EventEnvelope, event: Arc<E>) -> Self {
        Self {
            envelope,
            name: E::NAME,
            version: E::VERSION,
            payload: event,
        }
    }

    /// The envelope: ID, tracing IDs, metadata and timestamp.
    pub fn envelope(&self) -> &EventEnvelope {
        &self.envelope
    }

    /// The event ID.
    pub fn id(&self) -> Uuid {
        self.envelope.id
    }

    /// The event type name ([`Event::NAME`]).
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The schema version ([`Event::VERSION`]).
    pub fn version(&self) -> &'static str {
        self.version
    }

    /// Whether the payload is an `E`.
    pub fn is<E: Event>(&self) -> bool {
        self.payload.as_any().is::<E>()
    }

    /// The payload, if it is an `E`.
    pub fn downcast_ref<E: Event>(&self) -> Option<&E> {
        self.payload.as_any().downcast_ref()
    }

    /// The typed view, if the payload is an `E`. It shares the payload.
    pub fn downcast<E: Event>(&self) -> Option<Recorded<E>> {
        let event = Arc::clone(&self.payload).into_any().downcast::<E>().ok()?;
        Some(Recorded {
            envelope: self.envelope.clone(),
            event,
        })
    }

    /// The storable form (§3.3: `event.serialize()`).
    ///
    /// Fails with `SERIALIZATION_ERROR` if the payload can't be represented as
    /// JSON, such as a map with non-string keys.
    pub fn serialize(&self) -> Result<SerializedEvent> {
        let data = self.payload.to_json().map_err(|error| {
            FrameworkError::serialization_error(format!(
                "event {} can't be serialized: {error}",
                self.name
            ))
            .with_context("eventType", self.name)
            .with_context("eventId", self.envelope.id.to_string())
            .with_source(error)
        })?;
        Ok(SerializedEvent::new(
            &self.envelope,
            self.name,
            self.version,
            data,
        ))
    }
}

impl fmt::Debug for DomainEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DomainEvent")
            .field("name", &self.name)
            .field("version", &self.version)
            .field("envelope", &self.envelope)
            .field("event", &self.payload)
            .finish()
    }
}

/// A typed event with its envelope: what [`DomainEvent::downcast`] returns,
/// and what typed handlers receive.
///
/// Derefs to the event, so `deposited.cents` reads a field directly.
#[derive(Debug)]
pub struct Recorded<E> {
    /// The envelope: ID, tracing IDs, metadata and timestamp.
    pub envelope: EventEnvelope,
    /// The event, shared with the [`DomainEvent`] it came from.
    pub event: Arc<E>,
}

impl<E> Clone for Recorded<E> {
    fn clone(&self) -> Self {
        Self {
            envelope: self.envelope.clone(),
            event: Arc::clone(&self.event),
        }
    }
}

impl<E> Deref for Recorded<E> {
    type Target = E;

    fn deref(&self) -> &E {
        &self.event
    }
}

impl<E: Event> From<Recorded<E>> for DomainEvent {
    fn from(recorded: Recorded<E>) -> Self {
        Self::from_arc(recorded.envelope, recorded.event)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::DateTime;
    use serde::{Deserialize, Serializer};
    use serde_json::json;

    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct MoneyDeposited {
        cents: i64,
    }

    impl Event for MoneyDeposited {
        const NAME: &'static str = "MoneyDeposited";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct AccountClosed {}

    impl Event for AccountClosed {
        const NAME: &'static str = "AccountClosed";
        const VERSION: &'static str = "2.1.0";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Unserializable {
        #[serde(serialize_with = "fail")]
        value: BTreeMap<String, i64>,
    }

    fn fail<S: Serializer>(_: &BTreeMap<String, i64>, _: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("no JSON form"))
    }

    impl Event for Unserializable {
        const NAME: &'static str = "Unserializable";
    }

    fn deposited(cents: i64) -> DomainEvent {
        let at = DateTime::from_timestamp_millis(1_763_280_000_123).unwrap();
        DomainEvent::new(
            EventEnvelope::from_context(None, at),
            MoneyDeposited { cents },
        )
    }

    #[test]
    fn name_and_version_come_from_the_type() {
        let event = deposited(5);
        assert_eq!((event.name(), event.version()), ("MoneyDeposited", "1.0.0"));
        let closed = DomainEvent::new(event.envelope().clone(), AccountClosed {});
        assert_eq!(
            (closed.name(), closed.version()),
            ("AccountClosed", "2.1.0")
        );
        assert_eq!(event.id(), event.envelope().id);
    }

    #[test]
    fn downcast_to_the_right_type() {
        let event = deposited(5);
        assert!(event.is::<MoneyDeposited>());
        assert_eq!(
            event.downcast_ref::<MoneyDeposited>(),
            Some(&MoneyDeposited { cents: 5 })
        );

        let recorded = event.downcast::<MoneyDeposited>().unwrap();
        assert_eq!(recorded.cents, 5, "derefs to the event");
        assert_eq!(&recorded.envelope, event.envelope());
        assert_eq!(Arc::strong_count(&recorded.event), 2, "shares the payload");
    }

    #[test]
    fn downcast_to_the_wrong_type_is_none() {
        let event = deposited(5);
        assert!(!event.is::<AccountClosed>());
        assert!(event.downcast_ref::<AccountClosed>().is_none());
        assert!(event.downcast::<AccountClosed>().is_none());
    }

    #[test]
    fn recorded_converts_back() {
        let recorded = deposited(5).downcast::<MoneyDeposited>().unwrap();
        let event = DomainEvent::from(recorded.clone());
        assert_eq!(event.name(), "MoneyDeposited");
        assert_eq!(event.envelope(), &recorded.envelope);
        assert_eq!(event.downcast_ref::<MoneyDeposited>().unwrap().cents, 5);
    }

    #[test]
    fn serializes_to_the_storage_form() {
        let event = deposited(250);
        let serialized = event.serialize().unwrap();
        assert_eq!(serialized.event_type, "MoneyDeposited");
        assert_eq!(serialized.event_version, "1.0.0");
        assert_eq!(serialized.data, json!({"cents": 250}));
        assert_eq!(serialized.envelope(), *event.envelope());

        let json = serde_json::to_value(&serialized).unwrap();
        assert_eq!(json["occurred_at"], "2025-11-16T08:00:00.123Z");
    }

    #[test]
    fn a_payload_without_json_form_is_a_serialization_error() {
        let at = DateTime::from_timestamp_millis(0).unwrap();
        let event = DomainEvent::new(
            EventEnvelope::from_context(None, at),
            Unserializable {
                value: BTreeMap::new(),
            },
        );
        let error = event.serialize().unwrap_err();
        assert_eq!(error.code().as_str(), "SERIALIZATION_ERROR");
        assert_eq!(error.context()["eventType"], "Unserializable");
        assert!(error.retryable());
    }

    #[test]
    fn debug_shows_the_payload() {
        let text = format!("{:?}", deposited(5));
        assert!(text.contains("MoneyDeposited { cents: 5 }"), "{text}");
    }
}
