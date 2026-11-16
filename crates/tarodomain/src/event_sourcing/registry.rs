//! The event registry (spec §3.4): which Rust type reads each stored event
//! type, and the upcasters that bring old schema versions up to date on the
//! way (§3.3).

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde_json::Value;

use crate::domain::{DomainEvent, Event, EventEnvelope, SerializedEvent};
use crate::error::{FrameworkError, Result};

/// Reads business fields as one registered event type.
type ReadFn = fn(EventEnvelope, &Value) -> serde_json::Result<DomainEvent>;

/// Turns the business fields of one schema version into the next one.
type UpcastFn = Arc<dyn Fn(Value) -> Result<Value> + Send + Sync>;

#[derive(Clone, Copy)]
struct Registration {
    version: &'static str,
    /// The Rust type, for error messages only.
    type_name: &'static str,
    read: ReadFn,
}

#[derive(Clone)]
struct Upcaster {
    to: String,
    upcast: UpcastFn,
}

/// Maps stored event types to the Rust types that read them (spec §3.4), and
/// upgrades events stored at an older schema version with upcasters before
/// reading them.
///
/// Each event type is registered once, at its [`Event::VERSION`]. An event
/// stored at that version is read as is. One stored at an older version goes
/// through upcasters first, one step at a time, each turning the business
/// fields of one version into the next, until it reaches the registered
/// version (§3.4: v1.0.0 → v1.1.0 → v1.2.0).
///
/// Cloning is cheap: upcasters are shared.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use serde_json::json;
/// use tarodomain::domain::{Event, EventEnvelope, SerializedEvent};
/// use tarodomain::event_sourcing::EventRegistry;
///
/// // Version 1.0.0 stored `amount`; 1.1.0 renamed it to `cents`.
/// #[derive(Debug, Serialize, Deserialize)]
/// struct MoneyDeposited {
///     cents: i64,
/// }
///
/// impl Event for MoneyDeposited {
///     const NAME: &'static str = "MoneyDeposited";
///     const VERSION: &'static str = "1.1.0";
/// }
///
/// let mut registry = EventRegistry::new();
/// registry.register::<MoneyDeposited>()?;
/// registry.register_upcaster("MoneyDeposited", "1.0.0", "1.1.0", |data| {
///     Ok(json!({ "cents": data["amount"] }))
/// })?;
///
/// let envelope = EventEnvelope::from_context(None, chrono::Utc::now());
/// let stored = SerializedEvent::new(&envelope, "MoneyDeposited", "1.0.0", json!({"amount": 5}));
/// let event = registry.deserialize(&stored)?;
/// assert_eq!(event.downcast_ref::<MoneyDeposited>().unwrap().cents, 5);
/// assert_eq!(event.version(), "1.1.0");
/// # Ok::<(), tarodomain::error::FrameworkError>(())
/// ```
#[derive(Clone, Default)]
pub struct EventRegistry {
    events: BTreeMap<&'static str, Registration>,
    /// Event type → the version an upcaster reads → that upcaster. Each
    /// version has at most one upcaster, and they never form a loop.
    upcasters: BTreeMap<String, BTreeMap<String, Upcaster>>,
}

impl EventRegistry {
    /// An empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `E` as the type that reads events named [`Event::NAME`], at
    /// [`Event::VERSION`].
    ///
    /// A name can be registered only once: a second registration fails with
    /// `DUPLICATE_EVENT_REGISTRATION`, whether it is the same type or another
    /// type with the same name.
    pub fn register<E: Event>(&mut self) -> Result<()> {
        if let Some(existing) = self.events.get(E::NAME) {
            return Err(FrameworkError::duplicate_event_registration(format!(
                "event type {} is already registered at version {} by {}, so {} can't register it",
                E::NAME,
                existing.version,
                existing.type_name,
                std::any::type_name::<E>(),
            ))
            .with_context("eventType", E::NAME)
            .with_context("registeredVersion", existing.version)
            .with_context("version", E::VERSION));
        }
        self.events.insert(
            E::NAME,
            Registration {
                version: E::VERSION,
                type_name: std::any::type_name::<E>(),
                read: read_as::<E>,
            },
        );
        Ok(())
    }

    /// Registers an upcaster that turns the business fields of `event_type`
    /// at version `from` into those of version `to` (§3.4).
    ///
    /// Upcasters and event types can be registered in any order. Fails with
    /// `INVALID_CONFIGURATION` if `from` equals `to`, if the event type
    /// already has an upcaster from `from`, or if the new upcaster would close
    /// a loop, which would keep reading from ever finishing.
    ///
    /// When it runs, an error from `upcast` comes back from
    /// [`deserialize`](Self::deserialize) with its code unchanged and the
    /// event type, the event ID and both versions added to its context.
    pub fn register_upcaster(
        &mut self,
        event_type: impl Into<String>,
        from: impl Into<String>,
        to: impl Into<String>,
        upcast: impl Fn(Value) -> Result<Value> + Send + Sync + 'static,
    ) -> Result<()> {
        let (event_type, from, to) = (event_type.into(), from.into(), to.into());
        let invalid = |message: String| {
            FrameworkError::invalid_configuration(message)
                .with_context("eventType", event_type.as_str())
                .with_context("fromVersion", from.as_str())
                .with_context("toVersion", to.as_str())
        };
        if from == to {
            return Err(invalid(format!(
                "an upcaster for {event_type} must change the version, but goes from {from} to {to}"
            )));
        }
        if let Some(chain) = self.upcasters.get(&event_type) {
            if chain.contains_key(&from) {
                return Err(invalid(format!(
                    "{event_type} already has an upcaster from version {from}"
                )));
            }
            // The upcasters so far form no loop, so this walk ends. If it
            // reaches `from`, the new upcaster would close one.
            let mut version = to.as_str();
            while let Some(next) = chain.get(version) {
                version = &next.to;
                if version == from {
                    return Err(invalid(format!(
                        "an upcaster for {event_type} from {from} to {to} would form a loop, \
                         since upcasters already lead from {to} back to {from}"
                    )));
                }
            }
        }
        let upcaster = Upcaster {
            to,
            upcast: Arc::new(upcast),
        };
        self.upcasters
            .entry(event_type)
            .or_default()
            .insert(from, upcaster);
        Ok(())
    }

    /// Reads a stored event as its registered type (§3.4), applying
    /// upcasters first if it was stored at an older version.
    ///
    /// The event keeps its envelope, and its
    /// [`version`](DomainEvent::version) is the registered one. Fails with:
    ///
    /// - `UNKNOWN_EVENT_TYPE` if the event type isn't registered;
    /// - `EVENT_VERSION_MISMATCH` if no chain of upcasters leads from the
    ///   stored version to the registered one, including when the stored
    ///   version is newer;
    /// - `SERIALIZATION_ERROR` if the (upcast) business fields don't fit the
    ///   registered type;
    /// - the upcaster's own error if one fails.
    pub fn deserialize(&self, serialized: &SerializedEvent) -> Result<DomainEvent> {
        let event_type = serialized.event_type.as_str();
        let Some(registration) = self.events.get(event_type) else {
            return Err(FrameworkError::unknown_event_type(format!(
                "event type {event_type} isn't registered"
            ))
            .with_context("eventType", event_type)
            .with_context("eventId", serialized.event_id.to_string()));
        };
        let data = self.upcast(serialized, registration.version)?;
        (registration.read)(serialized.envelope(), &data).map_err(|error| {
            FrameworkError::serialization_error(format!(
                "event {event_type} {} can't be read as version {}: {error}",
                serialized.event_id, registration.version
            ))
            .with_context("eventType", event_type)
            .with_context("eventId", serialized.event_id.to_string())
            .with_context("eventVersion", registration.version)
            .with_source(error)
        })
    }

    /// The business fields of `serialized`, upcast to `target`. Borrowed when
    /// no upcaster runs.
    fn upcast<'a>(&self, serialized: &'a SerializedEvent, target: &str) -> Result<Cow<'a, Value>> {
        let event_type = serialized.event_type.as_str();
        let stored = serialized.event_version.as_str();
        let chain = self.upcasters.get(event_type);
        let mut data = Cow::Borrowed(&serialized.data);
        let mut version = stored;
        // Upcasters never form a loop, so this ends.
        while version != target {
            let Some(upcaster) = chain.and_then(|chain| chain.get(version)) else {
                return Err(FrameworkError::event_version_mismatch(format!(
                    "event {event_type} {} is stored at version {stored} and registered at \
                     {target}, but no upcaster reads version {version}",
                    serialized.event_id
                ))
                .with_context("eventType", event_type)
                .with_context("eventId", serialized.event_id.to_string())
                .with_context("storedVersion", stored)
                .with_context("registeredVersion", target)
                .with_context("missingFrom", version));
            };
            let upcast = (upcaster.upcast)(data.into_owned()).map_err(|error| {
                error
                    .with_context("eventType", event_type)
                    .with_context("eventId", serialized.event_id.to_string())
                    .with_context("fromVersion", version)
                    .with_context("toVersion", upcaster.to.as_str())
            })?;
            data = Cow::Owned(upcast);
            version = &upcaster.to;
        }
        Ok(data)
    }

    /// Whether `event_type` is registered at exactly `version`.
    ///
    /// An older version that upcasters can bring up to date doesn't count:
    /// this answers whether an event of that version may be stored, not
    /// whether a stored one can be read.
    pub fn knows(&self, event_type: &str, version: &str) -> bool {
        self.registered_version(event_type) == Some(version)
    }

    /// The version `event_type` is registered at, or `None` if it isn't
    /// registered.
    pub fn registered_version(&self, event_type: &str) -> Option<&'static str> {
        self.events
            .get(event_type)
            .map(|registration| registration.version)
    }

    /// The registered event types, in alphabetical order.
    pub fn names(&self) -> Vec<&'static str> {
        self.events.keys().copied().collect()
    }
}

impl fmt::Debug for EventRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let events: BTreeMap<_, _> = self
            .events
            .iter()
            .map(|(name, registration)| (*name, registration.version))
            .collect();
        let upcasters: BTreeMap<_, BTreeMap<_, _>> = self
            .upcasters
            .iter()
            .map(|(name, chain)| {
                let steps = chain
                    .iter()
                    .map(|(from, upcaster)| (from.as_str(), upcaster.to.as_str()))
                    .collect();
                (name.as_str(), steps)
            })
            .collect();
        f.debug_struct("EventRegistry")
            .field("events", &events)
            .field("upcasters", &upcasters)
            .finish()
    }
}

/// Reads `data` as an `E`, without copying it first.
fn read_as<E: Event>(envelope: EventEnvelope, data: &Value) -> serde_json::Result<DomainEvent> {
    E::deserialize(data).map(|event| DomainEvent::new(envelope, event))
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    use super::*;
    use crate::domain::{EventFamily, Metadata};
    use crate::error::ErrorCode;
    use crate::kernel::ids::new_v7;

    /// Version 1.0.0 had `amount`; 1.1.0 renamed it to `cents`; 1.2.0 added
    /// `currency`.
    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct MoneyDeposited {
        cents: i64,
        currency: String,
    }

    impl Event for MoneyDeposited {
        const NAME: &'static str = "MoneyDeposited";
        const VERSION: &'static str = "1.2.0";
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct AccountOpened {
        owner: String,
    }

    impl Event for AccountOpened {
        const NAME: &'static str = "AccountOpened";
    }

    /// Another type that claims `MoneyDeposited`'s name.
    #[derive(Debug, Serialize, Deserialize)]
    struct LegacyDeposit {
        amount: i64,
    }

    impl Event for LegacyDeposit {
        const NAME: &'static str = "MoneyDeposited";
    }

    crate::event_family!(
        enum AccountEvent {
            AccountOpened,
            MoneyDeposited,
        }
    );
    crate::event_family!(
        enum ClashingEvent {
            MoneyDeposited,
            LegacyDeposit,
        }
    );

    fn envelope() -> EventEnvelope {
        let at = DateTime::from_timestamp_millis(1_763_280_000_123).unwrap();
        EventEnvelope {
            causation_id: Some(new_v7()),
            metadata: Metadata::from([("userId", "u-1")]),
            ..EventEnvelope::from_context(None, at)
        }
    }

    fn stored(event_type: &str, version: &str, data: Value) -> SerializedEvent {
        SerializedEvent::new(&envelope(), event_type, version, data)
    }

    /// `MoneyDeposited` with the §3.4 chain, registered steps-first and out
    /// of order to show that the order doesn't matter.
    fn chained() -> EventRegistry {
        let mut registry = EventRegistry::new();
        registry
            .register_upcaster("MoneyDeposited", "1.1.0", "1.2.0", |mut data| {
                data["currency"] = json!("IDR");
                Ok(data)
            })
            .unwrap();
        registry
            .register_upcaster("MoneyDeposited", "1.0.0", "1.1.0", |data| {
                Ok(json!({ "cents": data["amount"] }))
            })
            .unwrap();
        registry.register::<MoneyDeposited>().unwrap();
        registry
    }

    fn deposited(cents: i64) -> MoneyDeposited {
        MoneyDeposited {
            cents,
            currency: "IDR".into(),
        }
    }

    #[test]
    fn reads_an_event_at_its_registered_version() {
        let mut registry = EventRegistry::new();
        registry.register::<AccountOpened>().unwrap();
        let event = DomainEvent::new(
            envelope(),
            AccountOpened {
                owner: "Ann".into(),
            },
        );

        let read = registry.deserialize(&event.serialize().unwrap()).unwrap();
        assert_eq!(read.envelope(), event.envelope());
        assert_eq!((read.name(), read.version()), ("AccountOpened", "1.0.0"));
        assert_eq!(
            read.downcast_ref::<AccountOpened>(),
            Some(&AccountOpened {
                owner: "Ann".into()
            })
        );
    }

    #[test]
    fn upcasts_one_step_at_a_time_to_the_registered_version() {
        let registry = chained();
        let cases = [
            ("1.0.0", json!({"amount": 5})),
            ("1.1.0", json!({"cents": 5})),
            ("1.2.0", json!({"cents": 5, "currency": "IDR"})),
        ];
        for (version, data) in cases {
            let serialized = stored("MoneyDeposited", version, data);
            let read = registry.deserialize(&serialized).unwrap();
            assert_eq!(read.downcast_ref(), Some(&deposited(5)), "from {version}");
            assert_eq!(read.version(), "1.2.0");
            assert_eq!(read.envelope(), &serialized.envelope());
        }
    }

    #[test]
    fn a_missing_step_is_a_version_mismatch() {
        let mut registry = EventRegistry::new();
        registry.register::<MoneyDeposited>().unwrap();
        registry
            .register_upcaster("MoneyDeposited", "1.0.0", "1.1.0", Ok)
            .unwrap();

        let serialized = stored("MoneyDeposited", "1.0.0", json!({"amount": 5}));
        let error = registry.deserialize(&serialized).unwrap_err();
        assert_eq!(error.code(), &ErrorCode::EventVersionMismatch);
        let context = error.context();
        assert_eq!(context["eventType"], "MoneyDeposited");
        assert_eq!(context["eventId"], serialized.event_id.to_string());
        assert_eq!(context["storedVersion"], "1.0.0");
        assert_eq!(context["registeredVersion"], "1.2.0");
        assert_eq!(context["missingFrom"], "1.1.0");
        assert!(!error.retryable());
    }

    #[test]
    fn a_newer_stored_version_is_a_version_mismatch() {
        let serialized = stored("MoneyDeposited", "2.0.0", json!({"cents": 5}));
        let error = chained().deserialize(&serialized).unwrap_err();
        assert_eq!(error.code(), &ErrorCode::EventVersionMismatch);
        assert_eq!(error.context()["missingFrom"], "2.0.0");
    }

    #[test]
    fn upcasters_belong_to_one_event_type() {
        let mut registry = chained();
        registry.register::<AccountOpened>().unwrap();
        let serialized = stored("AccountOpened", "0.9.0", json!({"owner": "Ann"}));
        let error = registry.deserialize(&serialized).unwrap_err();
        assert_eq!(error.code(), &ErrorCode::EventVersionMismatch);
    }

    #[test]
    fn an_unknown_type_is_rejected() {
        let serialized = stored("AccountClosed", "1.0.0", json!({}));
        let error = chained().deserialize(&serialized).unwrap_err();
        assert_eq!(error.code(), &ErrorCode::UnknownEventType);
        assert_eq!(error.context()["eventType"], "AccountClosed");
        assert_eq!(error.context()["eventId"], serialized.event_id.to_string());
    }

    #[test]
    fn fields_that_dont_fit_are_a_serialization_error() {
        let serialized = stored("MoneyDeposited", "1.2.0", json!({"cents": "five"}));
        let error = chained().deserialize(&serialized).unwrap_err();
        assert_eq!(error.code(), &ErrorCode::SerializationError);
        assert_eq!(error.context()["eventVersion"], "1.2.0");
        assert!(std::error::Error::source(&error).is_some());

        // An upcaster that produces the wrong shape fails the same way.
        let serialized = stored("MoneyDeposited", "1.1.0", json!({"amount": 5}));
        let error = chained().deserialize(&serialized).unwrap_err();
        assert_eq!(error.code(), &ErrorCode::SerializationError);
    }

    #[test]
    fn an_upcaster_error_keeps_its_code_and_gains_context() {
        let mut registry = EventRegistry::new();
        registry.register::<MoneyDeposited>().unwrap();
        registry
            .register_upcaster("MoneyDeposited", "1.1.0", "1.2.0", |_| {
                Err(FrameworkError::command_validation("no currency known"))
            })
            .unwrap();

        let serialized = stored("MoneyDeposited", "1.1.0", json!({"cents": 5}));
        let error = registry.deserialize(&serialized).unwrap_err();
        assert_eq!(error.code(), &ErrorCode::CommandValidation);
        assert_eq!(error.message(), "no currency known");
        assert_eq!(error.context()["fromVersion"], "1.1.0");
        assert_eq!(error.context()["toVersion"], "1.2.0");
        assert_eq!(error.context()["eventId"], serialized.event_id.to_string());
    }

    #[test]
    fn a_name_registers_only_once() {
        let mut registry = chained();
        let again = registry.register::<MoneyDeposited>().unwrap_err();
        assert_eq!(again.code(), &ErrorCode::DuplicateEventRegistration);
        assert_eq!(again.context()["registeredVersion"], "1.2.0");

        let other = registry.register::<LegacyDeposit>().unwrap_err();
        assert_eq!(other.code(), &ErrorCode::DuplicateEventRegistration);
        assert_eq!(other.context()["version"], "1.0.0");
        assert!(other.message().contains("LegacyDeposit"), "{other}");
        assert_eq!(registry.registered_version("MoneyDeposited"), Some("1.2.0"));
    }

    #[test]
    fn invalid_upcasters_are_refused() {
        let mut registry = chained();
        let refused = [
            ("1.0.0", "1.0.0"),
            ("1.0.0", "1.0.1"),
            ("1.2.0", "1.0.0"),
            ("1.2.0", "1.1.0"),
        ];
        for (from, to) in refused {
            let error = registry
                .register_upcaster("MoneyDeposited", from, to, Ok)
                .unwrap_err();
            assert_eq!(
                error.code(),
                &ErrorCode::InvalidConfiguration,
                "{from}→{to}"
            );
            assert_eq!(error.context()["fromVersion"], from);
            assert_eq!(error.context()["toVersion"], to);
        }
        // A step that leads elsewhere is fine, as is the same step for
        // another event type.
        registry
            .register_upcaster("MoneyDeposited", "1.2.0", "1.3.0", Ok)
            .unwrap();
        registry
            .register_upcaster("AccountOpened", "1.0.0", "1.1.0", Ok)
            .unwrap();
    }

    #[test]
    fn knows_the_registered_version_only() {
        let registry = chained();
        assert!(registry.knows("MoneyDeposited", "1.2.0"));
        assert!(
            !registry.knows("MoneyDeposited", "1.0.0"),
            "upcastable, not registered"
        );
        assert!(!registry.knows("AccountOpened", "1.0.0"));
        assert_eq!(registry.registered_version("AccountOpened"), None);
    }

    #[test]
    fn a_family_registers_all_its_events() {
        let mut registry = EventRegistry::new();
        AccountEvent::register_all(&mut registry).unwrap();
        assert_eq!(registry.names(), ["AccountOpened", "MoneyDeposited"]);
        assert!(registry.knows("MoneyDeposited", "1.2.0"));

        let error = AccountEvent::register_all(&mut registry).unwrap_err();
        assert_eq!(error.code(), &ErrorCode::DuplicateEventRegistration);
    }

    #[test]
    fn a_family_with_a_shared_name_fails_to_register() {
        let error = ClashingEvent::register_all(&mut EventRegistry::new()).unwrap_err();
        assert_eq!(error.code(), &ErrorCode::DuplicateEventRegistration);
        assert_eq!(error.context()["eventType"], "MoneyDeposited");
    }

    #[test]
    fn clones_share_upcasters_and_debug_lists_the_chain() {
        let registry = chained().clone();
        let serialized = stored("MoneyDeposited", "1.0.0", json!({"amount": 7}));
        assert_eq!(
            registry.deserialize(&serialized).unwrap().downcast_ref(),
            Some(&deposited(7))
        );
        let text = format!("{registry:?}");
        assert!(text.contains(r#""MoneyDeposited": "1.2.0""#), "{text}");
        assert!(text.contains(r#""1.0.0": "1.1.0""#), "{text}");
    }
}
