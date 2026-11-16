//! The event store wrapper (spec §3.3, §7.1) on the local provider: typed
//! events in, typed events out, and nothing stored that can't be read back.

use std::collections::BTreeMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize, Serializer};
use serde_json::json;
use tarodomain::adapters::{Provider, StreamIds};
use tarodomain::domain::{DomainEvent, Event, EventEnvelope, EventFamily, SerializedEvent};
use tarodomain::error::{ErrorCode, FrameworkError};
use tarodomain::event_family;
use tarodomain::event_sourcing::{EventRegistry, EventStore};
use tarodomain::kernel::ids::Uuid;
use tarodomain_local::LocalProvider;
use tarodomain_testing::fixtures;

/// A bank account was opened.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct AccountOpened {
    owner: String,
}

impl Event for AccountOpened {
    const NAME: &'static str = "AccountOpened";
}

/// Money was paid in. Version 1.0.0 called the field `amount`.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
struct MoneyDeposited {
    cents: i64,
}

impl Event for MoneyDeposited {
    const NAME: &'static str = "MoneyDeposited";
    const VERSION: &'static str = "1.1.0";
}

/// `MoneyDeposited` as an older build of the application declared it.
#[derive(Debug, Serialize, Deserialize)]
struct OldMoneyDeposited {
    amount: i64,
}

impl Event for OldMoneyDeposited {
    const NAME: &'static str = "MoneyDeposited";
}

/// Never registered.
#[derive(Debug, Serialize, Deserialize)]
struct AccountClosed {}

impl Event for AccountClosed {
    const NAME: &'static str = "AccountClosed";
}

/// Registered, but its payload has no JSON form.
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

event_family!(
    enum AccountEvent {
        AccountOpened,
        MoneyDeposited,
        Unserializable,
    }
);

/// The current build's registry: the account events at their current
/// versions, and the upcaster from `MoneyDeposited` 1.0.0.
fn registry() -> EventRegistry {
    let mut registry = EventRegistry::new();
    AccountEvent::register_all(&mut registry).unwrap();
    registry
        .register_upcaster("MoneyDeposited", "1.0.0", "1.1.0", |data| {
            Ok(json!({ "cents": data["amount"] }))
        })
        .unwrap();
    registry
}

fn store() -> EventStore {
    let adapter = LocalProvider::new().create_event_store_adapter();
    EventStore::new(adapter, Arc::new(registry()))
}

fn account(n: u32) -> String {
    fixtures::stream_id("BankAccount", n)
}

/// Event `n` (its ID and time from the fixtures) with `correlation_id`.
fn event_in(n: u32, correlation_id: Uuid, event: impl Event) -> DomainEvent {
    let envelope = EventEnvelope {
        id: fixtures::event_id(n),
        correlation_id,
        ..EventEnvelope::from_context(None, fixtures::time(u64::from(n)))
    };
    DomainEvent::new(envelope, event)
}

/// Event `n`, its own correlation root.
fn event(n: u32, event: impl Event) -> DomainEvent {
    event_in(n, fixtures::event_id(n), event)
}

fn opened(n: u32) -> DomainEvent {
    event(
        n,
        AccountOpened {
            owner: "Ann".into(),
        },
    )
}

fn deposited(n: u32, cents: i64) -> DomainEvent {
    event(n, MoneyDeposited { cents })
}

fn ids(events: &[DomainEvent]) -> Vec<Uuid> {
    events.iter().map(DomainEvent::id).collect()
}

fn cents(event: &DomainEvent) -> i64 {
    event.downcast_ref::<MoneyDeposited>().unwrap().cents
}

#[tokio::test]
async fn appended_events_come_back_typed() {
    let store = store();
    let written = [opened(1), deposited(2, 250), deposited(3, 100)];
    let result = store
        .append_to_stream(&account(1), &written, 0)
        .await
        .unwrap();
    assert_eq!(result.version, 3);

    let read = store.load_stream(&account(1), 0, None).await.unwrap();
    assert_eq!(ids(&read), ids(&written));
    for (read, written) in read.iter().zip(&written) {
        assert_eq!(read.envelope(), written.envelope());
        assert_eq!(
            (read.name(), read.version()),
            (written.name(), written.version())
        );
    }
    assert_eq!(
        read[0].downcast_ref::<AccountOpened>(),
        Some(&AccountOpened {
            owner: "Ann".into()
        })
    );
    assert_eq!(cents(&read[1]), 250);

    let tail = store.load_stream(&account(1), 1, Some(1)).await.unwrap();
    assert_eq!(ids(&tail), [fixtures::event_id(2)]);
}

#[tokio::test]
async fn cross_stream_reads_come_back_typed() {
    let store = store();
    let correlation = fixtures::uuid("correlation", 1);
    store
        .append_to_stream(&account(1), &[opened(1), deposited(2, 5)], 0)
        .await
        .unwrap();
    store
        .append_to_stream(
            &account(2),
            &[event_in(3, correlation, MoneyDeposited { cents: 7 })],
            0,
        )
        .await
        .unwrap();
    store
        .append_to_stream(
            &account(1),
            &[event_in(4, correlation, MoneyDeposited { cents: 9 })],
            2,
        )
        .await
        .unwrap();

    let all = store.load_all(None, None).await.unwrap();
    assert_eq!(all.len(), 4);
    assert_eq!(all.iter().skip(1).map(cents).collect::<Vec<_>>(), [5, 7, 9]);

    let after = store
        .load_all(Some(fixtures::event_id(2)), Some(1))
        .await
        .unwrap();
    assert_eq!(ids(&after), [fixtures::event_id(3)]);

    let category = store
        .load_stream_after_event("BankAccount", Some(fixtures::event_id(1)), None)
        .await
        .unwrap();
    assert_eq!(category.iter().map(cents).collect::<Vec<_>>(), [5, 7, 9]);

    let correlated = store.load_by_correlation_id(correlation).await.unwrap();
    assert_eq!(correlated.iter().map(cents).collect::<Vec<_>>(), [7, 9]);
}

#[tokio::test]
async fn events_the_registry_cant_read_back_are_refused() {
    let store = store();

    let unknown = [opened(1), event(2, AccountClosed {})];
    let error = store
        .append_to_stream(&account(1), &unknown, 0)
        .await
        .unwrap_err();
    assert_eq!(error.code(), &ErrorCode::UnknownEventType);
    assert_eq!(error.context()["eventType"], "AccountClosed");
    assert_eq!(
        error.context()["eventId"],
        fixtures::event_id(2).to_string()
    );
    assert_eq!(error.context()["streamId"], account(1));
    assert!(!error.retryable());

    let old = [opened(1), event(2, OldMoneyDeposited { amount: 5 })];
    let error = store
        .append_to_stream(&account(1), &old, 0)
        .await
        .unwrap_err();
    assert_eq!(error.code(), &ErrorCode::EventVersionMismatch);
    assert_eq!(error.context()["version"], "1.0.0");
    assert_eq!(error.context()["registeredVersion"], "1.1.0");

    let unserializable = [
        opened(1),
        event(
            2,
            Unserializable {
                value: BTreeMap::new(),
            },
        ),
    ];
    let error = store
        .append_to_stream(&account(1), &unserializable, 0)
        .await
        .unwrap_err();
    assert_eq!(error.code(), &ErrorCode::SerializationError);
    assert_eq!(error.context()["streamId"], account(1));

    assert_eq!(store.get_stream_version(&account(1)).await.unwrap(), 0);
    assert!(store.load_all(None, None).await.unwrap().is_empty());
}

#[tokio::test]
async fn events_an_older_build_stored_are_upcast_on_read() {
    let adapter = LocalProvider::new().create_event_store_adapter();
    let mut old_registry = EventRegistry::new();
    old_registry.register::<AccountOpened>().unwrap();
    old_registry.register::<OldMoneyDeposited>().unwrap();
    let old_build = EventStore::new(Arc::clone(&adapter), Arc::new(old_registry));
    let new_build = EventStore::new(adapter, Arc::new(registry()));

    let old = [opened(1), event(2, OldMoneyDeposited { amount: 250 })];
    old_build
        .append_to_stream(&account(1), &old, 0)
        .await
        .unwrap();
    new_build
        .append_to_stream(&account(1), &[deposited(3, 100)], 2)
        .await
        .unwrap();

    let read = new_build.load_stream(&account(1), 1, None).await.unwrap();
    assert_eq!(read.iter().map(cents).collect::<Vec<_>>(), [250, 100]);
    assert_eq!(read[0].version(), "1.1.0");
    assert_eq!(read[0].envelope(), old[1].envelope());

    // The old build can't read what the new one wrote.
    let error = old_build
        .load_stream(&account(1), 0, None)
        .await
        .unwrap_err();
    assert_eq!(error.code(), &ErrorCode::EventVersionMismatch);
    assert_eq!(error.context()["missingFrom"], "1.1.0");
    assert_eq!(error.context()["streamId"], account(1));
}

#[tokio::test]
async fn a_stored_event_nobody_registered_fails_the_load() {
    let store = store();
    let closed = SerializedEvent::new(
        &EventEnvelope::from_context(None, fixtures::time(1)),
        "AccountClosed",
        "1.0.0",
        json!({}),
    );
    let correlation_id = closed.correlation_id;
    store
        .adapter()
        .append_to_stream(&account(1), vec![closed], 0)
        .await
        .unwrap();

    let error = store.load_stream(&account(1), 0, None).await.unwrap_err();
    assert_eq!(error.code(), &ErrorCode::UnknownEventType);
    assert_eq!(error.context()["streamId"], account(1));

    let error = store.load_all(None, None).await.unwrap_err();
    assert_eq!(error.context()["streamId"], StreamIds::ALL);
    let error = store
        .load_stream_after_event("BankAccount", None, None)
        .await
        .unwrap_err();
    assert_eq!(error.context()["streamId"], "BankAccount");
    let error = store
        .load_by_correlation_id(correlation_id)
        .await
        .unwrap_err();
    assert_eq!(error.context()["correlationId"], correlation_id.to_string());

    // The raw read through the adapter still works.
    let raw = store.adapter().load_all(None, None).await.unwrap();
    assert_eq!(raw[0].event_type, "AccountClosed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_appends_at_one_version_let_one_win() {
    let store = store();
    let append = |n: u32| {
        let store = store.clone();
        tokio::spawn(async move {
            store
                .append_to_stream(&account(1), &[deposited(n, i64::from(n))], 0)
                .await
        })
    };
    let (first, second) = tokio::join!(append(1), append(2));
    let outcomes = [first.unwrap(), second.unwrap()];

    let errors: Vec<&FrameworkError> = outcomes.iter().filter_map(|r| r.as_ref().err()).collect();
    assert_eq!(errors.len(), 1, "{outcomes:?}");
    assert_eq!(errors[0].code(), &ErrorCode::OptimisticConcurrency);
    assert!(errors[0].retryable());
    assert_eq!(store.get_stream_version(&account(1)).await.unwrap(), 1);
}

#[tokio::test]
async fn stream_queries_pass_through() {
    let store = store();
    assert!(!store.stream_exists(&account(1)).await.unwrap());
    assert_eq!(store.get_stream_metadata(&account(1)).await.unwrap(), None);

    store
        .append_to_stream(&account(1), &[opened(1), deposited(2, 5)], 0)
        .await
        .unwrap();
    assert!(store.stream_exists(&account(1)).await.unwrap());
    let metadata = store
        .get_stream_metadata(&account(1))
        .await
        .unwrap()
        .unwrap();
    assert_eq!((metadata.stream_id, metadata.version), (account(1), 2));

    store.delete_stream(&account(1)).await.unwrap();
    assert_eq!(store.get_stream_version(&account(1)).await.unwrap(), 0);
    assert!(
        store
            .load_stream(&account(1), 0, None)
            .await
            .unwrap()
            .is_empty()
    );
}

#[test]
fn debug_shows_the_registry() {
    let text = format!("{:?}", store());
    assert!(
        text.starts_with("EventStore { registry: EventRegistry"),
        "{text}"
    );
    assert!(text.contains(r#""MoneyDeposited": "1.1.0""#), "{text}");
}
