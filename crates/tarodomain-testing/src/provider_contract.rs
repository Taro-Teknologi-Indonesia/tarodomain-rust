//! The provider contract (spec §24.1).

use std::sync::Arc;

use tarodomain::adapters::{ConsistencyMode, EventBusConfig, Provider};
use tarodomain::error::ErrorCode;

use crate::fixtures::{event_id, events, ids, stream_id};
use crate::{ProviderFactory, far_future, long_ago};

/// Checks a [`Provider`] itself: it names itself, the adapters it hands out
/// share its storage, `initialize` can run again without losing data, its
/// strong bus is subscribable and an unknown transport is
/// `INVALID_CONFIGURATION`, each unit of work is new, and its clock tells a
/// plausible time.
///
/// Panics on the first case that fails.
pub async fn provider_contract(factory: impl ProviderFactory) {
    adapters_share_the_storage(factory.create().await).await;
    initialize_can_run_again(factory.create().await).await;
    event_buses(factory.create().await);
    each_unit_of_work_is_new(factory.create().await);
    it_names_itself_and_tells_time(factory.create().await);
}

const ACCOUNT: &str = "BankAccount";

async fn adapters_share_the_storage(provider: Arc<dyn Provider>) {
    let stream = stream_id(ACCOUNT, 1);
    let writer = provider.create_event_store_adapter();
    let reader = provider.create_event_store_adapter();
    writer
        .append_to_stream(&stream, events([event_id(1)]), 0)
        .await
        .unwrap();
    let loaded = reader.load_stream(&stream, 0, None).await.unwrap();
    assert_eq!(
        ids(&loaded),
        [event_id(1)],
        "a second adapter sees the first's writes"
    );

    let (commands, other) = (
        provider.create_command_idempotency_store(),
        provider.create_command_idempotency_store(),
    );
    let command = event_id(2);
    commands
        .mark_processed(command, "DepositMoney", "DepositHandler")
        .await
        .unwrap();
    assert!(other.exists(command).await.unwrap());
}

async fn initialize_can_run_again(provider: Arc<dyn Provider>) {
    provider.initialize().await.unwrap();
    let stream = stream_id(ACCOUNT, 1);
    let store = provider.create_event_store_adapter();
    store
        .append_to_stream(&stream, events([event_id(1)]), 0)
        .await
        .unwrap();
    provider.initialize().await.unwrap();
    provider.initialize().await.unwrap();
    let version = store.get_stream_version(&stream).await.unwrap();
    assert_eq!(version, 1, "initialize kept the data");
}

fn event_buses(provider: Arc<dyn Provider>) {
    let strong = provider.create_event_bus(&EventBusConfig::new(ConsistencyMode::Strong));
    let strong = strong.unwrap();
    assert!(!strong.name().is_empty());
    assert!(
        strong.as_subscribable().is_some(),
        "the strong bus is subscribable"
    );
    let eventual = provider.create_event_bus(&EventBusConfig::new(ConsistencyMode::Eventual));
    assert!(eventual.is_ok(), "the default eventual transport");

    let unknown =
        EventBusConfig::new(ConsistencyMode::Eventual).with_transport("no-such-transport");
    match provider.create_event_bus(&unknown) {
        Err(error) => assert_eq!(*error.code(), ErrorCode::InvalidConfiguration),
        Ok(bus) => panic!("an unknown transport gave the bus {:?}", bus.name()),
    }
}

fn each_unit_of_work_is_new(provider: Arc<dyn Provider>) {
    let (Some(a), Some(b)) = (
        provider.create_unit_of_work(),
        provider.create_unit_of_work(),
    ) else {
        return;
    };
    assert!(!Arc::ptr_eq(&a, &b), "each call creates a new unit");
    assert!(!a.is_active() && !b.is_active(), "a new unit hasn't begun");
}

fn it_names_itself_and_tells_time(provider: Arc<dyn Provider>) {
    assert!(!provider.provider_type().is_empty());
    let now = provider.clock().now();
    assert!(
        long_ago() <= now && now < far_future(),
        "a plausible time: {now}"
    );
}
