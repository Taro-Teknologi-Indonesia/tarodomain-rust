//! The local provider (spec §24.2).

use std::sync::Arc;

use tarodomain::adapters::{
    CommandIdempotencyStore, DeadLetterStore, EventBusConfig, EventStoreAdapter,
    ProcessedEventStore, Provider, SagaStoreAdapter, SnapshotStoreAdapter, UnitOfWork,
};
use tarodomain::async_trait;
use tarodomain::bus::{EventBus, InMemoryEventBus};
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::clock::Clock;

use crate::{
    InMemoryCommandIdempotencyStore, InMemoryDeadLetterStore, InMemoryEventStoreAdapter,
    InMemoryProcessedEventStore, InMemorySagaStore, InMemorySnapshotStore, InMemoryUnitOfWork,
    LocalDatabase,
};

/// The local provider (spec §24.2): every store in one [`LocalDatabase`],
/// an [`InMemoryEventBus`] for both consistency modes, and
/// [`InMemoryUnitOfWork`] transactions.
///
/// For tests and local development: nothing is persisted, and eventual mode
/// delivers in process, like strong mode but outside the command's unit of
/// work. A clone shares the database.
///
/// ```
/// use tarodomain::adapters::Provider;
/// use tarodomain_local::{LocalDatabase, LocalProvider};
///
/// let db = LocalDatabase::new();
/// let provider = LocalProvider::with_database(db.clone());
/// assert_eq!(provider.provider_type(), "local");
/// assert!(provider.database().ptr_eq(&db));
/// ```
#[derive(Clone, Debug, Default)]
pub struct LocalProvider {
    db: LocalDatabase,
}

impl LocalProvider {
    /// The one event bus transport the provider has, also its default.
    pub const IN_MEMORY: &'static str = "in_memory";

    /// A provider over a new, empty database on the system clock.
    pub fn new() -> Self {
        Self::default()
    }

    /// A provider over `db`, such as one with a
    /// [`ManualClock`](tarodomain::kernel::clock::ManualClock), or one
    /// another provider uses too.
    pub fn with_database(db: LocalDatabase) -> Self {
        Self { db }
    }

    /// The database the provider's stores share.
    pub fn database(&self) -> &LocalDatabase {
        &self.db
    }
}

#[async_trait]
impl Provider for LocalProvider {
    /// `"local"`.
    fn provider_type(&self) -> &str {
        "local"
    }

    fn create_event_store_adapter(&self) -> Arc<dyn EventStoreAdapter> {
        Arc::new(InMemoryEventStoreAdapter::new(self.db.clone()))
    }

    fn create_snapshot_store_adapter(&self) -> Arc<dyn SnapshotStoreAdapter> {
        Arc::new(InMemorySnapshotStore::new(self.db.clone()))
    }

    fn create_saga_store_adapter(&self) -> Arc<dyn SagaStoreAdapter> {
        Arc::new(InMemorySagaStore::new(self.db.clone()))
    }

    fn create_processed_event_store(&self) -> Arc<dyn ProcessedEventStore> {
        Arc::new(InMemoryProcessedEventStore::new(self.db.clone()))
    }

    fn create_command_idempotency_store(&self) -> Arc<dyn CommandIdempotencyStore> {
        Arc::new(InMemoryCommandIdempotencyStore::new(self.db.clone()))
    }

    fn create_dead_letter_store(&self) -> Arc<dyn DeadLetterStore> {
        Arc::new(InMemoryDeadLetterStore::new(self.db.clone()))
    }

    /// A new [`InMemoryEventBus`] named `local-strong` or `local-eventual`.
    /// The transport must be unset or [`IN_MEMORY`](Self::IN_MEMORY); any
    /// other is `INVALID_CONFIGURATION`.
    fn create_event_bus(&self, config: &EventBusConfig) -> Result<Arc<dyn EventBus>> {
        match config.transport.as_deref() {
            None | Some(Self::IN_MEMORY) => {
                let name = format!("local-{}", config.consistency);
                Ok(Arc::new(InMemoryEventBus::new(name)))
            }
            Some(transport) => {
                let message = format!(
                    "the local provider has no {transport:?} transport, only {:?}",
                    Self::IN_MEMORY
                );
                let error = FrameworkError::invalid_configuration(message);
                Err(error.with_context("transport", transport))
            }
        }
    }

    /// A new [`InMemoryUnitOfWork`] over the provider's database.
    fn create_unit_of_work(&self) -> Option<Arc<dyn UnitOfWork>> {
        Some(Arc::new(InMemoryUnitOfWork::new(self.db.clone())))
    }

    /// The database's clock.
    fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(self.db.clock())
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use serde_json::json;
    use tarodomain::adapters::{ConsistencyMode, StreamIds};
    use tarodomain::domain::{Metadata, SerializedEvent};
    use tarodomain::error::ErrorCode;
    use tarodomain::kernel::clock::ManualClock;
    use tarodomain::kernel::ids::new_v7;

    use super::*;

    fn event() -> SerializedEvent {
        let id = new_v7();
        SerializedEvent {
            event_id: id,
            event_type: "AccountOpened".into(),
            event_version: "1.0.0".into(),
            causation_id: None,
            correlation_id: id,
            command_causation_id: None,
            data: json!({}),
            metadata: Metadata::new(),
            occurred_at: DateTime::UNIX_EPOCH,
        }
    }

    #[tokio::test]
    async fn adapters_share_the_database() {
        let provider = LocalProvider::new();
        let stream_id = StreamIds::for_aggregate("BankAccount", new_v7());
        let writer = provider.create_event_store_adapter();
        writer
            .append_to_stream(&stream_id, vec![event()], 0)
            .await
            .unwrap();
        let other = LocalProvider::with_database(provider.database().clone());
        let reader = other.create_event_store_adapter();
        assert_eq!(reader.get_stream_version(&stream_id).await.unwrap(), 1);
        assert!(provider.initialize().await.is_ok());
    }

    #[tokio::test]
    async fn a_unit_covers_every_store() {
        let provider = LocalProvider::new();
        let events = provider.create_event_store_adapter();
        let commands = provider.create_command_idempotency_store();
        let stream_id = StreamIds::for_aggregate("BankAccount", new_v7());
        let command_id = new_v7();
        let unit = provider.create_unit_of_work().unwrap();
        let result = unit
            .execute(async {
                events
                    .append_to_stream(&stream_id, vec![event()], 0)
                    .await?;
                commands
                    .mark_processed(command_id, "Open", "OpenHandler")
                    .await?;
                assert!(commands.exists(command_id).await?);
                Err::<(), _>(FrameworkError::aggregate_not_found("missing"))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(events.get_stream_version(&stream_id).await.unwrap(), 0);
        assert!(!commands.exists(command_id).await.unwrap());
    }

    #[test]
    fn each_unit_is_new() {
        let provider = LocalProvider::new();
        let (a, b) = (
            provider.create_unit_of_work(),
            provider.create_unit_of_work(),
        );
        assert!(!Arc::ptr_eq(&a.unwrap(), &b.unwrap()));
    }

    #[test]
    fn buses_are_in_memory_for_both_modes() {
        let provider = LocalProvider::new();
        for mode in [ConsistencyMode::Strong, ConsistencyMode::Eventual] {
            let bus = provider
                .create_event_bus(&EventBusConfig::new(mode))
                .unwrap();
            assert_eq!(bus.name(), format!("local-{mode}"));
            assert!(bus.as_subscribable().is_some());
            let config = EventBusConfig::new(mode).with_transport("in_memory");
            assert!(provider.create_event_bus(&config).is_ok());
        }
        let config =
            EventBusConfig::new(ConsistencyMode::Eventual).with_transport("cloudflare_queue");
        let error = provider.create_event_bus(&config).err().unwrap();
        assert_eq!(*error.code(), ErrorCode::InvalidConfiguration);
        assert_eq!(error.context()["transport"], "cloudflare_queue");
    }

    #[test]
    fn the_clock_is_the_database_clock() {
        let start = DateTime::UNIX_EPOCH;
        let db = LocalDatabase::with_clock(Arc::new(ManualClock::new(start)));
        let provider = LocalProvider::with_database(db);
        assert_eq!(provider.clock().now(), start);
        assert_eq!(provider.provider_type(), "local");
    }
}
