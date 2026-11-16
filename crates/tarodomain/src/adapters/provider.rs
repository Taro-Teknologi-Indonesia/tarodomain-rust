//! The provider contract (spec §24.1): one platform's stores, buses and
//! transactions.

use std::fmt;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use super::{
    CommandIdempotencyStore, DeadLetterStore, EventStoreAdapter, ProcessedEventStore,
    SagaStoreAdapter, SnapshotStoreAdapter, UnitOfWork,
};
use crate::async_trait;
use crate::bus::EventBus;
use crate::error::Result;
use crate::kernel::backoff::Sleeper;
use crate::kernel::clock::{Clock, SystemClock};

/// When a command's events reach projections and sagas (spec §15).
///
/// Serialized as `"strong"` or `"eventual"`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConsistencyMode {
    /// In process, before the command returns, in the command's unit of
    /// work: a failing handler rolls the whole command back (§15.1).
    Strong,
    /// Later, through a transport such as a queue, with the cron catch-up as
    /// a safety net. The append commits on its own (§15.2).
    Eventual,
}

impl ConsistencyMode {
    /// The text form, `"strong"` or `"eventual"`.
    pub const fn as_str(self) -> &'static str {
        match self {
            ConsistencyMode::Strong => "strong",
            ConsistencyMode::Eventual => "eventual",
        }
    }
}

impl fmt::Display for ConsistencyMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What [`Provider::create_event_bus`] should build (spec §24.1).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct EventBusConfig {
    /// The consistency mode the bus serves.
    pub consistency: ConsistencyMode,
    /// The transport to use in eventual mode, such as `"cloudflare_queue"`
    /// (§25.1). `None` lets the provider pick its default.
    pub transport: Option<String>,
}

impl EventBusConfig {
    /// A config for `consistency` with the provider's default transport.
    pub fn new(consistency: ConsistencyMode) -> Self {
        Self {
            consistency,
            transport: None,
        }
    }

    /// Names the transport.
    pub fn with_transport(mut self, transport: impl Into<String>) -> Self {
        self.transport = Some(transport.into());
        self
    }
}

/// One platform's implementation of the persistence layer (spec §24.1):
/// the six stores, the event buses and the unit of work.
///
/// The framework calls each `create_*` method once while it is built, and
/// [`create_unit_of_work`](Self::create_unit_of_work) once per unit. Adapters
/// from one provider share its storage, so a unit of work covers them all.
#[async_trait]
pub trait Provider: Send + Sync {
    /// The provider's name, such as `"local"` or `"cloudflare"`.
    fn provider_type(&self) -> &str;

    /// The event store's storage (§3.2).
    fn create_event_store_adapter(&self) -> Arc<dyn EventStoreAdapter>;

    /// The snapshot store's storage (§8.1).
    fn create_snapshot_store_adapter(&self) -> Arc<dyn SnapshotStoreAdapter>;

    /// The saga store's storage (§9.1).
    fn create_saga_store_adapter(&self) -> Arc<dyn SagaStoreAdapter>;

    /// Which events each projection and saga has handled (§12.3).
    fn create_processed_event_store(&self) -> Arc<dyn ProcessedEventStore>;

    /// Which commands have been processed (§14.1).
    fn create_command_idempotency_store(&self) -> Arc<dyn CommandIdempotencyStore>;

    /// Events that handlers gave up on (§21.1).
    fn create_dead_letter_store(&self) -> Arc<dyn DeadLetterStore>;

    /// The bus for one consistency mode.
    ///
    /// The strong bus must deliver in process and be subscribable
    /// ([`EventBus::as_subscribable`]), normally an
    /// [`InMemoryEventBus`](crate::bus::InMemoryEventBus). An eventual bus
    /// may hand events to a transport instead; if it is subscribable, the
    /// framework subscribes the handlers to it too. Fails with
    /// `INVALID_CONFIGURATION` for a transport the provider doesn't have.
    fn create_event_bus(&self, config: &EventBusConfig) -> Result<Arc<dyn EventBus>>;

    /// A new unit of work over this provider's stores, or `None` if the
    /// platform has no transactions. Strong consistency needs one.
    fn create_unit_of_work(&self) -> Option<Arc<dyn UnitOfWork>>;

    /// Prepares the storage, such as running migrations. The framework calls
    /// it once while it is built, and again from `migrate()`, so it must be
    /// safe to repeat. Does nothing by default.
    async fn initialize(&self) -> Result<()> {
        Ok(())
    }

    /// The clock the framework and the adapters tell time by. Defaults to
    /// [`SystemClock`].
    fn clock(&self) -> Arc<dyn Clock> {
        Arc::new(SystemClock)
    }

    /// How the framework waits between retries.
    ///
    /// Defaults to [`TokioSleeper`](crate::kernel::backoff::TokioSleeper)
    /// natively. On `wasm32` tokio's timers don't work, so the default
    /// returns at once, and a provider for a WebAssembly platform, such as
    /// Cloudflare Workers, returns its own.
    fn sleeper(&self) -> Arc<dyn Sleeper> {
        default_sleeper()
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn default_sleeper() -> Arc<dyn Sleeper> {
    Arc::new(crate::kernel::backoff::TokioSleeper)
}

#[cfg(target_arch = "wasm32")]
fn default_sleeper() -> Arc<dyn Sleeper> {
    use std::time::Duration;

    use crate::kernel::BoxFuture;

    /// Returns at once: there is no timer to wait on.
    struct NoWait;

    impl Sleeper for NoWait {
        fn sleep(&self, _: Duration) -> BoxFuture<'_, ()> {
            Box::pin(std::future::ready(()))
        }
    }

    Arc::new(NoWait)
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;

    #[test]
    fn consistency_modes_have_lower_case_text() {
        for (mode, text) in [
            (ConsistencyMode::Strong, "strong"),
            (ConsistencyMode::Eventual, "eventual"),
        ] {
            assert_eq!(mode.as_str(), text);
            assert_eq!(mode.to_string(), text);
            assert_eq!(serde_json::to_value(mode).unwrap(), text);
            let parsed: ConsistencyMode = serde_json::from_value(text.into()).unwrap();
            assert_eq!(parsed, mode);
        }
    }

    #[test]
    fn a_bus_config_names_its_transport_only_when_asked() {
        let config = EventBusConfig::new(ConsistencyMode::Eventual);
        assert_eq!(config.transport, None);
        let config = config.with_transport("cloudflare_queue");
        assert_eq!(config.consistency, ConsistencyMode::Eventual);
        assert_eq!(config.transport.as_deref(), Some("cloudflare_queue"));
    }

    /// A provider that implements only the required methods.
    struct Minimal;

    impl Provider for Minimal {
        fn provider_type(&self) -> &str {
            "minimal"
        }
        fn create_event_store_adapter(&self) -> Arc<dyn EventStoreAdapter> {
            unreachable!()
        }
        fn create_snapshot_store_adapter(&self) -> Arc<dyn SnapshotStoreAdapter> {
            unreachable!()
        }
        fn create_saga_store_adapter(&self) -> Arc<dyn SagaStoreAdapter> {
            unreachable!()
        }
        fn create_processed_event_store(&self) -> Arc<dyn ProcessedEventStore> {
            unreachable!()
        }
        fn create_command_idempotency_store(&self) -> Arc<dyn CommandIdempotencyStore> {
            unreachable!()
        }
        fn create_dead_letter_store(&self) -> Arc<dyn DeadLetterStore> {
            unreachable!()
        }
        fn create_event_bus(&self, _: &EventBusConfig) -> Result<Arc<dyn EventBus>> {
            unreachable!()
        }
        fn create_unit_of_work(&self) -> Option<Arc<dyn UnitOfWork>> {
            None
        }
    }

    #[tokio::test]
    async fn defaults_initialize_nothing_and_use_the_system_clock_and_tokio() {
        let provider: Arc<dyn Provider> = Arc::new(Minimal);
        assert_eq!(provider.provider_type(), "minimal");
        provider.initialize().await.unwrap();

        let before = chrono::Utc::now();
        let now = provider.clock().now();
        assert!(now >= before && now <= chrono::Utc::now());

        let start = Instant::now();
        provider.sleeper().sleep(Duration::from_millis(5)).await;
        assert!(start.elapsed() >= Duration::from_millis(5));
    }
}
