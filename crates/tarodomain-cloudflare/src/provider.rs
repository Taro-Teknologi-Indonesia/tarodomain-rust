//! The Cloudflare provider (spec §24.2–24.3).

use std::fmt;
use std::sync::Arc;

use tarodomain::adapters::{
    CommandIdempotencyStore, ConsistencyMode, DeadLetterStore, EventBusConfig, EventStoreAdapter,
    ProcessedEventStore, Provider, SagaStoreAdapter, SnapshotStoreAdapter, UnitOfWork,
};
use tarodomain::async_trait;
use tarodomain::bus::{EventBus, InMemoryEventBus};
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::backoff::Sleeper;
use tarodomain::kernel::clock::{Clock, SystemClock};
use tokio::sync::OnceCell;

use crate::d1::{D1Backend, D1Db};
use crate::dead_letter::D1DeadLetterStore;
use crate::event_store::D1EventStoreAdapter;
use crate::idempotency::{D1CommandIdempotencyStore, D1ProcessedEventStore};
use crate::migrations::migrate;
use crate::queue_bus::{CloudflareQueueEventBus, EventQueue};
use crate::saga_store::D1SagaStore;
use crate::snapshot_store::D1SnapshotStore;
use crate::unit_of_work::D1UnitOfWork;

/// The names of a Worker's bindings, as in `wrangler.toml`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bindings {
    /// The D1 database binding.
    pub d1: String,
    /// The queue the event bus produces to, if eventual mode is used.
    pub queue: Option<String>,
}

impl Bindings {
    /// The default binding name for D1 (`DB`), and no queue.
    pub fn new() -> Self {
        Self {
            d1: "DB".to_owned(),
            queue: None,
        }
    }

    /// Uses the D1 binding called `name`.
    pub fn d1(mut self, name: impl Into<String>) -> Self {
        self.d1 = name.into();
        self
    }

    /// Produces events to the queue binding called `name`.
    pub fn queue(mut self, name: impl Into<String>) -> Self {
        self.queue = Some(name.into());
        self
    }
}

impl Default for Bindings {
    fn default() -> Self {
        Self::new()
    }
}

/// TaroDomain's provider for Cloudflare: D1 stores everything, a Queue
/// carries events in eventual mode, and [`D1UnitOfWork`] makes a strong-mode
/// command's writes one atomic `batch`.
///
/// ```
/// use std::sync::Arc;
/// use tarodomain::adapters::Provider;
/// use tarodomain_cloudflare::provider::CloudflareProvider;
/// use tarodomain_cloudflare::queue_bus::RecordingQueue;
/// use tarodomain_cloudflare::sqlite_backend::SqliteBackend;
///
/// let backend = Arc::new(SqliteBackend::in_memory().unwrap());
/// let provider = CloudflareProvider::with_backend(backend, Some(Arc::new(RecordingQueue::new())));
/// assert_eq!(provider.provider_type(), "cloudflare");
/// ```
///
/// On a Worker, build it from the environment with
/// `CloudflareProvider::new` (wasm32 only). Clones share the database.
#[derive(Clone)]
pub struct CloudflareProvider {
    db: D1Db,
    queue: Option<Arc<dyn EventQueue>>,
    clock: Arc<dyn Clock>,
    sleeper: Arc<dyn Sleeper>,
    migrated: Arc<OnceCell<()>>,
}

impl CloudflareProvider {
    /// The queue transport of [`EventBusConfig`].
    pub const QUEUE: &'static str = "queue";
    /// The in-process transport of [`EventBusConfig`], the strong default.
    pub const IN_MEMORY: &'static str = "in_memory";

    /// A provider over any backend, for native tests: SQLite in place of D1
    /// and a recording queue in place of Queues.
    pub fn with_backend(backend: Arc<dyn D1Backend>, queue: Option<Arc<dyn EventQueue>>) -> Self {
        Self {
            db: D1Db::new(backend),
            queue,
            clock: Arc::new(SystemClock),
            sleeper: native_sleeper(),
            migrated: Arc::new(OnceCell::new()),
        }
    }

    /// A provider over the bindings of a Worker's `env`.
    #[cfg(target_arch = "wasm32")]
    pub fn new(env: &worker::Env, bindings: &Bindings) -> Result<Self> {
        use crate::d1::WorkerD1;
        use crate::platform::worker_error;
        use crate::queue_bus::WorkerQueue;

        let d1 = env
            .d1(&bindings.d1)
            .map_err(|error| worker_error("D1 binding", error))?;
        let queue = match &bindings.queue {
            Some(name) => {
                let queue = env
                    .queue(name)
                    .map_err(|error| worker_error("queue binding", error))?;
                Some(Arc::new(WorkerQueue::new(queue)) as Arc<dyn EventQueue>)
            }
            None => None,
        };
        let mut provider = Self::with_backend(Arc::new(WorkerD1::new(d1)), queue);
        provider.sleeper = Arc::new(crate::platform::WorkerSleeper);
        Ok(provider)
    }

    /// Uses `clock` for the stores' timestamps.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The statement layer, for the application's own tables.
    ///
    /// A read model that writes through it joins a strong-mode command's
    /// batch, so its writes commit or roll back with the events. Use upserts
    /// and increments (`balance = balance + ?`), not read-then-write: reads
    /// inside a unit see the database as it was before the unit began (§14.3).
    pub fn db(&self) -> &D1Db {
        &self.db
    }

    /// The queue the eventual bus sends to, if one is configured.
    pub fn queue(&self) -> Option<&Arc<dyn EventQueue>> {
        self.queue.as_ref()
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_sleeper() -> Arc<dyn Sleeper> {
    Arc::new(tarodomain::kernel::backoff::TokioSleeper)
}

#[cfg(target_arch = "wasm32")]
fn native_sleeper() -> Arc<dyn Sleeper> {
    Arc::new(crate::platform::WorkerSleeper)
}

#[async_trait]
impl Provider for CloudflareProvider {
    fn provider_type(&self) -> &str {
        "cloudflare"
    }

    fn create_event_store_adapter(&self) -> Arc<dyn EventStoreAdapter> {
        Arc::new(D1EventStoreAdapter::new(
            self.db.clone(),
            Arc::clone(&self.clock),
        ))
    }

    fn create_snapshot_store_adapter(&self) -> Arc<dyn SnapshotStoreAdapter> {
        Arc::new(D1SnapshotStore::new(
            self.db.clone(),
            Arc::clone(&self.clock),
        ))
    }

    fn create_saga_store_adapter(&self) -> Arc<dyn SagaStoreAdapter> {
        Arc::new(D1SagaStore::new(self.db.clone(), Arc::clone(&self.clock)))
    }

    fn create_processed_event_store(&self) -> Arc<dyn ProcessedEventStore> {
        Arc::new(D1ProcessedEventStore::new(
            self.db.clone(),
            Arc::clone(&self.clock),
        ))
    }

    fn create_command_idempotency_store(&self) -> Arc<dyn CommandIdempotencyStore> {
        Arc::new(D1CommandIdempotencyStore::new(
            self.db.clone(),
            Arc::clone(&self.clock),
        ))
    }

    fn create_dead_letter_store(&self) -> Arc<dyn DeadLetterStore> {
        Arc::new(D1DeadLetterStore::new(
            self.db.clone(),
            Arc::clone(&self.clock),
        ))
    }

    /// Strong mode defaults to an in-process bus, which delivers inside the
    /// command's unit of work. Eventual mode defaults to the queue bus, and
    /// fails with `INVALID_CONFIGURATION` if no queue binding is configured.
    /// Transport `"queue"` and `"in_memory"` choose either explicitly.
    fn create_event_bus(&self, config: &EventBusConfig) -> Result<Arc<dyn EventBus>> {
        let transport = match (config.transport.as_deref(), config.consistency) {
            (Some(transport), _) => transport,
            (None, ConsistencyMode::Strong) => Self::IN_MEMORY,
            (None, ConsistencyMode::Eventual) => Self::QUEUE,
        };
        match transport {
            Self::IN_MEMORY => Ok(Arc::new(InMemoryEventBus::new(format!(
                "cloudflare-{}",
                config.consistency
            )))),
            Self::QUEUE => {
                let queue = self.queue.clone().ok_or_else(|| {
                    FrameworkError::invalid_configuration(
                        "the queue transport needs a queue binding (Bindings::queue)",
                    )
                })?;
                Ok(Arc::new(CloudflareQueueEventBus::new(
                    self.db.clone(),
                    queue,
                )))
            }
            other => Err(FrameworkError::invalid_configuration(format!(
                "the cloudflare provider has no {other:?} transport, only {:?} and {:?}",
                Self::QUEUE,
                Self::IN_MEMORY
            ))
            .with_context("transport", other)),
        }
    }

    /// A new [`D1UnitOfWork`] over the provider's database.
    fn create_unit_of_work(&self) -> Option<Arc<dyn UnitOfWork>> {
        Some(Arc::new(D1UnitOfWork::new(self.db.clone())))
    }

    /// Applies the migrations that are missing, once per provider (a Worker
    /// keeps its provider for the life of the isolate).
    async fn initialize(&self) -> Result<()> {
        self.migrated
            .get_or_try_init(|| async { migrate(&self.db).await.map(drop) })
            .await
            .map(drop)
    }

    fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }

    fn sleeper(&self) -> Arc<dyn Sleeper> {
        Arc::clone(&self.sleeper)
    }
}

impl fmt::Debug for CloudflareProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudflareProvider")
            .field("queue", &self.queue.is_some())
            .finish()
    }
}
