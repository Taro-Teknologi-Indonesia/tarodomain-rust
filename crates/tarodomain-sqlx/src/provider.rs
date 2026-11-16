//! The SQL provider (spec §24): SQLite or PostgreSQL through sqlx.

use std::fmt;
use std::sync::Arc;

use tarodomain::adapters::{
    CommandIdempotencyStore, ConsistencyMode, DeadLetterStore, EventBusConfig, EventStoreAdapter,
    ProcessedEventStore, Provider, SagaStoreAdapter, SnapshotStoreAdapter, UnitOfWork,
};
use tarodomain::async_trait;
use tarodomain::bus::{EventBus, InMemoryEventBus};
use tarodomain::error::{FrameworkError, Result};
use tarodomain::kernel::clock::{Clock, SystemClock};
use tokio::sync::Notify;

use crate::db::{Db, Dialect};
use crate::dead_letter::SqlxDeadLetterStore;
use crate::event_bus::SqlxEventBus;
use crate::event_store::SqlxEventStoreAdapter;
use crate::idempotency::{SqlxCommandIdempotencyStore, SqlxProcessedEventStore};
use crate::saga_store::SqlxSagaStore;
use crate::snapshot_store::SqlxSnapshotStore;
use crate::unit_of_work::SqlxUnitOfWork;

/// TaroDomain's provider over SQL: the stores of `tarodomain_*` tables, and a
/// real transaction for a strong-mode command ([`SqlxUnitOfWork`]).
///
/// ```no_run
/// # async fn demo() -> tarodomain::error::Result<()> {
/// use tarodomain::adapters::Provider;
/// use tarodomain_sqlx::provider::SqlxProvider;
///
/// let pool = sqlx::SqlitePool::connect("sqlite://app.db?mode=rwc").await.unwrap();
/// let provider = SqlxProvider::sqlite(pool);
/// provider.initialize().await?; // runs the migrations
/// assert_eq!(provider.provider_type(), "sqlx");
/// # Ok(()) }
/// ```
///
/// A SQLite pool over an in-memory database must have one connection
/// (`max_connections(1)`), because each connection would see its own database.
/// Clones share the pool.
#[derive(Clone)]
pub struct SqlxProvider {
    db: Db,
    clock: Arc<dyn Clock>,
    wakeups: Arc<Notify>,
}

impl SqlxProvider {
    /// The in-process transport of [`EventBusConfig`], the strong default.
    pub const IN_MEMORY: &'static str = "in_memory";
    /// The transport that wakes the catch-up worker, the eventual default.
    pub const NOTIFY: &'static str = "notify";

    /// A provider over a SQLite pool.
    #[cfg(feature = "sqlite")]
    pub fn sqlite(pool: sqlx::SqlitePool) -> Self {
        Self::with_db(Db::sqlite(pool))
    }

    /// A provider over a PostgreSQL pool.
    #[cfg(feature = "postgres")]
    pub fn postgres(pool: sqlx::PgPool) -> Self {
        Self::with_db(Db::postgres(pool))
    }

    fn with_db(db: Db) -> Self {
        Self {
            db,
            clock: Arc::new(SystemClock),
            wakeups: Arc::new(Notify::new()),
        }
    }

    /// Uses `clock` for the stores' timestamps.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// The statement layer, for the application's own tables.
    ///
    /// A read model that writes through it joins a strong-mode command's
    /// transaction, so its writes commit or roll back with the events.
    pub fn db(&self) -> &Db {
        &self.db
    }

    /// What the eventual bus notifies when a command publishes events: a
    /// worker awaits it, then runs the catch-up job
    /// ([`run_job`](crate::jobs::run_job)). Clones of the provider share it.
    pub fn wakeups(&self) -> Arc<Notify> {
        Arc::clone(&self.wakeups)
    }

    /// Which database the provider talks to.
    pub fn dialect(&self) -> Dialect {
        self.db.dialect()
    }
}

#[async_trait]
impl Provider for SqlxProvider {
    fn provider_type(&self) -> &str {
        "sqlx"
    }

    fn create_event_store_adapter(&self) -> Arc<dyn EventStoreAdapter> {
        Arc::new(SqlxEventStoreAdapter::new(
            self.db.clone(),
            Arc::clone(&self.clock),
        ))
    }

    fn create_snapshot_store_adapter(&self) -> Arc<dyn SnapshotStoreAdapter> {
        Arc::new(SqlxSnapshotStore::new(
            self.db.clone(),
            Arc::clone(&self.clock),
        ))
    }

    fn create_saga_store_adapter(&self) -> Arc<dyn SagaStoreAdapter> {
        Arc::new(SqlxSagaStore::new(self.db.clone(), Arc::clone(&self.clock)))
    }

    fn create_processed_event_store(&self) -> Arc<dyn ProcessedEventStore> {
        Arc::new(SqlxProcessedEventStore::new(
            self.db.clone(),
            Arc::clone(&self.clock),
        ))
    }

    fn create_command_idempotency_store(&self) -> Arc<dyn CommandIdempotencyStore> {
        Arc::new(SqlxCommandIdempotencyStore::new(
            self.db.clone(),
            Arc::clone(&self.clock),
        ))
    }

    fn create_dead_letter_store(&self) -> Arc<dyn DeadLetterStore> {
        Arc::new(SqlxDeadLetterStore::new(
            self.db.clone(),
            Arc::clone(&self.clock),
        ))
    }

    /// Strong mode defaults to an in-process bus, which delivers inside the
    /// command's unit of work. Eventual mode defaults to [`SqlxEventBus`],
    /// which only wakes the catch-up worker, because the events are already
    /// stored. Transports `"in_memory"` and `"notify"` choose either; any
    /// other is `INVALID_CONFIGURATION`.
    fn create_event_bus(&self, config: &EventBusConfig) -> Result<Arc<dyn EventBus>> {
        let transport = match (config.transport.as_deref(), config.consistency) {
            (Some(transport), _) => transport,
            (None, ConsistencyMode::Strong) => Self::IN_MEMORY,
            (None, ConsistencyMode::Eventual) => Self::NOTIFY,
        };
        match transport {
            Self::IN_MEMORY => Ok(Arc::new(InMemoryEventBus::new(format!(
                "sqlx-{}",
                config.consistency
            )))),
            Self::NOTIFY => Ok(Arc::new(SqlxEventBus::new(
                self.db.clone(),
                Arc::clone(&self.wakeups),
            ))),
            other => Err(FrameworkError::invalid_configuration(format!(
                "the sqlx provider has no {other:?} transport, only {:?} and {:?}",
                Self::IN_MEMORY,
                Self::NOTIFY
            ))
            .with_context("transport", other)),
        }
    }

    /// A new [`SqlxUnitOfWork`] over the provider's pool.
    fn create_unit_of_work(&self) -> Option<Arc<dyn UnitOfWork>> {
        Some(Arc::new(SqlxUnitOfWork::new(self.db.clone())))
    }

    /// Applies the migrations that are missing.
    async fn initialize(&self) -> Result<()> {
        self.db.migrate().await
    }

    fn clock(&self) -> Arc<dyn Clock> {
        Arc::clone(&self.clock)
    }
}

impl fmt::Debug for SqlxProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SqlxProvider({:?})", self.dialect())
    }
}
