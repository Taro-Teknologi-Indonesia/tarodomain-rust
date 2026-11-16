//! The TaroDomain LocalProvider (spec §24.2): every store in memory, for
//! tests and local development.
//!
//! [`LocalProvider`] hands out one adapter per store, all keeping their rows
//! in one [`LocalDatabase`], so an [`InMemoryUnitOfWork`] covers them all.
//! Nothing is persisted: the data lives as long as the database does.

mod database;
mod dead_letter;
mod event_store;
mod idempotency;
mod provider;
mod saga_store;
mod snapshot_store;
mod unit_of_work;

pub use database::{LocalDatabase, Table};
pub use dead_letter::InMemoryDeadLetterStore;
pub use event_store::InMemoryEventStoreAdapter;
pub use idempotency::{InMemoryCommandIdempotencyStore, InMemoryProcessedEventStore};
pub use provider::LocalProvider;
pub use saga_store::InMemorySagaStore;
pub use snapshot_store::InMemorySnapshotStore;
pub use unit_of_work::InMemoryUnitOfWork;
