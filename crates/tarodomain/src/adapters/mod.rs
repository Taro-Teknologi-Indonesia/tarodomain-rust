//! The persistence layer's contracts (spec §3.2, §8–9, §11.4, §12.3, §14,
//! §21.1, §24.1).
//!
//! Each trait here is one store a provider implements, such as
//! [`EventStoreAdapter`]. Adapters speak the storable forms
//! ([`SerializedEvent`](crate::domain::SerializedEvent)), never typed events:
//! the framework's wrappers convert. A provider hands its adapters out as
//! `Arc<dyn Trait>`, so every method returns a boxed `Send` future (written
//! with [`async_trait`](crate::async_trait)).
//!
//! The rustdoc on each method is the contract. The suites in
//! `tarodomain-testing` check it, and every provider must pass them.

mod dead_letter;
mod event_store;
mod idempotency;
mod provider;
mod saga_store;
mod snapshot_store;
mod unit_of_work;

pub use dead_letter::{DeadLetterEntry, DeadLetterInput, DeadLetterStore};
pub use event_store::{
    AppendResult, EventStoreAdapter, StreamIds, StreamKind, StreamMetadata, Subscription,
};
pub use idempotency::{CommandIdempotencyStore, ProcessedCommand, ProcessedEventStore};
pub use provider::{ConsistencyMode, EventBusConfig, Provider};
pub use saga_store::{SagaQuery, SagaRecord, SagaSaveInput, SagaStatus, SagaStoreAdapter};
pub use snapshot_store::{Snapshot, SnapshotInput, SnapshotStoreAdapter};
pub use unit_of_work::{AsAny, UnitOfWork};
