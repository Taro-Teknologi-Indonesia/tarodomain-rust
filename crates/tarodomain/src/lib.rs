//! TaroDomain for Rust: event sourcing, CQRS and DDD with strong or eventual
//! consistency per command.
//!
//! This crate implements the platform-agnostic parts of the
//! [TaroDomain specification](https://github.com/Taro-Teknologi-Indonesia/tarodomain)
//! (§2–23, §25, §28–29). It never depends on a database driver, an HTTP
//! framework or a cloud SDK: providers live in their own crates and implement
//! the persistence and event bus contracts defined here.
//!
//! See `docs/IMPLEMENTATION_PLAN.md` in the repository for the roadmap.

pub mod adapters;
pub mod bus;
pub mod command;
pub mod domain;
pub mod error;
pub mod event_sourcing;
pub mod kernel;
pub mod projection;
pub mod repository;
pub mod saga;

/// The attribute the framework's async traits use, re-exported so that a crate
/// implementing an adapter or handler needs no `async-trait` dependency of its
/// own: put `#[tarodomain::async_trait]` on the `impl` block.
pub use async_trait::async_trait;

/// What a domain crate needs, in one import: `use tarodomain::prelude::*;`.
///
/// It brings in the domain building blocks (events, event families,
/// aggregates, IDs, value objects, commands), the error types, the macros
/// that generate the repetitive parts, the event handler and bus traits,
/// [`ConsistencyMode`](adapters::ConsistencyMode) and
/// [`async_trait`](crate::async_trait). [`Result`](error::Result) defaults
/// its error to [`FrameworkError`](error::FrameworkError), so
/// `Result<T, OtherError>` still works.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use tarodomain::prelude::*;
///
/// aggregate_id!(pub AccountId);
///
/// #[derive(Debug, Serialize, Deserialize)]
/// pub struct AccountOpened {
///     pub owner: String,
/// }
///
/// impl Event for AccountOpened {
///     const NAME: &'static str = "AccountOpened";
/// }
///
/// event_family!(pub enum AccountEvent { AccountOpened });
///
/// fn open(owner: &str) -> Result<AccountEvent> {
///     if owner.is_empty() {
///         let category = ErrorCategory::Domain;
///         return Err(FrameworkError::custom("OWNER_REQUIRED", category, "no owner"));
///     }
///     Ok(AccountOpened { owner: owner.into() }.into())
/// }
///
/// assert_eq!(AccountEvent::names(), ["AccountOpened"]);
/// assert!(open("").is_err());
/// assert!(AccountId::from_existing("not-a-uuid").is_err());
/// let _mode = ConsistencyMode::Strong;
/// ```
pub mod prelude {
    pub use crate::adapters::ConsistencyMode;
    pub use crate::async_trait;
    pub use crate::bus::{EventBus, EventHandler};
    pub use crate::domain::{
        Aggregate, AggregateCore, AggregateId, AnyCommand, Command, CommandMessage, DomainEvent,
        Event, EventEnvelope, EventFamily, ExecutionContext, Metadata, Recorded, ValueObject,
    };
    pub use crate::error::{BoxError, ErrorCategory, ErrorCode, FrameworkError, Result};
    pub use crate::{aggregate_id, event_family};
}

/// Dependencies the exported macros use, so that crates calling them don't
/// have to depend on them too. Not part of the public API.
#[doc(hidden)]
pub mod __private {
    pub use serde;
}
