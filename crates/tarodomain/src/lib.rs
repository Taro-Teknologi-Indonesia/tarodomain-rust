//! TaroDomain for Rust: event sourcing, CQRS and DDD with strong or eventual
//! consistency per command.
//!
//! This crate implements the platform-agnostic parts of the
//! [TaroDomain specification](https://github.com/Taro-Teknologi-Indonesia/tarodomain)
//! (§2–23, §25, §28–29). It never depends on a database driver, an HTTP
//! framework or a cloud SDK: providers live in their own crates and implement
//! the persistence and event bus contracts defined here.
//!
//! # Example
//!
//! A counter: an event, an aggregate, a command with its handler, a
//! projection, and a framework built on the in-memory provider.
//!
//! ```
//! use std::sync::{Arc, Mutex};
//!
//! use serde::{Deserialize, Serialize};
//! use tarodomain::command::{CommandContext, CommandHandler};
//! use tarodomain::framework::TaroDomain;
//! use tarodomain::prelude::*;
//! use tarodomain::projection::{Projection, ProjectionRoutes};
//! use tarodomain_local::LocalProvider;
//!
//! aggregate_id!(pub CounterId);
//!
//! // An event is a plain struct with a stored name.
//! #[derive(Debug, Serialize, Deserialize)]
//! pub struct Incremented {
//!     pub by: i64,
//! }
//! impl Event for Incremented {
//!     const NAME: &'static str = "Incremented";
//! }
//! event_family!(pub enum CounterEvent { Incremented });
//!
//! // An aggregate: `apply` rebuilds state from events, and never fails.
//! pub struct Counter {
//!     id: CounterId,
//!     core: AggregateCore,
//!     total: i64,
//! }
//! impl Aggregate for Counter {
//!     type Id = CounterId;
//!     type Event = CounterEvent;
//!     type Snapshot = i64;
//!     const TYPE: &'static str = "Counter";
//!
//!     fn new(id: CounterId) -> Self {
//!         Self { id, core: AggregateCore::new(), total: 0 }
//!     }
//!     fn id(&self) -> CounterId { self.id }
//!     fn core(&self) -> &AggregateCore { &self.core }
//!     fn core_mut(&mut self) -> &mut AggregateCore { &mut self.core }
//!     fn apply(&mut self, event: &CounterEvent) {
//!         match event {
//!             CounterEvent::Incremented(e) => self.total += e.by,
//!         }
//!     }
//!     fn snapshot_state(&self) -> i64 { self.total }
//!     fn restore_snapshot_state(&mut self, total: i64) { self.total = total }
//! }
//!
//! // A command, and the handler that runs it.
//! #[derive(Clone, Debug, Serialize, Deserialize)]
//! pub struct Increment {
//!     pub counter: CounterId,
//!     pub by: i64,
//! }
//! impl Command for Increment {
//!     const NAME: &'static str = "Increment";
//! }
//!
//! struct IncrementHandler(CommandContext);
//!
//! #[async_trait]
//! impl CommandHandler<Increment> for IncrementHandler {
//!     async fn handle(&self, command: &CommandMessage<Increment>) -> Result<()> {
//!         let repository = self.0.repository::<Counter>()?;
//!         let mut counter = match repository.load_or_none(command.counter).await? {
//!             Some(counter) => counter,
//!             None => Counter::new(command.counter),
//!         };
//!         counter.raise(Incremented { by: command.by });
//!         repository.save(&mut counter).await
//!     }
//! }
//!
//! // A projection: a read model fed by events.
//! struct Total(Arc<Mutex<i64>>);
//!
//! impl Total {
//!     async fn on_incremented(self: Arc<Self>, event: Recorded<Incremented>) -> Result<()> {
//!         *self.0.lock().unwrap() += event.by;
//!         Ok(())
//!     }
//! }
//!
//! impl Projection for Total {
//!     fn name(&self) -> &str { "Total" }
//!     fn routes(routes: &mut ProjectionRoutes<Self>) {
//!         routes.on::<Incremented, _, _>(Self::on_incremented);
//!     }
//! }
//!
//! # tokio_test_block(async {
//! let total = Arc::new(Mutex::new(0));
//! let read_model = total.clone();
//! let app = TaroDomain::builder()
//!     .use_provider(Arc::new(LocalProvider::new()))
//!     .consistency(ConsistencyMode::Strong, None)
//!     .aggregate::<Counter>(|a| a.event_sourced())
//!     .command::<Increment, _>(|_, context| IncrementHandler(context), None)
//!     .projection(move |_| Ok(Arc::new(Total(read_model))))
//!     .build()
//!     .await?;
//!
//! let counter = CounterId::generate();
//! app.dispatch(Increment { counter, by: 2 }).await?;
//! app.dispatch(Increment { counter, by: 3 }).await?;
//! assert_eq!(*total.lock().unwrap(), 5);
//! # Ok::<(), FrameworkError>(())
//! # }).unwrap();
//! # fn tokio_test_block<F: std::future::Future<Output = Result<()>>>(f: F) -> Result<()> {
//! #     tokio::runtime::Builder::new_current_thread().build().unwrap().block_on(f)
//! # }
//! ```
//!
//! The guides in the repository's `docs/` folder walk through the same pieces
//! in detail: `GETTING_STARTED.md` for the domain, and `CLOUDFLARE.md` for
//! deploying it. `docs/IMPLEMENTATION_PLAN.md` holds the roadmap.

pub mod adapters;
pub mod bus;
pub mod catch_up;
pub mod command;
pub mod dead_letter;
pub mod di;
pub mod domain;
pub mod error;
pub mod event_sourcing;
pub mod framework;
pub mod kernel;
pub mod projection;
pub mod rebuild;
pub mod repository;
pub mod saga;
pub mod trace;

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
