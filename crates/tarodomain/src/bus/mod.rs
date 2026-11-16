//! The event bus layer (spec §10): how events reach projections, sagas and
//! other handlers.
//!
//! [`EventBus`] publishes, [`EventHandler`] receives, and
//! [`InMemoryEventBus`] connects them in process. Providers add the
//! transports of eventual mode, such as a queue.

mod contracts;
mod in_memory;

pub use contracts::{EventBus, EventHandler, SubscribableEventBus};
pub use in_memory::{Delivery, InMemoryEventBus};
