//! The event bus contracts (spec §10.1–10.2, §10.4).

use std::slice;
use std::sync::Arc;

use crate::async_trait;
use crate::domain::DomainEvent;
use crate::error::Result;

/// Something that reacts to events (spec §10.2): a projection, a saga, or
/// any other subscriber.
///
/// Delivery is at least once, so [`handle`](Self::handle) must be
/// idempotent: the same event may arrive again after a retry, a redelivered
/// queue message or a catch-up run.
#[async_trait]
pub trait EventHandler: Send + Sync {
    /// The handler's name, unique on a bus. Subscriptions, idempotency
    /// records and dead letters are filed under it.
    fn name(&self) -> &str;

    /// Handles one event.
    async fn handle(&self, event: &DomainEvent) -> Result<()>;
}

/// Delivers events to handlers (spec §10.1).
///
/// The strong bus delivers in process before the command returns; an
/// eventual bus hands the events to a transport, such as a queue, and the
/// consumer delivers them later.
#[async_trait]
pub trait EventBus: Send + Sync {
    /// The bus's name, for logs.
    fn name(&self) -> &str;

    /// Publishes events, in order.
    async fn publish_all(&self, events: &[DomainEvent]) -> Result<()>;

    /// Publishes one event. Defaults to [`publish_all`](Self::publish_all)
    /// with just that event.
    async fn publish(&self, event: &DomainEvent) -> Result<()> {
        self.publish_all(slice::from_ref(event)).await
    }

    /// The bus as a [`SubscribableEventBus`], if handlers can subscribe to it
    /// directly. `None` by default, as for a bus that hands events to a
    /// queue.
    ///
    /// A provider returns its buses as `Arc<dyn EventBus>`, and this is how
    /// the framework finds out whether to subscribe handlers to one (trait
    /// upcasting needs a newer Rust than the crate's minimum).
    fn as_subscribable(&self) -> Option<&dyn SubscribableEventBus> {
        None
    }
}

/// A bus that handlers subscribe to directly (spec §10.4).
///
/// Subscriptions are usually made while the framework is built, before
/// anything is published.
pub trait SubscribableEventBus: EventBus {
    /// Adds a handler after the ones already subscribed.
    ///
    /// Subscribing the same handler (the same `Arc`) again does nothing.
    /// Fails with `INVALID_CONFIGURATION` when another handler already has
    /// the name.
    fn subscribe(&self, handler: Arc<dyn EventHandler>) -> Result<()>;

    /// Removes the handler with this name, and returns whether there was
    /// one. Names are unique on a bus, so the name identifies the handler.
    fn unsubscribe(&self, name: &str) -> bool;
}
