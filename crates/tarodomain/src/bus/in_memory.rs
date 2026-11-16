//! The in-process event bus (spec §10.3–10.4).

use std::fmt;
use std::sync::{Arc, PoisonError, RwLock};

use super::{EventBus, EventHandler, SubscribableEventBus};
use crate::async_trait;
use crate::domain::DomainEvent;
use crate::error::{FrameworkError, Result};
use crate::kernel::ids::Uuid;

/// One handler's outcome for one event, from
/// [`InMemoryEventBus::publish_all_with_results`].
#[derive(Debug)]
pub struct Delivery {
    /// The handler's name.
    pub handler_name: String,
    /// The event's ID.
    pub event_id: Uuid,
    /// What the handler returned.
    pub outcome: Result<()>,
}

/// Delivers events in process, to the subscribed handlers (spec §10.4).
///
/// Every event goes to every handler, in subscription order, one event after
/// the other: all handlers see the first event before any sees the second.
/// Each handler finishes before the next starts. The strong pipeline uses it
/// so that projections and sagas run inside the command's unit of work; the
/// queue consumer uses it to deliver what it receives.
///
/// The list of handlers is read when publishing starts, and no lock is held
/// while a handler runs, so a handler may publish to the same bus.
pub struct InMemoryEventBus {
    name: String,
    handlers: RwLock<Vec<Arc<dyn EventHandler>>>,
}

impl InMemoryEventBus {
    /// An empty bus with this name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            handlers: RwLock::default(),
        }
    }

    /// The subscribed handlers' names, in subscription order.
    pub fn handler_names(&self) -> Vec<String> {
        let handlers = self.handlers.read().unwrap_or_else(PoisonError::into_inner);
        handlers.iter().map(|h| h.name().to_owned()).collect()
    }

    /// Delivers every event to every handler like
    /// [`publish_all`](EventBus::publish_all), but carries on after a
    /// failure, and returns each handler's outcome for each event, in
    /// delivery order.
    pub async fn publish_all_with_results(&self, events: &[DomainEvent]) -> Vec<Delivery> {
        let handlers = self.snapshot();
        let mut deliveries = Vec::with_capacity(events.len() * handlers.len());
        for event in events {
            for handler in &handlers {
                deliveries.push(Delivery {
                    handler_name: handler.name().to_owned(),
                    event_id: event.id(),
                    outcome: handler.handle(event).await,
                });
            }
        }
        deliveries
    }

    /// The handlers now, so none is locked while it runs.
    fn snapshot(&self) -> Vec<Arc<dyn EventHandler>> {
        self.handlers
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl fmt::Debug for InMemoryEventBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InMemoryEventBus")
            .field("name", &self.name)
            .field("handlers", &self.handler_names())
            .finish()
    }
}

#[async_trait]
impl EventBus for InMemoryEventBus {
    fn name(&self) -> &str {
        &self.name
    }

    /// Delivers every event to every handler, and stops at the first
    /// failure: later handlers don't see that event, and nothing sees the
    /// events after it.
    ///
    /// The handler's error is returned unchanged, so a conflict stays
    /// retryable and the strong pipeline retries the command (§11.1).
    async fn publish_all(&self, events: &[DomainEvent]) -> Result<()> {
        let handlers = self.snapshot();
        for event in events {
            for handler in &handlers {
                handler.handle(event).await?;
            }
        }
        Ok(())
    }

    fn as_subscribable(&self) -> Option<&dyn SubscribableEventBus> {
        Some(self)
    }
}

impl SubscribableEventBus for InMemoryEventBus {
    fn subscribe(&self, handler: Arc<dyn EventHandler>) -> Result<()> {
        let mut handlers = self
            .handlers
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        match handlers.iter().find(|h| h.name() == handler.name()) {
            Some(existing) if Arc::ptr_eq(existing, &handler) => Ok(()),
            Some(_) => Err(FrameworkError::invalid_configuration(format!(
                "a different handler named {:?} is already subscribed to the {} bus",
                handler.name(),
                self.name
            ))
            .with_context("handler", handler.name())
            .with_context("bus", self.name.as_str())),
            None => {
                handlers.push(handler);
                Ok(())
            }
        }
    }

    fn unsubscribe(&self, name: &str) -> bool {
        let mut handlers = self
            .handlers
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let before = handlers.len();
        handlers.retain(|h| h.name() != name);
        handlers.len() != before
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use chrono::DateTime;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::domain::{Event, EventEnvelope};
    use crate::error::ErrorCode;

    #[derive(Debug, Serialize, Deserialize)]
    struct Pinged {
        n: u32,
    }

    impl Event for Pinged {
        const NAME: &'static str = "Pinged";
    }

    fn event(n: u32) -> DomainEvent {
        DomainEvent::new(EventEnvelope::new(DateTime::UNIX_EPOCH), Pinged { n })
    }

    type Log = Arc<Mutex<Vec<String>>>;

    /// Logs `<name> <n>` for each event, and fails on event `fail_on`.
    struct Logging {
        name: &'static str,
        log: Log,
        fail_on: Option<u32>,
    }

    impl Logging {
        fn handler(name: &'static str, log: &Log, fail_on: Option<u32>) -> Arc<dyn EventHandler> {
            Arc::new(Self {
                name,
                log: Arc::clone(log),
                fail_on,
            })
        }
    }

    #[async_trait]
    impl EventHandler for Logging {
        fn name(&self) -> &str {
            self.name
        }

        async fn handle(&self, event: &DomainEvent) -> Result<()> {
            let n = event.downcast_ref::<Pinged>().unwrap().n;
            self.log.lock().unwrap().push(format!("{} {n}", self.name));
            if self.fail_on == Some(n) {
                return Err(FrameworkError::optimistic_concurrency("Ping-1", 1, 2));
            }
            Ok(())
        }
    }

    fn entries(log: &Log) -> Vec<String> {
        log.lock().unwrap().clone()
    }

    fn bus_with(handlers: &[Arc<dyn EventHandler>]) -> InMemoryEventBus {
        let bus = InMemoryEventBus::new("strong");
        for handler in handlers {
            bus.subscribe(Arc::clone(handler)).unwrap();
        }
        bus
    }

    #[tokio::test]
    async fn each_event_reaches_every_handler_in_subscription_order() {
        let log = Log::default();
        let bus = bus_with(&[
            Logging::handler("a", &log, None),
            Logging::handler("b", &log, None),
        ]);
        bus.publish_all(&[event(1), event(2)]).await.unwrap();
        assert_eq!(entries(&log), ["a 1", "b 1", "a 2", "b 2"]);

        bus.publish(&event(3)).await.unwrap();
        assert_eq!(entries(&log)[4..], ["a 3", "b 3"]);
    }

    #[tokio::test]
    async fn the_first_failure_stops_delivery_and_is_returned_unchanged() {
        let log = Log::default();
        let bus = bus_with(&[
            Logging::handler("a", &log, Some(1)),
            Logging::handler("b", &log, None),
        ]);
        let error = bus.publish_all(&[event(1), event(2)]).await.unwrap_err();
        assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
        assert!(error.retryable());
        assert_eq!(entries(&log), ["a 1"]);
    }

    #[tokio::test]
    async fn with_results_carries_on_and_reports_every_delivery() {
        let log = Log::default();
        let bus = bus_with(&[
            Logging::handler("a", &log, Some(1)),
            Logging::handler("b", &log, None),
        ]);
        let events = [event(1), event(2)];
        let deliveries = bus.publish_all_with_results(&events).await;
        assert_eq!(entries(&log), ["a 1", "b 1", "a 2", "b 2"]);
        let summary: Vec<_> = deliveries
            .iter()
            .map(|d| (d.handler_name.as_str(), d.event_id, d.outcome.is_ok()))
            .collect();
        let (one, two) = (events[0].id(), events[1].id());
        assert_eq!(
            summary,
            [
                ("a", one, false),
                ("b", one, true),
                ("a", two, true),
                ("b", two, true)
            ]
        );
    }

    #[tokio::test]
    async fn subscribing_the_same_handler_twice_delivers_once() {
        let log = Log::default();
        let handler = Logging::handler("a", &log, None);
        let bus = bus_with(&[Arc::clone(&handler), handler]);
        assert_eq!(bus.handler_names(), ["a"]);
        bus.publish(&event(1)).await.unwrap();
        assert_eq!(entries(&log), ["a 1"]);
    }

    #[tokio::test]
    async fn handlers_subscribe_through_a_plain_event_bus() {
        let log = Log::default();
        let bus: Arc<dyn EventBus> = Arc::new(InMemoryEventBus::new("eventual"));
        let subscribable = bus
            .as_subscribable()
            .expect("the in-memory bus is subscribable");
        subscribable
            .subscribe(Logging::handler("a", &log, None))
            .unwrap();
        bus.publish(&event(1)).await.unwrap();
        assert_eq!(entries(&log), ["a 1"]);
    }

    #[test]
    fn another_handler_with_a_taken_name_is_rejected() {
        let log = Log::default();
        let bus = bus_with(&[Logging::handler("a", &log, None)]);
        let error = bus
            .subscribe(Logging::handler("a", &log, None))
            .unwrap_err();
        assert_eq!(*error.code(), ErrorCode::InvalidConfiguration);
        assert_eq!(error.context()["handler"], "a");
        assert_eq!(error.context()["bus"], "strong");
        assert_eq!(bus.handler_names(), ["a"]);
    }

    #[tokio::test]
    async fn unsubscribe_removes_by_name() {
        let log = Log::default();
        let bus = bus_with(&[
            Logging::handler("a", &log, None),
            Logging::handler("b", &log, None),
        ]);
        assert!(bus.unsubscribe("a"));
        assert!(!bus.unsubscribe("a"));
        bus.publish(&event(1)).await.unwrap();
        assert_eq!(entries(&log), ["b 1"]);
    }

    /// Publishes a follow-up event to the bus it is subscribed to.
    struct Echo {
        bus: Mutex<Option<Arc<InMemoryEventBus>>>,
    }

    #[async_trait]
    impl EventHandler for Echo {
        fn name(&self) -> &str {
            "echo"
        }

        async fn handle(&self, received: &DomainEvent) -> Result<()> {
            let n = received.downcast_ref::<Pinged>().unwrap().n;
            let bus = self.bus.lock().unwrap().clone().unwrap();
            if n == 1 {
                bus.publish(&event(2)).await?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_handler_may_publish_to_its_own_bus() {
        let log = Log::default();
        let bus = Arc::new(InMemoryEventBus::new("strong"));
        let echo = Arc::new(Echo {
            bus: Mutex::new(Some(Arc::clone(&bus))),
        });
        bus.subscribe(echo.clone()).unwrap();
        bus.subscribe(Logging::handler("log", &log, None)).unwrap();
        bus.publish(&event(1)).await.unwrap();
        assert_eq!(entries(&log), ["log 2", "log 1"]);
        echo.bus.lock().unwrap().take();
    }
}
