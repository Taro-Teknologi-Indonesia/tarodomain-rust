//! The projection trait, its route table and its event handler (spec §12.1–12.2).

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::adapters::ProcessedEventStore;
use crate::async_trait;
use crate::bus::EventHandler;
use crate::domain::{DomainEvent, Event, Recorded};
use crate::error::{FrameworkError, Result};

type RouteFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type Route<P> = Arc<dyn Fn(Arc<P>, DomainEvent) -> RouteFuture + Send + Sync>;

/// Decides whether a shard handler takes an event.
pub(crate) type EventFilter = Arc<dyn Fn(&DomainEvent) -> bool + Send + Sync>;

/// Called with the projection's name and an event it has no route for.
pub type UnhandledHook = Arc<dyn Fn(&str, &DomainEvent) + Send + Sync>;

/// A read model builder (spec §12.1).
///
/// The projection owns its storage; the framework never touches the read
/// model. Handlers must be idempotent in their own writes where the storage
/// allows it (§14.3), because a crash between the write and the processed
/// mark replays the event.
#[async_trait]
pub trait Projection: Send + Sync + Sized + 'static {
    /// The projection's unique name. Processed-event records and
    /// subscriptions are filed under it.
    fn name(&self) -> &str;

    /// Registers the events this projection handles (spec §12.2):
    /// `r.on::<MoneyDeposited>(Self::on_money_deposited)`.
    fn routes(routes: &mut ProjectionRoutes<Self>);

    /// The handler instance under which the event is recorded as processed.
    /// Defaults to the projection's name.
    fn instance_id(&self, _event: &DomainEvent) -> String {
        self.name().to_owned()
    }

    /// Called before a rebuild replays events, e.g. to drop tables.
    async fn on_rebuild_start(&self) -> Result<()> {
        Ok(())
    }

    /// Called after a rebuild has replayed all events, e.g. to create indexes.
    async fn on_rebuild_complete(&self) -> Result<()> {
        Ok(())
    }
}

/// The table of events a projection handles, built by
/// [`Projection::routes`].
pub struct ProjectionRoutes<P> {
    routes: BTreeMap<&'static str, Route<P>>,
    error: Option<FrameworkError>,
}

impl<P: Send + Sync + 'static> ProjectionRoutes<P> {
    pub(crate) fn new() -> Self {
        Self {
            routes: BTreeMap::new(),
            error: None,
        }
    }

    /// Handles events of type `E` with `handler`.
    ///
    /// A second route for the same event is `INVALID_CONFIGURATION`, reported
    /// when the [`ProjectionHandler`] is created.
    pub fn on<E, F, Fut>(&mut self, handler: F) -> &mut Self
    where
        E: Event,
        F: Fn(Arc<P>, Recorded<E>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        if self.routes.contains_key(E::NAME) {
            self.error.get_or_insert_with(|| {
                FrameworkError::invalid_configuration(format!(
                    "a projection routes {} twice",
                    E::NAME
                ))
            });
            return self;
        }
        let route: Route<P> = Arc::new(move |projection, event| match event.downcast::<E>() {
            Some(recorded) => Box::pin(handler(projection, recorded)),
            None => {
                let error = FrameworkError::invalid_configuration(format!(
                    "the route of {} was given a {}",
                    E::NAME,
                    event.name()
                ));
                Box::pin(async move { Err(error) })
            }
        });
        self.routes.insert(E::NAME, route);
        self
    }

    /// The names of the handled events, alphabetical.
    pub fn names(&self) -> Vec<&'static str> {
        self.routes.keys().copied().collect()
    }
}

impl<P> fmt::Debug for ProjectionRoutes<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionRoutes")
            .field("events", &self.routes.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Runs a [`Projection`] as an [`EventHandler`] (spec §12.1):
///
/// 1. skip an event the instance has already processed;
/// 2. route it by name;
/// 3. an event without a route is marked processed (so it isn't retried) and
///    reported to the [`on_unhandled`](Self::on_unhandled) hook;
/// 4. after the handler succeeds, mark the event processed. A failed event
///    is **not** marked, so redelivery runs it again.
pub struct ProjectionHandler<P: Projection> {
    projection: Arc<P>,
    routes: BTreeMap<&'static str, Route<P>>,
    processed: Arc<dyn ProcessedEventStore>,
    handler_name: String,
    fixed_instance: Option<String>,
    filter: Option<EventFilter>,
    on_unhandled: Option<UnhandledHook>,
}

impl<P: Projection> ProjectionHandler<P> {
    /// Builds the handler from the projection's routes. Fails with
    /// `INVALID_CONFIGURATION` if two routes share an event.
    pub fn new(projection: Arc<P>, processed: Arc<dyn ProcessedEventStore>) -> Result<Self> {
        let mut routes = ProjectionRoutes::new();
        P::routes(&mut routes);
        if let Some(error) = routes.error {
            return Err(error.with_context("projection", projection.name().to_owned()));
        }
        Ok(Self {
            handler_name: projection.name().to_owned(),
            projection,
            routes: routes.routes,
            processed,
            fixed_instance: None,
            filter: None,
            on_unhandled: None,
        })
    }

    /// Sets the hook called for events without a route (for the dispatch
    /// guard, §29.2).
    pub fn on_unhandled(mut self, hook: UnhandledHook) -> Self {
        self.on_unhandled = Some(hook);
        self
    }

    /// Restricts the handler to one shard: its name is the one given, events
    /// are recorded under the projection's name with `instance` as the
    /// instance, and events failing `accepts` are ignored without a mark.
    pub(crate) fn for_shard(
        mut self,
        handler_name: String,
        instance: String,
        accepts: EventFilter,
    ) -> Self {
        self.handler_name = handler_name;
        self.fixed_instance = Some(instance);
        self.filter = Some(accepts);
        self
    }

    /// The projection this handler runs.
    pub fn projection(&self) -> &Arc<P> {
        &self.projection
    }

    /// The names of the events the projection handles.
    pub fn handled_events(&self) -> Vec<&'static str> {
        self.routes.keys().copied().collect()
    }

    /// The name processed-event records are filed under: the projection's.
    pub fn record_name(&self) -> &str {
        self.projection.name()
    }

    fn instance(&self, event: &DomainEvent) -> String {
        match &self.fixed_instance {
            Some(instance) => instance.clone(),
            None => self.projection.instance_id(event),
        }
    }
}

impl<P: Projection> fmt::Debug for ProjectionHandler<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionHandler")
            .field("name", &self.handler_name)
            .field("events", &self.routes.keys().collect::<Vec<_>>())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<P: Projection> EventHandler for ProjectionHandler<P> {
    fn name(&self) -> &str {
        &self.handler_name
    }

    async fn handle(&self, event: &DomainEvent) -> Result<()> {
        if let Some(accepts) = &self.filter
            && !accepts(event)
        {
            return Ok(());
        }
        let record = self.projection.name();
        let instance = self.instance(event);
        if self.processed.exists(record, &instance, event.id()).await? {
            return Ok(());
        }
        match self.routes.get(event.name()) {
            Some(route) => route(self.projection.clone(), event.clone()).await?,
            None => {
                if let Some(hook) = &self.on_unhandled {
                    hook(record, event);
                }
            }
        }
        self.processed
            .mark_processed(record, &instance, event.id())
            .await
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::collections::HashSet;
    use std::sync::Mutex;

    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Serialize};
    use uuid::Uuid;

    use super::*;
    use crate::domain::EventEnvelope;
    use crate::kernel::ids::new_v7;

    #[derive(Default)]
    pub(crate) struct FakeProcessed(Mutex<HashSet<(String, String, Uuid)>>);

    impl FakeProcessed {
        pub(crate) fn len(&self) -> usize {
            self.0.lock().unwrap().len()
        }
        pub(crate) fn has(&self, name: &str, instance: &str, id: Uuid) -> bool {
            self.0
                .lock()
                .unwrap()
                .contains(&(name.into(), instance.into(), id))
        }
    }

    #[async_trait]
    impl ProcessedEventStore for FakeProcessed {
        async fn exists(&self, n: &str, i: &str, id: Uuid) -> Result<bool> {
            Ok(self.has(n, i, id))
        }
        async fn mark_processed(&self, n: &str, i: &str, id: Uuid) -> Result<()> {
            self.0.lock().unwrap().insert((n.into(), i.into(), id));
            Ok(())
        }
        async fn count(&self, _: &str, _: Option<&str>) -> Result<u64> {
            Ok(self.len() as u64)
        }
        async fn delete_by_handler(&self, _: &str, _: Option<&str>) -> Result<u64> {
            Ok(0)
        }
        async fn delete_old(&self, _: DateTime<Utc>) -> Result<u64> {
            Ok(0)
        }
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct Deposited {
        pub(crate) amount: i64,
    }
    impl Event for Deposited {
        const NAME: &'static str = "Deposited";
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub(crate) struct Closed;
    impl Event for Closed {
        const NAME: &'static str = "Closed";
    }

    pub(crate) fn event<E: Event>(e: E) -> DomainEvent {
        DomainEvent::new(EventEnvelope::new(Utc::now()), e)
    }

    #[derive(Default)]
    pub(crate) struct Balance {
        pub(crate) total: Mutex<i64>,
        pub(crate) fail: Mutex<bool>,
    }

    impl Balance {
        async fn on_deposited(self: Arc<Self>, e: Recorded<Deposited>) -> Result<()> {
            if *self.fail.lock().unwrap() {
                return Err(FrameworkError::custom(
                    "DOWN",
                    crate::error::ErrorCategory::Infrastructure,
                    "down",
                ));
            }
            *self.total.lock().unwrap() += e.amount;
            Ok(())
        }
    }

    #[async_trait]
    impl Projection for Balance {
        fn name(&self) -> &str {
            "balance"
        }
        fn routes(r: &mut ProjectionRoutes<Self>) {
            r.on::<Deposited, _, _>(Self::on_deposited);
        }
    }

    fn handler(p: &Arc<Balance>, store: &Arc<FakeProcessed>) -> ProjectionHandler<Balance> {
        ProjectionHandler::new(p.clone(), store.clone()).unwrap()
    }

    #[tokio::test]
    async fn a_redelivered_event_is_skipped() {
        let (p, store) = (
            Arc::new(Balance::default()),
            Arc::new(FakeProcessed::default()),
        );
        let h = handler(&p, &store);
        let e = event(Deposited { amount: 5 });
        h.handle(&e).await.unwrap();
        h.handle(&e).await.unwrap();
        assert_eq!(*p.total.lock().unwrap(), 5);
        assert!(store.has("balance", "balance", e.id()));
    }

    #[tokio::test]
    async fn an_unknown_event_is_marked_and_reported() {
        let (p, store) = (
            Arc::new(Balance::default()),
            Arc::new(FakeProcessed::default()),
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let h = handler(&p, &store).on_unhandled(Arc::new(move |name, e| {
            sink.lock().unwrap().push((name.to_owned(), e.name()));
        }));
        let e = event(Closed);
        h.handle(&e).await.unwrap();
        assert!(store.has("balance", "balance", e.id()));
        assert_eq!(*seen.lock().unwrap(), [("balance".to_owned(), "Closed")]);
    }

    #[tokio::test]
    async fn a_failed_event_is_not_marked_and_runs_again() {
        let (p, store) = (
            Arc::new(Balance::default()),
            Arc::new(FakeProcessed::default()),
        );
        let h = handler(&p, &store);
        let e = event(Deposited { amount: 7 });
        *p.fail.lock().unwrap() = true;
        assert!(h.handle(&e).await.is_err());
        assert_eq!(store.len(), 0);
        *p.fail.lock().unwrap() = false;
        h.handle(&e).await.unwrap();
        assert_eq!(*p.total.lock().unwrap(), 7);
    }

    struct Twice;
    #[async_trait]
    impl Projection for Twice {
        fn name(&self) -> &str {
            "twice"
        }
        fn routes(r: &mut ProjectionRoutes<Self>) {
            r.on::<Closed, _, _>(|_, _| async { Ok(()) });
            r.on::<Closed, _, _>(|_, _| async { Ok(()) });
        }
    }

    #[test]
    fn a_second_route_for_an_event_is_invalid_configuration() {
        let err = ProjectionHandler::new(Arc::new(Twice), Arc::new(FakeProcessed::default()))
            .unwrap_err();
        assert_eq!(err.code().as_str(), "INVALID_CONFIGURATION");
        assert!(err.message().contains("Closed"));
        let _ = new_v7();
    }
}
