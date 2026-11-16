//! The cron dispatcher (spec §18.3): pull-based catch-up.

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};

use super::SubscriptionManager;
use crate::adapters::{DeadLetterInput, DeadLetterStore, Provider, UnitOfWork};
use crate::bus::EventHandler;
use crate::domain::SerializedEvent;
use crate::error::{FrameworkError, Result};
use crate::event_sourcing::EventStore;
use crate::kernel::ids::Uuid;

/// The default number of events read per [`catch_up`](CronDispatcher::catch_up)
/// run.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// The event that stopped a catch-up run.
#[derive(Debug)]
pub struct CatchUpFailure {
    /// The event that failed.
    pub event_id: Uuid,
    /// How many times it has failed since the cursor last moved.
    pub attempts: u32,
    /// The handler's error, or the `DEAD_LETTER` error if the event had to
    /// be dead-lettered and that failed.
    pub error: FrameworkError,
}

/// What one [`catch_up`](CronDispatcher::catch_up) run did.
#[derive(Debug, Default)]
pub struct CatchUpResult {
    /// Events the handler handled and the cursor moved past.
    pub processed: usize,
    /// Events that were dead-lettered, with the cursor moved past them: a
    /// non-retryable failure, an unreadable event, or `max_attempts` reached.
    pub dead_lettered: usize,
    /// Whether more events were waiting when the batch ended. `false` when
    /// the run stopped on a failure, since it should wait for the next run.
    pub has_more: bool,
    /// The retryable failure that stopped the run, if one did. The cursor
    /// stays on the failed event.
    pub failed: Option<CatchUpFailure>,
}

struct Registration {
    handler: Arc<dyn EventHandler>,
    stream_id: Option<String>,
}

/// Delivers stored events to handlers that missed them (spec §18.3).
///
/// For each registered handler, a run reads the batch of events after the
/// subscription's cursor, hands each to the handler and moves the cursor
/// past it:
///
/// - A **retryable** error stops the run on that event, and the next run
///   tries it again. The attempt is counted in the subscription
///   ([`Subscription::failed_attempts`](crate::adapters::Subscription)).
///   With [`max_attempts`](Self::with_max_attempts) set, the event is
///   dead-lettered once it has failed that many times, and the cursor moves
///   on; by default a failing event blocks its subscription forever.
/// - A **non-retryable** error dead-letters the event at once and moves on.
/// - An event the registry **can't read** is treated the same way, whatever
///   the error's category: reading it again wouldn't help. Events are read
///   raw and deserialized one at a time for this reason.
///
/// With [`with_units_of_work`](Self::with_units_of_work), the handler and
/// the cursor move (or the dead letter and the cursor move) commit together,
/// so a handler that writes through the provider can't be applied twice.
/// Without one, a crash between the handler and the ack redelivers the
/// event, which handlers must tolerate anyway (delivery is at least once).
pub struct CronDispatcher {
    events: EventStore,
    subscriptions: Arc<SubscriptionManager>,
    dead_letters: Arc<dyn DeadLetterStore>,
    handlers: Mutex<BTreeMap<String, Arc<Registration>>>,
    batch_size: usize,
    max_attempts: Option<u32>,
    units: Option<Arc<dyn Provider>>,
}

impl CronDispatcher {
    /// A dispatcher that reads through `events` (and its adapter), tracks
    /// cursors in `subscriptions` and writes failures to `dead_letters`.
    pub fn new(
        events: EventStore,
        subscriptions: Arc<SubscriptionManager>,
        dead_letters: Arc<dyn DeadLetterStore>,
    ) -> Self {
        Self {
            events,
            subscriptions,
            dead_letters,
            handlers: Mutex::new(BTreeMap::new()),
            batch_size: DEFAULT_BATCH_SIZE,
            max_attempts: None,
            units: None,
        }
    }

    /// Events read per run, at least 1. Defaults to [`DEFAULT_BATCH_SIZE`].
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size.max(1);
        self
    }

    /// Dead-letters an event after it has failed `max_attempts` runs, even
    /// if the error is retryable, so a stuck subscription moves on. `0` is
    /// treated as 1.
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = Some(max_attempts.max(1));
        self
    }

    /// Runs each event's handler and cursor move in one of the provider's
    /// units of work (a provider without units runs them without one).
    pub fn with_units_of_work(mut self, provider: Arc<dyn Provider>) -> Self {
        self.units = Some(provider);
        self
    }

    /// The subscription manager the dispatcher registers its handlers with.
    pub fn subscriptions(&self) -> &Arc<SubscriptionManager> {
        &self.subscriptions
    }

    /// Registers `handler` on `stream_id` (an aggregate stream, a category,
    /// or `None` for `$all`). The subscription is named after the handler.
    ///
    /// Fails with `INVALID_CONFIGURATION` if a handler with that name is
    /// already registered.
    pub fn register(&self, handler: Arc<dyn EventHandler>, stream_id: Option<&str>) -> Result<()> {
        let name = handler.name().to_string();
        let mut handlers = self.handlers.lock().unwrap();
        if handlers.contains_key(&name) {
            return Err(FrameworkError::invalid_configuration(format!(
                "a handler named {name} is already registered for catch-up"
            ))
            .with_context("handler", name));
        }
        self.subscriptions.register(&name, stream_id)?;
        let stream_id = stream_id
            .filter(|stream| *stream != crate::adapters::StreamIds::ALL)
            .map(str::to_string);
        handlers.insert(name, Arc::new(Registration { handler, stream_id }));
        Ok(())
    }

    /// The registered handlers' names, in order.
    pub fn names(&self) -> Vec<String> {
        self.handlers.lock().unwrap().keys().cloned().collect()
    }

    /// Runs one batch for the handler `name`.
    ///
    /// Fails only when the store itself fails (reading the cursor or the
    /// events, or moving the cursor), or with `INVALID_CONFIGURATION` for an
    /// unknown name; an event's failure is reported in the result.
    pub async fn catch_up(&self, name: &str) -> Result<CatchUpResult> {
        let registration = self
            .handlers
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or_else(|| {
                FrameworkError::invalid_configuration(format!(
                    "no handler named {name} is registered for catch-up"
                ))
                .with_context("handler", name)
            })?;
        let adapter = self.events.adapter();
        let subscription = adapter
            .subscribe(name, registration.stream_id.as_deref())
            .await?;
        // One more than the batch, to know whether there is more.
        let mut batch = adapter
            .load_stream_after_event(
                subscription.stream(),
                subscription.last_seen,
                Some(self.batch_size.saturating_add(1)),
            )
            .await?;
        let has_more = batch.len() > self.batch_size;
        batch.truncate(self.batch_size);

        let mut result = CatchUpResult::default();
        for raw in &batch {
            match self.deliver(name, &registration.handler, raw).await? {
                Delivery::Handled => result.processed += 1,
                Delivery::DeadLettered => result.dead_lettered += 1,
                Delivery::Stopped(failure) => {
                    result.failed = Some(failure);
                    return Ok(result);
                }
            }
        }
        result.has_more = has_more;
        Ok(result)
    }

    /// Runs one batch for every registered handler, in name order. One
    /// handler's store failure doesn't stop the others.
    pub async fn run_all(&self) -> Vec<(String, Result<CatchUpResult>)> {
        let mut results = Vec::new();
        for name in self.names() {
            let result = self.catch_up(&name).await;
            results.push((name, result));
        }
        results
    }

    /// Handles one stored event and moves the cursor, or dead-letters it.
    async fn deliver(
        &self,
        name: &str,
        handler: &Arc<dyn EventHandler>,
        raw: &SerializedEvent,
    ) -> Result<Delivery> {
        let adapter = self.events.adapter();
        let event_id = raw.event_id;
        let outcome = match self.events.deserialize(raw) {
            Ok(event) => self
                .atomically(async {
                    handler.handle(&event).await?;
                    adapter.ack_subscription(name, event_id).await
                })
                .await
                .map_err(|error| (error, false)),
            // Unreadable: retrying would read it the same way.
            Err(error) => Err((error, true)),
        };
        let (error, unreadable) = match outcome {
            Ok(()) => return Ok(Delivery::Handled),
            Err(failure) => failure,
        };

        let attempts = adapter.record_failed_attempt(name, event_id).await?;
        let give_up = unreadable
            || !error.retryable()
            || self.max_attempts.is_some_and(|max| attempts >= max);
        if !give_up {
            return Ok(Delivery::Stopped(CatchUpFailure {
                event_id,
                attempts,
                error,
            }));
        }
        let entry = DeadLetterInput {
            handler_name: name.to_string(),
            event_id,
            event_type: raw.event_type.clone(),
            payload: raw.data.clone(),
            error_code: error.code().as_str().to_string(),
            error_message: error.message().to_string(),
            attempts,
        };
        let moved_on = self
            .atomically(async {
                self.dead_letters.write(entry).await?;
                adapter.ack_subscription(name, event_id).await
            })
            .await;
        match moved_on {
            Ok(()) => Ok(Delivery::DeadLettered),
            Err(write_error) => Ok(Delivery::Stopped(CatchUpFailure {
                event_id,
                attempts,
                error: FrameworkError::dead_letter(format!(
                    "event {event_id} failed for handler {name} and couldn't be dead-lettered: \
                     {write_error}"
                ))
                .with_context("handler", name)
                .with_context("eventId", event_id.to_string())
                .with_context("originalError", error.to_string())
                .with_source(write_error),
            })),
        }
    }

    /// Runs `work` in a unit of work, if the dispatcher has a provider
    /// whose units exist.
    async fn atomically<T>(&self, work: impl Future<Output = Result<T>>) -> Result<T> {
        let unit: Option<Arc<dyn UnitOfWork>> = self
            .units
            .as_ref()
            .and_then(|provider| provider.create_unit_of_work());
        match unit {
            Some(unit) => unit.execute(work).await,
            None => work.await,
        }
    }
}

impl fmt::Debug for CronDispatcher {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CronDispatcher")
            .field("handlers", &self.names())
            .field("batch_size", &self.batch_size)
            .field("max_attempts", &self.max_attempts)
            .finish_non_exhaustive()
    }
}

enum Delivery {
    Handled,
    DeadLettered,
    Stopped(CatchUpFailure),
}
