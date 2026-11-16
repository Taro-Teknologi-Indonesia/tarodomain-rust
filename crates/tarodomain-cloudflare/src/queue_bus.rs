//! The eventual-mode event bus on Cloudflare Queues (spec §10.5, §24.2).

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use tarodomain::async_trait;
use tarodomain::bus::EventBus;
use tarodomain::domain::DomainEvent;
use tarodomain::error::{FrameworkError, Result};

use crate::d1::D1Db;
use crate::unit_of_work::D1UnitOfWork;

/// A queue's limit on the size of one message, in bytes (Cloudflare Queues:
/// 128 KB).
pub const MAX_MESSAGE_BYTES: usize = 128 * 1024;

/// A queue's limit on the number of messages in one `send_batch`.
pub const MAX_BATCH_MESSAGES: usize = 100;

/// Where the bus sends events: a Cloudflare Queue in a Worker, a recording
/// queue in tests.
///
/// A message body is the JSON text of a
/// [`SerializedEvent`](tarodomain::domain::SerializedEvent).
#[async_trait]
pub trait EventQueue: Send + Sync {
    /// Sends the messages in one batch.
    async fn send_batch(&self, bodies: Vec<String>) -> Result<()>;
}

/// An in-memory queue that records what it was sent, for tests.
#[derive(Default)]
pub struct RecordingQueue {
    batches: Mutex<Vec<Vec<String>>>,
    fail: Mutex<bool>,
}

impl RecordingQueue {
    /// An empty queue.
    pub fn new() -> Self {
        Self::default()
    }

    /// Every batch sent so far.
    pub fn batches(&self) -> Vec<Vec<String>> {
        self.batches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Every message sent so far, in order.
    pub fn messages(&self) -> Vec<String> {
        self.batches().into_iter().flatten().collect()
    }

    /// Makes later sends fail (or work again).
    pub fn set_failing(&self, fail: bool) {
        *self.fail.lock().unwrap_or_else(PoisonError::into_inner) = fail;
    }
}

#[async_trait]
impl EventQueue for RecordingQueue {
    async fn send_batch(&self, bodies: Vec<String>) -> Result<()> {
        if *self.fail.lock().unwrap_or_else(PoisonError::into_inner) {
            return Err(FrameworkError::queue_error("the queue is down"));
        }
        self.batches
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(bodies);
        Ok(())
    }
}

/// Publishes events to a queue; a consumer (`CloudflareWorker::queue`) runs
/// the handlers.
///
/// Inside a unit of work the events are sent after the commit, so a
/// rolled-back batch never reaches the queue. If the send fails, the events
/// are already stored and the cron catch-up delivers them (§11.2), so the
/// failure is logged, not returned. An event over the queue's message size
/// limit is logged and left to the catch-up too.
#[derive(Clone)]
pub struct CloudflareQueueEventBus {
    name: String,
    db: D1Db,
    queue: Arc<dyn EventQueue>,
}

impl CloudflareQueueEventBus {
    /// A bus over `queue`. `db` finds the active unit of work.
    pub fn new(db: D1Db, queue: Arc<dyn EventQueue>) -> Self {
        Self {
            name: "cloudflare-queue".to_owned(),
            db,
            queue,
        }
    }

    /// The message bodies for `events`, leaving out the ones that are too big.
    fn bodies(events: &[DomainEvent]) -> Result<Vec<String>> {
        let mut bodies = Vec::with_capacity(events.len());
        for event in events {
            let body = serde_json::to_string(&event.serialize()?).map_err(|error| {
                FrameworkError::serialization_error("an event can't be queued").with_source(error)
            })?;
            if body.len() > MAX_MESSAGE_BYTES {
                tracing::warn!(
                    event_id = %event.envelope().id,
                    bytes = body.len(),
                    "an event is over the queue's message size limit; the catch-up will deliver it"
                );
                continue;
            }
            bodies.push(body);
        }
        Ok(bodies)
    }
}

async fn send(queue: Arc<dyn EventQueue>, bodies: Vec<String>) -> Result<()> {
    for chunk in bodies.chunks(MAX_BATCH_MESSAGES) {
        queue.send_batch(chunk.to_vec()).await?;
    }
    Ok(())
}

#[async_trait]
impl EventBus for CloudflareQueueEventBus {
    fn name(&self) -> &str {
        &self.name
    }

    async fn publish_all(&self, events: &[DomainEvent]) -> Result<()> {
        let bodies = Self::bodies(events)?;
        if bodies.is_empty() {
            return Ok(());
        }
        let queue = Arc::clone(&self.queue);
        // After the commit if there is a unit; at once otherwise.
        let result = D1UnitOfWork::after_commit(&self.db, move || send(queue, bodies)).await;
        if let Err(error) = result {
            tracing::warn!(%error, "publishing to the queue failed; the catch-up will deliver the events");
        }
        Ok(())
    }
}

impl fmt::Debug for CloudflareQueueEventBus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudflareQueueEventBus")
            .field("name", &self.name)
            .finish()
    }
}

#[cfg(target_arch = "wasm32")]
pub use worker_queue::WorkerQueue;

#[cfg(target_arch = "wasm32")]
mod worker_queue {
    use tarodomain::async_trait;
    use tarodomain::error::Result;
    use worker::send::SendFuture;

    use super::EventQueue;
    use crate::platform::{Handle, worker_error};

    /// A Cloudflare Queue binding as an [`EventQueue`].
    pub struct WorkerQueue {
        queue: Handle<worker::Queue>,
    }

    impl WorkerQueue {
        /// Wraps a queue binding.
        pub fn new(queue: worker::Queue) -> Self {
            Self {
                queue: Handle::new(queue),
            }
        }
    }

    #[async_trait]
    impl EventQueue for WorkerQueue {
        async fn send_batch(&self, bodies: Vec<String>) -> Result<()> {
            SendFuture::new(self.queue.send_batch(bodies))
                .await
                .map_err(|error| worker_error("queue send", error))
        }
    }
}

#[cfg(all(test, feature = "native-sqlite"))]
mod tests {
    use std::sync::Arc;

    use tarodomain::adapters::UnitOfWork;
    use tarodomain::domain::{EventEnvelope, SerializedEvent};
    use tarodomain_testing::fixtures::event_id;

    use super::*;
    use crate::sqlite_backend::SqliteBackend;

    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct Ping {
        note: String,
    }

    impl tarodomain::domain::Event for Ping {
        const NAME: &'static str = "Ping";
    }

    fn event(n: u32, note: &str) -> DomainEvent {
        let mut envelope = EventEnvelope::new(chrono::Utc::now());
        envelope.id = event_id(n);
        DomainEvent::new(envelope, Ping { note: note.into() })
    }

    fn bus() -> (CloudflareQueueEventBus, Arc<RecordingQueue>, D1Db) {
        let db = D1Db::new(Arc::new(SqliteBackend::in_memory().unwrap()));
        let queue = Arc::new(RecordingQueue::new());
        (
            CloudflareQueueEventBus::new(db.clone(), queue.clone()),
            queue,
            db,
        )
    }

    #[tokio::test]
    async fn events_are_sent_as_serialized_json() {
        let (bus, queue, _) = bus();
        bus.publish_all(&[event(1, "a"), event(2, "b")])
            .await
            .unwrap();
        let sent: Vec<SerializedEvent> = queue
            .messages()
            .iter()
            .map(|body| serde_json::from_str(body).unwrap())
            .collect();
        assert_eq!(
            sent.iter().map(|e| e.event_id).collect::<Vec<_>>(),
            [event_id(1), event_id(2)]
        );
        assert_eq!(sent[0].event_type, "Ping");
    }

    #[tokio::test]
    async fn more_than_a_batch_is_split() {
        let (bus, queue, _) = bus();
        let events: Vec<_> = (0..250).map(|n| event(n, "x")).collect();
        bus.publish_all(&events).await.unwrap();
        let sizes: Vec<usize> = queue.batches().iter().map(Vec::len).collect();
        assert_eq!(sizes, [100, 100, 50]);
    }

    #[tokio::test]
    async fn an_oversized_event_is_skipped_and_the_rest_sent() {
        let (bus, queue, _) = bus();
        let big = "x".repeat(MAX_MESSAGE_BYTES);
        bus.publish_all(&[event(1, &big), event(2, "small")])
            .await
            .unwrap();
        assert_eq!(queue.messages().len(), 1);
    }

    #[tokio::test]
    async fn a_failing_queue_does_not_fail_the_publish() {
        let (bus, queue, _) = bus();
        queue.set_failing(true);
        bus.publish_all(&[event(1, "a")]).await.unwrap();
        assert!(queue.messages().is_empty());
    }

    #[tokio::test]
    async fn inside_a_unit_events_go_out_after_the_commit_and_never_after_a_rollback() {
        let (bus, queue, db) = bus();
        let unit: Arc<dyn UnitOfWork> = Arc::new(D1UnitOfWork::new(db.clone()));
        let seen_inside = unit
            .execute(async {
                bus.publish_all(&[event(1, "a")]).await?;
                Ok(queue.messages().len())
            })
            .await
            .unwrap();
        assert_eq!(seen_inside, 0, "nothing is sent before the commit");
        assert_eq!(queue.messages().len(), 1);

        let unit: Arc<dyn UnitOfWork> = Arc::new(D1UnitOfWork::new(db));
        let result: Result<()> = unit
            .execute(async {
                bus.publish_all(&[event(2, "b")]).await?;
                Err(FrameworkError::invalid_configuration("no"))
            })
            .await;
        assert!(result.is_err());
        assert_eq!(
            queue.messages().len(),
            1,
            "a rolled-back batch never reaches the queue"
        );
    }
}
