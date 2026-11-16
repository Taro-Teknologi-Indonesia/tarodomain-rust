//! The subscription manager (spec §18.2): the cursors that catch-up reads
//! from.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::adapters::{EventStoreAdapter, StreamIds, Subscription};
use crate::error::{FrameworkError, Result};
use crate::kernel::ids::Uuid;

/// How far one subscription is behind (spec §18.2 `getLagReport`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionLag {
    /// The subscription's name.
    pub name: String,
    /// The stream it reads, `None` for `$all`.
    pub stream_id: Option<String>,
    /// The last event acked, `None` before the first.
    pub last_seen: Option<Uuid>,
    /// Events after `last_seen`, counted up to the report's limit.
    pub pending: usize,
    /// Whether there are more than `pending`: the count stopped at the limit.
    pub pending_capped: bool,
    /// The event that has failed since the last ack, if any.
    pub failed_event_id: Option<Uuid>,
    /// How many times it has failed.
    pub failed_attempts: u32,
}

impl SubscriptionLag {
    /// Whether nothing is waiting for the subscription.
    pub fn caught_up(&self) -> bool {
        self.pending == 0
    }
}

/// The lag of every subscription, in name order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LagReport {
    /// One entry per subscription.
    pub subscriptions: Vec<SubscriptionLag>,
}

impl LagReport {
    /// Whether every subscription is caught up.
    pub fn caught_up(&self) -> bool {
        self.subscriptions.iter().all(SubscriptionLag::caught_up)
    }
}

/// The cursor the manager works with: a stored subscription, or one that is
/// registered but not created yet.
struct Cursor {
    name: String,
    stream_id: Option<String>,
    last_seen: Option<Uuid>,
    failed_event_id: Option<Uuid>,
    failed_attempts: u32,
}

impl From<Subscription> for Cursor {
    fn from(sub: Subscription) -> Self {
        Self {
            name: sub.name,
            stream_id: sub.stream_id,
            last_seen: sub.last_seen,
            failed_event_id: sub.failed_event_id,
            failed_attempts: sub.failed_attempts,
        }
    }
}

/// Manages subscription cursors (spec §18.2).
///
/// A subscription is *registered* in memory (each handler does so at build)
/// and *created* in the store by
/// [`ensure_all_subscriptions`](Self::ensure_all_subscriptions), or by the first catch-up run. Registered
/// subscriptions that aren't created yet count as starting from the
/// beginning.
///
/// Cloning is not offered: share it as an `Arc`.
pub struct SubscriptionManager {
    adapter: Arc<dyn EventStoreAdapter>,
    registered: Mutex<BTreeMap<String, Option<String>>>,
}

impl SubscriptionManager {
    /// A manager over the store's subscriptions.
    pub fn new(adapter: Arc<dyn EventStoreAdapter>) -> Self {
        Self {
            adapter,
            registered: Mutex::new(BTreeMap::new()),
        }
    }

    /// Registers a subscription in memory: `stream_id` is an aggregate
    /// stream, a category, or `None` for `$all`. Registering the same name
    /// and stream again does nothing; the same name with another stream is
    /// `INVALID_CONFIGURATION`.
    pub fn register(&self, name: &str, stream_id: Option<&str>) -> Result<()> {
        let stream_id = normalize(stream_id);
        let mut registered = self.registered.lock().unwrap();
        match registered.get(name) {
            Some(existing) if *existing != stream_id => {
                Err(FrameworkError::invalid_configuration(format!(
                    "subscription {name} is registered on {} and can't also read {}",
                    existing.as_deref().unwrap_or(StreamIds::ALL),
                    stream_id.as_deref().unwrap_or(StreamIds::ALL)
                ))
                .with_context("subscription", name))
            }
            Some(_) => Ok(()),
            None => {
                registered.insert(name.to_string(), stream_id);
                Ok(())
            }
        }
    }

    /// The registered subscriptions, by name.
    pub fn registered(&self) -> Vec<(String, Option<String>)> {
        self.registered
            .lock()
            .unwrap()
            .iter()
            .map(|(name, stream)| (name.clone(), stream.clone()))
            .collect()
    }

    /// Creates the registered subscriptions the store doesn't have yet, and
    /// returns their names. Existing cursors are left alone.
    pub async fn ensure_all_subscriptions(&self) -> Result<Vec<String>> {
        let mut created = Vec::new();
        for (name, stream_id) in self.registered() {
            if self.adapter.get_subscription(&name).await?.is_none() {
                self.adapter.subscribe(&name, stream_id.as_deref()).await?;
                created.push(name);
            }
        }
        Ok(created)
    }

    /// The stored subscription, if there is one.
    pub async fn get(&self, name: &str) -> Result<Option<Subscription>> {
        self.adapter.get_subscription(name).await
    }

    /// Every stored subscription.
    pub async fn list(&self) -> Result<Vec<Subscription>> {
        self.adapter.list_subscriptions().await
    }

    /// Moves the cursor to `last_seen_event_id`. It only moves forward in
    /// append order, so acking an old event does nothing.
    pub async fn advance(&self, name: &str, last_seen_event_id: Uuid) -> Result<()> {
        self.adapter
            .ack_subscription(name, last_seen_event_id)
            .await
    }

    /// Deletes the stored subscription. It stays registered, and the next
    /// catch-up run starts it again from the beginning.
    pub async fn delete(&self, name: &str) -> Result<()> {
        self.adapter.delete_subscription(name).await
    }

    /// Whether nothing is waiting after the subscription's cursor.
    ///
    /// Fails with `INVALID_CONFIGURATION` for a name that is neither stored
    /// nor registered.
    pub async fn is_caught_up(&self, name: &str) -> Result<bool> {
        let cursor = self.cursor(name).await?;
        Ok(self.pending(&cursor, 1).await?.0 == 0)
    }

    /// Whether every subscription, stored or registered, is caught up.
    pub async fn are_all_caught_up(&self) -> Result<bool> {
        for cursor in self.cursors().await? {
            if self.pending(&cursor, 1).await?.0 > 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// How far every subscription is behind, counting at most `count_limit`
    /// pending events per subscription (the count reads the events, so keep
    /// the limit modest).
    pub async fn get_lag_report(&self, count_limit: usize) -> Result<LagReport> {
        let mut subscriptions = Vec::new();
        for cursor in self.cursors().await? {
            let (pending, pending_capped) = self.pending(&cursor, count_limit).await?;
            subscriptions.push(SubscriptionLag {
                name: cursor.name,
                stream_id: cursor.stream_id,
                last_seen: cursor.last_seen,
                pending,
                pending_capped,
                failed_event_id: cursor.failed_event_id,
                failed_attempts: cursor.failed_attempts,
            });
        }
        Ok(LagReport { subscriptions })
    }

    /// Deletes the subscription and creates it again on the same stream, so
    /// it reads from the beginning (for a rebuild).
    pub async fn reset(&self, name: &str) -> Result<()> {
        let cursor = self.cursor(name).await?;
        self.adapter.delete_subscription(name).await?;
        self.adapter
            .subscribe(name, cursor.stream_id.as_deref())
            .await?;
        Ok(())
    }

    /// [`reset`](Self::reset) for every stored and registered subscription.
    pub async fn reset_all(&self) -> Result<()> {
        for cursor in self.cursors().await? {
            self.reset(&cursor.name).await?;
        }
        Ok(())
    }

    /// The stored subscription, else the registered one with no progress.
    async fn cursor(&self, name: &str) -> Result<Cursor> {
        if let Some(stored) = self.adapter.get_subscription(name).await? {
            return Ok(stored.into());
        }
        let registered = self.registered.lock().unwrap().get(name).cloned();
        match registered {
            Some(stream_id) => Ok(unstarted(name, stream_id)),
            None => Err(FrameworkError::invalid_configuration(format!(
                "there is no subscription named {name}"
            ))
            .with_context("subscription", name)),
        }
    }

    /// Every stored subscription plus the registered ones not stored yet,
    /// in name order.
    async fn cursors(&self) -> Result<Vec<Cursor>> {
        let mut cursors: BTreeMap<String, Cursor> = self
            .adapter
            .list_subscriptions()
            .await?
            .into_iter()
            .map(|sub| (sub.name.clone(), sub.into()))
            .collect();
        for (name, stream_id) in self.registered() {
            cursors
                .entry(name.clone())
                .or_insert_with(|| unstarted(&name, stream_id));
        }
        Ok(cursors.into_values().collect())
    }

    /// How many events wait after the cursor, up to `limit`, and whether
    /// there are more than that.
    async fn pending(&self, cursor: &Cursor, limit: usize) -> Result<(usize, bool)> {
        let stream = cursor.stream_id.as_deref().unwrap_or(StreamIds::ALL);
        let events = self
            .adapter
            .load_stream_after_event(stream, cursor.last_seen, Some(limit.saturating_add(1)))
            .await?;
        Ok((events.len().min(limit), events.len() > limit))
    }
}

impl std::fmt::Debug for SubscriptionManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubscriptionManager")
            .field("registered", &self.registered())
            .finish_non_exhaustive()
    }
}

fn unstarted(name: &str, stream_id: Option<String>) -> Cursor {
    Cursor {
        name: name.to_string(),
        stream_id,
        last_seen: None,
        failed_event_id: None,
        failed_attempts: 0,
    }
}

/// `$all` is stored as no stream.
fn normalize(stream_id: Option<&str>) -> Option<String> {
    stream_id
        .filter(|stream| *stream != StreamIds::ALL)
        .map(str::to_string)
}
