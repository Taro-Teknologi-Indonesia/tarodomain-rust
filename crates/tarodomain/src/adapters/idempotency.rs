//! The idempotency store contracts: events handled per handler (spec §12.3,
//! §14.2) and commands processed (§14.1).

use chrono::{DateTime, Utc};

use crate::async_trait;
use crate::error::Result;
use crate::kernel::ids::Uuid;

/// Which events each projection and saga has handled (spec §12.3), so a
/// redelivered event is skipped.
///
/// The key is `(handler_name, handler_instance, event_id)`. A projection
/// uses its name for both (or the shard as instance), and a saga its type
/// and saga ID. Each record keeps when it was marked, by the store's clock.
///
/// The 30-day retention (§14.2) belongs to the framework's cleanup job,
/// which calls [`delete_old`](Self::delete_old). The store doesn't expire
/// records on its own.
#[async_trait]
pub trait ProcessedEventStore: Send + Sync {
    /// Whether the handler instance has handled the event.
    async fn exists(
        &self,
        handler_name: &str,
        handler_instance: &str,
        event_id: Uuid,
    ) -> Result<bool>;

    /// Records that the handler instance has handled the event. Idempotent:
    /// marking again keeps the first record and its time.
    async fn mark_processed(
        &self,
        handler_name: &str,
        handler_instance: &str,
        event_id: Uuid,
    ) -> Result<()>;

    /// How many events the handler has handled, in one instance if given,
    /// else in all of them.
    async fn count(&self, handler_name: &str, handler_instance: Option<&str>) -> Result<u64>;

    /// Forgets what the handler has handled, in one instance if given, else
    /// in all of them, and returns how many records it deleted. A rebuild
    /// calls it before replaying (§20).
    async fn delete_by_handler(
        &self,
        handler_name: &str,
        handler_instance: Option<&str>,
    ) -> Result<u64>;

    /// Deletes the records marked before `older_than`, and returns how many.
    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64>;
}

/// A command the pipeline has processed (spec §14.1).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProcessedCommand {
    /// The command's ID, the key.
    pub command_id: Uuid,
    /// The command's name ([`Command::NAME`](crate::domain::Command::NAME)).
    pub command_type: String,
    /// The handler that processed it.
    pub handler_name: String,
    /// When it was marked, by the store's clock.
    pub processed_at: DateTime<Utc>,
}

/// Which commands have been processed (spec §14.1), so a command delivered
/// twice runs once.
///
/// The key is the command ID. The pipeline checks before handling, on every
/// attempt, and marks after the handler succeeds; in strong mode the mark
/// joins the command's unit of work, so a rollback forgets it.
///
/// The 7-day retention (§14.1) belongs to the framework's cleanup job, which
/// calls [`delete_old`](Self::delete_old). The store doesn't expire records
/// on its own.
#[async_trait]
pub trait CommandIdempotencyStore: Send + Sync {
    /// Whether the command has been processed.
    ///
    /// Defaults to [`get_processed_at`](Self::get_processed_at) returning a
    /// time.
    async fn exists(&self, command_id: Uuid) -> Result<bool> {
        Ok(self.get_processed_at(command_id).await?.is_some())
    }

    /// Records that the command has been processed. Idempotent: marking
    /// again keeps the first record and its time.
    async fn mark_processed(
        &self,
        command_id: Uuid,
        command_type: &str,
        handler_name: &str,
    ) -> Result<()>;

    /// When the command was marked, if it has been.
    async fn get_processed_at(&self, command_id: Uuid) -> Result<Option<DateTime<Utc>>>;

    /// How many commands have been processed, by one handler if given.
    async fn count(&self, handler_name: Option<&str>) -> Result<u64>;

    /// The commands the handler has processed, oldest first (then by ID),
    /// at most `limit` of them.
    async fn list(&self, handler_name: &str, limit: Option<usize>)
    -> Result<Vec<ProcessedCommand>>;

    /// Deletes the records marked before `older_than`, and returns how many.
    async fn delete_old(&self, older_than: DateTime<Utc>) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::kernel::ids::new_v7;

    /// Implements the required methods over one marked command, to check the
    /// provided `exists`.
    struct OneCommand {
        id: Uuid,
        asked: Mutex<Vec<Uuid>>,
    }

    #[async_trait]
    impl CommandIdempotencyStore for OneCommand {
        async fn mark_processed(&self, _: Uuid, _: &str, _: &str) -> Result<()> {
            unreachable!()
        }
        async fn get_processed_at(&self, command_id: Uuid) -> Result<Option<DateTime<Utc>>> {
            self.asked.lock().unwrap().push(command_id);
            Ok((command_id == self.id).then_some(DateTime::UNIX_EPOCH))
        }
        async fn count(&self, _: Option<&str>) -> Result<u64> {
            unreachable!()
        }
        async fn list(&self, _: &str, _: Option<usize>) -> Result<Vec<ProcessedCommand>> {
            unreachable!()
        }
        async fn delete_old(&self, _: DateTime<Utc>) -> Result<u64> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn exists_asks_for_the_processed_time() {
        let (id, other) = (new_v7(), new_v7());
        let store = Arc::new(OneCommand {
            id,
            asked: Mutex::new(Vec::new()),
        });
        let dyn_store: Arc<dyn CommandIdempotencyStore> = store.clone();
        assert!(dyn_store.exists(id).await.unwrap());
        assert!(!dyn_store.exists(other).await.unwrap());
        assert_eq!(*store.asked.lock().unwrap(), [id, other]);
    }

    #[test]
    fn the_traits_are_dyn_compatible() {
        fn accepts(_: Option<Arc<dyn ProcessedEventStore>>) {}
        accepts(None);
    }
}
