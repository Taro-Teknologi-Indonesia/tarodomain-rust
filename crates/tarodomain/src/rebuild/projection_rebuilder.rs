//! The projection rebuilder (spec §20.1).

use std::fmt;
use std::sync::Arc;

use crate::adapters::ProcessedEventStore;
use crate::async_trait;
use crate::catch_up::CronDispatcher;
use crate::error::{FrameworkError, Result};
use crate::kernel::ids::Uuid;
use crate::projection::{Projection, ProjectionHandler};

/// A handler a [`ProjectionRebuilder`] can rebuild: a projection's handler
/// registered with the [`CronDispatcher`].
#[async_trait]
pub trait RebuildTarget: Send + Sync {
    /// The handler's name: its subscription in the dispatcher.
    fn handler_name(&self) -> &str;

    /// The name its processed-event records are filed under.
    fn record_name(&self) -> &str;

    /// The one instance of those records the handler owns (a shard), or
    /// `None` when it owns all of them.
    fn record_instance(&self) -> Option<&str>;

    /// Runs the projection's `on_rebuild_start` hook.
    async fn on_rebuild_start(&self) -> Result<()>;

    /// Runs the projection's `on_rebuild_complete` hook.
    async fn on_rebuild_complete(&self) -> Result<()>;
}

#[async_trait]
impl<P: Projection> RebuildTarget for ProjectionHandler<P> {
    fn handler_name(&self) -> &str {
        crate::bus::EventHandler::name(self)
    }

    fn record_name(&self) -> &str {
        ProjectionHandler::record_name(self)
    }

    fn record_instance(&self) -> Option<&str> {
        self.fixed_instance()
    }

    async fn on_rebuild_start(&self) -> Result<()> {
        self.projection().on_rebuild_start().await
    }

    async fn on_rebuild_complete(&self) -> Result<()> {
        self.projection().on_rebuild_complete().await
    }
}

/// How a rebuild runs (spec §20.1).
#[derive(Clone, Debug)]
pub struct RebuildOptions {
    /// Skip an event whose handler fails with a retryable error, record it
    /// and go on, instead of stopping. The cursor moves past it without a
    /// dead letter. For `rebuild_all`, also go on to the next target after
    /// one that didn't complete. Defaults to `false`.
    pub continue_on_error: bool,
    /// Forget the handler's processed-event records first, so the replay
    /// isn't skipped as already done. Defaults to `true`; without it a
    /// rebuild of a projection that has handled the events does nothing.
    pub reset_idempotency: bool,
    /// Call the projection's `on_rebuild_start` and `on_rebuild_complete`.
    /// Defaults to `true`.
    pub call_hooks: bool,
    /// Events per batch, or the dispatcher's own size if `None`.
    pub batch_size: Option<usize>,
    /// The most batches one rebuild may take, so it can't run forever.
    /// Defaults to 100 000.
    pub max_passes: usize,
}

impl Default for RebuildOptions {
    fn default() -> Self {
        Self {
            continue_on_error: false,
            reset_idempotency: true,
            call_hooks: true,
            batch_size: None,
            max_passes: 100_000,
        }
    }
}

/// An event a rebuild skipped under `continue_on_error`.
#[derive(Debug)]
pub struct SkippedEvent {
    /// The event.
    pub event_id: Uuid,
    /// Why its handler failed.
    pub error: FrameworkError,
}

/// What one rebuild did.
#[derive(Debug)]
pub struct RebuildResult {
    /// The handler that was rebuilt.
    pub handler: String,
    /// Events handled.
    pub processed: usize,
    /// Events dead-lettered by the replay (non-retryable failures).
    pub dead_lettered: usize,
    /// Batches taken.
    pub passes: usize,
    /// Events skipped under `continue_on_error`.
    pub skipped: Vec<SkippedEvent>,
    /// The failure that stopped the rebuild, if one did.
    pub failure: Option<FrameworkError>,
    /// Whether the replay reached the end of the log: no failure, and the
    /// pass limit wasn't hit. The completion hook runs only then.
    pub completed: bool,
}

/// What a rebuild of several handlers did.
#[derive(Debug, Default)]
pub struct RebuildAllResult {
    /// One result per handler that was rebuilt, in order.
    pub results: Vec<RebuildResult>,
}

impl RebuildAllResult {
    /// Whether every handler completed.
    pub fn completed(&self) -> bool {
        self.results.iter().all(|result| result.completed)
    }
}

/// Replays the event log into projections (spec §20.1).
///
/// For each handler:
///
/// 1. `on_rebuild_start` (the projection drops or truncates its tables);
/// 2. the subscription is reset, so it reads from the beginning;
/// 3. the handler's processed-event records are deleted;
/// 4. the dispatcher's catch-up runs batch after batch until nothing is left;
/// 5. `on_rebuild_complete` (the projection builds indexes).
///
/// The handler must be registered with the dispatcher. Rebuilding a
/// projection that is also fed live is the caller's concern: pause its
/// delivery first, or events handled live during the replay are skipped as
/// duplicates when the replay reaches them, which is harmless, but a table
/// dropped by `on_rebuild_start` misses writes in between.
///
/// If `on_rebuild_start` fails, nothing else happens and the error is
/// returned. A failure while replaying stops that handler, and is reported
/// in its [`RebuildResult`] (the store failing is returned as an error).
pub struct ProjectionRebuilder {
    dispatcher: Arc<CronDispatcher>,
    processed: Arc<dyn ProcessedEventStore>,
}

impl ProjectionRebuilder {
    /// A rebuilder that replays through `dispatcher` and forgets records in
    /// `processed`.
    pub fn new(dispatcher: Arc<CronDispatcher>, processed: Arc<dyn ProcessedEventStore>) -> Self {
        Self {
            dispatcher,
            processed,
        }
    }

    /// Rebuilds one handler.
    pub async fn rebuild(
        &self,
        target: &dyn RebuildTarget,
        options: &RebuildOptions,
    ) -> Result<RebuildResult> {
        if options.call_hooks {
            target.on_rebuild_start().await?;
        }
        let result = self.replay(target, options).await?;
        if options.call_hooks && result.completed {
            target.on_rebuild_complete().await?;
        }
        Ok(result)
    }

    /// Rebuilds several handlers in order. Without `continue_on_error` it
    /// stops after the first one that doesn't complete.
    pub async fn rebuild_all(
        &self,
        targets: &[&dyn RebuildTarget],
        options: &RebuildOptions,
    ) -> Result<RebuildAllResult> {
        let mut all = RebuildAllResult::default();
        for target in targets {
            let result = self.rebuild(*target, options).await?;
            let stop = !result.completed && !options.continue_on_error;
            all.results.push(result);
            if stop {
                break;
            }
        }
        Ok(all)
    }

    /// Rebuilds every shard of a projection. The hooks run once for the
    /// projection: start before the first shard, complete after the last,
    /// and only if every shard completed.
    pub async fn rebuild_shards<P: Projection>(
        &self,
        shards: &[ProjectionHandler<P>],
        options: &RebuildOptions,
    ) -> Result<RebuildAllResult> {
        let Some(first) = shards.first() else {
            return Ok(RebuildAllResult::default());
        };
        if options.call_hooks {
            RebuildTarget::on_rebuild_start(first).await?;
        }
        let inner = RebuildOptions {
            call_hooks: false,
            ..options.clone()
        };
        let mut all = RebuildAllResult::default();
        for shard in shards {
            let result = self.replay(shard, &inner).await?;
            let stop = !result.completed && !options.continue_on_error;
            all.results.push(result);
            if stop {
                break;
            }
        }
        if options.call_hooks && all.completed() && all.results.len() == shards.len() {
            RebuildTarget::on_rebuild_complete(first).await?;
        }
        Ok(all)
    }

    /// Steps 2 to 4, without the hooks.
    async fn replay(
        &self,
        target: &dyn RebuildTarget,
        options: &RebuildOptions,
    ) -> Result<RebuildResult> {
        let name = target.handler_name();
        let manager = self.dispatcher.subscriptions();
        manager.reset(name).await?;
        if options.reset_idempotency {
            self.processed
                .delete_by_handler(target.record_name(), target.record_instance())
                .await?;
        }
        let batch_size = options.batch_size;
        let mut result = RebuildResult {
            handler: name.to_string(),
            processed: 0,
            dead_lettered: 0,
            passes: 0,
            skipped: Vec::new(),
            failure: None,
            completed: false,
        };
        while result.passes < options.max_passes {
            result.passes += 1;
            let run = match batch_size {
                Some(size) => self.dispatcher.catch_up_batch(name, size).await?,
                None => self.dispatcher.catch_up(name).await?,
            };
            result.processed += run.processed;
            result.dead_lettered += run.dead_lettered;
            if let Some(failure) = run.failed {
                if options.continue_on_error {
                    manager.advance(name, failure.event_id).await?;
                    result.skipped.push(SkippedEvent {
                        event_id: failure.event_id,
                        error: failure.error,
                    });
                    continue;
                }
                result.failure = Some(failure.error);
                return Ok(result);
            }
            if !run.has_more {
                result.completed = true;
                return Ok(result);
            }
        }
        Ok(result)
    }
}

impl fmt::Debug for ProjectionRebuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectionRebuilder")
            .finish_non_exhaustive()
    }
}
