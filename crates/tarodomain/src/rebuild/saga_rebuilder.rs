//! The saga rebuilder (spec §20.2).

use std::collections::BTreeSet;
use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use crate::adapters::{ProcessedEventStore, SagaStatus, SagaStoreAdapter};
use crate::async_trait;
use crate::bus::EventHandler;
use crate::error::{FrameworkError, Result};
use crate::event_sourcing::EventStore;
use crate::kernel::ids::Uuid;
use crate::saga::{ProcessManager, SagaCommandDispatch, SagaEventHandler, SagaRoutes};

/// How many events one scan of the log reads.
const SCAN_BATCH: usize = 500;

/// How a saga rebuild runs.
#[derive(Clone, Debug, Default)]
pub struct SagaRebuildOptions {
    /// Go on to the next saga when one fails to rebuild, instead of stopping.
    /// Defaults to `false`.
    pub continue_on_error: bool,
}

/// What rebuilding one saga did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SagaRebuildResult {
    /// The saga's ID.
    pub saga_id: Uuid,
    /// How many events of its correlation chain were replayed.
    pub events_replayed: usize,
    /// The saga's status after the replay, `None` if no event started it.
    pub status: Option<SagaStatus>,
}

/// What rebuilding every saga of a type did.
#[derive(Debug)]
pub struct SagaRebuildAllResult {
    /// The saga type.
    pub saga_type: &'static str,
    /// The sagas that were rebuilt.
    pub rebuilt: Vec<SagaRebuildResult>,
    /// The sagas that failed, with why.
    pub failed: Vec<(Uuid, FrameworkError)>,
}

/// A saga type a [`SagaRebuilder`] can rebuild; use [`SagaType`] to get one
/// from a [`ProcessManager`].
#[async_trait]
pub trait SagaRebuildTarget: Send + Sync {
    /// The saga's type name.
    fn saga_type(&self) -> &'static str;

    /// Rebuilds one saga of this type.
    async fn rebuild_instance(
        &self,
        rebuilder: &SagaRebuilder,
        saga_id: Uuid,
    ) -> Result<SagaRebuildResult>;

    /// The IDs of the sagas that have started in the log.
    async fn find_instances(&self, rebuilder: &SagaRebuilder) -> Result<Vec<Uuid>>;
}

/// The [`SagaRebuildTarget`] of a process manager: `SagaType::<MySaga>::new()`.
pub struct SagaType<P>(PhantomData<fn() -> P>);

impl<P> SagaType<P> {
    /// The target for saga type `P`.
    pub fn new() -> Self {
        Self(PhantomData)
    }
}

impl<P> Default for SagaType<P> {
    fn default() -> Self {
        Self::new()
    }
}

impl<P> fmt::Debug for SagaType<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SagaType")
    }
}

#[async_trait]
impl<P: ProcessManager> SagaRebuildTarget for SagaType<P> {
    fn saga_type(&self) -> &'static str {
        P::SAGA_TYPE
    }

    async fn rebuild_instance(
        &self,
        rebuilder: &SagaRebuilder,
        saga_id: Uuid,
    ) -> Result<SagaRebuildResult> {
        rebuilder.rebuild_instance::<P>(saga_id).await
    }

    async fn find_instances(&self, rebuilder: &SagaRebuilder) -> Result<Vec<Uuid>> {
        rebuilder.find_instances::<P>().await
    }
}

/// Reconstructs saga state from the event log (spec §20.2).
///
/// For one saga:
///
/// 1. its stored state is deleted;
/// 2. its processed-event records are deleted;
/// 3. the events with its correlation ID are loaded, in append order;
/// 4. each is replayed through a [`SagaEventHandler`] whose commands are
///    **discarded**: they ran the first time, and their events are in the log;
/// 5. the saga's state, status and compensation stack are saved as the
///    replay leaves them.
///
/// Limits: a saga's `SagaTimeout` events aren't stored, so a saga that was
/// timed out comes back in the state its last stored event left it in, and
/// its timeout fires again if it is still overdue. Timestamps (`updated_at`)
/// are the time of the rebuild. If the replay fails half-way, the saga is
/// left as far as it got; rebuild it again once the cause is fixed.
pub struct SagaRebuilder {
    events: EventStore,
    sagas: Arc<dyn SagaStoreAdapter>,
    processed: Arc<dyn ProcessedEventStore>,
}

impl SagaRebuilder {
    /// A rebuilder over the event store, the saga store and the processed
    /// events the sagas record their handled events in.
    pub fn new(
        events: EventStore,
        sagas: Arc<dyn SagaStoreAdapter>,
        processed: Arc<dyn ProcessedEventStore>,
    ) -> Self {
        Self {
            events,
            sagas,
            processed,
        }
    }

    /// Rebuilds the saga of type `P` with this ID.
    pub async fn rebuild_instance<P: ProcessManager>(
        &self,
        saga_id: Uuid,
    ) -> Result<SagaRebuildResult> {
        let handler = SagaEventHandler::<P>::new(
            self.sagas.clone(),
            self.processed.clone(),
            SagaCommandDispatch::discard(),
        )?;
        self.sagas.delete(P::SAGA_TYPE, saga_id).await?;
        self.processed
            .delete_by_handler(P::SAGA_TYPE, Some(&saga_id.to_string()))
            .await?;
        let events = self.events.load_by_correlation_id(saga_id).await?;
        for event in &events {
            handler.handle(event).await.map_err(|error| {
                error
                    .with_context("sagaType", P::SAGA_TYPE)
                    .with_context("sagaId", saga_id.to_string())
                    .with_context("eventId", event.id().to_string())
            })?;
        }
        let status = self
            .sagas
            .load(P::SAGA_TYPE, saga_id)
            .await?
            .map(|record| record.status);
        Ok(SagaRebuildResult {
            saga_id,
            events_replayed: events.len(),
            status,
        })
    }

    /// The IDs of every saga of type `P` that has started in the log, found
    /// by scanning for the events its `starts_when` routes handle. (A saga's
    /// ID is the correlation ID of its start event.) Sorted.
    pub async fn find_instances<P: ProcessManager>(&self) -> Result<Vec<Uuid>> {
        let routes = SagaRoutes::<P>::build()?;
        let adapter = self.events.adapter();
        let mut ids = BTreeSet::new();
        let mut cursor = None;
        loop {
            let batch = adapter.load_all(cursor, Some(SCAN_BATCH)).await?;
            for raw in &batch {
                if routes.starts_on(&raw.event_type) {
                    ids.insert(raw.correlation_id);
                }
            }
            match batch.last() {
                Some(last) if batch.len() == SCAN_BATCH => cursor = Some(last.event_id),
                _ => break,
            }
        }
        Ok(ids.into_iter().collect())
    }

    /// Rebuilds every saga of type `P` the log has a start event for.
    pub async fn rebuild_type<P: ProcessManager>(
        &self,
        options: &SagaRebuildOptions,
    ) -> Result<SagaRebuildAllResult> {
        self.rebuild_target(&SagaType::<P>::new(), options).await
    }

    /// [`rebuild_type`](Self::rebuild_type) for several saga types, in
    /// order. Without `continue_on_error` it stops after the first type with
    /// a failed saga.
    pub async fn rebuild_all(
        &self,
        targets: &[&dyn SagaRebuildTarget],
        options: &SagaRebuildOptions,
    ) -> Result<Vec<SagaRebuildAllResult>> {
        let mut results = Vec::new();
        for target in targets {
            let result = self.rebuild_target(*target, options).await?;
            let stop = !result.failed.is_empty() && !options.continue_on_error;
            results.push(result);
            if stop {
                break;
            }
        }
        Ok(results)
    }

    async fn rebuild_target(
        &self,
        target: &dyn SagaRebuildTarget,
        options: &SagaRebuildOptions,
    ) -> Result<SagaRebuildAllResult> {
        let mut all = SagaRebuildAllResult {
            saga_type: target.saga_type(),
            rebuilt: Vec::new(),
            failed: Vec::new(),
        };
        for saga_id in target.find_instances(self).await? {
            match target.rebuild_instance(self, saga_id).await {
                Ok(result) => all.rebuilt.push(result),
                Err(error) => {
                    all.failed.push((saga_id, error));
                    if !options.continue_on_error {
                        break;
                    }
                }
            }
        }
        Ok(all)
    }
}

impl fmt::Debug for SagaRebuilder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SagaRebuilder").finish_non_exhaustive()
    }
}
