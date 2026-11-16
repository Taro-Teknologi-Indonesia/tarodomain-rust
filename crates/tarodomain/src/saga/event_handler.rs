//! The saga event handler (spec §13.3).

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use super::process_manager::{ProcessManager, SagaInstance, SagaRoutes};
use crate::adapters::{ProcessedEventStore, SagaStoreAdapter};
use crate::async_trait;
use crate::bus::EventHandler;
use crate::command::CommandDispatcher;
use crate::domain::{AnyCommand, DomainEvent};
use crate::error::Result;
use crate::kernel::context;
use crate::kernel::ids::Uuid;

type DispatchFuture = Pin<Box<dyn Future<Output = Result<()>> + Send>>;
type DispatchFn = dyn Fn(Vec<AnyCommand>) -> DispatchFuture + Send + Sync;

/// Where a saga's commands go. [`SagaEventHandler`] calls it before saving
/// the saga, so a failure leaves the saga as it was.
///
/// The rebuilder passes [`discard`](Self::discard), because replaying a saga
/// must not run its commands again (§20.2).
#[derive(Clone)]
pub struct SagaCommandDispatch(Arc<DispatchFn>);

impl SagaCommandDispatch {
    /// A dispatch from an async function over the batch of commands.
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: Fn(Vec<AnyCommand>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        Self(Arc::new(move |commands| Box::pin(f(commands))))
    }

    /// Dispatches each command through `dispatcher`, in order, and stops at
    /// the first failure.
    pub fn to_dispatcher(dispatcher: Arc<CommandDispatcher>) -> Self {
        Self::new(move |commands| {
            let dispatcher = dispatcher.clone();
            async move {
                for command in commands {
                    dispatcher.dispatch(command).await?;
                }
                Ok(())
            }
        })
    }

    /// Throws the commands away.
    pub fn discard() -> Self {
        Self::new(|_| async { Ok(()) })
    }

    /// Runs the dispatch.
    pub async fn dispatch(&self, commands: Vec<AnyCommand>) -> Result<()> {
        (self.0)(commands).await
    }
}

impl fmt::Debug for SagaCommandDispatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SagaCommandDispatch")
    }
}

/// Events waiting for a saga that is being handled further up this task's
/// stack, by (saga type, saga ID). A key is present while that saga is busy.
type Busy = HashMap<(&'static str, Uuid), VecDeque<DomainEvent>>;

#[derive(Clone, Default)]
struct Mailbox(Arc<Mutex<Busy>>);

/// Runs a [`ProcessManager`] as an [`EventHandler`] (spec §13.3).
///
/// For every event: skip it if this saga has processed it; load the saga by
/// the event's correlation ID (a saga with no record starts only on a
/// `starts_when` event); run the handler; **dispatch its commands, then save
/// the saga**, then mark the event processed. If dispatch fails, nothing is
/// saved and the event is retried against the old state.
///
/// - An event for a saga that has ended is recorded and skipped.
/// - An event a running saga has no handler for is skipped without saving,
///   so it doesn't push back the saga's timeout.
/// - **Mailbox:** with strong consistency a dispatched command publishes its
///   events, which reach this handler before the saga is saved. Such an event
///   waits in a per-saga mailbox (task-local) and is handled right after the
///   saga is saved, so it sees the saga's new state.
pub struct SagaEventHandler<P: ProcessManager> {
    routes: Arc<SagaRoutes<P>>,
    store: Arc<dyn SagaStoreAdapter>,
    processed: Arc<dyn ProcessedEventStore>,
    dispatch: SagaCommandDispatch,
}

impl<P: ProcessManager> SagaEventHandler<P> {
    /// Builds the handler. Fails with `INVALID_CONFIGURATION` if the saga's
    /// routes are invalid.
    pub fn new(
        store: Arc<dyn SagaStoreAdapter>,
        processed: Arc<dyn ProcessedEventStore>,
        dispatch: SagaCommandDispatch,
    ) -> Result<Self> {
        Ok(Self {
            routes: SagaRoutes::build()?,
            store,
            processed,
            dispatch,
        })
    }

    /// The saga's routes.
    pub fn routes(&self) -> &Arc<SagaRoutes<P>> {
        &self.routes
    }

    /// Handles one event with the saga loaded from the store. This is the
    /// §13.3 flow without the mailbox.
    async fn process(&self, event: &DomainEvent) -> Result<()> {
        let saga_id = event.envelope().correlation_id;
        let instance_key = saga_id.to_string();
        if self
            .processed
            .exists(P::SAGA_TYPE, &instance_key, event.id())
            .await?
        {
            return Ok(());
        }
        let mut saga = match self.store.load(P::SAGA_TYPE, saga_id).await? {
            Some(record) => SagaInstance::restore(self.routes.clone(), record)?,
            None if self.routes.starts_on(event.name()) => {
                SagaInstance::new(self.routes.clone(), saga_id)
            }
            None => return Ok(()),
        };
        if saga.state().is_terminal() {
            return self.mark(&instance_key, event).await;
        }
        if !saga.handles(event) {
            return Ok(());
        }
        let commands = saga.handle(event)?;
        if !commands.is_empty() {
            self.dispatch.dispatch(commands).await?;
        }
        self.store.save(saga.to_save_input()?).await?;
        self.mark(&instance_key, event).await
    }

    async fn mark(&self, instance_key: &str, event: &DomainEvent) -> Result<()> {
        self.processed
            .mark_processed(P::SAGA_TYPE, instance_key, event.id())
            .await
    }

    async fn handle_in(&self, mailbox: &Mailbox, event: &DomainEvent) -> Result<()> {
        let key = (P::SAGA_TYPE, event.envelope().correlation_id);
        {
            let mut busy = mailbox.0.lock().unwrap();
            if let Some(queue) = busy.get_mut(&key) {
                queue.push_back(event.clone());
                return Ok(());
            }
            busy.insert(key, VecDeque::new());
        }
        let mut result = self.process(event).await;
        while result.is_ok() {
            let next = mailbox
                .0
                .lock()
                .unwrap()
                .get_mut(&key)
                .and_then(VecDeque::pop_front);
            match next {
                Some(waiting) => result = self.process(&waiting).await,
                None => break,
            }
        }
        // On failure the waiting events are dropped; the failure reaches the
        // publisher, which retries or leaves them to catch-up.
        mailbox.0.lock().unwrap().remove(&key);
        result
    }
}

impl<P: ProcessManager> fmt::Debug for SagaEventHandler<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SagaEventHandler")
            .field("sagaType", &P::SAGA_TYPE)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<P: ProcessManager> EventHandler for SagaEventHandler<P> {
    fn name(&self) -> &str {
        P::SAGA_TYPE
    }

    async fn handle(&self, event: &DomainEvent) -> Result<()> {
        match context::get::<Mailbox>() {
            Some(mailbox) => self.handle_in(&mailbox, event).await,
            None => {
                let mailbox = Mailbox::default();
                context::scope(mailbox.clone(), self.handle_in(&mailbox, event)).await
            }
        }
    }
}
