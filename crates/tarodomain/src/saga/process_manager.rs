//! The process manager (spec §13.1, §13.4, §17).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use super::compensation::{CompensationDefs, CompensationStack};
use super::state_machine::{HandlerKind, SagaState, transition};
use crate::adapters::{SagaRecord, SagaSaveInput};
use crate::domain::{AnyCommand, DomainEvent, Event, ExecutionContext, Recorded};
use crate::error::{FrameworkError, Result};
use crate::kernel::ids::Uuid;

/// A saga type (spec §13.1): its name, its state, and which events it
/// reacts to. The type itself is never instantiated; the state lives in
/// [`SagaInstance`].
pub trait ProcessManager: Send + Sync + Sized + 'static {
    /// The saga type's name, stored with each saga and used for idempotency.
    const SAGA_TYPE: &'static str;

    /// The saga's own state, kept between events.
    type State: Default + Serialize + DeserializeOwned + Send + Sync + 'static;

    /// Registers the handlers (spec §13.1), e.g.
    /// `r.starts_when::<TransferRequested>(Self::start)`.
    fn routes(routes: &mut SagaRoutes<Self>);
}

/// What a saga handler works on: the saga's state and compensations.
pub struct SagaCtx<P: ProcessManager> {
    /// The saga's state. Handlers change it freely: it is saved after the
    /// commands are dispatched.
    pub state: P::State,
    saga_id: Uuid,
    compensations: CompensationStack,
    defs: Arc<CompensationDefs<P::State>>,
}

impl<P: ProcessManager> SagaCtx<P> {
    /// The saga's ID, the correlation ID of the event that started it.
    pub fn saga_id(&self) -> Uuid {
        self.saga_id
    }

    /// Records that a step needs undoing if the saga later fails: pushes the
    /// compensation `name`, defined with
    /// [`SagaRoutes::define_compensation`].
    pub fn register_compensation(&mut self, name: impl Into<String>) {
        self.compensations.push(name);
    }

    /// Pops every compensation, latest first, and returns the commands they
    /// build from the **current** state (§13.5).
    pub fn compensation_commands(&mut self) -> Result<Vec<AnyCommand>> {
        self.compensations.take_all(&self.defs, &self.state)
    }

    /// Pops only the latest compensation, for undoing step by step.
    pub fn pop_compensation(&mut self) -> Result<Vec<AnyCommand>> {
        self.compensations.pop(&self.defs, &self.state)
    }

    /// The compensations still pushed, oldest first.
    pub fn compensations(&self) -> &[String] {
        self.compensations.names()
    }
}

type Handler<P> =
    Arc<dyn Fn(&mut SagaCtx<P>, &DomainEvent) -> Result<Vec<AnyCommand>> + Send + Sync>;

/// A saga type's handlers, built from [`ProcessManager::routes`].
pub struct SagaRoutes<P: ProcessManager> {
    handlers: BTreeMap<(HandlerKind, &'static str), Handler<P>>,
    defs: Arc<CompensationDefs<P::State>>,
    error: Option<FrameworkError>,
}

impl<P: ProcessManager> SagaRoutes<P> {
    fn empty() -> Self {
        Self {
            handlers: BTreeMap::new(),
            defs: Arc::new(CompensationDefs::new()),
            error: None,
        }
    }

    /// Collects `P`'s routes. Fails with `INVALID_CONFIGURATION` if a kind
    /// has two handlers for one event, or the saga has no `starts_when`.
    pub fn build() -> Result<Arc<Self>> {
        let mut routes = Self::empty();
        P::routes(&mut routes);
        if let Some(error) = routes.error.take() {
            return Err(error.with_context("sagaType", P::SAGA_TYPE));
        }
        if !routes
            .handlers
            .keys()
            .any(|(k, _)| *k == HandlerKind::StartsWhen)
        {
            return Err(FrameworkError::invalid_configuration(format!(
                "saga {} has no starts_when handler",
                P::SAGA_TYPE
            )));
        }
        Ok(Arc::new(routes))
    }

    fn add<E, F>(&mut self, kind: HandlerKind, handler: F) -> &mut Self
    where
        E: Event,
        F: Fn(&mut SagaCtx<P>, &Recorded<E>) -> Result<Vec<AnyCommand>> + Send + Sync + 'static,
    {
        if self.handlers.contains_key(&(kind, E::NAME)) {
            self.error.get_or_insert_with(|| {
                FrameworkError::invalid_configuration(format!(
                    "saga {} has two {kind} handlers for {}",
                    P::SAGA_TYPE,
                    E::NAME
                ))
            });
            return self;
        }
        let wrapped: Handler<P> = Arc::new(move |ctx, event| {
            let recorded = event.downcast::<E>().ok_or_else(|| {
                FrameworkError::invalid_configuration(format!(
                    "the {kind} handler of {} was given a {}",
                    E::NAME,
                    event.name()
                ))
            })?;
            handler(ctx, &recorded)
        });
        self.handlers.insert((kind, E::NAME), wrapped);
        self
    }

    /// An event of type `E` starts a new saga.
    pub fn starts_when<E: Event>(
        &mut self,
        handler: impl Fn(&mut SagaCtx<P>, &Recorded<E>) -> Result<Vec<AnyCommand>>
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        self.add(HandlerKind::StartsWhen, handler)
    }

    /// An event of type `E` is an intermediate step.
    pub fn continue_when<E: Event>(
        &mut self,
        handler: impl Fn(&mut SagaCtx<P>, &Recorded<E>) -> Result<Vec<AnyCommand>>
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        self.add(HandlerKind::ContinueWhen, handler)
    }

    /// An event of type `E` starts or continues compensation.
    pub fn compensate_when<E: Event>(
        &mut self,
        handler: impl Fn(&mut SagaCtx<P>, &Recorded<E>) -> Result<Vec<AnyCommand>>
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        self.add(HandlerKind::CompensateWhen, handler)
    }

    /// An event of type `E` completes the saga.
    pub fn completed_when<E: Event>(
        &mut self,
        handler: impl Fn(&mut SagaCtx<P>, &Recorded<E>) -> Result<Vec<AnyCommand>>
        + Send
        + Sync
        + 'static,
    ) -> &mut Self {
        self.add(HandlerKind::CompletedWhen, handler)
    }

    /// Defines the compensation `name`: commands built from the state when
    /// compensation runs (§17).
    pub fn define_compensation(
        &mut self,
        name: impl Into<String>,
        factory: impl Fn(&P::State) -> Vec<AnyCommand> + Send + Sync + 'static,
    ) -> &mut Self {
        if let Some(defs) = Arc::get_mut(&mut self.defs) {
            defs.define(name, factory);
        }
        self
    }

    /// The handler kind that wins for an event: completed, then compensate,
    /// then continue, then starts (§13.1).
    pub fn kind_for(&self, event_name: &str) -> Option<HandlerKind> {
        [
            HandlerKind::CompletedWhen,
            HandlerKind::CompensateWhen,
            HandlerKind::ContinueWhen,
            HandlerKind::StartsWhen,
        ]
        .into_iter()
        .find(|kind| {
            self.handlers
                .keys()
                .any(|(k, n)| k == kind && *n == event_name)
        })
    }

    /// Whether the event can start a saga.
    pub fn starts_on(&self, event_name: &str) -> bool {
        self.handlers
            .keys()
            .any(|(k, n)| *k == HandlerKind::StartsWhen && *n == event_name)
    }

    /// Whether the saga has a handler of this kind for the event.
    pub fn handles(&self, kind: HandlerKind, event_name: &str) -> bool {
        self.handlers
            .keys()
            .any(|(k, n)| *k == kind && *n == event_name)
    }

    /// Every event name any handler reacts to, alphabetical.
    pub fn event_names(&self) -> Vec<&'static str> {
        let mut names: Vec<_> = self.handlers.keys().map(|(_, n)| *n).collect();
        names.sort_unstable();
        names.dedup();
        names
    }

    /// The compensations defined so far.
    pub fn compensations(&self) -> &CompensationDefs<P::State> {
        &self.defs
    }
}

impl<P: ProcessManager> fmt::Debug for SagaRoutes<P> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SagaRoutes")
            .field("sagaType", &P::SAGA_TYPE)
            .field("handlers", &self.handlers.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// One saga: its ID, state, status and compensations (spec §13.1).
///
/// After [`handle`](Self::handle) fails, drop the instance: its state may be
/// half changed. A saga handler event loads a fresh one from the store.
pub struct SagaInstance<P: ProcessManager> {
    routes: Arc<SagaRoutes<P>>,
    ctx: SagaCtx<P>,
    state: SagaState,
    current_step: Option<String>,
    failure_reason: Option<String>,
}

impl<P: ProcessManager> SagaInstance<P> {
    /// A saga that hasn't started, with the given ID.
    pub fn new(routes: Arc<SagaRoutes<P>>, saga_id: Uuid) -> Self {
        Self {
            ctx: SagaCtx {
                state: P::State::default(),
                saga_id,
                compensations: CompensationStack::new(),
                defs: routes.defs.clone(),
            },
            routes,
            state: SagaState::Initial,
            current_step: None,
            failure_reason: None,
        }
    }

    /// A saga read back from the store. A state that doesn't fit is
    /// `SERIALIZATION_ERROR`.
    pub fn restore(routes: Arc<SagaRoutes<P>>, record: SagaRecord) -> Result<Self> {
        let mut saga = Self::new(routes, record.saga_id);
        let bad = |what: &str, e: serde_json::Error| {
            FrameworkError::serialization_error(format!(
                "the {what} of saga {} {} can't be read: {e}",
                P::SAGA_TYPE,
                record.saga_id
            ))
            .with_source(e)
        };
        let state = record.state.get("state").cloned().unwrap_or(Value::Null);
        saga.ctx.state = serde_json::from_value(state).map_err(|e| bad("state", e))?;
        let names: Vec<String> = match record.state.get("compensations") {
            Some(v) => serde_json::from_value(v.clone()).map_err(|e| bad("compensations", e))?,
            None => Vec::new(),
        };
        saga.ctx.compensations = CompensationStack::from_names(names);
        saga.state = record.status.into();
        saga.current_step = Some(record.current_step);
        saga.failure_reason = record.failure_reason;
        Ok(saga)
    }

    /// The saga's ID.
    pub fn saga_id(&self) -> Uuid {
        self.ctx.saga_id
    }

    /// The saga's business state.
    pub fn data(&self) -> &P::State {
        &self.ctx.state
    }

    /// Where the saga is in its life.
    pub fn state(&self) -> SagaState {
        self.state
    }

    /// The last step that ran, `"<kind><EventName>"`, or `None` before the
    /// first.
    pub fn current_step(&self) -> Option<&str> {
        self.current_step.as_deref()
    }

    /// Why the saga failed, if it has.
    pub fn failure_reason(&self) -> Option<&str> {
        self.failure_reason.as_deref()
    }

    /// The compensations still pushed, oldest first.
    pub fn compensations(&self) -> &[String] {
        self.ctx.compensations()
    }

    /// Whether the saga has a handler for the event.
    pub fn handles(&self, event: &DomainEvent) -> bool {
        self.routes.kind_for(event.name()).is_some()
    }

    /// Handles an event (spec §13.1) and returns the commands to dispatch.
    ///
    /// - a saga that has ended: `SAGA_ALREADY_COMPLETED`;
    /// - no handler: `SAGA_NOT_STARTED` for a saga that hasn't started, else
    ///   no commands;
    /// - a transition the state machine forbids: its error.
    ///
    /// The handler runs under [`ExecutionContext::for_event`], so the
    /// commands it creates carry the saga's ID as correlation and the event
    /// as cause. Each command then gets an ID derived from the saga type,
    /// the event and its position, so a redelivered event recreates the same
    /// IDs and command idempotency skips them.
    pub fn handle(&mut self, event: &DomainEvent) -> Result<Vec<AnyCommand>> {
        if self.state.is_terminal() {
            return Err(FrameworkError::saga_already_completed(format!(
                "saga {} has already ended",
                self.ctx.saga_id
            ))
            .with_context("sagaType", P::SAGA_TYPE)
            .with_context("eventType", event.name()));
        }
        let Some(kind) = self.routes.kind_for(event.name()) else {
            if self.current_step.is_none() {
                return Err(FrameworkError::saga_not_started(format!(
                    "saga {} has no handler to start with {}",
                    P::SAGA_TYPE,
                    event.name()
                ))
                .with_context("eventType", event.name()));
            }
            return Ok(Vec::new());
        };
        let next = transition(self.state, kind).map_err(|e| {
            e.with_context("sagaType", P::SAGA_TYPE)
                .with_context("eventType", event.name())
        })?;
        let handler = self.routes.handlers[&(kind, event.name())].clone();
        let commands = ExecutionContext::for_event(event.envelope())
            .sync_scope(|| handler(&mut self.ctx, event))?;

        let seed = format!("{}/{}", P::SAGA_TYPE, event.id());
        let at = event.envelope().occurred_at;
        let commands = commands
            .into_iter()
            .enumerate()
            .map(|(i, c)| c.with_derived_id(&seed, u32::try_from(i).unwrap_or(u32::MAX), at))
            .collect();

        self.state = next;
        self.current_step = Some(format!("{}{}", kind.step_prefix(), event.name()));
        Ok(commands)
    }

    /// Ends the saga as `Failed` with a reason, as the timeout scheduler
    /// does (§19). A saga that has ended already is `SAGA_ALREADY_COMPLETED`.
    pub fn mark_failed(&mut self, reason: impl Into<String>) -> Result<()> {
        if self.state.is_terminal() {
            return Err(FrameworkError::saga_already_completed(format!(
                "saga {} has already ended",
                self.ctx.saga_id
            )));
        }
        self.state = SagaState::Failed;
        self.failure_reason = Some(reason.into());
        Ok(())
    }

    /// The input for [`SagaStoreAdapter::save`](crate::adapters::SagaStoreAdapter::save):
    /// the state together with the compensation names. A saga that hasn't
    /// started can't be saved (`INVALID_CONFIGURATION`).
    pub fn to_save_input(&self) -> Result<SagaSaveInput> {
        let status = self.state.status().ok_or_else(|| {
            FrameworkError::invalid_configuration("a saga that hasn't started can't be saved")
        })?;
        let state = serde_json::to_value(&self.ctx.state).map_err(|e| {
            FrameworkError::serialization_error(format!(
                "the state of saga {} can't be serialized: {e}",
                P::SAGA_TYPE
            ))
            .with_source(e)
        })?;
        Ok(SagaSaveInput {
            saga_id: self.ctx.saga_id,
            saga_type: P::SAGA_TYPE.to_owned(),
            correlation_id: self.ctx.saga_id,
            state: json!({ "state": state, "compensations": self.ctx.compensations.names() }),
            status,
            current_step: self.current_step.clone().unwrap_or_default(),
            failure_reason: self.failure_reason.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::adapters::SagaStatus;
    use crate::domain::{Command, CommandMessage, EventEnvelope};
    use crate::kernel::ids::new_v7;

    #[derive(Debug, Serialize, Deserialize)]
    struct Requested(i64);
    impl Event for Requested {
        const NAME: &'static str = "Requested";
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct Stepped;
    impl Event for Stepped {
        const NAME: &'static str = "Stepped";
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct Failed;
    impl Event for Failed {
        const NAME: &'static str = "Failed";
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct Done;
    impl Event for Done {
        const NAME: &'static str = "Done";
    }
    #[derive(Debug, Serialize, Deserialize)]
    struct Unrelated;
    impl Event for Unrelated {
        const NAME: &'static str = "Unrelated";
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Do(i64);
    impl Command for Do {
        const NAME: &'static str = "Do";
    }

    #[derive(Default, Serialize, Deserialize)]
    struct Data {
        amount: i64,
    }

    struct Demo;
    impl ProcessManager for Demo {
        const SAGA_TYPE: &'static str = "Demo";
        type State = Data;
        fn routes(r: &mut SagaRoutes<Self>) {
            r.starts_when::<Requested>(|ctx, e| {
                ctx.state.amount = e.0;
                ctx.register_compensation("undo");
                Ok(vec![CommandMessage::from(Do(1)).into(), Do(2).into()])
            });
            r.continue_when::<Stepped>(|_, _| Ok(vec![]));
            r.compensate_when::<Failed>(|ctx, _| ctx.compensation_commands());
            r.completed_when::<Done>(|_, _| Ok(vec![]));
            r.define_compensation("undo", |s| vec![Do(-s.amount).into()]);
        }
    }

    fn event<E: Event>(correlation: Uuid, e: E) -> DomainEvent {
        let mut envelope = EventEnvelope::new(Utc::now());
        envelope.correlation_id = correlation;
        DomainEvent::new(envelope, e)
    }

    fn saga() -> (SagaInstance<Demo>, Uuid) {
        let id = new_v7();
        (
            SagaInstance::new(SagaRoutes::<Demo>::build().unwrap(), id),
            id,
        )
    }

    #[test]
    fn commands_carry_the_saga_id_the_event_and_derived_ids() {
        let (mut s, id) = saga();
        let e = event(id, Requested(50));
        let commands = s.handle(&e).unwrap();
        assert_eq!(commands.len(), 2);
        for c in &commands {
            assert_eq!(c.envelope().correlation_id, id);
            assert_eq!(c.envelope().causation_id, Some(e.id()));
        }
        assert_ne!(commands[0].id(), commands[1].id());
        // A redelivery recreates the same IDs.
        let (mut again, _) = saga();
        let mut other = SagaInstance::<Demo>::new(SagaRoutes::build().unwrap(), id);
        let same = other.handle(&e).unwrap();
        assert_eq!(same[0].id(), commands[0].id());
        assert_eq!(same[1].id(), commands[1].id());
        let _ = again.handle(&event(again.saga_id(), Requested(1)));
        assert_eq!(s.state(), SagaState::Running);
        assert_eq!(s.current_step(), Some("startsWhenRequested"));
    }

    #[test]
    fn the_state_machine_applies_and_unhandled_events_are_ignored() {
        let (mut s, id) = saga();
        let err = s.handle(&event(id, Stepped)).unwrap_err();
        assert_eq!(err.code().as_str(), "SAGA_NOT_STARTED");
        let err = s.handle(&event(id, Unrelated)).unwrap_err();
        assert_eq!(err.code().as_str(), "SAGA_NOT_STARTED");
        s.handle(&event(id, Requested(5))).unwrap();
        assert!(s.handle(&event(id, Unrelated)).unwrap().is_empty());
        assert_eq!(s.current_step(), Some("startsWhenRequested"));
        let err = s.handle(&event(id, Requested(5))).unwrap_err();
        assert_eq!(err.code().as_str(), "SAGA_ALREADY_STARTED");
        s.handle(&event(id, Failed)).unwrap();
        assert_eq!(s.state(), SagaState::Compensating);
        s.handle(&event(id, Done)).unwrap();
        assert_eq!(s.state(), SagaState::Completed);
        let err = s.handle(&event(id, Stepped)).unwrap_err();
        assert_eq!(err.code().as_str(), "SAGA_ALREADY_COMPLETED");
    }

    #[test]
    fn compensation_uses_the_state_and_survives_a_save() {
        let (mut s, id) = saga();
        s.handle(&event(id, Requested(50))).unwrap();
        let input = s.to_save_input().unwrap();
        assert_eq!(input.status, SagaStatus::Running);
        assert_eq!(input.state["compensations"][0], "undo");
        let record = SagaRecord::saved(input, None, Utc::now());
        let mut back = SagaInstance::restore(SagaRoutes::<Demo>::build().unwrap(), record).unwrap();
        assert_eq!(back.data().amount, 50);
        let commands = back.handle(&event(id, Failed)).unwrap();
        assert_eq!(commands.len(), 1);
        assert_eq!(commands[0].downcast_ref::<Do>().unwrap().0, -50);
        assert!(back.compensations().is_empty());
    }

    #[test]
    fn mark_failed_ends_the_saga_and_unstarted_cant_be_saved() {
        let (mut s, id) = saga();
        assert!(s.to_save_input().is_err());
        s.handle(&event(id, Requested(1))).unwrap();
        s.mark_failed("timeout").unwrap();
        let input = s.to_save_input().unwrap();
        assert_eq!(input.status, SagaStatus::Failed);
        assert_eq!(input.failure_reason.as_deref(), Some("timeout"));
        assert!(s.mark_failed("again").is_err());
    }

    #[test]
    fn invalid_routes_are_rejected() {
        struct Twice;
        impl ProcessManager for Twice {
            const SAGA_TYPE: &'static str = "Twice";
            type State = Data;
            fn routes(r: &mut SagaRoutes<Self>) {
                r.starts_when::<Requested>(|_, _| Ok(vec![]));
                r.starts_when::<Requested>(|_, _| Ok(vec![]));
            }
        }
        struct NoStart;
        impl ProcessManager for NoStart {
            const SAGA_TYPE: &'static str = "NoStart";
            type State = Data;
            fn routes(r: &mut SagaRoutes<Self>) {
                r.continue_when::<Requested>(|_, _| Ok(vec![]));
            }
        }
        assert!(SagaRoutes::<Twice>::build().is_err());
        assert!(SagaRoutes::<NoStart>::build().is_err());
    }

    #[test]
    fn priority_is_completed_then_compensate_then_continue_then_starts() {
        struct Prio;
        impl ProcessManager for Prio {
            const SAGA_TYPE: &'static str = "Prio";
            type State = Data;
            fn routes(r: &mut SagaRoutes<Self>) {
                r.starts_when::<Requested>(|_, _| Ok(vec![]));
                r.continue_when::<Requested>(|_, _| Ok(vec![]));
                r.compensate_when::<Requested>(|_, _| Ok(vec![]));
                r.completed_when::<Requested>(|_, _| Ok(vec![]));
                r.starts_when::<Done>(|_, _| Ok(vec![]));
                r.continue_when::<Done>(|_, _| Ok(vec![]));
            }
        }
        let r = SagaRoutes::<Prio>::build().unwrap();
        assert_eq!(r.kind_for("Requested"), Some(HandlerKind::CompletedWhen));
        assert_eq!(r.kind_for("Done"), Some(HandlerKind::ContinueWhen));
        assert!(r.starts_on("Done"));
        assert_eq!(r.kind_for("Nope"), None);
    }
}
