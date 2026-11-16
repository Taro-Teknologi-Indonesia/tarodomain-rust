//! How one aggregate type is registered: event sourced or state stored, and
//! its snapshot interval (spec §25.1).

use std::sync::Arc;

use super::validation::{AggregateInfo, AggregateKind, Warning, WarningHook, WarningKind};
use crate::adapters::SnapshotStoreAdapter;
use crate::command::RepositoryRegistry;
use crate::di::Container;
use crate::domain::{Aggregate, EventFamily};
use crate::error::{FrameworkError, Result};
use crate::event_sourcing::{EventRegistry, EventStore};
use crate::repository::{
    EventSourcedRepository, Repository, StateRepository, StateStoredRepository,
};

type StateFactory<A> = Arc<dyn Fn(&Container) -> Result<Arc<dyn StateRepository<A>>> + Send + Sync>;

/// What a repository needs from the framework when it is built.
pub(crate) struct RepoWiring {
    pub store: EventStore,
    pub snapshots: Arc<dyn SnapshotStoreAdapter>,
    /// The global `snapshot_every`.
    pub snapshot_every: Option<u64>,
    pub warn: WarningHook,
    pub container: Container,
}

type Install = Box<dyn FnOnce(&mut RepositoryRegistry, &RepoWiring) -> Result<()> + Send>;

/// A configured aggregate, ready for the framework to register.
pub(crate) struct AggregateDecl {
    pub info: AggregateInfo,
    /// The event types the aggregate's family declares.
    pub events: &'static [&'static str],
    pub register_events: fn(&mut EventRegistry) -> Result<()>,
    pub warnings: Vec<Warning>,
    pub install: Install,
}

/// The settings of one aggregate, built inside
/// [`TaroDomainBuilder::aggregate`](super::TaroDomainBuilder::aggregate):
///
/// ```text
/// builder.aggregate::<BankAccount>(|a| a.event_sourced().snapshot_every(100))
/// builder.aggregate::<Ledger>(|a| a.state(|c| Ok(c.resolve(LEDGER_REPO)?)))
/// ```
///
/// An aggregate is event sourced unless `state` is called. Its event family
/// is registered with the framework automatically.
pub struct AggregateConfig<A: Aggregate> {
    state: Option<StateFactory<A>>,
    state_called: bool,
    snapshot_every: Option<u64>,
}

impl<A: Aggregate> AggregateConfig<A> {
    pub(crate) fn new() -> Self {
        Self {
            state: None,
            state_called: false,
            snapshot_every: None,
        }
    }

    /// Stores the aggregate as its events (§6.2). This is the default.
    #[must_use]
    pub fn event_sourced(mut self) -> Self {
        self.state = None;
        self.state_called = false;
        self
    }

    /// Stores the aggregate's state through the repository `factory` builds
    /// (§6.3). The factory runs once, when the framework is built.
    #[must_use]
    pub fn state(
        mut self,
        factory: impl Fn(&Container) -> Result<Arc<dyn StateRepository<A>>> + Send + Sync + 'static,
    ) -> Self {
        self.state = Some(Arc::new(factory));
        self.state_called = true;
        self
    }

    /// Takes a snapshot whenever a save crosses a multiple of `every`,
    /// instead of the global interval. Event-sourced aggregates only: on a
    /// state-stored one it is ignored, with a warning.
    #[must_use]
    pub fn snapshot_every(mut self, every: u64) -> Self {
        self.snapshot_every = Some(every);
        self
    }

    pub(crate) fn into_decl(self) -> AggregateDecl {
        let mut warnings = Vec::new();
        let kind = if self.state_called {
            AggregateKind::State {
                has_repository: self.state.is_some(),
            }
        } else {
            AggregateKind::EventSourced
        };
        if self.state_called && self.snapshot_every.is_some() {
            warnings.push(Warning::new(
                WarningKind::SnapshotOnStateAggregate,
                format!(
                    "snapshot_every is set on {}, which is state-stored and takes no snapshots",
                    A::TYPE
                ),
            ));
        }
        let (state, own_every) = (self.state, self.snapshot_every);
        let install: Install = Box::new(move |repositories, wiring| {
            let warn = wiring.warn.clone();
            let report = move |error: &FrameworkError| {
                warn(&Warning::new(
                    WarningKind::Recovered,
                    format!("{} recovered from an error: {error}", A::TYPE),
                ))
            };
            match state {
                Some(factory) => {
                    let repository = factory(&wiring.container)?;
                    repositories.register::<A>(move |bus| {
                        Arc::new(
                            StateStoredRepository::new(repository.clone(), bus)
                                .on_publish_error(report.clone()),
                        ) as Arc<dyn Repository<A>>
                    })
                }
                None => {
                    let every = own_every.or(wiring.snapshot_every).unwrap_or(0);
                    let (store, snapshots) = (wiring.store.clone(), wiring.snapshots.clone());
                    repositories.register::<A>(move |bus| {
                        Arc::new(
                            EventSourcedRepository::<A>::new(store.clone(), bus)
                                .with_snapshots(snapshots.clone(), every)
                                .on_snapshot_error(report.clone())
                                .on_publish_error(report.clone()),
                        ) as Arc<dyn Repository<A>>
                    })
                }
            }
        });
        AggregateDecl {
            info: AggregateInfo {
                name: A::TYPE,
                kind,
            },
            events: <A::Event as EventFamily>::names(),
            register_events: <A::Event as EventFamily>::register_all,
            warnings,
            install,
        }
    }
}
