//! The aggregate root (spec §5): a consistency boundary that changes state
//! only by raising events.

use std::any::type_name;
use std::borrow::Borrow;
use std::fmt;
use std::hash::Hash;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{AggregateId, DomainEvent, EventEnvelope, EventFamily, ExecutionContext};
use crate::error::{FrameworkError, Result};
use crate::kernel::clock;

/// The framework's bookkeeping inside an aggregate: the version, the events
/// raised since the last save, and the execution context the repository set.
///
/// An aggregate embeds one, starting from [`AggregateCore::new`], and hands
/// it out through [`Aggregate::core`] and [`Aggregate::core_mut`]. Only the
/// [`Aggregate`] methods change it.
#[derive(Debug, Default)]
pub struct AggregateCore {
    version: u64,
    uncommitted: Vec<DomainEvent>,
    context: Option<ExecutionContext>,
}

impl AggregateCore {
    /// An empty core: version 0, nothing raised, no context.
    pub fn new() -> Self {
        Self::default()
    }
}

/// An aggregate's state as of a version, as [`Aggregate::to_snapshot`]
/// returns it (spec §5.4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotData {
    /// The aggregate's [`Snapshot`](Aggregate::Snapshot) as JSON.
    pub data: Value,
    /// The version the state is at.
    pub version: u64,
}

/// An aggregate root (spec §5).
///
/// The aggregate's methods check invariants, then call
/// [`raise`](Self::raise), which applies the event and queues it for the
/// repository to store. [`apply`](Self::apply) is an exhaustive `match` over
/// the aggregate's [event family](EventFamily), and can't fail: everything
/// that could reject a change is checked before `raise`.
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use tarodomain::domain::{Aggregate, AggregateCore, Event};
/// use tarodomain::error::{ErrorCategory, FrameworkError, Result};
/// use tarodomain::{aggregate_id, event_family};
///
/// aggregate_id!(pub AccountId);
///
/// #[derive(Debug, Serialize, Deserialize)]
/// pub struct MoneyDeposited {
///     pub cents: i64,
/// }
///
/// impl Event for MoneyDeposited {
///     const NAME: &'static str = "MoneyDeposited";
/// }
///
/// event_family!(pub enum AccountEvent { MoneyDeposited });
///
/// pub struct BankAccount {
///     id: AccountId,
///     core: AggregateCore,
///     balance: i64,
/// }
///
/// impl BankAccount {
///     pub fn deposit(&mut self, cents: i64) -> Result<()> {
///         if cents <= 0 {
///             let category = ErrorCategory::Domain;
///             return Err(FrameworkError::custom("INVALID_AMOUNT", category, "must be positive"));
///         }
///         self.raise(MoneyDeposited { cents });
///         Ok(())
///     }
/// }
///
/// impl Aggregate for BankAccount {
///     type Id = AccountId;
///     type Event = AccountEvent;
///     type Snapshot = i64;
///     const TYPE: &'static str = "BankAccount";
///
///     fn new(id: AccountId) -> Self {
///         Self { id, core: AggregateCore::new(), balance: 0 }
///     }
///     fn id(&self) -> AccountId {
///         self.id
///     }
///     fn core(&self) -> &AggregateCore {
///         &self.core
///     }
///     fn core_mut(&mut self) -> &mut AggregateCore {
///         &mut self.core
///     }
///     fn apply(&mut self, event: &AccountEvent) {
///         match event {
///             AccountEvent::MoneyDeposited(e) => self.balance += e.cents,
///         }
///     }
///     fn snapshot_state(&self) -> i64 {
///         self.balance
///     }
///     fn restore_snapshot_state(&mut self, balance: i64) {
///         self.balance = balance;
///     }
/// }
///
/// let mut account = BankAccount::new(AccountId::generate());
/// account.deposit(250).unwrap();
/// assert_eq!((account.balance, account.version()), (250, 1));
/// assert!(account.deposit(0).is_err());
/// ```
pub trait Aggregate: Send + Sync + Sized + 'static {
    /// The ID type, usually declared with [`aggregate_id!`](crate::aggregate_id).
    type Id: Copy
        + Eq
        + Hash
        + fmt::Debug
        + fmt::Display
        + Into<AggregateId>
        + Send
        + Sync
        + 'static;

    /// The events the aggregate raises and applies, declared with
    /// [`event_family!`](crate::event_family).
    type Event: EventFamily;

    /// The business state stored in a snapshot (§5.4), without the ID and
    /// version.
    type Snapshot: Serialize + DeserializeOwned;

    /// The aggregate type name, the prefix of its stream IDs
    /// (`BankAccount-<uuid>`, §3.1). Must not contain `-`, and must not
    /// change once events are stored.
    const TYPE: &'static str;

    /// An empty aggregate at version 0 (§5.3).
    ///
    /// Must not raise events: repositories use it to rehydrate. Static
    /// factories such as `BankAccount::open(id, owner)` call it, then raise.
    fn new(id: Self::Id) -> Self;

    /// The aggregate's ID.
    fn id(&self) -> Self::Id;

    /// The embedded [`AggregateCore`].
    fn core(&self) -> &AggregateCore;

    /// The embedded [`AggregateCore`], mutably.
    fn core_mut(&mut self) -> &mut AggregateCore;

    /// Updates the state for `event`. Called by [`raise`](Self::raise) and
    /// when replaying history. Must not raise events.
    fn apply(&mut self, event: &Self::Event);

    /// The business state for a snapshot.
    fn snapshot_state(&self) -> Self::Snapshot;

    /// Restores the business state from a snapshot, on an aggregate fresh
    /// from [`new`](Self::new).
    fn restore_snapshot_state(&mut self, state: Self::Snapshot);

    /// Applies `event` and queues it for persistence (§5.2). Call it from
    /// the aggregate's own methods, after checking invariants.
    ///
    /// The envelope comes from the aggregate's own execution context, which
    /// the repository sets on load; else from the context in scope
    /// ([`ExecutionContext::current`]); else the event starts a new trace.
    /// The time comes from the clock in scope ([`clock::now`]).
    fn raise(&mut self, event: impl Into<Self::Event>) {
        let event = event.into();
        self.apply(&event);
        let core = self.core_mut();
        let ambient;
        let context = match &core.context {
            Some(own) => Some(own),
            None => {
                ambient = ExecutionContext::current();
                ambient.as_ref()
            }
        };
        let envelope = EventEnvelope::from_context(context, clock::now());
        core.version += 1;
        core.uncommitted.push(event.into_domain(envelope));
    }

    /// The number of events applied, including uncommitted ones.
    fn version(&self) -> u64 {
        self.core().version
    }

    /// The version as last stored: the expected version for the next save.
    fn committed_version(&self) -> u64 {
        let core = self.core();
        core.version - core.uncommitted.len() as u64
    }

    /// Whether events were raised since the last save.
    fn has_uncommitted_changes(&self) -> bool {
        !self.core().uncommitted.is_empty()
    }

    /// The events raised since the last save, oldest first.
    fn uncommitted_changes(&self) -> &[DomainEvent] {
        &self.core().uncommitted
    }

    /// Clears the uncommitted events once they are stored.
    fn mark_changes_as_committed(&mut self) {
        self.core_mut().uncommitted.clear();
    }

    /// Replays stored events without queueing them (§5.1). Replay continues
    /// from the current version: 0, or the snapshot's version after
    /// [`restore_snapshot`](Self::restore_snapshot).
    ///
    /// Fails with `MISSING_EVENT_HANDLER` for an event outside the family,
    /// and with `INVALID_AGGREGATE_STATE` if there are uncommitted changes.
    /// After an error the aggregate is partly replayed; discard it.
    fn load_from_history<I>(&mut self, events: I) -> Result<()>
    where
        I: IntoIterator,
        I::Item: Borrow<DomainEvent>,
    {
        check_no_uncommitted(self, "load_from_history")?;
        for event in events {
            let event = event.borrow();
            let Some(member) = Self::Event::from_domain(event) else {
                return Err(FrameworkError::missing_event_handler(format!(
                    "{} has no handler for {}",
                    Self::TYPE,
                    event.name()
                ))
                .with_context("aggregateType", Self::TYPE)
                .with_context("eventType", event.name())
                .with_context("eventId", event.id().to_string()));
            };
            self.apply(&member);
            self.core_mut().version += 1;
        }
        Ok(())
    }

    /// The business state and version, for a snapshot store (§5.4).
    ///
    /// Fails with `SERIALIZATION_ERROR` if the state has no JSON form.
    fn to_snapshot(&self) -> Result<SnapshotData> {
        let data = serde_json::to_value(self.snapshot_state()).map_err(|error| {
            FrameworkError::serialization_error(format!(
                "{} snapshot can't be serialized: {error}",
                Self::TYPE
            ))
            .with_context("aggregateType", Self::TYPE)
            .with_source(error)
        })?;
        Ok(SnapshotData {
            data,
            version: self.version(),
        })
    }

    /// Restores the state from a snapshot taken at `version`, on an
    /// aggregate fresh from [`new`](Self::new).
    ///
    /// Fails with `SERIALIZATION_ERROR` if `data` doesn't fit
    /// [`Snapshot`](Self::Snapshot), leaving the aggregate unchanged, and
    /// with `INVALID_AGGREGATE_STATE` if there are uncommitted changes.
    fn restore_snapshot(&mut self, data: Value, version: u64) -> Result<()> {
        check_no_uncommitted(self, "restore_snapshot")?;
        let state = serde_json::from_value(data).map_err(|error| {
            FrameworkError::serialization_error(format!(
                "{} snapshot can't be read: {error}",
                Self::TYPE
            ))
            .with_context("aggregateType", Self::TYPE)
            .with_context("version", version)
            .with_source(error)
        })?;
        self.restore_snapshot_state(state);
        self.core_mut().version = version;
        Ok(())
    }

    /// Sets the version of an aggregate rebuilt from stored state rather than
    /// events, such as a row a state repository loaded (§6.3).
    ///
    /// Fails with `INVALID_AGGREGATE_STATE` if there are uncommitted changes.
    fn restore_version(&mut self, version: u64) -> Result<()> {
        check_no_uncommitted(self, "restore_version")?;
        self.core_mut().version = version;
        Ok(())
    }

    /// Sets the aggregate's own execution context, which
    /// [`raise`](Self::raise) prefers over the one in scope. Repositories call
    /// it on load.
    #[doc(hidden)]
    fn set_execution_context(&mut self, context: ExecutionContext) {
        self.core_mut().context = Some(context);
    }

    /// Clears the aggregate's own execution context. Repositories call it
    /// after a save.
    #[doc(hidden)]
    fn clear_execution_context(&mut self) {
        self.core_mut().context = None;
    }
}

fn check_no_uncommitted<A: Aggregate>(aggregate: &A, method: &str) -> Result<()> {
    let pending = aggregate.uncommitted_changes().len();
    if pending == 0 {
        return Ok(());
    }
    Err(FrameworkError::invalid_aggregate_state(format!(
        "{method} on {} ({}) with {pending} uncommitted events",
        A::TYPE,
        type_name::<A>()
    ))
    .with_context("aggregateType", A::TYPE)
    .with_context("aggregateId", aggregate.id().to_string()))
}
