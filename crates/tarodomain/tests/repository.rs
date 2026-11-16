//! Repositories (spec §6) on the local provider: event-sourced with
//! snapshots, and state-stored through an application table.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tarodomain::adapters::{
    Provider, Snapshot, SnapshotInput, SnapshotStoreAdapter, StreamIds, UnitOfWork,
};
use tarodomain::async_trait;
use tarodomain::bus::EventBus;
use tarodomain::domain::{
    Aggregate, AggregateCore, DomainEvent, Event, EventFamily, ExecutionContext, Metadata,
};
use tarodomain::error::{ErrorCode, FrameworkError, Result};
use tarodomain::event_sourcing::{EventRegistry, EventStore};
use tarodomain::kernel::ids::{Uuid, new_v7};
use tarodomain::repository::{
    EventSourcedRepository, Repository, StateRepository, StateStoredRepository,
};
use tarodomain::{aggregate_id, event_family};
use tarodomain_local::LocalProvider;

aggregate_id! {
    /// Identifies a bank account.
    pub AccountId
}

/// A bank account was opened.
#[derive(Debug, Serialize, Deserialize)]
pub struct AccountOpened {
    owner: String,
}

impl Event for AccountOpened {
    const NAME: &'static str = "AccountOpened";
}

/// Money was paid in.
#[derive(Debug, Serialize, Deserialize)]
pub struct MoneyDeposited {
    cents: i64,
}

impl Event for MoneyDeposited {
    const NAME: &'static str = "MoneyDeposited";
}

event_family!(
    /// Everything that happens to a bank account.
    pub enum AccountEvent {
        AccountOpened,
        MoneyDeposited,
    }
);

/// The account's business state, which is also its snapshot.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct AccountState {
    owner: String,
    balance: i64,
}

/// A bank account, event-sourced or state-stored depending on the test.
#[derive(Debug)]
pub struct BankAccount {
    id: AccountId,
    core: AggregateCore,
    state: AccountState,
}

impl BankAccount {
    fn open(id: AccountId, owner: &str) -> Self {
        let mut account = Self::new(id);
        account.raise(AccountOpened {
            owner: owner.into(),
        });
        account
    }

    fn deposit(&mut self, cents: i64) {
        self.raise(MoneyDeposited { cents });
    }
}

impl Aggregate for BankAccount {
    type Id = AccountId;
    type Event = AccountEvent;
    type Snapshot = AccountState;
    const TYPE: &'static str = "BankAccount";

    fn new(id: AccountId) -> Self {
        Self {
            id,
            core: AggregateCore::new(),
            state: AccountState::default(),
        }
    }

    fn id(&self) -> AccountId {
        self.id
    }

    fn core(&self) -> &AggregateCore {
        &self.core
    }

    fn core_mut(&mut self) -> &mut AggregateCore {
        &mut self.core
    }

    fn apply(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::AccountOpened(e) => self.state.owner = e.owner.clone(),
            AccountEvent::MoneyDeposited(e) => self.state.balance += e.cents,
        }
    }

    fn snapshot_state(&self) -> AccountState {
        self.state.clone()
    }

    fn restore_snapshot_state(&mut self, state: AccountState) {
        self.state = state;
    }
}

/// A bus that records what it is given, or fails when told to.
#[derive(Default)]
struct RecordingBus {
    published: Mutex<Vec<DomainEvent>>,
    fail: AtomicBool,
}

impl RecordingBus {
    fn ids(&self) -> Vec<Uuid> {
        let published = self.published.lock().unwrap();
        published.iter().map(DomainEvent::id).collect()
    }
}

#[async_trait]
impl EventBus for RecordingBus {
    fn name(&self) -> &str {
        "recording"
    }

    async fn publish_all(&self, events: &[DomainEvent]) -> Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            return Err(FrameworkError::event_publish_failed("the queue is down"));
        }
        self.published.lock().unwrap().extend_from_slice(events);
        Ok(())
    }
}

/// The local snapshot store, failing loads or saves when told to.
struct FlakySnapshots {
    inner: Arc<dyn SnapshotStoreAdapter>,
    fail_load: AtomicBool,
    fail_save: AtomicBool,
}

#[async_trait]
impl SnapshotStoreAdapter for FlakySnapshots {
    async fn save(&self, input: SnapshotInput) -> Result<()> {
        if self.fail_save.load(Ordering::SeqCst) {
            return Err(FrameworkError::snapshot_save_failed("disk full"));
        }
        self.inner.save(input).await
    }

    async fn load_latest(&self, source_uuid: Uuid, source_type: &str) -> Result<Option<Snapshot>> {
        if self.fail_load.load(Ordering::SeqCst) {
            return Err(FrameworkError::snapshot_load_failed("timeout"));
        }
        self.inner.load_latest(source_uuid, source_type).await
    }

    async fn load_at_version(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        version: u64,
    ) -> Result<Option<Snapshot>> {
        self.inner
            .load_at_version(source_uuid, source_type, version)
            .await
    }

    async fn list(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Snapshot>> {
        self.inner.list(source_uuid, source_type, limit).await
    }

    async fn delete_all(&self, source_uuid: Uuid, source_type: &str) -> Result<()> {
        self.inner.delete_all(source_uuid, source_type).await
    }

    async fn delete_before(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        version: u64,
    ) -> Result<u64> {
        self.inner
            .delete_before(source_uuid, source_type, version)
            .await
    }

    async fn count(&self, source_uuid: Uuid, source_type: &str) -> Result<u64> {
        self.inner.count(source_uuid, source_type).await
    }
}

/// The errors a repository reported to its hooks, as (operation, code).
#[derive(Clone, Default)]
struct Reported(Arc<Mutex<Vec<(String, String)>>>);

impl Reported {
    fn hook(&self) -> impl Fn(&FrameworkError) + Send + Sync + 'static {
        let reported = self.clone();
        move |error| {
            let operation = error.context()["operation"].as_str().unwrap().to_owned();
            assert_eq!(error.context()["aggregateType"], "BankAccount");
            let entry = (operation, error.code().as_str().to_owned());
            reported.0.lock().unwrap().push(entry);
        }
    }

    fn take(&self) -> Vec<(String, String)> {
        std::mem::take(&mut self.0.lock().unwrap())
    }
}

struct Fixture {
    provider: LocalProvider,
    store: EventStore,
    bus: Arc<RecordingBus>,
    snapshots: Arc<FlakySnapshots>,
    reported: Reported,
}

impl Fixture {
    fn new() -> Self {
        let provider = LocalProvider::new();
        let mut registry = EventRegistry::new();
        AccountEvent::register_all(&mut registry).unwrap();
        let store = EventStore::new(provider.create_event_store_adapter(), Arc::new(registry));
        let snapshots = Arc::new(FlakySnapshots {
            inner: provider.create_snapshot_store_adapter(),
            fail_load: AtomicBool::new(false),
            fail_save: AtomicBool::new(false),
        });
        Self {
            provider,
            store,
            bus: Arc::default(),
            snapshots,
            reported: Reported::default(),
        }
    }

    /// An event-sourced repository taking a snapshot every `snapshot_every`
    /// events.
    fn repository(&self, snapshot_every: u64) -> EventSourcedRepository<BankAccount> {
        EventSourcedRepository::new(self.store.clone(), self.bus.clone())
            .with_snapshots(self.snapshots.clone(), snapshot_every)
            .on_snapshot_error(self.reported.hook())
            .on_publish_error(self.reported.hook())
    }

    async fn stream_version(&self, id: AccountId) -> u64 {
        let stream_id = StreamIds::for_aggregate(BankAccount::TYPE, id);
        self.store.get_stream_version(&stream_id).await.unwrap()
    }

    async fn stored_ids(&self, id: AccountId) -> Vec<Uuid> {
        let stream_id = StreamIds::for_aggregate(BankAccount::TYPE, id);
        let events = self.store.load_stream(&stream_id, 0, None).await.unwrap();
        events.iter().map(DomainEvent::id).collect()
    }

    async fn snapshot_versions(&self, id: AccountId) -> Vec<u64> {
        let snapshots = self
            .snapshots
            .list(id.as_aggregate_id().uuid(), BankAccount::TYPE, None)
            .await
            .unwrap();
        snapshots.iter().map(|s| s.source_version).collect()
    }

    async fn put_snapshot(&self, id: AccountId, version: u64, data: Value) {
        let input = SnapshotInput {
            source_uuid: id.as_aggregate_id().uuid(),
            source_version: version,
            source_type: BankAccount::TYPE.into(),
            data,
            metadata: Metadata::new(),
        };
        self.snapshots.inner.save(input).await.unwrap();
    }
}

/// Opens an account with `deposits` and saves it.
async fn opened_with(repository: &impl Repository<BankAccount>, deposits: &[i64]) -> BankAccount {
    let mut account = BankAccount::open(AccountId::generate(), "Ann");
    for cents in deposits {
        account.deposit(*cents);
    }
    repository.save(&mut account).await.unwrap();
    account
}

#[tokio::test]
async fn a_missing_aggregate_is_not_found() {
    let fixture = Fixture::new();
    let repository = fixture.repository(0);
    let id = AccountId::generate();

    let error = repository.load(id).await.unwrap_err();
    assert_eq!(error.code(), &ErrorCode::AggregateNotFound);
    assert_eq!(error.context()["aggregateType"], "BankAccount");
    assert_eq!(error.context()["aggregateId"], id.to_string());
    assert!(repository.load_or_none(id).await.unwrap().is_none());
    assert!(!repository.exists(id).await.unwrap());
}

#[tokio::test]
async fn a_save_publishes_the_appended_events_and_a_load_replays_them() {
    let fixture = Fixture::new();
    let repository = fixture.repository(0);
    let mut account = opened_with(&repository, &[250, 100]).await;

    assert_eq!(fixture.bus.ids(), fixture.stored_ids(account.id).await);
    assert_eq!(fixture.bus.ids().len(), 3);
    assert_eq!((account.version(), account.committed_version()), (3, 3));
    assert!(!account.has_uncommitted_changes());

    let loaded = repository.load(account.id).await.unwrap();
    assert_eq!(loaded.state, account.state);
    assert_eq!(loaded.state.balance, 350);
    assert_eq!(loaded.version(), 3);
    assert!(repository.exists(account.id).await.unwrap());

    repository.save(&mut account).await.unwrap();
    assert_eq!(
        fixture.bus.ids().len(),
        3,
        "nothing to save, nothing published"
    );
    assert!(fixture.snapshot_versions(account.id).await.is_empty());
}

#[tokio::test]
async fn a_snapshot_is_taken_when_a_save_crosses_a_multiple() {
    let fixture = Fixture::new();
    let repository = fixture.repository(3);
    let mut account = opened_with(&repository, &[1]).await;
    assert!(
        fixture.snapshot_versions(account.id).await.is_empty(),
        "at 2"
    );

    account.deposit(2);
    account.deposit(3);
    repository.save(&mut account).await.unwrap();
    assert_eq!(fixture.snapshot_versions(account.id).await, [4], "2 → 4");

    account.deposit(4);
    repository.save(&mut account).await.unwrap();
    assert_eq!(fixture.snapshot_versions(account.id).await, [4], "4 → 5");

    account.deposit(5);
    repository.save(&mut account).await.unwrap();
    assert_eq!(fixture.snapshot_versions(account.id).await, [6, 4], "5 → 6");

    let snapshot = fixture
        .snapshots
        .load_latest(account.id.as_aggregate_id().uuid(), BankAccount::TYPE)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(snapshot.data, json!({"owner": "Ann", "balance": 15}));
    assert!(fixture.reported.take().is_empty());
}

#[tokio::test]
async fn a_load_restores_the_latest_snapshot_and_replays_the_tail() {
    let fixture = Fixture::new();
    let repository = fixture.repository(3);
    let mut account = opened_with(&repository, &[1, 2, 3]).await;
    account.deposit(10);
    repository.save(&mut account).await.unwrap();

    // Doctor the snapshot at version 4: only a load that starts from it,
    // and replays nothing but event 5, ends at 1010.
    let doctored = json!({"owner": "Ann", "balance": 1000});
    fixture.put_snapshot(account.id, 4, doctored).await;

    let loaded = repository.load(account.id).await.unwrap();
    assert_eq!(loaded.state.balance, 1010);
    assert_eq!(loaded.version(), 5);
    assert!(fixture.reported.take().is_empty());
}

#[tokio::test]
async fn a_snapshot_that_fails_to_load_falls_back_to_a_full_replay() {
    let fixture = Fixture::new();
    let repository = fixture.repository(2);
    let account = opened_with(&repository, &[5, 6]).await;
    fixture
        .put_snapshot(account.id, 2, json!({"owner": "X", "balance": 999}))
        .await;

    fixture.snapshots.fail_load.store(true, Ordering::SeqCst);
    let loaded = repository.load(account.id).await.unwrap();
    assert_eq!((loaded.state.balance, loaded.version()), (11, 3));
    let reported = fixture.reported.take();
    assert_eq!(
        reported,
        [("loadSnapshot".into(), "SNAPSHOT_LOAD_FAILED".into())]
    );
}

#[tokio::test]
async fn a_snapshot_that_doesnt_fit_falls_back_to_a_full_replay() {
    let fixture = Fixture::new();
    let repository = fixture.repository(0);
    let account = opened_with(&repository, &[5, 6]).await;
    fixture
        .put_snapshot(account.id, 2, json!({"balance": "lots"}))
        .await;

    let loaded = repository.load(account.id).await.unwrap();
    assert_eq!(loaded.state, account.state);
    assert_eq!(loaded.version(), 3);
    let reported = fixture.reported.take();
    assert_eq!(
        reported,
        [("restoreSnapshot".into(), "SERIALIZATION_ERROR".into())]
    );
}

#[tokio::test]
async fn a_failed_snapshot_save_doesnt_fail_the_save() {
    let fixture = Fixture::new();
    fixture.snapshots.fail_save.store(true, Ordering::SeqCst);
    let repository = fixture.repository(1);
    let account = opened_with(&repository, &[5]).await;

    assert_eq!(fixture.stream_version(account.id).await, 2);
    assert!(!account.has_uncommitted_changes());
    assert!(fixture.snapshot_versions(account.id).await.is_empty());
    let reported = fixture.reported.take();
    assert_eq!(
        reported,
        [("saveSnapshot".into(), "SNAPSHOT_SAVE_FAILED".into())]
    );
}

#[tokio::test]
async fn a_concurrent_save_conflicts() {
    let fixture = Fixture::new();
    let repository = fixture.repository(0);
    let account = opened_with(&repository, &[]).await;
    let published = fixture.bus.ids().len();

    let mut first = repository.load(account.id).await.unwrap();
    let mut second = repository.load(account.id).await.unwrap();
    first.deposit(5);
    second.deposit(7);
    repository.save(&mut first).await.unwrap();

    let error = repository.save(&mut second).await.unwrap_err();
    assert_eq!(error.code(), &ErrorCode::OptimisticConcurrency);
    assert!(error.retryable());
    assert_eq!(error.context()["expectedVersion"], 1);
    assert_eq!(error.context()["actualVersion"], 2);
    assert!(
        second.has_uncommitted_changes(),
        "the stale aggregate keeps its events"
    );
    assert_eq!(
        fixture.bus.ids().len(),
        published + 1,
        "only the winner published"
    );

    let mut retried = repository.load(account.id).await.unwrap();
    retried.deposit(7);
    repository.save(&mut retried).await.unwrap();
    assert_eq!(repository.load(account.id).await.unwrap().state.balance, 12);
}

#[tokio::test]
async fn a_loaded_aggregate_keeps_the_execution_context_until_saved() {
    let fixture = Fixture::new();
    let repository = fixture.repository(0);
    let id = opened_with(&repository, &[]).await.id;

    let context = ExecutionContext::new(new_v7());
    let mut account = context.clone().scope(repository.load(id)).await.unwrap();
    account.deposit(5);
    let raised = &account.uncommitted_changes()[0];
    assert_eq!(raised.envelope().correlation_id, context.correlation_id);

    repository.save(&mut account).await.unwrap();
    account.deposit(6);
    let raised = &account.uncommitted_changes()[0];
    assert_ne!(raised.envelope().correlation_id, context.correlation_id);
}

#[tokio::test]
async fn a_publish_failure_after_a_durable_append_is_reported_not_returned() {
    let fixture = Fixture::new();
    fixture.bus.fail.store(true, Ordering::SeqCst);
    let repository = fixture.repository(0);
    let account = opened_with(&repository, &[5]).await;

    assert_eq!(
        fixture.stream_version(account.id).await,
        2,
        "the append stays"
    );
    assert!(!account.has_uncommitted_changes());
    let reported = fixture.reported.take();
    assert_eq!(
        reported,
        [("publish".into(), "EVENT_PUBLISH_FAILED".into())]
    );
}

#[tokio::test]
async fn a_publish_failure_inside_a_unit_of_work_fails_the_save() {
    let fixture = Fixture::new();
    fixture.bus.fail.store(true, Ordering::SeqCst);
    let repository = fixture.repository(1);
    let mut account = BankAccount::open(AccountId::generate(), "Ann");

    let unit = fixture.provider.create_unit_of_work().unwrap();
    let error = unit
        .execute(async { repository.save(&mut account).await })
        .await
        .unwrap_err();
    assert_eq!(error.code(), &ErrorCode::EventPublishFailed);
    assert!(<dyn UnitOfWork>::active().is_none());

    assert_eq!(fixture.stream_version(account.id).await, 0, "rolled back");
    assert!(account.has_uncommitted_changes(), "not marked committed");
    assert!(fixture.snapshot_versions(account.id).await.is_empty());
    assert!(fixture.reported.take().is_empty());
}

/// The application's own table for state-stored accounts: the state and
/// version per ID, through `to_snapshot` and `restore_snapshot`.
#[derive(Default)]
struct AccountTable {
    rows: Mutex<HashMap<AccountId, (Value, u64)>>,
}

#[async_trait]
impl StateRepository<BankAccount> for AccountTable {
    async fn load(&self, id: AccountId) -> Result<Option<BankAccount>> {
        let row = self.rows.lock().unwrap().get(&id).cloned();
        let Some((data, version)) = row else {
            return Ok(None);
        };
        let mut account = BankAccount::new(id);
        account.restore_snapshot(data, version)?;
        Ok(Some(account))
    }

    async fn save_state(&self, account: &BankAccount) -> Result<()> {
        let snapshot = account.to_snapshot()?;
        let mut rows = self.rows.lock().unwrap();
        let stored = rows.get(&account.id).map_or(0, |(_, version)| *version);
        if stored != account.committed_version() {
            let table_id = format!("accounts/{}", account.id);
            let expected = account.committed_version();
            return Err(FrameworkError::optimistic_concurrency(
                table_id, expected, stored,
            ));
        }
        rows.insert(account.id, (snapshot.data, snapshot.version));
        Ok(())
    }
}

fn state_repository(
    bus: Arc<RecordingBus>,
    reported: &Reported,
) -> StateStoredRepository<BankAccount> {
    StateStoredRepository::new(Arc::new(AccountTable::default()), bus)
        .on_publish_error(reported.hook())
}

#[tokio::test]
async fn a_state_stored_aggregate_saves_its_state_and_publishes_its_events() {
    let bus = Arc::new(RecordingBus::default());
    let repository = state_repository(bus.clone(), &Reported::default());
    let account = opened_with(&repository, &[250]).await;

    assert_eq!(bus.ids().len(), 2);
    assert!(!account.has_uncommitted_changes());
    let loaded = repository.load(account.id).await.unwrap();
    assert_eq!((loaded.state.balance, loaded.version()), (250, 2));
    assert!(
        repository.exists(account.id).await.unwrap(),
        "defaults to load"
    );

    let missing = AccountId::generate();
    let error = repository.load(missing).await.unwrap_err();
    assert_eq!(error.code(), &ErrorCode::AggregateNotFound);
    assert!(!repository.exists(missing).await.unwrap());
}

#[tokio::test]
async fn a_stale_state_save_conflicts_and_publishes_nothing() {
    let bus = Arc::new(RecordingBus::default());
    let repository = state_repository(bus.clone(), &Reported::default());
    let id = opened_with(&repository, &[]).await.id;

    let context = ExecutionContext::new(new_v7());
    let (mut first, mut second) = context
        .clone()
        .scope(async {
            let first = repository.load(id).await.unwrap();
            let second = repository.load(id).await.unwrap();
            (first, second)
        })
        .await;
    first.deposit(5);
    second.deposit(7);
    assert_eq!(
        first.uncommitted_changes()[0].envelope().correlation_id,
        context.correlation_id,
        "a loaded aggregate adopts the context in scope",
    );
    repository.save(&mut first).await.unwrap();

    let error = repository.save(&mut second).await.unwrap_err();
    assert_eq!(error.code(), &ErrorCode::OptimisticConcurrency);
    assert!(second.has_uncommitted_changes());
    assert_eq!(bus.ids().len(), 2, "the opening and the winner's deposit");
    assert_eq!(repository.load(id).await.unwrap().state.balance, 5);
}

#[tokio::test]
async fn a_state_stored_publish_failure_is_reported_not_returned() {
    let bus = Arc::new(RecordingBus::default());
    bus.fail.store(true, Ordering::SeqCst);
    let reported = Reported::default();
    let repository = state_repository(bus, &reported);
    let account = opened_with(&repository, &[5]).await;

    assert_eq!(repository.load(account.id).await.unwrap().state.balance, 5);
    let reported = reported.take();
    assert_eq!(
        reported,
        [("publish".into(), "EVENT_PUBLISH_FAILED".into())]
    );
}
