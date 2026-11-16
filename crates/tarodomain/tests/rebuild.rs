//! Rebuilds (spec §20) and correlation traces (spec §28.3), after a
//! transfer saga, in both consistency modes.

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use tarodomain::adapters::ConsistencyMode::{self, Eventual, Strong};
use tarodomain::adapters::{Provider, SagaStatus};
use tarodomain::bus::EventHandler;
use tarodomain::catch_up::{CronDispatcher, SubscriptionManager};
use tarodomain::domain::{CommandMessage, DomainEvent, Recorded};
use tarodomain::error::{ErrorCategory, FrameworkError, Result};
use tarodomain::kernel::ids::{Uuid, new_v7};
use tarodomain::projection::{
    Projection, ProjectionHandler, ProjectionRoutes, ShardProjection, shard_handlers,
};
use tarodomain::rebuild::{
    ProjectionRebuilder, RebuildOptions, RebuildTarget, SagaRebuildOptions, SagaRebuilder, SagaType,
};
use tarodomain::trace::{CorrelationTrace, NodeKind};

use support::Bank;
use support::bank::{AccountId, MoneyDeposited, MoneyWithdrawn};
use support::transfer::{QuietSaga, RequestTransfer, TransferId, TransferSaga};

const MODES: [ConsistencyMode; 2] = [Strong, Eventual];

/// A read model of every deposit and withdrawal: event ID → (type, cents).
struct Ledger {
    name: &'static str,
    entries: Mutex<BTreeMap<Uuid, (String, i64)>>,
    hooks: Mutex<Vec<&'static str>>,
    fail_on: Mutex<Option<Uuid>>,
}

impl Ledger {
    fn new(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            name,
            entries: Mutex::default(),
            hooks: Mutex::default(),
            fail_on: Mutex::default(),
        })
    }

    fn record(&self, id: Uuid, kind: &str, cents: i64) -> Result<()> {
        if *self.fail_on.lock().unwrap() == Some(id) {
            return Err(FrameworkError::custom(
                "LEDGER_DOWN",
                ErrorCategory::Infrastructure,
                "down",
            ));
        }
        self.entries
            .lock()
            .unwrap()
            .insert(id, (kind.to_string(), cents));
        Ok(())
    }

    async fn deposited(self: Arc<Self>, e: Recorded<MoneyDeposited>) -> Result<()> {
        self.record(e.envelope.id, "deposit", e.cents)
    }

    async fn withdrawn(self: Arc<Self>, e: Recorded<MoneyWithdrawn>) -> Result<()> {
        self.record(e.envelope.id, "withdrawal", e.cents)
    }

    fn entries(&self) -> BTreeMap<Uuid, (String, i64)> {
        self.entries.lock().unwrap().clone()
    }

    fn hooks(&self) -> Vec<&'static str> {
        self.hooks.lock().unwrap().clone()
    }
}

#[tarodomain::async_trait]
impl Projection for Ledger {
    fn name(&self) -> &str {
        self.name
    }

    fn routes(r: &mut ProjectionRoutes<Self>) {
        r.on::<MoneyDeposited, _, _>(Self::deposited);
        r.on::<MoneyWithdrawn, _, _>(Self::withdrawn);
    }

    async fn on_rebuild_start(&self) -> Result<()> {
        self.entries.lock().unwrap().clear();
        self.hooks.lock().unwrap().push("start");
        Ok(())
    }

    async fn on_rebuild_complete(&self) -> Result<()> {
        self.hooks.lock().unwrap().push("complete");
        Ok(())
    }
}

impl ShardProjection for Ledger {
    fn shard_count(&self) -> u32 {
        3
    }

    fn shard_key(&self, event: &DomainEvent) -> String {
        event.envelope().id.to_string()
    }
}

/// A bank after one completed transfer of 30 from Ann to Bob, with the
/// saga's ID.
async fn after_transfer(mode: ConsistencyMode) -> (Bank, Uuid) {
    let bank = Bank::new(mode, None);
    let ann = bank.open("Ann").await;
    let bob = bank.open("Bob").await;
    bank.dispatcher
        .dispatch(support::deposit(ann, 100))
        .await
        .unwrap();
    bank.pump().await.unwrap();
    let saga_id = transfer(&bank, ann, bob, None).await;
    (bank, saga_id)
}

async fn transfer(bank: &Bank, from: AccountId, to: AccountId, fail_after: Option<&str>) -> Uuid {
    let saga_id = new_v7();
    let command = RequestTransfer {
        transfer: TransferId::generate(),
        from,
        to,
        cents: 30,
        fail_after: fail_after.map(str::to_owned),
    };
    let message = CommandMessage::new(command).with_correlation_id(saga_id);
    bank.dispatcher.dispatch(message).await.unwrap();
    bank.pump().await.unwrap();
    saga_id
}

fn cron(bank: &Bank) -> Arc<CronDispatcher> {
    Arc::new(CronDispatcher::new(
        bank.store.clone(),
        Arc::new(SubscriptionManager::new(bank.store.adapter().clone())),
        bank.provider.create_dead_letter_store(),
    ))
}

/// Registers a ledger with the dispatcher and catches it up.
async fn live(
    bank: &Bank,
    cron: &CronDispatcher,
    ledger: &Arc<Ledger>,
) -> Arc<ProjectionHandler<Ledger>> {
    let handler = Arc::new(
        ProjectionHandler::new(ledger.clone(), bank.provider.create_processed_event_store())
            .unwrap(),
    );
    cron.register(handler.clone(), None).unwrap();
    while cron.catch_up(ledger.name).await.unwrap().has_more {}
    handler
}

fn rebuilder(bank: &Bank, cron: &Arc<CronDispatcher>) -> ProjectionRebuilder {
    ProjectionRebuilder::new(cron.clone(), bank.provider.create_processed_event_store())
}

#[tokio::test]
async fn a_projection_rebuilt_in_place_matches_the_live_one() {
    for mode in MODES {
        let (bank, _) = after_transfer(mode).await;
        let cron = cron(&bank);
        let ledger = Ledger::new("ledger");
        let handler = live(&bank, &cron, &ledger).await;
        let before = ledger.entries();
        assert_eq!(before.len(), 3, "the deposit, the withdrawal, the deposit");

        let result = rebuilder(&bank, &cron)
            .rebuild(&*handler, &RebuildOptions::default())
            .await
            .unwrap();
        assert!(result.completed, "{mode:?}");
        let total = bank.store.load_all(None, None).await.unwrap().len();
        assert_eq!(
            result.processed, total,
            "every event is read, routed or not"
        );
        assert_eq!(ledger.entries(), before, "{mode:?}");
        assert_eq!(ledger.hooks(), ["start", "complete"]);
    }
}

#[tokio::test]
async fn a_projection_rebuilt_fresh_from_the_store_matches_the_live_one() {
    for mode in MODES {
        let (bank, _) = after_transfer(mode).await;
        let cron = cron(&bank);
        let live_ledger = Ledger::new("ledger");
        live(&bank, &cron, &live_ledger).await;

        let fresh = Ledger::new("ledger-fresh");
        let handler = Arc::new(
            ProjectionHandler::new(fresh.clone(), bank.provider.create_processed_event_store())
                .unwrap(),
        );
        cron.register(handler.clone(), None).unwrap();
        let result = rebuilder(&bank, &cron)
            .rebuild(&*handler, &RebuildOptions::default())
            .await
            .unwrap();
        assert!(result.completed);
        assert_eq!(fresh.entries(), live_ledger.entries(), "{mode:?}");
    }
}

#[tokio::test]
async fn without_resetting_idempotency_nothing_is_replayed() {
    let (bank, _) = after_transfer(Strong).await;
    let cron = cron(&bank);
    let ledger = Ledger::new("ledger");
    let handler = live(&bank, &cron, &ledger).await;
    ledger.entries.lock().unwrap().clear();

    let options = RebuildOptions {
        reset_idempotency: false,
        call_hooks: false,
        ..Default::default()
    };
    let result = rebuilder(&bank, &cron)
        .rebuild(&*handler, &options)
        .await
        .unwrap();
    assert!(result.completed);
    assert!(
        ledger.entries().is_empty(),
        "every event was already marked"
    );
    assert!(ledger.hooks().is_empty());
}

#[tokio::test]
async fn a_failing_event_stops_a_rebuild_unless_told_to_continue() {
    let (bank, _) = after_transfer(Strong).await;
    let cron = cron(&bank);
    let ledger = Ledger::new("ledger");
    let handler = live(&bank, &cron, &ledger).await;
    let failing = *ledger.entries().keys().nth(1).unwrap();
    let ids: Vec<Uuid> = bank
        .store
        .load_all(None, None)
        .await
        .unwrap()
        .iter()
        .map(DomainEvent::id)
        .collect();
    let position = ids.iter().position(|id| *id == failing).unwrap();
    *ledger.fail_on.lock().unwrap() = Some(failing);

    let stopped = rebuilder(&bank, &cron)
        .rebuild(&*handler, &RebuildOptions::default())
        .await
        .unwrap();
    assert!(!stopped.completed);
    assert_eq!(stopped.processed, position);
    assert!(stopped.failure.is_some());
    assert_eq!(ledger.hooks(), ["start"], "no completion hook");

    let options = RebuildOptions {
        continue_on_error: true,
        ..Default::default()
    };
    let go_on = rebuilder(&bank, &cron)
        .rebuild(&*handler, &options)
        .await
        .unwrap();
    assert!(go_on.completed);
    assert_eq!(go_on.skipped.len(), 1);
    assert_eq!(go_on.skipped[0].event_id, failing);
    assert_eq!(go_on.processed, ids.len() - 1);
    assert!(!ledger.entries().contains_key(&failing));
    assert_eq!(ledger.hooks(), ["start", "start", "complete"]);
}

#[tokio::test]
async fn batch_size_and_max_passes_bound_a_rebuild() {
    let (bank, _) = after_transfer(Strong).await;
    let cron = cron(&bank);
    let ledger = Ledger::new("ledger");
    let handler = live(&bank, &cron, &ledger).await;
    let rebuilder = rebuilder(&bank, &cron);

    let capped = RebuildOptions {
        batch_size: Some(1),
        max_passes: 2,
        ..Default::default()
    };
    let result = rebuilder.rebuild(&*handler, &capped).await.unwrap();
    assert!(!result.completed);
    assert_eq!((result.passes, result.processed), (2, 2));

    let batched = RebuildOptions {
        batch_size: Some(1),
        ..Default::default()
    };
    let result = rebuilder.rebuild(&*handler, &batched).await.unwrap();
    assert!(result.completed);
    let total = bank.store.load_all(None, None).await.unwrap().len();
    assert_eq!(
        result.passes, total,
        "one event a batch, and the last has no more"
    );
    assert_eq!(ledger.entries().len(), 3);
}

#[tokio::test]
async fn rebuild_all_stops_at_the_first_failure_unless_told_to_continue() {
    let (bank, _) = after_transfer(Strong).await;
    let cron = cron(&bank);
    let (first, second) = (Ledger::new("first"), Ledger::new("second"));
    let first_handler = live(&bank, &cron, &first).await;
    let second_handler = live(&bank, &cron, &second).await;
    *first.fail_on.lock().unwrap() = first.entries().keys().next().copied();
    let targets: [&dyn RebuildTarget; 2] = [&*first_handler, &*second_handler];

    let stopped = rebuilder(&bank, &cron)
        .rebuild_all(&targets, &RebuildOptions::default())
        .await
        .unwrap();
    assert_eq!(stopped.results.len(), 1);
    assert!(!stopped.completed());

    let options = RebuildOptions {
        continue_on_error: true,
        ..Default::default()
    };
    let all = rebuilder(&bank, &cron)
        .rebuild_all(&targets, &options)
        .await
        .unwrap();
    assert_eq!(all.results.len(), 2);
    assert!(all.completed());
}

#[tokio::test]
async fn shards_are_rebuilt_with_the_hooks_called_once() {
    for mode in MODES {
        let (bank, _) = after_transfer(mode).await;
        let cron = cron(&bank);
        let ledger = Ledger::new("sharded");
        let shards: Vec<_> =
            shard_handlers(ledger.clone(), bank.provider.create_processed_event_store())
                .unwrap()
                .into_iter()
                .map(Arc::new)
                .collect();
        for shard in &shards {
            cron.register(shard.clone(), None).unwrap();
        }
        for shard in &shards {
            while cron.catch_up(shard.name()).await.unwrap().has_more {}
        }
        let before = ledger.entries();
        assert_eq!(before.len(), 3);

        let owned: Vec<ProjectionHandler<Ledger>> =
            shard_handlers(ledger.clone(), bank.provider.create_processed_event_store()).unwrap();
        let result = rebuilder(&bank, &cron)
            .rebuild_shards(&owned, &RebuildOptions::default())
            .await
            .unwrap();
        assert!(result.completed(), "{mode:?}");
        assert_eq!(result.results.len(), 3);
        assert_eq!(ledger.entries(), before, "{mode:?}");
        assert_eq!(ledger.hooks(), ["start", "complete"], "once, not per shard");
    }
}

/// What a saga record says apart from its timestamps.
fn essence(
    record: &tarodomain::adapters::SagaRecord,
) -> (SagaStatus, String, serde_json::Value, Option<String>) {
    (
        record.status,
        record.current_step.clone(),
        record.state.clone(),
        record.failure_reason.clone(),
    )
}

fn saga_rebuilder(bank: &Bank) -> SagaRebuilder {
    SagaRebuilder::new(
        bank.store.clone(),
        bank.saga_store.clone(),
        bank.provider.create_processed_event_store(),
    )
}

#[tokio::test]
async fn a_saga_is_rebuilt_without_running_its_commands_again() {
    for mode in MODES {
        let (bank, saga_id) = after_transfer(mode).await;
        let stored = bank.saga_record(saga_id).await.unwrap();
        let events_before = bank.store.load_all(None, None).await.unwrap().len();
        // Corrupt the state.
        bank.saga_store
            .delete("TransferSaga", saga_id)
            .await
            .unwrap();

        let result = saga_rebuilder(&bank)
            .rebuild_instance::<TransferSaga>(saga_id)
            .await
            .unwrap();
        assert_eq!(result.events_replayed, 4, "{mode:?}");
        assert_eq!(result.status, Some(SagaStatus::Completed));
        let rebuilt = bank.saga_record(saga_id).await.unwrap();
        assert_eq!(essence(&rebuilt), essence(&stored), "{mode:?}");
        // The commands were discarded: no new events, no new money moves.
        assert_eq!(
            bank.store.load_all(None, None).await.unwrap().len(),
            events_before
        );
    }
}

#[tokio::test]
async fn a_rebuild_replaces_state_that_is_wrong() {
    let (bank, saga_id) = after_transfer(Eventual).await;
    let stored = bank.saga_record(saga_id).await.unwrap();
    let mut wrong = tarodomain::adapters::SagaSaveInput {
        saga_id,
        saga_type: "TransferSaga".into(),
        correlation_id: saga_id,
        state: serde_json::json!({ "garbage": true }),
        status: SagaStatus::Running,
        current_step: "nonsense".into(),
        failure_reason: None,
    };
    wrong.state = serde_json::json!({ "garbage": true });
    bank.saga_store.save(wrong).await.unwrap();

    saga_rebuilder(&bank)
        .rebuild_instance::<TransferSaga>(saga_id)
        .await
        .unwrap();
    assert_eq!(
        essence(&bank.saga_record(saga_id).await.unwrap()),
        essence(&stored)
    );
}

#[tokio::test]
async fn a_compensating_saga_is_rebuilt_as_it_was() {
    let bank = Bank::new(Eventual, None);
    let ann = bank.open("Ann").await;
    let bob = bank.open("Bob").await;
    bank.dispatcher
        .dispatch(support::deposit(ann, 100))
        .await
        .unwrap();
    bank.pump().await.unwrap();
    let saga_id = transfer(&bank, ann, bob, Some("deposit")).await;
    let stored = bank.saga_record(saga_id).await.unwrap();
    assert_eq!(stored.status, SagaStatus::Compensating);

    saga_rebuilder(&bank)
        .rebuild_instance::<TransferSaga>(saga_id)
        .await
        .unwrap();
    assert_eq!(
        essence(&bank.saga_record(saga_id).await.unwrap()),
        essence(&stored)
    );
}

#[tokio::test]
async fn rebuilding_a_type_finds_its_sagas_by_their_start_events() {
    let (bank, saga_id) = after_transfer(Strong).await;
    let rebuilder = saga_rebuilder(&bank);
    assert_eq!(
        rebuilder.find_instances::<TransferSaga>().await.unwrap(),
        vec![saga_id]
    );
    let stored = bank.saga_record(saga_id).await.unwrap();
    bank.saga_store
        .delete("TransferSaga", saga_id)
        .await
        .unwrap();

    let all = rebuilder
        .rebuild_type::<TransferSaga>(&SagaRebuildOptions::default())
        .await
        .unwrap();
    assert_eq!(all.saga_type, "TransferSaga");
    assert_eq!(all.rebuilt.len(), 1);
    assert!(all.failed.is_empty());
    assert_eq!(
        essence(&bank.saga_record(saga_id).await.unwrap()),
        essence(&stored)
    );

    let transfer_saga = SagaType::<TransferSaga>::new();
    let quiet = SagaType::<QuietSaga>::new();
    let everything = rebuilder
        .rebuild_all(&[&transfer_saga, &quiet], &SagaRebuildOptions::default())
        .await
        .unwrap();
    assert_eq!(everything.len(), 2);
    assert_eq!(everything[1].saga_type, "QuietSaga");
    assert_eq!(everything[1].rebuilt.len(), 1);
}

#[tokio::test]
async fn the_transfer_saga_traces_as_one_chain_from_the_root_command() {
    for mode in MODES {
        let (bank, saga_id) = after_transfer(mode).await;
        let entries = CorrelationTrace::trace(&**bank.store.adapter(), saga_id)
            .await
            .unwrap();
        assert_eq!(entries.len(), 4);
        let tree = CorrelationTrace::build_tree(&entries);
        assert_eq!(tree.roots.len(), 1, "{mode:?}");

        // command → event → command → event → …, one child at each step.
        let mut path = Vec::new();
        let mut node = &tree.roots[0];
        loop {
            path.push((node.kind, node.name.clone()));
            match node.children.as_slice() {
                [] => break,
                [only] => node = only,
                more => panic!("{mode:?}: {} children under {:?}", more.len(), node.name),
            }
        }
        let event = |name: &str| (NodeKind::Event, Some(name.to_string()));
        assert_eq!(
            path,
            vec![
                (NodeKind::Command, None),
                event("TransferRequested"),
                (NodeKind::Command, None),
                event("MoneyWithdrawn"),
                (NodeKind::Command, None),
                event("MoneyDeposited"),
                (NodeKind::Command, None),
                event("TransferCompleted"),
            ],
            "{mode:?}"
        );
        let text = CorrelationTrace::format_tree(&tree);
        assert_eq!(text.lines().count(), 8);
        assert!(text.lines().last().unwrap().contains("TransferCompleted"));
    }
}
