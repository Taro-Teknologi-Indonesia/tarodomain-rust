//! Catch-up (spec §18): subscriptions and the cron dispatcher on the local
//! provider.

mod support;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tarodomain::adapters::{ConsistencyMode, Provider, StreamIds};
use tarodomain::async_trait;
use tarodomain::bus::EventHandler;
use tarodomain::catch_up::{CronDispatcher, SubscriptionManager};
use tarodomain::domain::DomainEvent;
use tarodomain::error::{ErrorCategory, ErrorCode, FrameworkError, Result};
use tarodomain::kernel::ids::{Uuid, new_v7};

use support::bank::AccountId;
use support::{Bank, deposit};

/// Records the events it handles, and fails on the ones it is told to.
struct Recorder {
    name: &'static str,
    seen: Mutex<Vec<Uuid>>,
    fail: Mutex<HashMap<Uuid, ErrorCategory>>,
    provider: Option<Arc<dyn Provider>>,
}

impl Recorder {
    fn new(name: &'static str) -> Arc<Self> {
        Arc::new(Self {
            name,
            seen: Mutex::default(),
            fail: Mutex::default(),
            provider: None,
        })
    }

    /// A recorder that also writes a processed-event row before it can fail,
    /// to show whether the write survives the failure.
    fn writing(name: &'static str, provider: Arc<dyn Provider>) -> Arc<Self> {
        Arc::new(Self {
            name,
            seen: Mutex::default(),
            fail: Mutex::default(),
            provider: Some(provider),
        })
    }

    fn fail_on(&self, id: Uuid, category: ErrorCategory) {
        self.fail.lock().unwrap().insert(id, category);
    }

    fn recover(&self, id: Uuid) {
        self.fail.lock().unwrap().remove(&id);
    }

    fn seen(&self) -> Vec<Uuid> {
        self.seen.lock().unwrap().clone()
    }
}

#[async_trait]
impl EventHandler for Recorder {
    fn name(&self) -> &str {
        self.name
    }

    async fn handle(&self, event: &DomainEvent) -> Result<()> {
        if let Some(provider) = &self.provider {
            provider
                .create_processed_event_store()
                .mark_processed("side-effect", "", event.id())
                .await?;
        }
        let category = self.fail.lock().unwrap().get(&event.id()).copied();
        if let Some(category) = category {
            return Err(FrameworkError::custom("HANDLER_DOWN", category, "down"));
        }
        self.seen.lock().unwrap().push(event.id());
        Ok(())
    }
}

/// A bank in eventual mode, so its events are stored and nothing handles
/// them, with `deposits` deposits on one account.
async fn stored_events(deposits: usize) -> (Bank, Vec<Uuid>) {
    let bank = Bank::new(ConsistencyMode::Eventual, None);
    let account = bank.open("ada").await;
    for _ in 0..deposits {
        bank.dispatcher.dispatch(deposit(account, 1)).await.unwrap();
    }
    let ids = all_ids(&bank).await;
    (bank, ids)
}

async fn all_ids(bank: &Bank) -> Vec<Uuid> {
    bank.store
        .adapter()
        .load_all(None, None)
        .await
        .unwrap()
        .iter()
        .map(|event| event.event_id)
        .collect()
}

fn manager(bank: &Bank) -> Arc<SubscriptionManager> {
    Arc::new(SubscriptionManager::new(bank.store.adapter().clone()))
}

fn dispatcher(bank: &Bank) -> CronDispatcher {
    CronDispatcher::new(
        bank.store.clone(),
        manager(bank),
        bank.provider.create_dead_letter_store(),
    )
}

#[tokio::test]
async fn catches_up_in_batches_and_moves_the_cursor() {
    let (bank, ids) = stored_events(4).await; // 5 events
    let cron = dispatcher(&bank).with_batch_size(2);
    let handler = Recorder::new("projector");
    cron.register(handler.clone(), None).unwrap();

    let first = cron.catch_up("projector").await.unwrap();
    assert_eq!((first.processed, first.has_more), (2, true));
    let second = cron.catch_up("projector").await.unwrap();
    assert_eq!((second.processed, second.has_more), (2, true));
    let third = cron.catch_up("projector").await.unwrap();
    assert_eq!((third.processed, third.has_more), (1, false));
    let fourth = cron.catch_up("projector").await.unwrap();
    assert_eq!((fourth.processed, fourth.has_more), (0, false));

    assert_eq!(handler.seen(), ids, "every event once, in append order");
    let stored = cron
        .subscriptions()
        .get("projector")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.last_seen, ids.last().copied());
}

#[tokio::test]
async fn a_batch_that_is_exactly_full_has_no_more() {
    let (bank, _) = stored_events(1).await; // 2 events
    let cron = dispatcher(&bank).with_batch_size(2);
    cron.register(Recorder::new("projector"), None).unwrap();
    let result = cron.catch_up("projector").await.unwrap();
    assert_eq!((result.processed, result.has_more), (2, false));
}

#[tokio::test]
async fn a_category_subscription_reads_only_its_stream() {
    let (bank, _) = stored_events(2).await;
    let cron = dispatcher(&bank);
    let handler = Recorder::new("accounts");
    cron.register(handler.clone(), Some("BankAccount")).unwrap();
    let nothing = Recorder::new("transfers");
    cron.register(nothing.clone(), Some("MoneyTransfer"))
        .unwrap();
    cron.run_all().await;
    assert_eq!(handler.seen().len(), 3);
    assert!(nothing.seen().is_empty());
}

#[tokio::test]
async fn a_retryable_failure_stops_the_run_and_the_next_run_resumes() {
    let (bank, ids) = stored_events(3).await; // 4 events
    let cron = dispatcher(&bank);
    let handler = Recorder::new("projector");
    cron.register(handler.clone(), None).unwrap();
    handler.fail_on(ids[1], ErrorCategory::Infrastructure);

    let stopped = cron.catch_up("projector").await.unwrap();
    assert_eq!(stopped.processed, 1);
    assert!(!stopped.has_more);
    let failure = stopped.failed.expect("the run stopped on a failure");
    assert_eq!((failure.event_id, failure.attempts), (ids[1], 1));
    let sub = cron
        .subscriptions()
        .get("projector")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sub.last_seen, Some(ids[0]), "the cursor stays before it");
    assert_eq!(sub.failed_attempts, 1);

    let again = cron.catch_up("projector").await.unwrap();
    assert_eq!(again.failed.unwrap().attempts, 2);

    handler.recover(ids[1]);
    let resumed = cron.catch_up("projector").await.unwrap();
    assert_eq!((resumed.processed, resumed.failed.is_none()), (3, true));
    assert_eq!(handler.seen(), ids);
    let sub = cron
        .subscriptions()
        .get("projector")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sub.failed_attempts, 0, "the ack clears the failure");
    assert!(
        bank.provider
            .create_dead_letter_store()
            .count(None)
            .await
            .unwrap()
            == 0
    );
}

#[tokio::test]
async fn a_non_retryable_failure_is_dead_lettered_and_skipped() {
    let (bank, ids) = stored_events(2).await;
    let cron = dispatcher(&bank);
    let handler = Recorder::new("projector");
    cron.register(handler.clone(), None).unwrap();
    handler.fail_on(ids[1], ErrorCategory::Domain);

    let result = cron.catch_up("projector").await.unwrap();
    assert_eq!((result.processed, result.dead_lettered), (2, 1));
    assert!(result.failed.is_none());
    assert_eq!(handler.seen(), vec![ids[0], ids[2]]);

    let entry = bank
        .provider
        .create_dead_letter_store()
        .get(ids[1], "projector")
        .await
        .unwrap()
        .expect("dead-lettered");
    assert_eq!(entry.error_code, "HANDLER_DOWN");
    assert_eq!(entry.attempts, 1);
    assert_eq!(entry.event_type, "MoneyDeposited");
}

#[tokio::test]
async fn max_attempts_dead_letters_a_retryable_failure() {
    let (bank, ids) = stored_events(1).await;
    let cron = dispatcher(&bank).with_max_attempts(3);
    let handler = Recorder::new("projector");
    cron.register(handler.clone(), None).unwrap();
    handler.fail_on(ids[0], ErrorCategory::Infrastructure);

    for attempt in 1..=2 {
        let run = cron.catch_up("projector").await.unwrap();
        assert_eq!(run.failed.unwrap().attempts, attempt);
    }
    let last = cron.catch_up("projector").await.unwrap();
    assert!(last.failed.is_none());
    assert_eq!((last.dead_lettered, last.processed), (1, 1));
    let entry = bank
        .provider
        .create_dead_letter_store()
        .get(ids[0], "projector")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.attempts, 3);
}

#[tokio::test]
async fn an_unreadable_event_is_skipped_whatever_its_error_category() {
    let (bank, _) = stored_events(1).await;
    // A stored event of a known type whose data doesn't fit it. Reading it
    // fails with SERIALIZATION_ERROR, which is an infrastructure error.
    let adapter = bank.store.adapter();
    let mut broken = adapter.load_all(None, Some(2)).await.unwrap().remove(1);
    broken.event_id = new_v7();
    broken.data = serde_json::json!({ "unexpected": true });
    let stream = StreamIds::for_aggregate("BankAccount", AccountId::generate());
    adapter
        .append_to_stream(&stream, vec![broken.clone()], 0)
        .await
        .unwrap();
    let ids = all_ids(&bank).await;
    assert_eq!(ids.len(), 3);
    assert_eq!(
        bank.store.deserialize(&broken).unwrap_err().code(),
        &ErrorCode::SerializationError
    );

    let cron = dispatcher(&bank);
    let handler = Recorder::new("projector");
    cron.register(handler.clone(), None).unwrap();
    let result = cron.catch_up("projector").await.unwrap();
    assert_eq!((result.processed, result.dead_lettered), (2, 1));
    assert!(result.failed.is_none(), "it doesn't block the batch");
    assert_eq!(handler.seen(), ids[..2].to_vec());
    let entry = bank
        .provider
        .create_dead_letter_store()
        .get(broken.event_id, "projector")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(entry.error_code, "SERIALIZATION_ERROR");
    assert_eq!(entry.payload, serde_json::json!({ "unexpected": true }));
}

#[tokio::test]
async fn a_unit_of_work_rolls_back_the_handlers_writes_with_the_cursor() {
    let (bank, ids) = stored_events(0).await;
    let provider_of_bank: Arc<dyn Provider> = bank.provider.clone();

    for (units, rows) in [(true, 0), (false, 1)] {
        let cron = dispatcher(&bank);
        let cron = if units {
            cron.with_units_of_work(provider_of_bank.clone())
        } else {
            cron
        };
        let name = if units { "with-units" } else { "without-units" };
        let handler = Recorder::writing(name, provider_of_bank.clone());
        handler.fail_on(ids[0], ErrorCategory::Infrastructure);
        cron.register(handler, None).unwrap();
        cron.catch_up(name).await.unwrap();
        let written = provider_of_bank
            .create_processed_event_store()
            .count("side-effect", None)
            .await
            .unwrap();
        // The first pass leaves 0 rows with units, and the second adds 1.
        assert_eq!(written, rows, "units: {units}");
    }
}

#[tokio::test]
async fn a_dead_letter_and_its_cursor_move_commit_together() {
    let (bank, ids) = stored_events(0).await;
    let provider: Arc<dyn Provider> = bank.provider.clone();
    let cron = dispatcher(&bank).with_units_of_work(provider.clone());
    let handler = Recorder::writing("projector", provider);
    handler.fail_on(ids[0], ErrorCategory::Domain);
    cron.register(handler, None).unwrap();
    let result = cron.catch_up("projector").await.unwrap();
    assert_eq!(result.dead_lettered, 1);
    let sub = cron
        .subscriptions()
        .get("projector")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(sub.last_seen, Some(ids[0]));
    assert_eq!(
        bank.provider
            .create_dead_letter_store()
            .count(Some("projector"))
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn registering_checks_names_and_streams() {
    let (bank, _) = stored_events(0).await;
    let cron = dispatcher(&bank);
    cron.register(Recorder::new("a"), None).unwrap();
    let duplicate = cron.register(Recorder::new("a"), None).unwrap_err();
    assert_eq!(duplicate.code(), &ErrorCode::InvalidConfiguration);
    let unknown = cron.catch_up("missing").await.unwrap_err();
    assert_eq!(unknown.code(), &ErrorCode::InvalidConfiguration);

    let manager = manager(&bank);
    manager.register("s", Some("BankAccount")).unwrap();
    manager.register("s", Some("BankAccount")).unwrap();
    assert_eq!(
        manager.register("s", None).unwrap_err().code(),
        &ErrorCode::InvalidConfiguration
    );
    manager.register("all", Some("$all")).unwrap();
    manager.register("all", None).unwrap();
}

#[tokio::test]
async fn the_manager_reports_lag_and_resets() {
    let (bank, ids) = stored_events(3).await; // 4 events
    let cron = dispatcher(&bank).with_batch_size(3);
    let manager = cron.subscriptions().clone();
    cron.register(Recorder::new("a"), None).unwrap();
    cron.register(Recorder::new("b"), Some("BankAccount"))
        .unwrap();

    // Registered, not created: everything is pending.
    assert!(manager.list().await.unwrap().is_empty());
    assert!(!manager.are_all_caught_up().await.unwrap());
    assert!(!manager.is_caught_up("a").await.unwrap());
    let report = manager.get_lag_report(100).await.unwrap();
    assert_eq!(
        report
            .subscriptions
            .iter()
            .map(|s| (s.name.as_str(), s.pending))
            .collect::<Vec<_>>(),
        vec![("a", 4), ("b", 4)]
    );
    assert_eq!(
        manager.ensure_all_subscriptions().await.unwrap(),
        vec!["a", "b"]
    );
    assert!(manager.ensure_all_subscriptions().await.unwrap().is_empty());

    cron.catch_up("a").await.unwrap(); // 3 of 4
    let report = manager.get_lag_report(100).await.unwrap();
    let a = &report.subscriptions[0];
    assert_eq!(
        (a.pending, a.pending_capped, a.last_seen),
        (1, false, Some(ids[2]))
    );
    assert!(!report.caught_up());
    let capped = manager.get_lag_report(2).await.unwrap();
    assert_eq!(
        (
            capped.subscriptions[1].pending,
            capped.subscriptions[1].pending_capped
        ),
        (2, true)
    );

    cron.run_all().await;
    cron.run_all().await;
    assert!(manager.are_all_caught_up().await.unwrap());
    assert!(manager.get_lag_report(10).await.unwrap().caught_up());

    manager.advance("a", ids[0]).await.unwrap();
    assert_eq!(
        manager.get("a").await.unwrap().unwrap().last_seen,
        ids.last().copied(),
        "advance only moves forward"
    );

    manager.reset("a").await.unwrap();
    let a = manager.get("a").await.unwrap().unwrap();
    assert_eq!(a.last_seen, None);
    assert!(!manager.is_caught_up("a").await.unwrap());
    manager.reset_all().await.unwrap();
    assert!(
        manager
            .list()
            .await
            .unwrap()
            .iter()
            .all(|s| s.last_seen.is_none())
    );

    manager.delete("b").await.unwrap();
    assert!(manager.get("b").await.unwrap().is_none());
    assert_eq!(
        manager.is_caught_up("nobody").await.unwrap_err().code(),
        &ErrorCode::InvalidConfiguration
    );
}
