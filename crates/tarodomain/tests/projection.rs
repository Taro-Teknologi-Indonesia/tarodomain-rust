//! Projections (spec §12) on the local provider's processed-event store.

mod support;

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tarodomain::adapters::{ConsistencyMode, Provider};
use tarodomain::async_trait;
use tarodomain::bus::EventHandler;
use tarodomain::domain::{DomainEvent, Recorded};
use tarodomain::error::{ErrorCategory, FrameworkError, Result};
use tarodomain::projection::{
    Projection, ProjectionHandler, ProjectionRoutes, ShardProjection, shard_handlers,
};

use support::bank::{AccountId, AccountOpened, MoneyDeposited};
use support::{Bank, deposit};

/// Balances per account, built from deposits; everything else is unknown.
#[derive(Default)]
struct Balances {
    totals: Mutex<BTreeMap<String, i64>>,
    down: AtomicBool,
}

impl Balances {
    async fn on_deposited(self: Arc<Self>, e: Recorded<MoneyDeposited>) -> Result<()> {
        if self.down.load(Ordering::SeqCst) {
            return Err(FrameworkError::custom(
                "READ_MODEL_DOWN",
                ErrorCategory::Infrastructure,
                "down",
            ));
        }
        let key = e.envelope.correlation_id.to_string();
        *self.totals.lock().unwrap().entry(key).or_default() += e.cents;
        Ok(())
    }

    fn total(&self) -> i64 {
        self.totals.lock().unwrap().values().sum()
    }
}

#[async_trait]
impl Projection for Balances {
    fn name(&self) -> &str {
        "balances"
    }
    fn routes(r: &mut ProjectionRoutes<Self>) {
        r.on::<MoneyDeposited, _, _>(Self::on_deposited);
    }
}

impl ShardProjection for Balances {
    fn shard_count(&self) -> u32 {
        4
    }
    fn shard_key(&self, event: &DomainEvent) -> String {
        event.envelope().correlation_id.to_string()
    }
}

async fn history(bank: &Bank, account: AccountId, deposits: &[i64]) -> Vec<DomainEvent> {
    for cents in deposits {
        bank.dispatcher
            .dispatch(deposit(account, *cents))
            .await
            .unwrap();
    }
    bank.store.load_all(None, None).await.unwrap()
}

#[tokio::test]
async fn a_redelivered_event_is_applied_once() {
    let bank = Bank::new(ConsistencyMode::Strong, None);
    let account = bank.open("Ann").await;
    let events = history(&bank, account, &[10, 20]).await;

    let projection = Arc::new(Balances::default());
    let handler = ProjectionHandler::new(
        projection.clone(),
        bank.provider.create_processed_event_store(),
    )
    .unwrap();
    for _ in 0..2 {
        for event in &events {
            handler.handle(event).await.unwrap();
        }
    }
    assert_eq!(projection.total(), 30);
}

#[tokio::test]
async fn an_unknown_event_is_marked_so_it_isnt_retried() {
    let bank = Bank::new(ConsistencyMode::Strong, None);
    let account = bank.open("Ann").await;
    let events = history(&bank, account, &[]).await;
    let opened = events.iter().find(|e| e.is::<AccountOpened>()).unwrap();

    let seen = Arc::new(Mutex::new(0));
    let counter = seen.clone();
    let store = bank.provider.create_processed_event_store();
    let handler = ProjectionHandler::new(Arc::new(Balances::default()), store.clone())
        .unwrap()
        .on_unhandled(Arc::new(move |_, _| *counter.lock().unwrap() += 1));
    handler.handle(opened).await.unwrap();

    assert!(
        store
            .exists("balances", "balances", opened.id())
            .await
            .unwrap()
    );
    assert_eq!(*seen.lock().unwrap(), 1);
    // Marked, so the second delivery doesn't even reach the hook.
    handler.handle(opened).await.unwrap();
    assert_eq!(*seen.lock().unwrap(), 1);
}

#[tokio::test]
async fn a_failing_handler_isnt_marked_and_the_retry_applies_it() {
    let bank = Bank::new(ConsistencyMode::Strong, None);
    let account = bank.open("Ann").await;
    let events = history(&bank, account, &[15]).await;
    let event = events.iter().find(|e| e.is::<MoneyDeposited>()).unwrap();

    let projection = Arc::new(Balances::default());
    let store = bank.provider.create_processed_event_store();
    let handler = ProjectionHandler::new(projection.clone(), store.clone()).unwrap();
    projection.down.store(true, Ordering::SeqCst);
    assert!(handler.handle(event).await.is_err());
    assert!(
        !store
            .exists("balances", "balances", event.id())
            .await
            .unwrap()
    );

    projection.down.store(false, Ordering::SeqCst);
    handler.handle(event).await.unwrap();
    assert_eq!(projection.total(), 15);
    assert!(
        store
            .exists("balances", "balances", event.id())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn shards_split_the_events_deterministically() {
    let bank = Bank::new(ConsistencyMode::Strong, None);
    let mut events = Vec::new();
    for _ in 0..12 {
        let account = bank.open("Ann").await;
        bank.dispatcher.dispatch(deposit(account, 5)).await.unwrap();
    }
    events.extend(bank.store.load_all(None, None).await.unwrap());

    let store = bank.provider.create_processed_event_store();
    let projection = Arc::new(Balances::default());
    let handlers = shard_handlers(projection.clone(), store.clone()).unwrap();
    assert_eq!(handlers.len(), 4);
    for event in &events {
        for handler in &handlers {
            handler.handle(event).await.unwrap();
        }
    }
    // Every deposit is applied exactly once, by exactly one shard.
    assert_eq!(projection.total(), 60);
    let mut recorded = 0;
    for i in 0..4 {
        let instance = format!("balances-shard-{i}");
        let n = store.count("balances", Some(&instance)).await.unwrap();
        let expected = events
            .iter()
            .filter(|e| projection.shard_of(e) == i)
            .count() as u64;
        assert_eq!(n, expected, "shard {i}");
        recorded += n;
    }
    // Unknown events (the openings) are marked too, once each.
    assert_eq!(recorded, 24);
    // The same event always lands on the same shard.
    for event in &events {
        assert_eq!(projection.shard_of(event), projection.shard_of(event));
    }
}
