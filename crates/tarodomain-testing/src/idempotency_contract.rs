//! The idempotency store contracts (spec §12.3, §14.1).

use std::sync::Arc;

use chrono::TimeDelta;
use tarodomain::adapters::{CommandIdempotencyStore, ProcessedCommand, ProcessedEventStore};
use tarodomain::kernel::ids::Uuid;

use crate::fixtures::{event_id, uuid};
use crate::{ProviderFactory, far_future, long_ago};

/// Checks a provider's [`ProcessedEventStore`] and
/// [`CommandIdempotencyStore`] against their rustdoc contracts: idempotent
/// marks that keep the first record, `exists`, `count` per handler and
/// instance, `delete_by_handler`, the command list order, and `delete_old`
/// with an exclusive cut-off.
///
/// Panics on the first case that fails.
pub async fn idempotency_contract(factory: impl ProviderFactory) {
    let events = || async { factory.create().await.create_processed_event_store() };
    processed_events_are_per_handler_instance(events().await).await;
    processed_event_counts_and_deletes(events().await).await;
    processed_events_delete_old(events().await).await;

    let commands = || async { factory.create().await.create_command_idempotency_store() };
    commands_keep_the_first_mark(commands().await).await;
    command_lists_are_oldest_first(commands().await).await;
    commands_delete_old(commands().await).await;
}

type Events = Arc<dyn ProcessedEventStore>;
type Commands = Arc<dyn CommandIdempotencyStore>;

const BALANCE: &str = "BalanceProjection";
const SAGA: &str = "TransferSaga";

async fn processed_events_are_per_handler_instance(store: Events) {
    let event = event_id(1);
    assert!(!store.exists(BALANCE, BALANCE, event).await.unwrap());
    store.mark_processed(BALANCE, BALANCE, event).await.unwrap();
    store.mark_processed(BALANCE, BALANCE, event).await.unwrap();

    assert!(store.exists(BALANCE, BALANCE, event).await.unwrap());
    assert!(!store.exists(BALANCE, "shard-1", event).await.unwrap());
    assert!(!store.exists(SAGA, BALANCE, event).await.unwrap());
    assert!(!store.exists(BALANCE, BALANCE, event_id(2)).await.unwrap());
    assert_eq!(
        store.count(BALANCE, None).await.unwrap(),
        1,
        "marking twice keeps one"
    );
}

async fn processed_event_counts_and_deletes(store: Events) {
    let saga = uuid("saga", 1).to_string();
    for n in 1..=3 {
        store
            .mark_processed(BALANCE, "shard-1", event_id(n))
            .await
            .unwrap();
    }
    store
        .mark_processed(BALANCE, "shard-2", event_id(1))
        .await
        .unwrap();
    store
        .mark_processed(SAGA, &saga, event_id(1))
        .await
        .unwrap();

    let count = async |name, instance| store.count(name, instance).await.unwrap();
    assert_eq!(count(BALANCE, None).await, 4);
    assert_eq!(count(BALANCE, Some("shard-1")).await, 3);
    assert_eq!(count(BALANCE, Some("shard-9")).await, 0);
    assert_eq!(count(SAGA, Some(saga.as_str())).await, 1);

    let deleted = store.delete_by_handler(BALANCE, Some("shard-2")).await;
    assert_eq!(deleted.unwrap(), 1);
    assert_eq!(count(BALANCE, None).await, 3);
    assert_eq!(store.delete_by_handler(BALANCE, None).await.unwrap(), 3);
    assert_eq!(store.delete_by_handler(BALANCE, None).await.unwrap(), 0);
    assert!(!store.exists(BALANCE, "shard-1", event_id(1)).await.unwrap());
    assert_eq!(count(SAGA, None).await, 1, "other handlers are kept");
}

async fn processed_events_delete_old(store: Events) {
    store
        .mark_processed(BALANCE, BALANCE, event_id(1))
        .await
        .unwrap();
    store.mark_processed(SAGA, SAGA, event_id(2)).await.unwrap();
    assert_eq!(store.delete_old(long_ago()).await.unwrap(), 0);
    assert_eq!(store.delete_old(far_future()).await.unwrap(), 2);
    assert!(!store.exists(BALANCE, BALANCE, event_id(1)).await.unwrap());
}

fn command_id(n: u32) -> Uuid {
    uuid("command", n)
}

async fn commands_keep_the_first_mark(store: Commands) {
    let id = command_id(1);
    assert!(!store.exists(id).await.unwrap());
    assert_eq!(store.get_processed_at(id).await.unwrap(), None);

    store
        .mark_processed(id, "DepositMoney", "DepositHandler")
        .await
        .unwrap();
    let first = store.get_processed_at(id).await.unwrap().unwrap();
    store
        .mark_processed(id, "Other", "OtherHandler")
        .await
        .unwrap();
    assert!(store.exists(id).await.unwrap());
    assert_eq!(store.get_processed_at(id).await.unwrap(), Some(first));

    let listed = store.list("DepositHandler", None).await.unwrap();
    let expected = ProcessedCommand {
        command_id: id,
        command_type: "DepositMoney".into(),
        handler_name: "DepositHandler".into(),
        processed_at: first,
    };
    assert_eq!(listed, [expected], "the first mark is kept");
    assert!(store.list("OtherHandler", None).await.unwrap().is_empty());
    assert_eq!(store.count(None).await.unwrap(), 1);
}

async fn command_lists_are_oldest_first(store: Commands) {
    for n in [3, 1, 2] {
        let id = command_id(n);
        store
            .mark_processed(id, "DepositMoney", "DepositHandler")
            .await
            .unwrap();
    }
    let open = command_id(4);
    store
        .mark_processed(open, "OpenAccount", "OpenHandler")
        .await
        .unwrap();

    let listed = store.list("DepositHandler", None).await.unwrap();
    let key = |c: &ProcessedCommand| (c.processed_at, c.command_id);
    for pair in listed.windows(2) {
        assert!(key(&pair[0]) <= key(&pair[1]), "list order: {pair:?}");
    }
    let mut ids: Vec<_> = listed.iter().map(|c| c.command_id).collect();
    ids.sort();
    let mut expected = [command_id(1), command_id(2), command_id(3)];
    expected.sort();
    assert_eq!(ids, expected);
    let two = store.list("DepositHandler", Some(2)).await.unwrap();
    assert_eq!(two, listed[..2]);

    assert_eq!(store.count(None).await.unwrap(), 4);
    assert_eq!(store.count(Some("DepositHandler")).await.unwrap(), 3);
    assert_eq!(store.count(Some("Missing")).await.unwrap(), 0);
}

async fn commands_delete_old(store: Commands) {
    let (old, other) = (command_id(1), command_id(2));
    store
        .mark_processed(old, "DepositMoney", "DepositHandler")
        .await
        .unwrap();
    store
        .mark_processed(other, "DepositMoney", "DepositHandler")
        .await
        .unwrap();
    let at = store.get_processed_at(old).await.unwrap().unwrap();
    let other_at = store.get_processed_at(other).await.unwrap().unwrap();

    assert_eq!(store.delete_old(long_ago()).await.unwrap(), 0);
    let kept_at_cut_off = store.delete_old(at.min(other_at)).await.unwrap();
    assert_eq!(kept_at_cut_off, 0, "the cut-off is exclusive");
    let cut_off = at.max(other_at) + TimeDelta::milliseconds(1);
    assert_eq!(store.delete_old(cut_off).await.unwrap(), 2);
    assert!(!store.exists(old).await.unwrap());
    assert_eq!(store.count(None).await.unwrap(), 0);
    assert_eq!(store.delete_old(far_future()).await.unwrap(), 0);
}
