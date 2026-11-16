//! The dead-letter store contract (spec §21.1).

use std::sync::Arc;

use chrono::TimeDelta;
use serde_json::json;
use tarodomain::adapters::{DeadLetterEntry, DeadLetterInput, DeadLetterStore};
use tarodomain::kernel::ids::Uuid;

use crate::fixtures::event_id;
use crate::{ProviderFactory, far_future, long_ago};

/// Checks a provider's [`DeadLetterStore`] against its rustdoc contract: the
/// stored fields, IDs that increase and are never reused, `list` in ID order
/// per handler, `get` returning the latest entry, `delete`, `count` and
/// `delete_old` with an exclusive cut-off.
///
/// Panics on the first case that fails.
pub async fn dead_letter_contract(factory: impl ProviderFactory) {
    let store = || async { factory.create().await.create_dead_letter_store() };
    entries_round_trip(store().await).await;
    ids_increase_and_are_never_reused(store().await).await;
    reads_are_per_handler_in_id_order(store().await).await;
    delete_old_cuts_off_by_write_time(store().await).await;
}

type Store = Arc<dyn DeadLetterStore>;

const BALANCE: &str = "BalanceProjection";
const AUDIT: &str = "AuditProjection";

fn input(handler_name: &str, event_id: Uuid, attempts: u32) -> DeadLetterInput {
    DeadLetterInput {
        handler_name: handler_name.into(),
        event_id,
        event_type: "MoneyDeposited".into(),
        payload: json!({ "eventId": event_id, "data": { "amount": 125 }, "raw": "body" }),
        error_code: "PROJECTION_HANDLER_FAILED".into(),
        error_message: "balance went negative".into(),
        attempts,
    }
}

fn ids(entries: &[DeadLetterEntry]) -> Vec<u64> {
    entries.iter().map(|entry| entry.id).collect()
}

async fn entries_round_trip(store: Store) {
    assert_eq!(store.get_by_id(1).await.unwrap(), None);
    let written = input(BALANCE, event_id(1), 5);
    let id = store.write(written.clone()).await.unwrap();
    let entry = store.get_by_id(id).await.unwrap().unwrap();
    assert_eq!(entry.id, id);
    assert_eq!(entry.handler_name, written.handler_name);
    assert_eq!(entry.event_id, written.event_id);
    assert_eq!(entry.event_type, written.event_type);
    assert_eq!(entry.payload, written.payload);
    assert_eq!(entry.error_code, written.error_code);
    assert_eq!(entry.error_message, written.error_message);
    assert_eq!(entry.attempts, 5);
    let by_event = store.get(event_id(1), BALANCE).await.unwrap();
    assert_eq!(by_event, Some(entry));
}

async fn ids_increase_and_are_never_reused(store: Store) {
    let event = event_id(1);
    let first = store.write(input(BALANCE, event, 1)).await.unwrap();
    let second = store.write(input(BALANCE, event, 2)).await.unwrap();
    assert!(
        second > first,
        "a new entry for the same event gets a new ID"
    );
    store.delete(second).await.unwrap();
    store.delete(second).await.unwrap();
    assert_eq!(store.get_by_id(second).await.unwrap(), None);
    let third = store.write(input(AUDIT, event, 1)).await.unwrap();
    assert!(third > second, "a deleted ID is not reused");
    assert_eq!(store.count(None).await.unwrap(), 2);
}

async fn reads_are_per_handler_in_id_order(store: Store) {
    let (a, b) = (event_id(1), event_id(2));
    let mut written = Vec::new();
    for (handler, event, attempts) in [
        (BALANCE, a, 1),
        (AUDIT, a, 1),
        (BALANCE, b, 1),
        (BALANCE, a, 3),
    ] {
        written.push(store.write(input(handler, event, attempts)).await.unwrap());
    }
    let balance = [written[0], written[2], written[3]];
    assert_eq!(ids(&store.list(BALANCE, None).await.unwrap()), balance);
    assert_eq!(
        ids(&store.list(BALANCE, Some(2)).await.unwrap()),
        balance[..2]
    );
    assert_eq!(ids(&store.list(AUDIT, None).await.unwrap()), [written[1]]);
    assert!(store.list("Missing", None).await.unwrap().is_empty());

    let latest = store.get(a, BALANCE).await.unwrap().unwrap();
    assert_eq!(
        (latest.id, latest.attempts),
        (written[3], 3),
        "the latest entry"
    );
    assert_eq!(store.get(b, AUDIT).await.unwrap(), None);
    assert_eq!(store.count(None).await.unwrap(), 4);
    assert_eq!(store.count(Some(BALANCE)).await.unwrap(), 3);
    assert_eq!(store.count(Some("Missing")).await.unwrap(), 0);
}

async fn delete_old_cuts_off_by_write_time(store: Store) {
    let first = store.write(input(BALANCE, event_id(1), 1)).await.unwrap();
    let second = store.write(input(AUDIT, event_id(2), 1)).await.unwrap();
    let written_at = async |id| store.get_by_id(id).await.unwrap().unwrap().created_at;
    let (first_at, second_at) = (written_at(first).await, written_at(second).await);
    assert!(first_at <= second_at);

    assert_eq!(store.delete_old(long_ago()).await.unwrap(), 0);
    assert_eq!(
        store.delete_old(first_at).await.unwrap(),
        0,
        "the cut-off is exclusive"
    );
    let cut_off = second_at + TimeDelta::milliseconds(1);
    assert_eq!(store.delete_old(cut_off).await.unwrap(), 2);
    assert_eq!(store.count(None).await.unwrap(), 0);
    assert_eq!(store.delete_old(far_future()).await.unwrap(), 0);
}
