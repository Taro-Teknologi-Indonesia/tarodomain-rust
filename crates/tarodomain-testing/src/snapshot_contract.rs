//! The snapshot store adapter contract (spec §8.1).

use std::sync::Arc;

use serde_json::json;
use tarodomain::adapters::{Snapshot, SnapshotInput, SnapshotStoreAdapter};
use tarodomain::domain::Metadata;
use tarodomain::kernel::ids::Uuid;

use crate::ProviderFactory;
use crate::fixtures::aggregate_id;

/// Checks a provider's [`SnapshotStoreAdapter`] against its rustdoc
/// contract: the stored fields, the upsert, reads per aggregate and type,
/// newest-first lists, `delete_before` (which keeps the cut-off version and
/// returns the count), `delete_all` and `count`.
///
/// Panics on the first case that fails.
pub async fn snapshot_contract(factory: impl ProviderFactory) {
    let store = || async { factory.create().await.create_snapshot_store_adapter() };
    snapshots_round_trip(store().await).await;
    save_is_an_upsert(store().await).await;
    reads_are_per_aggregate_and_type(store().await).await;
    lists_are_newest_first(store().await).await;
    delete_before_keeps_the_cut_off(store().await).await;
    delete_all_and_count(store().await).await;
}

type Store = Arc<dyn SnapshotStoreAdapter>;

const ACCOUNT: &str = "BankAccount";

fn input(source_uuid: Uuid, version: u64) -> SnapshotInput {
    SnapshotInput {
        source_uuid,
        source_version: version,
        source_type: ACCOUNT.into(),
        data: json!({ "balance": version * 10, "owner": { "name": "Ana" }, "flags": [true] }),
        metadata: Metadata::new().with("reason", "snapshot_every"),
    }
}

fn versions(snapshots: &[Snapshot]) -> Vec<u64> {
    snapshots.iter().map(|s| s.source_version).collect()
}

async fn save(store: &Store, source_uuid: Uuid, versions: impl IntoIterator<Item = u64>) {
    for version in versions {
        store.save(input(source_uuid, version)).await.unwrap();
    }
}

async fn snapshots_round_trip(store: Store) {
    let id = aggregate_id(1);
    assert_eq!(store.load_latest(id, ACCOUNT).await.unwrap(), None);
    store.save(input(id, 5)).await.unwrap();
    let mut bare = input(id, 6);
    bare.metadata = Metadata::new();
    store.save(bare.clone()).await.unwrap();

    let at_five = store
        .load_at_version(id, ACCOUNT, 5)
        .await
        .unwrap()
        .unwrap();
    let expected = input(id, 5);
    assert_eq!(at_five.source_uuid, expected.source_uuid);
    assert_eq!(at_five.source_version, 5);
    assert_eq!(at_five.source_type, ACCOUNT);
    assert_eq!(at_five.data, expected.data);
    assert_eq!(at_five.metadata, expected.metadata);
    let latest = store.load_latest(id, ACCOUNT).await.unwrap().unwrap();
    assert_eq!((latest.source_version, latest.metadata), (6, bare.metadata));
    assert_eq!(store.load_at_version(id, ACCOUNT, 4).await.unwrap(), None);
}

async fn save_is_an_upsert(store: Store) {
    let id = aggregate_id(1);
    store.save(input(id, 5)).await.unwrap();
    let first = store.load_latest(id, ACCOUNT).await.unwrap().unwrap();
    let mut replacement = input(id, 5);
    replacement.data = json!({ "balance": 999 });
    store.save(replacement).await.unwrap();

    assert_eq!(store.count(id, ACCOUNT).await.unwrap(), 1);
    let replaced = store
        .load_at_version(id, ACCOUNT, 5)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(replaced.data, json!({ "balance": 999 }));
    assert!(
        replaced.created_at >= first.created_at,
        "created_at is replaced"
    );
}

async fn reads_are_per_aggregate_and_type(store: Store) {
    let (id, other) = (aggregate_id(1), aggregate_id(2));
    save(&store, id, [3, 10, 7]).await;
    save(&store, other, [20]).await;
    let mut transfer = input(id, 30);
    transfer.source_type = "Transfer".into();
    store.save(transfer).await.unwrap();

    let latest = store.load_latest(id, ACCOUNT).await.unwrap().unwrap();
    assert_eq!(
        latest.source_version, 10,
        "the highest version, not the last saved"
    );
    let transfer = store.load_latest(id, "Transfer").await.unwrap().unwrap();
    assert_eq!(transfer.source_version, 30);
    assert_eq!(
        store.load_at_version(id, "Transfer", 10).await.unwrap(),
        None
    );
    assert_eq!(store.load_at_version(id, ACCOUNT, 30).await.unwrap(), None);
    assert_eq!(
        store.load_latest(aggregate_id(3), ACCOUNT).await.unwrap(),
        None
    );
    assert_eq!(store.count(id, ACCOUNT).await.unwrap(), 3);
    assert_eq!(store.count(id, "Transfer").await.unwrap(), 1);
}

async fn lists_are_newest_first(store: Store) {
    let id = aggregate_id(1);
    save(&store, id, [3, 10, 7]).await;
    save(&store, aggregate_id(2), [5]).await;
    let all = store.list(id, ACCOUNT, None).await.unwrap();
    assert_eq!(versions(&all), [10, 7, 3]);
    let two = store.list(id, ACCOUNT, Some(2)).await.unwrap();
    assert_eq!(versions(&two), [10, 7]);
    assert!(store.list(id, "Transfer", None).await.unwrap().is_empty());
}

async fn delete_before_keeps_the_cut_off(store: Store) {
    let (id, other) = (aggregate_id(1), aggregate_id(2));
    save(&store, id, [3, 5, 7, 10]).await;
    save(&store, other, [1]).await;
    assert_eq!(store.delete_before(id, ACCOUNT, 7).await.unwrap(), 2);
    let left = store.list(id, ACCOUNT, None).await.unwrap();
    assert_eq!(versions(&left), [10, 7]);
    assert_eq!(store.delete_before(id, ACCOUNT, 7).await.unwrap(), 0);
    assert_eq!(store.delete_before(id, "Transfer", 100).await.unwrap(), 0);
    assert_eq!(store.count(other, ACCOUNT).await.unwrap(), 1);
}

async fn delete_all_and_count(store: Store) {
    let (id, other) = (aggregate_id(1), aggregate_id(2));
    assert_eq!(store.count(id, ACCOUNT).await.unwrap(), 0);
    save(&store, id, [1, 2]).await;
    save(&store, other, [1]).await;
    store.delete_all(id, ACCOUNT).await.unwrap();
    store.delete_all(id, ACCOUNT).await.unwrap();
    assert_eq!(store.count(id, ACCOUNT).await.unwrap(), 0);
    assert_eq!(store.load_latest(id, ACCOUNT).await.unwrap(), None);
    assert_eq!(store.count(other, ACCOUNT).await.unwrap(), 1);
}
