//! The saga store adapter contract (spec §9.1, §19.1).

use std::sync::Arc;

use chrono::TimeDelta;
use serde_json::json;
use tarodomain::adapters::{SagaQuery, SagaRecord, SagaSaveInput, SagaStatus, SagaStoreAdapter};
use tarodomain::kernel::ids::Uuid;

use crate::fixtures::uuid;
use crate::{ProviderFactory, far_future, long_ago};

/// Checks a provider's [`SagaStoreAdapter`] against its rustdoc contract:
/// the stored fields and the timestamp rules of [`SagaRecord::saved`], the
/// idempotent partial `complete` and `fail`, the order of `load_by_status`,
/// `load_timed_out` with its type filter applied before the limit, `delete`
/// and `count`.
///
/// The provider tells time by its own clock, so the cases compare with the
/// timestamps it stored rather than with fixed times.
///
/// Panics on the first case that fails.
pub async fn saga_store_contract(factory: impl ProviderFactory) {
    let store = || async { factory.create().await.create_saga_store_adapter() };
    sagas_round_trip(store().await).await;
    a_terminal_save_sets_completed_at(store().await).await;
    complete_ends_a_saga_once(store().await).await;
    fail_ends_a_saga_once(store().await).await;
    load_by_status_filters_orders_and_limits(store().await).await;
    load_timed_out_takes_active_sagas_before_the_cut_off(store().await).await;
    delete_and_count(store().await).await;
}

type Store = Arc<dyn SagaStoreAdapter>;

const TRANSFER: &str = "TransferSaga";
const REFUND: &str = "RefundSaga";

fn saga_id(n: u32) -> Uuid {
    uuid("saga", n)
}

fn input(saga_type: &str, n: u32, status: SagaStatus) -> SagaSaveInput {
    SagaSaveInput {
        saga_id: saga_id(n),
        saga_type: saga_type.into(),
        correlation_id: saga_id(n),
        state: json!({ "amount": 100, "debited": true, "compensations": ["refund"] }),
        status,
        current_step: "continueWhenMoneyWithdrawn".into(),
        failure_reason: None,
    }
}

async fn save(store: &Store, saga_type: &str, n: u32, status: SagaStatus) {
    store.save(input(saga_type, n, status)).await.unwrap();
}

async fn load(store: &Store, saga_type: &str, n: u32) -> SagaRecord {
    let record = store.load(saga_type, saga_id(n)).await.unwrap();
    record.unwrap_or_else(|| panic!("saga {saga_type} {n} is missing"))
}

/// Asserts the contract's list order: `updated_at`, then type, then ID.
fn assert_list_order(records: &[SagaRecord]) {
    let key = |r: &SagaRecord| (r.updated_at, r.saga_type.clone(), r.saga_id);
    for pair in records.windows(2) {
        assert!(key(&pair[0]) <= key(&pair[1]), "list order: {pair:?}");
    }
}

/// The records' saga IDs, sorted, to compare which records came back.
fn ids(records: &[SagaRecord]) -> Vec<Uuid> {
    sorted(records.iter().map(|record| record.saga_id))
}

fn sorted(ids: impl IntoIterator<Item = Uuid>) -> Vec<Uuid> {
    let mut ids: Vec<_> = ids.into_iter().collect();
    ids.sort();
    ids
}

async fn sagas_round_trip(store: Store) {
    assert_eq!(store.load(TRANSFER, saga_id(1)).await.unwrap(), None);
    save(&store, TRANSFER, 1, SagaStatus::Running).await;
    let first = load(&store, TRANSFER, 1).await;
    let expected = input(TRANSFER, 1, SagaStatus::Running);
    assert_eq!(first.saga_id, expected.saga_id);
    assert_eq!(first.saga_type, TRANSFER);
    assert_eq!(first.correlation_id, expected.correlation_id);
    assert_eq!(first.state, expected.state);
    assert_eq!(first.status, SagaStatus::Running);
    assert_eq!(first.current_step, expected.current_step);
    assert_eq!(first.failure_reason, None);
    assert_eq!(first.completed_at, None);
    assert!(first.started_at <= first.updated_at);

    let mut next = input(TRANSFER, 1, SagaStatus::Compensating);
    next.state = json!({ "amount": 100, "debited": false });
    next.current_step = "compensateWhenDepositFailed".into();
    store.save(next.clone()).await.unwrap();
    let second = load(&store, TRANSFER, 1).await;
    assert_eq!(
        (second.status, &second.state),
        (SagaStatus::Compensating, &next.state)
    );
    assert_eq!(second.started_at, first.started_at, "started_at is kept");
    assert!(second.updated_at >= first.updated_at);
    assert_eq!(second.completed_at, None);

    assert_eq!(store.load(REFUND, saga_id(1)).await.unwrap(), None);
    let by_correlation = store.load_by_correlation_id(TRANSFER, saga_id(1)).await;
    assert_eq!(by_correlation.unwrap(), Some(second));
}

async fn a_terminal_save_sets_completed_at(store: Store) {
    let mut failed = input(TRANSFER, 1, SagaStatus::Failed);
    failed.failure_reason = Some("insufficient funds".into());
    store.save(failed).await.unwrap();
    let record = load(&store, TRANSFER, 1).await;
    assert_eq!(record.completed_at, Some(record.updated_at));
    assert_eq!(record.failure_reason.as_deref(), Some("insufficient funds"));
}

async fn complete_ends_a_saga_once(store: Store) {
    save(&store, TRANSFER, 1, SagaStatus::Running).await;
    let running = load(&store, TRANSFER, 1).await;
    store.complete(TRANSFER, saga_id(1)).await.unwrap();
    let completed = load(&store, TRANSFER, 1).await;
    assert_eq!(completed.status, SagaStatus::Completed);
    assert_eq!(completed.state, running.state, "the state is left as it is");
    assert_eq!(completed.current_step, running.current_step);
    assert_eq!(completed.started_at, running.started_at);
    assert!(completed.updated_at >= running.updated_at);
    assert_eq!(completed.completed_at, Some(completed.updated_at));

    store.complete(TRANSFER, saga_id(1)).await.unwrap();
    store.fail(TRANSFER, saga_id(1), "too late").await.unwrap();
    assert_eq!(
        load(&store, TRANSFER, 1).await,
        completed,
        "an ended saga is unchanged"
    );

    store.complete(TRANSFER, saga_id(9)).await.unwrap();
    store.fail(TRANSFER, saga_id(9), "missing").await.unwrap();
    assert_eq!(store.load(TRANSFER, saga_id(9)).await.unwrap(), None);
}

async fn fail_ends_a_saga_once(store: Store) {
    save(&store, TRANSFER, 1, SagaStatus::Compensating).await;
    store
        .fail(TRANSFER, saga_id(1), "refund rejected")
        .await
        .unwrap();
    let failed = load(&store, TRANSFER, 1).await;
    assert_eq!(failed.status, SagaStatus::Failed);
    assert_eq!(failed.failure_reason.as_deref(), Some("refund rejected"));
    assert_eq!(failed.completed_at, Some(failed.updated_at));

    store.fail(TRANSFER, saga_id(1), "again").await.unwrap();
    store.complete(TRANSFER, saga_id(1)).await.unwrap();
    assert_eq!(
        load(&store, TRANSFER, 1).await,
        failed,
        "an ended saga is unchanged"
    );
}

async fn load_by_status_filters_orders_and_limits(store: Store) {
    save(&store, TRANSFER, 1, SagaStatus::Running).await;
    save(&store, REFUND, 2, SagaStatus::Running).await;
    save(&store, TRANSFER, 3, SagaStatus::Running).await;
    save(&store, TRANSFER, 4, SagaStatus::Completed).await;
    save(&store, TRANSFER, 1, SagaStatus::Running).await;

    let running = store.load_by_status(SagaStatus::Running, SagaQuery::default());
    let running = running.await.unwrap();
    assert_list_order(&running);
    assert_eq!(ids(&running), sorted([saga_id(1), saga_id(2), saga_id(3)]));

    let transfers = |limit| SagaQuery {
        saga_type: Some(TRANSFER.into()),
        limit,
    };
    let all_transfers = store.load_by_status(SagaStatus::Running, transfers(None));
    let all_transfers = all_transfers.await.unwrap();
    assert_list_order(&all_transfers);
    assert_eq!(ids(&all_transfers), sorted([saga_id(1), saga_id(3)]));
    let first = store.load_by_status(SagaStatus::Running, transfers(Some(1)));
    let first = first.await.unwrap();
    assert_eq!(
        first,
        all_transfers[..1],
        "the type filter applies before the limit"
    );

    let completed = store.load_by_status(SagaStatus::Completed, SagaQuery::default());
    assert_eq!(ids(&completed.await.unwrap()), [saga_id(4)]);
    let failed = store.load_by_status(SagaStatus::Failed, SagaQuery::default());
    assert!(failed.await.unwrap().is_empty());
}

async fn load_timed_out_takes_active_sagas_before_the_cut_off(store: Store) {
    save(&store, TRANSFER, 1, SagaStatus::Running).await;
    save(&store, REFUND, 2, SagaStatus::Compensating).await;
    save(&store, TRANSFER, 3, SagaStatus::Running).await;
    save(&store, TRANSFER, 4, SagaStatus::Completed).await;
    save(&store, TRANSFER, 5, SagaStatus::Failed).await;

    let all = store.load_timed_out(far_future(), SagaQuery::default());
    let all = all.await.unwrap();
    assert_list_order(&all);
    assert_eq!(
        ids(&all),
        sorted([saga_id(1), saga_id(2), saga_id(3)]),
        "only active sagas"
    );
    let none = store.load_timed_out(long_ago(), SagaQuery::default());
    assert!(none.await.unwrap().is_empty());

    let first = load(&store, TRANSFER, 1).await;
    let query = SagaQuery {
        saga_type: Some(TRANSFER.into()),
        limit: None,
    };
    let at = store
        .load_timed_out(first.updated_at, query.clone())
        .await
        .unwrap();
    assert!(!at.contains(&first), "the cut-off is exclusive");
    let after = first.updated_at + TimeDelta::milliseconds(1);
    let after = store.load_timed_out(after, query).await.unwrap();
    assert!(after.contains(&first));
    assert!(after.iter().all(|record| record.saga_type == TRANSFER));

    let limited = SagaQuery {
        saga_type: Some(REFUND.into()),
        limit: Some(1),
    };
    let refunds = store.load_timed_out(far_future(), limited).await.unwrap();
    assert_eq!(
        ids(&refunds),
        sorted([saga_id(2)]),
        "the type filter applies before the limit"
    );
}

async fn delete_and_count(store: Store) {
    assert_eq!(store.count(None, None).await.unwrap(), 0);
    save(&store, TRANSFER, 1, SagaStatus::Running).await;
    save(&store, TRANSFER, 2, SagaStatus::Completed).await;
    save(&store, REFUND, 3, SagaStatus::Running).await;

    let count = async |saga_type, status| store.count(saga_type, status).await.unwrap();
    assert_eq!(count(None, None).await, 3);
    assert_eq!(count(Some(TRANSFER), None).await, 2);
    assert_eq!(count(None, Some(SagaStatus::Running)).await, 2);
    assert_eq!(count(Some(TRANSFER), Some(SagaStatus::Running)).await, 1);
    assert_eq!(count(Some(REFUND), Some(SagaStatus::Failed)).await, 0);

    store.delete(TRANSFER, saga_id(1)).await.unwrap();
    store.delete(TRANSFER, saga_id(1)).await.unwrap();
    store.delete(REFUND, saga_id(2)).await.unwrap();
    assert_eq!(store.load(TRANSFER, saga_id(1)).await.unwrap(), None);
    assert_eq!(count(None, None).await, 2);
}
