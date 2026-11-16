//! The D1 event store adapter on SQLite, case by case. The shared contract
//! suites run against the whole provider in `tests/contract.rs` (Step 10.6).

use std::sync::Arc;

use tarodomain::adapters::{EventStoreAdapter, UnitOfWork};
use tarodomain::error::ErrorCode;
use tarodomain::kernel::clock::SystemClock;
use tarodomain_cloudflare::d1::D1Db;
use tarodomain_cloudflare::event_store::D1EventStoreAdapter;
use tarodomain_cloudflare::migrations::migrate;
use tarodomain_cloudflare::sqlite_backend::SqliteBackend;
use tarodomain_cloudflare::unit_of_work::D1UnitOfWork;
use tarodomain_testing::fixtures::{event_id, events, ids, stream_id};

const ACCOUNT: &str = "BankAccount";

async fn store() -> (D1EventStoreAdapter, D1Db) {
    let db = D1Db::new(Arc::new(SqliteBackend::in_memory().unwrap()));
    migrate(&db).await.unwrap();
    (
        D1EventStoreAdapter::new(db.clone(), Arc::new(SystemClock)),
        db,
    )
}

#[tokio::test]
async fn versions_start_at_one_and_a_stale_writer_conflicts() {
    let (store, _) = store().await;
    let stream = stream_id(ACCOUNT, 1);
    assert_eq!(store.get_stream_version(&stream).await.unwrap(), 0);
    let result = store
        .append_to_stream(&stream, events([event_id(1), event_id(2)]), 0)
        .await
        .unwrap();
    assert_eq!(result.version, 2);
    let error = store
        .append_to_stream(&stream, events([event_id(3)]), 0)
        .await
        .unwrap_err();
    assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
    assert_eq!(error.context()["actualVersion"], 2);
    let error = store
        .append_to_stream(&stream, events([event_id(3)]), 5)
        .await
        .unwrap_err();
    assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
    assert_eq!(store.get_stream_version(&stream).await.unwrap(), 2);
}

#[tokio::test]
async fn racing_appends_let_exactly_one_win() {
    let (store, _) = store().await;
    let stream = stream_id(ACCOUNT, 1);
    let (a, b) = tokio::join!(
        store.append_to_stream(&stream, events([event_id(1)]), 0),
        store.append_to_stream(&stream, events([event_id(2)]), 0),
    );
    assert_eq!(a.is_ok() as u8 + b.is_ok() as u8, 1);
    let error = a.err().or(b.err()).unwrap();
    assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
}

#[tokio::test]
async fn duplicate_ids_and_non_aggregate_streams_write_nothing() {
    let (store, _) = store().await;
    let stream = stream_id(ACCOUNT, 1);
    store
        .append_to_stream(&stream, events([event_id(1)]), 0)
        .await
        .unwrap();
    for batch in [events([event_id(1)]), events([event_id(2), event_id(2)])] {
        let error = store.append_to_stream(&stream, batch, 1).await.unwrap_err();
        assert_eq!(*error.code(), ErrorCode::EventAppendFailed);
    }
    assert_eq!(store.get_stream_version(&stream).await.unwrap(), 1);
    let error = store
        .append_to_stream("BankAccount", events([event_id(9)]), 0)
        .await
        .unwrap_err();
    assert_eq!(*error.code(), ErrorCode::EventAppendFailed);
}

#[tokio::test]
async fn reads_by_stream_category_and_all_follow_append_order() {
    let (store, _) = store().await;
    let (one, two, other) = (
        stream_id(ACCOUNT, 1),
        stream_id(ACCOUNT, 2),
        stream_id("Transfer", 1),
    );
    store
        .append_to_stream(&one, events([event_id(1)]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&other, events([event_id(2)]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&two, events([event_id(3)]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&one, events([event_id(4)]), 1)
        .await
        .unwrap();

    assert_eq!(
        ids(&store.load_stream(&one, 0, None).await.unwrap()),
        [event_id(1), event_id(4)]
    );
    assert_eq!(
        ids(&store.load_stream(&one, 1, None).await.unwrap()),
        [event_id(4)]
    );
    assert_eq!(
        ids(&store.load_stream(&one, 0, Some(1)).await.unwrap()),
        [event_id(1)]
    );
    let category = store
        .load_stream_after_event(ACCOUNT, None, None)
        .await
        .unwrap();
    assert_eq!(ids(&category), [event_id(1), event_id(3), event_id(4)]);
    let all = store.load_all(Some(event_id(2)), None).await.unwrap();
    assert_eq!(ids(&all), [event_id(3), event_id(4)]);
    let limited = store.load_all(None, Some(2)).await.unwrap();
    assert_eq!(ids(&limited), [event_id(1), event_id(2)]);
}

#[tokio::test]
async fn a_deleted_cursor_falls_back_to_comparing_ids() {
    let (store, _) = store().await;
    let (one, two) = (stream_id(ACCOUNT, 1), stream_id(ACCOUNT, 2));
    store
        .append_to_stream(&one, events([event_id(1)]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&two, events([event_id(2), event_id(3)]), 0)
        .await
        .unwrap();
    store.delete_stream(&one).await.unwrap();
    let after = store.load_all(Some(event_id(1)), None).await.unwrap();
    assert_eq!(ids(&after), [event_id(2), event_id(3)]);
    assert!(!store.stream_exists(&one).await.unwrap());
}

#[tokio::test]
async fn a_unit_reads_its_own_staged_events_and_appends_after_them() {
    let (store, db) = store().await;
    let stream = stream_id(ACCOUNT, 1);
    store
        .append_to_stream(&stream, events([event_id(1)]), 0)
        .await
        .unwrap();
    let unit: Arc<dyn UnitOfWork> = Arc::new(D1UnitOfWork::new(db.clone()));
    unit.execute(async {
        store
            .append_to_stream(&stream, events([event_id(2)]), 1)
            .await?;
        assert_eq!(store.get_stream_version(&stream).await?, 2);
        store
            .append_to_stream(&stream, events([event_id(3)]), 2)
            .await?;
        assert_eq!(
            ids(&store.load_stream(&stream, 0, None).await?),
            [event_id(1), event_id(2), event_id(3)]
        );
        assert_eq!(
            ids(&store.load_stream(&stream, 2, None).await?),
            [event_id(3)]
        );
        assert_eq!(
            ids(&store.load_all(Some(event_id(2)), None).await?),
            [event_id(3)]
        );
        let metadata = store.get_stream_metadata(&stream).await?.unwrap();
        assert_eq!(metadata.version, 3);
        let error = store
            .append_to_stream(&stream, events([event_id(9)]), 1)
            .await
            .unwrap_err();
        assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(
        store.get_stream_version(&stream).await.unwrap(),
        3,
        "committed"
    );
}

#[tokio::test]
async fn a_rolled_back_unit_leaves_no_events() {
    let (store, db) = store().await;
    let stream = stream_id(ACCOUNT, 1);
    let unit: Arc<dyn UnitOfWork> = Arc::new(D1UnitOfWork::new(db));
    let result: tarodomain::error::Result<()> = unit
        .execute(async {
            store
                .append_to_stream(&stream, events([event_id(1)]), 0)
                .await?;
            Err(tarodomain::error::FrameworkError::invalid_configuration(
                "no",
            ))
        })
        .await;
    assert!(result.is_err());
    assert_eq!(store.get_stream_version(&stream).await.unwrap(), 0);
}

#[tokio::test]
async fn subscriptions_move_forward_only_and_count_failures() {
    let (store, _) = store().await;
    let stream = stream_id(ACCOUNT, 1);
    store
        .append_to_stream(&stream, events([event_id(1), event_id(2), event_id(3)]), 0)
        .await
        .unwrap();
    let first = store.subscribe("s", Some("$all")).await.unwrap();
    assert_eq!(first.stream_id, None);
    assert_eq!(first.last_seen, None);
    let again = store.subscribe("s", Some(ACCOUNT)).await.unwrap();
    assert_eq!(again.stream_id, None, "subscribe is idempotent");

    assert_eq!(
        store.record_failed_attempt("s", event_id(2)).await.unwrap(),
        1
    );
    assert_eq!(
        store.record_failed_attempt("s", event_id(2)).await.unwrap(),
        2
    );
    assert_eq!(
        store.record_failed_attempt("s", event_id(3)).await.unwrap(),
        1
    );
    assert_eq!(
        store
            .record_failed_attempt("missing", event_id(3))
            .await
            .unwrap(),
        0
    );

    store.ack_subscription("s", event_id(2)).await.unwrap();
    let acked = store.get_subscription("s").await.unwrap().unwrap();
    assert_eq!(acked.last_seen, Some(event_id(2)));
    assert_eq!((acked.failed_event_id, acked.failed_attempts), (None, 0));
    store.ack_subscription("s", event_id(1)).await.unwrap();
    assert_eq!(
        store
            .get_subscription("s")
            .await
            .unwrap()
            .unwrap()
            .last_seen,
        Some(event_id(2))
    );
    store.ack_subscription("s", event_id(3)).await.unwrap();
    assert_eq!(
        store.list_subscriptions().await.unwrap()[0].last_seen,
        Some(event_id(3))
    );
    store.delete_subscription("s").await.unwrap();
    assert!(store.get_subscription("s").await.unwrap().is_none());
}

#[tokio::test]
async fn correlation_and_metadata() {
    let (store, _) = store().await;
    let stream = stream_id(ACCOUNT, 1);
    let batch = events([event_id(1), event_id(2)]);
    let correlation = batch[0].correlation_id;
    store
        .append_to_stream(&stream, batch.clone(), 0)
        .await
        .unwrap();
    let found = store.load_by_correlation_id(correlation).await.unwrap();
    assert_eq!(
        found,
        batch
            .iter()
            .filter(|e| e.correlation_id == correlation)
            .cloned()
            .collect::<Vec<_>>()
    );
    let metadata = store.get_stream_metadata(&stream).await.unwrap().unwrap();
    assert_eq!(metadata.version, 2);
    assert!(metadata.updated_at >= metadata.created_at);
    assert_eq!(store.load_stream(&stream, 0, None).await.unwrap(), batch);
}
