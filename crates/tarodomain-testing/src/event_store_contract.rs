//! The event store adapter contract (spec §3.2, §7, §18.1).

use std::sync::Arc;

use tarodomain::adapters::{EventStoreAdapter, StreamIds};
use tarodomain::error::{ErrorCode, FrameworkError, Result};

use crate::ProviderFactory;
use crate::fixtures::{event, event_id, events, ids, same_millisecond_ids, stream_id, uuid};

/// Checks a provider's [`EventStoreAdapter`] against its rustdoc contract:
/// versions, optimistic concurrency (also between two racing appends),
/// atomic appends, reads by version and by cursor in append order (also
/// when IDs from one millisecond sort the other way), category and `$all`
/// reads, correlation, deletion, the stored fields, and subscriptions.
///
/// Panics on the first case that fails.
pub async fn event_store_contract(factory: impl ProviderFactory) {
    let store = || async { factory.create().await.create_event_store_adapter() };
    versions_count_from_one(store().await).await;
    a_stale_version_conflicts(store().await).await;
    racing_appends_let_exactly_one_win(store().await).await;
    an_empty_append_checks_the_version(store().await).await;
    appends_are_atomic_and_only_to_aggregate_streams(store().await).await;
    events_round_trip_unchanged(store().await).await;
    load_stream_reads_after_a_version(store().await).await;
    cross_stream_reads_follow_append_order(store().await).await;
    category_and_all_reads(store().await).await;
    a_cursor_without_its_event_compares_ids(store().await).await;
    correlated_events_in_append_order(store().await).await;
    delete_stream_removes_it_everywhere(store().await).await;
    subscriptions_are_idempotent(store().await).await;
    acks_only_move_forward(store().await).await;
    failed_attempts_count_per_event(store().await).await;
}

type Store = Arc<dyn EventStoreAdapter>;

const ACCOUNT: &str = "BankAccount";
const TRANSFER: &str = "Transfer";

fn assert_code<T: std::fmt::Debug>(
    result: Result<T>,
    code: ErrorCode,
    case: &str,
) -> FrameworkError {
    match result {
        Err(error) if *error.code() == code => error,
        other => panic!("{case}: expected {code}, got {other:?}"),
    }
}

async fn versions_count_from_one(store: Store) {
    let stream = stream_id(ACCOUNT, 1);
    assert_eq!(store.get_stream_version(&stream).await.unwrap(), 0);
    assert!(!store.stream_exists(&stream).await.unwrap());
    assert_eq!(store.get_stream_metadata(&stream).await.unwrap(), None);

    let first = store.append_to_stream(&stream, events([event_id(1), event_id(2)]), 0);
    assert_eq!(
        first.await.unwrap().version,
        2,
        "append returns the new version"
    );
    let second = store.append_to_stream(&stream, events([event_id(3)]), 2);
    assert_eq!(second.await.unwrap().version, 3);

    assert_eq!(store.get_stream_version(&stream).await.unwrap(), 3);
    assert!(store.stream_exists(&stream).await.unwrap());
    let metadata = store.get_stream_metadata(&stream).await.unwrap().unwrap();
    assert_eq!(metadata.stream_id, stream);
    assert_eq!(metadata.version, 3);
    assert!(metadata.created_at <= metadata.updated_at);
}

async fn a_stale_version_conflicts(store: Store) {
    let stream = stream_id(ACCOUNT, 1);
    store
        .append_to_stream(&stream, events([event_id(1), event_id(2)]), 0)
        .await
        .unwrap();
    for expected in [0, 1, 3] {
        let result = store.append_to_stream(&stream, events([event_id(3)]), expected);
        let error = assert_code(
            result.await,
            ErrorCode::OptimisticConcurrency,
            "stale append",
        );
        assert_eq!(error.context()["streamId"], stream.as_str());
        assert_eq!(error.context()["expectedVersion"], expected);
        assert_eq!(error.context()["actualVersion"], 2);
        assert!(error.retryable());
    }
    assert_eq!(store.get_stream_version(&stream).await.unwrap(), 2);
    let loaded = store.load_stream(&stream, 0, None).await.unwrap();
    assert_eq!(ids(&loaded), [event_id(1), event_id(2)]);
}

async fn racing_appends_let_exactly_one_win(store: Store) {
    let stream = stream_id(ACCOUNT, 1);
    let (a, b) = tokio::join!(
        store.append_to_stream(&stream, events([event_id(1)]), 0),
        store.append_to_stream(&stream, events([event_id(2), event_id(3)]), 0),
    );
    let winner = match (a, b) {
        (Ok(result), Err(error)) => {
            assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
            assert_eq!(result.version, 1);
            vec![event_id(1)]
        }
        (Err(error), Ok(result)) => {
            assert_eq!(*error.code(), ErrorCode::OptimisticConcurrency);
            assert_eq!(result.version, 2);
            vec![event_id(2), event_id(3)]
        }
        other => panic!("racing appends: expected one winner, got {other:?}"),
    };
    let loaded = store.load_stream(&stream, 0, None).await.unwrap();
    assert_eq!(ids(&loaded), winner);
}

async fn an_empty_append_checks_the_version(store: Store) {
    let stream = stream_id(ACCOUNT, 1);
    let result = store
        .append_to_stream(&stream, Vec::new(), 0)
        .await
        .unwrap();
    assert_eq!(result.version, 0);
    assert!(!store.stream_exists(&stream).await.unwrap());

    store
        .append_to_stream(&stream, events([event_id(1)]), 0)
        .await
        .unwrap();
    let result = store
        .append_to_stream(&stream, Vec::new(), 1)
        .await
        .unwrap();
    assert_eq!(result.version, 1);
    let stale = store.append_to_stream(&stream, Vec::new(), 0).await;
    assert_code(
        stale,
        ErrorCode::OptimisticConcurrency,
        "empty stale append",
    );
    assert_eq!(store.load_all(None, None).await.unwrap().len(), 1);
}

async fn appends_are_atomic_and_only_to_aggregate_streams(store: Store) {
    for stream in [StreamIds::ALL, ACCOUNT] {
        let result = store.append_to_stream(stream, events([event_id(1)]), 0);
        assert_code(result.await, ErrorCode::EventAppendFailed, stream);
    }
    assert!(store.load_all(None, None).await.unwrap().is_empty());

    let (account, other) = (stream_id(ACCOUNT, 1), stream_id(ACCOUNT, 2));
    store
        .append_to_stream(&account, events([event_id(1)]), 0)
        .await
        .unwrap();
    let stored_again = events([event_id(2), event_id(1)]);
    let result = store.append_to_stream(&other, stored_again, 0).await;
    assert_code(result, ErrorCode::EventAppendFailed, "an ID already stored");
    let twice = events([event_id(3), event_id(3)]);
    let result = store.append_to_stream(&other, twice, 0).await;
    assert_code(
        result,
        ErrorCode::EventAppendFailed,
        "an ID twice in one append",
    );

    assert_eq!(store.get_stream_version(&other).await.unwrap(), 0);
    let all = store.load_all(None, None).await.unwrap();
    assert_eq!(ids(&all), [event_id(1)], "a failed append wrote nothing");
}

async fn events_round_trip_unchanged(store: Store) {
    let stream = stream_id(ACCOUNT, 1);
    let mut minimal = event(event_id(2));
    minimal.causation_id = None;
    minimal.command_causation_id = None;
    minimal.metadata = Default::default();
    minimal.data = serde_json::json!({});
    let appended = vec![event(event_id(1)), minimal];
    store
        .append_to_stream(&stream, appended.clone(), 0)
        .await
        .unwrap();
    assert_eq!(store.load_stream(&stream, 0, None).await.unwrap(), appended);
    assert_eq!(store.load_all(None, None).await.unwrap(), appended);
}

async fn load_stream_reads_after_a_version(store: Store) {
    let (stream, other) = (stream_id(ACCOUNT, 1), stream_id(ACCOUNT, 2));
    let four = events((1..=4).map(event_id));
    store.append_to_stream(&stream, four, 0).await.unwrap();
    store
        .append_to_stream(&other, events([event_id(5)]), 0)
        .await
        .unwrap();

    let read = async |after, max| ids(&store.load_stream(&stream, after, max).await.unwrap());
    let all = [event_id(1), event_id(2), event_id(3), event_id(4)];
    assert_eq!(read(0, None).await, all);
    assert_eq!(read(2, None).await, all[2..]);
    assert_eq!(read(1, Some(2)).await, all[1..3]);
    assert!(read(0, Some(0)).await.is_empty());
    assert!(read(4, None).await.is_empty());
    assert!(read(9, None).await.is_empty());
    let missing = store.load_stream(&stream_id(ACCOUNT, 3), 0, None).await;
    assert!(missing.unwrap().is_empty());
}

async fn cross_stream_reads_follow_append_order(store: Store) {
    let (first, second) = same_millisecond_ids();
    let (account, transfer) = (stream_id(ACCOUNT, 1), stream_id(TRANSFER, 1));
    store
        .append_to_stream(&account, events([first]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&transfer, events([second]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&account, events([event_id(1)]), 1)
        .await
        .unwrap();

    let all = store.load_all(None, None).await.unwrap();
    assert_eq!(
        ids(&all),
        [first, second, event_id(1)],
        "append order, not ID order"
    );
    let after_first = store.load_all(Some(first), None).await.unwrap();
    assert_eq!(ids(&after_first), [second, event_id(1)]);
    let after_second = store.load_all(Some(second), None).await.unwrap();
    assert_eq!(ids(&after_second), [event_id(1)]);
    let category = store.load_stream_after_event(ACCOUNT, Some(first), None);
    assert_eq!(ids(&category.await.unwrap()), [event_id(1)]);
}

async fn category_and_all_reads(store: Store) {
    let (one, two) = (stream_id(ACCOUNT, 1), stream_id(ACCOUNT, 2));
    let transfer = stream_id(TRANSFER, 1);
    store
        .append_to_stream(&one, events([event_id(1), event_id(2)]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&transfer, events([event_id(3)]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&two, events([event_id(4)]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&one, events([event_id(5)]), 2)
        .await
        .unwrap();

    let read = async |stream: &str, after, max| {
        ids(&store
            .load_stream_after_event(stream, after, max)
            .await
            .unwrap())
    };
    let accounts = [event_id(1), event_id(2), event_id(4), event_id(5)];
    assert_eq!(read(ACCOUNT, None, None).await, accounts);
    assert_eq!(
        read(ACCOUNT, Some(event_id(2)), Some(1)).await,
        [event_id(4)]
    );
    assert_eq!(read(TRANSFER, None, None).await, [event_id(3)]);
    assert!(read("Unknown", None, None).await.is_empty());
    assert_eq!(
        read(&one, Some(event_id(1)), None).await,
        [event_id(2), event_id(5)]
    );
    let everything: Vec<_> = (1..=5).map(event_id).collect();
    assert_eq!(read(StreamIds::ALL, None, None).await, everything);
    assert_eq!(
        read(StreamIds::ALL, Some(event_id(3)), Some(1)).await,
        [event_id(4)]
    );
    let all = ids(&store.load_all(None, Some(2)).await.unwrap());
    assert_eq!(all, [event_id(1), event_id(2)]);
    assert!(
        read(StreamIds::ALL, Some(event_id(5)), None)
            .await
            .is_empty()
    );
}

async fn a_cursor_without_its_event_compares_ids(store: Store) {
    let (account, transfer) = (stream_id(ACCOUNT, 1), stream_id(TRANSFER, 1));
    store
        .append_to_stream(&transfer, events([event_id(5)]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&account, events([event_id(4), event_id(6)]), 0)
        .await
        .unwrap();
    store.delete_stream(&transfer).await.unwrap();
    let after = store.load_all(Some(event_id(5)), None).await.unwrap();
    assert_eq!(ids(&after), [event_id(6)], "IDs after the deleted cursor");
}

async fn correlated_events_in_append_order(store: Store) {
    let correlation = uuid("correlation", 1);
    let correlated = |n| {
        let mut event = event(event_id(n));
        event.correlation_id = correlation;
        event
    };
    let (account, transfer) = (stream_id(ACCOUNT, 1), stream_id(TRANSFER, 1));
    let first = vec![correlated(3), event(event_id(1))];
    store.append_to_stream(&account, first, 0).await.unwrap();
    store
        .append_to_stream(&transfer, vec![correlated(2)], 0)
        .await
        .unwrap();
    let loaded = store.load_by_correlation_id(correlation).await.unwrap();
    assert_eq!(ids(&loaded), [event_id(3), event_id(2)]);
    let none = store.load_by_correlation_id(uuid("correlation", 2)).await;
    assert!(none.unwrap().is_empty());
}

async fn delete_stream_removes_it_everywhere(store: Store) {
    let (account, other) = (stream_id(ACCOUNT, 1), stream_id(ACCOUNT, 2));
    store
        .append_to_stream(&account, events([event_id(1), event_id(2)]), 0)
        .await
        .unwrap();
    store
        .append_to_stream(&other, events([event_id(3)]), 0)
        .await
        .unwrap();
    store.delete_stream(&account).await.unwrap();
    store.delete_stream(&account).await.unwrap();
    store.delete_stream(&stream_id(ACCOUNT, 9)).await.unwrap();

    assert_eq!(store.get_stream_version(&account).await.unwrap(), 0);
    assert_eq!(store.get_stream_metadata(&account).await.unwrap(), None);
    assert!(
        store
            .load_stream(&account, 0, None)
            .await
            .unwrap()
            .is_empty()
    );
    let category = store.load_stream_after_event(ACCOUNT, None, None).await;
    assert_eq!(ids(&category.unwrap()), [event_id(3)]);
    assert_eq!(
        ids(&store.load_all(None, None).await.unwrap()),
        [event_id(3)]
    );
    let correlated = store.load_by_correlation_id(event_id(1)).await.unwrap();
    assert!(correlated.is_empty());

    let again = store.append_to_stream(&account, events([event_id(4)]), 0);
    assert_eq!(
        again.await.unwrap().version,
        1,
        "a deleted stream starts again"
    );
    let all = store.load_all(None, None).await.unwrap();
    assert_eq!(ids(&all), [event_id(3), event_id(4)]);
}

async fn subscriptions_are_idempotent(store: Store) {
    assert_eq!(store.get_subscription("balances").await.unwrap(), None);
    assert!(store.list_subscriptions().await.unwrap().is_empty());

    let created = store.subscribe("balances", Some(ACCOUNT)).await.unwrap();
    assert_eq!(created.name, "balances");
    assert_eq!(created.stream_id.as_deref(), Some(ACCOUNT));
    assert_eq!(created.last_seen, None);
    assert_eq!(
        (created.failed_event_id, created.failed_attempts),
        (None, 0)
    );
    let again = store.subscribe("balances", None).await.unwrap();
    assert_eq!(
        again, created,
        "subscribe returns the existing subscription"
    );

    let all = store
        .subscribe("audit", Some(StreamIds::ALL))
        .await
        .unwrap();
    assert_eq!(all.stream(), StreamIds::ALL);
    let everything = store.subscribe("alerts", None).await.unwrap();
    assert_eq!(everything.stream_id, None);

    let listed = store.list_subscriptions().await.unwrap();
    let names: Vec<_> = listed.iter().map(|s| s.name.as_str()).collect();
    assert_eq!(names, ["alerts", "audit", "balances"]);
    let loaded = store.get_subscription("balances").await.unwrap();
    assert_eq!(loaded, Some(created));

    store.delete_subscription("balances").await.unwrap();
    store.delete_subscription("balances").await.unwrap();
    assert_eq!(store.get_subscription("balances").await.unwrap(), None);
    assert_eq!(store.list_subscriptions().await.unwrap().len(), 2);
}

async fn acks_only_move_forward(store: Store) {
    let (first, second) = same_millisecond_ids();
    let stream = stream_id(ACCOUNT, 1);
    let appended = events([event_id(1), first, second]);
    store.append_to_stream(&stream, appended, 0).await.unwrap();
    store.subscribe("balances", None).await.unwrap();
    let last_seen = async || {
        let subscription = store.get_subscription("balances").await.unwrap();
        subscription.unwrap().last_seen
    };

    store.ack_subscription("balances", first).await.unwrap();
    assert_eq!(last_seen().await, Some(first));
    store
        .ack_subscription("balances", event_id(1))
        .await
        .unwrap();
    assert_eq!(
        last_seen().await,
        Some(first),
        "an earlier event doesn't move it back"
    );
    store.ack_subscription("balances", second).await.unwrap();
    assert_eq!(
        last_seen().await,
        Some(second),
        "append order, though the ID is lower"
    );
    store.ack_subscription("balances", first).await.unwrap();
    assert_eq!(last_seen().await, Some(second));
    store.ack_subscription("missing", first).await.unwrap();
    assert_eq!(store.get_subscription("missing").await.unwrap(), None);

    store.delete_subscription("balances").await.unwrap();
    let fresh = store.subscribe("balances", None).await.unwrap();
    assert_eq!(
        fresh.last_seen, None,
        "a new subscription starts from the beginning"
    );
}

async fn failed_attempts_count_per_event(store: Store) {
    let stream = stream_id(ACCOUNT, 1);
    let appended = events([event_id(1), event_id(2)]);
    store.append_to_stream(&stream, appended, 0).await.unwrap();
    store.subscribe("balances", None).await.unwrap();
    let failed = async |event| {
        store
            .record_failed_attempt("balances", event)
            .await
            .unwrap()
    };

    assert_eq!(failed(event_id(1)).await, 1);
    assert_eq!(failed(event_id(1)).await, 2);
    let subscription = store.get_subscription("balances").await.unwrap().unwrap();
    assert_eq!(subscription.failed_event_id, Some(event_id(1)));
    assert_eq!(subscription.failed_attempts, 2);
    assert_eq!(failed(event_id(2)).await, 1, "another event starts again");

    store
        .ack_subscription("balances", event_id(2))
        .await
        .unwrap();
    let subscription = store.get_subscription("balances").await.unwrap().unwrap();
    assert_eq!(
        (subscription.failed_event_id, subscription.failed_attempts),
        (None, 0)
    );
    let missing = store.record_failed_attempt("missing", event_id(1)).await;
    assert_eq!(missing.unwrap(), 0);
    assert_eq!(store.get_subscription("missing").await.unwrap(), None);
}
