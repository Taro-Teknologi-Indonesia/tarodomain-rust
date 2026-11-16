//! Concurrency: appends, reads and cursors while other work is in flight.

use std::cell::Cell;
use std::collections::HashSet;
use std::future::poll_fn;
use std::sync::Arc;
use std::task::Poll;

use tarodomain::adapters::EventStoreAdapter;
use tarodomain::error::ErrorCode;
use tarodomain::kernel::ids::Uuid;

use crate::ProviderFactory;
use crate::fixtures::{event_id, events, ids, stream_id};

/// Checks that a provider's event store stays correct under concurrent use:
/// a reader following `load_all` with a cursor while several writers append
/// sees every event exactly once, writers that retry on
/// `OPTIMISTIC_CONCURRENCY` lose no update, and 16 concurrent readers all
/// succeed with the same answer.
///
/// The work runs concurrently in one task (`tokio::join!` and
/// `futures::future::join_all`), so a provider whose adapters do real I/O
/// interleaves it at every `.await`.
///
/// Panics on the first case that fails.
pub async fn concurrency_contract(factory: impl ProviderFactory) {
    let store = || async { factory.create().await.create_event_store_adapter() };
    a_following_reader_sees_every_event_once(store().await).await;
    writers_retrying_on_conflict_lose_nothing(store().await).await;
    concurrent_readers_never_fail(store().await).await;
}

type Store = Arc<dyn EventStoreAdapter>;

const ACCOUNT: &str = "BankAccount";

/// Lets the other futures of a join run, without needing a runtime.
async fn yield_now() {
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            return Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    })
    .await;
}

/// Event `i` of writer `w`. Writers interleave, so later appends often have
/// lower IDs: the reader can only follow append order.
fn written_id(w: u32, i: u32) -> Uuid {
    event_id(w * 1000 + i)
}

/// Asserts that `seen` holds every event of `writers` writers with `each`
/// events, once, each writer's in its own order.
fn assert_each_once(seen: &[Uuid], writers: u32, each: u32) {
    let unique: HashSet<_> = seen.iter().collect();
    assert_eq!(unique.len(), seen.len(), "an event was seen twice");
    assert_eq!(seen.len(), (writers * each) as usize, "an event was missed");
    for w in 0..writers {
        let expected: Vec<_> = (0..each).map(|i| written_id(w, i)).collect();
        let theirs = seen.iter().copied().filter(|id| expected.contains(id));
        let theirs: Vec<_> = theirs.collect();
        assert_eq!(theirs, expected, "writer {w}'s events out of order");
    }
}

async fn a_following_reader_sees_every_event_once(store: Store) {
    const WRITERS: u32 = 4;
    const EACH: u32 = 25;
    let finished = Cell::new(0);
    let writer = |w: u32| {
        let (store, finished) = (&store, &finished);
        async move {
            let stream = stream_id(ACCOUNT, w);
            for i in 0..EACH {
                let appended = events([written_id(w, i)]);
                let version = u64::from(i);
                store
                    .append_to_stream(&stream, appended, version)
                    .await
                    .unwrap();
                yield_now().await;
            }
            finished.set(finished.get() + 1);
        }
    };
    let reader = async {
        let (mut seen, mut cursor) = (Vec::new(), None);
        loop {
            // Read the flag first: an empty read after every writer had
            // finished means there is nothing left.
            let done = finished.get() == WRITERS;
            let batch = store.load_all(cursor, Some(7)).await.unwrap();
            if batch.is_empty() && done {
                return seen;
            }
            if let Some(last) = batch.last() {
                cursor = Some(last.event_id);
            }
            seen.extend(ids(&batch));
            yield_now().await;
        }
    };
    let (.., seen) = tokio::join!(writer(0), writer(1), writer(2), writer(3), reader);
    assert_each_once(&seen, WRITERS, EACH);
    let all = store.load_all(None, None).await.unwrap();
    assert_eq!(ids(&all), seen, "the reader saw the events in append order");
}

async fn writers_retrying_on_conflict_lose_nothing(store: Store) {
    const WRITERS: u32 = 4;
    const EACH: u32 = 5;
    let stream = stream_id(ACCOUNT, 1);
    let writer = |w: u32| {
        let (store, stream) = (&store, &stream);
        async move {
            for i in 0..EACH {
                loop {
                    let version = store.get_stream_version(stream).await.unwrap();
                    yield_now().await;
                    let appended = events([written_id(w, i)]);
                    match store.append_to_stream(stream, appended, version).await {
                        Ok(_) => break,
                        Err(error) if *error.code() == ErrorCode::OptimisticConcurrency => {
                            yield_now().await;
                        }
                        Err(error) => panic!("writer {w}: {error:?}"),
                    }
                }
            }
        }
    };
    tokio::join!(writer(0), writer(1), writer(2), writer(3));
    let total = u64::from(WRITERS * EACH);
    assert_eq!(store.get_stream_version(&stream).await.unwrap(), total);
    let loaded = store.load_stream(&stream, 0, None).await.unwrap();
    assert_each_once(&ids(&loaded), WRITERS, EACH);
}

async fn concurrent_readers_never_fail(store: Store) {
    for w in 0..3 {
        let appended = events((0..3).map(|i| written_id(w, i)));
        let stream = stream_id(ACCOUNT, w);
        store.append_to_stream(&stream, appended, 0).await.unwrap();
    }
    let reader = |r: u32| {
        let store = &store;
        async move {
            let stream = stream_id(ACCOUNT, r % 3);
            let all = store.load_all(None, None).await;
            let category = store.load_stream_after_event(ACCOUNT, None, None).await;
            let one = store.load_stream(&stream, 0, None).await;
            let version = store.get_stream_version(&stream).await;
            (r, all, category, one, version)
        }
    };
    let results = futures::future::join_all((0..16).map(reader)).await;
    let expected_all = ids(&store.load_all(None, None).await.unwrap());
    assert_eq!(expected_all.len(), 9);
    for (r, all, category, one, version) in results {
        let expected_one: Vec<_> = (0..3).map(|i| written_id(r % 3, i)).collect();
        assert_eq!(ids(&all.unwrap()), expected_all, "reader {r}: load_all");
        assert_eq!(
            ids(&category.unwrap()),
            expected_all,
            "reader {r}: category"
        );
        assert_eq!(ids(&one.unwrap()), expected_one, "reader {r}: load_stream");
        assert_eq!(version.unwrap(), 3, "reader {r}: version");
    }
}
