//! Sharded projections (spec §12.1, §12.3).

use std::sync::Arc;

use super::projection::{Projection, ProjectionHandler};
use crate::adapters::ProcessedEventStore;
use crate::domain::DomainEvent;
use crate::error::{FrameworkError, Result};

/// The 32-bit FNV-1a hash of `data`.
pub fn fnv1a_32(data: &[u8]) -> u32 {
    let mut hash: u32 = 0x811c_9dc5;
    for byte in data {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(0x0100_0193);
    }
    hash
}

/// A projection split over several handlers, each processing the events
/// whose shard key hashes to its index.
///
/// Create the handlers with [`shard_handlers`]. Handler `i` is named
/// `<name>-shard-<i>` and records events under the projection's name with
/// `<name>-shard-<i>` as the instance (§12.3).
pub trait ShardProjection: Projection {
    /// How many shards there are. Must be at least 1.
    fn shard_count(&self) -> u32;

    /// The key whose hash picks the event's shard, e.g. the aggregate ID.
    fn shard_key(&self, event: &DomainEvent) -> String;

    /// The shard that handles the event: `fnv1a_32(key) % shard_count`.
    fn shard_of(&self, event: &DomainEvent) -> u32 {
        fnv1a_32(self.shard_key(event).as_bytes()) % self.shard_count().max(1)
    }
}

/// One [`ProjectionHandler`] per shard of `projection`.
///
/// Fails with `INVALID_CONFIGURATION` for zero shards or duplicate routes.
pub fn shard_handlers<P: ShardProjection>(
    projection: Arc<P>,
    processed: Arc<dyn ProcessedEventStore>,
) -> Result<Vec<ProjectionHandler<P>>> {
    let count = projection.shard_count();
    if count == 0 {
        return Err(FrameworkError::invalid_configuration(format!(
            "projection {} has no shards",
            projection.name()
        )));
    }
    (0..count)
        .map(|index| {
            let name = format!("{}-shard-{index}", projection.name());
            let owner = projection.clone();
            let accepts = Arc::new(move |event: &DomainEvent| owner.shard_of(event) == index);
            Ok(
                ProjectionHandler::new(projection.clone(), processed.clone())?.for_shard(
                    name.clone(),
                    name,
                    accepts,
                ),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::super::projection::tests::{Balance, Deposited, FakeProcessed, event};
    use super::super::projection::{Projection, ProjectionRoutes};
    use super::*;
    use crate::bus::EventHandler;
    use crate::domain::Recorded;

    #[test]
    fn fnv1a_matches_the_reference_vectors() {
        assert_eq!(fnv1a_32(b""), 0x811c_9dc5);
        assert_eq!(fnv1a_32(b"a"), 0xe40c_292c);
        assert_eq!(fnv1a_32(b"foobar"), 0xbf9c_f968);
    }

    struct Sharded(Balance);

    #[crate::async_trait]
    impl Projection for Sharded {
        fn name(&self) -> &str {
            "sharded"
        }
        fn routes(r: &mut ProjectionRoutes<Self>) {
            r.on::<Deposited, _, _>(|p: Arc<Self>, e: Recorded<Deposited>| async move {
                *p.0.total.lock().unwrap() += e.amount;
                Ok(())
            });
        }
    }

    impl ShardProjection for Sharded {
        fn shard_count(&self) -> u32 {
            3
        }
        fn shard_key(&self, event: &DomainEvent) -> String {
            event.envelope().correlation_id.to_string()
        }
    }

    #[tokio::test]
    async fn each_event_is_handled_by_exactly_its_shard() {
        let p = Arc::new(Sharded(Balance::default()));
        let store = Arc::new(FakeProcessed::default());
        let handlers = shard_handlers(p.clone(), store.clone()).unwrap();
        let names: Vec<_> = handlers.iter().map(|h| h.name().to_owned()).collect();
        assert_eq!(
            names,
            ["sharded-shard-0", "sharded-shard-1", "sharded-shard-2"]
        );

        for n in 1..=20 {
            let e = event(Deposited { amount: n });
            let shard = p.shard_of(&e);
            assert_eq!(shard, p.shard_of(&e), "deterministic");
            for h in &handlers {
                h.handle(&e).await.unwrap();
            }
            for i in 0..3 {
                let instance = format!("sharded-shard-{i}");
                assert_eq!(store.has("sharded", &instance, e.id()), i == shard);
            }
        }
        assert_eq!(*p.0.total.lock().unwrap(), 210);
        assert_eq!(store.len(), 20);
    }

    #[test]
    fn zero_shards_is_invalid_configuration() {
        struct None0;
        #[crate::async_trait]
        impl Projection for None0 {
            fn name(&self) -> &str {
                "none"
            }
            fn routes(_: &mut ProjectionRoutes<Self>) {}
        }
        impl ShardProjection for None0 {
            fn shard_count(&self) -> u32 {
                0
            }
            fn shard_key(&self, _: &DomainEvent) -> String {
                String::new()
            }
        }
        let err = shard_handlers(Arc::new(None0), Arc::new(FakeProcessed::default())).unwrap_err();
        assert_eq!(err.code().as_str(), "INVALID_CONFIGURATION");
    }
}
