//! The snapshot store adapter contract (spec §5.4, §8).

use chrono::{DateTime, Utc};
use serde_json::Value;

use crate::async_trait;
use crate::domain::Metadata;
use crate::error::Result;
use crate::kernel::ids::Uuid;

/// A snapshot to save (spec §8.2): an aggregate's state as of a version.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotInput {
    /// The aggregate's ID.
    pub source_uuid: Uuid,
    /// The aggregate's version the state is at.
    pub source_version: u64,
    /// The aggregate type ([`Aggregate::TYPE`](crate::domain::Aggregate::TYPE)).
    pub source_type: String,
    /// The state, as [`Aggregate::to_snapshot`](crate::domain::Aggregate::to_snapshot)
    /// returned it.
    pub data: Value,
    /// Free-form metadata, empty when there is none.
    pub metadata: Metadata,
}

/// A stored snapshot (spec §8.2): the [`SnapshotInput`] fields plus when it
/// was saved.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    /// The aggregate's ID.
    pub source_uuid: Uuid,
    /// The aggregate's version the state is at.
    pub source_version: u64,
    /// The aggregate type.
    pub source_type: String,
    /// The state.
    pub data: Value,
    /// Free-form metadata.
    pub metadata: Metadata,
    /// When the snapshot was last saved, by the store's clock.
    pub created_at: DateTime<Utc>,
}

impl Snapshot {
    /// The stored form of `input`, saved at `created_at`.
    pub fn new(input: SnapshotInput, created_at: DateTime<Utc>) -> Self {
        Self {
            source_uuid: input.source_uuid,
            source_version: input.source_version,
            source_type: input.source_type,
            data: input.data,
            metadata: input.metadata,
            created_at,
        }
    }
}

/// The snapshot store's storage (spec §8.1): aggregate states at versions,
/// so a repository can replay only the events after the latest one.
///
/// A snapshot's key is `(source_uuid, source_version)`. Every read also
/// filters on the aggregate type, so a snapshot is found only under the type
/// it was saved with. Snapshots are a cache: a repository treats any failure
/// here as non-critical and replays the whole stream instead (§6.2).
///
/// Deleting what doesn't exist does nothing.
#[async_trait]
pub trait SnapshotStoreAdapter: Send + Sync {
    /// Saves a snapshot. A save at a key that is already stored replaces that
    /// snapshot (an upsert), including its `created_at`.
    async fn save(&self, input: SnapshotInput) -> Result<()>;

    /// The snapshot with the highest version, if any.
    async fn load_latest(&self, source_uuid: Uuid, source_type: &str) -> Result<Option<Snapshot>>;

    /// The snapshot at exactly `version`, if any.
    async fn load_at_version(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        version: u64,
    ) -> Result<Option<Snapshot>>;

    /// The aggregate's snapshots, newest (highest version) first, at most
    /// `limit` of them.
    async fn list(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Snapshot>>;

    /// Deletes every snapshot of the aggregate.
    async fn delete_all(&self, source_uuid: Uuid, source_type: &str) -> Result<()>;

    /// Deletes the aggregate's snapshots below `version`, keeping the one at
    /// `version`, and returns how many it deleted.
    async fn delete_before(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        version: u64,
    ) -> Result<u64>;

    /// How many snapshots the aggregate has.
    async fn count(&self, source_uuid: Uuid, source_type: &str) -> Result<u64>;
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;

    use super::*;
    use crate::kernel::ids::new_v7;

    #[test]
    fn a_stored_snapshot_keeps_the_input_fields() {
        let input = SnapshotInput {
            source_uuid: new_v7(),
            source_version: 10,
            source_type: "BankAccount".into(),
            data: json!({"balance": 500}),
            metadata: Metadata::from([("reason", "every-10")]),
        };
        let at = DateTime::from_timestamp_millis(1_763_280_000_206).unwrap();
        let snapshot = Snapshot::new(input.clone(), at);
        assert_eq!(snapshot.source_uuid, input.source_uuid);
        assert_eq!(snapshot.source_version, 10);
        assert_eq!(snapshot.source_type, "BankAccount");
        assert_eq!(snapshot.data, input.data);
        assert_eq!(snapshot.metadata, input.metadata);
        assert_eq!(snapshot.created_at, at);
    }

    #[test]
    fn the_trait_is_dyn_compatible() {
        fn accepts(_: Option<Arc<dyn SnapshotStoreAdapter>>) {}
        accepts(None);
    }
}
