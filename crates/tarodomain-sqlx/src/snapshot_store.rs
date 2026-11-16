//! The SQL snapshot store (spec §8.1).

use std::sync::Arc;

use tarodomain::adapters::{Snapshot, SnapshotInput, SnapshotStoreAdapter};
use tarodomain::async_trait;
use tarodomain::error::Result;
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::Uuid;

use crate::db::{Db, Param, Row};

const COLUMNS: &str = "source_uuid, source_version, source_type, data, metadata, created_at";

/// Snapshots on SQL, keyed by `(source_uuid, source_version)`. Saving is an
/// upsert that also replaces `created_at`; reads filter on the type too.
#[derive(Clone)]
pub struct SqlxSnapshotStore {
    db: Db,
    clock: Arc<dyn Clock>,
}

impl SqlxSnapshotStore {
    /// A store over `db`. `created_at` comes from `clock`.
    pub fn new(db: Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }
}

fn parse(row: &Row) -> Result<Snapshot> {
    Ok(Snapshot {
        source_uuid: row.uuid("source_uuid")?,
        source_version: u64::try_from(row.i64("source_version")?).unwrap_or(0),
        source_type: row.str("source_type")?,
        data: row.json("data")?,
        metadata: row.metadata("metadata")?,
        created_at: row.time("created_at")?,
    })
}

#[async_trait]
impl SnapshotStoreAdapter for SqlxSnapshotStore {
    async fn save(&self, input: SnapshotInput) -> Result<()> {
        self.db
            .execute(
                &format!(
                    "INSERT INTO tarodomain_snapshots ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?) \
                     ON CONFLICT (source_uuid, source_version) DO UPDATE SET \
                     source_type = excluded.source_type, data = excluded.data, \
                     metadata = excluded.metadata, created_at = excluded.created_at"
                ),
                vec![
                    input.source_uuid.into(),
                    input.source_version.into(),
                    Param::text(input.source_type),
                    Param::Json(input.data),
                    Param::json(&input.metadata)?,
                    self.clock.now().into(),
                ],
            )
            .await
            .map(|_| ())
    }

    async fn load_latest(&self, source_uuid: Uuid, source_type: &str) -> Result<Option<Snapshot>> {
        let row = self
            .db
            .fetch_one(
                &format!(
                    "SELECT {COLUMNS} FROM tarodomain_snapshots \
                     WHERE source_uuid = ? AND source_type = ? \
                     ORDER BY source_version DESC LIMIT 1"
                ),
                vec![source_uuid.into(), Param::text(source_type)],
            )
            .await?;
        row.as_ref().map(parse).transpose()
    }

    async fn load_at_version(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        version: u64,
    ) -> Result<Option<Snapshot>> {
        let row = self
            .db
            .fetch_one(
                &format!(
                    "SELECT {COLUMNS} FROM tarodomain_snapshots \
                     WHERE source_uuid = ? AND source_type = ? AND source_version = ?"
                ),
                vec![source_uuid.into(), Param::text(source_type), version.into()],
            )
            .await?;
        row.as_ref().map(parse).transpose()
    }

    async fn list(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        limit: Option<usize>,
    ) -> Result<Vec<Snapshot>> {
        let rows = self
            .db
            .fetch(
                &format!(
                    "SELECT {COLUMNS} FROM tarodomain_snapshots \
                     WHERE source_uuid = ? AND source_type = ? \
                     ORDER BY source_version DESC LIMIT ?"
                ),
                vec![
                    source_uuid.into(),
                    Param::text(source_type),
                    self.db.limit(limit),
                ],
            )
            .await?;
        rows.iter().map(parse).collect()
    }

    async fn delete_all(&self, source_uuid: Uuid, source_type: &str) -> Result<()> {
        self.db
            .execute(
                "DELETE FROM tarodomain_snapshots WHERE source_uuid = ? AND source_type = ?",
                vec![source_uuid.into(), Param::text(source_type)],
            )
            .await
            .map(|_| ())
    }

    async fn delete_before(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        version: u64,
    ) -> Result<u64> {
        self.db
            .execute(
                "DELETE FROM tarodomain_snapshots \
                 WHERE source_uuid = ? AND source_type = ? AND source_version < ?",
                vec![source_uuid.into(), Param::text(source_type), version.into()],
            )
            .await
    }

    async fn count(&self, source_uuid: Uuid, source_type: &str) -> Result<u64> {
        let row = self
            .db
            .fetch_one(
                "SELECT COUNT(*) AS n FROM tarodomain_snapshots \
                 WHERE source_uuid = ? AND source_type = ?",
                vec![source_uuid.into(), Param::text(source_type)],
            )
            .await?;
        Ok(row
            .map_or(Ok(0), |row| row.i64("n"))?
            .try_into()
            .unwrap_or(0))
    }
}
