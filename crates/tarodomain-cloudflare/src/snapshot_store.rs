//! The D1 snapshot store (spec §8.1).

use std::sync::Arc;

use serde_json::json;
use tarodomain::adapters::{Snapshot, SnapshotInput, SnapshotStoreAdapter};
use tarodomain::async_trait;
use tarodomain::domain::format_timestamp;
use tarodomain::error::Result;
use tarodomain::kernel::clock::Clock;
use tarodomain::kernel::ids::Uuid;

use crate::d1::{D1Db, Row, RowExt, Statement, json_text};

const COLUMNS: &str = "source_uuid, source_version, source_type, data, metadata, created_at";

/// Snapshots on D1, keyed by `(source_uuid, source_version)`. Saving is an
/// upsert that also replaces `created_at`; reads filter on the type too.
#[derive(Clone)]
pub struct D1SnapshotStore {
    db: D1Db,
    clock: Arc<dyn Clock>,
}

impl D1SnapshotStore {
    /// A store over `db`. `created_at` comes from `clock`.
    pub fn new(db: D1Db, clock: Arc<dyn Clock>) -> Self {
        Self { db, clock }
    }
}

fn parse(row: &Row) -> Result<Snapshot> {
    Ok(Snapshot {
        source_uuid: row.require_uuid("source_uuid")?,
        source_version: u64::try_from(row.require_i64("source_version")?).unwrap_or(0),
        source_type: row.require_str("source_type")?.to_owned(),
        data: row.require_json("data")?,
        metadata: row.require_metadata("metadata")?,
        created_at: row.require_time("created_at")?,
    })
}

#[async_trait]
impl SnapshotStoreAdapter for D1SnapshotStore {
    async fn save(&self, input: SnapshotInput) -> Result<()> {
        self.db
            .write(vec![Statement::new(
                format!(
                    "INSERT INTO tarodomain_snapshots ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?) \
                     ON CONFLICT (source_uuid, source_version) DO UPDATE SET \
                     source_type = excluded.source_type, data = excluded.data, \
                     metadata = excluded.metadata, created_at = excluded.created_at"
                ),
                vec![
                    json!(input.source_uuid.to_string()),
                    json!(input.source_version),
                    json!(input.source_type),
                    json_text(&input.data),
                    json_text(&input.metadata),
                    json!(format_timestamp(self.clock.now())),
                ],
            )])
            .await
    }

    async fn load_latest(&self, source_uuid: Uuid, source_type: &str) -> Result<Option<Snapshot>> {
        let row = self
            .db
            .query_one(Statement::new(
                format!(
                    "SELECT {COLUMNS} FROM tarodomain_snapshots \
                     WHERE source_uuid = ? AND source_type = ? \
                     ORDER BY source_version DESC LIMIT 1"
                ),
                vec![json!(source_uuid.to_string()), json!(source_type)],
            ))
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
            .query_one(Statement::new(
                format!(
                    "SELECT {COLUMNS} FROM tarodomain_snapshots \
                     WHERE source_uuid = ? AND source_type = ? AND source_version = ?"
                ),
                vec![
                    json!(source_uuid.to_string()),
                    json!(source_type),
                    json!(version),
                ],
            ))
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
            .query(Statement::new(
                format!(
                    "SELECT {COLUMNS} FROM tarodomain_snapshots \
                     WHERE source_uuid = ? AND source_type = ? \
                     ORDER BY source_version DESC LIMIT ?"
                ),
                vec![
                    json!(source_uuid.to_string()),
                    json!(source_type),
                    json!(limit.map_or(-1, |n| i64::try_from(n).unwrap_or(i64::MAX))),
                ],
            ))
            .await?;
        rows.iter().map(parse).collect()
    }

    async fn delete_all(&self, source_uuid: Uuid, source_type: &str) -> Result<()> {
        self.db
            .write(vec![Statement::new(
                "DELETE FROM tarodomain_snapshots WHERE source_uuid = ? AND source_type = ?",
                vec![json!(source_uuid.to_string()), json!(source_type)],
            )])
            .await
    }

    async fn delete_before(
        &self,
        source_uuid: Uuid,
        source_type: &str,
        version: u64,
    ) -> Result<u64> {
        let params = vec![
            json!(source_uuid.to_string()),
            json!(source_type),
            json!(version),
        ];
        self.db
            .write_counting(
                Statement::new(
                    "DELETE FROM tarodomain_snapshots \
                     WHERE source_uuid = ? AND source_type = ? AND source_version < ?",
                    params.clone(),
                ),
                Statement::new(
                    "SELECT COUNT(*) AS n FROM tarodomain_snapshots \
                     WHERE source_uuid = ? AND source_type = ? AND source_version < ?",
                    params,
                ),
            )
            .await
    }

    async fn count(&self, source_uuid: Uuid, source_type: &str) -> Result<u64> {
        let row = self
            .db
            .query_one(Statement::new(
                "SELECT COUNT(*) AS n FROM tarodomain_snapshots \
                 WHERE source_uuid = ? AND source_type = ?",
                vec![json!(source_uuid.to_string()), json!(source_type)],
            ))
            .await?;
        Ok(row.map_or(0, |row| {
            u64::try_from(row.i64("n").unwrap_or(0)).unwrap_or(0)
        }))
    }
}
