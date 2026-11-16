//! The Sequencer: global append positions assigned after commit.
//!
//! A position handed out at insert time follows insert order, but readers see
//! events in commit order, so a reader could move its cursor past a position
//! whose event commits later (plan Risks: global ordering). Appends therefore
//! leave `position` NULL, and the Sequencer numbers events in a transaction of
//! its own:
//!
//! 1. read the `event-position` counter;
//! 2. take the unpositioned events it can see, in insert order (`seq`). A
//!    transaction sees only committed rows, so no event gets a position before
//!    it commits, and none that commits later gets a lower one;
//! 3. claim the counter with `UPDATE … WHERE value = <read value>`. A sequencer
//!    that loses the claim has been beaten by another one that is numbering the
//!    same events;
//! 4. number the events from the counter's old value.
//!
//! The counter never hands out a position twice, unlike `max(position) + 1`
//! after a stream is deleted.

use crate::db::{Db, Param};
use tarodomain::error::Result;

/// The counter row the Sequencer claims.
const COUNTER: &str = "event-position";

/// How many events one pass numbers at most.
const BATCH: usize = 500;

/// Assigns positions to committed events.
#[derive(Clone, Debug)]
pub struct Sequencer {
    db: Db,
}

/// What one pass did.
enum Pass {
    /// Numbered this many events.
    Numbered(u64),
    /// Nothing was waiting.
    Idle,
    /// Another sequencer claimed the counter first.
    Lost,
}

impl Sequencer {
    /// A sequencer over `db`.
    pub fn new(db: Db) -> Self {
        Self { db }
    }

    /// Numbers every committed, unpositioned event, retrying when another
    /// sequencer wins a claim. Returns how many events it numbered itself.
    pub async fn run(&self) -> Result<u64> {
        let mut total = 0;
        loop {
            match self.pass().await? {
                Pass::Numbered(n) => total += n,
                Pass::Idle => return Ok(total),
                Pass::Lost => tokio::task::yield_now().await,
            }
        }
    }

    /// Numbers what is waiting, but stops when another sequencer wins a claim:
    /// that one is numbering the same events. Reads call this before they
    /// look at positions. Returns how many events it numbered.
    pub async fn try_run(&self) -> Result<u64> {
        let mut total = 0;
        loop {
            match self.pass().await? {
                Pass::Numbered(n) => total += n,
                Pass::Idle | Pass::Lost => return Ok(total),
            }
        }
    }

    async fn pass(&self) -> Result<Pass> {
        let mut tx = self.db.begin_detached().await?;
        let result = self.number(&mut tx).await;
        match result {
            Ok(Pass::Numbered(n)) => {
                tx.commit().await?;
                Ok(Pass::Numbered(n))
            }
            Ok(other) => {
                tx.rollback().await?;
                Ok(other)
            }
            Err(error) => {
                // The error that matters is the original one.
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    async fn number(&self, tx: &mut crate::db::DetachedTx) -> Result<Pass> {
        tx.execute(
            "INSERT INTO tarodomain_counters (name, value) VALUES (?, 0) \
             ON CONFLICT (name) DO NOTHING",
            vec![Param::text(COUNTER)],
        )
        .await?;
        let value = tx
            .fetch(
                "SELECT value FROM tarodomain_counters WHERE name = ?",
                vec![Param::text(COUNTER)],
            )
            .await?
            .first()
            .map_or(Ok(0), |row| row.i64("value"))?;
        let waiting = tx
            .fetch(
                "SELECT seq FROM tarodomain_events WHERE position IS NULL ORDER BY seq LIMIT ?",
                vec![Param::from(BATCH as u64)],
            )
            .await?;
        if waiting.is_empty() {
            return Ok(Pass::Idle);
        }
        let count = waiting.len() as i64;
        let claimed = tx
            .execute(
                "UPDATE tarodomain_counters SET value = ? WHERE name = ? AND value = ?",
                vec![
                    Param::Int(value + count),
                    Param::text(COUNTER),
                    Param::Int(value),
                ],
            )
            .await?;
        if claimed == 0 {
            return Ok(Pass::Lost);
        }
        for (row, position) in waiting.iter().zip(value + 1..) {
            tx.execute(
                "UPDATE tarodomain_events SET position = ? WHERE seq = ? AND position IS NULL",
                vec![Param::Int(position), Param::Int(row.i64("seq")?)],
            )
            .await?;
        }
        Ok(Pass::Numbered(waiting.len() as u64))
    }
}
