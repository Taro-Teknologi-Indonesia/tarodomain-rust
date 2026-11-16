//! The storage every adapter of one local provider shares.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use tarodomain::kernel::clock::{Clock, SystemClock};

use crate::InMemoryUnitOfWork;

/// A type a [`LocalDatabase`] can keep a table of.
///
/// Tables are keyed by their type, so each one needs a type of its own: wrap
/// the rows in a newtype, such as `struct Balances(HashMap<Uuid, i64>)`,
/// rather than storing a bare `HashMap` another table might use too. A table
/// starts as its `Default`, and a unit of work copies it with `Clone`.
///
/// Implemented for every type that qualifies, so it is never written by hand.
pub trait Table: Any + Clone + Default + Send + Sync {}

impl<T: Any + Clone + Default + Send + Sync> Table for T {}

/// [`Table`] as a trait object, so that tables of different types can share a
/// map and still be copied.
trait AnyTable: Any + Send + Sync {
    fn clone_box(&self) -> Box<dyn AnyTable>;
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

impl<T: Table> AnyTable for T {
    fn clone_box(&self) -> Box<dyn AnyTable> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// Every table of a database, keyed by type. A clone is a deep copy.
#[derive(Default)]
pub(crate) struct Tables(HashMap<TypeId, Box<dyn AnyTable>>);

impl Tables {
    /// The table of type `T`, if it has been written.
    fn get<T: Table>(&self) -> Option<&T> {
        self.0.get(&TypeId::of::<T>())?.as_any().downcast_ref()
    }

    /// The table of type `T`, created empty if it has never been written.
    fn get_mut<T: Table>(&mut self) -> &mut T {
        self.0
            .entry(TypeId::of::<T>())
            .or_insert_with(|| Box::new(T::default()))
            .as_any_mut()
            .downcast_mut()
            .expect("a table is stored under its own type")
    }

    /// Calls `f` with the table of type `T`, an empty one if it has never
    /// been written.
    pub(crate) fn read<T: Table, R>(&self, f: impl FnOnce(&T) -> R) -> R {
        match self.get::<T>() {
            Some(table) => f(table),
            None => f(&T::default()),
        }
    }

    /// Calls `f` with the table of type `T` to change it.
    pub(crate) fn write<T: Table, R>(&mut self, f: impl FnOnce(&mut T) -> R) -> R {
        f(self.get_mut::<T>())
    }
}

impl Clone for Tables {
    fn clone(&self) -> Self {
        let tables = self.0.iter().map(|(&key, table)| (key, table.clone_box()));
        Self(tables.collect())
    }
}

/// The storage of one local provider: in-memory tables that every adapter of
/// the provider shares, plus the clock they tell time by.
///
/// A clone is another handle to the same storage, so a test can keep one to
/// build a second provider over the same data, or to look at a table.
///
/// # Tables
///
/// Each adapter keeps its rows in a [`Table`] of its own type, read with
/// [`read`](Self::read) and changed with [`write`](Self::write). An
/// application can keep its own in-memory read models here too, in tables of
/// its own types; they then share the provider's transactions.
///
/// # Locking
///
/// The tables sit behind one `std::sync::Mutex`, held only while a `read` or
/// `write` closure runs. The closures are synchronous, so the lock is never
/// held across an `.await`. Writes also take the database's turn, an async
/// lock, which serializes them: every write sees the one before it in full.
///
/// # Units of work
///
/// An [`InMemoryUnitOfWork`] holds the turn from `begin` until it commits or
/// rolls back, and works on its own copy of the tables. While the unit is
/// the task's active unit (inside
/// [`execute`](tarodomain::adapters::UnitOfWork#method.execute)), `read` and
/// `write` in that task use the copy, so the unit sees its own writes and
/// nothing else does until it commits. Elsewhere, reads see the committed
/// tables and writes wait for the unit to end, so a rollback never undoes
/// another writer's work.
///
/// So a task must not wait, inside a unit's work, for a write that runs
/// outside it: work handed to `tokio::spawn` doesn't inherit the unit, and
/// its writes would wait for the unit that is waiting for them.
#[derive(Clone)]
pub struct LocalDatabase {
    shared: Arc<Shared>,
}

struct Shared {
    tables: Mutex<Tables>,
    /// One writer at a time.
    turn: Arc<tokio::sync::Mutex<()>>,
    clock: Arc<dyn Clock>,
}

impl LocalDatabase {
    /// An empty database on the system clock.
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }

    /// An empty database that tells time by `clock`, such as a
    /// [`ManualClock`](tarodomain::kernel::clock::ManualClock) in a test.
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        let shared = Shared {
            tables: Mutex::default(),
            turn: Arc::default(),
            clock,
        };
        Self {
            shared: Arc::new(shared),
        }
    }

    /// The clock the database's adapters stamp rows with.
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.shared.clock
    }

    /// Calls `f` with the table of type `T`, an empty one if nothing has
    /// written it yet: the active unit's copy inside a unit of work over this
    /// database, else the committed table.
    pub fn read<T: Table, R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let read = |tables: &mut Tables| tables.read(f);
        match InMemoryUnitOfWork::with_active(self, read) {
            Ok(result) => result,
            Err(read) => read(&mut self.tables()),
        }
    }

    /// Calls `f` with the table of type `T` to change it.
    ///
    /// Inside a unit of work over this database, `f` changes the unit's copy
    /// at once. Elsewhere it changes the committed table, once every earlier
    /// write has finished and no unit holds the database.
    ///
    /// `f` runs under the lock, so a check and the change it allows happen
    /// atomically: return an error from `f` to change nothing.
    pub async fn write<T: Table, R>(&self, f: impl FnOnce(&mut T) -> R + Send) -> R {
        let write = |tables: &mut Tables| tables.write(f);
        match InMemoryUnitOfWork::with_active(self, write) {
            Ok(result) => result,
            Err(write) => {
                let _turn = self.shared.turn.lock().await;
                write(&mut self.tables())
            }
        }
    }

    /// Whether `self` and `other` are handles to the same storage.
    pub fn ptr_eq(&self, other: &LocalDatabase) -> bool {
        Arc::ptr_eq(&self.shared, &other.shared)
    }

    /// The lock a writer holds: a plain write for its closure, a unit of
    /// work from `begin` to its end.
    pub(crate) fn turn(&self) -> Arc<tokio::sync::Mutex<()>> {
        Arc::clone(&self.shared.turn)
    }

    /// The committed tables.
    pub(crate) fn tables(&self) -> MutexGuard<'_, Tables> {
        // A closure that panicked may have left its table half-changed, but
        // the map itself is intact, so the next caller can go on.
        let tables = self.shared.tables.lock();
        tables.unwrap_or_else(PoisonError::into_inner)
    }
}

impl Default for LocalDatabase {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for LocalDatabase {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LocalDatabase")
            .field("tables", &self.tables().0.len())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use tarodomain::kernel::clock::ManualClock;
    use tokio::task::yield_now;

    use super::*;

    #[derive(Clone, Debug, Default, PartialEq)]
    struct Names(Vec<String>);

    #[derive(Clone, Debug, Default, PartialEq)]
    struct Counts(BTreeMap<String, u32>);

    #[tokio::test]
    async fn a_table_starts_empty_and_keeps_what_is_written() {
        let db = LocalDatabase::new();
        assert_eq!(db.read(|names: &Names| names.clone()), Names::default());

        db.write(|names: &mut Names| names.0.push("a".into())).await;
        db.write(|names: &mut Names| names.0.push("b".into())).await;
        assert_eq!(db.read(|names: &Names| names.0.join(",")), "a,b");
    }

    #[tokio::test]
    async fn tables_are_keyed_by_type() {
        let db = LocalDatabase::new();
        db.write(|names: &mut Names| names.0.push("a".into())).await;
        db.write(|counts: &mut Counts| counts.0.insert("a".into(), 1))
            .await;
        assert_eq!(db.read(|names: &Names| names.0.len()), 1);
        assert_eq!(db.read(|counts: &Counts| counts.0["a"]), 1);
        assert_eq!(format!("{db:?}"), "LocalDatabase { tables: 2, .. }");
    }

    #[tokio::test]
    async fn a_write_returns_the_closure_result() {
        let db = LocalDatabase::new();
        let previous = db
            .write(|counts: &mut Counts| counts.0.insert("a".into(), 1))
            .await;
        assert_eq!(previous, None);
        let rejected: Result<(), &str> = db
            .write(|counts: &mut Counts| {
                if counts.0.contains_key("a") {
                    return Err("taken");
                }
                counts.0.insert("a".into(), 2);
                Ok(())
            })
            .await;
        assert_eq!(rejected, Err("taken"));
        assert_eq!(db.read(|counts: &Counts| counts.0["a"]), 1);
    }

    #[tokio::test]
    async fn clones_share_the_storage_and_new_databases_do_not() {
        let db = LocalDatabase::new();
        let clone = db.clone();
        let other = LocalDatabase::default();
        clone
            .write(|names: &mut Names| names.0.push("a".into()))
            .await;
        assert_eq!(db.read(|names: &Names| names.0.len()), 1);
        assert_eq!(other.read(|names: &Names| names.0.len()), 0);
        assert!(db.ptr_eq(&clone));
        assert!(!db.ptr_eq(&other));
    }

    #[test]
    fn the_clock_is_injectable() {
        let start = "2026-11-16T05:58:30.206Z".parse().unwrap();
        let db = LocalDatabase::with_clock(Arc::new(ManualClock::new(start)));
        assert_eq!(db.clock().now(), start);
    }

    #[tokio::test]
    async fn a_write_waits_for_the_turn() {
        let db = LocalDatabase::new();
        let turn = Arc::clone(&db.shared.turn).lock_owned().await;
        let write = db.write(|names: &mut Names| names.0.push("a".into()));
        tokio::pin!(write);
        tokio::select! {
            biased;
            () = &mut write => panic!("the write ran while another held the turn"),
            () = yield_now() => {}
        }
        assert_eq!(db.read(|names: &Names| names.0.len()), 0);
        drop(turn);
        write.await;
        assert_eq!(db.read(|names: &Names| names.0.len()), 1);
    }

    #[tokio::test]
    async fn a_copy_of_the_tables_is_independent() {
        let mut tables = Tables::default();
        tables.write(|names: &mut Names| names.0.push("a".into()));
        let copy = tables.clone();
        tables.write(|names: &mut Names| names.0.push("b".into()));
        assert_eq!(copy.read(|names: &Names| names.0.len()), 1);
        assert_eq!(tables.read(|names: &Names| names.0.len()), 2);
    }
}
