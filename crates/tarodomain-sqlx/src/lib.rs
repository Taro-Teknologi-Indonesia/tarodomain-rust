//! TaroDomain on SQL: a provider over [sqlx](https://github.com/launchbadge/sqlx)
//! for SQLite and PostgreSQL, for servers that don't run on Cloudflare.
//!
//! The schema (spec §27.1) lives in `migrations/{sqlite,postgres}`, and
//! [`SQLITE_MIGRATIONS`] and [`POSTGRES_MIGRATIONS`] embed them for the
//! provider's `initialize()`.

pub mod db;
pub mod dead_letter;
pub mod event_bus;
pub mod event_store;
pub mod idempotency;
pub mod jobs;
pub mod provider;
pub mod saga_store;
pub mod sequencer;
pub mod snapshot_store;
pub mod unit_of_work;

/// The SQLite migrations, applied by `initialize()` (Step 11.5).
#[cfg(feature = "sqlite")]
pub static SQLITE_MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("migrations/sqlite");

/// The PostgreSQL migrations, applied by `initialize()` (Step 11.5).
#[cfg(feature = "postgres")]
pub static POSTGRES_MIGRATIONS: sqlx::migrate::Migrator = sqlx::migrate!("migrations/postgres");
