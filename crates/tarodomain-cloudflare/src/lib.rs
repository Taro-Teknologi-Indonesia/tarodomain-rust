//! TaroDomain on Cloudflare: the spec's CloudflareProvider (§24.2–24.3) and
//! CloudflareWorker (§26.1), built on [workers-rs](https://github.com/cloudflare/workers-rs).
//!
//! D1 stores everything, Queues deliver events in eventual mode, and Cron
//! Triggers run catch-up, saga timeouts and cleanup.
//!
//! Code that touches the `worker` crate sits behind
//! `cfg(target_arch = "wasm32")`, so the crate also builds natively and its
//! logic is covered by `cargo test --workspace`.

pub mod d1;
pub mod dead_letter;
pub mod entry;
pub mod event_store;
pub mod idempotency;
pub mod lease;
pub mod migrations;
pub mod platform;
pub mod provider;
pub mod queue_bus;
pub mod saga_store;
pub mod snapshot_store;
#[cfg(feature = "native-sqlite")]
pub mod sqlite_backend;
pub mod unit_of_work;
