//! Projections (spec §12): event handlers that build read models.
//!
//! A [`Projection`] declares which events it handles in
//! [`routes`](Projection::routes). A [`ProjectionHandler`] wraps it as an
//! [`EventHandler`](crate::bus::EventHandler) that skips events it has already
//! processed, so redelivery is harmless. A [`ShardProjection`] splits one
//! projection over several handlers by a shard key.

#[allow(clippy::module_inception)]
mod projection;
mod shard;

pub use projection::{Projection, ProjectionHandler, ProjectionRoutes, UnhandledHook};
pub use shard::{ShardProjection, fnv1a_32, shard_handlers};
