//! Event sourcing (spec §3.3–3.4, §7): the event registry, which reads stored
//! events back as their Rust types and upgrades old schema versions on the
//! way, and the event store, which puts the adapter behind that boundary.

mod event_store;
mod registry;

pub use event_store::EventStore;
pub use registry::EventRegistry;
