//! Rebuilding read models and saga state from the event store (spec §20).
//!
//! [`ProjectionRebuilder`] replays the log into a projection;
//! [`SagaRebuilder`] reconstructs saga state, without running the saga's
//! commands again.

mod projection_rebuilder;
mod saga_rebuilder;

pub use projection_rebuilder::{
    ProjectionRebuilder, RebuildAllResult, RebuildOptions, RebuildResult, RebuildTarget,
    SkippedEvent,
};
pub use saga_rebuilder::{
    SagaRebuildAllResult, SagaRebuildOptions, SagaRebuildResult, SagaRebuildTarget, SagaRebuilder,
    SagaType,
};
