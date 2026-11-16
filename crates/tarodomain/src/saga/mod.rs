//! Process managers, or sagas (spec §13, §17, §19): long-running workflows
//! that react to events by dispatching commands, and undo their steps when
//! something fails.
//!
//! A saga's decisions are pure and synchronous: its handlers take the event
//! and return commands. [`SagaEventHandler`] does the I/O around them: it
//! loads the saga, dispatches the commands, saves the state and records the
//! event as processed.

mod compensation;
mod event_handler;
mod process_manager;
mod state_machine;
mod timeout;

pub use compensation::{CompensationDefs, CompensationStack};
pub use event_handler::{SagaCommandDispatch, SagaEventHandler};
pub use process_manager::{ProcessManager, SagaCtx, SagaInstance, SagaRoutes};
pub use state_machine::{HandlerKind, SagaState, transition};
pub use timeout::{SagaTimeout, SagaTimeoutScheduler, TimeoutRun};
