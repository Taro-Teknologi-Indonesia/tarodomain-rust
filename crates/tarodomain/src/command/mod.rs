//! Commands (spec §4, §11, §14, §16): handlers, the registry that finds them
//! by command name, and the results of dispatching.
//!
//! A [`CommandHandler`] runs one type of command. The [`CommandRegistry`]
//! maps command names to handler factories, and gives each handler a
//! [`CommandContext`] with the repositories of the registered aggregates, on
//! the event bus of the pipeline that runs it.

mod concurrency;
mod dispatcher;
mod handler;
mod pipeline;
mod registry;
mod result;

pub use concurrency::{ConcurrencyChecker, RetryHook};
pub use dispatcher::CommandDispatcher;
pub use handler::{CommandContext, CommandHandler};
pub use pipeline::CommandPipeline;
pub use registry::{AnyCommandHandler, CommandRegistration, CommandRegistry, RepositoryRegistry};
pub use result::{BatchDispatchResult, CommandDispatchResult, CommandOutcome, DispatchStatus};
