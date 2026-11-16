//! The command dispatcher (spec §4.2, §11.3).

use std::sync::Arc;

use super::pipeline::CommandPipeline;
use super::registry::CommandRegistry;
use super::result::{BatchDispatchResult, CommandDispatchResult, CommandOutcome};
use crate::adapters::ConsistencyMode;
use crate::domain::AnyCommand;
use crate::error::Result;

/// Sends each command to the pipeline its consistency mode asks for (§11.3):
/// the mode its registration names, else the application's default.
///
/// Cloning is cheap.
///
/// A dispatcher of the strong pipeline and the eventual one:
///
/// ```text
/// let dispatcher = CommandDispatcher::new(registry, strong, eventual, ConsistencyMode::Strong);
/// let result = dispatcher.dispatch(OpenAccount { owner: "Ada".into() }).await?;
/// ```
#[derive(Clone, Debug)]
pub struct CommandDispatcher {
    registry: Arc<CommandRegistry>,
    strong: CommandPipeline,
    eventual: CommandPipeline,
    default_consistency: ConsistencyMode,
}

impl CommandDispatcher {
    /// A dispatcher over `registry`, with one pipeline per mode.
    ///
    /// Panics if `strong` or `eventual` runs in the other mode.
    pub fn new(
        registry: Arc<CommandRegistry>,
        strong: CommandPipeline,
        eventual: CommandPipeline,
        default_consistency: ConsistencyMode,
    ) -> Self {
        assert_eq!(strong.consistency(), ConsistencyMode::Strong);
        assert_eq!(eventual.consistency(), ConsistencyMode::Eventual);
        Self {
            registry,
            strong,
            eventual,
            default_consistency,
        }
    }

    /// The registry the dispatcher resolves commands in.
    pub fn registry(&self) -> &Arc<CommandRegistry> {
        &self.registry
    }

    /// Whether a handler is registered for the command type `name`.
    pub fn can_dispatch(&self, name: &str) -> bool {
        self.registry.contains(name)
    }

    /// Runs one command in its pipeline.
    ///
    /// Fails with `COMMAND_HANDLER_NOT_REGISTERED` if nothing handles it.
    /// Any command, or [`CommandMessage`](crate::domain::CommandMessage),
    /// converts into an [`AnyCommand`].
    pub async fn dispatch(&self, command: impl Into<AnyCommand>) -> Result<CommandDispatchResult> {
        let command = command.into();
        let registration = self.registry.resolve(command.name())?;
        let consistency = registration
            .consistency()
            .unwrap_or(self.default_consistency);
        let pipeline = match consistency {
            ConsistencyMode::Strong => &self.strong,
            ConsistencyMode::Eventual => &self.eventual,
        };
        pipeline.dispatch(registration, &command).await
    }

    /// Runs `commands` one after another and always returns a result.
    ///
    /// A command that fails is recorded in the result. With
    /// `continue_on_error` the rest still run; without it they are listed as
    /// skipped.
    pub async fn dispatch_all(
        &self,
        commands: impl IntoIterator<Item = AnyCommand>,
        continue_on_error: bool,
    ) -> BatchDispatchResult {
        let mut batch = BatchDispatchResult::default();
        let mut stopped = false;
        for command in commands {
            if stopped {
                batch.skipped.push(command.id());
                continue;
            }
            let (command_id, command_name) = (command.id(), command.name());
            let result = self.dispatch(command).await;
            stopped = result.is_err() && !continue_on_error;
            batch.outcomes.push(CommandOutcome {
                command_id,
                command_name,
                result,
            });
        }
        batch
    }
}
