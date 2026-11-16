//! What dispatching reports: one result per command, and one per batch.

use crate::domain::DomainEvent;
use crate::error::{FrameworkError, Result};
use crate::kernel::ids::Uuid;

/// How a dispatched command ended, when it didn't fail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DispatchStatus {
    /// The handler ran and the command is now marked processed.
    Dispatched,
    /// A command with this ID was processed before (§14.1), so the handler
    /// didn't run.
    Duplicate,
}

/// The result of dispatching one command (spec §4.2).
#[derive(Clone, Debug)]
pub struct CommandDispatchResult {
    /// The command's ID.
    pub command_id: Uuid,
    /// Whether the handler ran or the command was a duplicate.
    pub status: DispatchStatus,
    /// The events the handler's repositories saved, in order. Empty for a
    /// duplicate.
    pub events: Vec<DomainEvent>,
}

impl CommandDispatchResult {
    /// A command whose handler ran and saved `events`.
    pub fn dispatched(command_id: Uuid, events: Vec<DomainEvent>) -> Self {
        Self {
            command_id,
            status: DispatchStatus::Dispatched,
            events,
        }
    }

    /// A command that was processed before.
    pub fn duplicate(command_id: Uuid) -> Self {
        Self {
            command_id,
            status: DispatchStatus::Duplicate,
            events: Vec::new(),
        }
    }

    /// Whether the command was skipped as a duplicate.
    pub fn is_duplicate(&self) -> bool {
        self.status == DispatchStatus::Duplicate
    }
}

/// How one command of a batch ended.
#[derive(Debug)]
pub struct CommandOutcome {
    /// The command's ID.
    pub command_id: Uuid,
    /// The command's name.
    pub command_name: &'static str,
    /// The dispatch result, or the error that stopped it.
    pub result: Result<CommandDispatchResult>,
}

impl CommandOutcome {
    /// The error, if the command failed.
    pub fn error(&self) -> Option<&FrameworkError> {
        self.result.as_ref().err()
    }
}

/// The result of dispatching several commands in order (spec §4.2).
///
/// A batch always produces a result: a failing command is recorded in
/// [`outcomes`](Self::outcomes), not returned as an error.
#[derive(Debug, Default)]
pub struct BatchDispatchResult {
    /// One outcome for each command that was attempted, in order.
    pub outcomes: Vec<CommandOutcome>,
    /// IDs of commands that were never attempted, because an earlier one
    /// failed and the batch wasn't asked to continue on error.
    pub skipped: Vec<Uuid>,
}

impl BatchDispatchResult {
    /// Whether every command was attempted and none failed.
    pub fn is_success(&self) -> bool {
        self.skipped.is_empty() && self.outcomes.iter().all(|outcome| outcome.result.is_ok())
    }

    /// The outcomes of the commands that failed.
    pub fn failures(&self) -> impl Iterator<Item = &CommandOutcome> {
        self.outcomes
            .iter()
            .filter(|outcome| outcome.result.is_err())
    }

    /// The results of the commands that succeeded, duplicates included.
    pub fn successes(&self) -> impl Iterator<Item = &CommandDispatchResult> {
        self.outcomes
            .iter()
            .filter_map(|outcome| outcome.result.as_ref().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kernel::ids::new_v7;

    fn outcome(result: Result<CommandDispatchResult>) -> CommandOutcome {
        CommandOutcome {
            command_id: new_v7(),
            command_name: "Deposit",
            result,
        }
    }

    #[test]
    fn constructors_set_the_status() {
        let id = new_v7();
        let dispatched = CommandDispatchResult::dispatched(id, Vec::new());
        assert_eq!(dispatched.status, DispatchStatus::Dispatched);
        assert!(!dispatched.is_duplicate());
        let duplicate = CommandDispatchResult::duplicate(id);
        assert!(duplicate.is_duplicate());
        assert!(duplicate.events.is_empty());
        assert_eq!(duplicate.command_id, id);
    }

    #[test]
    fn a_batch_is_a_success_only_if_nothing_failed_or_was_skipped() {
        let ok = || outcome(Ok(CommandDispatchResult::duplicate(new_v7())));
        let failed = || outcome(Err(FrameworkError::command_validation("no")));

        let all_ok = BatchDispatchResult {
            outcomes: vec![ok(), ok()],
            skipped: Vec::new(),
        };
        assert!(all_ok.is_success());
        assert_eq!(all_ok.successes().count(), 2);

        let one_failed = BatchDispatchResult {
            outcomes: vec![ok(), failed()],
            skipped: Vec::new(),
        };
        assert!(!one_failed.is_success());
        assert_eq!(one_failed.failures().count(), 1);
        assert_eq!(one_failed.successes().count(), 1);
        let error = one_failed.failures().next().unwrap().error().unwrap();
        assert_eq!(error.code().as_str(), "COMMAND_VALIDATION");

        let skipped = BatchDispatchResult {
            outcomes: vec![failed()],
            skipped: vec![new_v7()],
        };
        assert!(!skipped.is_success());
        assert!(BatchDispatchResult::default().is_success());
    }
}
