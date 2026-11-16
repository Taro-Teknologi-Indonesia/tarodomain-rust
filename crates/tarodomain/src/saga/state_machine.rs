//! The saga state machine (spec §13.2).

use std::fmt;

use crate::adapters::SagaStatus;
use crate::error::{FrameworkError, Result};

/// Where a saga is, including `Initial` before its first event.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SagaState {
    /// Not started: no `starts_when` event has arrived. Never stored.
    Initial,
    /// Started and moving forward.
    Running,
    /// Undoing its steps.
    Compensating,
    /// Ended successfully. Terminal.
    Completed,
    /// Ended unsuccessfully. Terminal.
    Failed,
}

impl SagaState {
    /// The stored status, or `None` for `Initial`.
    pub fn status(self) -> Option<SagaStatus> {
        match self {
            SagaState::Initial => None,
            SagaState::Running => Some(SagaStatus::Running),
            SagaState::Compensating => Some(SagaStatus::Compensating),
            SagaState::Completed => Some(SagaStatus::Completed),
            SagaState::Failed => Some(SagaStatus::Failed),
        }
    }

    /// Whether the saga has ended.
    pub fn is_terminal(self) -> bool {
        matches!(self, SagaState::Completed | SagaState::Failed)
    }
}

impl From<SagaStatus> for SagaState {
    fn from(status: SagaStatus) -> Self {
        match status {
            SagaStatus::Running => SagaState::Running,
            SagaStatus::Compensating => SagaState::Compensating,
            SagaStatus::Completed => SagaState::Completed,
            SagaStatus::Failed => SagaState::Failed,
        }
    }
}

/// The four kinds of saga handler (spec §13.1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum HandlerKind {
    /// `starts_when`: starts the saga.
    StartsWhen,
    /// `continue_when`: an intermediate step.
    ContinueWhen,
    /// `compensate_when`: begins or continues compensation.
    CompensateWhen,
    /// `completed_when`: completes the saga.
    CompletedWhen,
}

impl HandlerKind {
    /// The prefix of the saga's `current_step`: `startsWhen`, `continueWhen`,
    /// `compensateWhen` or `completedWhen`.
    pub fn step_prefix(self) -> &'static str {
        match self {
            HandlerKind::StartsWhen => "startsWhen",
            HandlerKind::ContinueWhen => "continueWhen",
            HandlerKind::CompensateWhen => "compensateWhen",
            HandlerKind::CompletedWhen => "completedWhen",
        }
    }
}

impl fmt::Display for HandlerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.step_prefix())
    }
}

/// The state after a handler of `kind` runs in `state` (spec §13.2), or the
/// error the table prescribes:
///
/// | State | `starts_when` | `continue_when` | `compensate_when` | `completed_when` |
/// |---|---|---|---|---|
/// | Initial | Running | `SAGA_NOT_STARTED` | `SAGA_NOT_STARTED` | `SAGA_NOT_STARTED` |
/// | Running | `SAGA_ALREADY_STARTED` | Running | Compensating | Completed |
/// | Compensating | `INVALID_SAGA_TRANSITION` | Compensating | Compensating | Completed |
/// | Completed, Failed | `SAGA_ALREADY_COMPLETED` | same | same | same |
///
/// `Compensating` with `starts_when` isn't in the spec's table; it is
/// invalid, because a saga that is undoing its work can't start again.
pub fn transition(state: SagaState, kind: HandlerKind) -> Result<SagaState> {
    use HandlerKind::*;
    use SagaState::*;
    let context = |e: FrameworkError| {
        e.with_context("state", format!("{state:?}"))
            .with_context("handler", kind.step_prefix())
    };
    match (state, kind) {
        (Initial, StartsWhen) => Ok(Running),
        (Initial, _) => Err(context(FrameworkError::saga_not_started(format!(
            "a saga can't handle {kind} before it has started"
        )))),
        (Running, ContinueWhen) => Ok(Running),
        (Running, CompensateWhen) => Ok(Compensating),
        (Running, CompletedWhen) => Ok(Completed),
        (Running, StartsWhen) => Err(context(FrameworkError::saga_already_started(
            "the saga has already started",
        ))),
        (Compensating, ContinueWhen | CompensateWhen) => Ok(Compensating),
        (Compensating, CompletedWhen) => Ok(Completed),
        (Compensating, StartsWhen) => Err(context(FrameworkError::invalid_saga_transition(
            "a compensating saga can't start again",
        ))),
        (Completed | Failed, _) => Err(context(FrameworkError::saga_already_completed(
            "the saga has already ended",
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::HandlerKind::*;
    use super::SagaState::*;
    use super::*;

    #[test]
    fn every_cell_of_the_table() {
        let ok = |s, k, to| assert_eq!(transition(s, k).unwrap(), to, "{s:?} + {k}");
        let err = |s, k, code: &str| {
            let e = transition(s, k).unwrap_err();
            assert_eq!(e.code().as_str(), code, "{s:?} + {k}");
        };
        ok(Initial, StartsWhen, Running);
        for k in [ContinueWhen, CompensateWhen, CompletedWhen] {
            err(Initial, k, "SAGA_NOT_STARTED");
        }
        ok(Running, ContinueWhen, Running);
        ok(Running, CompensateWhen, Compensating);
        ok(Running, CompletedWhen, Completed);
        err(Running, StartsWhen, "SAGA_ALREADY_STARTED");
        ok(Compensating, ContinueWhen, Compensating);
        ok(Compensating, CompensateWhen, Compensating);
        ok(Compensating, CompletedWhen, Completed);
        err(Compensating, StartsWhen, "INVALID_SAGA_TRANSITION");
        for s in [Completed, Failed] {
            for k in [StartsWhen, ContinueWhen, CompensateWhen, CompletedWhen] {
                err(s, k, "SAGA_ALREADY_COMPLETED");
            }
        }
    }

    #[test]
    fn status_round_trips() {
        for status in SagaStatus::ALL {
            assert_eq!(SagaState::from(status).status(), Some(status));
        }
        assert_eq!(Initial.status(), None);
        assert!(Completed.is_terminal() && Failed.is_terminal() && !Running.is_terminal());
    }
}
