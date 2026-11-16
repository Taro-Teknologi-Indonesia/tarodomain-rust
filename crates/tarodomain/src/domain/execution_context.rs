//! The execution context (spec §5.5): the tracing IDs and metadata of the
//! command or event being handled.

use std::future::Future;

use super::{CommandEnvelope, EventEnvelope, Metadata};
use crate::kernel::context;
use crate::kernel::ids::Uuid;

/// The tracing IDs and metadata of the command or event being handled (spec
/// §5.5, §28).
///
/// The pipeline runs each handler inside [`scope`](Self::scope). Events and
/// commands created in that scope take their correlation ID, causation ID and
/// metadata from the context, which links them to their cause (§28.2).
///
/// The context lives in task-local storage ([`crate::kernel::context`]), so
/// concurrent requests never see each other's. A nested scope replaces it
/// until the nested future ends, and the outer context is back after that.
///
/// A future passed to `tokio::spawn`, or to a Worker's `ctx.wait_until`,
/// starts without a context. Hand it over explicitly:
///
/// ```
/// use tarodomain::domain::ExecutionContext;
/// use tarodomain::kernel::ids::new_v7;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() {
/// let ctx = ExecutionContext::new(new_v7());
/// ExecutionContext::scope(ctx.clone(), async {
///     let current = ExecutionContext::current().unwrap();
///     let task = tokio::spawn(ExecutionContext::scope(current, async {
///         ExecutionContext::current()
///     }));
///     assert_eq!(task.await.unwrap(), Some(ctx.clone()));
/// })
/// .await;
/// assert_eq!(ExecutionContext::current(), None);
/// # }
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionContext {
    /// End-to-end trace ID, shared by every message of one business
    /// transaction.
    pub correlation_id: Uuid,
    /// ID of the command or event being handled, or `None` at the root.
    pub causation_id: Option<Uuid>,
    /// While a command is handled, that command's own causation ID, such as
    /// the event a saga reacted to. Events raised in the context record it
    /// (see [`EventEnvelope::command_causation_id`]).
    pub command_causation_id: Option<Uuid>,
    /// Metadata, such as a user or tenant ID, that new messages inherit.
    pub metadata: Metadata,
}

impl ExecutionContext {
    /// A root context: `correlation_id`, with no causation and no metadata.
    ///
    /// For example, the context of an HTTP request whose `x-request-id` is
    /// `correlation_id`.
    pub fn new(correlation_id: Uuid) -> Self {
        Self {
            correlation_id,
            causation_id: None,
            command_causation_id: None,
            metadata: Metadata::new(),
        }
    }

    /// The context for handling a command: its correlation ID and metadata,
    /// with the command as the cause and the command's own cause as the
    /// command causation ID.
    pub fn for_command(command: &CommandEnvelope) -> Self {
        Self {
            correlation_id: command.correlation_id,
            causation_id: Some(command.id),
            command_causation_id: command.causation_id,
            metadata: command.metadata.clone(),
        }
    }

    /// The context for reacting to an event, as a saga does: its correlation
    /// ID and metadata, with the event as the cause.
    pub fn for_event(event: &EventEnvelope) -> Self {
        Self {
            correlation_id: event.correlation_id,
            causation_id: Some(event.id),
            command_causation_id: None,
            metadata: event.metadata.clone(),
        }
    }

    /// Runs `fut` with this context in scope, alongside the other values in
    /// task-local storage.
    pub fn scope<F: Future>(self, fut: F) -> impl Future<Output = F::Output> {
        context::scope(self, fut)
    }

    /// Runs `f` with this context in scope.
    pub fn sync_scope<R>(self, f: impl FnOnce() -> R) -> R {
        context::sync_scope(self, f)
    }

    /// The context in scope, or `None` outside any scope.
    pub fn current() -> Option<Self> {
        context::get::<Self>()
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;

    use super::*;
    use crate::kernel::ids::new_v7;

    fn at() -> chrono::DateTime<chrono::Utc> {
        DateTime::from_timestamp_millis(1_763_280_000_123).unwrap()
    }

    #[test]
    fn no_context_outside_a_scope() {
        assert_eq!(ExecutionContext::current(), None);
    }

    #[tokio::test]
    async fn nested_scope_restores_the_outer_context() {
        let outer = ExecutionContext::new(new_v7());
        let inner = ExecutionContext::new(new_v7());
        let (seen_inner, seen_outer) = ExecutionContext::scope(outer.clone(), async {
            let seen_inner =
                ExecutionContext::scope(inner.clone(), async { ExecutionContext::current() }).await;
            (seen_inner, ExecutionContext::current())
        })
        .await;
        assert_eq!(seen_inner, Some(inner));
        assert_eq!(seen_outer, Some(outer));
        assert_eq!(ExecutionContext::current(), None);
    }

    #[test]
    fn sync_scope_restores_the_outer_context() {
        let outer = ExecutionContext::new(new_v7());
        let inner = ExecutionContext::new(new_v7());
        let seen = outer.clone().sync_scope(|| {
            let nested = inner.clone().sync_scope(ExecutionContext::current);
            (nested, ExecutionContext::current())
        });
        assert_eq!(seen, (Some(inner), Some(outer)));
    }

    /// The propagation chain of spec §28.2: command → event → saga command →
    /// event, all on one correlation ID.
    #[test]
    fn propagation_chain() {
        let request = ExecutionContext {
            metadata: Metadata::from([("userId", "u-1")]),
            ..ExecutionContext::new(new_v7())
        };
        let cmd_1 = request.clone().sync_scope(|| CommandEnvelope::new(at()));
        assert_eq!(cmd_1.correlation_id, request.correlation_id);
        assert_eq!(cmd_1.causation_id, None);

        let evt_1 = ExecutionContext::for_command(&cmd_1).sync_scope(|| EventEnvelope::new(at()));
        assert_eq!(evt_1.causation_id, Some(cmd_1.id));
        assert_eq!(evt_1.command_causation_id, None, "cmd_1 is a root");

        let cmd_2 = ExecutionContext::for_event(&evt_1).sync_scope(|| CommandEnvelope::new(at()));
        assert_eq!(cmd_2.causation_id, Some(evt_1.id));

        let evt_2 = ExecutionContext::for_command(&cmd_2).sync_scope(|| EventEnvelope::new(at()));
        assert_eq!(evt_2.causation_id, Some(cmd_2.id));
        assert_eq!(evt_2.command_causation_id, Some(evt_1.id));

        for correlation_id in [
            cmd_1.correlation_id,
            evt_1.correlation_id,
            cmd_2.correlation_id,
            evt_2.correlation_id,
        ] {
            assert_eq!(correlation_id, request.correlation_id);
        }
        assert_eq!(evt_2.metadata, request.metadata, "metadata flows along");
    }
}
