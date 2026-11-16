//! Startup and runtime validation (spec §29).
//!
//! [`FrameworkValidator`] checks the whole configuration when the framework
//! is built and reports every problem at once. [`DispatchGuard`] checks
//! single commands and events while the framework runs.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::command::CommandRegistry;
use crate::domain::DomainEvent;
use crate::error::{FrameworkError, Result};
use crate::event_sourcing::EventRegistry;

/// How an aggregate is stored.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AggregateKind {
    /// As its events (§6.2).
    EventSourced,
    /// As a row in the application's own table (§6.3).
    State {
        /// Whether a state repository was supplied.
        has_repository: bool,
    },
}

/// What validation needs to know about a registered aggregate.
#[derive(Clone, Debug)]
pub struct AggregateInfo {
    /// The aggregate type name.
    pub name: &'static str,
    /// How it is stored.
    pub kind: AggregateKind,
}

/// What validation needs to know about a registered projection.
#[derive(Clone, Debug)]
pub struct ProjectionInfo {
    /// The projection's name.
    pub name: String,
    /// The event types it routes.
    pub events: Vec<&'static str>,
}

/// What validation needs to know about a registered saga.
#[derive(Clone, Debug)]
pub struct SagaInfo {
    /// The saga type name.
    pub saga_type: &'static str,
    /// Every event type it has a handler for.
    pub events: Vec<&'static str>,
    /// How many `starts_when` handlers it has.
    pub starts: usize,
    /// How many `completed_when` handlers it has.
    pub completions: usize,
    /// Whether it handles `SagaTimeout`.
    pub handles_timeout: bool,
    /// Its timeout, if one is set.
    pub timeout: Option<Duration>,
}

/// The configuration [`FrameworkValidator`] checks. Registrations are lists,
/// not maps, so that a duplicate can be seen.
#[derive(Clone, Debug, Default)]
pub struct ValidationInput {
    /// Registered aggregates.
    pub aggregates: Vec<AggregateInfo>,
    /// The names of registered command types.
    pub commands: Vec<&'static str>,
    /// Registered projections.
    pub projections: Vec<ProjectionInfo>,
    /// Registered sagas.
    pub sagas: Vec<SagaInfo>,
    /// The names of registered event types, one entry per registration.
    pub events: Vec<&'static str>,
}

/// What kind of non-blocking problem a [`Warning`] reports.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WarningKind {
    /// A saga has no `completed_when` handler, so it never ends by itself.
    SagaNeverCompletes,
    /// A saga handles `SagaTimeout` but has no timeout, so it never fires.
    TimeoutWithoutTimeout,
    /// `snapshot_every` was set on a state-stored aggregate, which has no
    /// snapshots.
    SnapshotOnStateAggregate,
    /// A projection was given an event it has no handler for.
    UnhandledProjectionEvent,
    /// A repository recovered from an error (a failed snapshot or publish).
    Recovered,
}

/// A non-blocking problem.
#[derive(Clone, Debug)]
pub struct Warning {
    /// What kind of problem it is.
    pub kind: WarningKind,
    /// A description for people.
    pub message: String,
}

impl Warning {
    pub(crate) fn new(kind: WarningKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }
}

impl fmt::Display for Warning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

/// Receives warnings.
pub type WarningHook = Arc<dyn Fn(&Warning) + Send + Sync>;

/// The default [`WarningHook`]: logs through `tracing` at `warn` level.
pub fn default_warning_hook() -> WarningHook {
    Arc::new(|warning| tracing::warn!(kind = ?warning.kind, "{}", warning.message))
}

/// What [`FrameworkValidator::validate`] found.
#[derive(Clone, Debug, Default)]
pub struct ValidationReport {
    /// Problems that stop the build.
    pub errors: Vec<String>,
    /// Problems that don't.
    pub warnings: Vec<Warning>,
}

impl ValidationReport {
    /// `Ok` if there are no errors, else one `INVALID_CONFIGURATION` that
    /// lists every error, with the list also under the `errors` context key.
    /// The warnings go to `hook` either way.
    pub fn finish(self, hook: &WarningHook) -> Result<()> {
        for warning in &self.warnings {
            hook(warning);
        }
        if self.errors.is_empty() {
            return Ok(());
        }
        let message = format!(
            "invalid configuration, {} problem(s):\n- {}",
            self.errors.len(),
            self.errors.join("\n- ")
        );
        Err(FrameworkError::invalid_configuration(message)
            .with_context("errors", serde_json::Value::from(self.errors.clone())))
    }
}

/// Checks a configuration at build time (§29.1).
///
/// **Errors:** a duplicate aggregate, command, saga, projection or event; a
/// state-stored aggregate without a repository; a saga without `starts_when`;
/// a projection that routes no event; a zero saga timeout; and a projection
/// or saga that handles an event type nobody registered (`SagaTimeout`, which
/// the framework raises itself, is exempt).
///
/// **Warnings:** a saga that never completes, a `SagaTimeout` handler on a
/// saga without a timeout.
pub struct FrameworkValidator;

impl FrameworkValidator {
    /// Checks `input` and collects everything wrong with it.
    pub fn validate(input: &ValidationInput) -> ValidationReport {
        let mut report = ValidationReport::default();
        let errors = &mut report.errors;

        duplicates(
            errors,
            "aggregate",
            input.aggregates.iter().map(|a| a.name.to_owned()),
        );
        duplicates(
            errors,
            "command",
            input.commands.iter().map(|c| (*c).to_owned()),
        );
        duplicates(
            errors,
            "projection",
            input.projections.iter().map(|p| p.name.clone()),
        );
        duplicates(
            errors,
            "saga",
            input.sagas.iter().map(|s| s.saga_type.to_owned()),
        );
        duplicates(
            errors,
            "event",
            input.events.iter().map(|e| (*e).to_owned()),
        );

        for aggregate in &input.aggregates {
            if aggregate.kind
                == (AggregateKind::State {
                    has_repository: false,
                })
            {
                errors.push(format!(
                    "aggregate {} is state-stored but has no state repository",
                    aggregate.name
                ));
            }
        }

        let known: BTreeSet<&str> = input.events.iter().copied().collect();
        let unknown = |events: &[&'static str]| -> Vec<&'static str> {
            events
                .iter()
                .copied()
                .filter(|e| {
                    *e != <crate::saga::SagaTimeout as crate::domain::Event>::NAME
                        && !known.contains(e)
                })
                .collect()
        };
        for projection in &input.projections {
            if projection.events.is_empty() {
                errors.push(format!(
                    "projection {} doesn't handle any event",
                    projection.name
                ));
            }
            for event in unknown(&projection.events) {
                errors.push(format!(
                    "projection {} handles event {event}, which isn't registered",
                    projection.name
                ));
            }
        }
        for saga in &input.sagas {
            if saga.starts == 0 {
                errors.push(format!(
                    "saga {} has no starts_when handler, so it can never start",
                    saga.saga_type
                ));
            }
            if saga.timeout == Some(Duration::ZERO) {
                errors.push(format!(
                    "saga {} has a timeout of zero; it must be positive",
                    saga.saga_type
                ));
            }
            for event in unknown(&saga.events) {
                errors.push(format!(
                    "saga {} handles event {event}, which isn't registered",
                    saga.saga_type
                ));
            }
            if saga.completions == 0 {
                report.warnings.push(Warning::new(
                    WarningKind::SagaNeverCompletes,
                    format!(
                        "saga {} has no completed_when handler and never completes by itself",
                        saga.saga_type
                    ),
                ));
            }
            if saga.handles_timeout && saga.timeout.is_none() {
                report.warnings.push(Warning::new(
                    WarningKind::TimeoutWithoutTimeout,
                    format!(
                        "saga {} handles SagaTimeout but has no timeout, so the handler never runs",
                        saga.saga_type
                    ),
                ));
            }
        }
        report
    }
}

fn duplicates(errors: &mut Vec<String>, what: &str, names: impl Iterator<Item = String>) {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for name in names {
        *counts.entry(name).or_default() += 1;
    }
    for (name, count) in counts {
        if count > 1 {
            errors.push(format!("{what} {name} is registered {count} times"));
        }
    }
}

/// What the guard does when a projection is given an event it has no
/// handler for (§29.2).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProjectionMode {
    /// Ignore it. The default: a projection normally sees every event.
    #[default]
    Skip,
    /// Report a warning and carry on.
    Warn,
    /// Fail the delivery with `MISSING_EVENT_HANDLER`.
    Strict,
}

/// Checks commands, events and projection deliveries as they happen (§29.2).
#[derive(Clone)]
pub struct DispatchGuard {
    commands: Arc<CommandRegistry>,
    events: Arc<EventRegistry>,
    mode: ProjectionMode,
    warn: WarningHook,
}

impl DispatchGuard {
    /// A guard over the given registries.
    pub fn new(
        commands: Arc<CommandRegistry>,
        events: Arc<EventRegistry>,
        mode: ProjectionMode,
        warn: WarningHook,
    ) -> Self {
        Self {
            commands,
            events,
            mode,
            warn,
        }
    }

    /// The projection mode.
    pub fn projection_mode(&self) -> ProjectionMode {
        self.mode
    }

    /// Fails with `COMMAND_HANDLER_NOT_REGISTERED` if nothing handles `name`.
    pub fn validate_command(&self, name: &str) -> Result<()> {
        self.commands.resolve(name).map(|_| ())
    }

    /// Fails with `UNKNOWN_EVENT_TYPE` if `event`'s type isn't registered, or
    /// `EVENT_VERSION_MISMATCH` if it is registered at another version.
    pub fn validate_event(&self, event: &DomainEvent) -> Result<()> {
        let (name, version) = (event.name(), event.version());
        match self.events.registered_version(name) {
            Some(registered) if registered == version => Ok(()),
            Some(registered) => Err(FrameworkError::event_version_mismatch(format!(
                "event {name} is version {version}, but version {registered} is registered"
            ))
            .with_context("eventType", name)),
            None => Err(FrameworkError::unknown_event_type(format!(
                "event type {name} isn't registered"
            ))
            .with_context("eventType", name)),
        }
    }

    /// Applies the projection mode to a delivery of `event` to the
    /// projection `projection`, which handles `handled`.
    pub fn validate_projection_dispatch(
        &self,
        projection: &str,
        handled: &[&'static str],
        event: &DomainEvent,
    ) -> Result<()> {
        if self.mode == ProjectionMode::Skip || handled.contains(&event.name()) {
            return Ok(());
        }
        let message = format!(
            "projection {projection} has no handler for event {}",
            event.name()
        );
        match self.mode {
            ProjectionMode::Warn => {
                (self.warn)(&Warning::new(
                    WarningKind::UnhandledProjectionEvent,
                    message,
                ));
                Ok(())
            }
            _ => Err(FrameworkError::missing_event_handler(message)
                .with_context("projection", projection)
                .with_context("eventType", event.name())),
        }
    }
}

impl fmt::Debug for DispatchGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DispatchGuard")
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::domain::{Event, EventEnvelope};
    use crate::error::ErrorCode;
    use crate::kernel::clock::now;

    #[derive(Debug, Serialize, Deserialize)]
    struct Opened;
    impl Event for Opened {
        const NAME: &'static str = "Opened";
    }

    #[derive(Debug, Serialize, Deserialize)]
    struct Upgraded;
    impl Event for Upgraded {
        const NAME: &'static str = "Upgraded";
        const VERSION: &'static str = "2.0.0";
    }

    fn saga(name: &'static str) -> SagaInfo {
        SagaInfo {
            saga_type: name,
            events: vec!["Opened"],
            starts: 1,
            completions: 1,
            handles_timeout: false,
            timeout: None,
        }
    }

    fn valid() -> ValidationInput {
        ValidationInput {
            aggregates: vec![AggregateInfo {
                name: "Account",
                kind: AggregateKind::EventSourced,
            }],
            commands: vec!["Open"],
            projections: vec![ProjectionInfo {
                name: "balance".into(),
                events: vec!["Opened"],
            }],
            sagas: vec![saga("Transfer")],
            events: vec!["Opened"],
        }
    }

    fn errors(input: &ValidationInput) -> Vec<String> {
        FrameworkValidator::validate(input).errors
    }

    #[test]
    fn a_valid_configuration_has_no_errors_or_warnings() {
        let report = FrameworkValidator::validate(&valid());
        assert!(report.errors.is_empty() && report.warnings.is_empty());
    }

    #[test]
    fn every_error_is_reported_at_once() {
        let mut input = valid();
        input.aggregates.push(input.aggregates[0].clone());
        input.aggregates.push(AggregateInfo {
            name: "Ledger",
            kind: AggregateKind::State {
                has_repository: false,
            },
        });
        input.commands.push("Open");
        input.projections.push(ProjectionInfo {
            name: "empty".into(),
            events: vec![],
        });
        input.projections.push(input.projections[0].clone());
        input.projections[0].events.push("Ghost");
        let mut broken = saga("Transfer");
        broken.starts = 0;
        broken.timeout = Some(Duration::ZERO);
        input.sagas.push(broken);
        input.events.push("Opened");

        let found = errors(&input);
        assert_eq!(found.len(), 10, "{found:#?}");
        let error = FrameworkValidator::validate(&input)
            .finish(&default_warning_hook())
            .unwrap_err();
        assert_eq!(*error.code(), ErrorCode::InvalidConfiguration);
        assert!(error.message().contains("10 problem(s)"));
        assert_eq!(error.context()["errors"].as_array().unwrap().len(), 10);
    }

    #[test]
    fn each_rule_has_its_own_error() {
        let mut input = valid();
        input.projections[0].events = vec!["Ghost"];
        assert!(errors(&input)[0].contains("Ghost"));

        let mut input = valid();
        input.sagas[0].starts = 0;
        assert!(errors(&input)[0].contains("starts_when"));

        let mut input = valid();
        input.sagas[0].events.push("Missing");
        assert!(errors(&input)[0].contains("Missing"));
    }

    #[test]
    fn the_saga_timeout_event_needs_no_registration() {
        let mut input = valid();
        input.sagas[0].events.push("SagaTimeout");
        assert!(errors(&input).is_empty());
    }

    #[test]
    fn warnings_do_not_block_and_go_to_the_hook() {
        let mut input = valid();
        input.sagas[0].completions = 0;
        input.sagas[0].handles_timeout = true;
        let report = FrameworkValidator::validate(&input);
        let kinds: Vec<_> = report.warnings.iter().map(|w| w.kind).collect();
        assert_eq!(
            kinds,
            [
                WarningKind::SagaNeverCompletes,
                WarningKind::TimeoutWithoutTimeout
            ]
        );
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = seen.clone();
        let hook: WarningHook = Arc::new(move |w| sink.lock().unwrap().push(w.kind));
        report.finish(&hook).unwrap();
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    fn guard(mode: ProjectionMode) -> (DispatchGuard, Arc<Mutex<usize>>) {
        let mut events = EventRegistry::new();
        events.register::<Opened>().unwrap();
        events.register::<Upgraded>().unwrap();
        let warned = Arc::new(Mutex::new(0));
        let sink = warned.clone();
        let hook: WarningHook = Arc::new(move |_| *sink.lock().unwrap() += 1);
        let guard = DispatchGuard::new(
            Arc::new(CommandRegistry::new()),
            Arc::new(events),
            mode,
            hook,
        );
        (guard, warned)
    }

    fn event<E: Event>(e: E) -> DomainEvent {
        DomainEvent::new(EventEnvelope::new(now()), e)
    }

    #[test]
    fn the_guard_checks_commands_and_events() {
        let (guard, _) = guard(ProjectionMode::Skip);
        assert_eq!(
            *guard.validate_command("Nope").unwrap_err().code(),
            ErrorCode::CommandHandlerNotRegistered
        );
        assert!(guard.validate_event(&event(Opened)).is_ok());
        assert!(guard.validate_event(&event(Upgraded)).is_ok());

        #[derive(Debug, Serialize, Deserialize)]
        struct Stranger;
        impl Event for Stranger {
            const NAME: &'static str = "Stranger";
        }
        assert_eq!(
            *guard.validate_event(&event(Stranger)).unwrap_err().code(),
            ErrorCode::UnknownEventType
        );
        // Same name, other version.
        #[derive(Debug, Serialize, Deserialize)]
        struct OpenedV2;
        impl Event for OpenedV2 {
            const NAME: &'static str = "Opened";
            const VERSION: &'static str = "9.9.9";
        }
        assert_eq!(
            *guard.validate_event(&event(OpenedV2)).unwrap_err().code(),
            ErrorCode::EventVersionMismatch
        );
    }

    #[test]
    fn projection_modes_skip_warn_or_fail() {
        let upgraded = event(Upgraded);
        let handled = ["Opened"];

        let (skip, warned) = guard(ProjectionMode::Skip);
        skip.validate_projection_dispatch("p", &handled, &upgraded)
            .unwrap();
        assert_eq!(*warned.lock().unwrap(), 0);

        let (warn, warned) = guard(ProjectionMode::Warn);
        warn.validate_projection_dispatch("p", &handled, &upgraded)
            .unwrap();
        warn.validate_projection_dispatch("p", &handled, &event(Opened))
            .unwrap();
        assert_eq!(*warned.lock().unwrap(), 1);

        let (strict, _) = guard(ProjectionMode::Strict);
        let error = strict
            .validate_projection_dispatch("p", &handled, &upgraded)
            .unwrap_err();
        assert_eq!(*error.code(), ErrorCode::MissingEventHandler);
        strict
            .validate_projection_dispatch("p", &handled, &event(Opened))
            .unwrap();
    }
}
