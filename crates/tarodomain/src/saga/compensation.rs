//! Compensation tracking (spec §13.5, §17).

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use crate::domain::AnyCommand;
use crate::error::{FrameworkError, Result};

type Factory<S> = Arc<dyn Fn(&S) -> Vec<AnyCommand> + Send + Sync>;

/// The compensations a saga type defines, by name: each is a factory that
/// builds commands from the saga's state **when compensation runs**, so the
/// state is current.
pub struct CompensationDefs<S> {
    factories: BTreeMap<String, Factory<S>>,
}

impl<S> CompensationDefs<S> {
    /// An empty set.
    pub fn new() -> Self {
        Self {
            factories: BTreeMap::new(),
        }
    }

    /// Defines (or replaces) the compensation `name`.
    pub fn define(
        &mut self,
        name: impl Into<String>,
        factory: impl Fn(&S) -> Vec<AnyCommand> + Send + Sync + 'static,
    ) {
        self.factories.insert(name.into(), Arc::new(factory));
    }

    /// Whether `name` is defined.
    pub fn has(&self, name: &str) -> bool {
        self.factories.contains_key(name)
    }

    /// The defined names, alphabetical.
    pub fn names(&self) -> Vec<&str> {
        self.factories.keys().map(String::as_str).collect()
    }

    /// Builds the commands of `name` from `state`. An undefined name is
    /// `INVALID_CONFIGURATION`.
    pub fn commands(&self, name: &str, state: &S) -> Result<Vec<AnyCommand>> {
        let factory = self.factories.get(name).ok_or_else(|| {
            FrameworkError::invalid_configuration(format!("compensation {name:?} isn't defined"))
        })?;
        Ok(factory(state))
    }
}

impl<S> Default for CompensationDefs<S> {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> fmt::Debug for CompensationDefs<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.factories.keys()).finish()
    }
}

/// The compensations a running saga has pushed, last in first out.
///
/// It holds only **names**, so it serializes with the saga's state; the
/// factories are looked up in the saga type's [`CompensationDefs`] when
/// compensation runs. (The spec's stack of closures wouldn't survive: the
/// saga is reloaded from the store for every event.)
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompensationStack {
    names: Vec<String>,
}

impl CompensationStack {
    /// An empty stack.
    pub fn new() -> Self {
        Self::default()
    }

    /// A stack with these names, oldest first.
    pub fn from_names(names: Vec<String>) -> Self {
        Self { names }
    }

    /// The pushed names, oldest first.
    pub fn names(&self) -> &[String] {
        &self.names
    }

    /// Whether nothing is pushed.
    pub fn is_empty(&self) -> bool {
        self.names.is_empty()
    }

    /// Pushes the compensation `name` for a step that has completed.
    pub fn push(&mut self, name: impl Into<String>) {
        self.names.push(name.into());
    }

    /// Pops the latest compensation and builds its commands, or returns
    /// nothing if the stack is empty.
    pub fn pop<S>(&mut self, defs: &CompensationDefs<S>, state: &S) -> Result<Vec<AnyCommand>> {
        match self.names.last() {
            Some(name) => {
                let commands = defs.commands(name, state)?;
                self.names.pop();
                Ok(commands)
            }
            None => Ok(Vec::new()),
        }
    }

    /// Pops every compensation, latest first, and returns their commands in
    /// that order (§13.5).
    pub fn take_all<S>(
        &mut self,
        defs: &CompensationDefs<S>,
        state: &S,
    ) -> Result<Vec<AnyCommand>> {
        let mut commands = Vec::new();
        for name in self.names.iter().rev() {
            commands.extend(defs.commands(name, state)?);
        }
        self.names.clear();
        Ok(commands)
    }
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::domain::Command;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Refund(i64);
    impl Command for Refund {
        const NAME: &'static str = "Refund";
    }

    fn defs() -> CompensationDefs<i64> {
        let mut d = CompensationDefs::new();
        d.define("first", |s: &i64| vec![Refund(*s + 1).into()]);
        d.define("second", |s: &i64| vec![Refund(*s + 2).into()]);
        d
    }

    fn amounts(commands: &[AnyCommand]) -> Vec<i64> {
        commands
            .iter()
            .map(|c| c.downcast_ref::<Refund>().unwrap().0)
            .collect()
    }

    #[test]
    fn take_all_is_last_in_first_out_and_uses_current_state() {
        let d = defs();
        let mut stack = CompensationStack::new();
        stack.push("first");
        stack.push("second");
        // The state at compensation time, not at push time.
        let commands = stack.take_all(&d, &100).unwrap();
        assert_eq!(amounts(&commands), [102, 101]);
        assert!(stack.is_empty());
    }

    #[test]
    fn pop_takes_one_and_an_empty_stack_gives_nothing() {
        let d = defs();
        let mut stack = CompensationStack::from_names(vec!["first".into(), "second".into()]);
        assert_eq!(amounts(&stack.pop(&d, &0).unwrap()), [2]);
        assert_eq!(stack.names(), ["first"]);
        assert_eq!(amounts(&stack.pop(&d, &0).unwrap()), [1]);
        assert!(stack.pop(&d, &0).unwrap().is_empty());
    }

    #[test]
    fn an_undefined_name_is_invalid_configuration_and_keeps_the_stack() {
        let d = defs();
        let mut stack = CompensationStack::from_names(vec!["gone".into()]);
        let e = stack.pop(&d, &0).unwrap_err();
        assert_eq!(e.code().as_str(), "INVALID_CONFIGURATION");
        assert_eq!(stack.names(), ["gone"]);
    }
}
