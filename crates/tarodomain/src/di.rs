//! Dependency injection (§23): [`Token`] and [`Container`].
//!
//! A [`Token<T>`] is a typed key. A [`Container`] maps tokens to factories
//! with one of three [`Lifetime`]s, and `resolve(token)` returns an
//! `Arc<T>`. `T` may be a trait object (`Token<dyn Clock>`).
//!
//! ```
//! use std::sync::Arc;
//! use tarodomain::di::{Container, Token};
//!
//! const GREETING: Token<String> = Token::new("greeting");
//! const LENGTH: Token<usize> = Token::new("length");
//!
//! let container = Container::new();
//! container
//!     .bind_instance(GREETING, Arc::new("hello".to_string()))
//!     .bind_singleton(LENGTH, |c| Ok(Arc::new(c.resolve(GREETING)?.len())));
//! assert_eq!(*container.resolve(LENGTH).unwrap(), 5);
//! ```
//!
//! # Where a factory runs
//!
//! * A **singleton** is created once, with the container it was bound on, so
//!   it can't see a scope's values even when a scope asked for it. Every
//!   scope shares the instance.
//! * A **scoped** value is created once per container it is resolved from
//!   (the root counts as a scope of its own).
//! * A **transient** value is created on every `resolve`, with the container
//!   it is resolved from.
//!
//! A factory is synchronous and runs without any lock held. If two threads
//! resolve an unbuilt singleton at the same moment, both factories may run,
//! but only the first result is kept and both callers get it.

use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use crate::error::{FrameworkError, Result};

/// A type-safe key for a dependency. Declare it as a `const`:
/// `const CLOCK: Token<dyn Clock> = Token::new("clock");`.
///
/// Two tokens are the same key when their types and names are equal.
pub struct Token<T: ?Sized + 'static> {
    name: &'static str,
    _marker: PhantomData<fn() -> Arc<T>>,
}

impl<T: ?Sized + 'static> Token<T> {
    /// A token with the given name, which appears in error messages.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            _marker: PhantomData,
        }
    }

    /// The token's name.
    pub const fn name(&self) -> &'static str {
        self.name
    }

    fn key(&self) -> Key {
        Key {
            type_id: TypeId::of::<T>(),
            name: self.name,
        }
    }
}

impl<T: ?Sized + 'static> Clone for Token<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: ?Sized + 'static> Copy for Token<T> {}

impl<T: ?Sized + 'static> fmt::Debug for Token<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Token({})", self.name)
    }
}

/// How long a resolved value lives (§23.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Lifetime {
    /// One instance for the whole container tree.
    Singleton,
    /// A new instance on every `resolve`.
    Transient,
    /// One instance per container (the spec's `request`).
    Scoped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct Key {
    type_id: TypeId,
    name: &'static str,
}

type Value = Arc<dyn Any + Send + Sync>;
type Factory = dyn Fn(&Container) -> Result<Value> + Send + Sync;

struct Registration {
    lifetime: Lifetime,
    factory: Box<Factory>,
    /// The container the binding was made on; singletons are built with it.
    owner: Weak<Inner>,
    singleton: Mutex<Option<Value>>,
}

struct Inner {
    registrations: Arc<Mutex<HashMap<Key, Arc<Registration>>>>,
    scoped: Mutex<HashMap<(usize, Key), Value>>,
}

/// A dependency container. Cloning it gives another handle to the same
/// container; [`create_scope`](Self::create_scope) gives a child.
#[derive(Clone)]
pub struct Container {
    inner: Arc<Inner>,
}

impl Default for Container {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Container {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let count = lock(&self.inner.registrations).len();
        f.debug_struct("Container")
            .field("bindings", &count)
            .finish()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    // A factory that panics holds no lock, so poisoning carries no meaning.
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

/// The keys being resolved on this thread, innermost last. It is shared by
/// every container, so a cycle that crosses scopes is found too. Factories
/// are synchronous, so a resolution never moves to another thread.
type Frame = (usize, Key);

thread_local! {
    static STACK: RefCell<Vec<Frame>> = const { RefCell::new(Vec::new()) };
}

struct StackGuard;

impl Drop for StackGuard {
    fn drop(&mut self) {
        STACK.with(|s| {
            s.borrow_mut().pop();
        });
    }
}

impl Container {
    /// An empty root container.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Inner {
                registrations: Arc::default(),
                scoped: Mutex::default(),
            }),
        }
    }

    /// Binds `token` to a factory with the given lifetime. Binding a token
    /// again replaces the earlier binding.
    pub fn bind<T, F>(&self, token: Token<T>, factory: F, lifetime: Lifetime) -> &Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: Fn(&Container) -> Result<Arc<T>> + Send + Sync + 'static,
    {
        let registration = Registration {
            lifetime,
            factory: Box::new(move |c| Ok(Arc::new(factory(c)?) as Value)),
            owner: Arc::downgrade(&self.inner),
            singleton: Mutex::new(None),
        };
        lock(&self.inner.registrations).insert(token.key(), Arc::new(registration));
        self
    }

    /// [`bind`](Self::bind) with [`Lifetime::Singleton`].
    pub fn bind_singleton<T, F>(&self, token: Token<T>, factory: F) -> &Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: Fn(&Container) -> Result<Arc<T>> + Send + Sync + 'static,
    {
        self.bind(token, factory, Lifetime::Singleton)
    }

    /// [`bind`](Self::bind) with [`Lifetime::Transient`].
    pub fn bind_transient<T, F>(&self, token: Token<T>, factory: F) -> &Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: Fn(&Container) -> Result<Arc<T>> + Send + Sync + 'static,
    {
        self.bind(token, factory, Lifetime::Transient)
    }

    /// [`bind`](Self::bind) with [`Lifetime::Scoped`].
    pub fn bind_scoped<T, F>(&self, token: Token<T>, factory: F) -> &Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: Fn(&Container) -> Result<Arc<T>> + Send + Sync + 'static,
    {
        self.bind(token, factory, Lifetime::Scoped)
    }

    /// Binds an existing value as a singleton.
    pub fn bind_instance<T>(&self, token: Token<T>, instance: Arc<T>) -> &Self
    where
        T: ?Sized + Send + Sync + 'static,
    {
        self.bind_singleton(token, move |_| Ok(Arc::clone(&instance)))
    }

    /// Whether `token` is bound, here or in the container this is a scope of.
    pub fn has<T: ?Sized + 'static>(&self, token: Token<T>) -> bool {
        lock(&self.inner.registrations).contains_key(&token.key())
    }

    /// Resolves `token`. An unbound token is `MISSING_DEPENDENCY`, and a
    /// token that needs itself is `CIRCULAR_DEPENDENCY` with the path in the
    /// message. A factory's own error comes back unchanged and nothing is
    /// cached.
    pub fn resolve<T: ?Sized + Send + Sync + 'static>(&self, token: Token<T>) -> Result<Arc<T>> {
        self.resolve_or_none(token)?.ok_or_else(|| {
            FrameworkError::missing_dependency(format!(
                "no dependency is bound for token '{}'",
                token.name
            ))
            .with_context("token", token.name)
        })
    }

    /// Like [`resolve`](Self::resolve), but an unbound token gives `None`.
    /// Other errors still fail.
    pub fn resolve_or_none<T: ?Sized + Send + Sync + 'static>(
        &self,
        token: Token<T>,
    ) -> Result<Option<Arc<T>>> {
        let Some(value) = self.resolve_value(token.key())? else {
            return Ok(None);
        };
        // The value is an `Arc<T>` inside an `Arc<dyn Any>`: the extra layer
        // is what lets `T` be a trait object.
        match value.downcast_ref::<Arc<T>>() {
            Some(arc) => Ok(Some(Arc::clone(arc))),
            None => Err(FrameworkError::invalid_configuration(format!(
                "the binding of token '{}' holds another type",
                token.name
            ))),
        }
    }

    /// A child container for request-scoped values. It sees every binding,
    /// including ones added later, but keeps its own scoped instances.
    pub fn create_scope(&self) -> Container {
        Container {
            inner: Arc::new(Inner {
                registrations: Arc::clone(&self.inner.registrations),
                scoped: Mutex::default(),
            }),
        }
    }

    fn resolve_value(&self, key: Key) -> Result<Option<Value>> {
        let registration = lock(&self.inner.registrations).get(&key).cloned();
        let Some(registration) = registration else {
            return Ok(None);
        };
        // Keyed by the shared registry, so two separate container trees that
        // happen to use the same token don't look like a cycle.
        let tree = Arc::as_ptr(&self.inner.registrations) as usize;
        let frame = (tree, key);
        let _guard = self.enter(frame)?;

        let value = match registration.lifetime {
            Lifetime::Transient => (registration.factory)(self)?,
            Lifetime::Scoped => {
                let cached = lock(&self.inner.scoped).get(&(tree, key)).cloned();
                match cached {
                    Some(v) => v,
                    None => {
                        let built = (registration.factory)(self)?;
                        lock(&self.inner.scoped)
                            .entry((tree, key))
                            .or_insert(built)
                            .clone()
                    }
                }
            }
            Lifetime::Singleton => {
                let cached = lock(&registration.singleton).clone();
                match cached {
                    Some(v) => v,
                    None => {
                        let owner = registration
                            .owner
                            .upgrade()
                            .map(|inner| Container { inner })
                            .unwrap_or_else(|| self.clone());
                        let built = (registration.factory)(&owner)?;
                        lock(&registration.singleton).get_or_insert(built).clone()
                    }
                }
            }
        };
        Ok(Some(value))
    }

    /// Pushes `frame`, or fails if it is already being resolved.
    fn enter(&self, frame: Frame) -> Result<StackGuard> {
        STACK.with(|stack| {
            let mut stack = stack.borrow_mut();
            if let Some(start) = stack.iter().position(|f| *f == frame) {
                let path: Vec<&str> = stack[start..]
                    .iter()
                    .map(|f| f.1.name)
                    .chain(std::iter::once(frame.1.name))
                    .collect();
                let path = path.join(" -> ");
                return Err(FrameworkError::circular_dependency(format!(
                    "circular dependency: {path}"
                ))
                .with_context("path", path));
            }
            stack.push(frame);
            Ok(StackGuard)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::ErrorCode;

    const NUMBER: Token<usize> = Token::new("number");
    const TEXT: Token<String> = Token::new("text");
    const A: Token<usize> = Token::new("a");
    const B: Token<usize> = Token::new("b");

    fn counter() -> (Arc<AtomicUsize>, impl Fn(&Container) -> Result<Arc<usize>>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        (calls, move |_: &Container| {
            Ok(Arc::new(c.fetch_add(1, Ordering::SeqCst)))
        })
    }

    #[test]
    fn instance_is_returned_as_the_same_arc() {
        let container = Container::new();
        let value = Arc::new("x".to_string());
        container.bind_instance(TEXT, Arc::clone(&value));
        assert!(Arc::ptr_eq(&container.resolve(TEXT).unwrap(), &value));
        assert!(container.has(TEXT));
        assert!(!container.has(NUMBER));
    }

    #[test]
    fn missing_dependency() {
        let container = Container::new();
        let err = container.resolve(NUMBER).unwrap_err();
        assert_eq!(*err.code(), ErrorCode::MissingDependency);
        assert!(err.message().contains("number"));
        assert!(container.resolve_or_none(NUMBER).unwrap().is_none());
    }

    #[test]
    fn tokens_with_the_same_name_but_another_type_differ() {
        const OTHER: Token<String> = Token::new("number");
        let container = Container::new();
        container.bind_instance(NUMBER, Arc::new(1));
        assert!(!container.has(OTHER));
    }

    #[test]
    fn singleton_is_built_once_and_shared_by_scopes() {
        let (calls, factory) = counter();
        let root = Container::new();
        root.bind_singleton(NUMBER, factory);
        let scope = root.create_scope();
        let first = scope.resolve(NUMBER).unwrap();
        assert!(Arc::ptr_eq(&first, &root.resolve(NUMBER).unwrap()));
        assert!(Arc::ptr_eq(
            &first,
            &root.create_scope().resolve(NUMBER).unwrap()
        ));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn transient_is_built_every_time() {
        let (calls, factory) = counter();
        let root = Container::new();
        root.bind_transient(NUMBER, factory);
        let a = root.resolve(NUMBER).unwrap();
        let b = root.resolve(NUMBER).unwrap();
        assert_ne!(a, b);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn scoped_is_built_once_per_scope() {
        let (calls, factory) = counter();
        let root = Container::new();
        root.bind_scoped(NUMBER, factory);
        let one = root.create_scope();
        let two = root.create_scope();
        let a = one.resolve(NUMBER).unwrap();
        assert!(Arc::ptr_eq(&a, &one.resolve(NUMBER).unwrap()));
        assert!(!Arc::ptr_eq(&a, &two.resolve(NUMBER).unwrap()));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn a_singleton_is_built_with_the_container_it_was_bound_on() {
        const SCOPED: Token<usize> = Token::new("scoped");
        const HOLDER: Token<Arc<usize>> = Token::new("holder");
        let (_, factory) = counter();
        let root = Container::new();
        root.bind_scoped(SCOPED, factory);
        root.bind_singleton(HOLDER, |c| Ok(Arc::new(c.resolve(SCOPED)?)));

        let scope = root.create_scope();
        let held = scope.resolve(HOLDER).unwrap();
        // Built from the root, so it holds the root's scoped value, not the scope's.
        assert!(Arc::ptr_eq(&*held, &root.resolve(SCOPED).unwrap()));
        assert!(!Arc::ptr_eq(&*held, &scope.resolve(SCOPED).unwrap()));
    }

    #[test]
    fn transient_and_scoped_use_the_resolving_container() {
        const SCOPED: Token<usize> = Token::new("scoped");
        const HOLDER: Token<Arc<usize>> = Token::new("holder");
        let (_, factory) = counter();
        let root = Container::new();
        root.bind_scoped(SCOPED, factory);
        root.bind_transient(HOLDER, |c| Ok(Arc::new(c.resolve(SCOPED)?)));
        let scope = root.create_scope();
        let held = scope.resolve(HOLDER).unwrap();
        assert!(Arc::ptr_eq(&*held, &scope.resolve(SCOPED).unwrap()));
    }

    #[test]
    fn dependencies_resolve_through_the_container() {
        let root = Container::new();
        root.bind_instance(TEXT, Arc::new("hello".into()))
            .bind_singleton(NUMBER, |c| Ok(Arc::new(c.resolve(TEXT)?.len())));
        assert_eq!(*root.resolve(NUMBER).unwrap(), 5);
    }

    #[test]
    fn rebinding_replaces() {
        let root = Container::new();
        root.bind_instance(NUMBER, Arc::new(1));
        root.bind_instance(NUMBER, Arc::new(2));
        assert_eq!(*root.resolve(NUMBER).unwrap(), 2);
    }

    #[test]
    fn a_scope_sees_later_bindings() {
        let root = Container::new();
        let scope = root.create_scope();
        root.bind_instance(NUMBER, Arc::new(7));
        assert_eq!(*scope.resolve(NUMBER).unwrap(), 7);
    }

    #[test]
    fn trait_object_tokens() {
        trait Greeter: Send + Sync {
            fn greet(&self) -> &'static str;
        }
        struct English;
        impl Greeter for English {
            fn greet(&self) -> &'static str {
                "hello"
            }
        }
        const GREETER: Token<dyn Greeter> = Token::new("greeter");
        let root = Container::new();
        root.bind_singleton(GREETER, |_| Ok(Arc::new(English) as Arc<dyn Greeter>));
        assert_eq!(root.resolve(GREETER).unwrap().greet(), "hello");
    }

    #[test]
    fn a_circular_dependency_reports_its_path() {
        let root = Container::new();
        root.bind_singleton(A, |c| Ok(Arc::new(*c.resolve(B)?)));
        root.bind_singleton(B, |c| Ok(Arc::new(*c.resolve(A)?)));
        let err = root.resolve(A).unwrap_err();
        assert_eq!(*err.code(), ErrorCode::CircularDependency);
        assert!(err.message().contains("a -> b -> a"), "{}", err.message());
        // The stack is clean again: the same error, not a stale entry.
        assert_eq!(
            *root.resolve(B).unwrap_err().code(),
            ErrorCode::CircularDependency
        );
        assert!(root.resolve_or_none(NUMBER).unwrap().is_none());
    }

    #[test]
    fn a_circular_dependency_across_scopes_is_found() {
        let root = Container::new();
        root.bind_scoped(A, |c| Ok(Arc::new(*c.create_scope().resolve(A)?)));
        let err = root.create_scope().resolve(A).unwrap_err();
        assert_eq!(*err.code(), ErrorCode::CircularDependency);
    }

    #[test]
    fn a_failing_factory_is_not_cached() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let root = Container::new();
        root.bind_singleton(NUMBER, move |_| {
            if c.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(FrameworkError::event_store_error("down"))
            } else {
                Ok(Arc::new(9))
            }
        });
        assert_eq!(
            *root.resolve(NUMBER).unwrap_err().code(),
            ErrorCode::EventStoreError
        );
        assert_eq!(*root.resolve(NUMBER).unwrap(), 9);
    }

    #[test]
    fn the_same_token_in_two_trees_is_not_a_cycle() {
        let other = Container::new();
        other.bind_instance(NUMBER, Arc::new(3));
        let root = Container::new();
        root.bind_singleton(NUMBER, move |_| Ok(Arc::new(*other.resolve(NUMBER)? + 1)));
        assert_eq!(*root.resolve(NUMBER).unwrap(), 4);
    }
}
