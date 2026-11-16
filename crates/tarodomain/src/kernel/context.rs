//! Values scoped to the current task, such as the execution context (§5.5) and
//! the active unit of work (§11.4).
//!
//! The storage holds at most one value per type. [`scope`] runs a future with
//! a value set; the future sees a copy of the values in scope when it starts,
//! plus the new one, so `scope(a, scope(b, fut))` sees both. When it ends, the
//! outer values are back. Each task has its own values, so concurrent requests
//! never see each other's context.
//!
//! A future passed to `tokio::spawn` starts with no values. Wrap it in
//! [`scope`] before spawning to hand over exactly the values it should see.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

tokio::task_local! {
    static VALUES: Values;
}

/// The values in scope: an immutable map from type to value, cheap to clone.
#[derive(Clone, Default)]
struct Values(Arc<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>);

impl Values {
    fn current() -> Self {
        VALUES.try_with(Self::clone).unwrap_or_default()
    }

    fn with<T: Send + Sync + 'static>(&self, value: T) -> Self {
        let mut map = HashMap::clone(&self.0);
        map.insert(TypeId::of::<T>(), Arc::new(value));
        Self(Arc::new(map))
    }

    fn get<T: 'static>(&self) -> Option<&T> {
        self.0.get(&TypeId::of::<T>())?.downcast_ref()
    }
}

/// Runs `fut` with `value` in scope, alongside the values in scope when the
/// returned future is first polled.
///
/// The outer values are read when the future starts, not when `scope` is
/// called, so nested scopes compose however they are built.
pub async fn scope<T, F>(value: T, fut: F) -> F::Output
where
    T: Send + Sync + 'static,
    F: Future,
{
    VALUES.scope(Values::current().with(value), fut).await
}

/// Runs `f` with `value` in scope, alongside the values in scope now.
pub fn sync_scope<T, R>(value: T, f: impl FnOnce() -> R) -> R
where
    T: Send + Sync + 'static,
{
    VALUES.sync_scope(Values::current().with(value), f)
}

/// Calls `f` with the value of type `T` in scope, or returns `None` if there
/// is none.
pub fn try_with<T: 'static, R>(f: impl FnOnce(&T) -> R) -> Option<R> {
    VALUES
        .try_with(|values| values.get::<T>().map(f))
        .ok()
        .flatten()
}

/// A clone of the value of type `T` in scope, if there is one.
pub fn get<T: Clone + 'static>() -> Option<T> {
    try_with(T::clone)
}

#[cfg(test)]
mod tests {
    use tokio::task::yield_now;

    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq)]
    struct RequestId(u32);

    #[derive(Clone, Copy, Debug, PartialEq)]
    struct Tenant(&'static str);

    #[test]
    fn nothing_outside_a_scope() {
        assert_eq!(get::<RequestId>(), None);
        assert_eq!(try_with(|id: &RequestId| id.0), None);
    }

    #[tokio::test]
    async fn scope_sets_a_value() {
        let seen = scope(RequestId(1), async { get::<RequestId>() }).await;
        assert_eq!(seen, Some(RequestId(1)));
        assert_eq!(get::<RequestId>(), None);
    }

    #[tokio::test]
    async fn nested_scope_copies_the_outer_one_and_restores_it() {
        let outer = async {
            let inner = scope(RequestId(2), async {
                (get::<RequestId>(), get::<Tenant>())
            });
            assert_eq!(inner.await, (Some(RequestId(2)), Some(Tenant("acme"))));
            (get::<RequestId>(), get::<Tenant>())
        };
        let outer = scope(Tenant("acme"), scope(RequestId(1), outer));
        assert_eq!(outer.await, (Some(RequestId(1)), Some(Tenant("acme"))));
    }

    #[test]
    fn sync_scope_nests_too() {
        let seen = sync_scope(RequestId(1), || {
            let inner = sync_scope(RequestId(2), get::<RequestId>);
            (inner, get::<RequestId>())
        });
        assert_eq!(seen, (Some(RequestId(2)), Some(RequestId(1))));
    }

    #[tokio::test]
    async fn concurrent_tasks_see_only_their_own_value() {
        let task = |n| {
            scope(RequestId(n), async move {
                let mut seen = Vec::new();
                for _ in 0..3 {
                    seen.push(get::<RequestId>());
                    yield_now().await;
                }
                seen
            })
        };
        let (a, b) = tokio::join!(task(1), task(2));
        assert_eq!(a, [Some(RequestId(1)); 3]);
        assert_eq!(b, [Some(RequestId(2)); 3]);
    }

    #[tokio::test]
    async fn values_are_read_when_the_future_starts() {
        let fut = sync_scope(RequestId(7), || {
            scope(Tenant("acme"), async {
                (get::<RequestId>(), get::<Tenant>())
            })
        });
        assert_eq!(fut.await, (None, Some(Tenant("acme"))));

        let fut = scope(Tenant("acme"), async { get::<RequestId>() });
        assert_eq!(scope(RequestId(7), fut).await, Some(RequestId(7)));
    }

    #[tokio::test]
    async fn spawned_tasks_start_empty() {
        let seen = scope(RequestId(1), async {
            tokio::spawn(async { get::<RequestId>() }).await.unwrap()
        })
        .await;
        assert_eq!(seen, None);
    }
}
