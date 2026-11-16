//! Contract suites for TaroDomain providers (spec §24): the behaviour every
//! provider must have, written once and run against each of them.
//!
//! The persistence traits in [`tarodomain::adapters`] state their contract in
//! rustdoc. Each suite here checks one of them through a real provider: it
//! asks a [`ProviderFactory`] for a provider with empty storage, once per
//! case, and drives the adapters the provider hands out. A provider passes
//! when every suite returns without panicking.
//!
//! A provider crate runs them all with [`provider_contract_tests!`] in an
//! integration test. They can also be called one by one:
//!
//! ```
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! use tarodomain_local::LocalProvider;
//! use tarodomain_testing::*;
//!
//! let local = || async { LocalProvider::new() };
//! event_store_contract(local).await;
//! snapshot_contract(local).await;
//! saga_store_contract(local).await;
//! idempotency_contract(local).await;
//! dead_letter_contract(local).await;
//! unit_of_work_contract(local).await;
//! concurrency_contract(local).await;
//! provider_contract(local).await;
//! # }
//! ```
//!
//! The suites run their concurrent parts with `tokio::join!` and
//! `futures::future::join_all` inside one task, so they need no runtime of
//! their own and also run inside a Cloudflare Worker, where the harness calls
//! them.

use std::future::Future;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tarodomain::adapters::Provider;

mod concurrency_contract;
mod dead_letter_contract;
mod event_store_contract;
pub mod fixtures;
mod idempotency_contract;
mod provider_contract;
mod saga_store_contract;
mod snapshot_contract;
mod unit_of_work_contract;

pub use concurrency_contract::concurrency_contract;
pub use dead_letter_contract::dead_letter_contract;
pub use event_store_contract::event_store_contract;
pub use idempotency_contract::idempotency_contract;
pub use provider_contract::provider_contract;
pub use saga_store_contract::saga_store_contract;
pub use snapshot_contract::snapshot_contract;
pub use unit_of_work_contract::unit_of_work_contract;

/// Builds a provider whose stores are all empty, once per test case.
///
/// Implemented for every closure that returns a future of a provider, such
/// as `|| async { LocalProvider::new() }`. A provider over a real database
/// creates a fresh database (or empties one) each time.
pub trait ProviderFactory {
    /// A new provider with empty storage.
    fn create(&self) -> impl Future<Output = Arc<dyn Provider>>;
}

impl<F, Fut, P> ProviderFactory for F
where
    F: Fn() -> Fut,
    Fut: Future<Output = P>,
    P: Provider + 'static,
{
    fn create(&self) -> impl Future<Output = Arc<dyn Provider>> {
        let provider = self();
        async move { Arc::new(provider.await) as Arc<dyn Provider> }
    }
}

/// A cut-off before anything a store could have written: the Unix epoch.
pub(crate) fn long_ago() -> DateTime<Utc> {
    DateTime::UNIX_EPOCH
}

/// A cut-off after anything a store could have written: a century after the
/// fixtures' epoch.
pub(crate) fn far_future() -> DateTime<Utc> {
    const CENTURY_MS: u64 = 100 * 365 * 24 * 60 * 60 * 1000;
    fixtures::time(CENTURY_MS)
}

/// Expands into one test per contract suite, each running the suite against
/// the providers `$factory` builds.
///
/// `$factory` is a [`ProviderFactory`] expression, evaluated once per test.
/// The tests are plain `#[test]` functions that run their suite on a
/// current-thread tokio runtime, so the calling crate needs no tokio
/// dependency of its own. Put the call in an integration test, such as
/// `tests/contract.rs`, or in a module of its own: each test is named after
/// its suite, such as `event_store_contract`.
///
/// ```
/// use tarodomain_local::LocalProvider;
///
/// tarodomain_testing::provider_contract_tests!(|| async { LocalProvider::new() });
/// # fn main() {}
/// ```
#[macro_export]
macro_rules! provider_contract_tests {
    ($factory:expr $(,)?) => {
        $crate::provider_contract_tests!(@suites ($factory)
            event_store_contract
            snapshot_contract
            saga_store_contract
            idempotency_contract
            dead_letter_contract
            unit_of_work_contract
            concurrency_contract
            provider_contract
        );
    };
    (@suites ($factory:expr) $($suite:ident)+) => {
        $(
            #[test]
            fn $suite() {
                $crate::__private::run($crate::$suite($factory));
            }
        )+
    };
}

/// What [`provider_contract_tests!`] expands to calls. Not part of the
/// public API.
#[doc(hidden)]
pub mod __private {
    use std::future::Future;

    /// Runs a suite to completion on a new current-thread runtime.
    pub fn run(suite: impl Future<Output = ()>) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("a current-thread tokio runtime");
        runtime.block_on(suite);
    }
}
