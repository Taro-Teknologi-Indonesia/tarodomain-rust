//! The local provider passes every contract suite of `tarodomain-testing`.

use tarodomain_local::LocalProvider;

tarodomain_testing::provider_contract_tests!(|| async { LocalProvider::new() });

/// The same suites with the database's clock stopped, which shows that they
/// rely on no time passing between writes.
mod stopped_clock {
    use std::sync::Arc;

    use tarodomain::kernel::clock::ManualClock;
    use tarodomain_local::{LocalDatabase, LocalProvider};
    use tarodomain_testing::fixtures;

    fn provider() -> LocalProvider {
        let clock = Arc::new(ManualClock::new(fixtures::time(0)));
        LocalProvider::with_database(LocalDatabase::with_clock(clock))
    }

    tarodomain_testing::provider_contract_tests!(|| async { provider() });
}
