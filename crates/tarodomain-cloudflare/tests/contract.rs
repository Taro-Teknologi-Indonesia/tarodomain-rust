//! Every contract suite of `tarodomain-testing` against the Cloudflare
//! provider, on SQLite in place of D1 (the same suites run on D1 itself in
//! workerd, Step 10.8).

use std::sync::Arc;

use tarodomain::adapters::Provider;
use tarodomain_cloudflare::provider::CloudflareProvider;
use tarodomain_cloudflare::queue_bus::RecordingQueue;
use tarodomain_cloudflare::sqlite_backend::SqliteBackend;

/// A provider over a new, migrated database.
async fn provider() -> CloudflareProvider {
    let backend = Arc::new(SqliteBackend::in_memory().unwrap());
    let provider = CloudflareProvider::with_backend(backend, Some(Arc::new(RecordingQueue::new())));
    provider.initialize().await.unwrap();
    provider
}

tarodomain_testing::provider_contract_tests!(provider);
