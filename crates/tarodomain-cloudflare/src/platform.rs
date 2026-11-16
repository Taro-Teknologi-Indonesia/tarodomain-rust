//! Glue between the framework and the Workers runtime: sleeping, error
//! conversion and the wrappers that make JavaScript handles usable behind
//! the core's `Send + Sync` traits.
//!
//! A Worker isolate runs on one thread, so claiming `Send` for a JavaScript
//! handle is sound there. The wrappers are therefore created only from `Env`
//! bindings inside this crate, never in code that also runs natively.

use std::error::Error as StdError;

use tarodomain::error::FrameworkError;

/// Converts an error from the platform into a retryable infrastructure error
/// that keeps the original as its source.
///
/// "Any other error is infrastructure and is retried" (§22.1) holds because
/// foreign errors are converted where they enter.
pub fn infrastructure(
    what: &str,
    error: impl Into<Box<dyn StdError + Send + Sync + 'static>>,
) -> FrameworkError {
    let error = error.into();
    FrameworkError::event_store_error(format!("{what}: {error}")).with_source(error)
}

#[cfg(target_arch = "wasm32")]
pub use worker_glue::*;

#[cfg(target_arch = "wasm32")]
mod worker_glue {
    use std::future::Future;
    use std::time::Duration;

    use tarodomain::error::FrameworkError;
    use tarodomain::kernel::BoxFuture;
    use tarodomain::kernel::backoff::Sleeper;
    use worker::send::{SendFuture, SendWrapper};

    /// A JavaScript handle (a D1 database, a queue) that may sit in a
    /// `Send + Sync` adapter.
    pub type Handle<T> = SendWrapper<T>;

    /// Wraps a future that holds JavaScript handles so it is `Send`.
    pub fn send_future<F: Future>(future: F) -> SendFuture<F> {
        SendFuture::new(future)
    }

    /// Converts a `worker::Error` into an infrastructure error.
    pub fn worker_error(what: &str, error: worker::Error) -> FrameworkError {
        super::infrastructure(what, error.to_string())
    }

    /// Sleeps with `worker::Delay`, because tokio's timers don't work inside
    /// a Worker.
    #[derive(Clone, Copy, Debug, Default)]
    pub struct WorkerSleeper;

    impl Sleeper for WorkerSleeper {
        fn sleep(&self, duration: Duration) -> BoxFuture<'_, ()> {
            Box::pin(SendFuture::new(worker::Delay::from(duration)))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use super::*;

    #[test]
    fn a_foreign_error_becomes_a_retryable_infrastructure_error_with_its_source() {
        let error = infrastructure("D1 query", "no such table");
        assert!(error.retryable());
        assert_eq!(error.http_status(), 500);
        assert!(error.message().contains("D1 query"));
        assert_eq!(error.source().unwrap().to_string(), "no such table");
    }
}
