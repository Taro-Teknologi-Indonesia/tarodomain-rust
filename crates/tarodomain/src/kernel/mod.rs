//! The kernel layer (spec §1.3): IDs, time, backoff and request-scoped context
//! storage. Every other layer builds on it.

pub mod backoff;
pub mod clock;
pub mod context;
pub mod ids;

use std::future::Future;
use std::pin::Pin;

/// A boxed future that can move between threads, as returned by the
/// framework's object-safe async traits.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
