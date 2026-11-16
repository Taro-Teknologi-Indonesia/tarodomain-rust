//! Retries and dead letters (spec §21): what happens to an event a handler
//! keeps failing on.
//!
//! [`RetryPolicy`] retries retryable errors with backoff, and
//! [`DeadLetterWriter`] files the event in the dead letter store when the
//! retries run out, or at once for an error that retrying can't fix.

mod retry_policy;
mod writer;

pub use retry_policy::{Attempted, RetryPolicy};
pub use writer::DeadLetterWriter;
