//! TaroDomain for Rust: event sourcing, CQRS and DDD with strong or eventual
//! consistency per command.
//!
//! This crate implements the platform-agnostic parts of the
//! [TaroDomain specification](https://github.com/Taro-Teknologi-Indonesia/tarodomain)
//! (§2–23, §25, §28–29). It never depends on a database driver, an HTTP
//! framework or a cloud SDK: providers live in their own crates and implement
//! the persistence and event bus contracts defined here.
//!
//! See `docs/IMPLEMENTATION_PLAN.md` in the repository for the roadmap.

pub mod error;
pub mod kernel;
