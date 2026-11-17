//! The framework: builder, validation, runtime and HTTP API (spec §25, §26,
//! §29).

mod aggregate_config;
mod builder;
mod http;
mod taro_domain;
mod validation;
#[cfg(not(target_arch = "wasm32"))]
mod worker;

pub use validation::{
    AggregateInfo, AggregateKind, DispatchGuard, FrameworkValidator, ProjectionInfo,
    ProjectionMode, SagaInfo, ValidationInput, ValidationReport, Warning, WarningHook, WarningKind,
    default_warning_hook,
};

pub use aggregate_config::AggregateConfig;
pub use builder::{SagaOptions, TaroDomainBuilder};
pub use http::{HttpApi, HttpRequest, HttpResponse};
pub use taro_domain::{
    COMMAND_RETENTION, CleanupReport, EVENT_RETENTION, MessageOutcome, MessageResult, PROVIDER,
    QUEUE_CONSUMER, QueueMessage, ScheduledRun, TARO_DOMAIN, TaroDomain,
};

#[cfg(not(target_arch = "wasm32"))]
pub use worker::Worker;
