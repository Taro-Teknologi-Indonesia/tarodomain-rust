//! The HTTP API (spec §26.1) over plain request and response structs.
//!
//! No HTTP crate is involved: the Cloudflare worker and the axum server each
//! convert their own request type into an [`HttpRequest`], call
//! [`HttpApi::handle`] and convert the [`HttpResponse`] back, so both share
//! one route table.
//!
//! | Route | Does |
//! |---|---|
//! | `POST /commands/{name}` | Dispatches the command whose data is the JSON body. `x-request-id` (a UUID) becomes the correlation ID, and `x-command-id` (a UUID) the command ID, so a client can retry safely. The response header `x-request-id` carries the correlation ID in use. |
//! | `GET /health` | `200` with the provider type. |
//! | `POST /migrate` | Runs the provider's migrations. Off (`404`) unless [`HttpApi::allow_migrate`] turns it on. |
//!
//! Errors use [`FrameworkError::to_http_response`](crate::error::FrameworkError::to_http_response):
//! context is shown for client errors only and the source never is. An
//! unknown command is `404`, an unknown route `404`, a known route with the
//! wrong method `405`.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::TaroDomain;
use crate::domain::{Metadata, SerializedCommand};
use crate::error::FrameworkError;
use crate::kernel::clock;
use crate::kernel::ids::{new_v7, parse_uuid};

/// An incoming HTTP request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HttpRequest {
    /// The method, such as `POST`. Compared case-insensitively.
    pub method: String,
    /// The path, with or without a query string (which is ignored).
    pub path: String,
    /// The headers, with lower-case names.
    pub headers: BTreeMap<String, String>,
    /// The body.
    pub body: String,
}

impl HttpRequest {
    /// A request without headers or body.
    pub fn new(method: impl Into<String>, path: impl Into<String>) -> Self {
        Self {
            method: method.into(),
            path: path.into(),
            ..Self::default()
        }
    }

    /// Adds a header. The name is lower-cased.
    #[must_use]
    pub fn with_header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.insert(name.to_ascii_lowercase(), value.into());
        self
    }

    /// Sets the body.
    #[must_use]
    pub fn with_body(mut self, body: impl Into<String>) -> Self {
        self.body = body.into();
        self
    }

    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }
}

/// An outgoing HTTP response. The body is JSON.
#[derive(Clone, Debug, PartialEq)]
pub struct HttpResponse {
    /// The status code.
    pub status: u16,
    /// The headers, with lower-case names.
    pub headers: BTreeMap<String, String>,
    /// The JSON body.
    pub body: Value,
}

impl HttpResponse {
    fn json(status: u16, body: Value) -> Self {
        let mut headers = BTreeMap::new();
        headers.insert("content-type".to_owned(), "application/json".to_owned());
        Self {
            status,
            headers,
            body,
        }
    }

    fn with_header(mut self, name: &str, value: impl Into<String>) -> Self {
        self.headers.insert(name.to_owned(), value.into());
        self
    }

    fn error(status: u16, error: &FrameworkError) -> Self {
        let mut response = error.to_http_response();
        response.status = status;
        Self::json(status, response.body)
    }

    fn plain(status: u16, code: &str, message: &str) -> Self {
        Self::json(
            status,
            json!({ "error": { "code": code, "message": message } }),
        )
    }
}

/// Serves the §26.1 routes over a [`TaroDomain`]. Cloning is cheap.
#[derive(Clone, Debug)]
pub struct HttpApi {
    domain: TaroDomain,
    allow_migrate: bool,
}

impl HttpApi {
    /// An API over `domain`, with `POST /migrate` off.
    pub fn new(domain: TaroDomain) -> Self {
        Self {
            domain,
            allow_migrate: false,
        }
    }

    /// Turns `POST /migrate` on or off.
    #[must_use]
    pub fn allow_migrate(mut self, allow: bool) -> Self {
        self.allow_migrate = allow;
        self
    }

    /// Handles one request. It never fails: every error is a response.
    pub async fn handle(&self, request: HttpRequest) -> HttpResponse {
        let path = request.path.split('?').next().unwrap_or("");
        let path = path.trim_end_matches('/');
        let method = request.method.to_ascii_uppercase();
        match path {
            "/health" => match method.as_str() {
                "GET" | "HEAD" => HttpResponse::json(
                    200,
                    json!({ "status": "ok", "provider": self.domain.provider().provider_type() }),
                ),
                _ => not_allowed("GET"),
            },
            "/migrate" if self.allow_migrate => match method.as_str() {
                "POST" => match self.domain.migrate().await {
                    Ok(()) => HttpResponse::json(200, json!({ "migrated": true })),
                    Err(error) => HttpResponse::error(error.http_status(), &error),
                },
                _ => not_allowed("POST"),
            },
            _ => match path.strip_prefix("/commands/") {
                Some(name) if !name.is_empty() && !name.contains('/') => match method.as_str() {
                    "POST" => self.command(name, &request).await,
                    _ => not_allowed("POST"),
                },
                _ => HttpResponse::plain(404, "NOT_FOUND", &format!("no route for {path}")),
            },
        }
    }

    async fn command(&self, name: &str, request: &HttpRequest) -> HttpResponse {
        let correlation = request
            .header("x-request-id")
            .and_then(parse_uuid)
            .unwrap_or_else(new_v7);
        let result = self.dispatch(name, request, correlation).await;
        let response = match result {
            Ok(body) => HttpResponse::json(200, body),
            Err(error) => {
                let status = match error.code() {
                    crate::error::ErrorCode::CommandHandlerNotRegistered => 404,
                    _ => error.http_status(),
                };
                HttpResponse::error(status, &error)
            }
        };
        response.with_header("x-request-id", correlation.to_string())
    }

    async fn dispatch(
        &self,
        name: &str,
        request: &HttpRequest,
        correlation: crate::kernel::ids::Uuid,
    ) -> Result<Value, FrameworkError> {
        self.domain.guard().validate_command(name)?;
        let data = if request.body.trim().is_empty() {
            Value::Object(serde_json::Map::new())
        } else {
            serde_json::from_str(&request.body).map_err(|error| {
                FrameworkError::command_validation(format!("the body isn't valid JSON: {error}"))
                    .with_context("commandType", name)
            })?
        };
        let command_id = match request.header("x-command-id") {
            Some(id) => parse_uuid(id)
                .ok_or_else(|| FrameworkError::command_validation("x-command-id isn't a UUID"))?,
            None => new_v7(),
        };
        let serialized = SerializedCommand {
            command_id,
            command_type: name.to_owned(),
            causation_id: None,
            correlation_id: correlation,
            data,
            metadata: Metadata::default(),
            timestamp: clock::now(),
        };
        let command = self.domain.commands().deserialize(&serialized)?;
        let result = self.domain.dispatch(command).await?;
        Ok(json!({
            "commandId": result.command_id,
            "status": if result.is_duplicate() { "duplicate" } else { "dispatched" },
            "events": result
                .events
                .iter()
                .map(|e| json!({ "eventId": e.id(), "eventType": e.name() }))
                .collect::<Vec<_>>(),
        }))
    }
}

fn not_allowed(allow: &str) -> HttpResponse {
    HttpResponse::plain(405, "METHOD_NOT_ALLOWED", "that method isn't allowed here")
        .with_header("allow", allow)
}
