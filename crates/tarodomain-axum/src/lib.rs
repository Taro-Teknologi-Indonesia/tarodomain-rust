//! The native HTTP entry point of TaroDomain (spec §26): an [`axum`] router
//! over the core's [`HttpApi`].
//!
//! The router has one catch-all handler. It turns the axum request into an
//! [`HttpRequest`], calls [`HttpApi::handle`] and turns the [`HttpResponse`]
//! back, so the routes, the `x-request-id` handling and the error rendering
//! stay in the core and match the Cloudflare worker.
//!
//! ```no_run
//! # async fn serve(domain: tarodomain::framework::TaroDomain) {
//! let app = tarodomain_axum::router(domain);
//! let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await.unwrap();
//! axum::serve(listener, app).await.unwrap();
//! # }
//! ```

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::http::{HeaderName, HeaderValue, Request, Response, StatusCode, header};
use tarodomain::framework::{HttpApi, HttpRequest, HttpResponse, TaroDomain};

pub use axum;

/// The largest request body the router reads, in bytes.
pub const MAX_BODY_BYTES: usize = 1024 * 1024;

/// A router that serves the §26.1 routes of `domain`. `POST /migrate` is off.
pub fn router(domain: TaroDomain) -> Router {
    router_for(HttpApi::new(domain))
}

/// A router over a configured [`HttpApi`], for example one that
/// [allows `POST /migrate`](HttpApi::allow_migrate).
pub fn router_for(api: HttpApi) -> Router {
    Router::new().fallback(move |request: Request<Body>| {
        let api = api.clone();
        async move { serve(&api, request).await }
    })
}

async fn serve(api: &HttpApi, request: Request<Body>) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let bytes = match to_bytes(body, MAX_BODY_BYTES).await {
        Ok(bytes) => bytes,
        Err(_) => {
            return failure(
                StatusCode::PAYLOAD_TOO_LARGE,
                "the request body is too large",
            );
        }
    };
    let Ok(text) = String::from_utf8(bytes.to_vec()) else {
        return failure(StatusCode::BAD_REQUEST, "the request body is not UTF-8");
    };
    let mut converted = HttpRequest::new(
        parts.method.as_str(),
        parts
            .uri
            .path_and_query()
            .map_or_else(|| parts.uri.path(), |p| p.as_str()),
    )
    .with_body(text);
    for (name, value) in &parts.headers {
        if let Ok(value) = value.to_str() {
            converted = converted.with_header(name.as_str(), value);
        }
    }
    into_response(api.handle(converted).await)
}

fn into_response(response: HttpResponse) -> Response<Body> {
    let mut out = Response::new(Body::from(response.body.to_string()));
    *out.status_mut() = StatusCode::from_u16(response.status).unwrap_or(StatusCode::OK);
    for (name, value) in &response.headers {
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_bytes()),
            HeaderValue::from_str(value),
        ) {
            out.headers_mut().insert(name, value);
        }
    }
    out
}

/// A failure the core never sees, in the core's error shape.
fn failure(status: StatusCode, message: &str) -> Response<Body> {
    let body = serde_json::json!({
        "error": { "code": "INVALID_REQUEST", "message": message }
    });
    let mut out = Response::new(Body::from(body.to_string()));
    *out.status_mut() = status;
    out.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    out
}
