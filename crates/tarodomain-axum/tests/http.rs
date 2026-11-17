//! The axum router, driven through `tower::ServiceExt::oneshot`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tarodomain::adapters::ConsistencyMode;
use tarodomain::async_trait;
use tarodomain::command::CommandHandler;
use tarodomain::domain::{Command, CommandMessage};
use tarodomain::error::Result;
use tarodomain::framework::{HttpApi, TaroDomain};
use tarodomain_axum::{MAX_BODY_BYTES, router, router_for};
use tarodomain_local::LocalProvider;
use tower::ServiceExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Ping {
    n: u32,
}

impl Command for Ping {
    const NAME: &'static str = "Ping";
}

struct PingHandler(Arc<AtomicU32>);

#[async_trait]
impl CommandHandler<Ping> for PingHandler {
    async fn handle(&self, message: &CommandMessage<Ping>) -> Result<()> {
        self.0.fetch_add(message.n, Ordering::SeqCst);
        Ok(())
    }
}

async fn domain(pings: Arc<AtomicU32>) -> TaroDomain {
    TaroDomain::builder()
        .use_provider(Arc::new(LocalProvider::new()))
        .consistency(ConsistencyMode::Strong, None)
        .command::<Ping, _>(move |_, _| PingHandler(pings.clone()), None)
        .build()
        .await
        .unwrap()
}

async fn app() -> (Router, Arc<AtomicU32>) {
    let pings = Arc::new(AtomicU32::new(0));
    (router(domain(pings.clone()).await), pings)
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, axum::http::HeaderMap, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let (parts, body) = response.into_parts();
    let bytes = body.collect().await.unwrap().to_bytes();
    let json = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (parts.status, parts.headers, json)
}

fn post(path: &str, body: impl Into<Body>) -> Request<Body> {
    Request::post(path).body(body.into()).unwrap()
}

#[tokio::test]
async fn health_reports_the_provider() {
    let (app, _) = app().await;
    let (status, _, body) = send(&app, Request::get("/health").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
    assert_eq!(body["provider"], "local");
}

#[tokio::test]
async fn a_command_runs_and_the_request_id_comes_back() {
    let (app, pings) = app().await;
    let id = "0190a0b0-0000-7000-8000-000000000001";
    let request = Request::post("/commands/Ping")
        .header("x-request-id", id)
        .body(Body::from(json!({ "n": 3 }).to_string()))
        .unwrap();
    let (status, headers, _) = send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["x-request-id"], id);
    assert_eq!(headers["content-type"], "application/json");
    assert_eq!(pings.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn errors_use_the_core_rendering() {
    let (app, _) = app().await;
    let (status, _, body) = send(&app, post("/commands/Nope", "{}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"]["code"].is_string());

    let (status, _, body) = send(&app, post("/commands/Ping", "not json")).await;
    assert!(status.is_client_error(), "{status}");
    assert!(body["error"]["code"].is_string());

    let (status, _, _) = send(
        &app,
        Request::get("/commands/Ping").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    let (status, _, _) = send(
        &app,
        Request::get("/elsewhere").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn a_query_string_is_ignored() {
    let (app, pings) = app().await;
    let (status, _, _) = send(
        &app,
        post("/commands/Ping?debug=1", json!({ "n": 1 }).to_string()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pings.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn bodies_that_are_too_large_or_not_text_are_rejected() {
    let (app, pings) = app().await;
    let (status, _, body) =
        send(&app, post("/commands/Ping", vec![b'a'; MAX_BODY_BYTES + 1])).await;
    assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body["error"]["code"], "INVALID_REQUEST");

    let (status, _, _) = send(&app, post("/commands/Ping", vec![0xff, 0xfe])).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(pings.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn migrate_is_off_unless_the_api_allows_it() {
    let (app, _) = app().await;
    let (status, _, _) = send(&app, post("/migrate", "")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let open = router_for(HttpApi::new(domain(Arc::default()).await).allow_migrate(true));
    let (status, _, body) = send(&open, post("/migrate", "")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["migrated"], true);
}
