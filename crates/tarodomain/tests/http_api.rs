//! The HTTP API (spec §26.1) on the local provider.

mod support;

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tarodomain::adapters::ConsistencyMode;
use tarodomain::async_trait;
use tarodomain::command::CommandHandler;
use tarodomain::domain::{Command, CommandMessage};
use tarodomain::error::{FrameworkError, Result};
use tarodomain::framework::{HttpApi, HttpRequest, TaroDomain};
use tarodomain_local::LocalProvider;

use support::bank::{
    BankAccount, Counters, Deposit, DepositHandler, OpenAccount, OpenAccountHandler,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Boom {}

impl Command for Boom {
    const NAME: &'static str = "Boom";
}

struct BoomHandler;

#[async_trait]
impl CommandHandler<Boom> for BoomHandler {
    async fn handle(&self, _: &CommandMessage<Boom>) -> Result<()> {
        Err(FrameworkError::event_store_error("disk /dev/sda1 is full")
            .with_context("secret", "hunter2")
            .with_source(std::io::Error::other("connection string with password")))
    }
}

async fn api() -> HttpApi {
    let domain = TaroDomain::builder()
        .use_provider(Arc::new(LocalProvider::new()))
        .consistency(ConsistencyMode::Strong, None)
        .aggregate::<BankAccount>(|a| a)
        .command::<OpenAccount, _>(|_, context| OpenAccountHandler { context }, None)
        .command::<Deposit, _>(
            |_, context| DepositHandler {
                context,
                counters: Arc::new(Counters::default()),
            },
            None,
        )
        .command::<Boom, _>(|_, _| BoomHandler, None)
        .build()
        .await
        .unwrap();
    HttpApi::new(domain)
}

fn post(path: &str, body: Value) -> HttpRequest {
    HttpRequest::new("POST", path).with_body(body.to_string())
}

const ACCOUNT: &str = "0190a000-0000-7000-8000-000000000001";

fn open_body() -> Value {
    json!({ "account": ACCOUNT, "owner": "Ann" })
}

#[tokio::test]
async fn a_command_is_dispatched_and_its_events_listed() {
    let api = api().await;
    let response = api.handle(post("/commands/OpenAccount", open_body())).await;
    assert_eq!(response.status, 200, "{}", response.body);
    assert_eq!(response.body["status"], "dispatched");
    assert_eq!(response.body["events"][0]["eventType"], "AccountOpened");
    assert_eq!(response.headers["content-type"], "application/json");
    assert!(response.headers.contains_key("x-request-id"));
}

#[tokio::test]
async fn the_request_id_is_the_correlation_id() {
    let api = api().await;
    let id = "0190a000-0000-7000-8000-0000000000aa";
    let request = post("/commands/OpenAccount", open_body()).with_header("X-Request-Id", id);
    let response = api.handle(request).await;
    assert_eq!(response.headers["x-request-id"], id);
    let stored = api.handle(HttpRequest::new("GET", "/health")).await;
    assert_eq!(stored.status, 200);
}

#[tokio::test]
async fn a_repeated_command_id_is_a_duplicate() {
    let api = api().await;
    let command_id = "0190a000-0000-7000-8000-0000000000bb";
    let request =
        post("/commands/OpenAccount", open_body()).with_header("x-command-id", command_id);
    assert_eq!(
        api.handle(request.clone()).await.body["status"],
        "dispatched"
    );
    let again = api.handle(request).await;
    assert_eq!(again.status, 200);
    assert_eq!(again.body["status"], "duplicate");
    assert_eq!(again.body["events"], json!([]));
}

#[tokio::test]
async fn a_domain_error_shows_its_context() {
    let api = api().await;
    // The account doesn't exist.
    let body = json!({ "account": ACCOUNT, "cents": 5 });
    let response = api.handle(post("/commands/Deposit", body)).await;
    assert_eq!(response.status, 400, "{}", response.body);
    assert_eq!(response.body["error"]["code"], "AGGREGATE_NOT_FOUND");
    assert_eq!(
        response.body["error"]["context"]["aggregateType"],
        "BankAccount"
    );
}

#[tokio::test]
async fn an_infrastructure_error_hides_context_and_source() {
    let api = api().await;
    let response = api.handle(post("/commands/Boom", json!({}))).await;
    assert_eq!(response.status, 500);
    let text = response.body.to_string();
    assert!(response.body["error"].get("context").is_none(), "{text}");
    assert!(
        !text.contains("hunter2") && !text.contains("password"),
        "{text}"
    );
}

#[tokio::test]
async fn bad_input_is_a_400() {
    let api = api().await;
    let not_json = HttpRequest::new("POST", "/commands/OpenAccount").with_body("{oops");
    assert_eq!(api.handle(not_json).await.status, 400);

    let wrong_fields = post("/commands/OpenAccount", json!({ "account": 3 }));
    let response = api.handle(wrong_fields).await;
    assert_eq!(response.status, 400);
    assert_eq!(response.body["error"]["code"], "COMMAND_VALIDATION");

    let bad_id = post("/commands/OpenAccount", open_body()).with_header("x-command-id", "nope");
    assert_eq!(api.handle(bad_id).await.status, 400);
}

#[tokio::test]
async fn unknown_commands_and_routes_are_404_and_wrong_methods_405() {
    let api = api().await;
    assert_eq!(
        api.handle(post("/commands/Nope", json!({}))).await.status,
        404
    );
    assert_eq!(api.handle(post("/nowhere", json!({}))).await.status, 404);
    assert_eq!(api.handle(post("/commands/", json!({}))).await.status, 404);
    assert_eq!(
        api.handle(post("/commands/a/b", json!({}))).await.status,
        404
    );

    let response = api
        .handle(HttpRequest::new("GET", "/commands/OpenAccount"))
        .await;
    assert_eq!(response.status, 405);
    assert_eq!(response.headers["allow"], "POST");
    assert_eq!(api.handle(post("/health", json!({}))).await.status, 405);
}

#[tokio::test]
async fn health_and_query_strings() {
    let api = api().await;
    let response = api
        .handle(HttpRequest::new("get", "/health?verbose=1"))
        .await;
    assert_eq!(response.status, 200);
    assert_eq!(
        response.body,
        json!({ "status": "ok", "provider": "local" })
    );
}

#[tokio::test]
async fn migrate_is_off_unless_allowed() {
    let api = api().await;
    assert_eq!(api.handle(post("/migrate", json!({}))).await.status, 404);

    let api = api.allow_migrate(true);
    let response = api.handle(post("/migrate", json!({}))).await;
    assert_eq!(
        (response.status, &response.body),
        (200, &json!({ "migrated": true }))
    );
    assert_eq!(
        api.handle(HttpRequest::new("GET", "/migrate")).await.status,
        405
    );
}
