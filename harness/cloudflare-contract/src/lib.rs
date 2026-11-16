//! Runs the `tarodomain-testing` contract suites against
//! `CloudflareProvider` on a real D1, inside workerd (Step 10.8).
//!
//! `POST /suites/{name}` runs one suite and answers `ok`. Each case inside a
//! suite gets a provider over the same D1 database, emptied first. A failed
//! assertion panics; the panic message is written to the log by the panic
//! hook, and the request fails. `scripts/cloudflare-contract.sh` drives it.
#![cfg(target_arch = "wasm32")]

use tarodomain_cloudflare::migrations::clear_all;
use tarodomain_cloudflare::provider::{Bindings, CloudflareProvider};
use worker::{Context, Env, Request, Response, Result, event};

/// Every suite, by the name used in the URL.
const SUITES: &[&str] = &[
    "event_store_contract",
    "snapshot_contract",
    "saga_store_contract",
    "idempotency_contract",
    "dead_letter_contract",
    "unit_of_work_contract",
    "concurrency_contract",
    "provider_contract",
];

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    console_error_panic_hook::set_once();
    let path = req.path();
    if path == "/suites" {
        return Response::from_json(&SUITES);
    }
    let Some(name) = path.strip_prefix("/suites/") else {
        return Response::error("not found", 404);
    };
    if req.method() != worker::Method::Post {
        return Response::error("use POST", 405);
    }
    if !SUITES.contains(&name) {
        return Response::error(format!("no suite named {name:?}"), 404);
    }

    let bindings = Bindings::new().d1("DB").queue("EVENTS");
    let provider = CloudflareProvider::new(&env, &bindings)
        .map_err(|error| worker::Error::RustError(error.to_string()))?;
    tarodomain::adapters::Provider::initialize(&provider)
        .await
        .map_err(|error| worker::Error::RustError(error.to_string()))?;

    // A new provider over an empty database for each case.
    let factory = || {
        let provider = provider.clone();
        async move {
            clear_all(provider.db())
                .await
                .expect("emptying the database");
            provider
        }
    };
    match name {
        "event_store_contract" => tarodomain_testing::event_store_contract(factory).await,
        "snapshot_contract" => tarodomain_testing::snapshot_contract(factory).await,
        "saga_store_contract" => tarodomain_testing::saga_store_contract(factory).await,
        "idempotency_contract" => tarodomain_testing::idempotency_contract(factory).await,
        "dead_letter_contract" => tarodomain_testing::dead_letter_contract(factory).await,
        "unit_of_work_contract" => tarodomain_testing::unit_of_work_contract(factory).await,
        "concurrency_contract" => tarodomain_testing::concurrency_contract(factory).await,
        "provider_contract" => tarodomain_testing::provider_contract(factory).await,
        _ => unreachable!("checked against SUITES"),
    }
    Response::ok("ok")
}
