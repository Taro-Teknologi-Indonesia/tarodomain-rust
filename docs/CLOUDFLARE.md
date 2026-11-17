# Running on Cloudflare Workers

`tarodomain-cloudflare` runs a TaroDomain app as a Worker: **D1** stores the events, snapshots, sagas and your read models, a **Queue** delivers events in eventual mode, and **Cron Triggers** run catch-up, saga timeouts and cleanup. The domain code is the same as everywhere else ([GETTING_STARTED.md](GETTING_STARTED.md)); only the entry point differs.

[`examples/bank-account-worker`](../examples/bank-account-worker) is the working template: copy its `src/lib.rs` and `wrangler.toml`.

## Prerequisites

- Node.js, for `wrangler`
- `cargo install worker-build`
- the `wasm32-unknown-unknown` target (`rust-toolchain.toml` adds it)

No Cloudflare account is needed for `npx wrangler dev`: it runs workerd with a local D1 and queue.

## Project setup

A Worker crate is a `cdylib` that depends on your domain crate and on `tarodomain-cloudflare` (from git, pinned to a tag: see [Getting started](GETTING_STARTED.md#add-the-crates)):

```toml
[lib]
crate-type = ["cdylib", "rlib"]

[dependencies]
tarodomain = { git = "…", tag = "v0.1.0" }
tarodomain-cloudflare = { git = "…", tag = "v0.1.0" }
tokio = { version = "1", features = ["sync"] }      # OnceCell

[target.'cfg(target_arch = "wasm32")'.dependencies]
worker = { version = "0.9", default-features = false, features = ["d1", "queue"] }
console_error_panic_hook = "0.1"
```

Keep your domain in its own crate, as the bank does, and check it still builds for Workers: `cargo check --target wasm32-unknown-unknown -p your-domain`. The core never uses tokio's I/O or timers, so it builds there; if your domain pulls a crate that needs `getrandom` or a system clock, give it the JavaScript backend.

## Bindings

`wrangler.toml` declares the D1 database, the queue (as producer and consumer) and the crons. The names below are what the example's Rust code looks up:

```toml
name = "my-worker"
main = "build/worker/shim.mjs"
compatibility_date = "2025-11-01"

[build]
command = "cargo install -q worker-build && worker-build --release"

[vars]
CONSISTENCY = "eventual"            # or "strong"

[[d1_databases]]
binding = "DB"
database_name = "my-db"
database_id = "…"                   # from `npx wrangler d1 create my-db`

[[queues.producers]]
binding = "EVENTS"
queue = "my-events"

[[queues.consumers]]
queue = "my-events"
max_batch_size = 10
max_batch_timeout = 2
max_retries = 5
dead_letter_queue = "my-events-dlq"

[triggers]
crons = ["* * * * *", "0 3 * * *"]
```

In Rust, `Bindings::new().d1("DB").queue("EVENTS")` names the same bindings. Leave the queue out for a strong-only app.

## The entry point

Build the framework once per isolate (a `OnceCell`), then hand each Worker event to `CloudflareWorker`:

```rust
let bindings = Bindings::new().d1("DB").queue("EVENTS");
let provider = Arc::new(CloudflareProvider::new(env, &bindings)?);
provider.initialize().await?;                      // applies any missing migrations, once

let domain = TaroDomain::builder()
    .use_provider(provider.clone())
    .consistency(mode, None)
    /* … your aggregates, commands, projections, sagas … */
    .build()
    .await?;
let worker = CloudflareWorker::new(domain, &provider);

#[event(fetch)]     async fn fetch(req, env, _ctx)     { worker.fetch(req).await }
#[event(queue)]     async fn queue(batch, env, _ctx)   { worker.queue(batch).await }
#[event(scheduled)] async fn scheduled(event, env, _ctx) { worker.scheduled(event).await }
```

[`examples/bank-account-worker/src/lib.rs`](../examples/bank-account-worker/src/lib.rs) has the full version, including reading `CONSISTENCY` from `[vars]`.

`fetch` serves the core's HTTP API: `POST /commands/{name}` with a JSON body, and `GET /health`. Anything else is yours: route it before calling `worker.fetch` (the example serves `GET /balances` that way). `POST /migrate` exists but is off; turn it on with `.allow_migrate(true)` only behind your own authentication.

## Migrations

The framework's tables (`tarodomain_events`, `_streams`, `_snapshots`, `_subscriptions`, `_processed_events`, `_sagas`, `_dead_letters`, `_processed_commands`, `_locks`) come from migrations embedded in the crate and tracked in `schema_migrations`. `provider.initialize()` applies the missing ones on the first request of each isolate, so deploying needs no separate step.

Your own tables, such as read models, aren't part of that. Create them idempotently in the same place (`CREATE TABLE IF NOT EXISTS …`, as the example's `D1BalanceStore::init` does), or manage them with `wrangler d1 migrations` if you prefer. A table you change later needs a migration you write.

## Strong and eventual mode

| | `strong` | `eventual` (default) |
|---|---|---|
| Delivery | In process, inside the command's D1 batch | Events go to the Queue after the commit; the queue consumer runs the projections and sagas |
| `dispatch` returns | After projections and sagas ran, and everything committed together | After the events are stored |
| A handler fails | The whole batch rolls back | The queue retries with backoff (`RetryPolicy`); a non-retryable failure goes to the dead-letter table, and Cloudflare's DLQ is the last resort |

Set it with `.consistency(mode, None)`, or for one command with the last argument of `.command(…)`. The transport defaults to the in-process bus for strong and the queue for eventual (`Some("queue")` or `Some("in_memory")` to override).

D1 has no interactive transactions, so a command's writes are staged and sent as **one `batch`** at commit. Two consequences:

- Reads inside a command see the database as it was before the command began, plus what the framework itself wrote in the same unit (stream versions and events, sagas, idempotency marks). Your own read models don't get that overlay.
- Strong mode with a long saga builds one large batch. See [D1 limits](#d1-limits).

## Read models

A read model is your own D1 tables. Write to them **through `provider.db()`**, so that in strong mode the writes join the command's batch and commit or roll back with the events:

```rust
let db: D1Db = provider.db().clone();
db.write(vec![Statement::new(
    "INSERT INTO balances (account, cents) VALUES (?, ?) \
     ON CONFLICT (account) DO UPDATE SET cents = cents + excluded.cents",
    vec![json!(account), json!(delta)],
)]).await?;
```

Rules for a read model that has to be right:

1. **Write idempotently** (§14.3): events can be delivered twice. Keep a table of applied event IDs and guard the change with it in the same batch, as the example's `apply` does.
2. **Don't read-then-write.** Reads inside a unit don't see the unit's own pending writes, so use upserts and increments (`cents = cents + ?`).
3. Read with `db.query(…)` / `db.query_one(…)` outside commands (for example in your `fetch` route).

Writing to D1 with the `worker` crate directly bypasses the unit of work: the write happens at once and a rollback leaves it behind.

## Cron schedules

Two crons, matched by their expression:

| Cron | Does |
|---|---|
| any expression other than the cleanup one, e.g. `* * * * *` | Catch-up of every projection and saga handler from the event store, then saga timeouts |
| `0 3 * * *` (the default cleanup cron) | Retention (§14, §21): deletes processed-command records older than 7 days, and processed-event markers and dead letters older than 30 days |

If you pick a different cleanup schedule, tell the worker: `CloudflareWorker::new(…).cleanup_cron("0 4 * * 0")`, and put the same expression in `wrangler.toml`. Overlapping runs are guarded by a lease in `tarodomain_locks`: a run that finds the lease held skips.

Catch-up is also your safety net in eventual mode: an event that never reached the queue is delivered on the next run. Every minute is the finest Cron Trigger schedule.

Test the scheduled handler locally with `npx wrangler dev --test-scheduled`, then `curl "http://localhost:8787/__scheduled?cron=*+*+*+*+*"`.

## D1 limits

D1 limits the bound parameters per statement, statements per batch, queries per Worker invocation, the database size and the response size. The numbers differ by plan and change over time, so check Cloudflare's [D1 limits](https://developers.cloudflare.com/d1/platform/limits/) before sizing anything. What this means in practice:

- The provider splits multi-row inserts to stay under the bound-parameter limit (unit-tested). Nothing has yet run a very large batch against real D1.
- A strong-mode chain, such as a saga that dispatches several commands, becomes one batch. **Use strong mode for short chains and run long sagas eventual.**
- Keep read-model writes per event small (one or two statements).
- A Worker invocation has its own limits too (subrequests, CPU time). Catch-up processes events in batches for that reason (`catch_up_batch_size` on the builder).

## Run locally

```sh
cd examples/bank-account-worker      # or your Worker
npx wrangler dev
```

The example's README has a `curl` walkthrough. `wrangler dev` keeps a local copy of D1 and the queue under `.wrangler/`.

`scripts/cloudflare-contract.sh` runs every provider contract suite against D1 itself (inside workerd). The same suites run in plain `cargo test` on SQLite behind the same statement layer, so the day-to-day loop needs no Node.js.

## Deploy

```sh
npx wrangler d1 create my-db                 # paste the database_id into wrangler.toml
npx wrangler queues create my-events
npx wrangler queues create my-events-dlq
npx wrangler deploy
```

The first request creates the framework's tables. After deploying, `GET /health` should answer, and a `POST /commands/{name}` should return the dispatch result.

Release builds use `worker-build --release`. The example's `Cargo.toml` turns on `wasm-opt -Oz`, which gives a smaller module and faster cold starts (the example's README records its size).

## Troubleshooting

- **A projection stays behind in eventual mode.** Check the queue consumer's logs and the dead-letter table (`tarodomain_dead_letters`). The next catch-up run delivers anything missing.
- **`INVALID_CONFIGURATION` at start-up.** `build()` found a wiring problem (a command without a handler, a saga without a route). The error's context says which.
- **A write from a handler isn't rolled back.** It bypassed `provider.db()`.
- **A task spawned inside a handler loses the trace.** The execution context is task-local: pass it on explicitly (`ExecutionContext::scope`). The same applies to futures given to `ctx.wait_until`.
