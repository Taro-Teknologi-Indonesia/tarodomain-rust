# Getting started

This guide builds a small domain with TaroDomain for Rust: events, an aggregate, commands, a projection and a saga. Every piece comes from [`examples/bank-account`](../examples/bank-account), which compiles and is tested, so copy from there when in doubt. The crate-level example in [`crates/tarodomain/src/lib.rs`](../crates/tarodomain/src/lib.rs) is a shorter version that runs as a doctest.

## Add the crates

The crates are private (`publish = false`), so depend on them through git, pinned to a tag:

```toml
[dependencies]
tarodomain = { git = "https://github.com/Taro-Teknologi-Indonesia/tarodomain-rust", tag = "v0.1.0" }
tarodomain-local = { git = "https://github.com/Taro-Teknologi-Indonesia/tarodomain-rust", tag = "v0.1.0" }   # tests and local runs
serde = { version = "1", features = ["derive"] }
```

Cargo finds every crate of the workspace in that one repository, so use the same `git` and `tag` for each crate you need (`tarodomain-sqlx`, `tarodomain-cloudflare`, `tarodomain-axum`). Tag a release in this repository first (`git tag v0.1.0 && git push --tags`), and move all your dependencies to the new tag together. The API still changes between phases, which is why there is no crates.io release.

Pick one provider:

| Provider | Crate | Use it for |
|---|---|---|
| In memory | `tarodomain-local` | tests and local development |
| SQLite or PostgreSQL | `tarodomain-sqlx` | a server (pair it with `tarodomain-axum`) |
| Cloudflare D1, Queues, Cron | `tarodomain-cloudflare` | Cloudflare Workers (see [CLOUDFLARE.md](CLOUDFLARE.md)) |

`use tarodomain::prelude::*;` brings in what a domain crate needs. Your domain code never names a provider, so the same domain runs on all three.

## 1. Events

An event is a plain struct. Its stored name is declared, not taken from the struct, so renaming the struct can't orphan stored events:

```rust
use serde::{Deserialize, Serialize};
use tarodomain::prelude::*;

aggregate_id!(pub AccountId);

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountOpened { pub account: AccountId, pub owner: String }
impl Event for AccountOpened { const NAME: &'static str = "AccountOpened"; }

#[derive(Debug, Serialize, Deserialize)]
pub struct MoneyDeposited { pub account: AccountId, pub cents: i64 }
impl Event for MoneyDeposited { const NAME: &'static str = "MoneyDeposited"; }

// Everything that can happen to one aggregate.
event_family!(pub enum AccountEvent { AccountOpened, MoneyDeposited });
```

Put the aggregate's ID in the event if a projection or saga needs it: they see events, not streams. To change an event's shape later, bump `VERSION` and register an upcaster with `.upcaster(…)` on the builder.

## 2. An aggregate

An aggregate embeds an `AggregateCore` and implements `Aggregate`. `apply` is an exhaustive `match` over the family, and it never fails: check invariants in the business method, before `raise`.

```rust
pub struct BankAccount { id: AccountId, core: AggregateCore, balance: i64 }

impl Aggregate for BankAccount {
    type Id = AccountId;
    type Event = AccountEvent;
    type Snapshot = i64;                 // what a snapshot stores
    const TYPE: &'static str = "BankAccount";

    fn new(id: AccountId) -> Self { Self { id, core: AggregateCore::new(), balance: 0 } }
    fn id(&self) -> AccountId { self.id }
    fn core(&self) -> &AggregateCore { &self.core }
    fn core_mut(&mut self) -> &mut AggregateCore { &mut self.core }

    fn apply(&mut self, event: &AccountEvent) {
        match event {
            AccountEvent::AccountOpened(_) => {}
            AccountEvent::MoneyDeposited(e) => self.balance += e.cents,
        }
    }
    fn snapshot_state(&self) -> i64 { self.balance }
    fn restore_snapshot_state(&mut self, balance: i64) { self.balance = balance }
}

impl BankAccount {
    pub fn deposit(&mut self, cents: i64) -> Result<()> {
        if cents <= 0 {
            // A user-defined domain error: a code, a category, a message.
            return Err(FrameworkError::custom("INVALID_AMOUNT", ErrorCategory::Domain, "not positive"));
        }
        self.raise(MoneyDeposited { account: self.id, cents });
        Ok(())
    }
}
```

`raise` applies the event, bumps the version and queues it for the next save, with an envelope built from the execution context in scope (correlation and causation IDs, §28).

When a refusal should start something else, such as undoing a step in a saga, raise a rejection event instead of returning an error. The bank's `WithdrawalRejected` does that.

## 3. Commands and handlers

```rust
use tarodomain::command::{CommandContext, CommandHandler};

#[derive(Clone, Debug, Serialize, Deserialize)]   // registered commands must be Clone
pub struct Deposit { pub account: AccountId, pub cents: i64 }
impl Command for Deposit { const NAME: &'static str = "Deposit"; }

pub struct DepositHandler { pub context: CommandContext }

#[async_trait]
impl CommandHandler<Deposit> for DepositHandler {
    async fn handle(&self, command: &CommandMessage<Deposit>) -> Result<()> {
        let repository = self.context.repository::<BankAccount>()?;
        let mut account = repository.load(command.account).await?;   // AGGREGATE_NOT_FOUND if missing
        account.deposit(command.cents)?;
        repository.save(&mut account).await
    }
}
```

The handler gets its repository from the `CommandContext`, which builds it on the bus of the pipeline that runs the command (strong or eventual, below). The framework retries a handler that fails with a retryable error such as `OPTIMISTIC_CONCURRENCY` (a fresh handler and a fresh load each time), and skips a command ID it has already run.

## 4. A projection

A projection is a read model fed by events. It owns its storage; the framework only delivers events, once each, and remembers which it delivered. Register one handler per event:

```rust
use std::sync::Arc;
use tarodomain::projection::{Projection, ProjectionRoutes};

pub struct BalanceProjection { /* your read-model store */ }

impl BalanceProjection {
    async fn on_deposited(self: Arc<Self>, event: Recorded<MoneyDeposited>) -> Result<()> {
        // `event` derefs to MoneyDeposited; `event.envelope` has the ID and time.
        // Write idempotently: a crash between this write and the processed-event
        // mark replays the event (§14.3).
        Ok(())
    }
}

impl Projection for BalanceProjection {
    fn name(&self) -> &str { "Balances" }
    fn routes(routes: &mut ProjectionRoutes<Self>) {
        routes.on::<MoneyDeposited, _, _>(Self::on_deposited);
    }
}
```

Events a projection has no route for are skipped. Implement `on_rebuild_start` to clear the read model before a rebuild (`ProjectionRebuilder`).

## 5. A saga

A saga (`ProcessManager`) coordinates several aggregates by reacting to events with commands. Its handlers are **synchronous and pure**: they change `ctx.state` and return the commands to send. Nothing happens inside them, so replaying a saga has no side effects.

```rust
use tarodomain::saga::{ProcessManager, SagaCtx, SagaRoutes, SagaTimeout};

impl ProcessManager for TransferSaga {
    const SAGA_TYPE: &'static str = "TransferSaga";
    type State = TransferState;          // Serialize + Deserialize + Default

    fn routes(r: &mut SagaRoutes<Self>) {
        r.starts_when::<TransferRequested>(Self::start);          // creates the saga
        r.continue_when::<MoneyWithdrawn>(Self::withdrawn);
        r.compensate_when::<WithdrawalRejected>(Self::rejected);  // failure paths
        r.compensate_when::<SagaTimeout>(Self::timed_out);
        r.completed_when::<TransferCompleted>(|ctx, _| { Ok(vec![]) });
        // Compensations are named; the saga stores the names it has registered.
        r.define_compensation("refund", |state: &TransferState| vec![/* commands */]);
    }
}
```

In a handler, `ctx.register_compensation("refund")` pushes a step to undo, and `ctx.compensation_commands()?` returns the undo commands in reverse order. A saga's ID is the correlation ID of the events that start it. The full transfer saga (withdraw, deposit, complete, undo on failure or timeout) is [`saga.rs`](../examples/bank-account/src/saga.rs).

## 6. Build and dispatch

```rust
use tarodomain::framework::{SagaOptions, TaroDomain};

let app = TaroDomain::builder()
    .use_provider(Arc::new(LocalProvider::new()))
    .consistency(ConsistencyMode::Strong, None)               // the default for every command
    .aggregate::<BankAccount>(|a| a.event_sourced().snapshot_every(100))
    .command::<Deposit, _>(|_, context| DepositHandler { context }, None)
    .projection(|_| Ok(Arc::new(BalanceProjection { /* … */ })))
    .saga::<TransferSaga>(SagaOptions::with_timeout(Duration::from_secs(300)))
    .build()
    .await?;                                                  // validates the wiring (§29)

app.dispatch(Deposit { account, cents: 500 }).await?;
```

`build()` checks that every command, event and route lines up and fails with a configuration error, or warns, before anything runs. `register_bank` in [`examples/bank-account/src/lib.rs`](../examples/bank-account/src/lib.rs) is a complete builder.

To trace one request through commands, events and sagas, give it a correlation ID (`CommandMessage::new(cmd).with_correlation_id(id)`) and read it back with `CorrelationTrace::trace` (see the bank's `main.rs`).

## Strong or eventual consistency

The mode is chosen for the whole app with `.consistency(mode, transport)`, and overridden for one command with the last argument of `.command(…)`:

| | Strong | Eventual |
|---|---|---|
| Events are delivered | Inside the command's unit of work, on an in-process bus | After the commit, by the provider's bus (a queue on Cloudflare, a wake-up signal on SQL) |
| Projections and sagas | Updated when `dispatch` returns. A failure rolls the events back too | Updated a moment later. A failure doesn't undo the command |
| Suits | Short chains and read-your-writes | Long sagas and high throughput |

In eventual mode the events are already stored when the command returns, and a catch-up job (`app.run_catch_up()`, or the `Worker` loop, or a Cron Trigger) delivers anything the bus missed.

## Run it

| You want | Use |
|---|---|
| A test | `LocalProvider` and `dispatch` directly, as in the doctest and [`bank-account`'s `main.rs`](../examples/bank-account/src/main.rs) (`cargo run -p bank-account`) |
| A server | `SqlxProvider` with `tarodomain_axum::router(app)` and a `Worker` for the background jobs: [`examples/bank-account-server`](../examples/bank-account-server) |
| Cloudflare | [CLOUDFLARE.md](CLOUDFLARE.md) |

Test your own provider-agnostic code against `LocalProvider`, and put anything provider-specific (a read model's SQL) behind a small trait, as the bank's `BalanceStore` does, with an in-memory implementation for tests.

## Where to go next

- [The specification](../spec/README.md) for the behaviour behind every piece
- [The implementation plan](IMPLEMENTATION_PLAN.md) for the design decisions, where Rust differs from the spec, and the risks
