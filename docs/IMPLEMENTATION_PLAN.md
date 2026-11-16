# Implementation plan: TaroDomain for Rust

Status: Phases 0–4 done. Phases 0 and 1 are green on [GitHub CI](https://github.com/Taro-Teknologi-Indonesia/tarodomain-rust/actions); Phases 2–4 pass every check locally and haven't been pushed yet. Next is Step 5.1. Updated 2026-11-16: Cloudflare provider added as Phase 10; publishing, macros and the local container engine decided. Targets the [TaroDomain spec v1.0](../spec/README.md) at commit `a9243e3` (the `spec/` submodule). The [Dart implementation](https://github.com/Taro-Teknologi-Indonesia/tarodomain-dart) is the reference port. It pins the older spec commit `983d2cb`, and its [plan](https://github.com/Taro-Teknologi-Indonesia/tarodomain-dart/blob/main/docs/IMPLEMENTATION_PLAN.md) records the design problems this plan reuses.

## How to use this plan

- Work one step at a time, in order. Each step creates or edits **1–5 files, tests included**. Edits to `lib.rs`, `mod.rs` and `Cargo.toml` count. If a step turns out to need more files, split it here first, then write the code.
- Tick a step's boxes as each item lands, and update the status line above when a phase finishes.
- Unit tests go inline (`#[cfg(test)] mod tests`). A test that needs the local provider is an integration test under `crates/tarodomain/tests/` (see Risks: dev-dependency cycle).
- If the code has to depart from the spec, add a row to [Rust design decisions](#rust-design-decisions) in the same step.

### Definition of done (every step)

- [ ] Only the files listed for the step were touched
- [ ] `cargo fmt --all --check`
- [ ] `cargo clippy --workspace --all-targets -- -D warnings`
- [ ] `cargo test --workspace`
- [ ] `cargo check --target wasm32-unknown-unknown` on the crates in CI's wasm check, so everything that runs on Cloudflare Workers still builds for it (Step 0.4)
- [ ] Every new `pub` item has rustdoc (`missing_docs` warns, and CI denies warnings)
- [ ] The step's boxes are ticked here

## Goal

1. **`tarodomain`**: a pure-Rust implementation of the spec's platform-agnostic parts (§2–23, §25, §28–29). It depends on no database driver, HTTP framework or cloud SDK, and builds both natively and for `wasm32-unknown-unknown` (Cloudflare Workers).
2. **`tarodomain-local`**: the spec's LocalProvider (§24), in memory, for tests and local development.
3. **`tarodomain-cloudflare`**: the spec's CloudflareProvider (§24.2–24.3) and CloudflareWorker (§26.1) on [workers-rs](https://github.com/cloudflare/workers-rs). D1 stores everything, Queues deliver events in eventual mode, and Cron Triggers run catch-up, saga timeouts and cleanup. This is the main deployment target.
4. **`tarodomain-sqlx`**: a SQL provider on [sqlx](https://github.com/launchbadge/sqlx) for SQLite and PostgreSQL, for servers that don't run on Cloudflare. It is the Rust counterpart of the Dart Serverpod module.
5. **`tarodomain-axum`**: the native entry points (§26): an axum router over the core's HTTP API plus a background worker loop.

## Decisions

- **Async runtime: tokio, for `task_local!` and `sync` only.** The core uses `tokio::task_local!` for request-scoped context (§5.5). It uses no tokio I/O and never calls `tokio::spawn` or `tokio::time`. It sleeps through the `Sleeper` trait, which the provider supplies, because tokio's timers don't work inside a Cloudflare Worker.
- **Two targets: native and `wasm32-unknown-unknown`.** The core, the testing crate and the bank example's domain also build for Cloudflare Workers, and CI checks that on every push (Step 0.4). On that target the core turns on the JavaScript backends of `uuid` and `chrono`, so UUIDv7 and `Utc::now()` work inside a Worker. The core doesn't use `rand`, so `getrandom` isn't in its wasm build (Steps 1.3–1.4).
- **Async traits: `async-trait`, with `Send` futures.** Providers hand out adapters as `Arc<dyn Trait>`. Native `async fn` in traits can't be called through `dyn`, so `async-trait` boxes the futures. Revisit this when dyn-compatible async traits are stable. workers-rs handles aren't `Send`, so the Cloudflare adapters wrap them instead of the core dropping its `Send` bounds (see the design table).
- **IDs come from the `uuid` crate** (`v7`, `serde` features). The framework writes no UUID code of its own except the deterministic UUIDv7 derivation (step 1.3).
- **Time is `chrono::DateTime<Utc>`**, behind an injectable `Clock` so tests can fix it. Code that can't take a clock as an argument, such as `raise` and `CommandMessage::from`, reads the clock in scope (`kernel::clock::now()`, task-local like the execution context), which falls back to the system clock (Step 2.5). Envelope timestamps are cut to milliseconds, the precision every store keeps.
- **JSON is `serde_json::Value`.** Event `data`, `metadata`, snapshot and saga state go through serde.
- **Errors are one `FrameworkError` type** with an `ErrorCode` enum. There is no class per code (§22, and the design table below).
- **Publishing: private (decided 2026-11-16).** The crates keep `publish = false`, as in the Dart port, and other projects use a git dependency pinned to a tag. The API will keep changing while the phases land, and a crates.io release can only be yanked, never deleted. Revisit once the contract suites pass on every provider and one application runs on the framework; publishing then needs only `publish = true` and crates.io metadata.
- **Macros: `macro_rules!` only, no proc-macro crate (decided 2026-11-16).** `event_family!`, `aggregate_id!` and `provider_contract_tests!` cover the repetitive parts. A `#[derive(Event)]` would save three lines per event, and it would invite a default name taken from the struct, so renaming a struct would silently orphan its stored events. The explicit `const NAME` prevents that. Proc macros would also add `syn` to every domain crate's build. Revisit if the bank example (Phase 9) shows boilerplate the declarative macros can't remove.
- **Edition 2024, MSRV 1.85**, stable toolchain (`rust-toolchain.toml`).
- **Local containers: Kiri, not Docker Desktop (decided 2026-11-16).** The only container the project needs is PostgreSQL for the sqlx contract tests (Phase 11). The repo ships a plain `compose.yaml`, so any engine that speaks the Docker Engine API runs it. On the dev machine that engine is [Kiri](https://runkiri.com) (`docker --context kiri compose up -d`). CI uses a GitHub service container. Cloudflare development needs no containers: `wrangler dev` runs workerd and a local D1.
- **Out of scope:** the AWS provider (DynamoDB, SQS and Lambda: §24.4, §26.2, §27.3). `Provider::provider_type()` returns a string, so it can be added later as its own crate. The §26.3 template generators are replaced by `examples/bank-account-worker`, which is the template to copy.

## Workspace layout

| Path | Kind | Contents | Phase |
|---|---|---|---|
| `spec/` | git submodule | The specification, pinned | 0 |
| `crates/tarodomain` | library | Core framework | 0–9 |
| `crates/tarodomain-local` | library | In-memory provider | 4 |
| `crates/tarodomain-testing` | library | Contract test suites that every provider must pass | 4 |
| `crates/tarodomain-cloudflare` | library | Cloudflare provider and Worker entry points: D1, Queues, Cron Triggers | 10 |
| `harness/cloudflare-contract` | Worker | Runs the contract suites inside workerd against a local D1 | 10 |
| `crates/tarodomain-sqlx` | library | SQLite and PostgreSQL provider | 11 |
| `crates/tarodomain-axum` | library | Native HTTP entry point | 12 |
| `examples/bank-account` | library and binary | The spec's BankAccount / TransferMoney example on the local provider | 9 |
| `examples/bank-account-worker` | Worker | The same domain on Cloudflare | 10 |
| `examples/bank-account-server` | binary | The same domain served over HTTP on SQLite | 12 |

Dependency direction: `local → tarodomain ← cloudflare`, `sqlx → tarodomain`, `testing → tarodomain`, `axum → tarodomain`. `tarodomain` dev-depends on `tarodomain-local` and `tarodomain-testing` for its integration tests, and every provider crate (`local`, `cloudflare`, `sqlx`) dev-depends on `tarodomain-testing` to run the contract suites. `tarodomain-testing` dev-depends on `tarodomain-local` for its doc examples; dev-dependencies don't form a cycle at the library level.

Core module map (`crates/tarodomain/src/`), following the spec's layers (§1.2–1.3):

```
kernel/        ids, clock, backoff, context storage           (Kernel)
error/         FrameworkError, ErrorCode, reports             (Kernel, §22)
domain/        envelopes, events, commands, value objects,
               aggregate ids, aggregate root, ExecutionContext (Domain, §2, §5)
adapters/      persistence contracts + Provider               (Persistence, §3, §8, §9, §11.4, §14, §21, §24)
bus/           EventBus / EventHandler, InMemoryEventBus      (Event Bus, §10)
event_sourcing/ EventRegistry, EventStore                     (§3.3–3.4, §7)
repository/    Repository, EventSourcedRepository, StateRepository (§6)
command/       handlers, registry, dispatcher, pipelines, ConcurrencyChecker (§4, §11, §16)
projection/    Projection, ShardProjection                    (§12)
saga/          ProcessManager, state machine, compensations, SagaEventHandler, timeouts (§13, §17, §19)
catch_up/      SubscriptionManager, CronDispatcher, BackpressureController (§18)
rebuild/       ProjectionRebuilder, SagaRebuilder             (§20)
dead_letter/   RetryPolicy, DeadLetterWriter                  (§21)
trace.rs       CorrelationTrace                               (§28.3)
di.rs          Token, Container                               (§23)
framework/     builder, validation, runtime, http, worker     (§25, §26, §29)
```

## Rust design decisions

The spec assumes JavaScript in places. These rows are where the Rust port deliberately differs. The behaviour stays the same; only the mechanism changes. Rows marked **(Dart)** carry over a fix the Dart port found necessary; its plan has the full reasoning.

| Spec mechanism | Rust replacement | Why |
|---|---|---|
| Class inheritance: `extends AggregateRoot`, `DomainEvent`, `Command`, `ValueObject` (§1.1) | Traits plus composition. An aggregate embeds an `AggregateCore` field and implements `Aggregate`. `raise`, `version`, `load_from_history` and the rest are provided trait methods with the spec's names. | Rust has no inheritance. Provided methods keep the spec's API surface. |
| Event class with frozen fields, `eventName` defaulting to the class name (§2.1) | A plain struct of business fields implementing `Event` (`const NAME`, `const VERSION = "1.0.0"`, serde). The envelope is a separate `EventEnvelope`. Buses and handlers see a type-erased `DomainEvent` (envelope plus an `Arc` payload). `Recorded<E>` is the typed view (`event.downcast::<E>()`). | `std::any::type_name` isn't stable enough to persist, so the name is declared explicitly. Ownership makes events immutable: nothing hands out `&mut` after `raise`, and payloads are shared through `Arc`. |
| `on[EventName]` dispatch on aggregates (§5.2) | `type Event` is an enum generated by `event_family!`, and `apply(&mut self, &Self::Event)` is an exhaustive `match`. `apply` is infallible: invariants are checked before `raise`. | No reflection. The compiler checks exhaustiveness, which is stronger than the runtime `MissingEventHandlerError`. That error remains for stored events outside the family. |
| `on[EventName]` dispatch on projections (§12.2) | `fn routes(r: &mut ProjectionRoutes<Self>)` registers `r.on::<MoneyDeposited>(Self::on_money_deposited)`. A handler is `async fn(self: Arc<Self>, e: Recorded<E>) -> Result<()>`. Unknown events are skipped and marked as processed. | Owned `Arc` and `Recorded` arguments avoid higher-ranked lifetimes on stored async closures. The route table gives validation (§29) the list of handled events. |
| `startsWhen…` / `continueWhen…` / `compensateWhen…` / `completedWhen…` (§13.1) | Route methods with the same names on `SagaRoutes<Self>`, e.g. `r.starts_when::<TransferRequested>(Self::start)`. Saga handlers are **synchronous**: `fn(&mut SagaCtx<Self>, &Recorded<E>) -> Result<Vec<AnyCommand>>`. **(Dart)** | Sagas stay pure decision logic, so a rebuild that replays them has no side effects, and sync handlers are easy to store. |
| Rehydration without the constructor (`Object.create`) (§2.1) | `serde::Deserialize`, through factories registered in `EventRegistry` by `register::<E>()`. | Standard Rust serialization. |
| `ExecutionContext` in pluggable, process-wide context storage (§5.5) | `tokio::task_local!`: `ExecutionContext::scope(ctx, fut).await` and `ExecutionContext::current()`. The active unit of work lives in the same storage (§11.4). | Each task gets its own values, which removes the §5.5 caveat about concurrent requests. `tokio::spawn` doesn't inherit them (see Risks). |
| Envelope enrichment in `raise()` (§5.2) | Same as the spec: `raise()` builds the envelope from the aggregate's own context (set by the repository on load), else from the ambient `ExecutionContext`, else as a new root. | Unlike Dart, Rust needs no zone trick: the envelope is built where the spec says. |
| Events carry `correlationId` and `causationId` (§28.1) | Plus an optional `command_causation_id`: for an event raised by a command, that command's own `causation_id`. It is omitted from JSON when null. **(Dart)** | Commands aren't stored, so without it `CorrelationTrace` can't link a command to the event that caused it. |
| `ValueObject<T>`: deep freeze, `equals`/`hashCode`, `compareTo`, `with(mutator)` (§2.3) | A `ValueObject` trait over a newtype: `validate` (returning `Result<bool, BoxError>`, so the spec's three outcomes stay distinct), `from_validated`, `value()`, and provided `new` and `with(f)`, which validate. Equality and hashing are derived. Ordering exists only where the type implements `Ord`. | The compiler enforces immutability, and comparing an unordered value object is a compile error instead of `INCOMPARABLE_VALUE`. The code stays in the hierarchy for completeness. |
| `AggregateId` subclasses (§2.4) | `AggregateId` wraps `uuid::Uuid`. The `aggregate_id!(AccountId)` macro generates a typed newtype with `generate` / `from_existing` / `from_uuidv7`. | Typed IDs without inheritance. Mixing two ID types is a compile error. |
| A `FrameworkError` subclass per code (§22) | One `FrameworkError { code: ErrorCode, message, context, source }`. `ErrorCode` lists the 51 codes, plus `ErrorCode::Custom { code, category }` for user codes such as `INSUFFICIENT_FUNDS`. Each code has a constructor. | Idiomatic Rust (`std::error::Error` via `thiserror`). Callers can match on `code`, and user code can still add its own codes. |
| `shouldRetry` for errors outside the hierarchy (§22.1) | Handlers and adapters return `Result<_, FrameworkError>`. Foreign errors are converted at the boundary with `FrameworkError::infrastructure(err)`, so "any other error is infrastructure and retried" holds. | Rust errors are typed, so the conversion happens where they enter. |
| `toSnapshot()` from own enumerable properties, `#private` key (§5.4) | `type Snapshot: Serialize + DeserializeOwned` with required `snapshot_state()` and `restore_snapshot_state()`. There is no `#private` key. **(Dart)** | No reflection over fields. Making both methods required means the global `snapshot_every` never silently skips an aggregate. |
| `loadFromHistory(events, snapshotVersion?)` (§5.1) | `load_from_history(events)` continues from the current version, which `restore_snapshot` sets. **(Dart)** | Only one place sets the version. |
| Snapshot when `newVersion % snapshotEvery == 0` (§6.2) | Snapshot when a save *crosses* a multiple of `snapshot_every`. **(Dart)** | A save of several events can jump over the exact multiple. |
| `CommandHandler<C, A>` with `aggregateType` and an event bus constructor parameter (§4.1) | `CommandHandler<C>` with only `handle`. Repositories come from `CommandContext::repository::<A>()`, which builds them on the running pipeline's bus. **(Dart)** | A handler may touch several aggregates or none, and which bus applies depends on the pipeline (§11.3). |
| `CommandDispatcher.resolve` returns a handler (§4.2) | `resolve` returns the registration. The pipeline creates a fresh handler for each attempt. **(Dart)** | The handler instance depends on the pipeline's bus. |
| `Projection<TDb>` with a `db` property (§12.1) | `Projection` has no type parameter; each projection owns its storage. **(Dart)** | The framework never touches the read model. |
| `CompensationStack` of closures, not persisted (§13.5, §17) | Compensations are defined by name in `routes` (`r.define_compensation("refund", …)`). A step pushes the name, and the stack of names is saved with the saga state. Factories still run at compensation time. **(Dart)** | `SagaEventHandler` loads the saga from the store for every event and never replays it, so a stack of closures registered for an earlier event would be empty. |
| `loadStream(streamId, fromVersion?)` and `loadAll(fromEventId?)` (§3.2) | `load_stream(stream_id, after_version, max_count)` and `load_all(after_event_id, max_count)`. Both are exclusive, as the D1 queries of §24.3 are (`stream_version > ?`, `event_id > ?`). | The spec's names read as inclusive in Rust. A repository that restored a snapshot at version 5 passes 5, and a subscription passes its `last_seen`. |
| Saga commands get fresh IDs | Each command a saga handler returns gets a deterministic UUIDv7: the timestamp of the event's `occurred_at`, and the remaining bits from SHA-256 of (saga type, event ID, position). **(Dart)** | A redelivered event recreates the same command IDs, and command idempotency skips them. |
| `SagaEventHandler` dispatches before saving (§13.3) | Same, plus a per-saga mailbox in task-local storage. An event for a saga that is still being handled in the same task waits until that saga is saved. **(Dart)** | With synchronous delivery, a dispatched command publishes the next event before the saga is saved. |
| Subscription `lastSeen` only moves forward (§18.2) | Adapters order by an append **position**, not by event ID. The SQL adapter refuses to move a cursor back. **(Dart)** | Event IDs don't order events within one millisecond or across servers. |
| Catch-up retries a failing event forever (§18.3) | Same by default. The opt-in `catch_up_max_attempts(n)` dead-letters the event after `n` runs. **(Dart)** | A permanently stuck subscription is otherwise invisible. |
| A timeout handler that throws marks the saga `FAILED` (§19.1) | Only a non-retryable error does. A retryable one leaves the saga for the next run. The `SagaTimeout` ID is derived from the saga's type, ID and `updated_at`. **(Dart)** | A transient error shouldn't fail a saga for good, and a retry must dispatch the same command IDs. |
| `loadTimedOut(threshold, { maxCount })` (§19.1) | `load_timed_out(before, SagaQuery { saga_type, limit })`. **(Dart)** | With one scheduler per saga type (§19.3), filtering after the query would let other types fill the limit. |
| `UnitOfWork` `begin` / `commit` / `rollback` (§11.4) | Kept, plus `execute(work)`. A provider returns a new unit from each `create_unit_of_work()`, and `execute` makes it the task's active unit (task-local), which adapters downcast to find their transaction. A nested `execute`, or a strong dispatch inside one, joins the active unit and isn't retried on its own. **(Dart)** | A saga dispatching from inside a strong command has to share its transaction. After a conflict the outer one restarts. |
| `Framework.build() → Promise<Framework>` (§25) | `build().await -> Result<TaroDomain>`. One runtime per process; the provider (for example an sqlx pool) is shared. | Unlike Serverpod, Rust needs no per-request session, so the Dart port's definition/runtime split isn't needed. |
| `fetchHandler` / `queueHandler` / `scheduledHandler` (§25.2, §26) | The core exposes `dispatch`, `handle_events` (queue consumer), `run_scheduled` (catch-up, then saga timeouts), `run_cleanup` (the §14 and §21 retention) and `HttpApi`, which maps plain request and response structs to the §26.1 routes. `CloudflareWorker` connects fetch, queue and scheduled to them. `tarodomain-axum` serves `HttpApi` natively, and `framework::Worker` runs the scheduled jobs on intervals. | The core stays free of HTTP frameworks and platform code, and both hosts share one route table. |
| `Token<T>` / `Container` (§23) | `Token<T>` is a `const` with a name and `PhantomData<T>`. Values are stored as `Arc<dyn Any + Send + Sync>`, and `resolve` returns `Arc<T>`. | Type-safe resolution without reflection. |
| TS type helpers `defineEvents`, `ProjectionOf`, `SagaOf` (§29.3) | Not needed: event family enums and route tables cover them. | |
| `TransactionalDatabase` write-collecting proxy (§11.4) | Only D1 needs it. `D1UnitOfWork` stages writes and commits them in one `db.batch()`. It also keeps an overlay of the staged events, saga records and idempotency markers, which the D1 adapters read before D1. sqlx has real transactions, and the local provider copies its tables when a unit begins. | With the spec's proxy, reads inside a unit can't see the unit's own writes. A strong-mode transfer creates the transfer aggregate and the saga record and reads both again in the same unit, so without the overlay it can't find them. |
| JavaScript handles in a single-threaded Worker | The core's traits keep their `Send + Sync` bounds. `tarodomain-cloudflare` wraps D1 and Queue handles in workers-rs's `send::SendWrapper`, and adapter futures in `send::SendFuture`. | A Worker isolate runs on one thread, so the wrappers are sound there, and native providers keep compiler-checked `Send`. Dropping the bounds instead would need a `?Send` variant of every trait. |
| D1 `loadAll … ORDER BY event_id` (§24.3) | D1 orders by an `AUTOINCREMENT` position column. | Event IDs don't order events within one millisecond (the cursor row above). D1 runs one write at a time, so insert order is commit order and D1 needs no sequencer. |
| D1 append: `UPDATE streams … WHERE version = expected` inside the batch (§24.3) | The unique `(stream_id, stream_version)` key detects the conflict: a stale writer inserts a version that already exists, so the batch fails and rolls back. A guard statement also fails the batch when the expected version is ahead of the stream. The error becomes `OPTIMISTIC_CONCURRENCY`. | An `UPDATE` that matches no row isn't an error, so on its own it would let the batch commit. |
| `initialize()` runs D1 migrations; `POST /migrate` (§24.2, §26.1) | The migrations are embedded in the crate and tracked in `schema_migrations` (§27.1). `initialize()` applies any missing ones once per isolate. `POST /migrate` is off unless the app turns it on. | Deploying needs no separate migration step, and an open migrate endpoint isn't exposed by default. |
| Queue consumer: `message.retry({ delaySeconds })` (§10.5) | Same, with the delay from `RetryPolicy` and the attempt count from the message itself. | Queues already count attempts, so no extra table is needed. |

## Phases and steps

### Phase 0: Bootstrap (done 2026-11-16)

#### Step 0.1: Repository and spec
Files: `.gitmodules` (plus the spec submodule's gitlink), `.gitignore`, `LICENSE`, `README.md`, `docs/IMPLEMENTATION_PLAN.md`
- [x] `git init` on branch `main`
- [x] Spec added as the `spec/` submodule, pinned to `a9243e3`
- [x] `.gitignore` for `target/`, local SQLite files and `.env*`
- [x] `LICENSE`: MIT, the same as the spec and the Dart port
- [x] README: crate table, development commands, how to update the spec
- [x] This plan

#### Step 0.2: Cargo workspace and core crate skeleton
Files: `Cargo.toml`, `rust-toolchain.toml`, `crates/tarodomain/Cargo.toml`, `crates/tarodomain/src/lib.rs`
- [x] Workspace: resolver 3, edition 2024, `rust-version = "1.85"`, `publish = false`, shared `[workspace.dependencies]`, workspace lints (`unsafe_code = "forbid"`, `missing_docs = "warn"`, clippy `all`)
- [x] `rust-toolchain.toml`: stable, with rustfmt and clippy
- [x] Core crate with crate-level docs and no dependencies yet
- [x] `cargo check --workspace` passes (cargo 1.100, 2026-11-16)
- [x] `Cargo.lock` generated
- [x] `Cargo.lock` committed (with the first commit)

#### Step 0.3: Tooling and CI
Files: `rustfmt.toml`, `.editorconfig`, `.github/workflows/ci.yml`
- [x] `rustfmt.toml` (edition 2024) and `.editorconfig`
- [x] CI: checkout with submodules, then `fmt --check`, `clippy -D warnings`, `test` and `doc` with `-D warnings`
- [x] CI green on the first push (run 43843423321, commit `180bccb`)

#### Step 0.4: Workers target check
Files: `rust-toolchain.toml`, `.github/workflows/ci.yml`, `README.md`
- [x] `rust-toolchain.toml` adds the `wasm32-unknown-unknown` target
- [x] CI installs the target and runs `cargo check --target wasm32-unknown-unknown -p tarodomain`. Step 10.1 adds the Cloudflare crate to it, and Step 10.10 the harness and the example worker
- [x] README: the wasm command and the local containers section (Kiri)
- [x] Passes locally
- [x] Passes in CI

### Phase 1: Kernel and errors (§1.3 Kernel, §22) (done 2026-11-16)

#### Step 1.1: Error categories and codes
Files: `crates/tarodomain/Cargo.toml`, `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/error/mod.rs`, `crates/tarodomain/src/error/codes.rs`, `Cargo.toml`

- [x] Dependencies: `thiserror`, `serde`, `serde_json`
- [x] `ErrorCategory` (`Domain`, `Concurrency`, `Infrastructure`, `Configuration`) with `retryable()` and `http_status()` (400 / 409 / 500 / 500)
- [x] `ErrorCode`: all 51 codes of §22.2, with `as_str()` (`"OPTIMISTIC_CONCURRENCY"`), `FromStr` and `category()`. Plus `ErrorCode::Custom { code, category }` for user codes
- [x] `FrameworkError { code, message, context: serde_json::Map, source }`, with `retryable()`, `http_status()`, `with_context(key, value)` and `with_source(err)`. It implements `std::error::Error`
- [x] One constructor per code, e.g. `FrameworkError::optimistic_concurrency(stream_id, expected, actual)` (generated by a macro where repetitive)
- [x] `should_retry(&(dyn Error + 'static))`: a `FrameworkError` answers with `retryable()`, anything else is infrastructure and returns `true`
- [x] `pub type Result<T, E = FrameworkError>`
- [x] Tests: 19 / 3 / 18 / 11 codes per category; the retryable and HTTP status matrix for every code; `as_str` ↔ `FromStr` round trip; a custom code keeps its category
- [x] Root `Cargo.toml`: `cargo::unused_workspace_dependencies` allowed until Step 13.1, because the shared dependencies are declared before any crate uses them

#### Step 1.2: Error reports
Files: `crates/tarodomain/src/error/report.rs`, `crates/tarodomain/src/error/mod.rs`
- [x] `to_json()`: `code`, `category`, `message`, `retryable`, `httpStatus`, `context`
- [x] `to_http_response() -> HttpErrorResponse { status, body }`. `body.error` holds `code`, `category`, `message` and `retryable`, and adds `context` only for 4xx. The source is never included
- [x] `to_log_entry() -> LogEntry`: level `Warn` for domain errors and `Error` otherwise, with the source chain
- [x] Tests: 4xx includes context, 5xx omits it, the source never leaks, log levels

#### Step 1.3: IDs
Files: `crates/tarodomain/Cargo.toml`, `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/kernel/mod.rs`, `crates/tarodomain/src/kernel/ids.rs`
- [x] Dependencies: `uuid`, `sha2` (the resolver picks uuid 1.26, the newest that supports Rust 1.85)
- [x] For `wasm32-unknown-unknown` only: `uuid`'s `js` feature, so `Uuid::now_v7()` takes its time from `Date.now()` and its random bits from `crypto.getRandomValues`. `getrandom` isn't in the wasm build, so `.cargo/config.toml` isn't needed
- [x] `new_v7()`, `parse_uuid(&str)` (any version, case-insensitive, hyphenated form only), `is_v7(&Uuid)`. `Uuid` is re-exported from `kernel::ids`
- [x] `derived_v7(seed, index, unix_ms) -> Uuid`: the timestamp from `unix_ms` and the other 74 bits from SHA-256(seed, index). The same input always gives the same ID, and it is still a valid v7
- [x] Tests: v7 detection, upper and lower case parse equal, other text forms rejected, derived IDs are deterministic, carry their timestamp and depend on every input
- [x] The CI wasm check passes with the new dependencies

#### Step 1.4: Clock, backoff and sleeper
Files: `Cargo.toml`, `crates/tarodomain/Cargo.toml`, `crates/tarodomain/src/kernel/mod.rs`, `crates/tarodomain/src/kernel/clock.rs`, `crates/tarodomain/src/kernel/backoff.rs`
- [x] Dependencies: `chrono` and `tokio`. The workspace's chrono uses the `now` feature instead of `clock` (no local time zones), plus `wasmbind` on wasm32 so `Utc::now()` works in a Worker. `rand` is dropped from the workspace: jitter uses a small SplitMix64 generator seeded from a UUIDv7's random bits, which keeps `getrandom` out of the wasm build
- [x] `Clock` trait (`now() -> DateTime<Utc>`), `SystemClock`, and `ManualClock` (set and advance) for tests
- [x] `RandomSource` trait (`next_f64()` in [0, 1)) and `SplitMix64`
- [x] `Backoff { initial, multiplier, max, jitter }` with `delay(attempt, random)`: `initial * multiplier^attempt`, ±`jitter`, never more than `max`
- [x] `Sleeper` trait, `TokioSleeper` (native only: `cfg(not(target_arch = "wasm32"))`), and `RecordingSleeper` for tests
- [x] Tests: 10 → 20 → 40 ms with zero jitter, the cap, jitter within ±10%, the generator stays in [0, 1), the manual clock, the sleepers
- [x] The CI wasm check passes with the new dependencies

#### Step 1.5: Context storage
Files: `crates/tarodomain/src/kernel/mod.rs`, `crates/tarodomain/src/kernel/context.rs`
- [x] Context storage over `tokio::task_local!`: `scope(value, fut)`, `sync_scope(value, f)`, `try_with(f)`. A nested scope starts as a copy of the outer one, and the outer value is back after it ends. The outer values are read when the scoped future first runs, not when `scope` is called, so `scope(a, scope(b, fut))` sees both
- [x] Also `get::<T>()`, a clone of the value in scope
- [x] Tests: nested scopes (async and sync), two concurrent tasks each see only their own value, a spawned task starts empty, when values are read
- [x] The CI wasm check passes

### Phase 2: Domain primitives (§2, §5, §5.5, §28) (done 2026-11-16)

#### Step 2.1: Metadata, envelopes, ExecutionContext
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/domain/mod.rs`, `crates/tarodomain/src/domain/metadata.rs`, `crates/tarodomain/src/domain/envelope.rs`, `crates/tarodomain/src/domain/execution_context.rs`
- [x] `Metadata`: an immutable map from string to a primitive (`MetadataValue`: null, bool, number, string), cheap to clone (one shared `Arc` map, `with(key, value)` returns a copy), serialized as a JSON object in key order. Deserializing rejects arrays and objects
- [x] `EventEnvelope { id, correlation_id, causation_id, command_causation_id, metadata, occurred_at }` and `CommandEnvelope { id, correlation_id, causation_id, metadata, timestamp }`. The constructors take the timestamp as an argument (the framework never reads the system time itself; see Step 2.5) and cut it to whole milliseconds, the precision stores keep
- [x] Parsing IDs from strings fails with `INVALID_EVENT_ID` / `INVALID_CORRELATION_ID` / `INVALID_CAUSATION_ID`, or the `INVALID_COMMAND_*` codes: `parse_id`, `parse_correlation_id` and `parse_causation_id` on each envelope
- [x] `ExecutionContext { correlation_id, causation_id, command_causation_id, metadata }`, with `new(correlation_id)` (a root), `for_command`, `for_event`, `scope(ctx, fut)`, `sync_scope` and `current()`. Its rustdoc shows how to hand it to `tokio::spawn`
- [x] Defaults: a new v7 ID; the context's correlation ID or a new root; the context's causation ID or none. `new(at)` uses the context in scope, `from_context(ctx, at)` the one given
- [x] Tests: defaults outside any context, inheritance inside one, the right code for each invalid ID, a nested scope restores the outer one, the §28.2 propagation chain

#### Step 2.2: Value objects and aggregate IDs
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/domain/mod.rs`, `crates/tarodomain/src/domain/value_object.rs`, `crates/tarodomain/src/domain/aggregate_id.rs`, `crates/tarodomain/tests/value_objects.rs`
- [x] `ValueObject` trait: `type Value`, `validate(&Value) -> Result<bool, BoxError>`, `from_validated` (called only by `new`), `value()`, and provided `new` and `with(f)`, which validates again. A `false` from `validate` becomes `VALUE_OBJECT_INVALID`, a framework error passes through, and anything else becomes `VALUE_OBJECT_CONSTRUCTION_FAILED` with the error as its source
- [x] `AggregateId` over `Uuid`: `generate()`, `from_existing(&str)`, `from_uuidv7(&str)`, `from_uuid`, `uuid()`, `FromStr`. An invalid UUID is `VALUE_OBJECT_INVALID`. Displayed in lower case, serialized as a string
- [x] `aggregate_id!(pub AccountId)` generates a typed newtype with the same factories, `as_aggregate_id()` and `From<AccountId> for AggregateId`. It uses `$crate` paths, and implements serde through a hidden `tarodomain::__private::serde` re-export (in `lib.rs`), so the calling crate needs no serde dependency
- [x] Tests (from outside the crate, to check macro hygiene): an invalid value is rejected, a framework error passes through, a foreign error keeps its source, `with` validates, equality ignores case, the v7-only factory rejects v4, a `compile_fail` doctest shows two ID types don't mix (checked to fail with E0308)

#### Step 2.3: Events
Files: `crates/tarodomain/src/domain/mod.rs`, `crates/tarodomain/src/domain/event.rs`, `crates/tarodomain/src/domain/serialized.rs`
- [x] `Event` trait: `const NAME: &'static str`, `const VERSION: &'static str = "1.0.0"`, plus `Serialize + DeserializeOwned + Debug + Send + Sync + 'static`
- [x] `DomainEvent`: envelope, name, version and an `Arc` payload, with a cheap `Clone`, `downcast::<E>() -> Option<Recorded<E>>`, `downcast_ref::<E>()` and `is::<E>()`. The payload is a private trait object over `Any` that can also serialize itself, so `serialize()` needs no registry
- [x] `Recorded<E> { envelope, event: Arc<E> }`, which derefs to `E` and converts back into a `DomainEvent`
- [x] `SerializedEvent` with the §2.1 storage fields (`event_id`, `event_type`, `event_version`, `causation_id`, `correlation_id`, `data`, `metadata`, `occurred_at`) plus `command_causation_id`, typed (`Uuid`, `Metadata`, `DateTime<Utc>`), with `envelope()`. `DomainEvent::serialize()` fails with `SERIALIZATION_ERROR` for a payload without a JSON form
- [x] `format_timestamp` / `parse_timestamp`: the ISO 8601 form with exactly three fractional digits that every store writes
- [x] Tests: field names in the serialized JSON, a downcast to the wrong type returns `None`, `occurred_at` is ISO 8601 with milliseconds, JSON round trip

#### Step 2.4: Event families
Files: `crates/tarodomain/src/domain/mod.rs`, `crates/tarodomain/src/domain/family.rs`, `crates/tarodomain/tests/event_family.rs`
- [x] `EventFamily` trait: `names()`, `name(&self)`, `into_domain(self, EventEnvelope) -> DomainEvent`, `from_domain(&DomainEvent) -> Option<Self>`
- [x] `event_family! { pub enum AccountEvent { AccountOpened, MoneyDeposited, … } }` generates the enum (one variant per event struct, holding it in an `Arc`, so converting to and from `DomainEvent` shares the payload and events needn't be `Clone`), `From<E>` for each variant, and the trait impl. The enum derives `Clone` and `Debug`; extra derives pass through as attributes
- [x] Tests (from outside the crate): conversion both ways, the payload is shared, an event outside the family gives `None`, `names()` order

#### Step 2.5: Commands
Files: `crates/tarodomain/src/domain/mod.rs`, `crates/tarodomain/src/domain/command.rs`, `crates/tarodomain/src/kernel/clock.rs`
- [x] `clock::now()`: the clock in scope (task-local, like the execution context), else `SystemClock`; `clock::scope(clock, fut)` and `clock::sync_scope`. `CommandMessage::from` and `raise` (Step 2.6) take their timestamps from it, so a test or the runtime fixes them by scoping a `ManualClock` or the provider's clock
- [x] `Command` trait: `const NAME`, serde, `Debug + Send + Sync + 'static`
- [x] `CommandMessage<C> { envelope: CommandEnvelope, command: C }`, which derefs to `C`. `new` and `From<C>` take correlation, causation and metadata from `ExecutionContext::current()`. Builder methods `with_id`, `with_correlation_id`, `with_causation_id`, `with_metadata`
- [x] `AnyCommand`: a type-erased command with a shared (`Arc`) payload (`name`, `envelope`, `id`, `as_any`, `is`, `downcast_ref`, `to_serialized`), which any `C: Command` or `CommandMessage<C>` converts into with `.into()`
- [x] `SerializedCommand` with the §2.2 fields (`command_id`, `command_type`, `causation_id`, `correlation_id`, `data`, `metadata`, `timestamp`), and `envelope()`
- [x] `AnyCommand::with_derived_id(seed, index, at)` on top of `derived_v7`
- [x] Tests: defaults from the context, the timestamp from the clock in scope, builder methods, downcasts, serialized shape, derived IDs are stable and carry the event's time

#### Step 2.6: Aggregate root
Files: `crates/tarodomain/src/domain/mod.rs`, `crates/tarodomain/src/domain/aggregate.rs`, `crates/tarodomain/tests/aggregate.rs`
- [x] `AggregateCore`: version, uncommitted `Vec<DomainEvent>`, and the aggregate's own `Option<ExecutionContext>`. Its fields are private, so only the trait's methods change them
- [x] `Aggregate` trait: `type Id` (`Copy`, `Display`, `Into<AggregateId>`, …), `type Event: EventFamily`, `type Snapshot`, `const TYPE`, `new(id)` (must not raise), `id()`, `core()` / `core_mut()`, `apply(&mut self, &Self::Event)`, `snapshot_state()`, `restore_snapshot_state()`
- [x] Provided methods: `raise(e)` (apply, version + 1, queue with an envelope from the aggregate's own context, else the one in scope, else a new root, timed by `clock::now()`), `version()`, `committed_version()`, `has_uncommitted_changes()`, `uncommitted_changes()`, `mark_changes_as_committed()`, `load_from_history(events)`, `to_snapshot() -> SnapshotData { data, version }`, `restore_snapshot(data, version)`, `restore_version(version)` (for state-stored aggregates, §6.3) **(Dart)**, and the doc-hidden `set_execution_context` / `clear_execution_context`
- [x] `load_from_history` continues from the current version, fails with `MISSING_EVENT_HANDLER` on an event outside the family, and refuses to run with uncommitted changes (`INVALID_AGGREGATE_STATE`, as do `restore_snapshot` and `restore_version`). A snapshot that doesn't fit `Snapshot` is `SERIALIZATION_ERROR` and leaves the aggregate unchanged
- [x] Tests: version arithmetic, `raise` inside a context carries its correlation and causation, the aggregate's own context comes first, `raise` outside one starts a root, the time comes from the clock in scope, history replay doesn't queue, snapshot round trip

### Phase 3: Persistence and bus contracts (§3.2, §8–10, §11.4, §12.3, §14, §21.1, §24.1) (done 2026-11-16)

#### Step 3.1: Event store adapter contract
Files: `crates/tarodomain/Cargo.toml`, `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/adapters/mod.rs`, `crates/tarodomain/src/adapters/event_store.rs`
- [x] Dependency: `async-trait`, re-exported as `tarodomain::async_trait` so adapter crates need no dependency of their own. `futures` waits for the first step that uses it: cargo 1.100 warns on an unused dependency
- [x] `EventStoreAdapter` with every §3.2 method, plus `record_failed_attempt(name, event_id) -> u32` **(Dart)**. `load_all`, `stream_exists` and `get_stream_version` are provided methods (on `load_stream_after_event($all)` and `get_stream_metadata`), which adapters may override
- [x] `AppendResult { version }`, `StreamMetadata { stream_id, version, created_at, updated_at }`, `Subscription { name, stream_id, last_seen, created_at, failed_event_id, failed_attempts }` with `stream()` (`$all` when `stream_id` is `None`)
- [x] `StreamIds::ALL`, `StreamIds::for_aggregate(type, id)`, `StreamIds::category_of(stream_id)` (§3.1), plus `StreamIds::parse -> StreamKind { All, Category, Aggregate { category, id } }`. An aggregate stream ID ends in `-` and a lower-case UUID, so a type may contain hyphens
- [x] Rustdoc pins the semantics the contract suites test: versions count from 1, a missing stream has version 0, cross-stream reads come back in global **append** order (not event-ID order), deleting a missing row is a no-op, a stale expected version fails with `OPTIMISTIC_CONCURRENCY`. Also: appends go only to aggregate streams and are atomic; an empty append still checks the version; an invalid stream ID or a stored event ID is `EVENT_APPEND_FAILED`; `subscribe` is idempotent; acking or recording a failure on a missing subscription does nothing (and returns 0)
- [x] Tests: stream ID kinds, including hyphenated types and non-canonical IDs, and the provided methods through `Arc<dyn EventStoreAdapter>`

#### Step 3.2: Snapshot and saga store contracts
Files: `crates/tarodomain/src/adapters/mod.rs`, `crates/tarodomain/src/adapters/snapshot_store.rs`, `crates/tarodomain/src/adapters/saga_store.rs`
- [x] `SnapshotStoreAdapter` (§8.1), `SnapshotInput`, `Snapshot`. The key is `(source_uuid, source_version)`, and `save` is an upsert that also replaces `created_at`. Reads filter on the type too; `list` is newest first with an optional limit; `delete_before(v)` keeps the snapshot at `v`. Metadata is a `Metadata`, empty when there is none
- [x] `SagaStoreAdapter` (§9.1), `SagaStatus` (`Running`, `Compensating`, `Completed`, `Failed`, stored as `RUNNING` …, with `is_active` / `is_terminal`), `SagaSaveInput`, `SagaRecord`. `load_by_correlation_id` is a provided method on `load`, because a saga's ID is its correlation ID (§9.2)
- [x] `load_timed_out(before, SagaQuery { saga_type, limit })` **(Dart)**, and `load_by_status` takes the same `SagaQuery`. Lists are ordered by `updated_at`, then type and ID. `complete` and `fail` are partial updates and idempotent: a saga that has already ended is left unchanged
- [x] `SagaRecord::saved`, `complete`, `fail` and `is_timed_out` state the write rules once (`started_at` kept, `updated_at` moved, `completed_at` set on reaching a terminal status), for the in-memory adapter and the D1 overlay
- [x] Tests: status text round trip, the timestamp rules, ending a saga only once, the timed-out filter

#### Step 3.3: Idempotency, dead letter and unit of work contracts
Files: `crates/tarodomain/src/adapters/mod.rs`, `crates/tarodomain/src/adapters/idempotency.rs`, `crates/tarodomain/src/adapters/dead_letter.rs`, `crates/tarodomain/src/adapters/unit_of_work.rs`
- [x] `ProcessedEventStore` (§12.3), `CommandIdempotencyStore` (§14.1) and `ProcessedCommand`. Marking twice keeps the first record. `CommandIdempotencyStore::exists` is a provided method on `get_processed_at`; `list` is oldest first with an optional limit
- [x] `DeadLetterStore` (§21.1), `DeadLetterInput` (handler name, event ID and type, payload as JSON, error code and message, attempts), `DeadLetterEntry`. `write` returns the new ID; IDs increase in write order and are never reused; `get(event_id, handler)` returns the latest entry; `list` is in ID order
- [x] `UnitOfWork`: `begin`, `commit`, `rollback`, `is_active`, and `execute(work)` that rolls back on error. `execute` is an inherent method on `dyn UnitOfWork` (`self: Arc<Self>`), because a generic trait method isn't dyn-compatible. It puts the unit in task-local storage, so a nested `execute` joins it, and adapters find it with `<dyn UnitOfWork>::active()` and `downcast_ref::<T>()` (through an `AsAny` supertrait, since trait upcasting needs Rust 1.86). The work's error comes back unchanged, with `rollbackError` in its context if the rollback also failed
- [x] Rustdoc: the 7-day and 30-day retentions (§14, §21) belong to the cleanup job, not to the stores
- [x] Tests: commit and rollback, the error unchanged, a failed begin, commit or rollback, a nested `execute` joins (and its error rolls back the outer unit), two concurrent units each see their own

#### Step 3.4: Event bus contracts and InMemoryEventBus
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/bus/mod.rs`, `crates/tarodomain/src/bus/contracts.rs`, `crates/tarodomain/src/bus/in_memory.rs`
- [x] `EventBus` (`name`, `publish_all(&[DomainEvent])`, and `publish` provided on it), `EventHandler` (`name`, `handle(&DomainEvent)`), `SubscribableEventBus` (`subscribe(Arc<dyn EventHandler>)`, `unsubscribe(name) -> bool`; synchronous, since subscribing happens at build)
- [x] `InMemoryEventBus::new(name)`: delivers each event to every handler in subscription order before the next event. Subscribing the same handler (`Arc::ptr_eq`) twice is a no-op; a different handler with a taken name is `INVALID_CONFIGURATION`. No lock is held while a handler runs, so a handler may publish to the same bus
- [x] `publish_all` is fail-fast and returns the handler's error unchanged, so a conflict stays retryable. `publish_all_with_results` continues and returns a `Delivery { handler_name, event_id, outcome }` per handler and event
- [x] Tests: delivery order, fail-fast skips later handlers, with-results collects everything, duplicate names, unsubscribe, a handler publishing to its own bus

#### Step 3.5: Provider contract and prelude
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/adapters/mod.rs`, `crates/tarodomain/src/adapters/provider.rs`, `crates/tarodomain/src/bus/contracts.rs`, `crates/tarodomain/src/bus/in_memory.rs`
- [x] `ConsistencyMode` (`Strong`, `Eventual`; `"strong"` / `"eventual"` as text and in serde) and `EventBusConfig { consistency, transport: Option<String> }`
- [x] `Provider` (§24.1): `provider_type()`, a `create_*` method for each of the six stores, `create_event_bus(&config) -> Result<Arc<dyn EventBus>>` (an unknown transport is `INVALID_CONFIGURATION`), `create_unit_of_work() -> Option<Arc<dyn UnitOfWork>>` (a new unit per call), and `initialize()` (a no-op by default, safe to repeat)
- [x] Also `clock()` and `sleeper()`, defaulting to `SystemClock` and `TokioSleeper`. On wasm32 the default sleeper returns at once, since tokio's timers don't work there; a provider for a platform where those don't work (Cloudflare) returns its own
- [x] `EventBus::as_subscribable()` (`None` by default, `Some` for `InMemoryEventBus`), so the framework can subscribe handlers to a provider's bus without trait upcasting (Rust 1.86). Hence the two `bus/` files
- [x] `tarodomain::prelude` re-exports what a domain crate needs: the domain types, the error types, `aggregate_id!` and `event_family!`, `EventBus` and `EventHandler`, `ConsistencyMode` and `async_trait`. A doctest builds an event family through it

### Phase 4: Local provider and contract suites (§24 LocalProvider) (done 2026-11-16)

#### Step 4.1: Local crate and shared database
Files: `Cargo.toml`, `crates/tarodomain-local/Cargo.toml`, `crates/tarodomain-local/src/lib.rs`, `crates/tarodomain-local/src/database.rs`
- [x] Workspace member added
- [x] `LocalDatabase`: tables behind an `Arc<Mutex<…>>` shared by every adapter of one provider (clones are handles to the same storage), and an injectable `Clock`. Tables are keyed by type: each adapter keeps its rows in a `Table` of its own type through `read::<T>(f)` and `write::<T>(f)`, so later steps add tables without touching this file, and an application can keep in-memory read models in the same database. The global append position counter belongs to the event table (Step 4.2)
- [x] The lock is a `std::sync::Mutex` held only while a synchronous closure runs, never across `.await`. Writes also take the database's turn, an async lock, which a unit of work holds for its whole life (Step 4.4)
- [x] Tests: empty default, keyed by type, the closure's result, shared clones, the clock, a write waits for the turn, a copy of the tables is deep

#### Step 4.2: In-memory event store adapter
Files: `crates/tarodomain-local/Cargo.toml`, `crates/tarodomain-local/src/lib.rs`, `crates/tarodomain-local/src/event_store.rs`
- [x] Dependencies: `chrono`; `serde_json` for tests
- [x] `InMemoryEventStoreAdapter::new(db)`. One `EventTable` holds the rows by position, the event ID → position index, the aggregate streams (their positions in version order, `created_at`, `updated_at`), the subscriptions, and the last position given out
- [x] The expected-version check and the append happen in one `write`. A conflict is `OPTIMISTIC_CONCURRENCY` with the stream, expected and actual versions as context. An ID already stored, or twice in one append, is `EVENT_APPEND_FAILED` and writes nothing
- [x] Cross-stream reads are ordered by append position. An `after_event_id` cursor resolves to its position, falling back to comparing IDs only if that event was deleted
- [x] Category streams (`BankAccount`) and `$all`
- [x] Subscriptions: `subscribe` is idempotent (`Some("$all")` is stored as `None`), `ack` only moves forward in append order, and `record_failed_attempt` counts per event and resets on ack
- [x] Tests: versions and conflicts, empty appends, non-aggregate streams, duplicate IDs, `after_version` and `max_count`, append order across streams with same-millisecond IDs in reverse, the deleted-cursor fallback, delete and position reuse, correlation, clock-stamped metadata, subscriptions

#### Step 4.3: In-memory stores
Files: `crates/tarodomain-local/src/lib.rs`, `crates/tarodomain-local/src/snapshot_store.rs`, `crates/tarodomain-local/src/saga_store.rs`, `crates/tarodomain-local/src/idempotency.rs`, `crates/tarodomain-local/src/dead_letter.rs`
- [x] One adapter per contract of steps 3.2–3.3: `InMemorySnapshotStore`, `InMemorySagaStore`, `InMemoryProcessedEventStore`, `InMemoryCommandIdempotencyStore`, `InMemoryDeadLetterStore`, each with `new(db)` and a table of its own. Timestamps come from the database's clock
- [x] Sagas: `load_by_status` ordered by `updated_at` ascending, then type and ID; `load_timed_out` filters to `Running` / `Compensating`, by saga type and up to the limit. The write rules are `SagaRecord::saved`, `complete` and `fail`
- [x] Dead letters get auto-increment IDs, never reused after a delete
- [x] Tests: snapshot upsert and per-type reads and deletes; saga timestamps, ending once, list order, filter before limit; idempotent marks keep their first time; dead-letter IDs, per-handler reads and the latest entry, `delete_old`

#### Step 4.4: Unit of work and LocalProvider
Files: `crates/tarodomain-local/src/lib.rs`, `crates/tarodomain-local/src/database.rs`, `crates/tarodomain-local/src/unit_of_work.rs`, `crates/tarodomain-local/src/provider.rs`
- [x] `InMemoryUnitOfWork` takes the database's turn and copies the tables on `begin`. Inside `execute`, `LocalDatabase::read` / `write` use that copy (the task's active unit, if it is an in-memory unit over the same database), so the unit sees its own writes and other tasks don't. `commit` swaps the copy in; `rollback`, or dropping a begun unit, throws it away
- [x] Units on one database run one at a time (the turn, an async lock, is held for the unit), and writes outside a unit wait for it, so a commit never overwrites and a rollback never undoes another writer's work. Rustdoc warns against waiting, inside a unit, for a write that runs outside it (such as a spawned task's)
- [x] `begin` on an active unit and `commit` on an inactive one are `INVALID_CONFIGURATION`; `rollback` on an inactive one does nothing
- [x] `LocalProvider::new()`, `LocalProvider::with_database(db)` and `database()`. `create_event_bus` returns a new `InMemoryEventBus` (`local-strong` / `local-eventual`) for both consistency modes; a transport other than `in_memory` is `INVALID_CONFIGURATION`. `provider_type()` is `"local"`, and `clock()` is the database's
- [x] Tests: commit and rollback over several tables, others see writes only after commit, a write outside waits (and survives a rollback), units run one at a time, a unit over another database is ignored, dropping a begun unit, state checks; the provider's shared storage, a unit over two stores, new units, buses, clock

#### Step 4.5: Testing crate and event store contract
Files: `Cargo.toml`, `crates/tarodomain-testing/Cargo.toml`, `crates/tarodomain-testing/src/lib.rs`, `crates/tarodomain-testing/src/fixtures.rs`, `crates/tarodomain-testing/src/event_store_contract.rs`
- [x] Workspace member added. Dependencies: `chrono`, `serde_json`, `tokio`; `tarodomain-local` as a dev-dependency for the crate doc's example, which runs the event store suite against it
- [x] Each suite is a `pub async fn` that takes a `ProviderFactory`: any closure returning a future of a provider with empty storage, such as `|| async { LocalProvider::new() }`. Each case asks it for a fresh provider
- [x] The crate builds for wasm32, because the Cloudflare harness (Step 10.8) runs the suites inside workerd. Suites use only `tokio::join!` (and `futures`, from Step 4.7); a tokio runtime appears only in the `provider_contract_tests!` expansion (Step 4.8). Checked with `cargo check --target wasm32-unknown-unknown -p tarodomain-testing`; CI covers it once the harness build lands (Step 10.10)
- [x] Fixtures: fixed times from a documented epoch, event IDs that increase with `n`, a same-millisecond pair that sorts in reverse, aggregate and stream IDs, and a serialized event with every optional field set. Typed events wait for the first suite that needs them
- [x] Event store contract: versions from 1 and 0 for a missing stream, with metadata; a stale expected version conflicts (with its context), including two racing appends (`tokio::join!`); an empty append; non-aggregate streams and duplicate IDs fail atomically; events round-trip unchanged; `after_version` and `max_count`; category and `$all` reads; two same-millisecond IDs that sort in reverse come back in append order; a cursor whose event was deleted; load by correlation ID; delete stream; subscription create, ack, list, delete and failed attempts

#### Step 4.6: Store contracts
Files: `crates/tarodomain-testing/src/lib.rs`, `crates/tarodomain-testing/src/snapshot_contract.rs`, `crates/tarodomain-testing/src/saga_store_contract.rs`, `crates/tarodomain-testing/src/idempotency_contract.rs`, `crates/tarodomain-testing/src/dead_letter_contract.rs`
- [x] The provider tells time by its own clock (the system clock on D1 and SQL), so the suites never compare with fixed times: they compare with the timestamps the store returned, or with a cut-off long ago or far in the future, and check list order as a property (sorted by time, then by the tie-breakers)
- [x] Snapshots: the stored fields, upsert, latest (the highest version, not the last saved), at a version, per-type reads, list newest first, `delete_before` keeps the cut-off and returns the count, `delete_all`, `count`
- [x] Sagas: the stored fields and timestamp rules, a terminal save sets `completed_at`, idempotent partial `complete` / `fail` (and on a missing saga), `load_by_status` order and the type filter before the limit, `load_timed_out` (active only, exclusive cut-off, type filter before limit), `delete`, `count`
- [x] Processed events and commands: idempotent marks keep the first record, `exists` per instance, `count`, `delete_by_handler`, the command list order, `delete_old` with an exclusive cut-off
- [x] Dead letters: the stored fields, IDs increase and aren't reused, `list` in ID order, `get` returns the latest, `get_by_id`, `delete`, `count`, `delete_old`
- [x] The crate doc's example runs all five suites against the local provider; a mutation (command marks overwriting instead of keeping the first) fails the idempotency suite

#### Step 4.7: Unit of work and concurrency contracts
Files: `crates/tarodomain-testing/Cargo.toml`, `crates/tarodomain-testing/src/lib.rs`, `crates/tarodomain-testing/src/unit_of_work_contract.rs`, `crates/tarodomain-testing/src/concurrency_contract.rs`
- [x] Dependency: `futures` (`join_all`). Still builds for wasm32
- [x] Unit of work: commit keeps writes; rollback discards the writes of **every** store (one row each: event, subscription, snapshot, saga, processed event, processed command, dead letter); the work's error comes back unchanged; reads inside the unit see its writes and readers outside don't (`tokio::join!` with oneshot channels); a nested `execute` joins and never begins; a stale append inside a unit rolls back its other writes; two racing units on one stream let exactly one win. A provider without units passes trivially
- [x] Concurrency: four appenders plus a reader looping `load_all` with a cursor (IDs deliberately out of append order); the reader sees every event exactly once, in append order. Four writers retrying on conflict lose no update. 16 concurrent readers never fail and agree. A runtime-free `yield_now` makes the joined futures interleave

#### Step 4.8: Provider contract and the local provider's run
Files: `crates/tarodomain-testing/src/lib.rs`, `crates/tarodomain-testing/src/provider_contract.rs`, `crates/tarodomain-local/Cargo.toml`, `crates/tarodomain-local/tests/contract.rs`, `README.md`
- [x] Provider: `provider_type`, adapters share storage, `initialize` can run again without losing data, buses (the strong one subscribable, an unknown transport rejected), a new unit per call that hasn't begun, a plausible clock
- [x] `provider_contract_tests!(factory)` expands every suite into a plain `#[test]` named after it, which runs the suite on a current-thread runtime from the testing crate (`__private::run`), so a provider crate needs no tokio dev-dependency
- [x] The local provider passes every suite (`crates/tarodomain-local/tests/contract.rs`), also with its database's clock stopped, which shows the suites rely on no time passing. Mutations (cursor reads by ID instead of position; a rollback that commits) fail the event store, concurrency and unit of work suites
- [x] README: crate status

### Phase 5: Event sourcing (§3.3–3.4, §6, §7)

#### Step 5.1: Event registry and upcasters
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/event_sourcing/mod.rs`, `crates/tarodomain/src/event_sourcing/registry.rs`, `crates/tarodomain/src/domain/family.rs`
- [ ] `register::<E>()` at `E::VERSION`. A second registration of the same name is `DUPLICATE_EVENT_REGISTRATION`
- [ ] `register_upcaster(name, from, to, fn(Value) -> Result<Value>)`. Upcasters apply one step at a time up to the registered version; a gap in the chain is `EVENT_VERSION_MISMATCH`
- [ ] `deserialize(&SerializedEvent) -> Result<DomainEvent>`: an unknown type is `UNKNOWN_EVENT_TYPE`, and a serde failure is `SERIALIZATION_ERROR`
- [ ] `knows(name, version)` and `names()`
- [ ] `event_family!` also generates `register_all(&mut EventRegistry)`
- [ ] Tests: the v1.0.0 → v1.1.0 → v1.2.0 chain, a missing step, an unknown type, a duplicate

#### Step 5.2: EventStore wrapper
Files: `crates/tarodomain/Cargo.toml`, `crates/tarodomain/src/event_sourcing/mod.rs`, `crates/tarodomain/src/event_sourcing/event_store.rs`, `crates/tarodomain/tests/event_store.rs`
- [ ] Dev-dependencies: `tarodomain-local`, `tarodomain-testing`, tokio with `rt-multi-thread`
- [ ] `EventStore` wraps `Arc<dyn EventStoreAdapter>` and `Arc<EventRegistry>`. It refuses to append an event whose type or version isn't registered, so nothing is stored that can't be read back
- [ ] Every load method deserializes through the registry
- [ ] Integration tests on `LocalProvider`

#### Step 5.3: Repositories
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/repository/mod.rs`, `crates/tarodomain/src/repository/event_sourced.rs`, `crates/tarodomain/src/repository/state.rs`, `crates/tarodomain/tests/repository.rs`
- [ ] `Repository<A>`: `load(&A::Id)` (`AGGREGATE_NOT_FOUND`), `load_or_none`, `save(&mut A)`, `exists`
- [ ] `EventSourcedRepository` load follows §6.2: a snapshot failure goes to `on_snapshot_error` and falls back to full replay, and the aggregate gets the ambient execution context
- [ ] Save: append at `committed_version()`, then `publish_all`, then `mark_changes_as_committed`, then snapshot (non-critical) when the save crosses a multiple of `snapshot_every`
- [ ] `StateRepository` trait: the user implements `load`, `save_state` and `exists`; the provided `save` calls `save_state`, publishes, and marks the changes committed
- [ ] Tests: not found, snapshot plus tail replay, fallback after a snapshot failure, the crossing-a-multiple rule, a concurrent save conflicts, the published events are the appended ones

### Phase 6: Commands and pipelines (§4, §11, §14, §16)

#### Step 6.1: Handlers, registry and results
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/command/mod.rs`, `crates/tarodomain/src/command/handler.rs`, `crates/tarodomain/src/command/registry.rs`, `crates/tarodomain/src/command/result.rs`
- [ ] `CommandHandler<C>`: `async fn handle(&self, &CommandMessage<C>) -> Result<()>`
- [ ] `CommandContext`: the pipeline's consistency and event bus, and `repository::<A>()` for registered aggregates
- [ ] `CommandRegistry`: registers by `C::NAME` with a handler factory and an optional consistency override; `resolve`; `deserialize(SerializedCommand) -> AnyCommand` (§4.3). An unknown command is `COMMAND_HANDLER_NOT_REGISTERED`
- [ ] `CommandDispatchResult { command_id, status: Dispatched | Duplicate, events }` and `BatchDispatchResult { outcomes, skipped }`

#### Step 6.2: ConcurrencyChecker
Files: `crates/tarodomain/src/command/mod.rs`, `crates/tarodomain/src/command/concurrency.rs`
- [ ] `check(expected, actual, stream_id)`, `with_retry(op)`, `compute_delay(attempt)`
- [ ] Defaults: 3 retries after 10 / 20 / 40 ms, ±10% jitter, capped at 1 s. Only `OPTIMISTIC_CONCURRENCY` is retried. An `on_retry` hook, and an injectable sleeper and random source
- [ ] Tests with `RecordingSleeper`: the delays, giving up after 3 retries, a domain error isn't retried

#### Step 6.3: Pipelines and dispatcher
Files: `crates/tarodomain/src/command/mod.rs`, `crates/tarodomain/src/command/pipeline.rs`, `crates/tarodomain/src/command/dispatcher.rs`
- [ ] Strong (§11.1): retry → unit of work → `ExecutionContext::for_command` → idempotency check → fresh handler → `handle` → mark processed, all in one unit of work. Inside an active unit it joins and doesn't retry on its own
- [ ] Eventual (§11.2): retry → context → idempotency check → `handle` → mark processed
- [ ] A command already processed returns `Duplicate` without running the handler. Idempotency is checked again on every attempt
- [ ] `CommandDispatcher`: routes by per-command consistency (§11.3); `dispatch_all(continue_on_error)` always returns a result; `can_dispatch`

#### Step 6.4: Pipeline tests
Files: `crates/tarodomain/tests/support/mod.rs`, `crates/tarodomain/tests/support/bank.rs`, `crates/tarodomain/tests/command_pipeline.rs`
- [ ] Shared fixture domain: a small bank with an account aggregate, commands and handlers
- [ ] A forced conflict is retried and then succeeds
- [ ] Strong mode: a failing projection rolls back the events and the idempotency record
- [ ] A duplicate command ID is a no-op
- [ ] Eventual mode: the append survives a failing bus
- [ ] `dispatch_all` with and without `continue_on_error`

### Phase 7: Projections and sagas (§12, §13, §17)

#### Step 7.1: Projections
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/projection/mod.rs`, `crates/tarodomain/src/projection/projection.rs`, `crates/tarodomain/src/projection/shard.rs`
- [ ] `Projection` trait: `name()`, `routes(&mut ProjectionRoutes<Self>)`, `instance_id(&DomainEvent)`, `on_rebuild_start`, `on_rebuild_complete`
- [ ] `ProjectionRoutes::on::<E>(handler)` for `async fn(Arc<Self>, Recorded<E>) -> Result<()>`. A second route for the same event is `INVALID_CONFIGURATION`
- [ ] `ProjectionHandler<P>` implements `EventHandler` (§12.1): skip if processed; route by name; an unknown event is marked processed and reported to an `on_unhandled` hook (for `DispatchGuard`); a failed event is not marked
- [ ] `ShardProjection`: FNV-1a 32-bit hash of the shard key modulo the shard count. The handler is named `<name>-shard-<i>` and records events under the projection name with the shard as instance (§12.3)

#### Step 7.2: Saga state machine and compensations
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/saga/mod.rs`, `crates/tarodomain/src/saga/state_machine.rs`, `crates/tarodomain/src/saga/compensation.rs`
- [ ] `SagaState` (`Initial`, `Running`, `Compensating`, `Completed`, `Failed`), `HandlerKind`, and `transition()` for every §13.2 row. `Compensating` + `starts_when`, which the table leaves out, is `INVALID_SAGA_TRANSITION`
- [ ] `CompensationStack`: `define(name, factory)`, `push(name)`, `take_all()` (LIFO), `pop()`. The names serialize, and the factories run at compensation time **(Dart)**
- [ ] Tests: every state × kind cell, LIFO order

#### Step 7.3: Process manager
Files: `crates/tarodomain/src/saga/mod.rs`, `crates/tarodomain/src/saga/process_manager.rs`
- [ ] `ProcessManager` trait: `const SAGA_TYPE`, `type State: Default + Serialize + DeserializeOwned`, `routes(&mut SagaRoutes<Self>)`
- [ ] `SagaRoutes`: `starts_when`, `continue_when`, `compensate_when`, `completed_when`, `define_compensation`. Priority is completed > compensate > continue > starts
- [ ] `SagaInstance<P>`: saga ID, state, status, current step (`"<kind><EventName>"`), failure reason, compensations
- [ ] `handle(&DomainEvent) -> Result<Vec<AnyCommand>>` follows §13.1. Handlers run under `ExecutionContext::for_event`, so the commands carry the saga ID as correlation and the event as cause. Command IDs are derived from `"<saga type>/<event id>"`, the command's index and the event's `occurred_at` **(Dart)**. `mark_failed(reason)` ends the saga `Failed`
- [ ] `to_save_input()` and `restore(SagaRecord)` store the state together with the compensation names

#### Step 7.4: Saga event handler
Files: `crates/tarodomain/src/saga/mod.rs`, `crates/tarodomain/src/saga/event_handler.rs`
- [ ] The §13.3 flow, steps 1–9. Commands go through a `SagaCommandDispatch` function, so the rebuilder can pass one that discards them
- [ ] An event for a completed or failed saga is recorded and skipped, not an error. An event a running saga has no handler for is skipped without saving, so it doesn't push back the timeout **(Dart)**
- [ ] Per-saga mailbox in task-local storage **(Dart)**

#### Step 7.5: Projection and saga tests
Files: `crates/tarodomain/tests/support/mod.rs`, `crates/tarodomain/tests/support/transfer.rs`, `crates/tarodomain/tests/projection.rs`, `crates/tarodomain/tests/saga.rs`
- [ ] Projections: a redelivered event is skipped, an unknown event is marked, a failing handler is not marked, shards split deterministically
- [ ] TransferSaga end to end in both consistency modes: happy path; one- and two-step compensation in reverse order; a rejected first step; a redelivered event whose commands are skipped as duplicates; a crash between the withdrawal commit and the saga save moves the money only once; a strong-mode rollback of the whole saga
- [ ] Every invalid transition of §13.2 through the handler

### Phase 8: Catch-up and operations (§18–21, §28.3)

#### Step 8.1: Subscriptions and cron dispatcher
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/catch_up/mod.rs`, `crates/tarodomain/src/catch_up/subscription_manager.rs`, `crates/tarodomain/src/catch_up/cron_dispatcher.rs`
- [ ] `SubscriptionManager` (§18.2), including `get_lag_report` (pending events counted up to a limit, plus failed attempts)
- [ ] `CronDispatcher`: `register(handler, stream)`, `catch_up(name) -> CatchUpResult { processed, has_more, failed }`, `run_all`
- [ ] It reads raw events and deserializes them one at a time, so an unreadable event is skipped like any non-retryable failure instead of blocking the batch
- [ ] A retryable error stops the subscription until the next run. A non-retryable one is dead-lettered and the cursor moves past it
- [ ] An optional unit of work commits each event's handler and cursor move together. Opt-in `max_attempts` **(Dart)**

#### Step 8.2: Backpressure
Files: `crates/tarodomain/src/catch_up/mod.rs`, `crates/tarodomain/src/catch_up/backpressure.rs`
- [ ] `BackpressureController` (§18.4): one batch from each subscription in turn, a time budget (default 25 s), an event budget (default 10 000), passes per subscription, a throttle, and a progress callback (processed, rate, remaining)
- [ ] Tests with `ManualClock`: each budget stops the run, round-robin fairness

#### Step 8.3: Retry policy and dead-letter writer
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/dead_letter/mod.rs`, `crates/tarodomain/src/dead_letter/retry_policy.rs`, `crates/tarodomain/src/dead_letter/writer.rs`
- [ ] `RetryPolicy` (§21.3): 3 retries after 100 / 200 / 400 ms with ±10% jitter, only for retryable errors
- [ ] `DeadLetterWriter::execute_with_retry` (§21.2) dead-letters after the last attempt, or at once for a non-retryable error, and returns the error. If the dead-letter write also fails, it returns a `DEAD_LETTER` error so the caller doesn't ack

#### Step 8.4: Saga timeouts
Files: `crates/tarodomain/src/saga/mod.rs`, `crates/tarodomain/src/saga/timeout.rs`
- [ ] `SagaTimeout` event (`saga_type`, `saga_id`, `last_step`) with `correlation_id = saga_id`, and its ID and timestamp derived from the saga's type, ID and `updated_at`. It is never stored
- [ ] `SagaTimeoutScheduler`, one per saga type with its own timeout (§19.3). A saga with a `compensate_when::<SagaTimeout>` route gets the event through its `SagaEventHandler`; one without is failed with the reason `"timeout"`
- [ ] A non-retryable error from the handler fails the saga. A retryable one leaves it for the next run

#### Step 8.5: Rebuilders
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/rebuild/mod.rs`, `crates/tarodomain/src/rebuild/projection_rebuilder.rs`, `crates/tarodomain/src/rebuild/saga_rebuilder.rs`
- [ ] `ProjectionRebuilder` (§20.1): `rebuild`, `rebuild_all`, `rebuild_shards`, with `continue_on_error`, `reset_idempotency`, `call_hooks`, `batch_size` and `max_passes`. By default a failing event stops the rebuild
- [ ] `SagaRebuilder` (§20.2): `rebuild_instance`, `rebuild_type` (finds sagas by scanning for their start events), `rebuild_all`, with a dispatch that discards commands

#### Step 8.6: Correlation trace
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/trace.rs`
- [ ] `CorrelationTrace::trace`, `build_tree` and `format_tree` (§28.3)
- [ ] The tree infers command nodes between events through `command_causation_id` **(Dart)**

#### Step 8.7: Operations tests
Files: `crates/tarodomain/tests/catch_up.rs`, `crates/tarodomain/tests/dead_letter.rs`, `crates/tarodomain/tests/rebuild.rs`, `crates/tarodomain/tests/timeout.rs`
- [ ] A projection rebuilt in place, or fresh from the store, matches the live one after a transfer saga, in both modes
- [ ] A timed-out saga compensates (the refund runs, then it fails); one without a timeout handler is failed; a crash while saving the compensating saga, followed by a retry, refunds only once
- [ ] A handler that keeps failing is dead-lettered after `max_attempts`
- [ ] The transfer saga traces as a single chain from the root command to `TransferCompleted`

### Phase 9: DI, builder, validation, runtime and HTTP API (§23, §25, §26.1, §29)

#### Step 9.1: DI container
Files: `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/di.rs`
- [ ] `Token<T>` and `Container` with `bind`, `bind_singleton`, `bind_transient`, `bind_scoped`, `bind_instance`, `resolve`, `resolve_or_none`, `has` and `create_scope` (§23.2)
- [ ] A singleton is created with the container it was bound on; scoped and transient values with the container they are resolved from **(Dart)**
- [ ] `MISSING_DEPENDENCY`; `CIRCULAR_DEPENDENCY` with the path, detected across scopes **(Dart)**

#### Step 9.2: Validation
Files: `crates/tarodomain/Cargo.toml`, `crates/tarodomain/src/lib.rs`, `crates/tarodomain/src/framework/mod.rs`, `crates/tarodomain/src/framework/validation.rs`
- [ ] Dependency: `tracing` (the default sink for warnings)
- [ ] `FrameworkValidator` (§29.1) reports every error at once in one `INVALID_CONFIGURATION`: duplicate aggregates, commands, sagas or projections; a state aggregate without a repository; a saga without `starts_when`; a non-positive timeout; a handler for an unregistered event
- [ ] Warnings go to a callback: a saga that never completes, a `SagaTimeout` handler without a timeout
- [ ] `DispatchGuard` (§29.2): `validate_command`, `validate_event`, and `validate_projection_dispatch` in `Skip`, `Warn` or `Strict` mode

#### Step 9.3: Builder
Files: `crates/tarodomain/src/framework/mod.rs`, `crates/tarodomain/src/framework/builder.rs`, `crates/tarodomain/src/framework/aggregate_config.rs`
- [ ] The §25.1 DSL: `use_provider`, `consistency(mode, transport)`, `snapshot_every`, `aggregate::<A>(|a| a.event_sourced().snapshot_every(n))` or `.state(repository_factory)`, `event::<E>()`, `upcaster(…)`, `command::<C>(factory, consistency)`, `projection(factory)`, `saga::<P>(SagaOptions { timeout })`, the `bind_*` methods, `validation(projection_mode)`, `skip_validation`
- [ ] Also `on_warning`, `concurrency`, `clock`, `sleeper`, `catch_up_batch_size`, `catch_up_max_attempts`. An explicit `clock` or `sleeper` overrides the provider's
- [ ] The global `snapshot_every` applies to every event-sourced aggregate. Setting it explicitly on a state aggregate logs a warning

#### Step 9.4: Runtime
Files: `crates/tarodomain/src/framework/mod.rs`, `crates/tarodomain/src/framework/taro_domain.rs`, `crates/tarodomain/tests/framework.rs`
- [ ] `build().await -> Result<TaroDomain>`: initializes the provider and wires two buses, two repositories per aggregate, two pipelines, projections and sagas on the strong bus, the cron dispatcher, the timeout schedulers, and a container with the framework and provider bound
- [ ] `dispatch`, `dispatch_all`, `handle_events(Vec<SerializedEvent>)` (the queue consumer, §10.5, with an ack / retry / dead-letter outcome per message), `run_scheduled()` (catch-up, then saga timeouts), `consistency_for::<C>()`, and accessors for `di`, `event_store`, both buses, `commands` and `cron` (§25.2)
- [ ] `run_cleanup()`: deletes processed commands older than 7 days, and processed events and dead letters older than 30 (§14, §21). `migrate()` runs `Provider::initialize()` again
- [ ] A provider without a unit of work, when any command needs strong consistency, is `INVALID_CONFIGURATION`

#### Step 9.5: HTTP API
Files: `crates/tarodomain/src/framework/mod.rs`, `crates/tarodomain/src/framework/http.rs`, `crates/tarodomain/tests/http_api.rs`
- [ ] `HttpApi::new(framework)` handles plain `HttpRequest { method, path, headers, body }` and returns `HttpResponse { status, headers, body }`. No HTTP crate, so the Cloudflare and axum hosts each convert in a few lines
- [ ] The §26.1 routes: `POST /commands/{name}` (JSON body through `CommandRegistry::deserialize`; `x-request-id` becomes the correlation ID), `GET /health`, `POST /migrate`. Migrate is off unless `allow_migrate(true)`
- [ ] Errors are rendered with `to_http_response` (§22.3). An unknown route is 404, and a wrong method is 405
- [ ] Tests on the local provider: a dispatch, a domain error with its context, an infrastructure error without it, migrate off by default

#### Step 9.6: Bank example, domain
Files: `Cargo.toml`, `examples/bank-account/Cargo.toml`, `examples/bank-account/src/lib.rs`, `examples/bank-account/src/account.rs`, `examples/bank-account/src/transfer.rs`
- [ ] Workspace member added
- [ ] `BankAccount` (open, deposit, withdraw, which raises `WithdrawalRejected` when funds are short) and `MoneyTransfer` (request, complete, fail), with typed IDs, commands and handlers
- [ ] A custom domain error code (`INSUFFICIENT_FUNDS`)
- [ ] The library builds for wasm32 (`cargo check --target wasm32-unknown-unknown -p bank-account --lib`), because the example worker reuses it. Its `tarodomain-local` dependency and the binary's tokio runtime (`current_thread`) must not break that

#### Step 9.7: Bank example, saga, projection and scenario
Files: `examples/bank-account/src/lib.rs`, `examples/bank-account/src/saga.rs`, `examples/bank-account/src/projection.rs`, `examples/bank-account/src/main.rs`, `examples/bank-account/tests/bank.rs`
- [ ] `TransferSaga` with a `refund` compensation and a `SagaTimeout` handler; `BalanceProjection`; `register_bank(builder)`
- [ ] `BalanceProjection` writes through a small `BalanceStore` trait with idempotent upserts and increments (§14.3), so the example worker can back it with D1 (Step 10.9). The in-memory store lives here
- [ ] `cargo run -p bank-account` runs the spec scenario: open, deposit, transfer saga, projection query, correlation trace
- [ ] Tests in both modes, including a failed transfer, a projection rebuild and the timeout refund

### Phase 10: Cloudflare provider, `tarodomain-cloudflare` (§24.2–24.3, §26.1, §27.2)

Needs Node.js for `wrangler` from Step 10.8 on. Every D1 adapter step is tested natively on SQLite through the backend of Step 10.2, and Step 10.8 runs the same suites on D1 itself.

#### Step 10.1: Crate and platform glue
Files: `Cargo.toml`, `crates/tarodomain-cloudflare/Cargo.toml`, `crates/tarodomain-cloudflare/src/lib.rs`, `crates/tarodomain-cloudflare/src/platform.rs`, `.github/workflows/ci.yml`
- [ ] Workspace member added. `worker` (workers-rs) becomes a workspace dependency with its D1 and Queue features, used only on wasm32
- [ ] Code that touches `worker` sits behind `cfg(target_arch = "wasm32")`, so the crate also builds natively and `cargo test --workspace` covers its logic
- [ ] `platform.rs`: `WorkerSleeper` on `worker::Delay`; `worker::Error` → `FrameworkError::infrastructure`; the `SendWrapper` / `SendFuture` helpers every adapter uses (design table). No `unsafe` in this crate
- [ ] CI's wasm check covers the crate

#### Step 10.2: Statement layer
Files: `crates/tarodomain-cloudflare/Cargo.toml`, `crates/tarodomain-cloudflare/src/lib.rs`, `crates/tarodomain-cloudflare/src/d1.rs`, `crates/tarodomain-cloudflare/src/sqlite_backend.rs`, `crates/tarodomain-cloudflare/tests/d1_scan.rs`
- [ ] `D1Backend` trait (`run`, `all`, `batch`) with two implementations: workers-rs's `D1Database` on wasm32, and rusqlite (`batch` is one transaction) behind a `native-sqlite` feature that the crate's own tests turn on. The adapters' real SQL then runs under `cargo test`
- [ ] `D1Db` is the only code that runs statements. Reads run at once. Writes run at once outside a unit of work and are staged inside one (§11.4)
- [ ] Multi-row inserts are split to fit D1's bound-parameter limit per statement
- [ ] A constraint violation in a batch comes back as a typed error the adapters can map
- [ ] Source-scan test: no other file under `src/` calls a backend **(Dart)**

#### Step 10.3: Unit of work and schema
Files: `crates/tarodomain-cloudflare/src/lib.rs`, `crates/tarodomain-cloudflare/src/unit_of_work.rs`, `crates/tarodomain-cloudflare/src/migrations.rs`, `crates/tarodomain-cloudflare/migrations/0001_init.sql`
- [ ] `D1UnitOfWork`: `begin`, `commit` (every staged statement in one `batch`), `rollback` (drops them), a nested unit joins, `after_commit` callbacks
- [ ] The overlay: staged events per stream, staged saga records, and staged processed-event and processed-command markers, readable by the adapters until the unit ends (design table)
- [ ] Schema: the §27.1 tables prefixed `tarodomain_`, following §27.2. `events` has `position INTEGER PRIMARY KEY AUTOINCREMENT` and a unique `event_id`, and folds in `stream_events` (`stream_id` and `stream_version`, unique together) **(Dart)**. `subscriptions` adds `last_position`, `failed_event_id` and `failed_attempts`. Plus `locks` for job leases, and `schema_migrations`
- [ ] `migrations.rs` embeds the SQL (`include_str!`), applies each missing migration in one `batch` and records it in `schema_migrations`. Running it twice is a no-op. When two isolates migrate at once, the loser's batch fails on the `schema_migrations` key, and it checks again instead of failing
- [ ] Tests on SQLite: commit, rollback, reads through the overlay, migrating twice

#### Step 10.4: D1 event store adapter
Files: `crates/tarodomain-cloudflare/src/lib.rs`, `crates/tarodomain-cloudflare/src/event_store.rs`
- [ ] Append in one batch, or staged in the active unit: the events with their stream versions, the stream row, and the guard statement. A constraint failure is `OPTIMISTIC_CONCURRENCY` with the stream, the expected version and the actual one (read again)
- [ ] Stream, category and `$all` reads ordered by position. An `after_event_id` cursor resolves to its position
- [ ] Inside a unit, stream reads and stream versions include the overlay's staged events
- [ ] Subscriptions: idempotent subscribe, ack only moves forward, `record_failed_attempt`

#### Step 10.5: Other D1 adapters
Files: `crates/tarodomain-cloudflare/src/lib.rs`, `crates/tarodomain-cloudflare/src/snapshot_store.rs`, `crates/tarodomain-cloudflare/src/saga_store.rs`, `crates/tarodomain-cloudflare/src/idempotency.rs`, `crates/tarodomain-cloudflare/src/dead_letter.rs`
- [ ] Idempotent writes with `ON CONFLICT DO NOTHING` / `DO UPDATE` (§14.3)
- [ ] Inside a unit, saga loads and idempotency checks read the overlay first
- [ ] The same semantics as the in-memory adapters

#### Step 10.6: Queue event bus and provider
Files: `crates/tarodomain-cloudflare/src/lib.rs`, `crates/tarodomain-cloudflare/src/queue_bus.rs`, `crates/tarodomain-cloudflare/src/provider.rs`, `crates/tarodomain-cloudflare/tests/contract.rs`
- [ ] `CloudflareQueueEventBus`: `publish_all` sends `SerializedEvent`s with `send_batch`. Inside a unit of work it sends from `after_commit`, so a rolled-back batch never reaches the queue. An event over the queue's message size limit is logged and left to the cron catch-up
- [ ] `CloudflareProvider::new(&env, bindings)` (the D1 binding name and an optional queue binding). `provider_type()` is `"cloudflare"`, `create_unit_of_work()` returns a `D1UnitOfWork`, the eventual bus is the queue bus and the strong bus an `InMemoryEventBus`, and `sleeper()` is `WorkerSleeper`
- [ ] `initialize()` applies missing migrations once per isolate
- [ ] `provider.db()` gives applications the same `D1Db`, so read-model writes join the unit's batch in strong mode
- [ ] `CloudflareProvider::with_backend(backend)` for native tests, with a recording queue
- [ ] `tests/contract.rs`: every contract suite passes on SQLite (`provider_contract_tests!`)

#### Step 10.7: Worker entry points
Files: `crates/tarodomain-cloudflare/src/lib.rs`, `crates/tarodomain-cloudflare/src/entry.rs`, `crates/tarodomain-cloudflare/src/lease.rs`
- [ ] `CloudflareWorker::new(framework)` with `fetch`, `queue` and `scheduled`, called from a Worker crate's `#[event(...)]` functions
- [ ] `fetch` converts `worker::Request` and `Response` to and from `HttpApi` (Step 9.5)
- [ ] `queue`: `handle_events` on the batch, then per message `ack`, `retry_with_options` with the delay from `RetryPolicy` and `message.attempts()`, or dead-letter and `ack` for a non-retryable error (§10.5)
- [ ] `scheduled`: the cron expression picks `run_scheduled()` (catch-up, then timeouts) or `run_cleanup()` (`cleanup_cron(…)`, daily by default). Each job holds a lease in `tarodomain_locks`, so an overlapping run skips
- [ ] The backpressure time budget (25 s by default) stays below the Worker's CPU limit
- [ ] Measure `build()` per request, and cache the framework per isolate if it's significant
- [ ] Native tests of the pure parts: outcome → ack or retry, cron selection, lease expiry

#### Step 10.8: Contract harness in workerd
Files: `Cargo.toml`, `harness/cloudflare-contract/Cargo.toml`, `harness/cloudflare-contract/src/lib.rs`, `harness/cloudflare-contract/wrangler.toml`, `scripts/cloudflare-contract.sh`
- [ ] Workspace member added (a `cdylib` Worker)
- [ ] `POST /suites/{name}` runs one `tarodomain-testing` suite against `CloudflareProvider` on a local D1 that it empties first. A panic becomes a 500 with the message
- [ ] The script builds the harness, starts `wrangler dev` (local mode, no Cloudflare account needed), calls every suite, and exits non-zero on any failure
- [ ] Every suite passes on D1 itself, confirming the SQLite runs of Step 10.6

#### Step 10.9: Example worker
Files: `Cargo.toml`, `examples/bank-account-worker/Cargo.toml`, `examples/bank-account-worker/src/lib.rs`, `examples/bank-account-worker/wrangler.toml`, `examples/bank-account-worker/README.md`
- [ ] Workspace member added
- [ ] The bank domain from `examples/bank-account` behind `CloudflareWorker`, with a D1 `BalanceStore` that writes through `provider.db()`
- [ ] `wrangler.toml`: the D1 binding, the queue as producer and consumer (with a Cloudflare dead-letter queue as a last resort), and two cron triggers (catch-up every minute, cleanup daily). It is the template to copy, replacing §26.3's generators
- [ ] README: `npx wrangler dev`, a curl walkthrough in which an eventual deposit reaches the projection through the queue, and deploying (`wrangler d1 create`, `wrangler queues create`, `wrangler deploy`)
- [ ] The README records the compressed bundle size against the Workers limit (Risks: Worker size)

#### Step 10.10: Workers CI
Files: `.github/workflows/ci.yml`, `README.md`
- [ ] The wasm check adds `tarodomain-testing`, the harness and the example worker
- [ ] A `workers` job: Node, `worker-build`, then `scripts/cloudflare-contract.sh`
- [ ] README: crate table status, and a Cloudflare development section
- [ ] CI green

### Phase 11: SQL provider, `tarodomain-sqlx` (§24, §27)

#### Step 11.1: Crate and schema
Files: `Cargo.toml`, `crates/tarodomain-sqlx/Cargo.toml`, `crates/tarodomain-sqlx/src/lib.rs`, `crates/tarodomain-sqlx/migrations/sqlite/0001_init.sql`, `crates/tarodomain-sqlx/migrations/postgres/0001_init.sql`
- [ ] Workspace member added; cargo features `sqlite` and `postgres`
- [ ] The §27.1 tables, prefixed `tarodomain_`: `events`, `streams`, `snapshots`, `subscriptions`, `processed_events`, `sagas`, `dead_letters`, `processed_commands`, plus `counters` (the sequencer) and `locks` (job leases). sqlx migrations replace `schema_migrations`
- [ ] `stream_events` is folded into `events` (`stream_id`, `stream_version`, unique together), and `events.position` is nullable and unique **(Dart)**. `subscriptions` also has `last_position`, `failed_event_id` and `failed_attempts`
- [ ] SQLite starts from the D1 schema of Step 10.3. PostgreSQL uses `uuid`, `jsonb` and `timestamptz`

#### Step 11.2: Transaction plumbing
Files: `crates/tarodomain-sqlx/src/lib.rs`, `crates/tarodomain-sqlx/src/db.rs`, `crates/tarodomain-sqlx/src/unit_of_work.rs`, `crates/tarodomain-sqlx/tests/db_scan.rs`
- [ ] `Db` is the only code that runs queries. It uses the task-local transaction when a unit of work is active and the pool otherwise
- [ ] `SqlxUnitOfWork`: `begin` / `commit` / `rollback`, a nested unit joins, `after_commit` callbacks
- [ ] A source-scan test fails if any other file under `src/` executes queries directly **(Dart)**

#### Step 11.3: Event store adapter and sequencer
Files: `crates/tarodomain-sqlx/src/lib.rs`, `crates/tarodomain-sqlx/src/event_store.rs`, `crates/tarodomain-sqlx/src/sequencer.rs`
- [ ] Append: a conditional `UPDATE … WHERE stream_version = expected`, or insert-if-absent for a new stream. No row affected is `OPTIMISTIC_CONCURRENCY`. Events are inserted with `position` NULL
- [ ] `Sequencer` assigns positions after commit, in its own transaction, by claiming a counter row (see Risks). `load_all` and `load_stream_after_event` return only positioned events, ordered by position, and run `try_run` first when outside a unit of work
- [ ] A subscription ack only moves `last_position` forward

#### Step 11.4: Other SQL adapters
Files: `crates/tarodomain-sqlx/src/lib.rs`, `crates/tarodomain-sqlx/src/snapshot_store.rs`, `crates/tarodomain-sqlx/src/saga_store.rs`, `crates/tarodomain-sqlx/src/idempotency.rs`, `crates/tarodomain-sqlx/src/dead_letter.rs`
- [ ] Idempotent writes with `ON CONFLICT DO NOTHING` / `DO UPDATE` (§14.3)
- [ ] The same semantics as the in-memory adapters

#### Step 11.5: Provider, contracts and local database
Files: `.github/workflows/ci.yml`, `compose.yaml`, `crates/tarodomain-sqlx/src/provider.rs`, `crates/tarodomain-sqlx/tests/sqlite_contract.rs`, `crates/tarodomain-sqlx/tests/postgres_contract.rs`
- [ ] `SqlxProvider::sqlite(pool)` and `SqlxProvider::postgres(pool)`. `initialize()` runs the migrations
- [ ] `compose.yaml`: one `postgres:16` service on a non-default host port, so it doesn't collide with other projects' databases. Started with `docker --context kiri compose up -d` (Decisions), and it works with any Docker-API engine
- [ ] Every contract suite passes on SQLite and on PostgreSQL (`DATABASE_URL`; locally the compose service, in CI a service container). The PostgreSQL tests skip with a message when `DATABASE_URL` is unset
- [ ] PostgreSQL only: a reader makes progress while another transaction stays open

#### Step 11.6: Eventual bus and background jobs
Files: `crates/tarodomain-sqlx/src/lib.rs`, `crates/tarodomain-sqlx/src/event_bus.rs`, `crates/tarodomain-sqlx/src/jobs.rs`
- [ ] Eventual bus: the events are already durable in the store, so publishing only wakes the catch-up worker (`tokio::sync::Notify`). The scheduled catch-up is the safety net (§15.2)
- [ ] Jobs: catch-up (sequencer, then dispatcher), saga timeouts, and `run_cleanup()`. Each job runs under a lease in `tarodomain_locks`

### Phase 12: Native entry points (§26)

#### Step 12.1: Worker loop
Files: `crates/tarodomain/src/framework/mod.rs`, `crates/tarodomain/src/framework/worker.rs`, `crates/tarodomain/tests/worker.rs`
- [ ] `Worker::new(framework)` with `catch_up_every`, `timeouts_every` and `cleanup_every`, and `run(shutdown)`. It wakes early when the eventual bus signals
- [ ] Native only (`cfg(not(target_arch = "wasm32"))`): it uses tokio's timers. On Cloudflare, Cron Triggers do this job
- [ ] Tests: a shutdown signal stops it, and an eventual dispatch reaches the projection without waiting for the interval

#### Step 12.2: HTTP adapter (axum)
Files: `Cargo.toml`, `crates/tarodomain-axum/Cargo.toml`, `crates/tarodomain-axum/src/lib.rs`, `crates/tarodomain-axum/tests/http.rs`
- [ ] Workspace member added
- [ ] `router(framework)` serves `HttpApi` (Step 9.5): a catch-all handler converts the axum request and response, so the routes, the `x-request-id` handling and the error rendering stay in the core
- [ ] Tests through `tower::ServiceExt::oneshot`

#### Step 12.3: Example server
Files: `Cargo.toml`, `examples/bank-account-server/Cargo.toml`, `examples/bank-account-server/src/main.rs`, `README.md`
- [ ] Workspace member added
- [ ] The bank domain from `examples/bank-account` on SQLite through `tarodomain-sqlx`, served by axum with the worker running
- [ ] A curl walkthrough in the README. An eventual deposit reaches the read model within one catch-up tick

### Phase 13: Documentation

#### Step 13.1: Guides
Files: `README.md`, `docs/GETTING_STARTED.md`, `docs/CLOUDFLARE.md`, `crates/tarodomain/src/lib.rs`, `Cargo.toml`
- [ ] Remove the `unused_workspace_dependencies = "allow"` from the root `Cargo.toml` (added in Step 1.1) and drop any shared dependency nothing uses
- [ ] Getting started: define events, an aggregate, commands, a projection and a saga; build; dispatch
- [ ] Cloudflare: bindings, migrations, strong and eventual mode on D1 and Queues, read models through `provider.db()`, cron schedules, D1 limits, deploying
- [ ] Using the private crates from another repository through a git dependency pinned to a tag
- [ ] The crate-level example compiles as a doctest, and `cargo doc` is warning-free

## Risks

### Global ordering under concurrency (PostgreSQL, step 11.3)

A position handed out at insert time follows insert order, but readers see events in commit order. A reader that sees position 11 and moves its cursor past it skips position 10 if 10 commits later. In strong mode the append's transaction also runs projections, sagas and the commands sagas dispatch, so there's no bound on how long it stays open.

**Resolution (from the Dart port):** appends leave `position` NULL. The `Sequencer` assigns positions in its own transaction: it reads the `event-position` counter, takes the unpositioned events it can see in insert order, claims the counter with `UPDATE … WHERE value = <read value>`, and numbers the events from there. A transaction only sees committed rows, so no event gets a position before it commits, and none committed later gets a lower one. A sequencer that loses the claim retries (`run`), or stops because the winner is already positioning the same events (`try_run`, used by reads). The counter never hands out a position twice, unlike `max(position) + 1` after a stream is deleted. The concurrency contract (step 4.7) checks it on every provider. D1 and SQLite run one write at a time, so insert order is commit order there; D1 uses an `AUTOINCREMENT` position instead (Step 10.3).

### UUIDv7 order within one millisecond

Two UUIDv7s from the same millisecond can sort either way, and other servers' clocks disagree. On Workers, `Date.now()` only advances after I/O, so every ID from one stretch of computation shares a millisecond. Nothing relies on that order: per-stream order comes from the stream version and global order from the append position. Step 4.5 has a contract test with two same-millisecond IDs that sort in reverse.

### Task-local context and `tokio::spawn`

`ExecutionContext` and the active unit of work are task-local. A future passed to `tokio::spawn` starts with neither, so events raised in it start new traces and its writes run outside the transaction. The framework never spawns inside a dispatch. The rustdoc on `ExecutionContext` shows how to propagate it explicitly (`let ctx = ExecutionContext::current(); tokio::spawn(ExecutionContext::scope(ctx, work))`). The same applies to a future handed to a Worker's `ctx.wait_until`.

### Dev-dependency cycle

`tarodomain` dev-depends on `tarodomain-local`, which depends on `tarodomain`. In `#[cfg(test)]` unit tests that compiles two copies of the core crate whose types don't match. Integration tests under `tests/` link the one library both crates use. **Rule:** a test that needs the local provider goes in `crates/tarodomain/tests/`.

### A query that forgets the transaction (SQL and D1 providers)

An adapter that runs a query on the pool instead of the active transaction escapes the unit of work on PostgreSQL. On SQLite, which has one writer, it waits on the open transaction and hangs. On D1 it writes at once instead of joining the batch, so a rollback leaves it behind. **Resolution (from the Dart port):** one way in (`D1Db`, step 10.2; `Db`, step 11.2), a source-scan test that enforces it, and the unit of work contract's rollback test, which covers the writes of every store.

### D1 has no interactive transactions (steps 10.2–10.6)

D1 can't hold a transaction open across awaits; only one `batch` is atomic. So `D1UnitOfWork` stages every write and sends them in one batch at commit, and anything read inside the unit sees the database as it was before the unit began. **Resolution:** the overlay covers what the framework reads back inside a unit: stream events and versions, saga records, and idempotency markers. Read models are the application's: on D1 they must write through `provider.db()`, so their writes join the batch, and use upserts and increments (`balance = balance + ?`, §14.3) instead of read-then-write. The unit of work contract (rollback across every store) and the strong-mode transfer saga on D1, which creates and reloads the transfer aggregate and its saga in one unit, both cover it.

### D1 limits

D1 limits bound parameters per statement, statements per batch, queries per Worker invocation and response size, and those limits differ by plan and change over time. A strong-mode chain (a saga dispatching several commands) produces one large batch. **Resolution:** split multi-row inserts (Step 10.2), add a contract test with a large append, and check the current limits in Cloudflare's documentation when implementing Phase 10. Strong mode on D1 suits short chains; long sagas should run eventual.

### `SendWrapper` on Workers

workers-rs's `SendWrapper` claims `Send` for JavaScript handles. That holds only because a Worker isolate runs on one thread. **Rule:** wrappers are created only from `Env` bindings inside `tarodomain-cloudflare`, never in code that also runs natively.

### Worker size and cold start

A Worker's compressed size is capped, and a large wasm module slows cold starts. The core plus serde, chrono and uuid should fit, but nothing has measured it yet. **Resolution:** Step 10.9 records the bundle size of the example worker. If it's close to the limit, use the release profile (`opt-level = "z"`, LTO), let `worker-build` run `wasm-opt`, and trim dependency features.

### Kiri is new

Kiri is pre-1.0 (0.2.x in November 2026). The repo depends only on the Docker Engine API through `compose.yaml`, so if Kiri misbehaves, another engine runs the same file after a `docker context use`.

### Locks across `.await`

A `std::sync::Mutex` guard held across an `.await` can deadlock, and makes the future `!Send`. The local provider only locks in short synchronous sections. Serializing units of work uses an async lock (`tokio::sync::Mutex` or `Semaphore`). `clippy::await_holding_lock` is on through `clippy::all`.

### Boxed async traits

`async-trait` allocates a box for every adapter call. That's negligible next to I/O, but on the in-memory hot path it adds up. Revisit when dyn-compatible async traits are stable.

### Tooling

Rust is installed through `rustup` (cargo 1.100 on 2026-11-16), which reads `rust-toolchain.toml`, including the wasm32 target. Phase 10 still needs Node.js for `wrangler`; it isn't installed on the dev machine yet.

## Resolved questions

Decided on 2026-11-16 (details under [Decisions](#decisions)):

1. **Providers.** Cloudflare is in, as Phase 10, right after the core, ahead of sqlx. AWS stays out of scope.
2. **Publishing.** The crates stay private, consumed through git tags.
3. **Proc macros.** None. The `macro_rules!` macros stay, and the explicit `const NAME` per event is kept on purpose.

No open questions right now.
