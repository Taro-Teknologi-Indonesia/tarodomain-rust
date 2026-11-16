# TaroDomain (Rust)

Rust implementation of the [TaroDomain framework specification](spec/README.md): event sourcing, CQRS and DDD with strong or eventual consistency per command.

The spec is a git submodule at [`spec/`](spec), pinned to the version this code implements. Its upstream is [Taro-Teknologi-Indonesia/tarodomain](https://github.com/Taro-Teknologi-Indonesia/tarodomain). The [Dart implementation](https://github.com/Taro-Teknologi-Indonesia/tarodomain-dart) is the reference port.

**Status:** Phases 0–10 of the [implementation plan](docs/IMPLEMENTATION_PLAN.md): the whole core, the in-memory provider, the provider contract suites and the Cloudflare provider. The Cloudflare provider is tested on SQLite; its runs on D1 itself (`scripts/cloudflare-contract.sh`) and the example Worker under `wrangler dev` haven't been run yet. The plan also holds the roadmap, the step checklists and the places where Rust differs from the spec.

## Crates

| Crate | Spec sections | Contents | Status |
|---|---|---|---|
| [`tarodomain`](crates/tarodomain) | §2–23, §25, §28–29 | Domain primitives, event store wrapper and registry, aggregates, repositories, event bus, command pipelines, projections, sagas, idempotency, rebuild, DLQ, errors, DI, builder DSL | Done (Phases 1–9): the whole core |
| [`tarodomain-local`](crates/tarodomain-local) | §24 (LocalProvider) | In-memory adapters for tests and local development | Done (Phase 4); passes every contract suite |
| [`tarodomain-testing`](crates/tarodomain-testing) | §24 | Contract test suites that every provider must pass | Done (Phase 4) |
| [`tarodomain-cloudflare`](crates/tarodomain-cloudflare) | §24.2–24.3, §26.1, §27.2 | Cloudflare provider on workers-rs: D1, Queues, Cron Triggers, and the Worker entry points | Code done (Phase 10); passes every contract suite on SQLite. The suites on D1 itself are wired into CI and not yet run |
| `tarodomain-sqlx` | §24, §27 | SQLite and PostgreSQL provider on sqlx, for servers outside Cloudflare | Planned (Phase 11) |
| `tarodomain-axum` | §26 | Native HTTP entry point and worker loop | Planned (Phase 12) |
| `examples/bank-account` | — | The BankAccount / TransferMoney example used throughout the spec | Done |
| [`examples/bank-account-worker`](examples/bank-account-worker) | — | The same example deployed as a Cloudflare Worker; the template to copy | Done (Phase 10) |

`tarodomain` must stay platform-agnostic: it never depends on a provider, a database driver or an HTTP framework. It also builds for `wasm32-unknown-unknown`, the target Cloudflare Workers run.

## Development

Requires a Rust toolchain from [rustup](https://rustup.rs). `rust-toolchain.toml` selects stable with rustfmt, clippy and the `wasm32-unknown-unknown` target; the minimum supported version is 1.85 (edition 2024).

```sh
git clone --recurse-submodules <this repo>   # or, after a plain clone: git submodule update --init
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check --target wasm32-unknown-unknown -p tarodomain -p tarodomain-testing -p tarodomain-cloudflare \
  -p cloudflare-contract-harness -p bank-account -p bank-account-worker   # still builds for Cloudflare Workers
cargo doc --workspace --no-deps
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs the same commands, plus a `workers` job that runs the contract suites on D1 inside workerd.

### Cloudflare

Cloudflare development needs Node.js (for `wrangler`) and `worker-build` (`cargo install worker-build`), but no account and no containers: `wrangler dev` runs the Workers runtime with a local D1 and queue.

```sh
scripts/cloudflare-contract.sh               # every contract suite, on D1 itself
cd examples/bank-account-worker && npx wrangler dev   # the example Worker; its README has a curl walkthrough
```

The provider's adapters also run under plain `cargo test`: the crate's own tests use SQLite behind the same statement layer as D1 (`native-sqlite` feature), so no Node.js is needed for the day-to-day loop.

### Local containers

The PostgreSQL contract tests (Phase 11) need a local database. The repo will ship a plain `compose.yaml`, so any engine that speaks the Docker Engine API works. This project uses [Kiri](https://runkiri.com) instead of Docker Desktop:

```sh
docker --context kiri compose up -d
```

Cloudflare development needs no containers (see above).

To add a crate, create it under `crates/` with `version.workspace = true` (and the other `*.workspace` keys) plus `[lints] workspace = true`, then list it under `members` in the root `Cargo.toml`.

## Updating the spec

```sh
git submodule update --remote spec              # move to the latest upstream spec
git -C spec log -p --reverse HEAD@{1}..HEAD     # read what changed in the spec
```

Bring the code and [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) in line with the changes, then commit the new `spec` pointer in the same commit as the code that implements it.

## License

MIT, the same as the specification. See [LICENSE](LICENSE).
