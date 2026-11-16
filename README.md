# TaroDomain (Rust)

Rust implementation of the [TaroDomain framework specification](spec/README.md): event sourcing, CQRS and DDD with strong or eventual consistency per command.

The spec is a git submodule at [`spec/`](spec), pinned to the version this code implements. Its upstream is [Taro-Teknologi-Indonesia/tarodomain](https://github.com/Taro-Teknologi-Indonesia/tarodomain). The [Dart implementation](https://github.com/Taro-Teknologi-Indonesia/tarodomain-dart) is the reference port.

**Status:** bootstrap. See the [implementation plan](docs/IMPLEMENTATION_PLAN.md) for the roadmap, the step checklists and the places where Rust differs from the spec.

## Crates

| Crate | Spec sections | Contents | Status |
|---|---|---|---|
| [`tarodomain`](crates/tarodomain) | §2–23, §25, §28–29 | Domain primitives, event store wrapper and registry, aggregates, repositories, event bus, command pipelines, projections, sagas, idempotency, rebuild, DLQ, errors, DI, builder DSL | Skeleton |
| `tarodomain-local` | §24 (LocalProvider) | In-memory adapters for tests and local development | Planned (Phase 4) |
| `tarodomain-testing` | §24 | Contract test suites that every provider must pass | Planned (Phase 4) |
| `tarodomain-cloudflare` | §24.2–24.3, §26.1, §27.2 | Cloudflare provider on workers-rs: D1, Queues, Cron Triggers, and the Worker entry points | Planned (Phase 10) |
| `tarodomain-sqlx` | §24, §27 | SQLite and PostgreSQL provider on sqlx, for servers outside Cloudflare | Planned (Phase 11) |
| `tarodomain-axum` | §26 | Native HTTP entry point and worker loop | Planned (Phase 12) |
| `examples/bank-account` | — | The BankAccount / TransferMoney example used throughout the spec | Planned (Phase 9) |
| `examples/bank-account-worker` | — | The same example deployed as a Cloudflare Worker | Planned (Phase 10) |

`tarodomain` must stay platform-agnostic: it never depends on a provider, a database driver or an HTTP framework. It also builds for `wasm32-unknown-unknown`, the target Cloudflare Workers run.

## Development

Requires a Rust toolchain from [rustup](https://rustup.rs). `rust-toolchain.toml` selects stable with rustfmt, clippy and the `wasm32-unknown-unknown` target; the minimum supported version is 1.85 (edition 2024).

```sh
git clone --recurse-submodules <this repo>   # or, after a plain clone: git submodule update --init
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo check --target wasm32-unknown-unknown -p tarodomain   # still builds for Cloudflare Workers
cargo doc --workspace --no-deps
```

CI ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)) runs the same five commands.

### Local containers

The PostgreSQL contract tests (Phase 11) need a local database. The repo will ship a plain `compose.yaml`, so any engine that speaks the Docker Engine API works. This project uses [Kiri](https://runkiri.com) instead of Docker Desktop:

```sh
docker --context kiri compose up -d
```

Cloudflare development needs no containers: `wrangler dev` runs the Workers runtime and a local D1 database.

To add a crate, create it under `crates/` with `version.workspace = true` (and the other `*.workspace` keys) plus `[lints] workspace = true`, then list it under `members` in the root `Cargo.toml`.

## Updating the spec

```sh
git submodule update --remote spec              # move to the latest upstream spec
git -C spec log -p --reverse HEAD@{1}..HEAD     # read what changed in the spec
```

Bring the code and [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) in line with the changes, then commit the new `spec` pointer in the same commit as the code that implements it.

## License

MIT, the same as the specification. See [LICENSE](LICENSE).
