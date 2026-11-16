# bank-account-worker

The [bank example](../bank-account) as a Cloudflare Worker, and the template to copy for your own: D1 stores the events, sagas and read model, a Queue delivers events to the projection in eventual mode, and two Cron Triggers run catch-up and cleanup.

| File | What to copy |
|---|---|
| [`src/lib.rs`](src/lib.rs) | `D1BalanceStore` (a read model that writes through `provider.db()`), and the `fetch` / `queue` / `scheduled` functions that build the framework once per isolate |
| [`wrangler.toml`](wrangler.toml) | The D1 binding, the queue as producer and consumer (with Cloudflare's dead-letter queue as a last resort) and the cron triggers |

The domain itself (`BankAccount`, `MoneyTransfer`, `TransferSaga`) is `bank-account`, unchanged: it doesn't know it runs on Cloudflare.

## Run it locally

Needs Node.js and `worker-build` (`cargo install worker-build`). No Cloudflare account: `wrangler dev` runs workerd and a local D1 and queue.

```sh
cd examples/bank-account-worker
npx wrangler dev
```

In another terminal. The commands are JSON bodies on `POST /commands/{name}`; an account ID is a UUID.

```sh
URL=http://localhost:8787
ANN=$(uuidgen | tr A-Z a-z)

curl -s $URL/health
curl -s -X POST $URL/commands/OpenAccount -d "{\"account\":\"$ANN\",\"owner\":\"Ann\"}"

# Eventual mode (the default): the command returns once the events are stored.
# The queue then delivers them to the balance projection, a moment later.
curl -s -X POST $URL/commands/Deposit -d "{\"account\":\"$ANN\",\"cents\":10000}"
sleep 3
curl -s $URL/balances        # [{"account":"…","owner":"Ann","cents":10000}]
```

Set `CONSISTENCY = "strong"` in `wrangler.toml` and the projection runs inside the command's D1 batch instead: `/balances` is up to date as soon as the command returns, and a failure rolls the events and the balance back together. Strong mode suits short chains; a long saga should run eventual (the plan's Risks: D1 limits).

Scheduled jobs: `npx wrangler dev --test-scheduled`, then `curl "http://localhost:8787/__scheduled?cron=*+*+*+*+*"` runs catch-up and the saga timeouts, and `cron=0+3+*+*+*` runs cleanup.

## Deploy

```sh
npx wrangler d1 create tarodomain-bank            # paste the database_id into wrangler.toml
npx wrangler queues create tarodomain-bank-events
npx wrangler queues create tarodomain-bank-events-dlq
npx wrangler deploy
```

The framework's tables are created on the first request (`provider.initialize()` applies the embedded migrations), and `D1BalanceStore::init` creates the example's own two tables.

## Bundle size

`cargo build --release --target wasm32-unknown-unknown -p bank-account-worker` (2026-11-16):

| | Size |
|---|---|
| `bank_account_worker.wasm`, as cargo writes it | 2.69 MB |
| the same, gzip -9 | 678 KB |

Workers cap the compressed size of a script at 3 MB on the Free plan and 10 MB on Paid, so the example uses under a quarter of the smaller limit. This is the figure before `worker-build` runs `wasm-bindgen` and `wasm-opt`, which only make it smaller (the manifest asks for `-Oz`). The tools weren't installed when this was measured; re-measure the shipped bundle (`build/worker/index.wasm`) after the first `wrangler dev`.
