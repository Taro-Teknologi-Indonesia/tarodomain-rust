//! The native worker loop (spec §26): shutdown, the signal from the eventual
//! bus, and the intervals.

mod support;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tarodomain::adapters::ConsistencyMode;
use tarodomain::async_trait;
use tarodomain::domain::Recorded;
use tarodomain::error::Result;
use tarodomain::framework::{TaroDomain, Worker};
use tarodomain::projection::{Projection, ProjectionRoutes};
use tarodomain_local::LocalProvider;
use tokio::sync::{Notify, oneshot};

use support::bank::{
    AccountId, BankAccount, Counters, Deposit, DepositHandler, MoneyDeposited, OpenAccount,
    OpenAccountHandler,
};

#[derive(Default)]
struct Totals(Mutex<i64>);

impl Totals {
    async fn on_deposited(self: Arc<Self>, e: Recorded<MoneyDeposited>) -> Result<()> {
        *self.0.lock().unwrap() += e.cents;
        Ok(())
    }

    fn total(&self) -> i64 {
        *self.0.lock().unwrap()
    }
}

#[async_trait]
impl Projection for Totals {
    fn name(&self) -> &str {
        "totals"
    }

    fn routes(r: &mut ProjectionRoutes<Self>) {
        r.on::<MoneyDeposited, _, _>(Self::on_deposited);
    }
}

async fn bank() -> (TaroDomain, Arc<Totals>) {
    let totals = Arc::new(Totals::default());
    let shared = totals.clone();
    let domain = TaroDomain::builder()
        .use_provider(Arc::new(LocalProvider::new()))
        .consistency(ConsistencyMode::Eventual, None)
        .aggregate::<BankAccount>(|a| a.event_sourced())
        .command::<OpenAccount, _>(|_, context| OpenAccountHandler { context }, None)
        .command::<Deposit, _>(
            |_, context| DepositHandler {
                context,
                counters: Arc::new(Counters::default()),
            },
            None,
        )
        .projection(move |_| Ok(shared))
        .build()
        .await
        .unwrap();
    (domain, totals)
}

async fn deposit(domain: &TaroDomain, cents: i64) {
    let account = AccountId::generate();
    domain
        .dispatch(OpenAccount {
            account,
            owner: "Ann".into(),
        })
        .await
        .unwrap();
    domain.dispatch(Deposit { account, cents }).await.unwrap();
}

async fn until(what: &str, mut done: impl FnMut() -> bool) {
    for _ in 0..200 {
        if done() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test]
async fn a_shutdown_signal_stops_the_worker() {
    let (domain, _) = bank().await;
    let (stop, stopped) = oneshot::channel::<()>();
    let worker = Worker::new(domain);
    let task = tokio::spawn(async move {
        worker
            .run(async {
                stopped.await.ok();
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(30)).await;
    assert!(!task.is_finished());
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(5), task)
        .await
        .expect("the worker stops")
        .unwrap();
}

#[tokio::test]
async fn the_first_run_catches_up_on_what_was_missed() {
    let (domain, totals) = bank().await;
    deposit(&domain, 40).await;
    assert_eq!(totals.total(), 0);

    let (stop, stopped) = oneshot::channel::<()>();
    let worker = Worker::new(domain).catch_up_every(Duration::from_secs(3600));
    let task = tokio::spawn(async move {
        worker
            .run(async {
                stopped.await.ok();
            })
            .await;
    });
    until("the startup catch-up", || totals.total() == 40).await;
    stop.send(()).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn a_signal_reaches_the_projection_before_the_interval() {
    let (domain, totals) = bank().await;
    let signal = Arc::new(Notify::new());
    let (stop, stopped) = oneshot::channel::<()>();
    let worker = Worker::new(domain.clone())
        .catch_up_every(Duration::from_secs(3600))
        .timeouts_every(Duration::from_secs(3600))
        .wake_on(signal.clone());
    let task = tokio::spawn(async move {
        worker
            .run(async {
                stopped.await.ok();
            })
            .await;
    });
    // Let the startup run finish, so the next catch-up is an hour away.
    tokio::time::sleep(Duration::from_millis(50)).await;

    deposit(&domain, 25).await;
    assert_eq!(totals.total(), 0, "the interval is an hour");
    signal.notify_one();
    until("the signalled catch-up", || totals.total() == 25).await;
    stop.send(()).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn the_interval_catches_up_without_a_signal() {
    let (domain, totals) = bank().await;
    let (stop, stopped) = oneshot::channel::<()>();
    let worker = Worker::new(domain.clone()).catch_up_every(Duration::from_millis(20));
    let task = tokio::spawn(async move {
        worker
            .run(async {
                stopped.await.ok();
            })
            .await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    deposit(&domain, 15).await;
    until("the next interval", || totals.total() == 15).await;
    stop.send(()).unwrap();
    task.await.unwrap();
}
