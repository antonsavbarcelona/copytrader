//! Copy-trading of the Polyfox smart wallets on Polymarket: on paper, and live alongside it.
//!
//!     copytrader run    [--data DIR] [--stake 1] [--cap 0.02] [--sell-floor 0.05] [--market-gap-h 6]
//!                       [--top 10] [--top-days 3] [--top-min-closed 15]
//!     copytrader report [--data DIR] [--hours 24] [--live]
//!     copytrader check                     (the live keys: account and cash, no order)
//!     copytrader test-trade TOKEN [USD]    (a real buy of USD, default 1, then its sale)
//!
//! `run` follows every wallet on the Polyfox leaderboard (read again every 6 h) and copies
//! their buys and sales on paper against the live order book; see engine.rs for the rules.
//! Events go to DIR/events.jsonl, open lots to DIR/state.json, and with DATABASE_URL set
//! also to Postgres (see db.rs), from which a restart takes the state back.
//!
//! With the live keys in the environment (live.rs) and DATABASE_URL set, the same process
//! also copies for real: only the `--top` wallets whose paper copies did best over the last
//! `--top-days`, with their own trade stream and read-back, events in `live_events` and
//! DIR/live. LIVE=off leaves the keys unused.

mod api;
mod config;
mod db;
mod engine;
mod fill;
mod live;
mod report;
mod stream;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;

#[macro_export]
macro_rules! log {
    ($($a:tt)*) => {{
        let t = $crate::api::now() as u64 % 86400;
        println!("{:02}:{:02}:{:02} {}", t / 3600, t / 60 % 60, t % 60, format!($($a)*));
    }};
}

/// The run exits (and the host restarts it) when no followed wallet has traded this long.
const NO_TRADES_EXIT_S: u64 = 30 * 60;

type Wallets = Arc<RwLock<HashSet<String>>>;

async fn refresh_wallets(api: &api::Api, engine: &engine::Engine, wallets: &RwLock<HashSet<String>>,
    path: &std::path::Path, db: Option<&mpsc::UnboundedSender<db::Write>>) {
    match api.polyfox_wallets().await {
        Ok(list) => {
            if let Some(db) = db {
                let _ = db.send(db::Write::Wallets(list.iter().map(|w| (w.address.clone(), w.name.clone())).collect()));
            }
            let mut set: HashSet<String> = list.iter().map(|w| w.address.clone()).collect();
            let rows: Vec<serde_json::Value> =
                list.iter().map(|w| serde_json::json!({"wallet": w.address, "name": w.name})).collect();
            let _ = std::fs::write(path, serde_json::to_string_pretty(&rows).unwrap_or_default());
            // Wallets that left the leaderboard are still followed while we hold their lots.
            set.extend(engine.wallets_held().await);
            let mut w = wallets.write().await;
            let added = set.difference(&w).count();
            *w = set;
            log!("wallets: {} followed ({added} new)", w.len());
        }
        Err(e) => log!("wallets: Polyfox read failed: {e}"),
    }
}

/// Live: the wallets to copy, the best of the paper copies lately.
async fn refresh_top(cfg: &config::Config, url: &str, engine: &engine::Engine, wallets: &RwLock<HashSet<String>>) {
    let top = match db::connect(url).await {
        Ok(c) => db::top_wallets(&c, cfg.top_days, cfg.top_min_closed, cfg.top_n).await,
        Err(e) => Err(e),
    };
    match top {
        Ok(list) if !list.is_empty() => {
            for (w, name, roi, closed) in &list {
                log!("live top: {} {:+.1}% over {closed} closed", name.as_deref().unwrap_or(w), roi * 100.0);
            }
            let mut set: HashSet<String> = list.into_iter().map(|(w, ..)| w).collect();
            // Held lots keep their wallet followed, for its sales.
            set.extend(engine.wallets_held().await);
            let mut w = wallets.write().await;
            *w = set;
            log!("live: {} wallets followed", w.len());
        }
        Ok(_) => log!("live top: no wallet qualifies, list kept"),
        Err(e) => log!("live top: read failed, list kept: {e}"),
    }
}

/// One copier's trade feed (stream and REST read-back over its wallets), its engine and its
/// settlement checks. Returns the tasks whose end must end the run.
fn spawn_copier(name: &'static str, cfg: &config::Config, api: &api::Api, engine: &Arc<engine::Engine>, wallets: &Wallets)
    -> Vec<(String, JoinHandle<()>)> {
    let (tx, rx) = mpsc::channel(10_000);
    let tasks = vec![
        (format!("{name} stream"), tokio::spawn(stream::run_stream(wallets.clone(), tx.clone()))),
        (format!("{name} read-back"), tokio::spawn(stream::run_poller(api.clone(), wallets.clone(), tx, cfg.poll_gap_ms))),
        (format!("{name} engine"), tokio::spawn(engine.clone().run(rx))),
    ];
    let engine = engine.clone();
    let every = cfg.settle_every_s;
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(every)).await;
            engine.settle_once().await;
        }
    });
    tasks
}

fn live_wanted() -> bool {
    let set = |k: &str| std::env::var(k).is_ok_and(|v| !v.trim().is_empty());
    (set("POLYMARKET_PRIVATE_KEY") || set("PRIVATE_KEY"))
        && !std::env::var("LIVE").is_ok_and(|v| ["off", "0", "false"].contains(&v.trim()))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // The order SDK brings a second TLS backend: with both built in, rustls cannot pick one
    // on its own and the trade stream's connect panics.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str).unwrap_or("run");
    let rest = if args.is_empty() { &args[..] } else { &args[1..] };
    if cmd == "check" {
        // The live keys work: the account, its proxy wallet and its cash; no order is sent.
        let l = live::Live::connect().await?;
        println!("signer {}\nproxy wallet {}\ncash ${:.2}", l.signer_address(), l.funder, l.cash().await?);
        return Ok(());
    }
    if cmd == "order" {
        // An order's state as the exchange reports it; read only.
        let l = live::Live::connect().await?;
        let id = rest.first().context("order ID [TOKEN]")?;
        println!("{}", l.order_debug(id).await);
        if let Some(token) = rest.get(1) {
            println!("our buys of it (last day): {:?}", l.public_fill(token, true, api::now() - 86400.0).await?);
        }
        return Ok(());
    }
    if cmd == "test-trade" {
        return test_trade(rest.first().cloned().context("test-trade TOKEN [USD]")?, rest.get(1).cloned()).await;
    }
    let (cfg_args, extra): (Vec<String>, Vec<String>) = {
        // `report` takes --hours; everything else is a Config option.
        let mut c = Vec::new();
        let mut e = Vec::new();
        let mut i = 0;
        while i < rest.len() {
            if rest[i] == "--live" {
                c.push(rest[i].clone());
                i += 1;
                continue;
            }
            let pair = [rest[i].clone(), rest.get(i + 1).cloned().unwrap_or_default()];
            if rest[i] == "--hours" { e.extend(pair) } else { c.extend(pair) }
            i += 2;
        }
        (c, e)
    };
    let cfg = config::Config::from_args(&cfg_args)?;
    match cmd {
        "report" => report::run(&cfg, &extra).await,
        "run" => run(cfg).await,
        other => anyhow::bail!("unknown command {other} (run | report | check | test-trade)"),
    }
}

/// A real round trip: buys `usd` of `token` at market (up to a tick over the best ask), then
/// sells what the account holds of it (down to a tick under the best bid).
async fn test_trade(token: String, usd: Option<String>) -> anyhow::Result<()> {
    let usd: f64 = usd.as_deref().unwrap_or("1").parse()?;
    let (api, l) = (api::Api::new()?, live::Live::connect().await?);
    println!("cash before ${:.4}", l.cash().await?);
    let b = api.book(&token).await?;
    let ask = b.asks.first().context("no asks")?.0;
    let (f, r) = l.buy(&token, usd, fill::on_tick(ask + b.tick, b.tick, true)).await?;
    println!("BUY  {:.4} sh for ${:.4} @ {:.4}  {:?} {}", f.shares, f.usd, f.avg().unwrap_or(0.0), r.status, r.order_id);
    let mut have = 0.0;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(2)).await;
        have = l.shares(&token).await?;
        if have > 0.0 {
            break;
        }
    }
    println!("held {have:.4} sh");
    let b = api.book(&token).await?;
    let bid = b.bids.first().context("no bids")?.0;
    let (f, r) = l.sell(&token, have, fill::on_tick(bid - b.tick, b.tick, false)).await?;
    println!("SELL {:.4} sh for ${:.4} @ {:.4}  {:?} {}", f.shares, f.usd, f.avg().unwrap_or(0.0), r.status, r.order_id);
    tokio::time::sleep(Duration::from_secs(5)).await;
    println!("held after {:.4} sh, cash after ${:.4}", l.shares(&token).await?, l.cash().await?);
    Ok(())
}

async fn run(cfg: config::Config) -> anyhow::Result<()> {
    let api = api::Api::new()?;
    let db_url = std::env::var("DATABASE_URL").ok().filter(|u| !u.is_empty());
    let (db_tx, paper_state, live_state) = match &db_url {
        Some(url) => {
            // The state must come from the database: starting empty would orphan the open lots.
            let client = db::connect(url).await?;
            let (paper, live) = (db::load_state(&client, false).await?, db::load_state(&client, true).await?);
            log!("db: connected, {}", if paper.is_some() { "state restored" } else { "no saved state, starting fresh" });
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(db::writer(url.clone(), Some(client), rx));
            (Some(tx), paper, live)
        }
        None => {
            log!("db: DATABASE_URL not set, files only");
            (None, None, None)
        }
    };

    // Paper: every Polyfox wallet.
    let paper = engine::Engine::open(cfg.clone(), api.clone(), db_tx.clone(), paper_state, None)?;
    let paper_wallets: Wallets = Arc::new(RwLock::new(HashSet::new()));
    let wallets_path = cfg.data_dir.join("wallets.json");
    refresh_wallets(&api, &paper, &paper_wallets, &wallets_path, db_tx.as_ref()).await;
    if paper_wallets.read().await.is_empty() {
        anyhow::bail!("no wallets to follow (Polyfox read failed)");
    }
    let mut tasks = spawn_copier("paper", &cfg, &api, &paper, &paper_wallets);
    {
        let (api, engine, wallets, db_tx) = (api.clone(), paper.clone(), paper_wallets.clone(), db_tx.clone());
        let every = cfg.wallets_refresh_s;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(every)).await;
                refresh_wallets(&api, &engine, &wallets, &wallets_path, db_tx.as_ref()).await;
            }
        });
    }

    // Live, next to it: the top wallets, real orders.
    let mut live_engine = None;
    match (live_wanted(), &db_url) {
        (false, _) => log!("live: off (no PRIVATE_KEY)"),
        (true, None) => log!("live: OFF, needs DATABASE_URL (the paper results pick the wallets)"),
        (true, Some(url)) => match live::Live::connect().await {
            // The paper copy goes on: a bad key must not stop it.
            Err(e) => log!("live: OFF, account connect failed: {e:#}"),
            Ok(l) => {
                let l = Arc::new(l);
                let lcfg = config::Config {
                    live: true,
                    data_dir: cfg.data_dir.join("live"),
                    buy_cap_abs: cfg.live_cap_abs,
                    ..cfg.clone()
                };
                let engine = engine::Engine::open(lcfg.clone(), api.clone(), db_tx.clone(), live_state, Some(l.clone()))?;
                let cash = l.cash().await.unwrap_or(0.0);
                *engine.cash.lock().await = cash;
                log!("live: ON, signer {} proxy wallet {} cash ${cash:.2}, ${} a copy, up to +{} a share over the wallet",
                    l.signer_address(), l.funder, lcfg.stake_usd, lcfg.buy_cap_abs);
                let wallets: Wallets = Arc::new(RwLock::new(HashSet::new()));
                refresh_top(&lcfg, url, &engine, &wallets).await;
                tasks.extend(spawn_copier("live", &lcfg, &api, &engine, &wallets));
                {
                    let (engine, wallets, url) = (engine.clone(), wallets.clone(), url.clone());
                    tokio::spawn(async move {
                        loop {
                            tokio::time::sleep(Duration::from_secs(lcfg.wallets_refresh_s)).await;
                            refresh_top(&lcfg, &url, &engine, &wallets).await;
                        }
                    });
                }
                {
                    // The cash as the exchange sees it (fills, fees, redeems), every 30 s.
                    let engine = engine.clone();
                    tokio::spawn(async move {
                        loop {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            if let Ok(c) = l.cash().await {
                                *engine.cash.lock().await = c;
                            }
                        }
                    });
                }
                live_engine = Some(engine);
            }
        },
    }
    let engines: Vec<Arc<engine::Engine>> = std::iter::once(paper).chain(live_engine).collect();
    let save_all = |to_db: bool| {
        let engines = engines.clone();
        async move {
            for e in &engines {
                e.save(to_db).await;
            }
        }
    };

    stream::LAST_TRADE.store(api::now() as u64, std::sync::atomic::Ordering::Relaxed);
    let mut tick = 0u64;
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(15)) => {}
            _ = shutdown_signal() => {
                log!("stopping: saving state");
                // Copies already under way finish within a few seconds; then the database
                // writer drains.
                tokio::time::sleep(Duration::from_secs(3)).await;
                save_all(true).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
                return Ok(());
            }
        }
        tick += 1;
        save_all(tick % 4 == 0).await;
        if let Some((name, _)) = tasks.iter().find(|(_, h)| h.is_finished()) {
            save_all(true).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
            anyhow::bail!("{name} task stopped");
        }
        let quiet = (api::now() as u64).saturating_sub(stream::LAST_TRADE.load(std::sync::atomic::Ordering::Relaxed));
        if quiet > NO_TRADES_EXIT_S {
            save_all(true).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
            anyhow::bail!("no trades from the followed wallets for {} min", quiet / 60);
        }
    }
}

/// SIGTERM (a redeploy or stop) or Ctrl-C.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}
