//! Paper copy-trading of the Polyfox smart wallets on Polymarket.
//!
//!     copytrader run    [--data DIR] [--stake 1] [--cap 0.02] [--sell-floor 0.05] [--market-gap-h 6]
//!     copytrader report [--data DIR] [--hours 24]
//!
//! `run` follows every wallet on the Polyfox leaderboard (read again every 6 h) and copies
//! their buys and sales on paper against the live order book; see engine.rs for the rules.
//! Events go to DIR/events.jsonl, open lots to DIR/state.json, and with DATABASE_URL set
//! also to Postgres (see db.rs), from which a restart takes the state back.

mod api;
mod config;
mod db;
mod engine;
mod fill;
mod report;
mod stream;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{RwLock, mpsc};

#[macro_export]
macro_rules! log {
    ($($a:tt)*) => {{
        let t = $crate::api::now() as u64 % 86400;
        println!("{:02}:{:02}:{:02} {}", t / 3600, t / 60 % 60, t % 60, format!($($a)*));
    }};
}

/// The run exits (and the host restarts it) when no followed wallet has traded this long.
const NO_TRADES_EXIT_S: u64 = 30 * 60;

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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let cmd = args.first().map(String::as_str).unwrap_or("run");
    let rest = if args.is_empty() { &args[..] } else { &args[1..] };
    let (cfg_args, extra): (Vec<String>, Vec<String>) = {
        // `report` takes --hours; everything else is a Config option.
        let mut c = Vec::new();
        let mut e = Vec::new();
        let mut i = 0;
        while i < rest.len() {
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
        other => anyhow::bail!("unknown command {other} (run | report)"),
    }
}

async fn run(cfg: config::Config) -> anyhow::Result<()> {
    let api = api::Api::new()?;
    let (db_tx, db_state) = match std::env::var("DATABASE_URL").ok().filter(|u| !u.is_empty()) {
        Some(url) => {
            // The state must come from the database: starting empty would orphan the open lots.
            let client = db::connect(&url).await?;
            let st = db::load_state(&client).await?;
            log!("db: connected, {}", if st.is_some() { "state restored" } else { "no saved state, starting fresh" });
            let (tx, rx) = mpsc::unbounded_channel();
            tokio::spawn(db::writer(url, Some(client), rx));
            (Some(tx), st)
        }
        None => {
            log!("db: DATABASE_URL not set, files only");
            (None, None)
        }
    };
    let engine = engine::Engine::open(cfg.clone(), api.clone(), db_tx.clone(), db_state)?;
    let wallets = Arc::new(RwLock::new(HashSet::new()));
    let wallets_path = cfg.data_dir.join("wallets.json");
    refresh_wallets(&api, &engine, &wallets, &wallets_path, db_tx.as_ref()).await;
    if wallets.read().await.is_empty() {
        anyhow::bail!("no wallets to follow (Polyfox read failed)");
    }
    let (tx, rx) = mpsc::channel(10_000);
    let tasks = [
        ("stream", tokio::spawn(stream::run_stream(wallets.clone(), tx.clone()))),
        ("read-back", tokio::spawn(stream::run_poller(api.clone(), wallets.clone(), tx, cfg.poll_gap_ms))),
        ("engine", tokio::spawn(engine.clone().run(rx))),
    ];
    {
        let (api, engine, wallets) = (api.clone(), engine.clone(), wallets.clone());
        let every = cfg.wallets_refresh_s;
        let db_tx = db_tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(every)).await;
                refresh_wallets(&api, &engine, &wallets, &wallets_path, db_tx.as_ref()).await;
            }
        });
    }
    {
        let engine = engine.clone();
        let every = cfg.settle_every_s;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(every)).await;
                engine.settle_once().await;
            }
        });
    }
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
                engine.save(true).await;
                tokio::time::sleep(Duration::from_secs(3)).await;
                return Ok(());
            }
        }
        tick += 1;
        engine.save(tick % 4 == 0).await;
        if let Some((name, _)) = tasks.iter().find(|(_, h)| h.is_finished()) {
            engine.save(true).await;
            tokio::time::sleep(Duration::from_secs(3)).await;
            anyhow::bail!("{name} task stopped");
        }
        let quiet = (api::now() as u64).saturating_sub(stream::LAST_TRADE.load(std::sync::atomic::Ordering::Relaxed));
        if quiet > NO_TRADES_EXIT_S {
            engine.save(true).await;
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
