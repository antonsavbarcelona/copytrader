//! Paper copy-trading of the Polyfox smart wallets on Polymarket.
//!
//!     copytrader run    [--data DIR] [--stake 1] [--cap 0.02] [--sell-floor 0.05] [--market-gap-h 6]
//!     copytrader report [--data DIR] [--hours 24]
//!
//! `run` follows every wallet on the Polyfox leaderboard (read again every 6 h) and copies
//! their buys and sales on paper against the live order book; see engine.rs for the rules.
//! Events go to DIR/events.jsonl, open lots to DIR/state.json.

mod api;
mod config;
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

async fn refresh_wallets(api: &api::Api, engine: &engine::Engine, wallets: &RwLock<HashSet<String>>, path: &std::path::Path) {
    match api.polyfox_wallets().await {
        Ok(list) => {
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
    let engine = engine::Engine::open(cfg.clone(), api.clone())?;
    let wallets = Arc::new(RwLock::new(HashSet::new()));
    let wallets_path = cfg.data_dir.join("wallets.json");
    refresh_wallets(&api, &engine, &wallets, &wallets_path).await;
    let (tx, rx) = mpsc::channel(10_000);
    tokio::spawn(stream::run_stream(wallets.clone(), tx.clone()));
    tokio::spawn(stream::run_poller(api.clone(), wallets.clone(), tx, cfg.poll_gap_ms));
    tokio::spawn(engine.clone().run(rx));
    {
        let (api, engine, wallets) = (api.clone(), engine.clone(), wallets.clone());
        let every = cfg.wallets_refresh_s;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(every)).await;
                refresh_wallets(&api, &engine, &wallets, &wallets_path).await;
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
    loop {
        tokio::time::sleep(Duration::from_secs(15)).await;
        engine.save().await;
    }
}
