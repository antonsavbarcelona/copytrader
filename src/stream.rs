//! The wallets' trades as they happen: Polymarket's real-time data stream (every trade on the
//! exchange), filtered to the followed wallets, plus a REST read-back of each wallet in turn
//! (the stream drops trades now and then without disconnecting).
//!
//! The stream is held open STREAMS times at once: each connection drops different trades
//! (seen 2026-10-02: a local and a server copy each lost ~10%, rarely the same ones), and the
//! engine keeps the first copy of each trade.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::sync::{RwLock, mpsc};
use tokio_tungstenite::tungstenite::Message;

use crate::api::{Api, Side, Trade, now};
use crate::log;

const RTDS: &str = "wss://ws-live-data.polymarket.com";
/// The stream sends many trades a second: this long without one means it has stalled.
const SILENCE_S: u64 = 15;
/// The stream can also go on delivering other trades while dropping our wallets' ones (seen
/// 2026-10-01 for ~40 min). Trades the REST read-back finds first are counted here; this many
/// within one check makes the connection that brought the fewest reconnect.
pub static STREAM_MISSES: AtomicU64 = AtomicU64::new(0);
const MISSES_TO_RECONNECT: u64 = 3;
/// Connections to the stream held open at once.
const STREAMS: usize = 3;
/// A connection that brought under half of what the best one did in a check (of at least this
/// many of our wallets' trades) is dropping them: it reconnects.
const LAGGING_MIN: u64 = 4;
/// Unix seconds a followed wallet's trade last came in, by either path (the run's watchdog).
pub static LAST_TRADE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Source {
    Stream,
    Rest,
}

#[derive(Debug)]
pub struct Seen {
    pub trade: Trade,
    /// When we learned of it (unix seconds).
    pub recv: f64,
    pub source: Source,
}

fn s(v: &Value) -> String {
    v.as_str().map(str::to_string).unwrap_or_default()
}

fn num(v: &Value) -> f64 {
    match v {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(x) => x.parse().unwrap_or(0.0),
        _ => 0.0,
    }
}

fn parse(p: &Value) -> Option<Trade> {
    Some(Trade {
        wallet: s(&p["proxyWallet"]).to_lowercase(),
        side: match s(&p["side"]).to_uppercase().as_str() {
            "BUY" => Side::Buy,
            "SELL" => Side::Sell,
            _ => return None,
        },
        token: s(&p["asset"]),
        condition: s(&p["conditionId"]),
        size: num(&p["size"]),
        price: num(&p["price"]),
        ts: num(&p["timestamp"]),
        title: s(&p["title"]),
        tx: s(&p["transactionHash"]),
    })
}

struct Conn {
    /// Our wallets' trades this connection brought (duplicates of other connections included).
    brought: AtomicU64,
    /// Set by the check: this connection is to reconnect.
    kick: AtomicBool,
}

/// Runs forever: STREAMS connections, each reconnecting after an error, SILENCE_S without a
/// message, or being told to (dropping our wallets' trades while the others bring them).
pub async fn run_stream(wallets: Arc<RwLock<HashSet<String>>>, tx: mpsc::Sender<Seen>) {
    let conns: Arc<Vec<Conn>> =
        Arc::new((0..STREAMS).map(|_| Conn { brought: AtomicU64::new(0), kick: AtomicBool::new(false) }).collect());
    let mut tasks = Vec::new();
    for i in 0..STREAMS {
        tasks.push(tokio::spawn(run_one(i, conns.clone(), wallets.clone(), tx.clone())));
        // Not all at once, so they are less likely to land on the same server.
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
    let mut check = tokio::time::interval(Duration::from_secs(30));
    check.tick().await;
    let mut last: Vec<u64> = vec![0; STREAMS];
    let mut misses = STREAM_MISSES.load(Ordering::Relaxed);
    let mut checks = 0u64;
    let mut total: Vec<u64> = vec![0; STREAMS];
    loop {
        check.tick().await;
        if let Some(i) = tasks.iter().position(|t| t.is_finished()) {
            log!("stream {i}: task ended");
            return;
        }
        let now_b: Vec<u64> = conns.iter().map(|c| c.brought.load(Ordering::Relaxed)).collect();
        let d: Vec<u64> = now_b.iter().zip(&last).map(|(n, l)| n - l).collect();
        last = now_b;
        let best = d.iter().copied().max().unwrap_or(0);
        for (i, di) in d.iter().enumerate() {
            total[i] += di;
            if best >= LAGGING_MIN && di * 2 < best {
                log!("stream {i}: brought {di} of our wallets' trades vs {best} on another, reconnecting");
                conns[i].kick.store(true, Ordering::Relaxed);
            }
        }
        let now_m = STREAM_MISSES.load(Ordering::Relaxed);
        if now_m - misses >= MISSES_TO_RECONNECT {
            // Every connection missed these: renew the one that brought the fewest.
            let worst = (0..STREAMS).min_by_key(|&i| (d[i], i)).unwrap_or(0);
            log!("stream: {} of our wallets' trades came only over REST, reconnecting stream {worst}", now_m - misses);
            conns[worst].kick.store(true, Ordering::Relaxed);
        }
        misses = now_m;
        checks += 1;
        if checks % 20 == 0 {
            log!("stream: our wallets' trades brought in the last 10 min by connection: {:?}", total);
            total = vec![0; STREAMS];
        }
    }
}

async fn run_one(i: usize, conns: Arc<Vec<Conn>>, wallets: Arc<RwLock<HashSet<String>>>, tx: mpsc::Sender<Seen>) {
    let me = &conns[i];
    loop {
        match tokio_tungstenite::connect_async(RTDS).await {
            Ok((ws, _)) => {
                let (mut sink, mut read) = ws.split();
                let sub = r#"{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"}]}"#;
                if sink.send(Message::Text(sub.into())).await.is_err() {
                    continue;
                }
                me.kick.store(false, Ordering::Relaxed);
                log!("stream {i}: connected");
                let mut ping = tokio::time::interval(Duration::from_secs(10));
                let mut kick = tokio::time::interval(Duration::from_secs(5));
                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if sink.send(Message::Ping(Vec::new())).await.is_err() { break; }
                        }
                        _ = kick.tick() => {
                            if me.kick.load(Ordering::Relaxed) { break; }
                        }
                        msg = tokio::time::timeout(Duration::from_secs(SILENCE_S), read.next()) => {
                            let text = match msg {
                                Err(_) => { log!("stream {i}: silent for {SILENCE_S} s, reconnecting"); break; }
                                Ok(None) => { log!("stream {i}: closed, reconnecting"); break; }
                                Ok(Some(Err(e))) => { log!("stream {i} error: {e}, reconnecting"); break; }
                                Ok(Some(Ok(Message::Text(t)))) => t,
                                Ok(Some(Ok(_))) => continue,
                            };
                            let Ok(v) = serde_json::from_str::<Value>(&text) else { continue };
                            let p = &v["payload"];
                            let w = s(&p["proxyWallet"]).to_lowercase();
                            if w.is_empty() || !wallets.read().await.contains(&w) {
                                continue;
                            }
                            if let Some(trade) = parse(p) {
                                me.brought.fetch_add(1, Ordering::Relaxed);
                                LAST_TRADE.store(now() as u64, Ordering::Relaxed);
                                let _ = tx.send(Seen { trade, recv: now(), source: Source::Stream }).await;
                            }
                        }
                    }
                }
            }
            Err(e) => log!("stream {i}: connect failed: {e}"),
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

/// Reads each wallet's last 10 minutes of trades in turn, `gap_ms` apart. Trades the stream
/// already brought are dropped downstream (same transaction, token, side, size).
pub async fn run_poller(api: Api, wallets: Arc<RwLock<HashSet<String>>>, tx: mpsc::Sender<Seen>, gap_ms: u64) {
    loop {
        let list: Vec<String> = wallets.read().await.iter().cloned().collect();
        if list.is_empty() {
            tokio::time::sleep(Duration::from_secs(5)).await;
            continue;
        }
        for w in list {
            match api.trades_since(&w, now() - 600.0).await {
                Ok(trades) => {
                    let recv = now();
                    if !trades.is_empty() {
                        LAST_TRADE.store(recv as u64, Ordering::Relaxed);
                    }
                    for trade in trades {
                        let _ = tx.send(Seen { trade, recv, source: Source::Rest }).await;
                    }
                }
                Err(e) => log!("read-back {}: {e}", &w[..10.min(w.len())]),
            }
            tokio::time::sleep(Duration::from_millis(gap_ms)).await;
        }
    }
}
