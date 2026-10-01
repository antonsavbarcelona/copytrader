//! The wallets' trades as they happen: Polymarket's real-time data stream (every trade on the
//! exchange), filtered to the followed wallets, plus a REST read-back of each wallet in turn
//! (the stream drops trades now and then without disconnecting).

use std::collections::HashSet;
use std::sync::Arc;
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

/// Runs forever: reconnects after an error or SILENCE_S without a message.
pub async fn run_stream(wallets: Arc<RwLock<HashSet<String>>>, tx: mpsc::Sender<Seen>) {
    loop {
        match tokio_tungstenite::connect_async(RTDS).await {
            Ok((ws, _)) => {
                let (mut sink, mut read) = ws.split();
                let sub = r#"{"action":"subscribe","subscriptions":[{"topic":"activity","type":"trades"}]}"#;
                if sink.send(Message::Text(sub.into())).await.is_err() {
                    continue;
                }
                log!("stream: connected");
                let mut ping = tokio::time::interval(Duration::from_secs(10));
                loop {
                    tokio::select! {
                        _ = ping.tick() => {
                            if sink.send(Message::Ping(Vec::new())).await.is_err() { break; }
                        }
                        msg = tokio::time::timeout(Duration::from_secs(SILENCE_S), read.next()) => {
                            let text = match msg {
                                Err(_) => { log!("stream: silent for {SILENCE_S} s, reconnecting"); break; }
                                Ok(None) => { log!("stream: closed, reconnecting"); break; }
                                Ok(Some(Err(e))) => { log!("stream error: {e}, reconnecting"); break; }
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
                                let _ = tx.send(Seen { trade, recv: now(), source: Source::Stream }).await;
                            }
                        }
                    }
                }
            }
            Err(e) => log!("stream: connect failed: {e}"),
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
