//! The paper copier: every buy and sale of the followed wallets, copied the way the live
//! executor would, priced against the real order book at the moment our order would land.
//!
//! Rules (where the live $1 test of 2026-10-01 ended up):
//!   buy    $stake at market (FAK): the asks up to the cap (2% over the wallet's price, at
//!          least one tick); what the book lacks is missed. If under half filled, the book is
//!          read again a second later and, if back under the cap, the rest is taken (the live
//!          executor's single retry). A miss is also priced uncapped ("shadow"), to see what
//!          the cap saved or cost.
//!   once   one buy per wallet and market in `market_gap_s` (pieces of one order within
//!          `join_s` are one trade; wallets looping both sides of a market take one copy).
//!   late   a buy first seen over REST more than `rest_max_age_s` after the trade is a miss.
//!   sell   when the wallet sells an outcome we hold for it: the share it sold (from its
//!          position after the sale), then once the positions API has caught up
//!          (`recheck_s`) ours is set to the share of its peak it kept; out or under half
//!          kept and the rest under $1: all of it. Bids down to `sell_floor_pct` under its
//!          price. A sale under $1 that is not most of the position is held (exchange minimum).
//!   settle held outcomes are paid out at the final price once the market settles.
//! Fees: the market's taker fee per share, rate * (p(1-p))^exponent, on every fill.

use std::collections::{HashMap, HashSet, VecDeque};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::sync::{Mutex, Semaphore, mpsc};

use crate::db;

use crate::api::{Api, Market, Side, Trade, now};
use crate::config::Config;
use crate::fill::{self, Fill};
use crate::log;
use crate::stream::{Seen, Source};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Lot {
    pub wallet: String,
    pub token: String,
    pub condition: String,
    pub title: String,
    pub shares: f64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct State {
    /// "wallet|token" -> what we hold for that wallet.
    pub lots: HashMap<String, Lot>,
    /// "wallet|condition" -> when a buy was last copied there.
    pub market_copied: HashMap<String, f64>,
    /// "wallet|token" -> the most the wallet was seen holding.
    pub their_peak: HashMap<String, f64>,
    /// "wallet|token" -> the most we held for it.
    pub our_peak: HashMap<String, f64>,
}

fn key(a: &str, b: &str) -> String {
    format!("{a}|{b}")
}

pub struct Engine {
    pub cfg: Config,
    pub api: Api,
    pub state: Mutex<State>,
    events: Mutex<std::fs::File>,
    state_path: PathBuf,
    seen: Mutex<(HashSet<String>, VecDeque<String>)>,
    pending: Mutex<HashMap<(String, String, bool), f64>>,
    rechecks: Mutex<HashSet<String>>,
    limit: Semaphore,
    db: Option<mpsc::UnboundedSender<db::Write>>,
    /// Trades from before this run started (the first read-back reaches 10 minutes back) are
    /// not copies we could have made, nor misses.
    started: f64,
}

fn fill_json(f: &Fill) -> serde_json::Value {
    json!({"shares": round(f.shares, 6), "usd": round(f.usd, 6), "fee": round(f.fee, 6), "avg": f.avg().map(|p| round(p, 6))})
}

fn round(x: f64, d: i32) -> f64 {
    let m = 10f64.powi(d);
    (x * m).round() / m
}

impl Engine {
    /// `db_state`: the snapshot saved in the database, which wins over the local file (the
    /// local disk does not survive a redeploy).
    pub fn open(cfg: Config, api: Api, db: Option<mpsc::UnboundedSender<db::Write>>, db_state: Option<String>)
        -> anyhow::Result<Arc<Self>> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        let state_path = cfg.data_dir.join("state.json");
        let state: State = db_state
            .or_else(|| std::fs::read_to_string(&state_path).ok())
            .and_then(|s| serde_json::from_str(&s).ok())
            .unwrap_or_default();
        log!("state: {} lots held", state.lots.values().filter(|l| l.shares > 1e-6).count());
        let events = std::fs::OpenOptions::new().create(true).append(true).open(cfg.data_dir.join("events.jsonl"))?;
        Ok(Arc::new(Self {
            cfg,
            api,
            state: Mutex::new(state),
            events: Mutex::new(events),
            state_path,
            seen: Mutex::new((HashSet::new(), VecDeque::new())),
            pending: Mutex::new(HashMap::new()),
            rechecks: Mutex::new(HashSet::new()),
            limit: Semaphore::new(16),
            db,
            started: now(),
        }))
    }

    async fn write(&self, mut ev: serde_json::Value) {
        ev["at"] = json!(round(now(), 3));
        let mut f = self.events.lock().await;
        let _ = writeln!(f, "{ev}");
        let _ = f.flush();
        if let Some(db) = &self.db {
            let _ = db.send(db::Write::Event(ev));
        }
    }

    /// Saves the state to the local file and, with `to_db`, to the database. Drops what is no
    /// longer needed first, so a week's run does not grow without bound.
    pub async fn save(&self, to_db: bool) {
        let t = now();
        self.pending.lock().await.retain(|_, ts| t - *ts < 3600.0);
        let json = {
            let mut st = self.state.lock().await;
            st.lots.retain(|_, l| l.shares > 1e-6);
            let State { lots, market_copied, their_peak, our_peak } = &mut *st;
            market_copied.retain(|_, at| t - *at <= self.cfg.market_gap_s + 3600.0);
            their_peak.retain(|k, _| lots.contains_key(k));
            our_peak.retain(|k, _| lots.contains_key(k));
            serde_json::to_string(&*st)
        };
        let Ok(s) = json else { return };
        let tmp = self.state_path.with_extension("tmp");
        if std::fs::write(&tmp, &s).is_ok() {
            let _ = std::fs::rename(&tmp, &self.state_path);
        }
        if to_db {
            if let Some(db) = &self.db {
                let _ = db.send(db::Write::State(s));
            }
        }
    }

    /// The wallets whose lots are still open (followed even if they leave the leaderboard).
    pub async fn wallets_held(&self) -> HashSet<String> {
        self.state.lock().await.lots.values().filter(|l| l.shares > 1e-6).map(|l| l.wallet.clone()).collect()
    }

    /// Consumes the trade feed; each copy runs as its own task.
    pub async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<Seen>) {
        while let Some(seen) = rx.recv().await {
            self.clone().handle(seen).await;
        }
    }

    async fn handle(self: Arc<Self>, seen: Seen) {
        let t = &seen.trade;
        if t.token.is_empty() || !(t.price > 0.0 && t.price < 1.0) {
            return;
        }
        if t.ts < self.started - 5.0 {
            return;
        }
        let buy = t.side == Side::Buy;
        let tkey = format!("{}:{}:{}:{:.6}", t.tx, t.token, buy, t.size);
        {
            let mut s = self.seen.lock().await;
            if !s.0.insert(tkey.clone()) {
                return;
            }
            s.1.push_back(tkey);
            // New to us yet found over REST well after the trade: the stream dropped it.
            if seen.source == Source::Rest && seen.recv - t.ts > 20.0 {
                crate::stream::STREAM_MISSES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            if s.1.len() > 200_000 {
                if let Some(old) = s.1.pop_front() {
                    s.0.remove(&old);
                }
            }
        }
        let pkey = (t.wallet.clone(), t.token.clone(), buy);
        // Pieces of one order already being copied.
        if let Some(first) = self.pending.lock().await.get(&pkey) {
            let d = t.ts - first;
            if (0.0..=self.cfg.join_s).contains(&d) {
                return;
            }
        }
        if buy {
            let mkey = key(&t.wallet, if t.condition.is_empty() { &t.token } else { &t.condition });
            {
                let mut st = self.state.lock().await;
                if let Some(at) = st.market_copied.get(&mkey) {
                    if t.ts - at <= self.cfg.market_gap_s && t.ts >= *at - 60.0 {
                        return;
                    }
                }
                st.market_copied.insert(mkey, t.ts);
            }
            self.pending.lock().await.insert(pkey, t.ts);
            if seen.source == Source::Rest && seen.recv - t.ts > self.cfg.rest_max_age_s {
                // Our stream missed it: the live executor would have been too late as well.
                self.write(json!({"kind": "buy", "wallet": t.wallet, "token": t.token, "condition": t.condition,
                    "title": t.title, "wallet_price": t.price, "wallet_size": t.size, "trade_ts": t.ts,
                    "recv": seen.recv, "source": "rest", "missed": "late"})).await;
                return;
            }
            tokio::spawn(async move {
                let me = self.clone();
                let _p = self.limit.acquire().await;
                me.copy_buy(seen).await;
            });
        } else {
            let held = self.state.lock().await.lots.get(&key(&t.wallet, &t.token)).map(|l| l.shares).unwrap_or(0.0);
            if held <= 1e-6 {
                return;
            }
            self.pending.lock().await.insert(pkey, t.ts);
            tokio::spawn(async move { self.follow_sale(seen.trade).await });
        }
    }

    fn phase(m: Option<&Market>, ts: f64) -> &'static str {
        match m.and_then(|m| m.game_start) {
            Some(g) if ts < g => "pre-match",
            Some(_) => "in-play",
            None => "no start time",
        }
    }

    async fn copy_buy(self: Arc<Self>, seen: Seen) {
        let t = seen.trade;
        let stake = self.cfg.stake_usd;
        let market = self.api.market(&t.token, 3600.0).await.ok();
        let fee = |p: f64| market.as_ref().map(|m| m.fee_per_share(p)).unwrap_or(0.0);
        let book = match self.api.book(&t.token).await {
            Ok(b) => b,
            Err(e) => {
                self.write(json!({"kind": "buy", "wallet": t.wallet, "token": t.token, "title": t.title,
                    "wallet_price": t.price, "trade_ts": t.ts, "recv": seen.recv, "missed": "no book", "error": e.to_string()})).await;
                return;
            }
        };
        let book_at = now();
        let cap = fill::buy_cap(t.price, book.tick, self.cfg.buy_cap_pct, self.cfg.buy_cap_min_ticks);
        let mut f = fill::buy(&book.asks, stake, cap, fee);
        let mut retried = false;
        if f.usd < stake * 0.5 {
            // The live executor's single retry once the FAK came back short.
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Ok(b2) = self.api.book(&t.token).await {
                if b2.asks.first().is_some_and(|a| a.0 <= cap) {
                    retried = true;
                    let more = fill::buy(&b2.asks, stake - f.usd, cap, fee);
                    f.shares += more.shares;
                    f.usd += more.usd;
                    f.fee += more.fee;
                }
            }
        }
        let filled = f.usd >= 0.01;
        let shadow = (f.usd < stake * 0.5).then(|| fill::buy(&book.asks, stake, 1.0, fee));
        let k = key(&t.wallet, &t.token);
        if filled {
            let mut st = self.state.lock().await;
            let lot = st.lots.entry(k.clone()).or_insert_with(|| Lot {
                wallet: t.wallet.clone(),
                token: t.token.clone(),
                condition: t.condition.clone(),
                title: t.title.clone(),
                shares: 0.0,
            });
            lot.shares += f.shares;
            let held = lot.shares;
            let peak = st.our_peak.entry(k.clone()).or_insert(0.0);
            *peak = peak.max(held);
        }
        let vs = f.avg().map(|p| round((p / t.price - 1.0) * 1e4, 1));
        log!("{} BUY {:.3} -> {} {}", &t.wallet[..10], t.price,
            if filled { format!("filled ${:.2} @ {:.3} ({:+.0} bps)", f.usd, f.avg().unwrap_or(0.0), vs.unwrap_or(0.0)) }
            else { format!("missed (cap {cap})") },
            t.title.chars().take(50).collect::<String>());
        self.write(json!({
            "kind": "buy", "wallet": t.wallet, "token": t.token, "condition": t.condition, "title": t.title,
            "wallet_price": t.price, "wallet_size": t.size, "trade_ts": t.ts, "recv": seen.recv,
            "source": if seen.source == Source::Rest { "rest" } else { "stream" },
            "detect_s": round(seen.recv - t.ts, 3), "book_s": round(book_at - t.ts, 3),
            "phase": Self::phase(market.as_ref(), t.ts), "game_start": market.as_ref().and_then(|m| m.game_start),
            "tick": book.tick, "cap": cap, "best_ask": book.asks.first().map(|a| a.0),
            "fill": fill_json(&f), "vs_wallet_bps": vs, "retried": retried,
            "missed": if filled { serde_json::Value::Null } else { json!("cap") },
            "shadow": shadow.map(|s| fill_json(&s)),
        })).await;
        if filled {
            // The wallet's position once the API has caught up: the reference for its sales.
            let me = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs_f64(me.cfg.recheck_s)).await;
                if let Ok(p) = me.api.position(&t.wallet, &t.token).await {
                    let mut st = me.state.lock().await;
                    let e = st.their_peak.entry(k).or_insert(0.0);
                    *e = e.max(p);
                }
            });
        }
    }

    async fn follow_sale(self: Arc<Self>, t: Trade) {
        tokio::time::sleep(Duration::from_secs(3)).await;
        let after = self.api.position(&t.wallet, &t.token).await.ok();
        let frac = match after {
            Some(a) if a >= 0.01 => t.size / (t.size + a),
            _ => 1.0,
        };
        self.sell(&t, frac, "follow", json!({"their_after": after})).await;
        let k = key(&t.wallet, &t.token);
        let held = self.state.lock().await.lots.get(&k).map(|l| l.shares).unwrap_or(0.0);
        if held > 0.01 && self.rechecks.lock().await.insert(k.clone()) {
            let me = self.clone();
            tokio::spawn(async move {
                me.recheck(&t).await;
                me.rechecks.lock().await.remove(&k);
            });
        }
    }

    /// Once the positions API has caught up: ours kept in the share of its peak it kept.
    async fn recheck(&self, t: &Trade) {
        tokio::time::sleep(Duration::from_secs_f64(self.cfg.recheck_s)).await;
        let Ok(after) = self.api.position(&t.wallet, &t.token).await else { return };
        let k = key(&t.wallet, &t.token);
        let (held, ours, peak) = {
            let mut st = self.state.lock().await;
            let held = st.lots.get(&k).map(|l| l.shares).unwrap_or(0.0);
            let peak = st.their_peak.get(&k).copied().unwrap_or(0.0).max(after + t.size);
            st.their_peak.insert(k.clone(), peak);
            let ours = st.our_peak.get(&k).copied().unwrap_or(0.0).max(held);
            (held, ours, peak)
        };
        if held <= 0.01 {
            return;
        }
        let kept = if after < 0.01 || peak <= 0.0 { 0.0 } else { (after / peak).min(1.0) };
        let excess = held - ours * kept;
        if excess <= 0.01 {
            return;
        }
        let frac = if kept < 0.5 && excess * t.price < 1.0 { 1.0 } else { excess / held };
        self.sell(t, frac, "recheck", json!({"their_after": after, "their_peak": peak, "kept": round(kept, 4)})).await;
    }

    async fn sell(&self, t: &Trade, frac: f64, why: &str, info: serde_json::Value) {
        let k = key(&t.wallet, &t.token);
        let held = self.state.lock().await.lots.get(&k).map(|l| l.shares).unwrap_or(0.0);
        if held <= 1e-6 {
            return;
        }
        let mut qty = held * frac.min(1.0);
        // The exchange refuses sales under $1: all of it once the wallet sold most, else hold.
        if qty * t.price < 1.0 {
            qty = if frac >= 0.5 { held } else { 0.0 };
        }
        let base = json!({"kind": "sell", "why": why, "wallet": t.wallet, "token": t.token, "title": t.title,
            "wallet_price": t.price, "wallet_size": t.size, "trade_ts": t.ts, "frac": round(frac, 4),
            "held_before": round(held, 6), "info": info});
        if qty <= 1e-6 {
            let mut ev = base;
            ev["fill"] = fill_json(&Fill::default());
            ev["held"] = json!("kept: our share of the sale is under $1");
            self.write(ev).await;
            return;
        }
        let market = self.api.market(&t.token, 3600.0).await.ok();
        let fee = |p: f64| market.as_ref().map(|m| m.fee_per_share(p)).unwrap_or(0.0);
        let book = match self.api.book(&t.token).await {
            Ok(b) => b,
            Err(e) => {
                let mut ev = base;
                ev["error"] = json!(e.to_string());
                self.write(ev).await;
                return;
            }
        };
        let floor = fill::sell_floor(t.price, book.tick, self.cfg.sell_floor_pct);
        let f = fill::sell(&book.bids, qty, floor, fee);
        let after = {
            let mut st = self.state.lock().await;
            let lot = st.lots.get_mut(&k);
            match lot {
                Some(l) => {
                    l.shares = (l.shares - f.shares).max(0.0);
                    l.shares
                }
                None => 0.0,
            }
        };
        log!("{} SELL ({why}) {:.2} of {:.2} sh -> ${:.2} @ {:.3}", &t.wallet[..10], f.shares, held, f.usd,
            f.avg().unwrap_or(0.0));
        let mut ev = base;
        ev["floor"] = json!(floor);
        ev["fill"] = fill_json(&f);
        ev["held_after"] = json!(round(after, 6));
        self.write(ev).await;
    }

    /// Pays out held outcomes whose market has settled.
    pub async fn settle_once(&self) {
        let lots: Vec<Lot> = self.state.lock().await.lots.values().filter(|l| l.shares > 1e-6).cloned().collect();
        let mut tokens: Vec<String> = lots.iter().map(|l| l.token.clone()).collect();
        tokens.sort();
        tokens.dedup();
        // Only closed markets come back: one request per 40 held outcomes.
        let mut by_token: HashMap<String, Market> = HashMap::new();
        for chunk in tokens.chunks(40) {
            match self.api.closed_markets(chunk).await {
                Ok(ms) => {
                    for m in ms {
                        for t in &m.tokens {
                            by_token.insert(t.clone(), m.clone());
                        }
                    }
                }
                Err(e) => log!("settle: market read failed: {e}"),
            }
        }
        for lot in lots {
            let Some(m) = by_token.get(&lot.token) else { continue };
            if !m.settled() {
                continue;
            }
            let price = m.price_of(&lot.token).unwrap_or(0.0);
            let k = key(&lot.wallet, &lot.token);
            let shares = {
                let mut st = self.state.lock().await;
                st.lots.remove(&k).map(|l| l.shares).unwrap_or(0.0)
            };
            if shares > 1e-6 {
                log!("{} settled at {price}: {:.2} sh -> ${:.2} {}", &lot.wallet[..10], shares, shares * price,
                    lot.title.chars().take(50).collect::<String>());
                self.write(json!({"kind": "settle", "wallet": lot.wallet, "token": lot.token, "title": lot.title,
                    "price": price, "shares": round(shares, 6), "usd": round(shares * price, 6)})).await;
            }
        }
    }
}
