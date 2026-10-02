//! What the paper copies made: every filled buy is valued at what its shares became — sold
//! (proceeds after fees, shared pro rata among the buys of that wallet and outcome), paid out
//! at settlement, or still held at the market's current price.

use std::collections::{BTreeMap, HashMap};
use std::io::Write;

use anyhow::Result;
use serde_json::Value;

use crate::api::{Api, now};
use crate::config::Config;

#[derive(Default)]
struct Key {
    bought: f64,
    sold: f64,
    proceeds: f64,
    settled_shares: f64,
    settled_usd: f64,
}

#[derive(Default, Clone, Copy)]
struct Acc {
    n: usize,
    cost: f64,
    value: f64,
}

impl Acc {
    fn add(&mut self, cost: f64, value: f64) {
        self.n += 1;
        self.cost += cost;
        self.value += value;
    }
    fn line(&self, label: &str) -> String {
        let pnl = self.value - self.cost;
        let pct = if self.cost > 0.0 { pnl / self.cost * 100.0 } else { 0.0 };
        format!("  {label:28} {:5} buys  ${:8.2} -> ${:8.2}  {:+8.2} ({:+5.1}%)", self.n, self.cost, self.value, pnl, pct)
    }
}

fn f(v: &Value) -> f64 {
    v.as_f64().unwrap_or(0.0)
}

fn pctl(v: &mut [f64], q: f64) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() - 1) as f64 * q).round() as usize]
}

pub async fn run(cfg: &Config, args: &[String]) -> Result<()> {
    let mut since_h: Option<f64> = None;
    let mut i = 0;
    while i < args.len() {
        if args[i] == "--hours" {
            since_h = args.get(i + 1).and_then(|v| v.parse().ok());
            i += 1;
        }
        i += 1;
    }
    let since = since_h.map(|h| now() - h * 3600.0).unwrap_or(0.0);
    // DATABASE_URL set: the server's events from Postgres; else the local file.
    let events: Vec<Value> = match std::env::var("DATABASE_URL").ok().filter(|u| !u.is_empty()) {
        Some(url) => crate::db::load_events(&crate::db::connect(&url).await?).await?,
        None => {
            let text = std::fs::read_to_string(cfg.data_dir.join("events.jsonl")).unwrap_or_default();
            text.lines().filter_map(|l| serde_json::from_str(l).ok()).collect()
        }
    };
    let buys: Vec<&Value> = events.iter().filter(|e| e["kind"] == "buy" && f(&e["trade_ts"]) >= since).collect();
    let mut keys: HashMap<String, Key> = HashMap::new();
    for e in &events {
        let k = format!("{}|{}", e["wallet"].as_str().unwrap_or(""), e["token"].as_str().unwrap_or(""));
        let fill = &e["fill"];
        match e["kind"].as_str() {
            Some("buy") => keys.entry(k).or_default().bought += f(&fill["shares"]),
            Some("sell") => {
                let x = keys.entry(k).or_default();
                x.sold += f(&fill["shares"]);
                x.proceeds += f(&fill["usd"]) - f(&fill["fee"]);
            }
            Some("settle") => {
                let x = keys.entry(k).or_default();
                x.settled_shares += f(&e["shares"]);
                x.settled_usd += f(&e["usd"]);
            }
            _ => {}
        }
    }
    // Current prices for what is still held and for the missed buys' shadow copies.
    let api = Api::new()?;
    let mut price: HashMap<String, f64> = HashMap::new();
    let mut need: Vec<String> = Vec::new();
    for (k, x) in &keys {
        if x.bought - x.sold - x.settled_shares > 1e-6 {
            need.push(k.split('|').nth(1).unwrap_or("").to_string());
        }
    }
    for e in &buys {
        if !e["shadow"].is_null() {
            need.push(e["token"].as_str().unwrap_or("").to_string());
        }
    }
    need.sort();
    need.dedup();
    for t in need {
        if let Ok(m) = api.market(&t, 600.0).await {
            if let Some(p) = m.price_of(&t) {
                price.insert(t, p);
            }
        }
    }
    let key_value = |k: &str, x: &Key| -> f64 {
        let rest = (x.bought - x.sold - x.settled_shares).max(0.0);
        let tok = k.split('|').nth(1).unwrap_or("");
        x.proceeds + x.settled_usd + rest * price.get(tok).copied().unwrap_or(0.0)
    };

    let mut all = Acc::default();
    let mut by_phase: BTreeMap<String, Acc> = BTreeMap::new();
    let mut by_slip: BTreeMap<&str, Acc> = BTreeMap::new();
    let mut by_wallet: HashMap<String, Acc> = HashMap::new();
    let mut by_source: BTreeMap<String, Acc> = BTreeMap::new();
    let mut shadow = Acc::default();
    let (mut missed_cap, mut missed_late, mut missed_other, mut filled) = (0, 0, 0, 0);
    let mut detect: Vec<f64> = Vec::new();
    let mut book_lag: Vec<f64> = Vec::new();
    let mut slip: Vec<f64> = Vec::new();
    for e in &buys {
        match e["missed"].as_str() {
            Some("cap") => missed_cap += 1,
            Some("late") => missed_late += 1,
            Some(_) => missed_other += 1,
            None => filled += 1,
        }
        if let Some(d) = e["detect_s"].as_f64() {
            detect.push(d);
        }
        if let Some(d) = e["book_s"].as_f64() {
            book_lag.push(d);
        }
        let tok = e["token"].as_str().unwrap_or("");
        if !e["shadow"].is_null() {
            let s = &e["shadow"];
            if f(&s["shares"]) > 0.0 {
                shadow.add(f(&s["usd"]) + f(&s["fee"]), f(&s["shares"]) * price.get(tok).copied().unwrap_or(0.0));
            }
        }
        let fill = &e["fill"];
        let shares = f(&fill["shares"]);
        if shares <= 0.0 {
            continue;
        }
        let k = format!("{}|{}", e["wallet"].as_str().unwrap_or(""), tok);
        let x = &keys[&k];
        let value = if x.bought > 0.0 { shares / x.bought * key_value(&k, x) } else { 0.0 };
        let cost = f(&fill["usd"]) + f(&fill["fee"]);
        all.add(cost, value);
        by_phase.entry(e["phase"].as_str().unwrap_or("?").to_string()).or_default().add(cost, value);
        by_source.entry(e["source"].as_str().unwrap_or("?").to_string()).or_default().add(cost, value);
        let bps = f(&e["vs_wallet_bps"]);
        slip.push(bps);
        let bucket = if bps <= 0.0 { "a) at or under the wallet" } else if bps <= 100.0 { "b) +0..1%" } else if bps <= 200.0 { "c) +1..2%" } else { "d) over +2% (one tick)" };
        by_slip.entry(bucket).or_default().add(cost, value);
        by_wallet.entry(e["wallet"].as_str().unwrap_or("").to_string()).or_default().add(cost, value);
    }
    let open_keys = keys.values().filter(|x| x.bought - x.sold - x.settled_shares > 1e-6).count();
    println!("paper copies{}: {} wallet buys seen", since_h.map(|h| format!(" (last {h} h)")).unwrap_or_default(), buys.len());
    let tried = filled + missed_cap + missed_other;
    println!("  filled {filled}, missed by the cap {missed_cap}, other misses {missed_other}, seen too late {missed_late}  \
              (fill rate {:.0}% of the buys we reached)", filled as f64 / tried.max(1) as f64 * 100.0);
    println!("  trade -> seen p50 {:.2} s p90 {:.2} s | trade -> book read p50 {:.2} s p90 {:.2} s | price vs wallet p50 {:+.0} bps p90 {:+.0} bps",
        pctl(&mut detect.clone(), 0.5), pctl(&mut detect, 0.9), pctl(&mut book_lag.clone(), 0.5), pctl(&mut book_lag, 0.9),
        pctl(&mut slip.clone(), 0.5), pctl(&mut slip, 0.9));
    println!("  outcomes still open: {open_keys} (valued at the current price)");
    println!("{}", all.line("ALL filled buys"));
    println!("by phase (buy time vs the game's start)");
    for (k, a) in &by_phase {
        println!("{}", a.line(k));
    }
    println!("by price paid vs the wallet");
    for (k, a) in &by_slip {
        println!("{}", a.line(k));
    }
    println!("by how the trade was seen");
    for (k, a) in &by_source {
        println!("{}", a.line(k));
    }
    println!("missed by the cap, bought uncapped instead (shadow, held to settlement)");
    println!("{}", shadow.line("shadow"));
    let mut ws: Vec<(String, Acc)> = by_wallet.into_iter().collect();
    ws.sort_by(|a, b| (b.1.value - b.1.cost).total_cmp(&(a.1.value - a.1.cost)));
    println!("wallets: {} copied; best and worst", ws.len());
    let n = ws.len();
    for (i, (w, a)) in ws.iter().enumerate() {
        if i < 10 || i + 10 >= n {
            println!("{}", a.line(w));
        } else if i == 10 {
            println!("  ...");
        }
    }
    let mut csv = std::fs::File::create(cfg.data_dir.join("report_wallets.csv"))?;
    writeln!(csv, "wallet,buys,cost,value,pnl,roi_pct")?;
    for (w, a) in &ws {
        let pnl = a.value - a.cost;
        writeln!(csv, "{w},{},{:.4},{:.4},{:.4},{:.2}", a.n, a.cost, a.value, pnl, if a.cost > 0.0 { pnl / a.cost * 100.0 } else { 0.0 })?;
    }
    println!("per-wallet table: {}", cfg.data_dir.join("report_wallets.csv").display());
    Ok(())
}
