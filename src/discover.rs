//! Finds traders of the short crypto markets ("Bitcoin Up or Down - ...", ETH, SOL, XRP ...)
//! worth a paper trial: the wallets at the top of Polymarket's crypto leaderboard (week and
//! month), judged on their own closed positions in those markets only.
//!
//! A wallet qualifies with at least `min_positions` recent Up-or-Down positions: it trades
//! these markets. The week on paper decides whether copying it pays (the golden list), so by
//! default nothing else is asked; `min_pnl`, `min_roi`, `min_week_share` and `max_per_day`
//! can narrow the intake. The record found is saved with the wallet either way.

use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::{Value, json};

use crate::api::{Api, now};
use crate::log;

/// What a scan judges a wallet on: its newest closed positions, this many at most.
const POSITIONS: usize = 500;
/// A wallet judged is not judged again for this long.
const RESCAN_S: f64 = 24.0 * 3600.0;

#[derive(Clone, Debug)]
pub struct Criteria {
    pub min_positions: usize,
    pub min_pnl: f64,
    pub min_roi: f64,
    pub min_week_share: f64,
    pub max_per_day: f64,
}

pub struct Found {
    pub wallet: String,
    pub name: Option<String>,
    pub stats: Value,
}

/// The wallet's record in the Up-or-Down markets, and whether it qualifies.
fn judge(positions: &[Value], c: &Criteria) -> (bool, Value) {
    let ud: Vec<&Value> =
        positions.iter().filter(|p| p["title"].as_str().is_some_and(|t| t.contains("Up or Down"))).collect();
    let f = |v: &Value| v.as_f64().unwrap_or(0.0);
    let costs: Vec<f64> = ud.iter().map(|p| f(&p["avgPrice"]) * f(&p["totalBought"])).collect();
    let cost: f64 = costs.iter().sum();
    let pnl: f64 = ud.iter().map(|p| f(&p["realizedPnl"])).sum();
    let mut weeks: BTreeMap<i64, f64> = BTreeMap::new();
    let ts: Vec<f64> = ud.iter().map(|p| f(&p["timestamp"])).collect();
    for (p, t) in ud.iter().zip(&ts) {
        *weeks.entry((*t / (7.0 * 86400.0)) as i64).or_default() += f(&p["realizedPnl"]);
    }
    let weeks_up = weeks.values().filter(|v| **v > 0.0).count();
    let span_days = match (ts.iter().cloned().reduce(f64::min), ts.iter().cloned().reduce(f64::max)) {
        (Some(a), Some(b)) => ((b - a) / 86400.0).max(1.0),
        _ => 1.0,
    };
    let per_day = ud.len() as f64 / span_days;
    let mut sorted = costs.clone();
    sorted.sort_by(f64::total_cmp);
    let median_bet = sorted.get(sorted.len() / 2).copied().unwrap_or(0.0);
    let roi = if cost > 0.0 { pnl / cost } else { 0.0 };
    let week_share = if weeks.is_empty() { 0.0 } else { weeks_up as f64 / weeks.len() as f64 };
    let ok = ud.len() >= c.min_positions
        && pnl >= c.min_pnl
        && roi >= c.min_roi
        && week_share >= c.min_week_share
        && per_day <= c.max_per_day;
    let stats = json!({
        "positions": ud.len(), "days": (span_days * 10.0).round() / 10.0, "per_day": (per_day * 10.0).round() / 10.0,
        "cost": cost.round(), "pnl": pnl.round(), "roi": (roi * 1000.0).round() / 1000.0,
        "weeks_up": weeks_up, "weeks": weeks.len(), "median_bet": (median_bet * 100.0).round() / 100.0,
        "scanned_at": now().round(),
    });
    (ok, stats)
}

/// One scan: the leaderboard's wallets not followed yet (`known`) and not judged lately
/// (`judged`, updated) are judged; those that qualify are returned.
pub async fn scan(api: &Api, c: &Criteria, known: &HashSet<String>, judged: &mut HashMap<String, f64>) -> Vec<Found> {
    let mut leaders = Vec::new();
    for period in ["WEEK", "MONTH"] {
        match api.crypto_leaders(period, 100).await {
            Ok(l) => leaders.extend(l),
            Err(e) => log!("discover: leaderboard {period} read failed: {e}"),
        }
    }
    let t = now();
    let mut seen = HashSet::new();
    let mut found = Vec::new();
    let mut checked = 0;
    for w in leaders {
        if known.contains(&w.address) || !seen.insert(w.address.clone()) {
            continue;
        }
        if judged.get(&w.address).is_some_and(|at| t - at < RESCAN_S) {
            continue;
        }
        let positions = match api.closed_positions(&w.address, POSITIONS).await {
            Ok(p) => p,
            Err(e) => {
                log!("discover: {} positions read failed: {e}", w.address);
                continue;
            }
        };
        judged.insert(w.address.clone(), t);
        checked += 1;
        let (ok, stats) = judge(&positions, c);
        if ok {
            log!("discover: {} qualifies {stats}", w.name.as_deref().unwrap_or(&w.address));
            found.push(Found { wallet: w.address, name: w.name, stats });
        }
    }
    log!("discover: {checked} wallets judged, {} new to paper-copy", found.len());
    found
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(title: &str, price: f64, bought: f64, pnl: f64, ts: f64) -> Value {
        json!({"title": title, "avgPrice": price, "totalBought": bought, "realizedPnl": pnl, "timestamp": ts})
    }

    fn crit() -> Criteria {
        Criteria { min_positions: 3, min_pnl: 10.0, min_roi: 0.03, min_week_share: 0.6, max_per_day: 150.0 }
    }

    #[test]
    fn judges_only_up_or_down_and_spots_bots() {
        let week = 7.0 * 86400.0;
        let mut p = vec![
            pos("Bitcoin Up or Down - October 9, 5PM ET", 0.5, 100.0, 20.0, 0.0),
            pos("Ethereum Up or Down - October 1", 0.5, 100.0, -5.0, week),
            pos("Solana Up or Down - October 9", 0.5, 100.0, 10.0, 2.0 * week),
            pos("Will Arsenal win?", 0.5, 1000.0, -900.0, 2.0 * week),
        ];
        let (ok, s) = judge(&p, &crit());
        assert!(ok, "{s}");
        assert_eq!(s["positions"], 3);
        assert_eq!(s["pnl"], 25.0);
        assert_eq!(s["weeks_up"], 2);
        // the same record squeezed into one hour: a bot
        for x in &mut p {
            x["timestamp"] = json!(0.0);
        }
        p.extend((0..200).map(|_| pos("Bitcoin Up or Down - x", 0.5, 1.0, 0.0, 3600.0)));
        assert!(!judge(&p, &crit()).0);
    }
}
