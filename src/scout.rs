//! Round one: finding traders whose profit is a pattern, not luck. The candidates are the
//! wallets in the liquid markets (the biggest holders and the large trades of the markets
//! traded most over the last day) and those on Polymarket's leaderboards (every category,
//! month and all-time). Each is judged on the profit history behind its profile chart:
//!
//! - trading for `min_age_days` at least, and lately (the history goes on to this week);
//! - in profit in `min_months_up` of the last `months` months (30 days each), with
//!   `min_pnl` dollars or more over them;
//! - in profit in a `min_weeks_up` share of the weeks of those months;
//! - not made in one stroke: its best day is under `max_best_day` of the profit;
//! - with a real deposit: its median trade (the last 500) is `min_median_trade` dollars or
//!   more.
//!
//! Those that pass go on to the week on paper (trial.rs).

use std::collections::{HashMap, HashSet};

use futures_util::StreamExt;
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::api::{Api, WalletInfo, now};
use crate::log;

const DAY: f64 = 86400.0;
const CATEGORIES: [&str; 9] =
    ["OVERALL", "POLITICS", "SPORTS", "CRYPTO", "CULTURE", "WEATHER", "ECONOMICS", "TECH", "FINANCE"];

#[derive(Clone, Debug)]
pub struct Criteria {
    pub min_age_days: f64,
    pub months: usize,
    pub min_months_up: usize,
    pub min_pnl: f64,
    pub min_weeks_up: f64,
    pub max_best_day: f64,
    pub min_median_trade: f64,
}

impl Default for Criteria {
    fn default() -> Self {
        Self {
            min_age_days: 120.0,
            months: 4,
            min_months_up: 3,
            min_pnl: 5000.0,
            min_weeks_up: 0.6,
            max_best_day: 0.5,
            min_median_trade: 50.0,
        }
    }
}

pub struct Judged {
    pub wallet: String,
    pub name: Option<String>,
    pub ok: bool,
    pub stats: Value,
}

/// The cumulative profit at `t`: the last point at or before it (0 before the first).
pub fn at(h: &[(f64, f64)], t: f64) -> f64 {
    match h.partition_point(|(ts, _)| *ts <= t) {
        0 => 0.0,
        i => h[i - 1].1,
    }
}

/// The profit history's verdict (the deposit is checked apart), and the record.
fn judge_history(h: &[(f64, f64)], t: f64, c: &Criteria) -> (bool, Value) {
    let (Some(first), Some(last)) = (h.first(), h.last()) else {
        return (false, json!({"why": "no history"}));
    };
    let age = (t - first.0) / DAY;
    let end = last.0;
    let span = c.months as f64 * 30.0 * DAY;
    let window = |len: f64, k: usize| at(h, end - len * k as f64) - at(h, end - len * (k + 1) as f64);
    let months: Vec<f64> = (0..c.months).map(|k| window(30.0 * DAY, k)).collect();
    let weeks: Vec<f64> = (0..(c.months * 30 / 7)).map(|k| window(7.0 * DAY, k)).collect();
    let pnl = at(h, end) - at(h, end - span);
    let best_day = h.windows(2).filter(|w| w[1].0 > end - span).map(|w| w[1].1 - w[0].1).fold(0.0, f64::max);
    let months_up = months.iter().filter(|m| **m > 0.0).count();
    let weeks_up = weeks.iter().filter(|w| **w > 0.0).count() as f64 / weeks.len().max(1) as f64;
    let why = if age < c.min_age_days {
        "too new"
    } else if t - end > 7.0 * DAY {
        "not trading lately"
    } else if months_up < c.min_months_up {
        "months in loss"
    } else if pnl < c.min_pnl {
        "profit too small"
    } else if weeks_up < c.min_weeks_up {
        "weeks in loss"
    } else if best_day > c.max_best_day * pnl {
        "one lucky day"
    } else {
        ""
    };
    let r = |x: f64| x.round();
    let stats = json!({
        "age_days": r(age), "pnl": r(pnl), "months": months.iter().map(|m| r(*m)).collect::<Vec<_>>(),
        "months_up": months_up, "weeks_up": (weeks_up * 100.0).round() / 100.0, "best_day": r(best_day),
        "why": why,
    });
    (why.is_empty(), stats)
}

fn median(mut v: Vec<f64>) -> f64 {
    v.sort_by(f64::total_cmp);
    v.get(v.len() / 2).copied().unwrap_or(0.0)
}

/// The candidates: wallets in the liquid markets and on the leaderboards, by address.
async fn candidates(api: &Api) -> HashMap<String, Option<String>> {
    let mut out: HashMap<String, Option<String>> = HashMap::new();
    let add = |list: Vec<WalletInfo>, out: &mut HashMap<String, Option<String>>| {
        for w in list.into_iter().filter(|w| w.address.starts_with("0x")) {
            let e = out.entry(w.address).or_default();
            if e.is_none() {
                *e = w.name;
            }
        }
    };
    let markets = api.liquid_markets(400, 10_000.0).await.unwrap_or_else(|e| {
        log!("scout: markets read failed: {e}");
        Vec::new()
    });
    let mut from_markets = futures_util::stream::iter(markets.iter().map(|(c, _)| c.clone()).collect::<Vec<_>>().into_iter().map(|c| {
        let api = api.clone();
        async move { api.market_traders(&c, 100.0).await }
    }))
    .buffer_unordered(4);
    while let Some(r) = from_markets.next().await {
        match r {
            Ok(l) => add(l, &mut out),
            Err(e) => log!("scout: market read failed: {e}"),
        }
    }
    let in_markets = out.len();
    for cat in CATEGORIES {
        for period in ["MONTH", "ALL"] {
            match api.leaders(cat, period, 500).await {
                Ok(l) => add(l, &mut out),
                Err(e) => log!("scout: leaderboard {cat} {period} read failed: {e}"),
            }
        }
    }
    log!("scout: {} candidates ({} markets: {in_markets} wallets; leaderboards: {} more)", out.len(), markets.len(),
        out.len() - in_markets);
    out
}

/// One wallet: its history, then (if that passes) its trade sizes.
async fn judge(api: Api, c: Criteria, wallet: String, name: Option<String>) -> Option<Judged> {
    let h = match api.pnl_history(&wallet, "all", "1d").await {
        Ok(h) => h,
        Err(e) => {
            log!("scout: {wallet} history read failed: {e}");
            return None;
        }
    };
    let (mut ok, mut stats) = judge_history(&h, now(), &c);
    if ok {
        let sizes = api.trade_sizes(&wallet, 500).await.ok()?;
        let m = median(sizes);
        stats["median_trade"] = json!(m.round());
        if m < c.min_median_trade {
            ok = false;
            stats["why"] = json!("small trades");
        }
    }
    Some(Judged { wallet, name, ok, stats })
}

/// One scan: every candidate not in `skip` (on trial already, or judged lately) is judged,
/// each sent to `out` as soon as it is (a scan takes the best part of an hour). Returns how
/// many were judged and how many passed.
pub async fn scan(api: &Api, c: &Criteria, skip: &HashSet<String>, out: &mpsc::UnboundedSender<Judged>) -> (usize, usize) {
    let cands: Vec<(String, Option<String>)> = candidates(api).await.into_iter().filter(|(w, _)| !skip.contains(w)).collect();
    log!("scout: judging {} wallets", cands.len());
    // The profit history API takes seconds over an active wallet and allows ~20 requests a
    // second: 24 at a time keep it busy without hitting that.
    let mut judged = futures_util::stream::iter(cands.into_iter().map(|(w, n)| judge(api.clone(), c.clone(), w, n))).buffer_unordered(24);
    let (mut n, mut ok) = (0, 0);
    while let Some(j) = judged.next().await {
        if let Some(j) = j {
            n += 1;
            if n % 2000 == 0 {
                log!("scout: {n} judged, {ok} pass so far");
            }
            if j.ok {
                ok += 1;
                log!("scout: {} passes {}", j.name.as_deref().unwrap_or(&j.wallet), j.stats);
            }
            let _ = out.send(j);
        }
    }
    log!("scout: {n} judged, {ok} pass");
    (n, ok)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A history of `days` days ending at `t`, `daily(i)` dollars on day i.
    fn hist(t: f64, days: usize, daily: impl Fn(usize) -> f64) -> Vec<(f64, f64)> {
        let mut p = 0.0;
        (0..days)
            .map(|i| {
                p += daily(i);
                (t - (days - 1 - i) as f64 * DAY, p)
            })
            .collect()
    }

    #[test]
    fn steady_passes_luck_does_not() {
        let t = 1_800_000_000.0;
        let c = Criteria::default();
        let (ok, s) = judge_history(&hist(t, 200, |_| 100.0), t, &c);
        assert!(ok, "{s}");
        // the same profit in one day
        let (ok, s) = judge_history(&hist(t, 200, |i| if i == 150 { 12_000.0 } else { 0.0 }), t, &c);
        assert!(!ok);
        assert_eq!(s["why"], "months in loss");
        let (ok, s) = judge_history(&hist(t, 200, |i| if i % 30 == 0 { 3000.0 } else { -50.0 }), t, &c);
        assert!(!ok);
        assert_eq!(s["why"], "weeks in loss");
        // too new, and not trading lately
        assert_eq!(judge_history(&hist(t, 60, |_| 100.0), t, &c).1["why"], "too new");
        assert_eq!(judge_history(&hist(t - 30.0 * DAY, 200, |_| 100.0), t, &c).1["why"], "not trading lately");
        // losing months
        let (ok, _) = judge_history(&hist(t, 200, |i| if (i / 30) % 2 == 0 { 200.0 } else { -150.0 }), t, &c);
        assert!(!ok);
    }
}
