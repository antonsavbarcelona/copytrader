//! Round one: the big players. The candidates are the wallets in the liquid markets (the
//! biggest holders and the large trades of the markets traded most over the last day) and
//! those on Polymarket's leaderboards (every category, month and all-time). A wallet passes
//! when it trades often (`min_week_trades` trades in the last week, of its last 500) and has
//! made real money: `min_pnl` dollars over the last `months` months, from the profit history
//! behind its profile chart. (Trade sizes do not tell: an active trader's buy is often dozens
//! of small fills.)
//!
//! That history also gives its record (`record`, metric B): how often and for how long it has
//! made money. After a profitable week on paper the record tells a steady trader (`steady`:
//! golden) from a lucky one (dropped); a wallet younger than `min_age_days` has too short a
//! record and gets two weeks on paper instead, see trial.rs.

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
    /// Round one: a big player.
    pub min_week_trades: usize,
    pub min_pnl: f64,
    /// Steady (after the week on paper): trading for `min_age_days` (younger: two weeks on
    /// paper instead of the record), in profit in
    /// `min_months_up` of the last `months` months (30 days each) and over them, in a
    /// `min_weeks_up` share of their weeks, its best day under `max_best_day` of the profit.
    pub min_age_days: f64,
    pub months: usize,
    pub min_months_up: usize,
    pub min_weeks_up: f64,
    pub max_best_day: f64,
}

impl Default for Criteria {
    fn default() -> Self {
        Self {
            min_week_trades: 20,
            min_pnl: 50_000.0,
            min_age_days: 120.0,
            months: 4,
            min_months_up: 3,
            min_weeks_up: 0.6,
            max_best_day: 0.5,
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

/// The longest run of `true`, oldest to newest or the other way.
fn longest(v: &[bool]) -> usize {
    v.split(|x| !x).map(<[bool]>::len).max().unwrap_or(0)
}

/// Metric B, the wallet's record of success from its profit history (`h`, daily): its age, its
/// profit over the last `months` months, each month's and how many were up, the share of
/// weeks up, the longest runs of months and weeks up (newest first), the current run of weeks
/// up, and its best day.
pub fn record(h: &[(f64, f64)], t: f64, c: &Criteria) -> Value {
    let (Some(first), Some(last)) = (h.first(), h.last()) else {
        return json!({"age_days": 0});
    };
    let end = last.0;
    let span = c.months as f64 * 30.0 * DAY;
    let window = |len: f64, k: usize| at(h, end - len * k as f64) - at(h, end - len * (k + 1) as f64);
    let months: Vec<f64> = (0..c.months).map(|k| window(30.0 * DAY, k)).collect();
    let weeks: Vec<f64> = (0..(c.months * 30 / 7)).map(|k| window(7.0 * DAY, k)).collect();
    // The whole history in months, for the longest run.
    let all_months: Vec<bool> = (0..((end - first.0) / (30.0 * DAY)) as usize).map(|k| window(30.0 * DAY, k) > 0.0).collect();
    let weeks_up: Vec<bool> = weeks.iter().map(|w| *w > 0.0).collect();
    let pnl = at(h, end) - at(h, end - span);
    let best_day = h.windows(2).filter(|w| w[1].0 > end - span).map(|w| w[1].1 - w[0].1).fold(0.0, f64::max);
    let r = |x: f64| x.round();
    json!({
        "age_days": r((t - first.0) / DAY), "idle_days": r((t - end) / DAY), "pnl": r(pnl),
        "months": months.iter().map(|m| r(*m)).collect::<Vec<_>>(),
        "months_up": months.iter().filter(|m| **m > 0.0).count(),
        "longest_months_up": longest(&all_months),
        "weeks_up": (weeks_up.iter().filter(|w| **w).count() as f64 / weeks.len().max(1) as f64 * 100.0).round() / 100.0,
        "longest_weeks_up": longest(&weeks_up),
        "weeks_up_now": weeks_up.iter().take_while(|w| **w).count(),
        "best_day": r(best_day), "all_time": r(last.1),
    })
}

/// Whether the record is a steady trader's; if not, why.
pub fn steady(b: &Value, c: &Criteria) -> Result<(), &'static str> {
    let f = |k: &str| b[k].as_f64().unwrap_or(0.0);
    if f("age_days") < c.min_age_days {
        Err("too new")
    } else if (f("months_up") as usize) < c.min_months_up {
        Err("months in loss")
    } else if f("pnl") <= 0.0 {
        Err("in loss")
    } else if f("weeks_up") < c.min_weeks_up {
        Err("weeks in loss")
    } else if f("best_day") > c.max_best_day * f("pnl") {
        Err("one lucky day")
    } else {
        Ok(())
    }
}

/// The wallet's record now, from its profit history.
pub async fn record_of(api: &Api, wallet: &str, c: &Criteria) -> anyhow::Result<Value> {
    let h = api.pnl_history(wallet, "all", "1d").await?;
    let mut b = record(&h, now(), c);
    b["steady"] = json!(steady(&b, c).err().unwrap_or(""));
    Ok(b)
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

/// One wallet: its trades (active?), then the profit and record of those that are.
async fn judge(api: Api, c: Criteria, wallet: String, name: Option<String>) -> Option<Judged> {
    let trades = match api.trade_sizes(&wallet, 500).await {
        Ok(t) => t,
        Err(e) => {
            log!("scout: {wallet} trades read failed: {e}");
            return None;
        }
    };
    let week = trades.iter().filter(|(ts, _)| now() - ts < 7.0 * DAY).count();
    let m = median(trades.iter().map(|(_, usd)| *usd).collect());
    let mut why = if week < c.min_week_trades { "few trades this week" } else { "" };
    let mut stats = json!({"median_trade": m.round(), "week_trades": week});
    if why.is_empty() {
        match record_of(&api, &wallet, &c).await {
            Ok(b) => {
                if b["pnl"].as_f64().unwrap_or(0.0) < c.min_pnl {
                    why = "small profit";
                }
                stats["record"] = b;
            }
            Err(e) => {
                log!("scout: {wallet} history read failed: {e}");
                return None;
            }
        }
    }
    stats["why"] = json!(why);
    Some(Judged { wallet, name, ok: why.is_empty(), stats })
}

/// One scan: every candidate not in `skip` (on trial already, or judged lately) is judged,
/// each sent to `out` as soon as it is (a scan takes the best part of an hour). Returns how
/// many were judged and how many passed.
pub async fn scan(api: &Api, c: &Criteria, skip: &HashSet<String>, out: &mpsc::UnboundedSender<Judged>) -> (usize, usize) {
    let cands: Vec<(String, Option<String>)> = candidates(api).await.into_iter().filter(|(w, _)| !skip.contains(w)).collect();
    log!("scout: judging {} wallets", cands.len());
    // The data API answers in ~0.1 s; the profit history (active wallets only) takes seconds.
    let mut judged = futures_util::stream::iter(cands.into_iter().map(|(w, n)| judge(api.clone(), c.clone(), w, n))).buffer_unordered(16);
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

    fn steady_of(h: &[(f64, f64)], t: f64) -> Result<(), &'static str> {
        steady(&record(h, t, &Criteria::default()), &Criteria::default())
    }

    #[test]
    fn steady_or_lucky() {
        let t = 1_800_000_000.0;
        assert_eq!(steady_of(&hist(t, 200, |_| 100.0), t), Ok(()));
        let b = record(&hist(t, 200, |_| 100.0), t, &Criteria::default());
        assert_eq!(b["weeks_up_now"], 17);
        assert_eq!(b["longest_months_up"], 6);
        // the same profit in one day
        assert_eq!(steady_of(&hist(t, 200, |i| if i == 150 { 12_000.0 } else { 0.0 }), t), Err("months in loss"));
        assert_eq!(steady_of(&hist(t, 200, |i| if i % 30 == 0 { 3000.0 } else { -50.0 }), t), Err("weeks in loss"));
        assert_eq!(steady_of(&hist(t, 200, |i| if i == 190 { 9000.0 } else { 10.0 }), t), Err("one lucky day"));
        assert_eq!(steady_of(&hist(t, 60, |_| 100.0), t), Err("too new"));
        assert_eq!(steady_of(&hist(t, 200, |i| if (i / 30) % 2 == 0 { 200.0 } else { -150.0 }), t).is_err(), true);
    }
}
