//! The allocator: which of the wallets copied on paper a portfolio would copy, and how much.
//!
//! The paper copies every wallet followed at $1 (engine.rs). On top of it, a few virtual
//! portfolios ("books") each take only some of those copies: the wallets they pick, at their
//! weight, inside their caps. A book's copy fills as the paper's did (at $1 the size does not
//! move the price), so its result is the paper lots it took, scaled. Nothing here trades.
//!
//! Each day the wallets are ranked: the 90-day backtest (the prior, a file) and the paper's
//! closed lots of the last 30 days, pooled: score = (pnl) / (cost + SHRINK), so a short record
//! counts little. Eligible: a directional wallet with MIN_N+ backtest copies at ROI > MIN_ROI
//! whose pooled score stays above 0. The books:
//!
//!     TOP10, TOP30   the best by score, weight 1
//!     RANK30         the best 30, weight 2 for the first down to 0.5 for the 30th
//!     RANDOM30       30 eligible drawn each day: what the ranking adds over the filter alone
//!     ELIGIBLE       every eligible wallet, no caps: what the filter adds over the paper
//!
//! Caps (in $ of open cost, a copy is $1): the whole book, one wallet, one event (a game's
//! winner, spread and total are one bet), and an outcome another wallet already brought in is
//! not bought again. A wallet that leaves the picks gets no new copies, but its lots go on to
//! its sale or the settlement. Every copy of an eligible wallet's trade gets a decision row in
//! `alloc` (granted or why not); `alloc_rank` keeps each day's ranking.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_postgres::Client;

use crate::api::{Api, now};
use crate::db;
use crate::log;

const MIN_N: f64 = 100.0;
const MIN_ROI: f64 = 0.05;
const SHRINK: f64 = 50.0;
const PAPER_DAYS: i32 = 30;

#[derive(Clone, Copy, PartialEq)]
enum Pick {
    Top(usize),
    RankWeighted(usize),
    Random(usize),
    Eligible,
}

struct Book {
    name: &'static str,
    pick: Pick,
    /// $ of open cost: the book, one wallet, one event.
    total: f64,
    wallet: f64,
    event: f64,
}

const CAPPED: (f64, f64, f64) = (60.0, 6.0, 3.0);

fn books() -> Vec<Book> {
    let b = |name, pick, (total, wallet, event): (f64, f64, f64)| Book { name, pick, total, wallet, event };
    let free = (f64::INFINITY, f64::INFINITY, f64::INFINITY);
    vec![
        b("TOP10", Pick::Top(10), CAPPED),
        b("TOP30", Pick::Top(30), CAPPED),
        b("RANK30", Pick::RankWeighted(30), CAPPED),
        b("RANDOM30", Pick::Random(30), CAPPED),
        b("ELIGIBLE", Pick::Eligible, free),
    ]
}

/// A wallet's 90-day backtest: copies of its $100+ bets and their ROI after the haircut.
pub struct Prior {
    n: f64,
    roi: f64,
    style: String,
}

/// The backtest's CSV: wallet,n,roi,style.
pub fn read_prior(path: &Path) -> Result<HashMap<String, Prior>> {
    let mut out = HashMap::new();
    for l in std::fs::read_to_string(path)?.lines().skip(1) {
        let f: Vec<&str> = l.split(',').collect();
        if f.len() < 4 {
            continue;
        }
        let (Ok(n), Ok(roi)) = (f[1].parse(), f[2].parse()) else { continue };
        out.insert(f[0].trim().to_lowercase(), Prior { n, roi, style: f[3].trim().to_string() });
    }
    Ok(out)
}

#[derive(Clone, Serialize, Deserialize)]
struct Ranked {
    wallet: String,
    rank: usize,
    score: f64,
}

/// An open lot a book took: the wallet's outcome, its event and the $ still in it.
#[derive(Clone, Serialize, Deserialize)]
struct Lot {
    wallet: String,
    token: String,
    event: String,
    open: f64,
}

#[derive(Default, Serialize, Deserialize)]
struct State {
    /// The last paper event read.
    last_id: i64,
    /// The day (unix days) the ranking is of.
    day: i64,
    ranked: Vec<Ranked>,
    /// Per book: wallet -> weight.
    weights: HashMap<String, HashMap<String, f64>>,
    /// Per book: its open lots.
    lots: HashMap<String, Vec<Lot>>,
}

struct Decision {
    book: &'static str,
    granted: f64,
    reason: &'static str,
}

/// The allocator's loop: ranks once a day, reads the paper's new events every 15 s.
pub async fn run(url: String, api: Api, prior: HashMap<String, Prior>) {
    let books = books();
    let mut client: Option<Client> = None;
    let mut st: Option<State> = None;
    loop {
        if client.as_ref().is_none_or(|c| c.is_closed()) {
            client = db::connect(&url).await.map_err(|e| log!("alloc: db connect failed: {e}")).ok();
        }
        if let Some(c) = &client {
            if st.is_none() {
                st = load(c).await.map_err(|e| log!("alloc: state not read: {e}")).ok();
                if let Some(s) = &st {
                    log!("alloc: {} books, from paper event {}, {} lots open", books.len(), s.last_id,
                        s.lots.values().map(Vec::len).sum::<usize>());
                }
            }
            if let Some(s) = st.as_mut() {
                if let Err(e) = step(c, &api, &prior, &books, s).await {
                    log!("alloc: {e:#}");
                }
            }
        }
        tokio::time::sleep(Duration::from_secs(15)).await;
    }
}

async fn load(c: &Client) -> Result<State> {
    let row = c.query_opt("SELECT data FROM state WHERE id = 'alloc'", &[]).await?;
    match row {
        Some(r) => Ok(serde_json::from_value(r.get::<_, Value>(0))?),
        None => {
            // A first start reads the paper from now on.
            let last: i64 = c.query_one("SELECT coalesce(max(id), 0) FROM events", &[]).await?.get(0);
            Ok(State { last_id: last, ..Default::default() })
        }
    }
}

async fn save(c: &Client, s: &State) -> Result<()> {
    c.execute(
        "INSERT INTO state (id, saved_at, data) VALUES ('alloc', now(), $1)
         ON CONFLICT (id) DO UPDATE SET saved_at = now(), data = EXCLUDED.data",
        &[&serde_json::to_value(s)?],
    )
    .await?;
    Ok(())
}

async fn step(c: &Client, api: &Api, prior: &HashMap<String, Prior>, books: &[Book], s: &mut State) -> Result<()> {
    let day = (now() / 86400.0) as i64;
    if s.day != day {
        rank(c, prior, books, s, day).await?;
        save(c, s).await?;
    }
    let rows = c
        .query(
            "SELECT id, kind, wallet, token, data FROM events WHERE id > $1 AND kind IN ('buy', 'sell', 'settle')
             ORDER BY id LIMIT 5000",
            &[&s.last_id],
        )
        .await?;
    if rows.is_empty() {
        return Ok(());
    }
    let eligible: HashSet<String> = s.ranked.iter().map(|r| r.wallet.clone()).collect();
    let mut decided = Vec::new();
    for r in &rows {
        let (id, kind, data): (i64, String, Value) = (r.get(0), r.get(1), r.get(4));
        let wallet: String = r.get::<_, Option<String>>(2).unwrap_or_default();
        let token: String = r.get::<_, Option<String>>(3).unwrap_or_default();
        s.last_id = id;
        match kind.as_str() {
            "buy" => {
                let fill = &data["fill"];
                let cost = fill["usd"].as_f64().unwrap_or(0.0) + fill["fee"].as_f64().unwrap_or(0.0);
                if cost <= 0.0 || !eligible.contains(&wallet) {
                    continue;
                }
                let event = match api.market(&token, 7.0 * 86400.0).await {
                    Ok(m) => m.event.unwrap_or(m.condition),
                    Err(_) => data["condition"].as_str().unwrap_or(&token).to_string(),
                };
                for b in books {
                    let d = decide(b, s, &wallet, &token, &event, cost);
                    decided.push((id, wallet.clone(), token.clone(), event.clone(), cost, d));
                }
            }
            "sell" => {
                // What the paper still holds after the sale, as a share of before.
                let (before, after) = (data["held_before"].as_f64().unwrap_or(0.0), data["held_after"].as_f64().unwrap_or(0.0));
                if before > 0.0 && after < before {
                    let keep = (after / before).max(0.0);
                    for lots in s.lots.values_mut() {
                        for l in lots.iter_mut().filter(|l| l.wallet == wallet && l.token == token) {
                            l.open *= keep;
                        }
                        lots.retain(|l| l.open > 1e-6);
                    }
                }
            }
            _ => {
                for lots in s.lots.values_mut() {
                    lots.retain(|l| !(l.wallet == wallet && l.token == token));
                }
            }
        }
    }
    if !decided.is_empty() {
        let rank_of: HashMap<&str, &Ranked> = s.ranked.iter().map(|r| (r.wallet.as_str(), r)).collect();
        let stmt = c
            .prepare(
                "INSERT INTO alloc (at, book, event_id, wallet, token, event, rank, score, requested, granted, reason)
                 VALUES (now(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)",
            )
            .await?;
        for (id, w, t, e, cost, d) in &decided {
            let r = rank_of.get(w.as_str());
            c.execute(&stmt, &[&d.book, id, w, t, e, &r.map(|r| r.rank as i32), &r.map(|r| r.score), cost, &d.granted,
                &d.reason]).await?;
        }
    }
    save(c, s).await
}

fn decide(b: &Book, s: &mut State, wallet: &str, token: &str, event: &str, cost: f64) -> Decision {
    let no = |reason| Decision { book: b.name, granted: 0.0, reason };
    let Some(w) = s.weights.get(b.name).and_then(|m| m.get(wallet)).copied() else {
        return no("NOT_SELECTED");
    };
    let want = cost * w;
    let lots = s.lots.entry(b.name.to_string()).or_default();
    if lots.iter().any(|l| l.token == token && l.wallet != wallet) {
        return no("DUPLICATE");
    }
    let sum = |f: &dyn Fn(&Lot) -> bool| lots.iter().filter(|l| f(l)).map(|l| l.open).sum::<f64>();
    if sum(&|_| true) + want > b.total + 1e-9 {
        return no("TOTAL_CAP");
    }
    if sum(&|l| l.wallet == wallet) + want > b.wallet + 1e-9 {
        return no("WALLET_CAP");
    }
    if sum(&|l| l.event == event) + want > b.event + 1e-9 {
        return no("EVENT_CAP");
    }
    match lots.iter_mut().find(|l| l.wallet == wallet && l.token == token) {
        Some(l) => l.open += want,
        None => lots.push(Lot { wallet: wallet.into(), token: token.into(), event: event.into(), open: want }),
    }
    Decision { book: b.name, granted: want, reason: "GRANTED" }
}

/// The day's ranking: the backtest pooled with the paper's closed lots of the last 30 days.
async fn rank(c: &Client, prior: &HashMap<String, Prior>, books: &[Book], s: &mut State, day: i64) -> Result<()> {
    let rows = c
        .query(
            "WITH k AS (
               SELECT wallet, token,
                 sum(CASE WHEN kind = 'buy' THEN coalesce((data->'fill'->>'usd')::float8, 0) + coalesce((data->'fill'->>'fee')::float8, 0) ELSE 0 END) AS cost,
                 sum(CASE WHEN kind = 'sell' THEN coalesce((data->'fill'->>'usd')::float8, 0) - coalesce((data->'fill'->>'fee')::float8, 0)
                          WHEN kind = 'settle' THEN (data->>'usd')::float8 ELSE 0 END) AS back,
                 bool_or(kind = 'settle' OR (kind = 'sell' AND (data->>'held_after')::float8 < 1e-6)) AS closed
               FROM events WHERE at > now() - make_interval(days => $1) AND wallet IS NOT NULL
               GROUP BY 1, 2)
             SELECT wallet, count(*), sum(cost), sum(back - cost) FROM k WHERE closed AND cost > 0 GROUP BY 1",
            &[&PAPER_DAYS],
        )
        .await?;
    let paper: HashMap<String, (i64, f64, f64)> = rows.iter().map(|r| (r.get(0), (r.get(1), r.get(2), r.get(3)))).collect();
    let mut ranked: Vec<(String, f64, f64, f64, i64, f64, f64)> = prior
        .iter()
        .filter(|(_, p)| p.style == "directional" && p.n >= MIN_N && p.roi > MIN_ROI)
        .map(|(w, p)| {
            let (pn, pc, pp) = paper.get(w).copied().unwrap_or((0, 0.0, 0.0));
            let score = (p.roi * p.n + pp) / (p.n + pc + SHRINK);
            (w.clone(), score, p.n, p.roi, pn, pc, pp)
        })
        .filter(|r| r.1 > 0.0)
        .collect();
    ranked.sort_by(|a, b| b.1.total_cmp(&a.1));
    s.ranked = ranked.iter().enumerate().map(|(i, r)| Ranked { wallet: r.0.clone(), rank: i + 1, score: r.1 }).collect();
    s.weights.clear();
    for b in books {
        let all: Vec<&str> = s.ranked.iter().map(|r| r.wallet.as_str()).collect();
        let w: HashMap<String, f64> = match b.pick {
            Pick::Top(n) => all.iter().take(n).map(|w| (w.to_string(), 1.0)).collect(),
            Pick::RankWeighted(n) => {
                let k = all.len().min(n);
                all.iter().take(k).enumerate()
                    .map(|(i, w)| (w.to_string(), if k > 1 { 2.0 - 1.5 * i as f64 / (k - 1) as f64 } else { 1.0 }))
                    .collect()
            }
            Pick::Random(n) => {
                let mut v = all.clone();
                v.sort_by_key(|w| fnv(&format!("{day}{w}")));
                v.into_iter().take(n).map(|w| (w.to_string(), 1.0)).collect()
            }
            Pick::Eligible => all.iter().map(|w| (w.to_string(), 1.0)).collect(),
        };
        s.weights.insert(b.name.to_string(), w);
    }
    let stmt = c
        .prepare(
            "INSERT INTO alloc_rank (day, wallet, rank, score, prior_n, prior_roi, paper_n, paper_cost, paper_pnl)
             VALUES (to_timestamp($1::float8 * 86400)::date, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT DO NOTHING",
        )
        .await?;
    for (i, r) in ranked.iter().enumerate() {
        c.execute(&stmt, &[&(day as f64), &r.0, &(i as i32 + 1), &r.1, &r.2, &r.3, &r.4, &r.5, &r.6]).await?;
    }
    s.day = day;
    let top: Vec<String> = ranked.iter().take(5).map(|r| format!("{} {:+.1}%", &r.0[..10], r.1 * 100.0)).collect();
    log!("alloc: ranked {} eligible ({} with paper lots), top: {}", ranked.len(),
        ranked.iter().filter(|r| r.4 > 0).count(), top.join(", "));
    Ok(())
}

/// A stable hash (the day's random draw must not change on a restart).
fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf29ce484222325u64, |h, b| (h ^ b as u64).wrapping_mul(0x100000001b3))
}

/// Each book's result so far: its copies, what they cost, what the closed ones made, and why
/// copies were refused.
pub async fn report(url: &str) -> Result<()> {
    let c = db::connect(url).await?;
    let rows = c
        .query(
            "WITH g AS (SELECT book, wallet, token, sum(granted) AS granted, count(*) AS n FROM alloc
                        WHERE granted > 0 GROUP BY 1, 2, 3),
                  k AS (SELECT e.wallet, e.token,
                          sum(CASE WHEN kind = 'buy' THEN coalesce((data->'fill'->>'usd')::float8, 0) + coalesce((data->'fill'->>'fee')::float8, 0) ELSE 0 END) AS cost,
                          sum(CASE WHEN kind = 'sell' THEN coalesce((data->'fill'->>'usd')::float8, 0) - coalesce((data->'fill'->>'fee')::float8, 0)
                                   WHEN kind = 'settle' THEN (data->>'usd')::float8 ELSE 0 END) AS back,
                          bool_or(kind = 'settle' OR (kind = 'sell' AND (data->>'held_after')::float8 < 1e-6)) AS closed
                        FROM events e JOIN (SELECT DISTINCT wallet, token FROM g) t USING (wallet, token)
                        GROUP BY 1, 2)
             SELECT g.book, sum(g.n)::bigint, sum(g.granted), count(*) FILTER (WHERE k.closed)::bigint,
                    sum(g.granted) FILTER (WHERE k.closed),
                    sum(g.granted / nullif(k.cost, 0) * (k.back - k.cost)) FILTER (WHERE k.closed),
                    count(DISTINCT g.wallet)::bigint
             FROM g JOIN k USING (wallet, token) GROUP BY 1 ORDER BY 1",
            &[],
        )
        .await?;
    println!("{:9} {:>7} {:>9} {:>7} {:>9} {:>9} {:>8} {:>8}", "book", "copies", "$ in", "closed", "$ closed", "pnl", "ROI", "wallets");
    for r in &rows {
        let (b, n, g, nc, gc, p, w): (String, i64, f64, i64, Option<f64>, Option<f64>, i64) =
            (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6));
        let (gc, p) = (gc.unwrap_or(0.0), p.unwrap_or(0.0));
        println!("{b:9} {n:7} {g:9.2} {nc:7} {gc:9.2} {p:+9.2} {:+7.2}% {w:8}", if gc > 0.0 { p / gc * 100.0 } else { 0.0 });
    }
    println!("\nrefused:");
    for r in c.query("SELECT book, reason, count(*) FROM alloc WHERE granted = 0 GROUP BY 1, 2 ORDER BY 1, 3 DESC", &[]).await? {
        let (b, why, n): (String, String, i64) = (r.get(0), r.get(1), r.get(2));
        println!("  {b:9} {why:13} {n}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_and_duplicates() {
        let b = Book { name: "T", pick: Pick::Top(2), total: 4.0, wallet: 2.0, event: 3.0 };
        let mut s = State::default();
        s.weights.insert("T".into(), [("a".to_string(), 1.0), ("b".to_string(), 1.0)].into());
        let r = |s: &mut State, w: &str, t: &str, e: &str| decide(&b, s, w, t, e, 1.0).reason;
        assert_eq!(r(&mut s, "x", "t1", "e1"), "NOT_SELECTED");
        assert_eq!(r(&mut s, "a", "t1", "e1"), "GRANTED");
        assert_eq!(r(&mut s, "b", "t1", "e1"), "DUPLICATE");
        assert_eq!(r(&mut s, "a", "t1", "e1"), "GRANTED"); // the same wallet adds to its lot
        assert_eq!(r(&mut s, "a", "t2", "e2"), "WALLET_CAP");
        assert_eq!(r(&mut s, "b", "t3", "e1"), "GRANTED");
        assert_eq!(r(&mut s, "b", "t4", "e1"), "EVENT_CAP");
        assert_eq!(r(&mut s, "b", "t4", "e2"), "GRANTED");
        assert_eq!(r(&mut s, "b", "t5", "e3"), "TOTAL_CAP");
    }
}
