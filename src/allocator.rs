//! The allocator: which of the wallets copied on paper a portfolio would copy.
//!
//! The paper copies every wallet followed at $1 (engine.rs). On top of it, virtual portfolios
//! ("books") each take only some of those copies: the wallets their score ranks best, the
//! bets their filter lets through, inside their caps. A book's copy fills as the paper's did
//! (at $1 the size does not move the price), so its result is the paper lots it took. Nothing
//! here trades: the books run side by side so the paper shows which score picks best.
//!
//! Each day every wallet's record is the 90-day backtest (the prior, a file of per wallet
//! sums) pooled with the paper's lots closed in the last 30 days. Eligible: 100+ backtest
//! copies at ROI > 5% whose pooled ROI stays above 0. The scores (higher is better):
//!
//!     SHRUNK    pnl / (cost + 50)                 ROI, a short record pulled to 0
//!     EXPDD     (pnl / n) / max(drawdown, 5)      expectancy over drawdown
//!     CALMAR    pnl / max(drawdown, 5)
//!     PNL       pnl
//!     PNLSQRTN  (pnl / n) * sqrt(n)               expectancy times the square root of copies
//!     PF        (wins + 5) / (losses + 5)         profit factor
//!     TSTAT     (pnl / n) / standard error
//!     RANDOM    a new draw of the eligible each day: the score's control
//!     ELIGIBLE  every eligible wallet, no caps: the filter's control
//!
//! A book is a score, how many wallets (10, 30, 50) and which of their bets: all, MID (bought
//! at 0.20-0.50), NO95 (below 0.95) or DOG (below 0.50); the backtest's walk-forward found the
//! edge in the cheaper outcomes. Caps (a copy is $1): the book's open cost 6 x its wallets,
//! one wallet 6, one event 3 (a game's winner, spread and total are one bet), and an outcome
//! another wallet already brought in is not bought again. A wallet that leaves the picks gets
//! no new copies, but its lots go on to its sale or the settlement. Each copy a book wanted
//! gets a row in `alloc` (granted or which cap refused it); `alloc_rank` keeps each day's
//! records and ranks.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio_postgres::Client;

use crate::api::{Api, now};
use crate::db;
use crate::log;

const MIN_N: f64 = 100.0;
const MIN_ROI: f64 = 0.05;
const PAPER_DAYS: i32 = 30;
const WALLET_CAP: f64 = 6.0;
const EVENT_CAP: f64 = 3.0;
const BOOK_CAP_PER_WALLET: f64 = 6.0;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Score {
    Shrunk,
    ExpDd,
    Calmar,
    Pnl,
    PnlSqrtN,
    Pf,
    TStat,
    Random,
    Eligible,
}

#[derive(Clone, Copy, PartialEq, Debug)]
enum Bets {
    All,
    Mid,
    No95,
    Dog,
}

impl Bets {
    fn takes(self, p: f64) -> bool {
        match self {
            Bets::All => true,
            Bets::Mid => (0.2..0.5).contains(&p),
            Bets::No95 => p < 0.95,
            Bets::Dog => p < 0.5,
        }
    }
}

struct Book {
    name: String,
    score: Score,
    n: usize,
    bets: Bets,
    total: f64,
    wallet: f64,
    event: f64,
}

fn books() -> Vec<Book> {
    use Bets::*;
    use Score::*;
    let list: &[(&str, Score, usize, Bets)] = &[
        ("EXPDD", ExpDd, 10, All), ("EXPDD", ExpDd, 30, All), ("EXPDD", ExpDd, 50, All),
        ("CALMAR", Calmar, 10, All), ("CALMAR", Calmar, 30, All),
        ("PNL", Pnl, 10, All), ("PNL", Pnl, 30, All),
        ("PNLSQRTN", PnlSqrtN, 10, All), ("PNLSQRTN", PnlSqrtN, 30, All),
        ("PF", Pf, 30, All), ("PF", Pf, 50, All),
        ("TSTAT", TStat, 30, All),
        ("SHRUNK", Shrunk, 30, All),
        ("PF_MID", Pf, 30, Mid), ("CALMAR_MID", Calmar, 30, Mid),
        ("PF_NO95", Pf, 30, No95), ("CALMAR_NO95", Calmar, 10, No95), ("CALMAR_NO95", Calmar, 30, No95),
        ("PF_DOG", Pf, 30, Dog),
        ("RANDOM", Random, 10, All), ("RANDOM", Random, 30, All), ("RANDOM", Random, 50, All),
    ];
    let mut out: Vec<Book> = list
        .iter()
        .map(|&(name, score, n, bets)| Book {
            name: format!("{name}_{n}"),
            score,
            n,
            bets,
            total: BOOK_CAP_PER_WALLET * n as f64,
            wallet: WALLET_CAP,
            event: EVENT_CAP,
        })
        .collect();
    out.push(Book {
        name: "ELIGIBLE".into(),
        score: Eligible,
        n: usize::MAX,
        bets: All,
        total: f64::INFINITY,
        wallet: f64::INFINITY,
        event: f64::INFINITY,
    });
    out
}

/// A record of copies: how many, their cost and result, the winners' and losers' sums, the
/// sum of squared returns and the worst drawdown of the running result.
#[derive(Clone, Copy, Default, Serialize, Deserialize, Debug)]
pub struct Record {
    n: f64,
    cost: f64,
    pnl: f64,
    win: f64,
    loss: f64,
    r2: f64,
    dd: f64,
}

impl Record {
    fn roi(&self) -> f64 {
        if self.cost > 0.0 { self.pnl / self.cost } else { 0.0 }
    }

    fn score(&self, s: Score) -> f64 {
        let n = self.n.max(1.0);
        let mean = self.pnl / n;
        let dd = self.dd.max(5.0);
        match s {
            Score::Shrunk => self.pnl / (self.cost + 50.0),
            Score::ExpDd => mean / dd,
            Score::Calmar => self.pnl / dd,
            Score::Pnl => self.pnl,
            Score::PnlSqrtN => mean * n.sqrt(),
            Score::Pf => (self.win + 5.0) / (self.loss + 5.0),
            Score::TStat => mean / ((self.r2 / n - mean * mean).max(1e-6).sqrt() / n.sqrt()),
            Score::Random | Score::Eligible => 0.0,
        }
    }
}

/// The backtest's per wallet sums (CSV: wallet,n,cost,pnl,win,loss,r2,dd,...).
pub fn read_prior(path: &Path) -> Result<HashMap<String, Record>> {
    let text = std::fs::read_to_string(path)?;
    let mut lines = text.lines();
    let head: Vec<&str> = lines.next().unwrap_or_default().split(',').collect();
    let col = |k: &str| head.iter().position(|h| h.trim() == k).ok_or_else(|| anyhow::anyhow!("no column {k}"));
    let (w, n, cost, pnl, win, loss, r2, dd) =
        (col("wallet")?, col("n")?, col("cost")?, col("pnl")?, col("win")?, col("loss")?, col("r2")?, col("dd")?);
    let mut out = HashMap::new();
    for l in lines {
        let f: Vec<&str> = l.split(',').collect();
        let v = |i: usize| f.get(i).and_then(|x| x.trim().parse::<f64>().ok());
        if let (Some(n), Some(cost), Some(pnl), Some(win), Some(loss), Some(r2), Some(dd)) =
            (v(n), v(cost), v(pnl), v(win), v(loss), v(r2), v(dd))
        {
            out.insert(f[w].trim().to_lowercase(), Record { n, cost, pnl, win, loss, r2, dd });
        }
    }
    Ok(out)
}

/// A wallet a book picked today: its place and score.
#[derive(Clone, Serialize, Deserialize)]
struct Pick {
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
    /// The day (unix days) the picks are of.
    day: i64,
    /// Per book: the wallets it copies.
    picks: HashMap<String, HashMap<String, Pick>>,
    /// Per book: its open lots.
    lots: HashMap<String, Vec<Lot>>,
}

/// The allocator's loop: picks once a day, reads the paper's new events every 15 s.
pub async fn run(url: String, api: Api, prior: HashMap<String, Record>) {
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
    match c.query_opt("SELECT data FROM state WHERE id = 'alloc'", &[]).await? {
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

struct Decision {
    granted: f64,
    reason: &'static str,
}

async fn step(c: &Client, api: &Api, prior: &HashMap<String, Record>, books: &[Book], s: &mut State) -> Result<()> {
    let day = (now() / 86400.0) as i64;
    if s.day != day {
        pick(c, prior, books, s, day).await?;
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
    let wanted: HashSet<String> = s.picks.values().flat_map(|m| m.keys().cloned()).collect();
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
                if cost <= 0.0 || !wanted.contains(&wallet) {
                    continue;
                }
                let price = data["wallet_price"].as_f64().unwrap_or(0.0);
                let event = match api.market(&token, 7.0 * 86400.0).await {
                    Ok(m) => m.event.unwrap_or(m.condition),
                    Err(_) => data["condition"].as_str().unwrap_or(&token).to_string(),
                };
                for b in books {
                    if !b.bets.takes(price) {
                        continue;
                    }
                    let Some(p) = s.picks.get(&b.name).and_then(|m| m.get(&wallet)).cloned() else { continue };
                    let d = decide(b, s, &wallet, &token, &event, cost);
                    decided.push((b.name.clone(), id, wallet.clone(), token.clone(), event.clone(), p, price, cost, d));
                }
            }
            "sell" => {
                // What the paper still holds after the sale, as a share of before.
                let (before, after) =
                    (data["held_before"].as_f64().unwrap_or(0.0), data["held_after"].as_f64().unwrap_or(0.0));
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
        let stmt = c
            .prepare(
                "INSERT INTO alloc (at, book, event_id, wallet, token, event, rank, score, price, requested, granted, reason)
                 VALUES (now(), $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
            )
            .await?;
        for (b, id, w, t, e, p, price, cost, d) in &decided {
            c.execute(&stmt, &[b, id, w, t, e, &(p.rank as i32), &p.score, price, cost, &d.granted, &d.reason]).await?;
        }
    }
    save(c, s).await
}

fn decide(b: &Book, s: &mut State, wallet: &str, token: &str, event: &str, cost: f64) -> Decision {
    let no = |reason| Decision { granted: 0.0, reason };
    let lots = s.lots.entry(b.name.clone()).or_default();
    if lots.iter().any(|l| l.token == token && l.wallet != wallet) {
        return no("DUPLICATE");
    }
    let sum = |f: &dyn Fn(&Lot) -> bool| lots.iter().filter(|l| f(l)).map(|l| l.open).sum::<f64>();
    if sum(&|_| true) + cost > b.total + 1e-9 {
        return no("TOTAL_CAP");
    }
    if sum(&|l| l.wallet == wallet) + cost > b.wallet + 1e-9 {
        return no("WALLET_CAP");
    }
    if sum(&|l| l.event == event) + cost > b.event + 1e-9 {
        return no("EVENT_CAP");
    }
    match lots.iter_mut().find(|l| l.wallet == wallet && l.token == token) {
        Some(l) => l.open += cost,
        None => lots.push(Lot { wallet: wallet.into(), token: token.into(), event: event.into(), open: cost }),
    }
    Decision { granted: cost, reason: "GRANTED" }
}

/// The paper's lots closed in the last 30 days, per wallet, as a record.
async fn paper_records(c: &Client) -> Result<HashMap<String, Record>> {
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
             SELECT wallet, count(*)::float8, sum(cost), sum(back - cost), sum(greatest(back - cost, 0)),
                    sum(greatest(cost - back, 0)), sum(((back - cost) / cost) ^ 2)
             FROM k WHERE closed AND cost > 0 GROUP BY 1",
            &[&PAPER_DAYS],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            let pnl: f64 = r.get(3);
            let rec = Record { n: r.get(1), cost: r.get(2), pnl, win: r.get(4), loss: r.get(5), r2: r.get(6), dd: (-pnl).max(0.0) };
            (r.get(0), rec)
        })
        .collect())
}

/// The day's picks of every book: the backtest pooled with the paper's closed lots.
async fn pick(c: &Client, prior: &HashMap<String, Record>, books: &[Book], s: &mut State, day: i64) -> Result<()> {
    let paper = paper_records(c).await?;
    let mut pooled: Vec<(String, Record, Option<Record>)> = prior
        .iter()
        .filter(|(_, p)| p.n >= MIN_N && p.roi() > MIN_ROI)
        .map(|(w, p)| {
            let q = paper.get(w).copied();
            let mut r = *p;
            if let Some(q) = q {
                r.n += q.n;
                r.cost += q.cost;
                r.pnl += q.pnl;
                r.win += q.win;
                r.loss += q.loss;
                r.r2 += q.r2;
                r.dd = r.dd.max(q.dd);
            }
            (w.clone(), r, q)
        })
        .filter(|(_, r, _)| r.roi() > 0.0)
        .collect();
    pooled.sort_by(|a, b| a.0.cmp(&b.0));
    let mut ranks: HashMap<String, serde_json::Map<String, Value>> = HashMap::new();
    s.picks.clear();
    for b in books {
        let mut order: Vec<(&str, f64)> = pooled.iter().map(|(w, r, _)| (w.as_str(), r.score(b.score))).collect();
        match b.score {
            Score::Random => order.sort_by_key(|(w, _)| fnv(&format!("{day}{}{w}", b.name))),
            Score::Eligible => {}
            _ => order.sort_by(|a, b| b.1.total_cmp(&a.1)),
        }
        let m: HashMap<String, Pick> = order
            .iter()
            .take(b.n)
            .enumerate()
            .map(|(i, (w, sc))| (w.to_string(), Pick { rank: i + 1, score: *sc }))
            .collect();
        if b.score != Score::Eligible {
            for (w, p) in &m {
                ranks.entry(w.clone()).or_default().insert(b.name.clone(), json!(p.rank));
            }
        }
        s.picks.insert(b.name.clone(), m);
    }
    let stmt = c
        .prepare(
            "INSERT INTO alloc_rank (day, wallet, prior, paper, ranks)
             VALUES (to_timestamp($1::float8 * 86400)::date, $2, $3, $4, $5) ON CONFLICT DO NOTHING",
        )
        .await?;
    for (w, _, q) in &pooled {
        let p = prior.get(w).map(|p| json!(p)).unwrap_or(Value::Null);
        let q = q.map(|q| json!(q)).unwrap_or(Value::Null);
        let rk = Value::Object(ranks.remove(w).unwrap_or_default());
        c.execute(&stmt, &[&(day as f64), w, &p, &q, &rk]).await?;
    }
    s.day = day;
    log!("alloc: {} eligible ({} with paper lots), {} books picked", pooled.len(),
        pooled.iter().filter(|x| x.2.is_some()).count(), books.len());
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
    println!("{:15} {:>7} {:>9} {:>7} {:>9} {:>9} {:>8} {:>8}", "book", "copies", "$ in", "closed", "$ closed", "pnl", "ROI", "wallets");
    for r in &rows {
        let (b, n, g, nc, gc, p, w): (String, i64, f64, i64, Option<f64>, Option<f64>, i64) =
            (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5), r.get(6));
        let (gc, p) = (gc.unwrap_or(0.0), p.unwrap_or(0.0));
        println!("{b:15} {n:7} {g:9.2} {nc:7} {gc:9.2} {p:+9.2} {:+7.2}% {w:8}", if gc > 0.0 { p / gc * 100.0 } else { 0.0 });
    }
    println!("\nrefused:");
    for r in c.query("SELECT book, reason, count(*) FROM alloc WHERE granted = 0 GROUP BY 1, 2 ORDER BY 1, 3 DESC", &[]).await? {
        let (b, why, n): (String, String, i64) = (r.get(0), r.get(1), r.get(2));
        println!("  {b:15} {why:13} {n}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_and_duplicates() {
        let b = Book { name: "T".into(), score: Score::Pnl, n: 2, bets: Bets::All, total: 4.0, wallet: 2.0, event: 3.0 };
        let mut s = State::default();
        let r = |s: &mut State, w: &str, t: &str, e: &str| decide(&b, s, w, t, e, 1.0).reason;
        assert_eq!(r(&mut s, "a", "t1", "e1"), "GRANTED");
        assert_eq!(r(&mut s, "b", "t1", "e1"), "DUPLICATE");
        assert_eq!(r(&mut s, "a", "t1", "e1"), "GRANTED"); // the same wallet adds to its lot
        assert_eq!(r(&mut s, "a", "t2", "e2"), "WALLET_CAP");
        assert_eq!(r(&mut s, "b", "t3", "e1"), "GRANTED");
        assert_eq!(r(&mut s, "b", "t4", "e1"), "EVENT_CAP");
        assert_eq!(r(&mut s, "b", "t4", "e2"), "GRANTED");
        assert_eq!(r(&mut s, "b", "t5", "e3"), "TOTAL_CAP");
    }

    #[test]
    fn scores_and_bets() {
        let r = Record { n: 100.0, cost: 100.0, pnl: 10.0, win: 60.0, loss: 50.0, r2: 100.0, dd: 20.0 };
        assert!((r.score(Score::Shrunk) - 10.0 / 150.0).abs() < 1e-12);
        assert!((r.score(Score::ExpDd) - 0.1 / 20.0).abs() < 1e-12);
        assert!((r.score(Score::Calmar) - 0.5).abs() < 1e-12);
        assert!((r.score(Score::PnlSqrtN) - 1.0).abs() < 1e-12);
        assert!((r.score(Score::Pf) - 65.0 / 55.0).abs() < 1e-12);
        assert!(r.score(Score::TStat) > 0.0);
        assert!(Bets::Mid.takes(0.3) && !Bets::Mid.takes(0.1) && !Bets::Mid.takes(0.5));
        assert!(Bets::No95.takes(0.9) && !Bets::No95.takes(0.96));
        assert!(Bets::Dog.takes(0.49) && !Bets::Dog.takes(0.5));
        assert_eq!(books().len(), 23);
    }
}
