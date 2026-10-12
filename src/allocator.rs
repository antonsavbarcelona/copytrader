//! The allocator: which of the wallets copied on paper a portfolio would copy.
//!
//! The paper copies every wallet followed at $1 (engine.rs). On top of it, virtual portfolios
//! ("books") each take only some of those copies: the wallets their score ranks best, the
//! bets their filter lets through, inside their caps. A book's copy fills as the paper's did
//! (at $1 the size does not move the price), so its result is the paper lots it took. Nothing
//! here trades: the books run side by side so the paper shows which score picks best.
//!
//! Each day every wallet's record is the 90-day backtest (the prior, a file of per wallet
//! sums) pooled with the paper's lots closed in the last 30 days. Nothing is filtered for all:
//! each pick has its own gate, then ranks who passes it by its score and takes the top N.
//!
//! A gate is a minimum record (copies or days with a $100+ bet), and one rule chose each
//! score's (gate_pick.py, on the walk-forward of nine gates, alloc_gates.py): the gate whose
//! worst result (lags 7 and 21 days, first and second half, top 10 and 30) is best; within 1
//! point of it, the widest. A score that a short lucky record tops (a ratio: EXPDD, CALMAR,
//! TSTAT, SHRUNK) gets a long one, 30+ days of the backtest's 90; PF 45+ copies; PNLSQRTN,
//! which grows with the copies, 15+ days; PNL none (a big profit is a long record already).
//! EXPDD_ANY_30 is EXPDD with no gate: the gate's control. The scores (higher is better):
//!
//!     SHRUNK    pnl / (cost + 50)                 ROI, a short record pulled to 0
//!     EXPDD     (pnl / n) / max(drawdown, 5)      expectancy over drawdown
//!     CALMAR    pnl / max(drawdown, 5)
//!     PNL       pnl
//!     PNLSQRTN  (pnl / n) * sqrt(n)               expectancy times the square root of copies
//!     PF        (wins + 5) / (losses + 5)         profit factor
//!     TSTAT     (pnl / n) / standard error
//!     RANDOM       a new draw each day of every wallet followed: the selection's control
//!     RANDOM_ELIG  a new draw each day of those traded long: the score's control
//!     ELIGIBLE     every wallet traded long: the gate's control
//!
//! A book is a score, how many wallets (10, 30, 50) and which of their bets: all, MID (bought
//! at 0.20-0.50), NO95 (below 0.95) or DOG (below 0.50); the backtest's walk-forward found the
//! edge in the cheaper outcomes. Each such pick runs in three modes of risk, each a book with
//! its own $1000:
//!
//!     /W4    at most 4 copies of one wallet open; the ceiling is the most copies the book may
//!            hold open (the backtest's peak at that cap, plus 30%), a copy 1000 / ceiling dollars,
//!            so it never needs more than the deposit; one event 3 open, and an outcome another
//!            wallet already brought in is not bought again
//!     /W2    the same with 2 copies of one wallet: a bigger stake on fewer copies
//!     /1USD  $1 a copy, no caps: the pick alone, as the paper copies
//!
//! A wallet that leaves the picks gets no new copies, but its lots go on to its sale or the
//! settlement. The paper copies at $1; a book's dollars are its share of those lots times its
//! stake. Each copy a book wanted gets a row in `alloc` (granted or which cap refused it);
//! `alloc_rank` keeps each day's records and ranks. The state is saved every 5 minutes; after
//! a restart the events since are read again and their decisions, already written, kept.

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

/// The rules the picks are made by: when they change, the day's picks are made again.
const RULES: &str = "a gate for each pick";
const PAPER_DAYS: i32 = 30;
const EVENT_CAP: f64 = 3.0;
const DEPOSIT: f64 = 1000.0;
const SAVE_EVERY_S: f64 = 300.0;

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

/// A pick's gate: who it may rank, by copies, days with a $100+ bet (of the backtest's 90)
/// and pooled ROI.
#[derive(Clone, Copy, PartialEq, Debug)]
struct Gate {
    n: f64,
    days: f64,
    roi: f64,
}

impl Gate {
    const ANY: Gate = Gate { n: 0.0, days: 0.0, roi: f64::NEG_INFINITY };
    /// Traded long: $100+ bets on 30+ days of the 90.
    const LONG: Gate = Gate { days: 30.0, ..Gate::ANY };

    fn passes(&self, r: &Record) -> bool {
        r.n >= self.n && r.days >= self.days && r.roi() > self.roi
    }
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
    /// The pick and mode, as `alloc` names it: PF_30/W4.
    name: String,
    /// The pick (score, wallets, bets) the modes share.
    pick: String,
    score: Score,
    gate: Gate,
    n: usize,
    bets: Bets,
    /// Copies open at most: the book's, one wallet's, one event's.
    total: f64,
    wallet: f64,
    event: f64,
}

impl Book {
    /// Dollars a copy: the deposit over the ceiling; $1 with no ceiling.
    fn stake(&self) -> f64 {
        if self.total.is_finite() { DEPOSIT / self.total } else { 1.0 }
    }

    fn capped(&self) -> bool {
        self.total.is_finite()
    }
}

fn books() -> Vec<Book> {
    use Bets::*;
    use Score::*;
    let (any, long) = (Gate::ANY, Gate::LONG);
    let (n45, d15) = (Gate { n: 45.0, ..any }, Gate { days: 15.0, ..any });
    // pick, score, gate, wallets, bets, the ceilings with 2 and 4 copies of a wallet
    // (alloc_risk.py: the peak of copies open, lags 7 and 21 days, + 30%)
    let list: &[(&str, Score, Gate, usize, Bets, f64, f64)] = &[
        ("EXPDD_10", ExpDd, long, 10, All, 50.0, 100.0), ("EXPDD_30", ExpDd, long, 30, All, 110.0, 190.0),
        ("EXPDD_50", ExpDd, long, 50, All, 150.0, 260.0),
        ("EXPDD_ANY_30", ExpDd, any, 30, All, 110.0, 190.0),
        ("CALMAR_10", Calmar, long, 10, All, 40.0, 70.0), ("CALMAR_30", Calmar, long, 30, All, 90.0, 160.0),
        ("PNL_10", Pnl, any, 10, All, 40.0, 80.0), ("PNL_30", Pnl, any, 30, All, 110.0, 200.0),
        ("PNLSQRTN_10", PnlSqrtN, d15, 10, All, 40.0, 70.0), ("PNLSQRTN_30", PnlSqrtN, d15, 30, All, 110.0, 200.0),
        ("PF_30", Pf, n45, 30, All, 120.0, 220.0), ("PF_50", Pf, n45, 50, All, 180.0, 310.0),
        ("TSTAT_30", TStat, long, 30, All, 130.0, 250.0),
        ("SHRUNK_30", Shrunk, long, 30, All, 110.0, 190.0),
        ("PF_MID_30", Pf, n45, 30, Mid, 80.0, 120.0), ("CALMAR_MID_30", Calmar, long, 30, Mid, 60.0, 90.0),
        ("PF_NO95_30", Pf, n45, 30, No95, 120.0, 220.0), ("CALMAR_NO95_10", Calmar, long, 10, No95, 40.0, 70.0),
        ("CALMAR_NO95_30", Calmar, long, 30, No95, 90.0, 160.0),
        ("PF_DOG_30", Pf, n45, 30, Dog, 90.0, 150.0),
        ("RANDOM_10", Random, any, 10, All, 90.0, 140.0), ("RANDOM_30", Random, any, 30, All, 240.0, 350.0),
        ("RANDOM_50", Random, any, 50, All, 360.0, 510.0),
        ("RANDOM_ELIG_30", Random, long, 30, All, 240.0, 420.0),
        ("ELIGIBLE", Eligible, long, usize::MAX, All, 720.0, 1240.0),
    ];
    let inf = f64::INFINITY;
    let mut out = Vec::new();
    for &(pick, score, gate, n, bets, c2, c4) in list {
        for (mode, total, wallet, event) in [("W4", c4, 4.0, EVENT_CAP), ("W2", c2, 2.0, EVENT_CAP), ("1USD", inf, inf, inf)] {
            out.push(Book { name: format!("{pick}/{mode}"), pick: pick.into(), score, gate, n, bets, total, wallet, event });
        }
    }
    out
}

/// A record of copies: how many, their cost and result, the winners' and losers' sums, the
/// sum of squared returns, the worst drawdown of the running result, and on how many days.
#[derive(Clone, Copy, Default, Serialize, Deserialize, Debug)]
pub struct Record {
    n: f64,
    cost: f64,
    pnl: f64,
    win: f64,
    loss: f64,
    r2: f64,
    dd: f64,
    #[serde(default)]
    days: f64,
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

/// The backtest's per wallet sums (CSV: wallet,n,cost,pnl,win,loss,r2,dd,days,...).
pub fn read_prior(path: &Path) -> Result<HashMap<String, Record>> {
    let text = std::fs::read_to_string(path)?;
    let mut lines = text.lines();
    let head: Vec<&str> = lines.next().unwrap_or_default().split(',').collect();
    let col = |k: &str| head.iter().position(|h| h.trim() == k).ok_or_else(|| anyhow::anyhow!("no column {k}"));
    let (w, n, cost, pnl, win, loss, r2, dd, days) = (col("wallet")?, col("n")?, col("cost")?, col("pnl")?, col("win")?,
        col("loss")?, col("r2")?, col("dd")?, col("days")?);
    let mut out = HashMap::new();
    for l in lines {
        let f: Vec<&str> = l.split(',').collect();
        let v = |i: usize| f.get(i).and_then(|x| x.trim().parse::<f64>().ok());
        if let (Some(n), Some(cost), Some(pnl), Some(win), Some(loss), Some(r2), Some(dd), Some(days)) =
            (v(n), v(cost), v(pnl), v(win), v(loss), v(r2), v(dd), v(days))
        {
            out.insert(f[w].trim().to_lowercase(), Record { n, cost, pnl, win, loss, r2, dd, days });
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

/// An open lot a book took: the wallet's outcome, its event and the copies still in it.
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
    /// The day (unix days) the picks are of, and the rules they were made by.
    day: i64,
    #[serde(default)]
    rules: String,
    /// When the state was last saved (not saved itself).
    #[serde(skip)]
    saved: f64,
    /// Per book: the wallets it copies.
    picks: HashMap<String, HashMap<String, Pick>>,
    /// Per book: its open lots.
    lots: HashMap<String, Vec<Lot>>,
}

/// The allocator's loop: picks once a day, reads the paper's new events every 15 s.
pub async fn run(url: String, api: Api, prior: HashMap<String, Record>, everyone: Vec<String>) {
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
                if let Err(e) = step(c, &api, &prior, &everyone, &books, s).await {
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

async fn save(c: &Client, s: &mut State) -> Result<()> {
    s.saved = now();
    c.execute(
        "INSERT INTO state (id, saved_at, data) VALUES ('alloc', now(), $1)
         ON CONFLICT (id) DO UPDATE SET saved_at = now(), data = EXCLUDED.data",
        &[&serde_json::to_value(&*s)?],
    )
    .await?;
    Ok(())
}

struct Decision {
    granted: f64,
    reason: &'static str,
}

/// A decision as `alloc` keeps it: book, paper event, wallet, token, event, pick, price, cost.
type Decided = (String, i64, String, String, String, Pick, f64, f64, Decision);

async fn step(c: &Client, api: &Api, prior: &HashMap<String, Record>, everyone: &[String], books: &[Book], s: &mut State)
    -> Result<()> {
    let day = (now() / 86400.0) as i64;
    if s.day != day || s.rules != RULES {
        pick(c, prior, everyone, books, s, day).await?;
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
    let mut decided: Vec<Decided> = Vec::new();
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
                    let Some(p) = s.picks.get(&b.pick).and_then(|m| m.get(&wallet)).cloned() else { continue };
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
        // One statement for all: the database is a round trip away.
        let col = |f: &dyn Fn(&Decided) -> String| decided.iter().map(f).collect::<Vec<String>>();
        let num = |f: &dyn Fn(&Decided) -> f64| decided.iter().map(f).collect::<Vec<f64>>();
        let (books, wallets, tokens, events, reasons) =
            (col(&|x| x.0.clone()), col(&|x| x.2.clone()), col(&|x| x.3.clone()), col(&|x| x.4.clone()), col(&|x| x.8.reason.into()));
        let ids: Vec<i64> = decided.iter().map(|x| x.1).collect();
        let ranks: Vec<i32> = decided.iter().map(|x| x.5.rank as i32).collect();
        let (scores, prices, costs, granted) = (num(&|x| x.5.score), num(&|x| x.6), num(&|x| x.7), num(&|x| x.8.granted));
        c.execute(
            "INSERT INTO alloc (at, book, event_id, wallet, token, event, rank, score, price, requested, granted, reason)
             SELECT now(), * FROM unnest($1::text[], $2::int8[], $3::text[], $4::text[], $5::text[], $6::int4[], $7::float8[],
                                         $8::float8[], $9::float8[], $10::float8[], $11::text[])
             ON CONFLICT (book, event_id) DO NOTHING",
            &[&books, &ids, &wallets, &tokens, &events, &ranks, &scores, &prices, &costs, &granted, &reasons],
        )
        .await?;
    }
    if now() - s.saved >= SAVE_EVERY_S {
        save(c, s).await?;
    }
    Ok(())
}

fn decide(b: &Book, s: &mut State, wallet: &str, token: &str, event: &str, cost: f64) -> Decision {
    let no = |reason| Decision { granted: 0.0, reason };
    if !b.capped() {
        // No caps: nothing to keep.
        return Decision { granted: cost, reason: "GRANTED" };
    }
    let lots = s.lots.entry(b.name.clone()).or_default();
    if lots.iter().any(|l| l.token == token && l.wallet != wallet) {
        return no("DUPLICATE");
    }
    // Caps count copies open (a lot partly sold counts its share).
    let sum = |f: &dyn Fn(&Lot) -> bool| lots.iter().filter(|l| f(l)).map(|l| l.open).sum::<f64>();
    if sum(&|_| true) + 1.0 > b.total + 1e-9 {
        return no("TOTAL_CAP");
    }
    if sum(&|l| l.wallet == wallet) + 1.0 > b.wallet + 1e-9 {
        return no("WALLET_CAP");
    }
    if sum(&|l| l.event == event) + 1.0 > b.event + 1e-9 {
        return no("EVENT_CAP");
    }
    match lots.iter_mut().find(|l| l.wallet == wallet && l.token == token) {
        Some(l) => l.open += 1.0,
        None => lots.push(Lot { wallet: wallet.into(), token: token.into(), event: event.into(), open: 1.0 }),
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
            let rec = Record { n: r.get(1), cost: r.get(2), pnl, win: r.get(4), loss: r.get(5), r2: r.get(6), dd: (-pnl).max(0.0),
                days: 0.0 };
            (r.get(0), rec)
        })
        .collect())
}

/// The day's picks of every book: the backtest pooled with the paper's closed lots, every
/// wallet followed; each pick ranks those its gate lets through.
async fn pick(c: &Client, prior: &HashMap<String, Record>, everyone: &[String], books: &[Book], s: &mut State, day: i64)
    -> Result<()> {
    let paper = paper_records(c).await?;
    let all: HashSet<&String> = prior.keys().chain(everyone).collect();
    let mut pooled: Vec<(String, Record, Option<Record>)> = all
        .into_iter()
        .map(|w| {
            let q = paper.get(w).copied();
            let mut r = prior.get(w).copied().unwrap_or_default();
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
        .collect();
    pooled.sort_by(|a, b| a.0.cmp(&b.0));
    let mut ranks: HashMap<String, serde_json::Map<String, Value>> = HashMap::new();
    let mut gated = Vec::new();
    s.picks.clear();
    for b in books {
        if s.picks.contains_key(&b.pick) {
            continue;
        }
        let mut order: Vec<(&str, f64)> =
            pooled.iter().filter(|(_, r, _)| b.gate.passes(r)).map(|(w, r, _)| (w.as_str(), r.score(b.score))).collect();
        gated.push(format!("{} {}", b.pick, order.len()));
        match b.score {
            Score::Random => order.sort_by_key(|(w, _)| fnv(&format!("{day}{}{w}", b.pick))),
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
                ranks.entry(w.clone()).or_default().insert(b.pick.clone(), json!(p.rank));
            }
        }
        s.picks.insert(b.pick.clone(), m);
    }
    // The wallets some pick ranked (ELIGIBLE's are not ranked).
    let as_json = |r: Option<&Record>| r.map(|r| json!(r)).unwrap_or(Value::Null);
    let rows: Vec<&String> = ranks.keys().collect();
    let wallets: Vec<String> = rows.iter().map(|w| w.to_string()).collect();
    let priors: Vec<Value> = rows.iter().map(|w| as_json(prior.get(*w))).collect();
    let papers: Vec<Value> = rows.iter().map(|w| as_json(paper.get(*w))).collect();
    let rks: Vec<Value> = rows.iter().map(|w| Value::Object(ranks.get(*w).cloned().unwrap_or_default())).collect();
    c.execute(
        "INSERT INTO alloc_rank (day, wallet, prior, paper, ranks)
         SELECT to_timestamp($1::float8 * 86400)::date, * FROM unnest($2::text[], $3::jsonb[], $4::jsonb[], $5::jsonb[])
         ON CONFLICT (day, wallet) DO UPDATE SET prior = EXCLUDED.prior, paper = EXCLUDED.paper, ranks = EXCLUDED.ranks",
        &[&(day as f64), &wallets, &priors, &papers, &rks],
    )
    .await?;
    s.day = day;
    s.rules = RULES.to_string();
    log!("alloc: {} wallets ({} with paper lots), {} picks for {} books; through each gate: {}", pooled.len(),
        pooled.iter().filter(|x| x.2.is_some()).count(), s.picks.len(), books.len(), gated.join(", "));
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
    let stakes: HashMap<String, f64> = books().iter().map(|b| (b.name.clone(), b.stake())).collect();
    println!("Each book: $1000; W4/W2 a copy = 1000 / its ceiling, 1USD $1 a copy; the result of closed lots.");
    println!("{:20} {:>6} {:>7} {:>7} {:>9} {:>8} {:>9} {:>8} {:>7} {:>9}", "book", "$/copy", "copies", "closed", "$ closed", "pnl $",
        "balance", "ROI", "wallets", "pnl at $1");
    for r in &rows {
        let (b, n, nc, gc, p, w): (String, i64, i64, Option<f64>, Option<f64>, i64) =
            (r.get(0), r.get(1), r.get(3), r.get(4), r.get(5), r.get(6));
        let Some(&k) = stakes.get(&b) else { continue };
        let one = p.unwrap_or(0.0);
        let (gc, p) = (gc.unwrap_or(0.0) * k, one * k);
        println!("{b:20} {k:6.2} {n:7} {nc:7} {gc:9.2} {p:+8.2} {:9.2} {:+7.2}% {w:7} {one:+9.2}", DEPOSIT + p,
            if gc > 0.0 { p / gc * 100.0 } else { 0.0 });
    }
    println!("\nrefused:");
    for r in c.query("SELECT book, reason, count(*) FROM alloc WHERE granted = 0 GROUP BY 1, 2 ORDER BY 1, 3 DESC", &[]).await? {
        let (b, why, n): (String, String, i64) = (r.get(0), r.get(1), r.get(2));
        println!("  {b:20} {why:13} {n}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn caps_and_duplicates() {
        let b = Book { name: "T/W2".into(), pick: "T".into(), score: Score::Pnl, gate: Gate::ANY, n: 2, bets: Bets::All, total: 4.0,
            wallet: 2.0, event: 3.0 };
        assert_eq!(b.stake(), 250.0);
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
        let r = Record { n: 100.0, cost: 100.0, pnl: 10.0, win: 60.0, loss: 50.0, r2: 100.0, dd: 20.0, days: 40.0 };
        assert!((r.score(Score::Shrunk) - 10.0 / 150.0).abs() < 1e-12);
        assert!((r.score(Score::ExpDd) - 0.1 / 20.0).abs() < 1e-12);
        assert!((r.score(Score::Calmar) - 0.5).abs() < 1e-12);
        assert!((r.score(Score::PnlSqrtN) - 1.0).abs() < 1e-12);
        assert!((r.score(Score::Pf) - 65.0 / 55.0).abs() < 1e-12);
        assert!(r.score(Score::TStat) > 0.0);
        assert!(Bets::Mid.takes(0.3) && !Bets::Mid.takes(0.1) && !Bets::Mid.takes(0.5));
        assert!(Bets::No95.takes(0.9) && !Bets::No95.takes(0.96));
        assert!(Bets::Dog.takes(0.49) && !Bets::Dog.takes(0.5));
        assert_eq!(books().len(), 75);
        assert!(Gate::LONG.passes(&r) && !Gate::LONG.passes(&Record { days: 29.0, ..r }));
        assert!(Gate::ANY.passes(&Record::default()));
        let free = books().into_iter().find(|b| b.name == "PF_30/1USD").unwrap();
        assert_eq!(free.stake(), 1.0);
        let mut s = State::default();
        for i in 0..1000 {
            assert_eq!(decide(&free, &mut s, "a", &format!("t{i}"), "e", 1.0).reason, "GRANTED");
        }
        assert!(s.lots.is_empty());
    }
}
