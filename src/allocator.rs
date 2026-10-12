//! The allocator: which of the wallets copied on paper a portfolio would copy.
//!
//! The paper copies every wallet followed at $1 (engine.rs). On top of it, virtual portfolios
//! ("books") each take only some of those copies: of the wallets their score ranks best, the
//! real bets (orders of $100+, `BIG_ORDER`, as the prior counts them) that their price filter
//! lets through, inside their caps. A book's copy fills as the paper's did
//! (at $1 the size does not move the price), so its result is the paper lots it took. Nothing
//! here trades: the books run side by side so the paper shows which score picks best.
//!
//! Each day every wallet's record is the 90-day backtest (the prior, a file of per wallet
//! sums) pooled with the paper's lots closed in the last 30 days. Nothing is filtered for all:
//! each pick has its own gate, then ranks who passes it by its score and takes the top N.
//!
//! A gate is a minimum record (copies or days with a $100+ bet), and one rule chose each
//! score's (research/gate_pick.py, on the walk-forward of nine gates): the gate whose worst
//! result (lags 7 and 21 days, first and second half, top 10 and 30) is best; within 1 point
//! of it, the widest. EXPDD, TSTAT, PNL and PNLSQRTN rank wallets with $100+ bets on 30+ days
//! of the backtest's 90, PF those with 45+ copies, CALMAR and SHRUNK every wallet.
//! EXPDD_ANY_30 is EXPDD with no gate: the gate's control.
//!
//! Each pick but RANDOM_10/30/50 and EXPDD_ANY_30 also runs as a twin with the strict gate the
//! allocator began with (`_STRICT`: 100+ backtest copies at ROI > 5%, the pooled ROI above 0),
//! so the paper tells the soft gates from the strict. The scores (higher is better):
//!
//!     SHRUNK    pnl / (cost + 50)                 ROI, a short record pulled to 0
//!     EXPDD     (pnl / n) / max(drawdown, 5)      expectancy over drawdown
//!     CALMAR    pnl / max(drawdown, 5)
//!     PNL       pnl
//!     PNLSQRTN  (pnl / n) * sqrt(n)               expectancy times the square root of copies
//!     PF        (wins + 5) / (losses + 5)         profit factor
//!     TSTAT     (pnl / n) / standard error
//!     RANDOM       a new draw each day of every wallet followed: the selection's control
//!     RANDOM_ELIG  a new draw each day of those through the 30-day gate: the score's control
//!     ELIGIBLE     every wallet through the 30-day gate: the gate's control
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
//! stake. Every 6 h each book's balance, its open lots at the market's price, goes to
//! `alloc_snap`; `copytrader alloc` judges the books since their rules began (README).
//! Each copy a book wanted gets a row in `alloc` (granted or which cap refused it);
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
use crate::engine::BIG_ORDER;
use crate::log;

/// The rules the picks are made by: when they change, the day's picks are made again.
const RULES: &str = "a gate for each pick, strict twins; orders of $100+; prior v2";
const PAPER_DAYS: i32 = 30;
const EVENT_CAP: f64 = 3.0;
const DEPOSIT: f64 = 1000.0;
const SAVE_EVERY_S: f64 = 300.0;
const SNAP_EVERY_S: f64 = 6.0 * 3600.0;

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

/// A pick's gate: who it may rank, by the backtest's copies, days with a $100+ bet (of its
/// 90) and ROI, and the ROI pooled with the paper.
#[derive(Clone, Copy, PartialEq, Debug)]
struct Gate {
    n: f64,
    days: f64,
    roi: f64,
    pooled_roi: f64,
}

impl Gate {
    const ANY: Gate = Gate { n: 0.0, days: 0.0, roi: f64::NEG_INFINITY, pooled_roi: f64::NEG_INFINITY };
    /// Traded long: $100+ bets on 30+ days of the 90.
    const LONG: Gate = Gate { days: 30.0, ..Gate::ANY };
    /// The allocator's first gate: a long record that made money.
    const STRICT: Gate = Gate { n: 100.0, roi: 0.05, pooled_roi: 0.0, ..Gate::ANY };

    fn passes(&self, prior: &Record, pooled: &Record) -> bool {
        prior.n >= self.n && prior.days >= self.days && prior.roi() > self.roi && pooled.roi() > self.pooled_roi
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

/// A pick as `books` lists it: name, score, gate, wallets, bets, its ceilings at 2 and 4 copies
/// of a wallet, the strict twin's.
type Spec = (&'static str, Score, Gate, usize, Bets, f64, f64, f64, f64);

fn books() -> Vec<Book> {
    use Bets::*;
    use Score::*;
    let (any, long) = (Gate::ANY, Gate::LONG);
    let n45 = Gate { n: 45.0, ..any };
    // pick, score, gate, wallets, bets; the ceilings with 2 and 4 copies of a wallet open, then
    // the strict twin's (research/alloc_ceil2.py: the most copies open at once in the backtest
    // of lags 7 and 21 days, the worse, + 30%; 0: no twin)
    let list: &[Spec] = &[
        ("EXPDD_10", ExpDd, long, 10, All, 50.0, 90.0, 50.0, 70.0),
        ("EXPDD_30", ExpDd, long, 30, All, 110.0, 180.0, 110.0, 180.0),
        ("EXPDD_50", ExpDd, long, 50, All, 160.0, 260.0, 160.0, 270.0),
        ("EXPDD_ANY_30", ExpDd, any, 30, All, 70.0, 110.0, 0.0, 0.0),
        ("CALMAR_10", Calmar, any, 10, All, 40.0, 50.0, 30.0, 50.0),
        ("CALMAR_30", Calmar, any, 30, All, 90.0, 140.0, 90.0, 150.0),
        ("PNL_10", Pnl, long, 10, All, 40.0, 70.0, 40.0, 70.0),
        ("PNL_30", Pnl, long, 30, All, 90.0, 160.0, 100.0, 170.0),
        ("PNLSQRTN_10", PnlSqrtN, long, 10, All, 50.0, 80.0, 50.0, 80.0),
        ("PNLSQRTN_30", PnlSqrtN, long, 30, All, 100.0, 160.0, 100.0, 160.0),
        ("PF_30", Pf, n45, 30, All, 100.0, 170.0, 120.0, 210.0),
        ("PF_50", Pf, n45, 50, All, 170.0, 290.0, 180.0, 320.0),
        ("TSTAT_30", TStat, long, 30, All, 130.0, 230.0, 110.0, 190.0),
        ("SHRUNK_30", Shrunk, any, 30, All, 90.0, 150.0, 100.0, 150.0),
        ("PF_MID_30", Pf, n45, 30, Mid, 70.0, 100.0, 80.0, 130.0),
        ("CALMAR_MID_30", Calmar, any, 30, Mid, 60.0, 90.0, 70.0, 100.0),
        ("PF_NO95_30", Pf, n45, 30, No95, 100.0, 170.0, 120.0, 200.0),
        ("CALMAR_NO95_10", Calmar, any, 10, No95, 40.0, 50.0, 30.0, 50.0),
        ("CALMAR_NO95_30", Calmar, any, 30, No95, 90.0, 140.0, 90.0, 150.0),
        ("PF_DOG_30", Pf, n45, 30, Dog, 80.0, 120.0, 90.0, 150.0),
        ("RANDOM_10", Random, any, 10, All, 70.0, 110.0, 0.0, 0.0),
        ("RANDOM_30", Random, any, 30, All, 190.0, 260.0, 0.0, 0.0),
        ("RANDOM_50", Random, any, 50, All, 280.0, 380.0, 0.0, 0.0),
        ("RANDOM_ELIG_30", Random, long, 30, All, 260.0, 370.0, 200.0, 330.0),
        ("ELIGIBLE", Eligible, long, usize::MAX, All, 4190.0, 6130.0, 580.0, 960.0),
    ];
    let inf = f64::INFINITY;
    let mut out = Vec::new();
    // The strict twins: the picks with the allocator's first gate.
    let strict = list
        .iter()
        .filter(|x| x.7 > 0.0)
        .map(|&(pick, score, _, n, bets, _, _, s2, s4)| (format!("{pick}_STRICT"), score, Gate::STRICT, n, bets, s2, s4));
    let all = list.iter().map(|&(p, s, g, n, b, c2, c4, _, _)| (p.to_string(), s, g, n, b, c2, c4)).chain(strict);
    for (pick, score, gate, n, bets, c2, c4) in all {
        for (mode, total, wallet, event) in [("W4", c4, 4.0, EVENT_CAP), ("W2", c2, 2.0, EVENT_CAP), ("1USD", inf, inf, inf)] {
            out.push(Book { name: format!("{pick}/{mode}"), pick: pick.clone(), score, gate, n, bets, total, wallet, event });
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
    /// When the rules began (unix seconds): the books are judged on what they did since.
    #[serde(default)]
    since: f64,
    /// When the books' balances were last kept (`alloc_snap`).
    #[serde(default)]
    snapped: f64,
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
        if s.rules != RULES {
            s.since = now();
            s.snapped = 0.0;
        }
        pick(c, prior, everyone, books, s, day).await?;
        save(c, s).await?;
    }
    if now() - s.snapped >= SNAP_EVERY_S {
        snapshot(c, api, books, s.since).await?;
        s.snapped = now();
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
                let price = data["wallet_price"].as_f64().unwrap_or(0.0);
                // The trader's order in dollars (before the engine kept it: its first piece).
                let bet = data["wallet_usd"].as_f64().unwrap_or_else(|| data["wallet_size"].as_f64().unwrap_or(0.0) * price);
                if cost <= 0.0 || bet < BIG_ORDER || !wanted.contains(&wallet) {
                    continue;
                }
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

/// The paper's lots closed in the last 30 days of orders of $100+ (as the prior counts), per
/// wallet, as a record.
async fn paper_records(c: &Client) -> Result<HashMap<String, Record>> {
    let rows = c
        .query(
            "WITH k AS (
               SELECT wallet, token,
                 sum(CASE WHEN kind = 'buy' THEN coalesce((data->'fill'->>'usd')::float8, 0) + coalesce((data->'fill'->>'fee')::float8, 0) ELSE 0 END) AS cost,
                 sum(CASE WHEN kind = 'sell' THEN coalesce((data->'fill'->>'usd')::float8, 0) - coalesce((data->'fill'->>'fee')::float8, 0)
                          WHEN kind = 'settle' THEN (data->>'usd')::float8 ELSE 0 END) AS back,
                 bool_or(kind = 'settle' OR (kind = 'sell' AND (data->>'held_after')::float8 < 1e-6)) AS closed,
                 bool_or(kind = 'buy' AND coalesce((data->>'wallet_usd')::float8,
                         (data->>'wallet_size')::float8 * (data->>'wallet_price')::float8) >= $2) AS big
               FROM events WHERE at > now() - make_interval(days => $1) AND wallet IS NOT NULL
               GROUP BY 1, 2)
             SELECT wallet, count(*)::float8, sum(cost), sum(back - cost), sum(greatest(back - cost, 0)),
                    sum(greatest(cost - back, 0)), sum(((back - cost) / cost) ^ 2)
             FROM k WHERE closed AND big AND cost > 0 GROUP BY 1",
            &[&PAPER_DAYS, &BIG_ORDER],
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
    let none = Record::default();
    s.picks.clear();
    for b in books {
        if s.picks.contains_key(&b.pick) {
            continue;
        }
        let mut order: Vec<(&str, f64)> =
            pooled.iter().filter(|(w, r, _)| b.gate.passes(prior.get(w).unwrap_or(&none), r)).map(|(w, r, _)| (w.as_str(), r.score(b.score))).collect();
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

/// A book's share of one paper lot (wallet, outcome): the copies it took and their dollars
/// (at $1 a copy), the lot's cost, what came back and the shares still open, and when it
/// closed (unix seconds; None while open).
struct Held {
    book: String,
    token: String,
    n: i64,
    granted: f64,
    cost: f64,
    back: f64,
    shares: f64,
    closed: Option<f64>,
}

/// The books' shares of the paper lots, of the copies granted since `since` (unix seconds).
async fn held(c: &Client, since: f64) -> Result<Vec<Held>> {
    let rows = c
        .query(
            "WITH g AS (SELECT book, wallet, token, sum(granted) AS g, count(*) AS n FROM alloc
                        WHERE granted > 0 AND at >= to_timestamp($1) GROUP BY 1, 2, 3),
                  k AS (SELECT e.wallet, e.token,
                          sum(CASE WHEN kind = 'buy' THEN coalesce((data->'fill'->>'usd')::float8, 0) + coalesce((data->'fill'->>'fee')::float8, 0) ELSE 0 END) AS cost,
                          sum(CASE WHEN kind = 'sell' THEN coalesce((data->'fill'->>'usd')::float8, 0) - coalesce((data->'fill'->>'fee')::float8, 0)
                                   WHEN kind = 'settle' THEN (data->>'usd')::float8 ELSE 0 END) AS back,
                          sum(CASE WHEN kind = 'buy' THEN coalesce((data->'fill'->>'shares')::float8, 0)
                                   WHEN kind = 'sell' THEN -coalesce((data->'fill'->>'shares')::float8, 0) ELSE 0 END) AS shares,
                          coalesce(bool_or(kind = 'settle' OR (kind = 'sell' AND (data->>'held_after')::float8 < 1e-6)), false) AS closed,
                          extract(epoch FROM max(e.at) FILTER (WHERE kind IN ('sell', 'settle')))::float8 AS exit
                        FROM events e JOIN (SELECT DISTINCT wallet, token FROM g) t USING (wallet, token)
                        GROUP BY 1, 2)
             SELECT g.book, g.token, g.n, g.g, k.cost, k.back, k.shares, k.closed, k.exit
             FROM g JOIN k USING (wallet, token) WHERE k.cost > 0",
            &[&since],
        )
        .await?;
    Ok(rows
        .iter()
        .map(|r| {
            let closed: bool = r.get(7);
            Held {
                book: r.get(0),
                token: r.get(1),
                n: r.get(2),
                granted: r.get(3),
                cost: r.get(4),
                back: r.get(5),
                shares: if closed { 0.0 } else { r.get::<_, f64>(6).max(0.0) },
                closed: closed.then(|| r.get::<_, Option<f64>>(8).unwrap_or_else(now)),
            }
        })
        .collect())
}

/// A book's sums at $1 a copy: its copies and their cost, the closed ones' result, the open
/// ones' cost and value now; for the closed lots' returns (weighted by the dollars the book
/// put in) the sums a standard error needs; and the closed result in each half of the period.
#[derive(Default, Clone)]
struct Sum {
    copies: i64,
    cost: f64,
    closed_n: i64,
    closed_cost: f64,
    closed_pnl: f64,
    open_cost: f64,
    open_value: f64,
    w: f64,
    wr: f64,
    w2r: f64,
    w2r2: f64,
    w2: f64,
    halves: [(f64, f64); 2],
}

impl Sum {
    fn pnl(&self) -> f64 {
        self.closed_pnl + self.open_value - self.open_cost
    }

    fn roi(&self) -> f64 {
        if self.closed_cost > 0.0 { self.closed_pnl / self.closed_cost } else { 0.0 }
    }

    /// The closed lots' ROI, 95% lower and upper bounds.
    fn bounds(&self) -> (f64, f64) {
        if self.w <= 0.0 {
            return (0.0, 0.0);
        }
        let m = self.wr / self.w;
        let se = ((self.w2r2 - 2.0 * m * self.w2r + m * m * self.w2).max(0.0)).sqrt() / self.w;
        (m - 1.96 * se, m + 1.96 * se)
    }

    fn half_roi(&self, i: usize) -> f64 {
        let (c, p) = self.halves[i];
        if c > 0.0 { p / c } else { 0.0 }
    }
}

/// Each book's sums; open lots at `prices` (their cost when a price is missing); the halves
/// split at `split` (unix seconds).
fn sums(held: &[Held], prices: &HashMap<String, f64>, split: f64) -> HashMap<String, Sum> {
    let mut out: HashMap<String, Sum> = HashMap::new();
    for h in held {
        let s = out.entry(h.book.clone()).or_default();
        let share = h.granted / h.cost;
        s.copies += h.n;
        s.cost += h.granted;
        match h.closed {
            Some(at) => {
                let pnl = share * h.back - h.granted;
                let r = h.back / h.cost - 1.0;
                let w = h.granted;
                s.closed_n += h.n;
                s.closed_cost += w;
                s.closed_pnl += pnl;
                s.w += w;
                s.wr += w * r;
                s.w2r += w * w * r;
                s.w2r2 += w * w * r * r;
                s.w2 += w * w;
                let half = &mut s.halves[usize::from(at >= split)];
                half.0 += w;
                half.1 += pnl;
            }
            None => {
                s.open_cost += h.granted;
                s.open_value += prices.get(&h.token).map(|p| share * (h.back + h.shares * p)).unwrap_or(h.granted);
            }
        }
    }
    out
}

/// The tokens the books hold open.
fn open_tokens(held: &[Held]) -> Vec<String> {
    let set: HashSet<&String> = held.iter().filter(|h| h.closed.is_none()).map(|h| &h.token).collect();
    set.into_iter().cloned().collect()
}

/// Every book's balance now, open lots at the market's price, into `alloc_snap` (the history
/// the drawdowns are read from).
async fn snapshot(c: &Client, api: &Api, books: &[Book], since: f64) -> Result<()> {
    let h = held(c, since).await?;
    let prices = api.prices(&open_tokens(&h)).await?;
    let all = sums(&h, &prices, now());
    let none = Sum::default();
    let names: Vec<String> = books.iter().map(|b| b.name.clone()).collect();
    let copies: Vec<i64> = books.iter().map(|b| all.get(&b.name).unwrap_or(&none).copies).collect();
    // Each book's sums in its dollars.
    let col = |f: &dyn Fn(&Sum) -> f64| books.iter().map(|b| f(all.get(&b.name).unwrap_or(&none)) * b.stake()).collect::<Vec<f64>>();
    let stakes: Vec<f64> = books.iter().map(Book::stake).collect();
    let (cost, closed_cost, closed_pnl) = (col(&|s| s.cost), col(&|s| s.closed_cost), col(&|s| s.closed_pnl));
    let (open_cost, open_value) = (col(&|s| s.open_cost), col(&|s| s.open_value));
    c.execute(
        "INSERT INTO alloc_snap (at, book, stake, copies, cost, closed_cost, closed_pnl, open_cost, open_value)
         SELECT now(), * FROM unnest($1::text[], $2::float8[], $3::int8[], $4::float8[], $5::float8[], $6::float8[],
                                     $7::float8[], $8::float8[]) ON CONFLICT DO NOTHING",
        &[&names, &stakes, &copies, &cost, &closed_cost, &closed_pnl, &open_cost, &open_value],
    )
    .await?;
    log!("alloc: snapshot of {} books, {} open outcomes priced", books.len(), prices.len());
    Ok(())
}

/// The rule a book must pass to be followed (fixed before the results: README).
const RULE_DAYS: f64 = 14.0;
const RULE_CLOSED: i64 = 300;
const RULE_MAX_DD: f64 = 200.0;

/// Each book since the rules it runs by began: its copies, the closed ones' ROI with its 95%
/// bounds and in each half of the period, the result with the open lots at today's prices, the
/// worst drawdown of the balance; then the comparisons the paper is for, and the rule.
pub async fn report(url: &str) -> Result<()> {
    let c = db::connect(url).await?;
    let api = Api::new()?;
    let st = load(&c).await?;
    let since = st.since;
    let t = now();
    let days = (t - since) / 86400.0;
    let h = held(&c, since).await?;
    let prices = api.prices(&open_tokens(&h)).await.unwrap_or_default();
    let all = sums(&h, &prices, (since + t) / 2.0);
    let books = books();
    let none = Sum::default();
    let sum = |name: &str| all.get(name).unwrap_or(&none);
    // The worst drawdown: the snapshots' balances, then today's.
    let mut dd: HashMap<String, (f64, f64)> = HashMap::new(); // book -> (peak, worst)
    let snaps = c
        .query("SELECT book, closed_pnl + open_value - open_cost FROM alloc_snap WHERE at >= to_timestamp($1) ORDER BY at",
            &[&since])
        .await?;
    let mut track = |b: &str, bal: f64| {
        let e = dd.entry(b.to_string()).or_insert((DEPOSIT, 0.0));
        e.0 = e.0.max(bal);
        e.1 = e.1.max(e.0 - bal);
    };
    for r in &snaps {
        track(&r.get::<_, String>(0), DEPOSIT + r.get::<_, f64>(1));
    }
    for b in &books {
        track(&b.name, DEPOSIT + sum(&b.name).pnl() * b.stake());
    }
    let max_dd = |b: &str| dd.get(b).map(|x| x.1).unwrap_or(0.0);

    println!("Since {} UTC ({days:.1} days), the rules \"{RULES}\": copies of trader orders of ${BIG_ORDER}+.",
        fmt_time(since));
    println!("$1000 a book; W4/W2 a copy = 1000 / its ceiling, 1USD $1 a copy. ROI: the closed lots, [95% bounds],");
    println!("first half / second half by close; pnl and balance: open lots at today's price; dd: the worst drop.\n");
    println!("{:28} {:>6} {:>6} {:>6} {:>8} {:>17} {:>16} {:>9} {:>8} {:>7}", "book", "$/copy", "copies", "closed", "ROI",
        "95%", "halves", "pnl $", "balance", "dd $");
    for b in &books {
        let (s, k) = (sum(&b.name), b.stake());
        let (lo, hi) = s.bounds();
        println!("{:28} {k:6.2} {:6} {:6} {:+7.1}% [{:+6.1}%,{:+6.1}%] {:+6.1}%/{:+6.1}% {:+9.2} {:8.2} {:7.2}", b.name, s.copies,
            s.closed_n, s.roi() * 100.0, lo * 100.0, hi * 100.0, s.half_roi(0) * 100.0, s.half_roi(1) * 100.0, s.pnl() * k,
            DEPOSIT + s.pnl() * k, max_dd(&b.name));
    }

    // The comparisons, each mode apart: ROI of the closed lots pooled over the picks compared.
    let mode = |b: &Book| b.name.rsplit('/').next().unwrap_or("").to_string();
    let pooled = |names: &[String]| {
        let (c, p) = names.iter().map(|n| sum(n)).fold((0.0, 0.0), |a, s| (a.0 + s.closed_cost, a.1 + s.closed_pnl));
        if c > 0.0 { p / c * 100.0 } else { 0.0 }
    };
    let scored = |b: &Book| !matches!(b.score, Score::Random | Score::Eligible);
    for m in ["W4", "W2", "1USD"] {
        let of_mode: Vec<&Book> = books.iter().filter(|b| mode(b) == m).collect();
        println!("\n{m}:");
        // Soft gate against the strict twin.
        let pairs: Vec<(String, String)> = of_mode
            .iter()
            .filter(|b| !b.pick.ends_with("_STRICT"))
            .filter_map(|b| {
                let twin = format!("{}_STRICT/{m}", b.pick);
                books.iter().any(|x| x.name == twin).then(|| (b.name.clone(), twin))
            })
            .collect();
        let won = pairs.iter().filter(|(a, b)| sum(a).roi() > sum(b).roi()).count();
        let (soft, strict): (Vec<String>, Vec<String>) = pairs.iter().cloned().unzip();
        println!("  soft gate vs strict: soft ahead in {won} of {} pairs; pooled ROI {:+.1}% vs {:+.1}%", pairs.len(),
            pooled(&soft), pooled(&strict));
        // A score against a random pick of as many wallets.
        let mut beat = (0, 0);
        for b in of_mode.iter().filter(|b| scored(b)) {
            if let Some(r) = control(&books, b) {
                beat.1 += 1;
                beat.0 += usize::from(sum(&b.name).roi() > sum(&r).roi());
            }
        }
        let names = |f: &dyn Fn(&Book) -> bool| of_mode.iter().filter(|b| f(b)).map(|b| b.name.clone()).collect::<Vec<_>>();
        println!("  scores vs RANDOM of as many wallets: ahead in {} of {}; pooled ROI scored {:+.1}%, RANDOM_10/30/50 {:+.1}%",
            beat.0, beat.1, pooled(&names(&|b| scored(b))), pooled(&names(&|b| b.score == Score::Random && b.gate == Gate::ANY)));
        let ex = |p: &str| format!("{:+.1}%", sum(&format!("{p}/{m}")).roi() * 100.0);
        println!("  the gate, EXPDD_30: none {}, 30+ days {}, strict {}", ex("EXPDD_ANY_30"), ex("EXPDD_30"), ex("EXPDD_30_STRICT"));
        let worst: Vec<f64> = of_mode.iter().filter(|b| scored(b)).map(|b| max_dd(&b.name)).collect();
        println!("  scored books: pooled ROI {:+.1}%, drawdown mean ${:.0}, worst ${:.0}", pooled(&names(&|b| scored(b))),
            worst.iter().sum::<f64>() / worst.len().max(1) as f64, worst.iter().cloned().fold(0.0, f64::max));
    }

    // The rule.
    println!("\nThe rule (README): {RULE_DAYS}+ days, {RULE_CLOSED}+ closed copies, the 95% lower bound of ROI above 0, ROI above \
              its RANDOM of as many wallets, both halves in profit, drawdown under ${RULE_MAX_DD}.");
    let mut rows: Vec<(f64, String, Vec<&str>)> = books
        .iter()
        .filter(|b| scored(b))
        .map(|b| {
            let s = sum(&b.name);
            let ctl = control(&books, b).map(|r| sum(&r).roi());
            let mut miss = Vec::new();
            if days < RULE_DAYS {
                miss.push("days");
            }
            if s.closed_n < RULE_CLOSED {
                miss.push("closed");
            }
            if s.bounds().0 <= 0.0 {
                miss.push("bound");
            }
            if ctl.is_some_and(|r| s.roi() <= r) {
                miss.push("random");
            }
            if s.halves.iter().any(|h| h.1 <= 0.0) {
                miss.push("halves");
            }
            if max_dd(&b.name) >= RULE_MAX_DD {
                miss.push("dd");
            }
            (s.bounds().0, b.name.clone(), miss)
        })
        .collect();
    rows.sort_by(|a, b| b.0.total_cmp(&a.0));
    let pass: Vec<&(f64, String, Vec<&str>)> = rows.iter().filter(|r| r.2.is_empty()).collect();
    if pass.is_empty() {
        println!("  none passes yet; the best by the lower bound, and what they miss:");
        for (lo, name, miss) in rows.iter().take(10) {
            println!("    {name:28} {:+6.1}%  {}", lo * 100.0, miss.join(", "));
        }
    } else {
        for (lo, name, _) in pass {
            println!("  PASSES {name:28} lower bound {:+.1}%", lo * 100.0);
        }
    }

    println!("\nrefused:");
    for r in c
        .query("SELECT book, reason, count(*) FROM alloc WHERE granted = 0 AND at >= to_timestamp($1) GROUP BY 1, 2 ORDER BY 1, 3 DESC",
            &[&since])
        .await?
    {
        let (b, why, n): (String, String, i64) = (r.get(0), r.get(1), r.get(2));
        println!("  {b:28} {why:13} {n}");
    }
    Ok(())
}

/// The random book of as many wallets in the same mode: a score's control.
fn control(books: &[Book], b: &Book) -> Option<String> {
    let mode = b.name.rsplit('/').next()?;
    let name = format!("RANDOM_{}/{mode}", b.n);
    books.iter().any(|x| x.name == name).then_some(name)
}

fn fmt_time(t: f64) -> String {
    let days = (t / 86400.0).floor() as i64;
    // Civil date from days (Howard Hinnant's algorithm).
    let z = days + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    let s = (t - days as f64 * 86400.0) as i64;
    format!("{y}-{m:02}-{d:02} {:02}:{:02}", s / 3600, s / 60 % 60)
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
    fn book_sums() {
        let h = |granted: f64, cost: f64, back: f64, shares: f64, closed: Option<f64>| Held {
            book: "B".into(), token: "t".into(), n: 1, granted, cost, back, shares, closed };
        // Half of a $2 lot that came back $3; a lot of 10 shares open at 0.2 with $1 back.
        let held = [h(1.0, 2.0, 3.0, 0.0, Some(10.0)), h(1.0, 1.0, 1.0, 10.0, None), h(1.0, 1.0, 0.0, 0.0, Some(30.0))];
        let s = &sums(&held, &HashMap::from([("t".to_string(), 0.2)]), 20.0)["B"];
        assert_eq!((s.copies, s.closed_n), (3, 2));
        assert!((s.closed_pnl - (0.5 - 1.0)).abs() < 1e-12);
        assert!((s.open_value - 3.0).abs() < 1e-12 && (s.pnl() - (-0.5 + 2.0)).abs() < 1e-12);
        assert!((s.half_roi(0) - 0.5).abs() < 1e-12 && (s.half_roi(1) + 1.0).abs() < 1e-12);
        let (lo, hi) = s.bounds();
        assert!(lo < s.roi() && s.roi() < hi);
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
        assert_eq!(books().len(), 138);
        assert_eq!(books().iter().filter(|b| b.gate == Gate::STRICT).count(), 63);
        assert!(books().iter().any(|b| b.name == "PNL_30_STRICT/W4"));
        assert!(!books().iter().any(|b| b.name.starts_with("RANDOM_10_STRICT") || b.name.starts_with("EXPDD_ANY_30_STRICT")));
        assert!(Gate::LONG.passes(&r, &r) && !Gate::LONG.passes(&Record { days: 29.0, ..r }, &r));
        assert!(Gate::ANY.passes(&Record::default(), &Record::default()));
        assert!(Gate::STRICT.passes(&r, &r) && !Gate::STRICT.passes(&r, &Record { pnl: -1.0, ..r }));
        assert!(!Gate::STRICT.passes(&Record { pnl: 4.0, ..r }, &r));
        assert_eq!(fmt_time(1791676800.0 + 3723.0), "2026-10-11 01:02");
        let free = books().into_iter().find(|b| b.name == "PF_30/1USD").unwrap();
        assert_eq!(free.stake(), 1.0);
        let mut s = State::default();
        for i in 0..1000 {
            assert_eq!(decide(&free, &mut s, "a", &format!("t{i}"), "e", 1.0).reason, "GRANTED");
        }
        assert!(s.lots.is_empty());
    }
}
