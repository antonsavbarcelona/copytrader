//! The allocator: which of the wallets copied on paper a portfolio would copy, and how much.
//!
//! The paper copies every wallet followed at $1 (engine.rs). On top of it, virtual portfolios
//! ("books") of $10,000 each take some of those copies, scaled to their own size: a book's
//! position is the paper's buy times its stake over the paper's cost, sold as the paper sells
//! (the same share of the lot, at the paper's price) and paid out as the paper is at the
//! settlement. Nothing here trades: the books run side by side so the paper shows which way
//! of picking wallets and sizing their copies works.
//!
//! Who is picked: only the 90-day backtest of the copy (the prior, a file of per wallet sums;
//! research/stats.py). The paper does not enter the picks. One filter for all: the copy made
//! money (pnl > 0). Each score's own filter is only what it cannot be computed without: 20+
//! distinct markets for the scores that divide by risk, by the copies or lean on time; none
//! for the sums of profit (PNL, PNL30). The scores (higher is better; returns per $ copied):
//!
//!     PNL       the copy's result                  PNL30  of the copies of the last 30 days
//!     LCB       mean - 2 sd / sqrt(n)              the mean return with a margin for luck
//!     EXPDD     (pnl / n) / max(drawdown, $1)      PNLSQRTN  mean * sqrt(n)
//!     TSTAT     mean / sd * sqrt(n)                PF     wins / losses, at most 50
//!     SHARPE, SORTINO   of the daily result        CALMAR pnl / max(drawdown, $1)
//!     WEEKS     the share of weeks in profit       COMBO  the mean percentile of the 11 above
//!     RANDOM1..RANDOM10  ten draws of those in profit, new each day (by the date, the draw and
//!                        the size): the scores' control
//!
//! The grid: 22 picks x 10/20/30/50/100 wallets x a stake of 0.5/1/2% of the book's capital x
//! an event limit of 20/50/100% x a buy cap of 3% or 10% over the trader's price = 1980 books,
//! named like `LCB_20_R0.5_E20_C10`. The paper buys the wallets in profit at the 10% cap and
//! fills the same book at 3% alongside (engine.rs): a C10 book takes the first, a C3 book the
//! second. One pick takes the same wallets at every stake, limit and cap.
//!
//! A buy of a picked wallet, each book apart (capital = $10,000 + the closed positions' result;
//! a position's risk = its stake times the share of it still open):
//!   1. the wallet's entry and one add in this outcome open already -> ADD_CAP (2 a wallet and
//!      outcome; other wallets in it are apart)
//!   2. the stake = the book's share of the capital, cut to the room left under the caps:
//!      everything open <= 100% (TOTAL_CAP), the event <= the book's limit (EVENT_CAP), the
//!      wallet <= 20% (TRADER_CAP); under 1/8 of the stake or $1 -> refused
//!
//! A wallet that leaves the picks gets no new positions; the open ones go on to its sale or
//! the settlement.
//!
//! Tables: `alloc_trades` every closed position, `alloc_decisions` only the cut and refused
//! ones, `alloc_status` every book each hour. Every row has the generation: new rules, new
//! generation, the books start again. The state is saved every 15 minutes; after a restart the
//! events since are read again (the rows already written are kept). `copytrader alloc` judges
//! the scores (README).

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

/// The generation: the books start again when the rules change.
const GEN: i32 = 1;
const DEPOSIT: f64 = 10_000.0;
const SIZES: [usize; 5] = [10, 20, 30, 50, 100];
const STAKES: [f64; 3] = [0.005, 0.01, 0.02];
const EVENT_CAPS: [f64; 3] = [0.2, 0.5, 1.0];
/// The buy caps: 3% (the paper's fill at `NARROW_CAP`) and 10% (its own, `WIDE_CAP`).
const PRICE_CAPS: [u8; 2] = [3, 10];
const RANDOMS: u8 = 10;
const TOTAL_CAP: f64 = 1.0;
const TRADER_CAP: f64 = 0.2;
/// One wallet's positions in one outcome: its entry and one add.
const WALLET_OUTCOME: usize = 2;
/// A stake cut under this share of the book's is refused, as is one under $1.
const MIN_SHARE: f64 = 1.0 / 8.0;
const MIN_USD: f64 = 1.0;
/// The scores that divide by risk, by the copies or lean on time need this many markets.
const MIN_MARKETS: f64 = 20.0;
const DD_FLOOR: f64 = 1.0;
const PF_MAX: f64 = 50.0;
const HOUR: f64 = 3600.0;
const SAVE_EVERY_S: f64 = 900.0;
/// The verdict (README): after this many days, a score's mean place among the 10 random
/// draws at least this, and over 5 on this many of the 5 sizes.
const VERDICT_DAYS: f64 = 21.0;
const VERDICT_PLACE: f64 = 7.5;
const VERDICT_SIZES: usize = 4;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Score {
    Pnl,
    Pnl30,
    Lcb,
    ExpDd,
    PnlSqrtN,
    TStat,
    Pf,
    Sharpe,
    Sortino,
    Calmar,
    Weeks,
    Combo,
    Random(u8),
}

/// The scores COMBO averages the percentiles of.
const RATED: [Score; 11] = [Score::Pnl, Score::Pnl30, Score::Lcb, Score::ExpDd, Score::PnlSqrtN, Score::TStat, Score::Pf,
    Score::Sharpe, Score::Sortino, Score::Calmar, Score::Weeks];

impl Score {
    fn all() -> Vec<Score> {
        RATED.iter().copied().chain([Score::Combo]).chain((1..=RANDOMS).map(Score::Random)).collect()
    }

    fn name(self) -> String {
        match self {
            Score::Pnl => "PNL".into(),
            Score::Pnl30 => "PNL30".into(),
            Score::Lcb => "LCB".into(),
            Score::ExpDd => "EXPDD".into(),
            Score::PnlSqrtN => "PNLSQRTN".into(),
            Score::TStat => "TSTAT".into(),
            Score::Pf => "PF".into(),
            Score::Sharpe => "SHARPE".into(),
            Score::Sortino => "SORTINO".into(),
            Score::Calmar => "CALMAR".into(),
            Score::Weeks => "WEEKS".into(),
            Score::Combo => "COMBO".into(),
            Score::Random(k) => format!("RANDOM{k}"),
        }
    }

    /// Needs a record of 20+ markets (all but the sums of profit and the random draws).
    fn needs_markets(self) -> bool {
        !matches!(self, Score::Pnl | Score::Pnl30 | Score::Random(_))
    }
}

/// A wallet's backtest of the copy: per wallet sums (research/stats.py).
#[derive(Clone, Copy, Default, Debug)]
pub struct Record {
    n: f64,
    markets: f64,
    cost: f64,
    pnl: f64,
    win: f64,
    loss: f64,
    r2: f64,
    dd: f64,
    pnl30: f64,
    sharpe: f64,
    sortino: f64,
    weeks: f64,
}

impl Record {
    /// The mean return of a copy (per dollar) and its standard deviation.
    fn mean_sd(&self) -> (f64, f64) {
        let n = self.n.max(1.0);
        let m = if self.cost > 0.0 { self.pnl / self.cost } else { 0.0 };
        (m, (self.r2 / n - m * m).max(0.0).sqrt())
    }

    fn score(&self, s: Score) -> f64 {
        let n = self.n.max(1.0);
        let (m, sd) = self.mean_sd();
        match s {
            Score::Pnl => self.pnl,
            Score::Pnl30 => self.pnl30,
            Score::Lcb => m - 2.0 * sd / n.sqrt(),
            Score::ExpDd => self.pnl / n / self.dd.max(DD_FLOOR),
            Score::PnlSqrtN => m * n.sqrt(),
            Score::TStat => if sd > 0.0 { m / sd * n.sqrt() } else { 0.0 },
            Score::Pf => if self.loss > 0.0 { (self.win / self.loss).min(PF_MAX) } else { PF_MAX },
            Score::Sharpe => self.sharpe,
            Score::Sortino => self.sortino,
            Score::Calmar => self.pnl / self.dd.max(DD_FLOOR),
            Score::Weeks => self.weeks,
            Score::Combo | Score::Random(_) => 0.0,
        }
    }

    /// The filter for all: the copy made money.
    fn in_profit(&self) -> bool {
        self.pnl > 0.0 && self.n >= 1.0
    }
}

/// The backtest's per wallet sums (CSV with a header; research/stats.py).
pub fn read_prior(path: &Path) -> Result<HashMap<String, Record>> {
    let text = std::fs::read_to_string(path)?;
    let mut lines = text.lines();
    let head: Vec<&str> = lines.next().unwrap_or_default().split(',').collect();
    let col = |k: &str| head.iter().position(|h| h.trim() == k).ok_or_else(|| anyhow::anyhow!("no column {k}"));
    let w = col("wallet")?;
    let names = ["n", "markets", "cost", "pnl", "win", "loss", "r2", "dd", "pnl30", "sharpe", "sortino", "weeks"];
    let idx: Vec<usize> = names.iter().map(|k| col(k)).collect::<Result<_>>()?;
    let mut out = HashMap::new();
    for l in lines {
        let f: Vec<&str> = l.split(',').collect();
        let v: Option<Vec<f64>> = idx.iter().map(|&i| f.get(i).and_then(|x| x.trim().parse::<f64>().ok())).collect();
        let Some(v) = v else { continue };
        if v.iter().any(|x| !x.is_finite()) {
            continue;
        }
        let r = Record { n: v[0], markets: v[1], cost: v[2], pnl: v[3], win: v[4], loss: v[5], r2: v[6], dd: v[7], pnl30: v[8],
            sharpe: v[9], sortino: v[10], weeks: v[11] };
        out.insert(f[w].trim().to_lowercase(), r);
    }
    Ok(out)
}

/// The wallets a book may pick: the copy in profit.
pub fn pickable(prior: &HashMap<String, Record>) -> HashSet<String> {
    prior.iter().filter(|(_, r)| r.in_profit()).map(|(w, _)| w.clone()).collect()
}

/// The wallets of every pick (score, size), in order of rank, for the day.
fn picks(prior: &HashMap<String, Record>, day: i64) -> HashMap<(Score, usize), Vec<String>> {
    let mut pool: Vec<(&String, &Record)> = prior.iter().filter(|(_, r)| r.in_profit()).collect();
    pool.sort_by(|a, b| a.0.cmp(b.0));
    let rated: Vec<(&String, &Record)> = pool.iter().copied().filter(|(_, r)| r.markets >= MIN_MARKETS).collect();
    // COMBO: each wallet's mean percentile over the 11 scores, among those with the record.
    let mut combo: HashMap<&String, f64> = HashMap::new();
    for s in RATED {
        let mut v: Vec<(&String, f64)> = rated.iter().map(|(w, r)| (*w, r.score(s))).collect();
        v.sort_by(|a, b| a.1.total_cmp(&b.1));
        let k = (v.len().max(2) - 1) as f64;
        for (i, (w, _)) in v.iter().enumerate() {
            *combo.entry(*w).or_default() += i as f64 / k / RATED.len() as f64;
        }
    }
    let mut out = HashMap::new();
    for s in Score::all() {
        let from = if s.needs_markets() { &rated } else { &pool };
        let mut order: Vec<(&String, f64)> = from
            .iter()
            .map(|(w, r)| (*w, if s == Score::Combo { combo.get(w).copied().unwrap_or(0.0) } else { r.score(s) }))
            .collect();
        for &n in &SIZES {
            if let Score::Random(k) = s {
                order.sort_by_key(|(w, _)| fnv(&format!("{day}/{k}/{n}/{w}")));
            } else {
                order.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(b.0)));
            }
            out.insert((s, n), order.iter().take(n).map(|(w, _)| w.to_string()).collect());
        }
    }
    out
}

/// A stable hash (the day's random draw must not change on a restart).
fn fnv(s: &str) -> u64 {
    s.bytes().fold(0xcbf29ce484222325u64, |h, b| (h ^ b as u64).wrapping_mul(0x100000001b3))
}

struct Book {
    /// `LCB_20_R0.5_E20_C10`.
    name: String,
    score: Score,
    size: usize,
    /// The stake, a share of the capital, and the most of it one event may hold.
    stake: f64,
    event_cap: f64,
    /// Bought at the 3% cap (the paper's `narrow` fill), else at 10%.
    narrow: bool,
}

fn books() -> Vec<Book> {
    let pct = |x: f64| format!("{}", x * 100.0);
    let mut out = Vec::new();
    for score in Score::all() {
        for &size in &SIZES {
            for &stake in &STAKES {
                for &event_cap in &EVENT_CAPS {
                    for c in PRICE_CAPS {
                        let name = format!("{}_{size}_R{}_E{}_C{c}", score.name(), pct(stake), pct(event_cap));
                        out.push(Book { name, score, size, stake, event_cap, narrow: c == 3 });
                    }
                }
            }
        }
    }
    out
}

/// A book's open position: a share of one paper buy.
#[derive(Clone, Serialize, Deserialize, Debug)]
struct Position {
    /// The paper's buy event.
    id: i64,
    at: f64,
    wallet: String,
    token: String,
    market: String,
    event: String,
    price: f64,
    /// The capital when it was opened, the dollars put in, the shares bought and still open,
    /// the dollars back from the sales so far.
    capital: f64,
    stake: f64,
    shares0: f64,
    shares: f64,
    back: f64,
}

impl Position {
    /// The stake still at risk: a position partly sold weighs less.
    fn risk(&self) -> f64 {
        if self.shares0 > 0.0 { self.stake * self.shares / self.shares0 } else { 0.0 }
    }
}

#[derive(Default, Clone, Serialize, Deserialize)]
struct BookState {
    /// The closed positions' result.
    realized: f64,
    positions: Vec<Position>,
    entries: u64,
    cuts: u64,
    refusals: u64,
    closed: u64,
}

impl BookState {
    fn capital(&self) -> f64 {
        DEPOSIT + self.realized
    }
}

#[derive(Default, Serialize, Deserialize)]
struct State {
    #[serde(default)]
    generation: i32,
    /// The last paper event read.
    #[serde(default)]
    last_id: i64,
    /// When the generation began (unix seconds).
    #[serde(default)]
    since: f64,
    #[serde(default)]
    status_at: f64,
    #[serde(skip)]
    saved: f64,
    #[serde(default)]
    books: HashMap<String, BookState>,
    /// The day the picks are of; the wallets each book copies (made again on a start).
    #[serde(skip)]
    day: i64,
    #[serde(skip)]
    picked: HashMap<String, Vec<usize>>,
    /// "wallet|token" -> positions open over all books (to skip the paper's other sales fast).
    #[serde(skip)]
    open: HashMap<String, usize>,
    #[serde(skip)]
    not_picked: u64,
}

impl State {
    fn index_open(&mut self) {
        self.open.clear();
        for b in self.books.values() {
            for p in &b.positions {
                *self.open.entry(key(&p.wallet, &p.token)).or_default() += 1;
            }
        }
    }
}

fn key(wallet: &str, token: &str) -> String {
    format!("{wallet}|{token}")
}

/// A cut or refused entry, as `alloc_decisions` keeps it.
struct Decision {
    book: String,
    id: i64,
    wallet: String,
    token: String,
    market: String,
    event: String,
    wanted: f64,
    granted: f64,
    reason: &'static str,
}

/// A closed position, as `alloc_trades` keeps it.
struct Closed {
    book: String,
    pos: Position,
    close_id: i64,
    closed_at: f64,
    reason: &'static str,
}

/// One book's answer to a picked wallet's buy (paper cost and shares): the position, and the
/// cap that cut or refused it.
fn decide(b: &Book, s: &mut BookState, p: Position, paper_cost: f64, paper_shares: f64)
    -> (Option<Position>, Option<(&'static str, f64, f64)>) {
    let capital = s.capital();
    let want = b.stake * capital;
    if s.positions.iter().filter(|x| x.wallet == p.wallet && x.token == p.token).count() >= WALLET_OUTCOME {
        return (None, Some(("ADD_CAP", want, 0.0)));
    }
    let open = |f: &dyn Fn(&Position) -> bool| s.positions.iter().filter(|x| f(x)).map(Position::risk).sum::<f64>();
    let rooms = [
        ("TOTAL_CAP", TOTAL_CAP * capital - open(&|_| true)),
        ("EVENT_CAP", b.event_cap * capital - open(&|x| x.event == p.event)),
        ("TRADER_CAP", TRADER_CAP * capital - open(&|x| x.wallet == p.wallet)),
    ];
    let mut grant = want;
    let mut why = "";
    for (r, room) in rooms {
        if room < grant {
            grant = room.max(0.0);
            why = r;
        }
    }
    if grant < want * MIN_SHARE || grant < MIN_USD {
        return (None, Some((if why.is_empty() { "TOO_SMALL" } else { why }, want, 0.0)));
    }
    let k = grant / paper_cost;
    let pos = Position { capital, stake: grant, shares0: k * paper_shares, shares: k * paper_shares, ..p };
    s.positions.push(pos.clone());
    s.entries += 1;
    let cut = (grant < want - 1e-9).then_some((why, want, grant));
    if cut.is_some() {
        s.cuts += 1;
    }
    (Some(pos), cut)
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
                    log!("alloc: generation {GEN}, {} books, from paper event {}, {} positions open", books.len(), s.last_id,
                        s.books.values().map(|b| b.positions.len()).sum::<usize>());
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

/// The saved state; another generation's (or none) starts the books again from the paper's
/// next event.
async fn load(c: &Client) -> Result<State> {
    let saved = c.query_opt("SELECT data FROM state WHERE id = 'alloc'", &[]).await?;
    let mut s: State = saved.and_then(|r| serde_json::from_value(r.get::<_, Value>(0)).ok()).unwrap_or_default();
    if s.generation != GEN {
        let last: i64 = c.query_one("SELECT coalesce(max(id), 0) FROM events", &[]).await?.get(0);
        s = State { generation: GEN, last_id: last, since: now(), ..Default::default() };
        log!("alloc: generation {GEN} begins at paper event {last}");
    }
    s.index_open();
    Ok(s)
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

async fn step(c: &Client, api: &Api, prior: &HashMap<String, Record>, books: &[Book], s: &mut State) -> Result<()> {
    let day = (now() / 86400.0) as i64;
    if s.day != day {
        let p = picks(prior, day);
        s.picked.clear();
        for (i, b) in books.iter().enumerate() {
            for w in &p[&(b.score, b.size)] {
                s.picked.entry(w.clone()).or_default().push(i);
            }
        }
        s.day = day;
        let through = |f: &dyn Fn(&Record) -> bool| prior.values().filter(|r| f(r)).count();
        log!("alloc: picks for day {day}: {} wallets in the backtest, {} in profit, {} of them with {MIN_MARKETS}+ markets; \
              {} wallets in some book", prior.len(), through(&|r| r.in_profit()),
            through(&|r| r.in_profit() && r.markets >= MIN_MARKETS), s.picked.len());
    }
    if now() - s.status_at >= HOUR {
        status(c, api, books, s).await?;
        s.status_at = now();
        save(c, s).await?;
    }
    let rows = c
        .query(
            "SELECT id, kind, wallet, token, data, extract(epoch FROM at)::float8 FROM events
             WHERE id > $1 AND kind IN ('buy', 'sell', 'settle') ORDER BY id LIMIT 5000",
            &[&s.last_id],
        )
        .await?;
    if rows.is_empty() {
        return Ok(());
    }
    let mut decided: Vec<Decision> = Vec::new();
    let mut closed: Vec<Closed> = Vec::new();
    for r in &rows {
        let (id, kind, data, at): (i64, String, Value, f64) = (r.get(0), r.get(1), r.get(4), r.get(5));
        let wallet: String = r.get::<_, Option<String>>(2).unwrap_or_default();
        let token: String = r.get::<_, Option<String>>(3).unwrap_or_default();
        s.last_id = id;
        let read = |f: &Value| (f["usd"].as_f64().unwrap_or(0.0), f["fee"].as_f64().unwrap_or(0.0), f["shares"].as_f64().unwrap_or(0.0));
        let (usd, fee, shares) = read(&data["fill"]);
        // The 3% books' fill: the paper's at the narrow cap (its own fill when there is none).
        let (n_usd, n_fee, n_shares) = if data["narrow"].is_object() { read(&data["narrow"]["fill"]) } else { (usd, fee, shares) };
        match kind.as_str() {
            "buy" => {
                if (usd + fee <= 0.0 || shares <= 0.0) && (n_usd + n_fee <= 0.0 || n_shares <= 0.0) {
                    continue;
                }
                let Some(idx) = s.picked.get(&wallet).cloned() else {
                    s.not_picked += 1;
                    continue;
                };
                let market = data["condition"].as_str().filter(|c| !c.is_empty()).unwrap_or(&token).to_string();
                let event = match api.market(&token, 7.0 * 86400.0).await {
                    Ok(m) => m.event.unwrap_or(m.condition),
                    Err(_) => market.clone(),
                };
                let base = Position { id, at, wallet: wallet.clone(), token: token.clone(), market: market.clone(),
                    event: event.clone(), price: 0.0, capital: 0.0, stake: 0.0, shares0: 0.0, shares: 0.0, back: 0.0 };
                for i in idx {
                    let b = &books[i];
                    let (paid, fees, sh) = if b.narrow { (n_usd, n_fee, n_shares) } else { (usd, fee, shares) };
                    if paid + fees <= 0.0 || sh <= 0.0 {
                        // Not filled at this book's cap: no copy to take.
                        continue;
                    }
                    let bs = s.books.entry(b.name.clone()).or_default();
                    let (pos, why) = decide(b, bs, Position { price: paid / sh, ..base.clone() }, paid + fees, sh);
                    if pos.is_some() {
                        *s.open.entry(key(&wallet, &token)).or_default() += 1;
                    } else {
                        bs.refusals += 1;
                    }
                    if let Some((reason, wanted, granted)) = why {
                        decided.push(Decision { book: b.name.clone(), id, wallet: wallet.clone(), token: token.clone(),
                            market: market.clone(), event: event.clone(), wanted, granted, reason });
                    }
                }
            }
            "sell" | "settle" => {
                let k = key(&wallet, &token);
                if !s.open.contains_key(&k) {
                    continue;
                }
                // The share of the paper's lot sold and the dollars a share brought, net of fees;
                // the settlement pays the rest at the final price.
                let (frac, per_share, out, reason) = if kind == "sell" {
                    let before = data["held_before"].as_f64().unwrap_or(0.0);
                    let after = data["held_after"].as_f64().unwrap_or(before);
                    if before <= 0.0 || shares <= 0.0 {
                        continue;
                    }
                    ((shares / before).min(1.0), (usd - fee) / shares, after <= 1e-6, "TRADER_CLOSED")
                } else {
                    (1.0, data["price"].as_f64().unwrap_or(0.0), true, "SETTLED")
                };
                for (name, bs) in s.books.iter_mut() {
                    let mut i = 0;
                    while i < bs.positions.len() {
                        let p = &mut bs.positions[i];
                        if p.wallet != wallet || p.token != token {
                            i += 1;
                            continue;
                        }
                        let sold = if out { p.shares } else { p.shares * frac };
                        p.back += sold * per_share;
                        p.shares -= sold;
                        if out || p.shares <= 1e-9 {
                            let pos = bs.positions.swap_remove(i);
                            bs.realized += pos.back - pos.stake;
                            bs.closed += 1;
                            closed.push(Closed { book: name.clone(), pos, close_id: id, closed_at: at, reason });
                        } else {
                            i += 1;
                        }
                    }
                }
                if out {
                    s.open.remove(&k);
                }
            }
            _ => {}
        }
    }
    write(c, &decided, &closed).await?;
    if now() - s.saved >= SAVE_EVERY_S {
        save(c, s).await?;
    }
    Ok(())
}

/// The cut and refused entries and the closed positions, one statement each.
async fn write(c: &Client, decided: &[Decision], closed: &[Closed]) -> Result<()> {
    if !decided.is_empty() {
        let txt = |f: &dyn Fn(&Decision) -> String| decided.iter().map(f).collect::<Vec<String>>();
        let num = |f: &dyn Fn(&Decision) -> f64| decided.iter().map(f).collect::<Vec<f64>>();
        let ids: Vec<i64> = decided.iter().map(|d| d.id).collect();
        c.execute(
            "INSERT INTO alloc_decisions (gen, at, book, event_id, wallet, token, market, event, wanted, granted, reason)
             SELECT $1, now(), * FROM unnest($2::text[], $3::int8[], $4::text[], $5::text[], $6::text[], $7::text[],
                                              $8::float8[], $9::float8[], $10::text[])
             ON CONFLICT DO NOTHING",
            &[&GEN, &txt(&|d| d.book.clone()), &ids, &txt(&|d| d.wallet.clone()), &txt(&|d| d.token.clone()),
                &txt(&|d| d.market.clone()), &txt(&|d| d.event.clone()), &num(&|d| d.wanted), &num(&|d| d.granted),
                &txt(&|d| d.reason.to_string())],
        )
        .await?;
    }
    if !closed.is_empty() {
        let txt = |f: &dyn Fn(&Closed) -> String| closed.iter().map(f).collect::<Vec<String>>();
        let num = |f: &dyn Fn(&Closed) -> f64| closed.iter().map(f).collect::<Vec<f64>>();
        let int = |f: &dyn Fn(&Closed) -> i64| closed.iter().map(f).collect::<Vec<i64>>();
        c.execute(
            "INSERT INTO alloc_trades (gen, book, entry_id, close_id, wallet, token, market, event, opened_at, closed_at,
                                       price, capital, stake, shares, back, pnl, r, hold_h, reason)
             SELECT $1, b, e, x, w, t, m, ev, to_timestamp(o), to_timestamp(cl), p, cap, st, sh, bk, bk - st, (bk - st) / st,
                    (cl - o) / 3600, rs
             FROM unnest($2::text[], $3::int8[], $4::int8[], $5::text[], $6::text[], $7::text[], $8::text[], $9::float8[],
                         $10::float8[], $11::float8[], $12::float8[], $13::float8[], $14::float8[], $15::float8[], $16::text[])
                  AS u(b, e, x, w, t, m, ev, o, cl, p, cap, st, sh, bk, rs)
             ON CONFLICT DO NOTHING",
            &[&GEN, &txt(&|x| x.book.clone()), &int(&|x| x.pos.id), &int(&|x| x.close_id), &txt(&|x| x.pos.wallet.clone()),
                &txt(&|x| x.pos.token.clone()), &txt(&|x| x.pos.market.clone()), &txt(&|x| x.pos.event.clone()),
                &num(&|x| x.pos.at), &num(&|x| x.closed_at), &num(&|x| x.pos.price), &num(&|x| x.pos.capital),
                &num(&|x| x.pos.stake), &num(&|x| x.pos.shares0), &num(&|x| x.pos.back), &txt(&|x| x.reason.to_string())],
        )
        .await?;
    }
    Ok(())
}

/// A book's value now: its capital plus the open positions at the market's price (their
/// stake when a price is missing).
fn equity(b: &BookState, prices: &HashMap<String, f64>) -> f64 {
    b.capital()
        + b.positions
            .iter()
            .map(|p| prices.get(&p.token).map(|q| p.back + p.shares * q).unwrap_or(p.back + p.risk()) - p.stake)
            .sum::<f64>()
}

/// Every book's status into `alloc_status`, once an hour.
async fn status(c: &Client, api: &Api, books: &[Book], s: &State) -> Result<()> {
    let tokens: Vec<String> = s.open.keys().filter_map(|k| k.split('|').nth(1)).map(String::from).collect::<HashSet<_>>()
        .into_iter().collect();
    let prices = api.prices(&tokens).await.unwrap_or_default();
    let none = BookState::default();
    let st = |b: &Book| s.books.get(&b.name).unwrap_or(&none);
    let names: Vec<String> = books.iter().map(|b| b.name.clone()).collect();
    let num = |f: &dyn Fn(&BookState) -> f64| books.iter().map(|b| f(st(b))).collect::<Vec<f64>>();
    let int = |f: &dyn Fn(&BookState) -> i64| books.iter().map(|b| f(st(b))).collect::<Vec<i64>>();
    let event_max = |b: &BookState| {
        let mut m: HashMap<&str, f64> = HashMap::new();
        for p in &b.positions {
            *m.entry(&p.event).or_default() += p.risk();
        }
        m.values().cloned().fold(0.0, f64::max)
    };
    c.execute(
        "INSERT INTO alloc_status (gen, at, last_id, book, capital, equity, open_risk, event_max, positions, entries, cuts,
                                   refusals, closed)
         SELECT $1, now(), $2, * FROM unnest($3::text[], $4::float8[], $5::float8[], $6::float8[], $7::float8[], $8::int8[],
                                              $9::int8[], $10::int8[], $11::int8[], $12::int8[])
         ON CONFLICT DO NOTHING",
        &[&GEN, &s.last_id, &names, &num(&|b| b.capital()), &num(&|b| equity(b, &prices)),
            &num(&|b| b.positions.iter().map(Position::risk).sum()), &num(&|b| event_max(b)),
            &int(&|b| b.positions.len() as i64), &int(&|b| b.entries as i64), &int(&|b| b.cuts as i64),
            &int(&|b| b.refusals as i64), &int(&|b| b.closed as i64)],
    )
    .await?;
    log!("alloc: status of {} books, {} positions open over {} outcomes ({} priced); {} buys of wallets no book picked",
        books.len(), s.open.values().sum::<usize>(), tokens.len(), prices.len(), s.not_picked);
    Ok(())
}

/// A book in the report: its return now (open positions at today's price) and its worst
/// drawdown (the hourly statuses, then now).
struct Seen {
    ret: f64,
    dd: f64,
}

/// The report: each score's place among the random draws and the verdict; `--score X` all
/// of one score's books; `--detail NAME` one book.
pub async fn report(url: &str, args: &[String]) -> Result<()> {
    let c = db::connect(url).await?;
    let api = Api::new()?;
    let s = load(&c).await?;
    let books = books();
    let days = (now() - s.since) / 86400.0;
    let tokens: Vec<String> = s.open.keys().filter_map(|k| k.split('|').nth(1)).map(String::from).collect::<HashSet<_>>()
        .into_iter().collect();
    let prices = api.prices(&tokens).await.unwrap_or_default();
    let none = BookState::default();
    let mut peak: HashMap<String, (f64, f64)> = HashMap::new(); // book -> (peak, worst drop, as shares)
    for r in c.query("SELECT book, equity FROM alloc_status WHERE gen = $1 ORDER BY at", &[&GEN]).await? {
        let e = peak.entry(r.get(0)).or_insert((DEPOSIT, 0.0));
        let eq: f64 = r.get(1);
        e.0 = e.0.max(eq);
        e.1 = e.1.max(1.0 - eq / e.0);
    }
    let seen: HashMap<&str, Seen> = books
        .iter()
        .map(|b| {
            let eq = equity(s.books.get(&b.name).unwrap_or(&none), &prices);
            let (pk, worst) = peak.get(&b.name).copied().unwrap_or((DEPOSIT, 0.0));
            let pk = pk.max(eq);
            (b.name.as_str(), Seen { ret: eq / DEPOSIT - 1.0, dd: worst.max(1.0 - eq / pk) })
        })
        .collect();
    // A score's book against the 10 random draws of the same size, stake and limit: how many
    // of them it is ahead of (a tie counts a half).
    let place = |b: &Book| -> f64 {
        let me = seen[b.name.as_str()].ret;
        (1..=RANDOMS)
            .map(|k| {
                let r = format!("RANDOM{k}{}", &b.name[b.score.name().len()..]);
                let other = seen[r.as_str()].ret;
                if me > other { 1.0 } else if me == other { 0.5 } else { 0.0 }
            })
            .sum()
    };
    let median = |mut v: Vec<f64>| {
        v.sort_by(|a, b| a.total_cmp(b));
        if v.is_empty() { 0.0 } else { v[v.len() / 2] }
    };
    println!("Generation {GEN} since {} UTC ({days:.1} days); {} books of ${DEPOSIT:.0}; returns with the open positions \
              at today's price.", fmt_time(s.since), books.len());

    let flag = |k: &str| args.iter().position(|a| a == k).and_then(|i| args.get(i + 1)).cloned();
    if let Some(name) = flag("--detail") {
        let b = books.iter().find(|b| b.name == name).ok_or_else(|| anyhow::anyhow!("no book {name}"))?;
        let bs = s.books.get(&name).unwrap_or(&none);
        let v = &seen[name.as_str()];
        println!("\n{name}: return {:+.2}%, drawdown {:.1}%, capital ${:.2}, {} open, {} entries, {} cut, {} refused, {} closed",
            v.ret * 100.0, v.dd * 100.0, bs.capital(), bs.positions.len(), bs.entries, bs.cuts, bs.refusals, bs.closed);
        if !matches!(b.score, Score::Random(_)) {
            println!("place among the 10 random draws: {:.1}", place(b));
        }
        println!("\nopen:");
        for p in &bs.positions {
            println!("  {} {} {:.3} stake ${:.2} risk ${:.2} now {}", fmt_time(p.at), &p.wallet[..10], p.price, p.stake, p.risk(),
                prices.get(&p.token).map(|q| format!("{q:.3}")).unwrap_or("?".into()));
        }
        println!("\nthe last 20 closed:");
        for r in c
            .query("SELECT closed_at::text, wallet, price, stake, pnl, reason FROM alloc_trades WHERE gen = $1 AND book = $2
                    ORDER BY closed_at DESC LIMIT 20", &[&GEN, &name])
            .await?
        {
            let (at, w, p, st, pnl, why): (String, String, f64, f64, f64, String) =
                (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4), r.get(5));
            println!("  {} {} {p:.3} ${st:.2} -> {pnl:+.2} {why}", &at[..16], &w[..10]);
        }
        println!("\ncut or refused:");
        for r in c
            .query("SELECT reason, count(*) FROM alloc_decisions WHERE gen = $1 AND book = $2
                    GROUP BY 1 ORDER BY 2 DESC", &[&GEN, &name])
            .await?
        {
            let (why, n): (String, i64) = (r.get(0), r.get(1));
            println!("  {why:22} {n}");
        }
        return Ok(());
    }
    if let Some(sc) = flag("--score") {
        println!("\n{:28} {:>8} {:>6} {:>6} {:>5} {:>7} {:>5} {:>8}", "book", "return", "dd", "place", "open", "entries", "cut",
            "refused");
        for b in books.iter().filter(|b| b.score.name() == sc) {
            let (v, bs) = (&seen[b.name.as_str()], s.books.get(&b.name).unwrap_or(&none));
            let pl = if matches!(b.score, Score::Random(_)) { "-".into() } else { format!("{:.1}", place(b)) };
            println!("{:28} {:+7.2}% {:5.1}% {pl:>6} {:5} {:7} {:5} {:8}", b.name, v.ret * 100.0, v.dd * 100.0,
                bs.positions.len(), bs.entries, bs.cuts, bs.refusals);
        }
        return Ok(());
    }

    println!("\nThe verdict (README): after {VERDICT_DAYS} days a score works if its mean place among the 10 random draws \
              (0-10, a random pick 5) is {VERDICT_PLACE}+ over its 90 books, and over 5 on {VERDICT_SIZES} of the 5 sizes.\n");
    print!("{:10}", "score");
    for n in SIZES {
        print!(" {n:>5}");
    }
    println!(" {:>6} {:>9} {:>7}  verdict", "mean", "median r", "med dd");
    let mut scores: Vec<Score> = Score::all().into_iter().filter(|s| !matches!(s, Score::Random(_))).collect();
    let mean_place = |sc: Score| {
        let v: Vec<f64> = books.iter().filter(|b| b.score == sc).map(place).collect();
        v.iter().sum::<f64>() / v.len() as f64
    };
    scores.sort_by(|a, b| mean_place(*b).total_cmp(&mean_place(*a)));
    for sc in scores.iter().copied().chain((1..=RANDOMS).map(Score::Random)) {
        let of: Vec<&Book> = books.iter().filter(|b| b.score == sc).collect();
        print!("{:10}", sc.name());
        let random = matches!(sc, Score::Random(_));
        let mut over = 0;
        for n in SIZES {
            if random {
                print!(" {:>5}", "-");
                continue;
            }
            let v: Vec<f64> = of.iter().filter(|b| b.size == n).map(|b| place(b)).collect();
            let m = v.iter().sum::<f64>() / v.len() as f64;
            over += usize::from(m > 5.0);
            print!(" {m:5.1}");
        }
        let rets = median(of.iter().map(|b| seen[b.name.as_str()].ret).collect());
        let dds = median(of.iter().map(|b| seen[b.name.as_str()].dd).collect());
        if random {
            println!(" {:>6} {:+8.1}% {:6.1}%", "-", rets * 100.0, dds * 100.0);
            continue;
        }
        let m = mean_place(sc);
        let verdict = if days < VERDICT_DAYS {
            format!("wait (day {:.0} of {VERDICT_DAYS})", days)
        } else if m >= VERDICT_PLACE && over >= VERDICT_SIZES {
            "WORKS".into()
        } else {
            "no".into()
        };
        println!(" {m:6.2} {:+8.1}% {:6.1}%  {verdict}", rets * 100.0, dds * 100.0);
    }

    println!("\nThe buy cap: mean return / median drawdown at 3% and 10% over the trader's price.");
    println!("{:10} {:>17} {:>17}", "", "C3", "C10");
    for random in [false, true] {
        let cell = |narrow: bool| {
            let v: Vec<&Seen> = books
                .iter()
                .filter(|b| b.narrow == narrow && matches!(b.score, Score::Random(_)) == random)
                .map(|b| &seen[b.name.as_str()])
                .collect();
            let r = v.iter().map(|x| x.ret).sum::<f64>() / v.len() as f64;
            format!("{:+7.1}% / {:5.1}%", r * 100.0, median(v.iter().map(|x| x.dd).collect()) * 100.0)
        };
        println!("{:10} {:>17} {:>17}", if random { "random" } else { "scores" }, cell(true), cell(false));
    }

    println!("\nSizing: mean return / median drawdown, the scores' books and the random draws'.");
    println!("{:16} {:>17} {:>17}", "stake / event", "scores", "random");
    for &st in &STAKES {
        for &ev in &EVENT_CAPS {
            let cell = |random: bool| {
                let v: Vec<&Seen> = books
                    .iter()
                    .filter(|b| b.stake == st && b.event_cap == ev && matches!(b.score, Score::Random(_)) == random)
                    .map(|b| &seen[b.name.as_str()])
                    .collect();
                let r = v.iter().map(|x| x.ret).sum::<f64>() / v.len() as f64;
                format!("{:+7.1}% / {:5.1}%", r * 100.0, median(v.iter().map(|x| x.dd).collect()) * 100.0)
            };
            println!("{:16} {:>17} {:>17}", format!("{}% / {}%", st * 100.0, ev * 100.0), cell(false), cell(true));
        }
    }

    println!("\nCut or refused, all books:");
    for r in c.query("SELECT reason, count(*), count(*) FILTER (WHERE granted > 0) FROM alloc_decisions WHERE gen = $1
                      GROUP BY 1 ORDER BY 2 DESC", &[&GEN]).await?
    {
        let (why, n, cut): (String, i64, i64) = (r.get(0), r.get(1), r.get(2));
        println!("  {why:22} {:8} refused, {cut:8} cut", n - cut);
    }
    Ok(())
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

    fn pos(wallet: &str, market: &str, event: &str) -> Position {
        Position { id: 1, at: 0.0, wallet: wallet.into(), token: format!("{market}y"), market: market.into(), event: event.into(),
            price: 0.5, capital: 0.0, stake: 0.0, shares0: 0.0, shares: 0.0, back: 0.0 }
    }

    #[test]
    fn grid() {
        let b = books();
        assert_eq!(b.len(), 1980);
        assert!(b.iter().any(|x| x.name == "LCB_20_R0.5_E20_C3" && x.narrow));
        assert!(b.iter().any(|x| x.name == "RANDOM7_50_R1_E50_C10" && !x.narrow));
        assert!(b.iter().any(|x| x.name == "COMBO_100_R2_E100_C10"));
        assert_eq!(b.iter().map(|x| &x.name).collect::<HashSet<_>>().len(), 1980);
        assert_eq!(fmt_time(1791676800.0 + 3723.0), "2026-10-11 01:02");
    }

    #[test]
    fn caps() {
        // 2% a stake, an event at most 20%: $200 a position of $10,000.
        let b = Book { name: "T".into(), score: Score::Pnl, size: 10, stake: 0.02, event_cap: 0.2, narrow: false };
        let mut s = BookState::default();
        let mut go = |s: &mut BookState, w: &str, m: &str, e: &str| decide(&b, s, pos(w, m, e), 1.0, 2.0);
        let (p, why) = go(&mut s, "a", "m1", "e1");
        assert!(why.is_none() && (p.unwrap().stake - 200.0).abs() < 1e-9);
        assert_eq!(s.positions[0].shares, 400.0);
        // The wallet's add in the outcome is taken, a third entry not; another wallet's is.
        assert!(go(&mut s, "a", "m1", "e1").0.is_some());
        assert_eq!(go(&mut s, "a", "m1", "e1").1.unwrap().0, "ADD_CAP");
        assert!(go(&mut s, "b", "m1", "e1").0.is_some());
        // e1 holds $600 of its $2,000; then 7 more positions fill the event.
        for i in 0..7 {
            assert!(go(&mut s, &format!("w{i}"), &format!("n{i}"), "e1").0.is_some());
        }
        assert_eq!(go(&mut s, "z", "q", "e1").1.unwrap().0, "EVENT_CAP");
        // One wallet: at most $2,000, so 10 positions; the 11th is refused.
        for i in 0..10 {
            assert!(go(&mut s, "x", &format!("x{i}"), &format!("f{i}")).0.is_some());
        }
        let (p, why) = go(&mut s, "x", "x10", "f10");
        assert!(p.is_none() && why.unwrap().0 == "TRADER_CAP");
        // A cut: $50 of room in the event gives a $50 position (over 1/8 of $200).
        let mut s = BookState::default();
        s.positions.push(Position { stake: 1950.0, shares0: 1.0, shares: 1.0, ..pos("o", "m", "e") });
        let (p, why) = go(&mut s, "a", "m2", "e");
        assert!((p.unwrap().stake - 50.0).abs() < 1e-9);
        assert_eq!(why.unwrap().0, "EVENT_CAP");
        // A position half sold weighs half.
        assert!((Position { stake: 100.0, shares0: 10.0, shares: 5.0, ..pos("o", "m", "e") }.risk() - 50.0).abs() < 1e-12);
    }

    #[test]
    fn scores_and_picks() {
        let r = Record { n: 100.0, markets: 50.0, cost: 100.0, pnl: 10.0, win: 60.0, loss: 50.0, r2: 100.0, dd: 20.0, pnl30: 3.0,
            sharpe: 1.0, sortino: 2.0, weeks: 0.6 };
        let (m, sd) = r.mean_sd();
        assert!((m - 0.1).abs() < 1e-12 && (sd - (1.0f64 - 0.01).sqrt()).abs() < 1e-12);
        assert!((r.score(Score::Lcb) - (0.1 - 2.0 * sd / 10.0)).abs() < 1e-12);
        assert!((r.score(Score::ExpDd) - 0.1 / 20.0).abs() < 1e-12);
        assert!((r.score(Score::Pf) - 1.2).abs() < 1e-12);
        assert_eq!(Record { loss: 0.0, ..r }.score(Score::Pf), PF_MAX);
        let mut prior = HashMap::new();
        for i in 0..300 {
            let pnl = i as f64 - 50.0;
            prior.insert(format!("w{i:03}"), Record { pnl, markets: if i % 2 == 0 { 30.0 } else { 5.0 }, ..r });
        }
        let p = picks(&prior, 100);
        assert_eq!(p[&(Score::Pnl, 10)][0], "w299");
        // LCB needs 20+ markets: the odd wallets are out.
        assert!(p[&(Score::Lcb, 100)].iter().all(|w| w[1..].parse::<i32>().unwrap() % 2 == 0));
        // Random: from those in profit only, the same on a restart, new the next day.
        let r1 = &p[&(Score::Random(1), 20)];
        assert!(r1.iter().all(|w| w[1..].parse::<i32>().unwrap() > 50));
        assert_eq!(r1, &picks(&prior, 100)[&(Score::Random(1), 20)]);
        assert_ne!(r1, &picks(&prior, 101)[&(Score::Random(1), 20)]);
        assert_ne!(r1, &p[&(Score::Random(2), 20)]);
    }
}
