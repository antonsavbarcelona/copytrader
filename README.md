# copytrader

Copy-trading on Polymarket in three steps: traders are found by their record (round one), each
is copied on paper for a week against the live order book, the way a real $1 copy would fill
(round two), and those whose copies earn are copied live (the golden list).

```
cargo build --release
target/release/copytrader run    --data data            # runs until stopped
target/release/copytrader report --data data [--hours 24]
```

Needs Rust 1.85+ (`rust-version` in Cargo.toml; the lock file is resolved for it).
The running paper copy uses a copy of the binary (`data/bin/copytrader.exe`), so a rebuild
does not fail on a locked `target/release/copytrader.exe`.

`run` options: `--stake 1` (USD per buy), `--cap 0.02` (max price over the wallet's),
`--sell-floor 0.05`, `--market-gap-h 6`, `--no-scout` (no round one), `--polyfox` (also copy
every Polyfox wallet on paper, as before).

## Deploy (Railway)

1. New project -> Deploy from GitHub repo (this one). It builds the `Dockerfile`;
   `railway.json` restarts it whenever it exits.
2. Add a PostgreSQL database to the project and set on the copytrader service
   `DATABASE_URL = ${{Postgres.DATABASE_URL}}`.
3. That's it: no port, no volume. Every event, the followed wallets and the state snapshot
   (every minute and on SIGTERM) go to Postgres; a restart or redeploy restores the open lots
   from there.

The run exits on its own (and Railway restarts it) when a stream, read-back or engine task
stops, or when no followed wallet has traded for 30 minutes.

Report on the server's data from anywhere, with the database's public URL:

```
DATABASE_URL=postgresql://... target/release/copytrader report
```

## What the image runs now: the backtest's wallets and the allocator

The `Dockerfile` runs with `--no-scout --wallets-file wallets/all.txt --poll-file
wallets/picks.txt --alloc-prior wallets/backtest_stats.csv`:

- The paper copies every wallet in `all.txt` (14,254: active since 2026-09-10 with enough
  history for the 90-day copy backtest) at $1; the REST read-back reads only `picks.txt`
  (the backtest's 172 picks), the trade stream sees the rest. Round one and the trials stand
  still, and live buys nothing new: it only follows its held lots to their sale or the
  settlement and redeems them.
- The allocator (`src/allocator.rs`) keeps 138 virtual books over those copies, each with its
  own $1000 (details below). Decisions go to `alloc`, the rankings to `alloc_rank`, every
  book's balance every 6 h to `alloc_snap`.

### The allocator's books

**What a book copies.** Every buy the paper copied of the wallets it picked (one per wallet and
market in 6 h), whatever the trader's size; the event keeps the trader's order in dollars (its
fills of one outcome within 2 s) as `wallet_usd`. The prior, the backtest's per wallet sums,
counts copies by the same rule (`research/build2.py`), so a wallet is ranked on what the book
copies.

**Who a book copies.** No filter is common. Each pick has its own gate (a minimum record),
ranks the wallets through it by its score, daily, on the prior pooled with the paper's closed
lots of 30 days, and takes its top N. One rule chose each score's gate (`research/`): of nine
gates, the one with the best worst result over lags 7 and 21 days, both halves of the period
and top 10 and 30; within a point of it, the widest. The gates, of 14,254 wallets:

| score | gate | wallets |
|---|---|---|
| EXPDD, TSTAT, PNL, PNLSQRTN | bets on 30+ of the backtest's 90 days | 9,038 |
| PF | 45+ copies | 12,520 |
| CALMAR, SHRUNK | none | 14,254 |

The gates were chosen on the backtest of orders of $100+ and kept when the books went back to
copying every buy. Each pick but the random ones and EXPDD_ANY_30 also
runs as a `_STRICT` twin with the allocator's first gate (100+ copies at ROI > 5%, pooled ROI
above 0; up to 1,189 wallets): soft against strict on paper.

The picks: EXPDD 10/30/50, CALMAR 10/30, PNL 10/30, PNLSQRTN 10/30, PF 30/50, TSTAT 30, SHRUNK 30,
on the bets bought at 0.20-0.50 PF_MID_30 and CALMAR_MID_30, below 0.95 PF_NO95_30 and
CALMAR_NO95_10/30, below 0.50 PF_DOG_30; the controls RANDOM_10/30/50 (from every wallet),
RANDOM_ELIG_30 and ELIGIBLE (the 30-day gate's) and EXPDD_ANY_30 (EXPDD with no gate). 25
picks and 21 strict twins, each in three modes of risk: `W4` (4 copies of a wallet open at
most, an event 3, a copy = $1000 / the book's ceiling of copies open), `W2` (the same at 2)
and `1USD` ($1 a copy, no caps): 138 books.

**How a book is judged** (fixed before the results). `copytrader alloc` reports each book
since its rules began: the closed copies' ROI with its 95% bounds and in each half of the
period, the result with the open lots at today's price, the worst drawdown of the balance
(from the 6-hourly snapshots); then soft gates against strict, scores against RANDOM of as
many wallets, the gate on EXPDD, and the modes. A book is worth following when, after **14+
days**, it has **300+ closed copies**, the **95% lower bound of its ROI is above 0**, its ROI
is **above its RANDOM's** of as many wallets, **both halves** are in profit and its **drawdown
stayed under $200**; of those, the highest lower bound first. One week is not enough: at a few
hundred copies a book's ROI is known to about +-5-10 points.

```
DATABASE_URL=postgresql://... target/release/copytrader alloc    # each book in dollars
```

## Round one: the big players

Every 24 h (the first 2 min after the start) the run gathers candidates: the 50 biggest
holders of each outcome and the wallets of the last 1000 trades of $100+ in the ~400 markets
traded most over the last day (books of $10k+), and the top 500 of Polymarket's leaderboards
(every category, month and all-time). A wallet passes when it trades often, 20+ trades in
the last week (`--scout-min-week-trades`), and made real money: $50k+ over the last 4
months (`--scout-min-pnl`), from the profit history behind its profile chart. Trade sizes
are not judged: an active trader's buy is often dozens of small fills.

Each that passes also gets its record, **metric B**, from the same history: age, profit
over the last 4 months and each month's, months and weeks up, the longest runs of months and
weeks up, the current run of weeks up, its best day. B decides nothing in round one; it is
read again after a profitable week on paper.

Every wallet judged goes to `scouted` (with why it failed), and is judged again after 7
days. `copytrader scout` runs round one now and prints the verdicts without saving anything.

## Round two: paper, checked daily

A wallet that passes goes on trial (`trials`) and is copied on paper from then on (one
already copied starts its trial up to 7 days back, so those copies count). Once a week on
trial it is checked **every day** on its **last 7 days** (rolling, not calendar weeks): the
copies of the positions it opened in them (sold, settled, or held at the market's price
now); its own profit in them is kept next to them.

- Copies in profit on 3+ copies: B, read again, tells steady from lucky. **Steady** (traded
  120+ days; 3 of the last 4 months up and the 4 months in profit; 60%+ of their weeks up;
  best day under half the profit) goes on the **golden list**; **lucky** is dropped.
- A wallet younger than 4 months (`--scout-min-age-days 120`) has too short a record: it is
  checked from day 14 on its last 14 days instead, and copies in profit put it on the
  golden list.
- Otherwise it stays on trial; a week past its first check without one that worked it is
  `review` (look at what it trades), two weeks past it `rotated` and no longer followed.

Golden wallets stay on paper and are checked daily too: their last 7 days of copies in loss
and the 7 before them as well send one back to a new trial. The last 60 checks (and B when
read) are kept in `trials.weeks`.

On the first start with trials, the previous version's live top-10 (best paper copies of
the last 3 days) is saved as the `seed`: live copies it while the golden list is empty, and
it is retired once a wallet is golden. Those of it that pass round one start a trial.

## Live (real orders)

The same service also copies for real once the account's keys are in its variables (next to
`DATABASE_URL`; nothing else to change):

- `PRIVATE_KEY` (or `POLYMARKET_PRIVATE_KEY`): the email/Magic account's exported key.
- `RELAYER_API_KEY`, `RELAYER_ADDRESS`: polymarket.com, Settings -> Relayer API keys (the
  relayer pays the redeems' gas). Builder keys (`POLY_BUILDER_API_KEY`, `_SECRET`,
  `_PASSPHRASE`) work instead.
- Optional: `POLYMARKET_FUNDER` (the proxy wallet, derived from the key otherwise),
  `POLYGON_RPC`, `LIVE=off` to leave the keys unused.

The log says `live: ON, signer .. proxy wallet .. cash $..` at the start (or why it is off;
the paper copy runs either way). `copytrader check` shows the account and cash without an
order, `copytrader test-trade TOKEN [USD]` makes a real buy and sells it back.

Live copies the golden wallets (at most `--top` 10, the best copies of the last 7 days first),
picked again every 6 h, with its own trade stream and read-back (a 10-wallet round takes seconds);
wallets whose lots it holds stay followed for their sales. Buys are FAK market orders capped
at up to 3 cents a share over the wallet's price (`--live-cap-abs 0.03`; the paper cap when
that is wider), skipped when the cash is short; sales are FAK, never more than the
account holds; positions are held to resolution, then redeemed the way polymarket.com does
it (the proxy wallet calls Polymarket's collateral adapter, `redeemPositions(pUSD, 0,
condition, [1, 2])`, through `relayer-v2.polymarket.com`: no POL needed), tried again every
5 minutes if it fails. Events go to `live_events` and DIR/live, the state to `state` id
`live`; `copytrader report --live` reports on them.

### Tables

- `events`: one row per event (`kind` buy / sell / settle, `wallet`, `token`, `title`, the
  full event in `data` jsonb). The view `buys` lays the buys out in columns (phase, missed,
  wallet price, fill price, usd, fee, vs_wallet_bps, detect_s).
- `state`: the engine's state (`id = 'main'`); `saved_at` doubles as a heartbeat.
- `wallets`: the Polyfox wallets followed (with --polyfox), with first and last seen.
- `scouted`: round one's verdicts; `trials`: round two (status, weeks).

## Copy rules

They come from a live $1 copy test (2026-10-01, ~130 real buys):

- **Buys at market, $1.** The asks up to 2% over the wallet's price (at least one tick)
  are taken; what the book lacks there is missed. Limit orders at the wallet's price were
  dropped: they mostly filled when the wallet was wrong (live: -53% vs market +2%).
- **One retry**: if under half filled, the book is read again a second later and the rest
  taken if it is back under the cap.
- **Shadow**: every miss is also priced uncapped, to see what the cap saves or costs.
- **One buy per wallet and market per 6 h**; fills of one order within 5 s are one trade.
- **Late = missed**: a buy the stream dropped and the REST read-back found more than 60 s
  after the trade is counted as missed.
- **Sales follow the wallet**: the share it sold, from its position after the sale; once the
  positions API has caught up (90 s, it lags ~1 min) ours is set to the share of its peak it
  kept. Under $1 and not most of the position: held (exchange minimum).
- **Settlement** at the final price (0, 0.5 or 1) once the market closes.
- **Fees**: the market's taker fee, rate x (p(1-p))^exponent per share, on every fill.

## Data

- `data/events.jsonl`: one line per copied buy (fill, cap, shadow, latency, pre-match /
  in-play), sale and settlement.
- `data/state.json`: open lots per wallet and outcome.
- `data/wallets.json`: the wallets followed.
- `data/report_wallets.csv`: written by `report`.

Trades come from Polymarket's real-time stream (`wss://ws-live-data.polymarket.com`,
activity/trades), plus a REST read-back of each wallet in turn (the stream drops trades
without disconnecting).
