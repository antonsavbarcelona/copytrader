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

## Round one: traders with a record

Every 24 h (the first 2 min after the start) the run gathers candidates: the 50 biggest
holders of each outcome and the wallets of the last 1000 trades of $100+ in the ~400 markets
traded most over the last day (books of $10k+), and the top 500 of Polymarket's leaderboards
(every category, month and all-time). Each is judged on the profit history behind its
profile chart (scout.rs). It passes when it:

- has traded for 120 days or more, and this week;
- made money in 3 of the last 4 months (30 days each), $5,000 or more over them;
- made money in 60% of those weeks;
- did not make it in one day (its best day is under half of it);
- trades for real: its median trade (last 500) is $50 or more.

Every wallet judged goes to `scouted` (with its record and why it failed), and is judged
again after 7 days. `copytrader scout` runs round one now and prints the verdicts without
saving anything.

## Round two: a week on paper

A wallet that passes goes on trial (`trials`) and is copied on paper from then on (one
already copied starts its week up to 7 days back, so those copies count). When its week is
over (checked hourly) it is judged on the copies of the positions it opened that week (sold,
settled, or held at the market's price now) and on its own profit that week:

- both in profit, on 3+ copies: **golden**;
- otherwise another week; after two such weeks it is `review` (look at what it trades; it
  gets a third week), after three it is `rotated` and no longer followed.

Golden wallets stay on paper and are judged on: two losing weeks of copies in a row send one
back to trial. Every week's numbers are kept in `trials.weeks`.

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

Live copies the golden wallets (at most `--top` 10, the best copies of last week first),
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
