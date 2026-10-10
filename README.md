# copytrader

Paper copy-trading of the Polyfox smart wallets on Polymarket: every buy and sale of every
wallet on the [Polyfox leaderboard](https://polyfox.co/smart-wallets) (~450, read again every
6 h) is copied on paper against the live order book, the way a real $1 copy would fill.

```
cargo build --release
target/release/copytrader run    --data data            # runs until stopped
target/release/copytrader report --data data [--hours 24]
```

Needs Rust 1.85+ (`rust-version` in Cargo.toml; the lock file is resolved for it).
The running paper copy uses a copy of the binary (`data/bin/copytrader.exe`), so a rebuild
does not fail on a locked `target/release/copytrader.exe`.

`run` options: `--stake 1` (USD per buy), `--cap 0.02` (max price over the wallet's),
`--sell-floor 0.05`, `--market-gap-h 6`.

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

## Crypto traders and the golden list

Every 6 h the run also reads Polymarket's crypto leaderboard (week and month, top 100) and
adds to the paper copy every wallet with 10+ closed positions in the short crypto markets
("Bitcoin Up or Down", ETH, SOL, XRP ...): no other filter, bots included. They are saved in
`wallets` (`source = 'crypto'`, their own record in `stats`) and followed from then on.
`copytrader discover` shows who a scan would add, without saving anything.

The golden list (`golden` table, `active`) is the wallets followed on paper for 7 days or more
whose copies of the last 7 days closed 30+ times in profit (`--golden-days`,
`--golden-min-closed`); Polyfox wallets go by the same rule. The live copy picks its wallets
from it only.

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

Live copies only the `--top` (10) golden wallets whose paper copies returned the most over the last
`--top-days` (3; closed positions, at least `--top-min-closed` 15, positive), picked again
every 6 h, with its own trade stream and read-back (a 10-wallet round takes seconds);
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
- `wallets`: the Polyfox wallets followed, with first and last seen.

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
