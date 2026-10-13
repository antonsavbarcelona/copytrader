# research: where the allocator's numbers come from

The scripts behind the allocator's prior, gates and ceilings (`src/allocator.rs`). They run in
one working directory that holds the caches they build (not in the repo: gigabytes): `act/`
(each wallet's 90 days of trades from the data API) and `final.pkl` (each token's final or
last price).

1. `run_all.py CANDS.pkl` (with `sim.py`): fetches the trade histories into `act/` and the
   prices into `final.pkl` (and runs the first backtest).
2. `build2.py ../wallets/all.txt`: the prior as the books copy. A trader's order is its fills of
   one outcome within 2 s of the first (what the copier has seen when its own order lands);
   one copy per wallet and market in 6 h (as the engine does). Writes `rows2.pkl` (every
   copy).
3. `stats.py`: the per wallet sums the allocator reads, from `rows2.pkl` (`prior.csv` =
   `wallets/backtest_stats.csv`): copies, markets, result, drawdown, PNL30, Sharpe, Sortino,
   weeks in profit. (Steps 4-6 chose the gates and
   ceilings of the allocator before generation 1; it does not use them.)
4. `ROWS=rows2.pkl SUF=_v2 py alloc_gates.py 7` and `... 21`: the walk-forward of 7 scores
   under 9 gates (each day, the stats of the 60 days before the lag, the top 10 / 30 copied).
5. `SUF=_v2 py gate_pick.py`: one rule picks each score's gate: the best worst cell (lags 7
   and 21, first and second half, top 10 and 30); within 1 point, the widest gate; a gate
   that let nobody through in some cell cannot be judged and is left out.
6. `py alloc_ceil2.py`: the prod picks with those gates, day by day, under the W4 / W2 caps:
   each book's ceiling of copies open (the worse lag's peak + 30%) and its history at the
   formula stake.

Days and copies in the walk-forward are of a 60-day lookback, the prior's of 90: the gates
scale by 1.5 (20 days -> 30, 30 copies -> 45).

## The allocator's grid on the backtest (generation 1)

1. `py grid_picks.py 7`: each day's walk-forward picks of the 22 scores and draws x 5 sizes, on
   the copies entered before the day - 7.
2. `py market_meta.py grid_picks_7.pkl`: each market those copies are in, its close time and
   event (gamma).
3. `FIXED=1 py grid_bt.py 7` (`MINP=0.05`: without the copies under 0.05): the 990 books under
   the allocator's rules, each score's place among the 10 random draws.

2026-10-13: every score came out 9-10 of 10, the books +500-2,300% in 60 days. Not believed:
the same picked wallets on the same days (Oct 3-10) made +29.9% a copy in the backtest and
-16.5% on paper (-96% under 0.05), where only 26% of their buys filled. The backtest's copy
model has to be calibrated on the paper before its grid means anything.
