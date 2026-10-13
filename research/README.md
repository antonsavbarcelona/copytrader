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
