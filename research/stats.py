"""The allocator's prior: per wallet sums of its backtest copies (rows2.pkl, from build2.py).

py stats.py -> prior.csv (= wallets/backtest_stats.csv), the columns the allocator reads:
  n         copies (each $1 + fee; one per wallet and market in 6 h)
  markets   distinct markets copied: the independent bets (10 copies on one market are one)
  cost, pnl, win, loss   their cost, result, the winners' and the losers' sums
  r2        the sum of squared returns (pnl / cost)
  dd        the worst drop of the running result, by day of entry
  days      days with a copy; days_up: of them in profit
  pnl30     the result of the copies entered in the last 30 days
  sharpe, sortino   of the daily result over the 90 days (days without a copy at 0), a year
  weeks     the share of weeks in profit, of the weeks with a copy"""
import numpy as np, pandas as pd

NOW = 1791676800; START = NOW - 90 * 86400
DAYS = np.arange(START // 86400, NOW // 86400)

r = pd.read_pickle('rows2.pkl')
r['day'] = (r.t0 // 86400).astype(int)
r['ret'] = r.pnl / r.cost
g = r.groupby('wallet')
s = pd.DataFrame({
    'n': g.size(), 'markets': g.cond.nunique(), 'cost': g.cost.sum(), 'pnl': g.pnl.sum(),
    'win': g.pnl.apply(lambda x: x[x > 0].sum()), 'loss': g.pnl.apply(lambda x: -x[x < 0].sum()),
    'r2': g.ret.apply(lambda x: (x ** 2).sum()),
    'pnl30': r[r.t0 >= NOW - 30 * 86400].groupby('wallet').pnl.sum(),
})
s['pnl30'] = s.pnl30.fillna(0.0)

day = r.groupby(['wallet', 'day']).pnl.sum()
cum = day.groupby(level=0).cumsum()
s['dd'] = (cum.groupby(level=0).cummax() - cum).groupby(level=0).max().clip(lower=0)
s['days'] = day.groupby(level=0).size()
s['days_up'] = (day > 0).groupby(level=0).sum()

# Every day of the 90, a day without a copy at 0.
full = day.unstack(fill_value=0.0).reindex(columns=DAYS, fill_value=0.0)
mean, sd = full.mean(axis=1), full.std(axis=1, ddof=0)
down = np.sqrt((full.clip(upper=0) ** 2).mean(axis=1))
s['sharpe'] = np.where(sd > 0, mean / sd * np.sqrt(365), 0.0)
s['sortino'] = np.where(down > 0, mean / down * np.sqrt(365), np.where(mean > 0, 99.0, 0.0)).clip(max=99.0)

week = r.groupby(['wallet', r.day // 7]).pnl.sum()
s['weeks'] = (week > 0).groupby(level=0).mean()

s = s.reset_index().rename(columns={'index': 'wallet'})
cols = ['wallet', 'n', 'markets', 'cost', 'pnl', 'win', 'loss', 'r2', 'dd', 'days', 'days_up', 'pnl30', 'sharpe',
        'sortino', 'weeks']
s[cols].to_csv('prior.csv', index=False)
print('wallets', len(s), 'in profit', (s.pnl > 0).sum(), 'of them 20+ markets', ((s.pnl > 0) & (s.markets >= 20)).sum())
