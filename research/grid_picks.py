"""Walk-forward picks of the allocator's grid (src/allocator.rs, generation 1) on the backtest's
copies (rows2.pkl): each day d of the backtest the wallets are ranked only on the copies
entered before d - LAG (most markets have settled by then), from the backtest's first day on.
The filters and scores are the allocator's: the copy in profit, 20+ markets for the scores
that need a record.

py grid_picks.py LAG -> grid_picks_<lag>.pkl: {(day, score, size): [wallets]}"""
import sys, pickle
import numpy as np, pandas as pd

LAG = int(sys.argv[1] if len(sys.argv) > 1 else 7)
NOW = 1791676800; START = NOW - 90 * 86400
D0 = START // 86400
SIZES = [10, 20, 30, 50, 100]
RATED = ['PNL', 'PNL30', 'LCB', 'EXPDD', 'PNLSQRTN', 'TSTAT', 'PF', 'SHARPE', 'SORTINO', 'CALMAR', 'WEEKS']
FIRST = 30  # the first day traded: a month of record

r = pd.read_pickle('rows2.pkl')
r['d'] = (r.t0 // 86400).astype(int) - D0
r = r[(r.d >= 0) & (r.d < 90)]
W = sorted(r.wallet.unique())
wi = {w: i for i, w in enumerate(W)}
r['w'] = r.wallet.map(wi)
r['ret'] = r.pnl / r.cost
nw, nd = len(W), 90


def grid(vals):
    a = np.zeros((nw, nd))
    np.add.at(a, (r.w.values, r.d.values), vals)
    return a


n, cost, pnl = grid(1.0), grid(r.cost.values), grid(r.pnl.values)
win, loss, r2 = grid(r.pnl.clip(lower=0).values), grid((-r.pnl).clip(lower=0).values), grid((r.ret ** 2).values)
first = r.groupby(['w', 'cond']).d.min().reset_index()
mk = np.zeros((nw, nd)); np.add.at(mk, (first.w.values, first.d.values), 1.0)
# cumulative to the end of each day: c[:, k] = days 0..k
C = {k: np.cumsum(v, axis=1) for k, v in dict(n=n, cost=cost, pnl=pnl, win=win, loss=loss, r2=r2, mk=mk).items()}
cum = C['pnl']
dd = np.maximum.accumulate(np.maximum.accumulate(cum, axis=1) - cum, axis=1)
sq = np.cumsum(pnl ** 2, axis=1)
dsq = np.cumsum(np.minimum(pnl, 0) ** 2, axis=1)
wk = np.zeros((nw, 13)); np.add.at(wk, (r.w.values, r.d.values // 7), r.pnl.values)
wk_any = np.zeros((nw, 13)); np.add.at(wk_any, (r.w.values, r.d.values // 7), 1.0)


def stats(h):
    """The record of the copies entered on days 0..h-1."""
    k = h - 1
    g = {x: C[x][:, k] for x in C}
    days = h
    mean = g['pnl'] / days
    sd = np.sqrt(np.maximum(sq[:, k] / days - mean ** 2, 0))
    down = np.sqrt(dsq[:, k] / days)
    weeks_done = h // 7
    pos = (wk[:, :weeks_done] > 0).sum(axis=1)
    had = (wk_any[:, :weeks_done] > 0).sum(axis=1)
    s = pd.DataFrame(dict(n=g['n'], markets=g['mk'], cost=g['cost'], pnl=g['pnl'], win=g['win'], loss=g['loss'],
                          r2=g['r2'], dd=dd[:, k], pnl30=g['pnl'] - (C['pnl'][:, k - 30] if k >= 30 else 0.0),
                          sharpe=np.where(sd > 0, mean / np.where(sd > 0, sd, 1) * np.sqrt(365), 0.0),
                          sortino=np.clip(np.where(down > 0, mean / np.where(down > 0, down, 1) * np.sqrt(365),
                                                   np.where(mean > 0, 99.0, 0.0)), None, 99.0),
                          weeks=np.where(had > 0, pos / np.maximum(had, 1), 0.0)), index=W)
    return s


def scores(s):
    nn = s.n.clip(lower=1)
    m = np.where(s.cost > 0, s.pnl / s.cost.where(s.cost > 0, 1), 0.0)
    sd = np.sqrt(np.maximum(s.r2 / nn - m ** 2, 0))
    return pd.DataFrame({
        'PNL': s.pnl, 'PNL30': s.pnl30, 'LCB': m - 2 * sd / np.sqrt(nn), 'EXPDD': s.pnl / nn / s.dd.clip(lower=1),
        'PNLSQRTN': m * np.sqrt(nn), 'TSTAT': np.where(sd > 0, m / np.where(sd > 0, sd, 1) * np.sqrt(nn), 0.0),
        'PF': np.where(s.loss > 0, (s.win / s.loss.where(s.loss > 0, 1)).clip(upper=50), 50.0),
        'SHARPE': s.sharpe, 'SORTINO': s.sortino, 'CALMAR': s.pnl / s.dd.clip(lower=1), 'WEEKS': s.weeks}, index=s.index)


out = {}
for d in range(FIRST, nd):
    s = stats(d - LAG)
    pool = s[(s.pnl > 0) & (s.n >= 1)]
    rated = pool[pool.markets >= 20]
    sc = scores(rated)
    combo = sc.rank(method='first').sub(1).div(max(len(sc), 2) - 1).mean(axis=1)
    sc_pool = scores(pool)
    for name in RATED + ['COMBO']:
        col = combo if name == 'COMBO' else (sc_pool[name] if name in ('PNL', 'PNL30') else sc[name])
        order = col.sort_values(ascending=False, kind='mergesort').index
        for k in SIZES:
            out[(d, name, k)] = list(order[:k])
    rng_pool = np.array(sorted(pool.index))
    for j in range(1, 11):
        for k in SIZES:
            g = np.random.default_rng(d * 100000 + j * 1000 + k)
            out[(d, f'RANDOM{j}', k)] = list(g.choice(rng_pool, min(k, len(rng_pool)), replace=False))
    if d % 10 == 0:
        print(d, 'pool', len(pool), 'rated', len(rated), flush=True)
pickle.dump(out, open(f'grid_picks_{LAG}.pkl', 'wb'))
ws = {w for v in out.values() for w in v}
rows = r[r.wallet.isin(ws) & (r.d >= FIRST)]
print('wallets in some book', len(ws), 'their copies', len(rows), 'slugs', rows.slug.nunique())
