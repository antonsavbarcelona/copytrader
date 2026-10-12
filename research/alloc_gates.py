"""Walk-forward: which entry gate suits each score. No common gate: every wallet with a record.
Each day d: stats of [d - LAG - 60, d - LAG), each (score, gate) ranks the wallets passing its gate,
the top N copied on day d. py alloc_gates.py LAG -> alloc_gates_<lag>.pkl and a table."""
import sys, os
import numpy as np, pandas as pd

LAG = int(sys.argv[1] if len(sys.argv) > 1 else 7) * 86400
LOOK = 60 * 86400
GAP, EVENT_CAP = 6 * 3600, 3

ROWS = os.environ.get('ROWS', 'rows.pkl'); SUF = os.environ.get('SUF', '')
r = pd.read_pickle(ROWS)
r = r[r.usd >= 100].copy()
r['day'] = (r.t0 // 86400).astype(int)
r['event'] = r.title.str.split(':').str[0].str.strip()
r['ret'] = r.pnl / r.cost
days = sorted(r.day.unique())
d0 = days[0] + 30
rng = np.random.default_rng(0)
r['pos'] = r.pnl.clip(lower=0)
r['neg'] = (-r.pnl).clip(lower=0)
r['r2'] = r.ret ** 2
wd = r.groupby(['wallet', 'day']).agg(n=('pnl', 'size'), cost=('cost', 'sum'), pnl=('pnl', 'sum'), r2=('r2', 'sum'),
                                       win=('pos', 'sum'), loss=('neg', 'sum')).reset_index()
by_day = {d: g for d, g in r.groupby('day')}


def stats(d):
    hi, lo = d - LAG // 86400, d - (LAG + LOOK) // 86400
    w = wd[(wd.day < hi) & (wd.day >= lo)].sort_values(['wallet', 'day'])
    g = w.groupby('wallet')
    s = g[['n', 'cost', 'pnl', 'r2', 'win', 'loss']].sum()
    s['days'] = g.size()
    cum = g.pnl.cumsum()
    s['dd'] = (cum.groupby(w.wallet).cummax() - cum).groupby(w.wallet).max().clip(lower=0)
    s['roi'] = s.pnl / s.cost
    mean = s.pnl / s.n
    s['se'] = np.sqrt(np.maximum(s.r2 / s.n - mean ** 2, 1e-6)) / np.sqrt(s.n)
    return s


SCORES = {
    'SHRUNK': lambda s: s.pnl / (s.cost + 50),
    'PNL': lambda s: s.pnl,
    'TSTAT': lambda s: (s.pnl / s.n) / s.se,
    'CALMAR': lambda s: s.pnl / np.maximum(s.dd, 5),
    'EXPDD': lambda s: (s.pnl / s.n) / np.maximum(s.dd, 5),
    'PNLSQRTN': lambda s: s.pnl / np.sqrt(s.n),
    'PF': lambda s: (s.win + 5) / (s.loss + 5),
}
# days are of the 60-day lookback: 20 of 60 is the image's 30 of 90
GATES = {
    'none': lambda s: s.n >= 1,
    'n30': lambda s: s.n >= 30,
    'n100': lambda s: s.n >= 100,
    'd10': lambda s: s.days >= 10,
    'd20': lambda s: s.days >= 20,
    'd40': lambda s: s.days >= 40,
    'n30+': lambda s: (s.n >= 30) & (s.roi > 0),
    'd20+': lambda s: (s.days >= 20) & (s.roi > 0),
    'n100r5': lambda s: (s.n >= 100) & (s.roi > 0.05),
}
NS = (10, 30)


def caps(x):
    x = x.sort_values('t0')
    keep, last, ev = [], {}, {}
    for i, t, sl, e in zip(x.index, x.t0, x.slug, x.event):
        if t - last.get(sl, -1e18) <= GAP or ev.get(e, 0) >= EVENT_CAP:
            continue
        last[sl] = t
        ev[e] = ev.get(e, 0) + 1
        keep.append(i)
    return x.loc[keep]


def take(today, ws):
    x = caps(today[today.wallet.isin(ws)])
    return len(x), x.cost.sum(), x.pnl.sum()


res = {}
for d in days:
    if d < d0 or d not in by_day:
        continue
    s = stats(d)
    today = by_day[d]
    t = today[today.wallet.isin(s.index)]
    res.setdefault(('ALL', '', 0), []).append((d, *take(t, s.index)))
    for gn, gf in GATES.items():
        el = s[gf(s)]
        res.setdefault(('ELIGIBLE', gn, 0), []).append((d, *take(t, el.index)))
        for n in NS:
            k = [take(t, rng.choice(el.index, min(n, len(el)), replace=False)) for _ in range(10)] if len(el) else [(0, 0, 0)]
            res.setdefault(('RANDOM', gn, n), []).append((d, *np.mean(k, axis=0)))
        for sc, sf in SCORES.items():
            order = sf(el).sort_values(ascending=False).index
            for n in NS:
                res.setdefault((sc, gn, n), []).append((d, *take(t, order[:n])))
    if d % 10 == 0:
        print(pd.to_datetime(d * 86400, unit='s').date(), flush=True)

out = {k: pd.DataFrame(v, columns=['day', 'n', 'cost', 'pnl']).set_index('day') for k, v in res.items()}
pd.to_pickle(out, f'alloc_gates{SUF}_{LAG // 86400}.pkl')
half = len(out[('ALL', '', 0)]) // 2
roi = lambda df: df.pnl.sum() / max(df.cost.sum(), 1e-9)
print(f'\nlag {LAG // 86400}d; ROI (first half / second half), copies')
for sc in ['ALL', 'ELIGIBLE', 'RANDOM'] + list(SCORES):
    for k in sorted(k for k in out if k[0] == sc):
        df = out[k]
        print(f'{k[0]:9} {k[1]:7} {k[2]:3}  copies {int(df.n.sum()):7}  ROI {roi(df):+7.2%}  | '
              f'{roi(df.iloc[:half]):+7.2%} / {roi(df.iloc[half:]):+7.2%}')
