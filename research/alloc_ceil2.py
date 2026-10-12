"""The prod books on the v2 rows (orders of $100+, as the books copy): each day the 46 picks
with their own gates (in the 60-day lookback's units: 30 of 90 days = 20, 45 copies = 30),
their copies under the W4 / W2 caps (one wallet 4 / 2 open, an event 3, a market another
wallet brought in skipped), open until the wallet sells out or the market closes.
The ceiling: the most copies open at once, the worse of lags 7 and 21, + 30%, up to tens.
py alloc_ceil2.py  ->  ceil2.csv and a table (profit at the formula stake, max drawdown)."""
import os, sys, pickle, time, heapq, math
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import numpy as np, pandas as pd
import requests

r = pd.read_pickle('rows2.pkl')
r['day'] = (r.t0 // 86400).astype(int)
r['event'] = r.title.str.split(':').str[0].str.strip()
r['ret'] = r.pnl / r.cost
r['pos'] = r.pnl.clip(lower=0); r['neg'] = (-r.pnl).clip(lower=0); r['r2'] = r.ret ** 2
days = sorted(r.day.unique()); d0 = days[0] + 30
wd = r.groupby(['wallet', 'day']).agg(n=('pnl', 'size'), cost=('cost', 'sum'), pnl=('pnl', 'sum'), r2=('r2', 'sum'),
                                       win=('pos', 'sum'), loss=('neg', 'sum')).reset_index()
by_day = {d: g for d, g in r.groupby('day')}
LOOK = 60


def stats(d, lag):
    hi, lo = d - lag, d - lag - LOOK
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


SC = {'SHRUNK': lambda s: s.pnl / (s.cost + 50), 'PNL': lambda s: s.pnl, 'TSTAT': lambda s: (s.pnl / s.n) / s.se,
      'CALMAR': lambda s: s.pnl / np.maximum(s.dd, 5), 'EXPDD': lambda s: (s.pnl / s.n) / np.maximum(s.dd, 5),
      'PNLSQRTN': lambda s: s.pnl / np.sqrt(s.n), 'PF': lambda s: (s.win + 5) / (s.loss + 5)}
G = {'any': lambda s: s.n >= 1, 'long': lambda s: s.days >= 20, 'n45': lambda s: s.n >= 30,
     'strict': lambda s: (s.n >= 100) & (s.roi > 0.05)}
BET = {'all': lambda x: x, 'mid': lambda x: x[(x.p0 >= .2) & (x.p0 < .5)], 'no95': lambda x: x[x.p0 < .95],
       'dog': lambda x: x[x.p0 < .5]}
# the gates gate_pick.py chose on these rows
GATE = {'EXPDD': 'long', 'TSTAT': 'long', 'PF': 'n45', 'PNL': 'long', 'PNLSQRTN': 'long', 'SHRUNK': 'any', 'CALMAR': 'any'}
PICKS = [('EXPDD', 10, 'all'), ('EXPDD', 30, 'all'), ('EXPDD', 50, 'all'), ('CALMAR', 10, 'all'), ('CALMAR', 30, 'all'),
         ('PNL', 10, 'all'), ('PNL', 30, 'all'), ('PNLSQRTN', 10, 'all'), ('PNLSQRTN', 30, 'all'), ('PF', 30, 'all'),
         ('PF', 50, 'all'), ('TSTAT', 30, 'all'), ('SHRUNK', 30, 'all'), ('PF', 30, 'mid'), ('CALMAR', 30, 'mid'),
         ('PF', 30, 'no95'), ('CALMAR', 10, 'no95'), ('CALMAR', 30, 'no95'), ('PF', 30, 'dog')]


def name(sc, n, bets):
    return f'{sc}_{"" if bets == "all" else bets.upper() + "_"}{n}'


BOOKS = {}  # name -> (kind, score, gate, n, bets)
for sc, n, bets in PICKS:
    BOOKS[name(sc, n, bets)] = ('score', sc, GATE[sc], n, bets)
    BOOKS[name(sc, n, bets) + '_STRICT'] = ('score', sc, 'strict', n, bets)
BOOKS['EXPDD_ANY_30'] = ('score', 'EXPDD', 'any', 30, 'all')
for n in (10, 30, 50):
    BOOKS[f'RANDOM_{n}'] = ('random', None, 'any', n, 'all')
BOOKS['RANDOM_ELIG_30'] = ('random', None, 'long', 30, 'all')
BOOKS['RANDOM_ELIG_30_STRICT'] = ('random', None, 'strict', 30, 'all')
BOOKS['ELIGIBLE'] = ('all', None, 'long', 0, 'all')
BOOKS['ELIGIBLE_STRICT'] = ('all', None, 'strict', 0, 'all')

cand = {}
for lag in (7, 21):
    c = {b: [] for b in BOOKS}
    for d in days:
        if d < d0 or d not in by_day:
            continue
        s = stats(d, lag)
        today = by_day[d]
        for b, (kind, sc, g, n, bets) in BOOKS.items():
            el = s[G[g](s)]
            if kind == 'all':
                ws = el.index
            elif kind == 'random':
                ws = np.random.default_rng(d * 1000 + n + len(b)).choice(el.index, min(n, len(el)), replace=False)
            else:
                ws = SC[sc](el).sort_values(ascending=False).index[:n]
            c[b].append(BET[bets](today[today.wallet.isin(ws)]))
        if d % 10 == 0:
            print(lag, pd.to_datetime(d * 86400, unit='s').date(), flush=True)
    cand[lag] = {b: pd.concat(v) for b, v in c.items()}

# close times of the markets never sold out of
CT = 'close_times.pkl'
ct = pickle.load(open(CT, 'rb')) if os.path.exists(CT) else {}
slugs = sorted({sl for c in cand.values() for x in c.values() for sl in x.slug[x.hold.isna()]} - set(ct))
print('slugs to fetch', len(slugs), flush=True)


def fetch(batch):
    out = {}
    for extra in ('&closed=true', ''):
        q = '&'.join('slug=' + s for s in batch if s not in out)
        if not q:
            break
        for k in range(5):
            try:
                resp = requests.get(f'https://gamma-api.polymarket.com/markets?{q}{extra}&limit={len(batch) * 2}', timeout=30)
                if resp.status_code == 429:
                    time.sleep(2 + k * 3); continue
                for m in resp.json():
                    t = m.get('closedTime') if m.get('closed') else None
                    out[m['slug']] = pd.Timestamp(t).timestamp() if t else float('inf')
                break
            except Exception:
                time.sleep(2)
    return out


with ThreadPoolExecutor(8) as ex:
    for i, o in enumerate(ex.map(fetch, [slugs[i:i + 40] for i in range(0, len(slugs), 40)])):
        ct.update(o)
        if i % 100 == 0:
            pickle.dump(ct, open(CT, 'wb'))
pickle.dump(ct, open(CT, 'wb'))
END = days[-1] * 86400 + 86400


def run(x, wcap):
    x = x.sort_values('t0')
    tc = np.where(x.hold.notna(), x.t0 + x.hold.fillna(0), x.slug.map(ct).fillna(np.inf).values)
    x = x.assign(tc=np.maximum(tc, x.t0 + 60))
    heap, per_w, per_e, per_m = [], Counter(), Counter(), {}
    keep, n_open, peak = [], 0, 1
    for i, t0, tc, w, sl, ev in zip(x.index, x.t0.values, x.tc.values, x.wallet.values, x.slug.values, x.event.values):
        while heap and heap[0][0] <= t0:
            _, ow, osl, oev = heapq.heappop(heap)
            per_w[ow] -= 1; per_e[oev] -= 1; per_m[osl].discard(ow)
            n_open -= 1
        if any(o != w for o in per_m.get(sl, ())) or per_e[ev] >= 3 or per_w[w] >= wcap:
            continue
        heapq.heappush(heap, (min(tc, END), w, sl, ev))
        per_w[w] += 1; per_e[ev] += 1; per_m.setdefault(sl, set()).add(w)
        n_open += 1
        peak = max(peak, n_open)
        keep.append(i)
    return x.loc[keep], peak


def money(taken, stake):
    pnl = taken.pnl * stake
    day = (np.minimum(taken.tc, END - 1) // 86400).astype(int)
    eq = 1000 + pnl.groupby(day).sum().reindex(range(d0, days[-1] + 1), fill_value=0).cumsum()
    return pnl.sum(), (eq.cummax() - eq).max()


rows = []
for b in BOOKS:
    row = {'book': b}
    for wcap in (2, 4):
        runs = {lag: run(cand[lag][b], wcap) for lag in (7, 21)}
        ceil = int(math.ceil(max(p for _, p in runs.values()) * 1.3 / 10) * 10)
        row[f'c{wcap}'] = ceil
        for lag, (taken, _) in runs.items():
            prof, dd = money(taken, 1000 / ceil)
            row[f'W{wcap}_{lag}'] = round(prof)
            row[f'W{wcap}_{lag}_dd'] = round(dd)
            row[f'W{wcap}_{lag}_roi'] = round(taken.pnl.sum() / taken.cost.sum() * 100, 1) if len(taken) else 0
    rows.append(row)
res = pd.DataFrame(rows)
res.to_csv('ceil2.csv', index=False)
pd.set_option('display.width', 250); pd.set_option('display.max_rows', 100)
print(res.to_string(index=False))
