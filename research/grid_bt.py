"""The allocator's grid (src/allocator.rs, generation 1) on the backtest: each day's walk-forward
picks (grid_picks.py) copy their wallets' copies of that day (rows2.pkl) under the books' rules:
a stake of 0.5/1/2% of the capital ($10,000 + the closed positions' result), cut to the room
under everything open 100%, the event the book's limit (20/50/100%), the wallet 20%; 2
positions of a wallet in one market (its entry and one add); under 1/8 of the stake or $1
refused. A position closes when the wallet sold out (its hold) or the market closed (gamma's
closedTime, market_meta.py); still open at the end it is valued at the last price.

Each score's book gets its place among the 10 random draws of the same size, stake and limit
(how many it is ahead of, 0-10) and the README's verdict is applied to the 90-day history.
The drawdown is of the closed positions' capital day by day (with the open ones at their price
it would be deeper).

FIXED=1: the stake and the caps of the $10,000 deposit, not the capital (no compounding: the
backtest fills at the trader's price at any size). MINP=0.05: without the copies bought under
that price (a diagnosis, not a rule).

py grid_bt.py LAG -> grid_bt_<lag><suffix>.csv and the tables"""
import os, sys, pickle, heapq
from collections import defaultdict
from multiprocessing import Pool
import numpy as np, pandas as pd

LAG = int(sys.argv[1] if len(sys.argv) > 1 else 7)
NOW = 1791676800; START = NOW - 90 * 86400; D0 = START // 86400
SIZES = [10, 20, 30, 50, 100]
STAKES = [0.005, 0.01, 0.02]
CAPS = [0.2, 0.5, 1.0]
SCORES = ['PNL', 'PNL30', 'LCB', 'EXPDD', 'PNLSQRTN', 'TSTAT', 'PF', 'SHARPE', 'SORTINO', 'CALMAR', 'WEEKS', 'COMBO']
DEPOSIT, TOTAL_CAP, TRADER_CAP, ADDS = 10_000.0, 1.0, 0.2, 2
END = NOW
FIXED = os.environ.get('FIXED') == '1'
MINP = float(os.environ.get('MINP', '0'))
SUF = ('_fixed' if FIXED else '') + (f'_p{MINP}' if MINP else '')
rows = picks = None


def prepare():
    """The copies some pick takes (its wallets' of the days it picks them), with their close
    time, event and return: one small file for the workers."""
    picks = pickle.load(open(f'grid_picks_{LAG}.pkl', 'rb'))
    meta = pickle.load(open('market_meta.pkl', 'rb'))
    r = pd.read_pickle('rows2.pkl')
    r['d'] = (r.t0 // 86400).astype(int) - D0
    pairs = pd.DataFrame(list({(d, w) for (d, _, _), ws in picks.items() for w in ws}), columns=['d', 'wallet'])
    r = r.merge(pairs, on=['d', 'wallet'])
    closed = r.slug.map(lambda s: meta.get(s, (float('inf'), s))[0])
    r['event'] = r.slug.map(lambda s: meta.get(s, (0, s))[1])
    tc = np.where(r.hold.notna(), r.t0 + r.hold.fillna(0), closed)
    r['tc'] = np.maximum(np.minimum(tc, END), r.t0 + 60)
    r['ret'] = r.pnl / r.cost
    r[['d', 'wallet', 't0', 'tc', 'ret', 'cond', 'event', 'p0']].to_pickle(f'grid_rows_{LAG}.pkl')
    print('copies some pick takes', len(r), flush=True)


def init():
    global rows, picks
    picks = pickle.load(open(f'grid_picks_{LAG}.pkl', 'rb'))
    r = pd.read_pickle(f'grid_rows_{LAG}.pkl')
    r = r[r.p0 >= MINP]
    rows = {d: g for d, g in r.groupby('d')}


def stream(score, size):
    """The copies a pick takes: each day, its wallets' copies of that day, in order of time."""
    parts = []
    for d in range(30, 90):
        g = rows.get(d)
        if g is None:
            continue
        parts.append(g[g.wallet.isin(set(picks.get((d, score, size), [])))])
    x = pd.concat(parts).sort_values('t0', kind='mergesort')
    return list(zip(x.t0.values, x.tc.values, x.ret.values, x.wallet.values, x.cond.values, x.event.values))


def book(copies, stake, cap):
    capital = DEPOSIT
    heap = []  # (close time, id, risk, wallet, market, event, return)
    total = 0.0
    by_w, by_e, by_wm = defaultdict(float), defaultdict(float), defaultdict(int)
    entries = refused = cut = 0
    day_cap = {}
    for i, (t0, tc, ret, w, m, e) in enumerate(copies):
        while heap and heap[0][0] <= t0:
            t, _, st, ow, om, oe, oret = heapq.heappop(heap)
            capital += st * oret
            total -= st; by_w[ow] -= st; by_e[oe] -= st; by_wm[(ow, om)] -= 1
            day_cap[int(t // 86400)] = capital
        if by_wm[(w, m)] >= ADDS:
            refused += 1
            continue
        base = DEPOSIT if FIXED else capital
        want = stake * base
        grant = min(want, TOTAL_CAP * base - total, cap * base - by_e[e], TRADER_CAP * base - by_w[w])
        if grant < want / 8 or grant < 1.0:
            refused += 1
            continue
        cut += grant < want - 1e-9
        entries += 1
        heapq.heappush(heap, (tc, i, grant, w, m, e, ret))
        total += grant; by_w[w] += grant; by_e[e] += grant; by_wm[(w, m)] += 1
    # The positions closed by the end, then the rest at their last price.
    final = capital
    while heap:
        t, _, st, *_, oret = heapq.heappop(heap)
        final += st * oret
        if t < END:
            capital += st * oret
            day_cap[int(t // 86400)] = capital
    eq, peak, dd = DEPOSIT, DEPOSIT, 0.0
    for d in range(D0 + 30, D0 + 90):
        eq = day_cap.get(d, eq)
        peak = max(peak, eq)
        dd = max(dd, 1 - eq / peak)
    return final / DEPOSIT - 1, dd, entries, cut, refused


def run_pick(arg):
    score, size = arg
    c = stream(score, size)
    out = []
    for st in STAKES:
        for cap in CAPS:
            ret, dd, n, cut, ref = book(c, st, cap)
            out.append(dict(score=score, size=size, stake=st, cap=cap, ret=ret, dd=dd, entries=n, cut=cut, refused=ref,
                            copies=len(c)))
    return out


if __name__ == '__main__':
    if not os.path.exists(f'grid_rows_{LAG}.pkl') or 'p0' not in pd.read_pickle(f'grid_rows_{LAG}.pkl').columns:
        prepare()
    jobs = [(s, n) for s in SCORES + [f'RANDOM{k}' for k in range(1, 11)] for n in SIZES]
    res = []
    with Pool(8, initializer=init) as pool:
        for i, o in enumerate(pool.imap_unordered(run_pick, jobs)):
            res.extend(o)
            print(i, o[0]['score'], o[0]['size'], flush=True)
    df = pd.DataFrame(res)
    df.to_csv(f'grid_bt_{LAG}{SUF}.csv', index=False)
    rnd = df[df.score.str.startswith('RANDOM')]
    key = ['size', 'stake', 'cap']
    ctl = rnd.groupby(key).ret.apply(list).to_dict()
    sc = df[~df.score.str.startswith('RANDOM')].copy()
    sc['place'] = [sum(1.0 if r > x else 0.5 if r == x else 0.0 for x in ctl[(n, s, c)])
                   for r, n, s, c in zip(sc.ret, sc['size'], sc.stake, sc.cap)]
    pd.set_option('display.width', 200)
    print(f'\nlag {LAG}{SUF}: mean place among the 10 random draws (0-10, a random pick 5), the 9 stakes and limits per size')
    t = sc.pivot_table(index='score', columns='size', values='place', aggfunc='mean')
    t['mean'] = sc.groupby('score').place.mean()
    t['sizes>5'] = (t[SIZES] > 5).sum(axis=1)
    t['median ret %'] = sc.groupby('score').ret.median() * 100
    t['median dd %'] = sc.groupby('score').dd.median() * 100
    t['verdict'] = np.where((t['mean'] >= 7.5) & (t['sizes>5'] >= 4), 'WORKS', '')
    print(t.sort_values('mean', ascending=False).round(1).to_string())
    print('\nrandom draws: median return %.1f%%, median dd %.1f%%' % (rnd.ret.median() * 100, rnd.dd.median() * 100))
    print('\nstake / event limit: mean return / median dd, scores | random')
    for st in STAKES:
        for cap in CAPS:
            a = sc[(sc.stake == st) & (sc.cap == cap)]
            b = rnd[(rnd.stake == st) & (rnd.cap == cap)]
            print(f'{st * 100:>4}% / {cap * 100:>3.0f}%   {a.ret.mean() * 100:+6.1f}% / {a.dd.median() * 100:4.1f}%   |   '
                  f'{b.ret.mean() * 100:+6.1f}% / {b.dd.median() * 100:4.1f}%')
