"""The allocator's prior, as the books copy (v2): what a book can know at the moment it copies.

order   the wallet's fills of one outcome within ORDER_S of the first (what the copier has seen
        by the time its own order lands); its size is their sum
copy    the paper's rule: one copy per wallet and market in 6 h, except that a window a small
        order opened does not hold back a big one (the engine waits for the order's pieces)
book    takes only the copies of orders of MIN_BET+ (the trader's own money on it)

py build2.py WALLETS_FILE -> rows2.pkl (every big copy of every wallet) and prior2.csv (per
wallet sums, the columns the allocator reads: wallets/backtest_stats.csv)."""
import os, pickle, sys
import numpy as np, pandas as pd
from multiprocessing import Pool
import sim

NOW = 1791676800; START = NOW - 90 * 86400
ORDER_S, GAP, MIN_BET = 2, 6 * 3600, 100.0
final = None


def init():
    global final
    final = pickle.load(open('final.pkl', 'rb'))


def simulate(trades, haircut=0.022):
    ds = sim.decisions(trades, ORDER_S)
    win = {}   # cond -> (time the window opened, opened by a big order)
    pos, ours, rows = {}, {}, []
    for d in ds:
        tok = d['token']
        if d['side'] == 'BUY':
            pos[tok] = pos.get(tok, 0.0) + d['shares']
            big = d['usd'] >= MIN_BET
            w = win.get(d['cond'])
            if w and d['t0'] - w[0] <= GAP and (w[1] or not big):
                continue
            win[d['cond']] = (d['t0'], big)
            p = min(d['p0'] + sim.SLIP, 0.999)
            rows.append(dict(t0=d['t0'], token=tok, cond=d['cond'], title=d['title'], slug=d['slug'], p0=d['p0'],
                             usd=d['usd'], big=big, cost=1.0 + sim.fee(p), back=0.0, left=1.0 / p, hold=None))
            ours.setdefault(tok, []).append(len(rows) - 1)
        else:
            held = pos.get(tok, 0.0)
            frac = 1.0 if held <= 1e-9 else min(1.0, d['shares'] / held)
            pos[tok] = max(0.0, held - d['shares'])
            q = max(d['vwap'] - sim.SLIP, 0.001)
            for i in ours.get(tok, []):
                r = rows[i]
                if r['left'] <= 1e-9:
                    continue
                sell = r['left'] * frac
                r['back'] += sell * q - sim.FEE_RATE * q * (1 - q) * sell
                r['left'] -= sell
                if r['left'] <= 1e-9 and r['hold'] is None:
                    r['hold'] = d['t0'] - r['t0']
    out = []
    for r in rows:
        if not r['big']:
            continue
        f = final.get(r['token'])
        r['settled'] = f is not None and f[1]
        if r['left'] > 1e-9:
            if f is None:
                continue
            r['back'] += r['left'] * f[0]
        r['pnl'] = r['back'] - r['cost'] - haircut
        out.append(r)
    return out


def one(w):
    p = f'{sim.ACT}/{w}_{START}_{NOW}.pkl'
    if not os.path.exists(p):
        return w, None
    tr = pickle.load(open(p, 'rb'))
    if not tr:
        return w, None
    rows = simulate(tr)
    return w, [(r['t0'], r['p0'], r['usd'], r['cost'], r['pnl'], r['settled'], r['hold'], r['slug'], r['title'], r['cond'])
               for r in rows]


def stats(w, d):
    d = d.sort_values('t0')
    ret = d.pnl / d.cost
    day = d.groupby(d.t0 // 86400).pnl.sum()
    cum = day.cumsum()
    return dict(wallet=w, n=len(d), cost=d.cost.sum(), pnl=d.pnl.sum(), win=d.pnl[d.pnl > 0].sum(),
                loss=-d.pnl[d.pnl < 0].sum(), r2=(ret ** 2).sum(), dd=float((cum.cummax() - cum).max()),
                days=len(day), days_up=int((day > 0).sum()))


if __name__ == '__main__':
    path = sys.argv[1] if len(sys.argv) > 1 else 'all.txt'
    ws = [l.strip().lower() for l in open(path) if l.strip()]
    cols = ['t0', 'p0', 'usd', 'cost', 'pnl', 'settled', 'hold', 'slug', 'title', 'cond']
    frames, st = [], []
    with Pool(10, initializer=init) as pool:
        for i, (w, rows) in enumerate(pool.imap_unordered(one, ws, chunksize=20)):
            if rows:
                d = pd.DataFrame(rows, columns=cols)
                d.insert(0, 'wallet', w)
                frames.append(d)
                st.append(stats(w, d))
            if i % 1000 == 0:
                print(i, len(st), flush=True)
    r = pd.concat(frames, ignore_index=True)
    r.to_pickle('rows2.pkl')
    s = pd.DataFrame(st)
    s.to_csv('prior2.csv', index=False)
    print('rows', len(r), 'wallets', len(s))
