"""Backtest of copying one wallet, from its own trade history (data-api /activity).

decisions  the wallet's fills of one outcome are one decision while each comes within
           JOIN_S of the last (a big buy is often dozens of fills); its size is their sum.
copy       $1 at the decision's first fill price + SLIP, the taker fee rate*p*(1-p) per
           share; a decision is copied once (its later fills are the same idea).
miss       the fill model (fill_prob) gives the chance our order (~1 s later) is under the cap;
           the PnL is weighted by it, minus the measured adverse-selection haircut.
exit       a sale of the outcome sells the same share of ours at its price - SLIP (fee
           too); what is left is settled at the final price, or marked at the last price.
"""
import json, os, pickle, time, urllib.request, urllib.error
import numpy as np

JOIN_S = 600
SLIP = 0.003
FEE_RATE = 0.045
ACT = 'act'
MAX_FILLS = 60000  # over 90 days: ~700 fills a day, a bot


class TooMany(Exception):
    pass
os.makedirs(ACT, exist_ok=True)


def get(u):
    for i in range(8):
        try:
            return json.load(urllib.request.urlopen(urllib.request.Request(u, headers={'User-Agent': 'Mozilla/5.0'}), timeout=90))
        except urllib.error.HTTPError as e:
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(1 + 2 * i); continue
            raise
        except Exception:
            time.sleep(1 + 2 * i)
    raise RuntimeError(u)


def activity(w, start, end):
    """Every TRADE of the wallet in [start, end], oldest first (cached)."""
    p = f'{ACT}/{w}_{start}_{end}.pkl'
    if os.path.exists(p):
        return pickle.load(open(p, 'rb'))
    rows = []

    def slice_(s, e):
        got = []
        off = 0
        while off <= 10000:
            x = get(f'https://data-api.polymarket.com/trades?user={w}&takerOnly=false&limit=10000&offset={off}&start={s}&end={e}')
            got += x
            if len(x) < 10000:
                rows.extend(got)
                return
            off += 10000
        if e - s <= 60:
            rows.extend(got); return
        if len(rows) + len(got) > MAX_FILLS:
            raise TooMany()
        m = (s + e) // 2
        slice_(s, m); slice_(m + 1, e)

    try:
        slice_(start, end)
    except TooMany:
        pickle.dump(None, open(p, 'wb'))
        return None
    seen, out = set(), []
    for y in rows:
        k = (y['transactionHash'], y['asset'], y['side'], y['size'], y['price'])
        if k in seen:
            continue
        seen.add(k)
        out.append((int(y['timestamp']), y['side'], float(y['price']), float(y['size']), float(y.get('usdcSize') or float(y['price']) * float(y['size'])),
                    y['asset'], y['conditionId'], y.get('title', ''), y.get('slug', ''), y.get('eventSlug', '')))
    out.sort()
    pickle.dump(out, open(p, 'wb'))
    return out


def fee(p):
    return FEE_RATE * (1 - p)


def decisions(trades, join_s=JOIN_S):
    """BUY and SELL decisions per outcome: (t0, side, token, cond, first price, vwap, shares, usd, n fills, last t, title, slug)."""
    out = []
    open_ = {}
    for t, side, p, sz, usd, tok, cond, title, slug, ev in trades:
        k = (tok, side)
        d = open_.get(k)
        if d and t - d['last'] <= join_s:
            d['shares'] += sz; d['usd'] += usd or p * sz; d['n'] += 1; d['last'] = t
            d['pmax'] = max(d['pmax'], p); d['pmin'] = min(d['pmin'], p)
            d['p1s'] = d['p1s'] if t - d['t0'] > 1 else max(d['p1s'], p) if side == 'BUY' else min(d['p1s'], p)
            continue
        if d:
            out.append(d)
        open_[k] = dict(t0=t, side=side, token=tok, cond=cond, p0=p, p1s=p, pmax=p, pmin=p, shares=sz, usd=usd or p * sz,
                        n=1, last=t, title=title, slug=slug, ev=ev)
    out += open_.values()
    out.sort(key=lambda d: d['t0'])
    for d in out:
        d['vwap'] = d['usd'] / d['shares'] if d['shares'] else d['p0']
    return out


def simulate(trades, final, fill_prob=lambda d: 1.0, haircut=0.0, join_s=JOIN_S, market_gap=0):
    """Copies of the wallet's decisions. `final`: token -> final price (settled) or last price
    (open; marked). Returns one row per copied buy: dict(t0, token, cost, pnl, p, w(fill weight), ...)."""
    ds = decisions(trades, join_s)
    copied = {}  # cond -> last copy time
    pos = {}   # the wallet's shares per token (from the trades we see)
    ours = {}  # token -> list of open copies (index into rows)
    rows = []
    for d in ds:
        tok = d['token']
        if d['side'] == 'BUY':
            before = pos.get(tok, 0.0)
            pos[tok] = before + d['shares']
            if market_gap and d['t0'] - copied.get(d['cond'], -1e18) <= market_gap:
                continue
            copied[d['cond']] = d['t0']
            p = min(d['p0'] + SLIP, 0.999)
            w = fill_prob(d)
            r = dict(t0=d['t0'], token=tok, cond=d['cond'], title=d['title'], slug=d['slug'], ev=d['ev'], p=p, w=w,
                     their_usd=d['usd'], n=d['n'], shares=1.0 / p, cost=1.0 + fee(p), back=0.0, left=1.0 / p,
                     p0=d['p0'], pmax=d['pmax'], hold=None)
            rows.append(r)
            ours.setdefault(tok, []).append(len(rows) - 1)
        else:
            held = pos.get(tok, 0.0)
            frac = 1.0 if held <= 1e-9 else min(1.0, d['shares'] / held)
            pos[tok] = max(0.0, held - d['shares'])
            q = max(d['vwap'] - SLIP, 0.001)
            for i in ours.get(tok, []):
                r = rows[i]
                if r['left'] <= 1e-9:
                    continue
                sell = r['left'] * frac
                r['back'] += sell * q - FEE_RATE * q * (1 - q) * sell
                r['left'] -= sell
                if r['left'] <= 1e-9 and r['hold'] is None:
                    r['hold'] = d['t0'] - r['t0']
    for r in rows:
        f = final.get(r['token'])
        r['settled'] = f is not None and f[1]
        if r['left'] > 1e-9:
            if f is None:
                r['unknown'] = True
                continue
            r['back'] += r['left'] * f[0]
        r['pnl'] = (r['back'] - r['cost']) * r['w'] - haircut * r['w']
        r['cost_w'] = r['cost'] * r['w']
    return [r for r in rows if 'pnl' in r]
