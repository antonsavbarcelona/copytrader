"""The backtest over every candidate wallet: 90 days of its trades, copied $1 per decision,
judged in-sample (the first 60 days) and out-of-sample (the last 30)."""
import json, os, pickle, sys, time, re
import concurrent.futures as cf
import pandas as pd, numpy as np
import sim

NOW = 1791676800            # 2026-10-11 00:00 UTC
START = NOW - 90 * 86400
SPLIT = NOW - 30 * 86400
HAIRCUT = 0.022             # measured: the backtest over the paper copies, per $ copied
MIN_DECISION = 100          # copy decisions of $100+

cands = pd.read_pickle(sys.argv[1] if len(sys.argv) > 1 else 'cands.pkl')
wallets = list(cands.wallet)
print('wallets', len(wallets), flush=True)

# 1. histories
def fetch(w):
    try:
        a = sim.activity(w, START, NOW)
        return w, -2 if a is None else len(a)
    except Exception as e:
        return w, -1
t0 = time.time()
n_ok = 0
with cf.ThreadPoolExecutor(int(os.environ.get('THREADS', 8))) as ex:
    for i, (w, n) in enumerate(ex.map(fetch, wallets)):
        n_ok += n >= 0
        if i % 500 == 0:
            print(f'history {i}/{len(wallets)} {time.time() - t0:.0f}s', flush=True)
print('histories', n_ok, flush=True)

# 2. final prices of every token traded
tokens = set()
for w in wallets:
    p = f'{sim.ACT}/{w}_{START}_{NOW}.pkl'
    if os.path.exists(p):
        tokens.update(r[5] for r in (pickle.load(open(p, 'rb')) or []))
final = pickle.load(open('final.pkl', 'rb')) if os.path.exists('final.pkl') else {}
todo = [t for t in tokens if t not in final]
print('tokens', len(tokens), 'to price', len(todo), flush=True)
def price(chunk):
    out = {}
    q = '&'.join(f'clob_token_ids={t}' for t in chunk)
    for closed in ('true', 'false'):
        try:
            ms = sim.get(f'https://gamma-api.polymarket.com/markets?{q}&closed={closed}&limit=100')
        except Exception:
            continue
        for m in ms:
            toks = json.loads(m.get('clobTokenIds') or '[]')
            px = json.loads(m.get('outcomePrices') or '[]')
            done = bool(m.get('closed')) and m.get('umaResolutionStatus', 'resolved') in ('resolved', None, '') or bool(m.get('closed'))
            for t, p in zip(toks, px):
                out[t] = (float(p), bool(m.get('closed')))
    return out
with cf.ThreadPoolExecutor(8) as ex:
    for i, o in enumerate(ex.map(price, [todo[i:i + 40] for i in range(0, len(todo), 40)])):
        final.update(o)
        if i % 200 == 0:
            pickle.dump(final, open('final.pkl', 'wb'))
pickle.dump(final, open('final.pkl', 'wb'))
print('priced', len(final), flush=True)

# 3. per wallet: style and the copies
UPDOWN = re.compile(r'up or down', re.I)
def cat(slug, title):
    s = (slug or '') + ' ' + (title or '').lower()
    if UPDOWN.search(title or ''):
        return 'crypto_updown'
    if any(k in s for k in ('btc', 'bitcoin', 'eth', 'solana', 'xrp', 'crypto')):
        return 'crypto'
    if any(k in s for k in ('nfl', 'nba', 'mlb', 'nhl', 'epl', 'ucl', 'soccer', 'fc ', ' vs', 'tennis', 'atp', 'wta', 'ufc', 'cs2', 'lol', 'val', 'dota', 'game', 'match', 'win on')):
        return 'sports'
    if any(k in s for k in ('election', 'president', 'trump', 'senate', 'house', 'governor', 'nominee', 'prime minister', 'fed ', 'rate')):
        return 'politics'
    return 'other'

def judge(w):
    p = f'{sim.ACT}/{w}_{START}_{NOW}.pkl'
    if not os.path.exists(p):
        return None
    tr = pickle.load(open(p, 'rb'))
    if tr is None:
        return dict(wallet=w, bot=True)
    if len(tr) < 20:
        return None
    ds = sim.decisions(tr)
    buys = [d for d in ds if d['side'] == 'BUY']
    sells = [d for d in ds if d['side'] == 'SELL']
    if len(buys) < 20:
        return None
    # both sides of one market within an hour: market making / hedging
    by_cond = {}
    both = 0
    for d in buys:
        prev = by_cond.get(d['cond'])
        if prev and prev[0] != d['token'] and d['t0'] - prev[1] < 3600:
            both += 1
        by_cond[d['cond']] = (d['token'], d['t0'])
    p0 = np.array([d['p0'] for d in buys])
    usd = np.array([d['usd'] for d in buys])
    cats = pd.Series([cat(d['slug'], d['title']) for d in buys]).value_counts(normalize=True)
    rows = sim.simulate(tr, final, haircut=HAIRCUT)
    rows = [r for r in rows if r['their_usd'] >= MIN_DECISION]
    holds = [r['hold'] for r in rows if r['hold'] is not None]
    out = dict(wallet=w, fills=len(tr), buys=len(buys), sells=len(sells), both_sides=both / len(buys),
               median_bet=float(np.median(usd)), p90_bet=float(np.quantile(usd, .9)), volume=float(usd.sum()),
               share_95=float((p0 >= .95).mean()), share_05=float((p0 <= .05).mean()), mean_price=float(p0.mean()),
               hold_med_min=float(np.median(holds) / 60) if holds else np.nan, sold_share=len(holds) / max(len(rows), 1),
               cat=cats.index[0], cat_share=float(cats.iloc[0]), updown=float(cats.get('crypto_updown', 0)),
               last=max(d['t0'] for d in ds))
    r = pd.DataFrame(rows)
    if r.empty:
        return out
    r['ret'] = r.pnl / r.cost
    for name, part in [('is', r[r.t0 < SPLIT]), ('oos', r[r.t0 >= SPLIT]), ('all', r)]:
        out[f'{name}_n'] = len(part)
        out[f'{name}_cost'] = float(part.cost.sum())
        out[f'{name}_pnl'] = float(part.pnl.sum())
        out[f'{name}_roi'] = float(part.pnl.sum() / part.cost.sum()) if len(part) else np.nan
    # per-month copies' PnL
    r['month'] = ((r.t0 - START) // (30 * 86400)).astype(int)
    m = r.groupby('month').pnl.sum()
    out['months_up'] = int((m > 0).sum())
    for k in range(3):
        out[f'm{k}_pnl'] = float(m.get(k, 0.0))
    # bootstrap lower bound of the ROI (5%)
    rng = np.random.default_rng(0)
    pn, co = r.pnl.values, r.cost.values
    if len(r) >= 10:
        idx = rng.integers(0, len(r), (500, len(r)))
        boots = pn[idx].sum(1) / co[idx].sum(1)
        out['roi_p05'] = float(np.quantile(boots, .05))
    out['open_share'] = float((~r.settled).mean())
    return out

res = []
if True:
    for i, o in enumerate(map(judge, wallets)):
        if o:
            res.append(o)
        if i % 2000 == 0:
            print(f'judged {i}', flush=True)
out = pd.DataFrame(res).merge(cands, on='wallet', how='left', suffixes=('', '_mk'))
out.to_pickle('results.pkl')
print('done', len(out), flush=True)
