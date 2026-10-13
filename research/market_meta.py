"""Each market the grid backtest copies: when it closed (gamma's closedTime; inf while open) and
its event (the first event's slug, as the allocator's Api::market reads it).

py market_meta.py grid_picks_7.pkl [grid_picks_21.pkl ...] -> market_meta.pkl {slug: (closed_at, event)}"""
import os, sys, pickle, time
from concurrent.futures import ThreadPoolExecutor
import pandas as pd, requests

OUT = 'market_meta.pkl'
D0 = (1791676800 - 90 * 86400) // 86400
r = pd.read_pickle('rows2.pkl')
r['d'] = (r.t0 // 86400).astype(int) - D0
pairs = set()
for f in sys.argv[1:]:
    for (d, _, _), ws in pickle.load(open(f, 'rb')).items():
        pairs.update((d, w) for w in ws)
pairs = pd.DataFrame(list(pairs), columns=['d', 'wallet'])
need = set(r.merge(pairs, on=['d', 'wallet']).slug)
meta = pickle.load(open(OUT, 'rb')) if os.path.exists(OUT) else {}
todo = sorted(need - set(meta))
print('markets', len(need), 'to fetch', len(todo), flush=True)


def fetch(batch):
    out = {}
    for extra in ('&closed=true', ''):
        q = '&'.join('slug=' + s for s in batch if s not in out)
        if not q:
            break
        for k in range(6):
            try:
                resp = requests.get(f'https://gamma-api.polymarket.com/markets?{q}{extra}&limit={len(batch) * 2}', timeout=30)
                if resp.status_code == 429:
                    time.sleep(2 + k * 3)
                    continue
                for m in resp.json():
                    t = m.get('closedTime') if m.get('closed') else None
                    ev = (m.get('events') or [{}])[0].get('slug') or m['slug']
                    out[m['slug']] = (pd.Timestamp(t).timestamp() if t else float('inf'), ev)
                break
            except Exception:
                time.sleep(2)
    return out


with ThreadPoolExecutor(8) as ex:
    for i, o in enumerate(ex.map(fetch, [todo[i:i + 40] for i in range(0, len(todo), 40)])):
        meta.update(o)
        if i % 200 == 0:
            pickle.dump(meta, open(OUT, 'wb'))
            print(i * 40, len(meta), flush=True)
pickle.dump(meta, open(OUT, 'wb'))
print('done', len(meta), 'missing', len(need - set(meta)))
