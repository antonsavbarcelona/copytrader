"""The paper's books against the backtest's (research/grid_backtest.csv: the 990 books on the
backtest's days 30-90, a stake of the $10,000 deposit, research/grid_bt.py): what the paper
made so far and what the backtest made in as many days (its 60 days pro rata), the worst drop
of each; then each score's books pooled, and the paper's share of the backtest's result.

The backtest is known to be too kind to the picked wallets (research/README.md): this says how
much of it the paper keeps. (The paper's stake is of its capital, the backtest's of the
deposit: alike while the capital is near $10,000.)

DATABASE_URL=... py tools/alloc_vs_backtest.py [--book NAME]   (read only)"""
import os, sys, re
from collections import defaultdict
import psycopg2

GEN, DEPOSIT, BT_DAYS = 1, 10_000.0, 60.0
HERE = os.path.dirname(os.path.abspath(__file__))

# The backtest has no buy cap (it fills at the trader's price): both caps' books against it.
bt = {}
for i, l in enumerate(open(os.path.join(HERE, '..', 'research', 'grid_backtest.csv'))):
    if i:
        f = l.strip().split(',')
        for c in ('_C3', '_C10'):
            bt[f[0] + c] = (float(f[1]), float(f[2]))

c = psycopg2.connect(os.environ['DATABASE_URL'])
cur = c.cursor()
# The generation began at its first status (the allocator writes one as it starts).
cur.execute("SELECT extract(epoch FROM min(at))::float8 FROM alloc_status WHERE gen = %s", (GEN,))
row = cur.fetchone()
if not row or row[0] is None:
    sys.exit(f'no generation {GEN} statuses')
cur.execute("SELECT book, equity, extract(epoch FROM at)::float8 FROM alloc_status WHERE gen = %s ORDER BY at", (GEN,))
paper, peak, dd, last_at = {}, defaultdict(lambda: DEPOSIT), defaultdict(float), 0.0
for b, eq, at in cur.fetchall():
    paper[b] = eq - DEPOSIT
    peak[b] = max(peak[b], eq)
    dd[b] = max(dd[b], peak[b] - eq)
    last_at = max(last_at, at)
days = (last_at - float(row[0])) / 86400
k = days / BT_DAYS
print(f'generation {GEN}: {days:.1f} days of paper (the last hourly status); the backtest pro rata = its 60 days x {k:.3f}\n')

if '--book' in sys.argv:
    b = sys.argv[sys.argv.index('--book') + 1]
    p, (bp, bd) = paper.get(b, 0.0), bt[b]
    print(f'{b}: paper {p:+,.0f} $ (worst drop {dd[b]:,.0f} $), backtest in as many days {bp * k:+,.0f} $ (all 60 days {bp:+,.0f} $, '
          f'worst drop {bd:,.0f} $)')
    sys.exit()

score = lambda b: re.match(r'(.+?)_\d+_R', b).group(1)
size = lambda b: int(re.match(r'.+?_(\d+)_R', b).group(1))
groups = defaultdict(list)
for b in bt:
    groups[score(b) if not score(b).startswith('RANDOM') else 'RANDOM'].append(b)
print(f'{"score":10} {"books":>5} {"paper $":>10} {"backtest $":>11} {"kept":>6} {"paper dd $":>11} {"bt dd $ (60d)":>14}')
for g, bs in sorted(groups.items(), key=lambda x: -sum(paper.get(b, 0) for b in x[1]) / len(x[1])):
    p = sum(paper.get(b, 0.0) for b in bs) / len(bs)
    q = sum(bt[b][0] for b in bs) / len(bs) * k
    print(f'{g:10} {len(bs):5} {p:+10,.0f} {q:+11,.0f} {p / q if q else 0:6.0%} {sum(dd[b] for b in bs) / len(bs):11,.0f} '
          f'{sum(bt[b][1] for b in bs) / len(bs):14,.0f}')

print('\nby size, the scores\' books:')
for n in [10, 20, 30, 50, 100]:
    bs = [b for b in bt if size(b) == n and not b.startswith('RANDOM')]
    p = sum(paper.get(b, 0.0) for b in bs) / len(bs)
    q = sum(bt[b][0] for b in bs) / len(bs) * k
    print(f'  top {n:3}: paper {p:+9,.0f} $  backtest {q:+9,.0f} $  kept {p / q if q else 0:6.0%}')

print('\nby buy cap, the scores\' books and the random draws\':')
for c in ('_C3', '_C10'):
    for rnd in (False, True):
        bs = [b for b in bt if b.endswith(c) and b.startswith('RANDOM') == rnd]
        p = sum(paper.get(b, 0.0) for b in bs) / len(bs)
        q = sum(bt[b][0] for b in bs) / len(bs) * k
        print(f'  {c[1:]:4} {"random" if rnd else "scores":7} paper {p:+9,.0f} $ (dd {sum(dd[b] for b in bs) / len(bs):6,.0f} $)  '
              f'backtest {q:+9,.0f} $  kept {p / q if q else 0:6.0%}')

print('\nthe 15 best books on paper:')
for b in sorted(paper, key=lambda b: -paper[b])[:15]:
    print(f'  {b:28} paper {paper[b]:+9,.0f} $ dd {dd[b]:7,.0f} $   backtest {bt[b][0] * k:+9,.0f} $')
