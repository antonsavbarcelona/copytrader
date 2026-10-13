"""An independent check of the allocator (src/allocator.rs) by another way: from the paper's
events, the backtest file and the allocator's rows, it recomputes what each book should have
done and compares.

  picks     every entry and refusal is of a wallet the book picked that day (or the day before)
  complete  every paper buy of a picked wallet is in the book: an entry or a refusal
  sizes     stake = the book's share of its capital (or cut, never under 1/8 nor $1); the capital
            at entry = $10,000 + the result of the positions closed before
  caps      at each entry, with the positions open then (each at the share still open): 2 per
            wallet and outcome, everything 100%, the event the book's limit, the wallet 20%
  pnl       each position's dollars back from the paper's sales and settlement, to the cent
  balance   the state's and every status row's capital = $10,000 + the closed positions' result

DATABASE_URL=... py tools/alloc_audit.py [wallets/backtest_stats.csv]   (read only)"""
import os, sys, json, math
from collections import defaultdict
import psycopg2

GEN = 1
DEPOSIT, TOTAL_CAP, TRADER_CAP, WALLET_OUTCOME = 10_000.0, 1.0, 0.2, 2
MIN_SHARE, MIN_USD, MIN_MARKETS, DD_FLOOR, PF_MAX = 1 / 8, 1.0, 20, 1.0, 50.0
SIZES = [10, 20, 30, 50, 100]
RATED = ['PNL', 'PNL30', 'LCB', 'EXPDD', 'PNLSQRTN', 'TSTAT', 'PF', 'SHARPE', 'SORTINO', 'CALMAR', 'WEEKS']
EPS = 0.01  # a cent

problems = defaultdict(list)


def bad(check, msg):
    problems[check].append(msg)


# --- the picks, recomputed from the backtest file -------------------------------------------
def read_prior(path):
    out = {}
    lines = open(path).read().splitlines()
    head = lines[0].split(',')
    for l in lines[1:]:
        f = dict(zip(head, l.split(',')))
        try:
            r = {k: float(f[k]) for k in ['n', 'markets', 'cost', 'pnl', 'win', 'loss', 'r2', 'dd', 'pnl30', 'sharpe',
                                           'sortino', 'weeks']}
        except (KeyError, ValueError):
            continue
        if all(math.isfinite(v) for v in r.values()):
            out[f['wallet'].strip().lower()] = r
    return out


def score(r, s):
    n = max(r['n'], 1)
    m = r['pnl'] / r['cost'] if r['cost'] > 0 else 0.0
    sd = math.sqrt(max(r['r2'] / n - m * m, 0))
    return {
        'PNL': r['pnl'], 'PNL30': r['pnl30'], 'LCB': m - 2 * sd / math.sqrt(n),
        'EXPDD': r['pnl'] / n / max(r['dd'], DD_FLOOR), 'PNLSQRTN': m * math.sqrt(n),
        'TSTAT': m / sd * math.sqrt(n) if sd > 0 else 0.0,
        'PF': min(r['win'] / r['loss'], PF_MAX) if r['loss'] > 0 else PF_MAX,
        'SHARPE': r['sharpe'], 'SORTINO': r['sortino'], 'CALMAR': r['pnl'] / max(r['dd'], DD_FLOOR), 'WEEKS': r['weeks'],
    }[s]


def fnv(s):
    h = 0xcbf29ce484222325
    for b in s.encode():
        h = ((h ^ b) * 0x100000001b3) % 2 ** 64
    return h


def picks_of(prior, day):
    pool = sorted(w for w, r in prior.items() if r['pnl'] > 0 and r['n'] >= 1)
    rated = [w for w in pool if prior[w]['markets'] >= MIN_MARKETS]
    combo = defaultdict(float)
    for s in RATED:
        v = sorted(rated, key=lambda w: score(prior[w], s))  # stable, as Rust's sort_by
        k = max(len(v), 2) - 1
        for i, w in enumerate(v):
            combo[w] += i / k / len(RATED)
    out = {}
    for s in RATED + ['COMBO']:
        sc = (lambda w: combo[w]) if s == 'COMBO' else (lambda w, s=s: score(prior[w], s))
        frm = pool if s in ('PNL', 'PNL30') else rated
        order = sorted(frm, key=lambda w: (-sc(w), w))
        for n in SIZES:
            out[(s, n)] = set(order[:n])
    for k in range(1, 11):
        for n in SIZES:
            out[(f'RANDOM{k}', n)] = set(sorted(pool, key=lambda w: fnv(f'{day}/{k}/{n}/{w}'))[:n])
    return out


def parse(book):
    s, n, r, e = book.rsplit('_', 3)
    return s, int(n), float(r[1:]) / 100, float(e[1:]) / 100


def main():
    path = sys.argv[1] if len(sys.argv) > 1 else 'wallets/backtest_stats.csv'
    prior = read_prior(path)
    c = psycopg2.connect(os.environ['DATABASE_URL'])
    cur = c.cursor()
    cur.execute("SELECT data FROM state WHERE id = 'alloc'")
    st = cur.fetchone()[0]
    if st.get('generation') != GEN:
        sys.exit(f"the saved state is of generation {st.get('generation')}, not {GEN}")
    last_id, since = st['last_id'], st['since']
    cur.execute("SELECT book, entry_id, close_id, wallet, token, market, event, capital, stake, shares, back, pnl FROM alloc_trades "
                "WHERE gen = %s AND entry_id <= %s", (GEN, last_id))
    trades = {(r[0], r[1]): r for r in cur.fetchall()}
    cur.execute("SELECT book, event_id, wallet, reason, wanted, granted FROM alloc_decisions WHERE gen = %s AND event_id <= %s",
                (GEN, last_id))
    decisions = {(r[0], r[1]): r for r in cur.fetchall()}
    opened = {}
    for b, bs in st['books'].items():
        for p in bs['positions']:
            opened[(b, p['id'])] = p
    books = sorted(set(st['books']) | {k[0] for k in trades} | {k[0] for k in decisions})
    print(f"generation {GEN}, up to paper event {last_id}: {len(trades)} closed, {len(opened)} open, {len(decisions)} cut or "
          f"refused, {len(books)} books with something")

    # The paper's buys since the generation began, and the sales and settlements of what the books held.
    cur.execute("SELECT id, extract(epoch FROM at), kind, wallet, token, data FROM events WHERE at >= to_timestamp(%s) AND id <= %s "
                "AND kind IN ('buy', 'sell', 'settle') ORDER BY id", (since, last_id))
    events = cur.fetchall()
    day_picks = {}

    def picked(day):
        if day not in day_picks:
            day_picks[day] = picks_of(prior, day)
        return day_picks[day]

    # Entries per book, in order, with their paper buy.
    entries = defaultdict(list)
    for (b, i), t in trades.items():
        entries[b].append(i)
    for (b, i) in opened:
        entries[b].append(i)
    ev_by_id = {e[0]: e for e in events}

    # picks and completeness
    first = min((e[0] for e in events), default=0)
    by_wallet_books = defaultdict(set)
    for e in events:
        if e[2] != 'buy':
            continue
        f = (e[5] or {}).get('fill') or {}
        if (f.get('usd') or 0) + (f.get('fee') or 0) <= 0 or (f.get('shares') or 0) <= 0:
            continue
        day = int(e[1] // 86400)
        for b in books_all():
            s, n, _, _ = parse(b)
            if e[3] in picked(day)[(s, n)]:
                k = (b, e[0])
                if k not in trades and k not in opened and k not in decisions:
                    # the allocator may have made the day's picks a little after midnight
                    if e[3] not in picked(day - 1)[(s, n)]:
                        bad('complete', f'{b}: paper buy {e[0]} of {e[3][:10]} neither taken nor refused')
    for k in list(trades) + list(opened) + list(decisions):
        b, i = k
        e = ev_by_id.get(i)
        if e is None:
            if i >= first:
                bad('picks', f'{b}: event {i} not a paper buy of this generation')
            continue
        s, n, _, _ = parse(b)
        day = int(e[1] // 86400)
        if e[3] not in picked(day)[(s, n)] and e[3] not in picked(day - 1)[(s, n)]:
            bad('picks', f'{b}: {e[3][:10]} (event {i}) is not among its picks')

    # sizes, caps and pnl: each book replayed over the paper's events
    sells = defaultdict(list)  # (wallet, token) -> [(id, kind, data)]
    for e in events:
        if e[2] in ('sell', 'settle'):
            sells[(e[3], e[4])].append((e[0], e[2], e[5] or {}))
    for b in books:
        _, _, stake_share, event_cap = parse(b)
        ids = sorted(entries[b] + [i for (bb, i) in decisions if bb == b])
        pos = {}  # entry id -> dict
        realized = 0.0
        # The book's timeline: its entries and refusals, and the paper's sales of what it holds.
        timeline = sorted([(i, 'entry') for i in ids] + [(x[0], 'paper', key) for key, v in sells.items() for x in v],
                          key=lambda x: x[0])
        for item in timeline:
            i = item[0]
            if item[1] == 'paper':
                w, t = item[2]
                held = [p for p in pos.values() if p['wallet'] == w and p['token'] == t]
                if not held:
                    continue
                kind, d = next((k, dd) for (j, k, dd) in sells[(w, t)] if j == i)
                f = d.get('fill') or {}
                if kind == 'sell':
                    before, sh = d.get('held_before') or 0, f.get('shares') or 0
                    after = d.get('held_after', before)
                    if before <= 0 or sh <= 0:
                        continue
                    frac, per, out = min(sh / before, 1), ((f.get('usd') or 0) - (f.get('fee') or 0)) / sh, after <= 1e-6
                else:
                    frac, per, out = 1.0, d.get('price') or 0.0, True
                for p in held:
                    sold = p['shares'] if out else p['shares'] * frac
                    p['back'] += sold * per
                    p['shares'] -= sold
                    if out or p['shares'] <= 1e-9:
                        realized += p['back'] - p['stake']
                        t_row = trades.get((b, p['id']))
                        if t_row is None:
                            bad('pnl', f'{b}: position {p["id"]} closed at {i} by the paper, not in alloc_trades')
                        else:
                            if t_row[2] != i:
                                bad('pnl', f'{b}: position {p["id"]} closed at {t_row[2]}, the paper at {i}')
                            if abs(t_row[10] - p['back']) > EPS:
                                bad('pnl', f'{b}: position {p["id"]} back ${t_row[10]:.4f}, recomputed ${p["back"]:.4f}')
                        del pos[p['id']]
                continue
            capital = DEPOSIT + realized
            want = stake_share * capital
            e = ev_by_id.get(i)
            if e is None:
                continue
            row = trades.get((b, i)) or opened.get((b, i))
            dec = decisions.get((b, i))
            market = (row[5] if isinstance(row, tuple) else row['market']) if row else None
            event = (row[6] if isinstance(row, tuple) else row['event']) if row else None
            risk = lambda f: sum(p['stake'] * p['shares'] / p['shares0'] for p in pos.values() if f(p))
            if row is None:
                # refused: there must have been no room
                if dec[3] == 'ADD_CAP':
                    if sum(1 for p in pos.values() if p['wallet'] == e[3] and p['token'] == e[4]) < WALLET_OUTCOME:
                        bad('caps', f'{b}: refused {i} (ADD_CAP) with under 2 positions of the wallet in the outcome')
                    continue
                rooms = [TOTAL_CAP * capital - risk(lambda p: True), TRADER_CAP * capital - risk(lambda p: p['wallet'] == e[3])]
                if min(rooms) >= max(want * MIN_SHARE, MIN_USD) and dec[3] != 'EVENT_CAP':
                    bad('caps', f'{b}: refused {i} ({dec[3]}) with room ${min(rooms):.2f} for ${want:.2f}')
                if abs(dec[4] - want) > EPS:
                    bad('sizes', f'{b}: refusal {i} wanted ${dec[4]:.2f}, the capital gives ${want:.2f}')
                continue
            stake, shares0, cap0 = (row[8], row[9], row[7]) if isinstance(row, tuple) else (row['stake'], row['shares0'], row['capital'])
            if abs(cap0 - capital) > EPS:
                bad('sizes', f'{b}: entry {i} at capital ${cap0:.2f}, recomputed ${capital:.2f}')
            if stake > want + EPS or stake < max(want * MIN_SHARE, MIN_USD) - EPS:
                bad('sizes', f'{b}: entry {i} stake ${stake:.2f} for ${want:.2f} wanted')
            if stake < want - EPS and dec is None:
                bad('sizes', f'{b}: entry {i} cut to ${stake:.2f} of ${want:.2f}, no decision row')
            if sum(1 for p in pos.values() if p['wallet'] == e[3] and p['token'] == e[4]) >= WALLET_OUTCOME:
                bad('caps', f'{b}: entry {i} a 3rd position of {e[3][:10]} in one outcome')
            for name, used, cap in [('total', risk(lambda p: True), TOTAL_CAP), ('event', risk(lambda p: p['event'] == event), event_cap),
                                    ('wallet', risk(lambda p: p['wallet'] == e[3]), TRADER_CAP)]:
                if used + stake > cap * capital + EPS:
                    bad('caps', f'{b}: entry {i} takes {name} to ${used + stake:.2f} over ${cap * capital:.2f}')
            pos[i] = dict(id=i, wallet=e[3], token=e[4], market=market, event=event, stake=stake, shares0=shares0,
                          shares=shares0, back=0.0)
        # what is still open must be the state's
        for i, p in pos.items():
            o = opened.get((b, i))
            if o is None:
                bad('pnl', f'{b}: position {i} open by the paper, not in the state')
            elif abs(o['back'] - p['back']) > EPS or abs(o['shares'] - p['shares']) > 1e-4:
                bad('pnl', f'{b}: open position {i} back/shares {o["back"]:.4f}/{o["shares"]:.4f}, recomputed '
                           f'{p["back"]:.4f}/{p["shares"]:.4f}')
        got = st['books'].get(b, {}).get('realized', 0.0)
        if abs(got - realized) > EPS:
            bad('balance', f'{b}: the state realized ${got:.2f}, recomputed ${realized:.2f}')

    # every status row: capital = deposit + the positions closed by then
    cur.execute("SELECT book, last_id, capital FROM alloc_status WHERE gen = %s", (GEN,))
    closed = defaultdict(list)
    for (b, _), t in trades.items():
        closed[b].append((t[2], t[11]))
    rows = cur.fetchall()
    for b, lid, cap in rows:
        want = DEPOSIT + sum(p for x, p in closed[b] if x <= lid)
        if lid <= last_id and abs(cap - want) > EPS:
            bad('balance', f'{b}: status at event {lid} capital ${cap:.2f}, recomputed ${want:.2f}')

    print(f'{len(rows)} status rows, {len(events)} paper events read')
    for check in ['picks', 'complete', 'sizes', 'caps', 'pnl', 'balance']:
        v = problems[check]
        print(f'{check:9} {"OK" if not v else f"{len(v)} problems"}')
        for m in v[:10]:
            print('   ', m)
    sys.exit(1 if any(problems.values()) else 0)


def books_all():
    out = []
    for s in RATED + ['COMBO'] + [f'RANDOM{k}' for k in range(1, 11)]:
        for n in SIZES:
            for r in ['0.5', '1', '2']:
                for e in ['20', '50', '100']:
                    out.append(f'{s}_{n}_R{r}_E{e}')
    return out


if __name__ == '__main__':
    main()
