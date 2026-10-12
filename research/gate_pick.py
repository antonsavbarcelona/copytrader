"""One rule for every score: the gate whose WORST cell (lag 7/21 x first/second half x N 10/30)
is best. A gate with a cell of no copies (it let nobody through then) cannot be judged and is left out.
Ties within 1 pp: the wider gate (more wallets) wins - a gate is a cost, not a goal."""
import os, pandas as pd, numpy as np
SUF = os.environ.get('SUF', '')
o = {L: pd.read_pickle(f'alloc_gates{SUF}_{L}.pkl') for L in (7, 21)}
width = {g: o[7][('ELIGIBLE', g, 0)].n.sum() for g in ['none','n30','n100','d10','d20','d40','n30+','d20+','n100r5']}
def roi(df): return df.pnl.sum() / df.cost.sum() if df.cost.sum() > 0 else 0.0
rows = []
for sc in ['RANDOM','SHRUNK','PNL','TSTAT','CALMAR','EXPDD','PNLSQRTN','PF']:
    for g in width:
        cells = []
        for L in (7, 21):
            half = len(o[L][('ALL','',0)]) // 2
            for N in (10, 30):
                df = o[L][(sc, g, N)]
                cells += [roi(h) if h.cost.sum() > 0 else np.nan for h in (df.iloc[:half], df.iloc[half:])]
        allr = np.mean([roi(o[L][(sc, g, N)]) for L in (7, 21) for N in (10, 30)])
        rows.append((sc, g, -np.inf if np.isnan(cells).any() else min(cells), allr, width[g]))
t = pd.DataFrame(rows, columns=['score','gate','worst','mean','width'])
for sc, d in t.groupby('score', sort=False):
    best = d.worst.max()
    pick = d[d.worst >= best - 0.01].sort_values('width', ascending=False).iloc[0]
    print(f"{sc:9} pick {pick.gate:7} | " + '  '.join(f"{r.gate}:{r.worst:+.1%}/{r.mean:+.1%}" for r in d.itertuples()))
