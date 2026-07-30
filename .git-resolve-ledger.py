#!/usr/bin/env python3
"""Resolve conflict markers in proofs/USER_FACING_FUNCTIONS.tsv.
Rules: proved > (candidate|in-progress) > (untriaged|blocked|wall|parked).
If one side proved and other side wall/divergence/blocked -> DISAGREEMENT: keep
conservative side and print loudly (exit still 0; human adjudicates).
Equal status class: keep the row with the longer (more evidenced) note line.
"""
import re, sys
p = 'proofs/USER_FACING_FUNCTIONS.tsv'
s = open(p).read()
disagreements = []
def stat(l):
    f = l.split('\t')
    return f[3].split('(')[0].strip() if len(f) > 3 else ''
RANK = {'blocked':0,'wall':0,'parked':0,'untriaged':0,'divergence':0,
        'candidate':1,'in-progress':1,'excluded':2,'proved':3,'tested':2}
def resolve(m):
    hd = {l.split('\t')[0]: l for l in m.group(1).rstrip('\n').split('\n') if l.strip()}
    th = {l.split('\t')[0]: l for l in m.group(2).rstrip('\n').split('\n') if l.strip()}
    out = []
    for oid in dict.fromkeys(list(hd)+list(th)):
        h, t = hd.get(oid), th.get(oid)
        if not (h and t):
            out.append(h or t); continue
        hs, ts = stat(h), stat(t)
        hr, tr = RANK.get(hs, 1), RANK.get(ts, 1)
        # disagreement check: proved vs wall/divergence/blocked-hard verdict
        if {hs, ts} & {'proved'} and {hs, ts} & {'wall','divergence'}:
            disagreements.append((oid, hs, ts, h, t))
            out.append(h if hr < tr else t)  # conservative = lower rank
            continue
        if hr == tr:
            out.append(h if len(h) >= len(t) else t)
        else:
            out.append(h if hr > tr else t)
    return '\n'.join(out) + '\n'
s = re.sub(r'<<<<<<< [^\n]*\n(.*?)=======\n(.*?)>>>>>>> [^\n]*\n', resolve, s, flags=re.S)
open(p, 'w').write(s)
for d in disagreements:
    print('DISAGREEMENT oid=%s HEAD=%s THEIRS=%s' % d[:3])
    print('  H: %s' % d[3][:200]); print('  T: %s' % d[4][:200])
print('resolved; disagreements=%d' % len(disagreements))
