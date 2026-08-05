#!/usr/bin/env python3
"""Compare two lcov files' per-file DA hit-line sets, path-normalized to crates/... .
Usage: lcov_diff.py A.lcov B.lcov [file-substring-filter]
Prints per-file: total DA, hits in A, hits in B, A-only, B-only counts (and line lists when small).
"""
import sys, re

def norm(p):
    for anchor in ("crates/", "fuzz/core/"):
        i = p.find(anchor)
        if i >= 0:
            return p[i:]
    return p

def load(path):
    files = {}
    cur = None
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("SF:"):
                cur = norm(line[3:])
                files.setdefault(cur, {})
            elif line.startswith("DA:") and cur is not None:
                ln, cnt = line[3:].split(",")[:2]
                d = files[cur]
                ln = int(ln); cnt = int(cnt)
                d[ln] = max(d.get(ln, 0), cnt)
    return files

a, b = load(sys.argv[1]), load(sys.argv[2])
filt = sys.argv[3] if len(sys.argv) > 3 else ""
common = sorted(set(a) | set(b))
tot_aonly = tot_bonly = 0
for f in common:
    if filt and filt not in f:
        continue
    da_a, da_b = a.get(f, {}), b.get(f, {})
    hits_a = {l for l, c in da_a.items() if c > 0}
    hits_b = {l for l, c in da_b.items() if c > 0}
    aonly = hits_a - hits_b
    bonly = hits_b - hits_a
    tot_aonly += len(aonly); tot_bonly += len(bonly)
    if aonly or bonly:
        print(f"{f}: DA {len(da_a)}/{len(da_b)}  hitsA={len(hits_a)} hitsB={len(hits_b)} Aonly={len(aonly)} Bonly={len(bonly)}")
        if len(aonly) <= 12 and aonly:
            print("   A-only:", sorted(aonly))
        if len(bonly) <= 12 and bonly:
            print("   B-only:", sorted(bonly))
    else:
        print(f"{f}: IDENTICAL hit set ({len(hits_a)} lines)")
print(f"TOTAL A-only={tot_aonly} B-only={tot_bonly}")
