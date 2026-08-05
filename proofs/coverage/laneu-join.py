#!/usr/bin/env python3
"""laneu-join.py — adt/numeric gate join: v2-SLOC denominator (from a
merge-coverage.py run) x fuzz lines (fleet lcov exports, union) x banked
7-crate Kani capture line lists (proofs/coverage/files, valid: numeric src
files unchanged since the capture except keypack.rs, whose banked kani
credit is zero). Emits per-file residual line lists for the exceptions
pass."""
import json, re, sys, glob, subprocess

REPO = "/Users/malisper/dev/pgrust-fast/.wt-p1-laneu"
CRATE = "crates/backend/utils/adt/numeric"

lcovs = sys.argv[1:]
assert lcovs, "usage: laneu-join.py <lcov> [lcov...]"

# fuzz lines from lcov (DA>0), keyed by repo-relative path
fuzz = {}
for path in lcovs:
    cur = None
    for line in open(path):
        line = line.strip()
        if line.startswith("SF:"):
            sf = line[3:]
            i = sf.find(CRATE)
            cur = sf[i:] if i >= 0 else None
        elif line.startswith("DA:") and cur:
            ln, cnt = line[3:].split(",")[:2]
            if int(cnt) > 0:
                fuzz.setdefault(cur, set()).add(int(ln))

# SLOC v2 denominator: recompute per file with merge-coverage's own rule by
# invoking it? Instead reuse the per-file sloc COUNTS + the excluded lists
# from a prior merge outdir is insufficient (no line lists). Recompute v2
# sloc lines here with the documented textual rule.
def sloc_lines(p):
    out = []
    inblock = False
    for i, raw in enumerate(open(p, encoding="utf-8"), 1):
        s = raw
        # strip block comments (approximate, line-granular like the tool)
        res = []
        j = 0
        while j < len(s):
            if inblock:
                k = s.find("*/", j)
                if k < 0:
                    j = len(s); break
                inblock = False; j = k + 2
            else:
                k = s.find("/*", j)
                l = s.find("//", j)
                if 0 <= l and (k < 0 or l < k):
                    res.append(s[j:l]); j = len(s)
                elif k >= 0:
                    res.append(s[j:k]); inblock = True; j = k + 2
                else:
                    res.append(s[j:]); j = len(s)
        t = "".join(res)
        if any(ch not in " \t\r\n{}()[];," for ch in t):
            out.append(i)
    return out

kani = {}
for f in glob.glob(f"{REPO}/proofs/coverage/files/crates__backend__utils__adt__numeric__*.json"):
    d = json.load(open(f))
    kani[d["path"]] = set(d.get("kani", []))

import os
tot_s = tot_a = tot_f = tot_k = 0
residual = {}
for fn in sorted(os.listdir(f"{REPO}/{CRATE}/src")):
    if not fn.endswith(".rs") or fn == "tests.rs":
        continue
    rel = f"{CRATE}/src/{fn}"
    sl = set(sloc_lines(f"{REPO}/{rel}"))
    fz = fuzz.get(rel, set()) & sl
    kn = kani.get(rel, set()) & sl
    any_ = fz | kn
    residual[rel] = sorted(sl - any_)
    tot_s += len(sl); tot_a += len(any_); tot_f += len(fz); tot_k += len(kn)
    print(f"{rel}\t{len(sl)}\t{len(kn)}\t{len(fz)}\t{len(any_)}\t{len(sl)-len(any_)}")
print(f"TOTAL\t{tot_s}\t{tot_k}\t{tot_f}\t{tot_a}\t{tot_s-tot_a}\t{100.0*tot_a/tot_s:.2f}%")
json.dump(residual, open("/tmp/laneu-residual.json", "w"), indent=0)
