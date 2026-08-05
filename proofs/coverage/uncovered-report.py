#!/usr/bin/env python3
"""uncovered-report.py — list the largest contiguous SLOC runs covered by
NO source (kani/fuzz/regress), per crate, from the files/*.json detail.
Usage: uncovered-report.py [outdir] [top_n_per_crate]
"""
import glob, json, os, sys, collections

here = os.path.dirname(os.path.abspath(__file__))
outdir = sys.argv[1] if len(sys.argv) > 1 else here
topn = int(sys.argv[2]) if len(sys.argv) > 2 else 8

runs_by_crate = collections.defaultdict(list)
for f in glob.glob(os.path.join(outdir, "files", "*.json")):
    d = json.load(open(f))
    sloc = d["sloc"]
    covered = set(d["kani"]) | set(d["fuzz"]) | set(d["regress"])
    run = None
    for ln in sloc:
        if ln in covered:
            run = None
            continue
        if run and ln - run[1] <= 2:  # bridge across non-SLOC gaps of <=2
            run[1] = ln
            run[2] += 1
        else:
            run = [ln, ln, 1]
            runs_by_crate[d["path"].split("/src/")[0]].append((run, d["path"]))
for crate in sorted(runs_by_crate):
    rs = sorted(runs_by_crate[crate], key=lambda x: -x[0][2])[:topn]
    print(f"== {crate}")
    for (a, b, n), path in rs:
        print(f"  {n:4d} sloc  {path}:{a}-{b}")
