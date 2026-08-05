#!/usr/bin/env python3
"""Classify residual uncovered lines for the p1-laneaa done-gate.

Inputs: a merge-coverage outdir (files/*.json), the fuzz lcov (for the
no-DA / DA:0 split), and the source tree. Emits DRAFT exception rows
(phase1-exceptions.tsv columns) for mechanical classes, and a HUMAN-REVIEW
list for everything else. Draft classes:
  - instrument-unmappable: no DA record AND the line shape is a match-arm
    alternation continuation (`| Pattern`), a bare pattern list line inside a
    match, a `let x: T;` declaration, or a lone `)?;`/brace continuation.
  - unreachable-arm / defensive-c-parity: DA:0 (or no-DA) line whose text is
    (or is the sole body of) unreachable!(...) / panic!("...") mirroring a C
    elog(ERROR, "internal") arm — C counterpart filled by hand afterwards.
  - carve (excluded-state): line falls in a named carved function (list per
    crate below) — the claim's OUT carve at line grain.
Everything else -> REVIEW (needs a seed or a hand-written exception).

Usage: classify_residual_jsonpath.py <merge-outdir> <lcov> <repo-root> <crate-rel-path> [--carve fn1,fn2,...]
"""
import json
import glob
import os
import re
import sys

outdir, lcov, root, crate = sys.argv[1:5]
carve_fns = set()
if len(sys.argv) > 5 and sys.argv[5] == "--carve":
    carve_fns = set(sys.argv[6].split(","))

# lcov DA map
da = {}
cur = None
for line in open(lcov):
    line = line.strip()
    if line.startswith("SF:"):
        cur = line[3:]
    elif line.startswith("DA:") and cur and f"/{crate}/src/" in cur:
        ln, c = line[3:].split(",")[:2]
        da.setdefault(os.path.basename(cur), {})[int(ln)] = int(c)

PAT_CONT = re.compile(r"^\s*\|\s*\S")           # | Alternation
PAT_ARM_LABEL = re.compile(r"^\s*(?:\w+::)+\w+(?:\s*\{[^}]*\})?\s*$")  # bare pattern line
LET_DECL = re.compile(r"^\s*let\s+(?:mut\s+)?\w+\s*:\s*[^=;]+;\s*$")
TRIVIA = re.compile(r"^\s*(?:\)\??[;,]?|\}?\)*[;,]?|=>\s*\{?|\{)\s*$")
UNREACH = re.compile(r"^\s*(?:_\s*=>\s*)?(?:unreachable!|panic!)\s*[\(!]")

rows, review = [], []
for f in sorted(glob.glob(os.path.join(outdir, "files", "*.json"))):
    d = json.load(open(f))
    base = f.split("__")[-1].replace(".rs.json", ".rs")
    unc = sorted(set(d["sloc"]) - set(d["fuzz"]) - set(d["kani"]))
    if not unc:
        continue
    srcpath = os.path.join(root, "crates/backend/utils/adt", crate.split("/")[-1], "src", base)
    src = open(srcpath).read().splitlines()
    starts = []
    for i, l in enumerate(src, 1):
        m = re.match(r"\s*(?:pub )?fn (\w+)", l)
        if m:
            starts.append((i, m.group(1)))

    def fn_of(ln):
        n = "?"
        for s, name in starts:
            if s <= ln:
                n = name
            else:
                break
        return n

    m = da.get(base, {})
    rel = f"crates/backend/utils/adt/{crate.split('/')[-1]}/src/{base}"
    for ln in unc:
        text = src[ln - 1]
        fn = fn_of(ln)
        if fn in carve_fns:
            rows.append((rel, ln, "excluded-state", f"claim carve: {fn}", "session-state/engine carve named in the p1-laneaa claim scope_note; C counterpart executes the same region under session TZ / executor state"))
        elif UNREACH.search(text):
            rows.append((rel, ln, "unreachable-arm", "TODO-C-counterpart", f"defensive internal-error arm in {fn}: {text.strip()[:80]}"))
        elif ln not in m and (PAT_CONT.match(text) or PAT_ARM_LABEL.match(text) or LET_DECL.match(text) or TRIVIA.match(text)):
            shape = "match-arm alternation/pattern continuation" if (PAT_CONT.match(text) or PAT_ARM_LABEL.match(text)) else ("bare let decl" if LET_DECL.match(text) else "closing/trivia continuation")
            rows.append((rel, ln, "instrument-unmappable", f"{fn} ({shape})", f"no DA record emitted for the shape (verified in {os.path.basename(lcov)}); {shape}"))
        else:
            review.append((rel, ln, fn, "no-DA" if ln not in m else f"DA0", text.strip()[:100]))

print(f"# DRAFT exception rows: {len(rows)}")
for r in rows:
    print("\t".join(str(x) for x in r))
print(f"\n# REVIEW ({len(review)}):")
for r in review:
    print("\t".join(str(x) for x in r))
