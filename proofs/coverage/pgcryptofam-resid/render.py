#!/usr/bin/env python3
"""Render residual lines with source text + neighbor DA context (rendered-red audit)."""
import sys, os
sys.path.insert(0, "/Users/malisper/dev/pgrust-fast/.wt-pgcryptofam-resid/proofs/coverage")
import sloc_rules, test_scope

REPO = "/Users/malisper/dev/pgrust-fast/.wt-pgcryptofam-resid"
LCOV = sys.argv[1] if len(sys.argv) > 1 else "/tmp/claude-501/-Users-malisper-dev-pgrust-fast/2cd0edb9-da0d-4c8a-b94c-d03c38bec772/scratchpad/pgcf.lcov"
CRATE = "crates/contrib/pgcrypto"
IN_SCOPE = ["src/crypt.rs","src/crypt/bcrypt.rs","src/crypt/cryptdes.rs",
            "src/crypt/desc.rs","src/crypt/shacrypt.rs","src/hashing.rs",
            "src/lib.rs","src/pgp/armor.rs"]

da = {}; cur = None
for raw in open(LCOV, encoding="utf-8", errors="replace"):
    raw = raw.strip()
    if raw.startswith("SF:"):
        p = raw[3:]; cur = p if p.startswith(CRATE) else None
        if cur is not None: da.setdefault(cur, {})
    elif cur and raw.startswith("DA:"):
        ln, cnt = raw[3:].split(",")[:2]; ln, cnt = int(ln), int(cnt)
        d = da[cur]; d[ln] = max(d.get(ln, 0), cnt)
    elif raw == "end_of_record":
        cur = None

test_scope.set_repo_root(REPO)
for f in IN_SCOPE:
    rel = f"{CRATE}/{f}"
    lines = sloc_rules.sloc_lines(os.path.join(REPO, rel))
    d = da.get(rel, {})
    resid = sorted(ln for ln in lines if d.get(ln, 0) == 0)
    if not resid: continue
    src = open(os.path.join(REPO, rel), encoding="utf-8").readlines()
    print(f"\n===== {f}  ({len(resid)} residual) =====")
    # group contiguous
    runs = []
    for ln in resid:
        if runs and ln - runs[-1][-1] <= 2:
            runs[-1].append(ln)
        else:
            runs.append([ln])
    for run in runs:
        lo, hi = run[0], run[-1]
        for ln in range(max(1, lo-2), min(len(src), hi+2)+1):
            hits = d.get(ln)
            mark = "RED" if ln in run else ("   " if hits is None else f"{hits:>3}" if hits < 1000 else "BIG")
            insl = "*" if ln in lines else " "
            danote = "noDA" if hits is None else str(hits)
            flag = ">>" if ln in run else "  "
            print(f"{flag} {ln:4d} {insl} DA={danote:>8s}  {src[ln-1].rstrip()}")
        print("   ----")
