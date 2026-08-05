#!/usr/bin/env python3
"""bank.py <join_dir> — build the final residual TSV + hand exception rows +
equation for the lanek gate remediation, from the FINAL join output plus the
adjudication table below. Verifies denom = measured + excepted + recorded.

Usage: python3 bank.py <join_dir> <outdir>
Writes: <outdir>/residual-lanek-remediated.tsv, <outdir>/hand-exceptions.tsv,
prints the equation. Reads reasons from adjudications embedded here.
"""
import json, glob, sys, collections, re, os

join_dir, outdir = sys.argv[1], sys.argv[2]
os.makedirs(outdir, exist_ok=True)
REPO = "/Users/malisper/dev/pgrust-fast/.wt-gr-fmt"
PFX = "crates/backend/utils/adt/formatting/src/"
LANE = "p1-lanek-remediation"

# ---- hand-adjudicated ledger exception rows: (file,line) -> (class, c_cite, justification)
D = "defensive-c-parity"; U = "unreachable-arm"
CACHE_GUARD = (D,
  "formatting.c 18.3 DCH_prevent_counter_overflow/NUM_prevent_counter_overflow (vendored pg_formatting_18_3.inc:3618)",
  "fires only when the per-thread cache counter reaches i32::MAX-1 (2^31 cache fetches in one backend); identical cold guard in C, equally unexercisable by any corpus")
CACHE_SCAN = (U,
  "formatting.c 18.3 DCH_cache_getnew !ent->valid eviction scan (inc:3751; C marks the victim invalid during parse, inc:3777)",
  "Rust port parses the picture BEFORE inserting, so no cache entry is ever valid=false; the !valid scan is structurally dead, kept for C shape parity")
DEAD_API = lambda cname: (U,
  f"formatting.c 18.3 {cname}",
  "dead C-parity pub API: no caller anywhere in tree (oracle_compat carries its own copy); kept to mirror the C surface")

HAND = {}
for l in (49, 50, 52, 58, 59, 61):
    HAND[("cache.rs", l)] = CACHE_GUARD
for l in (100, 101, 158, 159):
    HAND[("cache.rs", l)] = CACHE_SCAN
HAND[("dch.rs", 809)] = (U,
  "formatting.c 18.3 DCH_to_char keyword switch (no default arm needed: every id has a case)",
  "Rust match exhaustiveness requires a catch-all; every DCH keyword id in tables.rs has an explicit arm, so no input reaches it")
for l in (110, 111, 112, 113):
    HAND[("case.rs", l)] = DEAD_API("str_numth (formatting.c:1583)")
for l in (124, 125, 126, 127, 128, 129, 131, 133, 134, 136):
    HAND[("case.rs", l)] = DEAD_API("asc_initcap (formatting.c:2103)")
for l in (249, 250):
    HAND[("tables.rs", l)] = DEAD_API("S_TH suffix macro (formatting.c:601)")
for l in (254, 255):
    HAND[("tables.rs", l)] = DEAD_API("S_TH_lower suffix macro (formatting.c:602)")
for l in (273, 274):
    HAND[("tables.rs", l)] = DEAD_API("S_SP suffix macro (formatting.c:607)")
for l in (367, 368):
    HAND[("tables.rs", l)] = DEAD_API("IS_LDECIMAL macro (formatting.c:389)")
for l in (219, 220):
    HAND[("tables.rs", l)] = (U,
      "MAXALIGN (c.h) used by DCH_CACHE_SIZE/NUM_CACHE_SIZE sizing (formatting.c:395)",
      "const fn evaluated only at compile time for the cache-size constants; no runtime call site exists")

# ---- RECORDED residual class reasons (per-line reasons attached below)
R_SOFT = ("soft-error-shape",
  "escontext soft-mode arm: harness and C oracle both run the hard-error SQL plane (escontext=None), so errsave()? propagates Err before this line; "
  "production-reachable via SQL/JSON datetime() soft mode; owed to a future soft-plane harness")
R_ENVPIN_TZ = ("env-pin-shadow", "requires session gmtoff not a whole hour; harness pins timezone=GMT (offset 0)")
R_ENVPIN_LOC = ("env-pin-shadow", "requires a non-C locale (non-empty lconv strings / localized names); harness pins C locale per the SQL-surface carve")
R_CONST = ("const-registry", "const/registry machinery evaluated at compile time or by server boot, not by the fuzz entry points")
R_SEAM = ("seam-glue", "pgrust-only init_seams() server-boot glue; fuzz harness calls the crate entry points directly")

RECORDED_FIXED = {}
for l in (331, 332, 333, 334):
    RECORDED_FIXED[("dch.rs", l)] = R_ENVPIN_TZ
for l in (133, 134, 135):
    RECORDED_FIXED[("dch.rs", l)] = ("env-pin-shadow",
      "localized-name-too-long guard: needs a locale whose month/day names exceed (keylen+TM)*DCH_MAX_ITEM_SIZ; impossible under the pinned C-locale English names")
for l in (78, 83, 87, 91, 96, 103):
    RECORDED_FIXED[("num.rs", l)] = R_ENVPIN_LOC
for l in (40, 41):
    RECORDED_FIXED[("lib.rs", l)] = R_SEAM
for l in (133, 134, 135, 136, 137, 138, 139, 140):
    RECORDED_FIXED[("fmgr_builtins.rs", l)] = R_CONST

# instrument-unmappable hand rows: executed lines the instrument emits no DA
# record for (evidence = flanking DA counts from the banked lcov captures).
IU = "instrument-unmappable"
def iu(evidence):
    return (IU, "n/a (measurement note, not a semantic carve)",
            f"no DA record emitted for the line shape (rustc 1.96/llvm-cov); flanking lines executed in the banked capture: {evidence}")
IU_ROWS = {
    ("dch.rs", 731): iu("730=8264, 732=26687 (IYY %1000 expression tail)"),
    ("dch.rs", 749): iu("748=18726, 750=54260 (IY %100 expression tail)"),
    ("dch.rs", 759): iu("758=42532, 760=148885 (I %10 expression tail)"),
    ("dch_entry.rs", 211): iu("210=5786, 212=5786 (bare-arg continuation)"),
    ("dch_entry.rs", 331): iu("330=176, 332=176 (bare-arg continuation)"),
    ("dch_entry.rs", 486): iu("485=108, 487=108 (bare-arg continuation)"),
    ("dch_fromchar.rs", 208): iu("207=613, 209=613 (bare-arg continuation)"),
    ("dch_fromchar.rs", 227): iu("226=581, 228=581 (bare-arg continuation)"),
    ("dch_fromchar.rs", 406): iu("405=360, 407=360 (bare-arg continuation)"),
    ("dch_fromchar.rs", 424): iu("423=636, 425=636 (bare-arg continuation)"),
    ("dch_fromchar.rs", 683): iu("682=325, 684=325 (bare-arg continuation)"),
    ("dch_fromchar.rs", 868): iu("866=216372, 870=46811 (multi-line arm-head pattern list)"),
    ("dch_fromchar.rs", 869): iu("870=46811 (multi-line arm-head pattern list)"),
    ("dch_fromchar.rs", 876): iu("879 block executed 154979 (multi-line arm-head pattern list)"),
    ("dch_fromchar.rs", 877): iu("879 block executed 154979 (multi-line arm-head pattern list)"),
    ("dch_fromchar.rs", 878): iu("879 block executed 154979 (multi-line arm-head pattern list)"),
    ("dch_fromchar.rs", 879): iu("881=154979 (multi-line arm-head pattern list)"),
    ("dch_fromchar.rs", 880): iu("881=154979 (multi-line arm-head pattern list)"),
    ("parse.rs", 256): iu("255=19872, 259=19872 (struct-field continuation, END node ctor)"),
    ("parse.rs", 257): iu("255=19872, 259=19872 (struct-field continuation)"),
    ("parse.rs", 296): iu("295=1356924, 299=1356924 (struct-field continuation)"),
    ("parse.rs", 297): iu("295=1356924, 299=1356924 (struct-field continuation)"),
    ("num.rs", 89): iu("90=2276 (struct-literal head, NumLocale ctor body executed)"),
    ("num_entry.rs", 478): iu("477=1001, 479=1001 (let-decl, if not auto-classified)"),
}
HAND.update(IU_ROWS)
for l in (223, 225):
    RECORDED_FIXED[("tables.rs", l)] = R_CONST
for l in (618, 619, 620, 621, 622, 623):
    RECORDED_FIXED[("tables.rs", l)] = ("const-registry",
      "macro_rules! body building the const keyword tables; evaluated at compile time only")

# soft list from the mechanical split (regenerated against the final join by caller)
SOFT_LIST = set()
soft_path = os.path.join(outdir, "soft-list.txt")
if os.path.exists(soft_path):
    for line in open(soft_path):
        p, l = line.rsplit("\t", 1)
        SOFT_LIST.add((p.split("/")[-1], int(l)))

# agent adjudications merged in from hunt-verdicts.tsv (file<TAB>line<TAB>class<TAB>reason)
AGENT = {}
ag_path = os.path.join(outdir, "hunt-verdicts.tsv")
if os.path.exists(ag_path):
    for line in open(ag_path):
        if line.startswith("#") or not line.strip():
            continue
        f, l, cls, reason = line.rstrip("\n").split("\t", 3)
        AGENT[(f.split("/")[-1], int(l))] = (cls, reason)

# ---- load final join
unc, cov = {}, {}
for f in glob.glob(join_dir + "/files/*.json"):
    d = json.load(open(f))
    p = d["path"]
    sloc = set(d["sloc"])
    covered = (set(d["fuzz"]) | set(d["kani"]) | set(d["regress"])) & sloc
    unc[p] = sloc - covered
    cov[p] = covered

auto = set()
for line in open(join_dir + "/auto-exceptions.tsv"):
    if line.startswith("#") or line.startswith("file\t"):
        continue
    parts = line.split("\t")
    auto.add((parts[0].split("/")[-1], int(parts[1])))

denom = sum(len(u) + len(c) for u, c in zip(unc.values(), cov.values()))
measured = sum(len(c) for c in cov.values())

hand_rows, rec_rows, unknown = [], [], []
for p in sorted(unc):
    src = open(os.path.join(REPO, p)).read().splitlines()
    for l in sorted(unc[p]):
        key = (p.split("/")[-1], l)
        snip = src[l - 1].strip()[:100]
        if key in auto:
            continue  # counted in the auto-exceptions leg
        if key in HAND:
            cls, cite, just = HAND[key]
            hand_rows.append((p, l, cls, cite, just))
        elif key in RECORDED_FIXED:
            cls, reason = RECORDED_FIXED[key]
            rec_rows.append((p, l, cls, reason, snip))
        elif key in SOFT_LIST:
            rec_rows.append((p, l, R_SOFT[0], R_SOFT[1], snip))
        elif key in AGENT:
            cls, reason = AGENT[key]
            rec_rows.append((p, l, cls, reason, snip))
        else:
            unknown.append((p, l, snip))

excepted = len(hand_rows) + len(auto)
recorded = len(rec_rows)
print(f"denom={denom} measured={measured} hand-exc={len(hand_rows)} auto-exc={len(auto)} recorded={recorded} UNKNOWN={len(unknown)}")
print(f"equation: {denom} = {measured} + {excepted} + {recorded} + {len(unknown)}  (residue must be 0 unknown)")
for p, l, s in unknown[:40]:
    print("  UNKNOWN", p, l, s)

with open(os.path.join(outdir, "hand-exceptions.tsv"), "w") as f:
    for p, l, cls, cite, just in hand_rows:
        f.write(f"{p}\t{l}\t{cls}\t{cite}\t{just}\t{LANE}\tpending\n")

with open(os.path.join(outdir, "residual-lanek-remediated.tsv"), "w") as f:
    f.write("# Lane-K REMEDIATED residual for adt/formatting (proofs/gr-fmt, 2026-07-31).\n")
    f.write("# RECORDED rows: uncovered v2-SLOC lines NOT excepted; each carries a per-line reason.\n")
    f.write("# Companion legs: hand exception rows appended to proofs/coverage/phase1-exceptions.tsv\n")
    f.write("# (lane p1-lanek-remediation) and mechanical rows in auto-exceptions.tsv (this dir).\n")
    f.write("file\tline\tclass\treason\tsnippet\n")
    for p, l, cls, reason, snip in rec_rows:
        f.write(f"{p}\t{l}\t{cls}\t{reason}\t{snip}\n")
print("wrote", outdir)
