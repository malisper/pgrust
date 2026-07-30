#!/usr/bin/env python3
"""Mechanical ledger-completeness check (see proofs/LEDGER-AUDIT-2026-07-28.md).

Cross-references three sources:
  1. The catalog: crates/backend/utils/fmgr/fmgr_core/src/canonical.rs
     (GENERATED from pg_proc.dat, PG 18.3, prolang=internal, prokind!='a').
  2. The dispatch inventory: every FmgrBuiltin registration in crates/**/*.rs
     (helper-fn calls like `b(oid, "name", ...)` where the helper returns
     FmgrBuiltin, plus `FmgrBuiltin { foid: N, name: "..." }` literals).
  3. The ledger: proofs/USER_FACING_FUNCTIONS.tsv.

Reports:
  - catalog oids with no ledger row (completeness gaps — the audit's failure class)
  - ledger oids not in the catalog, minus the 87 explained extras
    (3 SQL-language builtins pgrust natively implements + 84 prolang=c
     encoding conversions; see the audit doc)
  - catalog oids with no FmgrBuiltin registration (non-fmgr dispatch or
    unimplemented; triaged 2026-07-30, see ledger rows)
  - duplicate oids in ledger or catalog

Exit nonzero iff there are completeness gaps, unexplained extras, or duplicates.
Usage: python3 proofs/check-ledger-completeness.py [repo-root]
"""
import re
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).resolve().parent.parent
CANONICAL = ROOT / "crates/backend/utils/fmgr/fmgr_core/src/canonical.rs"
LEDGER = ROOT / "proofs/USER_FACING_FUNCTIONS.tsv"

# Ledger oids legitimately absent from canonical.rs (LEDGER-AUDIT-2026-07-28.md):
# obj_description / col_description / shobj_description are prolang=sql in PG,
# implemented as native builtins in pgrust; 4302-4387 are prolang=c encoding
# conversions, native builtins in pgrust. (The audit doc says "4302-4387,
# 84 rows": the range holds 86 mb/conv ledger rows, of which two oids are
# also in canonical.rs; membership is intersected with ledger-minus-catalog
# below, so listing the full range is exact.)
EXPLAINED_EXTRAS = {1215, 1216, 1993} | set(range(4302, 4388))

# Registrations built from non-literal oid expressions, resolved by hand
# (audit doc "Method" step 2). Keyed by the identifier that appears in source.
NONLITERAL_REGISTRATIONS = {
    "F_SATISFIES_HASH_PARTITION": 5028,
    # PGRUST_LANE_COVERAGE_FOID: pgrust-internal, not a catalog oid — ignored.
    # InvalidOid sentinel in fmgr_core — ignored.
}


def parse_canonical():
    rows = {}
    pat = re.compile(r'^\s*\((\d+),\s*"([^"]+)",\s*-?\d+,\s*(?:true|false),\s*(?:true|false)\),')
    for line in CANONICAL.read_text().splitlines():
        m = pat.match(line)
        if m:
            oid = int(m.group(1))
            if oid in rows:
                print(f"DUPLICATE catalog oid {oid}")
            rows[oid] = m.group(2)
    return rows


def parse_registrations():
    """oid -> set of files registering it."""
    helper_ret = re.compile(r"fn\s+(\w+)\s*\([^)]*\)\s*->\s*FmgrBuiltin\b")
    lit = re.compile(r"FmgrBuiltin\s*\{\s*foid:\s*(\d+)\b")
    regs = {}
    for f in sorted((ROOT / "crates").rglob("*.rs")):
        rel = f.relative_to(ROOT).as_posix()
        if rel.endswith("fmgr_core/src/tests.rs") or rel.endswith("fmgr_core/src/canonical.rs"):
            continue
        text = f.read_text(errors="replace")
        if "FmgrBuiltin" not in text:
            continue
        helpers = set(helper_ret.findall(text))
        for m in lit.finditer(text):
            regs.setdefault(int(m.group(1)), set()).add(rel)
        if helpers:
            call = re.compile(r"\b(" + "|".join(re.escape(h) for h in helpers) + r")\s*\(\s*(\w+)\s*,")
            for m in call.finditer(text):
                arg = m.group(2)
                if arg.isdigit():
                    regs.setdefault(int(arg), set()).add(rel)
                elif arg in NONLITERAL_REGISTRATIONS:
                    regs.setdefault(NONLITERAL_REGISTRATIONS[arg], set()).add(rel)
    return regs


def parse_ledger():
    lines = LEDGER.read_text().splitlines()
    rows = []
    for i, line in enumerate(lines[1:], start=2):
        if not line.strip():
            continue
        parts = line.split("\t")
        if not parts[0].isdigit():
            print(f"MALFORMED ledger line {i}: {line[:80]}")
            continue
        rows.append((int(parts[0]), parts[1] if len(parts) > 1 else ""))
    return rows


def main():
    catalog = parse_canonical()
    regs = parse_registrations()
    ledger = parse_ledger()
    ledger_oids = [o for o, _ in ledger]
    dup = [o for o, c in Counter(ledger_oids).items() if c > 1]
    ledger_set = set(ledger_oids)

    missing = sorted(set(catalog) - ledger_set)
    extras = sorted(ledger_set - set(catalog) - EXPLAINED_EXTRAS)
    unregistered = sorted(set(catalog) - set(regs))

    print(f"catalog rows (canonical.rs): {len(catalog)}")
    print(f"ledger data rows: {len(ledger)} ({len(ledger_set)} unique oids)")
    print(f"fmgr-registered catalog oids: {len(set(catalog) & set(regs))}")
    print(f"catalog oids with NO FmgrBuiltin registration: {len(unregistered)}")
    print(f"ledger extras explained by audit doc: {len(ledger_set - set(catalog)) - len(extras)}")
    print()
    ok = True
    if dup:
        ok = False
        print(f"FAIL: duplicate ledger oids ({len(dup)}): {dup}")
    if missing:
        ok = False
        print(f"FAIL: catalog oids MISSING from ledger ({len(missing)}):")
        for o in missing:
            print(f"  {o}\t{catalog[o]}")
    if extras:
        ok = False
        print(f"FAIL: ledger oids not in catalog and NOT explained ({len(extras)}): {extras}")
    if ok:
        print("PASS: ledger covers every canonical.rs oid; all extras explained; no duplicates.")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
