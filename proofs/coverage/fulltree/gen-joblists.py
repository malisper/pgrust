#!/usr/bin/env python3
"""gen-joblists.py — derive the full-tree Kani coverage joblist from SUITE.tsv.

Per COVERAGE.md known-distortion 7's capture rule: the joblist is derived from
SUITE.tsv, never from which families happen to build locally or from kaniraw
globs.

Emits, under proofs/coverage/fulltree/:
  joblists/<family>.tsv       one per family with any tier=per-commit row:
                              family<TAB>suite_harness<TAB>[override]
                              (override column left empty here; resolve-names.py
                              fills it from `cargo kani list` output)
  census/census-unmeasured.tsv census rows (status FAILED-TO-RUN, verdict
                              NOT-ATTEMPTED-FLEET-SOLVED) for every SUITE row
                              with expected=unmeasured — the fleet-solved /
                              dark-sweep registrations. These are NOT run
                              locally; the census records them UNMEASURED so
                              their functions cannot masquerade as uncovered.
  waivers.tsv                 --allow-unmeasured rows for exactly those.
  families.txt                run order (per-commit families, cheap first,
                              string/jsonb-heavy last) with per-family timeout.

Scope cut (recorded here because the census can only defend what it expects):
  - RUN:    every tier=per-commit row (the per-commit tier of the suite).
  - WAIVE:  every expected=unmeasured row (fleet-solved; no local solve leg).
  - OUT:    calibration / release-gate / defect-witness tiers, and
            tier=unmeasured rows whose expected!=unmeasured (locally-solved
            quickwin registrations never assigned a tier). Out-of-scope tiers
            are documented in COVERAGE.md, not silently absent: this docstring
            plus COVERAGE.md's tier statement are the record.
"""
import collections, os, sys

HERE = os.path.dirname(os.path.abspath(__file__))
SUITE = os.path.join(HERE, "..", "..", "SUITE.tsv")

# COVERAGE.md: 300 s for the string/jsonb-heavy families, 900 s elsewhere.
STRING_HEAVY = {"text-slice", "text-cmp", "bytea-cmp", "bytea-varbit",
                "json-escape", "jsonb-probe", "jsonb-gin", "utf8",
                "unicode-cat", "ascii-case", "oracle-compat"}
WAIVER_REASON = ("fleet-solved family registration (expected=unmeasured in "
                 "SUITE.tsv, dark-harness sweep); no local coverage leg — "
                 "COVERAGE.md known distortion 7 rule (b)")

def main():
    percommit = collections.OrderedDict()   # family -> [(harness, time_s)]
    unmeasured = []                          # (family, harness)
    with open(SUITE) as f:
        header = f.readline()
        for line in f:
            c = line.rstrip("\n").split("\t")
            if len(c) < 5 or not c[0]:
                continue
            fam, harness, expected, tier = c[0], c[1], c[3], c[4]
            t = c[5] if len(c) > 5 else "?"
            if tier == "per-commit":
                percommit.setdefault(fam, []).append((harness, t))
            if expected == "unmeasured":
                unmeasured.append((fam, harness))

    os.makedirs(os.path.join(HERE, "joblists"), exist_ok=True)
    os.makedirs(os.path.join(HERE, "census"), exist_ok=True)

    def cost(fam):
        s = 0.0
        for _, t in percommit[fam]:
            try:
                s += float(t.rstrip("s"))
            except ValueError:
                s += 10.0  # unknown time: assume mid
        return s

    n = 0
    for fam, rows in percommit.items():
        with open(os.path.join(HERE, "joblists", fam + ".tsv"), "w") as out:
            for h, _ in rows:
                out.write(f"{fam}\t{h}\t\n")
                n += 1

    # run order: cheap solve-sum first, string-heavy last
    fams = sorted(percommit, key=lambda f: (f in STRING_HEAVY, cost(f)))
    with open(os.path.join(HERE, "families.txt"), "w") as out:
        for fam in fams:
            t = 300 if fam in STRING_HEAVY else 900
            out.write(f"{fam}\t{t}\t{len(percommit[fam])}\t{cost(fam):.1f}\n")

    with open(os.path.join(HERE, "census", "census-unmeasured.tsv"), "w") as out:
        out.write("family\tsuite_harness\tkani_harness\trc\twall_s\tverdict"
                  "\tstatus\tkaniraw_new\n")
        for fam, h in unmeasured:
            out.write(f"{fam}\t{h}\t{h}\t-\t0\tNOT-ATTEMPTED-FLEET-SOLVED"
                      f"\tFAILED-TO-RUN\t0\n")
    with open(os.path.join(HERE, "waivers.tsv"), "w") as out:
        for fam, h in unmeasured:
            out.write(f"{fam}\t{h}\t{WAIVER_REASON}\n")

    print(f"per-commit: {n} harnesses across {len(percommit)} families")
    print(f"expected=unmeasured (waived, not run): {len(unmeasured)}")

if __name__ == "__main__":
    main()
