#!/usr/bin/env python3
"""reconcile-census.py PASS1 PASS2 ... > FINAL

Combine per-family runner censuses from successive passes into one census with
exactly one disposition per harness: the LAST pass that considered a harness
wins. This is the "fix and re-run the affected harnesses" path — e.g. pass 1
records NAME-UNRESOLVED, pass 2 re-runs those rows with the resolved override.
It never edits a row, only selects among runner-written rows.
"""
import sys

HDR = "family\tsuite_harness\tkani_harness\trc\twall_s\tverdict\tstatus\tkaniraw_new"

def rows(path):
    out = {}
    for line in open(path):
        line = line.rstrip("\n")
        if not line.strip() or line.startswith("#") or line.startswith("family\t"):
            continue
        f = line.split("\t")
        out[(f[0], f[1])] = line
    return out

def main():
    final = {}
    for p in sys.argv[1:]:
        final.update(rows(p))
    print(HDR)
    for k in sorted(final):
        print(final[k])

if __name__ == "__main__":
    main()
