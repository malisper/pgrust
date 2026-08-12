#!/usr/bin/env python3
"""LD6 drain measure: for each ERROR-ARM chunk row of
docs/fuzzing/line-drain-queue.tsv, count how many of its baseline unhit
lines (the six-arm-union report line-gap-report-001) the in-lane corpus
(cpg-cov/corpus.info) now hits. Reports per-function and per-chunk
before -> after unhit, ranked, plus per-file rollups.

Usage: fuzz/ld6-drain-measure.py [--tsv docs/fuzzing/line-drain-queue.tsv]
                                 [--info cpg-cov/corpus.info]
                                 [--out /dev/stdout]
"""
import argparse
import collections
import re
import sys


def parse_regions(spec):
    lines = set()
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-")
            lines.update(range(int(a), int(b) + 1))
        else:
            lines.add(int(part))
    return lines


def load_info_hits(path):
    """file -> set of hit line numbers (DA:line,count with count>0)."""
    hits = collections.defaultdict(set)
    cur = None
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("SF:"):
                cur = line[3:]
            elif line.startswith("DA:") and cur:
                ln, cnt = line[3:].split(",")[:2]
                if int(cnt) > 0:
                    hits[cur].add(int(ln))
    return hits


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tsv", default="docs/fuzzing/line-drain-queue.tsv")
    ap.add_argument("--info", default="cpg-cov/corpus.info")
    ap.add_argument("--bucket", default="ERROR-ARM")
    ap.add_argument("--top", type=int, default=40)
    args = ap.parse_args()

    hits = load_info_hits(args.info)
    # queue paths look like backend/commands/typecmds.c; info SF paths are
    # absolute into cpg-cov/src. Index info files by trailing path.
    by_suffix = {}
    for f in hits:
        m = re.search(r"src/(backend/.*|common/.*|port/.*)$", f)
        if m:
            by_suffix[m.group(1)] = hits[f]

    rows = []
    chunk_tot = collections.Counter()
    chunk_drained = collections.Counter()
    file_tot = collections.Counter()
    file_drained = collections.Counter()
    with open(args.tsv) as f:
        header = f.readline()
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 8:
                continue
            fn, path, subsys, hit, unhit, chunk, bucket, regions = parts[:8]
            if bucket != args.bucket:
                continue
            base_unhit = parse_regions(regions)
            hit_now = by_suffix.get(path, set())
            drained = base_unhit & hit_now
            rows.append((fn, path, chunk, len(base_unhit), len(drained)))
            chunk_tot[chunk] += len(base_unhit)
            chunk_drained[chunk] += len(drained)
            file_tot[path] += len(base_unhit)
            file_drained[path] += len(drained)

    tot = sum(chunk_tot.values())
    dr = sum(chunk_drained.values())
    print(f"ERROR-ARM bucket: {tot} baseline unhit lines; "
          f"{dr} drained by in-lane corpus ({100.0*dr/max(tot,1):.1f}%)")
    print("\nper chunk:")
    for c in sorted(chunk_tot, key=lambda c: -chunk_tot[c]):
        t, d = chunk_tot[c], chunk_drained[c]
        print(f"  {c:22s} {t:6d} -> {t-d:6d} unhit  ({d} drained, {100.0*d/max(t,1):.1f}%)")
    print("\ntop files:")
    for p in sorted(file_tot, key=lambda p: -file_drained[p])[:20]:
        t, d = file_tot[p], file_drained[p]
        print(f"  {p:48s} {t:5d} -> {t-d:5d} unhit  ({d} drained)")
    print(f"\ntop functions by drained lines (of {len(rows)} rows):")
    for fn, path, chunk, t, d in sorted(rows, key=lambda r: -r[4])[:args.top]:
        print(f"  {fn:42s} {path:44s} {t:4d} -> {t-d:4d}  ({d} drained)")


if __name__ == "__main__":
    sys.exit(main())
