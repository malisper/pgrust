#!/usr/bin/env python3
"""
Score a ClickBench result against the published c8g.4xlarge leaderboard rows.

Reads a ClickBench-format result JSON (the format produced by run-clickbench.sh
and used by github.com/ClickHouse/ClickBench) and prints TWO clearly labelled
scorings:

  (A) OFFICIAL-SITE RELATIVE SCORING
      The metric the ClickBench website ranks by.  Per query, the ratio
          (t + 10ms) / (baseline_t + 10ms)
      is taken against the baseline system, and the per-query ratios are
      combined with a geometric mean.  The +10 ms is upstream's damping term:
      it stops sub-millisecond queries from dominating the ratio.  A value
      BELOW 1.0 means the system under test is faster than the baseline.

      Upstream computes this over the *best* time per query.  We report the
      geomean separately over the cold column (run 1) and the hot column
      (min of runs 2 and 3), because the two axes behave very differently for
      this engine and averaging them hides that.

  (B) THE "COMBINED" FORMULA USED IN pgrust's OWN REPORTING
          combined = hot_geo^0.6 * cold_geo^0.2 * load_ratio^0.1 * size_ratio^0.1
      This is OUR weighting, not ClickBench's.  It is documented here so the
      auditor can see exactly what it does and disagree with it.  It is the
      formula behind the public "8.6% ahead of ClickHouse" statement.
      Below 1.0 = pgrust ahead.

BOTH numbers are printed every run.  Neither is presented as "the" score.

Usage:
    score-clickbench.py RESULT.json [--baseline baselines/clickhouse-c8g.json]
                                    [--all-baselines]
"""

import argparse
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
BASELINE_DIR = os.path.join(os.path.dirname(HERE), "baselines")

# Upstream ClickBench damping constant, in seconds.  Do not change: it is part
# of the official metric's definition.
DAMP = 0.010

# Weights for the pgrust "combined" formula.  Documented, not standard.
W_HOT, W_COLD, W_LOAD, W_SIZE = 0.6, 0.2, 0.1, 0.1


def load_json(path):
    with open(path) as f:
        return json.load(f)


def columns(result):
    """Split a ClickBench 43x3 matrix into (cold, hot) second-vectors.

    cold = try 1 (the run taken with a cold page cache and a freshly restarted
           server, per the official protocol)
    hot  = min(try 2, try 3)

    A null entry means the query did not complete; we propagate it as None so
    the caller can refuse to score rather than silently skip.
    """
    cold, hot = [], []
    for row in result:
        vals = [None if v is None else float(v) for v in row]
        if len(vals) != 3:
            raise ValueError(f"expected 3 tries per query, got {len(vals)}")
        cold.append(vals[0])
        warm = [v for v in vals[1:] if v is not None]
        hot.append(min(warm) if warm else None)
    return cold, hot


def geomean_ratio(sut, base, label):
    """Official per-query (t+10ms)/(base+10ms) ratio, geometric mean.

    Refuses to score if any query is missing on either side — a partial sweep
    is not comparable to a full one, and quietly dropping queries is the
    classic way to flatter a benchmark result.
    """
    if len(sut) != len(base):
        raise ValueError(f"{label}: query count mismatch {len(sut)} vs {len(base)}")
    missing = [i + 1 for i, (a, b) in enumerate(zip(sut, base)) if a is None or b is None]
    if missing:
        raise ValueError(
            f"{label}: cannot score, missing timings for queries {missing}. "
            "A partial sweep is not comparable to the published full sweep."
        )
    ratios = [(a + DAMP) / (b + DAMP) for a, b in zip(sut, base)]
    logsum = sum(math.log(r) for r in ratios)
    return math.exp(logsum / len(ratios)), ratios


def score(sut, base, verbose=False):
    sut_cold, sut_hot = columns(sut["result"])
    base_cold, base_hot = columns(base["result"])

    hot_geo, hot_ratios = geomean_ratio(sut_hot, base_hot, "hot")
    cold_geo, cold_ratios = geomean_ratio(sut_cold, base_cold, "cold")

    load_ratio = float(sut["load_time"]) / float(base["load_time"])
    size_ratio = float(sut["data_size"]) / float(base["data_size"])

    combined = (
        hot_geo ** W_HOT
        * cold_geo ** W_COLD
        * load_ratio ** W_LOAD
        * size_ratio ** W_SIZE
    )
    return {
        "hot_geo": hot_geo,
        "cold_geo": cold_geo,
        "load_ratio": load_ratio,
        "size_ratio": size_ratio,
        "combined": combined,
        "hot_ratios": hot_ratios,
        "cold_ratios": cold_ratios,
        "sut_hot": sut_hot,
        "sut_cold": sut_cold,
        "base_hot": base_hot,
        "base_cold": base_cold,
    }


def pct(x):
    """Render a ratio as a plain-English lead/deficit."""
    if x < 1.0:
        return f"AHEAD by {(1.0 - x) * 100:.2f}%"
    return f"behind by {(x - 1.0) * 100:.2f}%"


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("result", help="ClickBench-format result JSON from run-clickbench.sh")
    ap.add_argument("--baseline", default=os.path.join(BASELINE_DIR, "clickhouse-c8g.json"))
    ap.add_argument("--all-baselines", action="store_true",
                    help="also score against every other c8g.4xlarge baseline shipped in baselines/")
    ap.add_argument("--per-query", action="store_true", help="print the 43-row per-query table")
    args = ap.parse_args()

    sut = load_json(args.result)
    base = load_json(args.baseline)

    print("=" * 78)
    print("ClickBench scoring")
    print("=" * 78)
    print(f"  system under test : {sut.get('system', '?')}  ({sut.get('machine', '?')})")
    print(f"  result file       : {args.result}")
    print(f"  baseline          : {base.get('system','?')} {base.get('machine','?')}"
          f"  (published {base.get('date','?')})")
    print(f"  baseline file     : {args.baseline}")
    print()
    print("  Baseline provenance: the c8g.4xlarge rows published at")
    print("    https://benchmark.clickhouse.com/  (repo: https://github.com/ClickHouse/ClickBench)")
    print("  vendored under benchmarks/audit/baselines/ so this scoring is")
    print("  reproducible offline and against a pinned snapshot.")
    print()

    s = score(sut, base)

    print("-" * 78)
    print("(A) OFFICIAL-SITE RELATIVE SCORING   [(t+10ms)/(base+10ms), geometric mean]")
    print("-" * 78)
    print("    This is the metric benchmark.clickhouse.com ranks by. <1.0 = faster.")
    print()
    print(f"    hot  geomean (min of tries 2,3) : {s['hot_geo']:.4f}   {pct(s['hot_geo'])}")
    print(f"    cold geomean (try 1)            : {s['cold_geo']:.4f}   {pct(s['cold_geo'])}")
    print()
    print(f"    Sum of 43 hot  times : {sum(s['sut_hot']):9.3f} s   (baseline {sum(s['base_hot']):9.3f} s)")
    print(f"    Sum of 43 cold times : {sum(s['sut_cold']):9.3f} s   (baseline {sum(s['base_cold']):9.3f} s)")
    print()
    print(f"    load time : {sut['load_time']:>10} s   (baseline {base['load_time']:>10} s)"
          f"   ratio {s['load_ratio']:.4f}")
    print(f"    data size : {sut['data_size']:>10} B   (baseline {base['data_size']:>10} B)"
          f"   ratio {s['size_ratio']:.4f}")
    print()

    print("-" * 78)
    print("(B) pgrust's DOCUMENTED COMBINED FORMULA  [NOT the official metric]")
    print("-" * 78)
    print(f"    combined = hot^{W_HOT} * cold^{W_COLD} * load^{W_LOAD} * size^{W_SIZE}")
    print(f"             = {s['hot_geo']:.4f}^{W_HOT} * {s['cold_geo']:.4f}^{W_COLD}"
          f" * {s['load_ratio']:.4f}^{W_LOAD} * {s['size_ratio']:.4f}^{W_SIZE}")
    print()
    print(f"    combined = {s['combined']:.4f}   {pct(s['combined'])} vs {base.get('system','baseline')}")
    print()
    print("    This weighting is pgrust's own choice, not ClickBench's. It weights")
    print("    hot query time at 60% and gives load time and data size 10% each.")
    print("    Read (A) if you want the number the public leaderboard would show.")
    print()

    print("-" * 78)
    print("DRAW FLOOR — read before comparing this run to any other")
    print("-" * 78)
    print("    A single ClickBench cut on this rig is a point draw, not a mean.")
    print("    pgrust's own repeat measurements on this exact leg type spread by")
    print("    1.37 s of summed-43 hot time across five runs (10.358-11.727 s).")
    print("    Any hot-side difference smaller than about 1.4 s of Sigma-43, or")
    print("    about 0.02 of combined score, is NOT a resolvable engine effect.")
    print("    Run the sweep more than once before concluding anything from a")
    print("    small delta.  The cold, load and size axes are far outside that")
    print("    band and are much more stable.")
    print()

    if args.per_query:
        print("-" * 78)
        print("PER-QUERY TABLE  (r_* = (t+10ms)/(base+10ms); <1 = pgrust faster)")
        print("-" * 78)
        print(f"    {'q':>3} {'cold':>9} {'hot':>9} {'base cold':>10} {'base hot':>9} "
              f"{'r_cold':>8} {'r_hot':>8}")
        for i in range(len(s["sut_hot"])):
            print(f"    {i+1:>3} {s['sut_cold'][i]:9.3f} {s['sut_hot'][i]:9.3f} "
                  f"{s['base_cold'][i]:10.3f} {s['base_hot'][i]:9.3f} "
                  f"{s['cold_ratios'][i]:8.3f} {s['hot_ratios'][i]:8.3f}")
        print()
        print("    Note: queries whose baseline time is around 1 ms produce large")
        print("    r_cold values purely from the +10 ms damping term. Those are")
        print("    artefacts of the metric, not 6x engine differences.")
        print()

    if args.all_baselines:
        print("-" * 78)
        print("AGAINST THE REST OF THE PUBLISHED c8g.4xlarge FIELD")
        print("-" * 78)
        print(f"    {'system':<16} {'hot geo':>9} {'cold geo':>9} {'combined':>9}  verdict")
        rows = []
        for fn in sorted(os.listdir(BASELINE_DIR)):
            if not fn.endswith(".json"):
                continue
            b = load_json(os.path.join(BASELINE_DIR, fn))
            try:
                r = score(sut, b)
            except ValueError as e:
                print(f"    {b.get('system', fn):<16}  SKIPPED: {e}")
                continue
            rows.append((b.get("system", fn), r))
        for name, r in sorted(rows, key=lambda kv: kv[1]["combined"]):
            print(f"    {name:<16} {r['hot_geo']:9.4f} {r['cold_geo']:9.4f} "
                  f"{r['combined']:9.4f}  {pct(r['combined'])}")
        print()

    print("=" * 78)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)
