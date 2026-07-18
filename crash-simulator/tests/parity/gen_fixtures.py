#!/usr/bin/env python3
"""G-O2 classifier-parity fixture generator (WS-ORACLE).

Feeds the SAME inputs to triage.py's screens + classify() ladder that the
Rust port (src/oracle/classifier.rs) consumes, and records triage.py's
answers as ground truth. The Rust parity test then replays the fixtures and
must agree per statement (contract §3.4 G-O2).

Two fixture surfaces:
  - screens: underdetermined / VOLATILE_RE / NONDET_RE for EVERY corpus
    statement (real sqlsmith repro corpora = the pinned parity corpus);
  - matrix: the full classify() decision ladder over a synthetic
    outcome-pair matrix x the first --matrix-limit statements (screen
    interplay: the same pair classifies differently on volatile/nondet/
    underdetermined statements).

The live-engine leg of G-O2 (same statements through two booted engines)
rides WS-RUNNER's session driver on harness/h1-integration; this generator
pins the decision-logic parity, which is where drift can silently occur.

Usage:
  gen_fixtures.py --triage scripts/sqlsmith/triage.py \
      --corpus scripts/sqlsmith/repros-campaign1.sql [...] \
      --out fixtures-screens.jsonl [--matrix-limit 200]
"""
import argparse
import importlib.util
import json
import sys

# The synthetic outcome vocabulary. Names are the cross-language contract:
# tests/simharness_oracle_parity.rs constructs the same RunStatus values
# from these names. Do not change one side without the other.
OUTCOMES = {
    "ok_a": ("ok", (1, 3, "h1", False)),
    "ok_b": ("ok", (1, 3, "h2", False)),
    "ok_capped": ("ok", (2, -1, "capped", True)),
    "err_syntax": ("error", ("42601", 'syntax error at or near "x"')),
    "err_syntax2": ("error", ("42601", 'syntax error at or near "y"')),
    "err_div": ("error", ("22012", "division by zero")),
    "err_undef": ("error", ("42P01", "relation does not exist")),
    "err_cov": ("error", ("XX000", "not yet ported: foo")),
    "err_xx_other": ("error", ("XX000", "weird failure mode")),
    "err_timeout": ("error", ("57014", "canceling statement due to statement timeout")),
    "err_none": ("error", (None, "strange")),
    "crash": ("crash", "server closed the connection unexpectedly"),
    "fetch": ("fetch", "year 0 is out of range"),
}

PAIRS = [
    ("ok_a", "ok_a"),
    ("ok_a", "ok_b"),
    ("ok_capped", "ok_a"),
    ("err_syntax", "ok_a"),
    ("err_cov", "ok_a"),
    ("err_xx_other", "ok_a"),
    ("ok_a", "err_div"),
    ("err_syntax", "err_syntax"),
    ("err_syntax", "err_syntax2"),
    ("err_syntax", "err_undef"),
    ("err_syntax", "err_div"),
    ("err_timeout", "err_div"),
    ("err_none", "err_syntax"),
    ("crash", "ok_a"),
    ("ok_a", "crash"),
    ("fetch", "ok_a"),
    ("ok_a", "fetch"),
]


def load_triage(path):
    spec = importlib.util.spec_from_file_location("triage", path)
    mod = importlib.util.module_from_spec(spec)
    # triage.py hard-requires psycopg2 at import; stub it if absent (we only
    # need the pure functions: split_statements, underdetermined, regexes,
    # classify).
    if "psycopg2" not in sys.modules:
        try:
            import psycopg2  # noqa: F401
        except ImportError:
            import types

            stub = types.ModuleType("psycopg2")
            stub.Error = Exception
            sys.modules["psycopg2"] = stub
    spec.loader.exec_module(mod)
    return mod


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--triage", required=True)
    ap.add_argument("--corpus", required=True, nargs="+")
    ap.add_argument("--out", required=True)
    ap.add_argument("--matrix-limit", type=int, default=200)
    ap.add_argument("--stmt-limit", type=int, default=0, help="cap statements per corpus")
    args = ap.parse_args()

    tri = load_triage(args.triage)

    stmts = []
    for path in args.corpus:
        with open(path, encoding="utf-8", errors="replace") as f:
            got = tri.split_statements(f.read())
        if args.stmt_limit:
            got = got[: args.stmt_limit]
        stmts.extend(got)

    n_matrix = 0
    with open(args.out, "w", encoding="utf-8") as out:
        header = {
            "kind": "header",
            "pairs": [list(p) for p in PAIRS],
            "statements": len(stmts),
        }
        out.write(json.dumps(header, sort_keys=True) + "\n")
        for i, stmt in enumerate(stmts):
            rec = {
                "kind": "stmt",
                "i": i,
                "stmt": stmt,
                "screens": {
                    "underdetermined": tri.underdetermined(stmt),
                    "volatile": bool(tri.VOLATILE_RE.search(stmt)),
                    "nondet": bool(tri.NONDET_RE.search(stmt)),
                },
            }
            if i < args.matrix_limit:
                matrix = {}
                for rn, cn in PAIRS:
                    cls, sev = tri.classify(stmt, OUTCOMES[rn], OUTCOMES[cn])
                    matrix[f"{rn}|{cn}"] = [cls, sev]
                rec["matrix"] = matrix
                n_matrix += 1
            out.write(json.dumps(rec, sort_keys=True) + "\n")

    print(f"gen_fixtures: {len(stmts)} statements, matrix on {n_matrix}, -> {args.out}")


if __name__ == "__main__":
    main()
