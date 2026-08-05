#!/usr/bin/env python3
"""tree-sloc.py — whole-tree SLOC census under the EXACT same denominator rule
used by proofs/coverage/merge-coverage.py (and documented in proofs/COVERAGE.md).

The rule (copied verbatim in behavior from merge-coverage.py:sloc_lines):

  * only `.rs` files under `<crate>/src/` of every crate in `crates/`
    (a crate = a directory containing Cargo.toml);
  * a file is skipped entirely if it is TEST CODE, decided structurally by
    proofs/coverage/test_scope.py (the module graph: something declares it
    under `#[cfg(test)] mod …`, it carries `#![cfg(test)]`, it lives under a
    `tests/`/`benches/` path, or it holds only `#[test]`/`#[kani::proof]`
    items). Filenames are NOT consulted — the pre-2026-07-31 `^tests.*\\.rs$`
    prefix rule both missed and over-matched;
  * lines inside `#[cfg(test)]`-gated items (tokenizer-parsed item spans,
    braced or braceless) are skipped;
  * `//` line comments and `/* */` block comments are stripped;
  * what remains counts as SLOC iff it holds at least one character other
    than whitespace and the punctuation `{}()[];,`.

Outputs (stdout, plus optional --json):
  1. whole-tree total SLOC + file/crate counts;
  2. per-area breakdown (area = the crate directory's parent, e.g.
     `crates/backend/utils/adt`), sorted by SLOC;
  3. optional per-crate breakdown (--by-crate);
  4. optional scope reproduction check (--scope <file>) — recomputes the
     coverage lane's scope denominator so it can be verified against
     proofs/coverage/summary.json's `totals.sloc`.

Usage:
  python3 proofs/coverage/tree-sloc.py
  python3 proofs/coverage/tree-sloc.py --top 20 --by-crate --json /tmp/tree-sloc.json
  python3 proofs/coverage/tree-sloc.py --scope proofs/coverage/coverage-scope.txt
"""
import argparse, collections, json, os, re, subprocess, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sloc_rules  # single source of truth for the v1/v2 denominator rules
import test_scope  # structural test-code oracle (module graph + tokenizer)

# ---------------------------------------------------------------- SLOC rule
# Rule implementation lives in sloc_rules.py (shared with merge-coverage.py
# and recut-sloc.py). --sloc-rule v2 additionally drops pure control-flow
# syntax lines (else-only / loop-only / unsafe-only / arrow-only); see
# proofs/coverage/SLOC-RULE-V2.md. --exclude-const-tables is the separate
# generated-table knob. This tool has no instrument line tables, so the v2
# text classification is final here (the documented fallback).

_RULE = dict(rule="v1", tables=False)
CLASS_STATS = collections.Counter()

def sloc_lines(path):
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        return set()
    analysis = sloc_rules.analyze_text(text, path)
    denom, excl = sloc_rules.denominator(
        analysis, _RULE["rule"], exclude_const_tables=_RULE["tables"])
    for cls, lns in excl.items():
        CLASS_STATS[cls] += len(lns)
    # count what v2/tables WOULD remove even under v1, for the census
    if _RULE["rule"] == "v1":
        for cls, lns in analysis["structural"].items():
            CLASS_STATS["candidate:" + cls] += len(lns)
    if not _RULE["tables"]:
        CLASS_STATS["candidate:const-table"] += len(analysis["const_table"])
    return denom

# ---------------------------------------------------------------- census

def find_crates(repo, root="crates"):
    """Repo-relative dirs holding a Cargo.toml, excluding any nested under
    another crate's src/ (there are none today; guarded anyway)."""
    crates = []
    for dirpath, dirnames, names in os.walk(os.path.join(repo, root)):
        dirnames[:] = [d for d in dirnames if d not in ("target", ".git")]
        if "Cargo.toml" in names:
            crates.append(os.path.relpath(dirpath, repo))
    return sorted(c for c in crates if "/src/" not in c + "/")

def crate_files(repo, crate):
    """`.rs` files under <crate>/src, repo-relative, deduped and sorted."""
    out = []
    for dirpath, dirnames, names in os.walk(os.path.join(repo, crate, "src")):
        dirnames[:] = [d for d in dirnames if d != "target"]
        for nm in names:
            if nm.endswith(".rs"):
                out.append(os.path.relpath(os.path.join(dirpath, nm), repo))
    return sorted(out)

def census(repo):
    """-> (crate_rows, seen_files) where crate_rows[i] = dict(path, sloc,
    files, files_skipped)."""
    rows = []
    seen = set()
    for c in find_crates(repo):
        sloc = 0
        nf = 0
        skipped = 0
        for rel in crate_files(repo, c):
            if rel in seen:
                continue
            seen.add(rel)
            n = len(sloc_lines(os.path.join(repo, rel)))
            if test_scope.is_test_file(rel):
                skipped += 1
                continue
            sloc += n
            nf += 1
        rows.append(dict(path=c, area=os.path.dirname(c), sloc=sloc,
                         files=nf, test_files_skipped=skipped))
    return rows, seen

def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", default=os.path.dirname(os.path.dirname(here)))
    ap.add_argument("--top", type=int, default=20,
                    help="how many areas to print (0 = all)")
    ap.add_argument("--sloc-rule", choices=("v1", "v2"), default="v2",
                    help="denominator rule (sloc_rules.py / SLOC-RULE-V2.md); "
                         "v2 ADOPTED 2026-07-30")
    ap.add_argument("--exclude-const-tables", action="store_true",
                    dest="exclude_const_tables", default=True,
                    help="drop const/static table interiors — ADOPTED DEFAULT "
                         "(ruling 2026-07-30)")
    ap.add_argument("--include-const-tables", action="store_false",
                    dest="exclude_const_tables",
                    help="keep table interiors (pre-ruling comparability)")
    ap.add_argument("--by-crate", action="store_true",
                    help="also print the top crates by SLOC")
    ap.add_argument("--scope", default=None,
                    help="crate-list file to also total (reproduces the "
                         "coverage lane's scope denominator)")
    ap.add_argument("--json", default=None, help="write machine-readable output here")
    ap.add_argument("--strict-test-scope", action="store_true",
                    help="exit non-zero if any test-scope ambiguity was "
                         "recorded (see test_scope.py: cfg predicates that "
                         "mention `test` without implying it, unresolvable "
                         "`mod x;` targets)")
    a = ap.parse_args()
    _RULE["rule"] = a.sloc_rule
    _RULE["tables"] = a.exclude_const_tables
    repo = os.path.realpath(a.repo)
    test_scope.set_repo_root(repo)

    rows, seen = census(repo)
    total = sum(r["sloc"] for r in rows)
    nfiles = sum(r["files"] for r in rows)

    areas = {}
    for r in rows:
        ar = areas.setdefault(r["area"], dict(area=r["area"], sloc=0, crates=0, files=0))
        ar["sloc"] += r["sloc"]; ar["crates"] += 1; ar["files"] += r["files"]
    area_rows = sorted(areas.values(), key=lambda x: -x["sloc"])

    sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"],
                         capture_output=True, text=True).stdout.strip()

    scope_rows, scope_total = None, None
    if a.scope:
        scope = [l.strip() for l in open(a.scope)
                 if l.strip() and not l.startswith("#")]
        byp = {r["path"]: r for r in rows}
        scope_rows = [byp[c] for c in scope if c in byp]
        missing = [c for c in scope if c not in byp]
        scope_total = sum(r["sloc"] for r in scope_rows)
        if missing:
            print(f"WARN: scope entries not found as crates: {missing}", file=sys.stderr)

    print(f"head_sha  {sha}")
    rule_id = a.sloc_rule + ("+no-const-tables" if a.exclude_const_tables else "")
    print(f"sloc_rule {rule_id}")
    if CLASS_STATS:
        excl = {k: v for k, v in CLASS_STATS.items()
                if not k.startswith("candidate:")}
        cand = {k[10:]: v for k, v in CLASS_STATS.items()
                if k.startswith("candidate:")}
        if excl:
            print("excluded by class: "
                  + " ".join(f"{k}={v}" for k, v in sorted(excl.items())))
        if cand:
            print("would-exclude (candidates under the inactive knobs): "
                  + " ".join(f"{k}={v}" for k, v in sorted(cand.items())))
    print(f"TREE TOTAL  sloc={total}  files={nfiles}  crates={len(rows)}")
    print()
    print(f"{'area':52} {'sloc':>8} {'crates':>7} {'files':>6}  {'%tree':>6}")
    shown = area_rows if a.top == 0 else area_rows[:a.top]
    for r in shown:
        pc = 100.0 * r["sloc"] / total if total else 0.0
        print(f"{r['area']:52} {r['sloc']:>8} {r['crates']:>7} {r['files']:>6}  {pc:>5.2f}%")
    if a.top and len(area_rows) > a.top:
        rest = sum(r["sloc"] for r in area_rows[a.top:])
        print(f"{'(' + str(len(area_rows) - a.top) + ' remaining areas)':52} "
              f"{rest:>8} {'':>7} {'':>6}  {100.0*rest/total:>5.2f}%")

    if a.by_crate:
        print()
        print("top crates by sloc:")
        for r in sorted(rows, key=lambda x: -x["sloc"])[: (a.top or len(rows))]:
            print(f"  {r['path']:60} {r['sloc']:>7}")

    if scope_total is not None:
        print()
        print(f"SCOPE ({len(scope_rows)} crates from {a.scope}):")
        for r in sorted(scope_rows, key=lambda x: x["path"]):
            print(f"  {r['path']:60} {r['sloc']:>7}")
        pc = 100.0 * scope_total / total if total else 0.0
        print(f"  {'SCOPE TOTAL':60} {scope_total:>7}  = {pc:.2f}% of tree")

    if a.json:
        json.dump(dict(head_sha=sha, tree=dict(sloc=total, files=nfiles,
                                               crates=len(rows)),
                       sloc_rule=rule_id + " — see proofs/coverage/sloc_rules.py "
                                 "and proofs/coverage/SLOC-RULE-V2.md",
                       class_stats=dict(CLASS_STATS),
                       areas=area_rows, crates=rows,
                       scope=dict(total=scope_total,
                                  crates=[r["path"] for r in scope_rows])
                             if scope_total is not None else None),
                  open(a.json, "w"), indent=1)

    diag = test_scope.diagnostics()
    if diag:
        print(file=sys.stderr)
        print(f"TEST-SCOPE AMBIGUITIES ({len(diag)}) — kept IN scope, review:",
              file=sys.stderr)
        for d in diag:
            print("  " + d, file=sys.stderr)
        if a.strict_test_scope:
            return 1
    return 0

if __name__ == "__main__":
    sys.exit(main() or 0)
