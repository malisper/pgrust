#!/usr/bin/env python3
"""recut-sloc.py — re-cut an EXISTING coverage capture under a different SLOC
rule, as pure post-processing. No instrument is re-run; the covered-line sets
in files/<slug>.json are taken as-is, sources are read at the capture's own
head_sha via `git show`, and only the denominator (and the intersection of the
covered sets with it) changes.

    python3 proofs/coverage/recut-sloc.py <indir> <outdir> \
        --sloc-rule v2 [--exclude-const-tables] [--line-table-lcov F.lcov ...]
        [--repo REPO]

<indir> is a merge-coverage.py output dir (summary.json + files/). <outdir>
gets the same artifact shape (summary.json, files/, verification-coverage.tsv)
recomputed under the requested rule, plus recut-delta.json describing exactly
what moved (per-crate SLOC deltas, reclassified lines by class, and how many
of them were previously-red).

This is the contract that lets the full-tree capture running under v1 be
re-cut under v2 without re-solving anything: its raw kaniraw/lcov artifacts
are untouched; the per-file covered sets they produced are sufficient.

Line-table caveat: covered sets only record executed lines, so the v2
line-table reinstatement needs the capture's FULL lcov files (DA incl. 0). If
they were not preserved, run without --line-table-lcov: the text rule is the
defined fallback wherever no line table exists.
"""
import argparse, collections, json, os, subprocess, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sloc_rules  # noqa: E402
import test_scope  # noqa: E402  (structural test-code oracle: module graph)


def load_line_tables(paths, repo):
    cov = {}
    cur = None
    for p in paths:
        for line in open(p):
            line = line.strip()
            if line.startswith("SF:"):
                f = line[3:]
                if not os.path.isabs(f):
                    f = os.path.join(repo, f)
                f = os.path.realpath(f)
                cur = (os.path.relpath(f, repo)
                       if f.startswith(repo + "/crates/") else None)
            elif line.startswith("DA:") and cur:
                cov.setdefault(cur, set()).add(int(line[3:].split(",")[0]))
            elif line == "end_of_record":
                cur = None
    return cov


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("indir")
    ap.add_argument("outdir")
    ap.add_argument("--sloc-rule", choices=("v1", "v2"), default="v2")
    ap.add_argument("--exclude-const-tables", action="store_true",
                    dest="exclude_const_tables", default=True,
                    help="ADOPTED DEFAULT (ruling 2026-07-30)")
    ap.add_argument("--include-const-tables", action="store_false",
                    dest="exclude_const_tables",
                    help="keep table interiors (pre-ruling comparability)")
    ap.add_argument("--line-table-lcov", action="append", default=[])
    ap.add_argument("--accept-universe-drift", action="store_true",
                    help="proceed when the recomputed v1 universe differs from "
                         "the captured one, printing the per-file drift. The "
                         "2026-07-31 test-scope fix legitimately moves the "
                         "universe of files that contain test-gated items, so "
                         "a pre-fix capture needs this flag; a mismatch NOT "
                         "explained by that fix means the capture and the "
                         "sources disagree — do not pass the flag then.")
    ap.add_argument("--repo", default=None,
                    help="repo whose git objects hold the capture's head_sha "
                         "(default: two dirs up from this script)")
    a = ap.parse_args()

    here = os.path.dirname(os.path.abspath(__file__))
    repo = os.path.realpath(a.repo or os.path.dirname(os.path.dirname(here)))
    test_scope.set_repo_root(repo)
    summary = json.load(open(os.path.join(a.indir, "summary.json")))
    sha = summary["head_sha"]
    line_tables = load_line_tables(a.line_table_lcov, repo)

    def source_at(rel):
        r = subprocess.run(["git", "-C", repo, "show", f"{sha}:{rel}"],
                           capture_output=True, text=True)
        if r.returncode != 0:
            print(f"FATAL: cannot read {rel} at {sha}: {r.stderr.strip()}",
                  file=sys.stderr)
            sys.exit(1)
        return r.stdout

    os.makedirs(os.path.join(a.outdir, "files"), exist_ok=True)
    tsv = ["file\tsloc\tkani\tfuzz\tregress\tany"]
    excl_stats = collections.Counter()
    table_inv = []
    red_reclassified = collections.Counter()  # class -> previously-red lines removed
    cov_reclassified = collections.Counter()  # class -> previously-covered removed
    crate_rows = []
    universe_drift = []
    tot = dict(sloc=0, kani=0, fuzz=0, regress=0, any=0)
    delta_crates = []

    def pct(n, d):
        return round(100.0 * n / d, 2) if d else 0.0

    for cr in summary["crates"]:
        crec = dict(sloc=0, kani=0, fuzz=0, regress=0, any=0, files=[])
        old = dict(sloc=cr["sloc"], kani=cr["kani"], any=cr["any"])
        for fr in cr["files"]:
            d = json.load(open(os.path.join(a.indir, fr["detail"])))
            rel = d["path"]
            v1_sloc = set(d["sloc"])
            text = source_at(rel)
            analysis = sloc_rules.analyze_text(text, rel)
            if a.exclude_const_tables:
                table_inv.extend(sloc_rules.table_inventory(analysis, rel, text))
            if analysis["universe"] != v1_sloc:
                msg = (f"v1 universe mismatch for {rel} at {sha}: recomputed "
                       f"{len(analysis['universe'])} vs captured "
                       f"{len(v1_sloc)} lines "
                       f"(+{len(analysis['universe'] - v1_sloc)}/"
                       f"-{len(v1_sloc - analysis['universe'])}).")
                if not a.accept_universe_drift:
                    print(f"FATAL: {msg} Either the capture was not made under "
                          f"rule v1 of these sources, or it predates the "
                          f"2026-07-31 test-scope fix (which moves the universe "
                          f"of files holding test-gated items). Re-run with "
                          f"--accept-universe-drift ONLY if the latter, after "
                          f"checking the printed drift is confined to "
                          f"test-gated spans; refusing to re-cut.",
                          file=sys.stderr)
                    sys.exit(1)
                print(f"UNIVERSE-DRIFT: {msg} newly-in="
                      f"{sorted(analysis['universe'] - v1_sloc)[:20]} newly-out="
                      f"{sorted(v1_sloc - analysis['universe'])[:20]}",
                      file=sys.stderr)
                universe_drift.append(dict(
                    path=rel, newly_in=sorted(analysis["universe"] - v1_sloc),
                    newly_out=sorted(v1_sloc - analysis["universe"])))
            denom, excl = sloc_rules.denominator(
                analysis, a.sloc_rule,
                exclude_const_tables=a.exclude_const_tables,
                line_table=line_tables.get(rel))
            oldcov = {s: set(d[s]) for s in ("kani", "fuzz", "regress")}
            oldany = oldcov["kani"] | oldcov["fuzz"] | oldcov["regress"]
            for cls, lns in excl.items():
                excl_stats[cls] += len(lns)
                red_reclassified[cls] += len(set(lns) - oldany)
                cov_reclassified[cls] += len(set(lns) & oldany)
            k, fz, rg = (oldcov[s] & denom for s in ("kani", "fuzz", "regress"))
            anyl = k | fz | rg
            for key, v in (("sloc", len(denom)), ("kani", len(k)),
                           ("fuzz", len(fz)), ("regress", len(rg)),
                           ("any", len(anyl))):
                crec[key] += v
                tot[key] += v
            drec = {"path": rel, "sloc": sorted(denom), "kani": sorted(k),
                    "fuzz": sorted(fz), "regress": sorted(rg)}
            if excl:
                drec["excluded"] = excl
            json.dump(drec, open(os.path.join(a.outdir, fr["detail"]), "w"),
                      separators=(",", ":"))
            crec["files"].append(dict(path=rel, sloc=len(denom), kani=len(k),
                                      fuzz=len(fz), regress=len(rg),
                                      any=len(anyl), detail=fr["detail"]))
            tsv.append(f"{rel}\t{len(denom)}\t{len(k)}\t{len(fz)}\t{len(rg)}\t{len(anyl)}")
        crate_rows.append(dict(
            name=cr["name"], path=cr["path"], sloc=crec["sloc"],
            kani=crec["kani"], fuzz=crec["fuzz"], regress=crec["regress"],
            any=crec["any"],
            pct=dict(kani=pct(crec["kani"], crec["sloc"]),
                     fuzz=pct(crec["fuzz"], crec["sloc"]),
                     regress=pct(crec["regress"], crec["sloc"]),
                     any=pct(crec["any"], crec["sloc"])),
            files=crec["files"]))
        tsv.append(f"CRATE:{cr['path']}\t{crec['sloc']}\t{crec['kani']}"
                   f"\t{crec['fuzz']}\t{crec['regress']}\t{crec['any']}")
        delta_crates.append(dict(
            name=cr["name"], sloc_v1=old["sloc"], sloc=crec["sloc"],
            sloc_delta=crec["sloc"] - old["sloc"],
            kani_v1=old["kani"], kani=crec["kani"],
            any_pct_v1=pct(old["any"], old["sloc"]),
            any_pct=pct(crec["any"], crec["sloc"])))
    tsv.append(f"TOTAL\t{tot['sloc']}\t{tot['kani']}\t{tot['fuzz']}"
               f"\t{tot['regress']}\t{tot['any']}")

    out = dict(summary)
    out["sloc_rule_id"] = (a.sloc_rule
                           + ("+no-const-tables" if a.exclude_const_tables else ""))
    out["sloc_rule"] = ("v1 minus pure control-flow syntax lines — see "
                        "proofs/coverage/SLOC-RULE-V2.md"
                        if a.sloc_rule == "v2" else summary["sloc_rule"])
    out["sloc_exclusions"] = dict(excl_stats)
    out["recut_from"] = dict(indir=os.path.abspath(a.indir),
                             original_rule=summary.get("sloc_rule_id", "v1"),
                             line_table_inputs=a.line_table_lcov)
    out["totals"] = dict(**tot, pct=dict(kani=pct(tot["kani"], tot["sloc"]),
                                         fuzz=pct(tot["fuzz"], tot["sloc"]),
                                         regress=pct(tot["regress"], tot["sloc"]),
                                         any=pct(tot["any"], tot["sloc"])))
    out["crates"] = crate_rows
    json.dump(out, open(os.path.join(a.outdir, "summary.json"), "w"), indent=1)
    open(os.path.join(a.outdir, "verification-coverage.tsv"),
         "w").write("\n".join(tsv) + "\n")
    if a.exclude_const_tables:
        table_inv.sort(key=lambda r: -r["lines"])
        json.dump(dict(rule=out["sloc_rule_id"],
                       note="every const-table span excluded from the "
                            "denominator; review the const-array-heuristic "
                            "rows — a span swallowing real logic is a defect",
                       total_spans=len(table_inv),
                       total_lines=sum(r["lines"] for r in table_inv),
                       by_reason={k: sum(1 for r in table_inv if r["reason"] == k)
                                  for k in ("generated-file", "const-array-heuristic")},
                       spans=table_inv),
                  open(os.path.join(a.outdir, "excluded-tables.json"), "w"),
                  indent=1)
    delta = dict(rule=out["sloc_rule_id"], head_sha=sha,
                 excluded_by_class=dict(excl_stats),
                 previously_red_reclassified=dict(red_reclassified),
                 previously_covered_reclassified=dict(cov_reclassified),
                 totals_v1=summary["totals"], totals=out["totals"],
                 universe_drift=universe_drift,
                 test_scope_diagnostics=test_scope.diagnostics(),
                 crates=delta_crates)
    json.dump(delta, open(os.path.join(a.outdir, "recut-delta.json"), "w"),
              indent=1)

    t, o = out["totals"], summary["totals"]
    print(f"re-cut {summary.get('sloc_rule_id', 'v1')} -> {out['sloc_rule_id']}"
          f" at {sha[:12]}")
    print(f"  sloc {o['sloc']} -> {t['sloc']} ({t['sloc']-o['sloc']:+d})")
    for cls in sorted(excl_stats):
        print(f"    {cls:12} -{excl_stats[cls]:5} "
              f"(previously red {red_reclassified[cls]}, "
              f"previously covered {cov_reclassified[cls]})")
    for s in ("kani", "fuzz", "regress", "any"):
        print(f"  {s:8} {o[s]:>6} ({o['pct'][s]:6.2f}%) -> "
              f"{t[s]:>6} ({t['pct'][s]:6.2f}%)")


if __name__ == "__main__":
    main()
