#!/usr/bin/env python3
"""merge-coverage.py — unify Kani proof coverage, differential-fuzz coverage,
and pg_regress suite coverage into one line-granularity report over SHIPPED
pgrust source (crates/ only; harness/vendored-C lines are dropped on the
floor by path filtering).

Inputs (all optional; a missing source just reports zero coverage):
  --kani-glob   glob(s) for kanicov *kaniraw.json files (repeatable)
  --fuzz-lcov   lcov export(s) from cargo-fuzz coverage profdata (repeatable)
  --regress-lcov lcov export(s) from the instrumented-server regress run
  --repo        repo root (default: two dirs up from this file)
  --scope       file listing crate dirs (relative to repo) that get per-file
                detail + per-crate rows (default: coverage-scope.txt herein)
  --outdir      output dir (default: this file's dir)

SLOC rule (the shared denominator, applied identically to all sources):
a line counts as SLOC iff, after stripping // comments and /* */ block
comments, it contains at least one character other than whitespace and the
punctuation {}()[];, — i.e. blank lines, comment-only lines, and lone
brace/bracket lines are excluded. This is a TEXTUAL approximation, not a
compiler-grade statement count; it is documented in proofs/COVERAGE.md and
its whole job is to be the same yardstick for all three sources.

Coverage attribution:
  kani:    every SLOC line intersecting a region with status COVERED in any
           kaniraw file, for files under <repo>/crates/. Kani regions are
           span-granular; a multi-line region marks all its lines. PLUS the
           macro-invocation correction (see proofs/coverage/macro_attrib.py):
           a region inside a `macro_rules!` definition body also credits the
           unique invocation line that declares the generated function named
           in the region's `function` field. Without it, every macro-declared
           fmgr wrapper is uncoverable by construction (measured: 3.25pp
           undercount in adt_float alone).
  fuzz:    lcov DA:<line>,<count> with count>0, crates/ files only.
  regress: same, from the server profile.

FAIL-CLOSED CENSUS (2026-07-30 hardening)
-----------------------------------------
A harness that fails to RUN produces no kaniraw and, in a pipeline that globs
results at the end, is indistinguishable from a harness that ran and covered
nothing. That is how the `bool` family's 20 harnesses — all rc=1 under a wrong
`proofs::` qualification — would have merged as 20 harnesses' worth of
"legitimately uncovered" code (SMOKE-RESULT.md §7 blocker 2), the same
gate-blindness class as the 96 harnesses that never ran under `--exact`.

So this script refuses to emit a summary unless every EXPECTED harness is
accounted for. Pass the runner's census (`--census`, one or more TSVs written
by run-kani-coverage.sh) and, if some harnesses are knowingly not measured, an
explicit waiver file (`--allow-unmeasured`) whose every row carries a reason.
Every expected harness must end up in exactly one bucket — ran / walled /
failed-to-run — and any failed-to-run without a waiver is a hard error with no
summary.json written. Never print a bare coverage percentage for a run whose
census did not close.

Outputs in --outdir:
  summary.json                    (schema in proofs/COVERAGE.md)
  verification-coverage.tsv       (per-file + per-crate + TOTAL rows)
  files/<crate-slug>/<file>.json  (per-file line detail, lazy-loadable)
  census.json                     (expected/ran/walled/failed-to-run roll-up)
"""
import argparse, collections, glob, json, os, re, sys, datetime, subprocess

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from macro_attrib import MacroIndex  # noqa: E402
import sloc_rules  # noqa: E402  (v1/v2 denominator rules — single source of truth)
import test_scope  # noqa: E402  (structural test-code oracle: module graph)

# v1 compatibility shims (tree-sloc.py and older tooling import these).
cfg_test_spans = sloc_rules.cfg_test_spans

def sloc_lines(path):
    """v1 denominator for a file (compatibility wrapper; see sloc_rules.py)."""
    return sloc_rules.sloc_lines(path, "v1")

def build_macro_index(repo, scope):
    """Index every file that could hold a macro-invocation declaration line we
    might need to credit (the scope crates) plus every file that DEFINES a
    macro_rules! (so a region landing in a macro defined in another crate is
    recognised as a definition-body region, and #[macro_export] generators are
    known)."""
    idx = MacroIndex()
    cache = {}

    def lines_of(p):
        if p not in cache:
            try:
                cache[p] = open(p, encoding="utf-8",
                                errors="replace").read().splitlines()
            except OSError:
                cache[p] = []
        return cache[p]

    paths = set()
    for c in scope:
        for root, _, names in os.walk(os.path.join(repo, c)):
            if "/target/" in root:
                continue
            for nm in names:
                if nm.endswith(".rs"):
                    paths.add(os.path.join(root, nm))
    for root, dirs, names in os.walk(os.path.join(repo, "crates")):
        dirs[:] = [d for d in dirs if d != "target"]
        for nm in names:
            if not nm.endswith(".rs"):
                continue
            p = os.path.join(root, nm)
            if p in paths:
                continue
            try:
                with open(p, encoding="utf-8", errors="replace") as fh:
                    if "macro_rules!" in fh.read():
                        paths.add(p)
            except OSError:
                pass
    for p in sorted(paths):
        idx.add_file(p)
    idx.finalize(lines_of)
    return idx


def load_kani(globs, repo, idx=None):
    """Merge kaniraw COVERED regions into {relpath: set(lines)}.

    Returns (cov, n_raw_files, raw_paths, macro_stats). See the module docstring
    for the macro-invocation correction; `macro_stats` publishes its residual:
    regions inside a macro definition body whose generated function could not be
    resolved to a unique declaration line are counted as `unresolved`, not
    silently dropped."""
    cov = {}  # relpath -> set(lines)
    raw_paths = []
    st = collections.Counter()
    unresolved_names = collections.Counter()
    for g in globs:
        for f in sorted(glob.glob(g, recursive=True)):
            try:
                data = json.load(open(f)).get("data", {})
            except (json.JSONDecodeError, OSError) as e:
                print(f"WARN: skipping unreadable kaniraw {f}: {e}", file=sys.stderr)
                continue
            raw_paths.append(f)
            for fpath, regions in data.items():
                in_crates = fpath.startswith(repo + "/crates/")
                rel = os.path.relpath(fpath, repo) if in_crates else None
                for r in regions:
                    if r.get("status") != "COVERED":
                        continue
                    (l0, _), (l1, _) = r["region"]["start"], r["region"]["end"]
                    if in_crates:
                        cov.setdefault(rel, set()).update(range(l0, l1 + 1))
                    # --- macro-invocation correction -----------------------
                    # A region inside a macro_rules! BODY belongs textually to
                    # the macro template; the generated function's own SLOC
                    # line is the invocation that declares it. Credit that line
                    # too, but only on an exact, unique function-name match.
                    if idx is None or not idx.in_macro_def(fpath, l0, l1):
                        continue
                    st["macro_body_regions"] += 1
                    fn = r.get("function") or ""
                    hit = idx.resolve(fn)
                    if hit is None:
                        st["unresolved"] += 1
                        unresolved_names[fn.split("::")[-1]] += 1
                        continue
                    hpath, hline = hit
                    if not hpath.startswith(repo + "/crates/"):
                        st["out_of_tree"] += 1
                        continue
                    hrel = os.path.relpath(hpath, repo)
                    st["attributed"] += 1
                    before = len(cov.get(hrel, ()))
                    cov.setdefault(hrel, set()).add(hline)
                    if len(cov[hrel]) != before:
                        st["lines_added"] += 1
    macro_stats = dict(st)
    macro_stats["unresolved_functions"] = dict(unresolved_names.most_common(20))
    return cov, len(raw_paths), raw_paths, macro_stats

def load_lcov(paths, repo, any_count=False):
    """{relpath: set(lines)} from lcov DA records.

    any_count=False (default): executed lines only (count > 0) — coverage.
    any_count=True: every DA record including count 0 — the instrument's LINE
    TABLE, used by --sloc-rule v2 to reinstate structural candidates the
    instrument itself considers mappable."""
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
                cur = os.path.relpath(f, repo) if f.startswith(repo + "/crates/") else None
            elif line.startswith("DA:") and cur:
                ln, cnt = line[3:].split(",")[:2]
                if any_count or int(cnt) > 0:
                    cov.setdefault(cur, set()).add(int(ln))
            elif line == "end_of_record":
                cur = None
    return cov

CENSUS_COLS = ("family", "suite_harness", "kani_harness", "rc", "wall_s",
               "verdict", "status", "kaniraw_new")
CENSUS_STATUSES = ("RAN", "WALLED", "FAILED-TO-RUN", "NOFLAGS")


def read_census(paths):
    """Read runner census TSVs. Returns (rows, errors).

    Row shape is the header above; unknown statuses are an error rather than a
    silently ignored row (a status typo must not remove a harness from the
    denominator — that is exactly the hole this census closes)."""
    rows, errs = [], []
    for p in paths:
        for lineno, line in enumerate(open(p), 1):
            line = line.rstrip("\n")
            if not line.strip() or line.startswith("#"):
                continue
            f = line.split("\t")
            if f[0] == "family":
                continue  # header
            if len(f) < len(CENSUS_COLS):
                errs.append(f"{p}:{lineno}: census row has {len(f)} of "
                            f"{len(CENSUS_COLS)} columns: {line!r}")
                continue
            row = dict(zip(CENSUS_COLS, f))
            row["_src"] = f"{p}:{lineno}"
            if row["status"] not in CENSUS_STATUSES:
                errs.append(f"{p}:{lineno}: unknown status {row['status']!r} "
                            f"(expected one of {CENSUS_STATUSES})")
                continue
            rows.append(row)
    return rows, errs


def read_waivers(paths):
    """Explicit walled/excluded list: family<TAB>harness<TAB>reason.
    A waiver with an empty reason is rejected — "excluded" without a stated
    reason is how a silent undercount gets laundered into a green."""
    waivers, errs = {}, []
    for p in paths:
        for lineno, line in enumerate(open(p), 1):
            line = line.rstrip("\n")
            if not line.strip() or line.startswith("#"):
                continue
            f = line.split("\t")
            if len(f) < 3 or not f[2].strip():
                errs.append(f"{p}:{lineno}: waiver needs "
                            f"family<TAB>harness<TAB>reason (non-empty): "
                            f"{line!r}")
                continue
            waivers[(f[0], f[1])] = f[2].strip()
    return waivers, errs


def kaniraw_has(raw_paths, kani_harness):
    """Did this harness produce a kaniraw file?

    Kani names the file after the harness's mangled symbol, e.g.
    `_RNvNtCs..._11proof_casts6proofs8eq_dtoi4_kaniraw.json` for
    `proofs::eq_dtoi4`. Match the length-prefixed terminal segment so
    `eq_dtoi4` cannot be satisfied by `eq_dtoi4_wide`."""
    base = kani_harness.split("::")[-1]
    pat = re.compile(r"\d+" + re.escape(base) + r"_kaniraw\.json$")
    return [p for p in raw_paths if pat.search(os.path.basename(p))]


def run_census(census_paths, waiver_paths, raw_paths):
    """Reconcile expected harnesses against produced coverage artifacts.

    Returns (report_dict, errors, warnings). errors non-empty => no summary."""
    rows, errs = read_census(census_paths)
    waivers, werrs = read_waivers(waiver_paths)
    errs = list(errs) + werrs

    expected = len(rows)
    buckets = collections.Counter()
    ran, walled, failed, waived = [], [], [], []
    seen = set()
    for r in rows:
        key = (r["family"], r["suite_harness"])
        if key in seen:
            errs.append(f"{r['_src']}: duplicate census row for {key} — the "
                        f"census must have one disposition per harness")
            continue
        seen.add(key)
        produced = kaniraw_has(raw_paths, r["kani_harness"])
        status = r["status"]
        if status == "RAN" and not produced:
            # Ran clean but emitted nothing: the artifact is missing, so this
            # harness measured NOTHING. Treat as failed-to-run, never as zero.
            status = "FAILED-TO-RUN"
            r = dict(r, status=status,
                     verdict=r["verdict"] + "+NO-KANIRAW")
        if status == "RAN":
            buckets["ran"] += 1
            ran.append(key)
        elif status == "WALLED":
            buckets["walled"] += 1
            walled.append(key)
            if key not in waivers:
                errs.append(f"{r['_src']}: {key[0]}/{key[1]} WALLED "
                            f"({r['verdict']}) but is not in "
                            f"--allow-unmeasured; a wall is unmeasured "
                            f"coverage and needs a stated reason")
            else:
                waived.append(key)
        else:
            buckets["failed_to_run"] += 1
            failed.append((key, r["verdict"], r["rc"]))
            if key not in waivers:
                errs.append(
                    f"{r['_src']}: {key[0]}/{key[1]} status={status} "
                    f"rc={r['rc']} verdict={r['verdict']} produced NO "
                    f"coverage. UNMEASURED, not uncovered. Fix the harness "
                    f"name/run, or waive it explicitly in "
                    f"--allow-unmeasured with a reason.")
            else:
                waived.append(key)

    accounted = buckets["ran"] + buckets["walled"] + buckets["failed_to_run"]
    census_ok = accounted == len(seen) and not errs
    orphan = [os.path.basename(p) for p in raw_paths
              if not any(kaniraw_has([p], r["kani_harness"]) for r in rows)]

    print("\n== coverage census ==")
    print(f"  expected harnesses (census rows): {expected}")
    print(f"  ran (kaniraw produced):           {buckets['ran']}")
    print(f"  walled (timeout/OOM):             {buckets['walled']}")
    print(f"  failed to run (UNMEASURED):       {buckets['failed_to_run']}")
    print(f"  waived with a stated reason:      {len(waived)}")
    print(f"  kaniraw files merged:             {len(raw_paths)}")
    if orphan:
        print(f"  kaniraw with no census row:       {len(orphan)} "
              f"(e.g. {orphan[:3]})")
    for key, verdict, rc in failed[:40]:
        mark = "WAIVED" if key in waivers else "ERROR "
        print(f"    {mark} {key[0]}/{key[1]} rc={rc} {verdict}"
              + (f" — {waivers[key]}" if key in waivers else ""))
    if accounted != len(seen):
        print(f"\nCENSUS FAIL: ran({buckets['ran']}) + "
              f"walled({buckets['walled']}) + "
              f"failed({buckets['failed_to_run']}) = {accounted} != "
              f"{len(seen)} census rows — dispositions were lost; this run "
              f"certifies nothing.")

    report = dict(expected=expected, ran=buckets["ran"],
                  walled=buckets["walled"],
                  failed_to_run=buckets["failed_to_run"],
                  waived=len(waived), kaniraw_files=len(raw_paths),
                  kaniraw_without_census_row=len(orphan),
                  census_closed=bool(census_ok),
                  unmeasured=[dict(family=k[0], harness=k[1], verdict=v, rc=rc,
                                   waiver=waivers.get(k))
                              for k, v, rc in failed]
                             + [dict(family=k[0], harness=k[1],
                                     verdict="WALLED", rc="124",
                                     waiver=waivers.get(k)) for k in walled])
    return report, errs


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--kani-glob", action="append", default=[])
    ap.add_argument("--fuzz-lcov", action="append", default=[])
    ap.add_argument("--regress-lcov", action="append", default=[])
    ap.add_argument("--repo", default=os.path.dirname(os.path.dirname(here)))
    ap.add_argument("--scope", default=os.path.join(here, "coverage-scope.txt"))
    ap.add_argument("--outdir", default=here)
    ap.add_argument("--census", action="append", default=[],
                    help="runner census TSV (repeatable). Required unless "
                         "--no-census-required; see run-kani-coverage.sh")
    ap.add_argument("--allow-unmeasured", action="append", default=[],
                    help="TSV family<TAB>harness<TAB>reason waiving harnesses "
                         "that produced no coverage")
    ap.add_argument("--no-census-required", action="store_true",
                    help="ONLY for ad-hoc local exploration: emit a summary "
                         "with no census. The summary is stamped "
                         "census_closed=false and must not be published.")
    ap.add_argument("--no-macro-attribution", action="store_true",
                    help="disable the macro-invocation correction (for "
                         "measuring its size; the raw number is biased low)")
    ap.add_argument("--sloc-rule", choices=("v1", "v2"), default="v2",
                    help="denominator rule (see proofs/coverage/sloc_rules.py "
                         "and proofs/coverage/SLOC-RULE-V2.md). v2 ADOPTED "
                         "2026-07-30; v1 kept for comparability.")
    ap.add_argument("--line-table-lcov", action="append", default=[],
                    help="FULL lcov export(s) (DA records incl. count 0) used "
                         "as the instrument line table under --sloc-rule v2: "
                         "a structural-candidate line with a DA record is "
                         "reinstated into the denominator. Pass the raw "
                         "capture lcov files (regress/fuzz) here.")
    ap.add_argument("--auto-exceptions", action="store_true",
                    help="after the merge, mechanically classify the "
                         "uncovered residual's KNOWN instrument-unmappable "
                         "shapes (auto: rows, distinct from hand-adjudicated "
                         "exception rows) into <outdir>/auto-exceptions.tsv. "
                         "Classes + honesty rules: proofs/coverage/"
                         "auto_exceptions.py, licensed by rig-auto-classes.py. "
                         "--line-table-lcov inputs double as the DA veto/"
                         "evidence tables.")
    ap.add_argument("--exclude-const-tables", action="store_true",
                    dest="exclude_const_tables", default=True,
                    help="drop interior lines of multi-line const/static "
                         "bracket initializers (data tables) from the "
                         "denominator. ADOPTED DEFAULT (ruling 2026-07-30); "
                         "every excluded span is published in "
                         "excluded-tables.json.")
    ap.add_argument("--include-const-tables", action="store_false",
                    dest="exclude_const_tables",
                    help="keep table interiors in the denominator (pre-ruling "
                         "comparability)")
    a = ap.parse_args()
    repo = os.path.realpath(a.repo)
    test_scope.set_repo_root(repo)

    scope = [l.strip() for l in open(a.scope) if l.strip() and not l.startswith("#")]

    idx = None if a.no_macro_attribution else build_macro_index(repo, scope)
    kani, nraw, raw_paths, macro_stats = load_kani(a.kani_glob, repo, idx)
    fuzz = load_lcov(a.fuzz_lcov, repo)
    regress = load_lcov(a.regress_lcov, repo)
    line_tables = (load_lcov(a.line_table_lcov, repo, any_count=True)
                   if a.line_table_lcov else {})

    excl_stats = collections.Counter()
    table_inv = []
    _inv_seen = set()

    def denom_of(rel):
        """(denominator set, excluded_by_class) for one repo-relative file
        under the active rule. Pure function of (source, line tables). Feeds
        the excluded-tables inventory as a side effect (once per file)."""
        try:
            text = open(os.path.join(repo, rel), encoding="utf-8",
                        errors="replace").read()
        except OSError:
            return set(), {}
        analysis = sloc_rules.analyze_text(text, rel)
        if a.exclude_const_tables and rel not in _inv_seen:
            _inv_seen.add(rel)
            table_inv.extend(sloc_rules.table_inventory(analysis, rel, text))
        return sloc_rules.denominator(
            analysis, a.sloc_rule,
            exclude_const_tables=a.exclude_const_tables,
            line_table=line_tables.get(rel))

    # ---- fail closed BEFORE any number is produced -------------------------
    if a.census:
        census, cerrs = run_census(a.census, a.allow_unmeasured, raw_paths)
    elif a.no_census_required:
        census, cerrs = dict(expected=None, census_closed=False,
                             kaniraw_files=len(raw_paths),
                             note="run with --no-census-required: harnesses "
                                  "that failed to run are indistinguishable "
                                  "from covered-nothing; DO NOT PUBLISH"), []
        print("\n== coverage census ==\n  NOT REQUESTED (--no-census-required)."
              f" {len(raw_paths)} kaniraw merged, expected count UNKNOWN.\n"
              "  A harness that failed to run is silently folded into "
              "'uncovered' in this mode. Exploration only.")
    else:
        print("ERROR: no --census given. A coverage percentage merged from a "
              "glob cannot distinguish a harness that ran and covered nothing "
              "from a harness that never ran (SMOKE-RESULT.md §7 blocker 2). "
              "Pass the runner's census TSV, or --no-census-required for "
              "unpublishable local exploration.", file=sys.stderr)
        sys.exit(2)
    if cerrs:
        for e in cerrs:
            print(f"CENSUS ERROR {e}")
        print(f"\nmerge-coverage: REFUSING to write a summary — "
              f"{len(cerrs)} census error(s). Unmeasured harnesses would be "
              f"reported as uncovered code, which is a silent, confident "
              f"undercount. No summary.json, no percentages.", file=sys.stderr)
        os.makedirs(a.outdir, exist_ok=True)
        json.dump(dict(census=census, errors=cerrs),
                  open(os.path.join(a.outdir, "census.json"), "w"), indent=1)
        sys.exit(3)

    def crate_of(rel):
        for c in scope:
            if rel.startswith(c + "/"):
                return c
        return None

    # per-file rows for every source file in scope crates
    files = {}
    for c in scope:
        for root, _, names in os.walk(os.path.join(repo, c, "src")):
            for nm in names:
                if nm.endswith(".rs"):
                    rel = os.path.relpath(os.path.join(root, nm), repo)
                    files[rel] = None
    for rel in set(list(kani) + list(fuzz) + list(regress)):
        if crate_of(rel):
            files.setdefault(rel, None)

    os.makedirs(os.path.join(a.outdir, "files"), exist_ok=True)
    tsv = ["file\tsloc\tkani\tfuzz\tregress\tany"]
    crates = {}
    tot = dict(sloc=0, kani=0, fuzz=0, regress=0, any=0)
    for rel in sorted(files):
        sl, excl = denom_of(rel)
        for cls, lns in excl.items():
            excl_stats[cls] += len(lns)
        k = kani.get(rel, set()) & sl
        fz = fuzz.get(rel, set()) & sl
        rg = regress.get(rel, set()) & sl
        anyl = k | fz | rg
        c = crate_of(rel)
        crec = crates.setdefault(c, dict(sloc=0, kani=0, fuzz=0, regress=0, any=0, files=[]))
        for key, v in (("sloc", len(sl)), ("kani", len(k)), ("fuzz", len(fz)),
                       ("regress", len(rg)), ("any", len(anyl))):
            crec[key] += v
            tot[key] += v
        slug = rel.replace("/", "__")
        detail = os.path.join("files", slug + ".json")
        drec = {"path": rel, "sloc": sorted(sl), "kani": sorted(k),
                "fuzz": sorted(fz), "regress": sorted(rg)}
        if excl:
            drec["excluded"] = excl  # {class: [lines]} — render neutral
        json.dump(drec,
                  open(os.path.join(a.outdir, detail), "w"), separators=(",", ":"))
        crec["files"].append(dict(path=rel, sloc=len(sl), kani=len(k), fuzz=len(fz),
                                  regress=len(rg), any=len(anyl), detail=detail))
        tsv.append(f"{rel}\t{len(sl)}\t{len(k)}\t{len(fz)}\t{len(rg)}\t{len(anyl)}")

    def pct(n, d):
        return round(100.0 * n / d, 2) if d else 0.0

    crate_rows = []
    for c in sorted(crates):
        r = crates[c]
        crate_rows.append(dict(name=os.path.basename(c), path=c, sloc=r["sloc"],
                               kani=r["kani"], fuzz=r["fuzz"], regress=r["regress"],
                               any=r["any"],
                               pct=dict(kani=pct(r["kani"], r["sloc"]),
                                        fuzz=pct(r["fuzz"], r["sloc"]),
                                        regress=pct(r["regress"], r["sloc"]),
                                        any=pct(r["any"], r["sloc"])),
                               files=r["files"]))
        tsv.append(f"CRATE:{c}\t{r['sloc']}\t{r['kani']}\t{r['fuzz']}\t{r['regress']}\t{r['any']}")
    tsv.append(f"TOTAL\t{tot['sloc']}\t{tot['kani']}\t{tot['fuzz']}\t{tot['regress']}\t{tot['any']}")

    # whole-tree (all crates/ files seen by the dynamic sources; kani is
    # scope-bounded so a whole-tree kani % would be misleading — omitted)
    tree = {}
    for src, covmap in (("fuzz", fuzz), ("regress", regress)):
        covered = 0; sloc_total = 0
        for rel, lines in covmap.items():
            sl, _ = denom_of(rel)
            sloc_total += len(sl)
            covered += len(lines & sl)
        tree[src] = dict(files_touched=len(covmap), covered_sloc_in_touched_files=covered,
                         sloc_of_touched_files=sloc_total)

    sha = subprocess.run(["git", "-C", repo, "rev-parse", "HEAD"],
                         capture_output=True, text=True).stdout.strip()
    summary = dict(
        generated=datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        head_sha=sha,
        sloc_rule=("non-blank, non-comment-only lines containing at least one "
                   "character besides whitespace and {}()[];, — see proofs/COVERAGE.md"
                   if a.sloc_rule == "v1" else
                   "v1 minus pure control-flow syntax lines (else-only/loop-only/"
                   "unsafe-only/arrow-only), instrument line tables reinstating "
                   "— see proofs/coverage/SLOC-RULE-V2.md"),
        sloc_rule_id=a.sloc_rule
                     + ("+no-const-tables" if a.exclude_const_tables else ""),
        sloc_exclusions=dict(excl_stats),
        line_table_inputs=a.line_table_lcov,
        census=census,
        macro_attribution=dict(
            enabled=not a.no_macro_attribution,
            rule="a COVERED region inside a macro_rules! definition body also "
                 "credits the unique source line that declares the generated "
                 "function named in the region's `function` field; see "
                 "proofs/coverage/macro_attrib.py",
            stats=macro_stats,
            index=(idx.stats if idx else None)),
        sources=dict(
            kani=dict(kind="kani-source-coverage", raw_files=nraw,
                      meaning="region reachable under some proof harness's FENCED domain"),
            fuzz=dict(kind="llvm-lcov", inputs=a.fuzz_lcov,
                      meaning="line executed by at least one corpus input of a differential fuzz target"),
            regress=dict(kind="llvm-lcov", inputs=a.regress_lcov,
                         meaning="line executed while serving the pg_regress schedule ONCE"),
        ),
        caveat="A covered line is NOT a verified line. See proofs/COVERAGE.md.",
        scope=scope,
        totals=dict(**tot, pct=dict(kani=pct(tot["kani"], tot["sloc"]),
                                    fuzz=pct(tot["fuzz"], tot["sloc"]),
                                    regress=pct(tot["regress"], tot["sloc"]),
                                    any=pct(tot["any"], tot["sloc"]))),
        tree_touched=tree,
        crates=crate_rows,
    )
    json.dump(summary, open(os.path.join(a.outdir, "summary.json"), "w"), indent=1)
    json.dump(census, open(os.path.join(a.outdir, "census.json"), "w"), indent=1)
    if a.exclude_const_tables:
        table_inv.sort(key=lambda r: -r["lines"])
        json.dump(dict(
            rule=summary["sloc_rule_id"],
            note="every const-table span excluded from the denominator; "
                 "reason 'const-array-heuristic' rows are the ones to "
                 "review — a span swallowing real logic is a defect",
            total_spans=len(table_inv),
            total_lines=sum(r["lines"] for r in table_inv),
            by_reason={k: sum(1 for r in table_inv if r["reason"] == k)
                       for k in ("generated-file", "const-array-heuristic")},
            spans=table_inv),
            open(os.path.join(a.outdir, "excluded-tables.json"), "w"), indent=1)
    open(os.path.join(a.outdir, "verification-coverage.tsv"), "w").write("\n".join(tsv) + "\n")
    if a.auto_exceptions:
        import auto_exceptions
        # Line tables default to this merge's own raw lcov inputs: a full
        # lcov (DA records incl. count 0) is llvm's per-line mappability
        # verdict, and the evidence classes (fmt-cont, call-str-cont) can
        # only classify against one. Without it those rows land in
        # no_table_evidence and become gate-eyeball work.
        line_tables = list(a.line_table_lcov) or \
            list(a.fuzz_lcov) + list(a.regress_lcov)
        if not a.line_table_lcov and line_tables:
            print(f"auto-exceptions: no --line-table-lcov given; using this "
                  f"merge's own lcov inputs as the line table: "
                  f"{line_tables}")
        rows, astats = auto_exceptions.classify_outdir(
            a.outdir, repo, line_tables)
        auto_exceptions.write_tsv(
            rows, os.path.join(a.outdir, "auto-exceptions.tsv"), astats)
        summary["auto_exceptions"] = dict(
            rows=len(rows),
            by_class=collections.Counter(r["cls"] for r in rows),
            line_tables=line_tables,
            stats={k: v for k, v in astats.items()
                   if not k.startswith("auto:")},
            note="mechanical instrument-unmappable classification; see "
                 "auto-exceptions.tsv header. auto: rows are measurement "
                 "notes, never denominator changes.")
        json.dump(summary, open(os.path.join(a.outdir, "summary.json"), "w"),
                  indent=1)
        print(f"auto-exceptions: {len(rows)} rows -> auto-exceptions.tsv")
        held = {k: v for k, v in astats.items()
                if k.startswith("no_table_evidence")}
        if held:
            print(f"auto-exceptions: {sum(held.values())} shape-matched "
                  f"line(s) HELD for lack of line-table evidence "
                  f"({held}).\n  To auto-resolve them, re-run with the "
                  f"capture's FULL lcov files (DA records incl. count 0 — "
                  f"the raw regress/fuzz capture artifacts, e.g. "
                  f"regress.lcov and fuzz-<target>.lcov, the same files "
                  f"passed to --fuzz-lcov/--regress-lcov) as: "
                  f"--line-table-lcov <file> [--line-table-lcov <file> …]. "
                  f"Preserve those lcov files with the capture — this is "
                  f"also the SLOC-v2 line-table precedence input.")
    if idx:
        print(f"macro attribution: {macro_stats.get('attributed', 0)} regions "
              f"attributed to invocation lines "
              f"({macro_stats.get('lines_added', 0)} distinct SLOC lines "
              f"credited), {macro_stats.get('unresolved', 0)} unresolved of "
              f"{macro_stats.get('macro_body_regions', 0)} macro-body regions")
        if macro_stats.get("unresolved"):
            print(f"  UNRESOLVED (residual undercount, top names): "
                  f"{list(macro_stats['unresolved_functions'])[:8]}")
    t = summary["totals"]
    print(f"TOTAL sloc={t['sloc']} kani={t['kani']}({t['pct']['kani']}%) "
          f"fuzz={t['fuzz']}({t['pct']['fuzz']}%) regress={t['regress']}({t['pct']['regress']}%) "
          f"any={t['any']}({t['pct']['any']}%)")

if __name__ == "__main__":
    main()
