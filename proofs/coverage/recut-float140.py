#!/usr/bin/env python3
"""recut-float140.py — re-derive adt_float's 140-harness per-commit Kani
number (the "338/2,215 = 15.26%" row of proofs/COVERAGE.md) from the
checked-in solver logs, and re-cut it under any SLOC rule.

The 140-harness run's census/kaniraw were destroyed by a scratchpad sweep
(COVERAGE.md, headline table note), but its 140 solver logs survive in
proofs/coverage/instrument-fix/logs/kanicov-*.log and each contains the full
"Source-based code coverage results:" region report — file, generated
function, region span, COVERED/UNCOVERED. That is the same information as
kaniraw, so the number is re-derivable as pure post-processing:

  regions -> per-line coverage (all lines of a COVERED region)
          -> macro-invocation attribution (macro_attrib.py, function-name
             exact match) -> intersect with adt_float SLOC under the rule.

Census caveat: this reconstruction inherits the destroyed census — it proves
what those 140 logs measured, not that 140 was the complete per-commit set.
The verdict lines are in the logs (all VERIFICATION:- SUCCESSFUL), and the
joblist is instrument-fix/joblist140.tsv; treat the output as the documented
re-derivation of the published 338 row, not a fresh capture.

Usage:
  python3 proofs/coverage/recut-float140.py \
      --logs proofs/coverage/instrument-fix/logs \
      --src-sha d824ba3fe9376193455a7ce9202170a3d274ea8c \
      --log-root /Users/malisper/dev/pgrust-fast/.wt-covfix \
      [--sloc-rule v1|v2] [--exclude-const-tables]

--src-sha is exported (git archive) to a temp dir; --log-root is the absolute
worktree prefix the logs' paths refer to.
"""
import argparse, collections, glob, os, re, subprocess, sys, tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sloc_rules  # noqa: E402
from macro_attrib import MacroIndex  # noqa: E402

RE_FILEHDR = re.compile(r"^(/\S+\.rs) \((.+)\)$")
RE_REGION = re.compile(r"^ \* (\d+):\d+ - (\d+):\d+ (COVERED|UNCOVERED)$")


def parse_log(path):
    """Yield (abs_file, function, l0, l1, status) region tuples."""
    cur = None
    started = False
    for line in open(path, encoding="utf-8", errors="replace"):
        line = line.rstrip("\n")
        if line.startswith("Source-based code coverage results:"):
            started = True
            continue
        if not started:
            continue
        m = RE_FILEHDR.match(line)
        if m:
            cur = (m.group(1), m.group(2))
            continue
        m = RE_REGION.match(line)
        if m and cur:
            yield cur[0], cur[1], int(m.group(1)), int(m.group(2)), m.group(3)


def main():
    here = os.path.dirname(os.path.abspath(__file__))
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", default=os.path.join(here, "instrument-fix/logs"))
    ap.add_argument("--src-sha",
                    default="d824ba3fe9376193455a7ce9202170a3d274ea8c")
    ap.add_argument("--log-root",
                    default="/Users/malisper/dev/pgrust-fast/.wt-covfix")
    ap.add_argument("--repo",
                    default=os.path.dirname(os.path.dirname(here)))
    ap.add_argument("--sloc-rule", choices=("v1", "v2"), default="v1")
    ap.add_argument("--exclude-const-tables", action="store_true")
    a = ap.parse_args()
    repo = os.path.realpath(a.repo)

    logs = sorted(glob.glob(os.path.join(a.logs, "kanicov-*.log")))
    if not logs:
        sys.exit(f"no kanicov-*.log under {a.logs}")

    with tempfile.TemporaryDirectory() as tmp:
        # materialize crates/ at the capture sha
        tar = subprocess.Popen(
            ["git", "-C", repo, "archive", a.src_sha, "crates"],
            stdout=subprocess.PIPE)
        subprocess.run(["tar", "-x", "-C", tmp], stdin=tar.stdout, check=True)
        if tar.wait() != 0:
            sys.exit("git archive failed")

        def remap(p):
            if not p.startswith(a.log_root + "/"):
                return None
            return os.path.join(tmp, p[len(a.log_root) + 1:])

        # macro index over the extracted tree (float scope + all macro defs)
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
        scope_dir = os.path.join(tmp, "crates/backend/utils/adt/float")
        for root, _, names in os.walk(scope_dir):
            paths.update(os.path.join(root, nm) for nm in names
                         if nm.endswith(".rs"))
        for root, dirs, names in os.walk(os.path.join(tmp, "crates")):
            dirs[:] = [d for d in dirs if d != "target"]
            for nm in names:
                if nm.endswith(".rs"):
                    p = os.path.join(root, nm)
                    if p not in paths and "macro_rules!" in \
                            open(p, encoding="utf-8", errors="replace").read():
                        paths.add(p)
        for p in sorted(paths):
            idx.add_file(p)
        idx.finalize(lines_of)

        cov = {}  # tmp-abs path -> set(lines)
        st = collections.Counter()
        for lg in logs:
            for fpath, fn, l0, l1, status in parse_log(lg):
                if status != "COVERED":
                    continue
                mp = remap(fpath)
                if mp is None:
                    continue
                if "/crates/backend/utils/adt/float/" in mp:
                    cov.setdefault(mp, set()).update(range(l0, l1 + 1))
                if not idx.in_macro_def(mp, l0, l1):
                    continue
                st["macro_body_regions"] += 1
                hit = idx.resolve(fn)
                if hit is None:
                    st["unresolved"] += 1
                    continue
                hpath, hline = hit
                st["attributed"] += 1
                if "/crates/backend/utils/adt/float/" in hpath:
                    before = len(cov.get(hpath, ()))
                    cov.setdefault(hpath, set()).add(hline)
                    if len(cov[hpath]) != before:
                        st["lines_added"] += 1

        tot_sloc = tot_cov = 0
        print(f"{'file':58} {'sloc':>5} {'kani':>5}")
        for root, _, names in os.walk(scope_dir):
            for nm in sorted(names):
                if not nm.endswith(".rs"):
                    continue
                p = os.path.join(root, nm)
                rel = os.path.relpath(p, tmp)
                text = open(p, encoding="utf-8", errors="replace").read()
                analysis = sloc_rules.analyze_text(text, rel)
                denom, _ = sloc_rules.denominator(
                    analysis, a.sloc_rule,
                    exclude_const_tables=a.exclude_const_tables)
                c = cov.get(p, set()) & denom
                tot_sloc += len(denom)
                tot_cov += len(c)
                print(f"{rel:58} {len(denom):>5} {len(c):>5}")
        pct = 100.0 * tot_cov / tot_sloc if tot_sloc else 0.0
        print(f"\nlogs={len(logs)} rule={a.sloc_rule}"
              f"{'+no-const-tables' if a.exclude_const_tables else ''} "
              f"src={a.src_sha[:12]}")
        print(f"macro attribution: {dict(st)}")
        print(f"FLOAT kani {tot_cov}/{tot_sloc} = {pct:.2f}%")


if __name__ == "__main__":
    main()
