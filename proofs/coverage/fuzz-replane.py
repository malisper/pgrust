#!/usr/bin/env python3
"""fuzz-replane.py — refresh the FUZZ plane of the central coverage merge
(proofs/coverage/files/*.json + proofs/coverage/summary.json) from every
banked+validated fuzz lcov, so the coverage viewer stops under-reporting the
campaign's fuzz coverage (the 2026-07-31 full-tree capture only ever ingested
the 3 coverage-rf lcovs: float_in/float_out/geo -> fuzz=1,182 tree-wide while
the campaign's real fuzz mass lives in per-lane banked lcovs).

What it does (fuzz plane ONLY — kani/regress/sloc untouched):
  1. reads every lcov in MANIFEST (git ref + path; gz or plain) — banked
     local cov-export replays, plus post-covcap-fix fleet captures only
     (pre-a9f7920aba23 fleet lcovs are excluded: ASan fake-stack guard
     collapse made them partial for stack-guarded targets, see
     proofs/coverage/covcap-fleet/README.md);
  2. remaps SF paths to repo-relative crates/** and unions DA>0 lines;
  3. per file: fuzz := fuzz ∪ (covered ∩ (sloc ∪ excluded-universe));
     covered lines outside the file's line universe are counted and
     reported (drift signal), never silently added;
  4. creates files/*.json for covered files that have none (new crates since
     the e395 capture, e.g. dict_snowball), cutting sloc with the CURRENT
     sloc_rules (v2 + tables excluded + lcov DA line-table reinstatement);
  5. rebuilds the summary.json crate rollups touched, recomputes totals, and
     stamps provenance under key "fuzz_plane_refresh".

Run from anywhere inside the repo; writes proofs/coverage/ in the CURRENT
worktree. Report goes to stdout + proofs/coverage/fuzz-replane/REPORT.tsv.
"""
import gzip
import io
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(os.path.dirname(HERE))
sys.path.insert(0, HERE)
import sloc_rules  # noqa: E402
import test_scope  # noqa: E402

# test_scope's default repo root is proofs/ (two levels above its own file);
# without this, is_test_file silently classifies NOTHING as test code.
test_scope.set_repo_root(REPO)

# (git ref, path) — "WT" reads the working tree. Selection rules documented
# in the module docstring; the EXCLUDED-on-purpose list is at the bottom.
PIN_EVIDENCE_REBUILD_2 = "985c4f2caa7ec953bcd02229d15ca57e680af6fe"
PIN_GATE_REMEDIATION = "0c9fb1e47367744964eef2626717ef38234299c3"
PIN_COV_RESWEEP = "a594887353161e3130aa451f7bfba1915226f11a"
PIN_P1_SNOWBALL = "4b1bbb982397f7e2e7b57004bfae88f72a0d1c57"

EXCLUDED_ON_PURPOSE = [
    # (path, reason)
    ("proofs/coverage/fulltree/rf-remap/regress.lcov", "regress plane, not fuzz"),
    ("proofs/coverage/lanez/confirm-coverage-mbconv.lcov",
     "PRE-covcap-fix fleet capture (2026-07-31) — partial for stack-guarded "
     "targets; mb/conv coverage of record is the gate-remediation local join"),
]


def manifest():
    rows = []
    # 1. working tree: every fuzz lcov banked on main under proofs/coverage
    excl = {p for p, _ in EXCLUDED_ON_PURPOSE}
    out = subprocess.run(
        ["git", "-C", REPO, "ls-files", "proofs/coverage"],
        capture_output=True, text=True, check=True).stdout
    for p in out.splitlines():
        if re.search(r"\.lcov(\.gz)?$", p) and p not in excl:
            rows.append(("WT", p))
    # 2. unlanded evidence branches (same pins as phase1-number.py)
    for ref, sub in ((PIN_EVIDENCE_REBUILD_2, "proofs/coverage/evidence-rebuild-2"),
                     (PIN_GATE_REMEDIATION, "proofs/coverage/gate-remediation"),
                     (PIN_COV_RESWEEP, "proofs/coverage/cov-resweep")):
        out = subprocess.run(["git", "-C", REPO, "ls-tree", "-r",
                              "--name-only", ref, sub],
                             capture_output=True, text=True, check=True).stdout
        for p in out.splitlines():
            if re.search(r"\.lcov(\.gz)?$", p):
                rows.append((ref, p))
    # 3. p1-snowball lane branch: the three POST-covcap-fix fleet campaigns
    #    (1785601073 / 1785603060 / 1785603076 — all after the a9f7920aba23
    #    runner fix; earlier fleet-fuzz-results on that branch are pre-fix
    #    and are NOT ingested)
    out = subprocess.run(["git", "-C", REPO, "ls-tree", "-r", "--name-only",
                          PIN_P1_SNOWBALL, "fleet-fuzz-results"],
                         capture_output=True, text=True, check=True).stdout
    for p in out.splitlines():
        if p.endswith("coverage.lcov") and any(
                c in p for c in ("1785601073", "1785603060", "1785603076",
                                 "1785571714")):
            # 1785601073/1785603060/1785603076 = snowball_diff /
            # snowball_runtime_diff; 1785571714 = geo_io_diff + geo_ops_diff
            # (p1-geo-inc3 lane) — all four campaigns POST the a9f7920aba23
            # runner fix. Earlier fleet-fuzz-results on this branch are
            # pre-fix and are NOT ingested.
            rows.append((PIN_P1_SNOWBALL, p))
    return rows


def read_blob(ref, path):
    if ref == "WT":
        data = open(os.path.join(REPO, path), "rb").read()
    else:
        data = subprocess.run(["git", "-C", REPO, "show", f"{ref}:{path}"],
                              capture_output=True, check=True).stdout
    if path.endswith(".gz"):
        data = gzip.decompress(data)
    return data.decode("utf-8", errors="replace")


def parse_lcov(text):
    """-> {repo_rel_path: set(covered line numbers)} (crates/** only)."""
    per = defaultdict(set)
    cur = None
    for ln in text.splitlines():
        if ln.startswith("SF:"):
            sf = ln[3:].strip()
            i = sf.rfind("/crates/")
            cur = sf[i + 1:] if i != -1 else (
                sf if sf.startswith("crates/") else None)
        elif ln.startswith("DA:") and cur:
            try:
                n, cnt = ln[3:].split(",")[:2]
                if int(cnt) > 0:
                    per[cur].add(int(n))
            except ValueError:
                pass
    return per


def slug(path):
    return path.replace("/", "__").replace("crates__", "crates__", 1)


def file_json_path(path):
    return os.path.join(HERE, "files", path.replace("/", "__") + ".json")


def new_file_json(path):
    """Cut a fresh per-file record with the current (fixed) sloc rules."""
    ap = os.path.join(REPO, path)
    if not os.path.exists(ap):
        return None
    text = open(ap, encoding="utf-8", errors="replace").read()
    analysis = sloc_rules.analyze_text(text, path)
    denom, excluded = sloc_rules.denominator(
        analysis, "v2", exclude_const_tables=True)
    return dict(path=path, sloc=sorted(denom), kani=[], fuzz=[], regress=[],
                excluded={k: v for k, v in excluded.items()})


def main():
    man = manifest()
    print(f"manifest: {len(man)} lcov sources")
    union = defaultdict(set)
    for ref, p in man:
        try:
            per = parse_lcov(read_blob(ref, p))
        except Exception as e:
            print(f"  ERROR reading {ref[:10]}:{p}: {e}")
            continue
        nl = sum(len(v) for v in per.values())
        print(f"  {ref[:10] if ref != 'WT' else 'worktree':10s} {p}  "
              f"files={len(per)} covered-lines={nl}")
        for f, lines in per.items():
            union[f] |= lines

    # Files whose stored sloc predates the 2026-08-01 sloc_rules fix
    # (static-mut data tables were counted IN): recut their denominator with
    # the fixed rule, carrying measured planes across (intersected with the
    # new universe). Measured blast radius of the rule fix = exactly this
    # crate (tree-wide old-vs-new diff, see REFRESH 3 doc).
    RECUT_SLOC_PREFIXES = ("crates/backend/snowball/dict_snowball/",)

    report = []
    touched_crates = set()
    created = []

    # recut pass: EVERY stored file json under the prefixes, covered or not
    import glob as _glob
    slug_prefixes = tuple(p.rstrip("/").replace("/", "__") + "__"
                          for p in RECUT_SLOC_PREFIXES)
    for jp in _glob.glob(os.path.join(HERE, "files", "*.json")):
        if not os.path.basename(jp).startswith(slug_prefixes):
            continue
        d = json.load(open(jp))
        path = d.get("path", "")
        fresh = new_file_json(d.get("path", path))
        if fresh is None:
            continue
        uni = set(fresh["sloc"])
        for k2 in ("kani", "fuzz", "regress"):
            fresh[k2] = sorted(set(d.get(k2) or []) & uni)
        json.dump(fresh, open(jp, "w"))
        touched_crates.add(fresh["path"])

    for path in sorted(union):
        jp = file_json_path(path)
        if os.path.exists(jp):
            d = json.load(open(jp))
        else:
            d = new_file_json(path)
            if d is None:
                report.append((path, "SKIP-no-source", 0, 0, 0))
                continue
            created.append(path)
        sloc = set(d.get("sloc") or [])
        exc_universe = set()
        for v in (d.get("excluded") or {}).values():
            exc_universe.update(v)
        universe = sloc | exc_universe
        cov = union[path]
        add = (cov & universe) - set(d.get("fuzz") or [])
        outside = cov - universe
        if add:
            d["fuzz"] = sorted(set(d.get("fuzz") or []) | add)
            json.dump(d, open(jp, "w"))
        report.append((path, "ok", len(add), len(cov), len(outside)))
        touched_crates.add(path)

    # ---- rebuild summary.json rollups -----------------------------------
    sp = os.path.join(HERE, "summary.json")
    s = json.load(open(sp))
    by_crate_path = {c["path"]: c for c in s["crates"]}

    def crate_of(path):
        # crate dir = longest prefix that is a key in summary, else derive
        # by Cargo.toml walk
        parts = path.split("/")
        for i in range(len(parts) - 1, 0, -1):
            pref = "/".join(parts[:i])
            if pref in by_crate_path:
                return pref
        # walk up for a Cargo.toml (new crate since the capture)
        d = os.path.dirname(path)
        while d and d != "crates":
            if os.path.exists(os.path.join(REPO, d, "Cargo.toml")):
                return d
            d = os.path.dirname(d)
        return None

    recompute = defaultdict(set)
    for path in touched_crates:
        c = crate_of(path)
        if c:
            recompute[c].add(path)
        else:
            print(f"  WARN no crate for {path}")

    for cpath, paths in sorted(recompute.items()):
        row = by_crate_path.get(cpath)
        if row is None:
            row = dict(name=os.path.basename(cpath), path=cpath,
                       sloc=0, kani=0, fuzz=0, regress=0, any=0,
                       pct={}, files=[])
            s["crates"].append(row)
            by_crate_path[cpath] = row
        by_file = {f["path"]: f for f in row["files"]}
        for path in paths:
            fj = json.load(open(file_json_path(path)))
            fr = by_file.get(path)
            if fr is None:
                fr = dict(path=path, sloc=0, kani=0, fuzz=0, regress=0,
                          any=0, detail="files/" + path.replace("/", "__") + ".json")
                row["files"].append(fr)
                by_file[path] = fr
            k, fz, rg, sl = (set(fj.get("kani") or []), set(fj.get("fuzz") or []),
                             set(fj.get("regress") or []), set(fj.get("sloc") or []))
            fr["sloc"] = len(sl)
            fr["kani"], fr["fuzz"], fr["regress"] = len(k & sl), len(fz & sl), len(rg & sl)
            fr["any"] = len((k | fz | rg) & sl)
        for key in ("sloc", "kani", "fuzz", "regress", "any"):
            row[key] = sum(f[key] for f in row["files"])
        row["pct"] = {k2: round(100.0 * row[k2] / row["sloc"], 2) if row["sloc"] else 0.0
                      for k2 in ("kani", "fuzz", "regress", "any")}

    t = dict(sloc=0, kani=0, fuzz=0, regress=0, any=0)
    for row in s["crates"]:
        for k in t:
            t[k] += row.get(k, 0)
    s["totals"] = dict(**t, pct={k: round(100.0 * t[k] / t["sloc"], 2)
                                 for k in ("kani", "fuzz", "regress", "any")})
    s["fuzz_plane_refresh"] = dict(
        generated=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        tool="proofs/coverage/fuzz-replane.py",
        sources=len(man),
        pins=dict(evidence_rebuild_2=PIN_EVIDENCE_REBUILD_2,
                  gate_remediation=PIN_GATE_REMEDIATION,
                  cov_resweep=PIN_COV_RESWEEP,
                  p1_snowball=PIN_P1_SNOWBALL),
        excluded_on_purpose=EXCLUDED_ON_PURPOSE,
        note="fuzz plane unioned from banked lane lcovs; kani/regress planes "
             "and denominators unchanged from the 2026-07-31 capture except "
             "for files newly created (fuzz-covered files absent from that "
             "capture; cut with the fixed sloc_rules of 2026-08-01)")
    json.dump(s, open(sp, "w"))

    outdir = os.path.join(HERE, "fuzz-replane")
    os.makedirs(outdir, exist_ok=True)
    with open(os.path.join(outdir, "REPORT.tsv"), "w") as f:
        f.write("path\tstatus\tfuzz_lines_added\tcovered_in_lcovs\toutside_universe\n")
        for r in sorted(report, key=lambda r: -r[2]):
            f.write("\t".join(str(x) for x in r) + "\n")

    print(f"\nfiles touched: {len(touched_crates)}, created: {len(created)}")
    for p in created:
        print(f"  created {p}")
    drifty = [r for r in report if r[4] > 0 and r[4] >= 0.3 * max(r[3], 1)]
    print(f"drift-flagged files (>=30% of covered lines outside universe): {len(drifty)}")
    for r in sorted(drifty, key=lambda r: -r[4])[:20]:
        print(f"  {r[0]}  covered={r[3]} outside={r[4]}")
    print(f"\nnew totals: {s['totals']}")


if __name__ == "__main__":
    main()
