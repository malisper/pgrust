#!/usr/bin/env python3
"""Emit phase1-exceptions.tsv rows for the p1-laneaa done gate.

Accounting rule (fuzzuproof-crate DONE GATE item 1): every in-scope v2-SLOC
line is either fuzz-measured or carries ONE recorded executable-exception row.
Classes used here, in decision order:

  carve-file          whole file is OUT per the claim's scope_note
                      (jsonpath_exec/src/json_table.rs = JsonTable executor
                      glue) -> class excluded-state
  carve-fn            function is a named carve: session-TZ datetime family,
                      SRF/MultiFuncCall plumbing, fmgr/typcache datum entry
                      -> class excluded-state
  unreachable-arm     line is (or heads) an unreachable!()/panic!() defensive
                      arm mirroring a C elog(ERROR, "internal") site
  instrument-unmappable
                      no DA record emitted for the shape: match-arm
                      alternation continuation (`| Pattern`), bare pattern
                      line, `let x: T;` declaration, closing-paren/brace
                      continuation of a multi-line call
  const-eval-only     const fn body / const table head evaluated at compile
                      time into an excluded data table (builtins.rs `b()` +
                      the BUILTINS table)
  REVIEW              anything else -> printed to stderr, NOT emitted; the
                      gate does not close while REVIEW is non-empty.

Usage:
  emit_exceptions_laneaa.py <merge-outdir> <lcov> <crate-src-root> <crate-id> \
      [--carve-file f.rs,...] [--carve-fn fn,...] [--author lane]
"""
import argparse
import glob
import json
import os
import re
import sys

ap = argparse.ArgumentParser()
ap.add_argument("outdir")
ap.add_argument("lcov")
ap.add_argument("srcroot", help="e.g. crates/backend/utils/adt/jsonpath_exec/src")
ap.add_argument("crate_id", help="e.g. adt/jsonpath_exec")
ap.add_argument("--carve-file", default="")
ap.add_argument("--carve-fn", default="")
ap.add_argument("--author", default="p1-laneaa")
ap.add_argument("--carve-note", default="")
ap.add_argument("--manual-file", default="",
                help="TSV of reviewed per-line rows: basename<TAB>line<TAB>class"
                     "<TAB>c_counterpart<TAB>justification (# comments ok)")
a = ap.parse_args()

carve_files = {x for x in a.carve_file.split(",") if x}
carve_fns = {x for x in a.carve_fn.split(",") if x}

manual = {}
if a.manual_file:
    for raw in open(a.manual_file):
        raw = raw.rstrip("\n")
        if not raw or raw.startswith("#"):
            continue
        base_m, ln_m, cls_m, c_m, just_m = raw.split("\t")
        manual[(base_m, int(ln_m))] = (cls_m, c_m, just_m)

da = {}
cur = None
for line in open(a.lcov):
    line = line.strip()
    if line.startswith("SF:"):
        cur = line[3:]
    elif line.startswith("DA:") and cur and f"/{a.srcroot}/" in cur + "/":
        ln, c = line[3:].split(",")[:2]
        da.setdefault(os.path.basename(cur), {})[int(ln)] = int(c)

PAT_CONT = re.compile(r"^\s*\|\s*\S")
PAT_LABEL = re.compile(r"^\s*(?:\w+::)+\w+(?:\s*\{[^}]*\}|\s*\([^)]*\))?\s*(?:=>)?\s*$")
LET_DECL = re.compile(r"^\s*let\s+(?:mut\s+)?\w+\s*(?::\s*[^=;]+)?;\s*$")
TRIVIA = re.compile(r"^\s*(?:[\)\}\]]+\??[;,]?|=>\s*\{?|\{|\.\w+\(\)\??[;,]?|(?:true|false|None|self|\d+)\s*,)\s*$")
UNREACH = re.compile(r"(?:unreachable!|panic!)\s*[\(!]")
STRLIT_CONT = re.compile(r'^\s*"')
# struct-literal / call-argument continuation heads and bare-identifier
# argument lines: rustc attaches the enclosing expression's DA record to the
# expression head, not these lines.
STRUCT_HEAD = re.compile(r"^\s*(?:[\w.]+\s*=\s*|Ok\(|Some\(|self\.\w+\s*=\s*)?[\w:]+\s*\{\s*$")
BARE_ARG = re.compile(r"^\s*[A-Za-z_][A-Za-z0-9_:.]*\s*,\s*$")
CONST_TABLE_HEAD = re.compile(r"^\s*pub\s+const\s+\w+\s*:\s*.*=\s*&?\[\s*$")

rows, review = [], []
for f in sorted(glob.glob(os.path.join(a.outdir, "files", "*.json"))):
    d = json.load(open(f))
    base = f.split("__")[-1].replace(".rs.json", ".rs")
    unc = sorted(set(d["sloc"]) - set(d["fuzz"]) - set(d["kani"]))
    if not unc:
        continue
    src = open(os.path.join(a.srcroot, base)).read().splitlines()
    starts = []
    const_fns = set()
    for i, l in enumerate(src, 1):
        m = re.match(r"\s*(?:pub(?:\([^)]*\))? )?(?:const |unsafe |extern )*fn (\w+)", l)
        if m:
            starts.append((i, m.group(1)))
            if re.match(r"\s*(?:pub(?:\([^)]*\))? )?const fn ", l):
                const_fns.add(m.group(1))
    # macro_rules! definition bodies: no DA records are ever emitted for the
    # body lines themselves (expansion regions map to call sites / decl line).
    macro_lines = set()
    depth = 0
    for i, l in enumerate(src, 1):
        if depth == 0 and re.match(r"\s*macro_rules!\s*\w+", l):
            depth = l.count("{") - l.count("}")
            macro_lines.add(i)
            continue
        if depth > 0:
            macro_lines.add(i)
            depth += l.count("{") - l.count("}")

    def fn_of(ln):
        n = "<module>"
        for s, name in starts:
            if s <= ln:
                n = name
            else:
                break
        return n

    m = da.get(base, {})
    rel = f"{a.srcroot}/{base}"
    for ln in unc:
        text = src[ln - 1]
        fn = fn_of(ln)
        noda = ln not in m
        if base in carve_files:
            rows.append((rel, ln, "excluded-state", f"claim carve: whole file {base}",
                         f"{base} is OUT of phase-1 scope per the p1-laneaa claim scope_note ({a.carve_note or 'engine/executor glue'}); the C counterpart region runs under executor/SRF state"))
        elif fn in carve_fns:
            rows.append((rel, ln, "excluded-state", f"claim carve: {fn}",
                         f"named carve in the p1-laneaa claim scope_note (session-TZ datetime family / SRF plumbing / fmgr-datum entry); C counterpart executes the same region under session state"))
        elif (base, ln) in manual:
            cls_m, c_m, just_m = manual[(base, ln)]
            rows.append((rel, ln, cls_m, c_m, just_m))
        elif fn in const_fns or CONST_TABLE_HEAD.match(text):
            rows.append((rel, ln, "const-eval-only", f"{fn} (const context)",
                         "const fn body / const-table head evaluated at compile time (JSONPATH_EXEC_BUILTINS is linked by the driver as _EXEC_BUILTINS); runtime instruments cannot observe it"))
        elif noda and ln in macro_lines:
            rows.append((rel, ln, "instrument-unmappable", f"{fn} (macro_rules! body)",
                         f"macro_rules! definition body line: rustc attributes expansion regions to call sites, never the body (verified no DA record in {os.path.basename(a.lcov)}); the macro's expansions are exercised at its return_error!/call sites"))
        elif UNREACH.search(text):
            rows.append((rel, ln, "unreachable-arm",
                         "elog(ERROR) internal-error mirror (jsonpath_exec.c / jsonb_util.c; the panic message matches the C elog text)",
                         f"defensive internal-error arm in {fn}: {text.strip()[:90]}"))
        elif noda and (PAT_CONT.match(text) or PAT_LABEL.match(text) or LET_DECL.match(text)
                       or TRIVIA.match(text) or STRLIT_CONT.match(text)
                       or STRUCT_HEAD.match(text) or BARE_ARG.match(text)):
            if PAT_CONT.match(text) or PAT_LABEL.match(text):
                shape = "match-arm alternation/pattern continuation"
            elif LET_DECL.match(text):
                shape = "bare `let x;` declaration"
            elif STRLIT_CONT.match(text):
                shape = "string-literal continuation line of a multiline macro call"
            elif STRUCT_HEAD.match(text):
                shape = "struct-literal/expression head of a multiline expression"
            elif BARE_ARG.match(text):
                shape = "bare-identifier argument continuation line"
            else:
                shape = "closing/argument continuation line of a multiline call"
            rows.append((rel, ln, "instrument-unmappable", f"{fn} ({shape})",
                         f"rustc emits no DA record for this shape (verified against {os.path.basename(a.lcov)}); executed region, unmappable line"))
        else:
            review.append((rel, ln, fn, "no-DA" if noda else "DA0", text.strip()[:100]))

for r in rows:
    print("\t".join([str(r[0]), str(r[1]), r[2], r[3], r[4], a.author, "pending"]))
print(f"# emitted {len(rows)} rows; REVIEW {len(review)}", file=sys.stderr)
for r in review:
    print("REVIEW\t" + "\t".join(str(x) for x in r), file=sys.stderr)
