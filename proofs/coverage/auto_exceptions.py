#!/usr/bin/env python3
"""auto_exceptions.py — mechanical classification of MEASURED
instrument-unmappable shapes among the uncovered residual of a
merge-coverage.py output (campaign accelerator #1).

WHAT THIS IS
------------
Campaign lanes spend the tail of every crate hand-adjudicating red lines
that belong to KNOWN instrument-unmappable classes — shapes rustc 1.96
emits no llvm coverage mapping for (or whose mapping is displaced into a
macro definition body). This pass detects those shapes mechanically and
emits them as `auto:`-prefixed exception rows, DISTINCT from
hand-adjudicated rows: an auto row is a measurement note ("the instrument
cannot see this line"), never a semantic claim (unreachable-arm /
platform-other / defensive-c-parity rows remain human work).

HONESTY RULE (fail-open)
------------------------
A class may be classified shape-only ONLY while proofs/coverage/
rig-auto-classes.py is GREEN for it on the pinned toolchain — each class
below cites its rig demonstration. Any shape without a green rig
demonstration stays UNCLASSIFIED. Two guards apply on top:

  * line-table VETO: if a full lcov line table (DA records incl. count 0
    — llvm's own per-line mappability verdict) is provided and has a DA
    record for the line, the line is NEVER auto-classified, whatever the
    shape says. DA presence = mappable = a red line is a real signal.
  * context-evidence classes: shapes measured mapping-AMBIGUOUS in the
    rig (plain-call string continuations: mapped in the rig toy, NO-DA in
    real captures) classify ONLY when the line table proves this capture
    left them unmapped while neighbors in the same file are instrumented.

CLASSES
-------
  auto:fmt-cont       string-literal / bare-path argument continuation
                      lines of a multi-line format-family MACRO invocation
                      (rig: fmt-cont GREEN; call-bearing lines excluded —
                      rig boundary call-arg-boundary). CONTEXT-EVIDENCE
                      class: tree-scale measurement found 39% of shape
                      matches DID carry a DA record (mapping depends on
                      surrounding expression shape), so this classifies
                      only with per-capture line-table proof of no DA.
  auto:let-decl       bare `let x: T;` declarations without initializer
                      (rig: let-decl GREEN).
  auto:macro-decl     declaration rows inside a generator-macro invocation
                      block — the fc_*! fmgr rows; mapping is displaced
                      into the macro_rules! body (rig: macro-decl GREEN;
                      resolution machinery shared with macro_attrib.py).
                      NOTE: these lines are the proved/unproved signal for
                      Kani macro attribution — an auto row here documents
                      why llvm sources can't credit them; it does NOT
                      remove the line from anyone's denominator.
  auto:table-head     multi-line const/static bracket-initializer HEAD
                      lines (rig: table-head GREEN; interiors are handled
                      by the --exclude-const-tables denominator knob).
  auto:call-str-cont  string-literal-only continuation lines of a plain
                      (non-macro) call argument list. CONTEXT-EVIDENCE
                      class: requires a line table showing NO DA for the
                      line while the same file has DA records within
                      CONTEXT_WINDOW lines (rig boundary
                      plain-call-str-boundary: mapped in the toy, so
                      shape alone licenses nothing).
  auto:macro-decl-defn item-declaration rows (tuple/unit struct, impl/
                      trait headers, const/static, assoc type) inside a
                      macro_rules! DEFINITION body — items generated per
                      invocation carry no template-line mapping, while
                      template fn lines stay mapped and are NOT matched
                      (rig: macro-decl-defn GREEN; Lane-F gap 1).
  auto:macro-inv-cont head/argument lines of a multi-line PAREN-form
                      generator-macro invocation; whole span skipped if
                      any line carries a closure `|` (measured: closure
                      arg bodies at the invocation site ARE mapped)
                      (rig: macro-inv-cont GREEN; Lane-F gap 2).
  auto:include-row    whole-line include!(..) rows — included code's
                      spans live in the included file (rig: include-row
                      GREEN; Lane-F gap 3).
  auto:brace-table-head multi-line const/static data bindings: struct-
                      literal-initializer head + call/closure-free field
                      rows, and `=`-terminated heads with path/literal
                      continuation lines. A bare `= {` const BLOCK never
                      matches (const-eval code, the hand class
                      const-eval-only — measured false positive
                      otherwise) (rig: brace-table-head/-field,
                      eq-cont-head/eq-cont GREEN; Lane-F gap 3).

Usage:
  ./auto_exceptions.py --outdir <merge outdir> --repo <repo root>
        [--line-table-lcov <full lcov> ...] [--out <tsv>]

Writes <outdir>/auto-exceptions.tsv (columns: file, line, class, rule,
evidence) and prints per-class counts + veto stats. Also importable:
merge-coverage.py --auto-exceptions calls classify_outdir().
"""
import argparse
import collections
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import sloc_rules  # noqa: E402
from macro_attrib import scan_file  # noqa: E402

# format-family macros: expansion-site argument spans are discarded by
# rustc's span refinement (rig: fmt-cont). Conservative allow-list — a
# macro not on it fails open to UNCLASSIFIED.
FMT_MACROS = {
    "format", "format_args", "write", "writeln", "print", "println",
    "eprint", "eprintln", "panic", "unreachable", "todo", "assert",
    "assert_eq", "assert_ne", "debug_assert", "debug_assert_eq",
    "debug_assert_ne",
}

RE_STRING = re.compile(r'r?#*"(?:[^"\\]|\\.)*"#*')
# a continuation line whose code is ONLY literals/bare paths + commas
# (after string-literal removal): `"..."`, `"..."[,]`, `v`, `1.5,`
RE_BARE_REMAINDER = re.compile(r"^\s*(?:[A-Za-z0-9_:\.]+)?\s*,?\s*$")
# type ascription optional: `let shrunk;` (deferred-init, type inferred
# from a later branch assignment) is the same no-code shape (rig: let-decl,
# gap-4 probe).
RE_LET_DECL = re.compile(
    r"^\s*let\s+(?:mut\s+)?[A-Za-z_]\w*\s*(?::\s*[^=;]+)?;\s*$")
# declaration row inside a generator-macro invocation block:
# `fc_uuid_lt: uuid_lt -> from_bool;` / `fc_dtoi4: dtoi4(as_f64) -> from_i32;`
# `|` rejected: closure-valued rows (`fc_x: |r: i32| r > 0;`) carry a real
# function span and ARE mapped (measured: 11/554 tree-wide vetoes, all
# closures — the SLOC-v2 `|` keep-guard, same phenomenon).
RE_DECL_ROW = re.compile(
    r"^\s*(?:pub\s+)?[A-Za-z_]\w*\s*:\s*[^={}\"'|]*;\s*$")


def _balanced(code):
    """Bracket-balanced line — rejects mid-statement fragments like
    `let mut x: [Option<T>;` (array-size `;` inside a multi-line type,
    measured mapped: the statement's real span covers the line)."""
    return (code.count("(") == code.count(")")
            and code.count("[") == code.count("]")
            and code.count("{") == code.count("}"))
RE_MACRO_CALL_OPEN = re.compile(r"([A-Za-z_][\w]*)\s*!\s*\(")
RE_CALL_OPEN = re.compile(r"[A-Za-z_)\]>]\s*\($|\(\s*$")

_VIS = r"(?:pub(?:\s*\([^)]*\))?\s+)?"
# item-declaration shapes inside a macro_rules! DEFINITION body, matched on
# the $-normalized line (rig gap-1 probes; Lane-F hmac shape). fn signature
# lines are deliberately NOT here — template fn lines ARE mapped (rig:
# template).
RE_DEFN_DECLS = [
    re.compile(rf"^\s*{_VIS}struct\s+\w+[^={{|]*;\s*$"),      # tuple/unit
    re.compile(rf"^\s*(?:unsafe\s+)?impl\b[^;]*\{{\s*$"),     # impl header
    re.compile(rf"^\s*{_VIS}(?:unsafe\s+)?(?:auto\s+)?trait\s+\w+[^;]*\{{\s*$"),
    re.compile(rf"^\s*{_VIS}(?:const|static)\s+(?:mut\s+)?\w+\s*:[^;]*;\s*$"),
    # `.*;` not `[^;]*;` — assoc-type RHS may contain array types with an
    # interior `;` (`type Digest = [u8; $digest];`, Lane-F hmac:27)
    re.compile(rf"^\s*{_VIS}type\s+\w+\s*=.*;\s*$"),
]
RE_DOLLAR_FRAG = re.compile(r"\$([A-Za-z_]\w*)")
# `include!(...);` whole-line item row (rig: include-row); the included
# file's code carries spans in ITS file, never on this line.
RE_INCLUDE_ROW = re.compile(r"^\s*include!\s*\(.*\)\s*;?\s*$")
# multi-line const/static brace-initializer head: `pub static X: T = T {`
# (no call-parens, no closure — those initializers are mapped, SLOC-v2 `|`
# guard). rig: brace-table-head.
RE_BRACE_TABLE_HEAD = re.compile(
    rf"^\s*{_VIS}(?:const|static)\s+(?:mut\s+)?\w+\s*:[^=({{|]*=\s*"
    rf"&?[\w:]+(?:<[^{{]*>)?\s*\{{\s*$")
# NB the struct-literal TYPE PATH before `{` is mandatory: a bare `= {`
# is a const BLOCK (const-eval code, e.g. mac8 HEXLOOKUP builder) — that
# is the hand class const-eval-only, not a data binding, and matching it
# was a measured false positive against lane-0b's ledger.
# field row of such an initializer: `ident: <path/literal/&path/string>,`
# — call-free, closure-free, brace-free (rig: brace-table-field).
RE_BRACE_TABLE_FIELD = re.compile(r"^\s*\w+\s*:[^(){}|]*,?\s*$")
# non-bracket multi-line const/static: head ending `=` with the value on
# the following line(s) (rig: eq-cont-head / eq-cont).
RE_EQ_CONT_HEAD = re.compile(
    rf"^\s*{_VIS}(?:const|static)\s+(?:mut\s+)?\w+\s*:[^=({{|]*=\s*$")
RE_EQ_CONT_VALUE = re.compile(r"^\s*&?[\w:\.\[\]\s]+;?\s*$")

CONTEXT_WINDOW = 10  # lines; instrumented-context evidence for call-str-cont


def load_line_tables(paths, repo):
    """{relpath: set(lines with a DA record, any count)} — llvm's own
    mappability verdict per line."""
    tab = {}
    cur = None
    repo = os.path.realpath(repo)
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
                tab.setdefault(cur, set()).add(int(line[3:].split(",")[0]))
            elif line == "end_of_record":
                cur = None
    return tab


class FileShapes:
    """Per-file shape geometry needed by the classifier."""

    def __init__(self, abspath, text):
        self.lines = text.splitlines()
        stripped = list(sloc_rules.strip_line_comments(text))
        self.code = {n: c for n, c in stripped}
        # multi-line const/static table heads
        self.table_heads = {head for head, end, interior
                            in sloc_rules.const_table_spans(stripped)
                            if interior}
        # generator-macro invocation block interiors (needs generator
        # verdicts possibly from OTHER files; caller passes them in via
        # set_generators)
        self.facts = scan_file(abspath)
        self.gen_decl_lines = set()
        self.gen_inv_cont_lines = set()
        self.macro_def_decl_lines = set()
        self.include_rows = set()
        self.data_binding_lines = set()
        # paren-group spans: (open_line, close_line, kind) where kind is
        # 'fmt' (format-family macro), 'macro' (other macro), 'call'
        self.paren_spans = []
        self._scan_paren_groups()
        self._scan_data_spans()

    def set_generators(self, generators):
        for name, lo, hi in self.facts.call_spans:
            if not generators.get(name):
                continue
            self.gen_decl_lines.update(range(lo + 1, hi))
            # paren-form multi-line generator invocation (`hmac_hash!(a, b,`
            # + continuation lines): head AND continuations are macro input
            # tokens whose expansion spans land in the definition body
            # (rig: macro-inv-cont). Head recognised by `!(` on line lo.
            head = self.code.get(lo, "")
            if hi > lo and RE_MACRO_CALL_OPEN.search(
                    RE_STRING.sub('""', head)):
                # closure-bearing spans excluded WHOLE: closure argument
                # bodies at the invocation site carry real spans (measured:
                # 2 covered counterexamples tree-wide, both `|...|` args —
                # the SLOC-v2 `|` keep-guard a third time).
                if any("|" in RE_STRING.sub('""', self.code.get(m, ""))
                       for m in range(lo, hi + 1)):
                    continue
                self.gen_inv_cont_lines.update(range(lo, hi + 1))

    def _scan_data_spans(self):
        """macro_rules! definition-body decl rows, include! rows, and
        multi-line const/static data-binding spans (brace-initializer and
        `=`-continuation forms)."""
        def_lines = set()
        for lo, hi in self.facts.def_spans:
            def_lines.update(range(lo, hi + 1))
        ns = sorted(self.code)
        i = 0
        while i < len(ns):
            n = ns[i]
            code = self.code[n]
            stripped = RE_STRING.sub('""', code)
            if n in def_lines:
                norm = RE_DOLLAR_FRAG.sub(r"\1", stripped)
                if ("|" not in norm and
                        any(r.match(norm) for r in RE_DEFN_DECLS)):
                    self.macro_def_decl_lines.add(n)
                i += 1
                continue
            if RE_INCLUDE_ROW.match(stripped) and _balanced(stripped):
                self.include_rows.add(n)
                i += 1
                continue
            if RE_BRACE_TABLE_HEAD.match(stripped):
                # walk the brace span; field rows classified individually
                self.data_binding_lines.add(n)
                depth = stripped.count("{") - stripped.count("}")
                j = i + 1
                while j < len(ns) and depth > 0:
                    m = ns[j]
                    cs = RE_STRING.sub('""', self.code[m])
                    depth += cs.count("{") - cs.count("}")
                    if depth > 0 and RE_BRACE_TABLE_FIELD.match(cs):
                        self.data_binding_lines.add(m)
                    j += 1
                i = j
                continue
            if RE_EQ_CONT_HEAD.match(stripped):
                self.data_binding_lines.add(n)
                j = i + 1
                while j < len(ns):
                    m = ns[j]
                    cs = RE_STRING.sub('""', self.code[m])
                    if not RE_EQ_CONT_VALUE.match(cs):
                        break
                    self.data_binding_lines.add(m)
                    j += 1
                    if ";" in cs:
                        break
                i = j
                continue
            i += 1

    def _scan_paren_groups(self):
        """Multi-line paren groups opened by a macro call or plain call.
        Textual, comment-stripped, string-stripped depth count — same
        approximation family as sloc_rules/macro_attrib."""
        open_stack = []  # (line, kind)
        for n in sorted(self.code):
            c = RE_STRING.sub('""', self.code.get(n, ""))
            j = 0
            while j < len(c):
                ch = c[j]
                if ch == "(":
                    pre = c[:j]
                    m = None
                    for m in RE_MACRO_CALL_OPEN.finditer(c[:j + 1]):
                        pass
                    if m and m.end() == j + 1:
                        kind = ("fmt" if m.group(1) in FMT_MACROS
                                else "macro")
                    elif re.search(r"[A-Za-z_0-9)\]>]\s*$", pre):
                        kind = "call"
                    else:
                        kind = "group"
                    open_stack.append((n, kind))
                elif ch == ")":
                    if open_stack:
                        lo, kind = open_stack.pop()
                        if n > lo:
                            self.paren_spans.append((lo, n, kind))
                j += 1

    def enclosing_paren(self, n):
        """Innermost multi-line paren group strictly containing line n
        (n not the opening line). Returns kind or None."""
        best = None
        for lo, hi, kind in self.paren_spans:
            if lo < n <= hi:
                if best is None or lo > best[0]:
                    best = (lo, kind)
        return best[1] if best else None


def classify_line(shapes, n):
    """(class, rule) for line n of a file, or (None, None).
    Shape verdicts only — vetoes/evidence applied by the caller."""
    code = shapes.code.get(n, "")
    if not code.strip():
        return None, None
    if n in shapes.table_heads:
        return ("auto:table-head",
                "multi-line const/static bracket-initializer head "
                "(rig-auto-classes: table-head)")
    if n in shapes.macro_def_decl_lines:
        return ("auto:macro-decl-defn",
                "item-declaration row inside a macro_rules! DEFINITION body "
                "(struct/const/assoc-type/impl items generated per "
                "invocation; rig-auto-classes: macro-decl-defn)")
    if n in shapes.include_rows:
        return ("auto:include-row",
                "include!(..) build-generated module row — included code "
                "carries spans in the included file, never here "
                "(rig-auto-classes: include-row)")
    if n in shapes.data_binding_lines:
        return ("auto:brace-table-head",
                "multi-line const/static data binding (brace-initializer "
                "head/field rows or `=`-continuation; call/closure-bearing "
                "lines excluded; rig-auto-classes: brace-table-head/"
                "eq-cont)")
    if n in shapes.gen_inv_cont_lines and "(" not in \
            RE_STRING.sub('""', code).replace("!(", "", 1) and \
            "|" not in code:
        return ("auto:macro-inv-cont",
                "head/argument line of a multi-line paren-form generator-"
                "macro invocation — expansion spans land in the macro "
                "definition body (rig-auto-classes: macro-inv-cont)")
    if (n in shapes.gen_decl_lines and RE_DECL_ROW.match(code)
            and _balanced(code)):
        return ("auto:macro-decl",
                "declaration row inside generator-macro invocation block "
                "(rig-auto-classes: macro-decl; macro_attrib geometry)")
    if RE_LET_DECL.match(code) and _balanced(code):
        return ("auto:let-decl",
                "bare `let x: T;` without initializer "
                "(rig-auto-classes: let-decl)")
    remainder = RE_STRING.sub("", code)
    if RE_BARE_REMAINDER.match(remainder) and remainder != code:
        kind = shapes.enclosing_paren(n)
        if kind == "fmt":
            return ("auto:fmt-cont",
                    "literal/bare-arg continuation line of format-family "
                    "macro invocation (rig-auto-classes: fmt-cont)")
        if kind == "call":
            return ("auto:call-str-cont",
                    "string-literal continuation of plain call arg list "
                    "(context-evidence class; rig boundary "
                    "plain-call-str-boundary)")
    elif (RE_BARE_REMAINDER.match(remainder) and remainder == code
          and remainder.strip()):
        # bare-path (no string) continuation, e.g. lone `v` argument line
        if shapes.enclosing_paren(n) == "fmt":
            return ("auto:fmt-cont",
                    "bare-argument continuation line of format-family "
                    "macro invocation (rig-auto-classes: fmt-cont)")
    return None, None


# Context-evidence classes: the rig proves these shapes CAN be unmapped,
# but tree-scale measurement (covrf full-tree line table, 2026-07-31)
# shows their mapping is context-dependent — 1,693 of 4,382 fmt-cont
# shape-matches in the residual DID carry a DA record. So both classify
# ONLY against a per-capture line table (no DA at the line + instrumented
# context nearby). Shape-only licensed classes (let-decl, macro-decl,
# table-head) measured 0 vetoes tree-wide after the `|`/balance guards.
NEEDS_TABLE_EVIDENCE = {"auto:call-str-cont", "auto:fmt-cont"}


def classify_outdir(outdir, repo, line_table_paths=(), source_root=None):
    """Classify the uncovered residual of a merge output.

    Returns (rows, stats). rows = [dict(file, line, cls, rule, evidence)].
    source_root: where to read sources (defaults to repo; pass a worktree
    checked out at the capture's head_sha when repo has moved on).
    """
    repo = os.path.realpath(repo)
    src_root = os.path.realpath(source_root or repo)
    tables = load_line_tables(line_table_paths, repo) if line_table_paths \
        else None
    stats = collections.Counter()
    rows = []

    fdir = os.path.join(outdir, "files")
    shapes_cache = {}
    all_generators = {}

    details = []
    for name in sorted(os.listdir(fdir)):
        if not name.endswith(".json"):
            continue
        d = json.load(open(os.path.join(fdir, name)))
        details.append(d)

    # pass 1: per-file shape geometry + tree-wide generator verdicts.
    # A generator macro may be defined in another crate; harvest
    # macro_rules! verdicts from every scanned file first, then (pass 2)
    # from any crates/ file that defines macro_rules!.
    for d in details:
        rel = d["path"]
        p = os.path.join(src_root, rel)
        if not os.path.exists(p):
            stats["missing_source"] += 1
            continue
        sh = FileShapes(p, open(p, encoding="utf-8",
                                errors="replace").read())
        shapes_cache[rel] = sh
        for mname, gen in sh.facts.generator_names.items():
            all_generators[mname] = all_generators.get(mname, False) or gen
    # pass 2: unresolved invocation names -> scan tree for their defs
    unresolved = set()
    for sh in shapes_cache.values():
        for mname, _, _ in sh.facts.call_spans:
            if mname not in all_generators:
                unresolved.add(mname)
    if unresolved:
        for root, dirs, names in os.walk(os.path.join(src_root, "crates")):
            dirs[:] = [x for x in dirs if x != "target"]
            for nm in names:
                if not nm.endswith(".rs"):
                    continue
                p = os.path.join(root, nm)
                try:
                    txt = open(p, encoding="utf-8", errors="replace").read()
                except OSError:
                    continue
                if "macro_rules!" not in txt:
                    continue
                f = scan_file(p)
                for mname, gen in f.generator_names.items():
                    all_generators[mname] = (all_generators.get(mname, False)
                                             or gen)
    for sh in shapes_cache.values():
        sh.set_generators(all_generators)

    for d in details:
        rel = d["path"]
        sh = shapes_cache.get(rel)
        if sh is None:
            continue
        sl = set(d["sloc"])
        covered = set(d.get("kani", [])) | set(d.get("fuzz", [])) \
            | set(d.get("regress", []))
        uncovered = sorted(sl - covered)
        stats["uncovered_lines"] += len(uncovered)
        ft = tables.get(rel) if tables is not None else None
        for n in uncovered:
            cls, rule = classify_line(sh, n)
            if cls is None:
                stats["unclassified"] += 1
                continue
            # line-table veto: DA present => mappable => real red signal
            if ft and n in ft:
                stats[f"vetoed_da_present[{cls}]"] += 1
                continue
            if cls in NEEDS_TABLE_EVIDENCE:
                ctx = (ft is not None and any(
                    m in ft for m in range(n - CONTEXT_WINDOW,
                                           n + CONTEXT_WINDOW + 1)))
                if not ctx:
                    stats[f"no_table_evidence[{cls}]"] += 1
                    continue
                evidence = (f"no DA record at {rel}:{n}; instrumented "
                            f"context within +-{CONTEXT_WINDOW} lines "
                            f"(line table)")
            elif ft is not None:
                evidence = f"shape rig-verified; line table has no DA at {n}"
            else:
                evidence = "shape rig-verified; no line table supplied"
            stats[cls] += 1
            rows.append(dict(file=rel, line=n, cls=cls, rule=rule,
                             evidence=evidence))
    return rows, stats


def write_tsv(rows, out, stats=None):
    with open(out, "w") as fh:
        fh.write(
            "# auto-exceptions.tsv — MECHANICALLY classified "
            "instrument-unmappable residual lines.\n"
            "# Generated by proofs/coverage/auto_exceptions.py; classes "
            "licensed by proofs/coverage/rig-auto-classes.py (GREEN "
            "required).\n"
            "# DISTINCT from hand-adjudicated exception rows: an auto: row "
            "is a measurement note (the\n"
            "# instrument emits no mapping for the line), NOT a semantic "
            "adjudication. Rows here need\n"
            "# no PR review to exist, but they never move a line out of "
            "any denominator.\n"
            "file\tline\tclass\trule\tevidence\n")
        for r in sorted(rows, key=lambda r: (r["file"], r["line"])):
            fh.write(f"{r['file']}\t{r['line']}\t{r['cls']}\t{r['rule']}\t"
                     f"{r['evidence']}\n")
    if stats is not None:
        print("auto-exceptions:", dict(stats))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", required=True,
                    help="merge-coverage.py output dir (reads files/*.json)")
    ap.add_argument("--repo", required=True)
    ap.add_argument("--source-root", default=None,
                    help="worktree to read sources from if --repo has "
                         "moved past the capture's head_sha")
    ap.add_argument("--line-table-lcov", action="append", default=[],
                    help="FULL lcov (DA incl. count 0) — veto + evidence")
    ap.add_argument("--out", default=None,
                    help="output tsv (default <outdir>/auto-exceptions.tsv)")
    a = ap.parse_args()
    rows, stats = classify_outdir(a.outdir, a.repo, a.line_table_lcov,
                                  a.source_root)
    out = a.out or os.path.join(a.outdir, "auto-exceptions.tsv")
    write_tsv(rows, out, stats)
    print(f"wrote {out}: {len(rows)} auto rows")


if __name__ == "__main__":
    main()
