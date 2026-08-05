#!/usr/bin/env python3
"""sloc_rules.py — the SLOC denominator rules for the coverage pipeline.

ADOPTED (Michael, 2026-07-30): rule v2 WITH const tables excluded is the
denominator of record; v1 remains available behind flags for comparability.

One module, imported by merge-coverage.py, recut-sloc.py and tree-sloc.py, so
the rule can never fork between the merge, the re-cut and the tree census.

RULE v1 (the original, proofs/COVERAGE.md "SLOC rule")
------------------------------------------------------
A line counts as SLOC iff, after stripping // and /* */ comments, it contains
at least one character other than whitespace and {}()[];, — with tests*.rs /
tests/ files and #[cfg(test)]-gated items excluded entirely.

RULE v2 (2026-07-30, SLOC-RULE-V2.md)
-------------------------------------
v1, minus lines the coverage instruments cannot meaningfully map. Two groups,
both measured on rustc 1.96 (SLOC-RULE-V2.md §2/§2b), never assumed:

PURE CONTROL-FLOW SYNTAX — keyword but no expression/pattern/guard/condition:

  else-only   `} else {`, `else {`, bare `else`, `} else`
  loop-only   `loop {`
  unsafe-only `unsafe {`
  arrow-only  bodiless match-arm heads with no pattern content beyond
              wildcard/unit/tuple punctuation: `_ => {`, `() => {`, `) => {`,
              `} => {`, a lone `=>` (incl. `() => {{` macro-rule arms)
  arm-head    ANY bodiless, guard-less match-arm head (`None => {`,
              `Some(tok) => {`, trailing `Pattern =>`): llvm's mapping is
              position-dependent (statement matches map them, expression
              matches don't — 127 red-head-over-covered-body artifacts
              measured), and the arm's body lines always carry its signal.
              Guarded heads (`p if c => {`) and inline-body arms
              (`0 => 100,`) are mapped and STAY.

DECLARATION LINES — items that emit no instrumented code (rustc does not
even instrument #[derive]d impls: no FN records exist for them):

  use-mod       `use …;` (incl. multi-line use blocks), `mod x;`,
                `mod x {` headers, `extern crate …;`
  attr          `#[…]` / `#![…]` attribute lines (incl. multi-line)
  type-alias    `type A = …;` (incl. multi-line)
  const-static  SINGLE-LINE `const X: T = …;` / `static X: T = …;`
                WITHOUT a closure (a `|x| …` initializer is a function whose
                span sits on the line — measured mapped, so kept). Multi-line
                const/static stay (head = omission signal; interiors are the
                separate --exclude-const-tables knob).
  typedef       struct/enum/union definitions: header + field/variant lines
                (measured: derives land NO region on them, and 0/390 were
                ever covered in the 7-crate capture)
  impl-header   `impl Foo {` / `impl Trait for Foo {` header lines, incl.
                multi-line headers and their where-clauses. (fn-attached
                where/generic lines are MAPPED and stay.)
  trait-header  `trait Foo {` header lines (default-method bodies stay)
  fn-decl       body-less `fn …;` (trait requirements, extern blocks)
  extern-block  `extern "C" {` header lines

  thread-local  entire `thread_local! { … }` blocks (measured: even a
                non-const lazy initializer body gets no region — the $init
                fragment expands inside std's macro and refinement drops it)
  macro-scaffold `macro_rules! name {` headers, matcher arm heads at body
                depth 1, repetition/punctuation tails (`)*};`, `$(`), and
                bare `name! {` invocation-block headers. TEMPLATE BODY lines
                STAY (llvm maps them; Kani credits them — macro_attrib.py),
                and invocation DECLARATION lines (`fc_dtoi4: …;`) STAY (the
                macro-attribution targets).

  KEPT (measured mapped): fn signature lines and their parameter/where/
  generic continuation lines, closure heads, statics/consts with closure
  initializers, macro_rules! definition bodies (Kani covers the template
  lines via expansion), `let` bindings, and ALL assert shapes — `assert!*`
  is live code in every profile, and `debug_assert!*` lines stay MAPPED
  even under -C debug-assertions=off (measured; the cfg! skeleton keeps
  the span), so they are coverable on every capture axis. NB the converse
  of the debug-assert masking law: a green debug_assert line under a
  release-profile capture does not mean the assertion was armed.

Grounding (SLOC-RULE-V2.md has the full research table + citations):
  * rustc's coverage instrumentation emits ONLY Code/Branch regions — no gap
    regions — and its span refinement gives none of these constructs a region
    of their own (verified empirically on rustc 1.96.0: `unsafe {`, `loop {`,
    expression-position `} else {`, `_ => {`, and bodiless `1 =>` arm heads
    get NO DA record from llvm-cov; statement-position `} else {` sometimes
    gets one, but its count is the *then*-region's tail — the wrong branch's
    count, i.e. noise, not an else-execution signal).
  * every AST/bytecode-based tool (coverage.py, JaCoCo, Istanbul, gcov's
    line table) puts `else`-style structural lines outside the denominator.
  * measured on the 7-crate dataset: `loop {` 0/66 ever covered, else-only
    88/449, vs a 72.1% any-coverage baseline — the classes read permanently
    red not because code is dark but because no instrument maps them.

Explicitly KEPT (over-exclusion guards; each carries executable content and a
real llvm region):
  * `} else if cond {`          — the condition is evaluated (Branch region)
  * `) else {` / `}) else {`    — closing token of a multi-line let-else
    initializer: the initializer's Code region wraps the line
  * `)? else {`, `)?;`, `)?`    — the ? operator is a Branch region
  * match-arm heads with a pattern or guard (`Some(x) =>`, `n if n > 0 => {`)
  * `break;` / `continue;` / `return;` — statements with their own regions
  * `|| {` closure heads        — the closure's function span starts there
  * `&& {`, `|| cond`           — condition operators, wrapped by the branch

LINE-TABLE PRECEDENCE (v2 only)
-------------------------------
Where instrument line-table data EXISTS for a file, it beats the text
heuristic: a structural-candidate line is REINSTATED into the denominator iff
some full lcov export (DA records INCLUDING count 0 — pass the raw capture
lcov, not the covered-lines subset) has a DA record for that (file, line).
DA presence is llvm's own per-line mappability verdict; where the instrument
says a line is coverable, the instrument wins. Kani regions do NOT reinstate:
under this pipeline a Kani region is a multi-line span and intersection is
span spillover, not per-line evidence. Where no line table mentions the file,
the text classification is final. Classification is thus a pure function of
(source text, DA line sets of the artifacts passed) — deterministic, and
recomputable as post-processing without re-running any instrument.

TEST-CODE EXCLUSION (structural since 2026-07-31 — see test_scope.py)
---------------------------------------------------------------------
Test code is out of scope under both rules. Until 2026-07-31 "is this test
code?" was answered by FILENAME (a `tests`-prefix regex / `/tests/`) plus a
brace-searching `#[cfg(test)]` scan, which was wrong in both directions:
semicolon-style file modules (`#[cfg(test)] mod state_tests;`) were not
recognised at all (their whole file counted as in-scope), while the brace
search walked past a braceless item into the FOLLOWING production item and
excluded it. test_scope.py replaces both with the module graph plus a real
Rust tokenizer; filenames are never consulted. Ambiguities are reported
(test_scope.diagnostics()), never guessed.

GENERATED-TABLE EXCLUSION (separate knob, NOT part of v2)
---------------------------------------------------------
classify also tags `const-table` lines — interior lines of multi-line
`const`/`static` bracket initializers (the `*_BUILTINS: &[FmgrBuiltin]`
registration tables). They are const-evaluated data with no runtime counters,
inherently dark to every instrument, but excluding them is a POLICY call
(they are the omission-visibility signal for unregistered entry points), so
they are excluded under --exclude-const-tables — ADOPTED AS DEFAULT by the
2026-07-30 ruling (disable with --include-const-tables). The declaration
head line always stays (it names the item), and every excluded span is
published in excluded-tables.json (file, span, line count, reason:
generated-file marker vs const-array heuristic) so over-exclusion is
reviewable — a span swallowing real logic is a defect, not a rounding error.
"""
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import test_scope  # structural test-code oracle (module graph + tokenizer)

V2_CLASSES = ("else-only", "loop-only", "unsafe-only", "arrow-only",
              "arm-head",
              "use-mod", "attr", "type-alias", "const-static", "typedef",
              "impl-header", "trait-header", "fn-decl", "extern-block",
              "thread-local", "macro-scaffold")

RE_ELSE_ONLY = re.compile(r"^\}?\s*else\s*\{?$")
# --- declaration shapes (all measured unmapped on rustc 1.96; see
# SLOC-RULE-V2.md §2b). VIS = optional visibility prefix.
_VIS = r"(?:pub(?:\s*\([^)]*\))?\s+)?"
RE_USE_MOD_START = re.compile(
    rf"^{_VIS}(?:use\s|mod\s+[A-Za-z_]\w*\s*;|extern\s+crate\s)")
RE_MOD_HEADER = re.compile(rf"^{_VIS}(?:unsafe\s+)?mod\s+[A-Za-z_]\w*\s*\{{$")
RE_ATTR_START = re.compile(r"^#!?\[")
RE_TYPE_ALIAS_START = re.compile(rf"^{_VIS}type\s+[A-Za-z_]\w*")
RE_CONST_STATIC_LINE = re.compile(
    rf"^{_VIS}(?:const|static)\s+(?:mut\s+)?[A-Za-z_]\w*\s*:.*;$")
RE_TYPEDEF_START = re.compile(
    rf"^{_VIS}(?:unsafe\s+)?(?:struct|enum|union)\s+[A-Za-z_]\w*")
RE_TRAIT_START = re.compile(rf"^{_VIS}(?:unsafe\s+)?(?:auto\s+)?trait\s+[A-Za-z_]\w*")
RE_IMPL_START = re.compile(r"^(?:unsafe\s+)?impl\b")
RE_FN_DECL = re.compile(
    rf"^{_VIS}(?:default\s+)?(?:const\s+)?(?:async\s+)?(?:unsafe\s+)?"
    rf"(?:extern\s+\"[^\"]*\"\s+)?fn\s+[A-Za-z_]\w*.*;$")
RE_EXTERN_BLOCK = re.compile(r"^(?:unsafe\s+)?extern(?:\s+\"[^\"]*\")?\s*\{$")
# thread_local! { … }: measured (rustc 1.96) — NO line of the block gets a
# region, including non-const runtime initializer bodies (the $init fragment
# expands inside std's macro; its spans are dropped by span refinement).
RE_THREAD_LOCAL = re.compile(r"^(?:(?:::)?std::)?thread_local!\s*\{$")
RE_MACRO_RULES = re.compile(r"^macro_rules!\s+[A-Za-z_]\w*\s*\{$")
# item-position macro invocation opening a brace block with nothing else on
# the line (`fc1! {`, `int_var! {`): the header is never mapped; the
# DECLARATION lines inside the block stay (macro_attrib.py credits them).
RE_MACRO_CALL_HEADER = re.compile(r"^[A-Za-z_][\w:]*!\s*\{$")
# macro_rules! matcher arm head at body depth 1: `($(...)*) => {` / `{$(`/`{{`
RE_MACRO_ARM_HEAD = re.compile(r"^\(.*=>\s*[{$(]+$")
# repetition/punctuation-only scaffold lines: `)*};`, `$(`, `)+`, `});)+` …
RE_MACRO_PUNCT = re.compile(r"^[\s(){}\[\];,$*+?]+$")
RE_LOOP_ONLY = re.compile(r"^loop\s*\{$")
RE_UNSAFE_ONLY = re.compile(r"^unsafe\s*\{$")
RE_ARROW_ONLY = re.compile(r"^[\s(){}\[\]_,]*=>\s*\{{0,2}$")
# head of a multi-line const/static bracket initializer. Two initializer
# shapes: `= [` (possibly with leading tokens) and `= unsafe {` (c2rust-style
# static-mut tables whose bracket opens on the NEXT line — e.g. the snowball
# stemmer `static mut a_0: [among; 3] = unsafe {` heads). `mut` is optional
# in both. (2026-08-01 fix: `static mut` heads and `= unsafe {` initializers
# previously matched neither alternative, so those tables counted IN the
# denominator tree-wide.)
RE_CONST_TABLE_HEAD = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?(?:const|static)\s+(?:mut\s+)?\w+\s*:"
    r"(?:.*=.*\[[^\]]*$|.*=\s*unsafe\s*\{$)")


def strip_line_comments(text):
    """Yield (lineno, code) for every line with // and /* */ comments
    stripped — the exact comment model of the v1 rule."""
    in_block = False
    for i, s in enumerate(text.splitlines(), 1):
        buf = []
        j = 0
        while j < len(s):
            if in_block:
                k = s.find("*/", j)
                if k == -1:
                    j = len(s)
                else:
                    in_block = False
                    j = k + 2
            else:
                lc = s.find("//", j)
                bc = s.find("/*", j)
                if bc != -1 and (lc == -1 or bc < lc):
                    buf.append(s[j:bc]); in_block = True; j = bc + 2
                elif lc != -1:
                    buf.append(s[j:lc]); j = len(s)
                else:
                    buf.append(s[j:]); j = len(s)
        yield i, "".join(buf)


def classify_structural(code):
    """v2 class of one comment-stripped line, or None if it stays.

    `code` must already be comment-stripped (strip_line_comments). Only lines
    that are v1-SLOC are worth passing in, but the function is total."""
    s = " ".join(code.split())
    if not s:
        return None
    if RE_ELSE_ONLY.fullmatch(s):
        return "else-only"
    if RE_LOOP_ONLY.fullmatch(s):
        return "loop-only"
    if RE_UNSAFE_ONLY.fullmatch(s):
        return "unsafe-only"
    if RE_ARROW_ONLY.fullmatch(s):
        return "arrow-only"
    # bodiless match-arm heads WITHOUT a guard: `Pattern => {` or a trailing
    # `Pattern =>` with the value on the next line. Measured (rustc 1.96):
    # no region — the arm's count sits on its body lines; guard arms
    # (`p if cond => {`) ARE mapped (the guard evaluates) and stay, as do
    # arms with an inline body (`0 => 100,`).
    m = re.search(r"=>\s*(.*)$", s)
    if m is not None and re.fullmatch(r"\{{0,2}", m.group(1)) \
            and not re.search(r"\bif\b", s):
        return "arm-head"
    return None


def cfg_test_spans(text_or_lines, path=""):
    """1-based inclusive line ranges of test-only-gated items.

    STRUCTURAL since 2026-07-31 (test_scope.analyze_file_items): the item
    following the `#[cfg(test)]` attribute run is parsed with a real Rust
    tokenizer, so both the braced form (`mod tests { … }`, `impl`, `fn`) and
    the braceless form (`mod tests;`, `use …;`) end where the item ends.

    The superseded implementation searched forward for the next line
    containing `{`; for a braceless item that landed inside the NEXT,
    production item and excluded it, and when no later `{` existed it
    abandoned every remaining span in the file. Both directions of error are
    gone. Accepts text or a list of lines (back-compat)."""
    text = (text_or_lines if isinstance(text_or_lines, str)
            else "\n".join(text_or_lines))
    return test_scope.analyze_file_items(text, path)["cfg_test_spans"]


DECL_CLASSES = ("use-mod", "attr", "type-alias", "const-static", "typedef",
                "impl-header", "trait-header", "fn-decl", "extern-block",
                "thread-local", "macro-scaffold")

_SPAN_CAP = 100  # abandon a decl span that doesn't close within this many lines


def decl_lines(stripped):
    """{class: set(1-based lines)} of declaration lines (measured unmappable
    on rustc 1.96 — SLOC-RULE-V2.md §2b). `stripped` = list of (lineno, code)
    from strip_line_comments.

    Over-exclusion guards (all measured, not assumed):
      * const/static lines containing a closure `|` are KEPT — a closure
        initializer is a function whose coverage span sits on that line.
      * an impl/trait header line whose `{` is followed by content keeps that
        line (only the pure header is excluded).
      * fn signatures and their where/generic continuation lines are KEPT
        (the function's coverage span starts at the signature); only
        body-less `fn …;` declarations (trait requirements, extern blocks)
        are excluded.
      * a span that doesn't close within 100 lines is abandoned (no
        exclusion) rather than guessed at.
    """
    codes = {n: " ".join(c.split()) for n, c in stripped}
    out = {c: set() for c in DECL_CLASSES}
    if not codes:
        return out
    n_max = max(codes)

    def span_to_semicolon(start):
        depth = 0
        for m in range(start, min(start + _SPAN_CAP, n_max + 1)):
            c = codes.get(m, "")
            depth += (c.count("{") + c.count("(") + c.count("[")
                      - c.count("}") - c.count(")") - c.count("]"))
            if depth <= 0 and c.endswith(";"):
                return m
        return None

    def span_to_open_brace(start):
        """Last header line: the line carrying the block's opening `{`.
        Returns (end_line, brace_line_has_trailing_code)."""
        for m in range(start, min(start + _SPAN_CAP, n_max + 1)):
            c = codes.get(m, "")
            if ";" in c and "{" not in c:
                return None, False  # not a block after all
            if "{" in c:
                after = c.split("{", 1)[1]
                return m, bool(re.search(r"[^\s{}()\[\];,]", after))
        return None, False

    def span_balanced_braces(start):
        depth = 0
        opened = False
        for m in range(start, min(start + _SPAN_CAP, n_max + 1)):
            c = codes.get(m, "")
            depth += c.count("{") - c.count("}")
            opened = opened or "{" in c
            if not opened and c.endswith(";"):
                return m  # bodyless form (unit/tuple struct)
            if opened and depth <= 0:
                return m
        return None

    def mark(cls, lo, hi):
        out[cls].update(range(lo, hi + 1))

    n = 1
    while n <= n_max:
        c = codes.get(n, "")
        if not c:
            n += 1
            continue
        if RE_THREAD_LOCAL.match(c):
            end = span_balanced_braces(n)
            if end is not None:
                mark("thread-local", n, end)
                n = end + 1
                continue
        elif RE_MACRO_RULES.match(c):
            end = span_balanced_braces(n)
            if end is not None:
                # header + matcher scaffolding out; TEMPLATE BODY lines stay
                # (Kani attributes generated-code coverage to them — the
                # macro_attrib.py mechanism; measured mapped under llvm too).
                out["macro-scaffold"].add(n)
                depth = c.count("{") - c.count("}")
                for m in range(n + 1, end + 1):
                    cc = codes.get(m, "")
                    if (depth == 1 and RE_MACRO_ARM_HEAD.match(cc)) \
                            or RE_MACRO_PUNCT.match(cc):
                        out["macro-scaffold"].add(m)
                    depth += cc.count("{") - cc.count("}")
                n = end + 1
                continue
        elif RE_MACRO_CALL_HEADER.match(c):
            mark("macro-scaffold", n, n)  # header only; block body stays
        elif RE_ATTR_START.match(c):
            depth = 0
            end = None
            for m in range(n, min(n + _SPAN_CAP, n_max + 1)):
                cc = codes.get(m, "")
                depth += cc.count("[") - cc.count("]")
                if depth <= 0:
                    end = m
                    break
            if end is not None:
                mark("attr", n, end)
                n = end + 1
                continue
        elif RE_MOD_HEADER.match(c):
            mark("use-mod", n, n)  # header only; body processed normally
        elif RE_USE_MOD_START.match(c):
            end = span_to_semicolon(n)
            if end is not None:
                mark("use-mod", n, end)
                n = end + 1
                continue
        elif RE_EXTERN_BLOCK.match(c):
            mark("extern-block", n, n)  # header only; decls hit fn-decl
        elif RE_FN_DECL.match(c) and "|" not in c:
            mark("fn-decl", n, n)
        elif RE_TYPE_ALIAS_START.match(c) and "fn " not in c:
            end = span_to_semicolon(n)
            if end is not None:
                mark("type-alias", n, end)
                n = end + 1
                continue
        elif RE_CONST_STATIC_LINE.match(c) and "|" not in c:
            mark("const-static", n, n)  # single-line only; tables are a knob
        elif RE_TYPEDEF_START.match(c):
            end = span_balanced_braces(n)
            if end is not None:
                mark("typedef", n, end)
                n = end + 1
                continue
        elif RE_TRAIT_START.match(c) or RE_IMPL_START.match(c):
            cls = "trait-header" if RE_TRAIT_START.match(c) else "impl-header"
            end, trailing = span_to_open_brace(n)
            if end is not None:
                mark(cls, n, end - 1 if trailing else end)
                n = end + 1 if not trailing else end
                if not trailing:
                    continue
        n += 1
    return out


# "generated file" marker in the first few lines (mb/conv maps, unicode_norm
# tables, …): corroborates the const-table heuristic — recorded as the
# exclusion reason in the inventory, so heuristic-only exclusions stand out
# for review.
RE_GENERATED = re.compile(
    r"(?:@generated|generated\b.{0,120}?do(?:n'?t| not) edit)", re.I | re.S)


def is_generated_file(text):
    head = "\n".join(text.splitlines()[:5])
    return bool(RE_GENERATED.search(head))


def const_table_spans(stripped):
    """Multi-line const/static bracket initializers as spans.

    Returns [(head_line, end_line, interior_lines_set)]. The head line is
    never in interior_lines_set (it stays in the denominator: it names the
    item and is the omission signal)."""
    spans = []
    codes = {n: c for n, c in stripped}
    n_max = max(codes) if codes else 0
    n = 1
    while n <= n_max:
        c = codes.get(n, "")
        if RE_CONST_TABLE_HEAD.match(c):
            interior = set()
            # Depth over the INITIALIZER only (after the `=`): the type
            # annotation's own balanced `[T; N]` contributes 0, and the
            # `= unsafe {` form opens with a brace, so both bracket kinds
            # are counted.
            init = c.split("=", 1)[1]
            depth = (init.count("[") + init.count("{")
                     - init.count("]") - init.count("}"))
            m = n + 1
            while m <= n_max and depth > 0:
                cc = codes.get(m, "")
                depth += (cc.count("[") + cc.count("{")
                          - cc.count("]") - cc.count("}"))
                # interior or closing line
                if depth > 0 or cc.count("]") or cc.count("}"):
                    interior.add(m)
                m += 1
            spans.append((n, m - 1, interior))
            n = m
        else:
            n += 1
    return spans


def const_table_lines(stripped):
    """Union of interior lines of all const-table spans (compat helper)."""
    out = set()
    for _, _, interior in const_table_spans(stripped):
        out |= interior
    return out


def analyze_text(text, path=""):
    """Full per-file classification.

    Returns dict with:
      universe:    set of v1-SLOC line numbers (the reporting universe)
      structural:  {class: set(lines)} v2 candidates within the universe
      const_table: set(lines) within the universe (separate knob)
    Test-code files return empty sets — identified STRUCTURALLY by
    test_scope.is_test_file (module graph), not by filename."""
    empty = dict(universe=set(), structural={c: set() for c in V2_CLASSES},
                 const_table=set(), const_table_spans=[], generated=False)
    if path and test_scope.is_test_file(path):
        return empty
    test_spans = cfg_test_spans(text, path)

    def in_test(n):
        return any(a <= n <= b for a, b in test_spans)

    stripped = list(strip_line_comments(text))
    universe = set()
    structural = {c: set() for c in V2_CLASSES}
    for n, code in stripped:
        if in_test(n):
            continue
        if re.search(r"[^\s{}()\[\];,]", code):
            universe.add(n)
            cls = classify_structural(code)
            if cls:
                structural[cls].add(n)
    taken = set()
    for lines in structural.values():
        taken |= lines
    for cls, lines in decl_lines(stripped).items():
        add = (lines & universe) - taken
        structural[cls] |= add
        taken |= add
    spans = []
    ctab = set()
    for head, end, interior in const_table_spans(stripped):
        eff = {n for n in interior if n in universe and n not in taken}
        ctab |= eff
        if eff:
            spans.append(dict(head=head, start=min(eff), end=end,
                              lines=len(eff)))
    return dict(universe=universe, structural=structural, const_table=ctab,
                const_table_spans=spans, generated=is_generated_file(text))


def table_inventory(analysis, path, text=None):
    """Reviewable inventory rows for this file's excluded table spans.

    reason: 'generated-file' when the file carries a generated-do-not-edit
    marker (marker-based exclusion — corroborated), else
    'const-array-heuristic' (heuristic-only — the rows to eyeball; a span
    swallowing real logic here is a defect)."""
    reason = ("generated-file" if analysis.get("generated")
              else "const-array-heuristic")
    head_lines = text.splitlines() if text else None
    rows = []
    for s in analysis.get("const_table_spans", []):
        row = dict(path=path, head_line=s["head"], span=[s["start"], s["end"]],
                   lines=s["lines"], reason=reason)
        if head_lines and s["head"] - 1 < len(head_lines):
            row["head"] = head_lines[s["head"] - 1].strip()[:90]
        rows.append(row)
    return rows


def denominator(analysis, rule="v1", exclude_const_tables=False,
                line_table=None):
    """The denominator line set for one file under a rule.

    analysis:   analyze_text() result
    rule:       "v1" | "v2"
    line_table: optional set of line numbers with DA records (any count) for
                this file — reinstates structural candidates (v2 precedence).
    Returns (denom_set, excluded_by_class) where excluded_by_class maps class
    name -> sorted list of excluded lines (for reporting)."""
    denom = set(analysis["universe"])
    excluded = {}
    if rule == "v2":
        for cls, lines in analysis["structural"].items():
            drop = set(lines)
            if line_table:
                drop -= line_table
            if drop:
                excluded[cls] = sorted(drop)
                denom -= drop
    elif rule != "v1":
        raise ValueError(f"unknown sloc rule {rule!r}")
    if exclude_const_tables:
        drop = analysis["const_table"] & denom
        if drop:
            excluded["const-table"] = sorted(drop)
            denom -= drop
    return denom, excluded


def sloc_lines(path, rule="v2", exclude_const_tables=True, line_table=None):
    """Drop-in replacement for the v1 sloc_lines(path) — returns the
    denominator set for a file on disk under the given rule."""
    try:
        text = open(path, encoding="utf-8", errors="replace").read()
    except OSError:
        return set()
    analysis = analyze_text(text, path)
    denom, _ = denominator(analysis, rule, exclude_const_tables, line_table)
    return denom
