#!/usr/bin/env python3
"""macro_attrib.py — attribute Kani coverage of macro-GENERATED functions back
to the macro-INVOCATION line that declares them.

THE DEFECT THIS FIXES
---------------------
Kani's source-based coverage reports a region's file/line from the span the
compiler recorded for the generated MIR. For a function produced by a
`macro_rules!` expansion, that span is inside the macro DEFINITION body, never
at the invocation. So for

    macro_rules! fc1t {                      // builtins.rs:111
        ($($fc:ident: $core:ident($get:ident) -> $from:ident;)*) => {$(
            pub fn $fc(...) -> PgResult<Datum> {        // 113
                let [a] = fcinfo.args_n::<1>();        // 114
                Ok(Datum::$from(crate::$core(a.value.$get())?))
            }                                          // 116
        )*};
    }

    fc1t! {
        fc_dtoi4: dtoi4(as_f64) -> from_i32;   // builtins.rs:254  <-- SLOC line
    }

a proof that exercises `fc_dtoi4` yields regions at builtins.rs:113-116 with
`"function": "adt_float::builtins::fc_dtoi4"`. Line 254 — a real SLOC line in
the denominator, and the ONLY textual place the wrapper `fc_dtoi4` exists —
gets nothing. It is UNCOVERABLE BY CONSTRUCTION.

Measured on adt_float (2026-07-30 smoke, proofs/coverage/SMOKE-RESULT.md §3):
72 of 103 such declaration lines named a wrapper that a harness in the run
directly proved, yet all 103 read as uncovered. 72/2,215 = a 3.25-percentage-
point systematic undercount in one crate, and `fc*!` wrapper macros are the
house style for fmgr registration across the adt crates.

THE FIX, AND WHY THIS ONE
-------------------------
Two options were on the table:

  (a) attribute the generated function's regions back to the invocation line;
  (b) drop macro-invocation blocks from the SLOC denominator.

This module implements (a), with EXACT function identity, because:

  * (a) preserves the signal. A `fc*!` invocation line names a real fmgr entry
    point. If no proof reaches it, "uncovered" is the TRUE and useful answer —
    that is exactly the kind of line the report exists to surface. Option (b)
    would delete 103 entry-point declarations from adt_float's denominator and
    with them the distinction between a proved wrapper and an unproved one.
    In the smoke crate 31 of the 103 were genuinely unreached; (b) hides those
    31 and flatters the percentage in the same motion.
  * The attribution is not a heuristic. kaniraw carries a `function` field per
    region, so we know the generated function's NAME. We credit an invocation
    line only when that name resolves to exactly ONE declaration site. A
    macro's N invocations share one definition body, so "some region in this
    macro body is covered" alone could not tell us WHICH wrapper ran — naive
    option (a) as first sketched (credit the invocation because the definition
    body is covered) would credit all N. Matching on the function name avoids
    that and cannot over-credit.
  * Coverage of the definition body is NOT removed. The template lines really
    did execute; they stay covered. This fix only adds the invocation line.

Residual bias is REPORTED, never absorbed. A generated function whose name is
not a token at its invocation (`paste!`/`concat_idents!` name-composing macros)
cannot be resolved. Those regions are counted and printed as UNATTRIBUTED so
the remaining undercount is a published number instead of a silent one. Kani
also attributes `#[derive]`/proc-macro output to the deriving item's span,
which is a real line in the file, so those need no correction and get none.

WHAT COUNTS AS A GENERATOR MACRO
--------------------------------
Only `macro_rules!` definitions whose body declares an item (`fn`, `impl`,
`struct`, `enum`, `const`, `static`) can put generated code in the definition
body, so only those are considered. Invocations of any OTHER macro (`println!`,
`assert!`, `vec!`, …) are ignored: their expansion is inlined into the caller
and Kani already attributes it to the caller's own line. This keeps the fix
scoped to the defect class rather than rewriting attribution generally.

Usage as a library:  see MacroIndex.
Usage as a tool:     ./macro_attrib.py <repo> <file.rs>...   (census per file)
"""

import os
import re
import sys

# `macro_rules! name {`
RE_MACRO_DEF = re.compile(r"^\s*(?:#\[\w+.*\]\s*)?macro_rules!\s+([A-Za-z_]\w*)")
# a macro invocation opening a brace or paren block: `name! {` / `name!(`
RE_MACRO_CALL = re.compile(r"^\s*([A-Za-z_][\w]*)\s*!\s*([{(\[])")
# item keywords whose presence in a macro body means the macro GENERATES items
RE_GENERATES_ITEM = re.compile(
    r"(?:^|\s)(?:pub\s+(?:\([^)]*\)\s+)?)?(?:unsafe\s+|const\s+|extern\s+"
    r'(?:"[^"]*"\s+)?)*(?:fn|impl|struct|enum|trait|static)\s')
# leading identifier of a declaration line inside an invocation block
RE_LEAD_IDENT = re.compile(r"^\s*([A-Za-z_]\w*)\b")
RE_WORD = re.compile(r"[A-Za-z_]\w*")


def _block_end(lines, start_idx, opener):
    """Index (0-based) of the line closing the block opened on start_idx."""
    closer = {"{": "}", "(": ")", "[": "]"}[opener]
    depth = 0
    for k in range(start_idx, len(lines)):
        depth += lines[k].count(opener) - lines[k].count(closer)
        if depth <= 0 and k >= start_idx:
            return k
    return len(lines) - 1


class FileMacroFacts:
    """Per-file macro geometry: definition spans and invocation declarations."""

    __slots__ = ("def_spans", "generator_names", "decl_lines", "call_spans")

    def __init__(self):
        self.def_spans = []        # [(lo, hi)] 1-based, macro_rules! bodies
        self.generator_names = {}  # macro name -> True if body declares items
        self.decl_lines = {}       # candidate declared ident -> [line, ...]
        self.call_spans = []       # [(macro_name, lo, hi)] 1-based


def scan_file(path):
    """Parse one .rs file for macro definition spans and invocation blocks.

    Two passes are needed only conceptually; both are done in one walk.
    Invocation-block declaration candidates are recorded per line in two
    flavours: the line's LEADING identifier (the `fc_dtoi4:` shape, strongest
    signal) and every word on the line (fallback for `fc_cmp!(int4, cmp)`
    one-liners). Lead hits win over word hits at lookup time.
    """
    facts = FileMacroFacts()
    try:
        lines = open(path, encoding="utf-8", errors="replace").read().splitlines()
    except OSError:
        return facts

    i = 0
    while i < len(lines):
        m = RE_MACRO_DEF.match(lines[i])
        if m:
            # body starts at the first `{` on or after this line
            j = i
            while j < len(lines) and "{" not in lines[j]:
                j += 1
            if j >= len(lines):
                break
            end = _block_end(lines, j, "{")
            facts.def_spans.append((i + 1, end + 1))
            body = "\n".join(lines[j:end + 1])
            facts.generator_names[m.group(1)] = bool(RE_GENERATES_ITEM.search(body))
            i = end + 1
            continue
        i += 1

    def_line_set = set()
    for lo, hi in facts.def_spans:
        def_line_set.update(range(lo, hi + 1))

    i = 0
    while i < len(lines):
        if (i + 1) in def_line_set:
            i += 1
            continue
        m = RE_MACRO_CALL.match(lines[i])
        if m and m.group(1) != "macro_rules":
            end = _block_end(lines, i, m.group(2))
            facts.call_spans.append((m.group(1), i + 1, end + 1))
            i = end + 1
            continue
        i += 1
    return facts


class MacroIndex:
    """Tree-wide index used to resolve a generated function name to the source
    line that declares it.

    Built over a set of .rs files (the coverage scope plus every file that
    defines a macro_rules!, so cross-crate `#[macro_export]` generators
    resolve). Only invocations of GENERATOR macros contribute declaration
    candidates.
    """

    def __init__(self):
        self.facts = {}         # abs path -> FileMacroFacts
        self.generators = {}    # macro name -> generates items?
        self._lead = {}         # ident -> set((path, line))
        self._word = {}         # ident -> set((path, line))
        self.stats = dict(files=0, macro_defs=0, generator_macros=0,
                          call_blocks=0, decl_lines=0)

    def add_file(self, path):
        f = scan_file(path)
        self.facts[path] = f
        self.stats["files"] += 1
        self.stats["macro_defs"] += len(f.def_spans)
        for name, gen in f.generator_names.items():
            self.generators[name] = self.generators.get(name, False) or gen
            if gen:
                self.stats["generator_macros"] += 1
        return f

    def finalize(self, lines_of):
        """Second phase: now that every generator macro name is known, harvest
        declaration candidates from invocation blocks of those macros.
        `lines_of(path)` returns the file's lines (cached by the caller)."""
        for path, f in self.facts.items():
            for name, lo, hi in f.call_spans:
                if not self.generators.get(name):
                    continue
                self.stats["call_blocks"] += 1
                lines = lines_of(path)
                for n in range(lo, hi + 1):
                    if n - 1 >= len(lines):
                        break
                    text = lines[n - 1]
                    code = text.split("//")[0]
                    if not code.strip():
                        continue
                    lead = RE_LEAD_IDENT.match(code)
                    if lead and n != lo:
                        self._lead.setdefault(lead.group(1), set()).add((path, n))
                        self.stats["decl_lines"] += 1
                    for w in RE_WORD.findall(code):
                        self._word.setdefault(w, set()).add((path, n))

    def in_macro_def(self, path, l0, l1):
        f = self.facts.get(path)
        if not f:
            return False
        return any(lo <= l0 and l1 <= hi for lo, hi in f.def_spans)

    def resolve(self, func_name):
        """Map a kaniraw `function` value to (path, line) of its declaration.

        Returns (path, line) on a unique resolution, else None. Lead-identifier
        matches are tried first; a whole-word match is used only if unique.
        """
        # Kani names nested MIR bodies `path::to::f::{closure#0}` /
        # `::{constant#0}`. Those belong to the enclosing generated function, so
        # peel synthetic trailing segments before resolving.
        segs = [s for s in func_name.split("::") if not s.startswith("{")]
        base = segs[-1] if segs else ""
        base = re.sub(r"<.*", "", base).strip()
        if not base:
            return None
        hits = self._lead.get(base)
        if hits and len(hits) == 1:
            return next(iter(hits))
        if hits and len(hits) > 1:
            return None  # ambiguous — refuse rather than guess
        hits = self._word.get(base)
        if hits and len(hits) == 1:
            return next(iter(hits))
        return None


def main():
    repo = os.path.realpath(sys.argv[1])
    idx = MacroIndex()
    cache = {}

    def lines_of(p):
        if p not in cache:
            cache[p] = open(p, encoding="utf-8", errors="replace").read().splitlines()
        return cache[p]

    targets = [os.path.join(repo, p) if not os.path.isabs(p) else p
               for p in sys.argv[2:]]
    for p in targets:
        idx.add_file(p)
    idx.finalize(lines_of)
    print(f"generator macros: "
          f"{sorted(n for n, g in idx.generators.items() if g)}")
    for p in targets:
        f = idx.facts[p]
        gen_calls = [c for c in f.call_spans if idx.generators.get(c[0])]
        ndecl = sum(hi - lo for _, lo, hi in gen_calls)
        print(f"{os.path.relpath(p, repo)}: {len(f.def_spans)} macro defs, "
              f"{len(gen_calls)} generator-macro invocation blocks, "
              f"~{ndecl} declaration lines")
        for name, lo, hi in gen_calls:
            print(f"    {name}!  lines {lo}-{hi}")


if __name__ == "__main__":
    main()
