#!/usr/bin/env python3
"""test_scope.py — STRUCTURAL identification of test code for the SLOC
denominator.

WHY THIS EXISTS (measurement-integrity fix, 2026-07-31)
------------------------------------------------------
The denominator rule of record (SLOC-RULE-V2.md, adopted 2026-07-30) excludes
test code from the in-scope denominator. The original implementation decided
"is this test code?" by FILENAME (`^tests.*\\.rs$` / `/tests/`) plus a
brace-counted `#[cfg(test)]` scan. Both halves were wrong, in both directions:

  D1  semicolon-style file modules (`#[cfg(test)] mod state_tests;`) were not
      recognised as test code at all, so every line of the pointed-to file
      landed in the in-scope denominator. Files happened to be excluded only
      when their name started with `tests` — so `ws_tests.rs` / `c_cases.rs`
      counted, and a lane RENAMED sources to `tests_*.rs` to satisfy the tool.
      The tool was shaping the source; that is backwards.

  D2  the `#[cfg(test)]` span scanner searched forward for the next line
      containing `{` with no regard for the intervening item. For the
      semicolon form (`#[cfg(test)]` + `mod tests;`) there is no brace, so it
      walked into the FOLLOWING, production item and excluded it — silently
      removing real code from the denominator. When no later `{` existed at
      all it `break`s, silently abandoning every remaining cfg(test) span in
      the file (over-inclusion).

  D3  the filename rule matched by PREFIX, so a production file whose name
      merely starts with `tests` would be dropped whole.

This module replaces the filename heuristic with the module graph and a real
Rust tokenizer. It does NOT re-open the ruling (v2 + data tables excluded;
asserts and macro template bodies stay IN): it only makes the tool faithful
to it.

WHAT COUNTS AS TEST CODE
------------------------
A file is test code iff any of:
  T1  a file-module declaration reaching it is test-only gated — i.e. some
      in-crate file contains `#[cfg(test)] mod NAME;` (or an equivalent
      cfg predicate that can only hold under test) resolving to it. The
      property is inherited: every descendant module of a test module is
      test code, whatever its own gating.
  T2  the file carries a crate/module-level inner attribute `#![cfg(test)]`.
  T3  a path component is `tests` or `benches` (integration-test and bench
      trees; also `src/tests/…`).
  T4  the file has at least one `#[test]` / `#[kani::proof]` item and NO
      production item at all — a harness-only file.
  T5  the file is UNREACHABLE from the crate's entry points (no `mod` chain
      from src/lib.rs / src/main.rs / src/bin/*.rs / the Cargo.toml `path`
      keys reaches it) AND it holds `#[test]`/`#[kani::proof]` items: rustc
      never compiles it, so no instrument can ever reach it. Recorded as a
      LOUD diagnostic as well as excluded. An unreachable file WITHOUT test
      items is KEPT in scope and reported — dropping it silently would hide
      real code behind a resolver miss.

Filenames are NOT consulted. `tests.rs` is excluded because something
declares it under `#[cfg(test)]`, not because of its name.

AMBIGUITY IS LOUD, NEVER GUESSED
--------------------------------
Every case the analysis cannot decide is recorded as a diagnostic
(`diagnostics()`); tree-sloc/recut/merge print them, and `--strict-test-scope`
turns them into a hard error. Recorded cases:
  * a cfg predicate that MENTIONS `test` but does not imply it
    (`any(test, feature = "x")`, `not(test)`, …) — the item is kept in scope
    and the shape is reported;
  * a `mod NAME;` whose target file cannot be found on disk;
  * a `mod` declaration inside a macro body or otherwise unparsable span.
"""
import os
import re


def _read(path):
    with open(path, encoding="utf-8", errors="replace") as fh:
        return fh.read()

# ------------------------------------------------------------------ blanking

_RAW_START = re.compile(r'(?:b|c|br|cr|rb|rc)?r(#*)"')


def blank_literals(text):
    """Return `text` with comment and string/char-literal CONTENT replaced by
    spaces (length- and newline-preserving), so bracket/keyword scanning is
    lexically safe. Handles nested block comments, raw strings of any hash
    count, byte/c-string prefixes, escapes, and lifetime-vs-char ambiguity."""
    out = list(text)
    n = len(text)
    i = 0

    def blank(a, b):
        for k in range(a, b):
            if out[k] != "\n":
                out[k] = " "

    while i < n:
        c = text[i]
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            j = text.find("\n", i)
            j = n if j == -1 else j
            blank(i, j)
            i = j
        elif c == "/" and i + 1 < n and text[i + 1] == "*":
            depth = 1
            j = i + 2
            while j < n and depth:
                if text.startswith("/*", j):
                    depth += 1
                    j += 2
                elif text.startswith("*/", j):
                    depth -= 1
                    j += 2
                else:
                    j += 1
            blank(i, j)
            i = j
        elif c in "rbc" and _RAW_START.match(text, i):
            m = _RAW_START.match(text, i)
            close = '"' + "#" * len(m.group(1))
            j = text.find(close, m.end())
            j = n if j == -1 else j + len(close)
            blank(m.end() - 1, j)
            i = j
        elif c == '"':
            j = i + 1
            while j < n:
                if text[j] == "\\":
                    j += 2
                    continue
                if text[j] == '"':
                    j += 1
                    break
                j += 1
            blank(i, j)
            i = j
        elif c == "'":
            # char literal iff  '<esc>'  or  '<one char>'  — else a lifetime
            m = re.compile(r"'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]{1,6}\}|.)|[^'\\\n])'").match(text, i)
            if m:
                blank(i, m.end())
                i = m.end()
            else:
                i += 1
        else:
            i += 1
    return "".join(out)


def _line_index(text):
    """Sorted list of line-start offsets; line number = bisect_right - 1 + 1."""
    starts = [0]
    for m in re.finditer("\n", text):
        starts.append(m.end())
    return starts


def _lineno(starts, off):
    import bisect
    return bisect.bisect_right(starts, off)


# ------------------------------------------------------------ cfg predicates

class CfgAmbiguity(Exception):
    pass


def _split_top(s):
    """Split a comma-separated predicate list at paren depth 0."""
    parts, depth, cur = [], 0, []
    for ch in s:
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


def cfg_implies_test(pred):
    """Does this cfg predicate hold ONLY when `test` is set?

    Returns True / False. Conservative: anything not provably test-only is
    False. `mentions_test(pred) and not cfg_implies_test(pred)` is the
    ambiguity signal the callers report."""
    p = " ".join(pred.split())
    if p == "test":
        return True
    m = re.fullmatch(r"(all|any|not)\s*\((.*)\)", p, re.S)
    if not m:
        return False
    kind, inner = m.group(1), m.group(2)
    parts = _split_top(inner)
    if kind == "all":
        return any(cfg_implies_test(x) for x in parts)
    if kind == "any":
        return bool(parts) and all(cfg_implies_test(x) for x in parts)
    return False  # not(...) is never test-only


def mentions_test(pred):
    return re.search(r"\btest\b", pred) is not None


# --------------------------------------------------------------- item spans

_WS_OR_ATTR = re.compile(r"\s+")


def _skip_trivia(blanked, i, n):
    """Advance past whitespace (comments are already blanked)."""
    while i < n and blanked[i].isspace():
        i += 1
    return i


def _match_bracket(blanked, i, open_ch, close_ch):
    """`i` is at open_ch; return index just past the matching close, or None."""
    n = len(blanked)
    depth = 0
    while i < n:
        c = blanked[i]
        if c == open_ch:
            depth += 1
        elif c == close_ch:
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return None


def _item_end(blanked, i):
    """From the first token of an attributed construct, return
    (end_offset_exclusive, form) with form in 'semi' | 'block' | 'arm'.
    (None, None) when the span cannot be determined (reported, never guessed).

    'arm' covers `#[cfg(test)] Pattern => expr,` — attributes on match arms are
    legal and common (mock enum variants); the arm, not a following item, is
    the gated construct."""
    n = len(blanked)
    depth = 0
    while i < n:
        c = blanked[i]
        if c in "([":
            depth += 1
        elif c in ")]":
            depth -= 1
            if depth < 0:
                return None, None
        elif c == "{":
            end = _match_bracket(blanked, i, "{", "}")
            return (end, "block") if end else (None, None)
        elif c == ";" and depth <= 0:
            return i + 1, "semi"
        elif c == "=" and depth <= 0 and i + 1 < n and blanked[i + 1] == ">":
            return _arm_end(blanked, i + 2)
        i += 1
    return None, None


def _arm_end(blanked, i):
    """End of a match arm body starting just after its `=>`."""
    n = len(blanked)
    i = _skip_trivia(blanked, i, n)
    if i < n and blanked[i] == "{":
        end = _match_bracket(blanked, i, "{", "}")
        if end is None:
            return None, None
        j = _skip_trivia(blanked, end, n)
        return (j + 1 if j < n and blanked[j] == "," else end), "arm"
    depth = 0
    while i < n:
        c = blanked[i]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            if depth == 0:
                return i, "arm"      # last arm: the match block's `}`
            depth -= 1
        elif c == "," and depth == 0:
            return i + 1, "arm"
        i += 1
    return None, None


def _inline_mod_prefix(blanked):
    """[(offset, ('a','b',…))] checkpoints of the enclosing inline-`mod` path,
    so `mod x;` inside `pub mod stemmers { … }` resolves under stemmers/."""
    events = [(0, ())]
    stack = []
    mod_head = re.compile(r"\bmod\s+([A-Za-z_]\w*)\s*$")
    for i, c in enumerate(blanked):
        if c == "{":
            m = mod_head.search(blanked[max(0, i - 200):i])
            stack.append(m.group(1) if m else None)
            events.append((i + 1, tuple(x for x in stack if x)))
        elif c == "}":
            if stack:
                stack.pop()
            events.append((i + 1, tuple(x for x in stack if x)))
    return events


def _prefix_at(events, off):
    import bisect
    k = bisect.bisect_right([e[0] for e in events], off) - 1
    return events[max(0, k)][1]


_ATTR_RE = re.compile(r"#!?\[")
_MOD_DECL = re.compile(r"^\s*(?:pub\s*(?:\([^)]*\)\s*)?)?(?:unsafe\s+)?mod\s+([A-Za-z_]\w*)\s*;")
_PATH_ATTR = re.compile(r'path\s*=\s*"([^"]*)"')


def analyze_file_items(text, path=""):
    """One file's structural facts.

    Returns dict:
      cfg_test_spans : [(first_line, last_line)] 1-based inclusive, for
                       test-only-gated items (attributes included).
      test_mods      : [(name, path_override_or_None, decl_line, mod_prefix)]
                       file-module declarations that are test-only gated.
      plain_mods     : same shape, for every other file-module declaration
                       (used to inherit test-ness down the module graph).
                       `mod_prefix` is the enclosing inline-`mod` path.
      includes       : [(relative_path, line)] `include!("x.rs")` targets —
                       text includes, compiled as part of this file.
      inner_cfg_test : True if the file carries `#![cfg(test)]`.
      test_items     : count of `#[test]` / `#[kani::proof]` items.
      ambiguities    : [str] loud diagnostics for this file.
    """
    blanked = blank_literals(text)
    starts = _line_index(text)
    n = len(blanked)
    prefixes = _inline_mod_prefix(blanked)
    res = dict(cfg_test_spans=[], test_mods=[], plain_mods=[], includes=[],
               inner_cfg_test=False, test_items=0, ambiguities=[])

    # ---- attribute runs.  We walk every `#[`/`#![` in the blanked text; an
    # attribute that is not in item position simply never has a following
    # item that parses, and is reported rather than guessed at.
    consumed_to = -1
    for m in _ATTR_RE.finditer(blanked):
        i = m.start()
        if i < consumed_to:
            continue
        inner = blanked[i + 1] == "!"
        br = i + (2 if inner else 1)
        end_attr = _match_bracket(blanked, br, "[", "]")
        if end_attr is None:
            continue
        attr_src = " ".join(text[br + 1:end_attr - 1].split())
        line = _lineno(starts, i)

        if re.match(r"^(?:test|kani::proof)\b", attr_src) and not inner:
            res["test_items"] += 1

        cm = re.match(r"^cfg\s*\((.*)\)$", attr_src, re.S)
        if cm is None:
            continue
        pred = cm.group(1)
        is_test = cfg_implies_test(pred)
        if not is_test:
            if mentions_test(pred):
                res["ambiguities"].append(
                    f"{path}:{line}: cfg predicate mentions `test` but is not "
                    f"test-only, item KEPT in scope: #[cfg({pred})]")
            continue
        if inner:
            res["inner_cfg_test"] = True
            continue

        # collect the rest of the attribute run, then the item
        j = end_attr
        while True:
            j = _skip_trivia(blanked, j, n)
            am = _ATTR_RE.match(blanked, j)
            if not am:
                break
            k = _match_bracket(blanked, j + (2 if blanked[j + 1] == "!" else 1),
                               "[", "]")
            if k is None:
                break
            j = k
        item_start = j
        end, form = _item_end(blanked, item_start)
        if end is None:
            res["ambiguities"].append(
                f"{path}:{line}: #[cfg({pred})] item span could not be "
                f"determined (macro body or unparsable item) — KEPT in scope")
            continue
        res["cfg_test_spans"].append((line, _lineno(starts, end - 1)))
        consumed_to = end
        if form == "semi":
            md = _MOD_DECL.match(blanked[item_start:end])
            if md:
                over = _PATH_ATTR.search(text[i:item_start])
                res["test_mods"].append(
                    (md.group(1), over.group(1) if over else None, line,
                     _prefix_at(prefixes, item_start)))

    # ---- every file-module declaration (for test-ness inheritance)
    test_names = {t[0] for t in res["test_mods"]}
    for lm in re.finditer(r"(?m)^([^\n]*?)\bmod\s+([A-Za-z_]\w*)\s*;", blanked):
        md = _MOD_DECL.match(lm.group(0).lstrip())
        if not md or md.group(1) in test_names:
            continue
        decl_off = lm.start() + len(lm.group(1))
        line = _lineno(starts, decl_off)
        # a #[path = "…"] override sits on this or one of the preceding lines
        head = text[max(0, lm.start() - 300):decl_off]
        pa = list(re.finditer(r'#\s*\[\s*path\s*=\s*"([^"]*)"\s*\]', head))
        over = pa[-1].group(1) if pa and head[pa[-1].end():].count("\n") <= 1 \
            else None
        res["plain_mods"].append(
            (md.group(1), over, line, _prefix_at(prefixes, decl_off)))

    # ---- include!("x.rs"): a TEXT include, not a module. The included lines
    # are compiled as part of this file (and llvm attributes them to the
    # included file), so the target is reachable production code.
    for im in re.finditer(r'\binclude!\s*\(\s*"([^"]+\.rs)"', text):
        res["includes"].append((im.group(1), _lineno(starts, im.start())))
    return res


# ------------------------------------------------------------- module graph

def _child_dir(rel):
    """Directory that `mod NAME;` inside file `rel` resolves against."""
    d, base = os.path.split(rel)
    if base in ("lib.rs", "main.rs", "mod.rs"):
        return d
    return os.path.join(d, base[:-3])


def _resolve(repo, rel, name, override, prefix=()):
    """Resolve `mod name;` declared in `rel` inside inline modules `prefix`."""
    d = os.path.join(_child_dir(rel), *prefix)
    cands = ([os.path.join(d, override)] if override else
             [os.path.join(d, name + ".rs"), os.path.join(d, name, "mod.rs")])
    for c in cands:
        if os.path.isfile(os.path.join(repo, c)):
            return os.path.normpath(c)
    return None


_PATH_TEST_DIR = re.compile(r"(?:^|/)(?:tests|benches)/")


class TestScope:
    """Per-crate test-code oracle. Build once, query by repo-relative path."""

    def __init__(self, repo, crate):
        self.repo, self.crate = repo, crate
        self.ambiguities = []
        self.reason = {}          # rel -> reason token
        self._facts = {}
        self._build()

    # -- helpers
    def _files(self):
        root = os.path.join(self.repo, self.crate, "src")
        out = []
        for dp, dns, names in os.walk(root):
            dns[:] = [d for d in dns if d != "target"]
            for nm in names:
                if nm.endswith(".rs"):
                    out.append(os.path.normpath(os.path.relpath(
                        os.path.join(dp, nm), self.repo)))
        return sorted(out)

    def _fact(self, rel):
        if rel not in self._facts:
            try:
                text = _read(os.path.join(self.repo, rel))
            except OSError:
                self._facts[rel] = analyze_file_items("", rel)
                return self._facts[rel]
            f = analyze_file_items(text, rel)
            self.ambiguities.extend(f["ambiguities"])
            self._facts[rel] = f
        return self._facts[rel]

    def _build(self):
        files = self._files()
        children = {}
        seeds = []
        for rel in files:
            f = self._fact(rel)
            kids_test, kids_plain = [], []
            for name, over, line, pre in f["test_mods"]:
                tgt = _resolve(self.repo, rel, name, over, pre)
                if tgt is None:
                    self.ambiguities.append(
                        f"{rel}:{line}: test-gated `mod {name};` resolves to no "
                        f"file on disk — nothing excluded here; the module is "
                        f"either dead or misplaced (source defect)")
                else:
                    kids_test.append(tgt)
            for name, over, line, pre in f["plain_mods"]:
                tgt = _resolve(self.repo, rel, name, over, pre)
                if tgt is not None:
                    kids_plain.append(tgt)
            for inc, line in f["includes"]:
                tgt = os.path.normpath(os.path.join(os.path.dirname(rel), inc))
                if os.path.isfile(os.path.join(self.repo, tgt)):
                    kids_plain.append(tgt)
                else:
                    self.ambiguities.append(
                        f"{rel}:{line}: include!(\"{inc}\") target not found")
            children[rel] = kids_test + kids_plain
            for t in kids_test:
                seeds.append((t, "cfg-test-mod"))
            if f["inner_cfg_test"]:
                seeds.append((rel, "inner-cfg-test"))
            if _PATH_TEST_DIR.search(rel):
                seeds.append((rel, "test-dir"))

        # T4: harness-only file — has test items and nothing production-ish.
        for rel in files:
            f = self._fact(rel)
            if f["test_items"] and rel not in dict(seeds):
                if self._is_harness_only(rel, f):
                    seeds.append((rel, "harness-only"))

        # T5: unreachable from the crate's entry points => never compiled.
        reach = self._reachable(files, children)
        for rel in sorted(set(files) - reach):
            f = self._fact(rel)
            if f["test_items"]:
                seeds.append((rel, "orphan-test-file"))
                self.ambiguities.append(
                    f"{rel}: unreachable from the crate entry points and holds "
                    f"{f['test_items']} #[test]/#[kani::proof] item(s) — "
                    f"treated as TEST CODE (out of scope); rustc never "
                    f"compiles it, so no instrument can reach it")
            else:
                self.ambiguities.append(
                    f"{rel}: unreachable from the crate entry points (no `mod` "
                    f"chain reaches it) but holds no test items — KEPT in "
                    f"scope; verify whether it is dead code or a resolver miss")

        # transitive closure over the module graph
        stack = list(seeds)
        while stack:
            rel, why = stack.pop()
            if rel in self.reason:
                continue
            self.reason[rel] = why
            for kid in children.get(rel, ()):
                if kid not in self.reason:
                    stack.append((kid, "child-of-test-mod"))

    def _entry_points(self, files):
        """Crate compilation roots: the conventional entries plus every `path`
        key in Cargo.toml (covers [lib]/[[bin]]/[[test]]/[[bench]] overrides,
        parsed textually — we only need the set, not the section)."""
        roots = []
        for cand in ("src/lib.rs", "src/main.rs"):
            rel = os.path.normpath(os.path.join(self.crate, cand))
            if os.path.isfile(os.path.join(self.repo, rel)):
                roots.append(rel)
        bindir = os.path.join(self.repo, self.crate, "src", "bin")
        if os.path.isdir(bindir):
            for nm in sorted(os.listdir(bindir)):
                if nm.endswith(".rs"):
                    roots.append(os.path.normpath(
                        os.path.join(self.crate, "src", "bin", nm)))
                elif os.path.isfile(os.path.join(bindir, nm, "main.rs")):
                    roots.append(os.path.normpath(os.path.join(
                        self.crate, "src", "bin", nm, "main.rs")))
        try:
            man = _read(os.path.join(self.repo, self.crate, "Cargo.toml"))
        except OSError:
            man = ""
        for m in re.finditer(r'(?m)^\s*path\s*=\s*"([^"]*)"', man):
            rel = os.path.normpath(os.path.join(self.crate, m.group(1)))
            if os.path.isfile(os.path.join(self.repo, rel)):
                roots.append(rel)
        return [r for r in dict.fromkeys(roots) if r in set(files)]

    def _reachable(self, files, children):
        seen = set()
        stack = list(self._entry_points(files))
        while stack:
            rel = stack.pop()
            if rel in seen:
                continue
            seen.add(rel)
            stack.extend(children.get(rel, ()))
        return seen

    def _is_harness_only(self, rel, f):
        """True iff, outside cfg(test) spans, the file holds no item that
        could carry production code. Conservative: any `fn`/`impl`/`macro_rules!`
        outside a test span (and not itself #[test]/#[kani::proof]) disqualifies."""
        try:
            text = _read(os.path.join(self.repo, rel))
        except OSError:
            return False
        blanked = blank_literals(text)
        starts = _line_index(text)
        spans = f["cfg_test_spans"]
        # lines belonging to #[test]/#[kani::proof] items
        test_lines = set()
        for m in _ATTR_RE.finditer(blanked):
            br = m.start() + (2 if blanked[m.start() + 1] == "!" else 1)
            e = _match_bracket(blanked, br, "[", "]")
            if e is None:
                continue
            if not re.match(r"^\s*(?:test|kani::proof)\b", text[br + 1:e - 1]):
                continue
            end, _form = _item_end(blanked, _skip_trivia(blanked, e, len(blanked)))
            if end:
                test_lines.update(range(_lineno(starts, m.start()),
                                        _lineno(starts, end - 1) + 1))
        for m in re.finditer(r"(?m)^\s*(?:pub\s*(?:\([^)]*\)\s*)?)?"
                             r"(?:default\s+|const\s+|async\s+|unsafe\s+|extern\s+\"[^\"]*\"\s+)*"
                             r"(fn|impl|macro_rules!)\b", blanked):
            ln = _lineno(starts, m.start())
            if any(a <= ln <= b for a, b in spans) or ln in test_lines:
                continue
            return False
        return True

    def is_test_file(self, rel):
        return os.path.normpath(rel) in self.reason


# ---------------------------------------------------------------- repo cache

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_CACHE = {}
_DIAG = []


def set_repo_root(path):
    global _REPO_ROOT
    _REPO_ROOT = os.path.abspath(path)
    _CACHE.clear()
    del _DIAG[:]


def repo_root():
    return _REPO_ROOT


def crate_of(rel):
    """Nearest ancestor directory of `rel` holding a Cargo.toml, or None."""
    d = os.path.dirname(os.path.normpath(rel))
    while d and d not in (".", "/"):
        if os.path.isfile(os.path.join(_REPO_ROOT, d, "Cargo.toml")):
            return d
        d = os.path.dirname(d)
    return None


def scope_for_crate(crate):
    if crate not in _CACHE:
        ts = TestScope(_REPO_ROOT, crate)
        _CACHE[crate] = ts
        _DIAG.extend(ts.ambiguities)
    return _CACHE[crate]


def diagnostics():
    """All ambiguities recorded so far (deduped, stable order)."""
    seen, out = set(), []
    for d in _DIAG:
        if d not in seen:
            seen.add(d)
            out.append(d)
    return out


def is_test_file(path):
    """Is this path test code? `path` may be absolute or repo-relative.

    Structural: consults the enclosing crate's module graph. Falls back to the
    path-component rule for files outside any crate."""
    p = os.path.normpath(path)
    if os.path.isabs(p):
        try:
            p = os.path.relpath(p, _REPO_ROOT)
        except ValueError:
            return bool(_PATH_TEST_DIR.search(p))
    if p.startswith(".."):
        return bool(_PATH_TEST_DIR.search(p))
    if _PATH_TEST_DIR.search(p):
        return True
    crate = crate_of(p)
    if crate is None:
        return False
    return scope_for_crate(crate).is_test_file(p)


def test_reason(path):
    p = os.path.normpath(path)
    if os.path.isabs(p):
        p = os.path.relpath(p, _REPO_ROOT)
    if _PATH_TEST_DIR.search(p):
        return "test-dir"
    crate = crate_of(p)
    if crate is None:
        return None
    return scope_for_crate(crate).reason.get(p)
