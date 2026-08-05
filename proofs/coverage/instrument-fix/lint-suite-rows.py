#!/usr/bin/env python3
"""lint-suite-rows.py — offline STRUCTURAL linter for proofs/SUITE.tsv.

Complements check-suite-names.py, which validates harness NAMES against
ground truth (`cargo kani list`, minutes of compilation per family). This
linter validates row SHAPE only: no cargo, no solver, no network, runs in
well under a second. It is the authoring-time gate — run it before every
SUITE.tsv commit.

Why it exists: four separate classes of malformed rows were authored by four
different lanes and only surfaced on a full fleet solve run (see
proofs/FLEET-SOLVING.md "Measured baseline"), where each malformed row burns
a solver slot and reports as a deviation indistinguishable from a real
divergence:

  1. unexpanded `c/<FAMILY>.c` placeholders in the flags column
     -> goto-cc "No such file", 62 rows failed on the fleet;
  2. prose leaking into the flags column (`... --exact (recipe: run-cmp.sh
     incl ...)`) -> `cargo-kani: unrecognized subcommand '(recipe:'`;
  3. `expected` / `tier` tokens outside the vocabulary the runner knows
     (`info`, `wall`, `fail`, `red`, `covers`; tier `control`, `probe`)
     -> run-suite.sh BAD-MANIFEST-ROW, and (worse) a non-vocabulary TIER
     silently excludes the row from EVERY gate tier, so must-fail negative
     controls stop guarding against a vacuous rig without anything saying so;
  4. shuffled columns — time/expected/tier packed space-separated into the
     harness field, leaving expected/tier/time_s empty (fixed in 4b69bcd5a2).

Vocabulary is derived, not invented:
  expected  green | must-fail | wall-recorded   -- the three run-suite.sh
                                                  `case "$expected"` arms
            missing                             -- adjudicated-absent rows,
                                                  check-suite-names.py rule 2
            unmeasured                          -- dark-harness-sweep
                                                  registration (1e63949b81)
  tier      per-commit | release-gate | calibration | defect-witness |
            unmeasured                          -- README.md "Layout" +
                                                  run-suite.sh row_selected()

Checks (violations; any one exits 1):
  COLS        row does not have exactly 7 tab-separated columns
  EMPTY       a required column (all but `notes`) is empty
  FAMILY      family column is not a directory under proofs/
  HARNESS     harness is not a bare/qualified Rust path (catches packed data:
              embedded whitespace, flags, numbers)
  EXPECTED    expected is not in the vocabulary above
  TIER        tier is not in the vocabulary above
  TIME        time_s is neither a number nor the `?` unmeasured sentinel
  PLACEHOLDER an unexpanded `<...>` template placeholder in any column
  FLAGS       flags column is not a recognized invocation shape, or carries
              shell metacharacters / prose parentheses
  FLAGPATH    a `--c-lib`/`--c-libs` path in flags does not exist in the
              family crate (this is what actually caught the placeholders on
              the fleet; check-suite-names.py never looks at flag paths)

Advisories (reported, never fail): rows whose `expected` token is in the
manifest vocabulary but which run-suite.sh's own `case` cannot score
(`missing`, `unmeasured` -> BAD-MANIFEST-ROW under `run-suite.sh all`), and
exact duplicate rows.

Usage:
  ./lint-suite-rows.py                 # lint proofs/SUITE.tsv
  ./lint-suite-rows.py path/to.tsv     # lint another manifest copy
"""

import os
import re
import sys
from collections import Counter, defaultdict

PROOFS_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_TSV = os.path.join(PROOFS_DIR, "SUITE.tsv")

COLUMNS = ["family", "harness", "flags", "expected", "tier", "time_s", "notes"]
REQUIRED = COLUMNS[:-1]  # notes may legitimately be empty

EXPECTED_TOKENS = {"green", "must-fail", "wall-recorded", "missing",
                   "unmeasured"}
# Tokens run-suite.sh's `case "$expected"` can actually score; anything else
# in EXPECTED_TOKENS is manifest bookkeeping the runner reports as a bad row.
RUNNER_SCORED = {"green", "must-fail", "wall-recorded"}
TIER_TOKENS = {"per-commit", "release-gate", "calibration", "defect-witness",
               "unmeasured"}

RUST_PATH = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(::[A-Za-z_][A-Za-z0-9_]*)*$")
NUMBER = re.compile(r"^[0-9]+(\.[0-9]+)?$")
PLACEHOLDER = re.compile(r"<[^\t>]*>")
# Whole-field family-script sentinel, e.g. `(run-cmp.sh)`.
SCRIPT_SENTINEL = re.compile(r"^\((?:[\w./-]+\.sh)\)$")
NATIVE_BIN = re.compile(r"^cargo run\b")
C_LIB = re.compile(r"--c-libs?[= ]\s*(\S+)")
SHELL_META = re.compile(r"[;&|$`>]|\|\|")


class Report:
    def __init__(self):
        self.violations = []      # (lineno, code, message)
        self.advisories = []      # (lineno, code, message)
        self.by_code = Counter()

    def bad(self, lineno, code, msg):
        self.violations.append((lineno, code, msg))
        self.by_code[code] += 1

    def note(self, lineno, code, msg):
        self.advisories.append((lineno, code, msg))


def lint_row(rep, lineno, parts, raw):
    """Lint one data row. Returns False if the row is too broken to continue."""
    if len(parts) != 7:
        rep.bad(lineno, "COLS",
                f"{len(parts)} columns, expected 7 "
                f"(tab-separated: {'/'.join(COLUMNS)}); row starts "
                f"{raw[:70]!r}")
        if len(parts) < 4:
            return False
        parts = parts + [""] * (7 - len(parts))

    fam, harness, flags, expected, tier, time_s, _notes = parts[:7]

    for name, val in zip(REQUIRED, (fam, harness, flags, expected, tier,
                                    time_s)):
        if not val.strip():
            rep.bad(lineno, "EMPTY",
                    f"{fam or '?'}/{harness or '?'}: required column "
                    f"`{name}` is empty (shuffled/short row?)")

    for name, val in zip(COLUMNS, parts[:7]):
        m = PLACEHOLDER.search(val)
        if m and name != "notes":
            rep.bad(lineno, "PLACEHOLDER",
                    f"{fam}/{harness}: unexpanded template placeholder "
                    f"{m.group(0)!r} in `{name}` — the literal text is passed "
                    f"to the toolchain verbatim")

    if fam and not os.path.isdir(os.path.join(PROOFS_DIR, fam)):
        rep.bad(lineno, "FAMILY",
                f"{fam}/{harness}: no such family crate directory "
                f"proofs/{fam}/")

    if harness and not RUST_PATH.match(harness):
        rep.bad(lineno, "HARNESS",
                f"{fam}: harness field {harness!r} is not a bare or "
                f"`::`-qualified Rust path — looks like packed data, not a "
                f"harness name (columns shuffled?)")

    if expected and expected not in EXPECTED_TOKENS:
        rep.bad(lineno, "EXPECTED",
                f"{fam}/{harness}: expected={expected!r} is not in the "
                f"vocabulary {sorted(EXPECTED_TOKENS)}")
    elif expected in EXPECTED_TOKENS - RUNNER_SCORED:
        rep.note(lineno, "EXPECTED-UNSCORED",
                 f"{fam}/{harness}: expected={expected!r} is manifest "
                 f"bookkeeping; run-suite.sh scores it BAD-MANIFEST-ROW")

    if tier and tier not in TIER_TOKENS:
        rep.bad(lineno, "TIER",
                f"{fam}/{harness}: tier={tier!r} is not in the vocabulary "
                f"{sorted(TIER_TOKENS)} — run-suite.sh row_selected() never "
                f"picks it, so this row is silently in NO gate tier")

    if time_s and not (NUMBER.match(time_s) or time_s == "?"):
        rep.bad(lineno, "TIME",
                f"{fam}/{harness}: time_s={time_s!r} is neither a number of "
                f"seconds nor the `?` unmeasured sentinel (run-suite.sh "
                f"derives the per-harness timeout from it)")

    lint_flags(rep, lineno, fam, harness, flags)
    return True


def lint_flags(rep, lineno, fam, harness, flags):
    if not flags.strip():
        return  # already reported EMPTY
    kani = "-Z" in flags or flags.startswith("-")
    if not (kani or NATIVE_BIN.match(flags) or SCRIPT_SENTINEL.match(flags)):
        rep.bad(lineno, "FLAGS",
                f"{fam}/{harness}: flags {flags[:60]!r} is not a recognized "
                f"invocation (kani flags, `cargo run ... --bin X`, or a "
                f"`(script.sh)` sentinel)")
        return

    if kani:
        if "(" in flags or ")" in flags:
            rep.bad(lineno, "FLAGS",
                    f"{fam}/{harness}: parenthesised prose in the flags "
                    f"column — cargo-kani parses it as arguments "
                    f"({flags[flags.index('('):][:50]!r})")
        if flags.count('"') % 2 or flags.count("'") % 2:
            rep.bad(lineno, "FLAGS",
                    f"{fam}/{harness}: unbalanced quote in flags")
    # Placeholders are reported once, as PLACEHOLDER; don't re-report the `>`
    # inside `<FAMILY>` as a shell metacharacter.
    m = SHELL_META.search(PLACEHOLDER.sub("", flags))
    if m and not SCRIPT_SENTINEL.match(flags):
        rep.bad(lineno, "FLAGS",
                f"{fam}/{harness}: shell metacharacter {m.group(0)!r} in "
                f"flags (run-suite.sh word-splits this field unquoted)")

    if fam and os.path.isdir(os.path.join(PROOFS_DIR, fam)):
        for path in C_LIB.findall(flags):
            if PLACEHOLDER.search(path):
                continue  # already reported as PLACEHOLDER
            if not os.path.exists(os.path.join(PROOFS_DIR, fam, path)):
                rep.bad(lineno, "FLAGPATH",
                        f"{fam}/{harness}: --c-lib path {path!r} does not "
                        f"exist in proofs/{fam}/")


def main():
    tsv = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_TSV
    rep = Report()

    with open(tsv) as f:
        lines = f.read().split("\n")

    if not lines or not lines[0].startswith("\t".join(COLUMNS[:2])):
        sys.exit(f"FATAL: {tsv} header is not "
                 f"{'/'.join(COLUMNS)} (got {lines[0][:60]!r})")

    data_rows = 0
    comments = 0
    blanks = 0
    seen = defaultdict(list)
    for lineno, raw in enumerate(lines[1:], start=2):
        if not raw.strip():
            blanks += 1
            continue
        if raw.lstrip().startswith("#"):
            comments += 1
            continue
        parts = raw.split("\t")
        data_rows += 1
        lint_row(rep, lineno, parts, raw)
        if len(parts) >= 3:
            seen[(parts[0], parts[1], parts[2])].append(lineno)

    for key, linenos in sorted(seen.items()):
        if len(linenos) > 1:
            rep.note(linenos[0], "DUPLICATE",
                     f"{key[0]}/{key[1]}: identical (family, harness, flags) "
                     f"row repeated at lines {linenos} — each one burns a "
                     f"solver slot")

    # Advisories are aggregated by code (EXPECTED-UNSCORED alone covers ~700
    # rows; a per-row dump would bury the violations).
    adv_by_code = defaultdict(list)
    for lineno, code, msg in rep.advisories:
        adv_by_code[code].append((lineno, msg))
    for code in sorted(adv_by_code):
        items = adv_by_code[code]
        print(f"ADVISE  {code} — {len(items)} rows, e.g.:")
        for lineno, msg in items[:3]:
            print(f"          line {lineno}: {msg}")
    if rep.advisories:
        print()
    for lineno, code, msg in sorted(rep.violations):
        print(f"VIOLATION {code:16s} line {lineno}: {msg}")

    print(f"\nlint-suite-rows: {tsv}")
    print(f"  data rows:   {data_rows}  "
          f"(+{comments} comment, {blanks} blank)")
    print(f"  advisories:  {len(rep.advisories)}")
    print(f"  violations:  {len(rep.violations)}")
    for code, n in sorted(rep.by_code.items(), key=lambda kv: -kv[1]):
        print(f"      {code:12s} {n}")
    if rep.violations:
        bad_lines = len({ln for ln, _, _ in rep.violations})
        print(f"\nFAIL: {len(rep.violations)} structural violations on "
              f"{bad_lines} of {data_rows} rows.")
        return 1
    print("\nPASS: every row is structurally well-formed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
