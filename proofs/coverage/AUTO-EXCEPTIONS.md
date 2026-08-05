# Auto-classification of instrument-unmappable residual shapes (accelerator #1)

Michael-approved campaign accelerator: campaign lanes were spending the tail
of every crate hand-adjudicating red lines that belong to KNOWN
instrument-unmappable classes. `merge-coverage.py --auto-exceptions` now
detects those shapes mechanically among the uncovered residual and emits
them as `auto:`-prefixed rows in `<outdir>/auto-exceptions.tsv` — DISTINCT
from hand-adjudicated exception rows. An `auto:` row is a measurement note
("the instrument emits no mapping for this line"), never a semantic
adjudication and never a denominator change; unreachable-arm /
platform-other / defensive-c-parity remain human work.

Implementation: `auto_exceptions.py` (importable; also a standalone CLI for
re-classifying an existing merge output). Classes are licensed by
`rig-auto-classes.py` — the SLOC-RULE-V2 §2 verification pattern packaged as
a re-runnable self-checking rig (rustc 1.96 `-C instrument-coverage` toy →
its own llvm-cov lcov → per-line EXPECT-DA / EXPECT-NODA assertions). A
class may classify shape-only ONLY while the rig is green for it.

## Classes and their license

| class | shape | license |
|---|---|---|
| `auto:let-decl` | bare `let x: T;` without initializer, bracket-balanced | rig GREEN; 0 DA counterexamples in 222 tree-wide shape matches |
| `auto:macro-decl` | declaration rows inside generator-macro invocation blocks (macro_attrib.py geometry); `\|` (closure) rows rejected | rig GREEN; 0/554 after the closure guard (the 11 closure rows ARE mapped — SLOC-v2's `\|` keep-guard, measured again) |
| `auto:table-head` | multi-line const/static bracket-initializer HEAD | rig GREEN; 0/670 tree-wide |
| `auto:fmt-cont` | literal/bare-path continuation lines of format-family macro invocations (call-bearing lines excluded) | rig GREEN for the shape, but CONTEXT-DEPENDENT at tree scale: 1,693 of 4,382 residual shape matches DID carry a DA record → classifies ONLY with per-capture line-table proof of no DA |
| `auto:call-str-cont` | string-literal-only continuation of a plain (non-macro) call arg list | mapping ambiguous by construction (rig toy maps it; real captures show NO-DA for the same shape) → line-table evidence required |

| `auto:macro-decl-defn` | item-declaration rows (tuple/unit struct, impl/trait header, const/static, assoc `type`) inside `macro_rules!` DEFINITION bodies; template `fn` lines stay unmatched (they ARE mapped) | rig GREEN (gap-1 probes); 0 vetoes / 0 covered counterexamples in 59 tree-wide matches |
| `auto:macro-inv-cont` | head/argument lines of multi-line PAREN-form generator-macro invocations; whole span skipped when any line carries `\|` (closure arg bodies at the invocation site ARE mapped — 2 covered counterexamples found and excluded) | rig GREEN (gap-2 probes); 0/558 after the closure-span guard |
| `auto:include-row` | whole-line `include!(..)` rows (included code's spans live in the included file) | rig GREEN; 0/10 tree-wide |
| `auto:brace-table-head` | multi-line const/static data bindings: struct-literal head + call/closure-free field rows, and `=`-terminated heads + path/literal continuations. Bare `= {` const BLOCKS never match (const-eval code — matching one was a measured false positive vs lane-0b's const-eval-only row) | rig GREEN (brace-table-head/-field, eq-cont probes); 0/2,208 tree-wide |

(`auto:let-decl` now also covers the no-ascription `let x;` deferred-init
form — Lane-F gap 4, rig-probed; 0/525 tree-wide.)

Two standing guards on every class: the **line-table veto** (a DA record —
llvm's own per-line mappability verdict, from a full lcov incl. count 0 —
means the line is a real red signal and is never auto-classified) and
**fail-open** (any shape without a green rig demonstration stays
unclassified; the misses below are deliberate). Tree falsification for the
2026-07-31 classes ran on BOTH sides: DA vetoes among uncovered
shape-matches AND shape-matches inside the covered numerator (a covered
match = direct counterexample).

`merge-coverage.py --auto-exceptions` now defaults the line table to the
merge's own `--fuzz-lcov`/`--regress-lcov` inputs when `--line-table-lcov`
is not given, and prints exactly which lcov files to pass whenever
evidence-class rows are HELD as `no_table_evidence` — preserve the raw
capture lcov files; they are also the SLOC-v2 line-table precedence input.

## Validation round 2 (Lane-F, 2026-07-31)

Against Lane-F's 32 hand rows (`proofs/p1-lanef` @ 33cba48bf4,
`lanef/residual-rows-lanef.tsv`), replayed over the covrf capture
(gap-target files source-identical) with its regress+fuzz line tables:
**30/32 reproduced, 0 extras** (was 4/32 before the gap classes). The 2
misses are cryptohashfuncs 68-69 — `const fn` builder body lines —
deliberately unclassifiable: llvm MAPS const-fn bodies (SLOC-v2 §6b), so
"const-eval-only" needs the semantic fact that all callers are const
contexts; a mechanical match would misclassify genuinely-dead functions.
The 361-row corpus revalidated unchanged after the additions: 98/101, zero
semantic collisions, zero extras.

## Validation on real data (2026-07-31)

Against Lane-C's and Lane-0B's hand-adjudicated ledgers
(`proofs/p1-lanec` @ 7939205cbe `exceptions.tsv`, `proofs/p1-lane0b` @
022b35134df `phase1-exceptions.tsv`; 361 line-grain rows), replayed over
the covrf full-tree capture (same source state; mac/src/lib.rs +5/+6
comment-only offset mapped) with its preserved regress+fuzz lcov as the
line table:

- **Reproduced 98 of 101 mechanically-classifiable rows**: 64 macro-decl,
  14 let-decl, 10 table-head, 9 fmt-cont, 1 call-str-cont (the mac8
  `with_hint` line, via line-table evidence).
- **Missed 3, all outside the licensed classes by design** (fail-open):
  ryu d2s.rs:258 / f2s.rs:294 (` as u64;` cast continuation of a
  multi-line expression) and mac lib.rs:293 (struct-literal head
  continuation `Ok(MacAddr {`). Candidate future classes; each needs its
  own rig demonstration first.
- **Zero false positives**: no auto row collided with any of the 260
  semantic ledger rows, and zero auto rows fell outside the ledger.
- Tree-wide (covrf capture, 259,621 uncovered lines): 4,363 auto rows
  (fmt-cont 2,571, table-head 670, macro-decl 543, call-str-cont 359,
  let-decl 220) — the mechanical share of the residual is ~1.7%.
- Rendered-output audit (adt/uuid): auto rows are exactly the 7 `fc_*`
  declaration rows + the `UUID_BUILTINS` head; every remaining red line
  eyeballed as an honest signal (unexecuted send/recv paths, error
  constructors, const-fn bodies — correctly left to human adjudication).

Integration smoke: `merge-coverage.py --auto-exceptions --line-table-lcov
regress.lcov` over the uuid scope writes the tsv and a summary.json
`auto_exceptions` block (8 rows, matching the ledger exactly).

## Denominator defect: test-only files matched by PREFIX, not by cfg(test) (p1-laneac, 2026-07-31)

`proofs/coverage/sloc_rules.py:469` excludes test files with
`re.match(r"^tests.*\.rs$", base) or "/tests/" in path` — a **prefix** match. A
`#[cfg(test)]`-gated file module whose name does not START with `tests` is
therefore counted as product code in the v2 denominator:

- `crates/backend/utils/adt/multirangetypes/src/corpus_tests.rs` — **444 lines**,
  declared `#[cfg(test)] mod corpus_tests;` in lib.rs:3-4, counted in full.
- `crates/backend/utils/adt/geo/src/ws_tests.rs` — 33 lines, same class, already
  flagged by lane p1-laner in its adt/geo claim note.

`cfg_test_spans()` cannot catch these either: it looks for `#[cfg(test)]` followed
by a BRACE in the same file, and a file-module declaration ends in `;`, so the
gating lives in lib.rs while the lines live in the other file.

The lane p1-laneac accounting uses the corrected denominator (2,982 in-scope
non-test lines for its two crates, excluding corpus_tests.rs by hand). The
TREE-WIDE capture will over-count this crate by 444 lines until the rule is fixed.

PROPOSED FIX (not applied here — it moves every crate's denominator, so it wants
one owner and one re-baseline): when a crate root declares
`#[cfg(test)] mod NAME;`, exclude `NAME.rs` / `NAME/mod.rs` from that crate's
denominator. Mechanical, exact, and strictly better than a filename heuristic.
