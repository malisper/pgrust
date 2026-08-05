# Coverage instrument fixes — the two smoke-test blockers, closed

Companion to `SMOKE-RESULT.md`, which recommended running the full tree
but only after two instrument defects were fixed. Both are fixed here, on
`proofs/coverage-instrument-fix`. Everything below was run, not reasoned.

Artifacts of record live in `proofs/coverage/instrument-fix/` (joblists,
censuses, runner logs, merge outputs). The originals were produced under a
session scratchpad that a disk sweeper deleted mid-lane — twice — so every
run below was re-executed with outputs committed in-tree; the numbers were
re-verified to match, and one additional defect was found and fixed in the
re-run: `merge-coverage.py`'s refusal path crashed on a fresh `--outdir`
(census.json written before the directory existed) instead of exiting 3.

Toolchain: Kani 0.67.0, cargo 1.96.0, macOS aarch64, `--solver kissat`
per SUITE row. Crate under re-measurement: `adt_float`, 2,215 SLOC.

## Blocker 1 — macro-attribution false UNCOVERED

### The defect

Kani reports each coverage region at the span the compiler recorded for
the generated MIR. For a `macro_rules!`-generated function that span is in
the macro **definition** body. Concretely, from a `casts::eq_dtoi4`
kaniraw:

    COVERED  adt_float::builtins::fc_dtoi4  [113,9]..[115,57]

Lines 113-115 are inside the `fc1t!` definition. The line that actually
declares `fc_dtoi4` — `builtins.rs:254`, inside the `fc1t! { … }`
invocation block — gets nothing, in any run, ever. It is a SLOC line in
the denominator that is **uncoverable by construction**.

### Option chosen: (a) attribute back to the invocation line

Implemented in `proofs/coverage/macro_attrib.py`, applied by
`merge-coverage.py`:

> A `COVERED` region lying inside a `macro_rules!` definition body also
> credits the source line that declares the generated function named in
> that region's `function` field, when that name resolves to exactly one
> declaration site.

Rejected: (b) excluding `fc*!` invocation blocks from the SLOC
denominator.

**The tradeoff, stated.** Both options remove the same false negatives.
They differ in what happens to the invocation lines that are *correctly*
uncovered. A `fc*!` declaration line names a real fmgr entry point; when
no proof reaches it, "uncovered" is the true and actionable answer. In
adt_float, of 121 declaration lines in generator-macro blocks:

| declaration lines                                    | count |
|------------------------------------------------------|-------|
| wrapper executed by the measured per-commit tier      | **54** (were false-uncovered) |
| wrapper has **no harness anywhere** in SUITE.tsv      | 55    |
| wrapper's only harnesses are release-gate tier        | 7     |
| ... calibration + release-gate                        | 3     |
| ... calibration only                                  | 2     |

Option (b) would have deleted all 121 — 67 of them honest signals, 55 of
which are genuinely unproved entry points — to fix 54 false ones, while
raising the reported percentage by shrinking the denominator. Option (a)
fixes exactly the 54 and leaves the 67 visible. Silently dropping lines
is its own hazard, and here it would have dropped the more interesting
lines.

**Why it cannot over-credit.** The naive form of (a) — "the macro body has
a covered region, so credit its invocations" — would credit all N
invocations of a macro from a single covered one, because N invocations
share one definition body. This implementation matches on
**generated-function identity** (`function` field → unique declaration
site) and refuses ambiguous resolutions, so a wrapper is credited only
when that wrapper itself executed. Coverage of the definition-body lines
is unchanged: the template really did run.

**Residual bias is published, not absorbed.** A generated function whose
name is not a token at its invocation cannot be resolved;
`summary.json.macro_attribution.stats.unresolved` counts those. Across the
seven scope crates 262 of 277 declaration lines (94.6%) are uniquely
resolvable; the shortfall is 14 lines in geo (`close2!`, `path_pt!`,
`poly2!` — lead identifier is not the generated name) plus 1 in jsonb.

### Not just `fc*!` — the tree-wide blast radius

The fix is generic over `macro_rules!` and needed to be. Scanning all
2,529 `.rs` files under `crates/`:

- 318 `macro_rules!` definitions, of which **198 generate items** (`fn`,
  `impl`, `struct`, `const`, …) and so can misattribute;
- **475 generator-macro invocation blocks, 2,753 declaration lines.**

Largest concentrations: `nodes/src/tags.rs` (481 lines, 1 block),
`guc_tables` (270 lines / 83 blocks — `int_var!`, `string_var!`,
`bool_var!`), `float` (127), `mb/conv` (108 — `conv_pair!`),
`init_small` (107), `int8` (100), `pseudotypes` (97), `int` (95). Within
the coverage scope: float 121 lines, numeric 55, geo 44, varlena 27,
network 21, varbit 6, jsonb 3.

Scoped OUT, with reasons:
- **`#[derive]` and proc macros** — rustc attributes generated impls to
  the deriving item's own span, which is a real line in the file. No
  correction needed, none applied.
- **Non-generator macros** (`println!`, `assert!`, `vec!`, …) — expansion
  is inlined into the caller and already attributed to the caller's line.
  Touching them would be a rewrite of attribution generally, not a fix to
  this defect class.
- **`const *_BUILTINS` fmgr tables** — const-eval'd data with no runtime
  counters. Dark to all three instruments for a different reason
  (COVERAGE.md already says so); not a macro problem.

### Re-measurement of adt_float

Same 140-harness per-commit set (float's own 97 + geo-cmp 26 +
numeric-probe 17; the `cash`/`misc-ops`/`bool` residual probe contributes
0 by measurement and was omitted), same merge, macro attribution the only
variable:

| adt_float, per-commit tier          | kani SLOC | %      |
|-------------------------------------|-----------|--------|
| prior published capture             | 197       | 8.89%  |
| this run, `--no-macro-attribution`  | **284**   | 12.82% |
| this run, macro attribution ON      | **338**   | **15.26%** |

284 reproduces SMOKE-RESULT's full-set number **exactly**, which
validates the re-run. Per file the +54 lands entirely in `builtins.rs`
(49 → 103); `funcs.rs` 71, `lib.rs` 164, `io.rs` 0, `aggregates.rs` 0 are
untouched — both zero-pinned falsifiers still hold.

### Reconciliation: 338, not the predicted 356

SMOKE-RESULT.md §3 predicted 356 (= 284 + 72), from a hand count that
"72 of 103 declaration lines name a wrapper that a harness in this very
set directly proves". The measured answer is **54**, so the corrected
figure is 338 / 2,215 = 15.26%, a **2.44pp** correction rather than
3.25pp. The 356 was an overestimate, and the discrepancy is fully
attributable:

- **12 of the 72 name wrappers whose harnesses are not in the measured
  tier.** The hand count matched wrapper names against harness names
  across all of SUITE.tsv. `fc_float8pl` is the example the smoke doc
  itself cites: `float-arith::eq_float8pl` exists, but its SUITE row is
  **release-gate**, and this capture measures the per-commit tier. Ditto
  `fc_float4mul`, `fc_float8mi`, `fc_float48pl`, `fc_float8mul` (also
  calibration), `fc_degrees`/`fc_radians` (calibration). Correctly
  uncovered for what was run.
- **The remaining ~6** are wrappers whose *core* function is proved by a
  per-commit harness that calls the core directly, never the fmgr wrapper.
  A proof of `float8_pl` does not execute `fc_float8pl`, so the wrapper's
  declaration line is honestly uncovered.

Of the smoke's five named examples, four (`fc_dtoi4`, `fc_ftod`,
`fc_btfloat8cmp`, `fc_float48div`) are indeed now covered; the fifth
(`fc_float8pl`) is release-gate tier and correctly is not. The direction
and mechanism of the defect were right; only its magnitude was overstated.

## Blocker 2 — runner fails closed on harness-name mismatch

Three guards, verified below on the original defect (the `bool` family's
harnesses live in `mod harnesses`, so a naive `proofs::` prefix makes them
all exit rc=1 with no coverage):

1. `run-kani-coverage.sh` preflights every harness name against
   `cargo kani list` for its family *before* spending a solve. The `-Z`
   tokens are the **union over the family's SUITE rows** (bool carries
   `-Z stubbing` on some rows only; without it the crate does not compile
   and the listing fails — that looked exactly like a name defect on the
   first cut of this script). A listing that is not a well-formed kani
   table degrades to `PREFLIGHT-SKIPPED`, never to "no harnesses": a
   preflight that rejects everything is as useless as one that accepts
   everything.
2. Postcondition per run: at least one NEW kaniraw whose mangled filename
   ends in that harness's length-prefixed terminal segment. rc=0 with no
   artifact is recorded `FAILED-TO-RUN`, never zero coverage.
3. `merge-coverage.py --census` refuses to write `summary.json` if any
   expected harness produced no coverage, unless waived by name with a
   reason in `--allow-unmeasured`. `--census` is mandatory (exit 2
   without it). Walls need waivers too — a wall is unmeasured coverage.

### Proof: the deliberate mismatch

    $ run-kani-coverage.sh --joblist joblist-bool-bad.tsv --census census-bool-bad.tsv
    PREFLIGHT FAIL bool/eq_bool_accum: 'proofs::eq_bool_accum' is not a harness
      in this crate. Candidates: harnesses::eq_bool_accum harnesses::eq_bool_accum_inv
    PREFLIGHT FAIL bool/eq_bool_accum_inv: ... Candidates: harnesses::eq_bool_accum_inv
    PREFLIGHT FAIL bool/eq_bool_alltrue:   ... Candidates: harnesses::eq_bool_alltrue
    PREFLIGHT FAIL bool/eq_bool_anytrue:   ... Candidates: harnesses::eq_bool_anytrue

    == run-kani-coverage census ==
      jobs considered:            4
      ran (kaniraw produced):     0
      walled (timeout 300s):      0
      failed to run (UNMEASURED): 4
      no SUITE row:               0

    INCOMPLETE CAPTURE: 4 of 4 jobs produced no coverage. Those are UNMEASURED,
      not uncovered. merge-coverage.py will refuse a summary unless each is
      waived by name in --allow-unmeasured.
    exit 1

Caught in 8 seconds, before any solve, with the correct name suggested.

    $ merge-coverage.py --kani-glob '…' --census census-bool-bad.tsv
    merge-coverage: REFUSING to write a summary — 4 census error(s). Unmeasured
    harnesses would be reported as uncovered code, which is a silent, confident
    undercount. No summary.json, no percentages.

    == coverage census ==
      expected harnesses (census rows): 4
      ran (kaniraw produced):           0
      walled (timeout/OOM):             0
      failed to run (UNMEASURED):       4
      waived with a stated reason:      0
      kaniraw files merged:             144
      kaniraw with no census row:       140
        ERROR  bool/eq_bool_accum rc=- NAME-UNRESOLVED did-you-mean:harnesses::eq_bool_accum
        …
    exit 3     # census.json written, summary.json NOT written

### Positive control (a guard that only ever fails is not a guard)

Same four harnesses with the correct `harnesses::` qualification:

    == run-kani-coverage census ==
      jobs considered:            4
      ran (kaniraw produced):     4
      failed to run (UNMEASURED): 0
      all 4 jobs measured.
    exit 0

and the full 140-harness float capture runs 140/140 `RAN` with a closed
census (`census140.tsv`, `summary.json.census.census_closed = true`).

## PoC VERDICT (2026-07-31 re-verification): the fixed instrument works — YES

Scope per coordinator: fix-verification on the float-OWN-family captures
(casts 22 + float-arith 13 + float-cmp 32 + hash-rows 30 = 97 harnesses,
97/97 RAN, census CLOSED), not full-number reproduction. geo-cmp also
completed (26/26 RAN) before the stop and is committed as
`census-own97-geo26-partial.tsv`; numeric-probe was never started.
Artifacts: `proofs/coverage/instrument-fix/{census-own97.tsv,out-own97-raw,out-own97}`.

| float per-file (own-family 97 set) | smoke baseline | raw re-merge | attribution ON |
|------------------------------------|----------------|--------------|----------------|
| builtins.rs                        | 49             | 49           | **103 (+54)**  |
| funcs.rs                           | 71             | 71           | 71             |
| lib.rs                             | (125 own-set)  | 125          | 125            |
| io.rs (zero-pinned falsifier)      | 0              | 0            | 0              |
| aggregates.rs (zero-pinned)        | 0              | 0            | 0              |
| **crate total**                    | **245**        | **245**      | **299 (13.50%)** |

- Raw merge reproduces the smoke's own-family 245 **exactly**, per file
  (builtins/funcs/io/aggregates identical; the smoke's lib.rs 164 was the
  196-set number — the own-set 125 shows the cross-family +39 all lands in
  lib.rs, consistent with §"provenance correction").
- The ONLY delta under attribution is builtins.rs `fc*!` invocation lines:
  +54 lines, 98/98 macro-body regions attributed, **0 unresolved** in this
  scope. Nothing else moved — validation criterion met, no discrepancy.
- Spot-reads (5 of the 54): line 160 `fc_dtof` <- casts `eq_dtof` RAN;
  161 `fc_dtoi4` <- `eq_dtoi4`; 144 `fc_float8um` <- float-arith
  `eq_float8um`; 201-region `fc_float4eq` <- float-cmp `eq_float4eq`;
  232 `fc_float4pl` <- `eq_float4pl`; and the div wrappers (235/239/243/
  247) via the `eq_*div_zero` error-plane harnesses.
- **No over-credit witness:** lines 236-238 (`fc_float8pl/mi/mul`, whose
  only harnesses are release-gate tier and were not run) remained
  UNCOVERED while their same-macro-block neighbors flipped.
- Fail-closed census on the same data: 97 expected / 97 ran / 0 walled /
  0 failed-to-run, `census_closed: true`; the deliberate-mismatch bool run
  (this dir: `census-bool-bad.tsv`, `merge-bool-bad.err`) is refused with
  exit 3, census.json only, no summary.json.

The earlier full-set numbers in this document (284 -> 338 over 140
harnesses incl. geo-cmp + numeric-probe) were measured before the
scratchpad sweep destroyed their censuses; they are consistent with this
PoC but their census files are not preserved. The full number is
reproduced for free inside the eventual full-tree run.

## A third distortion, found after the smoke doc was written

Fleet-solved families are entirely absent from local captures: a family
whose solves happened on the fleet leaves no kaniraw here, so its
functions read 0% — `proofs/float-agg` vs `float/src/aggregates.rs`
(0/225) is the measured case. Documented as known distortion 7 in
COVERAGE.md, with the capture rule (joblist derived from SUITE.tsv; fleet
leg or explicit `--allow-unmeasured` waivers). float-agg's 31 harnesses
are registered `expected=unmeasured` in SUITE.tsv on main (dark-harness
sweep, `50b4e42892`); verified lint-clean with `lint-suite-rows.py`.

## What is NOT fixed here

- The other two smoke recommendations are documented in COVERAGE.md but
  not enforced by code: explicit family lists per capture (rule: record
  the joblist), and per-family timeouts (300 s string-heavy / 900 s else)
  which are a runner argument, not a default.
- Scope-wide re-measurement. Only `adt_float` was re-run. The other six
  scope crates' Kani numbers in COVERAGE.md remain pre-fix and biased low;
  the table now says so.
