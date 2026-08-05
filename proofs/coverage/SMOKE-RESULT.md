# Full-tier Kani coverage smoke test — RESULT

Companion to `SMOKE-PREDICTION.md`, which was written and committed
(`2cacd3e273`) before a single harness ran. Question under test: **does
running the COMPLETE fast tier for one crate produce a coverage number we
believe?**

Method: unchanged from `proofs/COVERAGE.md` §"How to regenerate" step 1 —
each harness's exact SUITE.tsv flags plus `--coverage -Z source-coverage`,
one harness per invocation, merged with `merge-coverage.py` scoped to the
one crate. Runner: `proofs/coverage/run-smoke.sh` (a 3-column joblist
variant of `run-kani-coverage.sh`; the third column overrides harness-name
qualification). Ledger: `proofs/coverage/smoke-runs.log` (196 rows).
Kani 0.67.0, cargo 1.96.0, macOS aarch64, `--solver kissat` per SUITE.

## Crate: `adt_float`. Harness count: 45 sampled -> 97 own-family -> 196 run.

## 0. The enumeration overturned the task's premise

The lane was chartered on the theory that each of the 7 scope crates had
one family sampled while many other fast-tier families also exercise it.
Enumerating the per-commit tier by *direct* dependency + call site:

| crate   | prior run | full per-commit set | gap  |
|---------|-----------|---------------------|------|
| network | 15        | 15 (spgist-inet has no SUITE rows) | **+0** |
| numeric | 17        | 17                  | **+0** |
| geo     | 26        | 26                  | **+0** |
| varbit  | 27        | 30                  | +3   |
| jsonb   | 8 (release-gate) | 0 per-commit tier exists | n/a |
| float   | 45        | 97                  | **+52** |
| varlena | 54        | 193                 | **+139** |

So the two crates the task nominated as the small candidates (varbit,
network) had **no meaningful gap to measure**; network and numeric had
none at all. Only float and varlena do. float was chosen: the largest
*affordable* gap, 2,215 SLOC (hand-auditable), and the two added families
(`casts`, `hash-rows`) aim at the exact region the prior capture named as
float's biggest dark block.

## 1. THE NUMBER

| set                                              | harnesses | kani SLOC | %      |
|--------------------------------------------------|-----------|-----------|--------|
| float-cmp + float-arith only                     | 45        | 155       | 7.00%  |
| **prior published capture (reproduced exactly)** | 88        | **197**   | **8.89%** |
| + casts + hash-rows (own-family full set)        | 97        | 245       | 11.06% |
| **FULL, all families that reach float**          | **196**   | **284**   | **12.82%** |
| full + macro-attribution correction (see §3)     | 196       | 356       | 16.07% |

Per file (full 196-harness set):

| file          | sloc | prior | predicted   | measured | verdict |
|---------------|------|-------|-------------|----------|---------|
| builtins.rs   | 490  | 11    | 40-90       | **49**   | in band |
| funcs.rs      | 663  | 30    | 45-75       | **71**   | in band |
| lib.rs        | 341  | 156   | 156-175     | **164**  | in band |
| io.rs         | 496  | 0     | **exactly 0** | **0**  | falsifier held |
| aggregates.rs | 225  | 0     | **exactly 0** | **0**  | falsifier held |

**Prediction scorecard.** Point estimate 250 SLOC / 11.3%, 80% interval
220-300. Measured 245 for the 97-harness set (point estimate off by 5
lines) and 284 for the full 196 — both inside the interval. Both
zero-pinned falsifiers held exactly. Walls predicted 0-3 of 97; **0 of
196 walled.** Wall time predicted 45-100 min; actual **914 s (15 min)**
for all 196. The instrument behaved as understood, with the one exception
in §3 — which the prediction had explicitly named as the risk it was
shading the estimate down for.

## 2. Sanity check A — monotonicity, and a reconciliation the check forced

284 >= 197. PASS.

But the first sub-result was alarming and worth recording: merging only
`float-cmp` + `float-arith` gives **155**, not the published 197. Since
the prior capture's log lists exactly those two as float's families, that
looked like a 42-line non-reproducibility. It is not. The prior capture
merged with `--kani-glob 'proofs/*/target/kani/...'` — **every** family it
ran — and `adt_geo` and `adt_numeric` both carry `adt_float = { path }`
and genuinely compute in float8. Adding `geo-cmp` (26) and
`numeric-probe` (17) reproduces **197 exactly, to the line**. That is a
strong reproducibility result for the whole pipeline, and it corrects the
prior capture's own attribution: float's 197 was never an 88-harness
2-family number in the sense its log implies.

It also means **static enumeration cannot bound the harness set.**
Transitive crate reachability is useless as a filter — 30 of 30 per-commit
families "reach" `adt_float` through the adt crates' mutual dependencies.
Measured instead: `cash` (11 harnesses), `misc-ops` (5) and `bool` (20)
were run as a residual probe and contribute **exactly 0** float SLOC each,
despite all three being statically reachable. Only families whose crates
*compute in floating point* contribute. There is no sound static test for
that; only the instrument decides.

## 3. Sanity check B — ground truth by hand (the important one)

### Covered lines: 12 probed, 12 genuinely reachable, ZERO false positives

Each was checked by reading the harness and confirming the call path, and
independently by re-merging each family alone to see which family's
kaniraw actually carries the line:

| line              | attributed to | verified path |
|-------------------|---------------|---------------|
| builtins.rs:114   | casts         | `fc1t!` macro body; `casts::eq_dtoi4` calls `adt_float::builtins::fc_dtoi4`, an `fc1t!` instance |
| builtins.rs:299   | hash-rows     | `fc_hashfloat4` body; `eq_hashfloat4` |
| builtins.rs:289   | hash-rows     | `f64::NAN` arm of `float8_hash_image`; reachable because the harness input is `kani::any::<f32>()`, NaN in domain |
| funcs.rs:82       | casts         | `dtof` overflow test; `eq_dtof` over `kani::any::<f64>()` |
| funcs.rs:66       | casts         | `integer_out_of_range()` cold ctor; reached via `dtoi4`'s error arm, which `eq_dtoi4` asserts with `kani::cover!(cerr != 0)` |
| funcs.rs:96       | casts         | `dtoi2` body; `eq_dtoi2` |
| lib.rs:106        | casts         | `float4_fits_in_int32`, called by `ftoi4`; `eq_ftoi4` |
| lib.rs:77         | casts, geo-cmp| `float_underflow_error` body; `dtof` underflow arm (`cerr == 2` covered) — and geo-cmp reaches it too, confirming geo executes float error paths |
| lib.rs:322        | float-cmp     | `float4_eq`; `eq_float4eq` |
| funcs.rs:827      | float-arith   | `float48mul` cross-width |
| funcs.rs:709      | float-arith   | `dpi()`; float-arith's `eq_dpi` |
| lib.rs:210        | float-arith   | float8 arithmetic core |

### Uncovered lines: 5 of 6 categories correct, 1 systematically WRONG

Correct (verified no harness in the 196-set can reach them — checked by
grepping every entry-point symbol across all 9 families' sources):

- **io.rs, all 482 logic lines** — `float4in`/`float8in`/`float8out`.
  0 references in any of the 9 families. Correct-uncovered. (This is the
  *fuzz* axis: the prior capture's `float_in_diff`/`float_out_diff`
  targets covered 361 lines here.)
- **aggregates.rs, all 217 logic lines** — `float8_accum`/`float8_combine`.
  0 references; `float-agg` has zero per-commit rows. Correct.
- **builtins.rs:20-52** — `fc_float4recv`/`fc_float8send` etc. Correct
  *for the per-commit tier*: `float-arith` does have an `eq_float8send`
  harness, but it carries no per-commit SUITE row, so it is out of the
  measured tier. The uncovered mark is honest about what we ran.
- **funcs.rs:33-51** — `erf`/`erfc`/`tgamma`/`lgamma`. 0 references.
  Correct — genuine unproved logic.
- **builtins.rs:479-633** — `const FLOAT_BUILTINS: &[FmgrBuiltin]`, 154
  SLOC. Correct-but-dark: const-eval'd data has no runtime counters. Dark
  to all three instruments, as COVERAGE.md already states.

**WRONG — 72 false-uncovered lines.** `builtins.rs` declares its fmgr
wrappers through four macros (`fc1!`, `fc1t!`, `fc2!`, `fc2t!`) whose
invocation blocks (lines 138-251) are one-line-per-wrapper argument lists.
Kani attributes the generated function's regions to the **macro definition
body** (e.g. builtins.rs:113-115), never to the invocation line. Of 103
such declaration lines, **72 name a wrapper that a harness in this very
set directly proves** — `fc_float8pl`, `fc_dtoi4`, `fc_ftod`,
`fc_btfloat8cmp`, `fc_float48div`, … The tool marks every one uncovered.

That is a **3.25-percentage-point systematic undercount in this crate
alone** (72 / 2,215), and it is not float-specific: `fc*!`-style wrapper
macros are the house style for fmgr registration across the adt crates.
Any per-crate Kani percentage from this pipeline is low by roughly the
count of macro-declared entry points that are proved. Corrected float
number: **356 / 2,215 = 16.07%**, vs the 8.89% on the books.

## 4. Sanity check C — uncovered composition

1,931 uncovered SLOC of 2,215. A percentage without this is useless:

| category                                                   | SLOC | share |
|------------------------------------------------------------|------|-------|
| **E. genuine unproved logic**                              | 1,352| 70.0% |
| D. fmgr wrapper shims (`fc_*` bodies) no harness calls      | 170  |  8.8% |
| A. fmgr registration table (`const FLOAT_BUILTINS`)        | 154  |  8.0% |
| C. imports / attributes / const & type decls (no counters)  | 148  |  7.7% |
| B. macro invocation arg lines — **72 of 107 FALSE** (§3)    | 107  |  5.5% |

So for float, **the const-table story is NOT the dominant block** — the
prior capture's note that `*_BUILTINS` tables top the uncovered list is
true per-region but only 8.0% of uncovered SLOC here. The dominant block
is real logic: `io.rs` (482 SLOC of float parse/print), `aggregates.rs`
(217 SLOC of accumulators), and the transcendental/rounding tail of
`funcs.rs` (565). Of the 1,931 uncovered, only about 21% (A+B+C = 409) is
instrument-dark or wrong; **~79% is honest "no proof reaches here"**, and
of that, 852 SLOC (io.rs + aggregates.rs) is *whole subsystems with no
per-commit proof family at all* rather than dark branches inside proved
functions.

## 5. Cost, walls, disk — measured

- **196 harnesses, 914 s total wall (15 min), mean 4.7 s/harness**, on a
  loaded laptop, including one cargo build per family.
- **0 of 196 walled.** Prior capture: 14 of 165 (8.5%), all in
  string/jsonb-heavy families (text-slice 9/14, jsonb 3/8). Coverage
  instrumentation is corrosive to *string* harnesses specifically, not to
  harnesses generally.
- **Inflation factor 2.0x wall-vs-plain-baseline** (914 s measured against
  453.6 s of SUITE baseline solve time), and that 2.0x *includes* build
  time — true solve inflation for scalar families is well under 2x, not
  the 3-10x COVERAGE.md extrapolated from the string families.
- Disk: 9 family target dirs, ~600-800 MB each, ~5.5 GB total — well under
  the 2-3 GB-per-family figure in COVERAGE.md. 161 GB free at finish. Not
  a constraint.

## 6. Extrapolation to the full tree

Full per-commit tier: 878 SUITE rows, ~862 harnesses, ~40 families,
baseline solve sum **2,059 s (0.57 h)** over the 834 rows carrying times.

- At the measured 2.0x factor: **~70 min of solve+build** for the scalar
  bulk.
- Plus the wall class. The string/jsonb families are the whole risk:
  text-slice alone burned 9 timeouts last capture. At a 900 s cap, ~15-25
  expected walls across the tier = **4-6 h of pure timeout** unless the
  cap is cut. **Recommend a 300 s cap for the string-heavy families** —
  the prior capture's walls were 75-560 s, so 300 s loses little and
  bounds that tail to ~2 h.
- Realistic total: **2.5-4 h serial on this laptop**, or well under an
  hour with fleet parallelism by family. This is *cheaper* than
  COVERAGE.md's 8-16 machine-hour estimate, which assumed the 3-10x
  inflation applied uniformly. The smoke test refutes that.

**Expected finding.** Scope-wide Kani would move from 6.0% to roughly
8-10% before correction: float +87 SLOC measured, varlena is the only
other crate with a large harness gap (54 -> 193) and is the swing factor;
network, numeric, geo and varbit cannot move at all because they were
already at their full per-commit sets. Add the §3 macro correction — which
applies to every crate's `fc*!` wrapper block — and the honest scope-wide
number is plausibly 9-12%. **It stays single-to-low-double digits. The
6.0% was not mostly a sampling artifact.**

## 7. Recommendation

**Run the full tree, but fix two things first.** The number is worth
having and the cost is a few hours, not a day. But shipping it as-is
would ship a known-wrong instrument:

1. **BLOCKER — fix the macro-attribution undercount.** 72 of 103
   declaration lines wrong in one crate, and `fc*!` wrapper macros are the
   house style tree-wide. Cheapest honest fix: have `merge-coverage.py`
   credit a macro invocation line when the function it declares has any
   COVERED region attributed to the macro's definition body. Failing that,
   the `fc*!` invocation blocks must be *excluded from the denominator*
   like `#[cfg(test)]` items are, and COVERAGE.md must say so. Do not
   publish per-crate percentages until one of these lands.
2. **BLOCKER — make the runner fail closed on harness-name mismatch.**
   The `bool` family's harnesses live in `mod harnesses`, not
   `mod proofs`; a naive `proofs::` prefix made all 20 exit rc=1 with
   `error: Failed to match the following harness(es)`. They produced no
   coverage and, in a runner that only merged globs at the end, would have
   silently read as 20 harnesses' worth of uncovered code. This is the
   gate-blindness class (fail-open, vacuous pass). The full run needs a
   post-condition asserting one kaniraw per attempted harness, and any
   `NO-VERDICT` row must abort the capture rather than be merged.

Also change, less urgently:
3. Drop `--kani-glob 'proofs/*/...'` in favour of an explicit family list
   per capture, and record it. The prior capture's float number was
   correct but its own log misattributes which families produced it.
4. Per-family timeout, not a global one: 300 s for string/jsonb families,
   900 s elsewhere.
5. Publish the uncovered-composition breakdown (§4) next to every
   percentage. For float, "8.89%" and "70% of the dark is two whole
   unproved subsystems, 8% is a const table" are different facts, and only
   the second is actionable.
