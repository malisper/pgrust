# SLOC rule v2 — the coverage denominator, aligned with what real coverage tools count

Status: **ADOPTED — Michael ruled 2026-07-30: rule v2 + data tables
EXCLUDED** (the 84.50% / 18,544-SLOC cell of §5). `--sloc-rule v2` and
`--exclude-const-tables` are now the DEFAULTS in `merge-coverage.py`,
`recut-sloc.py` and `tree-sloc.py`; the viewer's real path renders whatever
re-cut it is pointed at. v1 / `--include-const-tables` remain available
behind flags for comparability with pre-ruling numbers. Every excluded
table span is published in `excluded-tables.json` (§5b) — the
reviewability condition attached to the ruling.

Adopted headline (7-crate capture, head `41ef1dd381`): **SLOC 18,544 — any
84.50%, kani 6.98%, fuzz 2.65%, regress 83.22%**. Tree-wide denominator:
**640,701** (= 779,276 v2 − 138,575 table lines; was 883,248 under v1).

The trigger: `} else {` contains the keyword `else`, so rule v1 counts it as
SLOC — but (in the shapes our instruments can't map, below) no instrument ever
credits it, so it reads **permanently uncovered** in the viewer. Michael saw
one and asked us to mimic what established coverage tools do.

## 1. How established tools define the line denominator

All citations verified 2026-07-30.

| tool | denominator = | `} else {` / structural lines | file with NO data at all |
|---|---|---|---|
| **llvm-cov** (source-based) | lines where `LineCoverageStats.Mapped` — a counted segment wraps the line or a region starts on it; LCOV export emits `DA:` exactly for `isMapped()` lines, `LF` = their count [1][2] | C/C++: clang emits a **gap region** between then-block and `else` ("The 'else' count applies to the area immediately after the 'then'") [3]; a gap's count is used as the line count **only if no other region is on the line** [1][4] — the line stays IN the denominator | not instrumented ⇒ absent from the report entirely ("Uninstrumented code simply won't be accounted for in reports" [5]); instrumented-but-never-executed functions DO appear at 0 (mapping lives in the binary) [4] |
| **rustc / cargo-llvm-cov** | same llvm-cov line logic, but rustc emits **no gap regions at all** — `MappingKind` is only `Code`/`Branch` [6]; which lines are mapped is decided by MIR span refinement (discards spans that fill the whole body, merges/truncates overlaps) [7] | no dedicated region for `else`/`loop`/`unsafe` keywords; a structural line gets a `DA:` only if a refined Code-region span happens to touch it (measured below) | same as llvm-cov |
| **gcov / lcov / genhtml** | lines the compiler assigned to basic blocks in `.gcno` [8]; `DA:` only for "a line which resulted in executable code", `LF` = "number of instrumented lines" [9] | no-code lines print `-` ("not instrumented") vs `#####` (instrumented, unexecuted) [10] — bare `}`/`else` gets `-`, out of denominator | absent from the `.info` file and the report; the documented fix is the `lcov --initial` zero baseline, whose man page admits the percentage is otherwise wrong when files never load [11] |
| **coverage.py** | the .pyc **bytecode line table** minus docstrings ("compiled Python files have a table of line numbers… Coverage.py reads this table to get the set of executable lines" [12]) | `else:`/`finally:` compile to no bytecode ⇒ never statements, never in the denominator (parser walks only clause *bodies*) [13]; `def`/`class` lines DO count [14] | absent by default; with `source=` set, un-imported files are searched out and reported at 0% [15] |
| **Istanbul / nyc** | AST `Statement` nodes (`statementMap`); line coverage is reverse-derived from statements' start lines [16] | a `} else {` line has no statement start ⇒ not a coverable line | absent unless `--all` statically instruments never-loaded files to 0% [17] |
| **JaCoCo** | "a source line is considered executed when at least one instruction that is assigned to this line has been executed"; needs debug line tables [18] | `} else {` emits no instruction of its own ⇒ not a line | classes in the report scope without execution data show 0% [19] |
| **tarpaulin** | debug-info/LLVM coverable lines **corrected by its own Rust source analysis** — explicitly because C-oriented tools "cause language constructs which aren't actually executable code to be mistakenly included as misses" [20] | filtered by source analysis | adds back unused generics that emit no assembly, via source analysis [20] |
| **grcov** | pure aggregator: whatever DA/region records the profiles contain [21] | inherits producer | absent (no include-untested option; subtractive filters only) [21] |

Two shared facts, and one shared blind spot:

1. **Every tool's denominator is "lines the toolchain emitted code for"** —
   compiler line table (gcov, JaCoCo), bytecode (coverage.py), AST statements
   (Istanbul), llvm mapped regions (llvm-cov, rustc). Structural glue is out.
2. **The omission blind spot is universal**: a file that never entered the
   toolchain silently vanishes from every tool's total, and every ecosystem
   grew an explicit opt-in to fix it (`lcov --initial`, `source=`, nyc
   `--all`, JaCoCo report scope). Our text-SLOC universe is exactly that
   opt-in, made mandatory — this is why v2 keeps text SLOC as the universe
   rather than adopting instrument line tables as the universe.

## 2. What rustc 1.96 actually maps (measured, not taste)

Toy binary compiled `-C instrument-coverage` with the pinned 1.96.0, exported
through its own llvm-cov (the exact server-capture toolchain). Per line:

| construct | DA record? | note |
|---|---|---|
| statement-position `} else {` | **yes** — with the **then**-branch's count | the region of the *then* block ends on this line; the count is the wrong branch's — noise, not an else signal |
| statement-position bare `else {` (brace on prev line) | yes — else count | span-edge accident of formatting |
| expression-position `} else {` (if-else as tail value) | **no** | |
| `unsafe {` alone | **no** | block has no runtime semantics |
| `loop {` | **no** | count lands on body/condition lines |
| `_ => {` / bodiless `1 =>` arm head | **no** | arm count lands on the body lines |
| arm with guard `n if n > 50 => {` | yes — scrutinee count | guard is evaluated |
| arm with body on same line `0 => 100,` | yes | |
| `break;` / `continue;` / `return;` | yes — correct counts | real statements |
| `)?;` (lone, closing a multi-line call) | yes — the `?` error-branch count | `?` is a Branch region |
| `|| {` closure head / `.map(|i| {` | yes | closure's function span starts there |
| fn signature lines | yes | function span starts at the header |

## 2b. Declarations, asserts, thread_local, macro scaffolding (measured, round 2)

Same rig (rustc 1.96.0 + its llvm-cov, `-O`; asserts additionally measured
under both `-C debug-assertions=on` and `=off`):

| shape | DA record? | verdict |
|---|---|---|
| `use x::y;`, `pub use`, multi-line `use x::{` blocks (incl. interior name lines) | no | exclude (`use-mod`) |
| `mod foo;`, `mod foo {` header | no | exclude (`use-mod`) |
| `extern crate …;` | no | exclude (`use-mod`) |
| `#[attr]`, `#![attr]`, `#[derive(…)]` (incl. multi-line) | no | exclude (`attr`) |
| `type A = B;` (top-level, associated, or fn-local) | no | exclude (`type-alias`) |
| single-line `const X: T = …;` / `static X: T = …;`, **top-level or fn-local**, incl. non-trivial const-eval initializers (`i32::MAX / 2`, `u32::MAX.count_ones() + (1 << 4)`) | no | exclude (`const-static`) |
| `static F: fn(i32)->i32 = \|x\| x * 2;` and `LazyLock::new(\|\| …)` initializer lines | **yes** — the closure is a function whose span sits there | **KEEP** (the `\|` guard) |
| struct/enum/union header + field/variant lines, incl. explicit discriminants (`A = 1 << 4,`) | no — **even with `#[derive(Clone, Debug, PartialEq)]` exercised**: rustc does not instrument derived impls at all (zero FN records exist for them) | exclude (`typedef`) |
| `impl Foo {` / `impl Trait for Foo {` headers, incl. multi-line headers and their `where` lines | no | exclude (`impl-header`) |
| `trait Foo {` header; body-less `fn …;` (trait requirements, extern blocks) | no | exclude (`trait-header`, `fn-decl`) |
| `extern "C" {` header | no | exclude (`extern-block`) |
| fn signature lines, their `where`/generic continuation lines, fn parameter lines of multi-line signatures | **yes** (function span starts at the signature) | **KEEP** |
| `assert!` / `assert_eq!` | yes | KEEP (live in all profiles) |
| `debug_assert!` / `debug_assert_eq!` / `debug_assert_ne!` | **yes under BOTH `debug-assertions=on` and `=off`** (the `if cfg!` skeleton keeps the span mapped) | **KEEP** — see asymmetry note below |
| `thread_local! { … }` — the whole block, incl. `const { … }` initializers AND non-const lazy initializer bodies | no — measured: the runtime init body executed and still got no region ($init expands inside std's macro; refinement drops the spans) | exclude (`thread-local`) |
| `macro_rules! name {` header, matcher arm heads (`($(…)*) => {$(`), repetition tails (`)*};`, `$(`) | no | exclude (`macro-scaffold`) |
| macro TEMPLATE BODY lines inside the arms | **yes** (DA=2 for a twice-invoked generator; same lines Kani credits — the macro_attrib.py mechanism) | **KEEP** |
| generator-macro invocation headers (`fc1! {`) | no | exclude (`macro-scaffold`) |
| macro invocation DECLARATION lines (`fc_dtoi4: dtoi4(as_f64) -> from_i32;`) | no llvm DA, but **Kani credits them via macro attribution** — the honest proved/unproved signal | **KEEP** (load-bearing) |
| bodiless no-guard match-arm heads (`None => {`, `Some(tok) => {`, `1 =>`) | **inconsistent** — mapped with the arm count in statement-position matches, unmapped in expression-position ones; measured on the capture: 75 covered vs **127 red-head-over-covered-body** sandwiches | exclude (`arm-head`) — the body lines always carry the arm's signal; a head count, when present, only duplicates it |
| arm heads with a guard (`n if n > 50 => {`) or an inline body (`0 => 100,`, `_ => panic!(…),`) | yes | KEEP |

**Asserts / the debug-assert asymmetry.** No assert shape is excluded. The
capture profiles differ — regress server = `fast-profile` (inherits
`release`, debug-assertions OFF), fuzz = cargo-fuzz (debug-assertions ON by
default), Kani (checks `debug_assert!` as proof properties) — but the line
itself stays *mapped* even where the assertion compiles out, so it is
coverable on every axis and stays in the denominator. Note the flip side for
readers of the viewer (cf. the debug-assert masking law, 2026-07-24): a
GREEN `debug_assert!` line under regress means the enclosing code ran, NOT
that the assertion was armed — release profiles delete the check while
coverage still credits the line.

And on the real 7-crate capture (v1, head `41ef1dd381`, baseline any = 72.1%):

| class | lines in scope | ever covered by anything | of which kani/fuzz/regress |
|---|---|---|---|
| else-only | 449 | 88 (19.6%) | 17 / 1 / 73 |
| loop-only | 66 | **0** | 0 / 0 / 0 |
| unsafe-only | 43 | 13 | 2 / 0 / 12 |
| arrow-only | 20 | 5 | 0 / 0 / 5 |
| use-mod | 524 | 1 | 1 / 0 / 0 |
| attr | 517 | 2 | 2 / 0 / 0 |
| typedef | 390 | **0** | 0 / 0 / 0 |
| const-static | 163 | 1 | 1 / 0 / 0 |
| impl-header | 58 | **0** | 0 / 0 / 0 |
| fn-decl / trait-header / extern-block / type-alias | 14 | **0** | 0 / 0 / 0 |

A class sitting at 0–20% covered against a 72% baseline is not dark code —
it is unmappable syntax. The nonzero counts are span-edge spillover
(then-count on `} else {`, multi-line Kani regions), i.e. *another line's*
count leaking onto the structural line. The typedef 0/390 also settles the
derive question empirically: Kani does not in practice credit derive
coverage to definition lines in this tree.

## 2c. Test code is identified STRUCTURALLY (fix of 2026-07-31)

Both rules exclude test code from the universe. That is a scope decision, not
a mappability one, and it was the least sound part of the implementation: until
2026-07-31 "is this test code?" was answered by FILENAME (`^tests.*\.rs$`, or
`/tests/` in the path) plus a scan that, on seeing `#[cfg(test)]`, searched
forward for the next line containing `{` and excluded to its matching close.

Four defect classes, erring in BOTH directions (all measured at `70e8ab3911`):

| # | defect | direction | measured effect |
|---|---|---|---|
| D1 | semicolon file-modules (`#[cfg(test)] mod state_tests;`) were not recognised as test code | test lines counted as production | 11 files, 2,251 tree lines — incl. `multirangetypes/corpus_tests.rs` 444, `rowtypes/ws_tests.rs` 187 |
| D2 | the brace search walked past a braceless item into the FOLLOWING production item | production lines silently dropped | 82 files, 1,465 tree lines — worst `nodes/src/tags.rs` 480 (the whole tag table), `adt/pg_lsn` 11 (a complete parser function) |
| D2b | when no later `{` existed the scan `break`ed, abandoning every remaining span in the file | test lines counted as production | folded into D2's file set |
| D4 | only the literal `#[cfg(test)]` was matched, so `#[cfg(all(test, …))]` items were missed whole | test lines counted as production | e.g. `aio_uring/src/lib.rs` 490, `runtime/src/sink.rs` 375 |

D1 is the one that shaped the source: a lane RENAMED files to `tests_*.rs` to
satisfy the tool. A measurement tool must never do that.

The rule is now structural — `proofs/coverage/test_scope.py`, the crate module
graph plus a real Rust tokenizer (nested block comments, raw strings of any
hash count, byte/c-string prefixes, lifetime-vs-char). A file is test code iff
something declares it under a test-only `cfg` predicate, it carries
`#![cfg(test)]`, a path component is `tests`/`benches`, it holds only
`#[test]`/`#[kani::proof]` items, or it is unreachable from the crate's entry
points AND holds test items. Filenames are never consulted; `tests.rs` is
excluded because of its declaration, not its name. `cfg` predicates are
evaluated (`all(test, X)` is test-only; `any(test, X)`, `not(test)` are not),
and every undecidable case is a LOUD diagnostic that keeps the lines IN scope
(`--strict-test-scope` makes them fatal). Regression tests for each class:
`proofs/coverage/test_sloc_denominator.py`.

Whole-tree effect: 643,362 → 640,640 SLOC (−2,722, 104 files). Phase-1
in-scope: −659 over 14 of 122 candidate crates. Per-crate table:
`docs/verification/sloc-rebaseline-2026-07-31.tsv`; the accounting residuals it
opened for finished lanes: `proofs/coverage/sloc-rebaseline-residuals.tsv`.

## 3. Rule v2

**Universe (unchanged from v1):** all v1 text-SLOC lines. Files, crates and
the tree keep their v1 universe so never-instrumented code stays visible — a
crate with no harnesses can never read 100%. This deliberately diverges from
llvm-cov/lcov/coverage.py defaults, which silently drop no-data files (the
blind spot in §1); it is our equivalent of `lcov --initial` / nyc `--all`.

**Denominator: universe minus lines the instruments cannot meaningfully
map**, in two groups (every shape's verdict measured in §2/§2b, never
assumed):

Pure control-flow syntax:

| class | shapes | rationale |
|---|---|---|
| `else-only` | `} else {`, `else {`, `else`, `} else` | rustc emits no region for the keyword; where a DA appears it carries the *then* branch's count (measured, §2) — a non-signal. Every AST/bytecode tool excludes it (coverage.py, JaCoCo, Istanbul, gcov's `-`). |
| `loop-only` | `loop {` | no span; 0/66 ever covered on the real capture. The loop's execution is fully visible on its body/condition lines. |
| `unsafe-only` | `unsafe {` | lexical marker, no runtime semantics; unmapped in §2. Contents carry the coverage. |
| `arrow-only` | `_ => {`, `() => {`, `) => {`, `} => {`, lone `=>`, `() => {{` | punctuation/wildcard-only arm heads: no pattern content, no guard, no body; unmapped in §2. |
| `arm-head` | any bodiless, guard-less match-arm head: `None => {`, `Some(tok) => {`, `Pattern =>` | llvm's mapping of these is position-dependent (§2b): kept, they produce red-head-over-covered-body artifacts (127 measured on the capture); the arm's body lines always carry its execution signal, and a head count merely duplicates it. Guarded heads and inline-body arms stay. |

Declaration lines (rustc emits no instrumented code for the item syntax —
including `#[derive]`d impls, which get no coverage mapping at all):

| class | shapes |
|---|---|
| `use-mod` | `use …;` incl. multi-line blocks, `mod x;`, `mod x {` headers, `extern crate …;` |
| `attr` | `#[…]` / `#![…]` lines, incl. multi-line attributes |
| `type-alias` | `type A = …;` at any nesting depth |
| `const-static` | single-line `const`/`static` items at any nesting depth (incl. fn-local, e.g. float `io.rs` `const MAX_MANT_BITS: u32 = 120;`) — EXCEPT lines containing a closure, which are mapped and stay. Multi-line const/static heads stay (omission signal); interiors belong to the separate const-table knob. |
| `typedef` | struct/enum/union header + field/variant lines |
| `impl-header` | `impl …` header lines through the opening `{`, incl. their `where` lines |
| `trait-header` | `trait … {` header lines |
| `fn-decl` | body-less `fn …;` (trait requirements, extern blocks) |
| `extern-block` | `extern "…" {` headers |
| `thread-local` | entire `thread_local! { … }` blocks (measured: even non-const lazy initializer bodies get no region) |
| `macro-scaffold` | `macro_rules!` headers, matcher arm heads, repetition tails, and bare `name! {` invocation headers |

**Explicit keeps** (the over-exclusion guards; all verified mapped in §2/§2b
or carrying an evaluated expression): `} else if cond {`, `) else {` /
`}) else {` (multi-line let-else initializer close), `)? else {` / `)?;` /
`)?` (the `?` operator is a Branch region), guarded arm heads and arms with
inline bodies, `break;`/`continue;`/`return;`, `|| {` closure heads and
const/static initializers containing closures, fn signature lines + their
parameter/where/generic continuation lines, `assert!*` AND `debug_assert!*`
lines (§2b asymmetry note), macro_rules! TEMPLATE BODY lines, macro
invocation DECLARATION lines (`fc_dtoi4: …;` — the macro-attribution
targets), `&& {`, `match x {`, `while cond {`, `let` bindings (fn-local
`const`/`static`/`type` are items and leave; `let` is code and stays).

**Line-table precedence (deterministic).** Where instrument line-table data
exists for a file, the instrument beats the text heuristic: a structural-
candidate line is **reinstated** into the denominator iff a full lcov export
(DA records *including count 0* — the raw capture artifact, not the
covered-lines subset) has a DA record for that (file, line). DA presence is
llvm's own per-line mappability verdict. Kani regions do NOT reinstate: under
this pipeline a Kani region is a multi-line span, and intersection is span
spillover, not per-line evidence. Where no line table mentions a file, the
text classification is final. The denominator is therefore a pure function of
(source text at head_sha, DA line sets of the artifacts passed) — the same
inputs give the same denominator, with no dependence on what executed.

Pass the tables with `merge-coverage.py --line-table-lcov regress.lcov …` or
`recut-sloc.py --line-table-lcov …`. The 2026-07-30 7-crate capture's lcov
files were session artifacts and are gone, so its re-cut below runs on the
text fallback — the full-tree capture should preserve its lcov files and
re-cut with them.

**Post-processing contract.** `recut-sloc.py <indir> <outdir> --sloc-rule v2`
re-cuts any existing merge output without re-running any instrument: covered
sets are taken from `files/<slug>.json` as-is, sources are read at the
capture's own `head_sha` via `git show`, the v1 universe is recomputed and
verified line-for-line against the captured `sloc` arrays (hard-fails on
mismatch), and only the denominator (plus the intersection of covered sets
with it) changes. The running full-tree v1 capture can adopt v2 afterwards
for free. Raw kaniraw/lcov artifacts are never touched.

**Not part of v2 — the separate `const-table` knob.**
`--exclude-const-tables` drops interior lines of multi-line `const`/`static`
bracket initializers (registration tables, unicode/encoding maps). They are
const-evaluated data with no runtime counters — inherently dark to all three
instruments — but they are also the omission signal for unregistered entry
points, so excluding them is a policy call, presented to Michael below, not
smuggled into v2. The declaration head line always stays.

## 4. Measured deltas

### 7-crate dataset (capture of 2026-07-30, head `41ef1dd381`), re-cut under v2

Total SLOC 21,986 → **19,325** (−2,661; −12.1%). Reclassified lines by class
(previously-red in parens): else-only 449 (361), use-mod 522 (521), attr 515
(513), typedef 390 (390), arm-head 223 (148), macro-scaffold 184 (183),
const-static 156 (155), loop-only 66 (66), impl-header 58 (58), unsafe-only
43 (30), thread-local 21 (21), arrow-only 20 (15), fn-decl 7 (7), type-alias
5 (5), trait-header 1 (1), extern-block 1 (1). Total **2,661 removed, 2,475
of them previously red**; the 186 previously-covered removals are span-edge
spillover and duplicated arm-head counts, removed from numerator and
denominator alike.

| crate | SLOC v1→v2 | any% v1→v2 | kani v1→v2 |
|---|---|---|---|
| float | 2,215 → 1,946 | 63.25 → 71.63 | 197 → 197 |
| geo | 2,819 → 2,553 | 69.81 → 76.77 | 192 → 191 |
| jsonb | 5,875 → 5,037 | 75.97 → 87.35 | 118 → 115 |
| network | 1,518 → 1,373 | 68.38 → 75.24 | 136 → 135 |
| numeric | 5,713 → 4,979 | 72.47 → 81.56 | 318 → 301 |
| varbit | 866 → 801 | 75.75 → 81.15 | 182 → 179 |
| varlena | 2,980 → 2,636 | 73.49 → 82.40 | 178 → 177 |
| **all** | **21,986 → 19,325** | **72.12 → 81.09** | **1,321 → 1,295 (6.01% → 6.70%)** |

(the small kani-numerator drops are multi-line Kani-region spillover onto
excluded lines — exactly the class the rule removes from both sides.)

### The float headline (v1-with-macro-fix 338/2,215 = 15.26%)

float has 269 v2-excluded lines, so the v2 denominator is **1,946**.

- The intact, census-closed own-family capture (97 harnesses, at
  `d824ba3fe9`, `instrument-fix/out-own97`) re-cuts exactly:
  **299/2,215 = 13.50% → 298/1,946 = 15.31%** (one kani-covered else line
  left numerator+denominator).
- The 140-harness 338 row cannot be re-derived exactly: its kaniraw/census
  were destroyed (COVERAGE.md), and the surviving
  `instrument-fix/logs/kanicov-*.log` turn out to be **incoherent across
  families** — numeric-probe's 17 logs are missing, 4 bool logs are extra,
  and per-family runs happened at drifted source states (the same funcs.rs
  lines carry `dpi` in one log and `float48div` in another), so
  `recut-float140.py` over them yields 374/2,215, an upper bound over mixed
  shas, not the published row. Provenance note recorded here; COVERAGE.md
  already owes the exact number to the full-tree re-run.
- The v2 equivalent of the 338 row is therefore banded by the measured
  numerator behavior (0–2 kani-covered excluded lines in every coherent
  float capture): **≈337–338 / 1,946 = 17.32–17.37%, i.e. 15.26% → ≈17.3%
  (+2.1pp)**.

### Tree-wide excluded lines (tree-sloc.py --sloc-rule v2, at this branch)

Tree v1 SLOC 883,248 (2,084 files, 848 crates) → v2 **779,276** (−103,972,
−11.77%):

| class | tree-wide lines |
|---|---|
| typedef | 27,844 |
| use-mod | 19,259 |
| const-static | 12,928 |
| attr | 12,740 |
| arm-head | 12,091 |
| else-only | 9,654 |
| impl-header | 2,826 |
| thread-local | 1,631 |
| unsafe-only | 1,587 |
| loop-only | 1,107 |
| macro-scaffold | 881 |
| type-alias | 642 |
| arrow-only | 404 |
| fn-decl | 283 |
| trait-header | 80 |
| extern-block | 15 |
| **total v2-excluded** | **103,972 (11.77% of tree)** |
| const-table (separate knob) | 138,575 (15.69% of tree; 251 files, dominated by `unicode_norm/src/tables.rs` 39,353 and the `mb/conv` encoding maps) |

## 5. The ruling — four combinations (kept for historical comparability)

**RULED by Michael, 2026-07-30: rule v2 + tables OUT — the bolded cell — is
the denominator of record.** The other three cells remain reproducible via
`--sloc-rule v1` / `--include-const-tables` for comparability with earlier
published numbers.

7-crate dataset headline (`any` = executed by ≥1 instrument; kani in parens):

| | tables IN denominator | tables OUT (`--exclude-const-tables`) |
|---|---|---|
| **rule v1** | 72.12% (kani 6.01%) — pre-ruling published number | 74.77% (6.23%) — SLOC 21,205 |
| **rule v2** | 81.09% (6.70%) — SLOC 19,325 | **84.50% (6.98%) — SLOC 18,544 — ADOPTED** |

- **v2 vs v1** removes 2,661 lines, 2,475 of them permanently-red syntax and
  declarations — the `} else {` / `use` / `#[derive]` / `struct` field /
  `thread_local!` / macro-scaffolding artifact classes Michael found in the
  viewer, plus their measured siblings. This is the classification every
  established tool already embodies; the cost is 186 spillover-covered lines
  leaving the numerator too.
- **tables out** removes a further 781 scope lines (100% previously red —
  genuinely dark data), and tree-wide would remove 138,575 (15.7% of the
  tree — overwhelmingly generated unicode/encoding maps). Against: those
  lines are the omission signal for unregistered/never-loaded tables; the
  macro-attrib precedent (INSTRUMENT-FIX.md) chose "keep the honest signal"
  for the analogous fmgr-wrapper case. A middle path if wanted later:
  exclude only *generated* files (mb/conv maps, unicode tables carry
  generation headers), not hand-written registration tables.
- (Historical: the design note had recommended tables-IN with a
  marker-based revisit. Michael ruled tables OUT; the marker concern
  survives as the §5b inventory — over-exclusion is now caught by review of
  the published span list rather than prevented by keeping the lines.)

## 5b. Exclusion inventory — excluded-tables.json (the reviewability condition)

Every table span removed from the denominator is published alongside
`summary.json` in `excluded-tables.json`: file, head line (which STAYS in
the denominator), excluded span, line count, and the detection reason —
`generated-file` when the file carries a generated-do-not-edit marker
(marker-corroborated), `const-array-heuristic` otherwise. **The
heuristic-only rows are the standing audit surface: a span that swallows
real logic is a defect.**

7-crate capture: 7 spans, 781 lines, ALL const-array-heuristic — the seven
`*_BUILTINS: &[FmgrBuiltin]` fmgr registries (geo 230, float 153, numeric
113, varlena 112, jsonb 73, network 50, varbit 50). All seven verified data
rows only.

Tree-wide: **672 spans, 138,613 raw lines** (138,575 after the census's
crate-src/cfg-test filters), split:

| mechanism | spans | lines | files |
|---|---|---|---|
| generated-file marker | 107 | 120,277 (86.8%) | 18 |
| const-array heuristic | 565 | 18,336 (13.2%) | 232 |

All `utils/mb/conv/src/maps/*` and `common/unicode_norm/{tables,qc_tables}`
files carry `// Generated … do not edit.` markers — the entire big-table
mass is marker-corroborated, not heuristic-trusted. Top-20 spans by size:
the first 11 and 13-20 are all marker-backed mb-conv/unicode trees
(34,215-line `UnicodeDecompMain` down to 2,830-line `EUC_TW_FROM_UNICODE`),
the sole heuristic entry in the top tier being
`fmgr_core/src/canonical.rs:19-3121` `pub const CANONICAL:
&[CanonicalBuiltin]` (3,102 lines — the hand-written canonical fmgr
registry, verified data rows). Largest remaining heuristic spans, each
eyeballed as genuine data: `contrib/isn` ISBN_RANGE 910,
`adt_date/interval_corpus.rs` ARITH 846 + INTERVAL_IN_OK 469 (checked-in
test corpora), `mb/conv/src/lib.rs` CONV_BUILTINS 588, `saslprep`
codepoint ranges 396+360, `wchar` NONSPACING 334, nodes
`bms_c_vectors.rs` 303, plpgsql `errcodes.rs` EXCEPTION_LABEL_MAP 251,
`adt_misc/catalog_fk.rs` SYS_FK_RELATIONSHIPS 219.

**Marker candidates** (derived/generated-looking files that lack a marker
today — flagged for adding explicit markers upstream, NOT edited here):
`common/wchar/src/tables.rs`, `common/saslprep/src/tables.rs`,
`contrib/isn/src/tables.rs`, `adt_date/src/interval_corpus.rs`,
`types/nodes/src/bms_c_vectors.rs`. `common/unicode_norm/src/lib.rs` is
large but hand-written (no marker needed; its tables live in the marked
files).

## 6b. Rendered-output audit of the ADOPTED cut (v2 + tables out)

Re-audited after the ruling flip and 8644 regeneration: **sampled 165 red
lines, 3 per red-bearing file across all 59 files with red** (2,874 red
lines total under the adopted denominator). Classification:

- **Zero rule-defect bogus reds** — none of the excluded shape classes
  (else, imports, attrs, typedefs, thread_local, macro scaffolding, arm
  heads, single-line consts, table interiors) appeared red.
- The overwhelming majority are honest signals: unexercised error/edge
  paths (`return Err(…)`, `panic!`, NaN/Inf branches), whole never-executed
  functions (signatures + bodies red together), `fc_*` wrapper declaration
  lines (the macro-attribution proved/unproved signal), and macro template
  bodies no instrument exercised.
- Known-remainder hits, all counted and cosmetic (red lines, not
  denominator inflation): multi-line `const`/`static` with NON-bracket
  initializers — struct-literal fields (`numeric/src/var.rs` `digits: &[1],`)
  and OR-chain continuations (`jsonb/src/mutate.rs:22`) — the brace/scalar
  siblings of the bracket-table heuristic; inline `const { assert!(…) }`
  blocks (`jsonb/src/aggs.rs:58`); const-fn bodies (94 lines scope-wide,
  §6); multi-line pattern alternation lines (`| ItemType::AnyKey`); table
  HEAD lines (stay by design as the omission signal). The non-bracket
  const-initializer continuations are the natural next candidate class if
  Michael wants them folded in — a one-line extension of the span detector,
  listed here rather than smuggled in post-ruling.

## 6. Rendered-output audit (pre-ruling cut, kept for the record)

Audited the regenerated port-8644 bundle (this exact re-cut), not just the
totals: **sampled 72 red lines across 12 files** (weighted toward the
shape-rich files: the four `builtins.rs`, `lib.rs` headers, `io.rs`,
`aggregates.rs`, `fixed.rs`, `gin.rs`, `pton.rs`, varbit `lib.rs`).
Classification:

- **52 genuinely-executable / honest signals** — error paths (`return
  Err(float_overflow_error().into());`), unexercised branches (`0.0`
  if-else values, NaN/Inf arms), whole never-executed functions
  (`fixed.rs` 291/291 — the known `mul_var_short_fixed` gap), fn signature
  and parameter lines of unexecuted functions, statement macros
  (`tail_digit!(…)`), and the `fc_*` macro-invocation declaration lines
  (red = "this wrapper has no proof coverage" — the load-bearing
  macro-attribution signal, kept by design).
- **14 const-table policy rows** (`b(232, "dpow", 2, fc_dpow),`) — red under
  "tables IN" by the pending ruling; they leave under the knob (781 lines
  scope-wide). Not a rule defect; this is exactly what §5 decides.
- **6 const-fn-body lines** (`foid,` / `strict: false,` inside the
  `const fn b(…)` registration constructors) — llvm maps const-fn bodies
  (they are callable at runtime in principle), but these are only
  const-evaluated, so they read permanently red. **Known remainder, kept**
  (over-exclusion caution: a const fn is genuinely coverable): **94 red
  lines scope-wide** sit inside `const fn` bodies, concentrated next to the
  registration tables — they effectively ride with the tables ruling.
- **0 bogus reds of the six shape classes Michael found** (else, imports,
  debug_assert, thread_local, macro scaffolding, attributes) and 0 of the
  round-2 classes remained in the sample.

Known under-exclusion remainders (accepted, counted, all cosmetic-red not
denominator-inflating): multi-line pattern alternation lines before a
bodiless `=>` (the `| WjbToken::End` continuation shape; a handful
scope-wide), multi-line matcher heads inside `macro_rules!` (rare), `_ => {}`
empty arms (kept — inline body form). Each keeps the body-carried signal
adjacent, and none was hit in the sample.

Whatever is ruled, the running full-tree capture needs no re-run:
`recut-sloc.py` re-cuts its merge output under the ruled combination as pure
post-processing (with `--line-table-lcov` on its preserved lcov files for the
instrument-precedence path).

## Citations

[1] llvm/lib/ProfileData/Coverage/CoverageMapping.cpp, `LineCoverageStats`
    ctor — https://github.com/llvm/llvm-project/blob/main/llvm/lib/ProfileData/Coverage/CoverageMapping.cpp
[2] llvm/tools/llvm-cov/CoverageExporterLcov.cpp,
    `renderLineExecutionCounts` (`DA:` iff `isMapped()`), `renderLineSummary`
    — https://github.com/llvm/llvm-project/blob/main/llvm/tools/llvm-cov/CoverageExporterLcov.cpp
[3] clang/lib/CodeGen/CoverageMappingGen.cpp — gap emission for if/else,
    loops, switch; "The 'else' count applies to the area immediately after
    the 'then'." — https://github.com/llvm/llvm-project/blob/main/clang/lib/CodeGen/CoverageMappingGen.cpp
[4] LLVM Coverage Mapping Format — region kinds; "A count for a gap area is
    only used as the line execution count if there are no other regions on a
    line"; `__llvm_covmap` lives in the binary —
    https://llvm.org/docs/CoverageMappingFormat.html
[5] Clang Source-Based Code Coverage — "Uninstrumented code simply won't be
    accounted for in reports." —
    https://clang.llvm.org/docs/SourceBasedCodeCoverage.html
[6] rustc `MappingKind` (Code/Branch only, no Gap) —
    https://github.com/rust-lang/rust/blob/master/compiler/rustc_middle/src/mir/coverage.rs
[7] rustc span refinement —
    https://github.com/rust-lang/rust/blob/master/compiler/rustc_mir_transform/src/coverage/spans.rs ;
    pipeline: https://rustc-dev-guide.rust-lang.org/llvm-coverage-instrumentation.html
[8] GCC gcov data files (`.gcno` "assign source line numbers to blocks") —
    https://gcc.gnu.org/onlinedocs/gcc/Gcov-Data-Files.html
[9] geninfo(1) — DA/LF/LH definitions ("a line which resulted in executable
    code") — https://linux.die.net/man/1/geninfo
[10] Invoking Gcov — "`-` for lines containing no code"; `#####`/`=====` —
    https://gcc.gnu.org/onlinedocs/gcc/Invoking-Gcov.html
[11] lcov(1) `--initial` — zero baseline "to ensure that the percentage of
    total lines covered is correct even when not all source code files were
    loaded" — https://linux.die.net/man/1/lcov
[12] How coverage.py works —
     https://coverage.readthedocs.io/en/latest/howitworks.html
[13] coveragepy parser (clause bodies only; docstrings excluded) —
     https://github.com/nedbat/coveragepy/blob/master/coverage/parser.py ;
     https://coverage.readthedocs.io/en/latest/excluding.html
[14] coverage.py FAQ (`def`/`class` lines execute at import) —
     https://coverage.readthedocs.io/en/latest/faq.html
[15] coverage.py `source=` — reports un-executed files —
     https://coverage.readthedocs.io/en/latest/source.html
[16] Istanbul coverage object (`statementMap`; line coverage
     reverse-engineered from statements) —
     https://github.com/gotwarlost/istanbul/blob/master/coverage.json.md ;
     https://github.com/istanbuljs/istanbuljs (istanbul-lib-coverage
     `getLineCoverage`, istanbul-lib-instrument source-coverage.js)
[17] nyc README — default "only collects coverage for source files that are
     visited during a test"; `--all` — https://github.com/istanbuljs/nyc
[18] JaCoCo counters — https://www.jacoco.org/jacoco/trunk/doc/counters.html
[19] JaCoCo FAQ — missing execution data shows as not covered —
     https://www.jacoco.org/jacoco/trunk/doc/faq.html
[20] tarpaulin — README + Developers wiki ("what lines are uncoverable",
     constructs "mistakenly included as misses") —
     https://github.com/xd009642/tarpaulin ;
     https://github.com/xd009642/tarpaulin/wiki/Developers
[21] grcov README — aggregator, `--binary-path`, subtractive filters only —
     https://github.com/mozilla/grcov
