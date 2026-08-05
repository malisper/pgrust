# proofs/coverage-rf — TREE-WIDE regress + fuzz coverage axes

Captured 2026-07-30 on branch `proofs/coverage-fulltree-rf` (base
`proofs/sloc-rule-v2` @ ccd2d20b22, the adopted-denominator ruling).
This dataset carries **two of the three axes** — regress and fuzz — over
the full 848-crate scope (`scope-fulltree.txt`, identical to the Kani
lane's `proofs/coverage/fulltree/scope-fulltree.txt`). The **Kani axis is
deliberately empty here** (empty census, 0 expected harnesses — closes
trivially); it is captured by the concurrent `proofs/coverage-fulltree`
lane. Same schema as `proofs/coverage/` (summary.json + files/ + census),
separate directory so the final three-axis join composes without
collision.

## Headline (ADOPTED denominator: SLOC v2 + const/data tables excluded)

    scope SLOC  647,303   (848 crates)
    regress     387,529   (59.87%)
    fuzz          1,182   (0.18%)
    any (r|f)   387,682   (59.89%)

v1 footnote (pre-ruling rule, tables included): SLOC 883,248 —
regress 387,529 = **43.88%**, any 43.89%. (Covered counts are identical
under both rules; only the denominator moves.)

Sanity floor: the 7-crate capture's regress axis was 15,598 covered SLOC;
this capture's 387,529 exceeds it 24x, and the seven adt crates reproduce
their prior per-crate regress counts exactly where the source is unchanged
(jsonb 4,458; numeric 4,115).

**A covered line is not a verified line** — read proofs/COVERAGE.md's
READ-THIS-FIRST before quoting any percentage.

## What was run (provenance)

- **Regress**: instrumented whole-workspace server
  (`RUSTFLAGS=-Cinstrument-coverage`, profile fast-profile) built at this
  branch's base; full parallel_schedule via `proofs/coverage/pg-regress-cov.sh`
  (graceful-shutdown variant — the stock runner's `kill -9` discards the
  profile). Result: **218/230 ok, 1 segment, no crashes**; the 12 fails are
  the standing set (create_index, select_distinct, subselect, join, portals,
  tidscan, incremental_sort, limit, partition_join, partition_prune,
  partition_aggregate, memoize). One server process, one profraw
  (server-%p), exported with the rustc-1.96 sysroot llvm tools →
  `regress.lcov` (committed gzipped; `merge-rf.sh` gunzips).
  The prior capture's regress lcov/profdata artifacts were NOT recoverable
  (only fuzz profdata survived, and 19 crates/ files changed since that sha),
  so this is a fresh capture, not a re-merge.
- **Fuzz**: `cargo +nightly-2026-07-17 fuzz coverage` re-run per target at
  this sha over the surviving session corpora copied from the original
  capture worktree (.wt-coverage): float_in_diff 666 inputs,
  float_out_diff 106, geo_diff 705. No fuzzing was re-run — replay only.
  Exported with the nightly sysroot llvm-cov → `fuzz-*.lcov`. Tree-wide the
  corpora touch 10 files / 1,182 SLOC (float/geo/adt-adjacent) — everything
  else honestly uncovered on this axis.
- **Merge**: `merge-rf.sh` (defaults: `--sloc-rule v2`,
  `--exclude-const-tables`; all four lcovs double as the v2 instrument
  line tables).

## Top-15 crates by regress-covered SLOC

| crate | SLOC | regress | % |
|---|---|---|---|
| optimizer/plan/planner | 43,908 | 36,306 | 82.7 |
| executor/execmain | 34,818 | 13,035 | 37.4 |
| commands/tablecmds | 14,364 | 12,126 | 84.4 |
| executor/execexpr | 9,482 | 7,410 | 78.2 |
| parser/gram_core | 8,376 | 7,058 | 84.3 |
| access/nbtree/nbtree | 7,875 | 6,693 | 85.0 |
| executor/nodeagg | 15,122 | 5,906 | 39.1 |
| pl/plpgsql | 6,929 | 5,407 | 78.0 |
| utils/adt/ruleutils | 6,120 | 5,333 | 87.1 |
| regex/regex_core | 6,626 | 5,123 | 77.3 |
| executor/nodemodifytable | 5,564 | 4,505 | 81.0 |
| utils/adt/jsonb | 5,029 | 4,458 | 88.7 |
| access/gin/gin | 5,452 | 4,409 | 80.9 |
| utils/adt/numeric | 4,943 | 4,115 | 83.2 |
| access/heap/heapam | 4,790 | 3,262 | 68.1 |

## Red-line audit (pre-share bar)

74 uncovered (regress∪fuzz-red) lines sampled across 14 files
(seed 20260730, scripted). Classification:

- **60 genuinely-executable-uncovered** — error/ereport paths regress never
  triggers, unexercised strategy/verdict arms (TidRangeScan,
  NamedTuplestoreScan lanes), WAL rmgrdesc formatters, catcache hash fns for
  unused key types, hba include-file error paths, SIMD lanestitch paths not
  engaged under this config, ts_selfuncs early returns. Honest reds.
- **Class A (known bogus, 6 sampled): generator-macro invocation lines**
  (`seam!`, `fc*!`, …). LLVM attributes the generated fn's execution to the
  macro *definition* body, so the invocation line reads red even when the
  fn ran — verified: auth_seams' `client_authentication` declaration lines
  are red while its implementation (libpq/auth/src/lib.rs) shows regress=44.
  Same defect class the Kani axis fixed with macro_attrib.py; NOT yet fixed
  for the lcov axes. Bound: 2,753 declaration lines tree-wide (COVERAGE.md
  census) = 1.06% of the 259,621 red lines = at most **0.43pp** headline
  understatement.
- **Class B (known bogus, 8 sampled): residual data lines in generated
  encoding maps** — struct-literal static initializer fields + table-header
  lines the const-table heuristic deliberately keeps (johab.rs: 7,189/7,205
  table lines excluded; the 16 survivors include 8 data-field lines).
  Bound: 633 red lines across mb/conv/src/maps = 0.24% of red ≈ 0.02pp.

Zero unexplained bogus reds.

## Three-axis join (run by whichever lane lands second)

On a tree containing BOTH `proofs/coverage/fulltree/` (Kani lane) and this
directory, run the command documented at the top of `merge-rf.sh` — it is
the Kani lane's own `merge-fulltree.sh` invocation plus this dataset's
`--fuzz-lcov`/`--regress-lcov`/`--line-table-lcov` arguments, writing the
unified three-axis summary to `proofs/coverage/`.
