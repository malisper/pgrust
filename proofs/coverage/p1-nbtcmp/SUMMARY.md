# p1-nbtcmp coverage evidence (backend/access/nbtree/compare)

- Crate sha of record: 83d3458df7b08f26b67dd45d1629e7b7795f02d8 (source files
  identical through the corpus-bank commit 4376977bb2).
- Fleet campaign: pgrust-fuzz-campaign-1785613880-5d9d-16900 — nbtcmp_diff,
  10,000,000 execs, 0 divergences, 0 sanitizer artifacts, rc=0
  (campaign-stats-1785613880.json; lcov = fuzz-nbtcmp_diff-fleet-20260801.lcov.gz).
- Union inputs: fuzz-only (Kani credit carried as proved ledger rows;
  no per-crate Kani coverage re-run per the standing prohibition — the
  macro-row/const-eval residual is carried as exceptions, not proof credit).
- v2-SLOC accounting @ final sha (verified residual==exceptions, GATE PASS):
  - src/lib.rs: 61 covered + 14 exceptions = 75/75
  - src/builtins.rs: 17 covered + 21 exceptions = 38/38
  - crate: 78 measured + 35 recorded exceptions = 113/113
- Exceptions: proofs/coverage/phase1-exceptions.tsv rows author=p1-nbtcmp
  (instrument-unmappable macro-invocation rows for threeway!/skip_incdec!/
  fc_cmp!; const-eval-only rows for the const fn b() NBT_BUILTINS table).
- Injection sweep at plane creation: 5/5 planted defects CAUGHT.
- Corpus bank: fuzz/corpus/nbtcmp_diff = 239 files (merge-minimized from
  union 1068 = 441 seeds + local 2M growth + fleet snapshot 740).
