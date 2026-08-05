# excreview-flagfix evidence bank (2026-07-31)

Lane: ledger flag-resolution for the phase-1 exceptions ledger
(`proofs/coverage/phase1-exceptions.tsv`), resolving the excreview-bulk (254)
and excreview-a (800) FLAGGED rows. Branch `proofs/excreview-flagfix`.

## Files
- `fuzz-timestamp_diff-local-20260731.lcov.gz` — local full-corpus
  `fuzz/cov-export.sh timestamp_diff` replay at this branch's tip (base
  origin/main ce5f59b72b post-wave3 + this lane's harness/seed additions;
  the flagfix-iso-hexsubn leg merged via llvm-profdata into the corpus
  profile, single-input replay of the same coverage binary). This is the
  adt/adt_timestamp fuzz coverage of record for the pass-2 resolutions:
  - NEW measured lines vs the cov-resweep export: the TimestampDifference /
    Milliseconds / Exceeds / ExceedsSeconds family (lib.rs 492-528) via the
    new differential plane in `timestamp_mi_arm` (C twins added VERBATIM to
    `fuzz/core/csrc/pg_timestamp_verbatim.inc` via `extract_ts_verbatim.py`;
    shim entries `pg_tsdiff_timestamp_difference*`), plus interval.rs
    461/502/513/582 (interval_mul/div deep overflow + sentinel arms) and
    1378 (make_interval NOBEGIN sentinel) via `corpus/timestamp_diff/
    flagfix-*` seeds, plus lib.rs 1786 (float8_timestamptz ±inf).
  - lib.rs:1034 witness: previously recorded-covered but
    corpus-unreproducible (cov-resweep line-regression report); now driven
    by banked seeds `flagfix-mtzat-oor-east`/`-west`.
- `tsjoin-summary.json` — `merge-coverage.py --sloc-rule v2
  --exclude-const-tables` join of that lcov (scope adt_timestamp,
  exploration mode; kani leg joined separately from the fulltree capture in
  `proofs/coverage/files/`). Crate totals: fuzz 2141/3140 SLOC.

## Re-balanced equations (uncovered − excepted = 0)
Basis: v2+no-const-tables denominator at current main with instrument
line-table reinstatement; covered = kani(fulltree @e395e4c) ∪ fuzz(local
full-corpus lcov of record).

- adt/float (cov-resweep union lcov `proofs/coverage/cov-resweep/` on branch
  proofs/cov-resweep @a594887353, remapped +180 at old line 330 for the
  wave-3 strtod_c insertion, + the 15 banked fleet io.rs lines + this bank's
  timestamp_diff lcov for the strtod_c block): aggregates 183/183,
  builtins 198/198, funcs 139/139, io 9/9 + RECORDED residual {504,505},
  lib 58/58 — total denom 1913, covered 1324, uncovered 589 = excepted 587
  + residual 2. New rows: io.rs 559/629 instrument-unmappable (pre-wave3
  379/449), io.rs 344 defensive-c-parity (strtod ws-skip).
- adt/adt_timestamp (this bank's lcov): builtins 387/387, interval 132/132,
  lib 210/210 — denom 3140, covered 2411, uncovered 729 = excepted 729.
- RECORDED RESIDUALS:
  - json crate jsonapi.rs:970 — RETIRED 2026-07-31 by the jsonb-soft lane
    (proofs/jsonb-soft-plane): jsonbio_diff gained the compared soft-error
    (escontext) plane, both sides armed; line MEASURED (DA=69), witness
    lcov at proofs/coverage/jsonbsoft/.
  - float io.rs 504-505 — strtod_c subnormal-exactness compare tail; needs
    >200-byte tokens (docker-verified 1076-char exact decimal subnormal),
    beyond every in-tree harness text cap.
