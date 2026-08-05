# p1-laneah coverage merge (common/wchar + mb/mbutils)

Merge of record for the lane's DONE gate. Reproduce with:

    python3 proofs/coverage/merge-coverage.py \
      --scope proofs/coverage/laneah/scope.txt \
      --census proofs/coverage/laneah/census-wcharfam.tsv \
      --allow-unmeasured proofs/coverage/laneah/allow-unmeasured-wcharfam.tsv \
      --fuzz-lcov fleet-fuzz-results/pgrust-fuzz-campaign-1785508363-7d57-32869/wcharfam_diff/coverage.lcov \
      --fuzz-lcov proofs/coverage/laneah/exhaustive.lcov \
      --line-table-lcov <same two> \
      --sloc-rule v2 --auto-exceptions --outdir proofs/coverage/laneah/out

Inputs:
  - wcharfam_diff fleet campaign pgrust-fuzz-campaign-1785508363-7d57-32869
    at sha b0a31a926086: 10,000,000 execs, 0 divergences, 0 sanitizer
    artifacts, fail-closed fetch exit 0. lcov archived under
    fleet-fuzz-results/ (S3 7-day lifecycle).
  - exhaustive.lcov: instrumented run of the a0 EXHAUSTIVE-DIFF driver
    (fuzz/core/tests/wcharfam_exhaustive.rs, quick + x tier, all green;
    run log proofs/wcharfam/RUNLOG.md). The a0 sweeps are dual-exec
    differential (same oracle as the fuzz target), so their coverage is
    fed on the fuzz axis.

TOTAL sloc=1676 fuzz=1257 (75.0%), census_closed=true.

kani=0 is honest: no per-crate kanicov re-run (standing campaign rule); the
pre-existing utf8 + encnames harness families (60 rows, per-commit green)
are carried in the census and waived proof-covered-unmeasured / must-fail
controls.

Residual 419 lines, every one carried as an exception row:
  - 22 auto rows (proofs/coverage/laneah/out/auto-exceptions.tsv)
  - 397 hand rows appended to proofs/coverage/phase1-exceptions.tsv:
    375 excluded-state (the mb/mbutils phase-1 carve of record:
    client/database-encoding GUC state, conversion-proc fmgr dispatch,
    their error ctors, seam wiring, and the convert SQL builtins whose
    ledger rows are excluded/blocked), 17 platform-other (x86_64 +
    no-SIMD is_valid_ascii variants; aarch64 NEON arm fully covered),
    5 const-eval-only (UTF8_TRANSITION CR consts + pg_wchar_table
    initializer field rows; the table dispatch itself exhaustively diffed).

Covered(1257) + excepted(419) = sloc(1676): the done-gate identity holds.

Rendered red-line audit (standing pre-share rule): the residual ranges were
eyeballed against the source — all red lines are the named OUT-carve
functions, cfg'd-out platform arms, or const/table machinery; zero
unexplained bogus-red.
