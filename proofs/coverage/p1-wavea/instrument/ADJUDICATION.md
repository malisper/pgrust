# instrument_diff floor adjudication — p1-wavea, 2026-08-01

Crate: crates/backend/executor/instrument (backend/executor/instrument, A-carve, carve_files=src/lib.rs).
Floor job of record: `pgrust-fuzz-campaign-1785636987-6c92-72043` @ sha `1d5df602ebfb95634a0cd317cd7f13767dead4a9`
(branch proofs/p1-wavea-instr; src/lib.rs unchanged since the floor sha — lcov valid).

## Verdict

- execs 10,233,462 (requested 10,000,000) — floor met.
- divergences 0, sanitizer_artifacts 0, crashed_early 0.
- stray artifacts: 0 swept, 0 unclassified (runner a4ed674bd2 excludes git-tracked
  evidence strays; nothing remained to replay).
- fetch-fuzz-results printed `NOT CLEAN ... bad_rows=1`: SCRIPT ARTIFACT, adjudicated
  benign. The bad-rows grep counts every `"outcome":` key in campaign-stats.json that
  is not `"run"`; this schema carries the JOB-level `"outcome": "complete"` alongside
  the single target row `"outcome": "run"`, so a fully-green single-target job always
  counts 1. The only TARGET row is `"outcome": "run"` with the numbers above.

## Floor attempt 1 (consumed, failed): pgrust-fuzz-campaign-1785632272-6a94-97997 @ ab5e3bc363

DeadlineExceeded@3600s, no artifacts uploaded. Root cause was a HARNESS defect, not
infra and not a pgrust bug: the bit-exact f64 value plane false-fired on NaN payload
propagation (`tuplecount += nTuples` with two NaN operands: clang C propagated the
first-operand qNaN, rustc the quietened sNaN — LLVM commutes fadd; which payload
survives is IEEE-unspecified and compiler-dependent, C-vs-C diverges the same way).
Under `-fork=14 -ignore_crashes=1` the crash every ~400 execs churned restarts until
the deadline. Fixed by the diff.rs-pattern certified relaxation (any-NaN == any-NaN
on the 9 f64 fields only; NaN-ness and all non-NaN bits stay bit-exact), witness
seeds banked (cycle_nan_payload_qs/_sq, nan_payload_carve_witness), and the f64
plane re-proven live post-relaxation (plant F1 nloops+=2.0 CAUGHT).

## Injection sweep (honest counts)

6/6 planted defects CAUGHT from the banked-corpus replay, 0 first-pass misses,
0 seeds needed to close: V1 value-plane cross-field (bufusage.local_blks_hit),
E1 error-verdict (start-twice arm deleted), M1 error-message (literal mutated),
C1 clock-plane (+1 tick), W1 wrap-plane (saturating_add on wal_bytes),
F1 f64-plane post-relaxation (nloops += 2.0). Plus the inherited wire-layout pin
test (R1) covering symmetric-encode drift.

## Coverage equation (src/lib.rs, fleet lcov DA lines)

158 fuzz-measured + 18 exception rows = 176 total DA lines.
The 18 exception lines are exactly the census-OUT carves: lines 74-93
(InstrStart/End/AccumParallelQuery + WORKER_CONTRIB overlay — deliberate pgrust
divergence from C's write-into-live-globals scheme) and 96-98 (pg_wal_usage
global-counter reader). Rows in proofs/coverage/phase1-exceptions.tsv, class
census-carve.

## Corpus

Union grown 271 (fleet snapshot; superset of the 259 local bank) merge-minimized
to 117 coverage-preserving units + 113 directed named seeds (option matrices,
error shapes, zero-clock LAZY sentinel, nloops=0, async firsttuple re-latch,
all-16-BufferUsage-field and all-4-WalUsage-field single-field witness pairs both
orders, wal_bytes near-u64::MAX wrap pairs, negative-delta accum shapes, NaN
payload witnesses) = 230 banked.
