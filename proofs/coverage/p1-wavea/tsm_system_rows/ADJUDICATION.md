# tsm_system_rows_diff 10M floor adjudication (p1-wavea, 2026-08-01)

Job pgrust-fuzz-campaign-1785624206-40be-53905 @ 9f585beb9e87bfda9ca759c498c546e571078b87
(c8g.4xlarge, FUZZ_FORK=14, activeDeadlineSeconds=20000 verified in-spec).

- outcome=complete, rc=0; execs 10,012,650 / 10,000,000 requested; wall 25s (fuzz phase).
- sanitizer_artifacts 0; corpus 252 -> 257 (delta 5); cov_lines 472.
- divergences_total=24 — ALL STRAYS, not this target's: repro logs name
  crates/backend/regex/regex_core/src/regex_compile.rs:2821/:2163 assertion panics (x7 distinct)
  and core/src/network_diff.rs:368/:691 panics (x2), plus .repro.txt double-counting.
  campaign-stats corroborates: stray_artifacts_swept=24, unclassified_stray_artifacts=0.
  All 12 crash inputs replayed locally against the tsm_system_rows_diff binary at the same
  sha: 12/12 exit clean. VERDICT: PASS, 0 target-attributable divergences.
  FLAG (harness): the sweeper banks stray artifacts under the running target's
  divergences/ prefix — adjudicators must decode repro logs, not count files.

## Coverage equation (carve = crates/contrib/tsm_system_rows/src/lib.rs)
Job lcov: 126 DA lines on the carve file, 124 hit by the fuzz corpus.
Residual 2 lines = exception rows in proofs/coverage/phase1-exceptions.tsv
(L122 sampler_random_fract zero-draw retry, L153 clamp_row_est MAXIMUM_ROWCOUNT arm),
both defensive-c-parity, both witnessed executable by in-crate unit tests
(tests::sampler_random_fract_retries_zero_draw, tests::clamp_row_est_maximum_rowcount_arm).
measured(124) + exceptions(2) == 126 DA lines on the carve file. GATE HOLDS.

## Corpus bank
Union (67 directed + 251 committed + 257 fleet snapshot) merge-minimized with -merge=1
to 72 coverage-preserving files; 67 directed seeds (34 single-field witness variants)
re-added per the witness-pair obligation. Bank = 139 files, replays clean.

## Addendum (2026-08-01, runner-fix confirmation)
Coordinator-confirmed root cause (fabled fleet/fuzz-campaign-hardening @ a4ed674bd2): the
sweeper was ledgering GIT-TRACKED committed evidence files as strays. Verified here: the
swept crash-* hashes are tracked files under fuzz/fleet-evidence/regexp_diff-1785535304/
(git ls-files match). This independently corroborates the replay-clean adjudication above:
PASS stands, 0 target-attributable divergences.
