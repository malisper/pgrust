# tsm_system_time_diff 10M floor adjudication (p1-wavea, 2026-08-01)

Job pgrust-fuzz-campaign-1785625621-23ea-94965 @ b57ef00825072a1aa1e4dbde18d3675e63c3ca9d
(c8g.4xlarge, FUZZ_FORK=14, activeDeadlineSeconds=20000 verified in-spec).

- outcome=complete, rc=0; execs 10,224,662 / 10,000,000 requested; wall 15s (fuzz phase).
- sanitizer_artifacts 0; corpus 343 -> 348 (delta 5); cov_lines 484.
- divergences_total=24 — ALL STRAYS (same population as the unit-1 job: regex_core
  regex_compile.rs:2821/:2163 x7 + network_diff.rs:368/:691 x2 + .repro.txt dupes);
  stray_artifacts_swept=24, unclassified=0; all 12 inputs replayed locally against
  tsm_system_time_diff at the same sha: 12/12 clean. VERDICT: PASS, 0 target divergences.

## Coverage equation (carve = crates/contrib/tsm_system_time/src/lib.rs)
Job lcov: 127 DA lines, 126 hit. Residual 1 line = exception row (L125
sampler_random_fract zero-draw retry, defensive-c-parity, unit-test witnessed by
tests::sampler_random_fract_retries_zero_draw). measured(126) + exceptions(1) == 127. GATE HOLDS.

## Corpus bank
Union (342 committed + 348 fleet snapshot) merge-minimized to 75 coverage-preserving
files; 98 directed seeds (49 single-field witness variants) re-added per the
witness-pair obligation. Bank = 173 files, replays clean.

## Addendum (2026-08-01, runner-fix confirmation)
Coordinator-confirmed root cause (fabled fleet/fuzz-campaign-hardening @ a4ed674bd2): the
sweeper was ledgering GIT-TRACKED committed evidence files as strays. Verified here: the
swept crash-* hashes are tracked files under fuzz/fleet-evidence/regexp_diff-1785535304/
(git ls-files match). This independently corroborates the replay-clean adjudication above:
PASS stands, 0 target-attributable divergences.
