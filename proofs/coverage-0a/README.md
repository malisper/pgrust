# proofs/coverage-0a — Lane-0A done-gate coverage capture (2026-07-30)

Crates: adt/char, adt/bool, adt/pseudotypes, adt/pg_lsn (phase-1 campaign,
branch proofs/p1-lane0a). Metric of record: line coverage, SLOC rule v2,
tables excluded, numerator = kani ∪ coverage-guided differential fuzz
(regress counts for nothing — ruling of record).

Result: TOTAL sloc=665 kani=276 (41.5%) fuzz=514 (77.3%) any=612 (92.03%);
remaining 53 lines ALL carried by recorded exception rows
(proofs/coverage/phase1-exceptions.tsv, author=lane-0a) = 665/665 accounted.
Per crate: char 98+9, bool 186+18, pseudotypes 173+23, pg_lsn 155+3.

Provenance:
- Kani axis: fail-closed census `lane0a-census-all.tsv` (216 planned jobs:
  147 RAN locally before policy 5b2646b127 retired per-crate kanicov
  re-runs mid-capture; 69 NOT-ATTEMPTED, waived proof-covered-unmeasured in
  `lane0a-allow-unmeasured.tsv` — the full-tree pinned-sha join settles
  them) + 14 wrapper_fc first-solve harnesses (bool 8, pg_lsn 6, all green
  1.0-2.1s, registered in SUITE.tsv with 2 must-fail controls).
- Fuzz axis: fleet fuzz-campaign lcovs (gzipped here; S3 has 7-day
  lifecycle): jobs pgrust-fuzz-campaign-1785471723-10db-49150 (10M/target),
  ...-1785473310-0fd6-92969 (10M/target), ...-1785475724-2942-60217
  (bool_diff 10M). Local campaigns preceding: char 12M / bool 12.3M /
  pseudotypes 12.3M / pg_lsn 30.3M execs. ZERO divergences across all
  campaigns and both platforms (macOS host libc + fleet aarch64-linux/glibc).
- Oracle: verbatim Stamp-18.3 (62d6c7d3df) C in fuzz/core/csrc (pg_char.c,
  pg_bool.c, pg_pseudotypes.c, pg_lsn_oracle.c) — value + error-verdict +
  errcode planes; sancov union coverage on the C side (PGRUST_FUZZ_CSANCOV=1).
- summary.json / verification-coverage.tsv / files/ = merge-coverage.py
  output (census-closed); viewer cross-check passed (generate.py --from-real,
  none=53 == exception-row count; rendered red-line audit clean, zero
  unexplained bogus-red).

Mutants audit: TRAILING per policy 5b2646b127 (see claims-row notes).

## Mutation audit (trailing per policy 5b2646b127; completed 2026-07-30)

`cargo mutants -p <crate>` (unit-test oracle only — the campaign oracles,
differential fuzz + Kani, are NOT run by cargo-mutants):
char 142 tested (62 caught / 10 missed / 70 unviable), bool 186 (92/9/84+1
timeout), pseudotypes 205 (3/5/197), pg_lsn 178 (68/17/93). All 41
survivors triaged:

- **Killed-by-differential (demonstrated)**: representative mutant per class
  applied and replayed over the committed corpus via fuzz/replay-rail.sh —
  `chargt -> true` KILLED (char_diff), `is_octal -> true` KILLED
  (char_diff), pg_lsn_mi `delete -` KILLED (pg_lsn_diff, fuzz-only region),
  boolin `in_arg -> "xyzzy"` KILLED (bool_diff). The cmp/parse/agg survivor
  classes in proved-full-domain functions are additionally killed by the
  standing Kani harnesses over their full domains (mutants simply does not
  run either oracle).
- **Plane gap FOUND AND CLOSED**: 12 fc_pg_lsn_hash(_extended) fold mutants
  survived because the driver ran the hash wrappers execution-only. Fixed:
  wrapper-fold parity plane added to lsn_diff (independent recomputation of
  C hashint8's lo^hi fold, hashfunc.c), kill demonstrated (`^` -> `&`
  KILLED), fleet confirm job pgrust-fuzz-campaign-1785477282-2e31-75971.
- **Arid (4, pseudotypes)**: out_scratch fresh-buffer/`delete !` mutants
  (reused-scratch vs fresh-allocation — no semantic surface, output bytes
  identical; perf-only) and pstrdup capacity-hint `+ -> *` (PgVec grows;
  same bytes).

## SLOC re-baseline residue closure (2026-07-31, fix/sloc-residue)

The 2026-07-31 structural test-scope fix (proofs/coverage/test_scope.py)
revealed that the superseded scanner had walked from `#[cfg(test)] mod tests;`
into the next braced item in pg_lsn/src/lib.rs, silently dropping the ENTIRE
body of `pg_lsn_in_internal` (the LSN parser, lines 25-28,30-33,35-37 = 11
lines) from the denominator. The "158/158 = 100%" above was measured over that
short denominator.

Resolution — MEASURED, not assumed: fresh local full-corpus coverage export
(`fuzz/cov-export.sh pg_lsn_diff`, 1773 committed corpus inputs) at the
re-baselined scope (branch tools/sloc-denominator-fix, e25331c7e9; pg_lsn
sources bit-identical to this capture's head_sha e339e7ce8133). All 11 lines
are fuzz-covered with DA counts 45-1230 (evidence:
fuzz-pg_lsn_diff-residue-20260731.lcov.gz; agrees line-for-line with the
archived fleet lcov fuzz-pg_lsn_diff.lcov.gz). No driver or corpus change was
needed, so no new fleet CONFIRM is owed.

Amended accounting (this capture's summary.json/files/tsv updated in place;
provenance field `residue_amendment` in summary.json):
pg_lsn 169 sloc = 166 measured (kani 40, fuzz 160, lib.rs 93/93) + 3 exception
rows (builtins.rs 148/149/153, unchanged). TOTAL 676 = 623 any + 53 exceptions.
