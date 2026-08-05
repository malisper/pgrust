# p1-lanel coverage merge (adt_date + adt_datetime)

Merge of record for the lane's DONE gate. Reproduce with:

    python3 proofs/coverage/merge-coverage.py \
      --scope   proofs/coverage/lanel/scope.txt \
      --census  proofs/coverage/lanel/census-datetime-b.tsv \
      --allow-unmeasured proofs/coverage/lanel/allow-unmeasured-datetime-b.tsv \
      --fuzz-lcov <io> --fuzz-lcov <interval> --fuzz-lcov <engine> --fuzz-lcov <convert> \
      --line-table-lcov <io> --line-table-lcov <interval> --line-table-lcov <engine> --line-table-lcov <convert> \
      --sloc-rule v2 --auto-exceptions --outdir proofs/coverage/lanel/out

Fuzz inputs = the four FLOOR-CLEAN campaigns (10,000,000 execs each,
0 divergences, 0 sanitizer artifacts, fail-closed gate exit 0):
  datetime_io_diff      pgrust-fuzz-campaign-1785500964-6478-49313  @ db1e7f827e
  interval_engine_diff  pgrust-fuzz-campaign-1785500971-50ea-49589  @ db1e7f827e
  datetime_engine_diff  pgrust-fuzz-campaign-1785500977-79ec-49847  @ db1e7f827e
  datetime_convert_diff pgrust-fuzz-campaign-1785507243-039d-48526  @ d36bdb059b

## Merge of record (four targets, 2026-07-31)

TOTAL sloc=4037 fuzz=2817 (69.78%), census_closed=true (59 harnesses waived).
Per crate: adt_datetime 2375/2547 (93.2%), adt_date 442/1490 (29.7%).
Exceptions: 838 per-line rows appended to proofs/coverage/phase1-exceptions.tsv
(proof-covered-unmeasured 588, excluded-state 142, instrument-unmappable 61,
const-eval-only 28, defensive-c-parity 19). Honestly-uncovered owed residual:
382 lines, named per-line in OWED-UNCOVERED.tsv herein.

Measured + exception-carried: adt_datetime 2500/2547 = 98.15%,
adt_date 1155/1490 = 77.52%. NEITHER crate claims 100%; the owed work is
named, not dressed up (extract_date/extract_time numeric closure 72, timetz
part(tz) TRIAGE-HOLD 31, adt_timestamp cross-crate wrappers 68, planner-node
time_support 25, wrapper-level-undriven thin fc wrappers 107, unhit decode
error/overflow arms 45+34, interval-ctor cross-crate 2).

## kani = 0 is honest, not a gap

This lane runs NO per-crate kanicov re-run (standing campaign rule), so NO
harness emits a kaniraw and measured kani coverage is 0 BY CONSTRUCTION. The
census therefore carries all 34 datetime-b `hlp::` harnesses explicitly and
waives every one with a stated reason, so the fail-closed census still closes
(`census_closed: true`): 26 fleet-GREEN as proof-covered-unmeasured, 5
wall-recorded (601s > 600s cap under cadical, CBMC per-property refinement
phase — NOT symex), 3 must-fail controls that contribute non-vacuity and never
coverage. Those proof-covered lines are NOT counted as covered anywhere in
this merge; they are carried as exception rows. Read the totals as
"fuzz-measured coverage, with the proof-covered set named and excluded from
the numerator", never as "the proofs cover nothing".

## CLOSEOUT merge of record (p1-lanel2, 2026-07-31) — BOTH CRATES DONE

Six targets, LOCAL full-corpus cov-export at the lane branch tip (fleet
lcovs are partial captures and were NOT used). Reproduce with the command
above plus `--fuzz-lcov/--line-table-lcov` for timestamp_diff (merged from
p1-laney, 23,813-input corpus) and datetime_closeout_diff (new, 6,996).

Fuzz inputs = six FLOOR-CLEAN campaigns (10,000,000 execs each, 0
divergences, 0 sanitizer artifacts):
  datetime_io_diff      pgrust-fuzz-campaign-1785500964-6478-49313  @ db1e7f827e
  interval_engine_diff  pgrust-fuzz-campaign-1785532939-6eb9-40024  @ 74d2a7c545
  datetime_engine_diff  pgrust-fuzz-campaign-1785500977-79ec-49847  @ db1e7f827e
  datetime_convert_diff pgrust-fuzz-campaign-1785507243-039d-48526  @ d36bdb059b
  timestamp_diff        pgrust-fuzz-campaign-1785532946-1bcf-40480  @ 74d2a7c545
  datetime_closeout_diff pgrust-fuzz-campaign-1785532932-6eca-38597 @ 74d2a7c545
Proof CONFIRM: timetz part(tz) cluster 4/4 VERIFICATION SUCCESSFUL,
  pgrust-kani-suite-1785530147-689b-29633 @ 263d66d8c8.

TOTAL sloc=4165 fuzz=3449 (82.81%), census_closed=true.
  adt_date      940 measured + 550 exception-carried = 1490/1490 = 100.00%
  adt_datetime 2509 measured + 166 exception-carried = 2675/2675 = 100.00%
Zero owed lines (OWED-UNCOVERED.tsv records the discharge map); zero
exception rows on covered lines (one unreachable-arm adjudication was
DISPROVED by the fleet campaign reaching decode.rs:2833 — row removed,
line measured; the fuzzer beats static analysis, as it should).

Product fixes shipped by the closeout (each docker-18.3 ground-truthed):
timetz_cmp_internal wrapping_add (SQL-reachable debug panic via in_range
window frames), strtod_model long-exact-subnormal acceptance (hex >256
digits / all-decimal, both radixes; 275- and 1076-char witnesses), plus
p1-laney's merged decode.rs/calendar.rs fixes riding the lane merge.
