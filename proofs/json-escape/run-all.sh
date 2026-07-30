#!/bin/zsh
# json-escape census runner (kissat arm).
# STATUS: Kani verdicts for this family are BLOCKED — the CBMC 6.8 silent
# self-abort (status 15 after 8-25min at 2-4GB) kills the census harnesses;
# the len<=4 census was adjudicated by NATIVE DIFFERENTIAL instead (src/bin/*,
# TRIAGE "JSON ESCAPE CENSUS"). This script is kept for re-probing after a
# CBMC upgrade; do not treat its timeouts as verdicts.
# census_len1_cstr is the expected-fail NUL-truncation witness.
cd "$(dirname "$0")"
for h in census_len1_with_len census_len1_cstr census_len1_cstr_r2 census_len2_with_len; do
  echo "=== HARNESS $h start $(date +%H:%M:%S)"
  timeout 3000 ~/.cargo/bin/cargo-kani kani -Z c-ffi --c-lib c/pg_escape.c --solver kissat --harness $h > run-$h.log 2>&1
  echo "=== HARNESS $h exit=$? end $(date +%H:%M:%S)"
  grep -E "VERIFICATION|Failed Checks|verification time" run-$h.log | tail -6
done
echo ALLDONE
