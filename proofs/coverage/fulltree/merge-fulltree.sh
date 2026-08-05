#!/bin/bash
# merge-fulltree.sh — final merge of the full-tree Kani capture.
#   1. generates wall waivers from the per-family censuses (reason records the
#      measured timeout — a wall is unmeasured coverage, per COVERAGE.md);
#   2. runs merge-coverage.py over ALL banked kaniraw + censuses + waivers,
#      full-tree scope, writing proofs/coverage/ (summary.json, files/, tsv,
#      census.json).
# Exits nonzero if the merge refuses (census not closed) — fix and re-run the
# affected family; never hand-edit a census.
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
COV="$HERE/.."

# wall waivers: every WALLED row, reason = measured timeout
awk -F'\t' 'FNR>1 && $7=="WALLED" {
  printf "%s\t%s\twalled under --coverage at %ss (measured timeout; coverage instrumentation defeats formula slicing on string-heavy harnesses — COVERAGE.md known distortion 1)\n", $1, $2, $5
}' "$HERE"/census/census-*.tsv > "$HERE/waivers-walled.tsv"
echo "wall waivers: $(wc -l < "$HERE/waivers-walled.tsv")"

# completeness: every family joblist row must have a census row (the merge
# census cannot see the joblist, so a runner killed mid-family would otherwise
# leave a silent hole — observed once: misc-ops SIGTERM rc=143, 1/5 rows).
MISSING=0
for jl in "$HERE"/joblists/*.tsv; do
    fam=$(basename "$jl" .tsv)
    cf="$HERE/census/census-$fam.tsv"
    want=$(awk -F'\t' 'NF>=2 && $1!=""' "$jl" | wc -l)
    have=0; [ -f "$cf" ] && have=$(awk 'NR>1' "$cf" | wc -l)
    if [ "$want" -ne "$have" ]; then
        echo "INCOMPLETE FAMILY $fam: joblist $want rows, census $have" >&2
        MISSING=1
    fi
done
[ "$MISSING" -eq 1 ] && { echo "FATAL: incomplete families — re-run them before merging" >&2; exit 4; }

CENSUS_ARGS=()
for c in "$HERE"/census/census-*.tsv; do CENSUS_ARGS+=(--census "$c"); done

# THREE-AXIS JOIN (per proofs/coverage-rf/merge-rf.sh), adopted denominator:
# SLOC v2 + const tables excluded (defaults of the ccd2d20b22 tooling), with
# the rf lcov files REMAPPED onto this tree (remap-lcov.py; rf was measured at
# ccd2d20b22's crates content).
RF="$HERE/rf-remap"
python3 "$COV/merge-coverage.py" \
  --kani-glob "$HERE/kaniraw/**/*kaniraw.json" \
  "${CENSUS_ARGS[@]}" \
  --allow-unmeasured "$HERE/waivers.tsv" \
  --allow-unmeasured "$HERE/waivers-walled.tsv" \
  --fuzz-lcov "$RF/fuzz-float_in_diff.lcov" \
  --fuzz-lcov "$RF/fuzz-float_out_diff.lcov" \
  --fuzz-lcov "$RF/fuzz-geo_diff.lcov" \
  --regress-lcov "$RF/regress.lcov" \
  --line-table-lcov "$RF/regress.lcov" \
  --line-table-lcov "$RF/fuzz-float_in_diff.lcov" \
  --line-table-lcov "$RF/fuzz-float_out_diff.lcov" \
  --line-table-lcov "$RF/fuzz-geo_diff.lcov" \
  --sloc-rule v2 --exclude-const-tables \
  --scope "$HERE/scope-fulltree.txt" \
  --outdir "$COV" "$@"
