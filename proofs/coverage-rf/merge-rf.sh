#!/bin/bash
# merge-rf.sh — merge the REGRESS + FUZZ axes tree-wide (848-crate scope)
# into proofs/coverage-rf/ under the ADOPTED denominator (SLOC v2 +
# const/data tables excluded). The Kani axis is deliberately EMPTY here —
# it is captured by the concurrent proofs/coverage-fulltree lane; the
# empty census (census-empty.tsv, 0 expected harnesses) closes trivially
# and is correct, not an omission.
#
# THREE-AXIS JOIN (run by whichever lane lands second, on a tree that has
# BOTH proofs/coverage/fulltree/ and proofs/coverage-rf/):
#
#   CENSUS_ARGS=(); for c in proofs/coverage/fulltree/census/census-*.tsv; do CENSUS_ARGS+=(--census "$c"); done
#   LCOV_ARGS=(); for l in proofs/coverage-rf/*.lcov proofs/coverage-rf/*.lcov.gz; do
#     case "$l" in *.gz) gunzip -k "$l"; l="${l%.gz}";; esac
#     [ -f "$l" ] && LCOV_ARGS+=("$l"); done
#   python3 proofs/coverage/merge-coverage.py \
#     --kani-glob 'proofs/coverage/fulltree/kaniraw/**/*kaniraw.json' \
#     "${CENSUS_ARGS[@]}" \
#     --allow-unmeasured proofs/coverage/fulltree/waivers.tsv \
#     --allow-unmeasured proofs/coverage/fulltree/waivers-walled.tsv \
#     --fuzz-lcov proofs/coverage-rf/fuzz-float_in_diff.lcov \
#     --fuzz-lcov proofs/coverage-rf/fuzz-float_out_diff.lcov \
#     --fuzz-lcov proofs/coverage-rf/fuzz-geo_diff.lcov \
#     --regress-lcov proofs/coverage-rf/regress.lcov \
#     --line-table-lcov proofs/coverage-rf/regress.lcov \
#     --line-table-lcov proofs/coverage-rf/fuzz-float_in_diff.lcov \
#     --line-table-lcov proofs/coverage-rf/fuzz-float_out_diff.lcov \
#     --line-table-lcov proofs/coverage-rf/fuzz-geo_diff.lcov \
#     --scope proofs/coverage/fulltree/scope-fulltree.txt \
#     --outdir proofs/coverage
#
# (generate waivers-walled.tsv first via proofs/coverage/fulltree/merge-fulltree.sh's
# awk stanza if it is not committed.)
set -eu
HERE="$(cd "$(dirname "$0")" && pwd)"
COV="$HERE/../coverage"
for f in "$HERE"/*.lcov.gz; do [ -f "$f" ] && gunzip -kf "$f"; done
python3 "$COV/merge-coverage.py" \
  --census "$HERE/census-empty.tsv" \
  --fuzz-lcov "$HERE/fuzz-float_in_diff.lcov" \
  --fuzz-lcov "$HERE/fuzz-float_out_diff.lcov" \
  --fuzz-lcov "$HERE/fuzz-geo_diff.lcov" \
  --regress-lcov "$HERE/regress.lcov" \
  --line-table-lcov "$HERE/regress.lcov" \
  --line-table-lcov "$HERE/fuzz-float_in_diff.lcov" \
  --line-table-lcov "$HERE/fuzz-float_out_diff.lcov" \
  --line-table-lcov "$HERE/fuzz-geo_diff.lcov" \
  --scope "$HERE/scope-fulltree.txt" \
  --outdir "$HERE" \
  "$@"
