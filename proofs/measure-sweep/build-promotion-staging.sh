#!/usr/bin/env bash
# build-promotion-staging.sh — rebuild suite-promotion-candidates.tsv from the
# full measure sweep PLUS the recipe-fix re-sweep recoveries, and split into
# per-commit candidates (wall <= 60s) and release-gate candidates (> 60s).
#
# STAGING ONLY: no SUITE.tsv tier is edited here — tier adjudication of these
# candidates goes to Michael.
#
# usage: ./build-promotion-staging.sh <resweep-suite-results.tsv>
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
RESWEEP="${1:?path to re-sweep suite-results.tsv}"

OUT_ALL="$HERE/suite-promotion-candidates.tsv"
OUT_PC="$HERE/promotion-staging-le60s.tsv"
OUT_RG="$HERE/promotion-staging-gt60s.tsv"

# Original sweep greens (family, harness, wall_s), minus any row that was
# re-swept (re-sweep verdict supersedes), plus re-sweep greens.
tmp=$(mktemp)
awk -F'\t' 'NR>1 {print $1"\t"$2"\t"$3}' "$OUT_ALL" > "$tmp.orig" || true
# keys re-swept:
awk -F'\t' 'NR>1 {print $1"/"$2}' "$RESWEEP" | sort -u > "$tmp.rekeys"
awk -F'\t' -v K="$tmp.rekeys" '
  BEGIN { while ((getline k < K) > 0) re[k]=1 }
  !( ($1"/"$2) in re )' "$tmp.orig" > "$tmp.kept"
# re-sweep greens: suite-results.tsv columns family harness flags.. outcome wall
# (outcome col 5, wall col 6 per run-kani-suite.sh RESULTS layout)
awk -F'\t' 'NR>1 && $5=="unmeasured-green" {print $1"\t"$2"\t"$6}' "$RESWEEP" > "$tmp.new"

{ echo -e "family\tharness\twall_s"; sort -u "$tmp.kept" "$tmp.new"; } > "$OUT_ALL"
{ echo -e "family\tharness\twall_s"; awk -F'\t' 'NR>1 && $3+0<=60' "$OUT_ALL"; } > "$OUT_PC"
{ echo -e "family\tharness\twall_s"; awk -F'\t' 'NR>1 && $3+0>60'  "$OUT_ALL"; } > "$OUT_RG"

echo "total candidates: $(( $(wc -l < "$OUT_ALL") - 1 ))"
echo "  <=60s (per-commit candidates):   $(( $(wc -l < "$OUT_PC") - 1 ))"
echo "  >60s  (release-gate candidates): $(( $(wc -l < "$OUT_RG") - 1 ))"
echo "  re-sweep recovered greens:       $(wc -l < "$tmp.new" | tr -d ' ')"
rm -f "$tmp" "$tmp".*
