#!/bin/bash
# run-batch.sh <results-file> <harness>... — strictly serial (one solver at a
# time, mandatory memory protocol). Extra flags via KANI_EXTRA (space-split).
set -u
cd "$(dirname "$0")"
OUT="$1"; shift
for H in "$@"; do
    # shellcheck disable=SC2086
    ./run-one.sh "$H" ${KANI_EXTRA:-} >>"$OUT"
done
echo "BATCH-DONE $*" >>"$OUT"
