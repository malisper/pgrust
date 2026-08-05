#!/bin/bash
# run-smoke.sh <joblist.tsv> <timeout-s> <ledger>
# Same method as run-kani-coverage.sh: for each (family, suite-harness-name)
# take that row's exact flags from SUITE.tsv, append `--coverage
# -Z source-coverage`, one harness per invocation. Adds the `proofs::`
# qualification that --exact requires for the families whose SUITE rows store
# unqualified names.
set -u
JOBS=$1; T=$2; LEDGER=$3
DIR="$(cd "$(dirname "$0")" && pwd)"
SUITE="$DIR/../SUITE.tsv"
while IFS=$'\t' read -r FAM H KHIN; do
    [ -z "${FAM:-}" ] && continue
    FLAGS=$(awk -F'\t' -v f="$FAM" -v h="$H" '$1==f && $2==h && $5=="per-commit" {print $3; exit}' "$SUITE")
    if [ -z "$FLAGS" ]; then echo "NOFLAGS $FAM $H" >> "$LEDGER"; continue; fi
    # Kani --exact needs the module-qualified name; SUITE stores several rows
    # unqualified. Column 3 of the joblist overrides the default `proofs::`
    # prefix (e.g. the `bool` family's harnesses live in `mod harnesses`).
    if [ -n "${KHIN:-}" ]; then KH="$KHIN"
    else case "$H" in *::*) KH="$H";; *) KH="proofs::$H";; esac; fi
    case "$FLAGS" in *--exact*) EX="";; *) EX="--exact";; esac
    LOG="/tmp/kanicov-$FAM-${KH//:/_}.log"
    start=$(date +%s)
    ( cd "$DIR/../$FAM" && exec timeout "$T" cargo kani $FLAGS --coverage -Z source-coverage \
        --harness "$KH" $EX ) > "$LOG" 2>&1
    rc=$?
    end=$(date +%s)
    verdict=$(grep -Eo "VERIFICATION:- [A-Z]+" "$LOG" | tail -1)
    if [ $rc -eq 124 ]; then verdict="WALLED-TIMEOUT-${T}s"; fi
    echo "RUN $FAM $KH rc=$rc wall=$((end-start))s ${verdict:-NO-VERDICT}" >> "$LEDGER"
    if [ $rc -ne 0 ]; then
        grep -E "error(\[|:)|CBMC failed|Failed Checks|out of memory" "$LOG" | head -2 \
          | sed 's/^/    /' >> "$LEDGER"
    fi
done < "$JOBS"
echo "DONE $(date -u +%FT%TZ)" >> "$LEDGER"
