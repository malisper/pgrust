#!/bin/sh
# Paired dshash microbench (rig fat-LTO Rust vs cref.c clang -O3), method
# identical to bench/run.sh: best-of-REPS ns + INSTR=1 two-point perf slope.
# TUNE=neoverse-v2 CREF_CC=clang-16 for Graviton verdicts.
set -eu
cd "$(dirname "$0")"

REPS=${REPS:-5}
BENCHES=${*:-"dshash_find_shared_hit dshash_find_excl_hit dshash_find_miss dshash_fii_hit dshash_insert_delete"}

iters_for() {
    case "$1" in
        dshash_insert_delete) echo 5000000 ;;
        dshash_mt_fii_4t) echo 4000000 ;;
        *) echo 10000000 ;;
    esac
}

TDIR="target${TUNE:+-$TUNE}"
(cd rig && RUSTFLAGS="${TUNE:+-C target-cpu=$TUNE}" CARGO_TARGET_DIR="$TDIR" cargo build --release -q)
${CREF_CC:-clang} -O3 -std=gnu11 -Wall ${TUNE:+-mcpu=$TUNE} -o cref_dshash cref.c
RIG="rig/$TDIR/release/dshash-rig"
CREF=./cref_dshash

if [ "$(uname -s)" = "Darwin" ]; then
    instr_per_op() {
        /usr/bin/time -l "$1" "$2" "$3" 1 2>&1 | awk '/instructions retired/{print $1}'
    }
else
    instr_per_op() {
        perf stat -e instructions:u -x, -- "$1" "$2" "$3" 1 2>&1 >/dev/null \
            | awk -F, '$3 ~ /^instructions/{print $1; exit}'
    }
fi

slope_instr() {
    hi=$(instr_per_op "$1" "$2" "$3")
    lo=$(instr_per_op "$1" "$2" $(($3 / 10)))
    echo "$hi $lo $3" | awk '{printf "%.2f", ($1-$2)/($3 - $3/10)}'
}

printf '%-24s %10s %10s %8s' bench rust_ns c_ns ratio
[ "${INSTR:-0}" = "1" ] && printf ' %12s %12s %8s' rust_instr c_instr i_ratio
printf '\n'

for b in $BENCHES; do
    it=$(iters_for "$b")
    r=$("$RIG" "$b" "$it" "$REPS" | cut -f2)
    if [ "$b" = "dshash_mt_fii_4t" ]; then
        # informational, Rust-only (C ref wait path aborts under contention)
        printf '%-24s %10s %10s %8s\n' "$b" "$r" "n/a" "n/a"
        continue
    fi
    c=$("$CREF" "$b" "$it" "$REPS" | cut -f2)
    ratio=$(echo "$r $c" | awk '{printf "%.2f", $1/$2}')
    printf '%-24s %10s %10s %8s' "$b" "$r" "$c" "$ratio"
    if [ "${INSTR:-0}" = "1" ]; then
        rio=$(slope_instr "$RIG" "$b" "$it")
        cio=$(slope_instr "$CREF" "$b" "$it")
        iratio=$(echo "$rio $cio" | awk '{printf "%.2f", ($2 > 0) ? $1/$2 : 0}')
        printf ' %12s %12s %8s' "$rio" "$cio" "$iratio"
    fi
    printf '\n'
done
