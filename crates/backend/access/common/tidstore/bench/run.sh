#!/bin/sh
# Paired tidstore microbench (rig fat-LTO Rust vs vendored-C clang -O3),
# method identical to bench/run.sh: best-of-REPS ns + INSTR=1 two-point perf
# slope. TUNE=neoverse-v2 CREF_CC=clang-16 for Graviton verdicts.
# C side compiles the UNMODIFIED vendored tidstore.c (+ verbatim
# mcxt/aset/slab/bump) against real PG 18 server headers: PG_INCLUDE, the
# sibling checkout, or pg_config (PGDG on fleet pods).
set -eu
cd "$(dirname "$0")"

REPS=${REPS:-5}
BENCHES=${*:-"tidstore_set_dense tidstore_set_inline tidstore_member_hit tidstore_member_miss tidstore_iterate"}

iters_for() {
    case "$1" in
        tidstore_set_dense) echo 3000000 ;;
        tidstore_set_inline) echo 5000000 ;;
        tidstore_member_*) echo 20000000 ;;
        tidstore_iterate) echo 2000000 ;;
        *) echo 3000000 ;;
    esac
}

PG_INC="${PG_INCLUDE:-}"
[ -z "$PG_INC" ] && [ -f ../../../../../../../pgrust/postgres-18.3/src/include/postgres.h ] && PG_INC=../../../../../../../pgrust/postgres-18.3/src/include
if [ -z "$PG_INC" ] && [ "$(uname -s)" = "Linux" ]; then
    for pc in pg_config /usr/lib/postgresql/*/bin/pg_config; do
        command -v "$pc" >/dev/null 2>&1 || [ -x "$pc" ] || continue
        cand=$("$pc" --includedir-server 2>/dev/null)
        [ -n "$cand" ] && [ -f "$cand/postgres.h" ] && { PG_INC=$cand; break; }
    done
    if [ -z "$PG_INC" ]; then
        for cand in /usr/include/postgresql/*/server; do
            [ -f "$cand/postgres.h" ] && { PG_INC=$cand; break; }
        done
    fi
fi
[ -n "$PG_INC" ] || { echo "no PG include tree found (set PG_INCLUDE)" >&2; exit 1; }
echo "cref: PG includes from $PG_INC"

TDIR="target${TUNE:+-$TUNE}"
(cd rig && RUSTFLAGS="${TUNE:+-C target-cpu=$TUNE}" CARGO_TARGET_DIR="$TDIR" cargo build --release -q)
${CREF_CC:-clang} -O3 -std=gnu11 -Wall -Wno-unused-function ${TUNE:+-mcpu=$TUNE} -I"$PG_INC" \
    -o cref_tidstore cref/main.c cref/shims.c \
    cref/vendor/tidstore.c cref/vendor/mcxt.c cref/vendor/aset.c \
    cref/vendor/slab.c cref/vendor/bump.c cref/vendor/generation.c \
    cref/vendor/alignedalloc.c
RIG="rig/$TDIR/release/tidstore-rig"
CREF=./cref_tidstore

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
