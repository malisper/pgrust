#!/bin/zsh
# runner: own-process-group timeout discipline (skill trap: never pkill cbmc by name)
cd "$(dirname "$0")"
h=$1; t=${2:-400}; solver=${3:-kissat}
if [ "$solver" = "default" ]; then sflag=""; else sflag="--solver $solver"; fi
timeout $t ~/.cargo/bin/cargo-kani kani -Z c-ffi -Z stubbing --c-lib c/pg_numeric_cmp.c --c-lib c/pg_numeric_rows.c ${=sflag} --harness proofs::$h --exact 2>&1 | grep -E "VERIFICATION:|Verification Time|Failed Checks|unwinding assertion|Complete - |cover" | head -20
