#!/usr/bin/env bash
# psql fidelity gate: run gate/corpus.sql through (a) real PGDG psql 18 and
# (b) the Rust psql, against BOTH a stock PostgreSQL 18 server and a pgrust
# server; diff stdout and stderr byte-for-byte after normalization.
#
# Normalizations (each is a justified, inherently nondeterministic value):
#   - "Time: ... ms"        -> "Time: XXX ms"          (\timing wall time)
#   - "PID <n>"             -> "PID NNN"               (backend pid)
#   - " Backend PID ... | n"-> value masked            (\conninfo row)
#
# Usage: run-gate.sh REALPSQL RUSTPSQL SOCKDIR PORT LABEL OUTDIR
set -u
REAL="$1"; RUST="$2"; SOCK="$3"; PORT="$4"; LABEL="$5"; OUT="$6"
HERE="$(cd "$(dirname "$0")" && pwd)"
mkdir -p "$OUT"

normalize() {
    sed -E \
        -e 's/^Time: [0-9.]+ ms( \([^)]*\))?$/Time: XXX ms/' \
        -e 's/PID [0-9]+/PID NNN/' \
        -e 's/^( Backend PID +\| )[0-9]+ *$/\1NNN/'
}

run_client() { # $1 = client binary, $2 = out prefix
    local client="$1" pfx="$2"
    # Fresh, identical database state for every run.
    "$REAL" -h "$SOCK" -p "$PORT" -U pg -d postgres -X -q \
        -c "drop database if exists gate1" \
        -c "drop database if exists gate2" \
        -c "create database gate1" \
        -c "create database gate2" >/dev/null 2>&1
    (cd "$HERE" && "$client" -h "$SOCK" -p "$PORT" -U pg -d gate1 -X \
        < "$HERE/corpus.sql" \
        1> >(normalize > "$pfx.out") \
        2> >(normalize > "$pfx.err"))
    echo "rc=$?" > "$pfx.rc"
    sleep 0.2   # let process-substitution writers finish
}

run_client "$REAL" "$OUT/real-$LABEL"
run_client "$RUST" "$OUT/rust-$LABEL"

fail=0
for stream in out err rc; do
    if ! diff -u "$OUT/real-$LABEL.$stream" "$OUT/rust-$LABEL.$stream" \
            > "$OUT/diff-$LABEL.$stream" 2>&1; then
        echo "DIFF ($LABEL, $stream):"
        cat "$OUT/diff-$LABEL.$stream"
        fail=1
    fi
done
if [ $fail = 0 ]; then
    echo "GATE $LABEL: IDENTICAL (stdout, stderr, exit code)"
else
    echo "GATE $LABEL: DIVERGENT"
fi
exit $fail
