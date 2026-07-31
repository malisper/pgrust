#!/usr/bin/env bash
# Increment-2 verification: run gate/corpus-wasm.sql through the Rust psql
#   (N) natively against a native pgrust server (fresh initdb cluster), and
#   (W) as psql.wasm cross-piped to postgres --stdio-wire wasm (two wasm
#       instances under Node via tools/wasm-web/run-node-psql.mjs),
# then diff stdout/stderr byte-for-byte.
#
# Normalizations (justified, environment-inherent):
#   - "Time: ... ms"                          \timing wall time
#   - "PID <n>" / conninfo Backend PID row    backend pid
#   - conninfo "Server Port" row              native arm picks a free port;
#                                             the wasm arm's port is notional
#   - "NOTICE:  database system was shut down" lines on stderr — the wasm arm
#     boots one server INSTANCE PER CONNECTION (\c respawns it on the same
#     VFS), and each boot relays the recovery notice; a long-running native
#     server has no analog.
#
# Usage: run-wasm-gate.sh [OUTDIR]   (from anywhere; paths are self-rooted)
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
OUT="${1:-/tmp/psql-wasm-gate}"
PORT=5442
PGBIN=""
for cand in "${PGINSTALL:-}" /opt/homebrew/opt/postgresql@18/bin /opt/homebrew/bin /tmp/pgrust_pginstall/bin; do
    [ -n "$cand" ] && [ -x "$cand/initdb" ] && PGBIN="$cand" && break
done
[ -n "$PGBIN" ] || { echo "no PostgreSQL 18 initdb found (set PGINSTALL)"; exit 2; }
mkdir -p "$OUT"

normalize() {
    sed -E \
        -e 's/^Time: [0-9.]+ ms( \([^)]*\))?$/Time: XXX ms/' \
        -e 's/PID [0-9]+/PID NNN/' \
        -e 's/^( Backend PID +\| )[0-9]+ *$/\1NNN/' \
        -e 's/^( Server Port +\| )[0-9]+ *$/\1NNNN/' \
        -e '/^NOTICE:  database system was shut down$/d'
}

# ---- arm N: native ---------------------------------------------------------
DD="$OUT/native-datadir"
rm -rf "$DD"
"$PGBIN/initdb" -D "$DD" --no-locale --encoding=UTF8 -U postgres -A trust \
    > "$OUT/initdb.log" 2>&1 || { echo "initdb failed"; tail -5 "$OUT/initdb.log"; exit 2; }
for tz in "$PGBIN/../share/postgresql/timezone" "$PGBIN/../share/postgresql@18/timezone"; do
    [ -d "$tz" ] && export PGRUST_TZDIR="$tz" && export PGRUST_PGSHAREDIR="$(dirname "$tz")" && break
done
ulimit -s 65520 2>/dev/null
(RUST_MIN_STACK=67108864 exec "$ROOT/target/release/postgres" -D "$DD" -k /tmp -p "$PORT" \
    -c max_stack_depth=60000 -c io_method=sync -c autovacuum=off \
    -c wal_sync_method=fdatasync -c shared_buffers=32MB \
    -c timezone=UTC -c log_timezone=UTC > "$OUT/native-server.log" 2>&1) &
SRV=$!
for _ in $(seq 1 40); do
    kill -0 $SRV 2>/dev/null || { echo "native server died"; tail -10 "$OUT/native-server.log"; exit 2; }
    "$ROOT/target/release/psql" -h /tmp -p "$PORT" -U postgres -d postgres -X -c 'select 1' >/dev/null 2>&1 && break
    sleep 0.5
done
(cd "$HERE" && "$ROOT/target/release/psql" -h /tmp -p "$PORT" -U postgres -d postgres -X \
    < "$HERE/corpus-wasm.sql" \
    1> >(normalize > "$OUT/native.out") \
    2> >(normalize > "$OUT/native.err"))
echo "rc=$?" > "$OUT/native.rc"
sleep 0.3
kill -INT $SRV 2>/dev/null
wait $SRV 2>/dev/null

# ---- arm W: wasm x wasm ----------------------------------------------------
(cd "$ROOT/tools/wasm-web" && timeout 900 node run-node-psql.mjs \
    --script "$HERE/corpus-wasm.sql" \
    --vfs-file "$HERE/gate-include.sql:/gate-include.sql" \
    --out "$OUT/wasm.raw.out" --err "$OUT/wasm.raw.err" \
    > "$OUT/wasm-harness.log" 2>&1)
echo "rc=$?" > "$OUT/wasm.rc"
normalize < "$OUT/wasm.raw.out" > "$OUT/wasm.out"
normalize < "$OUT/wasm.raw.err" > "$OUT/wasm.err"

fail=0
for stream in out err rc; do
    if ! diff -u "$OUT/native.$stream" "$OUT/wasm.$stream" > "$OUT/diff.$stream" 2>&1; then
        echo "DIFF ($stream):"; cat "$OUT/diff.$stream"; fail=1
    fi
done
if [ $fail = 0 ]; then
    echo "WASM GATE: IDENTICAL to native (stdout, stderr, exit code)"
else
    echo "WASM GATE: DIVERGENT"
fi
exit $fail
