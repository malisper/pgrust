# Shared setup for the objkv end-to-end scripts. Sourced by each one.
#
# Prerequisites:
#   - the server built with the object-store client, which is off by default:
#         cargo build --bin postgres --features objkv-s3
#   - C PostgreSQL's initdb, psql and pg_config on PATH; python3
#   - an S3-compatible store. Default: MinIO at 127.0.0.1:9000 with
#     minioadmin / minioadmin. The bucket is created if missing.
#
# Every script empties the bucket, builds its own cluster under
# OBJKV_TEST_ROOT (default /tmp/objkv-tests/<script>) and stops its server on
# exit. Do not point OBJKV_S3_BUCKET at data you want, and do not run two
# scripts at once against one bucket. ./run_all.sh runs the whole set;
# `cargo test -p objkv -- --ignored e2e` does the same from cargo.
#
# Knobs: BIN, PORT, PGDATA, SOCKDIR, OBJKV_S3_ENDPOINT / BUCKET / KEY / SECRET,
# OBJKV_TEST_ROOT, OBJKV_TIMING=1 to include the load-sensitive timing checks.

set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NAME="$(basename "$0" .sh)"
ROOT="${OBJKV_TEST_ROOT:-/tmp/objkv-tests}"
WORK="$ROOT/$NAME"
PGDATA="${PGDATA:-$WORK/pgdata}"
SOCKDIR="${SOCKDIR:-$WORK/sock}"
LOG="${LOG:-$WORK/server.log}"
BIN="${BIN:-$HERE/../../../../target/debug/postgres}"

# One port per script. Nothing listens on TCP (listen_addresses is empty), but
# the port names the socket file and shared-memory key, so two servers on one
# machine must differ.
objkv_default_port() {
    case "$NAME" in
        alter_table)     echo 5401;;  bitmap)         echo 5402;;
        collection)      echo 5403;;  concurrency)    echo 5404;;
        database_scope)  echo 5405;;  finish_line)    echo 5406;;
        in_list)         echo 5407;;  index_demo)     echo 5408;;
        index_only)      echo 5409;;  lift)           echo 5410;;
        null_search)     echo 5411;;  oid_floor)      echo 5412;;
        ordinary_sql)    echo 5413;;  ranges)         echo 5414;;
        snapshot_timing) echo 5415;;  stale_entries)  echo 5416;;
        torn_commit)     echo 5417;;  truncate)       echo 5418;;
        group_commit)    echo 5419;;  stale_lift)     echo 5420;;
        partial_expression) echo 5421;;  analyze)     echo 5422;;
        last_commit)     echo 5423;;  desc_index)     echo 5424;;
        *)               echo 5488;;
    esac
}
PORT="${PORT:-$(objkv_default_port)}"

export PGRUST_TZDIR="${PGRUST_TZDIR:-$(pg_config --sharedir)/timezone}"
export OBJKV_S3_ENDPOINT="${OBJKV_S3_ENDPOINT:-http://127.0.0.1:9000}"
export OBJKV_S3_BUCKET="${OBJKV_S3_BUCKET:-objkv}"
export OBJKV_S3_KEY="${OBJKV_S3_KEY:-minioadmin}"
export OBJKV_S3_SECRET="${OBJKV_S3_SECRET:-minioadmin}"

RC=0
OBJKV_PIDS=""

# --- reporting -----------------------------------------------------------------

check()  { if [ "$2" = "$3" ]; then echo "  ok: $1"; else echo "  FAIL: $1 -- wanted [$2], got [$3]"; RC=1; fi; }
ok()     { echo "  ok: $1"; }
fail()   { echo "  FAIL: $1"; RC=1; }
# contains <what> <needle> <haystack>
contains() {
    case "$3" in
        *"$2"*) echo "  ok: $1";;
        *) echo "  FAIL: $1 -- no \"$2\" in: $(echo "$3" | head -3)"; RC=1;;
    esac
}
die()    { echo "  FAIL: $1" >&2; [ -n "${2:-}" ] && echo "$2" | head -5 >&2; exit 1; }
finish() { if [ "$RC" = 0 ]; then echo "PASS: ${1:-$NAME}"; else echo "FAIL: ${1:-$NAME}"; fi; exit "$RC"; }
timing_enabled() { [ "${OBJKV_TIMING:-0}" != 0 ]; }

# --- talking to the server ---------------------------------------------------

psqlx()  { psql -h "$SOCKDIR" -p "$PORT" -X "$@"; }
sql()    { psqlx -d "${DB:-postgres}" -tAc "$1" 2>&1; }
sql_in() { psqlx -d "$1" -tAc "$2" 2>&1; }
# A whole transaction down one connection, from stdin.
txn()    { psqlx -d "${DB:-postgres}" -tA 2>&1; }
# psql prints a command tag per statement, so the answer marks itself as
# 'RESULT=...' and this picks it out.
last()   { grep '^RESULT=' | tail -1 | cut -d= -f2-; }
# A setup step that has to work, or later checks compare two error messages.
must() {
    local out; out=$(sql "$1")
    case "$out" in ERROR*|*"could not connect"*|*"Connection refused"*) die "$1" "$out";; esac
    printf '%s' "$out"
}
dbs()    { sql "SELECT datname FROM pg_database WHERE datallowconn ORDER BY oid;"; }
am_of()  { sql "SELECT a.amname FROM pg_class c JOIN pg_am a ON a.oid = c.relam WHERE c.relname = '$1';"; }

# --- steering the planner ----------------------------------------------------
#
# idx/plan force the index side; IDX_OPTS adds settings (e.g. -c
# enable_bitmapscan=off). tbl forces a table read, which is the reference:
# with the index allowed the planner still prefers it, and comparing the
# index with itself proves nothing.
IDX_OPTS=""
idx()   { PGOPTIONS="${PGOPTIONS:-} -c enable_seqscan=off $IDX_OPTS" psqlx -d "${DB:-postgres}" -tAc "$1" 2>&1; }
plan()  { PGOPTIONS="${PGOPTIONS:-} -c enable_seqscan=off $IDX_OPTS" psqlx -d "${DB:-postgres}" -tAc "EXPLAIN $1" 2>&1; }
tbl()   { PGOPTIONS="${PGOPTIONS:-} -c enable_indexscan=off -c enable_bitmapscan=off -c enable_indexonlyscan=off" psqlx -d "${DB:-postgres}" -tAc "$1" 2>&1; }
agree() { check "$1" "$(tbl "$2")" "$(idx "$2")"; }
# shows <what> <node> <plan text>
shows() {
    if echo "$3" | grep -q "$2"; then echo "  ok: $1"
    else echo "  FAIL: $1 -- no \"$2\" in the plan"; echo "$3" | head -6; RC=1; fi
}
nosort() {
    local p; p=$(plan "$2")
    if echo "$p" | grep -q "Sort"; then echo "  FAIL: $1 -- still sorting"; echo "$p" | head -4; RC=1
    else echo "  ok: $1"; fi
}

# --- the scan trace ------------------------------------------------------------
#
# With PGRUST_OBJKV_TRACE=1 every index scan logs
#   OBJKVTRACE index_scan ... candidates=N kept=M
# (objkv_index.rs, load_scan). trace_mark remembers where the log was;
# trace_* read what came after.
trace_mark()  { TRACE_MARK=$(grep -ac "OBJKVTRACE index_scan" "$LOG" 2>/dev/null || true); TRACE_MARK=${TRACE_MARK:-0}; }
trace_since() { grep -a "OBJKVTRACE index_scan" "$LOG" 2>/dev/null | tail -n +$((${TRACE_MARK:-0} + 1)); }
trace_candidates()     { trace_since | sed -E 's/.*candidates=([0-9]+).*/\1/' | awk '{s += $1} END {print s + 0}'; }
trace_max_candidates() { trace_since | sed -E 's/.*candidates=([0-9]+).*/\1/' | sort -rn | head -1; }
trace_scans()          { grep -ac "OBJKVTRACE index_scan" "$LOG" 2>/dev/null || echo 0; }
# Scans whose candidate and kept counts differ, over the whole log.
trace_mismatches() {
    grep -a "OBJKVTRACE index_scan" "$LOG" 2>/dev/null \
        | sed -E 's/.*candidates=([0-9]+) kept=([0-9]+).*/\1 \2/' | awk '$1 != $2' | wc -l | tr -d ' '
}

# --- the server ----------------------------------------------------------------

objkv_boot() {  # objkv_boot <pgdata> [logfile]
    local data="$1" log="${2:-$LOG}"
    mkdir -p "$SOCKDIR" "$(dirname "$log")"
    # autovacuum off: its ANALYZE writes pg_statistic rows, and between two
    # lifts that is exactly the write the lift's counter check refuses. Right
    # for an operator, a coin toss for a test.
    "$BIN" -D "$data" -k "$SOCKDIR" -p "$PORT" -c listen_addresses='' -c autovacuum=off &>"$log" &
    OBJKV_PIDS="$OBJKV_PIDS $!"
}

# Polls until a connection works, or the server dies, or 90s pass.
objkv_wait_ready() {
    local pid="${OBJKV_PIDS##* }" i
    for i in $(seq 1 360); do
        psqlx -d postgres -tAc "SELECT 1" >/dev/null 2>&1 && return 0
        kill -0 "$pid" 2>/dev/null || die "the server exited during startup" "$(tail -20 "$LOG" 2>/dev/null)"
        sleep 0.25
    done
    die "the server did not accept connections within 90s" "$(tail -20 "$LOG" 2>/dev/null)"
}

boot() { objkv_boot "${1:-$PGDATA}"; objkv_wait_ready; }   # boot [pgdata]
stop() { objkv_stop "${1:-TERM}"; }                          # stop [TERM|KILL]

objkv_stop() {  # objkv_stop [TERM|KILL]
    local sig="${1:-KILL}" pid waited limit=60 stubborn=0
    # Runs from a trap on EXIT, so every step tolerates failure.
    for pid in $OBJKV_PIDS; do
        kill -"$sig" "$pid" 2>/dev/null || true
    done
    for pid in $OBJKV_PIDS; do
        waited=0
        while kill -0 "$pid" 2>/dev/null && [ "$waited" -lt "$limit" ]; do
            sleep 1
            waited=$((waited + 1))
        done
        if kill -0 "$pid" 2>/dev/null; then
            # A killed server drops its last commit, so escalating changes the
            # subject: recorded, and failed below.
            [ "$sig" = KILL ] || stubborn=1
            kill -9 "$pid" 2>/dev/null || true
        fi
        wait "$pid" 2>/dev/null || true
    done
    OBJKV_PIDS=""
    sleep 1  # the socket outlives the process, and the next boot binds it
    if [ "$stubborn" = 1 ]; then
        echo "  FAIL: the server did not shut down within ${limit}s of SIGTERM; it was killed," >&2
        echo "        and a killed server drops its last commit. Treating it as a failure." >&2
        trap - EXIT  # we are inside that trap; exiting would re-enter it
        exit 1
    fi
    return 0
}

objkv_require_port() {
    mkdir -p "$SOCKDIR"
    if psqlx -d postgres -tAc "SELECT 1" >/dev/null 2>&1; then
        echo "  FAIL: something is already listening on $SOCKDIR port $PORT." >&2
        echo "        This script will not stop it -- it is not ours. Stop it, or" >&2
        echo "        re-run with SOCKDIR= and PORT= set somewhere else." >&2
        exit 1
    fi
}

# --- building a cluster --------------------------------------------------------

# An empty bucket, a fresh initdb directory, a running server, and both
# objkv access methods in every database.
fresh_cluster() {
    objkv_require_port
    trap stop EXIT
    "$HERE/bucket.py" mk >/dev/null || die "cannot reach the bucket at $OBJKV_S3_ENDPOINT"
    "$HERE/bucket.py" rm >/dev/null || die "cannot empty the bucket"
    rm -rf "$PGDATA"
    mkdir -p "$WORK"
    initdb -D "$PGDATA" -U "$(id -un)" >"$WORK/initdb.log" 2>&1 \
        || die "initdb failed" "$(tail -5 "$WORK/initdb.log")"
    boot
    install_ams
}

# A second blank directory beside the first, with only the marker copied in.
blank_directory() {  # blank_directory <dir>
    rm -rf "$1"
    initdb -D "$1" -U "$(id -un)" >"$WORK/initdb2.log" 2>&1 \
        || die "initdb failed" "$(tail -5 "$WORK/initdb2.log")"
    cp "$PGDATA/objkv_catalogs" "$1/objkv_catalogs" || die "no marker to copy"
}

install_ams() {
    local db
    for db in $(dbs); do
        sql_in "$db" "CREATE ACCESS METHOD objkv TYPE TABLE HANDLER heap_tableam_handler;" >/dev/null 2>&1
        sql_in "$db" "CREATE ACCESS METHOD objkv_btree TYPE INDEX HANDLER bthandler;" >/dev/null 2>&1
    done
}

install_lift() {
    local db f
    for db in $(dbs); do
        for f in pgrust_objkv_lift pgrust_objkv_lift_verify pgrust_objkv_lift_finish; do
            sql_in "$db" "CREATE FUNCTION $f() RETURNS text AS '$f' LANGUAGE internal;" >/dev/null 2>&1
        done
    done
}

# Lifts every database and flips. The server keeps its pre-flip view until
# it is restarted; callers do that.
lift_all() {
    local db out
    for db in $(dbs); do
        out=$(sql_in "$db" "SELECT pgrust_objkv_lift();")
        echo "$out" | grep -q relations || die "lifting $db" "$out"
    done
    out=$(sql "SELECT pgrust_objkv_lift_finish();")
    echo "$out" | grep -q "catalogs are in the bucket" || die "finish" "$out"
}
