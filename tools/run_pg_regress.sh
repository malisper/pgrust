#!/usr/bin/env bash
#
# run_pg_regress.sh — drive PostgreSQL's pg_regress harness against our Rust
# `postgres` binary, against ONE persistent instance.
#
# Persistent-instance model (the faithful pg_regress model)
# ---------------------------------------------------------
# Earlier this harness re-`initdb`'d and restarted a fresh postmaster before
# EVERY test, because any test that hit an unported path crashed the backend
# and left the client socket half-open (psql hung), so one crash cascaded.
#
# That crash-resilience is now in place: the backend converts an unported-path
# panic / seam-miss into a recoverable SQL ERROR, re-arms ReadyForQuery after
# recovery, and a caught error no longer poisons the rest of the session
# (active-portal / double-panic-abort fixes). So we can now run the way real
# pg_regress does:
#
#   * initdb ONCE,
#   * start ONE postmaster ONCE,
#   * run ALL requested tests in sequence against that single live server
#     (pg_regress --use-existing --dbname=postgres), each test bounded by a
#     per-test wall-clock watchdog that kills only the hung pg_regress/psql
#     (NEVER the postmaster),
#   * stop the postmaster ONCE at the end.
#
# Tests therefore build on each other (a CREATE TABLE in one test is visible to
# the next) exactly as in a real regression run, and the run is far faster (no
# per-test ~seconds initdb + restart).
#
# Why --use-existing / --dbname=postgres
# --------------------------------------
# pg_regress normally runs `initdb` and `CREATE DATABASE regression` before any
# test. We can't bootstrap-`initdb` from our own binary (the C initdb resolves
# `postgres` relative to its real path), and `CREATE DATABASE` is still an
# unported path. `--use-existing --dbname=postgres` SKIPS both: tests run
# against the `postgres` database the C initdb created.
#
# Necessary GUC knobs:
#   * io_method=sync          — worker IO is unported (pgaio_worker_shmem panics)
#   * max_stack_depth=7000    — deep recursion in our interpreter
#   * fsync=off / -F          — speed only
#   * listen_addresses=''     — unix socket only
#   * PGOPTIONS=-c client_min_messages=error
#         — suppress the per-connection `WARNING: resource was not closed`
#           spew that otherwise pollutes every diff.
#   * --port=5432             — our postmaster ignores PGPORT (defaults to 5432).
#
# Usage:
#   tools/run_pg_regress.sh [test ...]
#   (default test set: smoke boolean char name int4 int8)
#
# Env overrides:
#   PGRUST_PGINSTALL   C install root (default /tmp/pgrust_pginstall)
#   PGRUST_PGSHAREDIR  share dir for the build (default /tmp/pgrust_share)
#   REGRESS_SRC        PG source regress dir (sql/ + expected/)
#   SKIP_BUILD=1       reuse an existing stable binary copy
#   PER_TEST_TIMEOUT   seconds before a hung test is killed (default 60)
#
set -u

PGINSTALL="${PGRUST_PGINSTALL:-/tmp/pgrust_pginstall}"
PGSHAREDIR="${PGRUST_PGSHAREDIR:-/tmp/pgrust_share}"
NODETAGS_H="${PGRUST_NODETAGS_H:-/Users/malisper/workspace/work/pgrust/postgres-18.3/src/backend/nodes/nodetags.h}"
REGRESS_SRC="${REGRESS_SRC:-/Users/malisper/workspace/work/pgrust/postgres-18.3/src/test/regress}"
PER_TEST_TIMEOUT="${PER_TEST_TIMEOUT:-60}"

# Resolve worktree root (this script lives in <root>/tools/).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PGREGRESS="$PGINSTALL/lib/postgresql/pgxs/src/test/regress/pg_regress"
WORK=/tmp/pgrust-regress-harness
BIN_STABLE="$WORK/postgres"          # crash-churn-proof copy of our binary
BINDIR="$WORK/bindir"                # C tools + OUR postgres
SOCK="$WORK/sock"
DATA="$WORK/data"
CONF="$WORK/regress.conf"
OUT="$WORK/out"
SQLDIR="$WORK/sql-input"             # holds our hand-written smoke test

mkdir -p "$WORK" "$OUT"

# --- 0. sanity ---------------------------------------------------------------
[ -x "$PGREGRESS" ] || { echo "FATAL: pg_regress not found at $PGREGRESS"; exit 1; }
[ -x "$PGINSTALL/bin/initdb" ] || { echo "FATAL: C initdb not found"; exit 1; }

# --- 1. build our postgres ---------------------------------------------------
if [ "${SKIP_BUILD:-0}" != "1" ]; then
  echo "## building our postgres ..."
  ( cd "$ROOT" && CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-10}" \
      PGRUST_PGSHAREDIR="$PGSHAREDIR" PGRUST_NODETAGS_H="$NODETAGS_H" \
      cargo build --bin postgres ) || { echo "FATAL: build failed"; exit 1; }
  # Copy out of target/ — concurrent agents churn /tmp worktrees and can wipe it.
  cp "$ROOT/target/debug/postgres" "$BIN_STABLE" || { echo "FATAL: copy binary"; exit 1; }
fi
[ -x "$BIN_STABLE" ] || { echo "FATAL: no stable binary at $BIN_STABLE (run without SKIP_BUILD)"; exit 1; }

# --- 2. bindir: C tools + OUR postgres --------------------------------------
mkdir -p "$BINDIR"
for f in initdb psql pg_ctl createdb pg_isready pg_config; do
  ln -sf "$PGINSTALL/bin/$f" "$BINDIR/$f"
done
ln -sf "$BIN_STABLE" "$BINDIR/postgres"

# --- 3. temp-config GUCs -----------------------------------------------------
cat > "$CONF" <<EOF
io_method = sync
max_stack_depth = 7000
listen_addresses = ''
fsync = off
EOF

# --- 4. smoke test fixture ---------------------------------------------------
mkdir -p "$SQLDIR/sql" "$SQLDIR/expected"
printf 'SELECT 1;\n' > "$SQLDIR/sql/smoke.sql"
# NOTE: psql pads the column header with a trailing space — it MUST be present
# in the expected file or the diff fails on whitespace.
{
  printf '%s\n' 'SELECT 1;'
  printf '%s\n' ' ?column? '
  printf '%s\n' '----------'
  printf '%s\n' '        1'
  printf '%s\n' '(1 row)'
  printf '\n'
} > "$SQLDIR/expected/smoke.out"

# --- helpers -----------------------------------------------------------------
PM_PID=""

start_pm_once() {
  # Start the ONE persistent postmaster on a freshly-initdb'd cluster.
  pkill -9 -f "$BIN_STABLE" 2>/dev/null; sleep 1
  rm -rf "$SOCK" "$DATA"; mkdir -p "$SOCK"
  echo "## initdb (once) ..."
  "$BINDIR/initdb" -D "$DATA" --no-locale --encoding=UTF8 -U postgres \
      > "$OUT/initdb.log" 2>&1 || { echo "  initdb failed"; tail "$OUT/initdb.log"; return 1; }
  cat "$CONF" >> "$DATA/postgresql.conf"
  echo "## starting ONE persistent postmaster ..."
  "$BIN_STABLE" -D "$DATA" -F -c "listen_addresses=" -k "$SOCK" -p 5432 \
      > "$OUT/postmaster.log" 2>&1 &
  PM_PID=$!
  for _ in $(seq 1 60); do
    "$PGINSTALL/bin/pg_isready" -h "$SOCK" -p 5432 >/dev/null 2>&1 && { echo "  ready (pid $PM_PID)"; return 0; }
    # If the postmaster died during startup, bail rather than spin.
    kill -0 "$PM_PID" 2>/dev/null || { echo "  postmaster exited during startup"; tail -5 "$OUT/postmaster.log"; return 1; }
    sleep 0.5
  done
  echo "  postmaster failed to become ready"; return 1
}

pm_alive() { [ -n "$PM_PID" ] && kill -0 "$PM_PID" 2>/dev/null; }

stop_pm_once() { pkill -9 -f "$BIN_STABLE" 2>/dev/null; true; }

# Run one test with a watchdog that kills ONLY the pg_regress/psql subtree, never
# the postmaster.  (Our caught errors no longer hang the client, but a genuinely
# unported BLOCKING path could; the watchdog bounds it without taking the server
# down, so the next test still runs against the same live instance.)
run_one() {
  local t="$1" inputdir="$2" expecteddir="$3"
  rm -rf "$OUT/results" "$OUT/regression.diffs" 2>/dev/null
  ( PGOPTIONS="-c client_min_messages=error" "$PGREGRESS" \
      --bindir="$BINDIR" --use-existing --dbname=postgres \
      --host="$SOCK" --port=5432 --user=postgres \
      --inputdir="$inputdir" --expecteddir="$expecteddir" \
      --outputdir="$OUT" --no-locale \
      "$t" > "$OUT/run_$t.log" 2>&1 ) &
  local p=$!
  ( sleep "$PER_TEST_TIMEOUT"; kill -9 "$p" 2>/dev/null; pkill -9 -f "psql.*--dbname=postgres" 2>/dev/null ) & local k=$!
  wait "$p" 2>/dev/null
  kill "$k" 2>/dev/null
  cp "$OUT/regression.diffs" "$OUT/diff_$t.txt" 2>/dev/null
}

# --- 5. run the requested tests against the ONE instance --------------------
TESTS=("$@")
if [ "${#TESTS[@]}" -eq 0 ]; then
  TESTS=(smoke boolean char name int4 int8)
fi

start_pm_once || { echo "FATAL: could not start persistent postmaster"; stop_pm_once; exit 1; }

echo
echo "## persistent-instance pg_regress run (no restart between tests):"
echo "   PGOPTIONS=-c client_min_messages=error \\"
echo "   $PGREGRESS --bindir=$BINDIR --use-existing --dbname=postgres \\"
echo "     --host=$SOCK --port=5432 --user=postgres --outputdir=$OUT --no-locale <test>"
echo

PASS=0; FAIL=0; RAN=0; DIED=0
for t in "${TESTS[@]}"; do
  if ! pm_alive; then
    echo "RESULT $t => INSTANCE_DEAD (postmaster is gone — a crash mode remains)"
    DIED=1; FAIL=$((FAIL+1)); continue
  fi
  if [ "$t" = "smoke" ]; then
    indir="$SQLDIR"; expdir="$SQLDIR"
  else
    indir="$REGRESS_SRC"; expdir="$REGRESS_SRC"
  fi
  run_one "$t" "$indir" "$expdir"
  RAN=$((RAN+1))
  if grep -qE "^ok " "$OUT/run_$t.log" 2>/dev/null; then
    echo "RESULT $t => PASS$(pm_alive && echo '' || echo ' (but instance died!)')"
    PASS=$((PASS+1))
  else
    first=$(grep -v "resource was not closed" "$OUT/results/$t.out" 2>/dev/null \
              | grep -E "ERROR:|FATAL:|server closed the connection" | head -1)
    if [ -z "$first" ]; then
      if [ -f "$OUT/results/$t.out" ]; then
        first="(output diff only — no error/crash; see $OUT/diff_$t.txt)"
      else
        first="(no result file — test hung/early-failed; see $OUT/run_$t.log)"
      fi
    fi
    alive_note=$(pm_alive && echo "instance still up" || echo "INSTANCE DIED")
    echo "RESULT $t => FAIL [$alive_note] — first wall: $first"
    FAIL=$((FAIL+1))
  fi
done

INSTANCE_FINAL=$(pm_alive && echo "UP" || echo "DOWN")
stop_pm_once

echo
echo "## summary: $PASS passed, $FAIL failed, $RAN tests ran against ONE instance."
echo "## persistent instance final state: $INSTANCE_FINAL (UP = it survived every test)."
echo "## artifacts under $OUT/"
[ "$DIED" = "1" ] && echo "## WARNING: the instance died mid-run — a crash mode remains."
exit 0
