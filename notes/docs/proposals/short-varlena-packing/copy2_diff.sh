#!/bin/bash
# Measure copy2.sql difflines vs C expected, using the lane binary.
set -u
WT="$(cd "$(dirname "$0")/../../.." && pwd)"
BIN=$WT/target/debug/postgres
INITDB=/tmp/pgrust_pginstall/bin/initdb
PSQL=/tmp/pgrust_pginstall/bin/psql
REGRESS=/Users/malisper/workspace/work/pgrust/postgres-18.3/src/test/regress
SQL=$REGRESS/sql; EXP=$REGRESS/expected
PORT=${PORT:-49556}
DD=/private/tmp/copy2_dd_$$; SOCK=/private/tmp/copy2_sock_$$; mkdir -p "$SOCK"
export PG_ABS_SRCDIR="$REGRESS" PG_LIBDIR=/tmp/pgrust_pginstall/lib/postgresql PG_DLSUFFIX=.dylib
export PGTZ="America/Los_Angeles" PGDATESTYLE="Postgres, MDY" LANG=C LC_MESSAGES=C
export PGOPTIONS="-c intervalstyle=postgres_verbose"
unset LC_ALL LC_COLLATE LC_CTYPE LC_MONETARY LC_NUMERIC LC_TIME LANGUAGE
cleanup() { kill -9 $PM 2>/dev/null; pkill -9 -f "$DD " 2>/dev/null; rm -rf "$DD" "$SOCK"; }
trap cleanup EXIT
rm -rf "$DD"
"$INITDB" -D "$DD" --no-locale --encoding=UTF8 -U postgres >/tmp/copy2_initdb.log 2>&1 || { echo INITDB FAIL; exit 1; }
"$BIN" -D "$DD" -F -c listen_addresses= -k "$SOCK" -p $PORT -c io_method=sync \
  -c max_stack_depth=7000 -c checkpoint_timeout=3600 -c autovacuum=on \
  -c max_logical_replication_workers=0 >/tmp/copy2_pm.log 2>&1 &
PM=$!
for k in $(seq 1 120); do "$PSQL" -h "$SOCK" -p $PORT -U postgres -c 'SELECT 1' >/dev/null 2>&1 && break; sleep 0.5; done
"$PSQL" -h "$SOCK" -p $PORT -U postgres -X -q -v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on < "$SQL/test_setup.sql" >/dev/null 2>&1
"$PSQL" -h "$SOCK" -p $PORT -U postgres -X -q -a -v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on < "$SQL/copy2.sql" > /tmp/copy2.out 2>&1
diff "$EXP/copy2.out" /tmp/copy2.out > /tmp/copy2.diff
echo "copy2 difflines: $(wc -l < /tmp/copy2.diff)"
