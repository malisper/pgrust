#!/bin/bash
PORT=$(( (RANDOM % 20000) + 30000 ))
DD=/tmp/agg3_dd_$$
SOCK=/tmp/agg3_sock_$$
mkdir -p $SOCK
REG=/Users/malisper/workspace/work/pgrust/postgres-18.3/src/test/regress
PG=/Users/malisper/workspace/work/pgrust/.claude/worktrees/agent-abf0944282871c8cc/target/debug/postgres
INITDB=/tmp/pgrust_pginstall/bin/initdb
PSQL=/tmp/pgrust_pginstall/bin/psql

export PGDATESTYLE="Postgres, MDY" PGTZ="America/Los_Angeles" PGOPTIONS="-c intervalstyle=postgres_verbose"
export PG_LIBDIR=$REG PG_DLSUFFIX=.dylib
ulimit -s 65520 2>/dev/null

$INITDB -D $DD --no-locale --encoding=UTF8 -U postgres >/tmp/agg3_initdb.log 2>&1

$PG -D $DD -F -c listen_addresses= -k $SOCK -p $PORT -c io_method=sync -c max_stack_depth=7000 -c autovacuum=on >/tmp/agg3_pg.log 2>&1 &
PGPID=$!
echo "postmaster pid $PGPID port $PORT dd $DD"

for i in $(seq 1 60); do
  if $PSQL -h $SOCK -p $PORT -U postgres -d postgres -c 'select 1' >/dev/null 2>&1; then break; fi
  sleep 0.5
done

$PSQL -h $SOCK -p $PORT -U postgres -d postgres -c 'CREATE DATABASE regression' >/tmp/agg3_createdb.log 2>&1

for f in test_setup create_index create_aggregate; do
  $PSQL -h $SOCK -p $PORT -U postgres -d regression -q \
    -v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on -v abs_srcdir=$REG \
    -f $REG/sql/$f.sql >/tmp/agg3_setup_$f.log 2>&1
done

$PSQL -h $SOCK -p $PORT -U postgres -d regression -X -a -q \
  -v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on -v abs_srcdir=$REG \
  < $REG/sql/aggregates.sql > /tmp/agg3_out.raw 2>&1

sed -E 's|^psql:[^:]*:[0-9]+: |psql:aggregates.sql: |' /tmp/agg3_out.raw > /tmp/agg3_out.txt
sed -E 's|^psql:[^:]*:[0-9]+: |psql:aggregates.sql: |' $REG/expected/aggregates.out > /tmp/agg3_exp.txt

kill -9 $PGPID 2>/dev/null
pkill -9 -f "$DD " 2>/dev/null
rm -rf $DD $SOCK

diff /tmp/agg3_exp.txt /tmp/agg3_out.txt > /tmp/agg3_diff.txt && echo "ZERO DIFF" || echo "DIFFLINES: $(grep -cE '^[<>]' /tmp/agg3_diff.txt)"
