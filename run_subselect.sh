#!/bin/zsh
set -e
WT=/Users/malisper/workspace/work/pgrust/.claude/worktrees/agent-a304480fc407aa246
REG=/Users/malisper/workspace/work/pgrust/postgres-18.3/src/test/regress
PG=$WT/target/debug/postgres
PORT=$((20000 + RANDOM % 20000))
DD=/tmp/subselect_dd_$$
SOCK=/tmp/subselect_sock_$$
mkdir -p $SOCK
export PGDATESTYLE="Postgres, MDY" PGTZ="America/Los_Angeles" PGOPTIONS="-c intervalstyle=postgres_verbose"

/tmp/pgrust_pginstall/bin/initdb -D $DD --no-locale --encoding=UTF8 -U postgres >/dev/null 2>&1

$PG -D $DD -F -c listen_addresses= -k $SOCK -p $PORT -c io_method=sync -c max_stack_depth=7000 -c autovacuum=on >$DD/log 2>&1 &
PMPID=$!
for i in {1..60}; do
  if /tmp/pgrust_pginstall/bin/psql -h $SOCK -p $PORT -U postgres -d postgres -c "select 1" >/dev/null 2>&1; then break; fi
  sleep 0.5
done

PSQL="/tmp/pgrust_pginstall/bin/psql -h $SOCK -p $PORT -U postgres -X -q -v ON_ERROR_STOP=0 -v abs_srcdir=$REG -v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on"

$PSQL -d postgres -c "CREATE DATABASE regression" >/dev/null 2>&1
$PSQL -d regression -f $REG/sql/test_setup.sql >/dev/null 2>&1
$PSQL -d regression -f $REG/sql/create_index.sql >/dev/null 2>&1
$PSQL -d regression -a -f $REG/sql/subselect.sql > $DD/out.txt 2>&1

# normalize psql file:line: prefix
sed -E 's/^psql:[^:]*:[0-9]+: //' $DD/out.txt > $WT/subselect_actual.out
diff $REG/expected/subselect.out $WT/subselect_actual.out > $WT/subselect.diff 2>&1 || true
echo "DIFFLINES: $(wc -l < $WT/subselect.diff)"
echo "DD=$DD SOCK=$SOCK PMPID=$PMPID PORT=$PORT"

kill -9 $PMPID 2>/dev/null || true
pkill -9 -f "$DD " 2>/dev/null || true
rm -rf $DD $SOCK
