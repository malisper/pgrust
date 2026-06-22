#!/bin/bash
set -e
cd /Users/malisper/workspace/work/pgrust/.claude/worktrees/agent-af825941e1c132173
export PGDATESTYLE="Postgres, MDY" PGTZ="America/Los_Angeles" PGOPTIONS="-c intervalstyle=postgres_verbose"
export PG_ABS_SRCDIR=/Users/malisper/workspace/work/pgrust/postgres-18.3/src/test/regress
export PG_LIBDIR=/tmp/pgrust_pginstall/lib/postgresql PG_DLSUFFIX=.dylib
ulimit -s unlimited 2>/dev/null || true

REG=/Users/malisper/workspace/work/pgrust/postgres-18.3/src/test/regress
DD=/private/tmp/inet_dd_$$
SOCK=/private/tmp/inet_sock_$$
PORT=$(( (RANDOM % 20000) + 30000 ))
mkdir -p $SOCK
BIN=$PWD/target/debug/postgres
PSQL=/tmp/pgrust_pginstall/bin/psql

/tmp/pgrust_pginstall/bin/initdb -D $DD --no-locale --encoding=UTF8 -U postgres >/dev/null 2>&1

$BIN -D $DD -F -c listen_addresses= -k $SOCK -p $PORT -c io_method=sync -c max_stack_depth=7000 -c autovacuum=on >/private/tmp/inet_log_$$ 2>&1 &
PMPID=$!
for i in $(seq 1 60); do
  $PSQL -h $SOCK -p $PORT -U postgres -d postgres -c 'select 1' >/dev/null 2>&1 && break
  sleep 0.5
done

$PSQL -h $SOCK -p $PORT -U postgres -d postgres -c 'CREATE DATABASE regression' >/dev/null 2>&1
$PSQL -h $SOCK -p $PORT -U postgres -d regression -q -f $REG/sql/test_setup.sql >/private/tmp/inet_setup_$$ 2>&1

$PSQL -h $SOCK -p $PORT -U postgres -d regression -X -a -q -v HIDE_TABLEAM=on -v HIDE_TOAST_COMPRESSION=on < $REG/sql/inet.sql > /private/tmp/inet_out_$$ 2>&1

sed -E 's/^[^ ]*inet\.sql:[0-9]+: //' /private/tmp/inet_out_$$ > /private/tmp/inet_out_norm_$$
diff /private/tmp/inet_out_norm_$$ $REG/expected/inet.out > /private/tmp/inet_diff_$$ || true
echo "=== DIFF (lines: $(wc -l < /private/tmp/inet_diff_$$)) ==="
cat /private/tmp/inet_diff_$$

kill -9 $PMPID 2>/dev/null || true
pkill -9 -f "$DD " 2>/dev/null || true
rm -rf $DD $SOCK /private/tmp/inet_log_$$ /private/tmp/inet_setup_$$ /private/tmp/inet_out_$$ /private/tmp/inet_out_norm_$$ /private/tmp/inet_diff_$$
