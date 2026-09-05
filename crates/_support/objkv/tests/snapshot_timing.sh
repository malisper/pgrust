#!/usr/bin/env bash
#
# A snapshot reads objkv as of when it was taken, not when it first touched an
# objkv table; placing it on objkv's clock lazily would expose every commit in
# between. READ COMMITTED still takes a fresh snapshot per statement.
#
. "$(dirname "$0")/server.sh"

fresh_cluster
sql "CREATE TABLE snaptime (id int, note text) USING objkv;" >/dev/null
sql "INSERT INTO snaptime VALUES (1,'before');" >/dev/null

echo "1. REPEATABLE READ: one snapshot, taken where it never reaches objkv"
SAW=$(psqlx -d postgres -tA <<SQL 2>&1 | last
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT count(*) FROM pg_class;
\\! psql -h "$SOCKDIR" -p "$PORT" -d postgres -tAc "INSERT INTO snaptime VALUES (2,'after');" >/dev/null
SELECT 'RESULT=' || string_agg(note, ',' ORDER BY id) FROM snaptime;
COMMIT;
SQL
)
check "a commit after the snapshot stays invisible" "before" "$SAW"

echo "2. READ COMMITTED: a snapshot per statement"
SAW=$(psqlx -d postgres -tA <<SQL 2>&1 | last
BEGIN;
SELECT 'RESULT=' || string_agg(note, ',' ORDER BY id) FROM snaptime;
\\! psql -h "$SOCKDIR" -p "$PORT" -d postgres -tAc "INSERT INTO snaptime VALUES (3,'later');" >/dev/null
SELECT 'RESULT=' || string_agg(note, ',' ORDER BY id) FROM snaptime;
COMMIT;
SQL
)
check "a later statement sees a commit from between" "before,after,later" "$SAW"

finish "snapshots read objkv as of when they were taken"
