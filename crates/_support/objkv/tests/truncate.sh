#!/usr/bin/env bash
#
# TRUNCATE as a line drawn rather than a deletion: one small object saying
# "empty from here", and reads skip anything older. An ordinary write in an
# ordinary commit, so it rolls back, keeps an earlier snapshot honest, and
# lets collection reclaim the space.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-3000}"
WALK="${WALK:-60}"   # how many commits back step 6 looks for the pre-truncate table

echo "0. a table with rows and two indexes"
fresh_cluster
sql "CREATE TABLE t (id int PRIMARY KEY, tag text COLLATE \"C\") USING objkv;" >/dev/null
sql "CREATE INDEX t_tag ON t (tag);" >/dev/null
sql "INSERT INTO t SELECT g, 'tag-' || g FROM generate_series(1,$ROWS) g;" >/dev/null
check "the rows are in" "$ROWS" "$(sql "SELECT count(*) FROM t;")"

echo "1. it empties the table"
check "TRUNCATE reports success"  "TRUNCATE TABLE" "$(sql "TRUNCATE t;")"
check "and the table is empty"    "0" "$(sql "SELECT count(*) FROM t;")"
check "by index lookup too"       "0" "$(idx "SELECT count(*) FROM t WHERE id = 7;")"
check "and by the other index"    "0" "$(idx "SELECT count(*) FROM t WHERE tag = 'tag-7';")"
check "a range finds nothing"     "0" "$(idx "SELECT count(*) FROM t WHERE id > 0;")"

echo "2. and the table still works afterwards"
check "the insert itself succeeds" "INSERT 0 1" "$(sql "INSERT INTO t VALUES (1, 'after');")"
check "a row can be inserted"     "after" "$(sql "SELECT tag FROM t WHERE id = 1;")"
check "including an id that was there before" "1" "$(sql "SELECT count(*) FROM t;")"
sql "INSERT INTO t SELECT g, 'again-' || g FROM generate_series(2,100) g;" >/dev/null
check "and a hundred more"        "100" "$(sql "SELECT count(*) FROM t;")"
check "the old value is not back" "0" "$(idx "SELECT count(*) FROM t WHERE tag = 'tag-50';")"

echo "3. it rolls back"
check "inside the transaction the table reads empty" "0" "$(txn <<'SQL' | last
BEGIN;
TRUNCATE t;
SELECT 'RESULT=' || count(*) FROM t;
ROLLBACK;
SQL
)"
check "after the rollback they are all there" "100" "$(sql "SELECT count(*) FROM t;")"

echo "4. inside a transaction, before and after"
check "rows written before the truncate are gone, ones after are not" "3" "$(txn <<'SQL' | last
BEGIN;
INSERT INTO t VALUES (500,'before-truncate'),(501,'also-before');
TRUNCATE t;
INSERT INTO t VALUES (600,'after-truncate'),(601,'also-after'),(602,'third');
SELECT 'RESULT=' || count(*) FROM t;
COMMIT;
SQL
)"
check "and that is what committed" "3" "$(sql "SELECT count(*) FROM t;")"
check "the right three"            "after-truncate,also-after,third" \
      "$(sql "SELECT string_agg(tag, ',' ORDER BY id) FROM t;")"

echo "5. a savepoint that truncates and rolls back"
check "the rows survive" "3" "$(txn <<'SQL' | last
BEGIN;
SAVEPOINT s;
TRUNCATE t;
ROLLBACK TO SAVEPOINT s;
SELECT 'RESULT=' || count(*) FROM t;
COMMIT;
SQL
)"

echo "6. a snapshot from before the truncate still sees the rows"
sql "CREATE TABLE past (id int PRIMARY KEY, tag text COLLATE \"C\") USING objkv;" >/dev/null
sql "INSERT INTO past SELECT g, 'old-' || g FROM generate_series(1,20) g;" >/dev/null
check "twenty rows"    "20" "$(sql "SELECT count(*) FROM past;")"
sql "TRUNCATE past;" >/dev/null
check "and now none"   "0"  "$(sql "SELECT count(*) FROM past;")"
# Which commit the truncate was is not readable from SQL, so walk back from
# the most recent WALK commits until a snapshot shows the full table.
at_seq() { psqlx -d postgres -tA -c "SET pgrust.objkv_snapshot_seq = $1;" -c "$2" 2>/dev/null | tail -1; }
FOUND=0
for seq in $(seq "$WALK" -1 1); do
    if [ "$(at_seq "$seq" "SELECT count(*) FROM past;")" = "20" ]; then FOUND=$seq; break; fi
done
check "some earlier commit still has all twenty" "t" "$([ "$FOUND" != 0 ] && echo t || echo f)"
if [ "$FOUND" != 0 ]; then
    echo "  as of commit $FOUND the table was still full"
    check "and the values are the ones from then" "old-7" "$(at_seq "$FOUND" "SELECT tag FROM past WHERE id = 7;")"
fi

echo "7. several tables in one statement"
sql "CREATE TABLE u (id int PRIMARY KEY) USING objkv;" >/dev/null
sql "CREATE TABLE v (id int PRIMARY KEY) USING objkv;" >/dev/null
sql "INSERT INTO u SELECT generate_series(1,50);" >/dev/null
sql "INSERT INTO v SELECT generate_series(1,50);" >/dev/null
check "both filled" "50,50" "$(sql "SELECT (SELECT count(*) FROM u) || ',' || (SELECT count(*) FROM v);")"
check "TRUNCATE u, v"  "TRUNCATE TABLE" "$(sql "TRUNCATE u, v;")"
check "both empty"     "0,0"  "$(sql "SELECT (SELECT count(*) FROM u) || ',' || (SELECT count(*) FROM v);")"

echo "8. a heap table in the same statement still works"
sql "CREATE TABLE h (id int PRIMARY KEY);" >/dev/null
sql "INSERT INTO h SELECT generate_series(1,10);" >/dev/null
sql "INSERT INTO u SELECT generate_series(1,10);" >/dev/null
check "one of each, truncated together" "TRUNCATE TABLE" "$(sql "TRUNCATE h, u;")"
check "both empty" "0,0" "$(sql "SELECT (SELECT count(*) FROM h) || ',' || (SELECT count(*) FROM u);")"

echo "9. it survives a restart"
sql "TRUNCATE t;" >/dev/null
sql "INSERT INTO t VALUES (9,'kept');" >/dev/null
check "one row before the restart" "1" "$(sql "SELECT count(*) FROM t;")"
stop
boot
check "still one row after it"     "1" "$(sql "SELECT count(*) FROM t;")"
check "and it is the right one"    "kept" "$(sql "SELECT tag FROM t WHERE id = 9;")"
check "the truncated values did not return" "0" "$(idx "SELECT count(*) FROM t WHERE tag = 'after-truncate';")"

finish "TRUNCATE"
