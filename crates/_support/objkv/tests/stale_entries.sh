#!/usr/bin/env bash
#
# Index entries must not outlive the value they describe. Postgres leaves
# removing an entry to vacuum; objkv withdraws it in the same commit as the
# row change. The claim is exact: a stale entry fails the recheck, so with
# none the traced candidate and kept counts agree on every scan.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-200}"
export PGRUST_OBJKV_TRACE=1

echo "0. an empty bucket, since older ones hold entries this test is about"
fresh_cluster
sql "CREATE TABLE churn (id int PRIMARY KEY, tag text COLLATE \"C\", owner text COLLATE \"C\") USING objkv;" >/dev/null
sql "CREATE INDEX churn_tag ON churn (tag);" >/dev/null
sql "CREATE UNIQUE INDEX churn_owner ON churn (owner);" >/dev/null
sql "INSERT INTO churn SELECT g, 'round0-' || g, 'owner-' || g FROM generate_series(1,$ROWS) g;" >/dev/null

echo "1. churn: three rounds of rewriting every indexed value, then half deleted"
for r in 1 2 3; do
    sql "UPDATE churn SET tag = 'round$r-' || id;" >/dev/null
done
sql "UPDATE churn SET owner = 'newowner-' || id WHERE id % 3 = 0;" >/dev/null
sql "DELETE FROM churn WHERE id % 2 = 0;" >/dev/null
LIVE=$(sql "SELECT count(*) FROM churn;")
echo "  $LIVE rows left after $((ROWS - LIVE)) deletes"

echo "2. values that were rewritten or deleted are gone from the index"
# Small tables are read whole by default, which proves nothing about entries.
check "an old tag from round 0 finds nothing"  "0" "$(idx "SELECT count(*) FROM churn WHERE tag = 'round0-7';")"
check "an old tag from round 2 finds nothing"  "0" "$(idx "SELECT count(*) FROM churn WHERE tag = 'round2-7';")"
check "the current tag finds its row"          "7" "$(idx "SELECT id FROM churn WHERE tag = 'round3-7';")"
check "a deleted row's tag finds nothing"      "0" "$(idx "SELECT count(*) FROM churn WHERE tag = 'round3-8';")"
check "a replaced owner finds nothing"         "0" "$(idx "SELECT count(*) FROM churn WHERE owner = 'owner-9';")"
check "the current owner finds its row"        "9" "$(idx "SELECT id FROM churn WHERE owner = 'newowner-9';")"
check "a deleted row's owner finds nothing"    "0" "$(idx "SELECT count(*) FROM churn WHERE owner = 'owner-8';")"

echo "3. the value a unique index freed can be taken by another row"
check "reusing a deleted row's owner is allowed" "INSERT 0 1" \
      "$(sql "INSERT INTO churn VALUES ($((ROWS + 1)), 'reuse', 'owner-8');")"
check "and it reads back" "$((ROWS + 1))" "$(idx "SELECT id FROM churn WHERE owner = 'owner-8';")"

echo "4. no scan ever saw a candidate it had to throw away"
BAD=$(trace_mismatches)
SCANS=$(trace_scans)
echo "  $SCANS index scans traced"
check "every scan kept every candidate" "0" "$BAD"
if [ "$BAD" != 0 ]; then
    grep -a "OBJKVTRACE index_scan" "$LOG" | grep -v -E "candidates=([0-9]+) kept=\1( |$)" | head -5 | sed 's/^/    /'
fi
check "and there were scans to check" "t" "$([ "$SCANS" -gt 0 ] && echo t || echo f)"

echo "5. the table still says the truth"
check "every live row is findable by its tag" "$((LIVE + 1))" \
      "$(idx "SELECT count(*) FROM churn c WHERE EXISTS (SELECT 1 FROM churn x WHERE x.tag = c.tag);")"
check "and the row count did not move" "$((LIVE + 1))" "$(sql "SELECT count(*) FROM churn;")"

echo "6. an equality match that returns many rows costs no fetch per row"
sql "CREATE TABLE many (id int PRIMARY KEY, bucket_no int, pad text) USING objkv;" >/dev/null
sql "CREATE INDEX many_bucket ON many (bucket_no);" >/dev/null
sql "INSERT INTO many SELECT g, g % 10, repeat('x', 40) FROM generate_series(1,5000) g;" >/dev/null
MS=$(sql "EXPLAIN (ANALYZE, TIMING OFF) SELECT count(*) FROM many WHERE bucket_no = 3;" \
     | grep -o "Execution Time: [0-9.]*" | grep -o "[0-9.]*")
check "it finds every one of them" "500" "$(idx "SELECT count(*) FROM many WHERE bucket_no = 3;")"
shows "and the plan uses the index" "Index Scan" "$(plan "SELECT count(*) FROM many WHERE bucket_no = 3;")"
echo "  500 rows through the index in ${MS}ms"

finish "no index entry outlives its value"
