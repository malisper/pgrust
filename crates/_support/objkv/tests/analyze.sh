#!/usr/bin/env bash
#
# ANALYZE on an objkv table: rows are sampled from the bucket, pg_statistic
# gets per-column statistics, and the planner's estimates follow them.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-20000}"

echo "0. a skewed table"
fresh_cluster
must "CREATE TABLE sk (id int PRIMARY KEY, grp int, tag text COLLATE \"C\") USING objkv;" >/dev/null
# grp: 90% are 0, the rest spread over 1..99
must "INSERT INTO sk SELECT g, CASE WHEN g % 10 = 0 THEN g % 100 ELSE 0 END, 't' || (g % 7) FROM generate_series(1,$ROWS) g;" >/dev/null
must "CREATE INDEX sk_grp ON sk (grp);" >/dev/null
check "no statistics yet" "0" "$(sql "SELECT count(*) FROM pg_stats WHERE tablename = 'sk';")"

echo "1. ANALYZE runs"
OUT=$(sql "ANALYZE sk;")
check "and succeeds" "ANALYZE" "$OUT"
check "pg_statistic has a row per column" "3" "$(sql "SELECT count(*) FROM pg_stats WHERE tablename = 'sk';")"
check "reltuples is the row count" "$ROWS" "$(sql "SELECT reltuples::int FROM pg_class WHERE relname = 'sk';")"
check "the common value of grp is 0" "0" "$(sql "SELECT (most_common_vals::text::int[])[1] FROM pg_stats WHERE tablename = 'sk' AND attname = 'grp';")"
check "tag has seven distinct values" "7" "$(sql "SELECT n_distinct::int FROM pg_stats WHERE tablename = 'sk' AND attname = 'tag';")"

echo "2. the planner uses them"
EST_ZERO=$(sql "EXPLAIN SELECT * FROM sk WHERE grp = 0;" | grep -o 'rows=[0-9]*' | head -1 | cut -d= -f2)
EST_ONE=$(sql "EXPLAIN SELECT * FROM sk WHERE grp = 42;" | grep -o 'rows=[0-9]*' | head -1 | cut -d= -f2)
echo "  estimated rows: grp = 0 -> $EST_ZERO, grp = 42 -> $EST_ONE"
check "grp = 0 is estimated as most of the table" "t" "$([ "$EST_ZERO" -gt $((ROWS / 2)) ] && echo t || echo f)"
check "grp = 42 is estimated as a sliver"          "t" "$([ "$EST_ONE" -lt $((ROWS / 20)) ] && echo t || echo f)"
shows "a rare value goes to the index" "Index" "$(sql "EXPLAIN SELECT * FROM sk WHERE grp = 42;")"
shows "the common value does not"      "Seq Scan" "$(sql "EXPLAIN SELECT * FROM sk WHERE grp = 0;")"

echo "3. ANALYZE with a column list, and on everything"
check "one column"  "ANALYZE" "$(sql "ANALYZE sk (tag);")"
check "the database" "ANALYZE" "$(sql "ANALYZE;")"
check "VACUUM ANALYZE" "VACUUM" "$(sql "VACUUM ANALYZE sk;")"

echo "4. the statistics survive a restart"
stop
boot
check "still there" "3" "$(sql "SELECT count(*) FROM pg_stats WHERE tablename = 'sk';")"

finish "ANALYZE on objkv tables"
