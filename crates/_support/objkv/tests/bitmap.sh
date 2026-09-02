#!/usr/bin/env bash
#
# Two indexes on one query: OR across columns, and AND that narrows by both.
# A bitmap scan collects every row id first so the planner can union or
# intersect them; the index has to produce a bitmap and the table has to read
# one. Every answer is compared with a forced table read.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-5000}"
export PGRUST_OBJKV_TRACE=1

echo "0. one table, two separate indexes"
fresh_cluster
sql "CREATE TABLE hits (id int PRIMARY KEY, grp int, tag text COLLATE \"C\") USING objkv;" >/dev/null
sql "INSERT INTO hits SELECT g, g % 50, 'tag-' || g FROM generate_series(1,$ROWS) g;" >/dev/null
sql "CREATE INDEX hits_grp ON hits (grp);" >/dev/null
sql "CREATE INDEX hits_tag ON hits (tag);" >/dev/null
check "the rows are in"   "$ROWS" "$(sql "SELECT count(*) FROM hits;")"
check "both indexes were built" "2" \
      "$(sql "SELECT count(*) FROM pg_class WHERE relname IN ('hits_grp','hits_tag');")"

echo "1. OR across two columns"
P=$(plan "SELECT id FROM hits WHERE grp = 3 OR tag = 'tag-11';")
shows "the plan unions two indexes" "BitmapOr" "$P"
shows "and reads the table from the bitmap" "Bitmap Heap Scan" "$P"
agree "the count matches a plain read" "SELECT count(*) FROM hits WHERE grp = 3 OR tag = 'tag-11';"
agree "and so do the rows" \
      "SELECT string_agg(id::text, ',' ORDER BY id) FROM hits WHERE grp = 3 AND id < 200 OR tag = 'tag-11';"

echo "2. AND that narrows by both"
# Two equalities on different columns, each selective on its own, so the
# planner reads both bitmaps and intersects rather than taking one index and
# filtering -- which would pass a bare "Bitmap" check without exercising the
# intersection.
P=$(plan "SELECT id FROM hits WHERE grp = 7 AND tag = 'tag-357';")
shows "the plan intersects two indexes" "BitmapAnd" "$P"
shows "and reads the table from the bitmap" "Bitmap Heap Scan" "$P"
agree "the intersection is the row in both" \
      "SELECT string_agg(id::text, ',' ORDER BY id) FROM hits WHERE grp = 7 AND tag = 'tag-357';"
agree "and an intersection of nothing is nothing" \
      "SELECT count(*) FROM hits WHERE grp = 7 AND tag = 'tag-11';"
# Which plan the wider shape gets is the planner's business; the answer is not.
agree "a range narrowed by an equality still agrees with the table" \
      "SELECT count(*) FROM hits WHERE grp = 7 AND tag > 'tag-4';"

echo "3. a longer OR list"
agree "five values" \
      "SELECT count(*) FROM hits WHERE tag = 'tag-1' OR tag = 'tag-2' OR tag = 'tag-3' OR tag = 'tag-4' OR tag = 'tag-5';"
agree "and one that matches nothing" \
      "SELECT count(*) FROM hits WHERE tag = 'nobody' OR grp = 999;"

echo "4. it reads the rows it was given, not the table"
trace_mark
idx "SELECT count(*) FROM hits WHERE grp = 3 OR tag = 'tag-11';" >/dev/null
READ=$(trace_candidates)
echo "  it looked at $READ entries, of $ROWS rows in the table"
check "far short of the whole table" "t" "$([ "$READ" -lt $((ROWS / 10)) ] && echo t || echo f)"

echo "5. and it stays right when the rows change"
sql "UPDATE hits SET grp = 3 WHERE id BETWEEN 1000 AND 1010;" >/dev/null
agree "after an update" "SELECT count(*) FROM hits WHERE grp = 3 OR tag = 'tag-11';"
sql "DELETE FROM hits WHERE grp = 3 AND id < 500;" >/dev/null
agree "after a delete"  "SELECT count(*) FROM hits WHERE grp = 3 OR tag = 'tag-11';"
agree "and the table still adds up" "SELECT count(*) FROM hits;"

echo "6. a row id survives the trip through a bitmap"
# A bitmap addresses rows as (page, slot) and rejects a slot no real page
# could hold, which fixes how a row id is split; the widest id in the table
# must come back out intact.
agree "the widest row id in the table round-trips" \
      "SELECT string_agg(id::text, ',' ORDER BY id) FROM hits WHERE id > $((ROWS - 3)) OR tag = 'tag-11';"

finish "two indexes, one query"
