#!/usr/bin/env bash
#
# IS NULL and IS NOT NULL, answered from the index. A NULL is stored as a
# single byte above every real value, so IS NULL is an equality against that
# byte and IS NOT NULL is everything below it.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-2000}"
NULLS=$((ROWS / 10))
export PGRUST_OBJKV_TRACE=1

echo "0. a table where one row in ten has no tag"
fresh_cluster
sql "CREATE TABLE notes (id int PRIMARY KEY, grp int, tag text COLLATE \"C\") USING objkv;" >/dev/null
sql "INSERT INTO notes SELECT g, g % 10, CASE WHEN g % 10 = 0 THEN NULL ELSE 'tag-' || g END
     FROM generate_series(1,$ROWS) g;" >/dev/null
# One index for now; the second goes in after the measurement in step 3, so
# the count there is a claim about what a seek on this one reads.
sql "CREATE INDEX notes_tag ON notes (tag);" >/dev/null
check "the rows are in"          "$ROWS"  "$(sql "SELECT count(*) FROM notes;")"
check "the index was built"      "1"      "$(sql "SELECT count(*) FROM pg_class WHERE relname = 'notes_tag';")"
check "and a tenth have no tag"  "$NULLS" "$(sql "SELECT count(*) FROM notes WHERE tag IS NULL;")"

echo "1. the answers, read through the index"
check "IS NULL"     "$NULLS"            "$(idx "SELECT count(*) FROM notes WHERE tag IS NULL;")"
check "IS NOT NULL" "$((ROWS - NULLS))" "$(idx "SELECT count(*) FROM notes WHERE tag IS NOT NULL;")"
check "and they are the rows, not just the count" "10,20,30" \
      "$(idx "SELECT string_agg(id::text, ',' ORDER BY id) FROM (SELECT id FROM notes WHERE tag IS NULL AND id <= 30) x;")"

echo "2. and the plans actually use it"
shows "IS NULL goes to the index"     "Index Scan" "$(plan "SELECT id FROM notes WHERE tag IS NULL;")"
shows "IS NOT NULL goes to the index" "Index Scan" "$(plan "SELECT id FROM notes WHERE tag IS NOT NULL;")"

echo "3. the point of it: reading $NULLS entries, not $ROWS"
trace_mark
idx "SELECT count(*) FROM notes WHERE tag IS NULL;" >/dev/null
READ=$(trace_candidates)
echo "  it looked at $READ entries to find $NULLS rows, out of $ROWS in the table"
check "it did not walk the whole index" "t" "$([ "$READ" -le $((NULLS * 2)) ] && echo t || echo f)"

echo "4. a null test on a column that is not the first one"
sql "CREATE INDEX notes_grp_tag ON notes (grp, tag);" >/dev/null
check "a two-column index builds" "1" \
      "$(sql "SELECT count(*) FROM pg_class WHERE relname = 'notes_grp_tag';")"
check "grp = 5 and no tag"     "0"      "$(idx "SELECT count(*) FROM notes WHERE grp = 5 AND tag IS NULL;")"
check "grp = 0 and no tag"     "$NULLS" "$(idx "SELECT count(*) FROM notes WHERE grp = 0 AND tag IS NULL;")"
check "grp = 0 and some tag"   "0"      "$(idx "SELECT count(*) FROM notes WHERE grp = 0 AND tag IS NOT NULL;")"
check "no tag, any group"      "$NULLS" "$(idx "SELECT count(*) FROM notes WHERE grp >= 0 AND tag IS NULL;")"

echo "5. a null test is not a comparison against NULL"
# `tag > NULL` is unknown for every row; turning one into the other would be
# the quiet kind of wrong.
check "greater than NULL matches nothing" "0" "$(sql "SELECT count(*) FROM notes WHERE tag > NULL;")"
check "equal to NULL matches nothing"     "0" "$(sql "SELECT count(*) FROM notes WHERE tag = NULL;")"
# A literal NULL folds away in the planner. A generic plan for a parameter
# does not, so the scan itself is handed a NULL key and must answer it.
NULLKEY=$(txn <<'SQL'
SET plan_cache_mode = force_generic_plan;
SET enable_seqscan = off;
PREPARE gt(text) AS SELECT count(*) FROM notes WHERE tag > $1;
PREPARE eq(text) AS SELECT count(*) FROM notes WHERE tag = $1;
EXPLAIN EXECUTE gt(NULL);
EXECUTE gt(NULL);
EXECUTE eq(NULL);
SQL
)
shows "a generic plan still goes to the index" "Index" "$NULLKEY"
check "and a NULL parameter matches nothing, either way" "0
0" "$(echo "$NULLKEY" | grep -E '^[0-9]+$' | tail -2)"

echo "6. nulls keep their place in the order"
check "sorting puts them last"  "" \
      "$(idx "SELECT coalesce(tag,'') FROM (SELECT tag FROM notes ORDER BY tag DESC LIMIT 1) x;")"
check "and the two halves add up" "$ROWS" \
      "$(idx "SELECT (SELECT count(*) FROM notes WHERE tag IS NULL) + (SELECT count(*) FROM notes WHERE tag IS NOT NULL);")"

echo "7. it stays true when the rows change"
sql "UPDATE notes SET tag = NULL WHERE id BETWEEN 1 AND 9;" >/dev/null
check "nine more nulls"     "$((NULLS + 9))" "$(idx "SELECT count(*) FROM notes WHERE tag IS NULL;")"
sql "UPDATE notes SET tag = 'filled' WHERE id = 10;" >/dev/null
check "and one fewer again" "$((NULLS + 8))" "$(idx "SELECT count(*) FROM notes WHERE tag IS NULL;")"
sql "DELETE FROM notes WHERE tag IS NULL;" >/dev/null
check "deleting them leaves none"  "0" "$(idx "SELECT count(*) FROM notes WHERE tag IS NULL;")"
check "and the rest are all tagged" "$((ROWS - NULLS - 8))" "$(idx "SELECT count(*) FROM notes WHERE tag IS NOT NULL;")"

finish "null tests read the index"
