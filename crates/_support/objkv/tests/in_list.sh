#!/usr/bin/env bash
#
# IN (...) as an index condition. The list is read from its lowest value to
# its highest with each key checked for membership, which keeps the scan in
# index order, so ORDER BY still skips the sort. `= ANY(NULL)` once crashed
# the backend; it is a no-rows answer now.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-5000}"
export PGRUST_OBJKV_TRACE=1

echo "0. a table with room to be wrong in"
fresh_cluster
sql "CREATE TABLE picks (id int PRIMARY KEY, grp int, tag text COLLATE \"C\") USING objkv;" >/dev/null
sql "INSERT INTO picks SELECT g, g % 50, 'tag-' || lpad(g::text, 5, '0') FROM generate_series(1,$ROWS) g;" >/dev/null
sql "CREATE INDEX picks_tag ON picks (tag);" >/dev/null
sql "CREATE INDEX picks_grp_id ON picks (grp, id);" >/dev/null
check "the rows are in" "$ROWS" "$(sql "SELECT count(*) FROM picks;")"

echo "1. the plan takes the list"
P=$(plan "SELECT id FROM picks WHERE tag IN ('tag-00001','tag-00002','tag-00003');")
shows "it is an index scan"          "Index Scan"   "$P"
shows "and the list is the condition" "ANY"         "$P"

echo "2. the answers"
agree "three values"        "SELECT count(*) FROM picks WHERE tag IN ('tag-00001','tag-00002','tag-00003');"
agree "and they are the rows" \
      "SELECT string_agg(id::text, ',' ORDER BY id) FROM picks WHERE tag IN ('tag-00001','tag-00002','tag-00003');"
agree "one that matches nothing" "SELECT count(*) FROM picks WHERE tag IN ('nope','also-nope');"
agree "a mix of hits and misses"  "SELECT count(*) FROM picks WHERE tag IN ('tag-00007','nope','tag-04999');"
agree "a list of one"             "SELECT count(*) FROM picks WHERE tag IN ('tag-00042');"
agree "duplicates in the list"    "SELECT count(*) FROM picks WHERE tag IN ('tag-00042','tag-00042');"
agree "a NULL in the list"        "SELECT count(*) FROM picks WHERE tag IN ('tag-00042', NULL);"
agree "on an integer column"      "SELECT count(*) FROM picks WHERE id IN (1,2,3,4,5);"
agree "on the leading column of a two-column index" \
      "SELECT count(*) FROM picks WHERE grp IN (3,7) AND id < 1000;"
agree "on the second column of one" \
      "SELECT count(*) FROM picks WHERE grp = 3 AND id IN (3,53,103);"

echo "3. it reads the span of the list, not the table"
trace_mark
idx "SELECT count(*) FROM picks WHERE tag IN ('tag-00001','tag-00002','tag-00003');" >/dev/null
READ=$(trace_candidates)
echo "  it looked at $READ entries, of $ROWS in the table"
check "three neighbouring values cost about three reads" "t" "$([ "$READ" -le 10 ] && echo t || echo f)"

echo "4. the scan stays in index order"
# Bitmap scans off: a bitmap loses the order and the planner may sort after.
IDX_OPTS="-c enable_bitmapscan=off"
nosort "ORDER BY needs no sort step" \
       "SELECT tag FROM picks WHERE tag IN ('tag-00003','tag-00001','tag-00002') ORDER BY tag;"
check "and the order is right" "tag-00001,tag-00002,tag-00003" \
      "$(idx "SELECT string_agg(tag, ',') FROM (SELECT tag FROM picks WHERE tag IN ('tag-00003','tag-00001','tag-00002') ORDER BY tag) x;")"
check "backwards too" "tag-00003,tag-00002,tag-00001" \
      "$(idx "SELECT string_agg(tag, ',') FROM (SELECT tag FROM picks WHERE tag IN ('tag-00003','tag-00001','tag-00002') ORDER BY tag DESC) x;")"

echo "5. still true after the rows change"
sql "UPDATE picks SET tag = 'tag-00001' WHERE id = 4000;" >/dev/null
agree "an updated row joins the list" "SELECT count(*) FROM picks WHERE tag IN ('tag-00001','tag-00002');"
sql "DELETE FROM picks WHERE tag = 'tag-00002';" >/dev/null
agree "a deleted one leaves it"       "SELECT count(*) FROM picks WHERE tag IN ('tag-00001','tag-00002');"

echo "6. a list that is not a list of equalities"
# `= ANY(NULL)` is a list-shaped key with no list behind it. Unknown is not a
# match: no rows, and the connection survives to say so.
check "= ANY(NULL) is no rows"      "0" "$(idx "SELECT count(*) FROM picks WHERE tag = ANY(NULL::text[]);")"
check "and the backend survived it" "t" "$(sql "SELECT count(*) > 0 FROM picks;")"
check "the same through a parameter" "0" \
      "$(idx "PREPARE p(text) AS SELECT count(*) FROM picks WHERE tag = \$1; EXECUTE p(NULL);" | tail -1)"

# `x > ANY(a,b)` is `x > the smaller`: the scan is bounded by the loosest
# value and every key is checked against the rest.
P=$(plan "SELECT count(*) FROM picks WHERE tag > ANY (ARRAY['tag-04990','tag-00002']);")
shows "a range over a list still reads the index" "Index" "$P"
agree "and it is the same answer as reading the table" \
      "SELECT count(*) FROM picks WHERE tag > ANY (ARRAY['tag-04990','tag-00002']);"
agree "the other direction too" \
      "SELECT count(*) FROM picks WHERE tag < ANY (ARRAY['tag-00010','tag-00003']);"
agree "and mixed with an equality on the same column" \
      "SELECT count(*) FROM picks WHERE tag >= ANY (ARRAY['tag-04999','tag-04997']);"

finish "IN lists read the index"
