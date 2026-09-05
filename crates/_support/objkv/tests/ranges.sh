#!/usr/bin/env bash
#
# Ranges: less-than, greater-than, BETWEEN, LIKE 'prefix%', multi-column,
# LIMIT windows, ORDER BY in either direction, and floating point. The key
# encoding makes byte order value order, so a range is a place to start
# reading and a place to stop.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-20000}"
export PGRUST_OBJKV_TRACE=1

echo "0. a table big enough that reading it all would show"
fresh_cluster
sql "CREATE TABLE events (id int PRIMARY KEY, day int, who text COLLATE \"C\") USING objkv;" >/dev/null
sql "CREATE INDEX events_day ON events (day, who);" >/dev/null
sql "INSERT INTO events SELECT g, g / 100, 'user-' || lpad(g::text, 6, '0') FROM generate_series(1,$ROWS) g;" >/dev/null
check "the rows are in" "$ROWS" "$(sql "SELECT count(*) FROM events;")"
check "and the index exists" "1" "$(sql "SELECT count(*) FROM pg_class WHERE relname = 'events_day';")"

echo "1. one-sided ranges on the leading column"
check "greater than"          "10" "$(idx "SELECT count(*) FROM events WHERE id > $((ROWS - 10));")"
check "greater or equal"      "11" "$(idx "SELECT count(*) FROM events WHERE id >= $((ROWS - 10));")"
check "less than"             "9"  "$(idx "SELECT count(*) FROM events WHERE id < 10;")"
check "less or equal"         "10" "$(idx "SELECT count(*) FROM events WHERE id <= 10;")"
check "and the bound itself is where it should be" "$((ROWS - 10))" \
      "$(idx "SELECT min(id) FROM events WHERE id >= $((ROWS - 10));")"

echo "2. two-sided"
check "between"               "101" "$(idx "SELECT count(*) FROM events WHERE id BETWEEN 5000 AND 5100;")"
check "half-open"             "100" "$(idx "SELECT count(*) FROM events WHERE id >= 5000 AND id < 5100;")"
check "empty range"           "0"   "$(idx "SELECT count(*) FROM events WHERE id > 100 AND id < 100;")"
check "reversed bounds"       "0"   "$(idx "SELECT count(*) FROM events WHERE id > 5000 AND id < 100;")"
check "the right rows, not just the right count" "5000,5001,5002" \
      "$(idx "SELECT string_agg(id::text, ',' ORDER BY id) FROM (SELECT id FROM events WHERE id >= 5000 AND id < 5003) x;")"

echo "3. text ranges and prefix matching"
check "text greater than"     "1"    "$(idx "SELECT count(*) FROM events WHERE who > 'user-019999';")"
check "a prefix match"        "9999" "$(idx "SELECT count(*) FROM events WHERE who LIKE 'user-00%';")"
check "and it agrees with the long way round" "9999" \
      "$(sql "SELECT count(*) FROM events WHERE substr(who,1,7) = 'user-00';")"

echo "4. equality on the first column, range on the second"
check "day = 50 and who above a bound" "50" \
      "$(idx "SELECT count(*) FROM events WHERE day = 50 AND who > 'user-005049';")"
check "day = 50, all of it"            "100" "$(idx "SELECT count(*) FROM events WHERE day = 50;")"
check "a range on the first column only" "200" \
      "$(idx "SELECT count(*) FROM events WHERE day >= 50 AND day < 52;")"

echo "4b. a bound of another type"
# The operator hands over a value of its own type. Every string type encodes
# alike, so a text bound on a name column is compared as it is. An integer
# bound of another width is restated at the column's width before it is
# encoded (objkv_index.rs, fit_int): a literal the column cannot hold keeps
# its Postgres answer instead of being refused or mis-sorted.
check "a text bound on a name column works" "t" \
      "$(idx "SELECT count(*) > 0 FROM pg_class WHERE relname LIKE 'pg\_cl%';")"
check "an int8 literal above int4 range: > matches nothing" "0" \
      "$(idx "SELECT count(*) FROM events WHERE id > 5000000000;")"
check "and < matches everything" "$ROWS" \
      "$(idx "SELECT count(*) FROM events WHERE id < 5000000000;")"
check "below int4 range: > matches everything" "$ROWS" \
      "$(idx "SELECT count(*) FROM events WHERE id > -5000000000;")"
check "and = matches nothing" "0" \
      "$(idx "SELECT count(*) FROM events WHERE id = 5000000000;")"
# The other way round: a bigint and a smallint column probed with a plain
# int4 literal, cross-checked against a forced table read (tbl, via agree).
sql "CREATE TABLE widths (b bigint PRIMARY KEY, s smallint NOT NULL) USING objkv;" >/dev/null
sql "CREATE INDEX widths_s ON widths (s);" >/dev/null
sql "INSERT INTO widths SELECT CASE WHEN g <= 500 THEN g ELSE g * 3000000000::bigint END, (g % 200) - 100 FROM generate_series(1,1000) g;" >/dev/null
check "the rows are in" "1000" "$(sql "SELECT count(*) FROM widths;")"
shows "bigint = int4 literal goes through the index" "Index" "$(plan "SELECT s FROM widths WHERE b = 42;")"
agree "bigint = int4 literal, the right row"     "SELECT s FROM widths WHERE b = 42;"
check "and exactly one"                          "1" "$(idx "SELECT count(*) FROM widths WHERE b = 42;")"
agree "bigint > int4 literal"                    "SELECT count(*) FROM widths WHERE b > 400;"
agree "bigint BETWEEN two int4 literals"         "SELECT string_agg(b::text, ',' ORDER BY b) FROM widths WHERE b BETWEEN 10 AND 13;"
agree "bigint = an int8 literal above int4 range" "SELECT s FROM widths WHERE b = 1503000000000;"
shows "smallint = int4 literal goes through the index" "Index" "$(plan "SELECT count(*) FROM widths WHERE s = 7;")"
agree "smallint = int4 literal"                  "SELECT count(*) FROM widths WHERE s = 7;"
agree "smallint = a negative int4 literal"       "SELECT count(*) FROM widths WHERE s = -99;"
agree "smallint < int4 literal"                  "SELECT count(*) FROM widths WHERE s < -90;"
check "smallint = a literal it cannot hold"      "0"    "$(idx "SELECT count(*) FROM widths WHERE s = 100000;")"
check "smallint < a literal it cannot hold"      "1000" "$(idx "SELECT count(*) FROM widths WHERE s < 100000;")"
check "smallint > a literal below its range"     "1000" "$(idx "SELECT count(*) FROM widths WHERE s > -100000;")"
check "smallint > a literal it cannot hold"      "0"    "$(idx "SELECT count(*) FROM widths WHERE s > 100000;")"
agree "smallint IN a list with one member too wide" "SELECT count(*) FROM widths WHERE s IN (7, 100000);"
sql "DROP TABLE widths;" >/dev/null

echo "5. a range reads the rows it needs and not the table"
trace_mark
shows "the plan uses the index" "Index Scan" \
      "$(idx "EXPLAIN (ANALYZE, TIMING OFF) SELECT count(*) FROM events WHERE id BETWEEN 5000 AND 5100;")"
CAND=$(trace_candidates)
echo "  the scan looked at $CAND entries out of $ROWS"
check "it did not walk the whole index" "t" "$([ "$CAND" -lt 500 ] && echo t || echo f)"

echo "5b. a LIMIT reads what it asked for, not the whole range"
# The range is the entire table; the scan reads a window at a time.
trace_mark
check "five rows"  "5" "$(idx "SELECT count(*) FROM (SELECT id FROM events WHERE id > 0 LIMIT 5) x;")"
WINDOW=$(trace_max_candidates)
echo "  the largest window it read was ${WINDOW:-0} entries, of $ROWS in range"
check "it did not read the range to answer a LIMIT" "t" "$([ "${WINDOW:-999999}" -le 512 ] && echo t || echo f)"

echo "5c. the index has an order, and the planner can use it"
nosort "ORDER BY needs no sort step" "SELECT id FROM events ORDER BY id LIMIT 5;"
check "ascending, and it is the right five" "1,2,3,4,5" \
      "$(idx "SELECT string_agg(id::text, ',') FROM (SELECT id FROM events ORDER BY id LIMIT 5) x;")"
TOP=$(sql "SELECT max(id) FROM events;")
check "descending gives the top five" "t" \
      "$(idx "SELECT string_agg(id::text, ',' ORDER BY id DESC) = '$TOP,$((TOP-1)),$((TOP-2)),$((TOP-3)),$((TOP-4))' FROM (SELECT id FROM events ORDER BY id DESC LIMIT 5) x;")"
check "descending inside a range"  "5100,5099,5098" \
      "$(idx "SELECT string_agg(id::text, ',') FROM (SELECT id FROM events WHERE id <= 5100 ORDER BY id DESC LIMIT 3) x;")"
check "a descending scan of everything is still every row" "$ROWS" \
      "$(idx "SELECT count(*) FROM (SELECT id FROM events ORDER BY id DESC) x;")"
check "and a column can be declared descending" "CREATE INDEX" "$(sql "CREATE INDEX events_desc ON events (day DESC);")"
agree "which answers a range the same way" "SELECT count(*) FROM events WHERE day > 20;"

echo "6. a range still tells the truth after the rows move"
sql "UPDATE events SET id = id + $((ROWS * 10)) WHERE id BETWEEN 5000 AND 5100;" >/dev/null
check "the old range is empty"      "0"   "$(idx "SELECT count(*) FROM events WHERE id BETWEEN 5000 AND 5100;")"
check "and the new one holds them"  "101" "$(idx "SELECT count(*) FROM events WHERE id > $((ROWS * 10));")"
sql "DELETE FROM events WHERE id > $((ROWS * 10));" >/dev/null
check "deleted, and gone from the range" "0" "$(idx "SELECT count(*) FROM events WHERE id > $((ROWS * 10));")"
check "the rest is untouched" "$((ROWS - 101))" "$(sql "SELECT count(*) FROM events;")"

echo "7. floating point"
# Sign bit flipped on positives, every bit inverted on negatives: the bit
# pattern sorts, with NaN above every number and NULL above that.
sql "CREATE TABLE temps (id int PRIMARY KEY, c float8, f float4) USING objkv;" >/dev/null
sql "INSERT INTO temps SELECT g, (g - 500) / 4.0, (g - 500) / 8.0 FROM generate_series(1,1000) g;" >/dev/null
sql "INSERT INTO temps VALUES (2001, 'NaN', 0), (2002, 'Infinity', 0), (2003, '-Infinity', 0), (2004, NULL, 0);" >/dev/null
sql "CREATE INDEX temps_c ON temps (c);" >/dev/null
sql "CREATE INDEX temps_f ON temps (f);" >/dev/null
check "the index was built"  "1" "$(sql "SELECT count(*) FROM pg_class WHERE relname = 'temps_c';")"
check "a range below zero"   "100" "$(idx "SELECT count(*) FROM temps WHERE c >= -50 AND c < -25;")"
check "a range across zero"  "41"  "$(idx "SELECT count(*) FROM temps WHERE c BETWEEN -5 AND 5;")"
check "equality on a negative" "1" "$(idx "SELECT count(*) FROM temps WHERE c = -124.75;")"
agree "single precision against a plain literal" "SELECT count(*) FROM temps WHERE f BETWEEN -2.5 AND 2.5;"
check "and it agrees with reading the table" "t" "$(idx "SELECT (SELECT count(*) FROM temps WHERE c < 0) = 500;")"
check "the smallest value is the lowest one" "-Infinity" "$(idx "SELECT min(c) FROM temps;")"
check "ordering puts NaN above every number" "NaN" \
      "$(idx "SELECT c FROM (SELECT c FROM temps WHERE c IS NOT NULL ORDER BY c DESC LIMIT 1) x;")"
check "and NULL above even that" "1" "$(idx "SELECT count(*) FROM temps WHERE c IS NULL;")"
# Postgres compares these two equal, so one unique index cannot hold both.
sql "CREATE TABLE zeros (z float8 PRIMARY KEY) USING objkv;" >/dev/null
sql "INSERT INTO zeros VALUES (0.0);" >/dev/null
contains "minus zero is the same key as zero" "duplicate key" "$(sql "INSERT INTO zeros VALUES (-0.0);")"

finish "ranges"
