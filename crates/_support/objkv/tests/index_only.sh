#!/usr/bin/env bash
#
# Index-only scans: answering from the entry without reading the row. The
# entry carries every indexed value; what is checked is the way back, from
# stored bytes to exact values, type by type, against a forced table read.
# The timing comparison runs only with OBJKV_TIMING=1.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-5000}"
IDX_OPTS="-c enable_bitmapscan=off"

only() {  # only <what> <query>: the plan is an index-only scan
    shows "$1" "Index Only Scan" "$(plan "$2")"
}

echo "0. a table of every type an index can order"
fresh_cluster
sql "CREATE TABLE readings (
        id int PRIMARY KEY, big bigint, small smallint, flag bool,
        c float8, f float4, when_ date, at_ timestamp,
        tag text COLLATE \"C\", vc varchar COLLATE \"C\", u uuid) USING objkv;" >/dev/null
sql "INSERT INTO readings SELECT g, g::bigint * 1000000000, (g % 300) - 150, g % 2 = 0,
        (g - 2500) / 4.0, (g - 2500) / 8.0,
        DATE '2020-01-01' + g, TIMESTAMP '2020-01-01 00:00:00' + (g || ' minutes')::interval,
        'tag-' || lpad(g::text, 5, '0'), 'vc-' || g,
        ('00000000-0000-0000-0000-' || lpad(g::text, 12, '0'))::uuid
     FROM generate_series(1,$ROWS) g;" >/dev/null
sql "CREATE INDEX readings_tag ON readings (tag);" >/dev/null
sql "CREATE INDEX readings_all ON readings (big, small, flag, c, f, when_, at_, vc, u);" >/dev/null
check "the rows are in"  "$ROWS" "$(sql "SELECT count(*) FROM readings;")"
check "both indexes built" "2" \
      "$(sql "SELECT count(*) FROM pg_class WHERE relname IN ('readings_tag','readings_all');")"

echo "1. the plan stops reading rows"
only "one indexed column"          "SELECT tag FROM readings WHERE tag > 'tag-04990';"
only "counting through the index"  "SELECT count(*) FROM readings WHERE tag > 'tag-04990';"
only "a leading column of a wide index" "SELECT big FROM readings WHERE big > 4990000000000;"

echo "2. the values come back exactly, type by type"
agree "text"              "SELECT string_agg(tag, ',' ORDER BY tag) FROM readings WHERE tag > 'tag-04997';"
agree "varchar"           "SELECT string_agg(vc, ',' ORDER BY vc) FROM readings WHERE big > 4998000000000;"
agree "bigint"            "SELECT sum(big) FROM readings WHERE big > 4990000000000;"
agree "smallint"          "SELECT string_agg(small::text, ',' ORDER BY big) FROM readings WHERE big > 4996000000000;"
agree "boolean"           "SELECT string_agg(flag::text, ',' ORDER BY big) FROM readings WHERE big > 4996000000000;"
agree "double precision"  "SELECT string_agg(c::text, ',' ORDER BY big) FROM readings WHERE big > 4996000000000;"
agree "real"              "SELECT string_agg(f::text, ',' ORDER BY big) FROM readings WHERE big > 4996000000000;"
agree "date"              "SELECT string_agg(when_::text, ',' ORDER BY big) FROM readings WHERE big > 4996000000000;"
agree "timestamp"         "SELECT string_agg(at_::text, ',' ORDER BY big) FROM readings WHERE big > 4996000000000;"
agree "uuid"              "SELECT string_agg(u::text, ',' ORDER BY big) FROM readings WHERE big > 4996000000000;"
agree "negative numbers"  "SELECT string_agg(small::text || ':' || c::text, ',' ORDER BY big) FROM readings WHERE big < 3000000000;"

echo "3. NULLs come back as NULLs, not as values"
sql "INSERT INTO readings (id, big, tag) VALUES (90001, 9000000000000, 'zzz-null');" >/dev/null
agree "a row with almost nothing in it" \
      "SELECT coalesce(small::text,'-') || '/' || coalesce(vc,'-') || '/' || coalesce(u::text,'-')
       FROM readings WHERE big = 9000000000000;"
agree "and it is genuinely null"  "SELECT count(*) FROM readings WHERE big > 8000000000000 AND vc IS NULL;"

echo "4. it agrees with reading the rows, at every size"
agree "the whole index"  "SELECT count(*), sum(big), max(tag) FROM readings;"
agree "one row"          "SELECT tag FROM readings WHERE tag = 'tag-02500';"
agree "backwards"        "SELECT string_agg(tag, ',') FROM (SELECT tag FROM readings ORDER BY tag DESC LIMIT 3) x;"

echo "5. and it never goes stale"
sql "UPDATE readings SET tag = 'tag-99999' WHERE id = 1;" >/dev/null
agree "an updated value"        "SELECT count(*) FROM readings WHERE tag = 'tag-99999';"
agree "and the old one is gone" "SELECT count(*) FROM readings WHERE tag = 'tag-00001';"
sql "DELETE FROM readings WHERE id = 2;" >/dev/null
agree "a deleted one"           "SELECT count(*) FROM readings WHERE tag = 'tag-00002';"
agree "the table still adds up" "SELECT count(*) FROM readings;"

echo "6. the point of it, in time"
if timing_enabled; then
    # What an index-only scan saves is reading the row, so it is worth most on
    # rows carrying a payload the query never asks for.
    sql "CREATE TABLE wide (k int, pad text COLLATE \"C\") USING objkv;" >/dev/null
    sql "INSERT INTO wide SELECT g, repeat('x', 2000) FROM generate_series(1,$ROWS) g;" >/dev/null
    sql "CREATE INDEX wide_k ON wide (k);" >/dev/null
    check "the wide rows are in" "$ROWS" "$(sql "SELECT count(*) FROM wide;")"
    ms() {
        idx "EXPLAIN (ANALYZE, TIMING OFF, FORMAT JSON) $1" \
            | grep -o '"Execution Time": [0-9.]*' | head -1 | sed 's/.*: //'
    }
    faster() { check "$1" "t" "$(awk -v a="${2:-1}" -v b="${3:-0}" 'BEGIN{print (a < b) ? "t" : "f"}')"; }
    N_ONLY=$(ms "SELECT count(tag) FROM readings WHERE tag > 'tag-00000';")
    N_ROWS=$(ms "SELECT count(id)  FROM readings WHERE tag > 'tag-00000';")
    echo "  narrow rows: ${N_ONLY}ms from the index alone, ${N_ROWS}ms fetching each row"
    W_ONLY=$(ms "SELECT count(k)   FROM wide WHERE k > 0;")
    W_ROWS=$(ms "SELECT count(pad) FROM wide WHERE k > 0;")
    echo "  2KB rows:    ${W_ONLY}ms from the index alone, ${W_ROWS}ms fetching each row"
    faster "on rows worth not reading, it is faster" "$W_ONLY" "$W_ROWS"
else
    echo "  (timing comparison skipped; OBJKV_TIMING=1 enables it)"
fi

finish "answered from the index alone"
