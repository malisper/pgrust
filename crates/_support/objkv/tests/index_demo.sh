#!/usr/bin/env bash
#
# Indexes on objkv tables, stored in the bucket beside the rows: primary keys,
# duplicates refused in both code paths, concurrent inserts of one unique
# value, concurrent NULLs, a point lookup that does not scan the table, and
# the types the catalogs are keyed on. ROWS sizes the lookup; the timing
# comparison runs only with OBJKV_TIMING=1.
#
. "$(dirname "$0")/server.sh"
ROWS="${ROWS:-100000}"
export PGOPTIONS="-c max_parallel_workers_per_gather=0"

fresh_cluster

echo "1. a primary key on an objkv table"
sql "CREATE TABLE demo_people (id int PRIMARY KEY, name text COLLATE \"C\") USING objkv;" >/dev/null
sql "INSERT INTO demo_people VALUES (1,'alice'),(2,'bob'),(3,'carol');" >/dev/null
check "the index is objkv's, not a local btree" "objkv_btree" "$(am_of demo_people_pkey)"
check "lookup by key" "bob" "$(sql "SELECT name FROM demo_people WHERE id = 2;")"

echo "2. duplicates are refused"
contains "in a later transaction" "duplicate key" "$(sql "INSERT INTO demo_people VALUES (2,'imposter');")"
# Both stage the same key, so this is caught against pending writes, not the store.
contains "inside one statement" "duplicate key" "$(sql "INSERT INTO demo_people VALUES (9,'x'),(9,'y');")"
check "nothing was left behind" "3" "$(sql "SELECT count(*) FROM demo_people;")"

echo "3. two concurrent inserts of one unique value"
sql "CREATE TABLE demo_uniq (id int, tag text COLLATE \"C\") USING objkv;" >/dev/null
sql "CREATE UNIQUE INDEX demo_uniq_tag ON demo_uniq (tag);" >/dev/null
# B commits the same value first, and A's commit finds the collision.
OUT=$(psqlx -d postgres -tA <<SQL 2>&1
BEGIN;
INSERT INTO demo_uniq VALUES (1, 'clash');
\\! psql -h "$SOCKDIR" -p "$PORT" -d postgres -tAc "INSERT INTO demo_uniq VALUES (2, 'clash');" >/dev/null 2>&1
COMMIT;
SQL
)
contains "the loser gets 40001" "serialize access" "$OUT"
check "exactly one row survived" "1" "$(sql "SELECT count(*) FROM demo_uniq WHERE tag='clash';")"

echo "4. two concurrent NULLs into the same unique column"
OUT=$(psqlx -d postgres -tA <<SQL 2>&1
BEGIN;
INSERT INTO demo_uniq VALUES (3, NULL);
\\! psql -h "$SOCKDIR" -p "$PORT" -d postgres -tAc "INSERT INTO demo_uniq VALUES (4, NULL);" >/dev/null 2>&1
COMMIT;
SQL
)
if echo "$OUT" | grep -qi "error"; then fail "a second NULL was refused: $OUT"; else ok "both NULLs accepted"; fi
check "both NULL rows are there" "2" "$(sql "SELECT count(*) FROM demo_uniq WHERE tag IS NULL;")"

echo "5. a point lookup on $ROWS rows does not scan them"
sql "CREATE TABLE demo_big (id int PRIMARY KEY, payload text COLLATE \"C\") USING objkv;" >/dev/null
sql "INSERT INTO demo_big SELECT g, 'row-' || g FROM generate_series(1, $ROWS) g;" >/dev/null
PROBE=$(( ROWS / 2 + 1 ))
shows "the planner chooses the index" "Index Scan" "$(sql "EXPLAIN SELECT payload FROM demo_big WHERE id = $PROBE;")"
check "and it finds the right row" "row-$PROBE" "$(sql "SELECT payload FROM demo_big WHERE id = $PROBE;")"
if timing_enabled; then
    # One session per measurement, so the numbers are query time, not setup.
    timed() { # timed <extra psql options> <query>
        PGOPTIONS="$PGOPTIONS $1" psqlx -d postgres -tA -c '\timing on' -c "$2" 2>&1 \
            | grep '^Time:' | tail -1 | sed 's/Time: \([0-9.]*\) ms.*/\1/'
    }
    IDX=$(timed "" "SELECT payload FROM demo_big WHERE id = $PROBE;")
    SEQ=$(timed "-c enable_indexscan=off -c enable_bitmapscan=off" "SELECT payload FROM demo_big WHERE id = $PROBE;")
    echo "  index scan: ${IDX}ms    forced sequential scan: ${SEQ}ms    rows: $ROWS"
    check "the lookup is at least 10x faster than reading the table" "t" \
          "$(awk -v i="${IDX:-1}" -v s="${SEQ:-0}" 'BEGIN{print (s >= i * 10) ? "t" : "f"}')"
else
    echo "  (timing comparison skipped; OBJKV_TIMING=1 enables it)"
fi

echo "6. options the encoding cannot honour are refused"
# NULLS NOT DISTINCT inverts the rule the key shape is built on; accepting it
# would leave the constraint silently unenforced.
contains "NULLS NOT DISTINCT refused" "NULLS NOT DISTINCT" \
         "$(sql "CREATE UNIQUE INDEX demo_nd ON demo_uniq (tag) NULLS NOT DISTINCT;")"

echo "7. the types the catalogs are keyed on"
# Every catalog index is keyed on oid and name; name is a raw 64-byte field.
sql "CREATE TABLE demo_catshape (relid oid PRIMARY KEY, relname name) USING objkv;" >/dev/null
sql "CREATE INDEX demo_catshape_name ON demo_catshape (relname);" >/dev/null
sql "INSERT INTO demo_catshape VALUES (1259,'pg_class'),(1249,'pg_attribute'),(2147483648,'high_oid'),(4294967295,'highest_oid');" >/dev/null
check "lookup by oid"  "pg_class" "$(sql "SELECT relname FROM demo_catshape WHERE relid = 1259;")"
check "lookup by name" "1249"     "$(sql "SELECT relid FROM demo_catshape WHERE relname = 'pg_attribute';")"
check "oids above 2^31 order as unsigned" "1249,1259,2147483648,4294967295" \
      "$(sql "SELECT string_agg(relid::text, ',' ORDER BY relid) FROM demo_catshape;")"
contains "an oid primary key enforces uniqueness" "duplicate key" \
         "$(sql "INSERT INTO demo_catshape VALUES (1259,'duplicate');")"

finish "objkv indexes"
