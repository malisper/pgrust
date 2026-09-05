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

echo "3. two concurrent inserts of one unique value (known divergence from Postgres)"
sql "CREATE TABLE demo_uniq (id int, tag text COLLATE \"C\") USING objkv;" >/dev/null
sql "CREATE UNIQUE INDEX demo_uniq_tag ON demo_uniq (tag);" >/dev/null
# Known divergence from Postgres. There, B's INSERT of a value A has staged
# blocks on A's transaction, and when A commits B fails with 23505 duplicate
# key; nobody gets 40001. objkv takes no key locks: both stage the value, B
# commits first, and A is refused at COMMIT with 40001 (first committer wins;
# see the isolation contract in objkv_am.rs and docs/objkv.md). This step
# pins the chosen contract so a change to it is noticed, not to bless it.
OUT=$(psqlx -d postgres -tA <<SQL 2>&1
BEGIN;
INSERT INTO demo_uniq VALUES (1, 'clash');
\\! psql -h "$SOCKDIR" -p "$PORT" -d postgres -tAc "INSERT INTO demo_uniq VALUES (2, 'clash');" >/dev/null 2>&1
COMMIT;
SQL
)
contains "known divergence from Postgres: the loser gets 40001 at COMMIT, where Postgres would block B and then raise 23505" \
         "serialize access" "$OUT"
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

echo "8. a char(n) primary key"
# bpchar pads the stored value to n and compares it with the padding ignored,
# so 'a', 'a  ' and 'a'::char(3) are one value. The key trims trailing blanks
# before it is encoded (index_key.rs, Col::Bpchar), on the stored entry and
# on the probe alike; an unpadded literal in WHERE meets the padded entry.
sql "CREATE TABLE demo_bp (c char(3) COLLATE \"C\" PRIMARY KEY, v int) USING objkv;" >/dev/null
sql "INSERT INTO demo_bp VALUES ('a', 1), ('ab', 2), ('abc', 3), ('a b', 4);" >/dev/null
check "four rows in"                                "4" "$(sql "SELECT count(*) FROM demo_bp;")"
shows "the lookup goes through the index"           "Index" "$(plan "SELECT v FROM demo_bp WHERE c = 'a';")"
agree "where c = 'a' finds the padded entry"        "SELECT v FROM demo_bp WHERE c = 'a';"
check "and exactly one row"                         "1" "$(idx "SELECT count(*) FROM demo_bp WHERE c = 'a';")"
agree "a padded probe finds the same row"           "SELECT v FROM demo_bp WHERE c = 'a  ';"
agree "an inner blank is part of the value"         "SELECT v FROM demo_bp WHERE c = 'a b';"
agree "and 'a ' is not 'ab'"                        "SELECT v FROM demo_bp WHERE c = 'a ';"
agree "a range, in bpchar order"                    "SELECT string_agg(c::text, ',' ORDER BY c) FROM demo_bp WHERE c > 'a';"
contains "a padded duplicate is a duplicate"        "duplicate key" "$(sql "INSERT INTO demo_bp VALUES ('a ', 5);")"
check "the index holds the trimmed value: the row comes back padded from the table, not the key" "a  |1" \
      "$(PGOPTIONS="$PGOPTIONS -c enable_seqscan=off" psqlx -d postgres -tAc "SELECT c, v FROM demo_bp WHERE c = 'a';" 2>&1)"

echo "9. a text primary key without COLLATE \"C\" is refused, and says so"
# Every text key in these scripts says COLLATE "C" because that is the only
# collation the key encoding can honour: byte order is value order only
# there. A plain text column takes the database default, which is the
# collation "default" (oid 100) even in a C-locale cluster, and the index
# refuses it rather than sort by bytes and call it ORDER BY. This is a known
# limitation; the step pins the exact message a user meets.
OUT=$(sql "CREATE TABLE demo_nocoll (k text PRIMARY KEY, v int) USING objkv; INSERT INTO demo_nocoll VALUES ('x', 1);")
contains "refused with the exact message" \
         "objkv indexes support only the C collation; column 1 of index \"demo_nocoll_pkey\" uses another" "$OUT"
check "and the table was not left behind" "0" "$(sql "SELECT count(*) FROM pg_class WHERE relname = 'demo_nocoll';")"
OUT=$(sql "CREATE TABLE demo_nocoll2 (v int PRIMARY KEY, k text) USING objkv; CREATE INDEX demo_nocoll2_k ON demo_nocoll2 (k);")
contains "the same for a secondary index on such a column" \
         "objkv indexes support only the C collation; column 1 of index \"demo_nocoll2_k\" uses another" "$OUT"
check "while the same column declared COLLATE \"C\" is accepted" "CREATE INDEX" \
      "$(sql "CREATE TABLE demo_ccoll (v int PRIMARY KEY, k text COLLATE \"C\") USING objkv; CREATE INDEX demo_ccoll_k ON demo_ccoll (k);" | tail -1)"

finish "objkv indexes"
