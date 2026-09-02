#!/usr/bin/env bash
#
# ALTER TABLE on an objkv table: the forms that only write catalog rows work,
# and the two that rewrite the table (ALTER COLUMN TYPE, SET ACCESS METHOD)
# are refused rather than reading old rows through a new layout. Run twice:
# with the catalogs on disk, then with them lifted into the bucket, since
# those write catalog rows by different paths.
#
. "$(dirname "$0")/server.sh"

run_cases() {
    local where="$1"
    sql "DROP TABLE IF EXISTS alt CASCADE;" >/dev/null
    sql "DROP SCHEMA IF EXISTS alt_s CASCADE;" >/dev/null
    sql "CREATE TABLE alt (id int PRIMARY KEY, tag text COLLATE \"C\", n int) USING objkv;" >/dev/null
    sql "INSERT INTO alt SELECT g, 'tag-' || g, g FROM generate_series(1,20) g;" >/dev/null

    echo "  -- renames ($where)"
    check "rename a column"  "3"  "$(sql "ALTER TABLE alt RENAME COLUMN n TO num; SELECT num FROM alt WHERE id = 3;" | tail -1)"
    check "rename the table" "20" "$(sql "ALTER TABLE alt RENAME TO alt2; SELECT count(*) FROM alt2;" | tail -1)"
    sql "ALTER TABLE alt2 RENAME TO alt;" >/dev/null

    echo "  -- defaults and not-null ($where)"
    check "set a default"    "99" "$(sql "ALTER TABLE alt ALTER COLUMN num SET DEFAULT 99; INSERT INTO alt (id, tag) VALUES (100,'d'); SELECT num FROM alt WHERE id = 100;" | tail -1)"
    check "drop it again"    "ALTER TABLE" "$(sql "ALTER TABLE alt ALTER COLUMN num DROP DEFAULT;")"
    check "set not null"     "ALTER TABLE" "$(sql "ALTER TABLE alt ALTER COLUMN tag SET NOT NULL;")"
    contains "and it is enforced" "null value" "$(sql "INSERT INTO alt (id, tag) VALUES (101, NULL);")"
    check "drop not null"    "ALTER TABLE" "$(sql "ALTER TABLE alt ALTER COLUMN tag DROP NOT NULL;")"

    echo "  -- constraints ($where)"
    check "add a check"      "ALTER TABLE" "$(sql "ALTER TABLE alt ADD CONSTRAINT c_pos CHECK (id > 0);")"
    contains "and it is enforced" "violates check constraint" "$(sql "INSERT INTO alt VALUES (-1,'no',1);")"
    check "drop the check"   "ALTER TABLE" "$(sql "ALTER TABLE alt DROP CONSTRAINT c_pos;")"
    check "add a unique constraint" "ALTER TABLE" "$(sql "ALTER TABLE alt ADD CONSTRAINT c_uniq UNIQUE (tag);")"
    check "its index is objkv's" "objkv_btree" "$(am_of c_uniq)"
    contains "and it is enforced" "duplicate key" "$(sql "INSERT INTO alt VALUES (102,'tag-3',1);")"

    echo "  -- columns ($where)"
    check "add a column"     "<null>" "$(sql "ALTER TABLE alt ADD COLUMN extra text; SELECT coalesce(extra,'<null>') FROM alt WHERE id = 3;" | tail -1)"
    check "add one with a default" "7" "$(sql "ALTER TABLE alt ADD COLUMN more int DEFAULT 7; SELECT more FROM alt WHERE id = 3;" | tail -1)"
    check "the old rows got it too" "21" "$(sql "SELECT count(*) FROM alt WHERE more = 7;")"
    check "drop a column"    "21" "$(sql "ALTER TABLE alt DROP COLUMN extra; SELECT count(*) FROM alt;" | tail -1)"
    check "the rest still reads" "tag-3" "$(sql "SELECT tag FROM alt WHERE id = 3;")"

    echo "  -- moving and labelling ($where)"
    sql "CREATE SCHEMA alt_s;" >/dev/null
    check "set schema"       "21" "$(sql "ALTER TABLE alt SET SCHEMA alt_s; SELECT count(*) FROM alt_s.alt;" | tail -1)"
    sql "ALTER TABLE alt_s.alt SET SCHEMA public;" >/dev/null
    check "set owner"        "ALTER TABLE" "$(sql "ALTER TABLE alt OWNER TO CURRENT_USER;")"
    check "comment"          "hello" "$(sql "COMMENT ON TABLE alt IS 'hello'; SELECT obj_description('alt'::regclass);" | tail -1)"
    check "set statistics"   "ALTER TABLE" "$(sql "ALTER TABLE alt ALTER COLUMN num SET STATISTICS 100;")"

    echo "  -- what rewrites the table is refused, not answered wrongly ($where)"
    contains "changing a column's type is refused" "cannot rewrite objkv table" \
             "$(sql "ALTER TABLE alt ALTER COLUMN num TYPE bigint;")"
    check "and the value is untouched" "3" "$(sql "SELECT num FROM alt WHERE id = 3;")"
    contains "changing the access method is refused" "cannot rewrite objkv table" \
             "$(sql "ALTER TABLE alt SET ACCESS METHOD heap;")"
    check "the table still reads" "21" "$(sql "SELECT count(*) FROM alt;")"
    check "and the workaround works" "3" \
          "$(sql "ALTER TABLE alt ADD COLUMN num8 bigint; UPDATE alt SET num8 = num; ALTER TABLE alt DROP COLUMN num; SELECT num8 FROM alt WHERE id = 3;" | tail -1)"
}

echo "0. an ordinary cluster"
fresh_cluster
run_cases "rows in the bucket, catalogs on disk"

echo "1. and again with the catalogs in the bucket too"
sql "DROP TABLE IF EXISTS alt CASCADE;" >/dev/null
sql "DROP SCHEMA IF EXISTS alt_s CASCADE;" >/dev/null
install_lift
lift_all
# The running server keeps its pre-flip view; the restart reads the marker.
stop
boot
run_cases "everything in the bucket"

finish "ALTER TABLE"
