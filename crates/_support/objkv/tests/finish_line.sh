#!/usr/bin/env bash
#
# The finish line: delete the machine's disk, boot a blank one, get the
# database back. Two things cross the deletion, both addresses rather than
# data: a blank initdb directory and the marker file naming the bucket.
#
. "$(dirname "$0")/server.sh"
SAVED="$WORK/marker"   # kept outside PGDATA, since PGDATA dies
ROWS="${ROWS:-500}"

echo "0. an empty bucket and a cluster built from nothing"
fresh_cluster
check "the bucket holds only the boot's own objects" "t" \
      "$([ "$("$HERE/bucket.py" count)" -lt 20 ] && echo t || echo f)"

echo "1. plant a database worth losing"
install_lift
must "CREATE TABLE ledger (id int PRIMARY KEY, who text COLLATE \"C\", amount int) USING objkv;" >/dev/null
must "INSERT INTO ledger SELECT g, 'payee-' || g, g * 7 FROM generate_series(1,$ROWS) g;" >/dev/null
must "CREATE INDEX ledger_who ON ledger USING objkv_btree (who);" >/dev/null
must "CREATE VIEW big_payments AS SELECT who FROM ledger WHERE amount > 3000;" >/dev/null
# An update and a delete, so the history has holes rather than one clean pass.
must "UPDATE ledger SET amount = 999999 WHERE id = 7;" >/dev/null
must "DELETE FROM ledger WHERE id % 100 = 0;" >/dev/null

WANT_COUNT=$(must "SELECT count(*) FROM ledger;")
WANT_SUM=$(must "SELECT sum(amount) FROM ledger;")
WANT_ONE=$(must "SELECT who FROM ledger WHERE id = 7;")
WANT_VIEW=$(must "SELECT count(*) FROM big_payments;")
WANT_GONE=$(must "SELECT count(*) FROM ledger WHERE id = 100;")
echo "  $WANT_COUNT rows, sum $WANT_SUM, view sees $WANT_VIEW"

echo "1b. what the lift refuses to carry"
# Only catalogs and objkv relations exist after the flip. A heap table's rows
# are in a file the deletion below takes with it, and a sequence's state is a
# local file too; so a lift that finds either refuses and names it, rather
# than producing a database where the table exists and reads empty.
must "CREATE TABLE stayed_local (id int, note text);" >/dev/null
must "INSERT INTO stayed_local VALUES (1,'on local disk'),(2,'and so is this'),(3,'gone after the flip');" >/dev/null
OUT=$(sqlv "SELECT pgrust_objkv_lift();")
contains "a heap table with rows is refused"     "stored outside the bucket" "$OUT"
contains "and named, with its kind and its AM"   "public.stayed_local (table, heap)" "$OUT"
contains "as the one relation (its TOAST table is folded into it)" "1 relation(s)" "$OUT"
check    "with sqlstate 0A000 feature_not_supported" "0A000" "$(sqlstate_of "$OUT")"
must "DROP TABLE stayed_local;" >/dev/null
must "CREATE TABLE numbered (id serial PRIMARY KEY, note text COLLATE \"C\") USING objkv;" >/dev/null
OUT=$(sqlv "SELECT pgrust_objkv_lift();")
contains "a serial column's sequence is refused" "sequences are not lifted" "$OUT"
contains "and named"                             "numbered_id_seq" "$OUT"
check    "with sqlstate 0A000 feature_not_supported" "0A000" "$(sqlstate_of "$OUT")"
must "DROP TABLE numbered;" >/dev/null
check "dropping them leaves the cluster quiet" "0" \
      "$(sql "SELECT count(*) FROM pg_class WHERE relname IN ('stayed_local','numbered','numbered_id_seq');")"

echo "2. move the catalogs into the bucket"
# The refusals above staged nothing, so this lift is the first.
lift_all
cp "$PGDATA/objkv_catalogs" "$SAVED" || die "no marker to save"
echo "  saved the marker outside the data directory:"
sed 's/^/    /' "$SAVED"
# The running server read the marker once, at boot; catalog writes from it
# are refused until it restarts.
contains "DDL before the restart is refused" "moved to the bucket" \
         "$(sql "CREATE TABLE too_soon (id int) USING objkv;")"
stop
boot
# CREATE DATABASE copies files, and a lifted database's catalogs are rows in
# the bucket: from template0 as much as any other, the copy would have no
# readable catalogs, so it is refused outright.
OUT=$(sqlv "CREATE DATABASE too_late TEMPLATE template0;")
contains "CREATE DATABASE after the flip is refused" "catalogs are in the objkv bucket" "$OUT"
check    "with sqlstate 0A000 feature_not_supported" "0A000" "$(sqlstate_of "$OUT")"
check    "and no database was left behind" "0" "$(sql "SELECT count(*) FROM pg_database WHERE datname = 'too_late';")"

echo "2b. and a whole schema built after the flip, catalogs and all"
# Its pg_class, pg_attribute and pg_index rows are objkv rows from the start,
# never written to the local file that is about to be deleted.
must "CREATE TABLE ledger_after (id int PRIMARY KEY, who text COLLATE \"C\") USING objkv;" >/dev/null
must "INSERT INTO ledger_after SELECT g, 'later-' || g FROM generate_series(1,50) g;" >/dev/null
must "CREATE INDEX ledger_after_who ON ledger_after (who);" >/dev/null
must "CREATE VIEW after_view AS SELECT who FROM ledger_after WHERE id > 40;" >/dev/null
must "CREATE TABLE dropped_after (id int) USING objkv;" >/dev/null
must "DROP TABLE dropped_after;" >/dev/null
WANT_AFTER=$(must "SELECT count(*) FROM ledger_after;")
WANT_AFTER_VIEW=$(must "SELECT count(*) FROM after_view;")
echo "  $WANT_AFTER rows, view sees $WANT_AFTER_VIEW, one table created and dropped"
stop
echo "  bucket holds $("$HERE/bucket.py" count) objects, $("$HERE/bucket.py" bytes) bytes"

echo "3. destroy the machine"
rm -rf "$PGDATA"
[ -d "$PGDATA" ] && die "the data directory is still there"
ok "$PGDATA is gone -- every catalog, row, index and config file with it"

echo "4. a blank machine, and nothing up its sleeve"
initdb -D "$PGDATA" -U "$(id -un)" >"$WORK/initdb2.log" 2>&1 || die "initdb" "$(tail -5 "$WORK/initdb2.log")"
boot
contains "the blank directory has never heard of the table" "does not exist" \
         "$(sql "SELECT count(*) FROM ledger;")"
stop

echo "5. tell it where its storage is, and boot"
cp "$SAVED" "$PGDATA/objkv_catalogs"
boot
check "the rows came back"      "$WANT_COUNT" "$(sql "SELECT count(*) FROM ledger;")"
check "with the right values"   "$WANT_SUM"   "$(sql "SELECT sum(amount) FROM ledger;")"
check "the update survived"     "$WANT_ONE"   "$(sql "SELECT who FROM ledger WHERE id = 7;")"
check "so did the delete"       "$WANT_GONE"  "$(sql "SELECT count(*) FROM ledger WHERE id = 100;")"
check "the view came back"      "$WANT_VIEW"  "$(sql "SELECT count(*) FROM big_payments;")"
check "the index came back"     "objkv_btree" "$(am_of ledger_who)"
check "the post-flip table"     "$WANT_AFTER"      "$(sql "SELECT count(*) FROM ledger_after;")"
check "its rows"                "later-7"          "$(sql "SELECT who FROM ledger_after WHERE id = 7;")"
check "its view"                "$WANT_AFTER_VIEW" "$(sql "SELECT count(*) FROM after_view;")"
check "its index is objkv's"    "objkv_btree"      "$(am_of ledger_after_who)"
check "and what was dropped stayed dropped" "0" \
      "$(sql "SELECT count(*) FROM pg_class WHERE relname = 'dropped_after';")"
check "and so did the heap table and the serial the lift refused" "0" \
      "$(sql "SELECT count(*) FROM pg_class WHERE relname IN ('stayed_local','numbered','numbered_id_seq');")"
check "and it still answers"    "payee-42" "$(idx "SELECT who FROM ledger WHERE who = 'payee-42';")"
shows "and the plan reads the index, not the whole table" "ledger_who" \
      "$(plan "SELECT who FROM ledger WHERE who = 'payee-42';")"

echo "6. and the blank machine can still be written to, schema and all"
must "INSERT INTO ledger VALUES (900001, 'after-the-reboot', 1);" >/dev/null
check "a row written on the new machine" "after-the-reboot" \
      "$(sql "SELECT who FROM ledger WHERE id = 900001;")"
# Captured before the table exists: a fresh directory restarts the object-id
# counter, and the bucket is full of ids it would otherwise hand out again.
OID_CEILING=$(sql "SELECT max(oid) FROM pg_class;")
check "a table created on it"   "CREATE TABLE" \
      "$(sql "CREATE TABLE born_here (id int PRIMARY KEY, tag text) USING objkv;")"
sql "INSERT INTO born_here VALUES (1,'born on a machine that was blank');" >/dev/null
check "and read back from it"   "born on a machine that was blank" \
      "$(sql "SELECT tag FROM born_here WHERE id = 1;")"
check "with an object id above everything that was already there" "t" \
      "$(sql "SELECT 'born_here'::regclass::oid > $OID_CEILING;")"

finish "the finish line"
