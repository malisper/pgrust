#!/usr/bin/env bash
#
# The commit object is the commit. Once it lands, a crash keeps it, whether
# or not the client heard back -- the same rule as a WAL commit record. A
# transaction that aborts after its object landed is the one case the
# object must not be applied, and a discard marker records that.
#
. "$(dirname "$0")/server.sh"

discards() { grep -c 'discarded .* unconfirmed commit object' "$LOG" 2>/dev/null || true; }
markers()  { "$HERE/bucket.py" count resolve/; }

fresh_cluster
sql "CREATE TABLE torn (id int, note text) USING objkv;" >/dev/null
sql "INSERT INTO torn VALUES (1,'legitimately committed');" >/dev/null
sql "INSERT INTO torn VALUES (2,'also committed');" >/dev/null
stop KILL

echo "1. the server dies after the object lands, before the client hears"
OBJKV_FAULT_AFTER_COMMIT_PUT=1 boot
psqlx -d postgres -c "INSERT INTO torn VALUES (99,'never acknowledged');" >/dev/null 2>&1
stop KILL
boot
sql "SELECT id || ' | ' || note FROM torn ORDER BY id;" | sed 's/^/  /'
check "settled row 1 survived"      "1" "$(sql "SELECT count(*) FROM torn WHERE id = 1;")"
check "acknowledged row 2 survived" "1" "$(sql "SELECT count(*) FROM torn WHERE id = 2;")"
check "row 99 is committed: its object landed" "1" "$(sql "SELECT count(*) FROM torn WHERE id = 99;")"
check "nothing was discarded" "0" "$(discards)"
stop

echo "2. the transaction aborts after its object lands"
OBJKV_FAULT_ERROR_AFTER_COMMIT_PUT=1 boot
OUT=$(psqlx -d postgres -c "INSERT INTO torn VALUES (98,'aborted after landing');" 2>&1)
contains "the client saw the error" "FAULT_ERROR_AFTER_COMMIT_PUT" "$OUT"
check "and the row is not there" "0" "$(sql "SELECT count(*) FROM torn WHERE id = 98;")"
check "a discard marker was written" "1" "$(markers)"
stop KILL

echo "3. the marker outranks the object on every later boot"
boot
check "row 98 stays aborted"   "0" "$(sql "SELECT count(*) FROM torn WHERE id = 98;")"
check "the others are all there" "3" "$(sql "SELECT count(*) FROM torn;")"
check "no re-deciding on boot" "0" "$(discards)"

finish "a landed object is a commit, an abort after landing is marked"
