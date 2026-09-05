#!/usr/bin/env bash
#
# A write between the lift and the flip. The guard that refuses company is a
# glance at who is connected now, and the lifts of a cluster's databases are
# separate sessions with nothing watching in between. A catalog row committed
# in that span lands in a local file the bucket has already photographed, and
# an unguarded flip would lose it. Each lift records the transaction counter,
# and the next lift and the flip refuse when it has moved.
#
. "$(dirname "$0")/server.sh"

fresh_cluster
install_lift

echo "1. lift every database but one"
LAST=$(dbs | tail -1)
FIRST=$(dbs | head -1)
for db in $(dbs); do
    [ "$db" = "$LAST" ] && continue
    contains "lifted $db" "relations" "$(sql_in "$db" "SELECT pgrust_objkv_lift();")"
done

echo "2. a session that slipped in creates a table"
# A plain heap table in a database already lifted: its pg_class row is in a
# local file, and the bucket's copy of that database predates it.
check "created in $FIRST after its lift" "CREATE TABLE" "$(sql_in "$FIRST" "CREATE TABLE slipped_in (id int);")"

echo "3. the next lift refuses"
OUT=$(DB="$LAST" sqlv "SELECT pgrust_objkv_lift();")
contains "refused, naming the stale lift" "has written since" "$OUT"
check "with sqlstate 55000 object_not_in_prerequisite_state" "55000" "$(sqlstate_of "$OUT")"
echo "  $(echo "$OUT" | head -1)"

echo "4. and so does the flip"
OUT=$(sqlv "SELECT pgrust_objkv_lift_finish();")
contains "refused" "has written since" "$OUT"
check "with sqlstate 55000 object_not_in_prerequisite_state" "55000" "$(sqlstate_of "$OUT")"
[ -f "$PGDATA/objkv_catalogs" ] && fail "a marker was written" || ok "no marker written"

echo "5. the refusal is about the write, not the company"
# The slipped-in session is long gone; a count of who is connected now would
# have let the flip through.
check "only this session is connected, and the refusal stands" "1" \
      "$(sql "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend';")"

finish "a stale lift"
