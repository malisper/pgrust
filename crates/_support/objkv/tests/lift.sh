#!/usr/bin/env bash
#
# The lift: a database's catalogs into the bucket. The guard refuses company,
# the lift stages one commit object, verification compares disk and bucket
# exactly, and the flip is one marker file.
#
. "$(dirname "$0")/server.sh"

fresh_cluster

echo "0. the entry points exist"
install_lift
check "three functions" "3" \
      "$(sql "SELECT count(*) FROM pg_proc WHERE proname LIKE 'pgrust_objkv_lift%';")"

# Both the lift and the flip must be the only session in the cluster, so both
# refusals are tested the same way: connect a second backend and try.
with_company() {  # with_company <statement>; the answer is in OUT
    psqlx -d postgres -c "SELECT pg_sleep(30);" >/dev/null 2>&1 &
    local sleeper=$! n i
    # The guard is about what is connected now, so wait until it actually is.
    for i in $(seq 1 20); do
        n=$(sql "SELECT count(*) FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep%';")
        [ "$n" -ge 1 ] 2>/dev/null && break
        sleep 1
    done
    OUT=$(sqlv "$1")
    kill "$sleeper" 2>/dev/null; wait "$sleeper" 2>/dev/null
    # Killing psql does not instantly retire its backend.
    for i in $(seq 1 30); do
        n=$(sql "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'client backend';")
        [ "$n" -le 1 ] 2>/dev/null && return 0
        sleep 1
    done
    fail "the cluster did not go quiet after the second session left"
}

echo "0b. and refuse anyone but a superuser"
must "CREATE ROLE objkv_nobody LOGIN;" >/dev/null
contains "lift"   "must be superuser" "$(psqlx -U objkv_nobody -d postgres -tAc "SELECT pgrust_objkv_lift();" 2>&1)"
contains "verify" "must be superuser" "$(psqlx -U objkv_nobody -d postgres -tAc "SELECT pgrust_objkv_lift_verify();" 2>&1)"
contains "finish" "must be superuser" "$(psqlx -U objkv_nobody -d postgres -tAc "SELECT pgrust_objkv_lift_finish();" 2>&1)"

echo "1. the lift refuses company"
# Shared catalogs belong to every database, so quiet here is not enough.
with_company "SELECT pgrust_objkv_lift();"
contains "refused while another backend is connected" "only session" "$OUT"
check "with sqlstate 55006 object_in_use" "55006" "$(sqlstate_of "$OUT")"

echo "2. the lift runs"
OUT=$(sql "SELECT pgrust_objkv_lift();")
contains "lifted" "relations" "$OUT"
echo "  $OUT" | tr '\n' ' '; echo

echo "3. it verifies against the local catalogs, row for row"
OUT=$(sql "SELECT pgrust_objkv_lift_verify();")
contains "$OUT" "identical" "$OUT"

echo "4. lifting twice is refused"
OUT=$(sqlv "SELECT pgrust_objkv_lift();")
contains "a second lift is refused" "already lifted" "$OUT"
check "with sqlstate 55000 object_not_in_prerequisite_state" "55000" "$(sqlstate_of "$OUT")"

echo "5. the flip needs every database"
OUT=$(sqlv "SELECT pgrust_objkv_lift_finish();")
contains "refused, naming the unlifted databases" "not lifted yet" "$OUT"
check "with sqlstate 55000 object_not_in_prerequisite_state" "55000" "$(sqlstate_of "$OUT")"

echo "6. lift the rest"
for db in $(sql "SELECT datname FROM pg_database WHERE datallowconn AND datname <> 'postgres' ORDER BY oid;"); do
    contains "lifted $db" "relations" "$(sql_in "$db" "SELECT pgrust_objkv_lift();")"
done

echo "7. the flip refuses company too"
with_company "SELECT pgrust_objkv_lift_finish();"
contains "refused while another backend is connected" "only session" "$OUT"
check "with sqlstate 55006 object_in_use" "55006" "$(sqlstate_of "$OUT")"

echo "8. and then it flips"
contains "flipped" "catalogs are in the bucket" "$(sql "SELECT pgrust_objkv_lift_finish();")"
[ -f "$PGDATA/objkv_catalogs" ] && ok "marker written" || fail "no marker file"

finish "the lift"
