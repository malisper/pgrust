#!/usr/bin/env bash
#
# Concurrent inserters must not take the same row number: once, the later
# write replaced the earlier row and both clients were told they succeeded.
# Row counts are the test.
#
. "$(dirname "$0")/server.sh"
WRITERS="${WRITERS:-8}"
PER="${PER:-25}"
ROUNDS="${ROUNDS:-3}"

fresh_cluster
sql "CREATE TABLE race (round int, who int, n int) USING objkv;" >/dev/null

echo "1. $WRITERS sessions inserting at once, $ROUNDS times over"
WANT=$((WRITERS * PER))
for r in $(seq 1 "$ROUNDS"); do
    JOBS=""
    for w in $(seq 1 "$WRITERS"); do
        ( seq 1 "$PER" | sed "s/.*/INSERT INTO race VALUES ($r,$w,&);/" \
            | psqlx -d postgres -q >/dev/null 2>&1 ) &
        # Named jobs: a bare `wait` waits on the server too.
        JOBS="$JOBS $!"
    done
    for j in $JOBS; do wait "$j"; done
    check "round $r kept every row" "$WANT" "$(sql "SELECT count(*) FROM race WHERE round=$r;")"
done
check "no two rows share a key" "$((WANT * ROUNDS))" \
      "$(sql "SELECT count(*) FROM (SELECT DISTINCT round,who,n FROM race) s;")"

finish "concurrent inserts"
