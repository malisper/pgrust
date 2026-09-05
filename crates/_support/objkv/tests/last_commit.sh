#!/usr/bin/env bash
#
# What a client saw committed survives a kill -9, with the client still
# connected. A backend's exit publishes a watermark of its own, so the
# session is held open: the commit object landing is the only guarantee.
#
. "$(dirname "$0")/server.sh"

discards() { grep -c 'discarded .* unconfirmed commit object' "$LOG" 2>/dev/null || true; }

fresh_cluster
must "CREATE TABLE lc (id int PRIMARY KEY, note text) USING objkv;" >/dev/null
must "INSERT INTO lc VALUES (1, 'before');" >/dev/null

echo "1. kill -9 with the client still connected"
psqlx -d postgres -tA >"$WORK/held.out" 2>&1 <<'SQL' &
INSERT INTO lc VALUES (2, 'acknowledged, then killed');
SELECT 'RESULT=' || count(*) FROM lc WHERE id = 2;
SELECT pg_sleep(20);
SQL
HELD=$!
for i in $(seq 1 80); do grep -q '^RESULT=1' "$WORK/held.out" 2>/dev/null && break; sleep 0.25; done
check "the client was told" "RESULT=1" "$(grep '^RESULT=' "$WORK/held.out")"
stop KILL
kill "$HELD" 2>/dev/null; wait "$HELD" 2>/dev/null
boot
check "the row survived the kill"    "1" "$(sql "SELECT count(*) FROM lc WHERE id = 2;")"
check "along with everything before" "2" "$(sql "SELECT count(*) FROM lc;")"
check "nothing discarded"            "0" "$(discards)"

echo "2. and so does a burst from many sessions, killed mid-stream"
JOBS=""
for w in $(seq 1 6); do
    ( seq 1 20 | sed "s/.*/INSERT INTO lc VALUES ($w * 1000 + &, 'burst');/" \
        | psqlx -d postgres >"$WORK/burst.$w.out" 2>&1 ) &
    JOBS="$JOBS $!"
done
sleep 1
stop KILL
for j in $JOBS; do wait "$j" 2>/dev/null; done
# Each session's log shows how many INSERTs were acknowledged before the kill.
ACKED=0
for w in $(seq 1 6); do ACKED=$((ACKED + $(grep -c '^INSERT 0 1' "$WORK/burst.$w.out" 2>/dev/null || true))); done
boot
GOT=$(sql "SELECT count(*) FROM lc WHERE note = 'burst';")
echo "  $ACKED inserts were acknowledged; $GOT are in the table"
check "every acknowledged insert is there" "t" "$([ "$GOT" -ge "$ACKED" ] && echo t || echo f)"
check "nothing discarded" "0" "$(discards)"

finish "the last commit survives a kill -9"
