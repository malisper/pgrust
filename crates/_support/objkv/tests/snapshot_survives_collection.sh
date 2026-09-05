#!/usr/bin/env bash
#
# An open snapshot survives collection. Collection drops the versions no
# reader can still ask for, and "can still ask for" includes a transaction
# that took its snapshot before the churn and has not finished with it. The
# horizon is held at the oldest snapshot in use (objkv_am.rs,
# collection_horizon), so session A, in REPEATABLE READ, must read the same
# rows after 150+ commits and a collection as it read before them -- not
# "has been collected", and not rows silently missing. The control is that
# collection really did run while A was open: a fresh session asking for a
# point below A's snapshot is refused.
#
. "$(dirname "$0")/server.sh"
HISTORY="${HISTORY:-30}"   # commits before A arrives, so its snapshot is well above seq 2
CHURN="${CHURN:-160}"      # past the 100-commit tidy-up threshold
KEEP="${KEEP:-5}"

bytes() { "$HERE/bucket.py" bytes; }
settled() {  # the bucket's byte count, once it has held still for 8s (as collection.sh)
    local prev=-1 cur same=0 i
    for i in $(seq 1 60); do
        cur=$(bytes)
        if [ "$cur" = "$prev" ]; then same=$((same + 1)); else same=0; fi
        [ "$same" -ge 3 ] && { echo "$cur"; return; }
        prev=$cur; sleep 2
    done
    echo "$cur"
}

fresh_cluster

echo "0. a table with some history behind it"
must "CREATE TABLE pinned (id int PRIMARY KEY, n int) USING objkv;" >/dev/null
must "INSERT INTO pinned SELECT g, g * 10 FROM generate_series(1,100) g;" >/dev/null
seq 1 "$HISTORY" | sed 's/.*/UPDATE pinned SET n = n + 0 WHERE id = &;/' \
    | psqlx -d postgres -q >"$WORK/history.out" 2>&1
WANT=$(must "SELECT count(*) || ':' || sum(n) FROM pinned;")
WANT1=$(must "SELECT n FROM pinned WHERE id = 1;")
echo "  100 rows, count:sum = $WANT, $HISTORY commits of history"

echo "1. session A opens REPEATABLE READ and reads"
# A is one psql fed through a pipe and kept open across the churn. Its output
# is a file psql flushes at exit, so progress is signalled with \! touch.
mkfifo "$WORK/a.in"
psqlx -d postgres -tA <"$WORK/a.in" >"$WORK/a.out" 2>&1 &
A_PID=$!
exec 3>"$WORK/a.in"
a() { printf '%s\n' "$1" >&3; }
a_reached() {  # a_reached <mark>: true once A has run everything sent before it
    local i
    a "\\! touch '$WORK/a.$1'"
    for i in $(seq 1 240); do [ -e "$WORK/a.$1" ] && return 0; sleep 0.25; done
    fail "session A did not get to '$1' within 60s: $(tail -3 "$WORK/a.out" 2>/dev/null | tr '\n' ' ')"
    return 1
}
a "BEGIN ISOLATION LEVEL REPEATABLE READ;"
a "SET enable_seqscan = off;"
a "SELECT 'FIRST=' || count(*) || ':' || sum(n) FROM pinned;"
a "SELECT 'FIRST_ID1=' || n FROM pinned WHERE id = 1;"
a "SELECT 'FIRST_ID95=' || count(*) FROM pinned WHERE id = 95;"
a_reached read1 && ok "A has taken its snapshot and read the table"

echo "2. $CHURN commits go by on another session, retention $KEEP, and collection runs"
# Every row A read is rewritten and some are deleted, so a collector that
# forgot A would take the very versions A is still entitled to.
{
    echo "SET pgrust.objkv_retain_commits = $KEEP;"
    seq 1 "$CHURN" | sed 's/.*/UPDATE pinned SET n = n + 1 WHERE id = (& % 100) + 1;/'
    echo "DELETE FROM pinned WHERE id > 90;"
} | psqlx -d postgres -q >"$WORK/churn.out" 2>&1
check "the churn ran clean" "0" "$(grep -c '^ERROR' "$WORK/churn.out")"
END=$(settled)
echo "  bucket settled at $END bytes"
# The control: history below A's snapshot is gone. Without this the survival
# below would be proof of nothing.
OUT=$(psqlx -d postgres -tA -c "SET pgrust.objkv_snapshot_seq = 2;" -c "SELECT n FROM pinned WHERE id = 1;" 2>&1 | tail -1)
contains "collection ran while A was open: a point below A's snapshot is refused" "has been collected" "$OUT"
check "and the present is the churned table" "90" "$(sql "SELECT count(*) FROM pinned;")"

echo "3. A reads again, from the same snapshot"
a "SELECT 'SECOND=' || count(*) || ':' || sum(n) FROM pinned;"
a "SELECT 'SECOND_ID1=' || n FROM pinned WHERE id = 1;"
a "SELECT 'SECOND_ID95=' || count(*) FROM pinned WHERE id = 95;"
a "COMMIT;"
a_reached read2 && ok "A read again and committed"
a "\\q"
exec 3>&-
wait "$A_PID" 2>/dev/null
got() { grep "^$1=" "$WORK/a.out" | head -1 | cut -d= -f2-; }
check "A's first read was the table as planted"  "$WANT"  "$(got FIRST)"
check "and its second read is the same rows"     "$WANT"  "$(got SECOND)"
check "a point lookup through the index, before" "$WANT1" "$(got FIRST_ID1)"
check "and after: the value A's snapshot holds, not the churned one" "$WANT1" "$(got SECOND_ID1)"
check "a row since deleted is still there for A"  "1"      "$(got SECOND_ID95)"
check "and A saw no error at all"                "0"      "$(grep -c '^ERROR' "$WORK/a.out")"

echo "4. with A gone, the present is what everyone else sees"
check "count"  "90" "$(sql "SELECT count(*) FROM pinned;")"
check "and the churned value" "t" "$(sql "SELECT n <> $WANT1 FROM pinned WHERE id = 1;")"

finish "an open snapshot survives collection"
