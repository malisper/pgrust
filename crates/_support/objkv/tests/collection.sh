#!/usr/bin/env bash
#
# Collection: the bucket stops growing for ever. Every write leaves the old
# version behind; collection drops the ones no reader can still ask for.
# Bytes are the measure, not object count, since tidy-up folds small objects
# into one large one either way.
#
. "$(dirname "$0")/server.sh"
# Comfortably past the 100-commit tidy-up threshold, so it runs more than once.
WRITES="${WRITES:-250}"
KEEP="${KEEP:-20}"
ROUNDS="${ROUNDS:-150}"
SETTLE="${SETTLE:-110}"

bytes() { "$HERE/bucket.py" bytes; }
churn() { # churn <retain> <count>
    local i
    for i in $(seq 1 "$2"); do
        sql "SET pgrust.objkv_retain_commits = $1; UPDATE churn SET n = $i WHERE id = 1;" >/dev/null
    done
}
refused_below_retention() {  # refused_below_retention <what>
    local out
    out=$(psqlx -d postgres -tA -c "SET pgrust.objkv_snapshot_seq = 2;" \
                -c "SELECT n FROM churn WHERE id = 1;" 2>&1 | tail -1)
    contains "$1" "has been collected" "$out"
}

fresh_cluster

echo "1. retention off: the same row, written over and over"
sql "CREATE TABLE churn (id int, n int) USING objkv;" >/dev/null
sql "INSERT INTO churn VALUES (1, 0);" >/dev/null
churn 0 "$WRITES"
GREW=$(bytes)
echo "  $WRITES updates, $GREW bytes in the bucket"

echo "2. retention on: the same workload again"
churn "$KEEP" "$WRITES"
KEPT=$(bytes)
echo "  $WRITES more updates, $KEPT bytes"
if [ "$KEPT" -lt "$GREW" ]; then
    ok "the bucket shrank while the workload continued ($GREW -> $KEPT bytes)"
else
    fail "the bucket did not shrink ($GREW -> $KEPT bytes)"
fi

echo "3. the data is still right"
check "the row reads back" "$WRITES" "$(sql "SELECT n FROM churn WHERE id = 1;")"
check "there is still one row" "1" "$(sql "SELECT count(*) FROM churn;")"

echo "4. reading further back than retention is an error, not a wrong answer"
refused_below_retention "refused, and said how far back it can go"

echo "5. and it survives a restart"
stop; boot
check "the row is still right" "$WRITES" "$(sql "SELECT n FROM churn WHERE id = 1;")"
refused_below_retention "and it still knows what it collected"

echo "6. index entries go with their rows, and the index still works"
# An update writes the row at a fresh id, stranding the entry for the old one.
sql "CREATE TABLE idxchurn (id int PRIMARY KEY, note text COLLATE \"C\") USING objkv;" >/dev/null
sql "INSERT INTO idxchurn SELECT g, 'v0' FROM generate_series(1, 50) g;" >/dev/null
INDEXED_START=$(bytes)
for i in $(seq 1 "$ROUNDS"); do
    sql "SET pgrust.objkv_retain_commits = $KEEP; UPDATE idxchurn SET note = 'v$i';" >/dev/null
done
CHURNED=$(bytes)
# Tidy-up runs every hundred commits, on its own thread, so settle first:
# otherwise this measures where the workload happened to stop, not what
# collection kept. Then wait for the bucket to stop changing.
churn "$KEEP" "$SETTLE"
settled() {  # the bucket's byte count, once it has held still for 8s
    # A merge changes nothing in the bucket until its PUT lands, and a debug
    # build takes seconds over a megabyte, so a short quiet spell proves
    # little. Four samples two seconds apart, up to two minutes.
    local prev=-1 cur same=0 i
    for i in $(seq 1 60); do
        cur=$(bytes)
        if [ "$cur" = "$prev" ]; then same=$((same + 1)); else same=0; fi
        [ "$same" -ge 3 ] && { echo "$cur"; return; }
        prev=$cur; sleep 2
    done
    echo "$cur"
}
INDEXED_END=$(settled)
echo "  50 rows x $ROUNDS updates: $INDEXED_START -> $CHURNED bytes, $INDEXED_END once settled"

# 50 x ROUNDS row versions and entries were written; what should remain is 50
# live rows, their entries, and the retained window.
BUDGET=$(( INDEXED_START * 4 ))
if [ "$INDEXED_END" -lt "$BUDGET" ]; then
    ok "the bucket stayed bounded under churn (under ${BUDGET} bytes)"
else
    fail "the bucket grew with the churn ($INDEXED_END bytes, budget $BUDGET)"
fi

check "a lookup by key still finds its row" "v$ROUNDS" "$(sql "SELECT note FROM idxchurn WHERE id = 42;")"
check "and finds exactly one" "1" "$(sql "SELECT count(*) FROM idxchurn WHERE id = 42;")"
check "every row is still there" "50" "$(sql "SELECT count(*) FROM idxchurn;")"
# Forced: 50 rows is small enough that a seqscan is the honest plan, and the
# point is that the index still answers after its entries were collected.
check "and the index path itself still answers" "v$ROUNDS" "$(idx "SELECT note FROM idxchurn WHERE id = 42;")"

finish "collection frees space and refuses what it freed"
