#!/usr/bin/env bash
#
# Group commit and asynchronous commit.
#
# Every commit is one PUT, and a PUT is ~25ms against real S3. Sessions
# committing at once used to queue for the lock and pay that each; now one
# writer thread lands everything queued behind the PUT in flight as one
# object. That is checked by counting objects: fewer than transactions.
#
# `pgrust.objkv_async_commit` reports COMMIT before the object is written.
# What must still hold: a clean shutdown loses nothing, a conflict is still
# refused, and a snapshot never sees half of a transaction.
#
. "$(dirname "$0")/server.sh"
WRITERS="${WRITERS:-8}"
PER="${PER:-10}"
objects() { "$HERE/bucket.py" count commit/; }

# Its own cluster, like every other script: an inherited data directory can
# name objects the emptied bucket no longer has.
fresh_cluster
sql "CREATE TABLE gc (who int, n int, note text) USING objkv;" >/dev/null
BEFORE=$(objects)

echo "1. $WRITERS sessions, $PER single-statement transactions each, at once"
WANT=$((WRITERS * PER))
JOBS=""
for w in $(seq 1 "$WRITERS"); do
    ( seq 1 "$PER" | sed "s/.*/INSERT INTO gc VALUES ($w,&,'sync');/" \
        | psql -h "$SOCKDIR" -p "$PORT" -d postgres -q >/dev/null 2>&1 ) &
    JOBS="$JOBS $!"
done
for j in $JOBS; do wait "$j"; done
AFTER=$(objects)
LANDED=$((AFTER - BEFORE))
check "every row arrived" "$WANT" "$(sql "SELECT count(*) FROM gc;")"
if [ "$LANDED" -lt "$WANT" ] && [ "$LANDED" -gt 0 ]; then
    echo "  ok: $WANT transactions landed as $LANDED objects"
else
    echo "  FAIL: $WANT transactions landed as $LANDED objects; nothing was grouped"; RC=1
fi

echo "2. a batch object is read back whole after a restart"
stop
boot
check "the rows survive" "$WANT" "$(sql "SELECT count(*) FROM gc;")"
check "and are distinct" "$WANT" "$(sql "SELECT count(*) FROM (SELECT DISTINCT who, n FROM gc) s;")"

echo "3. asynchronous commit"
sql "DROP TABLE IF EXISTS ac;" >/dev/null
sql "CREATE TABLE ac (id int PRIMARY KEY, v int) USING objkv;" >/dev/null
sql "INSERT INTO ac VALUES (1, 0), (2, 0);" >/dev/null

# Timing, informational: one session, sequential single-row transactions.
N=30
t0=$(date +%s%N)
seq 1 $N | sed "s/.*/INSERT INTO gc VALUES (0,&,'sync');/" | psql -h "$SOCKDIR" -p "$PORT" -d postgres -q >/dev/null 2>&1
t1=$(date +%s%N)
{ echo "SET pgrust.objkv_async_commit = on;"; seq 1 $N | sed "s/.*/INSERT INTO gc VALUES (-1,&,'async');/"; } \
    | psql -h "$SOCKDIR" -p "$PORT" -d postgres -q >/dev/null 2>&1
t2=$(date +%s%N)
SYNC_MS=$(( (t1 - t0) / 1000000 )); ASYNC_MS=$(( (t2 - t1) / 1000000 ))
echo "  $N sequential commits: sync ${SYNC_MS}ms, async ${ASYNC_MS}ms"
check "async rows are all there" "$N" "$(sql "SELECT count(*) FROM gc WHERE who = -1;")"

# A conflict is decided at pre-commit, before anyone is told anything, so
# async changes nothing here: B commits first, A's commit is refused.
#
# Known divergence from Postgres. Under READ COMMITTED Postgres would make B's
# UPDATE wait for A's transaction, let A commit, then re-evaluate B's row and
# apply it: both updates land, v ends at 2, and nobody sees 40001. objkv
# takes no row locks and validates at commit: B does not wait, and A -- whose
# read of the row is now stale -- is refused at COMMIT with 40001. v still
# ends at 2. The isolation contract is in objkv_am.rs and docs/objkv.md; this
# step pins it so a change is noticed, not to bless it.
OUT=$(psql -h "$SOCKDIR" -p "$PORT" -d postgres -tA <<SQL 2>&1
SET pgrust.objkv_async_commit = on;
BEGIN;
UPDATE ac SET v = 1 WHERE id = 1;
\\! psql -h "$SOCKDIR" -p "$PORT" -d postgres -tAc "SET pgrust.objkv_async_commit = on; UPDATE ac SET v = 2 WHERE id = 1;" >/dev/null 2>&1
COMMIT;
SQL
)
echo "$OUT" | grep -q "serialize access" \
    && echo "  ok: known divergence from Postgres: the loser gets 40001 at COMMIT (also under async commit), where Postgres READ COMMITTED would block B and let both updates land" \
    || { echo "  FAIL: no serialization failure under async commit (known divergence from Postgres; see the comment above): $OUT"; RC=1; }
check "and the winner's value stands" "2" "$(sql "SELECT v FROM ac WHERE id = 1;")"

# An abort after the write was queued must leave nothing behind.
sql "SET pgrust.objkv_async_commit = on; BEGIN; UPDATE ac SET v = 99 WHERE id = 2; ROLLBACK;" >/dev/null
check "a rolled-back async transaction leaves no trace" "0" "$(sql "SELECT v FROM ac WHERE id = 2;")"

echo "4. a clean shutdown loses no asynchronous commit"
# Queue a burst and stop straight away: the exit path drains the queue
# before publishing the watermark.
{ echo "SET pgrust.objkv_async_commit = on;"; seq 1 40 | sed "s/.*/INSERT INTO gc VALUES (-2,&,'tail');/"; } \
    | psql -h "$SOCKDIR" -p "$PORT" -d postgres -q >/dev/null 2>&1
stop
boot
check "every async commit before shutdown is back" "40" "$(sql "SELECT count(*) FROM gc WHERE who = -2;")"
check "and the earlier rows too" "$((WANT + 2 * N))" "$(sql "SELECT count(*) FROM gc WHERE who <> -2;")"

finish "group commit and async commit"
