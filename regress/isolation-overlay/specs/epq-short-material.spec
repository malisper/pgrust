# WS-Y wave-7 (se/wave7-epq-inc2): census-delta exercising spec — a
# Material node over the join source INSIDE the EPQ recheck plan (forced
# nestloop over seqscans: hashjoin/mergejoin/memoize/indexscan/bitmapscan
# off; the inequality join keeps the source on the materialized inner
# side). Material is a Y3 gate-delta
# shape: check_epq_plan ADMITS it but no lane-ownership surface exists at
# this head (verdict Short, lanev2/epq.rs) — this spec is the shape's
# exercising leg in notes/se-wave7-epq.md's census delta. The EXPLAIN
# step pins the plan shape into the expected file. Expected output is the
# C oracle's (PostgreSQL 18.3). Refusal-invariant across lane-knob arms.

setup
{
 CREATE TABLE st (k int PRIMARY KEY, v int);
 INSERT INTO st VALUES (1, 10), (2, 20);
 CREATE TABLE ms (k int, d int);
 INSERT INTO ms VALUES (1, 5), (2, 6);
}

teardown
{
 DROP TABLE st;
 DROP TABLE ms;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u	{ UPDATE st SET v = v + 100 WHERE k = 1; }
step s1k	{ UPDATE st SET k = 3 WHERE k = 1; }
step s1c	{ COMMIT; }

session s2
setup
{
 SET enable_hashjoin = off;
 SET enable_mergejoin = off;
 SET enable_memoize = off;
 SET enable_indexscan = off;
 SET enable_bitmapscan = off;
}
step expl	{ EXPLAIN (COSTS OFF) UPDATE st SET v = st.v + ms.d FROM ms WHERE st.k < ms.k; }
step upd	{ UPDATE st SET v = st.v + ms.d FROM ms WHERE st.k < ms.k RETURNING st.k, st.v; }

session s3
step sel	{ SELECT k, v FROM st ORDER BY k; }

# Pass arm: the recheck re-scans the Materialized source; k=1 still joins
# ms.k=2.
permutation expl s1u upd s1c sel
# Skip arm: key move to 3 -> no ms.k above it -> recheck join fails ->
# row skipped.
permutation expl s1k upd s1c sel
