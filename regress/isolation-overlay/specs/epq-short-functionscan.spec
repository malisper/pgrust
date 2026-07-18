# WS-Y wave-7 (se/wave7-epq-inc2): census-delta exercising spec — a
# FunctionScan join source INSIDE the EPQ recheck plan. FunctionScan is a
# Y3 gate-delta shape: check_epq_plan ADMITS it but no lane-ownership
# surface exists at this head (verdict Short, lanev2/epq.rs) — this spec
# is the shape's exercising leg in notes/se-wave7-epq.md's census delta.
# The EXPLAIN step pins the plan shape into the expected file. Expected
# output is the C oracle's (PostgreSQL 18.3). Refusal-invariant across
# lane-knob arms.

setup
{
 CREATE TABLE st (k int PRIMARY KEY, v int);
 INSERT INTO st VALUES (1, 10), (2, 20);
}

teardown
{
 DROP TABLE st;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u	{ UPDATE st SET v = v + 100 WHERE k = 1; }
step s1k	{ UPDATE st SET k = 3 WHERE k = 1; }
step s1c	{ COMMIT; }

session s2
step expl	{ EXPLAIN (COSTS OFF) UPDATE st SET v = st.v + g.k FROM generate_series(1, 2) AS g(k) WHERE st.k = g.k AND st.k = 1; }
step upd	{ UPDATE st SET v = st.v + g.k FROM generate_series(1, 2) AS g(k) WHERE st.k = g.k AND st.k = 1 RETURNING st.k, st.v; }

session s3
step sel	{ SELECT k, v FROM st ORDER BY k; }

# Pass arm: the recheck re-runs the FunctionScan source; quals still hold.
permutation expl s1u upd s1c sel
# Skip arm: key move -> recheck join fails -> row skipped.
permutation expl s1k upd s1c sel
