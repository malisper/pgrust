# WS-Y wave-7 (se/wave7-epq-inc2): census-delta exercising spec — a
# ValuesScan join source INSIDE the EPQ recheck plan. ValuesScan is a Y3
# gate-delta shape: check_epq_plan ADMITS it but no lane-ownership surface
# exists at this head (verdict Short, lanev2/epq.rs), so the inc-5
# es_epq_active lift stays gated until an owner lands one — this spec is
# the shape's exercising leg in notes/se-wave7-epq.md's census delta.
# The EXPLAIN step pins the plan shape into the expected file so planner
# drift is caught loudly. Expected output is the C oracle's (PostgreSQL
# 18.3). Refusal-invariant: byte-identical across lane-knob arms while
# es_epq_active refuses all lane ownership inside rechecks.

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
step expl	{ EXPLAIN (COSTS OFF) UPDATE st SET v = st.v + s.d FROM (VALUES (1, 5), (2, 6)) AS s(k, d) WHERE st.k = s.k AND st.k = 1; }
step upd	{ UPDATE st SET v = st.v + s.d FROM (VALUES (1, 5), (2, 6)) AS s(k, d) WHERE st.k = s.k AND st.k = 1 RETURNING st.k, st.v; }

session s3
step sel	{ SELECT k, v FROM st ORDER BY k; }

# Pass arm: the storm bumps v; the recheck re-joins the ValuesScan source
# and the moved row still satisfies the quals.
permutation expl s1u upd s1c sel
# Skip arm: the storm moves the key; the recheck join fails -> row skipped.
permutation expl s1k upd s1c sel
