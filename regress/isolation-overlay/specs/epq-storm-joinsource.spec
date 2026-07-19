# WS-U wave-5 (se/wave5-epq-inc1): TM_Updated storm under UPDATE ... FROM,
# with the join SOURCE re-scanned INSIDE the EPQ recheck (wave-5 contract
# §6.4a — the scanrelid==0 / joined-recheck surface).
#
# The victim blocks on the target row; after the storm commits, the
# recheck re-runs the whole join pipeline: the target substitutes its
# test tuple (relsubs_slot), the source rel is re-scanned for real inside
# the recheck. One session per forced join method (nestloop / hashjoin /
# mergejoin) plus an index-driven source arm (index-only-capable probe on
# the source pk). Expected output is the C-oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE jt (k int PRIMARY KEY, v int);
 CREATE TABLE js (k int PRIMARY KEY, delta int);
 INSERT INTO jt VALUES (1, 10), (2, 20);
 INSERT INTO js VALUES (1, 5), (2, 6);
}

teardown
{
 DROP TABLE jt;
 DROP TABLE js;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u1	{ UPDATE jt SET v = v + 100 WHERE k = 1; }
step s1c	{ COMMIT; }

session s2nl
setup
{
 SET enable_hashjoin = off;
 SET enable_mergejoin = off;
}
step nl	{ UPDATE jt SET v = jt.v + js.delta FROM js WHERE jt.k = js.k AND jt.k = 1 RETURNING jt.k, jt.v; }

session s2hj
setup
{
 SET enable_nestloop = off;
 SET enable_mergejoin = off;
}
step hj	{ UPDATE jt SET v = jt.v + js.delta FROM js WHERE jt.k = js.k AND jt.k = 1 RETURNING jt.k, jt.v; }

session s2mj
setup
{
 SET enable_nestloop = off;
 SET enable_hashjoin = off;
}
step mj	{ UPDATE jt SET v = jt.v + js.delta FROM js WHERE jt.k = js.k AND jt.k = 1 RETURNING jt.k, jt.v; }

session s2ix
setup
{
 SET enable_seqscan = off;
 SET enable_bitmapscan = off;
 SET enable_hashjoin = off;
 SET enable_mergejoin = off;
}
step ix	{ UPDATE jt SET v = jt.v + js.delta FROM js WHERE jt.k = js.k AND jt.k = 1 RETURNING jt.k, jt.v; }

session s3
step s3sel	{ SELECT k, v FROM jt ORDER BY k; }

permutation s1u1 nl s1c s3sel
permutation s1u1 hj s1c s3sel
permutation s1u1 mj s1c s3sel
permutation s1u1 ix s1c s3sel
