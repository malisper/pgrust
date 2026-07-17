# WS-U wave-5 (se/wave5-epq-inc1): forced TM_Updated storm against a
# SeqScan-shaped victim (wave-5 contract §6.4a).
#
# s1 builds a two-hop update chain on the same row inside one transaction
# (v1 -> v2 -> v3), so the victim's EvalPlanQual walk must follow TWO
# TM_Updated hops before the recheck runs. Arms: requalify-pass (qual on
# the key column survives the storm), requalify-fail UPDATE and DELETE
# (qual on the stormed value column fails the recheck -> row skipped).
# The victim session pins the seq-scan shape; expected output is the
# C-oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE storm_seq (a int, b int);
 INSERT INTO storm_seq VALUES (1, 10), (2, 20), (3, 30);
}

teardown
{
 DROP TABLE storm_seq;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u1	{ UPDATE storm_seq SET b = b + 100 WHERE a = 1; }
step s1u2	{ UPDATE storm_seq SET b = b + 1000 WHERE a = 1; }
step s1c	{ COMMIT; }

session s2
setup
{
 SET enable_indexscan = off;
 SET enable_bitmapscan = off;
}
step s2pass	{ UPDATE storm_seq SET b = b + 1 WHERE a = 1 RETURNING a, b; }
step s2skip	{ UPDATE storm_seq SET b = -1 WHERE b = 10 RETURNING a, b; }
step s2del	{ DELETE FROM storm_seq WHERE b = 10 RETURNING a, b; }

session s3
step s3sel	{ SELECT a, b FROM storm_seq ORDER BY a; }

permutation s1u1 s1u2 s2pass s1c s3sel
permutation s1u1 s1u2 s2skip s1c s3sel
permutation s1u1 s1u2 s2del s1c s3sel
