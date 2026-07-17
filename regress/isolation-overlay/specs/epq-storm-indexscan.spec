# WS-U wave-5 (se/wave5-epq-inc1): forced TM_Updated storm against an
# IndexScan-shaped victim (wave-5 contract §6.4a).
#
# Arms: single-hop requalify-pass, and the key-move storm — s1 moves the
# index key out from under the victim's qual (a: 1 -> 3), so the EPQ
# recheck of the substituted test tuple fails on the index qual and the
# row is skipped (UPDATE 0). The victim session pins the index-scan
# shape; expected output is the C-oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE storm_idx (a int PRIMARY KEY, b int);
 INSERT INTO storm_idx VALUES (1, 10), (2, 20);
}

teardown
{
 DROP TABLE storm_idx;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u1	{ UPDATE storm_idx SET b = b + 100 WHERE a = 1; }
step s1move	{ UPDATE storm_idx SET a = 3 WHERE a = 1; }
step s1c	{ COMMIT; }

session s2
setup
{
 SET enable_seqscan = off;
 SET enable_bitmapscan = off;
}
step s2pass	{ UPDATE storm_idx SET b = b + 1 WHERE a = 1 RETURNING a, b; }

session s3
step s3sel	{ SELECT a, b FROM storm_idx ORDER BY a, b; }

permutation s1u1 s2pass s1c s3sel
permutation s1u1 s1move s2pass s1c s3sel
