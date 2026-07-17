# WS-U wave-5 (se/wave5-epq-inc1): forced TM_Updated storm against a
# BitmapHeapScan-shaped victim (wave-5 contract §6.4a).
#
# Same two-hop chain as epq-storm-seqscan, with the victim session
# pinning the bitmap shape (seq + plain index scans disabled; the index
# on a makes the bitmap path the only viable one). Arms: requalify-pass
# on the key qual, requalify-fail on the stormed value column. Expected
# output is the C-oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE storm_bm (a int, b int);
 CREATE INDEX storm_bm_a ON storm_bm (a);
 INSERT INTO storm_bm VALUES (1, 10), (2, 20), (3, 30);
}

teardown
{
 DROP TABLE storm_bm;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u1	{ UPDATE storm_bm SET b = b + 100 WHERE a = 1; }
step s1u2	{ UPDATE storm_bm SET b = b + 1000 WHERE a = 1; }
step s1c	{ COMMIT; }

session s2
setup
{
 SET enable_seqscan = off;
 SET enable_indexscan = off;
}
step s2pass	{ UPDATE storm_bm SET b = b + 1 WHERE a = 1 RETURNING a, b; }
step s2skip	{ UPDATE storm_bm SET b = -1 WHERE a = 1 AND b = 10 RETURNING a, b; }

session s3
step s3sel	{ SELECT a, b FROM storm_bm ORDER BY a; }

permutation s1u1 s1u2 s2pass s1c s3sel
permutation s1u1 s1u2 s2skip s1c s3sel
