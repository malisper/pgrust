# WS-U wave-5 (se/wave5-epq-inc1): forced TM_Updated storm against
# TidScan- and TidRangeScan-shaped victims (wave-5 contract §6.4a).
#
# The adversarial edge: an update MOVES the row to a new ctid, and the
# two tid shapes answer the EPQ recheck differently by design —
# TidRecheck (nodeTidscan.c) bsearches the literal tid list, so the
# moved row FAILS the recheck (UPDATE 0); TidRangeRecheck
# (nodeTidrangescan.c) re-compares against the range, so a
# still-in-range moved row PASSES and the update applies. Expected
# output is the C-oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE storm_tid (a int, b int);
 INSERT INTO storm_tid VALUES (1, 10);
}

teardown
{
 DROP TABLE storm_tid;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u1	{ UPDATE storm_tid SET b = b + 100 WHERE a = 1; }
step s1c	{ COMMIT; }

session s2
step s2tid	{ UPDATE storm_tid SET b = b + 1 WHERE ctid = '(0,1)' RETURNING a, b; }
step s2range	{ UPDATE storm_tid SET b = b + 1 WHERE ctid >= '(0,1)' AND ctid <= '(0,100)' RETURNING a, b; }

session s3
step s3sel	{ SELECT a, b FROM storm_tid ORDER BY a; }

permutation s1u1 s2tid s1c s3sel
permutation s1u1 s2range s1c s3sel
