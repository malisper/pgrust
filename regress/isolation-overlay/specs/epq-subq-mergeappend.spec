# EPQ-unique admission wave (fix/epq-unique-recheck): MergeAppend INSIDE
# the EPQ recheck plan — a DISTINCT subquery over a hash-partitioned
# table whose sorted output must merge the per-partition index scans
# (hash partitioning interleaves key order across partitions, so ordered
# output NEEDS MergeAppend, not Append; sort/seqscan/hashagg/hashjoin
# off). Also exercises the check_epq_plan walker's mergeplans recursion.
# Same two-session storm as epq-storm-unique. The EXPLAIN step pins the
# plan shape into the expected file. Expected output is the C oracle's
# (PostgreSQL 18.3).

setup
{
 CREATE TABLE docs (id int PRIMARY KEY, status text);
 CREATE TABLE ap_part (doc_id int, approved bool) PARTITION BY HASH (doc_id);
 CREATE TABLE ap_p0 PARTITION OF ap_part FOR VALUES WITH (MODULUS 2, REMAINDER 0);
 CREATE TABLE ap_p1 PARTITION OF ap_part FOR VALUES WITH (MODULUS 2, REMAINDER 1);
 CREATE INDEX ap_p0_doc_id_idx ON ap_p0 (doc_id);
 CREATE INDEX ap_p1_doc_id_idx ON ap_p1 (doc_id);
 INSERT INTO docs SELECT g, 'pending' FROM generate_series(1, 5) g;
 INSERT INTO ap_part SELECT g, true FROM generate_series(1, 5) g;
 INSERT INTO ap_part SELECT g, true FROM generate_series(1, 5) g;
 ANALYZE docs;
 ANALYZE ap_part;
}

teardown
{
 DROP TABLE docs;
 DROP TABLE ap_part;
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1u	{ UPDATE docs SET status = 'revoked' WHERE id = 3; }
step s1k	{ UPDATE docs SET id = 99 WHERE id = 3; }
step s1c	{ COMMIT; }

session s2
setup
{
 SET enable_hashagg = off;
 SET enable_hashjoin = off;
 SET enable_seqscan = off;
 SET enable_sort = off;
}
step expl	{ EXPLAIN (COSTS OFF) SELECT d.* FROM docs d JOIN (SELECT DISTINCT doc_id FROM ap_part WHERE approved) a ON a.doc_id = d.id FOR UPDATE OF d; }
step lock	{ SELECT d.* FROM docs d JOIN (SELECT DISTINCT doc_id FROM ap_part WHERE approved) a ON a.doc_id = d.id FOR UPDATE OF d; }

session s3
step sel	{ SELECT id, status FROM docs ORDER BY id; }

permutation expl s1u lock s1c sel
permutation expl s1k lock s1c sel
