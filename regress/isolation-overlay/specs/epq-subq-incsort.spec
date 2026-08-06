# EPQ-unique admission wave (fix/epq-unique-recheck): IncrementalSort
# INSIDE the EPQ recheck plan — a two-column DISTINCT subquery whose
# input is presorted on the leading key by an index
# (hashagg/hashjoin/seqscan off), giving Unique over Incremental Sort
# over an index scan. Same two-session storm as epq-storm-unique. The
# EXPLAIN step pins the plan shape into the expected file. Expected
# output is the C oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE docs (id int PRIMARY KEY, status text);
 CREATE TABLE approvals (doc_id int, approver text, approved bool);
 CREATE INDEX approvals_doc_id_idx ON approvals (doc_id);
 INSERT INTO docs SELECT g, 'pending' FROM generate_series(1, 5) g;
 INSERT INTO approvals SELECT g, 'alice', true FROM generate_series(1, 5) g;
 INSERT INTO approvals SELECT g, 'bob', true FROM generate_series(1, 5) g;
 ANALYZE docs;
 ANALYZE approvals;
}

teardown
{
 DROP TABLE docs;
 DROP TABLE approvals;
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
}
step expl	{ EXPLAIN (COSTS OFF) SELECT d.* FROM docs d JOIN (SELECT DISTINCT doc_id, approver FROM approvals) a ON a.doc_id = d.id FOR UPDATE OF d; }
step lock	{ SELECT d.* FROM docs d JOIN (SELECT DISTINCT doc_id, approver FROM approvals) a ON a.doc_id = d.id FOR UPDATE OF d; }

session s3
step sel	{ SELECT id, status FROM docs ORDER BY id; }

permutation expl s1u lock s1c sel
permutation expl s1k lock s1c sel
