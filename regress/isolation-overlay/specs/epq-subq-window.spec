# EPQ-unique admission wave (fix/epq-unique-recheck): WindowAgg INSIDE
# the EPQ recheck plan — a row_number() window subquery under a
# row-locking join (hashagg/hashjoin off keep the sorted shape; the rn
# bound becomes a WindowAgg run condition). Same two-session storm as
# epq-storm-unique. The EXPLAIN step pins the plan shape into the
# expected file. Expected output is the C oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE docs (id int PRIMARY KEY, status text);
 CREATE TABLE approvals (doc_id int, approver text, approved bool);
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
}
step expl	{ EXPLAIN (COSTS OFF) SELECT d.* FROM docs d JOIN (SELECT doc_id, row_number() OVER (ORDER BY doc_id) rn FROM approvals WHERE approved) a ON a.doc_id = d.id AND a.rn <= 100 FOR UPDATE OF d; }
step lock	{ SELECT d.* FROM docs d JOIN (SELECT doc_id, row_number() OVER (ORDER BY doc_id) rn FROM approvals WHERE approved) a ON a.doc_id = d.id AND a.rn <= 100 FOR UPDATE OF d; }

session s3
step sel	{ SELECT id, status FROM docs ORDER BY id; }

permutation expl s1u lock s1c sel
permutation expl s1k lock s1c sel
