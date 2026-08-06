# Whitelist-completion wave (fix/epq-whitelist-completion): SampleScan
# INSIDE the EPQ recheck plan — a TABLESAMPLE join source under FOR UPDATE
# (LockRows -> HashJoin -> SampleScan + SeqScan on the locked rel).
# BERNOULLI (100) REPEATABLE (1) makes the sample total and deterministic.
# The sampled rel is a non-locked top-level rel, so the recheck answers it
# through its aux rowmark; the SampleScan node itself still inits inside
# the recheck tree (the tag the loud list previously refused). The EXPLAIN
# step pins the plan shape into the expected file. Expected output is the
# C oracle's (PostgreSQL 18.3).

setup
{
 CREATE TABLE docs (id int PRIMARY KEY, status text);
 CREATE TABLE approvals (doc_id int, approver text, approved bool);
 INSERT INTO docs SELECT g, 'pending' FROM generate_series(1, 5) g;
 INSERT INTO approvals SELECT g, 'alice', true FROM generate_series(1, 5) g;
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
step expl	{ EXPLAIN (COSTS OFF) SELECT d.* FROM docs d JOIN approvals a TABLESAMPLE BERNOULLI (100) REPEATABLE (1) ON a.doc_id = d.id FOR UPDATE OF d; }
step lock	{ SELECT d.* FROM docs d JOIN approvals a TABLESAMPLE BERNOULLI (100) REPEATABLE (1) ON a.doc_id = d.id FOR UPDATE OF d; }

session s3
step sel	{ SELECT id, status FROM docs ORDER BY id; }

# Pass arm: locked row re-returned with its NEW status.
permutation expl s1u lock s1c sel
# Skip arm: the locked row's join key moves away; row skipped.
permutation expl s1k lock s1c sel
