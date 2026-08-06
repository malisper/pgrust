# Whitelist-completion wave (fix/epq-whitelist-completion): ProjectSet
# INSIDE the EPQ recheck plan — an SRF in a subquery tlist under a locking
# join (LockRows -> HashJoin -> SubqueryScan -> ProjectSet -> SeqScan).
# The recheck re-runs the subquery (the SRF re-fires) while the locked rel
# substitutes its test tuple. The EXPLAIN step pins the plan shape into
# the expected file. Expected output is the C oracle's (PostgreSQL 18.3):
# pass arm re-returns the locked row (twice — one per SRF value) with its
# NEW status; skip arm moves the join key away and both pairs are skipped.

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
step expl	{ EXPLAIN (COSTS OFF) SELECT d.* FROM docs d JOIN (SELECT doc_id, generate_series(1, 2) AS n FROM approvals WHERE approved) a ON a.doc_id = d.id FOR UPDATE OF d; }
step lock	{ SELECT d.* FROM docs d JOIN (SELECT doc_id, generate_series(1, 2) AS n FROM approvals WHERE approved) a ON a.doc_id = d.id FOR UPDATE OF d; }

session s3
step sel	{ SELECT id, status FROM docs ORDER BY id; }

# Pass arm: the status change commits mid-lock; the recheck re-runs the
# SRF subquery and re-returns the locked row (per SRF copy) with its NEW
# status.
permutation expl s1u lock s1c sel
# Skip arm: the locked row's join key moves away (id -> 99); the recheck
# join fails and both SRF pairs for that row are skipped.
permutation expl s1k lock s1c sel
