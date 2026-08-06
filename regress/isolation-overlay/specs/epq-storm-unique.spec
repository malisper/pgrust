# EPQ-unique admission wave (fix/epq-unique-recheck): a Unique node INSIDE
# the EPQ recheck plan — the live 2026-08-05 refusal repro. A row-locking
# join against a SELECT DISTINCT subquery (hashagg/hashjoin off forces
# Unique+Sort under the SubqueryScan) blocks on a concurrent committed
# update of the locked rel; the recheck re-runs the whole tree, reaching
# the Unique under the SubqueryScan (check_epq_plan previously panicked
# here: "T_Unique recheck plan ... not exercised"). The EXPLAIN step pins
# the plan shape into the expected file. Expected output is the C
# oracle's (PostgreSQL 18.3): the non-locked subquery re-runs at the
# original snapshot, so the revoked approval is still visible and the
# locked row is re-returned with its NEW values.

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
step s1r	{ UPDATE approvals SET approved = false WHERE doc_id = 3; }
step s1u	{ UPDATE docs SET status = 'revoked' WHERE id = 3; }
step s1k	{ UPDATE docs SET id = 99 WHERE id = 3; }
step s1c	{ COMMIT; }

session s2
setup
{
 SET enable_hashagg = off;
 SET enable_hashjoin = off;
}
step expl	{ EXPLAIN (COSTS OFF) SELECT d.* FROM docs d JOIN (SELECT DISTINCT doc_id FROM approvals WHERE approved) a ON a.doc_id = d.id FOR UPDATE OF d; }
step lock	{ SELECT d.* FROM docs d JOIN (SELECT DISTINCT doc_id FROM approvals WHERE approved) a ON a.doc_id = d.id FOR UPDATE OF d; }

session s3
step sel	{ SELECT id, status FROM docs ORDER BY id; }

# Pass arm: the revocation commits mid-lock; the recheck re-runs the
# DISTINCT subquery at the original snapshot (doc 3 still approved there)
# and re-returns the locked row with its NEW status.
permutation expl s1r s1u lock s1c sel
# Skip arm: the locked row's join key moves away (id -> 99, not in
# approvals); the recheck join fails and the row is skipped.
permutation expl s1r s1k lock s1c sel
