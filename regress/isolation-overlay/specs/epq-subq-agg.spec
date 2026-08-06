# EPQ-unique admission wave (fix/epq-unique-recheck): Agg (sorted
# GroupAggregate, session s2a) and Group (grouping without aggregates,
# session s2g) INSIDE the EPQ recheck plan — the "subquery/aggregate EPQ"
# class named by the old panic message. Same two-session storm as
# epq-storm-unique; hashagg/hashjoin off pin the sorted-grouping shapes.
# The EXPLAIN steps pin the plan shapes into the expected file. Expected
# output is the C oracle's (PostgreSQL 18.3).

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

session s2a
setup
{
 SET enable_hashagg = off;
 SET enable_hashjoin = off;
}
step aexpl	{ EXPLAIN (COSTS OFF) SELECT d.* FROM docs d JOIN (SELECT doc_id, count(*) n FROM approvals WHERE approved GROUP BY doc_id) a ON a.doc_id = d.id AND a.n >= 2 FOR UPDATE OF d; }
step alock	{ SELECT d.* FROM docs d JOIN (SELECT doc_id, count(*) n FROM approvals WHERE approved GROUP BY doc_id) a ON a.doc_id = d.id AND a.n >= 2 FOR UPDATE OF d; }

session s2g
setup
{
 SET enable_hashagg = off;
 SET enable_hashjoin = off;
}
step gexpl	{ EXPLAIN (COSTS OFF) SELECT d.* FROM docs d JOIN (SELECT doc_id FROM approvals WHERE approved GROUP BY doc_id) a ON a.doc_id = d.id FOR UPDATE OF d; }
step glock	{ SELECT d.* FROM docs d JOIN (SELECT doc_id FROM approvals WHERE approved GROUP BY doc_id) a ON a.doc_id = d.id FOR UPDATE OF d; }

session s3
step sel	{ SELECT id, status FROM docs ORDER BY id; }

# GroupAggregate: pass arm (locked row re-returned with NEW status) and
# skip arm (join key moves away, row skipped).
permutation aexpl s1u alock s1c sel
permutation aexpl s1k alock s1c sel
# Group: same two arms.
permutation gexpl s1u glock s1c sel
permutation gexpl s1k glock s1c sel
