# Whitelist-completion wave (fix/epq-whitelist-completion):
# NamedTuplestoreScan INSIDE the EPQ recheck plan — an AFTER STATEMENT
# trigger's transition table (REFERENCING NEW TABLE), read by the
# trigger's own UPDATE ... FROM new_docs, which contends with a concurrent
# committed update of its target (ModifyTable subplan: HashJoin ->
# NamedTuplestoreScan + SeqScan on the contended rel). Named tuplestores
# exist only inside trigger execution, so the plan shape is pinned
# in-expected by the trigger itself: gated on the epq_expl control table,
# it EXPLAINs its statement into epq_plan (a NOTICE would interleave
# nondeterministically with the tester's step lines), and the showplan
# step SELECTs the captured lines. Expected output is the C oracle's
# (PostgreSQL 18.3): pass arm applies the trigger's +1 on top of the
# concurrent +10 via the recheck; skip arm deletes the target row so the
# trigger's join matches nothing.

setup
{
 CREATE TABLE docs (id int PRIMARY KEY, status text);
 CREATE TABLE audit_target (id int PRIMARY KEY, hits int DEFAULT 0);
 CREATE TABLE epq_expl (on_off bool);
 CREATE TABLE epq_plan (seq serial, ln text);
 INSERT INTO docs SELECT g, 'pending' FROM generate_series(1, 5) g;
 INSERT INTO audit_target SELECT g, 0 FROM generate_series(1, 5) g;
 CREATE FUNCTION docs_au() RETURNS trigger LANGUAGE plpgsql AS $$
 DECLARE l text;
 BEGIN
   IF EXISTS (SELECT 1 FROM epq_expl) THEN
     FOR l IN EXECUTE 'EXPLAIN (COSTS OFF) UPDATE audit_target t SET hits = t.hits + 1 FROM new_docs n WHERE t.id = n.id' LOOP
       INSERT INTO epq_plan (ln) VALUES (l);
     END LOOP;
   END IF;
   UPDATE audit_target t SET hits = t.hits + 1 FROM new_docs n WHERE t.id = n.id;
   RETURN NULL;
 END $$;
 CREATE TRIGGER docs_au AFTER UPDATE ON docs REFERENCING NEW TABLE AS new_docs
   FOR EACH STATEMENT EXECUTE FUNCTION docs_au();
 ANALYZE docs;
 ANALYZE audit_target;
}

teardown
{
 DROP TABLE docs;
 DROP TABLE audit_target;
 DROP TABLE epq_expl;
 DROP TABLE epq_plan;
 DROP FUNCTION docs_au();
}

session s1
setup		{ BEGIN ISOLATION LEVEL READ COMMITTED; }
step s1l	{ UPDATE audit_target SET hits = hits + 10 WHERE id = 2; }
step s1k	{ DELETE FROM audit_target WHERE id = 2; }
step s1c	{ COMMIT; }

session s2
step expl	{ INSERT INTO epq_expl VALUES (true); UPDATE docs SET status = status WHERE id = 1; DELETE FROM epq_expl; }
step upd	{ UPDATE docs SET status = 'touched' WHERE id = 2; }

session s3
step showplan	{ SELECT ln FROM epq_plan ORDER BY seq; }
step sel	{ SELECT id, hits FROM audit_target ORDER BY id; }

# Pass arm: the trigger's inner UPDATE blocks on s1's +10; at commit the
# recheck re-reads the transition row and applies +1 on the NEW version
# (row 2 ends at 11; row 1 carries the expl step's +1).
permutation expl showplan s1l upd s1c sel
# Skip arm: s1 deletes the target row; the trigger's join matches nothing
# and the row stays deleted.
permutation expl showplan s1k upd s1c sel
