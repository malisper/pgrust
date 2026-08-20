-- bug_942f9c77 (extended): SET CONSTRAINTS <name> on a DEFERRABLE constraint
-- that has NO associated triggers must be a legal no-op, matching C's
-- AfterTriggerSetState (trigger.c). A NOT ENFORCED foreign key gates trigger
-- creation on is_enforced while condeferrable may still be set, so it yields a
-- deferrable constraint with zero triggers. pgrust used to assert("no triggers
-- found ...") and turn the empty trigger scan into an error. Four routes reach
-- the same empty-scan path; all four must match C byte-for-byte.
CREATE TABLE pk (id int PRIMARY KEY);
CREATE TABLE fk (fk_id int);
ALTER TABLE fk ADD CONSTRAINT fk_fk_id_fkey FOREIGN KEY(fk_id) REFERENCES pk NOT ENFORCED DEFERRABLE;
BEGIN;
SET CONSTRAINTS fk_fk_id_fkey DEFERRED;
SET CONSTRAINTS fk_fk_id_fkey IMMEDIATE;
COMMIT;
-- SET CONSTRAINTS ALL still works with a NOT ENFORCED deferrable FK present.
BEGIN;
SET CONSTRAINTS ALL DEFERRED;
COMMIT;
DROP TABLE fk;
DROP TABLE pk;

-- Route 1: a named list mixing an ENFORCED and a NOT ENFORCED deferrable FK.
-- The list must succeed (the not-enforced member is an empty-scan no-op) and
-- the enforced member's deferral must actually take effect: a violating row is
-- tolerated mid-transaction and the error is raised at COMMIT.
CREATE TABLE r1_pk (id int PRIMARY KEY);
INSERT INTO r1_pk VALUES (1);
CREATE TABLE r1_fk (a int, b int);
ALTER TABLE r1_fk ADD CONSTRAINT r1_enf FOREIGN KEY(a) REFERENCES r1_pk DEFERRABLE;
ALTER TABLE r1_fk ADD CONSTRAINT r1_notenf FOREIGN KEY(b) REFERENCES r1_pk NOT ENFORCED DEFERRABLE;
BEGIN;
SET CONSTRAINTS r1_enf, r1_notenf DEFERRED;
INSERT INTO r1_fk VALUES (99, 99);
COMMIT;
SELECT count(*) FROM r1_fk;
DROP TABLE r1_fk;
DROP TABLE r1_pk;

-- Route 2: a NOT ENFORCED DEFERRABLE FK on a PARTITIONED parent. The
-- conparentid descendant expansion pulls in the per-partition constraints,
-- each of which also has zero triggers; SET CONSTRAINTS on the parent name
-- must be a clean no-op.
CREATE TABLE r2_pk (id int PRIMARY KEY);
CREATE TABLE r2_fk (id int, ref int) PARTITION BY RANGE (id);
CREATE TABLE r2_fk_p1 PARTITION OF r2_fk FOR VALUES FROM (1) TO (100);
CREATE TABLE r2_fk_p2 PARTITION OF r2_fk FOR VALUES FROM (100) TO (200);
ALTER TABLE r2_fk ADD CONSTRAINT r2_c FOREIGN KEY(ref) REFERENCES r2_pk NOT ENFORCED DEFERRABLE;
BEGIN;
SET CONSTRAINTS r2_c DEFERRED;
SET CONSTRAINTS r2_c IMMEDIATE;
COMMIT;
DROP TABLE r2_fk;
DROP TABLE r2_pk;

-- Route 3: same-name collateral. Two tables each carry a constraint of the
-- SAME name in the same schema, one ENFORCED and one NOT ENFORCED. A bare
-- SET CONSTRAINTS <sharedname> resolves to BOTH. The enforced one's deferral
-- must take effect and the not-enforced one must be a clean no-op that never
-- enforces.
CREATE TABLE r3_pk (id int PRIMARY KEY);
INSERT INTO r3_pk VALUES (1);
CREATE TABLE r3_a (x int);
CREATE TABLE r3_b (y int);
ALTER TABLE r3_a ADD CONSTRAINT shared_fk FOREIGN KEY(x) REFERENCES r3_pk DEFERRABLE;
ALTER TABLE r3_b ADD CONSTRAINT shared_fk FOREIGN KEY(y) REFERENCES r3_pk NOT ENFORCED DEFERRABLE;
-- (a) the enforced same-name constraint is genuinely deferred: violating row
--     tolerated mid-txn, error raised at COMMIT.
BEGIN;
SET CONSTRAINTS shared_fk DEFERRED;
INSERT INTO r3_a VALUES (42);
COMMIT;
SELECT count(*) FROM r3_a;
-- (b) the not-enforced same-name constraint never enforces: a violating row
--     commits cleanly under the same SET CONSTRAINTS.
BEGIN;
SET CONSTRAINTS shared_fk DEFERRED;
INSERT INTO r3_b VALUES (77);
COMMIT;
SELECT count(*) FROM r3_b;
DROP TABLE r3_a;
DROP TABLE r3_b;
DROP TABLE r3_pk;

-- Route 4: a schema-qualified name on a NOT ENFORCED constraint. The explicit
-- namespace lookup path must resolve the constraint and no-op cleanly.
CREATE SCHEMA r4_s;
CREATE TABLE r4_s.pk (id int PRIMARY KEY);
CREATE TABLE r4_s.fk (ref int);
ALTER TABLE r4_s.fk ADD CONSTRAINT r4_c FOREIGN KEY(ref) REFERENCES r4_s.pk NOT ENFORCED DEFERRABLE;
BEGIN;
SET CONSTRAINTS r4_s.r4_c DEFERRED;
SET CONSTRAINTS r4_s.r4_c IMMEDIATE;
COMMIT;
DROP SCHEMA r4_s CASCADE;
