-- Audit item A5 (notes/audits/unported-panic-inventory-2026-08-08.md):
-- ALTER TABLE ... ADD CONSTRAINT ... EXCLUDE was inventoried as an unported
-- ALTER lane. Pin the full lane at C parity: add on empty table, enforcement,
-- existing-data violation, invalid AM / operator identity, deferrable,
-- partitioned, foreign table, and ALTER TYPE rewrite preservation.
-- Expected output captured from real PostgreSQL 18.
CREATE TABLE ae (c circle, i int);
-- basic add on empty table
ALTER TABLE ae ADD CONSTRAINT ae_x EXCLUDE USING gist (c WITH &&);
INSERT INTO ae VALUES ('<(0,0),2>', 1);
INSERT INTO ae VALUES ('<(1,1),2>', 2);
INSERT INTO ae VALUES ('<(10,10),1>', 3);
\d ae
ALTER TABLE ae DROP CONSTRAINT ae_x;
-- existing-data violation
INSERT INTO ae VALUES ('<(0,0),3>', 4);
ALTER TABLE ae ADD CONSTRAINT ae_x2 EXCLUDE USING gist (c WITH &&);
-- non-indexable AM
ALTER TABLE ae ADD CONSTRAINT ae_gin EXCLUDE USING gin (i WITH =);
ALTER TABLE ae ADD CONSTRAINT ae_brin EXCLUDE USING brin (i WITH =);
-- operator not in opclass
ALTER TABLE ae ADD CONSTRAINT ae_badop EXCLUDE USING gist (c WITH =);
-- non-commutative operator
ALTER TABLE ae ADD CONSTRAINT ae_noncommut EXCLUDE USING gist (c WITH <<);
-- btree exclusion with where clause
DELETE FROM ae;
ALTER TABLE ae ADD CONSTRAINT ae_btree EXCLUDE USING btree (i WITH =) WHERE (i > 0);
INSERT INTO ae VALUES (NULL, 5);
INSERT INTO ae VALUES (NULL, 5);
INSERT INTO ae VALUES (NULL, -5);
INSERT INTO ae VALUES (NULL, -5);
\d ae
-- deferrable exclusion, violation caught at commit
ALTER TABLE ae ADD CONSTRAINT ae_def EXCLUDE USING btree (i WITH =) DEFERRABLE INITIALLY DEFERRED;
BEGIN;
INSERT INTO ae VALUES (NULL, 7);
INSERT INTO ae VALUES (NULL, 7);
COMMIT;
-- NOT VALID rejected
ALTER TABLE ae ADD CONSTRAINT ae_nv EXCLUDE USING btree (i WITH =) NOT VALID;
-- foreign table refusal
CREATE FOREIGN DATA WRAPPER a5dummy;
CREATE SERVER a5srv FOREIGN DATA WRAPPER a5dummy;
CREATE FOREIGN TABLE aef (i int) SERVER a5srv;
ALTER TABLE aef ADD CONSTRAINT aef_x EXCLUDE USING btree (i WITH =);
-- partitioned table: non-key exclusion refused, key-equality allowed
CREATE TABLE aep (i int, c circle) PARTITION BY LIST (i);
ALTER TABLE aep ADD CONSTRAINT aep_x EXCLUDE USING gist (c WITH &&);
ALTER TABLE aep ADD CONSTRAINT aep_ok EXCLUDE USING btree (i WITH =);
CREATE TABLE aep1 PARTITION OF aep FOR VALUES IN (1);
INSERT INTO aep VALUES (1, NULL);
INSERT INTO aep VALUES (1, NULL);
\d aep
-- expression key
CREATE TABLE aee (t text);
ALTER TABLE aee ADD CONSTRAINT aee_x EXCLUDE USING btree (lower(t) WITH =);
INSERT INTO aee VALUES ('Foo');
INSERT INTO aee VALUES ('FOO');
-- ALTER TYPE rewrite preserving an ALTER-added exclusion constraint
CREATE TABLE aer (i int);
ALTER TABLE aer ADD CONSTRAINT aer_x EXCLUDE USING btree (i WITH =);
ALTER TABLE aer ALTER COLUMN i TYPE bigint;
\d aer
INSERT INTO aer VALUES (9);
INSERT INTO aer VALUES (9);
-- cleanup
DROP TABLE ae, aep, aee, aer;
DROP FOREIGN TABLE aef;
DROP SERVER a5srv;
DROP FOREIGN DATA WRAPPER a5dummy;
