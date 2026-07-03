\set VERBOSITY verbose
CREATE TABLE t1 (
  a int NOT NULL DEFAULT 42 CHECK (a > 0),
  b text DEFAULT 'hello',
  c double precision CONSTRAINT c_small CHECK (c < 1000)
);
CREATE INDEX t1_b_idx ON t1 (b);
CREATE UNIQUE INDEX t1_a_key ON t1 (a DESC NULLS LAST);
COMMENT ON COLUMN t1.b IS 'b column comment';

CREATE TABLE t2 (LIKE t1 INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
INSERT INTO t2 (c) VALUES (1.5);
SELECT a, b, c FROM t2;
INSERT INTO t2 (a) VALUES (-1);
INSERT INTO t2 (a) VALUES (NULL);
INSERT INTO t2 (c) VALUES (2000);

SELECT a.attnum, a.attname, a.atttypid, a.attnotnull, a.atthasdef, a.attstorage
  FROM pg_attribute a, pg_class c
 WHERE a.attrelid = c.oid AND c.relname = 't2' AND a.attnum > 0
 ORDER BY a.attnum;
SELECT a.attnum, d.adbin
  FROM pg_attrdef d, pg_attribute a, pg_class c
 WHERE d.adrelid = c.oid AND a.attrelid = c.oid AND a.attnum = d.adnum AND c.relname = 't2'
 ORDER BY a.attnum;
SELECT r.conname, r.contype, r.condeferrable, r.conenforced, r.convalidated,
       r.conislocal, r.coninhcount, r.connoinherit, r.conkey, r.conbin
  FROM pg_constraint r, pg_class c
 WHERE r.conrelid = c.oid AND c.relname = 't2'
 ORDER BY r.conname;

CREATE TABLE t3 (LIKE t1 INCLUDING INDEXES);
SELECT c2.relname, i.indisunique, i.indisprimary, i.indkey
  FROM pg_index i, pg_class c2, pg_class c
 WHERE i.indexrelid = c2.oid AND i.indrelid = c.oid AND c.relname = 't3'
 ORDER BY c2.relname;
INSERT INTO t3 (a) VALUES (7);
INSERT INTO t3 (a) VALUES (7);

CREATE TABLE t6 (x text, LIKE t1 INCLUDING DEFAULTS INCLUDING CONSTRAINTS, y int);
SELECT a.attnum, a.attname
  FROM pg_attribute a, pg_class c
 WHERE a.attrelid = c.oid AND c.relname = 't6' AND a.attnum > 0
 ORDER BY a.attnum;
SELECT r.conname, r.conkey, r.conbin
  FROM pg_constraint r, pg_class c
 WHERE r.conrelid = c.oid AND c.relname = 't6' AND r.contype = 'c'
 ORDER BY r.conname;
INSERT INTO t6 (x) VALUES ('row');
SELECT x, a, b, c, y FROM t6;
INSERT INTO t6 (a) VALUES (-5);

CREATE TABLE t7 (LIKE t1 INCLUDING COMMENTS);
SELECT a.attname, dsc.description
  FROM pg_description dsc, pg_attribute a, pg_class c
 WHERE dsc.objoid = c.oid AND dsc.classoid = 1259 AND a.attrelid = c.oid
   AND a.attnum = dsc.objsubid AND c.relname = 't7'
 ORDER BY a.attnum;

CREATE TABLE t8 (LIKE t1 EXCLUDING ALL);
SELECT a.attnum, a.attname, a.attnotnull, a.atthasdef
  FROM pg_attribute a, pg_class c
 WHERE a.attrelid = c.oid AND c.relname = 't8' AND a.attnum > 0
 ORDER BY a.attnum;
SELECT count(*) FROM pg_constraint r, pg_class c
 WHERE r.conrelid = c.oid AND c.relname = 't8' AND r.contype = 'c';

CREATE TABLE t9 (LIKE no_such_table);
CREATE TABLE t10 (LIKE t1 INCLUDING BOGUS);

DROP TABLE t8;
DROP TABLE t7;
DROP TABLE t6;
DROP TABLE t3;
DROP TABLE t2;
DROP TABLE t1;
