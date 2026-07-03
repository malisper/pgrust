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

SELECT attnum, attname, atttypid, attnotnull, atthasdef, attstorage
  FROM pg_attribute WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 't2')
    AND attnum > 0 ORDER BY attnum;
SELECT adnum, adbin FROM pg_attrdef
 WHERE adrelid = (SELECT oid FROM pg_class WHERE relname = 't2') ORDER BY adnum;
SELECT conname, contype, condeferrable, conenforced, convalidated,
       conislocal, coninhcount, connoinherit, conkey, conbin
  FROM pg_constraint WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 't2')
 ORDER BY conname;
SELECT relchecks FROM pg_class WHERE relname = 't2';

CREATE TABLE t3 (LIKE t1 INCLUDING INDEXES);
SELECT relname, relkind FROM pg_class WHERE relname = 't3_a_idx';
SELECT relname, relkind FROM pg_class WHERE relname = 't3_b_idx';
SELECT indisunique, indisprimary, indkey, indcollation, indclass, indoption
  FROM pg_index WHERE indexrelid = (SELECT oid FROM pg_class WHERE relname = 't3_a_idx');
SELECT indisunique, indisprimary, indkey, indcollation, indclass, indoption
  FROM pg_index WHERE indexrelid = (SELECT oid FROM pg_class WHERE relname = 't3_b_idx');
INSERT INTO t3 (a) VALUES (7);
INSERT INTO t3 (a) VALUES (7);

CREATE TABLE t6 (x text, LIKE t1 INCLUDING DEFAULTS INCLUDING CONSTRAINTS, y int);
SELECT attnum, attname
  FROM pg_attribute WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 't6')
    AND attnum > 0 ORDER BY attnum;
SELECT conname, conkey, conbin
  FROM pg_constraint WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 't6')
    AND contype = 'c' ORDER BY conname;
INSERT INTO t6 (x) VALUES ('row');
SELECT x, a, b, c, y FROM t6;
INSERT INTO t6 (a) VALUES (-5);

CREATE TABLE t7 (LIKE t1 INCLUDING COMMENTS);
SELECT objsubid, description FROM pg_description
 WHERE objoid = (SELECT oid FROM pg_class WHERE relname = 't7') AND classoid = 1259
 ORDER BY objsubid;

CREATE TABLE t8 (LIKE t1 EXCLUDING ALL);
SELECT attnum, attname, attnotnull, atthasdef
  FROM pg_attribute WHERE attrelid = (SELECT oid FROM pg_class WHERE relname = 't8')
    AND attnum > 0 ORDER BY attnum;
SELECT count(*) FROM pg_constraint
 WHERE conrelid = (SELECT oid FROM pg_class WHERE relname = 't8') AND contype = 'c';

CREATE TABLE t9 (LIKE no_such_table);

DROP TABLE t8;
DROP TABLE t7;
DROP TABLE t6;
DROP TABLE t3;
DROP TABLE t2;
DROP TABLE t1;
