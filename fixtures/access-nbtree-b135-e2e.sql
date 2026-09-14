-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-135-backend-access-nbtree (C 18.6 oracle vs pgrust). Each leg is one
-- row of the batch (scripts/access-nbtree-b135-e2e.sh).
\set VERBOSITY verbose
CREATE DATABASE b135e2e TEMPLATE template0 ENCODING 'UTF8';
\c b135e2e
\set VERBOSITY verbose
-- fp-nbtree-nbtsort#1 / fp-nbtree-nbtutils-p2#1: a SQL-language BTEQUALIMAGE
-- support function runs in an armed result context (CREATE INDEX succeeds).
CREATE FUNCTION b135_eqimg(oid) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT true';
CREATE FUNCTION b135_eqimg_null(oid) RETURNS bool LANGUAGE sql IMMUTABLE AS 'SELECT NULL::bool';
CREATE OPERATOR CLASS b135_int4_ops FOR TYPE int4 USING btree AS
  OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >,
  FUNCTION 1 btint4cmp(int4,int4), FUNCTION 4 b135_eqimg(oid);
CREATE OPERATOR CLASS b135_int4_null_ops FOR TYPE int4 USING btree AS
  OPERATOR 1 <, OPERATOR 2 <=, OPERATOR 3 =, OPERATOR 4 >=, OPERATOR 5 >,
  FUNCTION 1 btint4cmp(int4,int4), FUNCTION 4 b135_eqimg_null(oid);
CREATE TABLE b135_t (a int);
INSERT INTO b135_t SELECT g % 10 FROM generate_series(1,1000) g;
CREATE INDEX b135_i ON b135_t (a b135_int4_ops);
SET enable_seqscan = off;
SELECT count(*) FROM b135_t WHERE a = 3;
RESET enable_seqscan;
-- fp-nbtree-nbtsort#2 / fp-nbtree-nbtutils-p2#2: a NULL from the equal-image
-- function is XX000 "function N returned NULL" (OID normalized).
DO $$ BEGIN
  CREATE INDEX b135_i2 ON b135_t (a b135_int4_null_ops);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE '% %', SQLSTATE, regexp_replace(SQLERRM, '\d+', 'N');
END $$;
SELECT count(*) FROM pg_class WHERE relname = 'b135_i2';
-- btbuildempty (unlogged index init fork) takes the same two paths.
CREATE UNLOGGED TABLE b135_ut (a int);
CREATE INDEX b135_ui ON b135_ut (a b135_int4_ops);
DO $$ BEGIN
  CREATE INDEX b135_ui2 ON b135_ut (a b135_int4_null_ops);
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE '% %', SQLSTATE, regexp_replace(SQLERRM, '\d+', 'N');
END $$;
-- fp-nbtree-nbtpreprocesskeys#1: scankey redundancy/contradiction checks and
-- array-element extreme comparisons on a composite (record_cmp) key column.
CREATE TYPE b135_ct AS (x int, y int);
CREATE TABLE b135_c (c b135_ct);
INSERT INTO b135_c SELECT ROW(g, g)::b135_ct FROM generate_series(1,100) g;
CREATE INDEX b135_c_i ON b135_c (c);
SET enable_seqscan = off;
SET enable_bitmapscan = off;
SELECT * FROM b135_c WHERE c > ROW(10,10)::b135_ct AND c > ROW(20,20)::b135_ct ORDER BY c LIMIT 3;
SELECT * FROM b135_c WHERE c >= ROW(10,10)::b135_ct AND c > ROW(9,9)::b135_ct ORDER BY c LIMIT 2;
SELECT * FROM b135_c WHERE c > ROW(10,10)::b135_ct AND c < ROW(14,14)::b135_ct ORDER BY c;
SELECT * FROM b135_c WHERE c > ROW(30,30)::b135_ct AND c < ROW(14,14)::b135_ct ORDER BY c;
SELECT * FROM b135_c WHERE c = ROW(30,30)::b135_ct AND c < ROW(14,14)::b135_ct ORDER BY c;
SELECT * FROM b135_c WHERE c > ANY(ARRAY[ROW(97,97)::b135_ct, ROW(98,98)::b135_ct]) ORDER BY c;
SELECT * FROM b135_c WHERE c < ANY(ARRAY[ROW(2,2)::b135_ct, ROW(3,3)::b135_ct]) ORDER BY c;
SELECT * FROM b135_c WHERE c = ANY(ARRAY[ROW(5,5)::b135_ct, ROW(7,7)::b135_ct]) AND c > ROW(6,6)::b135_ct ORDER BY c;
RESET enable_seqscan;
RESET enable_bitmapscan;
