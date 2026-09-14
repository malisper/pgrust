-- Two-binary differential corpus for the 2026-09-13 bug-inventory batch
-- batch-159-backend-utils-adt (C 18.6 oracle vs pgrust). Each leg is one row
-- of the batch (utils/adt: acl, jsonb, network, rangetypes, adt_amutils,
-- partitionfuncs, dbsize; commands/analyze).
\set VERBOSITY verbose
CREATE DATABASE b159e2e TEMPLATE template0 ENCODING 'UTF8';
\c b159e2e
\set VERBOSITY verbose
-- fp-adt-acl-p1#1: two aclitemout calls in one projection keep distinct results.
SELECT aclitemout(a), aclitemout(b) FROM (VALUES ('postgres=r/postgres'::aclitem, '=w/postgres'::aclitem)) v(a,b);
-- fp-adt-jsonfuncs-p2#1: a one-argument LANGUAGE internal alias of jsonb_strip_nulls.
CREATE FUNCTION b159_jsn(jsonb) RETURNS jsonb LANGUAGE internal AS 'jsonb_strip_nulls';
SELECT b159_jsn('{"a":null,"b":[null,1]}');
-- fp-adt-b2#2: ?| / ?& / #> over text arrays with NULL members (lazy key walk).
SELECT '{"a":1}'::jsonb ?| ARRAY[NULL,'a'], '{"a":1}'::jsonb ?& ARRAY[NULL,'a'], '{"a":1}'::jsonb ?& ARRAY['a','b'], '{"a":{"b":2}}'::jsonb #> ARRAY['a','b'], '{"a":{"b":2}}'::jsonb #> ARRAY['a',NULL];
-- fp-adt-network#1: set_masklen clones the stored (packed) image.
CREATE TABLE b159_inet(i inet); INSERT INTO b159_inet VALUES ('192.168.1.1/24');
SELECT pg_column_size(i), pg_column_size(set_masklen(i,16)), set_masklen(i,16), pg_column_size(set_masklen('192.168.1.1/24'::inet,16)) FROM b159_inet;
-- fp-adt-rangetypes#1: a canonical function that calls the type's own constructor
-- (one constructor per backend: C 18.6 segfaults after repeated re-entrant calls
-- through the typcache's shared FmgrInfo, see UPSTREAM-BUGS; \c reconnects).
CREATE TYPE b159_r;
CREATE FUNCTION b159_r_canon(b159_r) RETURNS b159_r LANGUAGE internal IMMUTABLE STRICT AS 'int4range_canonical';
CREATE TYPE b159_r AS RANGE (subtype = int4, canonical = b159_r_canon);
CREATE OR REPLACE FUNCTION b159_r_canon(b159_r) RETURNS b159_r LANGUAGE sql IMMUTABLE STRICT AS $$
  SELECT CASE WHEN NOT lower_inc($1) THEN b159_r(lower($1)+1, upper($1), '[' || CASE WHEN upper_inc($1) THEN ']' ELSE ')' END)
              WHEN upper_inc($1) THEN b159_r(lower($1), upper($1)+1, '[)')
              ELSE $1 END $$;
SELECT b159_r(1,5,'(]');
\c b159e2e
SELECT b159_r(1,5,'()');
\c b159e2e
SELECT b159_r(1,5,'[]');
\c b159e2e
SELECT '(1,5]'::b159_r;
\c b159e2e
-- fp-adt-rangetypes_gist#1: GiST keys over a SQL-language canonical (packed result).
CREATE TYPE b159_r2;
CREATE FUNCTION b159_r2_canon(b159_r2) RETURNS b159_r2 LANGUAGE internal IMMUTABLE STRICT AS 'int4range_canonical';
CREATE TYPE b159_r2 AS RANGE (subtype = int4, canonical = b159_r2_canon);
CREATE OR REPLACE FUNCTION b159_r2_canon(b159_r2) RETURNS b159_r2 LANGUAGE sql IMMUTABLE STRICT AS 'SELECT $1';
CREATE TABLE b159_rg(r b159_r2); INSERT INTO b159_rg SELECT b159_r2(g, g+3) FROM generate_series(0, 2000) g;
CREATE INDEX b159_rg_i ON b159_rg USING gist(r);
SET enable_seqscan = off;
SELECT count(*), min(lower(r)), max(upper(r)) FROM b159_rg WHERE r && b159_r2(1000,1010);
SELECT count(*) FROM b159_rg WHERE r = b159_r2(10,13);
RESET enable_seqscan;
-- fp-adt-amutils#1: a property name compared through its first NUL.
CREATE FUNCTION b159_bi(cstring) RETURNS text LANGUAGE internal AS 'byteain';
SELECT pg_indexam_has_property(a.oid, b159_bi('can_order\000junk')), pg_indexam_has_property(a.oid, b159_bi('\000can_order')) FROM pg_am a WHERE a.amname = 'btree';
-- fp-adt-array_typanalyze#1: an internal alias of array_typanalyze on a non-array type.
CREATE FUNCTION b159_ta(internal) RETURNS boolean LANGUAGE internal AS 'array_typanalyze';
CREATE TYPE b159_myint;
CREATE FUNCTION b159_myint_in(cstring) RETURNS b159_myint LANGUAGE internal IMMUTABLE STRICT AS 'int4in';
CREATE FUNCTION b159_myint_out(b159_myint) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'int4out';
CREATE TYPE b159_myint (INPUT = b159_myint_in, OUTPUT = b159_myint_out, LIKE = int4, ANALYZE = b159_ta);
CREATE TABLE b159_an(a b159_myint); INSERT INTO b159_an VALUES ('1'), ('2');
DO $$ BEGIN ANALYZE b159_an; EXCEPTION WHEN OTHERS THEN RAISE NOTICE '% %', SQLSTATE, regexp_replace(SQLERRM, '\d+$', 'N'); END $$;
-- fp-adt-b3#1: pg_partition_tree through a RECORD alias with a column definition list.
CREATE TABLE b159_p(a int) PARTITION BY LIST (a); CREATE TABLE b159_p1 PARTITION OF b159_p FOR VALUES IN (1);
CREATE FUNCTION b159_ppt(regclass) RETURNS SETOF record LANGUAGE internal AS 'pg_partition_tree';
SELECT * FROM b159_ppt('b159_p') AS t(relid regclass, parentrelid regclass, isleaf bool, level int) ORDER BY level, relid;
-- fp-adt-dbsize#1: a non-UTF-8 name reaches the lookup (SQL_ASCII database).
CREATE DATABASE b159e2ea TEMPLATE template0 ENCODING 'SQL_ASCII' LC_COLLATE 'C' LC_CTYPE 'C';
\c b159e2ea
\set VERBOSITY verbose
SELECT pg_database_size(chr(255)::name);
SELECT pg_tablespace_size(chr(255)::name);
