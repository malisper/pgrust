-- Differential fixture for the 2026-09-13 workers-io bug batch 05
-- (DDL error ordering, DROP CAST / DROP TABLE pg_temp messages, output
-- column names, object identities, message clipping, macaddr input, error
-- message client-encoding conversion, plpgsql record flattening, support
-- functions over FuncExpr, opclass support functions returning NULL,
-- hash_array arity, case-insensitive regex ranges, hstore versions). Run via
-- scripts/bugs-batch05-e2e.sh against the frozen expected captured from C
-- PostgreSQL 18.6. This file is UTF-8 (P088 uses U+01C4..U+01C6).
\set VERBOSITY verbose
\pset pager off

-- P024: DROP TABLE pg_temp.<missing>
CREATE TEMP TABLE b05_warm (k int);
DROP TABLE pg_temp.b05_absent;
DROP TABLE IF EXISTS pg_temp.b05_absent;
DROP TABLE b05_warm;

-- P025: partition key check order
CREATE TABLE b05_many (k bigint PRIMARY KEY, v bigint,
  x1 integer, x2 integer, x3 integer, x4 integer, x5 integer, x6 integer, x7 integer, x8 integer,
  x9 integer, x10 integer, x11 integer, x12 integer, x13 integer, x14 integer, x15 integer, x16 integer,
  x17 integer, x18 integer, x19 integer, x20 integer, x21 integer, x22 integer, x23 integer, x24 integer,
  x25 integer, x26 integer, x27 integer, x28 integer, x29 integer, x30 integer, x31 integer)
PARTITION BY LIST (k, v, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15, x16,
  x17, x18, x19, x20, x21, x22, x23, x24, x25, x26, x27, x28, x29, x30, x31);
CREATE TABLE b05_sys (k bigint PRIMARY KEY, v bigint) PARTITION BY RANGE (ctid, (bogus + 1));
CREATE TABLE b05_c1 (k bigint PRIMARY KEY, v bigint) PARTITION BY LIST (k, v);
CREATE TABLE b05_c2 (k bigint PRIMARY KEY, v bigint) PARTITION BY RANGE (nosuch, (bogus + 1));
CREATE TABLE b05_c3 (k bigint PRIMARY KEY, v bigint) PARTITION BY RANGE (ctid);

-- P031: CREATE FUNCTION SET before COST / ROWS
CREATE FUNCTION b05_f(a bigint) RETURNS bigint SET nosuch = 1 COST 0 LANGUAGE sql AS 'SELECT a * 2';
CREATE FUNCTION b05_g(a bigint) RETURNS SETOF bigint SET nosuch = 1 ROWS 0 LANGUAGE sql AS 'SELECT a';
CREATE FUNCTION b05_h2(a bigint) RETURNS bigint COST 0 LANGUAGE sql AS 'SELECT a';

-- P039: DROP CAST on a shell type
CREATE TYPE b05_sh;
DROP CAST IF EXISTS (b05_sh AS int4);
DROP CAST (b05_sh AS int4);
DROP TYPE b05_sh;

-- P042: column name of a cast scalar sub-select of alias.*
CREATE TABLE b05_t (k bigint PRIMARY KEY, v bigint);
INSERT INTO b05_t VALUES (1, 2), (2, 4);
SELECT (SELECT s.* FROM (SELECT k FROM b05_t) s LIMIT 1)::text FROM b05_t;
SELECT ((SELECT s.* FROM (SELECT k::text AS kt FROM b05_t) s LIMIT 1) COLLATE "C")::text FROM b05_t;
SELECT (SELECT s.* FROM (SELECT k FROM b05_t) s LIMIT 1) FROM b05_t;
DROP TABLE b05_t;

-- P047: identity of a pg_temp function / operator
CREATE FUNCTION pg_temp.b05_tf(a bigint) RETURNS text LANGUAGE sql AS 'SELECT a::text';
SELECT regexp_replace(identity, '^pg_temp_\d+', 'pg_temp_N') FROM pg_identify_object('pg_proc'::regclass, 'pg_temp.b05_tf(bigint)'::regprocedure, 0);
SELECT object_names FROM pg_identify_object_as_address('pg_proc'::regclass, 'pg_temp.b05_tf(bigint)'::regprocedure, 0);
CREATE OPERATOR pg_temp.~#~ (FUNCTION = pg_temp.b05_tf, RIGHTARG = bigint);
SELECT regexp_replace(identity, '^pg_temp_\d+', 'pg_temp_N') FROM pg_operator o, pg_identify_object('pg_operator'::regclass, o.oid, 0) WHERE o.oprname = '~#~';
DROP OPERATOR pg_temp.~#~ (NONE, bigint); DROP FUNCTION pg_temp.b05_tf(bigint);

-- P052: over-long unit / zone names clip at a character boundary
SELECT date_part(repeat('a', 62) || 'é', '2024-01-01'::timestamp);
SELECT '2024-01-01'::timestamp AT TIME ZONE (repeat('a', 254) || 'é');

-- P054: TypeName-shaped dictionary option keeps its []
CREATE TEXT SEARCH DICTIONARY b05_d (TEMPLATE = simple, stopwords = int4[]);

-- P055: macaddr octet overflow follows sscanf %x
SELECT '10000000000000000:00:00:00:00:00'::macaddr;
SELECT '1ffffffff:00:00:00:00:00'::macaddr;
SELECT '100000000:00:00:00:00:00'::macaddr;
SELECT '-fffffffe:00:00:00:00:00'::macaddr;
SELECT '08:00:2b:01:02:03'::macaddr;

-- P061: error and notice texts convert to the client encoding
SET client_encoding = 'LATIN1';
SELECT chr(937)::int;
DO $$ BEGIN RAISE EXCEPTION '%', chr(937); END $$;
DO $$ BEGIN RAISE NOTICE '%', chr(937); END $$;
DO $$ BEGIN RAISE NOTICE 'plain'; END $$;
SELECT chr(937);
RESET client_encoding;
SELECT chr(937)::int;

-- P070: a rowtype variable flattens its external fields
CREATE TABLE b05_rt (id int, big text);
ALTER TABLE b05_rt ALTER COLUMN big SET STORAGE EXTERNAL;
INSERT INTO b05_rt SELECT 1, repeat('x', 10000);
CREATE FUNCTION b05_size() RETURNS bigint LANGUAGE plpgsql AS $$
DECLARE r b05_rt%ROWTYPE;
BEGIN SELECT * INTO r FROM b05_rt; RETURN pg_column_size(r); END $$;
SELECT b05_size();
CREATE TABLE b05_keep (r b05_rt);
CREATE FUNCTION b05_keep_fn() RETURNS void LANGUAGE plpgsql AS $$ DECLARE r b05_rt%ROWTYPE; BEGIN SELECT * INTO r FROM b05_rt; INSERT INTO b05_keep VALUES (r); END $$;
SELECT b05_keep_fn(); DELETE FROM b05_rt; VACUUM b05_rt;
SELECT length((r).big) FROM b05_keep;
DROP TABLE b05_keep, b05_rt; DROP FUNCTION b05_size(), b05_keep_fn();

-- P076: network_sub()/network_sup() over an indexed inet column
CREATE TABLE b05_net (k bigint PRIMARY KEY, c1 inet);
INSERT INTO b05_net VALUES (1, '10.1.2.3'), (2, '10.1.0.0/16'), (3, '192.168.0.1');
CREATE INDEX b05_net_c1 ON b05_net (c1);
SELECT * FROM b05_net WHERE network_sub(c1, inet '10.0.0.0/8') ORDER BY k;
SELECT * FROM b05_net WHERE network_sup(inet '10.0.0.0/8', c1) ORDER BY k;
SELECT * FROM b05_net WHERE c1 << inet '10.0.0.0/8' ORDER BY k;
DROP TABLE b05_net;

-- P083 / P084: opclass support functions returning NULL; hash_array arity
CREATE TYPE b05_en AS ENUM ('a', 'b', 'c');
CREATE FUNCTION b05_nulleq(b05_en, b05_en) RETURNS boolean LANGUAGE sql AS 'SELECT NULL::boolean';
CREATE FUNCTION b05_nullcmp(b05_en, b05_en) RETURNS integer LANGUAGE sql AS 'SELECT NULL::integer';
CREATE OPERATOR === (LEFTARG = b05_en, RIGHTARG = b05_en, FUNCTION = b05_nulleq);
CREATE OPERATOR CLASS b05_ocn DEFAULT FOR TYPE b05_en USING btree AS
  OPERATOR 1 < (anyenum, anyenum), OPERATOR 2 <= (anyenum, anyenum), OPERATOR 3 ===,
  OPERATOR 4 >= (anyenum, anyenum), OPERATOR 5 > (anyenum, anyenum), FUNCTION 1 b05_nullcmp(b05_en, b05_en);
CREATE TYPE b05_cn AS (a b05_en);
SELECT (ROW('a'::b05_en)::b05_cn = ROW('a'::b05_en)::b05_cn)::text;
SELECT (ROW('a'::b05_en)::b05_cn <> ROW('a'::b05_en)::b05_cn)::text;
SELECT (ROW('a'::b05_en)::b05_cn < ROW('b'::b05_en)::b05_cn)::text;
SELECT s.r::text FROM (VALUES (ROW('b'::b05_en)::b05_cn), (ROW('a'::b05_en)::b05_cn)) s(r) ORDER BY s.r;
SELECT (ARRAY['a'::b05_en] = ARRAY['a'::b05_en])::text;
SELECT (ARRAY['a'::b05_en] < ARRAY['b'::b05_en])::text;
CREATE FUNCTION b05_nullhash(b05_en) RETURNS integer LANGUAGE sql AS 'SELECT NULL::integer';
CREATE FUNCTION b05_nullhashx(b05_en, bigint) RETURNS bigint LANGUAGE sql AS 'SELECT NULL::bigint';
CREATE OPERATOR CLASS b05_ocnh DEFAULT FOR TYPE b05_en USING hash AS OPERATOR 1 ===, FUNCTION 1 b05_nullhash(b05_en), FUNCTION 2 b05_nullhashx(b05_en, bigint);
SELECT hash_record(ROW('a'::b05_en)::b05_cn);
SELECT hash_array(ARRAY['a'::b05_en]);
SELECT hash_array_extended(ARRAY['a'::b05_en], 0);
DROP OPERATOR CLASS b05_ocnh USING hash;
CREATE FUNCTION b05_h1(b05_en) RETURNS integer LANGUAGE sql AS 'SELECT 7';
CREATE FUNCTION b05_h2(b05_en, bigint) RETURNS bigint LANGUAGE sql AS 'SELECT 7::bigint + $2';
CREATE OPERATOR CLASS b05_och DEFAULT FOR TYPE b05_en USING hash AS OPERATOR 1 ===, FUNCTION 1 b05_h1(b05_en), FUNCTION 2 b05_h2(b05_en, bigint);
SELECT hash_array(ARRAY['a'::b05_en]);
SELECT hash_array_extended(ARRAY['a'::b05_en], 0);
DROP TYPE b05_cn; DROP TYPE b05_en CASCADE;

-- P088: case-insensitive bracket range over a titlecase digraph
SELECT ('ǅ' COLLATE pg_c_utf8 ~* '[ǅ-ǅ]')::text;
SELECT ('Ǆ' COLLATE pg_c_utf8 ~* '[ǅ-ǅ]')::text;
SELECT regexp_replace('ǅ' COLLATE pg_c_utf8, '[ǅ-ǅ]', 'x', 'i');
SELECT ('ǅ' ~* '[ǅ-ǅ]')::text;

-- P053: older hstore versions and update paths
SELECT count(*) FROM pg_available_extension_versions WHERE name = 'hstore';
SELECT count(*) FROM pg_extension_update_paths('hstore');
SELECT count(*) FROM pg_extension_update_paths('citext');
CREATE EXTENSION hstore VERSION '1.4';
SELECT extversion FROM pg_extension WHERE extname = 'hstore';
ALTER EXTENSION hstore UPDATE;
SELECT extversion FROM pg_extension WHERE extname = 'hstore';
SELECT ('"a"=>1'::hstore)['a'];
DROP EXTENSION hstore;
