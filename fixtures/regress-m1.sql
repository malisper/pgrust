-- pgrust-fast M1/M2 differential-regress corpus.
-- Curated from PostgreSQL 18.3 src/test/regress idioms (select, int4, text,
-- errors, guc, prepare, transactions, limit) down to the M1 surface:
-- constants through the tcop/parse/analyze/plan/execute/printtup spine.
-- Run via scripts/regress-diff.sh (psql -X -q -a on stdin; VERBOSITY verbose
-- captures SQLSTATE + message + cursor; LOCATION lines stripped by the
-- harness). Statements after the '-- REQUIRES: table' marker need CREATE
-- TABLE/INSERT; gate with --skip-tables until DML lands.
\set VERBOSITY verbose
\pset pager off

-- ===== constants & arithmetic [tcop simple-query -> parser -> analyze -> planner Result -> execExpr -> printtup] =====
SELECT 1;
SELECT 1 + 2;
SELECT 2 * 3 - 4;
SELECT 7 / 2;
SELECT 7 % 3;
SELECT -5;
SELECT 1.1 + 2.2;
SELECT 2 ^ 10;
SELECT 'foo' || 'bar';
SELECT NULL;
SELECT NULL + 1;
SELECT 1 = 1, 1 <> 2, 1 < 2, 2 >= 2;
SELECT true AND false, true OR false, NOT true;
SELECT coalesce(NULL, 42);
SELECT greatest(1, 2, 3), least(1, 2, 3);
SELECT CASE WHEN 1 < 2 THEN 'lt' ELSE 'ge' END;
SELECT length('postgres');
SELECT upper('abc'), lower('ABC');

-- ===== column aliases [analyze target-list naming; commit "Add column alias (AS) support"] =====
SELECT 1 AS one;
SELECT 1 one;
SELECT 1 AS "Quoted Alias";
SELECT 1 + 2 AS sum, 3 * 4 AS product;

-- ===== casts [parse_coerce + I/O functions (types-* lanes)] =====
SELECT 'x'::text;
SELECT CAST('42' AS integer);
SELECT 42::bigint;
SELECT 42::int2;
SELECT 1.5::float8;
SELECT '3.14'::numeric;
SELECT 42::text;
SELECT 't'::bool, 'f'::boolean;
SELECT CAST(1.4 AS int4), CAST(1.5 AS int4);
SELECT '  7  '::int4;

-- ===== SET/SHOW round-trips [guc lane: set_config_option / GetConfigOptionByName] =====
SHOW work_mem;
SET work_mem = '8MB';
SHOW work_mem;
RESET work_mem;
SHOW work_mem;
SET datestyle = 'ISO, YMD';
SHOW datestyle;
RESET datestyle;
SET search_path TO public;
SHOW search_path;
BEGIN;
SET LOCAL work_mem = '16MB';
SHOW work_mem;
COMMIT;
SHOW work_mem;

-- ===== EXPLAIN [planner + commands/explain lane; costs pinned by C's cost model] =====
EXPLAIN (COSTS OFF) SELECT 1;
EXPLAIN SELECT 1;
EXPLAIN (COSTS OFF) SELECT 1 + 2 AS three;
EXPLAIN (COSTS OFF) SELECT 1 ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT 1 LIMIT 1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT 1 AS one;

-- ===== EXPLAIN ANALYZE [instrument lane; TIMING/SUMMARY/BUFFERS OFF for determinism] =====
EXPLAIN (ANALYZE, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT 1;
EXPLAIN (ANALYZE, TIMING OFF, SUMMARY OFF, BUFFERS OFF, COSTS OFF) SELECT 1;
EXPLAIN (ANALYZE, TIMING OFF, SUMMARY OFF, BUFFERS OFF, COSTS OFF) SELECT 1 LIMIT 0;

-- ===== transactions [xact lane: BEGIN/COMMIT/ROLLBACK/SAVEPOINT state machine] =====
BEGIN;
SELECT 1;
COMMIT;
BEGIN;
SELECT 2;
ROLLBACK;
BEGIN;
SAVEPOINT sp1;
SELECT 3;
ROLLBACK TO SAVEPOINT sp1;
RELEASE SAVEPOINT sp1;
COMMIT;
COMMIT;
BEGIN;
SELECT 1/0;
SELECT 1;
ROLLBACK;
BEGIN;
SAVEPOINT sp2;
SELECT 1/0;
ROLLBACK TO SAVEPOINT sp2;
SELECT 42;
COMMIT;
ROLLBACK TO SAVEPOINT nope;

-- ===== PREPARE/EXECUTE/DEALLOCATE [commands/prepare + plancache lane] =====
PREPARE regress_m1_p1 AS SELECT 1;
EXECUTE regress_m1_p1;
PREPARE regress_m1_p2(int) AS SELECT $1 + 1;
EXECUTE regress_m1_p2(41);
EXECUTE regress_m1_p2('not_an_int');
PREPARE regress_m1_p1 AS SELECT 2;
DEALLOCATE regress_m1_p2;
EXECUTE regress_m1_p2(1);
DEALLOCATE regress_m1_p1;
DEALLOCATE ALL;

-- ===== ORDER BY / LIMIT on constants [sort/limit executor nodes over Result/Values] =====
SELECT 1 ORDER BY 1;
SELECT 1 LIMIT 1;
SELECT 1 LIMIT 0;
SELECT 1 OFFSET 1;
VALUES (3), (1), (2) ORDER BY 1;
VALUES (3), (1), (2) ORDER BY 1 DESC LIMIT 2;
SELECT * FROM (VALUES (2, 'b'), (1, 'a')) AS v(n, s) ORDER BY n;

-- ===== error surface [elog lane: SQLSTATE + message + cursor position parity] =====
SELECT nonesuch;
SELECT 1 +;
SELECT 1/0;
SELECT 'foo'::int4;
SELECT ''::int4;
SELECT 2147483647::int4 + 1;
SHOW no_such_parameter;
SET work_mem = 'bogus';

-- REQUIRES: table
-- ===== point select / star / WHERE [heapam + seqscan executor; setup needs DML — gate with --skip-tables until INSERT lands] =====
CREATE TABLE regress_m1_t (id int4, val text);
INSERT INTO regress_m1_t VALUES (1, 'one'), (2, 'two'), (3, 'three');
SELECT * FROM regress_m1_t ORDER BY id;
SELECT val FROM regress_m1_t WHERE id = 2;
SELECT id, val FROM regress_m1_t WHERE id > 1 ORDER BY id DESC;
SELECT * FROM regress_m1_t WHERE id = 2 LIMIT 1;
SELECT count(*) FROM regress_m1_t;
SELECT vall FROM regress_m1_t;
SELECT * FROM no_such_table;
DROP TABLE regress_m1_t;

-- ===== jsonb core [adt_jsonb: in/out, JEntry tree, operators, cmp/hash] =====
SELECT '{"b": 2, "a": 1, "a": 3}'::jsonb;
SELECT '{"key": [1, 2.50, null, "x"], "nested": {"deep": [true, false]}}'::jsonb;
SELECT '[1e2, 0.5, "\u00e9\ud83d\ude00", ""]'::jsonb;
SELECT '-1.50'::jsonb::text;
SELECT jsonb_typeof('{"a":1}'::jsonb), jsonb_typeof('[1]'::jsonb), jsonb_typeof('"s"'::jsonb), jsonb_typeof('1'::jsonb), jsonb_typeof('true'::jsonb), jsonb_typeof('null'::jsonb);
SELECT '{"a": {"b": [10, 20, {"c": "d"}]}}'::jsonb -> 'a';
SELECT '{"a": {"b": "c"}}'::jsonb ->> 'a';
SELECT '["x", 1.50, true]'::jsonb -> 1, '["x", 1.50, true]'::jsonb ->> -1;
SELECT '["x"]'::jsonb -> 5, '{"a":1}'::jsonb -> 'zz';
SELECT '{"a": {"b": [10, 20]}}'::jsonb #> '{a,b,1}', '{"a": {"b": [10, 20]}}'::jsonb #>> '{a,b,-2}';
SELECT '{"a":1,"b":{"c":2}}'::jsonb @> '{"b":{"c":2}}'::jsonb, '{"a":1}'::jsonb @> '{"a":2}'::jsonb;
SELECT '[1,[2,3]]'::jsonb @> '[[3,2]]'::jsonb, '[1,2]'::jsonb <@ '[2,1,3]'::jsonb;
SELECT '{"a":1,"b":2}'::jsonb ? 'b', '["a","b"]'::jsonb ? 'c';
SELECT '{"a":1,"b":2}'::jsonb ?| '{x,b}', '{"a":1,"b":2}'::jsonb ?& '{a,b}';
SELECT '{"a":1}'::jsonb = '{"a": 1}'::jsonb, '{"a":1}'::jsonb < '{"a":1,"b":0}'::jsonb;
SELECT j FROM (VALUES (1, '[1]'::jsonb), (2, '{"a":1}'), (3, '"s"'), (4, '1'), (5, 'true'), (6, 'null'), (7, '[]'), (8, '{}')) AS v(i, j) ORDER BY j, i;
SELECT 'nope'::jsonb;
SELECT '"\u0000"'::jsonb;
SELECT '{"a":'::jsonb;

-- ===== adt out-functions: each call's cstring is its own (bugs/batch-01) =====
CREATE TEMP TABLE outfn_probe(a "char", b "char", n1 name, n2 name, l1 pg_lsn, l2 pg_lsn, t boolean, m1 money, m2 money, d date, r real, dp double precision, i2 smallint, i4 integer, i8 bigint, mc1 macaddr, mc2 macaddr, tm time, tz timetz);
INSERT INTO outfn_probe VALUES ('a','b','alpha','beta','0/1','0/2',true,'1','2','2000-01-01',12,34,5,12,11,'00:11:22:33:44:55','aa:bb:cc:dd:ee:ff','01:02:03','04:05:06+02');
SELECT charout(a), charout(b), nameout(n1), nameout(n2), pg_lsn_out(l1), pg_lsn_out(l2), boolout(t), boolout(NOT t) FROM outfn_probe;
SELECT cash_out(m1), cash_out(m2), date_out(d), date_out(d + 1), float4out(r), float8out(dp), int2out(i2), int2out((i2 + 1)::smallint), int4out(i4), int4out(i4 + 22), int8out(i8), int8out(i8 + 11) FROM outfn_probe;
SELECT macaddr_out(mc1), macaddr_out(mc2), time_out(tm), time_out(tm + interval '1 hour'), timetz_out(tz), timetz_out(tz + interval '1 hour'), format('%s %s', int4out(i4), int4out(i4 + 22)), concat(date_out(d), date_out(d + 1)) FROM outfn_probe;
CREATE TYPE outfn_e AS ENUM ('a','b');
CREATE TEMP TABLE outfn_et(x outfn_e, y outfn_e);
INSERT INTO outfn_et VALUES ('a','b');
SELECT enum_out(x), enum_out(y), format('%s %s', anyenum_out(x), anyenum_out(y)) FROM outfn_et;
DROP TABLE outfn_et;
DROP TYPE outfn_e;
DROP TABLE outfn_probe;

-- ===== gin_cmp_tslexeme exposes memcmp's raw difference =====
SELECT gin_cmp_tslexeme('a'::text, 'z'::text), gin_cmp_tslexeme('z'::text, 'a'::text), gin_cmp_tslexeme('ab'::text, 'a'::text);

-- ===== a typmodout returning NULL is an error, not a backend death =====
CREATE DOMAIN ft_null AS integer;
CREATE FUNCTION ft_null_out(integer) RETURNS cstring LANGUAGE internal AS 'pg_last_wal_receive_lsn';
UPDATE pg_type SET typmodout = 'ft_null_out(integer)'::regprocedure WHERE oid = 'ft_null'::regtype;
DO $$ BEGIN PERFORM format_type('ft_null'::regtype, 1); EXCEPTION WHEN OTHERS THEN RAISE NOTICE '%: %', SQLSTATE, regexp_replace(SQLERRM, '[0-9]+', 'N'); END $$;
DROP DOMAIN ft_null;
DROP FUNCTION ft_null_out(integer);

-- ===== aclexplode is value-per-call =====
SELECT aclexplode(array_fill(makeaclitem(0, 10, 'SELECT,INSERT', false), ARRAY[3])) LIMIT 2;
SELECT * FROM aclexplode('{postgres=arwdDxtm/postgres,=r/postgres}'::aclitem[]);

-- ===== array_append as an aggregate transition keeps linear state =====
CREATE AGGREGATE collect_int(integer) (SFUNC = array_append, STYPE = integer[], INITCOND = '{}');
SELECT cardinality(collect_int(i)), collect_int(i) FROM generate_series(1, 5) AS g(i);
SELECT cardinality(collect_int(i)) FROM generate_series(1, 5000) AS g(i);
DROP AGGREGATE collect_int(integer);

-- ===== width_bucket with a comparator that re-enters width_bucket =====
CREATE TYPE wbt AS (v integer);
CREATE FUNCTION wbt_cmp(a wbt, b wbt) RETURNS int LANGUAGE plpgsql AS $$ DECLARE r int; BEGIN IF a.v = 1 THEN r := width_bucket(ROW(0)::wbt, ARRAY[ROW(0)::wbt]); END IF; RETURN CASE WHEN a.v < b.v THEN -1 WHEN a.v > b.v THEN 1 ELSE 0 END; END $$;
CREATE FUNCTION wbt_lt(wbt, wbt) RETURNS bool LANGUAGE sql AS 'SELECT wbt_cmp($1, $2) < 0';
CREATE OPERATOR < (leftarg = wbt, rightarg = wbt, function = wbt_lt);
CREATE OPERATOR CLASS wbt_ops DEFAULT FOR TYPE wbt USING btree AS OPERATOR 1 <, FUNCTION 1 wbt_cmp(wbt, wbt);
SELECT width_bucket(ROW(1)::wbt, ARRAY[ROW(0)::wbt]);
DROP TYPE wbt CASCADE;

-- ===== RI: PK image equality ignores the varlena header form =====
CREATE TABLE ri_p(k text PRIMARY KEY);
ALTER TABLE ri_p ALTER COLUMN k SET STORAGE EXTERNAL;
INSERT INTO ri_p VALUES (repeat('ab', 1200));
CREATE TABLE ri_c(k text REFERENCES ri_p(k) ON UPDATE CASCADE);
INSERT INTO ri_c VALUES (repeat('ab', 1200));
ALTER TABLE ri_p ALTER COLUMN k SET STORAGE EXTENDED;
UPDATE ri_p SET k = k || '';
SELECT ctid FROM ri_c;
DROP TABLE ri_c, ri_p;

-- ===== a LANGUAGE internal alias of a LIKE support function =====
CREATE FUNCTION my_like_support(internal) RETURNS internal LANGUAGE internal AS 'textlike_support';
CREATE FUNCTION my_like(text, text) RETURNS boolean LANGUAGE internal IMMUTABLE STRICT SUPPORT my_like_support AS 'textlike';
CREATE TABLE tlike(s text);
INSERT INTO tlike VALUES ('abcd'), ('xyz');
CREATE INDEX ON tlike(s text_pattern_ops);
SELECT count(*) FROM tlike WHERE my_like(s, 'abc%');
DROP TABLE tlike;
DROP FUNCTION my_like(text, text);
DROP FUNCTION my_like_support(internal);

-- ===== pg_timezone_abbrevs_* read the session state per row =====
SET timezone = 'Europe/Paris';
SELECT pg_catalog.pg_timezone_abbrevs_zone(), set_config('TimeZone', 'UTC', false) LIMIT 2;
RESET timezone;
SELECT * FROM pg_timezone_abbrevs_abbrevs() LIMIT 1;
SELECT count(*) > 0 FROM pg_timezone_abbrevs_abbrevs();

-- ===== adt out-functions: each call's cstring is its own (bugs/batch-02) =====
CREATE TEMP TABLE b02_outfn(m1 macaddr8, m2 macaddr8, i1 inet, i2 inet, c1 cidr, c2 cidr, o1 oid, o2 oid, x1 xid, x2 xid, d1 cid, d2 cid);
INSERT INTO b02_outfn VALUES ('00:11:22:33:44:55:66:77', '88:99:aa:bb:cc:dd:ee:ff', '1.2.3.4', '5.6.7.8', '10.0.0.0/8', '11.0.0.0/8', 11, 22, '5', '6', '7', '8');
SELECT macaddr8_out(m1), macaddr8_out(m2), inet_out(i1), inet_out(i2), cidr_out(c1), cidr_out(c2) FROM b02_outfn;
SELECT oidout(o1), oidout(o2), xidout(x1), xidout(x2), cidout(d1), cidout(d2), format('%s/%s', oidout(o1), oidout(o2)) FROM b02_outfn;
SELECT format('%s/%s', regclassout((4000000000 + g)::oid), regclassout((4100000000 + g)::oid)), regtypeout(23), regtypeout(25), regprocout(1), regprocout(2) FROM generate_series(1, 2) AS g;
DROP TABLE b02_outfn;

-- ===== multirange_constructor2 with zero arguments =====
CREATE FUNCTION b02_mr_zero() RETURNS int4multirange AS 'multirange_constructor2' LANGUAGE internal;
SELECT b02_mr_zero(), isempty(b02_mr_zero());
DROP FUNCTION b02_mr_zero();

-- ===== regex whole match is leftmost-longest without alternation =====
SELECT substring('aaab' FROM 'a*(?:ab)?'), substring('aaab' FROM '^a*(?:ab)?$'), regexp_replace('aaab', 'a*(?:ab)?', 'X'), substring('xaaab' FROM 'a*(?:ab)?'), 'xaaab' ~ '^a*(?:ab)?$';
SELECT regexp_matches('aaab', '(a*)(?:ab)?'), substring('http://www.x.com/y' FROM '^https?://(?:www\.)?([^/]+)/.*$'), substring('abcabc' FROM '[a-c]*(?:cab)?');

-- ===== reg* numeric input is oidin (strtoul base 0) =====
SELECT '010'::regclass::oid, '010'::regtype::oid, '010'::regproc::oid, to_regclass('08') IS NULL, to_regtype('08') IS NULL;
SELECT '08'::regclass;
SELECT '4294967296'::regclass;

-- ===== RI key comparison over a descriptor-owned missing-column default =====
CREATE TABLE b02_p(id int);
INSERT INTO b02_p VALUES (1);
ALTER TABLE b02_p ADD COLUMN k text DEFAULT 'x';
ALTER TABLE b02_p ADD UNIQUE (k);
CREATE TABLE b02_f(k text REFERENCES b02_p(k));
INSERT INTO b02_f VALUES ('x');
UPDATE b02_p SET id = 2;
SELECT * FROM b02_p;
DROP TABLE b02_f, b02_p;

-- ===== pg_get_triggerdef detoasts a compressed tgargs =====
CREATE TABLE b02_t(a int);
CREATE FUNCTION b02_tf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
DO $$ BEGIN EXECUTE format('CREATE TRIGGER b02_trg BEFORE INSERT ON b02_t FOR EACH ROW EXECUTE FUNCTION b02_tf(%L)', repeat('x', 10000)); END $$;
SELECT length(pg_get_triggerdef(oid)), left(pg_get_triggerdef(oid), 80) FROM pg_trigger WHERE tgname = 'b02_trg';
DROP TABLE b02_t;
DROP FUNCTION b02_tf();

-- ===== opclass / collation visibility over more than 64 schemas =====
CREATE TABLE b02_pat(v text);
CREATE INDEX b02_pat_idx ON b02_pat(v text_pattern_ops);
DO $$ BEGIN FOR g IN 1..70 LOOP EXECUTE 'CREATE SCHEMA b02_s' || g; END LOOP; END $$;
CREATE COLLATION b02_s1.b02_c FROM pg_catalog."C";
CREATE VIEW b02_v AS SELECT 'x'::text COLLATE b02_s1.b02_c AS x;
SELECT set_config('search_path', string_agg('b02_s' || g, ',') || ',public', false) IS NOT NULL FROM generate_series(1, 70) g;
SELECT pg_get_indexdef('b02_pat_idx'::regclass);
SELECT pg_get_viewdef('b02_v'::regclass);
RESET search_path;
SELECT pg_get_viewdef('b02_v'::regclass);
DROP VIEW b02_v;
DROP TABLE b02_pat;
DO $$ BEGIN FOR g IN 1..70 LOOP EXECUTE 'DROP SCHEMA b02_s' || g || ' CASCADE'; END LOOP; END $$;

-- ===== get_variable: an attnum past the relation's columns is a catchable error =====
CREATE TABLE b02_expr_source(a int, b int CHECK (b > 0));
CREATE TABLE b02_expr_target(a int);
SELECT pg_get_expr(conbin, 'b02_expr_target'::regclass) FROM pg_constraint WHERE conrelid = 'b02_expr_source'::regclass AND contype = 'c';
DROP TABLE b02_expr_source, b02_expr_target;

-- ===== batch-03: out-function results are per-FmgrInfo (tid, xid8, uuid, timestamp family) =====
CREATE TEMP TABLE b03_outfn(t1 tid, t2 tid, x1 xid8, x2 xid8, u1 uuid, u2 uuid, s1 timestamp, s2 timestamp, z1 timestamptz, z2 timestamptz);
INSERT INTO b03_outfn VALUES ('(1,2)', '(3,4)', '33', '44', '00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', '2000-01-01', '2001-02-03', '2000-01-01 00:00+00', '2001-02-03 00:00+00');
SELECT tidout(t1), tidout(t2), xid8out(x1), xid8out(x2), uuid_out(u1), uuid_out(u2) FROM b03_outfn;
SELECT timestamp_out(s1), timestamp_out(s2), timestamptz_out(z1), timestamptz_out(z2) FROM b03_outfn;
SELECT timestamptypmodout(3), timestamptypmodout(4), timestamptztypmodout(1), timestamptztypmodout(2), intervaltypmodout(2147418115), intervaltypmodout(2147418116);
DROP TABLE b03_outfn;

-- ===== bitcmp is memcmp's raw difference =====
SELECT bitcmp(B'11111111', B'00000000'), bitcmp(B'00000000', B'11111111'), bitcmp(B'0101', B'0100'), varbitcmp(B'11111111', B'00000000'), bitcmp(B'010', B'0100'), B'0101' > B'0100';

-- ===== a codepoint >= 0xF8 after '<?xml' names a processing instruction =====
SELECT XMLPARSE(CONTENT '<?xmlα?>');
SELECT XMLPARSE(CONTENT '<?xmlα ?>');

-- ===== xpath numbers print through float8out (exponent form, extra_float_digits) =====
SELECT (xpath('100000000000000000000', '<r/>'::xml))[1]::text, (xpath('1000000000000000', '<r/>'::xml))[1]::text, (xpath('1 div 3', '<r/>'::xml))[1]::text, (xpath('1 div 0', '<r/>'::xml))[1]::text, (xpath('-1 div 0', '<r/>'::xml))[1]::text;
SET extra_float_digits = 0;
SELECT (xpath('1 div 3', '<r/>'::xml))[1]::text, (xpath('10000000000000000', '<r/>'::xml))[1]::text;
RESET extra_float_digits;

-- ===== XMLTABLE keeps its own libxml error context across a nested XML operation =====
SELECT * FROM XMLTABLE('/r/a' PASSING ('<r><a/><a/></r>'::xml) COLUMNS b boolean PATH 'missing' DEFAULT xml_is_well_formed_document('<' || random()::text), c text PATH 'name()');

-- ===== jsonb_object_agg finalfn is non-destructive on a shared transition state =====
CREATE AGGREGATE b03_joaus(text, anyelement) (sfunc = jsonb_object_agg_unique_strict_transfn, stype = internal, finalfunc = jsonb_object_agg_finalfn, parallel = safe);
SELECT jsonb_object_agg_unique_strict(k, v), b03_joaus(k, v) FROM (VALUES ('a', NULL::int), ('a2', NULL::int), ('b', 1), ('c', 2)) t(k, v);
DROP AGGREGATE b03_joaus(text, anyelement);
