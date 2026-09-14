-- bugs/batch-212-backend-utils-cache: cache fixes, expected captured from C 18.6.
\set VERBOSITY verbose
-- fp-cache-lsyscache-p2#1: get_typdefault converts a plain-literal typdefault
-- through InputFunctionCall, which raises XX000 when the input function
-- returns NULL (lsyscache.c:2793, fmgr.c:1558). pg_stat_get_function_calls
-- returns NULL for every OID while track_functions is none.
SET track_functions = none;
CREATE TYPE b212_t4;
CREATE FUNCTION b212_t4_in(cstring) RETURNS b212_t4 LANGUAGE internal IMMUTABLE STRICT AS 'int4in';
CREATE FUNCTION b212_t4_out(b212_t4) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'int4out';
CREATE TYPE b212_t4 (INPUT = b212_t4_in, OUTPUT = b212_t4_out, INTERNALLENGTH = 4, PASSEDBYVALUE, DEFAULT = '42');
CREATE OR REPLACE FUNCTION b212_t4_in(cstring) RETURNS b212_t4 LANGUAGE internal IMMUTABLE STRICT AS 'pg_stat_get_function_calls';
SELECT b212_t4_in('1') IS NULL AS input_returns_null;
CREATE TABLE b212_tt4 (c b212_t4);
DO $$ BEGIN INSERT INTO b212_tt4 DEFAULT VALUES; EXCEPTION WHEN OTHERS THEN RAISE NOTICE '% %', SQLSTATE, replace(SQLERRM, 'b212_t4_in'::regproc::oid::text, '<oid>'); END $$;
SELECT count(*) FROM b212_tt4;
CREATE TYPE b212_tv;
CREATE FUNCTION b212_tv_in(cstring) RETURNS b212_tv LANGUAGE internal IMMUTABLE STRICT AS 'textin';
CREATE FUNCTION b212_tv_out(b212_tv) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'textout';
CREATE TYPE b212_tv (INPUT = b212_tv_in, OUTPUT = b212_tv_out, INTERNALLENGTH = VARIABLE, DEFAULT = 'anything');
CREATE OR REPLACE FUNCTION b212_tv_in(cstring) RETURNS b212_tv LANGUAGE internal IMMUTABLE STRICT AS 'pg_stat_get_function_calls';
CREATE TABLE b212_ttv (c b212_tv);
DO $$ BEGIN INSERT INTO b212_ttv DEFAULT VALUES; EXCEPTION WHEN OTHERS THEN RAISE NOTICE '% %', SQLSTATE, replace(SQLERRM, 'b212_tv_in'::regproc::oid::text, '<oid>'); END $$;
SELECT count(*) FROM b212_ttv;
CREATE TYPE b212_t16;
CREATE FUNCTION b212_t16_in(cstring) RETURNS b212_t16 LANGUAGE internal IMMUTABLE STRICT AS 'int4in';
CREATE FUNCTION b212_t16_out(b212_t16) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'int4out';
CREATE TYPE b212_t16 (INPUT = b212_t16_in, OUTPUT = b212_t16_out, INTERNALLENGTH = 16, DEFAULT = '42');
CREATE OR REPLACE FUNCTION b212_t16_in(cstring) RETURNS b212_t16 LANGUAGE internal IMMUTABLE STRICT AS 'pg_stat_get_function_calls';
CREATE TABLE b212_tt16 (c b212_t16);
DO $$ BEGIN INSERT INTO b212_tt16 DEFAULT VALUES; EXCEPTION WHEN OTHERS THEN RAISE NOTICE '% %', SQLSTATE, replace(SQLERRM, 'b212_t16_in'::regproc::oid::text, '<oid>'); END $$;
SELECT count(*) FROM b212_tt16;
-- A plain-literal default whose input function returns a value still works.
CREATE TYPE b212_w;
CREATE FUNCTION b212_w_in(cstring) RETURNS b212_w LANGUAGE internal IMMUTABLE STRICT AS 'int4in';
CREATE FUNCTION b212_w_out(b212_w) RETURNS cstring LANGUAGE internal IMMUTABLE STRICT AS 'int4out';
CREATE TYPE b212_w (INPUT = b212_w_in, OUTPUT = b212_w_out, INTERNALLENGTH = 4, PASSEDBYVALUE, DEFAULT = '42');
CREATE TABLE b212_ok (c int DEFAULT 7, d b212_w);
INSERT INTO b212_ok (c) VALUES (1);
SELECT c, d FROM b212_ok;
-- fp-cache-relcache-p1#1: after the last trigger is dropped (relhastriggers
-- stays true until vacuum) the empty trigger descriptor is cached in the
-- relcache entry; pg_trigger is not rescanned per statement
-- (relcache.c:1260, trigger.c:1993).
CREATE TABLE b212_tr (a int);
CREATE FUNCTION b212_trf() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$;
CREATE TRIGGER b212_tg BEFORE INSERT ON b212_tr FOR EACH ROW EXECUTE FUNCTION b212_trf();
DROP TRIGGER b212_tg ON b212_tr;
INSERT INTO b212_tr VALUES (1);
SELECT pg_stat_force_next_flush();
SELECT idx_scan FROM pg_stat_all_indexes WHERE indexrelname = 'pg_trigger_tgrelid_tgname_index' \gset b212_
INSERT INTO b212_tr VALUES (2);
INSERT INTO b212_tr VALUES (3);
INSERT INTO b212_tr VALUES (4);
SELECT pg_stat_force_next_flush();
SELECT idx_scan - :b212_idx_scan AS pg_trigger_rescans FROM pg_stat_all_indexes WHERE indexrelname = 'pg_trigger_tgrelid_tgname_index';
SELECT relhastriggers FROM pg_class WHERE relname = 'b212_tr';
-- A trigger created afterwards is seen (relcache invalidation drops the cached empty descriptor).
CREATE FUNCTION b212_trf2() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE NOTICE 'b212 trigger fired'; RETURN NEW; END $$;
CREATE TRIGGER b212_tg2 BEFORE INSERT ON b212_tr FOR EACH ROW EXECUTE FUNCTION b212_trf2();
INSERT INTO b212_tr VALUES (5);
SELECT count(*) FROM b212_tr;
