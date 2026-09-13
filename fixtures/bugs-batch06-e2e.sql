-- Differential fixture for the 2026-09-13 workers-io bug batch 06
-- (pg_size_bytes exponent scan, TRUNCATE / REFRESH stats under
-- track_counts = off, range length histogram NULL slot, non-volatile SQL
-- function refusal names the lock strength, SQL-function validator check
-- order, multirange canonicalize sort ties, plpgsql %ROWTYPE tupdesc
-- pinning, RETURN NEXT of an empty record, ts_headline json option order,
-- binary COPY input-function lookup, domain_in over a shell type, WITHOUT
-- OVERLAPS over a system column). Run via scripts/bugs-batch06-e2e.sh
-- against the frozen expected captured from C PostgreSQL 18.6.
\set VERBOSITY verbose
\pset pager off

-- P056: pg_size_bytes exponent scan skips whitespace and a sign (strtol)
SELECT pg_size_bytes('1e 5');
SELECT pg_size_bytes('1e -2 kB');
SELECT pg_size_bytes('1e5'), pg_size_bytes('1e+5'), pg_size_bytes('1 e5');
SELECT pg_size_bytes('1eB');

-- P058: TRUNCATE / REFRESH MATERIALIZED VIEW do not touch stats under track_counts = off
CREATE TABLE b06_tc (k bigint PRIMARY KEY, v bigint);
INSERT INTO b06_tc SELECT g, g * 2 FROM generate_series(1, 4) g;
CREATE MATERIALIZED VIEW b06_mv AS SELECT * FROM b06_tc;
SELECT pg_stat_force_next_flush();
SELECT relname, n_live_tup, n_dead_tup, n_tup_ins FROM pg_stat_user_tables WHERE relname IN ('b06_tc', 'b06_mv') ORDER BY 1;
SET track_counts = off;
TRUNCATE b06_tc;
REFRESH MATERIALIZED VIEW b06_mv;
SELECT pg_stat_force_next_flush();
SELECT relname, n_live_tup, n_dead_tup, n_tup_ins FROM pg_stat_user_tables WHERE relname IN ('b06_tc', 'b06_mv') ORDER BY 1;
RESET track_counts;
TRUNCATE b06_tc;
REFRESH MATERIALIZED VIEW b06_mv;
SELECT pg_stat_force_next_flush();
SELECT relname, n_live_tup, n_dead_tup, n_tup_ins FROM pg_stat_user_tables WHERE relname IN ('b06_tc', 'b06_mv') ORDER BY 1;
DROP MATERIALIZED VIEW b06_mv;
DROP TABLE b06_tc;

-- P062: range length histogram slot is NULL below two non-empty values
CREATE TABLE b06_as (k bigint PRIMARY KEY, v bigint, c1 int4range);
INSERT INTO b06_as (k, v) SELECT g, g * 2 FROM generate_series(1, 9) g;
INSERT INTO b06_as (k, v, c1) VALUES (10, 3, '[1,5)');
ANALYZE b06_as;
SELECT range_length_histogram::text, range_empty_frac FROM pg_stats WHERE tablename = 'b06_as' AND attname = 'c1';
SELECT stakind1, stavalues1 IS NULL AS no_length_histogram FROM pg_statistic WHERE starelid = 'b06_as'::regclass AND staattnum = 3;
INSERT INTO b06_as (k, v, c1) VALUES (11, 3, '[2,9)');
ANALYZE b06_as;
SELECT range_length_histogram::text FROM pg_stats WHERE tablename = 'b06_as' AND attname = 'c1';
DROP TABLE b06_as;

-- P064: the refusal names SELECT FOR <strength>
CREATE TABLE b06_sfb (k bigint PRIMARY KEY, v bigint);
INSERT INTO b06_sfb VALUES (13, 1);
CREATE FUNCTION b06_f_lock(a bigint) RETURNS bigint STABLE LANGUAGE sql AS 'SELECT k FROM b06_sfb WHERE k = a FOR UPDATE';
CREATE FUNCTION b06_f_share(a bigint) RETURNS bigint STABLE LANGUAGE sql AS 'SELECT k FROM b06_sfb WHERE k = a FOR SHARE';
CREATE FUNCTION b06_f_nk(a bigint) RETURNS bigint STABLE LANGUAGE sql AS 'SELECT k FROM b06_sfb WHERE k = a FOR NO KEY UPDATE';
CREATE FUNCTION b06_f_ks(a bigint) RETURNS bigint IMMUTABLE LANGUAGE sql AS 'SELECT k FROM b06_sfb WHERE k = a FOR KEY SHARE';
CREATE FUNCTION b06_f_ins(a bigint) RETURNS bigint STABLE LANGUAGE sql AS 'INSERT INTO b06_sfb VALUES (a) RETURNING k';
CREATE FUNCTION b06_g_lock(a bigint) RETURNS bigint VOLATILE LANGUAGE sql AS 'SELECT k FROM b06_sfb WHERE k = a FOR UPDATE';
SELECT b06_f_lock(13);
SELECT b06_f_share(13);
SELECT b06_f_nk(13);
SELECT b06_f_ks(13);
SELECT b06_f_ins(14);
SELECT b06_g_lock(13);
DROP FUNCTION b06_f_lock(bigint), b06_f_share(bigint), b06_f_nk(bigint), b06_f_ks(bigint), b06_f_ins(bigint), b06_g_lock(bigint);
DROP TABLE b06_sfb;

-- P065: the validator analyses every statement before check_sql_fn_statements
CREATE PROCEDURE b06_p_out(a bigint, OUT r bigint) LANGUAGE plpgsql AS $$ BEGIN r := a + 1; END $$;
CREATE FUNCTION b06_f_bad(a bigint) RETURNS bigint LANGUAGE sql AS 'CALL b06_p_out(1, NULL); SELECT nosuch';
CREATE FUNCTION b06_f_bad2(a bigint) RETURNS bigint LANGUAGE sql BEGIN ATOMIC CALL b06_p_out(1, NULL); SELECT 1; END;
CREATE FUNCTION b06_f_ctl(a bigint) RETURNS bigint LANGUAGE sql AS 'CALL b06_p_out(1, NULL); SELECT 1';
DROP PROCEDURE b06_p_out(bigint, bigint);

-- P066: multirange_canonicalize sorts with qsort_arg (unstable from seven members)
SELECT nummultirange(numrange(1.0,2.0), numrange(3,4), numrange(5,6), numrange(1.00,2.00), numrange(7,8), numrange(9,10), numrange(11,12))::text;
SELECT '{[1.0,2.0),[3,4),[5,6),[1.00,2.00),[7,8),[9,10),[11,12)}'::nummultirange::text;
SELECT ('{[1.0,2.0),[3,4),[5,6)}'::nummultirange + '{[1.00,2.00),[7,8),[9,10),[11,12)}'::nummultirange)::text;
SELECT range_agg(r ORDER BY o)::text FROM (VALUES (1, numrange(1.0,2.0)), (2, numrange(3,4)), (3, numrange(5,6)),
       (4, numrange(1.00,2.00)), (5, numrange(7,8)), (6, numrange(9,10)), (7, numrange(11,12))) s(o, r);
SELECT nummultirange(numrange(1.0,2.0), numrange(3,4), numrange(5,6), numrange(1.00,2.00), numrange(7,8), numrange(9,10))::text;
SELECT nummultirange(numrange(1.0,2.0), numrange(3,4), numrange(5,6), numrange(1.00,2.00), numrange(7,8), numrange(9,10), numrange(11,12), numrange(13,14), numrange(15,16), numrange(1.000,2.000), numrange(17,18), numrange(19,20), numrange(21,22), numrange(23,24), numrange(25,26), numrange(27,28), numrange(29,30), numrange(31,32), numrange(33,34), numrange(35,36))::text;
SELECT int4multirange(int4range(5,6), int4range(1,2), int4range(3,4), int4range(2,3), int4range(9,10), int4range(7,8), int4range(6,7), int4range(20,30), int4range(15,25))::text;

-- P068: a %ROWTYPE variable keeps the tupdesc it was instantiated with
CREATE TABLE b06_rt1 (a int, b int);
CREATE FUNCTION b06_f_add() RETURNS int LANGUAGE plpgsql AS $$
DECLARE r b06_rt1%ROWTYPE;
BEGIN r.a := 0; EXECUTE 'ALTER TABLE b06_rt1 ADD COLUMN extra int'; r := ROW(1, 2, 3)::b06_rt1; RETURN r.extra; END $$;
SELECT b06_f_add();
CREATE TABLE b06_rt2 (a int, b int);
CREATE FUNCTION b06_f_drop() RETURNS int LANGUAGE plpgsql AS $$
DECLARE r b06_rt2%ROWTYPE;
BEGIN r.a := 0; EXECUTE 'ALTER TABLE b06_rt2 DROP COLUMN b'; r := ROW(1)::b06_rt2; RETURN r.b; END $$;
SELECT b06_f_drop();
CREATE TABLE b06_rt3 (a int, b int);
CREATE FUNCTION b06_f3() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r b06_rt3%ROWTYPE; q record;
BEGIN r := ROW(1, 2)::b06_rt3; q := r; RETURN r::text || ' ' || q::text; END $$;
SELECT b06_f3();
ALTER TABLE b06_rt3 ADD COLUMN c int;
SELECT b06_f3();
CREATE FUNCTION b06_f4() RETURNS text LANGUAGE plpgsql AS $$
DECLARE r b06_rt3%ROWTYPE; out text := '';
BEGIN
  FOR r IN SELECT * FROM (VALUES (1,2,3),(4,5,6)) v(a,b,c) LOOP out := out || r.c::text; END LOOP;
  r.a := 9; r := ROW(7,8,9)::b06_rt3; out := out || r.c::text;
  BEGIN DECLARE s b06_rt3%ROWTYPE; BEGIN s.a := 1; out := out || s::text; END; END;
  RETURN out; END $$;
SELECT b06_f4();
SELECT b06_f4();
DROP FUNCTION b06_f_add(), b06_f_drop(), b06_f3(), b06_f4();
DROP TABLE b06_rt1, b06_rt2, b06_rt3;

-- P069: RETURN NEXT of a never-assigned record instantiates it as a row of NULLs
CREATE TYPE b06_rct AS (a int, b text);
CREATE FUNCTION b06_f_rn() RETURNS SETOF b06_rct LANGUAGE plpgsql AS $$
DECLARE r b06_rct; s b06_rct;
BEGIN RETURN NEXT r; RETURN NEXT ROW(1, coalesce(r::text, 'NULL'))::b06_rct; r.a := 5; RETURN NEXT r;
      RETURN NEXT ROW(2, coalesce(s::text, 'NULL'))::b06_rct; RETURN; END $$;
SELECT x::text FROM b06_f_rn() AS x;
DROP FUNCTION b06_f_rn();
DROP TYPE b06_rct;

-- P071: ts_headline over json / jsonb parses the options before the HEADLINE method check
CREATE TEXT SEARCH PARSER b06_prs_nohl (START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype);
CREATE TEXT SEARCH CONFIGURATION b06_cfg_nohl (PARSER = b06_prs_nohl);
SELECT ts_headline('b06_cfg_nohl', '["x"]'::json, 'x'::tsquery, 'bogus');
SELECT ts_headline('b06_cfg_nohl', '["x"]'::jsonb, 'x'::tsquery, 'bogus');
SELECT ts_headline('b06_cfg_nohl', '["x"]'::json, 'x'::tsquery);
SELECT ts_headline('b06_cfg_nohl', 'a fat cat', 'cat'::tsquery);
SELECT ts_headline('b06_cfg_nohl', 'a fat cat', 'cat'::tsquery, 'bogus');
CREATE TEXT SEARCH PARSER b06_prs_hl (START = prsd_start, GETTOKEN = prsd_nexttoken, END = prsd_end, LEXTYPES = prsd_lextype, HEADLINE = prsd_headline);
CREATE TEXT SEARCH CONFIGURATION b06_cfg_hl (PARSER = b06_prs_hl);
SELECT ts_headline('b06_cfg_hl', '["x"]'::json, 'x'::tsquery, 'bogus');
SELECT ts_headline('b06_cfg_hl', '["x y"]'::jsonb, 'x'::tsquery, 'StartSel=<, StopSel=>');
DROP TEXT SEARCH CONFIGURATION b06_cfg_nohl, b06_cfg_hl;
DROP TEXT SEARCH PARSER b06_prs_nohl, b06_prs_hl;

-- P074: BeginCopyFrom fetches the input function of every non-dropped column
CREATE TABLE b06_cb (k bigint PRIMARY KEY, v bigint, c4 aclitem DEFAULT '=r/pg_read_all_data');
COPY b06_cb (k, v) FROM STDIN (FORMAT binary);
COPY b06_cb (v, k) FROM STDIN;
2	1
\.
SELECT * FROM b06_cb;
ALTER TABLE b06_cb DROP COLUMN c4;
COPY b06_cb (v, k) FROM STDIN;
4	3
\.
SELECT * FROM b06_cb ORDER BY 1;
DROP TABLE b06_cb;

-- P082: domain_in over a shell type is the typcache shell refusal
CREATE TYPE b06_sh;
SELECT pg_typeof(domain_in('1'::cstring, (SELECT oid FROM pg_type WHERE typname = 'b06_sh' AND typnamespace = 'public'::regnamespace), -1))::text;
SELECT pg_typeof(domain_in('1'::cstring, 'int4'::regtype::oid, -1))::text;
SELECT domain_in('1'::cstring, 0, -1);
DROP TYPE b06_sh;

-- P087: WITHOUT OVERLAPS over a system column on the ALTER TABLE path
CREATE TABLE b06_wo (k bigint PRIMARY KEY, v bigint, r int4range);
ALTER TABLE b06_wo ADD CONSTRAINT b06_c UNIQUE (k, ctid WITHOUT OVERLAPS);
ALTER TABLE b06_wo ADD CONSTRAINT b06_c2 UNIQUE (k, v WITHOUT OVERLAPS);
ALTER TABLE b06_wo ADD CONSTRAINT b06_c3 UNIQUE (ctid, r WITHOUT OVERLAPS);
ALTER TABLE b06_wo ADD CONSTRAINT b06_c4 UNIQUE (k, r WITHOUT OVERLAPS);
CREATE TABLE b06_wo2 (k bigint, v int4range, UNIQUE (k, ctid WITHOUT OVERLAPS));
DROP TABLE b06_wo;
