-- Differential fixture for the 2026-09-13 bug batch 185 (pl/plpgsql:
-- non-atomic argument detoasting, NULL composite-domain arguments,
-- declared-rowtype result transfer, CALL frames in PG_CONTEXT, simple-
-- expression checks before generic planning, per-transaction simple-
-- expression state, fixed grammar tokens, datatype warning positions,
-- single-token peek, block line numbers, RETURN NEXT row scratch).
-- Run via scripts/bugs-batch185-plpgsql-e2e.sh against the frozen expected
-- captured from C PostgreSQL 18.6.
\set VERBOSITY verbose
\pset pager off

-- fp-pl-plpgsql-pl_exec-p1#1 / pl_exec-p4#2: a non-atomic procedure stores IN arguments detoasted
CREATE TABLE b185_src(id int, t text);
ALTER TABLE b185_src ALTER COLUMN t SET STORAGE EXTERNAL;
INSERT INTO b185_src SELECT 1, string_agg(md5(i::text), '') FROM generate_series(1, 200) i;
CREATE FUNCTION b185_srcval() RETURNS text LANGUAGE sql AS 'SELECT t FROM b185_src WHERE id = 1';
CREATE PROCEDURE b185_p(x text) LANGUAGE plpgsql AS $$
BEGIN RAISE NOTICE 'arg external: %', pg_column_toast_chunk_id(x) IS NOT NULL; END $$;
CALL b185_p(b185_srcval());
DO $$ BEGIN CALL b185_p(b185_srcval()); END $$;
BEGIN;
CALL b185_p(b185_srcval());
COMMIT;
CREATE PROCEDURE b185_p2(x text) LANGUAGE plpgsql AS $$
BEGIN DELETE FROM b185_src; COMMIT; RAISE NOTICE 'after commit: %', length(x); END $$;
CALL b185_p2(b185_srcval());
DROP PROCEDURE b185_p, b185_p2; DROP FUNCTION b185_srcval; DROP TABLE b185_src;

-- fp-pl-plpgsql-pl_exec-p1#2: a NULL composite-domain argument is domain-checked at entry
CREATE TYPE b185_ct AS (a int);
CREATE DOMAIN b185_dct AS b185_ct CHECK (VALUE IS NOT NULL);
CREATE FUNCTION b185_f(x b185_dct) RETURNS text LANGUAGE plpgsql AS $$
BEGIN RETURN 'got ' || coalesce((x).a::text, 'null'); END $$;
SELECT b185_f((SELECT NULL::b185_dct WHERE false));
SELECT b185_f(ROW(1)::b185_ct);
CREATE DOMAIN b185_dct2 AS b185_ct;
CREATE FUNCTION b185_f2(x b185_dct2) RETURNS text LANGUAGE plpgsql AS $$
BEGIN RETURN 'x is null: ' || (x IS NULL)::text; END $$;
SELECT b185_f2((SELECT NULL::b185_dct2 WHERE false));
DROP FUNCTION b185_f, b185_f2; DROP DOMAIN b185_dct, b185_dct2;

-- fp-pl-plpgsql-pl_exec-p1#4: a result of the declared composite-domain type is not re-checked
CREATE FUNCTION b185_chk(x b185_ct) RETURNS bool LANGUAGE plpgsql AS $$
BEGIN RAISE NOTICE 'check called a=%', x.a; RETURN (x).a > 0; END $$;
CREATE DOMAIN b185_dchk AS b185_ct CHECK (b185_chk(VALUE));
CREATE FUNCTION b185_r1() RETURNS b185_dchk LANGUAGE plpgsql AS $$
DECLARE d b185_dchk; BEGIN d := ROW(1)::b185_ct; RAISE NOTICE 'assigned'; RETURN d; END $$;
SELECT b185_r1();
CREATE FUNCTION b185_r2() RETURNS b185_dchk LANGUAGE plpgsql AS $$
BEGIN RETURN ROW(2)::b185_ct::b185_dchk; END $$;
SELECT b185_r2();
CREATE FUNCTION b185_r3() RETURNS b185_dchk LANGUAGE plpgsql AS $$
BEGIN RETURN ROW(3)::b185_ct; END $$;
SELECT b185_r3();
CREATE FUNCTION b185_r4() RETURNS b185_dchk LANGUAGE plpgsql AS $$
BEGIN RETURN ROW(-4)::b185_ct; END $$;
SELECT b185_r4();
DROP FUNCTION b185_r1, b185_r2, b185_r3, b185_r4; DROP DOMAIN b185_dchk; DROP FUNCTION b185_chk;
DROP TYPE b185_ct;

-- fp-pl-plpgsql-pl_exec-p1#5: the CALL statement is a PG_CONTEXT frame
CREATE PROCEDURE b185_ctx() LANGUAGE plpgsql AS $$
DECLARE c text; BEGIN GET DIAGNOSTICS c = PG_CONTEXT; RAISE NOTICE '%', c; END $$;
DO $$ BEGIN CALL b185_ctx(); END $$;
CALL b185_ctx();
DROP PROCEDURE b185_ctx;

-- fp-pl-plpgsql-pl_exec-p4#1: a non-simple expression is never generic-planned
DO $$ DECLARE p int := 1; x int;
BEGIN x := (SELECT CASE WHEN p > 0 THEN 1 ELSE 1/0 END); RAISE NOTICE 'sublink: %', x; END $$;
DO $$ DECLARE p int := 1; x int;
BEGIN x := (SELECT CASE WHEN p > 0 THEN 1 ELSE 1/0 END FROM generate_series(1, 1)); RAISE NOTICE 'from: %', x; END $$;
DO $$ DECLARE p int := 1; x int;
BEGIN x := CASE WHEN p > 0 THEN 1 ELSE 1/0 END; RAISE NOTICE 'simple: %', x; END $$;
CREATE FUNCTION b185_w() RETURNS int LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN RAISE WARNING 'warn from b185_w'; RETURN 1; END $$;
DO $$ DECLARE x int; BEGIN x := b185_w() + (SELECT 1); RAISE NOTICE '%', x; END $$;
DROP FUNCTION b185_w;

-- fp-pl-plpgsql-pl_exec-p4#3 / pl_handler#1: simple-expression state is rebuilt per transaction
CREATE FUNCTION b185_g() RETURNS int LANGUAGE plpgsql AS 'BEGIN RETURN 1; END';
CREATE FUNCTION b185_call_g() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN b185_g(); END $$;
SET track_functions = 'none';
SELECT b185_call_g();
SELECT b185_call_g();
SET track_functions = 'all';
SELECT b185_call_g();
SELECT b185_call_g();
SELECT pg_stat_force_next_flush();
SELECT funcname, calls FROM pg_stat_user_functions WHERE funcname IN ('b185_g', 'b185_call_g') ORDER BY 1;
RESET track_functions;
DROP FUNCTION b185_call_g, b185_g;

-- fp-pl-plpgsql-pl_gram-p1#2: fixed grammar tokens are not matched by same-named variables
DO $$ DECLARE stacked int; c text;
BEGIN BEGIN RAISE EXCEPTION 'x'; EXCEPTION WHEN OTHERS THEN GET STACKED DIAGNOSTICS c = MESSAGE_TEXT; END; END $$;
DO $$ DECLARE current int; c text; BEGIN GET CURRENT DIAGNOSTICS c = PG_CONTEXT; END $$;
DO $$ DECLARE slice int; a int[]; x int[];
BEGIN a := ARRAY[[1,2],[3,4]]; FOREACH x SLICE 1 IN ARRAY a LOOP RAISE NOTICE '%', x; END LOOP; END $$;
CREATE PROCEDURE b185_chain() LANGUAGE plpgsql AS $$ DECLARE chain int; BEGIN COMMIT AND CHAIN; END $$;
CREATE PROCEDURE b185_nochain(no int) LANGUAGE plpgsql AS $$ BEGIN COMMIT AND NO CHAIN; END $$;
CREATE FUNCTION b185_vc(error int) RETURNS int LANGUAGE plpgsql AS $$ #variable_conflict error
BEGIN RETURN 1; END $$;
CREATE FUNCTION b185_vc2(use_column int) RETURNS int LANGUAGE plpgsql AS $$ #variable_conflict use_column
BEGIN RETURN 1; END $$;
CREATE FUNCTION b185_vc3() RETURNS int LANGUAGE plpgsql AS $$ #variable_conflict "error"
BEGIN RETURN 1; END $$;
DO $$ DECLARE c text; BEGIN GET CURRENT DIAGNOSTICS c = PG_CONTEXT; RAISE NOTICE '%', c; END $$;

-- fp-pl-plpgsql-pl_gram-p2#1: datatype warnings carry the declaration's position
DO $$ DECLARE x timestamp(9); BEGIN x := now(); END $$;
CREATE FUNCTION b185_ts() RETURNS int LANGUAGE plpgsql AS $$ DECLARE y timestamp(9); BEGIN RETURN 1; END $$;
SET check_function_bodies = off;
CREATE FUNCTION b185_ts2() RETURNS int LANGUAGE plpgsql AS $$ DECLARE y time(8); BEGIN RETURN 1; END $$;
RESET check_function_bodies;
SELECT b185_ts2();
DROP FUNCTION b185_ts, b185_ts2;

-- fp-pl-plpgsql-pl_scanner#2: the cursor-variable peek reads one raw token
DO $$ DECLARE i int; x int; BEGIN FETCH i bogus 'oops; END $$;
DO $$ DECLARE i int; x int; BEGIN OPEN i bogus 'oops; END $$;
DO $$ DECLARE i int; x int; BEGIN CLOSE i bogus 'oops; END $$;
DO $$ DECLARE i int; x int; BEGIN FETCH i INTO x; END $$;

-- fp-pl-plpgsql-pl_gram-p1#3: the compile-context line of an end-label error is the block's BEGIN
SET check_function_bodies = off;
CREATE FUNCTION b185_lbl() RETURNS int LANGUAGE plpgsql AS $$
DECLARE x int;
BEGIN
  x := 1;
  x := 2;
  <<lbl>>
  BEGIN
    x := 3;
  END other;
  RETURN x;
END $$;
SELECT b185_lbl();
CREATE FUNCTION b185_lbl2() RETURNS int LANGUAGE plpgsql AS $$
BEGIN
  PERFORM 1;
  PERFORM 2;
  BEGIN
    PERFORM 3;
  END other;
  RETURN 1;
END $$;
SELECT b185_lbl2();
RESET check_function_bodies;
DROP FUNCTION b185_lbl, b185_lbl2;

-- fp-pl-plpgsql-pl_exec-p2#3: RETURN NEXT of multiple OUT parameters builds its tuple in eval scratch
CREATE FUNCTION b185_srf(OUT a text, OUT b text) RETURNS SETOF record LANGUAGE plpgsql AS $$
DECLARE i int; m bigint;
BEGIN
  FOR i IN 1..20000 LOOP
    a := repeat('x', 1000); b := repeat('y', 1000);
    RETURN NEXT;
  END LOOP;
  SELECT sum(used_bytes) INTO m FROM pg_backend_memory_contexts;
  RAISE NOTICE 'under 8MB: %', m < 8 * 1048576;
  a := NULL; b := NULL;
  RETURN;
END $$;
SELECT count(*) FROM b185_srf();
DROP FUNCTION b185_srf;

-- fp-executor-spi-p2#3: planning-time warnings of a simple expression carry the assignment context
\set SHOW_CONTEXT always
CREATE FUNCTION b185_w2() RETURNS int LANGUAGE plpgsql IMMUTABLE AS $$
BEGIN RAISE WARNING 'warn from b185_w2'; RETURN 1; END $$;
DO $$ DECLARE x int; BEGIN x := b185_w2(); RAISE NOTICE '%', x; END $$;
DO $$ DECLARE x int; BEGIN x := b185_w2() + (SELECT 1); RAISE NOTICE '%', x; END $$;
DO $$ DECLARE x timestamp(9); BEGIN x := now(); END $$;
\set SHOW_CONTEXT errors
DROP FUNCTION b185_w2;
