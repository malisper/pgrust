-- SECURITY DEFINER / SET-clause wrapped functions (fmgr_security_definer)
-- driving set-returning SQL functions in the SELECT list: the srf_shutdown
-- hook must reach the INNER FmgrInfo's fcache (C: ShutdownSQLFunction is
-- registered with the fcache pointer, not re-derived from fn_extra).
CREATE ROLE secdef_srf_owner;
CREATE FUNCTION sd_ints() RETURNS SETOF int LANGUAGE sql SECURITY DEFINER
  AS $$ VALUES (1),(2),(3) $$;
CREATE FUNCTION sd_none() RETURNS SETOF int LANGUAGE sql SECURITY DEFINER
  AS $$ SELECT 1 WHERE false $$;
CREATE FUNCTION sd_recs() RETURNS SETOF record LANGUAGE sql SECURITY DEFINER
  AS $$ VALUES (1, 'a'::text), (2, 'b') $$;
CREATE FUNCTION sd_who() RETURNS SETOF text LANGUAGE sql SECURITY DEFINER
  AS $$ SELECT current_user::text UNION ALL SELECT session_user::text $$;
ALTER FUNCTION sd_who() OWNER TO secdef_srf_owner;
CREATE FUNCTION sd_arg(n int) RETURNS SETOF int LANGUAGE sql SECURITY DEFINER
  AS $$ SELECT g FROM generate_series(1, n) g $$;
CREATE FUNCTION sd_nested() RETURNS SETOF int LANGUAGE sql SECURITY DEFINER
  AS $$ SELECT x * 10 FROM sd_ints() x $$;
CREATE FUNCTION sd_scalar(n int) RETURNS int LANGUAGE sql SECURITY DEFINER
  AS $$ SELECT n + 100 $$;
CREATE FUNCTION set_ints() RETURNS SETOF int LANGUAGE sql SET work_mem = '64MB'
  AS $$ VALUES (7),(8) $$;
CREATE FUNCTION set_wm() RETURNS SETOF text LANGUAGE sql SET work_mem = '64MB'
  AS $$ SELECT current_setting('work_mem') UNION ALL SELECT current_setting('work_mem') $$;
CREATE FUNCTION sd_set_ints() RETURNS SETOF int LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog
  AS $$ VALUES (11),(12) $$;
CREATE FUNCTION leak_ints() RETURNS SETOF int LANGUAGE sql LEAKPROOF
  AS $$ VALUES (21),(22) $$;
CREATE FUNCTION sd_pl_ints() RETURNS SETOF int LANGUAGE plpgsql SECURITY DEFINER
  AS $$ BEGIN RETURN NEXT 31; RETURN NEXT 32; END $$;
CREATE FUNCTION sd_pl_who() RETURNS SETOF text LANGUAGE plpgsql SECURITY DEFINER
  AS $$ BEGIN RETURN NEXT current_user::text; RETURN NEXT session_user::text; END $$;
ALTER FUNCTION sd_pl_who() OWNER TO secdef_srf_owner;
-- select-list SRF, ExecutorEnd after >=1 row (the PostgREST panic)
SELECT sd_ints();
SELECT sd_ints() FROM generate_series(1, 2);
SELECT sd_none();
SELECT * FROM sd_recs() AS r(a int, b text);
SELECT sd_recs();
WITH r AS (SELECT sd_ints() x) SELECT json_agg(x) FROM r;
-- suspended execution at ExecutorEnd (the hook has real work to do)
SELECT sd_ints() LIMIT 1;
SELECT sd_ints() LIMIT 2;
SELECT sd_arg(5) LIMIT 3;
SELECT sd_who() LIMIT 1;
SELECT current_user = session_user;
-- security context around lazy suspension/resumption
SELECT sd_who();
SELECT sd_who(), current_user = session_user;
SELECT sd_pl_who();
-- rescan: ProjectSet under a correlated subplan
SELECT i, (SELECT sum(x) FROM (SELECT sd_arg(i) x) s) FROM generate_series(1, 4) i;
SELECT i, (SELECT max(x) FROM (SELECT sd_arg(i) x) s LIMIT 1) FROM generate_series(1, 3) i;
SELECT i, ARRAY(SELECT sd_arg(i)) FROM generate_series(0, 3) i;
-- nested secdef SRF inside secdef SRF
SELECT sd_nested();
SELECT sd_nested() LIMIT 2;
-- FROM path (nodeFunctionScan / materialize)
SELECT * FROM sd_ints();
SELECT * FROM sd_ints() LIMIT 1;
SELECT * FROM sd_none();
SELECT * FROM sd_who();
SELECT * FROM sd_arg(3) a, sd_arg(2) b ORDER BY a, b;
SELECT i, a FROM generate_series(1, 3) i, LATERAL sd_arg(i) a ORDER BY i, a;
SELECT * FROM ROWS FROM (sd_ints(), sd_arg(2)) AS t(a, b);
-- plpgsql SRF with SECURITY DEFINER
SELECT sd_pl_ints();
SELECT sd_pl_ints() LIMIT 1;
SELECT * FROM sd_pl_ints();
-- SET clause (proconfig) and LEAKPROOF
SELECT set_ints();
SELECT set_ints() LIMIT 1;
SELECT set_wm();
SELECT set_wm() LIMIT 1;
SELECT current_setting('work_mem') = '64MB';
SELECT * FROM set_ints();
SELECT sd_set_ints();
SELECT sd_set_ints() LIMIT 1;
SELECT leak_ints();
SELECT leak_ints() LIMIT 1;
-- non-SRF SECURITY DEFINER SQL function: fn_extra reuse across calls
SELECT sd_scalar(i) FROM generate_series(1, 5) i;
SELECT sd_scalar(1), sd_scalar(2), sd_scalar(sd_scalar(3));
SELECT i, sd_scalar(i), sd_ints() FROM generate_series(1, 2) i;
-- cursors: suspended SRF closed early / rescanned
BEGIN;
DECLARE c1 CURSOR FOR SELECT sd_ints();
FETCH 1 FROM c1;
CLOSE c1;
DECLARE c2 SCROLL CURSOR FOR SELECT sd_arg(3);
FETCH 2 FROM c2;
MOVE BACKWARD ALL FROM c2;
FETCH ALL FROM c2;
CLOSE c2;
COMMIT;
-- errors mid-set: cleanup on abort, then the same function works again
CREATE FUNCTION sd_err(n int) RETURNS SETOF int LANGUAGE sql SECURITY DEFINER
  AS $$ SELECT g / (2 - g) FROM generate_series(1, n) g $$;
SELECT sd_err(3);
SELECT sd_err(1);
SELECT sd_err(3) LIMIT 1;
SELECT sd_err(1);
-- prepared statement re-execution keeps the outer FmgrInfo (fn_extra reuse)
PREPARE p1 AS SELECT sd_ints() LIMIT 2;
EXECUTE p1;
EXECUTE p1;
EXECUTE p1;
DEALLOCATE p1;
DROP FUNCTION sd_ints(), sd_none(), sd_recs(), sd_who(), sd_arg(int), sd_nested(), sd_scalar(int);
DROP FUNCTION set_ints(), set_wm(), sd_set_ints(), leak_ints(), sd_pl_ints(), sd_pl_who(), sd_err(int);
DROP ROLE secdef_srf_owner;
