\set ON_ERROR_STOP on
SET work_mem = '64kB';
CREATE FUNCTION fp7_srf_exc() RETURNS SETOF text LANGUAGE plpgsql AS $$
BEGIN
  BEGIN
    FOR i IN 1..200000 LOOP RETURN NEXT repeat('x', 100); END LOOP;
  EXCEPTION WHEN others THEN RAISE;
  END;
  RETURN;
END $$;
SELECT count(*) FROM fp7_srf_exc();
SELECT count(*) FROM fp7_srf_exc();
CREATE FUNCTION fp7_srf_boundary(first_outside boolean, abort_block boolean)
RETURNS SETOF text LANGUAGE plpgsql AS $$
BEGIN
  IF first_outside THEN RETURN NEXT 'before'; END IF;
  BEGIN
    FOR i IN 1..2000 LOOP RETURN NEXT repeat('x', 100); END LOOP;
    IF abort_block THEN RAISE EXCEPTION 'rollback'; END IF;
  EXCEPTION WHEN raise_exception THEN RETURN NEXT 'caught';
  END;
  RETURN NEXT 'after';
END $$;
SELECT count(*), sum(length(v)) FROM fp7_srf_boundary(false, false) v;
SELECT count(*), sum(length(v)) FROM fp7_srf_boundary(true, false) v;
SELECT count(*), sum(length(v)) FROM fp7_srf_boundary(false, true) v;
SELECT count(*), sum(length(v)) FROM fp7_srf_boundary(true, true) v;
DROP FUNCTION fp7_srf_boundary(boolean, boolean);
DROP FUNCTION fp7_srf_exc();
SELECT 1 AS session_alive;
