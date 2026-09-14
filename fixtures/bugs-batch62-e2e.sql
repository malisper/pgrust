-- bugs/batch-62-backend-optimizer: planner divergences vs C 18.6.
-- Expected captured from C: scripts/regress-diff.sh --capture fixtures/bugs-batch62-e2e.expected \
--   --sql fixtures/bugs-batch62-e2e.sql "$PGINSTALL/postgres"
\set VERBOSITY verbose

-- fp-adt-b2#1: the geometric estimators ignore their arguments and return
-- constants; a LANGUAGE internal alias call returns them too.
CREATE FUNCTION b62_areasel() RETURNS double precision LANGUAGE internal AS 'areasel';
CREATE FUNCTION b62_areajoinsel() RETURNS double precision LANGUAGE internal AS 'areajoinsel';
CREATE FUNCTION b62_positionsel() RETURNS double precision LANGUAGE internal AS 'positionsel';
CREATE FUNCTION b62_positionjoinsel() RETURNS double precision LANGUAGE internal AS 'positionjoinsel';
CREATE FUNCTION b62_contsel() RETURNS double precision LANGUAGE internal AS 'contsel';
CREATE FUNCTION b62_contjoinsel() RETURNS double precision LANGUAGE internal AS 'contjoinsel';
SELECT b62_areasel(), b62_areajoinsel(), b62_positionsel(), b62_positionjoinsel(), b62_contsel(), b62_contjoinsel();

-- fp-adt-selfuncs-p1#1 / fp-adt-selfuncs-p2#1: a comparison operator whose
-- function returns NULL is a non-match for var_eq_const, mcv_selectivity,
-- histogram_selectivity, eqjoinsel_inner and eqjoinsel_semi, never an error.
CREATE TABLE b62_t3(x int);
INSERT INTO b62_t3 SELECT 1 FROM generate_series(1,1000);
INSERT INTO b62_t3 SELECT g FROM generate_series(1,50) g;
ANALYZE b62_t3;
CREATE FUNCTION b62_nullcmp(int,int) RETURNS boolean LANGUAGE plpgsql IMMUTABLE STRICT AS $$ BEGIN RETURN NULL; END $$;
CREATE OPERATOR === (LEFTARG=int, RIGHTARG=int, FUNCTION=b62_nullcmp, RESTRICT=eqsel, JOIN=eqjoinsel);
CREATE OPERATOR <<< (LEFTARG=int, RIGHTARG=int, FUNCTION=b62_nullcmp, RESTRICT=scalarltsel, JOIN=scalarltjoinsel);
EXPLAIN (COSTS OFF) SELECT * FROM b62_t3 WHERE x === 1;
SELECT count(*) FROM b62_t3 WHERE x === 1;
EXPLAIN (COSTS OFF) SELECT * FROM b62_t3 WHERE x <<< 1;
CREATE TABLE b62_a4(x int);
CREATE TABLE b62_b4(x int);
INSERT INTO b62_a4 SELECT g % 10 FROM generate_series(1,1000) g;
INSERT INTO b62_b4 SELECT g % 7 FROM generate_series(1,1000) g;
ANALYZE b62_a4;
ANALYZE b62_b4;
EXPLAIN (COSTS OFF) SELECT * FROM b62_a4 JOIN b62_b4 ON b62_a4.x === b62_b4.x;
EXPLAIN (COSTS OFF) SELECT * FROM b62_a4 WHERE EXISTS (SELECT FROM b62_b4 WHERE b62_a4.x === b62_b4.x);

-- fp-path-pathkeys#1: an opfamily without its same-type equality operator is
-- an ordinary XX000 error from make_pathkey_from_sortinfo (the opfamily OID
-- differs per cluster, so it is masked).
CREATE OPERATOR CLASS b62_incomplete_ops FOR TYPE integer USING btree AS FUNCTION 1 btint4cmp(integer, integer);
CREATE TABLE b62_pk(a integer);
CREATE INDEX b62_pk_idx ON b62_pk USING btree(a b62_incomplete_ops);
DO $$ BEGIN
  EXECUTE 'EXPLAIN SELECT a FROM b62_pk ORDER BY a';
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE '% %', SQLSTATE, regexp_replace(SQLERRM, 'opfamily \d+', 'opfamily <oid>');
END $$;

-- fp-plan-createplan-p1#1: order_qual_clauses' insertion sort tolerates NaN
-- costs (COST 1e39 x cpu_operator_cost 0) and orders them as C does.
CREATE FUNCTION b62_cost_probe(boolean) RETURNS boolean LANGUAGE plpgsql VOLATILE COST 1e39 AS $$ BEGIN RETURN $1; END $$;
CREATE TABLE b62_t23(a int);
INSERT INTO b62_t23 VALUES (1),(2);
SET cpu_operator_cost = 0;
EXPLAIN (COSTS OFF) SELECT 1 HAVING b62_cost_probe(true) AND b62_cost_probe(false);
SELECT 1 HAVING b62_cost_probe(true) AND b62_cost_probe(false);
EXPLAIN (COSTS OFF) SELECT * FROM b62_t23 WHERE b62_cost_probe(a=1) AND b62_cost_probe(a=2);
SELECT * FROM b62_t23 WHERE b62_cost_probe(a=1) AND b62_cost_probe(a=2);
EXPLAIN (COSTS OFF) SELECT a FROM b62_t23 GROUP BY a HAVING b62_cost_probe(a=1) AND b62_cost_probe(a=2);
RESET cpu_operator_cost;

-- fp-index-indexam#1: planning inside an index expression during REINDEX
-- refuses the index being rebuilt (index_can_return's RELATION_CHECKS).
CREATE TABLE b62_t11(a int);
INSERT INTO b62_t11 SELECT g FROM generate_series(1,10) g;
CREATE FUNCTION b62_f11(int) RETURNS int LANGUAGE plpgsql IMMUTABLE AS $$ DECLARE n bigint; BEGIN EXECUTE 'SELECT count(*) FROM public.b62_t11' INTO n; RETURN $1; END $$;
CREATE INDEX b62_t11_idx ON b62_t11(b62_f11(a));
SET enable_indexscan = off;
SET enable_indexonlyscan = off;
SET enable_bitmapscan = off;
DO $$ BEGIN
  REINDEX INDEX b62_t11_idx;
EXCEPTION WHEN OTHERS THEN
  RAISE NOTICE '% %', SQLSTATE, SQLERRM;
END $$;
RESET enable_indexscan;
RESET enable_indexonlyscan;
RESET enable_bitmapscan;
