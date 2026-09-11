-- walker-arms witness corpus: one SQL witness per allowlisted row of
-- crates/_support/seams_init/tests/lint-walker-arms.allow (ELSEWHERE / NOOP /
-- UNREACHABLE classes). Each section is headed by the allowlist key it
-- witnesses; the SQL is what makes C take the arm (or proves the arm cannot
-- be reached from SQL). Expected output is captured from stock PostgreSQL
-- 18.6 (scripts/walker-arms-witness-e2e.sh) and both binaries must match it
-- byte-for-byte. Keep it deterministic: no timing, no worker counts.
\set VERBOSITY verbose
SET client_min_messages = warning;
SET max_parallel_workers_per_gather = 0;
SET jit = off;

-- ===== common fixtures =====
CREATE TABLE wa (id int PRIMARY KEY, x int, b boolean, v varchar(10), t text);
INSERT INTO wa SELECT i, i % 10, i % 2 = 0, 'v' || (i % 5), 't' || i FROM generate_series(1, 1000) i;
CREATE INDEX wa_x ON wa (x);
CREATE TABLE wb (id int PRIMARY KEY, y int);
INSERT INTO wb SELECT i, i % 7 FROM generate_series(1, 500) i;
VACUUM ANALYZE wa;
VACUUM ANALYZE wb;

-- ===== src/backend/utils/adt/ruleutils.c:get_name_for_var_field|RTEKind =====
-- Field selection on a RECORD-typed Var: RTE_SUBQUERY / RTE_JOIN / RTE_CTE are
-- the live arms; RTE_VALUES / RTE_FUNCTION / RTE_GROUP columns of type RECORD
-- are rejected by the parser before any deparse.
CREATE VIEW wv_field1 AS SELECT (s.r).f1 AS f FROM (SELECT ROW(1, 2) AS r) s;
SELECT pg_get_viewdef('wv_field1'::regclass);
CREATE VIEW wv_field2 AS SELECT (j.r).f2 AS f FROM ((SELECT ROW(1, 2) AS r, 1 AS k) a JOIN (SELECT 1 AS k) b USING (k)) j;
SELECT pg_get_viewdef('wv_field2'::regclass);
CREATE VIEW wv_field3 AS WITH c AS (SELECT ROW(3, 4) AS r) SELECT (c.r).f1 AS f FROM c;
SELECT pg_get_viewdef('wv_field3'::regclass);
CREATE VIEW wv_field4 AS SELECT (s.r).f1 AS f FROM (SELECT ROW(id, x) AS r FROM wa) s GROUP BY s.r;
SELECT pg_get_viewdef('wv_field4'::regclass);
SELECT * FROM wv_field4 ORDER BY 1 LIMIT 2;
CREATE VIEW wv_field_values AS SELECT (v.r).f1 FROM (VALUES (ROW(1, 2))) v(r);
CREATE VIEW wv_field_func AS SELECT (f.r).f1 FROM (SELECT ROW(1, 2) AS r) f, LATERAL (SELECT f.r AS q) g;
SELECT pg_get_viewdef('wv_field_func'::regclass);

-- ===== src/backend/optimizer/prep/prepjointree.c:replace_vars_in_jointree|RTEKind =====
-- lateral references into a pulled-up subquery; the missing arms are RTE kinds
-- that can never be flagged LATERAL.
SELECT * FROM (SELECT id, x FROM wa WHERE id < 3) s, LATERAL (SELECT s.x + 1 AS y) t ORDER BY 1;
SELECT * FROM (SELECT id FROM wa WHERE id < 3) s, LATERAL generate_series(1, s.id) g ORDER BY 1, 2;
SELECT * FROM (SELECT id FROM wa WHERE id < 3) s LEFT JOIN LATERAL (SELECT wb.y FROM wb WHERE wb.id = s.id) t ON true ORDER BY 1;
WITH c AS (SELECT 1 AS k) SELECT * FROM (SELECT id FROM wa WHERE id < 2) s, LATERAL (SELECT s.id AS sid, k FROM c) t;
SELECT * FROM (SELECT id FROM wa WHERE id < 3) s, LATERAL (SELECT s.id AS sid FROM (VALUES (1)) v) t ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT id FROM wa WHERE id < 3) s LEFT JOIN LATERAL (SELECT wb.y FROM wb WHERE wb.id = s.id) t ON true;

-- ===== src/backend/parser/parse_target.c:FigureColnameInternal|SubLinkType =====
SELECT (SELECT 1), EXISTS(SELECT 1), 1 = ANY(SELECT 1), 1 < ALL(SELECT 2), ARRAY(SELECT 1), 1 IN (SELECT 1), (1, 2) < (SELECT 1, 2), (1, 2) = (SELECT 1, 2);
CREATE VIEW wv_colnames AS SELECT (SELECT 1), EXISTS(SELECT 1), 1 = ANY(SELECT 1), 1 < ALL(SELECT 2), ARRAY(SELECT 1), 1 IN (SELECT 1), (1, 2) < (SELECT 1, 2);
SELECT attname FROM pg_attribute WHERE attrelid = 'wv_colnames'::regclass AND attnum > 0 ORDER BY attnum;

-- ===== src/backend/executor/nodeAgg.c:ExecAgg|AggStrategy =====
SELECT count(*), sum(x) FROM wa;
EXPLAIN (COSTS OFF) SELECT count(*), sum(x) FROM wa;
SET enable_hashagg = off;
SELECT x, count(*) FROM wa GROUP BY x ORDER BY x;
EXPLAIN (COSTS OFF) SELECT x, count(*) FROM wa GROUP BY x ORDER BY x;
RESET enable_hashagg;
SET work_mem = '64kB';
EXPLAIN (COSTS OFF) SELECT x, b, v, count(*) FROM wa GROUP BY GROUPING SETS (ROLLUP (x, b), (v), ()) ORDER BY 1, 2, 3, 4;
SELECT x, b, v, count(*) FROM wa GROUP BY GROUPING SETS (ROLLUP (x, b), (v), ()) ORDER BY 1, 2, 3, 4;
RESET work_mem;
EXPLAIN (COSTS OFF) SELECT x, b, count(*) FROM wa GROUP BY GROUPING SETS ((x), (b), ()) ORDER BY 1, 2, 3;
SELECT x, b, count(*) FROM wa GROUP BY GROUPING SETS ((x), (b), ()) ORDER BY 1, 2, 3;

-- ===== src/backend/optimizer/prep/prepjointree.c:pull_up_simple_subquery|RTEKind =====
-- a LATERAL subquery over a transition table is pulled up; its child ENR RTE
-- takes the "can't contain lateral references" arm.
CREATE TABLE tt_base (id int, n int);
INSERT INTO tt_base VALUES (1, 10), (2, 20);
CREATE TABLE tt (id int);
CREATE FUNCTION tt_trg() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE r record;
BEGIN
  FOR r IN SELECT b.id, s.nid FROM tt_base b, LATERAL (SELECT nt.id AS nid FROM nt WHERE nt.id = b.id) s ORDER BY 1 LOOP
    RAISE NOTICE 'lateral enr: % %', r.id, r.nid;
  END LOOP;
  FOR r IN SELECT count(*) AS c FROM (SELECT * FROM nt) s LOOP
    RAISE NOTICE 'pulled-up enr count: %', r.c;
  END LOOP;
  FOR r IN EXECUTE 'EXPLAIN (COSTS OFF) SELECT * FROM nt' LOOP
    RAISE NOTICE 'plan: %', r."QUERY PLAN";
  END LOOP;
  FOR r IN EXECUTE 'EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT * FROM nt) s' LOOP
    RAISE NOTICE 'plan: %', r."QUERY PLAN";
  END LOOP;
  BEGIN
    CREATE VIEW tt_enr_view AS SELECT * FROM nt;
  EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'view over enr: % %', SQLSTATE, SQLERRM;
  END;
  RETURN NULL;
END $$;
CREATE TRIGGER tt_ai AFTER INSERT ON tt REFERENCING NEW TABLE AS nt FOR EACH STATEMENT EXECUTE FUNCTION tt_trg();
SET client_min_messages = notice;
INSERT INTO tt VALUES (1), (2), (3);
SET client_min_messages = warning;

-- ===== src/backend/optimizer/plan/createplan.c:create_plan_recurse|NodeTag =====
-- ===== src/backend/optimizer/plan/createplan.c:create_join_plan|NodeTag =====
-- one plan shape per pathtype arm (CustomScan is UNPORTED).
EXPLAIN (COSTS OFF) SELECT count(*) FROM wa;
EXPLAIN (COSTS OFF) SELECT id FROM wa WHERE id < 3 UNION ALL SELECT id FROM wb WHERE id < 3;
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM wa WHERE x = 3 OR x = 5;
EXPLAIN (COSTS OFF) SELECT * FROM wa ORDER BY x, id;
EXPLAIN (COSTS OFF) SELECT id FROM wa WHERE id < 5;
EXPLAIN (COSTS OFF) SELECT * FROM wa WHERE id = 5;
EXPLAIN (COSTS OFF) SELECT id FROM wa UNION ALL SELECT id FROM wb ORDER BY id;
RESET enable_seqscan;
EXPLAIN (COSTS OFF) SELECT * FROM wa LIMIT 1;
EXPLAIN (COSTS OFF) SELECT * FROM wa WHERE id = 1 FOR UPDATE;
SET enable_hashjoin = off; SET enable_mergejoin = off;
EXPLAIN (COSTS OFF) SELECT * FROM wa, wb WHERE wa.x = wb.y;
EXPLAIN (COSTS OFF) SELECT * FROM wb JOIN wa ON wa.x = wb.y WHERE wb.id < 100;
EXPLAIN (COSTS OFF) SELECT * FROM wb JOIN wa ON wa.id = wb.id;
RESET enable_hashjoin; RESET enable_mergejoin;
SET enable_hashjoin = off; SET enable_nestloop = off;
EXPLAIN (COSTS OFF) SELECT * FROM wb JOIN wa ON wa.id = wb.id;
RESET enable_hashjoin; RESET enable_nestloop;
EXPLAIN (COSTS OFF) SELECT * FROM wb JOIN wa ON wa.id = wb.id;
EXPLAIN (COSTS OFF) UPDATE wa SET x = x + 1 WHERE id = 1;
EXPLAIN (COSTS OFF) SELECT generate_series(1, 3);
EXPLAIN (COSTS OFF) WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT * FROM r;
EXPLAIN (COSTS OFF) SELECT id FROM wa INTERSECT SELECT id FROM wb;
EXPLAIN (COSTS OFF) SELECT id FROM wa EXCEPT ALL SELECT id FROM wb;
EXPLAIN (COSTS OFF) SELECT * FROM wa ORDER BY t;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT id, random() AS r FROM wa) s WHERE s.id < 3;
EXPLAIN (COSTS OFF) SELECT * FROM wa WHERE ctid >= '(0,1)' AND ctid < '(1,1)';
EXPLAIN (COSTS OFF) SELECT * FROM wa WHERE ctid = '(0,1)';
SET enable_hashagg = off;
EXPLAIN (COSTS OFF) SELECT DISTINCT x FROM wa;
EXPLAIN (COSTS OFF) SELECT x FROM wa GROUP BY x;
RESET enable_hashagg;
EXPLAIN (COSTS OFF) SELECT rank() OVER (ORDER BY x) FROM wa LIMIT 1;
SET parallel_setup_cost = 0; SET parallel_tuple_cost = 0; SET min_parallel_table_scan_size = 0; SET min_parallel_index_scan_size = 0;
SET max_parallel_workers_per_gather = 2;
EXPLAIN (COSTS OFF) SELECT count(*) FROM wa;
EXPLAIN (COSTS OFF) SELECT * FROM wa ORDER BY x;
SELECT count(*) FROM wa;
SET max_parallel_workers_per_gather = 0;

-- ===== src/backend/executor/execExpr.c:ExecInitExprRec|NodeTag (AggState/WindowAggState) =====
SELECT sum(x), count(DISTINCT b), string_agg(v, ',' ORDER BY v) FILTER (WHERE id < 3) FROM wa;
SELECT id, rank() OVER (ORDER BY x), sum(x) OVER (PARTITION BY b ORDER BY id ROWS 1 PRECEDING) FROM wa WHERE id < 5 ORDER BY id;
SELECT x, sum(id), rank() OVER (ORDER BY sum(id)) FROM wa GROUP BY x ORDER BY x LIMIT 3;

-- ===== src/backend/nodes/nodeFuncs.c:expression_tree_walker_impl|NodeTag (IndexClause/PlaceHolderInfo) =====
-- ===== src/backend/nodes/nodeFuncs.c:expression_tree_mutator_impl|NodeTag (IndexClause/PlaceHolderInfo) =====
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa LEFT JOIN (SELECT id, 1 AS one FROM wb) s ON wa.id = s.id WHERE s.one IS NULL AND wa.id < 5;
SELECT * FROM wa LEFT JOIN (SELECT id, 1 AS one FROM wb) s ON wa.id = s.id WHERE s.one IS NULL ORDER BY wa.id LIMIT 3;
SELECT wa.id, s.one FROM wa LEFT JOIN (SELECT id, 1 AS one FROM wb) s ON wa.id = s.id WHERE wa.id IN (1, 501) ORDER BY 1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT wa.id, s.one FROM wa LEFT JOIN (SELECT id, 1 AS one FROM wb) s ON wa.id = s.id WHERE wa.id IN (1, 501);
EXPLAIN (COSTS OFF) SELECT * FROM wa WHERE id = 5 AND x = 5;
EXPLAIN (COSTS OFF) SELECT * FROM wa WHERE v LIKE 'v1%' AND id < 10;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa WHERE id = ANY (ARRAY[1, 2, 3]);

-- ===== src/backend/optimizer/path/allpaths.c:set_rel_consider_parallel|RTEKind (RTE_GROUP/RTE_JOIN) =====
SET max_parallel_workers_per_gather = 2;
EXPLAIN (COSTS OFF) SELECT wa.x, count(*) FROM wa JOIN wb ON wa.id = wb.id GROUP BY wa.x;
SELECT wa.x, count(*) FROM wa JOIN wb ON wa.id = wb.id GROUP BY wa.x ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT s.x, count(*) FROM (SELECT x FROM wa GROUP BY x) s JOIN wb ON s.x = wb.y GROUP BY s.x;
SET max_parallel_workers_per_gather = 0;

-- ===== src/backend/optimizer/path/costsize.c:cost_qual_eval_walker|NodeTag (RestrictInfo) =====
EXPLAIN SELECT * FROM wa JOIN wb ON wa.id = wb.id WHERE wa.x > 5;
EXPLAIN SELECT * FROM wa WHERE x > 5 AND t <> 'zz' AND (b OR v = 'v1');
EXPLAIN SELECT * FROM wa LEFT JOIN wb ON wa.id = wb.id AND wb.y > 2 WHERE wa.x = 1;

-- ===== src/backend/parser/parse_clause.c:transformFromClauseItem|NodeTag (JoinExpr) =====
SELECT count(*) FROM (wa JOIN wb ON wa.id = wb.id) JOIN wa w2 ON w2.id = wb.id;
SELECT count(*) FROM wa CROSS JOIN (wb JOIN wa w3 USING (id)) WHERE wa.id < 3;
SELECT * FROM (wa JOIN wb USING (id)) AS j(a, b, c, d, e, f) ORDER BY a LIMIT 2;
SELECT * FROM ((SELECT 1 AS k, 'x' AS s) a NATURAL JOIN (SELECT 1 AS k, 'y' AS u) b) j;
SELECT * FROM wa JOIN (wb JOIN (SELECT 1 AS id) s ON s.id = wb.id) ON wa.id = wb.id;
SELECT * FROM (wa AS w1 JOIN wb AS w2 ON w1.id = w2.id) AS jj(id, x, b, v, t, id2, y) WHERE jj.id = 2;

-- ===== src/backend/utils/adt/selfuncs.c:index_other_operands_eval_cost|NodeTag (RestrictInfo) =====
EXPLAIN SELECT * FROM wa WHERE id = (SELECT max(id) FROM wb);
EXPLAIN SELECT * FROM wa WHERE id = length('abc' || 'd');
SET enable_hashjoin = off; SET enable_mergejoin = off;
EXPLAIN SELECT * FROM wb JOIN wa ON wa.id = wb.id + 1 WHERE wb.id < 10;
RESET enable_hashjoin; RESET enable_mergejoin;
EXPLAIN SELECT * FROM wa WHERE x = 3 AND id IN (SELECT id FROM wb WHERE y = 1);

-- ===== src/backend/commands/explain.c:ExplainTargetRel|NodeTag =====
EXPLAIN (COSTS OFF) INSERT INTO wa VALUES (2000, 1, true, 'v', 't');
EXPLAIN (VERBOSE, COSTS OFF) UPDATE wa SET x = 1 WHERE id = 1;
EXPLAIN (COSTS OFF) DELETE FROM wa WHERE id = 1;
EXPLAIN (COSTS OFF) MERGE INTO wa USING wb ON wa.id = wb.id WHEN MATCHED THEN UPDATE SET x = wb.y WHEN NOT MATCHED THEN INSERT VALUES (wb.id, wb.y, true, 'v', 't');
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa TABLESAMPLE SYSTEM (50) WHERE id = 1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa WHERE ctid = '(0,1)';
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa WHERE ctid > '(0,1)';
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM generate_series(1, 2) AS g(i);
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS v(i, s);
EXPLAIN (VERBOSE, COSTS OFF) WITH c AS MATERIALIZED (SELECT id FROM wa WHERE id < 3) SELECT * FROM c c1, c c2;
EXPLAIN (VERBOSE, COSTS OFF) WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 5) SELECT * FROM r;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM JSON_TABLE('[1, 2]', '$[*]' COLUMNS (a int PATH '$')) jt;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM XMLTABLE('/r/e' PASSING '<r><e>1</e></r>' COLUMNS a int PATH '.') xt;
SET enable_seqscan = off;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa WHERE x = 3 OR x = 5;
EXPLAIN (VERBOSE, COSTS OFF) SELECT id FROM wa WHERE id < 3;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa WHERE id < 3;
RESET enable_seqscan;

-- ===== src/backend/executor/execAmi.c:ExecSupportsMarkRestore|NodeTag (GroupResultPath/MinMaxAggPath) =====
-- merge join whose inner is a Result path: C returns false, so a Materialize is
-- interposed.
SET enable_hashjoin = off; SET enable_nestloop = off;
EXPLAIN (COSTS OFF) SELECT * FROM wa JOIN (SELECT min(id) AS m FROM wb) s ON wa.id = s.m;
SELECT * FROM wa JOIN (SELECT min(id) AS m FROM wb) s ON wa.id = s.m;
EXPLAIN (COSTS OFF) SELECT * FROM wa JOIN (SELECT 2 AS m GROUP BY 1) s ON wa.id = s.m;
SELECT * FROM wa JOIN (SELECT 2 AS m GROUP BY 1) s ON wa.id = s.m;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT min(id) AS m FROM wb) s JOIN wa ON wa.id = s.m;
RESET enable_hashjoin; RESET enable_nestloop;

-- ===== src/backend/optimizer/path/costsize.c:has_indexed_join_quals|NodeTag =====
SET enable_hashjoin = off; SET enable_mergejoin = off;
EXPLAIN SELECT * FROM wb JOIN wa ON wa.id = wb.id WHERE wb.id < 10;
EXPLAIN SELECT wa.id FROM wb JOIN wa ON wa.id = wb.id WHERE wb.id < 10;
SET enable_indexscan = off;
EXPLAIN SELECT * FROM wb JOIN wa ON wa.x = wb.y WHERE wb.id < 3;
RESET enable_indexscan;
RESET enable_hashjoin; RESET enable_mergejoin;

-- ===== src/backend/optimizer/plan/createplan.c:use_physical_tlist|NodeTag (BitmapHeapPath) =====
-- ===== src/backend/optimizer/util/plancat.c:build_physical_tlist|RTEKind =====
SET enable_seqscan = off;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa WHERE x = 3 OR x = 4;
EXPLAIN (VERBOSE, COSTS OFF) SELECT t FROM wa WHERE x = 3 OR x = 4;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa WHERE (x = 3 OR x = 4) AND t LIKE 't1%';
RESET enable_seqscan;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa WHERE x = 1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (SELECT * FROM wa WHERE x = 1 OFFSET 0) s;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM (VALUES (1, 2), (3, 4)) v(a, b);
EXPLAIN (VERBOSE, COSTS OFF) SELECT 1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM generate_series(1, 2) g;
EXPLAIN (VERBOSE, COSTS OFF) WITH c AS MATERIALIZED (SELECT * FROM wa WHERE id < 3) SELECT * FROM c;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM wa, wb WHERE wa.id = wb.id AND wa.id < 3;

-- ===== src/backend/catalog/dependency.c:find_expr_references_walker|RTEKind (RTE_JOIN/RTE_NAMEDTUPLESTORE) =====
CREATE TABLE dep_i (k int, a int);
CREATE TABLE dep_n (k numeric, b int);
CREATE TABLE dep_b (k bigint, c int);
CREATE VIEW dep_v AS SELECT * FROM dep_i JOIN dep_n USING (k);
CREATE VIEW dep_v2 AS SELECT k, a, c FROM dep_i FULL JOIN dep_b USING (k);
CREATE VIEW dep_v3 AS SELECT j.* FROM (dep_i JOIN dep_n USING (k)) j WHERE j.k > 1;
SELECT c.relname, pg_describe_object(d.refclassid, d.refobjid, d.refobjsubid) AS ref, d.deptype
FROM pg_depend d JOIN pg_rewrite r ON d.classid = 'pg_rewrite'::regclass AND d.objid = r.oid
JOIN pg_class c ON c.oid = r.ev_class
WHERE c.relname IN ('dep_v', 'dep_v2', 'dep_v3')
ORDER BY 1, 2, 3;

-- ===== src/backend/executor/execAmi.c:ExecReScan|NodeTag (HashState) =====
-- ===== src/backend/executor/execProcnode.c:ExecEndNode|NodeTag (HashState) =====
-- ===== src/backend/executor/execProcnode.c:ExecShutdownNode_walker|NodeTag (HashState) =====
SET enable_nestloop = off; SET enable_mergejoin = off;
EXPLAIN (COSTS OFF) SELECT o.y, (SELECT count(*) FROM wa JOIN wb ON wa.id = wb.id WHERE wb.y = o.y) FROM (VALUES (1), (2), (3)) o(y);
SELECT o.y, (SELECT count(*) FROM wa JOIN wb ON wa.id = wb.id WHERE wb.y = o.y) FROM (VALUES (1), (2), (3)) o(y) ORDER BY 1;
SELECT * FROM (VALUES (1), (2)) o(y), LATERAL (SELECT count(*) AS c FROM wa JOIN wb ON wa.id = wb.id WHERE wb.y = o.y) t ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT * FROM (VALUES (1), (2)) o(y), LATERAL (SELECT count(*) AS c FROM wa JOIN wb ON wa.id = wb.id WHERE wb.y = o.y) t;
SELECT count(*) FROM (SELECT * FROM wa JOIN wb ON wa.id = wb.id LIMIT 5) s;
SET max_parallel_workers_per_gather = 2; SET enable_parallel_hash = on;
EXPLAIN (COSTS OFF) SELECT count(*) FROM wa JOIN wb ON wa.id = wb.id;
SELECT count(*) FROM wa JOIN wb ON wa.id = wb.id;
SELECT count(*) FROM (SELECT * FROM wa JOIN wb ON wa.id = wb.id LIMIT 5) s;
SET max_parallel_workers_per_gather = 0;
RESET enable_nestloop; RESET enable_mergejoin;

-- ===== src/backend/commands/explain.c:ExplainNode|CmdType (CMD_SELECT) =====
EXPLAIN (COSTS OFF) INSERT INTO wb SELECT 1000, 1;
EXPLAIN (COSTS OFF) UPDATE wb SET y = 1 WHERE id = 1;
EXPLAIN (COSTS OFF) DELETE FROM wb WHERE id = 1;
EXPLAIN (COSTS OFF) MERGE INTO wb USING wa ON wa.id = wb.id WHEN MATCHED THEN DELETE;
EXPLAIN (COSTS OFF) INSERT INTO wb VALUES (1000, 1) ON CONFLICT (id) DO UPDATE SET y = EXCLUDED.y;

-- ===== src/backend/parser/parse_clause.c:flatten_grouping_sets|NodeTag (List) =====
SELECT x, b, count(*) FROM wa WHERE id < 20 GROUP BY GROUPING SETS ((x, b), ROLLUP (x), CUBE (b), ()) ORDER BY 1, 2, 3;
SELECT x, b, count(*) FROM wa WHERE id < 20 GROUP BY (x, b) ORDER BY 1, 2;
SELECT x, b, count(*) FROM wa WHERE id < 20 GROUP BY GROUPING SETS ((x, (b)), ((x), b)) ORDER BY 1, 2, 3;
SELECT x, count(*) FROM wa WHERE id < 20 GROUP BY GROUPING SETS (GROUPING SETS (x, ()), x) ORDER BY 1, 2;
SELECT x, b, v, count(*) FROM wa WHERE id < 20 GROUP BY x, (b, v) ORDER BY 1, 2, 3;
SELECT x, b, count(*) FROM wa WHERE id < 20 GROUP BY ROLLUP ((x, b)) ORDER BY 1, 2, 3;
EXPLAIN (COSTS OFF) SELECT x, b, count(*) FROM wa GROUP BY GROUPING SETS ((x, (b)), ((x), b));

-- ===== src/backend/utils/adt/ruleutils.c:get_sublink_expr|SubLinkType (CTE_SUBLINK) =====
CREATE VIEW wv_sublinks AS SELECT (SELECT 1) AS a, EXISTS(SELECT 1) AS e, 1 = ANY(SELECT id FROM wb) AS an,
  1 < ALL(SELECT id FROM wb) AS al, ARRAY(SELECT id FROM wb WHERE id < 3) AS ar, (1, 2) < (SELECT 1, 2) AS rc,
  1 IN (SELECT 1) AS i, 1 NOT IN (SELECT 2) AS ni, 5 > SOME(SELECT id FROM wb) AS so;
SELECT pg_get_viewdef('wv_sublinks'::regclass);
SELECT * FROM wv_sublinks;
CREATE VIEW wv_cte AS WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM r WHERE n < 3) SELECT n FROM r WHERE EXISTS (SELECT 1 FROM r r2 WHERE r2.n > r.n);
SELECT pg_get_viewdef('wv_cte'::regclass);
SELECT * FROM wv_cte;

-- ===== src/backend/parser/analyze.c:transformInsertRow|NodeTag (CoerceToDomain/FieldStore/SubscriptingRef) =====
CREATE TYPE cpx AS (a int, b int);
CREATE DOMAIN dpos AS int CHECK (VALUE > 0);
CREATE DOMAIN dcpx AS cpx CHECK ((VALUE).a IS NOT NULL);
CREATE TABLE wi (id int, c cpx, arr int[], d dpos, dc dcpx, arr2 int[][], darr dpos[]);
INSERT INTO wi (id, c.a, c.b) VALUES (1, 10, 20), (2, 30, 40);
INSERT INTO wi (id, arr[1], arr[2]) VALUES (3, 1, 2), (4, 3, 4);
INSERT INTO wi (id, d) VALUES (5, 7), (6, 8);
INSERT INTO wi (id, d) VALUES (7, -1);
INSERT INTO wi (id, d) VALUES (7, 1), (8, -1);
INSERT INTO wi (id, dc.a) VALUES (8, 1), (9, 2);
INSERT INTO wi (id, dc.a, dc.b) VALUES (10, 1, 2), (11, 3, 4);
INSERT INTO wi (id, dc.b) VALUES (12, 5);
INSERT INTO wi (id, arr2[1][1], arr2[1][2]) VALUES (13, 1, 2), (14, 3, 4);
INSERT INTO wi (id, c.a, arr[1]) SELECT 15, 5, 6;
INSERT INTO wi (id, c.a, c.b) VALUES (16, 1, 2) ON CONFLICT DO NOTHING;
INSERT INTO wi (id, darr[1], darr[2]) VALUES (17, 1, 2), (18, 3, 4);
INSERT INTO wi (id, darr[1]) VALUES (19, -5);
INSERT INTO wi (id, c.a) VALUES (20, DEFAULT), (21, 1);
INSERT INTO wi (id, arr[1:2]) VALUES (22, '{7,8}'), (23, '{9,10}');
INSERT INTO wi (id, c) VALUES (24, ROW(1, 2)), (25, DEFAULT);
INSERT INTO wi (id, c.a, c) VALUES (26, 1, ROW(3, 4));
INSERT INTO wi (id, c, c.a) VALUES (27, ROW(3, 4), 1);
INSERT INTO wi (id, c.a, c.a) VALUES (28, 1, 2);
SELECT * FROM wi ORDER BY id;

-- ===== src/backend/rewrite/rewriteManip.c:ChangeVarNodes_walker|NodeTag (AppendRelInfo/PlanRowMark) =====
-- self-join elimination with row marks and append-rel entries in the query.
CREATE TABLE sje (id int PRIMARY KEY, v int);
INSERT INTO sje SELECT i, i * 2 FROM generate_series(1, 10) i;
VACUUM ANALYZE sje;
EXPLAIN (COSTS OFF) SELECT a.id, b.v FROM sje a JOIN sje b ON a.id = b.id WHERE a.v > 4;
EXPLAIN (COSTS OFF) SELECT a.id, b.v FROM sje a JOIN sje b ON a.id = b.id WHERE a.v > 4 FOR UPDATE;
SELECT a.id, b.v FROM sje a JOIN sje b ON a.id = b.id WHERE a.v > 4 ORDER BY 1 LIMIT 3;
SELECT a.id, b.v FROM sje a JOIN sje b ON a.id = b.id WHERE a.v > 4 ORDER BY 1 LIMIT 3 FOR UPDATE OF b;
EXPLAIN (COSTS OFF) SELECT a.id, b.v, u.id FROM sje a JOIN sje b ON a.id = b.id JOIN (SELECT id FROM wa UNION ALL SELECT id FROM wb) u ON u.id = a.id WHERE b.v = 4;
SELECT a.id, b.v, u.id FROM sje a JOIN sje b ON a.id = b.id JOIN (SELECT id FROM wa UNION ALL SELECT id FROM wb) u ON u.id = a.id WHERE b.v = 4 ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT a.id, b.v FROM sje a JOIN sje b ON a.id = b.id JOIN (SELECT id FROM wa UNION ALL SELECT id FROM wb) u ON u.id = b.id WHERE b.v = 4 FOR SHARE OF a;
SELECT a.id, b.v FROM sje a JOIN sje b ON a.id = b.id JOIN (SELECT id FROM wa UNION ALL SELECT id FROM wb) u ON u.id = b.id WHERE b.v = 4 ORDER BY 1 FOR SHARE OF a;

-- ===== src/backend/statistics/mcv.c:mcv_get_match_bitmap|NodeTag (RestrictInfo/Var) =====
CREATE STATISTICS wa_mcv (mcv) ON x, b FROM wa;
ANALYZE wa;
EXPLAIN SELECT * FROM wa WHERE x = 1 AND b;
EXPLAIN SELECT * FROM wa WHERE x = 2 AND NOT b;
EXPLAIN SELECT * FROM wa WHERE x = 1 AND b IS TRUE;
EXPLAIN SELECT * FROM wa WHERE (x = 1 OR x = 2) AND b;
EXPLAIN SELECT * FROM wa WHERE x IN (1, 3) AND b = false;
SELECT count(*) FROM wa WHERE x = 1 AND b;
SELECT count(*) FROM wa WHERE x = 2 AND NOT b;

-- ===== src/backend/optimizer/plan/setrefs.c:set_join_references|NodeTag (Var) =====
SET enable_hashjoin = off; SET enable_mergejoin = off;
EXPLAIN (VERBOSE, COSTS OFF) SELECT wa.t FROM wb, wa WHERE wa.id = wb.id AND wb.id < 3;
SELECT wa.t FROM wb, wa WHERE wa.id = wb.id AND wb.id < 3 ORDER BY 1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT wa.t, wb.y FROM wb JOIN wa ON wa.x = wb.y + 1 WHERE wb.id = 3;
SELECT wa.t, wb.y FROM wb JOIN wa ON wa.x = wb.y + 1 WHERE wb.id = 3 ORDER BY 1 LIMIT 2;
RESET enable_hashjoin; RESET enable_mergejoin;

-- ===== src/backend/optimizer/prep/prepjointree.c:reduce_outer_joins_pass2|NodeTag (RangeTblRef) =====
EXPLAIN (COSTS OFF) SELECT * FROM wa LEFT JOIN wb ON wa.id = wb.id WHERE wb.y = 1;
EXPLAIN (COSTS OFF) SELECT * FROM wa LEFT JOIN (wb LEFT JOIN sje ON wb.id = sje.id) ON wa.id = wb.id WHERE sje.v = 4;
EXPLAIN (COSTS OFF) SELECT * FROM wa FULL JOIN wb ON wa.id = wb.id WHERE wa.x = 1;
EXPLAIN (COSTS OFF) SELECT * FROM wa RIGHT JOIN wb ON wa.id = wb.id WHERE wa.x = 1;
EXPLAIN (COSTS OFF) SELECT * FROM wa LEFT JOIN wb ON wa.id = wb.id LEFT JOIN sje ON sje.id = wb.id WHERE wb.y IS NOT NULL;
SELECT count(*) FROM wa LEFT JOIN (wb LEFT JOIN sje ON wb.id = sje.id) ON wa.id = wb.id WHERE sje.v = 4;

-- ===== src/backend/parser/parse_relation.c:expandRTE|NodeTag (Var) =====
SELECT * FROM wa JOIN wb USING (id) ORDER BY id LIMIT 2;
SELECT j FROM (wa JOIN wb USING (id)) j ORDER BY 1 LIMIT 2;
SELECT j.* FROM (wa JOIN wb USING (id)) j ORDER BY 1 LIMIT 2;
SELECT * FROM (wa FULL JOIN wb USING (id)) j WHERE id > 998 ORDER BY 1;
SELECT row_to_json(j) FROM (wa FULL JOIN wb USING (id)) j WHERE id > 998 ORDER BY id;
SELECT * FROM (SELECT 1 AS a) s NATURAL JOIN (SELECT 1::bigint AS a) t;
SELECT j FROM ((SELECT 1 AS a) s NATURAL JOIN (SELECT 1::bigint AS a) t) j;
SELECT * FROM ((SELECT 1 AS a, 'x' AS s) s NATURAL FULL JOIN (SELECT 2::numeric AS a, 'y' AS u) t) j ORDER BY 1;
SELECT pg_typeof(j.a) FROM ((SELECT 1 AS a) s NATURAL FULL JOIN (SELECT 2::numeric AS a) t) j LIMIT 1;

-- ===== src/backend/partitioning/partprune.c:gen_partprune_steps_internal|NodeTag (RestrictInfo) =====
-- ===== src/backend/partitioning/partprune.c:match_clause_to_partition_key|NodeTag (RelabelType) =====
-- ===== src/backend/optimizer/util/pathnode.c:path_is_reparameterizable_by_child|NodeTag (ForeignPath/GatherPath) =====
-- ===== src/backend/optimizer/util/pathnode.c:reparameterize_path_by_child|NodeTag (ForeignPath/GatherPath) =====
CREATE TABLE pa (id int, v text) PARTITION BY RANGE (id);
CREATE TABLE pa1 PARTITION OF pa FOR VALUES FROM (0) TO (100);
CREATE TABLE pa2 PARTITION OF pa FOR VALUES FROM (100) TO (200);
CREATE TABLE pb (id int, w text) PARTITION BY RANGE (id);
CREATE TABLE pb1 PARTITION OF pb FOR VALUES FROM (0) TO (100);
CREATE TABLE pb2 PARTITION OF pb FOR VALUES FROM (100) TO (200);
CREATE INDEX ON pb1 (id);
CREATE INDEX ON pb2 (id);
INSERT INTO pa SELECT i, 'a' || i FROM generate_series(0, 199) i;
INSERT INTO pb SELECT i, 'b' || i FROM generate_series(0, 199, 2) i;
VACUUM ANALYZE pa;
VACUUM ANALYZE pb;
EXPLAIN (COSTS OFF) SELECT * FROM pa WHERE id = 5;
EXPLAIN (COSTS OFF) SELECT * FROM pa WHERE id IN (5, 150);
EXPLAIN (COSTS OFF) SELECT * FROM pa WHERE id = ANY (ARRAY[5, 7]);
EXPLAIN (COSTS OFF) SELECT * FROM pa WHERE id IS NULL;
EXPLAIN (COSTS OFF) SELECT * FROM pa WHERE (id > 150 AND id < 160) OR id = 3;
EXPLAIN (COSTS OFF) SELECT * FROM pa WHERE id = 5 AND v = 'a5';
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT * FROM pa WHERE id = (SELECT 150);
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF) SELECT * FROM pa WHERE id = (SELECT 150) OR id = (SELECT 250);
SELECT * FROM pa WHERE id = (SELECT 150);
SET enable_partitionwise_join = on;
SET enable_hashjoin = off; SET enable_mergejoin = off;
EXPLAIN (COSTS OFF) SELECT * FROM pa JOIN pb ON pa.id = pb.id WHERE pa.v = 'a4';
SELECT * FROM pa JOIN pb ON pa.id = pb.id WHERE pa.v = 'a4';
EXPLAIN (COSTS OFF) SELECT * FROM pa JOIN pb ON pa.id = pb.id WHERE pa.v IN ('a4', 'a150');
SET enable_seqscan = off;
EXPLAIN (COSTS OFF) SELECT * FROM pa JOIN pb ON pa.id = pb.id WHERE pa.v IN ('a4', 'a150');
SET max_parallel_workers_per_gather = 2;
EXPLAIN (COSTS OFF) SELECT * FROM pa JOIN pb ON pa.id = pb.id WHERE pa.v IN ('a4', 'a150');
SET max_parallel_workers_per_gather = 0;
RESET enable_seqscan;
RESET enable_hashjoin; RESET enable_mergejoin;
RESET enable_partitionwise_join;
CREATE TABLE pl (k varchar(10), n int) PARTITION BY LIST (k);
CREATE TABLE pl_a PARTITION OF pl FOR VALUES IN ('a');
CREATE TABLE pl_b PARTITION OF pl FOR VALUES IN ('b', 'c');
CREATE TABLE pl_d PARTITION OF pl DEFAULT;
INSERT INTO pl VALUES ('a', 1), ('b', 2), ('c', 3), ('z', 26);
VACUUM ANALYZE pl;
EXPLAIN (COSTS OFF) SELECT * FROM pl WHERE k = 'a';
EXPLAIN (COSTS OFF) SELECT * FROM pl WHERE k = 'a'::varchar;
EXPLAIN (COSTS OFF) SELECT * FROM pl WHERE 'b' = k;
EXPLAIN (COSTS OFF) SELECT * FROM pl WHERE k IN ('a', 'c');
EXPLAIN (COSTS OFF) SELECT * FROM pl WHERE k = ANY (ARRAY['a', 'z']::varchar[]);
EXPLAIN (COSTS OFF) SELECT * FROM pl WHERE k = 'a'::name;
EXPLAIN (COSTS OFF) SELECT * FROM pl WHERE k::text = 'a';
EXPLAIN (COSTS OFF) SELECT * FROM pl WHERE k <> 'a';
EXPLAIN (COSTS OFF) SELECT * FROM pl WHERE k IS NULL;
EXPLAIN (COSTS OFF) SELECT * FROM pl WHERE k < 'b';
SELECT * FROM pl WHERE k = 'a'::varchar;
CREATE TABLE pc (k char(2), n int) PARTITION BY LIST (k);
CREATE TABLE pc_a PARTITION OF pc FOR VALUES IN ('a');
CREATE TABLE pc_d PARTITION OF pc DEFAULT;
INSERT INTO pc VALUES ('a', 1), ('b', 2);
EXPLAIN (COSTS OFF) SELECT * FROM pc WHERE k = 'a';
EXPLAIN (COSTS OFF) SELECT * FROM pc WHERE k = 'a'::varchar;
EXPLAIN (COSTS OFF) SELECT * FROM pc WHERE k = 'a'::text;
SELECT * FROM pc WHERE k = 'a'::varchar;

-- ===== src/backend/rewrite/rewriteHandler.c:RewriteQuery|NodeTag (FromExpr) =====
CREATE TABLE rl (a int DEFAULT 99);
CREATE TABLE rl_log (a int, op text);
CREATE RULE rl_ins AS ON INSERT TO rl DO ALSO INSERT INTO rl_log VALUES (NEW.a, 'ins');
INSERT INTO rl VALUES (1), (2);
INSERT INTO rl VALUES (3);
INSERT INTO rl SELECT 4;
INSERT INTO rl DEFAULT VALUES;
INSERT INTO rl (a) VALUES (DEFAULT), (5);
INSERT INTO rl SELECT i FROM generate_series(6, 7) i WHERE i > 6;
WITH w AS (SELECT 8 AS a) INSERT INTO rl SELECT a FROM w;
CREATE VIEW rv AS SELECT * FROM rl;
CREATE RULE rv_ins AS ON INSERT TO rv DO INSTEAD INSERT INTO rl VALUES (NEW.a * 10);
INSERT INTO rv VALUES (6), (7);
INSERT INTO rv SELECT 8;
INSERT INTO rv DEFAULT VALUES;
CREATE RULE rl_cond AS ON INSERT TO rl WHERE NEW.a > 50 DO ALSO INSERT INTO rl_log VALUES (NEW.a, 'big');
INSERT INTO rl VALUES (51), (2);
SELECT * FROM rl ORDER BY 1;
SELECT * FROM rl_log ORDER BY 1, 2;

-- ===== src/backend/rewrite/rewriteManip.c:OffsetVarNodes_walker|NodeTag (AppendRelInfo) =====
-- pull-up of a subquery whose own UNION ALL was already flattened: the
-- subquery's append_rel_list entries are offset.
SELECT * FROM (SELECT * FROM (SELECT id FROM wa WHERE id < 3 UNION ALL SELECT id FROM wb WHERE id < 2) u) s ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT * FROM (SELECT id FROM wa WHERE id < 3 UNION ALL SELECT id FROM wb WHERE id < 2) u) s;
SELECT * FROM (SELECT u.id, u.id * 2 AS d FROM (SELECT id FROM wa WHERE id < 3 UNION ALL SELECT id FROM wb WHERE id < 2) u) s JOIN wb ON wb.id = s.id ORDER BY 1;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT u.id, u.id * 2 AS d FROM (SELECT id FROM wa WHERE id < 3 UNION ALL SELECT id FROM wb WHERE id < 2) u) s JOIN wb ON wb.id = s.id;
SELECT * FROM wb, (SELECT * FROM (SELECT id FROM wa WHERE id < 3 UNION ALL SELECT id FROM wb WHERE id < 2) u) s WHERE s.id = wb.id ORDER BY 1, 2;
EXPLAIN (COSTS OFF) SELECT * FROM wb, (SELECT * FROM (SELECT id FROM pa WHERE id < 3 UNION ALL SELECT id FROM wb WHERE id < 2) u) s WHERE s.id = wb.id;

-- ===== src/backend/optimizer/util/pathnode.c:path_is_reparameterizable_by_child|NodeTag (ForeignPath) =====
-- ===== src/backend/optimizer/util/pathnode.c:reparameterize_path_by_child|NodeTag (ForeignPath) =====
-- postgres_fdw builds scan paths parameterized by the *parent* of the other
-- side; a partitionwise nested loop must reparameterize them by the child.
-- With use_remote_estimate the parameterized scan wins, so the plan shows
-- "Remote SQL: ... WHERE ((id = $1::integer))" under the child Nested Loop.
-- (GatherPath stays unreachable: partial paths are never parameterized,
-- pathnode.c add_partial_path.)
CREATE EXTENSION postgres_fdw;
DO $$ BEGIN EXECUTE format('CREATE SERVER loopback FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname ''postgres'', host ''127.0.0.1'', port %L)', current_setting('port')); END $$;
CREATE USER MAPPING FOR postgres SERVER loopback OPTIONS (user 'postgres');
CREATE TABLE fpa (id int, v text) PARTITION BY RANGE (id);
CREATE TABLE fpa1 PARTITION OF fpa FOR VALUES FROM (0) TO (100);
CREATE TABLE fpa2 PARTITION OF fpa FOR VALUES FROM (100) TO (200);
CREATE TABLE fpb (id int, w text) PARTITION BY RANGE (id);
CREATE TABLE fpb1_base (id int, w text);
CREATE TABLE fpb2_base (id int, w text);
CREATE INDEX ON fpb1_base (id);
CREATE INDEX ON fpb2_base (id);
CREATE FOREIGN TABLE fpb1 PARTITION OF fpb FOR VALUES FROM (0) TO (100) SERVER loopback OPTIONS (table_name 'fpb1_base');
CREATE FOREIGN TABLE fpb2 PARTITION OF fpb FOR VALUES FROM (100) TO (200) SERVER loopback OPTIONS (table_name 'fpb2_base');
INSERT INTO fpa SELECT i, 'a' || i FROM generate_series(0, 199) i;
INSERT INTO fpb1_base SELECT i, 'b' || i FROM generate_series(0, 99, 2) i;
INSERT INTO fpb2_base SELECT i, 'b' || i FROM generate_series(100, 199, 2) i;
VACUUM ANALYZE fpa;
VACUUM ANALYZE fpb1_base;
VACUUM ANALYZE fpb2_base;
ANALYZE fpb1;
ANALYZE fpb2;
SET enable_partitionwise_join = on;
SET enable_hashjoin = off; SET enable_mergejoin = off;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM fpa JOIN fpb ON fpa.id = fpb.id WHERE fpa.v IN ('a4', 'a150');
SELECT * FROM fpa JOIN fpb ON fpa.id = fpb.id WHERE fpa.v IN ('a4', 'a150') ORDER BY 1;
ALTER SERVER loopback OPTIONS (ADD use_remote_estimate 'true');
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM fpa JOIN fpb ON fpa.id = fpb.id WHERE fpa.v IN ('a4', 'a150');
SELECT * FROM fpa JOIN fpb ON fpa.id = fpb.id WHERE fpa.v IN ('a4', 'a150') ORDER BY 1;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM fpa JOIN fpb ON fpa.id = fpb.id WHERE fpa.v = 'a4';
SELECT * FROM fpa JOIN fpb ON fpa.id = fpb.id WHERE fpa.v = 'a4';
SET enable_partitionwise_join = off;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM fpa JOIN fpb ON fpa.id = fpb.id WHERE fpa.v IN ('a4', 'a150');
SET enable_partitionwise_join = on;
SET max_parallel_workers_per_gather = 2;
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM fpa JOIN fpb ON fpa.id = fpb.id WHERE fpa.v IN ('a4', 'a150');
EXPLAIN (COSTS OFF) SELECT * FROM fpa JOIN pb ON fpa.id = pb.id WHERE fpa.v IN ('a4', 'a150');
SET max_parallel_workers_per_gather = 0;
ALTER SERVER loopback OPTIONS (DROP use_remote_estimate);
RESET enable_hashjoin; RESET enable_mergejoin;
RESET enable_partitionwise_join;

-- ===== src/backend/executor/execCurrent.c:search_plan_tree|NodeTag (ForeignScanState) =====
-- WHERE CURRENT OF over a foreign scan never reaches search_plan_tree: the
-- planner refuses CURRENT OF on a foreign table first (0A000), and a cursor
-- over the foreign table is "not a simply updatable scan" of the base table.
CREATE TABLE cur_base (id int PRIMARY KEY, v text);
INSERT INTO cur_base VALUES (1, 'a'), (2, 'b');
CREATE FOREIGN TABLE cur_ft (id int, v text) SERVER loopback OPTIONS (table_name 'cur_base');
BEGIN;
DECLARE cur_c CURSOR FOR SELECT * FROM cur_ft;
FETCH 1 FROM cur_c;
UPDATE cur_ft SET v = 'z' WHERE CURRENT OF cur_c;
ROLLBACK;
BEGIN;
DECLARE cur_c CURSOR FOR SELECT * FROM cur_ft;
FETCH 1 FROM cur_c;
UPDATE cur_base SET v = 'z' WHERE CURRENT OF cur_c;
ROLLBACK;
BEGIN;
DECLARE cur_c CURSOR FOR SELECT * FROM cur_ft FOR UPDATE;
FETCH 1 FROM cur_c;
DELETE FROM cur_ft WHERE CURRENT OF cur_c;
ROLLBACK;
BEGIN;
DECLARE cur_c CURSOR FOR SELECT * FROM cur_base;
FETCH 1 FROM cur_c;
UPDATE cur_base SET v = 'z' WHERE CURRENT OF cur_c;
SELECT * FROM cur_base ORDER BY id;
ROLLBACK;
