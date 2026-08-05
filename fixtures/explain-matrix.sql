-- Run against a datadir prepared by fixtures/explain-matrix-setup.sql.
-- Envelope notes (queries shaped around loud unported lanes, all pre-existing):
--   external sort/tuplestore spill loud (tuplesort/tuplestore spill lanes);
--   range consts near histogram edges + mergejoin costing on an indexed rel
--   hit get_actual_variable_range (M2); JOIN..ON under count(*) hits
--   flatten_join_alias_vars; CTE-var join selectivity hits RTE_CTE arm;
--   BETWEEN grammar action and MATERIALIZED hint unported; hashtext (fn 400)
--   unported so no text-key hash joins.
SET compute_query_id = off;
SET max_parallel_workers_per_gather = 0;
SET jit = off;
SET enable_mergejoin = off;

-- seq scan + filter (Rows Removed by Filter)
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT count(*) FROM em WHERE b < 10;

-- in-memory sort (Sort Method: quicksort)
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT * FROM em_small ORDER BY y;

-- top-N heapsort
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT * FROM em ORDER BY b LIMIT 10;

-- hash aggregate (Batches/Memory)
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT b, count(*) FROM em GROUP BY b;

-- window aggregate (Storage line)
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT sum(x) OVER (PARTITION BY y) FROM em_small;

-- materialize under a rescanned nestloop inner (Storage line)
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT count(*) FROM em_small s1, em_small s2;

-- CTE scan, twice-referenced so it stays materialized (Storage line)
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) WITH w AS (SELECT * FROM em_small) SELECT count(*) FROM w w1, w w2;

-- plain index scan (Index Searches)
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT * FROM em WHERE a = 42;
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT * FROM em WHERE a >= 1000 AND a <= 1200;

-- index only scan (Heap Fetches + Index Searches)
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT a FROM em WHERE a >= 10000 AND a <= 10050;

-- (no loops>1 Index Searches leg: C plans NestLoop + parameterized IOS here,
-- but nestParams are loud at exec_init_nest_loop so that plan shape is
-- unreachable — planner lane, not an EXPLAIN gap; the display accumulates
-- xs_nsearches across rescans exactly like C when it lands)

-- bitmap heap scan (Recheck Cond, Heap Blocks exact, Index Searches)
SET enable_indexscan = off;
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT count(*) FROM em WHERE a < 2000;
-- lossy pages (Heap Blocks lossy + Rows Removed by Index Recheck)
SET work_mem = '64kB';
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT count(*) FROM em WHERE a < 30000;
RESET work_mem;
RESET enable_indexscan;

-- single-batch hash join (Buckets/Batches/Memory)
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT count(*) FROM em e, em_small s WHERE e.b = s.x;

-- multi-batch hash join (temp buffers + Batches > 1)
SET work_mem = '64kB';
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF, SUMMARY OFF) SELECT count(*) FROM em e1, em e2 WHERE e1.a = e2.a;
RESET work_mem;

-- BUFFERS: per-node shared hit/read + Planning: Buffers group
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, COSTS OFF, SUMMARY OFF) SELECT count(*) FROM em;
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, COSTS OFF, SUMMARY OFF) SELECT * FROM em WHERE a = 7;
SET enable_indexscan = off;
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, COSTS OFF, SUMMARY OFF) SELECT count(*) FROM em WHERE a < 2000;
RESET enable_indexscan;
SET work_mem = '64kB';
EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, COSTS OFF, SUMMARY OFF) SELECT count(*) FROM em e1, em e2 WHERE e1.a = e2.a;
RESET work_mem;

-- never-executed arm
EXPLAIN (ANALYZE, TIMING OFF, COSTS OFF) SELECT * FROM em WHERE a = 1 LIMIT 0;
