-- Hash join probe side: ExecInitHashJoin compiles the outer hash keys with
-- keep_nulls = HJ_FILL_OUTER, so on an inner join a strict NULL key aborts
-- the hash and ExecHashJoinOuterGetTuple discards the tuple before any
-- later key is evaluated. Outer-fill joins keep NULL keys and evaluate
-- every key.
SET max_parallel_workers_per_gather = 0;
SET enable_nestloop = off;
SET enable_mergejoin = off;
CREATE TABLE hj_null_o(a int, b int);
CREATE TABLE hj_null_i(a int, b int);
INSERT INTO hj_null_o SELECT NULL::int, 0 FROM generate_series(1, 10000);
INSERT INTO hj_null_o VALUES (1, 1);
INSERT INTO hj_null_i VALUES (1, 1);
ANALYZE hj_null_o;
ANALYZE hj_null_i;
EXPLAIN (COSTS OFF)
SELECT count(*) FROM hj_null_o o JOIN hj_null_i i ON o.a = i.a AND 1 / o.b = i.b;
-- NULL first key: 1 / o.b is never evaluated for those rows
SELECT count(*) FROM hj_null_o o JOIN hj_null_i i ON o.a = i.a AND 1 / o.b = i.b;
-- erroring key first: evaluated before the NULL key aborts
SELECT count(*) FROM hj_null_o o JOIN hj_null_i i ON 1 / o.b = i.b AND o.a = i.a;
-- LEFT JOIN fills unmatched outers, so NULL keys are kept and every key runs
SELECT count(*) FROM hj_null_o o LEFT JOIN hj_null_i i ON o.a = i.a AND 1 / o.b = i.b;
DROP TABLE hj_null_o;
DROP TABLE hj_null_i;
