-- The Hash node's EXPLAIN ANALYZE cadence under a rescanned HashJoin.
-- C (nodeHash.c MultiExecHash) times each build with InstrStartNode /
-- InstrStopNode(totalTuples) on the Hash PlanState's own instrument, and
-- ExecReScan on the Hash PlanState (execAmi.c) closes that cycle with
-- InstrEndLoop — from ExecReScanHashJoin's destroy arm (nodeHashjoin.c: a
-- multi-batch table, or a parallel hash), or deferred to MultiExecProcNode's
-- chgParam rescan when a NestLoop Param reaches the build side. So the Hash
-- row reports loops = the number of builds and rows = the build rows per
-- loop, in step with its HashJoin; a Param that reaches only the probe side
-- keeps the single-batch table (ExecReScanHashJoin's reuse arm) and the Hash
-- row stays at loops=1. pgrust rescanned the Hash's CHILD only, leaving the
-- Hash slot's cycle open: loops=1 with every build's rows summed (Gravity v3
-- L4 gate take-1/take-2 rescan cells hash-build-param, hash-build-scan-param,
-- hashjoin-build-param).
CREATE TABLE hnl_o (k int);
INSERT INTO hnl_o VALUES (1), (2), (3);
CREATE TABLE hnl_t (x int, z int);
INSERT INTO hnl_t SELECT i, i % 7 FROM generate_series(1, 2000) i;
CREATE TABLE hnl_u (k int, v int);
INSERT INTO hnl_u SELECT i % 50, i FROM generate_series(1, 100) i;
CREATE TABLE hnl_w (k int, v int, pad text);
INSERT INTO hnl_w SELECT i % 500, i, repeat('w', 100) FROM generate_series(1, 2000) i;
CREATE TABLE hnl_big (x int, z int);
INSERT INTO hnl_big SELECT i, i % 7 FROM generate_series(1, 20000) i;
ANALYZE hnl_o;
ANALYZE hnl_t;
ANALYZE hnl_u;
ANALYZE hnl_w;
ANALYZE hnl_big;
SET enable_mergejoin = off;
SET enable_memoize = off;
SET enable_material = off;
SET max_parallel_workers_per_gather = 0;
-- build-side Param: the Hash is rebuilt per outer row (loops=3, rows per build)
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*), sum(i.x), sum(i.v)
FROM hnl_o o,
     LATERAL (SELECT t.x, u.v FROM hnl_t t
              JOIN (SELECT k, v FROM hnl_u WHERE v > o.k * 20) u ON u.k = t.x
              WHERE t.z < 3 OFFSET 0) i;
-- probe-side Param only: the single-batch table is kept (Hash loops=1 under a HashJoin at loops=3)
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*), sum(i.x), sum(i.v)
FROM hnl_o o,
     LATERAL (SELECT t.x, u.v FROM hnl_t t
              JOIN hnl_u u ON u.k = t.x
              WHERE t.z < o.k OFFSET 0) i;
-- join-filter Param: the table is kept as well (loops=1)
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*), sum(i.x), sum(i.v)
FROM hnl_o o,
     LATERAL (SELECT t.x, u.v FROM hnl_t t
              JOIN hnl_u u ON u.k = t.x AND u.v + t.z > o.k * 20
              WHERE t.z < 3 OFFSET 0) i;
-- multi-batch table under a probe-side Param: ExecReScanHashJoin destroys and
-- rebuilds it on every rescan (Hash loops=3 with no build-side Param)
SET work_mem = '64kB';
SET hash_mem_multiplier = 1.0;
EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF, SUMMARY OFF, BUFFERS OFF)
SELECT count(*), sum(i.x), sum(i.v)
FROM hnl_o o,
     LATERAL (SELECT t.x, w.v FROM hnl_big t
              JOIN hnl_w w ON w.k = t.x
              WHERE t.z < o.k OFFSET 0) i;
RESET work_mem;
RESET hash_mem_multiplier;
DROP TABLE hnl_o, hnl_t, hnl_u, hnl_w, hnl_big;
