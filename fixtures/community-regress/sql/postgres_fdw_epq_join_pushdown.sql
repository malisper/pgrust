-- postgres_fdw join pushdown under row locking / UPDATE / DELETE (walker-arms
-- audit, PR #2102 row "postgresGetForeignPlan T_HashJoin,T_MergeJoin,
-- T_NestLoop"): C keeps an EPQ-capable local join path
-- (GetExistingLocalJoinPath) as the ForeignPath's fdw_outerpath, so a
-- pushed-down foreign join can still be rechecked by EvalPlanQual. pgrust
-- used to refuse the pushdown whenever rowMarks / UPDATE / DELETE were
-- present and planned local joins instead. Loopback server on this very
-- backend (C postgres_fdw regress recipe). Expected output captured from C
-- PG 18.
CREATE EXTENSION postgres_fdw;
DO $d$
BEGIN
  EXECUTE format('CREATE SERVER epq_loopback FOREIGN DATA WRAPPER postgres_fdw OPTIONS (dbname %L, port %L, host %L)',
                 current_database(), current_setting('port'), current_setting('unix_socket_directories'));
  EXECUTE format('CREATE USER MAPPING FOR CURRENT_USER SERVER epq_loopback OPTIONS (user %L)', current_user);
END
$d$;
CREATE TABLE epq_t1 (c1 int PRIMARY KEY, c2 int, c3 text);
CREATE TABLE epq_t2 (c1 int PRIMARY KEY, c2 int, c3 text);
CREATE TABLE epq_lt (id int PRIMARY KEY, v int);
INSERT INTO epq_t1 SELECT i, i % 5, 'a' || i FROM generate_series(1, 20) i;
INSERT INTO epq_t2 SELECT i, i % 3, 'b' || i FROM generate_series(1, 20) i;
INSERT INTO epq_lt SELECT i, i * 10 FROM generate_series(1, 20) i;
ANALYZE epq_t1;
ANALYZE epq_t2;
ANALYZE epq_lt;
CREATE FOREIGN TABLE epq_ft1 (c1 int, c2 int, c3 text) SERVER epq_loopback OPTIONS (table_name 'epq_t1');
CREATE FOREIGN TABLE epq_ft2 (c1 int, c2 int, c3 text) SERVER epq_loopback OPTIONS (table_name 'epq_t2');
-- SELECT FOR SHARE: the join is pushed down with a local (EPQ) join subplan
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM epq_ft1 t1 JOIN epq_ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c2 = 1 ORDER BY t1.c1 FOR SHARE;
SELECT t1.c1, t2.c1 FROM epq_ft1 t1 JOIN epq_ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c2 = 1 ORDER BY t1.c1 FOR SHARE;
-- FOR UPDATE OF one side only
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c3 FROM epq_ft1 t1 JOIN epq_ft2 t2 ON (t1.c1 = t2.c1) WHERE t2.c2 = 0 ORDER BY t1.c1 FOR UPDATE OF t1;
SELECT t1.c1, t2.c3 FROM epq_ft1 t1 JOIN epq_ft2 t2 ON (t1.c1 = t2.c1) WHERE t2.c2 = 0 ORDER BY t1.c1 FOR UPDATE OF t1;
-- a local condition that stays on the ForeignScan is removed from the outer
-- plan's quals (postgresGetForeignPlan outer_plan fixup)
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM epq_ft1 t1 JOIN epq_ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c3 || t2.c3 LIKE 'a1%' ORDER BY t1.c1 FOR SHARE;
SELECT t1.c1, t2.c1 FROM epq_ft1 t1 JOIN epq_ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c3 || t2.c3 LIKE 'a1%' ORDER BY t1.c1 FOR SHARE;
-- local table locked, foreign join pushed down beside it (join order pinned
-- and the local join strategies disabled so the ForeignPath, which carries
-- no disabled nodes, is chosen and sits under the LockRows; the EPQ
-- alternative is the disabled local join)
SET join_collapse_limit = 1;
SET enable_mergejoin = off;
SET enable_hashjoin = off;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT lt.id, lt.v, f1.c1, f2.c3 FROM epq_lt lt JOIN (epq_ft1 f1 JOIN epq_ft2 f2 ON f2.c1 = f1.c1) ON f1.c1 = lt.id WHERE lt.id < 4 ORDER BY lt.id FOR UPDATE OF lt;
SELECT lt.id, lt.v, f1.c1, f2.c3 FROM epq_lt lt JOIN (epq_ft1 f1 JOIN epq_ft2 f2 ON f2.c1 = f1.c1) ON f1.c1 = lt.id WHERE lt.id < 4 ORDER BY lt.id FOR UPDATE OF lt;
RESET join_collapse_limit;
RESET enable_mergejoin;
RESET enable_hashjoin;
-- UPDATE / DELETE with a pushed-down foreign join in FROM / USING
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE epq_lt SET v = v + 1 FROM epq_ft1 f1 JOIN epq_ft2 f2 ON (f1.c1 = f2.c1) WHERE epq_lt.id = f1.c1 AND f2.c2 = 0;
UPDATE epq_lt SET v = v + 1 FROM epq_ft1 f1 JOIN epq_ft2 f2 ON (f1.c1 = f2.c1) WHERE epq_lt.id = f1.c1 AND f2.c2 = 0;
SELECT id, v FROM epq_lt WHERE v % 10 = 1 ORDER BY id;
EXPLAIN (VERBOSE, COSTS OFF)
DELETE FROM epq_lt USING epq_ft1 f1 JOIN epq_ft2 f2 ON (f1.c1 = f2.c1) WHERE epq_lt.id = f1.c1 AND f1.c2 = 4;
DELETE FROM epq_lt USING epq_ft1 f1 JOIN epq_ft2 f2 ON (f1.c1 = f2.c1) WHERE epq_lt.id = f1.c1 AND f1.c2 = 4;
SELECT count(*) FROM epq_lt;
-- merge-join EPQ alternative: presorted-key bookkeeping in GetExistingLocalJoinPath
SET enable_hashjoin = off;
SET enable_nestloop = off;
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1 FROM epq_ft1 t1 JOIN epq_ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c2 = 2 ORDER BY t1.c1 FOR UPDATE;
SELECT t1.c1, t2.c1 FROM epq_ft1 t1 JOIN epq_ft2 t2 ON (t1.c1 = t2.c1) WHERE t1.c2 = 2 ORDER BY t1.c1 FOR UPDATE;
-- three-way: a foreign join whose child is itself a pushed-down join
EXPLAIN (VERBOSE, COSTS OFF)
SELECT t1.c1, t2.c1, t3.c1 FROM epq_ft1 t1 JOIN epq_ft2 t2 ON (t1.c1 = t2.c1) JOIN epq_ft1 t3 ON (t3.c1 = t2.c1) WHERE t1.c2 = 3 ORDER BY t1.c1 FOR SHARE;
SELECT t1.c1, t2.c1, t3.c1 FROM epq_ft1 t1 JOIN epq_ft2 t2 ON (t1.c1 = t2.c1) JOIN epq_ft1 t3 ON (t3.c1 = t2.c1) WHERE t1.c2 = 3 ORDER BY t1.c1 FOR SHARE;
RESET enable_hashjoin;
RESET enable_nestloop;
DROP FOREIGN TABLE epq_ft1, epq_ft2;
DROP TABLE epq_t1, epq_t2, epq_lt;
DROP USER MAPPING FOR CURRENT_USER SERVER epq_loopback;
DROP SERVER epq_loopback;
DROP EXTENSION postgres_fdw;
