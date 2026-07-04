-- EXPLAIN (COSTS ON) differential: the cost numbers are the product of
-- costsize/pathnode; every line must be byte-identical vs C 18.3.
-- Envelope: rows < 30000 keeps ANALYZE deterministic; no indexed-column
-- mergejoins (get_actual_variable_range is loud); no text hash keys.
SET compute_query_id = off;
SET max_parallel_workers_per_gather = 0;
SET jit = off;

CREATE TABLE ec_big(a int, b int, c float8, d text);
INSERT INTO ec_big SELECT g, g % 97, (g % 13)::float8 / 7, 'row' || ((g % 31)::text)
  FROM generate_series(1, 20000) g;
CREATE TABLE ec_small(x int, y int, z text);
INSERT INTO ec_small SELECT g, g % 11, 'v' || (g::text) FROM generate_series(1, 500) g;
CREATE TABLE ec_dup(k int, v int);
INSERT INTO ec_dup SELECT g % 50, g FROM generate_series(1, 5000) g;
-- ANALYZE before CREATE INDEX (index-stats leg is loud); numeric column
-- stats are loud too (fmgr result-mcx), so c is float8.
ANALYZE ec_big;
ANALYZE ec_small;
ANALYZE ec_dup;
CREATE INDEX ec_big_a ON ec_big(a);
-- (b, a): unique composite keeps both btree builds posting-list-free
-- (build-time dedup is the nbt-dedup lane; duplicate-key indexes differ
-- physically until it lands)
CREATE INDEX ec_big_b ON ec_big(b, a);

-- scans
EXPLAIN SELECT * FROM ec_big;
EXPLAIN SELECT * FROM ec_big WHERE b < 10;
EXPLAIN SELECT * FROM ec_big WHERE b < 10 AND c > 0.5;
EXPLAIN SELECT a FROM ec_big WHERE a = 42;
EXPLAIN SELECT a FROM ec_big WHERE a BETWEEN 100 AND 200;
EXPLAIN SELECT * FROM ec_big WHERE a = 42 OR b = 3;
EXPLAIN SELECT * FROM ec_big WHERE a < 500 AND b = 3;
EXPLAIN SELECT * FROM ec_big WHERE d = 'row7';

-- sort / limit / distinct
EXPLAIN SELECT * FROM ec_big ORDER BY c;
EXPLAIN SELECT * FROM ec_big ORDER BY b LIMIT 10;
EXPLAIN SELECT * FROM ec_big ORDER BY b LIMIT 10 OFFSET 20;
EXPLAIN SELECT DISTINCT b FROM ec_big;
EXPLAIN SELECT a FROM ec_big ORDER BY a;
EXPLAIN SELECT * FROM ec_small ORDER BY y, x;

-- aggregates
EXPLAIN SELECT count(*) FROM ec_big;
EXPLAIN SELECT sum(a), avg(c) FROM ec_big WHERE b < 50;
EXPLAIN SELECT b, count(*) FROM ec_big GROUP BY b;
EXPLAIN SELECT b, count(*) FROM ec_big GROUP BY b HAVING count(*) > 100;
EXPLAIN SELECT b, count(*) FROM ec_big GROUP BY b ORDER BY b;
EXPLAIN SELECT y, max(x) FROM ec_small GROUP BY y HAVING max(x) > 100;
EXPLAIN SELECT min(a), max(a) FROM ec_big;

-- joins
EXPLAIN SELECT count(*) FROM ec_big, ec_small WHERE ec_big.b = ec_small.x;
EXPLAIN SELECT count(*) FROM ec_big, ec_dup WHERE ec_big.a = ec_dup.v;
-- two-sided MCV eqjoinsel
EXPLAIN SELECT count(*) FROM ec_dup d1, ec_dup d2 WHERE d1.v = d2.v;
EXPLAIN SELECT count(*) FROM ec_dup d1 WHERE EXISTS (SELECT 1 FROM ec_dup d2 WHERE d1.v = d2.v);
EXPLAIN SELECT count(*) FROM ec_small s1, ec_small s2 WHERE s1.x = s2.y;

EXPLAIN SELECT count(*) FROM ec_small s1, ec_small s2;
EXPLAIN SELECT * FROM ec_small WHERE NOT EXISTS (SELECT 1 FROM ec_dup WHERE ec_dup.k = ec_small.x);
EXPLAIN SELECT count(*) FROM ec_big b1, ec_big b2 WHERE b1.a = b2.a AND b1.b < 10;

-- forced join methods over the same query
SET enable_hashjoin = off;
EXPLAIN SELECT count(*) FROM ec_small s1, ec_small s2 WHERE s1.x = s2.y;
SET enable_mergejoin = off;
EXPLAIN SELECT count(*) FROM ec_small s1, ec_small s2 WHERE s1.x = s2.y;
RESET enable_hashjoin;
RESET enable_mergejoin;
SET enable_seqscan = off;
EXPLAIN SELECT * FROM ec_big WHERE b < 10;
RESET enable_seqscan;
SET enable_sort = off;
EXPLAIN SELECT b, count(*) FROM ec_big GROUP BY b ORDER BY b;
RESET enable_sort;

-- subqueries / values / cte / setops / append
EXPLAIN SELECT * FROM (SELECT b, count(*) AS n FROM ec_big GROUP BY b) sub;
-- (single-use CTE: C inlines it since PG12; pgrust's inline decision is the
-- prepjointree lane, so no CTE query here)
EXPLAIN SELECT x FROM ec_small UNION SELECT k FROM ec_dup;
EXPLAIN SELECT x FROM ec_small UNION ALL SELECT k FROM ec_dup;
EXPLAIN SELECT x FROM ec_small INTERSECT SELECT k FROM ec_dup;
EXPLAIN SELECT x FROM ec_small EXCEPT SELECT k FROM ec_dup;

-- UNION ALL flattening (pull_up_simple_union_all / flatten_simple_union_all);
-- ordered unions stay off indexed rels (MergeAppend/child-EC lane is loud)
EXPLAIN SELECT 1 UNION ALL SELECT 2;
EXPLAIN SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3;
EXPLAIN SELECT x FROM ec_small UNION ALL SELECT 42;
EXPLAIN SELECT x FROM ec_small UNION ALL SELECT k FROM ec_dup ORDER BY 1;
EXPLAIN SELECT x FROM ec_small WHERE y = 3 UNION ALL SELECT k FROM ec_dup WHERE v < 100;
EXPLAIN SELECT * FROM (SELECT x FROM ec_small UNION ALL SELECT k FROM ec_dup) u;
EXPLAIN SELECT count(*) FROM (SELECT x FROM ec_small UNION ALL SELECT k FROM ec_dup) u;
EXPLAIN SELECT * FROM (SELECT x, y FROM ec_small UNION ALL SELECT k, v FROM ec_dup) u WHERE u.x = 3;
EXPLAIN SELECT * FROM (SELECT 1 AS n UNION ALL SELECT 2) u;
EXPLAIN SELECT DISTINCT x FROM (SELECT x FROM ec_small UNION ALL SELECT k FROM ec_dup) u;
-- unpullable leaf keeps a Subquery Scan on "*SELECT* N"
EXPLAIN SELECT * FROM (SELECT DISTINCT x FROM ec_small UNION ALL SELECT k FROM ec_dup) u;
-- unindexed group column: GROUP BY sets query_pathkeys and an indexed child
-- would trip the (pre-existing) ordered-append/child-EC loud arm
EXPLAIN SELECT b, count(*) FROM (SELECT y FROM ec_small UNION ALL SELECT k FROM ec_dup) u(b) GROUP BY b;

-- window
EXPLAIN SELECT sum(x) OVER (PARTITION BY y) FROM ec_small;
EXPLAIN SELECT rank() OVER (ORDER BY x) FROM ec_small;
EXPLAIN SELECT sum(x) OVER (PARTITION BY y ORDER BY x ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM ec_small;

-- relation-size stability probe (tail queries once priced ec_big at ~16
-- pages; the head query must reprice identically here)
SELECT relpages, reltuples FROM pg_class WHERE relname = 'ec_big';
EXPLAIN SELECT * FROM ec_big;

-- expressions in targetlist / quals (cost_qual_eval coverage);
-- SAOP-indexqual and ARRAY[] grammar are loud lanes, kept out.
EXPLAIN SELECT a + b * 2, upper(d) FROM ec_big WHERE c IS NOT NULL;
EXPLAIN SELECT CASE WHEN b < 10 THEN 'lo' ELSE 'hi' END FROM ec_big WHERE d IN ('row1', 'row2');
EXPLAIN SELECT ec_big FROM ec_big WHERE a = 5;

-- multi-index choice + bitmap AND/OR selection
EXPLAIN SELECT * FROM ec_big WHERE a = 42 AND b = 3;
EXPLAIN SELECT * FROM ec_big WHERE a < 100 AND b < 5;
EXPLAIN SELECT * FROM ec_big WHERE a < 100 OR b > 90;
EXPLAIN SELECT * FROM ec_big WHERE (a < 50 AND b = 1) OR a > 19900;
EXPLAIN SELECT * FROM ec_big WHERE b = 5 AND c < 0.5;
EXPLAIN SELECT a FROM ec_big WHERE b = 5 ORDER BY a;
EXPLAIN SELECT * FROM ec_big WHERE 10 > b;

-- EC-implied equalities: const propagation and derived join clauses
EXPLAIN SELECT * FROM ec_big, ec_small WHERE ec_big.a = ec_small.x AND ec_small.x = 42;
EXPLAIN SELECT * FROM ec_big, ec_small WHERE ec_big.a = ec_small.x AND ec_big.a = 42;
EXPLAIN SELECT count(*) FROM ec_big b1, ec_big b2, ec_small s WHERE b1.a = b2.a AND b2.a = s.x;
EXPLAIN SELECT count(*) FROM ec_big, ec_dup, ec_small WHERE ec_big.a = ec_dup.v AND ec_dup.v = ec_small.x;
EXPLAIN SELECT count(*) FROM ec_small s1, ec_small s2, ec_dup d WHERE s1.x = s2.x AND s2.x = d.k AND d.v < 100;

-- join order beyond 2-way
EXPLAIN SELECT count(*) FROM ec_big b, ec_small s1, ec_small s2, ec_dup d
  WHERE b.a = s1.x AND s1.x = s2.x AND s2.x = d.v;
EXPLAIN SELECT count(*) FROM ec_small s1 JOIN ec_dup d ON s1.x = d.k JOIN ec_small s2 ON d.v = s2.x WHERE s1.y < 5;

-- parameterized index scan under nestloop
SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN SELECT count(*) FROM ec_small s, ec_big b WHERE b.a = s.x;
EXPLAIN SELECT count(*) FROM ec_small s, ec_big b WHERE b.a = s.x AND b.b < 20;
RESET enable_hashjoin;
RESET enable_mergejoin;

DROP TABLE ec_big, ec_small, ec_dup;

-- TID scan cost shapes (tidscan lane); ANALYZE first: unanalyzed
-- estimate_rel_size diverges from C (known off-lane residual)
CREATE TABLE ec_tid (id int, t text);
INSERT INTO ec_tid SELECT g, 'row' || g FROM generate_series(1, 1000) g;
ANALYZE ec_tid;
EXPLAIN SELECT * FROM ec_tid WHERE ctid = '(0,2)';
EXPLAIN SELECT * FROM ec_tid WHERE ctid = ANY(ARRAY['(0,1)','(1,3)']::tid[]);
EXPLAIN SELECT * FROM ec_tid WHERE ctid = '(0,1)' OR ctid = '(2,5)';
EXPLAIN SELECT * FROM ec_tid WHERE ctid > '(1,0)';
EXPLAIN SELECT * FROM ec_tid WHERE ctid >= '(0,5)' AND ctid < '(2,10)';
EXPLAIN SELECT * FROM ec_tid WHERE ctid = '(0,2)' AND id = 2;
EXPLAIN SELECT id FROM ec_tid WHERE ctid < '(1,1)' ORDER BY id;
DROP TABLE ec_tid;

-- nested-CTE references (parent-root resolution) + sublink-view pull-up
CREATE TABLE ec_ct(a int, b int);
INSERT INTO ec_ct SELECT g, g % 23 FROM generate_series(1, 3000) g;
CREATE TABLE ec_cd(k int, v int);
INSERT INTO ec_cd SELECT g % 40, g FROM generate_series(1, 2000) g;
ANALYZE ec_ct;
ANALYZE ec_cd;

-- CTE cross-reference: y's body reads x at ctelevelsup 1 while the outer
-- level is still mid-SS_process_ctes
EXPLAIN WITH x AS MATERIALIZED (SELECT a, b FROM ec_ct WHERE b < 7),
  y AS MATERIALIZED (SELECT a FROM x WHERE b = 3)
  SELECT count(*) FROM y;
EXPLAIN WITH x AS MATERIALIZED (SELECT a, b FROM ec_ct WHERE b < 7),
  y AS MATERIALIZED (SELECT x.a FROM x, ec_cd WHERE x.a = ec_cd.v)
  SELECT count(*) FROM x, y WHERE x.a = y.a;
-- outer-CTE reference from an unflattenable subquery (levelsup resolved
-- through the suspended chain during set_subquery_pathlist)
EXPLAIN WITH x AS MATERIALIZED (SELECT a, b FROM ec_ct WHERE b < 7)
  SELECT * FROM (SELECT a FROM x ORDER BY a LIMIT 5) sub;
EXPLAIN WITH x AS MATERIALIZED (SELECT a, b FROM ec_ct WHERE b < 7)
  SELECT * FROM (SELECT * FROM (SELECT a FROM x ORDER BY a LIMIT 5) s1 ORDER BY a DESC LIMIT 3) s2;

-- sublink views: pulled-up subquery bodies carrying SubLinks
CREATE VIEW ec_vw1 AS SELECT a, b FROM ec_ct
  WHERE EXISTS (SELECT 1 FROM ec_cd WHERE ec_cd.k = ec_ct.b);
EXPLAIN SELECT count(*) FROM ec_vw1 WHERE a < 100;
CREATE VIEW ec_vw2 AS SELECT a, b FROM ec_ct
  WHERE b IN (SELECT k FROM ec_cd WHERE v < 500);
EXPLAIN SELECT count(*) FROM ec_vw2;
CREATE VIEW ec_vw3 AS SELECT a, b FROM ec_ct
  WHERE NOT EXISTS (SELECT 1 FROM ec_cd WHERE ec_cd.k = ec_ct.b AND ec_cd.v > 1500);
EXPLAIN SELECT count(*) FROM ec_vw3 WHERE a BETWEEN 10 AND 200;
-- scalar sublink retained in the view tlist (substituted into the parent)
CREATE VIEW ec_vw4 AS SELECT a,
  (SELECT count(*) FROM ec_cd WHERE ec_cd.k = ec_ct.b) AS n FROM ec_ct;
EXPLAIN SELECT * FROM ec_vw4 WHERE a < 50;
-- view-over-view, sublinks at both levels
CREATE VIEW ec_vw5 AS SELECT a FROM ec_vw1 WHERE b IN (SELECT k FROM ec_cd);
EXPLAIN SELECT count(*) FROM ec_vw5;
DROP VIEW ec_vw5;
DROP VIEW ec_vw4;
DROP VIEW ec_vw3;
DROP VIEW ec_vw2;
DROP VIEW ec_vw1;
DROP TABLE ec_ct, ec_cd;

-- correlated + hashed sublink shapes (plan-init-subselect lane)
CREATE TABLE ec_sc(a int, b int);
INSERT INTO ec_sc SELECT g, g % 17 FROM generate_series(1, 2500) g;
CREATE TABLE ec_sd(k int, v int);
INSERT INTO ec_sd SELECT g % 50, g FROM generate_series(1, 1500) g;
ANALYZE ec_sc;
ANALYZE ec_sd;
-- correlated scalar sublink in the tlist (SubPlan with parParam/args)
EXPLAIN SELECT a, (SELECT max(v) FROM ec_sd WHERE ec_sd.k = ec_sc.b) FROM ec_sc WHERE a < 40;
-- correlated scalar sublink in an expression-context qual (CASE shell)
EXPLAIN SELECT a FROM ec_sc
  WHERE (CASE WHEN b > 5 THEN (SELECT min(v) FROM ec_sd WHERE ec_sd.k = ec_sc.b) ELSE 0 END) > 10;
-- hashed ANY (uncorrelated IN under OR keeps the SubPlan un-pulled)
EXPLAIN SELECT count(*) FROM ec_sc WHERE b IN (SELECT k FROM ec_sd) OR a < 0;
-- hashed NOT IN (unknownEqFalse hashnulls table)
EXPLAIN SELECT count(*) FROM ec_sc WHERE b NOT IN (SELECT k FROM ec_sd WHERE v < 900);
-- EXISTS-to-ANY hashed twin (AlternativeSubPlan choice at setrefs)
EXPLAIN SELECT count(*) FROM ec_sc
  WHERE EXISTS (SELECT 1 FROM ec_sd WHERE ec_sd.k = ec_sc.b) OR a < 0;
-- correlated ANY in a join filter expression context
EXPLAIN SELECT count(*) FROM ec_sc s1 JOIN ec_sd d1 ON s1.a = d1.v
  WHERE (CASE WHEN s1.b IN (SELECT k FROM ec_sd WHERE v <= s1.a) THEN 1 ELSE 2 END) = 1;
DROP TABLE ec_sc, ec_sd;

-- subquery qual pushdown (set_subquery_pathlist / subquery_is_pushdown_safe)
-- grouping-column qual pushes into the subquery (HAVING, then moved to WHERE)
EXPLAIN SELECT * FROM (SELECT b, count(*) AS n FROM ec_big GROUP BY b) sub WHERE sub.b < 10;
-- aggregate-output qual pushes into HAVING and stays there
EXPLAIN SELECT * FROM (SELECT b, count(*) AS n FROM ec_big GROUP BY b) sub WHERE sub.n > 100;
EXPLAIN SELECT * FROM (SELECT b, count(*) AS n FROM ec_big GROUP BY b) sub WHERE sub.b < 10 AND sub.n > 100;
-- LIMIT/OFFSET fence: qual stays a SubqueryScan filter
EXPLAIN SELECT * FROM (SELECT b, count(*) AS n FROM ec_big GROUP BY b LIMIT 20) sub WHERE sub.b < 10;
EXPLAIN SELECT * FROM (SELECT b, count(*) AS n FROM ec_big GROUP BY b OFFSET 5) sub WHERE sub.b < 10;
-- set-op branches: qual pushed into each UNION/INTERSECT arm
EXPLAIN SELECT * FROM (SELECT x AS q FROM ec_small UNION SELECT k FROM ec_dup) sub WHERE sub.q < 10;
EXPLAIN SELECT * FROM (SELECT x AS q FROM ec_small INTERSECT SELECT k FROM ec_dup) sub WHERE sub.q < 10;
-- EXCEPT fence
EXPLAIN SELECT * FROM (SELECT x AS q FROM ec_small EXCEPT SELECT k FROM ec_dup) sub WHERE sub.q < 10;
-- DISTINCT: nonvolatile qual pushes, volatile qual fences
EXPLAIN SELECT * FROM (SELECT DISTINCT b FROM ec_big) sub WHERE sub.b < 10;
EXPLAIN SELECT * FROM (SELECT DISTINCT b FROM ec_big) sub WHERE sub.b < random() * 20;
-- DISTINCT ON: qual on the DISTINCT column pushes, non-DISTINCT column fences
EXPLAIN SELECT * FROM (SELECT DISTINCT ON (b) b, a FROM ec_big ORDER BY b, a) sub WHERE sub.b < 10;
EXPLAIN SELECT * FROM (SELECT DISTINCT ON (b) b, a FROM ec_big ORDER BY b, a) sub WHERE sub.a < 100;
-- volatile output column fences quals that reference it
EXPLAIN SELECT * FROM (SELECT b, random() AS r FROM ec_big GROUP BY b) sub WHERE sub.r > 0.5;
-- window fences: PARTITION BY column pushes, other columns / wfunc output stay
EXPLAIN SELECT * FROM (SELECT y, x, sum(x) OVER (PARTITION BY y) AS s FROM ec_small) sub WHERE sub.y = 3;
EXPLAIN SELECT * FROM (SELECT y, x, sum(x) OVER (PARTITION BY y) AS s FROM ec_small) sub WHERE sub.x = 3;
EXPLAIN SELECT * FROM (SELECT y, x, sum(x) OVER (PARTITION BY y) AS s FROM ec_small) sub WHERE sub.s > 100;
-- security_barrier: leaky qual fences, leakproof operator pushes
CREATE VIEW ec_sb WITH (security_barrier) AS SELECT b, d FROM ec_big WHERE a < 15000;
EXPLAIN SELECT * FROM ec_sb WHERE b = 3;
EXPLAIN SELECT * FROM ec_sb WHERE d LIKE 'row1%';
DROP VIEW ec_sb;
-- remove_unused_subquery_outputs: unread aggregate outputs NULLed (width)
EXPLAIN SELECT sub.b FROM (SELECT b, count(*) AS n, sum(a) AS s FROM ec_big GROUP BY b) sub WHERE sub.b < 10;
