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

-- outer-join strength reduction (reduce_outer_joins): strict upper quals
-- turn LEFT/RIGHT/FULL into INNER/LEFT/RIGHT; IS NULL forcing turns LEFT
-- into ANTI
EXPLAIN SELECT count(*) FROM ec_small s LEFT JOIN ec_dup d ON s.x = d.k WHERE d.v < 100;
EXPLAIN SELECT count(*) FROM ec_small s LEFT JOIN ec_dup d ON s.x = d.k WHERE d.v IS NOT NULL;
EXPLAIN SELECT count(*) FROM ec_small s RIGHT JOIN ec_dup d ON s.x = d.k WHERE s.y = 3;
EXPLAIN SELECT count(*) FROM ec_small s LEFT JOIN ec_dup d ON s.x = d.k WHERE d.k IS NULL;
EXPLAIN SELECT count(*) FROM ec_small s FULL JOIN ec_dup d ON s.x = d.k WHERE s.y = 3 AND d.v < 100;
EXPLAIN SELECT count(*) FROM ec_small s FULL JOIN ec_dup d ON s.x = d.k WHERE s.y = 3;
EXPLAIN SELECT count(*) FROM ec_small s FULL JOIN ec_dup d ON s.x = d.k WHERE d.v < 100;
EXPLAIN SELECT count(*) FROM ec_small s1 LEFT JOIN (ec_small s2 LEFT JOIN ec_dup d ON s2.x = d.k) ON s1.x = s2.x WHERE d.v IS NOT NULL;
EXPLAIN SELECT count(*) FROM ec_small s FULL JOIN ec_dup d ON s.x = d.k;

-- RTE_RESULT removal (remove_useless_results_recurse): pulled-up empty-FROM
-- subqueries joined via explicit JOIN syntax
EXPLAIN SELECT count(*) FROM ec_small s, (SELECT 1 AS one) r;
EXPLAIN SELECT count(*) FROM ec_small s JOIN (SELECT 1 AS one) r ON s.y = r.one;
EXPLAIN SELECT count(*) FROM ec_small s JOIN (SELECT 2 AS two) r ON true;
EXPLAIN SELECT count(*) FROM ec_small s LEFT JOIN (SELECT 1 AS one) r ON true;
EXPLAIN SELECT count(*) FROM ec_small s LEFT JOIN (SELECT 1 AS one) r ON s.y = r.one;
EXPLAIN SELECT s.x, r.one FROM ec_small s JOIN (SELECT 1 AS one) r ON s.y = r.one WHERE s.x < 20;
EXPLAIN SELECT count(*) FROM (SELECT 1 AS one) r1 JOIN (SELECT 2 AS two) r2 ON r1.one < r2.two;
EXPLAIN SELECT count(*) FROM ec_small s JOIN (ec_dup d JOIN (SELECT 3 AS three) r ON d.k = r.three) ON s.x = d.k;

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

-- plain GROUP BY without aggregates (Group node; hashagg off forces the
-- sorted grouping leg)
SET enable_hashagg = off;
EXPLAIN SELECT b FROM ec_big GROUP BY b;
SELECT b FROM ec_big GROUP BY b ORDER BY b LIMIT 7;
EXPLAIN SELECT b, c FROM ec_big GROUP BY b, c ORDER BY b, c LIMIT 5;
SELECT b, c FROM ec_big GROUP BY b, c ORDER BY b, c LIMIT 5;
EXPLAIN SELECT b + 1 FROM ec_big GROUP BY b;
SELECT b + 1 FROM ec_big GROUP BY b ORDER BY 1 LIMIT 5;
EXPLAIN SELECT k FROM ec_dup GROUP BY k HAVING k < 5;
SELECT k FROM ec_dup GROUP BY k HAVING k < 5 ORDER BY k;
RESET enable_hashagg;

-- SubqueryScan execution (OFFSET 0 blocks pull-up; qual + projection above
-- the unflattened subquery)
EXPLAIN SELECT * FROM (SELECT y, x FROM ec_small OFFSET 0) s WHERE s.y < 3;
SELECT * FROM (SELECT y, x FROM ec_small OFFSET 0) s WHERE s.y < 3 ORDER BY x LIMIT 8;
SELECT count(*), sum(s.n) FROM (SELECT k, count(*) AS n FROM ec_dup GROUP BY k) s;
SELECT * FROM (SELECT DISTINCT x FROM ec_small UNION ALL SELECT k FROM ec_dup) u ORDER BY 1 LIMIT 6;

-- mergejoin with Materialized inner: mark/restore over the tuplestore
SET enable_hashjoin = off;
SET enable_nestloop = off;
EXPLAIN SELECT count(*) FROM ec_dup d1, ec_dup d2 WHERE d1.k = d2.k;
SELECT count(*) FROM ec_dup d1, ec_dup d2 WHERE d1.k = d2.k;
SELECT count(*), sum(d.v) FROM ec_small s, ec_dup d WHERE s.y = d.k;
RESET enable_hashjoin;
RESET enable_nestloop;

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
-- ordered partition shapes: MergeAppend vs ordered Append
CREATE TABLE ec_pr (a int, b int, c text) PARTITION BY RANGE (a);
CREATE TABLE ec_pr_p1 PARTITION OF ec_pr FOR VALUES FROM (0) TO (100);
CREATE TABLE ec_pr_p2 PARTITION OF ec_pr FOR VALUES FROM (100) TO (200);
CREATE TABLE ec_pr_p3 PARTITION OF ec_pr FOR VALUES FROM (200) TO (300);
INSERT INTO ec_pr SELECT i % 300, i % 17, 'v' || (i % 50) FROM generate_series(0, 2999) i;
CREATE INDEX ec_pr_a ON ec_pr (a);
CREATE INDEX ec_pr_b ON ec_pr (b);
ANALYZE ec_pr_p1; ANALYZE ec_pr_p2; ANALYZE ec_pr_p3;
-- partition-order match: ordered Append over child index scans
EXPLAIN SELECT * FROM ec_pr ORDER BY a LIMIT 10;
EXPLAIN SELECT * FROM ec_pr ORDER BY a;
EXPLAIN SELECT * FROM ec_pr ORDER BY a DESC LIMIT 10;
-- non-partition key: MergeAppend over child index scans
EXPLAIN SELECT * FROM ec_pr ORDER BY b LIMIT 10;
EXPLAIN SELECT * FROM ec_pr ORDER BY b;
EXPLAIN SELECT * FROM ec_pr ORDER BY b DESC LIMIT 10;
EXPLAIN SELECT * FROM ec_pr WHERE a < 150 ORDER BY b LIMIT 10;
-- multi-key ordering: sorts under MergeAppend
EXPLAIN SELECT * FROM ec_pr ORDER BY b, c LIMIT 10;
-- DEFAULT partition kills partition ordering
CREATE TABLE ec_pr_pd PARTITION OF ec_pr DEFAULT;
INSERT INTO ec_pr SELECT 300 + i FROM generate_series(0, 49) i;
ANALYZE ec_pr_pd;
EXPLAIN SELECT * FROM ec_pr ORDER BY a LIMIT 10;
-- LIST partitions: interleaved values force MergeAppend
CREATE TABLE ec_pl (a int, b int) PARTITION BY LIST (a);
CREATE TABLE ec_pl_p1 PARTITION OF ec_pl FOR VALUES IN (1, 3);
CREATE TABLE ec_pl_p2 PARTITION OF ec_pl FOR VALUES IN (2, 4);
INSERT INTO ec_pl SELECT 1 + i % 4, i FROM generate_series(0, 799) i;
CREATE INDEX ec_pl_a ON ec_pl (a);
ANALYZE ec_pl_p1; ANALYZE ec_pl_p2;
EXPLAIN SELECT * FROM ec_pl ORDER BY a LIMIT 10;
-- LIST partitions in order: plain ordered Append
CREATE TABLE ec_pl2 (a int, b int) PARTITION BY LIST (a);
CREATE TABLE ec_pl2_p1 PARTITION OF ec_pl2 FOR VALUES IN (1, 2);
CREATE TABLE ec_pl2_p2 PARTITION OF ec_pl2 FOR VALUES IN (3, 4);
INSERT INTO ec_pl2 SELECT 1 + i % 4, i FROM generate_series(0, 799) i;
CREATE INDEX ec_pl2_a ON ec_pl2 (a);
ANALYZE ec_pl2_p1; ANALYZE ec_pl2_p2;
EXPLAIN SELECT * FROM ec_pl2 ORDER BY a LIMIT 10;
-- sorted UNION (C: MergeAppend + Unique) needs convert_subquery_pathkeys
-- (tracked pathkeys gap): child ordered paths never surface, so the ordered
-- UNION arm stays dead and the shape would diverge; omitted here.
-- DROP of a table with a DEFAULT partition is the update_default_partition_oid
-- loud (heap.c lane); ec_pr is left in place.
DROP TABLE ec_pl2;
DROP TABLE ec_pl;

-- outer-join pullup: nulled Vars over pulled-up subqueries (plain-Var and
-- strict-expression outputs; PHV-requiring var-free outputs are separate)
EXPLAIN SELECT s.x, sub.v FROM ec_small s LEFT JOIN (SELECT k, v FROM ec_dup) sub ON s.x = sub.k;
EXPLAIN SELECT s.x, sub.v1 FROM ec_small s LEFT JOIN (SELECT k, v + 1 AS v1 FROM ec_dup) sub ON s.x = sub.k;
EXPLAIN SELECT s.x, sub.v1 FROM ec_small s LEFT JOIN (SELECT k, v + 1 AS v1 FROM ec_dup) sub ON s.x = sub.k WHERE sub.v1 IS NULL;
EXPLAIN SELECT sub.y, d.v FROM (SELECT x, y FROM ec_small) sub FULL JOIN ec_dup d ON sub.x = d.k;
EXPLAIN SELECT s.x, sub.v FROM ec_small s LEFT JOIN (SELECT k, v FROM ec_dup WHERE v > 10) sub ON s.x = sub.k ORDER BY sub.v LIMIT 5;
EXPLAIN SELECT s.x, s2.q FROM ec_small s LEFT JOIN (SELECT s1.x AS p, d.v AS q FROM ec_small s1 JOIN ec_dup d ON s1.x = d.k) s2 ON s.x = s2.p;
-- OJ identity 3: lower-OJ nullable side referenced by an upper OJ clause
EXPLAIN SELECT count(*) FROM ec_big a LEFT JOIN ec_small b ON a.b = b.x LEFT JOIN ec_dup c ON b.y = c.k;
EXPLAIN SELECT count(*) FROM ec_big a LEFT JOIN ec_small b ON a.b = b.x LEFT JOIN ec_dup c ON b.y = c.k WHERE c.v IS NULL;
EXPLAIN SELECT count(*) FROM ec_small a LEFT JOIN ec_dup b ON a.x = b.k LEFT JOIN ec_big c ON b.v = c.a AND a.y = c.b;
EXPLAIN SELECT count(*) FROM ec_small a LEFT JOIN (ec_dup b LEFT JOIN ec_big c ON b.v = c.a) ON a.x = b.k;

-- expression partition keys: static + runtime pruning shapes
CREATE TABLE ec_pe (a int, b int, c int) PARTITION BY RANGE (a, abs(b));
CREATE TABLE ec_pe_p1 PARTITION OF ec_pe FOR VALUES FROM (0, 0) TO (10, 10);
CREATE TABLE ec_pe_p2 PARTITION OF ec_pe FOR VALUES FROM (10, 10) TO (20, 20);
INSERT INTO ec_pe SELECT i % 20, i % 20, i FROM generate_series(0, 799) i;
ANALYZE ec_pe_p1; ANALYZE ec_pe_p2;
EXPLAIN SELECT * FROM ec_pe WHERE a = 5 AND abs(b) = 5;
EXPLAIN SELECT * FROM ec_pe WHERE a = 15 AND abs(b) = 15;
EXPLAIN SELECT * FROM ec_pe WHERE a = 15 AND abs(b) < 12;
-- no leading-key clause: no pruning
EXPLAIN SELECT * FROM ec_pe WHERE abs(b) = 5;
-- runtime (init) pruning over the expression key via a generic plan
PREPARE ec_pe_q (int, int) AS SELECT * FROM ec_pe WHERE a = $1 AND abs(b) = $2;
SET plan_cache_mode = force_generic_plan;
EXPLAIN EXECUTE ec_pe_q (5, 5);
RESET plan_cache_mode;
DEALLOCATE ec_pe_q;
-- boolean expression list key
CREATE TABLE ec_pb (a bool) PARTITION BY LIST ((NOT a));
CREATE TABLE ec_pb_t PARTITION OF ec_pb FOR VALUES IN (true);
CREATE TABLE ec_pb_f PARTITION OF ec_pb FOR VALUES IN (false);
INSERT INTO ec_pb SELECT i % 2 = 0 FROM generate_series(0, 99) i;
ANALYZE ec_pb_t; ANALYZE ec_pb_f;
EXPLAIN SELECT * FROM ec_pb WHERE NOT a;
EXPLAIN SELECT * FROM ec_pb WHERE a;
-- named opclass on a column partition key (ResolveOpClass)
CREATE TABLE ec_poc (b varchar, a int) PARTITION BY LIST (b varchar_ops);
CREATE TABLE ec_poc_ab PARTITION OF ec_poc FOR VALUES IN ('ab', 'cd');
CREATE TABLE ec_poc_ef PARTITION OF ec_poc FOR VALUES IN ('ef', 'gh');
INSERT INTO ec_poc SELECT case when i % 4 = 0 then 'ab' when i % 4 = 1 then 'cd' when i % 4 = 2 then 'ef' else 'gh' end, i FROM generate_series(0, 199) i;
ANALYZE ec_poc_ab; ANALYZE ec_poc_ef;
EXPLAIN SELECT * FROM ec_poc WHERE b = 'cd';
-- expression-keyed partition-wise join
SET enable_partitionwise_join = on;
CREATE TABLE ec_pe2 (a int, b int) PARTITION BY RANGE (a, abs(b));
CREATE TABLE ec_pe2_p1 PARTITION OF ec_pe2 FOR VALUES FROM (0, 0) TO (10, 10);
CREATE TABLE ec_pe2_p2 PARTITION OF ec_pe2 FOR VALUES FROM (10, 10) TO (20, 20);
INSERT INTO ec_pe2 SELECT i % 20, i % 20 FROM generate_series(0, 399) i;
ANALYZE ec_pe2_p1; ANALYZE ec_pe2_p2;
-- mergejoin off: the C-chosen MergeJoin sorts on abs(b), which needs
-- prepare_sort_from_pathkeys' resjunk sort-column injection (M2 lane);
-- the hash shape still pins partition-wise join over expression keys.
SET enable_mergejoin = off;
EXPLAIN SELECT * FROM ec_pe t1 JOIN ec_pe2 t2 ON t1.a = t2.a AND abs(t1.b) = abs(t2.b);
RESET enable_mergejoin;
RESET enable_partitionwise_join;
DROP TABLE ec_pe2;
DROP TABLE ec_poc;
DROP TABLE ec_pb;
DROP TABLE ec_pe;

-- tablefunc RTE selectivity: examine_simple_variable no-stats fallthrough
EXPLAIN SELECT * FROM XMLTABLE('/r/e' PASSING '<r><e><n>1</n></e><e><n>2</n></e></r>'::xml COLUMNS n int PATH 'n') xt WHERE n = 1;
EXPLAIN SELECT * FROM XMLTABLE('/r/e' PASSING '<r><e><n>1</n></e><e><n>2</n></e></r>'::xml COLUMNS n int PATH 'n') xt WHERE n > 1 AND n IS NOT NULL;
EXPLAIN SELECT * FROM ec_small s JOIN XMLTABLE('/r/e' PASSING '<r><e><n>1</n></e><e><n>2</n></e></r>'::xml COLUMNS n int PATH 'n') xt ON s.x = xt.n;
-- self-join elimination (analyzejoins.c remove_useless_self_joins)
CREATE TABLE ec_sj (a int UNIQUE NOT NULL, b int, c int NOT NULL);
CREATE UNIQUE INDEX ec_sj_bc ON ec_sj (b, c) NULLS NOT DISTINCT;
INSERT INTO ec_sj SELECT i, i % 17, i % 23 FROM generate_series(0, 499) i;
ANALYZE ec_sj;
EXPLAIN SELECT * FROM ec_sj t1, ec_sj t2 WHERE t1.a = t2.a;
EXPLAIN SELECT t1.b FROM ec_sj t1 JOIN ec_sj t2 ON t1.a = t2.a WHERE t2.c > 3;
EXPLAIN SELECT * FROM ec_sj t1, ec_sj t2 WHERE t1.b = t2.b AND t1.c = t2.c;
EXPLAIN SELECT * FROM ec_sj t1, ec_sj t2, ec_sj t3 WHERE t1.a = t2.a AND t2.a = t3.a;
EXPLAIN SELECT * FROM ec_sj t1, ec_sj t2 WHERE t1.a = t2.b;
EXPLAIN SELECT t1.a, (SELECT a FROM ec_sj WHERE a = t2.a AND a = t1.a) FROM ec_sj t1, ec_sj t2 WHERE t1.a = t2.a;
EXPLAIN SELECT * FROM ec_sj t1 JOIN ec_sj t2 ON t1.a = t2.a FOR UPDATE OF t1;
SET enable_self_join_elimination = off;
EXPLAIN SELECT * FROM ec_sj t1, ec_sj t2 WHERE t1.a = t2.a;
RESET enable_self_join_elimination;
-- redundant GROUP BY columns (initsplan.c remove_useless_groupby_columns)
CREATE TABLE ec_gb (pk int PRIMARY KEY, x int NOT NULL, y int, z int);
CREATE UNIQUE INDEX ec_gb_x ON ec_gb (x);
INSERT INTO ec_gb SELECT i, i, i % 7, i % 11 FROM generate_series(0, 499) i;
ANALYZE ec_gb;
EXPLAIN SELECT pk, y, count(*) FROM ec_gb GROUP BY pk, y, z;
EXPLAIN SELECT x, y, count(*) FROM ec_gb GROUP BY x, y;
EXPLAIN SELECT y, z, count(*) FROM ec_gb GROUP BY y, z;
DROP TABLE ec_gb;
DROP TABLE ec_sj;
-- partial (parallel) join path shapes: a partial join feeding a Gather (the
-- final-joinrel gather path). EXPLAIN-only, so no parallel executor is entered.
-- Projected (not aggregated) so the top is Gather-over-join and the comparison
-- pins the join-level parallel costing — get_parallel_divisor row scaling and
-- initial_cost_hashjoin's parallel-hash inner_rows_total undo — byte-for-byte
-- against C. (Two-phase parallel aggregation is a separate upperrel lane.)
SET max_parallel_workers_per_gather = 2;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
CREATE TABLE pj_a (id int, v int);
CREATE TABLE pj_b (id int, v int);
INSERT INTO pj_a SELECT i, i % 500 FROM generate_series(1, 30000) i;
INSERT INTO pj_b SELECT i, i % 500 FROM generate_series(1, 30000) i;
ANALYZE pj_a; ANALYZE pj_b;
-- default-on shape: Parallel Hash Join (shared table from a partial inner)
EXPLAIN SELECT a.id, b.id FROM pj_a a JOIN pj_b b ON a.v = b.v;
SET enable_parallel_hash = off;
EXPLAIN SELECT a.id, b.id FROM pj_a a JOIN pj_b b ON a.v = b.v;
RESET enable_parallel_hash;
SET enable_hashjoin = off;
EXPLAIN SELECT a.id, b.id FROM pj_a a JOIN pj_b b ON a.v = b.v;
RESET enable_hashjoin;
RESET min_parallel_table_scan_size;
RESET parallel_tuple_cost;
RESET parallel_setup_cost;
RESET max_parallel_workers_per_gather;
DROP TABLE pj_a;
DROP TABLE pj_b;

-- FK-based join selectivity (get_relation_foreign_keys ->
-- match_foreign_keys_to_quals -> get_foreign_key_join_selectivity): matched
-- FK clauses drop out of the restrictlist and estimate as 1/ref_tuples.
CREATE TABLE fk_ref (id int PRIMARY KEY, grp int);
CREATE TABLE fk_con (id int, ref_id int REFERENCES fk_ref(id), filler int);
INSERT INTO fk_ref SELECT i, i % 10 FROM generate_series(1, 1000) i;
INSERT INTO fk_con SELECT i, (i % 1000) + 1, i % 3 FROM generate_series(1, 5000) i;
ANALYZE fk_ref; ANALYZE fk_con;
EXPLAIN SELECT * FROM fk_con c JOIN fk_ref r ON c.ref_id = r.id;
EXPLAIN SELECT * FROM fk_con c JOIN fk_ref r ON c.ref_id = r.id WHERE r.grp = 3;
EXPLAIN SELECT * FROM fk_con c LEFT JOIN fk_ref r ON c.ref_id = r.id;
-- semi/anti: referenced rel exactly the inside -> rows/tuples leg
EXPLAIN SELECT * FROM fk_con c WHERE EXISTS (SELECT 1 FROM fk_ref r WHERE r.id = c.ref_id);
EXPLAIN SELECT * FROM fk_con c WHERE NOT EXISTS (SELECT 1 FROM fk_ref r WHERE r.id = c.ref_id);
EXPLAIN SELECT * FROM fk_con c WHERE EXISTS (SELECT 1 FROM fk_ref r WHERE r.id = c.ref_id AND r.grp < 4);
-- const-EC leg: "var = const" restriction divided back out of fkselec
EXPLAIN SELECT * FROM fk_con c JOIN fk_ref r ON c.ref_id = r.id WHERE r.id = 42;
-- multi-column FK: independent-clause selectivity replaced by FK semantics
CREATE TABLE fk_ref2 (a int, b int, PRIMARY KEY (a, b));
CREATE TABLE fk_con2 (a int, b int, v int, FOREIGN KEY (a, b) REFERENCES fk_ref2 (a, b));
INSERT INTO fk_ref2 SELECT i / 10, i % 10 FROM generate_series(0, 999) i;
INSERT INTO fk_con2 SELECT (i % 1000) / 10, i % 10, i FROM generate_series(0, 4999) i;
ANALYZE fk_ref2; ANALYZE fk_con2;
EXPLAIN SELECT * FROM fk_con2 c JOIN fk_ref2 r ON c.a = r.a AND c.b = r.b;
-- partially-matched multicolumn FK is dropped by match_foreign_keys_to_quals
EXPLAIN SELECT * FROM fk_con2 c JOIN fk_ref2 r ON c.a = r.a;
-- two FKs matched to one EC: second FK punts (shared EC-derived clause)
EXPLAIN SELECT * FROM fk_con c1 JOIN fk_con c2 ON c1.ref_id = c2.ref_id JOIN fk_ref r ON r.id = c1.ref_id;
DROP TABLE fk_con2; DROP TABLE fk_ref2;
DROP TABLE fk_con; DROP TABLE fk_ref;

-- parallel index scan / index-only scan cost parity (cost_index partial leg)
CREATE TABLE ec_pis (i int, j int, t text);
INSERT INTO ec_pis SELECT g, g % 100, (g % 1000)::text FROM generate_series(1, 30000) g;
CREATE INDEX ec_pis_i ON ec_pis (i);
VACUUM ANALYZE ec_pis;
SET max_parallel_workers_per_gather = 4;
SET parallel_setup_cost = 0;
SET parallel_tuple_cost = 0;
SET min_parallel_table_scan_size = 0;
SET min_parallel_index_scan_size = 0;
SET enable_seqscan = off;
SET enable_bitmapscan = off;
EXPLAIN SELECT * FROM ec_pis WHERE i > 1000;
EXPLAIN SELECT * FROM ec_pis WHERE i > 1000 ORDER BY i;
EXPLAIN SELECT * FROM ec_pis WHERE i < 29000 ORDER BY i DESC;
EXPLAIN SELECT i FROM ec_pis WHERE i > 1000;
EXPLAIN SELECT i FROM ec_pis WHERE i > 1000 ORDER BY i;
RESET enable_seqscan;
RESET enable_bitmapscan;
RESET max_parallel_workers_per_gather;
RESET parallel_setup_cost;
RESET parallel_tuple_cost;
RESET min_parallel_table_scan_size;
RESET min_parallel_index_scan_size;
DROP TABLE ec_pis;
