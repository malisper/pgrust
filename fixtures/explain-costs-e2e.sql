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
-- joins keep one side MCV-free (two-sided MCV eqjoinsel is a loud lane)
EXPLAIN SELECT count(*) FROM ec_big, ec_dup WHERE ec_big.a = ec_dup.v;
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
EXPLAIN SELECT count(*) FROM ec_big, ec_dup, ec_small WHERE ec_big.a = ec_dup.v AND ec_dup.k = ec_small.y;
EXPLAIN SELECT count(*) FROM ec_small s1, ec_small s2, ec_dup d WHERE s1.x = s2.x AND s2.x = d.k AND d.v < 100;

-- join order beyond 2-way
EXPLAIN SELECT count(*) FROM ec_big b, ec_small s1, ec_small s2, ec_dup d
  WHERE b.b = s1.x AND s1.x = s2.y AND s2.x = d.k;
EXPLAIN SELECT count(*) FROM ec_small s1 JOIN ec_dup d ON s1.x = d.k JOIN ec_small s2 ON d.v = s2.x WHERE s1.y < 5;

-- parameterized index scan under nestloop
SET enable_hashjoin = off;
SET enable_mergejoin = off;
EXPLAIN SELECT count(*) FROM ec_small s, ec_big b WHERE b.a = s.x;
EXPLAIN SELECT count(*) FROM ec_small s, ec_big b WHERE b.a = s.x AND b.b < 20;
RESET enable_hashjoin;
RESET enable_mergejoin;

DROP TABLE ec_big, ec_small, ec_dup;
