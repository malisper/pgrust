-- EXPLAIN (COSTS ON) differential: the cost numbers are the product of
-- costsize/pathnode; every line must be byte-identical vs C 18.3.
-- Envelope: rows < 30000 keeps ANALYZE deterministic; no indexed-column
-- mergejoins (get_actual_variable_range is loud); no text hash keys.
SET compute_query_id = off;
SET max_parallel_workers_per_gather = 0;
SET jit = off;

CREATE TABLE ec_big(a int, b int, c numeric, d text);
INSERT INTO ec_big SELECT g, g % 97, (g % 13)::numeric / 7, 'row' || (g % 31)
  FROM generate_series(1, 20000) g;
CREATE INDEX ec_big_a ON ec_big(a);
CREATE INDEX ec_big_b ON ec_big(b);
CREATE TABLE ec_small(x int, y int, z text);
INSERT INTO ec_small SELECT g, g % 11, 'v' || g FROM generate_series(1, 500) g;
CREATE TABLE ec_dup(k int, v int);
INSERT INTO ec_dup SELECT g % 50, g FROM generate_series(1, 5000) g;
ANALYZE ec_big;
ANALYZE ec_small;
ANALYZE ec_dup;

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
EXPLAIN SELECT DISTINCT ON (b) b, a FROM ec_big ORDER BY b, a;
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
EXPLAIN SELECT count(*) FROM ec_big JOIN ec_dup ON ec_big.b = ec_dup.k;
EXPLAIN SELECT count(*) FROM ec_small s1, ec_small s2 WHERE s1.x = s2.y;
EXPLAIN SELECT count(*) FROM ec_small LEFT JOIN ec_dup ON ec_small.x = ec_dup.k;
EXPLAIN SELECT count(*) FROM ec_small s1, ec_small s2;
EXPLAIN SELECT * FROM ec_small WHERE EXISTS (SELECT 1 FROM ec_dup WHERE ec_dup.k = ec_small.x);
EXPLAIN SELECT * FROM ec_small WHERE NOT EXISTS (SELECT 1 FROM ec_dup WHERE ec_dup.k = ec_small.x);
EXPLAIN SELECT * FROM ec_small WHERE x IN (SELECT k FROM ec_dup);
EXPLAIN SELECT count(*) FROM ec_big b1 JOIN ec_big b2 ON b1.b = b2.b WHERE b1.a < 100;

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
EXPLAIN SELECT * FROM (SELECT b, count(*) AS n FROM ec_big GROUP BY b) sub WHERE n > 150;
EXPLAIN SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) v(n, s) WHERE n > 1;
EXPLAIN WITH t AS (SELECT b, count(*) AS n FROM ec_big GROUP BY b) SELECT * FROM t WHERE n > 150;
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

DROP TABLE ec_big, ec_small, ec_dup;
