-- Issue #106: planner "bad levelsup for CTE" on ordered-aggregate GROUP-BY
-- CTEs referenced across set-operation branches. The child subroot's parent
-- link must stay climbable while build_setop_child_paths creates sorted
-- paths for a set-op child (C: rel->subroot's parent_root, prepunion.c;
-- examine_simple_variable, selfuncs.c).

-- 1. minimal repro from the issue (two-level nesting: leaf -> setop
--    subquery -> outer WITH; ordered agg + GROUP BY in the CTE bodies)
WITH a AS (SELECT 1 AS x),
 g AS (SELECT x, array_agg(x ORDER BY x) AS c FROM a GROUP BY x),
 h AS (SELECT x, array_agg(x ORDER BY x) AS c FROM a GROUP BY x)
SELECT 'l', x FROM ((SELECT x, c FROM g EXCEPT SELECT x, c FROM h)
                    UNION ALL
                    (SELECT x, c FROM h EXCEPT SELECT x, c FROM g)) z;

-- 2. branches themselves carry the GROUP BY (one-level nesting, plain agg)
WITH a AS (SELECT 1 AS x)
(SELECT x, count(*) FROM a GROUP BY x)
EXCEPT
(SELECT x, count(*) FROM a GROUP BY x);

-- 3. data-bearing asymmetric bodies: the EXCEPT branches return rows
WITH a AS (SELECT i % 5 AS x, i FROM generate_series(1, 40) i),
 g AS (SELECT x, array_agg(i ORDER BY i) AS c FROM a GROUP BY x),
 h AS (SELECT x, array_agg(i ORDER BY i) AS c FROM a WHERE i <= 35 GROUP BY x)
SELECT * FROM ((SELECT x, c FROM g EXCEPT SELECT x, c FROM h)
               UNION ALL
               (SELECT x, c FROM h EXCEPT SELECT x, c FROM g)) z
ORDER BY 1, 2;

-- 4. INTERSECT instead of EXCEPT
WITH a AS (SELECT i % 3 AS x, i FROM generate_series(1, 12) i),
 g AS (SELECT x, array_agg(i ORDER BY i) AS c FROM a GROUP BY x),
 h AS (SELECT x, array_agg(i ORDER BY i) AS c FROM a WHERE i <= 12 GROUP BY x)
SELECT * FROM ((SELECT x, c FROM g INTERSECT SELECT x, c FROM h)
               UNION ALL
               (SELECT x, c FROM h INTERSECT SELECT x, c FROM g)) z
ORDER BY 1, 2;

-- 5. UNION (dedup) of the two CTE references
WITH a AS (SELECT i % 4 AS x, i FROM generate_series(1, 20) i),
 g AS (SELECT x, array_agg(i ORDER BY i) AS c FROM a GROUP BY x),
 h AS (SELECT x, array_agg(i ORDER BY i) AS c FROM a WHERE i <= 16 GROUP BY x)
SELECT x, c FROM g UNION SELECT x, c FROM h ORDER BY 1, 2;

-- 6. deeper nesting: the set-op subquery wrapped in one more subquery level
WITH a AS (SELECT i % 3 AS x, i FROM generate_series(1, 9) i),
 g AS (SELECT x, array_agg(i ORDER BY i) AS c FROM a GROUP BY x),
 h AS (SELECT x, array_agg(i ORDER BY i) AS c FROM a WHERE i < 9 GROUP BY x)
SELECT * FROM (
  SELECT * FROM ((SELECT x, c FROM g EXCEPT SELECT x, c FROM h)
                 UNION ALL
                 (SELECT x, c FROM h EXCEPT SELECT x, c FROM g)) z1
) z2 ORDER BY 1, 2;

-- 7. unordered-agg control (never failed; must keep passing)
WITH a AS (SELECT i % 3 AS x, i FROM generate_series(1, 9) i),
 g AS (SELECT x, count(i) AS c FROM a GROUP BY x),
 h AS (SELECT x, count(i) AS c FROM a WHERE i < 9 GROUP BY x)
SELECT * FROM ((SELECT x, c FROM g EXCEPT SELECT x, c FROM h)
               UNION ALL
               (SELECT x, c FROM h EXCEPT SELECT x, c FROM g)) z
ORDER BY 1, 2;

-- 8. EXCEPT ALL / INTERSECT ALL set-op kinds
WITH a AS (SELECT i % 3 AS x, i FROM generate_series(1, 9) i),
 g AS (SELECT x, array_agg(i ORDER BY i) AS c FROM a GROUP BY x),
 h AS (SELECT x, array_agg(i ORDER BY i) AS c FROM a WHERE i < 9 GROUP BY x)
SELECT * FROM ((SELECT x, c FROM g EXCEPT ALL SELECT x, c FROM h)
               UNION ALL
               (SELECT x, c FROM g INTERSECT ALL SELECT x, c FROM h)) z
ORDER BY 1, 2;
