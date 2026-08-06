-- CTE references across set-operation branches with grouping (issue #106):
-- planner used to panic 'bad levelsup for CTE' when the incremental-sort
-- costing of a set-op child needed the child's uplevel CTE reference.
-- Differential corpus vs stock PG 18: the formerly-failing shapes plus every
-- passing ingredient-boundary control from the issue, plus shadowed-CTE-name
-- cases proving levelsup-correct binding.

-- ==== formerly failing shapes ====

-- branches themselves carry GROUP BY, same CTE in both branches
WITH a AS (SELECT 1 AS x)
(SELECT x, count(*) FROM a GROUP BY x)
EXCEPT
(SELECT x, count(*) FROM a GROUP BY x);

-- ordered-agg + GROUP BY CTE pair, each referenced twice across nested
-- set-op branches (symmetric difference), 2-level chain
WITH a AS (SELECT 1 AS x),
 g AS (SELECT x, array_agg(x ORDER BY x) AS c FROM a GROUP BY x),
 h AS (SELECT x, array_agg(x ORDER BY x) AS c FROM a GROUP BY x)
SELECT 'l', x FROM ((SELECT x, c FROM g EXCEPT SELECT x, c FROM h)
                    UNION ALL
                    (SELECT x, c FROM h EXCEPT SELECT x, c FROM g)) z;

-- 3-level set-op chain over the same pair
WITH a AS (SELECT 1 AS x),
 g AS (SELECT x, array_agg(x ORDER BY x) AS c FROM a GROUP BY x),
 h AS (SELECT x, array_agg(x ORDER BY x) AS c FROM a GROUP BY x)
SELECT count(*) FROM (((SELECT x, c FROM g EXCEPT SELECT x, c FROM h)
                       UNION ALL
                       (SELECT x, c FROM h EXCEPT SELECT x, c FROM g))
                      UNION ALL
                      (SELECT x, c FROM g INTERSECT SELECT x, c FROM h)) z;

-- multi-row CTE body, GROUP BY branches, INTERSECT
WITH a AS (SELECT i % 3 AS x FROM generate_series(1, 9) i)
(SELECT x, count(*) FROM a GROUP BY x)
INTERSECT
(SELECT x, count(*) FROM a GROUP BY x)
ORDER BY 1, 2;

-- ==== passing controls (ingredient boundary from the issue) ====

-- same CTE twice in set-op branches, no GROUP BY
WITH a AS (SELECT 1 AS x)
(SELECT x FROM a) EXCEPT (SELECT x FROM a);

-- plain agg without GROUP BY in branches
WITH a AS (SELECT 1 AS x)
(SELECT count(*) FROM a) EXCEPT (SELECT count(*) FROM a);

-- DISTINCT instead of GROUP BY
WITH a AS (SELECT 1 AS x)
(SELECT DISTINCT x FROM a) EXCEPT (SELECT DISTINCT x FROM a);

-- single reference to an ordered-agg+GROUP-BY CTE
WITH a AS (SELECT 1 AS x),
 g AS (SELECT x, array_agg(x ORDER BY x) AS c FROM a GROUP BY x)
SELECT x, c FROM g;

-- unordered array_agg + GROUP BY CTE pair referenced twice each
WITH a AS (SELECT 1 AS x),
 g AS (SELECT x, array_agg(x) AS c FROM a GROUP BY x),
 h AS (SELECT x, array_agg(x) AS c FROM a GROUP BY x)
SELECT 'l', x FROM ((SELECT x, c FROM g EXCEPT SELECT x, c FROM h)
                    UNION ALL
                    (SELECT x, c FROM h EXCEPT SELECT x, c FROM g)) z;

-- same CTE twice with GROUP BY from two FROM-subqueries (join, no set op)
WITH a AS (SELECT 1 AS x)
SELECT l.x, r.n
FROM (SELECT x, count(*) AS n FROM a GROUP BY x) l,
     (SELECT x, count(*) AS n FROM a GROUP BY x) r
WHERE l.x = r.x;

-- ==== shadowed CTE names across levels: binding must follow ctelevelsup ====

-- inner WITH shadows outer 'a' in one branch only: left branch groups the
-- inner a (x = 2), right branch the outer a (x = 1)
WITH a AS (SELECT 1 AS x)
(SELECT x, count(*) FROM (WITH a AS (SELECT 2 AS x UNION ALL SELECT 2)
                          SELECT x FROM a) s GROUP BY x)
EXCEPT
(SELECT x, count(*) FROM a GROUP BY x)
ORDER BY 1, 2;

-- shadowing CTE referenced twice (materialized) inside set-op branches
-- nested within a branch that also groups; outer 'a' grouped in the other
-- branch of the same set-op
WITH a AS (SELECT 1 AS x)
(SELECT x, count(*)
 FROM (WITH a AS (SELECT 3 AS x)
       (SELECT x, count(*) FROM a GROUP BY x)
       EXCEPT
       (SELECT x, count(*) FROM a GROUP BY x HAVING count(*) > 1)) t(x, n)
 GROUP BY x)
UNION ALL
(SELECT x, count(*) FROM a GROUP BY x)
ORDER BY 1, 2;

-- both branches reference BOTH levels: outer 'a' (levelsup 2 from inside the
-- inner subquery) joined with the shadowing 'a'
WITH a AS (SELECT 1 AS x)
(SELECT x, count(*)
 FROM (WITH b AS (SELECT 10 AS x)
       SELECT b.x + a.x AS x FROM b, a) s GROUP BY x)
EXCEPT
(SELECT x + 10, count(*) FROM a GROUP BY x)
ORDER BY 1, 2;
