-- go-jet appbench lane (notes/appbench/jet.md, difference 2): an aggregate
-- whose arguments belong to a query two or more levels up (agglevelsup >= 2)
-- inside a FROM-subquery that pull_up_simple_subquery flattens. C's
-- IncrementVarSublevelsUp(subquery, -1, 1) adjusts Aggref.agglevelsup and
-- GroupingFunc.agglevelsup along with Var.varlevelsup; pgrust's functional
-- offset pass (offset_expr) and the deep-copy gate (query_has_uplevel_vars)
-- only knew Var / PlaceHolderVar / ReturningExpr, so the pulled-up Aggref kept
-- agglevelsup 2 and replace_outer_agg walked off the ancestor chain
-- ("agglevelsup 2 exceeds the ancestor chain"; "plan should not reference
-- subplan's variable" when wrapped in an outer FROM-subquery).

-- Minimal shape: depth 2.
SELECT (SELECT s FROM (SELECT SUM(g) AS s) r) FROM generate_series(1,3) g;

-- Depth 3.
SELECT (SELECT (SELECT s2 FROM (SELECT s AS s2) r2) FROM (SELECT SUM(g) AS s) r)
FROM generate_series(1,3) g;

-- Depth-2 aggregate that stays a SubqueryScan (OFFSET 0 blocks the pull-up):
-- the ancestor-chain walk itself.
SELECT (SELECT s FROM (SELECT SUM(g) AS s OFFSET 0) r) FROM generate_series(1,3) g;

-- Same with GROUP BY on the owning level.
SELECT g % 2 AS k, (SELECT s FROM (SELECT SUM(g) AS s) r)
FROM generate_series(1,7) g GROUP BY g % 2 ORDER BY 1;

-- Multiple outer-level aggregates in one pulled-up subquery.
SELECT g % 3 AS k,
       (SELECT row_to_json(r) FROM (SELECT SUM(g) AS s, AVG(g) AS a, COUNT(*) AS c) r)
FROM generate_series(1,10) g GROUP BY g % 3 ORDER BY 1;

-- jet's SELECT_JSON_ARR over SELECT_JSON_OBJ(SUM(...)) shape: the failing
-- query is itself a FROM-subquery of an outer json_agg.
SELECT json_agg(x ORDER BY k) FROM (SELECT g%3 AS k,
   (SELECT row_to_json(r) FROM (SELECT SUM(g) AS s) r)
 FROM generate_series(1,10) g GROUP BY g%3) x;

-- GroupingFunc two levels up, through the same pull-up.
SELECT k, (SELECT gr FROM (SELECT GROUPING(k) AS gr) r)
FROM (SELECT g % 2 AS k FROM generate_series(1,4) g) t GROUP BY ROLLUP (k) ORDER BY 1;

-- Mixed: an outer-level aggregate next to a local one in the pulled-up body.
SELECT (SELECT s + c FROM (SELECT SUM(g) AS s, COUNT(*) AS c) r)
FROM generate_series(1,3) g;

-- Uplevel aggregate inside a UNION ALL member (flatten_simple_union_all path).
SELECT (SELECT s FROM (SELECT SUM(g) AS s UNION ALL SELECT 0) r ORDER BY 1 DESC LIMIT 1)
FROM generate_series(1,3) g;

-- In a HAVING-side sublink.
SELECT g % 2 AS k FROM generate_series(1,10) g GROUP BY g % 2
HAVING (SELECT s FROM (SELECT sum(g) AS s) r) > 25 ORDER BY 1;

-- Plan shape: the Aggref becomes a SubPlan param of the owning Agg node.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT (SELECT s FROM (SELECT SUM(g) AS s) r) FROM generate_series(1,3) g;
