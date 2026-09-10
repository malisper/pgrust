-- go-jet appbench lane (notes/appbench/jet.md, difference 3): a SubLink as a
-- direct argument of an ordered-set aggregate. C compiles aggdirectargs with
-- ExecInitExprList(..., (PlanState *) aggstate) so SubPlans / initplan Params
-- in them resolve under the Agg node and evaluate at finalize time; pgrust's
-- port compiled them through a subplan-less entry point and evaluated them
-- through the driver-less kernel ("SubPlan step or pending-initplan
-- PARAM_EXEC fetch evaluated through a driver-less entry point").

-- Uncorrelated scalar sublink (initplan) direct argument.
SELECT percentile_disc((SELECT 0.5)) WITHIN GROUP (ORDER BY g) FROM generate_series(1,10) g;
SELECT percentile_cont((SELECT 0.5)) WITHIN GROUP (ORDER BY g) FROM generate_series(1,10) g;

-- Array form.
SELECT percentile_disc((SELECT array_agg(s) FROM generate_series(0,1,0.25) s))
  WITHIN GROUP (ORDER BY g) FROM generate_series(1,10) g;

-- With GROUP BY (sorted and hashed strategies both land on finalize).
SELECT g % 2 AS k, percentile_disc((SELECT 0.5)) WITHIN GROUP (ORDER BY g)
FROM generate_series(1,10) g GROUP BY g % 2 ORDER BY 1;

-- Grouping sets.
SELECT g % 2 AS k, percentile_cont((SELECT 0.5)) WITHIN GROUP (ORDER BY g)
FROM generate_series(1,10) g GROUP BY ROLLUP (g % 2) ORDER BY 1;

-- Hypothetical-set aggregate with a sublink direct argument.
SELECT rank((SELECT 5)) WITHIN GROUP (ORDER BY g) FROM generate_series(1,10) g;
SELECT dense_rank((SELECT 5)) WITHIN GROUP (ORDER BY g), mode() WITHIN GROUP (ORDER BY g % 3)
FROM generate_series(1,10) g;

-- Expression around the sublink; two direct-argument sublinks.
SELECT percentile_disc((SELECT 1) / 4.0) WITHIN GROUP (ORDER BY g) FROM generate_series(1,10) g;
SELECT percentile_disc(ARRAY[(SELECT 0.25), (SELECT 0.75)]) WITHIN GROUP (ORDER BY g)
FROM generate_series(1,10) g;

-- Correlated (per-group) sublink as a direct argument.
SELECT k, percentile_disc((SELECT k / 10.0)) WITHIN GROUP (ORDER BY g)
FROM (SELECT g, g % 5 + 1 AS k FROM generate_series(1,20) g) t GROUP BY k ORDER BY 1;

-- EXISTS / IN sublinks, and a sublink inside CASE, as direct arguments.
SELECT percentile_disc(CASE WHEN EXISTS (SELECT 1) THEN 0.5 ELSE 0.1 END) WITHIN GROUP (ORDER BY g)
FROM generate_series(1,10) g;
SELECT rank((SELECT 3) + 2) WITHIN GROUP (ORDER BY g) FROM generate_series(1,10) g;

-- Same class in the other per-aggregate expression lists: FILTER and plain
-- arguments carrying sublinks (transition-time evaluation).
SELECT sum(g) FILTER (WHERE g > (SELECT 2)) FROM generate_series(1,10) g;
SELECT sum((SELECT g)) FROM generate_series(1,10) g;
SELECT sum(g + (SELECT 1)) FROM generate_series(1,10) g;
SELECT g % 2 AS k, sum(g) FILTER (WHERE g > (SELECT 2)), count((SELECT g))
FROM generate_series(1,10) g GROUP BY g % 2 ORDER BY 1;
SELECT string_agg(g::text, (SELECT ',') ORDER BY g) FROM generate_series(1,5) g;
SELECT percentile_disc((SELECT 0.5)) WITHIN GROUP (ORDER BY g) FILTER (WHERE g > (SELECT 2))
FROM generate_series(1,10) g;

-- Plan shape: the direct argument is an InitPlan param of the Aggregate.
EXPLAIN (VERBOSE, COSTS OFF)
SELECT percentile_disc((SELECT 0.5)) WITHIN GROUP (ORDER BY g) FROM generate_series(1,10) g;
