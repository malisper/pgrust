-- Plan-difference regression queries extracted from regression-history run 2026-05-04T2229Z.
-- Source diffs: https://github.com/your-github-org/pgrust/tree/regression-history/runs/2026-05-04T2229Z/diff
-- Usage: psql -X -v ON_ERROR_STOP=0 -f scripts/regression_plan_diff_queries_2026_05_04.sql
-- For fast dependency-aware EXPLAIN execution, use scripts/run_explain_regression_suite.sh.
-- For full source-regression execution, use scripts/run_regression_plan_diff_queries.sh.
-- Run this in a database prepared with the PostgreSQL regression setup/state for the referenced tests.
-- The script keeps going after errors so unsupported EXPLAIN forms remain visible.
\set ON_ERROR_STOP off
\pset pager off

\echo ==== aggregates ====
\echo ---- plan-diff 1: aggregates.diff:938 ----
-- Basic cases
explain (costs off)
  select min(unique1) from tenk1;

\echo ---- plan-diff 2: aggregates.diff:955 ----
explain (costs off)
  select max(unique1) from tenk1;

\echo ---- plan-diff 3: aggregates.diff:972 ----
explain (costs off)
  select max(unique1) from tenk1 where unique1 < 42;

\echo ---- plan-diff 4: aggregates.diff:989 ----
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42;

\echo ---- plan-diff 5: aggregates.diff:1012 ----
explain (costs off)
  select max(unique1) from tenk1 where unique1 > 42000;

\echo ---- plan-diff 6: aggregates.diff:1031 ----
-- multi-column index (uses tenk1_thous_tenthous)
explain (costs off)
  select max(tenthous) from tenk1 where thousand = 33;

\echo ---- plan-diff 7: aggregates.diff:1048 ----
explain (costs off)
  select min(tenthous) from tenk1 where thousand = 33;

\echo ---- plan-diff 8: aggregates.diff:1067 ----
-- check parameter propagation into an indexscan subquery
explain (costs off)
  select f1, (select min(unique1) from tenk1 where unique1 > f1) AS gt
    from int4_tbl;

\echo ---- plan-diff 9: aggregates.diff:1092 ----
-- check some cases that were handled incorrectly in 8.3.0
explain (costs off)
  select distinct max(unique2) from tenk1;

\echo ---- plan-diff 10: aggregates.diff:1111 ----
explain (costs off)
  select max(unique2) from tenk1 order by 1;

\echo ---- plan-diff 11: aggregates.diff:1130 ----
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2);

\echo ---- plan-diff 12: aggregates.diff:1149 ----
explain (costs off)
  select max(unique2) from tenk1 order by max(unique2)+1;

\echo ---- plan-diff 13: aggregates.diff:1168 ----
explain (costs off)
  select max(unique2), generate_series(1,3) as g from tenk1 order by g desc;

\echo ---- plan-diff 14: aggregates.diff:1191 ----
-- interesting corner case: constant gets optimized into a seqscan
explain (costs off)
  select max(100) from tenk1;

\echo ---- plan-diff 15: aggregates.diff:1222 ----
explain (costs off)
  select min(f1), max(f1) from minmaxtest;

\echo ---- plan-diff 16: aggregates.diff:1258 ----
-- DISTINCT doesn't do anything useful here, but it shouldn't fail
explain (costs off)
  select distinct min(f1), max(f1) from minmaxtest;

\echo ---- plan-diff 17: aggregates.diff:1305 ----
explain (costs off)
  select f1, (select distinct min(t1.f1) from int4_tbl t1 where t1.f1 = t0.f1)
  from int4_tbl t0;

\echo ---- plan-diff 18: aggregates.diff:1411 ----
-- Cannot optimize when PK is deferrable
explain (costs off) select * from t3 group by a,b,c;

\echo ---- plan-diff 19: aggregates.diff:1442 ----
-- Ensure we can remove non-PK columns for partitioned tables.
explain (costs off) select * from p_t1 group by a,b,c,d;

\echo ---- plan-diff 20: aggregates.diff:1465, aggregates.diff:1570 ----
explain (costs off) select y,z from t2 group by y,z;

\echo ---- plan-diff 21: aggregates.diff:1476 ----
-- When there are multiple supporting unique indexes and the GROUP BY contains
-- columns to cover all of those, ensure we pick the index with the least
-- number of columns so that we can remove more columns from the GROUP BY.
explain (costs off) select x,y,z from t2 group by x,y,z;

\echo ---- plan-diff 22: aggregates.diff:1486 ----
-- As above but try ordering the columns differently to ensure we get the
-- same result.
explain (costs off) select x,y,z from t2 group by z,x,y;

\echo ---- plan-diff 23: aggregates.diff:1585 ----
-- Ensure we order by four.  This suits the most aggregate functions.
explain (costs off)
select sum(two order by two),max(four order by four), min(four order by four)
from tenk1;

\echo ---- plan-diff 24: aggregates.diff:1685 ----
explain (costs off)
select sum(two order by two) from tenk1;

\echo ---- plan-diff 25: aggregates.diff:2172 ----
-- Ensure parallel aggregation is actually being used.
explain (costs off) select * from v_pagg_test order by y;

\echo ---- plan-diff 26: aggregates.diff:2947 ----
-- Utilize the ordering of index scan to avoid a Sort operation
EXPLAIN (COSTS OFF)
SELECT count(*) FROM btg GROUP BY y, x;

\echo ---- plan-diff 27: aggregates.diff:2971 ----
-- Engage incremental sort
EXPLAIN (COSTS OFF)
SELECT count(*) FROM btg GROUP BY z, y, w, x;

\echo ---- plan-diff 28: aggregates.diff:2988 ----
-- Utilize the ordering of subquery scan to avoid a Sort operation
EXPLAIN (COSTS OFF) SELECT count(*)
FROM (SELECT * FROM btg ORDER BY x, y, w, z) AS q1
GROUP BY w, x, z, y;

\echo ---- plan-diff 29: aggregates.diff:3008 ----
EXPLAIN (COSTS OFF)
SELECT count(*)
  FROM btg t1 JOIN btg t2 ON t1.w = t2.w AND t1.x = t2.x AND t1.z = t2.z
  GROUP BY t1.w, t1.z, t1.x;

\echo ---- plan-diff 30: aggregates.diff:3038 ----
-- Utilize incremental sort to make the ORDER BY rule a bit cheaper
EXPLAIN (COSTS OFF)
SELECT count(*) FROM btg GROUP BY w, x, y, z ORDER BY x*x, z;

\echo ---- plan-diff 31: aggregates.diff:3066 ----
EXPLAIN (VERBOSE, COSTS OFF)
SELECT y, x, array_agg(distinct w)
  FROM btg WHERE y < 0 GROUP BY x, y;

\echo ---- plan-diff 32: aggregates.diff:3101 ----
EXPLAIN (COSTS OFF)
SELECT avg(c1.f ORDER BY c1.x, c1.y)
FROM group_agg_pk c1 JOIN group_agg_pk c2 ON c1.x = c2.x
GROUP BY c1.w, c1.z;

\echo ---- plan-diff 33: aggregates.diff:3115, aggregates.diff:3125 ----
-- Pathkeys, built in a subtree, can be used to optimize GROUP-BY clause
-- ordering.  Also, here we check that it doesn't depend on the initial clause
-- order in the GROUP-BY list.
EXPLAIN (COSTS OFF)
SELECT c1.y,c1.x FROM group_agg_pk c1
  JOIN group_agg_pk c2
  ON c1.x = c2.x
GROUP BY c1.y,c1.x,c2.x;

\echo ---- plan-diff 34: aggregates.diff:3139, aggregates.diff:3158 ----
EXPLAIN (COSTS OFF)
SELECT c1.y,c1.x FROM group_agg_pk c1
  JOIN group_agg_pk c2
  ON c1.x = c2.x
GROUP BY c1.y,c2.x,c1.x;

\echo ---- plan-diff 35: aggregates.diff:3198 ----
EXPLAIN (COSTS OFF)
SELECT array_agg(c1 ORDER BY c2),c2
FROM agg_sort_order WHERE c2 < 100 GROUP BY c1 ORDER BY 2;

\echo ---- plan-diff 36: aggregates.diff:3268 ----
EXPLAIN (COSTS OFF) SELECT balk(hundred) FROM tenk1;

\echo ---- plan-diff 37: aggregates.diff:3306 ----
-- variance(int4) covers numeric_poly_combine
-- sum(int8) covers int8_avg_combine
-- regr_count(float8, float8) covers int8inc_float8_float8 and aggregates with > 1 arg
EXPLAIN (COSTS OFF, VERBOSE)
SELECT variance(unique1::int4), sum(unique1::int8), regr_count(unique1::float8, unique1::float8)
FROM (SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1
      UNION ALL SELECT * FROM tenk1) u;

\echo ---- plan-diff 38: aggregates.diff:3498 ----
explain (costs off)
select g%10000 as c1, sum(g::numeric) as c2, count(*) as c3
  from agg_data_20k group by g%10000;

\echo ==== alter_table ====
\echo ---- plan-diff 39: alter_table.diff:2593, alter_table.diff:2633 ----
explain (verbose, costs off) select * from at_view_2;

\echo ==== btree_index ====
\echo ---- plan-diff 40: btree_index.diff:154 ----
explain (costs off)
SELECT proname, proargtypes, pronamespace
   FROM pg_proc
   WHERE (proname, pronamespace) > ('abs', 0)
ORDER BY proname, proargtypes, pronamespace LIMIT 1;

\echo ---- plan-diff 41: btree_index.diff:179 ----
explain (costs off)
SELECT proname, proargtypes, pronamespace
   FROM pg_proc
   WHERE (proname, pronamespace) < ('abs', 1_000_000)
ORDER BY proname DESC, proargtypes DESC, pronamespace DESC LIMIT 1;

\echo ---- plan-diff 42: btree_index.diff:204 ----
explain (costs off)
SELECT proname, proargtypes, pronamespace
   FROM pg_proc
   WHERE (proname, proargtypes) >= ('abs', NULL) AND proname <= 'abs'
ORDER BY proname, proargtypes, pronamespace;

\echo ---- plan-diff 43: btree_index.diff:226 ----
explain (costs off)
SELECT proname, proargtypes, pronamespace
   FROM pg_proc
   WHERE proname >= 'abs' AND (proname, proargtypes) < ('abs', NULL)
ORDER BY proname, proargtypes, pronamespace;

\echo ---- plan-diff 44: btree_index.diff:249 ----
explain (costs off)
SELECT proname, proargtypes, pronamespace
   FROM pg_proc
   WHERE proname >= 'abs' AND (proname, proargtypes) <= ('abs', NULL)
ORDER BY proname DESC, proargtypes DESC, pronamespace DESC;

\echo ---- plan-diff 45: btree_index.diff:271 ----
explain (costs off)
SELECT proname, proargtypes, pronamespace
   FROM pg_proc
   WHERE (proname, proargtypes) > ('abs', NULL) AND proname <= 'abs'
ORDER BY proname DESC, proargtypes DESC, pronamespace DESC;

\echo ---- plan-diff 46: btree_index.diff:294 ----
-- Makes B-Tree preprocessing deal with unmarking redundant keys that were
-- initially marked required (test case relies on current row compare
-- preprocessing limitations)
explain (costs off)
SELECT proname, proargtypes, pronamespace
   FROM pg_proc
   WHERE proname = 'zzzzzz' AND (proname, proargtypes) > ('abs', NULL)
   AND pronamespace IN (1, 2, 3) AND proargtypes IN ('26 23', '5077')
ORDER BY proname, proargtypes, pronamespace;

\echo ==== collate ====
\echo ---- plan-diff 47: collate.diff:636 ----
-- EXPLAIN
EXPLAIN (COSTS OFF)
  SELECT * FROM collate_test10 ORDER BY x, y;

\echo ==== create_function_sql ====
\echo ---- plan-diff 48: create_function_sql.diff:598 ----
EXPLAIN (verbose, costs off) SELECT * FROM functest_sri1();

\echo ---- plan-diff 49: create_function_sql.diff:619, create_function_sql.diff:644 ----
EXPLAIN (verbose, costs off) SELECT * FROM functest_sri2();

\echo ==== create_index ====
\echo ---- plan-diff 50: create_index.diff:591 ----
EXPLAIN (COSTS OFF)
SELECT circle_center(f1), round(radius(f1)) as radius FROM gcircle_tbl ORDER BY f1 <-> '(200,300)'::point LIMIT 10;

\echo ---- plan-diff 51: create_index.diff:1909 ----
EXPLAIN (COSTS OFF)
SELECT * FROM tenk1
  WHERE thousand = 42 AND (tenthous = 1 OR tenthous = 3 OR tenthous = 42 OR tenthous = 0);

\echo ---- plan-diff 52: create_index.diff:1927 ----
EXPLAIN (COSTS OFF)
SELECT * FROM tenk1
  WHERE thousand = 42 AND (tenthous = 1 OR tenthous = (SELECT 1 + 2) OR tenthous = 42);

\echo ---- plan-diff 53: create_index.diff:1942, create_index.diff:1954 ----
EXPLAIN (COSTS OFF)
SELECT * FROM tenk1
  WHERE thousand = 42 AND (tenthous = 1 OR tenthous = 3 OR tenthous = 42 OR tenthous IS NULL);

\echo ---- plan-diff 54: create_index.diff:1966 ----
EXPLAIN (COSTS OFF)
SELECT * FROM tenk1
  WHERE thousand = 42 AND (tenthous = 1::int2 OR tenthous::int2 = 3::int8 OR tenthous = 42::int8);

\echo ---- plan-diff 55: create_index.diff:2040 ----
EXPLAIN (COSTS OFF)
SELECT * FROM tenk1
  WHERE thousand = 42 AND (tenthous = 1::numeric OR tenthous = 3::int4 OR tenthous = 42::numeric);

\echo ---- plan-diff 56: create_index.diff:2061 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM tenk1 t1
  WHERE t1.thousand = 42 OR t1.thousand = (SELECT t2.tenthous FROM tenk1 t2 WHERE t2.thousand = t1.tenthous + 1 LIMIT 1);

\echo ---- plan-diff 57: create_index.diff:2083 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM tenk1
  WHERE hundred = 42 AND (thousand = 42 OR thousand = 99);

\echo ---- plan-diff 58: create_index.diff:2127 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM tenk1
  WHERE thousand = 42 AND (tenthous = 1 OR tenthous = 3) OR thousand = 41;

\echo ---- plan-diff 59: create_index.diff:2156 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM tenk1
  WHERE hundred = 42 AND (thousand = 42 OR thousand = 99 OR tenthous < 2) OR thousand = 41;

\echo ---- plan-diff 60: create_index.diff:2183 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM tenk1
  WHERE hundred = 42 AND (thousand = 42 OR thousand = 41 OR thousand = 99 AND tenthous = 2);

\echo ---- plan-diff 61: create_index.diff:2220 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM tenk1, tenk2
  WHERE tenk1.hundred = 42 AND (tenk2.thousand = 42 OR tenk2.thousand = 41 OR tenk2.tenthous = 2) AND
  tenk2.hundred = tenk1.hundred;

\echo ---- plan-diff 62: create_index.diff:2392 ----
explain (costs off)
SELECT unique1 FROM tenk1 WHERE unique1 IN (1, 42, 7) and unique1 = ANY('{7, 8, 9}');

\echo ---- plan-diff 63: create_index.diff:2405 ----
explain (costs off)
SELECT unique1 FROM tenk1 WHERE unique1 = ANY('{7, 14, 22}') and unique1 = ANY('{33, 44}'::bigint[]);

\echo ---- plan-diff 64: create_index.diff:2541 ----
explain (costs off)
SELECT unique1 FROM tenk1 WHERE unique1 < 3 and unique1 < (-1)::bigint;

\echo ---- plan-diff 65: create_index.diff:2616 ----
-- Skip array preprocessing increments "thousand > -1" to  "thousand >= 0"
explain (costs off)
SELECT thousand, tenthous FROM tenk1
WHERE thousand > -1 AND tenthous IN (1001,3000)
ORDER BY thousand limit 2;

\echo ---- plan-diff 66: create_index.diff:3417 ----
-- No OR-clause groupings should happen, and there should be no clause
-- permutations in the filtering conditions we could see in the EXPLAIN.
EXPLAIN (COSTS OFF)
SELECT * FROM tenk1 WHERE unique1 < 1 OR hundred < 2;

\echo ---- plan-diff 67: create_index.diff:3441 ----
-- OR clauses in the 'unique1' column are grouped, so clause permutation
-- occurs. W e can see it in the 'Recheck Cond': the second clause involving
-- the 'unique1' column goes just after the first one.
EXPLAIN (COSTS OFF)
SELECT * FROM tenk1 WHERE unique1 < 1 OR unique1 < 3 OR hundred < 2;

\echo ---- plan-diff 68: create_index.diff:3459 ----
EXPLAIN (COSTS OFF)
SELECT * FROM bitmap_split_or WHERE (a = 1 OR a = 2) AND b = 2;

\echo ==== create_index_spgist ====
\echo ---- plan-diff 69: create_index_spgist.diff:603 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t = 'P0123456789abcdef';

\echo ---- plan-diff 70: create_index_spgist.diff:618 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t = 'P0123456789abcde';

\echo ---- plan-diff 71: create_index_spgist.diff:633 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t = 'P0123456789abcdefF';

\echo ---- plan-diff 72: create_index_spgist.diff:648 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t <    'Aztec                         Ct  ';

\echo ---- plan-diff 73: create_index_spgist.diff:663 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t ~<~  'Aztec                         Ct  ';

\echo ---- plan-diff 74: create_index_spgist.diff:678 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t <=   'Aztec                         Ct  ';

\echo ---- plan-diff 75: create_index_spgist.diff:693 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t ~<=~ 'Aztec                         Ct  ';

\echo ---- plan-diff 76: create_index_spgist.diff:708 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t =    'Aztec                         Ct  ';

\echo ---- plan-diff 77: create_index_spgist.diff:723 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t =    'Worth                         St  ';

\echo ---- plan-diff 78: create_index_spgist.diff:738 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t >=   'Worth                         St  ';

\echo ---- plan-diff 79: create_index_spgist.diff:753 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t ~>=~ 'Worth                         St  ';

\echo ---- plan-diff 80: create_index_spgist.diff:768 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t >    'Worth                         St  ';

\echo ---- plan-diff 81: create_index_spgist.diff:783 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t ~>~  'Worth                         St  ';

\echo ---- plan-diff 82: create_index_spgist.diff:798 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE t ^@	 'Worth';

\echo ---- plan-diff 83: create_index_spgist.diff:813 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM radix_text_tbl WHERE starts_with(t, 'Worth');

\echo ==== create_table ====
\echo ---- plan-diff 84: create_table.diff:340 ----
explain (costs off)
select * from partitioned where row(a,b)::partitioned = '(1,2)'::partitioned;

\echo ==== create_view ====
\echo ---- plan-diff 85: create_view.diff:1671 ----
-- ... and you can even EXPLAIN it ...
explain (verbose, costs off) select * from tt14v;

\echo ==== equivclass ====
\echo ---- plan-diff 86: equivclass.diff:97 ----
explain (costs off)
  select * from ec0 where ff = f1 and f1 = '42'::int8;

\echo ---- plan-diff 87: equivclass.diff:199 ----
explain (costs off)
  select * from ec1, ec2 where ff = x1 and x1 = '42'::int8alias2;

\echo ---- plan-diff 88: equivclass.diff:399 ----
-- without any RLS, we'll treat {a.ff, b.ff, 43} as an EquivalenceClass
explain (costs off)
  select * from ec0 a, ec1 b
  where a.ff = b.ff and a.ff = 43::bigint::int8alias1;

\echo ---- plan-diff 89: equivclass.diff:417 ----
-- with RLS active, the non-leakproof a.ff = 43 clause is not treated
-- as a suitable source for an EquivalenceClass; currently, this is true
-- even though the RLS clause has nothing to do directly with the EC
explain (costs off)
  select * from ec0 a, ec1 b
  where a.ff = b.ff and a.ff = 43::bigint::int8alias1;

\echo ---- plan-diff 90: equivclass.diff:431 ----
-- check that X=X is converted to X IS NOT NULL when appropriate
explain (costs off)
  select * from tenk1 where unique1 = unique1 and unique2 = unique2;

\echo ---- plan-diff 91: equivclass.diff:468 ----
-- this could be converted, but isn't at present
explain (costs off)
  select * from tenk1 where unique1 = unique1 or unique2 = unique2;

\echo ---- plan-diff 92: equivclass.diff:492 ----
explain (costs off)  -- this should not require a sort
  select * from overview where sqli = 'foo' order by sqli;

\echo ---- plan-diff 93: equivclass.diff:508 ----
explain (costs off)
select * from tbl_nocom t1 full join tbl_nocom t2 on t2.a = t1.b;

\echo ==== explain ====
\echo ---- plan-diff 94: explain.diff:254 ----
-- Check expansion of window definitions
select explain_filter('explain verbose select sum(unique1) over w, sum(unique2) over (w order by hundred), sum(tenthous) over (w order by hundred) from tenk1 window w as (partition by ten)');

\echo ---- plan-diff 95: explain.diff:273 ----
select explain_filter('explain verbose select sum(unique1) over w1, sum(unique2) over (w1 order by hundred), sum(tenthous) over (w1 order by hundred rows 10 preceding) from tenk1 window w1 as (partition by ten)');

\echo ---- plan-diff 96: explain.diff:368 ----
select explain_filter_to_json('explain (settings, format json) select * from int8_tbl i8') #> '{0,Settings,plan_cache_mode}';

\echo ---- plan-diff 97: explain.diff:530 ----
-- should scan gen_part_1_1 and gen_part_1_2, but not gen_part_2
select explain_filter('explain (generic_plan) select key1, key2 from gen_part where key1 = 1 and key2 = $1');

\echo ---- plan-diff 98: explain.diff:778 ----
-- Test tuplestore storage usage in Window aggregate (memory case)
select explain_filter('explain (analyze,buffers off,costs off) select sum(n) over() from generate_series(1,10) a(n)');

\echo ---- plan-diff 99: explain.diff:791 ----
select explain_filter('explain (analyze,buffers off,costs off) select sum(n) over() from generate_series(1,2500) a(n)');

\echo ==== fast_default ====
\echo ---- plan-diff 100: fast_default.diff:391 ----
EXPLAIN (VERBOSE TRUE, COSTS FALSE)
SELECT c_bigint, c_text FROM T WHERE c_bigint = -1 LIMIT 1;

\echo ---- plan-diff 101: fast_default.diff:488, fast_default.diff:496 ----
EXPLAIN (VERBOSE TRUE, COSTS FALSE)
SELECT * FROM T WHERE c_bigint > -1 ORDER BY c_bigint, c_text, pk LIMIT 10;

\echo ---- plan-diff 102: fast_default.diff:519 ----
EXPLAIN (VERBOSE TRUE, COSTS FALSE)
DELETE FROM T WHERE pk BETWEEN 10 AND 20 RETURNING *;

\echo ==== generated_stored ====
\echo ---- plan-diff 103: generated_stored.diff:726, generated_stored.diff:739 ----
EXPLAIN (COSTS OFF) SELECT * FROM gtest22c WHERE b = 4;

\echo ---- plan-diff 104: generated_stored.diff:752 ----
EXPLAIN (COSTS OFF) SELECT * FROM gtest22c WHERE b * 3 = 6;

\echo ---- plan-diff 105: generated_stored.diff:767 ----
EXPLAIN (COSTS OFF) SELECT * FROM gtest22c WHERE a = 1 AND b > 0;

\echo ---- plan-diff 106: generated_stored.diff:780 ----
EXPLAIN (COSTS OFF) SELECT * FROM gtest22c WHERE b = 8;

\echo ---- plan-diff 107: generated_stored.diff:793 ----
EXPLAIN (COSTS OFF) SELECT * FROM gtest22c WHERE b * 3 = 12;

\echo ==== generated_virtual ====
\echo ---- plan-diff 108: generated_virtual.diff:1552 ----
-- Ensure that outer-join removal functions correctly after the propagation of nullingrel bits
explain (costs off)
select t1.a from gtest32 t1 left join gtest32 t2 on t1.a = t2.a
where coalesce(t2.b, 1) = 2;

\echo ---- plan-diff 109: generated_virtual.diff:1572 ----
explain (costs off)
select t1.a from gtest32 t1 left join gtest32 t2 on t1.a = t2.a
where coalesce(t2.b, 1) = 2 or t1.a is null;

\echo ---- plan-diff 110: generated_virtual.diff:1592 ----
-- Ensure that the generation expressions are wrapped into PHVs if needed
explain (verbose, costs off)
select t2.* from gtest32 t1 left join gtest32 t2 on false;

\echo ---- plan-diff 111: generated_virtual.diff:1642 ----
-- should get a dummy Result, not a seq scan
explain (costs off)
select * from gtest33 where b < 10;

\echo ==== gin ====
\echo ---- plan-diff 112: gin.diff:114 ----
explain (costs off)
select * from t_gin_test_tbl where i @> '{}';

\echo ---- plan-diff 113: gin.diff:253 ----
explain (costs off)
select count(*) from t_gin_test_tbl where j @> '{}'::int[];

\echo ==== gist ====
\echo ---- plan-diff 114: gist.diff:208 ----
-- Also test an index-only knn-search
explain (costs off)
select b from gist_tbl where b <@ box(point(5,5), point(6,6))
order by b <-> point(5.2, 5.91);

\echo ---- plan-diff 115: gist.diff:246 ----
-- Check commuted case as well
explain (costs off)
select b from gist_tbl where b <@ box(point(5,5), point(6,6))
order by point(5.2, 5.91) <-> b;

\echo ---- plan-diff 116: gist.diff:323 ----
explain (verbose, costs off)
select circle(p,1) from gist_tbl
where p <@ box(point(5, 5), point(5.3, 5.3));

\echo ---- plan-diff 117: gist.diff:345 ----
-- Similarly, test that index rechecks involving a non-returnable column
-- are done correctly.
explain (verbose, costs off)
select p from gist_tbl where circle(p,1) @> circle(point(0,0),0.95);

\echo ---- plan-diff 118: gist.diff:361 ----
-- Also check that use_physical_tlist doesn't trigger in such cases.
explain (verbose, costs off)
select count(*) from gist_tbl;

\echo ---- plan-diff 119: gist.diff:377 ----
-- This case isn't supported, but it should at least EXPLAIN correctly.
explain (verbose, costs off)
select p from gist_tbl order by circle(p,1) <-> point(0,0) limit 1;

\echo ==== groupingsets ====
\echo ---- plan-diff 120: groupingsets.diff:684 ----
-- min max optimization should still work with GROUP BY ()
explain (costs off)
  select min(unique1) from tenk1 GROUP BY ();

\echo ---- plan-diff 121: groupingsets.diff:864 ----
-- Test reordering of grouping sets
explain (costs off)
select * from gstest1 group by grouping sets((a,b,v),(v)) order by v,b,a;

\echo ---- plan-diff 122: groupingsets.diff:947 ----
explain (costs off)
  select a,count(*) from gstest2 group by rollup(a) having a is distinct from 1 order by a;

\echo ---- plan-diff 123: groupingsets.diff:971 ----
explain (costs off)
  select v.c, (select count(*) from gstest2 group by () having v.c)
    from (values (false),(true)) v(c) order by v.c;

\echo ---- plan-diff 124: groupingsets.diff:1013 ----
explain (costs off)
select a, b, count(*) from gstest2 group by rollup(a), b having b > 1;

\echo ---- plan-diff 125: groupingsets.diff:1071 ----
explain (costs off)
select a, b, count(*) from gstest2 group by grouping sets ((a), (b)) having false;

\echo ---- plan-diff 126: groupingsets.diff:1295 ----
explain (costs off) select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by grouping sets ((a),(b)) order by 3,1,2;

\echo ---- plan-diff 127: groupingsets.diff:1329 ----
explain (costs off) select a, b, grouping(a,b), sum(v), count(*), max(v)
  from gstest1 group by cube(a,b) order by 3,1,2;

\echo ---- plan-diff 128: groupingsets.diff:1345 ----
-- shouldn't try and hash
explain (costs off)
  select a, b, grouping(a,b), array_agg(v order by v)
    from gstest1 group by cube(a,b);

\echo ---- plan-diff 129: groupingsets.diff:1607 ----
explain (costs off)
  select a, b, grouping(a,b), sum(v), count(*), max(v)
    from gstest1 group by grouping sets ((a,b),(a+1,b+1),(a+2,b+2)) order by 3,6;

\echo ---- plan-diff 130: groupingsets.diff:1635 ----
explain (costs off)
  select a, b, sum(c), sum(sum(c)) over (order by a,b) as rsum
    from gstest2 group by cube (a,b) order by rsum, a, b;

\echo ---- plan-diff 131: groupingsets.diff:1722 ----
EXPLAIN (COSTS OFF) SELECT a, b, count(*), max(a), max(b) FROM gstest3 GROUP BY GROUPING SETS(a, b,()) ORDER BY a, b;

\echo ---- plan-diff 132: groupingsets.diff:2341 ----
-- test handling of outer GroupingFunc within subqueries
explain (costs off)
select (select grouping(v1)) from (values ((select 1))) v(v1) group by cube(v1);

\echo ---- plan-diff 133: groupingsets.diff:2488 ----
-- test handling of expressions that should match lower target items
explain (costs off)
select a < b and b < 3 from (values (1, 2)) t(a, b) group by rollup(a < b and b < 3) having a < b and b < 3;

\echo ---- plan-diff 134: groupingsets.diff:2505 ----
explain (costs off)
select not a from (values(true)) t(a) group by rollup(not a) having not not a;

\echo ---- plan-diff 135: groupingsets.diff:2609 ----
explain (costs off)
select 1 as one group by rollup(one) order by one nulls first;

\echo ==== incremental_sort ====
\echo ---- plan-diff 136: incremental_sort.diff:4 ----
-- When there is a LIMIT clause, incremental sort is beneficial because
-- it only has to sort some of the groups, and not the entire table.
explain (costs off)
select * from (select * from tenk1 order by four) t order by four, ten
limit 1;

\echo ---- plan-diff 137: incremental_sort.diff:20 ----
explain (costs off)
select * from (select * from tenk1 order by four) t order by four, ten;

\echo ---- plan-diff 138: incremental_sort.diff:132 ----
begin
  execute 'explain (analyze, costs off, summary off, timing off, buffers off, format ''json'') ' || query into strict elements;

\echo ---- plan-diff 139: incremental_sort.diff:180, incremental_sort.diff:842, incremental_sort.diff:1158 ----
explain (costs off) select * from (select * from t order by a) s order by a, b limit 31;

\echo ---- plan-diff 140: incremental_sort.diff:229, incremental_sort.diff:891, incremental_sort.diff:1207 ----
explain (costs off) select * from (select * from t order by a) s order by a, b limit 32;

\echo ---- plan-diff 141: incremental_sort.diff:279, incremental_sort.diff:941, incremental_sort.diff:1257 ----
explain (costs off) select * from (select * from t order by a) s order by a, b limit 33;

\echo ---- plan-diff 142: incremental_sort.diff:361, incremental_sort.diff:1023, incremental_sort.diff:1339 ----
explain (costs off) select * from (select * from t order by a) s order by a, b limit 65;

\echo ---- plan-diff 143: incremental_sort.diff:448, incremental_sort.diff:1110 ----
explain (costs off) select * from (select * from t order by a) s order by a, b limit 66;

\echo ---- plan-diff 144: incremental_sort.diff:521 ----
-- Test EXPLAIN ANALYZE with only a fullsort group.
select explain_analyze_without_memory('select * from (select * from t order by a) s order by a, b limit 55');

\echo ---- plan-diff 145: incremental_sort.diff:670 ----
explain (costs off) select * from (select * from t order by a) s order by a, b limit 70;

\echo ---- plan-diff 146: incremental_sort.diff:702 ----
explain (costs off) select * from t left join (select * from (select * from t order by a) v order by a, b) s on s.a = t.a where t.a in (1, 2);

\echo ---- plan-diff 147: incremental_sort.diff:725 ----
-- Test EXPLAIN ANALYZE with both fullsort and presorted groups.
select explain_analyze_without_memory('select * from (select * from t order by a) s order by a, b limit 70');

\echo ---- plan-diff 148: incremental_sort.diff:1440, incremental_sort.diff:1451 ----
explain (costs off) select a,b,sum(c) from t group by 1,2 order by 1,2,3 limit 1;

\echo ---- plan-diff 149: incremental_sort.diff:1519 ----
-- Parallel sort below join.
explain (costs off) select distinct sub.unique1, stringu1
from tenk1, lateral (select tenk1.unique1 from generate_series(1, 1000)) as sub;

\echo ---- plan-diff 150: incremental_sort.diff:1707 ----
-- Ensure we get an incremental sort on the outer side of the mergejoin
explain (costs off)
select * from
  (select * from tenk1 order by four) t1 join tenk1 t2 on t1.four = t2.four and t1.two = t2.two
order by t1.four, t1.two limit 1;

\echo ==== index_including ====
\echo ---- plan-diff 151: index_including.diff:130 ----
explain (costs off)
select * from tbl where (c1,c2,c3) < (2,5,1);

\echo ---- plan-diff 152: index_including.diff:152 ----
explain (costs off)
select * from tbl where (c1,c2,c3) < (262,1,1) limit 1;

\echo ==== inherit ====
\echo ---- plan-diff 153: inherit.diff:556 ----
explain (costs off)
update some_tab set f3 = 11 where f1 = 12 and f2 = 13;

\echo ---- plan-diff 154: inherit.diff:698, inherit.diff:821 ----
-- modifies partition key, but no rows will actually be updated
explain update parted_tab set a = 2 where false;

\echo ---- plan-diff 155: inherit.diff:1595, inherit.diff:1620, inherit.diff:1668 ----
explain (costs off)
select * from patest0 join (select f1 from int4_tbl limit 1) ss on id = f1;

\echo ---- plan-diff 156: inherit.diff:1683, inherit.diff:1698 ----
set enable_indexscan = off;  -- force use of seqscan/sort, so no merge
explain (verbose, costs off) select * from matest0 order by 1-id;

\echo ---- plan-diff 157: inherit.diff:1723 ----
explain (verbose, costs off) select min(1-id) from matest0;

\echo ---- plan-diff 158: inherit.diff:1794 ----
explain (verbose, costs off)  -- bug #18652
select 1 - id as c from
(select id from matest3 t1 union all select id * 2 from matest3 t2) ss
order by c;

\echo ---- plan-diff 159: inherit.diff:1868 ----
explain (costs off)
select * from matest0 where a < 100 order by a;

\echo ---- plan-diff 160: inherit.diff:1895 ----
-- Check handling of duplicated, constant, or volatile targetlist items
explain (costs off)
SELECT thousand, tenthous FROM tenk1
UNION ALL
SELECT thousand, thousand FROM tenk1
ORDER BY thousand, tenthous;

\echo ---- plan-diff 161: inherit.diff:1910 ----
explain (costs off)
SELECT thousand, tenthous, thousand+tenthous AS x FROM tenk1
UNION ALL
SELECT 42, 42, hundred FROM tenk1
ORDER BY thousand, tenthous;

\echo ---- plan-diff 162: inherit.diff:1925 ----
explain (costs off)
SELECT thousand, tenthous FROM tenk1
UNION ALL
SELECT thousand, random()::integer FROM tenk1
ORDER BY thousand, tenthous;

\echo ---- plan-diff 163: inherit.diff:1940 ----
-- Check min/max aggregate optimization
explain (costs off)
SELECT min(x) FROM
  (SELECT unique1 AS x FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x FROM tenk1 b) s;

\echo ---- plan-diff 164: inherit.diff:1958 ----
explain (costs off)
SELECT min(y) FROM
  (SELECT unique1 AS x, unique1 AS y FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x, unique2 AS y FROM tenk1 b) s;

\echo ---- plan-diff 165: inherit.diff:1978 ----
-- XXX planner doesn't recognize that index on unique2 is sufficiently sorted
explain (costs off)
SELECT x, y FROM
  (SELECT thousand AS x, tenthous AS y FROM tenk1 a
   UNION ALL
   SELECT unique2 AS x, unique2 AS y FROM tenk1 b) s
ORDER BY x, y;

\echo ---- plan-diff 166: inherit.diff:2041, inherit.diff:2089 ----
explain (verbose, costs off)
update inhpar i set (f1, f2) = (select i.f1, i.f2 || '-' from int4_tbl limit 1);

\echo ---- plan-diff 167: inherit.diff:3034 ----
explain (costs off) select * from list_parted where a = 'ab';

\echo ---- plan-diff 168: inherit.diff:3076 ----
explain (costs off) select * from range_list_parted where a between 3 and 23 and b in ('ab');

\echo ---- plan-diff 169: inherit.diff:3275 ----
-- MergeAppend must be used when a default partition exists
explain (costs off) select * from mcrparted order by a, abs(b), c;

\echo ---- plan-diff 170: inherit.diff:3305, inherit.diff:3321 ----
-- Append is used with subpaths in reverse order with backwards index scans
explain (costs off) select * from mcrparted order by a desc, abs(b) desc, c desc;

\echo ---- plan-diff 171: inherit.diff:3347 ----
-- check that an Append plan is used and the sub-partitions are flattened
-- into the main Append when the sub-partition is unordered but contains
-- just a single sub-partition.
explain (costs off) select a, abs(b) from mcrparted order by a, abs(b), c;

\echo ---- plan-diff 172: inherit.diff:3390 ----
explain (costs off) select * from mclparted order by a;

\echo ---- plan-diff 173: inherit.diff:3426, inherit.diff:3435 ----
explain (costs off) select * from mclparted where a in(1,2,4) or a is null order by a;

\echo ---- plan-diff 174: inherit.diff:3445 ----
-- Ensure MergeAppend is used since 0 and NULLs are in the same partition.
explain (costs off) select * from mclparted where a in(1,2,4) or a is null order by a;

\echo ---- plan-diff 175: inherit.diff:3460 ----
explain (costs off) select * from mclparted where a in(0,1,2,4) order by a;

\echo ---- plan-diff 176: inherit.diff:3489, inherit.diff:3512 ----
-- Ensure MergeAppend is used when the default partition is not pruned
explain (costs off) select * from mclparted where a in(1,2,4,100) order by a;

\echo ---- plan-diff 177: inherit.diff:3533 ----
-- Ensure Append node can be used when the partition is ordered by some
-- pathkeys which were deemed redundant.
explain (costs off) select * from mcrparted where a = 10 order by a, abs(b), c;

\echo ---- plan-diff 178: inherit.diff:3566 ----
explain (costs off) select * from bool_rp where b = true order by b,a;

\echo ---- plan-diff 179: inherit.diff:3622 ----
explain (costs off) select * from range_parted order by a desc,b desc,c desc;

\echo ---- plan-diff 180: inherit.diff:3647 ----
-- without stats access, these queries would produce hash join plans:
explain (costs off)
  select * from permtest_parent p1 inner join permtest_parent p2
  on p1.a = p2.a and p1.c ~ 'a1$';

\echo ---- plan-diff 181: inherit.diff:3675 ----
explain (costs off)
  select p2.a, p1.c from permtest_parent p1 inner join permtest_parent p2
  on p1.a = p2.a and p1.c ~ 'a1$';

\echo ---- plan-diff 182: inherit.diff:3838 ----
explain (costs off)
select * from tuplesest_tab join
  (select b from tuplesest_parted where c < 100 group by b) sub
  on tuplesest_tab.a = sub.b;

\echo ==== insert_conflict ====
\echo ---- plan-diff 183: insert_conflict.diff:20 ----
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (fruit) do nothing;

\echo ---- plan-diff 184: insert_conflict.diff:61 ----
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (key, fruit) do update set fruit = excluded.fruit
  where exists (select 1 from insertconflicttest ii where ii.key = excluded.key);

\echo ---- plan-diff 185: insert_conflict.diff:129 ----
-- Okay, but only accepts the single index where both opclass and collation are
-- specified (plus expression variant)
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (lower(fruit) collate "C", key, key) do nothing;

\echo ---- plan-diff 186: insert_conflict.diff:159 ----
-- fails:
explain (costs off) insert into insertconflicttest values(0, 'Crowberry') on conflict (lower(fruit) text_pattern_ops, upper(fruit) collate "C") do nothing;

\echo ---- plan-diff 187: insert_conflict.diff:186 ----
explain (costs off) insert into insertconflicttest values (0, 'Bilberry') on conflict (key) do update set fruit = excluded.fruit;

\echo ---- plan-diff 188: insert_conflict.diff:213 ----
-- Does the same, but JSON format shows "Conflict Arbiter Index" as JSON array:
explain (costs off, format json) insert into insertconflicttest values (0, 'Bilberry') on conflict (key) do update set fruit = excluded.fruit where insertconflicttest.fruit != 'Lime' returning *;

\echo ==== join_hash ====
\echo ---- plan-diff 189: join_hash.diff:163, join_hash.diff:276 ----
explain (costs off)
  select count(*) from simple r join simple s using (id);

\echo ---- plan-diff 190: join_hash.diff:397 ----
explain (costs off)
  select count(*) from simple r join bigger_than_it_looks s using (id);

\echo ---- plan-diff 191: join_hash.diff:473, join_hash.diff:509 ----
explain (costs off)
  select count(*) from simple r join extremely_skewed s using (id);

\echo ---- plan-diff 192: join_hash.diff:797, join_hash.diff:819 ----
explain (costs off)
     select  count(*) from simple r full outer join simple s using (id);

\echo ---- plan-diff 193: join_hash.diff:848, join_hash.diff:868, join_hash.diff:890 ----
explain (costs off)
     select  count(*) from simple r full outer join simple s on (r.id = 0 - s.id);

\echo ---- plan-diff 194: join_hash.diff:923 ----
explain (costs off)
  select length(max(s.t))
  from wide left join (select id, coalesce(t, '') || '' as t from wide) s using (id);

\echo ==== limit ====
\echo ---- plan-diff 195: limit.diff:369 ----
explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by unique2 limit 10;

\echo ---- plan-diff 196: limit.diff:402 ----
explain (verbose, costs off)
select unique1, unique2, nextval('testseq')
  from tenk1 order by tenthous limit 10;

\echo ---- plan-diff 197: limit.diff:440 ----
explain (verbose, costs off)
select unique1, unique2, generate_series(1,10)
  from tenk1 order by unique2 limit 7;

\echo ---- plan-diff 198: limit.diff:466 ----
explain (verbose, costs off)
select unique1, unique2, generate_series(1,10)
  from tenk1 order by tenthous limit 7;

\echo ---- plan-diff 199: limit.diff:495 ----
-- use of random() is to keep planner from folding the expressions together
explain (verbose, costs off)
select generate_series(0,2) as s1, generate_series((random()*.1)::int,2) as s2;

\echo ---- plan-diff 200: limit.diff:513 ----
explain (verbose, costs off)
select generate_series(0,2) as s1, generate_series((random()*.1)::int,2) as s2
order by s2 desc;

\echo ---- plan-diff 201: limit.diff:536 ----
-- test for failure to set all aggregates' aggtranstype
explain (verbose, costs off)
select sum(tenthous) as s1, sum(tenthous) + random()*0 as s2
  from tenk1 group by thousand order by thousand limit 3;

\echo ==== matview ====
\echo ---- plan-diff 202: matview.diff:104, matview.diff:116, matview.diff:125 ----
EXPLAIN (costs off)
  CREATE MATERIALIZED VIEW mvtest_tvvm AS SELECT * FROM mvtest_tvv;

\echo ---- plan-diff 203: matview.diff:624 ----
EXPLAIN (ANALYZE, COSTS OFF, SUMMARY OFF, TIMING OFF, BUFFERS OFF)
  CREATE MATERIALIZED VIEW matview_schema.mv_withdata2 (a) AS
  SELECT generate_series(1, 10) WITH DATA;

\echo ==== memoize ====
\echo ---- plan-diff 204: memoize.diff:425 ----
-- Exercise Memoize code that flushes the cache when a parameter changes which
-- is not part of the cache key.
-- Ensure we get a Memoize plan
EXPLAIN (COSTS OFF)
SELECT unique1 FROM tenk1 t0
WHERE unique1 < 3
  AND EXISTS (
	SELECT 1 FROM tenk1 t1
	INNER JOIN tenk1 t2 ON t1.unique1 = t2.hundred
	WHERE t0.ten = t1.twenty AND t0.two <> t2.four OFFSET 0);

\echo ==== misc_functions ====
\echo ---- plan-diff 205: misc_functions.diff:606 ----
EXPLAIN (COSTS OFF)
SELECT * FROM tenk1 a JOIN tenk1 b ON a.unique1 = b.unique1
WHERE my_int_eq(a.unique2, 42);

\echo ---- plan-diff 206: misc_functions.diff:622 ----
EXPLAIN (COSTS OFF)
SELECT * FROM tenk1 a JOIN my_gen_series(1,1000) g ON a.unique1 = g;

\echo ==== partition_aggregate ====
\echo ---- plan-diff 207: partition_aggregate.diff:29 ----
-- When GROUP BY clause matches; full aggregation is performed for each partition.
EXPLAIN (COSTS OFF)
SELECT c, sum(a), avg(b), count(*), min(a), max(b) FROM pagg_tab GROUP BY c HAVING avg(d) < 15 ORDER BY 1, 2, 3;

\echo ---- plan-diff 208: partition_aggregate.diff:96 ----
-- Check with multiple columns in GROUP BY
EXPLAIN (COSTS OFF)
SELECT a, c, count(*) FROM pagg_tab GROUP BY a, c;

\echo ---- plan-diff 209: partition_aggregate.diff:179 ----
-- When GROUP BY clause matches full aggregation is performed for each partition.
EXPLAIN (COSTS OFF)
SELECT c, sum(a), avg(b), count(*) FROM pagg_tab GROUP BY 1 HAVING avg(d) < 15 ORDER BY 1, 2, 3;

\echo ---- plan-diff 210: partition_aggregate.diff:232 ----
-- When GROUP BY clause does not match; partial aggregation is performed for each partition.
EXPLAIN (COSTS OFF)
SELECT a, sum(b), avg(b), count(*) FROM pagg_tab GROUP BY 1 HAVING avg(d) < 15 ORDER BY 1, 2, 3;

\echo ---- plan-diff 211: partition_aggregate.diff:262, partition_aggregate.diff:271 ----
-- Test partitionwise grouping without any aggregates
EXPLAIN (COSTS OFF)
SELECT c FROM pagg_tab GROUP BY c ORDER BY 1;

\echo ---- plan-diff 212: partition_aggregate.diff:302, partition_aggregate.diff:316 ----
EXPLAIN (COSTS OFF)
SELECT a FROM pagg_tab WHERE a < 3 GROUP BY a ORDER BY 1;

\echo ---- plan-diff 213: partition_aggregate.diff:339 ----
-- Test partitionwise aggregation with ordered append path built from fractional paths
EXPLAIN (COSTS OFF)
SELECT count(*) FROM pagg_tab GROUP BY c ORDER BY c LIMIT 1;

\echo ---- plan-diff 214: partition_aggregate.diff:373 ----
-- ROLLUP, partitionwise aggregation does not apply
EXPLAIN (COSTS OFF)
SELECT c, sum(a) FROM pagg_tab GROUP BY rollup(c) ORDER BY 1, 2;

\echo ---- plan-diff 215: partition_aggregate.diff:390 ----
-- ORDERED SET within the aggregate.
-- Full aggregation; since all the rows that belong to the same group come
-- from the same partition, having an ORDER BY within the aggregate doesn't
-- make any difference.
EXPLAIN (COSTS OFF)
SELECT c, sum(b order by a) FROM pagg_tab GROUP BY c ORDER BY 1, 2;

\echo ---- plan-diff 216: partition_aggregate.diff:417 ----
-- Since GROUP BY clause does not match with PARTITION KEY; we need to do
-- partial aggregation. However, ORDERED SET are not partial safe and thus
-- partitionwise aggregation plan is not generated.
EXPLAIN (COSTS OFF)
SELECT a, sum(b order by a) FROM pagg_tab GROUP BY a ORDER BY 1, 2;

\echo ---- plan-diff 217: partition_aggregate.diff:490 ----
-- Check with whole-row reference; partitionwise aggregation does not apply
EXPLAIN (COSTS OFF)
SELECT t1.x, sum(t1.y), count(t1) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.x ORDER BY 1, 2, 3;

\echo ---- plan-diff 218: partition_aggregate.diff:562, partition_aggregate.diff:571 ----
EXPLAIN (COSTS OFF)
SELECT t1.y, sum(t1.x), count(*) FROM pagg_tab1 t1, pagg_tab2 t2 WHERE t1.x = t2.y GROUP BY t1.y HAVING avg(t1.x) > 10 ORDER BY 1, 2, 3;

\echo ---- plan-diff 219: partition_aggregate.diff:701, partition_aggregate.diff:709 ----
-- FULL JOIN, should produce partial partitionwise aggregation plan as
-- GROUP BY is on nullable column
EXPLAIN (COSTS OFF)
SELECT a.x, sum(b.x) FROM pagg_tab1 a FULL OUTER JOIN pagg_tab2 b ON a.x = b.y GROUP BY a.x ORDER BY 1 NULLS LAST;

\echo ---- plan-diff 220: partition_aggregate.diff:803 ----
-- FULL JOIN, with dummy relations on both sides, ideally
-- should produce partial partitionwise aggregation plan as GROUP BY is on
-- nullable columns.
-- But right now we are unable to do partitionwise join in this case.
EXPLAIN (COSTS OFF)
SELECT a.x, b.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x < 20) a FULL JOIN (SELECT * FROM pagg_tab2 WHERE y > 10) b ON a.x = b.y WHERE a.x > 5 or b.y < 20  GROUP BY a.x, b.y ORDER BY 1, 2;

\echo ---- plan-diff 221: partition_aggregate.diff:836 ----
-- Empty join relation because of empty outer side, no partitionwise agg plan
EXPLAIN (COSTS OFF)
SELECT a.x, a.y, count(*) FROM (SELECT * FROM pagg_tab1 WHERE x = 1 AND x = 2) a LEFT JOIN pagg_tab2 b ON a.x = b.y GROUP BY a.x, a.y ORDER BY 1, 2;

\echo ---- plan-diff 222: partition_aggregate.diff:933 ----
-- Full aggregation as PARTITION KEY is part of GROUP BY clause
EXPLAIN (COSTS OFF)
SELECT a, c, sum(b), avg(c), count(*) FROM pagg_tab_m GROUP BY (a+b)/2, 2, 1 HAVING sum(b) = 50 AND avg(c) > 25 ORDER BY 1, 2, 3;

\echo ---- plan-diff 223: partition_aggregate.diff:983, partition_aggregate.diff:993 ----
-- Full aggregation at level 1 as GROUP BY clause matches with PARTITION KEY
-- for level 1 only. For subpartitions, GROUP BY clause does not match with
-- PARTITION KEY, but still we do not see a partial aggregation as array_agg()
-- is not partial agg safe.
EXPLAIN (COSTS OFF)
SELECT a, sum(b), array_agg(distinct c), count(*) FROM pagg_tab_ml GROUP BY a HAVING avg(b) < 3 ORDER BY 1, 2, 3;

\echo ---- plan-diff 224: partition_aggregate.diff:1028, partition_aggregate.diff:1039 ----
-- Without ORDER BY clause, to test Gather at top-most path
EXPLAIN (COSTS OFF)
SELECT a, sum(b), array_agg(distinct c), count(*) FROM pagg_tab_ml GROUP BY a HAVING avg(b) < 3;

\echo ---- plan-diff 225: partition_aggregate.diff:1072, partition_aggregate.diff:1084, partition_aggregate.diff:1207 ----
-- Full aggregation at level 1 as GROUP BY clause matches with PARTITION KEY
-- for level 1 only. For subpartitions, GROUP BY clause does not match with
-- PARTITION KEY, thus we will have a partial aggregation for them.
EXPLAIN (COSTS OFF)
SELECT a, sum(b), count(*) FROM pagg_tab_ml GROUP BY a HAVING avg(b) < 3 ORDER BY 1, 2, 3;

\echo ---- plan-diff 226: partition_aggregate.diff:1270 ----
-- Partial aggregation at all levels as GROUP BY clause does not match with
-- PARTITION KEY
EXPLAIN (COSTS OFF)
SELECT b, sum(a), count(*) FROM pagg_tab_ml GROUP BY b ORDER BY 1, 2, 3;

\echo ---- plan-diff 227: partition_aggregate.diff:1311 ----
-- Full aggregation at all levels as GROUP BY clause matches with PARTITION KEY
EXPLAIN (COSTS OFF)
SELECT a, sum(b), count(*) FROM pagg_tab_ml GROUP BY a, b, c HAVING avg(b) > 7 ORDER BY 1, 2, 3;

\echo ---- plan-diff 228: partition_aggregate.diff:1375 ----
-- When GROUP BY clause matches; full aggregation is performed for each partition.
EXPLAIN (COSTS OFF)
SELECT x, sum(y), avg(y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;

\echo ---- plan-diff 229: partition_aggregate.diff:1412 ----
-- When GROUP BY clause does not match; partial aggregation is performed for each partition.
EXPLAIN (COSTS OFF)
SELECT y, sum(x), avg(x), count(*) FROM pagg_tab_para GROUP BY y HAVING avg(x) < 12 ORDER BY 1, 2, 3;

\echo ---- plan-diff 230: partition_aggregate.diff:1451, partition_aggregate.diff:1485 ----
EXPLAIN (COSTS OFF)
SELECT x, sum(y), avg(y), sum(x+y), count(*) FROM pagg_tab_para GROUP BY x HAVING avg(y) < 7 ORDER BY 1, 2, 3;

\echo ==== partition_join ====
\echo ---- plan-diff 231: partition_join.diff:29 ----
-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 232: partition_join.diff:69 ----
-- inner join with partially-redundant join clauses
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.a AND t1.a = t2.b ORDER BY t1.a, t2.b;

\echo ---- plan-diff 233: partition_join.diff:107 ----
-- left outer join, 3-way
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM prt1 t1
  LEFT JOIN prt1 t2 ON t1.a = t2.a
  LEFT JOIN prt1 t3 ON t2.a = t3.a;

\echo ---- plan-diff 234: partition_join.diff:154, partition_join.diff:168 ----
-- left outer join, with whole-row reference; partitionwise join does not apply
EXPLAIN (COSTS OFF)
SELECT t1, t2 FROM prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 235: partition_join.diff:191, partition_join.diff:207 ----
-- right outer join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 236: partition_join.diff:231 ----
-- full outer join, with placeholder vars
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

\echo ---- plan-diff 237: partition_join.diff:272 ----
-- Join with pruned partitions from joining relations
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.a < 450 AND t2.b > 250 AND t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 238: partition_join.diff:294 ----
-- Currently we can't do partitioned join if nullable-side partitions are pruned
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 239: partition_join.diff:333 ----
-- Currently we can't do partitioned join if nullable-side partitions are pruned
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a < 450) t1 FULL JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 OR t2.a = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 240: partition_join.diff:376 ----
-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t2.b FROM prt2 t2 WHERE t2.a = 0) AND t1.b = 0 ORDER BY t1.a;

\echo ---- plan-diff 241: partition_join.diff:442 ----
-- lateral reference
EXPLAIN (COSTS OFF)
SELECT * FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.a = ss.t2a WHERE t1.b = 0 ORDER BY t1.a;

\echo ---- plan-diff 242: partition_join.diff:496 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, ss.t2a, ss.t2c FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t3.a AS t3a, t2.b t2b, t2.c t2c, least(t1.a,t2.a,t3.b) FROM prt1 t2 JOIN prt2 t3 ON (t2.a = t3.b)) ss
			  ON t1.c = ss.t2c WHERE (t1.b + coalesce(ss.t2b, 0)) = 0 ORDER BY t1.a;

\echo ---- plan-diff 243: partition_join.diff:550 ----
-- lateral reference in sample scan
EXPLAIN (COSTS OFF)
SELECT * FROM prt1 t1 JOIN LATERAL
			  (SELECT * FROM prt1 t2 TABLESAMPLE SYSTEM (t1.a) REPEATABLE(t1.b)) s
			  ON t1.a = s.a;

\echo ---- plan-diff 244: partition_join.diff:575 ----
-- lateral reference in scan's restriction clauses
EXPLAIN (COSTS OFF)
SELECT count(*) FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t1.b AS t1b, t2.* FROM prt2 t2) s
			  ON t1.a = s.b WHERE s.t1b = s.a;

\echo ---- plan-diff 245: partition_join.diff:608 ----
EXPLAIN (COSTS OFF)
SELECT count(*) FROM prt1 t1 LEFT JOIN LATERAL
			  (SELECT t1.b AS t1b, t2.* FROM prt2 t2) s
			  ON t1.a = s.b WHERE s.t1b = s.b;

\echo ---- plan-diff 246: partition_join.diff:644 ----
EXPLAIN (COSTS OFF)
SELECT a, b FROM prt1 FULL JOIN prt2 p2(b,a,c) USING(a,b)
  WHERE a BETWEEN 490 AND 510
  GROUP BY 1, 2 ORDER BY 1, 2;

\echo ---- plan-diff 247: partition_join.diff:717 ----
-- bug in freeing the SpecialJoinInfo of a child-join
EXPLAIN (COSTS OFF)
SELECT * FROM prt1 t1 JOIN prt1 t2 ON t1.a = t2.a WHERE t1.a IN (SELECT a FROM prt1 t3);

\echo ---- plan-diff 248: partition_join.diff:769 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_e t1, prt2_e t2 WHERE (t1.a + t1.b)/2 = (t2.b + t2.a)/2 AND t1.c = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 249: partition_join.diff:813 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM prt1 t1, prt2 t2, prt1_e t3 WHERE t1.a = t2.b AND t1.a = (t3.a + t3.b)/2 AND t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 250: partition_join.diff:856 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) LEFT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.b = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

\echo ---- plan-diff 251: partition_join.diff:915, partition_join.diff:1308 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a + t3.b, t3.c FROM (prt1 t1 LEFT JOIN prt2 t2 ON t1.a = t2.b) RIGHT JOIN prt1_e t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t3.c = 0 ORDER BY t1.a, t2.b, t3.a + t3.b;

\echo ---- plan-diff 252: partition_join.diff:969 ----
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM prt1 FULL JOIN prt2 p2(b,a,c) USING(a,b) FULL JOIN prt2 p3(b,a,c) USING (a, b)
  WHERE a BETWEEN 490 AND 510;

\echo ---- plan-diff 253: partition_join.diff:1018 ----
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM prt1 FULL JOIN prt2 p2(b,a,c) USING(a,b) FULL JOIN prt2 p3(b,a,c) USING (a, b) FULL JOIN prt1 p4 (a,b,c) USING (a, b)
  WHERE a BETWEEN 490 AND 510;

\echo ---- plan-diff 254: partition_join.diff:1077 ----
-- make sure these go to null as expected
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.phv, t2.b, t2.phv, t3.a + t3.b, t3.phv FROM ((SELECT 50 phv, * FROM prt1 WHERE prt1.b = 0) t1 FULL JOIN (SELECT 75 phv, * FROM prt2 WHERE prt2.a = 0) t2 ON (t1.a = t2.b)) FULL JOIN (SELECT 50 phv, * FROM prt1_e WHERE prt1_e.c = 0) t3 ON (t1.a = (t3.a + t3.b)/2) WHERE t1.a = t1.phv OR t2.b = t2.phv OR (t3.a + t3.b)/2 = t3.phv ORDER BY t1.a, t2.b, t3.a + t3.b;

\echo ---- plan-diff 255: partition_join.diff:1133 ----
-- Semi-join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1, prt1_e t2 WHERE t1.a = 0 AND t1.b = (t2.a + t2.b)/2) AND t1.b = 0 ORDER BY t1.a;

\echo ---- plan-diff 256: partition_join.diff:1189, partition_join.diff:1246 ----
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1 t1 WHERE t1.a IN (SELECT t1.b FROM prt2 t1 WHERE t1.b IN (SELECT (t1.a + t1.b)/2 FROM prt1_e t1 WHERE t1.c = 0)) AND t1.b = 0 ORDER BY t1.a;

\echo ---- plan-diff 257: partition_join.diff:1385 ----
-- MergeAppend on nullable column
-- This should generate a partitionwise join, but currently fails to
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM (SELECT * FROM prt1 WHERE a < 450) t1 LEFT JOIN (SELECT * FROM prt2 WHERE b > 250) t2 ON t1.a = t2.b WHERE t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 258: partition_join.diff:1425 ----
-- partitionwise join does not apply
EXPLAIN (COSTS OFF)
SELECT t1.a, t2.b FROM prt1 t1, prt2 t2 WHERE t1::text = t2::text AND t1.a = t2.b ORDER BY t1.a;

\echo ---- plan-diff 259: partition_join.diff:1474 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_m WHERE prt1_m.c = 0) t1 FULL JOIN (SELECT * FROM prt2_m WHERE prt2_m.c = 0) t2 ON (t1.a = (t2.b + t2.a)/2 AND t2.b = (t1.a + t1.b)/2) ORDER BY t1.a, t2.b;

\echo ---- plan-diff 260: partition_join.diff:1550 ----
-- test partition matching with N-way join
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM plt1 t1, plt2 t2, plt1_e t3 WHERE t1.b = t2.b AND t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

\echo ---- plan-diff 261: partition_join.diff:1622 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a = 1 AND a = 2) t1 RIGHT JOIN prt2 t2 ON t1.a = t2.b, prt1 t3 WHERE t2.b = t3.a;

\echo ---- plan-diff 262: partition_join.diff:1661 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1 WHERE a = 1 AND a = 2) t1 FULL JOIN prt2 t2 ON t1.a = t2.b WHERE t2.a = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 263: partition_join.diff:1694 ----
-- test partition matching with N-way join
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), avg(t3.a + t3.b), t1.c, t2.c, t3.c FROM pht1 t1, pht2 t2, pht1_e t3 WHERE t1.b = t2.b AND t1.c = t2.c AND ltrim(t3.c, 'A') = t1.c GROUP BY t1.c, t2.c, t3.c ORDER BY t1.c, t2.c, t3.c;

\echo ---- plan-diff 264: partition_join.diff:1750 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt2 t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 265: partition_join.diff:1784 ----
EXPLAIN (COSTS OFF)
SELECT avg(t1.a), avg(t2.b), t1.c, t2.c FROM plt1 t1 RIGHT JOIN plt2 t2 ON t1.c = t2.c WHERE t1.a % 25 = 0 GROUP BY t1.c, t2.c ORDER BY t1.c, t2.c;

\echo ---- plan-diff 266: partition_join.diff:1848 ----
-- inner join, qual covering only top-level partitions
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.b AND t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 267: partition_join.diff:1881 ----
-- inner join with partially-redundant join clauses
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1, prt2_l t2 WHERE t1.a = t2.a AND t1.a = t2.b AND t1.c = t2.c ORDER BY t1.a, t2.b;

\echo ---- plan-diff 268: partition_join.diff:1930 ----
-- left join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1 LEFT JOIN prt2_l t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 269: partition_join.diff:1983 ----
-- right join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_l t1 RIGHT JOIN prt2_l t2 ON t1.a = t2.b AND t1.c = t2.c WHERE t2.a = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 270: partition_join.diff:2032 ----
-- full join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE prt1_l.b = 0) t1 FULL JOIN (SELECT * FROM prt2_l WHERE prt2_l.a = 0) t2 ON (t1.a = t2.b AND t1.c = t2.c) ORDER BY t1.a, t2.b;

\echo ---- plan-diff 271: partition_join.diff:2093 ----
-- lateral partitionwise join
EXPLAIN (COSTS OFF)
SELECT * FROM prt1_l t1 LEFT JOIN LATERAL
			  (SELECT t2.a AS t2a, t2.c AS t2c, t2.b AS t2b, t3.b AS t3b, least(t1.a,t2.a,t3.b) FROM prt1_l t2 JOIN prt2_l t3 ON (t2.a = t3.b AND t2.c = t3.c)) ss
			  ON t1.a = ss.t2a AND t1.c = ss.t2c WHERE t1.b = 0 ORDER BY t1.a;

\echo ---- plan-diff 272: partition_join.diff:2165 ----
-- partitionwise join with lateral reference in sample scan
EXPLAIN (COSTS OFF)
SELECT * FROM prt1_l t1 JOIN LATERAL
			  (SELECT * FROM prt1_l t2 TABLESAMPLE SYSTEM (t1.a) REPEATABLE(t1.b)) s
			  ON t1.a = s.a AND t1.b = s.b AND t1.c = s.c;

\echo ---- plan-diff 273: partition_join.diff:2201 ----
-- partitionwise join with lateral reference in scan's restriction clauses
EXPLAIN (COSTS OFF)
SELECT COUNT(*) FROM prt1_l t1 LEFT JOIN LATERAL
			  (SELECT t1.b AS t1b, t2.* FROM prt2_l t2) s
			  ON t1.a = s.b AND t1.b = s.a AND t1.c = s.c
			  WHERE s.t1b = s.a;

\echo ---- plan-diff 274: partition_join.diff:2239 ----
-- join with one side empty
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT * FROM prt1_l WHERE a = 1 AND a = 2) t1 RIGHT JOIN prt2_l t2 ON t1.a = t2.b AND t1.b = t2.a AND t1.c = t2.c;

\echo ---- plan-diff 275: partition_join.diff:2315 ----
-- partitionwise join can not be applied if the partition ranges differ
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1 t1, prt4_n t2 WHERE t1.a = t2.a;

\echo ---- plan-diff 276: partition_join.diff:2385 ----
-- equi-join with join condition on partial keys does not qualify for
-- partitionwise join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_m t1, prt2_m t2 WHERE t1.a = (t2.b + t2.a)/2;

\echo ---- plan-diff 277: partition_join.diff:2440 ----
-- partitionwise join can not be applied for a join between list and range
-- partitioned tables
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_n t1 LEFT JOIN prt2_n t2 ON (t1.c = t2.c);

\echo ---- plan-diff 278: partition_join.diff:2481 ----
-- partitionwise join can not be applied for a join between key column and
-- non-key column
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_n t1 FULL JOIN prt1 t2 ON (t1.c = t2.c);

\echo ---- plan-diff 279: partition_join.diff:2621, partition_join.diff:2830, partition_join.diff:3136, partition_join.diff:3154 ----
-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_adv t1 INNER JOIN prt2_adv t2 ON (t1.a = t2.b) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 280: partition_join.diff:2666, partition_join.diff:2875, partition_join.diff:3178 ----
-- semi join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_adv t1 WHERE EXISTS (SELECT 1 FROM prt2_adv t2 WHERE t1.a = t2.b) AND t1.b = 0 ORDER BY t1.a;

\echo ---- plan-diff 281: partition_join.diff:2703, partition_join.diff:2912, partition_join.diff:3202 ----
-- left join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_adv t1 LEFT JOIN prt2_adv t2 ON (t1.a = t2.b) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 282: partition_join.diff:2752, partition_join.diff:3226 ----
-- anti join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt1_adv t1 WHERE NOT EXISTS (SELECT 1 FROM prt2_adv t2 WHERE t1.a = t2.b) AND t1.b = 0 ORDER BY t1.a;

\echo ---- plan-diff 283: partition_join.diff:2785, partition_join.diff:3247 ----
-- full join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 175 phv, * FROM prt1_adv WHERE prt1_adv.b = 0) t1 FULL JOIN (SELECT 425 phv, * FROM prt2_adv WHERE prt2_adv.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

\echo ---- plan-diff 284: partition_join.diff:2958, partition_join.diff:2977 ----
-- left join; currently we can't do partitioned join if there are no matched
-- partitions on the nullable side
EXPLAIN (COSTS OFF)
SELECT t1.b, t1.c, t2.a, t2.c FROM prt2_adv t1 LEFT JOIN prt1_adv t2 ON (t1.b = t2.a) WHERE t1.a = 0 ORDER BY t1.b, t2.a;

\echo ---- plan-diff 285: partition_join.diff:3021, partition_join.diff:3040 ----
-- anti join; currently we can't do partitioned join if there are no matched
-- partitions on the nullable side
EXPLAIN (COSTS OFF)
SELECT t1.* FROM prt2_adv t1 WHERE NOT EXISTS (SELECT 1 FROM prt1_adv t2 WHERE t1.b = t2.a) AND t1.a = 0 ORDER BY t1.b;

\echo ---- plan-diff 286: partition_join.diff:3047, partition_join.diff:3062 ----
-- full join; currently we can't do partitioned join if there are no matched
-- partitions on the nullable side
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM (SELECT 175 phv, * FROM prt1_adv WHERE prt1_adv.b = 0) t1 FULL JOIN (SELECT 425 phv, * FROM prt2_adv WHERE prt2_adv.a = 0) t2 ON (t1.a = t2.b) WHERE t1.phv = t1.a OR t2.phv = t2.b ORDER BY t1.a, t2.b;

\echo ---- plan-diff 287: partition_join.diff:3079 ----
-- 3-way join where not every pair of relations can do partitioned join
EXPLAIN (COSTS OFF)
SELECT t1.b, t1.c, t2.a, t2.c, t3.a, t3.c FROM prt2_adv t1 LEFT JOIN prt1_adv t2 ON (t1.b = t2.a) INNER JOIN prt1_adv t3 ON (t1.b = t3.a) WHERE t1.a = 0 ORDER BY t1.b, t2.a, t3.a;

\echo ---- plan-diff 288: partition_join.diff:3271 ----
-- We can do partitioned join even if only one of relations has the default
-- partition
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_adv t1 INNER JOIN prt2_adv t2 ON (t1.a = t2.b) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 289: partition_join.diff:3311, partition_join.diff:3328, partition_join.diff:3339, partition_join.diff:3356 ----
-- Partitioned join can't be applied because the default partition of prt1_adv
-- matches prt2_adv_p1 and prt2_adv_p3
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_adv t1 INNER JOIN prt2_adv t2 ON (t1.a = t2.b) WHERE t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 290: partition_join.diff:3372 ----
-- 3-way join to test the default partition of a join relation
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c, t3.a, t3.c FROM prt1_adv t1 LEFT JOIN prt2_adv t2 ON (t1.a = t2.b) LEFT JOIN prt3_adv t3 ON (t1.a = t3.a) WHERE t1.b = 0 ORDER BY t1.a, t2.b, t3.a;

\echo ---- plan-diff 291: partition_join.diff:3431 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_adv t1 INNER JOIN prt2_adv t2 ON (t1.a = t2.b) WHERE t1.a < 300 AND t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 292: partition_join.diff:3470 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.b, t2.c FROM prt1_adv t1 INNER JOIN prt2_adv t2 ON (t1.a = t2.b) WHERE t1.a >= 100 AND t1.a < 300 AND t1.b = 0 ORDER BY t1.a, t2.b;

\echo ---- plan-diff 293: partition_join.diff:3520, partition_join.diff:3713, partition_join.diff:3945, partition_join.diff:3963, partition_join.diff:4080, +2 more ----
-- inner join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1_adv t1 INNER JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 294: partition_join.diff:3561, partition_join.diff:3754, partition_join.diff:3987, partition_join.diff:4121 ----
-- semi join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1_adv t1 WHERE EXISTS (SELECT 1 FROM plt2_adv t2 WHERE t1.a = t2.a AND t1.c = t2.c) AND t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 295: partition_join.diff:3598, partition_join.diff:3787, partition_join.diff:4011, partition_join.diff:4158, partition_join.diff:4413 ----
-- left join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1_adv t1 LEFT JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 296: partition_join.diff:3637, partition_join.diff:4035, partition_join.diff:4198 ----
-- anti join
EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt1_adv t1 WHERE NOT EXISTS (SELECT 1 FROM plt2_adv t2 WHERE t1.a = t2.a AND t1.c = t2.c) AND t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 297: partition_join.diff:3668, partition_join.diff:4044, partition_join.diff:4230, partition_join.diff:4458 ----
-- full join
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1_adv t1 FULL JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE coalesce(t1.b, 0) < 10 AND coalesce(t2.b, 0) < 10 ORDER BY t1.a, t2.a;

\echo ---- plan-diff 298: partition_join.diff:3827, partition_join.diff:3846 ----
-- left join; currently we can't do partitioned join if there are no matched
-- partitions on the nullable side
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt2_adv t1 LEFT JOIN plt1_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 299: partition_join.diff:3888, partition_join.diff:3907 ----
-- anti join; currently we can't do partitioned join if there are no matched
-- partitions on the nullable side
EXPLAIN (COSTS OFF)
SELECT t1.* FROM plt2_adv t1 WHERE NOT EXISTS (SELECT 1 FROM plt1_adv t2 WHERE t1.a = t2.a AND t1.c = t2.c) AND t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 300: partition_join.diff:3917, partition_join.diff:4352 ----
-- full join; currently we can't do partitioned join if there are no matched
-- partitions on the nullable side
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1_adv t1 FULL JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE coalesce(t1.b, 0) < 10 AND coalesce(t2.b, 0) < 10 ORDER BY t1.a, t2.a;

\echo ---- plan-diff 301: partition_join.diff:4323, partition_join.diff:4342 ----
-- left join; currently we can't do partitioned join if there are no matched
-- partitions on the nullable side
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1_adv t1 LEFT JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 302: partition_join.diff:4507 ----
-- 3-way join to test the NULL partition of a join relation
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c, t3.a, t3.c FROM plt1_adv t1 LEFT JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) LEFT JOIN plt1_adv t3 ON (t1.a = t3.a AND t1.c = t3.c) WHERE t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 303: partition_join.diff:4578 ----
-- We can do partitioned join even if only one of relations has the default
-- partition
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1_adv t1 INNER JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 304: partition_join.diff:4615, partition_join.diff:4640 ----
-- Partitioned join can't be applied because the default partition of plt1_adv
-- matches plt2_adv_p1 and plt2_adv_p2_ext
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1_adv t1 INNER JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 305: partition_join.diff:4669 ----
-- 3-way join to test the default partition of a join relation
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c, t3.a, t3.c FROM plt1_adv t1 LEFT JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) LEFT JOIN plt3_adv t3 ON (t1.a = t3.a AND t1.c = t3.c) WHERE t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 306: partition_join.diff:4715, partition_join.diff:4750 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1_adv t1 INNER JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 307: partition_join.diff:4791, partition_join.diff:4844 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1_adv t1 INNER JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE t1.c IN ('0003', '0004', '0005') AND t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 308: partition_join.diff:4820, partition_join.diff:4873 ----
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c FROM plt1_adv t1 LEFT JOIN plt2_adv t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE t1.c IS NULL AND t1.b < 10 ORDER BY t1.a;

\echo ---- plan-diff 309: partition_join.diff:4915 ----
-- This tests that when merging partitions from plt1_adv and plt2_adv in
-- merge_list_bounds(), process_outer_partition() returns an already-assigned
-- merged partition when re-called with plt1_adv_p1 for the second list value
-- '0001' of that partition
EXPLAIN (COSTS OFF)
SELECT t1.a, t1.c, t2.a, t2.c, t3.a, t3.c FROM (plt1_adv t1 LEFT JOIN plt2_adv t2 ON (t1.c = t2.c)) FULL JOIN plt3_adv t3 ON (t1.c = t3.c) WHERE coalesce(t1.a, 0) % 5 != 3 AND coalesce(t1.a, 0) % 5 != 4 ORDER BY t1.c, t1.a, t2.a, t3.a;

\echo ---- plan-diff 310: partition_join.diff:5036 ----
EXPLAIN (COSTS OFF)
SELECT t1.*, t2.* FROM alpha t1 INNER JOIN beta t2 ON (t1.a = t2.a AND t1.b = t2.b) WHERE t1.b >= 125 AND t1.b < 225 ORDER BY t1.a, t1.b;

\echo ---- plan-diff 311: partition_join.diff:5116 ----
EXPLAIN (COSTS OFF)
SELECT t1.*, t2.* FROM alpha t1 INNER JOIN beta t2 ON (t1.a = t2.a AND t1.c = t2.c) WHERE ((t1.b >= 100 AND t1.b < 110) OR (t1.b >= 200 AND t1.b < 210)) AND ((t2.b >= 100 AND t2.b < 110) OR (t2.b >= 200 AND t2.b < 210)) AND t1.c IN ('0004', '0009') ORDER BY t1.a, t1.b, t2.b;

\echo ---- plan-diff 312: partition_join.diff:5174 ----
EXPLAIN (COSTS OFF)
SELECT t1.*, t2.* FROM alpha t1 INNER JOIN beta t2 ON (t1.a = t2.a AND t1.b = t2.b AND t1.c = t2.c) WHERE ((t1.b >= 100 AND t1.b < 110) OR (t1.b >= 200 AND t1.b < 210)) AND ((t2.b >= 100 AND t2.b < 110) OR (t2.b >= 200 AND t2.b < 210)) AND t1.c IN ('0004', '0009') ORDER BY t1.a, t1.b;

\echo ---- plan-diff 313: partition_join.diff:5231 ----
EXPLAIN (COSTS OFF)
SELECT x.id, y.id FROM fract_t x LEFT JOIN fract_t y USING (id) ORDER BY x.id ASC LIMIT 10;

\echo ---- plan-diff 314: partition_join.diff:5248 ----
EXPLAIN (COSTS OFF)
SELECT x.id, y.id FROM fract_t x LEFT JOIN fract_t y USING (id) ORDER BY x.id DESC LIMIT 10;

\echo ---- plan-diff 315: partition_join.diff:5266, partition_join.diff:5285 ----
EXPLAIN (COSTS OFF) -- Should use NestLoop with parameterised inner scan
SELECT x.id, y.id FROM fract_t x LEFT JOIN fract_t y USING (id)
ORDER BY x.id DESC LIMIT 2;

\echo ==== partition_prune ====
\echo ---- plan-diff 316: partition_prune.diff:399 ----
explain (costs off) select * from rlp where a = 16 and b in ('not', 'in', 'here');

\echo ---- plan-diff 317: partition_prune.diff:414 ----
explain (costs off) select * from rlp where a = 16 and b <= 'ab';

\echo ---- plan-diff 318: partition_prune.diff:739 ----
explain (costs off) select * from mc3p where a = 1 and abs(b) = 1 and c < 8;

\echo ---- plan-diff 319: partition_prune.diff:989 ----
explain (costs off) select * from mc2p where a = 2 and b < 1;

\echo ---- plan-diff 320: partition_prune.diff:1012 ----
explain (costs off) select * from mc2p where a = 1 and b > 1;

\echo ---- plan-diff 321: partition_prune.diff:1627 ----
EXPLAIN (VERBOSE, COSTS OFF) SELECT * FROM part p(x) ORDER BY x;

\echo ---- plan-diff 322: partition_prune.diff:1706, partition_prune.diff:1716 ----
-- also here, because values for all keys are provided
explain (costs off) select * from mc2p t1, lateral (select count(*) from mc3p t2 where t2.a = 1 and abs(t2.b) = 1 and t2.c = 1) s where t1.a = 1;

\echo ---- plan-diff 323: partition_prune.diff:1799 ----
-- check that it also works for a partitioned table that's not root,
-- which in this case are partitions of rlp that are themselves
-- list-partitioned on b
explain (costs off) select * from rlp where a = 15 and b <> 'ab' and b <> 'cd' and b <> 'xy' and b is not null;

\echo ---- plan-diff 324: partition_prune.diff:2227 ----
explain (analyze, costs off, summary off, timing off, buffers off) execute ab_q2 (2, 2);

\echo ---- plan-diff 325: partition_prune.diff:2245 ----
explain (analyze, costs off, summary off, timing off, buffers off) execute ab_q3 (2, 2);

\echo ---- plan-diff 326: partition_prune.diff:2664, partition_prune.diff:2677 ----
-- Test run-time partition pruning with an initplan
explain (analyze, costs off, summary off, timing off, buffers off)
select * from ab where a = (select max(a) from lprt_a) and b = (select max(a)-1 from lprt_a);

\echo ---- plan-diff 327: partition_prune.diff:2747 ----
-- Test run-time partition pruning with UNION ALL parents
explain (analyze, costs off, summary off, timing off, buffers off)
select * from (select * from ab where a = 1 union all select * from ab) ab where b = (select 1);

\echo ---- plan-diff 328: partition_prune.diff:2850 ----
-- A case containing a UNION ALL with a non-partitioned child.
explain (analyze, costs off, summary off, timing off, buffers off)
select * from (select * from ab where a = 1 union all (values(10,5)) union all select * from ab) ab where b = (select 1);

\echo ---- plan-diff 329: partition_prune.diff:2890, partition_prune.diff:2953 ----
-- Ensure the xy_1 subplan is not pruned.
explain (analyze, costs off, summary off, timing off, buffers off) execute ab_q6(1);

\echo ---- plan-diff 330: partition_prune.diff:3341 ----
-- Ensure Params that evaluate to NULL properly prune away all partitions
explain (analyze, costs off, summary off, timing off, buffers off)
select * from listp where a = (select null::int);

\echo ---- plan-diff 331: partition_prune.diff:3381 ----
-- timestamp < timestamptz comparison is only stable, not immutable
explain (analyze, costs off, summary off, timing off, buffers off)
select * from stable_qual_pruning where a < '2000-02-01'::timestamptz;

\echo ---- plan-diff 332: partition_prune.diff:3490 ----
explain (analyze, costs off, summary off, timing off, buffers off)
execute ps1(1);

\echo ---- plan-diff 333: partition_prune.diff:3505 ----
explain (analyze, costs off, summary off, timing off, buffers off)
execute ps2(1);

\echo ---- plan-diff 334: partition_prune.diff:3526 ----
explain (analyze, costs off, summary off, timing off, buffers off)
select * from boolp where a = (select value from boolvalues where value);

\echo ---- plan-diff 335: partition_prune.diff:3627 ----
-- Ensure output list looks sane when the MergeAppend has no subplans.
explain (analyze, verbose, costs off, summary off, timing off, buffers off) execute mt_q2 (35);

\echo ---- plan-diff 336: partition_prune.diff:3696 ----
explain (costs off) select * from pp_arrpart where a in ('{4, 5}', '{1}');

\echo ---- plan-diff 337: partition_prune.diff:4094 ----
explain (analyze, costs off, summary off, timing off, buffers off)
select * from listp where a = (select 2) and b <> 10;

\echo ---- plan-diff 338: partition_prune.diff:4157, partition_prune.diff:4178 ----
explain (costs off) update listp1 set a = 1 where a = 2;

\echo ---- plan-diff 339: partition_prune.diff:4219 ----
-- Ensure run-time pruning works on the nested Merge Append
explain (analyze on, costs off, timing off, summary off, buffers off)
select * from rangep where b IN((select 1),(select 2)) order by a;

\echo ---- plan-diff 340: partition_prune.diff:4266 ----
-- Don't call get_steps_using_prefix() with the last partition key c plus
-- an invalid prefix (ie, b = 1)
explain (costs off) select * from rp_prefix_test2 where a <= 1 and b = 1 and c >= 0;

\echo ---- plan-diff 341: partition_prune.diff:4288 ----
-- Test that get_steps_using_prefix() handles a prefix that contains multiple
-- clauses for the partition key b (ie, b >= 1 and b = 2)  (This also tests
-- that the caller arranges clauses in that prefix in the required order)
explain (costs off) select * from rp_prefix_test3 where a >= 1 and b >= 1 and b = 2 and c = 2 and d >= 0;

\echo ---- plan-diff 342: partition_prune.diff:4557 ----
-- Only the unpruned partition should be shown in the list of relations to be
-- updated
explain (verbose, costs off) execute update_part_abc_view (1, 'd');

\echo ---- plan-diff 343: partition_prune.diff:4576 ----
explain (verbose, costs off) execute update_part_abc_view (2, 'a');

\echo ==== plpgsql ====
\echo ---- plan-diff 344: plpgsql.diff:4112 ----
explain (verbose, costs off) select error_trap_test();

\echo ---- plan-diff 345: plpgsql.diff:5146 ----
-- bug #14174
explain (verbose, costs off)
select i, a from
  (select returns_rw_array(1) as a offset 0) ss,
  lateral consumes_rw_array(a) i;

\echo ---- plan-diff 346: plpgsql.diff:5169 ----
explain (verbose, costs off)
select consumes_rw_array(a), a from returns_rw_array(1) a;

\echo ==== portals ====
\echo ---- plan-diff 347: portals.diff:1473 ----
EXPLAIN (costs off)
DECLARE c1 CURSOR FOR SELECT stringu1 FROM onek WHERE stringu1 = 'DZAAAA';

\echo ---- plan-diff 348: portals.diff:1481 ----
explain (costs off) declare c1 cursor for select (select 42) as x;

\echo ---- plan-diff 349: portals.diff:1512 ----
explain (costs off) declare c2 cursor for select generate_series(1,3) as g;

\echo ==== privileges ====
\echo ---- plan-diff 350: privileges.diff:486 ----
-- This plan should use nestloop, knowing that few rows will be selected.
EXPLAIN (COSTS OFF) SELECT * FROM atest12v x, atest12v y WHERE x.a = y.b;

\echo ---- plan-diff 351: privileges.diff:544 ----
EXPLAIN (COSTS OFF) SELECT * FROM atest12sbv WHERE a >>> 0;

\echo ---- plan-diff 352: privileges.diff:600 ----
-- But a security barrier view isolates the leaky operator.
EXPLAIN (COSTS OFF) SELECT * FROM atest12sbv x, atest12sbv y
  WHERE x.a = y.b and abs(y.a) <<< 5;

\echo ==== rangefuncs ====
\echo ---- plan-diff 353: rangefuncs.diff:1892 ----
-- with "strict", this function can't be inlined in FROM
explain (verbose, costs off)
  select * from array_to_set(array['one', 'two']) as t(f1 numeric(4,2),f2 text);

\echo ---- plan-diff 354: rangefuncs.diff:1985 ----
explain (verbose, costs off)
select testrngfunc();

\echo ---- plan-diff 355: rangefuncs.diff:2359 ----
explain (verbose, costs off)
select x from int8_tbl, extractq2(int8_tbl) f(x);

\echo ---- plan-diff 356: rangefuncs.diff:2385 ----
explain (verbose, costs off)
select x from int8_tbl, extractq2_2(int8_tbl) f(x);

\echo ---- plan-diff 357: rangefuncs.diff:2411 ----
explain (verbose, costs off)
select x from int8_tbl, extractq2_2_opt(int8_tbl) f(x);

\echo ==== returning ====
\echo ---- plan-diff 358: returning.diff:655 ----
EXPLAIN (verbose, costs off)
DELETE FROM foo WHERE f1 = 4 RETURNING old.*,new.*, *;

\echo ---- plan-diff 359: returning.diff:757 ----
EXPLAIN (verbose, costs off)
UPDATE joinview SET f3 = f3 + 1, f4 = 7 WHERE f3 = 58
  RETURNING old.*, new.*, *, new.f3 - old.f3 AS delta_f3;

\echo ==== rowsecurity ====
\echo ---- plan-diff 360: rowsecurity.diff:266, rowsecurity.diff:331, rowsecurity.diff:425 ----
EXPLAIN (COSTS OFF) SELECT * FROM document WHERE f_leak(dtitle);

\echo ---- plan-diff 361: rowsecurity.diff:660 ----
EXPLAIN (COSTS OFF) SELECT * FROM document NATURAL JOIN category WHERE f_leak(dtitle);

\echo ---- plan-diff 362: rowsecurity.diff:689, rowsecurity.diff:710, rowsecurity.diff:889, rowsecurity.diff:988, rowsecurity.diff:1569 ----
EXPLAIN (COSTS OFF) SELECT * FROM t1 WHERE f_leak(b);

\echo ---- plan-diff 363: rowsecurity.diff:733, rowsecurity.diff:756 ----
EXPLAIN (COSTS OFF) SELECT *, t1 FROM t1;

\echo ---- plan-diff 364: rowsecurity.diff:787, rowsecurity.diff:808 ----
EXPLAIN (COSTS OFF) SELECT * FROM t1 WHERE f_leak(b) FOR SHARE;

\echo ---- plan-diff 365: rowsecurity.diff:847 ----
EXPLAIN (COSTS OFF) SELECT a, b, tableoid::regclass FROM t2 UNION ALL SELECT a, b, tableoid::regclass FROM t3;

\echo ---- plan-diff 366: rowsecurity.diff:1030, rowsecurity.diff:1061, rowsecurity.diff:1139, rowsecurity.diff:1177, rowsecurity.diff:1229, +1 more ----
EXPLAIN (COSTS OFF) SELECT * FROM part_document WHERE f_leak(dtitle);

\echo ---- plan-diff 367: rowsecurity.diff:1439 ----
EXPLAIN (COSTS OFF) SELECT * FROM only s1 WHERE f_leak(b);

\echo ---- plan-diff 368: rowsecurity.diff:1459, rowsecurity.diff:1474 ----
EXPLAIN (COSTS OFF) SELECT * FROM s1 WHERE f_leak(b);

\echo ---- plan-diff 369: rowsecurity.diff:1503 ----
EXPLAIN (COSTS OFF) SELECT (SELECT x FROM s1 LIMIT 1) xx, * FROM s2 WHERE y like '%28%';

\echo ---- plan-diff 370: rowsecurity.diff:1545, rowsecurity.diff:1590 ----
EXPLAIN (COSTS OFF) EXECUTE p1(2);

\echo ---- plan-diff 371: rowsecurity.diff:1613 ----
EXPLAIN (COSTS OFF) EXECUTE p2(2);

\echo ---- plan-diff 372: rowsecurity.diff:1705 ----
-- updates with from clause
EXPLAIN (COSTS OFF) UPDATE t2 SET b=t2.b FROM t3
WHERE t2.a = 3 and t3.a = 2 AND f_leak(t2.b) AND f_leak(t3.b);

\echo ---- plan-diff 373: rowsecurity.diff:1790 ----
EXPLAIN (COSTS OFF) UPDATE t1 t1_1 SET b = t1_2.b FROM t1 t1_2
WHERE t1_1.a = 4 AND t1_2.a = t1_1.a AND t1_2.b = t1_1.b
AND f_leak(t1_1.b) AND f_leak(t1_2.b) RETURNING *, t1_1, t1_2;

\echo ---- plan-diff 374: rowsecurity.diff:1909 ----
EXPLAIN (COSTS OFF) SELECT * FROM bv1 WHERE f_leak(b);

\echo ---- plan-diff 375: rowsecurity.diff:1937 ----
INSERT INTO bv1 VALUES (12, 'xxx'); -- ok
EXPLAIN (COSTS OFF) UPDATE bv1 SET b = 'yyy' WHERE a = 4 AND f_leak(b);

\echo ---- plan-diff 376: rowsecurity.diff:1947 ----
EXPLAIN (COSTS OFF) DELETE FROM bv1 WHERE a = 6 AND f_leak(b);

\echo ---- plan-diff 377: rowsecurity.diff:2386, rowsecurity.diff:2437, rowsecurity.diff:2485, rowsecurity.diff:2533 ----
EXPLAIN (COSTS OFF) SELECT * FROM z1 WHERE f_leak(b);

\echo ---- plan-diff 378: rowsecurity.diff:2394 ----
EXPLAIN (COSTS OFF) EXECUTE plancache_test;

\echo ---- plan-diff 379: rowsecurity.diff:2592 ----
EXPLAIN (COSTS OFF) EXECUTE plancache_test3;

\echo ---- plan-diff 380: rowsecurity.diff:2614, rowsecurity.diff:2639, rowsecurity.diff:2658, rowsecurity.diff:2684, rowsecurity.diff:2736, +7 more ----
EXPLAIN (COSTS OFF) SELECT * FROM rls_view;

\echo ---- plan-diff 381: rowsecurity.diff:3086 ----
EXPLAIN (COSTS OFF) SELECT * FROM rls_sbv WHERE (a = 1);

\echo ---- plan-diff 382: rowsecurity.diff:3136 ----
EXPLAIN (COSTS OFF) SELECT * FROM y2 WHERE f_leak(b);

\echo ---- plan-diff 383: rowsecurity.diff:3186 ----
EXPLAIN (COSTS OFF) SELECT * FROM y2 WHERE f_leak('abc');

\echo ---- plan-diff 384: rowsecurity.diff:3206 ----
EXPLAIN (COSTS OFF) SELECT * FROM y2 JOIN test_qual_pushdown ON (b = abc) WHERE f_leak(abc);

\echo ---- plan-diff 385: rowsecurity.diff:3237, rowsecurity.diff:3263 ----
EXPLAIN (COSTS OFF) SELECT * FROM y2 JOIN test_qual_pushdown ON (b = abc) WHERE f_leak(b);

\echo ---- plan-diff 386: rowsecurity.diff:3273 ----
-- Check plan
EXPLAIN (COSTS OFF) EXECUTE role_inval;

\echo ---- plan-diff 387: rowsecurity.diff:3283 ----
-- Check plan- should be different
EXPLAIN (COSTS OFF) EXECUTE role_inval;

\echo ---- plan-diff 388: rowsecurity.diff:3332, rowsecurity.diff:3395 ----
EXPLAIN (COSTS OFF)
WITH cte1 AS MATERIALIZED (SELECT * FROM t1 WHERE f_leak(b)) SELECT * FROM cte1;

\echo ---- plan-diff 389: rowsecurity.diff:3420 ----
EXPLAIN (COSTS OFF) INSERT INTO t2 (SELECT * FROM t1);

\echo ---- plan-diff 390: rowsecurity.diff:3555 ----
EXPLAIN (COSTS OFF) SELECT * FROM t2;

\echo ---- plan-diff 391: rowsecurity.diff:3883 ----
-- Plan should be a subquery TID scan
EXPLAIN (COSTS OFF) UPDATE current_check SET payload = payload WHERE CURRENT OF current_check_cursor;

\echo ---- plan-diff 392: rowsecurity.diff:3947 ----
EXPLAIN (COSTS OFF)
UPDATE current_check_2 SET b = 'Manzana' WHERE CURRENT OF current_check_cursor;

\echo ---- plan-diff 393: rowsecurity.diff:4616 ----
EXPLAIN (VERBOSE, COSTS OFF)
INSERT INTO rls_tbl
  SELECT * FROM (SELECT b, c FROM rls_tbl ORDER BY a) ss;

\echo ==== rowtypes ====
\echo ---- plan-diff 394: rowtypes.diff:451 ----
explain (costs off)
select a,b from test_table where (a,b) > ('a','a') order by a,b;

\echo ---- plan-diff 395: rowtypes.diff:469 ----
explain (costs off)
select * from int8_tbl i8
where i8 in (row(123,456)::int8_tbl, '(4567890123456789,123)');

\echo ---- plan-diff 396: rowtypes.diff:1133 ----
explain (costs off)
select row_to_json(q) from
  (select thousand, tenthous from tenk1
   where thousand = 42 and tenthous < 2000 offset 0) q;

\echo ==== select_distinct ====
\echo ---- plan-diff 397: select_distinct.diff:233 ----
-- Ensure we get a parallel plan
EXPLAIN (costs off)
SELECT DISTINCT four FROM tenk1;

\echo ---- plan-diff 398: select_distinct.diff:280 ----
-- Ensure we do parallel distinct now that the function is parallel safe
EXPLAIN (COSTS OFF)
SELECT DISTINCT distinct_func(1) FROM tenk1;

\echo ---- plan-diff 399: select_distinct.diff:322 ----
-- Ensure we get a plan with a Limit 1
EXPLAIN (COSTS OFF)
SELECT DISTINCT four FROM tenk1 WHERE four = 0 AND two <> 0;

\echo ---- plan-diff 400: select_distinct.diff:356 ----
-- Ensure we get a plan with a Limit 1 in both partial distinct and final
-- distinct
EXPLAIN (COSTS OFF)
SELECT DISTINCT four FROM tenk1 WHERE four = 10;

\echo ---- plan-diff 401: select_distinct.diff:538 ----
EXPLAIN (COSTS OFF)
SELECT DISTINCT y, x FROM distinct_tbl limit 10;

\echo ==== select_parallel ====
\echo ---- plan-diff 402: select_parallel.diff:27 ----
-- Parallel Append with partial-subplans
explain (costs off)
  select round(avg(aa)), sum(aa) from a_star;

\echo ---- plan-diff 403: select_parallel.diff:53, select_parallel.diff:78, select_parallel.diff:109 ----
explain (costs off)
  select round(avg(aa)), sum(aa) from a_star;

\echo ---- plan-diff 404: select_parallel.diff:149 ----
explain (costs off)
	select (select max((select pa1.b from part_pa_test pa1 where pa1.a = pa2.a)))
	from part_pa_test pa2;

\echo ---- plan-diff 405: select_parallel.diff:216 ----
explain (verbose, costs off)
select sp_parallel_restricted(unique1) from tenk1
  where stringu1 = 'GRAAAA' order by 1;

\echo ---- plan-diff 406: select_parallel.diff:255 ----
explain (costs off)
	select stringu1, count(*) from tenk1 group by stringu1 order by stringu1;

\echo ---- plan-diff 407: select_parallel.diff:269 ----
-- test that parallel plan for aggregates is not selected when
-- target list contains parallel restricted clause.
explain (costs off)
	select  sum(sp_parallel_restricted(unique1)) from tenk1
	group by(sp_parallel_restricted(unique1));

\echo ---- plan-diff 408: select_parallel.diff:303 ----
explain (costs off)
	select count(*) from tenk1 where (two, four) not in
	(select hundred, thousand from tenk2 where thousand > 100);

\echo ---- plan-diff 409: select_parallel.diff:327 ----
-- this is not parallel-safe due to use of random() within SubLink's testexpr:
explain (costs off)
	select * from tenk1 where (unique1 + random())::integer not in
	(select ten from tenk2);

\echo ---- plan-diff 410: select_parallel.diff:344 ----
explain (costs off)
	select count(*) from tenk1
        where tenk1.unique1 = (Select max(tenk2.unique1) from tenk2);

\echo ---- plan-diff 411: select_parallel.diff:381 ----
explain (costs off)
	select  count((unique1)) from tenk1 where hundred > 1;

\echo ---- plan-diff 412: select_parallel.diff:396 ----
-- Parallel ScalarArrayOp index scan
explain (costs off)
  select count((unique1)) from tenk1
  where hundred = any ((select array_agg(i) from generate_series(1, 100, 15) i)::int[]);

\echo ---- plan-diff 413: select_parallel.diff:419 ----
-- test parallel index-only scans.
explain (costs off)
	select  count(*) from tenk1 where thousand > 95;

\echo ---- plan-diff 414: select_parallel.diff:441 ----
explain (costs off)
select * from
  (select count(unique1) from tenk1 where hundred > 10) ss
  right join (values (1),(2),(3)) v(x) on true;

\echo ---- plan-diff 415: select_parallel.diff:467 ----
explain (costs off)
select * from
  (select count(*) from tenk1 where thousand > 99) ss
  right join (values (1),(2),(3)) v(x) on true;

\echo ---- plan-diff 416: select_parallel.diff:546 ----
set work_mem='64kB';  --set small work mem to force lossy pages
explain (costs off)
	select count(*) from tenk1, tenk2 where tenk1.hundred > 1 and tenk2.thousand=0;

\echo ---- plan-diff 417: select_parallel.diff:584 ----
explain (analyze, timing off, summary off, costs off, buffers off)
   select count(*) from tenk1, tenk2 where tenk1.hundred > 1
        and tenk2.thousand=0;

\echo ---- plan-diff 418: select_parallel.diff:648 ----
explain (costs off)
	select  count(*) from tenk1, tenk2 where tenk1.unique1 = tenk2.unique1;

\echo ---- plan-diff 419: select_parallel.diff:754 ----
explain (costs off, verbose)
    select ten, sp_simple_func(ten) from tenk1 where ten < 100 order by ten;

\echo ---- plan-diff 420: select_parallel.diff:773 ----
-- test handling of SRFs in targetlist (bug in 10.0)
explain (costs off)
   select count(*), generate_series(1,2) from tenk1 group by twenty;

\echo ---- plan-diff 421: select_parallel.diff:922 ----
-- check parallelized int8 aggregate (bug #14897)
explain (costs off)
select avg(unique1::int8) from tenk1;

\echo ---- plan-diff 422: select_parallel.diff:1017 ----
-- Test gather merge atop of a sort of a partial path
explain (costs off)
select * from tenk1 where four = 2
order by four, hundred, parallel_safe_volatile(thousand);

\echo ---- plan-diff 423: select_parallel.diff:1033 ----
explain (costs off)
select * from tenk1 where four = 2
order by four, hundred, parallel_safe_volatile(thousand);

\echo ---- plan-diff 424: select_parallel.diff:1050 ----
-- Test GROUP BY with a gather merge path atop of a sort of a partial path
explain (costs off)
select count(*) from tenk1
group by twenty, parallel_safe_volatile(two);

\echo ---- plan-diff 425: select_parallel.diff:1068 ----
explain (costs off)
  select stringu1::int2 from tenk1 where unique1 = 1;

\echo ---- plan-diff 426: select_parallel.diff:1126 ----
-- Window function calculation can't be pushed to workers.
explain (costs off, verbose)
  select count(*) from tenk1 a where (unique1, two) in
    (select unique1, row_number() over() from tenk1 b);

\echo ---- plan-diff 427: select_parallel.diff:1154 ----
-- LIMIT/OFFSET within sub-selects can't be pushed to workers.
explain (costs off)
  select * from tenk1 a where two in
    (select two from tenk1 b where stringu1 like '%AAAA' limit 3);

\echo ---- plan-diff 428: select_parallel.diff:1176 ----
EXPLAIN (analyze, timing off, summary off, costs off, buffers off) SELECT * FROM tenk1;

\echo ---- plan-diff 429: select_parallel.diff:1218 ----
-- can't use multiple subqueries under a single Gather node due to initPlans
EXPLAIN (COSTS OFF)
SELECT unique1 FROM tenk1 WHERE fivethous =
	(SELECT unique1 FROM tenk1 WHERE fivethous = 1 LIMIT 1)
UNION ALL
SELECT unique1 FROM tenk1 WHERE fivethous =
	(SELECT unique2 FROM tenk1 WHERE fivethous = 1 LIMIT 1)
ORDER BY 1;

\echo ---- plan-diff 430: select_parallel.diff:1255 ----
EXPLAIN (VERBOSE, COSTS OFF)
SELECT generate_series(1, two), array(select generate_series(1, two))
  FROM tenk1 ORDER BY tenthous;

\echo ---- plan-diff 431: select_parallel.diff:1280, select_parallel.diff:1311 ----
-- must disallow pushing sort below gather when pathkey contains an SRF
EXPLAIN (VERBOSE, COSTS OFF)
SELECT unnest(ARRAY[]::integer[]) + 1 AS pathkey
  FROM tenk1 t1 JOIN tenk1 t2 ON TRUE
  ORDER BY pathkey;

\echo ---- plan-diff 432: select_parallel.diff:1331 ----
EXPLAIN (COSTS OFF)
SELECT 1 FROM tenk1_vw_sec
  WHERE (SELECT sum(f1) FROM int4_tbl WHERE f1 < unique1) < 100;

\echo ==== select_views ====
\echo ---- plan-diff 433: select_views.diff:1327, select_views.diff:1341 ----
EXPLAIN (COSTS OFF) SELECT * FROM my_property_normal WHERE f_leak(passwd);

\echo ---- plan-diff 434: select_views.diff:1368 ----
EXPLAIN (COSTS OFF) SELECT * FROM my_property_normal v
		WHERE f_leak('passwd') AND f_leak(passwd);

\echo ---- plan-diff 435: select_views.diff:1387, select_views.diff:1410 ----
EXPLAIN (COSTS OFF) SELECT * FROM my_property_secure v
		WHERE f_leak('passwd') AND f_leak(passwd);

\echo ---- plan-diff 436: select_views.diff:1429 ----
EXPLAIN (COSTS OFF) SELECT * FROM my_credit_card_normal WHERE f_leak(cnum);

\echo ---- plan-diff 437: select_views.diff:1437 ----
EXPLAIN (COSTS OFF) SELECT * FROM my_credit_card_secure WHERE f_leak(cnum);

\echo ---- plan-diff 438: select_views.diff:1458 ----
EXPLAIN (COSTS OFF) SELECT * FROM my_credit_card_usage_normal
       WHERE f_leak(cnum) AND ymd >= '2011-10-01' AND ymd < '2011-11-01';

\echo ---- plan-diff 439: select_views.diff:1489 ----
EXPLAIN (COSTS OFF) SELECT * FROM my_credit_card_usage_secure
       WHERE f_leak(cnum) AND ymd >= '2011-10-01' AND ymd < '2011-11-01';

\echo ==== spgist ====
\echo ---- plan-diff 440: spgist.diff:57 ----
explain (costs off)
select * from spgist_domain_tbl where f1 = 'fo';

\echo ==== sqljson ====
\echo ---- plan-diff 441: sqljson.diff:75 ----
EXPLAIN (VERBOSE, COSTS OFF) SELECT JSON('123');

\echo ---- plan-diff 442: sqljson.diff:276 ----
EXPLAIN (VERBOSE, COSTS OFF) SELECT JSON_SCALAR('123');

\echo ---- plan-diff 443: sqljson.diff:974 ----
-- Test JSON_OBJECT deparsing
EXPLAIN (VERBOSE, COSTS OFF)
SELECT JSON_OBJECT('foo' : '1' FORMAT JSON, 'bar' : 'baz' RETURNING json);

\echo ---- plan-diff 444: sqljson.diff:1030 ----
EXPLAIN (VERBOSE, COSTS OFF)
SELECT JSON_OBJECTAGG(i: ('111' || i)::bytea FORMAT JSON WITH UNIQUE RETURNING text) OVER (PARTITION BY i % 2)
FROM generate_series(1,5) i;

\echo ---- plan-diff 445: sqljson.diff:1055 ----
-- Test JSON_ARRAYAGG deparsing
EXPLAIN (VERBOSE, COSTS OFF)
SELECT JSON_ARRAYAGG(('111' || i)::bytea FORMAT JSON NULL ON NULL RETURNING text) FILTER (WHERE i > 3)
FROM generate_series(1,5) i;

\echo ---- plan-diff 446: sqljson.diff:1067, sqljson.diff:1084 ----
EXPLAIN (VERBOSE, COSTS OFF)
SELECT JSON_ARRAYAGG(('111' || i)::bytea FORMAT JSON NULL ON NULL RETURNING text) OVER (PARTITION BY i % 2)
FROM generate_series(1,5) i;

\echo ---- plan-diff 447: sqljson.diff:1291, sqljson.diff:1331 ----
-- Test IS JSON deparsing
EXPLAIN (VERBOSE, COSTS OFF)
SELECT '1' IS JSON AS "any", ('1' || i) IS JSON SCALAR AS "scalar", '[]' IS NOT JSON ARRAY AS "array", '{}' IS JSON OBJECT WITH UNIQUE AS "object" FROM generate_series(1, 3) i;

\echo ==== subselect ====
\echo ---- plan-diff 448: subselect.diff:206 ----
-- Check ROWCOMPARE cases, both correlated and not
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ROW(1, 2) = (SELECT f1, f2) AS eq FROM SUBSELECT_TBL;

\echo ---- plan-diff 449: subselect.diff:230 ----
EXPLAIN (VERBOSE, COSTS OFF)
SELECT ROW(1, 2) = (SELECT 3, 4) AS eq FROM SUBSELECT_TBL;

\echo ---- plan-diff 450: subselect.diff:376 ----
-- check materialization of an initplan reference (bug #14524)
explain (verbose, costs off)
select 1 = all (select (select 1));

\echo ---- plan-diff 451: subselect.diff:402 ----
explain (costs off)
select * from int4_tbl o where exists
  (select 1 from int4_tbl i where i.f1=o.f1 limit null);

\echo ---- plan-diff 452: subselect.diff:796 ----
explain (costs off)
select (1 = any(array_agg(f1))) = any (select false) from int4_tbl;

\echo ---- plan-diff 453: subselect.diff:928 ----
explain (verbose, costs off)
select 'foo'::text in (select 'bar'::name union all select 'bar'::name);

\echo ---- plan-diff 454: subselect.diff:952 ----
explain (verbose, costs off)
select row(row(row(1))) = any (select row(row(1)));

\echo ---- plan-diff 455: subselect.diff:995, subselect.diff:1016, subselect.diff:1037 ----
explain (costs off)
select * from int8_tbl where q1 in (select c1 from inner_text);

\echo ---- plan-diff 456: subselect.diff:1060 ----
explain (costs off)
select count(*) from tenk1 t
where (exists(select 1 from tenk1 k where k.unique1 = t.unique2) or ten < 0);

\echo ---- plan-diff 457: subselect.diff:1110, subselect.diff:1121 ----
explain (costs off)
select * from exists_tbl t1
  where (exists(select 1 from exists_tbl t2 where t1.c1 = t2.c2) or c3 < 0);

\echo ---- plan-diff 458: subselect.diff:1160 ----
explain (verbose, costs off)
  select x, x from
    (select (select now()) as x from (values(1),(2)) v(y)) ss;

\echo ---- plan-diff 459: subselect.diff:1188 ----
explain (verbose, costs off)
  select x, x from
    (select (select now() where y=y) as x from (values(1),(2)) v(y)) ss;

\echo ---- plan-diff 460: subselect.diff:1445 ----
-- another variant of that (bug #16213)
explain (verbose, costs off)
select * from
(values
  (3 not in (select * from (values (1), (2)) ss1)),
  (false)
) ss;

\echo ---- plan-diff 461: subselect.diff:1472 ----
explain (verbose, costs off)
select * from int4_tbl where
  (case when f1 in (select unique1 from tenk1 a) then f1 else null end) in
  (select ten from tenk1 b);

\echo ---- plan-diff 462: subselect.diff:1500 ----
explain (verbose, costs off)
select * from int4_tbl o where (f1, f1) in
  (select f1, generate_series(1,50) / 10 g from int4_tbl i group by f1);

\echo ---- plan-diff 463: subselect.diff:1679 ----
explain (verbose, costs off)
select * from
  (select 9 as x, unnest(array[1,2,3,11,12,13]) as u) ss
  where tattle(x, 8);

\echo ---- plan-diff 464: subselect.diff:1740 ----
explain (verbose, costs off)
select * from
  (select generate_series(1, ten) as g, count(*) from tenk1 group by 1) ss
  where ss.g = 1;

\echo ---- plan-diff 465: subselect.diff:1771, subselect.diff:1807 ----
explain (verbose, costs off)
select * from
  (select tattle(3, ten) as v, count(*) from tenk1 where unique1 < 3 group by 1) ss
  where ss.v;

\echo ---- plan-diff 466: subselect.diff:1914 ----
explain (verbose, costs off)
select * from json_tab t1 left join (select json_array(1, a) from json_tab t2) s on false;

\echo ---- plan-diff 467: subselect.diff:2444 ----
-- Basic subquery that can be inlined
explain (verbose, costs off)
with x as (select * from (select f1 from subselect_tbl) ss)
select * from x where f1 = 1;

\echo ---- plan-diff 468: subselect.diff:2456 ----
-- Explicitly request materialization
explain (verbose, costs off)
with x as materialized (select * from (select f1 from subselect_tbl) ss)
select * from x where f1 = 1;

\echo ---- plan-diff 469: subselect.diff:2481 ----
-- Volatile functions prevent inlining
explain (verbose, costs off)
with x as (select * from (select f1, random() from subselect_tbl) ss)
select * from x where f1 = 1;

\echo ---- plan-diff 470: subselect.diff:2654 ----
-- Check handling of outer references
explain (verbose, costs off)
with x as (select * from int4_tbl)
select * from (with y as (select * from x) select * from y) ss;

\echo ---- plan-diff 471: subselect.diff:2668 ----
explain (verbose, costs off)
with x as materialized (select * from int4_tbl)
select * from (with y as (select * from x) select * from y) ss;

\echo ---- plan-diff 472: subselect.diff:2677 ----
-- Ensure that we inline the correct CTE when there are
-- multiple CTEs with the same name
explain (verbose, costs off)
with x as (select 1 as y)
select * from (with x as (select 2 as y) select * from x) ss;

\echo ---- plan-diff 473: subselect.diff:2729 ----
-- we should only try to pull up the sublink into RHS of a left join
-- but a.hundred is not available.
explain (costs off)
SELECT * FROM tenk1 A LEFT JOIN tenk2 B
ON A.hundred in (SELECT c.hundred FROM tenk2 C WHERE c.odd = b.odd);

\echo ---- plan-diff 474: subselect.diff:2746 ----
-- we should only try to pull up the sublink into RHS of a left join
-- but a.odd is not available for this.
explain (costs off)
SELECT * FROM tenk1 A LEFT JOIN tenk2 B
ON B.hundred in (SELECT c.hundred FROM tenk2 C WHERE c.odd = a.odd);

\echo ---- plan-diff 475: subselect.diff:2762 ----
-- should be able to pull up since all the references are available.
explain (costs off)
SELECT * FROM tenk1 A LEFT JOIN tenk2 B
ON B.hundred in (SELECT c.hundred FROM tenk2 C WHERE c.odd = b.odd);

\echo ---- plan-diff 476: subselect.diff:2781 ----
-- we can pull up the sublink into the inner JoinExpr.
explain (costs off)
SELECT * FROM tenk1 A INNER JOIN tenk2 B
ON A.hundred in (SELECT c.hundred FROM tenk2 C WHERE c.odd = b.odd)
WHERE a.thousand < 750;

\echo ---- plan-diff 477: subselect.diff:2828 ----
-- VtA transformation for joined VALUES is not supported
EXPLAIN (COSTS OFF)
SELECT * FROM onek, (VALUES('RFAAAA'), ('VJAAAA')) AS v (i)
  WHERE onek.stringu1 = v.i;

\echo ---- plan-diff 478: subselect.diff:2841 ----
-- VtA transformation for a composite argument is not supported
EXPLAIN (COSTS OFF)
SELECT * FROM onek
  WHERE (unique1,ten) IN (VALUES (1,1), (20,0), (99,9), (17,99))
  ORDER BY unique1;

\echo ---- plan-diff 479: subselect.diff:2883 ----
-- Recursive evaluation of constant queries is not yet supported
EXPLAIN (COSTS OFF)
SELECT * FROM onek
  WHERE unique1 IN (SELECT x * x FROM (VALUES(1200), (1)) AS x(x));

\echo ---- plan-diff 480: subselect.diff:2926 ----
-- VtA shouldn't depend on the side of the join probing with the VALUES expression.
EXPLAIN (COSTS OFF)
SELECT c.unique1,c.ten FROM tenk1 c JOIN onek a USING (ten)
WHERE a.ten IN (VALUES (1), (2));

\echo ---- plan-diff 481: subselect.diff:2973 ----
EXPLAIN (COSTS OFF)
-- VtA allows NULLs in the list
SELECT ten FROM onek WHERE sin(two)+four IN (VALUES (sin(0.5)), (NULL), (2));

\echo ---- plan-diff 482: subselect.diff:3066 ----
EXPLAIN (COSTS OFF)
SELECT ten FROM onek t
WHERE unique1 IN (VALUES (0), ((2 IN (SELECT unique2 FROM onek c
  WHERE c.unique2 IN (VALUES (sin(0.5)), (2))))::integer));

\echo ---- plan-diff 483: subselect.diff:3085 ----
-- VtA is not allowed with subqueries
EXPLAIN (COSTS OFF)
SELECT ten FROM onek t WHERE unique1 IN (VALUES (0), ((2 IN
  (SELECT (3)))::integer)
);

\echo ==== tidrangescan ====
\echo ---- plan-diff 484: tidrangescan.diff:5 ----
-- empty table
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid < '(1, 0)';

\echo ---- plan-diff 485: tidrangescan.diff:18 ----
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid > '(9, 0)';

\echo ---- plan-diff 486: tidrangescan.diff:40 ----
-- range scans with upper bound
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid < '(1,0)';

\echo ---- plan-diff 487: tidrangescan.diff:63 ----
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid <= '(1,5)';

\echo ---- plan-diff 488: tidrangescan.diff:91 ----
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid < '(0,0)';

\echo ---- plan-diff 489: tidrangescan.diff:105 ----
-- range scans with lower bound
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid > '(2,8)';

\echo ---- plan-diff 490: tidrangescan.diff:120 ----
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE '(2,8)' < ctid;

\echo ---- plan-diff 491: tidrangescan.diff:135 ----
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid >= '(2,8)';

\echo ---- plan-diff 492: tidrangescan.diff:151 ----
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid >= '(100,0)';

\echo ---- plan-diff 493: tidrangescan.diff:165 ----
-- range scans with both bounds
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE ctid > '(1,4)' AND '(1,7)' >= ctid;

\echo ---- plan-diff 494: tidrangescan.diff:181 ----
EXPLAIN (COSTS OFF)
SELECT ctid FROM tidrangescan WHERE '(1,7)' >= ctid AND ctid > '(1,4)';

\echo ---- plan-diff 495: tidrangescan.diff:258 ----
-- cursors
-- Ensure we get a TID Range scan without a Materialize node.
EXPLAIN (COSTS OFF)
DECLARE c SCROLL CURSOR FOR SELECT ctid FROM tidrangescan WHERE ctid < '(1,0)';

\echo ==== tsearch ====
\echo ---- plan-diff 496: tsearch.diff:2519 ----
-- Test inlining of immutable constant functions
-- to_tsquery(text) is not immutable, so it won't be inlined
explain (costs off)
select * from test_tsquery, to_tsquery('new') q where txtsample @@ q;

\echo ---- plan-diff 497: tsearch.diff:2531 ----
-- to_tsquery(regconfig, text) is an immutable function.
-- That allows us to get rid of using function scan and join at all.
explain (costs off)
select * from test_tsquery, to_tsquery('english', 'new') q where txtsample @@ q;

\echo ==== tsrf ====
\echo ---- plan-diff 498: tsrf.diff:62 ----
-- check proper nesting of SRFs in different expressions
explain (verbose, costs off)
SELECT generate_series(1, generate_series(1, 3)), generate_series(2, 4);

\echo ---- plan-diff 499: tsrf.diff:87 ----
-- SRF with a provably-dummy relation
explain (verbose, costs off)
SELECT unnest(ARRAY[1, 2]) FROM few WHERE false;

\echo ---- plan-diff 500: tsrf.diff:104 ----
-- SRF shouldn't prevent upper query from recognizing lower as dummy
explain (verbose, costs off)
SELECT * FROM few f1,
  (SELECT unnest(ARRAY[1,2]) FROM few f2 WHERE false OFFSET 0) ss;

\echo ---- plan-diff 501: tsrf.diff:458, tsrf.diff:491, tsrf.diff:637 ----
-- case with degenerate ORDER BY
explain (verbose, costs off)
select 'foo' as f, generate_series(1,2) as g from few order by 1;

\echo ---- plan-diff 502: tsrf.diff:667 ----
explain (verbose, costs off)
select generate_series(1,3)+1 order by generate_series(1,3);

\echo ---- plan-diff 503: tsrf.diff:690 ----
-- Check that SRFs of same nesting level run in lockstep
explain (verbose, costs off)
select generate_series(1,3) as x, generate_series(3,6) + 1 as y;

\echo ==== union ====
\echo ---- plan-diff 504: union.diff:351, union.diff:437 ----
explain (costs off)
select count(*) from
  ( select unique1 from tenk1 union select fivethous from tenk1 ) ss;

\echo ---- plan-diff 505: union.diff:371, union.diff:459 ----
explain (costs off)
select count(*) from
  ( select unique1 from tenk1 intersect select fivethous from tenk1 ) ss;

\echo ---- plan-diff 506: union.diff:390, union.diff:478 ----
explain (costs off)
select unique1 from tenk1 except select unique2 from tenk1 where unique2 != 10;

\echo ---- plan-diff 507: union.diff:408 ----
-- the hashed implementation is sensitive to child plans' tuple slot types
explain (costs off)
select * from int8_tbl intersect select q2, q1 from int8_tbl order by 1, 2;

\echo ---- plan-diff 508: union.diff:514, union.diff:527 ----
explain (costs off)
select x from (values ('11'::varbit), ('10'::varbit)) _(x) union select x from (values ('11'::varbit), ('10'::varbit)) _(x);

\echo ---- plan-diff 509: union.diff:541, union.diff:550, union.diff:613 ----
explain (costs off)
select x from (values (array[1, 2]), (array[1, 3])) _(x) union select x from (values (array[1, 2]), (array[1, 4])) _(x);

\echo ---- plan-diff 510: union.diff:573, union.diff:649 ----
explain (costs off)
select x from (values (array[1, 2]), (array[1, 3])) _(x) except select x from (values (array[1, 2]), (array[1, 4])) _(x);

\echo ---- plan-diff 511: union.diff:592 ----
-- non-hashable type
explain (costs off)
select x from (values (array['10'::varbit]), (array['11'::varbit])) _(x) union select x from (values (array['10'::varbit]), (array['01'::varbit])) _(x);

\echo ---- plan-diff 512: union.diff:630 ----
explain (costs off)
select x from (values (array[1, 2]), (array[1, 3])) _(x) intersect select x from (values (array[1, 2]), (array[1, 4])) _(x);

\echo ---- plan-diff 513: union.diff:674, union.diff:780 ----
explain (costs off)
select x from (values (row(1, 2)), (row(1, 3))) _(x) union select x from (values (row(1, 2)), (row(1, 4))) _(x);

\echo ---- plan-diff 514: union.diff:691, union.diff:797 ----
explain (costs off)
select x from (values (row(1, 2)), (row(1, 3))) _(x) intersect select x from (values (row(1, 2)), (row(1, 4))) _(x);

\echo ---- plan-diff 515: union.diff:710, union.diff:816, union.diff:837 ----
explain (costs off)
select x from (values (row(1, 2)), (row(1, 3))) _(x) except select x from (values (row(1, 2)), (row(1, 4))) _(x);

\echo ---- plan-diff 516: union.diff:735 ----
-- non-hashable type
-- With an anonymous row type, the typcache does not report that the
-- type is hashable.  (Otherwise, this would fail at execution time.)
explain (costs off)
select x from (values (row('10'::varbit)), (row('11'::varbit))) _(x) union select x from (values (row('10'::varbit)), (row('01'::varbit))) _(x);

\echo ---- plan-diff 517: union.diff:758 ----
explain (costs off)
select x from (values (row('10'::varbit)::ct1), (row('11'::varbit)::ct1)) _(x) union select x from (values (row('10'::varbit)::ct1), (row('01'::varbit)::ct1)) _(x);

\echo ---- plan-diff 518: union.diff:988 ----
-- We've no way to check hashed UNION as the empty pathkeys in the Append are
-- fine to make use of Unique, which is cheaper than HashAggregate and we've
-- no means to disable Unique.
explain (costs off)
select from generate_series(1,5) intersect select from generate_series(1,3);

\echo ---- plan-diff 519: union.diff:1030 ----
explain (costs off)
select from generate_series(1,5) intersect select from generate_series(1,3);

\echo ---- plan-diff 520: union.diff:1162 ----
explain (costs off)
  SELECT * FROM
  (SELECT a || b AS ab FROM t1
   UNION ALL
   SELECT ab FROM t2) t
  ORDER BY 1 LIMIT 8;

\echo ---- plan-diff 521: union.diff:1412 ----
explain (costs off)
select * from
  (select * from t3 a union all select * from t3 b) ss
  join int4_tbl on f1 = expensivefunc(x);

\echo ==== updatable_views ====
\echo ---- plan-diff 522: updatable_views.diff:537 ----
EXPLAIN (costs off) UPDATE rw_view1 SET a=6 WHERE a=5;

\echo ---- plan-diff 523: updatable_views.diff:943 ----
EXPLAIN (costs off) DELETE FROM rw_view2 WHERE aaa=4;

\echo ---- plan-diff 524: updatable_views.diff:1237 ----
EXPLAIN (costs off) UPDATE rw_view2 SET a=3 WHERE a=2;

\echo ---- plan-diff 525: updatable_views.diff:1361 ----
EXPLAIN (costs off)
UPDATE rw_view1 v SET bb='Updated row 2' WHERE rw_view1_aa(v)=2
  RETURNING rw_view1_aa(v), v.bb;

\echo ---- plan-diff 526: updatable_views.diff:2080 ----
EXPLAIN (verbose, costs off) UPDATE rw_view1 SET b = b + 1 RETURNING *;

\echo ---- plan-diff 527: updatable_views.diff:2438 ----
EXPLAIN (costs off)
UPDATE rw_view1 SET a = a + 1000 FROM other_tbl_parent WHERE a = id;

\echo ---- plan-diff 528: updatable_views.diff:2747 ----
EXPLAIN (costs off) INSERT INTO rw_view1 VALUES (5);

\echo ---- plan-diff 529: updatable_views.diff:3006 ----
EXPLAIN (costs off) UPDATE rw_view1 SET person=person WHERE snoop(person);

\echo ---- plan-diff 530: updatable_views.diff:3104 ----
EXPLAIN (costs off) UPDATE rw_view2 SET person=person WHERE snoop(person);

\echo ---- plan-diff 531: updatable_views.diff:3168 ----
EXPLAIN (costs off) DELETE FROM rw_view1 WHERE id = 1 AND snoop(data);

\echo ---- plan-diff 532: updatable_views.diff:3241 ----
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE v1 SET a=100 WHERE snoop(a) AND leakproof(a) AND a < 7 AND a != 6;

\echo ---- plan-diff 533: updatable_views.diff:3288, updatable_views.diff:3503 ----
EXPLAIN (VERBOSE, COSTS OFF)
UPDATE v1 SET a=a+1 WHERE snoop(a) AND leakproof(a) AND a = 8;

\echo ---- plan-diff 534: updatable_views.diff:3682 ----
explain (costs off)
insert into uv_iocu_view (a, b) values ('xyxyxy', 3)
   on conflict (a) do update set b = excluded.b where excluded.c > 0;

\echo ==== with ====
\echo ---- plan-diff 535: with.diff:678 ----
-- test that column statistics from a materialized CTE are available
-- to upper planner (otherwise, we'd get a stupider plan)
explain (costs off)
with x as materialized (select unique1 from tenk1 b)
select count(*) from tenk1 a
  where unique1 in (select * from x);

\echo ---- plan-diff 536: with.diff:714 ----
-- test that pathkeys from a materialized CTE are propagated up to the
-- outer query
explain (costs off)
with x as materialized (select unique1 from tenk1 b order by unique1)
select count(*) from tenk1 a
  where unique1 in (select * from x);

\echo ---- plan-diff 537: with.diff:2922 ----
-- check case where CTE reference is removed due to optimization
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q1 FROM
(
  WITH t_cte AS (SELECT * FROM int8_tbl t)
  SELECT q1, (SELECT q2 FROM t_cte WHERE t_cte.q1 = i8.q1) AS t_sub
  FROM int8_tbl i8
) ss;

\echo ---- plan-diff 538: with.diff:2951 ----
EXPLAIN (VERBOSE, COSTS OFF)
SELECT q1 FROM
(
  WITH t_cte AS MATERIALIZED (SELECT * FROM int8_tbl t)
  SELECT q1, (SELECT q2 FROM t_cte WHERE t_cte.q1 = i8.q1) AS t_sub
  FROM int8_tbl i8
) ss;

\echo ---- plan-diff 539: with.diff:3623 ----
-- check EXPLAIN VERBOSE for a wCTE with RETURNING
EXPLAIN (VERBOSE, COSTS OFF)
WITH wcte AS ( INSERT INTO int8_tbl VALUES ( 42, 47 ) RETURNING q2 )
DELETE FROM a_star USING wcte WHERE aa = q2;

\echo ==== xml ====
\echo ---- plan-diff 540: xml.diff:1350 ----
EXPLAIN (COSTS OFF) SELECT * FROM xmltableview1;

\echo ---- plan-diff 541: xml.diff:1574 ----
EXPLAIN (VERBOSE, COSTS OFF)
SELECT f.* FROM xmldata, LATERAL xmltable('/ROWS/ROW[COUNTRY_NAME="Japan" or COUNTRY_NAME="India"]' PASSING data COLUMNS "COUNTRY_NAME" text, "REGION_ID" int) AS f WHERE "COUNTRY_NAME" = 'Japan';
