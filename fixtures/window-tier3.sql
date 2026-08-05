-- window tier-3 differential fixture: by-ref RANGE offsets (timestamp /
-- interval / numeric in_range), GROUPS mode, all EXCLUDE variants,
-- runCondition (Run Condition EXPLAIN shape + pass-through execution),
-- and finalfn/moving aggregates (numeric/int8 avg/sum/stddev).
-- Diffed against real C 18.3.
\set VERBOSITY verbose
CREATE TABLE wt3 (id int, grp int, n numeric, ts timestamp, tstz timestamptz, d date, t time, iv interval);
INSERT INTO wt3 SELECT g, g % 3,
  (g * 10 + g % 4)::numeric / 4,
  '2024-01-01'::timestamp + (g || ' hours')::interval,
  '2024-01-01 00:00:00+00'::timestamptz + (g || ' hours')::interval,
  '2024-01-01'::date + g,
  '00:00'::time + (g || ' minutes')::interval,
  (g || ' days')::interval
FROM generate_series(1, 12) g;
INSERT INTO wt3 VALUES (13, 1, NULL, NULL, NULL, NULL, NULL, NULL);
-- RANGE offsets over by-ref types (in_range support family)
SELECT id, ts, count(*) OVER (ORDER BY ts RANGE BETWEEN '3 hours'::interval PRECEDING AND '2 hours'::interval FOLLOWING) FROM wt3;
SELECT id, tstz, sum(id) OVER (ORDER BY tstz RANGE BETWEEN '5 hours'::interval PRECEDING AND CURRENT ROW) FROM wt3;
SELECT id, d, count(*) OVER (ORDER BY d RANGE BETWEEN '2 days'::interval PRECEDING AND '1 day'::interval FOLLOWING) FROM wt3;
SELECT id, t, count(*) OVER (ORDER BY t RANGE BETWEEN '2 minutes'::interval PRECEDING AND '2 minutes'::interval FOLLOWING) FROM wt3;
SELECT id, iv, count(*) OVER (ORDER BY iv RANGE BETWEEN '2 days'::interval PRECEDING AND '2 days'::interval FOLLOWING) FROM wt3;
SELECT id, n, sum(id) OVER (ORDER BY n RANGE BETWEEN 5.0 PRECEDING AND 5.0 FOLLOWING) FROM wt3;
SELECT id, ts, first_value(id) OVER (ORDER BY ts DESC RANGE BETWEEN '3 hours'::interval PRECEDING AND '3 hours'::interval FOLLOWING) FROM wt3;
-- GROUPS frame mode
SELECT id, grp, sum(id) OVER (ORDER BY grp GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM wt3;
SELECT id, grp, count(*) OVER (ORDER BY grp GROUPS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM wt3;
SELECT id, grp, last_value(id) OVER (ORDER BY grp GROUPS BETWEEN CURRENT ROW AND 1 FOLLOWING) FROM wt3;
-- EXCLUDE variants (ROWS / RANGE / GROUPS)
SELECT id, sum(id) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING EXCLUDE CURRENT ROW) FROM wt3;
SELECT id, grp, sum(id) OVER (ORDER BY grp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE GROUP) FROM wt3;
SELECT id, grp, sum(id) OVER (ORDER BY grp RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW EXCLUDE TIES) FROM wt3;
SELECT id, grp, array_agg(id) OVER (ORDER BY grp GROUPS BETWEEN 1 PRECEDING AND 1 FOLLOWING EXCLUDE GROUP) FROM wt3;
SELECT id, first_value(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 3 FOLLOWING EXCLUDE CURRENT ROW) FROM wt3;
SELECT id, last_value(id) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 3 FOLLOWING EXCLUDE TIES) FROM wt3;
SELECT id, grp, nth_value(id, 2) OVER (ORDER BY grp RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING EXCLUDE GROUP) FROM wt3;
-- finalfn / moving aggregates over by-ref & internal transtypes
SELECT id, avg(n) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM wt3;
SELECT id, sum(n) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM wt3;
SELECT id, avg(id) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) FROM wt3;
SELECT id, stddev_samp(id) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM wt3;
SELECT id, var_pop(n) OVER (ORDER BY id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) FROM wt3;
SELECT grp, avg(iv) OVER (PARTITION BY grp ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wt3;
SELECT id, avg(n) OVER () FROM wt3;
SELECT id, every(id % 2 = 0) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) FROM wt3;
-- runCondition: EXPLAIN shape + execution (monotonic early-out)
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT id, row_number() OVER (ORDER BY id) rn FROM wt3) s WHERE rn <= 3;
SELECT * FROM (SELECT id, row_number() OVER (ORDER BY id) rn FROM wt3) s WHERE rn <= 3;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT id, grp, rank() OVER (PARTITION BY grp ORDER BY id) rk FROM wt3) s WHERE rk < 2;
SELECT * FROM (SELECT id, grp, rank() OVER (PARTITION BY grp ORDER BY id) rk FROM wt3) s WHERE rk < 2;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT id, count(*) OVER (ORDER BY id) c FROM wt3) s WHERE s.c = 5;
SELECT * FROM (SELECT id, count(*) OVER (ORDER BY id) c FROM wt3) s WHERE s.c = 5;
EXPLAIN (COSTS OFF) SELECT * FROM (SELECT id, ntile(4) OVER (ORDER BY id) nt, row_number() OVER (ORDER BY id) rn FROM wt3) s WHERE nt <= 2 AND rn <= 6;
SELECT * FROM (SELECT id, ntile(4) OVER (ORDER BY id) nt, row_number() OVER (ORDER BY id) rn FROM wt3) s WHERE nt <= 2 AND rn <= 6;
DROP TABLE wt3;
