-- interval-lane differential corpus: interval I/O (all four IntervalStyles,
-- ISO8601/postgres/sql_standard verbose input forms), typmod encode/rounding,
-- arithmetic incl. DST-aware timestamptz +/- interval, comparisons, justify,
-- part/extract, trunc, make_interval, age, izone/timetz zone forms,
-- SET TIME ZONE INTERVAL, OVERLAPS, error arms (22007/22008/22015/22023).
-- Run via scripts/regress-diff.sh --sql fixtures/interval-e2e.sql.
\set VERBOSITY verbose
\pset pager off

SET TIME ZONE 'UTC';
SET datestyle = 'ISO, MDY';
SET intervalstyle = 'postgres';

-- ===== interval_in: postgres + SQL + ISO8601 formats =====
SELECT '1 year 2 mons 3 days 04:05:06.789'::interval;
SELECT '-1 day +5 hours'::interval;
SELECT '1-2'::interval;
SELECT '3 12:34:56'::interval;
SELECT '@ 1 day ago'::interval;
SELECT '@ 2 years 5 months 12 hours ago'::interval;
SELECT 'P1Y2M3DT4H5M6.7S'::interval;
SELECT 'P0001-02-03T04:05:06'::interval;
SELECT 'PT0S'::interval;
SELECT 'P-1M10DT-1H'::interval;
SELECT 'infinity'::interval;
SELECT '-infinity'::interval;
SELECT '1.5 mons'::interval;
SELECT '1.5 weeks'::interval;
SELECT '1.5 days'::interval;
SELECT '0.7 secs'::interval;
SELECT '2 years -5 months 3 days -10:00:00'::interval;
SELECT '12:34'::interval;
SELECT '12:34:56.789'::interval;
SELECT '-00:00:01'::interval;
SELECT interval '1' second;
SELECT interval '1' minute;
SELECT interval '100' hour;
SELECT interval '7' day;
SELECT interval '4' month;
SELECT interval '3' year;
SELECT interval '1 2:03:04' day to second;
SELECT interval '1 2:03:04.5678' day to second(2);
SELECT interval '12:34' hour to minute;
SELECT interval '12:34' minute to second;
SELECT interval '5 4:03:02.1' day to minute;
SELECT interval '1-2' year to month;
SELECT interval(3) '1.123456 sec';
SELECT interval '2.7 secs'(0);
SELECT '1 day 02:03:04.55555'::interval(3);
SELECT interval '999' second(2);

-- ===== error arms =====
SELECT 'bogus'::interval;
SELECT '100000000000000000000 years'::interval;
SELECT '2147483647 years'::interval;
SELECT interval '1 day' second(9);
SELECT '5 seconds 3 minutes'::interval;
SELECT '1 year ago 2 days'::interval;

-- ===== IntervalStyle output styles =====
SET intervalstyle = 'sql_standard';
SELECT '1 year 2 mons 3 days 04:05:06.789'::interval;
SELECT '-1 year -2 mons -3 days -04:05:06.789'::interval;
SELECT '-1 day +5 hours'::interval;
SELECT '1-2'::interval;
SELECT '04:05:06'::interval;
SET intervalstyle = 'postgres_verbose';
SELECT '1 year 2 mons 3 days 04:05:06.789'::interval;
SELECT '-1 day +5 hours'::interval;
SELECT '0'::interval;
SELECT '@ 1 day ago'::interval;
SET intervalstyle = 'iso_8601';
SELECT '1 year 2 mons 3 days 04:05:06.789'::interval;
SELECT '-1 day +5 hours'::interval;
SELECT '0'::interval;
SET intervalstyle = 'postgres';

-- ===== sql_standard input sign semantics =====
SET intervalstyle = 'sql_standard';
SELECT interval '-1 2:03:04';
SELECT interval '-1-2 3 -4:05:06';
SET intervalstyle = 'postgres';
SELECT interval '-1 2:03:04';

-- ===== arithmetic: interval +/- interval, unary minus =====
SELECT interval '1 day 3 hours' + interval '2 mons -1 day';
SELECT interval '3 mons' - interval '1 day';
SELECT - interval '1 mon -3 days +04:00:00';
SELECT interval 'infinity' + interval '1 day';
SELECT interval '-infinity' + interval '1 day';
SELECT interval 'infinity' + interval '-infinity';
SELECT interval 'infinity' - interval 'infinity';
SELECT - interval '-infinity';

-- ===== timestamp/date/time/timetz +/- interval =====
SELECT timestamp '2025-01-31 10:00:00' + interval '1 mon';
SELECT timestamp '2025-01-31 10:00:00' - interval '1 mon';
SELECT timestamp '2024-02-29 10:00:00' + interval '1 year';
SELECT timestamp '2025-03-15 12:00:00' - interval '90 days';
SELECT timestamp '2025-03-15 12:00:00' + interval '1 mon 2 days 03:04:05.678';
SELECT timestamp 'infinity' + interval '1 day';
SELECT timestamp 'infinity' + interval '-infinity';
SELECT date '2025-06-01' + interval '1 mon 2 days 03:04:05';
SELECT date '2025-06-01' - interval '36 hours';
SELECT time '23:30:00' + interval '2 hours';
SELECT time '01:00:00' - interval '2 hours';
SELECT time '12:00:00' + interval '1 day 1 hour';
SELECT timetz '10:00:00-04' + interval '30 hours';
SELECT timetz '01:00:00+05:30' - interval '2 hours';
SELECT time '12:00:00' + interval 'infinity';

-- ===== DST-aware timestamptz arithmetic: '1 day' vs '24 hours' =====
SET TIME ZONE 'America/New_York';
SELECT timestamptz '2025-03-08 12:00:00' + interval '1 day';
SELECT timestamptz '2025-03-08 12:00:00' + interval '24 hours';
SELECT timestamptz '2025-11-01 12:00:00' + interval '1 day';
SELECT timestamptz '2025-11-01 12:00:00' + interval '24 hours';
SELECT timestamptz '2025-03-09 12:00:00' - interval '1 day';
SELECT timestamptz '2025-03-09 12:00:00' - interval '24 hours';
SELECT timestamptz '2025-03-08 01:30:00' + interval '1 mon';
SELECT timestamptz '2025-01-31 23:30:00' + interval '1 mon';
SELECT timestamptz '2025-11-02 01:30:00-04' + interval '1 hour';

-- ===== timestamp_mi and age =====
SELECT timestamp '2025-01-02 03:00:00' - timestamp '2024-12-31 00:00:00';
SELECT timestamp '2024-12-31 00:00:00' - timestamp '2025-01-02 03:00:00';
SELECT timestamptz '2025-11-02 06:30:00+00' - timestamptz '2025-11-01 06:30:00+00';
SELECT timestamp 'infinity' - timestamp '2025-01-01';
SELECT timestamp 'infinity' - timestamp 'infinity';
SELECT age(timestamp '2025-03-15 10:30:00', timestamp '2024-01-20 08:00:00');
SELECT age(timestamp '2024-01-20 08:00:00', timestamp '2025-03-15 10:30:00');
SELECT age(timestamp '2025-03-01', timestamp '2025-01-30');
SELECT age(timestamptz '2025-03-15 10:30:00+00', timestamptz '2024-01-20 08:00:00+00');
SELECT age(timestamp 'infinity', timestamp '2025-01-01');
SELECT age(timestamp 'infinity', timestamp 'infinity');

-- ===== comparisons + hash-driven plans =====
SELECT interval '30 days' = interval '1 mon';
SELECT interval '24 hours' = interval '1 day';
SELECT interval '25 hours' > interval '1 day';
SELECT interval '1 mon' < interval '31 days';
SELECT interval 'infinity' > interval '999999 years';
SELECT interval '-infinity' < interval '-999999 years';

-- ===== justify + make_interval =====
SELECT justify_hours(interval '27 hours');
SELECT justify_days(interval '35 days');
SELECT justify_interval(interval '29 days 25 hours');
SELECT justify_interval(interval '1 mon -1 hour');
SELECT justify_hours(interval '-30 hours');
SELECT justify_days(interval '-35 days');
SELECT justify_interval(interval 'infinity');
SELECT make_interval(1, 2, 3, 4, 5, 6, 7.5);
SELECT make_interval(0, 0, 0, 0, 0, 0, -0.5);
SELECT make_interval(0, 0, 0, 0, 0, 0, 'Infinity'::float8);

-- ===== extract / date_part / date_trunc on interval =====
SELECT date_part('day', interval '2 years 5 mons 10 days 12:30:45.678');
SELECT date_part('epoch', interval '2 years 5 mons 10 days 12:30:45.678');
SELECT extract(epoch from interval '2 years 5 mons 10 days 12:30:45.678');
SELECT extract(year from interval '2 years 5 mons'), extract(month from interval '2 years 5 mons');
SELECT extract(quarter from interval '-4 mons');
SELECT extract(second from interval '00:00:45.678');
SELECT extract(millisecond from interval '00:00:45.678');
SELECT extract(microseconds from interval '00:00:45.678');
SELECT extract(hour from interval '123:45:06');
SELECT extract(decade from interval '25 years'), extract(century from interval '250 years'), extract(millennium from interval '2500 years');
SELECT extract(week from interval '15 days');
SELECT extract(month from '-infinity'::interval);
SELECT extract(year from 'infinity'::interval);
SELECT extract(epoch from '-infinity'::interval);
SELECT extract(timezone from interval '1 day');
SELECT extract(bogus from interval '1 day');
SELECT date_trunc('hour', interval '2 years 5 mons 10 days 12:30:45.678');
SELECT date_trunc('month', interval '2 years 5 mons 10 days 12:30:45.678');
SELECT date_trunc('second', interval '2 years 5 mons 10 days 12:30:45.678');
SELECT date_trunc('milliseconds', interval '00:00:45.678999');
SELECT date_trunc('decade', interval '25 years 3 mons');
SELECT date_trunc('week', interval '1 year');
SELECT date_trunc('day', interval 'infinity');

-- ===== extract on date/time/timetz =====
SELECT extract(year from date '2025-03-15'), extract(dow from date '2025-03-15'), extract(doy from date '2025-03-15');
SELECT extract(epoch from date '2025-03-15');
SELECT extract(hour from time '12:34:56.789'), extract(minute from time '12:34:56.789');
SELECT extract(second from time '12:34:56.789'), extract(epoch from time '12:34:56.789');
SELECT date_part('second', time '12:34:56.789');
SELECT extract(timezone from timetz '12:34:56-04:30'), extract(timezone_hour from timetz '12:34:56-04:30'), extract(timezone_minute from timetz '12:34:56-04:30');
SELECT extract(epoch from timetz '13:30:25.123456-04:30');
SELECT extract(day from time '12:00:00');

-- ===== isfinite, casts, time/interval conversions =====
SELECT isfinite(interval '1 day'), isfinite(interval 'infinity'), isfinite(interval '-infinity');
SELECT time '12:34:56'::interval;
SELECT interval '25:34:56'::time;
SELECT interval '-1 hour'::time;
SELECT interval 'infinity'::time;

-- ===== timezone(interval/text, ...) forms =====
SET TIME ZONE 'UTC';
SELECT timezone(interval '-5 hours', timestamp '2025-06-01 12:00:00');
SELECT timezone(interval '5 hours 30 minutes', timestamptz '2025-06-01 12:00:00+00');
SELECT timezone(interval '1 mon', timestamp '2025-06-01 12:00:00');
SELECT timezone(interval 'infinity', timestamp '2025-06-01 12:00:00');
SELECT timezone('EST', timetz '12:00:00+00');
SELECT timezone(interval '-5 hours', timetz '12:00:00+00');
SELECT timezone(interval '+05:30', timetz '23:00:00-08');
SELECT timezone(interval '1 mon', timetz '12:00:00+00');
SELECT timestamp '2025-06-01 12:00:00' AT TIME ZONE interval '-5 hours';

-- ===== SET TIME ZONE INTERVAL =====
SET TIME ZONE INTERVAL '+05:30' HOUR TO MINUTE;
SHOW timezone;
SELECT '2025-01-01 00:00:00+00'::timestamptz;
SET TIME ZONE INTERVAL '-08:00' HOUR TO MINUTE;
SHOW timezone;
SELECT '2025-01-01 00:00:00+00'::timestamptz;
SET TIME ZONE 'UTC';

-- ===== OVERLAPS =====
SELECT (timestamp '2025-01-01', timestamp '2025-02-01') OVERLAPS (timestamp '2025-01-15', timestamp '2025-03-01');
SELECT (timestamp '2025-01-01', timestamp '2025-02-01') OVERLAPS (timestamp '2025-02-01', timestamp '2025-03-01');
SELECT (timestamp '2025-02-01', timestamp '2025-01-01') OVERLAPS (timestamp '2025-01-15', timestamp '2025-01-20');
SELECT (timestamp '2025-01-01', NULL::timestamp) OVERLAPS (timestamp '2025-01-15', timestamp '2025-03-01');
SELECT (NULL::timestamp, NULL::timestamp) OVERLAPS (timestamp '2025-01-15', timestamp '2025-03-01');

-- ===== interval columns e2e: CREATE/INSERT/SELECT/ORDER BY/GROUP BY =====
CREATE TABLE iv_e2e(id int, iv interval);
INSERT INTO iv_e2e VALUES (1, '1 day'), (2, '25 hours'), (3, '-1 mon'), (4, '30 days'), (5, '1 mon'), (6, NULL), (7, '24 hours');
SELECT id, iv FROM iv_e2e ORDER BY iv, id;
SELECT iv, count(*) FROM iv_e2e GROUP BY iv ORDER BY iv;
SELECT id, iv FROM iv_e2e WHERE iv > interval '20 hours' ORDER BY id;
SELECT id, iv + interval '1 hour' FROM iv_e2e ORDER BY id;
DROP TABLE iv_e2e;

-- ===== interval columns with typmod =====
CREATE TABLE iv_tm(a interval hour to minute, b interval(2), c interval day to second(1), d interval year to month);
INSERT INTO iv_tm VALUES ('1 day 02:03:04.5', '00:00:00.789', '5 days 01:02:03.45', '2 years 7 mons');
SELECT * FROM iv_tm;
DROP TABLE iv_tm;
