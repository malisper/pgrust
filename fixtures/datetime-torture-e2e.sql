-- datetime-lane torture corpus: typmod DDL round trips, EXTRACT matrix over
-- edge dates (BC, epoch boundaries, far future, infinities), DST-transition
-- arithmetic incl. date_add/date_subtract at zone, to_char/to_timestamp
-- format matrix over datestyles incl. TZ abbrev from_char, make_interval
-- named args, interval avg/sum with infinities.
-- Run via scripts/regress-diff.sh --sql fixtures/datetime-torture-e2e.sql.
-- Excluded (known non-datetime gaps, retest when their lanes land): psql \d
-- (IN-subquery hits the subquery-scan lane panic); >max-precision typmods
-- (our WARNINGs lack the parser errposition LINE/caret C attaches via the
-- pstate error-context callback).
\set VERBOSITY verbose
\pset pager off

SET TIME ZONE 'UTC';
SET datestyle = 'ISO, MDY';
SET intervalstyle = 'postgres';

-- ===== typmod I/O: precision DDL + regurgitation =====
CREATE TABLE dtt_typmod (
  t0 timestamp(0), t2 timestamp(2), t6 timestamp(6),
  z0 timestamptz(0), z3 timestamptz(3),
  h2 time(2), htz4 timetz(4),
  iv2 interval second(2), ivm interval minute
);
SELECT attname, format_type(atttypid, atttypmod) FROM pg_attribute
  WHERE attrelid = 'dtt_typmod'::regclass AND attnum > 0 ORDER BY attnum;
INSERT INTO dtt_typmod VALUES (
  '2021-06-01 12:34:56.789999', '2021-06-01 12:34:56.789999', '2021-06-01 12:34:56.789999',
  '2021-06-01 12:34:56.789999+02', '2021-06-01 12:34:56.789999+02',
  '23:59:59.987654', '23:59:59.987654-07',
  '1 day 02:03:04.5678', '1 day 02:03:04.5678');
SELECT * FROM dtt_typmod;
SELECT pg_typeof(t2), pg_typeof(z3), pg_typeof(h2), pg_typeof(htz4), pg_typeof(iv2) FROM dtt_typmod;
-- negative precision errors
SELECT '2021-01-01'::timestamp(-1);
SELECT '12:00'::time(-1);
-- casts through typmod (interval_support relabel path)
SELECT '1 day 02:03:04.567'::interval::interval minute;
SELECT '1 day 02:03:04.567'::interval(2)::interval(4);
SELECT '1 day 02:03:04.5678'::interval(4)::interval(2);
SELECT '2021-06-01 12:34:56.789999'::timestamp(6)::timestamp(1);

-- ===== EXTRACT matrix over edge dates =====
CREATE TABLE dtt_edge (d timestamptz);
INSERT INTO dtt_edge VALUES
  ('-infinity'), ('infinity'),
  ('4714-11-24 00:00:00+00 BC'), ('0001-12-31 23:59:59+00 BC'), ('0001-01-01 00:00:00+00'),
  ('1969-12-31 23:59:59.999999+00'), ('1970-01-01 00:00:00+00'),
  ('1999-12-31 23:59:59+00'), ('2000-02-29 12:00:00+00'), ('2004-02-29 23:59:59.5+00'),
  ('2038-01-19 03:14:07+00'), ('294276-12-31 23:59:59+00');
SELECT d,
  extract(epoch FROM d) AS epoch, extract(century FROM d) AS century,
  extract(decade FROM d) AS decade, extract(isoyear FROM d) AS isoyear,
  extract(year FROM d) AS yr, extract(quarter FROM d) AS q,
  extract(month FROM d) AS mon, extract(week FROM d) AS wk
FROM dtt_edge ORDER BY d;
SELECT d,
  extract(day FROM d) AS day, extract(doy FROM d) AS doy,
  extract(dow FROM d) AS dow, extract(isodow FROM d) AS isodow,
  extract(hour FROM d) AS hr, extract(minute FROM d) AS mi,
  extract(second FROM d) AS sec, extract(milliseconds FROM d) AS ms,
  extract(microseconds FROM d) AS us, extract(julian FROM d) AS julian
FROM dtt_edge ORDER BY d;
SELECT d, extract(timezone FROM d) AS tz, extract(timezone_hour FROM d) AS tzh,
  extract(timezone_minute FROM d) AS tzm
FROM dtt_edge ORDER BY d;
-- same matrix through the local-timestamp lens in a DST-observing zone
SET TIME ZONE 'America/New_York';
SELECT d, d::timestamp AS local, extract(timezone FROM d) AS tzoff FROM dtt_edge ORDER BY d;
SET TIME ZONE 'UTC';
-- extract from date/time/timetz/interval
SELECT extract(epoch FROM date '2000-01-01'), extract(julian FROM date '0001-01-01 BC');
SELECT extract(hour FROM time '23:59:59.999999'), extract(microseconds FROM time '00:00:01.5');
SELECT extract(timezone_hour FROM timetz '12:00:00+05:45'), extract(timezone_minute FROM timetz '12:00:00+05:45');
SELECT extract(epoch FROM interval '100 years'), extract(month FROM interval '2 years 14 months');
SELECT extract(epoch FROM interval 'infinity'), extract(year FROM interval '-infinity');
SELECT extract(day FROM interval 'infinity');

-- ===== DST boundaries: spring-forward / fall-back arithmetic =====
SET TIME ZONE 'America/New_York';
SELECT '2021-03-14 06:59:59+00'::timestamptz, '2021-03-14 07:00:00+00'::timestamptz;
SELECT '2021-03-14 01:59:59'::timestamptz + interval '1 minute';
SELECT '2021-03-13 12:00:00'::timestamptz + interval '1 day' AS wall_plus_day,
       '2021-03-13 12:00:00'::timestamptz + interval '24 hours' AS abs_plus_24h;
SELECT '2021-11-06 12:00:00'::timestamptz + interval '1 day' AS wall_plus_day,
       '2021-11-06 12:00:00'::timestamptz + interval '24 hours' AS abs_plus_24h;
SELECT timestamptz '2021-11-07 01:30:00' AS ambiguous_fallback;
SELECT timestamptz '2021-03-14 02:30:00' AS gap_time;
SET TIME ZONE 'UTC';
SELECT date_add('2021-10-30 00:00:00+02'::timestamptz, '1 day'::interval, 'Europe/Warsaw');
SELECT date_add('2021-10-30 00:00:00+02'::timestamptz, '1 day'::interval);
SELECT date_subtract('2021-11-01 00:00:00+01'::timestamptz, '1 day'::interval, 'Europe/Warsaw');
SELECT date_subtract('2021-11-01 00:00:00+01'::timestamptz, '1 day'::interval);
SELECT '2021-03-13 12:00:00-05'::timestamptz AT TIME ZONE 'America/New_York',
       '2021-03-15 12:00:00-04'::timestamptz AT TIME ZONE 'America/New_York';
SELECT timezone('America/New_York', '2021-03-14 02:30:00'::timestamp);

-- ===== infinity arithmetic =====
SELECT timestamp 'infinity' + interval '1 day', timestamp '-infinity' - interval '1 day';
SELECT timestamp '2000-01-01' + interval 'infinity', timestamp '2000-01-01' - interval 'infinity';
SELECT timestamp 'infinity' + interval '-infinity';
SELECT timestamp '-infinity' - interval '-infinity';
SELECT interval 'infinity' + interval '-infinity';
SELECT interval 'infinity' - interval 'infinity';
SELECT -interval 'infinity', -interval '-infinity';
SELECT interval 'infinity' * 2, interval 'infinity' * -1, interval 'infinity' * 0;
SELECT interval 'infinity' / 4, interval '-infinity' / -2;
SELECT age(timestamp 'infinity'), age(timestamp '-infinity', timestamp 'infinity');
SELECT justify_hours(interval 'infinity'), justify_days(interval '-infinity');
SELECT date_bin('1 hour', timestamp 'infinity', timestamp '2000-01-01');
SELECT isfinite(timestamp 'infinity'), isfinite(date '-infinity'), isfinite(interval 'infinity');

-- ===== interval aggregates incl. infinities =====
CREATE TABLE dtt_iv (iv interval);
INSERT INTO dtt_iv VALUES ('1 year'), ('2 years'), ('6 years 3 days'), (NULL);
SELECT avg(iv), sum(iv) FROM dtt_iv;
INSERT INTO dtt_iv VALUES ('infinity');
SELECT avg(iv), sum(iv) FROM dtt_iv;
INSERT INTO dtt_iv VALUES ('-infinity');
SELECT avg(iv) FROM dtt_iv;
SELECT sum(iv) FROM dtt_iv;
SELECT avg(iv) FROM dtt_iv WHERE iv IS NULL;

-- ===== make_interval: named args, defaults, overflow surfaces =====
SELECT make_interval(years := 2);
SELECT make_interval(years := 1, months := 6);
SELECT make_interval(months := 2, years := 1);
SELECT make_interval(years := 1, months := -1, weeks := 5, days := -7, hours := 25, mins := -180);
SELECT make_interval() = make_interval(years := 0, months := 0, weeks := 0, days := 0, mins := 0, secs := 0.0);
SELECT make_interval(hours := -2, mins := -10, secs := -25.3);
SELECT make_interval(secs := 7e12);
SELECT make_interval(years := 178956971);
SELECT make_interval(years := 1, months := 2147483647);
SELECT make_interval(weeks := 1, days := 2147483647);
SELECT make_interval(secs := 1e308);
SELECT make_interval(secs := 1e18);
SELECT make_interval(secs := 'inf');
SELECT make_interval(2, 3);

-- ===== to_char matrix over datestyles =====
CREATE TABLE dtt_fmt (d timestamptz);
INSERT INTO dtt_fmt VALUES ('0044-03-15 13:14:15+00 BC'), ('1997-06-10 17:32:01+00'), ('2021-12-31 23:59:59.987+00');
SET datestyle = 'Postgres, MDY';
SELECT d::timestamp, d FROM dtt_fmt ORDER BY d;
SET datestyle = 'SQL, DMY';
SELECT d::timestamp, d FROM dtt_fmt ORDER BY d;
SET datestyle = 'German';
SELECT d::timestamp, d FROM dtt_fmt ORDER BY d;
SET datestyle = 'ISO, YMD';
SELECT to_char(d, 'YYYY-MM-DD HH24:MI:SS.US TZ TZH:TZM OF') FROM dtt_fmt ORDER BY d;
SELECT to_char(d, 'FMDay, FMDDth FMMonth Y,YYY BC IYYY-IW-ID J Q RM') FROM dtt_fmt ORDER BY d;
SELECT to_char(d, 'AD A.D. ad a.d. CC SSSS "quoted" ""') FROM dtt_fmt ORDER BY d;
SELECT to_char(interval '3 years 2 mons 25 days 12:34:56.789', 'YYYY MM DD HH24 MI SS MS US');
SELECT to_char(interval '-13 months', 'MONTH Month month RM rm');
SELECT to_char(interval '100000 hours 59 min', 'HH24:MI:SS');
SELECT to_char('2021-06-01 12:00 +05:30'::timestamptz, 'HH12 AM hh12 am TZ tz');

-- ===== to_timestamp / to_date incl. TZ abbrev consumption =====
SELECT to_timestamp('2011-12-18 11:38 PST', 'YYYY-MM-DD HH12:MI TZ');
SELECT to_timestamp('2011-12-18 11:38 EDT', 'YYYY-MM-DD HH12:MI TZ');
SELECT to_timestamp('2011-12-18 11:38 MSK', 'YYYY-MM-DD HH12:MI TZ');
SELECT to_timestamp('2011-12-18 11:38 +05:30', 'YYYY-MM-DD HH12:MI TZ');
SELECT to_timestamp('2011-12-18 11:38 -08', 'YYYY-MM-DD HH12:MI TZ');
SELECT to_timestamp('2011-12-18 11:38 XYZ', 'YYYY-MM-DD HH12:MI TZ');
SELECT to_timestamp('2011-12-18 23:38:15', 'YYYY-MM-DD HH24:MI:SS');
SELECT to_timestamp('97/Feb/16', 'YY/Mon/DD');
SELECT to_timestamp('19971116', 'YYYYMMDD');
SELECT to_timestamp('1,582,000', 'J');
SELECT to_timestamp('0097 BC', 'YYYY BC');
SELECT to_date('2011 12  18', 'YYYY MM DD');
SELECT to_date('2011 x12 x18', 'YYYY xMM xDD');
SELECT to_timestamp('2011-12-18 24:38', 'YYYY-MM-DD HH24:MI');

-- ===== >4-digit years, trailing abbrevs =====
CREATE TABLE dtt_bigyear (a int, b timestamptz);
INSERT INTO dtt_bigyear VALUES (1, 'Sat Mar 12 23:58:48 1000 IST');
INSERT INTO dtt_bigyear VALUES (2, 'Sat Mar 12 23:58:48 10000 IST');
INSERT INTO dtt_bigyear VALUES (3, 'Sat Mar 12 23:58:48 100000 IST');
INSERT INTO dtt_bigyear VALUES (4, '10000 Mar 12 23:58:48 IST');
INSERT INTO dtt_bigyear VALUES (5, '100000312 23:58:48 IST');
INSERT INTO dtt_bigyear VALUES (6, '1000000312 23:58:48 IST');
SELECT * FROM dtt_bigyear ORDER BY a;

DROP TABLE dtt_typmod, dtt_edge, dtt_iv, dtt_fmt, dtt_bigyear;
