-- tz-surface differential corpus: TimeZone GUC, AT TIME ZONE, EXTRACT,
-- date_trunc(+zone), now-family, make_timestamptz. Zones pinned to UTC,
-- America/New_York (both 2025 DST edges), Asia/Kolkata (+05:30).
-- Run via scripts/regress-diff.sh --sql fixtures/tz-e2e.sql.
\set VERBOSITY verbose
\pset pager off

-- ===== TimeZone GUC forms =====
SET TIME ZONE 'UTC';
SHOW timezone;
SHOW TIME ZONE;
SELECT '2025-03-09 06:30:00+00'::timestamptz;
SET TIME ZONE 'America/New_York';
SHOW timezone;
SELECT '2025-03-09 06:30:00+00'::timestamptz;
SELECT '2025-03-09 02:30:00'::timestamptz;
SELECT '2025-11-02 01:30:00'::timestamptz;
SELECT '2025-07-04 12:00:00'::timestamptz;
SELECT '2025-01-04 12:00:00'::timestamptz;
SET timezone = 'Asia/Kolkata';
SHOW timezone;
SELECT '2025-01-01 00:00:00+00'::timestamptz;
SELECT '2025-01-01 00:00:00'::timestamptz;
SET timezone TO 'America/New_York';
SHOW timezone;
SET TIME ZONE -7;
SHOW timezone;
SELECT '2025-01-01 00:00:00+00'::timestamptz;
SET TIME ZONE 7.5;
SHOW timezone;
SELECT '2025-01-01 00:00:00+00'::timestamptz;
SET TIME ZONE DEFAULT;
RESET timezone;
RESET TIME ZONE;
SET TIME ZONE 'invalid/zone';
SET timezone = 'Not/AZone';
SET TIME ZONE 'UTC';

-- ===== AT TIME ZONE both directions across DST edges =====
SELECT TIMESTAMP '2025-03-09 02:30:00' AT TIME ZONE 'America/New_York';
SELECT TIMESTAMP '2025-03-09 01:30:00' AT TIME ZONE 'America/New_York';
SELECT TIMESTAMP '2025-03-09 03:30:00' AT TIME ZONE 'America/New_York';
SELECT TIMESTAMP '2025-11-02 01:30:00' AT TIME ZONE 'America/New_York';
SELECT TIMESTAMPTZ '2025-03-09 06:30:00+00' AT TIME ZONE 'America/New_York';
SELECT TIMESTAMPTZ '2025-03-09 07:30:00+00' AT TIME ZONE 'America/New_York';
SELECT TIMESTAMPTZ '2025-11-02 05:30:00+00' AT TIME ZONE 'America/New_York';
SELECT TIMESTAMPTZ '2025-11-02 06:30:00+00' AT TIME ZONE 'America/New_York';
SELECT TIMESTAMP '2025-06-01 12:00:00' AT TIME ZONE 'Asia/Kolkata';
SELECT TIMESTAMPTZ '2025-06-01 12:00:00+00' AT TIME ZONE 'Asia/Kolkata';
SELECT TIMESTAMP '2025-06-01 12:00:00' AT TIME ZONE 'UTC';
SELECT timezone('UTC', TIMESTAMPTZ '2025-06-01 00:00:00+00');
SELECT timezone('America/New_York', TIMESTAMP '2025-11-02 01:30:00');
SELECT TIMESTAMP '2025-06-01 12:00:00' AT TIME ZONE 'EST';
SELECT TIMESTAMPTZ '2025-06-01 12:00:00+00' AT TIME ZONE 'EST';
SET TIME ZONE 'America/New_York';
SELECT TIMESTAMPTZ '2025-06-01 12:00:00+00' AT LOCAL;
SELECT TIMESTAMP '2025-06-01 12:00:00' AT LOCAL;
SET TIME ZONE 'UTC';
SELECT TIMESTAMP '2025-06-01 12:00:00' AT TIME ZONE 'Nowhere/Zone';
SELECT TIMESTAMPTZ 'infinity' AT TIME ZONE 'America/New_York';
SELECT TIMESTAMP '-infinity' AT TIME ZONE 'America/New_York';

-- ===== EXTRACT / date_part =====
SELECT EXTRACT(YEAR FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(MONTH FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(DAY FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(HOUR FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(MINUTE FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(SECOND FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(microseconds FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(milliseconds FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(epoch FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(epoch FROM TIMESTAMPTZ '2025-01-01 00:00:00+00');
SELECT EXTRACT(week FROM TIMESTAMP '2004-01-01 00:00:00');
SELECT EXTRACT(quarter FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(dow FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(isodow FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(doy FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(isoyear FROM TIMESTAMP '2004-01-01 00:00:00');
SELECT EXTRACT(decade FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(century FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(millennium FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(julian FROM TIMESTAMP '2004-02-29 13:24:56.789');
SELECT EXTRACT(year FROM TIMESTAMP '0044-03-15 00:00:00 BC');
SELECT EXTRACT(century FROM TIMESTAMP '0001-01-01 00:00:00 BC');
SET TIME ZONE 'America/New_York';
SELECT EXTRACT(timezone FROM TIMESTAMPTZ '2025-07-04 12:00:00+00');
SELECT EXTRACT(timezone_hour FROM TIMESTAMPTZ '2025-07-04 12:00:00+00');
SELECT EXTRACT(timezone_minute FROM TIMESTAMPTZ '2025-07-04 12:00:00+00');
SET TIME ZONE 'Asia/Kolkata';
SELECT EXTRACT(timezone FROM TIMESTAMPTZ '2025-07-04 12:00:00+00');
SELECT EXTRACT(timezone_hour FROM TIMESTAMPTZ '2025-07-04 12:00:00+00');
SELECT EXTRACT(timezone_minute FROM TIMESTAMPTZ '2025-07-04 12:00:00+00');
SET TIME ZONE 'UTC';
SELECT EXTRACT(timezone FROM TIMESTAMP '2025-01-01 00:00:00');
SELECT EXTRACT(bogus FROM TIMESTAMP '2025-01-01 00:00:00');
SELECT EXTRACT('epoch' FROM TIMESTAMP 'infinity');
SELECT EXTRACT(day FROM TIMESTAMP 'infinity');
SELECT EXTRACT(epoch FROM TIMESTAMP '-infinity');
SELECT date_part('epoch', TIMESTAMP '2004-02-29 13:24:56.789');
SELECT date_part('second', TIMESTAMP '2004-02-29 13:24:56.789');
SELECT date_part('julian', TIMESTAMP '2004-02-29 13:24:56.789');
SELECT date_part('timezone', TIMESTAMPTZ '2025-07-04 12:00:00+00');
SELECT date_part('epoch', TIMESTAMP 'infinity');

-- ===== date_trunc =====
SELECT date_trunc('week', TIMESTAMP '2004-02-29 13:24:56.789');
SELECT date_trunc('quarter', TIMESTAMP '2004-02-29 13:24:56.789');
SELECT date_trunc('decade', TIMESTAMP '2004-02-29 13:24:56.789');
SELECT date_trunc('century', TIMESTAMP '2004-02-29 13:24:56.789');
SELECT date_trunc('millennium', TIMESTAMP '2004-02-29 13:24:56.789');
SELECT date_trunc('hour', TIMESTAMP '2004-02-29 13:24:56.789');
SELECT date_trunc('milliseconds', TIMESTAMP '2004-02-29 13:24:56.789');
SET TIME ZONE 'America/New_York';
SELECT date_trunc('day', TIMESTAMPTZ '2025-03-09 06:30:00+00');
SELECT date_trunc('day', TIMESTAMPTZ '2025-11-02 05:30:00+00');
SELECT date_trunc('week', TIMESTAMPTZ '2025-03-12 00:00:00+00');
SELECT date_trunc('day', TIMESTAMPTZ '2025-03-09 06:30:00+00', 'Asia/Kolkata');
SELECT date_trunc('day', TIMESTAMPTZ '2025-03-09 06:30:00+00', 'UTC');
SELECT date_trunc('bogus', TIMESTAMP '2025-01-01 00:00:00');
SELECT date_trunc('week', TIMESTAMP 'infinity');
SELECT date_trunc('timezone', TIMESTAMP '2025-01-01 00:00:00');
SET TIME ZONE 'UTC';

-- ===== make_timestamp / make_timestamptz =====
SELECT make_timestamp(2025, 7, 4, 12, 30, 15.5);
SELECT make_timestamp(-44, 3, 15, 0, 0, 0);
SET TIME ZONE 'America/New_York';
SELECT make_timestamptz(2025, 7, 4, 12, 30, 15.5);
SELECT make_timestamptz(2025, 3, 9, 2, 30, 0);
SELECT make_timestamptz(2025, 7, 4, 12, 30, 15.5, 'Asia/Kolkata');
SELECT make_timestamptz(2025, 7, 4, 12, 30, 15.5, '+05:30');
SELECT make_timestamptz(2025, 11, 2, 1, 30, 0, 'America/New_York');
SELECT make_timestamptz(2025, 7, 4, 12, 30, 15.5, 'Nowhere/Zone');
SELECT make_timestamptz(2025, 13, 1, 0, 0, 0);
SET TIME ZONE 'UTC';

-- ===== now-family (value-free predicates; wall values are nondeterministic) =====
SELECT now() = transaction_timestamp();
BEGIN;
SELECT now() = transaction_timestamp();
SELECT statement_timestamp() >= now();
SELECT clock_timestamp() >= statement_timestamp();
COMMIT;
SELECT timeofday() <> '';
SELECT CURRENT_TIMESTAMP = now();
SELECT EXTRACT(microseconds FROM CURRENT_TIMESTAMP(2))::bigint % 10000 = 0;
SELECT LOCALTIMESTAMP = timezone('UTC', now());
SELECT EXTRACT(microseconds FROM LOCALTIMESTAMP(0))::bigint % 1000000 = 0;
SET TIME ZONE 'America/New_York';
SELECT LOCALTIMESTAMP = now() AT TIME ZONE 'America/New_York';
SET TIME ZONE 'UTC';

-- ===== timestamptz round-trips under different session zones =====
CREATE TABLE tz_e2e(id int, t timestamptz, ts timestamp);
SET TIME ZONE 'America/New_York';
INSERT INTO tz_e2e VALUES (1, '2025-07-04 12:00:00', '2025-07-04 12:00:00');
INSERT INTO tz_e2e VALUES (2, '2025-03-09 02:30:00', '2025-03-09 02:30:00');
INSERT INTO tz_e2e VALUES (3, '2025-11-02 01:30:00-05', '2025-11-02 01:30:00');
SET TIME ZONE 'Asia/Kolkata';
INSERT INTO tz_e2e VALUES (4, '2025-07-04 12:00:00', '2025-07-04 12:00:00');
SELECT id, t, ts FROM tz_e2e ORDER BY id;
SET TIME ZONE 'UTC';
SELECT id, t, ts FROM tz_e2e ORDER BY id;
SELECT id, t AT TIME ZONE 'America/New_York', ts AT TIME ZONE 'America/New_York' FROM tz_e2e ORDER BY id;
SELECT id, EXTRACT(epoch FROM t) FROM tz_e2e ORDER BY id;
DROP TABLE tz_e2e;

-- ===== interval lane: tz-relevant interval surface =====
SET TIME ZONE 'America/New_York';
SELECT timestamptz '2025-03-08 12:00:00' + interval '1 day';
SELECT timestamptz '2025-03-08 12:00:00' + interval '24 hours';
SELECT timestamptz '2025-11-01 12:00:00' + interval '1 day';
SELECT timestamptz '2025-11-01 12:00:00' + interval '24 hours';
SELECT timestamptz '2025-11-02 06:30:00+00' - timestamptz '2025-11-01 06:30:00+00';
SELECT timestamp '2025-06-01 12:00:00' AT TIME ZONE interval '-5 hours';
SET TIME ZONE INTERVAL '+05:30' HOUR TO MINUTE;
SHOW timezone;
SELECT '2025-01-01 00:00:00+00'::timestamptz;
SET TIME ZONE 'UTC';
SELECT age(timestamptz '2025-03-15 10:30:00+00', timestamptz '2024-01-20 08:00:00+00');
SELECT extract(epoch from interval '2 years 5 mons 10 days 12:30:45.678');
