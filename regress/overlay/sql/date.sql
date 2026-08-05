--
-- DATE
--

CREATE TABLE DATE_TBL (f1 date);

INSERT INTO DATE_TBL VALUES ('1957-04-09');
INSERT INTO DATE_TBL VALUES ('1957-06-13');
INSERT INTO DATE_TBL VALUES ('1996-02-28');
INSERT INTO DATE_TBL VALUES ('1996-02-29');
INSERT INTO DATE_TBL VALUES ('1996-03-01');
INSERT INTO DATE_TBL VALUES ('1996-03-02');
INSERT INTO DATE_TBL VALUES ('1997-02-28');
INSERT INTO DATE_TBL VALUES ('1997-02-29');
INSERT INTO DATE_TBL VALUES ('1997-03-01');
INSERT INTO DATE_TBL VALUES ('1997-03-02');
INSERT INTO DATE_TBL VALUES ('2000-04-01');
INSERT INTO DATE_TBL VALUES ('2000-04-02');
INSERT INTO DATE_TBL VALUES ('2000-04-03');
INSERT INTO DATE_TBL VALUES ('2038-04-08');
INSERT INTO DATE_TBL VALUES ('2039-04-09');
INSERT INTO DATE_TBL VALUES ('2040-04-10');
INSERT INTO DATE_TBL VALUES ('2040-04-10 BC');

-- pgrust:rowsort
SELECT f1 FROM DATE_TBL;

-- pgrust:rowsort
SELECT f1 FROM DATE_TBL WHERE f1 < '2000-01-01';

-- pgrust:rowsort
SELECT f1 FROM DATE_TBL
  WHERE f1 BETWEEN '2000-01-01' AND '2001-01-01';

--
-- Check all the documented input formats
--
SET datestyle TO iso;  -- display results in ISO

SET datestyle TO ymd;

-- pgrust:rowsort
SELECT date 'January 8, 1999';
-- pgrust:rowsort
SELECT date '1999-01-08';
-- pgrust:rowsort
SELECT date '1999-01-18';
SELECT date '1/8/1999';
SELECT date '1/18/1999';
SELECT date '18/1/1999';
-- pgrust:rowsort
SELECT date '01/02/03';
-- pgrust:rowsort
SELECT date '19990108';
-- pgrust:rowsort
SELECT date '990108';
-- pgrust:rowsort
SELECT date '1999.008';
-- pgrust:rowsort
SELECT date 'J2451187';
SELECT date 'January 8, 99 BC';

-- pgrust:rowsort
SELECT date '99-Jan-08';
-- pgrust:rowsort
SELECT date '1999-Jan-08';
SELECT date '08-Jan-99';
-- pgrust:rowsort
SELECT date '08-Jan-1999';
SELECT date 'Jan-08-99';
-- pgrust:rowsort
SELECT date 'Jan-08-1999';
SELECT date '99-08-Jan';
SELECT date '1999-08-Jan';

-- pgrust:rowsort
SELECT date '99 Jan 08';
-- pgrust:rowsort
SELECT date '1999 Jan 08';
SELECT date '08 Jan 99';
-- pgrust:rowsort
SELECT date '08 Jan 1999';
SELECT date 'Jan 08 99';
-- pgrust:rowsort
SELECT date 'Jan 08 1999';
-- pgrust:rowsort
SELECT date '99 08 Jan';
-- pgrust:rowsort
SELECT date '1999 08 Jan';

-- pgrust:rowsort
SELECT date '99-01-08';
-- pgrust:rowsort
SELECT date '1999-01-08';
SELECT date '08-01-99';
SELECT date '08-01-1999';
SELECT date '01-08-99';
SELECT date '01-08-1999';
-- pgrust:rowsort
SELECT date '99-08-01';
-- pgrust:rowsort
SELECT date '1999-08-01';

-- pgrust:rowsort
SELECT date '99 01 08';
-- pgrust:rowsort
SELECT date '1999 01 08';
SELECT date '08 01 99';
SELECT date '08 01 1999';
SELECT date '01 08 99';
SELECT date '01 08 1999';
-- pgrust:rowsort
SELECT date '99 08 01';
-- pgrust:rowsort
SELECT date '1999 08 01';

SET datestyle TO dmy;

-- pgrust:rowsort
SELECT date 'January 8, 1999';
-- pgrust:rowsort
SELECT date '1999-01-08';
-- pgrust:rowsort
SELECT date '1999-01-18';
-- pgrust:rowsort
SELECT date '1/8/1999';
SELECT date '1/18/1999';
-- pgrust:rowsort
SELECT date '18/1/1999';
-- pgrust:rowsort
SELECT date '01/02/03';
-- pgrust:rowsort
SELECT date '19990108';
-- pgrust:rowsort
SELECT date '990108';
-- pgrust:rowsort
SELECT date '1999.008';
-- pgrust:rowsort
SELECT date 'J2451187';
-- pgrust:rowsort
SELECT date 'January 8, 99 BC';

SELECT date '99-Jan-08';
-- pgrust:rowsort
SELECT date '1999-Jan-08';
-- pgrust:rowsort
SELECT date '08-Jan-99';
-- pgrust:rowsort
SELECT date '08-Jan-1999';
-- pgrust:rowsort
SELECT date 'Jan-08-99';
-- pgrust:rowsort
SELECT date 'Jan-08-1999';
SELECT date '99-08-Jan';
SELECT date '1999-08-Jan';

SELECT date '99 Jan 08';
-- pgrust:rowsort
SELECT date '1999 Jan 08';
-- pgrust:rowsort
SELECT date '08 Jan 99';
-- pgrust:rowsort
SELECT date '08 Jan 1999';
-- pgrust:rowsort
SELECT date 'Jan 08 99';
-- pgrust:rowsort
SELECT date 'Jan 08 1999';
SELECT date '99 08 Jan';
-- pgrust:rowsort
SELECT date '1999 08 Jan';

SELECT date '99-01-08';
-- pgrust:rowsort
SELECT date '1999-01-08';
-- pgrust:rowsort
SELECT date '08-01-99';
-- pgrust:rowsort
SELECT date '08-01-1999';
-- pgrust:rowsort
SELECT date '01-08-99';
-- pgrust:rowsort
SELECT date '01-08-1999';
SELECT date '99-08-01';
-- pgrust:rowsort
SELECT date '1999-08-01';

SELECT date '99 01 08';
-- pgrust:rowsort
SELECT date '1999 01 08';
-- pgrust:rowsort
SELECT date '08 01 99';
-- pgrust:rowsort
SELECT date '08 01 1999';
-- pgrust:rowsort
SELECT date '01 08 99';
-- pgrust:rowsort
SELECT date '01 08 1999';
SELECT date '99 08 01';
-- pgrust:rowsort
SELECT date '1999 08 01';

SET datestyle TO mdy;

-- pgrust:rowsort
SELECT date 'January 8, 1999';
-- pgrust:rowsort
SELECT date '1999-01-08';
-- pgrust:rowsort
SELECT date '1999-01-18';
-- pgrust:rowsort
SELECT date '1/8/1999';
-- pgrust:rowsort
SELECT date '1/18/1999';
SELECT date '18/1/1999';
-- pgrust:rowsort
SELECT date '01/02/03';
-- pgrust:rowsort
SELECT date '19990108';
-- pgrust:rowsort
SELECT date '990108';
-- pgrust:rowsort
SELECT date '1999.008';
-- pgrust:rowsort
SELECT date 'J2451187';
-- pgrust:rowsort
SELECT date 'January 8, 99 BC';

SELECT date '99-Jan-08';
-- pgrust:rowsort
SELECT date '1999-Jan-08';
-- pgrust:rowsort
SELECT date '08-Jan-99';
-- pgrust:rowsort
SELECT date '08-Jan-1999';
-- pgrust:rowsort
SELECT date 'Jan-08-99';
-- pgrust:rowsort
SELECT date 'Jan-08-1999';
SELECT date '99-08-Jan';
SELECT date '1999-08-Jan';

SELECT date '99 Jan 08';
-- pgrust:rowsort
SELECT date '1999 Jan 08';
-- pgrust:rowsort
SELECT date '08 Jan 99';
-- pgrust:rowsort
SELECT date '08 Jan 1999';
-- pgrust:rowsort
SELECT date 'Jan 08 99';
-- pgrust:rowsort
SELECT date 'Jan 08 1999';
SELECT date '99 08 Jan';
-- pgrust:rowsort
SELECT date '1999 08 Jan';

SELECT date '99-01-08';
-- pgrust:rowsort
SELECT date '1999-01-08';
-- pgrust:rowsort
SELECT date '08-01-99';
-- pgrust:rowsort
SELECT date '08-01-1999';
-- pgrust:rowsort
SELECT date '01-08-99';
-- pgrust:rowsort
SELECT date '01-08-1999';
SELECT date '99-08-01';
-- pgrust:rowsort
SELECT date '1999-08-01';

SELECT date '99 01 08';
-- pgrust:rowsort
SELECT date '1999 01 08';
-- pgrust:rowsort
SELECT date '08 01 99';
-- pgrust:rowsort
SELECT date '08 01 1999';
-- pgrust:rowsort
SELECT date '01 08 99';
-- pgrust:rowsort
SELECT date '01 08 1999';
SELECT date '99 08 01';
-- pgrust:rowsort
SELECT date '1999 08 01';

-- Check upper and lower limits of date range
-- pgrust:rowsort
SELECT date '4714-11-24 BC';
SELECT date '4714-11-23 BC';  -- out of range
-- pgrust:rowsort
SELECT date '5874897-12-31';
SELECT date '5874898-01-01';  -- out of range

-- Test non-error-throwing API
SELECT pg_input_is_valid('now', 'date');
SELECT pg_input_is_valid('garbage', 'date');
SELECT pg_input_is_valid('6874898-01-01', 'date');
SELECT * FROM pg_input_error_info('garbage', 'date');
SELECT * FROM pg_input_error_info('6874898-01-01', 'date');

RESET datestyle;

--
-- Simple math
-- Leave most of it for the horology tests
--

-- pgrust:rowsort
SELECT f1 - date '2000-01-01' AS "Days From 2K" FROM DATE_TBL;

-- pgrust:rowsort
SELECT f1 - date 'epoch' AS "Days From Epoch" FROM DATE_TBL;

-- pgrust:rowsort
SELECT date 'yesterday' - date 'today' AS "One day";

-- pgrust:rowsort
SELECT date 'today' - date 'tomorrow' AS "One day";

-- pgrust:rowsort
SELECT date 'yesterday' - date 'tomorrow' AS "Two days";

-- pgrust:rowsort
SELECT date 'tomorrow' - date 'today' AS "One day";

-- pgrust:rowsort
SELECT date 'today' - date 'yesterday' AS "One day";

-- pgrust:rowsort
SELECT date 'tomorrow' - date 'yesterday' AS "Two days";

--
-- test extract!
--
-- pgrust:rowsort
SELECT f1 as "date",
    date_part('year', f1) AS year,
    date_part('month', f1) AS month,
    date_part('day', f1) AS day,
    date_part('quarter', f1) AS quarter,
    date_part('decade', f1) AS decade,
    date_part('century', f1) AS century,
    date_part('millennium', f1) AS millennium,
    date_part('isoyear', f1) AS isoyear,
    date_part('week', f1) AS week,
    date_part('dow', f1) AS dow,
    date_part('isodow', f1) AS isodow,
    date_part('doy', f1) AS doy,
    date_part('julian', f1) AS julian,
    date_part('epoch', f1) AS epoch
    FROM date_tbl;
--
-- epoch
--
-- pgrust:rowsort
SELECT EXTRACT(EPOCH FROM DATE        '1970-01-01');     --  0
--
-- century
--
-- pgrust:rowsort
SELECT EXTRACT(CENTURY FROM DATE '0101-12-31 BC'); -- -2
-- pgrust:rowsort
SELECT EXTRACT(CENTURY FROM DATE '0100-12-31 BC'); -- -1
-- pgrust:rowsort
SELECT EXTRACT(CENTURY FROM DATE '0001-12-31 BC'); -- -1
-- pgrust:rowsort
SELECT EXTRACT(CENTURY FROM DATE '0001-01-01');    --  1
-- pgrust:rowsort
SELECT EXTRACT(CENTURY FROM DATE '0001-01-01 AD'); --  1
-- pgrust:rowsort
SELECT EXTRACT(CENTURY FROM DATE '1900-12-31');    -- 19
-- pgrust:rowsort
SELECT EXTRACT(CENTURY FROM DATE '1901-01-01');    -- 20
-- pgrust:rowsort
SELECT EXTRACT(CENTURY FROM DATE '2000-12-31');    -- 20
-- pgrust:rowsort
SELECT EXTRACT(CENTURY FROM DATE '2001-01-01');    -- 21
-- pgrust:rowsort
SELECT EXTRACT(CENTURY FROM CURRENT_DATE)>=21 AS True;     -- true
--
-- millennium
--
-- pgrust:rowsort
SELECT EXTRACT(MILLENNIUM FROM DATE '0001-12-31 BC'); -- -1
-- pgrust:rowsort
SELECT EXTRACT(MILLENNIUM FROM DATE '0001-01-01 AD'); --  1
-- pgrust:rowsort
SELECT EXTRACT(MILLENNIUM FROM DATE '1000-12-31');    --  1
-- pgrust:rowsort
SELECT EXTRACT(MILLENNIUM FROM DATE '1001-01-01');    --  2
-- pgrust:rowsort
SELECT EXTRACT(MILLENNIUM FROM DATE '2000-12-31');    --  2
-- pgrust:rowsort
SELECT EXTRACT(MILLENNIUM FROM DATE '2001-01-01');    --  3
-- next test to be fixed on the turn of the next millennium;-)
-- pgrust:rowsort
SELECT EXTRACT(MILLENNIUM FROM CURRENT_DATE);         --  3
--
-- decade
--
-- pgrust:rowsort
SELECT EXTRACT(DECADE FROM DATE '1994-12-25');    -- 199
-- pgrust:rowsort
SELECT EXTRACT(DECADE FROM DATE '0010-01-01');    --   1
-- pgrust:rowsort
SELECT EXTRACT(DECADE FROM DATE '0009-12-31');    --   0
-- pgrust:rowsort
SELECT EXTRACT(DECADE FROM DATE '0001-01-01 BC'); --   0
-- pgrust:rowsort
SELECT EXTRACT(DECADE FROM DATE '0002-12-31 BC'); --  -1
-- pgrust:rowsort
SELECT EXTRACT(DECADE FROM DATE '0011-01-01 BC'); --  -1
-- pgrust:rowsort
SELECT EXTRACT(DECADE FROM DATE '0012-12-31 BC'); --  -2
--
-- all possible fields
--
SELECT EXTRACT(MICROSECONDS  FROM DATE '2020-08-11');
SELECT EXTRACT(MILLISECONDS  FROM DATE '2020-08-11');
SELECT EXTRACT(SECOND        FROM DATE '2020-08-11');
SELECT EXTRACT(MINUTE        FROM DATE '2020-08-11');
SELECT EXTRACT(HOUR          FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(DAY           FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(MONTH         FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(YEAR          FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(YEAR          FROM DATE '2020-08-11 BC');
-- pgrust:rowsort
SELECT EXTRACT(DECADE        FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(CENTURY       FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(MILLENNIUM    FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(ISOYEAR       FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(ISOYEAR       FROM DATE '2020-08-11 BC');
-- pgrust:rowsort
SELECT EXTRACT(QUARTER       FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(WEEK          FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(DOW           FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(DOW           FROM DATE '2020-08-16');
-- pgrust:rowsort
SELECT EXTRACT(ISODOW        FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(ISODOW        FROM DATE '2020-08-16');
-- pgrust:rowsort
SELECT EXTRACT(DOY           FROM DATE '2020-08-11');
SELECT EXTRACT(TIMEZONE      FROM DATE '2020-08-11');
SELECT EXTRACT(TIMEZONE_M    FROM DATE '2020-08-11');
SELECT EXTRACT(TIMEZONE_H    FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(EPOCH         FROM DATE '2020-08-11');
-- pgrust:rowsort
SELECT EXTRACT(JULIAN        FROM DATE '2020-08-11');
--
-- test trunc function!
--
-- pgrust:rowsort
SELECT DATE_TRUNC('MILLENNIUM', TIMESTAMP '1970-03-20 04:30:00.00000'); -- 1001
-- pgrust:rowsort
SELECT DATE_TRUNC('MILLENNIUM', DATE '1970-03-20'); -- 1001-01-01
-- pgrust:rowsort
SELECT DATE_TRUNC('CENTURY', TIMESTAMP '1970-03-20 04:30:00.00000'); -- 1901
-- pgrust:rowsort
SELECT DATE_TRUNC('CENTURY', DATE '1970-03-20'); -- 1901
-- pgrust:rowsort
SELECT DATE_TRUNC('CENTURY', DATE '2004-08-10'); -- 2001-01-01
-- pgrust:rowsort
SELECT DATE_TRUNC('CENTURY', DATE '0002-02-04'); -- 0001-01-01
-- pgrust:rowsort
SELECT DATE_TRUNC('CENTURY', DATE '0055-08-10 BC'); -- 0100-01-01 BC
-- pgrust:rowsort
SELECT DATE_TRUNC('DECADE', DATE '1993-12-25'); -- 1990-01-01
-- pgrust:rowsort
SELECT DATE_TRUNC('DECADE', DATE '0004-12-25'); -- 0001-01-01 BC
-- pgrust:rowsort
SELECT DATE_TRUNC('DECADE', DATE '0002-12-31 BC'); -- 0011-01-01 BC
--
-- test infinity
--
-- pgrust:rowsort
select 'infinity'::date, '-infinity'::date;
-- pgrust:rowsort
select 'infinity'::date > 'today'::date as t;
-- pgrust:rowsort
select '-infinity'::date < 'today'::date as t;
-- pgrust:rowsort
select isfinite('infinity'::date), isfinite('-infinity'::date), isfinite('today'::date);
-- pgrust:rowsort
select 'infinity'::date = '+infinity'::date as t;

--
-- oscillating fields from non-finite date:
--
-- pgrust:rowsort
SELECT EXTRACT(DAY FROM DATE 'infinity');      -- NULL
-- pgrust:rowsort
SELECT EXTRACT(DAY FROM DATE '-infinity');     -- NULL
-- all supported fields
-- pgrust:rowsort
SELECT EXTRACT(DAY           FROM DATE 'infinity');    -- NULL
-- pgrust:rowsort
SELECT EXTRACT(MONTH         FROM DATE 'infinity');    -- NULL
-- pgrust:rowsort
SELECT EXTRACT(QUARTER       FROM DATE 'infinity');    -- NULL
-- pgrust:rowsort
SELECT EXTRACT(WEEK          FROM DATE 'infinity');    -- NULL
-- pgrust:rowsort
SELECT EXTRACT(DOW           FROM DATE 'infinity');    -- NULL
-- pgrust:rowsort
SELECT EXTRACT(ISODOW        FROM DATE 'infinity');    -- NULL
-- pgrust:rowsort
SELECT EXTRACT(DOY           FROM DATE 'infinity');    -- NULL
--
-- monotonic fields from non-finite date:
--
-- pgrust:rowsort
SELECT EXTRACT(EPOCH FROM DATE 'infinity');         --  Infinity
-- pgrust:rowsort
SELECT EXTRACT(EPOCH FROM DATE '-infinity');        -- -Infinity
-- all supported fields
-- pgrust:rowsort
SELECT EXTRACT(YEAR       FROM DATE 'infinity');    --  Infinity
-- pgrust:rowsort
SELECT EXTRACT(DECADE     FROM DATE 'infinity');    --  Infinity
-- pgrust:rowsort
SELECT EXTRACT(CENTURY    FROM DATE 'infinity');    --  Infinity
-- pgrust:rowsort
SELECT EXTRACT(MILLENNIUM FROM DATE 'infinity');    --  Infinity
-- pgrust:rowsort
SELECT EXTRACT(JULIAN     FROM DATE 'infinity');    --  Infinity
-- pgrust:rowsort
SELECT EXTRACT(ISOYEAR    FROM DATE 'infinity');    --  Infinity
-- pgrust:rowsort
SELECT EXTRACT(EPOCH      FROM DATE 'infinity');    --  Infinity
--
-- wrong fields from non-finite date:
--
SELECT EXTRACT(MICROSEC  FROM DATE 'infinity');     -- error

-- test constructors
-- pgrust:rowsort
select make_date(2013, 7, 15);
-- pgrust:rowsort
select make_date(-44, 3, 15);
-- pgrust:rowsort
select make_time(8, 20, 0.0);
-- should fail
select make_date(0, 7, 15);
select make_date(2013, 2, 30);
select make_date(2013, 13, 1);
select make_date(2013, 11, -1);
SELECT make_date(-2147483648, 1, 1);
select make_time(10, 55, 100.1);
select make_time(24, 0, 2.1);
