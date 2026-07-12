--
-- INT8
-- Test int8 64-bit integers.
--

-- int8_tbl was already created and filled in test_setup.sql.
-- Here we just try to insert bad values.

INSERT INTO INT8_TBL(q1) VALUES ('      ');
INSERT INTO INT8_TBL(q1) VALUES ('xxx');
INSERT INTO INT8_TBL(q1) VALUES ('3908203590239580293850293850329485');
INSERT INTO INT8_TBL(q1) VALUES ('-1204982019841029840928340329840934');
INSERT INTO INT8_TBL(q1) VALUES ('- 123');
INSERT INTO INT8_TBL(q1) VALUES ('  345     5');
INSERT INTO INT8_TBL(q1) VALUES ('');

-- pgrust:rowsort
SELECT * FROM INT8_TBL;

-- Also try it with non-error-throwing API
SELECT pg_input_is_valid('34', 'int8');
SELECT pg_input_is_valid('asdf', 'int8');
SELECT pg_input_is_valid('10000000000000000000', 'int8');
SELECT * FROM pg_input_error_info('10000000000000000000', 'int8');

-- int8/int8 cmp
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 = 4567890123456789;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 <> 4567890123456789;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 < 4567890123456789;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 > 4567890123456789;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 <= 4567890123456789;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 >= 4567890123456789;

-- int8/int4 cmp
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 = 456;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 <> 456;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 < 456;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 > 456;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 <= 456;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 >= 456;

-- int4/int8 cmp
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE 123 = q1;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE 123 <> q1;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE 123 < q1;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE 123 > q1;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE 123 <= q1;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE 123 >= q1;

-- int8/int2 cmp
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 = '456'::int2;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 <> '456'::int2;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 < '456'::int2;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 > '456'::int2;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 <= '456'::int2;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE q2 >= '456'::int2;

-- int2/int8 cmp
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE '123'::int2 = q1;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE '123'::int2 <> q1;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE '123'::int2 < q1;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE '123'::int2 > q1;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE '123'::int2 <= q1;
-- pgrust:rowsort
SELECT * FROM INT8_TBL WHERE '123'::int2 >= q1;


-- pgrust:rowsort
SELECT q1 AS plus, -q1 AS minus FROM INT8_TBL;

-- pgrust:rowsort
SELECT q1, q2, q1 + q2 AS plus FROM INT8_TBL;
-- pgrust:rowsort
SELECT q1, q2, q1 - q2 AS minus FROM INT8_TBL;
SELECT q1, q2, q1 * q2 AS multiply FROM INT8_TBL;
-- pgrust:rowsort
SELECT q1, q2, q1 * q2 AS multiply FROM INT8_TBL
 WHERE q1 < 1000 or (q2 > 0 and q2 < 1000);
-- pgrust:rowsort
SELECT q1, q2, q1 / q2 AS divide, q1 % q2 AS mod FROM INT8_TBL;

-- pgrust:rowsort
SELECT q1, float8(q1) FROM INT8_TBL;
-- pgrust:rowsort
SELECT q2, float8(q2) FROM INT8_TBL;

-- pgrust:rowsort
SELECT 37 + q1 AS plus4 FROM INT8_TBL;
-- pgrust:rowsort
SELECT 37 - q1 AS minus4 FROM INT8_TBL;
-- pgrust:rowsort
SELECT 2 * q1 AS "twice int4" FROM INT8_TBL;
-- pgrust:rowsort
SELECT q1 * 2 AS "twice int4" FROM INT8_TBL;

-- int8 op int4
-- pgrust:rowsort
SELECT q1 + 42::int4 AS "8plus4", q1 - 42::int4 AS "8minus4", q1 * 42::int4 AS "8mul4", q1 / 42::int4 AS "8div4" FROM INT8_TBL;
-- int4 op int8
-- pgrust:rowsort
SELECT 246::int4 + q1 AS "4plus8", 246::int4 - q1 AS "4minus8", 246::int4 * q1 AS "4mul8", 246::int4 / q1 AS "4div8" FROM INT8_TBL;

-- int8 op int2
-- pgrust:rowsort
SELECT q1 + 42::int2 AS "8plus2", q1 - 42::int2 AS "8minus2", q1 * 42::int2 AS "8mul2", q1 / 42::int2 AS "8div2" FROM INT8_TBL;
-- int2 op int8
-- pgrust:rowsort
SELECT 246::int2 + q1 AS "2plus8", 246::int2 - q1 AS "2minus8", 246::int2 * q1 AS "2mul8", 246::int2 / q1 AS "2div8" FROM INT8_TBL;

-- pgrust:rowsort
SELECT q2, abs(q2) FROM INT8_TBL;
-- pgrust:rowsort
SELECT min(q1), min(q2) FROM INT8_TBL;
-- pgrust:rowsort
SELECT max(q1), max(q2) FROM INT8_TBL;


-- TO_CHAR()
--
-- pgrust:rowsort
SELECT to_char(q1, '9G999G999G999G999G999'), to_char(q2, '9,999,999,999,999,999')
	FROM INT8_TBL;

-- pgrust:rowsort
SELECT to_char(q1, '9G999G999G999G999G999D999G999'), to_char(q2, '9,999,999,999,999,999.999,999')
	FROM INT8_TBL;

-- pgrust:rowsort
SELECT to_char( (q1 * -1), '9999999999999999PR'), to_char( (q2 * -1), '9999999999999999.999PR')
	FROM INT8_TBL;

-- pgrust:rowsort
SELECT to_char( (q1 * -1), '9999999999999999S'), to_char( (q2 * -1), 'S9999999999999999')
	FROM INT8_TBL;

-- pgrust:rowsort
SELECT to_char(q2, 'MI9999999999999999')     FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, '9999999999999999PL')     FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, 'FMS9999999999999999')    FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, 'FM9999999999999999THPR') FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, 'SG9999999999999999th')   FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, '0999999999999999')       FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, 'S0999999999999999')      FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, 'FM0999999999999999')     FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, 'FM9999999999999999.000') FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, 'L9999999999999999.000')  FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, 'FM9999999999999999.999') FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, 'S 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 9 . 9 9 9') FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, E'99999 "text" 9999 "9999" 999 "\\"text between quote marks\\"" 9999') FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, '999999SG9999999999')     FROM INT8_TBL;
-- pgrust:rowsort
SELECT to_char(q2, 'FMRN')                   FROM INT8_TBL;

-- pgrust:rowsort
SELECT to_char(1234, '9.99EEEE');
-- pgrust:rowsort
SELECT to_char(1234::int8, '9.99eeee');
-- pgrust:rowsort
SELECT to_char(-1234::int8, '9.99eeee');
-- pgrust:rowsort
SELECT to_char(1234, '99999V99');
-- pgrust:rowsort
SELECT to_char(1234::int8, '99999V99');

-- check min/max values and overflow behavior

-- pgrust:rowsort
select '-9223372036854775808'::int8;
select '-9223372036854775809'::int8;
-- pgrust:rowsort
select '9223372036854775807'::int8;
select '9223372036854775808'::int8;

-- pgrust:rowsort
select -('-9223372036854775807'::int8);
select -('-9223372036854775808'::int8);
select 0::int8 - '-9223372036854775808'::int8;

select '9223372036854775800'::int8 + '9223372036854775800'::int8;
select '-9223372036854775800'::int8 + '-9223372036854775800'::int8;

select '9223372036854775800'::int8 - '-9223372036854775800'::int8;
select '-9223372036854775800'::int8 - '9223372036854775800'::int8;

select '9223372036854775800'::int8 * '9223372036854775800'::int8;

select '9223372036854775800'::int8 / '0'::int8;
select '9223372036854775800'::int8 % '0'::int8;

select abs('-9223372036854775808'::int8);

select '9223372036854775800'::int8 + '100'::int4;
select '-9223372036854775800'::int8 - '100'::int4;
select '9223372036854775800'::int8 * '100'::int4;

select '100'::int4 + '9223372036854775800'::int8;
select '-100'::int4 - '9223372036854775800'::int8;
select '100'::int4 * '9223372036854775800'::int8;

select '9223372036854775800'::int8 + '100'::int2;
select '-9223372036854775800'::int8 - '100'::int2;
select '9223372036854775800'::int8 * '100'::int2;
select '-9223372036854775808'::int8 / '0'::int2;

select '100'::int2 + '9223372036854775800'::int8;
select '-100'::int2 - '9223372036854775800'::int8;
select '100'::int2 * '9223372036854775800'::int8;
select '100'::int2 / '0'::int8;

-- pgrust:rowsort
SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 = 456;
SELECT CAST(q1 AS int4) FROM int8_tbl WHERE q2 <> 456;

-- pgrust:rowsort
SELECT CAST(q1 AS int2) FROM int8_tbl WHERE q2 = 456;
SELECT CAST(q1 AS int2) FROM int8_tbl WHERE q2 <> 456;

-- pgrust:rowsort
SELECT CAST('42'::int2 AS int8), CAST('-37'::int2 AS int8);

-- pgrust:rowsort
SELECT CAST(q1 AS float4), CAST(q2 AS float8) FROM INT8_TBL;
-- pgrust:rowsort
SELECT CAST('36854775807.0'::float4 AS int8);
SELECT CAST('922337203685477580700.0'::float8 AS int8);

SELECT CAST(q1 AS oid) FROM INT8_TBL;
SELECT oid::int8 FROM pg_class WHERE relname = 'pg_class';


-- bit operations

-- pgrust:rowsort
SELECT q1, q2, q1 & q2 AS "and", q1 | q2 AS "or", q1 # q2 AS "xor", ~q1 AS "not" FROM INT8_TBL;
-- pgrust:rowsort
SELECT q1, q1 << 2 AS "shl", q1 >> 3 AS "shr" FROM INT8_TBL;


-- generate_series

-- pgrust:rowsort
SELECT * FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8);
SELECT * FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8, 0);
-- pgrust:rowsort
SELECT * FROM generate_series('+4567890123456789'::int8, '+4567890123456799'::int8, 2);

-- corner case
-- pgrust:rowsort
SELECT (-1::int8<<63)::text;
-- pgrust:rowsort
SELECT ((-1::int8<<63)+1)::text;

-- check sane handling of INT64_MIN overflow cases
SELECT (-9223372036854775808)::int8 * (-1)::int8;
SELECT (-9223372036854775808)::int8 / (-1)::int8;
-- pgrust:rowsort
SELECT (-9223372036854775808)::int8 % (-1)::int8;
SELECT (-9223372036854775808)::int8 * (-1)::int4;
SELECT (-9223372036854775808)::int8 / (-1)::int4;
-- pgrust:rowsort
SELECT (-9223372036854775808)::int8 % (-1)::int4;
SELECT (-9223372036854775808)::int8 * (-1)::int2;
SELECT (-9223372036854775808)::int8 / (-1)::int2;
-- pgrust:rowsort
SELECT (-9223372036854775808)::int8 % (-1)::int2;

-- check rounding when casting from float
-- pgrust:rowsort
SELECT x, x::int8 AS int8_value
FROM (VALUES (-2.5::float8),
             (-1.5::float8),
             (-0.5::float8),
             (0.0::float8),
             (0.5::float8),
             (1.5::float8),
             (2.5::float8)) t(x);

-- check rounding when casting from numeric
-- pgrust:rowsort
SELECT x, x::int8 AS int8_value
FROM (VALUES (-2.5::numeric),
             (-1.5::numeric),
             (-0.5::numeric),
             (0.0::numeric),
             (0.5::numeric),
             (1.5::numeric),
             (2.5::numeric)) t(x);

-- test gcd()
-- pgrust:rowsort
SELECT a, b, gcd(a, b), gcd(a, -b), gcd(b, a), gcd(-b, a)
FROM (VALUES (0::int8, 0::int8),
             (0::int8, 29893644334::int8),
             (288484263558::int8, 29893644334::int8),
             (-288484263558::int8, 29893644334::int8),
             ((-9223372036854775808)::int8, 1::int8),
             ((-9223372036854775808)::int8, 9223372036854775807::int8),
             ((-9223372036854775808)::int8, 4611686018427387904::int8)) AS v(a, b);

SELECT gcd((-9223372036854775808)::int8, 0::int8); -- overflow
SELECT gcd((-9223372036854775808)::int8, (-9223372036854775808)::int8); -- overflow

-- test lcm()
-- pgrust:rowsort
SELECT a, b, lcm(a, b), lcm(a, -b), lcm(b, a), lcm(-b, a)
FROM (VALUES (0::int8, 0::int8),
             (0::int8, 29893644334::int8),
             (29893644334::int8, 29893644334::int8),
             (288484263558::int8, 29893644334::int8),
             (-288484263558::int8, 29893644334::int8),
             ((-9223372036854775808)::int8, 0::int8)) AS v(a, b);

SELECT lcm((-9223372036854775808)::int8, 1::int8); -- overflow
SELECT lcm(9223372036854775807::int8, 9223372036854775806::int8); -- overflow


-- non-decimal literals

-- pgrust:rowsort
SELECT int8 '0b100101';
-- pgrust:rowsort
SELECT int8 '0o273';
-- pgrust:rowsort
SELECT int8 '0x42F';

SELECT int8 '0b';
SELECT int8 '0o';
SELECT int8 '0x';

-- cases near overflow
-- pgrust:rowsort
SELECT int8 '0b111111111111111111111111111111111111111111111111111111111111111';
SELECT int8 '0b1000000000000000000000000000000000000000000000000000000000000000';
-- pgrust:rowsort
SELECT int8 '0o777777777777777777777';
SELECT int8 '0o1000000000000000000000';
-- pgrust:rowsort
SELECT int8 '0x7FFFFFFFFFFFFFFF';
SELECT int8 '0x8000000000000000';

-- pgrust:rowsort
SELECT int8 '-0b1000000000000000000000000000000000000000000000000000000000000000';
SELECT int8 '-0b1000000000000000000000000000000000000000000000000000000000000001';
-- pgrust:rowsort
SELECT int8 '-0o1000000000000000000000';
SELECT int8 '-0o1000000000000000000001';
-- pgrust:rowsort
SELECT int8 '-0x8000000000000000';
SELECT int8 '-0x8000000000000001';


-- underscores

-- pgrust:rowsort
SELECT int8 '1_000_000';
-- pgrust:rowsort
SELECT int8 '1_2_3';
-- pgrust:rowsort
SELECT int8 '0x1EEE_FFFF';
-- pgrust:rowsort
SELECT int8 '0o2_73';
-- pgrust:rowsort
SELECT int8 '0b_10_0101';

-- error cases
SELECT int8 '_100';
SELECT int8 '100_';
SELECT int8 '100__000';
