--
-- MONEY
--
-- Note that we assume lc_monetary has been set to C.
--

CREATE TABLE money_data (m money);

INSERT INTO money_data VALUES ('123');
-- pgrust:rowsort
SELECT * FROM money_data;
-- pgrust:rowsort
SELECT m + '123' FROM money_data;
-- pgrust:rowsort
SELECT m + '123.45' FROM money_data;
-- pgrust:rowsort
SELECT m - '123.45' FROM money_data;
-- pgrust:rowsort
SELECT m / '2'::money FROM money_data;
-- pgrust:rowsort
SELECT m * 2 FROM money_data;
-- pgrust:rowsort
SELECT 2 * m FROM money_data;
-- pgrust:rowsort
SELECT m / 2 FROM money_data;
-- pgrust:rowsort
SELECT m * 2::int2 FROM money_data;
-- pgrust:rowsort
SELECT 2::int2 * m FROM money_data;
-- pgrust:rowsort
SELECT m / 2::int2 FROM money_data;
-- pgrust:rowsort
SELECT m * 2::int8 FROM money_data;
-- pgrust:rowsort
SELECT 2::int8 * m FROM money_data;
-- pgrust:rowsort
SELECT m / 2::int8 FROM money_data;
-- pgrust:rowsort
SELECT m * 2::float8 FROM money_data;
-- pgrust:rowsort
SELECT 2::float8 * m FROM money_data;
-- pgrust:rowsort
SELECT m / 2::float8 FROM money_data;
-- pgrust:rowsort
SELECT m * 2::float4 FROM money_data;
-- pgrust:rowsort
SELECT 2::float4 * m FROM money_data;
-- pgrust:rowsort
SELECT m / 2::float4 FROM money_data;

-- All true
-- pgrust:rowsort
SELECT m = '$123.00' FROM money_data;
-- pgrust:rowsort
SELECT m != '$124.00' FROM money_data;
-- pgrust:rowsort
SELECT m <= '$123.00' FROM money_data;
-- pgrust:rowsort
SELECT m >= '$123.00' FROM money_data;
-- pgrust:rowsort
SELECT m < '$124.00' FROM money_data;
-- pgrust:rowsort
SELECT m > '$122.00' FROM money_data;

-- All false
-- pgrust:rowsort
SELECT m = '$123.01' FROM money_data;
-- pgrust:rowsort
SELECT m != '$123.00' FROM money_data;
-- pgrust:rowsort
SELECT m <= '$122.99' FROM money_data;
-- pgrust:rowsort
SELECT m >= '$123.01' FROM money_data;
-- pgrust:rowsort
SELECT m > '$124.00' FROM money_data;
-- pgrust:rowsort
SELECT m < '$122.00' FROM money_data;

-- pgrust:rowsort
SELECT cashlarger(m, '$124.00') FROM money_data;
-- pgrust:rowsort
SELECT cashsmaller(m, '$124.00') FROM money_data;
-- pgrust:rowsort
SELECT cash_words(m) FROM money_data;
-- pgrust:rowsort
SELECT cash_words(m + '1.23') FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.45');
-- pgrust:rowsort
SELECT * FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.451');
-- pgrust:rowsort
SELECT * FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.454');
-- pgrust:rowsort
SELECT * FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.455');
-- pgrust:rowsort
SELECT * FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.456');
-- pgrust:rowsort
SELECT * FROM money_data;

DELETE FROM money_data;
INSERT INTO money_data VALUES ('$123.459');
-- pgrust:rowsort
SELECT * FROM money_data;

-- input checks
-- pgrust:rowsort
SELECT '1234567890'::money;
-- pgrust:rowsort
SELECT '12345678901234567'::money;
SELECT '123456789012345678'::money;
SELECT '9223372036854775807'::money;
-- pgrust:rowsort
SELECT '-12345'::money;
-- pgrust:rowsort
SELECT '-1234567890'::money;
-- pgrust:rowsort
SELECT '-12345678901234567'::money;
SELECT '-123456789012345678'::money;
SELECT '-9223372036854775808'::money;

-- special characters
-- pgrust:rowsort
SELECT '(1)'::money;
-- pgrust:rowsort
SELECT '($123,456.78)'::money;

-- test non-error-throwing API
SELECT pg_input_is_valid('\x0001', 'money');
SELECT * FROM pg_input_error_info('\x0001', 'money');
SELECT pg_input_is_valid('192233720368547758.07', 'money');
SELECT * FROM pg_input_error_info('192233720368547758.07', 'money');

-- documented minimums and maximums
-- pgrust:rowsort
SELECT '-92233720368547758.08'::money;
-- pgrust:rowsort
SELECT '92233720368547758.07'::money;

SELECT '-92233720368547758.09'::money;
SELECT '92233720368547758.08'::money;

-- rounding
SELECT '-92233720368547758.085'::money;
SELECT '92233720368547758.075'::money;

-- rounding vs. truncation in division
-- pgrust:rowsort
SELECT '878.08'::money / 11::float8;
-- pgrust:rowsort
SELECT '878.08'::money / 11::float4;
-- pgrust:rowsort
SELECT '878.08'::money / 11::bigint;
-- pgrust:rowsort
SELECT '878.08'::money / 11::int;
-- pgrust:rowsort
SELECT '878.08'::money / 11::smallint;

-- check for precision loss in division
-- pgrust:rowsort
SELECT '90000000000000099.00'::money / 10::bigint;
-- pgrust:rowsort
SELECT '90000000000000099.00'::money / 10::int;
-- pgrust:rowsort
SELECT '90000000000000099.00'::money / 10::smallint;

-- Cast int4/int8/numeric to money
-- pgrust:rowsort
SELECT 1234567890::money;
-- pgrust:rowsort
SELECT 12345678901234567::money;
-- pgrust:rowsort
SELECT (-12345)::money;
-- pgrust:rowsort
SELECT (-1234567890)::money;
-- pgrust:rowsort
SELECT (-12345678901234567)::money;
-- pgrust:rowsort
SELECT 1234567890::int4::money;
-- pgrust:rowsort
SELECT 12345678901234567::int8::money;
-- pgrust:rowsort
SELECT 12345678901234567::numeric::money;
-- pgrust:rowsort
SELECT (-1234567890)::int4::money;
-- pgrust:rowsort
SELECT (-12345678901234567)::int8::money;
-- pgrust:rowsort
SELECT (-12345678901234567)::numeric::money;

-- Cast from money to numeric
-- pgrust:rowsort
SELECT '12345678901234567'::money::numeric;
-- pgrust:rowsort
SELECT '-12345678901234567'::money::numeric;
-- pgrust:rowsort
SELECT '92233720368547758.07'::money::numeric;
-- pgrust:rowsort
SELECT '-92233720368547758.08'::money::numeric;

-- overflow checks
SELECT '92233720368547758.07'::money + '0.01'::money;
SELECT '-92233720368547758.08'::money - '0.01'::money;
SELECT '92233720368547758.07'::money * 2::float8;
SELECT '-1'::money / 1.175494e-38::float4;
SELECT '92233720368547758.07'::money * 2::int4;
SELECT '1'::money / 0::int2;
SELECT '42'::money * 'inf'::float8;
SELECT '42'::money * '-inf'::float8;
SELECT '42'::money * 'nan'::float4;
