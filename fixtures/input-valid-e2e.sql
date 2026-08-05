-- pg_input_is_valid / pg_input_error_info parity matrix over the landed
-- input functions. Run via scripts/input-valid-e2e.sh (two-binary diff vs C).
\set VERBOSITY verbose
-- boolean
SELECT pg_input_is_valid('true', 'bool');
SELECT pg_input_is_valid('junk', 'bool');
SELECT pg_input_is_valid('asdf', 'bool');
SELECT * FROM pg_input_error_info('junk', 'bool');
-- int2
SELECT pg_input_is_valid('34', 'int2');
SELECT pg_input_is_valid('asdf', 'int2');
SELECT pg_input_is_valid('50000', 'int2');
SELECT * FROM pg_input_error_info('50000', 'int2');
-- int4
SELECT pg_input_is_valid('34', 'int4');
SELECT pg_input_is_valid('asdf', 'int4');
SELECT pg_input_is_valid('1000000000000', 'int4');
SELECT * FROM pg_input_error_info('1000000000000', 'int4');
SELECT * FROM pg_input_error_info('asdf', 'int4');
-- int8
SELECT pg_input_is_valid('34', 'int8');
SELECT pg_input_is_valid('asdf', 'int8');
SELECT pg_input_is_valid('10000000000000000000', 'int8');
SELECT * FROM pg_input_error_info('10000000000000000000', 'int8');
-- float4
SELECT pg_input_is_valid('34.5', 'float4');
SELECT pg_input_is_valid('asdf', 'float4');
SELECT pg_input_is_valid('xyz', 'float4');
SELECT pg_input_is_valid('1e400', 'float4');
SELECT * FROM pg_input_error_info('1e400', 'float4');
-- float8
SELECT pg_input_is_valid('34.5', 'float8');
SELECT pg_input_is_valid('asdf', 'float8');
SELECT pg_input_is_valid('xyz', 'float8');
SELECT pg_input_is_valid('1e4000', 'float8');
SELECT * FROM pg_input_error_info('1e4000', 'float8');
-- numeric
SELECT pg_input_is_valid('34.5', 'numeric');
SELECT pg_input_is_valid('asdf', 'numeric');
SELECT pg_input_is_valid('1e347', 'numeric(7,4)');
SELECT * FROM pg_input_error_info('1e347', 'numeric(7,4)');
SELECT * FROM pg_input_error_info('112', 'numeric(2,1)');
SELECT * FROM pg_input_error_info('asdf', 'numeric');
-- money
SELECT pg_input_is_valid('$34.50', 'money');
SELECT pg_input_is_valid('asdf', 'money');
SELECT pg_input_is_valid('10000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000$', 'money');
SELECT * FROM pg_input_error_info('asdf', 'money');
SELECT pg_input_is_valid('\x0001', 'money');
SELECT * FROM pg_input_error_info('\x0001', 'money');
SELECT pg_input_is_valid('192233720368547758.07', 'money');
SELECT * FROM pg_input_error_info('192233720368547758.07', 'money');
-- date
SELECT pg_input_is_valid('2022-01-01', 'date');
SELECT pg_input_is_valid('asdf', 'date');
SELECT pg_input_is_valid('now', 'date');
SELECT pg_input_is_valid('garbage', 'date');
SELECT pg_input_is_valid('6874898-01-01', 'date');
SELECT * FROM pg_input_error_info('garbage', 'date');
SELECT * FROM pg_input_error_info('6874898-01-01', 'date');
SELECT pg_input_is_valid('1995-08-06 epoch', 'date');
SELECT * FROM pg_input_error_info('1995-08-06 epoch', 'date');
SELECT * FROM pg_input_error_info('asdf', 'date');
-- time
SELECT pg_input_is_valid('12:00:00', 'time');
SELECT pg_input_is_valid('25:00:00', 'time');
SELECT * FROM pg_input_error_info('25:00:00', 'time');
-- timestamp
SELECT pg_input_is_valid('2022-01-01 12:00:00', 'timestamp');
SELECT pg_input_is_valid('asdf', 'timestamp');
SELECT pg_input_is_valid('2022-01-01 25:00:00', 'timestamp');
SELECT * FROM pg_input_error_info('2022-01-01 25:00:00', 'timestamp');
-- timestamptz
SELECT pg_input_is_valid('2022-01-01 12:00:00 PST', 'timestamptz');
SELECT pg_input_is_valid('asdf', 'timestamptz');
SELECT * FROM pg_input_error_info('asdf', 'timestamptz');
-- json
SELECT pg_input_is_valid('{"a":1}', 'json');
SELECT pg_input_is_valid('{"a":1', 'json');
SELECT * FROM pg_input_error_info('{"a":true', 'json');
-- jsonb
SELECT pg_input_is_valid('{"a":1}', 'jsonb');
SELECT pg_input_is_valid('{"a":1', 'jsonb');
SELECT * FROM pg_input_error_info('{"a":true', 'jsonb');
SELECT * FROM pg_input_error_info('{"a":1e1000000}', 'jsonb');
-- bytea
SELECT pg_input_is_valid('\xDEADBEEF', 'bytea');
SELECT pg_input_is_valid('\xDEADBEEF0', 'bytea');
SELECT * FROM pg_input_error_info('\xDEADBEEF0', 'bytea');
-- text / name / "char" (unfailable input functions)
SELECT pg_input_is_valid('foo', 'text');
SELECT pg_input_is_valid('foo', 'name');
SELECT pg_input_is_valid('x', '"char"');
SELECT * FROM pg_input_error_info('foo', 'text');
-- arrays
SELECT pg_input_is_valid('{1,2,3}', 'integer[]');
SELECT pg_input_is_valid('{1,2', 'integer[]');
SELECT pg_input_is_valid('{1,zed}', 'integer[]');
SELECT * FROM pg_input_error_info('{1,zed}', 'integer[]');
SELECT * FROM pg_input_error_info('{1,2', 'text[]');
-- qualified and quoted type names
SELECT pg_input_is_valid('42', 'pg_catalog.int4');
SELECT * FROM pg_input_error_info('junk', 'pg_catalog.int8');
-- caching across calls: same statement, repeated call sites
SELECT pg_input_is_valid('1', 'int4'), pg_input_is_valid('junk', 'int4'), pg_input_is_valid('-42', 'int4');
-- hard errors: bad type names and disallowed typmods
SELECT pg_input_is_valid('42', 'no_such_type');
SELECT pg_input_is_valid('42', 'int4(3)');
SELECT * FROM pg_input_error_info('1234', 'numeric(2000)');
SELECT * FROM pg_input_error_info('1234', 'numeric(1,2,3)');
SELECT pg_input_is_valid('42', '');
SELECT pg_input_is_valid('42', 'SETOF int4');
