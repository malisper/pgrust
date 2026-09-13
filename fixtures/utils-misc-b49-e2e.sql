-- bugs/batch-49-backend-utils-misc: SQL-visible fixes in utils, expected
-- captured from C 18.6.
\set VERBOSITY verbose
-- elog.c:2172 check_backtrace_functions and :2213 check_log_destination
-- fail through GUC_check_errdetail: 22023 with the parameter wrapper.
SET backtrace_functions = 'bad-name';
ALTER SYSTEM SET log_destination = 'nowhere';
ALTER SYSTEM SET log_destination = 'stderr,';
SET backtrace_functions = 'b49_ok, b49_also_ok';
SHOW backtrace_functions;
RESET backtrace_functions;
-- pg_controldata.c:41: a RECORD-declared alias takes its descriptor from
-- the column definition list; a scalar alias is the row-type elog.
CREATE FUNCTION b49_control_record() RETURNS record LANGUAGE internal AS 'pg_control_system';
SELECT pg_control_version, catalog_version_no FROM b49_control_record() AS t(pg_control_version integer, catalog_version_no integer, system_identifier bigint, pg_control_last_modified timestamptz);
CREATE FUNCTION b49_control_int() RETURNS integer LANGUAGE internal AS 'pg_control_system';
SELECT b49_control_int();
DROP FUNCTION b49_control_record();
DROP FUNCTION b49_control_int();
