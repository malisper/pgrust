-- functions.c:2665: a scalar SQL-function result is held in the junkfilter
-- slot, never spooled through a spillable tuplestore, so temp_file_limit
-- never applies to it.
SET work_mem = '64kB';
SET temp_file_limit = 0;
CREATE FUNCTION sqlfn_large_scalar() RETURNS text LANGUAGE sql VOLATILE
  AS $$SELECT 1; SELECT repeat('x', 1000000);$$;
SELECT length(sqlfn_large_scalar());
SELECT length(sqlfn_large_scalar()) FROM generate_series(1, 3);
DROP FUNCTION sqlfn_large_scalar();
RESET temp_file_limit;
RESET work_mem;
