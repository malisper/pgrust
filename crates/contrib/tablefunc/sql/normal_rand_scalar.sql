CREATE EXTENSION tablefunc;

-- normal_rand runs SRF_FIRSTCALL_INIT before validating the row count, so a
-- scalar-declared entry point fails with 0A000 before 22023 (tablefunc.c:194).
CREATE FUNCTION tf_scalar(integer, double precision, double precision)
  RETURNS double precision
  AS '$libdir/tablefunc', 'normal_rand' LANGUAGE C STRICT;
SELECT tf_scalar(-1, 0, 1);
SELECT tf_scalar(1, 0, 1);
DROP FUNCTION tf_scalar(integer, double precision, double precision);
SELECT * FROM normal_rand(-1, 0, 1);
SELECT count(*) FROM normal_rand(5, 0, 1);
