-- Imported from public PR malisper/pgrust#35 (author: jschaf, head 61d680bd5e).
-- Expected output captured from real PostgreSQL 18.
-- SQL-body introspection must support functions declared with SQL-standard
-- bodies; pg_dump uses this function while reading their definitions.
CREATE FUNCTION function_sqlbody(value integer)
RETURNS integer
LANGUAGE SQL
RETURN value + 1;

SELECT pg_get_function_sqlbody('function_sqlbody(integer)'::regprocedure);
