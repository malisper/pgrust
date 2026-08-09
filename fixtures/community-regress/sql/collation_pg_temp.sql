-- malisper/pgrust#78: CREATE COLLATION targeting the session's temp namespace
-- panicked the backend instead of following C's namespace resolution
-- (QualifiedNameGetCreationNamespace maps the pg_temp alias, and a pending
-- temp creation namespace on the search path, to the session temp namespace;
-- temp collations are legal and are dropped with the temp schema).
CREATE COLLATION pg_temp.temp_coll FROM "C";
SELECT collname FROM pg_collation WHERE collname = 'temp_coll';
SELECT n.nspname LIKE 'pg_temp%' AS in_temp_namespace
  FROM pg_collation c JOIN pg_namespace n ON n.oid = c.collnamespace
 WHERE c.collname = 'temp_coll';
-- unqualified name with pg_temp as the creation schema
SET search_path = pg_temp, public;
CREATE COLLATION temp_coll2 (locale = 'C');
SELECT n.nspname LIKE 'pg_temp%' AS in_temp_namespace
  FROM pg_collation c JOIN pg_namespace n ON n.oid = c.collnamespace
 WHERE c.collname = 'temp_coll2';
RESET search_path;
-- temp collations are usable and droppable like any other
SELECT 'b' < 'a' COLLATE pg_temp.temp_coll AS lt;
DROP COLLATION pg_temp.temp_coll;
DROP COLLATION pg_temp.temp_coll2;
SELECT count(*) FROM pg_collation WHERE collname IN ('temp_coll', 'temp_coll2');
