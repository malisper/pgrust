-- bugs/batch-60-backend-utils-cache: cache fixes, expected captured from C 18.6.
\set VERBOSITY verbose
-- fp-cache-plancache#4: plancache.c:1100 labels the CachedPlan context with
-- the source query string, like the CachedPlanSource context.
PREPARE b60_probe AS SELECT 1 AS b60_probe_col;
EXECUTE b60_probe;
SELECT name, ident FROM pg_backend_memory_contexts
  WHERE name IN ('CachedPlan', 'CachedPlanSource') AND ident LIKE 'PREPARE b60_probe%' ORDER BY 1;
DEALLOCATE b60_probe;
-- fp-cache-relcache-p2#3: relcache.c:3678 withholds the default replica
-- identity only from IsCatalogNamespace (pg_catalog), not from pg_toast.
SET allow_system_table_mods = on;
CREATE TABLE pg_toast.b60_identity (id integer);
CREATE TABLE pg_catalog.b60_identity (id integer);
SELECT relname, relreplident FROM pg_class WHERE relname = 'b60_identity' ORDER BY relnamespace;
DROP TABLE pg_toast.b60_identity;
DROP TABLE pg_catalog.b60_identity;
RESET allow_system_table_mods;
