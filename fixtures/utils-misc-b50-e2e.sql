-- bugs/batch-50-backend-utils-misc: SQL-visible fixes in utils/mmgr, expected
-- captured from C 18.6.
\set VERBOSITY verbose
-- portalmem.c:121 hash_create: the portal table owns a "Portal hash" context
-- directly under TopMemoryContext.
SELECT name, level FROM pg_backend_memory_contexts WHERE name = 'Portal hash';
-- portalmem.c:595 PortalDrop deletes the portal context: closed cursors leave
-- no ident-less PortalContext rows behind.
BEGIN;
DECLARE b50_c1 CURSOR FOR SELECT 1;
DECLARE b50_c2 CURSOR FOR SELECT 2;
DECLARE b50_c3 CURSOR FOR SELECT 3;
SELECT ident FROM pg_backend_memory_contexts WHERE name = 'PortalContext' AND ident LIKE 'b50_c%' ORDER BY 1;
CLOSE ALL;
SELECT count(*) FROM pg_backend_memory_contexts WHERE name = 'PortalContext' AND ident IS NULL;
DECLARE b50_c4 CURSOR FOR SELECT 4;
SELECT ident FROM pg_backend_memory_contexts WHERE name = 'PortalContext' AND ident LIKE 'b50_c%' ORDER BY 1;
ROLLBACK;
SELECT count(*) FROM pg_backend_memory_contexts WHERE name = 'PortalContext' AND ident IS NULL;
