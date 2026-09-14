-- bugs/batch-154-backend-storage-buffer: SQL-visible fixes in storage/buffer,
-- expected captured from C 18.6.
\set VERBOSITY verbose
-- bufmgr.c:4049 InitBufferManagerAccess hash_create("PrivateRefCount"): every
-- backend owns the context from start-up, including one that never pinned
-- more than the eight inline refcount entries at once (a fresh database has
-- no relcache init file to overflow them).
CREATE DATABASE b154_privref;
\c b154_privref
SELECT name, level FROM pg_backend_memory_contexts WHERE name = 'PrivateRefCount';
\c postgres
DROP DATABASE b154_privref;
